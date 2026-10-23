//! The data-plane pump: loadsmith sits between source and destination.
//!
//! Architecture: the source writes an Arrow IPC stream into one pipe (read end
//! held by core), and the destination reads an Arrow IPC stream from another
//! pipe (write end held by core). The pump copies every RecordBatch from the
//! source stream to the destination stream, counting rows/batches as it goes.
//!
//! Neither plugin knows the other exists — data always flows through the
//! control plane (loadsmith), which is what lets us observe and report progress.
//!
//! **The pump never ends the destination's stream.** When the source's stream
//! ends, the pump returns the still-open destination stream ([`PumpOutput`])
//! and the runner decides: it writes the end-of-stream marker only after the
//! source also reported `finished` with success. After any upstream failure
//! the destination therefore never sees a clean end of input; it gets
//! `cancel` instead, and its fd3 stays open until it has exited.
//!
//! **Flow control.** With a source that obeys credits, the pump owns the
//! [`FlowWindow`]: after each batch it hands to the destination it grants the
//! source the next batches, up to a window sized from the batches seen and
//! the memory budget. The runner relays each grant as a `credit` message.

use std::fs::File;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use loadsmith_arrow::{IpcReader, IpcWriter};
use loadsmith_config::FlowConfig;
use tokio::sync::watch;

use crate::telemetry;


/// Totals observed by the pump as data flowed through it.
#[derive(Debug, Default, Clone, Copy)]
pub struct PumpStats {
    pub rows: u64,
    pub batches: u64,
    /// Time spent reading the source's stream (waiting for it included).
    pub reading: Duration,
    /// Time spent writing the destination's stream (waiting for it included).
    pub writing: Duration,
    /// The largest flow-control window granted (0 without flow control).
    pub peak_window: u64,
}

/// The core's flow control for one source: how many batches the source may
/// have fetched beyond those the pump already handed to the destination.
///
/// The window starts at two batches and is resized after every batch to what
/// the memory budget holds at the largest batch seen so far, within
/// `max_inflight_batches`. Grants are cumulative and never shrink: a smaller
/// window takes effect as the destination catches up.
#[derive(Debug)]
pub struct FlowWindow {
    tx: watch::Sender<u64>,
    max: u64,
    budget: u64,
    window: u64,
    largest: u64,
}

impl FlowWindow {
    /// A window per `config`, and the receiver of its grants. The initial
    /// grant (for `start.credit`) is [`granted`](Self::granted).
    pub fn new(config: &FlowConfig) -> (Self, watch::Receiver<u64>) {
        let max = config.max_inflight_batches.max(1);
        let window = max.min(2);
        let (tx, rx) = watch::channel(window);
        (Self { tx, max, budget: config.memory_budget_mib.saturating_mul(1 << 20), window, largest: 0 }, rx)
    }

    /// The highest batch ordinal granted so far.
    pub fn granted(&self) -> u64 {
        *self.tx.borrow()
    }

    pub fn window(&self) -> u64 {
        self.window
    }

    /// The destination took batch `forwarded` (1-based), of `bytes`.
    fn forwarded(&mut self, forwarded: u64, bytes: u64) {
        self.largest = self.largest.max(bytes);
        self.window = match (self.budget, self.largest) {
            (0, _) | (_, 0) => self.max,
            (budget, largest) => (budget / largest).clamp(1, self.max),
        };
        let grant = forwarded + self.window;
        self.tx.send_if_modified(|granted| {
            let grows = grant > *granted;
            if grows {
                *granted = grant;
            }
            grows
        });
    }
}

/// Shared between the pump thread and the runner.
#[derive(Debug, Default)]
pub struct PumpControl {
    /// Set by the runner when the run aborts: stop forwarding to the
    /// destination (the source is still drained, so it is never blocked on a
    /// full pipe and can see its `cancel`).
    pub abort: AtomicBool,
    /// Batches read from the source so far (for the stall watchdog).
    pub batches: AtomicU64,
}

/// The destination's end of the data plane, still open.
pub enum PumpOutput {
    /// The stream is open: schema (and batches) written, no end marker yet.
    Open(IpcWriter<File>),
    /// Writing to the destination failed; the pipe is gone.
    Closed,
}

impl PumpOutput {
    /// Writes the end-of-stream marker and closes fd3: the destination may
    /// now finalize. Only after the source finished successfully.
    pub fn finish(self) -> anyhow::Result<()> {
        match self {
            PumpOutput::Open(w) => w.finish().map_err(|e| anyhow::anyhow!("data plane finish: {e}")),
            PumpOutput::Closed => anyhow::bail!("the destination stream is closed"),
        }
    }
}

/// How the source's stream ended, and what is left of the destination's.
pub struct PumpOutcome {
    pub stats: PumpStats,
    /// The source's stream ended with its end-of-stream marker.
    pub source_clean: bool,
    /// Reading the source's stream failed (not a clean or truncated end).
    pub read_error: Option<String>,
    /// Writing to the destination failed (it stopped reading).
    pub write_error: Option<String>,
    pub output: PumpOutput,
}

/// Copies the Arrow IPC stream from `reader` (the source's, its header
/// already read by the runner) to `write_out` (destination) until the
/// source's stream ends, invoking
/// `on_progress(rows, batches)` at exponentially growing batch intervals
/// (1, 2, 4, 8, …) so logs never flood.
///
/// A failed write to the destination (or `control.abort`) does not stop the
/// pump: it keeps draining the source, so a source never blocks on a full
/// pipe while the run is being aborted.
///
/// This is synchronous, blocking I/O — intended to run on a blocking thread
/// (e.g. `tokio::task::spawn_blocking`).
pub fn pump<F>(
    mut reader: IpcReader<File>,
    write_out: File,
    control: Arc<PumpControl>,
    mut flow: Option<FlowWindow>,
    mut on_progress: F,
) -> PumpOutcome
where
    F: FnMut(u64, u64),
{
    let mut stats = PumpStats::default();
    let schema = reader.schema();
    let mut write_error = None;
    let mut writer = match IpcWriter::new(write_out, schema.as_ref()) {
        Ok(w) => Some(w),
        Err(e) => {
            write_error = Some(format!("data plane writer: {e}"));
            None
        }
    };

    let mut next_log: u64 = 1;
    let mut read_error = None;
    if let Some(f) = &flow {
        stats.peak_window = f.window();
    }
    loop {
        let began = Instant::now();
        let next = reader.read_batch();
        stats.reading += began.elapsed();
        match next {
            Ok(Some(batch)) => {
                let rows = batch.num_rows() as u64;
                let bytes =
                    if telemetry::enabled() || flow.is_some() { loadsmith_arrow::batch_bytes(&batch) } else { 0 };
                stats.rows += rows;
                stats.batches += 1;
                control.batches.store(stats.batches, Ordering::Relaxed);
                telemetry::record_batch("read", rows, bytes);
                if !control.abort.load(Ordering::Relaxed) {
                    if let Some(w) = writer.as_mut() {
                        let began = Instant::now();
                        match w.write_batch(&batch) {
                            Ok(()) => telemetry::record_batch("written", rows, bytes),
                            Err(e) => {
                                write_error = Some(format!("write batch: {e}"));
                                writer = None;
                            }
                        }
                        stats.writing += began.elapsed();
                    }
                }
                // Granted after the write: the batch has left the core. After
                // an abort the source is drained, so it keeps getting credit.
                if let Some(f) = flow.as_mut() {
                    f.forwarded(stats.batches, bytes);
                    stats.peak_window = stats.peak_window.max(f.window());
                }
                if stats.batches == next_log {
                    on_progress(stats.rows, stats.batches);
                    next_log *= 2;
                }
            }
            Ok(None) => break,
            Err(e) => {
                read_error = Some(format!("read batch: {e}"));
                break;
            }
        }
    }

    // Emit a final progress tick if the last batch didn't land on a power of two,
    // so the displayed total always matches reality.
    if stats.batches > 0 && stats.batches != next_log / 2 {
        on_progress(stats.rows, stats.batches);
    }

    PumpOutcome {
        stats,
        source_clean: read_error.is_none() && reader.ended_cleanly(),
        read_error,
        write_error,
        output: match writer {
            Some(w) => PumpOutput::Open(w),
            None => PumpOutput::Closed,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::io::Read;

    fn batch() -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let b = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
            .unwrap();
        (schema, b)
    }

    fn pipe() -> (File, File) {
        crate::spawner::create_data_pipe().unwrap()
    }

    /// Writes `n` batches to a fresh pipe, optionally with the end marker, and
    /// returns a reader on it (header read, as the runner hands it over).
    fn source_stream(n: usize, finish: bool) -> IpcReader<File> {
        let (r, w) = pipe();
        let (schema, b) = batch();
        let mut writer = IpcWriter::new(w, &schema).unwrap();
        for _ in 0..n {
            writer.write_batch(&b).unwrap();
        }
        if finish {
            writer.finish().unwrap();
        } else {
            drop(writer);
        }
        IpcReader::new(r).unwrap()
    }

    #[test]
    fn a_clean_source_stream_leaves_the_destination_stream_open() {
        let (dst_r, dst_w) = pipe();
        let out = pump(source_stream(3, true), dst_w, Arc::default(), None, |_, _| {});
        assert!(out.source_clean);
        assert_eq!(out.stats.batches, 3);
        assert!(matches!(out.output, PumpOutput::Open(_)));
        // The runner ends the stream; until then the destination has no marker.
        out.output.finish().unwrap();
        let mut reader = IpcReader::new(dst_r).unwrap();
        let mut n = 0;
        while reader.read_batch().unwrap().is_some() {
            n += 1;
        }
        assert_eq!(n, 3);
        assert!(reader.ended_cleanly());
    }

    /// The bug this guards against: a source that dies at a batch boundary
    /// used to look like a clean end, and the pump ended the destination's
    /// stream, so the destination committed a partial load.
    #[test]
    fn a_truncated_source_stream_is_not_clean_and_is_not_forwarded_as_ended() {
        let (mut dst_r, dst_w) = pipe();
        let out = pump(source_stream(2, false), dst_w, Arc::default(), None, |_, _| {});
        assert!(!out.source_clean);
        assert!(out.read_error.is_none());
        assert_eq!(out.stats.batches, 2);
        // Drop the output without finishing (what an abort does once the
        // destination is gone): no end marker ever reaches the reader.
        drop(out);
        let mut bytes = Vec::new();
        dst_r.read_to_end(&mut bytes).unwrap();
        let mut reader = IpcReader::new(bytes.as_slice()).unwrap();
        while reader.read_batch().unwrap().is_some() {}
        assert!(!reader.ended_cleanly());
    }

    fn flow(max_inflight_batches: u64, memory_budget_mib: u64) -> (FlowWindow, watch::Receiver<u64>) {
        FlowWindow::new(&FlowConfig { max_inflight_batches, memory_budget_mib })
    }

    #[test]
    fn the_window_starts_at_two_batches() {
        assert_eq!(flow(8, 256).0.granted(), 2);
        assert_eq!(flow(1, 256).0.granted(), 1, "never past the ceiling");
    }

    #[test]
    fn each_forwarded_batch_grants_a_window_ahead() {
        let (mut w, rx) = flow(8, 0);
        w.forwarded(1, 1000);
        assert_eq!(*rx.borrow(), 9, "no memory limit: the full ceiling");
        w.forwarded(2, 1000);
        assert_eq!(*rx.borrow(), 10);
    }

    #[test]
    fn the_memory_budget_sizes_the_window_from_the_largest_batch() {
        let (mut w, rx) = flow(8, 1);
        w.forwarded(1, 300 << 10); // 1 MiB / 300 KiB ⇒ 3 batches
        assert_eq!((w.window(), *rx.borrow()), (3, 4));
        w.forwarded(2, 2 << 20); // larger than the budget: still one at a time
        assert_eq!(w.window(), 1);
        assert_eq!(*rx.borrow(), 4, "a grant never shrinks");
        w.forwarded(5, 10);
        assert_eq!(*rx.borrow(), 6, "the largest batch seen still counts");
    }

    #[test]
    fn the_pump_grants_as_it_forwards() {
        let (_dst_r, dst_w) = pipe();
        let (w, rx) = flow(4, 0);
        // The destination's pipe holds these few tiny batches.
        let out = pump(source_stream(3, true), dst_w, Arc::default(), Some(w), |_, _| {});
        assert_eq!(out.stats.batches, 3);
        assert_eq!(*rx.borrow(), 3 + 4);
        assert_eq!(out.stats.peak_window, 4);
    }

    #[test]
    fn abort_stops_forwarding_but_keeps_draining() {
        let (mut dst_r, dst_w) = pipe();
        let control = Arc::new(PumpControl::default());
        control.abort.store(true, Ordering::Relaxed);
        let out = pump(source_stream(4, true), dst_w, control.clone(), None, |_, _| {});
        assert_eq!(out.stats.batches, 4, "the source is drained");
        assert_eq!(control.batches.load(Ordering::Relaxed), 4);
        drop(out);
        let mut bytes = Vec::new();
        dst_r.read_to_end(&mut bytes).unwrap();
        let mut reader = IpcReader::new(bytes.as_slice()).unwrap();
        assert!(reader.read_batch().unwrap().is_none(), "nothing forwarded after abort");
        assert!(!reader.ended_cleanly());
    }
}
