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

use std::fs::File;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use loadsmith_arrow::{IpcReader, IpcWriter};

/// Totals observed by the pump as data flowed through it.
#[derive(Debug, Default, Clone, Copy)]
pub struct PumpStats {
    pub rows: u64,
    pub batches: u64,
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
    /// The source never produced a stream, so nothing was written yet.
    Unopened(File),
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
            PumpOutput::Unopened(_) => anyhow::bail!("the destination stream was never opened"),
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

/// Copies the Arrow IPC stream from `read_in` (source) to `write_out`
/// (destination) until the source's stream ends, invoking
/// `on_progress(rows, batches)` at exponentially growing batch intervals
/// (1, 2, 4, 8, …) so logs never flood.
///
/// A failed write to the destination (or `control.abort`) does not stop the
/// pump: it keeps draining the source, so a source never blocks on a full
/// pipe while the run is being aborted.
///
/// This is synchronous, blocking I/O — intended to run on a blocking thread
/// (e.g. `tokio::task::spawn_blocking`).
pub fn pump<F>(read_in: File, write_out: File, control: Arc<PumpControl>, mut on_progress: F) -> PumpOutcome
where
    F: FnMut(u64, u64),
{
    let mut stats = PumpStats::default();
    let mut reader = match IpcReader::new(read_in) {
        Ok(r) => r,
        Err(e) => {
            return PumpOutcome {
                stats,
                source_clean: false,
                read_error: Some(format!("data plane reader: {e}")),
                write_error: None,
                output: PumpOutput::Unopened(write_out),
            }
        }
    };
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
    loop {
        match reader.read_batch() {
            Ok(Some(batch)) => {
                stats.rows += batch.num_rows() as u64;
                stats.batches += 1;
                control.batches.store(stats.batches, Ordering::Relaxed);
                if !control.abort.load(Ordering::Relaxed) {
                    if let Some(w) = writer.as_mut() {
                        if let Err(e) = w.write_batch(&batch) {
                            write_error = Some(format!("write batch: {e}"));
                            writer = None;
                        }
                    }
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
    /// returns the read end.
    fn source_stream(n: usize, finish: bool) -> File {
        let (r, w) = pipe();
        let (schema, b) = batch();
        let mut writer = IpcWriter::new(w, &schema).unwrap();
        for _ in 0..n {
            writer.write_batch(&b).unwrap();
        }
        if finish {
            writer.finish().unwrap();
        }
        r
    }

    #[test]
    fn a_clean_source_stream_leaves_the_destination_stream_open() {
        let (dst_r, dst_w) = pipe();
        let out = pump(source_stream(3, true), dst_w, Arc::default(), |_, _| {});
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
        let out = pump(source_stream(2, false), dst_w, Arc::default(), |_, _| {});
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

    #[test]
    fn abort_stops_forwarding_but_keeps_draining() {
        let (mut dst_r, dst_w) = pipe();
        let control = Arc::new(PumpControl::default());
        control.abort.store(true, Ordering::Relaxed);
        let out = pump(source_stream(4, true), dst_w, control.clone(), |_, _| {});
        assert_eq!(out.stats.batches, 4, "the source is drained");
        assert_eq!(control.batches.load(Ordering::Relaxed), 4);
        drop(out);
        let mut bytes = Vec::new();
        dst_r.read_to_end(&mut bytes).unwrap();
        let mut reader = IpcReader::new(bytes.as_slice()).unwrap();
        assert!(reader.read_batch().unwrap().is_none(), "nothing forwarded after abort");
        assert!(!reader.ended_cleanly());
    }

    #[test]
    fn a_source_that_never_wrote_leaves_the_output_unopened() {
        let (r, w) = pipe();
        drop(w);
        let (_dst_r, dst_w) = pipe();
        let out = pump(r, dst_w, Arc::default(), |_, _| {});
        assert!(!out.source_clean);
        assert!(out.read_error.is_some());
        assert!(matches!(out.output, PumpOutput::Unopened(_)));
    }
}
