use std::os::unix::io::FromRawFd;

use anyhow::Result;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use loadsmith_protocol::{Finished, Message, PluginKind};
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

use crate::context::EventSender;
use crate::telemetry::{self, phase_span};
use std::time::Instant;
use tracing::Instrument;

use crate::config::PluginConfig;
use crate::control::{answer_configure, conclude, handshake, recv_configure, recv_start, Control, Flow};

/// A finalized file a sink can deliver, and the batches it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyObject {
    pub path: std::path::PathBuf,
    /// The first and last batch ordinal in the file (inclusive), when known.
    pub batches: Option<loadsmith_protocol::BatchRange>,
}

/// Implemented by destination plugins.
#[async_trait]
pub trait DestinationPlugin: Send {
    fn plugin_name(&self) -> &str;
    fn plugin_version(&self) -> &str;

    /// Capabilities advertised during the handshake. Defaults to `batch_write`.
    /// File-output destinations that stage objects for a sink should also
    /// advertise `object_output` so the core lets a sink be attached.
    fn capabilities(&self) -> Vec<String> {
        vec!["batch_write".into()]
    }

    /// Returns (and clears) the local paths of files finalized since the last
    /// call — drained by the SDK after each `write_batch` and after `finalize`,
    /// and announced to the core as `ObjectReady` events on fd4. Destinations
    /// with no file output (databases, `null`) leave the default empty impl.
    fn take_ready_objects(&mut self) -> Vec<std::path::PathBuf> {
        Vec::new()
    }

    /// Like [`take_ready_objects`](Self::take_ready_objects), with the batches
    /// each file holds. A destination that knows them overrides this one
    /// instead: the core then counts a file's batches durable as soon as the
    /// sink delivered it, so the incremental watermark advances file by file
    /// rather than only at the end of the run. The batch ordinals are the
    /// 1-based count of `write_batch` calls. The default wraps
    /// `take_ready_objects` without ranges.
    fn take_ready(&mut self) -> Vec<ReadyObject> {
        self.take_ready_objects().into_iter().map(|path| ReadyObject { path, batches: None }).collect()
    }

    /// Called after `start`, before `prepare`, when the core retries a run
    /// whose earlier attempt already delivered files: number the files staged
    /// from now on starting at `seq`, so no file takes the name of one already
    /// delivered under the same run. Destinations that stage no numbered
    /// files leave the default no-op.
    fn first_object_seq(&mut self, _seq: u64) {}

    /// Called after `configure`. Plugin validates config and prepares.
    async fn configure(&mut self, config: PluginConfig) -> Result<()>;

    /// Called after `start`, before the first batch arrives.
    /// Plugin opens files, connections, etc. SDK sends `ready` after this.
    async fn prepare(&mut self) -> Result<()>;

    /// Called for each Arrow batch read from fd3.
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;

    /// The highest batch ordinal (1-based, in fd3 arrival order — the same
    /// global ordinal the source and pump count) that is now **durably**
    /// committed. Consulted by the SDK after each `write_batch`; when it
    /// advances, the SDK emits a `Committed` so the core may persist the
    /// matching watermark. A destination that only becomes durable at
    /// `finalize` (e.g. staged + atomic swap) leaves the default `None` — the
    /// SDK emits a single final `Committed` covering every batch.
    fn durable_through(&mut self) -> Option<u64> {
        None
    }

    /// Called once the input is **complete**: the core ended fd3 with the
    /// end-of-stream marker, which it does only after the source finished
    /// successfully. Flush and commit here. Never called after `cancel`.
    async fn finalize(&mut self) -> Result<u64>;

    /// Called when the run will not complete, once `start` was received: the
    /// core sent `cancel` or `error`, the core is gone (fd0 closed, or fd3
    /// ended without the end-of-stream marker), or this destination's own
    /// `write_batch`/`finalize` failed. **Undo the partial load**: roll back
    /// the open transaction, drop the staging table, delete partial output
    /// files. Everything already reported durable through `durable_through`
    /// may stay. `finalize` is never called after this.
    ///
    /// The SDK checks for `cancel` between batches (never in the middle of a
    /// `write_batch` or `finalize` call), so this runs with no write in flight.
    async fn cancel(&mut self);
}

/// Entry point for destination plugin binaries. Never returns normally.
///
/// Exit code: `0` after success or a `cancel`, `1` after a failure, an
/// `error` from the core, or a lost core.
pub async fn run_destination<P: DestinationPlugin + 'static>(mut plugin: P) -> ! {
    telemetry::install();
    let mut ctl = Control::stdio();
    let mut events = EventSender::fd4();
    let mut started = false;
    let result = drive_destination(&mut plugin, &mut ctl, &mut events, &mut started).await;
    let code = conclude(&mut ctl, result, started, || plugin.cancel()).await;
    events.close().await;
    std::process::exit(code);
}

/// What the fd3 reader thread hands to the lifecycle.
enum DataEvent {
    Batch(RecordBatch),
    /// The stream ended; `clean` ⇔ it ended with the end-of-stream marker.
    End { clean: bool },
    Failed(String),
}

/// Reads fd3 on a plain thread (Arrow IPC is blocking I/O) and forwards each
/// batch, so the lifecycle can wait for data and for the core together. The
/// channel holds one batch: the thread blocks (and so does the pipe) until the
/// lifecycle takes it, preserving backpressure.
fn spawn_fd3_reader() -> mpsc::Receiver<DataEvent> {
    let (tx, rx) = mpsc::channel(1);
    std::thread::spawn(move || {
        let data_fd = unsafe { std::fs::File::from_raw_fd(3) };
        let mut reader = match loadsmith_arrow::IpcReader::new(data_fd) {
            Ok(r) => r,
            Err(e) => {
                let _ = tx.blocking_send(DataEvent::Failed(e.to_string()));
                return;
            }
        };
        loop {
            let event = match reader.read_batch() {
                Ok(Some(batch)) => DataEvent::Batch(batch),
                Ok(None) => DataEvent::End { clean: reader.ended_cleanly() },
                Err(e) => DataEvent::Failed(e.to_string()),
            };
            let last = !matches!(event, DataEvent::Batch(_));
            if tx.blocking_send(event).is_err() || last {
                return;
            }
        }
    });
    rx
}

async fn drive_destination<P: DestinationPlugin, W: AsyncWrite + Unpin, E: AsyncWrite + Unpin>(
    plugin: &mut P,
    ctl: &mut Control<W>,
    events: &mut EventSender<E>,
    started: &mut bool,
) -> Result<(), Flow> {
    // ── Handshake + capabilities ─────────────────────────────────────────
    handshake(
        ctl,
        PluginKind::Destination,
        plugin.plugin_name(),
        plugin.plugin_version(),
        plugin.capabilities(),
    )
    .await?;

    // ── Configure ────────────────────────────────────────────────────────
    let (config, warnings) = recv_configure(ctl).await?;
    let span = phase_span!("configure");
    let outcome = plugin.configure(config).instrument(span.clone()).await;
    telemetry::outcome(&span, &outcome);
    drop(span);
    answer_configure(ctl, events, &warnings, outcome).await?;

    // ── Start + Prepare ───────────────────────────────────────────────────
    let params = recv_start(ctl).await?;
    *started = true;
    if let Some(seq) = params.first_object_seq {
        plugin.first_object_seq(seq);
    }
    let span = phase_span!("start");
    let prepared = plugin.prepare().instrument(span.clone()).await;
    telemetry::outcome(&span, &prepared);
    drop(span);
    prepared.map_err(|e| Flow::fatal("PREPARE_FAILED", e))?;
    ctl.send(Message::Ready).await?;

    // ── Data loop (Arrow IPC on fd3) ──────────────────────────────────────
    // fd0 is watched throughout: the core sends `cancel` when the run aborts,
    // and it never ends fd3 cleanly after an upstream failure, so a
    // destination blocked waiting for data still learns it must roll back.
    let mut data = spawn_fd3_reader();
    let mut batches_total: u64 = 0;
    let mut committed_seq: u64 = 0;

    loop {
        let event = tokio::select! {
            biased;
            inbound = ctl.next_inbound() => {
                match ctl.process(inbound).await? {
                    None => continue,
                    Some(other) => {
                        tracing::warn!("ignoring unexpected control message mid-stream: {other:?}");
                        continue;
                    }
                }
            }
            event = data.recv() => event.unwrap_or(DataEvent::Failed("fd3 reader stopped".into())),
        };
        match event {
            DataEvent::Batch(batch) => {
                batches_total += 1;
                let (rows, bytes) = (batch.num_rows() as u64, telemetry::batch_bytes(&batch));
                let span = telemetry::batch_span(batches_total, "written");
                let began = Instant::now();
                if let Err(e) = plugin.write_batch(batch).instrument(span.clone()).await {
                    telemetry::fail(&span, &e);
                    drop(span);
                    events.error_msg(format!("write_batch error: {e:#}")).await;
                    return Err(Flow::fatal("WRITE_FAILED", e));
                }
                telemetry::batch_done(&span, "written", rows, bytes, began.elapsed());
                drop(span);
                // Announce any files finalized by this batch (e.g. a chunk that
                // rolled over) so the sink can start delivering mid-pump.
                for object in plugin.take_ready() {
                    events.object_ready_with(object.path.to_string_lossy().into_owned(), object.batches).await;
                }
                // Tell the core how far durability has advanced, so it may
                // persist the matching watermark mid-run.
                if let Some(seq) = plugin.durable_through() {
                    if seq > committed_seq {
                        committed_seq = seq;
                        events.committed(seq).await;
                    }
                }
            }
            DataEvent::End { clean: true } => break,
            DataEvent::End { clean: false } => {
                // The core died (or abandoned the run) without ending the
                // stream: the input is incomplete and must not be committed.
                return Err(Flow::fatal(
                    "INPUT_TRUNCATED",
                    anyhow::anyhow!("the data stream ended without an end-of-stream marker"),
                ));
            }
            DataEvent::Failed(e) => {
                return Err(Flow::fatal("IPC_READ_FAILED", anyhow::anyhow!("{e}")));
            }
        }
    }

    let span = phase_span!("finish");
    let finalized = plugin.finalize().instrument(span.clone()).await;
    telemetry::outcome(&span, &finalized);
    drop(span);
    let rows_written = finalized.map_err(|e| Flow::fatal("FINALIZE_FAILED", e))?;
    // Announce the final files (single-file mode closes its only file here, and
    // chunked mode closes the last partial chunk). These events must reach fd4
    // before the process exits so the sink delivers every object.
    for object in plugin.take_ready() {
        events.object_ready_with(object.path.to_string_lossy().into_owned(), object.batches).await;
    }
    // finalize() succeeded ⇒ everything is durable. Emit a final Committed
    // covering every batch (the only durability signal for staged/atomic
    // destinations, and the closing one for checkpointed destinations).
    if batches_total > committed_seq {
        events.committed(batches_total).await;
    }
    events.progress_destination(rows_written, batches_total).await;
    events.close().await;

    ctl.send(Message::Finished(Finished::success_destination(rows_written, batches_total)))
        .await?;
    Ok(())
}
