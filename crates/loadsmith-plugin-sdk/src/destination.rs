use std::os::unix::io::FromRawFd;

use anyhow::Result;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use loadsmith_protocol::{Finished, Message, PluginKind};
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

use crate::context::EventSender;
use crate::config::PluginConfig;
use crate::control::{answer_configure, conclude, handshake, recv_configure, recv_start, Control, Flow};

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
    let mut ctl = Control::stdio();
    let mut started = false;
    let result = drive_destination(&mut plugin, &mut ctl, &mut started).await;
    let code = conclude(&mut ctl, result, started, || plugin.cancel()).await;
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

async fn drive_destination<P: DestinationPlugin, W: AsyncWrite + Unpin>(
    plugin: &mut P,
    ctl: &mut Control<W>,
    started: &mut bool,
) -> Result<(), Flow> {
    let event_fd = unsafe { std::fs::File::from_raw_fd(4) };
    let mut events = EventSender::new(tokio::fs::File::from_std(event_fd));

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
    let outcome = plugin.configure(config).await;
    answer_configure(ctl, &mut events, &warnings, outcome).await?;

    // ── Start + Prepare ───────────────────────────────────────────────────
    recv_start(ctl).await?;
    *started = true;
    plugin.prepare().await.map_err(|e| Flow::fatal("PREPARE_FAILED", e))?;
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
                if let Err(e) = plugin.write_batch(batch).await {
                    events.error_msg(format!("write_batch error: {e:#}")).await;
                    return Err(Flow::fatal("WRITE_FAILED", e));
                }
                // Announce any files finalized by this batch (e.g. a chunk that
                // rolled over) so the sink can start delivering mid-pump.
                for path in plugin.take_ready_objects() {
                    events.object_ready(path.to_string_lossy().into_owned()).await;
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

    let rows_written = plugin.finalize().await.map_err(|e| Flow::fatal("FINALIZE_FAILED", e))?;
    // Announce the final files (single-file mode closes its only file here, and
    // chunked mode closes the last partial chunk). These events must reach fd4
    // before the process exits so the sink delivers every object.
    for path in plugin.take_ready_objects() {
        events.object_ready(path.to_string_lossy().into_owned()).await;
    }
    // finalize() succeeded ⇒ everything is durable. Emit a final Committed
    // covering every batch (the only durability signal for staged/atomic
    // destinations, and the closing one for checkpointed destinations).
    if batches_total > committed_seq {
        events.committed(batches_total).await;
    }
    events.progress_destination(rows_written, batches_total).await;

    ctl.send(Message::Finished(Finished::success_destination(rows_written, batches_total)))
        .await?;
    Ok(())
}
