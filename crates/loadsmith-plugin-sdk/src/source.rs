use std::os::unix::io::FromRawFd;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use loadsmith_protocol::{ConfigureAck, Finished, Message, PluginKind};
use tokio::io::AsyncWrite;

use crate::context::EventSender;
use crate::control::{conclude, handshake, recv_configure, recv_start, Control, Flow};

/// Implemented by source plugins. The SDK drives the full lifecycle;
/// implementors only provide data and metadata.
#[async_trait]
pub trait SourcePlugin: Send {
    fn plugin_name(&self) -> &str;
    fn plugin_version(&self) -> &str;

    /// Capabilities advertised during the handshake. Defaults to
    /// `["batch_read", "schema_inference"]`. An incremental source should also
    /// advertise `incremental_state`.
    fn capabilities(&self) -> Vec<String> {
        vec!["batch_read".into(), "schema_inference".into()]
    }

    /// Called after `start`, before `schema()`/`next_batch()`. Carries the
    /// opaque watermark persisted by the previous run (`None` ⇒ full read). An
    /// incremental source resumes reading after this value; non-incremental
    /// sources leave the default no-op.
    async fn resume_from(&mut self, _cursor_value: Option<serde_json::Value>) {}

    /// Called after `configure`. Plugin validates its config and prepares.
    /// Return `Err` to send `configure_ack { error }` and exit.
    async fn configure(&mut self, config: serde_json::Value) -> Result<()>;

    /// Return the Arrow schema of the data this source will produce.
    async fn schema(&mut self) -> Result<Schema>;

    /// Produce the next batch. Return `Ok(None)` when exhausted.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>>;

    /// The high watermark of the cursor column produced so far, as an opaque
    /// scalar the core will persist. Consulted by the SDK after each batch.
    /// `None` (the default) ⇒ this source has no watermark to report.
    fn current_watermark(&self) -> Option<serde_json::Value> {
        None
    }

    /// Called when the run will not complete, once `start` was received: the
    /// core sent `cancel` or `error`, the core is gone, or this source failed.
    /// Stop producing and release what you hold (cursors, connections).
    ///
    /// The SDK waits for `next_batch` and for the core at the same time, so on
    /// `cancel` an in-flight `next_batch` future may have been **dropped**
    /// before this runs; `cancel` must cope with that (for example, by dropping
    /// a connection that was mid-query). Never called before `start`.
    async fn cancel(&mut self);
}

/// Entry point for source plugin binaries. Never returns normally.
///
/// Exit code: `0` after success or a `cancel`, `1` after a failure, an
/// `error` from the core, or a lost core.
pub async fn run_source<P: SourcePlugin + 'static>(mut plugin: P) -> ! {
    let mut ctl = Control::stdio();
    let mut started = false;
    let result = drive_source(&mut plugin, &mut ctl, &mut started).await;
    let code = conclude(&mut ctl, result, started, || plugin.cancel()).await;
    std::process::exit(code);
}

async fn drive_source<P: SourcePlugin, W: AsyncWrite + Unpin>(
    plugin: &mut P,
    ctl: &mut Control<W>,
    started: &mut bool,
) -> Result<(), Flow> {
    // fd0 = stdin (control in), fd1 = stdout (control out), fd4 = event out
    let event_fd = unsafe { std::fs::File::from_raw_fd(4) };
    let mut events = EventSender::new(tokio::fs::File::from_std(event_fd));

    // ── Handshake + capabilities ─────────────────────────────────────────
    handshake(
        ctl,
        PluginKind::Source,
        plugin.plugin_name(),
        plugin.plugin_version(),
        plugin.capabilities(),
    )
    .await?;

    // ── Configure ────────────────────────────────────────────────────────
    let config = recv_configure(ctl).await?;
    if let Err(e) = plugin.configure(config).await {
        ctl.send(Message::ConfigureAck(ConfigureAck::error("CONFIGURE_FAILED", format!("{e:#}"))))
            .await?;
        return Err(Flow::fatal("CONFIGURE_FAILED", e.context("configure rejected")));
    }
    ctl.send(Message::ConfigureAck(ConfigureAck::ok())).await?;

    // ── Start ─────────────────────────────────────────────────────────────
    let params = recv_start(ctl).await?;
    *started = true;
    plugin.resume_from(params.resume.map(|r| r.cursor_value)).await;

    // ── Schema ────────────────────────────────────────────────────────────
    let schema = plugin.schema().await.map_err(|e| Flow::fatal("SCHEMA_FAILED", e))?;
    // The exact protocol form of the Arrow schema (types, units, time zones,
    // precision, nullability, children). The IPC header written next carries
    // the same schema and is what the core treats as authoritative.
    let protocol_fields = loadsmith_arrow::protocol_fields_from_schema(&schema);
    ctl.send(Message::Schema(loadsmith_protocol::Schema { fields: protocol_fields })).await?;

    // ── Data loop (Arrow IPC on fd3) ──────────────────────────────────────
    let data_fd = unsafe { std::fs::File::from_raw_fd(3) };
    let mut ipc_writer = loadsmith_arrow::IpcWriter::new(data_fd, &schema)
        .map_err(|e| Flow::fatal("IPC_WRITE_FAILED", anyhow::anyhow!("{e}")))?;

    let mut rows_total: u64 = 0;
    let mut batches_total: u64 = 0;

    loop {
        // Wait for the next batch *and* the core at the same time, so a
        // `cancel` stops a source even while it is busy producing (some
        // sources buffer their whole result before the first batch). Only a
        // stopping message drops the in-flight `next_batch` future; `ping` and
        // `resend` are handled without touching it.
        let next = {
            let batch = plugin.next_batch();
            tokio::pin!(batch);
            loop {
                tokio::select! {
                    biased;
                    inbound = ctl.next_inbound() => {
                        match ctl.process(inbound).await {
                            Ok(None) => {}
                            Ok(Some(other)) => {
                                tracing::warn!("ignoring unexpected control message mid-stream: {other:?}");
                            }
                            Err(flow) => break Err(flow),
                        }
                    }
                    result = &mut batch => break Ok(result),
                }
            }
        };
        match next? {
            Ok(Some(batch)) => {
                rows_total += batch.num_rows() as u64;
                batches_total += 1;
                ipc_writer
                    .write_batch(&batch)
                    .map_err(|e| Flow::fatal("IPC_WRITE_FAILED", anyhow::anyhow!("{e}")))?;
                events.progress_source(rows_total, batches_total).await;
                // Report the high watermark for this batch ordinal. The core
                // pairs it with the destination's durable-commit ack before
                // persisting, so emitting per batch is safe (and cheap on fd4).
                if let Some(watermark) = plugin.current_watermark() {
                    events.checkpoint(batches_total, watermark).await;
                }
            }
            Ok(None) => break,
            Err(e) => {
                events.error_msg(format!("batch error: {e:#}")).await;
                return Err(Flow::fatal("BATCH_FAILED", e));
            }
        }
    }

    // The end-of-stream marker tells the core the stream is complete. It is
    // written only after the last batch succeeded; a source that dies before
    // this point leaves a stream the core recognises as truncated.
    ipc_writer
        .finish()
        .map_err(|e| Flow::fatal("IPC_WRITE_FAILED", anyhow::anyhow!("IPC finish: {e}")))?;

    ctl.send(Message::Finished(Finished::success_source(rows_total, batches_total))).await?;
    Ok(())
}
