use std::os::unix::io::FromRawFd;

use anyhow::Result;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use loadsmith_protocol::{
    ConfigureAck, Finished, HandshakeAck, Message, PluginKind, SetProtocolVersion,
};
use loadsmith_transport::{ControlReader, ControlWriter};
use tokio::io::{AsyncWriteExt, BufReader};

use crate::context::EventSender;

const PROTOCOL_VERSION: u32 = 1;

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
    async fn configure(&mut self, config: serde_json::Value) -> Result<()>;

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

    /// Called after all batches are received (fd3 EOF).
    /// Plugin should flush and finalize.
    async fn finalize(&mut self) -> Result<u64>;

    /// Called when core sends `cancel`. Plugin should abort promptly.
    async fn cancel(&mut self);
}

/// Entry point for destination plugin binaries. Never returns normally.
pub async fn run_destination<P: DestinationPlugin + 'static>(mut plugin: P) -> ! {
    if let Err(e) = run_destination_inner(&mut plugin).await {
        eprintln!("[plugin-sdk] fatal error: {e:#}");
        std::process::exit(1);
    }
    std::process::exit(0);
}

async fn run_destination_inner<P: DestinationPlugin>(plugin: &mut P) -> Result<()> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let event_fd = unsafe { std::fs::File::from_raw_fd(4) };
    let event_async = tokio::fs::File::from_std(event_fd);

    let mut reader = ControlReader::new(BufReader::new(stdin));
    let mut writer = ControlWriter::new(stdout);
    let mut events = EventSender::new(event_async);

    // ── Handshake ────────────────────────────────────────────────────────
    let msg = reader.recv().await?.ok_or_else(|| anyhow::anyhow!("EOF before handshake"))?;
    anyhow::ensure!(matches!(msg, Message::Handshake), "expected Handshake");

    writer
        .send(&Message::HandshakeAck(HandshakeAck {
            protocol_supported_versions: vec![PROTOCOL_VERSION],
            plugin_name: plugin.plugin_name().to_string(),
            plugin_version: plugin.plugin_version().to_string(),
            kind: PluginKind::Destination,
        }))
        .await?;

    let msg = reader.recv().await?.ok_or_else(|| anyhow::anyhow!("EOF after handshake_ack"))?;
    match msg {
        Message::SetProtocolVersion(SetProtocolVersion { protocol_version }) => {
            anyhow::ensure!(protocol_version == PROTOCOL_VERSION, "unsupported version");
        }
        other => anyhow::bail!("expected set_protocol_version, got {other:?}"),
    }

    // ── Capabilities ─────────────────────────────────────────────────────
    let msg = reader.recv().await?.ok_or_else(|| anyhow::anyhow!("EOF before capabilities"))?;
    anyhow::ensure!(matches!(msg, Message::CapabilitiesRequest));
    writer
        .send(&Message::CapabilitiesResponse(loadsmith_protocol::CapabilitiesResponse {
            supports: plugin.capabilities(),
        }))
        .await?;

    // ── Configure ────────────────────────────────────────────────────────
    let msg = reader.recv().await?.ok_or_else(|| anyhow::anyhow!("EOF before configure"))?;
    let config = match msg {
        Message::Configure(c) => c.config,
        other => anyhow::bail!("expected configure, got {other:?}"),
    };

    match plugin.configure(config).await {
        Ok(()) => writer.send(&Message::ConfigureAck(ConfigureAck::ok())).await?,
        Err(e) => {
            writer
                .send(&Message::ConfigureAck(ConfigureAck::error("CONFIGURE_FAILED", e.to_string())))
                .await?;
            anyhow::bail!("configure rejected: {e}");
        }
    }

    // ── Start + Prepare ───────────────────────────────────────────────────
    let msg = reader.recv().await?.ok_or_else(|| anyhow::anyhow!("EOF before start"))?;
    anyhow::ensure!(matches!(msg, Message::Start(_)), "expected start");

    plugin.prepare().await?;
    writer.send(&Message::Ready).await?;

    // ── Data loop (Arrow IPC on fd3) ──────────────────────────────────────
    let data_fd = unsafe { std::fs::File::from_raw_fd(3) };
    let mut ipc_reader =
        loadsmith_arrow::IpcReader::new(data_fd).map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut batches_total: u64 = 0;
    let mut committed_seq: u64 = 0;

    loop {
        match ipc_reader.read_batch() {
            Ok(Some(batch)) => {
                batches_total += 1;
                if let Err(e) = plugin.write_batch(batch).await {
                    events.error_msg(format!("write_batch error: {e}")).await;
                    writer
                        .send(&Message::Finished(Finished::error("WRITE_FAILED", e.to_string())))
                        .await?;
                    return Ok(());
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
            Ok(None) => break,
            Err(e) => {
                writer
                    .send(&Message::Finished(Finished::error("IPC_READ_FAILED", e.to_string())))
                    .await?;
                return Ok(());
            }
        }
    }

    let rows_written = plugin.finalize().await?;
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

    writer
        .send(&Message::Finished(Finished::success_destination(rows_written, batches_total)))
        .await?;

    let mut stdout = tokio::io::stdout();
    stdout.flush().await?;
    Ok(())
}
