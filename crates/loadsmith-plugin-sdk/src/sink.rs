use std::os::unix::io::FromRawFd;
use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use loadsmith_protocol::{
    ConfigureAck, Finished, HandshakeAck, Message, ObjectDelivered, PluginKind, SetProtocolVersion,
};
use loadsmith_transport::{ControlReader, ControlWriter};
use tokio::io::{AsyncWriteExt, BufReader};

use crate::context::EventSender;

const PROTOCOL_VERSION: u32 = 1;

/// Implemented by sink plugins.
///
/// A sink is a *delivery* stage, not a data-plane participant: it never reads
/// Arrow batches (no fd3). The core hands it the local path of each staged file
/// (one `DeliverObject` per object) and the sink ships it to its remote target.
///
/// **`deliver` must be idempotent.** The core owns the delivery ledger; if a
/// sink crashes mid-run the core respawns it and re-sends every object it had
/// not yet acknowledged, so the same path may arrive more than once (possibly
/// after a partial delivery). Re-delivering must converge to the same result.
#[async_trait]
pub trait SinkPlugin: Send {
    fn plugin_name(&self) -> &str;
    fn plugin_version(&self) -> &str;

    /// Capabilities advertised during the handshake.
    fn capabilities(&self) -> Vec<String> {
        vec!["object_delivery".into()]
    }

    /// Called after `configure`. Plugin validates config and prepares.
    async fn configure(&mut self, config: serde_json::Value) -> Result<()>;

    /// Called after `start`, before the first object. Open connections, etc.
    async fn prepare(&mut self) -> Result<()>;

    /// Deliver one staged file. Must be idempotent (see trait docs).
    async fn deliver(&mut self, path: PathBuf) -> Result<()>;

    /// Called after all objects are delivered (control-plane EOF).
    async fn finalize(&mut self) -> Result<u64>;

    /// Called when core sends `cancel`. Plugin should abort promptly.
    async fn cancel(&mut self);
}

/// Entry point for sink plugin binaries. Never returns normally.
pub async fn run_sink<P: SinkPlugin + 'static>(mut plugin: P) -> ! {
    if let Err(e) = run_sink_inner(&mut plugin).await {
        eprintln!("[plugin-sdk] fatal error: {e:#}");
        std::process::exit(1);
    }
    std::process::exit(0);
}

async fn run_sink_inner<P: SinkPlugin>(plugin: &mut P) -> Result<()> {
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
            kind: PluginKind::Sink,
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

    // ── Delivery loop (DeliverObject on the control plane) ─────────────────
    // The core sends one DeliverObject per staged file and closes our stdin
    // (EOF) once everything is delivered. Each successful deliver is ack'd with
    // ObjectDelivered so the core's ledger knows what survived a crash.
    let mut delivered_total: u64 = 0;

    loop {
        let msg = match reader.recv().await? {
            Some(m) => m,
            None => break, // EOF — no more objects.
        };
        match msg {
            Message::DeliverObject(d) => {
                let path = PathBuf::from(&d.path);
                if let Err(e) = plugin.deliver(path).await {
                    events.error_msg(format!("deliver error for {}: {e}", d.path)).await;
                    writer
                        .send(&Message::Finished(Finished::error("DELIVER_FAILED", e.to_string())))
                        .await?;
                    return Ok(());
                }
                delivered_total += 1;
                writer.send(&Message::ObjectDelivered(ObjectDelivered { path: d.path })).await?;
            }
            Message::Ping => {
                writer.send(&Message::Pong).await?;
            }
            Message::Cancel(_) => {
                plugin.cancel().await;
                writer.send(&Message::Finished(Finished::cancelled())).await?;
                let mut stdout = tokio::io::stdout();
                stdout.flush().await?;
                return Ok(());
            }
            other => {
                anyhow::bail!("unexpected message in delivery loop: {other:?}");
            }
        }
    }

    let count = plugin.finalize().await?;
    let _ = delivered_total; // ack count is a per-instance sanity check only
    // Reuse the Finished shape to carry the object count back to the core
    // (rows_written/batches_written both = objects delivered) — no new field.
    writer.send(&Message::Finished(Finished::success_destination(count, count))).await?;

    let mut stdout = tokio::io::stdout();
    stdout.flush().await?;
    Ok(())
}
