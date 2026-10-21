use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use loadsmith_protocol::{Finished, Message, ObjectDelivered, PluginKind};
use tokio::io::AsyncWrite;

use crate::context::EventSender;
use crate::telemetry::{self, phase_span};
use tracing::Instrument;

use crate::config::PluginConfig;
use crate::control::{answer_configure, conclude, handshake, recv_configure, recv_start, unexpected, Control, Flow};

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
    async fn configure(&mut self, config: PluginConfig) -> Result<()>;

    /// Called after `start`, before the first object. Open connections, etc.
    async fn prepare(&mut self) -> Result<()>;

    /// Deliver one staged file. Must be idempotent (see trait docs).
    async fn deliver(&mut self, path: PathBuf) -> Result<()>;

    /// Called after all objects are delivered (control-plane EOF).
    async fn finalize(&mut self) -> Result<u64>;

    /// Called when the run will not complete, once `start` was received: the
    /// core sent `cancel` or `error`, or this sink's own `deliver`/`finalize`
    /// failed. Abort promptly and clean up (for example, abort an in-progress
    /// multipart upload). Objects already acknowledged stay delivered: a sink
    /// is not transactional. Checked between objects, never mid-`deliver`.
    async fn cancel(&mut self);
}

/// Entry point for sink plugin binaries. Never returns normally.
///
/// Exit code: `0` after success or a `cancel`, `1` after a failure or an
/// `error` from the core.
pub async fn run_sink<P: SinkPlugin + 'static>(mut plugin: P) -> ! {
    telemetry::install();
    let mut ctl = Control::stdio();
    let mut events = EventSender::fd4();
    let mut started = false;
    let result = drive_sink(&mut plugin, &mut ctl, &mut events, &mut started).await;
    let code = conclude(&mut ctl, result, started, || plugin.cancel()).await;
    events.close().await;
    std::process::exit(code);
}

async fn drive_sink<P: SinkPlugin, W: AsyncWrite + Unpin, E: AsyncWrite + Unpin>(
    plugin: &mut P,
    ctl: &mut Control<W>,
    events: &mut EventSender<E>,
    started: &mut bool,
) -> Result<(), Flow> {
    // ── Handshake + capabilities ─────────────────────────────────────────
    handshake(
        ctl,
        PluginKind::Sink,
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
    recv_start(ctl).await?;
    *started = true;
    let span = phase_span!("start");
    let prepared = plugin.prepare().instrument(span.clone()).await;
    telemetry::outcome(&span, &prepared);
    drop(span);
    prepared.map_err(|e| Flow::fatal("PREPARE_FAILED", e))?;
    ctl.send(Message::Ready).await?;

    // ── Delivery loop (DeliverObject on the control plane) ─────────────────
    // The core sends one DeliverObject per staged file and closes our stdin
    // (EOF) once everything is delivered. Each successful deliver is ack'd with
    // ObjectDelivered so the core's ledger knows what survived a crash. `ping`,
    // `resend`, `cancel` and `error` are handled by `Control`.
    while let Some(msg) = ctl.recv_or_eof().await? {
        match msg {
            Message::DeliverObject(d) => {
                let path = PathBuf::from(&d.path);
                let span = telemetry::object_span("loadsmith.deliver", &d.path);
                if let Err(e) = plugin.deliver(path).instrument(span.clone()).await {
                    telemetry::fail(&span, &e);
                    drop(span);
                    events.error_msg(format!("deliver error for {}: {e:#}", d.path)).await;
                    return Err(Flow::fatal("DELIVER_FAILED", e));
                }
                drop(span);
                events.flush_telemetry().await;
                ctl.send(Message::ObjectDelivered(ObjectDelivered { path: d.path })).await?;
            }
            other => return Err(unexpected("deliver_object", &other)),
        }
    }

    let span = phase_span!("finish");
    let finalized = plugin.finalize().instrument(span.clone()).await;
    telemetry::outcome(&span, &finalized);
    drop(span);
    let count = finalized.map_err(|e| Flow::fatal("FINALIZE_FAILED", e))?;
    events.close().await;
    // Reuse the Finished shape to carry the object count back to the core
    // (rows_written/batches_written both = objects delivered) — no new field.
    ctl.send(Message::Finished(Finished::success_destination(count, count))).await?;
    Ok(())
}
