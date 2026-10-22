use std::collections::VecDeque;
use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use loadsmith_protocol::{Finished, Message, ObjectDelivered, PluginKind};
use tokio::io::AsyncWrite;

use crate::context::EventSender;
use crate::telemetry::{self, phase_span};
use tracing::Instrument;

use crate::config::PluginConfig;
use crate::control::{
    answer_configure, conclude, handshake, recv_configure, recv_start, unexpected, Control, Flow, Inbound,
};

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
    // `resend`, `cancel` and `error` are handled by `Control` — also while an
    // object is being delivered: the core pings a sink every few seconds and
    // takes a missing pong for a hang, and an upload over a slow link easily
    // outlasts that. Objects the core hands over meanwhile queue behind it.
    let mut queue: VecDeque<String> = VecDeque::new();
    let mut input_done = false;
    loop {
        let Some(path) = queue.pop_front() else {
            if input_done {
                break;
            }
            match ctl.next_inbound().await {
                Inbound::Eof => input_done = true,
                inbound => match ctl.process(inbound).await? {
                    None => {}
                    Some(Message::DeliverObject(d)) => queue.push_back(d.path),
                    Some(other) => return Err(unexpected("deliver_object", &other)),
                },
            }
            continue;
        };

        let span = telemetry::object_span("loadsmith.deliver", &path);
        let outcome = {
            let work = plugin.deliver(PathBuf::from(&path)).instrument(span.clone());
            tokio::pin!(work);
            loop {
                tokio::select! {
                    biased;
                    inbound = ctl.next_inbound(), if !input_done => match inbound {
                        Inbound::Eof => input_done = true,
                        inbound => match ctl.process(inbound).await {
                            Ok(None) => {}
                            Ok(Some(Message::DeliverObject(d))) => queue.push_back(d.path),
                            Ok(Some(other)) => break Err(unexpected("deliver_object", &other)),
                            Err(flow) => break Err(flow),
                        },
                    },
                    result = &mut work => break Ok(result),
                }
            }
        }?;
        if let Err(e) = outcome {
            telemetry::fail(&span, &e);
            drop(span);
            events.error_msg(format!("deliver error for {path}: {e:#}")).await;
            return Err(Flow::fatal("DELIVER_FAILED", e));
        }
        drop(span);
        events.flush_telemetry().await;
        ctl.send(Message::ObjectDelivered(ObjectDelivered { path })).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};
    use tokio::sync::Notify;

    /// Delivers by recording the path; `slow*` paths wait for `release`
    /// (an upload over a slow link), `boom` fails.
    #[derive(Default)]
    struct Fake {
        delivered: Arc<Mutex<Vec<String>>>,
        release: Arc<Notify>,
        cancelled: bool,
    }

    #[async_trait]
    impl SinkPlugin for Fake {
        fn plugin_name(&self) -> &str {
            "loadsmith-sink-fake"
        }
        fn plugin_version(&self) -> &str {
            "0.0.0"
        }
        async fn configure(&mut self, _config: PluginConfig) -> Result<()> {
            Ok(())
        }
        async fn prepare(&mut self) -> Result<()> {
            Ok(())
        }
        async fn deliver(&mut self, path: PathBuf) -> Result<()> {
            let path = path.to_string_lossy().into_owned();
            if path.starts_with("slow") {
                self.release.notified().await;
            }
            anyhow::ensure!(path != "boom", "upload failed");
            self.delivered.lock().unwrap().push(path);
            Ok(())
        }
        async fn finalize(&mut self) -> Result<u64> {
            Ok(self.delivered.lock().unwrap().len() as u64)
        }
        async fn cancel(&mut self) {
            self.cancelled = true;
        }
    }

    struct Core {
        to_plugin: DuplexStream,
        from_plugin: BufReader<DuplexStream>,
    }

    impl Core {
        async fn send(&mut self, line: &str) {
            self.to_plugin.write_all(format!("{line}\n").as_bytes()).await.unwrap();
        }
        async fn deliver(&mut self, path: &str) {
            self.send(&format!(r#"{{"type":"deliver_object","path":"{path}"}}"#)).await;
        }
        async fn close(&mut self) {
            self.to_plugin.shutdown().await.unwrap();
        }
        async fn recv(&mut self) -> String {
            let mut s = String::new();
            tokio::time::timeout(Duration::from_secs(5), self.from_plugin.read_line(&mut s))
                .await
                .expect("the sink answers")
                .unwrap();
            s.trim_end().to_string()
        }
        /// Up to `ready`.
        async fn start(&mut self) {
            self.send(r#"{"type":"handshake"}"#).await;
            assert!(self.recv().await.contains(r#""kind":"sink""#));
            self.send(r#"{"type":"set_protocol_version","protocol_version":1}"#).await;
            self.send(r#"{"type":"capabilities_request"}"#).await;
            assert!(self.recv().await.starts_with(r#"{"type":"capabilities_response""#));
            self.send(r#"{"type":"configure","config":{}}"#).await;
            assert_eq!(self.recv().await, r#"{"type":"configure_ack","status":"ok"}"#);
            self.send(r#"{"type":"start"}"#).await;
            assert_eq!(self.recv().await, r#"{"type":"ready"}"#);
        }
    }

    /// Runs the sink on in-memory pipes; returns the core's ends, the fake's
    /// `release` handle and the task yielding (lifecycle ok, exit code,
    /// `cancel` called, delivered paths).
    #[allow(clippy::type_complexity)]
    fn spawn() -> (Core, Arc<Notify>, tokio::task::JoinHandle<(bool, i32, bool, Vec<String>)>) {
        let (core_in, plugin_in) = tokio::io::duplex(1 << 16);
        let (plugin_out, core_out) = tokio::io::duplex(1 << 16);
        let mut plugin = Fake::default();
        let release = plugin.release.clone();
        let task = tokio::spawn(async move {
            let mut ctl = Control::new(BufReader::new(plugin_in), plugin_out);
            let mut events = EventSender::new(tokio::io::sink());
            let mut started = false;
            let result = drive_sink(&mut plugin, &mut ctl, &mut events, &mut started).await;
            let ok = result.is_ok();
            let mut cancelled = false;
            let code = conclude(&mut ctl, result, started, || {
                cancelled = true;
                async {}
            })
            .await;
            let delivered = plugin.delivered.lock().unwrap().clone();
            (ok, code, cancelled || plugin.cancelled, delivered)
        });
        (Core { to_plugin: core_in, from_plugin: BufReader::new(core_out) }, release, task)
    }

    #[tokio::test]
    async fn delivers_each_object_then_finishes_on_eof() {
        let (mut core, _release, task) = spawn();
        core.start().await;
        core.deliver("a.parquet").await;
        assert_eq!(core.recv().await, r#"{"type":"object_delivered","path":"a.parquet"}"#);
        core.deliver("b.parquet").await;
        assert_eq!(core.recv().await, r#"{"type":"object_delivered","path":"b.parquet"}"#);
        core.close().await;
        assert!(core.recv().await.starts_with(r#"{"type":"finished","status":"success""#));
        let (ok, code, cancelled, delivered) = task.await.unwrap();
        assert_eq!((ok, code, cancelled), (true, 0, false));
        assert_eq!(delivered, ["a.parquet", "b.parquet"]);
    }

    #[tokio::test]
    async fn answers_pings_while_a_slow_delivery_runs_and_queues_new_objects() {
        let (mut core, release, task) = spawn();
        core.start().await;
        core.deliver("slow.parquet").await;
        core.deliver("next.parquet").await;
        // Mid-upload: the core's liveness check must still get its pong.
        core.send(r#"{"type":"ping"}"#).await;
        assert_eq!(core.recv().await, r#"{"type":"pong"}"#);
        core.send(r#"{"type":"ping"}"#).await;
        assert_eq!(core.recv().await, r#"{"type":"pong"}"#);
        release.notify_one();
        assert_eq!(core.recv().await, r#"{"type":"object_delivered","path":"slow.parquet"}"#);
        assert_eq!(core.recv().await, r#"{"type":"object_delivered","path":"next.parquet"}"#);
        core.close().await;
        assert!(core.recv().await.starts_with(r#"{"type":"finished","status":"success""#));
        let (ok, _, _, delivered) = task.await.unwrap();
        assert!(ok);
        assert_eq!(delivered, ["slow.parquet", "next.parquet"]);
    }

    #[tokio::test]
    async fn cancel_interrupts_a_slow_delivery() {
        let (mut core, _release, task) = spawn();
        core.start().await;
        core.deliver("slow.parquet").await;
        core.send(r#"{"type":"cancel","reason":"destination failed"}"#).await;
        assert_eq!(core.recv().await, r#"{"type":"finished","status":"cancelled"}"#);
        let (ok, code, cancelled, delivered) = tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap();
        assert_eq!((ok, code, cancelled), (false, 0, true));
        assert!(delivered.is_empty());
    }

    #[tokio::test]
    async fn a_failed_delivery_fails_the_sink() {
        let (mut core, _release, task) = spawn();
        core.start().await;
        core.deliver("boom").await;
        let finished = core.recv().await;
        assert!(finished.contains(r#""status":"error","code":"DELIVER_FAILED""#), "{finished}");
        assert!(finished.contains("upload failed"), "{finished}");
        let (ok, code, cancelled, _) = task.await.unwrap();
        assert_eq!((ok, code, cancelled), (false, 1, true));
    }
}
