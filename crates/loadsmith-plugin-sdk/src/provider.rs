//! The provider runtime: resolves the references of a pipeline's
//! `secret('<instance>', '<ref>')` template calls.
//!
//! Lifecycle (driven by the core, before any data plugin starts): handshake →
//! capabilities (`resolve`) → `configure` → `start` → `ready`, then one
//! `resolve{id, ref}` per reference, each answered with `resolved{id, value}`
//! or `resolved{id, error}`. The core closes stdin when it has everything;
//! the provider then sends `finished` and exits. `cancel`/`error` from the
//! core stop it at any point, also in the middle of a `resolve`.
//!
//! A provider answers each reference independently: a reference it cannot
//! resolve is a `resolved` error, not the end of the conversation. The SDK
//! never logs a resolved value.

use std::collections::VecDeque;
use std::os::unix::io::FromRawFd;

use anyhow::Result;
use async_trait::async_trait;
use loadsmith_protocol::{capabilities, ConfigureAck, Finished, Message, PluginKind, Resolve, Resolved};
use tokio::io::AsyncWrite;

use crate::context::EventSender;
use crate::control::{conclude, handshake, recv_configure, recv_start, unexpected, Control, Flow, Inbound};

/// The default `resolved.error.code` when a `resolve` fails without a
/// [`ResolveFailure`].
pub const RESOLVE_FAILED: &str = "RESOLVE_FAILED";

/// A `resolve` failure with a specific code. Return it (as the
/// `anyhow::Error`) from [`ProviderPlugin::resolve`] to choose the
/// `resolved.error.code`; any other error is sent as [`RESOLVE_FAILED`].
/// Suggested codes: `NOT_FOUND`, `ACCESS_DENIED`, `INVALID_REF`.
#[derive(Debug, Clone)]
pub struct ResolveFailure {
    pub code: String,
    pub message: String,
}

impl ResolveFailure {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self { code: code.into(), message: message.into() }
    }
}

impl std::fmt::Display for ResolveFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ResolveFailure {}

/// Implemented by provider plugins. The SDK drives the whole conversation;
/// implementors validate their config and resolve one reference at a time.
#[async_trait]
pub trait ProviderPlugin: Send {
    fn plugin_name(&self) -> &str;
    fn plugin_version(&self) -> &str;

    /// Capabilities advertised during the handshake. Defaults to `resolve`,
    /// which the core requires.
    fn capabilities(&self) -> Vec<String> {
        vec![capabilities::RESOLVE.into()]
    }

    /// Called after `configure`. Validate the config (it may use `env()` and
    /// `file()`, never `secret()`) and prepare: open a client, check a base
    /// directory. Return `Err` to send `configure_ack { error }` and exit.
    async fn configure(&mut self, config: serde_json::Value) -> Result<()>;

    /// Resolves one reference (the second argument of a `secret()` call;
    /// its syntax is yours to define, e.g. `db.json#password`). Return the
    /// value, or an error — a [`ResolveFailure`] to pick the code. Never put
    /// a value in an error message.
    ///
    /// The SDK waits for this and for the core at the same time: on `cancel`
    /// the in-flight future is dropped. The core sends each reference once
    /// per run; caching across references is up to you.
    async fn resolve(&mut self, reference: &str) -> Result<String>;

    /// Called when the run is aborted after `start` (the core sent `cancel`
    /// or `error`, or is gone). Release what you hold. Default: nothing.
    async fn cancel(&mut self) {}
}

/// Entry point for provider plugin binaries. Never returns normally.
///
/// Exit code: `0` after success or a `cancel`, `1` after a failure, an
/// `error` from the core, or a lost core.
pub async fn run_provider<P: ProviderPlugin + 'static>(mut plugin: P) -> ! {
    let mut ctl = Control::stdio();
    let mut started = false;
    // fd4 = event out (logs). The core always wires and drains it.
    let event_fd = unsafe { std::fs::File::from_raw_fd(4) };
    let mut events = EventSender::new(tokio::fs::File::from_std(event_fd));
    let result = drive_provider(&mut plugin, &mut ctl, &mut events, &mut started).await;
    let code = conclude(&mut ctl, result, started, || plugin.cancel()).await;
    std::process::exit(code);
}

pub(crate) async fn drive_provider<P, W, E>(
    plugin: &mut P,
    ctl: &mut Control<W>,
    events: &mut EventSender<E>,
    started: &mut bool,
) -> Result<(), Flow>
where
    P: ProviderPlugin,
    W: AsyncWrite + Unpin,
    E: AsyncWrite + Unpin,
{
    // ── Handshake + capabilities ─────────────────────────────────────────
    handshake(ctl, PluginKind::Provider, plugin.plugin_name(), plugin.plugin_version(), plugin.capabilities())
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
    recv_start(ctl).await?;
    *started = true;
    ctl.send(Message::Ready).await?;

    // ── Resolve loop ──────────────────────────────────────────────────────
    // One `resolve` at a time; any that arrive while one is in flight are
    // queued. EOF on stdin means "nothing more to resolve": finish what is
    // queued, then `finished`.
    let mut queue: VecDeque<Resolve> = VecDeque::new();
    let mut input_done = false;
    let (mut resolved, mut failed) = (0u64, 0u64);
    loop {
        let Some(request) = queue.pop_front() else {
            if input_done {
                break;
            }
            match ctl.next_inbound().await {
                Inbound::Eof => input_done = true,
                inbound => match ctl.process(inbound).await? {
                    None => {}
                    Some(Message::Resolve(r)) => queue.push_back(r),
                    Some(other) => return Err(unexpected("resolve", &other)),
                },
            }
            continue;
        };

        let outcome = {
            let work = plugin.resolve(&request.reference);
            tokio::pin!(work);
            loop {
                tokio::select! {
                    biased;
                    inbound = ctl.next_inbound(), if !input_done => match inbound {
                        Inbound::Eof => input_done = true,
                        inbound => match ctl.process(inbound).await {
                            Ok(None) => {}
                            Ok(Some(Message::Resolve(r))) => queue.push_back(r),
                            Ok(Some(other)) => break Err(unexpected("resolve", &other)),
                            Err(flow) => break Err(flow),
                        },
                    },
                    result = &mut work => break Ok(result),
                }
            }
        }?;

        let reply = match outcome {
            Ok(value) => {
                resolved += 1;
                Resolved::value(request.id, value)
            }
            Err(e) => {
                failed += 1;
                let (code, message) = match e.downcast_ref::<ResolveFailure>() {
                    Some(f) => (f.code.clone(), f.message.clone()),
                    None => (RESOLVE_FAILED.to_string(), format!("{e:#}")),
                };
                events.warn(format!("could not resolve '{}' ({code})", request.reference)).await;
                Resolved::error(request.id, code, message)
            }
        };
        ctl.send(Message::Resolved(reply)).await?;
    }

    events.info(format!("resolved {resolved} reference(s), {failed} failed")).await;
    ctl.send(Message::Finished(Finished::success())).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};

    /// Resolves `k` → `v-of-k`; `missing*` fail with NOT_FOUND, `boom` with a
    /// plain error, `slow` never returns.
    #[derive(Default)]
    struct Fake {
        cancelled: bool,
    }

    #[async_trait]
    impl ProviderPlugin for Fake {
        fn plugin_name(&self) -> &str {
            "loadsmith-provider-fake"
        }
        fn plugin_version(&self) -> &str {
            "0.0.0"
        }
        async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
            anyhow::ensure!(config.get("bad").is_none(), "bad config");
            Ok(())
        }
        async fn resolve(&mut self, reference: &str) -> Result<String> {
            match reference {
                r if r.starts_with("missing") => Err(ResolveFailure::new("NOT_FOUND", format!("no {r}")).into()),
                "boom" => anyhow::bail!("exploded"),
                "slow" => {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    unreachable!()
                }
                r => Ok(format!("v-of-{r}")),
            }
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
        async fn close(&mut self) {
            self.to_plugin.shutdown().await.unwrap();
        }
        async fn recv(&mut self) -> String {
            let mut s = String::new();
            self.from_plugin.read_line(&mut s).await.unwrap();
            s.trim_end().to_string()
        }
        /// Up to `ready`.
        async fn start(&mut self, config: &str) {
            self.send(r#"{"type":"handshake"}"#).await;
            assert!(self.recv().await.contains(r#""kind":"provider""#));
            self.send(r#"{"type":"set_protocol_version","protocol_version":1}"#).await;
            self.send(r#"{"type":"capabilities_request"}"#).await;
            assert_eq!(self.recv().await, r#"{"type":"capabilities_response","supports":["resolve"]}"#);
            self.send(&format!(r#"{{"type":"configure","config":{config}}}"#)).await;
            assert_eq!(self.recv().await, r#"{"type":"configure_ack","status":"ok"}"#);
            self.send(r#"{"type":"start"}"#).await;
            assert_eq!(self.recv().await, r#"{"type":"ready"}"#);
        }
    }

    /// Runs the provider on in-memory pipes; returns the core's ends and the
    /// task yielding (the lifecycle result, exit code, `cancel` called).
    fn spawn() -> (Core, tokio::task::JoinHandle<(bool, i32, bool)>) {
        let (core_in, plugin_in) = tokio::io::duplex(1 << 16);
        let (plugin_out, core_out) = tokio::io::duplex(1 << 16);
        let task = tokio::spawn(async move {
            let mut ctl = Control::new(BufReader::new(plugin_in), plugin_out);
            let mut events = EventSender::new(tokio::io::sink());
            let mut plugin = Fake::default();
            let mut started = false;
            let result = drive_provider(&mut plugin, &mut ctl, &mut events, &mut started).await;
            let ok = result.is_ok();
            let mut cancelled = false;
            let code = conclude(&mut ctl, result, started, || {
                cancelled = true;
                async {}
            })
            .await;
            (ok, code, cancelled || plugin.cancelled)
        });
        (Core { to_plugin: core_in, from_plugin: BufReader::new(core_out) }, task)
    }

    #[tokio::test]
    async fn resolves_until_eof_then_finishes() {
        let (mut core, task) = spawn();
        core.start("{}").await;
        core.send(r#"{"type":"resolve","id":1,"ref":"db#pw"}"#).await;
        assert_eq!(core.recv().await, r#"{"type":"resolved","id":1,"value":"v-of-db#pw"}"#);
        core.send(r#"{"type":"resolve","id":2,"ref":"missing.json"}"#).await;
        assert_eq!(
            core.recv().await,
            r#"{"type":"resolved","id":2,"error":{"code":"NOT_FOUND","message":"no missing.json"}}"#
        );
        core.send(r#"{"type":"resolve","id":3,"ref":"boom"}"#).await;
        assert!(core.recv().await.contains(r#""code":"RESOLVE_FAILED","message":"exploded""#));
        core.close().await;
        assert_eq!(core.recv().await, r#"{"type":"finished","status":"success"}"#);
        assert_eq!(task.await.unwrap(), (true, 0, false));
    }

    #[tokio::test]
    async fn pipelined_resolves_are_queued_and_eof_waits_for_them() {
        let (mut core, task) = spawn();
        core.start("{}").await;
        core.send(r#"{"type":"resolve","id":1,"ref":"a"}"#).await;
        core.send(r#"{"type":"resolve","id":2,"ref":"b"}"#).await;
        core.send(r#"{"type":"ping"}"#).await;
        core.close().await;
        let mut lines = Vec::new();
        for _ in 0..4 {
            lines.push(core.recv().await);
        }
        assert!(lines.contains(&r#"{"type":"resolved","id":1,"value":"v-of-a"}"#.to_string()), "{lines:?}");
        assert!(lines.contains(&r#"{"type":"resolved","id":2,"value":"v-of-b"}"#.to_string()), "{lines:?}");
        assert!(lines.contains(&r#"{"type":"pong"}"#.to_string()), "{lines:?}");
        assert_eq!(lines.last().unwrap(), r#"{"type":"finished","status":"success"}"#);
        assert!(task.await.unwrap().0);
    }

    #[tokio::test]
    async fn cancel_interrupts_a_slow_resolve() {
        let (mut core, task) = spawn();
        core.start("{}").await;
        core.send(r#"{"type":"resolve","id":1,"ref":"slow"}"#).await;
        core.send(r#"{"type":"cancel","reason":"another provider failed"}"#).await;
        assert_eq!(core.recv().await, r#"{"type":"finished","status":"cancelled"}"#);
        let (ok, code, cancelled) = tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap();
        assert_eq!((ok, code, cancelled), (false, 0, true));
    }

    #[tokio::test]
    async fn resend_replays_the_last_resolved() {
        let (mut core, task) = spawn();
        core.start("{}").await;
        core.send(r#"{"type":"resolve","id":7,"ref":"k"}"#).await;
        let first = core.recv().await;
        core.send(r#"{"type":"resend"}"#).await;
        assert_eq!(core.recv().await, first);
        core.send(r#"{"type":"error","code":"TIMEOUT","message":"too slow"}"#).await;
        assert_eq!(task.await.unwrap().1, 1);
    }

    #[tokio::test]
    async fn rejected_config_acks_an_error() {
        let (mut core, task) = spawn();
        core.send(r#"{"type":"handshake"}"#).await;
        core.recv().await;
        core.send(r#"{"type":"set_protocol_version","protocol_version":1}"#).await;
        core.send(r#"{"type":"capabilities_request"}"#).await;
        core.recv().await;
        core.send(r#"{"type":"configure","config":{"bad":true}}"#).await;
        let ack = core.recv().await;
        assert!(ack.contains(r#""status":"error""#) && ack.contains("CONFIGURE_FAILED"), "{ack}");
        assert_eq!(task.await.unwrap().1, 1);
    }

    #[tokio::test]
    async fn eof_before_start_is_core_gone() {
        let (mut core, task) = spawn();
        core.send(r#"{"type":"handshake"}"#).await;
        core.recv().await;
        core.close().await;
        assert_eq!(task.await.unwrap().1, 1);
    }
}
