//! The raw-source runtime: a source that produces **files**, not Arrow.
//!
//! Lifecycle (driven by the core, with a `parser:` in the pipeline):
//! handshake (kind `source`) → capabilities (`object_output`) → `configure`
//! → `start` → `ready`. Then one `object_ready{path}` event on fd4 per
//! finished local file, and `finished` once there are no more. A raw source
//! has no fd3: the core hands each path to the pipeline's parser, which
//! writes the Arrow stream.
//!
//! The core lets the parser see the end of its input only after this source
//! sent `finished` with success, so a raw source that fails half-way never
//! causes a partial load. `cancel`/`error` from the core stop it at any
//! point, also in the middle of `next_object`.

use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use loadsmith_protocol::{capabilities, Finished, Message, PluginKind};
use tokio::io::AsyncWrite;

use crate::context::EventSender;
use crate::telemetry::{self, phase_span};
use tracing::Instrument;

use crate::config::PluginConfig;
use crate::control::{answer_configure, conclude, handshake, recv_configure, recv_start, Control, Flow};

/// Implemented by raw sources. The SDK drives the conversation;
/// implementors list (or fetch) files one at a time.
#[async_trait]
pub trait ObjectSourcePlugin: Send {
    fn plugin_name(&self) -> &str;
    fn plugin_version(&self) -> &str;

    /// Capabilities advertised during the handshake. Defaults to
    /// `object_output`, which marks a raw source (the core then requires a
    /// `parser:`).
    fn capabilities(&self) -> Vec<String> {
        vec![capabilities::OBJECT_OUTPUT.into()]
    }

    /// Called after `configure`. Validate the config and prepare. Return
    /// `Err` to send `configure_ack { error }` and exit.
    async fn configure(&mut self, config: PluginConfig) -> Result<()>;

    /// The next file, complete and readable at a local path; `Ok(None)` when
    /// there are no more. Files that are read in place (a local directory, a
    /// mounted bucket) are never deleted by the core.
    async fn next_object(&mut self) -> Result<Option<PathBuf>>;

    /// Called when the run will not complete, once `start` was received: the
    /// core sent `cancel` or `error`, the core is gone, or this source
    /// failed. Stop and release what you hold (a half-downloaded file). An
    /// in-flight `next_object` future may have been dropped before this runs.
    async fn cancel(&mut self);
}

/// Entry point for raw-source plugin binaries. Never returns normally.
///
/// Exit code: `0` after success or a `cancel`, `1` after a failure, an
/// `error` from the core, or a lost core.
pub async fn run_object_source<P: ObjectSourcePlugin + 'static>(mut plugin: P) -> ! {
    telemetry::install();
    let mut ctl = Control::stdio();
    let mut started = false;
    let mut events = EventSender::fd4();
    let result = drive_object_source(&mut plugin, &mut ctl, &mut events, &mut started).await;
    let code = conclude(&mut ctl, result, started, || plugin.cancel()).await;
    events.close().await;
    std::process::exit(code);
}

pub(crate) async fn drive_object_source<P, W, E>(
    plugin: &mut P,
    ctl: &mut Control<W>,
    events: &mut EventSender<E>,
    started: &mut bool,
) -> Result<(), Flow>
where
    P: ObjectSourcePlugin,
    W: AsyncWrite + Unpin,
    E: AsyncWrite + Unpin,
{
    // ── Handshake + capabilities ─────────────────────────────────────────
    handshake(ctl, PluginKind::Source, plugin.plugin_name(), plugin.plugin_version(), plugin.capabilities())
        .await?;

    // ── Configure ────────────────────────────────────────────────────────
    let (config, warnings) = recv_configure(ctl).await?;
    let span = phase_span!("configure");
    let outcome = plugin.configure(config).instrument(span.clone()).await;
    telemetry::outcome(&span, &outcome);
    drop(span);
    answer_configure(ctl, events, &warnings, outcome).await?;

    // ── Start ─────────────────────────────────────────────────────────────
    recv_start(ctl).await?;
    *started = true;
    ctl.send(Message::Ready).await?;

    // ── Object loop (object_ready on fd4) ─────────────────────────────────
    let mut objects: u64 = 0;
    loop {
        // Wait for the next file *and* the core at the same time, so a
        // `cancel` stops a source in the middle of a long download.
        let span = tracing::info_span!(
            "loadsmith.object",
            loadsmith.object.path = tracing::field::Empty,
            error = tracing::field::Empty,
        );
        let next = {
            let work = plugin.next_object().instrument(span.clone());
            tokio::pin!(work);
            loop {
                tokio::select! {
                    biased;
                    inbound = ctl.next_inbound() => match ctl.process(inbound).await {
                        Ok(None) => {}
                        Ok(Some(other)) => {
                            tracing::warn!("ignoring unexpected control message: {other:?}");
                        }
                        Err(flow) => break Err(flow),
                    },
                    result = &mut work => break Ok(result),
                }
            }
        };
        match next? {
            Ok(Some(path)) => {
                span.record("loadsmith.object.path", path.to_string_lossy().as_ref());
                drop(span);
                objects += 1;
                events.object_ready(path.to_string_lossy().into_owned()).await;
            }
            Ok(None) => break,
            Err(e) => {
                telemetry::fail(&span, &e);
                drop(span);
                events.error_msg(format!("object error: {e:#}")).await;
                return Err(Flow::fatal("OBJECT_FAILED", e));
            }
        }
    }

    events.info(format!("{objects} object(s) ready")).await;
    events.close().await;
    ctl.send(Message::Finished(Finished::success())).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    /// Yields `n` paths (`/in/1` …); `fail` errors after them instead of
    /// ending; `hang` never yields after them.
    struct Fake {
        n: u64,
        given: u64,
        mode: &'static str,
    }

    #[async_trait]
    impl ObjectSourcePlugin for Fake {
        fn plugin_name(&self) -> &str {
            "loadsmith-source-fakefiles"
        }
        fn plugin_version(&self) -> &str {
            "0.0.0"
        }
        async fn configure(&mut self, _config: crate::PluginConfig) -> Result<()> {
            Ok(())
        }
        async fn next_object(&mut self) -> Result<Option<PathBuf>> {
            if self.given == self.n {
                match self.mode {
                    "fail" => anyhow::bail!("listing failed"),
                    "hang" => std::future::pending::<()>().await,
                    _ => return Ok(None),
                }
            }
            self.given += 1;
            Ok(Some(PathBuf::from(format!("/in/{}", self.given))))
        }
        async fn cancel(&mut self) {}
    }

    async fn run(n: u64, mode: &'static str, cancel: bool) -> (Vec<String>, String, i32) {
        let (mut core_in, plugin_in) = tokio::io::duplex(1 << 16);
        let (plugin_out, core_out) = tokio::io::duplex(1 << 16);
        let (ev_w, mut ev_r) = tokio::io::duplex(1 << 16);
        let task = tokio::spawn(async move {
            let mut ctl = Control::new(BufReader::new(plugin_in), plugin_out);
            let mut events = EventSender::new(ev_w);
            let mut plugin = Fake { n, given: 0, mode };
            let mut started = false;
            let result = drive_object_source(&mut plugin, &mut ctl, &mut events, &mut started).await;
            conclude(&mut ctl, result, started, || async {}).await
        });
        let mut from = BufReader::new(core_out);
        let mut lines = Vec::new();
        for l in [
            r#"{"type":"handshake"}"#,
            r#"{"type":"set_protocol_version","protocol_version":1}"#,
            r#"{"type":"capabilities_request"}"#,
            r#"{"type":"configure","config":{}}"#,
            r#"{"type":"start"}"#,
        ] {
            core_in.write_all(format!("{l}\n").as_bytes()).await.unwrap();
        }
        if cancel {
            tokio::time::sleep(Duration::from_millis(50)).await;
            core_in.write_all(b"{\"type\":\"cancel\",\"reason\":\"r\"}\n").await.unwrap();
        }
        loop {
            let mut s = String::new();
            if from.read_line(&mut s).await.unwrap() == 0 {
                break;
            }
            let done = s.contains(r#""type":"finished""#);
            lines.push(s.trim_end().to_string());
            if done {
                break;
            }
        }
        let code = tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap();
        let mut ev = String::new();
        ev_r.read_to_string(&mut ev).await.unwrap();
        (lines, ev, code)
    }

    #[tokio::test]
    async fn announces_each_file_then_finishes() {
        let (lines, events, code) = run(2, "", false).await;
        assert!(lines[0].contains(r#""kind":"source""#));
        assert_eq!(lines[1], r#"{"type":"capabilities_response","supports":["object_output"]}"#);
        assert_eq!(lines[3], r#"{"type":"ready"}"#);
        assert_eq!(lines.last().unwrap(), r#"{"type":"finished","status":"success"}"#);
        assert!(events.contains(r#"{"type":"object_ready","path":"/in/1"}"#));
        assert!(events.contains(r#"{"type":"object_ready","path":"/in/2"}"#));
        assert_eq!(code, 0);
    }

    #[tokio::test]
    async fn a_failure_is_finished_error() {
        let (lines, _, code) = run(1, "fail", false).await;
        assert!(lines.last().unwrap().contains(r#""code":"OBJECT_FAILED""#), "{lines:?}");
        assert_eq!(code, 1);
    }

    #[tokio::test]
    async fn cancel_interrupts_a_slow_object() {
        let (lines, _, code) = run(1, "hang", true).await;
        assert_eq!(lines.last().unwrap(), r#"{"type":"finished","status":"cancelled"}"#);
        assert_eq!(code, 0);
    }
}
