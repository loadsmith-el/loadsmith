//! The plugin side of the control channel (fd0 in, fd1 out), shared by every
//! SDK plugin kind.
//!
//! A dedicated task reads fd0 and forwards each line over a channel, so a
//! plugin can wait for its own work (a batch, a write) and for the core at the
//! same time: `cancel` must be able to arrive mid-stream. [`Control`] also
//! implements the transport-level rules every kind shares:
//!
//! - `ping` is answered with `pong`;
//! - `resend` re-sends the most recent control message this plugin sent;
//! - a malformed line from the core is answered with `resend` once; a second
//!   consecutive malformed line is fatal;
//! - `cancel`, `error` and an unexpected EOF on fd0 stop the plugin
//!   ([`Stop`]).

use loadsmith_protocol::{
    CapabilitiesResponse, ConfigureAck, HandshakeAck, Message, PluginKind, Resend, SetProtocolVersion,
    StartParams,
};
use loadsmith_transport::{ControlReader, ControlWriter, TransportError};
use tokio::io::{AsyncBufRead, AsyncWrite, BufReader, Stdout};
use tokio::sync::mpsc;

use crate::config::{take_warnings, ConfigRejection, PluginConfig};
use crate::context::EventSender;

pub(crate) const PROTOCOL_VERSION: u32 = 1;

/// One line (or the end) of the core → plugin control channel.
#[derive(Debug)]
pub(crate) enum Inbound {
    Msg(Message),
    Malformed(String),
    Eof,
    Failed(String),
}

/// Why a plugin stops before finishing normally, on the core's behalf.
#[derive(Debug)]
pub(crate) enum Stop {
    /// The core sent `cancel`.
    Cancel(String),
    /// The core sent `error`.
    CoreError { code: String, message: String },
    /// fd0 closed (or failed) while the plugin still expected the core: the
    /// core is gone or has abandoned the run.
    CoreGone,
}

/// Control flow out of a plugin's lifecycle.
#[derive(Debug)]
pub(crate) enum Flow {
    /// Stop on the core's behalf (see [`Stop`]).
    Stop(Stop),
    /// The plugin itself failed. `code` goes into `finished`.
    Fatal { code: &'static str, error: anyhow::Error },
}

impl Flow {
    pub(crate) fn fatal(code: &'static str, error: impl Into<anyhow::Error>) -> Self {
        Flow::Fatal { code, error: error.into() }
    }
}

impl From<TransportError> for Flow {
    fn from(e: TransportError) -> Self {
        Flow::fatal("CONTROL_CHANNEL_FAILED", e)
    }
}

impl From<anyhow::Error> for Flow {
    fn from(e: anyhow::Error) -> Self {
        Flow::fatal("PLUGIN_FAILED", e)
    }
}

pub(crate) struct Control<W> {
    rx: mpsc::UnboundedReceiver<Inbound>,
    writer: ControlWriter<W>,
    last_sent: Option<Message>,
    malformed_streak: u32,
    ended: bool,
    finished_sent: bool,
}

impl Control<Stdout> {
    /// The control channel over the process's stdin/stdout.
    pub(crate) fn stdio() -> Self {
        Control::new(BufReader::new(tokio::io::stdin()), tokio::io::stdout())
    }
}

impl<W: AsyncWrite + Unpin> Control<W> {
    pub(crate) fn new<R>(reader: R, writer: W) -> Self
    where
        R: AsyncBufRead + Unpin + Send + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut reader = ControlReader::new(reader);
            loop {
                let inbound = match reader.recv().await {
                    Ok(Some(msg)) => Inbound::Msg(msg),
                    Ok(None) => Inbound::Eof,
                    Err(TransportError::Serialize(e)) => Inbound::Malformed(e.to_string()),
                    Err(e) => Inbound::Failed(e.to_string()),
                };
                let last = matches!(inbound, Inbound::Eof | Inbound::Failed(_));
                if tx.send(inbound).is_err() || last {
                    break;
                }
            }
        });
        Self {
            rx,
            writer: ControlWriter::new(writer),
            last_sent: None,
            malformed_streak: 0,
            ended: false,
            finished_sent: false,
        }
    }

    /// Sends a control message and remembers it for a later `resend`.
    pub(crate) async fn send(&mut self, msg: Message) -> Result<(), TransportError> {
        self.writer.send(&msg).await?;
        if matches!(msg, Message::Finished(_)) {
            self.finished_sent = true;
        }
        self.last_sent = Some(msg);
        Ok(())
    }

    /// Whether this plugin already sent its `finished`.
    pub(crate) fn finished_sent(&self) -> bool {
        self.finished_sent
    }

    /// The next raw line from the core. Cancel-safe: use it in `select!`, then
    /// hand the result to [`process`](Self::process) outside the select.
    pub(crate) async fn next_inbound(&mut self) -> Inbound {
        if self.ended {
            return Inbound::Eof;
        }
        let inbound = self.rx.recv().await.unwrap_or(Inbound::Eof);
        if matches!(inbound, Inbound::Eof | Inbound::Failed(_)) {
            self.ended = true;
        }
        inbound
    }

    /// Applies the shared transport rules to one inbound line. Returns the
    /// message the lifecycle has to act on, `Ok(None)` when the line was fully
    /// handled here, or the [`Flow`] that stops the plugin.
    pub(crate) async fn process(&mut self, inbound: Inbound) -> Result<Option<Message>, Flow> {
        match inbound {
            Inbound::Msg(msg) => {
                self.malformed_streak = 0;
                match msg {
                    Message::Ping => {
                        // Not remembered: a `resend` must replay the lifecycle
                        // message, never a pong.
                        self.writer.send(&Message::Pong).await?;
                        Ok(None)
                    }
                    Message::Resend(_) => {
                        if let Some(last) = self.last_sent.clone() {
                            self.writer.send(&last).await?;
                        }
                        Ok(None)
                    }
                    Message::Cancel(c) => Err(Flow::Stop(Stop::Cancel(c.reason))),
                    Message::Error(e) => {
                        Err(Flow::Stop(Stop::CoreError { code: e.code, message: e.message }))
                    }
                    other => Ok(Some(other)),
                }
            }
            Inbound::Malformed(reason) => {
                self.malformed_streak += 1;
                if self.malformed_streak >= 2 {
                    return Err(Flow::fatal(
                        "MALFORMED_MESSAGE",
                        anyhow::anyhow!("two consecutive malformed control lines from the core: {reason}"),
                    ));
                }
                self.writer.send(&Message::Resend(Resend { reason: Some(reason) })).await?;
                Ok(None)
            }
            Inbound::Failed(e) => {
                eprintln!("[plugin-sdk] reading the control channel failed: {e}");
                Err(Flow::Stop(Stop::CoreGone))
            }
            Inbound::Eof => Err(Flow::Stop(Stop::CoreGone)),
        }
    }

    /// The next lifecycle message. EOF stops the plugin ([`Stop::CoreGone`]).
    pub(crate) async fn recv(&mut self) -> Result<Message, Flow> {
        loop {
            let inbound = self.next_inbound().await;
            if let Some(msg) = self.process(inbound).await? {
                return Ok(msg);
            }
        }
    }

    /// Like [`recv`](Self::recv), but a clean EOF is `Ok(None)` — for a sink,
    /// whose stdin closing means "no more objects".
    pub(crate) async fn recv_or_eof(&mut self) -> Result<Option<Message>, Flow> {
        loop {
            match self.next_inbound().await {
                Inbound::Eof => return Ok(None),
                inbound => {
                    if let Some(msg) = self.process(inbound).await? {
                        return Ok(Some(msg));
                    }
                }
            }
        }
    }
}

/// Handshake, version negotiation and the capability exchange, common to
/// every plugin kind.
pub(crate) async fn handshake<W: AsyncWrite + Unpin>(
    ctl: &mut Control<W>,
    kind: PluginKind,
    name: &str,
    version: &str,
    capabilities: Vec<String>,
) -> Result<(), Flow> {
    let msg = ctl.recv().await?;
    if !matches!(msg, Message::Handshake) {
        return Err(unexpected("handshake", &msg));
    }
    ctl.send(Message::HandshakeAck(HandshakeAck {
        protocol_supported_versions: vec![PROTOCOL_VERSION],
        plugin_name: name.to_string(),
        plugin_version: version.to_string(),
        kind,
    }))
    .await?;

    match ctl.recv().await? {
        Message::SetProtocolVersion(SetProtocolVersion { protocol_version })
            if protocol_version == PROTOCOL_VERSION => {}
        Message::SetProtocolVersion(SetProtocolVersion { protocol_version }) => {
            return Err(Flow::fatal(
                "UNSUPPORTED_PROTOCOL_VERSION",
                anyhow::anyhow!("unsupported protocol version {protocol_version}"),
            ))
        }
        other => return Err(unexpected("set_protocol_version", &other)),
    }

    let msg = ctl.recv().await?;
    if !matches!(msg, Message::CapabilitiesRequest) {
        return Err(unexpected("capabilities_request", &msg));
    }
    ctl.send(Message::CapabilitiesResponse(CapabilitiesResponse { supports: capabilities }))
        .await?;
    Ok(())
}

/// Waits for `configure` and returns its config block with the
/// unknown-keys policy, and the handle its parse warnings collect in.
pub(crate) async fn recv_configure<W: AsyncWrite + Unpin>(
    ctl: &mut Control<W>,
) -> Result<(PluginConfig, ConfigWarnings), Flow> {
    match ctl.recv().await? {
        Message::Configure(c) => {
            let policy = c.unknown_keys();
            let config = PluginConfig::new(c.config, policy);
            let warnings = config.warnings();
            Ok((config, warnings))
        }
        other => Err(unexpected("configure", &other)),
    }
}

/// Where [`PluginConfig::parse`] leaves its warnings.
pub(crate) type ConfigWarnings = std::sync::Arc<std::sync::Mutex<Vec<String>>>;

/// Answers `configure` with the outcome of the plugin's `configure()`:
/// first one `warn` log per ignored unknown key on fd4, then
/// `configure_ack`: `ok`, or `error` with the code of a [`ConfigRejection`]
/// (`CONFIGURE_FAILED` for any other error).
pub(crate) async fn answer_configure<W: AsyncWrite + Unpin, E: AsyncWrite + Unpin>(
    ctl: &mut Control<W>,
    events: &mut EventSender<E>,
    warnings: &ConfigWarnings,
    outcome: anyhow::Result<()>,
) -> Result<(), Flow> {
    for warning in take_warnings(warnings) {
        events.warn(warning).await;
    }
    match outcome {
        Ok(()) => {
            ctl.send(Message::ConfigureAck(ConfigureAck::ok())).await?;
            Ok(())
        }
        Err(e) => {
            let code = e.downcast_ref::<ConfigRejection>().map_or("CONFIGURE_FAILED", ConfigRejection::code);
            ctl.send(Message::ConfigureAck(ConfigureAck::error(code, format!("{e:#}")))).await?;
            Err(Flow::fatal(code, e.context("configure rejected")))
        }
    }
}

/// Waits for `start`.
pub(crate) async fn recv_start<W: AsyncWrite + Unpin>(
    ctl: &mut Control<W>,
) -> Result<StartParams, Flow> {
    match ctl.recv().await? {
        Message::Start(params) => Ok(params),
        other => Err(unexpected("start", &other)),
    }
}

pub(crate) fn unexpected(expected: &str, got: &Message) -> Flow {
    Flow::fatal("PROTOCOL_VIOLATION", anyhow::anyhow!("expected {expected}, got {got:?}"))
}

/// What the process does once a plugin's lifecycle is over: report, then exit
/// with the returned code. `started` says whether the plugin got `start` (only
/// then can it have side effects to undo; `cancel_hook` runs only then).
///
/// - success → exit 0;
/// - `cancel` → undo, `finished{cancelled}`, exit 0;
/// - `error` from the core, or the core gone → undo, exit 1;
/// - the plugin's own failure → undo, `finished{error}` (unless one was
///   already sent), exit 1.
///
/// The undo runs **before** `finished`, so `finished` really is the last
/// thing the plugin does.
pub(crate) async fn conclude<W, F, Fut>(
    ctl: &mut Control<W>,
    result: Result<(), Flow>,
    started: bool,
    cancel_hook: F,
) -> i32
where
    W: AsyncWrite + Unpin,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let code = match result {
        Ok(()) => 0,
        Err(Flow::Stop(stop)) => {
            if started {
                cancel_hook().await;
            }
            match stop {
                Stop::Cancel(reason) => {
                    eprintln!("[plugin-sdk] cancelled by the core: {reason}");
                    let _ = ctl.send(Message::Finished(loadsmith_protocol::Finished::cancelled())).await;
                    0
                }
                Stop::CoreError { code, message } => {
                    eprintln!("[plugin-sdk] the core aborted the run ({code}): {message}");
                    1
                }
                Stop::CoreGone => {
                    eprintln!("[plugin-sdk] control channel closed by the core; aborting");
                    1
                }
            }
        }
        Err(Flow::Fatal { code, error }) => {
            eprintln!("[plugin-sdk] fatal error: {error:#}");
            if started {
                cancel_hook().await;
                if !ctl.finished_sent() {
                    let _ = ctl
                        .send(Message::Finished(loadsmith_protocol::Finished::error(
                            code,
                            format!("{error:#}"),
                        )))
                        .await;
                }
            }
            1
        }
    };
    code
}

#[cfg(test)]
mod tests {
    use super::*;
    use loadsmith_protocol::{Cancel, Configure};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, DuplexStream};

    /// A `Control` wired to in-memory pipes: returns (control, the core's write
    /// end of fd0, the core's read end of fd1).
    fn wired() -> (Control<DuplexStream>, DuplexStream, BufReader<DuplexStream>) {
        let (core_in, plugin_in) = tokio::io::duplex(4096);
        let (plugin_out, core_out) = tokio::io::duplex(4096);
        (Control::new(BufReader::new(plugin_in), plugin_out), core_in, BufReader::new(core_out))
    }

    async fn line(r: &mut BufReader<DuplexStream>) -> String {
        let mut s = String::new();
        r.read_line(&mut s).await.unwrap();
        s.trim_end().to_string()
    }

    #[tokio::test]
    async fn one_malformed_line_asks_for_a_resend_and_recovers() {
        let (mut ctl, mut core_in, mut core_out) = wired();
        core_in.write_all(b"{not json\n{\"type\":\"handshake\"}\n").await.unwrap();
        assert!(matches!(ctl.recv().await.unwrap(), Message::Handshake));
        let sent = line(&mut core_out).await;
        assert!(sent.starts_with(r#"{"type":"resend""#), "{sent}");
    }

    #[tokio::test]
    async fn two_consecutive_malformed_lines_are_fatal() {
        let (mut ctl, mut core_in, _core_out) = wired();
        core_in.write_all(b"garbage\nmore garbage\n").await.unwrap();
        match ctl.recv().await {
            Err(Flow::Fatal { code, .. }) => assert_eq!(code, "MALFORMED_MESSAGE"),
            other => panic!("expected fatal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_good_line_resets_the_malformed_streak() {
        let (mut ctl, mut core_in, _core_out) = wired();
        core_in
            .write_all(b"garbage\n{\"type\":\"ping\"}\ngarbage\n{\"type\":\"start\"}\n")
            .await
            .unwrap();
        assert!(matches!(ctl.recv().await.unwrap(), Message::Start(_)));
    }

    #[tokio::test]
    async fn resend_replays_the_last_lifecycle_message_not_a_pong() {
        let (mut ctl, mut core_in, mut core_out) = wired();
        ctl.send(Message::Ready).await.unwrap();
        assert_eq!(line(&mut core_out).await, r#"{"type":"ready"}"#);
        core_in.write_all(b"{\"type\":\"ping\"}\n{\"type\":\"resend\"}\n").await.unwrap();
        core_in.write_all(b"{\"type\":\"start\"}\n").await.unwrap();
        assert!(matches!(ctl.recv().await.unwrap(), Message::Start(_)));
        assert_eq!(line(&mut core_out).await, r#"{"type":"pong"}"#);
        assert_eq!(line(&mut core_out).await, r#"{"type":"ready"}"#);
    }

    #[tokio::test]
    async fn cancel_error_and_eof_stop_the_plugin() {
        let (mut ctl, mut core_in, _o) = wired();
        let cancel = serde_json::to_string(&Message::Cancel(Cancel { reason: "r".into() })).unwrap();
        core_in.write_all(format!("{cancel}\n").as_bytes()).await.unwrap();
        assert!(matches!(ctl.recv().await, Err(Flow::Stop(Stop::Cancel(r))) if r == "r"));

        let (mut ctl, mut core_in, _o) = wired();
        core_in
            .write_all(b"{\"type\":\"error\",\"code\":\"TIMEOUT\",\"message\":\"m\"}\n")
            .await
            .unwrap();
        assert!(matches!(ctl.recv().await, Err(Flow::Stop(Stop::CoreError { code, .. })) if code == "TIMEOUT"));

        let (mut ctl, core_in, _o) = wired();
        drop(core_in);
        assert!(matches!(ctl.recv().await, Err(Flow::Stop(Stop::CoreGone))));
    }

    #[tokio::test]
    async fn unknown_types_are_ignored() {
        let (mut ctl, mut core_in, _o) = wired();
        let configure =
            serde_json::to_string(&Message::Configure(Configure::new(serde_json::json!({}))))
                .unwrap();
        core_in
            .write_all(format!("{{\"type\":\"from_the_future\"}}\n{configure}\n").as_bytes())
            .await
            .unwrap();
        assert!(matches!(ctl.recv().await.unwrap(), Message::Configure(_)));
    }

    #[tokio::test]
    async fn sink_eof_is_not_an_abort() {
        let (mut ctl, core_in, _o) = wired();
        drop(core_in);
        assert!(ctl.recv_or_eof().await.unwrap().is_none());
    }
}
