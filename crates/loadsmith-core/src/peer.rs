//! The core's side of one plugin's control channel (fd0 out, fd1 in), plus
//! the process it belongs to.
//!
//! A dedicated task reads fd1 and forwards each line over a channel, so the
//! runner can wait on several plugins, the data pump, timeouts and signals at
//! once (`ControlReader::recv` is not cancel-safe; the channel is). [`Peer`]
//! applies the transport rules of protocol v1 on top:
//!
//! - one malformed line is answered with `resend`; a second consecutive one
//!   is a protocol failure (`MALFORMED_MESSAGE`);
//! - an inbound `resend` replays the last control message the core sent;
//! - `finished` is recorded, so the outcome survives an abort.

use std::process::ExitStatus;
use std::time::Duration;

use loadsmith_protocol::{error_codes, Cancel, Finished, Message, ProtocolError, Resend};
use loadsmith_transport::{ControlReader, ControlWriter, TransportError};
use tokio::io::BufReader;
use tokio::process::{Child, ChildStdin};
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::time::Instant;

use crate::error::CoreError;
use crate::shutdown::{Halt, Shutdown};
use crate::spawner::SpawnedPlugin;

/// One line (or the end) of a plugin's fd1.
#[derive(Debug)]
pub enum Inbound {
    Msg(Message),
    Malformed(String),
    Eof,
}

/// What [`Peer::process`] hands back to the caller.
#[derive(Debug)]
pub enum Event {
    Msg(Message),
    /// The plugin asked for a resend and [`Peer::replay_on_resend`] is off:
    /// the caller decides what to re-send.
    ResendRequested,
    /// fd1 closed: the plugin exited (or closed stdout).
    Eof,
}

/// A failed run step, with enough context to abort the run correctly: the
/// plugin at fault (`culprit`, a peer label) gets a core → plugin `error`
/// carrying `code`; every other plugin gets `cancel`.
#[derive(Debug)]
pub struct Failure {
    pub error: CoreError,
    pub culprit: Option<&'static str>,
    pub code: Option<&'static str>,
}

impl Failure {
    pub fn new(error: CoreError) -> Self {
        Self { error, culprit: None, code: None }
    }

    /// A failure caused by `culprit`, which is told why with `error{code}`.
    pub fn by(culprit: &'static str, code: &'static str, error: CoreError) -> Self {
        Self { error, culprit: Some(culprit), code: Some(code) }
    }

    /// A failure caused by `culprit`, which already knows (it reported or
    /// crashed): it gets `cancel` like the others, if it is still alive.
    pub fn of(culprit: &'static str, error: CoreError) -> Self {
        Self { error, culprit: Some(culprit), code: None }
    }

    pub fn halted(halt: Halt) -> Self {
        match halt {
            Halt::Signal(sig) => Failure::new(CoreError::Interrupted(sig)),
            Halt::Abort(reason) => Failure::new(CoreError::PluginProcess(format!("aborted: {reason}"))),
        }
    }

    /// The text of the `cancel`/`error` sent to plugins.
    pub fn reason(&self) -> String {
        self.error.to_string()
    }
}

impl From<CoreError> for Failure {
    fn from(error: CoreError) -> Self {
        Failure::new(error)
    }
}

pub struct Peer {
    label: &'static str,
    child: Child,
    writer: Option<ControlWriter<ChildStdin>>,
    rx: UnboundedReceiver<Inbound>,
    reader_task: tokio::task::JoinHandle<()>,
    last_sent: Option<Message>,
    malformed_streak: u32,
    eof: bool,
    finished: Option<Finished>,
    exit: Option<ExitStatus>,
    /// Replay the last control message on an inbound `resend` (the default).
    /// Off, `process` yields [`Event::ResendRequested`] instead.
    pub replay_on_resend: bool,
}

impl Peer {
    /// Wraps a spawned plugin whose event fd (fd4) was already taken.
    pub fn new(label: &'static str, plugin: SpawnedPlugin) -> Self {
        let SpawnedPlugin { child, control_in, control_out, .. } = plugin;
        let (tx, rx) = mpsc::unbounded_channel();
        let reader_task = tokio::spawn(async move {
            let mut reader = ControlReader::new(BufReader::new(control_out));
            loop {
                let inbound = match reader.recv().await {
                    Ok(Some(msg)) => Inbound::Msg(msg),
                    Ok(None) => Inbound::Eof,
                    Err(TransportError::Serialize(e)) => Inbound::Malformed(e.to_string()),
                    Err(e) => {
                        tracing::warn!(plugin = label, "control channel read failed: {e}");
                        Inbound::Eof
                    }
                };
                let last = matches!(inbound, Inbound::Eof);
                if tx.send(inbound).is_err() || last {
                    break;
                }
            }
        });
        Self {
            label,
            child,
            writer: Some(ControlWriter::new(control_in)),
            rx,
            reader_task,
            last_sent: None,
            malformed_streak: 0,
            eof: false,
            finished: None,
            exit: None,
            replay_on_resend: true,
        }
    }

    pub fn label(&self) -> &'static str {
        self.label
    }

    /// The plugin's `finished`, once received.
    pub fn finished(&self) -> Option<&Finished> {
        self.finished.as_ref()
    }

    /// Whether fd1 reached EOF (the plugin exited or closed stdout).
    pub fn at_eof(&self) -> bool {
        self.eof
    }

    /// Sends a lifecycle message and remembers it for a `resend`.
    pub async fn send(&mut self, msg: Message) -> Result<(), CoreError> {
        self.send_raw(&msg).await?;
        self.last_sent = Some(msg);
        Ok(())
    }

    /// Sends without remembering (`ping`, `resend`).
    pub async fn send_raw(&mut self, msg: &Message) -> Result<(), CoreError> {
        let label = self.label;
        let writer = self.writer.as_mut().ok_or_else(|| {
            CoreError::PluginProcess(format!("{label}: control input already closed"))
        })?;
        writer.send(msg).await.map_err(|e| CoreError::PluginProcess(format!("{label}: write: {e}")))
    }

    /// Closes the plugin's stdin (EOF). For a sink, "no more objects".
    pub fn close_input(&mut self) {
        self.writer = None;
    }

    /// The next raw line from the plugin. Cancel-safe; hand the result to
    /// [`process`](Self::process) outside the `select!`.
    pub async fn next_inbound(&mut self) -> Inbound {
        if self.eof {
            return std::future::pending().await;
        }
        self.rx.recv().await.unwrap_or(Inbound::Eof)
    }

    /// Applies the transport rules to one inbound line. `Ok(None)` means the
    /// line was fully handled here.
    pub async fn process(&mut self, inbound: Inbound) -> Result<Option<Event>, Failure> {
        match inbound {
            Inbound::Msg(msg) => {
                self.malformed_streak = 0;
                match msg {
                    Message::Resend(_) if self.replay_on_resend => {
                        if let Some(last) = self.last_sent.clone() {
                            tracing::debug!(plugin = self.label, "plugin asked for a resend");
                            // A failed write surfaces as EOF on fd1 soon after.
                            let _ = self.send_raw(&last).await;
                        }
                        Ok(None)
                    }
                    Message::Resend(_) => Ok(Some(Event::ResendRequested)),
                    Message::Finished(f) => {
                        self.finished = Some(f.clone());
                        Ok(Some(Event::Msg(Message::Finished(f))))
                    }
                    other => Ok(Some(Event::Msg(other))),
                }
            }
            Inbound::Malformed(reason) => {
                self.malformed_streak += 1;
                if self.malformed_streak >= 2 {
                    return Err(Failure::by(
                        self.label,
                        error_codes::MALFORMED_MESSAGE,
                        CoreError::Protocol(format!(
                            "{}: two consecutive malformed control lines (last: {reason})",
                            self.label
                        )),
                    ));
                }
                tracing::warn!(plugin = self.label, "malformed control line, asking for a resend: {reason}");
                let _ = self.send_raw(&Message::Resend(Resend { reason: Some(reason) })).await;
                Ok(None)
            }
            Inbound::Eof => {
                self.eof = true;
                Ok(Some(Event::Eof))
            }
        }
    }

    /// Waits for the next lifecycle message, within `limit`, unless
    /// `shutdown` fires first. Log/progress/pong lines are skipped. EOF, a
    /// timeout, or a `finished` (the plugin gave up) are failures.
    pub async fn recv_expect(
        &mut self,
        what: &str,
        limit: Option<Duration>,
        shutdown: &mut Shutdown,
    ) -> Result<Message, Failure> {
        let deadline = limit.map(|d| Instant::now() + d);
        loop {
            let inbound = tokio::select! {
                inbound = self.next_inbound() => inbound,
                halt = shutdown.wait() => return Err(Failure::halted(halt)),
                () = sleep_until(deadline) => {
                    return Err(Failure::by(
                        self.label,
                        error_codes::TIMEOUT,
                        CoreError::Timeout(format!(
                            "{}: no {what} within {}s",
                            self.label,
                            limit.unwrap_or_default().as_secs()
                        )),
                    ));
                }
            };
            match self.process(inbound).await? {
                None => {}
                Some(Event::Eof) => {
                    let status = self.exit_status_soon().await;
                    return Err(Failure::of(
                        self.label,
                        CoreError::PluginFailed(format!(
                            "{} {} while the core waited for {what}",
                            self.label,
                            describe_exit(status)
                        )),
                    ));
                }
                Some(Event::Msg(Message::Finished(f))) => {
                    return Err(Failure::of(self.label, finished_failure(self.label, &f)));
                }
                Some(Event::Msg(Message::Progress(_) | Message::Log(_) | Message::Pong)) => {}
                Some(Event::ResendRequested) => {}
                Some(Event::Msg(msg)) => return Ok(msg),
            }
        }
    }

    /// The exit status, waiting briefly for a plugin that just closed fd1.
    pub async fn exit_status_soon(&mut self) -> Option<ExitStatus> {
        if self.exit.is_none() {
            if let Ok(Ok(status)) =
                tokio::time::timeout(Duration::from_secs(2), self.child.wait()).await
            {
                self.exit = Some(status);
            }
        }
        self.exit
    }

    /// Tells a live plugin that the run is over: `error{code}` when `code`
    /// is set (this plugin is at fault), `cancel` otherwise.
    pub async fn notify_abort(&mut self, reason: &str, code: Option<&str>) {
        if self.eof || self.finished.is_some() {
            return;
        }
        let msg = match code {
            Some(code) => Message::Error(ProtocolError { code: code.into(), message: reason.into() }),
            None => Message::Cancel(Cancel { reason: reason.into() }),
        };
        tracing::debug!(plugin = self.label, ?msg, "→ abort");
        if let Err(e) = self.send(msg).await {
            tracing::debug!(plugin = self.label, "could not deliver the abort: {e}");
        }
    }

    /// Waits up to `grace` for the plugin to exit on its own (recording a
    /// `finished` it sends meanwhile), then kills its process group, and
    /// reaps it.
    pub async fn wind_down(&mut self, grace: Duration) {
        let deadline = Instant::now() + grace;
        while !self.eof {
            let inbound = tokio::select! {
                inbound = self.next_inbound() => inbound,
                () = tokio::time::sleep_until(deadline) => break,
            };
            match self.process(inbound).await {
                Ok(Some(Event::Eof)) | Err(_) => break,
                _ => {}
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if self.exit.is_none() {
            if let Ok(Ok(status)) = tokio::time::timeout(remaining, self.child.wait()).await {
                self.exit = Some(status);
            }
        }
        if self.exit.is_none() {
            tracing::warn!(plugin = self.label, "did not exit within the grace period; killing it");
        }
        self.kill();
        if self.exit.is_none() {
            if let Ok(status) = self.child.wait().await {
                self.exit = Some(status);
            }
        }
        self.writer = None;
        self.reader_task.abort();
    }

    /// Reaps a plugin that is expected to exit now (after `finished`).
    pub async fn reap(&mut self, grace: Duration) {
        self.wind_down(grace).await;
    }

    /// SIGKILLs the plugin's process group (the plugin and anything it
    /// spawned). A no-op once the plugin has been reaped.
    pub fn kill(&mut self) {
        if let Some(pid) = self.child.id() {
            if let Some(pid) = rustix::process::Pid::from_raw(pid as i32) {
                let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::Kill);
            }
            let _ = self.child.start_kill();
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        // Children are spawned with kill_on_drop, which only reaches the
        // plugin itself; also take down its process group.
        self.kill();
        self.reader_task.abort();
    }
}

/// `None` → never resolves.
async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending().await,
    }
}

/// "exited with exit status: 3", "was killed by signal 9", or "exited".
pub fn describe_exit(status: Option<ExitStatus>) -> String {
    use std::os::unix::process::ExitStatusExt;
    match status {
        Some(s) if s.code().is_some() => format!("exited with status {}", s.code().unwrap_or(0)),
        Some(s) if s.signal().is_some() => format!("was killed by signal {}", s.signal().unwrap_or(0)),
        _ => "exited".to_string(),
    }
}

/// The error for a plugin that sent a non-success `finished`.
pub fn finished_failure(label: &str, f: &Finished) -> CoreError {
    CoreError::PluginFailed(format!(
        "{label} reported {}: {}{}",
        match f.status {
            loadsmith_protocol::FinishedStatus::Success => "success",
            loadsmith_protocol::FinishedStatus::Error => "an error",
            loadsmith_protocol::FinishedStatus::Cancelled => "cancelled",
        },
        f.code.as_deref().map(|c| format!("{c}: ")).unwrap_or_default(),
        f.message.as_deref().unwrap_or("(no message)")
    ))
}
