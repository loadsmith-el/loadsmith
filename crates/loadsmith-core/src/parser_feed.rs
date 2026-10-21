//! The parser stage's upstream: a raw source and the files it announces.
//!
//! With a `parser:` in the pipeline, the source is a *raw* source (capability
//! `object_output`): it has no fd3, answers `start` with `ready`, and
//! announces each finished local file with `object_ready{path}` on fd4. Its
//! event drain forwards those paths over an unbounded channel; a [`Feed`]
//! hands each one to the parser as `parse_object{path}`. The parser's stdin
//! is owned by a writer task ([`Peer::queue_input`]), so a parser that reads
//! its stdin slowly never stalls the runner.
//!
//! **The parser's input ends gracefully only after the upstream succeeded:**
//! [`Feed::maybe_end_input`] closes the parser's stdin (its "no more files")
//! only when the path channel is closed (the raw source's fd4 reached EOF, so
//! every `object_ready` was read) **and** the raw source sent `finished` with
//! success. After any other outcome the parser gets `cancel` through the
//! abort path instead, and so never ends its stream after an upstream
//! failure.

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use loadsmith_config::Timeouts;
use loadsmith_protocol::{error_codes, FinishedStatus, Message, ParseObject};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Instant;

use crate::error::CoreError;
use crate::peer::{describe_exit, finished_failure, Event, Failure, Inbound, Peer};
use crate::shutdown::Shutdown;

/// How often the deadlines below are checked.
const TICK: Duration = Duration::from_millis(250);

/// The raw source's path channel and what was handed to the parser. The
/// raw source's [`Peer`] (label `source`) stays with the runner and is passed
/// in.
pub struct Feed {
    objects: UnboundedReceiver<PathBuf>,
    objects_open: bool,
    /// Every path handed to the parser, in order (`parse_object` is sent
    /// once per path).
    sent: HashSet<PathBuf>,
    finish_limit: Option<Duration>,
    /// Set when the raw source finished successfully: by then its fd4 must
    /// reach EOF (it exits), or no "no more files" can ever be told.
    closing_deadline: Option<Instant>,
}

/// One thing that happened upstream of the parser.
pub enum FeedEvent {
    Raw(Inbound),
    Object(Option<PathBuf>),
}

impl Feed {
    pub fn new(objects: UnboundedReceiver<PathBuf>, timeouts: &Timeouts) -> Self {
        Self {
            objects,
            objects_open: true,
            sent: HashSet::new(),
            finish_limit: Timeouts::limit(timeouts.finish_seconds),
            closing_deadline: None,
        }
    }

    /// The raw source sent `finished` with success.
    pub fn raw_succeeded(raw: &Peer) -> bool {
        raw.finished().is_some_and(|f| f.status == FinishedStatus::Success)
    }

    /// Files handed to the parser so far.
    pub fn objects_sent(&self) -> usize {
        self.sent.len()
    }

    /// The next event upstream. Cancel-safe.
    pub async fn next(&mut self, raw: &mut Peer) -> FeedEvent {
        tokio::select! {
            biased;
            path = self.objects.recv(), if self.objects_open => FeedEvent::Object(path),
            inbound = raw.next_inbound() => FeedEvent::Raw(inbound),
        }
    }

    /// Applies one upstream event: a path goes to the parser; the raw
    /// source's `finished`, crash or exit is recorded or is the failure.
    pub async fn handle(&mut self, event: FeedEvent, raw: &mut Peer, parser: &mut Peer) -> Result<(), Failure> {
        match event {
            FeedEvent::Object(Some(path)) => {
                if !self.sent.insert(path.clone()) {
                    tracing::debug!(path = %path.display(), "source announced a file twice; parsing it once");
                    return Ok(());
                }
                tracing::debug!(path = %path.display(), "→ parser: parse_object");
                // Handing a file to the parser (queued: the span is short;
                // the parser's own `loadsmith.parse` span covers the work).
                let _feed = tracing::info_span!(
                    target: "loadsmith::parser_feed",
                    parent: parser.scope().span(),
                    "parser.feed",
                    loadsmith.object.path = %path.display(),
                )
                .entered();
                let msg = Message::ParseObject(ParseObject { path: path.to_string_lossy().into_owned() });

                // Queued, never blocks. It fails only once the parser's
                // stdin is gone, which the parser's fd1 reports.
                if let Err(e) = parser.send(msg).await {
                    tracing::debug!("could not hand a file to the parser: {e}");
                }
            }
            FeedEvent::Object(None) => {
                self.objects_open = false;
                tracing::debug!(objects = self.sent.len(), "the source's event channel closed");
            }
            FeedEvent::Raw(inbound) => match raw.process(inbound).await? {
                Some(Event::Msg(Message::Finished(f))) => {
                    if f.status != FinishedStatus::Success {
                        return Err(Failure::of("source", finished_failure("source", &f, raw.mask())));
                    }
                    tracing::info!("source finished — {} file(s) announced", self.sent.len());
                    self.closing_deadline = self.finish_limit.map(|d| Instant::now() + d);
                }
                Some(Event::Eof) => {
                    if raw.finished().is_none() {
                        let status = raw.exit_status_soon().await;
                        return Err(Failure::of(
                            "source",
                            CoreError::PluginFailed(format!(
                                "source {} without sending finished",
                                describe_exit(status)
                            )),
                        ));
                    }
                }
                Some(Event::Msg(Message::Log(_) | Message::Progress(_) | Message::Pong)) => {}
                Some(Event::Msg(other)) => {
                    let shown = raw.mask().apply(&format!("{other:?}"));
                    tracing::warn!(plugin = "source", "ignoring unexpected control message: {shown}");
                }
                Some(Event::ResendRequested) | None => {}
            },
        }
        Ok(())
    }

    /// Ends the parser's input (stdin EOF: "no more files") once no more
    /// paths can arrive **and** the raw source succeeded. Never otherwise.
    pub fn maybe_end_input(&mut self, raw: &Peer, parser: &mut Peer) {
        if !parser.input_closed() && !self.objects_open && Self::raw_succeeded(raw) {
            parser.close_input();
            tracing::info!("all {} file(s) handed to the parser — its input ended", self.sent.len());
        }
    }

    /// The raw source finished, but its event channel stayed open past
    /// `finish_seconds` (it did not exit).
    pub fn overdue(&self, now: Instant) -> Option<Failure> {
        (self.objects_open && self.closing_deadline.is_some_and(|d| now >= d)).then(|| {
            Failure::by(
                "source",
                error_codes::TIMEOUT,
                CoreError::Timeout(format!(
                    "source: its event channel (fd4) is still open {}s after it finished \
                     (timeouts.finish_seconds)",
                    self.finish_limit.unwrap_or_default().as_secs()
                )),
            )
        })
    }
}

/// Waits for the parser's `schema` (within `start_seconds`, measured from
/// here), handing it the raw source's files meanwhile: a parser that infers
/// its schema sends it only after it opened the first file.
pub async fn await_parser_schema(
    parser: &mut Peer,
    raw: &mut Peer,
    feed: &mut Feed,
    timeouts: &Timeouts,
    shutdown: &mut Shutdown,
) -> Result<loadsmith_protocol::Schema, Failure> {
    let limit = Timeouts::limit(timeouts.start_seconds);
    let deadline = limit.map(|d| Instant::now() + d);
    let mut tick = tokio::time::interval(TICK);
    loop {
        feed.maybe_end_input(raw, parser);
        tokio::select! {
            biased;
            halt = shutdown.wait() => return Err(Failure::halted(halt)),
            inbound = parser.next_inbound() => match parser.process(inbound).await? {
                Some(Event::Msg(Message::Schema(s))) => {
                    tracing::debug!(plugin = "parser", fields = s.fields.len(), "← schema");
                    return Ok(s);
                }
                Some(Event::Msg(Message::Finished(f))) if f.status != FinishedStatus::Success => {
                    return Err(Failure::of("parser", finished_failure("parser", &f, parser.mask())));
                }
                Some(Event::Msg(Message::Finished(_))) => {
                    return Err(Failure::by(
                        "parser",
                        error_codes::PROTOCOL_VIOLATION,
                        CoreError::Protocol("parser reported success before it sent its schema".into()),
                    ));
                }
                Some(Event::Eof) => {
                    let status = parser.exit_status_soon().await;
                    return Err(Failure::of(
                        "parser",
                        CoreError::PluginFailed(format!(
                            "parser {} while the core waited for its schema",
                            describe_exit(status)
                        )),
                    ));
                }
                Some(Event::Msg(Message::Log(_) | Message::Progress(_) | Message::Pong))
                | Some(Event::ResendRequested)
                | None => {}
                Some(Event::Msg(other)) => {
                    return Err(Failure::by(
                        "parser",
                        error_codes::PROTOCOL_VIOLATION,
                        CoreError::Protocol(parser.mask().apply(&format!("parser: expected schema, got {other:?}"))),
                    ));
                }
            },
            event = feed.next(raw) => feed.handle(event, raw, parser).await?,
            () = sleep_until(deadline) => {
                return Err(Failure::by(
                    "parser",
                    error_codes::TIMEOUT,
                    CoreError::Timeout(format!(
                        "parser: no schema within {}s (timeouts.start_seconds)",
                        timeouts.start_seconds
                    )),
                ));
            }
            _ = tick.tick() => {
                if let Some(failure) = feed.overdue(Instant::now()) {
                    return Err(failure);
                }
            }
        }
    }
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending().await,
    }
}
