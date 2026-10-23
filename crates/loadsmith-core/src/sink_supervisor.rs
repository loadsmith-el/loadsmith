//! Supervises the sink delivery stage.
//!
//! The sink is a *delivery* plugin, not a data-plane participant. The
//! destination stages files locally and announces each finalized file as an
//! `ObjectReady` event on fd4; the core's event drain forwards those paths here
//! (via an unbounded channel), and this supervisor hands each to the sink as a
//! `DeliverObject` control message, collecting an `ObjectDelivered` ack per
//! file.
//!
//! **The core owns the delivery ledger.** The sink is stateless — if it hangs,
//! dies, or is torn down, the supervisor respawns it and re-sends every object
//! that has not yet been acknowledged, so delivery resumes from where it
//! stopped. This matches the project rule that the core is the sole owner of
//! state and plugins are memoryless tools. A `resend` from the sink (it got a
//! malformed line) is answered the same way: every unacknowledged object is
//! re-sent, which is safe because `deliver` is idempotent.
//!
//! When the run aborts (or a signal arrives), the `shutdown` input fires: the
//! supervisor sends `cancel` to the live sink, waits the grace period, kills
//! it, and returns an error. Objects already acknowledged stay delivered.
//!
//! The control channel is read through [`Peer`], whose inbound channel is
//! cancel-safe, so the supervisor can `select!` over it.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use loadsmith_config::Timeouts;
use loadsmith_protocol::{DeliverObject, FinishedStatus, Message, PluginKind, UnknownKeys};
use tokio::io::BufReader;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::events::StagedObject;
use crate::{
    error::CoreError,
    events::drain_events,
    telemetry,

    lifecycle::{run_handshake_and_configure, start_and_await_ready},
    peer::{Event, Peer},
    shutdown::{Halt, Shutdown},
    spawner::{spawn_plugin, StderrRoute},
};

/// How often to ping a live sink, and the window for a missing pong to count as
/// a hang. One missed interval ⇒ considered hung and torn down.
const PING_INTERVAL: Duration = Duration::from_secs(5);

/// Max times the supervisor will respawn the sink before giving up. Guards
/// against an endlessly-failing sink (e.g. permanently bad credentials).
const MAX_RESTARTS: u32 = 5;

/// Result of the sink stage.
pub struct SinkOutcome {
    pub name: String,
    pub version: String,
    pub objects_delivered: u64,
}

/// Why a sink instance stopped driving — distinguishes "all done" from a
/// recoverable failure (respawn + resume) and a halt (give up).
enum Driven {
    Done,
    Recoverable(String),
    Halted(Halt),
}

/// What every sink instance is spawned and configured with.
pub struct SinkLaunch {
    pub binary: PathBuf,
    pub config: serde_json::Value,
    pub unknown_keys: UnknownKeys,
    pub mask: loadsmith_config::MaskList,
    /// `configure.run`: the same for every respawn.
    pub run: Option<loadsmith_protocol::RunInfo>,
}

/// Runs the sink stage to completion, respawning the sink as needed.
///
/// `object_rx` yields staged files as the destination finalizes them and
/// closes when the destination's event channel reaches EOF (i.e. no more files
/// will be produced). `delivered_tx` receives the [`DeliveredPrefix`] each time
/// it advances.
pub async fn run_sink_supervisor(
    launch: SinkLaunch,
    mut object_rx: UnboundedReceiver<StagedObject>,
    delivered_tx: Option<UnboundedSender<u64>>,
    timeouts: Timeouts,
    mut shutdown: Shutdown,
) -> Result<SinkOutcome, CoreError> {
    let SinkLaunch { binary, config, unknown_keys, mask, run } = launch;
    // Ledger — lives across respawns.
    let mut ledger = Ledger { emitted: Vec::new(), delivered: HashSet::new(), prefix: DeliveredPrefix::new(delivered_tx) };
    let mut input_closed = false;
    let mut restarts: u32 = 0;
    let grace = Duration::from_secs(timeouts.cancel_grace_seconds);

    loop {
        // ── Spawn one sink instance and bring it up to Ready ──────────────
        let mut sp = spawn_plugin(&binary, None, StderrRoute::Forward(mask.clone()))
            .map_err(|e| CoreError::PluginIncompatible(format!("spawn sink: {e}")))?;
        let event = tokio::fs::File::from_std(sp.event_fd_take());
        let mut peer = Peer::new(PluginKind::Sink.as_str(), sp).with_mask(mask.clone()).with_run(run.clone());
        if restarts > 0 {
            peer.scope().respawned(restarts);
            peer.scope().event("respawn", vec![opentelemetry::KeyValue::new("loadsmith.plugin.respawns", i64::from(restarts))]);
        }
        let event_mask = mask.clone();
        let event_scope = Some(peer.scope().clone());
        let event_task = tokio::spawn(async move {
            drain_events(
                loadsmith_transport::ControlReader::new(BufReader::new(event)),
                PluginKind::Sink.as_str(),
                crate::events::EventForward { mask: event_mask, scope: event_scope, ..Default::default() },
            )
            .await;
        });

        let setup = async {
            let info = run_handshake_and_configure(
                &mut peer,
                config.clone(),
                unknown_keys,
                PluginKind::Sink,
                &timeouts,
                &mut shutdown,
            )
            .await?;
            start_and_await_ready(&mut peer, &timeouts, &mut shutdown).await?;
            Ok::<_, crate::peer::Failure>((info.name, info.version))
        }
        .await;
        let (sink_name, sink_version) = match setup {
            Ok(id) => id,
            Err(failure) => {
                let reason = peer.mask().apply(&failure.reason());
                peer.notify_abort(&reason, failure.code).await;
                peer.wind_down(grace).await;
                let _ = event_task.await;
                return Err(failure.error);
            }
        };

        // From here on a `resend` means "re-send every unacked object".
        peer.replay_on_resend = false;

        // Resume: (re-)send every object not yet acknowledged, in order.
        let pending: Vec<PathBuf> =
            ledger.pending().cloned().collect();
        if restarts > 0 && !pending.is_empty() {
            tracing::warn!(count = pending.len(), "resuming sink — re-sending unacked objects");
            peer.scope().event("retry", vec![opentelemetry::KeyValue::new("loadsmith.objects", pending.len() as i64)]);
        }
        let mut send_failed = false;
        let mut deliveries = Deliveries::default();
        for p in &pending {
            if let Err(e) = send_deliver(&mut peer, p, &mut deliveries).await {
                tracing::warn!("re-send failed: {e}");
                send_failed = true;
                break;
            }
        }

        // ── Drive this instance ───────────────────────────────────────────
        let outcome = if send_failed {
            Driven::Recoverable("re-send to fresh sink failed".into())
        } else {
            drive_instance(
                &mut peer,
                &mut object_rx,
                &mut ledger,
                &mut input_closed,
                &mut shutdown,
                &mut deliveries,
                grace,
            )
            .await
        };
        // A sink that failed while the run was halting is not respawned.
        let outcome = match (outcome, shutdown.fired()) {
            (Driven::Recoverable(_), Some(halt)) => Driven::Halted(halt),
            (outcome, _) => outcome,
        };
        deliveries.abandon();

        // Tear the instance down regardless of outcome.
        match &outcome {
            Driven::Halted(halt) => {
                let reason = match halt {
                    Halt::Signal(sig) => format!("interrupted by {}", sig.name()),
                    Halt::Abort(reason) => reason.clone(),
                };
                let reason = peer.mask().apply(&reason);
                peer.notify_abort(&reason, None).await;
                peer.wind_down(grace).await;
            }
            _ => peer.wind_down(Duration::ZERO).await,
        }
        let _ = event_task.await;

        match outcome {
            Driven::Done => {
                return Ok(SinkOutcome {
                    name: sink_name,
                    version: sink_version,
                    objects_delivered: ledger.delivered.len() as u64,
                });
            }
            Driven::Halted(halt) => {
                return Err(match halt {
                    Halt::Signal(sig) => CoreError::Interrupted(sig),
                    Halt::Abort(reason) => CoreError::PluginProcess(format!(
                        "sink cancelled ({reason}); {} of {} objects delivered",
                        ledger.delivered.len(),
                        ledger.emitted.len()
                    )),
                });
            }
            Driven::Recoverable(reason) => {
                restarts += 1;
                if restarts > MAX_RESTARTS {
                    return Err(CoreError::PluginFailed(format!(
                        "sink failed after {MAX_RESTARTS} restarts (last: {reason}); \
                         {} of {} objects delivered",
                        ledger.delivered.len(),
                        ledger.emitted.len()
                    )));
                }
                tracing::warn!(restarts, reason, "sink stopped — respawning");
                telemetry::record_respawn(PluginKind::Sink.as_str());
                // Loop respawns and resumes from the ledger.
            }
        }
    }
}

/// What the sink was handed and what it acknowledged — lives across respawns.
struct Ledger {
    /// Every object announced, in order.
    emitted: Vec<PathBuf>,
    delivered: HashSet<PathBuf>,
    prefix: DeliveredPrefix,
}

impl Ledger {
    /// The objects not yet acknowledged, in order.
    fn pending(&self) -> impl Iterator<Item = &PathBuf> {
        self.emitted.iter().filter(|p| !self.delivered.contains(*p))
    }

    fn all_delivered(&self) -> bool {
        self.emitted.iter().all(|p| self.delivered.contains(p))
    }
}

/// Fires at the halt's deadline; never when not halting.
async fn sleep_until_halt(halting: &Option<(Halt, tokio::time::Instant)>) {
    match halting {
        Some((_, deadline)) => tokio::time::sleep_until(*deadline).await,
        None => std::future::pending().await,
    }
}

/// The highest batch ordinal whose rows are all delivered: every file holding
/// batches `1..=n` was acknowledged by the sink. Files announce their batch
/// ranges in order; a file without a range stops the prefix (that
/// destination's batches count only at the end of the run).
pub(crate) struct DeliveredPrefix {
    ranges: HashMap<PathBuf, (u64, u64)>,
    through: u64,
    tx: Option<UnboundedSender<u64>>,
}

impl DeliveredPrefix {
    pub(crate) fn new(tx: Option<UnboundedSender<u64>>) -> Self {
        Self { ranges: HashMap::new(), through: 0, tx }
    }

    fn record(&mut self, object: &StagedObject) {
        if let Some(range) = object.batches {
            self.ranges.insert(object.path.clone(), range);
        }
    }

    /// Recomputes the prefix after a delivery; reports it when it advanced.
    fn update(&mut self, emitted: &[PathBuf], delivered: &HashSet<PathBuf>) {
        let mut through = 0;
        for path in emitted {
            match self.ranges.get(path) {
                Some(&(first, last)) if delivered.contains(path) && first <= through + 1 => {
                    through = through.max(last);
                }
                _ => break,
            }
        }
        if through > self.through {
            self.through = through;
            if let Some(tx) = &self.tx {
                let _ = tx.send(through);
            }
        }
    }
}

/// Drives a single live sink instance until it finishes, fails, hangs, or the
/// run is halted.
///
/// Closing the sink's stdin to signal "no more objects" is done by dropping
/// its writer ([`Peer::close_input`]): tokio's `ChildStdin` only closes its
/// pipe on drop, so a `shutdown` would leave the sink waiting for an EOF that
/// never comes.
///
/// A halt with objects still unacknowledged waits, up to `grace`, for the next
/// acknowledgement — the delivery in flight, whose object may already be at
/// the target — and sends nothing new meanwhile: an object the target holds
/// then counts as delivered (and its rows are not read again on resume).
async fn drive_instance(
    peer: &mut Peer,
    object_rx: &mut UnboundedReceiver<StagedObject>,
    ledger: &mut Ledger,
    input_closed: &mut bool,
    shutdown: &mut Shutdown,
    deliveries: &mut Deliveries,
    grace: Duration,
) -> Driven {
    let mut halting: Option<(Halt, tokio::time::Instant)> = None;
    let mut input_open = true;
    let mut awaiting_pong = false;
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.tick().await; // discard the immediate first tick

    loop {
        // Once the destination is done and every emitted object is acked, close
        // the sink's stdin so it sees EOF and finalizes.
        if *input_closed && input_open && ledger.all_delivered() {
            peer.close_input();
            input_open = false;
            tracing::debug!("all objects delivered; closed sink stdin");
        }

        tokio::select! {
            halt = shutdown.wait(), if halting.is_none() => {
                if ledger.pending().next().is_none() || grace.is_zero() {
                    return Driven::Halted(halt);
                }
                tracing::debug!("halting: waiting for the delivery in flight");
                halting = Some((halt, tokio::time::Instant::now() + grace));
            }
            _ = sleep_until_halt(&halting) => {
                let (halt, _) = halting.take().expect("guarded by sleep_until_halt");
                return Driven::Halted(halt);
            }
            maybe = object_rx.recv(), if !*input_closed => {
                match maybe {
                    Some(object) => {
                        ledger.prefix.record(&object);
                        let path = object.path;
                        if !ledger.delivered.contains(&path) && !ledger.emitted.contains(&path) {
                            ledger.emitted.push(path.clone());
                            if input_open && halting.is_none() {
                                if let Err(e) = send_deliver(peer, &path, deliveries).await {
                                    return Driven::Recoverable(format!("deliver send failed: {e}"));
                                }
                            }
                        }
                    }
                    None => {
                        *input_closed = true;
                    }
                }
            }
            inbound = peer.next_inbound() => {
                let event = match peer.process(inbound).await {
                    Ok(Some(event)) => event,
                    Ok(None) => continue,
                    Err(failure) => return Driven::Recoverable(failure.reason()),
                };
                match event {
                    Event::Msg(Message::ObjectDelivered(d)) => {
                        let path = PathBuf::from(d.path);
                        deliveries.done(&path);
                        ledger.delivered.insert(path);
                        ledger.prefix.update(&ledger.emitted, &ledger.delivered);
                        if let Some((halt, _)) = halting.take() {
                            return Driven::Halted(halt);
                        }
                    }
                    Event::Msg(Message::Pong) => {
                        awaiting_pong = false;
                    }
                    Event::Msg(Message::Finished(f)) => {
                        return match f.status {
                            FinishedStatus::Success => Driven::Done,
                            _ => Driven::Recoverable(format!(
                                "sink reported {:?}: {}",
                                f.status,
                                peer.mask().apply(&f.message.unwrap_or_default())
                            )),
                        };
                    }
                    Event::ResendRequested => {
                        if input_open {
                            let pending: Vec<PathBuf> =
                                ledger.pending().cloned().collect();
                            tracing::debug!(count = pending.len(), "sink asked for a resend");
                            for p in &pending {
                                if let Err(e) = send_deliver(peer, p, deliveries).await {
                                    return Driven::Recoverable(format!("re-send failed: {e}"));
                                }
                            }
                        }
                    }
                    Event::Msg(_) => { /* stray log/progress — ignore */ }
                    Event::Eof => {
                        return Driven::Recoverable("sink control channel closed (died)".into());
                    }
                }
            }
            _ = ping.tick(), if input_open => {
                if awaiting_pong {
                    return Driven::Recoverable("sink hung (no pong within interval)".into());
                }
                if peer.send_raw(&Message::Ping).await.is_err() {
                    return Driven::Recoverable("ping write failed".into());
                }
                awaiting_pong = true;
            }
        }
    }
}

async fn send_deliver(peer: &mut Peer, path: &Path, deliveries: &mut Deliveries) -> Result<(), CoreError> {
    deliveries.start(peer, path);
    peer.send(Message::DeliverObject(DeliverObject { path: path.to_string_lossy().into_owned() }))
        .await
}

/// The `sink.deliver` spans in flight: from `deliver_object` to the sink's
/// `object_delivered`, under the sink process's span.
#[derive(Default)]
struct Deliveries(std::collections::HashMap<PathBuf, tracing::Span>);

impl Deliveries {
    fn start(&mut self, peer: &Peer, path: &Path) {
        if !telemetry::traces_enabled() {
            return;
        }
        let span = tracing::info_span!(
            target: "loadsmith::sink",
            parent: peer.scope().span(),
            "sink.deliver",
            loadsmith.object.path = %path.display(),
            error = tracing::field::Empty,
        );
        self.0.insert(path.to_path_buf(), span);
    }

    fn done(&mut self, path: &Path) {
        self.0.remove(path);
    }

    /// The instance is gone: what it did not acknowledge failed (it is
    /// re-sent to the next instance, under a new span).
    fn abandon(&mut self) {
        for (_, span) in self.0.drain() {
            span.record("error", "not acknowledged by the sink");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn staged(name: &str, batches: Option<(u64, u64)>) -> StagedObject {
        StagedObject { path: PathBuf::from(name), batches }
    }

    /// Marks `name` delivered; returns the prefixes reported.
    fn deliver(ledger: &mut Ledger, name: &str, rx: &mut UnboundedReceiver<u64>) -> Vec<u64> {
        ledger.delivered.insert(PathBuf::from(name));
        ledger.prefix.update(&ledger.emitted, &ledger.delivered);
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    fn ledger(objects: &[StagedObject]) -> (Ledger, UnboundedReceiver<u64>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut ledger =
            Ledger { emitted: Vec::new(), delivered: HashSet::new(), prefix: DeliveredPrefix::new(Some(tx)) };
        for o in objects {
            ledger.prefix.record(o);
            ledger.emitted.push(o.path.clone());
        }
        (ledger, rx)
    }

    #[test]
    fn the_prefix_waits_for_every_earlier_file() {
        let (mut l, mut rx) = ledger(&[staged("a", Some((1, 3))), staged("b", Some((4, 6))), staged("c", Some((7, 7)))]);
        assert_eq!(deliver(&mut l, "b", &mut rx), Vec::<u64>::new(), "a is still pending");
        assert_eq!(deliver(&mut l, "a", &mut rx), vec![6]);
        assert_eq!(deliver(&mut l, "c", &mut rx), vec![7]);
    }

    #[test]
    fn a_file_without_a_range_stops_the_prefix() {
        let (mut l, mut rx) = ledger(&[staged("a", Some((1, 2))), staged("b", None), staged("c", Some((5, 6)))]);
        assert_eq!(deliver(&mut l, "a", &mut rx), vec![2]);
        assert_eq!(deliver(&mut l, "c", &mut rx), Vec::<u64>::new());
        assert_eq!(deliver(&mut l, "b", &mut rx), Vec::<u64>::new(), "b's batches are unknown");
    }

    #[test]
    fn a_gap_between_ranges_stops_the_prefix() {
        let (mut l, mut rx) = ledger(&[staged("a", Some((1, 2))), staged("b", Some((4, 5)))]);
        assert_eq!(deliver(&mut l, "a", &mut rx), vec![2]);
        assert_eq!(deliver(&mut l, "b", &mut rx), Vec::<u64>::new(), "batch 3 is in no file");
    }
}
