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

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use loadsmith_config::Timeouts;
use loadsmith_protocol::{DeliverObject, FinishedStatus, Message, PluginKind, UnknownKeys};
use tokio::io::BufReader;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::{
    error::CoreError,
    events::drain_events,
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

/// Runs the sink stage to completion, respawning the sink as needed.
///
/// `object_rx` yields staged file paths as the destination finalizes them and
/// closes when the destination's event channel reaches EOF (i.e. no more files
/// will be produced).
pub async fn run_sink_supervisor(
    binary: PathBuf,
    config: serde_json::Value,
    unknown_keys: UnknownKeys,
    mut object_rx: UnboundedReceiver<PathBuf>,
    timeouts: Timeouts,
    mut shutdown: Shutdown,
    mask: loadsmith_config::MaskList,
) -> Result<SinkOutcome, CoreError> {
    // Ledger — lives across respawns.
    let mut emitted: Vec<PathBuf> = Vec::new();
    let mut delivered: HashSet<PathBuf> = HashSet::new();
    let mut input_closed = false;
    let mut restarts: u32 = 0;
    let grace = Duration::from_secs(timeouts.cancel_grace_seconds);

    loop {
        // ── Spawn one sink instance and bring it up to Ready ──────────────
        let mut sp = spawn_plugin(&binary, None, StderrRoute::Forward(mask.clone()))
            .map_err(|e| CoreError::PluginIncompatible(format!("spawn sink: {e}")))?;
        let event = tokio::fs::File::from_std(sp.event_fd_take());
        let event_mask = mask.clone();
        let event_task = tokio::spawn(async move {
            drain_events(
                loadsmith_transport::ControlReader::new(BufReader::new(event)),
                PluginKind::Sink.as_str(),
                crate::events::EventForward { mask: event_mask, ..Default::default() },
            )
            .await;
        });
        let mut peer = Peer::new(PluginKind::Sink.as_str(), sp).with_mask(mask.clone());

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
            emitted.iter().filter(|p| !delivered.contains(*p)).cloned().collect();
        if restarts > 0 && !pending.is_empty() {
            tracing::warn!(count = pending.len(), "resuming sink — re-sending unacked objects");
        }
        let mut send_failed = false;
        for p in &pending {
            if let Err(e) = send_deliver(&mut peer, p).await {
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
                &mut emitted,
                &mut delivered,
                &mut input_closed,
                &mut shutdown,
            )
            .await
        };

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
                    objects_delivered: delivered.len() as u64,
                });
            }
            Driven::Halted(halt) => {
                return Err(match halt {
                    Halt::Signal(sig) => CoreError::Interrupted(sig),
                    Halt::Abort(reason) => CoreError::PluginProcess(format!(
                        "sink cancelled ({reason}); {} of {} objects delivered",
                        delivered.len(),
                        emitted.len()
                    )),
                });
            }
            Driven::Recoverable(reason) => {
                restarts += 1;
                if restarts > MAX_RESTARTS {
                    return Err(CoreError::PluginFailed(format!(
                        "sink failed after {MAX_RESTARTS} restarts (last: {reason}); \
                         {} of {} objects delivered",
                        delivered.len(),
                        emitted.len()
                    )));
                }
                tracing::warn!(restarts, reason, "sink stopped — respawning");
                // Loop respawns and resumes from the ledger.
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
async fn drive_instance(
    peer: &mut Peer,
    object_rx: &mut UnboundedReceiver<PathBuf>,
    emitted: &mut Vec<PathBuf>,
    delivered: &mut HashSet<PathBuf>,
    input_closed: &mut bool,
    shutdown: &mut Shutdown,
) -> Driven {
    let mut input_open = true;
    let mut awaiting_pong = false;
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.tick().await; // discard the immediate first tick

    loop {
        // Once the destination is done and every emitted object is acked, close
        // the sink's stdin so it sees EOF and finalizes.
        if *input_closed && input_open && emitted.iter().all(|p| delivered.contains(p)) {
            peer.close_input();
            input_open = false;
            tracing::debug!("all objects delivered; closed sink stdin");
        }

        tokio::select! {
            halt = shutdown.wait() => return Driven::Halted(halt),
            maybe = object_rx.recv(), if !*input_closed => {
                match maybe {
                    Some(path) => {
                        if !delivered.contains(&path) && !emitted.contains(&path) {
                            emitted.push(path.clone());
                            if input_open {
                                if let Err(e) = send_deliver(peer, &path).await {
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
                        delivered.insert(PathBuf::from(d.path));
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
                                emitted.iter().filter(|p| !delivered.contains(*p)).cloned().collect();
                            tracing::debug!(count = pending.len(), "sink asked for a resend");
                            for p in &pending {
                                if let Err(e) = send_deliver(peer, p).await {
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

async fn send_deliver(peer: &mut Peer, path: &Path) -> Result<(), CoreError> {
    peer.send(Message::DeliverObject(DeliverObject { path: path.to_string_lossy().into_owned() }))
        .await
}
