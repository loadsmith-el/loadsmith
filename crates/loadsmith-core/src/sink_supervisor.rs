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
//! state and plugins are memoryless tools.
//!
//! Cancellation-safety note: `ControlReader::recv` clears its line buffer each
//! call and is therefore *not* cancel-safe. We never poll it inside `select!`;
//! instead each sink instance gets a dedicated reader task that forwards parsed
//! messages over an mpsc, and the supervisor selects only over cancel-safe
//! channels (`mpsc`, `interval`).

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use loadsmith_protocol::{DeliverObject, FinishedStatus, Message, PluginKind};
use loadsmith_transport::{ControlReader, ControlWriter};
use tokio::io::BufReader;
use tokio::process::ChildStdin;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::{
    error::CoreError,
    events::drain_events,
    lifecycle::{run_handshake_and_configure, start_sink},
    spawner::spawn_plugin,
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
    pub objects_delivered: u64,
}

/// Why a sink instance stopped driving — distinguishes "all done" from a
/// recoverable failure (respawn + resume) and a fatal one (give up).
enum Driven {
    Done,
    Recoverable(String),
}

/// Runs the sink stage to completion, respawning the sink as needed.
///
/// `object_rx` yields staged file paths as the destination finalizes them and
/// closes when the destination's event channel reaches EOF (i.e. no more files
/// will be produced).
pub async fn run_sink_supervisor(
    binary: PathBuf,
    config: serde_json::Value,
    mut object_rx: UnboundedReceiver<PathBuf>,
) -> Result<SinkOutcome, CoreError> {
    // Ledger — lives across respawns.
    let mut emitted: Vec<PathBuf> = Vec::new();
    let mut delivered: HashSet<PathBuf> = HashSet::new();
    let mut input_closed = false;
    let mut restarts: u32 = 0;

    loop {
        let sink_name;
        // ── Spawn one sink instance and bring it up to Ready ──────────────
        let mut sp = spawn_plugin(&binary, None)
            .map_err(|e| CoreError::PluginProcess(format!("spawn sink: {e}")))?;
        let event = tokio::fs::File::from_std(sp.event_fd_take());
        let event_task = tokio::spawn(async move {
            drain_events(
                ControlReader::new(BufReader::new(event)),
                "sink",
                crate::events::EventForward::default(),
            )
            .await;
        });
        let crate::spawner::SpawnedPlugin { mut child, control_in, control_out, .. } = sp;

        // Handshake/configure/start synchronously over borrowed streams. The
        // sink sends nothing after Ready until we send a DeliverObject, so no
        // bytes are buffered past Ready when we hand control_out to the reader
        // task below.
        let mut control_in = control_in;
        let mut control_out = control_out;
        {
            let mut reader = ControlReader::new(BufReader::new(&mut control_out));
            let mut writer = ControlWriter::new(&mut control_in);
            let info = run_handshake_and_configure(
                &mut reader,
                &mut writer,
                config.clone(),
                PluginKind::Sink,
            )
            .await?;
            start_sink(&mut reader, &mut writer).await?;
            sink_name = info.name;
        }

        // Dedicated reader task: forwards parsed control messages so the
        // supervisor's select! only touches cancel-safe channels.
        let (ctrl_tx, ctrl_rx) = tokio::sync::mpsc::unbounded_channel();
        let reader_task = tokio::spawn(async move {
            let mut r = ControlReader::new(BufReader::new(control_out));
            while let Ok(Some(m)) = r.recv().await {
                if ctrl_tx.send(m).is_err() {
                    break;
                }
            }
        });

        let mut writer = ControlWriter::new(control_in);

        // Resume: (re-)send every object not yet acknowledged, in order.
        let pending: Vec<PathBuf> =
            emitted.iter().filter(|p| !delivered.contains(*p)).cloned().collect();
        if restarts > 0 && !pending.is_empty() {
            tracing::warn!(count = pending.len(), "resuming sink — re-sending unacked objects");
        }
        let mut send_failed = false;
        for p in &pending {
            if let Err(e) = send_deliver(&mut writer, p).await {
                tracing::warn!("re-send failed: {e}");
                send_failed = true;
                break;
            }
        }

        // ── Drive this instance ───────────────────────────────────────────
        // `writer` is moved in so that dropping it (not `shutdown`, which does
        // not close a tokio ChildStdin pipe) signals EOF to the sink.
        let outcome = if send_failed {
            Driven::Recoverable("re-send to fresh sink failed".into())
        } else {
            drive_instance(
                writer,
                ctrl_rx,
                &mut object_rx,
                &mut emitted,
                &mut delivered,
                &mut input_closed,
            )
            .await
        };

        // Tear the instance down regardless of outcome.
        let _ = child.start_kill();
        let _ = child.wait().await;
        reader_task.abort();
        let _ = event_task.await;

        match outcome {
            Driven::Done => {
                return Ok(SinkOutcome {
                    name: sink_name,
                    objects_delivered: delivered.len() as u64,
                });
            }
            Driven::Recoverable(reason) => {
                restarts += 1;
                if restarts > MAX_RESTARTS {
                    return Err(CoreError::PluginProcess(format!(
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

/// Drives a single live sink instance until it finishes, fails, or hangs.
///
/// Takes `writer` by value: closing the sink's stdin to signal "no more objects"
/// is done by **dropping** the writer (`writer = None`). `ControlWriter::shutdown`
/// would not work here — tokio's `ChildStdin` only closes its pipe on drop, so a
/// shutdown leaves the sink blocked on `read_line` waiting for an EOF that never
/// comes.
async fn drive_instance(
    writer: ControlWriter<ChildStdin>,
    mut ctrl_rx: UnboundedReceiver<Message>,
    object_rx: &mut UnboundedReceiver<PathBuf>,
    emitted: &mut Vec<PathBuf>,
    delivered: &mut HashSet<PathBuf>,
    input_closed: &mut bool,
) -> Driven {
    let mut writer = Some(writer);
    let mut awaiting_pong = false;
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.tick().await; // discard the immediate first tick

    loop {
        // Once the destination is done and every emitted object is acked, close
        // the sink's stdin by dropping the writer so it sees EOF and finalizes.
        if *input_closed && writer.is_some() && emitted.iter().all(|p| delivered.contains(p)) {
            writer = None;
            tracing::debug!("all objects delivered; closed sink stdin");
        }

        tokio::select! {
            maybe = object_rx.recv(), if !*input_closed => {
                match maybe {
                    Some(path) => {
                        if !delivered.contains(&path) && !emitted.contains(&path) {
                            emitted.push(path.clone());
                            if let Some(w) = writer.as_mut() {
                                if let Err(e) = send_deliver(w, &path).await {
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
            msg = ctrl_rx.recv() => {
                match msg {
                    Some(Message::ObjectDelivered(d)) => {
                        delivered.insert(PathBuf::from(d.path));
                    }
                    Some(Message::Pong) => {
                        awaiting_pong = false;
                    }
                    Some(Message::Finished(f)) => {
                        return match f.status {
                            FinishedStatus::Success => Driven::Done,
                            _ => Driven::Recoverable(format!(
                                "sink reported {:?}: {}",
                                f.status,
                                f.message.unwrap_or_default()
                            )),
                        };
                    }
                    Some(_) => { /* stray log/progress — ignore */ }
                    None => {
                        return Driven::Recoverable("sink control channel closed (died)".into());
                    }
                }
            }
            _ = ping.tick(), if writer.is_some() => {
                if awaiting_pong {
                    return Driven::Recoverable("sink hung (no pong within interval)".into());
                }
                if let Some(w) = writer.as_mut() {
                    if w.send(&Message::Ping).await.is_err() {
                        return Driven::Recoverable("ping write failed".into());
                    }
                    awaiting_pong = true;
                }
            }
        }
    }
}

async fn send_deliver(
    writer: &mut ControlWriter<ChildStdin>,
    path: &Path,
) -> Result<(), CoreError> {
    writer
        .send(&Message::DeliverObject(DeliverObject {
            path: path.to_string_lossy().into_owned(),
        }))
        .await
        .map_err(|e| CoreError::Protocol(format!("deliver_object: {e}")))
}
