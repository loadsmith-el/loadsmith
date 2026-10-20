use std::path::PathBuf;

use loadsmith_config::MaskList;
use loadsmith_protocol::{LogEvent, LogLevel, Message};
use loadsmith_transport::{ControlReader, TransportError};
use tokio::io::AsyncBufRead;
use tokio::sync::mpsc::UnboundedSender;

/// Channels the event drain forwards selected events onto. All optional and
/// unbounded by design: the drain must never block, or a full fd4 would stall
/// the plugin and deadlock the pump. A closed receiver is ignored (the
/// corresponding supervisor has exited).
#[derive(Default)]
pub struct EventForward {
    /// `ObjectReady` paths → sink supervisor.
    pub object_tx: Option<UnboundedSender<PathBuf>>,
    /// `Checkpoint` (batch_seq, cursor_value) → state supervisor. Source drain.
    pub checkpoint_tx: Option<UnboundedSender<(u64, serde_json::Value)>>,
    /// `Committed` batch_seq → state supervisor. Destination drain.
    pub committed_tx: Option<UnboundedSender<u64>>,
    /// Resolved template values to redact from the plugin's log lines and
    /// from anything else this drain prints. A shared handle: values added
    /// later are masked too.
    pub mask: MaskList,
}

/// Logs one plugin log line through `tracing`, masked.
pub fn emit_log(plugin: &str, log: &LogEvent, mask: &MaskList) {
    let message = mask.apply(&log.message);
    match log.level {
        LogLevel::Trace => tracing::trace!(target: "plugin", plugin, "{message}"),
        LogLevel::Debug => tracing::debug!(target: "plugin", plugin, "{message}"),
        LogLevel::Info => tracing::info!(target: "plugin", plugin, "{message}"),
        LogLevel::Warn => tracing::warn!(target: "plugin", plugin, "{message}"),
        LogLevel::Error => tracing::error!(target: "plugin", plugin, "{message}"),
    }
}

/// At most this many held log lines are kept by [`hold_events`].
const MAX_HELD_LOGS: usize = 10_000;

/// Drains a plugin's fd4 to the end (so the plugin never blocks on it) and
/// returns its log lines instead of logging them: for a provider, whose
/// values the core learns only from its replies — a log line written before
/// the core read the matching `resolved` could not be masked yet. Log them
/// with [`emit_log`] once the conversation is over. Other events are
/// ignored (a provider has none).
pub async fn hold_events<R: AsyncBufRead + Unpin>(mut reader: ControlReader<R>) -> Vec<LogEvent> {
    let mut logs = Vec::new();
    loop {
        match reader.recv().await {
            Ok(Some(Message::Log(log))) => {
                if logs.len() < MAX_HELD_LOGS {
                    logs.push(log);
                }
            }
            Ok(Some(_)) | Err(TransportError::Serialize(_)) => {}
            Ok(None) | Err(_) => break,
        }
    }
    logs
}

/// Drains the log/progress event channel (fd4) of a plugin, forwarding to
/// `tracing` (stderr, controlled by `--log-level`) and routing structured
/// events (`ObjectReady`, `Checkpoint`, `Committed`) to the supervisors via
/// `forward`.
///
/// Draining is mandatory: with loadsmith in the data path, a plugin that blocks
/// writing to a full fd4 pipe would stall the whole pipeline. Running this
/// concurrently keeps fd4 flowing.
pub async fn drain_events<R: AsyncBufRead + Unpin>(
    mut reader: ControlReader<R>,
    plugin: &str,
    forward: EventForward,
) {
    loop {
        match reader.recv().await {
            Ok(Some(Message::Log(log))) => emit_log(plugin, &log, &forward.mask),
            Ok(Some(Message::Progress(p))) => {
                tracing::debug!(
                    plugin,
                    rows_read = ?p.rows_read,
                    rows_written = ?p.rows_written,
                    "plugin progress"
                );
            }
            Ok(Some(Message::ObjectReady(o))) => {
                tracing::debug!(plugin, path = %o.path, "object ready for delivery");
                if let Some(tx) = &forward.object_tx {
                    let _ = tx.send(PathBuf::from(o.path));
                }
            }
            Ok(Some(Message::Checkpoint(c))) => {
                tracing::debug!(plugin, batch_seq = c.batch_seq, "source checkpoint");
                if let Some(tx) = &forward.checkpoint_tx {
                    let _ = tx.send((c.batch_seq, c.cursor_value));
                }
            }
            Ok(Some(Message::Committed(c))) => {
                tracing::debug!(plugin, batch_seq = c.batch_seq, "destination durable commit");
                if let Some(tx) = &forward.committed_tx {
                    let _ = tx.send(c.batch_seq);
                }
            }
            // A message type that is valid elsewhere but meaningless on fd4.
            // Never stop draining over it: a stopped drain lets the pipe fill
            // and blocks the plugin forever.
            Ok(Some(other)) => {
                let shown = forward.mask.apply(&format!("{other:?}"));
                tracing::warn!(plugin, "ignoring unexpected message on the event channel: {shown}");
            }
            Ok(None) => break,
            // Malformed line (bad JSON, bad UTF-8, invalid body for a known
            // type): the reader has already consumed it, so warn and keep
            // draining. Unknown message types never get here — the reader
            // skips them (tolerant-reader rule).
            Err(TransportError::Serialize(e)) => {
                let shown = forward.mask.apply(&e.to_string());
                tracing::warn!(plugin, "skipping malformed event line: {shown}");
            }
            // The pipe itself failed: nothing more can be read from it.
            Err(e) => {
                tracing::warn!(plugin, "event channel read failed, stopping drain: {e}");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    /// Regression: the drain used to `break` on the first unknown or malformed
    /// line, after which fd4 filled up and the plugin blocked forever.
    #[tokio::test]
    async fn drain_survives_unknown_and_malformed_lines() {
        let mut input = String::new();
        input.push_str("{\"type\":\"from_the_future\",\"x\":1}\n");
        input.push_str("this is not json\n");
        input.push_str("{\"type\":\"checkpoint\"}\n"); // known type, invalid body
        input.push_str("{\"type\":\"ping\"}\n"); // valid, but not an fd4 event
        for i in 0..1000 {
            input.push_str(&format!("{{\"type\":\"object_ready\",\"path\":\"/tmp/o{i}\"}}\n"));
        }
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let forward = EventForward { object_tx: Some(tx), ..Default::default() };
        drain_events(ControlReader::new(BufReader::new(input.as_bytes())), "test", forward).await;

        let mut got = 0;
        while let Ok(p) = rx.try_recv() {
            assert_eq!(p, PathBuf::from(format!("/tmp/o{got}")));
            got += 1;
        }
        assert_eq!(got, 1000);
    }

    #[tokio::test]
    async fn drain_survives_invalid_utf8() {
        let mut input: Vec<u8> = b"{\"type\":\"log\",\"level\":\"info\",\"message\":\"".to_vec();
        input.extend_from_slice(&[0xff, 0xfe]);
        input.extend_from_slice(b"\"}\n{\"type\":\"object_ready\",\"path\":\"/tmp/after\"}\n");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let forward = EventForward { object_tx: Some(tx), ..Default::default() };
        drain_events(ControlReader::new(BufReader::new(&input[..])), "test", forward).await;
        assert_eq!(rx.try_recv().unwrap(), PathBuf::from("/tmp/after"));
    }
}
