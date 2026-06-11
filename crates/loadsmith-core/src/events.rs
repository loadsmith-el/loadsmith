use std::path::PathBuf;

use loadsmith_protocol::{LogLevel, Message};
use loadsmith_transport::ControlReader;
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
            Ok(Some(Message::Log(log))) => match log.level {
                LogLevel::Trace => tracing::trace!(target: "plugin", plugin, "{}", log.message),
                LogLevel::Debug => tracing::debug!(target: "plugin", plugin, "{}", log.message),
                LogLevel::Info => tracing::info!(target: "plugin", plugin, "{}", log.message),
                LogLevel::Warn => tracing::warn!(target: "plugin", plugin, "{}", log.message),
                LogLevel::Error => tracing::error!(target: "plugin", plugin, "{}", log.message),
            },
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
            Ok(Some(_)) | Ok(None) => break,
            Err(_) => break,
        }
    }
}
