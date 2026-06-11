use loadsmith_protocol::{Checkpoint, Committed, LogLevel, Message, ObjectReady, Progress};
use loadsmith_transport::EventWriter;
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// Helper passed to plugins for sending logs and progress to fd4.
pub struct EventSender<W> {
    writer: EventWriter<W>,
}

impl<W: AsyncWrite + Unpin> EventSender<W> {
    pub fn new(writer: W) -> Self {
        Self { writer: EventWriter::new(writer) }
    }

    pub async fn log(&mut self, level: LogLevel, message: impl Into<String>) {
        // Errors writing to the event channel are non-fatal — the pipeline
        // continues; the observation is just lost.
        let _ = self.writer.log(level, message).await;
    }

    pub async fn info(&mut self, message: impl Into<String>) {
        self.log(LogLevel::Info, message).await;
    }

    pub async fn warn(&mut self, message: impl Into<String>) {
        self.log(LogLevel::Warn, message).await;
    }

    pub async fn error_msg(&mut self, message: impl Into<String>) {
        self.log(LogLevel::Error, message).await;
    }

    pub async fn progress_source(&mut self, rows_read: u64, batches_read: u64) {
        let msg = Message::Progress(Progress {
            rows_read: Some(rows_read),
            batches_read: Some(batches_read),
            rows_written: None,
            batches_written: None,
        });
        let _ = self.writer.send(&msg).await;
    }

    pub async fn progress_destination(&mut self, rows_written: u64, batches_written: u64) {
        let msg = Message::Progress(Progress {
            rows_read: None,
            batches_read: None,
            rows_written: Some(rows_written),
            batches_written: Some(batches_written),
        });
        let _ = self.writer.send(&msg).await;
    }

    /// Announce that a staged file is finalized and ready for delivery. Sent on
    /// the event plane (fd4) so it overlaps the live data pump — the core's
    /// event drain forwards it to the sink supervisor.
    pub async fn object_ready(&mut self, path: impl Into<String>) {
        let msg = Message::ObjectReady(ObjectReady { path: path.into() });
        let _ = self.writer.send(&msg).await;
    }

    /// Source → core: the high watermark of the cursor column produced through
    /// `batch_seq`. Sent on fd4 so it overlaps the pump; the core's event drain
    /// forwards it to the state supervisor.
    pub async fn checkpoint(&mut self, batch_seq: u64, cursor_value: serde_json::Value) {
        let msg = Message::Checkpoint(Checkpoint { cursor_value, batch_seq });
        let _ = self.writer.send(&msg).await;
    }

    /// Destination → core: everything through `batch_seq` is durably committed.
    pub async fn committed(&mut self, batch_seq: u64) {
        let msg = Message::Committed(Committed { batch_seq });
        let _ = self.writer.send(&msg).await;
    }
}

/// Flush any remaining buffered output.
pub async fn flush_writer<W: AsyncWrite + Unpin>(w: &mut W) {
    let _ = w.flush().await;
}
