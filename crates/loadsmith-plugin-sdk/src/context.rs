use loadsmith_protocol::{Checkpoint, Committed, LogEvent, LogLevel, Message, ObjectParsed, ObjectReady, Progress};
use loadsmith_transport::EventWriter;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::telemetry;

/// Helper passed to plugins for sending logs and progress to fd4.
///
/// It is also the way out for the plugin's telemetry: every send first
/// writes the spans and logs recorded since the previous one (see
/// [`telemetry`](crate::telemetry)), and the aggregated metrics when their
/// flush interval has passed.
pub struct EventSender<W> {
    writer: EventWriter<W>,
}

impl EventSender<tokio::fs::File> {
    /// The event channel of a plugin process: fd4, which the core wires up.
    pub(crate) fn fd4() -> Self {
        use std::os::unix::io::FromRawFd;
        // SAFETY: the core always opens fd4 for a plugin; the SDK takes it
        // exactly once per process.
        let event_fd = unsafe { std::fs::File::from_raw_fd(4) };
        Self::new(tokio::fs::File::from_std(event_fd))
    }
}

impl<W: AsyncWrite + Unpin> EventSender<W> {
    pub fn new(writer: W) -> Self {
        Self { writer: EventWriter::new(writer) }
    }

    /// Writes one message after the telemetry recorded so far. Errors writing
    /// to the event channel are non-fatal — the pipeline continues; the
    /// observation is just lost.
    async fn send(&mut self, msg: &Message) {
        self.write_telemetry(false).await;
        let _ = self.writer.send(msg).await;
    }

    async fn write_telemetry(&mut self, force_metrics: bool) {
        for msg in telemetry::GLOBAL.take_pending() {
            let _ = self.writer.send(&msg).await;
        }
        if let Some(metrics) = telemetry::GLOBAL.take_metrics(force_metrics) {
            let _ = self.writer.send(&metrics).await;
        }
    }

    /// Writes the spans and logs recorded so far, and the metrics if their
    /// flush interval has passed. The SDK calls it between units of work; a
    /// plugin with a long quiet phase may call it too.
    pub async fn flush_telemetry(&mut self) {
        self.write_telemetry(false).await;
    }

    /// The last write of the process: everything recorded, metrics
    /// included, then a flush.
    pub(crate) async fn close(&mut self) {
        self.write_telemetry(true).await;
    }

    /// A log line. With telemetry on it carries the trace context of the
    /// current `tracing` span.
    pub async fn log(&mut self, level: LogLevel, message: impl Into<String>) {
        let mut log = LogEvent::new(level, message);
        if let Some((trace_id, span_id)) = telemetry::current_context() {
            log.trace_id = Some(trace_id);
            log.span_id = Some(span_id);
            log.time_unix_nano = Some(telemetry::unix_nanos());
        }
        self.send(&Message::Log(log)).await;
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
        self.send(&msg).await;
    }

    pub async fn progress_destination(&mut self, rows_written: u64, batches_written: u64) {
        let msg = Message::Progress(Progress {
            rows_read: None,
            batches_read: None,
            rows_written: Some(rows_written),
            batches_written: Some(batches_written),
        });
        self.send(&msg).await;
    }

    /// Announce that a staged file is finalized and ready for delivery. Sent on
    /// the event plane (fd4) so it overlaps the live data pump — the core's
    /// event drain forwards it to the sink supervisor.
    pub async fn object_ready(&mut self, path: impl Into<String>) {
        self.object_ready_with(path, None).await;
    }

    /// Destination → core: a staged file is finalized, holding the batches
    /// `batches` (when the destination tracks them).
    pub async fn object_ready_with(&mut self, path: impl Into<String>, batches: Option<loadsmith_protocol::BatchRange>) {
        let msg = Message::ObjectReady(ObjectReady { path: path.into(), batches });
        self.send(&msg).await;
    }

    /// Parser → core: a file was fully parsed into `batches` Arrow batches
    /// (`rows` rows); `last_batch_seq` is the global ordinal of its last
    /// batch (`0` when it produced none). Informational.
    pub async fn object_parsed(&mut self, path: impl Into<String>, rows: u64, batches: u64, last_batch_seq: u64) {
        let msg = Message::ObjectParsed(ObjectParsed { path: path.into(), rows, batches, last_batch_seq });
        self.send(&msg).await;
    }

    /// Source → core: the high watermark of the cursor column produced through
    /// `batch_seq`. Sent on fd4 so it overlaps the pump; the core's event drain
    /// forwards it to the state supervisor.
    pub async fn checkpoint(&mut self, batch_seq: u64, cursor_value: serde_json::Value) {
        let msg = Message::Checkpoint(Checkpoint { cursor_value, batch_seq });
        self.send(&msg).await;
    }

    /// Destination → core: everything through `batch_seq` is durably committed.
    pub async fn committed(&mut self, batch_seq: u64) {
        let msg = Message::Committed(Committed { batch_seq });
        self.send(&msg).await;
    }
}

/// Flush any remaining buffered output.
pub async fn flush_writer<W: AsyncWrite + Unpin>(w: &mut W) {
    let _ = w.flush().await;
}
