use loadsmith_protocol::{LogEvent, LogLevel, Message};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::TransportError;

/// Writes structured log/event messages to fd4 (event channel).
pub struct EventWriter<W> {
    inner: W,
}

impl<W: AsyncWrite + Unpin> EventWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { inner: writer }
    }

    pub async fn send(&mut self, msg: &Message) -> Result<(), TransportError> {
        let mut line = serde_json::to_string(msg)?;
        line.push('\n');
        self.inner.write_all(line.as_bytes()).await?;
        self.inner.flush().await?;
        Ok(())
    }

    pub async fn log(
        &mut self,
        level: LogLevel,
        message: impl Into<String>,
    ) -> Result<(), TransportError> {
        self.send(&Message::Log(LogEvent { level, message: message.into() })).await
    }

    pub async fn info(&mut self, message: impl Into<String>) -> Result<(), TransportError> {
        self.log(LogLevel::Info, message).await
    }

    pub async fn warn(&mut self, message: impl Into<String>) -> Result<(), TransportError> {
        self.log(LogLevel::Warn, message).await
    }

    pub async fn error_msg(&mut self, message: impl Into<String>) -> Result<(), TransportError> {
        self.log(LogLevel::Error, message).await
    }
}
