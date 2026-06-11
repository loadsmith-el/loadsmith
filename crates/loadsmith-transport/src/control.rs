use loadsmith_protocol::Message;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use crate::TransportError;

pub struct ControlWriter<W> {
    inner: W,
}

impl<W: AsyncWrite + Unpin> ControlWriter<W> {
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

    /// Closes the underlying writer. For a child's stdin this signals EOF to the
    /// plugin — used to tell a sink that no more objects will be delivered.
    pub async fn shutdown(&mut self) -> Result<(), TransportError> {
        self.inner.shutdown().await?;
        Ok(())
    }
}

pub struct ControlReader<R> {
    inner: R,
    buf: String,
}

impl<R: AsyncBufRead + Unpin> ControlReader<R> {
    pub fn new(reader: R) -> Self {
        Self { inner: reader, buf: String::new() }
    }

    /// Returns `Ok(None)` on clean EOF.
    pub async fn recv(&mut self) -> Result<Option<Message>, TransportError> {
        self.buf.clear();
        let n = self.inner.read_line(&mut self.buf).await?;
        if n == 0 {
            return Ok(None);
        }
        let trimmed = self.buf.trim();
        if trimmed.is_empty() {
            // Skip blank lines, try again
            return Box::pin(self.recv()).await;
        }
        let msg = serde_json::from_str(trimmed)?;
        Ok(Some(msg))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use loadsmith_protocol::{Cancel, Message};
    use tokio::io::BufReader;

    #[tokio::test]
    async fn send_recv_roundtrip() {
        let (client, server) = tokio::io::duplex(4096);
        let (server_read, _server_write) = tokio::io::split(server);
        let (_client_read, client_write) = tokio::io::split(client);

        let mut writer = ControlWriter::new(client_write);
        let mut reader = ControlReader::new(BufReader::new(server_read));

        let msg = Message::Cancel(Cancel { reason: "test".into() });
        writer.send(&msg).await.unwrap();

        let received = reader.recv().await.unwrap().unwrap();
        assert!(matches!(received, Message::Cancel(_)));
    }

    #[tokio::test]
    async fn multiple_messages_in_sequence() {
        let (client, server) = tokio::io::duplex(4096);
        let (server_read, _) = tokio::io::split(server);
        let (_, client_write) = tokio::io::split(client);

        let mut writer = ControlWriter::new(client_write);
        let mut reader = ControlReader::new(BufReader::new(server_read));

        writer.send(&Message::Ping).await.unwrap();
        writer.send(&Message::Pong).await.unwrap();
        writer.send(&Message::Start(Default::default())).await.unwrap();

        assert!(matches!(reader.recv().await.unwrap().unwrap(), Message::Ping));
        assert!(matches!(reader.recv().await.unwrap().unwrap(), Message::Pong));
        assert!(matches!(reader.recv().await.unwrap().unwrap(), Message::Start(_)));
    }

    #[tokio::test]
    async fn eof_returns_none() {
        let (client, server) = tokio::io::duplex(4096);
        let (server_read, _) = tokio::io::split(server);
        let (_, client_write) = tokio::io::split(client);

        // Write one message then drop the writer to close the stream
        let mut writer = ControlWriter::new(client_write);
        writer.send(&Message::Ping).await.unwrap();
        drop(writer);

        let mut reader = ControlReader::new(BufReader::new(server_read));
        assert!(reader.recv().await.unwrap().is_some());
        assert!(reader.recv().await.unwrap().is_none());
    }
}
