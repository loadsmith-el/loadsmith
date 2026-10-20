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
    buf: Vec<u8>,
}

impl<R: AsyncBufRead + Unpin> ControlReader<R> {
    pub fn new(reader: R) -> Self {
        Self { inner: reader, buf: Vec::new() }
    }

    /// Reads the next message. Returns `Ok(None)` on clean EOF.
    ///
    /// Implements the protocol's **tolerant-reader** rule:
    /// - blank lines are skipped;
    /// - a well-formed JSON object whose `type` this build does not know is
    ///   logged (`warn`) and skipped — a newer peer may send message types an
    ///   older one has never heard of;
    /// - a line that is not valid JSON (or not valid UTF-8), lacks a `type`, or
    ///   carries a known `type` with an invalid body is returned as
    ///   [`TransportError::Serialize`]. The line is consumed either way, so a
    ///   caller that chooses to tolerate the error (the core's fd4 event drain
    ///   does) can simply call `recv` again.
    pub async fn recv(&mut self) -> Result<Option<Message>, TransportError> {
        loop {
            self.buf.clear();
            let n = self.inner.read_until(b'\n', &mut self.buf).await?;
            if n == 0 {
                return Ok(None);
            }
            let line = self.buf.trim_ascii();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_slice::<Message>(line)? {
                Message::Unknown => {
                    tracing::warn!(
                        message_type = unknown_type_name(line).as_deref().unwrap_or("?"),
                        "skipping protocol message of unknown type (sent by a newer peer?)"
                    );
                }
                msg => return Ok(Some(msg)),
            }
        }
    }
}

/// The `type` of an already-parsed line, for the unknown-type warning.
fn unknown_type_name(line: &[u8]) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct Tag {
        #[serde(rename = "type")]
        ty: String,
    }
    serde_json::from_slice::<Tag>(line).ok().map(|t| t.ty)
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

    fn reader_over(bytes: &'static [u8]) -> ControlReader<BufReader<&'static [u8]>> {
        ControlReader::new(BufReader::new(bytes))
    }

    #[tokio::test]
    async fn unknown_message_type_is_skipped() {
        let mut reader = reader_over(
            b"{\"type\":\"from_the_future\",\"payload\":{\"a\":1}}\n\n{\"type\":\"ping\"}\n",
        );
        assert!(matches!(reader.recv().await.unwrap().unwrap(), Message::Ping));
        assert!(reader.recv().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn unknown_fields_on_known_type_are_ignored() {
        let mut reader = reader_over(b"{\"type\":\"cancel\",\"reason\":\"x\",\"new_field\":true}\n");
        assert!(matches!(reader.recv().await.unwrap().unwrap(), Message::Cancel(_)));
    }

    #[tokio::test]
    async fn malformed_line_errors_but_is_consumed() {
        let mut reader = reader_over(b"not json\n{\"type\":\"ping\"}\n");
        assert!(matches!(reader.recv().await, Err(TransportError::Serialize(_))));
        // The bad line was consumed: the next call yields the next message.
        assert!(matches!(reader.recv().await.unwrap().unwrap(), Message::Ping));
    }

    #[tokio::test]
    async fn invalid_utf8_is_a_parse_error_not_io() {
        let mut reader = reader_over(b"{\"type\":\"log\",\"level\":\"info\",\"message\":\"\xff\"}\n");
        assert!(matches!(reader.recv().await, Err(TransportError::Serialize(_))));
        assert!(reader.recv().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn missing_type_and_bad_known_body_are_errors() {
        let mut reader = reader_over(b"{\"no_type\":1}\n{\"type\":\"checkpoint\"}\n");
        assert!(matches!(reader.recv().await, Err(TransportError::Serialize(_))));
        assert!(matches!(reader.recv().await, Err(TransportError::Serialize(_))));
    }
}
