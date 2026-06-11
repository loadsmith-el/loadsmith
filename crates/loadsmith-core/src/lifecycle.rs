/// Drives the handshake → capabilities → configure sequence for a plugin.
use loadsmith_protocol::{
    Configure, Message, PluginKind, ResumeCursor, SetProtocolVersion, StartParams,
};
use loadsmith_transport::{ControlReader, ControlWriter};
use tokio::io::{AsyncBufRead, AsyncWrite};

use crate::error::CoreError;

const SUPPORTED_VERSIONS: &[u32] = &[1];

/// What core learned about a plugin during the handshake.
#[derive(Debug, Clone)]
pub struct PluginHandshake {
    pub name: String,
    pub version: String,
    pub protocol_version: u32,
    /// Capabilities the plugin advertised (e.g. `object_output`). Used by the
    /// core to gate features like attaching a sink to a destination.
    pub supports: Vec<String>,
}

/// Runs handshake, negotiates protocol version, requests capabilities,
/// and sends configure. Returns identifying info about the plugin.
pub async fn run_handshake_and_configure<R, W>(
    reader: &mut ControlReader<R>,
    writer: &mut ControlWriter<W>,
    config: serde_json::Value,
    expected_kind: PluginKind,
) -> Result<PluginHandshake, CoreError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let label = format!("{expected_kind:?}").to_lowercase();

    // Send Handshake
    tracing::debug!(plugin = %label, "→ handshake");
    writer.send(&Message::Handshake).await?;

    // Await HandshakeAck
    let ack = recv_expect(reader, "handshake_ack").await?;
    let (plugin_name, plugin_version, plugin_versions, kind) = match ack {
        Message::HandshakeAck(a) => {
            (a.plugin_name, a.plugin_version, a.protocol_supported_versions, a.kind)
        }
        other => {
            return Err(CoreError::Protocol(format!(
                "expected handshake_ack, got {other:?}"
            )))
        }
    };
    tracing::debug!(
        plugin = %label,
        name = %plugin_name,
        version = %plugin_version,
        supported = ?plugin_versions,
        "← handshake_ack"
    );

    if kind != expected_kind {
        return Err(CoreError::Protocol(format!(
            "expected plugin kind {expected_kind:?}, got {kind:?}"
        )));
    }

    // Choose the highest mutually supported version
    let chosen = SUPPORTED_VERSIONS
        .iter()
        .rev()
        .find(|v| plugin_versions.contains(v))
        .copied()
        .ok_or_else(|| {
            CoreError::Protocol(format!(
                "no compatible protocol version: core supports {SUPPORTED_VERSIONS:?}, plugin supports {plugin_versions:?}"
            ))
        })?;
    tracing::debug!(plugin = %label, protocol_version = chosen, "→ set_protocol_version");

    writer
        .send(&Message::SetProtocolVersion(SetProtocolVersion {
            protocol_version: chosen,
        }))
        .await?;

    // Capabilities exchange
    writer.send(&Message::CapabilitiesRequest).await?;
    let caps = recv_expect(reader, "capabilities_response").await?;
    let supports = match caps {
        Message::CapabilitiesResponse(c) => {
            tracing::debug!(plugin = %label, supports = ?c.supports, "← capabilities");
            c.supports
        }
        other => {
            return Err(CoreError::Protocol(format!(
                "expected capabilities_response, got {other:?}"
            )))
        }
    };

    // Send configuration
    tracing::debug!(plugin = %label, "→ configure");
    writer.send(&Message::Configure(Configure { config })).await?;

    // Await ConfigureAck
    let ack = recv_expect(reader, "configure_ack").await?;
    match ack {
        Message::ConfigureAck(a) if a.status == loadsmith_protocol::ConfigureStatus::Ok => {
            tracing::debug!(plugin = %label, "← configure_ack ok");
            tracing::info!(plugin = %label, name = %plugin_name, version = %plugin_version, "configured");
        }
        Message::ConfigureAck(a) => {
            return Err(CoreError::Protocol(format!(
                "plugin rejected config: {} — {}",
                a.code.unwrap_or_default(),
                a.message.unwrap_or_default()
            )))
        }
        other => {
            return Err(CoreError::Protocol(format!(
                "expected configure_ack, got {other:?}"
            )))
        }
    }

    Ok(PluginHandshake {
        name: plugin_name,
        version: plugin_version,
        protocol_version: chosen,
        supports,
    })
}

/// Sends Start and waits for Schema (source) or Ready (destination).
///
/// `resume` carries the opaque watermark persisted by a previous run (when the
/// pipeline has incremental state and prior state exists); the source uses it to
/// resume reading. `None` means a full read.
pub async fn start_source<R, W>(
    reader: &mut ControlReader<R>,
    writer: &mut ControlWriter<W>,
    resume: Option<serde_json::Value>,
) -> Result<loadsmith_protocol::Schema, CoreError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    tracing::debug!(plugin = "source", has_resume = resume.is_some(), "→ start");
    let params = StartParams {
        resume: resume.map(|cursor_value| ResumeCursor { cursor_value }),
    };
    writer.send(&Message::Start(params)).await?;
    let msg = recv_expect(reader, "schema").await?;
    match msg {
        Message::Schema(s) => {
            tracing::debug!(plugin = "source", fields = s.fields.len(), "← schema");
            Ok(s)
        }
        other => Err(CoreError::Protocol(format!("expected schema, got {other:?}"))),
    }
}

pub async fn start_destination<R, W>(
    reader: &mut ControlReader<R>,
    writer: &mut ControlWriter<W>,
) -> Result<(), CoreError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    tracing::debug!(plugin = "destination", "→ start");
    writer.send(&Message::Start(StartParams::default())).await?;
    let msg = recv_expect(reader, "ready").await?;
    match msg {
        Message::Ready => {
            tracing::debug!(plugin = "destination", "← ready");
            tracing::info!("destination ready — pump starting");
            Ok(())
        }
        other => Err(CoreError::Protocol(format!("expected ready, got {other:?}"))),
    }
}

/// Sends Start to a sink and waits for Ready. Same shape as `start_destination`
/// — a sink has no schema, it just signals it is prepared to receive objects.
pub async fn start_sink<R, W>(
    reader: &mut ControlReader<R>,
    writer: &mut ControlWriter<W>,
) -> Result<(), CoreError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    tracing::debug!(plugin = "sink", "→ start");
    writer.send(&Message::Start(StartParams::default())).await?;
    let msg = recv_expect(reader, "ready").await?;
    match msg {
        Message::Ready => {
            tracing::debug!(plugin = "sink", "← ready");
            Ok(())
        }
        other => Err(CoreError::Protocol(format!("expected ready, got {other:?}"))),
    }
}

/// Drains the control channel until Finished, returning the final message.
pub async fn await_finished<R: AsyncBufRead + Unpin>(
    reader: &mut ControlReader<R>,
) -> Result<loadsmith_protocol::Finished, CoreError> {
    loop {
        let msg = reader.recv().await?.ok_or_else(|| {
            CoreError::Protocol("plugin closed control channel without sending finished".into())
        })?;
        match msg {
            Message::Finished(f) => return Ok(f),
            Message::Progress(_) | Message::Log(_) | Message::Pong => {
                // These can arrive between start and finished; just continue.
            }
            other => {
                tracing::warn!("unexpected message while awaiting finished: {other:?}");
            }
        }
    }
}

async fn recv_expect<R: AsyncBufRead + Unpin>(
    reader: &mut ControlReader<R>,
    what: &str,
) -> Result<Message, CoreError> {
    reader.recv().await?.ok_or_else(|| {
        CoreError::Protocol(format!("EOF while waiting for {what}"))
    })
}
