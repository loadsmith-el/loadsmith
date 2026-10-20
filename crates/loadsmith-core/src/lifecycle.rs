//! Drives the handshake → capabilities → configure → start sequence for a
//! plugin. Every wait is bounded by the pipeline's `timeouts` and interrupted
//! by a [`Shutdown`].
use loadsmith_config::Timeouts;
use loadsmith_protocol::{
    error_codes, Configure, Message, PluginKind, ResumeCursor, SetProtocolVersion, StartParams,
};

use crate::error::CoreError;
use crate::peer::{Failure, Peer};
use crate::shutdown::Shutdown;

/// Protocol versions this core can speak. The handshake picks the highest
/// version also advertised by the plugin; `loadsmith plugin install` checks a
/// manifest's `protocol` range against this before fetching.
pub const SUPPORTED_VERSIONS: &[u32] = &[1];

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

fn violation(peer: &Peer, expected: &str, got: &Message) -> Failure {
    Failure::by(
        peer.label(),
        error_codes::PROTOCOL_VIOLATION,
        CoreError::Protocol(peer.mask().apply(&format!("{}: expected {expected}, got {got:?}", peer.label()))),
    )
}

/// Runs handshake, negotiates protocol version, requests capabilities,
/// and sends configure. Returns identifying info about the plugin.
pub async fn run_handshake_and_configure(
    peer: &mut Peer,
    config: serde_json::Value,
    expected_kind: PluginKind,
    timeouts: &Timeouts,
    shutdown: &mut Shutdown,
) -> Result<PluginHandshake, Failure> {
    let label = peer.label();
    let handshake_limit = Timeouts::limit(timeouts.handshake_seconds);

    tracing::debug!(plugin = %label, "→ handshake");
    peer.send(Message::Handshake).await?;

    let ack = peer.recv_expect("handshake_ack", handshake_limit, shutdown).await?;
    let (plugin_name, plugin_version, plugin_versions, kind) = match ack {
        Message::HandshakeAck(a) => {
            (a.plugin_name, a.plugin_version, a.protocol_supported_versions, a.kind)
        }
        other => return Err(violation(peer, "handshake_ack", &other)),
    };
    tracing::debug!(
        plugin = %label,
        name = %plugin_name,
        version = %plugin_version,
        supported = ?plugin_versions,
        "← handshake_ack"
    );

    if kind != expected_kind {
        return Err(Failure::by(
            label,
            error_codes::WRONG_PLUGIN_KIND,
            CoreError::PluginIncompatible(format!(
                "{plugin_name}: expected plugin kind {expected_kind}, got {kind}"
            )),
        ));
    }

    // Choose the highest mutually supported version
    let chosen = SUPPORTED_VERSIONS
        .iter()
        .rev()
        .find(|v| plugin_versions.contains(v))
        .copied()
        .ok_or_else(|| {
            Failure::by(
                label,
                error_codes::UNSUPPORTED_PROTOCOL_VERSION,
                CoreError::PluginIncompatible(format!(
                    "{plugin_name}: no compatible protocol version: core supports \
                     {SUPPORTED_VERSIONS:?}, plugin supports {plugin_versions:?}"
                )),
            )
        })?;
    tracing::debug!(plugin = %label, protocol_version = chosen, "→ set_protocol_version");
    peer.send(Message::SetProtocolVersion(SetProtocolVersion { protocol_version: chosen }))
        .await?;

    // Capabilities exchange
    peer.send(Message::CapabilitiesRequest).await?;
    let supports = match peer.recv_expect("capabilities_response", handshake_limit, shutdown).await? {
        Message::CapabilitiesResponse(c) => {
            tracing::debug!(plugin = %label, supports = ?c.supports, "← capabilities");
            c.supports
        }
        other => return Err(violation(peer, "capabilities_response", &other)),
    };

    // Send configuration
    tracing::debug!(plugin = %label, "→ configure");
    peer.send(Message::Configure(Configure { config })).await?;

    let configure_limit = Timeouts::limit(timeouts.configure_seconds);
    match peer.recv_expect("configure_ack", configure_limit, shutdown).await? {
        Message::ConfigureAck(a) if a.status == loadsmith_protocol::ConfigureStatus::Ok => {
            tracing::debug!(plugin = %label, "← configure_ack ok");
            tracing::info!(plugin = %label, name = %plugin_name, version = %plugin_version, "configured");
        }
        Message::ConfigureAck(a) => {
            return Err(Failure::of(
                label,
                CoreError::PluginRejectedConfig(peer.mask().apply(&format!(
                    "{plugin_name}: {} — {}",
                    a.code.unwrap_or_default(),
                    a.message.unwrap_or_default()
                ))),
            ))
        }
        other => return Err(violation(peer, "configure_ack", &other)),
    }

    Ok(PluginHandshake {
        name: plugin_name,
        version: plugin_version,
        protocol_version: chosen,
        supports,
    })
}

/// Sends Start and waits for the source's Schema.
///
/// `resume` carries the opaque watermark persisted by a previous run (when the
/// pipeline has incremental state and prior state exists); the source uses it to
/// resume reading. `None` means a full read.
pub async fn start_source(
    peer: &mut Peer,
    resume: Option<serde_json::Value>,
    timeouts: &Timeouts,
    shutdown: &mut Shutdown,
) -> Result<loadsmith_protocol::Schema, Failure> {
    tracing::debug!(plugin = "source", has_resume = resume.is_some(), "→ start");
    let params = StartParams { resume: resume.map(|cursor_value| ResumeCursor { cursor_value }) };
    peer.send(Message::Start(params)).await?;
    match peer.recv_expect("schema", Timeouts::limit(timeouts.start_seconds), shutdown).await? {
        Message::Schema(s) => {
            tracing::debug!(plugin = "source", fields = s.fields.len(), "← schema");
            Ok(s)
        }
        other => Err(violation(peer, "schema", &other)),
    }
}

/// Sends Start and waits for Ready — a destination, or a sink (which has no
/// schema; it just signals it is prepared to receive objects).
pub async fn start_and_await_ready(
    peer: &mut Peer,
    timeouts: &Timeouts,
    shutdown: &mut Shutdown,
) -> Result<(), Failure> {
    let label = peer.label();
    tracing::debug!(plugin = %label, "→ start");
    peer.send(Message::Start(StartParams::default())).await?;
    match peer.recv_expect("ready", Timeouts::limit(timeouts.start_seconds), shutdown).await? {
        Message::Ready => {
            tracing::debug!(plugin = %label, "← ready");
            Ok(())
        }
        other => Err(violation(peer, "ready", &other)),
    }
}
