use thiserror::Error;

use crate::shutdown::StopSignal;

/// Why a run could not complete. The variants are coarse on purpose: the CLI
/// maps each to a stable exit code (see the CLI reference).
#[derive(Debug, Error)]
pub enum CoreError {
    /// No plugin binary for this kind and type.
    #[error("plugin not found: {0}")]
    PluginNotFound(String),

    /// The plugin cannot work with this engine: it could not be started, it
    /// speaks no protocol version this core supports, or it declared another
    /// kind than the one it was spawned as.
    #[error("incompatible plugin: {0}")]
    PluginIncompatible(String),

    /// A plugin answered `configure` with `configure_ack { status: error }`.
    #[error("plugin rejected its config: {0}")]
    PluginRejectedConfig(String),

    /// A plugin reported a failure, crashed, or exited unexpectedly.
    #[error("plugin failed: {0}")]
    PluginFailed(String),

    #[error("plugin process error: {0}")]
    PluginProcess(String),

    /// A plugin broke the protocol (an out-of-place or repeatedly malformed
    /// message).
    #[error("protocol error: {0}")]
    Protocol(String),

    /// A plugin did not answer within the configured timeout, or the data
    /// plane stalled.
    #[error("timeout: {0}")]
    Timeout(String),

    /// The run was stopped by an operator signal.
    #[error("interrupted by {}", .0.name())]
    Interrupted(StopSignal),

    #[error("configuration error: {0}")]
    Config(#[from] loadsmith_config::ConfigError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("transport error: {0}")]
    Transport(#[from] loadsmith_transport::TransportError),

    #[error("state error: {0}")]
    State(String),
}
