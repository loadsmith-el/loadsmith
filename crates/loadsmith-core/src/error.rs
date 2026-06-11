use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("plugin not found: {0}")]
    PluginNotFound(String),

    #[error("plugin process error: {0}")]
    PluginProcess(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("configuration error: {0}")]
    Config(#[from] loadsmith_config::ConfigError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("transport error: {0}")]
    Transport(#[from] loadsmith_transport::TransportError),

    #[error("state error: {0}")]
    State(String),
}
