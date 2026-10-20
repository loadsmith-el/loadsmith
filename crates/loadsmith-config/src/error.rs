use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("YAML parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("template error: {0}")]
    Template(String),

    #[error("validation error: {0}")]
    Validation(String),

    /// A provider instance could not resolve the pipeline's `secret()`
    /// values (it failed, timed out, broke the protocol, or answered a
    /// reference with an error). The text names the instance and starts with
    /// `provider`.
    #[error("{0}")]
    Provider(String),

    /// The pipeline file could not be read.
    #[error("cannot read {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
