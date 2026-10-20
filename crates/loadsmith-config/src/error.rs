use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("YAML parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("template error: {0}")]
    Template(String),

    #[error("validation error: {0}")]
    Validation(String),

    /// Keys the engine does not know in the pipeline file's own blocks
    /// (everything but the contents of a plugin's `config:`), as paths.
    #[error(
        "unknown key{} in the pipeline file: {} (fix or remove {}, or run with --ignore-unknown-keys \
         to ignore unknown keys)",
        if .0.len() == 1 { "" } else { "s" },
        .0.iter().map(|k| format!("`{k}`")).collect::<Vec<_>>().join(", "),
        if .0.len() == 1 { "it" } else { "them" }
    )]
    UnknownKeys(Vec<String>),

    /// A provider instance could not resolve the pipeline's `secret()`
    /// values (it failed, timed out, broke the protocol, or answered a
    /// reference with an error). The text names the instance and starts with
    /// `provider`.
    #[error("{0}")]
    Provider(String),

    /// The pipeline file could not be read.
    #[error("cannot read {path}: {error}")]
    Read { path: String, error: std::io::Error },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
