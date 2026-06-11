use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("YAML parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("template error: {0}")]
    Template(String),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("provider error: {0}")]
    Provider(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
