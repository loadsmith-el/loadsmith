use crate::{error::ConfigError, model::PipelineConfig};

/// Whether `s` holds a `{{ }}` template (so its value is only known after
/// resolution).
fn templated(s: &str) -> bool {
    s.contains("{{")
}

/// Whether `name` is a valid provider instance name: ASCII letters, digits,
/// `_` and `-`.
pub fn valid_instance_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Validates the core-owned fields of a PipelineConfig.
/// Plugin-specific config is validated by the plugin itself via configure_ack.
///
/// Works on resolved and unresolved configs alike: a check that depends on a
/// value is skipped while that value is still a template.
pub fn validate_pipeline(config: &PipelineConfig) -> Result<(), ConfigError> {
    if config.pipeline.name.trim().is_empty() {
        return Err(ConfigError::Validation("pipeline.name must not be empty".into()));
    }
    if config.source.plugin_type.trim().is_empty() {
        return Err(ConfigError::Validation("source.type must not be empty".into()));
    }
    if config.destination.plugin_type.trim().is_empty() {
        return Err(ConfigError::Validation("destination.type must not be empty".into()));
    }
    // The sink block is optional. Whether the chosen destination actually
    // produces objects to deliver (the `object_output` capability) is enforced
    // at runtime once the destination's capabilities are known — here we only
    // check the static shape.
    if let Some(sink) = &config.sink {
        if sink.plugin_type.trim().is_empty() {
            return Err(ConfigError::Validation("sink.type must not be empty".into()));
        }
    }
    if let Some(parser) = &config.parser {
        if parser.plugin_type.trim().is_empty() {
            return Err(ConfigError::Validation("parser.type must not be empty".into()));
        }
        if config.state.is_some() {
            return Err(ConfigError::Validation(
                "a parser: cannot be combined with a state: block (incremental loads through a \
                 parser are not supported yet)"
                    .into(),
            ));
        }
    }
    for (name, provider) in &config.providers {
        if !valid_instance_name(name) {
            return Err(ConfigError::Validation(format!(
                "invalid provider instance name '{name}' under providers: (use letters, digits, \
                 '_' and '-')"
            )));
        }
        if provider.plugin_type.trim().is_empty() {
            return Err(ConfigError::Validation(format!("providers.{name}.type must not be empty")));
        }
    }
    if let Some(state) = &config.state {
        if state.backend.trim().is_empty() {
            return Err(ConfigError::Validation("state.backend must not be empty".into()));
        }
        if !matches!(state.backend.as_str(), "local" | "s3") && !templated(&state.backend) {
            return Err(ConfigError::Validation(format!(
                "unsupported state.backend '{}' (available: 'local', 's3')",
                state.backend
            )));
        }
        if state.path.trim().is_empty() {
            return Err(ConfigError::Validation("state.path must not be empty".into()));
        }
        if state.backend == "s3" && !templated(&state.path) && !state.path.starts_with("s3://") {
            return Err(ConfigError::Validation(format!(
                "state.path '{}' must be an S3 object URI (s3://<bucket>/<key>) with the 's3' backend",
                state.path
            )));
        }
    }
    Ok(())
}
