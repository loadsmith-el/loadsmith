use crate::{error::ConfigError, model::PipelineConfig};

/// Validates the core-owned fields of a PipelineConfig.
/// Plugin-specific config is validated by the plugin itself via configure_ack.
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
    if let Some(state) = &config.state {
        if state.backend.trim().is_empty() {
            return Err(ConfigError::Validation("state.backend must not be empty".into()));
        }
        if state.backend != "local" {
            return Err(ConfigError::Validation(format!(
                "unsupported state.backend '{}' (only 'local' is available)",
                state.backend
            )));
        }
        if state.path.trim().is_empty() {
            return Err(ConfigError::Validation("state.path must not be empty".into()));
        }
    }
    Ok(())
}

