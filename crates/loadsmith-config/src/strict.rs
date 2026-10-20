//! Unknown keys in the core-owned part of a pipeline file.
//!
//! The typed model accepts unknown keys; this module finds them (at any
//! depth, with their paths) so the operator's policy decides: an error by
//! default, or a warning per key with `--ignore-unknown-keys`. The contents
//! of a plugin's `config:` are opaque here; the plugin checks those itself
//! under the same policy (it receives it in `configure`).
use serde_yaml::Value;

use crate::error::ConfigError;
use crate::model::PipelineConfig;

pub use loadsmith_protocol::UnknownKeys;

/// Deserializes the pipeline envelope and lists the keys it does not know,
/// as paths (`pipeline.timeouts.stall_secs`, `x-generator`). Under
/// [`UnknownKeys::Error`] any such key is an error; under
/// [`UnknownKeys::Warn`] they are returned.
pub fn deserialize_envelope(raw: Value, policy: UnknownKeys) -> Result<(PipelineConfig, Vec<String>), ConfigError> {
    let mut unknown = Vec::new();
    let config: PipelineConfig = serde_ignored::deserialize(raw, |path| unknown.push(render(&path)))?;
    match policy {
        UnknownKeys::Error if !unknown.is_empty() => Err(ConfigError::UnknownKeys(unknown)),
        UnknownKeys::Error => Ok((config, Vec::new())),
        UnknownKeys::Warn => Ok((config, unknown)),
    }
}

/// A key path without serde_ignored's `?` markers for `Option`s.
fn render(path: &serde_ignored::Path<'_>) -> String {
    use serde_ignored::Path;
    match path {
        Path::Root => String::new(),
        Path::Seq { parent, index } => format!("{}[{index}]", render(parent)),
        Path::Map { parent, key } => match render(parent) {
            p if p.is_empty() => key.clone(),
            p => format!("{p}.{key}"),
        },
        Path::Some { parent } | Path::NewtypeStruct { parent } | Path::NewtypeVariant { parent } => render(parent),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const YAML: &str = r#"
x-generator: { tool: gen, version: 3 }
pipeline:
  name: p
  heartbeat_secs: 10
  timeouts: { stall_secs: 5 }
providers:
  vault: { type: file, timeout_secs: 3 }
source: { type: pg, confg: { host: h }, config: { anything: [1, 2] } }
destination: { type: jsonl, config: { path: /tmp/x } }
sink: { type: s3, extra: 1 }
state: { backend: local, path: /tmp/s, on_schema_chnage: continue }
"#;

    fn raw(yaml: &str) -> Value {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn every_unknown_envelope_key_is_found_with_its_path() {
        let (config, unknown) = deserialize_envelope(raw(YAML), UnknownKeys::Warn).unwrap();
        assert_eq!(config.pipeline.name, "p");
        assert_eq!(
            unknown,
            [
                "x-generator",
                "pipeline.heartbeat_secs",
                "pipeline.timeouts.stall_secs",
                "providers.vault.timeout_secs",
                "source.confg",
                "sink.extra",
                "state.on_schema_chnage",
            ]
        );
    }

    #[test]
    fn unknown_envelope_keys_are_an_error_by_default() {
        let err = deserialize_envelope(raw(YAML), UnknownKeys::Error).unwrap_err().to_string();
        assert!(err.contains("`x-generator`") && err.contains("`source.confg`"), "{err}");
        assert!(err.contains("--ignore-unknown-keys"), "{err}");
    }

    #[test]
    fn plugin_config_contents_are_not_checked_here() {
        let yaml = "pipeline: { name: p }\nsource: { type: a, config: { whatever: { deep: 1 } } }\n\
                    destination: { type: b }\n";
        for policy in [UnknownKeys::Error, UnknownKeys::Warn] {
            assert!(deserialize_envelope(raw(yaml), policy).unwrap().1.is_empty());
        }
    }
}
