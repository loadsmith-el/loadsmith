use crate::{error::ConfigError, mask::MaskList, model::PipelineConfig, template::resolve_value};

/// Parses a YAML pipeline config string, resolves templates, and returns
/// the typed config along with a MaskList of resolved secret values.
pub fn parse_pipeline_yaml(content: &str) -> Result<(PipelineConfig, MaskList), ConfigError> {
    // First pass: parse raw YAML into a Value so we can run template resolution
    // before deserializing into the typed struct.
    let raw: serde_yaml::Value = serde_yaml::from_str(content)?;

    let mut mask = MaskList::new();
    let resolved = resolve_value(raw, &mut mask)?;

    // Second pass: deserialize the resolved Value into PipelineConfig.
    let config: PipelineConfig = serde_yaml::from_value(resolved)?;

    Ok((config, mask))
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASIC_YAML: &str = r#"
pipeline:
  name: test_pipeline

source:
  type: postgres
  config:
    host: localhost
    port: 5432

destination:
  type: jsonl
  config:
    path: /tmp/out.jsonl
"#;

    #[test]
    fn parses_basic_pipeline() {
        let (config, mask) = parse_pipeline_yaml(BASIC_YAML).unwrap();
        assert_eq!(config.pipeline.name, "test_pipeline");
        assert_eq!(config.source.plugin_type, "postgres");
        assert_eq!(config.destination.plugin_type, "jsonl");
        assert!(mask.is_empty());
    }

    #[test]
    fn rejects_unknown_top_level_fields() {
        let yaml = r#"
pipeline:
  name: x
source:
  type: postgres
destination:
  type: jsonl
unknown_field: bad
"#;
        let result = parse_pipeline_yaml(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn resolves_env_template_in_config() {
        std::env::set_var("LS_PARSER_TEST_HOST", "db.example.com");
        let yaml = r#"
pipeline:
  name: env_test
source:
  type: postgres
  config:
    host: "{{ env('LS_PARSER_TEST_HOST') }}"
destination:
  type: jsonl
"#;
        let (config, mask) = parse_pipeline_yaml(yaml).unwrap();
        let host = config.source.config["host"].as_str().unwrap();
        assert_eq!(host, "db.example.com");
        assert_eq!(mask.apply("db.example.com"), "***");
    }
}
