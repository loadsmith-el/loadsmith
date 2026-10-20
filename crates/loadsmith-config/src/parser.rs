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

    /// Regression: plugin blocks accepted any key, so `confg:` validated and
    /// the plugin silently ran with an empty config.
    #[test]
    fn rejects_unknown_keys_in_plugin_blocks() {
        for block in ["source", "destination", "sink"] {
            let mut yaml = String::from(
                "pipeline:\n  name: x\nsource:\n  type: postgres\ndestination:\n  type: jsonl\n",
            );
            if block == "sink" {
                yaml.push_str("sink:\n  type: s3\n");
            }
            // Insert a typo'd key right after this block's `type:` line.
            let at = yaml.find(&format!("{block}:\n")).unwrap();
            let line_end = at + yaml[at..].find("type:").unwrap();
            let line_end = line_end + yaml[line_end..].find('\n').unwrap() + 1;
            yaml.insert_str(line_end, "  confg:\n    host: db\n");
            let err = parse_pipeline_yaml(&yaml).expect_err(block).to_string();
            assert!(err.contains("confg"), "{block}: {err}");
        }
    }

    #[test]
    fn plugin_config_contents_stay_free_form() {
        let yaml = r#"
pipeline:
  name: x
source:
  type: postgres
  config:
    anything: goes
    nested: { deeply: [1, 2, 3] }
destination:
  type: jsonl
"#;
        assert!(parse_pipeline_yaml(yaml).is_ok());
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
