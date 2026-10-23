use crate::{
    document::Document, error::ConfigError, mask::MaskList, model::PipelineConfig,
    template::SecretValues,
};

/// Parses a YAML pipeline config string, resolves its `env()`/`file()`
/// templates, and returns the typed config along with a MaskList of the
/// resolved values.
///
/// A file with `secret()` calls is rejected: those need provider plugins,
/// which only the engine can run (`loadsmith_core::load`).
pub fn parse_pipeline_yaml(content: &str) -> Result<(PipelineConfig, MaskList), ConfigError> {
    let doc = Document::parse(content)?;
    if let Some(first) = doc.secrets().first() {
        return Err(ConfigError::Template(format!(
            "{first} needs provider resolution, which is not available here"
        )));
    }
    let mask = MaskList::new();
    let config = doc.resolve(&SecretValues::new(), &mask)?;
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
    fn timeouts_default_when_absent() {
        let (config, _) = parse_pipeline_yaml(BASIC_YAML).unwrap();
        assert_eq!(config.pipeline.timeouts, crate::Timeouts::default());
        assert_eq!(config.pipeline.timeouts.stall_seconds, 0, "stall detection is opt-in");
    }

    #[test]
    fn flow_is_partial_and_strict() {
        let (config, _) = parse_pipeline_yaml(BASIC_YAML).unwrap();
        assert_eq!(config.pipeline.flow, crate::FlowConfig::default());
        let yaml = "pipeline:\n  name: x\n  flow:\n    max_inflight_batches: 3\nsource:\n  type: postgres\ndestination:\n  type: jsonl\n";
        let (config, _) = parse_pipeline_yaml(yaml).unwrap();
        assert_eq!(config.pipeline.flow.max_inflight_batches, 3);
        assert_eq!(config.pipeline.flow.memory_budget_mib, crate::FlowConfig::default().memory_budget_mib);
        let err = parse_pipeline_yaml(&yaml.replace("max_inflight_batches", "max_inflight"))
            .expect_err("unknown key")
            .to_string();
        assert!(err.contains("pipeline.flow.max_inflight"), "{err}");
    }

    #[test]
    fn recovery_is_off_by_default() {
        let (config, _) = parse_pipeline_yaml(BASIC_YAML).unwrap();
        assert_eq!(config.pipeline.recovery.max_attempts, 1);
        let yaml = "pipeline:\n  name: x\n  recovery:\n    max_attempts: 3\nsource:\n  type: postgres\ndestination:\n  type: jsonl\n";
        let (config, _) = parse_pipeline_yaml(yaml).unwrap();
        assert_eq!(config.pipeline.recovery, crate::RecoveryConfig { max_attempts: 3, backoff_seconds: 30 });
    }

    #[test]
    fn timeouts_are_partial_and_strict() {
        let yaml = r#"
pipeline:
  name: x
  timeouts:
    stall_seconds: 120
    cancel_grace_seconds: 5
source:
  type: postgres
destination:
  type: jsonl
"#;
        let (config, _) = parse_pipeline_yaml(yaml).unwrap();
        let t = config.pipeline.timeouts;
        assert_eq!(t.stall_seconds, 120);
        assert_eq!(t.cancel_grace_seconds, 5);
        assert_eq!(t.handshake_seconds, crate::Timeouts::default().handshake_seconds);

        let typo = yaml.replace("stall_seconds", "stall_secs");
        let err = parse_pipeline_yaml(&typo).expect_err("unknown key").to_string();
        assert!(err.contains("stall_secs"), "{err}");
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
