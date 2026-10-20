//! A pipeline file, parsed and statically checked, ready to be resolved.
//!
//! Resolution is staged so that nothing with side effects runs before the
//! whole file is known to be well formed:
//!
//! 1. [`Document::parse`] — YAML, the strict typed shape and the core-owned
//!    validation on the *unresolved* tree, the syntax of every template, and
//!    the `secret()` rules: each names a declared provider instance and none
//!    sits in a block that must resolve without providers (`pipeline:`,
//!    `state:`, `providers:`). This is all `loadsmith validate` checks by
//!    default.
//! 2. [`Document::resolve_subtrees`] — `env()`/`file()` in chosen top-level
//!    blocks only (`state show`/`state rm` need just `pipeline:` + `state:`;
//!    the engine resolves `providers:` this way before starting them).
//! 3. [`Document::resolve`] — one substitution pass over the whole tree with
//!    `env()`/`file()` and the provider values, then the typed config.
use std::collections::BTreeSet;

use serde_yaml::Value;

use crate::error::ConfigError;
use crate::mask::MaskList;
use crate::model::PipelineConfig;
use crate::template::{scan_value, substitute_value, FoundSecret, SecretRef, SecretValues, Secrets};
use crate::validate::validate_pipeline;

/// Top-level blocks where `secret()` is not allowed: they are resolved
/// without providers (`pipeline:` and `state:` by the state commands,
/// `providers:` before the providers start).
pub const SECRET_FREE_BLOCKS: &[&str] = &["pipeline", "state", "providers"];

/// A statically checked pipeline file.
#[derive(Debug, Clone)]
pub struct Document {
    raw: Value,
    config: PipelineConfig,
    secrets: Vec<SecretRef>,
}

impl Document {
    /// Parses and statically checks a pipeline file (step 1 above). Evaluates
    /// nothing: no environment variable, file or provider is read.
    pub fn parse(content: &str) -> Result<Self, ConfigError> {
        let raw: Value = serde_yaml::from_str(content)?;
        if !raw.is_mapping() {
            return Err(ConfigError::Validation("a pipeline file must be a YAML mapping".into()));
        }
        // The structure is checked before any template is evaluated: a typo
        // in a key must fail without side effects.
        let config: PipelineConfig = serde_yaml::from_value(raw.clone())?;
        validate_pipeline(&config)?;

        let mut found: Vec<FoundSecret> = Vec::new();
        scan_value(&raw, "", &mut found)?;
        let mut seen = BTreeSet::new();
        let mut secrets = Vec::new();
        for FoundSecret { call, at } in found {
            let block = at.split(['.', '[']).next().unwrap_or_default();
            if SECRET_FREE_BLOCKS.contains(&block) {
                return Err(ConfigError::Validation(format!(
                    "{at}: {call} is not allowed in the {block}: block (pipeline:, state: and \
                     providers: resolve without providers; use env() or file() there)"
                )));
            }
            if !config.providers.contains_key(&call.instance) {
                let declared: Vec<&str> = config.providers.keys().map(String::as_str).collect();
                return Err(ConfigError::Validation(format!(
                    "{at}: {call} names the provider instance '{}', which is not declared under \
                     providers: (declared: {})",
                    call.instance,
                    if declared.is_empty() { "none".to_string() } else { declared.join(", ") }
                )));
            }
            if seen.insert(call.clone()) {
                secrets.push(call);
            }
        }
        Ok(Self { raw, config, secrets })
    }

    /// The typed config with every template still unresolved.
    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    /// The distinct `secret()` calls, in document order.
    pub fn secrets(&self) -> &[SecretRef] {
        &self.secrets
    }

    /// The provider instances some `secret()` call references, in
    /// declaration (name) order.
    pub fn referenced_instances(&self) -> Vec<&str> {
        self.config
            .providers
            .keys()
            .filter(|name| self.secrets.iter().any(|s| &s.instance == *name))
            .map(String::as_str)
            .collect()
    }

    /// Declared provider instances no `secret()` call references.
    pub fn unused_instances(&self) -> Vec<&str> {
        let used = self.referenced_instances();
        self.config.providers.keys().map(String::as_str).filter(|n| !used.contains(n)).collect()
    }

    /// The config with `env()`/`file()` resolved in the top-level `blocks`
    /// only; every other block keeps its templates verbatim. Values are
    /// recorded in `mask`.
    pub fn resolve_subtrees(&self, blocks: &[&str], mask: &MaskList) -> Result<PipelineConfig, ConfigError> {
        let resolved = resolve_subtrees(&self.raw, blocks, mask)?;
        let config: PipelineConfig = serde_yaml::from_value(resolved)?;
        validate_pipeline(&config)?;
        Ok(config)
    }

    /// Evaluates every `env()`/`file()` in the whole tree (recording the
    /// values in `mask`) without resolving `secret()`: surfaces an unset
    /// variable or an unreadable file before any provider starts.
    pub fn check_builtins(&self, mask: &MaskList) -> Result<(), ConfigError> {
        substitute_value(self.raw.clone(), Secrets::Placeholder, mask).map(|_| ())
    }

    /// The fully resolved config: `env()`/`file()` everywhere and each
    /// `secret()` call from `secrets`. Values are recorded in `mask`.
    pub fn resolve(&self, secrets: &SecretValues, mask: &MaskList) -> Result<PipelineConfig, ConfigError> {
        for value in secrets.values() {
            mask.add(value.clone());
        }
        let resolved = substitute_value(self.raw.clone(), Secrets::Values(secrets), mask)?;
        let config: PipelineConfig = serde_yaml::from_value(resolved)?;
        validate_pipeline(&config)?;
        Ok(config)
    }
}

/// Resolves `env()`/`file()` in the top-level `blocks` of a pipeline tree;
/// every other block is copied verbatim. A `secret()` call inside `blocks` is
/// an error.
pub fn resolve_subtrees(raw: &Value, blocks: &[&str], mask: &MaskList) -> Result<Value, ConfigError> {
    let Value::Mapping(map) = raw else {
        return Err(ConfigError::Validation("a pipeline file must be a YAML mapping".into()));
    };
    let mut out = serde_yaml::Mapping::new();
    for (k, v) in map {
        let wanted = k.as_str().is_some_and(|k| blocks.contains(&k));
        let v = if wanted { substitute_value(v.clone(), Secrets::None, mask)? } else { v.clone() };
        out.insert(k.clone(), v);
    }
    Ok(Value::Mapping(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    const WITH_PROVIDERS: &str = r#"
pipeline:
  name: p
providers:
  creds:
    type: file
    config: { base_dir: /secrets }
  sm:
    type: aws-secrets-manager
    timeout_seconds: 5
source:
  type: postgres
  config:
    password: "{{ secret('creds', 'db.json#password') }}"
    again: "x{{ secret('creds', 'db.json#password') }}y"
    user: "{{ secret('creds', 'db.json#user') }}"
destination:
  type: jsonl
"#;

    fn call(i: &str, r: &str) -> SecretRef {
        SecretRef { instance: i.into(), reference: r.into() }
    }

    #[test]
    fn parse_collects_distinct_secrets_and_referenced_instances() {
        let doc = Document::parse(WITH_PROVIDERS).unwrap();
        assert_eq!(doc.secrets(), [call("creds", "db.json#password"), call("creds", "db.json#user")]);
        assert_eq!(doc.referenced_instances(), ["creds"]);
        assert_eq!(doc.unused_instances(), ["sm"]);
        assert_eq!(doc.config().providers["sm"].timeout_seconds, Some(5));
        assert_eq!(
            doc.config().providers["creds"].timeout(),
            Some(std::time::Duration::from_secs(crate::model::DEFAULT_PROVIDER_TIMEOUT_SECONDS))
        );
    }

    #[test]
    fn parse_evaluates_nothing() {
        // An unset variable and a missing file pass the static check.
        let yaml = "pipeline:\n  name: p\nsource:\n  type: x\n  config:\n    a: \"{{ env('LS_DOC_UNSET_XYZ') }}\"\n    b: \"{{ file('/does/not/exist') }}\"\ndestination:\n  type: y\n";
        assert!(Document::parse(yaml).is_ok());
    }

    #[test]
    fn undeclared_instance_is_a_static_error() {
        let yaml = WITH_PROVIDERS.replace("secret('creds', 'db.json#user')", "secret('vault', 'u')");
        let err = Document::parse(&yaml).unwrap_err().to_string();
        assert!(err.contains("'vault'") && err.contains("not declared"), "{err}");
        assert!(err.contains("source.config.user"), "{err}");
        assert!(err.contains("creds, sm"), "{err}");
    }

    #[test]
    fn secret_in_a_secret_free_block_is_a_static_error() {
        for (block, yaml) in [
            ("pipeline", WITH_PROVIDERS.replace("  name: p", "  name: \"{{ secret('creds', 'n') }}\"")),
            (
                "state",
                format!("{WITH_PROVIDERS}state:\n  backend: local\n  path: \"{{{{ secret('creds', 'p') }}}}\"\n"),
            ),
            ("providers", WITH_PROVIDERS.replace("/secrets", "\"{{ secret('sm', 'dir') }}\"")),
        ] {
            let err = Document::parse(&yaml).expect_err(block).to_string();
            assert!(err.contains(&format!("the {block}: block")), "{block}: {err}");
        }
    }

    #[test]
    fn providers_and_parser_blocks_are_strict() {
        let typo = WITH_PROVIDERS.replace("timeout_seconds", "timeout_secs");
        assert!(Document::parse(&typo).unwrap_err().to_string().contains("timeout_secs"));
        let bad_name = WITH_PROVIDERS.replace("  sm:", "  \"s m\":");
        assert!(Document::parse(&bad_name).unwrap_err().to_string().contains("instance name"));
        let parser = "pipeline:\n  name: p\nsource:\n  type: file\nparser:\n  type: csv\n  confg: {}\ndestination:\n  type: y\n";
        assert!(Document::parse(parser).unwrap_err().to_string().contains("confg"));
        let ok = parser.replace("confg", "config");
        assert_eq!(Document::parse(&ok).unwrap().config().parser.as_ref().unwrap().plugin_type, "csv");
        let with_state = format!("{ok}state:\n  backend: local\n  path: /s.json\n");
        assert!(Document::parse(&with_state).unwrap_err().to_string().contains("parser"));
    }

    #[test]
    fn template_syntax_is_checked_everywhere() {
        let yaml = "pipeline:\n  name: p\nsource:\n  type: x\n  config:\n    a: \"{{ aws.sm('/x') }}\"\ndestination:\n  type: y\n";
        let err = Document::parse(yaml).unwrap_err().to_string();
        assert!(err.contains("source.config.a") && err.contains("secret("), "{err}");
    }

    #[test]
    fn resolve_substitutes_provider_values_and_masks_them() {
        let doc = Document::parse(WITH_PROVIDERS).unwrap();
        let mut values = SecretValues::new();
        values.insert(call("creds", "db.json#password"), "hunter22".into());
        values.insert(call("creds", "db.json#user"), "loader".into());
        let mask = MaskList::new();
        let config = doc.resolve(&values, &mask).unwrap();
        assert_eq!(config.source.config["password"].as_str(), Some("hunter22"));
        assert_eq!(config.source.config["again"].as_str(), Some("xhunter22y"));
        assert_eq!(mask.apply("pw hunter22 user loader"), "pw *** user ***");
        // A missing value is an error, never an empty string.
        values.remove(&call("creds", "db.json#user"));
        assert!(doc.resolve(&values, &MaskList::new()).is_err());
    }

    #[test]
    fn subtrees_resolve_only_the_named_blocks() {
        std::env::set_var("LS_DOC_STATE_DIR", "/var/state");
        let yaml = format!(
            "{WITH_PROVIDERS}state:\n  backend: local\n  path: \"{{{{ env('LS_DOC_STATE_DIR') }}}}/p.json\"\n"
        )
        .replace("type: jsonl", "type: jsonl\n  config:\n    path: \"{{ env('LS_DOC_UNSET_ABC') }}\"");
        let doc = Document::parse(&yaml).unwrap();
        let mask = MaskList::new();
        // No provider runs and the unset variable in destination: is not read.
        let config = doc.resolve_subtrees(&["pipeline", "state"], &mask).unwrap();
        assert_eq!(config.state.unwrap().path, "/var/state/p.json");
        assert!(config.source.config["password"].as_str().unwrap().contains("secret("));
        assert_eq!(mask.apply("/var/state"), "***");
    }
}
