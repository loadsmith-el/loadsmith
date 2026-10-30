//! The published JSON Schema of a plugin's `config:` block.
//!
//! A plugin derives [`schemars::JsonSchema`] next to `Deserialize` on its
//! config types and returns [`schema_for`] from its trait's `config_schema`:
//!
//! ```ignore
//! #[derive(serde::Deserialize, schemars::JsonSchema)]
//! struct MyConfig { host: String, #[serde(default)] port: Option<u16> }
//!
//! impl SourcePlugin for MySource {
//!     fn config_schema() -> Option<serde_json::Value> {
//!         Some(loadsmith_plugin_sdk::config_schema::schema_for::<MyConfig>())
//!     }
//!     // …
//! }
//! ```
//!
//! `<binary> --config-schema` prints it (the SDK answers the flag before the
//! handshake), and the canonical release publishes it next to the binaries,
//! referenced from the manifest's `provides[].config_schema`. Tooling that
//! builds pipelines (a generator, a UI, a Lambda) validates a `config:` block
//! against it without running the plugin.
//!
//! The schema is draft 2020-12, generated from the same types
//! [`PluginConfig::parse`](crate::PluginConfig::parse) uses, so the two agree
//! on names, types and required fields. Objects are closed
//! (`unevaluatedProperties: false`): an unknown key fails validation, like the
//! default unknown-keys policy. Templates only appear inside strings and
//! strings stay strings once rendered, so an unresolved block validates too —
//! except a templated value in a field restricted to fixed spellings, which
//! only the rendered value can satisfy.
//!
//! Keys the core fills in itself are optional in the printed schema, whatever
//! the type says: a destination's `path` (the staging directory the core
//! allocates when a sink is attached) and an object source's `scratch_dir`.

use schemars::JsonSchema;
use serde_json::{Map, Value};

/// The flag that prints the schema instead of running the plugin.
pub const FLAG: &str = "--config-schema";

/// The schema of `T` as a plugin config block: draft 2020-12, objects closed.
pub fn schema_for<T: JsonSchema>() -> Value {
    let mut schema = schemars::generate::SchemaSettings::draft2020_12()
        .into_generator()
        .into_root_schema_for::<T>()
        .to_value();
    close(&mut schema, false);
    schema
}

const COMBINATORS: [&str; 3] = ["allOf", "anyOf", "oneOf"];

/// Adds `unevaluatedProperties: false` to every object schema (one that lists
/// `properties` or is `type: object`, like an empty struct) that says nothing
/// about extra keys. A combinator branch of a
/// schema with its own `properties` (a `flatten`ed enum) is merged into that
/// schema, which closes over both: the branch itself stays open.
fn close(schema: &mut Value, merged: bool) {
    let Value::Object(map) = schema else { return };
    let has_properties = map.contains_key("properties");
    let branches_merged = merged || has_properties;
    for key in COMBINATORS {
        if let Some(Value::Array(branches)) = map.get_mut(key) {
            branches.iter_mut().for_each(|b| close(b, branches_merged));
        }
    }
    for key in ["properties", "$defs", "patternProperties"] {
        if let Some(Value::Object(children)) = map.get_mut(key) {
            children.values_mut().for_each(|c| close(c, false));
        }
    }
    for key in ["items", "additionalProperties", "not", "if", "then", "else"] {
        if let Some(child) = map.get_mut(key) {
            close(child, false);
        }
    }
    if let Some(Value::Array(items)) = map.get_mut("prefixItems") {
        items.iter_mut().for_each(|c| close(c, false));
    }
    let is_object = has_properties || map.get("type").and_then(Value::as_str) == Some("object");
    if is_object && !merged && !mentions_extra_keys(map) {
        map.insert("unevaluatedProperties".into(), Value::Bool(false));
    }
}

fn mentions_extra_keys(map: &Map<String, Value>) -> bool {
    ["additionalProperties", "unevaluatedProperties", "patternProperties"].iter().any(|k| map.contains_key(*k))
}

/// Answers [`FLAG`]: prints the schema, with `core_supplied` top-level keys
/// made optional, and exits `0`, or exits `2` for a plugin that publishes
/// none. Without the flag, returns.
pub(crate) fn serve_flag(schema: fn() -> Option<Value>, core_supplied: &[&str]) {
    if std::env::args().nth(1).as_deref() != Some(FLAG) {
        return;
    }
    match schema() {
        Some(mut schema) => {
            optional(&mut schema, core_supplied);
            println!("{}", serde_json::to_string_pretty(&schema).expect("a JSON value serializes"));
            std::process::exit(0);
        }
        None => {
            eprintln!("this plugin publishes no config schema");
            std::process::exit(2);
        }
    }
}

/// Drops `keys` from the top-level `required` list.
fn optional(schema: &mut Value, keys: &[&str]) {
    if let Some(Value::Array(required)) = schema.get_mut("required") {
        required.retain(|k| !k.as_str().is_some_and(|k| keys.contains(&k)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[allow(dead_code)]
    #[derive(serde::Deserialize, JsonSchema)]
    struct Conn {
        /// The host.
        host: String,
        #[serde(default)]
        port: Option<u16>,
    }

    #[allow(dead_code)]
    #[derive(serde::Deserialize, JsonSchema)]
    #[serde(rename_all = "snake_case")]
    enum Mode {
        Full,
        Incremental { cursor: String },
    }

    #[allow(dead_code)]
    #[derive(serde::Deserialize, JsonSchema)]
    struct Config {
        #[serde(flatten)]
        conn: Conn,
        query: String,
        #[serde(default)]
        mode: Option<Mode>,
        #[serde(default)]
        labels: std::collections::BTreeMap<String, String>,
    }

    #[allow(dead_code)]
    #[derive(serde::Deserialize, JsonSchema)]
    struct WithFlattenedEnum {
        name: String,
        #[serde(flatten)]
        auth: Auth,
    }

    #[allow(dead_code)]
    #[derive(serde::Deserialize, JsonSchema)]
    #[serde(untagged)]
    enum Auth {
        Password { user: String, password: String },
        Token { token: String },
    }

    fn valid(schema: &Value, instance: Value) -> bool {
        jsonschema::draft202012::is_valid(schema, &instance)
    }

    #[test]
    fn the_schema_accepts_what_parse_accepts_and_closes_objects() {
        let schema = schema_for::<Config>();
        assert_eq!(schema["$schema"], "https://json-schema.org/draft/2020-12/schema");
        assert!(valid(&schema, json!({"host": "h", "query": "q"})));
        assert!(valid(&schema, json!({"host": "h", "port": 5432, "query": "q", "mode": {"incremental": {"cursor": "id"}}})));
        assert!(valid(&schema, json!({"host": "h", "query": "q", "mode": "full", "labels": {"a": "b"}})));
        // A typo at the top, in the flattened block's place, and nested.
        assert!(!valid(&schema, json!({"host": "h", "query": "q", "qeury": "q"})));
        assert!(!valid(&schema, json!({"host": "h", "query": "q", "mode": {"incremental": {"cursor": "id", "x": 1}}})));
        // Missing, mistyped.
        assert!(!valid(&schema, json!({"query": "q"})));
        assert!(!valid(&schema, json!({"host": "h", "query": "q", "port": "5432"})));
        assert!(!valid(&schema, json!({"host": "h", "query": "q", "mode": "partial"})));
        // Doc comments become descriptions.
        assert!(schema.to_string().contains("The host."));
    }

    #[allow(dead_code)]
    #[derive(serde::Deserialize, JsonSchema)]
    struct Empty {}

    #[test]
    fn an_empty_struct_takes_no_keys() {
        let schema = schema_for::<Empty>();
        assert!(valid(&schema, json!({})));
        assert!(!valid(&schema, json!({"x": 1})));
    }

    #[test]
    fn core_supplied_keys_are_optional() {
        let mut schema = schema_for::<Config>();
        optional(&mut schema, &["host"]);
        assert!(valid(&schema, json!({"query": "q"})));
        assert!(!valid(&schema, json!({"host": "h"})));
    }

    #[test]
    fn a_flattened_enum_is_closed_over_both_its_branch_and_the_parent() {
        let schema = schema_for::<WithFlattenedEnum>();
        assert!(valid(&schema, json!({"name": "n", "user": "u", "password": "p"})));
        assert!(valid(&schema, json!({"name": "n", "token": "t"})));
        assert!(!valid(&schema, json!({"name": "n", "token": "t", "tokne": "t"})));
        assert!(!valid(&schema, json!({"name": "n"})));
    }
}
