//! End-to-end tests of the unknown-keys policy: the loader checks the
//! pipeline file's own blocks, and the policy reaches every plugin in
//! `configure`, where the SDK helper applies it to the plugin's `config:`.
//! The plugins are the fakes of `support/fake_plugins.rs` (this very
//! executable); `destination-typed` parses its config with the helper.
//!
//! `harness = false`, as for `abort.rs`: `main` first checks whether it was
//! started as a plugin. Filter with a substring argument:
//! `cargo test -p loadsmith-core --test unknown_keys -- warn`.

#[path = "support/fake_plugins.rs"]
mod fake_plugins;

use std::path::{Path, PathBuf};
use std::time::Instant;

use fake_plugins::{install, journal, Scratch};
use loadsmith_config::ConfigError;
use loadsmith_core::{load_pipeline, run_pipeline, CoreError, LoadOpts, Mode, RunOpts, Summary, UnknownKeys};
use serde_json::json;

fn main() {
    fake_plugins::run_as_plugin_if_invoked();

    let tests: &[(&str, fn())] = &[
        ("plugin_typo_fails_configure_by_default", plugin_typo_fails_configure_by_default),
        ("plugin_typo_only_warns_under_warn", plugin_typo_only_warns_under_warn),
        ("known_keys_run_under_either_policy", known_keys_run_under_either_policy),
        ("envelope_typo_fails_loading_by_default", envelope_typo_fails_loading_by_default),
        ("envelope_typo_only_warns_under_warn", envelope_typo_only_warns_under_warn),
    ];
    let filter: Vec<String> = std::env::args().skip(1).filter(|a| !a.starts_with('-')).collect();
    let mut failed = Vec::new();
    let mut ran = 0;
    for (name, test) in tests {
        if !filter.is_empty() && !filter.iter().any(|f| name.contains(f.as_str())) {
            continue;
        }
        ran += 1;
        let started = Instant::now();
        let ok = std::panic::catch_unwind(test).is_ok();
        println!(
            "test {name} ... {} ({:.1}s)",
            if ok { "ok" } else { "FAILED" },
            started.elapsed().as_secs_f64()
        );
        if !ok {
            failed.push(*name);
        }
    }
    println!(
        "\ntest result: {}. {} passed; {} failed; 0 ignored; 0 measured; {} filtered out",
        if failed.is_empty() { "ok" } else { "FAILED" },
        ran - failed.len(),
        failed.len(),
        tests.len() - ran
    );
    if !failed.is_empty() {
        std::process::exit(101);
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

struct Env {
    scratch: Scratch,
    plugins: PathBuf,
}

impl Env {
    fn new(tag: &str) -> Self {
        let scratch = Scratch::new(&format!("keys-{tag}"));
        let plugins = install(&scratch.0);
        Env { scratch, plugins }
    }

    fn journal(&self) -> PathBuf {
        self.scratch.path("destination.journal")
    }

    /// The fake source → `destination-typed`, with `dst_extra` merged into
    /// the destination's config and `top` appended to the file.
    fn pipeline(&self, dst_extra: serde_json::Value, top: &str) -> PathBuf {
        let mut dst = json!({ "journal": self.journal() });
        if let (Some(d), serde_json::Value::Object(extra)) = (dst.as_object_mut(), dst_extra) {
            d.extend(extra);
        }
        let yaml = format!(
            "pipeline:\n  name: keys\n  heartbeat_seconds: 0\n  timeouts:\n    cancel_grace_seconds: 2\n\
             source:\n  type: fake\n  config: {{ batches: 2 }}\n\
             destination:\n  type: typed\n  config: {dst}\n{top}"
        );
        let path = self.scratch.path("pipeline.yaml");
        std::fs::write(&path, yaml).unwrap();
        path
    }

    fn load(&self, path: &Path, unknown_keys: UnknownKeys) -> Result<loadsmith_config::PipelineConfig, CoreError> {
        let opts = LoadOpts { unknown_keys, ..LoadOpts::new(&self.plugins) };
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(load_pipeline(path, Mode::Full, &opts)).map(|(config, _)| config)
    }

    fn run(&self, path: &Path, unknown_keys: UnknownKeys) -> Result<Summary, CoreError> {
        let config = self.load(path, unknown_keys)?;
        let mut opts = RunOpts::new(&self.plugins);
        opts.color = false;
        opts.unknown_keys = unknown_keys;
        opts.human_report = false;
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        rt.block_on(run_pipeline(config, opts))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

fn plugin_typo_fails_configure_by_default() {
    let env = Env::new("plugin-default");
    // `fail_aftr`: a typo of a field of a flattened struct, which serde's
    // own deny_unknown_fields could not have caught.
    let path = env.pipeline(json!({ "fail_aftr": 0, "x-generator": { "by": "tool" } }), "");
    let err = env.run(&path, UnknownKeys::Error).expect_err("a typo must fail the run");
    let CoreError::PluginRejectedConfig(message) = &err else { panic!("not a rejected config: {err:?}") };
    assert!(message.contains("UNKNOWN_CONFIG_KEYS"), "{message}");
    assert!(message.contains("`fail_aftr`") && message.contains("`x-generator`"), "{message}");
    // The policy reached the plugin, which then refused its config.
    assert_eq!(journal(&env.journal()), ["unknown_keys=error"]);
}

fn plugin_typo_only_warns_under_warn() {
    let env = Env::new("plugin-warn");
    let path = env.pipeline(json!({ "fail_aftr": 0 }), "");
    let summary = env.run(&path, UnknownKeys::Warn).expect("an ignored key must not fail the run");
    assert_eq!(summary.rows_written, 20);
    let lines = journal(&env.journal());
    assert_eq!(lines[..2], ["unknown_keys=warn", "configure"]);
    assert!(lines.contains(&"finalize".to_string()), "{lines:?}");
}

fn known_keys_run_under_either_policy() {
    for policy in [UnknownKeys::Error, UnknownKeys::Warn] {
        let env = Env::new(&format!("known-{}", policy.as_str()));
        let path = env.pipeline(json!({ "fail_after": 5 }), "");
        let summary = env.run(&path, policy).expect("known keys only");
        assert_eq!(summary.rows_written, 20);
        assert_eq!(journal(&env.journal())[0], format!("unknown_keys={}", policy.as_str()));
    }
}

fn envelope_typo_fails_loading_by_default() {
    let env = Env::new("envelope-default");
    let path = env.pipeline(json!({}), "x-generator:\n  by: tool\n");
    let err = env.load(&path, UnknownKeys::Error).expect_err("unknown top-level key");
    match err {
        CoreError::Config(ConfigError::UnknownKeys(keys)) => assert_eq!(keys, ["x-generator"]),
        other => panic!("{other:?}"),
    }
    // Nothing ran.
    assert!(journal(&env.journal()).is_empty());
}

fn envelope_typo_only_warns_under_warn() {
    let env = Env::new("envelope-warn");
    let path = env.pipeline(json!({}), "x-generator:\n  by: tool\n");
    let summary = env.run(&path, UnknownKeys::Warn).expect("an ignored key must not fail the run");
    assert_eq!(summary.rows_written, 20);
}
