//! End-to-end tests of provider resolution: the real loader runs the fake
//! provider (`support/fake_plugins.rs`, this very executable) as a separate
//! process and resolves the pipeline's `secret()` calls through it.
//!
//! `harness = false`, as for `abort.rs`: `main` first checks whether it was
//! started as a plugin. Filter with a substring argument:
//! `cargo test -p loadsmith-core --test providers -- timeout`.

#[path = "support/fake_plugins.rs"]
mod fake_plugins;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use fake_plugins::{install, journal, pipeline_yaml, timeouts, Scratch};
use loadsmith_config::{ConfigError, MaskList, PipelineConfig};
use loadsmith_core::{load_pipeline, run_pipeline, CoreError, LoadOpts, Mode, RunOpts, SummaryStatus};
use serde_json::json;

fn main() {
    fake_plugins::run_as_plugin_if_invoked();

    let tests: &[(&str, fn())] = &[
        ("resolves_secrets_once_before_the_run", resolves_secrets_once_before_the_run),
        ("resolved_error_is_a_config_error", resolved_error_is_a_config_error),
        ("timeout_is_a_config_error_and_cancels", timeout_is_a_config_error_and_cancels),
        ("crash_is_a_config_error", crash_is_a_config_error),
        ("rejected_config_names_the_instance", rejected_config_names_the_instance),
        ("missing_binary_is_plugin_not_found", missing_binary_is_plugin_not_found),
        ("a_failing_instance_cancels_the_others", a_failing_instance_cancels_the_others),
        ("leaked_values_are_masked_in_logs_and_causes", leaked_values_are_masked_in_logs_and_causes),
        ("static_and_builtins_modes_start_no_provider", static_and_builtins_modes_start_no_provider),
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

const SECRET: &str = "Sup3r-S3cret-Value";

struct Env {
    scratch: Scratch,
    plugins: PathBuf,
}

impl Env {
    fn new(tag: &str) -> Self {
        let scratch = Scratch::new(&format!("prov-{tag}"));
        let plugins = install(&scratch.0);
        Env { scratch, plugins }
    }

    fn journal_path(&self, who: &str) -> PathBuf {
        self.scratch.path(&format!("{who}.journal"))
    }

    fn journal(&self, who: &str) -> Vec<String> {
        journal(&self.journal_path(who))
    }

    /// A `providers:` entry for the fake provider, journaling as `name`.
    fn provider(&self, name: &str, plugin_type: &str, config: serde_json::Value, extra: &str) -> String {
        let mut c = json!({ "journal": self.journal_path(name) });
        if let (Some(b), serde_json::Value::Object(e)) = (c.as_object_mut(), config) {
            b.extend(e);
        }
        format!("  {name}:\n    type: {plugin_type}\n    config: {c}\n{extra}")
    }

    /// The fake source reading `password` (and maybe more) from `secret()`.
    fn yaml(&self, source_extra: serde_json::Value, providers: &[String]) -> String {
        let mut src = json!({
            "journal": self.journal_path("source"),
            "password": "{{ secret('creds', 'db#pw') }}",
            "again": "{{ secret('creds', 'db#pw') }}",
        });
        if let (Some(s), serde_json::Value::Object(e)) = (src.as_object_mut(), source_extra) {
            s.extend(e);
        }
        pipeline_yaml(
            "prov",
            "fake",
            src,
            json!({ "journal": self.journal_path("destination") }),
            &timeouts(""),
            &format!("providers:\n{}", providers.concat()),
        )
    }

    fn load(&self, yaml: &str, mode: Mode) -> Result<(PipelineConfig, MaskList), CoreError> {
        let path = self.scratch.path("pipeline.yaml");
        std::fs::write(&path, yaml).unwrap();
        let opts = LoadOpts::new(&self.plugins);
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        rt.block_on(load_pipeline(&path, mode, &opts))
    }
}

fn values() -> serde_json::Value {
    json!({ "values": { "db#pw": SECRET } })
}

#[track_caller]
fn provider_error(result: Result<(PipelineConfig, MaskList), CoreError>) -> String {
    match result {
        Err(CoreError::Config(ConfigError::Provider(m))) => m,
        other => panic!("expected a provider (config-class) error, got {other:?}"),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

fn resolves_secrets_once_before_the_run() {
    let env = Env::new("ok");
    let spare = env.provider("spare", "fake", values(), "");
    let yaml = env.yaml(json!({}), &[env.provider("creds", "fake", values(), ""), spare]);
    let (config, mask) = env.load(&yaml, Mode::Full).expect("resolved");
    assert_eq!(config.source.config["password"].as_str(), Some(SECRET));
    assert_eq!(config.source.config["again"].as_str(), Some(SECRET));
    // One resolve per distinct reference; an unused instance never starts.
    assert_eq!(env.journal("creds"), ["configure", "resolve db#pw"]);
    assert!(env.journal("spare").is_empty(), "{:?}", env.journal("spare"));
    assert_eq!(mask.apply(SECRET), "***");

    let mut opts = RunOpts::new(&env.plugins);
    opts.color = false;
    opts.mask = mask;
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let summary = rt.block_on(run_pipeline(config, opts)).expect("run");
    assert_eq!(summary.status, SummaryStatus::Success);
    assert!(env.journal("source").contains(&format!("password={SECRET}")));
}

fn resolved_error_is_a_config_error() {
    let env = Env::new("err");
    let yaml = env.yaml(json!({}), &[env.provider("creds", "fake", json!({ "values": {} }), "")]);
    let m = provider_error(env.load(&yaml, Mode::Full));
    assert!(m.contains("provider 'creds' could not resolve 'db#pw'") && m.contains("NOT_FOUND"), "{m}");
    assert!(env.journal("source").is_empty(), "no data plugin may start");
}

fn timeout_is_a_config_error_and_cancels() {
    let env = Env::new("timeout");
    let creds = env.provider("creds", "fake", json!({ "hang": true }), "    timeout_seconds: 1\n");
    let yaml = env.yaml(json!({}), &[creds]);
    let started = Instant::now();
    let m = provider_error(env.load(&yaml, Mode::Full));
    assert!(m.contains("provider 'creds' did not finish within 1s"), "{m}");
    assert!(started.elapsed() < Duration::from_secs(8), "{:?}", started.elapsed());
    // The provider got `error{TIMEOUT}` and ran its cancel hook.
    assert!(env.journal("creds").contains(&"cancel".to_string()), "{:?}", env.journal("creds"));
}

fn crash_is_a_config_error() {
    let env = Env::new("crash");
    let yaml = env.yaml(json!({}), &[env.provider("creds", "fake", json!({ "crash": true }), "")]);
    let m = provider_error(env.load(&yaml, Mode::Full));
    assert!(m.contains("provider 'creds'") && m.contains("exited with status 44"), "{m}");
}

fn rejected_config_names_the_instance() {
    let env = Env::new("reject");
    let yaml = env.yaml(json!({}), &[env.provider("creds", "fake", json!({ "reject": true }), "")]);
    match env.load(&yaml, Mode::Full) {
        Err(CoreError::PluginRejectedConfig(m)) => {
            assert!(m.starts_with("provider 'creds':") && m.contains("rejects this config"), "{m}")
        }
        other => panic!("{other:?}"),
    }
}

fn missing_binary_is_plugin_not_found() {
    let env = Env::new("missing");
    let yaml = env.yaml(json!({}), &[env.provider("creds", "not-installed", json!({}), "")]);
    match env.load(&yaml, Mode::Full) {
        Err(CoreError::PluginNotFound(m)) => {
            assert!(m.contains("'creds'") && m.contains("loadsmith-provider-not-installed"), "{m}")
        }
        other => panic!("{other:?}"),
    }
}

fn a_failing_instance_cancels_the_others() {
    let env = Env::new("cascade");
    // `slow` would hang forever (no timeout): only the abort can stop it.
    let slow = env.provider("slow", "fake", json!({ "hang": true }), "    timeout_seconds: 0\n");
    let creds = env.provider("creds", "fake", json!({ "values": {} }), "");
    let yaml = env.yaml(json!({ "other": "{{ secret('slow', 'k') }}" }), &[creds, slow]);
    let started = Instant::now();
    let m = provider_error(env.load(&yaml, Mode::Full));
    assert!(m.contains("provider 'creds' could not resolve"), "the root cause wins: {m}");
    assert!(started.elapsed() < Duration::from_secs(8), "{:?}", started.elapsed());
    assert!(fake_plugins::wait_until(Duration::from_secs(2), || env.journal("slow").contains(&"cancel".to_string())));
}

fn leaked_values_are_masked_in_logs_and_causes() {
    // The value comes back masked in the error even though the provider
    // put it in its message (via a reference named after it).
    let env = Env::new("leak");
    let creds = env.provider(
        "creds",
        "fake",
        json!({ "values": { "db#pw": SECRET }, "errors": [SECRET], "leak": true }),
        "",
    );
    let yaml = env.yaml(json!({ "other": format!("{{{{ secret('creds', '{SECRET}') }}}}") }), &[creds]);
    let m = provider_error(env.load(&yaml, Mode::Full));
    // The reference itself is not secret, but here it equals a resolved
    // value, which the mask hides wherever it appears.
    assert!(!m.contains(SECRET), "{m}");
    assert!(m.contains("***"), "{m}");
}

fn static_and_builtins_modes_start_no_provider() {
    let env = Env::new("static");
    let state = env.scratch.path("state.json");
    let yaml = format!(
        "{}state:\n  backend: local\n  path: {}\n",
        env.yaml(json!({}), &[env.provider("creds", "not-installed", json!({}), "")]),
        state.display()
    );
    let (config, _) = env.load(&yaml, Mode::Static).expect("static");
    assert!(config.source.config["password"].as_str().unwrap().contains("secret("));
    let (config, _) = env.load(&yaml, Mode::Builtins).expect("builtins");
    assert_eq!(config.state.unwrap().path, state.display().to_string());
}
