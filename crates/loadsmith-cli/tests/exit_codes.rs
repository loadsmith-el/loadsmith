//! The `loadsmith` exit codes and signal handling, end to end: each test runs
//! the real binary against fake plugins (this executable, linked into a
//! temporary plugin directory; see the core's `tests/support/fake_plugins.rs`).
//!
//! `harness = false`: `main` first checks whether it was started as a plugin.

#[path = "../../loadsmith-core/tests/support/fake_plugins.rs"]
mod fake_plugins;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fake_plugins::{install, journal, pipeline_yaml, timeouts, wait_until, Scratch};
use serde_json::json;

fn main() {
    fake_plugins::run_as_plugin_if_invoked();

    let tests: &[(&str, fn())] = &[
        ("success_exits_0", success_exits_0),
        ("run_failure_exits_1", run_failure_exits_1),
        ("usage_error_exits_2", usage_error_exits_2),
        ("config_error_exits_3", config_error_exits_3),
        ("rejected_capability_exits_3", rejected_capability_exits_3),
        ("plugin_not_found_exits_4", plugin_not_found_exits_4),
        ("state_lock_held_exits_5", state_lock_held_exits_5),
        ("sigterm_cancels_and_exits_143", sigterm_cancels_and_exits_143),
        ("sigint_cancels_and_exits_130", sigint_cancels_and_exits_130),
        ("json_summary_on_success", json_summary_on_success),
        ("json_summary_on_setup_failures", json_summary_on_setup_failures),
        ("json_summary_on_run_failure", json_summary_on_run_failure),
        ("json_summary_when_cancelled", json_summary_when_cancelled),
        ("json_summary_for_a_dry_run", json_summary_for_a_dry_run),
        ("output_json_conflicts_with_print_resolved_config", output_json_conflicts_with_print_resolved_config),
        ("provider_secret_never_appears_in_output", provider_secret_never_appears_in_output),
        ("provider_failures_exit_3_and_4", provider_failures_exit_3_and_4),
        ("validate_is_static_unless_resolve", validate_is_static_unless_resolve),
        ("state_commands_start_no_provider", state_commands_start_no_provider),
        ("unknown_keys_exit_3_by_default", unknown_keys_exit_3_by_default),
        ("ignore_unknown_keys_flag_warns_and_succeeds", ignore_unknown_keys_flag_warns_and_succeeds),
        ("ignore_unknown_keys_env_var_warns_and_succeeds", ignore_unknown_keys_env_var_warns_and_succeeds),
        ("ignore_unknown_keys_flag_wins_over_env_var", ignore_unknown_keys_flag_wins_over_env_var),
    ];
    let filter: Vec<String> = std::env::args().skip(1).filter(|a| !a.starts_with('-')).collect();
    let mut failed = Vec::new();
    let mut ran = 0;
    for (name, test) in tests {
        if !filter.is_empty() && !filter.iter().any(|f| name.contains(f.as_str())) {
            continue;
        }
        ran += 1;
        let ok = std::panic::catch_unwind(test).is_ok();
        println!("test {name} ... {}", if ok { "ok" } else { "FAILED" });
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

struct Env {
    scratch: Scratch,
    plugins: PathBuf,
}

impl Env {
    fn new(tag: &str) -> Self {
        let scratch = Scratch::new(&format!("cli-{tag}"));
        let plugins = install(&scratch.0);
        Env { scratch, plugins }
    }

    fn journal_path(&self, who: &str) -> PathBuf {
        self.scratch.path(&format!("{who}.journal"))
    }

    fn write_pipeline(&self, yaml: &str) -> PathBuf {
        let path = self.scratch.path("pipeline.yaml");
        std::fs::write(&path, yaml).unwrap();
        path
    }

    fn write_named(&self, name: &str, yaml: &str) -> PathBuf {
        let path = self.scratch.path(name);
        std::fs::write(&path, yaml).unwrap();
        path
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_loadsmith"));
        cmd.args(args).arg("--no-color").stdout(Stdio::piped()).stderr(Stdio::piped());
        // Strict by default, whatever the developer's shell exports.
        cmd.env_remove("LOADSMITH_IGNORE_UNKNOWN_KEYS");
        cmd
    }

    fn run(&self, pipeline: &Path) -> Command {
        let mut cmd = self.command(&["run"]);
        cmd.arg(pipeline).arg("--plugin-dir").arg(&self.plugins);
        cmd
    }

    fn basic(&self, source_extra: serde_json::Value, extra_top: &str) -> PathBuf {
        let mut src = json!({ "journal": self.journal_path("source") });
        if let (Some(s), serde_json::Value::Object(e)) = (src.as_object_mut(), source_extra) {
            s.extend(e);
        }
        self.write_pipeline(&pipeline_yaml(
            "cli",
            "fake",
            src,
            json!({ "journal": self.journal_path("destination") }),
            &timeouts(""),
            extra_top,
        ))
    }
}

#[track_caller]
fn exit_code(cmd: &mut Command) -> (i32, String) {
    let out = cmd.output().expect("run loadsmith");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.code().expect("exited normally"), text)
}

fn success_exits_0() {
    let env = Env::new("ok");
    let (code, out) = exit_code(&mut env.run(&env.basic(json!({}), "")));
    assert_eq!(code, 0, "{out}");
}

fn run_failure_exits_1() {
    let env = Env::new("fail");
    let (code, out) = exit_code(&mut env.run(&env.basic(json!({ "crash_after": 1 }), "")));
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("Status:       error"), "{out}");
    assert!(out.contains("Cause:"), "the summary must carry the root cause: {out}");
    let dst = journal(&env.journal_path("destination"));
    assert!(!dst.iter().any(|l| l == "finalize"), "{dst:?}");
}

fn usage_error_exits_2() {
    let env = Env::new("usage");
    let (code, out) = exit_code(&mut env.command(&["run"]));
    assert_eq!(code, 2, "clap usage error: {out}");
    let (code, out) = exit_code(&mut env.command(&["plugin", "install"]));
    assert_eq!(code, 2, "nothing to install: {out}");
}

fn config_error_exits_3() {
    let env = Env::new("config");
    let bad = env.write_pipeline("pipeline:\n  name: x\n  bogus_key: 1\n");
    let (code, out) = exit_code(&mut env.run(&bad));
    assert_eq!(code, 3, "{out}");
    let (code, out) = exit_code(&mut env.run(&env.scratch.path("missing.yaml")));
    assert_eq!(code, 3, "unreadable pipeline file: {out}");
    let (code, out) = exit_code(env.command(&["validate"]).arg(&bad));
    assert_eq!(code, 3, "{out}");
}

fn rejected_capability_exits_3() {
    // A sink on a destination without `object_output` is a config error found
    // only once the plugins are up; they must still be cancelled cleanly.
    let env = Env::new("cap");
    let pipeline = env.basic(json!({}), "sink:\n  type: fake\n");
    // There is no fake sink binary, so link one name to make discovery pass.
    std::os::unix::fs::symlink(
        std::env::current_exe().unwrap(),
        env.plugins.join("loadsmith-sink-fake"),
    )
    .unwrap();
    let (code, out) = exit_code(&mut env.run(&pipeline));
    assert_eq!(code, 3, "{out}");
    assert!(out.contains("object_output"), "{out}");
}

fn plugin_not_found_exits_4() {
    let env = Env::new("notfound");
    let pipeline = env.write_pipeline(&pipeline_yaml(
        "cli",
        "does-not-exist",
        json!({}),
        json!({}),
        "",
        "",
    ));
    let (code, out) = exit_code(&mut env.run(&pipeline));
    assert_eq!(code, 4, "{out}");
}

fn state_lock_held_exits_5() {
    let env = Env::new("lock");
    let state = env.scratch.path("state.json");
    let pipeline = env.basic(
        json!({ "incremental": true }),
        &format!("state:\n  backend: local\n  path: {}\n", state.display()),
    );
    // A freshly renewed lease held by someone else.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    std::fs::write(
        format!("{}.lock", state.display()),
        format!(r#"{{"holder":"another-run","renewed_at_unix_ms":{now}}}"#),
    )
    .unwrap();
    let (code, out) = exit_code(&mut env.run(&pipeline));
    assert_eq!(code, 5, "{out}");
}

/// Spawns a long run with state, waits for data to flow, signals the engine
/// and checks the graceful-cancel contract.
fn signal_run(tag: &str, signal: &str, expected: i32) {
    let env = Env::new(tag);
    let state = env.scratch.path("state.json");
    let pipeline = env.basic(
        json!({ "batches": 100000, "delay_ms": 20, "incremental": true }),
        &format!("state:\n  backend: local\n  path: {}\n", state.display()),
    );
    let child: Child = env.run(&pipeline).spawn().unwrap();
    let dst_journal = env.journal_path("destination");
    assert!(
        wait_until(Duration::from_secs(20), || journal(&dst_journal).iter().any(|l| l == "batch 3")),
        "data never flowed"
    );
    let status = Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(child.id().to_string())
        .status()
        .unwrap();
    assert!(status.success());
    let started = Instant::now();
    let out = child.wait_with_output().unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(out.status.code(), Some(expected), "{text}");
    assert!(started.elapsed() < Duration::from_secs(10), "graceful stop took too long");
    assert!(text.contains("Status:       cancelled"), "{text}");
    let dst = journal(&dst_journal);
    assert!(dst.iter().any(|l| l == "cancel"), "destination not cancelled: {dst:?}");
    assert!(!dst.iter().any(|l| l == "finalize"), "destination finalized: {dst:?}");
    let src = journal(&env.journal_path("source"));
    assert!(src.iter().any(|l| l == "cancel"), "source not cancelled: {src:?}");
    assert!(
        !Path::new(&format!("{}.lock", state.display())).exists(),
        "the state lock was not released"
    );
}

fn sigterm_cancels_and_exits_143() {
    signal_run("sigterm", "TERM", 143);
}

fn sigint_cancels_and_exits_130() {
    signal_run("sigint", "INT", 130);
}

// ── Machine-readable run summary ──────────────────────────────────────────────

/// The frozen top-level keys of run-summary version 1, in document order.
const SUMMARY_V1_KEYS: &[&str] = &[
    "version",
    "loadsmith_version",
    "pipeline",
    "status",
    "exit_code",
    "cause",
    "signal",
    "dry_run",
    "route",
    "rows_read",
    "rows_written",
    "batches",
    "duration_ms",
    "throughput_rows_per_sec",
    "started_at",
    "finished_at",
    "state",
    "sink",
];

/// Runs `cmd` and parses its stdout as exactly one JSON document.
#[track_caller]
fn json_run(cmd: &mut Command) -> (i32, serde_json::Value, String) {
    let out = cmd.output().expect("run loadsmith");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    let lines: Vec<&str> = stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 1, "stdout must hold exactly one JSON line:\n{stdout}\n--- stderr ---\n{stderr}");
    let doc: serde_json::Value = serde_json::from_str(lines[0]).expect("stdout is JSON");
    (out.status.code().expect("exited normally"), doc, stderr)
}

#[track_caller]
fn assert_v1_shape(doc: &serde_json::Value) {
    let keys: Vec<&str> = doc.as_object().expect("an object").keys().map(String::as_str).collect();
    let mut expected: Vec<&str> = SUMMARY_V1_KEYS.to_vec();
    let mut got = keys.clone();
    expected.sort_unstable();
    got.sort_unstable();
    assert_eq!(got, expected, "top-level keys of the run summary");
    assert_eq!(doc["version"], 1);
    for key in ["started_at", "finished_at"] {
        let t = doc[key].as_str().unwrap();
        assert!(t.len() == 24 && t.ends_with('Z') && &t[10..11] == "T", "{key}: {t}");
    }
    for entry in doc["route"].as_array().unwrap() {
        let mut k: Vec<&str> = entry.as_object().unwrap().keys().map(String::as_str).collect();
        k.sort_unstable();
        assert_eq!(k, ["kind", "name", "path", "type", "version"]);
    }
}

fn json_summary_on_success() {
    let env = Env::new("json-ok");
    let state = env.scratch.path("state.json");
    let pipeline = env.basic(
        json!({ "incremental": true }),
        &format!("state:\n  backend: local\n  path: {}\n", state.display()),
    );
    let (code, doc, stderr) = json_run(env.run(&pipeline).args(["--output", "json"]));
    assert_eq!(code, 0, "{doc}\n{stderr}");
    assert_v1_shape(&doc);
    assert_eq!(doc["pipeline"], "cli");
    assert_eq!(doc["status"], "success");
    assert_eq!(doc["exit_code"], 0);
    assert_eq!(doc["cause"], serde_json::Value::Null);
    assert_eq!(doc["signal"], serde_json::Value::Null);
    assert_eq!(doc["dry_run"], false);
    assert_eq!(doc["rows_read"], 30);
    assert_eq!(doc["rows_written"], 30);
    assert_eq!(doc["batches"], 3);
    let route = doc["route"].as_array().unwrap();
    assert_eq!(route.len(), 2);
    assert_eq!(route[0]["kind"], "source");
    assert_eq!(route[0]["type"], "fake");
    assert_eq!(route[0]["name"], "fake-source");
    assert_eq!(route[0]["version"], "0.0.0");
    assert!(route[0]["path"].as_str().unwrap().ends_with("loadsmith-source-fake"));
    assert_eq!(route[1]["kind"], "destination");
    assert_eq!(route[1]["name"], "fake-destination");
    // First incremental run: nothing before, the fake source's watermark after.
    assert_eq!(doc["state"], json!({ "watermark_before": null, "watermark_after": 3 }));
    assert_eq!(doc["sink"], serde_json::Value::Null);
    // The human report is not on stdout; logs still go to stderr.
    assert!(stderr.contains("schema negotiated"), "{stderr}");

    // Second run resumes from the recorded watermark.
    let (code, doc, _) = json_run(env.run(&pipeline).args(["--output", "json"]));
    assert_eq!(code, 0);
    assert_eq!(doc["state"]["watermark_before"], 3);
}

fn json_summary_on_setup_failures() {
    // Plugin not found: exit 4, a document with the cause, no counts.
    let env = Env::new("json-setup");
    let pipeline = env.write_pipeline(&pipeline_yaml("cli", "does-not-exist", json!({}), json!({}), "", ""));
    let (code, doc, stderr) = json_run(env.run(&pipeline).args(["--output", "json"]));
    assert_eq!(code, 4, "{doc}");
    assert_v1_shape(&doc);
    assert_eq!(doc["status"], "error");
    assert_eq!(doc["exit_code"], 4);
    assert!(doc["cause"].as_str().unwrap().contains("not found"), "{doc}");
    assert_eq!(doc["route"][0]["type"], "does-not-exist");
    assert_eq!(doc["route"][0]["path"], serde_json::Value::Null);
    assert_eq!(doc["rows_read"], 0);
    assert!(stderr.contains("error:"), "the error line stays on stderr: {stderr}");

    // An unreadable pipeline file: exit 3, nothing known about the pipeline.
    let (code, doc, _) =
        json_run(env.run(&env.scratch.path("missing.yaml")).args(["--output", "json"]));
    assert_eq!(code, 3);
    assert_v1_shape(&doc);
    assert_eq!(doc["pipeline"], serde_json::Value::Null);
    assert_eq!(doc["route"], json!([]));

    // A held lock with --summary-file: exit 5, the file holds the document,
    // stdout keeps the human report (there is none before the lock).
    let state = env.scratch.path("state.json");
    let pipeline = env.basic(
        json!({ "incremental": true }),
        &format!("state:\n  backend: local\n  path: {}\n", state.display()),
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    std::fs::write(
        format!("{}.lock", state.display()),
        format!(r#"{{"holder":"another-run","renewed_at_unix_ms":{now}}}"#),
    )
    .unwrap();
    let file = env.scratch.path("summary.json");
    let (code, _) = exit_code(env.run(&pipeline).arg("--summary-file").arg(&file));
    assert_eq!(code, 5);
    let doc: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
    assert_v1_shape(&doc);
    assert_eq!(doc["exit_code"], 5);
    assert_eq!(doc["status"], "error");
    assert!(doc["cause"].as_str().unwrap().contains("locked"), "{doc}");
    assert_eq!(doc["state"], json!({ "watermark_before": null, "watermark_after": null }));
}

fn json_summary_on_run_failure() {
    let env = Env::new("json-fail");
    let (code, doc, _) =
        json_run(env.run(&env.basic(json!({ "crash_after": 1 }), "")).args(["--output", "json"]));
    assert_eq!(code, 1);
    assert_v1_shape(&doc);
    assert_eq!(doc["status"], "error");
    assert_eq!(doc["exit_code"], 1);
    assert!(doc["cause"].as_str().unwrap().contains("source"), "{doc}");
    assert_eq!(doc["route"][0]["name"], "fake-source");
}

fn json_summary_when_cancelled() {
    let env = Env::new("json-cancel");
    let file = env.scratch.path("summary.json");
    let pipeline = env.basic(json!({ "batches": 100000, "delay_ms": 20 }), "");
    let mut cmd = env.run(&pipeline);
    cmd.args(["--output", "json", "--summary-file"]).arg(&file);
    let child: Child = cmd.spawn().unwrap();
    let dst_journal = env.journal_path("destination");
    assert!(
        wait_until(Duration::from_secs(20), || journal(&dst_journal).iter().any(|l| l == "batch 3")),
        "data never flowed"
    );
    assert!(Command::new("kill").arg("-TERM").arg(child.id().to_string()).status().unwrap().success());
    let out = child.wait_with_output().unwrap();
    assert_eq!(out.status.code(), Some(143));
    let stdout = String::from_utf8_lossy(&out.stdout);
    let doc: serde_json::Value = serde_json::from_str(stdout.trim()).expect("one JSON document");
    assert_v1_shape(&doc);
    assert_eq!(doc["status"], "cancelled");
    assert_eq!(doc["signal"], "SIGTERM");
    assert_eq!(doc["exit_code"], 143);
    assert!(doc["rows_read"].as_u64().unwrap() >= 30, "{doc}");
    // The file holds the same document (pretty-printed).
    let from_file: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
    assert_eq!(from_file, doc);
}

fn json_summary_for_a_dry_run() {
    let env = Env::new("json-dry");
    let (code, doc, _) = json_run(env.run(&env.basic(json!({}), "")).args(["--dry-run", "--output", "json"]));
    assert_eq!(code, 0);
    assert_v1_shape(&doc);
    assert_eq!(doc["dry_run"], true);
    assert_eq!(doc["status"], "success");
}

fn output_json_conflicts_with_print_resolved_config() {
    let env = Env::new("json-conflict");
    let pipeline = env.basic(json!({}), "");
    let (code, out) =
        exit_code(env.run(&pipeline).args(["--output", "json", "--print-resolved-config"]));
    assert_eq!(code, 2, "{out}");
}

// ── Providers ─────────────────────────────────────────────────────────────────

/// A value with a long second line, as a PEM key would have.
const SECRET: &str = "Sup3r-S3cret-Value\nsecond-line-of-the-key";

impl Env {
    /// The fake source with `password` (and `extra`) from `secret()`, and a
    /// `creds` instance of the fake provider configured with `provider`.
    fn with_provider(&self, source: serde_json::Value, provider: serde_json::Value, extra_top: &str) -> PathBuf {
        let mut src = json!({
            "journal": self.journal_path("source"),
            "password": "{{ secret('creds', 'db#pw') }}",
        });
        if let (Some(s), serde_json::Value::Object(e)) = (src.as_object_mut(), source) {
            s.extend(e);
        }
        let mut prov = json!({ "journal": self.journal_path("creds") });
        if let (Some(p), serde_json::Value::Object(e)) = (prov.as_object_mut(), provider) {
            p.extend(e);
        }
        self.write_pipeline(&pipeline_yaml(
            "cli",
            "fake",
            src,
            json!({ "journal": self.journal_path("destination") }),
            &timeouts(""),
            &format!("providers:\n  creds:\n    type: fake\n    config: {prov}\n{extra_top}"),
        ))
    }
}

#[track_caller]
fn assert_no_secret(what: &str, text: &str) {
    for part in SECRET.lines().chain([SECRET]) {
        assert!(!text.contains(part), "{what} leaks {part:?}:\n{text}");
    }
}

fn provider_secret_never_appears_in_output() {
    let env = Env::new("prov-mask");
    // Both the source and the provider log the value on fd4; the source
    // also puts it in its failure message.
    let leaky = json!({ "values": { "db#pw": SECRET }, "leak": true });
    let ok = env.with_provider(json!({ "leak": true }), leaky.clone(), "");
    let file = env.scratch.path("summary.json");
    let out = env
        .run(&ok)
        .args(["--log-level", "debug", "--summary-file"])
        .arg(&file)
        .output()
        .unwrap();
    let (stdout, stderr) = (String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert_eq!(out.status.code(), Some(0), "{stdout}{stderr}");
    assert_no_secret("stdout", &stdout);
    assert_no_secret("stderr", &stderr);
    assert!(stderr.contains("configured with") && stderr.contains("***"), "the leak was logged, masked: {stderr}");
    assert_no_secret("summary file", &std::fs::read_to_string(&file).unwrap());
    // The value did reach the source (its journal is a file, not output).
    let src = journal(&env.journal_path("source"));
    assert!(src.iter().any(|l| l.starts_with("password=Sup3r-S3cret-Value")), "{src:?}");

    // --print-resolved-config and --dry-run resolve providers too, masked.
    let (code, out) = exit_code(env.run(&ok).arg("--print-resolved-config"));
    assert_eq!(code, 0, "{out}");
    assert_no_secret("print-resolved-config", &out);
    assert!(out.contains("password:") && out.contains("***"), "{out}");
    let resolves = || journal(&env.journal_path("creds")).iter().filter(|l| *l == "resolve db#pw").count();
    let before = resolves();
    let (code, out) = exit_code(env.run(&ok).arg("--dry-run"));
    assert_eq!(code, 0, "{out}");
    assert_eq!(resolves(), before + 1, "--dry-run must resolve providers");
    assert_no_secret("dry-run", &out);

    // A failure whose message carries the value: exit 1, masked everywhere.
    let failing = env.with_provider(json!({ "leak": true, "fail_after": 1 }), leaky, "");
    let (code, doc, stderr) = json_run(env.run(&failing).args(["--output", "json"]));
    assert_eq!(code, 1, "{doc}\n{stderr}");
    let cause = doc["cause"].as_str().unwrap();
    assert!(cause.contains("(password \"***\")"), "{cause}");
    assert_no_secret("json summary", &doc.to_string());
    assert_no_secret("stderr", &stderr);
}

fn provider_failures_exit_3_and_4() {
    let env = Env::new("prov-fail");
    // A reference the provider cannot resolve: config class, nothing ran.
    let missing = env.with_provider(json!({}), json!({ "values": {} }), "");
    let base = std::fs::read_to_string(&missing).unwrap();
    let (code, doc, stderr) = json_run(env.run(&missing).args(["--output", "json"]));
    assert_eq!(code, 3, "{doc}\n{stderr}");
    assert_v1_shape(&doc);
    assert_eq!(doc["pipeline"], "cli", "the static check names the pipeline: {doc}");
    assert!(doc["cause"].as_str().unwrap().contains("provider 'creds' could not resolve 'db#pw'"), "{doc}");
    assert!(stderr.contains("error: configuration error: provider 'creds' could not resolve"), "{stderr}");
    assert!(journal(&env.journal_path("source")).is_empty(), "the source must not start");

    // A provider that times out: config class too.
    let slow = env.with_provider(json!({}), json!({ "hang": true }), "    timeout_seconds: 1\n");
    let (code, out) = exit_code(&mut env.run(&slow));
    assert_eq!(code, 3, "{out}");
    assert!(out.contains("did not finish within 1s"), "{out}");

    // An undeclared instance: static, before anything runs.
    let undeclared = env.write_named(
        "undeclared.yaml",
        &base.replace("secret('creds'", "secret('vault'"),
    );
    let (code, out) = exit_code(&mut env.run(&undeclared));
    assert_eq!(code, 3, "{out}");
    assert!(out.contains("'vault'") && out.contains("not declared"), "{out}");

    // A provider binary that is not installed: plugin class.
    let absent = env.write_named(
        "absent.yaml",
        &base.replace("type: fake\n    config: {\"journal\"", "type: absent\n    config: {\"journal\""),
    );
    let (code, out) = exit_code(&mut env.run(&absent));
    assert_eq!(code, 4, "{out}");
    assert!(out.contains("provider instance 'creds'") && out.contains("loadsmith-provider-absent"), "{out}");
}

fn validate_is_static_unless_resolve() {
    let env = Env::new("prov-validate");
    let pipeline = env.with_provider(
        json!({ "host": "{{ env('LS_CLI_TEST_UNSET_VAR') }}" }),
        json!({ "values": { "db#pw": SECRET } }),
        "",
    );
    // Static: an unset variable and an uninstalled provider are fine.
    let (code, out) = exit_code(env.command(&["validate"]).arg(&pipeline));
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("static check"), "{out}");
    assert!(journal(&env.journal_path("creds")).is_empty(), "static validate starts nothing");
    // --resolve: the unset variable now fails (config class).
    let (code, out) = exit_code(
        env.command(&["validate", "--resolve"]).arg("--plugin-dir").arg(&env.plugins).arg(&pipeline),
    );
    assert_eq!(code, 3, "{out}");
    assert!(out.contains("LS_CLI_TEST_UNSET_VAR"), "{out}");
    // With the variable set, --resolve runs the provider.
    let (code, out) = exit_code(
        env.command(&["validate", "--resolve"])
            .arg("--plugin-dir")
            .arg(&env.plugins)
            .arg(&pipeline)
            .env("LS_CLI_TEST_UNSET_VAR", "db.local"),
    );
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("fully resolved; 1 secret() reference resolved"), "{out}");
    assert_eq!(journal(&env.journal_path("creds")), ["configure", "resolve db#pw"]);
    assert_no_secret("validate --resolve", &out);
}

fn state_commands_start_no_provider() {
    let env = Env::new("prov-state");
    let state = env.scratch.path("state.json");
    // The provider type is not installed and the source has an unset
    // variable: neither matters to the state commands.
    let pipeline = env.with_provider(
        json!({ "host": "{{ env('LS_CLI_TEST_UNSET_VAR') }}" }),
        json!({}),
        &format!("state:\n  backend: local\n  path: {}\n", state.display()),
    );
    let pipeline = env.write_pipeline(
        &std::fs::read_to_string(&pipeline).unwrap().replace("    type: fake\n    config: {\"journal\"", "    type: absent\n    config: {\"journal\""),
    );
    let (code, out) = exit_code(env.command(&["state", "show"]).arg(&pipeline));
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("No state recorded for pipeline 'cli'"), "{out}");
    let (code, out) = exit_code(env.command(&["state", "rm"]).arg(&pipeline));
    assert_eq!(code, 0, "{out}");
}

// ── Unknown keys ─────────────────────────────────────────────────────────────

impl Env {
    /// The fake source → `destination-typed` (which parses its config with
    /// the SDK helper), with a typo in the destination's config (`fail_aftr`)
    /// when `plugin_typo`, and an unknown top-level key (`x-generator`) when
    /// `envelope_typo`. With a `state:` block for the state commands.
    fn with_unknown_keys(&self, plugin_typo: bool, envelope_typo: bool) -> PathBuf {
        let mut dst = json!({ "journal": self.journal_path("destination") });
        if plugin_typo {
            dst["fail_aftr"] = json!(0);
        }
        let state = self.scratch.path("state.json");
        let mut yaml = format!(
            "pipeline:\n  name: cli\n  heartbeat_seconds: 0\n\
             source:\n  type: fake\n  config: {{ batches: 1, incremental: true }}\n\
             destination:\n  type: typed\n  config: {dst}\n\
             state:\n  backend: local\n  path: {}\n",
            state.display()
        );
        if envelope_typo {
            yaml.push_str("x-generator:\n  tool: gen\n");
        }
        self.write_pipeline(&yaml)
    }
}

const IGNORE_ENV: &str = "LOADSMITH_IGNORE_UNKNOWN_KEYS";

fn unknown_keys_exit_3_by_default() {
    let env = Env::new("keys-default");
    // A typo in a plugin's config: the plugin rejects it in `configure`.
    let plugin = env.with_unknown_keys(true, false);
    let (code, out) = exit_code(&mut env.run(&plugin));
    assert_eq!(code, 3, "{out}");
    assert!(out.contains("UNKNOWN_CONFIG_KEYS") && out.contains("`fail_aftr`"), "{out}");
    assert!(out.contains("--ignore-unknown-keys"), "the error names the way out: {out}");
    assert_eq!(journal(&env.journal_path("destination")), ["unknown_keys=error"]);
    // An unknown key in the file's own blocks: every command that reads the
    // file fails, before any plugin starts.
    let envelope = env.with_unknown_keys(false, true);
    for args in [&["validate"][..], &["state", "show"], &["state", "rm"]] {
        let (code, out) = exit_code(env.command(args).arg(&envelope));
        assert_eq!(code, 3, "{args:?}: {out}");
        assert!(out.contains("`x-generator`"), "{args:?}: {out}");
    }
    let (code, out) = exit_code(&mut env.run(&envelope));
    assert_eq!(code, 3, "{out}");
    assert_eq!(journal(&env.journal_path("destination")), ["unknown_keys=error"], "nothing new ran");
}

fn ignore_unknown_keys_flag_warns_and_succeeds() {
    let env = Env::new("keys-flag");
    let pipeline = env.with_unknown_keys(true, true);
    let (code, out) = exit_code(env.run(&pipeline).arg("--ignore-unknown-keys"));
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("ignoring unknown key `x-generator` in the pipeline file"), "{out}");
    assert!(out.contains("ignoring unknown config key `fail_aftr`"), "{out}");
    assert_eq!(journal(&env.journal_path("destination"))[..2], ["unknown_keys=warn", "configure"]);
    for args in [&["validate"][..], &["state", "show"], &["state", "rm"]] {
        let (code, out) = exit_code(env.command(args).arg("--ignore-unknown-keys").arg(&pipeline));
        assert_eq!(code, 0, "{args:?}: {out}");
        assert!(out.contains("`x-generator`"), "{args:?} warns: {out}");
    }
}

fn ignore_unknown_keys_env_var_warns_and_succeeds() {
    let env = Env::new("keys-env");
    let pipeline = env.with_unknown_keys(true, true);
    for on in ["1", "true", "YES"] {
        let (code, out) = exit_code(env.run(&pipeline).env(IGNORE_ENV, on));
        assert_eq!(code, 0, "{IGNORE_ENV}={on}: {out}");
        assert!(out.contains("ignoring unknown config key `fail_aftr`"), "{out}");
    }
    for off in ["0", "false", ""] {
        let (code, out) = exit_code(env.run(&pipeline).env(IGNORE_ENV, off));
        assert_eq!(code, 3, "{IGNORE_ENV}={off}: {out}");
    }
}

fn ignore_unknown_keys_flag_wins_over_env_var() {
    let env = Env::new("keys-precedence");
    let pipeline = env.with_unknown_keys(true, true);
    let (code, out) = exit_code(env.run(&pipeline).arg("--ignore-unknown-keys=false").env(IGNORE_ENV, "1"));
    assert_eq!(code, 3, "the flag turns the variable off: {out}");
    let (code, out) = exit_code(env.run(&pipeline).arg("--ignore-unknown-keys").env(IGNORE_ENV, "0"));
    assert_eq!(code, 0, "the flag turns it on despite the variable: {out}");
    let (code, out) = exit_code(env.run(&pipeline).arg("--ignore-unknown-keys=true").env(IGNORE_ENV, "bogus"));
    assert_eq!(code, 0, "with the flag the variable is not read: {out}");
    let (code, out) = exit_code(env.run(&pipeline).env(IGNORE_ENV, "bogus"));
    assert_eq!(code, 2, "an unreadable variable is a usage error: {out}");
    assert!(out.contains(IGNORE_ENV), "{out}");
}
