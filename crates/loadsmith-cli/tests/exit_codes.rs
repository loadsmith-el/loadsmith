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

    fn command(&self, args: &[&str]) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_loadsmith"));
        cmd.args(args).arg("--no-color").stdout(Stdio::piped()).stderr(Stdio::piped());
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
