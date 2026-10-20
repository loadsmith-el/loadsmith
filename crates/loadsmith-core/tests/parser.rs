//! End-to-end tests of the parser stage: a raw source announces files, the
//! core hands each to the parser, the parser's Arrow stream goes through the
//! pump to the destination. The real runner drives real plugin processes
//! (the fake plugins in `support/fake_plugins.rs`, which are this very
//! executable).
//!
//! `harness = false`: `main` first checks whether it was started as a plugin,
//! then runs the tests one after the other. Filter with a substring argument:
//! `cargo test -p loadsmith-core --test parser -- mismatch`.

#[path = "support/fake_plugins.rs"]
mod fake_plugins;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use fake_plugins::{install, journal, timeouts, write_fake_file, Scratch};
use loadsmith_core::{run_pipeline, CoreError, Halt, RunOpts, Shutdown, StopSignal, Summary, SummaryStatus};
use serde_json::json;

fn main() {
    fake_plugins::run_as_plugin_if_invoked();

    let tests: &[(&str, fn())] = &[
        ("happy_path_parses_every_file_in_order", happy_path_parses_every_file_in_order),
        ("raw_source_failure_cancels_the_parser", raw_source_failure_cancels_the_parser),
        ("raw_source_crash_cancels_the_parser", raw_source_crash_cancels_the_parser),
        ("parser_crash_aborts_the_run", parser_crash_aborts_the_run),
        ("parser_error_aborts_the_run", parser_error_aborts_the_run),
        ("schema_mismatch_fails_the_run", schema_mismatch_fails_the_run),
        ("zero_files_without_a_declared_schema_is_no_input", zero_files_without_a_declared_schema_is_no_input),
        ("zero_files_with_a_declared_schema_is_an_empty_load", zero_files_with_a_declared_schema_is_an_empty_load),
        ("parser_needs_a_raw_source", parser_needs_a_raw_source),
        ("raw_source_needs_a_parser", raw_source_needs_a_parser),
        ("missing_parser_binary_is_plugin_not_found", missing_parser_binary_is_plugin_not_found),
        ("signal_during_parse_cancels_everything", signal_during_parse_cancels_everything),
        ("download_scratch_is_removed_after_success", download_scratch_is_removed_after_success),
        ("download_scratch_is_kept_after_a_failure", download_scratch_is_kept_after_a_failure),
        ("a_user_scratch_dir_is_never_removed", a_user_scratch_dir_is_never_removed),
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

struct Run {
    scratch: Scratch,
    plugins: PathBuf,
}

impl Run {
    fn new(tag: &str) -> Self {
        let scratch = Scratch::new(tag);
        let plugins = install(&scratch.0);
        Run { scratch, plugins }
    }

    fn journal_path(&self, who: &str) -> PathBuf {
        self.scratch.path(&format!("{who}.journal"))
    }

    fn journal(&self, who: &str) -> Vec<String> {
        journal(&self.journal_path(who))
    }

    /// Writes `name` with `columns` and `rows` and returns its path.
    fn file(&self, name: &str, columns: &[&str], rows: u64) -> String {
        let path = self.scratch.path(name);
        write_fake_file(&path, columns, rows);
        path.display().to_string()
    }

    /// A pipeline: the `source_type` raw source → `fake` parser → `fake`
    /// destination, each with a journal and the given extra config.
    fn yaml(
        &self,
        source_type: &str,
        source: serde_json::Value,
        parser: Option<serde_json::Value>,
        timeouts_fields: &str,
    ) -> String {
        let with_journal = |who: &str, extra: serde_json::Value| {
            let mut c = json!({ "journal": self.journal_path(who) });
            if let (Some(b), serde_json::Value::Object(e)) = (c.as_object_mut(), extra) {
                b.extend(e);
            }
            c
        };
        let parser_block = match parser {
            Some(cfg) => format!("parser:\n  type: fake\n  config: {}\n", with_journal("parser", cfg)),
            None => String::new(),
        };
        format!(
            "pipeline:\n  name: parse\n  heartbeat_seconds: 0\n{}\
             source:\n  type: {source_type}\n  config: {}\n{parser_block}\
             destination:\n  type: fake\n  config: {}\n",
            timeouts(timeouts_fields),
            with_journal("source", source),
            with_journal("destination", json!({})),
        )
    }

    fn exec_with(&self, yaml: &str, shutdown: Shutdown) -> Result<Summary, CoreError> {
        let (config, _) = loadsmith_config::parse_pipeline_yaml(yaml).expect("pipeline yaml");
        let mut opts = RunOpts::new(&self.plugins);
        opts.color = false;
        opts.shutdown = shutdown;
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        rt.block_on(run_pipeline(config, opts))
    }

    fn exec(&self, yaml: &str) -> Result<Summary, CoreError> {
        self.exec_with(yaml, Shutdown::never())
    }
}

fn has(lines: &[String], line: &str) -> bool {
    lines.iter().any(|l| l == line)
}

#[track_caller]
fn assert_rolled_back(dst: &[String]) {
    assert!(!has(dst, "finalize"), "destination finalized an aborted load: {dst:?}");
}

#[track_caller]
fn assert_cause(summary: &Summary, needle: &str) {
    let cause = summary.root_cause.as_deref().unwrap_or("");
    assert!(cause.contains(needle), "root cause {cause:?} does not mention {needle:?}");
}

/// The `scratch <dir>` the raw source was configured with.
fn scratch_of(source_journal: &[String]) -> PathBuf {
    let line = source_journal.iter().find_map(|l| l.strip_prefix("scratch ")).expect("a scratch dir");
    PathBuf::from(line)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

fn happy_path_parses_every_file_in_order() {
    let run = Run::new("p-happy");
    let files = [run.file("a.txt", &["id", "n"], 3), run.file("b.txt", &["id", "n"], 4), run.file("c.txt", &["id", "n"], 1)];
    // A file announced twice is parsed once.
    let listed = json!([files[0], files[1], files[1], files[2]]);
    let yaml = run.yaml("files", json!({ "files": listed }), Some(json!({})), "");
    let summary = run.exec(&yaml).expect("run");
    assert_eq!(summary.status, SummaryStatus::Success, "{:?}", summary.root_cause);
    assert_eq!(summary.rows_read, 8);
    assert_eq!(summary.rows_written, 8);
    // 2 rows per batch: 2 + 2 + 1 batches.
    assert_eq!(summary.batches_read, 5);
    assert_eq!(summary.parser_name.as_deref(), Some("fake-parser"));
    let prs = run.journal("parser");
    let opened: Vec<&String> = prs.iter().filter(|l| l.starts_with("open ")).collect();
    assert_eq!(opened, ["open a.txt", "open b.txt", "open c.txt"]);
    assert!(!has(&prs, "cancel"), "{prs:?}");
    let dst = run.journal("destination");
    assert_eq!(dst.iter().filter(|l| *l == "finalize").count(), 1, "{dst:?}");
    assert!(!has(&dst, "cancel"), "{dst:?}");
    assert!(!has(&run.journal("source"), "cancel"));
}

/// The rule this pins: after an upstream failure the parser is cancelled —
/// never told "no more files" — so it never ends its stream, and the
/// destination never finalizes the files parsed so far.
fn raw_source_failure_cancels_the_parser() {
    let run = Run::new("p-rawfail");
    let files: Vec<String> = (0..5).map(|i| run.file(&format!("f{i}.txt"), &["id"], 2)).collect();
    let yaml = run.yaml("files", json!({ "files": files, "fail_after": 3, "delay_ms": 50 }), Some(json!({})), "");
    let summary = run.exec(&yaml).expect("data-phase failures are a summary");
    assert_eq!(summary.status, SummaryStatus::Error);
    assert_cause(&summary, "OBJECT_FAILED");
    let prs = run.journal("parser");
    assert!(has(&prs, "cancel"), "the parser was not cancelled: {prs:?}");
    assert!(has(&prs, "open f2.txt"), "{prs:?}");
    let dst = run.journal("destination");
    assert!(has(&dst, "cancel"), "{dst:?}");
    assert_rolled_back(&dst);
}

fn raw_source_crash_cancels_the_parser() {
    let run = Run::new("p-rawcrash");
    let files: Vec<String> = (0..5).map(|i| run.file(&format!("f{i}.txt"), &["id"], 2)).collect();
    let yaml = run.yaml("files", json!({ "files": files, "crash_after": 2, "delay_ms": 50 }), Some(json!({})), "");
    let summary = run.exec(&yaml).expect("summary");
    assert_eq!(summary.status, SummaryStatus::Error);
    assert_cause(&summary, "source exited with status 46");
    assert!(has(&run.journal("parser"), "cancel"), "{:?}", run.journal("parser"));
    assert_rolled_back(&run.journal("destination"));
}

fn parser_crash_aborts_the_run() {
    let run = Run::new("p-prscrash");
    let files: Vec<String> = (0..4).map(|i| run.file(&format!("f{i}.txt"), &["id"], 2)).collect();
    let yaml = run.yaml(
        "files",
        json!({ "files": files, "delay_ms": 100 }),
        Some(json!({ "crash_on": "f1.txt" })),
        "",
    );
    let summary = run.exec(&yaml).expect("summary");
    assert_eq!(summary.status, SummaryStatus::Error);
    assert_cause(&summary, "parser");
    let dst = run.journal("destination");
    assert!(has(&dst, "cancel"), "{dst:?}");
    assert_rolled_back(&dst);
    // The raw source was still listing: it is cancelled too.
    assert!(has(&run.journal("source"), "cancel"), "{:?}", run.journal("source"));
}

fn parser_error_aborts_the_run() {
    let run = Run::new("p-prserr");
    let files: Vec<String> = (0..3).map(|i| run.file(&format!("f{i}.txt"), &["id"], 2)).collect();
    let yaml = run.yaml("files", json!({ "files": files }), Some(json!({ "fail_on": "f1.txt" })), "");
    let summary = run.exec(&yaml).expect("summary");
    assert_eq!(summary.status, SummaryStatus::Error);
    assert_cause(&summary, "parser reported an error: OPEN_FAILED");
    assert_rolled_back(&run.journal("destination"));
}

fn schema_mismatch_fails_the_run() {
    let run = Run::new("p-mismatch");
    let files = [run.file("a.txt", &["id"], 3), run.file("b.txt", &["id", "extra"], 3)];
    let yaml = run.yaml("files", json!({ "files": files }), Some(json!({})), "");
    let summary = run.exec(&yaml).expect("summary");
    assert_eq!(summary.status, SummaryStatus::Error);
    assert_cause(&summary, "SCHEMA_MISMATCH");
    assert_cause(&summary, "b.txt: 2 columns, the stream has 1");
    let dst = run.journal("destination");
    assert!(has(&dst, "cancel"), "{dst:?}");
    assert_rolled_back(&dst);
}

/// No file and no declared schema: the parser cannot describe the stream,
/// so the run fails before the destination starts.
fn zero_files_without_a_declared_schema_is_no_input() {
    let run = Run::new("p-noinput");
    let yaml = run.yaml("files", json!({ "files": [] }), Some(json!({})), "");
    match run.exec(&yaml) {
        Err(CoreError::PluginFailed(msg)) => assert!(msg.contains("NO_INPUT"), "{msg}"),
        other => panic!("expected a NO_INPUT failure, got {other:?}"),
    }
    assert!(!has(&run.journal("destination"), "prepare"), "{:?}", run.journal("destination"));
}

fn zero_files_with_a_declared_schema_is_an_empty_load() {
    let run = Run::new("p-empty");
    let yaml = run.yaml("files", json!({ "files": [] }), Some(json!({ "columns": ["id"] })), "");
    let summary = run.exec(&yaml).expect("run");
    assert_eq!(summary.status, SummaryStatus::Success, "{:?}", summary.root_cause);
    assert_eq!((summary.rows_read, summary.rows_written), (0, 0));
    assert!(has(&run.journal("destination"), "finalize"));
}

fn parser_needs_a_raw_source() {
    let run = Run::new("p-gate1");
    let yaml = run.yaml("fake", json!({}), Some(json!({})), "");
    match run.exec(&yaml) {
        Err(CoreError::Config(e)) => assert!(e.to_string().contains("object_output"), "{e}"),
        other => panic!("expected a capability error, got {other:?}"),
    }
    // Nothing started: the gate runs before `start`.
    assert!(!has(&run.journal("source"), "start"), "{:?}", run.journal("source"));
    assert!(!has(&run.journal("destination"), "prepare"));
}

fn raw_source_needs_a_parser() {
    let run = Run::new("p-gate2");
    let file = run.file("a.txt", &["id"], 1);
    let yaml = run.yaml("files", json!({ "files": [file] }), None, "");
    match run.exec(&yaml) {
        Err(CoreError::Config(e)) => {
            let e = e.to_string();
            assert!(e.contains("produces files") && e.contains("parser:"), "{e}");
        }
        other => panic!("expected a capability error, got {other:?}"),
    }
    assert!(!has(&run.journal("source"), "file a.txt"), "{:?}", run.journal("source"));
}

fn missing_parser_binary_is_plugin_not_found() {
    let run = Run::new("p-missing");
    let yaml = run.yaml("files", json!({ "files": [] }), Some(json!({})), "").replace("type: fake\n  config: {\"journal\":", "type: nosuch\n  config: {\"journal\":");
    match run.exec(&yaml) {
        Err(CoreError::PluginNotFound(msg)) => assert!(msg.contains("loadsmith-parser-nosuch"), "{msg}"),
        other => panic!("expected PluginNotFound, got {other:?}"),
    }
}

fn signal_during_parse_cancels_everything() {
    let run = Run::new("p-sigterm");
    let files: Vec<String> = (0..200).map(|i| run.file(&format!("f{i}.txt"), &["id"], 4)).collect();
    let yaml = run.yaml("files", json!({ "files": files, "delay_ms": 20 }), Some(json!({ "delay_ms": 10 })), "");
    let (trigger, shutdown) = Shutdown::channel();
    let dst_journal = run.journal_path("destination");
    let watcher = std::thread::spawn(move || {
        fake_plugins::wait_until(Duration::from_secs(20), || journal(&dst_journal).iter().any(|l| l == "batch 3"));
        trigger.trigger(Halt::Signal(StopSignal::Terminate));
    });
    let summary = run.exec_with(&yaml, shutdown).expect("summary");
    watcher.join().unwrap();
    assert_eq!(summary.status, SummaryStatus::Cancelled);
    assert_eq!(summary.signal, Some(StopSignal::Terminate));
    let dst = run.journal("destination");
    assert!(has(&dst, "cancel"), "{dst:?}");
    assert_rolled_back(&dst);
    assert!(has(&run.journal("parser"), "cancel"), "{:?}", run.journal("parser"));
    assert!(has(&run.journal("source"), "cancel"), "{:?}", run.journal("source"));
}

/// A raw source that downloads (capability `scratch_dir`) gets a
/// core-allocated directory, removed after a successful run.
fn download_scratch_is_removed_after_success() {
    let run = Run::new("p-dl-ok");
    let files = [run.file("a.txt", &["id"], 3), run.file("b.txt", &["id"], 2)];
    let yaml = run.yaml("files", json!({ "files": files, "download": true }), Some(json!({})), "");
    let summary = run.exec(&yaml).expect("run");
    assert_eq!(summary.status, SummaryStatus::Success, "{:?}", summary.root_cause);
    assert_eq!(summary.rows_written, 5);
    let dir = scratch_of(&run.journal("source"));
    assert!(dir.starts_with(std::env::temp_dir()), "{dir:?}");
    assert!(!dir.exists(), "the core-allocated scratch dir was not removed");
    // The originals are never touched.
    assert!(std::path::Path::new(&files[0]).exists());
}

fn download_scratch_is_kept_after_a_failure() {
    let run = Run::new("p-dl-fail");
    let files = [run.file("a.txt", &["id"], 3), run.file("b.txt", &["id", "x"], 2)];
    let yaml = run.yaml("files", json!({ "files": files, "download": true }), Some(json!({})), "");
    let summary = run.exec(&yaml).expect("summary");
    assert_eq!(summary.status, SummaryStatus::Error);
    let dir = scratch_of(&run.journal("source"));
    assert!(dir.join("a.txt").exists(), "downloaded files must be kept after a failure");
    std::fs::remove_dir_all(&dir).unwrap();
}

fn a_user_scratch_dir_is_never_removed() {
    let run = Run::new("p-dl-user");
    let mine = run.scratch.path("mine");
    std::fs::create_dir_all(&mine).unwrap();
    let files = [run.file("a.txt", &["id"], 3)];
    let yaml = run.yaml(
        "files",
        json!({ "files": files, "download": true, "scratch_dir": mine }),
        Some(json!({})),
        "",
    );
    let summary = run.exec(&yaml).expect("run");
    assert_eq!(summary.status, SummaryStatus::Success, "{:?}", summary.root_cause);
    assert_eq!(scratch_of(&run.journal("source")), mine);
    assert!(mine.join("a.txt").exists(), "a user-given scratch dir is left alone");
}
