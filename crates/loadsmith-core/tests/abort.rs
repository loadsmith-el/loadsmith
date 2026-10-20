//! End-to-end tests of the abort rules, timeouts and the `resend` rule: the
//! real runner drives real plugin processes (the fake plugins in
//! `support/fake_plugins.rs`, which are this very executable).
//!
//! `harness = false`: `main` first checks whether it was started as a plugin,
//! then runs the tests one after the other. Filter with a substring argument:
//! `cargo test -p loadsmith-core --test abort -- stall`.

#[path = "support/fake_plugins.rs"]
mod fake_plugins;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fake_plugins::{install, journal, pipeline_yaml, timeouts, Scratch};
use loadsmith_core::{
    run_pipeline, CoreError, Halt, RunOpts, Shutdown, StopSignal, Summary, SummaryStatus,
};
use serde_json::json;

fn main() {
    fake_plugins::run_as_plugin_if_invoked();

    let tests: &[(&str, fn())] = &[
        ("happy_path_finalizes_once", happy_path_finalizes_once),
        ("source_crash_mid_stream_cancels_destination", source_crash_mid_stream_cancels_destination),
        ("source_finished_error_cancels_destination", source_finished_error_cancels_destination),
        ("destination_crash_cancels_source", destination_crash_cancels_source),
        ("destination_error_cancels_source", destination_error_cancels_source),
        ("signal_cancels_both_and_releases_the_lock", signal_cancels_both_and_releases_the_lock),
        ("signal_before_start_is_an_interrupted_error", signal_before_start_is_an_interrupted_error),
        ("stall_timeout_kills_a_deaf_source", stall_timeout_kills_a_deaf_source),
        ("first_batch_timeout", first_batch_timeout),
        ("finalize_timeout", finalize_timeout),
        ("malformed_line_once_recovers_with_resend", malformed_line_once_recovers_with_resend),
        ("malformed_line_twice_is_fatal", malformed_line_twice_is_fatal),
        ("ignored_resend_times_out", ignored_resend_times_out),
        ("unknown_message_types_are_ignored", unknown_message_types_are_ignored),
        ("state_lock_released_after_a_crash", state_lock_released_after_a_crash),
        ("schema_drift_is_detected_from_the_arrow_stream", schema_drift_is_detected_from_the_arrow_stream),
        ("v1_state_file_is_migrated", v1_state_file_is_migrated),
        ("source_without_a_stream_header_fails_setup", source_without_a_stream_header_fails_setup),
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

    fn src_config(&self, extra: serde_json::Value) -> serde_json::Value {
        let mut c = json!({ "journal": self.journal_path("source") });
        merge(&mut c, extra);
        c
    }

    fn dst_config(&self, extra: serde_json::Value) -> serde_json::Value {
        let mut c = json!({ "journal": self.journal_path("destination") });
        merge(&mut c, extra);
        c
    }

    /// Runs `yaml` with the given shutdown input, on a fresh runtime.
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

fn merge(base: &mut serde_json::Value, extra: serde_json::Value) {
    if let (Some(b), serde_json::Value::Object(e)) = (base.as_object_mut(), extra) {
        b.extend(e);
    }
}

fn has(lines: &[String], line: &str) -> bool {
    lines.iter().any(|l| l == line)
}

#[track_caller]
fn assert_rolled_back(dst: &[String]) {
    assert!(has(dst, "cancel"), "destination was not cancelled: {dst:?}");
    assert!(!has(dst, "finalize"), "destination finalized an aborted load: {dst:?}");
}

#[track_caller]
fn assert_cause(summary: &Summary, needle: &str) {
    let cause = summary.root_cause.as_deref().unwrap_or("");
    assert!(cause.contains(needle), "root cause {cause:?} does not mention {needle:?}");
}

// ── Tests ─────────────────────────────────────────────────────────────────────

fn happy_path_finalizes_once() {
    let run = Run::new("happy");
    let yaml = pipeline_yaml(
        "happy",
        "fake",
        run.src_config(json!({ "batches": 5 })),
        run.dst_config(json!({})),
        &timeouts(""),
        "",
    );
    let summary = run.exec(&yaml).expect("run");
    assert_eq!(summary.status, SummaryStatus::Success);
    assert_eq!(summary.rows_read, 50);
    assert_eq!(summary.rows_written, 50);
    assert!(summary.root_cause.is_none());
    let dst = run.journal("destination");
    assert_eq!(dst.iter().filter(|l| *l == "finalize").count(), 1, "{dst:?}");
    assert!(!has(&dst, "cancel"), "{dst:?}");
    assert!(!has(&run.journal("source"), "cancel"));
}

/// The bug: a source dying at a batch boundary looked like a clean end, the
/// destination finalized (committed) a partial load, and the run was marked
/// failed anyway.
fn source_crash_mid_stream_cancels_destination() {
    let run = Run::new("srccrash");
    let yaml = pipeline_yaml(
        "srccrash",
        "fake",
        run.src_config(json!({ "batches": 10, "crash_after": 3 })),
        run.dst_config(json!({})),
        &timeouts(""),
        "",
    );
    let summary = run.exec(&yaml).expect("data-phase failures are a summary");
    assert_eq!(summary.status, SummaryStatus::Error);
    assert_cause(&summary, "source");
    let dst = run.journal("destination");
    assert_rolled_back(&dst);
    assert!(has(&run.journal("source"), "crash"));
}

fn source_finished_error_cancels_destination() {
    let run = Run::new("srcerr");
    let yaml = pipeline_yaml(
        "srcerr",
        "fake",
        run.src_config(json!({ "batches": 10, "fail_after": 2 })),
        run.dst_config(json!({})),
        &timeouts(""),
        "",
    );
    let summary = run.exec(&yaml).expect("summary");
    assert_eq!(summary.status, SummaryStatus::Error);
    assert_cause(&summary, "BATCH_FAILED");
    assert_rolled_back(&run.journal("destination"));
    // The failing source runs its own cancel() (undo) too.
    assert!(has(&run.journal("source"), "cancel"));
}

fn destination_crash_cancels_source() {
    let run = Run::new("dstcrash");
    let yaml = pipeline_yaml(
        "dstcrash",
        "fake",
        run.src_config(json!({ "batches": 1000, "delay_ms": 20 })),
        run.dst_config(json!({ "crash_after": 2 })),
        &timeouts(""),
        "",
    );
    let started = Instant::now();
    let summary = run.exec(&yaml).expect("summary");
    assert_eq!(summary.status, SummaryStatus::Error);
    assert_cause(&summary, "destination exited with status 43");
    let src = run.journal("source");
    assert!(has(&src, "cancel"), "source was not cancelled: {src:?}");
    assert!(!has(&src, "done"), "source ran to completion: {src:?}");
    assert!(started.elapsed() < Duration::from_secs(10));
}

fn destination_error_cancels_source() {
    let run = Run::new("dsterr");
    let yaml = pipeline_yaml(
        "dsterr",
        "fake",
        run.src_config(json!({ "batches": 1000, "delay_ms": 20 })),
        run.dst_config(json!({ "fail_after": 2 })),
        &timeouts(""),
        "",
    );
    let summary = run.exec(&yaml).expect("summary");
    assert_eq!(summary.status, SummaryStatus::Error);
    assert_cause(&summary, "WRITE_FAILED");
    assert!(has(&run.journal("source"), "cancel"));
    // The destination undoes its own partial work after its failure.
    assert_rolled_back(&run.journal("destination"));
}

fn state_block(path: &Path) -> String {
    format!("state:\n  backend: local\n  path: {}\n", path.display())
}

fn lock_path(state: &Path) -> PathBuf {
    PathBuf::from(format!("{}.lock", state.display()))
}

fn signal_cancels_both_and_releases_the_lock() {
    let run = Run::new("sigterm");
    let state = run.scratch.path("state.json");
    let yaml = pipeline_yaml(
        "sigterm",
        "fake",
        run.src_config(json!({ "batches": 100000, "delay_ms": 20, "incremental": true })),
        run.dst_config(json!({})),
        &timeouts(""),
        &state_block(&state),
    );
    let (trigger, shutdown) = Shutdown::channel();
    let dst_journal = run.journal_path("destination");
    let watcher = std::thread::spawn(move || {
        // Fire once data is flowing.
        fake_plugins::wait_until(Duration::from_secs(20), || {
            journal(&dst_journal).iter().any(|l| l == "batch 3")
        });
        trigger.trigger(Halt::Signal(StopSignal::Terminate));
    });
    let summary = run.exec_with(&yaml, shutdown).expect("summary");
    watcher.join().unwrap();
    assert_eq!(summary.status, SummaryStatus::Cancelled);
    assert_eq!(summary.signal, Some(StopSignal::Terminate));
    assert_cause(&summary, "SIGTERM");
    assert_rolled_back(&run.journal("destination"));
    assert!(has(&run.journal("source"), "cancel"));
    assert!(!lock_path(&state).exists(), "the state lock was not released");
}

fn signal_before_start_is_an_interrupted_error() {
    let run = Run::new("sigearly");
    let yaml = pipeline_yaml(
        "sigearly",
        "fake",
        run.src_config(json!({})),
        run.dst_config(json!({})),
        &timeouts(""),
        "",
    );
    let (trigger, shutdown) = Shutdown::channel();
    trigger.trigger(Halt::Signal(StopSignal::Interrupt));
    match run.exec_with(&yaml, shutdown) {
        Err(CoreError::Interrupted(StopSignal::Interrupt)) => {}
        other => panic!("expected Interrupted, got {other:?}"),
    }
}

/// A source that hangs mid-stream *and* ignores `cancel`: the stall timeout
/// fails the run, and the grace period bounds how long the core waits.
fn stall_timeout_kills_a_deaf_source() {
    let run = Run::new("stall");
    let yaml = pipeline_yaml(
        "stall",
        "deaf",
        json!({}),
        run.dst_config(json!({})),
        &timeouts("    stall_seconds: 1\n"),
        "",
    );
    let started = Instant::now();
    let summary = run.exec(&yaml).expect("summary");
    let took = started.elapsed();
    assert_eq!(summary.status, SummaryStatus::Error);
    assert_cause(&summary, "stalled");
    assert_rolled_back(&run.journal("destination"));
    // 1s stall + 2s grace + slack; never "forever".
    assert!(took < Duration::from_secs(10), "took {took:?}");
}

fn first_batch_timeout() {
    let run = Run::new("firstbatch");
    let yaml = pipeline_yaml(
        "firstbatch",
        "fake",
        run.src_config(json!({ "hang_after": 0 })),
        run.dst_config(json!({})),
        &timeouts("    stall_seconds: 30\n    first_batch_seconds: 1\n"),
        "",
    );
    let summary = run.exec(&yaml).expect("summary");
    assert_eq!(summary.status, SummaryStatus::Error);
    assert_cause(&summary, "no first batch");
    // The SDK source sees `cancel` even while blocked in next_batch.
    assert!(has(&run.journal("source"), "cancel"), "{:?}", run.journal("source"));
    assert_rolled_back(&run.journal("destination"));
}

/// A destination stuck in `finalize` (a hung commit) is bounded by
/// `finalize_seconds`; the core then kills it rather than wait forever.
fn finalize_timeout() {
    let run = Run::new("finalize");
    let yaml = pipeline_yaml(
        "finalize",
        "fake",
        run.src_config(json!({})),
        run.dst_config(json!({ "hang_in_finalize": true })),
        &timeouts("    finalize_seconds: 1\n"),
        "",
    );
    let started = Instant::now();
    let summary = run.exec(&yaml).expect("summary");
    assert_eq!(summary.status, SummaryStatus::Error);
    assert_cause(&summary, "finalize_seconds");
    assert!(started.elapsed() < Duration::from_secs(10));
}

fn malformed_line_once_recovers_with_resend() {
    let run = Run::new("garbled1");
    let yaml = pipeline_yaml(
        "garbled1",
        "garbled1",
        json!({}),
        run.dst_config(json!({})),
        &timeouts("    handshake_seconds: 5\n"),
        "",
    );
    let summary = run.exec(&yaml).expect("run");
    assert_eq!(summary.status, SummaryStatus::Success);
    assert_eq!(summary.rows_written, 20);
}

fn malformed_line_twice_is_fatal() {
    let run = Run::new("garbled2");
    let yaml = pipeline_yaml(
        "garbled2",
        "garbled2",
        json!({}),
        run.dst_config(json!({})),
        &timeouts("    handshake_seconds: 5\n"),
        "",
    );
    match run.exec(&yaml) {
        Err(CoreError::Protocol(msg)) => assert!(msg.contains("malformed"), "{msg}"),
        other => panic!("expected a protocol error, got {other:?}"),
    }
    assert!(!has(&run.journal("destination"), "finalize"));
}

/// A peer that ignores `resend` cannot hang the core: the phase timeout fires.
fn ignored_resend_times_out() {
    let run = Run::new("noresend");
    let yaml = pipeline_yaml(
        "noresend",
        "noresend",
        json!({}),
        run.dst_config(json!({})),
        &timeouts("    handshake_seconds: 1\n"),
        "",
    );
    let started = Instant::now();
    match run.exec(&yaml) {
        Err(CoreError::Timeout(msg)) => assert!(msg.contains("handshake_ack"), "{msg}"),
        other => panic!("expected a timeout, got {other:?}"),
    }
    assert!(started.elapsed() < Duration::from_secs(10));
}

fn unknown_message_types_are_ignored() {
    let run = Run::new("chatty");
    let yaml = pipeline_yaml(
        "chatty",
        "chatty",
        json!({}),
        run.dst_config(json!({})),
        &timeouts(""),
        "",
    );
    let summary = run.exec(&yaml).expect("run");
    assert_eq!(summary.status, SummaryStatus::Success);
    assert_eq!(summary.rows_written, 20);
}

fn state_lock_released_after_a_crash() {
    let run = Run::new("statecrash");
    let state = run.scratch.path("state.json");
    let yaml = pipeline_yaml(
        "statecrash",
        "fake",
        run.src_config(json!({ "batches": 10, "crash_after": 2, "incremental": true })),
        run.dst_config(json!({})),
        &timeouts(""),
        &state_block(&state),
    );
    let summary = run.exec(&yaml).expect("summary");
    assert_eq!(summary.status, SummaryStatus::Error);
    assert!(!lock_path(&state).exists(), "the state lock was not released");
    // Nothing was durably committed, so no watermark may have been persisted.
    assert!(!state.exists(), "a watermark was persisted for an aborted load");
}

fn state_doc(path: &Path) -> serde_json::Value {
    serde_json::from_str(&std::fs::read_to_string(path).expect("state file")).unwrap()
}

/// The fingerprint comes from the source's Arrow stream: a changed column
/// type is refused on the next run, before the destination is started.
fn schema_drift_is_detected_from_the_arrow_stream() {
    let run = Run::new("drift");
    let state = run.scratch.path("state.json");
    let yaml = |wide: bool| {
        pipeline_yaml(
            "drift",
            "fake",
            run.src_config(json!({ "incremental": true, "wide": wide })),
            run.dst_config(json!({})),
            &timeouts(""),
            &state_block(&state),
        )
    };
    let summary = run.exec(&yaml(false)).expect("first run");
    assert_eq!(summary.status, SummaryStatus::Success);
    let doc = state_doc(&state);
    assert_eq!(doc["version"], loadsmith_core::state::STATE_FORMAT_VERSION);
    let recorded = doc["schema_hash"].clone();

    // Same shape: accepted.
    assert_eq!(run.exec(&yaml(false)).expect("same schema").status, SummaryStatus::Success);
    assert_eq!(state_doc(&state)["schema_hash"], recorded);

    // A new column: refused (exit 5 in the CLI), and nothing was loaded.
    let finalized_before = run.journal("destination").iter().filter(|l| *l == "finalize").count();
    match run.exec(&yaml(true)) {
        Err(CoreError::State(msg)) => assert!(msg.contains("schema changed"), "{msg}"),
        other => panic!("expected a drift refusal, got {other:?}"),
    }
    let finalized_after = run.journal("destination").iter().filter(|l| *l == "finalize").count();
    assert_eq!(finalized_before, finalized_after, "a drifted run must not load anything");
    assert!(!lock_path(&state).exists());
}

/// A state file written in format 1 (fingerprint of the lossy protocol
/// labels) is honoured, not refused, and upgraded to the current format.
fn v1_state_file_is_migrated() {
    let run = Run::new("migrate");
    let state = run.scratch.path("state.json");
    std::fs::write(
        &state,
        r#"{"version":1,"pipeline":"migrate","cursor_value":5,"schema_hash":"a8e4fe49824728ac","run_id":"old","updated_at_unix_ms":1}"#,
    )
    .unwrap();
    let yaml = pipeline_yaml(
        "migrate",
        "fake",
        run.src_config(json!({ "incremental": true, "wide": true })),
        run.dst_config(json!({})),
        &timeouts(""),
        &state_block(&state),
    );
    let summary = run.exec(&yaml).expect("an old fingerprint must not be refused");
    assert_eq!(summary.status, SummaryStatus::Success);
    let doc = state_doc(&state);
    assert_eq!(doc["version"], loadsmith_core::state::STATE_FORMAT_VERSION);
    assert_ne!(doc["schema_hash"], "a8e4fe49824728ac");
    assert_eq!(doc["run_id"].as_str().map(|r| r != "old"), Some(true));
}

/// A source that declares a schema but never writes its stream fails the
/// run during setup; the destination is never started.
fn source_without_a_stream_header_fails_setup() {
    let run = Run::new("noheader");
    let yaml = pipeline_yaml(
        "noheader",
        "noheader",
        json!({}),
        run.dst_config(json!({})),
        &timeouts(""),
        "",
    );
    match run.exec(&yaml) {
        Err(CoreError::PluginFailed(msg)) => assert!(msg.contains("source"), "{msg}"),
        other => panic!("expected a source failure, got {other:?}"),
    }
    assert!(!has(&run.journal("destination"), "prepare"), "{:?}", run.journal("destination"));
}
