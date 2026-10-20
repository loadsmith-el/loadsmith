//! Fake plugins for the end-to-end test harnesses.
//!
//! The test executable doubles as every plugin: [`install`] symlinks
//! `loadsmith-<kind>-<type>` names to the running test binary in a temporary
//! plugin directory, and [`run_as_plugin_if_invoked`] (called first thing in
//! the harness's `main`) recognises such an `argv[0]` and becomes that plugin.
//!
//! - `source-fake` / `destination-fake` are real SDK plugins whose behaviour
//!   (crash, fail, hang, slow down) comes from their pipeline `config`. They
//!   append what happened to them (`prepare`, `batch N`, `finalize`,
//!   `cancel`, …) to the `journal` file named in their config.
//! - `provider-fake` is an SDK provider: it resolves references from its
//!   `values` config and can fail, hang, crash or leak a value into its logs
//!   (see [`FakeProvider`]).
//! - `source-files` is an SDK raw source (`object_output`) announcing the
//!   files listed in its config, and `parser-fake` an SDK parser of a tiny
//!   text format (see [`FakeFiles`], [`FakeParser`]).
//! - `source-<scenario>` for the scenarios in [`RAW_SOURCES`] are hand-rolled
//!   protocol speakers that misbehave on the wire in ways the SDK never would
//!   (malformed lines, unknown message types, ignoring `cancel`/`resend`).
#![allow(dead_code)]

use std::io::{BufRead, Write};
use std::os::unix::io::FromRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arrow_array::{Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use loadsmith_plugin_sdk::{
    DestinationPlugin, ObjectSourcePlugin, ParserPlugin, PluginConfig, ProviderPlugin, ResolveFailure, SourcePlugin,
};
use loadsmith_protocol::{
    CapabilitiesResponse, ConfigureAck, Finished, HandshakeAck, Message, PluginKind,
};

/// Raw (non-SDK) source scenarios.
pub const RAW_SOURCES: &[&str] = &["garbled1", "garbled2", "noresend", "chatty", "deaf", "noheader", "eager"];

/// Creates a plugin directory under `dir` with every fake plugin linked to the
/// current executable, and returns it.
pub fn install(dir: &Path) -> PathBuf {
    let plugins = dir.join("plugins");
    std::fs::create_dir_all(&plugins).unwrap();
    let exe = std::env::current_exe().unwrap();
    let mut names = vec![
        "loadsmith-source-fake".to_string(),
        "loadsmith-destination-fake".to_string(),
        "loadsmith-destination-typed".to_string(),
        "loadsmith-provider-fake".to_string(),
        "loadsmith-source-files".to_string(),
        "loadsmith-parser-fake".to_string(),
        "loadsmith-parser-eager".to_string(),
        "loadsmith-parser-noparse".to_string(),
    ];
    names.extend(RAW_SOURCES.iter().map(|s| format!("loadsmith-source-{s}")));
    for name in names {
        let link = plugins.join(name);
        let _ = std::fs::remove_file(&link);
        std::os::unix::fs::symlink(&exe, &link).unwrap();
    }
    plugins
}

/// If this process was started as a fake plugin, runs it and never returns.
pub fn run_as_plugin_if_invoked() {
    let argv0 = std::env::args().next().unwrap_or_default();
    let name = Path::new(&argv0).file_name().and_then(|n| n.to_str()).unwrap_or("").to_string();
    let Some(rest) = name.strip_prefix("loadsmith-") else { return };
    let rt = || tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    match rest {
        "source-fake" => rt().block_on(loadsmith_plugin_sdk::run_source(FakeSource::default())),
        "destination-fake" => {
            rt().block_on(loadsmith_plugin_sdk::run_destination(FakeDestination::default()))
        }
        "destination-typed" => rt().block_on(loadsmith_plugin_sdk::run_destination(FakeDestination {
            typed: true,
            ..Default::default()
        })),
        "provider-fake" => rt().block_on(loadsmith_plugin_sdk::run_provider(FakeProvider::default())),
        "source-files" => rt().block_on(loadsmith_plugin_sdk::run_object_source(FakeFiles::default())),
        "parser-fake" => rt().block_on(loadsmith_plugin_sdk::run_parser(FakeParser::default())),
        "parser-eager" => eager_parser(),
        "parser-noparse" => {
            rt().block_on(loadsmith_plugin_sdk::run_parser(FakeParser { no_parse: true, ..Default::default() }))
        }
        other => match other.strip_prefix("source-") {
            Some(scenario) if RAW_SOURCES.contains(&scenario) => raw_source(scenario),
            _ => {}
        },
    }
}

/// The lines a fake plugin wrote to its journal (empty if none).
pub fn journal(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .map(|s| s.lines().map(str::to_string).collect())
        .unwrap_or_default()
}

fn note(journal: &Option<PathBuf>, line: &str) {
    if let Some(path) = journal {
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(path).unwrap();
        writeln!(f, "{line}").unwrap();
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn batch(seq: u64, rows: u64) -> RecordBatch {
    let ids: Vec<i64> = (0..rows).map(|i| (seq * 1000 + i) as i64).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(ids))]).unwrap()
}

/// The `wide` variant: `id` plus a microsecond UTC timestamp column.
fn wide_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("seen_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
    ]))
}

fn wide_batch(seq: u64, rows: u64) -> RecordBatch {
    let ids: Vec<i64> = (0..rows).map(|i| (seq * 1000 + i) as i64).collect();
    let ts = TimestampMicrosecondArray::from(ids.clone()).with_timezone("UTC");
    RecordBatch::try_new(wide_schema(), vec![Arc::new(Int64Array::from(ids)), Arc::new(ts)]).unwrap()
}

fn opt_u64(config: &serde_json::Value, key: &str) -> Option<u64> {
    config.get(key).and_then(|v| v.as_u64())
}

// ── SDK source ────────────────────────────────────────────────────────────────

/// Config: `batches` (default 3), `rows` per batch (default 10), `delay_ms`
/// before each batch, and at most one fault, applied when batch N+1 is asked
/// for: `crash_after` (exit 42), `fail_after` (`next_batch` errors),
/// `hang_after` (never returns). `incremental: true` advertises
/// `incremental_state` and reports a watermark. `wide: true` adds a
/// `seen_at` timestamp column (a schema change).
#[derive(Default)]
pub struct FakeSource {
    config: serde_json::Value,
    journal: Option<PathBuf>,
    produced: u64,
}

impl FakeSource {
    fn wide(&self) -> bool {
        self.config.get("wide").and_then(|v| v.as_bool()) == Some(true)
    }

    /// `leak: true`: log the whole config (a `password` included) on fd4,
    /// and put the password in a `fail_after` error — a careless plugin.
    fn leaks(&self) -> bool {
        self.config.get("leak").and_then(|v| v.as_bool()) == Some(true)
    }
}

/// Writes one `log` event straight to fd4, as a careless plugin would.
fn leak_log(message: &str) {
    let line = serde_json::to_string(&Message::Log(loadsmith_protocol::LogEvent {
        level: loadsmith_protocol::LogLevel::Error,
        message: message.to_string(),
    }))
    .unwrap();
    // The SDK owns fd4; borrow it without closing it.
    let mut fd4 = std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(4) });
    let _ = writeln!(fd4, "{line}");
}

// ── SDK provider ──────────────────────────────────────────────────────────────

/// Config: `journal`; `values` (reference → value); `errors` (references
/// answered with a `NOT_FOUND` resolved error; so is any reference missing
/// from `values`); `hang: true` (never answers a `resolve`), `crash: true`
/// (exits 44 on the first `resolve`), `reject: true` (rejects its config);
/// `leak: true` (logs every value it resolves on fd4).
#[derive(Default)]
pub struct FakeProvider {
    config: serde_json::Value,
    journal: Option<PathBuf>,
}

#[async_trait]
impl ProviderPlugin for FakeProvider {
    fn plugin_name(&self) -> &str {
        "fake-provider"
    }
    fn plugin_version(&self) -> &str {
        "0.0.0"
    }
    async fn configure(&mut self, config: PluginConfig) -> Result<()> {
        self.journal = config.get("journal").and_then(|v| v.as_str()).map(PathBuf::from);
        note(&self.journal, "configure");
        anyhow::ensure!(config.get("reject").is_none(), "fake provider rejects this config");
        self.config = config.into_value();
        Ok(())
    }
    async fn resolve(&mut self, reference: &str) -> Result<String> {
        note(&self.journal, &format!("resolve {reference}"));
        let flag = |k: &str| self.config.get(k).and_then(|v| v.as_bool()) == Some(true);
        if flag("hang") {
            std::future::pending::<()>().await;
        }
        if flag("crash") {
            note(&self.journal, "crash");
            std::process::exit(44);
        }
        let listed_error = self.config["errors"].as_array().is_some_and(|e| e.iter().any(|r| r == reference));
        match self.config["values"].get(reference).and_then(|v| v.as_str()) {
            Some(value) if !listed_error => {
                if flag("leak") {
                    leak_log(&format!("resolved {reference} to {value}"));
                }
                Ok(value.to_string())
            }
            _ => Err(ResolveFailure::new("NOT_FOUND", format!("fake provider has no '{reference}'")).into()),
        }
    }
    async fn cancel(&mut self) {
        note(&self.journal, "cancel");
    }
}

#[async_trait]
impl SourcePlugin for FakeSource {
    fn plugin_name(&self) -> &str {
        "fake-source"
    }
    fn plugin_version(&self) -> &str {
        "0.0.0"
    }
    fn capabilities(&self) -> Vec<String> {
        vec!["batch_read".into(), "incremental_state".into()]
    }
    async fn configure(&mut self, config: PluginConfig) -> Result<()> {
        self.journal = config.get("journal").and_then(|v| v.as_str()).map(PathBuf::from);
        self.config = config.into_value();
        note(&self.journal, "configure");
        if let Some(pw) = self.config.get("password").and_then(|v| v.as_str()) {
            // The journal is a file, not output: it proves what arrived.
            note(&self.journal, &format!("password={pw}"));
        }
        if self.leaks() {
            leak_log(&format!("configured with {}", self.config));
        }
        Ok(())
    }
    async fn schema(&mut self) -> Result<Schema> {
        note(&self.journal, "start");
        Ok(if self.wide() { wide_schema() } else { schema() }.as_ref().clone())
    }
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let n = self.produced;
        if let Some(ms) = opt_u64(&self.config, "delay_ms") {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
        if opt_u64(&self.config, "crash_after") == Some(n) {
            note(&self.journal, "crash");
            std::process::exit(42);
        }
        if opt_u64(&self.config, "fail_after") == Some(n) {
            if self.leaks() {
                anyhow::bail!("fake source failure after {n} batches (password {})", self.config["password"]);
            }
            anyhow::bail!("fake source failure after {n} batches");
        }
        if opt_u64(&self.config, "hang_after") == Some(n) {
            note(&self.journal, "hang");
            std::future::pending::<()>().await;
        }
        if n >= opt_u64(&self.config, "batches").unwrap_or(3) {
            note(&self.journal, "done");
            return Ok(None);
        }
        self.produced += 1;
        note(&self.journal, &format!("batch {}", self.produced));
        let rows = opt_u64(&self.config, "rows").unwrap_or(10);
        Ok(Some(if self.wide() { wide_batch(self.produced, rows) } else { batch(self.produced, rows) }))
    }
    fn current_watermark(&self) -> Option<serde_json::Value> {
        (self.config.get("incremental").and_then(|v| v.as_bool()) == Some(true))
            .then(|| serde_json::json!(self.produced))
    }
    async fn cancel(&mut self) {
        note(&self.journal, "cancel");
    }
}

// ── SDK destination ───────────────────────────────────────────────────────────

/// Config: `journal`, and at most one fault, applied on batch N+1:
/// `crash_after` (exit 43) or `fail_after` (`write_batch` errors); or
/// `hang_in_finalize: true`.
///
/// `destination-typed` parses its config with the SDK helper
/// ([`PluginConfig::parse`]) into [`TypedConfig`], so an unknown key fails
/// `configure` (or is only warned about under the `warn` policy). It
/// journals the policy it received (`unknown_keys=<policy>`).
#[derive(Default)]
pub struct FakeDestination {
    config: serde_json::Value,
    journal: Option<PathBuf>,
    rows: u64,
    batches: u64,
    typed: bool,
}

/// The `destination-typed` config: a flattened struct, the case serde's own
/// `deny_unknown_fields` cannot check.
#[derive(serde::Deserialize)]
#[allow(dead_code)]
pub struct TypedConfig {
    #[serde(default)]
    journal: Option<PathBuf>,
    #[serde(flatten)]
    faults: TypedFaults,
}

#[derive(serde::Deserialize)]
#[allow(dead_code)]
pub struct TypedFaults {
    #[serde(default)]
    crash_after: Option<u64>,
    #[serde(default)]
    fail_after: Option<u64>,
}

#[async_trait]
impl DestinationPlugin for FakeDestination {
    fn plugin_name(&self) -> &str {
        "fake-destination"
    }
    fn plugin_version(&self) -> &str {
        "0.0.0"
    }
    async fn configure(&mut self, config: PluginConfig) -> Result<()> {
        self.journal = config.get("journal").and_then(|v| v.as_str()).map(PathBuf::from);
        if self.typed {
            note(&self.journal, &format!("unknown_keys={}", config.unknown_keys().as_str()));
            let _: TypedConfig = config.parse()?;
        }
        self.config = config.into_value();
        note(&self.journal, "configure");
        Ok(())
    }
    async fn prepare(&mut self) -> Result<()> {
        note(&self.journal, "prepare");
        Ok(())
    }
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if opt_u64(&self.config, "crash_after") == Some(self.batches) {
            note(&self.journal, "crash");
            std::process::exit(43);
        }
        if opt_u64(&self.config, "fail_after") == Some(self.batches) {
            anyhow::bail!("fake destination failure after {} batches", self.batches);
        }
        self.batches += 1;
        self.rows += batch.num_rows() as u64;
        note(&self.journal, &format!("batch {}", self.batches));
        Ok(())
    }
    async fn finalize(&mut self) -> Result<u64> {
        if self.config.get("hang_in_finalize").and_then(|v| v.as_bool()) == Some(true) {
            note(&self.journal, "hang");
            std::future::pending::<()>().await;
        }
        note(&self.journal, "finalize");
        Ok(self.rows)
    }
    async fn cancel(&mut self) {
        note(&self.journal, "cancel");
    }
}

// ── SDK raw source + parser ───────────────────────────────────────────────────

/// A raw source. Config: `journal`; `files` (paths, announced in order);
/// `delay_ms` before each; at most one fault, applied when file N+1 is asked
/// for: `crash_after` (exit 46), `fail_after` (`next_object` errors),
/// `hang_after` (never returns); `download: true` advertises `scratch_dir`
/// and announces a copy of each file made in the injected `scratch_dir`.
#[derive(Default)]
pub struct FakeFiles {
    config: serde_json::Value,
    journal: Option<PathBuf>,
    given: u64,
}

impl FakeFiles {
    fn downloads(&self) -> bool {
        self.config.get("download").and_then(|v| v.as_bool()) == Some(true)
    }
}

#[async_trait]
impl ObjectSourcePlugin for FakeFiles {
    fn plugin_name(&self) -> &str {
        "fake-files"
    }
    fn plugin_version(&self) -> &str {
        "0.0.0"
    }
    fn capabilities(&self) -> Vec<String> {
        // Capabilities come before the config, so the fake always asks for
        // a scratch dir; it writes to it only with `download: true`.
        vec!["object_output".into(), "scratch_dir".into()]
    }
    async fn configure(&mut self, config: PluginConfig) -> Result<()> {
        self.journal = config.get("journal").and_then(|v| v.as_str()).map(PathBuf::from);
        self.config = config.into_value();
        note(&self.journal, "configure");
        if let Some(dir) = self.config.get("scratch_dir").and_then(|v| v.as_str()) {
            note(&self.journal, &format!("scratch {dir}"));
        }
        Ok(())
    }
    async fn next_object(&mut self) -> Result<Option<PathBuf>> {
        let n = self.given;
        if let Some(ms) = opt_u64(&self.config, "delay_ms") {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
        if opt_u64(&self.config, "crash_after") == Some(n) {
            note(&self.journal, "crash");
            std::process::exit(46);
        }
        if opt_u64(&self.config, "fail_after") == Some(n) {
            anyhow::bail!("fake raw source failure after {n} files");
        }
        if opt_u64(&self.config, "hang_after") == Some(n) {
            note(&self.journal, "hang");
            std::future::pending::<()>().await;
        }
        let files = self.config["files"].as_array().cloned().unwrap_or_default();
        let Some(file) = files.get(n as usize).and_then(|f| f.as_str()) else {
            note(&self.journal, "done");
            return Ok(None);
        };
        self.given += 1;
        let mut path = PathBuf::from(file);
        if self.downloads() {
            let dir = PathBuf::from(self.config["scratch_dir"].as_str().expect("an injected scratch_dir"));
            let copy = dir.join(path.file_name().unwrap());
            std::fs::copy(&path, &copy)?;
            path = copy;
        }
        note(&self.journal, &format!("file {}", path.file_name().unwrap().to_string_lossy()));
        Ok(Some(path))
    }
    async fn cancel(&mut self) {
        note(&self.journal, "cancel");
    }
}

/// Writes a file in the [`FakeParser`] format: a header line of column
/// names, then one line per row of comma-separated integers.
pub fn write_fake_file(path: &Path, columns: &[&str], rows: u64) {
    let mut text = columns.join(",");
    text.push('\n');
    for r in 0..rows {
        let row: Vec<String> = columns.iter().enumerate().map(|(i, _)| (r * 10 + i as u64).to_string()).collect();
        text.push_str(&row.join(","));
        text.push('\n');
    }
    std::fs::write(path, text).unwrap();
}

/// A parser of [`write_fake_file`] files (every column `int64`, not null).
/// Config: `journal`; `columns` (declares the schema); `batch_rows`
/// (default 2); `delay_ms` before each batch; `crash_on` / `fail_on` (a
/// file name: exit 45 / an error when opening it).
#[derive(Default)]
pub struct FakeParser {
    config: serde_json::Value,
    journal: Option<PathBuf>,
    header: Vec<String>,
    rows: std::collections::VecDeque<Vec<i64>>,
    /// `parser-noparse`: advertises no `parse` capability.
    no_parse: bool,
}

fn int_schema(names: &[String]) -> Schema {
    Schema::new(names.iter().map(|n| Field::new(n, DataType::Int64, false)).collect::<Vec<_>>())
}

#[async_trait]
impl ParserPlugin for FakeParser {
    fn plugin_name(&self) -> &str {
        "fake-parser"
    }
    fn plugin_version(&self) -> &str {
        "0.0.0"
    }
    fn capabilities(&self) -> Vec<String> {
        if self.no_parse { vec![] } else { vec!["parse".into()] }
    }
    async fn configure(&mut self, config: PluginConfig) -> Result<()> {
        self.journal = config.get("journal").and_then(|v| v.as_str()).map(PathBuf::from);
        self.config = config.into_value();
        note(&self.journal, "configure");
        Ok(())
    }
    fn declared_schema(&self) -> Option<Schema> {
        let cols = self.config.get("columns")?.as_array()?;
        Some(int_schema(&cols.iter().filter_map(|c| c.as_str().map(str::to_string)).collect::<Vec<_>>()))
    }
    async fn open(&mut self, path: &Path) -> Result<Schema> {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        note(&self.journal, &format!("open {name}"));
        if self.config.get("crash_on").and_then(|v| v.as_str()) == Some(name.as_str()) {
            note(&self.journal, "crash");
            std::process::exit(45);
        }
        if self.config.get("fail_on").and_then(|v| v.as_str()) == Some(name.as_str()) {
            anyhow::bail!("fake parser cannot read {name}");
        }
        let text = std::fs::read_to_string(path)?;
        let mut lines = text.lines();
        let header: Vec<String> = lines.next().unwrap_or("").split(',').map(str::to_string).collect();
        self.header = header.clone();
        self.rows = lines.map(|l| l.split(',').map(|v| v.parse().unwrap()).collect()).collect();
        Ok(int_schema(&header))
    }
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if let Some(ms) = opt_u64(&self.config, "delay_ms") {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
        if self.rows.is_empty() {
            return Ok(None);
        }
        let take = (opt_u64(&self.config, "batch_rows").unwrap_or(2) as usize).min(self.rows.len());
        let rows: Vec<Vec<i64>> = self.rows.drain(..take).collect();
        let columns: Vec<arrow_array::ArrayRef> = (0..self.header.len())
            .map(|c| Arc::new(Int64Array::from(rows.iter().map(|r| r[c]).collect::<Vec<_>>())) as _)
            .collect();
        Ok(Some(RecordBatch::try_new(Arc::new(int_schema(&self.header)), columns)?))
    }
    async fn cancel(&mut self) {
        note(&self.journal, "cancel");
    }
}

// ── Raw (hand-rolled protocol) sources ────────────────────────────────────────

struct Raw {
    lines: std::io::Lines<std::io::StdinLock<'static>>,
    last: Option<String>,
}

impl Raw {
    fn send(&mut self, msg: &Message) {
        let line = serde_json::to_string(msg).unwrap();
        self.send_line(&line);
        self.last = Some(line);
    }

    fn send_line(&mut self, line: &str) {
        let mut out = std::io::stdout().lock();
        writeln!(out, "{line}").unwrap();
        out.flush().unwrap();
    }

    /// Next message from the core; `resend` is answered when `honor_resend`.
    /// Exits on EOF, `cancel` or `error` (after replying `finished` to cancel).
    fn recv(&mut self, honor_resend: bool) -> Message {
        loop {
            let Some(Ok(line)) = self.lines.next() else { std::process::exit(1) };
            let msg: Message = serde_json::from_str(&line).unwrap_or(Message::Unknown);
            match msg {
                Message::Resend(_) if honor_resend => {
                    if let Some(last) = self.last.clone() {
                        self.send_line(&last);
                    }
                }
                Message::Resend(_) => {}
                Message::Cancel(_) => {
                    self.send(&Message::Finished(Finished::cancelled()));
                    std::process::exit(0);
                }
                Message::Error(_) => std::process::exit(1),
                other => return other,
            }
        }
    }
}

/// A source that speaks the protocol by hand:
/// - `garbled1`: its first `handshake_ack` is a malformed line (recovers on
///   `resend`); `garbled2`: two malformed lines in a row;
/// - `noresend`: one malformed line, then ignores `resend` (so the core's
///   handshake timeout must fire);
/// - `chatty`: an unknown message type before every reply and mid-stream;
/// - `deaf`: one batch, then hangs forever and ignores `cancel`;
/// - `noheader`: sends `schema`, then exits without writing its stream.
fn raw_source(scenario: &str) -> ! {
    let mut raw = Raw { lines: std::io::stdin().lock().lines(), last: None };
    let chatty = scenario == "chatty";
    let unknown = r#"{"type":"from_the_future","payload":{"x":1}}"#;

    assert!(matches!(raw.recv(true), Message::Handshake));
    let ack = Message::HandshakeAck(HandshakeAck {
        protocol_supported_versions: vec![1],
        plugin_name: format!("raw-{scenario}"),
        plugin_version: "0.0.0".into(),
        kind: PluginKind::Source,
    });
    match scenario {
        "garbled1" => {
            // Remember the real ack, but put a malformed line on the wire.
            raw.last = Some(serde_json::to_string(&ack).unwrap());
            raw.send_line("{this is not json");
        }
        "garbled2" => {
            raw.send_line("{this is not json");
            assert!(matches!(raw.lines.next(), Some(Ok(l)) if l.contains("resend")));
            raw.send_line("still not json");
            // The core gives up and sends `error`.
            raw.recv(false);
            std::process::exit(1);
        }
        "noresend" => {
            raw.send_line("{this is not json");
            // Ignore the resend; never answer again.
            loop {
                raw.recv(false);
            }
        }
        _ => {
            if chatty {
                raw.send_line(unknown);
            }
            raw.send(&ack);
        }
    }

    assert!(matches!(raw.recv(true), Message::SetProtocolVersion(_)));
    assert!(matches!(raw.recv(true), Message::CapabilitiesRequest));
    if chatty {
        raw.send_line(unknown);
    }
    raw.send(&Message::CapabilitiesResponse(CapabilitiesResponse { supports: vec!["batch_read".into()] }));
    assert!(matches!(raw.recv(true), Message::Configure(_)));
    if chatty {
        raw.send_line(unknown);
    }
    raw.send(&Message::ConfigureAck(ConfigureAck::ok()));
    assert!(matches!(raw.recv(true), Message::Start(_)));
    raw.send(&Message::Schema(loadsmith_protocol::Schema {
        fields: vec![loadsmith_protocol::Field::new("id", loadsmith_protocol::FieldType::Int64)
            .with_nullable(false)],
    }));

    if scenario == "noheader" {
        std::process::exit(3);
    }
    if scenario == "eager" {
        // `finished` first, the stream after: the core reads `finished`
        // while it still waits for the stream header.
        raw.send(&Message::Finished(Finished::success_source(20, 2)));
        std::thread::sleep(Duration::from_millis(300));
        let fd3 = unsafe { std::fs::File::from_raw_fd(3) };
        let mut writer = loadsmith_arrow::IpcWriter::new(fd3, schema().as_ref()).unwrap();
        writer.write_batch(&batch(1, 10)).unwrap();
        writer.write_batch(&batch(2, 10)).unwrap();
        writer.finish().unwrap();
        std::process::exit(0);
    }
    let fd3 = unsafe { std::fs::File::from_raw_fd(3) };
    let mut writer = loadsmith_arrow::IpcWriter::new(fd3, schema().as_ref()).unwrap();
    writer.write_batch(&batch(1, 10)).unwrap();
    if scenario == "deaf" {
        // Hang with the stream open, never reading stdin again.
        loop {
            std::thread::sleep(Duration::from_secs(3600));
        }
    }
    if chatty {
        raw.send_line(unknown);
    }
    writer.write_batch(&batch(2, 10)).unwrap();
    writer.finish().unwrap();
    raw.send(&Message::Finished(Finished::success_source(20, 2)));
    std::process::exit(0);
}

/// A parser that, on its first `parse_object`, sends `schema` and at once
/// `finished` success, then writes a one-batch stream and exits — without
/// waiting for "no more files" (stdin EOF). The core must not commit that.
fn eager_parser() -> ! {
    let mut raw = Raw { lines: std::io::stdin().lock().lines(), last: None };
    assert!(matches!(raw.recv(true), Message::Handshake));
    raw.send(&Message::HandshakeAck(HandshakeAck {
        protocol_supported_versions: vec![1],
        plugin_name: "eager-parser".into(),
        plugin_version: "0.0.0".into(),
        kind: PluginKind::Parser,
    }));
    assert!(matches!(raw.recv(true), Message::SetProtocolVersion(_)));
    assert!(matches!(raw.recv(true), Message::CapabilitiesRequest));
    raw.send(&Message::CapabilitiesResponse(CapabilitiesResponse { supports: vec!["parse".into()] }));
    assert!(matches!(raw.recv(true), Message::Configure(_)));
    raw.send(&Message::ConfigureAck(ConfigureAck::ok()));
    assert!(matches!(raw.recv(true), Message::Start(_)));
    assert!(matches!(raw.recv(true), Message::ParseObject(_)));
    raw.send(&Message::Schema(loadsmith_protocol::Schema {
        fields: vec![loadsmith_protocol::Field::new("id", loadsmith_protocol::FieldType::Int64)
            .with_nullable(false)],
    }));
    raw.send(&Message::Finished(Finished::success_source(10, 1)));
    std::thread::sleep(Duration::from_millis(300));
    let fd3 = unsafe { std::fs::File::from_raw_fd(3) };
    let mut writer = loadsmith_arrow::IpcWriter::new(fd3, schema().as_ref()).unwrap();
    writer.write_batch(&batch(1, 10)).unwrap();
    writer.finish().unwrap();
    std::process::exit(0);
}

// ── Test plumbing ─────────────────────────────────────────────────────────────

/// A scratch directory removed on drop.
pub struct Scratch(pub PathBuf);

impl Scratch {
    pub fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "loadsmith-e2e-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Scratch(dir)
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A pipeline YAML for `source_type` → the fake destination, with plugin
/// configs given as inline JSON (valid YAML flow mappings).
pub fn pipeline_yaml(
    name: &str,
    source_type: &str,
    source_config: serde_json::Value,
    destination_config: serde_json::Value,
    extra_pipeline: &str,
    extra_top: &str,
) -> String {
    format!(
        "pipeline:\n  name: {name}\n  heartbeat_seconds: 0\n{extra_pipeline}\
         source:\n  type: {source_type}\n  config: {source_config}\n\
         destination:\n  type: fake\n  config: {destination_config}\n{extra_top}"
    )
}

/// A `timeouts:` block (under `pipeline:`) with short limits for tests.
pub fn timeouts(fields: &str) -> String {
    format!("  timeouts:\n    cancel_grace_seconds: 2\n{fields}")
}

/// Polls `cond` every 20ms for up to `limit`.
pub fn wait_until(limit: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < limit {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    cond()
}
