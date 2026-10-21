//! End-to-end telemetry tests: real plugin processes (the fakes of
//! `support/fake_plugins.rs`, built on the current SDK) send spans, metrics
//! and logs on fd4; the core re-emits them, with its own, into in-memory
//! exporters this test reads back.
//!
//! Telemetry is installed once per process, so the order matters: the
//! "off" test runs first, before anything is installed.
//!
//! `harness = false`, as for `abort.rs`: `main` first checks whether it was
//! started as a plugin.

#[path = "support/fake_plugins.rs"]
mod fake_plugins;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use fake_plugins::{install, journal, pipeline_yaml, Scratch};
use loadsmith_config::{MaskList, OtlpProtocol, TelemetrySignal};
use loadsmith_core::telemetry::{self, RunIdentity, TelemetrySettings};
use loadsmith_core::{load_pipeline, run_pipeline, LoadOpts, Mode, RunOpts};
use loadsmith_protocol::LogLevel;
use opentelemetry::trace::SpanId;
use opentelemetry_sdk::logs::InMemoryLogExporter;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::InMemoryMetricExporter;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SpanData};
use serde_json::json;
use tracing::Instrument;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const SECRET: &str = "s3cr3t-telemetry-pw";

fn main() {
    fake_plugins::run_as_plugin_if_invoked();
    tracing_subscriber::registry().with(telemetry::layers()).init();

    let tests: &[(&str, fn())] = &[
        ("off_by_default_plugins_record_nothing", off_by_default_plugins_record_nothing),
        ("a_run_is_one_trace_with_plugin_spans_metrics_and_logs", a_run_is_one_trace_with_plugin_spans_metrics_and_logs),
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
        println!("test {name} ... {} ({:.1}s)", if ok { "ok" } else { "FAILED" }, started.elapsed().as_secs_f64());
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
        let scratch = Scratch::new(&format!("telemetry-{tag}"));
        let plugins = install(&scratch.0);
        Env { scratch, plugins }
    }

    /// Runs the fake source (probing telemetry) → the fake destination, in
    /// `span`, with the source's password from an `env()` template (so it
    /// is in the mask).
    fn run(&self, mask: &MaskList, span: tracing::Span) -> loadsmith_core::Summary {
        std::env::set_var("TELEMETRY_TEST_PW", SECRET);
        let yaml = pipeline_yaml(
            "tel",
            "fake",
            json!({
                "journal": self.scratch.path("source.journal"),
                "telemetry_probe": true,
                "leak": true,
                "password": "{{ env('TELEMETRY_TEST_PW') }}",
            }),
            json!({ "journal": self.scratch.path("destination.journal") }),
            &fake_plugins::timeouts(""),
            "",
        );
        let path = self.scratch.path("pipeline.yaml");
        std::fs::write(&path, yaml).unwrap();
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        rt.block_on(
            async {
                let load = LoadOpts { mask: mask.clone(), ..LoadOpts::new(&self.plugins) };
                let (config, _) = load_pipeline(&path, Mode::Full, &load).await.unwrap();
                let mut opts = RunOpts::new(&self.plugins);
                opts.color = false;
                opts.human_report = false;
                opts.mask = mask.clone();
                run_pipeline(config, opts).await.unwrap()
            }
            .instrument(span),
        )
    }

    fn source_journal(&self) -> Vec<String> {
        journal(&self.scratch.path("source.journal"))
    }
}

fn settings() -> TelemetrySettings {
    TelemetrySettings {
        protocol: OtlpProtocol::HttpProtobuf,
        endpoint: "http://unused".into(),
        endpoints: [TelemetrySignal::Traces, TelemetrySignal::Metrics, TelemetrySignal::Logs]
            .into_iter()
            .map(|s| (s, "http://unused".to_string()))
            .collect(),
        headers: BTreeMap::new(),
        timeout: Duration::from_secs(5),
        sampling_ratio: 1.0,
        metrics_interval: Duration::from_secs(3600),
        log_level: LogLevel::Info,
        resource: BTreeMap::new(),
        certificate: None,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

fn off_by_default_plugins_record_nothing() {
    assert!(!telemetry::enabled());
    let env = Env::new("off");
    let summary = env.run(&MaskList::new(), tracing::Span::none());
    assert_eq!(summary.rows_written, 30);
    // The plugin was never told to record: its SDK stayed off.
    assert!(env.source_journal().contains(&"telemetry=false".to_string()), "{:?}", env.source_journal());
}

fn a_run_is_one_trace_with_plugin_spans_metrics_and_logs() {
    let spans = InMemorySpanExporter::default();
    let metrics = InMemoryMetricExporter::default();
    let logs = InMemoryLogExporter::default();
    let mask = MaskList::new();
    let identity = RunIdentity { pipeline: "tel".into(), run_id: "run-1".into(), version: "test".into() };
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let installed = rt.block_on(async {
        telemetry::install_with_exporters(settings(), &identity, &mask, spans.clone(), metrics.clone(), logs.clone())
    });
    assert!(installed && telemetry::enabled());

    let env = Env::new("on");
    let run_span = telemetry::run_span(&identity);
    let summary = env.run(&mask, run_span.clone());
    assert_eq!(summary.rows_written, 30);
    assert!(env.source_journal().contains(&"telemetry=true".to_string()));
    let trace_id = telemetry::trace_id(&run_span).expect("the run is traced");
    drop(run_span);
    telemetry::force_flush();

    // ── Spans: one trace, plugin spans under the core's plugin spans ────
    let finished = spans.get_finished_spans().unwrap();
    let by_name = |name: &str| -> Vec<&SpanData> { finished.iter().filter(|s| s.name == name).collect() };
    let names: Vec<&str> = finished.iter().map(|s| s.name.as_ref()).collect();
    for s in &finished {
        assert_eq!(s.span_context.trace_id().to_string(), trace_id, "{} is in another trace", s.name);
    }
    let run = by_name("loadsmith.run");
    assert_eq!(run.len(), 1, "{names:?}");
    let run = run[0];
    let plugin_source = by_name("plugin fake-source");
    assert_eq!(plugin_source.len(), 1, "{names:?}");
    let plugin_source = plugin_source[0];
    assert_eq!(plugin_source.parent_span_id, run.span_context.span_id());
    let attr = |s: &SpanData, key: &str| {
        s.attributes.iter().find(|kv| kv.key.as_str() == key).map(|kv| kv.value.to_string())
    };
    assert_eq!(attr(plugin_source, "loadsmith.plugin.kind").as_deref(), Some("source"));
    assert_eq!(attr(plugin_source, "loadsmith.plugin.version").as_deref(), Some("0.0.0"));
    // The core's lifecycle spans and the plugin's own `configure` span sit
    // under the plugin process's span.
    let children_of = |parent: SpanId| -> Vec<&str> {
        finished.iter().filter(|s| s.parent_span_id == parent).map(|s| s.name.as_ref()).collect()
    };
    let under_source = children_of(plugin_source.span_context.span_id());
    for expected in ["handshake", "configure", "start", "finish"] {
        assert!(under_source.contains(&expected), "{expected} missing under the source: {under_source:?}");
    }
    assert_eq!(under_source.iter().filter(|n| **n == "configure").count(), 2, "core + plugin: {under_source:?}");
    // Per-batch spans from both plugins, with their attributes.
    let batches = by_name("loadsmith.batch");
    assert!(batches.len() >= 6, "{names:?}");
    // A plugin's custom span, nested in its batch span, masked.
    let query = by_name("fake.query");
    assert_eq!(query.len(), 3, "{names:?}");
    assert!(batches.iter().any(|b| b.span_context.span_id() == query[0].parent_span_id));
    assert_eq!(query[0].span_kind, opentelemetry::trace::SpanKind::Client);
    let text = attr(query[0], "db.query.text").unwrap();
    assert!(!text.contains(SECRET) && text.contains("***"), "{text}");

    // ── Metrics: the core's and the plugins' ─────────────────────────────
    let exported = metrics.get_finished_metrics().unwrap();
    let mut sums: BTreeMap<String, f64> = BTreeMap::new();
    let mut histogram_counts: BTreeMap<String, u64> = BTreeMap::new();
    for rm in &exported {
        for sm in rm.scope_metrics() {
            for m in sm.metrics() {
                let key = |attrs: Vec<String>| format!("{}{{{}}}", m.name(), attrs.join(","));
                match m.data() {
                    AggregatedMetrics::U64(MetricData::Sum(s)) => {
                        for dp in s.data_points() {
                            let attrs = dp.attributes().map(|kv| format!("{}={}", kv.key, kv.value)).collect();
                            sums.insert(key(attrs), dp.value() as f64);
                        }
                    }
                    AggregatedMetrics::F64(MetricData::Sum(s)) => {
                        for dp in s.data_points() {
                            let mut attrs: Vec<String> = dp
                                .attributes()
                                .filter(|kv| kv.key.as_str() != "loadsmith.plugin.name" && kv.key.as_str() != "loadsmith.plugin.version")
                                .map(|kv| format!("{}={}", kv.key, kv.value))
                                .collect();
                            attrs.sort();
                            sums.insert(key(attrs), dp.value());
                        }
                    }
                    AggregatedMetrics::F64(MetricData::Histogram(h)) => {
                        for dp in h.data_points() {
                            *histogram_counts.entry(m.name().to_string()).or_default() += dp.count();
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    assert_eq!(sums.get("loadsmith.rows{direction=read}"), Some(&30.0), "{sums:?}");
    assert_eq!(sums.get("loadsmith.rows{direction=written}"), Some(&30.0), "{sums:?}");
    assert_eq!(sums.get("loadsmith.batches{direction=read}"), Some(&3.0), "{sums:?}");
    assert_eq!(sums.get("loadsmith.plugin.rows{direction=read,loadsmith.plugin.kind=source}"), Some(&30.0), "{sums:?}");
    assert_eq!(
        sums.get("loadsmith.plugin.rows{direction=written,loadsmith.plugin.kind=destination}"),
        Some(&30.0),
        "{sums:?}"
    );
    assert_eq!(sums.get("fake.queries{db.namespace=fake,loadsmith.plugin.kind=source}"), Some(&3.0), "{sums:?}");
    assert_eq!(histogram_counts.get("loadsmith.plugin.batch.duration"), Some(&6), "{histogram_counts:?}");

    // ── Logs: the core's and the plugins', correlated and masked ────────
    let records = logs.get_emitted_logs().unwrap();
    let bodies: Vec<String> =
        records.iter().map(|l| l.record.body().map(|b| format!("{b:?}")).unwrap_or_default()).collect();
    for b in &bodies {
        assert!(!b.contains(SECRET), "unmasked log: {b}");
    }
    let produced: Vec<_> = records
        .iter()
        .filter(|l| l.record.body().is_some_and(|b| format!("{b:?}").contains("fake source produced a batch")))
        .collect();
    assert_eq!(produced.len(), 3, "{bodies:?}");
    let ctx = produced[0].record.trace_context().expect("plugin log lines carry trace context");
    assert_eq!(ctx.trace_id.to_string(), trace_id);
    assert!(query.iter().any(|q| q.span_context.span_id() == ctx.span_id), "the log is in its span");
    // The careless plugin's raw fd4 line (with the password) arrived, masked.
    assert!(bodies.iter().any(|b| b.contains("configured with") && b.contains("***")), "{bodies:?}");
    // A core log line, inside the run's trace.
    let core = records
        .iter()
        .find(|l| l.record.body().is_some_and(|b| format!("{b:?}").contains("data pump started")))
        .expect("core logs are exported");
    assert_eq!(core.record.trace_context().unwrap().trace_id.to_string(), trace_id);
}
