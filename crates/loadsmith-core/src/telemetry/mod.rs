//! OpenTelemetry: traces, metrics and logs of a run, exported over OTLP.
//!
//! The core is the only exporter. Plugins record in-process and send their
//! spans, metrics and logs on fd4; the event drains hand them to
//! [`PluginScope`], which re-emits them here under the run's trace. See
//! `docs/src/architecture/telemetry.md`.
//!
//! - [`layers`] goes into the process's `tracing` subscriber at startup
//!   (dormant until [`install`]): core spans through `tracing-opentelemetry`,
//!   core log events as OTLP log records.
//! - [`install`] turns telemetry on for the run with resolved
//!   [`TelemetrySettings`]; nothing is exported before, and nothing at all
//!   when it is never called (zero cost: the layers' filters reject
//!   everything after one atomic load).
//! - [`shutdown`] flushes every signal within a bound.
//!
//! Export failures never fail the run: the first is reported once on
//! stderr, the rest are silent.

mod export;
mod layer;
mod plugin;
pub mod settings;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use loadsmith_config::{MaskList, TelemetrySignal};
use opentelemetry::logs::LoggerProvider as _;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, MeterProvider as _};
use opentelemetry::trace::{TraceContextExt, TracerProvider as _};
use opentelemetry::KeyValue;
use opentelemetry_sdk::logs::{SdkLogger, SdkLoggerProvider};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use opentelemetry_sdk::Resource;

pub use layer::layers;
pub use plugin::PluginScope;
pub use settings::{resolve, TelemetryFlags, TelemetrySettings};

/// The instrumentation scope of everything the engine itself records.
const SCOPE: &str = "loadsmith";
/// The instrumentation scope of what plugins send.
const PLUGIN_SCOPE: &str = "loadsmith.plugin";

static TELEMETRY: OnceLock<Telemetry> = OnceLock::new();
static TRACES_ON: AtomicBool = AtomicBool::new(false);
static LOGS_ON: AtomicBool = AtomicBool::new(false);
/// Most verbose level exported (`ERROR` = 0 … `TRACE` = 4).
static LOG_LEVEL: AtomicU8 = AtomicU8::new(2);

/// Who is running: the resource attributes of everything exported.
#[derive(Debug, Clone)]
pub struct RunIdentity {
    pub pipeline: String,
    pub run_id: String,
    /// The engine's version (`service.version`).
    pub version: String,
}

struct Telemetry {
    settings: TelemetrySettings,
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
    core_tracer: Option<SdkTracer>,
    plugin_tracer: Option<SdkTracer>,
    core_logger: Option<SdkLogger>,
    plugin_logger: Option<SdkLogger>,
    metrics: Option<CoreMetrics>,
    mask: MaskList,
}

/// The engine's own instruments.
struct CoreMetrics {
    rows: Counter<u64>,
    batches: Counter<u64>,
    bytes: Counter<u64>,
    run_duration: Histogram<f64>,
    runs: Counter<u64>,
    respawns: Counter<u64>,
    checkpoint_lag: Gauge<u64>,
    state_commit: Histogram<f64>,
    plugin_meter: Meter,
    plugin_instruments: Mutex<HashMap<(String, u8), plugin::Instrument>>,
}

fn get() -> Option<&'static Telemetry> {
    TELEMETRY.get()
}

/// Whether telemetry is on for this process.
pub fn enabled() -> bool {
    get().is_some()
}

/// Whether the traces signal is exported.
pub fn traces_enabled() -> bool {
    TRACES_ON.load(Ordering::Relaxed)
}

/// The run's telemetry settings, when on.
pub fn settings() -> Option<&'static TelemetrySettings> {
    get().map(|t| &t.settings)
}

/// Turns telemetry on with `settings`. Builds the exporters; if that fails
/// (an endpoint the exporter cannot use, TLS setup), warns once and leaves
/// telemetry off — the run goes on. Must run inside the Tokio runtime (the
/// gRPC exporter's channel lives on it). Only the first call counts.
pub fn install(settings: TelemetrySettings, run: &RunIdentity, mask: &MaskList) -> bool {
    install_using(settings, run, mask, export::build)
}

/// [`install`] with the given exporters instead of OTLP ones (tests read
/// in-memory exporters back). Each signal is on when its exporter is given
/// and `settings` selects it.
#[doc(hidden)]
pub fn install_with_exporters<S, M, L>(
    settings: TelemetrySettings,
    run: &RunIdentity,
    mask: &MaskList,
    spans: S,
    metrics: M,
    logs: L,
) -> bool
where
    S: opentelemetry_sdk::trace::SpanExporter + 'static,
    M: opentelemetry_sdk::metrics::exporter::PushMetricExporter,
    L: opentelemetry_sdk::logs::LogExporter + 'static,
{
    install_using(settings, run, mask, move |s, resource, mask| {
        Ok(export::pipelines(
            s,
            resource,
            mask,
            s.signal(TelemetrySignal::Traces).then_some(spans),
            s.signal(TelemetrySignal::Metrics).then_some(metrics),
            s.signal(TelemetrySignal::Logs).then_some(logs),
        ))
    })
}

fn install_using(
    settings: TelemetrySettings,
    run: &RunIdentity,
    mask: &MaskList,
    build: impl FnOnce(&TelemetrySettings, Resource, &MaskList) -> Result<export::Pipelines, String>,
) -> bool {
    if TELEMETRY.get().is_some() {
        return true;
    }
    for value in settings.headers.values() {
        mask.add(value.clone());
    }
    let resource = resource(&settings, run);
    let pipelines = match build(&settings, resource, mask) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(target: "loadsmith::telemetry", "telemetry is off for this run: {}", mask.apply(&e));
            return false;
        }
    };
    let core_tracer = pipelines.tracer_provider.as_ref().map(|p| p.tracer(SCOPE));
    let plugin_tracer = pipelines.tracer_provider.as_ref().map(|p| p.tracer(PLUGIN_SCOPE));
    let core_logger = pipelines.logger_provider.as_ref().map(|p| p.logger(SCOPE));
    let plugin_logger = pipelines.logger_provider.as_ref().map(|p| p.logger(PLUGIN_SCOPE));
    let metrics = pipelines.meter_provider.as_ref().map(|p| CoreMetrics::new(&p.meter(SCOPE), p.meter(PLUGIN_SCOPE)));
    let level = layer::level_index_of(&settings.log_level);
    let traces = core_tracer.is_some();
    let logs = core_logger.is_some();
    let telemetry = Telemetry {
        settings,
        tracer_provider: pipelines.tracer_provider,
        meter_provider: pipelines.meter_provider,
        logger_provider: pipelines.logger_provider,
        core_tracer,
        plugin_tracer,
        core_logger,
        plugin_logger,
        metrics,
        mask: mask.clone(),
    };
    if TELEMETRY.set(telemetry).is_err() {
        return true;
    }
    LOG_LEVEL.store(level, Ordering::Relaxed);
    TRACES_ON.store(traces, Ordering::Release);
    LOGS_ON.store(logs, Ordering::Release);
    tracing::callsite::rebuild_interest_cache();
    true
}

/// Exports what is left and stops every exporter, within `timeout` (the
/// signals flush in parallel). Exporters still failing are not reported
/// again.
pub async fn shutdown(timeout: Duration) {
    let Some(t) = get() else { return };
    TRACES_ON.store(false, Ordering::Release);
    LOGS_ON.store(false, Ordering::Release);
    let mut tasks = Vec::new();
    if let Some(p) = t.tracer_provider.clone() {
        tasks.push(tokio::task::spawn_blocking(move || drop(p.shutdown_with_timeout(timeout))));
    }
    if let Some(p) = t.meter_provider.clone() {
        tasks.push(tokio::task::spawn_blocking(move || drop(p.shutdown_with_timeout(timeout))));
    }
    if let Some(p) = t.logger_provider.clone() {
        tasks.push(tokio::task::spawn_blocking(move || drop(p.shutdown_with_timeout(timeout))));
    }
    // The bound holds even if an exporter ignores its own timeout.
    let all = async {
        for task in tasks {
            let _ = task.await;
        }
    };
    let _ = tokio::time::timeout(timeout + Duration::from_millis(500), all).await;
}

/// Exports what every signal holds now, without stopping (tests).
#[doc(hidden)]
pub fn force_flush() {
    let Some(t) = get() else { return };
    if let Some(p) = &t.tracer_provider {
        let _ = p.force_flush();
    }
    if let Some(p) = &t.meter_provider {
        let _ = p.force_flush();
    }
    if let Some(p) = &t.logger_provider {
        let _ = p.force_flush();
    }
}

/// The root span of a run (`loadsmith.run`). Record `loadsmith.run.outcome`
/// and `loadsmith.run.exit_code` on it when the run ends.
pub fn run_span(run: &RunIdentity) -> tracing::Span {
    tracing::info_span!(
        target: "loadsmith::run",
        "loadsmith.run",
        loadsmith.pipeline.name = %run.pipeline,
        loadsmith.run.id = %run.run_id,
        loadsmith.run.outcome = tracing::field::Empty,
        loadsmith.run.exit_code = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        error = tracing::field::Empty,
    )
}

/// Adds an event to `span` (when traces are exported), masked on export.
/// Unlike a `tracing` event it prints nothing on stderr.
pub fn span_event(span: &tracing::Span, name: &'static str, attributes: Vec<KeyValue>) {
    if traces_enabled() {
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        span.add_event(name, attributes);
    }
}

/// The trace id of `span` (hex), when it is exported.
pub fn trace_id(span: &tracing::Span) -> Option<String> {
    if !traces_enabled() {
        return None;
    }
    let sc = span.in_scope(|| opentelemetry::Context::current().span().span_context().clone());
    sc.is_valid().then(|| sc.trace_id().to_string())
}


// ── Core metrics ──────────────────────────────────────────────────────────────

impl CoreMetrics {
    fn new(meter: &Meter, plugin_meter: Meter) -> Self {
        Self {
            rows: meter.u64_counter("loadsmith.rows").with_unit("{row}").with_description("Rows through the data plane").build(),
            batches: meter
                .u64_counter("loadsmith.batches")
                .with_unit("{batch}")
                .with_description("Arrow batches through the data plane")
                .build(),
            bytes: meter
                .u64_counter("loadsmith.bytes")
                .with_unit("By")
                .with_description("In-memory size of the Arrow batches through the data plane")
                .build(),
            run_duration: meter
                .f64_histogram("loadsmith.run.duration")
                .with_unit("s")
                .with_description("Duration of a run")
                .with_boundaries(vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0])
                .build(),
            runs: meter.u64_counter("loadsmith.runs").with_unit("{run}").with_description("Runs, by outcome").build(),
            respawns: meter
                .u64_counter("loadsmith.plugin.respawns")
                .with_unit("{respawn}")
                .with_description("Plugin processes respawned after a failure")
                .build(),
            checkpoint_lag: meter
                .u64_gauge("loadsmith.checkpoint.lag")
                .with_unit("{batch}")
                .with_description("Batches checkpointed by the source but not yet persisted as state")
                .build(),
            state_commit: meter
                .f64_histogram("loadsmith.state.commit.duration")
                .with_unit("s")
                .with_description("Time to persist the incremental state")
                .build(),
            plugin_meter,
            plugin_instruments: Mutex::new(HashMap::new()),
        }
    }
}

fn metrics() -> Option<&'static CoreMetrics> {
    get().and_then(|t| t.metrics.as_ref())
}

/// One batch through the data plane, `direction` `read` or `written`.
pub fn record_batch(direction: &'static str, rows: u64, bytes: u64) {
    if let Some(m) = metrics() {
        let attrs = [KeyValue::new("direction", direction)];
        m.rows.add(rows, &attrs);
        m.batches.add(1, &attrs);
        m.bytes.add(bytes, &attrs);
    }
}

/// A finished run: `loadsmith.runs` by outcome and `loadsmith.run.duration`.
pub fn record_run(outcome: &str, duration: Duration) {
    if let Some(m) = metrics() {
        let attrs = [KeyValue::new("outcome", outcome.to_string())];
        m.runs.add(1, &attrs);
        m.run_duration.record(duration.as_secs_f64(), &attrs);
    }
}

/// A plugin process respawned (the sink after a failure).
pub fn record_respawn(kind: &'static str) {
    if let Some(m) = metrics() {
        m.respawns.add(1, &[KeyValue::new("loadsmith.plugin.kind", kind)]);
    }
}

/// Batches checkpointed but not yet persisted.
pub fn record_checkpoint_lag(batches: u64) {
    if let Some(m) = metrics() {
        m.checkpoint_lag.record(batches, &[]);
    }
}

/// How long persisting the state took.
pub fn record_state_commit(took: Duration) {
    if let Some(m) = metrics() {
        m.state_commit.record(took.as_secs_f64(), &[]);
    }
}

// ── Resource ──────────────────────────────────────────────────────────────────

fn resource(settings: &TelemetrySettings, run: &RunIdentity) -> Resource {
    let mut attrs: Vec<KeyValue> = vec![
        KeyValue::new("service.name", "loadsmith"),
        KeyValue::new("service.version", run.version.clone()),
        KeyValue::new("loadsmith.pipeline.name", run.pipeline.clone()),
        KeyValue::new("loadsmith.run.id", run.run_id.clone()),
        KeyValue::new("os.type", std::env::consts::OS),
        KeyValue::new("host.arch", std::env::consts::ARCH),
        KeyValue::new("process.pid", i64::from(std::process::id())),
    ];
    if let Some(host) = hostname() {
        attrs.push(KeyValue::new("host.name", host));
    }
    if let Some(exe) = std::env::current_exe().ok().and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned())) {
        attrs.push(KeyValue::new("process.executable.name", exe));
    }
    // OTEL_RESOURCE_ATTRIBUTES, OTEL_SERVICE_NAME and `telemetry.resource`,
    // already merged in that order, win over the engine's defaults.
    attrs.retain(|kv| !settings.resource.contains_key(kv.key.as_str()));
    attrs.extend(settings.resource.iter().map(|(k, v)| KeyValue::new(k.clone(), v.clone())));
    Resource::builder_empty()
        .with_attributes(attrs)
        .with_detector(Box::new(opentelemetry_sdk::resource::TelemetryResourceDetector))
        .build()
}

fn hostname() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
}

/// Whether `signal` is exported (telemetry on and the signal selected).
pub fn signal_enabled(signal: TelemetrySignal) -> bool {
    get().is_some_and(|t| t.settings.signal(signal))
}

