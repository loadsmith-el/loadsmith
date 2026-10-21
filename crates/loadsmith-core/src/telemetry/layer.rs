//! The engine's `tracing` → OpenTelemetry bridge: present from startup,
//! dormant until [`super::install`].

use std::sync::atomic::Ordering;
use std::sync::OnceLock;
use std::time::SystemTime;

use loadsmith_protocol::LogLevel;
use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, Severity};
use opentelemetry::trace::{Tracer, TracerProvider as _};
use opentelemetry_sdk::trace::{Sampler, SdkTracer, SdkTracerProvider};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Filter, Layer};
use tracing_subscriber::registry::LookupSpan;

use super::{get, LOGS_ON, LOG_LEVEL, TRACES_ON};

/// The layers to add to the process's subscriber, once, at startup: core
/// spans and span events (`tracing-opentelemetry`), and core log events as
/// OTLP log records. Both stay off until telemetry is installed.
pub fn layers<S>() -> impl Layer<S> + 'static
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let traces = tracing_opentelemetry::layer()
        .with_tracer(LateTracer)
        .with_location(false)
        .with_threads(false)
        .with_tracked_inactivity(false)
        .with_filter(TraceFilter);
    let logs = LogLayer.with_filter(LogFilter);
    traces.and_then(logs)
}

/// Targets never exported: plugin log lines (forwarded with their own trace
/// context instead), the telemetry pipeline's own warnings, and the
/// exporters' HTTP/gRPC stacks (exporting their logs would feed back).
fn excluded(target: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "loadsmith::telemetry",
        "opentelemetry",
        "tracing_opentelemetry",
        "h2",
        "hyper",
        "tonic",
        "tower",
        "reqwest",
        "rustls",
    ];
    target == "plugin" || PREFIXES.iter().any(|p| target.starts_with(p))
}

pub(super) fn level_index_of(level: &LogLevel) -> u8 {
    match level {
        LogLevel::Error => 0,
        LogLevel::Warn => 1,
        LogLevel::Info => 2,
        LogLevel::Debug => 3,
        LogLevel::Trace => 4,
    }
}

fn level_index(level: &Level) -> u8 {
    match *level {
        Level::ERROR => 0,
        Level::WARN => 1,
        Level::INFO => 2,
        Level::DEBUG => 3,
        Level::TRACE => 4,
    }
}

fn event_exported(meta: &Metadata<'_>) -> bool {
    level_index(meta.level()) <= LOG_LEVEL.load(Ordering::Relaxed) && !excluded(meta.target())
}

fn interest(on: bool) -> tracing::subscriber::Interest {
    // Settings never change once installed, so the answer per callsite is
    // fixed; `install` rebuilds the callsite cache. Before it (and when
    // telemetry stays off) every callsite is `never` for these layers: no
    // cost at all.
    if on {
        tracing::subscriber::Interest::always()
    } else {
        tracing::subscriber::Interest::never()
    }
}

fn max_level(on: bool) -> Option<tracing::level_filters::LevelFilter> {
    use tracing::level_filters::LevelFilter;
    if !on {
        return Some(LevelFilter::OFF);
    }
    Some(match LOG_LEVEL.load(Ordering::Relaxed) {
        0 => LevelFilter::ERROR,
        1 => LevelFilter::WARN,
        2 => LevelFilter::INFO,
        3 => LevelFilter::DEBUG,
        _ => LevelFilter::TRACE,
    })
}

/// Core spans (`INFO` and above, from the engine's own crates) and the
/// events inside them, while traces are exported.
struct TraceFilter;

impl TraceFilter {
    fn wants(meta: &Metadata<'_>) -> bool {
        if !TRACES_ON.load(Ordering::Relaxed) {
            return false;
        }
        if meta.is_span() {
            *meta.level() <= Level::INFO && meta.target().starts_with("loadsmith")
        } else {
            event_exported(meta)
        }
    }
}

impl<S> Filter<S> for TraceFilter {
    fn enabled(&self, meta: &Metadata<'_>, _: &Context<'_, S>) -> bool {
        Self::wants(meta)
    }

    fn callsite_enabled(&self, meta: &'static Metadata<'static>) -> tracing::subscriber::Interest {
        interest(Self::wants(meta))
    }

    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        let on = TRACES_ON.load(Ordering::Relaxed);
        // Spans are exported from INFO up, events from the log level.
        max_level(on).map(|l| l.max(if on { tracing::level_filters::LevelFilter::INFO } else { l }))
    }
}

/// Core log events at or above `telemetry.log_level`, while logs are
/// exported.
struct LogFilter;

impl LogFilter {
    fn wants(meta: &Metadata<'_>) -> bool {
        LOGS_ON.load(Ordering::Relaxed) && meta.is_event() && event_exported(meta)
    }
}

impl<S> Filter<S> for LogFilter {
    fn enabled(&self, meta: &Metadata<'_>, _: &Context<'_, S>) -> bool {
        Self::wants(meta)
    }

    fn callsite_enabled(&self, meta: &'static Metadata<'static>) -> tracing::subscriber::Interest {
        interest(Self::wants(meta))
    }

    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        max_level(LOGS_ON.load(Ordering::Relaxed))
    }
}


/// The tracer behind `tracing-opentelemetry`: the run's, once installed (a
/// non-recording one before; the filter keeps it unused then).
#[derive(Debug, Clone, Copy)]
struct LateTracer;

fn idle_tracer() -> &'static SdkTracer {
    static IDLE: OnceLock<SdkTracer> = OnceLock::new();
    IDLE.get_or_init(|| SdkTracerProvider::builder().with_sampler(Sampler::AlwaysOff).build().tracer("idle"))
}

impl Tracer for LateTracer {
    type Span = opentelemetry_sdk::trace::Span;

    fn build_with_context(&self, builder: opentelemetry::trace::SpanBuilder, parent_cx: &opentelemetry::Context) -> Self::Span {
        let tracer = get().and_then(|t| t.core_tracer.as_ref()).unwrap_or_else(|| idle_tracer());
        tracer.build_with_context(builder, parent_cx)
    }
}

/// Core log events → OTLP log records (masked; trace context from the
/// active span, which `tracing-opentelemetry` activates on enter).
struct LogLayer;

impl<S> Layer<S> for LogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let Some(t) = get() else { return };
        let Some(logger) = &t.core_logger else { return };
        let mut fields = Fields::default();
        event.record(&mut fields);
        let meta = event.metadata();
        let mut record = logger.create_log_record();
        let (severity, text) = severity(meta.level());
        let now = SystemTime::now();
        record.set_timestamp(now);
        record.set_observed_timestamp(now);
        record.set_severity_number(severity);
        record.set_severity_text(text);
        record.set_target(meta.target().to_string());
        record.set_body(AnyValue::String(t.mask.apply(&fields.message.unwrap_or_default()).into()));
        for (k, v) in fields.attributes {
            record.add_attribute(k, mask_any(v, &t.mask));
        }
        logger.emit(record);
    }
}

pub(super) fn severity(level: &Level) -> (Severity, &'static str) {
    match *level {
        Level::TRACE => (Severity::Trace, "TRACE"),
        Level::DEBUG => (Severity::Debug, "DEBUG"),
        Level::INFO => (Severity::Info, "INFO"),
        Level::WARN => (Severity::Warn, "WARN"),
        Level::ERROR => (Severity::Error, "ERROR"),
    }
}

fn mask_any(v: AnyValue, mask: &loadsmith_config::MaskList) -> AnyValue {
    match v {
        AnyValue::String(s) => AnyValue::String(mask.apply(s.as_str()).into()),
        other => other,
    }
}

#[derive(Default)]
struct Fields {
    message: Option<String>,
    attributes: Vec<(String, AnyValue)>,
}

impl Fields {
    fn put(&mut self, field: &Field, value: AnyValue) {
        if field.name() == "message" {
            self.message = Some(match value {
                AnyValue::String(s) => s.to_string(),
                other => format!("{other:?}"),
            });
        } else if !field.name().starts_with("log.") {
            self.attributes.push((field.name().to_string(), value));
        }
    }
}

impl Visit for Fields {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.put(field, AnyValue::Double(value));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field, AnyValue::Int(value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        match i64::try_from(value) {
            Ok(v) => self.put(field, AnyValue::Int(v)),
            Err(_) => self.put(field, AnyValue::String(value.to_string().into())),
        }
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field, AnyValue::Boolean(value));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, AnyValue::String(value.to_string().into()));
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.put(field, AnyValue::String(value.to_string().into()));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.put(field, AnyValue::String(format!("{value:?}").into()));
    }
}
