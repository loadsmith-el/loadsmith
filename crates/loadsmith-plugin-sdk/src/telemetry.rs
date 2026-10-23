//! Plugin telemetry: spans, metrics and logs recorded in-process and sent to
//! the core on the event plane (fd4). The core is the only exporter — a
//! plugin never opens a network connection for telemetry and carries no
//! OTLP stack.
//!
//! **Spans and logs** come from [`tracing`]: the SDK's run entry points
//! install a subscriber with [`layer()`], which turns every closed
//! `info_span!` (or `warn_span!`/`error_span!`) into a `span` message and
//! every event at or above the run's telemetry log level into a `log`
//! message carrying the trace context. Spans hang under the core's
//! per-plugin span (`configure.telemetry.traceparent`). Special span fields,
//! as in `tracing-opentelemetry`:
//!
//! | field | effect |
//! |---|---|
//! | `otel.name` | the span's name (the macro's name is static) |
//! | `otel.kind` | `client` / `server` / `producer` / `consumer` / `internal` |
//! | `otel.status_code` | `ok` / `error` |
//! | `otel.status_message`, `error` | the error status and its message |
//!
//! Any other field becomes an attribute: name them after the OTel semantic
//! conventions (`db.namespace`, `db.operation.name`, `http.request.method`,
//! …). Events inside a span also become span events; an `error!` event marks
//! its span as failed.
//!
//! **Metrics** use the thin instruments of this module — [`counter`],
//! [`up_down_counter`], [`histogram`], [`gauge`] — aggregated in-process
//! (delta temporality) and flushed periodically and at the end of the run.
//!
//! Everything is a no-op when the core did not enable telemetry for the run
//! (the default): an instrument call is one atomic load, and spans and
//! events are filtered out before they are built.
//!
//! The volume is bounded: at most [`MAX_SPANS_PER_SECOND`] spans per second
//! and [`MAX_PENDING`] queued messages; the excess is dropped and counted in
//! the `loadsmith.telemetry.dropped` metric. Instrument per query, batch,
//! request or file — never per row.
//!
//! A plugin that installs its own global `tracing` subscriber must add
//! [`layer()`] to it, or its spans and logs are not forwarded.

use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use loadsmith_protocol::{
    encode_hex, Attributes, HistogramValue, LogEvent, LogLevel, Message, MetricKind, MetricPoint, MetricsBatch,
    SpanData, SpanEvent, SpanKind, SpanStatus, StatusCode, TelemetryContext, Temporality, TraceParent,
};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes as SpanAttributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Filter, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// Spans recorded per second at most; the rest are dropped (and counted).
pub const MAX_SPANS_PER_SECOND: u32 = 1000;
/// Messages (spans and logs) waiting for fd4 at most; the rest are dropped.
pub const MAX_PENDING: usize = 10_000;
/// Distinct metric series (instrument × attribute set) per flush at most.
pub const MAX_SERIES: usize = 2_000;
/// The metric flush interval when the core does not set one.
pub const DEFAULT_METRICS_INTERVAL: Duration = Duration::from_secs(15);
/// The counter that records what the bounds above dropped.
pub const DROPPED_METRIC: &str = "loadsmith.telemetry.dropped";

/// Default histogram bounds for durations in seconds (the OTel semantic
/// conventions' recommendation for `*.duration` histograms).
pub const DURATION_BOUNDS: &[f64] =
    &[0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0];
/// The OTel SDK's default explicit histogram bounds.
pub const DEFAULT_BOUNDS: &[f64] =
    &[0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0, 10000.0];

// ── Recorder ──────────────────────────────────────────────────────────────────

/// The run's telemetry settings, fixed by `configure`.
#[derive(Debug, Clone)]
struct Settings {
    trace_id: [u8; 16],
    /// The core's per-plugin span: the parent of this plugin's top spans.
    root_span: [u8; 8],
    /// Whether the core samples this trace (parent-based: unsampled ⇒ no
    /// spans, logs and metrics still flow).
    sampled: bool,
    log_level: Level,
    metrics_interval: Duration,
}

/// Holds what the plugin recorded until the SDK writes it to fd4.
pub(crate) struct Recorder {
    enabled: AtomicBool,
    settings: OnceLock<Settings>,
    /// `max level` index (see [`level_index`]) for events, cached.
    log_level: AtomicU8,
    pending: Mutex<VecDeque<Message>>,
    metrics: Mutex<BTreeMap<SeriesKey, Series>>,
    span_budget: Mutex<(u64, u32)>,
    dropped_spans: AtomicU64,
    dropped_logs: AtomicU64,
    dropped_points: AtomicU64,
    last_metrics_flush: Mutex<Option<Instant>>,
}

/// The process's recorder: what [`layer()`], the instruments and the SDK's
/// fd4 writer share.
pub(crate) static GLOBAL: Recorder = Recorder::new();

impl Recorder {
    pub(crate) const fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            settings: OnceLock::new(),
            log_level: AtomicU8::new(0),
            pending: Mutex::new(VecDeque::new()),
            metrics: Mutex::new(BTreeMap::new()),
            span_budget: Mutex::new((0, 0)),
            dropped_spans: AtomicU64::new(0),
            dropped_logs: AtomicU64::new(0),
            dropped_points: AtomicU64::new(0),
            last_metrics_flush: Mutex::new(None),
        }
    }

    /// Applies `configure.telemetry`. Only the first enabling call counts
    /// (one `configure` per process).
    pub(crate) fn start(&self, ctx: Option<&TelemetryContext>) {
        let Some(ctx) = ctx.filter(|c| c.enabled) else { return };
        let parent = ctx.traceparent.as_deref().and_then(TraceParent::parse);
        let settings = Settings {
            trace_id: parent.map(|p| p.trace_id).unwrap_or_else(random_trace_id),
            root_span: parent.map(|p| p.span_id).unwrap_or([0; 8]),
            sampled: parent.is_none_or(|p| p.sampled),
            log_level: to_tracing_level(&ctx.log_level),
            metrics_interval: ctx
                .metrics_interval_ms
                .filter(|ms| *ms > 0)
                .map(Duration::from_millis)
                .unwrap_or(DEFAULT_METRICS_INTERVAL),
        };
        if self.settings.set(settings.clone()).is_ok() {
            self.log_level.store(level_index(&settings.log_level), Ordering::Relaxed);
            *lock(&self.last_metrics_flush) = Some(Instant::now());
            self.enabled.store(true, Ordering::Release);
        }
    }

    fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    fn settings(&self) -> Option<&Settings> {
        self.is_enabled().then(|| self.settings.get()).flatten()
    }

    fn push(&self, msg: Message, dropped: &AtomicU64) {
        let mut q = lock(&self.pending);
        if q.len() >= MAX_PENDING {
            dropped.fetch_add(1, Ordering::Relaxed);
        } else {
            q.push_back(msg);
        }
    }

    /// One more span this second, unless over [`MAX_SPANS_PER_SECOND`].
    fn admit_span(&self) -> bool {
        let second = unix_nanos() / 1_000_000_000;
        let mut budget = lock(&self.span_budget);
        if budget.0 != second {
            *budget = (second, 0);
        }
        if budget.1 >= MAX_SPANS_PER_SECOND {
            self.dropped_spans.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        budget.1 += 1;
        true
    }

    /// The spans and logs waiting for fd4.
    pub(crate) fn take_pending(&self) -> Vec<Message> {
        if !self.is_enabled() {
            return Vec::new();
        }
        lock(&self.pending).drain(..).collect()
    }

    /// The aggregated metrics as one `metrics` message, if `force` or the
    /// flush interval has passed, and there is anything to send. Resets the
    /// aggregation (delta temporality).
    pub(crate) fn take_metrics(&self, force: bool) -> Option<Message> {
        let settings = self.settings()?;
        {
            let mut last = lock(&self.last_metrics_flush);
            let due = last.is_none_or(|t| t.elapsed() >= settings.metrics_interval);
            if !(force || due) {
                return None;
            }
            *last = Some(Instant::now());
        }
        let series = std::mem::take(&mut *lock(&self.metrics));
        let mut points: Vec<MetricPoint> = series.into_iter().map(|(k, s)| s.into_point(k)).collect();
        for (signal, counter) in
            [("spans", &self.dropped_spans), ("logs", &self.dropped_logs), ("metrics", &self.dropped_points)]
        {
            let n = counter.swap(0, Ordering::Relaxed);
            if n > 0 {
                points.push(MetricPoint {
                    name: DROPPED_METRIC.into(),
                    unit: Some("{item}".into()),
                    description: Some("Telemetry items the plugin SDK dropped over its volume bounds".into()),
                    kind: MetricKind::Counter,
                    temporality: Temporality::Delta,
                    attributes: [("loadsmith.telemetry.signal".to_string(), signal.into())].into(),
                    value: Some(n as f64),
                    histogram: None,
                });
            }
        }
        (!points.is_empty()).then_some(Message::Metrics(MetricsBatch { points }))
    }

    fn record_metric(&self, inst: &Instrument, value: f64, attrs: &[KeyValue]) {
        if !self.is_enabled() || !value.is_finite() {
            return;
        }
        let key = SeriesKey {
            name: inst.name.to_string(),
            kind: inst.kind as u8,
            attrs: attrs.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect(),
        };
        let mut map = lock(&self.metrics);
        if !map.contains_key(&key) && map.len() >= MAX_SERIES {
            self.dropped_points.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let series = map.entry(key).or_insert_with(|| Series::new(inst, attrs));
        series.record(value);
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Whether the core enabled telemetry for this run. Use it to skip work
/// that only feeds telemetry (formatting an expensive attribute).
pub fn enabled() -> bool {
    GLOBAL.is_enabled()
}

// ── Metrics ───────────────────────────────────────────────────────────────────

/// One attribute of a metric data point: [`kv`]`("db.namespace", "public")`.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyValue {
    pub key: Cow<'static, str>,
    pub value: serde_json::Value,
}

/// An attribute: a key and a string, boolean, integer or float value.
pub fn kv(key: impl Into<Cow<'static, str>>, value: impl Into<serde_json::Value>) -> KeyValue {
    KeyValue { key: key.into(), value: value.into() }
}

/// A number an instrument records (integers and floats; a
/// [`Duration`](std::time::Duration) records its seconds).
pub trait MetricValue {
    fn into_f64(self) -> f64;
}

macro_rules! metric_value {
    ($($t:ty),*) => {$(
        impl MetricValue for $t {
            fn into_f64(self) -> f64 {
                self as f64
            }
        }
    )*};
}
metric_value!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, f32, f64);

impl MetricValue for Duration {
    fn into_f64(self) -> f64 {
        self.as_secs_f64()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Counter = 0,
    UpDownCounter = 1,
    Histogram = 2,
    Gauge = 3,
}

/// A metric instrument. Cheap to build: make one where you record, or keep
/// one around. Recording is a no-op while telemetry is off.
#[derive(Debug, Clone)]
pub struct Instrument {
    name: Cow<'static, str>,
    kind: Kind,
    unit: Option<Cow<'static, str>>,
    description: Option<Cow<'static, str>>,
    bounds: Option<Cow<'static, [f64]>>,
}

impl Instrument {
    fn new(name: impl Into<Cow<'static, str>>, kind: Kind) -> Self {
        Self { name: name.into(), kind, unit: None, description: None, bounds: None }
    }

    /// The UCUM unit (`s`, `By`, `{row}`).
    pub fn with_unit(mut self, unit: impl Into<Cow<'static, str>>) -> Self {
        self.unit = Some(unit.into());
        self
    }

    pub fn with_description(mut self, description: impl Into<Cow<'static, str>>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Histogram bucket bounds (ascending). Default: [`DURATION_BOUNDS`] when
    /// the unit is `s`, else [`DEFAULT_BOUNDS`].
    pub fn with_bounds(mut self, bounds: impl Into<Cow<'static, [f64]>>) -> Self {
        self.bounds = Some(bounds.into());
        self
    }

    /// Counter / up-down counter: adds `value` (a counter ignores negative
    /// values).
    pub fn add(&self, value: impl MetricValue, attrs: &[KeyValue]) {
        let value = value.into_f64();
        if self.kind == Kind::Counter && value < 0.0 {
            return;
        }
        GLOBAL.record_metric(self, value, attrs);
    }

    /// Histogram: records one measurement. Gauge: sets the current value.
    pub fn record(&self, value: impl MetricValue, attrs: &[KeyValue]) {
        GLOBAL.record_metric(self, value.into_f64(), attrs);
    }

    fn bounds(&self) -> Vec<f64> {
        match (&self.bounds, self.unit.as_deref()) {
            (Some(b), _) => b.to_vec(),
            (None, Some("s")) => DURATION_BOUNDS.to_vec(),
            (None, _) => DEFAULT_BOUNDS.to_vec(),
        }
    }
}

/// A monotonic sum (`add`): rows, bytes, requests.
pub fn counter(name: impl Into<Cow<'static, str>>) -> Instrument {
    Instrument::new(name, Kind::Counter)
}

/// A sum that goes up and down (`add`): open connections, queue depth.
pub fn up_down_counter(name: impl Into<Cow<'static, str>>) -> Instrument {
    Instrument::new(name, Kind::UpDownCounter)
}

/// A distribution (`record`): durations, sizes.
pub fn histogram(name: impl Into<Cow<'static, str>>) -> Instrument {
    Instrument::new(name, Kind::Histogram)
}

/// The last value of something (`record`): lag, a pool's size.
pub fn gauge(name: impl Into<Cow<'static, str>>) -> Instrument {
    Instrument::new(name, Kind::Gauge)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SeriesKey {
    name: String,
    kind: u8,
    attrs: Vec<(String, String)>,
}

#[derive(Debug)]
struct Series {
    unit: Option<String>,
    description: Option<String>,
    attributes: Attributes,
    agg: Agg,
}

#[derive(Debug)]
enum Agg {
    Sum { kind: MetricKind, value: f64 },
    Last(f64),
    Histogram { count: u64, sum: f64, min: f64, max: f64, buckets: Vec<u64>, bounds: Vec<f64> },
}

impl Series {
    fn new(inst: &Instrument, attrs: &[KeyValue]) -> Self {
        let agg = match inst.kind {
            Kind::Counter => Agg::Sum { kind: MetricKind::Counter, value: 0.0 },
            Kind::UpDownCounter => Agg::Sum { kind: MetricKind::UpDownCounter, value: 0.0 },
            Kind::Gauge => Agg::Last(0.0),
            Kind::Histogram => {
                let bounds = inst.bounds();
                Agg::Histogram {
                    count: 0,
                    sum: 0.0,
                    min: f64::INFINITY,
                    max: f64::NEG_INFINITY,
                    buckets: vec![0; bounds.len() + 1],
                    bounds,
                }
            }
        };
        Self {
            unit: inst.unit.as_deref().map(str::to_string),
            description: inst.description.as_deref().map(str::to_string),
            attributes: attrs.iter().map(|kv| (kv.key.to_string(), kv.value.clone())).collect(),
            agg,
        }
    }

    fn record(&mut self, v: f64) {
        match &mut self.agg {
            Agg::Sum { value, .. } => *value += v,
            Agg::Last(last) => *last = v,
            Agg::Histogram { count, sum, min, max, buckets, bounds } => {
                *count += 1;
                *sum += v;
                *min = min.min(v);
                *max = max.max(v);
                let i = bounds.iter().position(|b| v <= *b).unwrap_or(bounds.len());
                buckets[i] += 1;
            }
        }
    }

    fn into_point(self, key: SeriesKey) -> MetricPoint {
        let (kind, value, histogram) = match self.agg {
            Agg::Sum { kind, value } => (kind, Some(value), None),
            Agg::Last(v) => (MetricKind::Gauge, Some(v), None),
            Agg::Histogram { count, sum, min, max, buckets, bounds } => (
                MetricKind::Histogram,
                None,
                Some(HistogramValue { count, sum, min, max, bucket_counts: buckets, explicit_bounds: bounds }),
            ),
        };
        MetricPoint {
            name: key.name,
            unit: self.unit,
            description: self.description,
            kind,
            temporality: Temporality::Delta,
            attributes: self.attributes,
            value,
            histogram,
        }
    }
}

// ── Spans and logs: the tracing layer ─────────────────────────────────────────

/// The `tracing` layer that forwards spans and log events to the core. The
/// SDK installs it; add it yourself only if your plugin sets its own global
/// subscriber.
pub fn layer<S>() -> impl Layer<S> + Send + Sync + 'static
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    layer_for(&GLOBAL)
}

pub(crate) fn layer_for<S>(rec: &'static Recorder) -> impl Layer<S> + Send + Sync + 'static
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    TelemetryLayer { rec }.with_filter(TelemetryFilter { rec })
}

/// Installs the global subscriber with [`layer()`], unless the plugin set
/// one already (then it should include [`layer()`] itself).
pub(crate) fn install() {
    let subscriber = tracing_subscriber::registry().with(layer());
    let _ = tracing::subscriber::set_global_default(subscriber);
}

/// Decides, per callsite hit, whether the layer sees a span or an event:
/// nothing while telemetry is off; spans at `INFO` and above (when the
/// trace is sampled); events at or above the run's telemetry log level.
struct TelemetryFilter {
    rec: &'static Recorder,
}

impl<S> Filter<S> for TelemetryFilter {
    fn enabled(&self, meta: &Metadata<'_>, _: &Context<'_, S>) -> bool {
        let Some(settings) = self.rec.settings() else { return false };
        if meta.is_span() {
            settings.sampled && *meta.level() <= Level::INFO
        } else {
            level_index(meta.level()) <= self.rec.log_level.load(Ordering::Relaxed)
        }
    }

    fn callsite_enabled(&self, _: &'static Metadata<'static>) -> tracing::subscriber::Interest {
        // Re-evaluated on every hit: telemetry turns on at `configure`.
        tracing::subscriber::Interest::sometimes()
    }
}

struct TelemetryLayer {
    rec: &'static Recorder,
}

/// A span being recorded (in the span's extensions).
struct OpenSpan {
    id: [u8; 8],
    parent: [u8; 8],
    name: String,
    kind: SpanKind,
    start: u64,
    status: SpanStatus,
    attributes: Attributes,
    events: Vec<SpanEvent>,
}

/// A span the budget dropped: its children hang on its nearest recorded
/// ancestor.
struct DroppedSpan;

impl TelemetryLayer {
    /// The id of the nearest recorded span from `span` up, or the core's
    /// per-plugin span.
    fn context_of<S>(&self, span: Option<tracing_subscriber::registry::SpanRef<'_, S>>, root: [u8; 8]) -> [u8; 8]
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        span.and_then(|s| s.scope().find_map(|s| s.extensions().get::<OpenSpan>().map(|o| o.id))).unwrap_or(root)
    }
}

impl<S> Layer<S> for TelemetryLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &SpanAttributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(settings) = self.rec.settings() else { return };
        let Some(span) = ctx.span(id) else { return };
        if !self.rec.admit_span() {
            span.extensions_mut().insert(DroppedSpan);
            return;
        }
        let parent = self.context_of(span.parent(), settings.root_span);
        let mut open = OpenSpan {
            id: random_span_id(),
            parent,
            name: attrs.metadata().name().to_string(),
            kind: SpanKind::Internal,
            start: unix_nanos(),
            status: SpanStatus::default(),
            attributes: Attributes::new(),
            events: Vec::new(),
        };
        attrs.record(&mut SpanVisitor(&mut open));
        span.extensions_mut().insert(open);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut ext = span.extensions_mut();
        if let Some(open) = ext.get_mut::<OpenSpan>() {
            values.record(&mut SpanVisitor(open));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let Some(settings) = self.rec.settings() else { return };
        let mut fields = EventVisitor::default();
        event.record(&mut fields);
        let now = unix_nanos();
        let level = *event.metadata().level();
        let message = fields.message.unwrap_or_else(|| event.metadata().name().to_string());

        let span = ctx.event_span(event);
        let recorded = span.as_ref().and_then(|s| {
            s.scope().find(|s| s.extensions().get::<OpenSpan>().is_some())
        });
        let span_id = match &recorded {
            Some(s) => {
                let mut ext = s.extensions_mut();
                let open = ext.get_mut::<OpenSpan>().expect("found above");
                open.events.push(SpanEvent {
                    name: message.clone(),
                    time_unix_nano: now,
                    attributes: fields.attributes.clone(),
                });
                if level == Level::ERROR && open.status.code != StatusCode::Error {
                    open.status = SpanStatus { code: StatusCode::Error, message: Some(message.clone()) };
                }
                open.id
            }
            None => settings.root_span,
        };

        let mut log = LogEvent::new(to_log_level(&level), message);
        log.time_unix_nano = Some(now);
        log.attributes = fields.attributes;
        if settings.root_span != [0; 8] || span_id != [0; 8] {
            log.trace_id = Some(encode_hex(&settings.trace_id));
            log.span_id = Some(encode_hex(&span_id));
        }
        self.rec.push(Message::Log(log), &self.rec.dropped_logs);
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(settings) = self.rec.settings() else { return };
        let Some(span) = ctx.span(&id) else { return };
        let Some(open) = span.extensions_mut().remove::<OpenSpan>() else { return };
        let data = SpanData {
            trace_id: encode_hex(&settings.trace_id),
            span_id: encode_hex(&open.id),
            parent_span_id: (open.parent != [0; 8]).then(|| encode_hex(&open.parent)),
            name: open.name,
            kind: open.kind,
            start_unix_nano: open.start,
            end_unix_nano: unix_nanos().max(open.start),
            status: open.status,
            attributes: open.attributes,
            events: open.events,
            links: Vec::new(),
        };
        self.rec.push(Message::Span(Box::new(data)), &self.rec.dropped_spans);
    }
}

/// Trace context of the current `tracing` span for a log line the plugin
/// sends itself (`EventSender::log`): `(trace_id, span_id)` in hex.
pub(crate) fn current_context() -> Option<(String, String)> {
    let settings = GLOBAL.settings()?;
    let span_id = tracing::Span::current()
        .with_subscriber(|(id, dispatch)| {
            let registry = dispatch.downcast_ref::<tracing_subscriber::Registry>()?;
            let span = registry.span(id)?;
            span.scope().find_map(|s| s.extensions().get::<OpenSpan>().map(|o| o.id))
        })
        .flatten()
        .unwrap_or(settings.root_span);
    (span_id != [0; 8]).then(|| (encode_hex(&settings.trace_id), encode_hex(&span_id)))
}

struct SpanVisitor<'a>(&'a mut OpenSpan);

impl SpanVisitor<'_> {
    fn put(&mut self, field: &Field, value: serde_json::Value) {
        let span = &mut *self.0;
        match (field.name(), &value) {
            ("otel.name", serde_json::Value::String(s)) => span.name = s.clone(),
            ("otel.kind", serde_json::Value::String(s)) => {
                span.kind = match s.to_ascii_lowercase().as_str() {
                    "client" => SpanKind::Client,
                    "server" => SpanKind::Server,
                    "producer" => SpanKind::Producer,
                    "consumer" => SpanKind::Consumer,
                    _ => SpanKind::Internal,
                }
            }
            ("otel.status_code", serde_json::Value::String(s)) => match s.to_ascii_lowercase().as_str() {
                "ok" => span.status.code = StatusCode::Ok,
                "error" => span.status.code = StatusCode::Error,
                _ => {}
            },
            ("otel.status_message" | "otel.status_description", serde_json::Value::String(s)) => {
                span.status.message = Some(s.clone())
            }
            ("error", _) => {
                let text = match value {
                    serde_json::Value::String(s) => s,
                    other => other.to_string(),
                };
                span.status = SpanStatus { code: StatusCode::Error, message: Some(text.clone()) };
                span.attributes.insert("error.message".into(), text.into());
            }
            (name, _) => {
                span.attributes.insert(name.to_string(), value);
            }
        }
    }
}

impl Visit for SpanVisitor<'_> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.put(field, value.into());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field, value.into());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field, value.into());
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, value.into());
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.put(field, value.to_string().into());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.put(field, format!("{value:?}").into());
    }
}

#[derive(Default)]
struct EventVisitor {
    message: Option<String>,
    attributes: Attributes,
}

impl EventVisitor {
    fn put(&mut self, field: &Field, value: serde_json::Value) {
        if field.name() == "message" {
            self.message = Some(match value {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            });
        } else if !field.name().starts_with("log.") {
            self.attributes.insert(field.name().to_string(), value);
        }
    }
}

impl Visit for EventVisitor {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.put(field, value.into());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field, value.into());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field, value.into());
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, value.into());
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.put(field, value.to_string().into());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.put(field, format!("{value:?}").into());
    }
}

// ── The SDK's built-in instrumentation ────────────────────────────────────────

/// A lifecycle phase span (`configure`, `start`, `finish`, …) with room for
/// an error.
macro_rules! phase_span {
    ($name:literal) => {
        tracing::info_span!($name, error = tracing::field::Empty)
    };
}
pub(crate) use phase_span;

/// The span of one unit of work: a batch read or written.
pub(crate) fn batch_span(seq: u64, direction: &'static str) -> tracing::Span {
    tracing::info_span!(
        "loadsmith.batch",
        loadsmith.batch.seq = seq,
        loadsmith.batch.direction = direction,
        loadsmith.batch.rows = tracing::field::Empty,
        loadsmith.batch.bytes = tracing::field::Empty,
        error = tracing::field::Empty,
    )
}

/// The span of one object (file) handled: parsed, produced or delivered.
pub(crate) fn object_span(operation: &'static str, path: &str) -> tracing::Span {
    tracing::info_span!(
        "loadsmith.object",
        otel.name = operation,
        loadsmith.object.path = path,
        loadsmith.object.rows = tracing::field::Empty,
        error = tracing::field::Empty,
    )
}

/// Marks `span` failed with `error` (masked by the core before export).
pub(crate) fn fail(span: &tracing::Span, error: &anyhow::Error) {
    span.record("error", format!("{error:#}"));
}

/// Marks `span` failed when `result` is an error.
pub(crate) fn outcome<T>(span: &tracing::Span, result: &anyhow::Result<T>) {
    if let Err(e) = result {
        fail(span, e);
    }
}

/// The baseline metrics of one batch: `loadsmith.plugin.rows`,
/// `loadsmith.plugin.batches`, `loadsmith.plugin.bytes` and
/// `loadsmith.plugin.batch.duration`, by `direction` (`read`/`written`).
/// The Arrow size of a batch for its span and metrics; `0` (not computed)
/// with telemetry off.
pub(crate) fn batch_bytes(batch: &arrow_array::RecordBatch) -> u64 {
    if enabled() { loadsmith_arrow::batch_bytes(batch) } else { 0 }
}

pub(crate) fn batch_done(span: &tracing::Span, direction: &'static str, rows: u64, bytes: u64, took: Duration) {
    if !enabled() {
        return;
    }
    span.record("loadsmith.batch.rows", rows);
    span.record("loadsmith.batch.bytes", bytes);
    let attrs = [kv("direction", direction)];
    counter("loadsmith.plugin.rows").with_unit("{row}").add(rows, &attrs);
    counter("loadsmith.plugin.batches").with_unit("{batch}").add(1u64, &attrs);
    counter("loadsmith.plugin.bytes").with_unit("By").add(bytes, &attrs);
    histogram("loadsmith.plugin.batch.duration")
        .with_unit("s")
        .with_description("Time the plugin spent producing or writing one batch")
        .record(took, &attrs);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Index of a level, `ERROR` = 0 … `TRACE` = 4 (more verbose is higher).
fn level_index(level: &Level) -> u8 {
    match *level {
        Level::ERROR => 0,
        Level::WARN => 1,
        Level::INFO => 2,
        Level::DEBUG => 3,
        Level::TRACE => 4,
    }
}

fn to_tracing_level(level: &LogLevel) -> Level {
    match level {
        LogLevel::Trace => Level::TRACE,
        LogLevel::Debug => Level::DEBUG,
        LogLevel::Info => Level::INFO,
        LogLevel::Warn => Level::WARN,
        LogLevel::Error => Level::ERROR,
    }
}

fn to_log_level(level: &Level) -> LogLevel {
    match *level {
        Level::TRACE => LogLevel::Trace,
        Level::DEBUG => LogLevel::Debug,
        Level::INFO => LogLevel::Info,
        Level::WARN => LogLevel::Warn,
        Level::ERROR => LogLevel::Error,
    }
}

pub(crate) fn unix_nanos() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

fn random_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
    h.write_u64(unix_nanos());
    h.write_u32(std::process::id());
    h.finish()
}

fn random_span_id() -> [u8; 8] {
    loop {
        let id = random_u64().to_be_bytes();
        if id != [0; 8] {
            return id;
        }
    }
}

fn random_trace_id() -> [u8; 16] {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&random_span_id());
    id[8..].copy_from_slice(&random_span_id());
    id
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use tracing_subscriber::layer::SubscriberExt;

    const TP: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    fn enabled_recorder(log_level: LogLevel) -> &'static Recorder {
        let rec: &'static Recorder = Box::leak(Box::new(Recorder::new()));
        rec.start(Some(&TelemetryContext {
            enabled: true,
            traceparent: Some(TP.into()),
            tracestate: None,
            log_level,
            metrics_interval_ms: None,
        }));
        rec
    }

    fn spans(msgs: &[Message]) -> Vec<&SpanData> {
        msgs.iter().filter_map(|m| if let Message::Span(s) = m { Some(&**s) } else { None }).collect()
    }

    fn logs(msgs: &[Message]) -> Vec<&LogEvent> {
        msgs.iter().filter_map(|m| if let Message::Log(l) = m { Some(l) } else { None }).collect()
    }

    #[test]
    fn closed_spans_become_span_messages_under_the_core_span() {
        let rec = enabled_recorder(LogLevel::Info);
        let subscriber = tracing_subscriber::registry().with(layer_for(rec));
        tracing::subscriber::with_default(subscriber, || {
            let outer = tracing::info_span!(
                "query",
                otel.kind = "client",
                db.namespace = "public",
                db.response.returned_rows = tracing::field::Empty
            );
            outer.in_scope(|| {
                tracing::info_span!("batch", otel.name = "loadsmith.batch", rows = 10u64).in_scope(|| {
                    tracing::info!(rows = 10u64, "wrote a batch");
                });
                tracing::debug_span!("too.verbose").in_scope(|| {});
                tracing::error!("boom");
            });
            outer.record("db.response.returned_rows", 3u64);
            drop(outer);
        });
        let msgs = rec.take_pending();
        let spans = spans(&msgs);
        assert_eq!(spans.len(), 2, "{msgs:?}");
        let (inner, outer) = (spans[0], spans[1]);
        assert_eq!(inner.name, "loadsmith.batch");
        assert_eq!(inner.parent_span_id.as_deref(), Some(outer.span_id.as_str()));
        assert_eq!(outer.parent_span_id.as_deref(), Some("b7ad6b7169203331"));
        assert_eq!(outer.trace_id, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(outer.kind, SpanKind::Client);
        assert_eq!(outer.attributes["db.namespace"], "public");
        assert_eq!(outer.attributes["db.response.returned_rows"], 3);
        assert_eq!(outer.status.code, StatusCode::Error, "an error! event fails its span");
        assert_eq!(inner.events[0].name, "wrote a batch");
        assert!(inner.start_unix_nano <= inner.end_unix_nano);

        let logs = logs(&msgs);
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].message, "wrote a batch");
        assert_eq!(logs[0].span_id.as_deref(), Some(inner.span_id.as_str()));
        assert_eq!(logs[0].attributes["rows"], 10);
        assert_eq!(logs[1].level, LogLevel::Error);
        assert_eq!(logs[1].trace_id.as_deref(), Some("0af7651916cd43dd8448eb211c80319c"));
    }

    #[test]
    fn error_field_sets_the_status_and_log_level_filters_events() {
        let rec = enabled_recorder(LogLevel::Warn);
        let subscriber = tracing_subscriber::registry().with(layer_for(rec));
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("op", error = tracing::field::Empty);
            span.in_scope(|| {
                tracing::info!("not exported: below warn");
                tracing::warn!("exported");
            });
            span.record("error", "it broke");
        });
        let msgs = rec.take_pending();
        let span = spans(&msgs)[0];
        assert_eq!(span.status, SpanStatus { code: StatusCode::Error, message: Some("it broke".into()) });
        assert_eq!(logs(&msgs).iter().map(|l| l.message.as_str()).collect::<Vec<_>>(), ["exported"]);
    }

    #[test]
    fn nothing_is_recorded_while_disabled() {
        let rec: &'static Recorder = Box::leak(Box::new(Recorder::new()));
        let subscriber = tracing_subscriber::registry().with(layer_for(rec));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info_span!("op").in_scope(|| tracing::error!("x"));
        });
        assert!(rec.take_pending().is_empty());
        assert!(rec.take_metrics(true).is_none());
        // A configure without telemetry (or disabled) leaves it off.
        rec.start(Some(&TelemetryContext::disabled()));
        rec.start(None);
        assert!(!rec.is_enabled());
    }

    #[test]
    fn unsampled_traces_record_no_spans_but_keep_logs() {
        let rec: &'static Recorder = Box::leak(Box::new(Recorder::new()));
        rec.start(Some(&TelemetryContext {
            enabled: true,
            traceparent: Some(TP.replace("-01", "-00")),
            ..TelemetryContext::disabled()
        }));
        let subscriber = tracing_subscriber::registry().with(layer_for(rec));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info_span!("op").in_scope(|| tracing::info!("hello"));
        });
        let msgs = rec.take_pending();
        assert!(spans(&msgs).is_empty());
        assert_eq!(logs(&msgs)[0].span_id.as_deref(), Some("b7ad6b7169203331"));
    }

    #[test]
    fn metrics_aggregate_as_deltas() {
        let rec = enabled_recorder(LogLevel::Info);
        let rows = counter("rows").with_unit("{row}");
        let dur = histogram("dur").with_unit("s");
        let g = gauge("lag");
        for (i, v) in [0.004, 0.2, 30.0].into_iter().enumerate() {
            rec.record_metric(&rows, 10.0, &[kv("direction", "read")]);
            rec.record_metric(&dur, v, &[]);
            rec.record_metric(&g, i as f64, &[]);
        }
        rec.record_metric(&rows, 1.0, &[kv("direction", "written")]);
        let Some(Message::Metrics(m)) = rec.take_metrics(true) else { panic!("no metrics") };
        let find = |name: &str, dir: Option<&str>| {
            m.points
                .iter()
                .find(|p| p.name == name && dir.is_none_or(|d| p.attributes["direction"] == d))
                .unwrap()
                .clone()
        };
        assert_eq!(find("rows", Some("read")).value, Some(30.0));
        assert_eq!(find("rows", Some("written")).value, Some(1.0));
        assert_eq!(find("lag", None).value, Some(2.0));
        let h = find("dur", None).histogram.unwrap();
        assert_eq!((h.count, h.min, h.max), (3, 0.004, 30.0));
        assert_eq!(h.explicit_bounds, DURATION_BOUNDS);
        assert_eq!(h.bucket_counts.iter().sum::<u64>(), 3);
        assert_eq!(h.bucket_counts[0], 1);
        assert_eq!(*h.bucket_counts.last().unwrap(), 1);
        // Delta: the next flush starts from zero.
        assert!(rec.take_metrics(true).is_none());
        // Not forced and not due yet: nothing.
        rec.record_metric(&rows, 1.0, &[]);
        assert!(rec.take_metrics(false).is_none());
    }

    #[test]
    fn the_span_budget_drops_and_counts() {
        let rec = enabled_recorder(LogLevel::Error);
        let subscriber = tracing_subscriber::registry().with(layer_for(rec));
        tracing::subscriber::with_default(subscriber, || {
            let parent = tracing::info_span!("parent");
            let _g = parent.enter();
            for _ in 0..(MAX_SPANS_PER_SECOND + 50) {
                tracing::info_span!("child").in_scope(|| {});
            }
        });
        let sent = spans(&rec.take_pending()).len() as u64;
        let Some(Message::Metrics(m)) = rec.take_metrics(true) else { panic!() };
        let dropped = m.points.iter().find(|p| p.name == DROPPED_METRIC).unwrap().value.unwrap() as u64;
        // A second boundary may refill the budget mid-loop: count, don't pin.
        assert_eq!(sent + dropped, u64::from(MAX_SPANS_PER_SECOND) + 51);
        assert!(dropped > 0);
    }
}
