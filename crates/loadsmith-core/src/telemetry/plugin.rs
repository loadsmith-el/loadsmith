//! A plugin process in the run's trace: its span (`plugin <name>`), the
//! context it is told in `configure`, and the re-emission of what it sends
//! on fd4 (spans, metrics, logs) through the core's pipelines.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use loadsmith_protocol::{
    decode_hex, Attributes, HistogramValue, LogEvent, LogLevel, MetricKind, MetricPoint, MetricsBatch, SpanData,
    SpanKind, StatusCode, TelemetryContext, Temporality, TraceParent,
};
use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _};
use opentelemetry::metrics::{Counter, Gauge, Histogram, UpDownCounter};
use opentelemetry::trace::{
    Link, Span as _, SpanBuilder, SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState, Tracer as _,
};
use opentelemetry::{Array, KeyValue, StringValue, Value};

use super::{export, get, layer, metrics, traces_enabled};

/// The telemetry identity of one plugin process. Cheap to clone: the peer
/// and its event drain share it; the span ends when both let go.
#[derive(Clone)]
pub struct PluginScope(Arc<Inner>);

struct Inner {
    kind: &'static str,
    span: tracing::Span,
    identity: Mutex<Option<(String, String)>>,
}

impl PluginScope {
    /// The span of a plugin process of `kind` (`label` when the core names
    /// the instance, e.g. `provider 'vault'`), child of the current span.
    pub fn new(kind: &'static str, label: &str, pid: Option<u32>) -> Self {
        let span = tracing::info_span!(
            target: "loadsmith::plugin",
            "plugin",
            otel.name = %format!("plugin {label}"),
            loadsmith.plugin.kind = kind,
            loadsmith.plugin.name = tracing::field::Empty,
            loadsmith.plugin.version = tracing::field::Empty,
            loadsmith.plugin.respawns = tracing::field::Empty,
            process.pid = pid.map(i64::from).unwrap_or(-1),
            otel.status_code = tracing::field::Empty,
            error = tracing::field::Empty,
        );
        Self(Arc::new(Inner { kind, span, identity: Mutex::new(None) }))
    }

    /// A scope that records nothing (a peer outside any run).
    pub fn none(kind: &'static str) -> Self {
        Self(Arc::new(Inner { kind, span: tracing::Span::none(), identity: Mutex::new(None) }))
    }

    pub fn span(&self) -> &tracing::Span {
        &self.0.span
    }

    pub fn kind(&self) -> &'static str {
        self.0.kind
    }

    /// The plugin introduced itself (`handshake_ack`).
    pub fn identify(&self, name: &str, version: &str) {
        // `otel.name` only names a span not started yet; this one may be
        // (its children exist by now), so rename the OTel span itself.
        if traces_enabled() {
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            self.0.span.context().span().update_name(format!("plugin {name}"));
        }
        self.0.span.record("loadsmith.plugin.name", name);
        self.0.span.record("loadsmith.plugin.version", version);
        *self.0.identity.lock().unwrap_or_else(|e| e.into_inner()) = Some((name.to_string(), version.to_string()));
    }

    /// How many times this plugin was respawned before this process.
    pub fn respawned(&self, count: u32) {
        self.0.span.record("loadsmith.plugin.respawns", i64::from(count));
    }

    /// Marks the plugin's span failed (the text is masked on export).
    pub fn failed(&self, reason: &str) {
        self.0.span.record("error", reason);
    }

    /// A span event on the plugin's span (`cancel`, `resend`, `respawn`, …).
    /// Not a log line: stderr is unchanged.
    pub fn event(&self, name: &'static str, attributes: Vec<KeyValue>) {
        super::span_event(&self.0.span, name, attributes);
    }

    /// `configure.telemetry` for this plugin, when telemetry is on: the W3C
    /// context of its span, so its spans hang under it.
    pub fn configure_context(&self) -> Option<TelemetryContext> {
        let t = get()?;
        let traceparent = self.span_context().map(|sc| {
            TraceParent {
                trace_id: sc.trace_id().to_bytes(),
                span_id: sc.span_id().to_bytes(),
                sampled: sc.is_sampled(),
            }
            .to_string()
        });
        Some(TelemetryContext {
            enabled: true,
            traceparent,
            tracestate: None,
            log_level: t.settings.log_level.clone(),
            metrics_interval_ms: Some(t.settings.metrics_interval.as_millis() as u64),
        })
    }

    fn span_context(&self) -> Option<SpanContext> {
        if !traces_enabled() {
            return None;
        }
        let sc = self.0.span.in_scope(|| opentelemetry::Context::current().span().span_context().clone());
        sc.is_valid().then_some(sc)
    }

    /// `loadsmith.plugin.kind`, `.name`, `.version`: on every re-emitted
    /// metric and log record of this plugin.
    fn attributes(&self) -> Vec<KeyValue> {
        let mut attrs = vec![KeyValue::new("loadsmith.plugin.kind", self.0.kind)];
        if let Some((name, version)) = self.0.identity.lock().unwrap_or_else(|e| e.into_inner()).clone() {
            attrs.push(KeyValue::new("loadsmith.plugin.name", name));
            attrs.push(KeyValue::new("loadsmith.plugin.version", version));
        }
        attrs
    }

    // ── Re-emission ──────────────────────────────────────────────────────

    /// A plugin's finished span, under the run's trace with its own ids.
    pub fn forward_span(&self, span: Box<SpanData>) {
        let Some(t) = get() else { return };
        let Some(tracer) = &t.plugin_tracer else { return };
        let (Some(trace_id), Some(span_id)) = (trace_id_of(&span.trace_id), parse_span_id(&span.span_id)) else {
            tracing::debug!(plugin = self.0.kind, "dropping a span with malformed ids");
            return;
        };
        let own = self.span_context();
        let parent_id = span.parent_span_id.as_deref().and_then(parse_span_id).or_else(|| own.as_ref().map(|sc| sc.span_id()));
        let sampled = own.as_ref().is_none_or(|sc| sc.is_sampled());
        let flags = if sampled { TraceFlags::SAMPLED } else { TraceFlags::default() };
        let parent_cx = match parent_id {
            Some(pid) => opentelemetry::Context::new().with_remote_span_context(SpanContext::new(
                trace_id,
                pid,
                flags,
                true,
                TraceState::default(),
            )),
            None => opentelemetry::Context::new(),
        };
        let mut attributes = attributes(&span.attributes);
        export::mask_attributes(&mut attributes, &t.mask);
        attributes.extend(self.attributes());
        let links = span
            .links
            .iter()
            .filter_map(|l| {
                let sc = SpanContext::new(
                    trace_id_of(&l.trace_id)?,
                    parse_span_id(&l.span_id)?,
                    TraceFlags::default(),
                    true,
                    TraceState::default(),
                );
                Some(Link::with_context(sc))
            })
            .collect();
        let builder = SpanBuilder::from_name(t.mask.apply(&span.name))
            .with_kind(kind(span.kind))
            .with_start_time(time(span.start_unix_nano))
            .with_attributes(attributes)
            .with_links(links);
        let mut otel = export::with_span_id(span_id, || tracer.build_with_context(builder, &parent_cx));
        for event in &span.events {
            let mut attrs = self::attributes(&event.attributes);
            export::mask_attributes(&mut attrs, &t.mask);
            otel.add_event_with_timestamp(t.mask.apply(&event.name), time(event.time_unix_nano), attrs);
        }
        match span.status.code {
            StatusCode::Ok => otel.set_status(opentelemetry::trace::Status::Ok),
            StatusCode::Error => otel.set_status(opentelemetry::trace::Status::error(
                t.mask.apply(span.status.message.as_deref().unwrap_or_default()),
            )),
            StatusCode::Unset => {}
        }
        otel.end_with_timestamp(time(span.end_unix_nano.max(span.start_unix_nano)));
    }

    /// A plugin's metric points, through the plugin meter (with the
    /// plugin's identity as attributes).
    pub fn forward_metrics(&self, batch: MetricsBatch) {
        let Some(m) = metrics() else { return };
        let Some(t) = get() else { return };
        for point in batch.points {
            if point.temporality != Temporality::Delta || point.kind == MetricKind::Unknown {
                continue;
            }
            let mut attrs = attributes(&point.attributes);
            export::mask_attributes(&mut attrs, &t.mask);
            attrs.extend(self.attributes());
            let key = (point.name.clone(), point.kind as u8);
            let mut cache = m.plugin_instruments.lock().unwrap_or_else(|e| e.into_inner());
            let inst = cache.entry(key).or_insert_with(|| Instrument::new(&m.plugin_meter, &point));
            inst.record(&point, &attrs);
        }
    }

    /// A plugin's log line as an OTLP log record, with its trace context
    /// (the plugin's, or else this plugin's span). `log.message` is already
    /// masked by the caller only for stderr: it is masked again here.
    pub fn forward_log(&self, log: &LogEvent) {
        let Some(t) = get() else { return };
        let Some(logger) = &t.plugin_logger else { return };
        if layer::level_index_of(&log.level) > layer::level_index_of(&t.settings.log_level) {
            return;
        }
        let mut record = logger.create_log_record();
        let now = SystemTime::now();
        record.set_timestamp(log.time_unix_nano.map(time).unwrap_or(now));
        record.set_observed_timestamp(now);
        let (severity, text) = layer::severity(&tracing_level(&log.level));
        record.set_severity_number(severity);
        record.set_severity_text(text);
        record.set_target(format!("loadsmith.plugin.{}", self.0.kind));
        record.set_body(AnyValue::String(t.mask.apply(&log.message).into()));
        let mut attrs = attributes(&log.attributes);
        export::mask_attributes(&mut attrs, &t.mask);
        attrs.extend(self.attributes());
        for kv in attrs {
            record.add_attribute(kv.key, any_value(kv.value));
        }
        if traces_enabled() {
            let ids = log.trace_id.as_deref().and_then(trace_id_of).zip(log.span_id.as_deref().and_then(parse_span_id));
            let ids = ids.or_else(|| self.span_context().map(|sc| (sc.trace_id(), sc.span_id())));
            if let Some((tid, sid)) = ids {
                record.set_trace_context(tid, sid, Some(TraceFlags::SAMPLED));
            }
        }
        logger.emit(record);
    }
}

// ── Plugin metric instruments ─────────────────────────────────────────────────

/// At most this many measurements re-recorded per histogram point.
const MAX_REPLAY: u64 = 100_000;

pub(super) enum Instrument {
    Counter(Counter<f64>),
    UpDown(UpDownCounter<f64>),
    Gauge(Gauge<f64>),
    Histogram(Histogram<f64>),
}

impl Instrument {
    fn new(meter: &opentelemetry::metrics::Meter, p: &MetricPoint) -> Self {
        let name = p.name.clone();
        let unit = p.unit.clone().unwrap_or_default();
        let description = p.description.clone().unwrap_or_default();
        match p.kind {
            MetricKind::Counter | MetricKind::Unknown => {
                Instrument::Counter(meter.f64_counter(name).with_unit(unit).with_description(description).build())
            }
            MetricKind::UpDownCounter => Instrument::UpDown(
                meter.f64_up_down_counter(name).with_unit(unit).with_description(description).build(),
            ),
            MetricKind::Gauge => {
                Instrument::Gauge(meter.f64_gauge(name).with_unit(unit).with_description(description).build())
            }
            MetricKind::Histogram => {
                let mut b = meter.f64_histogram(name).with_unit(unit).with_description(description);
                if let Some(h) = &p.histogram {
                    b = b.with_boundaries(h.explicit_bounds.clone());
                }
                Instrument::Histogram(b.build())
            }
        }
    }

    fn record(&self, p: &MetricPoint, attrs: &[KeyValue]) {
        match (self, p.value, &p.histogram) {
            (Instrument::Counter(c), Some(v), _) if v >= 0.0 && v.is_finite() => c.add(v, attrs),
            (Instrument::UpDown(c), Some(v), _) if v.is_finite() => c.add(v, attrs),
            (Instrument::Gauge(g), Some(v), _) if v.is_finite() => g.record(v, attrs),
            (Instrument::Histogram(h), _, Some(hv)) => replay(hv, |v| h.record(v, attrs)),
            _ => {}
        }
    }
}

/// Re-records an aggregated histogram point as measurements: the exact
/// count, min, max and bucket counts; the sum exactly when the values fit
/// their buckets (otherwise as close as the buckets allow).
pub(super) fn replay(h: &HistogramValue, mut record: impl FnMut(f64)) {
    if h.count == 0 || !h.min.is_finite() || !h.max.is_finite() {
        return;
    }
    let bounds = &h.explicit_bounds;
    let mut counts: Vec<u64> = if h.bucket_counts.len() == bounds.len() + 1 {
        h.bucket_counts.clone()
    } else {
        // Malformed buckets: one bucket holding everything.
        let mut c = vec![0; bounds.len() + 1];
        c[bucket_of(bounds, h.max)] = h.count;
        c
    };
    let total: u64 = counts.iter().sum::<u64>().max(1);
    if total > MAX_REPLAY {
        for c in counts.iter_mut() {
            *c = (*c as u128 * MAX_REPLAY as u128 / total as u128) as u64;
        }
    }
    let take = |counts: &mut Vec<u64>, v: f64| {
        let i = bucket_of(bounds, v);
        if counts[i] > 0 {
            counts[i] -= 1;
            true
        } else {
            false
        }
    };
    let mut rest_sum = h.sum;
    if take(&mut counts, h.min) {
        record(h.min);
        rest_sum -= h.min;
    }
    if counts.iter().sum::<u64>() > 0 && take(&mut counts, h.max) {
        record(h.max);
        rest_sum -= h.max;
    }
    let rest: u64 = counts.iter().sum();
    if rest == 0 {
        return;
    }
    let mean = rest_sum / rest as f64;
    for (i, c) in counts.iter().enumerate() {
        let lo = if i == 0 { h.min } else { bounds[i - 1].max(h.min) };
        let hi = if i == bounds.len() { h.max } else { bounds[i].min(h.max) };
        let v = if mean > lo && mean <= hi {
            mean
        } else if mean <= lo {
            // Just above the lower bound (buckets are `(lo, hi]`).
            if i == 0 { lo } else { next_up(lo).min(hi) }
        } else {
            hi
        };
        for _ in 0..*c {
            record(v);
        }
    }
}

fn bucket_of(bounds: &[f64], v: f64) -> usize {
    bounds.iter().position(|b| v <= *b).unwrap_or(bounds.len())
}

fn next_up(v: f64) -> f64 {
    if v.is_finite() {
        let bits = v.to_bits();
        f64::from_bits(if v >= 0.0 { bits + 1 } else { bits - 1 })
    } else {
        v
    }
}

// ── Conversions ───────────────────────────────────────────────────────────────

fn trace_id_of(hex: &str) -> Option<TraceId> {
    let bytes: [u8; 16] = decode_hex(hex)?.try_into().ok()?;
    (bytes != [0; 16]).then(|| TraceId::from_bytes(bytes))
}


fn parse_span_id(hex: &str) -> Option<SpanId> {
    let bytes: [u8; 8] = decode_hex(hex)?.try_into().ok()?;
    (bytes != [0; 8]).then(|| SpanId::from_bytes(bytes))
}

fn time(unix_nano: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(unix_nano)
}

fn kind(k: SpanKind) -> opentelemetry::trace::SpanKind {
    match k {
        SpanKind::Client => opentelemetry::trace::SpanKind::Client,
        SpanKind::Server => opentelemetry::trace::SpanKind::Server,
        SpanKind::Producer => opentelemetry::trace::SpanKind::Producer,
        SpanKind::Consumer => opentelemetry::trace::SpanKind::Consumer,
        SpanKind::Internal => opentelemetry::trace::SpanKind::Internal,
    }
}

fn tracing_level(level: &LogLevel) -> tracing::Level {
    match level {
        LogLevel::Trace => tracing::Level::TRACE,
        LogLevel::Debug => tracing::Level::DEBUG,
        LogLevel::Info => tracing::Level::INFO,
        LogLevel::Warn => tracing::Level::WARN,
        LogLevel::Error => tracing::Level::ERROR,
    }
}

/// JSON attributes → OTel ones; values OTel cannot carry are dropped.
pub(super) fn attributes(attrs: &Attributes) -> Vec<KeyValue> {
    attrs.iter().filter_map(|(k, v)| Some(KeyValue::new(k.clone(), value(v)?))).collect()
}

fn value(v: &serde_json::Value) -> Option<Value> {
    use serde_json::Value as J;
    Some(match v {
        J::String(s) => Value::String(s.clone().into()),
        J::Bool(b) => Value::Bool(*b),
        J::Number(n) => match n.as_i64() {
            Some(i) => Value::I64(i),
            None => Value::F64(n.as_f64()?),
        },
        J::Array(items) => {
            if items.iter().all(|i| i.is_string()) {
                Value::Array(Array::String(
                    items.iter().filter_map(|i| i.as_str().map(|s| StringValue::from(s.to_string()))).collect(),
                ))
            } else if items.iter().all(|i| i.is_boolean()) {
                Value::Array(Array::Bool(items.iter().filter_map(|i| i.as_bool()).collect()))
            } else if items.iter().all(|i| i.is_i64()) {
                Value::Array(Array::I64(items.iter().filter_map(|i| i.as_i64()).collect()))
            } else if items.iter().all(|i| i.is_number()) {
                Value::Array(Array::F64(items.iter().filter_map(|i| i.as_f64()).collect()))
            } else {
                return None;
            }
        }
        J::Null | J::Object(_) => return None,
    })
}

fn any_value(v: Value) -> AnyValue {
    match v {
        Value::Bool(b) => AnyValue::Boolean(b),
        Value::I64(i) => AnyValue::Int(i),
        Value::F64(f) => AnyValue::Double(f),
        Value::String(s) => AnyValue::String(s),
        other => AnyValue::String(other.to_string().into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replayed(h: &HistogramValue) -> Vec<f64> {
        let mut out = Vec::new();
        replay(h, |v| out.push(v));
        out
    }

    #[test]
    fn histogram_replay_keeps_count_min_max_and_buckets() {
        let h = HistogramValue {
            count: 5,
            sum: 0.5 + 0.7 + 0.9 + 3.0 + 40.0,
            min: 0.5,
            max: 40.0,
            bucket_counts: vec![3, 1, 1],
            explicit_bounds: vec![1.0, 10.0],
        };
        let v = replayed(&h);
        assert_eq!(v.len(), 5);
        assert_eq!(v.iter().cloned().fold(f64::INFINITY, f64::min), 0.5);
        assert_eq!(v.iter().cloned().fold(f64::NEG_INFINITY, f64::max), 40.0);
        let mut buckets = [0u64; 3];
        for x in &v {
            buckets[bucket_of(&h.explicit_bounds, *x)] += 1;
        }
        assert_eq!(buckets, [3, 1, 1]);
        // One bucket: the sum is exact.
        let one = HistogramValue {
            count: 4,
            sum: 10.0,
            min: 1.0,
            max: 4.0,
            bucket_counts: vec![4],
            explicit_bounds: vec![],
        };
        assert!((replayed(&one).iter().sum::<f64>() - 10.0).abs() < 1e-9);
        assert!(replayed(&HistogramValue { count: 0, ..one }).is_empty());
    }

    #[test]
    fn json_attributes_convert_and_odd_values_drop() {
        let attrs: Attributes = serde_json::from_str(
            r#"{"s":"x","b":true,"i":-3,"f":1.5,"u":18446744073709551615,"as":["a","b"],"mixed":[1,"a"],"n":null,"o":{}}"#,
        )
        .unwrap();
        let kv = attributes(&attrs);
        let keys: Vec<&str> = kv.iter().map(|k| k.key.as_str()).collect();
        assert_eq!(keys, ["as", "b", "f", "i", "s", "u"]);
    }

    #[test]
    fn ids_parse_and_reject_zero() {
        assert!(trace_id_of("0af7651916cd43dd8448eb211c80319c").is_some());
        assert!(trace_id_of("00000000000000000000000000000000").is_none());
        assert!(parse_span_id("b7ad6b7169203331").is_some());
        assert!(parse_span_id("b7ad").is_none());
    }
}
