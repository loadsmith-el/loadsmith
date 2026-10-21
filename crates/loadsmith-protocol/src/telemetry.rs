//! Telemetry on the event plane (fd4): what a plugin needs to take part in the
//! run's trace, and the `span` / `metrics` messages it sends back.
//!
//! The core is the only exporter. A plugin never opens a network connection
//! for telemetry: it records spans, metrics and logs in-process and writes
//! them to fd4; the core re-emits them, under the run's trace, through its
//! OTLP pipeline. Everything here is additive to protocol v1 and tolerant:
//!
//! - a plugin that does not advertise `handshake_ack.telemetry` never gets a
//!   `configure.telemetry`, and sends none of these messages;
//! - a core that predates them skips `span` / `metrics` as unknown types and
//!   ignores the new optional fields;
//! - unknown enum values inside them read as a safe default (see each type).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::LogLevel;

/// The telemetry protocol revision a plugin advertises in
/// `handshake_ack.telemetry`. The core sends `configure.telemetry` only to a
/// plugin that advertises at least this.
pub const TELEMETRY_VERSION: u32 = 1;

/// Span, event, link and metric attributes: OTel attribute values as JSON —
/// a string, a boolean, an integer, a float, or an array of one of those.
/// Anything else (null, objects, mixed arrays) is dropped by the core.
pub type Attributes = BTreeMap<String, serde_json::Value>;

// ── configure.telemetry ───────────────────────────────────────────────────────

/// `configure.telemetry`: whether the plugin records telemetry for this run
/// and where its spans hang in the run's trace. Absent (or `enabled: false`)
/// means the plugin records nothing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetryContext {
    pub enabled: bool,
    /// W3C `traceparent` of the plugin's parent span (the core's per-plugin
    /// span): `00-<32 hex trace id>-<16 hex span id>-<2 hex flags>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// W3C `tracestate` that goes with `traceparent`, verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,
    /// Minimum level of the log events the plugin sends with trace context.
    #[serde(default = "default_log_level")]
    pub log_level: LogLevel,
    /// How often the plugin flushes its aggregated metrics (milliseconds).
    /// Absent: the plugin's default (15 s). A final flush always happens at
    /// the end of the plugin's run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_interval_ms: Option<u64>,
}

fn is_default<T: Default + PartialEq>(v: &T) -> bool {
    *v == T::default()
}

fn default_log_level() -> LogLevel {
    LogLevel::Info
}

impl TelemetryContext {
    /// Telemetry off.
    pub fn disabled() -> Self {
        Self { enabled: false, traceparent: None, tracestate: None, log_level: LogLevel::Info, metrics_interval_ms: None }
    }
}

/// A parsed W3C `traceparent` (version `00`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceParent {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub sampled: bool,
}

impl TraceParent {
    /// Parses `00-<trace id>-<span id>-<flags>`. All-zero ids, other versions'
    /// extra fields aside, are rejected as the W3C spec requires.
    pub fn parse(s: &str) -> Option<Self> {
        let mut parts = s.trim().split('-');
        let version = parts.next()?;
        let (trace, span, flags) = (parts.next()?, parts.next()?, parts.next()?);
        if version.len() != 2 || version == "ff" || (version == "00" && parts.next().is_some()) {
            return None;
        }
        let trace_id: [u8; 16] = decode_hex(trace)?.try_into().ok()?;
        let span_id: [u8; 8] = decode_hex(span)?.try_into().ok()?;
        let flags = decode_hex(flags)?;
        if flags.len() != 1 || trace_id == [0; 16] || span_id == [0; 8] {
            return None;
        }
        Some(Self { trace_id, span_id, sampled: flags[0] & 1 == 1 })
    }

    pub fn trace_id_hex(&self) -> String {
        encode_hex(&self.trace_id)
    }

    pub fn span_id_hex(&self) -> String {
        encode_hex(&self.span_id)
    }
}

impl std::fmt::Display for TraceParent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "00-{}-{}-{:02x}", self.trace_id_hex(), self.span_id_hex(), u8::from(self.sampled))
    }
}

/// Lower-case hex of `bytes` (the spelling of trace and span ids on the wire).
pub fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[(b >> 4) as usize] as char);
        s.push(DIGITS[(b & 0xf) as usize] as char);
    }
    s
}

/// Bytes of a hex string (either case); `None` if it is not hex.
pub fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok()).collect()
}

// ── span ──────────────────────────────────────────────────────────────────────

/// Body of `span` (plugin → core, fd4): one finished span. Sent when the span
/// closes; ids are lower-case hex (`trace_id` 32 digits, span ids 16).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpanData {
    pub trace_id: String,
    pub span_id: String,
    /// The parent span; for a plugin's top-level spans, the span of
    /// `configure.telemetry.traceparent`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_span_id: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "is_default")]
    pub kind: SpanKind,
    pub start_unix_nano: u64,
    pub end_unix_nano: u64,
    #[serde(default, skip_serializing_if = "is_default")]
    pub status: SpanStatus,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: Attributes,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<SpanEvent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<SpanLink>,
}

/// OTel span kind. An unknown kind reads as `internal`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanKind {
    Client,
    Server,
    Producer,
    Consumer,
    #[default]
    #[serde(other)]
    Internal,
}

/// A span's status. `message` only goes with `code: error`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanStatus {
    #[serde(default)]
    pub code: StatusCode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// OTel status code. An unknown code reads as `unset`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatusCode {
    Ok,
    Error,
    #[default]
    #[serde(other)]
    Unset,
}

/// A timestamped event inside a span.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpanEvent {
    pub name: String,
    pub time_unix_nano: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: Attributes,
}

/// A link to another span (in this trace or another).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpanLink {
    pub trace_id: String,
    pub span_id: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: Attributes,
}

// ── metrics ───────────────────────────────────────────────────────────────────

/// Body of `metrics` (plugin → core, fd4): data points aggregated in-process
/// since the previous `metrics` message (delta temporality).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricsBatch {
    pub points: Vec<MetricPoint>,
}

/// One aggregated data point: an instrument (`name`, `kind`) and one set of
/// `attributes`. Counters, up-down counters and gauges carry `value`;
/// histograms carry `histogram`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricPoint {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub kind: MetricKind,
    #[serde(default)]
    pub temporality: Temporality,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: Attributes,
    /// Counter / up-down counter: the sum of the increments in the interval.
    /// Gauge: the last value recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub histogram: Option<HistogramValue>,
}

/// The instrument kind of a [`MetricPoint`]. An unknown kind reads as
/// `unknown`, and the core skips the point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    Counter,
    UpDownCounter,
    Histogram,
    Gauge,
    #[serde(other)]
    Unknown,
}

/// Aggregation temporality. Only `delta` is defined; any other value reads
/// as `unknown`, and the core skips the point.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Temporality {
    #[default]
    Delta,
    #[serde(other)]
    Unknown,
}

/// An explicit-bucket histogram: `bucket_counts` has one more entry than
/// `explicit_bounds` (the last bucket is everything above the last bound);
/// bucket `i` holds values `v` with `bounds[i-1] < v <= bounds[i]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistogramValue {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub bucket_counts: Vec<u64>,
    pub explicit_bounds: Vec<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Message;

    #[test]
    fn traceparent_roundtrip_and_rejects() {
        let tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let parsed = TraceParent::parse(tp).unwrap();
        assert!(parsed.sampled);
        assert_eq!(parsed.to_string(), tp);
        assert_eq!(parsed.trace_id_hex(), "0af7651916cd43dd8448eb211c80319c");
        for bad in [
            "",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331",
            "00-00000000000000000000000000000000-b7ad6b7169203331-01",
            "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-extra",
            "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            "00-zzf7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        ] {
            assert!(TraceParent::parse(bad).is_none(), "{bad}");
        }
        assert!(!TraceParent::parse("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00").unwrap().sampled);
    }

    #[test]
    fn span_roundtrip_and_minimal_form() {
        let line = r#"{"type":"span","trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"b7ad6b7169203331","name":"q","start_unix_nano":1,"end_unix_nano":2}"#;
        let Message::Span(s) = serde_json::from_str::<Message>(line).unwrap() else { panic!("wrong variant") };
        assert_eq!((s.kind, s.status.code), (SpanKind::Internal, StatusCode::Unset));
        assert_eq!(serde_json::to_string(&Message::Span(s)).unwrap(), line);
    }

    #[test]
    fn span_unknown_enum_values_are_tolerated() {
        let line = r#"{"type":"span","trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"b7ad6b7169203331","name":"q","kind":"quantum","start_unix_nano":1,"end_unix_nano":2,"status":{"code":"maybe"},"future":1,"events":[{"name":"e","time_unix_nano":1,"attributes":{"a":[1,2]}}]}"#;
        let Message::Span(s) = serde_json::from_str::<Message>(line).unwrap() else { panic!("wrong variant") };
        assert_eq!((s.kind, s.status.code), (SpanKind::Internal, StatusCode::Unset));
        assert_eq!(s.events[0].attributes["a"], serde_json::json!([1, 2]));
    }

    #[test]
    fn metrics_roundtrip_and_tolerance() {
        let msg = Message::Metrics(MetricsBatch {
            points: vec![
                MetricPoint {
                    name: "loadsmith.plugin.rows".into(),
                    unit: Some("{row}".into()),
                    description: None,
                    kind: MetricKind::Counter,
                    temporality: Temporality::Delta,
                    attributes: [("direction".to_string(), serde_json::json!("read"))].into(),
                    value: Some(10.0),
                    histogram: None,
                },
                MetricPoint {
                    name: "d".into(),
                    unit: Some("s".into()),
                    description: None,
                    kind: MetricKind::Histogram,
                    temporality: Temporality::Delta,
                    attributes: Attributes::new(),
                    value: None,
                    histogram: Some(HistogramValue {
                        count: 2,
                        sum: 3.0,
                        min: 1.0,
                        max: 2.0,
                        bucket_counts: vec![1, 1, 0],
                        explicit_bounds: vec![1.0, 5.0],
                    }),
                },
            ],
        });
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.starts_with(r#"{"type":"metrics","points":[{"name":"loadsmith.plugin.rows""#), "{json}");
        let Message::Metrics(back) = serde_json::from_str::<Message>(&json).unwrap() else { panic!() };
        let Message::Metrics(orig) = msg else { unreachable!() };
        assert_eq!(back, orig);

        let odd = r#"{"type":"metrics","points":[{"name":"x","kind":"summary","temporality":"cumulative","value":1}]}"#;
        let Message::Metrics(m) = serde_json::from_str::<Message>(odd).unwrap() else { panic!() };
        assert_eq!((m.points[0].kind, m.points[0].temporality), (MetricKind::Unknown, Temporality::Unknown));
    }
}
