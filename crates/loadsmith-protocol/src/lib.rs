use serde::{Deserialize, Serialize};

// ── Top-level message enum ────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    // Handshake
    Handshake,
    HandshakeAck(HandshakeAck),
    SetProtocolVersion(SetProtocolVersion),

    // Capabilities
    CapabilitiesRequest,
    CapabilitiesResponse(CapabilitiesResponse),

    // Configuration
    Configure(Configure),
    ConfigureAck(ConfigureAck),

    // Execution
    Start(StartParams),
    Schema(Schema),
    Ready,
    Progress(Progress),

    // Incremental state (core ↔ plugin)
    /// Source → core (event plane, fd4): the high watermark of the ordered
    /// cursor column produced so far, tied to a batch ordinal. The core records
    /// it and persists it once the destination confirms the matching batch is
    /// durable.
    Checkpoint(Checkpoint),
    /// Destination → core (event plane, fd4): everything up to and including this
    /// batch ordinal is durably committed. Gates which watermark the core may
    /// persist.
    Committed(Committed),

    // Object delivery (file destination → core → sink)
    /// Destination → core (event plane, fd4): a staged file is finalized and
    /// ready for delivery. Carries the local path the sink should pick up.
    ObjectReady(ObjectReady),
    /// Core → sink (control plane): deliver the staged file at this path.
    DeliverObject(DeliverObject),
    /// Sink → core (control plane): the object at this path was delivered.
    /// Feeds the core's delivery ledger so a crashed sink can resume.
    ObjectDelivered(ObjectDelivered),

    // Observability
    Log(LogEvent),

    // Healthcheck
    Ping,
    Pong,

    // Control
    Cancel(Cancel),
    Error(ProtocolError),
    Finished(Finished),
}

// ── Handshake ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeAck {
    pub protocol_supported_versions: Vec<u32>,
    pub plugin_name: String,
    pub plugin_version: String,
    pub kind: PluginKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetProtocolVersion {
    pub protocol_version: u32,
}

// ── Capabilities ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilitiesResponse {
    pub supports: Vec<String>,
}

// ── Configuration ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Configure {
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigureAck {
    pub status: ConfigureStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfigureStatus {
    Ok,
    Error,
}

// ── Start ─────────────────────────────────────────────────────────────────────

/// Parameters carried by `start`. Empty by default — `{"type":"start"}` still
/// deserializes — so existing plugins are unaffected. For an incremental source
/// the core fills `resume` with the watermark persisted by the previous run.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StartParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume: Option<ResumeCursor>,
}

/// The opaque cursor value to resume from. The core never interprets it — it
/// stores the scalar and hands it back; the source decides how to use it (e.g.
/// `WHERE cursor > value`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeCursor {
    pub cursor_value: serde_json::Value,
}

// ── Incremental state ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Opaque high watermark of the cursor column produced so far.
    pub cursor_value: serde_json::Value,
    /// Batch ordinal this watermark corresponds to (1-based, global to the run).
    pub batch_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Committed {
    /// Highest batch ordinal durably committed at the destination.
    pub batch_seq: u64,
}

// ── Schema ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: FieldType,
}

/// Arrow-compatible type names used in the protocol.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Int32,
    Int64,
    Float32,
    Float64,
    Utf8,
    Bool,
    Date32,
    TimestampMs,
    Binary,
}

/// Matches the protocol's own `snake_case` wire representation — what the
/// human report shows is exactly what appears on the wire.
impl std::fmt::Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            FieldType::Int32 => "int32",
            FieldType::Int64 => "int64",
            FieldType::Float32 => "float32",
            FieldType::Float64 => "float64",
            FieldType::Utf8 => "utf8",
            FieldType::Bool => "bool",
            FieldType::Date32 => "date32",
            FieldType::TimestampMs => "timestamp_ms",
            FieldType::Binary => "binary",
        };
        write!(f, "{s}")
    }
}

// ── Progress ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Progress {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_read: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batches_read: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_written: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batches_written: Option<u64>,
}

// ── Object delivery ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectReady {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliverObject {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectDelivered {
    pub path: String,
}

// ── Log event ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEvent {
    pub level: LogLevel,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

// ── Cancel ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cancel {
    pub reason: String,
}

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolError {
    pub code: String,
    pub message: String,
}

// ── Finished ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Finished {
    pub status: FinishedStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_read: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batches_read: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_written: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batches_written: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FinishedStatus {
    Success,
    Error,
    Cancelled,
}

// ── Plugin kind ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PluginKind {
    Source,
    Destination,
    Sink,
    Parser,
    ConfigProvider,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

impl ConfigureAck {
    pub fn ok() -> Self {
        Self { status: ConfigureStatus::Ok, code: None, message: None }
    }

    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status: ConfigureStatus::Error,
            code: Some(code.into()),
            message: Some(message.into()),
        }
    }
}

impl Finished {
    pub fn success_source(rows: u64, batches: u64) -> Self {
        Self {
            status: FinishedStatus::Success,
            rows_read: Some(rows),
            batches_read: Some(batches),
            rows_written: None,
            batches_written: None,
            code: None,
            message: None,
        }
    }

    pub fn success_destination(rows: u64, batches: u64) -> Self {
        Self {
            status: FinishedStatus::Success,
            rows_read: None,
            batches_read: None,
            rows_written: Some(rows),
            batches_written: Some(batches),
            code: None,
            message: None,
        }
    }

    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status: FinishedStatus::Error,
            rows_read: None,
            batches_read: None,
            rows_written: None,
            batches_written: None,
            code: Some(code.into()),
            message: Some(message.into()),
        }
    }

    pub fn cancelled() -> Self {
        Self {
            status: FinishedStatus::Cancelled,
            rows_read: None,
            batches_read: None,
            rows_written: None,
            batches_written: None,
            code: None,
            message: None,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: &Message) -> Message {
        let json = serde_json::to_string(msg).expect("serialize");
        serde_json::from_str(&json).expect("deserialize")
    }

    #[test]
    fn handshake_roundtrip() {
        let msg = Message::Handshake;
        assert!(matches!(roundtrip(&msg), Message::Handshake));
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"handshake\""));
    }

    #[test]
    fn handshake_ack_roundtrip() {
        let msg = Message::HandshakeAck(HandshakeAck {
            protocol_supported_versions: vec![1],
            plugin_name: "loadsmith-source-postgres".into(),
            plugin_version: "0.1.0".into(),
            kind: PluginKind::Source,
        });
        if let Message::HandshakeAck(ack) = roundtrip(&msg) {
            assert_eq!(ack.protocol_supported_versions, vec![1]);
            assert_eq!(ack.kind, PluginKind::Source);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn set_protocol_version_roundtrip() {
        let msg = Message::SetProtocolVersion(SetProtocolVersion { protocol_version: 1 });
        if let Message::SetProtocolVersion(s) = roundtrip(&msg) {
            assert_eq!(s.protocol_version, 1);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn capabilities_request_roundtrip() {
        assert!(matches!(roundtrip(&Message::CapabilitiesRequest), Message::CapabilitiesRequest));
    }

    #[test]
    fn capabilities_response_roundtrip() {
        let msg = Message::CapabilitiesResponse(CapabilitiesResponse {
            supports: vec!["batch_read".into(), "schema_inference".into()],
        });
        if let Message::CapabilitiesResponse(r) = roundtrip(&msg) {
            assert_eq!(r.supports.len(), 2);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn configure_roundtrip() {
        let msg = Message::Configure(Configure {
            config: serde_json::json!({"host": "localhost", "port": 5432}),
        });
        if let Message::Configure(c) = roundtrip(&msg) {
            assert_eq!(c.config["host"], "localhost");
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn configure_ack_ok_roundtrip() {
        let msg = Message::ConfigureAck(ConfigureAck::ok());
        if let Message::ConfigureAck(a) = roundtrip(&msg) {
            assert_eq!(a.status, ConfigureStatus::Ok);
            assert!(a.code.is_none());
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn configure_ack_error_roundtrip() {
        let msg = Message::ConfigureAck(ConfigureAck::error("INVALID_CONFIG", "campo obrigatório"));
        if let Message::ConfigureAck(a) = roundtrip(&msg) {
            assert_eq!(a.status, ConfigureStatus::Error);
            assert_eq!(a.code.as_deref(), Some("INVALID_CONFIG"));
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn start_roundtrip() {
        let msg = Message::Start(StartParams::default());
        assert!(matches!(roundtrip(&msg), Message::Start(_)));
        // An empty start must serialize back to the bare `{"type":"start"}` so
        // plugins on protocol v1 see no new fields.
        assert_eq!(serde_json::to_string(&msg).unwrap(), r#"{"type":"start"}"#);
    }

    #[test]
    fn start_with_resume_roundtrip() {
        let msg = Message::Start(StartParams {
            resume: Some(ResumeCursor {
                cursor_value: serde_json::json!("2026-06-09T08:00:00Z"),
            }),
        });
        if let Message::Start(p) = roundtrip(&msg) {
            assert_eq!(p.resume.unwrap().cursor_value, "2026-06-09T08:00:00Z");
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn bare_start_deserializes_to_empty_params() {
        // Forward/backward compat: a v1 plugin emitting `{"type":"start"}`.
        let msg: Message = serde_json::from_str(r#"{"type":"start"}"#).unwrap();
        if let Message::Start(p) = msg {
            assert!(p.resume.is_none());
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn checkpoint_roundtrip() {
        let msg = Message::Checkpoint(Checkpoint {
            cursor_value: serde_json::json!(123456),
            batch_seq: 42,
        });
        if let Message::Checkpoint(c) = roundtrip(&msg) {
            assert_eq!(c.cursor_value, 123456);
            assert_eq!(c.batch_seq, 42);
        } else {
            panic!("wrong variant");
        }
        assert!(serde_json::to_string(&msg).unwrap().contains("\"type\":\"checkpoint\""));
    }

    #[test]
    fn committed_roundtrip() {
        let msg = Message::Committed(Committed { batch_seq: 7 });
        if let Message::Committed(c) = roundtrip(&msg) {
            assert_eq!(c.batch_seq, 7);
        } else {
            panic!("wrong variant");
        }
        assert!(serde_json::to_string(&msg).unwrap().contains("\"type\":\"committed\""));
    }

    #[test]
    fn schema_roundtrip() {
        let msg = Message::Schema(Schema {
            fields: vec![
                Field { name: "id".into(), field_type: FieldType::Int64 },
                Field { name: "nome".into(), field_type: FieldType::Utf8 },
                Field { name: "ativo".into(), field_type: FieldType::Bool },
            ],
        });
        if let Message::Schema(s) = roundtrip(&msg) {
            assert_eq!(s.fields.len(), 3);
            assert_eq!(s.fields[0].field_type, FieldType::Int64);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn ready_roundtrip() {
        assert!(matches!(roundtrip(&Message::Ready), Message::Ready));
    }

    #[test]
    fn progress_source_roundtrip() {
        let msg = Message::Progress(Progress {
            rows_read: Some(10_000),
            batches_read: Some(10),
            rows_written: None,
            batches_written: None,
        });
        if let Message::Progress(p) = roundtrip(&msg) {
            assert_eq!(p.rows_read, Some(10_000));
            assert!(p.rows_written.is_none());
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn object_ready_roundtrip() {
        let msg = Message::ObjectReady(ObjectReady {
            path: "/scratch/events.00000001.snappy.parquet".into(),
        });
        if let Message::ObjectReady(o) = roundtrip(&msg) {
            assert_eq!(o.path, "/scratch/events.00000001.snappy.parquet");
        } else {
            panic!("wrong variant");
        }
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"object_ready\""));
    }

    #[test]
    fn deliver_object_roundtrip() {
        let msg = Message::DeliverObject(DeliverObject { path: "/scratch/part.parquet".into() });
        if let Message::DeliverObject(d) = roundtrip(&msg) {
            assert_eq!(d.path, "/scratch/part.parquet");
        } else {
            panic!("wrong variant");
        }
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"deliver_object\""));
    }

    #[test]
    fn object_delivered_roundtrip() {
        let msg = Message::ObjectDelivered(ObjectDelivered { path: "/scratch/part.parquet".into() });
        if let Message::ObjectDelivered(d) = roundtrip(&msg) {
            assert_eq!(d.path, "/scratch/part.parquet");
        } else {
            panic!("wrong variant");
        }
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"object_delivered\""));
    }

    #[test]
    fn sink_kind_roundtrip() {
        let msg = Message::HandshakeAck(HandshakeAck {
            protocol_supported_versions: vec![1],
            plugin_name: "loadsmith-sink-local-copy".into(),
            plugin_version: "0.1.0".into(),
            kind: PluginKind::Sink,
        });
        if let Message::HandshakeAck(ack) = roundtrip(&msg) {
            assert_eq!(ack.kind, PluginKind::Sink);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn log_event_roundtrip() {
        let msg = Message::Log(LogEvent { level: LogLevel::Info, message: "conectado".into() });
        if let Message::Log(l) = roundtrip(&msg) {
            assert_eq!(l.level, LogLevel::Info);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn ping_pong_roundtrip() {
        assert!(matches!(roundtrip(&Message::Ping), Message::Ping));
        assert!(matches!(roundtrip(&Message::Pong), Message::Pong));
    }

    #[test]
    fn cancel_roundtrip() {
        let msg = Message::Cancel(Cancel { reason: "timeout".into() });
        if let Message::Cancel(c) = roundtrip(&msg) {
            assert_eq!(c.reason, "timeout");
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn error_roundtrip() {
        let msg = Message::Error(ProtocolError {
            code: "UNSUPPORTED_PROTOCOL_VERSION".into(),
            message: "plugin suporta [2], core suporta [1]".into(),
        });
        if let Message::Error(e) = roundtrip(&msg) {
            assert_eq!(e.code, "UNSUPPORTED_PROTOCOL_VERSION");
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn finished_success_source_roundtrip() {
        let msg = Message::Finished(Finished::success_source(21_473_000, 2_148));
        if let Message::Finished(f) = roundtrip(&msg) {
            assert_eq!(f.status, FinishedStatus::Success);
            assert_eq!(f.rows_read, Some(21_473_000));
            assert!(f.rows_written.is_none());
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn finished_cancelled_roundtrip() {
        let msg = Message::Finished(Finished::cancelled());
        if let Message::Finished(f) = roundtrip(&msg) {
            assert_eq!(f.status, FinishedStatus::Cancelled);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let json = r#"{"type":"handshake","future_field":"valor_desconhecido"}"#;
        let msg: Message = serde_json::from_str(json).expect("deve ignorar campo desconhecido");
        assert!(matches!(msg, Message::Handshake));
    }

    #[test]
    fn progress_omits_none_fields() {
        let msg = Message::Progress(Progress {
            rows_read: Some(100),
            batches_read: Some(1),
            rows_written: None,
            batches_written: None,
        });
        let json = serde_json::to_string(&msg).unwrap();
        assert!(!json.contains("rows_written"));
        assert!(!json.contains("batches_written"));
    }
}
