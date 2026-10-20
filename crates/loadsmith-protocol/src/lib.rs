use serde::{Deserialize, Serialize};

mod schema;
pub use schema::{Field, FieldType, IntervalUnit, TimeUnit};

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

    // Parsing (raw source → core → parser)
    /// Core → parser (control plane): parse the local file at this path. The
    /// path comes from a raw source's `object_ready`. Idempotent per path.
    ParseObject(ParseObject),
    /// Parser → core (event plane, fd4): informational — a file was fully
    /// parsed into Arrow batches.
    ObjectParsed(ObjectParsed),

    // Value resolution (core ↔ provider)
    /// Core → provider (control plane): resolve one reference (the second
    /// argument of a `secret()` template call).
    Resolve(Resolve),
    /// Provider → core (control plane): the answer to the `resolve` with the
    /// same `id` — a value or an error, never both.
    Resolved(Resolved),

    // Observability
    Log(LogEvent),

    // Healthcheck
    Ping,
    Pong,

    // Control
    /// Core → plugin: abort the run. The plugin stops, undoes its partial work
    /// (a destination rolls back), and replies `finished` with status
    /// `cancelled`. Never followed by a clean end of the data stream.
    Cancel(Cancel),
    /// Core → plugin: the core is aborting the run because of a fault in *this*
    /// plugin (incompatible protocol, wrong kind, protocol violation, timeout).
    /// The plugin stops and undoes its partial work, as for `cancel`; no reply
    /// is expected.
    Error(ProtocolError),
    Finished(Finished),
    /// Either direction, control channel only: the previous line received
    /// from this peer was malformed; re-send your most recent control message
    /// once. A second consecutive malformed line is fatal.
    Resend(Resend),

    // Forward compatibility
    /// A message whose `type` this build does not know (sent by a newer peer).
    /// Only produced by deserialization: it carries nothing and cannot be
    /// serialized. `ControlReader` never returns it — under the tolerant-reader
    /// rule (see the protocol reference) unknown types are logged and skipped.
    #[serde(other, skip_serializing)]
    Unknown,
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
    /// What the plugin does with keys in `config` it does not recognise.
    /// Optional and additive: absent means [`UnknownKeys::Error`], and so
    /// does a value this build does not know (a newer core's), so a plugin
    /// never becomes less strict than asked. Older plugins ignore the field.
    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "lenient_unknown_keys")]
    pub unknown_keys: Option<UnknownKeys>,
}

impl Configure {
    /// A `configure` with the default policy (unknown keys are an error).
    pub fn new(config: serde_json::Value) -> Self {
        Self { config, unknown_keys: None }
    }

    /// The policy the plugin must apply: `unknown_keys`, or the default.
    pub fn unknown_keys(&self) -> UnknownKeys {
        self.unknown_keys.unwrap_or_default()
    }
}

/// The unknown-keys policy for a plugin's config block: whether a key the
/// plugin does not recognise fails `configure` (`error`, the default) or is
/// logged as a warning and ignored (`warn`). The operator chooses it when
/// running a pipeline (`--ignore-unknown-keys`); it applies to every plugin.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnknownKeys {
    /// An unknown key fails `configure`, naming every unknown key path.
    #[default]
    Error,
    /// Each unknown key is logged as a warning (fd4) and ignored.
    Warn,
}

impl UnknownKeys {
    pub fn as_str(self) -> &'static str {
        match self {
            UnknownKeys::Error => "error",
            UnknownKeys::Warn => "warn",
        }
    }
}

/// Tolerant reader for `configure.unknown_keys`: a value this build does not
/// know (or of the wrong type) reads as absent, i.e. the strict default.
fn lenient_unknown_keys<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<UnknownKeys>, D::Error> {
    let raw = serde_json::Value::deserialize(d)?;
    Ok(serde_json::from_value(raw).ok())
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

// ── Parsing ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParseObject {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectParsed {
    pub path: String,
    /// Rows parsed from this file.
    pub rows: u64,
    /// Arrow batches written to fd3 for this file.
    pub batches: u64,
    /// Global batch ordinal (1-based, as on `checkpoint`) of this file's last
    /// batch; `0` when the file produced no batch.
    pub last_batch_seq: u64,
}

// ── Value resolution ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resolve {
    /// Chosen by the core, unique within one provider conversation; echoed
    /// back in `resolved`.
    pub id: u64,
    /// The provider-defined reference, opaque to the core (e.g.
    /// `db.json#password`).
    #[serde(rename = "ref")]
    pub reference: String,
}

/// Body of `resolved`. Exactly one of `value` / `error` is set.
///
/// `Debug` never prints the value: a resolved value is a secret.
#[derive(Clone, Serialize, Deserialize)]
pub struct Resolved {
    pub id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ResolveError>,
}

impl std::fmt::Debug for Resolved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Resolved")
            .field("id", &self.id)
            .field("value", &self.value.as_ref().map(|_| "***"))
            .field("error", &self.error)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveError {
    pub code: String,
    pub message: String,
}

impl Resolved {
    pub fn value(id: u64, value: impl Into<String>) -> Self {
        Self { id, value: Some(value.into()), error: None }
    }

    pub fn error(id: u64, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            id,
            value: None,
            error: Some(ResolveError { code: code.into(), message: message.into() }),
        }
    }
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

// ── Resend ────────────────────────────────────────────────────────────────────

/// Body of `resend`. `{"type":"resend"}` is the whole message; `reason` is an
/// optional, human-readable description of what was wrong with the line.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Resend {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

// ── Error ─────────────────────────────────────────────────────────────────────

/// The `code`s the core puts in a core → plugin `error`. Plugins must treat an
/// unknown code like any other `error` (stop and undo partial work).
pub mod error_codes {
    /// No protocol version is supported by both sides.
    pub const UNSUPPORTED_PROTOCOL_VERSION: &str = "UNSUPPORTED_PROTOCOL_VERSION";
    /// The plugin's `handshake_ack` declared a different kind than the core
    /// spawned it as.
    pub const WRONG_PLUGIN_KIND: &str = "WRONG_PLUGIN_KIND";
    /// The plugin sent a message that is out of place for the lifecycle phase.
    pub const PROTOCOL_VIOLATION: &str = "PROTOCOL_VIOLATION";
    /// Two consecutive malformed control lines (a `resend` did not help).
    pub const MALFORMED_MESSAGE: &str = "MALFORMED_MESSAGE";
    /// The plugin did not answer within the configured timeout.
    pub const TIMEOUT: &str = "TIMEOUT";
}

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PluginKind {
    Source,
    Destination,
    Sink,
    Parser,
    Provider,
}

/// Every kind's user-facing name, in [`PluginKind::ALL`] order. The same
/// spelling is used on the wire (`handshake_ack.kind`), in binary names
/// (`loadsmith-<kind>-<type>`), in plugin manifests, on the command line and
/// in log labels.
pub const PLUGIN_KIND_NAMES: [&str; 5] = [
    PluginKind::Source.as_str(),
    PluginKind::Destination.as_str(),
    PluginKind::Sink.as_str(),
    PluginKind::Parser.as_str(),
    PluginKind::Provider.as_str(),
];

impl PluginKind {
    pub const ALL: [PluginKind; 5] = [
        PluginKind::Source,
        PluginKind::Destination,
        PluginKind::Sink,
        PluginKind::Parser,
        PluginKind::Provider,
    ];

    /// The kind's one spelling: wire, binary prefix, manifest, CLI, logs.
    pub const fn as_str(self) -> &'static str {
        match self {
            PluginKind::Source => "source",
            PluginKind::Destination => "destination",
            PluginKind::Sink => "sink",
            PluginKind::Parser => "parser",
            PluginKind::Provider => "provider",
        }
    }

    /// The kind spelled `name` (as [`as_str`](Self::as_str) spells it).
    pub fn from_name(name: &str) -> Option<PluginKind> {
        PluginKind::ALL.into_iter().find(|k| k.as_str() == name)
    }

    /// The binary that implements `plugin_type` for this kind, e.g.
    /// `loadsmith-provider-file`.
    pub fn binary_name(self, plugin_type: &str) -> String {
        format!("loadsmith-{}-{plugin_type}", self.as_str())
    }
}

impl std::fmt::Display for PluginKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ── Capability names ──────────────────────────────────────────────────────────

/// Capability names (`capabilities_response.supports`) with a defined
/// meaning. The core gates features on some of them; unknown names are
/// ignored.
pub mod capabilities {
    /// Source: streams Arrow batches on fd3 (informational).
    pub const BATCH_READ: &str = "batch_read";
    /// Destination: reads Arrow batches from fd3 (informational).
    pub const BATCH_WRITE: &str = "batch_write";
    /// Source: accepts a resume cursor and reports watermarks. Required by a
    /// `state:` block.
    pub const INCREMENTAL_STATE: &str = "incremental_state";
    /// Destination: stages files and announces each with `object_ready`
    /// (required to attach a sink). Source: a *raw* source — it produces
    /// files instead of Arrow and announces each with `object_ready`
    /// (requires a `parser:`).
    pub const OBJECT_OUTPUT: &str = "object_output";
    /// Sink: delivers staged files (`deliver_object`).
    pub const OBJECT_DELIVERY: &str = "object_delivery";
    /// Parser: turns files (`parse_object`) into Arrow batches on fd3.
    /// Required of a `parser:`.
    pub const PARSE: &str = "parse";
    /// Raw source: downloads its files to a local directory. The core then
    /// adds `scratch_dir` (a fresh directory) to its config unless the user
    /// set one, and removes that directory after a successful run.
    pub const SCRATCH_DIR: &str = "scratch_dir";
    /// Provider: answers `resolve` with `resolved`.
    pub const RESOLVE: &str = "resolve";
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
    /// `finished{success}` without counters (a provider).
    pub fn success() -> Self {
        Self {
            status: FinishedStatus::Success,
            rows_read: None,
            batches_read: None,
            rows_written: None,
            batches_written: None,
            code: None,
            message: None,
        }
    }

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
    fn unknown_type_deserializes_to_unknown() {
        let msg: Message =
            serde_json::from_str(r#"{"type":"from_the_future","x":1}"#).expect("deserialize");
        assert!(matches!(msg, Message::Unknown));
    }

    #[test]
    fn unknown_cannot_be_serialized() {
        assert!(serde_json::to_string(&Message::Unknown).is_err());
    }

    #[test]
    fn known_type_with_bad_fields_is_still_an_error() {
        // `other` only catches unknown tags; a known tag with a malformed body
        // stays a protocol violation.
        assert!(serde_json::from_str::<Message>(r#"{"type":"checkpoint"}"#).is_err());
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
        let msg = Message::Configure(Configure::new(serde_json::json!({"host": "localhost", "port": 5432})));
        if let Message::Configure(c) = roundtrip(&msg) {
            assert_eq!(c.config["host"], "localhost");
            assert_eq!(c.unknown_keys(), UnknownKeys::Error);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn configure_unknown_keys_is_optional_and_tolerant() {
        let parse = |line: &str| match serde_json::from_str::<Message>(line).unwrap() {
            Message::Configure(c) => c,
            other => panic!("wrong variant: {other:?}"),
        };
        // Absent: the strict default, and nothing is written back.
        let c = parse(r#"{"type":"configure","config":{}}"#);
        assert_eq!((c.unknown_keys, c.unknown_keys()), (None, UnknownKeys::Error));
        assert_eq!(serde_json::to_string(&Message::Configure(c)).unwrap(), r#"{"type":"configure","config":{}}"#);
        assert_eq!(parse(r#"{"type":"configure","config":{},"unknown_keys":"warn"}"#).unknown_keys(), UnknownKeys::Warn);
        // A value from a newer core (or of the wrong type) reads as the default.
        for odd in [r#""ignore""#, "1", "null", r#"{"mode":"warn"}"#] {
            let c = parse(&format!(r#"{{"type":"configure","config":{{}},"unknown_keys":{odd}}}"#));
            assert_eq!(c.unknown_keys(), UnknownKeys::Error, "{odd}");
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
                Field::new("id", FieldType::Int64).with_nullable(false),
                Field::new("nome", FieldType::Utf8),
                Field::new("ativo", FieldType::Bool),
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
    fn kind_names_are_the_wire_names() {
        for kind in PluginKind::ALL {
            let wire = serde_json::to_string(&kind).unwrap();
            assert_eq!(wire, format!("\"{}\"", kind.as_str()));
            assert_eq!(PluginKind::from_name(kind.as_str()), Some(kind));
            assert_eq!(kind.to_string(), kind.as_str());
        }
        assert_eq!(PLUGIN_KIND_NAMES, PluginKind::ALL.map(PluginKind::as_str));
        assert_eq!(PluginKind::Provider.binary_name("file"), "loadsmith-provider-file");
        assert_eq!(PluginKind::from_name("config_provider"), None);
    }

    #[test]
    fn resolve_roundtrip() {
        let msg = Message::Resolve(Resolve { id: 3, reference: "db.json#password".into() });
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(json, r#"{"type":"resolve","id":3,"ref":"db.json#password"}"#);
        let Message::Resolve(r) = roundtrip(&msg) else { panic!("wrong variant") };
        assert_eq!((r.id, r.reference.as_str()), (3, "db.json#password"));
    }

    #[test]
    fn resolved_forms_roundtrip() {
        let ok = Message::Resolved(Resolved::value(1, "s3cr3t"));
        assert_eq!(
            serde_json::to_string(&ok).unwrap(),
            r#"{"type":"resolved","id":1,"value":"s3cr3t"}"#
        );
        let err = Message::Resolved(Resolved::error(2, "NOT_FOUND", "no such key"));
        let Message::Resolved(r) = roundtrip(&err) else { panic!("wrong variant") };
        assert!(r.value.is_none());
        assert_eq!(r.error.unwrap().code, "NOT_FOUND");
    }

    #[test]
    fn resolved_debug_never_shows_the_value() {
        let shown = format!("{:?}", Message::Resolved(Resolved::value(1, "hunter22")));
        assert!(!shown.contains("hunter22"), "{shown}");
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
    fn resend_roundtrip() {
        let bare = Message::Resend(Resend::default());
        assert_eq!(serde_json::to_string(&bare).unwrap(), r#"{"type":"resend"}"#);
        let msg: Message = serde_json::from_str(r#"{"type":"resend"}"#).unwrap();
        assert!(matches!(msg, Message::Resend(Resend { reason: None })));
        let with_reason = Message::Resend(Resend { reason: Some("bad json".into()) });
        if let Message::Resend(r) = roundtrip(&with_reason) {
            assert_eq!(r.reason.as_deref(), Some("bad json"));
        } else {
            panic!("wrong variant");
        }
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
