use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A pipeline file. The structs of this module accept unknown keys on
/// purpose: [`crate::strict::deserialize_envelope`] finds them, with their
/// paths, and the operator's policy rejects them (the default) or warns and
/// ignores them (`--ignore-unknown-keys`). Never deserialize a pipeline file
/// from untrusted input without it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub pipeline: PipelineMeta,
    /// Provider instances, by name: each resolves the `secret('<name>', …)`
    /// template calls that name it. Only the instances a `secret()` call
    /// references are started.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub providers: BTreeMap<String, ProviderRef>,
    pub source: PluginRef,
    /// Optional parsing stage between a raw source (one advertising
    /// `object_output`: it produces files, not Arrow) and the destination:
    /// the parser turns each file into Arrow batches. Required by a raw
    /// source, rejected for any other (checked at run time, once the
    /// capabilities are known); not combinable with `state:` in 1.0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parser: Option<PluginRef>,
    pub destination: PluginRef,
    /// Optional delivery stage. Only valid for file-output destinations (those
    /// advertising the `object_output` capability) — the core enforces that at
    /// runtime. When present, the destination stages files locally and the sink
    /// delivers each finalized file to its remote target.
    #[serde(default)]
    pub sink: Option<PluginRef>,
    #[serde(default)]
    pub state: Option<StateConfig>,
    /// OpenTelemetry export (traces, metrics, logs over OTLP). Every field is
    /// optional; CLI flags override it and it overrides the standard `OTEL_*`
    /// variables. Resolved before the providers start, so it may use
    /// `env()`/`file()` but never `secret()`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telemetry: Option<TelemetryConfig>,
}

/// The `telemetry:` block. Absent fields fall back to the standard `OTEL_*`
/// environment variables, then to the defaults; see
/// `docs/src/architecture/telemetry.md`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TelemetryConfig {
    /// Default: on exactly when an endpoint is resolved. `false` always wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// The OTLP endpoint (base URL; `/v1/<signal>` is appended for
    /// `http-protobuf`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<OtlpProtocol>,
    /// Extra request headers (gRPC metadata), merged over
    /// `OTEL_EXPORTER_OTLP_HEADERS`. Their values are always masked.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    /// Per-export timeout. Default 10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
    /// The signals to export. Default: all three.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signals: Option<Vec<TelemetrySignal>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampling: Option<SamplingConfig>,
    /// How often metrics are exported. Default 15; a final export always
    /// happens at the end of the run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_interval_seconds: Option<u64>,
    /// Minimum level exported as OTLP logs (stderr is unaffected). Default
    /// `info`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_level: Option<loadsmith_protocol::LogLevel>,
    /// Resource attributes, merged over `OTEL_RESOURCE_ATTRIBUTES` /
    /// `OTEL_SERVICE_NAME`. Scalar values only.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub resource: BTreeMap<String, serde_yaml::Value>,
}

/// OTLP transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OtlpProtocol {
    #[serde(rename = "grpc")]
    Grpc,
    #[serde(rename = "http-protobuf", alias = "http/protobuf")]
    HttpProtobuf,
}

impl OtlpProtocol {
    /// The spelling in the pipeline file and on the command line.
    pub fn as_str(self) -> &'static str {
        match self {
            OtlpProtocol::Grpc => "grpc",
            OtlpProtocol::HttpProtobuf => "http-protobuf",
        }
    }

    /// Parses the pipeline-file spelling, and `OTEL_EXPORTER_OTLP_PROTOCOL`'s
    /// (`http/protobuf`).
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "grpc" => Some(OtlpProtocol::Grpc),
            "http-protobuf" | "http/protobuf" => Some(OtlpProtocol::HttpProtobuf),
            _ => None,
        }
    }
}

/// One telemetry signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetrySignal {
    Traces,
    Metrics,
    Logs,
}

/// `telemetry.sampling`: a parent-based trace-id ratio sampler.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SamplingConfig {
    /// Fraction of runs traced, `0.0`–`1.0`.
    pub ratio: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineMeta {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// Seconds between "still running" heartbeat logs during the data pump.
    /// `0` disables the heartbeat. Defaults to 30.
    #[serde(default = "default_heartbeat_seconds")]
    pub heartbeat_seconds: u64,
    /// Limits on how long the core waits for plugins. Every field is optional.
    #[serde(default)]
    pub timeouts: Timeouts,
}

fn default_heartbeat_seconds() -> u64 {
    30
}

/// How long the core waits for a plugin before it aborts the run. Values are
/// seconds; `0` disables that limit (for `cancel_grace_seconds`, `0` kills at
/// once). A timeout fails the run through the abort path: the core sends
/// `error` (code `TIMEOUT`) to the late plugin and `cancel` to the others.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Timeouts {
    /// Each reply during the handshake and capability exchange.
    #[serde(default = "default_handshake_seconds")]
    pub handshake_seconds: u64,
    /// `configure` → `configure_ack`. Plugins often connect here.
    #[serde(default = "default_configure_seconds")]
    pub configure_seconds: u64,
    /// `start` → `schema` (source) or `ready` (destination, sink). A source
    /// may run its query before it knows its schema.
    #[serde(default = "default_start_seconds")]
    pub start_seconds: u64,
    /// The source's data stream ended → its `finished`.
    #[serde(default = "default_finish_seconds")]
    pub finish_seconds: u64,
    /// The core ended the destination's input → its `finished`. Covers
    /// `finalize` (a final commit, a staged `MERGE`), so the default is long.
    #[serde(default = "default_finalize_seconds")]
    pub finalize_seconds: u64,
    /// After `cancel`/`error`, how long a plugin gets to undo its work and
    /// exit before it is killed.
    #[serde(default = "default_cancel_grace_seconds")]
    pub cancel_grace_seconds: u64,
    /// Opt-in data-plane stall detection: fail the run when no batch passes
    /// through the pump for this long. `0` (the default) disables it, and with
    /// it `first_batch_seconds`.
    #[serde(default)]
    pub stall_seconds: u64,
    /// With `stall_seconds` on: the budget for the *first* batch, measured from
    /// the start of the pump. Separate and generous because some sources buffer
    /// their whole result before emitting a batch.
    #[serde(default = "default_first_batch_seconds")]
    pub first_batch_seconds: u64,
}

fn default_handshake_seconds() -> u64 {
    60
}
fn default_configure_seconds() -> u64 {
    300
}
fn default_start_seconds() -> u64 {
    600
}
fn default_finish_seconds() -> u64 {
    300
}
fn default_finalize_seconds() -> u64 {
    6 * 3600
}
fn default_cancel_grace_seconds() -> u64 {
    30
}
fn default_first_batch_seconds() -> u64 {
    3600
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            handshake_seconds: default_handshake_seconds(),
            configure_seconds: default_configure_seconds(),
            start_seconds: default_start_seconds(),
            finish_seconds: default_finish_seconds(),
            finalize_seconds: default_finalize_seconds(),
            cancel_grace_seconds: default_cancel_grace_seconds(),
            stall_seconds: 0,
            first_batch_seconds: default_first_batch_seconds(),
        }
    }
}

impl Timeouts {
    /// `seconds` as a limit: `None` when `0` (disabled).
    pub fn limit(seconds: u64) -> Option<std::time::Duration> {
        (seconds > 0).then(|| std::time::Duration::from_secs(seconds))
    }
}

/// A reference to a plugin with its opaque config block. Checked for unknown
/// keys like the rest of the core-owned schema (a typo such as `confg:` must
/// not silently drop the plugin's whole config); `config`'s *contents* are
/// the plugin's to check, under the same policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginRef {
    #[serde(rename = "type")]
    pub plugin_type: String,
    #[serde(default)]
    pub config: serde_yaml::Value,
}

/// How long the core waits for one provider instance by default: from its
/// spawn to its `finished`, every reference included.
pub const DEFAULT_PROVIDER_TIMEOUT_SECONDS: u64 = 60;

/// A provider instance (`providers.<name>`): a plugin of kind `provider`
/// (binary `loadsmith-provider-<type>`) with its opaque config. Its config
/// may use `env()`/`file()` but never `secret()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderRef {
    #[serde(rename = "type")]
    pub plugin_type: String,
    #[serde(default)]
    pub config: serde_yaml::Value,
    /// The instance's whole budget, from spawn to `finished`. Defaults to
    /// [`DEFAULT_PROVIDER_TIMEOUT_SECONDS`]; `0` disables it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
}

impl ProviderRef {
    /// The effective timeout (`None` when disabled).
    pub fn timeout(&self) -> Option<std::time::Duration> {
        Timeouts::limit(self.timeout_seconds.unwrap_or(DEFAULT_PROVIDER_TIMEOUT_SECONDS))
    }
}

/// Core-owned state block. Enables incremental loads: the core persists the
/// source's high watermark here and hands it back on the next run. The cursor
/// column itself lives in `source.config` (plugin-validated) — the core only
/// stores the opaque value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    /// Where the state document lives. `local` is the only backend today.
    pub backend: String,
    /// Backend-specific location (a filesystem path for `local`).
    pub path: String,
    /// What to do when the source schema differs from the one recorded in state.
    #[serde(default)]
    pub on_schema_change: SchemaChangePolicy,
    /// Throttle for intra-run watermark persistence: persist the safe watermark
    /// at most once every N durably-committed batches. `0` ⇒ persist only at the
    /// end of a successful run.
    #[serde(default)]
    pub checkpoint_interval: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SchemaChangePolicy {
    /// Abort the run if the schema drifted from the recorded state (default).
    #[default]
    Refuse,
    /// Proceed anyway, overwriting the recorded schema fingerprint.
    Continue,
}
