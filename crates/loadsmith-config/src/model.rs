use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    pub pipeline: PipelineMeta,
    pub source: PluginRef,
    pub destination: PluginRef,
    /// Optional delivery stage. Only valid for file-output destinations (those
    /// advertising the `object_output` capability) — the core enforces that at
    /// runtime. When present, the destination stages files locally and the sink
    /// delivers each finalized file to its remote target.
    #[serde(default)]
    pub sink: Option<PluginRef>,
    #[serde(default)]
    pub state: Option<StateConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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

/// A reference to a plugin with its opaque config block. Strict like the rest
/// of the core-owned schema (a typo such as `confg:` must not silently drop
/// the plugin's whole config); only `config`'s *contents* are free-form — the
/// plugin validates those.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginRef {
    #[serde(rename = "type")]
    pub plugin_type: String,
    #[serde(default)]
    pub config: serde_yaml::Value,
}

/// Core-owned state block. Enables incremental loads: the core persists the
/// source's high watermark here and hands it back on the next run. The cursor
/// column itself lives in `source.config` (plugin-validated) — the core only
/// stores the opaque value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
