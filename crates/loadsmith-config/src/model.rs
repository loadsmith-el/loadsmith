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
}

fn default_heartbeat_seconds() -> u64 {
    30
}

/// A reference to a plugin with its opaque config block.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
