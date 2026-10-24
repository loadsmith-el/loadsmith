# Writing a Sink Plugin

A sink plugin delivers finalized files to a remote target. Unlike a destination,
a sink is **not a data-plane participant** — it never reads Arrow batches (no
fd3). The core hands it one local file path at a time and the sink ships each to
its target. See [Sink Delivery](../architecture/sink-delivery.md) for the design.

## The `SinkPlugin` trait

```rust
#[async_trait]
pub trait SinkPlugin: Send {
    fn plugin_name(&self) -> &str;
    fn plugin_version(&self) -> &str;

    fn capabilities(&self) -> Vec<String> { vec!["object_delivery".into()] }

    async fn configure(&mut self, config: PluginConfig) -> Result<()>;
    async fn prepare(&mut self) -> Result<()>;
    async fn deliver(&mut self, path: PathBuf) -> Result<()>; // one file
    async fn finalize(&mut self) -> Result<u64>;              // objects delivered
    async fn cancel(&mut self);
}
```

| Method | When called | What to do |
|---|---|---|
| `configure(config)` | After protocol negotiation | Parse with `config.parse::<YourConfig>()?` (unknown keys: see [Configuration](writing-a-source.md#configuration)) and validate (e.g. bucket/credentials). Don't transfer yet |
| `prepare()` | After `start` | Open connections / clients. SDK sends `Ready` when done |
| `deliver(path)` | Once per finalized file | Ship the file at `path` to the target. **Must be idempotent** |
| `finalize()` | After all objects are delivered (control EOF) | Flush, close, return the object count |
| `cancel()` | The run is aborted (the core sent `cancel`/`error`) or `deliver`/`finalize` failed, after `start` | Best-effort cleanup (e.g. abort an in-progress multipart upload). Objects already acknowledged stay delivered: a sink is not transactional |

## `deliver` must be idempotent

The core owns the delivery ledger. If the sink crashes mid-run, the core
respawns it and **re-sends every object it had not acknowledged** — so the same
path can arrive again, possibly after a partial transfer. `deliver` must
converge to the same result on a repeat. For a file copy, overwrite the target;
for S3, overwrite the object key (or abort any orphaned multipart upload first).
Name objects with the SDK's `ObjectKey` (a `key` template: `{file}`,
`{run_id}`, `{run_date}`, `{run_time}`, `{pipeline}`; default
`{run_date}/{run_id}/{file}`), as the canonical sinks do: every run gets its own
prefix, a continued run keeps the failed run's, and a pipeline moved from one
sink to another keeps its layout.

The SDK acknowledges each successful `deliver` to the core with an
`object_delivered` message automatically — you just return `Ok(())`.

## `deliver` can take as long as the transfer does

The core pings a sink every 5 s and takes a missing `pong` for a hang. The SDK
keeps answering while `deliver` runs (and queues the objects the core hands
over meanwhile), so an upload over a slow link is not mistaken for one. Keep
`deliver` async all the way down: blocking I/O on the runtime's thread (`std::fs`,
a blocking HTTP client) stalls the answers with it — use `tokio::fs` and an
async client, or `spawn_blocking`.

## Skeleton: the `local-copy` sink

```rust
// src/main.rs
mod plugin;

#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_sink(plugin::LocalCopyPlugin::new()).await
}
```

```rust
// src/plugin.rs
use std::path::PathBuf;
use anyhow::{Context, Result};
use async_trait::async_trait;
use loadsmith_plugin_sdk::{PluginConfig, SinkPlugin};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Config { dest: PathBuf }

pub struct LocalCopyPlugin { config: Option<Config>, delivered: u64 }

impl LocalCopyPlugin {
    pub fn new() -> Self { Self { config: None, delivered: 0 } }
}

#[async_trait]
impl SinkPlugin for LocalCopyPlugin {
    fn plugin_name(&self) -> &str { "loadsmith-sink-local-copy" }
    fn plugin_version(&self) -> &str { env!("CARGO_PKG_VERSION") }

    async fn configure(&mut self, config: PluginConfig) -> Result<()> {
        let cfg: Config = config.parse()?;
        std::fs::create_dir_all(&cfg.dest)?;
        self.config = Some(cfg);
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> { Ok(()) }

    async fn deliver(&mut self, path: PathBuf) -> Result<()> {
        let cfg = self.config.as_ref().unwrap();
        let name = path.file_name().context("path has no file name")?;
        // copy() overwrites → idempotent re-delivery after a crash.
        std::fs::copy(&path, cfg.dest.join(name))?;
        self.delivered += 1;
        Ok(())
    }

    async fn finalize(&mut self) -> Result<u64> { Ok(self.delivered) }
    async fn cancel(&mut self) {}
}
```

The binary name must be `loadsmith-sink-{type}` (e.g. `loadsmith-sink-local-copy`)
so the core can discover it from `--plugin-dir` (or `PATH`). Add the package to
`loadsmith-canonical-plugins` (or your own repo) with install metadata:

```toml
[package.metadata.loadsmith]
name     = "<type>"
summary  = "…"
protocol = "^1"
provides = [
    { kind = "sink", bin = "loadsmith-sink-<type>" },
]
```

and release it, so `loadsmith plugin install` can fetch it. A canonical plugin is
also picked up by the `-bundled` image, which runs `loadsmith plugin install --all`
against the canonical index.

## Telemetry

When the run has telemetry on, `run_sink` records spans `configure`, `start`
(`prepare()`), one `loadsmith.deliver` per `deliver()`
(`loadsmith.object.path`) and `finish` (`finalize()`); a failing step marks
its span `error`. The core adds its own `sink.deliver` span (from
`deliver_object` to `object_delivered`), and `respawn`/`retry` events when it
restarts a crashed sink. Add spans for the remote calls with `tracing`
(an HTTP API: one `client` span per request with `http.request.method`,
`url.full` without secrets, `http.response.status_code`, and a `retry` event
per retry). The API, the rules and the conventions table are in
[Writing a source → Telemetry](writing-a-source.md#telemetry).

## End-to-end testing


A sink is exercised by attaching it to a file-output destination. Add a
loadsmith-lab case whose pipeline stages Parquet and delivers it:

```yaml
destination:
  type: parquet
  config: { prefix: events, compression: snappy, max_file_size: "512KiB" }
sink:
  type: local-copy
  config: { dest: /output }
```

To exercise crash recovery, the `local-copy` sink honours
`LOADSMITH_SINK_CRASH_AFTER=N` (set via the case's `loadsmith.env`) to abort once
after `N` deliveries; the run must still succeed as the core respawns the sink
and resumes. See [Testing with loadsmith-lab](../getting-started/testing-with-lab.md).
