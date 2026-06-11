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

    async fn configure(&mut self, config: serde_json::Value) -> Result<()>;
    async fn prepare(&mut self) -> Result<()>;
    async fn deliver(&mut self, path: PathBuf) -> Result<()>; // one file
    async fn finalize(&mut self) -> Result<u64>;              // objects delivered
    async fn cancel(&mut self);
}
```

| Method | When called | What to do |
|---|---|---|
| `configure(config)` | After protocol negotiation | Deserialize and validate config (e.g. bucket/credentials). Don't transfer yet |
| `prepare()` | After `start` | Open connections / clients. SDK sends `Ready` when done |
| `deliver(path)` | Once per finalized file | Ship the file at `path` to the target. **Must be idempotent** |
| `finalize()` | After all objects are delivered (control EOF) | Flush, close, return the object count |
| `cancel()` | On abort | Best-effort cleanup |

## `deliver` must be idempotent

The core owns the delivery ledger. If the sink crashes mid-run, the core
respawns it and **re-sends every object it had not acknowledged** — so the same
path can arrive again, possibly after a partial transfer. `deliver` must
converge to the same result on a repeat. For a file copy, overwrite the target;
for S3, overwrite the object key (or abort any orphaned multipart upload first).

The SDK acknowledges each successful `deliver` to the core with an
`object_delivered` message automatically — you just return `Ok(())`.

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
use loadsmith_plugin_sdk::SinkPlugin;
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

    async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
        let cfg: Config = serde_json::from_value(config)?;
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
so the core can discover it from `--plugin-dir`. Register the crate in the
workspace root `Cargo.toml` and add it to the `Dockerfile`'s build/copy lists.

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
