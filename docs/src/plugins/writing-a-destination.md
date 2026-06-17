# Writing a Destination Plugin

A destination plugin is a standalone Rust binary that consumes Apache Arrow
record batches and writes them to some sink. The SDK drives the entire protocol;
you implement one trait.

## The `DestinationPlugin` trait

```rust
#[async_trait]
pub trait DestinationPlugin: Send {
    fn plugin_name(&self) -> &str;
    fn plugin_version(&self) -> &str;

    // Optional — defaults shown.
    fn capabilities(&self) -> Vec<String> { /* ["batch_write"] */ }
    fn durable_through(&mut self) -> Option<u64> { None }

    async fn configure(&mut self, config: serde_json::Value) -> Result<()>;
    async fn prepare(&mut self) -> Result<()>;
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;
    async fn finalize(&mut self) -> Result<u64>;
    async fn cancel(&mut self);
}
```

| Method | When called | What to do |
|---|---|---|
| `plugin_name()` | During handshake | Return `"loadsmith-destination-{type}"` exactly |
| `plugin_version()` | During handshake | Return the semver version string |
| `capabilities()` | During handshake | Add `"object_output"` (stages files for a sink) or `"staged_merge"` as applicable |
| `configure(config)` | After protocol negotiation | Deserialize and validate config. Do not open resources yet |
| `prepare()` | After `start` signal, before first batch | Open files, connections, writers. Send `Ready` when done |
| `write_batch(batch)` | Once per incoming batch | Write this `RecordBatch` to the sink |
| `durable_through()` | After each batch | Highest batch ordinal now **durably** committed — gates the core's watermark persistence. Leave `None` if only durable at `finalize` |
| `finalize()` | After the IPC stream ends | Flush buffers, close resources, return total `rows_written` |
| `cancel()` | On abort | Clean up partial output (delete files, rollback, etc.) |

> **Durability & incremental state.** The SDK emits a `committed` event whenever
> `durable_through()` advances, plus a final one at `finalize()`. That ack is
> what lets the core persist the source's watermark for incremental loads — and
> an idempotent destination (e.g. an upsert/`MERGE` swap) turns the at-least-once
> guarantee into exactly-once. See [Incremental State](../architecture/incremental-state.md).

## The three lifecycle phases

The destination lifecycle has three distinct phases:

**Phase 1 — Configure.** The plugin receives its config and validates it. No
files are opened, no connections made. If validation fails, `configure()` returns
`Err` and the core aborts before any data flows.

**Phase 2 — Prepare.** After the source has declared its schema and the core has
started the pump, the plugin opens its sink (creates the output file, opens the
database connection, initializes the writer). When `prepare()` returns `Ok(())`,
the SDK sends `Ready` to the core and data begins flowing.

**Phase 3 — Stream.** `write_batch()` is called once per `RecordBatch`. Write
incrementally — do not accumulate. When the IPC stream ends (source exhausted),
`finalize()` is called.

## Step-by-step example: Parquet destination

> **This is a deliberately minimal teaching example** — one file, no
> compression choice, no splitting. The **shipped**
> `loadsmith-destination-parquet` builds on exactly this skeleton and adds
> three operator knobs: a configurable `compression` codec, a filename
> `prefix`, and size-based file splitting (`max_file_size`). For the full
> configuration reference see
> [Parquet](https://loadsmith-el.github.io/loadsmith-canonical-plugins/config/parquet.html)
> in the canonical plugins docs.

### 1. Create the crate

```
plugins/destinations/parquet/
  Cargo.toml
  src/
    main.rs
    plugin.rs
```

### 2. `Cargo.toml`

```toml
[package]
name = "loadsmith-destination-parquet"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "loadsmith-destination-parquet"
path = "src/main.rs"

[dependencies]
tokio        = { workspace = true }
anyhow       = { workspace = true }
tracing      = { workspace = true }
serde        = { workspace = true }
serde_json   = { workspace = true }
async-trait  = { workspace = true }
arrow-array  = { workspace = true }
loadsmith-plugin-sdk = { path = "../../../crates/loadsmith-plugin-sdk" }
loadsmith-arrow      = { path = "../../../crates/loadsmith-arrow" }

# Parquet writer. The `parquet` crate is released in lockstep with `arrow`
# (shared major version), so it must match the workspace's `arrow = "54"`.
parquet = "54"
```

### 3. `src/main.rs`

```rust
mod plugin;

#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_destination(plugin::ParquetPlugin::new()).await
}
```

### 4. `src/plugin.rs`

```rust
use anyhow::Result;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use loadsmith_plugin_sdk::DestinationPlugin;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
struct ParquetConfig {
    path: PathBuf,
}

pub struct ParquetPlugin {
    config: Option<ParquetConfig>,
    writer: Option<parquet::arrow::ArrowWriter<std::fs::File>>,
    rows_written: u64,
}

impl ParquetPlugin {
    pub fn new() -> Self {
        Self { config: None, writer: None, rows_written: 0 }
    }
}

#[async_trait]
impl DestinationPlugin for ParquetPlugin {
    fn plugin_name(&self) -> &str {
        "loadsmith-destination-parquet"
    }

    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
        let cfg: ParquetConfig = serde_json::from_value(config)?;

        // Validate — check the parent directory exists, etc.
        // Do NOT open the file here.
        if let Some(parent) = cfg.path.parent() {
            if !parent.exists() {
                anyhow::bail!("output directory does not exist: {}", parent.display());
            }
        }
        self.config = Some(cfg);
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        // Open the file and initialize the writer.
        // The schema arrives with the first batch via Arrow IPC header,
        // so we use ArrowWriter which reads the schema from the stream.
        let path = &self.config.as_ref().unwrap().path;
        let file = std::fs::File::create(path)?;
        // Writer is initialized per-batch or you can use parquet::ArrowWriter
        // which accepts the schema on construction — derive from first batch.
        // For simplicity, defer full writer init to first write_batch().
        Ok(())
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let path = &self.config.as_ref().unwrap().path;

        // Initialize writer on first batch (schema is known from the batch).
        if self.writer.is_none() {
            let file = std::fs::File::create(path)?;
            let writer = parquet::arrow::ArrowWriter::try_new(
                file,
                batch.schema(),
                None,
            )?;
            self.writer = Some(writer);
        }

        let n = batch.num_rows() as u64;
        self.writer.as_mut().unwrap().write(&batch)?;
        self.rows_written += n;
        Ok(())
    }

    async fn finalize(&mut self) -> Result<u64> {
        // Flush all row groups and write the Parquet footer.
        if let Some(writer) = self.writer.take() {
            writer.close()?;
        }
        Ok(self.rows_written)
    }

    async fn cancel(&mut self) {
        // Drop the writer and delete the partial file.
        self.writer = None;
        if let Some(cfg) = &self.config {
            let _ = std::fs::remove_file(&cfg.path);
        }
    }
}
```

### 5. Register in workspace root `Cargo.toml`

```toml
members = [
    # ... existing members ...
    "plugins/destinations/parquet",
]
```

### 6. Build and verify

```bash
cargo build -p loadsmith-destination-parquet
```

## `finalize()` is the official row count

The `u64` returned by `finalize()` is the `rows_written` value reported in the
run summary. Count accurately — the operator uses this to verify completeness.

## Write incrementally per batch

`write_batch()` is called once per batch. Write to the sink immediately; do not
buffer. If the source emits 50,000 batches of 2,000 rows each (100M rows total),
buffering all batches in memory before writing in `finalize()` would exhaust
available RAM.

```rust
// WRONG — accumulates all batches in memory
async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
    self.pending.push(batch);  // could be 100M rows
    Ok(())
}

// RIGHT — write immediately
async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
    self.writer.write(&batch)?;
    self.rows_written += batch.num_rows() as u64;
    Ok(())
}
```

## Using Arrow helpers

`loadsmith-arrow` provides `record_batch_to_json_rows` for converting a
`RecordBatch` to a `Vec<serde_json::Map>` (used by the JSONL destination):

```rust
use loadsmith_arrow::record_batch_to_json_rows;

async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
    let rows = record_batch_to_json_rows(&batch)?;
    for row in rows {
        let line = serde_json::to_string(&row)?;
        writeln!(self.writer, "{}", line)?;
        self.rows_written += 1;
    }
    Ok(())
}
```

## End-to-end testing

Add a loadsmith-lab case to verify your destination against a real source:

```yaml
# cases/postgres-to-parquet/case.yaml
case:
  name: postgres-to-parquet
services:
  - image: loadsmith-lab-postgres:15
    alias: pg
    readiness:
      tcp: 5432
      postgres:
        probe_query: "SELECT 1 FROM spacecraft_telemetry_events LIMIT 1"
# ...
expect:
  status: success
  rows_read: 100000
  rows_written: 100000
```

See [Testing with loadsmith-lab](../getting-started/testing-with-lab.md).
