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
    fn take_ready_objects(&mut self) -> Vec<PathBuf> { Vec::new() }

    async fn configure(&mut self, config: PluginConfig) -> Result<()>;
    async fn prepare(&mut self) -> Result<()>;
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;
    async fn finalize(&mut self) -> Result<u64>;
    async fn cancel(&mut self);
}
```

| Method | When called | What to do |
|---|---|---|
| `plugin_name()` | During handshake | Return `"loadsmith-destination-{type}"` (convention, shown in the run summary) |
| `plugin_version()` | During handshake | Return the semver version string |
| `capabilities()` | During handshake | Add `"object_output"` (stages files for a sink) or `"staged_merge"` as applicable |
| `configure(config)` | After protocol negotiation | Parse with `config.parse::<YourConfig>()?` (unknown keys: see [Configuration](writing-a-source.md#configuration)) and validate. Do not open resources yet |
| `prepare()` | After `start` signal, before first batch | Open files, connections, writers. Send `Ready` when done |
| `write_batch(batch)` | Once per incoming batch | Write this `RecordBatch` to the sink |
| `durable_through()` | After each batch | Highest batch ordinal now **durably** committed — gates the core's watermark persistence. Leave `None` if only durable at `finalize` |
| `take_ready_objects()` | After each `write_batch()` and after `finalize()` | Return (and clear) the paths of files finalized since the last call — the SDK emits an `object_ready` for each. Required with `object_output`: advertising the capability alone delivers nothing to the sink |
| `finalize()` | After the IPC stream ends **cleanly** (the core writes the end marker only after the source succeeded) | Flush buffers, commit, close resources, return total `rows_written` |
| `cancel()` | The run will not complete (after `start`): the core sent `cancel`/`error`, the core is gone (stdin closed, or fd3 ended without the end-of-stream marker), or `write_batch`/`finalize` failed | **Undo the partial load**: roll back the open transaction, drop the staging table, delete partial files. `finalize()` is never called after it |

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

**Phase 2 — Prepare.** After the source has declared its schema, the core sends
`start` and the plugin opens its output (creates the output file, opens the
database connection, initializes the writer). The pump starts only once
`prepare()` returns `Ok(())` and the SDK sends `Ready` to the core; then data
begins flowing.

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

Plugins don't live in the loadsmith repo: create the package in your own repo or
as a member of
[`loadsmith-canonical-plugins`](https://github.com/loadsmith-el/loadsmith-canonical-plugins),
where the real one lives at `parquet/`.

```
parquet/
  Cargo.toml
  src/
    main.rs
    plugin.rs
```

### 2. `Cargo.toml`

The SDK crates are git dependencies pinned by `rev`, and
`[package.metadata.loadsmith]` is the install metadata a release turns into the
published `loadsmith-plugin.yaml` read by `loadsmith plugin install`.

```toml
[package]
name = "loadsmith-destination-parquet"
version = "0.1.0"
edition = "2021"

[package.metadata.loadsmith]
name     = "parquet"
summary  = "Parquet destination"
protocol = "^1"
provides = [
    { kind = "destination", bin = "loadsmith-destination-parquet" },
]

[[bin]]
name = "loadsmith-destination-parquet"
path = "src/main.rs"

[dependencies]
tokio        = { version = "1", features = ["full"] }
anyhow       = "1"
tracing      = "0.1"
serde        = { version = "1", features = ["derive"] }
serde_json   = "1"
async-trait  = "0.1"
arrow-array  = "54"
loadsmith-plugin-sdk = { git = "https://github.com/loadsmith-el/loadsmith", rev = "<commit>" }
loadsmith-arrow      = { git = "https://github.com/loadsmith-el/loadsmith", rev = "<commit>" }

# Parquet writer. The `parquet` crate is released in lockstep with `arrow`
# (shared major version), so it must match `arrow = "54"`.
parquet = "54"
```

(Inside `loadsmith-canonical-plugins`, use `{ workspace = true }`; the workspace
root pins the SDK `rev` once for every plugin.)

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
use loadsmith_plugin_sdk::{DestinationPlugin, PluginConfig};
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

    async fn configure(&mut self, config: PluginConfig) -> Result<()> {
        let cfg: ParquetConfig = config.parse()?;

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
        // Called when the run aborts (or after our own failure): the input
        // is incomplete, so drop the writer and delete the partial file.
        self.writer = None;
        if let Some(cfg) = &self.config {
            let _ = std::fs::remove_file(&cfg.path);
        }
    }
}
```

### 5. Build and verify

```bash
cargo build --release    # → target/release/loadsmith-destination-parquet
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
`RecordBatch` to a `Vec<serde_json::Value>` — one JSON object per row (used by
the JSONL destination):

```rust
use loadsmith_arrow::record_batch_to_json_rows;

async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
    let rows = record_batch_to_json_rows(&batch);
    for row in rows {
        let line = serde_json::to_string(&row)?;
        writeln!(self.writer, "{}", line)?;
        self.rows_written += 1;
    }
    Ok(())
}
```

The conversion handles every Arrow type and never panics. Its JSON forms:
integers and floats as numbers (NaN and ±Infinity as `null`), decimals as exact
strings (`"123.45"`), strings as strings, binaries as base64, `date32` as
integer days, `date64`, timestamps of any unit, times and durations as integer
milliseconds (finer units floored), intervals as objects (`{"months": …}`),
lists as arrays, structs and maps as objects, dictionaries as their decoded
values. Types outside the [protocol vocabulary](../protocol/messages.md#schema)
(`union`, `run_end_encoded`, list views) become `null`. The full table is in
the `loadsmith_arrow::convert` module docs.

## Telemetry

When the run has telemetry on, `run_destination` records spans `configure`,
`start` (`prepare()`), one `loadsmith.batch` per `write_batch()`
(`direction=written`, rows, bytes) and `finish` (`finalize()`), plus the
`loadsmith.plugin.*` batch metrics; a failing step marks its span `error`. The
core exports them under the run's trace — the plugin carries no OTLP code.
Add your own spans with `tracing` and metrics with
`loadsmith_plugin_sdk::telemetry::{counter, histogram, gauge}`, following the
OpenTelemetry semantic conventions — for a database, one `client` span per
statement, COPY, batch insert or merge phase (`db.system.name`,
`db.namespace`, `db.collection.name`, `db.operation.name`, `db.query.text` =
the generated statement, never bound values) and the
`db.client.operation.duration` histogram; for files, one span per file
written. Never one per row. The API, the rules and the conventions table are
in [Writing a source → Telemetry](writing-a-source.md#telemetry).

## End-to-end testing


Add a loadsmith-lab case (in `loadsmith-lab-canonical-catalog/cases/`, or a
directory passed with `--cases-dir`) to verify your destination against a real
source. The catalog already has `postgres-to-parquet-single`, `-chunked` and
`-localcopy` cases:

```yaml
# cases/postgres-to-parquet-single/case.yaml
case:
  name: postgres-to-parquet-single
services:
  - image: images/lab-postgres-15
    alias: pg
    readiness:
      tcp: 5432
      postgres:
        dbname: lab
        user: lab
        password: lab
        probe_query: "SELECT 1 FROM spacecraft_telemetry_events LIMIT 1"
pipeline:
  file: pipeline.yaml
expect:
  status: success
  rows_read: 100000
  rows_written: 100000
```

Pass `--plugin <path to your plugin crate>` to `loadsmith-lab run` to test your
local destination build instead of the published one.

See [Testing with loadsmith-lab](../getting-started/testing-with-lab.md).
