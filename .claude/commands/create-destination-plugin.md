---
description: Scaffold a new Loadsmith destination plugin (Arrow batches → sink)
argument-hint: <name> [notes]
allowed-tools: [Read, Write, Edit, Bash]
---

Scaffold a new **destination** plugin: a standalone binary that consumes Apache
Arrow batches and writes them to some sink. The SDK drives the whole protocol; you
implement only the `DestinationPlugin` trait.

The user invoked this command with: $ARGUMENTS

## Read these first (the working reference)

1. `crates/loadsmith-plugin-sdk/src/destination.rs` — the `DestinationPlugin` trait
2. `plugins/destinations/jsonl/src/plugin.rs` — a real destination (prepare, write, finalize)
3. `plugins/destinations/jsonl/{Cargo.toml,src/main.rs}` — the binary wiring
4. `crates/loadsmith-arrow/src/lib.rs` — Arrow helpers (e.g. `record_batch_to_json_rows`)

## Architecture you MUST respect

- **You never touch fd3/fd4 or the protocol.** The SDK (`run_destination`) reads
  the Arrow IPC stream on fd3 and calls your `write_batch` once per batch. You only
  implement the trait.
- **The lifecycle has three phases.** `prepare()` opens resources (files,
  connections) and the SDK then signals `ready`; `write_batch()` is called for
  every incoming `RecordBatch`; `finalize()` runs after the stream ends — flush and
  close there, and return the total rows written.
- **`finalize()`'s return is the official `rows_written`** in the run summary —
  count accurately.
- **Don't buffer the whole stream.** Write incrementally per batch; `finalize()`
  is for flushing, not for doing all the work.
- **`plugin_name()` must equal the binary name** (`loadsmith-destination-<name>`).

## Steps

### 1. Parse arguments
- **name** — the destination type as used in a pipeline's `destination.type` (e.g.
  `parquet`, `stdout`, `s3`). The binary becomes `loadsmith-destination-<name>`.
- **notes** — optional context (format, partitioning, auth, etc.)

### 2. `plugins/destinations/<name>/Cargo.toml`
Mirror `plugins/destinations/jsonl/Cargo.toml`. Package + `[[bin]]` both named
`loadsmith-destination-<name>`. Always depend on:
```toml
tokio, anyhow, tracing, serde, serde_json, async-trait
arrow-array   # via workspace (add arrow/arrow-schema if you need them)
loadsmith-plugin-sdk = { path = "../../../crates/loadsmith-plugin-sdk" }
loadsmith-arrow      = { path = "../../../crates/loadsmith-arrow" }
```
Add whatever writer/client crate the sink needs.

### 3. `plugins/destinations/<name>/src/main.rs`
```rust
mod plugin;

#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_destination(plugin::<Name>Plugin::new()).await
}
```

### 4. `plugins/destinations/<name>/src/plugin.rs`
A struct holding sink state + `impl DestinationPlugin`:
- `configure(config)` — deserialize a typed config struct; validate (don't open
  resources yet). Return `Err` to reject bad config.
- `prepare()` — open the file/connection/writer. The schema arrives with the data
  stream, so derive output shape from the first batch if needed.
- `write_batch(batch)` — write this `RecordBatch` to the sink. Use the Arrow
  helpers where they fit (e.g. `loadsmith_arrow::record_batch_to_json_rows`).
- `finalize()` — flush, close, and return the total rows written as `u64`.
- `cancel()` — abort and clean up partial output promptly.

### 5. Register the workspace member
Add `"plugins/destinations/<name>"` to `members` in the root `Cargo.toml`.

## Verify

```bash
cargo build -p loadsmith-destination-<name>     # must compile
```

Do not wire it into a real pipeline run yourself. The right end-to-end check is a
**loadsmith-lab** case that routes a real source into this destination and asserts
the output (see `../loadsmith-lab`). Print a summary and point the user there.
