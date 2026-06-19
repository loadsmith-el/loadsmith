# Skill: create-source-plugin

Scaffold a new **source** plugin: a standalone binary that reads from some origin
and streams the data as Apache Arrow batches. The SDK drives the whole protocol;
you implement only the `SourcePlugin` trait.

This skill takes arguments `<name> [notes]` (passed by the invoking agent — e.g.
Claude's `/create-source-plugin` slash command, or directly).

## Read these first (the working reference)

1. `crates/loadsmith-plugin-sdk/src/source.rs` — the `SourcePlugin` trait you implement
2. `plugins/sources/postgres/src/plugin.rs` — a real source (connect, schema, batched fetch)
3. `plugins/sources/postgres/src/types.rs` — how it maps native types → Arrow
4. `plugins/sources/postgres/{Cargo.toml,src/main.rs}` — the binary wiring
5. `crates/loadsmith-arrow/src/lib.rs` — the Arrow helpers available

## Architecture you MUST respect

- **You never touch fd3/fd4 or the protocol.** The SDK (`run_source`) handles the
  handshake, control plane, the Arrow IPC data stream on fd3, and the event
  channel. You only provide data through the trait.
- **`schema()` is a contract.** Every `RecordBatch` you return from `next_batch()`
  must match the schema you declared, exactly — same fields, same order, same
  Arrow types. Mismatches corrupt the data plane.
- **Stream, don't slurp.** Return data in batches of roughly `batch_size` rows
  (read it from config; sensible default ~1000). Return `Ok(None)` when exhausted.
  Never load the whole dataset into memory — that's the entire point of Loadsmith.
- **`plugin_name()` must equal the binary name** (`loadsmith-source-<name>`).

## Steps

### 1. Parse arguments
- **name** — the source type as used in a pipeline's `source.type` (e.g. `mysql`,
  `s3`, `http`). The binary becomes `loadsmith-source-<name>`.
- **notes** — optional context (driver, auth, etc.)

### 2. `plugins/sources/<name>/Cargo.toml`
Mirror `plugins/sources/postgres/Cargo.toml`. Package + `[[bin]]` both named
`loadsmith-source-<name>`. Always depend on:
```toml
tokio, anyhow, tracing, serde, serde_json, async-trait
arrow, arrow-array, arrow-schema   # via workspace
loadsmith-plugin-sdk = { path = "../../../crates/loadsmith-plugin-sdk" }
loadsmith-arrow      = { path = "../../../crates/loadsmith-arrow" }
```
Add whatever driver crate the source needs.

### 3. `plugins/sources/<name>/src/main.rs`
```rust
mod plugin;
mod types;   // only if you split out type mapping

#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_source(plugin::<Name>Plugin::new()).await
}
```

### 4. `plugins/sources/<name>/src/plugin.rs`
A struct holding connection/cursor state and `impl SourcePlugin`:
- `configure(config)` — deserialize a typed config struct (serde from
  `serde_json::Value`), connect, prepare, and determine the output schema. Return
  `Err` to reject bad config.
- `schema()` — return the Arrow `Schema` of the rows you'll emit.
- `next_batch()` — fetch the next ~`batch_size` rows, build a `RecordBatch`,
  return `Ok(Some(batch))`; `Ok(None)` when done.
- `cancel()` — abort promptly (close cursor, rollback, etc.).

### 5. `plugins/sources/<name>/src/types.rs` (when type mapping is non-trivial)
Map the source's native types to Arrow `DataType` and build column arrays with
`arrow_array::builder::*`. **Lesson from postgres:** prefer the database's text
representation and parse into Arrow when the native binary decoders don't cover
all types (NUMERIC, TIME, etc.) — see `plugins/sources/postgres/src/types.rs`.

### 6. Register the workspace member
Add `"plugins/sources/<name>"` to `members` in the root `Cargo.toml`.

## Verify

```bash
cargo build -p loadsmith-source-<name>     # must compile
```

Do not wire it into a real pipeline run yourself. The right end-to-end check is a
**loadsmith-lab** case against a real instance of the service (see
`../loadsmith-lab`). Print a summary and tell the user to add a lab case to verify.
