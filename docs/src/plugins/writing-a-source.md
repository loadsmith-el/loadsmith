# Writing a Source Plugin

A source plugin is a standalone Rust binary that reads from an origin system and
streams data as Apache Arrow record batches. The SDK drives the entire protocol;
you implement one trait.

## The `SourcePlugin` trait

```rust
#[async_trait]
pub trait SourcePlugin: Send {
    fn plugin_name(&self) -> &str;
    fn plugin_version(&self) -> &str;

    // Optional — defaults shown; override only for incremental sources.
    fn capabilities(&self) -> Vec<String> { /* ["batch_read","schema_inference"] */ }
    async fn resume_from(&mut self, _cursor_value: Option<serde_json::Value>) {}
    fn current_watermark(&self) -> Option<serde_json::Value> { None }

    async fn configure(&mut self, config: PluginConfig) -> Result<()>;
    async fn schema(&mut self) -> Result<Schema>;
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
    async fn cancel(&mut self);
}
```

| Method | When called | What to do |
|---|---|---|
| `plugin_name()` | During handshake | Return the binary name, `"loadsmith-source-{type}"` (convention, shown in the run summary) |
| `plugin_version()` | During handshake | Return the semver version string |
| `capabilities()` | During handshake | Override to add `"incremental_state"` if you support resume |
| `configure(config)` | After protocol negotiation | Parse with `config.parse::<YourConfig>()?` (see [Configuration](#configuration)), validate, open connection |
| `resume_from(cursor)` | After `start`, before `schema()` | Stash the opaque resume watermark (incremental sources) |
| `schema()` | After `start` signal | Return the Arrow schema of the rows you will emit |
| `next_batch()` | Repeatedly, until done | Return the next `RecordBatch`, or `Ok(None)` when exhausted |
| `current_watermark()` | After each batch | Return the high cursor value so far — the core persists it (durability-gated) |
| `cancel()` | The run will not complete (after `start`): the core sent `cancel`/`error`, the core is gone, or the source itself failed | Stop producing and release the cursor/connection. On `cancel`, an in-flight `next_batch()` future may have been dropped first |

> **Incremental sources.** Advertise `incremental_state`, store the value from
> `resume_from`, build `WHERE cursor > value ORDER BY cursor` yourself (the core
> never builds queries), and return the running max from `current_watermark`.
> Declare any cursor lazily — `resume_from` is delivered *after* `configure`. See
> [Incremental State](../architecture/incremental-state.md).

## Configuration

`configure` receives a `PluginConfig`: the plugin's `config:` block (after
template resolution) and the operator's unknown-keys policy. Parse it with
`PluginConfig::parse` into your own `Deserialize` type. Every plugin kind does
it the same way, so a typo behaves the same in every plugin:

```rust
use loadsmith_plugin_sdk::PluginConfig;
use serde::Deserialize;

#[derive(Deserialize)]
struct MyConfig {
    host: String,
    #[serde(default)]
    port: Option<u16>,
    #[serde(flatten)]
    tls: TlsOptions, // flattened structs are checked too
}

async fn configure(&mut self, config: PluginConfig) -> Result<()> {
    let cfg: MyConfig = config.parse()?;
    // validate, connect, keep `cfg`
    Ok(())
}
```

`parse` finds every key your type does not use, at any depth: nested structs,
arrays of structs, enum variants, and next to or inside `#[serde(flatten)]`
structs (where serde's own `deny_unknown_fields` does not work). What happens
then is the operator's choice, not the plugin's:

- **By default** the config is rejected: `configure_ack` `error` with code
  `UNKNOWN_CONFIG_KEYS` and a message listing every unknown key path, with a
  suggestion when one is a likely typo (`` `tls.verfy` (did you mean
  `tls.verify`?) ``). The run exits `3`.
- **With `loadsmith run --ignore-unknown-keys`** (or
  `LOADSMITH_IGNORE_UNKNOWN_KEYS=1`) the SDK logs one warning per unknown key
  on the event channel and the run continues.

A config that does not have the expected shape (a missing field, a value of
the wrong type) is rejected under either policy with code `INVALID_CONFIG`
and the path of the bad value.

Rules for config types:

- **Don't** put `#[serde(deny_unknown_fields)]` on them: it would make the
  `--ignore-unknown-keys` policy impossible. `parse` does that job.
- A free-form part (`HashMap<String, String>`, `serde_json::Value`) accepts
  any key, as it should.
- `config.unknown_keys()` returns the policy; `config.value()` (or `*config`,
  it derefs to `serde_json::Value`) the raw block; `loadsmith_plugin_sdk::config::parse`
  is the same check as a free function, returning the ignored keys.

## Step-by-step example: MySQL source

> A full MySQL connector already ships in
> [`loadsmith-canonical-plugins/mysql`](https://github.com/loadsmith-el/loadsmith-canonical-plugins)
> (package `loadsmith-mysql`, source + destination). Use it as the real
> reference; this walkthrough is a minimal skeleton of the same shape. See also
> the canonical book's
> [Plugin Anatomy](https://loadsmith-el.github.io/loadsmith-canonical-plugins/anatomy.html)
> and [Building](https://loadsmith-el.github.io/loadsmith-canonical-plugins/building.html)
> pages.

### 1. Create the crate

Plugins don't live in the loadsmith repo: create the package in your own repo or
as a member of `loadsmith-canonical-plugins`.

```
mysql-source/
  Cargo.toml
  src/
    main.rs
    plugin.rs
    types.rs   (optional — for type mapping)
```

(A connector that ships both a source and a destination uses a shared `lib` plus
`src/bin/source.rs` and `src/bin/destination.rs`, like the canonical `mysql`.)

### 2. `Cargo.toml`

The SDK crates are git dependencies pinned by `rev`, and
`[package.metadata.loadsmith]` is the install metadata a release turns into the
published `loadsmith-plugin.yaml` — without it the plugin can't be installed with
`loadsmith plugin install`.

```toml
[package]
name = "loadsmith-source-mysql"
version = "0.1.0"
edition = "2021"

[package.metadata.loadsmith]
name     = "mysql"
summary  = "MySQL source"
protocol = "^1"
provides = [
    { kind = "source", bin = "loadsmith-source-mysql" },
]

[[bin]]
name = "loadsmith-source-mysql"
path = "src/main.rs"

[dependencies]
tokio        = { version = "1", features = ["full"] }
anyhow       = "1"
tracing      = "0.1"
serde        = { version = "1", features = ["derive"] }
serde_json   = "1"
async-trait  = "0.1"
arrow        = "54"
arrow-array  = "54"
arrow-schema = "54"
loadsmith-plugin-sdk = { git = "https://github.com/loadsmith-el/loadsmith", rev = "<commit>" }
loadsmith-arrow      = { git = "https://github.com/loadsmith-el/loadsmith", rev = "<commit>" }

# Add your MySQL driver (the canonical connector uses pure-Rust mysql_async):
mysql_async = { version = "0.37", default-features = false, features = ["minimal-rust", "default-rustls-no-provider"] }
```

(Inside `loadsmith-canonical-plugins`, use `{ workspace = true }`; the workspace
root pins the SDK `rev` once for every plugin.)

### 3. `src/main.rs`

```rust
mod plugin;
mod types;

#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_source(plugin::MysqlPlugin::new()).await
}
```

### 4. `src/plugin.rs`

```rust
use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use loadsmith_plugin_sdk::{PluginConfig, SourcePlugin};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
struct MysqlConfig {
    host: String,
    port: u16,
    dbname: String,
    user: String,
    password: String,
    query: String,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
}

fn default_batch_size() -> usize { 1000 }

pub struct MysqlPlugin {
    config: Option<MysqlConfig>,
    schema: Option<Arc<Schema>>,
    // connection, cursor fields...
}

impl MysqlPlugin {
    pub fn new() -> Self {
        Self { config: None, schema: None }
    }
}

#[async_trait]
impl SourcePlugin for MysqlPlugin {
    fn plugin_name(&self) -> &str {
        "loadsmith-source-mysql"
    }

    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: PluginConfig) -> Result<()> {
        let cfg: MysqlConfig = config.parse()?;

        // Open connection, validate credentials, prepare query.
        // If anything fails here, return Err — the core will abort cleanly.
        // Store resolved state in self.
        self.config = Some(cfg);
        Ok(())
    }

    async fn schema(&mut self) -> Result<Schema> {
        // Return the Arrow schema of the rows you will emit.
        // Called once, immediately after start.
        // Must match every RecordBatch you return from next_batch().
        todo!("derive schema from query result metadata")
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        // Fetch up to batch_size rows.
        // Return Ok(Some(batch)) while there is data.
        // Return Ok(None) when the cursor is exhausted.
        // Never return Ok(None) and then Ok(Some(...)) again.
        todo!("fetch next batch")
    }

    async fn cancel(&mut self) {
        // Close cursor, rollback transaction, disconnect.
        // Called when the run aborts (or after our own failure): stop
        // producing and release the cursor / connection. An in-flight
        // `next_batch()` may already have been dropped.
        // Return quickly — do not block.
    }
}
```

### 5. Build and verify

```bash
cargo build --release    # → target/release/loadsmith-source-mysql
```

## Implementing `schema()` correctly

`schema()` is a contract. Every `RecordBatch` you return from `next_batch()` must
exactly match this schema:
- Same field names
- Same order
- Same Arrow types
- Same nullability flags

A mismatch corrupts the IPC stream and will cause the destination to fail or
produce wrong data. Derive the schema from query metadata if your driver supports
it, or hardcode it from the table DDL during development.

Use `loadsmith_arrow::schema_from_protocol_fields` to convert a vec of protocol
`Field` structs to an Arrow `Schema`:

```rust
use loadsmith_arrow::schema_from_protocol_fields;
use loadsmith_protocol::{Field, FieldType};

let fields = vec![
    Field::new("id", FieldType::Utf8).with_nullable(false),
    Field::new("amount", FieldType::Decimal128 { precision: 12, scale: 2 }),
];
let schema = schema_from_protocol_fields(&fields);
```

You never build the `schema` message yourself: the SDK derives it from the
Arrow `Schema` you return, exactly (types, units, time zones, precision,
nullability and nested children). See the
[type vocabulary](../protocol/messages.md#schema).

This needs `loadsmith-protocol` as a dependency too (a git dep from the same repo
and `rev` as `loadsmith-plugin-sdk`). Alternatively, build the Arrow `Schema`
directly with `arrow_schema` — which is what the canonical plugins do.

## Type mapping guidance

Arrow types cover most SQL types, but some require special handling:

| SQL type | Arrow type | Notes |
|---|---|---|
| SMALLINT, INT | `Int32` | |
| BIGINT | `Int64` | |
| REAL | `Float32` | |
| DOUBLE, FLOAT8 | `Float64` | |
| BOOLEAN | `Boolean` | |
| DATE | `Date32` | days since Unix epoch |
| TIMESTAMP | `Timestamp(Microsecond, None)` | pick the unit that matches the database's precision |
| TIMESTAMPTZ | `Timestamp(Microsecond, Some("UTC"))` | normalise to UTC |
| NUMERIC(p, s), DECIMAL(p, s) | `Decimal128(p, s)` | `Decimal256` above 38 digits |
| TIME | `Time64(Microsecond)` | |
| BYTEA, BLOB | `Binary` | raw bytes |
| arrays | `List(item)` | |
| VARCHAR, TEXT | `Utf8` | also a safe fallback for types you cannot map |

Every one of these is in the protocol's
[type vocabulary](../protocol/messages.md#schema), so the run log and the
schema fingerprint show the real type.

**Lesson from the Postgres source:** the binary wire protocol for some databases
does not decode `NUMERIC`, `DECIMAL`, or `TIME` out of the box. The Postgres
source uses `simple_query` (text protocol) and parses all values from their string
representation. This trades a small amount of parsing overhead for correctness
across all types. When in doubt, stringify and parse.

## Sending log events

You don't emit progress yourself: after each `next_batch()` the SDK sends a
`progress` event (and, for incremental sources, a `checkpoint` built from
`current_watermark()`) on fd4.

The SDK does not hand plugins a handle for custom fd4 `log` events today — no
`SourcePlugin` method receives an `EventSender`, and opening your own fd4 writer
would race the SDK's. For diagnostics, log with `tracing` to stderr, which the
plugin inherits from the core.

## Streaming, not slurping

`next_batch()` is called repeatedly in a loop. Return data in chunks of
`batch_size` rows. Never accumulate the entire dataset in memory before returning:

```rust
// WRONG — loads everything into memory
async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
    if self.all_rows.is_empty() {
        return Ok(None);
    }
    let batch = build_batch(&self.all_rows);
    self.all_rows.clear();
    Ok(Some(batch))
}

// RIGHT — fetch a page from the cursor
async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
    let rows = self.cursor.fetch(self.batch_size).await?;
    if rows.is_empty() {
        return Ok(None);
    }
    let batch = rows_to_batch(&rows, &self.schema)?;
    Ok(Some(batch))
}
```

## End-to-end testing

Do not wire your new plugin into a pipeline by hand to test it. The right
approach is a **loadsmith-lab** case:

1. Add a case under `cases/<name>/case.yaml` in
   `loadsmith-lab-canonical-catalog` (or point `--cases-dir` at a directory of
   ad-hoc cases). A `mysql-to-jsonl` case already exists there.
2. Reference a lab service image that seeds the database — MySQL already has
   `images/lab-mysql-8` in the `images` origin.
3. Run it against your local build with `--plugin`, which builds the crate as a
   static musl binary and overrides the cached canonical plugin (without it, the
   lab runs the published canonical plugins):
   `./target/debug/loadsmith-lab run --select catalog/mysql-to-jsonl --plugin ../loadsmith-canonical-plugins/mysql`

See [Testing with loadsmith-lab](../getting-started/testing-with-lab.md).
