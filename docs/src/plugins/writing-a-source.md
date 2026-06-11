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

    async fn configure(&mut self, config: serde_json::Value) -> Result<()>;
    async fn schema(&mut self) -> Result<Schema>;
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
    async fn cancel(&mut self);
}
```

| Method | When called | What to do |
|---|---|---|
| `plugin_name()` | During handshake | Return the binary name exactly: `"loadsmith-source-{type}"` |
| `plugin_version()` | During handshake | Return the semver version string |
| `capabilities()` | During handshake | Override to add `"incremental_state"` if you support resume |
| `configure(config)` | After protocol negotiation | Deserialize config, validate, open connection |
| `resume_from(cursor)` | After `start`, before `schema()` | Stash the opaque resume watermark (incremental sources) |
| `schema()` | After `start` signal | Return the Arrow schema of the rows you will emit |
| `next_batch()` | Repeatedly, until done | Return the next `RecordBatch`, or `Ok(None)` when exhausted |
| `current_watermark()` | After each batch | Return the high cursor value so far — the core persists it (durability-gated) |
| `cancel()` | On abort | Close cursors, rollback transactions |

> **Incremental sources.** Advertise `incremental_state`, store the value from
> `resume_from`, build `WHERE cursor > value ORDER BY cursor` yourself (the core
> never builds queries), and return the running max from `current_watermark`.
> Declare any cursor lazily — `resume_from` is delivered *after* `configure`. See
> [Incremental State](../architecture/incremental-state.md).

## Step-by-step example: MySQL source

### 1. Create the crate

```
plugins/sources/mysql/
  Cargo.toml
  src/
    main.rs
    plugin.rs
    types.rs   (optional — for type mapping)
```

### 2. `Cargo.toml`

```toml
[package]
name = "loadsmith-source-mysql"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "loadsmith-source-mysql"
path = "src/main.rs"

[dependencies]
tokio       = { workspace = true }
anyhow      = { workspace = true }
tracing     = { workspace = true }
serde       = { workspace = true }
serde_json  = { workspace = true }
async-trait = { workspace = true }
arrow       = { workspace = true }
arrow-array = { workspace = true }
arrow-schema = { workspace = true }
loadsmith-plugin-sdk = { path = "../../../crates/loadsmith-plugin-sdk" }
loadsmith-arrow      = { path = "../../../crates/loadsmith-arrow" }

# Add your MySQL driver:
sqlx = { version = "0.8", features = ["mysql", "runtime-tokio"] }
```

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
use loadsmith_plugin_sdk::SourcePlugin;
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

    async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
        let cfg: MysqlConfig = serde_json::from_value(config)?;

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
        // This is called on Ctrl+C or when the destination fails.
        // Return quickly — do not block.
    }
}
```

### 5. Register in `Cargo.toml` (workspace root)

```toml
[workspace]
members = [
    # ... existing members ...
    "plugins/sources/mysql",
]
```

### 6. Build and verify

```bash
cargo build -p loadsmith-source-mysql
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
use loadsmith_protocol::Field;

let fields = vec![
    Field { name: "id".into(), field_type: FieldType::Utf8 },
    Field { name: "amount".into(), field_type: FieldType::Float64 },
];
let schema = schema_from_protocol_fields(&fields)?;
```

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
| TIMESTAMP | `Timestamp(Millisecond, None)` | millis since Unix epoch |
| BYTEA, BLOB | `Binary` | raw bytes |
| VARCHAR, TEXT, NUMERIC, DECIMAL, TIME | `Utf8` | stringify everything else |

**Lesson from the Postgres source:** the binary wire protocol for some databases
does not decode `NUMERIC`, `DECIMAL`, or `TIME` out of the box. The Postgres
source uses `simple_query` (text protocol) and parses all values from their string
representation. This trades a small amount of parsing overhead for correctness
across all types. When in doubt, stringify and parse.

## Sending log events

Use `EventSender` (provided by the SDK) to send log events and progress from
within `next_batch()`:

```rust
async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
    let batch = fetch_rows()?;
    // The SDK provides an EventSender before calling your methods.
    // Access it via self.events if you stored it from the SDK's call.
    Ok(Some(batch))
}
```

The SDK passes an `EventSender` to lifecycle methods via the internal driver.
Progress events are emitted by calling `events.progress_source(rows, batches)`.
For simple log messages: `events.info("cursor opened").await`.

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

1. Create `../loadsmith-lab/cases/mysql-to-jsonl/case.yaml`
2. Create a lab service image that seeds a MySQL database
3. Run: `./target/debug/loadsmith-lab run --local --select mysql-to-jsonl`

See [Testing with loadsmith-lab](../getting-started/testing-with-lab.md).
