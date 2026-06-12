# Your First Pipeline

This guide walks you through running a real pipeline: reading from PostgreSQL and
writing to a JSONL file.

## Prerequisites

- Loadsmith built: `cargo build`, and plugins installed:
  `loadsmith plugin install --all` (see [Installation](./installation.md))
- A running PostgreSQL instance with a table to read

## Step 1 — Write the pipeline

Create a file named `pipeline.yaml`:

```yaml
pipeline:
  name: my-first-pipeline
  description: "Export orders to JSONL"

source:
  type: postgres
  config:
    host: localhost
    port: 5432
    dbname: mydb
    user: myuser
    password: "{{ env('PG_PASSWORD') }}"
    query: "SELECT * FROM orders ORDER BY id"
    batch_size: 2000

destination:
  type: jsonl
  config:
    path: /tmp/orders.jsonl
```

### Config fields explained

**Source (`type: postgres`):**

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | string | — | Postgres server hostname |
| `port` | integer | `5432` | Postgres server port |
| `dbname` | string | — | Database name |
| `user` | string | — | Login user |
| `password` | string | — | Login password (use `{{ env(...) }}`) |
| `query` | string | — | SQL query to execute |
| `batch_size` | integer | `1000` | Rows fetched per batch |

**Destination (`type: jsonl`):**

| Field | Type | Default | Description |
|---|---|---|---|
| `path` | string | — | Output file path. Omit to write to stdout |

## Step 2 — Set the secret

The `{{ env('PG_PASSWORD') }}` template is resolved by the core before sending
config to the plugin. The raw value is masked in `--print-resolved-config` output.

```bash
export PG_PASSWORD="my-secret-password"
```

## Step 3 — Validate the config (dry run)

Before running, validate the pipeline without executing it:

```bash
./target/debug/loadsmith run pipeline.yaml \
  --dry-run
```

To see the fully resolved config with secrets masked:

```bash
./target/debug/loadsmith run pipeline.yaml \
  --dry-run \
  --print-resolved-config
```

Output:
```yaml
pipeline:
  name: my-first-pipeline
source:
  type: postgres
  config:
    host: localhost
    port: 5432
    dbname: mydb
    user: myuser
    password: "***"
    query: SELECT * FROM orders ORDER BY id
    batch_size: 2000
destination:
  type: jsonl
  config:
    path: /tmp/orders.jsonl
```

## Step 4 — Run

```bash
./target/debug/loadsmith run pipeline.yaml```

You'll see:

```
Loadsmith v0.1.0  ·  postgres → jsonl

  schema negotiated — 4 columns
    id: int64
    customer: utf8
    total: float64
    created_at: timestamp_ms

  batch   1    2,000 rows
  batch   2    4,000 rows
  batch   4    8,000 rows
  batch   8   16,000 rows
  batch  16   24,500 rows

─────────────────────────────────────────────────────
Pipeline:     my-first-pipeline
Route:        loadsmith-source-postgres → loadsmith-destination-jsonl
Status:       success
Rows read:    24,500
Rows written: 24,500
Batches:      16
Duration:     0:00:03
Throughput:   8,166 rows/s
─────────────────────────────────────────────────────
```

**Stdout** carries the human report's header and final summary box.
**Stderr** carries everything else, controlled by `--log-level` — tracing
diagnostics plus the timestamped `INFO` lines shown above: live progress and,
right after the source negotiates it, the schema (column names and
Arrow-compatible types, one line per column, before any data flows).

## Step 5 — Verify the output

```bash
wc -l /tmp/orders.jsonl        # should equal rows_written
head -1 /tmp/orders.jsonl | jq .
```

## Inspecting the protocol handshake

To see every control message exchanged between the core and each plugin:

```bash
./target/debug/loadsmith run pipeline.yaml \
  --log-level debug
```

This prints the full handshake sequence to stderr:

```
DEBUG loadsmith_core::lifecycle: → handshake (source)
DEBUG loadsmith_core::lifecycle: ← handshake_ack name=loadsmith-source-postgres version=0.1.0 supported=[1]
DEBUG loadsmith_core::lifecycle: → set_protocol_version version=1
DEBUG loadsmith_core::lifecycle: ← capabilities_response supports=[]
DEBUG loadsmith_core::lifecycle: → configure
DEBUG loadsmith_core::lifecycle: ← configure_ack ok
DEBUG loadsmith_core::lifecycle: → start
DEBUG loadsmith_core::lifecycle: ← schema fields=34
...
```

## Using `--no-color`

When redirecting stdout to a file or running in a CI environment where ANSI
escape codes appear as garbage:

```bash
loadsmith run pipeline.yaml --no-color```

Or set the environment variable `NO_COLOR=1` (follows the [no-color.org](https://no-color.org) convention).
