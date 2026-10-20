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
| `host` | string or list | `localhost` | Postgres server hostname (a list for multi-node clusters) |
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

To see the fully resolved config with secrets masked (`--print-resolved-config`
prints and exits on its own, so `--dry-run` is not needed):

```bash
./target/debug/loadsmith run pipeline.yaml \
  --print-resolved-config
```

Output (the whole config, defaults included):
```yaml
pipeline:
  name: my-first-pipeline
  description: Export orders to JSONL
  heartbeat_seconds: 30
source:
  type: postgres
  config:
    host: localhost
    port: 5432
    dbname: mydb
    user: myuser
    password: ***
    query: SELECT * FROM orders ORDER BY id
    batch_size: 2000
destination:
  type: jsonl
  config:
    path: /tmp/orders.jsonl
sink: null
state: null
```

## Step 4 — Run

```bash
./target/debug/loadsmith run pipeline.yaml
```

You'll see:

```
loadsmith 0.2.0
postgres → jsonl

2026-10-20T14:03:08.398204Z  INFO loadsmith_core::lifecycle: configured plugin=source name=loadsmith-source-postgres version=0.2.1
…
2026-10-20T14:03:08.412204Z  INFO loadsmith_core::reporter: schema negotiated — 4 columns
2026-10-20T14:03:08.412204Z  INFO loadsmith_core::reporter: id: int64
2026-10-20T14:03:08.412204Z  INFO loadsmith_core::reporter: customer: utf8
2026-10-20T14:03:08.412204Z  INFO loadsmith_core::reporter: total: float64
2026-10-20T14:03:08.412204Z  INFO loadsmith_core::reporter: created_at: timestamp_ms
…
2026-10-20T14:03:08.731204Z  INFO loadsmith_core::reporter: 2,000 rows · 1 batch
2026-10-20T14:03:08.902204Z  INFO loadsmith_core::reporter: 4,000 rows · 2 batches
2026-10-20T14:03:09.288204Z  INFO loadsmith_core::reporter: 8,000 rows · 4 batches
2026-10-20T14:03:10.096204Z  INFO loadsmith_core::reporter: 16,000 rows · 8 batches
2026-10-20T14:03:11.371204Z  INFO loadsmith_core::reporter: 24,500 rows · 13 batches
…

───────────────────────────────────────────
Pipeline:     my-first-pipeline
Route:        loadsmith-source-postgres → loadsmith-destination-jsonl
Status:       success
Rows read:    24,500
Rows written: 24,500
Batches:      13
Duration:     00:00:03.012
Throughput:   8,134 rows/s
───────────────────────────────────────────
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

This prints the full handshake sequence to stderr (timestamps trimmed):

```
DEBUG loadsmith_core::lifecycle: → handshake plugin=source
DEBUG loadsmith_core::lifecycle: ← handshake_ack plugin=source name=loadsmith-source-postgres version=0.2.1 supported=[1]
DEBUG loadsmith_core::lifecycle: → set_protocol_version plugin=source protocol_version=1
DEBUG loadsmith_core::lifecycle: ← capabilities plugin=source supports=["batch_read", "schema_inference", "incremental_state"]
DEBUG loadsmith_core::lifecycle: → configure plugin=source
DEBUG loadsmith_core::lifecycle: ← configure_ack ok plugin=source
 INFO loadsmith_core::lifecycle: configured plugin=source name=loadsmith-source-postgres version=0.2.1
...
DEBUG loadsmith_core::lifecycle: → start plugin="source" has_resume=false
DEBUG loadsmith_core::lifecycle: ← schema plugin="source" fields=4
...
```

The `capabilities_request` the core sends is not logged; only the plugin's answer
is.

## Using `--no-color`

When redirecting stdout to a file or running in a CI environment where ANSI
escape codes appear as garbage:

```bash
loadsmith run pipeline.yaml --no-color
```

Or set the environment variable `NO_COLOR=1` (follows the [no-color.org](https://no-color.org) convention).
