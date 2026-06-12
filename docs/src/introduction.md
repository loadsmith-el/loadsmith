# Introduction

Loadsmith is a **plugin-first EL (Extract & Load) tool** written in Rust. Its core
is intentionally small: it orchestrates process-isolated plugins — sources,
destinations, and config providers — that move data in Apache Arrow batches from
one place to another.

## What problem it solves

Extracting data from databases, APIs, and file systems, then loading it into
warehouses, object stores, and queues, is a universal problem. Most tools in this
space are either monolithic (everything built-in, no extension model) or rely on
shared-process plugins (a misbehaving plugin can take down the whole runtime).

Loadsmith takes a different approach: **every source and every destination is a
separate OS process**. The core binary is a thin orchestrator that wires them
together, pumps data between them, and enforces the protocol. A plugin that
crashes, leaks memory, or blocks indefinitely can't harm the orchestrator or any
other plugin.

## Design goals

- **Process isolation** — plugins run as child processes connected via file
  descriptors. A plugin failure is observable and recoverable; it cannot corrupt
  the core.

- **Arrow-native** — data moves as [Apache Arrow IPC](https://arrow.apache.org/docs/format/IPC.html)
  batches on a dedicated file descriptor. Arrow is the lingua franca of modern
  analytics; every plugin works in the same in-memory format with no conversion.

- **Schema-first** — the source declares a full Arrow schema before any data
  flows. The core and the destination see the schema before the first batch,
  enabling validation, transformation (future), and typed output without
  schema inference.

- **Composable by configuration** — a `pipeline.yaml` file declares the source
  type, destination type, and their configurations. No code is needed to connect
  two existing plugins.

- **Minimal core** — loadsmith itself does not know about Postgres, JSONL, S3,
  or any other system. All domain logic lives in plugins. The core only knows
  about the protocol.

## What loadsmith is NOT

- **Not a transformation engine.** Loadsmith is EL, not ETL. Heavy SQL joins,
  aggregations, and analytical transformations belong in the database or in a
  query layer on top of the loaded data. The core will never embed DuckDB or dbt.

- **Not a scheduler.** Loadsmith runs a single pipeline on demand. Scheduling,
  incremental loads, and orchestration are concerns for the operator (cron,
  Airflow, whatever).

- **Not a streaming platform.** Each `loadsmith run` is a bounded job with a
  clear start and end. Continuous/CDC streaming is a future concern and will
  arrive as a protocol extension, not as a core rewrite.

## Key properties at a glance

| Property | Detail |
|---|---|
| Language | Rust (2021 edition) |
| Data format | Apache Arrow IPC (streaming variant) |
| Control protocol | JSONL (newline-delimited JSON) |
| Plugin isolation | Separate OS process per plugin |
| Plugin discovery | Binary named `loadsmith-{kind}-{type}` in plugin dir |
| Plugin dir default | `~/.loadsmith/plugins/` |
| Protocol version | 1 (current) |

## A pipeline in 30 seconds

```yaml
# pipeline.yaml
pipeline:
  name: pg-to-jsonl

source:
  type: postgres
  config:
    host: localhost
    port: 5432
    dbname: mydb
    user: myuser
    password: "{{ env('PG_PASSWORD') }}"
    query: "SELECT * FROM orders WHERE created_at > '2024-01-01'"
    batch_size: 2000

destination:
  type: jsonl
  config:
    path: /data/orders.jsonl
```

```
$ loadsmith run pipeline.yaml

Loadsmith v0.1.0  ·  postgres → jsonl
  batch   1   2,000 rows
  batch   2   4,000 rows
  batch   4   8,000 rows
  batch   8  16,000 rows
  batch  16  24,500 rows

─────────────────────────────────────────────────────
Pipeline:     pg-to-jsonl
Route:        loadsmith-source-postgres → loadsmith-destination-jsonl
Status:       success
Rows read:    24,500
Rows written: 24,500
Batches:      16
Duration:     0:00:03
Throughput:   8,166 rows/s
─────────────────────────────────────────────────────
```

The `{{ env('PG_PASSWORD') }}` template is resolved by the core before any plugin
sees the config. The raw value is masked in logs and in `--print-resolved-config`
output.
