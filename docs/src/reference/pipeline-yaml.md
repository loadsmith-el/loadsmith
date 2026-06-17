# Pipeline YAML Reference

A pipeline YAML file declares a data movement operation: which source to read
from, which destination to write to, and the configuration for each.

## Full schema

```yaml
pipeline:
  name: string            # required — identifier for logs and the summary box
  description: string     # optional — human-readable description
  heartbeat_seconds: int  # optional — interval of the "still running" log during
                          #   the data pump. 0 disables it. Default: 30

source:
  type: string            # required — source plugin type (e.g., "postgres")
  config:                 # required — arbitrary object passed to the source plugin
    # plugin-specific fields

destination:
  type: string            # required — destination plugin type (e.g., "jsonl")
  config:                 # required — arbitrary object passed to the destination plugin
    # plugin-specific fields

sink:                     # optional — delivery stage; only valid with a
  type: string            #   file-output destination (advertises object_output)
  config:                 # remote target config (bucket/prefix/dest/…)
    # plugin-specific fields

state:                    # optional — incremental state (core-owned watermark)
  backend: string         #   "local" (only backend today)
  path: string            #   state file path for this pipeline
  on_schema_change: string  # optional — "refuse" (default) | "continue"
  checkpoint_interval: int  # optional — persist at most once per N durably-
                            #   committed batches; 0 ⇒ only at end of run
```

## Field reference

### `pipeline`

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Pipeline identifier. Appears in progress output and the summary |
| `description` | string | no | Human-readable description. Informational only |
| `heartbeat_seconds` | integer | no | Interval of the "still running" heartbeat log emitted during the data pump. `0` disables it. Default: `30` |

### `source`

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | yes | Source plugin type. Resolved to binary `loadsmith-source-{type}` |
| `config` | object | yes | Passed verbatim to the source plugin's `configure()` after template resolution |

### `destination`

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | yes | Destination plugin type. Resolved to binary `loadsmith-destination-{type}` |
| `config` | object | yes | Passed verbatim to the destination plugin's `configure()` after template resolution |

### `sink`

Optional. Delivers the destination's finalized files to a remote target,
keeping format (the destination) separate from location (the sink). Only valid
with a destination that advertises the `object_output` capability (e.g.
`parquet`); attaching one to a database destination is a config error. See
[Sink Delivery](../architecture/sink-delivery.md).

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | yes | Sink plugin type. Resolved to binary `loadsmith-sink-{type}` |
| `config` | object | yes | Passed verbatim to the sink plugin's `configure()` after template resolution |

When a sink is present and the destination has no explicit `path`, the core
allocates a staging scratch dir (removed after delivery). Point the
destination's `path` at a mounted volume to stage very large outputs elsewhere;
a user-given path is left in place after the run.

### `state`

Optional. Enables **incremental loads**: the core persists a *watermark* (the
high value of an ordered cursor column) and hands it back on the next run, so the
source resumes where it left off. **The core owns the watermark and never
interprets it** — the source decides how to use it (the cursor column lives in
`source.config.incremental`, and the source builds the `WHERE`). See
[Incremental State](../architecture/incremental-state.md).

| Field | Type | Required | Description |
|---|---|---|---|
| `backend` | string | yes | State backend. Only `local` exists today (a state file on disk). |
| `path` | string | yes | Location of the state file for this pipeline. A sibling `<path>.lock` guards against concurrent runs. |
| `on_schema_change` | `refuse` / `continue` | no | What to do if the source schema differs from the recorded state. Default `refuse`. |
| `checkpoint_interval` | integer | no | Persist the safe watermark at most once per this many durably-committed batches. `0` (default) ⇒ persist only at the end of a successful run. |

**Guarantee.** Loadsmith is **at-least-once**: on a crash the next run rereads
from the last *durable* watermark and may re-deliver the boundary. Pair it with
an **idempotent destination** (e.g. the postgres destination in `staged_merge`
mode) for exactly-once. The source requires the `incremental_state` capability;
attaching `state:` to a source without it is a config error.

```yaml
state:
  backend: local
  path: .loadsmith/state/orders.json
  checkpoint_interval: 50
```

`loadsmith state show pipeline.yaml` prints the current watermark; `loadsmith
state rm pipeline.yaml` clears it (the next run is a full read).

## Template syntax

Any string value in the `config` sections can include template expressions.
Templates are resolved by the core before sending config to plugins.

### `{{ env('VAR_NAME') }}`

Reads the value of environment variable `VAR_NAME`. The variable is added to the
mask list; it will appear as `***` in `--print-resolved-config` output and in logs.

```yaml
source:
  type: postgres
  config:
    password: "{{ env('PG_PASSWORD') }}"
```

If the variable is not set, the pipeline fails before spawning any plugin.

### `{{ file('/path/to/file') }}`

Reads the contents of the file at `/path/to/file` as a UTF-8 string. The file
contents are used verbatim. Useful for reading key files, certificates, or
multi-line secrets.

```yaml
source:
  type: postgres
  config:
    ssl_cert: "{{ file('/etc/ssl/certs/pg.crt') }}"
```

### Combining templates

Multiple templates can appear in a single string:

```yaml
config:
  connection_string: "host={{ env('PG_HOST') }} port={{ env('PG_PORT') }} dbname=mydb"
```

## Plugin `config:` blocks

The fields each plugin accepts **inside** its `source`/`destination`/`sink`
`config:` object are documented with the plugins themselves, in the canonical
plugins docs:

> **[Plugin Configuration Reference](https://loadsmith-el.github.io/loadsmith-canonical-plugins/config/overview.html)**
> — `postgres`, `mysql`, `jsonl`, `parquet`, `null`, `local-copy`, `file`.

This split is deliberate: a plugin's config is **opaque to the core** (passed
through verbatim after template resolution), so its schema lives where the
plugin is maintained — change a plugin, change its doc in the same repo. The
sections above (top-level shape, templates, `state:`) are the engine's contract
and stay here.

## Full example

```yaml
pipeline:
  name: orders-export
  description: "Daily export of completed orders to JSONL for the analytics warehouse"

source:
  type: postgres
  config:
    host: "{{ env('PG_HOST') }}"
    port: 5432
    dbname: production
    user: readonly_user
    password: "{{ env('PG_PASSWORD') }}"
    query: |
      SELECT
        o.id,
        o.customer_id,
        o.total_amount,
        o.status,
        o.created_at,
        o.shipped_at
      FROM orders o
      WHERE o.status = 'completed'
        AND o.created_at >= '2024-01-01'
      ORDER BY o.created_at
    batch_size: 5000

destination:
  type: jsonl
  config:
    path: /data/orders-2024.jsonl
```

Run it:
```bash
PG_HOST=prod-db.internal \
PG_PASSWORD=secret \
loadsmith run orders.yaml --plugin-dir ~/.loadsmith/plugins
```
