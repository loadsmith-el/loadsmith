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

## Plugin-specific config reference

### Postgres source (`type: postgres`)

#### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | string or list | `"localhost"` | Server hostname(s). Use a list for multi-node clusters (see `target_session_attrs`). |
| `port` | integer | `5432` | TCP port. Shared by all hosts in a multi-host list. |
| `dbname` | string | — | Database name. |
| `user` | string | — | Login role. |
| `password` | string | — | Login password. Supports `{{ env(...) }}` template. |
| `query` | string | — | Any valid `SELECT` statement, including CTEs and window functions. |
| `batch_size` | integer | `1000` | Rows per `FETCH FORWARD n`. |
| `application_name` | string | — | Appears in `pg_stat_activity`. Useful for identifying pipelines in monitoring. |
| `connect_timeout` | duration | — | Maximum time to establish a connection. E.g. `10s`, `500ms`. |
| `statement_timeout` | duration | — | Kills the query if it exceeds this wall time. Applied via `set_config`. E.g. `30m`. |
| `options` | map | — | Extra GUC parameters passed in the startup message (e.g. `search_path: myschema`). |
| `tls` | object | — | TLS block — see [TLS section](#tls) below. |
| `channel_binding` | `disable` / `prefer` / `require` | — | SCRAM channel-binding mode. `require` ties authentication to the TLS session, preventing credential relay. |
| `target_session_attrs` | `any` / `read-write` / `read-only` | — | With multi-host, controls which node to accept. Useful for pointing at a replica. |
| `transaction_isolation` | `read-committed` / `repeatable-read` / `serializable` | — | Isolation level for the cursor transaction. Default (`read-committed`) takes per-statement snapshots. `repeatable-read` gives a consistent view of the whole extraction. |
| `session` | map | — | Postgres GUC parameters applied via `set_config($1, $2, false)` after connecting. Safe (bind parameters, no SQL interpolation). E.g. `work_mem: 256MB`. |
| `tcp_keepalives` | object | — | TCP keepalive settings — see below. |
| `incremental` | object | — | Activates incremental reads. Requires a pipeline `state:` block — see below. |

#### `incremental`

When present, the source wraps `query` as `SELECT * FROM (<query>) AS _ls WHERE
_ls."<cursor_column>" > <watermark> ORDER BY _ls."<cursor_column>" ASC`, so the
last row of each batch carries the new high watermark (reported to the core).

| Field | Type | Description |
|---|---|---|
| `cursor_column` | string | Ordered, monotonically-advancing column to resume on (e.g. an `updated_at` timestamp). Must be in the query's output. |
| `initial_value` | string | Watermark for the very first run (before any state). Omit to read everything on the first run. |

> **Cursor choice matters.** Use a column that captures both inserts and updates
> (an `updated_at` timestamp, not a bare integer PK). The boundary is compared
> with `>`; rows sharing the exact watermark value, plus in-flight transactions,
> are the classic at-least-once hazard — pair with an idempotent destination, or
> have the source subtract a safety lag.

#### `tcp_keepalives`

| Field | Type | Description |
|---|---|---|
| `idle` | duration | Idle time before the first keepalive probe. E.g. `30s`. |
| `interval` | duration | Time between consecutive probes. E.g. `10s`. |
| `retries` | integer | Number of failed probes before the connection is dropped. |

#### TLS

The `tls` block is optional. Omitting it means no TLS (`mode: disable`). All certificates are optional beyond what each mode requires.

| Field | Type | Description |
|---|---|---|
| `mode` | `disable` / `prefer` / `require` / `verify-ca` / `verify-full` | TLS level. Default: `disable`. |
| `root_cert` | string (PEM) | CA certificate for server verification. Required only for `verify-ca` and `verify-full`. Use `{{ file(...) }}` to load from disk. |
| `client_cert` | string (PEM) | Client certificate for mutual TLS (mTLS). Optional. |
| `client_key` | string (PEM) | Private key for the client certificate. Required when `client_cert` is set. |

**TLS modes explained:**

| Mode | What it does |
|---|---|
| `disable` | No TLS. |
| `prefer` | TLS if the server supports it; plaintext fallback. |
| `require` | Encrypted channel required. No certificate checks. Equivalent to `useSSL=true` in JDBC. **No certificate fields needed.** |
| `verify-ca` | Encrypts + verifies the certificate chain against `root_cert`. Hostname not checked. |
| `verify-full` | Encrypts + verifies chain AND server hostname. Strictest mode. |

#### Type mapping

| PostgreSQL type | Arrow type | Notes |
|---|---|---|
| `SMALLINT`, `INT`, `INT4` | `Int32` | |
| `BIGINT`, `INT8` | `Int64` | |
| `REAL`, `FLOAT4` | `Float32` | |
| `DOUBLE PRECISION`, `FLOAT8` | `Float64` | |
| `BOOLEAN` | `Boolean` | |
| `DATE` | `Date32` | |
| `TIMESTAMP`, `TIMESTAMPTZ` | `Timestamp(Millisecond, None)` | |
| `BYTEA` | `Binary` | |
| `anytype[]` (arrays) | `List(inner)` | Inner type follows same mapping. `NULL` elements preserved. |
| `hstore` | `Utf8` (JSON) | Converted from `"k"=>"v"` to `{"k":"v"}`. Null values become JSON `null`. |
| Everything else (`NUMERIC`, `TEXT`, `UUID`, `TIME`, …) | `Utf8` | Text as returned by the Postgres text protocol. |

#### Connection behavior

The query is executed via a server-side cursor (`DECLARE ... CURSOR FOR`) inside a transaction:
- Any valid `SELECT` works — joins, CTEs, window functions, etc.
- `transaction_isolation: repeatable-read` gives a consistent snapshot across all `FETCH` calls.
- Rows are streamed in `batch_size` chunks, so large results never buffer fully in memory.
- Very large sorts or hash operations may still need `work_mem` tuned — use the `session` map to set it per-pipeline without touching the server's global config.

#### Examples

**Minimal (dev local):**
```yaml
source:
  type: postgres
  config:
    host: localhost
    dbname: lab
    user: lab
    password: "{{ env('PG_PASSWORD') }}"
    query: "SELECT * FROM events WHERE created_at >= now() - interval '1 day'"
```

**Encrypt without certificates (`useSSL=true` equivalent):**
```yaml
source:
  type: postgres
  config:
    host: legacy-db.internal
    dbname: billing
    user: loadsmith_reader
    password: "{{ env('PG_PASSWORD') }}"
    query: "SELECT * FROM invoices WHERE status = 'closed'"
    tls:
      mode: require
```
`mode: require` alone encrypts the channel — no certificates needed. This is the direct equivalent of `useSSL=true`/`sslmode=require` in JDBC.

**Managed cloud DB with strict TLS and session tuning:**
```yaml
source:
  type: postgres
  config:
    host: prod-db.cluster-xyz.us-east-1.rds.amazonaws.com
    dbname: analytics
    user: loadsmith_reader
    password: "{{ env('PG_PASSWORD') }}"
    query: "SELECT * FROM orders WHERE updated_at >= '{{ var('since') }}'"
    application_name: loadsmith-orders-export
    connect_timeout: 10s
    statement_timeout: 30m
    tls:
      mode: verify-full
      root_cert: "{{ file('/etc/ssl/certs/rds-ca-bundle.pem') }}"
    channel_binding: require
    session:
      work_mem: 256MB
      lock_timeout: 30s
      idle_in_transaction_session_timeout: 300s
```

**HA cluster with replicas, mTLS, and consistency guarantees:**
```yaml
source:
  type: postgres
  config:
    host: ["pg-node-a.internal", "pg-node-b.internal", "pg-node-c.internal"]
    port: 5432
    dbname: warehouse
    user: loadsmith_replica_reader
    password: "{{ env('PG_PASSWORD') }}"
    query: "SELECT * FROM shipments_view"
    target_session_attrs: read-only
    transaction_isolation: repeatable-read
    application_name: loadsmith-shipments-nightly
    tls:
      mode: verify-ca
      root_cert: "{{ file('/etc/ssl/certs/internal-ca.crt') }}"
      client_cert: "{{ file('/etc/ssl/certs/loadsmith-client.crt') }}"
      client_key: "{{ file('/etc/ssl/private/loadsmith-client.key') }}"
    tcp_keepalives:
      idle: 30s
      interval: 10s
      retries: 3
    batch_size: 5000
```

### JSONL destination (`type: jsonl`)

```yaml
destination:
  type: jsonl
  config:
    path: /output/data.jsonl  # string, optional — omit to write to stdout
```

Each row is written as a single-line JSON object followed by a newline. Column
values follow JSON types: strings as JSON strings, integers as JSON numbers,
booleans as `true`/`false`, nulls as `null`.

Arrow `Timestamp` and `Date32` columns are written as ISO 8601 strings.
Arrow `Binary` columns are written as base64-encoded strings.

### Parquet destination (`type: parquet`)

Writes Apache Parquet files to a local directory, with a configurable
compression codec and optional size-based file splitting.

```yaml
destination:
  type: parquet
  config:
    path: /output           # directory the files are written into (must exist)
    prefix: events          # filename prefix
    compression: snappy     # optional, default: snappy
    max_file_size: "500KiB" # optional — omit to write a single file
```

| Field | Required | Description |
|---|---|---|
| `path` | yes | Output **directory** (must already exist — files are created inside it). |
| `prefix` | yes | Filename prefix, e.g. `events`. |
| `compression` | no | One of `snappy` (default), `gzip`, `zstd`, `lz4`, `uncompressed`. |
| `max_file_size` | no | Size cap per file as a Docker-style string. Omit ⇒ a single file. |

**`max_file_size` syntax.** A Docker-style human size string: `"500KiB"`,
`"10MB"`, `"2GiB"`. Binary suffixes (`KiB`/`MiB`/`GiB`) are 1024-based; SI
suffixes (`kB`/`MB`/`GB`) are 1000-based. A **bare number** (no suffix, e.g.
`"500"`) is interpreted as **KiB** — a raw-bytes default would be nonsensical
for Parquet, whose per-file footer overhead alone is multiple KiB. The minimum
accepted value is **64 KiB**; smaller values are rejected (not silently
clamped). This 64 KiB floor is a schema-agnostic sanity guardrail, not a hard
Parquet limit — below it a file would be almost entirely footer overhead.

**File naming.** The codec is always embedded in the name so the file is
self-describing:

- **Single file** (`max_file_size` omitted): `<prefix>.<compression>.parquet`
  — e.g. `events.snappy.parquet`.
- **Split** (`max_file_size` set): `<prefix>.<sequence>.<compression>.parquet`
  with an 8-digit zero-padded sequence starting at `1` — e.g.
  `events.00000001.snappy.parquet`, `events.00000002.snappy.parquet`, … Once a
  file passes the size cap it is finalized and the next one is opened. (At an
  extreme run of more than 99,999,999 chunks the sequence simply widens past 8
  digits, at which point lexical and numeric ordering disagree at that one
  boundary — academic at any realistic scale.)

Files are streamed to disk incrementally (one row group at a time); the whole
dataset is never buffered in memory, so a multi-gigabyte export is safe.

### Postgres destination (`type: postgres`)

Writes Arrow batches into a Postgres table via `COPY` (text format, symmetric
with the source's text-protocol read path so NUMERIC/TIME/TIMESTAMP round-trip
cleanly). Two commit modes:

```yaml
destination:
  type: postgres
  config:
    host: localhost
    dbname: warehouse
    user: loadsmith_writer
    password: "{{ env('PG_PASSWORD') }}"
    target_table: public.orders
    mode: staged_merge       # "atomic" (default) | "staged_merge"
    merge_key: [id]          # required for staged_merge
```

| Field | Type | Required | Description |
|---|---|---|---|
| connection fields | — | yes (host/port default `localhost`/`5432`) | Same connection block as the [postgres source](#postgres-source-type-postgres) — `host`/`port`/`dbname`/`user`/`password`, plus `tls` (all modes + mTLS), `channel_binding`, `target_session_attrs`, `tcp_keepalives`, `session`, timeouts. Shared verbatim. |
| `target_table` | string | yes | Destination table, optionally schema-qualified. Must already exist. |
| `mode` | `atomic` / `staged_merge` | no | Commit strategy. Default `atomic`. |
| `merge_key` | list of strings | for `staged_merge` | Key columns for the `MERGE` (the table's PK / natural key). |

The full [TLS block](#tls) (`disable`/`prefer`/`require`/`verify-ca`/`verify-full`
+ mTLS) works identically on the destination — e.g. `tls: { mode: require }`.

**`atomic`** — one transaction: `COPY` straight into the target, `COMMIT` at the
end. All-or-nothing; a crash leaves the target untouched. At-least-once when
combined with incremental state (a re-run reinserts the delta, so the target
needs no duplicates — best for append-only or truncate-load tables).

**`staged_merge`** — `COPY` into a temporary staging table, then in the same
transaction `MERGE` by `merge_key` into the target and `COMMIT`. The merge is
**idempotent by key**, so re-running the delta is harmless — this is the
exactly-once-effective mode. Requires Postgres 15+ (`MERGE`).

Durability is at the final `COMMIT`/swap, so the core persists the incremental
watermark at end of run. See [Incremental State](../architecture/incremental-state.md).

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
