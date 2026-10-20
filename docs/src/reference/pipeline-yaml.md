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
  timeouts:               # optional — how long the core waits for plugins
    handshake_seconds: int    # each handshake reply. Default: 60
    configure_seconds: int    # configure → configure_ack. Default: 300
    start_seconds: int        # start → schema / ready. Default: 600
    finish_seconds: int       # source data ended → its finished. Default: 300
    finalize_seconds: int     # destination input ended → its finished. Default: 21600
    cancel_grace_seconds: int # cancel → plugin exit, then kill. Default: 30
    stall_seconds: int        # opt-in: no batch for N s fails the run. Default: 0 (off)
    first_batch_seconds: int  # with stall_seconds on: budget for the first batch. Default: 3600

providers:                # optional — provider instances for secret() templates
  <instance>:             #   name: letters, digits, '_' and '-'
    type: string          #   required — provider plugin type → loadsmith-provider-<type>
    config:               #   optional — passed to the provider (env()/file() allowed,
      # provider fields   #     never secret())
    timeout_seconds: int  #   optional — budget from spawn to finished. Default: 60; 0 = none

source:
  type: string            # required — source plugin type (e.g., "postgres")
  config:                 # optional — arbitrary object passed to the source plugin
    # plugin-specific fields

parser:                   # optional — parses the files of a raw source
  type: string            #   parser plugin type → loadsmith-parser-<type>
  config:                 #   optional — passed to the parser
    # plugin-specific fields

destination:
  type: string            # required — destination plugin type (e.g., "jsonl")
  config:                 # optional — arbitrary object passed to the destination plugin
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

## Strictness: unknown keys

A key Loadsmith does not know is an **error** by default, not silently
ignored, anywhere in the file:

- **The file's own keys** (everything in the schema above, outside the
  `config:` blocks): an unknown key, such as a typo (`confg:`,
  `heartbeat_secs:`) or a key another program added (`x-generator:`), fails
  `run`, `validate` and `state show|rm` with exit `3`, before anything starts.
  The message lists every unknown key by path (`pipeline.timeouts.stall_secs`).
- **Each plugin's `config:`**: the core passes it through, and the plugin
  rejects a key it does not use, at any depth (`tls.verify_mode`,
  `tables[2].nmae`), when it is configured; the run exits `3` before any data
  moves. (Plugins built with an SDK older than this rule apply their own;
  most ignore unknown keys.)

There is no key to relax this in the file itself. The operator decides when
running it: with `loadsmith run --ignore-unknown-keys` (or
`LOADSMITH_IGNORE_UNKNOWN_KEYS=1`) every unknown key, in both places, is
logged as a warning and ignored, so a generated file with extra metadata runs
unedited. See [Unknown keys](cli.md#unknown-keys) for the flag, the
environment variable and their precedence.

## Field reference

### `pipeline`

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Pipeline identifier. Appears in progress output and the summary |
| `description` | string | no | Human-readable description. Informational only |
| `heartbeat_seconds` | integer | no | Interval of the "still running" heartbeat log emitted during the data pump. `0` disables it. Default: `30` |
| `timeouts` | object | no | Limits on how long the core waits for plugins. See [`pipeline.timeouts`](#pipelinetimeouts) |

### `pipeline.timeouts`

Every wait of the core on a plugin is bounded. When a limit is hit, the run is
aborted: the late plugin gets `error` (code `TIMEOUT`), the others `cancel`,
and the run exits `1` with the timeout as its root cause (see the
[abort rules](../protocol/reference.md#abort-rules)). All fields are optional;
`0` disables a limit.

| Field | Default | What it bounds |
|---|---|---|
| `handshake_seconds` | `60` | Each reply during the handshake and the capability exchange |
| `configure_seconds` | `300` | `configure` → `configure_ack`. Plugins often connect here |
| `start_seconds` | `600` | `start` → `schema` (source) or `ready` (destination, sink). A source may run its query before it knows its schema |
| `finish_seconds` | `300` | The source's data stream ended → its `finished` |
| `finalize_seconds` | `21600` (6 h) | The core ended the destination's input → its `finished`. Covers `finalize` — a final commit or a staged `MERGE` — so raise it for very large staged loads |
| `cancel_grace_seconds` | `30` | After `cancel`/`error` (or a signal), how long each plugin gets to undo its work and exit before its process group is killed. `0` kills at once |
| `stall_seconds` | `0` (off) | **Opt-in.** Fail the run when no batch passes through the pump for this long. Counts backpressure too: a destination that takes longer than this for one batch trips it |
| `first_batch_seconds` | `3600` | Only with `stall_seconds` on: the budget for the *first* batch, from the start of the pump. Separate and generous because some sources (e.g. `mysql`, `oracle`) buffer their result before they emit a batch. `0` leaves the first batch unbounded |

```yaml
pipeline:
  name: nightly-orders
  timeouts:
    stall_seconds: 300        # fail if the data plane stops moving for 5 min
    cancel_grace_seconds: 20  # stay inside a 30 s orchestrator grace period
```

> **Orchestrator grace periods.** On SIGTERM the engine cancels the plugins and
> waits up to `cancel_grace_seconds` before killing them. Keep it below your
> orchestrator's own grace period (`docker stop` waits 10 s by default — use
> `docker stop -t`; Kubernetes waits `terminationGracePeriodSeconds`, 30 s by
> default), or the engine itself is killed before the plugins finish rolling
> back.

### `source`

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | yes | Source plugin type. Resolved to binary `loadsmith-source-{type}` |
| `config` | object | no | Passed verbatim to the source plugin's `configure()` after template resolution. Defaults to empty; the plugin decides which of its fields are required |

### `providers`

Optional. Declares the **provider** instances whose values the pipeline reads
with [`secret()`](#-secretinstance-ref-). Each key is an instance name
(letters, digits, `_`, `-`); the same provider type may back several instances
with different configs.

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | yes | Provider plugin type. Resolved to binary `loadsmith-provider-{type}` |
| `config` | object | no | Passed to the provider's `configure()`. May use `env()` and `file()`, never `secret()` |
| `timeout_seconds` | integer | no | The instance's whole budget, from spawn to its `finished` (every reference included). Default `60`; `0` disables it. A timeout fails the run (exit `3`) |

```yaml
providers:
  creds:
    type: file
    config: { base_dir: /mnt/secrets }
source:
  type: postgres
  config:
    password: "{{ secret('creds', 'db.json#password') }}"
```

Only instances that some `secret()` call references are started; a declared,
unused instance is logged as a warning and never spawned. Provider instances
run before the state lock is taken and before any data plugin starts. The
pipeline-wide `pipeline.timeouts` (handshake, configure, start,
`cancel_grace_seconds`) apply to providers too, inside their own budget.

### `parser`

Optional. A parser turns the files a *raw* source produces (a source that
advertises `object_output`, such as `file`) into Arrow batches, which flow to
the destination like a regular source's. See
[Writing a Parser](../plugins/writing-a-parser.md).

```yaml
source: { type: file, config: { path: /mnt/s3/inbox, glob: "*.csv" } }
parser: { type: csv,  config: { delimiter: ",", header: true } }
destination: { type: postgres, config: { … } }
```

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | yes | Parser plugin type. Resolved to binary `loadsmith-parser-{type}` |
| `config` | object | no | Passed verbatim to the parser's `configure()` after template resolution |

- A `parser:` needs a raw source, and a raw source needs a `parser:`: either
  mismatch is a configuration error (exit `3`), reported before anything
  starts. A parser without the `parse` capability is exit `4`.
- Not combinable with `state:` in 1.0 (validation error); incremental loads
  through a parser are planned for 1.x. A `sink:` works as usual.
- Every file must have the stream's schema (`SCHEMA_MISMATCH` otherwise). No
  file at all is an empty load if the parser's config declares the columns,
  and a `NO_INPUT` failure otherwise.
- A raw source that downloads files gets a core-allocated `scratch_dir` in its
  config unless you set one; the core deletes only the directory it
  allocated, and only after a successful run.

### `destination`

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | yes | Destination plugin type. Resolved to binary `loadsmith-destination-{type}` |
| `config` | object | no | Passed verbatim to the destination plugin's `configure()` after template resolution. Defaults to empty; the plugin decides which of its fields are required |

### `sink`

Optional. Delivers the destination's finalized files to a remote target,
keeping format (the destination) separate from location (the sink). Only valid
with a destination that advertises the `object_output` capability (e.g.
`parquet`); attaching one to a database destination is a config error. See
[Sink Delivery](../architecture/sink-delivery.md).

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | yes | Sink plugin type. Resolved to binary `loadsmith-sink-{type}` |
| `config` | object | no | Passed verbatim to the sink plugin's `configure()` after template resolution. Defaults to empty; the plugin decides which of its fields are required |

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
| `on_schema_change` | `refuse` / `continue` | no | What to do if the source schema (the exact Arrow schema of its stream: names, order, types with their parameters, nullability) differs from the recorded state. Default `refuse`. |
| `checkpoint_interval` | integer | no | Persist the safe watermark at most once per this many durably-committed batches. `0` (default) ⇒ persist only at the end of a successful run. |

**Guarantee.** Loadsmith is **at-least-once**: on a crash the next run rereads
from the last *durable* watermark and may re-deliver the boundary. Pair it with
an **idempotent destination** (e.g. a database destination in `mode: merge`)
for exactly-once. The source requires the `incremental_state` capability;
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

Any string value anywhere in the file (including the `config` sections,
`pipeline.name`, `state.path`, …) can include template expressions; mapping keys
are never templates. Arguments are string literals in single or double quotes,
without escapes, and calls do not nest. An unknown expression or a malformed call
is an error that names its location (e.g. `source.config.password`); the old
`aws.sm(...)` form is retired (use a provider and `secret()`).

Resolution order, all before the state lock and before any data plugin starts:

1. the file is parsed and checked **statically** — the strict shape and the
   core-owned validation on the unresolved tree, every template's syntax, and
   the [`secret()` rules](#-secretinstance-ref-). Nothing is evaluated yet;
2. `env()`/`file()` everywhere are evaluated (an unset variable fails here,
   before any provider starts);
3. the referenced provider instances run and resolve each distinct
   `secret()` call;
4. one substitution pass replaces every call with its value, and the result is
   validated again.

### Masking

Every value produced by `env()`, `file()` or `secret()` is **masked** as `***`
wherever the engine prints text that could carry it: plugin log lines,
plugin stderr (piped through the core), plugin error messages (`configure_ack`,
`finished`, `resolved`), the abort reasons sent to plugins, the final `error:`
line, the run summary's `cause` and `--print-resolved-config`. A multi-line
value (a PEM key) is also masked line by line (every trimmed line of at least 8
characters), and its JSON-escaped spelling is masked too. Masking is substring
replacement: a very short value (`1`, `on`) masks every occurrence of that text,
and a value a plugin transforms before printing it (base64, URL-encoding) is not
recognised.

### `{{ env('VAR_NAME') }}`

Reads the value of environment variable `VAR_NAME`. The value is masked.

```yaml
source:
  type: postgres
  config:
    password: "{{ env('PG_PASSWORD') }}"
```

If the variable is not set, the pipeline fails before spawning any plugin.

### `{{ file('/path/to/file') }}`

Reads the contents of the file at `/path/to/file` as a UTF-8 string. The
contents are inserted with trailing whitespace and newlines trimmed. The value
is masked, like `env()`'s. Useful for reading key files, certificates, or
multi-line secrets.

```yaml
source:
  type: postgres
  config:
    tls:
      mode: verify-full
      root_cert: "{{ file('/etc/ssl/certs/pg-ca.crt') }}"
```

### `{{ secret('instance', 'ref') }}`

Resolved by the provider instance declared as `providers.<instance>`. `ref` is
opaque to the core — its syntax is the provider's (the `file` provider takes
`path[#key]`, e.g. `db.json#password`). The value is masked.

```yaml
providers:
  creds:
    type: file
    config: { base_dir: /mnt/secrets }
source:
  type: postgres
  config:
    user: "{{ secret('creds', 'db.json#user') }}"
    password: "{{ secret('creds', 'db.json#password') }}"
```

Rules:

- the instance must be declared under `providers:` — an undeclared one is a
  static error (`loadsmith validate` catches it, exit `3`);
- `secret()` is not allowed in `pipeline:`, `state:` or `providers:`: those
  blocks resolve without providers (`loadsmith state show|rm` read only
  `pipeline:` and `state:`, and a provider's own config is resolved before it
  starts);
- each distinct `(instance, ref)` pair is resolved once per run, however often
  it appears; values are kept in memory for the run only, never on disk;
- a reference the provider cannot resolve, a provider crash, a protocol error
  or a timeout fails the run before anything else starts (exit `3`); a provider
  binary that is not installed exits `4`.

### Combining templates

Multiple templates can appear in a single string:

```yaml
destination:
  type: jsonl
  config:
    path: "/data/{{ env('ENV') }}-{{ env('TABLE') }}.jsonl"
```

## Plugin `config:` blocks

The fields each plugin accepts **inside** its `source`/`destination`/`sink`
`config:` object are documented with the plugins themselves, in the canonical
plugins docs:

> **[Plugin Configuration Reference](https://loadsmith-el.github.io/loadsmith-canonical-plugins/config/overview.html)**
> — `postgres`, `mysql`, `oracle`, `sharepoint`, `jsonl`, `parquet`, `null`,
> `local-copy`, `file`.

This split is deliberate: a plugin's config is **opaque to the core** (passed
through verbatim after template resolution), so its schema lives where the
plugin is maintained — change a plugin, change its doc in the same repo. The
plugin checks it, unknown keys included, under the
[strictness](#strictness-unknown-keys) rule above. The
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
