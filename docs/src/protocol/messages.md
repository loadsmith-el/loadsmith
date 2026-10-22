# Message Reference

All messages are JSON objects with a `"type"` field that acts as a discriminator.
The JSON is serialized in snake_case. Optional fields are omitted when not present
(never serialized as `null`).

## Handshake phase

### `handshake`

Sent by the **core** immediately after spawning the plugin.

```json
{"type": "handshake"}
```

No fields.

---

### `handshake_ack`

Sent by the **plugin** in response to `handshake`.

```json
{
  "type": "handshake_ack",
  "plugin_name": "loadsmith-source-postgres",
  "plugin_version": "0.2.1",
  "protocol_supported_versions": [1],
  "kind": "source",
  "telemetry": 1
}
```

| Field | Type | Description |
|---|---|---|
| `plugin_name` | string | Should match the binary name (convention, shown in the run summary). Not enforced by the core |
| `plugin_version` | string | Semver version string |
| `protocol_supported_versions` | array of integers | Protocol versions this plugin understands |
| `kind` | string | One of: `"source"`, `"destination"`, `"sink"`, `"parser"`, `"provider"`. A raw source is a `"source"`. The same spelling names the binary: `loadsmith-<kind>-<type>` |
| `telemetry` | integer (optional) | The [telemetry](#telemetry-on-the-event-channel) revision the plugin speaks (`1`). Absent: the plugin gets no `configure.telemetry` and sends no `span`/`metrics`. The Rust SDK always sends `1` |

---

### `set_protocol_version`

Sent by the **core** after receiving `handshake_ack`. The core picks the highest
version both sides support.

```json
{"type": "set_protocol_version", "protocol_version": 1}
```

| Field | Type | Description |
|---|---|---|
| `protocol_version` | integer | Negotiated version (currently always `1`) |

---

## Capabilities phase

### `capabilities_request`

Sent by the **core** after `set_protocol_version`.

```json
{"type": "capabilities_request"}
```

No fields.

---

### `capabilities_response`

Sent by the **plugin** in response to `capabilities_request`.

```json
{"type": "capabilities_response", "supports": ["batch_read", "schema_inference", "incremental_state"]}
```

| Field | Type | Description |
|---|---|---|
| `supports` | array of strings | Capability identifiers (see below) |

**Known capabilities:**

| Capability | Advertised by | Enforced by the core |
|---|---|---|
| `batch_read` | sources (SDK default) | no |
| `schema_inference` | sources (SDK default) | no |
| `incremental_state` | incremental sources (postgres, mysql, oracle) | yes — required when the pipeline has a `state:` block |
| `batch_write` | destinations (SDK default) | no |
| `object_output` | file-output destinations whose files a sink can deliver (parquet); also *raw sources*, which produce files instead of Arrow (see [`parse_object`](#parse_object)) | yes — required on the destination when the pipeline has a `sink:` block; on a source it requires a `parser:` block, and a `parser:` requires it |
| `scratch_dir` | raw sources that download their files | yes — the core adds a fresh `scratch_dir` path to the source's `config` (unless the user set one) and removes that directory after a successful run |
| `staged_merge` | postgres, mysql, oracle destinations | no |
| `object_delivery` | sinks (SDK default) | no |
| `resolve` | providers (SDK default) | yes — required of every provider instance |
| `parse` | parsers (SDK default) | yes — required of the pipeline's parser |

---

## Configuration phase

### `configure`

Sent by the **core** after `capabilities_response`. Contains the plugin's
configuration as an opaque JSON object taken from the pipeline YAML after
template resolution.

```json
{
  "type": "configure",
  "config": {
    "host": "localhost",
    "port": 5432,
    "dbname": "mydb",
    "user": "myuser",
    "password": "s3cr3t",
    "query": "SELECT * FROM orders",
    "batch_size": 2000
  },
  "unknown_keys": "error",
  "telemetry": {
    "enabled": true,
    "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
    "log_level": "info",
    "metrics_interval_ms": 15000
  },
  "run": {
    "id": "4242-1761134400000",
    "pipeline": "orders-to-s3",
    "started_at_unix_ms": 1761134400000
  }
}
```

| Field | Type | Description |
|---|---|---|
| `config` | object | Arbitrary JSON — the plugin's configuration |
| `unknown_keys` | string (optional) | What the plugin does with a key of `config` it does not recognise, at any depth: `"error"` (the default) rejects the config with `configure_ack` `error` (code `UNKNOWN_CONFIG_KEYS`), naming every unknown key path; `"warn"` logs one `warn` event per unknown key on fd4 and ignores it. Absent, or a value the plugin does not know, means `"error"` |
| `telemetry` | object (optional) | Sent only to a plugin that advertised `handshake_ack.telemetry`, and only when the run exports telemetry. `enabled` (bool); `traceparent` (optional, W3C: the core's span for this plugin process — the parent of the plugin's top-level spans; flag `00` = not sampled: record no spans); `tracestate` (optional, W3C, verbatim); `log_level` (minimum level of the `log` lines to send with trace context, default `info`); `metrics_interval_ms` (optional, how often to flush `metrics`). Absent, `enabled: false`, or a shape the plugin cannot read ⇒ the plugin records nothing (never a failed `configure`). See [telemetry](#telemetry-on-the-event-channel) |
| `run` | object (optional) | The run this plugin works for: `id` (the run's id, as in the state file and the run summary), `pipeline` (`pipeline.name`), `started_at_unix_ms` (when the run started, UTC). The same for every plugin of the run and across a plugin's respawns within it, so a sink can name what it delivers per run (a later run never overwrites an earlier one) and a re-delivery after a respawn lands on the same name. Sent to sources, parsers, destinations and sinks; not to providers (they run before the run starts). Absent (an older core) or unreadable ⇒ unknown, never a failed `configure`. The Rust SDK exposes it as `PluginConfig::run()` |

`unknown_keys` is how the operator's choice reaches every plugin: the core
sends `"warn"` when the run was started with `--ignore-unknown-keys` (or
`LOADSMITH_IGNORE_UNKNOWN_KEYS=1`), `"error"` otherwise. A plugin built with
the Rust SDK applies it by parsing its config with `PluginConfig::parse` (see
[Writing a source](../plugins/writing-a-source.md#configuration)). An older
plugin ignores the field (tolerant reader) and keeps its own behaviour; a
plugin in another language should implement the same rule. A core older than
the field never sends it, which a plugin reads as `"error"`.

---

### `configure_ack`

Sent by the **plugin** in response to `configure`.

**Success:**
```json
{"type": "configure_ack", "status": "ok"}
```

**Failure:**
```json
{
  "type": "configure_ack",
  "status": "error",
  "code": "CONFIGURE_FAILED",
  "message": "invalid postgres source config: missing field `dbname`"
}
```

| Field | Type | Description |
|---|---|---|
| `status` | string | `"ok"` or `"error"` |
| `code` | string (optional) | Machine-readable error code. Present only when `status` is `"error"`. The Rust SDK sends `UNKNOWN_CONFIG_KEYS` (unknown keys under the `error` policy), `INVALID_CONFIG` (a missing field, a value of the wrong type) or `CONFIGURE_FAILED` (any other error from `configure()`) |
| `message` | string (optional) | Human-readable error message. Present only when `status` is `"error"` |

---

## Execution phase

### `start`

Sent by the **core** after both plugins have sent `configure_ack: ok`.

```json
{"type": "start"}
{"type": "start", "resume": {"cursor_value": "2026-06-09T08:00:00Z"}}
```

The signal to begin execution. For an incremental source whose pipeline has a
watermark persisted from a previous run, the core includes `resume` with the
opaque `cursor_value` to resume after; the core never interprets it. The bare
form (no `resume`) is unchanged and still valid.

---

### `schema`

Sent by a **source** on the control channel (fd1) immediately after
receiving `start`, before writing any Arrow data to fd3. A **parser** sends it
right after `start` when its config declares the schema, otherwise after it
opened the first file (the core keeps sending `parse_object` meanwhile).

```json
{
  "type": "schema",
  "fields": [
    {"name": "id",         "type": "int64", "nullable": false},
    {"name": "amount",     "type": "decimal128", "precision": 12, "scale": 2, "nullable": true},
    {"name": "created_at", "type": "timestamp", "unit": "us", "timezone": "UTC", "nullable": true},
    {"name": "tags",       "type": "list",
                           "item": {"name": "item", "type": "utf8", "nullable": true},
                           "nullable": true}
  ]
}
```

| Field | Type | Description |
|---|---|---|
| `fields` | array of Field | The columns, in order |

**Field object.** A JSON object with the column's `name`, its `type`, the
type's parameters beside `type` in the same object, and `nullable`:

| Key | Type | Description |
|---|---|---|
| `name` | string | Column name. Required |
| `type` | string | A type name from the table below. Required |
| *(parameters)* | | Per type, see the table |
| `nullable` | bool | Whether the column may hold nulls. Optional; absent means `true` |

A child that is itself a field (a list `item`, a `struct` member, a `map`
`key`/`value`) is a full field object with its own `name` and `nullable`. A
child that is only a type (a `dictionary`'s `index` and `value`) is a **type
object**: a field object without `name` and `nullable`, such as
`{"type": "int32"}`.

**Types.** The names follow Arrow's logical types; each maps to exactly one
Arrow type:

| `type` | Parameters | Arrow type |
|---|---|---|
| `null` | — | `Null` |
| `bool` | — | `Boolean` |
| `int8`, `int16`, `int32`, `int64` | — | `Int8` … `Int64` |
| `uint8`, `uint16`, `uint32`, `uint64` | — | `UInt8` … `UInt64` |
| `float16`, `float32`, `float64` | — | `Float16` … `Float64` |
| `decimal128` | `precision` (1–38), `scale` (integer, may be negative; default 0) | `Decimal128(p, s)` |
| `decimal256` | `precision` (1–76), `scale` | `Decimal256(p, s)` |
| `utf8`, `large_utf8`, `utf8_view` | — | `Utf8`, `LargeUtf8`, `Utf8View` |
| `binary`, `large_binary`, `binary_view` | — | `Binary`, `LargeBinary`, `BinaryView` |
| `fixed_size_binary` | `byte_width` | `FixedSizeBinary(n)` |
| `date32` | — | `Date32` (days since 1970-01-01) |
| `date64` | — | `Date64` (milliseconds since the epoch) |
| `time32` | `unit`: `s` or `ms` | `Time32(unit)` |
| `time64` | `unit`: `us` or `ns` | `Time64(unit)` |
| `timestamp` | `unit`: `s`, `ms`, `us` or `ns`; `timezone` (optional: an IANA name such as `UTC` or an offset such as `+02:00`; absent means no time zone) | `Timestamp(unit, tz)` |
| `duration` | `unit`: `s`, `ms`, `us` or `ns` | `Duration(unit)` |
| `interval` | `unit`: `year_month`, `day_time` or `month_day_nano` | `Interval(unit)` |
| `list`, `large_list` | `item` (field) | `List`, `LargeList` |
| `fixed_size_list` | `item` (field), `size` | `FixedSizeList(item, size)` |
| `struct` | `fields` (array of fields) | `Struct` |
| `map` | `key` (field), `value` (field), `keys_sorted` (bool, default `false`) | `Map` (entries struct named `entries`) |
| `dictionary` | `index` (type object, an integer type), `value` (type object) | `Dictionary(index, value)` |

Arrow types outside this vocabulary (`union`, `run_end_encoded`, `list_view`,
`large_list_view`) are sent with that name and no parameters, and every
receiver treats them as unknown (below). Field metadata is not carried.

**Older plugins.** Plugins built with an SDK before this vocabulary send only
`name` and `type` (no `nullable`) and use the type names `int32`, `int64`,
`float32`, `float64`, `utf8`, `bool`, `date32`, `binary` and `timestamp_ms`.
All of them stay valid: `nullable` defaults to `true`, and `timestamp_ms` reads
as `{"type": "timestamp", "unit": "ms"}` (no time zone). A new plugin must not
send `timestamp_ms`.

**Tolerant reading.** The `schema` message is informational; the
authoritative schema is the Arrow IPC stream header on fd3 (see below). A
receiver therefore never fails a run over a field type:

- a `type` it does not know (from a newer vocabulary, or an Arrow type outside
  it) is read as *unknown*, keeping the raw name, and shown as
  `<name> (unknown)` in the run log;
- a known `type` with missing or invalid parameters (a `decimal128` without
  `precision`, a `time32` in `ns`) is read as unknown too;
- unknown extra keys are ignored.

Only a field that is not an object, or has no string `name` or `type`, makes
the message malformed (and subject to [`resend`](reference.md#malformed-lines-and-resend)).

New types and new optional parameters are added in minor releases, so a plugin
that emits a type newer than the engine still runs: the engine shows it as
unknown, and the data itself flows through the IPC stream untouched.

> **Authoritative schema.** The exact Arrow schema travels in the IPC stream
> header on fd3. The core reads that header before it starts the destination,
> and uses it for the run log and for the incremental-state
> [schema fingerprint](../architecture/incremental-state.md#state-file-format).
> The SDK derives the `schema` message from the same Arrow schema exactly. If a
> (non-SDK) plugin's message disagrees with its stream, the core logs a warning
> and goes by the stream.

---

### `ready`

Sent on the control channel (fd1) after receiving `start` by a **destination**
(ready to receive Arrow batches on fd3), a **sink** (ready for
`deliver_object`), a **provider** (ready for [`resolve`](#resolve)), or a
**raw source** (it answers `start` with `ready` instead of `schema` and then
announces files with [`object_ready`](#object_ready)).

```json
{"type": "ready"}
```

No fields. The core starts the data pump after the destination's `ready`.

---

## Event channel messages (fd4)

These messages are written by plugins to fd4 (not fd1). They are consumed by the
core concurrently with the data pump.

### `progress`

Progress update from a plugin.

**Source progress:**
```json
{
  "type": "progress",
  "rows_read": 4000,
  "batches_read": 2
}
```

**Destination progress:**
```json
{
  "type": "progress",
  "rows_written": 4000,
  "batches_written": 2
}
```

| Field | Type | Present on |
|---|---|---|
| `rows_read` | integer (optional) | source progress |
| `batches_read` | integer (optional) | source progress |
| `rows_written` | integer (optional) | destination progress |
| `batches_written` | integer (optional) | destination progress |

---

### `log`

Structured log event from a plugin.

```json
{"type": "log", "level": "info", "message": "cursor opened, fetching batches"}
{"type": "log", "level": "warn", "message": "slow query", "trace_id": "0af7651916cd43dd8448eb211c80319c",
 "span_id": "00f067aa0ba902b7", "time_unix_nano": 1792612760627164860, "attributes": {"db.namespace": "public"}}
```

| Field | Type | Values |
|---|---|---|
| `level` | string | `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"` |
| `message` | string | Human-readable message |
| `trace_id` | string (optional) | Telemetry: 32 hex digits, the run's trace |
| `span_id` | string (optional) | Telemetry: 16 hex digits, the plugin span (or the core's span for the plugin) the line belongs to |
| `time_unix_nano` | integer (optional) | When it happened |
| `attributes` | object (optional) | Structured fields (strings, booleans, numbers, arrays of one of those) |

The core logs `message` with every resolved `env()`/`file()`/`secret()` value
masked (`***`). A provider's log lines are held until its conversation is over
and masked then, so a value it logs before the core read its `resolved` is
masked too. With telemetry on, the core also exports the line as an OTLP log
record (masked, with its trace context; without one, the plugin process's
span's).

---

### Telemetry on the event channel

Part of protocol v1, additive: only a plugin that advertised
`handshake_ack.telemetry` and received `configure.telemetry.enabled: true`
sends these; a core that predates them skips them as unknown types. The core
is the only exporter: it re-emits them under the run's trace, masked. Design:
[Telemetry](../architecture/telemetry.md). Bound the volume: spans per
query, batch, request or file — never per row.

#### `span`

One finished span, sent when it closes.

```json
{"type": "span", "trace_id": "0af7651916cd43dd8448eb211c80319c", "span_id": "53995c3f42cd8ad8",
 "parent_span_id": "b7ad6b7169203331", "name": "SELECT orders", "kind": "client",
 "start_unix_nano": 1792612760600000000, "end_unix_nano": 1792612760627164860,
 "status": {"code": "error", "message": "connection reset"},
 "attributes": {"db.system.name": "postgresql", "db.namespace": "shop", "db.response.returned_rows": 2000},
 "events": [{"name": "retry", "time_unix_nano": 1792612760610000000, "attributes": {"attempt": 2}}],
 "links": [{"trace_id": "…", "span_id": "…", "attributes": {}}]}
```

| Field | Type | Description |
|---|---|---|
| `trace_id` | string | 32 lower-case hex digits: the trace of `configure.telemetry.traceparent` |
| `span_id` | string | 16 hex digits, unique in the trace; kept by the core (children and log lines refer to it) |
| `parent_span_id` | string (optional) | The parent: another span of this plugin, or the `traceparent` span. Absent: the core parents it on the plugin process's span |
| `name` | string | Span name (semantic-convention style: `SELECT orders`, `GET`, `loadsmith.batch`) |
| `kind` | string (optional) | `internal` (default), `client`, `server`, `producer`, `consumer`; unknown ⇒ `internal` |
| `start_unix_nano`, `end_unix_nano` | integer | Timestamps (ns since the Unix epoch) |
| `status` | object (optional) | `code`: `unset` (default), `ok`, `error` (unknown ⇒ `unset`); `message` with `error` |
| `attributes` | object (optional) | Attribute values: string, boolean, integer, float, or an array of one of those; anything else is dropped |
| `events` | array (optional) | `{name, time_unix_nano, attributes?}` |
| `links` | array (optional) | `{trace_id, span_id, attributes?}` |

#### `metrics`

Data points aggregated in the plugin since the previous `metrics` (delta
temporality), sent periodically (`configure.telemetry.metrics_interval_ms`) and
once more before the plugin exits.

```json
{"type": "metrics", "points": [
  {"name": "loadsmith.plugin.rows", "unit": "{row}", "kind": "counter", "temporality": "delta",
   "attributes": {"direction": "read"}, "value": 4000},
  {"name": "db.client.operation.duration", "unit": "s", "kind": "histogram", "temporality": "delta",
   "attributes": {"db.namespace": "shop"},
   "histogram": {"count": 2, "sum": 0.31, "min": 0.12, "max": 0.19,
                 "bucket_counts": [0, 0, 0, 0, 0, 0, 2, 0], "explicit_bounds": [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25]}}]}
```

| Field | Type | Description |
|---|---|---|
| `name` | string | Instrument name (`loadsmith.plugin.*`, or a semantic-convention name) |
| `unit`, `description` | string (optional) | UCUM unit (`s`, `By`, `{row}`) and text |
| `kind` | string | `counter`, `up_down_counter`, `histogram`, `gauge`; unknown ⇒ the point is skipped |
| `temporality` | string (optional) | `delta` (default, the only one defined); anything else ⇒ skipped |
| `attributes` | object (optional) | As for spans. The core adds `loadsmith.plugin.name`, `.kind`, `.version` |
| `value` | number | Counter / up-down counter: the sum over the interval; gauge: the last value |
| `histogram` | object | `count`, `sum`, `min`, `max`, `bucket_counts` (one more than `explicit_bounds`; bucket *i* holds `bounds[i-1] < v ≤ bounds[i]`), `explicit_bounds` |


### `object_ready`

Sent by a **file-output destination** to fd4 when it finalizes a staged file
(writes the footer / closes the chunk). The core forwards the path to the sink
supervisor, which delivers it — so delivery overlaps the data pump. Emitting
this on fd4 (rather than the control channel) is what lets a chunk be shipped
the moment it closes while the pump keeps filling the next.

```json
{"type": "object_ready", "path": "/scratch/events.00000001.snappy.parquet"}
```

| Field | Type | Description |
|---|---|---|
| `path` | string | Local path of the finalized file |

A **raw source** (capability `object_output`) sends `object_ready` for each
local file it produces, once the file is complete; the core forwards each
path to the parser as [`parse_object`](#parse_object). The core tells the
parser "no more files" only after the raw source's fd4 reached EOF (every
`object_ready` was read) **and** it sent `finished` with success.

---

### `object_parsed`

Sent by a **parser** to fd4 when a file is fully parsed. Informational: the
core logs it. `last_batch_seq` ties the file to the batch ordinals, so
per-file checkpoints can be added in 1.x without a new message.

```json
{"type": "object_parsed", "path": "/mnt/s3/inbox/2026-10-01.csv", "rows": 1500, "batches": 2, "last_batch_seq": 2}
```

| Field | Type | Description |
|---|---|---|
| `path` | string | The file (as in `parse_object`) |
| `rows` | integer | Rows parsed from this file |
| `batches` | integer | Arrow batches written to fd3 for this file |
| `last_batch_seq` | integer | Global batch ordinal (1-based, as on `checkpoint`) of the file's last batch; `0` if it produced none |

---

### `checkpoint`

Sent by an **incremental source** to fd4: the high watermark of the cursor column
produced through batch `batch_seq`. `cursor_value` is opaque to the core. The
core persists it only once the destination's `committed` confirms the matching
batch is durable — the durability gate that makes resume gap-free.

```json
{"type": "checkpoint", "cursor_value": "2026-06-09T08:00:00Z", "batch_seq": 42}
```

| Field | Type | Description |
|---|---|---|
| `cursor_value` | any | Opaque high watermark (string/number/…). Stored and echoed verbatim |
| `batch_seq` | integer | Global batch ordinal (1-based) this watermark covers |

---

### `committed`

Sent by a **destination** to fd4: everything through `batch_seq` is durably
committed. A destination durable only at the end emits one final `committed`
covering all batches; one with incremental flush emits them during the run.

```json
{"type": "committed", "batch_seq": 42}
```

| Field | Type | Description |
|---|---|---|
| `batch_seq` | integer | Highest durably-committed batch ordinal |

---

## Control messages

### `ping` / `pong`

A **sink-only** liveness check today. The core's sink supervisor sends `ping`
every 5 s (fixed); the sink responds with `pong`. A missing `pong` by the next
tick counts as a hang: the sink is respawned (up to 5 restarts) and every unacked
object is re-sent. Sources and destinations are not pinged. (The SDK answers `ping`
for every plugin kind.)

```json
{"type": "ping"}
{"type": "pong"}
```

---

### `deliver_object`

Sent by the **core** to a **sink** on the control channel — one per staged file
to deliver. The core closes the sink's stdin (EOF) once every object has been
delivered, signalling the sink to finalize.

```json
{"type": "deliver_object", "path": "/scratch/events.00000001.snappy.parquet"}
```

| Field | Type | Description |
|---|---|---|
| `path` | string | Local path the sink should deliver |

---

### `object_delivered`

Sent by a **sink** to the core after a successful `deliver`. This feeds the
core's delivery ledger: if the sink later crashes, the core respawns it and
re-sends only the objects not yet acknowledged.

```json
{"type": "object_delivered", "path": "/scratch/events.00000001.snappy.parquet"}
```

| Field | Type | Description |
|---|---|---|
| `path` | string | Local path that was delivered |

---

### `resolve`

Sent by the **core** to a **provider** on the control channel: resolve one
reference — the second argument of a `secret('<instance>', '<ref>')` template
call. The reference is opaque to the core (its syntax is the provider's).
The core sends each distinct reference once per run, one `resolve` at a time
(waiting for its `resolved`), then closes the provider's stdin (EOF).

```json
{"type": "resolve", "id": 1, "ref": "db.json#password"}
```

| Field | Type | Description |
|---|---|---|
| `id` | integer | Chosen by the core, unique within the conversation; echoed in `resolved` |
| `ref` | string | The provider-defined reference |

---

### `resolved`

Sent by a **provider** to the core on the control channel: the answer to the
`resolve` with the same `id`. Carries **exactly one** of `value` or `error`.
A reference the provider cannot resolve is an answer, not the end of the
conversation; the core then aborts resolution with that error (exit 3).

```json
{"type": "resolved", "id": 1, "value": "s3cr3t"}
{"type": "resolved", "id": 2, "error": {"code": "NOT_FOUND", "message": "no such file: missing.json"}}
```

| Field | Type | Description |
|---|---|---|
| `id` | integer | The `id` of the `resolve` being answered |
| `value` | string (optional) | The resolved value. Masked by the core everywhere from then on |
| `error` | object (optional) | `code` (string) and `message` (string). Suggested codes: `NOT_FOUND`, `ACCESS_DENIED`, `INVALID_REF`; SDK default `RESOLVE_FAILED`. Never put a value in `message` |

A wrong `id`, both fields, or neither is a protocol violation (`error`, code
`PROTOCOL_VIOLATION`).

---

### `parse_object`

Sent by the **core** to a **parser** on the control channel: parse the local
file at `path` (from a raw source's `object_ready`). The core sends each path
once; a parser treats a repeated path as already handled (idempotent per
path). The parser handles files one at a time, in arrival order. Stdin EOF
means "no more files": the parser ends its IPC stream with the end-of-stream
marker and sends `finished`. The core closes the parser's stdin only after the
raw source succeeded; after an upstream failure the parser gets `cancel`
instead, so it never ends its stream.

```json
{"type": "parse_object", "path": "/mnt/s3/inbox/2026-10-01.csv"}
```

| Field | Type | Description |
|---|---|---|
| `path` | string | Local path of the file to parse |

---

### `cancel`

Sent by the **core** to abort the run: another plugin failed, a timeout fired,
or the operator sent SIGINT/SIGTERM. May arrive at any time, including
mid-stream. The plugin stops, undoes its partial work (a destination rolls
back; a source releases its cursor), does **not** finalize, and replies
`finished` with `status: "cancelled"`. The SDK does all of this and calls the
plugin's `cancel()`. See the [abort rules](./reference.md#abort-rules).

```json
{"type": "cancel", "reason": "plugin failed: source exited with status 42 without sending finished"}
```

| Field | Type | Description |
|---|---|---|
| `reason` | string | Human-readable reason for cancellation |

---

### `error`

Sent by the **core** when it aborts the run because of a fault in **this**
plugin. The plugin stops and undoes its partial work, as for `cancel`; the core
expects no reply. A plugin must treat an unknown `code` like any other `error`.

```json
{
  "type": "error",
  "code": "PROTOCOL_VIOLATION",
  "message": "source: expected schema, got ready"
}
```

| Field | Type | Description |
|---|---|---|
| `code` | string | Machine-readable error code (below) |
| `message` | string | Human-readable description |

| Code | Meaning |
|---|---|
| `UNSUPPORTED_PROTOCOL_VERSION` | No protocol version is supported by both sides |
| `WRONG_PLUGIN_KIND` | `handshake_ack` declared another kind than the one spawned |
| `PROTOCOL_VIOLATION` | A message out of place for the lifecycle phase |
| `MALFORMED_MESSAGE` | Two consecutive malformed control lines |
| `TIMEOUT` | No answer within the configured [timeout](./reference.md#timeouts) |

---

### `resend`

Sent by **either side** on the control channel when the previous line it
received from the peer was malformed. The peer re-sends its most recent control
message (never a `pong` or a `resend`); a sink is re-sent every unacknowledged
`deliver_object` instead. A second consecutive malformed line is fatal. See
[Malformed lines and `resend`](./reference.md#malformed-lines-and-resend).

```json
{"type": "resend"}
{"type": "resend", "reason": "expected value at line 1 column 1"}
```

| Field | Type | Description |
|---|---|---|
| `reason` | string, optional | What was wrong with the line. Informational |

---

## Terminal message

### `finished`

Sent by the **plugin** as its last message on the control channel (fd1). After
sending this, the plugin must close fd3 and fd4 and exit.

**Successful source:**
```json
{
  "type": "finished",
  "status": "success",
  "rows_read": 100000,
  "batches_read": 50
}
```

**Successful destination:**
```json
{
  "type": "finished",
  "status": "success",
  "rows_written": 100000,
  "batches_written": 50
}
```

**Error:**
```json
{
  "type": "finished",
  "status": "error",
  "code": "BATCH_FAILED",
  "message": "connection reset by peer after 45,000 rows"
}
```

**Cancelled:**
```json
{"type": "finished", "status": "cancelled"}
```

The SDK's error codes: `SCHEMA_FAILED`, `BATCH_FAILED`, `IPC_WRITE_FAILED`
(source); `PREPARE_FAILED`, `WRITE_FAILED`, `IPC_READ_FAILED`,
`INPUT_TRUNCATED` (fd3 ended without the end-of-stream marker),
`FINALIZE_FAILED` (destination); `DELIVER_FAILED` (sink); `OBJECT_FAILED`
(raw source); `OPEN_FAILED`, `PARSE_FAILED`, `SCHEMA_MISMATCH`, `NO_INPUT`,
`IPC_WRITE_FAILED` (parser); and, for any kind,
`MALFORMED_MESSAGE`, `PROTOCOL_VIOLATION`, `CONTROL_CHANNEL_FAILED`,
`PLUGIN_FAILED`. Hand-written plugins may use their own. A plugin that failed
after `start` undoes its partial work *before* it sends this `finished`.

| Field | Type | Description |
|---|---|---|
| `status` | string | `"success"`, `"error"`, or `"cancelled"` |
| `rows_read` | integer (optional) | Total rows read (source or parser, on success) |
| `batches_read` | integer (optional) | Total batches read (source or parser, on success) |
| `rows_written` | integer (optional) | Total rows written (destination only, on success) |
| `batches_written` | integer (optional) | Total batches written (destination only, on success) |
| `code` | string (optional) | Machine-readable error code (on error) |
| `message` | string (optional) | Human-readable error message (on error). The core masks resolved template values in it |

**Sink form:** a sink reuses `finished` `success` with `rows_written` =
`batches_written` = the number of objects delivered.

**Provider form:** after the core closes its stdin, a provider sends the bare
`{"type": "finished", "status": "success"}` — no counters and no payload (values
travel only in [`resolved`](#resolved)).

**Raw-source form:** the bare `{"type": "finished", "status": "success"}`
once its last file was announced; the core takes the row counts from the
parser.

**Parser form:** like a source — `rows_read` / `batches_read` over all files,
sent after the parser ended its IPC stream with the end-of-stream marker. A
file whose schema differs from the stream's (the declared one, or the first
file's) is `status: "error"`, `code: "SCHEMA_MISMATCH"`; stdin EOF before any
file without a declared schema is `NO_INPUT`.
