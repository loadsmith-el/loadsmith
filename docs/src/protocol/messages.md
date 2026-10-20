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
  "kind": "source"
}
```

| Field | Type | Description |
|---|---|---|
| `plugin_name` | string | Should match the binary name (convention, shown in the run summary). Not enforced by the core |
| `plugin_version` | string | Semver version string |
| `protocol_supported_versions` | array of integers | Protocol versions this plugin understands |
| `kind` | string | One of: `"source"`, `"destination"`, `"sink"`, `"parser"` (reserved, no runtime yet), `"config_provider"` |

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
| `object_output` | file-output destinations whose files a sink can deliver (parquet) | yes — required when the pipeline has a `sink:` block |
| `staged_merge` | postgres, mysql, oracle destinations | no |
| `object_delivery` | sinks (SDK default) | no |
| `fetch` | config providers (SDK default) | no |

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
  }
}
```

| Field | Type | Description |
|---|---|---|
| `config` | object | Arbitrary JSON — the plugin's configuration |

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
| `code` | string (optional) | Machine-readable error code. Present only when `status` is `"error"` |
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

Sent by the **source plugin** on the control channel (fd1) immediately after
receiving `start`, before writing any Arrow data to fd3.

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

Sent by the **destination plugin** on the control channel (fd1) immediately after
receiving `start`. Signals that the destination has completed preparation and is
ready to receive Arrow batches on fd3.

```json
{"type": "ready"}
```

No fields. The core starts the data pump after receiving `ready`.

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
```

| Field | Type | Values |
|---|---|---|
| `level` | string | `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"` |
| `message` | string | Human-readable message |

---

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
`FINALIZE_FAILED` (destination); `DELIVER_FAILED` (sink); and, for any kind,
`MALFORMED_MESSAGE`, `PROTOCOL_VIOLATION`, `CONTROL_CHANNEL_FAILED`,
`PLUGIN_FAILED`. Hand-written plugins may use their own. A plugin that failed
after `start` undoes its partial work *before* it sends this `finished`.

| Field | Type | Description |
|---|---|---|
| `status` | string | `"success"`, `"error"`, or `"cancelled"` |
| `rows_read` | integer (optional) | Total rows read (source only, on success) |
| `batches_read` | integer (optional) | Total batches read (source only, on success) |
| `rows_written` | integer (optional) | Total rows written (destination only, on success) |
| `batches_written` | integer (optional) | Total batches written (destination only, on success) |
| `code` | string (optional) | Machine-readable error code (on error) |
| `message` | string (optional) | Human-readable error message (on error); for a config provider, the fetched document (see below) |

**Sink form:** a sink reuses `finished` `success` with `rows_written` =
`batches_written` = the number of objects delivered.

**Config-provider form:** a config provider answers with `status: "success"` and
carries the fetched document in `message`, as a (lossily converted) UTF-8 string —
the only channel for the provider's payload.
