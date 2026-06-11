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
  "plugin_version": "0.1.0",
  "protocol_supported_versions": [1],
  "kind": "source"
}
```

| Field | Type | Description |
|---|---|---|
| `plugin_name` | string | Must match the binary name exactly |
| `plugin_version` | string | Semver version string |
| `protocol_supported_versions` | array of integers | Protocol versions this plugin understands |
| `kind` | string | One of: `"source"`, `"destination"`, `"sink"`, `"config_provider"` |

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
{"type": "capabilities_response", "supports": []}
```

| Field | Type | Description |
|---|---|---|
| `supports` | array of strings | Capability identifiers, e.g. `"batch_write"`, `"object_output"` (a file-output destination whose files a sink can deliver), `"object_delivery"` (a sink) |

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
  "code": "invalid_config",
  "message": "field 'host' is required"
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
    {"name": "id",         "type": "utf8"},
    {"name": "amount",     "type": "float64"},
    {"name": "created_at", "type": "timestamp_ms"},
    {"name": "is_active",  "type": "bool"}
  ]
}
```

| Field | Type | Description |
|---|---|---|
| `fields` | array of Field | The Arrow schema |

**Field object:**

| Field | Type | Values |
|---|---|---|
| `name` | string | Column name |
| `type` | string | One of the types below |

**Available types:**

| Type string | Arrow type | Notes |
|---|---|---|
| `"int32"` | `Int32` | 32-bit signed integer |
| `"int64"` | `Int64` | 64-bit signed integer |
| `"float32"` | `Float32` | 32-bit IEEE 754 |
| `"float64"` | `Float64` | 64-bit IEEE 754 |
| `"utf8"` | `Utf8` | UTF-8 string |
| `"bool"` | `Boolean` | Boolean |
| `"date32"` | `Date32` | Days since Unix epoch (1970-01-01) |
| `"timestamp_ms"` | `Timestamp(Millisecond, None)` | Milliseconds since Unix epoch |
| `"binary"` | `Binary` | Raw byte array |

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

Sent by the **core** at any time; the plugin responds with `pong`. Used to
detect liveness.

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

Sent by the **core** when the run is aborted (Ctrl+C, other plugin failed, etc.).
The plugin must clean up and send `finished` with `status: "cancelled"`.

```json
{"type": "cancel", "reason": "source plugin failed"}
```

| Field | Type | Description |
|---|---|---|
| `reason` | string | Human-readable reason for cancellation |

---

### `error`

Sent by the **core** on a fatal protocol violation.

```json
{
  "type": "error",
  "code": "protocol_violation",
  "message": "expected handshake_ack, got configure"
}
```

| Field | Type | Description |
|---|---|---|
| `code` | string | Machine-readable error code |
| `message` | string | Human-readable description |

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
  "code": "cursor_failed",
  "message": "connection reset by peer after 45,000 rows"
}
```

**Cancelled:**
```json
{"type": "finished", "status": "cancelled"}
```

| Field | Type | Description |
|---|---|---|
| `status` | string | `"success"`, `"error"`, or `"cancelled"` |
| `rows_read` | integer (optional) | Total rows read (source only, on success) |
| `batches_read` | integer (optional) | Total batches read (source only, on success) |
| `rows_written` | integer (optional) | Total rows written (destination only, on success) |
| `batches_written` | integer (optional) | Total batches written (destination only, on success) |
| `code` | string (optional) | Machine-readable error code (on error) |
| `message` | string (optional) | Human-readable error message (on error) |
