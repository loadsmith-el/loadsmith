# Loadsmith — Core/Plugin Protocol

## 1. Overview

This document defines the communication contract between the Loadsmith Core and its plugins. All communication is message-oriented. There are three channels, separated by purpose:

| Channel | Direction | Format | Purpose |
|---|---|---|---|
| stdin (fd0) | Core → Plugin | JSONL | Control messages from the core to the plugin |
| stdout (fd1) | Plugin → Core | JSONL | Control messages from the plugin to the core |
| fd3 | Depends on kind* | Apache Arrow IPC | Batches of tabular data |
| fd4 | Plugin → Core | JSONL | Logs and structured events |

*fd3 is written by sources and read by destinations. Sinks have no fd3.

`stderr` is not used as a communication channel.

---

## 2. Message format

Each message is a JSON object serialized on a single line, delimited by `\n`.

Rules:
- One message per line
- No multi-line JSON
- No JSON without a line delimiter
- The `type` field is mandatory in every message
- Unknown fields must be ignored — this guarantees forward compatibility

---

## 3. Versioning

The protocol uses **ordinal integer versioning**, starting at `1`. It is incremented on every incompatible change.

The version is negotiated in the handshake. Once set via `set_protocol_version`, it holds for the whole run.

---

## 4. Lifecycle

### 4.1 Protocol negotiation

```
Core → Plugin   handshake
Plugin → Core   handshake_ack
Core → Plugin   set_protocol_version    (if incompatible: the core aborts the run; nothing is sent)
```

### 4.2 Capabilities

```
Core → Plugin   capabilities_request
Plugin → Core   capabilities_response
```

### 4.3 Configuration

```
Core → Plugin   configure
Plugin → Core   configure_ack           (ok or error; on error the core aborts the run)
```

### 4.4 Execution

```
Core → Plugin   start                   (may carry resume for incremental sources)
Plugin → Core   schema                  (source: before the first batch)
Plugin → Core   ready                   (destination: ready to receive batches)
                ... data over fd3 ...
                ... progress and log happen during execution ...
                ... checkpoint (source) and committed (destination) happen on fd4 ...
                ... ping/pong happens with sinks only (see below) ...
Plugin → Core   finished
```

### 4.5 Cancellation

> **Reserved — not currently emitted by the core.** The core never sends
> `cancel` today; only the sink SDK reacts to it.

Intended to be possible at any time after `start`:

```
Core → Plugin   cancel
Plugin → Core   finished  (status: cancelled)
```

---

## 5. Message reference

### `handshake`

**Core → Plugin.** The first message the core sends after starting the plugin process.

```jsonl
{"type":"handshake"}
```

---

### `handshake_ack`

**Plugin → Core.** Immediate reply to `handshake`. The plugin declares the protocol versions it supports, its identity and its kind.

```jsonl
{"type":"handshake_ack","protocol_supported_versions":[1],"plugin_name":"loadsmith-source-csv","plugin_version":"0.1.0","kind":"source"}
```

Fields:

| Field | Type | Description |
|---|---|---|
| `protocol_supported_versions` | `int[]` | Versions supported by the plugin, in ascending order |
| `plugin_name` | `string` | Plugin identifier (by convention the binary name; not enforced by the core) |
| `plugin_version` | `string` | Version of the plugin binary |
| `kind` | `string` | `"source"`, `"destination"`, `"sink"`, `"parser"` or `"config_provider"` |

---

### `set_protocol_version`

**Core → Plugin.** The core picks the version to use — the highest one present in `protocol_supported_versions` that the core also supports. From this point on, the protocol of the chosen version is in force.

If there is no compatible version, the core aborts the run with a `no compatible protocol version` error and sends nothing to the plugin (the plugin sees its stdin close).

```jsonl
{"type":"set_protocol_version","protocol_version":1}
```

---

### `capabilities_request`

**Core → Plugin.** Requests the plugin's capabilities.

```jsonl
{"type":"capabilities_request"}
```

---

### `capabilities_response`

**Plugin → Core.**

```jsonl
{"type":"capabilities_response","supports":["schema_inference","batch_read","incremental_state"]}
```

Defined capabilities (extensible):

| Capability | Description |
|---|---|
| `schema_inference` | The plugin can infer the source's schema |
| `batch_read` | Reads in batches (source SDK default) |
| `incremental_state` | Source: accepts a resume cursor and reports watermarks via `checkpoint`. Required by the core when the pipeline has a `state:` block |
| `batch_write` | Writes in batches (destination SDK default) |
| `object_output` | Destination: stages files and announces each via `object_ready`. Required by the core when the pipeline has a `sink:` block |
| `staged_merge` | Destination: writes to staging and does a `MERGE` by PK at the end — exactly-once effective. Advertised, but not read by the core |
| `object_delivery` | Sink: delivers staged objects (sink SDK default) |
| `fetch` | Config provider: fetches a configuration document (config-provider SDK default) |

Intra-run durability needs no capability: a destination reports it by emitting `committed` during the run (in the SDK, by implementing `durable_through()`).

---

### `configure`

**Core → Plugin.** Sends the plugin's configuration block — the contents of `source.config`, `destination.config`, etc. The contents of `config` are opaque to the core.

```jsonl
{"type":"configure","config":{...}}
```

---

### `configure_ack`

**Plugin → Core.** Result of the plugin's own validation of the configuration.

Success:

```jsonl
{"type":"configure_ack","status":"ok"}
```

Error:

```jsonl
{"type":"configure_ack","status":"error","code":"INVALID_CONFIG","message":"field 'path' is required"}
```

If `status` is `error`, the core shows `message` to the user and aborts the run. (SDK-based plugins always use the code `CONFIGURE_FAILED`.)

---

### `start`

**Core → Plugin.** Starts execution. For an incremental source whose pipeline has
state persisted from a previous run, the core includes `resume` with the opaque
watermark to resume from — the core never interprets `cursor_value`, it only
stores and returns it. The short form `{"type":"start"}` remains valid (the field
is optional).

```jsonl
{"type":"start"}
{"type":"start","resume":{"cursor_value":"2026-06-09T08:00:00Z"}}
```

---

### `schema`

**Plugin → Core. Sources only.** Sent before the first batch. Describes the schema of the data that will be produced.

```jsonl
{"type":"schema","fields":[{"name":"id","type":"int64"},{"name":"name","type":"utf8"},{"name":"created_at","type":"timestamp_ms"}]}
```

`fields` entries:

| Field | Type | Description |
|---|---|---|
| `name` | `string` | Column name |
| `type` | `string` | Arrow type: `int32`, `int64`, `float32`, `float64`, `utf8`, `bool`, `date32`, `timestamp_ms`, `binary` |

---

### `ready`

**Plugin → Core. Destinations and sinks.** Sent after `start`, signalling that the plugin is ready to receive batches via fd3 (or, for a sink, objects to deliver).

```jsonl
{"type":"ready"}
```

---

### `progress`

**Plugin → Core.** Sent periodically during execution.

Source:

```jsonl
{"type":"progress","rows_read":10000,"batches_read":1}
```

Destination:

```jsonl
{"type":"progress","rows_written":10000,"batches_written":1}
```

---

### `log`

**Plugin → Core. Via fd4 (the event channel). Never via stdout.**

```jsonl
{"type":"log","level":"info","message":"connected to the database"}
```

Levels: `trace`, `debug`, `info`, `warn`, `error`.

---

### `checkpoint`

**Source → Core. Via fd4 (the event channel).** The highest watermark of the
cursor column produced through batch `batch_seq`. `cursor_value` is opaque to the
core. The core only persists this value after the destination confirms (via
`committed`) that the matching batch is durable — this durability gate is what
makes resume gap-free (at-least-once; boundary duplicates absorbed by an
idempotent destination).

```jsonl
{"type":"checkpoint","cursor_value":"2026-06-09T08:00:00Z","batch_seq":42}
```

---

### `committed`

**Destination → Core. Via fd4 (the event channel).** Everything through batch
`batch_seq` (inclusive) is durably written. A destination that only becomes
durable at the end (e.g. staging + atomic swap) emits a single `committed`
covering every batch at `finalize`; a destination with incremental flush emits
them during the run.

```jsonl
{"type":"committed","batch_seq":42}
```

---

### `object_ready`

**Destination → Core. Via fd4 (the event channel).** Sent by a file-output
destination when a staged file is finalized (footer written). The core forwards
the path to the sink supervisor, which delivers it — so delivery overlaps the
pump. Using the event channel (already drained concurrently) is what allows a
chunk to be delivered the moment it closes.

```jsonl
{"type":"object_ready","path":"/scratch/events.00000001.snappy.parquet"}
```

---

### `deliver_object`

**Core → Sink. Via the control channel.** One per file to deliver. The core
closes the sink's stdin (EOF) once every object has been delivered, signalling
the sink to finalize.

```jsonl
{"type":"deliver_object","path":"/scratch/events.00000001.snappy.parquet"}
```

---

### `object_delivered`

**Sink → Core. Via the control channel.** Per-object delivery ack. It feeds the
core's delivery ledger: if the sink dies, the core respawns it and re-sends only
the objects not yet acknowledged. `deliver` must be idempotent.

```jsonl
{"type":"object_delivered","path":"/scratch/events.00000001.snappy.parquet"}
```

---

### `ping`

**Core → Sink.** Liveness check. Today only the sink supervisor sends `ping`, on
a fixed 5-second interval; sources and destinations are never pinged.

```jsonl
{"type":"ping"}
```

---

### `pong`

**Sink → Core.** Immediate reply to `ping`.

```jsonl
{"type":"pong"}
```

If the core has not received a `pong` by the next ping tick, it treats the sink
as hung: it respawns the sink and re-sends every unacked object from the delivery
ledger, up to 5 restarts, after which delivery fails. Nothing about this is
configurable.

---

### `cancel`

**Core → Plugin. Reserved — not currently emitted by the core.** Requests
cancellation: the plugin should stop as fast as possible and send `finished`.
Only the sink SDK handles it today.

```jsonl
{"type":"cancel","reason":"timeout"}
```

---

### `error`

**Core → Plugin. Reserved — not currently emitted by the core.** Meant for an
unrecoverable error raised by the core before or during the handshake. Today the
core instead aborts the run with a local error (e.g. `no compatible protocol
version: …`) and the plugin just sees its stdin close.

```jsonl
{"type":"error","code":"UNSUPPORTED_PROTOCOL_VERSION","message":"plugin supports versions [2, 3], core supports only [1]"}
```

---

### `finished`

**Plugin → Core.** The last message the plugin sends.

Success (source):

```jsonl
{"type":"finished","status":"success","rows_read":21473000,"batches_read":2148}
```

Success (destination):

```jsonl
{"type":"finished","status":"success","rows_written":21473000,"batches_written":2148}
```

Error:

```jsonl
{"type":"finished","status":"error","code":"CONNECTION_FAILED","message":"failed to connect: connection refused"}
```

Cancelled:

```jsonl
{"type":"finished","status":"cancelled"}
```

---

## 6. Forward compatibility

Unknown fields in any message must be ignored. New optional fields may be added within the same protocol version.

Adding new mandatory message types, or changing the semantics of existing messages, requires incrementing the protocol version.
