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
Core → Plugin   set_protocol_version    (if incompatible: the core sends error and aborts the run)
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
Source → Core   finished                (after writing the IPC end-of-stream marker)
                ... commit point: the core ends the destination's fd3 ...
Destination → Core   finished           (after finalize)
```

**Commit point.** The core writes the Arrow IPC end-of-stream marker to the
destination's fd3 only after (a) the source's stream ended with its own
end-of-stream marker and (b) the source sent `finished` with status `success`.
A destination must treat a clean end of fd3 as "the input is complete" and only
then finalize (commit). A stream that ends **without** the marker is truncated:
the destination must not finalize it.

### 4.5 Cancellation and abort

The core may send `cancel` at any time, to any plugin:

```
Core → Plugin   cancel
Plugin → Core   finished  (status: cancelled)   (after undoing its partial work)
```

The core aborts a run on any failure: a plugin crash or unexpected exit, a
`finished` with status `error`, a protocol violation, a timeout (§4.7), a
stalled data plane, or an operator signal (SIGINT/SIGTERM). Then:

1. it stops forwarding batches and **never** writes the end-of-stream marker
   to the destination;
2. it sends `cancel` to every plugin still running, or `error` to the plugin at
   fault (an incompatible protocol, a wrong kind, a protocol violation, two
   malformed lines, a timeout);
3. it waits `timeouts.cancel_grace_seconds` (default 30) for each plugin to
   exit, then kills each plugin's process group with SIGKILL;
4. it closes the destination's fd3 only after the destination has exited;
5. it reports both plugins' outcomes and the root cause in the summary.

A plugin that receives `cancel` (or `error`) stops, undoes its partial work (a
destination rolls back its transaction or deletes partial output; a source
releases its cursor) and exits, replying `finished` with status `cancelled` to
`cancel`. It must not finalize. A plugin should also stop the same way when
its stdin reaches EOF unexpectedly, or when its fd3 input ends without the
end-of-stream marker: both mean the core is gone.

To be able to observe `cancel` mid-stream, a plugin must keep reading stdin
while it reads or writes fd3. Each plugin runs in its own process group, so a
terminal's Ctrl+C reaches only the core, which then cancels the plugins.

### 4.6 Malformed lines and `resend`

A malformed control line (not JSON, not UTF-8, no `type`, or a known `type`
with an invalid body) is answered **once** with `resend`: the receiver asks its
peer to re-send its most recent control message. Two consecutive malformed
lines from the same peer are fatal (the core aborts the run with `error`, code
`MALFORMED_MESSAGE`). Any well-formed line resets the count. Both sides follow
this rule.

The sender keeps its most recent control message (never a `pong` or a
`resend`) and re-sends it on `resend`. Exception: a sink that asks for a
`resend` gets every `deliver_object` it has not acknowledged yet (`deliver` is
idempotent).

Pipes do not corrupt data, so in practice this only catches a transient bug in
a peer's encoder. The lifecycle is request/response, so the most recent message
is almost always the garbled one; when it is not (for example
`set_protocol_version` and `capabilities_request`, which the core sends back to
back), the replay arrives out of place and the run fails as a protocol
violation, as it would have without `resend`. A peer that ignores `resend`
cannot hang the run: the timeouts of §4.7 end it.

### 4.7 Timeouts

Every wait of the core is bounded by `pipeline.timeouts` in `pipeline.yaml`:

| Wait | Key | Default |
|---|---|---|
| each `handshake_ack` / `capabilities_response` | `handshake_seconds` | 60 |
| `configure` → `configure_ack` | `configure_seconds` | 300 |
| `start` → `schema` / `ready` | `start_seconds` | 600 |
| source stream ended → source `finished` | `finish_seconds` | 300 |
| commit point → destination `finished` | `finalize_seconds` | 21600 |
| `cancel`/`error` → plugin exit | `cancel_grace_seconds` | 30 |
| no batch through the pump (opt-in) | `stall_seconds` | 0 (off) |
| first batch, when `stall_seconds` is on | `first_batch_seconds` | 3600 |

`0` disables a limit. A timeout aborts the run (§4.5) and sends `error` with
code `TIMEOUT` to the plugin that is late.

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

If there is no compatible version, the core sends `error` (code `UNSUPPORTED_PROTOCOL_VERSION`) and aborts the run.

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
{"type":"schema","fields":[{"name":"id","type":"int64","nullable":false},{"name":"name","type":"utf8","nullable":true},{"name":"created_at","type":"timestamp","unit":"us","timezone":"UTC","nullable":true}]}
```

`fields` entries are field objects: `name` (string), `type` (string), the
type's parameters beside `type`, and `nullable` (bool, optional, default
`true`). Nested children (`item`, struct `fields`, map `key`/`value`) are field
objects; a dictionary's `index`/`value` are type objects (no `name`/`nullable`).

| `type` | Parameters |
|---|---|
| `null`, `bool`, `int8`…`int64`, `uint8`…`uint64`, `float16`, `float32`, `float64` | — |
| `decimal128`, `decimal256` | `precision`, `scale` |
| `utf8`, `large_utf8`, `utf8_view`, `binary`, `large_binary`, `binary_view` | — |
| `fixed_size_binary` | `byte_width` |
| `date32`, `date64` | — |
| `time32` (`s`/`ms`), `time64` (`us`/`ns`), `duration` | `unit` |
| `timestamp` | `unit` (`s`/`ms`/`us`/`ns`), optional `timezone` |
| `interval` | `unit` (`year_month`/`day_time`/`month_day_nano`) |
| `list`, `large_list` | `item` |
| `fixed_size_list` | `item`, `size` |
| `struct` | `fields` |
| `map` | `key`, `value`, `keys_sorted` |
| `dictionary` | `index`, `value` |

The legacy name `timestamp_ms` (older SDKs) still reads as a millisecond
timestamp without a time zone. An unknown `type`, or a known one with invalid
parameters, is read as unknown and never fails the run: the authoritative
schema is the Arrow IPC header on fd3. Full rules:
[messages reference](../docs/src/protocol/messages.md#schema).

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

**Core → Plugin.** The run is being aborted (§4.5): another plugin failed, a
timeout fired, or the operator sent SIGINT/SIGTERM. The plugin stops, undoes its
partial work, and replies `finished` with status `cancelled`. It must not
finalize. May arrive at any time, including mid-stream.

```jsonl
{"type":"cancel","reason":"plugin failed: source exited with status 42 without sending finished"}
```

---

### `error`

**Core → Plugin.** The core is aborting the run because of a fault in **this**
plugin. The plugin stops and undoes its partial work, as for `cancel`; no reply
is expected. Codes (a plugin must treat an unknown code like any other):

| Code | Meaning |
|---|---|
| `UNSUPPORTED_PROTOCOL_VERSION` | No protocol version is supported by both sides |
| `WRONG_PLUGIN_KIND` | `handshake_ack` declared another kind than the one spawned |
| `PROTOCOL_VIOLATION` | A message out of place for the lifecycle phase |
| `MALFORMED_MESSAGE` | Two consecutive malformed control lines |
| `TIMEOUT` | No answer within the configured timeout (§4.7) |

```jsonl
{"type":"error","code":"UNSUPPORTED_PROTOCOL_VERSION","message":"no compatible protocol version: core supports [1], plugin supports [2]"}
```

---

### `resend`

**Either direction, control channel only.** The previous line received from
this peer was malformed; re-send your most recent control message (§4.6).
`reason` is optional and informational. A peer that does not know `resend`
ignores it (tolerant reader), and the requester's timeout ends the run.

```jsonl
{"type":"resend"}
{"type":"resend","reason":"expected value at line 1 column 1"}
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

Readers are **tolerant**: a well-formed message whose `type` is unknown is logged and skipped on every channel. A malformed line (not JSON, not UTF-8, missing `type`, or a known `type` with an invalid body) is answered with `resend` once on the control channel (fd0/fd1), and a second consecutive one is fatal (§4.6); on the event channel (fd4), which must never stop being drained, it is logged and skipped. See `docs/src/protocol/reference.md` ("Compatibility rule: tolerant reader").

New informational message types (ones a receiver may safely ignore) may be added within the same protocol version. Adding new mandatory message types, or changing the semantics of existing messages, requires incrementing the protocol version.
