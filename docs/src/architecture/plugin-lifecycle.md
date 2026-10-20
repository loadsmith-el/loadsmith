# Plugin Lifecycle

Every plugin, regardless of kind, goes through the same lifecycle. The SDK
(`loadsmith-plugin-sdk`) drives this protocol automatically; a plugin author only
implements the trait methods that map to each phase.

## Full sequence

```
core                                plugin
────────────────────────────────────────────────────────
spawn child process
wire fd0/fd1/fd3/fd4
                        [plugin initializes, opens fd3/fd4]

→ {"type": "handshake"}
                        ← {"type": "handshake_ack",
                              "plugin_name": "loadsmith-source-postgres",
                              "plugin_version": "0.2.1",
                              "protocol_supported_versions": [1],
                              "kind": "source"}

→ {"type": "set_protocol_version", "protocol_version": 1}

→ {"type": "capabilities_request"}
                        ← {"type": "capabilities_response",
                              "supports": ["batch_read", "schema_inference",
                                           "incremental_state"]}

→ {"type": "configure", "config": { ... }}
                        ← {"type": "configure_ack", "status": "ok"}
                             or
                           {"type": "configure_ack", "status": "error",
                              "code": "CONFIGURE_FAILED",
                              "message": "invalid postgres source config: missing field `dbname`"}

→ {"type": "start"}
     (an incremental source may get {"type": "start",
                                     "resume": {"cursor_value": ...}})

[source only]:
                        ← {"type": "schema", "fields": [
                               {"name": "id", "type": "utf8"},
                               {"name": "created_at", "type": "timestamp_ms"},
                               ...
                           ]}
                        ← [Arrow IPC stream on fd3: schema header + batches]
                        ← [progress (and checkpoint) events on fd4]
                        ← {"type": "finished", "status": "success",
                              "rows_read": 100000, "batches_read": 50}

[destination only]:
                        ← {"type": "ready"}
                        ← [consumes Arrow IPC on fd3]
                        ← [progress (and committed) events on fd4]
                        ← {"type": "finished", "status": "success",
                              "rows_written": 100000, "batches_written": 50}
```

## Phase 1 — Handshake

The core sends `{"type": "handshake"}` immediately after spawning the plugin.
The plugin responds with `HandshakeAck` announcing:

- `plugin_name` — by convention the binary name (e.g., `loadsmith-source-postgres`);
  shown in the run summary, not enforced by the core
- `plugin_version` — semver string from `Cargo.toml`
- `protocol_supported_versions` — the protocol versions this plugin understands
- `kind` — `source`, `destination`, `sink`, `parser` or `config_provider`; the
  core rejects a plugin whose kind is not the one it expects

## Phase 2 — Version negotiation

The core inspects `protocol_supported_versions` and picks the highest version
both sides support. It then sends `SetProtocolVersion`. The current protocol
version is **1**.

## Phase 3 — Capabilities

The core asks for a capabilities list. Plugins respond with the `supports` array.
The SDK defaults are `batch_read` + `schema_inference` for sources and
`batch_write` for destinations; plugins add their own (e.g. `incremental_state`,
`object_output`, `staged_merge`). The core enforces two of them:

- `incremental_state` on the source, required when the pipeline has a `state:`
  block;
- `object_output` on the destination, required when the pipeline has a `sink:`
  block.

The full list is in the [Message Reference](../protocol/messages.md#capabilities_response).

## Phase 4 — Configure

The core sends the plugin's configuration as a `serde_json::Value`, taken
verbatim from the pipeline YAML (after template resolution). The plugin deserializes
it into its own config struct and validates it:

```json
{"type": "configure", "config": {
  "host": "localhost",
  "port": 5432,
  "dbname": "mydb",
  "user": "myuser",
  "password": "s3cr3t",
  "query": "SELECT * FROM orders",
  "batch_size": 2000
}}
```

If validation fails, the plugin responds with:

```json
{"type": "configure_ack", "status": "error", "code": "CONFIGURE_FAILED", "message": "invalid postgres source config: missing field `dbname`"}
```

Error codes are free-form strings; SDK-based plugins always use
`CONFIGURE_FAILED` here.

And the core aborts the run immediately, printing the error. If validation
succeeds, the plugin may open connections, validate credentials, and prepare
cursors during this phase.

## Phase 5 — Start

The core sends `{"type": "start"}`. This is the signal to begin execution. The
source is started first; the destination only gets `start` once the source has
declared its schema.

**For sources:** for an incremental pipeline with prior state, `start` carries
`"resume": {"cursor_value": ...}`, which the SDK hands to the plugin's
`resume_from()`. The plugin sends `Schema` on the control channel (declaring the
schema of the data it will emit), then begins writing Arrow IPC batches to fd3.
The core uses the `Schema` message to report the schema and to fingerprint it for
incremental-state drift checks; it is not forwarded to the destination, which
reads the exact Arrow schema from the IPC stream header on its fd3. During the
run the SDK also emits `checkpoint` events on fd4, and the destination emits
`committed` — see [Incremental State](./incremental-state.md).

**For destinations:** the plugin sends `{"type": "ready"}`, signaling that it has
completed preparation and is ready to receive Arrow batches on fd3. The core
begins the pump only after receiving `Ready`.

## Phase 6 — Data stream

The data plane is active. The source writes batches to its fd3; the core relays
them through the pump to the destination's fd3. Both plugins concurrently write
events to fd4.

This phase ends when:
- The source exhausts its data, closes its IPC writer, and fd3 reaches EOF for
  the core's reader.
- The pump closes the write end of `out_pipe`, which sends EOF to the destination.
- The destination's `finalize()` is called.

## Phase 7 — Finished

Both plugins send a `Finished` message on their control channel (fd1). The core
reads both messages, validates the outcome, and prints the summary.

A successful source finished:
```json
{"type": "finished", "status": "success", "rows_read": 100000, "batches_read": 50}
```

A successful destination finished:
```json
{"type": "finished", "status": "success", "rows_written": 100000, "batches_written": 50}
```

If a plugin encounters an unrecoverable error at any point, it sends:
```json
{"type": "finished", "status": "error", "code": "BATCH_FAILED", "message": "connection reset"}
```

The SDK's codes are `BATCH_FAILED` (source) and `WRITE_FAILED` /
`IPC_READ_FAILED` (destination); hand-written plugins may use their own.

## Cancellation

> **Planned — not yet wired up.** The protocol defines `cancel`, but the core
> never sends it and has no SIGINT handler; only the sink SDK reacts to it today.
> The source and destination `cancel()` trait methods are never called.

Current behavior: when a plugin fails, the core returns an error and the run
ends with a non-zero exit code. A terminal Ctrl+C sends SIGINT to the whole
foreground process group — plugins are not put in their own group — so every
process exits without running `cancel()`. A signal sent only to the loadsmith pid
(e.g. `kill`, some container stops) is not forwarded to the plugins.

The intended design: on SIGINT or a failure in one plugin, the core sends

```json
{"type": "cancel", "reason": "source plugin failed"}
```

to the other, and each plugin's `cancel()` aborts promptly — closing cursors,
rolling back transactions, removing partial output files.
