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
                              "plugin_version": "0.1.0",
                              "protocol_supported_versions": [1],
                              "kind": "source"}

→ {"type": "set_protocol_version", "protocol_version": 1}

→ {"type": "capabilities_request"}
                        ← {"type": "capabilities_response", "supports": []}

→ {"type": "configure", "config": { ... }}
                        ← {"type": "configure_ack", "status": "ok"}
                             or
                           {"type": "configure_ack", "status": "error",
                              "code": "invalid_config",
                              "message": "host is required"}

→ {"type": "start"}

[source only]:
                        ← {"type": "schema", "fields": [
                               {"name": "id", "type": "utf8"},
                               {"name": "created_at", "type": "timestamp_ms"},
                               ...
                           ]}
                        ← [Arrow IPC stream on fd3: schema header + batches]
                        ← [progress events on fd4]
                        ← {"type": "finished", "status": "success",
                              "rows_read": 100000, "batches_read": 50}

[destination only]:
                        ← {"type": "ready"}
                        ← [consumes Arrow IPC on fd3]
                        ← [progress events on fd4]
                        ← {"type": "finished", "status": "success",
                              "rows_written": 100000, "batches_written": 50}
```

## Phase 1 — Handshake

The core sends `{"type": "handshake"}` immediately after spawning the plugin.
The plugin responds with `HandshakeAck` announcing:

- `plugin_name` — must match the binary name exactly (e.g., `loadsmith-source-postgres`)
- `plugin_version` — semver string from `Cargo.toml`
- `protocol_supported_versions` — the protocol versions this plugin understands
- `kind` — `source`, `destination`, or `config_provider`

## Phase 2 — Version negotiation

The core inspects `protocol_supported_versions` and picks the highest version
both sides support. It then sends `SetProtocolVersion`. The current protocol
version is **1**.

## Phase 3 — Capabilities

The core asks for a capabilities list. Plugins respond with the `supports` array.
In protocol version 1, no capabilities are defined; the array is always empty.
This phase exists as an extension point for future features (e.g., `"incremental"`,
`"parallel_writes"`).

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
{"type": "configure_ack", "status": "error", "code": "invalid_config", "message": "host is required"}
```

And the core aborts the run immediately, printing the error. If validation
succeeds, the plugin may open connections, validate credentials, and prepare
cursors during this phase.

## Phase 5 — Start

The core sends `{"type": "start"}`. This is the signal to begin execution.

**For sources:** the plugin sends `Schema` on the control channel (declaring the
Arrow schema of the data it will emit), then begins writing Arrow IPC batches to
fd3. The `Schema` message is sent *before* the first batch so the destination
can prepare its sink.

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
{"type": "finished", "status": "error", "code": "cursor_failed", "message": "connection reset"}
```

## Cancellation

If the user sends SIGINT (Ctrl+C) or the core encounters an error from one
plugin, it sends `Cancel` to the other:

```json
{"type": "cancel", "reason": "source plugin failed"}
```

Each plugin's `cancel()` trait method is responsible for aborting promptly —
closing cursors, rolling back transactions, removing partial output files.
