# Protocol Specification

The loadsmith plugin protocol is a structured message exchange over file
descriptors. It is designed to be simple enough to implement in any language
while providing strong guarantees about lifecycle ordering.

## Channel layout

```
fd0 (stdin)  — JSONL, core → plugin  — lifecycle commands
fd1 (stdout) — JSONL, plugin → core  — lifecycle responses
fd3          — Arrow IPC stream       — data (source writes, destination reads)
fd4          — JSONL, plugin → core  — events (logs, progress, object_ready, checkpoint, committed)
```

fd0 and fd1 are the standard Unix stdin/stdout. fd3 and fd4 are created by the
core as OS pipes before forking, then `dup2`'d into the child at the appropriate
fd numbers. Plugins must not assume fd3/fd4 are available before the process
starts — they are set up by the core.

## Message framing

Messages are plain JSON Lines: each message is a single-line JSON object
terminated by `\n`, with no length prefix:

```
{"type": "handshake"}
```

Newlines inside string values are JSON-escaped (`\n`), so splitting on line
breaks is always safe. Blank lines are ignored. The `loadsmith-transport` crate
implements `ControlReader`/`ControlWriter` and `EventWriter` to handle this
framing automatically. Plugin authors never write
raw bytes to these channels — the SDK handles all framing.

## Protocol version

The current protocol version is **1**. The version is negotiated during the
handshake phase and governs the set of valid message types and their fields.
Future versions may add new message types or new optional fields to existing
messages, always in a backward-compatible manner.

## Full lifecycle sequence

The complete sequence for a source + destination run:

```
core                                 source plugin
─────────────────────────────────────────────────────────────────────────────
→ handshake
                                     ← handshake_ack
→ set_protocol_version
→ capabilities_request
                                     ← capabilities_response
→ configure
                                     ← configure_ack (ok or error)
→ start
                                     ← schema        [sends arrow schema on fd1]
                                     [writes arrow IPC stream on fd3]
                                     [writes events on fd4]
                                     ← finished

core                                 destination plugin
─────────────────────────────────────────────────────────────────────────────
→ handshake
                                     ← handshake_ack
→ set_protocol_version
→ capabilities_request
                                     ← capabilities_response
→ configure
                                     ← configure_ack (ok or error)
→ start
                                     ← ready
                                     [reads arrow IPC stream on fd3]
                                     [writes events on fd4]
                                     ← finished
```

Both plugins are spawned up front, but the core drives them in a fixed order:

1. handshake / configure the source, then the destination;
2. capability gates: `incremental_state` on the source if the pipeline has a
   `state:` block, `object_output` on the destination if it has a `sink:` block;
3. spawn the sink supervisor (only with `sink:`), which brings the sink up to
   `ready`;
4. `start` the source and wait for its `schema`;
5. schema-drift check and state supervisor (only with `state:`);
6. `start` the destination and wait for `ready`;
7. start the data pump.

## Sink delivery (optional stage)

When the pipeline has a `sink` block, the destination must advertise the
`object_output` capability (else the core rejects the config). The destination
stages files locally and announces each finalized one as an `object_ready` event
on fd4; the core's sink supervisor delivers them:

```
destination                core                         sink plugin
──────────────────────────────────────────────────────────────────────────────
[stages file, fd4]→ object_ready
                           → handshake … configure … start
                                                        ← ready
                           → deliver_object (per file)
                                                        ← object_delivered (ack)
                           (all delivered → close sink stdin / EOF)
                                                        ← finished
```

A sink has **no fd3** — it is a delivery stage, not a data-plane participant.
The core owns the delivery ledger (which paths were acked): if the sink hangs
(no `pong`), dies, or errors, the core respawns it and re-sends every unacked
object, so `deliver` must be idempotent. The sink can outlive source and
destination, delivering its queue at its own pace. See
[Sink Delivery](../architecture/sink-delivery.md).

## Ordering guarantees

- The core never sends `start` before receiving `configure_ack: ok`.
- The core never starts the data pump before receiving both `schema` (from source)
  and `ready` (from destination).
- `finished` is always the last message a plugin sends. After sending it, the
  plugin must close fd3 and fd4 and exit.
- The core reads `finished` after the pump completes (source fd3 reaches EOF).

## Error handling

If `configure_ack` carries `status: "error"`, or a plugin violates the protocol,
the core aborts the run with an error (`plugin rejected config: …`) and exits
with a non-zero code. No `cancel` is sent to the other plugin.

If either plugin sends `finished` with `status: "error"`, the run is marked
failed in the summary and the exit code is non-zero.

If a plugin process exits unexpectedly (crashes), the core sees EOF on the
plugin's stdout (fd1) before `finished` (`EOF while waiting for …` or `plugin
closed control channel without sending finished`) and treats it as a protocol
error.

Propagating `cancel` to the other plugin on failure is planned but not yet
implemented.

## Implementing the protocol in another language

The protocol is transport-agnostic JSON. Any language that can:
- Read/write newline-delimited JSON (JSONL) on stdin/stdout
- Write Apache Arrow IPC to fd3 (via `arrow::ipc::writer::StreamWriter`), or,
  for a destination, read an Arrow IPC stream from fd3
- Write JSONL to fd4

can implement a loadsmith plugin. The message schema is in
[Message Reference](./messages.md).
