# Protocol Specification

The loadsmith plugin protocol is a structured message exchange over file
descriptors. It is designed to be simple enough to implement in any language
while providing strong guarantees about lifecycle ordering.

## Channel layout

```
fd0 (stdin)  — JSONL, core → plugin  — lifecycle commands
fd1 (stdout) — JSONL, plugin → core  — lifecycle responses
fd2 (stderr) — free text, plugin → core — copied to the engine's stderr, masked
fd3          — Arrow IPC stream       — data (sources and parsers write, destinations read)
fd4          — JSONL, plugin → core  — events (logs, progress, object_ready, object_parsed, checkpoint, committed;
                                        telemetry: span, metrics)
```

fd0 and fd1 are the standard Unix stdin/stdout. fd3 and fd4 are created by the
core as OS pipes before forking, then `dup2`'d into the child at the appropriate
fd numbers. Plugins must not assume fd3/fd4 are available before the process
starts — they are set up by the core.

| Kind | fd3 | fd4 |
|---|---|---|
| source | writes | yes |
| destination | reads | yes |
| sink | none | yes |
| provider | none | yes |
| raw source (`object_output`) | none | yes (`object_ready` per file) |
| parser | writes | yes (`object_parsed`) |

fd4 is always wired and always drained, for every kind. stderr is not a
protocol channel: the core pipes it and copies it to its own stderr line by
line with every resolved template value masked (a provider's stderr is held
until its conversation is over, then printed masked).

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

## Compatibility rule: tolerant reader

Both sides — the core and every plugin — read the JSONL channels (fd0, fd1,
fd4) as **tolerant readers**. This is what makes "add a message type or an
optional field" a compatible change within protocol version 1:

| What arrives | Control channel (fd0 / fd1) | Event channel (fd4) |
|---|---|---|
| Blank line | skipped | skipped |
| Unknown **field** on a known message | ignored | ignored |
| Well-formed JSON object with an unknown **`type`** | logged (`warn`) and skipped | logged (`warn`) and skipped |
| A known message that is out of place for this channel/phase | protocol error where the lifecycle expects a specific reply; otherwise logged and ignored | logged (`warn`) and ignored |
| Malformed line: not JSON, not UTF-8, no `type`, or a known `type` with an invalid body | answered with `resend` **once**; a second consecutive malformed line is **fatal** (see [below](#malformed-lines-and-resend)) | logged (`warn`) and skipped; draining continues |

Why the two channels differ on malformed input: the control channel carries the
lifecycle, so a garbled line there means a misbehaving peer: it gets exactly one
chance to repeat itself, and continuing past a second garbled line would only
produce a confusing failure later. The event channel carries logs and
progress; it **must never stop being drained** — a core that stopped reading
fd4 would let the pipe fill and block the plugin forever — so a bad event line
costs only that line.

### Malformed lines and `resend`

On the control channel, both sides apply the same rule:

1. The receiver of a malformed line sends `{"type":"resend"}` (with an optional
   `reason`) and waits again.
2. The peer re-sends its most recent control message. It never counts a `pong`
   or a `resend` as that message. A sink that asks for a resend gets every
   `deliver_object` it has not acknowledged yet (`deliver` is idempotent).
3. Two consecutive malformed lines from the same peer are fatal: the core
   aborts the run (sending `error` with code `MALFORMED_MESSAGE`), and an SDK
   plugin stops. Any well-formed line resets the count.

Pipes are reliable, so this mostly catches a transient bug in a peer's
encoder; it is not a retransmission protocol. The lifecycle is
request/response, so the most recent message is nearly always the garbled one.
When it is not (the core sends `set_protocol_version` and
`capabilities_request` back to back), the replay arrives out of place and the
run fails as a protocol violation, exactly as it would have without `resend`.
A peer that ignores `resend` cannot hang the run: the core's
[timeouts](#timeouts) end it.

Consequences for evolving the protocol within version 1:

- New **optional** fields and new **informational** message types (ones a peer
  may safely ignore) are compatible additions.
- A new message type the receiver **must** act on, a new required field, or a
  change to the meaning of an existing message requires a new protocol version
  (negotiated in the handshake).

`ControlReader` in `loadsmith-transport` implements this rule, so plugins built
on the SDK get it for free.

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
→ start (credit: N with flow_credit)
                                     ← schema        [sends arrow schema on fd1]
                                     [writes arrow IPC stream on fd3]
                                     [writes events on fd4]
→ credit (with flow_credit: after each batch the core hands on)
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
   `state:` block, `object_output` on the destination if it has a `sink:` block,
   no `object_output` on the source (that is a raw source, which needs a
   `parser:`; see [Parsers](#parsers));
3. spawn the sink supervisor (only with `sink:`), which brings the sink up to
   `ready`;
4. `start` the source and wait for its `schema`, then for the header of its
   Arrow IPC stream on fd3 (the authoritative schema; both within
   `start_seconds`);
5. schema-drift check against the IPC schema, and the state supervisor (only
   with `state:`);
6. `start` the destination and wait for `ready`;
7. start the data pump. With a source that advertised `flow_credit`, the pump
   grants it a window of batches after each batch it hands to the destination,
   and the core relays each grant as [`credit`](messages.md#credit).

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

## Providers (value resolution)

A pipeline's `secret('<instance>', '<ref>')` calls are resolved by provider
plugins (kind `provider`, binary `loadsmith-provider-<type>`) **before** the
state lock and before any data plugin is spawned. The core spawns each
referenced instance once, all instances in parallel (a declared instance no
`secret()` uses is not started):

```
core                                 provider plugin
─────────────────────────────────────────────────────────────────────────────
→ handshake … capabilities_request
                                     ← capabilities_response  (must include resolve)
→ configure
                                     ← configure_ack
→ start
                                     ← ready
→ resolve {id: 1, ref: "db.json#password"}
                                     ← resolved {id: 1, value: "…"}
→ resolve {id: 2, ref: "db.json#user"}
                                     ← resolved {id: 2, error: {code, message}}
(close stdin / EOF)
                                     ← finished {status: success}
```

- One `resolve` in flight at a time, each distinct reference once per run;
  a `resend` therefore replays exactly the request in flight.
- A provider has **no fd3**. Its fd4 is drained throughout; its log lines and
  its stderr are held and printed (masked) once the conversation is over,
  because a value is known — and maskable — only once the core has read its
  `resolved`.
- Each instance has one budget, spawn to `finished`:
  `providers.<name>.timeout_seconds` (default 60, `0` disables). On expiry the
  core sends `error` with code `TIMEOUT`.
- The first failing instance (a `resolved` error, a crash, a protocol
  violation, a timeout) aborts the others with the [abort rules](#abort-rules):
  `error` to the culprit, `cancel` to the rest, grace, kill. Nothing else has
  started, so the run ends there with a configuration error (exit `3`); a
  missing or incompatible provider binary is exit `4`.

## Parsers

With a `parser:` block, the source is a **raw source** (capability
`object_output`): it has no fd3, answers `start` with `ready`, and announces
each complete local file with `object_ready{path}` on fd4. The core hands each
path, once, to the parser (capability `parse`) as `parse_object{path}` on fd0.
The parser takes the source's place on the Arrow side: `schema`, Arrow IPC on
fd3 (through the unchanged pump), `object_parsed` events on fd4, and on stdin
EOF the end-of-stream marker and `finished`:

```
raw source ──fd4 object_ready{path}──▶ core ──fd0 parse_object{path}──▶ parser ──fd3──▶ core ──▶ destination
```

```
core                    raw source                     parser
──────────────────────────────────────────────────────────────────────────────
→ handshake … configure (both, then the destination)
  capability gates
→ start                                                (parser)
→ start                 ← ready
                        [fd4] object_ready{a.csv}
→ parse_object{a.csv}                                  ← schema (after its
                                                         first file, or at
                                                         once if declared)
  (IPC header read; destination start → ready; pump)
                        [fd4] object_ready{b.csv}      [fd3] batches…
→ parse_object{b.csv}                                  [fd4] object_parsed{a.csv}
                        ← finished{success}
                        (exits: fd4 EOF)
(close parser stdin: "no more files")
                                                       [fd3] end-of-stream
                                                       ← finished{rows_read, …}
(end the destination's fd3: commit point)
```

- The core waits for the parser's `schema` within `start_seconds`, handing it
  files meanwhile, then for its IPC stream header.
- **The parser's stdin is closed gracefully only after the raw source's fd4
  reached EOF (every `object_ready` was read) and the raw source sent
  `finished` with success.** After any upstream failure the parser gets
  `cancel` through the [abort rules](#abort-rules) and never sees EOF.
- **Commit point:** the destination's input ends cleanly only after the
  parser's stream ended with its marker and both the parser and the raw source
  finished with success.
- A parser that sends `finished` success before its stdin was closed is a
  protocol violation (`PROTOCOL_VIOLATION`); a raw source whose fd4 stays open
  `finish_seconds` after its `finished` is a timeout.
- The core's control lines to a parser are written by a task that owns its
  stdin, so a parser that reads stdin slowly never stalls the core. A parser
  `resend` replays the most recent line, as for every kind; `parse_object` is
  idempotent per path.
- A raw source that advertises `scratch_dir` gets `scratch_dir: <dir>` added
  to its config (unless set); the core removes that directory after a
  successful run only.
- Capability gates: a `parser:` needs a raw source and a raw source needs a
  `parser:` (configuration error, exit `3`); a parser without `parse` is
  incompatible (exit `4`). A `parser:` cannot be combined with `state:` in 1.0.

See [Writing a Parser](../plugins/writing-a-parser.md).

## Masking

The core redacts (`***`) every value it resolved from `env()`, `file()` or
`secret()` templates from: plugin log lines (fd4), the texts of
`configure_ack`, `finished` and `resolved` errors, the `cancel`/`error`
reasons it sends to plugins, plugin stderr, the final `error:` line, the
summary's root cause and the JSON run summary's `cause`, and — with telemetry
on — everything it exports: span names, string attributes, events and status
messages, log bodies and attributes (a plugin's included). Each line of at least

8 characters of a multi-line value (a PEM key) and the value's JSON-escaped
spelling are masked too. A plugin should still never log a value.

## Ordering guarantees

- The core never sends `start` before receiving `configure_ack: ok`.
- The core never starts the data pump before receiving both `schema` (from source)
  and `ready` (from destination).
- A source writes the Arrow IPC end-of-stream marker on fd3 only after its last
  batch succeeded, then sends `finished`.
- **Commit point:** the core writes the end-of-stream marker to the
  destination's fd3 only after the source's stream ended with its marker
  **and** the source sent `finished` with `status: "success"` (with a parser:
  the parser's stream and `finished`, **and** the raw source's `finished`). A
  `finished` that arrives before the core read the stream header is kept for
  this check. A destination
  finalizes (commits) only on that clean end of input; a stream that ends
  without the marker is truncated and must be rolled back.
- `finished` is always the last message a plugin sends, after any cleanup.
  After sending it, the plugin must close fd3 and fd4 and exit.

## Abort rules

The core aborts the run on the first failure: a plugin that crashes or exits
without `finished`, a `finished` with `status: "error"`, a rejected config, a
failed capability gate, a protocol violation, two malformed lines in a row, a
[timeout](#timeouts), a stalled data plane, or SIGINT/SIGTERM. Then it:

1. stops forwarding batches and **never** ends the destination's fd3 cleanly
   (nor a parser's stdin);
2. sends `cancel` to every plugin still running, and `error` instead to the
   plugin at fault, with a code from the
   [`error` reference](./messages.md#error);
3. waits `timeouts.cancel_grace_seconds` (default 30) for each plugin to exit,
   recording any `finished` it sends, then kills each plugin's process group
   (SIGKILL);
4. closes the destination's fd3 only after the destination is gone, so even a
   plugin that ignores `cancel` never sees a clean end of input;
5. releases the state lock (only durably committed watermarks are persisted);
6. prints the summary with `Status: error` (or `cancelled` for a signal) and
   the root cause, and exits with the matching
   [exit code](../reference/cli.md#exit-codes).

A failure before data flows (a missing or incompatible plugin, a rejected
config, the state lock) ends the run the same way, but the cause is printed as
an error instead of a summary.

On `cancel` (or `error`), a plugin stops, undoes its partial work — a
destination rolls back its transaction or deletes partial files; a source
releases its cursor — and exits, replying `finished` with `status:
"cancelled"` to a `cancel`. It never finalizes. A plugin must keep reading
stdin while it moves data, so `cancel` can reach it mid-stream; the SDK does
this. A plugin should treat an unexpected EOF on stdin, or a fd3 input that
ends without the end-of-stream marker, the same way: the core is gone.

Each plugin runs in its own process group: a terminal's Ctrl+C reaches only
the core, which cancels the plugins gracefully. The engine handles SIGINT and
SIGTERM explicitly because it is PID 1 in the container images, where an
unhandled SIGTERM would be ignored.

A sink is cancelled too when the run aborts. Objects it already delivered stay
delivered: a sink is not transactional.

## Timeouts

Every wait is bounded by `pipeline.timeouts` (see the
[pipeline reference](../reference/pipeline-yaml.md#pipelinetimeouts)). A
timeout aborts the run and sends `error` with code `TIMEOUT` to the late
plugin. The data-plane stall timeout is opt-in (`stall_seconds`, off by
default); with it on, the first batch gets its own, generous budget
(`first_batch_seconds`) because some sources buffer their whole result before
they emit a batch.

## Implementing the protocol in another language

The protocol is transport-agnostic JSON. Any language that can:
- Read/write newline-delimited JSON (JSONL) on stdin/stdout
- Write Apache Arrow IPC to fd3 (via `arrow::ipc::writer::StreamWriter`), or,
  for a destination, read an Arrow IPC stream from fd3
- Write JSONL to fd4

can implement a loadsmith plugin (telemetry is optional: a plugin that does not
advertise `handshake_ack.telemetry` simply takes no part in it). The message
schema is in [Message Reference](./messages.md).


