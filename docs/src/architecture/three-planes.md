# The Three Planes

Every plugin process is connected to the loadsmith core through exactly three
communication channels, each assigned to a specific file descriptor. These
channels are completely separate by design: different fd numbers, different formats,
different purposes, different failure modes.

```
Plugin process
  fd0 (stdin)  ←──── JSONL ──── core    control plane (core → plugin)
  fd1 (stdout) ────► JSONL ──── core    control plane (plugin → core)
  fd3          ────► Arrow IPC ─ core    data plane    (source → core)
               ←──── Arrow IPC ─ core    data plane    (core → destination)
  fd4          ────► JSONL ──── core    event plane   (plugin → core)
```

fd0 and fd1 are the standard process stdin/stdout — the core wires them to
`ChildStdin`/`ChildStdout` pipes. fd3 and fd4 are created by the core as raw OS
pipes and passed to the child via `dup2` in a `pre_exec` hook.

## Control plane — fd0/fd1

The control plane carries **lifecycle messages**: handshake, capabilities,
configuration, schema, start signals, and the final `Finished` message. All
messages are JSONL (newline-delimited JSON) with a type discriminator field:

```json
{"type": "handshake"}
{"type": "handshake_ack", "plugin_name": "loadsmith-source-postgres", "plugin_version": "0.1.0", "protocol_supported_versions": [1], "kind": "source"}
{"type": "set_protocol_version", "protocol_version": 1}
```

The transport layer (`loadsmith-transport`) adds a length prefix to each line and
reads/writes with `BufReader`/`BufWriter` for efficiency. The core uses
`ControlReader` to deserialize incoming messages and `ControlWriter` to serialize
outgoing ones.

**Why stdin/stdout?** They are universally available, work across all Unix
platforms, and require no additional setup. A plugin author can test their plugin
by piping JSON to stdin and reading JSON from stdout.

## Data plane — fd3

The data plane carries **Apache Arrow IPC streaming batches**. It is a separate
file descriptor — never mixed with the control channel — for several reasons:

1. **Volume**: batches can be hundreds of megabytes. Muxing them with JSONL would
   require complex framing and hurt throughput.
2. **Format**: Arrow IPC is a binary format designed for zero-copy reads. Writing
   it to a dedicated fd allows direct kernel I/O with no intermediate buffering.
3. **Backpressure**: a full pipe blocks the writer naturally. If the destination
   is slower than the source, the pump blocks and the source slows down. No
   explicit flow control is needed.

The source plugin writes an IPC stream (schema header + record batches) to its
fd3. The core reads this stream, relays each batch to the destination's fd3, and
counts rows as it goes.

**Important:** the source's fd3 (write end) and the destination's fd3 (read end)
are **not the same pipe**. The core sits in the middle with two separate pipes:

```
source ──[in_pipe write]──► [in_pipe read]  core  [out_pipe write]──► [out_pipe read]── destination
```

This is mandatory. See [Data Flow](./data-flow.md).

## Event plane — fd4

The event plane carries **plugin-emitted events**: log messages and progress
updates. Plugins write JSONL events to fd4 at any point during execution; the
core drains this channel concurrently and routes each event to the tracing system.

```json
{"type": "log", "level": "info", "message": "cursor opened, fetching batches"}
{"type": "progress", "rows_read": 2000, "batches_read": 1}
{"type": "progress", "rows_read": 4000, "batches_read": 2}
{"type": "object_ready", "path": "/scratch/events.00000001.snappy.parquet"}
```

A file-output destination also emits `object_ready` here as each staged file is
finalized; the core forwards the path to the sink supervisor. Putting it on the
event plane (already drained concurrently) is what lets delivery overlap the
pump — see [Sink Delivery](./sink-delivery.md).

**Why a separate fd?** If plugins wrote log events to stdout (their control
channel), the core would need to mux progress events and control messages and
demux them on the read side — fragile and error-prone. A dedicated fd keeps the
channels independent.

**The draining requirement.** fd4 is backed by an OS pipe with a finite kernel
buffer (typically 64 KB on Linux). If the core does not read from it, a plugin
that writes many events will block waiting for the pipe to drain. Because the
data pump is a blocking operation (it runs in `tokio::task::spawn_blocking`), the
core must drain fd4 concurrently — it does this with a `tokio::spawn` task for
each plugin before the pump starts. Failure to drain fd4 would cause a deadlock:
the plugin blocks writing events; the pump blocks waiting for more data; nobody
makes progress.

## Separation of concerns

| Channel | Format | Direction | What goes here |
|---|---|---|---|
| fd0 (stdin) | JSONL | core → plugin | Lifecycle commands (configure, start, cancel) |
| fd1 (stdout) | JSONL | plugin → core | Lifecycle responses (ack, schema, ready, finished) |
| fd3 | Arrow IPC | source→core, core→dest | Record batches (a sink has no fd3) |
| fd4 | JSONL | plugin → core | Log events, progress updates, `object_ready` |

No other information crosses these boundaries. A plugin does not know what the
core will do with its data, and the core does not know how the plugin stores its
state. The file descriptors are the complete interface.
