# Architecture Overview

Loadsmith is structured as a set of Rust crates that form a strict dependency
hierarchy, plus plugin binaries that live outside the core dependency graph.

## Crate map

```
loadsmith-cli               ← the loadsmith binary (clap), plugin install/uninstall
  └─ loadsmith-core         ← orchestration: pipeline loader, providers, discovery, spawner, lifecycle, pump, events,
  │                           reporter, summary, sink supervisor, incremental state
  │    └─ loadsmith-config  ← YAML parse, static checks, template resolution (env/file/secret), masking
  │    └─ loadsmith-arrow   ← Arrow IPC reader/writer, schema helpers, JSON conversion
  │    └─ loadsmith-protocol← control message types (serialization/deserialization)
  │    └─ loadsmith-transport← JSONL framing for control + event channels (uses protocol, arrow)
  └─ loadsmith-config
  └─ loadsmith-plugin-manifest ← plugin manifest schema + validation for `plugin install`
  └─ loadsmith-tls          ← rustls ClientConfig + TLS modes (installer HTTPS; network plugins)

loadsmith-plugin-sdk        ← drives plugin lifecycle; source/destination/sink/provider entry points
  └─ loadsmith-protocol
  └─ loadsmith-transport
  └─ loadsmith-arrow
```

Plugin binaries depend on `loadsmith-plugin-sdk` (plus, optionally,
`loadsmith-arrow` and `loadsmith-tls`). They never depend on `loadsmith-core` —
the protocol is the only coupling.

## The plugin model

A **plugin** is an ordinary OS binary that implements the loadsmith protocol over
file descriptors. The core spawns it as a child process, wires up the three
communication channels, and orchestrates its lifecycle.

There are four runnable kinds of plugin:

| Kind | Role | SDK entry point |
|---|---|---|
| `source` | Reads from an origin system and emits Arrow batches | `run_source` |
| `destination` | Consumes Arrow batches and writes them to a target | `run_destination` |
| `sink` | Delivers finalized staged files to a target (see [Sink Delivery](./sink-delivery.md)) | `run_sink` |
| `provider` | Resolves the pipeline's `secret('<instance>', '<ref>')` values before the run (`resolve` → `resolved`) | `run_provider` |

`parser` is a reserved kind: its protocol messages (`parse_object`,
`object_parsed`) are defined in v1, but the core does not drive parsers yet and
the SDK has no parser runtime.

Providers run before anything else: the core's loader (`load.rs`) checks the
pipeline file statically, starts each provider instance a `secret()` call
references (`providers.rs`, all in parallel), substitutes the values, and
only then takes the state lock and spawns the data plugins. Every resolved
value goes into a shared mask that redacts it from plugin logs, plugin
stderr, error texts and the summary.

### Binary naming convention

The core discovers plugins by resolving the binary name from the pipeline's
`source.type`, `destination.type`, (when present) `sink.type`, and each
`providers.<name>.type` that a `secret()` call references:

```
loadsmith-{kind}-{type}
```

Examples:

| `type` in pipeline | Kind | Binary name |
|---|---|---|
| `postgres` | source | `loadsmith-source-postgres` |
| `jsonl` | destination | `loadsmith-destination-jsonl` |
| `local-copy` | sink | `loadsmith-sink-local-copy` |
| `file` (under `providers:`) | provider | `loadsmith-provider-file` |

The core searches for the binary first in the plugin directory (default
`~/.loadsmith/plugins/`, override with `--plugin-dir`), then in `PATH`.

## Process isolation in practice

Each `loadsmith run` spawns two child processes — the source plugin and the
destination plugin — plus a sink process when a `sink:` block is configured, and,
before those, one short-lived process per referenced provider instance (see
[Sink Delivery](./sink-delivery.md)). The core communicates with them via file descriptors — no
network sockets, no shared memory, no IPC libraries.

```
   ┌──────────────────────────────────────────────┐
   │                  loadsmith                    │
   │  ┌──────────┐  pump  ┌───────────────────┐   │
   │  │ source   │───fd3──│  destination      │   │
   │  │ plugin   │        │  plugin           │   │
   │  └──────────┘        └───────────────────┘   │
   │  stdin/stdout          stdin/stdout           │
   │  (control JSONL)       (control JSONL)        │
   │  fd4 (events)          fd4 (events)           │
   └──────────────────────────────────────────────┘
```

If a plugin crashes:
- Its file descriptors are closed by the OS.
- The core detects EOF on the control channel (or a data stream that ends
  without its end-of-stream marker) and aborts the run.
- The other plugin gets `cancel`, undoes its partial work and exits; after a
  grace period, whatever is left is killed. The destination never sees a clean
  end of input after an upstream failure, so it never commits a partial load.
  (A crashed sink is the exception — its supervisor respawns it.)
- The summary reports `Status: error` with the root cause, and the run exits
  `1`. See the [abort rules](../protocol/reference.md#abort-rules).

## Why Rust

The core's entire job is reliable I/O orchestration: spawning processes, wiring
file descriptors, pumping binary streams, and enforcing a protocol. Rust's
ownership model eliminates whole classes of bugs (double-close, use-after-free on
raw fds) that plague C equivalents. Tokio's async runtime lets the core drain
multiple channels concurrently without threads for each.

`unsafe` is confined to file-descriptor plumbing: the pipe setup and the
`pre_exec` hook in
[`spawner.rs`](https://github.com/loadsmith-el/loadsmith/blob/main/crates/loadsmith-core/src/spawner.rs),
and the SDK adopting fd3/fd4 (`File::from_raw_fd`) in the plugin runtimes. The
`pre_exec` hook runs in the forked child and must only call async-signal-safe
functions (`dup2`, `close` via libc).
