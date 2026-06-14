# Architecture Overview

Loadsmith is structured as a set of Rust crates that form a strict dependency
hierarchy, plus plugin binaries that live outside the core dependency graph.

## Crate map

```
loadsmith-cli               ← the loadsmith binary (clap)
  └─ loadsmith-core         ← orchestration: spawner, lifecycle, pump, events, summary
       └─ loadsmith-config  ← YAML parse, template resolution, secret masking
       └─ loadsmith-arrow   ← Arrow IPC reader/writer, schema helpers, JSON conversion
       └─ loadsmith-protocol← control message types (serialization/deserialization)
       └─ loadsmith-transport← JSONL framing for control + event channels

loadsmith-plugin-sdk        ← drives plugin lifecycle; source/destination/provider entry points
  └─ loadsmith-protocol
  └─ loadsmith-transport
  └─ loadsmith-arrow
```

Plugin binaries depend only on `loadsmith-plugin-sdk`. They do not depend on
`loadsmith-core` — the protocol is the only coupling.

## The plugin model

A **plugin** is an ordinary OS binary that implements the loadsmith protocol over
file descriptors. The core spawns it as a child process, wires up the three
communication channels, and orchestrates its lifecycle.

There are three kinds of plugin:

| Kind | Role | SDK entry point |
|---|---|---|
| `source` | Reads from an origin system and emits Arrow batches | `run_source` |
| `destination` | Consumes Arrow batches and writes to a sink | `run_destination` |
| `config-provider` | Loads configuration YAML content from a URI | `run_config_provider` |

### Binary naming convention

The core discovers plugins by resolving the binary name from the pipeline's
`source.type` and `destination.type` fields:

```
loadsmith-{kind}-{type}
```

Examples:

| `type` in pipeline | Kind | Binary name |
|---|---|---|
| `postgres` | source | `loadsmith-source-postgres` |
| `jsonl` | destination | `loadsmith-destination-jsonl` |
| `file` | config-provider | `loadsmith-config-provider-file` |

The core searches for the binary first in the plugin directory (default
`~/.loadsmith/plugins/`, override with `--plugin-dir` or `LOADSMITH_PLUGIN_PATH`),
then in `PATH`.

## Process isolation in practice

Each `loadsmith run` spawns exactly two child processes: the source plugin and the
destination plugin. The core communicates with them via file descriptors — no
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
- The core detects EOF on the control channel and reports the failure.
- The other plugin receives a `Cancel` message and is given a chance to clean up.

## Why Rust

The core's entire job is reliable I/O orchestration: spawning processes, wiring
file descriptors, pumping binary streams, and enforcing a protocol. Rust's
ownership model eliminates whole classes of bugs (double-close, use-after-free on
raw fds) that plague C equivalents. Tokio's async runtime lets the core drain
multiple channels concurrently without threads for each.

The single `unsafe` block in the entire project is in
[`spawner.rs`](https://github.com/loadsmith-el/loadsmith/blob/main/crates/loadsmith-core/src/spawner.rs), inside a `pre_exec`
hook that runs in the forked child and must only call async-signal-safe functions
(`dup2`, `close` via libc).
