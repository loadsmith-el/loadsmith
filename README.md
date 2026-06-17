# Loadsmith

> 📖 **Full documentation:** <https://loadsmith-el.github.io/loadsmith/>

**A modern, plugin-first EL (Extract & Load) tool for declarative data pipelines.**

Loadsmith moves data from a source to a destination with real streaming, low
memory use, and good observability. It's inspired by Embulk's declarative,
plugin-driven model, rebuilt around a small typed core, process-isolated plugins,
and a clean separation between control, data, and event channels.

> Loadsmith is an EL tool, not a transformation engine. It deliberately leaves
> heavy SQL, joins, and analytics to the database or to tools like dbt/Spark. Its
> job is to move data **predictably**, in batches, without loading whole datasets
> into memory.

---

## Why Loadsmith

- **Plugin-first.** Sources, destinations, parsers and config/secret providers are
  separate executables. The core knows nothing about Postgres, S3 or Parquet —
  that knowledge lives in plugins. Plugins can be written in any language that
  speaks the protocol.
- **Real streaming.** Data flows in Apache Arrow batches with server-side cursors
  where available, so a 100M-row table never has to fit in RAM.
- **Three separate planes.** Control, data, and events each get their own channel,
  so heavy data traffic never competes with logs or protocol messages.
- **Typed core.** Configuration is YAML on the outside, strongly-typed Rust structs
  on the inside (via Serde). The core is strict where it owns the contract and
  delegates plugin config to the plugin that understands it.
- **Observable from day one.** Live progress, a final run summary with throughput,
  structured logs, and `--log-level debug` to watch the full protocol handshake.

---

## Architecture

```text
Loadsmith Core (the control plane)
  ├── YAML config → typed Rust structs (Serde)
  ├── template & secret resolution
  ├── plugin discovery & process spawning
  ├── protocol + capability negotiation
  ├── the data pump (source → destination)
  └── lifecycle, cancellation, metrics, final summary

Plugins (separate processes)
  ├── sources         read data        (e.g. postgres)
  ├── destinations    write data       (e.g. postgres, jsonl, parquet)
  ├── sinks           deliver files    (e.g. local-copy → later s3)
  ├── parsers         raw → tabular
  └── providers       config / secrets (e.g. file)

Channels
  ├── control plane : JSONL over stdin/stdout
  ├── data plane    : Apache Arrow IPC over fd3
  └── event plane   : JSONL over fd4 (progress, logs, object-ready)
```

The workspace is split into focused crates:

| Crate | Responsibility |
|---|---|
| `loadsmith-protocol` | JSONL control-message types, versioned from `1` |
| `loadsmith-transport` | control/event framing over pipes |
| `loadsmith-arrow` | Arrow IPC read/write + schema helpers |
| `loadsmith-plugin-sdk` | drive the full plugin lifecycle; you implement the data |
| `loadsmith-config` | YAML parsing, validation, templates, secret masking |
| `loadsmith-core` | orchestration, the data pump, the run summary |
| `loadsmith-cli` | the `loadsmith` binary |

Plugins live in their own repo,
[`loadsmith-canonical-plugins`](https://github.com/loadsmith-el/loadsmith-canonical-plugins)
— standalone binaries (`loadsmith-source-postgres`, `loadsmith-destination-jsonl`,
`loadsmith-destination-parquet`, `loadsmith-sink-local-copy`, …) installed on
demand with `loadsmith plugin install <name>`. This repo is the **core + the SDK
crates** those plugins build against. The official image is slim (core only).

A **sink** is an optional delivery stage that separates *format* from *location*:
the destination writes files (Parquet, CSV) to a local staging dir, and the sink
delivers each finalized file to a remote target (S3, GCS, …) — so "Parquet on S3"
is `destination: parquet` + `sink: s3`, not a combinatorial `s3_parquet` plugin.
The sink runs under a core supervisor that respawns and resumes it if it fails.
See [Sink Delivery](docs/src/architecture/sink-delivery.md).

---

## Execution & data flow

Loadsmith sits **in the middle** of the data path. A source never talks to a
destination directly — every batch passes through the core, which is what lets
Loadsmith count rows, report progress, and (later) apply light transforms.

```text
          control plane (JSONL)                control plane (JSONL)
        ┌──────────◀──────────┐              ┌──────────◀──────────┐
        │                     │              │                     │
   ┌────▼─────┐   fd3 Arrow   │   ┌──────────▼─────────┐  fd3 Arrow │  ┌──────────────┐
   │  source  │ ────────────────▶ │  loadsmith core    │ ──────────────▶ │ destination  │
   │  plugin  │                    │   (the pump)       │                  │   plugin     │
   └────┬─────┘                    └──────────┬─────────┘                  └──────┬───────┘
        │  fd4 events (JSONL) ───────────────▶│◀─────────────── fd4 events (JSONL)│
        └─────────────────────────────────────┘────────────────────────────────────┘
```

A run proceeds through a fixed lifecycle (visible with `--log-level debug`):

```text
handshake → negotiate protocol version → capabilities → configure
  → start → source emits schema, destination signals ready
  → pump: core copies every Arrow batch source ▶ destination, reporting progress
  → finished (rows read / written, batches, duration, throughput)
```

---

## Quickstart

### Build

```bash
cargo build                 # produces target/debug/loadsmith (the core)
loadsmith plugin install --all   # fetch the canonical plugins → ~/.loadsmith/plugins
```

### A pipeline

```yaml
# pipeline.yaml
pipeline:
  name: postgres-to-jsonl

source:
  type: postgres
  config:
    host: 127.0.0.1
    port: 5432
    dbname: lab
    user: lab
    password: lab
    query: "SELECT * FROM spacecraft_telemetry_events ORDER BY event_sequence"
    batch_size: 2000

destination:
  type: jsonl
  config:
    path: /tmp/events.jsonl
```

### Run it

```bash
loadsmith run pipeline.yaml   # discovers plugins in ~/.loadsmith/plugins
```

```text
loadsmith 0.1.0
postgres → jsonl

  schema negotiated — 34 columns
  id: utf8
  spacecraft_id: utf8
  event_sequence: int64
  sensor_name: utf8
  reading_double: float64
  is_anomaly: bool
  event_timestamp: timestamp_ms
  ...

           2,000 rows · 1 batch
           4,000 rows · 2 batches
          ...
         100,000 rows · 50 batches

───────────────────────────────────────────
Pipeline:     postgres-to-jsonl
Route:        loadsmith-source-postgres → loadsmith-destination-jsonl
Status:       success
Rows read:    100,000
Rows written: 100,000
Duration:     00:00:03
Throughput:   31,888 rows/s
───────────────────────────────────────────
```

---

## CLI

```bash
loadsmith run pipeline.yaml                 # run a pipeline
loadsmith run pipeline.yaml --dry-run       # validate without executing
loadsmith run pipeline.yaml --print-resolved-config   # show resolved config (secrets masked)
loadsmith run pipeline.yaml --plugin-dir ./target/debug

loadsmith validate pipeline.yaml            # validate config only
loadsmith plugin list                       # list installed plugins
loadsmith plugin install ./my-plugin-binary # install a plugin
loadsmith state show pipeline.yaml          # show a pipeline's persisted watermark
loadsmith state rm pipeline.yaml            # reset incremental state (next run is full)

# Global flags
--log-level trace|debug|info|warn|error     # debug shows the full handshake
--no-color                                  # plain output for files / CloudWatch
```

Plugins are discovered in `~/.loadsmith/plugins/` by default (override with
`LOADSMITH_PLUGIN_PATH` or `--plugin-dir`). The core only runs binaries that are
explicitly installed — it never scans `PATH`.

---

## Testing

The fastest unit feedback is the workspace test suite:

```bash
cargo test --workspace
```

But the **best way to validate a real pipeline end-to-end** is
**[loadsmith-lab](https://github.com/loadsmith-el/loadsmith-lab)** — a companion harness that spins up real
services in Docker, seeds them with canonical data, runs Loadsmith against them,
and checks the output. It's how the Postgres → JSONL path above is verified on
every change:

```bash
# from the loadsmith-lab repo
loadsmith-lab run --loadsmith ../loadsmith --select catalog/postgres-to-jsonl
```

---

## Status

Loadsmith is at **v0.1.0** — the architecture is proven end-to-end:

- ✅ Core orchestration, process-isolated plugins, the data pump
- ✅ Control plane (JSONL), data plane (Arrow IPC), event plane (JSONL)
- ✅ Plugins: `postgres` source (server-side cursor) & `postgres` destination
  (`atomic` / `staged_merge`), `jsonl` & `parquet` destinations, `file` provider
- ✅ Live progress + run summary, `--log-level`, `--no-color`
- ✅ Incremental state & checkpoints (core-owned watermark, locking, `loadsmith
  state`); at-least-once, exactly-once with an idempotent destination
- 🔜 Templates & secret providers, remote (`s3`) state backend, more plugins

The full design rationale lives in [`definitions/loadsmith.md`](definitions/loadsmith.md)
and the protocol in [`definitions/protocol.md`](definitions/protocol.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
