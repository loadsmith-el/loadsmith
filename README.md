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

- **Plugin-first.** Sources, destinations and sinks are separate executables. The
  core knows nothing about Postgres, S3 or Parquet — that knowledge lives in
  plugins. Plugins can be written in any language that speaks the protocol.
  Provider plugins resolve `{{ secret() }}` values before a run. (Parser
  plugins are defined by the protocol but not yet wired into the core.)
- **Real streaming.** Data flows in Apache Arrow batches with server-side cursors
  where available, so a 100M-row table never has to fit in RAM.
- **Three separate planes.** Control, data, and events each get their own channel,
  so heavy data traffic never competes with logs or protocol messages.
- **Typed core.** Configuration is YAML on the outside, strongly-typed Rust structs
  on the inside (via Serde). The core is strict where it owns the contract and
  delegates plugin config to the plugin that understands it.
- **Observable from day one.** Live progress, a final run summary with throughput,
  structured logs, and `--log-level debug` to watch the full protocol handshake.
  OpenTelemetry built in: traces, metrics and logs of every run — the plugins'
  included — exported over OTLP by the core (`--otel-endpoint`, or the
  standard `OTEL_*` variables).


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
  ├── parsers         raw files → Arrow (a raw source announces files, the
  │                   core hands each to the parser, e.g. file → csv)
  └── providers       secret() values (e.g. file), run by the core before the
                      data plugins; {{ env() }} / {{ file() }} need none

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
| `loadsmith-plugin-manifest` | the `loadsmith-plugin.yaml` manifest schema + validation, used by `plugin install` |
| `loadsmith-tls` | shared rustls + rustls-rustcrypto TLS config for network plugins |
| `loadsmith-aws` | AWS credential chain, SigV4 signing and a minimal S3 client (core + AWS plugins) |
| `loadsmith-core` | orchestration, the data pump, the run summary |
| `loadsmith-cli` | the `loadsmith` binary |

Plugins live in their own repo,
[`loadsmith-canonical-plugins`](https://github.com/loadsmith-el/loadsmith-canonical-plugins)
— standalone binaries (`loadsmith-source-postgres`, `loadsmith-destination-jsonl`,
`loadsmith-destination-parquet`, `loadsmith-sink-local-copy`, …) installed on
demand with `loadsmith plugin install <name>`. This repo is the **core + the SDK
crates** those plugins build against. Releases ship static binaries and four
image flavors, from a ~13MB distroless `-static` to a `-bundled` image with every
canonical plugin (see
[Container Images](docs/src/getting-started/container-images.md)). To go from a
pipeline to a tested production image, see
[Build an EL, End to End](docs/src/getting-started/build-an-el.md).

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

The destination only sees the end of its input — and commits — after the source
finished successfully. If anything fails first (a plugin crash or error, a
timeout, Ctrl+C or SIGTERM), the core cancels the plugins instead: the
destination rolls back, the summary names the root cause, and the exit code
says what kind of failure it was (see the
[CLI reference](docs/src/reference/cli.md#exit-codes)).

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

A key Loadsmith does not know, in the file or in a plugin's `config:` (a typo
such as `batch_sise`), fails the run with every unknown key named;
`loadsmith run --ignore-unknown-keys` turns them into warnings instead (see
[Unknown keys](docs/src/reference/cli.md#unknown-keys)).

### Run it

```bash
loadsmith run pipeline.yaml   # discovers plugins in ~/.loadsmith/plugins
```

The header and the summary box go to stdout; schema and progress lines are
timestamped `INFO` log lines on stderr (shortened here):

```text
loadsmith 0.2.0
postgres → jsonl

… INFO loadsmith_core::reporter: schema negotiated — 34 columns
… INFO loadsmith_core::reporter: id: utf8
… INFO loadsmith_core::reporter: spacecraft_id: utf8
… INFO loadsmith_core::reporter: event_sequence: int64
… INFO loadsmith_core::reporter: event_timestamp: timestamp(ms)
…
… INFO loadsmith_core::reporter: 2,000 rows · 1 batch
… INFO loadsmith_core::reporter: 4,000 rows · 2 batches
…
… INFO loadsmith_core::reporter: 100,000 rows · 50 batches

───────────────────────────────────────────
Pipeline:     postgres-to-jsonl
Route:        loadsmith-source-postgres → loadsmith-destination-jsonl
Status:       success
Rows read:    100,000
Rows written: 100,000
Batches:      50
Duration:     00:00:03.137
Throughput:   31,877 rows/s
───────────────────────────────────────────
```

For automation, `--output json` puts one versioned JSON run summary on stdout
instead (status, exit code, root cause, route with plugin versions, counts,
timings, watermarks), and `--summary-file <path>` writes the same document to
a file; see the [CLI reference](docs/src/reference/cli.md#machine-readable-run-summary).

---

## CLI

```bash
loadsmith run pipeline.yaml                 # run a pipeline
loadsmith run pipeline.yaml --dry-run       # resolve everything (providers too), run nothing
loadsmith run pipeline.yaml --print-resolved-config   # show resolved config (secrets masked)
loadsmith run pipeline.yaml --output json   # one JSON run summary on stdout
loadsmith run pipeline.yaml --summary-file run.json   # also write it to a file
loadsmith run pipeline.yaml --plugin-dir ../loadsmith-canonical-plugins/target/debug

loadsmith validate pipeline.yaml            # static check, evaluates nothing (--resolve: full)
loadsmith plugin list                       # list installed plugins
loadsmith plugin install postgres           # a whole package (source + destination)
loadsmith plugin install mysql:source@0.1.1 # one kind, pinned version (or --kind source)
loadsmith plugin install --all              # every package in the canonical index
loadsmith plugin install -f loadsmith-plugin.yaml      # from a manifest (path or URL)
loadsmith plugin install --binary ./my-plugin-binary   # a local binary, no manifest
loadsmith plugin uninstall postgres         # remove a package (or postgres:source)
loadsmith state show pipeline.yaml          # show a pipeline's persisted watermark
loadsmith state rm pipeline.yaml            # reset incremental state (next run is full)

# Global flags
--log-level trace|debug|info|warn|error     # debug shows the full handshake
--no-color                                  # plain output for files / CloudWatch
```

Plugins are looked up as `loadsmith-{kind}-{type}` in the plugin dir
(`~/.loadsmith/plugins/` by default, override with `--plugin-dir`), then on `PATH`
as a fallback (the `-bundled` image relies on it). `list`, `install`, `uninstall`,
`run` and `validate` all take `--plugin-dir`; `install` also takes `--index` to
use another plugin index. See the
[CLI reference](docs/src/reference/cli.md) for every flag.

---

## Testing

The fastest unit feedback is the workspace test suite. CI
([`ci.yml`](.github/workflows/ci.yml)) gates every push and PR to `main` on the
same commands, with the toolchain pinned in `rust-toolchain.toml`:

```bash
cargo build --locked
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
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

Loadsmith is at **v0.2.0** — the architecture is proven end-to-end:

- ✅ Core orchestration, process-isolated plugins, the data pump
- ✅ Control plane (JSONL), data plane (Arrow IPC), event plane (JSONL)
- ✅ Canonical plugins: `postgres`, `mysql` and `oracle` source + destination
  (`atomic` / `staged_merge`), `sharepoint` source, `jsonl` / `null` / `parquet`
  destinations, `local-copy` sink, and the `file` provider (being ported to the
  new `provider` kind)
- ✅ Sink delivery stage (supervised, respawn + resume from the delivery ledger)
- ✅ `loadsmith plugin install` / `uninstall` from the canonical index
  (sha256-verified, per-kind, pinned versions)
- ✅ Static musl binaries and four image flavors (`-static`, `-static-debug`,
  `-slim` = `:latest`, `-bundled`)
- ✅ `{{ env() }}` / `{{ file() }}` / `{{ secret() }}` config templates; provider
  plugins resolve `secret()` before a run; every resolved value is masked in
  logs, plugin stderr, errors, the run summary and `--print-resolved-config`
- ✅ Live progress + run summary, `--log-level`, `--no-color`
- ✅ Incremental state & checkpoints (core-owned watermark, locking, `loadsmith
  state`) in a local file or an S3 object (`backend: s3`, locked by conditional
  writes); at-least-once, exactly-once with an idempotent destination
- ✅ Pipeline files read from S3 (`loadsmith run s3://bucket/key`)
- ✅ Parser stage: a raw source announces files, a parser turns them into
  Arrow (`ParserPlugin` / `ObjectSourcePlugin` in the SDK); a failure
  upstream never commits a partial load
- 🔜 More providers (Vault, other cloud secret managers; `aws-secrets-manager`
  ships in the canonical plugins), whole-document `--config-ref` through a
  provider (1.x), more plugins — see [ROADMAP.md](ROADMAP.md)

What a version number promises (and what 1.0 will freeze: the wire protocol,
CLI, `pipeline.yaml`, state format, plugin manifest/index, image tags and plugin
discovery) is set out in the
[Stability Policy](docs/src/reference/stability.md). Changes are tracked in
[CHANGELOG.md](CHANGELOG.md).

The full design rationale lives in [`definitions/loadsmith.md`](definitions/loadsmith.md)
and the protocol in [`definitions/protocol.md`](definitions/protocol.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
