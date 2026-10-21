# Telemetry (OpenTelemetry)

Loadsmith exports **traces, metrics and logs** of every run over **OTLP**
(gRPC or http/protobuf), to any OpenTelemetry Collector or OTLP-speaking
backend. Telemetry is **off by default** and costs nothing when off.

This page is the design record (approved 2026-10-21) and the reference for
how it works. The user-facing settings are in the
[pipeline YAML reference](../reference/pipeline-yaml.md#telemetry) and the
[CLI reference](../reference/cli.md#telemetry-flags); the wire messages in the
[message reference](../protocol/messages.md#telemetry-on-the-event-channel).

## Principles

These are fixed decisions:

- Three signals: traces, metrics, logs. Export via OTLP only (gRPC and
  http/protobuf).
- **Centralized export:** plugins instrument; the core is the **only**
  exporter. Plugins never open network connections for telemetry and carry no
  OTLP/TLS stack.
- Config precedence: CLI flags > `telemetry:` block in `pipeline.yaml` >
  standard `OTEL_*` env vars > defaults. Off by default; enabled when an
  endpoint is resolved (or `telemetry.enabled: true`). `--no-telemetry` /
  `telemetry.enabled: false` always wins off.
- Zero cost when off: plugins are told telemetry is disabled and record
  nothing.
- Part of protocol v1. Additive and tolerant: old plugins (no telemetry) and
  old cores (which skip unknown fd4 types) keep working.
- Standardize, never cut: OTel semantic conventions everywhere (`db.*`,
  `http.*`, …).

## Architecture

```text
 plugin process                      core (loadsmith)                    collector
┌──────────────────────┐  fd4   ┌───────────────────────────────┐  OTLP  ┌────────┐
│ tracing spans/events │──────▶ │ event drain → PluginScope     │──────▶ │        │
│ SDK instruments      │ span   │   re-emit under the run trace │ grpc / │  OTel  │
│ (in-process, delta)  │ metrics│ core tracing → OTel bridge    │ http   │        │
│ no network           │ log    │ core metrics, core logs       │        │        │
└──────────────────────┘        └───────────────────────────────┘        └────────┘
```

- The plugin SDK records in-process and writes `span`, `metrics` and `log`
  messages to fd4 (the event channel), so plugins stay free of any OTLP or
  network code.
- The core turns its own `tracing` spans into OpenTelemetry spans
  (`tracing-opentelemetry`), exports its log events as OTLP log records, keeps
  its own metrics, and **re-emits** what each plugin sends through the same
  pipelines: plugin spans keep their ids and hang under the core's span for
  that plugin process; plugin metrics and logs get the plugin's identity as
  attributes.
- Everything leaves through one set of exporters (batching, retries and
  timeouts of the OpenTelemetry Rust SDK).

## Configuration

`pipeline.yaml` (all optional; unknown keys are an error like everywhere
else):

```yaml
telemetry:
  enabled: true                 # default: true iff an endpoint is resolved
  endpoint: https://otel:4318   # else OTEL_EXPORTER_OTLP_ENDPOINT (+ per-signal *_TRACES/_METRICS/_LOGS_ENDPOINT)
  protocol: http-protobuf       # grpc | http-protobuf; else OTEL_EXPORTER_OTLP_PROTOCOL; default http-protobuf
  headers: { authorization: "Bearer {{ env('OTEL_TOKEN') }}" }   # over OTEL_EXPORTER_OTLP_HEADERS; values masked
  timeout_seconds: 10           # per export; else OTEL_EXPORTER_OTLP_TIMEOUT (ms); default 10
  signals: [traces, metrics, logs]   # subset; default all (or OTEL_<SIGNAL>_EXPORTER=none drops one)
  sampling: { ratio: 1.0 }      # parent-based ratio; else OTEL_TRACES_SAMPLER(_ARG); default 1.0
  metrics_interval_seconds: 15  # else OTEL_METRIC_EXPORT_INTERVAL (ms); default 15; final flush always
  log_level: info               # minimum level exported as OTLP logs (stderr filtering unchanged)
  resource: { deployment.environment.name: prod }  # over OTEL_RESOURCE_ATTRIBUTES / OTEL_SERVICE_NAME
```

The approved draft spelled the durations as strings (`timeout: 10s`), "like the
rest of the YAML"; the rest of the file actually uses integer `*_seconds`
keys (`heartbeat_seconds`, `pipeline.timeouts.*_seconds`), so the block
follows that convention.

`telemetry:` is resolved **before** the providers start (so their resolution
is part of the traced run): like `pipeline:`, `state:` and `providers:`, it may
use `env()` and `file()` but never `secret()`. Header values are masked
wherever they came from (file, flag or environment).

CLI flags on `run`, also accepted by `validate` for checking:
`--otel-endpoint URL`, `--otel-protocol grpc|http-protobuf`,
`--otel-header KEY=VALUE` (repeatable), `--no-telemetry`. The effective block
is printed (masked) by `--print-resolved-config`. `OTEL_SDK_DISABLED=true` turns
telemetry off unless the file or a flag turns it on. `OTEL_SERVICE_NAME`,
`OTEL_RESOURCE_ATTRIBUTES` and `OTEL_EXPORTER_OTLP_CERTIFICATE` (an extra PEM CA
bundle) are honored.

On or off, in order: `--no-telemetry` or `enabled: false` → off;
`--otel-endpoint`, `enabled: true` or `telemetry.endpoint` → on;
`OTEL_SDK_DISABLED=true` → off; any `OTEL_EXPORTER_OTLP_*ENDPOINT` → on;
otherwise off. With `enabled: true` and no endpoint anywhere, the OTel default
for the protocol is used (`http://localhost:4318` / `:4317`). For
http/protobuf, `/v1/traces`, `/v1/metrics` and `/v1/logs` are appended to a
base endpoint; a per-signal environment endpoint is used as is.

**Exporter errors never fail the run.** The first failed export is reported
once on stderr (`telemetry export failed (…); the run goes on, further export
errors are not reported`); the rest are silent. An exporter that cannot even be
built (unusable endpoint, TLS setup) leaves telemetry off with one warning.
At exit every signal is flushed within the export timeout — within
`cancel_grace_seconds` too after SIGINT/SIGTERM.

Resource: `service.name=loadsmith` (overridable), `service.version`,
`loadsmith.pipeline.name`, `loadsmith.run.id`, `host.name`, `host.arch`,
`os.type`, `process.pid`, `process.executable.name`, `telemetry.sdk.*`.

**Not in 1.0:** the OpenTelemetry declarative configuration file
(`OTEL_CONFIG_FILE`). It may come later, below the flags and the file.

## Protocol (fd4 event plane, additive to v1)

- `handshake_ack.telemetry: 1` — the plugin speaks the telemetry messages.
  Only such a plugin gets `configure.telemetry`.
- `configure.telemetry: {enabled, traceparent?, tracestate?, log_level,
  metrics_interval_ms?}` — W3C trace context of the plugin's parent span (the
  core's span for that plugin process). Absent, disabled or unreadable ⇒ the
  SDK records nothing. A `traceparent` with the sampled flag off ⇒ no spans
  (logs and metrics still flow).
- New fd4 messages, plugin → core: `span` (one finished span) and `metrics`
  (delta-aggregated points).
- `log` gains optional `trace_id`, `span_id`, `time_unix_nano`, `attributes`.

Exact shapes: [message reference](../protocol/messages.md#telemetry-on-the-event-channel).
A core that predates them skips `span`/`metrics` (tolerant reader) and ignores
the extra fields; a plugin that predates them never advertises
`handshake_ack.telemetry` and never sees `configure.telemetry`. Unknown enum
values inside the new messages read as safe defaults (span kind `internal`,
status `unset`; a metric of unknown kind or temporality is skipped).

Volume is bounded in the SDK: at most 1000 spans per second and 10 000 queued
messages; the excess is dropped and counted in
`loadsmith.telemetry.dropped` (attribute `loadsmith.telemetry.signal`).
Instrument per query, batch, request or file — never per row. The core masks
every text it re-emits (span names, string attributes, events, statuses, log
bodies and attributes) with the run's mask list.

## Plugin SDK

The SDK's run entry points (`run_source`, `run_destination`, `run_sink`,
`run_parser`, `run_provider`, `run_object_source`) install a global `tracing`
subscriber with `loadsmith_plugin_sdk::telemetry::layer()`:

- every closed span at `INFO` or above becomes a `span` message, parented on
  the plugin's enclosing span or on `configure.telemetry.traceparent`;
  fields become attributes; `otel.name`, `otel.kind` (`client`, `server`,
  `producer`, `consumer`, `internal`), `otel.status_code` (`ok`, `error`),
  `otel.status_message` and `error` are honored;
- every event at or above the run's `log_level` becomes a `log` message with
  trace context, and a span event of its span; an `error!` event marks its
  span failed;
- `EventSender::log/info/warn/error_msg` carry the trace context of the
  current span.

Metrics are thin delta-aggregating instruments — chosen over the
`opentelemetry` metrics API with an in-process exporter as the lighter
option: no OTel dependency in plugins at all, a few hundred lines, one atomic
load when off:

```rust
use loadsmith_plugin_sdk::telemetry::{counter, histogram, kv};

let attrs = [kv("db.namespace", "public"), kv("db.collection.name", "events")];
let _span = tracing::info_span!("SELECT events", otel.kind = "client", db.namespace = "public").entered();
counter("db.client.rows").with_unit("{row}").add(rows, &attrs);
histogram("db.client.operation.duration").with_unit("s").record(started.elapsed(), &attrs);
```

Instruments: `counter`, `up_down_counter`, `histogram` (bounds: the semconv
duration bounds for unit `s`, else the OTel defaults, or `with_bounds`),
`gauge`. Points flush every `metrics_interval` and at the end of the run;
everything recorded is written to fd4 before the plugin exits.

**Built in, for every plugin:**

| What | Name | Attributes |
|---|---|---|
| span | `configure`, `start`, `finish` | `error` on failure |
| span | `loadsmith.batch` (per batch read or written) | `loadsmith.batch.seq`, `.direction`, `.rows`, `.bytes` |
| span | `loadsmith.parse` / `loadsmith.object` / `loadsmith.deliver` / `loadsmith.resolve` (per file parsed, file produced, object delivered, reference resolved) | `loadsmith.object.path`, `loadsmith.provider.ref` |
| counter | `loadsmith.plugin.rows`, `loadsmith.plugin.batches`, `loadsmith.plugin.bytes` | `direction` |
| histogram | `loadsmith.plugin.batch.duration` (s) | `direction` |

A plugin that sets its own global subscriber must add `telemetry::layer()` to
it. `telemetry::enabled()` tells whether to bother computing an expensive
attribute.

## Core

Exporters: `opentelemetry-otlp` over **reqwest (blocking) + rustls** for
http/protobuf and **tonic + rustls** for gRPC, both with the pure-Rust crypto
provider (`rustls-rustcrypto`) and the webpki roots (multi-arch rule: no
`ring`, no `aws-lc-rs`). The gRPC exporter is the default `grpc` cargo
feature; the release binary stays fully static (musl). Telemetry as a whole
takes the static x86_64 release binary from 8.7 MB to 13.5 MB, of which the
gRPC exporter is 1.5 MB (`--no-default-features` builds an http-only engine,
12.0 MB). Bridged with

`tracing-opentelemetry`; core log events go out through the SDK logger.

**Span tree** of a run (one trace per run):

```text
loadsmith.run                       pipeline, run id, outcome, exit code
├── provider.resolve                instances, references
│   └── plugin provider '<name>'    kind, name, version, pid
│       ├── handshake
│       ├── configure
│       └── … the provider's own spans (loadsmith.resolve …)
├── lock.acquire, state.load        (with state:)
├── plugin <name>                   one per plugin process (respawns counted)
│   ├── handshake
│   ├── configure
│   ├── parser.feed                 (parser: one per file handed over)
│   ├── sink.deliver                (sink: deliver_object → object_delivered)
│   └── … the plugin's own spans (configure, start, loadsmith.batch, …, finish)
├── checkpoint                      (state: intra-run persist)
└── state.commit                    (state: final persist)
```

Span events (not stderr lines): `cancel`, `error` (with `loadsmith.code`) and
`resend` on a plugin's span; `respawn` and `retry` on a respawned sink's
span; schema-mismatch and abort warnings of the core appear as events of
the run span.

**Core metrics:**

| Name | Kind | Unit | Attributes |
|---|---|---|---|
| `loadsmith.rows` | counter | `{row}` | `direction` = `read` / `written` |
| `loadsmith.batches` | counter | `{batch}` | `direction` |
| `loadsmith.bytes` | counter | `By` | `direction` |
| `loadsmith.run.duration` | histogram | `s` | `outcome` |
| `loadsmith.runs` | counter | `{run}` | `outcome` = `success` / `error` / `cancelled` |
| `loadsmith.plugin.respawns` | counter | `{respawn}` | `loadsmith.plugin.kind` |
| `loadsmith.checkpoint.lag` | gauge | `{batch}` | — |
| `loadsmith.state.commit.duration` | histogram | `s` | — |

Plugin metrics are re-recorded through a `loadsmith.plugin` meter with
`loadsmith.plugin.name`, `.kind` and `.version` added. Histograms are
replayed from their aggregate (exact count, min, max and buckets; the sum is
exact when the values fit their buckets).

**Logs:** stderr is unchanged. Core log events at or above
`telemetry.log_level` are also exported as OTLP log records with the trace
context of the span they happen in; plugin log lines are exported with the
plugin's trace context (or its process span's). The exporters' own HTTP/gRPC
stacks are never exported (no feedback loop).

**Run summary:** `--output json` / `--summary-file` gain `trace_id` when the
run's traces were exported.

## Verification

- Unit tests: settings precedence (flag > file > env, off switches, bad
  values), protocol round-trips and tolerance, the SDK layer (span tree,
  status, log level, sampling, budget, metric aggregation), histogram replay.
- Core end-to-end (`tests/telemetry.rs`, fake plugins on the current SDK,
  in-memory exporters): one trace per run, plugin spans under the core's
  plugin spans, plugin and core metrics, logs with trace context, masking;
  off ⇒ plugins record nothing.
- CLI end-to-end: a dead endpoint (http and gRPC) never fails the run and
  warns once; flags are validated; `--print-resolved-config` shows the
  effective, masked block.
- Host check against `otel/opentelemetry-collector-contrib` (file exporter):
  postgres → parquet over http and gRPC.

## Plugins (next step)

Re-pin the SDK in `loadsmith-canonical-plugins` and instrument with the
semantic conventions:

- postgres/mysql/oracle: span per statement/COPY/batch insert/merge phase
  (`db.system.name`, `db.namespace`, `db.collection.name`,
  `db.operation.name`, `db.query.text` = generated statement only, never bound
  values), connection/TLS setup span, rows per batch;
  `db.client.operation.duration`.
- sharepoint: `http.client` spans per Graph call (`http.request.method`,
  `url.full` without query secrets, `http.response.status_code`), retry
  events with Retry-After, token acquisition span.
- parquet/jsonl/local-copy: span per file written/delivered (path, bytes,
  rows), rename/cleanup spans.
- csv/excel/file parser/provider/source: span per object parsed/listed/resolved.
- null: baseline only.
