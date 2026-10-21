# OpenTelemetry for Loadsmith 1.0 — spec (approved design 2026-10-21)

## Principles (user-approved, non-negotiable)
- Three signals: traces, metrics, logs. Export via OTLP only (grpc and http/protobuf).
- **Centralized export:** plugins instrument; the core is the ONLY exporter. Plugins never open
  network connections for telemetry and carry no OTLP/TLS stack.
- Config precedence: CLI flags > `telemetry:` block in pipeline.yaml > standard `OTEL_*` env vars
  > defaults. Off by default; enabled when an endpoint is resolved (or `telemetry.enabled: true`).
  `--no-telemetry` / `telemetry.enabled: false` always wins off.
- Zero cost when off: plugins are told telemetry is disabled and record nothing.
- Must land before the protocol v1 freeze. Additive + tolerant: old plugins (no telemetry) and old
  cores (ignore unknown fd4 types) keep working.
- Standardize, never cut: OTel semantic conventions everywhere (`db.*`, `http.*`, `messaging`-free).

## 1. Configuration (engine: loadsmith-config + loadsmith-cli)
pipeline.yaml (all optional; strict unknown-key checking applies like every other block):
```yaml
telemetry:
  enabled: true                 # default: true iff an endpoint is resolved
  endpoint: https://otel:4318   # else OTEL_EXPORTER_OTLP_ENDPOINT (+ per-signal *_TRACES/_METRICS/_LOGS_ENDPOINT)
  protocol: http-protobuf       # grpc | http-protobuf; else OTEL_EXPORTER_OTLP_PROTOCOL; default http-protobuf
  headers: { authorization: "Bearer ${env:OTEL_TOKEN}" }   # merged over OTEL_EXPORTER_OTLP_HEADERS; values masked
  timeout: 10s                  # duration string, like the rest of the YAML
  signals: [traces, metrics, logs]   # subset; default all
  sampling: { ratio: 1.0 }      # parent-based ratio; else OTEL_TRACES_SAMPLER(_ARG)
  metrics_interval: 15s         # else OTEL_METRIC_EXPORT_INTERVAL; final flush at end of run always
  log_level: info               # minimum level exported as OTLP logs (stderr filtering unchanged)
  resource: { deployment.environment.name: prod }  # merged over OTEL_RESOURCE_ATTRIBUTES / OTEL_SERVICE_NAME
```
CLI flags on `run` (and accepted by `validate` for checking): `--otel-endpoint`, `--otel-protocol`,
`--otel-header k=v` (repeatable), `--no-telemetry`. Resolved config shown (masked) in
`--print-resolved-config`. `OTEL_SDK_DISABLED=true` honored. Exporter errors never fail the run:
warn once on stderr, keep loading. Shutdown flushes with a bounded timeout (also on cancel/SIGTERM,
within cancel_grace).
Default resource: `service.name=loadsmith` (override via OTEL_SERVICE_NAME / resource),
`service.version`, `loadsmith.pipeline.name`, `loadsmith.run.id`, host/os/process detectors if cheap.
OTel declarative config file (`OTEL_CONFIG_FILE`): NOT in 1.0, document as future.

## 2. Protocol (loadsmith-protocol) — fd4 event plane, additive
- `Configure` gains optional `telemetry: { enabled, traceparent, tracestate?, log_level }` —
  W3C trace context of the plugin's parent span (the core's per-plugin span). Absent/disabled ⇒ the
  SDK records nothing.
- New fd4 messages (plugin → core):
  - `span`: `{trace_id, span_id, parent_span_id, name, kind, start_unix_nano, end_unix_nano,
    status {code, message?}, attributes {..}, events [{name, time_unix_nano, attributes}],
    links? }` — sent when the span closes.
  - `metrics`: batch of data points `{name, unit?, description?, kind: counter|up_down_counter|
    histogram|gauge, temporality: delta, attributes, value | {count,sum,min,max,bucket_counts,
    explicit_bounds}}` — the SDK aggregates in-process and flushes on an interval + at finish.
- `log` gains optional `trace_id`, `span_id`, `attributes`, `time_unix_nano` (additive).
- Handshake/capabilities: plugin advertises `telemetry: 1` so the core knows to send context.
- Bound the volume: SDK drops (and counts in a `loadsmith.telemetry.dropped` metric) beyond a cap of
  spans/sec; spans per query/batch/request/file, never per row. Core masks secrets in span/log
  attributes with the existing mask (mask.rs) before export.

## 3. Plugin SDK (loadsmith-plugin-sdk)
- A `tracing` subscriber Layer installed by the SDK's run entry points (all plugin kinds) that:
  maps closed spans → `span` messages (fields → attributes, `otel.kind`, `otel.status_code`,
  `error` fields honored), events → span events, and log events → `log` (with trace/span ids).
  Root of plugin spans = traceparent from `Configure`.
- Metrics API: thin helpers (`ctx.counter(name)`, `histogram`, `gauge`) or the `opentelemetry` metrics
  API with an in-process exporter that writes `metrics` messages — pick the lighter; no network deps.
- Existing `ctx.log/info/warn` keep working and gain trace context.
- Built-in SDK spans/metrics so every plugin gets a baseline for free: `configure`, `start`, per-batch
  read/write (`loadsmith.batch` with rows, bytes), `finish`; counters rows/batches/bytes, batch
  duration histogram.
- Docs: docs/src/plugins/writing-a-*.md section "Telemetry" with the conventions table.

## 4. Core (loadsmith-core / loadsmith-cli)
- Use `opentelemetry` + `opentelemetry_sdk` + `opentelemetry-otlp` (http via reqwest/rustls — reuse
  loadsmith-tls roots — and grpc via tonic if it stays musl-static-friendly; if grpc bloats or breaks
  static builds, report before shipping it). Bridge core `tracing` with `tracing-opentelemetry`.
- Span tree: `loadsmith.run` (root; pipeline, run id, outcome, exit code) →
  `plugin <name>` per plugin process (kind, name, version, pid, respawn count) with children
  `handshake`, `configure`, plugin spans; plus core spans `state.load`, `state.commit`, `lock.acquire`,
  `checkpoint`, `sink.deliver`, `provider.resolve`, `parser.feed`. Events: respawn, resend, cancel,
  retry, schema mismatch.
- Re-emit plugin `span`/`metrics`/`log` into the SDK pipeline (spans with their own ids under the run
  trace; metrics through the meter with plugin attributes `loadsmith.plugin.name/kind/version`).
- Core metrics (OTel naming): `loadsmith.rows` (counter, attr direction=read|written), `loadsmith.batches`,
  `loadsmith.bytes`, `loadsmith.run.duration` (histogram, s), `loadsmith.runs` (counter, attr outcome),
  `loadsmith.plugin.respawns`, `loadsmith.checkpoint.lag`, `loadsmith.state.commit.duration`.
- Logs: core stderr logs unchanged; additionally exported as OTLP logs ≥ `telemetry.log_level`, plugin
  logs too, all with trace context.
- `--output json` / `--summary-file` summary gains `trace_id` when telemetry is on.

## 5. Tests & verification
- Unit tests: config precedence matrix (flag > yaml > env), masking of headers/attributes, protocol
  round-trips + tolerance (old plugin without telemetry; unknown fd4 types), SDK layer → messages.
- Integration (core tests with fake plugins): spans parented correctly, metrics aggregated, export to an
  in-memory exporter; exporter down ⇒ run still succeeds with one warning; disabled ⇒ no messages.
- End-to-end on the host: `otel/opentelemetry-collector-contrib` with the file exporter; run a lab
  pipeline (postgres → parquet, and a sharepoint-free case) and assert the JSON output contains the
  run trace with core + plugin spans, metrics and logs; both grpc and http. Clean up containers.
- Static musl build still works (`release.yml` path) and binary size delta reported.

## 6. Plugins (second agent, after the engine merge)
Re-pin SDK; instrument with semconv:
- postgres/mysql/oracle: span per statement/COPY/batch insert/merge phase (`db.system.name`,
  `db.namespace`, `db.collection.name`, `db.operation.name`, `db.query.text` = generated statement only,
  never bound values), connection/TLS setup span, rows per batch; `db.client.operation.duration`.
- sharepoint: `http.client` spans per Graph call (`http.request.method`, `url.full` without query
  secrets, `http.response.status_code`), retry events with Retry-After, token acquisition span.
- parquet/jsonl/local-copy: span per file written/delivered (path, bytes, rows), rename/cleanup spans.
- csv/excel/file parser/provider/source: span per object parsed/listed/resolved.
- null: baseline only.
Docs per plugin (what it emits), CHANGELOG, versions (fold into unreleased bumps).
Lab: optionally a case with a collector service once the lab can assert output; for now host check.
