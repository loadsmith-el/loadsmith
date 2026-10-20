# Loadsmith — Initial Architecture Document

> This is the original design document. Where it describes behavior, it has been
> updated to match the code; the user-facing reference is the
> [documentation](../docs/src) and the [README](../README.md). Items marked
> *planned* are not implemented yet.

## 1. Vision

**Loadsmith** is a modern **EL** — *Extract and Load* — tool built to run declarative data ingestion and loading pipelines.

The goal is to be a modern alternative to Embulk, keeping the idea of declarative configuration and plugins, but with a cleaner, more extensible, faster and safer architecture.

Loadsmith must be built around one central principle:

> **A small, stable, generic core; specialized plugins doing the specific work.**

The goal is not to build a heavy transformation tool, nor to replace dbt, Spark or SQL engines. Loadsmith must focus on moving data from a source to a destination with predictability, real streaming, good observability and support for modern sources/destinations.

---

## 2. Goals

### 2.1 Main goals

* Run declarative ingestion and loading pipelines.
* Support varied sources and destinations through plugins.
* Use streaming/batches to avoid loading whole datasets into memory.
* Allow plugins written in multiple languages.
* Clearly separate control, data and events/logs.
* Use simple, efficient formats for communication:

  * **JSONL** for control.
  * **Apache Arrow IPC** for data.
* Use **YAML** as the canonical configuration format.
* Use typed Rust structs as the internal configuration model.
* Have a portable architecture, starting with Linux, but without blocking future Windows support.
* Support remote whole-document configuration loading (*planned*: `--config-ref`, 1.x).
* Support safe secret resolution via **provider** plugins (`secret()` templates).
* Have good observability from the start.

### 2.2 Initial non-goals

* Not to be a heavy transformation engine.
* Not to replace dbt.
* Not to create a complex DSL for SQL, joins or analytical transformations.
* Not to run distributed processing in the MVP.
* Not to aim for 100% compatibility with Embulk.
* Not to load plugins as dynamic libraries inside the main process.
* Not to use `stderr` as the normal log channel.
* Not to use MessagePack in the initial control protocol.
* Not to accept multi-line JSON or undelimited JSON in the control protocol.

---

## 3. Motivation

The main motivation comes from limitations observed in Embulk and its plugins:

* old or outdated plugins;
* difficulty with modern drivers;
* inconsistent fetch and streaming behavior;
* risk of running out of memory;
* little predictability on large loads;
* difficulty evolving the ecosystem;
* poor ergonomics for creating or maintaining plugins;
* no clear separation between the control plane and the data plane.

A concrete case was the read behavior on databases via JDBC/MySQL, especially regarding `fetch_rows`, server-side cursors and memory use. This kind of problem reinforces the need for a tool with real streaming, clear contracts and predictable behavior.

---

## 4. General architecture

Loadsmith's architecture is based on three main parts:

```text
Loadsmith Core
  ├── YAML configuration
  ├── Template resolution
  ├── Provider/secret resolution
  ├── Typed internal model in Rust
  ├── Plugin orchestration
  ├── Execution control
  ├── Observability
  └── State/checkpoints

Plugins
  ├── Sources
  ├── Destinations
  ├── Sinks
  ├── Parsers                        (raw source → parser → pump)
  └── Providers                      (resolve `secret()` values before a run)

Protocols
  ├── Control plane: JSONL
  ├── Data plane: Apache Arrow IPC
  └── Event/log plane: JSONL/structured events
```

The core must not know internal details of Oracle, MySQL, Snowflake, S3, Excel, CSV or any other specific technology. Those details belong to the plugins.

---

## 5. Core

The **Loadsmith Core** is the execution orchestrator.

Responsibilities:

* read the YAML configuration;
* parse the configuration safely;
* resolve templates;
* resolve external configuration references (*planned*, 1.x);
* resolve secrets (`secret()` calls, through provider plugins);
* convert the resolved configuration into typed Rust structs;
* validate the pipeline;
* discover available plugins;
* start plugins as child processes;
* negotiate protocol and capabilities;
* coordinate the flow between source and destination;
* control the plugins' lifecycle;
* apply cancellation, timeouts and failure handling;
* receive events, progress and logs;
* consolidate metrics;
* manage state/checkpoints when applicable.

The core must remain small and generic.

---

## 6. Configuration model

Loadsmith's canonical configuration format is **YAML**.

YAML stands for **YAML Ain't Markup Language**.

YAML was chosen because real pipelines tend to contain large lists and repeated structures, such as:

* columns of XLSX/CSV files;
* transforms;
* validations;
* multiple sheets;
* multiple outputs;
* rejection rules;
* schema drift policies;
* metadata;
* ordered steps.

These cases are more readable and easier to maintain in YAML than in TOML.

### 6.1 Internal model

Internally, Loadsmith must not work with loose dynamic maps.

The internal model is made of **typed Rust structs**, serialized and deserialized via **Serde**.

Serde is not a format. Serde is the standard Rust framework/ecosystem for serializing and deserializing structs.

Conceptual flow:

```text
pipeline.yaml
  -> safe YAML parse
  -> static check of the unresolved tree (strict typed shape, core-owned
     validation, template syntax, secret() rules)          — no side effects
  -> env()/file() in providers:, then the referenced provider instances run
     (one resolve per distinct secret() call)
  -> one substitution pass: env()/file()/secret() everywhere
  -> deserialization into typed Rust structs + validation
  -> state lock, data plugins, execution
```

All of this happens before the state lock is taken and before any data
plugin is spawned. (Whole-document loading from a remote reference,
`--config-ref`, is planned for 1.x; see §12.)

### 6.2 Separating core config from plugin config

The core validates the blocks that belong to Loadsmith itself:

```yaml
pipeline:
  name: sales_mysql_to_s3

state:
  backend: local
  path: .loadsmith/state/sales.json
```

(`local` is the only state backend today; an `s3` backend is planned.)

The core also knows the plugins' envelope:

```yaml
source:
  type: postgres
  config:
    ...
```

But the contents of `source.config`, `destination.config` and `sink.config` must be validated by the responsible plugin.

The model in Rust (`crates/loadsmith-config/src/model.rs`, abridged):

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    pub pipeline: PipelineMeta,
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderRef>, // instance name → provider
    pub source: PluginRef,
    #[serde(default)]
    pub parser: Option<PluginRef>, // with a raw source (object_output) only
    pub destination: PluginRef,
    #[serde(default)]
    pub sink: Option<PluginRef>,
    #[serde(default)]
    pub state: Option<StateConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRef {
    #[serde(rename = "type")]
    pub plugin_type: String,
    #[serde(default)]
    pub config: serde_yaml::Value,
    #[serde(default)]
    pub timeout_seconds: Option<u64>, // default 60; 0 disables
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineMeta {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default = "default_heartbeat_seconds")] // 30
    pub heartbeat_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginRef {
    #[serde(rename = "type")]
    pub plugin_type: String,
    #[serde(default)]
    pub config: serde_yaml::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateConfig {
    pub backend: String, // only "local" passes validation today
    pub path: String,
    #[serde(default)]
    pub on_schema_change: SchemaChangePolicy, // refuse (default) | continue
    #[serde(default)]
    pub checkpoint_interval: u64,
}
```

The important point is that the core is typed where it is in charge, and flexible where it must delegate to the plugin.

---

## 7. Controlled YAML

Loadsmith must use YAML as the canonical format, but must not accept "free and wild" YAML.

Proposed rules:

* use a safe parser;
* prefer YAML 1.2 compatibility when possible;
* validate everything against the internal schema;
* unknown fields must produce an error;
* types must be validated by the Rust structs;
* templates must be resolved before final validation, or in well-defined phases;
* anchors, aliases and merge keys should be avoided in the MVP;
* ambiguous strings must be explicitly strings;
* the resolved configuration must be printable with `--print-resolved-config`.

Examples of values that must be handled carefully:

```yaml
code: "00123"
reference_date: "2026-06-05"
active: true
```

---

## 8. Plugins

Plugins are separate executables, started by the core as child processes.

This decision avoids ABI, runtime, native-dependency and cross-language compatibility problems.

ABI stands for **Application Binary Interface**.

### 8.0 Plugin discovery

Loadsmith manages plugins in a dedicated directory.

Default directory:

```text
~/.loadsmith/plugins/
```

Overridden only by the `--plugin-dir` flag (there is no environment variable):

```bash
loadsmith run pipeline.yaml --plugin-dir ./plugins/
```

A plugin binary is looked up as `loadsmith-{kind}-{type}` in the plugin directory
first, then on `PATH` as a fallback. The `-bundled` container image relies on
that fallback: it installs every canonical plugin into `/usr/local/bin`.

Management commands:

```bash
loadsmith plugin install postgres                     # from the canonical index: <name>[:<kind>][@<version>]
loadsmith plugin install --all                        # every package in the index
loadsmith plugin install -f ./loadsmith-plugin.yaml   # from a manifest (path or URL)
loadsmith plugin install --binary ./my-plugin-binary  # a local binary, no manifest
loadsmith plugin list
loadsmith plugin uninstall postgres                   # or postgres:source
```

Index and manifest installs download a per-platform artifact, verify its sha256,
and check the manifest's protocol range against the core before fetching.

Plugins can be written in:

* Rust;
* Python;
* Go;
* Java;
* C#;
* Node;
* any language able to implement the Loadsmith protocol.

### 8.1 Main plugin kinds

#### Source

Reads data from an origin.

Examples:

* JDBC;
* MySQL;
* PostgreSQL;
* Oracle;
* Snowflake;
* S3;
* local file;
* SharePoint;
* HTTP APIs.

#### Destination

Writes data to a target. For databases, it writes via the native protocol
(consuming Arrow directly). For files, it writes to a local staging directory;
remote delivery is the job of a **sink** (below).

Examples:

* local file/staging in Parquet, CSV, JSONL;
* stdout;
* PostgreSQL;
* Snowflake;
* Redshift.

#### Sink

An optional **delivery** stage, outside the data plane. It separates *format*
(the destination) from *location* (the sink), avoiding the N×M explosion of
plugins such as `s3_parquet`. A file-output destination writes files to staging
and announces each one as ready via `object_ready` (fd4); the sink delivers each
finalized file to the remote target. It is only valid with destinations that
advertise the `object_output` capability. The core owns the delivery ledger — if
the sink dies, it is respawned and resumes where it left off.

Examples:

* local copy (`local-copy`, shipped);
* S3;
* Google Cloud Storage;
* sftp;
* e-mail.

#### Parser

Converts files into tabular data: a *raw* source (capability `object_output`)
announces local files with `object_ready`, the core hands each to the parser
(`parse_object`), and the parser writes Arrow on fd3 in the source's place.
The core closes the parser's input only after the raw source succeeded and
ends the destination's input only after both succeeded, so a failure upstream
never commits the files parsed so far. SDK: `ParserPlugin` / `run_parser` and
`ObjectSourcePlugin` / `run_object_source`.

Examples:

* CSV;
* JSON;
* Excel/XLSX;
* XML;
* Parquet.

#### Provider

Resolves configuration values: each `{{ secret('<instance>', '<ref>') }}` call
is answered by the provider instance declared as `providers.<instance>` (binary
`loadsmith-provider-<type>`). The core runs the referenced instances before the
run, sends one `resolve` per distinct reference, and masks every value (see
§11–13). SDK: `ProviderPlugin` / `run_provider`.

Examples:

* local file (`file`, ref `path[#key]`; being updated to the provider API);
* AWS Secrets Manager (planned, 1.x — needs SigV4);
* AWS SSM Parameter Store, Vault, Azure Key Vault, Google Secret Manager (future).

---

## 9. Communication between core and plugins

Communication must be separated by purpose.

### 9.1 Control plane

The control plane uses **JSONL**.

JSONL stands for **JSON Lines**.
JSON stands for **JavaScript Object Notation**.

Each control message is a JSON object serialized on a single line and delimited by `\n`.

Example:

```jsonl
{"type":"handshake"}
{"type":"handshake_ack","protocol_supported_versions":[1],"plugin_name":"loadsmith-source-postgres","plugin_version":"0.2.1","kind":"source"}
{"type":"set_protocol_version","protocol_version":1}
{"type":"capabilities_request"}
{"type":"finished","status":"success","rows_read":10000,"batches_read":1}
```

Loadsmith does not consider undelimited JSON or multi-line JSON a control-protocol format. Whenever this document mentions JSON on the control plane, the right decision is **JSONL**.

Control plane responsibilities:

* handshake;
* capabilities;
* configuration;
* starting execution;
* cancellation;
* schema;
* status;
* progress;
* errors;
* completion.

The message set (full shapes in [`definitions/protocol.md`](protocol.md)):

```text
control (fd0/fd1):  handshake, handshake_ack, set_protocol_version,
                    capabilities_request, capabilities_response,
                    configure, configure_ack, start, schema, ready,
                    deliver_object, object_delivered, ping, pong,
                    cancel, error, resend, finished
events (fd4):       progress, log, object_ready, checkpoint, committed
```

### 9.2 Why JSONL for control

Choosing JSONL favors:

* simplicity;
* debuggability;
* cross-language interoperability;
* ease of logging;
* direct inspection with `cat`, `tail`, `jq` and common tools;
* a lower barrier for community plugins.

Since the heavy data volume travels over the data plane using Apache Arrow IPC, the overhead of JSONL on the control plane is acceptable.

MessagePack may be re-evaluated in the future if the control plane becomes a bottleneck, but it will not be the initial format.

### 9.3 Data plane

The data plane uses **Apache Arrow IPC**.

IPC stands for **Inter-Process Communication**.

It carries tabular batches between plugins.

The source must produce Arrow batches. The destination must consume Arrow batches.

### 9.4 Event/log plane

Logs and events must travel over a dedicated channel, not `stderr`.

The natural format for structured events/logs is also **JSONL**.

Desired events:

* step start;
* step end;
* progress;
* rows processed;
* bytes read/written;
* warnings;
* structured errors;
* metrics.

---

## 10. Local transport

On Linux, the initial design can use:

```text
stdin/stdout -> JSONL control
fd3          -> Arrow IPC data
fd4          -> JSONL events/logs
```

However, this must sit behind a transport abstraction.

The idea is to support Linux first, without preventing Windows in the future.

Possible implementations:

```text
LinuxTransport
  -> pipes/file descriptors

WindowsTransport
  -> named pipes

FallbackTransport
  -> local TCP, only if needed
```

Local TCP is not the preference, but may be evaluated as a fallback.

---

## 11. Templates

Loadsmith supports templates in the configuration via an **internal expression parser**, with no dependency on external engines such as Jinja2, Liquid or Tera.

The syntax uses `{{ }}` as the delimiter. There are three functions:

* `{{ env('VAR') }}` — the value of an environment variable;
* `{{ file('/path') }}` — the raw contents of a local file, with trailing whitespace trimmed;
* `{{ secret('<instance>', '<ref>') }}` — a value resolved by the provider
  instance declared as `providers.<instance>` (see §13). `<ref>` is opaque to the
  core: its syntax belongs to the provider (e.g. `db.json#password`).

Arguments are string literals in single or double quotes, with no escapes; calls
do not nest (`secret('a', env('X'))` is an error). Templates are resolved in any
string value of the YAML document (never in mapping keys), before it is
deserialized.

Examples:

```yaml
url: "{{ env('DATABASE_URL') }}"
password: "{{ secret('creds', 'db.json#password') }}"
```

The retired `aws.sm('/path').field` form is rejected with an error pointing to
`secret()`; AWS Secrets Manager becomes a provider (planned, 1.x). Dot-field
access on `file()` is not supported — a provider (such as the `file` provider's
`path#key` reference) selects fields.

Resolution is staged: a static pass parses every template (a syntax error
reports its dotted location, e.g. `source.config.password`) and collects the
`secret()` calls without evaluating anything; a single substitution pass then
replaces each call with its value. Every resolved value — `env()`, `file()` and
`secret()` alike — is masked (see §13).

The parser resolves expressions inside YAML string values. It does not support loops, conditionals, filters or inheritance — these features are out of scope. The surface area is intentionally small: a function call with string-literal arguments.

If the need grows in the future, migrating to MiniJinja is viable without breaking the syntax, since the `{{ }}` delimiter is compatible.

---

## 12. Remote configuration documents

> **Planned for 1.x — not implemented.** Loading a *whole* pipeline document
> from a remote reference (the `LOADSMITH_CONFIG_REF` variable and the
> `--config-ref` flag below) does not exist yet. Providers (kind `provider`)
> resolve inline `secret()` values today (§13); a later minor release may let
> them load whole documents too. This section is the intended design.

Remote configuration lets the pipeline document itself be loaded from external sources. Providers are pluggable — the community can create new ones.

### 12.1 Reference format

The rule is: if the URI scheme uniquely identifies the provider, use a plain URI. If the provider uses `https` underneath (and the scheme alone does not identify it), use `provider|url`.

| Reference | Provider |
|---|---|
| `file:///opt/config.yml` | local file |
| `s3://bucket/key` | S3 |
| `gs://bucket/key` | Google Cloud Storage |
| `https://example.com/config.yml` | plain HTTP (direct download, no special auth) |
| `azure\|https://account.blob.core.windows.net/container/blob` | Azure Blob Storage |

A plain `https://` means a direct HTTP download with no proprietary logic. Any provider that uses HTTPS but needs authentication or its own logic (Azure, SharePoint, OAuth etc.) uses the explicit `provider|` prefix.

### 12.2 Usage (planned)

Via environment variable:

```bash
LOADSMITH_CONFIG_REF="s3://bucket/path/config.yml"
LOADSMITH_CONFIG_REF="azure|https://account.blob.core.windows.net/container/config.yml"
```

Via CLI flag:

```bash
loadsmith run --config-ref "s3://bucket/path/config.yml"
```

### 12.3 Planned document providers

* `file://` — local file
* `s3://` — Amazon S3
* `gs://` — Google Cloud Storage
* `https://` / `http://` — plain HTTP
* `azure|https://` — Azure Blob Storage

Additional providers can be implemented as plugins following the same contract.

---

## 13. Secrets

Secrets must not be hardcoded in the configuration.

Secrets are resolved **inline in the template**, using the same `{{ }}` syntax as configuration templates. There is no separate secrets block — the value is resolved where it is referenced.

```yaml
source:
  type: postgres
  config:
    user: "{{ env('PG_USER') }}"
    password: "{{ env('PG_PASSWORD') }}"
```

```yaml
url: "{{ env('DATABASE_URL') }}"
```

Secret stores are reached through **provider** plugins. A pipeline declares the
instances it uses under `providers:` and references them with `secret()`:

```yaml
providers:                       # optional; instance name → provider (strict keys)
  sm:
    type: aws-secrets-manager    # → loadsmith-provider-aws-secrets-manager (planned)
    config: { region: us-east-1, prefix: "{{ env('ENV') }}/" }
    timeout_seconds: 30          # optional; default 60, 0 disables
  creds:
    type: file                   # → loadsmith-provider-file
    config: { base_dir: /mnt/s3/secrets }
source:
  type: postgres
  config:
    password: "{{ secret('creds', 'db.json#password') }}"
```

Rules:

* instance names use letters, digits, `_` and `-`; a `secret()` naming an
  undeclared instance is a static error;
* `secret()` is not allowed in `pipeline:`, `state:` or `providers:` — those
  blocks resolve without providers (the state commands read just `pipeline:` +
  `state:`; a provider's own config may use `env()`/`file()`);
* only instances some `secret()` references are started (a declared, unused one
  is logged as a warning); each is spawned once, all in parallel, and asked once
  per distinct reference;
* resolved values live in memory for the run only — never on disk;
* everything is resolved before the state lock is taken and before any data
  plugin starts; a provider failure (an unresolvable reference, a crash, a
  timeout) fails the run with the configuration exit class and nothing runs.

Value sources:

* `env('VAR')` — environment variable;
* `file('/path')` — raw contents of a local file;
* `secret('<instance>', '<ref>')` — a provider: `file` (canonical, being updated
  to the provider API); AWS Secrets Manager (planned, 1.x); AWS SSM Parameter
  Store, Vault, Azure Key Vault, Google Secret Manager in the future.

**Masking.** Every value resolved by `env()`, `file()` or `secret()` is masked as
`***` — and, for a multi-line value (a PEM key), every trimmed line of at least 8
characters, and the JSON-escaped spelling of the value. The mask is applied to
plugin log lines (fd4), plugin stderr (piped through the core), plugin error
texts (`configure_ack`, `finished`, `resolved`), the abort reasons sent to
plugins, the CLI's final `error:` line, the run summary's `cause`, and
`--print-resolved-config`. Masking is substring replacement: a very short value
masks every occurrence of that text, and a value a plugin transforms (base64,
URL-encoding) is not recognised.

---

## 14. Internal pipeline

The pipeline as implemented: a source writes Arrow, or a raw source produces
files that a parser turns into Arrow; either way one Arrow stream goes through
the core's pump to the destination, and an optional sink delivers the
destination's files:

```text
Source ────────────────────────────┐
                                   ├─> core pump -> Destination [-> Sink]
Raw source -(files)-> core -> Parser┘
```

The parser is not a transformation stage: it only changes the representation
(file → Arrow). The core stays in the middle of every hop — the raw source
never talks to the parser, the parser never talks to the destination.

A possible future model:

```text
Source -> Parser -> Filter -> Buffer -> Destination
```

The main concern is not to turn Loadsmith into a heavy transformation tool.

Acceptable transformations:

* parsing;
* type normalization;
* schema inference;
* format conversion;
* simple adjustments needed for loading.

Transformations not wanted in the core:

* complex joins;
* heavy analytical rules;
* a custom SQL DSL;
* an internal relational engine.

DSL stands for **Domain-Specific Language**.

For complex SQL, the ideal is to let the database do it:

```yaml
source:
  type: jdbc
  config:
    query: |
      select ...
      from ...
      join ...
```

---

## 15. Apache Arrow and batches

Loadsmith must use tabular batches as the unit of processing.

Desired characteristics:

* batched reads;
* batched writes;
* backpressure;
* low memory use;
* batch size control;
* explicit schema;
* Parquet compatibility;
* cross-language interoperability.

The format chosen for the data plane is:

```text
Apache Arrow IPC
```

---

## 16. Performance and memory

Important requirements:

* do not load everything into memory;
* support large datasets;
* avoid heap/memory exhaustion;
* allow a configurable batch size;
* control backpressure;
* allow incremental writes;
* expose throughput metrics;
* allow diagnosing bottlenecks.

Desired metrics:

* rows read;
* rows written;
* bytes read;
* bytes written;
* batches processed;
* time per step;
* average speed;
* recent speed;
* approximate memory use, if possible.

---

## 17. State and checkpoints

Loadsmith has room for state, mainly for incremental loads.

**The core is the sole owner of the state.** Plugins are memoryless tools — they run, report progress, and exit. Whoever persists, reads and manages the state is always the core.

Use cases:

* last processed timestamp;
* last ID;
* offset;
* cursor;
* pagination;
* `search_after`;
* last processed file;
* write checkpoint.

The plugin reports to the core the state value to persist, but the decision of when and how to persist it belongs to the core. This has shipped:

```yaml
state:
  backend: local
  path: .loadsmith/state/orders.json
  on_schema_change: refuse     # or continue
  checkpoint_interval: 50      # 0 ⇒ persist only at the end of the run
```

A remote backend is planned (not yet available — `backend: s3` fails validation):

```yaml
state:
  backend: s3
  path: s3://bucket/loadsmith/state/
```

How the formerly open points were settled:

* **How the plugin reports state:** the source emits `checkpoint { cursor_value,
  batch_seq }` and the destination emits `committed { batch_seq }`, both on fd4;
  the core persists a watermark only once the matching batch is durable.
* **Consistency on partial failure:** the guarantee is at-least-once (exactly-once
  with an idempotent destination such as `staged_merge`); a lease-based lock file
  stops concurrent runs of one pipeline.
* **Exact state file format:** a JSON `StateDoc` (pipeline, cursor value, schema
  fingerprint, run id, update time), written atomically.
* **Idempotent retry:** still open — there is no retry in the core.

See [Incremental State](../docs/src/architecture/incremental-state.md).

---

## 18. Observability

Loadsmith must have observability from the start.

Desired outputs:

* human logs in the terminal;
* structured JSONL logs;
* JSONL progress events;
* errors with context;
* per-step metrics;
* a final run summary.

Example of the final summary:

```text
Pipeline: mysql_to_s3
Status: success
Rows read: 21,473,000
Rows written: 21,473,000
Batches: 2,148
Duration: 00:18:42
Average throughput: 19,139 rows/s
Destination: s3://bucket/raw/table/
```

Possible future support:

* Prometheus;
* OpenTelemetry;
* CloudWatch;
* Datadog;
* JSONL file;
* events for external orchestrators.

---

## 19. Embulk compatibility

Loadsmith is inspired by Embulk, but does not need to be compatible with it.

Desirable similarities:

* declarative configuration;
* the input/output concept;
* templates;
* plugins;
* pipeline execution.

Non-goals:

* full compatibility with Embulk YAML;
* compatibility with Embulk plugins;
* replicating the old runtime;
* keeping problematic architectural decisions.

---

## 20. CLI

The command tree:

```bash
loadsmith run pipeline.yaml
loadsmith validate pipeline.yaml [--resolve]   # static by default; --resolve runs the providers
loadsmith plugin list
loadsmith plugin install <name>[:<kind>][@<version>] | --all | -f <manifest> | --binary <path>
loadsmith plugin uninstall <name>[:<kind>]
loadsmith state show pipeline.yaml
loadsmith state rm pipeline.yaml
```

Useful debugging commands:

```bash
loadsmith run pipeline.yaml --log-level debug
loadsmith run pipeline.yaml --dry-run
loadsmith run pipeline.yaml --print-resolved-config
```

Ideas, not implemented: `loadsmith run --config-ref <ref>` (see §12),
`loadsmith plugin inspect <name>` and `loadsmith capabilities <name>`.

---

## 21. Project structure

The engine repository:

```text
loadsmith/
  crates/
    loadsmith-protocol/
    loadsmith-transport/
    loadsmith-arrow/
    loadsmith-plugin-sdk/
    loadsmith-config/
    loadsmith-plugin-manifest/
    loadsmith-core/
    loadsmith-cli/
    loadsmith-tls/
  definitions/
  docs/
```

Plugins live in the separate
[`loadsmith-canonical-plugins`](https://github.com/loadsmith-el/loadsmith-canonical-plugins)
repository: `file`, `jsonl`, `null`, `parquet`, `local-copy`, `mysql`,
`sharepoint`, `postgres` and `oracle`.

Conceptual split:

```text
loadsmith-protocol
  JSONL control message types.

loadsmith-transport
  Pipes, file descriptors, named pipes, etc.; JSONL framing.

loadsmith-arrow
  Arrow IPC utilities and schemas.

loadsmith-plugin-sdk
  Helpers to build plugins.

loadsmith-config
  YAML configuration, templates and secret masking.

loadsmith-plugin-manifest
  The loadsmith-plugin.yaml manifest schema and validation, used by
  `plugin install`.

loadsmith-core
  Execution orchestration.

loadsmith-cli
  Command-line interface.

loadsmith-tls
  Shared rustls + rustls-rustcrypto TLS configuration for network plugins.
```

---

## 22. MVP

A realistic MVP must validate the architecture without trying to solve everything.

### 22.1 Minimal MVP

* `loadsmith run` CLI;
* reading local YAML;
* basic templates;
* environment variables;
* internal model in Rust structs;
* plugins as child processes;
* JSONL control plane;
* Arrow IPC data plane;
* dedicated JSONL event/log channel;
* a simple source;
* a simple destination;
* logs and a final summary.

Possible plugins for the minimal MVP:

```text
source:
  local_csv

destination:
  local_parquet
  stdout
```

### 22.2 A more useful MVP for corporate environments

* JDBC source;
* S3 Parquet destination;
* local and S3 providers (`file` provider; whole-document loading planned for 1.x);
* secrets via env and providers (AWS Secrets Manager planned, 1.x);
* configurable batch size;
* structured logs;
* final summary;
* basic error handling.

Target pipeline:

```text
JDBC -> Arrow batches -> S3 Parquet
```

---

## 23. Settled decisions

* Project name: **Loadsmith**.
* Visual/conceptual identity: forge, anvil, blacksmith.
* A tool focused on EL.
* Must not become a heavy transformation engine.
* Small, generic core.
* Plugin-first architecture.
* Plugins as separate executables/processes.
* Canonical configuration in YAML.
* Internal model in typed Rust structs.
* Serde as the serialization/deserialization layer.
* Control via JSONL.
* Data via Apache Arrow IPC.
* Logs/events on a dedicated channel.
* Structured logs/events in JSONL.
* Do not use `stderr` as the normal log channel.
* Linux first.
* Windows later, through a transport abstraction.
* Localhost TCP is not the preference.
* Templates are required.
* Providers must be pluggable (kind `provider`).
* Secret providers are required.
* Plugins in other languages must be possible.
* Complex SQL must stay in the database/query, not in a Loadsmith DSL.
* **Core language: Rust.**
* **Async runtime: Tokio.**
* **Templates via an internal expression parser** — no external engine; `{{ env('VAR') }}`, `{{ file('/path') }}` and `{{ secret('<instance>', '<ref>') }}` (string-literal arguments, no nesting; the old `aws.sm()` form is retired); MiniJinja as a future migration option if needed.
* **Plugin discovery via a managed directory** — `~/.loadsmith/plugins/` by default, overridden with `--plugin-dir`; lookup is the plugin directory, then `PATH`; managed with `loadsmith plugin install` (index / `--manifest` / `--binary`), `plugin list` and `plugin uninstall`.
* **The core is the sole owner of state** — plugins report progress and state values to the core via JSONL, but persist nothing directly.
* **Formalized JSONL protocol** — ordinal integer versioning starting at `1`; handshake initiated by the core; the shapes of every message are defined in `definitions/protocol.md`.
* **Secrets resolved inline in the template** — same `{{ }}` syntax; `secret()` calls are resolved by **provider** plugins declared under `providers:`, before the state lock and before any data plugin. Every `env()`/`file()`/`secret()` value is masked in logs, plugin stderr, error texts, the run summary and `--print-resolved-config`.
* **Remote document reference format** (*planned*, 1.x) — a plain URI when the scheme identifies the provider (`s3://`, `gs://`, `file://`, `https://`); a `provider|url` prefix when the provider uses HTTPS but has its own logic (`azure|https://...`).
* **Multi-arch first** — images published for `linux/amd64` and `linux/arm64` (AWS Graviton); the rule is not "no TLS", it is **no per-architecture native/assembly code**. Releases build natively on a dedicated runner per architecture (`ubuntu-latest` / `ubuntu-24.04-arm`, no QEMU) inside a pinned `rust:<version>-alpine` (same version as `rust-toolchain.toml`), producing a static musl binary; C/assembly crypto would still mean a fragile toolchain (`cmake`/`perl`/`nasm`).
* **TLS via `rustls` + `rustls-rustcrypto`** — the TLS protocol through the mature `rustls` layer (a foundational decision); primitives through the pure-Rust `rustls-rustcrypto` provider. Banned as native crypto: `native-tls`, `openssl-sys`, `ring`, `aws-lc-rs`. The provider is swappable via `CryptoProvider::install_default` (a reversible knob, with per-binary blast radius thanks to process isolation); it applies uniformly to every network plugin (postgres, mysql, oracle, sharepoint, s3).
* **State communication** — the source reports watermarks with `checkpoint` and the destination reports durability with `committed`, both on fd4.
* **State file format and partial-failure guarantee** — a JSON `StateDoc` written atomically, guarded by a lease-based lock file; at-least-once, exactly-once with an idempotent destination.
* **Idempotency** — provided by `staged_merge` destinations (staging + `MERGE` by key).
* **Plugin packaging and distribution** — a `loadsmith-plugin.yaml` manifest, a canonical `index.json`, sha256-verified installs, and per-plugin release tags in `loadsmith-canonical-plugins`.

---

## 24. Open decisions

* Define a formal capabilities contract (future extension beyond the built-in ones).
* Define retry (none exists today).
* Define backpressure.
* Decide between a generic JDBC plugin and native per-database plugins.
* Define a schema contract for per-plugin configuration validation (JSON Schema, a custom contract, etc.); today each plugin validates in `configure_ack`.
* Define a compatibility-testing strategy between core and plugins.

---

## 25. Initial core/plugin contract

The priority point to deepen. (The formal contract is now
[`definitions/protocol.md`](protocol.md).)

Conceptual flow:

```text
1. The core starts the plugin.
2. The plugin takes part in the JSONL handshake.
3. The core checks the protocol version.
4. The core requests capabilities.
5. The plugin answers with its capabilities.
6. The core sends the configuration.
7. The plugin validates/prepares.
8. The core starts execution.
9. The source plugin produces Arrow batches.
10. The destination plugin consumes Arrow batches.
11. Plugins emit progress, logs and events in JSONL.
12. The core finishes the run, or aborts it on error.
```

Possible capabilities:

```yaml
plugin:
  name: jdbc
  kind: source
  protocol_version: 1
  supports:
    - schema_inference
    - batch_read
    - incremental_state
  config_schema:
    ...
```

Example JSONL messages:

```jsonl
{"type":"handshake"}
{"type":"handshake_ack","protocol_supported_versions":[1],"plugin_name":"loadsmith-source-jdbc","plugin_version":"0.1.0","kind":"source"}
{"type":"set_protocol_version","protocol_version":1}
{"type":"capabilities_response","supports":["schema_inference","batch_read"]}
{"type":"progress","rows_read":10000,"batches_read":1}
{"type":"finished","status":"error","code":"JDBC_CONNECTION_FAILED","message":"failed to connect to database"}
```

---

## 26. Questions for the next stage

These were the open questions at the start of the project; most are answered by
§23 and the protocol.

1. Will the core officially be in Rust?
2. Will Tokio be the default async runtime?
3. Will the first source be `local_csv` or `jdbc`?
4. Will the first destination be `stdout`, `local_parquet` or `s3_parquet`?
5. Should the MVP validate the architecture or already solve a real corporate case?
6. Will the plugin SDK be part of the MVP?
7. How will plugins be found?
8. What will the minimal error message format be?
9. How will the core know a plugin is compatible?
10. How to version the JSONL protocol?
11. How to separate control messages, events and logs?
12. Will the event/log channel be mandatory from the MVP, or can it start together with control?

---

## 27. Vision statement

> **Loadsmith is a modern, plugin-first EL tool that orchestrates data extraction and loading using YAML configuration, process-isolated plugins, JSONL control and tabular data via Apache Arrow IPC.**

Another formulation:

> **A modern Embulk — safer, faster and more extensible — built for declarative data ingestion pipelines in local, cloud and corporate environments.**
