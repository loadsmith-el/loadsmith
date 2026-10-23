# Stability Policy

This page says which parts of Loadsmith you can build on, and what a version
number promises about them. It takes full effect at **1.0**. During 0.x the
project already works to it, but an interface may still change in a minor
release. Any such change is called out in the
[CHANGELOG](https://github.com/loadsmith-el/loadsmith/blob/main/CHANGELOG.md).

## Versioning

The **engine** (the `loadsmith` binary, its release archives and its container
images) follows [Semantic Versioning](https://semver.org) over the interfaces
listed under [Covered](#covered-frozen-at-10):

- **MAJOR**: an incompatible change to a covered interface.
- **MINOR**: a compatible addition, such as a new CLI flag, a new optional
  pipeline key or a new informational protocol message.
- **PATCH**: bug fixes only.

Plugins version themselves; each plugin package has its own release line. The
contract between the engine and a plugin is the **wire protocol**, not a Rust
API. A plugin built against protocol v1 works with every 1.x engine, whatever
SDK version (or language) it was built with.

## Covered (frozen at 1.0)

| Interface | What is frozen | Compatible evolution (minor) |
|---|---|---|
| **Wire protocol v1** | The fd layout (fd0 control core→plugin, fd1 control plugin→core, fd3 Arrow IPC data, fd4 events; stderr is free-form). JSONL framing. Every message's `type` and required fields, including the provider conversation (`resolve`/`resolved`, capability `resolve`, `ready` after `start`, stdin EOF ends it), the parser stage (a raw source's `object_output`, `ready` after `start` and `object_ready` per file; the parser's `parse` capability, `parse_object`, `schema` then Arrow on fd3, `object_parsed`, stdin EOF ends it; the `scratch_dir` capability) and the kind spellings `source`, `destination`, `sink`, `parser`, `provider`. The `schema` message's [field-type vocabulary](../protocol/messages.md#schema), its field-object shape, and the rule that the Arrow IPC header is the authoritative schema. Handshake and version negotiation (`protocol_supported_versions` → `set_protocol_version`). Lifecycle ordering, including the commit point (the core ends the destination's fd3 only after the source succeeded; with a parser, after the parser and the raw source both succeeded, and the parser's stdin is closed gracefully only after the raw source succeeded). The [abort rules](../protocol/reference.md#abort-rules): `cancel`, `error` and its codes, the grace period, then kill. The [`resend` rule](../protocol/reference.md#malformed-lines-and-resend) for malformed control lines. The [tolerant-reader rule](../protocol/reference.md#compatibility-rule-tolerant-reader). The optional `configure.unknown_keys` policy (`error` | `warn`; absent or unknown reads as `error`). The optional `configure.run` (`id`, `pipeline`, `started_at_unix_ms`; absent or unreadable reads as unknown). The [telemetry extension](../protocol/messages.md#telemetry-on-the-event-channel): `handshake_ack.telemetry`, `configure.telemetry` (W3C `traceparent`; absent, disabled or unreadable reads as off), the fd4 `span` and `metrics` messages and their shapes (unknown span kinds and status codes read as `internal`/`unset`; metric points of an unknown kind or temporality are skipped), the optional `log` fields `trace_id`, `span_id`, `time_unix_nano`, `attributes`; the core is the only exporter. [Flow control](../protocol/messages.md#credit) (1.1): the source capability `flow_credit`, the optional `start.credit` (absent means no credits follow) and the `credit` message (cumulative, never shrinks). `object_ready.batches` (1.2; optional, contiguous ranges in order) and `start.first_object_seq` (1.2; optional). | New optional fields. New message types a receiver may safely ignore. New field types and type parameters (read as unknown by an older receiver). A change the receiver must understand needs **protocol v2**, negotiated in the handshake. Every 1.x engine keeps speaking v1. |
| **CLI** | Commands and subcommands, flags and their defaults and meaning (including the telemetry flags `--otel-endpoint`, `--otel-protocol`, `--otel-header`, `--no-telemetry` and their precedence over the file and the `OTEL_*` variables), the [exit-code table](cli.md#exit-codes) (`0`–`5`, `130`, `143`), the [unknown-keys policy](cli.md#unknown-keys) (`--ignore-unknown-keys`, `LOADSMITH_IGNORE_UNKNOWN_KEYS`, their precedence), graceful cancel on SIGINT/SIGTERM, and the split of the human report on stdout from diagnostics on stderr. The [JSON run summary](cli.md#machine-readable-run-summary) of `run` (`--output json`, `--summary-file`), versioned by its `version` field (now `1`): every field, its type and meaning. | New commands and new flags. A new exit code only for a case that exits `1` today. New run-summary fields (consumers ignore unknown ones); a change to an existing field needs a new summary `version`. |
| **`pipeline.yaml` schema** | Every core-owned key, its type, default and meaning (including `pipeline.timeouts`). Unknown keys are rejected by default, the file's own keys and each plugin's `config:` alike (see [decision 11](#decisions-for-10)). The `providers:` block (`type`, `config`, `timeout_seconds`). The `parser:` block (`type`, `config`) and its rules (only with a raw source; not with `state:`). The `telemetry:` block (every key, default and environment fallback). The template syntax `{{ env('VAR') }}` / `{{ file('/path') }}` / `{{ secret('<instance>', '<ref>') }}`, the `secret()` rules (declared instance; not in `pipeline:`/`state:`/`providers:`/`telemetry:`) and the masking of every resolved value. | New optional keys and new template functions. The contents of each plugin's `config:` block belong to that plugin and are versioned by it. |
| **Incremental state file** | The [state file format](../architecture/incremental-state.md#state-file-format), versioned by its `version` field, and the lock file's lease semantics. | A new engine reads every older format and upgrades it on its next save. An engine refuses a format newer than it understands rather than downgrading it. |
| **Plugin manifest** | `apiVersion: loadsmith.dev/plugin/v1` and its fields (`name`, `version`, `protocol`, `provides`, `runtime`). | New optional fields. Incompatible changes go into a new `apiVersion`, and v1 stays accepted throughout 1.x. |
| **Plugin index** | The `index.json` shape: `{"plugins": {"<name>": {"latest": "<version>", "versions": {"<version>": "<manifest URL>"}}}}`. | New optional fields. |
| **Image tags** | `ghcr.io/loadsmith-el/loadsmith` with the flavors `static`, `static-debug`, `slim` and `bundled`. Each release publishes `:vX.Y.Z-<flavor>` and `:sha-<sha7>-<flavor>`. A stable release also moves the rolling `:<flavor>` and `:latest` (= `slim`); a prerelease moves no rolling tag. The binary is `/usr/local/bin/loadsmith` and is the entrypoint. | New flavors. |
| **Telemetry names** | The [core metrics](../architecture/telemetry.md#core) (`loadsmith.rows`, `.batches`, `.bytes`, `.run.duration`, `.runs`, `.plugin.respawns`, `.checkpoint.lag`, `.state.commit.duration`): name, kind, unit and attribute keys and values; the SDK's built-in plugin metrics (`loadsmith.plugin.*`) and span names (`configure`, `start`, `finish`, `loadsmith.batch`); the run's root span `loadsmith.run` with a `plugin <name>` child per plugin process; the resource attributes `service.name` (default `loadsmith`), `loadsmith.pipeline.name`, `loadsmith.run.id`; the run summary's `trace_id`. | New metrics, spans, events and attributes. The core's inner spans (`handshake`, `state.commit`, …) and log record contents may change. |
| **Plugin discovery** |
 A plugin of kind `<kind>` and type `<type>` is the executable `loadsmith-<kind>-<type>` (a provider: `loadsmith-provider-<type>`). It is looked up in `--plugin-dir` (default `$HOME/.loadsmith/plugins`) first, then on `PATH`. | New kinds. |

## Not covered

These can change in any release:

- **Rust crate APIs.** `loadsmith-plugin-sdk`, `loadsmith-protocol`,
  `loadsmith-arrow`, `loadsmith-transport` and `loadsmith-tls` stay **0.x**,
  versioned apart from the engine. Their public APIs expose `arrow` and `rustls`
  types, so they have to follow those crates' breaking releases. Plugins pin
  them by git revision. A newer SDK may need code changes in a plugin, but the
  plugin's **wire** compatibility does not change. They are not published to
  crates.io (`publish = false`; see [decision 13](#decisions-for-10)).
  `loadsmith-plugin-manifest` is in the same position: the manifest *format* is covered and the crate API is
  not.
- **Engine internals.** `loadsmith-core`, `loadsmith-config` and `loadsmith-cli`
  are not libraries. They are marked `publish = false`.
- **Human-readable output.** The report header, the summary box, progress lines,
  log messages and the format of the tracing output on stderr. Don't parse them;
  use the [JSON run summary](cli.md#machine-readable-run-summary) and the exit
  code.
- **Experimental parts**, when a part is documented with that label. None
  is today; an experimental part becomes covered when it is documented
  without the label.
- **Behavior that contradicts the documentation** is a bug. Fixing it is not a
  breaking change.

## How interfaces evolve

- **Tolerant reader.** On every channel, a well-formed message of an unknown
  `type` is logged and skipped, and unknown fields are ignored. A malformed line
  on the control channel is answered with `resend` once and is fatal the second
  time in a row; on the event channel, which is never left undrained, it is
  skipped. The full table is in the
  [protocol reference](../protocol/reference.md#compatibility-rule-tolerant-reader).
- **Deprecation.** Before a covered item is removed, it is deprecated. It keeps
  working for the rest of the major version, warns when used, and is listed in
  the CHANGELOG. It is removed only in the next major.
- **Toolchain.** The engine is built with the Rust version pinned in
  `rust-toolchain.toml`. Building from source with an older compiler is not
  supported.

## Decisions for 1.0

These were open and are now settled. 1.0 freezes them as described.

1. **Exit codes: distinct codes**, not just "non-zero": `0` success, `1` run
   failed, `2` usage, `3` configuration, `4` plugin unavailable or
   incompatible, `5` state, `130`/`143` stopped by SIGINT/SIGTERM. See the
   [table](cli.md#exit-codes).
2. **Malformed control-channel lines: one retry.** The receiver answers the
   first malformed line with `resend` and the peer repeats its most recent
   control message; a second consecutive malformed line is fatal. Pipes do not
   corrupt data, so this only absorbs a transient encoder bug in a peer. See
   [Malformed lines and `resend`](../protocol/reference.md#malformed-lines-and-resend).
3. **`cancel` / `error` from the core: wired, not dropped.** On any failure,
   timeout or SIGINT/SIGTERM the core sends `cancel` to every live plugin
   (`error` to the one at fault), waits the grace period, and kills what is
   left. A cancelled plugin undoes its partial work and never finalizes. See
   the [abort rules](../protocol/reference.md#abort-rules).
4. **The commit point.** The core ends the destination's input cleanly only
   after the source finished successfully, so a destination never commits a
   partial load after an upstream failure.
5. **Timeouts.** Every wait of the core on a plugin has a default limit, and a
   data-plane stall timeout is available as an opt-in, in
   [`pipeline.timeouts`](pipeline-yaml.md#pipelinetimeouts).
6. **Arrow field types: a complete vocabulary now, tolerant reading, no
   negotiation.** The `schema` message carries every common Arrow type with
   its parameters (unit, time zone, precision and scale, children) and
   per-field `nullable`, in a flat field object (`{"name", "type",
   params…, "nullable"}`); see the
   [vocabulary](../protocol/messages.md#schema). New types and parameters are
   added in minor releases. They need no negotiation, because the `schema`
   message is informational: the Arrow IPC stream header is the authoritative
   schema, and a receiver reads an unknown type (or a known type with invalid
   parameters) as *unknown* and carries on. So a plugin that emits a type newer
   than the engine still runs; the engine just labels that column unknown.
   Older plugins' type strings, including `timestamp_ms`, stay valid
   throughout 1.x.
7. **Schema drift is checked against the Arrow stream.** The incremental-state
   fingerprint is computed from the exact Arrow schema the source writes on
   fd3 (names, order, types with their parameters, nullability; not metadata),
   read before the destination starts, with a documented, Arrow-independent
   encoding. This changed the fingerprint, so the state format is now
   **version 2**; legacy and version 1 files are migrated (the drift check is
   skipped once, with a warning, and the file is rewritten on the next save).
   See the [state file format](../architecture/incremental-state.md#state-file-format).
8. **Machine-readable run output: in 1.0.** `loadsmith run --output json`
   prints one JSON run summary on stdout instead of the human report, and
   `--summary-file <path>` writes the same document to a file. Two flags
   because the two consumers differ: a scheduler or container log that captures
   stdout, and an orchestrator that wants the human report on stdout and a
   result file beside it. The document has a `version` (now `1`), always
   carries every field (`null` when unknown), and is produced for setup
   failures too, with the exit code and the cause. Its schema is frozen in the
   [CLI reference](cli.md#machine-readable-run-summary); fields are only added
   within a version.
9. **Providers are wired into 1.0 (inline values).** A `providers:` block
   declares instances; `{{ secret('<instance>', '<ref>') }}` reads a value from
   one. The core runs the referenced instances before the state lock and before
   any data plugin, one `resolve` per distinct reference, and fails the run
   with exit `3` (`4` for a missing binary) if a value cannot be had. Loading a
   whole document from a remote reference (`--config-ref`) is deferred to 1.x.
   Three details were decided with it:
   - **D1 — one kind name: `provider`.** The former `config_provider` /
     `config-provider` kind is renamed everywhere: wire `"provider"`, manifest
     `kind: provider`, `--kind provider`, binary `loadsmith-provider-<type>`. The
     old spelling is not accepted (no engine ever ran one). Its `fetch`
     capability and the `finished.message` payload form are gone; a provider
     speaks `resolve`/`resolved`.
   - **D2 — `file()` values are masked**, like `env()` and `secret()` values:
     every resolved template value is treated as a secret, in logs, plugin
     stderr, error texts, the run summary and `--print-resolved-config`.
   - **D3 — `loadsmith validate` is static by default**: structure, template
     syntax, and every `secret()` naming a declared instance, with nothing
     evaluated or started. `validate --resolve` performs the full resolution,
     providers included.

10. **The parser kind is wired into 1.0 and covered.** A raw source
    (`object_output`) announces files; the core hands each to the `parser:`
    (`parse_object`), whose Arrow stream takes the source's place in the pump.
    The parser's input ends gracefully only after the raw source succeeded,
    and the commit point needs both to have succeeded, so a failure upstream
    never commits the files parsed so far. One schema per stream
    (`SCHEMA_MISMATCH`), zero files is `NO_INPUT` unless the parser declares
    its columns. **Deferred to 1.x:** a `parser:` combined with `state:`
    (incremental loads through a parser; `object_parsed.last_batch_seq` keeps
    per-file checkpoints additive).

11. **Unknown keys: an error by default, relaxed only at execution time.** A
    key Loadsmith does not know fails the run with exit `3`, wherever it is:
    in the file's own blocks (checked by the engine before anything starts)
    and inside each plugin's `config:` (checked by the plugin in `configure`,
    at any depth, `#[serde(flatten)]`ed structs included). Every unknown key
    is named by path. The operator relaxes this when running a file, never in
    the file: `--ignore-unknown-keys` on `run`/`validate`/`state show|rm`, or
    `LOADSMITH_IGNORE_UNKNOWN_KEYS=1`, turns each unknown key into a warning
    and ignores it, for the whole file and every plugin; the flag wins over
    the variable. Files generated by other programs often carry their own
    metadata keys, and they must run unedited; a per-block switch in the file
    was rejected because the choice belongs to whoever runs the file. The
    policy reaches plugins as the optional `configure.unknown_keys`
    (`error` | `warn`): additive, absent or unknown means `error`, ignored by
    plugins built before it. The Rust SDK applies it through
    `PluginConfig::parse`, so every SDK plugin behaves the same.

12. **Remote state backends: one state format.** A remote backend (the
    planned `s3` one) stores the same document as the local file, under the
    same `version` field and the same upgrade rules, so a state file moves
    between backends by copying it. Only where the document lives and how the
    lock is taken (a conditional write instead of a lock file) belong to the
    backend; that lock is documented with the backend when it ships, as an
    addition.

13. **The Rust crates are not published.** Plugins consume the SDK and its
    sibling crates by git revision; every crate in the workspace is marked
    `publish = false`. Publishing them to crates.io later is additive and
    does not change what 1.0 covers.
