# Changelog

All notable changes to the Loadsmith engine are documented here. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) over
the interfaces listed in the
[Stability Policy](docs/src/reference/stability.md). Plugins have their own
release notes in their repositories.

## [Unreleased]

### Added

- [Stability Policy](docs/src/reference/stability.md): what 1.0 freezes (wire
  protocol v1, CLI, `pipeline.yaml`, state format, plugin manifest and index,
  image tags, plugin discovery), what it does not cover, and the decisions
  still open before 1.0.
- Protocol: the **tolerant-reader** compatibility rule, documented and
  implemented. A message of an unknown `type` is logged and skipped on every
  channel. Malformed lines are fatal on the control channel and skipped on the
  event channel.
- The state file has a format `version` (now `1`). `loadsmith state show`
  prints it.
- CI on every push and PR to `main`: build, tests and clippy with
  `-D warnings`, all `--locked`, on a toolchain pinned in
  `rust-toolchain.toml`.
- **Graceful cancel.** On any failure, timeout, or SIGINT/SIGTERM the core
  sends `cancel` to every live plugin (`error`, with a code, to the plugin at
  fault), waits `pipeline.timeouts.cancel_grace_seconds` (default 30), then
  kills each plugin's process group. The engine handles both signals
  explicitly (it is PID 1 in the images), releases the state lock, and exits
  `130` (SIGINT) or `143` (SIGTERM). Plugins now run in their own process
  group, so a terminal's Ctrl+C reaches only the engine.
- `pipeline.timeouts` in `pipeline.yaml`: limits for every wait on a plugin
  (handshake 60 s, configure 300 s, start 600 s, source finish 300 s,
  destination finalize 6 h), and an opt-in data-plane stall timeout
  (`stall_seconds`, off by default) with a separate first-batch budget.
- Protocol: the `resend` message. A malformed control line is answered once
  with `resend` and the peer repeats its last control message; a second
  malformed line in a row is fatal. Golden JSONL fixtures pin the wire form of
  `cancel`, `error`, `resend` and `finished`.
- **Distinct exit codes:** `0` success, `1` run failed, `2` usage, `3`
  configuration, `4` plugin not found or incompatible, `5` state (lock held,
  bad state file, schema drift), `130`/`143` signals. See the
  [CLI reference](docs/src/reference/cli.md#exit-codes).
- The run summary has a `Cause:` line with the root cause when a run fails or
  is cancelled.
- **Machine-readable run summary.** `loadsmith run --output json` prints one
  JSON document on stdout instead of the human report (logs stay on stderr),
  and `--summary-file <path>` writes the same document to a file. It carries a
  `version` (`1`), the pipeline, status, exit code, root cause, signal, the
  route (each plugin's kind, type, handshake name and version, binary path),
  rows read/written, batches, duration, throughput, RFC 3339 start/end times,
  the incremental watermark before and after, and sink deliveries. It is also
  produced for setup failures (unreadable pipeline, missing plugin, rejected
  config, held lock), which otherwise print only an `error:` line. The schema
  is covered by the stability policy. See the
  [CLI reference](docs/src/reference/cli.md#machine-readable-run-summary).
- Protocol: a **complete field-type vocabulary** for the `schema` message,
  covering Arrow's common types with their parameters: `int8`…`uint64`,
  `float16`, `decimal128`/`decimal256` (precision, scale), `large_utf8`,
  `utf8_view`, `large_binary`, `binary_view`, `fixed_size_binary`, `date64`,
  `time32`/`time64`, `timestamp` (unit `s`/`ms`/`us`/`ns`, optional
  `timezone`), `duration`, `interval`, `list`/`large_list`/`fixed_size_list`,
  `struct`, `map`, `dictionary`, `null`, and a per-field `nullable`. Parameters
  sit beside `type` in the field object. Older plugins keep working: bare type
  strings and `timestamp_ms` still parse, and `nullable` defaults to `true`. An
  unknown type (or a known one with invalid parameters) is read as unknown and
  never fails a run. See the
  [messages reference](docs/src/protocol/messages.md#schema).
- SDK: every plugin kind reads its control channel concurrently with its
  work, answers `ping`, honours `resend`, and stops on `cancel`, `error`, or a
  lost core. `cancel()` is now called whenever a run will not complete after
  `start` (including the plugin's own failure), before `finished`.
- **Providers are wired into the core.** A `providers:` block in
  `pipeline.yaml` declares provider instances (`type`, `config`,
  `timeout_seconds`), and `{{ secret('<instance>', '<ref>') }}` reads a value
  from one. Before the state lock and before any data plugin starts, the core
  spawns each referenced instance once (in parallel; unused ones are warned
  about and not started), sends one `resolve` per distinct reference, and
  substitutes the values. Values stay in memory, never on disk. A provider
  failure (unresolvable reference, crash, protocol violation, timeout —
  default 60 s per instance) fails the command with exit `3`; a missing or
  unusable provider binary with `4`. `secret()` naming an undeclared instance,
  or used in `pipeline:`, `state:` or `providers:`, is a static error. See the
  [pipeline reference](docs/src/reference/pipeline-yaml.md#providers).
- Protocol: `resolve{id, ref}` (core → provider) and `resolved{id, value |
  error{code, message}}` (provider → core), capability `resolve`; a provider
  replies `ready` to `start` and sends `finished` after stdin EOF. SDK:
  `ProviderPlugin` / `run_provider` / `ResolveFailure`. See
  [Writing a Provider](docs/src/plugins/writing-a-provider.md).
- Protocol (experimental, not wired into the core yet): the parser messages
  `parse_object{path}` and `object_parsed{path, rows, batches,
  last_batch_seq}`, capabilities `parse` and `object_output` for a raw source
  (which replies `ready` to `start`), and a strict `parser:` block in
  `pipeline.yaml` (rejected by `loadsmith run` for now; not combinable with
  `state:`). Golden fixtures pin every new message.
- `loadsmith validate --resolve`: the full resolution `run` performs,
  providers included.
- `loadsmith state show|rm` resolve only `pipeline:` and `state:`: they never
  start a provider and no longer need the other blocks' templates (database
  credentials) to resolve.

### Changed

- `plugin install --all` (no package name) skips, with a warning, any package
  whose manifest this loadsmith cannot use (unknown plugin kind, no supported
  protocol version) instead of failing the whole install — e.g. the published
  `file` 0.1.x, whose manifest still declares `kind: config-provider`.

- **Breaking: the `config_provider` kind is renamed `provider`** (wire
  `"provider"`, manifest `kind: provider`, `--kind provider`, binary
  `loadsmith-provider-<type>`); the old spelling is not accepted. The provider
  `fetch` capability, the `finished.message` payload form and the SDK's
  `ConfigProviderPlugin` / `run_config_provider` are removed.
- **Breaking: `loadsmith validate` is static by default.** It checks the
  structure, the template syntax and that every `secret()` names a declared
  provider instance, and evaluates nothing (an unset environment variable
  passes). Use `--resolve` for the previous behavior, plus providers.
- **Breaking: the `aws.sm(...)` template form is retired** (it only ever
  returned an error); use a provider and `secret()`. Template arguments are now
  parsed strictly (string literals, no nesting) and errors name their location.
- **Breaking: plugin stderr is piped through the core** instead of inherited:
  it is copied to the engine's stderr line by line, masked. A provider's log
  lines and stderr are held until its conversation is over, then printed.
- **Every resolved template value is masked, everywhere**: `env()`, `file()`
  (newly masked) and `secret()` values, also line by line for multi-line
  values (trimmed lines of at least 8 characters) and in their JSON-escaped
  spelling, in plugin log lines, plugin stderr, plugin error texts
  (`configure_ack`, `finished`, `resolved`), abort reasons, the final `error:`
  line, the run summary's `cause` and `--print-resolved-config` (before:
  `env()` values in `--print-resolved-config` only).
- `run --dry-run` and `run --print-resolved-config` resolve providers too
  (they start the provider plugins, never the data plugins).
- `run` evaluates every `env()`/`file()` before it starts a provider, so an
  unset variable fails without side effects.

- **Protocol v1 now has a commit point.** The core writes the Arrow IPC
  end-of-stream marker to the destination's fd3 only after the source's
  stream ended with its own marker and the source sent `finished` with
  success. After an upstream failure the destination gets `cancel` and its
  fd3 stays open until it exits. Destinations must commit only on a clean end
  of input and roll back in `cancel()`; the canonical plugins need an SDK bump
  for their `cancel()` to take effect (older SDK builds are killed after the
  grace period instead, which also rolls a database transaction back).
- The core sends `error` (code `UNSUPPORTED_PROTOCOL_VERSION`,
  `WRONG_PLUGIN_KIND`, `PROTOCOL_VIOLATION`, `MALFORMED_MESSAGE` or `TIMEOUT`)
  instead of just closing a faulty plugin's stdin. `cancel` and `error` are no
  longer experimental.
- SDK: a destination whose fd3 ends without the end-of-stream marker reports
  `INPUT_TRUNCATED` and rolls back instead of finalizing. New `finished` error
  codes: `SCHEMA_FAILED`, `PREPARE_FAILED`, `IPC_WRITE_FAILED`,
  `FINALIZE_FAILED`, `MALFORMED_MESSAGE`, `PROTOCOL_VIOLATION`,
  `CONTROL_CHANNEL_FAILED`, `PLUGIN_FAILED`. A plugin that fails after `start`
  now exits `1`.
- **`pipeline.yaml`: unknown keys in `source`, `destination` and `sink` blocks
  are now rejected.** Before, a typo such as `confg:` passed validation and the
  plugin ran with an empty config. Only `type` and `config` are allowed there;
  `config:` contents are still free-form.
- The schema fingerprint in the state file is now a stable 64-bit FNV-1a
  hash. **Migration is automatic.** A state file written by 0.2.0 or earlier
  still loads and its cursor is honoured. For that one run the schema-drift
  check is skipped (with a warning), because the old fingerprint cannot be
  compared. The file is then rewritten in the new format on the next save.
- **The schema-drift fingerprint now comes from the source's Arrow IPC
  stream**, not the `schema` message: names, order, nullability and exact
  types with their parameters (timestamp unit and time zone, decimal
  precision, list/struct/map children; metadata is ignored). The core reads
  the stream header before it starts the destination (within
  `timeouts.start_seconds`), so a drifted run is refused before anything is
  loaded, and a source that declares a schema but never writes its stream
  fails during setup. The state format is now **version 2**. **Migration is
  automatic**: a legacy or version 1 state file loads, its cursor is honoured,
  the drift check is skipped for that one run with a warning, and the file is
  rewritten as version 2 on the next save. `loadsmith state show` says when a
  file still has an old fingerprint.
- The run log's schema lines show the exact Arrow types from the stream
  (`created_at: timestamp(us, UTC)`, `amount: decimal128(12, 2) not null`,
  `tags: list<utf8?>`) instead of the lossy labels. A source whose `schema`
  message disagrees with its stream gets a warning.
- A state file with a newer format version than the engine understands is
  refused instead of being silently downgraded.
- Release builds pin `rust:1.99.0-alpine` instead of the floating
  `rust:1-alpine`, run the workspace tests on each architecture before
  building, and build with `--locked`. The hermetic `Dockerfile` uses the same
  pinned image. Workflow actions moved to their current major versions.
- SDK: a source's `schema` message is now derived from its Arrow schema
  exactly. Before, every timestamp unit was labelled `timestamp_ms` and every
  other type outside nine names (a postgres array, a decimal, a time) was
  reported as `utf8`. Crate API: `loadsmith_protocol::Field` gains `nullable`
  (build fields with `Field::new`), `FieldType` gains the new types, and
  `loadsmith-arrow` gains exact Arrow ↔ protocol conversions
  (`protocol_fields_from_schema`, `arrow_type_from_protocol`, …).
- `loadsmith-core`, `loadsmith-config` and `loadsmith-cli` are marked
  `publish = false`.
- `async-trait` 0.1.89 → 0.1.92, which clears 17 clippy `double_must_use`
  warnings on the SDK plugin traits.

### Fixed

- The `error:` line of a configuration error printed the inner message twice
  (`configuration error: validation error: X: validation error: X`).
- Error texts no longer print a plugin kind's Rust spelling
  (`ConfigProvider`): one helper spells every kind.
- `loadsmith_arrow::record_batch_to_json_rows` (the jsonl destination's
  converter) **panicked on any timestamp not in milliseconds** and silently
  wrote `null` for `int8`/`int16`, unsigned integers, decimals, lists,
  structs, maps, `large_binary`, times and more; NaN and ±Infinity became `0`.
  It now handles every Arrow type without panicking: decimals as exact
  strings, lists as arrays, structs and maps as objects, timestamps of every
  unit as epoch milliseconds (like millisecond timestamps already were),
  NaN/Infinity as `null`. Output for the types that already worked is
  unchanged.
- **A source that crashed mid-stream made the destination commit a partial
  load.** arrow-rs reads EOF at a batch boundary as a clean end of stream, so
  the pump ended the destination's input and postgres/mysql/oracle committed
  while the run was marked failed. The core now tells a clean end from a
  truncated one and never ends the destination's input after a failure.
- A failing source made the run return early: the destination's outcome was
  never collected and no summary was printed. Both outcomes are now always
  collected, and the summary always prints once data has flowed.
- The other plugin was never told about a failure; it now gets `cancel`.
- The core waited forever on a plugin that stopped answering during the
  handshake, configure, start or finish phases; each phase now has a timeout.
- The event-channel (fd4) drain stopped at the first unknown or malformed
  line. The pipe then filled up and the plugin blocked forever. It now logs
  the line and keeps draining until EOF.
- `ControlReader` failed on any unknown message `type`, which contradicted the
  protocol's forward-compatibility promise. A line with invalid UTF-8 is now a
  parse error that consumes the line, not an I/O error.
- Template resolution mangled every non-ASCII character in `pipeline.yaml`
  (`São` became `SÃ£o`), including in queries sent to plugins.
- The state lock lease was renewed only after the source's first query
  returned, so a start slower than the 60s lease let another run steal the
  lock. Renewal now starts as soon as the lock is acquired.
- A run whose lease had expired and been stolen deleted the new holder's lock
  file when it finished. Each acquisition now has a unique holder token, and a
  run renews or removes only a lock that still carries its own token.

## [0.2.0] - 2026-10-16

### Added

- Static musl build of the engine (no libc or dynamic loader), with mimalloc as
  the global allocator. One binary per architecture runs on any Linux.
- Four multi-arch image flavors from that binary: `-static` (distroless,
  ~13MB), `-static-debug` (+ busybox), `-slim` (`:latest`) and `-bundled` (slim
  + the canonical plugins pinned from the index at release time). Each is
  tagged `<tag>-<flavor>`, `sha-*-<flavor>` and the rolling `<flavor>`.
- The hermetic `Dockerfile` gains the same targets.
- Container Images documentation page.

[Unreleased]: https://github.com/loadsmith-el/loadsmith/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/loadsmith-el/loadsmith/compare/v0.1.1...v0.2.0
