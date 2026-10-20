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
| **Wire protocol v1** | The fd layout (fd0 control core→plugin, fd1 control plugin→core, fd3 Arrow IPC data, fd4 events; stderr is free-form). JSONL framing. Every message's `type` and required fields. Handshake and version negotiation (`protocol_supported_versions` → `set_protocol_version`). Lifecycle ordering. The [tolerant-reader rule](../protocol/reference.md#compatibility-rule-tolerant-reader). | New optional fields. New message types a receiver may safely ignore. A change the receiver must understand needs **protocol v2**, negotiated in the handshake. Every 1.x engine keeps speaking v1. |
| **CLI** | Commands and subcommands, flags and their defaults and meaning, exit codes (`0` success, non-zero failure; see the [CLI reference](cli.md)), and the split of the human report on stdout from diagnostics on stderr. | New commands and new flags. |
| **`pipeline.yaml` schema** | Every core-owned key, its type, default and meaning. Unknown core-owned keys are rejected. The template syntax `{{ env('VAR') }}` / `{{ file('/path') }}`. | New optional keys and new template functions. The contents of each plugin's `config:` block belong to that plugin and are versioned by it. |
| **Incremental state file** | The [state file format](../architecture/incremental-state.md#state-file-format), versioned by its `version` field, and the lock file's lease semantics. | A new engine reads every older format and upgrades it on its next save. An engine refuses a format newer than it understands rather than downgrading it. |
| **Plugin manifest** | `apiVersion: loadsmith.dev/plugin/v1` and its fields (`name`, `version`, `protocol`, `provides`, `runtime`). | New optional fields. Incompatible changes go into a new `apiVersion`, and v1 stays accepted throughout 1.x. |
| **Plugin index** | The `index.json` shape: `{"plugins": {"<name>": {"latest": "<version>", "versions": {"<version>": "<manifest URL>"}}}}`. | New optional fields. |
| **Image tags** | `ghcr.io/loadsmith-el/loadsmith` with the flavors `static`, `static-debug`, `slim` and `bundled`. Each release publishes `:vX.Y.Z-<flavor>` and `:sha-<sha7>-<flavor>`. A stable release also moves the rolling `:<flavor>` and `:latest` (= `slim`); a prerelease moves no rolling tag. The binary is `/usr/local/bin/loadsmith` and is the entrypoint. | New flavors. |
| **Plugin discovery** | A plugin of kind `<kind>` and type `<type>` is the executable `loadsmith-<kind>-<type>`. It is looked up in `--plugin-dir` (default `$HOME/.loadsmith/plugins`) first, then on `PATH`. | New kinds. |

## Not covered

These can change in any release:

- **Rust crate APIs.** `loadsmith-plugin-sdk`, `loadsmith-protocol`,
  `loadsmith-arrow`, `loadsmith-transport` and `loadsmith-tls` stay **0.x**,
  versioned apart from the engine. Their public APIs expose `arrow` and `rustls`
  types, so they have to follow those crates' breaking releases. Plugins pin
  them by git revision. A newer SDK may need code changes in a plugin, but the
  plugin's **wire** compatibility does not change. `loadsmith-plugin-manifest`
  is in the same position: the manifest *format* is covered and the crate API is
  not.
- **Engine internals.** `loadsmith-core`, `loadsmith-config` and `loadsmith-cli`
  are not libraries. They are marked `publish = false`.
- **Human-readable output.** The report header, the summary box, progress lines,
  log messages and the format of the tracing output on stderr. Don't parse them.
- **Experimental parts** (defined, but not wired into the core):
  - the `parser` and `config-provider` plugin kinds. They exist in the protocol,
    the SDK and the manifest's known kinds, but the core never spawns them.
  - the core-to-plugin `cancel` and `error` control messages. They are part of
    the message set, but the core does not send them today, and their semantics
    may change before they are.

  Experimental parts become covered when they are documented without this label.
- **Behavior that contradicts the documentation** is a bug. Fixing it is not a
  breaking change.

## How interfaces evolve

- **Tolerant reader.** On every channel, a well-formed message of an unknown
  `type` is logged and skipped, and unknown fields are ignored. A malformed line
  is fatal on the control channel and skipped on the event channel, which is
  never left undrained. The full table is in the
  [protocol reference](../protocol/reference.md#compatibility-rule-tolerant-reader).
- **Deprecation.** Before a covered item is removed, it is deprecated. It keeps
  working for the rest of the major version, warns when used, and is listed in
  the CHANGELOG. It is removed only in the next major.
- **Toolchain.** The engine is built with the Rust version pinned in
  `rust-toolchain.toml`. Building from source with an older compiler is not
  supported.

## Open decisions before 1.0

These are still open, and each one changes what 1.0 freezes:

1. **Exit codes.** Today every failure exits `1`. Before the freeze, decide
   whether to reserve distinct codes (for example config/validation vs.
   plugin/protocol vs. data failure) or to promise only "non-zero".
2. **Machine-readable run output.** Human output is not covered, so automation
   has nothing stable to parse except the exit code. A JSON summary (a flag or
   a file) would close that gap. Add it before 1.0 or as a 1.x minor?
3. **Malformed control-channel lines** are fatal (the current rule). The
   alternative is to skip them like event-channel lines. Fatal is the
   conservative choice: a garbled lifecycle means a broken peer.
4. **Experimental kinds.** Wire `config-provider` (and `parser`) into the core
   before 1.0, or ship 1.0 without them and add them in a 1.x minor?
5. **`cancel` / `error` from the core.** Wire them (for example graceful cancel
   on SIGTERM) or drop them from protocol v1 before it is frozen.
6. **Arrow field types.** A plugin that emits a field type added in 1.y needs a
   1.y engine. Is "minor, with a minimum engine version" acceptable, or should
   new field types be negotiated?
7. **Remote state backends.** The `s3` state backend will need its own format
   and lock semantics. Should they fall under the same state-format version?
8. **Publishing the SDK crates** to crates.io (they are consumed by git
   revision today), and whether `loadsmith-plugin-manifest` should also be
   `publish = false`.
