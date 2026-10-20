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

### Changed

- **`pipeline.yaml`: unknown keys in `source`, `destination` and `sink` blocks
  are now rejected.** Before, a typo such as `confg:` passed validation and the
  plugin ran with an empty config. Only `type` and `config` are allowed there;
  `config:` contents are still free-form.
- The schema fingerprint in the state file is now a stable 64-bit FNV-1a
  hash. **Migration is automatic.** A state file written by 0.2.0 or earlier
  still loads and its cursor is honoured. For that one run the schema-drift
  check is skipped (with a warning), because the old fingerprint cannot be
  compared. The file is then rewritten in the new format on the next save.
- A state file with a newer format version than the engine understands is
  refused instead of being silently downgraded.
- Release builds pin `rust:1.99.0-alpine` instead of the floating
  `rust:1-alpine`, run the workspace tests on each architecture before
  building, and build with `--locked`. The hermetic `Dockerfile` uses the same
  pinned image. Workflow actions moved to their current major versions.
- `loadsmith-core`, `loadsmith-config` and `loadsmith-cli` are marked
  `publish = false`.
- `async-trait` 0.1.89 → 0.1.92, which clears 17 clippy `double_must_use`
  warnings on the SDK plugin traits.

### Fixed

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
