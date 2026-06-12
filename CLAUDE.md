# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository. It is **operating instructions only** — for what
Loadsmith is, its architecture, and design rationale, read
[README.md](README.md), the docs under [`docs/src`](docs/src),
[`definitions/loadsmith.md`](definitions/loadsmith.md) /
[`definitions/protocol.md`](definitions/protocol.md), or the source itself.
Don't guess at "why" — go read it.

## Conventions

- **English only.** All artifacts committed to this repo — docs, code comments,
  commit messages, identifiers — must be in English, even when the user writes
  in Portuguese.
- **Keep docs in sync.** Whenever you change behavior, commands, architecture,
  or what's shipped, check whether `README.md`, the docs under
  [`docs/src`](docs/src), and [`ROADMAP.md`](ROADMAP.md) need updating too —
  "doc" means all three, not just one. Docs that drift from the code are worse
  than no docs.
- **Check rejected ideas before proposing architecture changes.** Before
  analyzing or proposing a non-trivial architectural change, check
  [docs/src/architecture/rejected-ideas.md](docs/src/architecture/rejected-ideas.md)
  — it may have already been considered and rejected, with reasoning that
  still applies. If you propose something already logged there, surface that
  instead of re-deriving the analysis from scratch.
- **Linux-first.** The core does Linux-specific fd passing via `dup2`/`pre_exec`
  in [`spawner.rs`](crates/loadsmith-core/src/spawner.rs) — the project's
  **only `unsafe` code**. It runs in the forked child and may only call
  async-signal-safe functions (libc `dup2`/`close`/`pipe2`); touch it carefully.
- **Multi-arch first.** Loadsmith targets both `linux/amd64` and `linux/arm64`
  (so it can run on AWS Graviton instances). The constraint is **not** "no TLS"
  — it's "no native/arch-specific code." Don't add dependencies, `unsafe`
  blocks, or build steps that assume x86_64 or pull in native/assembly-per-arch
  toolchains, because the release image is built `cargo build`-native inside each
  arch (under QEMU) and C/assembly crypto would mean slow emulated C builds plus
  fragile `cmake`/`perl`/`nasm` tooling. TLS **is** allowed and expected: do it
  via **`rustls`** with the pure-Rust crypto provider **`rustls-rustcrypto`**.
  Still banned because they pull native/assembly-per-arch crypto: `native-tls`,
  `openssl-sys`, `ring`, and `aws-lc-rs` (rustls's default provider — C). The
  crypto provider is a swappable knob (`CryptoProvider::install_default`), so if
  perf/FIPS ever forces `aws-lc-rs` it's a one-line change — but accept the C
  toolchain cost then, not now. See
  [docs/src/architecture/multi-arch-and-tls.md](docs/src/architecture/multi-arch-and-tls.md).
  The release image is built with
  `docker buildx build --platform linux/amd64,linux/arm64 -t loadsmith:<tag> .`
  against the existing [`Dockerfile`](Dockerfile) — it needs no per-arch
  branches; both base images already publish `arm64` variants.

## Commands

```bash
cargo build                      # builds the loadsmith core binary into target/debug/
cargo test --workspace           # all tests
cargo test -p loadsmith-core     # one crate
cargo test -p loadsmith-protocol handshake_roundtrip   # one test by name

# Plugins live in loadsmith-canonical-plugins now — install them, then run:
./target/debug/loadsmith plugin install --all          # the canonical set → ~/.loadsmith/plugins
./target/debug/loadsmith run pipeline.yaml             # discovers plugins in ~/.loadsmith/plugins
./target/debug/loadsmith run pipeline.yaml --dry-run --print-resolved-config
./target/debug/loadsmith run pipeline.yaml --log-level debug   # shows the full protocol handshake
```

There is no separate lint step beyond `cargo clippy`. The CI-equivalent gate is
`cargo build && cargo test --workspace`.

## Verify real changes against loadsmith-lab

Unit tests cover crates in isolation. The **real** validation — running
Loadsmith against an actual seeded Postgres and checking the output — lives in
the sibling repo **loadsmith-lab** (`../loadsmith-lab`, a separate workspace).
After changing core plumbing or a plugin, verify with:

```bash
cd ../loadsmith && cargo build          # the lab runs ../loadsmith/target/debug
cd ../loadsmith-lab && ./target/debug/loadsmith-lab run --loadsmith ../loadsmith --select catalog/postgres-to-jsonl
```

## Hard rules — read before touching these areas

- **Never wire a source plugin's data-plane pipe directly to a destination's.**
  The core must stay in the middle and pump every batch through two separate
  pipes ([`runner.rs`](crates/loadsmith-core/src/runner.rs),
  [`pump.rs`](crates/loadsmith-core/src/pump.rs)) — collapsing them into one
  bypasses the control plane and reintroduces a real bug from an early
  prototype. See [docs/architecture/data-flow.md](docs/src/architecture/data-flow.md).
- **Never let plugin event channels (fd4) go undrained.** The pump is
  synchronous Arrow I/O on `spawn_blocking`; fd4 must be drained concurrently
  ([`events.rs`](crates/loadsmith-core/src/events.rs)) or a plugin blocking on
  a full pipe deadlocks the run.
- **Keep the human report on stdout and tracing/diagnostics on stderr** — both
  loadsmith-lab and `--no-color`/`NO_COLOR`/`--log-level` consumers depend on
  that split.
- **Plugins live in their own repo now** —
  [`loadsmith-canonical-plugins`](../loadsmith-canonical-plugins) (a Cargo
  workspace that git-deps the SDK crates here, pinned by rev). This repo is the
  **core + SDK**. Plugin-specific rules (e.g. the Postgres source's deliberate
  `simple_query` text-protocol choice in its `types.rs`) live with the plugin
  there. To change a plugin, work in that repo; to change the protocol/SDK, work
  here and bump the rev the plugins pin.
