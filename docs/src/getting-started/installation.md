# Installation

## Prerequisites

- Rust toolchain (stable, 2021 edition) — install via [rustup.rs](https://rustup.rs)
- `cargo` in `PATH`
- Linux (the core uses Linux-specific fd passing via `dup2`; macOS support is
  planned)

## Build from source

```bash
git clone https://github.com/loadsmith-el/loadsmith
cd loadsmith
cargo build --release
```

This produces the loadsmith **core** binary at `target/release/loadsmith` (for
development, `cargo build` puts it at `target/debug/loadsmith`). Plugins are not
built here — they live in
[`loadsmith-canonical-plugins`](https://github.com/loadsmith-el/loadsmith-canonical-plugins)
and are installed on demand.

## Plugins

Loadsmith discovers plugins by searching for binaries named
`loadsmith-{kind}-{type}` in the plugin directory (default `~/.loadsmith/plugins/`).
Install the canonical set with the built-in installer:

```bash
loadsmith plugin install --all        # install every canonical plugin
loadsmith plugin install postgres     # or just one (resolved from the index)
```

You can also install from a manifest (`--manifest <path|URL>`) or a local binary
(`--binary <path>`) — see the [CLI reference](../reference/cli.md). Index and
manifest installs download a per-platform artifact and verify its sha256.

## Overriding the plugin directory

You can override the plugin directory at runtime:

```bash
# via flag
loadsmith run pipeline.yaml --plugin-dir /path/to/plugins

# via environment variable
LOADSMITH_PLUGIN_PATH=/path/to/plugins loadsmith run pipeline.yaml
```

The flag takes precedence over the environment variable.

## Checking installed plugins

```bash
loadsmith plugin list
# loadsmith-source-postgres (0.1.0)
# loadsmith-destination-jsonl (0.1.0)
# loadsmith-config-provider-file (0.1.0)
```

## Environment variables

| Variable | Purpose |
|---|---|
| `LOADSMITH_PLUGIN_PATH` | Override plugin directory |
| `NO_COLOR` | Disable ANSI color output (same as `--no-color`) |
| Any variable referenced by `{{ env('VAR') }}` in pipeline.yaml | Secret injection |
