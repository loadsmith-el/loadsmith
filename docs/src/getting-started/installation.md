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

This produces all binaries under `target/release/`:

```
target/release/
  loadsmith                           ← the CLI
  loadsmith-source-postgres           ← postgres source plugin
  loadsmith-destination-jsonl         ← JSONL destination plugin
  loadsmith-destination-null          ← null destination plugin (discards rows)
  loadsmith-config-provider-file      ← file:// config provider plugin
```

For development, use `cargo build` (debug mode):

```bash
cargo build
./target/debug/loadsmith run pipeline.yaml --plugin-dir target/debug
```

## Plugin directory

Loadsmith discovers plugins by searching for binaries named
`loadsmith-{kind}-{type}` in the plugin directory. The default plugin directory
is:

```
~/.loadsmith/plugins/
```

To install the built plugins:

```bash
mkdir -p ~/.loadsmith/plugins
cp target/release/loadsmith-source-postgres ~/.loadsmith/plugins/
cp target/release/loadsmith-destination-jsonl ~/.loadsmith/plugins/
cp target/release/loadsmith-destination-null ~/.loadsmith/plugins/
cp target/release/loadsmith-config-provider-file ~/.loadsmith/plugins/
```

Or use the built-in install command:

```bash
loadsmith plugin install target/release/loadsmith-source-postgres
```

This copies the binary to the default plugin directory.

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
