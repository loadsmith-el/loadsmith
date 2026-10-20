# Installation

## Prerequisites

- Linux (the core uses Linux-specific fd passing via `dup2`)

## Prebuilt binaries and images

Each [GitHub Release](https://github.com/loadsmith-el/loadsmith/releases) attaches
static `linux/amd64` and `linux/arm64` binaries that run on any Linux
distribution, and publishes the container images described in
[Container Images](./container-images.md):

```bash
docker run --rm ghcr.io/loadsmith-el/loadsmith:latest --version
```

## Build from source

Building needs a Rust toolchain (stable, 2021 edition — install via
[rustup.rs](https://rustup.rs)) with `cargo` in `PATH`.

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
`loadsmith-{kind}-{type}` in the plugin directory (default `~/.loadsmith/plugins/`),
then on `PATH` as a fallback. Install the canonical set with the built-in installer:

```bash
loadsmith plugin install --all        # install every canonical plugin
loadsmith plugin install postgres     # or one package — all its binaries (source + destination)
loadsmith plugin install mysql:source@0.1.1   # one kind of a package, pinned version
loadsmith plugin uninstall postgres   # remove a package (or `postgres:source`)
```

You can also install from a manifest (`--manifest <path|URL>`) or a local binary
(`--binary <path>`) — see the [CLI reference](../reference/cli.md). Index and
manifest installs download a per-platform artifact and verify its sha256.

## Overriding the plugin directory

You can override the plugin directory at runtime with `--plugin-dir`:

```bash
loadsmith run pipeline.yaml --plugin-dir /path/to/plugins
```

A binary not found there is still looked up on `PATH`.

## Checking installed plugins

```bash
loadsmith plugin list
# Plugins in /home/you/.loadsmith/plugins:
#   loadsmith-source-postgres
#   loadsmith-destination-postgres
#   loadsmith-destination-jsonl
```

With an empty directory it prints `No plugins found in <dir>`.

## Environment variables

| Variable | Purpose |
|---|---|
| `NO_COLOR` | Disable ANSI color output (same as `--no-color`) |
| Any variable referenced by `{{ env('VAR') }}` in pipeline.yaml | Secret injection |
