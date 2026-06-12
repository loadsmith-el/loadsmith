# CLI Reference

The `loadsmith` binary is the entry point for all operations.

## Global flags

These flags apply to every subcommand:

| Flag | Default | Description |
|---|---|---|
| `--log-level <level>` | `info` | Log verbosity. Values: `trace`, `debug`, `info`, `warn`, `error`. Logs go to stderr |
| `--no-color` | off | Disable ANSI color and formatting in all output. Equivalent to setting `NO_COLOR=1` |

## `loadsmith run <pipeline>`

Execute a pipeline.

```bash
loadsmith run pipeline.yaml
loadsmith run pipeline.yaml --plugin-dir /opt/loadsmith/plugins   # override the default ~/.loadsmith/plugins
loadsmith run pipeline.yaml --dry-run
loadsmith run pipeline.yaml --dry-run --print-resolved-config
loadsmith run pipeline.yaml --log-level debug
loadsmith run pipeline.yaml --no-color
```

**Arguments:**

| Argument | Description |
|---|---|
| `<pipeline>` | Path to the pipeline YAML file |

**Flags:**

| Flag | Default | Description |
|---|---|---|
| `--plugin-dir <path>` | `~/.loadsmith/plugins/` | Directory to search for plugin binaries |
| `--dry-run` | off | Validate the pipeline, discover plugins, and exit without running |
| `--print-resolved-config` | off | Print the resolved config (with secrets masked) to stdout and exit. Implies `--dry-run` |

**Exit codes:**

| Code | Meaning |
|---|---|
| `0` | Pipeline completed successfully |
| `1` | Pipeline failed (source error, destination error, validation error) |
| `2` | Configuration error (invalid YAML, missing plugin binary) |

**Output:**

- **stdout** — the human report: header line, progress lines at doubling intervals,
  and the summary box. Suitable for piping to a file.
- **stderr** — tracing output at the configured log level. Includes the full
  protocol handshake at `--log-level debug`.

**Examples:**

Run a pipeline using plugins from a custom directory:
```bash
./target/debug/loadsmith run pipeline.yaml --plugin-dir /opt/loadsmith/plugins
```

Validate without running (useful in CI to catch config errors early):
```bash
loadsmith run pipeline.yaml --dry-run
```

See the full resolved config with secrets masked:
```bash
loadsmith run pipeline.yaml --dry-run --print-resolved-config
```

Run without colors (for log files, CloudWatch, CI):
```bash
NO_COLOR=1 loadsmith run pipeline.yaml
# or:
loadsmith run pipeline.yaml --no-color
```

Inspect the full protocol handshake:
```bash
loadsmith run pipeline.yaml --log-level debug 2>handshake.log
```

---

## `loadsmith validate <pipeline>`

Validate a pipeline file and check that the required plugin binaries exist.
Does not connect to any system or run any data.

```bash
loadsmith validate pipeline.yaml
loadsmith validate pipeline.yaml --plugin-dir /opt/loadsmith/plugins
```

**Arguments:**

| Argument | Description |
|---|---|
| `<pipeline>` | Path to the pipeline YAML file |

**Flags:**

| Flag | Default | Description |
|---|---|---|
| `--plugin-dir <path>` | `~/.loadsmith/plugins/` | Plugin directory for binary discovery check |

**What is checked:**
1. YAML syntax is valid
2. Required fields are present (`pipeline.name`, `source.type`, `destination.type`)
3. Template expressions (`{{ env(...) }}`, `{{ file(...) }}`) can be resolved
4. Plugin binaries `loadsmith-source-{type}` and `loadsmith-destination-{type}`
   exist in the plugin directory or PATH

---

## `loadsmith plugin list`

List installed plugins in the plugin directory.

```bash
loadsmith plugin list
loadsmith plugin list --plugin-dir /opt/loadsmith/plugins
```

**Flags:**

| Flag | Default | Description |
|---|---|---|
| `--plugin-dir <path>` | `~/.loadsmith/plugins/` | Directory to search |

**Output:**
```
loadsmith-source-postgres (0.1.0)
loadsmith-destination-jsonl (0.1.0)
loadsmith-config-provider-file (0.1.0)
```

The version is read from the binary's `--version` output.

---

## `loadsmith plugin install [<name>]`

Install plugins into the plugin directory (`~/.loadsmith/plugins/`). Plugins come
from [`loadsmith-canonical-plugins`](https://github.com/loadsmith-el/loadsmith-canonical-plugins)
via a canonical index, but you can also install from a manifest or a local binary.

```bash
loadsmith plugin install postgres            # resolve from the index (latest)
loadsmith plugin install postgres@0.1.0      # a pinned version
loadsmith plugin install --all               # the whole canonical set
loadsmith plugin install --manifest ./loadsmith-plugin.yaml   # a manifest path or file/http/https URL
loadsmith plugin install --binary ./target/release/loadsmith-destination-jsonl  # a local binary
```

**Flags:**

| Flag | Default | Description |
|---|---|---|
| `--all` | — | Install every plugin in the index |
| `--manifest <path\|URL>` | — | Install from a `loadsmith-plugin.yaml` |
| `--binary <path>` | — | Install a single local plugin binary |
| `--index <url>` | the canonical index | Override the plugin index |
| `--plugin-dir <path>` | `~/.loadsmith/plugins/` | Target directory |

Index/manifest installs download a per-platform artifact, **verify its sha256**,
and refuse a manifest whose declared `protocol` range is incompatible with this
core (before fetching).

## `loadsmith plugin uninstall <name>`

Remove an installed plugin's binaries by type name (e.g. `uninstall postgres`
removes `loadsmith-*-postgres`).

```bash
loadsmith plugin uninstall postgres
```

---

## Environment variables

| Variable | Equivalent to | Description |
|---|---|---|
| `NO_COLOR` | `--no-color` | Disable ANSI color (any non-empty value) |
| `LOADSMITH_PLUGIN_PATH` | `--plugin-dir` | Override the plugin directory |
| Any variable in `{{ env('NAME') }}` | — | Injected into pipeline template resolution |

`--no-color` and `--plugin-dir` flags take precedence over their corresponding
environment variables when both are set.
