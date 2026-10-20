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
| `--dry-run` | off | Parse, resolve templates and validate the pipeline, then exit without spawning plugins (plugin binaries are not checked) |
| `--print-resolved-config` | off | Print the resolved config (with secrets masked) to stdout and exit, without running |

**Exit codes:**

| Code | Meaning |
|---|---|
| `0` | Pipeline completed successfully (summary `Status: success`) |
| `1` | Anything else: the pipeline ran and failed (`Status: error` — a source, destination or sink error), or it couldn't start (invalid YAML/config, missing plugin binary, protocol mismatch). The cause is printed to stderr |

**Output:**

- **stdout** — the human report: the header (`loadsmith <version>`, then the
  route) and the summary box. Suitable for piping to a file.
- **stderr** — tracing output at the configured log level, including the schema
  and the progress lines at doubling batch intervals (timestamped `INFO`
  records). Includes the full protocol handshake at `--log-level debug`.

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

Validate a pipeline file. Does not look for plugin binaries, connect to any
system, or run any data — plugin-specific config is only checked by the plugin
itself at run time.

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
| `--plugin-dir <path>` | `~/.loadsmith/plugins/` | Accepted but currently unused |

**What is checked:**
1. YAML syntax is valid
2. Template expressions (`{{ env(...) }}`, `{{ file(...) }}`) can be resolved
3. Unknown keys at the top level, under `pipeline:` and under `state:` are
   rejected (extra keys beside a plugin's `type`/`config` are not)
4. `pipeline.name`, `source.type`, `destination.type` and (if present)
   `sink.type` are non-empty
5. With a `state:` block, `state.backend` is `local` and `state.path` is
   non-empty

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
Plugins in /home/u/.loadsmith/plugins:
  loadsmith-source-postgres
  loadsmith-destination-postgres
```

Every file in the directory whose name starts with `loadsmith-` is listed; the
binaries are not run, so no versions are shown. An empty directory prints
`No plugins found in <dir>`.

---

## `loadsmith plugin install [<name>]`

Install plugins into the plugin directory (`~/.loadsmith/plugins/`). Plugins come
from [`loadsmith-canonical-plugins`](https://github.com/loadsmith-el/loadsmith-canonical-plugins)
via a canonical index, but you can also install from a manifest or a local binary.

A **package** (e.g. `mysql`) provides one or more **plugins** — its binaries,
one per kind (`loadsmith-source-mysql`, `loadsmith-destination-mysql`). The
install spec is `<name>[:<kind>][@<version>]`: a bare package installs all its
plugins; a `:<kind>` (or `--kind`) installs just one.

```bash
loadsmith plugin install postgres            # the package — all its plugins
loadsmith plugin install postgres --all      # explicit "all plugins of postgres"
loadsmith plugin install mysql:source        # just the source plugin
loadsmith plugin install mysql --kind source # same, via the flag
loadsmith plugin install postgres@0.1.0      # a pinned version
loadsmith plugin install mysql:source@0.1.0  # pinned version, one kind
loadsmith plugin install --all               # the whole canonical set
loadsmith plugin install --manifest ./loadsmith-plugin.yaml   # a manifest path or file/http/https URL
loadsmith plugin install --binary ./target/release/loadsmith-destination-jsonl  # a local binary
```

**Flags:**

| Flag | Default | Description |
|---|---|---|
| `--kind <kind>` | — | Install only this kind from the package (`source`/`destination`/`sink`/`parser`/`config-provider`). Same as the `:<kind>` spec suffix. |
| `--all` | — | With no name: every package in the index. With a `<name>`: every plugin of that package. |
| `-f, --manifest <path\|URL>` | — | Install from a `loadsmith-plugin.yaml` |
| `--binary <path>` | — | Install a single local plugin binary |
| `--index <url>` | the canonical index | Override the plugin index |
| `--plugin-dir <path>` | `~/.loadsmith/plugins/` | Target directory |

**Version pinning.** `@<version>` resolves a specific release from the index
(`entry.versions`; otherwise the package's `latest`) and composes with the kind
selector — `mysql:source@0.1.0` is the source plugin of mysql 0.1.0. There is
**one installed version at a time**: the on-disk binary is `loadsmith-{kind}-{name}`
(no version in the filename), so reinstalling a different version overwrites it.
`--all` always pulls each package's `latest`.

Index/manifest installs download a per-platform artifact, **verify its sha256**,
and refuse a manifest whose declared `protocol` range is incompatible with this
core (before fetching). Selecting a kind a package doesn't provide (e.g.
`jsonl:source`) is a clear error.

## `loadsmith plugin uninstall <name>[:<kind>]`

Remove an installed plugin's binaries. A bare package removes all of them
(`uninstall postgres` → `loadsmith-*-postgres`); a `:<kind>` (or `--kind`)
removes just that one.

```bash
loadsmith plugin uninstall postgres            # all of postgres' binaries
loadsmith plugin uninstall mysql:source        # just loadsmith-source-mysql
loadsmith plugin uninstall mysql --kind source # same, via the flag
```

**Flags:**

| Flag | Default | Description |
|---|---|---|
| `--kind <kind>` | — | Remove only this kind (same as the `:<kind>` suffix) |
| `--plugin-dir <path>` | `~/.loadsmith/plugins/` | Directory to remove from |

An `@<version>` is rejected (`uninstall takes no @version`): only one version is
installed at a time.

---

## `loadsmith state show <pipeline>`

Print the incremental state persisted for a pipeline. The pipeline's `state:`
block locates the state.

```bash
loadsmith state show pipeline.yaml
```

**Output:**
```
Pipeline:     orders-incremental
Format:       v1
Cursor value: "2026-06-09T08:00:00Z"
Schema hash:  …
Run id:       …
Updated at:   1781078400000 (unix ms)
```

or `No state recorded for pipeline '<name>'.` when nothing has been persisted
yet.

## `loadsmith state rm <pipeline>`

Clear a pipeline's persisted state, so the next run is a full read. It takes the
state lock first, so it fails rather than clear state out from under a running
pipeline. Prints `Cleared state for pipeline '<name>'.`

```bash
loadsmith state rm pipeline.yaml
```

Both commands fail with `pipeline '<name>' has no state: block` when the pipeline
has no `state:` block.

---

## Environment variables

| Variable | Equivalent to | Description |
|---|---|---|
| `NO_COLOR` | `--no-color` | Disable ANSI color (when set, to any value — even empty) |
| Any variable in `{{ env('NAME') }}` | — | Injected into pipeline template resolution |

Either `--no-color` or `NO_COLOR` disables color. The plugin directory has no
environment variable: use `--plugin-dir`.
