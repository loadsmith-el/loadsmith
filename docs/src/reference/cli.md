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
loadsmith run pipeline.yaml --output json                # one JSON run summary on stdout
loadsmith run pipeline.yaml --summary-file run.json     # human report on stdout, JSON in a file
```

**Arguments:**

| Argument | Description |
|---|---|
| `<pipeline>` | Path to the pipeline YAML file |

**Flags:**

| Flag | Default | Description |
|---|---|---|
| `--plugin-dir <path>` | `~/.loadsmith/plugins/` | Directory to search for plugin binaries |
| `--dry-run` | off | Parse, resolve every template and validate the pipeline — the provider instances `secret()` references are started and resolve their values — then exit without spawning any data plugin (source/destination/sink binaries are not checked) |
| `--print-resolved-config` | off | Resolve the config (providers included), print it to stdout with every `env()`/`file()`/`secret()` value masked, and exit without starting any data plugin |
| `--output <human\|json>` | `human` | What stdout carries: the human report, or one JSON [run summary](#machine-readable-run-summary) instead |
| `--summary-file <path>` | — | Also write the JSON [run summary](#machine-readable-run-summary) to this file, whatever `--output` says |

**Exit codes:** see [Exit codes](#exit-codes). A run that fails once data is
flowing prints the summary with `Status: error` and a `Cause:` line; a run that
cannot start prints the cause as an `error:` line on stderr. With
`--output json` or `--summary-file`, both cases also produce the
[run summary](#machine-readable-run-summary) with the exit code and cause.

**Providers:** before the state lock and before any data plugin, `run`
(including `--dry-run` and `--print-resolved-config`) starts the
[provider instances](pipeline-yaml.md#providers) the pipeline's `secret()` calls
reference and resolves every value. A provider failure stops the command right
there with exit `3` (a missing provider binary: `4`), for example:

```
error: configuration error: provider 'creds' could not resolve 'db.json#password': NOT_FOUND: no such file: db.json
```

Every resolved value is masked in everything the engine prints — logs, plugin
stderr, error lines, the run summary (see
[Masking](pipeline-yaml.md#masking)).

**Signals:** SIGINT (Ctrl+C) and SIGTERM stop a run gracefully: the engine
sends `cancel` to every plugin (a destination rolls back), waits up to
`pipeline.timeouts.cancel_grace_seconds` (default 30), kills what is left,
releases the state lock, prints the summary with `Status: cancelled`, and exits
`130` or `143`. The plugins run in their own process groups, so a terminal's
Ctrl+C reaches only the engine. The handlers are explicit because the engine is
PID 1 in the container images, where an unhandled SIGTERM would be ignored.
Keep the grace period below your orchestrator's (see
[`pipeline.timeouts`](pipeline-yaml.md#pipelinetimeouts)).

**Output:**

- **stdout** — the human report: the header (`loadsmith <version>`, then the
  route) and the summary box. Suitable for piping to a file. Its layout is not
  a stable interface; for automation use the
  [run summary](#machine-readable-run-summary). With `--output json`, stdout
  carries only that JSON document.
- **stderr** — tracing output at the configured log level, including the schema
  and the progress lines at doubling batch intervals (timestamped `INFO`
  records). Includes the full protocol handshake at `--log-level debug`.

### Machine-readable run summary

`--output json` replaces the human report on stdout with **one JSON document
on a single line**, printed when the command ends; `--summary-file <path>`
writes the same document (pretty-printed, atomically through a temporary file
renamed into place) whatever `--output` says. Logs, progress and the `error:`
line stay on stderr. Use `--output json` when a scheduler or a container log
captures stdout, and `--summary-file` to keep the human report on stdout and
hand the document to an orchestrator as a file.

The document is produced for **every run that got past argument parsing**:
successes, run failures, cancellations by a signal, dry runs, and setup
failures that print only an `error:` line in human mode (an unreadable or
invalid pipeline file, a missing or incompatible plugin, a rejected config, a
held state lock, schema drift). Usage errors (exit `2`) come from the argument
parser before anything runs and produce no document. `--output json` and
`--summary-file` cannot be combined with `--print-resolved-config`.

```json
{
  "version": 1,
  "loadsmith_version": "0.3.0",
  "pipeline": "orders-incremental",
  "status": "success",
  "exit_code": 0,
  "cause": null,
  "signal": null,
  "dry_run": false,
  "route": [
    {"kind": "source", "type": "postgres", "name": "loadsmith-source-postgres",
     "version": "0.2.1", "path": "/home/u/.loadsmith/plugins/loadsmith-source-postgres"},
    {"kind": "destination", "type": "jsonl", "name": "loadsmith-destination-jsonl",
     "version": "0.2.1", "path": "/home/u/.loadsmith/plugins/loadsmith-destination-jsonl"}
  ],
  "rows_read": 100000,
  "rows_written": 100000,
  "batches": 50,
  "duration_ms": 3137,
  "throughput_rows_per_sec": 31877,
  "started_at": "2026-10-20T14:03:08.398Z",
  "finished_at": "2026-10-20T14:03:11.535Z",
  "state": {"watermark_before": "2026-10-19T00:00:00Z", "watermark_after": "2026-10-20T14:00:00Z"},
  "sink": null
}
```

A setup failure, for example a held state lock:

```json
{"version":1,"loadsmith_version":"0.3.0","pipeline":"orders-incremental","status":"error","exit_code":5,"cause":"state error: pipeline 'orders-incremental' is locked by a running process (lease renewed 812ms ago); another run is in progress against state file 'state.json'","signal":null,"dry_run":false,"route":[{"kind":"source","type":"postgres","name":null,"version":null,"path":null},{"kind":"destination","type":"jsonl","name":null,"version":null,"path":null}],"rows_read":0,"rows_written":0,"batches":0,"duration_ms":3,"throughput_rows_per_sec":0,"started_at":"2026-10-20T14:03:08.398Z","finished_at":"2026-10-20T14:03:08.401Z","state":{"watermark_before":null,"watermark_after":null},"sink":null}
```

**Fields (version 1).** Every field is always present; `null` means unknown or
not applicable.

| Field | Type | Meaning |
|---|---|---|
| `version` | integer | Version of this document's schema. Currently `1` |
| `loadsmith_version` | string | Version of the engine that ran |
| `pipeline` | string \| null | `pipeline.name`; `null` if the pipeline file could not be read or parsed |
| `status` | string | `success`, `error` or `cancelled` (stopped by SIGINT/SIGTERM) |
| `exit_code` | integer | The process exit code (see [Exit codes](#exit-codes)) |
| `cause` | string \| null | The root cause when the run did not succeed (the summary's `Cause:` line, or the `error:` line of a setup failure), with every resolved template value masked. A provider failure is a setup failure: `pipeline` and `route` are still known (the static check runs first), `exit_code` is `3`, and `cause` reads e.g. `configuration error: provider 'creds' could not resolve 'db.json#password': NOT_FOUND: …` |
| `signal` | string \| null | `SIGINT` or `SIGTERM` when a signal stopped the run |
| `dry_run` | bool | The run was `--dry-run` (nothing was executed) |
| `route` | array | One entry per stage, in order: `kind` (`source`, `parser`, `destination`, `sink`; a `parser` entry only with a `parser:` block), `type` (the `type:` in `pipeline.yaml`), `name` and `version` (from the plugin's handshake), `path` (the binary that was resolved). `name`/`version`/`path` are `null` until the run learned them; `route` is `[]` if the pipeline file could not be parsed. Provider instances are not listed |
| `rows_read` | integer | Rows the source produced |
| `rows_written` | integer | Rows the destination reported written |
| `batches` | integer | Batches the source produced |
| `duration_ms` | integer | Wall-clock time of the command, `finished_at − started_at` |
| `throughput_rows_per_sec` | integer | `rows_read` per second of `duration_ms` (`0` for an instant run) |
| `started_at`, `finished_at` | string | UTC timestamps, RFC 3339 with milliseconds (`2026-10-20T14:03:08.398Z`) |
| `state` | object \| null | With a `state:` block: `watermark_before` (the cursor loaded at start, `null` if none) and `watermark_after` (the cursor in the state after the run: the one this run persisted, else unchanged). The values are the opaque cursors, verbatim. `null` without a `state:` block |
| `sink` | object \| null | With a `sink:` block: `objects_delivered` (`null` if delivery did not complete). `null` without a sink |

If `--summary-file` cannot be written, the error is reported on stderr and a
run that otherwise succeeded exits `1`.

The document's schema is covered by the [Stability Policy](stability.md):
within a `version`, fields are only ever added, never removed or changed in
meaning. Consumers should ignore fields they do not know.

**Examples:**

Run a pipeline using plugins from a custom directory:
```bash
./target/debug/loadsmith run pipeline.yaml --plugin-dir /opt/loadsmith/plugins
```

Resolve everything (providers included) without starting a data plugin:
```bash
loadsmith run pipeline.yaml --dry-run
```
(For a check with no side effects at all, use
[`loadsmith validate`](#loadsmith-validate-pipeline).)

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

Validate a pipeline file. **Static by default:** it evaluates nothing — no
environment variable or file is read, no plugin or provider is started, no
system is contacted. `--resolve` adds the full resolution `run` performs before
it starts anything, providers included. Plugin-specific config is only checked
by the plugin itself at run time.

```bash
loadsmith validate pipeline.yaml                      # static (CI-friendly)
loadsmith validate pipeline.yaml --resolve            # also env()/file() and providers
loadsmith validate pipeline.yaml --resolve --plugin-dir /opt/loadsmith/plugins
```

**Arguments:**

| Argument | Description |
|---|---|
| `<pipeline>` | Path to the pipeline YAML file |

**Flags:**

| Flag | Default | Description |
|---|---|---|
| `--resolve` | off | Also resolve every template: read the `env()`/`file()` values and run the provider instances the `secret()` calls reference |
| `--plugin-dir <path>` | `~/.loadsmith/plugins/` | Where `--resolve` finds provider binaries |

**What is checked (always):**
1. YAML syntax is valid
2. The core-owned keys are strict: an unknown key at the top level, under
   `pipeline:`, `providers:`, `state:`, or beside a plugin's `type`/`config` is
   rejected
3. `pipeline.name`, `source.type`, `destination.type`, (if present)
   `sink.type`/`parser.type` and every `providers.<name>.type` are non-empty;
   provider instance names use letters, digits, `_` and `-`
4. With a `state:` block, `state.backend` is `local` and `state.path` is
   non-empty; a `parser:` is not combined with `state:`
5. Every template parses (`env('…')`, `file('…')`, `secret('…', '…')`), every
   `secret()` names a declared provider instance, and none sits in `pipeline:`,
   `state:` or `providers:`

An unset environment variable passes the static check. **With `--resolve`:**
every `env()`/`file()` is evaluated and every `secret()` is resolved by its
provider — the same failures, with the same exit codes, as `run` (see
[Exit codes](#exit-codes)).

**Output:**
```
Pipeline 'orders' is valid (static check; --resolve also resolves env()/file() and runs the providers).
Pipeline 'orders' is valid (fully resolved; 2 secret() references resolved).
```

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
| `--kind <kind>` | — | Install only this kind from the package (`source`/`destination`/`sink`/`parser`/`provider`). Same as the `:<kind>` spec suffix. |
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
`--all` always pulls each package's `latest`. With no name, a package this
loadsmith cannot use — its manifest doesn't validate (e.g. a plugin kind this
version doesn't know) or it targets no supported protocol version — is skipped
with a warning instead of failing the whole install; download and checksum
errors still fail it. Naming the package explicitly reports the error.

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
Format:       v2
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

Both read only what locates the state: `env()`/`file()` are resolved in
`pipeline:` and `state:` alone, and no provider is ever started (`secret()` is
not allowed in those blocks). The other blocks' templates — database
credentials, `secret()` calls — need not resolve.

---

## Exit codes

Every `loadsmith` command exits with one of these codes. The table is a covered
interface: it is frozen at 1.0 (see the [Stability Policy](stability.md)), and
a new code can only be added in a minor release for a case that today falls
under `1`.

| Code | Meaning | Examples |
|---|---|---|
| `0` | Success | `run`: summary `Status: success` |
| `1` | The run failed | A plugin reported an error, crashed, broke the protocol or timed out; the data plane stalled; sink delivery or state persistence failed; any failure not listed below |
| `2` | Usage error | Unknown flag or missing argument (from the argument parser); conflicting selectors such as `plugin install mysql:source --kind destination` |
| `3` | Configuration error | Unreadable pipeline file, invalid YAML, unknown key, failed template, validation error, a `secret()` naming an undeclared provider instance or used in `pipeline:`/`state:`/`providers:`, a provider failure (a reference it could not resolve, a crash, a protocol violation, a timeout), a plugin or provider rejected its `config` (`configure_ack` error), a capability gate (a `sink:` on a destination without `object_output`, a `state:` block with a non-incremental source, a `parser:` without a raw source or a raw source without a `parser:`), `state show` on a pipeline with no `state:` block |
| `4` | Plugin unavailable | Plugin binary (provider binaries included) not found, cannot be started, no protocol version in common, wrong kind, a provider without the `resolve` capability, a parser without the `parse` capability |
| `5` | State error | The state lock is held by another run, the state file is unreadable or from a newer engine, the source schema drifted (`on_schema_change: refuse`) |
| `130` | Interrupted (SIGINT) | Ctrl+C, after cancelling the plugins (also while providers are resolving) |
| `143` | Terminated (SIGTERM) | `docker stop`, a Kubernetes pod shutdown, after cancelling the plugins |

`130` and `143` follow the shell convention of 128 + the signal number.
`plugin install`/`uninstall` failures other than usage errors (a download, a
checksum, a manifest whose `protocol` range this engine does not support) exit
`1`.

## Environment variables

| Variable | Equivalent to | Description |
|---|---|---|
| `NO_COLOR` | `--no-color` | Disable ANSI color (when set, to any value — even empty) |
| Any variable in `{{ env('NAME') }}` | — | Injected into pipeline template resolution |

Either `--no-color` or `NO_COLOR` disables color. The plugin directory has no
environment variable: use `--plugin-dir`.
