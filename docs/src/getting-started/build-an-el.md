# Build an EL, End to End

[Your First Pipeline](./first-pipeline.md) runs a pipeline from a host build.
This guide goes all the way to production: one pipeline, packaged as a container
image that carries only the plugins it needs, and tested against a real database
with loadsmith-lab before you ship it.

The example is a typical first EL: an **incremental** export of a PostgreSQL
table to JSONL files. Each run reads only the rows changed since the previous
run and writes them to a new file.

By the end you'll have a small project directory:

```
telemetry-el/
  pipeline.yaml                         ← what to move (Step 1)
  Dockerfile                            ← the image that runs it (Step 3)
  lab/                                  ← a loadsmith-lab origin (Step 5)
    loadsmith-lab.toml
    cases/
      telemetry-to-jsonl/
        case.yaml
```

The table is `spacecraft_telemetry_events`, the table of loadsmith-lab's
canonical dataset (100,000 rows, 34 columns, with an `updated_at` column). That
choice is deliberate: in Step 5 the lab spins up a Postgres seeded with exactly
this table, so the pipeline you test is the one you ship, unchanged.

## Prerequisites

- **Docker** with Buildx (bundled with current Docker releases). Nothing else is
  needed on the host for Steps 1–4: every `loadsmith` command below runs inside
  the published image.
- For Step 5, **loadsmith-lab** built from source (`cargo build` in a clone of
  [loadsmith-lab](https://github.com/loadsmith-el/loadsmith-lab)); see its
  [Prerequisites](https://loadsmith-el.github.io/loadsmith-lab/getting-started/prerequisites.html).

The guide pins the engine at **v0.2.0** and the plugins at the versions the
[canonical index](https://github.com/loadsmith-el/loadsmith-canonical-plugins/blob/main/index.json)
lists as latest for it: `postgres` 0.2.1 and `jsonl` 0.1.2. Use the current
versions when you follow along.

## Step 1 — Write the pipeline

`pipeline.yaml`:

```yaml
pipeline:
  name: telemetry-to-jsonl
  description: "Incremental export of spacecraft telemetry events to JSONL"

source:
  type: postgres
  config:
    host: "{{ env('PG_HOST') }}"
    port: 5432
    dbname: "{{ env('PG_DATABASE') }}"
    user: "{{ env('PG_USER') }}"
    password: "{{ env('PG_PASSWORD') }}"
    application_name: telemetry-el
    query: "SELECT * FROM spacecraft_telemetry_events"
    batch_size: 5000
    incremental:
      cursor_column: updated_at

destination:
  type: jsonl
  config:
    path: "{{ env('DATA_DIR') }}/telemetry-{{ env('RUN_ID') }}.jsonl"

state:
  backend: local
  path: "{{ env('DATA_DIR') }}/state/telemetry-to-jsonl.json"
```

What each part does:

- **Everything environment-specific is an `{{ env('...') }}` template**: the
  connection (including the password, which never lands in the file or the
  image), the data directory, and the run id. The same file then runs
  unchanged in production and in the lab, with different environment values.
  An unset variable fails the run before any plugin starts. Templates are
  covered in [Pipeline YAML](../reference/pipeline-yaml.md#template-syntax).
- **`incremental.cursor_column: updated_at`** makes the postgres source wrap the
  query as `… WHERE updated_at > <watermark> ORDER BY updated_at`, so leave
  `ORDER BY` out of `query`. With no state yet (and no `initial_value`), the first
  run reads the whole table. A column that changes on update, like `updated_at`,
  also picks up updated rows, not only new ones. See the
  [postgres config reference](https://loadsmith-el.github.io/loadsmith-canonical-plugins/config/postgres.html#incremental)
  for the cursor caveats.
- **`state:`** is required for incremental reads: it is where the core keeps the
  watermark between runs. It must sit on storage that outlives the container
  (Step 4). See [Incremental State](../architecture/incremental-state.md).
- **A new output file per run (`RUN_ID`).** The `jsonl` destination creates its
  file at the start of every run and overwrites an existing one. With a fixed
  `path`, each incremental run would replace the previous run's rows. A per-run
  name keeps every delta. (`parquet` behaves the same way: put the run id in its
  `prefix`.)

The fields inside each `config:` block belong to the plugin and are documented
with it: [postgres](https://loadsmith-el.github.io/loadsmith-canonical-plugins/config/postgres.html),
[jsonl](https://loadsmith-el.github.io/loadsmith-canonical-plugins/config/jsonl.html).

## Step 2 — Validate it

Run the checks in the slim core image, mounting the project directory. Set the
variables the templates need (placeholder values are fine here, nothing
connects to the database):

```bash
export PG_HOST=pg.internal PG_DATABASE=missions PG_USER=loadsmith_reader \
       PG_PASSWORD=change-me DATA_DIR=/data RUN_ID=20261020T0100Z

docker run --rm -v "$PWD":/work -w /work \
  -e PG_HOST -e PG_DATABASE -e PG_USER -e PG_PASSWORD -e DATA_DIR -e RUN_ID \
  ghcr.io/loadsmith-el/loadsmith:v0.2.0-slim validate pipeline.yaml
# Pipeline 'telemetry-to-jsonl' is valid.
```

`validate` checks the YAML, resolves the templates and checks the engine-level
structure (`pipeline`, `source`, `destination`, `state`). It does not look for
plugins or check what is inside a plugin's `config:` block: the plugin does
that when the pipeline runs. `run pipeline.yaml --dry-run` runs the same checks.

To see the config the plugins will receive, with defaults filled in, swap
`validate pipeline.yaml` for `run pipeline.yaml --print-resolved-config` in the
command above:

```yaml
pipeline:
  name: telemetry-to-jsonl
  description: Incremental export of spacecraft telemetry events to JSONL
  heartbeat_seconds: 30
source:
  type: postgres
  config:
    host: ***
    port: 5432
    dbname: ***
    user: ***
    password: ***
    application_name: telemetry-el
    query: SELECT * FROM spacecraft_telemetry_events
    batch_size: 5000
    incremental:
      cursor_column: updated_at
destination:
  type: jsonl
  config:
    path: ***/telemetry-***.jsonl
sink: null
state:
  backend: local
  path: ***/state/telemetry-to-jsonl.json
  on_schema_change: refuse
  checkpoint_interval: 0
```

Every value read through `env()` is masked, and the masking is a plain text
replacement over the whole output: wherever that value appears, it becomes
`***`. Here `DATA_DIR` (`/data`) is masked inside both paths. A short or common
value masks more than you might expect. With `PG_DATABASE=telemetry`, for
example, every occurrence of `telemetry` (the pipeline name, the table name,
the file names) would be masked too.

The [CLI Reference](../reference/cli.md) documents every flag.

## Step 3 — Build the image

The default image is the **slim** core, which has no plugins. Your image
installs just the plugins this pipeline uses, at pinned versions, and bakes in
the pipeline.

`Dockerfile`:

```dockerfile
FROM ghcr.io/loadsmith-el/loadsmith:v0.2.0-slim

# Only the plugins this pipeline uses, at pinned versions. Installing into
# /usr/local/bin (on PATH) makes them visible to any user the container runs as.
RUN ["loadsmith", "plugin", "install", "postgres:source@0.2.1", "--plugin-dir", "/usr/local/bin"]
RUN ["loadsmith", "plugin", "install", "jsonl@0.1.2", "--plugin-dir", "/usr/local/bin"]

COPY pipeline.yaml /etc/loadsmith/pipeline.yaml
ENV DATA_DIR=/data

# The base image's ENTRYPOINT is `loadsmith`.
CMD ["run", "/etc/loadsmith/pipeline.yaml"]
```

- **`postgres:source@0.2.1`** installs only `loadsmith-source-postgres`, not
  the postgres destination this pipeline doesn't use. `jsonl` provides a single
  plugin, so the bare package name is enough. The spec syntax
  (`<name>[:<kind>][@<version>]`) is in the
  [CLI Reference](../reference/cli.md#loadsmith-plugin-install-name). Pinning
  every version (engine tag and plugins) makes a rebuild produce the same image.
  `install` downloads the build for the image's architecture and verifies its
  sha256.
- **`--plugin-dir /usr/local/bin`.** By default `plugin install` writes to
  `~/.loadsmith/plugins`, which at build time is `/root/.loadsmith/plugins`.
  `/root` is not readable by other users, and a non-root user has a different
  `HOME` anyway. If the container runs as anyone but root (Kubernetes
  `runAsNonRoot`, `docker run --user`, loadsmith-lab), the core would not find
  those plugins. A binary that isn't in the plugin dir is looked up on `PATH`,
  so plugins in `/usr/local/bin` work for every user. The `-bundled` image puts
  its plugins in the same place.
- **`ENV DATA_DIR=/data`** gives the template a default, so a run only has to
  set it to write somewhere else. The other variables have no defaults: they
  are secrets or differ per environment.

Build it for both architectures:

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t telemetry-el:1.0.0 --load .
```

The plugin install runs once per platform. For `linux/arm64` on an amd64 host,
it runs under QEMU emulation, which Docker Desktop includes (on Linux, install
`qemu-user-static` or run `docker run --privileged --rm tonistiigi/binfmt --install arm64`).
`--load` keeps a multi-platform image in your local Docker only when Docker uses
the containerd image store. With the classic store, build one platform locally
(`--platform linux/amd64 --load`) and use `--push` for the multi-arch build
(see [Ship it](#ship-it)).

Check what went in:

```bash
docker run --rm telemetry-el:1.0.0 plugin list --plugin-dir /usr/local/bin
# Plugins in /usr/local/bin:
#   loadsmith-destination-jsonl
#   loadsmith-source-postgres
```

The [Container Images](./container-images.md) page explains the flavors. To
make the same image on the distroless `-static` flavor, change the `FROM` line
to `ghcr.io/loadsmith-el/loadsmith:v0.2.0-static`. The `RUN` lines already use
the exec form, which needs no shell.

## Step 4 — Run it

```bash
docker volume create telemetry-data

docker run --rm \
  -e PG_HOST=pg.internal -e PG_DATABASE=missions -e PG_USER=loadsmith_reader \
  -e PG_PASSWORD \
  -e RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)" \
  -e NO_COLOR=1 \
  -v telemetry-data:/data \
  telemetry-el:1.0.0
```

- **Secrets** come in as environment variables (`-e PG_PASSWORD` passes the
  value from your shell). On a scheduler, use its secret mechanism (a Kubernetes
  Secret, ECS task secrets, …) to set the same variables.
- **`/data`** holds both the output files and the state file, so it must be a
  volume that outlives the container. If it is lost, the next run has no
  watermark and reads the whole table again. To keep output and state apart,
  point the two paths at different variables or mounts.
- **`RUN_ID`** names this run's output file. A timestamp makes the files sort
  in run order.
- **`NO_COLOR=1`** turns off ANSI colors in the logs. The core prints them even
  without a terminal, which garbles log collectors.

The first run reads the whole table:

```
Pipeline:     telemetry-to-jsonl
Route:        loadsmith-source-postgres → loadsmith-destination-jsonl
Status:       success
Rows read:    100,000
Rows written: 100,000
```

Later runs read only the rows whose `updated_at` moved past the watermark. With
5 rows updated and 10 rows inserted in between, the next run reads 15 rows:

```
Rows read:    15
Rows written: 15
```

A run with nothing new reports `Rows read: 0` and leaves an empty file for that
`RUN_ID`. The exit code is `0` on success and non-zero on failure (`1` for a failed run;
the full table is in the [CLI reference](../reference/cli.md#exit-codes)), which is what
schedulers and retries key on.

**Inspecting and resetting the watermark.** `state show` and `state rm` read the
`state:` block of the pipeline, so they need the same environment and volume as
a run. The templates are resolved over the whole file, which means every
variable must be set, even the ones these commands don't use:

```bash
docker run --rm -e PG_HOST=x -e PG_DATABASE=x -e PG_USER=x -e PG_PASSWORD=x -e RUN_ID=x \
  -v telemetry-data:/data telemetry-el:1.0.0 state show /etc/loadsmith/pipeline.yaml
# Pipeline:     telemetry-to-jsonl
# Cursor value: "2026-10-20 11:33:11.710961"
# …
```

`state rm` instead of `state show` clears the watermark, so the next run is a
full read again (see the [CLI Reference](../reference/cli.md#loadsmith-state-show-pipeline)).

**Running as non-root.** The image needs no root. With a bind mount, run as
the user that owns the directory, and the files it writes are owned by that
user:

```bash
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/data":/data … telemetry-el:1.0.0
```

The `local` state backend takes a lock next to the state file
(`<path>.lock`), so two overlapping runs of the same pipeline can't corrupt the
watermark: the second one fails instead.

## Step 5 — Test it with loadsmith-lab

loadsmith-lab runs a pipeline against real, seeded services in Docker and
checks the result (see [Testing with loadsmith-lab](./testing-with-lab.md)). Its
canonical Postgres image is seeded with `spacecraft_telemetry_events`, so the
lab can run your pipeline, in your image, against a database where the right
answer is known: a first incremental run must read all 100,000 rows.

Lab cases come from **origins**. Make the project's `lab/` directory one by
giving it a manifest, `lab/loadsmith-lab.toml`:

```toml
[cases]
telemetry-to-jsonl = "First (full) incremental run of the telemetry EL, in the production image"
```

and the case, `lab/cases/telemetry-to-jsonl/case.yaml`:

```yaml
case:
  name: telemetry-to-jsonl
  description: First (full) incremental run of the telemetry EL, in the production image
  tags: [postgres, jsonl, incremental]

services:
  - image: images/lab-postgres-15
    alias: pg
    readiness:
      tcp: 5432
      timeout_seconds: 300
      postgres:
        dbname: lab
        user: lab
        password: lab
        probe_query: "SELECT 1 FROM spacecraft_telemetry_events LIMIT 1"

loadsmith:
  image: telemetry-el:1.0.0
  env:
    - PG_HOST=pg
    - PG_DATABASE=lab
    - PG_USER=lab
    - PG_PASSWORD=lab
    - DATA_DIR=/output
    - RUN_ID=lab

pipeline:
  file: ../../../pipeline.yaml

expect:
  status: success
  rows_read: 100000
  rows_written: 100000
  output:
    file: telemetry-lab.jsonl
    row_count: 100000
```

- **`pipeline.file: ../../../pipeline.yaml`** points at the project's own
  pipeline (the path is relative to the case directory). The lab tests the file
  you ship, not a copy.
- **`loadsmith.env`** supplies the variables the pipeline reads, just as your
  scheduler does in production. Inside the lab, the database is the service
  alias `pg` with the lab's credentials.
- **`DATA_DIR=/output`** sends both the output file and the state file to the
  directory the lab mounts for every run. The lab checks `expect.output.file`
  relative to that directory: `telemetry-lab.jsonl` must have 100,000 lines. The
  output directory is new for every run, so every lab run starts with no state
  and does a full first read.
- **`loadsmith.image: telemetry-el:1.0.0`** runs your image instead of the
  canonical one. The lab uses a local image if one exists and otherwise pulls
  it, so a registry reference works too, as long as the lab host can pull it.

Register the origin and run the case (from the project directory, with `lab`
pointing at your loadsmith-lab binary):

```bash
lab=../loadsmith-lab/target/debug/loadsmith-lab

$lab install images/lab-postgres-15       # only if `images` is a remote origin (the default)
$lab origin local add telemetry ./lab
$lab run --select telemetry/telemetry-to-jsonl
```

`install` copies the Postgres image's build context from the default `images`
origin. The first run then builds the image, which generates and loads the
dataset and takes a few minutes. Later runs reuse it. A local origin is read
live, so edits to the case or the pipeline need no reinstall.

```
▶ telemetry/telemetry-to-jsonl  First (full) incremental run of the telemetry EL, in the production image
  · waiting for pg at 172.18.0.2:5432…
  · pg ready (1.5s)
  │ loadsmith 0.2.0
  │ postgres → jsonl
  │ …
  │ Pipeline:     telemetry-to-jsonl
  │ Route:        loadsmith-source-postgres → loadsmith-destination-jsonl
  │ Status:       success
  │ Rows read:    100,000
  │ Rows written: 100,000
  │ …
  ✓ telemetry-to-jsonl   100,000 read · 100,000 written   2.9s

──────────────────────────────────────────────────
1 passed, 0 failed
```

The exit code is `0` only when every case passed, so the same command works as
a CI gate.

**What the lab does and doesn't cover:**

- **Do not pass `--tag` or `--loadsmith`.** Either one overrides the case's
  `loadsmith.image`, and the case would run the canonical image (or a local
  core) instead of yours.
- **The plugins come from the lab, not from your image.** The lab always mounts
  its plugin cache (the canonical set, installed once into
  `~/.cache/loadsmith-lab/plugins`) at `/plugins` and runs
  `loadsmith run … --plugin-dir /plugins`, and the plugin dir takes precedence
  over `PATH`. The run log shows it:
  `spawning plugins source="/plugins/loadsmith-source-postgres" …`. So the case
  tests your engine version, your pipeline and your environment against a real
  database, but with the cache's plugin versions. Those match the versions you
  pinned only if the cache was filled when they were the latest. Check the
  `configured plugin=… version=…` lines in the case output. To refresh the
  cache, delete it and the next run reinstalls the latest set (see
  [Run Modes](https://loadsmith-el.github.io/loadsmith-lab/architecture/run-modes.html#plugins--a-mounted-cache-with---plugin-overrides)).
- **The lab overrides the image's `CMD`.** It mounts the pipeline at
  `/case/pipeline.yaml` and runs it as the host user. Plugins installed in
  `/usr/local/bin` (Step 3) work for that user too.

## Ship it

Build the release for both architectures and push it to your registry in one
step:

```bash
docker buildx build --platform linux/amd64,linux/arm64 \
  -t registry.example.com/data/telemetry-el:1.0.0 --push .
```

A typical CI job for the project runs, in order: `loadsmith validate` (Step 2),
a single-platform `--load` build, the lab case (Step 5) against that tag, and
then the multi-arch `--push`. To upgrade the engine or a plugin, bump the
version in the `Dockerfile`, then rebuild and rerun the case.

## What's next: a custom image builder

Steps 3 and 5 are plain Docker and lab commands, so you write the Dockerfile and
the case yourself. The roadmap has a **custom image builder**: a `loadsmith`
subcommand that takes a manifest of the plugins you want, builds the slim
multi-arch image with only those, optionally validates it against relevant lab
cases, and pushes it to your registry. It is planned and does not exist yet;
until it ships, the steps in this guide are how you build the image.

## See also

- [Container Images](./container-images.md): the image flavors and their tags
- [Pipeline YAML](../reference/pipeline-yaml.md): the full pipeline schema,
  templates and `state:`
- [CLI Reference](../reference/cli.md): `run`, `validate`, `plugin install`,
  `state`
- [Incremental State](../architecture/incremental-state.md): how the watermark
  is persisted and its delivery guarantees
- [Plugin Configuration Reference](https://loadsmith-el.github.io/loadsmith-canonical-plugins/config/overview.html):
  every plugin's `config:` fields
- [loadsmith-lab: case.yaml](https://loadsmith-el.github.io/loadsmith-lab/writing-cases/case-yaml.html):
  every field of a lab case
