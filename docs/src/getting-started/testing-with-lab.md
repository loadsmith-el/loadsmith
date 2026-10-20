# Testing with loadsmith-lab

Unit tests cover individual crates in isolation. The real validation — running
loadsmith against an actual seeded database and asserting the output — requires a
separate harness that can spin up real Docker services. That harness is
**loadsmith-lab**.

## What loadsmith-lab is

loadsmith-lab is a sibling repository at `../loadsmith-lab` (relative to the
loadsmith checkout). It is a Rust workspace that:

1. Reads declarative **case files** (`case.yaml`) that describe a test scenario.
2. Spins up Docker services (databases, file servers, …) needed by the case.
3. Runs the loadsmith binary against those services.
4. Validates the output against expected row counts and file contents.
5. Prints a structured pass/fail report.

Each case is self-contained: its own Docker network, its own output directory,
torn down and cleaned up after the run regardless of success or failure.

## Prerequisites

- Docker (the Docker daemon must be running)
- loadsmith-lab built: `cargo build` in the loadsmith-lab repo

The loadsmith core does not need a host build: `--loadsmith <project dir>` builds
it inside Docker, and without `--loadsmith` the lab pulls a published image.

## Repository layout

The lab itself is only the harness; cases, service images and seed data live in
sibling repos that the lab fetches as **origins**:

```
loadsmith-lab/                          ← the harness (this is what you build)
  crates/
    loadsmith-lab-cli/
    loadsmith-lab-runner/
    loadsmith-lab-docker/
    loadsmith-lab-report/
  docs/

loadsmith-lab-canonical-catalog/        ← origin `catalog`: cases and bundles
  cases/
    postgres-to-jsonl/
      case.yaml
      pipeline.yaml
    …
  bundles/

loadsmith-lab-canonical-images/         ← origin `images`: service images
  images/
    lab-postgres-15/                    ← Dockerfile + init.sql
    lab-mysql-8/
    lab-oracle/
    lab-mock-graph/

loadsmith-lab-canonical-data/           ← the seed-data generator
  generate.py                           ← stdlib-only, seed=42
```

Cases are selected as `<origin>/<name>`, e.g. `catalog/postgres-to-jsonl`.

## Running a case

```bash
cd ../loadsmith-lab
cargo build
./target/debug/loadsmith-lab run --loadsmith ../loadsmith --select catalog/postgres-to-jsonl
```

`--loadsmith <path>` runs a local core — a project dir (built hermetically in
Docker, as above) or a prebuilt binary. Without it, the lab pulls the published
`ghcr.io/loadsmith-el/loadsmith:slim` image; `--tag` picks another published tag
(e.g. `--tag v0.2.0-static`). You can likewise test a local plugin with
`--plugin <binary|project>` (a plugin project is built as a static musl binary in a `rust:1-alpine`
container, like a release).

## How a case is declared

`cases/postgres-to-jsonl/case.yaml` in `loadsmith-lab-canonical-catalog`:

```yaml
case:
  name: postgres-to-jsonl
  description: Reads 100k rows from PostgreSQL and writes them to JSONL
  tags: [postgres, jsonl, smoke]

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

pipeline:
  file: pipeline.yaml

expect:
  status: success
  rows_read: 100000
  rows_written: 100000
  output:
    file: events.jsonl   # validated under the host side of the /output mount
    row_count: 100000
```

### Key fields

**`services`** — a list of Docker services to start before running loadsmith.
Each service gets a hostname alias (`pg`) that the pipeline can use; loadsmith
runs on the same Docker network and reaches services by that alias.

**`services[].image`** — a service image as `<origin>/<name>`; here the
`lab-postgres-15` directory of the `images` origin.

**`readiness.tcp`** — the port to wait for. The lab polls `TcpStream::connect`
every 500 ms until the port is open or the timeout expires.

**`readiness.postgres.probe_query`** — an additional readiness check. After the
TCP port is open, the lab connects to Postgres and runs this query, retrying
until it returns at least one row. This is necessary because `COPY` in Postgres
is transactional: the TCP port opens long before the 100,000-row import commits.
Without this probe, loadsmith could start before the table has data.

**`expect`** — the assertions. The lab checks:
- `status: success` — loadsmith exited with code 0
- `rows_read: 100000` — the summary box said "Rows read: 100,000"
- `rows_written: 100000` — the summary box said "Rows written: 100,000"
- `output.file` / `output.row_count` — the output file exists and has the right
  number of lines (`file` is relative to the run's `/output` mount)

An optional `loadsmith:` block can set `loadsmith.image` to a full image
reference, overriding the core image chosen by `--loadsmith` / `--tag`.

## Image resolution

When a case references `images/lab-postgres-15`, the lab tags it locally as
`loadsmith-lab/images/lab-postgres-15:local` and tries in order:

1. **Local Docker cache** — if the image already exists locally, use it.
2. **Pull** — try to pull that tag (a local-only tag fails and falls through).
3. **Build from the origin's Dockerfile** — build
   `images/lab-postgres-15/Dockerfile` from the `images` origin.

The postgres Dockerfile generates its own seed data: a build stage clones
`loadsmith-lab-canonical-data` at a pinned `DATA_REF` and runs `generate.py`, so
nothing is copied from your checkout. The same images are also published to GHCR
by the images repo's CI.

On first run, the image is built automatically. This takes a few minutes as it
loads 100,000 rows into Postgres. Subsequent runs use the cached image and start
in seconds.

## The canonical test dataset

`spacecraft_telemetry_events.csv` (generated by `loadsmith-lab-canonical-data`,
never committed) contains 100,000 rows of synthetic
spacecraft telemetry with 34 columns covering all Arrow-supported types:
integers, bigints, doubles, booleans, decimals (as strings), dates, timestamps,
times, and nullable variants of each.

The dataset is deterministic (seed=42) and covers null handling, numeric
precision, and timestamp parsing — the hardest parts of any EL tool.

To regenerate it:

```bash
cd loadsmith-lab-canonical-data
python generate.py     # stdlib only, no pip install
```

## Report output

A passing run looks like:

```
loadsmith-lab 0.1.0  ·  local loadsmith

▶ catalog/postgres-to-jsonl  Reads 100k rows from PostgreSQL and writes them to JSONL
  · waiting for pg at 172.18.0.2:5432…
  · pg ready (2.0s)
  │ loadsmith 0.2.0
  │ postgres → jsonl
  │
  │ 2026-10-16T12:00:03.112408Z  INFO loadsmith_core::reporter: schema negotiated — 34 columns
  │ ...
  │ 2026-10-16T12:00:04.201733Z  INFO loadsmith_core::reporter: 2,000 rows · 1 batch
  │ 2026-10-16T12:00:04.233019Z  INFO loadsmith_core::reporter: 4,000 rows · 2 batches
  │ ...
  │ 2026-10-16T12:01:17.950274Z  INFO loadsmith_core::reporter: 100,000 rows · 50 batches
  │
  │ ───────────────────────────────────────────
  │ Pipeline:     postgres-to-jsonl-smoke
  │ Route:        loadsmith-source-postgres → loadsmith-destination-jsonl
  │ Status:       success
  │ Rows read:    100,000
  │ Rows written: 100,000
  │ Batches:      50
  │ Duration:     00:01:14.012
  │ Throughput:   1,351 rows/s
  │ ───────────────────────────────────────────
  ✓ postgres-to-jsonl   100,000 read · 100,000 written   74.2s

──────────────────────────────────────────────────
1 passed, 0 failed
```

See the loadsmith-lab book's *Understanding the Output* page for every line.
