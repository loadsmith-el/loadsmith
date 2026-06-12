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
- loadsmith built: `cargo build` in the loadsmith repo
- loadsmith-lab built: `cargo build` in the loadsmith-lab repo

## Repository layout

```
loadsmith-lab/
  data/
    spacecraft_telemetry_events.csv    ← 100,000 rows, seed=42
    generate/
      generate.py                      ← regenerate with Python
      requirements.txt
  images/
    lab-postgres-15/
      Dockerfile
      init.sql
  cases/
    postgres-to-jsonl/
      case.yaml
      pipeline.yaml
  crates/
    loadsmith-lab-cli/
    loadsmith-lab-runner/
    loadsmith-lab-docker/
    loadsmith-lab-report/
```

## Running a case

```bash
# from the loadsmith repo first:
cd ../loadsmith && cargo build

# then run the lab:
cd ../loadsmith-lab
cargo build
./target/debug/loadsmith-lab run --loadsmith ../loadsmith --select catalog/postgres-to-jsonl
```

`--loadsmith <path>` runs a local core — a project dir (built hermetically in
Docker, as above) or a prebuilt binary. Without it, the lab uses a published
image. You can likewise test a local plugin with
`--plugin <binary|project>` (a project is built in a `rust:bookworm` container).

## How a case is declared

`cases/postgres-to-jsonl/case.yaml`:

```yaml
case:
  name: postgres-to-jsonl
  description: "Read 100k rows from PostgreSQL and write to JSONL"
  tags: [postgres, jsonl, smoke]

services:
  - image: loadsmith-lab-postgres:15
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
  image: local
  volumes:
    - host: /tmp/ls-lab-output
      container: /output

pipeline:
  file: pipeline.yaml

expect:
  status: success
  rows_read: 100000
  rows_written: 100000
  output:
    file: /tmp/ls-lab-output/events.jsonl
    row_count: 100000
```

### Key fields

**`services`** — a list of Docker services to start before running loadsmith.
Each service gets a hostname alias (`pg`) that the pipeline can use; loadsmith
runs on the same Docker network and reaches services by that alias.

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
  number of lines

## Image resolution

When the lab needs an image named `loadsmith-lab-postgres:15`, it tries in order:

1. **Local Docker cache** — if the image already exists locally, use it.
2. **Pull from registry** — try `docker pull loadsmith-lab-postgres:15`.
3. **Build from Dockerfile** — look for `images/lab-postgres-15/Dockerfile` and
   build it. The build context always includes `data/spacecraft_telemetry_events.csv`.

On first run, the `loadsmith-lab-postgres:15` image will be built automatically.
This takes 2–3 minutes as it loads 100,000 rows into Postgres. Subsequent runs
use the cached image and start in seconds.

## The canonical test dataset

`data/spacecraft_telemetry_events.csv` contains 100,000 rows of synthetic
spacecraft telemetry with 34 columns covering all Arrow-supported types:
integers, bigints, doubles, booleans, decimals (as strings), dates, timestamps,
times, and nullable variants of each.

The dataset is deterministic (seed=42) and covers null handling, numeric
precision, and timestamp parsing — the hardest parts of any EL tool.

To regenerate it:

```bash
cd loadsmith-lab/data/generate
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python generate.py
```

## Report output

A passing run looks like:

```
loadsmith-lab v0.1.0   local mode

postgres-to-jsonl
  "Read 100k rows from PostgreSQL and write to JSONL"
  │
  │ Loadsmith v0.1.0  ·  postgres → jsonl
  │
  │   batch   1    2,000 rows
  │   batch   2    4,000 rows
  │   ...
  │
  │ ─────────────────────────────────────────
  │ Pipeline:     postgres-to-jsonl-smoke
  │ Status:       success
  │ Rows read:    100,000
  │ Rows written: 100,000
  │ Duration:     0:01:12
  │
  ✓ passed   100,000 rows read · 100,000 written

────────────────────────────────────────────
1 passed, 0 failed
```
