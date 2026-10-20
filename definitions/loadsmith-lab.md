## Lab platform: `loadsmith-lab`

> This is the original design note for the lab (first sketched under the working
> name `ls-lab`). It has been updated to match what shipped; the reference for the
> real CLI, case and bundle formats is the
> [loadsmith-lab](https://github.com/loadsmith-el/loadsmith-lab) repository and its
> docs (`docs/src/reference/cli.md`, `case-yaml-schema.md`,
> `bundle-yaml-schema.md`). Items marked *not implemented* are ideas only.

Loadsmith has an auxiliary platform called **`loadsmith-lab`**.

`loadsmith-lab` is a project separate from Loadsmith's main runtime, in its own
repository. It is not part of normal pipeline execution in production. Its job is
to act as a laboratory, integration and release-validation platform.

The main goal of `loadsmith-lab` is to validate a specific Loadsmith version against real or simulated scenarios, including external dependencies, plugins, databases, files, protocol, events, state and expected outputs.

---

## The role of `loadsmith-lab`

`loadsmith-lab` is a **release-validation orchestrator**.

It can:

* run a published Loadsmith image, picked by tag;
* use local code in development mode (a local core or a local plugin);
* build the core and plugins when needed;
* build or reuse Docker images;
* start test dependencies in containers;
* run validation pipelines;
* capture the run's output;
* validate the generated outputs;
* compare against expected results;
* produce a final run report.

Conceptual example:

```text
loadsmith-lab
  ├── selects the Loadsmith image tag (or a local core)
  ├── builds the core/plugins from a local checkout, or uses a published image
  ├── builds service images if needed
  ├── starts the scenario's dependencies
  ├── runs the test cases
  ├── collects the run output
  ├── collects the generated outputs
  ├── compares them with the expected outputs
  └── produces a validation report
```

---

## Running by tag

`loadsmith-lab` can validate a specific Loadsmith version. `--tag` picks the tag of
the published image `ghcr.io/loadsmith-el/loadsmith:<tag>` (default `slim`):

```bash
loadsmith-lab run --tag v0.2.0-static --select catalog/oracle-to-jsonl
```

Behavior:

```text
1. Pulls ghcr.io/loadsmith-el/loadsmith:v0.2.0-static.
2. Resolves the selected case from its origin.
3. Builds or reuses the service images the case needs.
4. Starts the environment for the case.
5. Runs the case.
6. Validates status, row counts and outputs.
7. Produces the final report.
```

Using a tag makes release validation reproducible. (Resolving a source tag and
compiling it, as first sketched, is not implemented.)

---

## Running a local core or plugin

Instead of a published image, `--loadsmith <PATH>` runs a local core — a binary,
or a project directory built hermetically in Docker from its own `Dockerfile`:

```bash
loadsmith-lab run --loadsmith ../loadsmith --select catalog/postgres-to-jsonl
```

`--plugin <PATH>` (repeatable) overrides a cached canonical plugin with a local
one — a binary, a plugin crate or a workspace root; a crate is built as a static
musl binary in a `rust:1-alpine` container, like a release:

```bash
loadsmith-lab run --select catalog/mysql-to-jsonl --plugin ../loadsmith-canonical-plugins/mysql
```

`loadsmith-lab` reuses existing images whenever possible. Rule:

```text
If the required image already exists locally, loadsmith-lab does not rebuild it.
```

*Not implemented:* `--image <ref>`, `--rebuild` and `--no-cache` flags. To force a
service-image rebuild, remove the local image (`loadsmith-lab/<origin>/<name>:local`)
first — `loadsmith-lab build` also reuses an image that already exists.

---

## Running every case

`--all` runs every available case:

```bash
loadsmith-lab run --all
loadsmith-lab run --tag v0.2.0-slim --all
```

Behavior:

```text
1. Discovers every lab case in the configured origins.
2. Resolves the dependencies they need.
3. Starts the required containers.
4. Runs every case.
5. Produces a consolidated report.
```

`--all` is meant for broad validation before a release.

---

## Selecting cases

Besides `--all`, cases are selected one at a time as `<origin>/<name>`:

```bash
loadsmith-lab run --select catalog/oracle-to-jsonl
loadsmith-lab run --select catalog/postgres-to-parquet-single
loadsmith-lab list                       # list the available cases
```

Cases come from **origins** — the default ones are `catalog`
(`loadsmith-lab-canonical-catalog`: cases and bundles) and `images`
(`loadsmith-lab-canonical-images`: service images). `--cases-dir <dir>` runs
ad-hoc cases from a directory, bypassing origins.

**Bundles** sequence cases with setup/validate/cleanup hooks:

```bash
loadsmith-lab bundle run --select catalog/parquet-destination
loadsmith-lab bundle list
```

The other subcommands: `build` (build service images, e.g.
`--select images/lab-postgres-15`), `origin` (manage origins), and
`install` / `uninstall` (items or whole origins).

Possible future filters (*not implemented*): by tag (`smoke`, `regression`,
`failure`), by plugin kind (`source:jdbc`, `destination:s3_parquet`).

---

## Running with real dependencies

`loadsmith-lab` starts real dependencies in containers to validate plugins.

Service images in `loadsmith-lab-canonical-images` today: `lab-postgres-15`,
`lab-mysql-8`, `lab-oracle` and `lab-mock-graph` (a fake Microsoft Graph for the
SharePoint source). Other candidates:

```text
MinIO/local S3
Azurite for Azure Blob Storage
fake HTTP server
fake OAuth server
```

Example for Oracle:

```bash
loadsmith-lab run --select catalog/oracle-to-jsonl
```

Flow:

```text
1. Starts the Oracle container.
2. Waits for readiness.
3. The image's own init scripts create the tables and seed the test data.
4. Runs the Loadsmith pipeline.
5. Validates rows read/written.
6. Validates the output.
7. Tears the environment down.
```

---

## Build and cache

`loadsmith-lab` has smart build/cache behavior.

Goals:

* avoid unnecessary rebuilds;
* speed up local runs;
* allow reproducibility for releases;
* tell a local image, a tagged image and a local-code build apart.

Behavior:

```text
Service images:
  Local Docker cache → pull → build from the origin's Dockerfile.

With --tag:
  Uses the published image for that tag.

With --loadsmith:
  Uses (or builds) the local core.

Neither:
  Uses ghcr.io/loadsmith-el/loadsmith:slim.
```

---

## Test cases

`loadsmith-lab` should support scenarios such as:

```text
happy path
  The pipeline runs successfully.

plugin failure
  A plugin fails while reading or writing.

invalid config
  An invalid configuration must fail at the right validation step.

protocol mismatch
  A plugin uses an incompatible protocol version.

schema mismatch
  The source produces a schema different from the expected one.

cancel execution
  The core cancels the run and the plugins exit cleanly.

checkpoint recovery
  The pipeline resumes from a previous state.

reject rows
  Invalid rows are diverted to rejects.

backpressure
  A slow destination must not blow up the core's/source's memory.

secret redaction
  No secret may leak into logs/events.

docker image execution
  The pipeline runs correctly inside the final Docker image.

tag compatibility
  A specific tag runs the expected suite.
```

---

## Case layout

Cases live in origin repositories, e.g. `loadsmith-lab-canonical-catalog`:

```text
loadsmith-lab-canonical-catalog/
  cases/
    postgres-to-jsonl/
      case.yaml
      pipeline.yaml
    oracle-to-jsonl/
      case.yaml
      pipeline.yaml
  bundles/
    parquet-destination/
    postgres-types/
    tls-spike/
```

A real `case.yaml` (`cases/postgres-to-jsonl/case.yaml`):

```yaml
case:
  name: postgres-to-jsonl
  description: Reads 100k rows from PostgreSQL and writes them to JSONL
  tags: [postgres, jsonl, smoke]

services:
  - image: images/lab-postgres-15      # <origin>/<name>
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
    file: events.jsonl   # under the run's /output mount
    row_count: 100000
```

The full schema is in loadsmith-lab's `docs/src/reference/case-yaml-schema.md`.
Event assertions (`expect.events`) and schema assertions, as first sketched, are
not implemented.

---

## Relationship with plugins

`loadsmith-lab` helps validate both official and community plugins.

`--plugin <PATH>` runs any case against a local plugin build. A plugin repository
could also publish its own lab suite as an origin (cases plus service images),
which gives the ecosystem a standard way to test compatibility.

---

## Relationship with releases

Before a release, `loadsmith-lab` should run a validation matrix.

Example:

```bash
loadsmith-lab run --tag v0.2.0-slim --all
```

Conceptual matrix:

```text
Core v0.2.0
  x postgres, mysql, oracle (source + destination)
  x sharepoint source
  x jsonl, null, parquet destinations, local-copy sink
  x TLS modes
  x simulated plugin failures
  x incompatible-protocol cases
  x state/checkpoint cases
```

The goal is to avoid breaking:

* the JSONL control-plane contract;
* the Arrow IPC data-plane contract;
* the handshake;
* capabilities;
* configuration validation;
* plugin lifecycle;
* clean shutdown;
* logs/events;
* state/checkpoints;
* Docker image compatibility;
* core/plugin compatibility.

---

## Relationship with observability

`loadsmith-lab` captures the output emitted by the core and the plugins.

Possible validations:

```text
- must report the schema before the first batch
- must report progress at doubling batch intervals
- must print the final summary
- must mask secrets in logs
- must shut plugins down correctly on error
```

---

## Non-goals of `loadsmith-lab`

`loadsmith-lab` must not be:

* a production scheduler;
* an Airflow-style orchestrator;
* a replacement for Step Functions;
* a distributed runtime;
* a mandatory tool to run simple pipelines;
* a production dependency of the core.

It is a platform for development, testing, validation and compatibility certification.

---

## Project structure

`loadsmith-lab` lives in its own repository; cases, images and seed data live in
sibling repositories that the lab fetches as origins.

```text
loadsmith/                          (the engine — see definitions/loadsmith.md §21)
  crates/
    loadsmith-protocol/
    loadsmith-transport/
    loadsmith-arrow/
    loadsmith-plugin-sdk/
    loadsmith-config/
    loadsmith-plugin-manifest/
    loadsmith-core/
    loadsmith-cli/
    loadsmith-tls/

loadsmith-canonical-plugins/        (file, jsonl, null, parquet, local-copy,
                                     mysql, sharepoint, postgres, oracle)

loadsmith-lab/
  crates/
    loadsmith-lab-cli/
    loadsmith-lab-runner/
    loadsmith-lab-docker/
    loadsmith-lab-report/
  docs/

loadsmith-lab-canonical-catalog/    (origin `catalog`: cases/, bundles/)
loadsmith-lab-canonical-images/     (origin `images`: images/lab-postgres-15, …)
loadsmith-lab-canonical-data/       (generate.py — the canonical seed dataset)
```

---

## MVP (delivered)

* the `loadsmith-lab` repository and crates;
* `loadsmith-lab run --select <origin>/<case>`;
* `loadsmith-lab run --all`;
* image selection by tag with `--tag`;
* running against Docker images;
* image cache/reuse when possible;
* local core and plugin overrides (`--loadsmith`, `--plugin`);
* cases with containerized dependencies;
* validation of final status, row counts and simple outputs;
* bundles with setup/validate/cleanup hooks.

Examples:

```bash
loadsmith-lab run --select catalog/postgres-to-jsonl
loadsmith-lab run --select catalog/oracle-to-jsonl --tag v0.2.0-static
loadsmith-lab run --all
loadsmith-lab bundle run --select catalog/tls-spike
```

---

## Settled decisions

* The ecosystem has an auxiliary platform called **`loadsmith-lab`** (the
  `ls-lab` working name was dropped).
* `loadsmith-lab` lives in its own repository, separate from the engine.
* `loadsmith-lab` is used as a laboratory and release-validation orchestrator.
* `--tag` picks the tag of the published image `ghcr.io/loadsmith-el/loadsmith`
  (default `slim`).
* `loadsmith-lab` can run Docker images, and can build a local core
  (`--loadsmith`) or local plugins (`--plugin`) when needed.
* `loadsmith-lab` avoids unnecessary rebuilds by default.
* `--all` runs every available case; `--select <origin>/<name>` runs one.
* Cases and bundles are YAML files (`case.yaml`, `bundle.yaml`) in origin
  repositories; service images live in their own origin.
* The first matrix covers PostgreSQL, MySQL, Oracle and SharePoint (via a mock
  Graph), plus TLS.
* `loadsmith-lab` is not a production runtime.
* `loadsmith-lab` validates the core, plugins, protocol, failures, outputs and
  compatibility.
* `loadsmith-lab` helps certify official and community plugins.

---

## Open decisions

* Define how to compare Arrow/Parquet outputs beyond row counts.
* Define how to simulate plugin failures.
* Define how to version compatibility suites.
* Define whether community plugins can publish lab suites (as origins) alongside
  the plugin.
* Define a forced-rebuild option for service images.
