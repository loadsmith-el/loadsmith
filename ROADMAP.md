# Roadmap

What's shipped and what's queued next. Shipped items are documented in
[README.md](README.md) and [docs/](docs/src) — no details repeated here.

## Shipped

- [x] Core pump architecture — process-isolated plugins, three-plane protocol
      (control / Arrow IPC data / event)
- [x] `postgres` source plugin (server-side cursor, full type mapping)
- [x] `jsonl`, `null`, `parquet` destination plugins
- [x] **Providers** — `providers:` instances and `{{ secret('<instance>', '<ref>') }}`
      templates, resolved by provider plugins before the state lock and any data
      plugin (`resolve`/`resolved`, SDK `ProviderPlugin`); every `env()`/`file()`/
      `secret()` value masked in logs, plugin stderr, errors and the run summary;
      `validate` static by default, `--resolve` for the full resolution
- [x] **`s3` sink** (canonical plugins) — delivers staged files to AWS S3 or
      S3-compatible stores: pure-Rust SigV4 over rustls, multipart uploads,
      retries, and keys naming the run (`configure.run`) so incremental runs
      add objects instead of overwriting them
- [x] **Sink delivery stage** — separates format from location (no `s3_parquet`
      explosion); `local-copy` sink, `object_ready`/`deliver_object` protocol,
      and a core supervisor that respawns + resumes a crashed/hung sink from the
      delivery ledger
- [x] Live progress reporting, run summary, `--log-level`, `--no-color`
- [x] Multi-arch release image + binaries — slim core published to
      `ghcr.io/loadsmith-el/loadsmith` (`:slim`, `:vX.Y.Z-slim`) plus per-arch
      binary archives, from a `vX.Y.Z` tag by
      [`release.yml`](.github/workflows/release.yml) (`linux/amd64` + `linux/arm64` / Graviton)
- [x] **Static musl builds + four image flavors** — the engine and every
      canonical plugin are static musl binaries (with mimalloc), so one binary
      per arch runs on any Linux. Each release publishes `-static`
      (distroless, ~13MB), `-static-debug` (+ busybox), `-slim` (`:latest`) and
      `-bundled` (slim + the canonical plugins at release time). See
      [Container Images](docs/src/getting-started/container-images.md)
- [x] **Incremental state & checkpoints** — core-owned watermark state with a
      pluggable backend (`local`, with locking for concurrent runs), source
      resume cursor + watermark reporting (`incremental_state`), and intra-run
      durability-gated checkpointing (destination `committed` acks). `loadsmith
      state show|rm` to inspect/reset. At-least-once; exactly-once with an
      idempotent destination.
- [x] **`postgres` destination** — `COPY` into the target (`atomic`) or into a
      staging table then `MERGE` by PK (`staged_merge`, exactly-once effective)
- [x] **Plugin distribution & `loadsmith plugin install`** — plugins split out to
      [`loadsmith-canonical-plugins`](https://github.com/loadsmith-el/loadsmith-canonical-plugins):
      a manifest contract (`loadsmith-plugin.yaml`), per-plugin multi-arch GitHub
      Releases + a canonical index, and `loadsmith plugin install <name>` /
      `--all` / `--manifest` / `--binary` / `uninstall` (sha256-verified,
      protocol-range-checked). The default image (`:latest`) is **slim** (core
      only), with plugins installed on demand; a `-bundled` flavor carries the
      canonical plugins (see the static musl item above).
- [x] **Published docs** — the mdbook is hosted on GitHub Pages
      (<https://loadsmith-el.github.io/loadsmith/>), built and deployed on every
      push to `main` by [`docs.yml`](.github/workflows/docs.yml). The official
      plugin repo
      [`loadsmith-canonical-plugins`](https://loadsmith-el.github.io/loadsmith-canonical-plugins/)
      now ships its own published mdbook too.
- [x] **`mysql` connector** — a full source + destination
      (`loadsmith-source-mysql` / `loadsmith-destination-mysql`) on the pure-Rust
      `mysql_async` driver with rustls TLS (the same rustls-rustcrypto provider as
      postgres — no ring/aws-lc). Source: streaming read + incremental watermark;
      destination: `atomic` and `staged_merge` (ON DUPLICATE KEY upsert). Proven
      end-to-end in loadsmith-lab against MySQL 8 (smoke, TLS, both destination
      modes).
- [x] **`sharepoint` source connector** — reads from Microsoft 365 via the Graph
      API (`loadsmith-source-sharepoint`): an Excel/CSV file download or
      SharePoint List items, with in-plugin tabular transforms (filter with a
      structured predicate / normalize / fix-merged-columns / drop-empty-rows)
      applied before emitting Arrow. Source-only (SharePoint is read-only here).
      Pure-Rust HTTPS (reqwest over rustls-rustcrypto — no ring/aws-lc), Excel via
      calamine, CSV via the csv crate. Ported from the Python `sp-bulk`. Validated
      without a live tenant: unit tests plus a fake-Graph integration suite, and a
      `mock-graph` lab image (Graph is just HTTP, so the client's base URLs are
      configurable — which also serves sovereign clouds).
- [x] **`oracle` connector** — a full source + destination
      (`loadsmith-source-oracle` / `loadsmith-destination-oracle`) in
      `loadsmith-canonical-plugins`, on Oracle's official pure-Rust thin driver
      [`oracledb`](https://github.com/oracle/rust-oracledb) (26.0.0 beta, pinned):
      no Instant Client, amd64 + arm64 from the same build, TLS (TCPS) via the
      same rustls-rustcrypto provider. Source: wide reads + incremental watermark;
      destination: `atomic` (array INSERT) and `staged_merge` (staging GTT + one
      set-based `MERGE`). Proven in loadsmith-lab against Oracle Free on amd64
      and arm64 (7 cases, content-checked). Caveat: the driver still compiles
      `aws-lc-sys` at build time (unused at runtime). Replaces the out-of-canon
      ODPI-C build (`loadsmith-oracle-plugins`, retired).
- [x] **Per-kind plugin install** — `loadsmith plugin install <name>` installs a
      whole **package** (all the binaries its manifest `provides`); a `:<kind>`
      spec suffix or `--kind` flag installs a single **plugin**
      (`install mysql:source` / `install mysql --kind source` → just
      `loadsmith-source-mysql`), so a pipeline that reads from one system and
      writes to another doesn't carry the unused side. `<package> --all` is the
      explicit "all kinds" form; `:<kind>` composes with `@<version>`. No index
      or release change — the package is the index key and `kind` filters
      `provides` (the manifest already carries a `kind` per binary). `uninstall`
      takes the same `:<kind>` selector.
- [x] **End-to-end "build an EL" guide** —
      [Build an EL, End to End](docs/src/getting-started/build-an-el.md): one
      narrative from a `pipeline.yaml` (incremental postgres → jsonl, secrets via
      `{{ env() }}`), to a pinned multi-arch image on the slim core with only the
      needed plugins (`loadsmith plugin install`), to testing that image with a
      loadsmith-lab case in a local origin. Verified end to end against the
      v0.2.0 images. The Custom image builder (below) is its future shortcut.

## Planned

- [ ] **1.0** — freeze the interfaces listed in the
      [Stability Policy](docs/src/reference/stability.md). Done so far: the
      policy itself; a CI gate (build/test/clippy `-D warnings`, pinned
      toolchain) and tests before every release; the 1.0 blockers fixed
      (tolerant protocol reader and an fd4 drain that never stops, UTF-8-safe
      templates, a versioned state format with a stable fingerprint, strict
      plugin blocks in `pipeline.yaml`, lock-lease fixes); a correct abort
      (the destination's input ends only after the source succeeded; on any
      failure or SIGINT/SIGTERM the core sends `cancel`/`error`, waits a grace
      period and kills — the destination rolls back instead of committing a
      partial load), per-phase timeouts and an opt-in stall timeout, one
      `resend` for a malformed control line, and distinct exit codes; a
      complete Arrow field-type vocabulary in the `schema` message (with
      nullability and tolerant reading) and drift detection on the exact Arrow
      stream schema (state format v2, migrated automatically); a versioned
      JSON run summary (`--output json`, `--summary-file`).
      Done since: providers wired into the core (`providers:` + `secret()`,
      the `provider` kind rename, masking everywhere, static `validate`) and
      the parser stage (wired and covered; `parser:` + `state:` deferred to
      1.x), and the unknown-keys policy (an error by default, warnings with
      `--ignore-unknown-keys`; SDK `PluginConfig::parse`), OpenTelemetry, and
      the canonical plugins on the new SDK (`cancel()` rolling back, config
      through `PluginConfig::parse`). The policy's last two decisions are
      settled (one state format for every backend; the Rust crates stay
      unpublished). Remaining: the release candidate's soak, then 1.0.0.

- [ ] **`s3` sink follow-ups** — the sink ships in the canonical plugins
      (pure-Rust SigV4 + multipart, one key prefix per run via
      `configure.run`). Next: instance/task-role credentials (EC2 instance
      profile, ECS task role, EKS IRSA); server-side encryption options.
- [ ] **Parsers** — shipped: the core wiring, the SDK runtimes, and in the
      canonical plugins a `file` raw source, a streaming `csv` parser and an
      `excel` parser. Later (1.x): `parser:` + `state:` (per-file checkpoints
      from `object_parsed.last_batch_seq`), raw-source backpressure.
- [ ] **More providers** — the canonical `file` provider ships on the
      `provider` kind and API; next, an `aws-secrets-manager` provider (1.x, needs SigV4 like the s3 sink);
      Vault and other cloud secret managers.
- [ ] **Whole-document config loading** (`--config-ref`, 1.x) — load the
      pipeline document itself from a remote reference through a provider.
- [ ] **Remote state backend** — an `s3` state backend (conditional PUT for a
      distributed lock) behind the existing `StateBackend` trait, reusing the
      s3 sink's SigV4/TLS. Local-file state with locking ships today.
- [ ] **`oracle` follow-ups** — drop the build-time `aws-lc-sys` (needs
      `oracledb` to make its rustls provider optional, oracle/rust-oracledb#34);
      enforce TLS DN pinning once the driver applies it (#25); schema-qualified
      `target_table`; CLOB values over 4000 bytes; real NLS coverage in the lab
      (a server-side logon trigger, since thin drivers ignore `NLS_LANG`); move
      the lab's Oracle readiness probe off the community `oracle-rs`.
- [ ] **More plugins** — additional source/destination/sink types beyond the
      current postgres + mysql + oracle source+destination / sharepoint source / jsonl / null / parquet /
      local-copy / file set (sqlserver sources; gcs/sftp sinks).
- [ ] **Parallel plugin I/O** — a wishlist item, not yet designed in detail.
      Source/destination connect+fetch/write usually dominates runtime far more
      than the pump's data-plane transfer, so plugins fanning out internally
      (multiple connections/partitions on the source; multiple concurrent
      writers on the destination) is an attractive perf win. The core/pump/
      protocol stay a single ordered stream — this is plugin-internal
      parallelism, with results reordered/buffered back to that ordering at the
      boundary. See the "Ordering invariants for plugin-internal parallelism"
      section in [incremental-state.md](docs/src/architecture/incremental-state.md)
      for the constraints any such design must satisfy (cumulative ack on the
      destination side, idempotency at every durable layer).
- [ ] **Custom image builder** — a first-class `loadsmith` subcommand that
      takes a manifest of desired plugins, builds a slim custom image
      (multi-arch) containing only those, optionally validates it against
      relevant lab cases before release, and pushes to a registry (ECR, GHCR,
      DockerHub, …). Lowers the friction of going from "official slim image" to
      "my production image" — the project is loadsmith *plus* its tooling, and
      adoption hinges on how easy that whole loop is.
