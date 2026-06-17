# Roadmap

What's shipped and what's queued next. Shipped items are documented in
[README.md](README.md) and [docs/](docs/src) — no details repeated here.

## Shipped

- [x] Core pump architecture — process-isolated plugins, three-plane protocol
      (control / Arrow IPC data / event)
- [x] `postgres` source plugin (server-side cursor, full type mapping)
- [x] `jsonl`, `null`, `parquet` destination plugins
- [x] `file` config provider
- [x] **Sink delivery stage** — separates format from location (no `s3_parquet`
      explosion); `local-copy` sink, `object_ready`/`deliver_object` protocol,
      and a core supervisor that respawns + resumes a crashed/hung sink from the
      delivery ledger
- [x] Live progress reporting, run summary, `--log-level`, `--no-color`
- [x] Multi-arch release image + binaries — slim core published to
      `ghcr.io/loadsmith-el/loadsmith` (`:slim`, `:vX.Y.Z-slim`) plus per-arch
      binary archives, from a `vX.Y.Z` tag by
      [`release.yml`](.github/workflows/release.yml) (`linux/amd64` + `linux/arm64` / Graviton)
- [x] **Incremental state & checkpoints** — core-owned watermark state with a
      pluggable backend (`local`, with locking for concurrent runs), source
      resume cursor + watermark reporting (`incremental_state`), and intra-run
      durability-gated checkpointing (destination `committed` acks). `loadsmith
      state show|rm` to inspect/reset. At-least-once; exactly-once with an
      idempotent destination.
- [x] **`postgres` destination** — `COPY` into the target (`atomic`) or into a
      staging table then `MERGE` by PK (`staged_merge`, exactly-once effective)
- [x] **Plugin distribution & `loadsmith install`** — plugins split out to
      [`loadsmith-canonical-plugins`](https://github.com/loadsmith-el/loadsmith-canonical-plugins):
      a manifest contract (`loadsmith-plugin.yaml`), per-plugin multi-arch GitHub
      Releases + a canonical index, and `loadsmith plugin install <name>` /
      `--all` / `--manifest` / `--binary` / `uninstall` (sha256-verified,
      protocol-range-checked). The official image is now **slim** (core only);
      plugins are installed on demand.
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

## Planned

- [ ] **`s3` sink** — multipart-upload delivery of staged files to S3. The TLS
      direction is settled (`rustls` + the pure-Rust `rustls-rustcrypto` provider
      — see [docs](docs/src/architecture/multi-arch-and-tls.md)); the remaining
      work is the pure-Rust multipart upload + SigV4 signing, not the crypto stack.
- [ ] **Templates & secret providers** — config interpolation today covers
      `{{ env(...) }}`; broader templating and dedicated secret-provider
      plugins (vault, cloud secret managers, …) would get credentials out of
      plain environment variables.
- [ ] **Remote state backend** — an `s3` state backend (conditional PUT for a
      distributed lock) behind the existing `StateBackend` trait, reusing the
      s3 sink's SigV4/TLS. Local-file state with locking ships today.
- [ ] **More plugins** — additional source/destination/sink types beyond the
      current postgres + mysql source+destination / jsonl / null / parquet /
      local-copy / file set (oracle, sqlserver sources; gcs/sftp sinks).
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
- [ ] **End-to-end "build an EL" guide** — one narrative from a `pipeline.yaml`
      (source → destination), to a runnable image with the needed plugins
      installed on the slim core (`loadsmith plugin install`), to testing it with
      loadsmith-lab. The getting-started-for-a-real-pipeline doc; pairs with the
      Custom image builder above.
