# Hermetic, multi-stage build of the loadsmith **core** images.
#
# Plugins are not bundled (except in the `bundled` target) — they live in
# loadsmith-canonical-plugins and are fetched on demand with
# `loadsmith plugin install <name>` (into ~/.loadsmith/plugins, or a
# `--plugin-dir`).
#
# The build compiles entirely inside Docker (never copies host-built ELF) in
# rust:1-alpine, whose host target is *-unknown-linux-musl: the binary is fully
# static, so the same build runs on every runtime below. cargo-chef caches the
# dependency compilation layer.
#
# Targets (`docker build --target <t>`; the default is the last one, `slim`):
#   static        distroless/static: the binary + CA certs, tzdata, /tmp, passwd
#   static-debug  the same plus a busybox shell (`--entrypoint sh`)
#   bundled       slim + every canonical plugin (latest from the index), on PATH
#   slim          debian:bookworm-slim
#
# This is the hermetic, build-from-source image (used locally and by the lab's
# `--loadsmith <dir>` path). CI publishes the same runtimes from a prebuilt
# native binary via `ci/runtime.Dockerfile` — keep its stages in sync with the
# runtime stages below.

FROM rust:1-alpine AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

# Compute the dependency recipe from the workspace manifests.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Cook dependencies (cached layer), then build just the core binary.
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin loadsmith

FROM gcr.io/distroless/static-debian12 AS static
COPY --from=builder /app/target/release/loadsmith /usr/local/bin/loadsmith
ENTRYPOINT ["/usr/local/bin/loadsmith"]

FROM gcr.io/distroless/static-debian12:debug AS static-debug
COPY --from=builder /app/target/release/loadsmith /usr/local/bin/loadsmith
ENTRYPOINT ["/usr/local/bin/loadsmith"]

# The core on PATH. It resolves plugins by name (loadsmith-{kind}-{type}) in
# --plugin-dir, then PATH. (The installer verifies HTTPS against its own
# built-in roots; ca-certificates is for whatever else users run here.)
FROM debian:bookworm-slim AS slim-base
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/loadsmith /usr/local/bin/
ENTRYPOINT ["loadsmith"]

# Plugins on PATH (discovery's fallback after the plugin dir), so they work for
# any user/HOME and a plugin installed into the plugin dir still wins.
FROM slim-base AS bundled
RUN ["loadsmith", "plugin", "install", "--all", "--plugin-dir", "/usr/local/bin"]

FROM slim-base AS slim
