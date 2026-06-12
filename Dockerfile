# Hermetic, multi-stage build of the slim loadsmith **core** image.
#
# Plugins are not bundled — they live in loadsmith-canonical-plugins and are
# fetched on demand with `loadsmith plugin install <name>` (into
# ~/.loadsmith/plugins, or a `--plugin-dir`). This image ships just the core.
#
# The build compiles entirely inside Docker (never copies host-built ELF), so it
# is correct regardless of the host's glibc. Build and runtime share the same
# Debian base (bookworm). cargo-chef caches the dependency compilation layer.
#
# This is the hermetic, build-from-source image (used locally and by the lab's
# `--loadsmith <dir>` path). CI publishes the same runtime from a prebuilt
# native binary via `ci/runtime.Dockerfile` — keep its runtime layer in sync
# with the `runtime` stage below.

FROM rust:1-bookworm AS chef
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

# Slim runtime: the core on PATH. It resolves plugins by name
# (loadsmith-{kind}-{type}) in --plugin-dir then PATH. ca-certificates lets the
# installer fetch plugins over HTTPS.
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/loadsmith /usr/local/bin/
ENTRYPOINT ["loadsmith"]
