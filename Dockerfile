# Hermetic, multi-stage build of loadsmith + its plugin binaries.
#
# The build compiles entirely inside Docker (never copies host-built ELF), so it
# is correct regardless of the host's glibc. Build and runtime share the same
# Debian base (bookworm) so the dynamically-linked binaries match the runtime's
# glibc. cargo-chef caches the dependency compilation in its own layer, so
# source-only changes rebuild fast.

FROM rust:1-bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

# Compute the dependency recipe from the workspace manifests.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Cook dependencies (cached layer), then build the workspace binaries.
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release \
      --bin loadsmith \
      --bin loadsmith-source-postgres \
      --bin loadsmith-destination-jsonl \
      --bin loadsmith-destination-null \
      --bin loadsmith-destination-parquet \
      --bin loadsmith-sink-local-copy \
      --bin loadsmith-config-provider-file

# Slim runtime with the binaries on PATH. The core resolves plugins by name
# (loadsmith-{kind}-{type}) in --plugin-dir then PATH, so all of them in
# /usr/local/bin satisfy discovery.
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/loadsmith                      /usr/local/bin/
COPY --from=builder /app/target/release/loadsmith-source-postgres      /usr/local/bin/
COPY --from=builder /app/target/release/loadsmith-destination-jsonl    /usr/local/bin/
COPY --from=builder /app/target/release/loadsmith-destination-null     /usr/local/bin/
COPY --from=builder /app/target/release/loadsmith-destination-parquet  /usr/local/bin/
COPY --from=builder /app/target/release/loadsmith-sink-local-copy      /usr/local/bin/
COPY --from=builder /app/target/release/loadsmith-config-provider-file /usr/local/bin/
ENTRYPOINT ["loadsmith"]
