# CI-only runtime image for the slim loadsmith core.
#
# The release workflow (.github/workflows/release.yml) compiles `loadsmith`
# NATIVELY on a per-arch runner, then feeds the resulting binary here as the
# build context — so this image never compiles anything (no QEMU-emulated Rust
# build). The hermetic, build-from-source image is ../Dockerfile; keep the
# runtime layer below in sync with that Dockerfile's `runtime` stage.
#
# Build context = a directory containing just the prebuilt `loadsmith` binary.
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY loadsmith /usr/local/bin/loadsmith
RUN chmod 755 /usr/local/bin/loadsmith
ENTRYPOINT ["loadsmith"]
