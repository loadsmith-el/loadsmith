# CI-only runtime images for the loadsmith core: one target per image flavor.
#
# The release workflow (.github/workflows/release.yml) compiles `loadsmith`
# NATIVELY on a per-arch runner as a static musl binary, then feeds it here as
# the build context — so these images never compile anything (no QEMU-emulated
# Rust build). The hermetic, build-from-source image is ../Dockerfile; keep the
# runtime stages below in sync with its stages of the same name.
#
# Build context:
#   loadsmith   the prebuilt static binary
#   plugins/    the canonical plugin binaries (only the `bundled` target uses it)
#
# Targets (published as ghcr.io/loadsmith-el/loadsmith:<tag>-<target>):
#   static        distroless/static: the binary + CA certs, tzdata, /tmp, passwd
#   static-debug  the same plus a busybox shell (`--entrypoint sh`)
#   slim          debian:bookworm-slim (`:latest`)
#   bundled       slim + every canonical plugin, on PATH

FROM gcr.io/distroless/static-debian12 AS static
COPY --chmod=755 loadsmith /usr/local/bin/loadsmith
ENTRYPOINT ["/usr/local/bin/loadsmith"]

FROM gcr.io/distroless/static-debian12:debug AS static-debug
COPY --chmod=755 loadsmith /usr/local/bin/loadsmith
ENTRYPOINT ["/usr/local/bin/loadsmith"]

FROM debian:bookworm-slim AS slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --chmod=755 loadsmith /usr/local/bin/loadsmith
ENTRYPOINT ["loadsmith"]

# The plugins sit on PATH, which plugin discovery falls back to after the plugin
# dir: they work for any user/HOME, and a plugin installed into the plugin dir
# (or a --plugin-dir) still takes precedence over the bundled one.
FROM slim AS bundled
ARG PLUGINS=""
COPY --chmod=755 plugins/ /usr/local/bin/
LABEL dev.loadsmith.plugins="${PLUGINS}"
