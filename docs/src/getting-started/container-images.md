# Container Images

Every release publishes four image flavors to `ghcr.io/loadsmith-el/loadsmith`,
all multi-arch (`linux/amd64` + `linux/arm64` / Graviton) and all carrying the
**same static binary**. The binary is built against musl and has no runtime
dependencies, so the flavor only decides what sits around it.

| Flavor | Tag | Base | Size (download) | Shell | Plugins |
|---|---|---|---|---|---|
| **static** | `vX.Y.Z-static` | `gcr.io/distroless/static-debian12` | ~13MB (~4MB) | no | install your own |
| **static-debug** | `vX.Y.Z-static-debug` | distroless static `:debug` | ~14MB (~5MB) | busybox | install your own |
| **slim** | `vX.Y.Z-slim` | `debian:bookworm-slim` | ~100MB (~35MB) | bash | install your own |
| **bundled** | `vX.Y.Z-bundled` | `debian:bookworm-slim` | ~170MB (~60MB) | bash | all canonical plugins |

Each flavor also has a rolling tag (`:static`, `:static-debug`, `:slim`,
`:bundled`) that stable releases move, and `:latest` is `:slim`. Every release
also publishes an immutable `sha-<short-sha>-<flavor>` tag per flavor. Prereleases
(`vX.Y.Z-<suffix>`) publish only their versioned and `sha-` tags.

## Installing plugins on `-static`

`-static` has a filesystem (`/tmp`, `/etc/passwd`, CA certificates, tzdata) but
no shell and no OS userland. It doesn't need one: the plugin installer is the
`loadsmith` binary itself, and every canonical plugin is a static binary too.
Two ways to get plugins in:

**At build time (recommended).** Use the exec form of `RUN`, which needs no
shell:

```dockerfile
FROM ghcr.io/loadsmith-el/loadsmith:v0.2.0-static
RUN ["loadsmith", "plugin", "install", "postgres", "--plugin-dir", "/usr/local/bin"]
RUN ["loadsmith", "plugin", "install", "parquet", "--plugin-dir", "/usr/local/bin"]
COPY pipeline.yaml /pipeline.yaml
CMD ["run", "/pipeline.yaml"]
```

Installing into `/usr/local/bin` (as the `-bundled` image does) keeps the
plugins visible to any user: the core falls back to `PATH`. The default plugin
dir is `$HOME/.loadsmith/plugins`, which at build time is `/root/...` — a
container started with `--user` or Kubernetes `runAsNonRoot` would not find
plugins installed there.

That is a complete Postgres → Parquet image of about 35MB (the `postgres`
package brings both its source and its destination).

**At run time.** Mount a plugin directory and point `--plugin-dir` at it:

```bash
docker run --rm -v ~/.loadsmith/plugins:/plugins -v "$PWD":/work \
  ghcr.io/loadsmith-el/loadsmith:static run /work/pipeline.yaml --plugin-dir /plugins
```

**Kubernetes with `readOnlyRootFilesystem: true`.** Install the plugins at build
time as above, or mount an `emptyDir` for the plugin dir. Also mount an
`emptyDir` at `/tmp`: some plugins write temporary files there (for example, the
Oracle plugin writes a temporary wallet for TLS).

**Stopping a run.** The engine is the entrypoint, so it runs as PID 1 and
receives `docker stop`'s or Kubernetes' SIGTERM directly. It handles the signal
explicitly: it cancels the plugins (a destination rolls back), waits up to
`pipeline.timeouts.cancel_grace_seconds` (default 30 s), kills what is left,
releases the state lock and exits `143`. `docker stop` only waits 10 s before
it sends SIGKILL, so either give it more time (`docker stop -t 40`) or lower
`cancel_grace_seconds`; in Kubernetes keep it under
`terminationGracePeriodSeconds`.

## Choosing a flavor

- **static**: production, when you want the smallest image and the smallest
  attack surface. Build a derived image with exactly the plugins you use.
- **static-debug**: the same, when you need to get a shell into the container
  (`docker run --entrypoint sh ...`) to inspect files or the network.
- **slim**: when your pipeline needs a shell or OS tools around `loadsmith`, or
  you prefer a familiar Debian base.
- **bundled**: to try Loadsmith or run ad-hoc pipelines without installing
  anything. Its plugins are those the canonical index listed as latest when that
  engine release was built, listed in the image label `dev.loadsmith.plugins`:

  ```bash
  docker inspect --format '{{ index .Config.Labels "dev.loadsmith.plugins" }}' \
    ghcr.io/loadsmith-el/loadsmith:bundled
  ```

  A plugin release does not rebuild it. To get a newer plugin, install it over
  the bundled one (`loadsmith plugin install <name>`): the plugin dir is searched
  before the bundled plugins on `PATH`.

## Running on AWS

On ECS (Fargate or Managed Instances), you can mount an Amazon S3 Files file
system into the task and pass the pipeline's path in the container `command`,
e.g. `["run", "/mnt/pipelines/orders.yaml"]`. The pipeline YAML then lives in
S3 and needs no image rebuild when it changes. (S3 Files is not available to
the EC2 launch type.)

## Static binaries outside Docker

The binary archives attached to each GitHub Release are the same static
binaries, so they run on any Linux distribution of the matching architecture,
glibc or musl, with no dependencies to install.
