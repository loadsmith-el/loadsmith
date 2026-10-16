# Multi-arch & TLS

Loadsmith publishes its release images (four flavors, see
[Container Images](../getting-started/container-images.md)) and binaries for
both `linux/amd64` and `linux/arm64` (AWS Graviton). That goal — **multi-arch
first** — drives a dependency rule that is easy to misread as "no TLS." It is
not. TLS is required (databases demand secure connections; the S3 sink will
too). The real constraint is **no arch-specific native code in the dependency
tree that needs its own toolchain**, and this page records how we satisfy both
at once.

## How the release is built

[`release.yml`](https://github.com/loadsmith-el/loadsmith/blob/main/.github/workflows/release.yml)
compiles `loadsmith` natively on one runner per architecture (no QEMU), inside
`rust:1-alpine`. Its host target is `*-unknown-linux-musl`, so a plain
`cargo build --release` produces a fully static binary: no libc, no dynamic
loader, nothing to match on the target system. The four image flavors wrap that
same binary, and the canonical plugins are built the same way.

Static musl retired the problem the earlier glibc builds had: a binary built on
a newer distro linked newer glibc symbols and would not start on an older base
image. The price is musl's memory allocator, which is slow under the many small
allocations of Arrow batch handling, so the engine and every plugin set
[mimalloc](https://github.com/microsoft/mimalloc) as the global allocator.

## Why native code is still avoided

mimalloc is portable C that any `cc` compiles, so it costs nothing. Native
*crypto* backends are a different matter:

1. **Fragile toolchains** — they drag in build-time tooling (OpenSSL headers,
   `cmake`, NASM for some) and per-arch assembly, which breaks the "needs no
   per-arch branches" promise.
2. **Slow or brittle cross-builds** — anyone building an image for the other
   arch locally (`docker buildx --platform`) compiles that C under QEMU
   emulation, which can inflate build time by an order of magnitude.

Avoiding them keeps every build, local or CI, simple and fast.

## Two layers, opposite risk profiles

TLS in Rust is two distinct layers, and conflating them is what makes the
decision feel risky when it isn't:

| Layer | Choice | Nature |
|---|---|---|
| TLS protocol | **`rustls`** | Foundational, safe — maintained by the ISRG (Let's Encrypt's parent) with Rust Foundation backing, past 1.0, audited, used in production at scale. This is the long-term bet, and it does not die. |
| Crypto provider (primitives: AES, curves) | **`rustls-rustcrypto`** | A swappable knob, installed once per process via `CryptoProvider::install_default()`. The choice is reversible and has per-binary blast radius. |

The commitment that "pulls the whole project along" is only the protocol layer —
`rustls` — and that is precisely the safe choice. The provider is tactical.

## Why `rustls-rustcrypto`

The blessed crypto providers for `rustls` both violate the multi-arch rule:

- **`aws-lc-rs`** (rustls's default) — AWS's BoringSSL fork. Fast, FIPS-capable,
  will not die — but it is C/assembly with `cmake` at build time.
- **`ring`** — assembly per arch, and historically a single-maintainer project.

`rustls-rustcrypto` is a 100% Rust provider backed by the RustCrypto crates
(`aes`, `chacha20poly1305`, `p256`, …). No assembly, no build tooling — it
compiles anywhere, which is exactly what multi-arch-first needs.

**Honest trade-off.** `rustls-rustcrypto` is the least mature / least independently
audited link in the chain — but only at the *primitive* level; the TLS protocol
logic on top stays `rustls` (audited). For a tool whose hot path is bulk data
streaming, not the TLS handshake, this is an acceptable price for build
simplicity. The escape hatch is built in: if performance or FIPS ever forces it,
`CryptoProvider::install_default(aws-lc-rs)` is a one-line swap — at which point we
accept the C toolchain cost deliberately, not by accident.

## It applies to everything

This is not a postgres-only decision. Every network-facing plugin speaks `rustls`:

- **Postgres** (`sslmode=require`, …) — `tokio-postgres-rustls-improved` plugs a
  rustls-based `MakeTlsConnect` into the existing `tokio-postgres` driver.
- **MySQL** (`useSSL`, …) — `mysql_async` / `sqlx` expose a `rustls-tls` feature.
- **S3 / HTTP** (the future sink) — `object_store` / `reqwest` / `hyper` all ship
  a rustls feature.

Because each plugin is a separate process, the provider is installed per-binary —
so the blast radius of the crypto choice is one plugin, never the whole tool.
There is no global ABI lock-in.

## Where the generic stack lives: `loadsmith-tls`

The generic, driver-agnostic part of this — installing the provider, the five
TLS modes (`disable` / `prefer` / `require` / `verify-ca` / `verify-full`), mTLS,
the cert loaders and verifiers — lives in the **`loadsmith-tls`** crate
(`crates/loadsmith-tls`). It exposes a generic `TlsConfig` and a
`client_config(&TlsConfig) -> Option<rustls::ClientConfig>`; it depends on
`rustls` only, **never** on any database/transport driver. Each network plugin
takes that `rustls::ClientConfig` and wraps it in its own connector (postgres:
`MakeRustlsConnect`; mysql/s3: their own). This is the boundary that keeps the
loadsmith core/crates agnostic: loadsmith knows *TLS* (a shared networking
concern), never *postgres*.

## Status: first real consumer

The postgres plugins (`plugins/postgres`, emitting `loadsmith-source-postgres`
and `loadsmith-destination-postgres`) are the **first consumers** of
`loadsmith-tls`, both over the same connection layer. They implement all five TLS
modes, mTLS, and the `rustls-rustcrypto` provider install. The crate choices that
make it work:

| Crate | Version | Role |
|---|---|---|
| `rustls` | 0.23 | TLS protocol, `default-features = false` to drop `aws-lc-rs` |
| `rustls-rustcrypto` | 0.0.2-alpha | Pure-Rust crypto provider (`CryptoProvider::install_default`) |
| `tokio-postgres-rustls-improved` | 0.16 | Bridge (`MakeRustlsConnect`), `default-features = false` to drop `aws-lc-rs`; fixes SCRAM/SASL channel-binding bug vs the original crate |
| `rustls-pemfile` | 2 | PEM parsing for root and client certificates |

The **TLS spike gate**, cross-database handshakes against Postgres, MySQL and
Oracle, runs as the lab bundle `tls-spike` (`loadsmith-lab bundle run --select
catalog/tls-spike`) and passes on `linux/amd64`. A run on `linux/arm64` is
still to be done.
