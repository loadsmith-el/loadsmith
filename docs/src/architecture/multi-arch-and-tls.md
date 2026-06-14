# Multi-arch & TLS

Loadsmith publishes a single release image for both `linux/amd64` and
`linux/arm64` (AWS Graviton). That goal — **multi-arch first** — drives a
dependency rule that is easy to misread as "no TLS." It is not. TLS is required
(databases demand secure connections; the S3 sink will too). The real constraint
is **no native/arch-specific code in the dependency tree**, and this page records
how we satisfy both at once.

## Why native code is the problem, not TLS

The release image is built with:

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t loadsmith:<tag> .
```

against the [`Dockerfile`](https://github.com/loadsmith-el/loadsmith/blob/main/Dockerfile), which is a plain `cargo build
--release` inside a Debian base. There is no cross-compilation toolchain, no
`--target`, no per-arch linker setup — each platform is built `cargo build`-native
*inside* that architecture (under QEMU emulation for the non-host arch).

Under that model, C/assembly crypto does not *fail* to compile — it compiles
*natively under emulation*, which means:

1. **Slow emulated C builds** — `cc`/`cmake`/`perl`/`nasm` running under QEMU can
   inflate build time by an order of magnitude.
2. **Fragile toolchains** — every native crypto backend drags in build-time
   tooling (OpenSSL headers, `cmake`, NASM for some) that breaks the "needs no
   per-arch branches" promise.

So the cost of native crypto is real even though it is not *impossible*. Avoiding
it keeps the multi-arch build simple and fast.

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

The **TLS spike gate** — cross-database handshakes against Postgres and MySQL, on
both `linux/amd64` and `linux/arm64` under QEMU — remains to be executed before
declaring the stack validated for all future source plugins. See the plan at
`.claude/plans/planeja-para-a-gente-joyful-clarke.md`.
