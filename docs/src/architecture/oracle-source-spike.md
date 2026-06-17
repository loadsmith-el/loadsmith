# Oracle Source — Research Spike

A decision memo, not an implementation. It records why an Oracle source is *not*
a drop-in like the MySQL connector, the options, and the recommendation for the
1.0 plugin scope. See [Multi-arch & TLS](./multi-arch-and-tls.md) for the
constraint that drives the whole analysis.

## The constraint

Loadsmith's foundational rule is **pure-Rust, multi-arch** (`linux/amd64` +
`linux/arm64`, built `cargo build`-native inside each arch under QEMU). No
native/assembly-per-arch code, no C toolchains in the plugin build. TLS is done
with `rustls` + the pure-Rust `rustls-rustcrypto` provider; `native-tls`,
`openssl-sys`, `ring`, and `aws-lc-rs` are all banned. The postgres and mysql
connectors both satisfy this with pure-Rust wire-protocol drivers
(`tokio-postgres`, `mysql_async`).

Oracle is the first source where that rule bites.

## Driver landscape (as of this writing)

| Option | Pure Rust? | Multi-arch posture | Notes |
|---|---|---|---|
| [`oracle`](https://crates.io/crates/oracle) crate | ❌ | breaks it | Wraps **ODPI-C** → requires the native **Oracle Instant Client** (C, per-arch, separately licensed, not redistributable in a slim image). |
| `sibyl` | ❌ | breaks it | Also OCI/native-client based. |
| A pure-Rust TNS driver | ✅ (would be) | satisfies it | **None mature exists.** The Oracle wire protocol (TNS) is undocumented but has been reimplemented from scratch without OCI elsewhere — see "Feasibility" below. |

The key finding: **every mature Rust Oracle driver is a thin shell over the
native Oracle client.** Adopting one means shipping (or requiring) the Instant
Client, per-arch native builds, and the licensing/redistribution constraints that
come with it — exactly the posture the project rejects.

## Feasibility of a pure-Rust Oracle driver

Reimplementing the TNS wire protocol in Rust (a "thin mode" driver, no OCI) is
**feasible but a large, sustained effort**:

- **`python-oracledb`** ships a pure-Python *thin mode* that speaks TNS directly
  — proof the protocol can be implemented without OCI, and a reference for the
  handshake, data types, and auth.
- **`go-ora`** is a mature, pure-Go Oracle driver that talks TNS with no native
  client — proof a from-scratch implementation in a systems language is viable
  and maintainable.

Neither is small. TNS covers connection negotiation, several auth mechanisms
(including O5LOGON / password verifiers), the full type system (including Oracle
`NUMBER`, which—like our text-protocol decimals—wants careful handling), and LOB
streaming. This is months of work and an ongoing maintenance commitment, not a
weekend port.

## Options matrix

1. **Defer past 1.0 (keep the posture).** Ship 1.0 with postgres + mysql (and
   any other pure-Rust-drivable sources). Oracle waits for a deliberate
   investment. *Cost:* no Oracle at 1.0. *Benefit:* the slim, pure-Rust,
   multi-arch story stays intact and simple.
2. **Scoped native exception for Oracle only.** Build the Oracle plugin on the
   `oracle`/ODPI-C stack, accepting native, per-arch builds and an Instant Client
   dependency **for that one plugin**. *Cost:* the Oracle plugin can't ride the
   normal slim multi-arch image/CI path — it needs its own build with the Instant
   Client, arch-specific artifacts, and a licensing note; it's the camel's nose
   for native deps. *Benefit:* Oracle support relatively quickly.
3. **Invest in a pure-Rust TNS driver.** Write (or sponsor) a thin-mode Oracle
   driver in Rust, then build the plugin on it like postgres/mysql. *Cost:*
   months of protocol work + maintenance. *Benefit:* Oracle support that fully
   honors the posture; a reusable asset for the ecosystem.

## Recommendation

**Defer Oracle past 1.0 (option 1), and treat option 3 as the real long-term
path; do not adopt option 2 as a default.**

Rationale: the pure-Rust/multi-arch posture is a load-bearing product decision
(slim images, trivial Graviton support, no C toolchain in CI). A native exception
for Oracle erodes exactly that and tends to spread. 1.0 is strong with
postgres + mysql connectors proven end-to-end in the lab. If a concrete Oracle
requirement arrives before a pure-Rust driver is realistic, option 2 can be
revisited as an **explicitly-isolated** plugin (its own image/CI lane, clearly
labeled as not part of the slim multi-arch set) — but that is a deliberate, eyes-
open exception, not the plan of record.

If/when option 3 is taken on, `python-oracledb` thin mode and `go-ora` are the
references to mine for the protocol.
