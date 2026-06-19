# Skill: create-config-provider-plugin

Scaffold a new **config-provider** plugin: a standalone binary that loads
configuration content from an external location (an `s3://`, `https://`, … URI) and
returns it as raw bytes. The SDK drives the whole protocol; you implement only the
`ConfigProviderPlugin` trait.

This skill takes arguments `<name> [notes]` (passed by the invoking agent — e.g.
Claude's `/create-config-provider-plugin` slash command, or directly).

## Know the boundary first

A **config provider** *loads configuration* (the YAML pipeline itself, or a fragment
of it) from somewhere. It is **not** the secret mechanism — secrets are resolved
inline by the core via `{{ }}` templates (`crates/loadsmith-config/src/template.rs`).
If the user is really asking for a secrets source, that's a different track; confirm
which one they mean.

## Read these first (the working reference)

1. `crates/loadsmith-plugin-sdk/src/config_provider.rs` — the `ConfigProviderPlugin`
   trait and the protocol the SDK runs for you
2. `plugins/config-providers/file/src/main.rs` — the `file://` reference provider
   (it's a single small file: configure validates the URI, fetch returns bytes)

## Architecture you MUST respect

- **You never touch the protocol.** `run_config_provider` does the handshake,
  capabilities, configure, and returns your fetched bytes to the core.
- **Two methods, that's it.** `configure(config)` validates the URI/scheme and
  stores what you need; `fetch()` returns the content as `Vec<u8>`. No streaming,
  no Arrow — config payloads are small.
- **Validate the scheme in `configure`, fail fast.** Reject a URI whose scheme this
  provider doesn't own (the `file` provider requires a `file://` prefix).
- **`plugin_name()` must equal the binary name** (`loadsmith-config-provider-<name>`).

## Steps

### 1. Parse arguments
- **name** — the provider/scheme name (e.g. `s3`, `http`, `gcs`). The binary
  becomes `loadsmith-config-provider-<name>`.
- **notes** — optional context (auth, region, etc.)

### 2. `plugins/config-providers/<name>/Cargo.toml`
Mirror `plugins/config-providers/file/Cargo.toml`. Package + `[[bin]]` both named
`loadsmith-config-provider-<name>`. Base deps:
```toml
tokio, anyhow, tracing, serde, serde_json, async-trait
loadsmith-plugin-sdk = { path = "../../../crates/loadsmith-plugin-sdk" }
```
Add whatever client crate the source needs (e.g. an HTTP or S3 client).

### 3. `plugins/config-providers/<name>/src/main.rs`
A single file is fine (follow the `file` provider). A config struct via serde, a
small struct holding resolved state, `impl ConfigProviderPlugin`, and:
```rust
#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_config_provider(<Name>Provider::new()).await
}
```
- `configure(config)` — deserialize the typed config (typically a `uri`), validate
  the scheme, store the resolved location.
- `fetch()` — read the content and return `Vec<u8>` (the core decodes/parses it).

### 4. Register the workspace member
Add `"plugins/config-providers/<name>"` to `members` in the root `Cargo.toml`.

## Verify

```bash
cargo build -p loadsmith-config-provider-<name>     # must compile
```

Print a short summary of what was created and how a user would reference this
provider (its URI scheme). Do not run anything beyond the compile check.
