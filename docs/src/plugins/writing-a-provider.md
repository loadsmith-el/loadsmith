# Writing a Provider

A **provider** is a standalone binary that resolves configuration values — the
`{{ secret('<instance>', '<ref>') }}` calls of a pipeline file. Secret stores
(a mounted file, AWS Secrets Manager, Vault…) plug into Loadsmith this way.

```yaml
providers:
  creds:                       # an instance name, used by secret()
    type: env-file             # → binary loadsmith-provider-env-file
    config: { path: /run/secrets/db.env }
source:
  type: postgres
  config:
    user: "{{ secret('creds', 'PG_USER') }}"
    password: "{{ secret('creds', 'PG_PASSWORD') }}"
```

Before the state lock and before any data plugin starts, the core spawns every
instance some `secret()` references (once each, in parallel), sends one
`resolve` per distinct reference, closes the provider's stdin, and waits for its
`finished`. The values are substituted into the config and **masked** in
everything the engine prints. See the
[`providers:` block](../reference/pipeline-yaml.md#providers) and
[`secret()`](../reference/pipeline-yaml.md#-secretinstance-ref-) reference.

Plain environment variables and local files need no provider: use
`{{ env('VAR') }}` and `{{ file('/path') }}`. Write a provider when the value
lives in a store that needs a client, credentials, or a lookup syntax of its own
(`db.json#password`, `prod/db:password`).

## The `ProviderPlugin` trait

```rust
#[async_trait]
pub trait ProviderPlugin: Send {
    fn plugin_name(&self) -> &str;
    fn plugin_version(&self) -> &str;

    /// Defaults to ["resolve"], which the core requires.
    fn capabilities(&self) -> Vec<String> { vec!["resolve".into()] }

    async fn configure(&mut self, config: serde_json::Value) -> Result<()>;
    async fn resolve(&mut self, reference: &str) -> Result<String>;

    /// Called when the run is aborted after `start`. Default: nothing.
    async fn cancel(&mut self) {}
}
```

| Method | When called | What to do |
|---|---|---|
| `plugin_name()` | During handshake | Return the binary name, `"loadsmith-provider-{type}"` |
| `plugin_version()` | During handshake | Return `env!("CARGO_PKG_VERSION")` |
| `capabilities()` | During handshake | Keep the default `["resolve"]`; a provider without it is rejected (exit `4`) |
| `configure(config)` | After the core sends `configure` | Parse and validate the instance's `config:` (it may use `env()`/`file()`, never `secret()`); open clients, check paths. Return `Err` to reject it (exit `3`) |
| `resolve(reference)` | Once per distinct reference | Return the value, or an error (see below). The reference syntax is yours to define |
| `cancel()` | When the core sends `cancel`/`error` after `start` | Release what you hold |

`run_provider` drives the whole conversation: handshake → capabilities →
`configure` → `start` → `ready`, then a `resolve` loop until stdin closes, then
`finished`. `ping` and `resend` are answered for you. The SDK waits for
`resolve()` and the core at the same time, so a `cancel` drops an in-flight
`resolve()` future — keep `resolve()` free of state that a drop would corrupt.

### Errors are answers

A reference you cannot resolve is not the end of the conversation: the SDK
answers it with `resolved{error}` and carries on; the core then fails the run
(exit `3`) naming the instance and the reference. Pick the error code with
`ResolveFailure`:

```rust
use loadsmith_plugin_sdk::ResolveFailure;

return Err(ResolveFailure::new("NOT_FOUND", format!("no key '{key}'")).into());
```

Any other error is sent as `RESOLVE_FAILED`. Suggested codes: `NOT_FOUND`,
`ACCESS_DENIED`, `INVALID_REF`.

### Never print a value

**Never put a resolved value in an error message, a log line, or stderr.** The
core masks every value it has received, but it can only mask what it knows: a
value transformed before printing (base64, URL-encoded) is not recognised. The
SDK itself logs only reference names and counts.

## Example: an `env-file` provider

Resolves `KEY` references from a `KEY=value` file.

Plugins don't live in the loadsmith repo. Create the package in your own repo,
or as a member of
[`loadsmith-canonical-plugins`](https://github.com/loadsmith-el/loadsmith-canonical-plugins)
(see its [Plugin Anatomy](https://loadsmith-el.github.io/loadsmith-canonical-plugins/anatomy.html)
page):

```
env-file/
  Cargo.toml
  src/main.rs
```

### `Cargo.toml`

The SDK is a git dependency pinned by `rev`, and `[package.metadata.loadsmith]`
is the install metadata a release turns into the published `loadsmith-plugin.yaml`
that `loadsmith plugin install` reads.

```toml
[package]
name = "loadsmith-provider-env-file"
version = "0.1.0"
edition = "2021"

[package.metadata.loadsmith]
name     = "env-file"
summary  = "Resolves secret() values from a KEY=value file"
protocol = "^1"
provides = [
    { kind = "provider", bin = "loadsmith-provider-env-file" },
]

[[bin]]
name = "loadsmith-provider-env-file"
path = "src/main.rs"

[dependencies]
tokio        = { version = "1", features = ["full"] }
anyhow       = "1"
serde        = { version = "1", features = ["derive"] }
serde_json   = "1"
async-trait  = "0.1"
loadsmith-plugin-sdk = { git = "https://github.com/loadsmith-el/loadsmith", rev = "<commit>" }
```

(Inside `loadsmith-canonical-plugins`, use `{ workspace = true }` for these; the
workspace root pins the SDK `rev` once for every plugin.)

### `src/main.rs`

```rust
use std::collections::HashMap;

use anyhow::{Context, Result};
use async_trait::async_trait;
use loadsmith_plugin_sdk::{ProviderPlugin, ResolveFailure};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    path: String,
}

#[derive(Default)]
struct EnvFileProvider {
    values: HashMap<String, String>,
}

#[async_trait]
impl ProviderPlugin for EnvFileProvider {
    fn plugin_name(&self) -> &str {
        "loadsmith-provider-env-file"
    }
    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
        let cfg: Config = serde_json::from_value(config).context("invalid env-file config")?;
        // The path is not secret; the file's contents are — never echo them.
        let text = std::fs::read_to_string(&cfg.path)
            .with_context(|| format!("cannot read {}", cfg.path))?;
        for line in text.lines().map(str::trim) {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = line.split_once('=') {
                self.values.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
        Ok(())
    }

    async fn resolve(&mut self, reference: &str) -> Result<String> {
        self.values
            .get(reference)
            .cloned()
            .ok_or_else(|| ResolveFailure::new("NOT_FOUND", format!("no key '{reference}'")).into())
    }
}

#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_provider(EnvFileProvider::default()).await
}
```

### Build and install

```bash
cargo build --release    # → target/release/loadsmith-provider-env-file
loadsmith plugin install --binary target/release/loadsmith-provider-env-file
loadsmith validate pipeline.yaml --resolve   # runs the provider, prints nothing secret
```

## Timeouts and failures

Each instance has one budget, from spawn to `finished`:
`providers.<name>.timeout_seconds` (default 60, `0` disables it). On a timeout
the provider receives `error{TIMEOUT}`; when another instance fails, it receives
`cancel`. Either way the run stops before any data plugin starts. A crash, an
early exit or a protocol violation fails the run with exit `3`; a missing
binary exits `4`.

## The canonical `file` provider

[`loadsmith-canonical-plugins`](https://github.com/loadsmith-el/loadsmith-canonical-plugins)
ships a `file` provider (binary `loadsmith-provider-file`): the reference is
`path[#key]`, where `#key` selects a field of a JSON/YAML file (a dotted path is
fine), relative to an optional `base_dir`. It is being updated to this API.
