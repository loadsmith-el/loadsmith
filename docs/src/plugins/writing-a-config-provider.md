# Writing a Config Provider

A config provider is a standalone Rust binary that loads configuration content
from a URI and returns it as raw bytes. The intended use is fetching pipeline
YAML from external locations — S3 buckets, HTTP endpoints, secret managers, and
similar.

> **Planned — not yet wired into the core.** The SDK trait (`ConfigProviderPlugin`,
> `run_config_provider`) and the canonical `file` provider exist and can be
> installed, but `loadsmith run` never spawns or calls a config provider, and no
> pipeline or CLI option takes a config URI yet. The core-side behavior described
> on this page is the planned design.

## Know the boundary first

Two mechanisms exist for getting external values into a pipeline. They are
distinct and serve different purposes:

**Config providers** (planned) — load the entire pipeline YAML (or a fragment)
from a URI. The core would call the provider, receive the raw bytes of the YAML
document, then parse it normally. Example: a `s3://my-bucket/pipelines/orders.yaml`
URI.

**Secret resolution** — inline template substitution via `{{ env('VAR') }}` or
`{{ file('/path') }}` in any pipeline field. The core resolves these templates
before sending config to plugins. No config provider is involved.

If you want to pull a secret value into a specific pipeline field (like a
password), use `{{ env('PG_PASSWORD') }}`. If you want to load the entire
pipeline document from S3, write a config provider.

## The `ConfigProviderPlugin` trait

```rust
#[async_trait]
pub trait ConfigProviderPlugin: Send {
    fn plugin_name(&self) -> &str;
    fn plugin_version(&self) -> &str;

    async fn configure(&mut self, config: serde_json::Value) -> Result<()>;
    async fn fetch(&mut self) -> Result<Vec<u8>>;
}
```

| Method | When called | What to do |
|---|---|---|
| `plugin_name()` | During handshake | Return `"loadsmith-config-provider-{type}"` — the binary name, conventionally named after the scheme it handles (not enforced by the core) |
| `plugin_version()` | During handshake | Return the semver version string |
| `configure(config)` | After protocol negotiation | Validate the URI scheme. Store the location. Do not fetch yet |
| `fetch()` | After `start` | Fetch and return the raw bytes of the configuration content |

## The `file://` reference implementation

The simplest config provider is the canonical `file` provider
(`loadsmith-canonical-plugins/file`, installed with `loadsmith plugin install
file`), which handles `file://` URIs. Its entire logic:

```rust
async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
    let cfg: FileConfig = serde_json::from_value(config)?;
    if !cfg.uri.starts_with("file://") {
        anyhow::bail!("unsupported URI scheme — expected file://");
    }
    self.path = Some(PathBuf::from(cfg.uri.strip_prefix("file://").unwrap()));
    Ok(())
}

async fn fetch(&mut self) -> Result<Vec<u8>> {
    let path = self.path.as_ref().unwrap();
    Ok(std::fs::read(path)?)
}
```

## Step-by-step example: HTTP config provider

Plugins don't live in the loadsmith repo. Create the package in your own repo,
or as a member of
[`loadsmith-canonical-plugins`](https://github.com/loadsmith-el/loadsmith-canonical-plugins)
(see its [Plugin Anatomy](https://loadsmith-el.github.io/loadsmith-canonical-plugins/anatomy.html)
page):

```
http/
  Cargo.toml
  src/main.rs
```

### `Cargo.toml`

The SDK is a git dependency pinned by `rev`, and `[package.metadata.loadsmith]`
is the install metadata a release turns into the published `loadsmith-plugin.yaml`
that `loadsmith plugin install` reads.

```toml
[package]
name = "loadsmith-config-provider-http"
version = "0.1.0"
edition = "2021"

[package.metadata.loadsmith]
name     = "http"
summary  = "HTTP config provider"
protocol = "^1"
provides = [
    { kind = "config-provider", bin = "loadsmith-config-provider-http" },
]

[[bin]]
name = "loadsmith-config-provider-http"
path = "src/main.rs"

[dependencies]
tokio        = { version = "1", features = ["full"] }
anyhow       = "1"
tracing      = "0.1"
serde        = { version = "1", features = ["derive"] }
serde_json   = "1"
async-trait  = "0.1"
loadsmith-plugin-sdk = { git = "https://github.com/loadsmith-el/loadsmith", rev = "<commit>" }

reqwest = { version = "0.12", features = ["rustls-tls"] }
```

(Inside `loadsmith-canonical-plugins`, use `{ workspace = true }` for these; the
workspace root pins the SDK `rev` once for every plugin.)

### `src/main.rs`

```rust
use anyhow::Result;
use async_trait::async_trait;
use loadsmith_plugin_sdk::ConfigProviderPlugin;
use serde::Deserialize;

#[derive(Deserialize)]
struct HttpConfig {
    uri: String,
    #[serde(default)]
    headers: std::collections::HashMap<String, String>,
}

struct HttpProvider {
    uri: Option<String>,
    headers: std::collections::HashMap<String, String>,
}

impl HttpProvider {
    pub fn new() -> Self {
        Self { uri: None, headers: Default::default() }
    }
}

#[async_trait]
impl ConfigProviderPlugin for HttpProvider {
    fn plugin_name(&self) -> &str {
        "loadsmith-config-provider-http"
    }

    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
        let cfg: HttpConfig = serde_json::from_value(config)?;
        if !cfg.uri.starts_with("https://") && !cfg.uri.starts_with("http://") {
            anyhow::bail!("unsupported URI scheme — expected http:// or https://");
        }
        self.uri = Some(cfg.uri);
        self.headers = cfg.headers;
        Ok(())
    }

    async fn fetch(&mut self) -> Result<Vec<u8>> {
        let mut req = reqwest::Client::new()
            .get(self.uri.as_ref().unwrap());

        for (k, v) in &self.headers {
            req = req.header(k, v);
        }

        let bytes = req.send().await?.error_for_status()?.bytes().await?;
        Ok(bytes.to_vec())
    }
}

#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_config_provider(HttpProvider::new()).await
}
```

### Build

```bash
cargo build --release    # → target/release/loadsmith-config-provider-http
```

## Validate the scheme in `configure()`

A config provider must reject URIs whose scheme it doesn't own. If it is handed
`uri: s3://my-bucket/pipeline.yaml`, the HTTP provider should fail immediately in
`configure()` rather than making a nonsensical HTTP request.

```rust
async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
    if !cfg.uri.starts_with("https://") {
        anyhow::bail!("this provider only handles https:// URIs");
    }
    Ok(())
}
```

## `fetch()` returns raw bytes

Return the raw bytes of the YAML document. Parsing is the caller's job (the core,
once integration lands) — the provider does not need to understand the YAML
structure. The SDK sends the bytes back in `finished.message` as a UTF-8 string.

Config provider payloads are expected to be small (a pipeline YAML is kilobytes,
not gigabytes). There is no streaming; `fetch()` returns `Vec<u8>` and the whole
document is buffered.
