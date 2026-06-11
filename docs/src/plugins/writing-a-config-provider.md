# Writing a Config Provider

A config provider is a standalone Rust binary that loads configuration content
from a URI and returns it as raw bytes. The core uses config providers to fetch
pipeline YAML from external locations — S3 buckets, HTTP endpoints, secret
managers, and similar.

## Know the boundary first

Two mechanisms exist for getting external values into a pipeline. They are
distinct and serve different purposes:

**Config providers** — load the entire pipeline YAML (or a fragment) from a URI.
The core calls the provider and receives the raw bytes of the YAML document, then
parses it normally. Example: a `s3://my-bucket/pipelines/orders.yaml` URI.

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
| `plugin_name()` | During handshake | Return `"loadsmith-config-provider-{scheme}"` exactly |
| `plugin_version()` | During handshake | Return the semver version string |
| `configure(config)` | After protocol negotiation | Validate the URI scheme. Store the location. Do not fetch yet |
| `fetch()` | After `start` | Fetch and return the raw bytes of the configuration content |

## The `file://` reference implementation

The simplest config provider is the built-in `file://` one. Its entire logic:

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

```
plugins/config-providers/http/
  Cargo.toml
  src/main.rs
```

### `Cargo.toml`

```toml
[package]
name = "loadsmith-config-provider-http"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "loadsmith-config-provider-http"
path = "src/main.rs"

[dependencies]
tokio        = { workspace = true }
anyhow       = { workspace = true }
tracing      = { workspace = true }
serde        = { workspace = true }
serde_json   = { workspace = true }
async-trait  = { workspace = true }
loadsmith-plugin-sdk = { path = "../../../crates/loadsmith-plugin-sdk" }

reqwest = { version = "0.12", features = ["rustls-tls"] }
```

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

### Register in workspace root

```toml
members = [
    # ...
    "plugins/config-providers/http",
]
```

### Build

```bash
cargo build -p loadsmith-config-provider-http
```

## Validate the scheme in `configure()`

A config provider must reject URIs whose scheme it doesn't own. If the pipeline
declares `uri: s3://my-bucket/pipeline.yaml` but the binary resolved by the core
is `loadsmith-config-provider-http`, the HTTP provider should fail immediately in
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

Return the raw bytes of the YAML document. The core handles parsing — the
provider does not need to understand the YAML structure.

Config provider payloads are expected to be small (a pipeline YAML is kilobytes,
not gigabytes). There is no streaming; `fetch()` returns `Vec<u8>` and the core
buffers the entire document.
