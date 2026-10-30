//! AWS plumbing for Loadsmith, without the AWS SDK: the credential chain
//! ([`credentials`]), SigV4 request signing ([`sigv4`]) and a minimal S3
//! object client ([`s3`]). Shared by the core (`run s3://…`, the `s3` state
//! backend) and the AWS plugins.
//!
//! Pure Rust: RustCrypto `hmac`/`sha2` for the signatures and rustls with the
//! installed `rustls-rustcrypto` provider for TLS — call
//! [`loadsmith_tls::install_provider`] before the first request.

use std::time::Duration;

use anyhow::{Context, Result};

pub mod credentials;
pub mod s3;
pub mod sigv4;

pub use credentials::{Credentials, Explicit, Provider};

pub(crate) fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// The region to use: `explicit`, then `AWS_REGION`, then
/// `AWS_DEFAULT_REGION`, then `us-east-1`.
pub fn region(explicit: Option<&str>) -> String {
    explicit
        .map(str::to_string)
        .filter(|r| !r.trim().is_empty())
        .or_else(|| env_var("AWS_REGION"))
        .or_else(|| env_var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|| "us-east-1".to_string())
}

/// An HTTP client for AWS APIs: rustls on the installed provider, the
/// public web roots, `timeout` per request.
pub fn http_client(timeout: Duration) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .tls_backend_preconfigured(loadsmith_tls::public_web_config(&[b"h2", b"http/1.1"])?)
        .timeout(timeout)
        .build()
        .context("failed to build the HTTP client")
}
