//! Generic, driver-agnostic TLS for loadsmith's network plugins.
//!
//! Loadsmith may legitimately know about *TLS* — it's a cross-cutting networking
//! concern shared by postgres, mysql, s3/aws and any future network plugin. It
//! must **not** know about any specific protocol. So this crate produces the
//! generic artifact — a [`rustls::ClientConfig`] — and each plugin wraps it in
//! its own driver connector (postgres: `MakeRustlsConnect`; an HTTP client for
//! s3; etc.). Nothing here depends on a database/transport driver.
//!
//! Crypto is the pure-Rust `rustls-rustcrypto` provider (multi-arch-first); the
//! provider is swappable via `CryptoProvider::install_default`.

use std::sync::{Arc, OnceLock};

use anyhow::{bail, Context, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, RootCertStore};
use serde::Deserialize;

// ── Generic TLS config ────────────────────────────────────────────────────────

/// TLS posture, shared by all network plugins.
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum TlsMode {
    /// No TLS.
    #[default]
    Disable,
    /// Encrypt if offered; no certificate checks.
    Prefer,
    /// Encrypted channel required; no certificate checks.
    Require,
    /// Verify the certificate chain against `root_cert`, but not the hostname.
    VerifyCa,
    /// Verify chain AND hostname (standard TLS).
    VerifyFull,
}

/// A generic TLS configuration block (PEM material is inline strings — load it
/// with `{{ file(...) }}` in pipeline YAML).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TlsConfig {
    #[serde(default)]
    pub mode: TlsMode,
    pub root_cert: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
}

// ── Provider ──────────────────────────────────────────────────────────────────

static PROVIDER_INSTALLED: OnceLock<()> = OnceLock::new();

/// Installs rustls-rustcrypto as the process-wide CryptoProvider. Idempotent.
pub fn install_provider() {
    PROVIDER_INSTALLED.get_or_init(|| {
        rustls_rustcrypto::provider()
            .install_default()
            .expect("failed to install rustls-rustcrypto provider");
    });
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    rustls::crypto::CryptoProvider::get_default()
        .expect("install_provider() must be called before building TLS configs")
        .clone()
}

// ── ClientConfig builder ──────────────────────────────────────────────────────

/// Builds a [`rustls::ClientConfig`] for `cfg`, or `None` for [`TlsMode::Disable`]
/// (the caller should then connect without TLS). Validates the config and
/// installs the crypto provider as needed.
pub fn client_config(cfg: &TlsConfig) -> Result<Option<ClientConfig>> {
    match cfg.mode {
        TlsMode::VerifyCa | TlsMode::VerifyFull if cfg.root_cert.is_none() => {
            bail!("tls.root_cert is required for mode verify-ca and verify-full");
        }
        _ => {}
    }
    match (&cfg.client_cert, &cfg.client_key) {
        (Some(_), None) => bail!("tls.client_key is required when tls.client_cert is set"),
        (None, Some(_)) => bail!("tls.client_cert is required when tls.client_key is set"),
        _ => {}
    }

    if cfg.mode == TlsMode::Disable {
        return Ok(None);
    }

    install_provider();
    let client_cert_key = cfg.client_cert.as_deref().zip(cfg.client_key.as_deref());

    let config = match cfg.mode {
        TlsMode::Disable => unreachable!("handled above"),
        TlsMode::Prefer | TlsMode::Require => encrypt_only(client_cert_key)?,
        TlsMode::VerifyCa => verify_ca(cfg.root_cert.as_deref().unwrap(), client_cert_key)?,
        TlsMode::VerifyFull => verify_full(cfg.root_cert.as_deref().unwrap(), client_cert_key)?,
    };
    Ok(Some(config))
}

/// Encrypt only — no certificate or hostname verification (modes `require`/`prefer`).
fn encrypt_only(client_cert_key: Option<(&str, &str)>) -> Result<ClientConfig> {
    let b = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .context("TLS protocol versions setup failed")?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier));
    finish(b, client_cert_key)
}

/// Verify the chain against a trusted CA but NOT the hostname (mode `verify-ca`).
fn verify_ca(root_cert_pem: &str, client_cert_key: Option<(&str, &str)>) -> Result<ClientConfig> {
    let root_store = load_root_certs(root_cert_pem)?;
    let inner =
        rustls::client::WebPkiServerVerifier::builder_with_provider(Arc::new(root_store), provider())
            .build()
            .map_err(|e| anyhow::anyhow!("TLS CA verifier build failed: {e}"))?;
    let b = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .context("TLS protocol versions setup failed")?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(ChainOnlyVerifier { inner }));
    finish(b, client_cert_key)
}

/// Verify chain AND hostname (mode `verify-full`).
fn verify_full(root_cert_pem: &str, client_cert_key: Option<(&str, &str)>) -> Result<ClientConfig> {
    let root_store = load_root_certs(root_cert_pem)?;
    let b = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .context("TLS protocol versions setup failed")?
        .with_root_certificates(root_store);
    finish(b, client_cert_key)
}

/// Applies optional mTLS client auth, completing the config.
fn finish(
    b: rustls::ConfigBuilder<ClientConfig, rustls::client::WantsClientCert>,
    client_cert_key: Option<(&str, &str)>,
) -> Result<ClientConfig> {
    Ok(if let Some((cert_pem, key_pem)) = client_cert_key {
        let (certs, key) = load_client_cert_key(cert_pem, key_pem)?;
        b.with_client_auth_cert(certs, key).context("mTLS client cert/key load failed")?
    } else {
        b.with_no_client_auth()
    })
}

fn load_root_certs(pem: &str) -> Result<RootCertStore> {
    let mut store = RootCertStore::empty();
    let mut cursor = std::io::Cursor::new(pem.as_bytes());
    let mut count = 0usize;
    for cert in rustls_pemfile::certs(&mut cursor) {
        store.add(cert.context("invalid PEM in root_cert")?).context("failed to add root cert")?;
        count += 1;
    }
    anyhow::ensure!(count > 0, "root_cert contains no valid PEM certificates");
    Ok(store)
}

fn load_client_cert_key(
    cert_pem: &str,
    key_pem: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let certs: Vec<CertificateDer<'_>> =
        rustls_pemfile::certs(&mut std::io::Cursor::new(cert_pem.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
            .context("invalid PEM in client_cert")?;
    anyhow::ensure!(!certs.is_empty(), "client_cert contains no valid certificates");

    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(key_pem.as_bytes()))
        .context("invalid PEM in client_key")?
        .ok_or_else(|| anyhow::anyhow!("no private key found in client_key"))?;

    Ok((certs, key))
}

// ── Certificate verifiers ─────────────────────────────────────────────────────

/// Skips certificate verification entirely (encrypts channel only).
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        let p = provider_or_err()?;
        rustls::crypto::verify_tls12_signature(message, cert, dss, &p.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        let p = provider_or_err()?;
        rustls::crypto::verify_tls13_signature(message, cert, dss, &p.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::CryptoProvider::get_default()
            .map(|p| p.signature_verification_algorithms.supported_schemes())
            .unwrap_or_default()
    }
}

/// Verifies the certificate chain against a trusted CA but accepts any hostname.
#[derive(Debug)]
struct ChainOnlyVerifier {
    inner: Arc<rustls::client::WebPkiServerVerifier>,
}

impl rustls::client::danger::ServerCertVerifier for ChainOnlyVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        match self
            .inner
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
        {
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName)) => {
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

fn provider_or_err() -> Result<Arc<rustls::crypto::CryptoProvider>, rustls::Error> {
    rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .ok_or_else(|| rustls::Error::General("no crypto provider".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disable_yields_no_config() {
        let cfg = TlsConfig { mode: TlsMode::Disable, ..Default::default() };
        assert!(client_config(&cfg).unwrap().is_none());
    }

    #[test]
    fn require_yields_a_config() {
        let cfg = TlsConfig { mode: TlsMode::Require, ..Default::default() };
        assert!(client_config(&cfg).unwrap().is_some());
    }

    #[test]
    fn verify_ca_requires_root_cert() {
        let cfg = TlsConfig { mode: TlsMode::VerifyCa, ..Default::default() };
        assert!(client_config(&cfg).is_err());
    }

    #[test]
    fn client_cert_without_key_is_rejected() {
        let cfg = TlsConfig {
            mode: TlsMode::Require,
            client_cert: Some("PEM".into()),
            client_key: None,
            ..Default::default()
        };
        assert!(client_config(&cfg).is_err());
    }

    #[test]
    fn mode_deserializes_kebab_case() {
        let m: TlsMode = serde_json::from_value(serde_json::json!("verify-full")).unwrap();
        assert_eq!(m, TlsMode::VerifyFull);
    }
}
