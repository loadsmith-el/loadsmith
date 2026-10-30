//! AWS Signature Version 4, header form: signs one request in place by adding
//! `host`, `x-amz-date`, `x-amz-security-token` (with temporary credentials)
//! and `authorization`.
//!
//! Every header the caller passes is signed. The URL path must already be
//! encoded the way it goes on the wire ([`uri_encode`]); it is used verbatim
//! as the canonical URI, which is S3's rule (other services double-encode, but
//! the paths they take — `/` — are the same either way).

use std::time::SystemTime;

use hmac::{Hmac, Mac};
use reqwest::Url;
use sha2::{Digest, Sha256};

use crate::credentials::Credentials;

/// The SHA-256 of an empty payload, hex.
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Lowercase hex SHA-256 of `data`.
pub fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes any key size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// AWS's URI encoding: every byte but the unreserved `A-Za-z0-9-._~` as
/// `%XX`; `/` kept when `keep_slash` (an S3 key in a path).
pub fn uri_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => out.push(b as char),
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// What is being signed.
pub struct Request<'a> {
    pub method: &'a str,
    pub url: &'a Url,
    /// Headers sent besides `host`/`x-amz-date`/`x-amz-security-token`/
    /// `authorization` (which [`sign`] adds). All of them are signed.
    pub headers: &'a mut Vec<(String, String)>,
    /// Hex SHA-256 of the body ([`sha256_hex`], [`EMPTY_SHA256`]).
    pub payload_sha256: &'a str,
}

/// Signs `req` for `service` in `region` at `at`, adding the auth headers.
pub fn sign(req: Request<'_>, credentials: &Credentials, region: &str, service: &str, at: SystemTime) {
    let at: chrono::DateTime<chrono::Utc> = at.into();
    let amz_date = at.format("%Y%m%dT%H%M%SZ").to_string();
    let date = &amz_date[..8];

    let host = match req.url.port() {
        Some(port) => format!("{}:{port}", req.url.host_str().unwrap_or_default()),
        None => req.url.host_str().unwrap_or_default().to_string(),
    };
    req.headers.retain(|(k, _)| {
        !matches!(k.to_ascii_lowercase().as_str(), "host" | "x-amz-date" | "x-amz-security-token" | "authorization")
    });
    req.headers.push(("host".into(), host));
    req.headers.push(("x-amz-date".into(), amz_date.clone()));
    if let Some(token) = credentials.session_token() {
        req.headers.push(("x-amz-security-token".into(), token.to_string()));
    }

    let mut canonical_headers: Vec<(String, String)> = req
        .headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.split_whitespace().collect::<Vec<_>>().join(" ")))
        .collect();
    canonical_headers.sort();
    let signed_headers = canonical_headers.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(";");
    let header_block: String = canonical_headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();

    let mut query: Vec<(String, String)> =
        req.url.query_pairs().map(|(k, v)| (uri_encode(&k, false), uri_encode(&v, false))).collect();
    query.sort();
    let query = query.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
    let path = if req.url.path().is_empty() { "/" } else { req.url.path() };

    let canonical = format!(
        "{}\n{path}\n{query}\n{header_block}\n{signed_headers}\n{}",
        req.method, req.payload_sha256
    );
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}", sha256_hex(canonical.as_bytes()));

    let k_date = hmac(format!("AWS4{}", credentials.secret_access_key()).as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    let k_signing = hmac(&k_service, b"aws4_request");
    let signature = hex(&hmac(&k_signing, to_sign.as_bytes()));

    req.headers.push((
        "authorization".into(),
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            credentials.access_key_id()
        ),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    fn at(unix: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(unix)
    }

    fn authorization(headers: &[(String, String)]) -> &str {
        &headers.iter().find(|(k, _)| k == "authorization").unwrap().1
    }

    /// `get-vanilla` from AWS's SigV4 test suite.
    #[test]
    fn signs_the_test_suite_vanilla_get() {
        let creds = Credentials::new("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", None);
        let url: Url = "https://example.amazonaws.com/".parse().unwrap();
        let mut headers = Vec::new();
        let req = Request { method: "GET", url: &url, headers: &mut headers, payload_sha256: EMPTY_SHA256 };
        sign(req, &creds, "us-east-1", "service", at(1_440_938_160)); // 2015-08-30T12:36:00Z
        assert_eq!(
            authorization(&headers),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=host;x-amz-date, \
             Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    /// The `GET Object` example of the S3 SigV4 documentation.
    #[test]
    fn signs_the_s3_documentation_get_object() {
        let creds = Credentials::new("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", None);
        let url: Url = "https://examplebucket.s3.amazonaws.com/test.txt".parse().unwrap();
        let mut headers = vec![
            ("range".to_string(), "bytes=0-9".to_string()),
            ("x-amz-content-sha256".to_string(), EMPTY_SHA256.to_string()),
        ];
        let req = Request { method: "GET", url: &url, headers: &mut headers, payload_sha256: EMPTY_SHA256 };
        sign(req, &creds, "us-east-1", "s3", at(1_369_353_600)); // 2013-05-24T00:00:00Z
        assert_eq!(
            authorization(&headers),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn temporary_credentials_sign_their_token() {
        let creds = Credentials::new("ASIA", "s", Some("tok"));
        let url: Url = "https://secretsmanager.us-east-1.amazonaws.com/".parse().unwrap();
        let mut headers = vec![("x-amz-target".to_string(), "secretsmanager.GetSecretValue".to_string())];
        let req = Request { method: "POST", url: &url, headers: &mut headers, payload_sha256: EMPTY_SHA256 };
        sign(req, &creds, "us-east-1", "secretsmanager", at(0));
        assert!(headers.iter().any(|(k, v)| k == "x-amz-security-token" && v == "tok"));
        assert!(authorization(&headers).contains("SignedHeaders=host;x-amz-date;x-amz-security-token;x-amz-target"));
    }

    #[test]
    fn uri_encoding_keeps_only_unreserved_bytes() {
        assert_eq!(uri_encode("a b/ç+~", true), "a%20b/%C3%A7%2B~");
        assert_eq!(uri_encode("a/b", false), "a%2Fb");
    }
}
