//! A minimal S3 object client: whole-object `GET`, `PUT` (optionally
//! conditional) and `DELETE`, against AWS or an S3-compatible store. For small
//! documents — a pipeline file, a state document, a lock — not bulk data (the
//! s3 sink does multipart uploads on its own).
//!
//! Every request is signed anew for each attempt and retried on network
//! errors, `429`, `5xx` and `409` (S3's answer to concurrent conditional
//! writes on one key), with 1s, 2s, 4s, … between attempts.

use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use reqwest::{Client, Method, StatusCode, Url};

use crate::credentials::{Explicit, Provider};
use crate::sigv4;

/// `s3://bucket/key`, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Uri {
    pub bucket: String,
    pub key: String,
}

impl S3Uri {
    /// `None` when `text` is not an `s3://` URI; an error when it is one
    /// without a bucket or a key.
    pub fn parse(text: &str) -> Option<Result<Self>> {
        let rest = text.strip_prefix("s3://")?;
        Some(match rest.split_once('/') {
            Some((bucket, key)) if !bucket.is_empty() && !key.is_empty() => {
                Ok(Self { bucket: bucket.to_string(), key: key.to_string() })
            }
            _ => Err(anyhow::anyhow!("`{text}` is not an S3 object URI (expected s3://<bucket>/<key>)")),
        })
    }
}

impl std::fmt::Display for S3Uri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s3://{}/{}", self.bucket, self.key)
    }
}

/// How object URLs are formed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UrlStyle {
    /// `https://<bucket>.<host>/<key>` — AWS's default.
    VirtualHost,
    /// `https://<host>/<bucket>/<key>` — what most S3-compatible stores need.
    Path,
}

/// Where and how to reach a bucket.
#[derive(Debug, Clone, Default)]
pub struct Settings {
    pub bucket: String,
    /// Default [`crate::region`].
    pub region: Option<String>,
    /// An S3-compatible endpoint (`http://minio:9000`). Default: AWS,
    /// `https://s3.<region>.amazonaws.com` (or `AWS_ENDPOINT_URL_S3`).
    pub endpoint: Option<String>,
    /// Default `VirtualHost` on AWS, `Path` with an endpoint.
    pub url_style: Option<UrlStyle>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    /// Limit for one request. Default 60s.
    pub request_timeout: Option<Duration>,
    /// Tries per request. Default 4.
    pub max_attempts: Option<u32>,
}

/// A condition on a `PUT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Precondition {
    None,
    /// Only if no object exists at the key (`If-None-Match: *`).
    Absent,
    /// Only if the object still has this ETag (`If-Match`).
    Matches(String),
}

/// What a `PUT` did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Put {
    /// Written; the new object's ETag.
    Written(String),
    /// Its [`Precondition`] did not hold (`412`); nothing was written.
    PreconditionFailed,
}

/// An object read whole.
#[derive(Debug, Clone)]
pub struct Object {
    pub body: Vec<u8>,
    pub etag: String,
}

pub struct S3Client {
    bucket: String,
    region: String,
    endpoint: Url,
    style: UrlStyle,
    credentials: Provider,
    http: Client,
    max_attempts: u32,
}

impl S3Client {
    /// Resolves the region, endpoint and credentials (the chain, unless keys
    /// are set) — failing here, before any request, when there are none.
    pub async fn new(settings: Settings) -> Result<Self> {
        if settings.bucket.trim().is_empty() {
            bail!("the S3 bucket is empty");
        }
        let region = crate::region(settings.region.as_deref());
        let explicit_endpoint = settings.endpoint.clone().or_else(|| crate::env_var("AWS_ENDPOINT_URL_S3"));
        let (endpoint, default_style) = match explicit_endpoint {
            Some(e) => (e, UrlStyle::Path),
            None => (format!("https://s3.{region}.amazonaws.com"), UrlStyle::VirtualHost),
        };
        let endpoint: Url = endpoint.parse().with_context(|| format!("S3 endpoint {endpoint:?} is not a URL"))?;
        let explicit = Explicit {
            access_key_id: settings.access_key_id.as_deref(),
            secret_access_key: settings.secret_access_key.as_deref(),
            session_token: settings.session_token.as_deref(),
        };
        let credentials = Provider::resolve(explicit, &region).await?;
        let http = crate::http_client(settings.request_timeout.unwrap_or(Duration::from_secs(60)))?;
        let max_attempts = settings.max_attempts.unwrap_or(4).max(1);
        Ok(Self {
            bucket: settings.bucket,
            region,
            endpoint,
            style: settings.url_style.unwrap_or(default_style),
            credentials,
            http,
            max_attempts,
        })
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    fn object_url(&self, key: &str) -> Result<Url> {
        let key = sigv4::uri_encode(key, true);
        let base = self.endpoint.as_str().trim_end_matches('/');
        let url = match self.style {
            UrlStyle::Path => format!("{base}/{}/{key}", sigv4::uri_encode(&self.bucket, false)),
            UrlStyle::VirtualHost => {
                let host = self.endpoint.host_str().context("the S3 endpoint has no host")?;
                let port = self.endpoint.port().map(|p| format!(":{p}")).unwrap_or_default();
                format!("{}://{}.{host}{port}/{key}", self.endpoint.scheme(), self.bucket)
            }
        };
        url.parse().with_context(|| format!("object URL {url:?}"))
    }

    /// Sends one signed request, retrying what may pass on another try.
    /// `accept` names the non-2xx statuses returned to the caller instead of
    /// failing.
    async fn send(
        &self,
        method: Method,
        key: &str,
        extra: &[(&str, String)],
        body: &[u8],
        accept: &[StatusCode],
    ) -> Result<reqwest::Response> {
        let url = self.object_url(key)?;
        let what = format!("{method} s3://{}/{key}", self.bucket);
        let payload = sigv4::sha256_hex(body);
        let mut attempt = 1;
        loop {
            let credentials = self.credentials.get().await?;
            let mut headers: Vec<(String, String)> = extra.iter().map(|(k, v)| (k.to_string(), v.clone())).collect();
            headers.push(("x-amz-content-sha256".into(), payload.clone()));
            sigv4::sign(
                sigv4::Request { method: method.as_str(), url: &url, headers: &mut headers, payload_sha256: &payload },
                &credentials,
                &self.region,
                "s3",
                SystemTime::now(),
            );
            let mut req = self.http.request(method.clone(), url.clone());
            for (k, v) in &headers {
                if k != "host" {
                    req = req.header(k.as_str(), v.as_str());
                }
            }
            let outcome = req.body(body.to_vec()).send().await;
            let retryable = match &outcome {
                Ok(r) if r.status().is_success() || accept.contains(&r.status()) => {
                    return Ok(outcome.expect("checked Ok"));
                }
                Ok(r) => {
                    r.status() == StatusCode::TOO_MANY_REQUESTS
                        || r.status() == StatusCode::CONFLICT
                        || r.status().is_server_error()
                }
                Err(e) => !e.is_builder(),
            };
            if !retryable || attempt >= self.max_attempts {
                return Err(match outcome {
                    Ok(r) => {
                        let status = r.status();
                        let body = r.text().await.unwrap_or_default();
                        anyhow::anyhow!("{what}: HTTP {status}: {}", error_message(&body))
                    }
                    Err(e) => anyhow::anyhow!("{what}: {e}"),
                });
            }
            let wait = Duration::from_secs(1 << (attempt - 1).min(5));
            tracing::warn!(attempt, max_attempts = self.max_attempts, "{what} failed, retrying in {wait:?}");
            tokio::time::sleep(wait).await;
            attempt += 1;
        }
    }

    /// The object at `key`, or `None` when there is none.
    pub async fn get(&self, key: &str) -> Result<Option<Object>> {
        let resp = self.send(Method::GET, key, &[], &[], &[StatusCode::NOT_FOUND]).await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let etag = etag(&resp);
        let body = resp.bytes().await.with_context(|| format!("reading s3://{}/{key}", self.bucket))?;
        Ok(Some(Object { body: body.to_vec(), etag }))
    }

    /// Writes `body` at `key` when `condition` holds.
    pub async fn put(&self, key: &str, body: &[u8], condition: Precondition) -> Result<Put> {
        let extra: Vec<(&str, String)> = match condition {
            Precondition::None => vec![],
            Precondition::Absent => vec![("if-none-match", "*".to_string())],
            Precondition::Matches(etag) => vec![("if-match", etag)],
        };
        let resp = self.send(Method::PUT, key, &extra, body, &[StatusCode::PRECONDITION_FAILED]).await?;
        if resp.status() == StatusCode::PRECONDITION_FAILED {
            return Ok(Put::PreconditionFailed);
        }
        Ok(Put::Written(etag(&resp)))
    }

    /// Deletes the object at `key` (no error when there is none).
    pub async fn delete(&self, key: &str) -> Result<()> {
        self.send(Method::DELETE, key, &[], &[], &[StatusCode::NOT_FOUND]).await?;
        Ok(())
    }
}

fn etag(resp: &reqwest::Response) -> String {
    resp.headers().get("etag").and_then(|v| v.to_str().ok()).unwrap_or_default().to_string()
}

/// The text of the first `<name>…</name>` element of an S3/STS XML body.
pub fn xml_element(body: &str, name: &str) -> Option<String> {
    let open = format!("<{name}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&format!("</{name}>"))? + start;
    Some(body[start..end].trim().to_string())
}

/// The `Code: Message` of an S3 XML error body, or the body itself (trimmed).
pub fn error_message(body: &str) -> String {
    let tag = |name: &str| xml_element(body, name);
    match (tag("Code"), tag("Message")) {
        (Some(code), Some(msg)) => format!("{code}: {msg}"),
        (Some(code), None) => code,
        _ => body.trim().chars().take(300).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;

    async fn client(server: &MockServer) -> S3Client {
        loadsmith_tls::install_provider();
        S3Client::new(Settings {
            bucket: "lake".into(),
            region: Some("us-east-1".into()),
            endpoint: Some(server.base_url()),
            access_key_id: Some("AKID".into()),
            secret_access_key: Some("secret".into()),
            max_attempts: Some(2),
            ..Default::default()
        })
        .await
        .unwrap()
    }

    #[test]
    fn s3_uris_are_parsed() {
        assert_eq!(S3Uri::parse("s3://b/p/x.yaml").unwrap().unwrap(), S3Uri { bucket: "b".into(), key: "p/x.yaml".into() });
        assert!(S3Uri::parse("s3://b").unwrap().is_err());
        assert!(S3Uri::parse("s3:///k").unwrap().is_err());
        assert!(S3Uri::parse("pipeline.yaml").is_none());
    }

    #[tokio::test]
    async fn get_reads_the_object_and_its_etag_or_none() {
        let server = MockServer::start_async().await;
        let hit = server
            .mock_async(|when, then| {
                when.method(GET).path("/lake/pipelines/a%20b.yaml").header_exists("authorization");
                then.status(200).header("etag", "\"e1\"").body("pipeline: {}");
            })
            .await;
        server
            .mock_async(|when, then| {
                when.method(GET).path("/lake/missing");
                then.status(404).body("<Error><Code>NoSuchKey</Code></Error>");
            })
            .await;
        let c = client(&server).await;
        let o = c.get("pipelines/a b.yaml").await.unwrap().unwrap();
        assert_eq!((o.body.as_slice(), o.etag.as_str()), (b"pipeline: {}".as_slice(), "\"e1\""));
        assert!(c.get("missing").await.unwrap().is_none());
        hit.assert_async().await;
    }

    #[tokio::test]
    async fn conditional_puts_send_their_condition_and_report_412() {
        let server = MockServer::start_async().await;
        let create = server
            .mock_async(|when, then| {
                when.method(PUT).path("/lake/l").header("if-none-match", "*");
                then.status(412);
            })
            .await;
        let swap = server
            .mock_async(|when, then| {
                when.method(PUT).path("/lake/l").header("if-match", "\"e1\"").body("x");
                then.status(200).header("etag", "\"e2\"");
            })
            .await;
        let c = client(&server).await;
        assert_eq!(c.put("l", b"x", Precondition::Absent).await.unwrap(), Put::PreconditionFailed);
        assert_eq!(c.put("l", b"x", Precondition::Matches("\"e1\"".into())).await.unwrap(), Put::Written("\"e2\"".into()));
        create.assert_async().await;
        swap.assert_async().await;
    }

    #[tokio::test]
    async fn errors_are_retried_then_reported() {
        let server = MockServer::start_async().await;
        let put = server
            .mock_async(|when, then| {
                when.method(PUT);
                then.status(503).body("<Error><Code>SlowDown</Code><Message>Reduce your request rate.</Message></Error>");
            })
            .await;
        let denied = server
            .mock_async(|when, then| {
                when.method(GET);
                then.status(403).body("<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>");
            })
            .await;
        let c = client(&server).await;
        let e = format!("{:#}", c.put("k", b"x", Precondition::None).await.unwrap_err());
        assert!(e.contains("SlowDown: Reduce your request rate."), "{e}");
        put.assert_hits_async(2).await;
        assert!(format!("{:#}", c.get("k").await.unwrap_err()).contains("AccessDenied"));
        denied.assert_hits_async(1).await;
    }

    #[tokio::test]
    async fn virtual_host_urls_put_the_bucket_in_the_host() {
        loadsmith_tls::install_provider();
        let c = S3Client::new(Settings {
            bucket: "lake".into(),
            region: Some("sa-east-1".into()),
            access_key_id: Some("a".into()),
            secret_access_key: Some("s".into()),
            ..Default::default()
        })
        .await
        .unwrap();
        assert_eq!(c.object_url("a/b c").unwrap().as_str(), "https://lake.s3.sa-east-1.amazonaws.com/a/b%20c");
    }
}
