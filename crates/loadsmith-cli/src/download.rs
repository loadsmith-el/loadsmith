//! A minimal, dependency-light fetcher for plugin artifacts.
//!
//! Supports `file://` (and bare local paths via the caller), `http://`, and
//! `https://`. TLS is **pure-Rust** (`rustls` + the `rustls-rustcrypto`
//! provider installed by `loadsmith-tls`) — the project bans native/per-arch
//! crypto (ring/aws-lc/native-tls) so the release image builds clean on both
//! amd64 and arm64 (see the repo's multi-arch-and-tls doc). HTTP is HTTP/1.1
//! with `Connection: close`: we read the body to EOF, handling redirects and
//! chunked transfer-encoding.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};

const MAX_REDIRECTS: usize = 6;

/// Fetch the full body at `url` (following redirects). Schemes: `file`, `http`,
/// `https`.
pub fn fetch(url: &str) -> Result<Vec<u8>> {
    let mut current = url.to_string();
    for _ in 0..MAX_REDIRECTS {
        let parsed = Url::parse(&current)?;
        match parsed.scheme.as_str() {
            "file" => {
                return std::fs::read(&parsed.target)
                    .with_context(|| format!("reading {}", parsed.target));
            }
            "http" | "https" => {
                let resp = http_get(&parsed)?;
                if let Some(loc) = resp.redirect {
                    current = resolve_redirect(&parsed, &loc)?;
                    continue;
                }
                if !(200..300).contains(&resp.status) {
                    bail!("GET {current} → HTTP {}", resp.status);
                }
                return Ok(resp.body);
            }
            other => bail!("unsupported URL scheme {other:?} in {current}"),
        }
    }
    bail!("too many redirects fetching {url}")
}

struct Response {
    status: u16,
    redirect: Option<String>,
    body: Vec<u8>,
}

fn http_get(url: &Url) -> Result<Response> {
    let addr = format!("{}:{}", url.host, url.port);
    let tcp = TcpStream::connect(&addr).with_context(|| format!("connecting to {addr}"))?;
    if url.scheme == "https" {
        let mut tls = tls_stream(tcp, &url.host)?;
        exchange(&mut tls, &url.host, &url.target)
    } else {
        let mut tcp = tcp;
        exchange(&mut tcp, &url.host, &url.target)
    }
}

fn exchange<S: Read + Write>(stream: &mut S, host: &str, target: &str) -> Result<Response> {
    let req = format!(
        "GET {target} HTTP/1.1\r\n\
         Host: {host}\r\n\
         User-Agent: loadsmith\r\n\
         Accept: */*\r\n\
         Connection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes())?;
    stream.flush()?;

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;

    let split = find_subslice(&raw, b"\r\n\r\n")
        .ok_or_else(|| anyhow!("malformed HTTP response (no header terminator)"))?;
    let head = std::str::from_utf8(&raw[..split]).context("non-UTF8 HTTP headers")?;
    let body = &raw[split + 4..];

    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status = parse_status(status_line)?;

    let mut location = None;
    let mut chunked = false;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let (k, v) = (k.trim().to_ascii_lowercase(), v.trim());
            match k.as_str() {
                "location" => location = Some(v.to_string()),
                "transfer-encoding" if v.eq_ignore_ascii_case("chunked") => chunked = true,
                _ => {}
            }
        }
    }

    if (300..400).contains(&status) {
        if let Some(loc) = location {
            return Ok(Response { status, redirect: Some(loc), body: Vec::new() });
        }
    }

    let body = if chunked { dechunk(body)? } else { body.to_vec() };
    Ok(Response { status, redirect: None, body })
}

fn parse_status(line: &str) -> Result<u16> {
    // "HTTP/1.1 302 Found"
    line.split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| anyhow!("malformed HTTP status line {line:?}"))
}

/// Decode HTTP/1.1 chunked transfer-encoding.
fn dechunk(mut data: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let nl = find_subslice(data, b"\r\n").ok_or_else(|| anyhow!("truncated chunk size"))?;
        let size_str = std::str::from_utf8(&data[..nl]).context("bad chunk size")?;
        // ignore any chunk extensions after ';'
        let size_hex = size_str.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16).context("invalid chunk size")?;
        data = &data[nl + 2..];
        if size == 0 {
            break;
        }
        if data.len() < size {
            bail!("truncated chunk body");
        }
        out.extend_from_slice(&data[..size]);
        data = &data[size..];
        // skip trailing CRLF after the chunk
        if data.starts_with(b"\r\n") {
            data = &data[2..];
        }
    }
    Ok(out)
}

// ── TLS (pure-Rust rustls + rustcrypto provider) ────────────────────────────

fn tls_stream(tcp: TcpStream, host: &str) -> Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>> {
    let config = client_config()?;
    let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
        .with_context(|| format!("invalid TLS server name {host:?}"))?;
    let conn = rustls::ClientConnection::new(Arc::new(config), server_name)
        .context("starting TLS session")?;
    Ok(rustls::StreamOwned::new(conn, tcp))
}

fn client_config() -> Result<rustls::ClientConfig> {
    // Ensure the pure-Rust crypto provider is the process default.
    loadsmith_tls::install_provider();
    let provider = rustls::crypto::CryptoProvider::get_default()
        .ok_or_else(|| anyhow!("no rustls crypto provider installed"))?
        .clone();

    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("rustls protocol versions")?
        .with_root_certificates(roots)
        .with_no_client_auth()
        .pipe(Ok)
}

// ── tiny URL parser ─────────────────────────────────────────────────────────

struct Url {
    scheme: String,
    host: String,
    port: u16,
    /// Request target: path + query (`/a/b?c=d`), or the filesystem path for `file://`.
    target: String,
}

impl Url {
    fn parse(s: &str) -> Result<Url> {
        let (scheme, rest) = s
            .split_once("://")
            .ok_or_else(|| anyhow!("not an absolute URL: {s:?}"))?;
        let scheme = scheme.to_ascii_lowercase();

        if scheme == "file" {
            // file:///abs/path → /abs/path
            let path = rest.strip_prefix('/').map(|p| format!("/{p}")).unwrap_or_else(|| rest.to_string());
            return Ok(Url { scheme, host: String::new(), port: 0, target: path });
        }

        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, "/"),
        };
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse().context("invalid port")?),
            None => (
                authority.to_string(),
                if scheme == "https" { 443 } else { 80 },
            ),
        };
        Ok(Url { scheme, host, port, target: path.to_string() })
    }

    fn origin(&self) -> String {
        format!("{}://{}:{}", self.scheme, self.host, self.port)
    }
}

/// Resolve a (possibly relative) redirect `Location` against the request URL.
fn resolve_redirect(base: &Url, location: &str) -> Result<String> {
    if location.contains("://") {
        Ok(location.to_string())
    } else if let Some(abs_path) = location.strip_prefix('/') {
        Ok(format!("{}/{}", base.origin(), abs_path))
    } else {
        bail!("unsupported relative redirect {location:?}")
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Tiny `.pipe()` helper so the rustls builder chain reads top-to-bottom.
trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}
impl<T> Pipe for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_https_url() {
        let u = Url::parse("https://github.com/loadsmith-el/x/releases/download/v1/a.tar.gz?token=abc").unwrap();
        assert_eq!(u.scheme, "https");
        assert_eq!(u.host, "github.com");
        assert_eq!(u.port, 443);
        assert_eq!(u.target, "/loadsmith-el/x/releases/download/v1/a.tar.gz?token=abc");
    }

    #[test]
    fn parses_http_with_port() {
        let u = Url::parse("http://localhost:8080/index.json").unwrap();
        assert_eq!(u.port, 8080);
        assert_eq!(u.target, "/index.json");
    }

    #[test]
    fn parses_file_url() {
        let u = Url::parse("file:///tmp/x.tar.gz").unwrap();
        assert_eq!(u.scheme, "file");
        assert_eq!(u.target, "/tmp/x.tar.gz");
    }

    #[test]
    fn fetches_file_url() {
        let dir = std::env::temp_dir();
        let p = dir.join("loadsmith-download-test.txt");
        std::fs::write(&p, b"hello").unwrap();
        let got = fetch(&format!("file://{}", p.display())).unwrap();
        assert_eq!(got, b"hello");
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn dechunk_decodes() {
        // "Wiki" + "pedia" chunked
        let raw = b"4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n";
        assert_eq!(dechunk(raw).unwrap(), b"Wikipedia");
    }

    #[test]
    fn resolves_cross_host_redirect() {
        let base = Url::parse("https://github.com/a/b").unwrap();
        let abs = resolve_redirect(&base, "https://cdn.example.com/x?sig=1").unwrap();
        assert_eq!(abs, "https://cdn.example.com/x?sig=1");
        let rel = resolve_redirect(&base, "/c/d").unwrap();
        assert_eq!(rel, "https://github.com:443/c/d");
    }
}
