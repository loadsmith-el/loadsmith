//! The `s3` state backend: the state document is an S3 object
//! (`state.path: s3://bucket/key`), the lock a sibling `<key>.lock`.
//!
//! The lock is the local backend's lease ([`crate::state::acquire`]) over
//! S3's conditional writes: it is created with `If-None-Match: *` (only one
//! run's `PUT` succeeds) and renewed or stolen with `If-Match: <ETag>` — a
//! true compare-and-swap, so two runs taking an expired lease at once cannot
//! both win. Region, endpoint (`AWS_ENDPOINT_URL_S3` for an S3-compatible
//! store, which then needs conditional writes too) and credentials come from
//! the environment, as for `run s3://…`.
//!
//! [`StateBackend`] is synchronous and is called from async code, so every
//! request runs on a small runtime of its own ([`block_on`]).

use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use loadsmith_aws::s3::{Precondition, Put, S3Client, S3Uri, Settings};

use crate::error::CoreError;
use crate::state::{acquire, parse_doc, LockFile, LockGuard, LockStore, Seen, StateBackend, StateDoc, LEASE_POLL, LEASE_TTL};

/// Runs `f` to completion on the backend's own runtime and waits for it —
/// from any thread, inside an async context or not.
fn block_on<T: Send + 'static>(f: impl Future<Output = T> + Send + 'static) -> T {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("loadsmith-state-s3")
            .enable_all()
            .build()
            .expect("the S3 state runtime starts")
    });
    let (tx, rx) = std::sync::mpsc::channel();
    runtime.spawn(async move {
        let _ = tx.send(f.await);
    });
    rx.recv().expect("an S3 state request ends with an answer")
}

fn s3_error(e: anyhow::Error) -> CoreError {
    CoreError::State(format!("{e:#}"))
}

pub struct S3Backend {
    client: Arc<S3Client>,
    uri: S3Uri,
    /// How long an unchanged lease is watched before it is taken.
    observe: Duration,
}

impl S3Backend {
    /// Resolves the client (credentials included) for `path`
    /// (`s3://bucket/key`).
    pub fn open(path: &str) -> Result<Self, CoreError> {
        Self::open_with(path, Settings::default())
    }

    fn open_with(path: &str, base: Settings) -> Result<Self, CoreError> {
        let uri = S3Uri::parse(path)
            .ok_or_else(|| CoreError::State(format!("state.path '{path}' is not an S3 object URI (s3://<bucket>/<key>)")))?
            .map_err(s3_error)?;
        loadsmith_tls::install_provider();
        let settings = Settings { bucket: uri.bucket.clone(), ..base };
        let client = block_on(S3Client::new(settings)).map_err(s3_error)?;
        Ok(Self { client: Arc::new(client), uri, observe: LEASE_TTL })
    }

    fn lock_store(&self) -> S3Lock {
        S3Lock { client: Arc::clone(&self.client), key: format!("{}.lock", self.uri.key), bucket: self.uri.bucket.clone() }
    }
}

impl StateBackend for S3Backend {
    fn lock(&self, key: &str) -> Result<LockGuard, CoreError> {
        // Each look at a held lock is a GET: poll less often than locally.
        acquire(Arc::new(self.lock_store()), key, self.observe, LEASE_POLL * 4)
    }

    fn load(&self, key: &str) -> Result<Option<StateDoc>, CoreError> {
        let (client, object) = (Arc::clone(&self.client), self.uri.key.clone());
        let Some(found) = block_on(async move { client.get(&object).await }).map_err(s3_error)? else {
            return Ok(None);
        };
        let raw = String::from_utf8(found.body).map_err(|_| CoreError::State(format!("state object {} is not UTF-8", self.uri)))?;
        parse_doc(&raw, key, &self.uri.to_string()).map(Some)
    }

    fn store(&self, doc: &StateDoc) -> Result<(), CoreError> {
        let json = serde_json::to_vec_pretty(doc).map_err(|e| CoreError::State(format!("serialize state: {e}")))?;
        let (client, object) = (Arc::clone(&self.client), self.uri.key.clone());
        // A PUT replaces the object atomically: readers see the old document
        // or the whole new one.
        block_on(async move { client.put(&object, &json, Precondition::None).await }).map_err(s3_error)?;
        Ok(())
    }

    fn clear(&self, _key: &str) -> Result<(), CoreError> {
        let (client, object) = (Arc::clone(&self.client), self.uri.key.clone());
        block_on(async move { client.delete(&object).await }).map_err(s3_error)
    }
}

/// The lock object.
struct S3Lock {
    client: Arc<S3Client>,
    key: String,
    bucket: String,
}

impl S3Lock {
    fn put(&self, lease: &LockFile, condition: Precondition) -> Result<bool, CoreError> {
        let (client, key, body) = (Arc::clone(&self.client), self.key.clone(), lease.to_json().into_bytes());
        match block_on(async move { client.put(&key, &body, condition).await }).map_err(s3_error)? {
            Put::Written(_) => Ok(true),
            Put::PreconditionFailed => Ok(false),
        }
    }
}

impl LockStore for S3Lock {
    fn describe(&self) -> String {
        format!("state lock object 's3://{}/{}'", self.bucket, self.key)
    }

    fn create(&self, lease: &LockFile) -> Result<bool, CoreError> {
        self.put(lease, Precondition::Absent)
    }

    fn read(&self) -> Result<Option<Seen>, CoreError> {
        let (client, key) = (Arc::clone(&self.client), self.key.clone());
        let found = block_on(async move { client.get(&key).await }).map_err(s3_error)?;
        Ok(found.map(|o| Seen::parse(o.etag, &String::from_utf8_lossy(&o.body))))
    }

    fn replace(&self, seen: &Seen, lease: &LockFile) -> Result<bool, CoreError> {
        self.put(lease, Precondition::Matches(seen.version.clone()))
    }

    fn remove(&self) -> Result<(), CoreError> {
        let (client, key) = (Arc::clone(&self.client), self.key.clone());
        block_on(async move { client.delete(&key).await }).map_err(s3_error)
    }
}

#[cfg(test)]
mod tests {
    //! Against a tiny in-memory S3 with conditional writes (ETags change on
    //! every write), served over HTTP so the real client and signing run.

    use super::*;
    use std::collections::HashMap;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Bucket {
        objects: HashMap<String, (Vec<u8>, String)>,
        writes: u64,
    }

    /// Serves `GET`/`PUT`/`DELETE` with `If-None-Match: *` and `If-Match`.
    fn fake_s3() -> (String, Arc<Mutex<Bucket>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let bucket = Arc::new(Mutex::new(Bucket::default()));
        let shared = Arc::clone(&bucket);
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let shared = Arc::clone(&shared);
                std::thread::spawn(move || serve(stream, &shared));
            }
        });
        (url, bucket)
    }

    fn serve(stream: TcpStream, bucket: &Mutex<Bucket>) {
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut out = stream;
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).unwrap_or(0) == 0 {
                return;
            }
            let mut parts = line.split_whitespace();
            let (method, path) = (parts.next().unwrap_or("").to_string(), parts.next().unwrap_or("").to_string());
            let mut headers = HashMap::new();
            loop {
                let mut h = String::new();
                reader.read_line(&mut h).unwrap();
                let h = h.trim_end();
                if h.is_empty() {
                    break;
                }
                if let Some((k, v)) = h.split_once(':') {
                    headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
                }
            }
            let len: usize = headers.get("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);
            let mut body = vec![0; len];
            reader.read_exact(&mut body).unwrap();
            let (status, etag, payload) = handle(bucket, &method, &path, &headers, body);
            let etag = etag.map(|e| format!("etag: {e}\r\n")).unwrap_or_default();
            write!(out, "HTTP/1.1 {status} X\r\n{etag}content-length: {}\r\n\r\n", payload.len()).unwrap();
            out.write_all(&payload).unwrap();
        }
    }

    fn handle(
        bucket: &Mutex<Bucket>,
        method: &str,
        path: &str,
        headers: &HashMap<String, String>,
        body: Vec<u8>,
    ) -> (u16, Option<String>, Vec<u8>) {
        let mut b = bucket.lock().unwrap();
        let key = path.to_string();
        let current = b.objects.get(&key).map(|(_, e)| e.clone());
        match method {
            "GET" => match b.objects.get(&key) {
                Some((data, etag)) => (200, Some(etag.clone()), data.clone()),
                None => (404, None, b"<Error><Code>NoSuchKey</Code></Error>".to_vec()),
            },
            "PUT" => {
                let blocked = (headers.get("if-none-match").is_some_and(|v| v == "*") && current.is_some())
                    || headers.get("if-match").is_some_and(|v| current.as_deref() != Some(v.as_str()));
                if blocked {
                    return (412, None, b"<Error><Code>PreconditionFailed</Code></Error>".to_vec());
                }
                b.writes += 1;
                let etag = format!("\"v{}\"", b.writes);
                b.objects.insert(key, (body, etag.clone()));
                (200, Some(etag), vec![])
            }
            "DELETE" => {
                b.objects.remove(&key);
                (204, None, vec![])
            }
            _ => (405, None, vec![]),
        }
    }

    fn backend(url: &str, observe: Duration) -> S3Backend {
        let base = Settings {
            endpoint: Some(url.to_string()),
            region: Some("us-east-1".into()),
            access_key_id: Some("AKID".into()),
            secret_access_key: Some("s".into()),
            max_attempts: Some(1),
            ..Default::default()
        };
        S3Backend { observe, ..S3Backend::open_with("s3://lake/state/orders.json", base).unwrap() }
    }

    const LOCK: &str = "/lake/state/orders.json.lock";

    #[test]
    fn state_round_trips_and_clears() {
        let (url, _bucket) = fake_s3();
        let b = backend(&url, LEASE_TTL);
        assert!(b.load("orders").unwrap().is_none());
        let doc = StateDoc::new("orders", serde_json::json!(42), "h", "r1");
        b.store(&doc).unwrap();
        assert_eq!(b.load("orders").unwrap().unwrap().cursor_value, serde_json::json!(42));
        assert!(b.load("other").unwrap_err().to_string().contains("belongs to pipeline 'orders'"));
        b.clear("orders").unwrap();
        assert!(b.load("orders").unwrap().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_held_lock_excludes_a_second_run_and_is_released() {
        let (url, bucket) = fake_s3();
        let b = backend(&url, Duration::from_secs(3));
        let mut guard = tokio::task::spawn_blocking({
            let b = backend(&url, Duration::from_secs(3));
            move || b.lock("orders")
        })
        .await
        .unwrap()
        .unwrap();
        guard.start_renewal_every(Duration::from_millis(200));
        let second = tokio::task::spawn_blocking(move || b.lock("orders")).await.unwrap();
        let Err(e) = second else { panic!("a second run took a held lock") };
        assert!(e.to_string().contains("locked by a running process"), "{e}");
        drop(guard);
        assert!(!bucket.lock().unwrap().objects.contains_key(LOCK), "released");
    }

    #[test]
    fn an_expired_lease_is_stolen_once() {
        let (url, bucket) = fake_s3();
        let stale = LockFile { holder: "crashed".into(), renewed_at_unix_ms: 1 }.to_json();
        bucket.lock().unwrap().objects.insert(LOCK.into(), (stale.into_bytes(), "\"old\"".into()));
        let b = backend(&url, LEASE_TTL);
        let began = std::time::Instant::now();
        let guard = b.lock("orders").unwrap();
        assert!(began.elapsed() < Duration::from_secs(2), "expired: taken at once");
        let (raw, _) = bucket.lock().unwrap().objects[LOCK].clone();
        assert!(!String::from_utf8(raw).unwrap().contains("crashed"));
        drop(guard);
    }

    #[test]
    fn a_steal_loses_to_a_concurrent_write() {
        let (url, bucket) = fake_s3();
        let b = backend(&url, LEASE_TTL);
        let store = b.lock_store();
        store.create(&LockFile { holder: "a".into(), renewed_at_unix_ms: 1 }).unwrap();
        let seen = store.read().unwrap().unwrap();
        // Another run renews (or steals) between our read and our write.
        store.replace(&seen, &LockFile { holder: "b".into(), renewed_at_unix_ms: 2 }).unwrap();
        assert!(!store.replace(&seen, &LockFile { holder: "c".into(), renewed_at_unix_ms: 3 }).unwrap());
        let (raw, _) = bucket.lock().unwrap().objects[LOCK].clone();
        assert!(String::from_utf8(raw).unwrap().contains("\"b\""));
    }
}
