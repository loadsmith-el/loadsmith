//! Core-owned incremental state.
//!
//! **The core is the sole owner of state.** Plugins are memoryless: a source
//! reports its high watermark as opaque `Checkpoint` events and a destination
//! reports durability as `Committed` events; this module persists the watermark
//! and hands it back on the next run. The cursor value is never interpreted —
//! it is an opaque `serde_json::Value` stored and echoed verbatim.
//!
//! The backend is pluggable behind [`StateBackend`], mirroring the
//! config-provider / sink pattern. Only [`LocalFileBackend`] exists today; an
//! `S3Backend` (conditional PUT for a distributed lock) slots in behind the same
//! trait later, reusing the s3 sink's SigV4/TLS work.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::CoreError;
use loadsmith_config::{SchemaChangePolicy, StateConfig};

/// Version of the state document format this build writes.
///
/// - `0` (implicit, field absent): written by loadsmith ≤ 0.2.0. Its
///   `schema_hash` came from std's `DefaultHasher`, which is not stable across
///   Rust releases, so it cannot be compared with a current fingerprint. Such a
///   document is accepted (its cursor is honoured) and is rewritten in the
///   current format on the next save.
/// - `1`: adds `version`; `schema_hash` is the FNV-1a fingerprint of
///   [`schema_fingerprint`].
///
/// A document with a version **greater** than this one was written by a newer
/// loadsmith and is refused rather than silently downgraded.
pub const STATE_FORMAT_VERSION: u32 = 1;

/// The persisted state document for one pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateDoc {
    /// State format version (see [`STATE_FORMAT_VERSION`]). Absent in legacy
    /// documents, which deserialize as `0`.
    #[serde(default)]
    pub version: u32,
    /// Pipeline name — guards against pointing a run at another pipeline's file.
    pub pipeline: String,
    /// Opaque high watermark of the cursor column. The core never interprets it.
    pub cursor_value: serde_json::Value,
    /// Fingerprint of the source schema when this watermark was written, used to
    /// detect schema drift across runs.
    pub schema_hash: String,
    /// Identifier of the run that last wrote this document.
    pub run_id: String,
    /// Wall-clock time of the last write, epoch milliseconds.
    pub updated_at_unix_ms: u64,
}

impl StateDoc {
    pub fn new(
        pipeline: impl Into<String>,
        cursor_value: serde_json::Value,
        schema_hash: impl Into<String>,
        run_id: impl Into<String>,
    ) -> Self {
        Self {
            version: STATE_FORMAT_VERSION,
            pipeline: pipeline.into(),
            cursor_value,
            schema_hash: schema_hash.into(),
            run_id: run_id.into(),
            updated_at_unix_ms: now_unix_ms(),
        }
    }

    /// True for a document written before the format was versioned (≤ 0.2.0):
    /// its `schema_hash` is not comparable with [`schema_fingerprint`].
    pub fn is_legacy(&self) -> bool {
        self.version == 0
    }
}

/// Backend that persists [`StateDoc`]s and provides an exclusive lock per
/// pipeline. The lock guards against concurrent runs corrupting the watermark.
pub trait StateBackend: Send {
    /// Acquire an exclusive lock for `key` (the pipeline name). Fails if another
    /// live run already holds it.
    fn lock(&self, key: &str) -> Result<LockGuard, CoreError>;
    /// Load the stored document, if any.
    fn load(&self, key: &str) -> Result<Option<StateDoc>, CoreError>;
    /// Persist `doc`, replacing any prior document atomically.
    fn store(&self, doc: &StateDoc) -> Result<(), CoreError>;
    /// Remove the stored document (used by `loadsmith state rm`).
    fn clear(&self, key: &str) -> Result<(), CoreError>;
}

/// Opens the backend named by `cfg`. Only `local` exists today.
pub fn open_backend(cfg: &StateConfig) -> Result<Box<dyn StateBackend>, CoreError> {
    match cfg.backend.as_str() {
        "local" => Ok(Box::new(LocalFileBackend::new(&cfg.path))),
        other => Err(CoreError::State(format!(
            "unsupported state backend '{other}' (only 'local' is available)"
        ))),
    }
}

/// Fingerprint of a source schema (field names + types, in order), for drift
/// detection. Persisted in the state file, so it must be **stable across Rust
/// releases and platforms**: 64-bit FNV-1a over a length-prefixed encoding
/// (`len(name) name len(type) type` per field, lengths as u64 little-endian;
/// `type` is the protocol's wire name, e.g. `int64`). Not cryptographic — it
/// only has to notice a changed shape. Changing this function changes every
/// stored fingerprint: bump [`STATE_FORMAT_VERSION`] and migrate if you do.
pub fn schema_fingerprint(schema: &loadsmith_protocol::Schema) -> String {
    let mut h = Fnv1a64::new();
    for f in &schema.fields {
        h.write_prefixed(f.name.as_bytes());
        h.write_prefixed(f.field_type.to_string().as_bytes());
    }
    format!("{:016x}", h.0)
}

/// Schema-drift guard, run once per run after the source reports its schema:
/// if the source shape changed since `prior` was written, a blind resume could
/// corrupt the load, so refuse (default) or warn per `policy`.
///
/// A legacy document (see [`STATE_FORMAT_VERSION`]) carries a fingerprint from
/// an unstable hasher that cannot be compared: it is accepted with a one-time
/// notice, and the run's next save rewrites it with the current fingerprint
/// and version.
pub fn check_schema_drift(
    prior: &StateDoc,
    current_hash: &str,
    policy: SchemaChangePolicy,
) -> Result<(), CoreError> {
    if prior.is_legacy() {
        tracing::warn!(
            pipeline = %prior.pipeline,
            "state file predates state format v{STATE_FORMAT_VERSION}: its schema fingerprint \
             cannot be compared, so schema drift is not checked this run; the file is \
             upgraded on the next save"
        );
        return Ok(());
    }
    if prior.schema_hash == current_hash {
        return Ok(());
    }
    match policy {
        SchemaChangePolicy::Refuse => Err(CoreError::State(format!(
            "source schema changed since last run (recorded {}, now {current_hash}); \
             set state.on_schema_change: continue to override",
            prior.schema_hash
        ))),
        SchemaChangePolicy::Continue => {
            tracing::warn!(
                recorded = %prior.schema_hash,
                current = %current_hash,
                "source schema changed since last run — continuing per policy"
            );
            Ok(())
        }
    }
}

/// 64-bit FNV-1a (http://www.isthe.com/chongo/tech/comp/fnv/). Inline so the
/// persisted fingerprint never depends on a hasher whose output may change.
struct Fnv1a64(u64);

impl Fnv1a64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    fn new() -> Self {
        Self(Self::OFFSET)
    }

    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }

    fn write_prefixed(&mut self, bytes: &[u8]) {
        self.write(&(bytes.len() as u64).to_le_bytes());
        self.write(bytes);
    }
}

/// A generated run identifier (pid + epoch ms) — no uuid dependency needed.
pub fn new_run_id() -> String {
    format!("{}-{}", std::process::id(), now_unix_ms())
}

fn now_unix_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// How long a lock lease is honoured without renewal. A holder must renew
/// within this window (see [`LEASE_RENEW_INTERVAL`]) or its lock becomes
/// stealable — this is what lets a crashed run's lock be reclaimed automatically,
/// with no process-liveness probe (the core stays platform-agnostic).
const LEASE_TTL: Duration = Duration::from_secs(60);

/// How often a live holder should renew its lease. Comfortably below
/// [`LEASE_TTL`] so a healthy run never has its lock stolen.
pub const LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(20);

/// On-disk lock file. The lease is purely time-based: `renewed_at_unix_ms` is
/// what decides staleness (no liveness probing). `holder` is a token unique to
/// one acquisition, used to tell "still mine" from "stolen by another run" —
/// a holder never renews or deletes a lock whose token is not its own.
#[derive(Debug, Serialize, Deserialize)]
struct LockFile {
    holder: String,
    renewed_at_unix_ms: u64,
}

/// A holder token unique to this acquisition: pid (diagnostic — every
/// container's entrypoint is pid 1, so the pid alone is not unique) + clock +
/// a per-process counter.
fn holder_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos =
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    format!("{}-{nanos:x}-{}", std::process::id(), SEQ.fetch_add(1, Ordering::Relaxed))
}

/// The holder recorded in the lock file right now, if it is readable.
fn current_holder(lock_path: &Path) -> Option<String> {
    let raw = fs::read_to_string(lock_path).ok()?;
    serde_json::from_str::<LockFile>(&raw).ok().map(|lf| lf.holder)
}

/// Atomically (temp + rename) writes a fresh lease to `lock_path`.
fn write_lease(lock_path: &Path, holder: &str) -> Result<(), CoreError> {
    let lf = LockFile { holder: holder.to_string(), renewed_at_unix_ms: now_unix_ms() };
    let json = serde_json::to_string(&lf)
        .map_err(|e| CoreError::State(format!("serialize lock: {e}")))?;
    let mut tmp = lock_path.to_path_buf().into_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    fs::write(&tmp, json.as_bytes())
        .map_err(|e| CoreError::State(format!("write lock tmp: {e}")))?;
    fs::rename(&tmp, lock_path)
        .map_err(|e| CoreError::State(format!("rename lock file: {e}")))?;
    Ok(())
}

/// The lease as seen by its holder. Shared (behind a mutex) between the
/// [`LockGuard`] and its background renewal task, so a renewal can never run
/// concurrently with — or after — the release.
struct Lease {
    lock_path: PathBuf,
    holder: String,
    released: bool,
    /// Set once a renewal found another holder's token: stop renewing.
    lost: bool,
}

impl Lease {
    fn renew(&mut self) {
        if self.released || self.lost {
            return;
        }
        match current_holder(&self.lock_path) {
            Some(h) if h == self.holder => {
                if let Err(e) = write_lease(&self.lock_path, &self.holder) {
                    tracing::warn!(path = %self.lock_path.display(), "could not renew state lock lease: {e}");
                }
            }
            other => {
                // Stolen (our lease expired and another run took it) or removed.
                // Never overwrite it: the other run owns the state now.
                self.lost = true;
                tracing::error!(
                    path = %self.lock_path.display(),
                    now_held_by = other.as_deref().unwrap_or("nobody"),
                    "state lock lease was lost to another run; no longer renewing it"
                );
            }
        }
    }

    fn release(&mut self) {
        self.released = true;
        match current_holder(&self.lock_path) {
            Some(h) if h == self.holder => {
                if let Err(e) = fs::remove_file(&self.lock_path) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        tracing::warn!(path = %self.lock_path.display(), "could not remove state lock: {e}");
                    }
                }
            }
            Some(other) => tracing::warn!(
                path = %self.lock_path.display(),
                now_held_by = %other,
                "state lock is held by another run; leaving it in place"
            ),
            None => {} // already gone (or unreadable — leave it for its owner)
        }
    }
}

/// Held for the duration of a run; releasing it (drop) removes the lock file —
/// but only if it is still ours (a lock stolen after our lease expired belongs
/// to the run that stole it). While held, the lease must be renewed: call
/// [`LockGuard::start_renewal`] right after acquiring.
pub struct LockGuard {
    lease: Arc<Mutex<Lease>>,
    renewal: Option<tokio::task::AbortHandle>,
}

impl LockGuard {
    fn new(lock_path: PathBuf, holder: String) -> Self {
        let lease = Lease { lock_path, holder, released: false, lost: false };
        Self { lease: Arc::new(Mutex::new(lease)), renewal: None }
    }

    /// Refreshes the lease timestamp, unless the lock now belongs to another
    /// run. Errors are non-fatal — a single missed renewal is well within
    /// `LEASE_TTL`.
    pub fn renew(&self) {
        lock_lease(&self.lease).renew();
    }

    /// Renews the lease every [`LEASE_RENEW_INTERVAL`] on the current tokio
    /// runtime until the guard is dropped. Start it as soon as the lock is
    /// acquired: anything slow before the data flows (spawning plugins, a long
    /// first query) must not let the lease expire.
    pub fn start_renewal(&mut self) {
        self.start_renewal_every(LEASE_RENEW_INTERVAL);
    }

    fn start_renewal_every(&mut self, every: Duration) {
        if let Some(old) = self.renewal.take() {
            old.abort();
        }
        let lease = Arc::clone(&self.lease);
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.tick().await; // the first tick is immediate; the lease is fresh
            loop {
                tick.tick().await;
                lock_lease(&lease).renew();
            }
        });
        self.renewal = Some(task.abort_handle());
    }
}

/// Locks the lease, recovering from a poisoned mutex (the data is plain flags).
fn lock_lease(lease: &Mutex<Lease>) -> std::sync::MutexGuard<'_, Lease> {
    lease.lock().unwrap_or_else(|p| p.into_inner())
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if let Some(task) = self.renewal.take() {
            task.abort();
        }
        // Under the mutex: a renewal in flight finishes first, and any later
        // one sees `released` and does nothing.
        lock_lease(&self.lease).release();
    }
}

/// Local-filesystem backend.
///
/// `path` is the state **file** for this pipeline; the lock is a sibling
/// `<path>.lock` created with `O_EXCL` (atomic). The lock carries a time-based
/// **lease**: a holder renews it periodically, and a lock whose lease has
/// expired (no renewal within [`LEASE_TTL`]) is stealable — so a crashed run's
/// lock is reclaimed automatically without probing process liveness (keeping the
/// core platform-agnostic). A live, freshly-renewed lock fails a second run
/// fast. Writes are atomic via a temp file + `rename`.
pub struct LocalFileBackend {
    path: PathBuf,
}

impl LocalFileBackend {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    fn lock_path(&self) -> PathBuf {
        let mut p = self.path.clone().into_os_string();
        p.push(".lock");
        PathBuf::from(p)
    }

    fn ensure_parent(&self) -> Result<(), CoreError> {
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)
                    .map_err(|e| CoreError::State(format!("create state dir: {e}")))?;
            }
        }
        Ok(())
    }
}

impl StateBackend for LocalFileBackend {
    fn lock(&self, key: &str) -> Result<LockGuard, CoreError> {
        self.ensure_parent()?;
        let lock_path = self.lock_path();
        let holder = holder_id();

        // First attempt: atomic exclusive create wins the race to hold the lock.
        match fs::OpenOptions::new().write(true).create_new(true).open(&lock_path) {
            Ok(mut f) => {
                let lf = LockFile { holder: holder.clone(), renewed_at_unix_ms: now_unix_ms() };
                let _ = f.write_all(serde_json::to_string(&lf).unwrap_or_default().as_bytes());
                return Ok(LockGuard::new(lock_path, holder));
            }
            Err(e) if e.kind() != std::io::ErrorKind::AlreadyExists => {
                return Err(CoreError::State(format!("acquire state lock: {e}")));
            }
            Err(_) => { /* already exists — inspect the lease below */ }
        }

        // Lock exists. Steal it only if its lease has expired (no renewal within
        // LEASE_TTL). An unparseable/torn lock is treated as live (don't steal).
        let raw = fs::read_to_string(&lock_path).unwrap_or_default();
        let age_ms = match serde_json::from_str::<LockFile>(&raw) {
            Ok(lf) => now_unix_ms().saturating_sub(lf.renewed_at_unix_ms),
            Err(_) => 0, // ambiguous — treat as freshly held
        };

        if age_ms <= LEASE_TTL.as_millis() as u64 {
            return Err(CoreError::State(format!(
                "pipeline '{key}' is locked by a running process (lease renewed {age_ms}ms ago); \
                 another run is in progress against state file '{}'",
                self.path.display()
            )));
        }

        tracing::warn!(
            path = %lock_path.display(),
            age_ms,
            "stealing expired state lock lease"
        );
        write_lease(&lock_path, &holder)?;
        Ok(LockGuard::new(lock_path, holder))
    }

    fn load(&self, key: &str) -> Result<Option<StateDoc>, CoreError> {
        let raw = match fs::read_to_string(&self.path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(CoreError::State(format!("read state file: {e}"))),
        };
        let doc: StateDoc = serde_json::from_str(&raw)
            .map_err(|e| CoreError::State(format!("parse state file: {e}")))?;
        if doc.version > STATE_FORMAT_VERSION {
            return Err(CoreError::State(format!(
                "state file '{}' has format version {}, but this loadsmith only understands \
                 up to {STATE_FORMAT_VERSION}; it was written by a newer loadsmith — upgrade",
                self.path.display(),
                doc.version
            )));
        }
        if doc.pipeline != key {
            return Err(CoreError::State(format!(
                "state file '{}' belongs to pipeline '{}', not '{key}'",
                self.path.display(),
                doc.pipeline
            )));
        }
        Ok(Some(doc))
    }

    fn store(&self, doc: &StateDoc) -> Result<(), CoreError> {
        self.ensure_parent()?;
        let json = serde_json::to_string_pretty(doc)
            .map_err(|e| CoreError::State(format!("serialize state: {e}")))?;
        let tmp = {
            let mut p = self.path.clone().into_os_string();
            p.push(".tmp");
            PathBuf::from(p)
        };
        fs::write(&tmp, json.as_bytes())
            .map_err(|e| CoreError::State(format!("write state tmp: {e}")))?;
        fs::rename(&tmp, &self.path)
            .map_err(|e| CoreError::State(format!("rename state file: {e}")))?;
        Ok(())
    }

    fn clear(&self, _key: &str) -> Result<(), CoreError> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(CoreError::State(format!("remove state file: {e}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Removes the test's temp dir when dropped (so `cargo test` leaves no
    /// `/tmp/loadsmith-*-test-*` behind).
    struct TmpDir(PathBuf);
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn tmp_state_path(tag: &str) -> (TmpDir, PathBuf) {
        let dir = std::env::temp_dir().join(format!("loadsmith-state-test-{}-{}", tag, std::process::id()));
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("state.json");
        (TmpDir(dir), path)
    }

    #[test]
    fn store_then_load_roundtrip() {
        let (_tmp, path) = tmp_state_path("roundtrip");
        let _ = fs::remove_file(&path);
        let backend = LocalFileBackend::new(&path);
        assert!(backend.load("p1").unwrap().is_none());

        let doc = StateDoc::new("p1", serde_json::json!("2026-06-09"), "abc", "run-1");
        backend.store(&doc).unwrap();

        let loaded = backend.load("p1").unwrap().unwrap();
        assert_eq!(loaded.pipeline, "p1");
        assert_eq!(loaded.cursor_value, "2026-06-09");
        assert_eq!(loaded.schema_hash, "abc");
    }

    fn schema(fields: &[(&str, loadsmith_protocol::FieldType)]) -> loadsmith_protocol::Schema {
        loadsmith_protocol::Schema {
            fields: fields
                .iter()
                .map(|(n, t)| loadsmith_protocol::Field { name: (*n).into(), field_type: t.clone() })
                .collect(),
        }
    }

    /// The fingerprint is persisted, so its value is part of the state format:
    /// this golden value (computed independently) must never change without a
    /// STATE_FORMAT_VERSION bump.
    #[test]
    fn schema_fingerprint_is_pinned() {
        use loadsmith_protocol::FieldType::*;
        let s = schema(&[("id", Int64), ("nome", Utf8)]);
        assert_eq!(schema_fingerprint(&s), "a8e4fe49824728ac");
    }

    #[test]
    fn schema_fingerprint_detects_changes() {
        use loadsmith_protocol::FieldType::*;
        let base = schema_fingerprint(&schema(&[("id", Int64), ("nome", Utf8)]));
        assert_ne!(base, schema_fingerprint(&schema(&[("id", Int32), ("nome", Utf8)])));
        assert_ne!(base, schema_fingerprint(&schema(&[("nome", Utf8), ("id", Int64)])));
        assert_ne!(base, schema_fingerprint(&schema(&[("id", Int64)])));
        // Length prefixes keep field boundaries unambiguous.
        assert_ne!(
            schema_fingerprint(&schema(&[("ab", Utf8), ("c", Utf8)])),
            schema_fingerprint(&schema(&[("a", Utf8), ("bc", Utf8)]))
        );
    }

    #[test]
    fn new_docs_carry_the_current_version() {
        let (_tmp, path) = tmp_state_path("version");
        let backend = LocalFileBackend::new(&path);
        backend.store(&StateDoc::new("p1", serde_json::json!(1), "h", "r")).unwrap();
        let raw: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(raw["version"], STATE_FORMAT_VERSION);
        assert!(!backend.load("p1").unwrap().unwrap().is_legacy());
    }

    /// Migration: a state file written by loadsmith ≤ 0.2.0 has no `version`
    /// and a DefaultHasher fingerprint. It must load, keep its cursor, pass the
    /// drift check even under `refuse`, and be upgraded by the next save.
    #[test]
    fn legacy_doc_loads_and_is_upgraded_on_next_save() {
        let (_tmp, path) = tmp_state_path("legacy");
        fs::write(
            &path,
            r#"{
  "pipeline": "p1",
  "cursor_value": "2026-06-09T10:00:00",
  "schema_hash": "1f2e3d4c5b6a7980",
  "run_id": "4242-1760000000000",
  "updated_at_unix_ms": 1760000000000
}"#,
        )
        .unwrap();
        let backend = LocalFileBackend::new(&path);
        let prior = backend.load("p1").unwrap().unwrap();
        assert!(prior.is_legacy());
        assert_eq!(prior.cursor_value, "2026-06-09T10:00:00");

        let current = schema_fingerprint(&schema(&[("id", loadsmith_protocol::FieldType::Int64)]));
        assert_ne!(prior.schema_hash, current);
        check_schema_drift(&prior, &current, SchemaChangePolicy::Refuse)
            .expect("a legacy fingerprint must not trigger a drift refusal");

        // Next save (what the state supervisor does) rewrites it in the new format.
        backend.store(&StateDoc::new("p1", serde_json::json!("2026-06-10"), &current, "r2")).unwrap();
        let upgraded = backend.load("p1").unwrap().unwrap();
        assert_eq!(upgraded.version, STATE_FORMAT_VERSION);
        assert_eq!(upgraded.schema_hash, current);
        check_schema_drift(&upgraded, &current, SchemaChangePolicy::Refuse).unwrap();
    }

    #[test]
    fn drift_check_on_current_docs() {
        let doc = StateDoc::new("p1", serde_json::json!(1), "aaaa", "r");
        assert!(check_schema_drift(&doc, "aaaa", SchemaChangePolicy::Refuse).is_ok());
        assert!(check_schema_drift(&doc, "bbbb", SchemaChangePolicy::Refuse).is_err());
        assert!(check_schema_drift(&doc, "bbbb", SchemaChangePolicy::Continue).is_ok());
    }

    #[test]
    fn load_refuses_a_newer_format_version() {
        let (_tmp, path) = tmp_state_path("future");
        fs::write(
            &path,
            format!(
                r#"{{"version":{},"pipeline":"p1","cursor_value":1,"schema_hash":"h","run_id":"r","updated_at_unix_ms":1}}"#,
                STATE_FORMAT_VERSION + 1
            ),
        )
        .unwrap();
        let err = LocalFileBackend::new(&path).load("p1").unwrap_err().to_string();
        assert!(err.contains("newer loadsmith"), "{err}");
    }

    #[test]
    fn load_rejects_wrong_pipeline() {
        let (_tmp, path) = tmp_state_path("wrong-pipeline");
        let _ = fs::remove_file(&path);
        let backend = LocalFileBackend::new(&path);
        backend
            .store(&StateDoc::new("p1", serde_json::json!(1), "h", "r"))
            .unwrap();
        assert!(backend.load("p2").is_err());
    }

    #[test]
    fn lock_excludes_second_holder() {
        let (_tmp, path) = tmp_state_path("lock-excl");
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(LocalFileBackend::new(&path).lock_path());
        let backend = LocalFileBackend::new(&path);

        let guard = backend.lock("p1").unwrap();
        // A second lock attempt sees a live holder (this very process) → error.
        assert!(backend.lock("p1").is_err());
        drop(guard);
        // After releasing, locking succeeds again.
        let _g2 = backend.lock("p1").unwrap();
    }

    #[test]
    fn steals_expired_lease() {
        let (_tmp, path) = tmp_state_path("lock-steal");
        let _ = fs::remove_file(&path);
        let backend = LocalFileBackend::new(&path);
        // Write a lock whose lease was last renewed long before LEASE_TTL ago.
        let stale = LockFile { holder: "999999".into(), renewed_at_unix_ms: 1 };
        fs::write(backend.lock_path(), serde_json::to_string(&stale).unwrap()).unwrap();
        // Expired ⇒ stolen, not rejected. No process-liveness probe involved.
        let _g = backend.lock("p1").unwrap();
    }

    #[test]
    fn renew_keeps_lock_held() {
        let (_tmp, path) = tmp_state_path("lock-renew");
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(LocalFileBackend::new(&path).lock_path());
        let backend = LocalFileBackend::new(&path);

        let guard = backend.lock("p1").unwrap();
        // Backdate our own lease past TTL, then renew: a second acquire must still fail.
        backdate(&backend.lock_path(), &holder_of(&guard));
        guard.renew();
        assert!(backend.lock("p1").is_err());
    }

    fn holder_of(guard: &LockGuard) -> String {
        lock_lease(&guard.lease).holder.clone()
    }

    fn backdate(lock_path: &Path, holder: &str) {
        let stale = LockFile { holder: holder.into(), renewed_at_unix_ms: 1 };
        fs::write(lock_path, serde_json::to_string(&stale).unwrap()).unwrap();
    }

    fn lock_file(lock_path: &Path) -> LockFile {
        serde_json::from_str(&fs::read_to_string(lock_path).unwrap()).unwrap()
    }

    #[test]
    fn holder_tokens_are_unique_per_acquisition() {
        assert_ne!(holder_id(), holder_id());
    }

    /// Regression: dropping a guard whose lease had expired and been stolen
    /// deleted the *new* holder's lock, letting a third run in concurrently.
    #[test]
    fn drop_leaves_a_stolen_lock_alone() {
        let (_tmp, path) = tmp_state_path("lock-stolen-drop");
        let backend = LocalFileBackend::new(&path);
        let guard = backend.lock("p1").unwrap();
        // Our lease expired; another run stole it.
        backdate(&backend.lock_path(), &holder_of(&guard));
        let thief = backend.lock("p1").unwrap();
        let thief_holder = holder_of(&thief);

        drop(guard);
        assert_eq!(lock_file(&backend.lock_path()).holder, thief_holder);
        assert!(backend.lock("p1").is_err(), "the thief still holds the lock");
        drop(thief);
        assert!(!backend.lock_path().exists());
    }

    #[test]
    fn renew_does_not_overwrite_a_stolen_lock() {
        let (_tmp, path) = tmp_state_path("lock-stolen-renew");
        let backend = LocalFileBackend::new(&path);
        let guard = backend.lock("p1").unwrap();
        backdate(&backend.lock_path(), &holder_of(&guard));
        let thief = backend.lock("p1").unwrap();

        guard.renew();
        assert_eq!(lock_file(&backend.lock_path()).holder, holder_of(&thief));
        drop(thief);
        // Lost for good: a later renew must not resurrect the lock either.
        guard.renew();
        assert!(!backend.lock_path().exists());
    }

    /// Regression: renewal used to start only when the state supervisor was
    /// spawned (after the source's first query returned), so a slow start let
    /// the 60s lease expire. Now it runs from acquisition.
    #[tokio::test]
    async fn renewal_runs_from_acquisition() {
        let (_tmp, path) = tmp_state_path("lock-autorenew");
        let backend = LocalFileBackend::new(&path);
        let mut guard = backend.lock("p1").unwrap();
        guard.start_renewal_every(Duration::from_millis(10));
        backdate(&backend.lock_path(), &holder_of(&guard));

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while lock_file(&backend.lock_path()).renewed_at_unix_ms == 1 {
            assert!(std::time::Instant::now() < deadline, "lease was never renewed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(backend.lock("p1").is_err());

        drop(guard);
        // Released, and the aborted renewal task does not recreate it.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!backend.lock_path().exists());
    }
}
