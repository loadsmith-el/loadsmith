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

use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::CoreError;
use loadsmith_config::StateConfig;

/// The persisted state document for one pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateDoc {
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
            pipeline: pipeline.into(),
            cursor_value,
            schema_hash: schema_hash.into(),
            run_id: run_id.into(),
            updated_at_unix_ms: now_unix_ms(),
        }
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

/// Fingerprint of a source schema (field names + types), for drift detection.
/// Not cryptographic — `DefaultHasher` is enough to notice a changed shape.
pub fn schema_fingerprint(schema: &loadsmith_protocol::Schema) -> String {
    let mut h = DefaultHasher::new();
    for f in &schema.fields {
        f.name.hash(&mut h);
        f.field_type.to_string().hash(&mut h);
    }
    format!("{:016x}", h.finish())
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

/// On-disk lock file. The lease is purely time-based: `holder` is diagnostic
/// only (no liveness probing), and `renewed_at_unix_ms` is what decides
/// staleness.
#[derive(Debug, Serialize, Deserialize)]
struct LockFile {
    holder: String,
    renewed_at_unix_ms: u64,
}

/// A diagnostic holder identifier (the pid as a string). Used only in messages.
fn holder_id() -> String {
    std::process::id().to_string()
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

/// Held for the duration of a run; releasing it (drop) removes the lock file.
/// While held, the run renews the lease periodically (see
/// [`LockGuard::renew`]).
pub struct LockGuard {
    lock_path: PathBuf,
    holder: String,
}

impl LockGuard {
    /// Refreshes the lease timestamp. Called on a wall-clock interval by the
    /// state supervisor so a live (even quiet) run keeps its lock. Errors are
    /// non-fatal — a single missed renewal is well within `LEASE_TTL`.
    pub fn renew(&self) {
        if let Err(e) = write_lease(&self.lock_path, &self.holder) {
            tracing::warn!(path = %self.lock_path.display(), "could not renew state lock lease: {e}");
        }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if let Err(e) = fs::remove_file(&self.lock_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %self.lock_path.display(), "could not remove state lock: {e}");
            }
        }
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
                return Ok(LockGuard { lock_path, holder });
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
        Ok(LockGuard { lock_path, holder })
    }

    fn load(&self, key: &str) -> Result<Option<StateDoc>, CoreError> {
        let raw = match fs::read_to_string(&self.path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(CoreError::State(format!("read state file: {e}"))),
        };
        let doc: StateDoc = serde_json::from_str(&raw)
            .map_err(|e| CoreError::State(format!("parse state file: {e}")))?;
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

    fn tmp_state_path(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("loadsmith-state-test-{}-{}", tag, std::process::id()));
        let _ = fs::create_dir_all(&dir);
        dir.join("state.json")
    }

    #[test]
    fn store_then_load_roundtrip() {
        let path = tmp_state_path("roundtrip");
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

    #[test]
    fn load_rejects_wrong_pipeline() {
        let path = tmp_state_path("wrong-pipeline");
        let _ = fs::remove_file(&path);
        let backend = LocalFileBackend::new(&path);
        backend
            .store(&StateDoc::new("p1", serde_json::json!(1), "h", "r"))
            .unwrap();
        assert!(backend.load("p2").is_err());
    }

    #[test]
    fn lock_excludes_second_holder() {
        let path = tmp_state_path("lock-excl");
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
        let path = tmp_state_path("lock-steal");
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
        let path = tmp_state_path("lock-renew");
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(LocalFileBackend::new(&path).lock_path());
        let backend = LocalFileBackend::new(&path);

        let guard = backend.lock("p1").unwrap();
        // Backdate the lease past TTL, then renew: a second acquire must still fail.
        let stale = LockFile { holder: "x".into(), renewed_at_unix_ms: 1 };
        fs::write(backend.lock_path(), serde_json::to_string(&stale).unwrap()).unwrap();
        guard.renew();
        assert!(backend.lock("p1").is_err());
    }
}
