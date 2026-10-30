//! Core-owned incremental state.
//!
//! **The core is the sole owner of state.** Plugins are memoryless: a source
//! reports its high watermark as opaque `Checkpoint` events and a destination
//! reports durability as `Committed` events; this module persists the watermark
//! and hands it back on the next run. The cursor value is never interpreted —
//! it is an opaque `serde_json::Value` stored and echoed verbatim.
//!
//! The backend is pluggable behind [`StateBackend`], mirroring the
//! config-provider / sink pattern: [`LocalFileBackend`] (a file) and
//! [`crate::state_s3::S3Backend`] (an S3 object, locked by conditional
//! `PUT`s). Both share the lock's lease logic ([`acquire`]) through
//! [`LockStore`].

use std::fs;
use std::io::Write;
use std::path::PathBuf;
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
/// - `1`: adds `version`; `schema_hash` is FNV-1a over the field names and
///   the lossy type labels of the protocol `schema` message (every timestamp
///   was `timestamp_ms`, most other types `utf8`, no nullability).
/// - `2`: `schema_hash` is FNV-1a over the exact Arrow schema of the source's
///   IPC stream (see [`schema_fingerprint`]).
/// - `3`: adds `resume` ([`ResumeRecord`]): a run that failed, to be resumed.
///   Written only while a record is present — a run that succeeds writes `2`
///   again — so an older loadsmith refuses a file it could not resume
///   correctly and reads every other one.
///
/// A document older than [`FINGERPRINT_SINCE`] is migrated like a legacy one:
/// its cursor is honoured, the drift check is skipped once with a warning
/// (its fingerprint cannot be compared), and the next save rewrites it.
///
/// A document with a version **greater** than this one was written by a newer
/// loadsmith and is refused rather than silently downgraded.
pub const STATE_FORMAT_VERSION: u32 = 3;

/// The format of a document without a resume record.
const PLAIN_FORMAT_VERSION: u32 = 2;

/// The first state format whose `schema_hash` is comparable with
/// [`schema_fingerprint`]. Bump it (with [`STATE_FORMAT_VERSION`]) whenever
/// the fingerprint changes.
pub const FINGERPRINT_SINCE: u32 = 2;

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
    /// Present while the run that wrote this document has not succeeded: how
    /// the next run continues it (format 3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume: Option<ResumeRecord>,
}

/// A run that failed after part of its output became durable: the next run
/// continues it — the same run id and start time (so a sink names its
/// objects as this run did, in the same prefix), the destination's file
/// numbering after the delivered files, and the source's resume token. Kept
/// until a run succeeds; refused when the pipeline's configuration changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeRecord {
    /// The run being continued.
    pub run_id: String,
    pub started_at_unix_ms: u64,
    /// Fingerprint of the run's configuration ([`config_fingerprint`]).
    pub config_hash: String,
    /// With a sink: the number of the first file not known to be delivered —
    /// the next run stages its files from here, so it replaces a file the
    /// failed run may have left half-delivered instead of adding a second.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_object_seq: Option<u64>,
    /// The source's last durable resume token (a partitioned full load's
    /// finished partitions), handed back to it in `start.resume_token`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<serde_json::Value>,
}

impl StateDoc {
    pub fn new(
        pipeline: impl Into<String>,
        cursor_value: serde_json::Value,
        schema_hash: impl Into<String>,
        run_id: impl Into<String>,
    ) -> Self {
        Self {
            version: PLAIN_FORMAT_VERSION,
            pipeline: pipeline.into(),
            cursor_value,
            schema_hash: schema_hash.into(),
            run_id: run_id.into(),
            updated_at_unix_ms: now_unix_ms(),
            resume: None,
        }
    }

    /// With a resume record (format 3), or without one (format 2).
    pub fn with_resume(mut self, resume: Option<ResumeRecord>) -> Self {
        self.version = if resume.is_some() { STATE_FORMAT_VERSION } else { PLAIN_FORMAT_VERSION };
        self.resume = resume;
        self
    }

    /// The incremental watermark to resume reading after: none for a source
    /// that reports no cursor (a full load's document holds `null`).
    pub fn watermark(&self) -> Option<&serde_json::Value> {
        Some(&self.cursor_value).filter(|v| !v.is_null())
    }

    /// True for a document written before the format was versioned (≤ 0.2.0):
    /// its `schema_hash` is not comparable with [`schema_fingerprint`].
    pub fn is_legacy(&self) -> bool {
        self.version == 0
    }

    /// True when `schema_hash` can be compared with [`schema_fingerprint`]
    /// (format ≥ [`FINGERPRINT_SINCE`]).
    pub fn fingerprint_is_comparable(&self) -> bool {
        self.version >= FINGERPRINT_SINCE
    }
}

/// Backend that persists [`StateDoc`]s and provides an exclusive lock per
/// pipeline. The lock guards against concurrent runs corrupting the watermark.
pub trait StateBackend: Send {
    /// Acquire an exclusive lock for `key` (the pipeline name). Fails if another
    /// live run already holds it. Blocking: a held lock may be watched for up to
    /// a lease TTL to tell a live holder from a crashed one.
    fn lock(&self, key: &str) -> Result<LockGuard, CoreError>;
    /// Load the stored document, if any.
    fn load(&self, key: &str) -> Result<Option<StateDoc>, CoreError>;
    /// Persist `doc`, replacing any prior document atomically.
    fn store(&self, doc: &StateDoc) -> Result<(), CoreError>;
    /// Remove the stored document (used by `loadsmith state rm`).
    fn clear(&self, key: &str) -> Result<(), CoreError>;
}

/// Opens the backend named by `cfg`: `local` (a file) or `s3` (an object).
pub fn open_backend(cfg: &StateConfig) -> Result<Box<dyn StateBackend>, CoreError> {
    match cfg.backend.as_str() {
        "local" => Ok(Box::new(LocalFileBackend::new(&cfg.path))),
        "s3" => Ok(Box::new(crate::state_s3::S3Backend::open(&cfg.path)?)),
        other => Err(CoreError::State(format!(
            "unsupported state backend '{other}' (available: 'local', 's3')"
        ))),
    }
}

/// Fingerprint of a source's Arrow schema, for drift detection.
///
/// It covers what a resume depends on: field order, names, nullability, and
/// exact types including their parameters (timestamp unit and time zone,
/// decimal precision and scale, list/struct/map children, dictionary index
/// and value types). Field metadata is ignored, so a changed column comment
/// is not drift.
///
/// The schema is first mapped to the protocol's type vocabulary (owned by
/// Loadsmith, so an Arrow upgrade cannot change it) and then encoded as a
/// sequence of length-prefixed tokens (`u64` little-endian length, then the
/// UTF-8 bytes) hashed with 64-bit FNV-1a, printed as 16 hex digits:
///
/// - the schema: the field count, then each field;
/// - a field: its name, `nullable` or `required`, then its type;
/// - a type: its wire name (e.g. `timestamp`), then its parameters in a fixed
///   order — numbers in decimal, units by their wire name, a time zone as
///   `=<tz>` or `-` when absent, child counts in decimal, child fields and
///   types recursively.
///
/// Persisted in the state file, so it must be **stable across Rust releases
/// and platforms**. Not cryptographic — it only has to notice a changed
/// shape. Changing this encoding changes every stored fingerprint: bump
/// [`STATE_FORMAT_VERSION`] and [`FINGERPRINT_SINCE`] if you do.
pub fn schema_fingerprint(schema: &arrow_schema::Schema) -> String {
    let fields = loadsmith_arrow::protocol_fields_from_schema(schema);
    let mut h = Fnv1a64::new();
    h.write_prefixed(fields.len().to_string().as_bytes());
    for f in &fields {
        hash_field(&mut h, f);
    }
    format!("{:016x}", h.0)
}

fn hash_field(h: &mut Fnv1a64, f: &loadsmith_protocol::Field) {
    h.write_prefixed(f.name.as_bytes());
    h.write_prefixed(if f.nullable { b"nullable" } else { b"required" });
    hash_type(h, &f.field_type);
}

fn hash_type(h: &mut Fnv1a64, t: &loadsmith_protocol::FieldType) {
    use loadsmith_protocol::FieldType as T;
    h.write_prefixed(t.type_name().as_bytes());
    let num = |h: &mut Fnv1a64, n: i64| h.write_prefixed(n.to_string().as_bytes());
    match t {
        T::Decimal32 { precision, scale }
        | T::Decimal64 { precision, scale }
        | T::Decimal128 { precision, scale }
        | T::Decimal256 { precision, scale } => {
            num(h, i64::from(*precision));
            num(h, i64::from(*scale));
        }
        T::FixedSizeBinary { byte_width } => num(h, i64::from(*byte_width)),
        T::Time32 { unit } | T::Time64 { unit } | T::Duration { unit } => {
            h.write_prefixed(unit.as_str().as_bytes());
        }
        T::Timestamp { unit, timezone } => {
            h.write_prefixed(unit.as_str().as_bytes());
            match timezone {
                Some(tz) => h.write_prefixed(format!("={tz}").as_bytes()),
                None => h.write_prefixed(b"-"),
            }
        }
        T::Interval { unit } => h.write_prefixed(unit.as_str().as_bytes()),
        T::List { item } | T::LargeList { item } => hash_field(h, item),
        T::FixedSizeList { item, size } => {
            hash_field(h, item);
            num(h, i64::from(*size));
        }
        T::Struct { fields } => {
            num(h, fields.len() as i64);
            for f in fields {
                hash_field(h, f);
            }
        }
        T::Map { key, value, keys_sorted } => {
            hash_field(h, key);
            hash_field(h, value);
            h.write_prefixed(if *keys_sorted { b"sorted" } else { b"unsorted" });
        }
        T::Dictionary { index, value } => {
            hash_type(h, index);
            hash_type(h, value);
        }
        _ => {}
    }
}

/// Schema-drift guard, run once per run after the source's stream header is
/// read: if the source shape changed since `prior` was written, a blind
/// resume could corrupt the load, so refuse (default) or warn per `policy`.
///
/// A document older than [`FINGERPRINT_SINCE`] (legacy or format 1) carries a
/// fingerprint computed differently, which cannot be compared: it is accepted
/// with a one-time warning, and the run's next save rewrites it with the
/// current fingerprint and version.
pub fn check_schema_drift(
    prior: &StateDoc,
    current_hash: &str,
    policy: SchemaChangePolicy,
) -> Result<(), CoreError> {
    if !prior.fingerprint_is_comparable() {
        let format = if prior.is_legacy() { "legacy".to_string() } else { format!("v{}", prior.version) };
        tracing::warn!(
            pipeline = %prior.pipeline,
            "state file is format {format}, older than v{FINGERPRINT_SINCE}: its schema fingerprint \
             was computed differently and cannot be compared, so schema drift is not checked \
             this run; the file is upgraded to v{STATE_FORMAT_VERSION} on the next save"
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

/// Fingerprint of a pipeline's configuration, for [`ResumeRecord::config_hash`]:
/// FNV-1a over its JSON form with every resolved secret masked (a state file
/// must not carry a hash a password could be recovered from). Any change to
/// the pipeline — a query, a target, a batch size — makes a different run, and
/// a failed run is resumed only by the same configuration.
pub fn config_fingerprint(config_json: &str) -> String {
    let mut h = Fnv1a64::new();
    h.write_prefixed(config_json.as_bytes());
    format!("{:016x}", h.0)
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
/// with no process-liveness probe (the core stays platform-agnostic, and a run
/// in another container or on another host could not be probed anyway).
pub(crate) const LEASE_TTL: Duration = Duration::from_secs(15);

/// How often a live holder should renew its lease. Comfortably below
/// [`LEASE_TTL`] so a healthy run never has its lock stolen.
pub const LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(5);

/// How often a run waiting on a lock looks at it again.
pub(crate) const LEASE_POLL: Duration = Duration::from_millis(250);

/// The lock document. The lease is purely time-based: `renewed_at_unix_ms` is
/// what decides staleness (no liveness probing). `holder` is a token unique to
/// one acquisition, used to tell "still mine" from "stolen by another run" —
/// a holder never renews or deletes a lock whose token is not its own.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LockFile {
    pub(crate) holder: String,
    pub(crate) renewed_at_unix_ms: u64,
}

impl LockFile {
    fn fresh(holder: &str) -> Self {
        Self { holder: holder.to_string(), renewed_at_unix_ms: now_unix_ms() }
    }

    pub(crate) fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
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

/// One look at a lock: its lease (`None` when unreadable or torn) and a
/// version that changes with every write — the raw content locally, the ETag
/// on S3 — which is how a renewal is noticed.
pub(crate) struct Seen {
    pub(crate) version: String,
    pub(crate) lease: Option<LockFile>,
}

impl Seen {
    pub(crate) fn parse(version: String, raw: &str) -> Self {
        Self { version, lease: serde_json::from_str(raw).ok() }
    }

    fn holder(&self) -> Option<&str> {
        self.lease.as_ref().map(|l| l.holder.as_str())
    }
}

/// Where a backend keeps its lock. The lease logic ([`acquire`], [`Lease`])
/// is shared; a store only has to make `create` atomic, and makes `replace`
/// a compare-and-swap where it can.
pub(crate) trait LockStore: Send + Sync {
    /// The lock's location, for messages.
    fn describe(&self) -> String;
    /// Creates the lock holding `lease` if there is none: `false` when one
    /// already exists.
    fn create(&self, lease: &LockFile) -> Result<bool, CoreError>;
    /// The lock as it is now; `None` when there is none.
    fn read(&self) -> Result<Option<Seen>, CoreError>;
    /// Writes `lease` if the lock is still the one `seen` (`false` when it
    /// changed in between).
    fn replace(&self, seen: &Seen, lease: &LockFile) -> Result<bool, CoreError>;
    /// Removes the lock.
    fn remove(&self) -> Result<(), CoreError>;
}

/// Takes the lock in `store` for pipeline `key`.
///
/// The first run to create it holds it. A run that finds one takes it at
/// once if its lease expired (no renewal within [`LEASE_TTL`] by the clock it
/// records); otherwise it watches it for `observe`: a renewal means a live
/// holder (the run fails, within one renew interval), a lease left unrenewed
/// for that long — measured on this run's own clock, so clock skew between
/// hosts cannot fool it — is a crashed holder's, and is taken.
pub(crate) fn acquire(
    store: Arc<dyn LockStore>,
    key: &str,
    observe: Duration,
    poll: Duration,
) -> Result<LockGuard, CoreError> {
    let holder = holder_id();
    let busy = |store: &dyn LockStore| {
        CoreError::State(format!(
            "pipeline '{key}' is locked by a running process (its lease is being renewed); \
             another run is in progress against {}",
            store.describe()
        ))
    };
    'race: loop {
        if store.create(&LockFile::fresh(&holder))? {
            return Ok(LockGuard::new(store, holder));
        }
        let Some(seen) = store.read()? else {
            continue 'race; // released meanwhile: race for it again
        };
        let age_ms = match &seen.lease {
            Some(lf) => now_unix_ms().saturating_sub(lf.renewed_at_unix_ms),
            None => 0, // ambiguous (torn, unreadable) — watch it like a fresh one
        };
        if age_ms <= LEASE_TTL.as_millis() as u64 {
            tracing::info!(
                lock = %store.describe(),
                age_ms,
                "the state lock is held; waiting up to {}s to see whether its holder is alive",
                observe.as_secs()
            );
            let watched_since = std::time::Instant::now();
            loop {
                std::thread::sleep(poll);
                match store.read() {
                    Ok(None) => continue 'race,
                    Ok(Some(now)) if now.version != seen.version => return Err(busy(&*store)),
                    // Unchanged, or not readable this time (try again).
                    _ => {}
                }
                if watched_since.elapsed() >= observe {
                    break;
                }
            }
        }

        tracing::warn!(
            lock = %store.describe(),
            age_ms,
            "stealing expired state lock lease (its holder stopped renewing it: a crashed run)"
        );
        if store.replace(&seen, &LockFile::fresh(&holder))? {
            return Ok(LockGuard::new(store, holder));
        }
        // Renewed or taken by another run at the last moment: it is live.
        return Err(busy(&*store));
    }
}

/// The lease as seen by its holder. Shared (behind a mutex) between the
/// [`LockGuard`] and its background renewal task, so a renewal can never run
/// concurrently with — or after — the release.
struct Lease {
    store: Arc<dyn LockStore>,
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
        let seen = match self.store.read() {
            Ok(seen) => seen,
            // Transient (a network error): the next renewal tries again,
            // well within LEASE_TTL.
            Err(e) => {
                tracing::warn!(lock = %self.store.describe(), "could not read the state lock to renew it: {e}");
                return;
            }
        };
        let now_held_by = match seen {
            Some(seen) if seen.holder() == Some(self.holder.as_str()) => {
                match self.store.replace(&seen, &LockFile::fresh(&self.holder)) {
                    Ok(true) => return,
                    Ok(false) => "another run".to_string(),
                    Err(e) => {
                        tracing::warn!(lock = %self.store.describe(), "could not renew state lock lease: {e}");
                        return;
                    }
                }
            }
            Some(seen) => seen.holder().unwrap_or("an unreadable lease").to_string(),
            None => "nobody".to_string(),
        };
        // Stolen (our lease expired and another run took it) or removed.
        // Never overwrite it: the other run owns the state now.
        self.lost = true;
        tracing::error!(
            lock = %self.store.describe(),
            now_held_by = %now_held_by,
            "state lock lease was lost to another run; no longer renewing it"
        );
    }

    fn release(&mut self) {
        self.released = true;
        match self.store.read() {
            Ok(Some(seen)) if seen.holder() == Some(self.holder.as_str()) => {
                if let Err(e) = self.store.remove() {
                    tracing::warn!(lock = %self.store.describe(), "could not remove state lock: {e}");
                }
            }
            Ok(Some(seen)) => tracing::warn!(
                lock = %self.store.describe(),
                now_held_by = seen.holder().unwrap_or("an unreadable lease"),
                "state lock is held by another run; leaving it in place"
            ),
            Ok(None) => {} // already gone
            // Unreadable: leave it; its lease expires on its own.
            Err(e) => tracing::warn!(lock = %self.store.describe(), "could not read the state lock to release it: {e}"),
        }
    }
}

/// Held for the duration of a run; releasing it (drop) removes the lock —
/// but only if it is still ours (a lock stolen after our lease expired belongs
/// to the run that stole it). While held, the lease must be renewed: call
/// [`LockGuard::start_renewal`] right after acquiring.
pub struct LockGuard {
    lease: Arc<Mutex<Lease>>,
    renewal: Option<tokio::task::AbortHandle>,
}

impl LockGuard {
    fn new(store: Arc<dyn LockStore>, holder: String) -> Self {
        let lease = Lease { store, holder, released: false, lost: false };
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

    pub(crate) fn start_renewal_every(&mut self, every: Duration) {
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

/// Checks a state document read from `location` for pipeline `key`.
pub(crate) fn parse_doc(raw: &str, key: &str, location: &str) -> Result<StateDoc, CoreError> {
    let doc: StateDoc = serde_json::from_str(raw)
        .map_err(|e| CoreError::State(format!("parse state file: {e}")))?;
    if doc.version > STATE_FORMAT_VERSION {
        return Err(CoreError::State(format!(
            "state file '{location}' has format version {}, but this loadsmith only understands \
             up to {STATE_FORMAT_VERSION}; it was written by a newer loadsmith — upgrade",
            doc.version
        )));
    }
    if doc.pipeline != key {
        return Err(CoreError::State(format!(
            "state file '{location}' belongs to pipeline '{}', not '{key}'",
            doc.pipeline
        )));
    }
    Ok(doc)
}

/// The local lock: a file created with `O_EXCL` (atomic), renewed by an
/// atomic temp file + `rename`.
struct LocalLock {
    path: PathBuf,
}

impl LockStore for LocalLock {
    fn describe(&self) -> String {
        format!("state lock file '{}'", self.path.display())
    }

    fn create(&self, lease: &LockFile) -> Result<bool, CoreError> {
        match fs::OpenOptions::new().write(true).create_new(true).open(&self.path) {
            Ok(mut f) => {
                let _ = f.write_all(lease.to_json().as_bytes());
                Ok(true)
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(CoreError::State(format!("acquire state lock: {e}"))),
        }
    }

    fn read(&self) -> Result<Option<Seen>, CoreError> {
        match fs::read_to_string(&self.path) {
            Ok(raw) => Ok(Some(Seen::parse(raw.clone(), &raw))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(CoreError::State(format!("read state lock: {e}"))),
        }
    }

    /// Not a compare-and-swap: a local filesystem has none. The window
    /// between the caller's read and this write is a few microseconds.
    fn replace(&self, _seen: &Seen, lease: &LockFile) -> Result<bool, CoreError> {
        let mut tmp = self.path.clone().into_os_string();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        fs::write(&tmp, lease.to_json().as_bytes())
            .map_err(|e| CoreError::State(format!("write lock tmp: {e}")))?;
        fs::rename(&tmp, &self.path)
            .map_err(|e| CoreError::State(format!("rename lock file: {e}")))?;
        Ok(true)
    }

    fn remove(&self) -> Result<(), CoreError> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(CoreError::State(format!("remove state lock: {e}"))),
        }
    }
}

/// Local-filesystem backend.
///
/// `path` is the state **file** for this pipeline; the lock is a sibling
/// `<path>.lock` ([`LocalLock`]) carrying a time-based **lease** (see
/// [`acquire`]): a holder renews it periodically, and a lock whose lease has
/// expired is stealable — so a crashed run's lock is reclaimed automatically
/// without probing process liveness (keeping the core platform-agnostic).
/// Writes are atomic via a temp file + `rename`.
pub struct LocalFileBackend {
    path: PathBuf,
    /// How long an unchanged lease is watched before it is taken.
    observe: Duration,
}

impl LocalFileBackend {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into(), observe: LEASE_TTL }
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
        acquire(Arc::new(LocalLock { path: self.lock_path() }), key, self.observe, LEASE_POLL)
    }

    fn load(&self, key: &str) -> Result<Option<StateDoc>, CoreError> {
        let raw = match fs::read_to_string(&self.path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(CoreError::State(format!("read state file: {e}"))),
        };
        parse_doc(&raw, key, &self.path.display().to_string()).map(Some)
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
    use std::path::Path;

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

    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    fn schema(fields: &[(&str, DataType, bool)]) -> Schema {
        Schema::new(
            fields.iter().map(|(n, t, nullable)| Field::new(*n, t.clone(), *nullable)).collect::<Vec<_>>(),
        )
    }

    fn simple(fields: &[(&str, DataType)]) -> Schema {
        schema(&fields.iter().map(|(n, t)| (*n, t.clone(), true)).collect::<Vec<_>>())
    }

    /// The fingerprint is persisted, so its value is part of the state format:
    /// these golden values (computed independently, from the documented
    /// token encoding) must never change without a STATE_FORMAT_VERSION bump.
    #[test]
    fn schema_fingerprint_is_pinned() {
        let s = schema(&[("id", DataType::Int64, false), ("nome", DataType::Utf8, true)]);
        assert_eq!(schema_fingerprint(&s), "4cd1b622e90bcb1b");
        let s = schema(&[
            ("ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
            ("amount", DataType::Decimal128(12, 2), false),
            ("tags", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true),
        ]);
        assert_eq!(schema_fingerprint(&s), "bcda90d5848a8a39");
    }

    #[test]
    fn schema_fingerprint_detects_changes() {
        let base = schema_fingerprint(&simple(&[("id", DataType::Int64), ("nome", DataType::Utf8)]));
        let changed = |s: Schema| assert_ne!(base, schema_fingerprint(&s), "{s:?}");
        changed(simple(&[("id", DataType::Int32), ("nome", DataType::Utf8)]));
        changed(simple(&[("nome", DataType::Utf8), ("id", DataType::Int64)]));
        changed(simple(&[("id", DataType::Int64)]));
        let dec = |p, s| schema_fingerprint(&simple(&[("d", DataType::Decimal64(p, s))]));
        assert_ne!(dec(18, 2), dec(18, 4));
        assert_ne!(dec(18, 2), dec(10, 2));
        changed(simple(&[("id", DataType::Int64), ("nome", DataType::LargeUtf8)]));
        // Nullability is part of the shape.
        changed(schema(&[("id", DataType::Int64, false), ("nome", DataType::Utf8, true)]));
        // Length prefixes keep field boundaries unambiguous.
        assert_ne!(
            schema_fingerprint(&simple(&[("ab", DataType::Utf8), ("c", DataType::Utf8)])),
            schema_fingerprint(&simple(&[("a", DataType::Utf8), ("bc", DataType::Utf8)]))
        );
        // Units, time zones, precision and children all count.
        let fp = |t: DataType| schema_fingerprint(&simple(&[("c", t)]));
        let all = [
            fp(DataType::Timestamp(TimeUnit::Millisecond, None)),
            fp(DataType::Timestamp(TimeUnit::Microsecond, None)),
            fp(DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))),
            fp(DataType::Timestamp(TimeUnit::Microsecond, Some("".into()))),
            fp(DataType::Decimal128(12, 2)),
            fp(DataType::Decimal128(12, 3)),
            fp(DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))),
            fp(DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)))),
            fp(DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))),
        ];
        let unique: std::collections::BTreeSet<_> = all.iter().collect();
        assert_eq!(unique.len(), all.len(), "{all:?}");
    }

    #[test]
    fn schema_fingerprint_ignores_metadata() {
        let plain = simple(&[("id", DataType::Int64)]);
        let with_meta = Schema::new(vec![Field::new("id", DataType::Int64, true).with_metadata(
            std::collections::HashMap::from([("comment".to_string(), "primary key".to_string())]),
        )])
        .with_metadata(std::collections::HashMap::from([("origin".to_string(), "pg".to_string())]));
        assert_eq!(schema_fingerprint(&plain), schema_fingerprint(&with_meta));
    }

    #[test]
    fn new_docs_carry_the_current_version() {
        let (_tmp, path) = tmp_state_path("version");
        let backend = LocalFileBackend::new(&path);
        backend.store(&StateDoc::new("p1", serde_json::json!(1), "h", "r")).unwrap();
        let raw: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(raw["version"], PLAIN_FORMAT_VERSION, "no resume record: format 2");
        assert!(raw.get("resume").is_none());
        let doc = backend.load("p1").unwrap().unwrap();
        assert!(!doc.is_legacy());
        assert!(doc.fingerprint_is_comparable());
    }

    /// A document with a resume record is format 3 (an older loadsmith
    /// refuses it); once the run succeeds it is format 2 again.
    #[test]
    fn a_resume_record_is_format_3_and_roundtrips() {
        let (_tmp, path) = tmp_state_path("resume");
        let backend = LocalFileBackend::new(&path);
        let record = ResumeRecord {
            run_id: "4242-1781078400000".into(),
            started_at_unix_ms: 1_781_078_400_000,
            config_hash: config_fingerprint("{}"),
            next_object_seq: Some(9),
            token: Some(serde_json::json!({ "done": [0, 1, 4] })),
        };
        backend.store(&StateDoc::new("p1", serde_json::Value::Null, "h", "r").with_resume(Some(record.clone()))).unwrap();
        let doc = backend.load("p1").unwrap().unwrap();
        assert_eq!(doc.version, 3);
        assert_eq!(doc.resume.as_ref(), Some(&record));
        assert_eq!(doc.watermark(), None, "a null cursor is no watermark");

        backend.store(&StateDoc::new("p1", serde_json::json!(5), "h", "r").with_resume(None)).unwrap();
        let doc = backend.load("p1").unwrap().unwrap();
        assert_eq!((doc.version, doc.resume.clone(), doc.watermark()), (2, None, Some(&serde_json::json!(5))));
    }

    #[test]
    fn config_fingerprints_differ_with_the_config() {
        assert_eq!(config_fingerprint(r#"{"a":1}"#), config_fingerprint(r#"{"a":1}"#));
        assert_ne!(config_fingerprint(r#"{"a":1}"#), config_fingerprint(r#"{"a":2}"#));
    }

    /// Migration: an older state file must load, keep its cursor, pass the
    /// drift check even under `refuse` (its fingerprint is not comparable),
    /// and be upgraded to the current format by the next save.
    fn assert_migrates(tag: &str, raw: &str, expect_version: u32) {
        let (_tmp, path) = tmp_state_path(tag);
        fs::write(&path, raw).unwrap();
        let backend = LocalFileBackend::new(&path);
        let prior = backend.load("p1").unwrap().unwrap();
        assert_eq!(prior.version, expect_version);
        assert!(!prior.fingerprint_is_comparable());
        assert_eq!(prior.cursor_value, "2026-06-09T10:00:00");

        let current = schema_fingerprint(&simple(&[("id", DataType::Int64)]));
        assert_ne!(prior.schema_hash, current);
        check_schema_drift(&prior, &current, SchemaChangePolicy::Refuse)
            .expect("an old fingerprint must not trigger a drift refusal");

        // Next save (what the state supervisor does) rewrites it in the new format.
        backend.store(&StateDoc::new("p1", serde_json::json!("2026-06-10"), &current, "r2")).unwrap();
        let upgraded = backend.load("p1").unwrap().unwrap();
        assert_eq!(upgraded.version, PLAIN_FORMAT_VERSION);
        assert!(upgraded.fingerprint_is_comparable());
        assert_eq!(upgraded.schema_hash, current);
        check_schema_drift(&upgraded, &current, SchemaChangePolicy::Refuse).unwrap();
        // And from now on drift is detected again.
        assert!(check_schema_drift(&upgraded, "0000000000000000", SchemaChangePolicy::Refuse).is_err());
    }

    /// Format 0: written by loadsmith ≤ 0.2.0, no `version`, a DefaultHasher
    /// fingerprint.
    #[test]
    fn legacy_doc_loads_and_is_upgraded_on_next_save() {
        assert_migrates(
            "legacy",
            r#"{
  "pipeline": "p1",
  "cursor_value": "2026-06-09T10:00:00",
  "schema_hash": "1f2e3d4c5b6a7980",
  "run_id": "4242-1760000000000",
  "updated_at_unix_ms": 1760000000000
}"#,
            0,
        );
    }

    /// Format 1: FNV-1a over the lossy protocol labels (`a8e4fe49824728ac` is
    /// what it recorded for `id: int64, nome: utf8`).
    #[test]
    fn v1_doc_loads_and_is_upgraded_on_next_save() {
        assert_migrates(
            "v1",
            r#"{
  "version": 1,
  "pipeline": "p1",
  "cursor_value": "2026-06-09T10:00:00",
  "schema_hash": "a8e4fe49824728ac",
  "run_id": "4242-1781078400000",
  "updated_at_unix_ms": 1781078400000
}"#,
            1,
        );
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

    /// A backend that watches a held lease briefly (tests can't wait a TTL).
    fn quick(path: &Path) -> LocalFileBackend {
        LocalFileBackend { path: path.to_path_buf(), observe: Duration::from_millis(400) }
    }

    /// Locks on a blocking thread, as the runner does: the watch sleeps.
    async fn lock_async(path: &Path) -> Result<LockGuard, CoreError> {
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || quick(&path).lock("p1")).await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_renewed_lease_excludes_a_second_run() {
        let (_tmp, path) = tmp_state_path("lock-excl");
        let backend = quick(&path);
        let mut guard = backend.lock("p1").unwrap();
        guard.start_renewal_every(Duration::from_millis(50));
        let err = lock_async(&path).await.err().expect("a live holder keeps the lock").to_string();
        assert!(err.contains("being renewed"), "{err}");
        drop(guard);
        // After releasing, locking succeeds again, at once.
        let _g2 = backend.lock("p1").unwrap();
    }

    /// A crashed run (SIGKILL) leaves a fresh lease that nobody renews: the
    /// next run takes it after watching it for a TTL, instead of failing.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unrenewed_lease_is_taken_after_watching_it() {
        let (_tmp, path) = tmp_state_path("lock-crashed");
        let backend = quick(&path);
        let crashed = backend.lock("p1").unwrap();
        std::mem::forget(crashed); // no renewal, no release: the process died
        let began = std::time::Instant::now();
        let _g = lock_async(&path).await.expect("an unrenewed lease is a dead holder's");
        assert!(began.elapsed() >= Duration::from_millis(400), "watched for the whole window first");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_lock_released_while_watched_is_taken() {
        let (_tmp, path) = tmp_state_path("lock-released");
        let backend = quick(&path);
        let guard = backend.lock("p1").unwrap();
        let watched = path.clone();
        let waiting = tokio::spawn(async move { lock_async(&watched).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(guard);
        assert!(waiting.await.unwrap().is_ok());
    }

    #[test]
    fn steals_expired_lease() {
        let (_tmp, path) = tmp_state_path("lock-steal");
        let _ = fs::remove_file(&path);
        let backend = LocalFileBackend::new(&path);
        // Write a lock whose lease was last renewed long before LEASE_TTL ago.
        let stale = LockFile { holder: "999999".into(), renewed_at_unix_ms: 1 };
        fs::write(backend.lock_path(), serde_json::to_string(&stale).unwrap()).unwrap();
        // Expired ⇒ stolen at once, not watched. No process-liveness probe involved.
        let began = std::time::Instant::now();
        let _g = backend.lock("p1").unwrap();
        assert!(began.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn renew_refreshes_the_lease() {
        let (_tmp, path) = tmp_state_path("lock-renew");
        let backend = LocalFileBackend::new(&path);
        let guard = backend.lock("p1").unwrap();
        // Backdate our own lease past TTL, then renew: it is fresh again.
        backdate(&backend.lock_path(), &holder_of(&guard));
        guard.renew();
        assert!(lock_file(&backend.lock_path()).renewed_at_unix_ms > 1);
        assert_eq!(lock_file(&backend.lock_path()).holder, holder_of(&guard));
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
        assert_eq!(lock_file(&backend.lock_path()).holder, thief_holder, "the thief still holds the lock");
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
    #[tokio::test(flavor = "multi_thread")]
    async fn renewal_runs_from_acquisition() {
        let (_tmp, path) = tmp_state_path("lock-autorenew");
        let backend = quick(&path);
        let mut guard = backend.lock("p1").unwrap();
        guard.start_renewal_every(Duration::from_millis(10));
        backdate(&backend.lock_path(), &holder_of(&guard));

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while lock_file(&backend.lock_path()).renewed_at_unix_ms == 1 {
            assert!(std::time::Instant::now() < deadline, "lease was never renewed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(lock_async(&path).await.is_err());

        drop(guard);
        // Released, and the aborted renewal task does not recreate it.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!backend.lock_path().exists());
    }
}
