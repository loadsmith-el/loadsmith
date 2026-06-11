//! Supervises incremental-state persistence during a run.
//!
//! Mirrors the sink supervisor's shape: a concurrent task fed by the event
//! drains. It receives `Checkpoint` (batch_seq → opaque watermark) from the
//! source's fd4 drain and `Committed` (batch_seq) from the destination's, and
//! persists the **safe** watermark — the watermark of the highest batch the
//! destination has confirmed durable. That durability gate is what makes resume
//! gap-free: everything `≤` the persisted watermark is durably at the
//! destination, so the next run rereads strictly after it (at-least-once;
//! boundary dups absorbed by an idempotent destination).
//!
//! Persistence is throttled by `checkpoint_interval` (batches). `0` ⇒ persist
//! only once, at the end. Every persisted value is durability-gated, so flushing
//! mid-run — even if the run later fails — never records a watermark ahead of
//! what landed.

use std::collections::BTreeMap;

use serde_json::Value;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::error::CoreError;
use crate::state::{LockGuard, StateBackend, StateDoc};

/// Identity + policy for the run whose state is being tracked.
pub struct StateRun {
    pub pipeline: String,
    pub schema_hash: String,
    pub run_id: String,
    /// Persist at most once per this many durably-committed batches. `0` ⇒ only
    /// a single persist at the end.
    pub checkpoint_interval: u64,
}

/// Runs until both event channels close (i.e. both plugins have exited), then
/// does a final flush. Returns the last watermark actually persisted, if any.
///
/// `guard` is held for the lifetime of this task and dropped on return, so the
/// pipeline's state lock is released exactly when state tracking ends.
pub async fn run_state_supervisor(
    backend: Box<dyn StateBackend>,
    guard: LockGuard,
    run: StateRun,
    mut checkpoint_rx: UnboundedReceiver<(u64, Value)>,
    mut committed_rx: UnboundedReceiver<u64>,
) -> Result<Option<Value>, CoreError> {
    let mut watermark_at: BTreeMap<u64, Value> = BTreeMap::new();
    let mut durable_through: u64 = 0;
    let mut last_persisted_seq: u64 = 0;
    let mut persisted: Option<Value> = None;
    let mut ckpt_open = true;
    let mut commit_open = true;

    // Renew the lock lease on a wall clock so a live run — even a quiet one
    // (e.g. a long query producing no batches) — never has its lock stolen.
    let mut renew = tokio::time::interval(crate::state::LEASE_RENEW_INTERVAL);
    renew.tick().await; // discard the immediate first tick

    while ckpt_open || commit_open {
        tokio::select! {
            ckpt = checkpoint_rx.recv(), if ckpt_open => match ckpt {
                Some((seq, value)) => { watermark_at.insert(seq, value); }
                None => ckpt_open = false,
            },
            commit = committed_rx.recv(), if commit_open => match commit {
                Some(seq) => { durable_through = durable_through.max(seq); }
                None => commit_open = false,
            },
            _ = renew.tick() => guard.renew(),
        }

        // Throttled, durability-gated persist. interval 0 ⇒ defer to final flush.
        if run.checkpoint_interval > 0 {
            if let Some((seq, value)) = safe_watermark(&watermark_at, durable_through) {
                if seq >= last_persisted_seq + run.checkpoint_interval {
                    persist(&*backend, &run, &value)?;
                    last_persisted_seq = seq;
                    persisted = Some(value);
                }
            }
        }
    }

    // Final flush: persist the latest safe watermark if it advanced.
    if let Some((seq, value)) = safe_watermark(&watermark_at, durable_through) {
        if seq > last_persisted_seq {
            persist(&*backend, &run, &value)?;
            persisted = Some(value);
        }
    }

    drop(guard); // release the pipeline state lock
    Ok(persisted)
}

/// The (batch_seq, watermark) of the highest checkpoint at or below the durable
/// frontier — the latest watermark it is safe to persist.
fn safe_watermark(watermark_at: &BTreeMap<u64, Value>, durable_through: u64) -> Option<(u64, Value)> {
    watermark_at
        .range(..=durable_through)
        .next_back()
        .map(|(seq, value)| (*seq, value.clone()))
}

fn persist(backend: &dyn StateBackend, run: &StateRun, value: &Value) -> Result<(), CoreError> {
    let doc = StateDoc::new(
        run.pipeline.clone(),
        value.clone(),
        run.schema_hash.clone(),
        run.run_id.clone(),
    );
    backend.store(&doc)?;
    tracing::debug!(pipeline = %run.pipeline, "persisted incremental watermark");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::LocalFileBackend;
    use std::fs;
    use std::path::PathBuf;

    fn tmp_path(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("loadsmith-statesup-test-{}-{}", tag, std::process::id()));
        let _ = fs::create_dir_all(&dir);
        dir.join("state.json")
    }

    #[test]
    fn safe_watermark_respects_durable_frontier() {
        let mut m = BTreeMap::new();
        m.insert(1u64, serde_json::json!("a"));
        m.insert(2u64, serde_json::json!("b"));
        m.insert(3u64, serde_json::json!("c"));
        // Only batches 1 and 2 are durable → watermark is "b".
        assert_eq!(safe_watermark(&m, 2), Some((2, serde_json::json!("b"))));
        // Nothing durable yet → no safe watermark.
        assert_eq!(safe_watermark(&m, 0), None);
        // Durable past the last checkpoint → latest checkpoint.
        assert_eq!(safe_watermark(&m, 9), Some((3, serde_json::json!("c"))));
    }

    #[tokio::test]
    async fn persists_only_durable_watermark_at_end() {
        let path = tmp_path("durable-end");
        let _ = fs::remove_file(&path);
        let backend = Box::new(LocalFileBackend::new(&path));
        let guard = backend.lock("p1").unwrap();

        let (ck_tx, ck_rx) = tokio::sync::mpsc::unbounded_channel();
        let (cm_tx, cm_rx) = tokio::sync::mpsc::unbounded_channel();

        // Source produced watermarks for batches 1,2,3 but only 1,2 are durable.
        ck_tx.send((1, serde_json::json!(10))).unwrap();
        ck_tx.send((2, serde_json::json!(20))).unwrap();
        ck_tx.send((3, serde_json::json!(30))).unwrap();
        cm_tx.send(2).unwrap();
        drop(ck_tx);
        drop(cm_tx);

        let run = StateRun {
            pipeline: "p1".into(),
            schema_hash: "h".into(),
            run_id: "r".into(),
            checkpoint_interval: 0,
        };
        let persisted = run_state_supervisor(backend, guard, run, ck_rx, cm_rx).await.unwrap();
        assert_eq!(persisted, Some(serde_json::json!(20)));

        // And the file reflects the durable watermark, not the unconfirmed 30.
        let doc = LocalFileBackend::new(&path).load("p1").unwrap().unwrap();
        assert_eq!(doc.cursor_value, 20);
    }
}
