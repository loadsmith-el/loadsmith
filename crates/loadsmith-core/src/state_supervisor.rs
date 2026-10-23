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
//!
//! **With a sink, the destination's `committed` is not durability.** A file
//! destination commits when it has written its files locally; they reach the
//! target only when the sink delivers them. So with a sink the durable
//! frontier is the **delivered prefix**: the highest batch whose files (and
//! every earlier file) the sink acknowledged, from the batch ranges the
//! destination announces with each file. The watermark advances file by file,
//! throttled like any other. A destination that announces no ranges keeps the
//! *delivery gate* alone: nothing is persisted before the sink reports every
//! object delivered. If delivery fails, the watermark stays at the delivered
//! prefix and the next run rereads the rest.

use std::collections::BTreeMap;

use serde_json::Value;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;

use crate::error::CoreError;
use crate::state::{LockGuard, StateBackend, StateDoc};
use crate::telemetry;

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
/// `delivery` is the delivery gate of a run with a sink: it resolves `true`
/// once the sink has delivered every object, `false` (or is dropped) if it did
/// not. `delivered_rx` carries the sink's delivered prefix as it advances;
/// with a sink it is the durable frontier, not the destination's `committed`.
///
/// `guard` is held for the lifetime of this task and dropped on return, so the
/// pipeline's state lock is released exactly when state tracking ends.
pub async fn run_state_supervisor(
    backend: Box<dyn StateBackend>,
    guard: LockGuard,
    run: StateRun,
    mut checkpoint_rx: UnboundedReceiver<(u64, Value)>,
    mut committed_rx: UnboundedReceiver<u64>,
    delivery: Option<oneshot::Receiver<bool>>,
    mut delivered_rx: Option<UnboundedReceiver<u64>>,
) -> Result<Option<Value>, CoreError> {
    let with_sink = delivery.is_some();
    let mut delivered_through: u64 = 0;
    let mut watermark_at: BTreeMap<u64, Value> = BTreeMap::new();
    let mut durable_through: u64 = 0;
    let mut last_persisted_seq: u64 = 0;
    let mut persisted: Option<Value> = None;
    let mut ckpt_open = true;
    let mut commit_open = true;

    // The lease is renewed by the guard's own background task, started at
    // acquisition (`LockGuard::start_renewal`) — not here, so it also covers
    // the time before this supervisor exists (spawn, handshake, first query).
    let mut latest_checkpoint: u64 = 0;
    while ckpt_open || commit_open {
        tokio::select! {
            ckpt = checkpoint_rx.recv(), if ckpt_open => match ckpt {
                Some((seq, value)) => {
                    latest_checkpoint = latest_checkpoint.max(seq);
                    watermark_at.insert(seq, value);
                    telemetry::record_checkpoint_lag(latest_checkpoint.saturating_sub(last_persisted_seq));
                }
                None => ckpt_open = false,
            },
            commit = committed_rx.recv(), if commit_open => match commit {
                Some(seq) => { durable_through = durable_through.max(seq); }
                None => commit_open = false,
            },
            seq = next_delivered(&mut delivered_rx) => {
                delivered_through = delivered_through.max(seq);
            }
        }

        // Throttled, durability-gated persist. interval 0 ⇒ defer to final flush.
        if run.checkpoint_interval > 0 {
            let frontier = if with_sink { delivered_through } else { durable_through };
            if let Some((seq, value)) = safe_watermark(&watermark_at, frontier) {
                if seq >= last_persisted_seq + run.checkpoint_interval {
                    let span = tracing::info_span!(target: "loadsmith::state", "checkpoint", loadsmith.batch.seq = seq);
                    span.in_scope(|| persist(&*backend, &run, &value))?;
                    last_persisted_seq = seq;
                    persisted = Some(value);
                    telemetry::record_checkpoint_lag(latest_checkpoint.saturating_sub(last_persisted_seq));
                }
            }
        }
    }

    // Delivery gate: committed files only count once the sink delivered them.
    // Short of that, the delivered prefix does (the sink has exited when the
    // gate resolves, so every prefix it reported is queued by then).
    let mut frontier = durable_through;
    if let Some(gate) = delivery {
        let all_delivered = gate.await.unwrap_or(false);
        if let Some(rx) = delivered_rx.as_mut() {
            while let Ok(seq) = rx.try_recv() {
                delivered_through = delivered_through.max(seq);
            }
        }
        if !all_delivered {
            frontier = delivered_through;
            tracing::warn!(
                pipeline = %run.pipeline,
                delivered_through,
                "the sink did not deliver every object: the incremental watermark advances only \
                 through the delivered files"
            );
        }
    }

    // Final flush: persist the latest safe watermark if it advanced.
    if let Some((seq, value)) = safe_watermark(&watermark_at, frontier) {
        if seq > last_persisted_seq {
            let span = tracing::info_span!(target: "loadsmith::state", "state.commit", loadsmith.batch.seq = seq);
            span.in_scope(|| persist(&*backend, &run, &value))?;
            last_persisted_seq = seq;
            persisted = Some(value);
        }
    }
    telemetry::record_checkpoint_lag(latest_checkpoint.saturating_sub(last_persisted_seq));

    drop(guard); // release the pipeline state lock
    Ok(persisted)
}

/// The next delivered prefix; never, without a sink (or once it closed).
async fn next_delivered(rx: &mut Option<UnboundedReceiver<u64>>) -> u64 {
    match rx {
        Some(r) => match r.recv().await {
            Some(seq) => seq,
            None => {
                *rx = None;
                std::future::pending().await
            }
        },
        None => std::future::pending().await,
    }
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
    let began = std::time::Instant::now();
    backend.store(&doc)?;
    telemetry::record_state_commit(began.elapsed());

    tracing::debug!(pipeline = %run.pipeline, "persisted incremental watermark");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::LocalFileBackend;
    use std::fs;
    use std::path::PathBuf;

    /// Removes the test's temp dir when dropped (so `cargo test` leaves no
    /// `/tmp/loadsmith-*-test-*` behind).
    struct TmpDir(PathBuf);
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn tmp_path(tag: &str) -> (TmpDir, PathBuf) {
        let dir = std::env::temp_dir()
            .join(format!("loadsmith-statesup-test-{}-{}", tag, std::process::id()));
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("state.json");
        (TmpDir(dir), path)
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
        let (_tmp, path) = tmp_path("durable-end");
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
        let persisted = run_state_supervisor(backend, guard, run, ck_rx, cm_rx, None, None).await.unwrap();
        assert_eq!(persisted, Some(serde_json::json!(20)));

        // And the file reflects the durable watermark, not the unconfirmed 30.
        let doc = LocalFileBackend::new(&path).load("p1").unwrap().unwrap();
        assert_eq!(doc.cursor_value, 20);
    }

    /// Runs the supervisor over a fully committed run (batches 1..=2) with a
    /// delivery gate resolved to `delivered`, after the sink reported the
    /// delivered `prefixes`; returns what it persisted.
    async fn gated_run(
        tag: &str,
        interval: u64,
        delivered: Option<bool>,
        prefixes: &[u64],
    ) -> (Option<Value>, Option<StateDoc>) {
        let (_tmp, path) = tmp_path(tag);
        let backend = Box::new(LocalFileBackend::new(&path));
        let guard = backend.lock("p1").unwrap();
        let (ck_tx, ck_rx) = tokio::sync::mpsc::unbounded_channel();
        let (cm_tx, cm_rx) = tokio::sync::mpsc::unbounded_channel();
        ck_tx.send((1, serde_json::json!(10))).unwrap();
        ck_tx.send((2, serde_json::json!(20))).unwrap();
        cm_tx.send(2).unwrap();
        drop(ck_tx);
        drop(cm_tx);
        let (dl_tx, dl_rx) = tokio::sync::mpsc::unbounded_channel();
        for p in prefixes {
            dl_tx.send(*p).unwrap();
        }
        drop(dl_tx);
        let (gate_tx, gate_rx) = oneshot::channel();
        match delivered {
            Some(ok) => gate_tx.send(ok).unwrap(),
            None => drop(gate_tx), // the sink task died without answering
        }
        let run = StateRun {
            pipeline: "p1".into(),
            schema_hash: "h".into(),
            run_id: "r".into(),
            checkpoint_interval: interval,
        };
        let persisted = run_state_supervisor(backend, guard, run, ck_rx, cm_rx, Some(gate_rx), Some(dl_rx)).await.unwrap();
        (persisted, LocalFileBackend::new(&path).load("p1").unwrap())
    }

    #[tokio::test]
    async fn a_sink_failure_keeps_the_watermark() {
        // Committed by the destination, but the sink failed: nothing persisted,
        // mid-run (interval 1) or at the end.
        for (tag, interval) in [("gate-fail-end", 0), ("gate-fail-mid", 1)] {
            let (persisted, doc) = gated_run(tag, interval, Some(false), &[]).await;
            assert_eq!(persisted, None, "{tag}");
            assert!(doc.is_none(), "{tag}");
        }
        let (persisted, doc) = gated_run("gate-dropped", 0, None, &[]).await;
        assert_eq!(persisted, None);
        assert!(doc.is_none());
    }

    #[tokio::test]
    async fn a_full_delivery_persists_the_watermark() {
        let (persisted, doc) = gated_run("gate-ok", 1, Some(true), &[]).await;
        assert_eq!(persisted, Some(serde_json::json!(20)));
        assert_eq!(doc.unwrap().cursor_value, 20);
    }

    /// The sink delivered the file holding batch 1, then failed: the
    /// watermark advances through batch 1 only — mid-run or at the end.
    #[tokio::test]
    async fn a_sink_failure_keeps_the_delivered_prefix() {
        for (tag, interval) in [("prefix-end", 0), ("prefix-mid", 1)] {
            let (persisted, doc) = gated_run(tag, interval, Some(false), &[1]).await;
            assert_eq!(persisted, Some(serde_json::json!(10)), "{tag}");
            assert_eq!(doc.unwrap().cursor_value, 10, "{tag}");
        }
    }
}
