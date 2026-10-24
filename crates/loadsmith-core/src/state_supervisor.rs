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
use crate::events::SourceCheckpoint;
use crate::sink_supervisor::Delivered;
use crate::state::{LockGuard, ResumeRecord, StateBackend, StateDoc};
use crate::telemetry;

/// Identity + policy for the run whose state is being tracked.
pub struct StateRun {
    pub pipeline: String,
    pub schema_hash: String,
    pub run_id: String,
    /// Persist at most once per this many durably-committed batches. `0` ⇒ only
    /// a single persist at the end.
    pub checkpoint_interval: u64,
    /// What a resume record names, when the run can be resumed (see
    /// [`ResumeRecord`]): kept in the state while the run has not succeeded.
    pub resume: Option<ResumeIdentity>,
    /// The document the run started from: on success its resume record is
    /// cleared even when nothing new was persisted.
    pub prior: Option<StateDoc>,
}

/// The run as a resume record names it.
pub struct ResumeIdentity {
    pub started_at_unix_ms: u64,
    pub config_hash: String,
    /// With a sink: the number of this attempt's first staged file; the
    /// record's `next_object_seq` counts the delivered ones from it.
    pub first_object_seq: Option<u64>,
}

/// The inputs of the supervisor, fed by the event drains and the runner.
pub struct StateInputs {
    pub checkpoints: UnboundedReceiver<SourceCheckpoint>,
    pub committed: UnboundedReceiver<u64>,
    /// The delivery gate of a run with a sink: resolves `true` once the sink
    /// has delivered every object, `false` (or is dropped) if it did not.
    pub delivery: Option<oneshot::Receiver<bool>>,
    /// The sink's delivered prefix as it advances; with a sink it is the
    /// durable frontier, not the destination's `committed`.
    pub delivered: Option<UnboundedReceiver<Delivered>>,
    /// Resolves `true` when every plugin finished successfully (the sink
    /// aside, which the delivery gate reports): the run succeeded, and its
    /// resume record is cleared. Dropped or `false`: the run failed.
    pub outcome: Option<oneshot::Receiver<bool>>,
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
    inputs: StateInputs,
) -> Result<Option<Value>, CoreError> {
    let StateInputs { mut checkpoints, mut committed, delivery, delivered: mut delivered_rx, outcome } = inputs;
    let with_sink = delivery.is_some();
    let mut delivered = Delivered { through: 0, objects: 0 };
    let mut checkpoint_at: BTreeMap<u64, SourceCheckpoint> = BTreeMap::new();
    let mut durable_through: u64 = 0;
    let mut last_persisted_seq: u64 = 0;
    let mut persisted: Option<Value> = None;
    let mut record_written = false;
    let mut ckpt_open = true;
    let mut commit_open = true;

    // The lease is renewed by the guard's own background task, started at
    // acquisition (`LockGuard::start_renewal`) — not here, so it also covers
    // the time before this supervisor exists (spawn, handshake, first query).
    let mut latest_checkpoint: u64 = 0;
    while ckpt_open || commit_open {
        tokio::select! {
            ckpt = checkpoints.recv(), if ckpt_open => match ckpt {
                Some(c) => {
                    latest_checkpoint = latest_checkpoint.max(c.batch_seq);
                    checkpoint_at.insert(c.batch_seq, c);
                    telemetry::record_checkpoint_lag(latest_checkpoint.saturating_sub(last_persisted_seq));
                }
                None => ckpt_open = false,
            },
            commit = committed.recv(), if commit_open => match commit {
                Some(seq) => { durable_through = durable_through.max(seq); }
                None => commit_open = false,
            },
            d = next_delivered(&mut delivered_rx) => {
                if d.through > delivered.through {
                    delivered = d;
                }
            }
        }

        // Throttled, durability-gated persist. interval 0 ⇒ defer to final flush.
        if run.checkpoint_interval > 0 {
            let frontier = if with_sink { delivered.through } else { durable_through };
            if let Some(ckpt) = safe_checkpoint(&checkpoint_at, frontier) {
                if ckpt.batch_seq >= last_persisted_seq + run.checkpoint_interval {
                    let span = tracing::info_span!(target: "loadsmith::state", "checkpoint", loadsmith.batch.seq = ckpt.batch_seq);
                    let record = record_for(&run, ckpt, with_sink.then_some(delivered));
                    span.in_scope(|| persist(&*backend, &run, &ckpt.cursor_value, record.clone()))?;
                    record_written |= record.is_some();
                    last_persisted_seq = ckpt.batch_seq;
                    persisted = Some(ckpt.cursor_value.clone());
                    telemetry::record_checkpoint_lag(latest_checkpoint.saturating_sub(last_persisted_seq));
                }
            }
        }
    }

    // Delivery gate: committed files only count once the sink delivered them.
    // Short of that, the delivered prefix does (the sink has exited when the
    // gate resolves, so every prefix it reported is queued by then).
    let mut frontier = durable_through;
    let mut all_delivered = true;
    if let Some(gate) = delivery {
        all_delivered = gate.await.unwrap_or(false);
        if let Some(rx) = delivered_rx.as_mut() {
            while let Ok(d) = rx.try_recv() {
                if d.through > delivered.through {
                    delivered = d;
                }
            }
        }
        if !all_delivered {
            frontier = delivered.through;
            tracing::warn!(
                pipeline = %run.pipeline,
                delivered_through = delivered.through,
                "the sink did not deliver every object: the incremental watermark advances only \
                 through the delivered files"
            );
        }
    }
    // The run succeeded: its resume record goes.
    let succeeded = all_delivered
        && match outcome {
            Some(rx) => rx.await.unwrap_or(false),
            None => false,
        };

    // Final flush: persist the latest safe watermark if it advanced — and,
    // when the run succeeded, drop a resume record written before.
    let final_ckpt = safe_checkpoint(&checkpoint_at, frontier);
    let had_record = record_written || run.prior.as_ref().is_some_and(|p| p.resume.is_some());
    match final_ckpt {
        Some(ckpt) if ckpt.batch_seq > last_persisted_seq || (succeeded && had_record) => {
            let span = tracing::info_span!(target: "loadsmith::state", "state.commit", loadsmith.batch.seq = ckpt.batch_seq);
            let record = if succeeded { None } else { record_for(&run, ckpt, with_sink.then_some(delivered)) };
            span.in_scope(|| persist(&*backend, &run, &ckpt.cursor_value, record))?;
            last_persisted_seq = ckpt.batch_seq;
            persisted = Some(ckpt.cursor_value.clone());
        }
        None if succeeded && had_record => {
            // Nothing new was read: keep the watermark, end the resumed run.
            if let Some(prior) = &run.prior {
                persist(&*backend, &run, &prior.cursor_value, None)?;
            }
        }
        _ => {}
    }
    telemetry::record_checkpoint_lag(latest_checkpoint.saturating_sub(last_persisted_seq));

    drop(guard); // release the pipeline state lock
    Ok(persisted)
}

/// The resume record that goes with the checkpoint `ckpt` (persisted at the
/// durable frontier): none when the run cannot be resumed.
fn record_for(run: &StateRun, ckpt: &SourceCheckpoint, delivered: Option<Delivered>) -> Option<ResumeRecord> {
    let identity = run.resume.as_ref()?;
    let next_object_seq = match (delivered, identity.first_object_seq) {
        (Some(d), first) => Some(first.unwrap_or(1) + d.objects),
        (None, _) => None,
    };
    if next_object_seq.is_none() && ckpt.resume_token.is_none() {
        return None; // nothing a resumed run needs beyond the watermark
    }
    Some(ResumeRecord {
        run_id: run.run_id.clone(),
        started_at_unix_ms: identity.started_at_unix_ms,
        config_hash: identity.config_hash.clone(),
        next_object_seq,
        token: ckpt.resume_token.clone(),
    })
}

/// The next delivered prefix; never, without a sink (or once it closed).
async fn next_delivered(rx: &mut Option<UnboundedReceiver<Delivered>>) -> Delivered {
    match rx {
        Some(r) => match r.recv().await {
            Some(d) => d,
            None => {
                *rx = None;
                std::future::pending().await
            }
        },
        None => std::future::pending().await,
    }
}

/// The highest checkpoint at or below the durable frontier — the latest
/// watermark (and resume token) it is safe to persist.
fn safe_checkpoint(checkpoint_at: &BTreeMap<u64, SourceCheckpoint>, durable_through: u64) -> Option<&SourceCheckpoint> {
    checkpoint_at.range(..=durable_through).next_back().map(|(_, c)| c)
}

fn persist(backend: &dyn StateBackend, run: &StateRun, value: &Value, resume: Option<ResumeRecord>) -> Result<(), CoreError> {
    let doc = StateDoc::new(
        run.pipeline.clone(),
        value.clone(),
        run.schema_hash.clone(),
        run.run_id.clone(),
    )
    .with_resume(resume);
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

    fn ckpt(seq: u64, cursor: Value, token: Option<Value>) -> SourceCheckpoint {
        SourceCheckpoint { batch_seq: seq, cursor_value: cursor, resume_token: token }
    }

    fn state_run(interval: u64, resume: Option<ResumeIdentity>, prior: Option<StateDoc>) -> StateRun {
        StateRun {
            pipeline: "p1".into(),
            schema_hash: "h".into(),
            run_id: "r".into(),
            checkpoint_interval: interval,
            resume,
            prior,
        }
    }

    fn identity(first_object_seq: Option<u64>) -> ResumeIdentity {
        ResumeIdentity { started_at_unix_ms: 7, config_hash: "c".into(), first_object_seq }
    }

    /// One run of the supervisor over `checkpoints`, the destination
    /// committing through `committed`; with a sink when `delivery` is given
    /// (its gate, then the delivered prefixes); `ok` is the run's outcome.
    struct Scenario {
        checkpoints: Vec<SourceCheckpoint>,
        committed: u64,
        delivery: Option<(Option<bool>, Vec<Delivered>)>,
        ok: bool,
        run: StateRun,
    }

    async fn supervise(tag: &str, sc: Scenario) -> (Option<Value>, Option<StateDoc>) {
        let (_tmp, path) = tmp_path(tag);
        let backend = Box::new(LocalFileBackend::new(&path));
        if let Some(prior) = &sc.run.prior {
            backend.store(prior).unwrap();
        }
        let guard = backend.lock("p1").unwrap();
        let (ck_tx, ck_rx) = tokio::sync::mpsc::unbounded_channel();
        let (cm_tx, cm_rx) = tokio::sync::mpsc::unbounded_channel();
        for c in sc.checkpoints {
            ck_tx.send(c).unwrap();
        }
        if sc.committed > 0 {
            cm_tx.send(sc.committed).unwrap();
        }
        drop(ck_tx);
        drop(cm_tx);
        let (delivery, delivered) = match sc.delivery {
            None => (None, None),
            Some((gate, prefixes)) => {
                let (dl_tx, dl_rx) = tokio::sync::mpsc::unbounded_channel();
                for p in prefixes {
                    dl_tx.send(p).unwrap();
                }
                drop(dl_tx);
                let (gate_tx, gate_rx) = oneshot::channel();
                match gate {
                    Some(ok) => gate_tx.send(ok).unwrap(),
                    None => drop(gate_tx), // the sink task died without answering
                }
                (Some(gate_rx), Some(dl_rx))
            }
        };
        let (out_tx, out_rx) = oneshot::channel();
        out_tx.send(sc.ok).unwrap();
        let inputs = StateInputs { checkpoints: ck_rx, committed: cm_rx, delivery, delivered, outcome: Some(out_rx) };
        let persisted = run_state_supervisor(backend, guard, sc.run, inputs).await.unwrap();
        (persisted, LocalFileBackend::new(&path).load("p1").unwrap())
    }

    fn two_batches() -> Vec<SourceCheckpoint> {
        vec![ckpt(1, serde_json::json!(10), None), ckpt(2, serde_json::json!(20), None)]
    }

    #[test]
    fn safe_checkpoint_respects_durable_frontier() {
        let mut m = BTreeMap::new();
        for (seq, v) in [(1u64, "a"), (2, "b"), (3, "c")] {
            m.insert(seq, ckpt(seq, serde_json::json!(v), None));
        }
        let at = |frontier| safe_checkpoint(&m, frontier).map(|c| c.cursor_value.clone());
        // Only batches 1 and 2 are durable → watermark is "b".
        assert_eq!(at(2), Some(serde_json::json!("b")));
        // Nothing durable yet → no safe watermark.
        assert_eq!(at(0), None);
        // Durable past the last checkpoint → latest checkpoint.
        assert_eq!(at(9), Some(serde_json::json!("c")));
    }

    #[tokio::test]
    async fn persists_only_durable_watermark_at_end() {
        // Source produced watermarks for batches 1,2,3 but only 1,2 are durable.
        let mut checkpoints = two_batches();
        checkpoints.push(ckpt(3, serde_json::json!(30), None));
        let sc = Scenario { checkpoints, committed: 2, delivery: None, ok: false, run: state_run(0, None, None) };
        let (persisted, doc) = supervise("durable-end", sc).await;
        assert_eq!(persisted, Some(serde_json::json!(20)));
        // And the file reflects the durable watermark, not the unconfirmed 30.
        let doc = doc.unwrap();
        assert_eq!(doc.cursor_value, 20);
        assert!(doc.resume.is_none(), "no sink, no token: nothing to resume beyond the watermark");
    }

    fn gated(gate: Option<bool>, prefixes: &[u64], interval: u64) -> Scenario {
        let prefixes = prefixes.iter().map(|&through| Delivered { through, objects: through }).collect();
        Scenario {
            checkpoints: two_batches(),
            committed: 2,
            delivery: Some((gate, prefixes)),
            ok: gate == Some(true),
            run: state_run(interval, None, None),
        }
    }

    #[tokio::test]
    async fn a_sink_failure_keeps_the_watermark() {
        // Committed by the destination, but the sink failed: nothing persisted,
        // mid-run (interval 1) or at the end.
        for (tag, interval) in [("gate-fail-end", 0), ("gate-fail-mid", 1)] {
            let (persisted, doc) = supervise(tag, gated(Some(false), &[], interval)).await;
            assert_eq!(persisted, None, "{tag}");
            assert!(doc.is_none(), "{tag}");
        }
        let (persisted, doc) = supervise("gate-dropped", gated(None, &[], 0)).await;
        assert_eq!(persisted, None);
        assert!(doc.is_none());
    }

    #[tokio::test]
    async fn a_full_delivery_persists_the_watermark() {
        let (persisted, doc) = supervise("gate-ok", gated(Some(true), &[], 1)).await;
        assert_eq!(persisted, Some(serde_json::json!(20)));
        assert_eq!(doc.unwrap().cursor_value, 20);
    }

    /// The sink delivered the file holding batch 1, then failed: the
    /// watermark advances through batch 1 only — mid-run or at the end.
    #[tokio::test]
    async fn a_sink_failure_keeps_the_delivered_prefix() {
        for (tag, interval) in [("prefix-end", 0), ("prefix-mid", 1)] {
            let (persisted, doc) = supervise(tag, gated(Some(false), &[1], interval)).await;
            assert_eq!(persisted, Some(serde_json::json!(10)), "{tag}");
            assert_eq!(doc.unwrap().cursor_value, 10, "{tag}");
        }
    }

    /// F3c: a failed run with a sink leaves a resume record naming the run,
    /// and the file after the delivered ones — mid-run or at the end.
    #[tokio::test]
    async fn a_failed_run_records_how_to_resume_it() {
        for (tag, interval) in [("record-end", 0), ("record-mid", 1)] {
            let mut sc = gated(Some(false), &[], interval);
            sc.delivery = Some((Some(false), vec![Delivered { through: 1, objects: 2 }]));
            sc.run.resume = Some(identity(Some(5)));
            let (_, doc) = supervise(tag, sc).await;
            let doc = doc.unwrap();
            assert_eq!(doc.version, 3, "{tag}");
            let record = doc.resume.expect(tag);
            assert_eq!((record.run_id.as_str(), record.started_at_unix_ms), ("r", 7), "{tag}");
            assert_eq!(record.next_object_seq, Some(7), "{tag}: files 5 and 6 delivered");
            assert_eq!(record.token, None, "{tag}");
        }
    }

    #[tokio::test]
    async fn a_successful_run_clears_the_record() {
        let mut sc = gated(Some(true), &[2], 1);
        sc.run.resume = Some(identity(None));
        let (_, doc) = supervise("record-cleared", sc).await;
        let doc = doc.unwrap();
        assert_eq!((doc.version, doc.resume.is_none(), doc.cursor_value.clone()), (2, true, serde_json::json!(20)));
    }

    /// A resumed run that reads nothing new and succeeds still ends the
    /// resumed run: the record goes, the watermark stays.
    #[tokio::test]
    async fn a_resumed_run_with_nothing_new_clears_the_record() {
        let record = ResumeRecord {
            run_id: "old".into(),
            started_at_unix_ms: 1,
            config_hash: "c".into(),
            next_object_seq: Some(4),
            token: None,
        };
        let prior = StateDoc::new("p1", serde_json::json!(99), "h", "old").with_resume(Some(record));
        let sc = Scenario {
            checkpoints: vec![],
            committed: 0,
            delivery: Some((Some(true), vec![])),
            ok: true,
            run: state_run(1, Some(identity(Some(4))), Some(prior)),
        };
        let (_, doc) = supervise("record-nothing-new", sc).await;
        let doc = doc.unwrap();
        assert_eq!((doc.resume, doc.cursor_value), (None, serde_json::json!(99)));
    }

    /// A partitioned full load's token is persisted at the durable frontier,
    /// with a null cursor.
    #[tokio::test]
    async fn a_resume_token_is_persisted_at_the_frontier() {
        let token = |done: &[u64]| Some(serde_json::json!({ "done": done }));
        let sc = Scenario {
            checkpoints: vec![
                ckpt(3, Value::Null, token(&[0])),
                ckpt(6, Value::Null, token(&[0, 2])),
                ckpt(9, Value::Null, token(&[0, 1, 2])),
            ],
            committed: 9,
            delivery: Some((Some(false), vec![Delivered { through: 7, objects: 3 }])),
            ok: false,
            run: state_run(1, Some(identity(None)), None),
        };
        let (_, doc) = supervise("token", sc).await;
        let doc = doc.unwrap();
        assert_eq!(doc.watermark(), None);
        let record = doc.resume.unwrap();
        assert_eq!(record.token, token(&[0, 2]), "batch 9 was not delivered");
        assert_eq!(record.next_object_seq, Some(4));
    }
}
