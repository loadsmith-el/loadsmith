//! Flow control on the source side: the core grants, the SDK enforces.
//!
//! The core sends a cumulative grant (`start.credit`, then `credit`): the
//! highest batch ordinal the source may have fetched. The SDK never asks the
//! plugin for a batch past it, and hands the plugin a [`FlowGate`] so read-ahead
//! inside the plugin (a fetch task, partition workers) waits on the same grant.

use tokio::sync::watch;

/// The core's grant, as seen by a source. Cheap to clone; every clone sees
/// the same grant.
#[derive(Debug, Clone)]
pub struct FlowGate {
    rx: watch::Receiver<u64>,
    /// A [`fixed`](Self::fixed) gate's own sender, kept so the channel stays
    /// open (a closed one would let every wait through).
    _fixed: Option<std::sync::Arc<watch::Sender<u64>>>,
}

impl FlowGate {
    /// A gate that never waits: the core sends no credits (an older core).
    pub fn open() -> Self {
        FlowGrant::new(None).1
    }

    /// A gate held at `granted` for good: no grant ever raises it. For
    /// testing a source's read-ahead against a fixed window.
    pub fn fixed(granted: u64) -> Self {
        let (tx, rx) = watch::channel(granted);
        Self { rx, _fixed: Some(std::sync::Arc::new(tx)) }
    }

    /// Waits until the core lets this source fetch batch `batch_seq`
    /// (1-based, the order the batches are written in). Call it before
    /// fetching each batch from the database, wherever that happens.
    pub async fn wait_for(&mut self, batch_seq: u64) {
        // A closed channel means the run is over: nothing to wait for.
        let _ = self.rx.wait_for(|granted| *granted >= batch_seq).await;
    }

    /// The highest batch ordinal granted so far (`u64::MAX` without flow
    /// control).
    pub fn granted(&self) -> u64 {
        *self.rx.borrow()
    }
}

/// The SDK's end of the gate: records the core's grants.
#[derive(Debug)]
pub(crate) struct FlowGrant {
    tx: watch::Sender<u64>,
}

impl FlowGrant {
    /// `initial` is `start.credit`; `None` opens the gate for good.
    pub(crate) fn new(initial: Option<u64>) -> (Self, FlowGate) {
        let (tx, rx) = watch::channel(initial.unwrap_or(u64::MAX));
        (Self { tx }, FlowGate { rx, _fixed: None })
    }

    /// Applies a `credit`. Grants only grow: an older, smaller one is ignored.
    pub(crate) fn grant(&self, batch_seq: u64) {
        self.tx.send_if_modified(|granted| {
            let grows = batch_seq > *granted;
            if grows {
                *granted = batch_seq;
            }
            grows
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn waits_until_the_grant_covers_the_batch() {
        let (grant, mut gate) = FlowGrant::new(Some(2));
        gate.wait_for(2).await;
        let mut waiter = gate.clone();
        let blocked = tokio::spawn(async move { waiter.wait_for(3).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!blocked.is_finished(), "batch 3 is past the grant");
        grant.grant(3);
        tokio::time::timeout(Duration::from_secs(1), blocked).await.unwrap().unwrap();
    }

    #[test]
    fn grants_only_grow() {
        let (grant, gate) = FlowGrant::new(Some(5));
        grant.grant(3);
        assert_eq!(gate.granted(), 5);
        grant.grant(8);
        assert_eq!(gate.granted(), 8);
    }

    #[tokio::test]
    async fn a_fixed_gate_never_opens_further() {
        let mut gate = FlowGate::fixed(1);
        gate.wait_for(1).await;
        let waited = tokio::time::timeout(Duration::from_millis(20), gate.wait_for(2)).await;
        assert!(waited.is_err(), "batch 2 is past the fixed grant");
    }

    #[tokio::test]
    async fn without_credits_the_gate_is_open() {
        let mut gate = FlowGate::open();
        gate.wait_for(u64::MAX).await;
        assert_eq!(gate.granted(), u64::MAX);
    }
}
