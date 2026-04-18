//! Cross-instance barrier coordination via chitchat KV.
//!
//! The elected leader writes a `BarrierAnnouncement` under its own
//! chitchat KV key. Each follower observes via gossip (~100 ms
//! propagation), triggers its local barrier injection, snapshots state,
//! then writes its own `BarrierAck`. The leader polls for acks and
//! decides quorum.
//!
//! See the parent design doc §7 (control plane) and §9 (barrier
//! protocol) plus `docs/plans/two-phase-ordering.md`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};

use crate::cluster::discovery::NodeId;

/// Chitchat KV key (on the leader's own state) announcing a barrier.
pub const ANNOUNCEMENT_KEY: &str = "control:barrier";

/// Chitchat KV key (on each follower's own state) carrying that
/// follower's ack. Readers iterate all nodes' states and pick up the
/// key from each.
pub const ACK_KEY: &str = "control:barrier-ack";

/// Barrier announcement written by the elected leader into its own
/// chitchat KV state. Followers observe via gossip.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarrierAnnouncement {
    /// Monotonic epoch identifier.
    pub epoch: u64,
    /// Checkpoint identifier assigned by the leader's coordinator.
    pub checkpoint_id: u64,
    /// Barrier flags (see `checkpoint::barrier::flags`).
    pub flags: u64,
}

/// Follower's acknowledgment that it has snapshotted state for the
/// announced epoch. Written into the follower's own chitchat KV state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarrierAck {
    /// The epoch being acknowledged.
    pub epoch: u64,
    /// Whether the follower's snapshot succeeded. Failed snapshots
    /// still ack (with `ok = false`) so the leader doesn't timeout
    /// waiting for a doomed peer; the leader then aborts the epoch.
    pub ok: bool,
    /// Optional error message when `ok = false`.
    pub error: Option<String>,
}

/// Outcome of a `wait_for_quorum` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuorumOutcome {
    /// Every expected instance acked successfully.
    Reached {
        /// Instances that acked with `ok = true`.
        acks: Vec<NodeId>,
    },
    /// Deadline passed before every expected instance acked.
    TimedOut {
        /// Instances that acked (in any state).
        got: Vec<NodeId>,
        /// Instances that did not ack.
        missing: Vec<NodeId>,
    },
    /// At least one follower reported snapshot failure.
    Failed {
        /// Instances that reported failure, with the error message.
        failures: Vec<(NodeId, String)>,
    },
}

/// The KV operations the coordinator needs from the gossip layer.
///
/// Two impls today: [`InMemoryKv`] for tests and
/// [`ChitchatKv`](super::chitchat_kv::ChitchatKv) for production.
/// Methods are `async` because chitchat's cluster state lives behind
/// a `tokio::sync::Mutex`; the in-memory impl simply returns
/// immediately.
#[async_trait]
pub trait ClusterKv: Send + Sync + 'static {
    /// Write `value` under `key` into this instance's own state.
    /// Overwrites any prior value for the same key.
    async fn write(&self, key: &str, value: String);

    /// Read `key` from the named instance's state, if present.
    async fn read_from(&self, who: NodeId, key: &str) -> Option<String>;

    /// Iterate every live instance's state and pull out the value for
    /// `key` where it exists. Used to collect acks.
    async fn scan(&self, key: &str) -> Vec<(NodeId, String)>;
}

/// In-memory KV for tests. Simulates gossip by making writes
/// immediately visible to all readers.
#[derive(Debug)]
pub struct InMemoryKv {
    local_id: NodeId,
    state: Mutex<FxHashMap<(NodeId, String), String>>,
}

impl InMemoryKv {
    /// Create a new in-memory KV identified as `local_id`.
    #[must_use]
    pub fn new(local_id: NodeId) -> Self {
        Self {
            local_id,
            state: Mutex::new(FxHashMap::default()),
        }
    }

    /// Seed a remote peer's state (for simulating multi-instance gossip
    /// in a single test).
    pub fn seed(&self, peer: NodeId, key: &str, value: String) {
        self.state.lock().insert((peer, key.to_string()), value);
    }
}

#[async_trait]
impl ClusterKv for InMemoryKv {
    async fn write(&self, key: &str, value: String) {
        self.state
            .lock()
            .insert((self.local_id, key.to_string()), value);
    }

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        self.state.lock().get(&(who, key.to_string())).cloned()
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        self.state
            .lock()
            .iter()
            .filter(|((_, k), _)| k == key)
            .map(|((n, _), v)| (*n, v.clone()))
            .collect()
    }
}

/// Coordinates cross-instance barriers on top of a [`ClusterKv`] seam.
pub struct BarrierCoordinator {
    kv: Arc<dyn ClusterKv>,
}

impl std::fmt::Debug for BarrierCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BarrierCoordinator").finish_non_exhaustive()
    }
}

impl BarrierCoordinator {
    /// Wrap a KV implementation.
    #[must_use]
    pub fn new(kv: Arc<dyn ClusterKv>) -> Self {
        Self { kv }
    }

    /// Leader-side: publish a barrier announcement. Followers observe
    /// via gossip once this is written.
    ///
    /// # Errors
    /// Returns a string error if JSON encoding fails (shouldn't happen
    /// for the fixed-shape announcement type).
    pub async fn announce(&self, ann: &BarrierAnnouncement) -> Result<(), String> {
        let json = serde_json::to_string(ann).map_err(|e| e.to_string())?;
        self.kv.write(ANNOUNCEMENT_KEY, json).await;
        Ok(())
    }

    /// Follower-side: read the leader's current announcement, if any.
    ///
    /// # Errors
    /// Returns a string error if the leader's stored announcement
    /// can't be decoded.
    pub async fn observe(&self, leader: NodeId) -> Result<Option<BarrierAnnouncement>, String> {
        match self.kv.read_from(leader, ANNOUNCEMENT_KEY).await {
            Some(json) => serde_json::from_str(&json).map(Some).map_err(|e| e.to_string()),
            None => Ok(None),
        }
    }

    /// Follower-side: write this instance's ack for an epoch.
    ///
    /// # Errors
    /// String error on JSON encode failure.
    pub async fn ack(&self, ack: &BarrierAck) -> Result<(), String> {
        let json = serde_json::to_string(ack).map_err(|e| e.to_string())?;
        self.kv.write(ACK_KEY, json).await;
        Ok(())
    }

    /// Leader-side: poll until every expected instance acks, or until
    /// `deadline` passes. Returns a [`QuorumOutcome`] describing the
    /// terminal state.
    ///
    /// Poll interval is 50 ms — tight enough to see gossip updates
    /// promptly but loose enough to not hammer the KV.
    pub async fn wait_for_quorum(
        &self,
        epoch: u64,
        expected: &[NodeId],
        deadline: Duration,
    ) -> QuorumOutcome {
        let start = Instant::now();
        let expected_set: FxHashSet<NodeId> = expected.iter().copied().collect();
        let mut successful: Vec<NodeId> = Vec::new();
        let mut failures: Vec<(NodeId, String)> = Vec::new();

        loop {
            successful.clear();
            failures.clear();

            for (from, json) in self.kv.scan(ACK_KEY).await {
                if !expected_set.contains(&from) {
                    continue;
                }
                let Ok(ack) = serde_json::from_str::<BarrierAck>(&json) else {
                    continue;
                };
                if ack.epoch != epoch {
                    continue;
                }
                if ack.ok {
                    successful.push(from);
                } else {
                    failures.push((from, ack.error.unwrap_or_default()));
                }
            }

            if !failures.is_empty() {
                return QuorumOutcome::Failed { failures };
            }
            if successful.len() == expected.len() {
                return QuorumOutcome::Reached { acks: successful };
            }
            if start.elapsed() >= deadline {
                let got: FxHashSet<NodeId> = successful.iter().copied().collect();
                let missing: Vec<NodeId> = expected
                    .iter()
                    .copied()
                    .filter(|n| !got.contains(n))
                    .collect();
                return QuorumOutcome::TimedOut {
                    got: successful,
                    missing,
                };
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv(id: NodeId) -> Arc<InMemoryKv> {
        Arc::new(InMemoryKv::new(id))
    }

    #[tokio::test]
    async fn leader_announces_follower_observes() {
        let leader_kv = kv(NodeId(1));
        let coord = BarrierCoordinator::new(leader_kv.clone());
        coord
            .announce(&BarrierAnnouncement {
                epoch: 5,
                checkpoint_id: 42,
                flags: 0,
            })
            .await
            .unwrap();
        let got = coord.observe(NodeId(1)).await.unwrap().unwrap();
        assert_eq!(got.epoch, 5);
        assert_eq!(got.checkpoint_id, 42);
    }

    #[tokio::test]
    async fn observe_returns_none_when_leader_silent() {
        let k = kv(NodeId(1));
        let coord = BarrierCoordinator::new(k);
        assert!(coord.observe(NodeId(1)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn quorum_reached_when_all_ack_success() {
        let k = kv(NodeId(1));
        // Simulate followers' acks being gossiped in.
        let ack_json = serde_json::to_string(&BarrierAck {
            epoch: 7,
            ok: true,
            error: None,
        })
        .unwrap();
        k.seed(NodeId(2), ACK_KEY, ack_json.clone());
        k.seed(NodeId(3), ACK_KEY, ack_json);

        let coord = BarrierCoordinator::new(k);
        let outcome = coord
            .wait_for_quorum(7, &[NodeId(2), NodeId(3)], Duration::from_millis(200))
            .await;
        match outcome {
            QuorumOutcome::Reached { mut acks } => {
                acks.sort_by_key(|n| n.0);
                assert_eq!(acks, vec![NodeId(2), NodeId(3)]);
            }
            other => panic!("expected Reached, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn quorum_timeout_when_follower_silent() {
        let k = kv(NodeId(1));
        let ack_json = serde_json::to_string(&BarrierAck {
            epoch: 8,
            ok: true,
            error: None,
        })
        .unwrap();
        k.seed(NodeId(2), ACK_KEY, ack_json);
        // NodeId(3) never acks.

        let coord = BarrierCoordinator::new(k);
        let outcome = coord
            .wait_for_quorum(8, &[NodeId(2), NodeId(3)], Duration::from_millis(150))
            .await;
        match outcome {
            QuorumOutcome::TimedOut { got, missing } => {
                assert_eq!(got, vec![NodeId(2)]);
                assert_eq!(missing, vec![NodeId(3)]);
            }
            other => panic!("expected TimedOut, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn quorum_fails_fast_on_reported_error() {
        let k = kv(NodeId(1));
        let good = serde_json::to_string(&BarrierAck {
            epoch: 9,
            ok: true,
            error: None,
        })
        .unwrap();
        let bad = serde_json::to_string(&BarrierAck {
            epoch: 9,
            ok: false,
            error: Some("state snapshot failed: disk full".into()),
        })
        .unwrap();
        k.seed(NodeId(2), ACK_KEY, good);
        k.seed(NodeId(3), ACK_KEY, bad);

        let coord = BarrierCoordinator::new(k);
        let outcome = coord
            .wait_for_quorum(9, &[NodeId(2), NodeId(3)], Duration::from_secs(2))
            .await;
        match outcome {
            QuorumOutcome::Failed { failures } => {
                assert_eq!(failures.len(), 1);
                assert_eq!(failures[0].0, NodeId(3));
                assert!(failures[0].1.contains("disk full"));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn wrong_epoch_ack_is_ignored() {
        let k = kv(NodeId(1));
        // NodeId(2) acks a DIFFERENT epoch — stale ack, should not
        // count.
        let stale = serde_json::to_string(&BarrierAck {
            epoch: 9,
            ok: true,
            error: None,
        })
        .unwrap();
        k.seed(NodeId(2), ACK_KEY, stale);

        let coord = BarrierCoordinator::new(k);
        let outcome = coord
            .wait_for_quorum(10, &[NodeId(2)], Duration::from_millis(100))
            .await;
        assert!(
            matches!(outcome, QuorumOutcome::TimedOut { .. }),
            "stale-epoch ack must not satisfy quorum"
        );
    }
}
