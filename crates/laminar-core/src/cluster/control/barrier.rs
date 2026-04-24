//! Cross-instance barrier protocol over a gossip KV. Leader announces,
//! followers ack, leader polls for quorum.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};

use crate::cluster::discovery::NodeId;

/// KV key for the leader's barrier announcement.
pub const ANNOUNCEMENT_KEY: &str = "control:barrier";

/// KV key for a follower's barrier ack.
pub const ACK_KEY: &str = "control:barrier-ack";

/// Barrier phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    /// Snapshot state and pre-commit sinks.
    Prepare,
    /// Durability gate passed; commit sinks.
    Commit,
    /// Prepare failed; roll back.
    Abort,
}

/// Leader-written barrier announcement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarrierAnnouncement {
    /// Monotonic epoch id.
    pub epoch: u64,
    /// Coordinator-assigned checkpoint id.
    pub checkpoint_id: u64,
    /// Phase this announcement signals.
    pub phase: Phase,
    /// Reserved for unaligned/other flags.
    pub flags: u64,
    /// Cluster-wide minimum watermark at announce time: the `min`
    /// across every live node's local watermark, computed by the
    /// leader from follower acks (see `BarrierAck.local_watermark_ms`)
    /// plus the leader's own watermark. Populated on
    /// [`Phase::Commit`] announcements. `None` on `Prepare`/`Abort`
    /// (computed only after acks are in) and on legacy payloads
    /// deserialised via the `#[serde(default)]` fallback.
    ///
    /// Consumers consult this value instead of their local watermark
    /// when deciding whether an event-time window has closed
    /// cluster-wide — local progress on one node is stale if another
    /// node is still processing earlier events.
    #[serde(default)]
    pub min_watermark_ms: Option<i64>,
}

/// Follower ack. `ok = false` forces the leader to abort instead of wait.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarrierAck {
    /// Epoch being acknowledged.
    pub epoch: u64,
    /// `false` = snapshot failed locally; leader should abort.
    pub ok: bool,
    /// Free-text error; populated when `ok = false`.
    pub error: Option<String>,
    /// Follower's local watermark at ack time (ms since epoch or
    /// arbitrary monotonic domain, matching the source's event-time
    /// units). The leader folds this into the cluster-wide min
    /// emitted in the matching `Commit` announcement.
    ///
    /// `None` means the follower's watermark is unset (fresh boot,
    /// no source events yet) — treated as "infinity" by the leader:
    /// it doesn't cap the cluster min downward.
    #[serde(default)]
    pub local_watermark_ms: Option<i64>,
}

/// Outcome of `wait_for_quorum`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuorumOutcome {
    /// All expected peers acked with `ok = true`.
    Reached {
        /// Peers that acked successfully.
        acks: Vec<NodeId>,
        /// The minimum watermark across every successful ack's
        /// `local_watermark_ms` (ignoring `None` values). `None`
        /// means no follower reported a watermark — the leader
        /// falls back to its own local value for the Commit
        /// announcement.
        min_follower_watermark_ms: Option<i64>,
    },
    /// Deadline expired with at least one peer silent.
    TimedOut {
        /// Peers that did ack.
        got: Vec<NodeId>,
        /// Peers that didn't.
        missing: Vec<NodeId>,
    },
    /// At least one peer acked `ok = false`.
    Failed {
        /// `(peer, error_message)` for every failed ack.
        failures: Vec<(NodeId, String)>,
    },
}

/// Gossip-KV seam.
#[async_trait]
pub trait ClusterKv: Send + Sync + 'static {
    /// Write `value` to this instance's `key` slot (overwrites).
    async fn write(&self, key: &str, value: String);
    /// Read `key` from `who`'s slot.
    async fn read_from(&self, who: NodeId, key: &str) -> Option<String>;
    /// Every visible instance's value for `key`.
    async fn scan(&self, key: &str) -> Vec<(NodeId, String)>;
}

/// In-memory KV for tests.
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

    /// Seed a remote peer's state for tests.
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

/// Cross-instance barrier coordination over a [`ClusterKv`].
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

    /// Leader-side announce.
    ///
    /// # Errors
    /// Returns a string on JSON encode failure.
    pub async fn announce(&self, ann: &BarrierAnnouncement) -> Result<(), String> {
        let json = serde_json::to_string(ann).map_err(|e| e.to_string())?;
        self.kv.write(ANNOUNCEMENT_KEY, json).await;
        Ok(())
    }

    /// Follower-side observe.
    ///
    /// # Errors
    /// Returns a string on JSON decode failure.
    pub async fn observe(&self, leader: NodeId) -> Result<Option<BarrierAnnouncement>, String> {
        match self.kv.read_from(leader, ANNOUNCEMENT_KEY).await {
            Some(json) => serde_json::from_str(&json)
                .map(Some)
                .map_err(|e| e.to_string()),
            None => Ok(None),
        }
    }

    /// Follower-side ack.
    ///
    /// # Errors
    /// Returns a string on JSON encode failure.
    pub async fn ack(&self, ack: &BarrierAck) -> Result<(), String> {
        let json = serde_json::to_string(ack).map_err(|e| e.to_string())?;
        self.kv.write(ACK_KEY, json).await;
        Ok(())
    }

    /// Leader-side: poll acks every 50 ms until quorum or `deadline`.
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
        let mut min_follower_wm: Option<i64>;

        loop {
            successful.clear();
            failures.clear();
            min_follower_wm = None;

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
                    if let Some(wm) = ack.local_watermark_ms {
                        min_follower_wm = Some(match min_follower_wm {
                            Some(cur) => cur.min(wm),
                            None => wm,
                        });
                    }
                } else {
                    failures.push((from, ack.error.unwrap_or_default()));
                }
            }

            if !failures.is_empty() {
                return QuorumOutcome::Failed { failures };
            }
            if successful.len() == expected.len() {
                return QuorumOutcome::Reached {
                    acks: successful,
                    min_follower_watermark_ms: min_follower_wm,
                };
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
                phase: Phase::Prepare,
                flags: 0,
                min_watermark_ms: None,
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
            local_watermark_ms: None,
        })
        .unwrap();
        k.seed(NodeId(2), ACK_KEY, ack_json.clone());
        k.seed(NodeId(3), ACK_KEY, ack_json);

        let coord = BarrierCoordinator::new(k);
        let outcome = coord
            .wait_for_quorum(7, &[NodeId(2), NodeId(3)], Duration::from_millis(200))
            .await;
        match outcome {
            QuorumOutcome::Reached {
                mut acks,
                min_follower_watermark_ms,
            } => {
                acks.sort_by_key(|n| n.0);
                assert_eq!(acks, vec![NodeId(2), NodeId(3)]);
                assert_eq!(
                    min_follower_watermark_ms, None,
                    "no follower reported a watermark — min is None"
                );
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
            local_watermark_ms: None,
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
            local_watermark_ms: None,
        })
        .unwrap();
        let bad = serde_json::to_string(&BarrierAck {
            epoch: 9,
            ok: false,
            error: Some("state snapshot failed: disk full".into()),
            local_watermark_ms: None,
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
            local_watermark_ms: None,
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
