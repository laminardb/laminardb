//! `ClusterController` — the facade the checkpoint coordinator and
//! shuffle layer use to talk to the cluster control plane.
//!
//! Composes the primitives from this module: a [`ClusterKv`] for
//! gossip-based coordination, a [`BarrierCoordinator`] for checkpoint
//! barrier announce/ack, an optional [`AssignmentSnapshotStore`] for
//! durable assignment history, and a `watch::Receiver<Vec<NodeInfo>>`
//! from discovery to derive leader / live-instance queries.
//!
//! Wired into `LaminarDB` via `LaminarDbBuilder::cluster_controller`.
//! Held as an optional field on `CheckpointCoordinator`; when present,
//! cluster mode is active. When `None`, single-instance semantics.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use super::barrier::{
    BarrierAck, BarrierAnnouncement, BarrierCoordinator, ClusterKv, QuorumOutcome,
};
use super::leader::leader_of;
use super::snapshot::AssignmentSnapshotStore;
use crate::cluster::discovery::{NodeId, NodeInfo, NodeState};

/// Thin facade composing the cluster-control primitives.
///
/// Construction is "wire up what's already running" — this type
/// doesn't start any background tasks. It holds references; the
/// discovery layer continues to drive the membership watch.
pub struct ClusterController {
    instance_id: NodeId,
    barrier: BarrierCoordinator,
    snapshot: Option<Arc<AssignmentSnapshotStore>>,
    members_rx: watch::Receiver<Vec<NodeInfo>>,
}

impl std::fmt::Debug for ClusterController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterController")
            .field("instance_id", &self.instance_id)
            .finish_non_exhaustive()
    }
}

impl ClusterController {
    /// Wrap the given primitives.
    #[must_use]
    pub fn new(
        instance_id: NodeId,
        kv: Arc<dyn ClusterKv>,
        snapshot: Option<Arc<AssignmentSnapshotStore>>,
        members_rx: watch::Receiver<Vec<NodeInfo>>,
    ) -> Self {
        Self {
            instance_id,
            barrier: BarrierCoordinator::new(kv),
            snapshot,
            members_rx,
        }
    }

    /// This instance's ID.
    #[must_use]
    pub fn instance_id(&self) -> NodeId {
        self.instance_id
    }

    /// Derive the current leader from the membership watch. Returns
    /// `None` if no live instances are visible yet.
    ///
    /// Only `Active` peers count — suspected / draining / left nodes
    /// are filtered out so a crashed leader isn't chosen again before
    /// phi-accrual clears its entry.
    #[must_use]
    pub fn current_leader(&self) -> Option<NodeId> {
        let members = self.members_rx.borrow();
        let mut ids: Vec<NodeId> = members
            .iter()
            .filter(|m| matches!(m.state, NodeState::Active))
            .map(|m| m.id)
            .collect();
        // Include ourselves — we're trivially Active from our own view.
        ids.push(self.instance_id);
        leader_of(&ids)
    }

    /// True if this instance is currently the leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.current_leader() == Some(self.instance_id)
    }

    /// Snapshot of currently-live instance IDs (`Active`-state peers
    /// plus self).
    #[must_use]
    pub fn live_instances(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self
            .members_rx
            .borrow()
            .iter()
            .filter(|m| matches!(m.state, NodeState::Active))
            .map(|m| m.id)
            .collect();
        ids.push(self.instance_id);
        ids
    }

    /// Leader-side: announce a barrier.
    ///
    /// # Errors
    /// Propagates [`BarrierCoordinator::announce`] errors.
    pub async fn announce_barrier(&self, ann: &BarrierAnnouncement) -> Result<(), String> {
        self.barrier.announce(ann).await
    }

    /// Follower-side: read the current leader's barrier announcement.
    ///
    /// # Errors
    /// Propagates [`BarrierCoordinator::observe`] errors. Returns
    /// `Ok(None)` if no leader is currently visible.
    pub async fn observe_barrier(&self) -> Result<Option<BarrierAnnouncement>, String> {
        let Some(leader) = self.current_leader() else {
            return Ok(None);
        };
        self.barrier.observe(leader).await
    }

    /// Follower-side: publish this instance's ack.
    ///
    /// # Errors
    /// Propagates [`BarrierCoordinator::ack`] errors.
    pub async fn ack_barrier(&self, ack: &BarrierAck) -> Result<(), String> {
        self.barrier.ack(ack).await
    }

    /// Leader-side: poll until every expected instance acks, or until
    /// `deadline` passes.
    pub async fn wait_for_quorum(
        &self,
        epoch: u64,
        expected: &[NodeId],
        deadline: Duration,
    ) -> QuorumOutcome {
        self.barrier.wait_for_quorum(epoch, expected, deadline).await
    }

    /// Access the assignment snapshot store, if configured.
    #[must_use]
    pub fn snapshot_store(&self) -> Option<&AssignmentSnapshotStore> {
        self.snapshot.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::control::barrier::InMemoryKv;
    use crate::cluster::discovery::{NodeMetadata, NodeState};

    fn info(id: u64) -> NodeInfo {
        NodeInfo {
            id: NodeId(id),
            name: format!("n{id}"),
            rpc_address: String::new(),
            raft_address: String::new(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        }
    }

    fn ctl(self_id: u64, peers: Vec<NodeInfo>) -> ClusterController {
        let (_tx, rx) = watch::channel(peers);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(NodeId(self_id)));
        ClusterController::new(NodeId(self_id), kv, None, rx)
    }

    #[test]
    fn is_leader_when_lowest_id() {
        let c = ctl(1, vec![info(5), info(7)]);
        assert!(c.is_leader());
    }

    #[test]
    fn follower_when_peer_has_lower_id() {
        let c = ctl(7, vec![info(3), info(5)]);
        assert!(!c.is_leader());
        assert_eq!(c.current_leader(), Some(NodeId(3)));
    }

    #[test]
    fn solo_instance_is_leader() {
        let c = ctl(42, vec![]);
        assert!(c.is_leader());
    }

    #[tokio::test]
    async fn announce_observe_roundtrip_when_alone() {
        // Single-instance: self == leader; own announcement is visible
        // to own observe.
        let c = ctl(1, vec![]);
        c.announce_barrier(&BarrierAnnouncement {
            epoch: 5,
            checkpoint_id: 1,
            flags: 0,
        })
        .await
        .unwrap();
        let got = c.observe_barrier().await.unwrap().unwrap();
        assert_eq!(got.epoch, 5);
    }
}
