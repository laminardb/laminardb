//! Facade over `ClusterKv` + `BarrierCoordinator` + membership watch.
//! `None` on `CheckpointCoordinator` means single-instance mode.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use super::barrier::{
    BarrierAck, BarrierAnnouncement, BarrierCoordinator, ClusterKv, Phase, QuorumOutcome,
};
use super::leader::leader_of;
use super::snapshot::AssignmentSnapshotStore;
use crate::cluster::discovery::{assignable_node_ids, NodeId, NodeInfo, NodeState};
use crate::state::Locality;

/// Facade composing the cluster-control primitives.
pub struct ClusterController {
    instance_id: NodeId,
    kv: Arc<dyn ClusterKv>,
    barrier: BarrierCoordinator,
    snapshot: Option<Arc<AssignmentSnapshotStore>>,
    members_rx: watch::Receiver<Vec<NodeInfo>>,
    /// Latest cluster-wide minimum watermark published by the leader
    /// in a `Commit` announcement. `i64::MIN` means uninitialised
    /// (no Commit observed yet). Operators consult this instead of
    /// their local watermark so event-time decisions stay consistent
    /// across the cluster.
    cluster_min_watermark: Arc<AtomicI64>,
    /// Set once this node begins graceful drain. While set, the node
    /// excludes itself from [`Self::assignable_instances`] so the next
    /// rotation sheds its vnodes elsewhere before it exits.
    draining: Arc<AtomicBool>,
    /// Whether this node has announced itself as Active.
    active: Arc<AtomicBool>,
    /// This node's own failure-domain locality (peers carry theirs in
    /// `members_rx`; self is only known by id). Set once at startup.
    self_locality: parking_lot::RwLock<Locality>,
    /// Handler serving cross-node `RemoteScan`, shared with the query server.
    #[cfg(feature = "cluster")]
    query_handler: super::query::QueryHandlerSlot,
    /// Pooled channels to peers for cross-node `RemoteScan`.
    #[cfg(feature = "cluster")]
    query_client_pool: super::query::QueryClientPool,
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
        let mut barrier = BarrierCoordinator::new(Arc::clone(&kv));
        #[cfg(feature = "cluster")]
        barrier.set_leader_election(instance_id, members_rx.clone());
        Self {
            instance_id,
            barrier,
            kv,
            snapshot,
            members_rx,
            cluster_min_watermark: Arc::new(AtomicI64::new(i64::MIN)),
            draining: Arc::new(AtomicBool::new(false)),
            active: Arc::new(AtomicBool::new(true)),
            self_locality: parking_lot::RwLock::new(Locality::default()),
            #[cfg(feature = "cluster")]
            query_handler: Arc::new(parking_lot::RwLock::new(None)),
            #[cfg(feature = "cluster")]
            query_client_pool: Arc::new(parking_lot::Mutex::new(rustc_hash::FxHashMap::default())),
        }
    }

    /// Register the handler serving cross-node `RemoteScan`.
    #[cfg(feature = "cluster")]
    pub fn register_query_handler(&self, handler: Arc<dyn super::query::RemoteQueryHandler>) {
        *self.query_handler.write() = Some(handler);
    }

    /// Access the connection pool for remote queries.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn query_client_pool(&self) -> &super::query::QueryClientPool {
        &self.query_client_pool
    }

    /// Latest cluster-wide minimum watermark seen by this instance.
    /// `None` until the leader has published a `Commit` with a
    /// populated `min_watermark_ms`.
    #[must_use]
    pub fn cluster_min_watermark(&self) -> Option<i64> {
        let v = self.cluster_min_watermark.load(Ordering::Acquire);
        if v == i64::MIN {
            None
        } else {
            Some(v)
        }
    }

    /// Leader-side monotonic publish. The leader computes the
    /// cluster-wide minimum watermark in `await_prepare_quorum`
    /// (its own local watermark folded with every follower's ack)
    /// and must mirror it into the controller atomic so its own
    /// operators see the same value that followers pick up via
    /// `observe_barrier` on the matching `Commit`. Never lowers the
    /// published value — event-time progress is monotonic.
    pub fn publish_cluster_min_watermark(&self, wm: i64) {
        let mut cur = self.cluster_min_watermark.load(Ordering::Acquire);
        while wm > cur {
            match self.cluster_min_watermark.compare_exchange(
                cur,
                wm,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// This instance's ID.
    #[must_use]
    pub fn instance_id(&self) -> NodeId {
        self.instance_id
    }

    /// The cluster gossip KV, exposed so higher layers can advertise/discover
    /// per-stream state alongside the control-plane keys.
    #[must_use]
    pub fn kv(&self) -> &Arc<dyn ClusterKv> {
        &self.kv
    }

    /// Current leader (lowest id among `Active` peers plus self).
    #[must_use]
    pub fn current_leader(&self) -> Option<NodeId> {
        let members = self.members_rx.borrow();
        let mut ids: Vec<NodeId> = members
            .iter()
            .filter(|m| matches!(m.state, NodeState::Active))
            .map(|m| m.id)
            .collect();
        // Include ourselves if we are active.
        if self.active.load(Ordering::SeqCst) {
            ids.push(self.instance_id);
        }
        leader_of(&ids)
    }

    /// True if this instance is currently the leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.current_leader() == Some(self.instance_id)
    }

    /// Mark this node's active status.
    pub fn set_active(&self, active: bool) {
        self.active.store(active, Ordering::SeqCst);
    }

    /// Live instance IDs: `Active` peers plus self.
    #[must_use]
    pub fn live_instances(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self
            .members_rx
            .borrow()
            .iter()
            .filter(|m| matches!(m.state, NodeState::Active))
            .map(|m| m.id)
            .collect();
        if self.active.load(Ordering::SeqCst) {
            ids.push(self.instance_id);
        }
        ids
    }

    /// Mark this node as draining. Idempotent.
    pub fn begin_drain(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    /// Whether this node is draining.
    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    /// Node ids eligible to own vnodes: `Active` peers, plus self unless
    /// this node is draining. Mirrors how [`Self::live_instances`] folds
    /// self in, but filters non-`Active` peers (see [`assignable_node_ids`])
    /// so Joining/Suspected/Draining/Left nodes never receive vnodes.
    #[must_use]
    pub fn assignable_instances(&self) -> Vec<NodeId> {
        let mut ids = assignable_node_ids(&self.members_rx.borrow());
        if self.active.load(Ordering::SeqCst)
            && !self.is_draining()
            && !self.instance_id.is_unassigned()
        {
            ids.push(self.instance_id);
        }
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Record this node's own locality. Call once at startup.
    pub fn set_self_locality(&self, locality: Locality) {
        *self.self_locality.write() = locality;
    }

    /// [`Self::assignable_instances`] paired with each node's [`Locality`]
    /// (peers' from `members_rx`, self's from [`Self::set_self_locality`]).
    #[must_use]
    pub fn assignable_with_locality(&self) -> Vec<(NodeId, Locality)> {
        let members = self.members_rx.borrow();
        self.assignable_instances()
            .into_iter()
            .map(|id| {
                let locality = if id == self.instance_id {
                    self.self_locality.read().clone()
                } else {
                    members
                        .iter()
                        .find(|m| m.id == id)
                        .and_then(|m| m.metadata.failure_domain.as_deref())
                        .map(Locality::parse)
                        .unwrap_or_default()
                };
                (id, locality)
            })
            .collect()
    }

    /// Cloneable membership watch. Background tasks subscribe to
    /// this to react to join/leave events (`changed().await`) without
    /// polling [`Self::live_instances`] on a timer.
    #[must_use]
    pub fn members_watch(&self) -> watch::Receiver<Vec<NodeInfo>> {
        self.members_rx.clone()
    }

    /// Write the current assignment snapshot version to gossip KV.
    pub async fn announce_snapshot_version(&self, version: u64) {
        self.kv
            .write("control:snapshot-version", version.to_string())
            .await;
    }

    /// Read the snapshot version from all peers in gossip KV and return the maximum version.
    pub async fn read_snapshot_version(&self) -> Option<u64> {
        let scans = self.kv.scan("control:snapshot-version").await;
        scans
            .into_iter()
            .filter_map(|(_, v)| v.parse::<u64>().ok())
            .max()
    }

    /// Start the direct gRPC barrier sync server.
    ///
    /// # Errors
    /// Propagates [`BarrierCoordinator::start_server`] errors.
    #[cfg(feature = "cluster")]
    pub async fn start_barrier_server(
        &self,
        bind_addr: std::net::SocketAddr,
        advertise_host: Option<String>,
    ) -> Result<std::net::SocketAddr, String> {
        self.barrier
            .start_server(bind_addr, advertise_host, Arc::clone(&self.query_handler))
            .await
    }

    /// Leader-side announce.
    ///
    /// # Errors
    /// Propagates [`BarrierCoordinator::announce`] errors.
    pub async fn announce_barrier(&self, ann: &BarrierAnnouncement) -> Result<(), String> {
        self.barrier.announce(ann).await
    }

    /// Follower-side observe; `Ok(None)` if no leader is visible.
    ///
    /// As a side effect, an `Aligned` or `Commit` announcement with a
    /// populated `min_watermark_ms` updates the shared
    /// cluster-min-watermark atomic so operators on this instance see
    /// the cluster-wide minimum without a separate polling path
    /// (`Aligned` carries it so a resuming pipeline sees fresh
    /// event-time progress before the upload-gated `Commit`).
    ///
    /// # Errors
    /// Propagates [`BarrierCoordinator::observe`] errors.
    pub async fn observe_barrier(&self) -> Result<Option<BarrierAnnouncement>, String> {
        let Some(leader) = self.current_leader() else {
            return Ok(None);
        };
        let observed = self.barrier.observe(leader).await?;
        if let Some(ref ann) = observed {
            if matches!(ann.phase, Phase::Commit | Phase::Aligned) {
                if let Some(wm) = ann.min_watermark_ms {
                    // Monotonic publish — never lower the watermark,
                    // even if a stale announcement re-gossips.
                    let mut cur = self.cluster_min_watermark.load(Ordering::Acquire);
                    while wm > cur {
                        match self.cluster_min_watermark.compare_exchange(
                            cur,
                            wm,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => break,
                            Err(observed) => cur = observed,
                        }
                    }
                }
            }
        }
        Ok(observed)
    }

    /// Follower-side ack.
    ///
    /// # Errors
    /// Propagates [`BarrierCoordinator::ack`] errors.
    pub async fn ack_barrier(&self, ack: &BarrierAck) -> Result<(), String> {
        self.barrier.ack(ack).await
    }

    /// Leader-side: poll until quorum or `deadline`.
    pub async fn wait_for_quorum(
        &self,
        epoch: u64,
        expected: &[NodeId],
        deadline: Duration,
    ) -> QuorumOutcome {
        self.barrier
            .wait_for_quorum(epoch, expected, deadline)
            .await
    }

    /// Assignment snapshot store, if configured.
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

    #[test]
    fn assignable_instances_excludes_draining_peer_and_self_on_drain() {
        let mut draining_peer = info(5);
        draining_peer.state = NodeState::Draining;
        let c = ctl(1, vec![info(3), draining_peer]);

        // Active peers + self; the Draining peer is shed.
        assert_eq!(c.assignable_instances(), vec![NodeId(1), NodeId(3)]);
        assert!(!c.is_draining());

        // After begin_drain, self drops out too.
        c.begin_drain();
        assert!(c.is_draining());
        assert_eq!(c.assignable_instances(), vec![NodeId(3)]);
    }

    #[test]
    fn assignable_with_locality_attaches_self_and_peer_domains() {
        let mut peer = info(3);
        peer.metadata.failure_domain = Some("region=r;zone=z2".to_string());
        let c = ctl(1, vec![peer]);
        c.set_self_locality(Locality::parse("region=r;zone=z1"));

        let pairs = c.assignable_with_locality();
        // Same set as assignable_instances (self + active peer), sorted by id.
        let ids: Vec<NodeId> = pairs.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![NodeId(1), NodeId(3)]);
        // Self's locality comes from set_self_locality; peer's from gossip.
        let self_loc = &pairs.iter().find(|(id, _)| *id == NodeId(1)).unwrap().1;
        let peer_loc = &pairs.iter().find(|(id, _)| *id == NodeId(3)).unwrap().1;
        assert_eq!(self_loc.domain_at(1), "r;z1");
        assert_eq!(peer_loc.domain_at(1), "r;z2");
    }

    #[test]
    fn assignable_with_locality_defaults_unlabeled_to_empty_domain() {
        // A peer with no failure_domain and unset self locality both collapse
        // to the empty "unknown" domain — safe degradation, never a panic.
        let c = ctl(1, vec![info(3)]);
        let pairs = c.assignable_with_locality();
        assert_eq!(pairs.len(), 2);
        assert!(pairs.iter().all(|(_, loc)| loc.domain_at(0).is_empty()));
    }

    #[tokio::test]
    async fn announce_observe_roundtrip_when_alone() {
        // Single-instance: self == leader; own announcement is visible
        // to own observe.
        let c = ctl(1, vec![]);
        c.announce_barrier(&BarrierAnnouncement {
            epoch: 5,
            checkpoint_id: 1,
            phase: crate::cluster::control::Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
            seq: 0,
        })
        .await
        .unwrap();
        let got = c.observe_barrier().await.unwrap().unwrap();
        assert_eq!(got.epoch, 5);
    }

    #[test]
    fn publish_cluster_min_watermark_is_monotonic() {
        // Leader-side publish mirrors the monotonic contract the
        // follower path already enforces via observe_barrier.
        let c = ctl(1, vec![]);
        assert_eq!(c.cluster_min_watermark(), None);

        c.publish_cluster_min_watermark(100);
        assert_eq!(c.cluster_min_watermark(), Some(100));

        // Higher value advances.
        c.publish_cluster_min_watermark(250);
        assert_eq!(c.cluster_min_watermark(), Some(250));

        // Lower value must not regress.
        c.publish_cluster_min_watermark(42);
        assert_eq!(c.cluster_min_watermark(), Some(250));

        // Equal value is a no-op; still Some(250).
        c.publish_cluster_min_watermark(250);
        assert_eq!(c.cluster_min_watermark(), Some(250));
    }

    #[tokio::test]
    async fn observe_commit_publishes_cluster_min_watermark() {
        // Commit announcements with `min_watermark_ms` populated
        // propagate into the shared atomic so operators can read
        // cluster-wide progress without a separate channel.
        let c = ctl(1, vec![]);
        assert_eq!(c.cluster_min_watermark(), None, "uninitialised");

        c.announce_barrier(&BarrierAnnouncement {
            epoch: 9,
            checkpoint_id: 1,
            phase: crate::cluster::control::Phase::Commit,
            flags: 0,
            min_watermark_ms: Some(12_345),
            seq: 0,
        })
        .await
        .unwrap();
        c.observe_barrier().await.unwrap();
        assert_eq!(c.cluster_min_watermark(), Some(12_345));

        // A later Commit with a lower value must NOT regress the atomic —
        // event-time can only advance.
        c.announce_barrier(&BarrierAnnouncement {
            epoch: 10,
            checkpoint_id: 2,
            phase: crate::cluster::control::Phase::Commit,
            flags: 0,
            min_watermark_ms: Some(100), // stale re-gossip
            seq: 0,
        })
        .await
        .unwrap();
        c.observe_barrier().await.unwrap();
        assert_eq!(
            c.cluster_min_watermark(),
            Some(12_345),
            "stale Commit must not lower the published watermark",
        );

        // A Prepare announcement (no min_watermark_ms carried) is a no-op.
        c.announce_barrier(&BarrierAnnouncement {
            epoch: 11,
            checkpoint_id: 3,
            phase: crate::cluster::control::Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
            seq: 0,
        })
        .await
        .unwrap();
        c.observe_barrier().await.unwrap();
        assert_eq!(c.cluster_min_watermark(), Some(12_345));
    }
}
