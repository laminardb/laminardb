//! Facade over `ClusterKv` + `BarrierCoordinator` + membership watch.
//! `None` on `CheckpointCoordinator` means single-instance mode.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use uuid::Uuid;

use super::barrier::{
    BarrierAck, BarrierAnnouncement, BarrierCoordinator, ClusterKv, Phase, QuorumOutcome,
};
use super::leader::leader_of;
use super::snapshot::AssignmentSnapshotStore;
use crate::checkpoint::{
    AssignmentDrainId, AssignmentDrainTransition, CheckpointAssignmentAdoption,
    CheckpointAssignmentFence, CheckpointParticipant, NodeDrainReceiptAggregate,
};
use crate::cluster::discovery::{assignable_node_ids, NodeId, NodeInfo, NodeState};
use crate::state::{CheckpointAttempt, Locality};

const RECOVERY_INCARNATION_KEY: &str = "control:recovery-incarnation";
const DRAIN_ACK_KEY: &str = "control:drain-ack";
const RECOVERY_CONTROL_IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_DRAIN_ACK_BYTES: usize = 1_024;

#[cfg(feature = "cluster")]
struct LeaderLeaseGate {
    lease: watch::Receiver<Option<super::LeaderLease>>,
    owner: super::LeaderLeaseOwner,
    deadline: Arc<super::LeaseDeadline>,
}

/// Immutable identity of one coordinated recovery attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecoveryRoundId {
    /// Monotonic recovery generation.
    pub generation: u64,
    /// Random attempt identity. This prevents a restarted driver from reusing a generation.
    pub nonce: Uuid,
    /// Node that owns and drives this round.
    pub driver: NodeId,
}

/// Frozen recovery quorum and its assignment certificate.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecoveryRound {
    /// Unique round identity.
    pub id: RecoveryRoundId,
    /// Exact owner-complete assignment cut from which the quorum was frozen.
    pub assignment_fence: CheckpointAssignmentFence,
}

impl RecoveryRound {
    /// Construct a fresh uniquely identified recovery round.
    ///
    /// # Errors
    /// Returns an error when the generation, driver, or assignment certificate is invalid.
    pub fn new(
        generation: u64,
        driver: NodeId,
        assignment_fence: CheckpointAssignmentFence,
    ) -> Result<Self, String> {
        let round = Self {
            id: RecoveryRoundId {
                generation,
                nonce: Uuid::new_v4(),
                driver,
            },
            assignment_fence,
        };
        round.validate()?;
        Ok(round)
    }

    /// Whether `node` belongs to the immutable recovery quorum.
    #[must_use]
    pub fn contains(&self, node: NodeId) -> bool {
        self.assignment_fence.contains(node.0)
    }

    /// Frozen quorum as runtime node identifiers.
    #[must_use]
    pub fn participants(&self) -> Vec<NodeId> {
        self.assignment_fence
            .participants
            .iter()
            .map(|participant| NodeId(participant.node_id))
            .collect()
    }

    /// Frozen boot identity for `node`.
    #[must_use]
    pub fn participant_incarnation(&self, node: NodeId) -> Option<Uuid> {
        self.assignment_fence.participant_incarnation(node.0)
    }

    fn validate(&self) -> Result<(), String> {
        if self.id.generation == 0 {
            return Err("recovery generation must be nonzero".into());
        }
        if self.id.nonce.is_nil() {
            return Err("recovery nonce must be non-nil".into());
        }
        if self.id.driver.is_unassigned() {
            return Err("recovery driver must be assigned".into());
        }
        if !self.assignment_fence.is_canonical() {
            return Err("recovery assignment certificate is not canonical".into());
        }
        if !self.contains(self.id.driver) {
            return Err("recovery driver is absent from the frozen quorum".into());
        }
        Ok(())
    }
}

/// Phase of a coordinated recovery round, carried in the `control:recover` slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RecoverPhase {
    /// Stop the pipeline and ack; the rewind target is announced after the cluster quiesces.
    Prepare,
    /// Rewind to `epoch` and restart.
    Start {
        /// Rewind target; `0` means no committed cut exists — restart fresh.
        epoch: u64,
    },
    /// Terminal authorisation to open source gates after the exact restore quorum.
    Release {
        /// The identical rewind target carried by `Start`.
        epoch: u64,
    },
}

/// Durable announcement for one exact recovery round.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecoveryAnnouncement {
    /// Frozen round identity and quorum.
    pub round: RecoveryRound,
    /// Current phase; `Start` is valid only after the identical `Prepare`.
    pub phase: RecoverPhase,
}

impl RecoveryAnnouncement {
    fn validate(&self) -> Result<(), String> {
        self.round.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct RecoveryRoundAck {
    round: RecoveryRound,
    incarnation: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct RecoveryAnnouncementAck {
    announcement: RecoveryAnnouncement,
    incarnation: Uuid,
}

/// Bounded durable proof that one exact process paused its revoking partitions for one
/// assignment handoff. The certificate digest binds the version, vnode-owner map, and complete
/// boot-incarnation roster; a version alone is not an assignment identity.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DrainAck {
    participant: CheckpointParticipant,
    round: AssignmentDrainId,
    sources: NodeDrainReceiptAggregate,
}

impl DrainAck {
    fn for_transition(
        participant: CheckpointParticipant,
        transition: &AssignmentDrainTransition,
        sources: NodeDrainReceiptAggregate,
    ) -> Self {
        Self {
            participant,
            round: transition.id(),
            sources,
        }
    }

    fn is_canonical(&self) -> bool {
        self.participant.node_id != 0
            && !self.participant.boot_incarnation.is_nil()
            && self.round.is_canonical()
            && self.sources.is_canonical()
            && self.sources.round_digest == self.round.digest
    }

    fn matches_transition(&self, transition: &AssignmentDrainTransition) -> bool {
        self.is_canonical()
            && self.round == transition.id()
            && transition
                .predecessor
                .participant_incarnation(self.participant.node_id)
                == Some(self.participant.boot_incarnation)
    }
}

fn encode_drain_ack(ack: &DrainAck) -> Result<String, String> {
    if !ack.is_canonical() {
        return Err("drain acknowledgement is not canonical".into());
    }
    let encoded = serde_json::to_string(ack)
        .map_err(|error| format!("could not encode drain acknowledgement: {error}"))?;
    if encoded.len() > MAX_DRAIN_ACK_BYTES {
        return Err(format!(
            "drain acknowledgement is {} bytes; maximum is {MAX_DRAIN_ACK_BYTES}",
            encoded.len()
        ));
    }
    Ok(encoded)
}

fn parse_drain_ack(raw: &str, publisher: NodeId) -> Result<DrainAck, String> {
    if raw.len() > MAX_DRAIN_ACK_BYTES {
        return Err(format!(
            "drain acknowledgement from {publisher} is {} bytes; maximum is {MAX_DRAIN_ACK_BYTES}",
            raw.len()
        ));
    }
    let ack: DrainAck = serde_json::from_str(raw)
        .map_err(|error| format!("invalid drain acknowledgement from {publisher}: {error}"))?;
    if !ack.is_canonical() || ack.participant.node_id != publisher.0 {
        return Err(format!(
            "drain acknowledgement from {publisher} has a non-canonical publisher"
        ));
    }
    if encode_drain_ack(&ack)? != raw {
        return Err(format!(
            "drain acknowledgement from {publisher} is not canonically encoded"
        ));
    }
    Ok(ack)
}

fn parse_recovery_announcement(raw: &str) -> Result<Option<RecoveryAnnouncement>, String> {
    if raw.is_empty() {
        return Ok(None);
    }
    let announcement: RecoveryAnnouncement = serde_json::from_str(raw)
        .map_err(|error| format!("invalid recovery announcement: {error}"))?;
    announcement.validate()?;
    Ok(Some(announcement))
}

fn parse_recovery_round_ack(raw: &str, publisher: NodeId) -> Result<RecoveryRound, String> {
    let ack: RecoveryRoundAck =
        serde_json::from_str(raw).map_err(|error| format!("invalid recovery ack: {error}"))?;
    ack.round.validate()?;
    if ack.round.participant_incarnation(publisher) != Some(ack.incarnation) {
        return Err(format!(
            "recovery acknowledgement from {publisher} has a stale process incarnation"
        ));
    }
    Ok(ack.round)
}

fn parse_recovery_announcement_ack(
    raw: &str,
    publisher: NodeId,
) -> Result<RecoveryAnnouncement, String> {
    let ack: RecoveryAnnouncementAck = serde_json::from_str(raw)
        .map_err(|error| format!("invalid recovery phase acknowledgement: {error}"))?;
    ack.announcement.validate()?;
    if ack.announcement.round.participant_incarnation(publisher) != Some(ack.incarnation) {
        return Err(format!(
            "recovery phase acknowledgement from {publisher} has a stale process incarnation"
        ));
    }
    Ok(ack.announcement)
}

/// Facade composing the cluster-control primitives.
pub struct ClusterController {
    instance_id: NodeId,
    /// Fast, process-local/discovery KV used for barriers, routing, and assignment liveness.
    kv: Arc<dyn ClusterKv>,
    /// Durable authority for coordinated recovery. This is deliberately separate from gossip:
    /// terminal recovery state and generations must survive every process in the cluster.
    recovery_kv: Arc<dyn ClusterKv>,
    barrier: BarrierCoordinator,
    snapshot: Option<Arc<AssignmentSnapshotStore>>,
    members_rx: watch::Receiver<Vec<NodeInfo>>,
    /// Locally computed version-bound checkpoint fence; admission/recovery borrows it instead of
    /// performing a gossip scan on the critical path.
    checkpoint_assignment_fence: watch::Sender<Option<CheckpointAssignmentFence>>,
    /// Locally audited drain transition. While present, the predecessor fence remains usable only
    /// for a checkpoint carrying the transition's exact leader term.
    checkpoint_drain_transition: watch::Sender<Option<AssignmentDrainTransition>>,
    /// Process-unique recovery identity, published before this node becomes Active.
    recovery_incarnation: Uuid,
    /// Cluster-wide minimum watermark from the leader's `Commit`; operators read this instead
    /// of their local watermark for consistent event-time. `i64::MIN` = uninitialised.
    cluster_min_watermark: Arc<AtomicI64>,
    /// While draining, the node excludes itself from [`Self::assignable_instances`] so the
    /// next rotation sheds its vnodes before it exits.
    draining: Arc<AtomicBool>,
    /// Held while a coordinated restart is in flight; the checkpoint gate consults it so
    /// no checkpoint is injected mid-recovery.
    recovering: Arc<AtomicBool>,
    /// Whether this node has announced itself as Active.
    active: Arc<AtomicBool>,
    /// False permanently after the durable stable-node lease is lost.
    process_lease_live: Arc<AtomicBool>,
    /// Monotonic deadline consulted directly by hot compute/control paths.
    process_lease_deadline: std::sync::OnceLock<Arc<super::LeaseDeadline>>,
    /// Whether this node may be elected leader. Draining nodes retain checkpoint ownership but
    /// must yield coordination immediately so a successor can rotate their vnodes away.
    leader_eligible: Arc<AtomicBool>,
    /// Wakes the leader candidacy relay when local eligibility changes.
    leader_eligibility_changes: watch::Sender<bool>,
    /// Serialises this node's recovery-slot compare-and-clear with phase transitions.
    recovery_writes: tokio::sync::Mutex<()>,
    /// Peers that recently missed a capture quorum, keyed by node id. Gossip failure detection
    /// can lag a hard kill by tens of seconds; the checkpoint gate consults this faster local
    /// signal to fail doomed epochs. Cleared on re-ack, expires after [`UNRESPONSIVE_TTL`].
    unresponsive: Arc<parking_lot::Mutex<rustc_hash::FxHashMap<u64, std::time::Instant>>>,
    /// This node's own failure-domain locality; peers carry theirs in `members_rx`.
    self_locality: parking_lot::RwLock<Locality>,
    /// Handler serving cross-node `RemoteScan`, shared with the query server.
    #[cfg(feature = "cluster")]
    query_handler: super::query::QueryHandlerSlot,
    /// Pooled channels to peers for cross-node `RemoteScan`.
    #[cfg(feature = "cluster")]
    query_client_pool: super::query::QueryClientPool,
    /// When wired, leadership is lease-fenced: [`Self::is_leader`] also requires the durable
    /// lease. Absent in embedded / static-discovery deployments (gossip-only leadership).
    #[cfg(feature = "cluster")]
    leader_lease: std::sync::OnceLock<LeaderLeaseGate>,
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
        Self::new_with_recovery_kv(instance_id, Arc::clone(&kv), kv, snapshot, members_rx)
    }

    /// Wrap the given primitives with a separate durable recovery authority.
    ///
    /// The barrier/discovery KV remains the low-latency path. `recovery_kv` owns recovery
    /// incarnations, generations, phase announcements, and acknowledgements.
    #[must_use]
    pub fn new_with_recovery_kv(
        instance_id: NodeId,
        kv: Arc<dyn ClusterKv>,
        recovery_kv: Arc<dyn ClusterKv>,
        snapshot: Option<Arc<AssignmentSnapshotStore>>,
        members_rx: watch::Receiver<Vec<NodeInfo>>,
    ) -> Self {
        Self::new_with_recovery_incarnation(
            instance_id,
            kv,
            recovery_kv,
            snapshot,
            members_rx,
            Uuid::new_v4(),
        )
    }

    /// Wrap the control primitives with a caller-generated boot identity.
    #[must_use]
    pub fn new_with_recovery_incarnation(
        instance_id: NodeId,
        kv: Arc<dyn ClusterKv>,
        recovery_kv: Arc<dyn ClusterKv>,
        snapshot: Option<Arc<AssignmentSnapshotStore>>,
        members_rx: watch::Receiver<Vec<NodeInfo>>,
        recovery_incarnation: Uuid,
    ) -> Self {
        let leader_eligible = Arc::new(AtomicBool::new(true));
        let mut barrier = BarrierCoordinator::new(Arc::clone(&kv));
        #[cfg(feature = "cluster")]
        barrier.set_leader_election(
            instance_id,
            members_rx.clone(),
            Arc::clone(&leader_eligible),
        );
        Self {
            instance_id,
            barrier,
            kv,
            recovery_kv,
            snapshot,
            members_rx,
            // A new leader must not checkpoint until it proves exact assignment convergence.
            checkpoint_assignment_fence: watch::channel(None).0,
            checkpoint_drain_transition: watch::channel(None).0,
            recovery_incarnation,
            cluster_min_watermark: Arc::new(AtomicI64::new(i64::MIN)),
            draining: Arc::new(AtomicBool::new(false)),
            recovering: Arc::new(AtomicBool::new(false)),
            active: Arc::new(AtomicBool::new(true)),
            process_lease_live: Arc::new(AtomicBool::new(true)),
            process_lease_deadline: std::sync::OnceLock::new(),
            leader_eligible,
            leader_eligibility_changes: watch::channel(false).0,
            recovery_writes: tokio::sync::Mutex::new(()),
            unresponsive: Arc::new(parking_lot::Mutex::new(rustc_hash::FxHashMap::default())),
            self_locality: parking_lot::RwLock::new(Locality::default()),
            #[cfg(feature = "cluster")]
            query_handler: Arc::new(parking_lot::RwLock::new(None)),
            #[cfg(feature = "cluster")]
            query_client_pool: Arc::new(parking_lot::Mutex::new(rustc_hash::FxHashMap::default())),
            #[cfg(feature = "cluster")]
            leader_lease: std::sync::OnceLock::new(),
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

    /// Install the durable authority used to validate clustered checkpoint barriers.
    #[cfg(feature = "cluster")]
    pub fn set_leader_lease_store(&self, store: Arc<super::LeaderLeaseStore>) {
        self.barrier.set_leader_lease_store(store);
    }

    /// Exact durable authority installed for this cluster controller.
    ///
    /// Cluster checkpoint code must use this handle rather than standalone outcome keys.
    #[cfg(feature = "cluster")]
    pub fn checkpoint_authority(
        &self,
    ) -> Result<Arc<super::LeaderLeaseStore>, super::ClusterCheckpointAuthorityError> {
        self.barrier.checkpoint_authority()
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

    /// Mirror the leader's computed cluster-min watermark into the atomic so its own operators
    /// match followers. Monotonic — never lowers the published value.
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

    /// The cluster gossip KV, for advertising/discovering per-stream state.
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
            .filter(|m| m.id != self.instance_id && matches!(m.state, NodeState::Active))
            .map(|m| m.id)
            .collect();
        if self.leader_eligible.load(Ordering::SeqCst) && self.process_lease_is_live() {
            ids.push(self.instance_id);
        }
        leader_of(&ids)
    }

    /// True if this node is the gossip-elected candidate (lowest active id), ignoring the
    /// lease. The lease manager acquires only while this holds.
    #[must_use]
    pub fn is_gossip_leader(&self) -> bool {
        self.current_leader() == Some(self.instance_id)
    }

    /// True if this node may act as leader — the gate all leader-gated work inherits. With a
    /// lease wired, also requires this exact process's live local-monotonic grant.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        if !self.process_lease_is_live() {
            return false;
        }
        if !self.is_gossip_leader() {
            return false;
        }
        #[cfg(feature = "cluster")]
        if let Some(gate) = self.leader_lease.get() {
            return super::lease_grants_leadership(
                &gate.lease.borrow(),
                &gate.owner,
                &gate.deadline,
            );
        }
        true
    }

    /// Whether this controller is wired to a durable leader lease. Gossip-only leadership is
    /// adequate for best-effort coordination but cannot certify exactly-once cluster decisions.
    #[must_use]
    #[cfg(feature = "cluster")]
    pub fn has_leader_lease_fencing(&self) -> bool {
        self.leader_lease.get().is_some()
    }

    /// Current live fencing token when this node owns the durable lease.
    ///
    /// This is an observation for detecting that an operation crossed leadership terms. Reading
    /// it does **not** fence a later object-store write: a durable mutation is fenced only when
    /// the storage operation atomically validates the token. Callers must not stamp this value
    /// into an otherwise unconditional write and treat that as a correctness boundary.
    #[must_use]
    #[cfg(feature = "cluster")]
    pub fn leader_fencing_token(&self) -> Option<u64> {
        self.capture_leader_proof().map(|proof| proof.fencing_token)
    }

    /// Capture the exact durable leader term currently authorized by local monotonic leases.
    ///
    /// Gossip determines candidacy only. A proof is available solely when this process is the
    /// candidate, both process and leader deadlines are live, and the observed durable grant has
    /// this process's exact owner identity.
    #[must_use]
    #[cfg(feature = "cluster")]
    pub fn capture_leader_proof(&self) -> Option<super::LeaderProof> {
        if !self.process_lease_proof_is_live() || !self.is_gossip_leader() {
            return None;
        }
        let gate = self.leader_lease.get()?;
        let lease = gate.lease.borrow();
        if !super::lease_grants_leadership(&lease, &gate.owner, &gate.deadline) {
            return None;
        }
        lease.as_ref().map(super::LeaderLease::proof)
    }

    /// Whether a captured proof remains the exact current locally authorized leader term.
    #[must_use]
    #[cfg(feature = "cluster")]
    pub fn proof_is_live(&self, proof: &super::LeaderProof) -> bool {
        if !self.process_lease_proof_is_live() || !self.is_gossip_leader() {
            return false;
        }
        let Some(gate) = self.leader_lease.get() else {
            return false;
        };
        super::lease_grants_proof(&gate.lease.borrow(), &gate.owner, &gate.deadline, proof)
    }

    /// Capture the durable leader grant while forming a cluster with no active member.
    ///
    /// Every joining process may contend in a cold start, but the shared lease store admits only
    /// one exact owner. As soon as an active member is visible, a joining process ceases to be a
    /// candidate and cannot use its observation to publish catalog state.
    #[must_use]
    #[cfg(feature = "cluster")]
    pub fn capture_catalog_bootstrap_proof(&self) -> Option<super::LeaderProof> {
        if !self.process_lease_proof_is_live() || !self.is_leader_lease_candidate() {
            return None;
        }
        let gate = self.leader_lease.get()?;
        let lease = gate.lease.borrow();
        if !super::lease_grants_leadership(&lease, &gate.owner, &gate.deadline) {
            return None;
        }
        lease.as_ref().map(super::LeaderLease::proof)
    }

    /// Whether a catalog-bootstrap proof remains owned by this cold-start candidate.
    #[must_use]
    #[cfg(feature = "cluster")]
    pub fn catalog_bootstrap_proof_is_live(&self, proof: &super::LeaderProof) -> bool {
        if !self.process_lease_proof_is_live() || !self.is_leader_lease_candidate() {
            return false;
        }
        let Some(gate) = self.leader_lease.get() else {
            return false;
        };
        super::lease_grants_proof(&gate.lease.borrow(), &gate.owner, &gate.deadline, proof)
    }

    /// Subscribe to leader-grant changes for evented proof cancellation.
    ///
    /// After notification, callers must use [`Self::proof_is_live`]. Candidacy changes are
    /// available separately through [`Self::leader_candidacy_watch`]. The lease manager publishes
    /// `None` when its monotonic deadline expires, so no polling loop is required.
    #[must_use]
    #[cfg(feature = "cluster")]
    pub fn leader_grant_watch(&self) -> Option<watch::Receiver<Option<super::LeaderLease>>> {
        self.leader_lease.get().map(|gate| gate.lease.clone())
    }

    #[cfg(feature = "cluster")]
    fn process_lease_proof_is_live(&self) -> bool {
        self.process_lease_live.load(Ordering::Acquire)
            && self
                .process_lease_deadline
                .get()
                .is_some_and(|deadline| deadline.is_live())
    }

    /// Wire the exact owner and local deadline used to fence leader work.
    ///
    /// # Errors
    /// Rejects an owner for another node or duplicate installation.
    #[cfg(feature = "cluster")]
    pub fn set_leader_lease_watch(
        &self,
        lease: watch::Receiver<Option<super::LeaderLease>>,
        owner: super::LeaderLeaseOwner,
        deadline: Arc<super::LeaseDeadline>,
    ) -> Result<(), String> {
        if owner.node != self.instance_id || owner.boot.is_nil() || owner.process_term == 0 {
            return Err("leader lease owner does not match this process".into());
        }
        self.leader_lease
            .set(LeaderLeaseGate {
                lease,
                owner,
                deadline,
            })
            .map_err(|_| "leader lease fencing is already installed".into())
    }

    fn notify_leader_eligibility_change(&self) {
        self.leader_eligibility_changes.send_modify(|signal| {
            *signal = !*signal;
        });
    }

    fn is_leader_lease_candidate(&self) -> bool {
        if self.is_gossip_leader() {
            return true;
        }
        !self.active.load(Ordering::Acquire)
            && !self.is_draining()
            && self.process_lease_is_live()
            && !self
                .members_rx
                .borrow()
                .iter()
                .any(|member| matches!(member.state, NodeState::Active))
    }

    /// Evented lease candidacy, including cold cluster formation and normal gossip leadership.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn leader_candidacy_watch(self: &Arc<Self>) -> watch::Receiver<bool> {
        let (candidate_tx, candidate_rx) = watch::channel(self.is_leader_lease_candidate());
        let mut members = self.members_rx.clone();
        let mut eligibility = self.leader_eligibility_changes.subscribe();
        let controller = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = candidate_tx.closed() => return,
                    changed = members.changed() => {
                        if changed.is_err() {
                            candidate_tx.send_replace(false);
                            return;
                        }
                    }
                    changed = eligibility.changed() => {
                        if changed.is_err() {
                            candidate_tx.send_replace(false);
                            return;
                        }
                    }
                }
                let Some(controller) = controller.upgrade() else {
                    return;
                };
                candidate_tx.send_replace(controller.is_leader_lease_candidate());
            }
        });
        candidate_rx
    }

    /// Mark this node's active status.
    pub fn set_active(&self, active: bool) {
        let active = active && self.process_lease_is_live();
        self.active.store(active, Ordering::SeqCst);
        let eligible = active && !self.is_draining();
        if self.leader_eligible.swap(eligible, Ordering::SeqCst) != eligible {
            self.notify_leader_eligibility_change();
        }
    }

    /// Permanently fence controller mutations after stable-node lease loss.
    pub fn fence_process_lease(&self) {
        self.process_lease_live.store(false, Ordering::SeqCst);
        if let Some(deadline) = self.process_lease_deadline.get() {
            deadline.fence();
        }
        self.active.store(false, Ordering::SeqCst);
        if self.leader_eligible.swap(false, Ordering::SeqCst) {
            self.notify_leader_eligibility_change();
        }
        self.recovering.store(true, Ordering::SeqCst);
    }

    /// Whether this process still owns its stable node identity.
    #[must_use]
    pub fn process_lease_is_live(&self) -> bool {
        self.process_lease_live.load(Ordering::Acquire)
            && self
                .process_lease_deadline
                .get()
                .is_none_or(|deadline| deadline.is_live())
    }

    /// Install the stable-node lease deadline before the runtime starts.
    pub fn set_process_lease_deadline(&self, deadline: Arc<super::LeaseDeadline>) {
        let _ = self.process_lease_deadline.set(deadline);
    }

    /// Live instance IDs: `Active` peers plus self.
    #[must_use]
    pub fn live_instances(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self
            .members_rx
            .borrow()
            .iter()
            .filter(|m| m.id != self.instance_id && matches!(m.state, NodeState::Active))
            .map(|m| m.id)
            .collect();
        if self.active.load(Ordering::SeqCst) && self.process_lease_is_live() {
            ids.push(self.instance_id);
        }
        ids
    }

    /// Nodes that must participate in a checkpoint for the current assignment.
    ///
    /// Draining owners remain responsible for their old vnodes through the handoff checkpoint,
    /// while joining, suspected, left, and missing nodes cannot contribute to a restorable cut.
    /// This is deliberately distinct from [`Self::assignable_instances`], which excludes draining
    /// nodes so they never receive new ownership.
    #[must_use]
    pub fn checkpoint_instances(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self
            .members_rx
            .borrow()
            .iter()
            .filter(|member| {
                member.id != self.instance_id
                    && matches!(member.state, NodeState::Active | NodeState::Draining)
            })
            .map(|member| member.id)
            .collect();
        if self.active.load(Ordering::SeqCst) && self.process_lease_is_live() {
            ids.push(self.instance_id);
        }
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Record peers that failed to ack a capture quorum in time.
    pub fn note_unresponsive(&self, peers: &[NodeId]) {
        let now = std::time::Instant::now();
        let mut map = self.unresponsive.lock();
        for p in peers {
            map.insert(p.0, now);
        }
    }

    /// Clear peers that acked (they are demonstrably alive).
    pub fn note_responsive(&self, peers: &[NodeId]) {
        let mut map = self.unresponsive.lock();
        for p in peers {
            map.remove(&p.0);
        }
    }

    /// Whether `peer` failed a capture quorum within the TTL window.
    #[must_use]
    pub fn is_recently_unresponsive(&self, peer: NodeId) -> bool {
        /// How long a quorum miss keeps a peer suspect for the gate.
        const UNRESPONSIVE_TTL: Duration = Duration::from_secs(60);
        self.unresponsive
            .lock()
            .get(&peer.0)
            .is_some_and(|at| at.elapsed() < UNRESPONSIVE_TTL)
    }

    /// Mark this node as draining. Idempotent.
    pub fn begin_drain(&self) {
        self.draining.store(true, Ordering::SeqCst);
        if self.leader_eligible.swap(false, Ordering::SeqCst) {
            self.notify_leader_eligibility_change();
        }
    }

    /// Whether this node is draining.
    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    /// Set or clear the coordinated-recovery fence.
    pub fn set_recovering(&self, recovering: bool) {
        self.recovering.store(recovering, Ordering::SeqCst);
    }

    /// Whether a coordinated restart is in flight on this node.
    #[must_use]
    pub fn is_recovering(&self) -> bool {
        self.recovering.load(Ordering::SeqCst)
    }

    /// Write a `u64` control signal into this node's `key` slot.
    async fn write_u64(&self, key: &str, value: u64) {
        if self.process_lease_is_live() {
            self.kv.write(key, value.to_string()).await;
        }
    }

    /// Every visible node's `u64` value for `key`.
    async fn read_u64_map(&self, key: &str) -> Vec<(NodeId, u64)> {
        self.kv
            .scan(key)
            .await
            .into_iter()
            .filter_map(|(n, v)| v.parse::<u64>().ok().map(|x| (n, x)))
            .collect()
    }

    async fn write_recovery_value(&self, key: &str, value: String) -> Result<(), String> {
        if !self.process_lease_is_live() {
            return Err("stable node process lease is no longer live".into());
        }
        tokio::time::timeout(
            RECOVERY_CONTROL_IO_TIMEOUT,
            self.recovery_kv.write_checked(key, value),
        )
        .await
        .map_err(|_| format!("recovery control write for {key} timed out"))?
        .map_err(|error| format!("recovery control write for {key} failed: {error}"))
    }

    async fn write_recovery_value_exact(&self, key: &str, value: String) -> Result<(), String> {
        self.write_recovery_value(key, value.clone()).await?;
        let observed = self
            .read_recovery_value(self.instance_id, key)
            .await?
            .ok_or_else(|| format!("recovery control value for {key} vanished after write"))?;
        if observed != value {
            return Err(format!(
                "recovery control read-back mismatch for {key}; write was not durable"
            ));
        }
        Ok(())
    }

    async fn read_recovery_value(&self, node: NodeId, key: &str) -> Result<Option<String>, String> {
        tokio::time::timeout(
            RECOVERY_CONTROL_IO_TIMEOUT,
            self.recovery_kv.read_from_checked(node, key),
        )
        .await
        .map_err(|_| format!("recovery control read for {key} from {node} timed out"))?
        .map_err(|error| format!("recovery control read for {key} from {node} failed: {error}"))
    }

    async fn scan_recovery_values(&self, key: &str) -> Result<Vec<(NodeId, String)>, String> {
        tokio::time::timeout(
            RECOVERY_CONTROL_IO_TIMEOUT,
            self.recovery_kv.scan_checked(key),
        )
        .await
        .map_err(|_| format!("recovery control scan for {key} timed out"))?
        .map_err(|error| format!("recovery control scan for {key} failed: {error}"))
    }

    /// Highest durable recovery generation replicated by any visible participant.
    ///
    /// # Errors
    /// Fails closed when the durable scan is unavailable or contains malformed state.
    pub async fn max_recovery_generation(&self) -> Result<u64, String> {
        let mut maximum = 0;
        for (node, raw) in self.scan_recovery_values("control:recovery-gen").await? {
            let generation = raw.parse::<u64>().map_err(|error| {
                format!("invalid replicated recovery generation from {node}: {error}")
            })?;
            maximum = maximum.max(generation);
        }
        Ok(maximum)
    }

    /// Monotonically persist this participant's recovery generation and read it back exactly.
    ///
    /// # Errors
    /// Rejects zero, regression, unavailable durable storage, or a mismatched read-back.
    pub async fn adopt_recovery_generation(&self, generation: u64) -> Result<(), String> {
        if generation == 0 {
            return Err("recovery generation must be nonzero".into());
        }
        let current = self
            .read_recovery_value(self.instance_id, "control:recovery-gen")
            .await?
            .map(|raw| {
                raw.parse::<u64>()
                    .map_err(|error| format!("invalid local recovery generation: {error}"))
            })
            .transpose()?;
        if let Some(current) = current {
            if current > generation {
                return Err(format!(
                    "local recovery generation {current} is newer than proposed generation {generation}"
                ));
            }
            if current == generation {
                return Ok(());
            }
        }
        self.write_recovery_value_exact("control:recovery-gen", generation.to_string())
            .await
    }

    async fn read_recovery_round_map(
        &self,
        key: &str,
    ) -> Result<Vec<(NodeId, RecoveryRound)>, String> {
        self.scan_recovery_values(key)
            .await?
            .into_iter()
            .map(|(node, raw)| parse_recovery_round_ack(&raw, node).map(|round| (node, round)))
            .collect()
    }

    async fn read_recovery_announcement_map(
        &self,
        key: &str,
    ) -> Result<Vec<(NodeId, RecoveryAnnouncement)>, String> {
        let mut announcements = Vec::new();
        for (node, raw) in self.scan_recovery_values(key).await? {
            let announcement = parse_recovery_announcement_ack(&raw, node)?;
            announcements.push((node, announcement));
        }
        Ok(announcements)
    }

    /// This process's boot-unique recovery identity.
    #[must_use]
    pub fn recovery_incarnation(&self) -> Uuid {
        self.recovery_incarnation
    }

    /// Publish and read back this process's incarnation before announcing the node Active.
    ///
    /// # Errors
    /// Returns an error when the control write/read is unavailable or the read-back differs.
    pub async fn publish_recovery_incarnation(&self) -> Result<(), String> {
        self.write_recovery_value(
            RECOVERY_INCARNATION_KEY,
            self.recovery_incarnation.to_string(),
        )
        .await?;
        let observed = self
            .read_recovery_value(self.instance_id, RECOVERY_INCARNATION_KEY)
            .await?
            .ok_or_else(|| "recovery incarnation was not readable after publication".to_string())?;
        let observed = Uuid::parse_str(&observed)
            .map_err(|error| format!("invalid recovery incarnation after publication: {error}"))?;
        if observed != self.recovery_incarnation {
            return Err("recovery incarnation read-back mismatch".into());
        }
        Ok(())
    }

    /// Publish the recovery identity only when it matches an acquired stable-node lease.
    ///
    /// # Errors
    /// Rejects a lease for another node or boot identity, or a failed durable publication.
    pub async fn publish_leased_recovery_incarnation(
        &self,
        lease: &super::ProcessLease,
    ) -> Result<(), String> {
        if lease.node != self.instance_id || lease.owner != self.recovery_incarnation {
            return Err("process lease does not bind this recovery incarnation".into());
        }
        self.publish_recovery_incarnation().await
    }

    async fn recovery_incarnation_is_current(&self) -> Result<bool, String> {
        let Some(raw) = self
            .read_recovery_value(self.instance_id, RECOVERY_INCARNATION_KEY)
            .await?
        else {
            return Ok(false);
        };
        let observed = Uuid::parse_str(&raw)
            .map_err(|error| format!("invalid local recovery incarnation: {error}"))?;
        Ok(observed == self.recovery_incarnation)
    }

    /// Resolve the exact live boot identity for every canonical participant.
    ///
    /// # Errors
    /// Fails closed on a missing, malformed, nil, duplicate, or unexpected participant identity.
    pub async fn recovery_participant_incarnations(
        &self,
        participants: &[u64],
    ) -> Result<Vec<CheckpointParticipant>, String> {
        if participants.is_empty()
            || participants.contains(&0)
            || participants.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err("recovery incarnation roster requires canonical participants".into());
        }
        let expected: std::collections::BTreeSet<u64> = participants.iter().copied().collect();
        let mut reported = std::collections::BTreeMap::new();
        for (node, raw) in self.scan_recovery_values(RECOVERY_INCARNATION_KEY).await? {
            if !expected.contains(&node.0) {
                continue;
            }
            let incarnation = Uuid::parse_str(&raw).map_err(|error| {
                format!("invalid recovery incarnation published by {node}: {error}")
            })?;
            if incarnation.is_nil() {
                return Err(format!("nil recovery incarnation published by {node}"));
            }
            if reported.insert(node.0, incarnation).is_some() {
                return Err(format!(
                    "duplicate recovery incarnation published by {node}"
                ));
            }
        }
        participants
            .iter()
            .map(|node_id| {
                reported
                    .get(node_id)
                    .copied()
                    .map(|incarnation| CheckpointParticipant {
                        node_id: *node_id,
                        boot_incarnation: incarnation,
                    })
                    .ok_or_else(|| format!("node {node_id} has no current recovery incarnation"))
            })
            .collect()
    }

    /// Whether every current participant boot identity still equals the frozen round.
    ///
    /// # Errors
    /// Returns an error when the current incarnation roster is unavailable or malformed.
    pub async fn recovery_incarnations_match(&self, round: &RecoveryRound) -> Result<bool, String> {
        Ok(self
            .recovery_participant_incarnations(&round.assignment_fence.participant_ids())
            .await?
            == round.assignment_fence.participants)
    }

    /// Publish this node's fault sequence so the leader drives a recovery round.
    ///
    /// # Errors
    /// Fails when the bounded write/read-back does not persist the exact sequence.
    pub async fn report_fault(&self, seq: u64) -> Result<(), String> {
        if seq == 0 {
            return Err("fault sequence must be nonzero".into());
        }
        self.write_recovery_value_exact("control:fault-report", seq.to_string())
            .await
    }

    /// Clear this node's fault report (`0` = no fault) after it recovers, so a restarted
    /// leader doesn't re-trigger recovery for an already-handled fault.
    ///
    /// # Errors
    /// Returns an error when the bounded write or exact read-back fails.
    pub async fn clear_fault_report(&self) -> Result<(), String> {
        self.write_recovery_value_exact("control:fault-report", "0".into())
            .await
    }

    /// Each visible node's reported fault sequence.
    ///
    /// # Errors
    /// Returns an error when the bounded scan fails or any report is malformed.
    pub async fn read_fault_reports(&self) -> Result<Vec<(NodeId, u64)>, String> {
        self.scan_recovery_values("control:fault-report")
            .await?
            .into_iter()
            .map(|(node, raw)| {
                raw.parse::<u64>()
                    .map(|sequence| (node, sequence))
                    .map_err(|error| format!("invalid fault sequence from {node}: {error}"))
            })
            .collect()
    }

    /// Publish that this node restored the exact frozen recovery round.
    ///
    /// # Errors
    /// Returns an error for an invalid round or when this node is outside its frozen quorum.
    pub async fn announce_recovered(&self, start: &RecoveryAnnouncement) -> Result<(), String> {
        start.validate()?;
        if !matches!(start.phase, RecoverPhase::Start { .. }) {
            return Err("restore acknowledgement must bind a Start target".into());
        }
        if !start.round.contains(self.instance_id) {
            return Err("node outside recovery quorum cannot acknowledge restore".into());
        }
        if start.round.participant_incarnation(self.instance_id) != Some(self.recovery_incarnation)
        {
            return Err("restore acknowledgement has a stale local process incarnation".into());
        }
        if !self.recovery_incarnation_is_current().await? {
            return Err("restore acknowledgement came from a superseded local process".into());
        }
        let encoded = serde_json::to_string(&RecoveryAnnouncementAck {
            announcement: start.clone(),
            incarnation: self.recovery_incarnation,
        })
        .map_err(|error| format!("could not encode recovery ack: {error}"))?;
        self.write_recovery_value_exact("control:recovered", encoded)
            .await
    }

    /// Each visible node's exact restored recovery round.
    ///
    /// # Errors
    /// Fails closed when any visible acknowledgement is malformed.
    pub async fn read_recovered(&self) -> Result<Vec<(NodeId, RecoveryAnnouncement)>, String> {
        self.read_recovery_announcement_map("control:recovered")
            .await
    }

    /// Publish that this exact process consumed the `Release` terminal record.
    ///
    /// # Errors
    /// Returns an error for a non-Release phase or a stale/nonparticipant process.
    pub async fn announce_released(&self, release: &RecoveryAnnouncement) -> Result<(), String> {
        release.validate()?;
        if !matches!(release.phase, RecoverPhase::Release { .. }) {
            return Err("release acknowledgement must bind a Release target".into());
        }
        if release.round.participant_incarnation(self.instance_id)
            != Some(self.recovery_incarnation)
        {
            return Err("release acknowledgement has a stale local process incarnation".into());
        }
        if !self.recovery_incarnation_is_current().await? {
            return Err("release acknowledgement came from a superseded local process".into());
        }
        let encoded = serde_json::to_string(&RecoveryAnnouncementAck {
            announcement: release.clone(),
            incarnation: self.recovery_incarnation,
        })
        .map_err(|error| format!("could not encode release acknowledgement: {error}"))?;
        self.write_recovery_value_exact("control:recovery-released", encoded)
            .await
    }

    /// Each visible node's exact terminal Release acknowledgement.
    ///
    /// # Errors
    /// Returns an error when the bounded scan fails or any acknowledgement is malformed.
    pub async fn read_released(&self) -> Result<Vec<(NodeId, RecoveryAnnouncement)>, String> {
        self.read_recovery_announcement_map("control:recovery-released")
            .await
    }

    /// Node ids eligible to own vnodes: `Active` peers, plus self unless draining. Unlike
    /// [`Self::live_instances`], non-`Active` peers are filtered so they never receive vnodes.
    #[must_use]
    pub fn assignable_instances(&self) -> Vec<NodeId> {
        let mut ids = assignable_node_ids(&self.members_rx.borrow());
        ids.retain(|id| *id != self.instance_id);
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

    /// Cloneable membership watch for reacting to join/leave events without polling.
    #[must_use]
    pub fn members_watch(&self) -> watch::Receiver<Vec<NodeInfo>> {
        self.members_rx.clone()
    }

    /// Write the current assignment snapshot version to gossip KV.
    pub async fn announce_snapshot_version(&self, version: u64) {
        self.write_u64("control:snapshot-version", version).await;
    }

    /// Max snapshot version across all peers.
    pub async fn read_snapshot_version(&self) -> Option<u64> {
        self.read_u64_map("control:snapshot-version")
            .await
            .into_iter()
            .map(|(_, v)| v)
            .max()
    }

    async fn drain_transition_authority_is_current(
        &self,
        transition: &AssignmentDrainTransition,
    ) -> Result<bool, String> {
        #[cfg(feature = "cluster")]
        {
            let authority = self
                .checkpoint_authority()
                .map_err(|error| error.to_string())?;
            let Some(lease) = authority.load().await.map_err(|error| error.to_string())? else {
                return Ok(false);
            };
            Ok(lease.matches_proof(&transition.leader))
        }
        #[cfg(not(feature = "cluster"))]
        {
            let _ = transition;
            Err("assignment drain authority requires the cluster feature".into())
        }
    }

    /// Publish and exactly read back proof that every local source reached one exact external
    /// input cut for `transition`.
    ///
    /// # Errors
    /// Rejects a nonparticipant, stale boot, superseded process, malformed certificate, or failed
    /// durable write/read-back.
    pub async fn announce_drain_ack(
        &self,
        transition: &AssignmentDrainTransition,
        sources: NodeDrainReceiptAggregate,
    ) -> Result<(), String> {
        if !transition.is_canonical()
            || !sources.is_canonical()
            || sources.round_digest != transition.digest()
        {
            return Err("drain acknowledgement requires an exact source transition proof".into());
        }
        let participant = CheckpointParticipant {
            node_id: self.instance_id.0,
            boot_incarnation: self.recovery_incarnation,
        };
        if transition
            .predecessor
            .participant_incarnation(participant.node_id)
            != Some(participant.boot_incarnation)
        {
            return Err("drain acknowledgement does not bind the local process".into());
        }
        if self.checkpoint_drain_transition.borrow().as_ref() != Some(transition) {
            return Err("drain acknowledgement is not the locally audited transition".into());
        }
        if !self.process_lease_is_live()
            || !self
                .drain_transition_authority_is_current(transition)
                .await?
        {
            return Err("drain acknowledgement authority is no longer live".into());
        }
        if !self.recovery_incarnation_is_current().await? {
            return Err("drain acknowledgement came from a superseded local process".into());
        }
        self.write_recovery_value_exact(
            DRAIN_ACK_KEY,
            encode_drain_ack(&DrainAck::for_transition(participant, transition, sources))?,
        )
        .await
    }

    /// Whether every process in the exact frozen assignment roster durably acknowledged the same
    /// certificate and still owns its certified boot identity.
    ///
    /// Records from nodes outside `fence` are ignored because durable per-node slots can outlive
    /// membership. A missing or different-version record never contributes to quorum.
    ///
    /// # Errors
    /// Fails closed when the certificate, current boot roster, durable scan, or an expected
    /// participant's record is malformed.
    pub async fn drain_ack_quorum_reached(
        &self,
        transition: &AssignmentDrainTransition,
    ) -> Result<bool, String> {
        if !transition.is_canonical() {
            return Err("drain quorum requires a canonical assignment transition".into());
        }
        if self.checkpoint_drain_transition.borrow().as_ref() != Some(transition)
            || !self
                .drain_transition_authority_is_current(transition)
                .await?
        {
            return Ok(false);
        }
        if self
            .recovery_participant_incarnations(&transition.predecessor.participant_ids())
            .await?
            != transition.predecessor.participants
        {
            return Ok(false);
        }

        let expected: std::collections::BTreeSet<NodeId> = transition
            .required_participants()
            .iter()
            .map(|participant| NodeId(participant.node_id))
            .collect();
        let mut seen = std::collections::BTreeSet::new();
        let mut matching = std::collections::BTreeSet::new();
        for (publisher, raw) in self.scan_recovery_values(DRAIN_ACK_KEY).await? {
            if !expected.contains(&publisher) {
                continue;
            }
            if !seen.insert(publisher) {
                return Err(format!(
                    "duplicate drain acknowledgement from expected participant {publisher}"
                ));
            }
            let ack = parse_drain_ack(&raw, publisher)?;
            if ack.matches_transition(transition) {
                matching.insert(publisher);
            }
        }
        if matching != expected {
            return Ok(false);
        }
        // Close the read/read race: a process can restart after the first incarnation scan but
        // before its old acknowledgement is observed. Revalidate after the exact ack cut.
        Ok(self
            .recovery_participant_incarnations(&transition.predecessor.participant_ids())
            .await?
            == transition.predecessor.participants
            && self
                .drain_transition_authority_is_current(transition)
                .await?)
    }

    /// Publish and exactly read back this process's adopted assignment map.
    ///
    /// # Errors
    /// Fails closed for a malformed/stale process report or unavailable control storage.
    pub async fn announce_adopted_assignment(
        &self,
        adoption: &CheckpointAssignmentAdoption,
    ) -> Result<(), String> {
        if !adoption.is_canonical()
            || adoption.participant.node_id != self.instance_id.0
            || adoption.participant.boot_incarnation != self.recovery_incarnation
            || !self.recovery_incarnation_is_current().await?
        {
            return Err("adopted assignment report does not bind the current process".into());
        }
        let encoded = serde_json::to_string(adoption)
            .map_err(|error| format!("could not encode adopted assignment: {error}"))?;
        self.write_recovery_value_exact("control:adopted-assignment", encoded)
            .await
    }

    /// Each visible process's exact adopted assignment identity.
    ///
    /// # Errors
    /// Fails closed if any visible report is malformed or claims another publisher.
    pub async fn read_adopted_assignments(
        &self,
    ) -> Result<Vec<(NodeId, CheckpointAssignmentAdoption)>, String> {
        self.scan_recovery_values("control:adopted-assignment")
            .await?
            .into_iter()
            .map(|(node, raw)| {
                let adoption: CheckpointAssignmentAdoption = serde_json::from_str(&raw)
                    .map_err(|error| format!("invalid adopted assignment from {node}: {error}"))?;
                if !adoption.is_canonical() || adoption.participant.node_id != node.0 {
                    return Err(format!(
                        "adopted assignment from {node} has a non-canonical publisher"
                    ));
                }
                Ok((node, adoption))
            })
            .collect()
    }

    /// Publish this node's version-bound checkpoint fence. Called off the hot path by the snapshot
    /// watcher; admission and recovery revalidate it locally against current membership.
    pub fn publish_checkpoint_assignment_fence(&self, fence: Option<CheckpointAssignmentFence>) {
        self.checkpoint_assignment_fence.send_replace(fence);
    }

    /// Publish the exact locally audited drain transition, or clear it after commit/abort.
    pub fn publish_checkpoint_drain_transition(
        &self,
        transition: Option<AssignmentDrainTransition>,
    ) {
        self.checkpoint_drain_transition.send_replace(transition);
    }

    /// Current locally audited drain transition.
    #[must_use]
    pub fn checkpoint_drain_transition(&self) -> Option<AssignmentDrainTransition> {
        self.checkpoint_drain_transition.borrow().clone()
    }

    /// Subscribe to local assignment-fence changes so a blocking durability probe can be
    /// interrupted when its checkpoint cut becomes stale.
    #[must_use]
    pub fn checkpoint_assignment_watch(
        &self,
    ) -> watch::Receiver<Option<CheckpointAssignmentFence>> {
        self.checkpoint_assignment_fence.subscribe()
    }

    /// Return the exact locally certified assignment cut when it still matches current
    /// membership. The clone is retained by the admitted attempt and propagated to followers.
    #[must_use]
    pub fn checkpoint_assignment_fence(
        &self,
        assignment_version: u64,
    ) -> Option<CheckpointAssignmentFence> {
        let current: Vec<u64> = self
            .checkpoint_instances()
            .into_iter()
            .map(|node| node.0)
            .collect();
        let transition = self.checkpoint_drain_transition.borrow();
        self.checkpoint_assignment_fence
            .borrow()
            .as_ref()
            .filter(|fence| {
                fence.is_canonical()
                    && fence.assignment_version == assignment_version
                    && fence.participant_incarnation(self.instance_id.0)
                        == Some(self.recovery_incarnation)
                    && (fence.participant_ids().as_slice() == current.as_slice()
                        || transition.as_ref().is_some_and(|transition| {
                            transition.is_canonical() && &transition.predecessor == *fence
                        }))
            })
            .cloned()
    }

    /// Return the locally certified assignment only when the announcing leader proof exactly
    /// matches the current durable authority. A predecessor retained during an in-progress drain
    /// additionally remains bound to the leader term that opened that drain.
    #[cfg(feature = "cluster")]
    pub async fn checkpoint_assignment_fence_for_leader(
        &self,
        assignment_version: u64,
        leader: &crate::checkpoint::LeaderProof,
    ) -> Option<CheckpointAssignmentFence> {
        if !leader.is_canonical() {
            return None;
        }
        let fence = self.checkpoint_assignment_fence(assignment_version)?;
        let authority = self.checkpoint_authority().ok()?;
        let lease = authority.load().await.ok().flatten()?;
        if !lease.matches_proof(leader) {
            return None;
        }
        let transition = self.checkpoint_drain_transition.borrow();
        if transition
            .as_ref()
            .is_some_and(|transition| transition.predecessor == fence)
            && transition.as_ref().map(|transition| &transition.leader) != Some(leader)
        {
            return None;
        }
        Some(fence)
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
        if !self.process_lease_is_live() {
            return Err("stable node process lease is no longer live".into());
        }
        self.barrier.announce(ann).await
    }

    /// Highest valid attempt announced anywhere in the cluster — used by a node reclaiming
    /// leadership to advance its allocator past the in-flight epoch.
    ///
    /// # Errors
    /// Fails when announcement history is malformed or conflicts across attempt dimensions.
    pub async fn max_announced_epoch(&self) -> Result<Option<CheckpointAttempt>, String> {
        self.barrier.max_announced().await
    }

    /// Follower-side observe; `Ok(None)` if no leader is visible.
    ///
    /// Observation is deliberately side-effect free. The caller must validate the exact
    /// checkpoint identity and assignment certificate before passing an `Aligned` or `Commit`
    /// to [`Self::accept_barrier_watermark`]. This prevents a malformed or stale announcement
    /// from advancing event time merely because it was visible in the control plane.
    ///
    /// # Errors
    /// Propagates [`BarrierCoordinator::observe`] errors.
    pub async fn observe_barrier(&self) -> Result<Option<BarrierAnnouncement>, String> {
        let Some(leader) = self.current_leader() else {
            return Ok(None);
        };
        self.barrier.observe(leader).await
    }

    /// Local monotonic receipt time for this exact direct Prepare, if gRPC delivered it.
    #[must_use]
    pub fn checkpoint_prepare_received_at(
        &self,
        prepare: &BarrierAnnouncement,
    ) -> Option<std::time::Instant> {
        self.barrier.prepare_received_at(prepare)
    }

    /// Accept an exact, assignment-certified checkpoint phase.
    ///
    /// Returns `true` when the announcement matches the expected attempt and is an `Aligned` or
    /// `Commit` phase. Only an immutable `Commit` may advance the recovery-safe watermark;
    /// `Aligned` releases the live data path but can still abort before a durable decision. A
    /// matching phase without a watermark is accepted without changing state. All mismatches fail
    /// closed and leave the monotonic watermark untouched.
    pub fn accept_barrier_watermark(
        &self,
        announcement: &BarrierAnnouncement,
        expected_epoch: u64,
        expected_checkpoint_id: u64,
        expected_fence: &CheckpointAssignmentFence,
    ) -> bool {
        if !expected_fence.is_canonical()
            || announcement.epoch != expected_epoch
            || announcement.checkpoint_id != expected_checkpoint_id
            || announcement.assignment_fence.as_ref() != Some(expected_fence)
            || !matches!(announcement.phase, Phase::Aligned | Phase::Commit)
        {
            return false;
        }
        if announcement.phase == Phase::Commit {
            if let Some(watermark) = announcement.min_watermark_ms {
                self.publish_cluster_min_watermark(watermark);
            }
        }
        true
    }

    /// Follower-side ack.
    ///
    /// # Errors
    /// Propagates [`BarrierCoordinator::ack`] errors.
    pub async fn ack_barrier(&self, ack: &BarrierAck) -> Result<(), String> {
        if !self.process_lease_is_live() {
            return Err("stable node process lease is no longer live".into());
        }
        self.barrier.ack(ack).await
    }

    /// Announce phase 1 with the immutable assignment-certified quorum.
    ///
    /// # Errors
    /// Returns an error unless this node is the round's current leader and driver.
    pub async fn announce_recover_prepare(&self, round: &RecoveryRound) -> Result<(), String> {
        round.validate()?;
        if round.id.driver != self.instance_id || !self.is_leader() {
            return Err("only the current leader may prepare its recovery round".into());
        }
        if !self.recovery_incarnations_match(round).await? {
            return Err("recovery process-incarnation roster changed before Prepare".into());
        }
        let _guard = self.recovery_writes.lock().await;
        if !self.is_leader() {
            return Err("recovery driver lost leadership before Prepare publication".into());
        }
        let announcement = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Prepare,
        };
        let encoded = serde_json::to_string(&announcement)
            .map_err(|error| format!("could not encode recovery prepare: {error}"))?;
        self.write_recovery_value_exact("control:recover", encoded)
            .await
    }

    /// Transition the identical prepared round to `Start` with a target bound into the
    /// announcement. A missing or different `Prepare` is never upgraded.
    ///
    /// # Errors
    /// Returns an error on lost leadership, an invalid round, or a mismatched prior phase.
    pub async fn announce_recover_start(
        &self,
        round: &RecoveryRound,
        epoch: u64,
    ) -> Result<(), String> {
        round.validate()?;
        if round.id.driver != self.instance_id || !self.is_leader() {
            return Err("only the current leader may start its recovery round".into());
        }
        let _guard = self.recovery_writes.lock().await;
        if !self.is_leader() || !self.recovery_incarnations_match(round).await? {
            return Err(
                "recovery driver or process-incarnation roster changed before Start".into(),
            );
        }
        let current = self
            .read_recovery_value(self.instance_id, "control:recover")
            .await?
            .ok_or_else(|| "recovery Prepare disappeared before Start".to_string())?;
        let prepared = parse_recovery_announcement(&current)?
            .ok_or_else(|| "recovery Prepare was cleared before Start".to_string())?;
        if prepared.round != *round || prepared.phase != RecoverPhase::Prepare {
            return Err("recovery Start does not match the exact active Prepare".into());
        }
        let announcement = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Start { epoch },
        };
        let encoded = serde_json::to_string(&announcement)
            .map_err(|error| format!("could not encode recovery start: {error}"))?;
        self.write_recovery_value_exact("control:recover", encoded)
            .await
    }

    /// Transition the identical `Start` to the terminal `Release`. Source gates may open
    /// only after observing this exact record; it is never deleted as cleanup.
    ///
    /// # Errors
    /// Returns an error on lost leadership, a changed incarnation roster, or a mismatched Start.
    pub async fn announce_recover_release(
        &self,
        round: &RecoveryRound,
        epoch: u64,
    ) -> Result<(), String> {
        round.validate()?;
        if round.id.driver != self.instance_id || !self.is_leader() {
            return Err("only the current leader may release its recovery round".into());
        }
        let _guard = self.recovery_writes.lock().await;
        if !self.is_leader() || !self.recovery_incarnations_match(round).await? {
            return Err(
                "recovery driver or process-incarnation roster changed before Release".into(),
            );
        }
        let current = self
            .read_recovery_value(self.instance_id, "control:recover")
            .await?
            .ok_or_else(|| "recovery Start disappeared before Release".to_string())?;
        let started = parse_recovery_announcement(&current)?
            .ok_or_else(|| "recovery Start was cleared before Release".to_string())?;
        if started.round != *round || started.phase != (RecoverPhase::Start { epoch }) {
            return Err("recovery Release does not match the exact active Start target".into());
        }
        let release = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Release { epoch },
        };
        let encoded = serde_json::to_string(&release)
            .map_err(|error| format!("could not encode recovery Release: {error}"))?;
        self.write_recovery_value_exact("control:recover", encoded)
            .await
    }

    /// Active recovery announcement from the locally elected driver. Reading its exact slot is
    /// O(1); durable slots from retired drivers can neither mask nor conflict with the successor.
    ///
    /// # Errors
    /// Returns an error for malformed current-driver state or publisher/driver mismatch.
    pub async fn observe_recover(&self) -> Result<Option<RecoveryAnnouncement>, String> {
        let Some(current_driver) = self.current_leader() else {
            return Ok(None);
        };
        let Some(raw) = self
            .read_recovery_value(current_driver, "control:recover")
            .await?
        else {
            return Ok(None);
        };
        let Some(announcement) = parse_recovery_announcement(&raw)? else {
            return Ok(None);
        };
        if announcement.round.id.driver != current_driver {
            return Err(format!(
                "recovery publisher {current_driver} is not declared driver {}",
                announcement.round.id.driver
            ));
        }
        Ok(Some(announcement))
    }

    /// Whether the round's declared driver is the current elected leader in this local view.
    #[must_use]
    pub fn recovery_driver_is_current(&self, round: &RecoveryRound) -> bool {
        self.current_leader() == Some(round.id.driver)
    }

    /// Whether the frozen round names this exact process, not only its stable node id.
    #[must_use]
    pub fn recovery_round_contains_current_process(&self, round: &RecoveryRound) -> bool {
        round.participant_incarnation(self.instance_id) == Some(self.recovery_incarnation)
    }

    /// Ack phase 1 for the exact frozen round.
    ///
    /// # Errors
    /// Returns an error for invalid state or when this node is outside the quorum.
    pub async fn announce_stopped(&self, round: &RecoveryRound) -> Result<(), String> {
        round.validate()?;
        if !round.contains(self.instance_id) {
            return Err("node outside recovery quorum cannot acknowledge Prepare".into());
        }
        if !self.recovery_round_contains_current_process(round) {
            return Err("Prepare acknowledgement has a stale local process incarnation".into());
        }
        if !self.recovery_incarnation_is_current().await? {
            return Err("Prepare acknowledgement came from a superseded local process".into());
        }
        let encoded = serde_json::to_string(&RecoveryRoundAck {
            round: round.clone(),
            incarnation: self.recovery_incarnation,
        })
        .map_err(|error| format!("could not encode recovery ack: {error}"))?;
        self.write_recovery_value_exact("control:recovery-stopped", encoded)
            .await
    }

    /// Each visible node's exact stopped-for round.
    ///
    /// # Errors
    /// Fails closed when any visible acknowledgement is malformed.
    pub async fn read_stopped(&self) -> Result<Vec<(NodeId, RecoveryRound)>, String> {
        self.read_recovery_round_map("control:recovery-stopped")
            .await
    }

    /// Clear only this driver's still-identical recovery announcement. The per-controller lock
    /// makes the read/clear conditional with respect to a concurrent local phase transition.
    ///
    /// # Errors
    /// Returns an error when the visible local announcement is malformed.
    pub async fn clear_recover(&self, round: &RecoveryRound) -> Result<bool, String> {
        let _guard = self.recovery_writes.lock().await;
        let Some(raw) = self
            .read_recovery_value(self.instance_id, "control:recover")
            .await?
        else {
            return Ok(false);
        };
        let Some(active) = parse_recovery_announcement(&raw)? else {
            return Ok(false);
        };
        if active.round != *round {
            return Ok(false);
        }
        if matches!(active.phase, RecoverPhase::Release { .. }) {
            return Ok(false);
        }
        self.write_recovery_value_exact("control:recover", String::new())
            .await?;
        Ok(true)
    }

    /// Wait until [`Self::observe_barrier`] yields an announcement matching `pred`, or `timeout`
    /// expires (→ `Ok(None)`). Observation remains side-effect free; a caller consuming event-time
    /// progress must subsequently use [`Self::accept_barrier_watermark`] after its exact identity
    /// validation. Push-driven
    /// off the gRPC announcement watch when available; gossip-KV-only
    /// deployments (and KV-only announcements) are covered by a
    /// fallback poll — 250ms with the watch, 25ms without.
    ///
    /// # Errors
    /// Returns the first observation error instead of converting a known protocol or transport
    /// failure into a timeout.
    #[cfg(feature = "cluster")]
    pub async fn wait_for_barrier<F>(
        &self,
        mut pred: F,
        timeout: Duration,
    ) -> Result<Option<BarrierAnnouncement>, String>
    where
        F: FnMut(&BarrierAnnouncement) -> bool,
    {
        let mut watch = self.barrier.announcement_watch();
        // Recomputed per iteration: when the watch sender drops
        // mid-wait, the fallback must tighten to the no-watch cadence.
        let poll_for = |watch: &Option<_>| {
            if watch.is_some() {
                Duration::from_millis(250)
            } else {
                Duration::from_millis(25)
            }
        };
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(ann) = self.observe_barrier().await? {
                if pred(&ann) {
                    return Ok(Some(ann));
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            let poll = poll_for(&watch);
            let pushed = async {
                match watch.as_mut() {
                    Some(w) => w.changed().await.is_ok(),
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                ok = pushed => {
                    if !ok {
                        // Sender gone (server shutdown) — degrade to
                        // polling instead of spinning on the error.
                        watch = None;
                    }
                }
                () = tokio::time::sleep(poll) => {}
                () = tokio::time::sleep_until(deadline) => return Ok(None),
            }
        }
    }

    /// Leader-side: poll until quorum or `deadline`.
    pub async fn wait_for_quorum(
        &self,
        prepare: &BarrierAnnouncement,
        expected: &[NodeId],
        deadline: Duration,
    ) -> QuorumOutcome {
        self.barrier
            .wait_for_quorum(prepare, expected, deadline)
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
    #[cfg(feature = "cluster")]
    use crate::cluster::control::barrier::{ANNOUNCEMENT_KEY, BARRIER_ADDR_KEY};
    use crate::cluster::discovery::{NodeMetadata, NodeState};

    struct FailedWriteKv;

    #[async_trait::async_trait]
    impl ClusterKv for FailedWriteKv {
        async fn write(&self, _key: &str, _value: String) {}

        async fn write_checked(&self, _key: &str, _value: String) -> Result<(), String> {
            Err("injected durable write failure".into())
        }

        async fn read_from(&self, _who: NodeId, _key: &str) -> Option<String> {
            None
        }

        async fn scan(&self, _key: &str) -> Vec<(NodeId, String)> {
            Vec::new()
        }
    }

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

    #[cfg(feature = "cluster")]
    #[test]
    fn checkpoint_authority_access_is_exact_and_fails_closed_when_unwired() {
        let controller = ctl(1, Vec::new());
        assert!(matches!(
            controller.checkpoint_authority(),
            Err(super::super::ClusterCheckpointAuthorityError::NotConfigured)
        ));
        let authority = Arc::new(super::super::LeaderLeaseStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            1_000,
        ));
        controller.set_leader_lease_store(Arc::clone(&authority));
        assert!(Arc::ptr_eq(
            &controller.checkpoint_authority().unwrap(),
            &authority
        ));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn checkpoint_assignment_fence_requires_the_exact_durable_leader_term() {
        use crate::checkpoint::{AssignmentDrainTransition, CheckpointParticipant};
        use crate::cluster::control::{LeaderLeaseOwner, LeaseOutcome};
        use uuid::Uuid;

        let controller = ctl(7, vec![info(1)]);
        let leader_boot = Uuid::from_u128(1);
        let fence = CheckpointAssignmentFence::from_owner_map(
            4,
            &[1, 7],
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: leader_boot,
                },
                CheckpointParticipant {
                    node_id: 7,
                    boot_incarnation: controller.recovery_incarnation(),
                },
            ],
        )
        .unwrap();
        controller.publish_checkpoint_assignment_fence(Some(fence.clone()));

        let authority = Arc::new(super::super::LeaderLeaseStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            1_000,
        ));
        let owner = LeaderLeaseOwner {
            node: NodeId(1),
            boot: leader_boot,
            process_term: 3,
        };
        let LeaseOutcome::Acquired(lease) = authority.try_acquire(&owner, 0).await.unwrap() else {
            panic!("empty test authority must be acquired");
        };
        controller.set_leader_lease_store(authority);
        let exact = lease.proof();

        assert_eq!(
            controller
                .checkpoint_assignment_fence_for_leader(4, &exact)
                .await,
            Some(fence.clone())
        );

        let mut stale_token = exact.clone();
        stale_token.fencing_token += 1;
        assert!(controller
            .checkpoint_assignment_fence_for_leader(4, &stale_token)
            .await
            .is_none());

        let mut stale_process_term = exact.clone();
        stale_process_term.owner.process_term += 1;
        assert!(controller
            .checkpoint_assignment_fence_for_leader(4, &stale_process_term)
            .await
            .is_none());

        let target = CheckpointAssignmentFence::from_owner_map(
            5,
            &[7, 7],
            vec![CheckpointParticipant {
                node_id: 7,
                boot_incarnation: controller.recovery_incarnation(),
            }],
        )
        .unwrap();
        controller.publish_checkpoint_drain_transition(Some(
            AssignmentDrainTransition::new(fence.clone(), target, exact.clone()).unwrap(),
        ));
        assert_eq!(
            controller
                .checkpoint_assignment_fence_for_leader(4, &exact)
                .await,
            Some(fence)
        );
        assert!(controller
            .checkpoint_assignment_fence_for_leader(4, &stale_token)
            .await
            .is_none());
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

    /// When a lease is wired, the gossip candidate is leader only while it holds
    /// an unexpired lease; every other leader-gated path inherits this fencing.
    #[cfg(feature = "cluster")]
    #[test]
    fn is_leader_requires_held_lease_when_wired() {
        use crate::cluster::control::{LeaderLease, LeaderLeaseOwner, LeaderProof, LeaseDeadline};
        let owner = |node, boot, process_term| LeaderLeaseOwner {
            node: NodeId(node),
            boot: Uuid::from_u128(boot),
            process_term,
        };
        let lease = |owner| LeaderLease {
            seq: 1,
            token: 1,
            owner,
            expires_at_ms: i64::MIN,
            catalog_manifest: None,
        };
        let expected = owner(1, 1, 1);
        let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(10)));

        let c = ctl(1, vec![info(5)]); // lowest id → gossip candidate
        assert!(c.is_leader(), "gossip-only leadership when no lease wired");
        assert!(!c.has_leader_lease_fencing());
        assert_eq!(c.leader_fencing_token(), None);

        let (tx, rx) = watch::channel(None);
        c.set_leader_lease_watch(rx, expected.clone(), Arc::clone(&deadline))
            .unwrap();
        assert!(c.has_leader_lease_fencing());
        assert!(!c.is_leader(), "fenced out until a lease is held");
        assert_eq!(c.leader_fencing_token(), None);

        tx.send(Some(lease(owner(2, 2, 1)))).unwrap();
        assert!(!c.is_leader(), "another node holds the lease");
        assert_eq!(c.leader_fencing_token(), None);

        tx.send(Some(lease(expected))).unwrap();
        assert!(c.is_leader(), "the exact process owns a live local lease");
        assert!(
            c.capture_leader_proof().is_none(),
            "durable proof also requires the stable-node process deadline"
        );
        c.set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(10))));
        assert_eq!(c.leader_fencing_token(), Some(1));
        let proof = c.capture_leader_proof().unwrap();
        assert_eq!(proof.fencing_token, 1);
        assert!(c.proof_is_live(&proof));

        assert!(c.is_gossip_leader());
        deadline.fence();
        assert!(!c.is_leader(), "the local monotonic lease expired");
        assert_eq!(c.leader_fencing_token(), None);
        assert!(!c.proof_is_live(&proof));

        let invalid = LeaderProof {
            owner: proof.owner,
            fencing_token: 0,
        };
        assert!(!c.proof_is_live(&invalid));
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn leader_proof_rejects_restarted_owner_and_stale_token() {
        use crate::cluster::control::{LeaderLease, LeaderLeaseOwner, LeaseDeadline};
        let owner = |boot, process_term| LeaderLeaseOwner {
            node: NodeId(1),
            boot: Uuid::from_u128(boot),
            process_term,
        };
        let lease = |token, owner| LeaderLease {
            seq: token,
            token,
            owner,
            expires_at_ms: i64::MIN,
            catalog_manifest: None,
        };
        let expected = owner(1, 7);
        let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(10)));
        let c = ctl(1, vec![info(5)]);
        c.set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(10))));
        let (tx, rx) = watch::channel(Some(lease(11, expected.clone())));
        c.set_leader_lease_watch(rx, expected.clone(), deadline)
            .unwrap();
        let mut grant_changes = c.leader_grant_watch().unwrap();

        let stale = c.capture_leader_proof().unwrap();
        assert!(c.proof_is_live(&stale));

        tx.send(Some(lease(12, expected))).unwrap();
        assert!(grant_changes.has_changed().unwrap());
        grant_changes.borrow_and_update();
        assert!(
            !c.proof_is_live(&stale),
            "a superseded token must fail closed"
        );
        assert_eq!(c.capture_leader_proof().unwrap().fencing_token, 12);

        tx.send(Some(lease(13, owner(1, 8)))).unwrap();
        assert!(grant_changes.has_changed().unwrap());
        grant_changes.borrow_and_update();
        assert!(
            c.capture_leader_proof().is_none(),
            "a newer process term on the same node and boot is a different owner"
        );

        tx.send(Some(lease(14, owner(2, 8)))).unwrap();
        assert!(grant_changes.has_changed().unwrap());
        assert!(
            c.capture_leader_proof().is_none(),
            "a new boot on the same stable node is a different owner"
        );
        assert!(!c.proof_is_live(&stale));
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
        assert_eq!(c.current_leader(), Some(NodeId(3)));
        assert!(!c.is_leader(), "a draining owner must yield leadership");
    }

    #[test]
    fn checkpoint_instances_keep_draining_owners_and_exclude_unavailable_nodes() {
        let mut draining = info(2);
        draining.state = NodeState::Draining;
        let mut joining = info(3);
        joining.state = NodeState::Joining;
        let mut suspected = info(4);
        suspected.state = NodeState::Suspected;
        let mut left = info(5);
        left.state = NodeState::Left;
        let c = ctl(1, vec![draining, joining, suspected, left, info(6)]);

        assert_eq!(
            c.checkpoint_instances(),
            vec![NodeId(1), NodeId(2), NodeId(6)]
        );
        assert_eq!(c.assignable_instances(), vec![NodeId(1), NodeId(6)]);

        c.begin_drain();
        assert!(
            c.checkpoint_instances().contains(&NodeId(1)),
            "self remains responsible for its old vnodes while draining"
        );
        assert!(!c.assignable_instances().contains(&NodeId(1)));
    }

    #[test]
    fn checkpoint_assignment_fence_is_invalidated_by_membership_change() {
        let self_id = NodeId(1);
        let (members_tx, members_rx) = watch::channel(vec![info(2)]);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
        let c = ClusterController::new(self_id, kv, None, members_rx);
        let expected = CheckpointAssignmentFence::from_owner_map(
            7,
            &[1, 2],
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: c.recovery_incarnation(),
                },
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: Uuid::new_v4(),
                },
            ],
        )
        .unwrap();
        c.publish_checkpoint_assignment_fence(Some(expected.clone()));
        assert_eq!(c.checkpoint_assignment_fence(7), Some(expected.clone()));
        assert_eq!(c.checkpoint_assignment_fence(6), None);
        c.note_unresponsive(&[NodeId(2)]);
        assert_eq!(
            c.checkpoint_assignment_fence(7),
            Some(expected),
            "node-local quorum history must not make the shared assignment proof diverge"
        );

        let mut suspected = info(2);
        suspected.state = NodeState::Suspected;
        members_tx.send(vec![suspected]).unwrap();
        assert_eq!(
            c.checkpoint_assignment_fence(7),
            None,
            "a cached fence must close immediately when a participant becomes unavailable"
        );
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
            assignment_fence: None,
            leader_proof: None,
            phase: crate::cluster::control::Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        })
        .await
        .unwrap();
        let got = c.observe_barrier().await.unwrap().unwrap();
        assert_eq!(got.epoch, 5);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn wait_for_barrier_propagates_observation_failure_immediately() {
        let c = ctl(1, vec![]);
        c.kv.write(ANNOUNCEMENT_KEY, "not-json".into()).await;

        let error = c
            .wait_for_barrier(|_| true, Duration::from_secs(10))
            .await
            .expect_err("malformed control history must fail instead of timing out");

        assert!(
            error.contains("malformed durable barrier announcement"),
            "{error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn wait_for_barrier_propagates_mixed_attempt_conflict_immediately() {
        let leader_kv = Arc::new(InMemoryKv::new(NodeId(1)));
        let follower_kv = Arc::new(InMemoryKv::new(NodeId(2)));
        let (_leader_members_tx, leader_members_rx) = watch::channel(vec![info(2)]);
        let (_follower_members_tx, follower_members_rx) = watch::channel(vec![info(1)]);
        let leader = ClusterController::new(NodeId(1), leader_kv.clone(), None, leader_members_rx);
        let follower =
            ClusterController::new(NodeId(2), follower_kv.clone(), None, follower_members_rx);
        let leader_addr = leader
            .start_barrier_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let follower_addr = follower
            .start_barrier_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        leader_kv.seed(NodeId(2), BARRIER_ADDR_KEY, follower_addr.to_string());
        follower_kv.seed(NodeId(1), BARRIER_ADDR_KEY, leader_addr.to_string());

        leader
            .announce_barrier(&BarrierAnnouncement {
                epoch: 10,
                checkpoint_id: 100,
                assignment_fence: None,
                leader_proof: None,
                phase: Phase::Abort,
                flags: 0,
                min_watermark_ms: None,
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let delivered = follower
                    .barrier
                    .announcement_watch()
                    .is_some_and(|watch| watch.borrow().is_some());
                if delivered {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("direct announcement must reach the follower before conflict injection");

        follower_kv.seed(
            NodeId(1),
            ANNOUNCEMENT_KEY,
            serde_json::to_string(&BarrierAnnouncement {
                epoch: 11,
                checkpoint_id: 99,
                assignment_fence: None,
                leader_proof: None,
                phase: Phase::Abort,
                flags: 0,
                min_watermark_ms: None,
            })
            .unwrap(),
        );
        let error = follower
            .wait_for_barrier(|_| true, Duration::from_secs(10))
            .await
            .expect_err("mixed attempt dimensions must fail instead of timing out");

        assert!(
            error.contains("conflicting direct and durable barrier attempts"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn adopted_assignment_report_binds_the_current_process_and_exact_map() {
        let c = ctl(1, vec![]);
        c.publish_recovery_incarnation().await.unwrap();
        let owners = [1, 1, 1];
        let report = CheckpointAssignmentAdoption {
            participant: CheckpointParticipant {
                node_id: 1,
                boot_incarnation: c.recovery_incarnation(),
            },
            assignment_version: 7,
            partitioning_abi_version: crate::state::PARTITIONING_ABI_VERSION,
            vnode_count: u32::try_from(owners.len()).unwrap(),
            assignment_digest: CheckpointAssignmentFence::owner_map_digest(3, &owners),
        };
        c.announce_adopted_assignment(&report).await.unwrap();
        assert_eq!(
            c.read_adopted_assignments().await.unwrap(),
            vec![(NodeId(1), report.clone())]
        );

        let mut restarted = report;
        restarted.participant.boot_incarnation = Uuid::new_v4();
        assert!(c.announce_adopted_assignment(&restarted).await.is_err());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn drain_quorum_requires_the_exact_current_boot_roster_and_certificate() {
        let self_id = NodeId(1);
        let peer_id = NodeId(2);
        let self_boot = Uuid::from_u128(11);
        let peer_boot = Uuid::from_u128(22);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let control: Arc<dyn ClusterKv> = kv.clone();
        let recovery: Arc<dyn ClusterKv> = kv.clone();
        let (_members_tx, members_rx) = watch::channel(vec![info(peer_id.0)]);
        let controller = ClusterController::new_with_recovery_incarnation(
            self_id, control, recovery, None, members_rx, self_boot,
        );
        controller.publish_recovery_incarnation().await.unwrap();
        kv.seed(peer_id, RECOVERY_INCARNATION_KEY, peer_boot.to_string());
        let participants = vec![
            CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: self_boot,
            },
            CheckpointParticipant {
                node_id: peer_id.0,
                boot_incarnation: peer_boot,
            },
        ];
        let predecessor = CheckpointAssignmentFence::from_owner_map(
            7,
            &[self_id.0, peer_id.0],
            participants.clone(),
        )
        .unwrap();
        let target = CheckpointAssignmentFence::from_owner_map(
            8,
            &[peer_id.0, self_id.0],
            participants.clone(),
        )
        .unwrap();
        let authority = Arc::new(super::super::LeaderLeaseStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            1_000,
        ));
        let owner = super::super::LeaderLeaseOwner {
            node: self_id,
            boot: self_boot,
            process_term: 1,
        };
        let super::super::LeaseOutcome::Acquired(lease) =
            authority.try_acquire(&owner, 1).await.unwrap()
        else {
            panic!("empty authority must be acquired");
        };
        controller.set_leader_lease_store(authority);
        let transition =
            AssignmentDrainTransition::new(predecessor, target, lease.proof()).unwrap();
        controller.publish_checkpoint_drain_transition(Some(transition.clone()));
        let sources =
            |candidate: &AssignmentDrainTransition, marker: u8| NodeDrainReceiptAggregate {
                round_digest: candidate.digest(),
                source_plan_digest: [marker; 32],
                receipt_count: 1,
                receipt_set_digest: [marker.saturating_add(1); 32],
            };
        controller
            .announce_drain_ack(&transition, sources(&transition, 1))
            .await
            .unwrap();
        assert!(
            !controller
                .drain_ack_quorum_reached(&transition)
                .await
                .unwrap(),
            "one acknowledgement cannot satisfy a two-process roster"
        );

        let seed_peer_ack =
            |ack: DrainAck| kv.seed(peer_id, DRAIN_ACK_KEY, encode_drain_ack(&ack).unwrap());
        seed_peer_ack(DrainAck::for_transition(
            CheckpointParticipant {
                node_id: peer_id.0,
                boot_incarnation: Uuid::from_u128(21),
            },
            &transition,
            sources(&transition, 2),
        ));
        assert!(!controller
            .drain_ack_quorum_reached(&transition)
            .await
            .unwrap());

        let stale_predecessor = CheckpointAssignmentFence::from_owner_map(
            6,
            &[self_id.0, peer_id.0],
            participants.clone(),
        )
        .unwrap();
        let stale_transition = AssignmentDrainTransition::new(
            stale_predecessor,
            transition.predecessor.clone(),
            transition.leader.clone(),
        )
        .unwrap();
        seed_peer_ack(DrainAck::for_transition(
            participants[1],
            &stale_transition,
            sources(&stale_transition, 2),
        ));
        assert!(!controller
            .drain_ack_quorum_reached(&transition)
            .await
            .unwrap());

        let future_target = CheckpointAssignmentFence::from_owner_map(
            9,
            &[self_id.0, peer_id.0],
            participants.clone(),
        )
        .unwrap();
        let future_transition = AssignmentDrainTransition::new(
            transition.target.clone(),
            future_target,
            transition.leader.clone(),
        )
        .unwrap();
        seed_peer_ack(DrainAck::for_transition(
            participants[1],
            &future_transition,
            sources(&future_transition, 2),
        ));
        assert!(!controller
            .drain_ack_quorum_reached(&transition)
            .await
            .unwrap());

        let other_target = CheckpointAssignmentFence::from_owner_map(
            transition.target.assignment_version,
            &[self_id.0, peer_id.0],
            participants.clone(),
        )
        .unwrap();
        let other_transition = AssignmentDrainTransition::new(
            transition.predecessor.clone(),
            other_target,
            transition.leader.clone(),
        )
        .unwrap();
        seed_peer_ack(DrainAck::for_transition(
            participants[1],
            &other_transition,
            sources(&other_transition, 2),
        ));
        assert!(!controller
            .drain_ack_quorum_reached(&transition)
            .await
            .unwrap());

        seed_peer_ack(DrainAck::for_transition(
            participants[1],
            &transition,
            sources(&transition, 2),
        ));
        assert!(controller
            .drain_ack_quorum_reached(&transition)
            .await
            .unwrap());
        controller
            .announce_drain_ack(&transition, sources(&transition, 1))
            .await
            .expect("an exact retry is idempotent");
        assert!(controller
            .drain_ack_quorum_reached(&transition)
            .await
            .unwrap());

        kv.seed(
            peer_id,
            RECOVERY_INCARNATION_KEY,
            Uuid::from_u128(23).to_string(),
        );
        assert!(
            !controller
                .drain_ack_quorum_reached(&transition)
                .await
                .unwrap(),
            "a restart invalidates an acknowledgement from the previous boot"
        );
    }

    #[test]
    fn drain_ack_encoding_is_canonical_and_bounded() {
        let predecessor = CheckpointAssignmentFence::from_owner_map(
            7,
            &[1],
            vec![CheckpointParticipant {
                node_id: 1,
                boot_incarnation: Uuid::from_u128(11),
            }],
        )
        .unwrap();
        let target =
            CheckpointAssignmentFence::from_owner_map(8, &[1], predecessor.participants.clone())
                .unwrap();
        let transition = AssignmentDrainTransition::new(
            predecessor.clone(),
            target,
            crate::checkpoint::LeaderProof {
                owner: crate::checkpoint::LeaderProofOwner {
                    node_id: 1,
                    boot_id: Uuid::from_u128(11),
                    process_term: 1,
                },
                fencing_token: 1,
            },
        )
        .unwrap();
        let sources = NodeDrainReceiptAggregate {
            round_digest: transition.digest(),
            source_plan_digest: [1; 32],
            receipt_count: 1,
            receipt_set_digest: [2; 32],
        };
        let ack = DrainAck::for_transition(predecessor.participants[0], &transition, sources);
        let encoded = encode_drain_ack(&ack).unwrap();
        assert!(encoded.len() <= MAX_DRAIN_ACK_BYTES);
        assert_eq!(parse_drain_ack(&encoded, NodeId(1)).unwrap(), ack);
        assert!(parse_drain_ack(&format!(" {encoded}"), NodeId(1)).is_err());
        assert!(parse_drain_ack(&"x".repeat(MAX_DRAIN_ACK_BYTES + 1), NodeId(1)).is_err());
        let mut noncanonical = ack;
        noncanonical.round.target_version += 1;
        assert!(encode_drain_ack(&noncanonical).is_err());
        assert!(
            parse_drain_ack(&serde_json::to_string(&noncanonical).unwrap(), NodeId(1)).is_err()
        );
    }

    #[test]
    fn publish_cluster_min_watermark_is_monotonic() {
        // Leader-side publish mirrors the monotonic contract the certified follower acceptance
        // path enforces through `accept_barrier_watermark`.
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
    async fn only_an_exact_accepted_barrier_publishes_cluster_min_watermark() {
        let c = ctl(1, vec![]);
        assert_eq!(c.cluster_min_watermark(), None, "uninitialised");
        #[cfg(feature = "cluster")]
        let leader_proof = {
            use crate::cluster::control::{LeaderLeaseOwner, LeaderLeaseStore, LeaseOutcome};
            use object_store::memory::InMemory;

            let store = Arc::new(LeaderLeaseStore::new(Arc::new(InMemory::new()), 1_000));
            let owner = LeaderLeaseOwner {
                node: c.instance_id(),
                boot: c.recovery_incarnation(),
                process_term: 1,
            };
            let lease = match store.try_acquire(&owner, 1).await.unwrap() {
                LeaseOutcome::Acquired(lease) => lease,
                LeaseOutcome::Held(_) => unreachable!(),
            };
            c.set_leader_lease_store(store);
            Some(lease.proof())
        };
        #[cfg(not(feature = "cluster"))]
        let leader_proof = None;
        let fence = CheckpointAssignmentFence::from_owner_map(
            7,
            &[1],
            vec![CheckpointParticipant {
                node_id: 1,
                boot_incarnation: c.recovery_incarnation(),
            }],
        )
        .unwrap();

        c.announce_barrier(&BarrierAnnouncement {
            epoch: 8,
            checkpoint_id: 1,
            assignment_fence: Some(fence.clone()),
            leader_proof,
            phase: crate::cluster::control::Phase::Aligned,
            flags: 0,
            min_watermark_ms: Some(12_000),
        })
        .await
        .unwrap();
        let aligned = c.observe_barrier().await.unwrap().unwrap();
        assert!(c.accept_barrier_watermark(&aligned, 8, 1, &fence));
        assert_eq!(
            c.cluster_min_watermark(),
            None,
            "Aligned is reversible and must not advance the recovery-safe watermark"
        );

        c.announce_barrier(&BarrierAnnouncement {
            epoch: 9,
            checkpoint_id: 1,
            assignment_fence: Some(fence.clone()),
            leader_proof: None,
            phase: crate::cluster::control::Phase::Commit,
            flags: 0,
            min_watermark_ms: Some(12_345),
        })
        .await
        .unwrap();
        let observed = c.observe_barrier().await.unwrap().unwrap();
        assert_eq!(
            c.cluster_min_watermark(),
            None,
            "observation alone must not mutate event-time state"
        );
        assert!(c.accept_barrier_watermark(&observed, 9, 1, &fence));
        assert_eq!(c.cluster_min_watermark(), Some(12_345));

        let wrong_fence = CheckpointAssignmentFence::from_owner_map(
            7,
            &[2],
            vec![CheckpointParticipant {
                node_id: 2,
                boot_incarnation: Uuid::new_v4(),
            }],
        )
        .unwrap();
        c.announce_barrier(&BarrierAnnouncement {
            epoch: 9,
            checkpoint_id: 1,
            assignment_fence: Some(wrong_fence),
            leader_proof: None,
            phase: crate::cluster::control::Phase::Commit,
            flags: 0,
            min_watermark_ms: Some(99_999),
        })
        .await
        .unwrap();
        let observed = c.observe_barrier().await.unwrap().unwrap();
        assert!(!c.accept_barrier_watermark(&observed, 9, 1, &fence));
        assert_eq!(
            c.cluster_min_watermark(),
            Some(12_345),
            "a different assignment certificate must not advance event time"
        );

        // A later Commit with a lower value must NOT regress the atomic —
        // event-time can only advance.
        c.announce_barrier(&BarrierAnnouncement {
            epoch: 10,
            checkpoint_id: 2,
            assignment_fence: Some(fence.clone()),
            leader_proof: None,
            phase: crate::cluster::control::Phase::Commit,
            flags: 0,
            min_watermark_ms: Some(100), // stale re-gossip
        })
        .await
        .unwrap();
        let observed = c.observe_barrier().await.unwrap().unwrap();
        assert!(c.accept_barrier_watermark(&observed, 10, 2, &fence));
        assert_eq!(
            c.cluster_min_watermark(),
            Some(12_345),
            "stale Commit must not lower the published watermark",
        );

        // A Prepare announcement (no min_watermark_ms carried) is a no-op.
        c.announce_barrier(&BarrierAnnouncement {
            epoch: 11,
            checkpoint_id: 3,
            assignment_fence: None,
            leader_proof: None,
            phase: crate::cluster::control::Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        })
        .await
        .unwrap();
        let observed = c.observe_barrier().await.unwrap().unwrap();
        assert!(!c.accept_barrier_watermark(&observed, 11, 3, &fence));
        assert_eq!(c.cluster_min_watermark(), Some(12_345));
    }

    fn recovery_round(
        controller: &ClusterController,
        generation: u64,
        driver: u64,
        participants: &[u64],
    ) -> RecoveryRound {
        let participant_roster = participants
            .iter()
            .map(|node_id| CheckpointParticipant {
                node_id: *node_id,
                boot_incarnation: if *node_id == controller.instance_id().0 {
                    controller.recovery_incarnation()
                } else {
                    Uuid::new_v4()
                },
            })
            .collect();
        RecoveryRound::new(
            generation,
            NodeId(driver),
            CheckpointAssignmentFence::from_owner_map(7, participants, participant_roster).unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn recovery_start_and_clear_require_the_identical_prepared_round() {
        let c = ctl(1, vec![]);
        c.publish_recovery_incarnation().await.unwrap();
        let round = recovery_round(&c, 11, 1, &[1]);
        let other = recovery_round(&c, 12, 1, &[1]);
        c.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));

        c.announce_recover_prepare(&round).await.unwrap();
        assert_eq!(
            c.observe_recover().await.unwrap().unwrap(),
            RecoveryAnnouncement {
                round: round.clone(),
                phase: RecoverPhase::Prepare,
            }
        );
        assert!(c.announce_recover_start(&other, 9).await.is_err());
        assert!(!c.clear_recover(&other).await.unwrap());

        c.announce_recover_start(&round, 9).await.unwrap();
        let start = c.observe_recover().await.unwrap().unwrap();
        assert_eq!(start.round, round);
        assert_eq!(start.phase, RecoverPhase::Start { epoch: 9 });
        assert!(c.clear_recover(&start.round).await.unwrap());
        assert!(c.observe_recover().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn stale_noncurrent_recovery_slot_does_not_mask_the_current_driver() {
        let self_id = NodeId(2);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let mut old_driver = info(1);
        old_driver.state = NodeState::Suspected;
        let (_tx, rx) = watch::channel(vec![old_driver]);
        let c = ClusterController::new(self_id, kv.clone(), None, rx);
        c.publish_recovery_incarnation().await.unwrap();
        let local = recovery_round(&c, 17, 2, &[2]);
        let stale = recovery_round(&c, 99, 1, &[1, 2]);
        c.publish_checkpoint_assignment_fence(Some(local.assignment_fence.clone()));
        c.announce_recover_prepare(&local).await.unwrap();
        kv.seed(
            NodeId(1),
            "control:recover",
            serde_json::to_string(&RecoveryAnnouncement {
                round: stale,
                phase: RecoverPhase::Start { epoch: 9 },
            })
            .unwrap(),
        );

        assert_eq!(
            c.observe_recover().await.unwrap(),
            Some(RecoveryAnnouncement {
                round: local,
                phase: RecoverPhase::Prepare,
            })
        );
    }

    #[tokio::test]
    async fn malformed_current_driver_recovery_slot_fails_closed() {
        let self_id = NodeId(2);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let (_tx, rx) = watch::channel(Vec::new());
        let c = ClusterController::new(self_id, kv.clone(), None, rx);
        kv.seed(self_id, "control:recover", "not-json".into());

        let error = c.observe_recover().await.unwrap_err();
        assert!(error.contains("invalid recovery announcement"), "{error}");
    }

    #[tokio::test]
    async fn recovered_ack_binds_the_start_target() {
        let c = ctl(1, vec![]);
        c.publish_recovery_incarnation().await.unwrap();
        let round = recovery_round(&c, 21, 1, &[1]);
        let start = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Start { epoch: 4 },
        };
        c.announce_recovered(&start).await.unwrap();
        assert_eq!(c.read_recovered().await.unwrap(), vec![(NodeId(1), start)]);
        assert!(c
            .announce_recovered(&RecoveryAnnouncement {
                round,
                phase: RecoverPhase::Prepare,
            })
            .await
            .is_err());
    }

    #[tokio::test]
    async fn release_is_terminal_and_cannot_be_cleared() {
        let c = ctl(1, vec![]);
        c.publish_recovery_incarnation().await.unwrap();
        let round = recovery_round(&c, 31, 1, &[1]);
        c.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
        c.announce_recover_prepare(&round).await.unwrap();
        c.announce_recover_start(&round, 8).await.unwrap();

        c.announce_recover_release(&round, 8).await.unwrap();
        let release = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Release { epoch: 8 },
        };
        assert_eq!(c.observe_recover().await.unwrap(), Some(release.clone()));
        assert!(!c.clear_recover(&round).await.unwrap());
        c.announce_released(&release).await.unwrap();
        assert_eq!(c.read_released().await.unwrap(), vec![(NodeId(1), release)]);
    }

    #[tokio::test]
    async fn durable_recovery_state_survives_fast_kv_reconstruction() {
        let node = NodeId(1);
        let fast = Arc::new(InMemoryKv::new(node));
        let durable = Arc::new(InMemoryKv::new(node));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = ClusterController::new_with_recovery_kv(
            node,
            fast.clone(),
            durable.clone(),
            None,
            members_rx,
        );
        controller.publish_recovery_incarnation().await.unwrap();
        let round = recovery_round(&controller, 51, 1, &[1]);
        controller.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
        controller.adopt_recovery_generation(51).await.unwrap();
        controller.announce_recover_prepare(&round).await.unwrap();
        controller.announce_recover_start(&round, 13).await.unwrap();
        controller
            .announce_recover_release(&round, 13)
            .await
            .unwrap();

        assert!(fast.read_from(node, "control:recover").await.is_none());
        drop(controller);

        let replacement_fast = Arc::new(InMemoryKv::new(node));
        let (_replacement_tx, replacement_rx) = watch::channel(Vec::new());
        let replacement = ClusterController::new_with_recovery_kv(
            node,
            replacement_fast,
            durable,
            None,
            replacement_rx,
        );
        assert_eq!(replacement.max_recovery_generation().await.unwrap(), 51);
        assert_eq!(
            replacement.observe_recover().await.unwrap(),
            Some(RecoveryAnnouncement {
                round,
                phase: RecoverPhase::Release { epoch: 13 },
            })
        );
    }

    #[tokio::test]
    async fn durable_recovery_write_failure_is_returned() {
        let node = NodeId(1);
        let fast: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
        let durable: Arc<dyn ClusterKv> = Arc::new(FailedWriteKv);
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller =
            ClusterController::new_with_recovery_kv(node, fast, durable, None, members_rx);

        let error = controller.publish_recovery_incarnation().await.unwrap_err();
        assert!(error.contains("injected durable write failure"), "{error}");
    }

    #[tokio::test]
    async fn superseded_same_id_process_cannot_ack_an_old_round() {
        let node = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(node));
        let (_old_tx, old_rx) = watch::channel(Vec::new());
        let old = ClusterController::new(node, kv.clone(), None, old_rx);
        old.publish_recovery_incarnation().await.unwrap();
        let old_round = recovery_round(&old, 41, 1, &[1]);

        let (_new_tx, new_rx) = watch::channel(Vec::new());
        let replacement = ClusterController::new(node, kv, None, new_rx);
        replacement.publish_recovery_incarnation().await.unwrap();

        let error = old.announce_stopped(&old_round).await.unwrap_err();
        assert!(error.contains("superseded local process"), "{error}");
    }
}
