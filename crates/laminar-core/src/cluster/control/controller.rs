//! Facade over `ClusterKv` + `BarrierCoordinator` + membership watch.
//! `None` on `CheckpointCoordinator` means single-instance mode.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use sha2::{Digest as _, Sha256};
use tokio::sync::watch;
use uuid::Uuid;

use super::barrier::{
    BarrierAck, BarrierAnnouncement, BarrierCoordinator, ClusterKv, Phase, QuorumOutcome,
};
use super::leader::leader_of;
use super::snapshot::AssignmentSnapshotStore;
use crate::checkpoint::{
    AssignmentDrainId, AssignmentDrainTransition, CheckpointAssignmentAdoption,
    CheckpointAssignmentFence, CheckpointParticipant, LeaderProof,
};
use crate::cluster::discovery::{assignable_node_ids, NodeId, NodeInfo, NodeState};
use crate::state::{CheckpointAttempt, Locality};

const RECOVERY_INCARNATION_KEY: &str = "control:recovery-incarnation";
const DRAIN_ACK_KEY: &str = "control:drain-ack";
const DRAIN_ACK_PROTOCOL_VERSION: u16 = 1;
const RELEASE_READY_ACK_KEY: &str = "control:recovery-release-ready";
const RELEASE_READY_PROTOCOL_VERSION: u16 = 1;
const RECOVERY_CONTROL_IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_DRAIN_ACK_BYTES: usize = 1_024;
const MAX_RELEASE_READY_ACK_BYTES: usize = 1_024;
const CONTROL_ROSTER_IO_CONCURRENCY: usize = 32;

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

/// Frozen recovery quorum, assignment certificate, and durable driver term.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryRound {
    /// Unique round identity.
    pub id: RecoveryRoundId,
    /// Exact durable leader term that created and may advance this round.
    pub leader_proof: LeaderProof,
    /// Exact owner-complete assignment cut from which the quorum was frozen.
    pub assignment_fence: CheckpointAssignmentFence,
    /// Canonical nonzero fault reports covered by this round's terminal `Release`.
    pub faults: Vec<RecoveryFault>,
}

/// One durable fault report covered by a coordinated recovery round.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryFault {
    /// Stable node slot that published the report.
    pub reporter: NodeId,
    /// Nonzero boot-bound report sequence observed by the recovery driver.
    pub sequence: u64,
}

impl RecoveryRound {
    /// Construct a fresh uniquely identified recovery round.
    ///
    /// # Errors
    /// Returns an error when the generation, leader proof, or assignment certificate is invalid.
    pub fn new(
        generation: u64,
        leader_proof: LeaderProof,
        assignment_fence: CheckpointAssignmentFence,
        faults: Vec<RecoveryFault>,
    ) -> Result<Self, String> {
        let driver = NodeId(leader_proof.owner.node_id);
        let round = Self {
            id: RecoveryRoundId {
                generation,
                nonce: Uuid::new_v4(),
                driver,
            },
            leader_proof,
            assignment_fence,
            faults,
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

    /// Exact fault sequence this round covers for `node`.
    #[must_use]
    pub fn fault_sequence(&self, node: NodeId) -> Option<u64> {
        self.faults
            .binary_search_by_key(&node, |fault| fault.reporter)
            .ok()
            .map(|index| self.faults[index].sequence)
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
        if !self.leader_proof.is_canonical() || self.id.driver.0 != self.leader_proof.owner.node_id
        {
            return Err("recovery driver must match a canonical leader proof".into());
        }
        if !self.assignment_fence.is_canonical() {
            return Err("recovery assignment certificate is not canonical".into());
        }
        if !self.contains(self.id.driver) {
            return Err("recovery driver is absent from the frozen quorum".into());
        }
        if self.participant_incarnation(self.id.driver) != Some(self.leader_proof.owner.boot_id) {
            return Err("recovery leader proof is not bound to the frozen driver process".into());
        }
        if self.faults.is_empty()
            || self
                .faults
                .iter()
                .any(|fault| fault.reporter.is_unassigned() || fault.sequence == 0)
            || self
                .faults
                .windows(2)
                .any(|pair| pair[0].reporter >= pair[1].reporter)
        {
            return Err("recovery fault set is not canonical".into());
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
    /// Every restored owner must prepare its local transport authority and publish readiness while
    /// source gates remain closed.
    Release {
        /// The identical rewind target carried by `Start`.
        epoch: u64,
    },
    /// Leader-fenced terminal decision that every exact owner was ready for the release intent.
    ReleaseCommitted {
        /// The identical rewind target carried by `Start` and `Release`.
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
    pub(crate) fn validate(&self) -> Result<(), String> {
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryReleaseId {
    protocol_version: u16,
    generation: u64,
    nonce: Uuid,
    driver: NodeId,
    epoch: u64,
    round_sha256: String,
}

#[derive(serde::Serialize)]
struct RecoveryReleaseDigestInput<'a> {
    protocol: &'static str,
    round: &'a RecoveryRound,
    epoch: u64,
}

impl RecoveryReleaseId {
    pub(crate) fn for_pending(release: &RecoveryAnnouncement) -> Result<Self, String> {
        let RecoverPhase::Release { epoch } = release.phase else {
            return Err("release readiness must bind a pending Release target".into());
        };
        release.validate()?;
        let encoded = serde_json::to_vec(&RecoveryReleaseDigestInput {
            protocol: "laminardb-recovery-release-v1",
            round: &release.round,
            epoch,
        })
        .map_err(|error| format!("could not encode recovery release identity: {error}"))?;
        Ok(Self {
            protocol_version: RELEASE_READY_PROTOCOL_VERSION,
            generation: release.round.id.generation,
            nonce: release.round.id.nonce,
            driver: release.round.id.driver,
            epoch,
            round_sha256: sha256_hex(&encoded),
        })
    }

    pub(crate) fn is_canonical(&self) -> bool {
        self.protocol_version == RELEASE_READY_PROTOCOL_VERSION
            && self.generation != 0
            && !self.nonce.is_nil()
            && !self.driver.is_unassigned()
            && is_sha256_hex(&self.round_sha256)
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryReleaseReadyAck {
    release: RecoveryReleaseId,
    participant: CheckpointParticipant,
}

impl RecoveryReleaseReadyAck {
    fn is_canonical(&self) -> bool {
        self.release.is_canonical()
            && self.participant.node_id != 0
            && !self.participant.boot_incarnation.is_nil()
    }
}

/// Classified recovery-control failure. Only [`Self::Uncertain`] is retryable.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RecoveryControlError {
    /// A bounded durable operation has an unknown outcome.
    #[error("recovery control outcome is uncertain: {0}")]
    Uncertain(String),
    /// Durable state is malformed or contradicts the exact protocol identity.
    #[error("recovery control conflict: {0}")]
    Conflict(String),
    /// A newer process, round, fault set, or leader term replaced this operation.
    #[error("recovery round was superseded: {0}")]
    Superseded(String),
}

impl RecoveryControlError {
    #[cfg(feature = "cluster")]
    fn from_authority(error: super::ClusterCheckpointAuthorityError) -> Self {
        match error {
            super::ClusterCheckpointAuthorityError::Authority(super::LeaseError::Io(reason)) => {
                Self::Uncertain(reason)
            }
            super::ClusterCheckpointAuthorityError::Fenced => {
                Self::Superseded("durable leader authority changed".into())
            }
            error => Self::Conflict(error.to_string()),
        }
    }
}

/// Exact leader-side observation of the compact release-readiness roster.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReleaseReadyStatus {
    /// At least one frozen owner has not published readiness for this release intent.
    Pending {
        /// Exact owner slots still missing a matching record.
        missing: Vec<NodeId>,
    },
    /// Every frozen owner published the exact release identity.
    Complete,
}

/// One bounded leader attempt to commit a pending recovery Release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseCommitStatus {
    /// At least one frozen owner has not prepared this exact release intent.
    Pending {
        /// Exact owner slots still missing a matching readiness record.
        missing: Vec<NodeId>,
    },
    /// The leader durably admitted the immutable terminal into shared authority.
    Committed {
        /// Exact terminal record followers resolve from shared authority across takeover.
        terminal: RecoveryAnnouncement,
    },
}

/// Held after this process conditionally consumes a committed recovery fault and until its local
/// source gate transition is complete. A concurrent fault report cannot cross that transition.
#[must_use = "keep the release guard until local source intake is opened or definitively fenced"]
pub struct RecoveryReleaseGuard<'a> {
    _write_guard: tokio::sync::MutexGuard<'a, ()>,
}

impl std::fmt::Debug for RecoveryReleaseGuard<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RecoveryReleaseGuard")
            .finish_non_exhaustive()
    }
}

fn sha256_hex(encoded: &[u8]) -> String {
    let digest = Sha256::digest(encoded);
    let mut hex = String::with_capacity(64);
    use std::fmt::Write as _;
    for byte in digest {
        write!(&mut hex, "{byte:02x}").expect("writing to a String cannot fail");
    }
    hex
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Bounded durable proof that one exact predecessor process reached the global source frontier
/// for an assignment handoff. The round binds the version, vnode-owner map, and complete
/// boot-incarnation roster; a version alone is not an assignment identity.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DrainAck {
    protocol_version: u16,
    participant: CheckpointParticipant,
    round: AssignmentDrainId,
}

impl DrainAck {
    fn for_transition(
        participant: CheckpointParticipant,
        transition: &AssignmentDrainTransition,
    ) -> Self {
        Self {
            protocol_version: DRAIN_ACK_PROTOCOL_VERSION,
            participant,
            round: transition.id(),
        }
    }

    fn is_canonical(&self) -> bool {
        self.protocol_version == DRAIN_ACK_PROTOCOL_VERSION
            && self.participant.node_id != 0
            && !self.participant.boot_incarnation.is_nil()
            && self.round.is_canonical()
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

fn encode_release_ready_ack(ack: &RecoveryReleaseReadyAck) -> Result<String, String> {
    if !ack.is_canonical() {
        return Err("release readiness acknowledgement is not canonical".into());
    }
    let encoded = serde_json::to_string(ack)
        .map_err(|error| format!("could not encode release readiness: {error}"))?;
    if encoded.len() > MAX_RELEASE_READY_ACK_BYTES {
        return Err(format!(
            "release readiness is {} bytes; maximum is {MAX_RELEASE_READY_ACK_BYTES}",
            encoded.len()
        ));
    }
    Ok(encoded)
}

fn parse_release_ready_ack(
    raw: &str,
    publisher: NodeId,
) -> Result<RecoveryReleaseReadyAck, String> {
    if raw.len() > MAX_RELEASE_READY_ACK_BYTES {
        return Err(format!(
            "release readiness from {publisher} is {} bytes; maximum is {MAX_RELEASE_READY_ACK_BYTES}",
            raw.len()
        ));
    }
    let ack: RecoveryReleaseReadyAck = serde_json::from_str(raw)
        .map_err(|error| format!("invalid release readiness from {publisher}: {error}"))?;
    if !ack.is_canonical() || ack.participant.node_id != publisher.0 {
        return Err(format!(
            "release readiness from {publisher} has a non-canonical publisher"
        ));
    }
    if encode_release_ready_ack(&ack)? != raw {
        return Err(format!(
            "release readiness from {publisher} is not canonically encoded"
        ));
    }
    Ok(ack)
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
    /// Shared durable authority used to prove an unresponsive process term was revoked.
    process_lease_authority: std::sync::OnceLock<Arc<super::process_lease::ProcessLeaseAuthority>>,
    /// Whether this node may be elected leader. A certified leader may retain coordination while
    /// draining so it can checkpoint its own predecessor cut.
    leader_eligible: Arc<AtomicBool>,
    /// Wakes the leader candidacy relay when local eligibility changes.
    leader_eligibility_changes: watch::Sender<bool>,
    /// Last certified assignment roster. Retained across transient certificate suspension so an
    /// ownerless worker cannot displace an available data owner. If every certified owner is
    /// unavailable, a durably lease-fenced idle worker may lead placement repair only.
    leadership_participants: parking_lot::RwLock<Option<(u64, Vec<u64>)>>,
    /// Serialises this node's recovery and fault-slot conditional writes.
    recovery_writes: tokio::sync::Mutex<()>,
    /// Exact process incarnations that missed a capture quorum, keyed by stable node id. Entries
    /// remain quarantined until that process acknowledges or a different lease-bound boot appears;
    /// elapsed time is not evidence that a stalled owner became safe.
    unresponsive: Arc<parking_lot::Mutex<rustc_hash::FxHashMap<u64, Option<Uuid>>>>,
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
            process_lease_authority: std::sync::OnceLock::new(),
            leader_eligible,
            leader_eligibility_changes: watch::channel(false).0,
            leadership_participants: parking_lot::RwLock::new(None),
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

    /// Serve only leader proofs that remain live on this process's monotonic lease gate.
    #[cfg(feature = "cluster")]
    pub fn install_local_leader_proof_provider(self: &Arc<Self>) {
        let controller = Arc::downgrade(self);
        self.barrier
            .set_local_leader_proof_provider(Arc::new(move || {
                controller
                    .upgrade()
                    .and_then(|controller| controller.capture_leader_proof())
            }));
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

    /// Current leader: the lowest available active certified participant. A locally certified
    /// incumbent may retain candidacy while it drains, but a remote draining process is never
    /// nominated. When no certified participant is available, only a controller wired to durable
    /// leader fencing may nominate the lowest idle worker so it can repair assignment authority.
    #[must_use]
    pub fn current_leader(&self) -> Option<NodeId> {
        let members = self.members_rx.borrow();
        let participants = self.leadership_participants.read();
        let unresponsive = self.unresponsive.lock();
        let is_participant = |node: NodeId| {
            participants
                .as_ref()
                .is_none_or(|(_, roster)| roster.binary_search(&node.0).is_ok())
        };
        let mut eligible: Vec<NodeId> = members
            .iter()
            .filter(|m| {
                m.id != self.instance_id
                    && matches!(m.state, NodeState::Active)
                    && !unresponsive.contains_key(&m.id.0)
            })
            .map(|m| m.id)
            .collect();
        if self.leader_eligible.load(Ordering::SeqCst)
            && self.process_lease_is_live()
            && !unresponsive.contains_key(&self.instance_id.0)
        {
            eligible.push(self.instance_id);
        }
        let certified = eligible
            .iter()
            .copied()
            .filter(|node| is_participant(*node))
            .collect::<Vec<_>>();
        if let Some(leader) = leader_of(&certified) {
            return Some(leader);
        }
        #[cfg(feature = "cluster")]
        if self.has_leader_lease_fencing() {
            return leader_of(&eligible);
        }
        None
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

    /// Install the shared stable-node fencing authority before recovery starts.
    ///
    /// # Errors
    /// Rejects replacing an already-installed authority.
    pub fn set_process_lease_authority(
        &self,
        authority: Arc<super::process_lease::ProcessLeaseAuthority>,
    ) -> Result<(), String> {
        self.process_lease_authority
            .set(authority)
            .map_err(|_| "process lease authority is already installed".to_string())
    }

    /// Prove that an exact assignment participant was durably superseded within `deadline`.
    ///
    /// # Errors
    /// Fails closed when no shared authority is installed or fencing cannot be proven in time.
    pub async fn fence_process_incarnation(
        &self,
        participant: CheckpointParticipant,
        deadline: tokio::time::Instant,
    ) -> Result<super::process_lease::ProcessLeaseFence, String> {
        self.process_lease_authority
            .get()
            .ok_or_else(|| "process lease authority is not installed".to_string())?
            .fence_incarnation(participant, deadline)
            .await
            .map_err(|error| error.to_string())
    }

    /// Bound process fencing by the authority TTL plus the caller's existing control-plane I/O
    /// budget, avoiding a second configurable timeout dimension.
    ///
    /// # Errors
    /// Fails when no shared authority is installed or the deadline cannot be represented.
    pub fn process_fencing_deadline(
        &self,
        io_budget: Duration,
    ) -> Result<tokio::time::Instant, String> {
        self.process_lease_authority
            .get()
            .ok_or_else(|| "process lease authority is not installed".to_string())?
            .fencing_deadline(io_budget)
            .map_err(|error| error.to_string())
    }

    /// Verify one successor roster identity against the current durable process-lease head.
    ///
    /// # Errors
    /// Fails when no authority is installed or the bounded durable read is uncertain.
    pub async fn verify_current_process_incarnation(
        &self,
        participant: CheckpointParticipant,
        deadline: tokio::time::Instant,
    ) -> Result<bool, String> {
        self.process_lease_authority
            .get()
            .ok_or_else(|| "process lease authority is not installed".to_string())?
            .verify_current_participant(participant, deadline)
            .await
            .map_err(|error| error.to_string())
    }

    /// Re-read the exact durable records behind a process fence within `deadline`.
    ///
    /// # Errors
    /// Fails closed when no shared authority is installed or verification cannot finish in time.
    pub async fn verify_process_lease_fence(
        &self,
        fence: &super::process_lease::ProcessLeaseFence,
        deadline: tokio::time::Instant,
    ) -> Result<bool, String> {
        self.process_lease_authority
            .get()
            .ok_or_else(|| "process lease authority is not installed".to_string())?
            .verify_fence(fence, deadline)
            .await
            .map_err(|error| error.to_string())
    }

    /// Admit a recovery assignment only after re-verifying every durable process revocation.
    ///
    /// Keeping this operation on the controller binds the process-lease authority and leader
    /// authority installed for this runtime. Callers cannot bypass revocation verification by
    /// writing a structurally valid recovery decision directly to the leader store.
    ///
    /// # Errors
    /// Fails closed when either authority is absent, a process fence is not durable, the deadline
    /// expires, leadership changes, the decision conflicts, or storage is unavailable.
    pub async fn record_assignment_recovery_decision(
        &self,
        proof: &super::LeaderProof,
        decision: super::AssignmentRecoveryDecision,
        deadline: tokio::time::Instant,
    ) -> Result<super::RecordAssignmentRecoveryDecisionResult, String> {
        let process_authority = Arc::clone(
            self.process_lease_authority
                .get()
                .ok_or_else(|| "process lease authority is not installed".to_string())?,
        );
        let checks = futures::stream::iter(decision.removed_process_fences.iter().cloned())
            .map(|fence| {
                let process_authority = Arc::clone(&process_authority);
                async move { process_authority.verify_fence(&fence, deadline).await }
            })
            .buffer_unordered(CONTROL_ROSTER_IO_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        for check in checks {
            match check {
                Ok(true) => {}
                Ok(false) => {
                    return Err("assignment recovery contains an unverified process fence".into());
                }
                Err(error) => {
                    return Err(format!(
                        "assignment recovery process fence verification failed: {error}"
                    ));
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("assignment recovery authority admission exceeded its deadline".into());
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|error| error.to_string())?;
        tokio::time::timeout_at(
            deadline,
            authority.record_assignment_recovery_decision(proof, decision),
        )
        .await
        .map_err(|_| "assignment recovery authority admission exceeded its deadline".to_string())?
        .map_err(|error| error.to_string())
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
        let fence = self.checkpoint_assignment_fence.borrow().clone();
        let mut map = self.unresponsive.lock();
        let mut changed = false;
        for p in peers {
            let incarnation = fence
                .as_ref()
                .and_then(|certificate| certificate.participant_incarnation(p.0));
            changed |= map.insert(p.0, incarnation) != Some(incarnation);
        }
        drop(map);
        if changed {
            self.notify_leader_eligibility_change();
        }
    }

    /// Clear peers that acked (they are demonstrably alive).
    pub fn note_responsive(&self, peers: &[NodeId]) {
        let mut map = self.unresponsive.lock();
        let mut changed = false;
        for p in peers {
            changed |= map.remove(&p.0).is_some();
        }
        drop(map);
        if changed {
            self.notify_leader_eligibility_change();
        }
    }

    /// Whether `peer` has an unresolved capture-quorum failure.
    #[must_use]
    pub fn is_unresponsive(&self, peer: NodeId) -> bool {
        self.unresponsive.lock().contains_key(&peer.0)
    }

    /// Admit a placement candidate only when it was never quarantined or it is a different boot
    /// incarnation. Callers must obtain `participant` from the lease-validated durable control KV.
    pub fn admit_successor_process(&self, participant: CheckpointParticipant) -> bool {
        let mut unresponsive = self.unresponsive.lock();
        let admitted = match unresponsive.get(&participant.node_id).copied() {
            None => true,
            Some(Some(failed_boot)) if failed_boot != participant.boot_incarnation => {
                unresponsive.remove(&participant.node_id);
                drop(unresponsive);
                self.notify_leader_eligibility_change();
                return true;
            }
            Some(Some(_) | None) => false,
        };
        drop(unresponsive);
        admitted
    }

    /// Mark this node as draining. Returns whether this exact certified leader must retain its
    /// lease long enough to coordinate the predecessor cut. Idempotent.
    pub fn begin_drain(&self) -> bool {
        #[cfg(feature = "cluster")]
        let has_durable_leadership = self.has_leader_lease_fencing();
        #[cfg(not(feature = "cluster"))]
        let has_durable_leadership = false;
        let retain_leadership = has_durable_leadership
            && self.is_leader()
            && self
                .checkpoint_assignment_fence
                .borrow()
                .as_ref()
                .is_some_and(|fence| {
                    fence.is_canonical()
                        && fence.participant_incarnation(self.instance_id.0)
                            == Some(self.recovery_incarnation)
                });
        self.draining.store(true, Ordering::SeqCst);
        if !retain_leadership && self.leader_eligible.swap(false, Ordering::SeqCst) {
            self.notify_leader_eligibility_change();
        }
        retain_leadership
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
        let available = self
            .available_recovery_participant_incarnations(participants)
            .await?;
        if available.len() != participants.len() {
            let available_ids: std::collections::BTreeSet<u64> = available
                .iter()
                .map(|participant| participant.node_id)
                .collect();
            let missing = participants
                .iter()
                .find(|node_id| !available_ids.contains(node_id))
                .copied()
                .unwrap_or(0);
            return Err(format!(
                "node {missing} has no current recovery incarnation"
            ));
        }
        Ok(available)
    }

    /// Resolve every currently readable boot identity among a canonical candidate set. Missing
    /// candidates are omitted so placement can exclude lease-revoked or not-yet-started nodes
    /// without weakening exact checkpoint/recovery roster validation.
    ///
    /// # Errors
    /// Fails closed on malformed input, a durable scan error, or a malformed/duplicate identity.
    pub async fn available_recovery_participant_incarnations(
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
        Ok(participants
            .iter()
            .filter_map(|node_id| {
                reported
                    .get(node_id)
                    .copied()
                    .map(|incarnation| CheckpointParticipant {
                        node_id: *node_id,
                        boot_incarnation: incarnation,
                    })
            })
            .collect())
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

    async fn recovery_incarnations_match_control(
        &self,
        round: &RecoveryRound,
    ) -> Result<bool, RecoveryControlError> {
        let participants = round.assignment_fence.participant_ids();
        let expected: std::collections::BTreeSet<u64> = participants.iter().copied().collect();
        let mut reported = std::collections::BTreeMap::new();
        for (node, raw) in self
            .scan_recovery_values(RECOVERY_INCARNATION_KEY)
            .await
            .map_err(RecoveryControlError::Uncertain)?
        {
            if !expected.contains(&node.0) {
                continue;
            }
            let incarnation = Uuid::parse_str(&raw).map_err(|error| {
                RecoveryControlError::Conflict(format!(
                    "invalid recovery incarnation published by {node}: {error}"
                ))
            })?;
            if incarnation.is_nil() || reported.insert(node.0, incarnation).is_some() {
                return Err(RecoveryControlError::Conflict(format!(
                    "noncanonical recovery incarnation published by {node}"
                )));
            }
        }
        if reported.len() != participants.len() {
            return Ok(false);
        }
        Ok(round
            .assignment_fence
            .participants
            .iter()
            .all(|participant| {
                reported.get(&participant.node_id) == Some(&participant.boot_incarnation)
            }))
    }

    /// Publish this node's fault sequence so the leader drives a recovery round.
    ///
    /// # Errors
    /// Fails when the bounded write/read-back does not persist the exact sequence.
    pub async fn report_fault(&self, seq: u64) -> Result<(), String> {
        if seq == 0 {
            return Err("fault sequence must be nonzero".into());
        }
        let _guard = self.recovery_writes.lock().await;
        self.write_recovery_value_exact("control:fault-report", seq.to_string())
            .await
    }

    /// Clear this node's fault report only while it is still `expected`.
    ///
    /// `false` means a newer nonzero report replaced the released sequence. A missing or already
    /// cleared report is an idempotent success. The local write mutex excludes `report_fault`
    /// between the comparison and clear because `ClusterKv` has no compare-and-swap operation.
    ///
    /// # Errors
    /// Returns an error for zero `expected`, malformed local state, or a failed bounded I/O.
    pub async fn clear_fault_report(&self, expected: u64) -> Result<bool, String> {
        self.begin_recovery_release(Some(expected))
            .await
            .map(|guard| guard.is_some())
            .map_err(|error| error.to_string())
    }

    /// Conditionally consume this process's released fault while retaining the local fault-write
    /// fence. The caller must hold the returned guard through its source-gate transition.
    ///
    /// `Ok(None)` means a nonzero local report does not match the committed round. Missing and
    /// already-cleared slots are idempotent success for a round that expected a fault.
    ///
    /// # Errors
    /// Returns an error for a zero expected sequence, malformed state, or failed durable I/O.
    pub async fn begin_recovery_release(
        &self,
        expected: Option<u64>,
    ) -> Result<Option<RecoveryReleaseGuard<'_>>, RecoveryControlError> {
        if expected == Some(0) {
            return Err(RecoveryControlError::Conflict(
                "expected fault sequence must be nonzero".into(),
            ));
        }
        let guard = self.recovery_writes.lock().await;
        let Some(raw) = self
            .read_recovery_value(self.instance_id, "control:fault-report")
            .await
            .map_err(RecoveryControlError::Uncertain)?
        else {
            return Ok(Some(RecoveryReleaseGuard {
                _write_guard: guard,
            }));
        };
        let current = raw.parse::<u64>().map_err(|error| {
            RecoveryControlError::Conflict(format!("invalid local fault sequence: {error}"))
        })?;
        if current == 0 {
            return Ok(Some(RecoveryReleaseGuard {
                _write_guard: guard,
            }));
        }
        if Some(current) != expected {
            return Ok(None);
        }
        self.write_recovery_value_exact("control:fault-report", "0".into())
            .await
            .map_err(RecoveryControlError::Uncertain)?;
        Ok(Some(RecoveryReleaseGuard {
            _write_guard: guard,
        }))
    }

    /// This process's durable fault sequence, if its slot contains a nonzero report.
    ///
    /// # Errors
    /// Returns an error when the point read fails or the local slot is malformed.
    pub async fn read_local_fault_report_control(
        &self,
    ) -> Result<Option<u64>, RecoveryControlError> {
        let Some(raw) = self
            .read_recovery_value(self.instance_id, "control:fault-report")
            .await
            .map_err(RecoveryControlError::Uncertain)?
        else {
            return Ok(None);
        };
        let sequence = raw.parse::<u64>().map_err(|error| {
            RecoveryControlError::Conflict(format!("invalid local fault sequence: {error}"))
        })?;
        Ok((sequence != 0).then_some(sequence))
    }

    /// This process's durable nonzero fault report with a display-stable error.
    ///
    /// # Errors
    /// Returns an error when the point read fails or the local slot is malformed.
    pub async fn read_local_fault_report(&self) -> Result<Option<u64>, String> {
        self.read_local_fault_report_control()
            .await
            .map_err(|error| error.to_string())
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

    async fn read_fault_reports_control(&self) -> Result<Vec<(NodeId, u64)>, RecoveryControlError> {
        self.scan_recovery_values("control:fault-report")
            .await
            .map_err(RecoveryControlError::Uncertain)?
            .into_iter()
            .map(|(node, raw)| {
                raw.parse::<u64>()
                    .map(|sequence| (node, sequence))
                    .map_err(|error| {
                        RecoveryControlError::Conflict(format!(
                            "invalid fault sequence from {node}: {error}"
                        ))
                    })
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

    /// Publish that this exact process is prepared for the pending release intent.
    ///
    /// Readiness is published only after local recovery state, shuffle loss accounting, and
    /// assignment transport authority are installed while source intake remains closed.
    ///
    /// # Errors
    /// Returns an error for a non-Release phase, changed local fault, or stale process.
    pub async fn announce_release_ready(
        &self,
        release: &RecoveryAnnouncement,
    ) -> Result<(), RecoveryControlError> {
        let release_id =
            RecoveryReleaseId::for_pending(release).map_err(RecoveryControlError::Conflict)?;
        if release.round.participant_incarnation(self.instance_id)
            != Some(self.recovery_incarnation)
        {
            return Err(RecoveryControlError::Superseded(
                "release readiness has a stale local process incarnation".into(),
            ));
        }
        if !self
            .recovery_incarnation_is_current()
            .await
            .map_err(RecoveryControlError::Uncertain)?
        {
            return Err(RecoveryControlError::Superseded(
                "release readiness came from a superseded local process".into(),
            ));
        }
        if self.read_local_fault_report_control().await?
            != release.round.fault_sequence(self.instance_id)
        {
            return Err(RecoveryControlError::Superseded(
                "local fault set changed before release readiness".into(),
            ));
        }
        match self.observe_recover_control().await? {
            Some(active) if active == *release => {}
            _ => {
                return Err(RecoveryControlError::Superseded(
                    "release readiness no longer matches the active intent".into(),
                ));
            }
        }
        let participant = CheckpointParticipant {
            node_id: self.instance_id.0,
            boot_incarnation: self.recovery_incarnation,
        };
        let encoded = encode_release_ready_ack(&RecoveryReleaseReadyAck {
            release: release_id,
            participant,
        })
        .map_err(RecoveryControlError::Conflict)?;
        self.write_recovery_value_exact(RELEASE_READY_ACK_KEY, encoded)
            .await
            .map_err(RecoveryControlError::Uncertain)
    }

    /// Point-read the exact frozen owner roster's compact readiness records.
    ///
    /// Unrelated visible nodes are never scanned. Older records count as missing; malformed,
    /// same-generation divergent, or newer records are explicit conflicts.
    ///
    /// # Errors
    /// Returns an error only when an exact point read is transport-uncertain.
    async fn read_release_ready(
        &self,
        release: &RecoveryAnnouncement,
    ) -> Result<ReleaseReadyStatus, RecoveryControlError> {
        let expected =
            RecoveryReleaseId::for_pending(release).map_err(RecoveryControlError::Conflict)?;
        let reads = futures::stream::iter(
            release
                .round
                .assignment_fence
                .participants
                .iter()
                .copied()
                .map(|participant| async move {
                    let value = self
                        .read_recovery_value(NodeId(participant.node_id), RELEASE_READY_ACK_KEY)
                        .await;
                    (participant, value)
                }),
        )
        .buffer_unordered(CONTROL_ROSTER_IO_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        let mut reads = reads;
        reads.sort_unstable_by_key(|(participant, _)| participant.node_id);
        let mut missing = Vec::new();
        for (participant, value) in reads {
            let raw = value.map_err(RecoveryControlError::Uncertain)?;
            let Some(raw) = raw else {
                missing.push(NodeId(participant.node_id));
                continue;
            };
            let ack = parse_release_ready_ack(&raw, NodeId(participant.node_id))
                .map_err(RecoveryControlError::Conflict)?;
            if ack.participant != participant {
                return Err(RecoveryControlError::Conflict(format!(
                    "release readiness from {} does not match the frozen process",
                    participant.node_id
                )));
            }
            if ack.release != expected {
                if ack.release.generation < expected.generation {
                    missing.push(NodeId(participant.node_id));
                    continue;
                }
                if ack.release.generation > expected.generation {
                    return Err(RecoveryControlError::Superseded(format!(
                        "release readiness from {} has newer generation {}",
                        participant.node_id, ack.release.generation
                    )));
                }
                return Err(RecoveryControlError::Conflict(format!(
                    "release readiness from {} conflicts with generation {}",
                    participant.node_id, expected.generation
                )));
            }
        }
        if !missing.is_empty() {
            return Ok(ReleaseReadyStatus::Pending { missing });
        }
        Ok(ReleaseReadyStatus::Complete)
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
    ) -> Result<(), String> {
        if !transition.is_canonical() {
            return Err("drain acknowledgement requires an exact assignment transition".into());
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
            encode_drain_ack(&DrainAck::for_transition(participant, transition))?,
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
        if let Some(fence) = fence.as_ref().filter(|fence| fence.is_canonical()) {
            let participants = fence.participant_ids();
            let changed = {
                let mut installed = self.leadership_participants.write();
                match installed.as_ref() {
                    Some((version, _)) if *version >= fence.assignment_version => false,
                    _ => {
                        *installed = Some((fence.assignment_version, participants));
                        true
                    }
                }
            };
            if changed {
                self.notify_leader_eligibility_change();
            }
        }
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

    /// Return the exact locally certified assignment cut while every participant remains
    /// checkpoint-capable. Active workers that own no vnode do not expand the checkpoint quorum.
    /// The clone is retained by the admitted attempt and propagated to followers.
    #[must_use]
    pub fn checkpoint_assignment_fence(
        &self,
        assignment_version: u64,
    ) -> Option<CheckpointAssignmentFence> {
        let checkpoint_capable: Vec<u64> = self
            .checkpoint_instances()
            .into_iter()
            .map(|node| node.0)
            .collect();
        self.checkpoint_assignment_fence
            .borrow()
            .as_ref()
            .filter(|fence| {
                fence.is_canonical()
                    && fence.assignment_version == assignment_version
                    && fence
                        .participant_incarnation(self.instance_id.0)
                        .is_none_or(|incarnation| incarnation == self.recovery_incarnation)
                    && fence.participants.iter().all(|participant| {
                        checkpoint_capable
                            .binary_search(&participant.node_id)
                            .is_ok()
                    })
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

    /// Confirm that one exact remote process still holds a proof read from durable authority.
    ///
    /// The RPC returns only a challenge acknowledgement and never returns authority material.
    ///
    /// # Errors
    /// Fails closed when the control RPC is unavailable, malformed, or misses `deadline`.
    #[cfg(feature = "cluster")]
    pub async fn confirm_remote_leader_proof(
        &self,
        proof: &LeaderProof,
        deadline: tokio::time::Instant,
    ) -> Result<bool, String> {
        self.barrier
            .confirm_remote_leader_proof(proof, deadline)
            .await
    }

    #[cfg(feature = "cluster")]
    async fn confirm_assignment_leader_proof(
        &self,
        leader: NodeId,
        proof: &LeaderProof,
        deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        if tokio::time::Instant::now() >= deadline {
            return Err("assignment leader authority audit deadline expired".into());
        }
        if proof.owner.node_id != leader.0 {
            return Err("assignment leader proof does not match the current leader".into());
        }
        let confirmed = if leader == self.instance_id {
            self.capture_leader_proof().as_ref() == Some(proof)
        } else {
            self.confirm_remote_leader_proof(proof, deadline).await?
        };
        if !confirmed {
            return Err(format!(
                "assignment leader process {} has no live exact grant",
                leader.0
            ));
        }
        Ok(())
    }

    /// Audit one exact assignment leader against durable and process-local authority.
    ///
    /// This is a single bounded attempt intended for a caller that already holds its assignment
    /// and execution locks. `expected_proof` binds predecessor execution during an active drain;
    /// a stable assignment passes `None` and adopts the current durable grant. The audit never
    /// waits for convergence and never mutates assignment, lease, or source-gate state.
    ///
    /// # Errors
    /// Fails closed when the installed assignment, elected participant, leader authority, live
    /// local/remote grant, or durable process term changes or cannot be verified before `deadline`.
    #[cfg(feature = "cluster")]
    pub async fn audit_assignment_leader_authority(
        &self,
        fence: &CheckpointAssignmentFence,
        expected_proof: Option<&LeaderProof>,
        deadline: tokio::time::Instant,
    ) -> Result<LeaderProof, String> {
        if tokio::time::Instant::now() >= deadline {
            return Err("assignment leader authority audit deadline expired".into());
        }
        if !fence.is_canonical()
            || self
                .checkpoint_assignment_fence(fence.assignment_version)
                .as_ref()
                != Some(fence)
        {
            return Err(
                "assignment leader authority audit requires the exact installed fence".into(),
            );
        }
        if expected_proof.is_some_and(|proof| !proof.is_canonical()) {
            return Err("assignment leader authority audit expected proof is not canonical".into());
        }

        let leader = self
            .current_leader()
            .ok_or_else(|| "assignment leader authority audit has no current leader".to_string())?;
        let participant = fence
            .participants
            .iter()
            .find(|participant| participant.node_id == leader.0)
            .copied()
            .ok_or_else(|| {
                "current leader is not a participant in the exact assignment fence".to_string()
            })?;
        let authority = self
            .checkpoint_authority()
            .map_err(|error| format!("assignment leader authority is unavailable: {error}"))?;
        let initial = tokio::time::timeout_at(deadline, authority.load())
            .await
            .map_err(|_| "assignment leader authority initial read timed out".to_string())?
            .map_err(|error| format!("assignment leader authority initial read failed: {error}"))?
            .ok_or_else(|| "assignment leader authority has no durable grant".to_string())?;
        if initial.owner.node != leader || initial.owner.boot != participant.boot_incarnation {
            return Err(
                "durable leader grant does not match the elected assignment participant".into(),
            );
        }
        let proof = match expected_proof {
            Some(expected) if initial.matches_proof(expected) => expected.clone(),
            Some(_) => {
                return Err(
                    "durable leader grant does not match the drain-bound expected proof".into(),
                );
            }
            None => initial.proof(),
        };

        self.confirm_assignment_leader_proof(leader, &proof, deadline)
            .await?;
        let process_authority = self
            .process_lease_authority
            .get()
            .ok_or_else(|| "process lease authority is not installed".to_string())?;
        let process_current = process_authority
            .verify_current_participant_term(participant, proof.owner.process_term, deadline)
            .await
            .map_err(|error| {
                format!("assignment leader process-term verification failed: {error}")
            })?;
        if !process_current {
            return Err("assignment leader durable process term is no longer current".to_string());
        }
        self.confirm_assignment_leader_proof(leader, &proof, deadline)
            .await?;

        let final_grant = tokio::time::timeout_at(deadline, authority.load())
            .await
            .map_err(|_| "assignment leader authority final read timed out".to_string())?
            .map_err(|error| format!("assignment leader authority final read failed: {error}"))?
            .ok_or_else(|| "assignment leader authority grant disappeared".to_string())?;
        if final_grant.owner != initial.owner
            || final_grant.token != initial.token
            || final_grant.seq < initial.seq
            || !final_grant.matches_proof(&proof)
        {
            return Err("assignment leader durable grant changed during the audit".into());
        }
        if self.current_leader() != Some(leader)
            || self
                .checkpoint_assignment_fence(fence.assignment_version)
                .as_ref()
                != Some(fence)
        {
            return Err("assignment leader or exact fence changed during the audit".into());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("assignment leader authority audit deadline expired".into());
        }
        Ok(proof)
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

    async fn durable_recovery_proof_is_current(&self, proof: &LeaderProof) -> Result<bool, String> {
        if !proof.is_canonical() {
            return Ok(false);
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|error| format!("durable recovery authority is unavailable: {error}"))?;
        let Some(lease) = authority
            .load()
            .await
            .map_err(|error| format!("durable recovery authority read failed: {error}"))?
        else {
            return Ok(false);
        };
        Ok(lease.matches_proof(proof))
    }

    async fn recovery_driver_proof_is_current(
        &self,
        round: &RecoveryRound,
    ) -> Result<bool, String> {
        Ok(round.id.driver == self.instance_id
            && self.proof_is_live(&round.leader_proof)
            && self
                .durable_recovery_proof_is_current(&round.leader_proof)
                .await?)
    }

    async fn require_recovery_driver_proof(
        &self,
        round: &RecoveryRound,
        boundary: &str,
    ) -> Result<(), String> {
        if self.recovery_driver_proof_is_current(round).await? {
            Ok(())
        } else {
            Err(format!(
                "recovery driver proof is no longer live at {boundary}"
            ))
        }
    }

    async fn require_recovery_driver_proof_control(
        &self,
        round: &RecoveryRound,
        boundary: &str,
    ) -> Result<(), RecoveryControlError> {
        if round.id.driver != self.instance_id || !self.proof_is_live(&round.leader_proof) {
            return Err(RecoveryControlError::Superseded(format!(
                "recovery driver proof is no longer live at {boundary}"
            )));
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|error| RecoveryControlError::Conflict(error.to_string()))?;
        let current = authority.load().await.map_err(|error| match error {
            super::LeaseError::Io(reason) => RecoveryControlError::Uncertain(reason),
            error => RecoveryControlError::Conflict(error.to_string()),
        })?;
        if !current.is_some_and(|lease| lease.matches_proof(&round.leader_proof)) {
            return Err(RecoveryControlError::Superseded(format!(
                "durable recovery driver proof changed at {boundary}"
            )));
        }
        Ok(())
    }

    /// Announce phase 1 with the immutable assignment-certified quorum.
    ///
    /// # Errors
    /// Returns an error unless this node is the round's current leader and driver.
    pub async fn announce_recover_prepare(&self, round: &RecoveryRound) -> Result<(), String> {
        round.validate()?;
        if round.id.driver != self.instance_id {
            return Err("only the current leader may prepare its recovery round".into());
        }
        self.require_recovery_driver_proof(round, "Prepare preflight")
            .await?;
        if !self.recovery_incarnations_match(round).await? {
            return Err("recovery process-incarnation roster changed before Prepare".into());
        }
        let _guard = self.recovery_writes.lock().await;
        self.require_recovery_driver_proof(round, "Prepare publication")
            .await?;
        let announcement = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Prepare,
        };
        let encoded = serde_json::to_string(&announcement)
            .map_err(|error| format!("could not encode recovery prepare: {error}"))?;
        self.write_recovery_value_exact("control:recover", encoded)
            .await?;
        self.require_recovery_driver_proof(round, "Prepare read-back")
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
        if round.id.driver != self.instance_id {
            return Err("only the current leader may start its recovery round".into());
        }
        self.require_recovery_driver_proof(round, "Start preflight")
            .await?;
        let _guard = self.recovery_writes.lock().await;
        if !self.recovery_incarnations_match(round).await? {
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
        self.require_recovery_driver_proof(round, "Start publication")
            .await?;
        let announcement = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Start { epoch },
        };
        let encoded = serde_json::to_string(&announcement)
            .map_err(|error| format!("could not encode recovery start: {error}"))?;
        self.write_recovery_value_exact("control:recover", encoded)
            .await?;
        self.require_recovery_driver_proof(round, "Start read-back")
            .await
    }

    /// Transition the identical `Start` to a pending `Release`. Source gates remain closed until
    /// the leader commits the exact compact readiness roster.
    ///
    /// # Errors
    /// Returns an error on lost leadership, a changed incarnation roster, or a mismatched Start.
    pub async fn announce_recover_release(
        &self,
        round: &RecoveryRound,
        epoch: u64,
    ) -> Result<(), String> {
        round.validate()?;
        if round.id.driver != self.instance_id {
            return Err("only the current leader may release its recovery round".into());
        }
        self.require_recovery_driver_proof(round, "Release preflight")
            .await?;
        let _guard = self.recovery_writes.lock().await;
        if !self.recovery_incarnations_match(round).await? {
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
        self.require_recovery_driver_proof(round, "Release publication")
            .await?;
        let release = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Release { epoch },
        };
        let encoded = serde_json::to_string(&release)
            .map_err(|error| format!("could not encode recovery Release: {error}"))?;
        self.write_recovery_value_exact("control:recover", encoded)
            .await?;
        self.require_recovery_driver_proof(round, "Release read-back")
            .await
    }

    /// Commit a pending release after every frozen owner published its compact readiness record.
    ///
    /// Pending attempts point-read only the readiness roster. Once complete, the process roster
    /// and fault set are validated under the driver's phase-transition mutex before admitting the
    /// content-addressed terminal into durable leader authority.
    ///
    /// # Errors
    /// Returns a classified uncertain, conflict, or superseded outcome. Missing readiness remains
    /// a normal pending status.
    pub async fn try_commit_recover_release(
        &self,
        release: &RecoveryAnnouncement,
    ) -> Result<ReleaseCommitStatus, RecoveryControlError> {
        release.validate().map_err(RecoveryControlError::Conflict)?;
        let RecoverPhase::Release { epoch } = release.phase else {
            return Err(RecoveryControlError::Conflict(
                "release commit must bind a pending Release target".into(),
            ));
        };
        let round = &release.round;
        if round.id.driver != self.instance_id {
            return Err(RecoveryControlError::Superseded(
                "only the current leader may commit its recovery Release".into(),
            ));
        }
        self.require_recovery_driver_proof_control(round, "Release commit preflight")
            .await?;
        match self.read_release_ready(release).await? {
            ReleaseReadyStatus::Complete => {}
            ReleaseReadyStatus::Pending { missing } => {
                return Ok(ReleaseCommitStatus::Pending { missing });
            }
        }
        let committed = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::ReleaseCommitted { epoch },
        };
        let authority = self
            .checkpoint_authority()
            .map_err(|error| RecoveryControlError::Conflict(error.to_string()))?;
        let reference = authority
            .stage_recovery_release_terminal(&committed)
            .await
            .map_err(RecoveryControlError::from_authority)?;

        let _guard = self.recovery_writes.lock().await;
        if !self.recovery_incarnations_match_control(round).await? {
            return Err(RecoveryControlError::Superseded(
                "recovery process-incarnation roster changed before Release commit".into(),
            ));
        }
        let mut observed_faults = self
            .read_fault_reports_control()
            .await?
            .into_iter()
            .filter_map(|(reporter, sequence)| {
                (sequence != 0).then_some(RecoveryFault { reporter, sequence })
            })
            .collect::<Vec<_>>();
        observed_faults.sort_unstable_by_key(|fault| fault.reporter);
        if observed_faults != round.faults {
            return Err(RecoveryControlError::Superseded(
                "recovery fault set changed before Release commit".into(),
            ));
        }
        let Some(current) = self
            .read_recovery_value(self.instance_id, "control:recover")
            .await
            .map_err(RecoveryControlError::Uncertain)?
        else {
            return Err(RecoveryControlError::Superseded(
                "pending recovery Release disappeared before commit".into(),
            ));
        };
        let active = match parse_recovery_announcement(&current) {
            Ok(Some(active)) => active,
            Ok(None) => {
                return Err(RecoveryControlError::Superseded(
                    "pending recovery Release was cleared before commit".into(),
                ));
            }
            Err(reason) => return Err(RecoveryControlError::Conflict(reason)),
        };
        if active != *release {
            let error = if active.round.id.generation > round.id.generation {
                RecoveryControlError::Superseded(
                    "a newer recovery intent replaced the pending Release".into(),
                )
            } else {
                RecoveryControlError::Conflict(
                    "recovery Release commit does not match the exact pending intent".into(),
                )
            };
            return Err(error);
        }
        self.require_recovery_driver_proof_control(round, "Release commit publication")
            .await?;
        match authority
            .record_recovery_release_commit(&round.leader_proof, reference)
            .await
            .map_err(RecoveryControlError::from_authority)?
        {
            super::leader_lease::RecordRecoveryReleaseCommitResult::Created(_)
            | super::leader_lease::RecordRecoveryReleaseCommitResult::Unchanged(_) => {
                Ok(ReleaseCommitStatus::Committed {
                    terminal: committed,
                })
            }
            super::leader_lease::RecordRecoveryReleaseCommitResult::Conflict { winner } => {
                if winner.generation() > round.id.generation {
                    Err(RecoveryControlError::Superseded(format!(
                        "recovery release generation {} replaced generation {}",
                        winner.generation(),
                        round.id.generation
                    )))
                } else {
                    Err(RecoveryControlError::Conflict(format!(
                        "recovery release generation {} has a different durable winner",
                        round.id.generation
                    )))
                }
            }
        }
    }

    /// Active recovery announcement with semantic failures separated from uncertain I/O.
    ///
    /// # Errors
    /// Classifies malformed state, superseded authority, and retryable durable I/O separately.
    pub async fn observe_recover_control(
        &self,
    ) -> Result<Option<RecoveryAnnouncement>, RecoveryControlError> {
        let Some(current_driver) = self.current_leader() else {
            return Ok(None);
        };
        let Some(raw) = self
            .read_recovery_value(current_driver, "control:recover")
            .await
            .map_err(RecoveryControlError::Uncertain)?
        else {
            return Ok(None);
        };
        let Some(announcement) =
            parse_recovery_announcement(&raw).map_err(RecoveryControlError::Conflict)?
        else {
            return Ok(None);
        };
        if matches!(announcement.phase, RecoverPhase::ReleaseCommitted { .. }) {
            return Err(RecoveryControlError::Conflict(
                "committed recovery release appeared in the mutable intent slot".into(),
            ));
        }
        if announcement.round.id.driver != current_driver {
            return Err(RecoveryControlError::Conflict(format!(
                "recovery publisher {current_driver} is not declared driver {}",
                announcement.round.id.driver
            )));
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|error| RecoveryControlError::Conflict(error.to_string()))?;
        let Some(authority_before) = authority.load().await.map_err(|error| match error {
            super::LeaseError::Io(reason) => RecoveryControlError::Uncertain(reason),
            error => RecoveryControlError::Conflict(error.to_string()),
        })?
        else {
            return Err(RecoveryControlError::Superseded(
                "durable recovery authority has no leader".into(),
            ));
        };
        if !authority_before.matches_proof(&announcement.round.leader_proof) {
            return Err(RecoveryControlError::Superseded(format!(
                "recovery phase from {current_driver} does not match durable leader authority"
            )));
        }
        let Some(authority_after) = authority.load().await.map_err(|error| match error {
            super::LeaseError::Io(reason) => RecoveryControlError::Uncertain(reason),
            error => RecoveryControlError::Conflict(error.to_string()),
        })?
        else {
            return Err(RecoveryControlError::Superseded(
                "durable recovery authority vanished during observation".into(),
            ));
        };
        if self.current_leader() != Some(current_driver)
            || !authority_after.matches_proof(&announcement.round.leader_proof)
        {
            return Err(RecoveryControlError::Superseded(format!(
                "recovery authority changed while observing {current_driver}"
            )));
        }
        Ok(Some(announcement))
    }

    /// Latest irrevocable recovery release admitted by the append-only leader authority.
    ///
    /// # Errors
    /// Classifies missing/corrupt terminal state as conflict, takeover as supersession, and
    /// durable I/O as uncertainty.
    pub async fn latest_committed_recover_release(
        &self,
    ) -> Result<Option<RecoveryAnnouncement>, RecoveryControlError> {
        self.checkpoint_authority()
            .map_err(|error| RecoveryControlError::Conflict(error.to_string()))?
            .latest_recovery_release_terminal()
            .await
            .map_err(RecoveryControlError::from_authority)
    }

    /// Resolve the exact committed terminal for one pending release intent across leader
    /// takeover. An older terminal is unrelated; a same-generation divergence is corruption and a
    /// newer terminal supersedes the caller.
    ///
    /// # Errors
    /// Returns a classified conflict, supersession, or uncertain durable read.
    pub async fn observe_committed_recover_release(
        &self,
        round: &RecoveryRound,
        epoch: u64,
    ) -> Result<Option<RecoveryAnnouncement>, RecoveryControlError> {
        round.validate().map_err(RecoveryControlError::Conflict)?;
        let expected = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::ReleaseCommitted { epoch },
        };
        let Some(terminal) = self.latest_committed_recover_release().await? else {
            return Ok(None);
        };
        match terminal.round.id.generation.cmp(&round.id.generation) {
            std::cmp::Ordering::Less => Ok(None),
            std::cmp::Ordering::Greater => Err(RecoveryControlError::Superseded(format!(
                "recovery release generation {} replaced generation {}",
                terminal.round.id.generation, round.id.generation
            ))),
            std::cmp::Ordering::Equal if terminal == expected => Ok(Some(terminal)),
            std::cmp::Ordering::Equal => Err(RecoveryControlError::Conflict(format!(
                "committed recovery release generation {} differs from the expected round",
                round.id.generation
            ))),
        }
    }

    /// Best-effort cleanup for this driver's mutable `Release` discovery hint after the exact
    /// terminal is irrevocably present in leader authority. Cleanup never contributes to commit
    /// validity; followers resolve the terminal from authority even if this write is uncertain.
    ///
    /// # Errors
    /// Classifies malformed or divergent local intent separately from retryable I/O.
    pub async fn retire_committed_recover_release_hint(
        &self,
        round: &RecoveryRound,
        epoch: u64,
    ) -> Result<bool, RecoveryControlError> {
        round.validate().map_err(RecoveryControlError::Conflict)?;
        if round.id.driver != self.instance_id {
            return Err(RecoveryControlError::Conflict(
                "only the publishing driver may retire its recovery Release hint".into(),
            ));
        }
        if self
            .observe_committed_recover_release(round, epoch)
            .await?
            .is_none()
        {
            return Ok(false);
        }
        let pending = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Release { epoch },
        };
        let _guard = self.recovery_writes.lock().await;
        let Some(raw) = self
            .read_recovery_value(self.instance_id, "control:recover")
            .await
            .map_err(RecoveryControlError::Uncertain)?
        else {
            return Ok(false);
        };
        let active = parse_recovery_announcement(&raw)
            .map_err(RecoveryControlError::Conflict)?
            .ok_or_else(|| {
                RecoveryControlError::Conflict(
                    "recovery Release hint decoded as an empty announcement".into(),
                )
            })?;
        if active != pending {
            return if active.round.id.generation > round.id.generation {
                Err(RecoveryControlError::Superseded(
                    "a newer recovery intent replaced the committed Release hint".into(),
                ))
            } else {
                Err(RecoveryControlError::Conflict(
                    "mutable recovery intent differs from its committed Release".into(),
                ))
            };
        }
        self.write_recovery_value_exact("control:recover", String::new())
            .await
            .map_err(RecoveryControlError::Uncertain)?;
        Ok(true)
    }

    /// Active nonterminal recovery announcement from the locally elected driver.
    ///
    /// # Errors
    /// Returns a display-stable classified control error.
    pub async fn observe_recover(&self) -> Result<Option<RecoveryAnnouncement>, String> {
        self.observe_recover_control()
            .await
            .map_err(|error| error.to_string())
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

    struct DelayedRecoveryKv {
        inner: InMemoryKv,
        block_next_recovery_write: std::sync::atomic::AtomicBool,
        entered: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
    }

    struct FaultyReadyReadKv {
        inner: InMemoryKv,
        remaining_failures: std::sync::atomic::AtomicUsize,
    }

    impl FaultyReadyReadKv {
        fn new(local_id: NodeId) -> Self {
            Self {
                inner: InMemoryKv::new(local_id),
                remaining_failures: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn fail_next_ready_reads(&self, failures: usize) {
            self.remaining_failures.store(failures, Ordering::Release);
        }

        fn should_fail_ready_read(&self) -> bool {
            self.remaining_failures
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    (remaining != 0).then(|| remaining.saturating_sub(1))
                })
                .is_ok()
        }
    }

    impl DelayedRecoveryKv {
        fn new(local_id: NodeId) -> Self {
            Self {
                inner: InMemoryKv::new(local_id),
                block_next_recovery_write: std::sync::atomic::AtomicBool::new(false),
                entered: tokio::sync::Semaphore::new(0),
                release: tokio::sync::Semaphore::new(0),
            }
        }

        fn block_next_recovery_write(&self) {
            self.block_next_recovery_write
                .store(true, Ordering::Release);
        }

        async fn wait_until_blocked(&self) {
            self.entered.acquire().await.unwrap().forget();
        }

        fn release_blocked_write(&self) {
            self.release.add_permits(1);
        }
    }

    #[async_trait::async_trait]
    impl ClusterKv for DelayedRecoveryKv {
        async fn write(&self, key: &str, value: String) {
            let _ = self.write_checked(key, value).await;
        }

        async fn write_checked(&self, key: &str, value: String) -> Result<(), String> {
            if key == "control:recover"
                && self.block_next_recovery_write.swap(false, Ordering::AcqRel)
            {
                self.entered.add_permits(1);
                self.release
                    .acquire()
                    .await
                    .map_err(|error| error.to_string())?
                    .forget();
            }
            self.inner.write(key, value).await;
            Ok(())
        }

        async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
            self.inner.read_from(who, key).await
        }

        async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
            self.inner.scan(key).await
        }
    }

    #[async_trait::async_trait]
    impl ClusterKv for FaultyReadyReadKv {
        async fn write(&self, key: &str, value: String) {
            self.inner.write(key, value).await;
        }

        async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
            self.inner.read_from(who, key).await
        }

        async fn read_from_checked(
            &self,
            who: NodeId,
            key: &str,
        ) -> Result<Option<String>, String> {
            if key == RELEASE_READY_ACK_KEY && self.should_fail_ready_read() {
                return Err("injected release readiness read failure".into());
            }
            Ok(self.inner.read_from(who, key).await)
        }

        async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
            self.inner.scan(key).await
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
    async fn recovery_authority_rejects_a_structural_but_undurable_process_fence() {
        use crate::cluster::control::{
            AssignmentRecoveryDecision, AssignmentSnapshotRef, LeaderLeaseOwner, LeaseOutcome,
            ProcessLease, ProcessLeaseAuthority, ProcessLeaseFence,
        };

        let controller = ctl(1, Vec::new());
        let backing: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let process_authority = Arc::new(
            ProcessLeaseAuthority::new(Arc::clone(&backing), Duration::from_millis(1)).unwrap(),
        );
        controller
            .set_process_lease_authority(process_authority)
            .unwrap();
        let authority = Arc::new(super::super::LeaderLeaseStore::new(
            Arc::clone(&backing),
            1_000,
        ));
        let owner = LeaderLeaseOwner {
            node: NodeId(1),
            boot: controller.recovery_incarnation(),
            process_term: 1,
        };
        let LeaseOutcome::Acquired(lease) = authority.try_acquire(&owner, 0).await.unwrap() else {
            panic!("empty leader authority must be acquired");
        };
        controller.set_leader_lease_store(Arc::clone(&authority));

        let removed_boot = Uuid::from_u128(2);
        let predecessor = CheckpointAssignmentFence::from_owner_map(
            1,
            &[1, 2],
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: owner.boot,
                },
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: removed_boot,
                },
            ],
        )
        .unwrap();
        let target = CheckpointAssignmentFence::from_owner_map(
            2,
            &[1, 1],
            vec![CheckpointParticipant {
                node_id: 1,
                boot_incarnation: owner.boot,
            }],
        )
        .unwrap();
        let forged = ProcessLeaseFence::new(
            ProcessLease {
                node: NodeId(2),
                owner: removed_boot,
                term: 1,
                seq: 1,
                expires_at_ms: 1,
            },
            ProcessLease {
                node: NodeId(2),
                owner: Uuid::from_u128(3),
                term: 2,
                seq: 2,
                expires_at_ms: 2,
            },
        )
        .unwrap();
        let decision = AssignmentRecoveryDecision::new(
            predecessor,
            target,
            AssignmentSnapshotRef {
                version: 2,
                sha256: "0".repeat(64),
                encoded_len: 1,
            },
            vec![forged],
            lease.proof(),
        )
        .unwrap();

        let error = controller
            .record_assignment_recovery_decision(
                &lease.proof(),
                decision,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(
            error.contains("process fence verification failed"),
            "{error}"
        );
        assert!(authority
            .assignment_recovery_decision(2)
            .await
            .unwrap()
            .is_none());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn assignment_leader_audit_accepts_only_the_exact_local_grant_and_process_term() {
        use crate::cluster::control::{
            LeaderLeaseOwner, LeaderLeaseStore, LeaseDeadline, LeaseOutcome, ProcessLeaseAuthority,
            ProcessLeaseOutcome,
        };

        let node = NodeId(1);
        let boot = Uuid::from_u128(11);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = ClusterController::new_with_recovery_incarnation(
            node,
            Arc::clone(&kv),
            kv,
            None,
            members_rx,
            boot,
        );
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(30))));

        let backing: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let process_authority = Arc::new(
            ProcessLeaseAuthority::new(Arc::clone(&backing), Duration::from_millis(1)).unwrap(),
        );
        let process_store = process_authority.store_for(node);
        let ProcessLeaseOutcome::Acquired(process_lease) =
            process_store.try_acquire(boot, 0).await.unwrap()
        else {
            panic!("empty process authority must be acquired");
        };
        controller
            .set_process_lease_authority(process_authority)
            .unwrap();

        let leader_authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&backing), 1));
        let owner = LeaderLeaseOwner {
            node,
            boot,
            process_term: process_lease.term,
        };
        let LeaseOutcome::Acquired(leader_lease) =
            leader_authority.try_acquire(&owner, 0).await.unwrap()
        else {
            panic!("empty leader authority must be acquired");
        };
        controller.set_leader_lease_store(Arc::clone(&leader_authority));
        let (_leader_tx, leader_rx) = watch::channel(Some(leader_lease.clone()));
        controller
            .set_leader_lease_watch(
                leader_rx,
                owner,
                Arc::new(LeaseDeadline::live_for(Duration::from_secs(30))),
            )
            .unwrap();

        let fence = CheckpointAssignmentFence::from_owner_map(
            1,
            &[node.0],
            vec![CheckpointParticipant {
                node_id: node.0,
                boot_incarnation: boot,
            }],
        )
        .unwrap();
        controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
        let deadline = || tokio::time::Instant::now() + Duration::from_secs(1);

        let proof = controller
            .audit_assignment_leader_authority(&fence, None, deadline())
            .await
            .unwrap();
        assert_eq!(proof, leader_lease.proof());
        assert_eq!(
            controller
                .audit_assignment_leader_authority(&fence, Some(&proof), deadline())
                .await
                .unwrap(),
            proof
        );

        let mut stale = proof.clone();
        stale.fencing_token += 1;
        let stale_error = controller
            .audit_assignment_leader_authority(&fence, Some(&stale), deadline())
            .await
            .unwrap_err();
        assert!(
            stale_error.contains("drain-bound expected proof"),
            "{stale_error}"
        );

        let other_fence = CheckpointAssignmentFence::from_owner_map(
            2,
            &[node.0],
            vec![CheckpointParticipant {
                node_id: node.0,
                boot_incarnation: boot,
            }],
        )
        .unwrap();
        let fence_error = controller
            .audit_assignment_leader_authority(&other_fence, None, deadline())
            .await
            .unwrap_err();
        assert!(
            fence_error.contains("exact installed fence"),
            "{fence_error}"
        );

        let observation = process_store.observe_rival(&process_lease).unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(matches!(
            process_store
                .try_takeover(Uuid::from_u128(12), &observation, 2)
                .await
                .unwrap(),
            ProcessLeaseOutcome::Acquired(_)
        ));
        let process_error = controller
            .audit_assignment_leader_authority(&fence, None, deadline())
            .await
            .unwrap_err();
        assert!(
            process_error.contains("process term is no longer current"),
            "{process_error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn assignment_leader_audit_rejects_takeover_after_second_remote_confirmation() {
        use crate::cluster::control::{
            LeaderLeaseOwner, LeaderLeaseStore, LeaseOutcome, ProcessLeaseAuthority,
            ProcessLeaseOutcome,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};

        let leader_node = NodeId(1);
        let observer_node = NodeId(2);
        let leader_boot = Uuid::from_u128(11);
        let observer_boot = Uuid::from_u128(22);
        let observer_kv = Arc::new(InMemoryKv::new(observer_node));
        let observer_control: Arc<dyn ClusterKv> = observer_kv.clone();
        let (_members_tx, members_rx) = watch::channel(vec![info(leader_node.0)]);
        let observer = Arc::new(ClusterController::new_with_recovery_incarnation(
            observer_node,
            Arc::clone(&observer_control),
            observer_control,
            None,
            members_rx,
            observer_boot,
        ));

        let backing: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let process_authority = Arc::new(
            ProcessLeaseAuthority::new(Arc::clone(&backing), Duration::from_millis(1)).unwrap(),
        );
        let ProcessLeaseOutcome::Acquired(process_lease) = process_authority
            .store_for(leader_node)
            .try_acquire(leader_boot, 0)
            .await
            .unwrap()
        else {
            panic!("leader process authority must be acquired");
        };
        observer
            .set_process_lease_authority(process_authority)
            .unwrap();

        let leader_authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&backing), 1));
        let initial_owner = LeaderLeaseOwner {
            node: leader_node,
            boot: leader_boot,
            process_term: process_lease.term,
        };
        let LeaseOutcome::Acquired(initial_lease) = leader_authority
            .try_acquire(&initial_owner, 0)
            .await
            .unwrap()
        else {
            panic!("leader authority must be acquired");
        };
        observer.set_leader_lease_store(Arc::clone(&leader_authority));
        let fence = CheckpointAssignmentFence::from_owner_map(
            1,
            &[leader_node.0],
            vec![CheckpointParticipant {
                node_id: leader_node.0,
                boot_incarnation: leader_boot,
            }],
        )
        .unwrap();
        observer.publish_checkpoint_assignment_fence(Some(fence.clone()));

        observer
            .start_barrier_server("127.0.0.1:0".parse().unwrap(), None)
            .await
            .unwrap();
        let remote = BarrierCoordinator::new(Arc::new(InMemoryKv::new(leader_node)));
        let calls = Arc::new(AtomicUsize::new(0));
        let (second_tx, mut second_rx) = tokio::sync::mpsc::unbounded_channel();
        let release = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let provider_calls = Arc::clone(&calls);
        let provider_release = Arc::clone(&release);
        let provider_proof = initial_lease.proof();
        remote.set_local_leader_proof_provider(Arc::new(move || {
            if provider_calls.fetch_add(1, Ordering::AcqRel) == 1 {
                let _ = second_tx.send(());
                let (lock, ready) = &*provider_release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
            }
            Some(provider_proof.clone())
        }));
        let remote_addr = remote
            .start_server(
                "127.0.0.1:0".parse().unwrap(),
                None,
                Arc::new(parking_lot::RwLock::new(None)),
            )
            .await
            .unwrap();
        observer_kv.seed(leader_node, BARRIER_ADDR_KEY, remote_addr.to_string());

        let successor = LeaderLeaseOwner {
            node: observer_node,
            boot: observer_boot,
            process_term: 1,
        };
        let observation = leader_authority
            .observe_rival(&successor, &initial_lease)
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        let audit = {
            let observer = Arc::clone(&observer);
            let fence = fence.clone();
            tokio::spawn(async move {
                observer
                    .audit_assignment_leader_authority(
                        &fence,
                        None,
                        tokio::time::Instant::now() + Duration::from_secs(2),
                    )
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), second_rx.recv())
            .await
            .unwrap()
            .expect("the audit must reach its second live confirmation");
        assert!(matches!(
            leader_authority
                .try_takeover(&successor, &observation, 2)
                .await
                .unwrap(),
            LeaseOutcome::Acquired(_)
        ));
        {
            let (lock, ready) = &*release;
            *lock.lock().unwrap() = true;
            ready.notify_all();
        }

        let error = audit.await.unwrap().unwrap_err();
        assert!(
            error.contains("durable grant changed during the audit"),
            "{error}"
        );
        assert_eq!(calls.load(Ordering::Acquire), 2);
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
    fn checkpoint_assignment_fence_allows_active_workers_without_vnodes() {
        let self_id = NodeId(1);
        let (_members_tx, members_rx) = watch::channel(vec![info(2), info(3)]);
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

        assert_eq!(
            c.checkpoint_assignment_fence(7),
            Some(expected),
            "an active worker that owns no vnode must not expand the checkpoint quorum"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn certified_leader_retains_local_candidacy_to_coordinate_its_drain() {
        use crate::cluster::control::{LeaderLease, LeaderLeaseOwner, LeaseDeadline};

        let self_id = NodeId(1);
        let idle = NodeId(2);
        let (_members_tx, members_rx) = watch::channel(vec![info(idle.0)]);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
        let c = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
        let fence = CheckpointAssignmentFence::from_owner_map(
            7,
            &[self_id.0],
            vec![CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: c.recovery_incarnation(),
            }],
        )
        .unwrap();
        c.publish_checkpoint_assignment_fence(Some(fence.clone()));
        let owner = LeaderLeaseOwner {
            node: self_id,
            boot: c.recovery_incarnation(),
            process_term: 1,
        };
        let (_lease_tx, lease_rx) = watch::channel(Some(LeaderLease {
            seq: 1,
            token: 1,
            owner: owner.clone(),
            expires_at_ms: i64::MIN,
            catalog_manifest: None,
        }));
        c.set_leader_lease_watch(
            lease_rx,
            owner,
            Arc::new(LeaseDeadline::live_for(Duration::from_secs(10))),
        )
        .unwrap();
        let (idle_members_tx, idle_members_rx) = watch::channel(vec![info(self_id.0)]);
        let idle_kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(idle));
        let idle_observer = ClusterController::new(idle, idle_kv, None, idle_members_rx);
        idle_observer.publish_checkpoint_assignment_fence(Some(fence));
        assert_eq!(idle_observer.current_leader(), Some(self_id));
        let mut candidacy = c.leader_candidacy_watch();
        assert!(*candidacy.borrow_and_update());

        assert!(c.begin_drain());
        let mut advertised_draining = info(self_id.0);
        advertised_draining.state = NodeState::Draining;
        idle_members_tx.send(vec![advertised_draining]).unwrap();
        assert!(c.is_draining());
        assert_eq!(c.assignable_instances(), vec![idle]);
        assert_eq!(c.current_leader(), Some(self_id));
        assert!(c.is_gossip_leader());
        assert_eq!(idle_observer.current_leader(), None);
        assert!(!idle_observer.is_gossip_leader());
        assert!(
            !candidacy.has_changed().unwrap(),
            "coordinating the predecessor cut must not revoke the current leader's lease"
        );
    }

    #[test]
    fn certified_draining_peer_does_not_displace_an_active_participant() {
        let observer = NodeId(3);
        let draining = NodeId(1);
        let active = NodeId(2);
        let mut draining_info = info(draining.0);
        draining_info.state = NodeState::Draining;
        let (_members_tx, members_rx) = watch::channel(vec![info(active.0), draining_info]);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(observer));
        let c = ClusterController::new(observer, kv, None, members_rx);
        let fence = CheckpointAssignmentFence::from_owner_map(
            7,
            &[active.0, draining.0],
            vec![
                CheckpointParticipant {
                    node_id: draining.0,
                    boot_incarnation: Uuid::new_v4(),
                },
                CheckpointParticipant {
                    node_id: active.0,
                    boot_incarnation: Uuid::new_v4(),
                },
            ],
        )
        .unwrap();
        c.publish_checkpoint_assignment_fence(Some(fence));

        assert_eq!(c.current_leader(), Some(active));
        assert!(!c.is_gossip_leader());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn certified_idle_worker_yields_leadership_to_an_assignment_participant() {
        let self_id = NodeId(1);
        let owner = NodeId(2);
        let (_members_tx, members_rx) = watch::channel(vec![info(owner.0)]);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
        let c = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
        let mut candidacy = c.leader_candidacy_watch();
        assert!(*candidacy.borrow_and_update());
        assert_eq!(c.current_leader(), Some(self_id));

        let fence = CheckpointAssignmentFence::from_owner_map(
            7,
            &[owner.0],
            vec![CheckpointParticipant {
                node_id: owner.0,
                boot_incarnation: Uuid::new_v4(),
            }],
        )
        .unwrap();
        c.publish_checkpoint_assignment_fence(Some(fence.clone()));

        assert_eq!(c.checkpoint_assignment_fence(7), Some(fence.clone()));
        assert_eq!(c.current_leader(), Some(owner));
        assert!(!c.is_gossip_leader());
        tokio::time::timeout(Duration::from_secs(1), candidacy.changed())
            .await
            .expect("leader candidacy relay did not observe the assignment roster")
            .unwrap();
        assert!(!*candidacy.borrow_and_update());

        c.publish_checkpoint_assignment_fence(None);
        assert_eq!(
            c.current_leader(),
            Some(owner),
            "transient authority suspension must not make an idle worker leader"
        );

        let newer = CheckpointAssignmentFence::from_owner_map(
            8,
            &[self_id.0],
            vec![CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: c.recovery_incarnation(),
            }],
        )
        .unwrap();
        c.publish_checkpoint_assignment_fence(Some(newer));
        assert_eq!(c.current_leader(), Some(self_id));
        c.publish_checkpoint_assignment_fence(Some(fence));
        assert_eq!(
            c.current_leader(),
            Some(self_id),
            "a delayed older certificate must not restore an obsolete leadership roster"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn durably_fenced_idle_worker_leads_when_every_assignment_owner_is_unavailable() {
        let self_id = NodeId(1);
        let owner = NodeId(2);
        let (_members_tx, members_rx) = watch::channel(vec![info(owner.0)]);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
        let c = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
        let owner_process = CheckpointParticipant {
            node_id: owner.0,
            boot_incarnation: Uuid::from_u128(22),
        };
        c.publish_checkpoint_assignment_fence(Some(
            CheckpointAssignmentFence::from_owner_map(7, &[owner.0], vec![owner_process]).unwrap(),
        ));
        let (_authority, _proof) = install_recovery_authority(&c, 1_000).await;
        let mut candidacy = c.leader_candidacy_watch();

        assert_eq!(c.current_leader(), Some(owner));
        assert!(!*candidacy.borrow_and_update());
        c.note_unresponsive(&[owner]);
        tokio::time::timeout(Duration::from_secs(1), candidacy.changed())
            .await
            .expect("placement fallback candidacy did not update")
            .unwrap();

        assert!(*candidacy.borrow_and_update());
        assert_eq!(c.current_leader(), Some(self_id));
        assert!(c.is_leader());
        assert!(!c
            .checkpoint_assignment_fence(7)
            .unwrap()
            .contains(self_id.0));
    }

    #[test]
    fn checkpoint_assignment_fence_rejects_missing_or_suspected_participant() {
        let self_id = NodeId(1);
        let (members_tx, members_rx) = watch::channel(vec![info(2), info(3)]);
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
            Some(expected.clone()),
            "node-local quorum history must not make the shared assignment proof diverge"
        );

        let mut suspected = info(2);
        suspected.state = NodeState::Suspected;
        members_tx.send(vec![suspected, info(3)]).unwrap();
        assert_eq!(
            c.checkpoint_assignment_fence(7),
            None,
            "a cached fence must close immediately when a participant becomes unavailable"
        );

        members_tx.send(vec![info(2), info(3)]).unwrap();
        assert_eq!(c.checkpoint_assignment_fence(7), Some(expected));
        members_tx.send(vec![info(3)]).unwrap();
        assert_eq!(
            c.checkpoint_assignment_fence(7),
            None,
            "a missing vnode owner must invalidate the cached fence"
        );
    }

    #[test]
    fn quorum_miss_quarantine_requires_an_ack_or_a_different_boot() {
        let c = ctl(1, vec![info(2)]);
        let failed = CheckpointParticipant {
            node_id: 2,
            boot_incarnation: Uuid::from_u128(22),
        };
        let fence = CheckpointAssignmentFence::from_owner_map(
            7,
            &[1, 2],
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: c.recovery_incarnation(),
                },
                failed,
            ],
        )
        .unwrap();
        c.publish_checkpoint_assignment_fence(Some(fence));
        c.note_unresponsive(&[NodeId(2)]);

        assert!(c.is_unresponsive(NodeId(2)));
        assert!(!c.admit_successor_process(failed));
        assert!(c.admit_successor_process(CheckpointParticipant {
            node_id: 2,
            boot_incarnation: Uuid::from_u128(23),
        }));
        assert!(!c.is_unresponsive(NodeId(2)));
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
        controller.announce_drain_ack(&transition).await.unwrap();
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
        seed_peer_ack(DrainAck::for_transition(participants[1], &stale_transition));
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
        seed_peer_ack(DrainAck::for_transition(participants[1], &other_transition));
        assert!(!controller
            .drain_ack_quorum_reached(&transition)
            .await
            .unwrap());

        seed_peer_ack(DrainAck::for_transition(participants[1], &transition));
        assert!(controller
            .drain_ack_quorum_reached(&transition)
            .await
            .unwrap());
        controller
            .announce_drain_ack(&transition)
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
        let ack = DrainAck::for_transition(predecessor.participants[0], &transition);
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

        let mut prior_protocol = DrainAck::for_transition(predecessor.participants[0], &transition);
        prior_protocol.protocol_version = DRAIN_ACK_PROTOCOL_VERSION - 1;
        assert!(encode_drain_ack(&prior_protocol).is_err());
        assert!(
            parse_drain_ack(&serde_json::to_string(&prior_protocol).unwrap(), NodeId(1)).is_err()
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

    async fn install_recovery_authority(
        controller: &ClusterController,
        ttl_ms: i64,
    ) -> (Arc<super::super::LeaderLeaseStore>, LeaderProof) {
        use super::super::{LeaderLeaseOwner, LeaseDeadline, LeaseOutcome};

        let authority = Arc::new(super::super::LeaderLeaseStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            ttl_ms,
        ));
        let owner = LeaderLeaseOwner {
            node: controller.instance_id(),
            boot: controller.recovery_incarnation(),
            process_term: 1,
        };
        let LeaseOutcome::Acquired(lease) = authority.try_acquire(&owner, 0).await.unwrap() else {
            panic!("empty recovery authority must be acquired");
        };
        let (_lease_tx, lease_rx) = watch::channel(Some(lease.clone()));
        let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(10)));
        controller.set_process_lease_deadline(Arc::clone(&deadline));
        controller
            .set_leader_lease_watch(lease_rx, owner, deadline)
            .unwrap();
        controller.set_leader_lease_store(Arc::clone(&authority));
        (authority, lease.proof())
    }

    fn test_leader_proof(node_id: u64, boot_id: Uuid, process_term: u64) -> LeaderProof {
        LeaderProof {
            owner: crate::checkpoint::LeaderProofOwner {
                node_id,
                boot_id,
                process_term,
            },
            fencing_token: process_term,
        }
    }

    fn recovery_round(
        controller: &ClusterController,
        generation: u64,
        leader_proof: &LeaderProof,
        participants: &[u64],
    ) -> RecoveryRound {
        let participant_roster = participants
            .iter()
            .map(|node_id| CheckpointParticipant {
                node_id: *node_id,
                boot_incarnation: if *node_id == leader_proof.owner.node_id {
                    leader_proof.owner.boot_id
                } else if *node_id == controller.instance_id().0 {
                    controller.recovery_incarnation()
                } else {
                    Uuid::from_u128((u128::from(generation) << 64) | u128::from(*node_id))
                },
            })
            .collect();
        RecoveryRound::new(
            generation,
            leader_proof.clone(),
            CheckpointAssignmentFence::from_owner_map(7, participants, participant_roster).unwrap(),
            vec![RecoveryFault {
                reporter: NodeId(leader_proof.owner.node_id),
                sequence: generation,
            }],
        )
        .unwrap()
    }

    fn seed_release_ready(
        kv: &InMemoryKv,
        participant: CheckpointParticipant,
        release: &RecoveryAnnouncement,
    ) {
        let encoded = encode_release_ready_ack(&RecoveryReleaseReadyAck {
            release: RecoveryReleaseId::for_pending(release).unwrap(),
            participant,
        })
        .unwrap();
        kv.seed(NodeId(participant.node_id), RELEASE_READY_ACK_KEY, encoded);
    }

    async fn two_owner_pending_release() -> (
        ClusterController,
        Arc<InMemoryKv>,
        RecoveryAnnouncement,
        CheckpointParticipant,
    ) {
        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let (_members_tx, members_rx) = watch::channel(vec![info(2)]);
        let controller = ClusterController::new(self_id, kv.clone(), None, members_rx);
        controller.publish_recovery_incarnation().await.unwrap();
        let (_authority, proof) = install_recovery_authority(&controller, 10_000).await;
        let round = recovery_round(&controller, 41, &proof, &[1, 2]);
        let remote = round.assignment_fence.participants[1];
        kv.seed(
            NodeId(remote.node_id),
            RECOVERY_INCARNATION_KEY,
            remote.boot_incarnation.to_string(),
        );
        controller.report_fault(41).await.unwrap();
        controller.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
        controller.announce_recover_prepare(&round).await.unwrap();
        controller.announce_recover_start(&round, 8).await.unwrap();
        controller
            .announce_recover_release(&round, 8)
            .await
            .unwrap();
        let release = RecoveryAnnouncement {
            round,
            phase: RecoverPhase::Release { epoch: 8 },
        };
        (controller, kv, release, remote)
    }

    async fn faulty_single_owner_pending_release() -> (
        ClusterController,
        Arc<FaultyReadyReadKv>,
        RecoveryAnnouncement,
    ) {
        let self_id = NodeId(1);
        let kv = Arc::new(FaultyReadyReadKv::new(self_id));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = ClusterController::new(self_id, kv.clone(), None, members_rx);
        controller.publish_recovery_incarnation().await.unwrap();
        let (_authority, proof) = install_recovery_authority(&controller, 10_000).await;
        let round = recovery_round(&controller, 43, &proof, &[1]);
        controller.report_fault(43).await.unwrap();
        controller.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
        controller.announce_recover_prepare(&round).await.unwrap();
        controller.announce_recover_start(&round, 9).await.unwrap();
        controller
            .announce_recover_release(&round, 9)
            .await
            .unwrap();
        let release = RecoveryAnnouncement {
            round,
            phase: RecoverPhase::Release { epoch: 9 },
        };
        controller.announce_release_ready(&release).await.unwrap();
        (controller, kv, release)
    }

    #[test]
    fn recovery_round_requires_a_canonical_nonzero_fault_set() {
        let c = ctl(1, vec![]);
        let proof = test_leader_proof(1, c.recovery_incarnation(), 1);
        let exact = recovery_round(&c, 11, &proof, &[1]);
        assert_eq!(exact.fault_sequence(NodeId(1)), Some(11));

        let mut mismatched_driver = exact.clone();
        mismatched_driver.id.driver = NodeId(2);
        assert!(mismatched_driver.validate().is_err());

        let mut mismatched_boot = exact.clone();
        mismatched_boot.leader_proof.owner.boot_id = Uuid::new_v4();
        assert!(mismatched_boot.validate().is_err());

        let mut empty = exact.clone();
        empty.faults.clear();
        assert!(empty.validate().is_err());

        let mut zero = exact.clone();
        zero.faults[0].sequence = 0;
        assert!(zero.validate().is_err());

        let mut duplicate = exact;
        duplicate.faults.push(duplicate.faults[0]);
        assert!(duplicate.validate().is_err());
    }

    #[tokio::test]
    async fn fault_clear_never_overwrites_a_newer_local_sequence() {
        let c = ctl(1, vec![]);
        c.report_fault(12).await.unwrap();

        assert!(!c.clear_fault_report(11).await.unwrap());
        assert_eq!(c.read_fault_reports().await.unwrap(), vec![(NodeId(1), 12)]);

        assert!(c.clear_fault_report(12).await.unwrap());
        assert!(c.clear_fault_report(12).await.unwrap());
        assert_eq!(c.read_fault_reports().await.unwrap(), vec![(NodeId(1), 0)]);
    }

    #[tokio::test]
    async fn recovery_start_and_clear_require_the_identical_prepared_round() {
        let c = ctl(1, vec![]);
        c.publish_recovery_incarnation().await.unwrap();
        let (_authority, proof) = install_recovery_authority(&c, 1_000).await;
        let round = recovery_round(&c, 11, &proof, &[1]);
        let other = recovery_round(&c, 12, &proof, &[1]);
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
        let (_authority, local_proof) = install_recovery_authority(&c, 1_000).await;
        let stale_proof = test_leader_proof(1, Uuid::from_u128(99), 99);
        let local = recovery_round(&c, 17, &local_proof, &[2]);
        let stale = recovery_round(&c, 99, &stale_proof, &[1, 2]);
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
        let (_authority, _proof) = install_recovery_authority(&c, 1_000).await;
        kv.seed(self_id, "control:recover", "not-json".into());

        let error = c.observe_recover().await.unwrap_err();
        assert!(error.contains("invalid recovery announcement"), "{error}");
    }

    #[tokio::test]
    async fn recovered_ack_binds_the_start_target() {
        let c = ctl(1, vec![]);
        c.publish_recovery_incarnation().await.unwrap();
        let proof = test_leader_proof(1, c.recovery_incarnation(), 1);
        let round = recovery_round(&c, 21, &proof, &[1]);
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
    async fn release_terminal_is_authoritative_and_hint_is_retirable() {
        let c = ctl(1, vec![]);
        c.publish_recovery_incarnation().await.unwrap();
        let (_authority, proof) = install_recovery_authority(&c, 1_000).await;
        let round = recovery_round(&c, 31, &proof, &[1]);
        c.report_fault(31).await.unwrap();
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
        c.announce_release_ready(&release).await.unwrap();
        assert_eq!(
            c.read_release_ready(&release).await.unwrap(),
            ReleaseReadyStatus::Complete
        );
        let ReleaseCommitStatus::Committed { terminal } =
            c.try_commit_recover_release(&release).await.unwrap()
        else {
            panic!("exact readiness must commit the pending Release");
        };
        assert_eq!(terminal.phase, RecoverPhase::ReleaseCommitted { epoch: 8 });
        assert_eq!(c.observe_recover().await.unwrap(), Some(release.clone()));
        assert_eq!(
            c.observe_committed_recover_release(&round, 8)
                .await
                .unwrap(),
            Some(terminal)
        );
        assert!(!c.clear_recover(&round).await.unwrap());
        assert!(c
            .retire_committed_recover_release_hint(&round, 8)
            .await
            .unwrap());
        assert_eq!(c.observe_recover().await.unwrap(), None);
    }

    #[tokio::test]
    async fn release_commit_requires_the_exact_compact_ready_roster() {
        let (controller, kv, release, remote) = two_owner_pending_release().await;
        controller.announce_release_ready(&release).await.unwrap();

        assert_eq!(
            controller.read_release_ready(&release).await.unwrap(),
            ReleaseReadyStatus::Pending {
                missing: vec![NodeId(remote.node_id)]
            }
        );
        kv.seed(
            NodeId(99),
            RELEASE_READY_ACK_KEY,
            "unrelated malformed value".into(),
        );
        assert!(matches!(
            controller
                .try_commit_recover_release(&release)
                .await
                .unwrap(),
            ReleaseCommitStatus::Pending { .. }
        ));

        seed_release_ready(&kv, remote, &release);
        assert_eq!(
            controller.read_release_ready(&release).await.unwrap(),
            ReleaseReadyStatus::Complete
        );
        let ReleaseCommitStatus::Committed { terminal } = controller
            .try_commit_recover_release(&release)
            .await
            .unwrap()
        else {
            panic!("the exact compact readiness roster must commit");
        };
        assert_eq!(terminal.phase, RecoverPhase::ReleaseCommitted { epoch: 8 });
        let local_ack = kv
            .read_from(NodeId(1), RELEASE_READY_ACK_KEY)
            .await
            .unwrap();
        assert!(local_ack.len() < 512);
        assert!(!local_ack.contains("assignment_fence"));
        assert!(!local_ack.contains("faults"));
    }

    #[tokio::test]
    async fn restarted_ready_owner_cannot_commit_its_old_process_vote() {
        let (controller, kv, release, remote) = two_owner_pending_release().await;
        controller.announce_release_ready(&release).await.unwrap();
        seed_release_ready(&kv, remote, &release);
        kv.seed(
            NodeId(remote.node_id),
            RECOVERY_INCARNATION_KEY,
            Uuid::new_v4().to_string(),
        );

        let RecoveryControlError::Superseded(reason) = controller
            .try_commit_recover_release(&release)
            .await
            .unwrap_err()
        else {
            panic!("a restarted owner must invalidate its old process vote");
        };
        assert!(reason.contains("process-incarnation roster changed"));
        assert_eq!(controller.observe_recover().await.unwrap(), Some(release));
    }

    #[tokio::test]
    async fn fail_once_ready_read_retries_the_same_pending_release() {
        let (controller, kv, release) = faulty_single_owner_pending_release().await;
        kv.fail_next_ready_reads(1);

        assert!(controller
            .try_commit_recover_release(&release)
            .await
            .is_err());
        assert_eq!(
            controller.observe_recover().await.unwrap(),
            Some(release.clone())
        );
        assert!(matches!(
            controller
                .try_commit_recover_release(&release)
                .await
                .unwrap(),
            ReleaseCommitStatus::Committed { .. }
        ));
    }

    #[tokio::test]
    async fn persistent_ready_read_failure_leaves_the_release_pending_at_deadline() {
        let (controller, kv, release) = faulty_single_owner_pending_release().await;
        kv.fail_next_ready_reads(usize::MAX);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(25);

        while tokio::time::Instant::now() < deadline {
            assert!(controller
                .try_commit_recover_release(&release)
                .await
                .is_err());
            tokio::task::yield_now().await;
        }

        assert_eq!(controller.observe_recover().await.unwrap(), Some(release));
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
        let (authority, proof) = install_recovery_authority(&controller, 1_000).await;
        let round = recovery_round(&controller, 51, &proof, &[1]);
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
        replacement.set_leader_lease_store(authority);
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
    async fn delayed_old_phase_cannot_clobber_same_process_new_leader_term() {
        use super::super::{LeaderLeaseOwner, LeaseDeadline, LeaseOutcome};

        let node = NodeId(1);
        let delayed = Arc::new(DelayedRecoveryKv::new(node));
        let recovery_kv: Arc<dyn ClusterKv> = delayed.clone();
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(node, recovery_kv, None, members_rx));
        controller.publish_recovery_incarnation().await.unwrap();

        let authority = Arc::new(super::super::LeaderLeaseStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            1,
        ));
        let owner = LeaderLeaseOwner {
            node,
            boot: controller.recovery_incarnation(),
            process_term: 1,
        };
        let LeaseOutcome::Acquired(old_lease) = authority.try_acquire(&owner, 0).await.unwrap()
        else {
            panic!("empty recovery authority must be acquired");
        };
        let (lease_tx, lease_rx) = watch::channel(Some(old_lease.clone()));
        let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(10)));
        controller.set_process_lease_deadline(Arc::clone(&deadline));
        controller
            .set_leader_lease_watch(lease_rx, owner.clone(), deadline)
            .unwrap();
        controller.set_leader_lease_store(Arc::clone(&authority));

        let old_proof = old_lease.proof();
        let old_round = recovery_round(&controller, 61, &old_proof, &[1]);
        controller.publish_checkpoint_assignment_fence(Some(old_round.assignment_fence.clone()));
        controller
            .announce_recover_prepare(&old_round)
            .await
            .unwrap();

        delayed.block_next_recovery_write();
        let stale_start = {
            let controller = Arc::clone(&controller);
            let round = old_round.clone();
            tokio::spawn(async move { controller.announce_recover_start(&round, 4).await })
        };
        delayed.wait_until_blocked().await;

        let rival = LeaderLeaseOwner {
            node: NodeId(2),
            boot: Uuid::new_v4(),
            process_term: 1,
        };
        let rival_observation = authority.observe_rival(&rival, &old_lease).unwrap();
        tokio::time::sleep(Duration::from_millis(3)).await;
        let LeaseOutcome::Acquired(rival_lease) = authority
            .try_takeover(&rival, &rival_observation, 10)
            .await
            .unwrap()
        else {
            panic!("rival must take over the expired old term");
        };
        let return_observation = authority.observe_rival(&owner, &rival_lease).unwrap();
        tokio::time::sleep(Duration::from_millis(3)).await;
        let LeaseOutcome::Acquired(current_lease) = authority
            .try_takeover(&owner, &return_observation, 20)
            .await
            .unwrap()
        else {
            panic!("original process must acquire a higher leader term");
        };
        assert_eq!(current_lease.owner, old_lease.owner);
        assert!(current_lease.token > old_lease.token);
        lease_tx.send_replace(Some(current_lease.clone()));

        let current_round = recovery_round(&controller, 62, &current_lease.proof(), &[1]);
        controller
            .publish_checkpoint_assignment_fence(Some(current_round.assignment_fence.clone()));
        let current_prepare = {
            let controller = Arc::clone(&controller);
            let round = current_round.clone();
            tokio::spawn(async move { controller.announce_recover_prepare(&round).await })
        };
        tokio::task::yield_now().await;
        assert!(
            !current_prepare.is_finished(),
            "the replacement phase must serialize behind the in-flight old write"
        );

        delayed.release_blocked_write();
        let stale_error = stale_start.await.unwrap().unwrap_err();
        assert!(
            stale_error.contains("proof is no longer live at Start read-back"),
            "{stale_error}"
        );
        current_prepare.await.unwrap().unwrap();
        assert_eq!(
            controller.observe_recover().await.unwrap(),
            Some(RecoveryAnnouncement {
                round: current_round,
                phase: RecoverPhase::Prepare,
            })
        );
    }

    #[tokio::test]
    async fn committed_release_terminal_survives_leader_takeover() {
        use super::super::{LeaderLeaseOwner, LeaseDeadline, LeaseOutcome};

        let node = NodeId(1);
        let delayed = Arc::new(DelayedRecoveryKv::new(node));
        let recovery_kv: Arc<dyn ClusterKv> = delayed.clone();
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(node, recovery_kv, None, members_rx));
        controller.publish_recovery_incarnation().await.unwrap();
        let authority = Arc::new(super::super::LeaderLeaseStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            1,
        ));
        let owner = LeaderLeaseOwner {
            node,
            boot: controller.recovery_incarnation(),
            process_term: 1,
        };
        let LeaseOutcome::Acquired(old_lease) = authority.try_acquire(&owner, 0).await.unwrap()
        else {
            panic!("empty recovery authority must be acquired");
        };
        let (_lease_tx, lease_rx) = watch::channel(Some(old_lease.clone()));
        let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(10)));
        controller.set_process_lease_deadline(Arc::clone(&deadline));
        controller
            .set_leader_lease_watch(lease_rx, owner, deadline)
            .unwrap();
        controller.set_leader_lease_store(Arc::clone(&authority));

        let round = recovery_round(&controller, 63, &old_lease.proof(), &[1]);
        controller.report_fault(63).await.unwrap();
        controller.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
        controller.announce_recover_prepare(&round).await.unwrap();
        controller.announce_recover_start(&round, 5).await.unwrap();
        controller
            .announce_recover_release(&round, 5)
            .await
            .unwrap();
        let pending = RecoveryAnnouncement {
            round,
            phase: RecoverPhase::Release { epoch: 5 },
        };
        controller.announce_release_ready(&pending).await.unwrap();

        let ReleaseCommitStatus::Committed { terminal } = controller
            .try_commit_recover_release(&pending)
            .await
            .unwrap()
        else {
            panic!("exact readiness must commit the authority terminal");
        };
        let raw = delayed.read_from(node, "control:recover").await.unwrap();
        assert!(raw.contains("\"Release\""));
        assert!(!raw.contains("ReleaseCommitted"));

        let rival = LeaderLeaseOwner {
            node: NodeId(2),
            boot: Uuid::new_v4(),
            process_term: 1,
        };
        let committed_lease = authority.load().await.unwrap().unwrap();
        let observation = authority.observe_rival(&rival, &committed_lease).unwrap();
        tokio::time::sleep(Duration::from_millis(3)).await;
        let LeaseOutcome::Acquired(rival_lease) = authority
            .try_takeover(&rival, &observation, 10)
            .await
            .unwrap()
        else {
            panic!("rival must take over the expired release driver");
        };

        let (_replacement_tx, replacement_rx) = watch::channel(Vec::new());
        let replacement = ClusterController::new(rival.node, delayed.clone(), None, replacement_rx);
        let (_rival_lease_tx, rival_lease_rx) = watch::channel(Some(rival_lease));
        let rival_deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(10)));
        replacement.set_process_lease_deadline(Arc::clone(&rival_deadline));
        replacement
            .set_leader_lease_watch(rival_lease_rx, rival, rival_deadline)
            .unwrap();
        replacement.set_leader_lease_store(authority);
        assert_eq!(
            replacement
                .observe_committed_recover_release(&pending.round, 5)
                .await
                .unwrap(),
            Some(terminal)
        );
        assert_eq!(
            delayed.read_from(node, "control:recover").await.unwrap(),
            raw
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
        let proof = test_leader_proof(1, old.recovery_incarnation(), 1);
        let old_round = recovery_round(&old, 41, &proof, &[1]);

        let (_new_tx, new_rx) = watch::channel(Vec::new());
        let replacement = ClusterController::new(node, kv, None, new_rx);
        replacement.publish_recovery_incarnation().await.unwrap();

        let error = old.announce_stopped(&old_round).await.unwrap_err();
        assert!(error.contains("superseded local process"), "{error}");
    }
}
