//! Cross-instance barrier protocol. Direct gRPC leader-to-follower calls
//! under `cluster`, falling back to gossip-KV announce/ack/poll.

#[cfg(feature = "cluster")]
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};

use crate::checkpoint::CheckpointWatermark;
use crate::cluster::discovery::NodeId;
#[cfg(feature = "cluster")]
use crate::cluster::discovery::{NodeInfo, NodeState};
use crate::state::{CheckpointAttempt, CheckpointAttemptRelation};
#[cfg(feature = "cluster")]
use tokio::sync::watch;

/// KV key for the leader's barrier announcement.
pub const ANNOUNCEMENT_KEY: &str = "control:barrier";

/// KV key for a follower's barrier ack.
pub const ACK_KEY: &str = "control:barrier-ack";

/// Gossip KV key used by follower barrier servers to advertise their bound address.
#[cfg(feature = "cluster")]
pub const BARRIER_ADDR_KEY: &str = "barrier:addr";

/// Upper bound for a non-Prepare phase notification round. The durable KV
/// announcement is authoritative; direct gRPC delivery is only the low-latency
/// path and must never hold the checkpoint coordinator indefinitely.
#[cfg(feature = "cluster")]
const PHASE_RPC_TIMEOUT: Duration = Duration::from_secs(3);

/// Barrier phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    /// Align the shuffle, capture state locally, ack. The durable tail
    /// (sink pre-commit, manifest, uploads) runs after the ack.
    Prepare,
    /// Every node has aligned + captured this epoch (full-membership
    /// capture quorum). Pipelines may resume the next epoch; the epoch
    /// is NOT yet restorable.
    Aligned,
    /// Durability gate passed; commit sinks. The epoch is restorable.
    Commit,
    /// Prepare failed; roll back.
    Abort,
}

const fn is_terminal_phase(phase: Phase) -> bool {
    matches!(phase, Phase::Commit | Phase::Abort)
}

/// Leader-written barrier announcement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarrierAnnouncement {
    /// Monotonic epoch id.
    pub epoch: u64,
    /// Coordinator-assigned checkpoint id.
    pub checkpoint_id: u64,
    /// Exact clustered assignment cut captured when this attempt was admitted. Required on every
    /// clustered `Prepare` and retained on terminal phases for exact follower validation.
    #[serde(default)]
    pub assignment_fence: Option<super::CheckpointAssignmentFence>,
    /// Exact durable leader term that issued this announcement. Clustered reversible phases
    /// (`Prepare` and `Aligned`) are rejected unless this proof is present and still live.
    /// Terminal notifications carry it only for diagnostics; their authority comes from the
    /// immutable durable checkpoint outcome.
    #[serde(default)]
    pub leader_proof: Option<super::LeaderProof>,
    /// Phase this announcement signals.
    pub phase: Phase,
    /// Reserved for unaligned/other flags.
    pub flags: u64,
    /// Cluster-wide minimum watermark at announce time: the `min`
    /// across every live node's local watermark, computed by the
    /// leader from follower acks (see [`BarrierAck::watermark`])
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

fn announcement_attempt(ann: &BarrierAnnouncement) -> CheckpointAttempt {
    CheckpointAttempt::new(ann.epoch, ann.checkpoint_id)
}

fn same_announcement_identity(left: &BarrierAnnouncement, right: &BarrierAnnouncement) -> bool {
    announcement_attempt(left) == announcement_attempt(right)
        && left.assignment_fence == right.assignment_fence
        && left.leader_proof == right.leader_proof
        && left.flags == right.flags
}

/// Merge direct deliveries without allowing a delayed non-terminal phase to regress the same
/// exact barrier identity. Conflicting attempts, certificates, or terminal phases fail closed.
#[cfg(feature = "cluster")]
fn merge_direct_announcement(
    current: BarrierAnnouncement,
    incoming: BarrierAnnouncement,
) -> Result<BarrierAnnouncement, String> {
    match announcement_attempt(&incoming).relation_to(announcement_attempt(&current)) {
        CheckpointAttemptRelation::Newer => Ok(incoming),
        CheckpointAttemptRelation::Older => Ok(current),
        CheckpointAttemptRelation::Conflict => Err(format!(
            "conflicting direct barrier attempts: retained ({}, {}), incoming ({}, {})",
            current.epoch, current.checkpoint_id, incoming.epoch, incoming.checkpoint_id
        )),
        CheckpointAttemptRelation::Exact => {
            if !same_announcement_identity(&current, &incoming) {
                return Err(format!(
                    "conflicting direct barrier certificates for exact attempt ({}, {})",
                    current.epoch, current.checkpoint_id
                ));
            }

            if is_terminal_phase(current.phase) && is_terminal_phase(incoming.phase) {
                return if current.phase == incoming.phase {
                    Ok(current)
                } else {
                    Err(format!(
                        "conflicting direct terminal phases for exact attempt ({}, {})",
                        current.epoch, current.checkpoint_id
                    ))
                };
            }
            if is_terminal_phase(current.phase)
                || (current.phase == Phase::Aligned && incoming.phase == Phase::Prepare)
            {
                Ok(current)
            } else {
                Ok(incoming)
            }
        }
    }
}

/// Merge the low-latency direct value with the durable leader announcement.
/// At the same exact attempt a terminal KV value is the decision authority;
/// otherwise phase progress is monotonic while gossip catches up.
fn merge_observed_announcement(
    grpc: BarrierAnnouncement,
    durable: BarrierAnnouncement,
) -> Result<BarrierAnnouncement, String> {
    match announcement_attempt(&durable).relation_to(announcement_attempt(&grpc)) {
        CheckpointAttemptRelation::Newer => Ok(durable),
        CheckpointAttemptRelation::Older => Ok(grpc),
        CheckpointAttemptRelation::Conflict => Err(format!(
            "conflicting direct and durable barrier attempts: direct ({}, {}), durable ({}, {})",
            grpc.epoch, grpc.checkpoint_id, durable.epoch, durable.checkpoint_id
        )),
        CheckpointAttemptRelation::Exact => {
            if !same_announcement_identity(&grpc, &durable) {
                return Err(format!(
                    "conflicting direct and durable certificates for exact attempt ({}, {})",
                    grpc.epoch, grpc.checkpoint_id
                ));
            }
            if is_terminal_phase(durable.phase) {
                Ok(durable)
            } else if is_terminal_phase(grpc.phase)
                || (grpc.phase == Phase::Aligned && durable.phase == Phase::Prepare)
            {
                Ok(grpc)
            } else {
                Ok(durable)
            }
        }
    }
}

/// Merge per-node durable histories for leader reclamation. Nodes may legitimately lag at an
/// earlier phase of the same certified attempt, but they may not disagree about its certificate,
/// or terminal outcome.
fn merge_scanned_announcement(
    current: BarrierAnnouncement,
    incoming: BarrierAnnouncement,
) -> Result<BarrierAnnouncement, String> {
    match announcement_attempt(&incoming).relation_to(announcement_attempt(&current)) {
        CheckpointAttemptRelation::Newer => Ok(incoming),
        CheckpointAttemptRelation::Older => Ok(current),
        CheckpointAttemptRelation::Conflict => Err(format!(
            "conflicting announced checkpoint history: ({}, {}) versus ({}, {})",
            current.epoch, current.checkpoint_id, incoming.epoch, incoming.checkpoint_id
        )),
        CheckpointAttemptRelation::Exact => {
            if !same_announcement_identity(&current, &incoming) {
                return Err(format!(
                    "conflicting durable barrier certificates for exact attempt ({}, {})",
                    current.epoch, current.checkpoint_id
                ));
            }
            if current.phase == incoming.phase {
                // Reconciliation may re-announce the same terminal decision without the earlier
                // diagnostic watermark. It does not change allocation or recovery authority.
                return Ok(current);
            }
            if is_terminal_phase(current.phase) && is_terminal_phase(incoming.phase) {
                return Err(format!(
                    "conflicting durable terminal phases for exact attempt ({}, {})",
                    current.epoch, current.checkpoint_id
                ));
            }
            if is_terminal_phase(current.phase)
                || (current.phase == Phase::Aligned && incoming.phase == Phase::Prepare)
            {
                Ok(current)
            } else {
                Ok(incoming)
            }
        }
    }
}

fn validate_scanned_announcements(
    mut announcements: Vec<BarrierAnnouncement>,
) -> Result<Option<BarrierAnnouncement>, String> {
    // This sort groups exact attempts for validation; ordering authority remains `relation_to`.
    announcements
        .sort_unstable_by_key(|announcement| (announcement.epoch, announcement.checkpoint_id));
    announcements
        .into_iter()
        .try_fold(None, |highest, announcement| {
            Ok(Some(match highest {
                None => announcement,
                Some(current) => merge_scanned_announcement(current, announcement)?,
            }))
        })
}

/// Follower ack. `ok = false` forces the leader to abort instead of wait.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarrierAck {
    /// Epoch being acknowledged.
    pub epoch: u64,
    /// Exact coordinator-assigned checkpoint id being acknowledged.
    #[serde(default)]
    pub checkpoint_id: u64,
    /// SHA-256 binding of the announcement's assignment certificate.
    #[serde(default)]
    pub assignment_digest: Option<[u8; 32]>,
    /// `false` = snapshot failed locally; leader should abort.
    pub ok: bool,
    /// Free-text error; populated when `ok = false`.
    pub error: Option<String>,
    /// Follower event-time state at ack time. Uninitialized inputs block advancement; only
    /// explicitly idle inputs are excluded from the active cluster minimum.
    #[serde(default)]
    pub watermark: CheckpointWatermark,
}

/// Outcome of `wait_for_quorum`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuorumOutcome {
    /// All expected peers acked with `ok = true`.
    Reached {
        /// Peers that acked successfully.
        acks: Vec<NodeId>,
        /// Safe aggregate across all required followers.
        follower_watermark: CheckpointWatermark,
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
    /// Write with transport failure reporting when the backend supports it.
    ///
    /// Fast gossip implementations may use the default because their write API has no result.
    /// Durable control implementations override this so recovery never treats a dropped write as
    /// successful.
    ///
    /// # Errors
    /// Durable implementations return a transport or storage error when the value was not
    /// accepted by their authority.
    async fn write_checked(&self, key: &str, value: String) -> Result<(), String> {
        self.write(key, value).await;
        Ok(())
    }
    /// Read `key` from `who`'s slot.
    async fn read_from(&self, who: NodeId, key: &str) -> Option<String>;
    /// Read with transport failure reporting when the backend supports it.
    ///
    /// # Errors
    /// Durable implementations return a transport or storage error. A genuinely absent key is
    /// `Ok(None)`.
    async fn read_from_checked(&self, who: NodeId, key: &str) -> Result<Option<String>, String> {
        Ok(self.read_from(who, key).await)
    }
    /// Every visible instance's value for `key`.
    async fn scan(&self, key: &str) -> Vec<(NodeId, String)>;
    /// Scan with transport failure reporting when the backend supports it.
    ///
    /// # Errors
    /// Durable implementations fail the whole scan when any visible participant cannot be read.
    async fn scan_checked(&self, key: &str) -> Result<Vec<(NodeId, String)>, String> {
        Ok(self.scan(key).await)
    }
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

#[cfg(feature = "cluster")]
#[allow(
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::missing_const_for_fn,
    clippy::must_use_candidate,
    clippy::too_many_lines,
    missing_docs
)]
pub(crate) mod barrier_v1 {
    tonic::include_proto!("laminar.barrier.v1");
}

#[cfg(feature = "cluster")]
type BarrierFlavor = crossfire::mpsc::Array<BarrierAnnouncement>;

/// Full retry identity for direct barrier traffic. `CheckpointAttempt` alone is
/// insufficient because an assignment rotation can leave delayed traffic with
/// the same epoch/checkpoint pair but a different participant cut.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BarrierIdentity {
    attempt: CheckpointAttempt,
    assignment_digest: Option<[u8; 32]>,
}

#[cfg(feature = "cluster")]
impl BarrierIdentity {
    fn from_announcement(ann: &BarrierAnnouncement) -> Self {
        Self {
            attempt: CheckpointAttempt::new(ann.epoch, ann.checkpoint_id),
            assignment_digest: ann
                .assignment_fence
                .as_ref()
                .map(super::CheckpointAssignmentFence::digest),
        }
    }

    const fn from_ack(ack: &BarrierAck) -> Self {
        Self {
            attempt: CheckpointAttempt::new(ack.epoch, ack.checkpoint_id),
            assignment_digest: ack.assignment_digest,
        }
    }
}

#[cfg(feature = "cluster")]
const MAX_RETAINED_BARRIER_IDENTITIES: usize = 256;
#[cfg(feature = "cluster")]
const MAX_PREPARE_WAITERS_PER_IDENTITY: usize = 32;
#[cfg(feature = "cluster")]
const PREPARE_RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// Follower-side state shared by the Prepare RPC and the local checkpoint
/// completion path. One exact Prepare may have several retrying RPC waiters;
/// they must all receive the same immutable local result.
#[cfg(feature = "cluster")]
#[derive(Default)]
struct PrepareAckState {
    pending: FxHashMap<BarrierIdentity, Vec<PendingPrepareWaiter>>,
    completed: FxHashMap<BarrierIdentity, BarrierAck>,
    received_at: FxHashMap<BarrierIdentity, std::time::Instant>,
    next_waiter_id: u64,
}

#[cfg(feature = "cluster")]
struct PendingPrepareWaiter {
    id: u64,
    response: tokio::sync::oneshot::Sender<BarrierAck>,
}

/// Cancellation-safe removal for one Prepare RPC registration. Tonic may drop
/// a handler future when the client deadline expires, so cleanup cannot rely on
/// reaching the explicit timeout branch below.
#[cfg(feature = "cluster")]
struct PrepareWaiterRegistration {
    state: Arc<parking_lot::Mutex<PrepareAckState>>,
    identity: BarrierIdentity,
    waiter_id: u64,
}

#[cfg(feature = "cluster")]
impl Drop for PrepareWaiterRegistration {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        let remove_entry = state
            .pending
            .get_mut(&self.identity)
            .is_some_and(|waiters| {
                waiters.retain(|waiter| waiter.id != self.waiter_id);
                waiters.is_empty()
            });
        if remove_entry {
            state.pending.remove(&self.identity);
        }
    }
}

#[cfg(feature = "cluster")]
impl PrepareAckState {
    fn trim_bounded<K, V>(entries: &mut FxHashMap<K, V>)
    where
        K: Copy + Eq + std::hash::Hash,
    {
        while entries.len() > MAX_RETAINED_BARRIER_IDENTITIES {
            let Some(victim) = entries.keys().next().copied() else {
                break;
            };
            entries.remove(&victim);
        }
    }

    fn next_waiter_id(&mut self) -> u64 {
        loop {
            self.next_waiter_id = self.next_waiter_id.wrapping_add(1);
            let candidate = self.next_waiter_id;
            if self
                .pending
                .values()
                .flatten()
                .all(|waiter| waiter.id != candidate)
            {
                return candidate;
            }
        }
    }

    fn record_ack(&mut self, identity: BarrierIdentity, ack: &BarrierAck) -> BarrierAck {
        use std::collections::hash_map::Entry;

        let cached = match self.completed.entry(identity) {
            Entry::Vacant(entry) => entry.insert(ack.clone()),
            Entry::Occupied(mut entry) => {
                if entry.get().ok && !ack.ok {
                    entry.insert(ack.clone());
                }
                entry.into_mut()
            }
        }
        .clone();
        Self::trim_bounded(&mut self.completed);
        cached
    }

    fn record_receipt(&mut self, identity: BarrierIdentity) {
        self.received_at
            .entry(identity)
            .or_insert_with(std::time::Instant::now);
        Self::trim_bounded(&mut self.received_at);
    }
}

/// Per-peer barrier gRPC client pool, keyed by `NodeId`. Entries are evicted on
/// RPC failure so a restarted/moved peer is re-resolved on the next round.
#[cfg(feature = "cluster")]
type BarrierClientPool = Arc<
    parking_lot::Mutex<
        FxHashMap<
            NodeId,
            barrier_v1::barrier_sync_client::BarrierSyncClient<tonic::transport::Channel>,
        >,
    >,
>;

#[cfg(feature = "cluster")]
struct GrpcState {
    /// Latest gRPC-delivered announcement, fed in arrival order by the
    /// relay task draining the incoming queue. Latest-wins semantics
    /// (matching the gossip-KV fallback) so concurrent observers — the
    /// pipeline's resume gate and the background durable tail — never
    /// steal announcements from each other.
    latest_rx: watch::Receiver<Option<BarrierAnnouncement>>,
    merge_error: Arc<parking_lot::Mutex<Option<String>>>,
    prepare_acks: Arc<parking_lot::Mutex<PrepareAckState>>,
    clients: BarrierClientPool,
    server_handle: Arc<parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    relay_handle: Arc<parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    advertise_addr: String,
}

#[cfg(feature = "cluster")]
type ActiveLeaderState = Option<(NodeId, watch::Receiver<Vec<NodeInfo>>, Arc<AtomicBool>)>;

#[cfg(feature = "cluster")]
struct GrpcBarrierServer {
    incoming_tx: crossfire::MAsyncTx<BarrierFlavor>,
    prepare_acks: Arc<parking_lot::Mutex<PrepareAckState>>,
    leader_lease_store: Arc<parking_lot::Mutex<Option<Arc<super::LeaderLeaseStore>>>>,
}

#[cfg(feature = "cluster")]
#[derive(Clone, Default)]
struct WireAssignmentFence {
    version: u64,
    vnode_count: u32,
    map_digest: Vec<u8>,
    participants: Vec<barrier_v1::CheckpointParticipant>,
}

#[cfg(feature = "cluster")]
fn leader_proof_from_wire(
    proof: Option<barrier_v1::LeaderProof>,
) -> Result<Option<super::LeaderProof>, tonic::Status> {
    proof
        .map(|proof| {
            let boot = uuid::Uuid::from_slice(&proof.boot_id).map_err(|_| {
                tonic::Status::invalid_argument(
                    "Leader proof boot identity must contain exactly 16 bytes",
                )
            })?;
            if proof.node_id == 0
                || boot.is_nil()
                || proof.process_term == 0
                || proof.fencing_token == 0
            {
                return Err(tonic::Status::invalid_argument(
                    "Leader proof identity and fencing token must be nonzero",
                ));
            }
            Ok(super::LeaderProof {
                owner: crate::checkpoint::LeaderProofOwner {
                    node_id: proof.node_id,
                    boot_id: boot,
                    process_term: proof.process_term,
                },
                fencing_token: proof.fencing_token,
            })
        })
        .transpose()
}

#[cfg(feature = "cluster")]
fn leader_proof_to_wire(proof: Option<&super::LeaderProof>) -> Option<barrier_v1::LeaderProof> {
    proof.map(|proof| barrier_v1::LeaderProof {
        node_id: proof.owner.node_id,
        boot_id: proof.owner.boot_id.as_bytes().to_vec(),
        process_term: proof.owner.process_term,
        fencing_token: proof.fencing_token,
    })
}

#[cfg(feature = "cluster")]
fn assignment_fence_from_wire(
    version: u64,
    vnode_count: u32,
    map_digest: Vec<u8>,
    participants: Vec<barrier_v1::CheckpointParticipant>,
) -> Result<Option<super::CheckpointAssignmentFence>, tonic::Status> {
    if version == 0 && vnode_count == 0 && map_digest.is_empty() && participants.is_empty() {
        return Ok(None);
    }

    let assignment_digest: [u8; 32] = map_digest.try_into().map_err(|_| {
        tonic::Status::invalid_argument(
            "Checkpoint assignment map digest must contain exactly 32 bytes",
        )
    })?;
    let participants = participants
        .into_iter()
        .map(|participant| {
            let boot_incarnation =
                uuid::Uuid::from_slice(&participant.boot_incarnation).map_err(|_| {
                    tonic::Status::invalid_argument(
                        "Checkpoint participant incarnation must contain exactly 16 bytes",
                    )
                })?;
            Ok(super::CheckpointParticipant {
                node_id: participant.node_id,
                boot_incarnation,
            })
        })
        .collect::<Result<Vec<_>, tonic::Status>>()?;
    let fence = super::CheckpointAssignmentFence {
        assignment_version: version,
        vnode_count,
        assignment_digest,
        participants,
    };
    if !fence.is_canonical() {
        return Err(tonic::Status::invalid_argument(
            "Non-canonical checkpoint assignment certificate",
        ));
    }
    Ok(Some(fence))
}

#[cfg(feature = "cluster")]
fn assignment_fence_to_wire(
    fence: Option<&super::CheckpointAssignmentFence>,
) -> WireAssignmentFence {
    fence.map_or_else(WireAssignmentFence::default, |fence| WireAssignmentFence {
        version: fence.assignment_version,
        vnode_count: fence.vnode_count,
        map_digest: fence.assignment_digest.to_vec(),
        participants: fence
            .participants
            .iter()
            .map(|participant| barrier_v1::CheckpointParticipant {
                node_id: participant.node_id,
                boot_incarnation: participant.boot_incarnation.as_bytes().to_vec(),
            })
            .collect(),
    })
}

#[cfg(feature = "cluster")]
fn checkpoint_watermark_from_wire(
    status: i32,
    active_watermark_ms: Option<i64>,
) -> Result<CheckpointWatermark, String> {
    use barrier_v1::CheckpointWatermarkStatus as WireStatus;

    let wire_status = WireStatus::try_from(status)
        .map_err(|_| format!("unknown checkpoint watermark status {status}"))?;
    let watermark = match (wire_status, active_watermark_ms) {
        (WireStatus::CheckpointWatermarkUninitialized, None) => CheckpointWatermark::Uninitialized,
        (WireStatus::CheckpointWatermarkIdle, None) => CheckpointWatermark::Idle,
        (WireStatus::CheckpointWatermarkActive, Some(value)) => CheckpointWatermark::Active(value),
        _ => {
            return Err(format!(
                "invalid checkpoint watermark status {status} with active value {active_watermark_ms:?}"
            ));
        }
    };
    watermark.validate()?;
    Ok(watermark)
}

#[cfg(feature = "cluster")]
fn grpc_ack(ack: BarrierAck) -> barrier_v1::Ack {
    use barrier_v1::CheckpointWatermarkStatus as WireStatus;

    let (watermark_status, local_watermark_ms) = match ack.watermark {
        CheckpointWatermark::Uninitialized => {
            (WireStatus::CheckpointWatermarkUninitialized as i32, None)
        }
        CheckpointWatermark::Idle => (WireStatus::CheckpointWatermarkIdle as i32, None),
        CheckpointWatermark::Active(value) => {
            (WireStatus::CheckpointWatermarkActive as i32, Some(value))
        }
    };
    barrier_v1::Ack {
        epoch: ack.epoch,
        ok: ack.ok,
        error: ack.error,
        local_watermark_ms,
        checkpoint_id: ack.checkpoint_id,
        assignment_digest: ack
            .assignment_digest
            .map_or_else(Vec::new, |digest| digest.to_vec()),
        watermark_status,
    }
}

#[cfg(feature = "cluster")]
fn validate_phase_ack(ack: &barrier_v1::Ack, ann: &BarrierAnnouncement) -> Result<(), String> {
    let expected_digest = ann
        .assignment_fence
        .as_ref()
        .map(super::CheckpointAssignmentFence::digest);
    if ack.epoch != ann.epoch
        || ack.checkpoint_id != ann.checkpoint_id
        || ack.assignment_digest.as_slice()
            != expected_digest
                .as_ref()
                .map_or(&[][..], <[u8; 32]>::as_slice)
    {
        return Err("Barrier phase acknowledgement identity mismatch".into());
    }
    if !ack.ok {
        return Err(ack
            .error
            .clone()
            .unwrap_or_else(|| "Barrier phase was rejected by follower".into()));
    }
    Ok(())
}

#[cfg(feature = "cluster")]
impl GrpcBarrierServer {
    async fn require_latest_proof(&self, proof: &super::LeaderProof) -> Result<(), tonic::Status> {
        let store = self.leader_lease_store.lock().clone().ok_or_else(|| {
            tonic::Status::failed_precondition("Durable leader lease store is not installed")
        })?;
        let lease = store
            .load()
            .await
            .map_err(|error| {
                tonic::Status::unavailable(format!("Leader lease read failed: {error}"))
            })?
            .ok_or_else(|| tonic::Status::permission_denied("No durable leader lease exists"))?;
        if !lease.matches_proof(proof) {
            return Err(tonic::Status::permission_denied(
                "Leader proof does not match the latest durable lease",
            ));
        }
        Ok(())
    }

    async fn validate_reversible_leader(
        &self,
        proof: Option<barrier_v1::LeaderProof>,
    ) -> Result<super::LeaderProof, tonic::Status> {
        let proof = leader_proof_from_wire(proof)?
            .ok_or_else(|| tonic::Status::permission_denied("Missing durable leader proof"))?;
        self.require_latest_proof(&proof).await?;
        Ok(proof)
    }
}

#[cfg(feature = "cluster")]
#[tonic::async_trait]
impl barrier_v1::barrier_sync_server::BarrierSync for GrpcBarrierServer {
    async fn prepare(
        &self,
        request: tonic::Request<barrier_v1::PrepareRequest>,
    ) -> Result<tonic::Response<barrier_v1::Ack>, tonic::Status> {
        let req = request.into_inner();
        let leader_proof = self
            .validate_reversible_leader(req.leader_proof.clone())
            .await?;
        let attempt = CheckpointAttempt::new(req.epoch, req.checkpoint_id);
        let assignment_fence = assignment_fence_from_wire(
            req.assignment_version,
            req.assignment_vnode_count,
            req.assignment_map_digest,
            req.assignment_participants,
        )?;
        let assignment_digest = assignment_fence
            .as_ref()
            .map(super::CheckpointAssignmentFence::digest);
        let identity = BarrierIdentity {
            attempt,
            assignment_digest,
        };

        let (waiter_id, rx) = {
            let mut state = self.prepare_acks.lock();
            state.record_receipt(identity);
            if let Some(ack) = state.completed.get(&identity) {
                return Ok(tonic::Response::new(grpc_ack(ack.clone())));
            }
            if state.pending.get(&identity).map_or(0, Vec::len) >= MAX_PREPARE_WAITERS_PER_IDENTITY
            {
                return Err(tonic::Status::resource_exhausted(
                    "Too many concurrent retries for one checkpoint Prepare",
                ));
            }
            if !state.pending.contains_key(&identity)
                && state.pending.len() >= MAX_RETAINED_BARRIER_IDENTITIES
            {
                return Err(tonic::Status::resource_exhausted(
                    "Too many concurrent checkpoint Prepare identities",
                ));
            }

            let (tx, rx) = tokio::sync::oneshot::channel::<BarrierAck>();
            let waiter_id = state.next_waiter_id();
            state
                .pending
                .entry(identity)
                .or_default()
                .push(PendingPrepareWaiter {
                    id: waiter_id,
                    response: tx,
                });
            (waiter_id, rx)
        };
        let _registration = PrepareWaiterRegistration {
            state: Arc::clone(&self.prepare_acks),
            identity,
            waiter_id,
        };

        let ann = BarrierAnnouncement {
            epoch: req.epoch,
            checkpoint_id: req.checkpoint_id,
            assignment_fence,
            leader_proof: Some(leader_proof.clone()),
            phase: Phase::Prepare,
            flags: req.flags,
            min_watermark_ms: None,
        };

        if self.incoming_tx.send(ann).await.is_err() {
            return Err(tonic::Status::aborted("Follower coordinator shutdown"));
        }

        match tokio::time::timeout(PREPARE_RPC_TIMEOUT, rx).await {
            Ok(Ok(ack))
                if ack.checkpoint_id == attempt.checkpoint_id
                    && ack.assignment_digest == assignment_digest =>
            {
                self.require_latest_proof(&leader_proof).await?;
                Ok(tonic::Response::new(grpc_ack(ack)))
            }
            Ok(Ok(_)) => Err(tonic::Status::failed_precondition(
                "Follower acknowledgement identity mismatch",
            )),
            Ok(Err(_)) => Err(tonic::Status::internal("Ack sender dropped")),
            Err(_) => Err(tonic::Status::deadline_exceeded(
                "Follower checkpoint prepare timed out",
            )),
        }
    }

    async fn aligned(
        &self,
        request: tonic::Request<barrier_v1::AlignedRequest>,
    ) -> Result<tonic::Response<barrier_v1::Ack>, tonic::Status> {
        let req = request.into_inner();
        let leader_proof = self
            .validate_reversible_leader(req.leader_proof.clone())
            .await?;
        let assignment_fence = assignment_fence_from_wire(
            req.assignment_version,
            req.assignment_vnode_count,
            req.assignment_map_digest,
            req.assignment_participants,
        )?;

        // Unlike Commit/Abort, Aligned is mid-protocol: the epoch's ack
        // bookkeeping stays untouched — only the announcement is relayed
        // so the pipeline's resume gate can release.
        let ann = BarrierAnnouncement {
            epoch: req.epoch,
            checkpoint_id: req.checkpoint_id,
            assignment_fence: assignment_fence.clone(),
            leader_proof: Some(leader_proof),
            phase: Phase::Aligned,
            flags: req.flags,
            min_watermark_ms: req.min_watermark_ms,
        };
        if self.incoming_tx.send(ann).await.is_err() {
            return Err(tonic::Status::aborted("Follower coordinator shutdown"));
        }
        Ok(tonic::Response::new(barrier_v1::Ack {
            epoch: req.epoch,
            ok: true,
            error: None,
            local_watermark_ms: None,
            checkpoint_id: req.checkpoint_id,
            assignment_digest: assignment_fence
                .as_ref()
                .map_or_else(Vec::new, |fence| fence.digest().to_vec()),
            watermark_status: 0,
        }))
    }

    async fn commit(
        &self,
        request: tonic::Request<barrier_v1::CommitRequest>,
    ) -> Result<tonic::Response<barrier_v1::Ack>, tonic::Status> {
        let req = request.into_inner();
        let leader_proof = leader_proof_from_wire(req.leader_proof.clone())?;
        let assignment_fence = assignment_fence_from_wire(
            req.assignment_version,
            req.assignment_vnode_count,
            req.assignment_map_digest,
            req.assignment_participants,
        )?;

        let ann = BarrierAnnouncement {
            epoch: req.epoch,
            checkpoint_id: req.checkpoint_id,
            assignment_fence: assignment_fence.clone(),
            leader_proof,
            phase: Phase::Commit,
            flags: req.flags,
            min_watermark_ms: req.min_watermark_ms,
        };
        if self.incoming_tx.send(ann).await.is_err() {
            return Err(tonic::Status::aborted("Follower coordinator shutdown"));
        }
        Ok(tonic::Response::new(barrier_v1::Ack {
            epoch: req.epoch,
            ok: true,
            error: None,
            local_watermark_ms: None,
            checkpoint_id: req.checkpoint_id,
            assignment_digest: assignment_fence
                .as_ref()
                .map_or_else(Vec::new, |fence| fence.digest().to_vec()),
            watermark_status: 0,
        }))
    }

    async fn abort(
        &self,
        request: tonic::Request<barrier_v1::AbortRequest>,
    ) -> Result<tonic::Response<barrier_v1::Ack>, tonic::Status> {
        let req = request.into_inner();
        let leader_proof = leader_proof_from_wire(req.leader_proof.clone())?;
        let assignment_fence = assignment_fence_from_wire(
            req.assignment_version,
            req.assignment_vnode_count,
            req.assignment_map_digest,
            req.assignment_participants,
        )?;

        let ann = BarrierAnnouncement {
            epoch: req.epoch,
            checkpoint_id: req.checkpoint_id,
            assignment_fence: assignment_fence.clone(),
            leader_proof,
            phase: Phase::Abort,
            flags: req.flags,
            min_watermark_ms: None,
        };
        if self.incoming_tx.send(ann).await.is_err() {
            return Err(tonic::Status::aborted("Follower coordinator shutdown"));
        }
        Ok(tonic::Response::new(barrier_v1::Ack {
            epoch: req.epoch,
            ok: true,
            error: None,
            local_watermark_ms: None,
            checkpoint_id: req.checkpoint_id,
            assignment_digest: assignment_fence
                .as_ref()
                .map_or_else(Vec::new, |fence| fence.digest().to_vec()),
            watermark_status: 0,
        }))
    }
}

#[cfg(feature = "cluster")]
async fn get_barrier_client(
    peer: NodeId,
    pool: &BarrierClientPool,
    kv: &Arc<dyn ClusterKv>,
) -> Option<barrier_v1::barrier_sync_client::BarrierSyncClient<tonic::transport::Channel>> {
    if let Some(client) = pool.lock().get(&peer) {
        return Some(client.clone());
    }

    let addr_str = kv.read_from(peer, BARRIER_ADDR_KEY).await?;
    let endpoint = super::tls::client_endpoint(&addr_str).ok()?;
    let channel = endpoint.connect_lazy();
    let client = barrier_v1::barrier_sync_client::BarrierSyncClient::new(channel);

    pool.lock().insert(peer, client.clone());
    Some(client)
}

/// Fan a non-Prepare phase announcement to one peer over gRPC. A failed
/// RPC evicts the pooled client so the next round re-resolves the peer.
#[cfg(feature = "cluster")]
async fn send_phase_rpc(
    peer: NodeId,
    clients_pool: BarrierClientPool,
    kv: Arc<dyn ClusterKv>,
    ann: BarrierAnnouncement,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    let rpc = match ann.phase {
        Phase::Aligned => "aligned",
        Phase::Commit => "commit",
        Phase::Abort => "abort",
        Phase::Prepare => "prepare",
    };

    let result = tokio::time::timeout_at(deadline, async {
        let mut client = get_barrier_client(peer, &clients_pool, &kv)
            .await
            .ok_or_else(|| format!("failed to get client for peer {}", peer.0))?;
        let request_timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        let assignment = assignment_fence_to_wire(ann.assignment_fence.as_ref());

        match ann.phase {
            Phase::Aligned => {
                let mut req = tonic::Request::new(barrier_v1::AlignedRequest {
                    epoch: ann.epoch,
                    checkpoint_id: ann.checkpoint_id,
                    flags: ann.flags,
                    min_watermark_ms: ann.min_watermark_ms,
                    assignment_version: assignment.version,
                    assignment_participants: assignment.participants,
                    assignment_vnode_count: assignment.vnode_count,
                    assignment_map_digest: assignment.map_digest,
                    leader_proof: leader_proof_to_wire(ann.leader_proof.as_ref()),
                });
                req.set_timeout(request_timeout);
                client
                    .aligned(req)
                    .await
                    .map_err(|e| e.to_string())
                    .and_then(|response| validate_phase_ack(&response.into_inner(), &ann))
            }
            Phase::Commit => {
                let mut req = tonic::Request::new(barrier_v1::CommitRequest {
                    epoch: ann.epoch,
                    checkpoint_id: ann.checkpoint_id,
                    flags: ann.flags,
                    min_watermark_ms: ann.min_watermark_ms,
                    assignment_version: assignment.version,
                    assignment_participants: assignment.participants,
                    assignment_vnode_count: assignment.vnode_count,
                    assignment_map_digest: assignment.map_digest,
                    leader_proof: leader_proof_to_wire(ann.leader_proof.as_ref()),
                });
                req.set_timeout(request_timeout);
                client
                    .commit(req)
                    .await
                    .map_err(|e| e.to_string())
                    .and_then(|response| validate_phase_ack(&response.into_inner(), &ann))
            }
            Phase::Abort => {
                let mut req = tonic::Request::new(barrier_v1::AbortRequest {
                    epoch: ann.epoch,
                    checkpoint_id: ann.checkpoint_id,
                    flags: ann.flags,
                    assignment_version: assignment.version,
                    assignment_participants: assignment.participants,
                    assignment_vnode_count: assignment.vnode_count,
                    assignment_map_digest: assignment.map_digest,
                    leader_proof: leader_proof_to_wire(ann.leader_proof.as_ref()),
                });
                req.set_timeout(request_timeout);
                client
                    .abort(req)
                    .await
                    .map_err(|e| e.to_string())
                    .and_then(|response| validate_phase_ack(&response.into_inner(), &ann))
            }
            // Prepare RPCs are issued by wait_for_quorum, not here.
            Phase::Prepare => Ok(()),
        }
    })
    .await;

    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            clients_pool.lock().remove(&peer);
            if tokio::time::Instant::now() >= deadline || error.contains("Timeout expired") {
                Err(format!(
                    "{rpc} RPC to peer {} exceeded its request deadline",
                    peer.0
                ))
            } else {
                Err(format!("{rpc} RPC to peer {} failed: {error}", peer.0))
            }
        }
        Err(_) => {
            clients_pool.lock().remove(&peer);
            Err(format!(
                "{rpc} RPC to peer {} exceeded its request deadline",
                peer.0
            ))
        }
    }
}

/// Typed prepare-failure classification for the quorum wait:
/// `Unreachable` counts toward `TimedOut{missing}` (the peer cannot
/// participate), `Nack` toward `Failed` (a live follower answered
/// `ok = false`).
#[cfg(feature = "cluster")]
enum PeerFailure {
    Unreachable,
    Nack(String),
}

/// Cross-instance barrier coordination.
pub struct BarrierCoordinator {
    kv: Arc<dyn ClusterKv>,
    #[cfg(feature = "cluster")]
    grpc: Arc<parking_lot::Mutex<Option<Arc<GrpcState>>>>,
    #[cfg(feature = "cluster")]
    leader_election: Arc<parking_lot::Mutex<ActiveLeaderState>>,
    #[cfg(feature = "cluster")]
    leader_lease_store: Arc<parking_lot::Mutex<Option<Arc<super::LeaderLeaseStore>>>>,
}

impl std::fmt::Debug for BarrierCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BarrierCoordinator").finish_non_exhaustive()
    }
}

impl Drop for BarrierCoordinator {
    fn drop(&mut self) {
        #[cfg(feature = "cluster")]
        {
            let grpc_opt = self.grpc.lock().take();
            if let Some(state) = grpc_opt {
                let handle_opt = state.server_handle.lock().take();
                if let Some(handle) = handle_opt {
                    handle.abort();
                }
                let relay_opt = state.relay_handle.lock().take();
                if let Some(handle) = relay_opt {
                    handle.abort();
                }
            }
        }
    }
}

impl BarrierCoordinator {
    /// Wrap a KV implementation.
    #[must_use]
    pub fn new(kv: Arc<dyn ClusterKv>) -> Self {
        Self {
            kv,
            #[cfg(feature = "cluster")]
            grpc: Arc::new(parking_lot::Mutex::new(None)),
            #[cfg(feature = "cluster")]
            leader_election: Arc::new(parking_lot::Mutex::new(None)),
            #[cfg(feature = "cluster")]
            leader_lease_store: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    /// Install the durable authority used to validate clustered reversible barrier phases.
    /// Without it, clustered `Prepare` and `Aligned` traffic fails closed.
    #[cfg(feature = "cluster")]
    pub fn set_leader_lease_store(&self, store: Arc<super::LeaderLeaseStore>) {
        *self.leader_lease_store.lock() = Some(store);
    }

    /// Exact durable authority installed for clustered barriers and checkpoint decisions.
    ///
    /// Embedded and single-node runtimes do not call this path. A cluster runtime that omitted
    /// authority wiring fails closed instead of falling back to standalone outcome objects.
    #[cfg(feature = "cluster")]
    pub fn checkpoint_authority(
        &self,
    ) -> Result<Arc<super::LeaderLeaseStore>, super::ClusterCheckpointAuthorityError> {
        self.leader_lease_store
            .lock()
            .clone()
            .ok_or(super::ClusterCheckpointAuthorityError::NotConfigured)
    }

    #[cfg(feature = "cluster")]
    async fn validate_reversible_announcement(
        &self,
        ann: &BarrierAnnouncement,
    ) -> Result<(), String> {
        if !matches!(ann.phase, Phase::Prepare | Phase::Aligned) {
            return Ok(());
        }
        // An assignment certificate is the barrier layer's cluster-runtime marker. Embedded and
        // single-node coordinators can be built with the cluster feature enabled, but do not have
        // a remote leader lease and must retain their local KV path.
        if ann.assignment_fence.is_none() {
            return Ok(());
        }
        let proof = ann.leader_proof.as_ref().ok_or_else(|| {
            format!(
                "clustered {:?} for checkpoint {}/{} is missing a durable leader proof",
                ann.phase, ann.epoch, ann.checkpoint_id
            )
        })?;
        let store = self
            .leader_lease_store
            .lock()
            .clone()
            .ok_or_else(|| "durable leader lease store is not installed".to_string())?;
        let lease = store
            .load()
            .await
            .map_err(|error| format!("leader lease read failed: {error}"))?
            .ok_or_else(|| "no durable leader lease exists".to_string())?;
        if !lease.matches_proof(proof) {
            return Err(format!(
                "clustered {:?} for checkpoint {}/{} does not match the latest durable leader lease",
                ann.phase, ann.epoch, ann.checkpoint_id
            ));
        }
        Ok(())
    }

    /// Configure membership used to target active barrier peers.
    /// Gossip election is not a barrier authority boundary.
    #[cfg(feature = "cluster")]
    pub fn set_leader_election(
        &mut self,
        instance_id: NodeId,
        members_rx: watch::Receiver<Vec<NodeInfo>>,
        leader_eligible: Arc<AtomicBool>,
    ) {
        *self.leader_election.lock() = Some((instance_id, members_rx, leader_eligible));
    }

    /// Local monotonic receipt time for this exact gRPC Prepare.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn prepare_received_at(&self, prepare: &BarrierAnnouncement) -> Option<std::time::Instant> {
        if prepare.phase != Phase::Prepare {
            return None;
        }
        let identity = BarrierIdentity::from_announcement(prepare);
        self.grpc.lock().as_ref().and_then(|state| {
            state
                .prepare_acks
                .lock()
                .received_at
                .get(&identity)
                .copied()
        })
    }

    /// Bind and run the follower's direct gRPC barrier sync server.
    ///
    /// # Errors
    /// Returns an error string on bind or socket address retrieval failures.
    #[cfg(feature = "cluster")]
    pub async fn start_server(
        &self,
        bind_addr: std::net::SocketAddr,
        advertise_host: Option<String>,
        query_handler: super::QueryHandlerSlot,
    ) -> Result<std::net::SocketAddr, String> {
        use super::query::query_service_server;
        use barrier_v1::barrier_sync_server::BarrierSyncServer;
        use std::net::TcpListener;
        use tonic::transport::Server;

        let listener = TcpListener::bind(bind_addr).map_err(|e| e.to_string())?;
        let local_addr = listener.local_addr().map_err(|e| e.to_string())?;
        listener.set_nonblocking(true).map_err(|e| e.to_string())?;
        let tokio_listener =
            tokio::net::TcpListener::from_std(listener).map_err(|e| e.to_string())?;

        let (incoming_tx, incoming_rx) = crossfire::mpsc::bounded_async::<BarrierAnnouncement>(128);
        let prepare_acks = Arc::new(parking_lot::Mutex::new(PrepareAckState::default()));
        let clients = Arc::new(parking_lot::Mutex::new(FxHashMap::default()));

        let server_impl = GrpcBarrierServer {
            incoming_tx: incoming_tx.clone(),
            prepare_acks: Arc::clone(&prepare_acks),
            leader_lease_store: Arc::clone(&self.leader_lease_store),
        };

        // The pull-path query service shares this control-plane port; peers
        // reach it at the same address published under `BARRIER_ADDR_KEY`.
        let query_svc = query_service_server(query_handler);
        // Apply TLS synchronously so a bad cert fails start_server (before
        // publishing BARRIER_ADDR_KEY) rather than silently never serving.
        let mut builder = Server::builder();
        if let Some(tls) = super::tls::server_tls() {
            builder = builder
                .tls_config(tls.clone())
                .map_err(|e| format!("cluster control-plane TLS config: {e}"))?;
        }
        let router = builder
            .add_service(BarrierSyncServer::new(server_impl))
            .add_service(query_svc);
        let server_task = tokio::spawn(async move {
            let incoming_stream = tokio_stream::wrappers::TcpListenerStream::new(tokio_listener);
            let _ = router.serve_with_incoming(incoming_stream).await;
        });

        let advertise_addr = if let Some(ref host) = advertise_host {
            format!("{host}:{}", local_addr.port())
        } else if local_addr.ip().is_unspecified() {
            let hostname = gethostname::gethostname();
            let hostname = hostname.to_string_lossy();
            if hostname.is_empty() {
                local_addr.to_string()
            } else {
                format!("{hostname}:{}", local_addr.port())
            }
        } else {
            local_addr.to_string()
        };

        // Relay every gRPC-delivered announcement into a relation-validated
        // watch in arrival order. Observation is then non-destructive,
        // so the pipeline's resume gate and the background durable
        // tail can watch concurrently (matching the gossip-KV
        // fallback's read-latest semantics).
        let (latest_tx, latest_rx) = watch::channel::<Option<BarrierAnnouncement>>(None);
        let merge_error = Arc::new(parking_lot::Mutex::new(None));
        let relay_merge_error = Arc::clone(&merge_error);
        let relay_task = tokio::spawn(async move {
            while let Ok(ann) = incoming_rx.recv().await {
                let merged = match latest_tx.borrow().clone() {
                    Some(current) => merge_direct_announcement(current, ann),
                    None => Ok(ann),
                };
                match merged {
                    Ok(merged) => {
                        let changed = latest_tx.borrow().as_ref() != Some(&merged);
                        if changed {
                            let _ = latest_tx.send(Some(merged));
                        }
                    }
                    Err(error) => {
                        tracing::error!(%error, "rejecting conflicting direct barrier history");
                        let mut retained = relay_merge_error.lock();
                        if retained.is_none() {
                            *retained = Some(error);
                        }
                    }
                }
            }
        });

        let grpc_state = Arc::new(GrpcState {
            latest_rx,
            merge_error,
            prepare_acks,
            clients,
            server_handle: Arc::new(parking_lot::Mutex::new(Some(server_task))),
            relay_handle: Arc::new(parking_lot::Mutex::new(Some(relay_task))),
            advertise_addr: advertise_addr.clone(),
        });

        *self.grpc.lock() = Some(grpc_state);

        self.kv.write(BARRIER_ADDR_KEY, advertise_addr).await;

        Ok(local_addr)
    }

    /// Leader-side announce.
    ///
    /// # Errors
    /// Returns a string on JSON encode failure.
    pub async fn announce(&self, ann: &BarrierAnnouncement) -> Result<(), String> {
        #[cfg(feature = "cluster")]
        {
            self.validate_reversible_announcement(ann).await?;
            let grpc_opt = self.grpc.lock().clone();
            if let Some(state) = grpc_opt {
                // Record the decision in KV before delivery, so a reclaiming
                // leader's `max_announced()` and a recovering peer's KV fallback
                // still see this epoch even if a peer RPC below fails and returns
                // early (the RPC receiver does not persist the announcement).
                let json = serde_json::to_string(ann).map_err(|e| e.to_string())?;
                self.kv.write(ANNOUNCEMENT_KEY, json).await;
                if ann.phase == Phase::Prepare {
                    // Prepare RPCs come from wait_for_quorum; a redundant one here double-fires.
                } else {
                    // A node's barrier address lingers in the KV after it dies,
                    // so announce to peers still Active in membership — a Commit
                    // RPC to a departed peer returns Err and wedges every epoch.
                    // The KV-fallback write below still lets a recovering peer
                    // observe the announcement.
                    let live: Option<FxHashSet<NodeId>> =
                        self.leader_election
                            .lock()
                            .clone()
                            .map(|(_, members_rx, _)| {
                                members_rx
                                    .borrow()
                                    .iter()
                                    // A draining owner remains a checkpoint participant until its
                                    // handoff cut commits; it must receive Aligned/Commit/Abort too.
                                    .filter(|m| {
                                        matches!(m.state, NodeState::Active | NodeState::Draining)
                                    })
                                    .map(|m| m.id)
                                    .collect()
                            });
                    let mut expected = Vec::new();
                    for (node_id, addr) in self.kv.scan(BARRIER_ADDR_KEY).await {
                        if addr == state.advertise_addr {
                            continue;
                        }
                        if live.as_ref().is_some_and(|live| !live.contains(&node_id)) {
                            continue;
                        }
                        expected.push(node_id);
                    }

                    // One absolute deadline bounds the whole concurrent fan-out.
                    // Reusing it for every peer prevents a slow address lookup or
                    // live handler from extending the round one timeout at a time.
                    let rpc_deadline = tokio::time::Instant::now() + PHASE_RPC_TIMEOUT;
                    let mut futures = Vec::new();
                    for peer in expected {
                        let clients_pool = Arc::clone(&state.clients);
                        let kv = Arc::clone(&self.kv);
                        let ann_clone = ann.clone();
                        futures.push(send_phase_rpc(
                            peer,
                            clients_pool,
                            kv,
                            ann_clone,
                            rpc_deadline,
                        ));
                    }
                    let results = futures::future::join_all(futures).await;
                    for res in results {
                        match res {
                            Ok(()) => {}
                            // Aligned is best-effort per peer: a missed
                            // delivery only delays that peer's pipeline
                            // resume until Commit (or its gate timeout) —
                            // never fail the announce, and never skip the
                            // KV write below.
                            Err(e) if ann.phase == Phase::Aligned => {
                                tracing::warn!(
                                    epoch = ann.epoch,
                                    error = %e,
                                    "aligned announcement RPC failed; peer resumes on Commit"
                                );
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }

                return Ok(());
            }
        }

        let json = serde_json::to_string(ann).map_err(|e| e.to_string())?;
        self.kv.write(ANNOUNCEMENT_KEY, json).await;
        Ok(())
    }

    /// Watch over gRPC-delivered announcements, for push-driven waits
    /// (the decision wait and the Aligned resume gate). `None` until
    /// the gRPC server is started — gossip-KV-only deployments fall
    /// back to polling [`observe`](Self::observe).
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn announcement_watch(&self) -> Option<watch::Receiver<Option<BarrierAnnouncement>>> {
        self.grpc.lock().as_ref().map(|s| s.latest_rx.clone())
    }

    /// Follower-side observe — returns the *latest* announcement
    /// (non-destructive; repeated calls return the same value until a
    /// newer one arrives, matching the gossip-KV fallback). Callers
    /// already dedup by exact attempt/phase. The gRPC-delivered value and gossip-KV value must be
    /// related in both attempt dimensions. Within an exact attempt phase progress is monotonic
    /// while gossip catches up; a terminal durable KV value is the decision authority.
    ///
    /// # Errors
    /// Returns a string on transport, decode, or conflicting-history failure.
    pub async fn observe(&self, leader: NodeId) -> Result<Option<BarrierAnnouncement>, String> {
        #[cfg(feature = "cluster")]
        let grpc_latest: Option<BarrierAnnouncement> = {
            let grpc_opt = self.grpc.lock().clone();
            if let Some(error) = grpc_opt
                .as_ref()
                .and_then(|state| state.merge_error.lock().clone())
            {
                return Err(error);
            }
            grpc_opt.and_then(|state| state.latest_rx.borrow().clone())
        };
        #[cfg(not(feature = "cluster"))]
        let grpc_latest: Option<BarrierAnnouncement> = None;

        let kv_latest: Option<BarrierAnnouncement> =
            match self.kv.read_from_checked(leader, ANNOUNCEMENT_KEY).await? {
                Some(json) => Some(serde_json::from_str(&json).map_err(|error| {
                    format!("malformed durable barrier announcement from {leader}: {error}")
                })?),
                None => None,
            };

        let observed = match (grpc_latest, kv_latest) {
            (Some(g), Some(k)) => Some(merge_observed_announcement(g, k)?),
            (Some(g), None) => Some(g),
            (None, k) => k,
        };
        #[cfg(feature = "cluster")]
        if let Some(ann) = observed.as_ref() {
            self.validate_reversible_announcement(ann).await?;
        }
        Ok(observed)
    }

    /// Highest valid attempt any node has announced across the gossiped per-node keys.
    ///
    /// # Errors
    /// Returns an error for malformed values or histories whose epoch and checkpoint ID do not
    /// move together; a reclaiming leader must not advance from a lexicographically invented cut.
    pub async fn max_announced(&self) -> Result<Option<CheckpointAttempt>, String> {
        let mut announcements = Vec::new();
        for (node, json) in self.kv.scan_checked(ANNOUNCEMENT_KEY).await? {
            let announcement: BarrierAnnouncement = serde_json::from_str(&json)
                .map_err(|error| format!("malformed barrier announcement from {node}: {error}"))?;
            announcements.push(announcement);
        }
        let highest = validate_scanned_announcements(announcements)?;
        Ok(highest.as_ref().map(announcement_attempt))
    }

    /// Follower-side ack.
    ///
    /// # Errors
    /// Returns a string on JSON encode failure.
    pub async fn ack(&self, ack: &BarrierAck) -> Result<(), String> {
        #[cfg(feature = "cluster")]
        {
            let grpc_opt = self.grpc.lock().clone();
            if let Some(state) = grpc_opt {
                let identity = BarrierIdentity::from_ack(ack);
                let (cached, waiters) = {
                    let mut prepare = state.prepare_acks.lock();
                    let cached = prepare.record_ack(identity, ack);
                    let waiters = prepare.pending.remove(&identity).unwrap_or_default();
                    (cached, waiters)
                };
                for waiter in waiters {
                    let _ = waiter.response.send(cached.clone());
                }
                return Ok(());
            }
        }

        let json = serde_json::to_string(ack).map_err(|e| e.to_string())?;
        self.kv.write(ACK_KEY, json).await;
        Ok(())
    }

    /// Leader-side: wait until quorum or `deadline`.
    #[allow(clippy::too_many_lines)]
    // `PeerFailure` (module level, below) classifies each peer's
    // prepare outcome.
    pub async fn wait_for_quorum(
        &self,
        prepare: &BarrierAnnouncement,
        expected: &[NodeId],
        deadline: Duration,
    ) -> QuorumOutcome {
        let epoch = prepare.epoch;
        let checkpoint_id = prepare.checkpoint_id;
        let assignment_digest = prepare
            .assignment_fence
            .as_ref()
            .map(super::CheckpointAssignmentFence::digest);
        #[cfg(feature = "cluster")]
        {
            let grpc_opt = self.grpc.lock().clone();
            if let Some(state) = grpc_opt {
                let assignment = assignment_fence_to_wire(prepare.assignment_fence.as_ref());
                let leader_proof = leader_proof_to_wire(prepare.leader_proof.as_ref());
                let mut futures = Vec::new();
                for &peer in expected {
                    let clients_pool = Arc::clone(&state.clients);
                    let kv = Arc::clone(&self.kv);
                    let assignment = assignment.clone();
                    let leader_proof = leader_proof.clone();
                    futures.push(async move {
                        let client_opt = get_barrier_client(peer, &clients_pool, &kv).await;
                        let Some(mut client) = client_opt else {
                            return Err((peer, PeerFailure::Unreachable));
                        };

                        let mut req = tonic::Request::new(barrier_v1::PrepareRequest {
                            epoch,
                            checkpoint_id,
                            flags: 0,
                            assignment_version: assignment.version,
                            assignment_participants: assignment.participants,
                            assignment_vnode_count: assignment.vnode_count,
                            assignment_map_digest: assignment.map_digest,
                            leader_proof,
                        });
                        req.set_timeout(deadline);

                        match tokio::time::timeout(deadline, client.prepare(req)).await {
                            Ok(Ok(response)) => {
                                let ack = response.into_inner();
                                if ack.epoch != epoch
                                    || ack.checkpoint_id != checkpoint_id
                                    || ack.assignment_digest.as_slice()
                                        != assignment_digest
                                            .as_ref()
                                            .map_or(&[][..], <[u8; 32]>::as_slice)
                                {
                                    Err((
                                        peer,
                                        PeerFailure::Nack(
                                            "Prepare acknowledgement identity mismatch".into(),
                                        ),
                                    ))
                                } else if ack.ok {
                                    checkpoint_watermark_from_wire(
                                        ack.watermark_status,
                                        ack.local_watermark_ms,
                                    )
                                    .map(|watermark| (peer, watermark))
                                    .map_err(|error| (peer, PeerFailure::Nack(error)))
                                } else {
                                    Err((
                                        peer,
                                        PeerFailure::Nack(ack.error.unwrap_or_else(|| {
                                            "Unknown prepare failure".to_string()
                                        })),
                                    ))
                                }
                            }
                            Ok(Err(status)) => {
                                clients_pool.lock().remove(&peer);
                                // Classify by gRPC status code, not message
                                // text: transport-level codes mean the peer
                                // cannot participate (same epistemic state
                                // as a timeout); anything else is a live
                                // server refusing the call.
                                match status.code() {
                                    tonic::Code::Unavailable
                                    | tonic::Code::DeadlineExceeded
                                    | tonic::Code::Cancelled
                                    | tonic::Code::Aborted => Err((peer, PeerFailure::Unreachable)),
                                    _ => Err((peer, PeerFailure::Nack(status.to_string()))),
                                }
                            }
                            Err(_) => {
                                clients_pool.lock().remove(&peer);
                                Err((peer, PeerFailure::Unreachable))
                            }
                        }
                    });
                }

                let results = futures::future::join_all(futures).await;

                let mut successful = Vec::new();
                let mut failures = Vec::new();
                let mut follower_watermark = None;
                let mut timed_out = Vec::new();

                for res in results {
                    match res {
                        Ok((peer, wm)) => {
                            successful.push(peer);
                            follower_watermark = Some(follower_watermark.map_or(wm, |current| {
                                CheckpointWatermark::cluster_min(current, wm)
                            }));
                        }
                        Err((peer, PeerFailure::Unreachable)) => timed_out.push(peer),
                        Err((peer, PeerFailure::Nack(msg))) => failures.push((peer, msg)),
                    }
                }

                if !failures.is_empty() {
                    return QuorumOutcome::Failed { failures };
                }

                if !timed_out.is_empty() || successful.len() < expected.len() {
                    let got = successful;
                    let mut missing = timed_out;
                    for &peer in expected {
                        if !got.contains(&peer) && !missing.contains(&peer) {
                            missing.push(peer);
                        }
                    }
                    return QuorumOutcome::TimedOut { got, missing };
                }

                return QuorumOutcome::Reached {
                    acks: successful,
                    follower_watermark: follower_watermark
                        .unwrap_or(CheckpointWatermark::Uninitialized),
                };
            }
        }

        let start = Instant::now();
        let expected_set: FxHashSet<NodeId> = expected.iter().copied().collect();
        let mut successful: Vec<NodeId> = Vec::new();
        let mut failures: Vec<(NodeId, String)> = Vec::new();
        let mut follower_watermark: Option<CheckpointWatermark>;

        loop {
            successful.clear();
            failures.clear();
            follower_watermark = None;

            for (from, json) in self.kv.scan(ACK_KEY).await {
                if !expected_set.contains(&from) {
                    continue;
                }
                let Ok(ack) = serde_json::from_str::<BarrierAck>(&json) else {
                    continue;
                };
                if ack.epoch != epoch
                    || ack.checkpoint_id != checkpoint_id
                    || ack.assignment_digest != assignment_digest
                {
                    continue;
                }
                if ack.ok {
                    if let Err(error) = ack.watermark.validate() {
                        failures.push((from, error));
                    } else {
                        successful.push(from);
                        follower_watermark =
                            Some(follower_watermark.map_or(ack.watermark, |current| {
                                current.cluster_min(ack.watermark)
                            }));
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
                    follower_watermark: follower_watermark
                        .unwrap_or(CheckpointWatermark::Uninitialized),
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

    fn test_fence(
        assignment_version: u64,
        owners: &[u64],
        participants: &[(u64, u128)],
    ) -> crate::checkpoint::CheckpointAssignmentFence {
        crate::checkpoint::CheckpointAssignmentFence::from_owner_map(
            assignment_version,
            owners,
            participants
                .iter()
                .map(
                    |(node_id, incarnation)| crate::checkpoint::CheckpointParticipant {
                        node_id: *node_id,
                        boot_incarnation: uuid::Uuid::from_u128(*incarnation),
                    },
                )
                .collect(),
        )
        .unwrap()
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn wire_watermark_requires_an_exact_status_value_shape() {
        use barrier_v1::CheckpointWatermarkStatus as WireStatus;

        assert_eq!(
            checkpoint_watermark_from_wire(
                WireStatus::CheckpointWatermarkUninitialized as i32,
                None,
            )
            .unwrap(),
            CheckpointWatermark::Uninitialized
        );
        assert_eq!(
            checkpoint_watermark_from_wire(WireStatus::CheckpointWatermarkIdle as i32, None,)
                .unwrap(),
            CheckpointWatermark::Idle
        );
        assert_eq!(
            checkpoint_watermark_from_wire(WireStatus::CheckpointWatermarkActive as i32, Some(10),)
                .unwrap(),
            CheckpointWatermark::Active(10)
        );
        assert!(checkpoint_watermark_from_wire(
            WireStatus::CheckpointWatermarkIdle as i32,
            Some(10),
        )
        .is_err());
        assert!(
            checkpoint_watermark_from_wire(WireStatus::CheckpointWatermarkActive as i32, None,)
                .is_err()
        );
        assert!(checkpoint_watermark_from_wire(99, None).is_err());
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn assignment_fence_wire_round_trip_preserves_exact_map_and_processes() {
        let fence = test_fence(17, &[1, 2, 1, 2], &[(1, 11), (2, 22)]);
        let wire = assignment_fence_to_wire(Some(&fence));
        let decoded = assignment_fence_from_wire(
            wire.version,
            wire.vnode_count,
            wire.map_digest,
            wire.participants,
        )
        .unwrap();
        assert_eq!(decoded, Some(fence));

        assert_eq!(
            assignment_fence_from_wire(0, 0, Vec::new(), Vec::new()).unwrap(),
            None
        );
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn assignment_fence_wire_rejects_partial_and_noncanonical_certificates() {
        let fence = test_fence(17, &[1, 2], &[(1, 11), (2, 22)]);

        let mut wrong_digest_length = assignment_fence_to_wire(Some(&fence));
        wrong_digest_length.map_digest.pop();
        assert!(assignment_fence_from_wire(
            wrong_digest_length.version,
            wrong_digest_length.vnode_count,
            wrong_digest_length.map_digest,
            wrong_digest_length.participants,
        )
        .is_err());

        let mut wrong_incarnation_length = assignment_fence_to_wire(Some(&fence));
        wrong_incarnation_length.participants[0]
            .boot_incarnation
            .pop();
        assert!(assignment_fence_from_wire(
            wrong_incarnation_length.version,
            wrong_incarnation_length.vnode_count,
            wrong_incarnation_length.map_digest,
            wrong_incarnation_length.participants,
        )
        .is_err());

        let mut unordered = assignment_fence_to_wire(Some(&fence));
        unordered.participants.swap(0, 1);
        assert!(assignment_fence_from_wire(
            unordered.version,
            unordered.vnode_count,
            unordered.map_digest,
            unordered.participants,
        )
        .is_err());

        assert!(assignment_fence_from_wire(17, 0, Vec::new(), Vec::new()).is_err());
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn assignment_fence_wire_rejects_oversized_forged_certificate() {
        let maximum = u64::try_from(crate::checkpoint::MAX_CHECKPOINT_PARTICIPANTS).unwrap();
        let participants = (1..=maximum + 1)
            .map(|node_id| barrier_v1::CheckpointParticipant {
                node_id,
                boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id))
                    .as_bytes()
                    .to_vec(),
            })
            .collect();

        let error = assignment_fence_from_wire(17, 1, vec![1; 32], participants).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("Non-canonical"));
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn leader_proof_wire_round_trip_preserves_exact_process_term() {
        let proof = super::super::LeaderProof {
            owner: crate::checkpoint::LeaderProofOwner {
                node_id: 7,
                boot_id: uuid::Uuid::from_u128(70),
                process_term: 9,
            },
            fencing_token: 11,
        };
        let decoded = leader_proof_from_wire(leader_proof_to_wire(Some(&proof))).unwrap();
        assert_eq!(decoded.as_ref(), Some(&proof));
        assert_eq!(leader_proof_from_wire(None).unwrap(), None);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn reversible_barrier_rejects_same_node_new_boot_and_old_token() {
        use object_store::memory::InMemory;

        let store = Arc::new(super::super::LeaderLeaseStore::new(
            Arc::new(InMemory::new()),
            1,
        ));
        let original = super::super::LeaderLeaseOwner {
            node: NodeId(7),
            boot: uuid::Uuid::from_u128(70),
            process_term: 1,
        };
        let replacement = super::super::LeaderLeaseOwner {
            node: NodeId(7),
            boot: uuid::Uuid::from_u128(71),
            process_term: 2,
        };
        let original_lease = match store.try_acquire(&original, 1).await.unwrap() {
            super::super::LeaseOutcome::Acquired(lease) => lease,
            super::super::LeaseOutcome::Held(_) => unreachable!(),
        };
        let observation = store.observe_rival(&replacement, &original_lease).unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        let replacement_lease = match store
            .try_takeover(&replacement, &observation, 2)
            .await
            .unwrap()
        {
            super::super::LeaseOutcome::Acquired(lease) => lease,
            super::super::LeaseOutcome::Held(_) => unreachable!(),
        };

        let (incoming_tx, _incoming_rx) = crossfire::mpsc::bounded_async::<BarrierAnnouncement>(1);
        let server = GrpcBarrierServer {
            incoming_tx,
            prepare_acks: Arc::new(parking_lot::Mutex::new(PrepareAckState::default())),
            leader_lease_store: Arc::new(parking_lot::Mutex::new(Some(Arc::clone(&store)))),
        };
        assert!(server
            .require_latest_proof(&replacement_lease.proof())
            .await
            .is_ok());

        assert!(server
            .require_latest_proof(&original_lease.proof())
            .await
            .is_err());
        let wrong_boot = super::super::LeaderProof {
            owner: crate::checkpoint::LeaderProofOwner {
                node_id: replacement_lease.owner.node.0,
                boot_id: uuid::Uuid::from_u128(72),
                process_term: replacement_lease.owner.process_term,
            },
            fencing_token: replacement_lease.token,
        };
        assert!(server.require_latest_proof(&wrong_boot).await.is_err());
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn phase_ack_rejects_same_map_from_a_restarted_process() {
        let expected = test_fence(17, &[1, 2], &[(1, 11), (2, 22)]);
        let restarted = test_fence(17, &[1, 2], &[(1, 111), (2, 22)]);
        let announcement = BarrierAnnouncement {
            epoch: 20,
            checkpoint_id: 200,
            assignment_fence: Some(expected),
            leader_proof: None,
            phase: Phase::Commit,
            flags: 0,
            min_watermark_ms: None,
        };
        let ack = barrier_v1::Ack {
            epoch: announcement.epoch,
            ok: true,
            error: None,
            local_watermark_ms: None,
            checkpoint_id: announcement.checkpoint_id,
            assignment_digest: restarted.digest().to_vec(),
            watermark_status: 0,
        };

        assert!(validate_phase_ack(&ack, &announcement).is_err());
    }

    #[cfg(all(test, feature = "cluster"))]
    mod grpc_tests {
        use super::*;
        use object_store::memory::InMemory;
        use std::net::SocketAddr;

        async fn lease_authority() -> (
            Arc<crate::cluster::control::LeaderLeaseStore>,
            crate::cluster::control::LeaderProof,
        ) {
            let store = Arc::new(crate::cluster::control::LeaderLeaseStore::new(
                Arc::new(InMemory::new()),
                1_000,
            ));
            let owner = crate::cluster::control::LeaderLeaseOwner {
                node: NodeId(1),
                boot: uuid::Uuid::from_u128(1),
                process_term: 1,
            };
            let lease = match store.try_acquire(&owner, 1).await.unwrap() {
                crate::cluster::control::LeaseOutcome::Acquired(lease) => lease,
                crate::cluster::control::LeaseOutcome::Held(_) => unreachable!(),
            };
            (store, lease.proof())
        }

        fn coordinator(
            kv: Arc<dyn ClusterKv>,
            store: Arc<crate::cluster::control::LeaderLeaseStore>,
        ) -> BarrierCoordinator {
            let coordinator = BarrierCoordinator::new(kv);
            coordinator.set_leader_lease_store(store);
            coordinator
        }

        /// Observation is latest-wins (non-destructive), so wait for the
        /// expected phase specifically — earlier phases may linger.
        async fn wait_observe(
            coord: &BarrierCoordinator,
            leader: NodeId,
            phase: Phase,
        ) -> BarrierAnnouncement {
            for _ in 0..100 {
                if let Some(ann) = coord.observe(leader).await.unwrap() {
                    if ann.phase == phase {
                        return ann;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            panic!("timed out waiting for {phase:?} announcement from leader {leader:?}");
        }

        #[tokio::test]
        async fn test_grpc_barrier_flow() {
            let leader_kv = kv(NodeId(1));
            let follower_kv = kv(NodeId(2));
            let (store, proof) = lease_authority().await;
            let leader_coord = coordinator(leader_kv.clone(), Arc::clone(&store));
            let follower_coord = coordinator(follower_kv.clone(), store);

            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let slot = || Arc::new(parking_lot::RwLock::new(None));
            let leader_addr = leader_coord.start_server(addr, None, slot()).await.unwrap();
            let bound_addr = follower_coord
                .start_server(addr, None, slot())
                .await
                .unwrap();

            leader_kv.seed(NodeId(2), BARRIER_ADDR_KEY, bound_addr.to_string());
            follower_kv.seed(NodeId(1), BARRIER_ADDR_KEY, leader_addr.to_string());

            // Sequencing handshake: observation is latest-wins, so the
            // leader must not announce Commit until the follower has
            // observed Aligned (otherwise Commit may overwrite it).
            let (aligned_seen_tx, aligned_seen_rx) = tokio::sync::oneshot::channel::<()>();
            let assignment_fence = test_fence(9, &[1, 2, 1, 2], &[(1, 11), (2, 22)]);
            let follower_fence = assignment_fence.clone();

            let follower_task = tokio::spawn(async move {
                let ann = wait_observe(&follower_coord, NodeId(1), Phase::Prepare).await;
                assert_eq!(ann.epoch, 1);
                assert_eq!(ann.checkpoint_id, 42);
                assert_eq!(ann.assignment_fence.as_ref(), Some(&follower_fence));

                follower_coord
                    .ack(&BarrierAck {
                        epoch: 1,
                        checkpoint_id: 42,
                        assignment_digest: Some(follower_fence.digest()),
                        ok: true,
                        error: None,
                        watermark: CheckpointWatermark::Active(100),
                    })
                    .await
                    .unwrap();

                let aligned_ann = wait_observe(&follower_coord, NodeId(1), Phase::Aligned).await;
                assert_eq!(aligned_ann.epoch, 1);
                assert_eq!(aligned_ann.min_watermark_ms, Some(100));
                assert_eq!(aligned_ann.assignment_fence.as_ref(), Some(&follower_fence));
                aligned_seen_tx.send(()).unwrap();

                let commit_ann = wait_observe(&follower_coord, NodeId(1), Phase::Commit).await;
                assert_eq!(commit_ann.min_watermark_ms, Some(100));
                assert_eq!(commit_ann.assignment_fence.as_ref(), Some(&follower_fence));
            });

            let prepare = BarrierAnnouncement {
                epoch: 1,
                checkpoint_id: 42,
                assignment_fence: Some(assignment_fence),
                leader_proof: Some(proof),
                phase: Phase::Prepare,
                flags: 0,
                min_watermark_ms: None,
            };
            leader_coord.announce(&prepare).await.unwrap();

            let outcome = leader_coord
                .wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_secs(5))
                .await;
            match outcome {
                QuorumOutcome::Reached {
                    acks,
                    follower_watermark,
                } => {
                    assert_eq!(acks, vec![NodeId(2)]);
                    assert_eq!(follower_watermark, CheckpointWatermark::Active(100));

                    // Two-level completion: resume gate first…
                    leader_coord
                        .announce(&BarrierAnnouncement {
                            epoch: 1,
                            checkpoint_id: 42,
                            assignment_fence: prepare.assignment_fence.clone(),
                            leader_proof: prepare.leader_proof.clone(),
                            phase: Phase::Aligned,
                            flags: 0,
                            min_watermark_ms: follower_watermark.active_value(),
                        })
                        .await
                        .unwrap();
                    aligned_seen_rx.await.unwrap();

                    // …then the restorable decision.
                    leader_coord
                        .announce(&BarrierAnnouncement {
                            epoch: 1,
                            checkpoint_id: 42,
                            assignment_fence: prepare.assignment_fence.clone(),
                            leader_proof: prepare.leader_proof.clone(),
                            phase: Phase::Commit,
                            flags: 0,
                            min_watermark_ms: follower_watermark.active_value(),
                        })
                        .await
                        .unwrap();
                }
                other => panic!("expected Reached, got {other:?}"),
            }

            follower_task.await.unwrap();
        }

        #[tokio::test]
        async fn terminal_hint_does_not_authorize_or_close_a_prepare_attempt() {
            let leader_kv = kv(NodeId(1));
            let follower_kv = kv(NodeId(2));
            let (store, proof) = lease_authority().await;
            let leader_coord = coordinator(leader_kv.clone(), Arc::clone(&store));
            let follower_coord = coordinator(follower_kv.clone(), store);
            let slot = || Arc::new(parking_lot::RwLock::new(None));
            let leader_addr = leader_coord
                .start_server("127.0.0.1:0".parse().unwrap(), None, slot())
                .await
                .unwrap();
            let follower_addr = follower_coord
                .start_server("127.0.0.1:0".parse().unwrap(), None, slot())
                .await
                .unwrap();
            leader_kv.seed(NodeId(2), BARRIER_ADDR_KEY, follower_addr.to_string());
            follower_kv.seed(NodeId(1), BARRIER_ADDR_KEY, leader_addr.to_string());

            let prepare = BarrierAnnouncement {
                epoch: 11,
                checkpoint_id: 101,
                assignment_fence: None,
                leader_proof: Some(proof),
                phase: Phase::Prepare,
                flags: 0,
                min_watermark_ms: None,
            };
            leader_coord.announce(&prepare).await.unwrap();

            let first =
                leader_coord.wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_secs(2));
            let duplicate =
                leader_coord.wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_secs(2));
            let follower = async {
                let _ = wait_observe(&follower_coord, NodeId(1), Phase::Prepare).await;
                // Give both identical RPCs time to register before completing
                // the local checkpoint once.
                tokio::time::sleep(Duration::from_millis(100)).await;
                follower_coord
                    .ack(&BarrierAck {
                        epoch: prepare.epoch,
                        checkpoint_id: prepare.checkpoint_id,
                        assignment_digest: None,
                        ok: true,
                        error: None,
                        watermark: CheckpointWatermark::Active(77),
                    })
                    .await
                    .unwrap();
            };
            let (first, duplicate, ()) = tokio::join!(first, duplicate, follower);
            assert!(matches!(first, QuorumOutcome::Reached { .. }), "{first:?}");
            assert!(
                matches!(duplicate, QuorumOutcome::Reached { .. }),
                "{duplicate:?}"
            );

            // More retries before the decision reuse the immutable cached ACK;
            // no second local checkpoint execution is required.
            for _ in 0..2 {
                let retry = leader_coord
                    .wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_millis(500))
                    .await;
                assert!(matches!(retry, QuorumOutcome::Reached { .. }), "{retry:?}");
            }

            let commit = BarrierAnnouncement {
                phase: Phase::Commit,
                min_watermark_ms: Some(77),
                ..prepare.clone()
            };
            leader_coord.announce(&commit).await.unwrap();
            let retry_after_hint = leader_coord
                .wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_millis(500))
                .await;
            assert!(
                matches!(retry_after_hint, QuorumOutcome::Reached { .. }),
                "a terminal hint must not close an attempt without its immutable outcome: {retry_after_hint:?}"
            );
        }

        #[tokio::test]
        async fn prepare_nack_replaces_an_earlier_capture_ack() {
            let leader_kv = kv(NodeId(1));
            let follower_kv = kv(NodeId(2));
            let (store, proof) = lease_authority().await;
            let leader_coord = coordinator(leader_kv.clone(), Arc::clone(&store));
            let follower_coord = coordinator(follower_kv.clone(), store);
            let slot = || Arc::new(parking_lot::RwLock::new(None));
            let leader_addr = leader_coord
                .start_server("127.0.0.1:0".parse().unwrap(), None, slot())
                .await
                .unwrap();
            let follower_addr = follower_coord
                .start_server("127.0.0.1:0".parse().unwrap(), None, slot())
                .await
                .unwrap();
            leader_kv.seed(NodeId(2), BARRIER_ADDR_KEY, follower_addr.to_string());
            follower_kv.seed(NodeId(1), BARRIER_ADDR_KEY, leader_addr.to_string());

            let prepare = BarrierAnnouncement {
                epoch: 12,
                checkpoint_id: 102,
                assignment_fence: None,
                leader_proof: Some(proof),
                phase: Phase::Prepare,
                flags: 0,
                min_watermark_ms: None,
            };
            leader_coord.announce(&prepare).await.unwrap();
            let capture_wait =
                leader_coord.wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_secs(2));
            let capture = async {
                let _ = wait_observe(&follower_coord, NodeId(1), Phase::Prepare).await;
                let received_at = follower_coord
                    .prepare_received_at(&prepare)
                    .expect("direct Prepare receipt must be retained");
                tokio::time::sleep(Duration::from_millis(50)).await;
                assert!(
                    received_at.elapsed() >= Duration::from_millis(50),
                    "delayed follower admission must not receive a fresh attempt budget"
                );
                follower_coord
                    .ack(&BarrierAck {
                        epoch: prepare.epoch,
                        checkpoint_id: prepare.checkpoint_id,
                        assignment_digest: None,
                        ok: true,
                        error: None,
                        watermark: CheckpointWatermark::Active(77),
                    })
                    .await
                    .unwrap();
            };
            let (capture_outcome, ()) = tokio::join!(capture_wait, capture);
            assert!(
                matches!(capture_outcome, QuorumOutcome::Reached { .. }),
                "{capture_outcome:?}"
            );

            follower_coord
                .ack(&BarrierAck {
                    epoch: prepare.epoch,
                    checkpoint_id: prepare.checkpoint_id,
                    assignment_digest: None,
                    ok: false,
                    error: Some("durable prepare failed".into()),
                    watermark: CheckpointWatermark::Active(77),
                })
                .await
                .unwrap();

            let retry = leader_coord
                .wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_secs(2))
                .await;
            assert_eq!(
                retry,
                QuorumOutcome::Failed {
                    failures: vec![(NodeId(2), "durable prepare failed".into())],
                }
            );
        }

        #[tokio::test]
        async fn certificate_conflict_cannot_steal_an_exact_prepare_waiter() {
            let leader_kv = kv(NodeId(1));
            let follower_kv = kv(NodeId(2));
            let (store, proof) = lease_authority().await;
            let leader_coord = coordinator(leader_kv.clone(), Arc::clone(&store));
            let follower_coord = coordinator(follower_kv.clone(), store);
            let slot = || Arc::new(parking_lot::RwLock::new(None));
            let leader_addr = leader_coord
                .start_server("127.0.0.1:0".parse().unwrap(), None, slot())
                .await
                .unwrap();
            let follower_addr = follower_coord
                .start_server("127.0.0.1:0".parse().unwrap(), None, slot())
                .await
                .unwrap();
            leader_kv.seed(NodeId(2), BARRIER_ADDR_KEY, follower_addr.to_string());
            follower_kv.seed(NodeId(1), BARRIER_ADDR_KEY, leader_addr.to_string());

            let accepted_fence = test_fence(9, &[1, 2, 1, 2], &[(1, 11), (2, 22)]);
            let conflicting_fence = test_fence(9, &[2, 1, 1, 2], &[(1, 11), (2, 22)]);
            let accepted = BarrierAnnouncement {
                epoch: 12,
                checkpoint_id: 102,
                assignment_fence: Some(accepted_fence.clone()),
                leader_proof: Some(proof),
                phase: Phase::Prepare,
                flags: 0,
                min_watermark_ms: None,
            };
            let conflicting = BarrierAnnouncement {
                assignment_fence: Some(conflicting_fence),
                ..accepted.clone()
            };
            leader_coord.announce(&accepted).await.unwrap();

            let accepted_wait =
                leader_coord.wait_for_quorum(&accepted, &[NodeId(2)], Duration::from_secs(2));
            let follower = async {
                let _ = wait_observe(&follower_coord, NodeId(1), Phase::Prepare).await;
                let conflicting_outcome = leader_coord
                    .wait_for_quorum(&conflicting, &[NodeId(2)], Duration::from_millis(300))
                    .await;
                follower_coord
                    .ack(&BarrierAck {
                        epoch: accepted.epoch,
                        checkpoint_id: accepted.checkpoint_id,
                        assignment_digest: Some(accepted_fence.digest()),
                        ok: true,
                        error: None,
                        watermark: CheckpointWatermark::Uninitialized,
                    })
                    .await
                    .unwrap();
                conflicting_outcome
            };
            let (accepted_outcome, conflicting_outcome) = tokio::join!(accepted_wait, follower);
            assert!(
                matches!(accepted_outcome, QuorumOutcome::Reached { .. }),
                "{accepted_outcome:?}"
            );
            assert!(
                matches!(conflicting_outcome, QuorumOutcome::TimedOut { .. }),
                "a different certificate must not consume the accepted ACK: {conflicting_outcome:?}"
            );

            // A terminal notification alone cannot mutate authoritative Prepare state.
            leader_coord
                .announce(&BarrierAnnouncement {
                    phase: Phase::Abort,
                    ..conflicting
                })
                .await
                .unwrap();
            let state = follower_coord.grpc.lock().clone().unwrap();
            assert!(state.prepare_acks.lock().pending.is_empty());
        }

        #[tokio::test]
        async fn phase_rpc_deadline_bounds_a_live_handler() {
            use object_store::throttle::{ThrottleConfig, ThrottledStore};

            let durable = Arc::new(ThrottledStore::new(
                InMemory::new(),
                ThrottleConfig::default(),
            ));
            let store = Arc::new(crate::cluster::control::LeaderLeaseStore::new(
                durable.clone(),
                1_000,
            ));
            let owner = crate::cluster::control::LeaderLeaseOwner {
                node: NodeId(1),
                boot: uuid::Uuid::from_u128(1),
                process_term: 1,
            };
            let lease = match store.try_acquire(&owner, 1).await.unwrap() {
                crate::cluster::control::LeaseOutcome::Acquired(lease) => lease,
                crate::cluster::control::LeaseOutcome::Held(_) => unreachable!(),
            };
            durable.config_mut(|config| config.wait_get_per_call = Duration::from_secs(5));

            let follower_coord = coordinator(kv(NodeId(2)), store);
            let slot = Arc::new(parking_lot::RwLock::new(None));
            let bound_addr = follower_coord
                .start_server("127.0.0.1:0".parse().unwrap(), None, slot)
                .await
                .unwrap();

            let leader_kv = kv(NodeId(1));
            leader_kv.seed(NodeId(2), BARRIER_ADDR_KEY, bound_addr.to_string());
            let clients = Arc::new(parking_lot::Mutex::new(FxHashMap::default()));
            let started = tokio::time::Instant::now();
            let error = send_phase_rpc(
                NodeId(2),
                Arc::clone(&clients),
                leader_kv,
                BarrierAnnouncement {
                    epoch: 7,
                    checkpoint_id: 9,
                    assignment_fence: Some(test_fence(1, &[1, 2], &[(1, 11), (2, 22)])),
                    leader_proof: Some(lease.proof()),
                    phase: Phase::Aligned,
                    flags: 0,
                    min_watermark_ms: None,
                },
                started + Duration::from_millis(500),
            )
            .await
            .unwrap_err();

            assert!(error.contains("request deadline"), "{error}");
            assert!(started.elapsed() >= Duration::from_millis(400));
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "phase RPC exceeded its absolute deadline"
            );
            assert!(
                clients.lock().is_empty(),
                "a timed-out client must be evicted"
            );
        }
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn direct_phase_merge_is_monotonic_for_an_exact_attempt() {
        let base = BarrierAnnouncement {
            epoch: 20,
            checkpoint_id: 200,
            assignment_fence: None,
            leader_proof: None,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        };

        for terminal in [Phase::Commit, Phase::Abort] {
            let decided = BarrierAnnouncement {
                phase: terminal,
                ..base.clone()
            };
            for delayed in [Phase::Prepare, Phase::Aligned] {
                let merged = merge_direct_announcement(
                    decided.clone(),
                    BarrierAnnouncement {
                        phase: delayed,
                        ..base.clone()
                    },
                )
                .unwrap();
                assert_eq!(merged.phase, terminal);
            }
        }

        let commit = BarrierAnnouncement {
            phase: Phase::Commit,
            ..base.clone()
        };
        let conflicting_abort = BarrierAnnouncement {
            phase: Phase::Abort,
            ..base.clone()
        };
        assert!(merge_direct_announcement(commit, conflicting_abort).is_err());

        let newer_attempt = BarrierAnnouncement {
            epoch: base.epoch + 1,
            checkpoint_id: base.checkpoint_id + 1,
            ..base
        };
        assert_eq!(
            merge_direct_announcement(
                BarrierAnnouncement {
                    epoch: 20,
                    checkpoint_id: 200,
                    phase: Phase::Commit,
                    ..newer_attempt.clone()
                },
                newer_attempt.clone(),
            )
            .unwrap()
            .checkpoint_id,
            201,
            "both attempt dimensions advanced"
        );

        for conflicting in [
            BarrierAnnouncement {
                epoch: 20,
                checkpoint_id: 201,
                phase: Phase::Prepare,
                ..newer_attempt.clone()
            },
            BarrierAnnouncement {
                epoch: 21,
                checkpoint_id: 200,
                phase: Phase::Prepare,
                ..newer_attempt.clone()
            },
        ] {
            assert!(merge_direct_announcement(newer_attempt.clone(), conflicting).is_err());
        }
    }

    #[test]
    fn durable_terminal_is_authoritative_during_channel_merge() {
        let base = BarrierAnnouncement {
            epoch: 21,
            checkpoint_id: 210,
            assignment_fence: None,
            leader_proof: None,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        };

        for (direct, durable) in [(Phase::Commit, Phase::Abort), (Phase::Abort, Phase::Commit)] {
            let merged = merge_observed_announcement(
                BarrierAnnouncement {
                    phase: direct,
                    ..base.clone()
                },
                BarrierAnnouncement {
                    phase: durable,
                    ..base.clone()
                },
            )
            .unwrap();
            assert_eq!(merged.phase, durable);
        }

        let merged = merge_observed_announcement(
            BarrierAnnouncement {
                phase: Phase::Commit,
                ..base.clone()
            },
            base.clone(),
        )
        .unwrap();
        assert_eq!(
            merged.phase,
            Phase::Commit,
            "lagging durable Prepare must not hide a delivered terminal phase"
        );

        for durable in [
            BarrierAnnouncement {
                epoch: base.epoch,
                checkpoint_id: base.checkpoint_id + 1,
                ..base.clone()
            },
            BarrierAnnouncement {
                epoch: base.epoch + 1,
                checkpoint_id: base.checkpoint_id,
                ..base.clone()
            },
            BarrierAnnouncement {
                epoch: base.epoch - 1,
                checkpoint_id: base.checkpoint_id + 1,
                ..base.clone()
            },
            BarrierAnnouncement {
                epoch: base.epoch + 1,
                checkpoint_id: base.checkpoint_id - 1,
                ..base.clone()
            },
        ] {
            assert!(merge_observed_announcement(base.clone(), durable).is_err());
        }

        assert!(merge_observed_announcement(
            base.clone(),
            BarrierAnnouncement {
                phase: Phase::Commit,
                flags: crate::checkpoint::flags::FULL_SNAPSHOT,
                ..base
            },
        )
        .is_err());
    }

    /// The gRPC-vs-gossip merge in `observe`: a newer attempt's
    /// gossip-only announcement (the early `Prepare` is KV-only)
    /// supersedes an older epoch in the gRPC watch, while lagging
    /// gossip for the same exact attempt never masks terminal progress.
    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn observe_merges_grpc_and_gossip_by_epoch() {
        let leader_kv = kv(NodeId(1));
        let follower_kv = kv(NodeId(2));
        let leader_coord = BarrierCoordinator::new(leader_kv.clone());
        let follower_coord = BarrierCoordinator::new(follower_kv.clone());

        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let slot = || Arc::new(parking_lot::RwLock::new(None));
        let leader_addr = leader_coord.start_server(addr, None, slot()).await.unwrap();
        let bound_addr = follower_coord
            .start_server(addr, None, slot())
            .await
            .unwrap();
        leader_kv.seed(NodeId(2), BARRIER_ADDR_KEY, bound_addr.to_string());
        follower_kv.seed(NodeId(1), BARRIER_ADDR_KEY, leader_addr.to_string());

        // Epoch 5 aborts — delivered over gRPC, lands in the
        // follower's latest-wins watch.
        leader_coord
            .announce(&BarrierAnnouncement {
                epoch: 5,
                checkpoint_id: 9,
                assignment_fence: None,
                leader_proof: None,
                phase: Phase::Abort,
                flags: 0,
                min_watermark_ms: None,
            })
            .await
            .unwrap();
        for _ in 0..100 {
            if let Some(ann) = follower_coord.observe(NodeId(1)).await.unwrap() {
                if ann.phase == Phase::Abort {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        // The next epoch's early Prepare reaches this follower via
        // gossip KV only (its prepare RPC comes later, at quorum time)
        // and must win the merge over the stale watch value.
        let next = serde_json::to_string(&BarrierAnnouncement {
            epoch: 6,
            checkpoint_id: 10,
            assignment_fence: None,
            leader_proof: None,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        })
        .unwrap();
        follower_kv.seed(NodeId(1), ANNOUNCEMENT_KEY, next);
        let got = follower_coord.observe(NodeId(1)).await.unwrap().unwrap();
        assert_eq!(got.epoch, 6);
        assert_eq!(got.phase, Phase::Prepare);

        // Same-epoch lagging gossip must not mask the fresher gRPC
        // value (RPC arrival order is authoritative within an epoch).
        let stale = serde_json::to_string(&BarrierAnnouncement {
            epoch: 5,
            checkpoint_id: 9,
            assignment_fence: None,
            leader_proof: None,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        })
        .unwrap();
        follower_kv.seed(NodeId(1), ANNOUNCEMENT_KEY, stale);
        let got = follower_coord.observe(NodeId(1)).await.unwrap().unwrap();
        assert_eq!(
            got.phase,
            Phase::Abort,
            "lagging gossip must not mask the fresher gRPC announcement",
        );
    }

    #[tokio::test]
    async fn leader_announces_follower_observes() {
        let leader_kv = kv(NodeId(1));
        let coord = BarrierCoordinator::new(leader_kv.clone());
        coord
            .announce(&BarrierAnnouncement {
                epoch: 5,
                checkpoint_id: 42,
                assignment_fence: None,
                leader_proof: None,
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

    fn announcement_json(epoch: u64, checkpoint_id: u64) -> String {
        serde_json::to_string(&BarrierAnnouncement {
            epoch,
            checkpoint_id,
            assignment_fence: None,
            leader_proof: None,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn max_announced_requires_consistent_attempt_dimensions() {
        let valid = kv(NodeId(1));
        valid.seed(NodeId(1), ANNOUNCEMENT_KEY, announcement_json(5, 50));
        valid.seed(NodeId(2), ANNOUNCEMENT_KEY, announcement_json(6, 60));
        assert_eq!(
            BarrierCoordinator::new(valid)
                .max_announced()
                .await
                .unwrap(),
            Some(CheckpointAttempt::new(6, 60))
        );

        for ((left_epoch, left_id), (right_epoch, right_id)) in [
            ((5, 50), (5, 51)),
            ((5, 50), (6, 50)),
            ((5, 51), (6, 50)),
            ((6, 50), (5, 51)),
        ] {
            let conflicting = kv(NodeId(1));
            conflicting.seed(
                NodeId(1),
                ANNOUNCEMENT_KEY,
                announcement_json(left_epoch, left_id),
            );
            conflicting.seed(
                NodeId(2),
                ANNOUNCEMENT_KEY,
                announcement_json(right_epoch, right_id),
            );
            assert!(BarrierCoordinator::new(conflicting)
                .max_announced()
                .await
                .is_err());
        }
    }

    fn certified_announcement(
        fence: crate::checkpoint::CheckpointAssignmentFence,
        proof: crate::cluster::control::LeaderProof,
        phase: Phase,
    ) -> BarrierAnnouncement {
        BarrierAnnouncement {
            epoch: 5,
            checkpoint_id: 50,
            assignment_fence: Some(fence),
            leader_proof: Some(proof),
            phase,
            flags: 0,
            min_watermark_ms: None,
        }
    }

    fn test_leader_proof() -> crate::cluster::control::LeaderProof {
        crate::cluster::control::LeaderProof {
            owner: crate::checkpoint::LeaderProofOwner {
                node_id: 1,
                boot_id: uuid::Uuid::from_u128(11),
                process_term: 3,
            },
            fencing_token: 7,
        }
    }

    fn plain_announcement(epoch: u64, checkpoint_id: u64) -> BarrierAnnouncement {
        BarrierAnnouncement {
            epoch,
            checkpoint_id,
            assignment_fence: None,
            leader_proof: None,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        }
    }

    #[tokio::test]
    async fn max_announced_rejects_exact_attempt_equivocation() {
        let fence = test_fence(9, &[1, 2], &[(1, 11), (2, 22)]);
        let proof = test_leader_proof();

        let cases = [
            (
                certified_announcement(fence.clone(), proof.clone(), Phase::Prepare),
                certified_announcement(
                    test_fence(10, &[1, 2], &[(1, 11), (2, 22)]),
                    proof.clone(),
                    Phase::Prepare,
                ),
            ),
            (
                certified_announcement(fence.clone(), proof.clone(), Phase::Prepare),
                certified_announcement(
                    fence.clone(),
                    crate::cluster::control::LeaderProof {
                        fencing_token: proof.fencing_token + 1,
                        ..proof.clone()
                    },
                    Phase::Prepare,
                ),
            ),
            (
                certified_announcement(fence.clone(), proof.clone(), Phase::Commit),
                certified_announcement(fence.clone(), proof.clone(), Phase::Abort),
            ),
            (
                certified_announcement(fence.clone(), proof.clone(), Phase::Prepare),
                BarrierAnnouncement {
                    flags: crate::checkpoint::flags::FULL_SNAPSHOT,
                    ..certified_announcement(fence.clone(), proof.clone(), Phase::Prepare)
                },
            ),
        ];

        for (left, right) in cases {
            let conflicting = kv(NodeId(1));
            conflicting.seed(
                NodeId(1),
                ANNOUNCEMENT_KEY,
                serde_json::to_string(&left).unwrap(),
            );
            conflicting.seed(
                NodeId(2),
                ANNOUNCEMENT_KEY,
                serde_json::to_string(&right).unwrap(),
            );
            assert!(BarrierCoordinator::new(conflicting)
                .max_announced()
                .await
                .is_err());
        }

        let progressing = kv(NodeId(1));
        progressing.seed(
            NodeId(1),
            ANNOUNCEMENT_KEY,
            serde_json::to_string(&certified_announcement(
                fence.clone(),
                proof.clone(),
                Phase::Prepare,
            ))
            .unwrap(),
        );
        progressing.seed(
            NodeId(2),
            ANNOUNCEMENT_KEY,
            serde_json::to_string(&certified_announcement(fence, proof, Phase::Commit)).unwrap(),
        );
        assert_eq!(
            BarrierCoordinator::new(progressing)
                .max_announced()
                .await
                .unwrap(),
            Some(CheckpointAttempt::new(5, 50))
        );
    }

    #[test]
    fn scanned_history_cannot_hide_earlier_conflicts_behind_a_newer_attempt() {
        let fence = test_fence(9, &[1, 2], &[(1, 11), (2, 22)]);
        let proof = test_leader_proof();
        let newer = plain_announcement(6, 60);
        let cases = [
            vec![
                plain_announcement(5, 50),
                newer.clone(),
                plain_announcement(5, 51),
            ],
            vec![
                certified_announcement(fence.clone(), proof.clone(), Phase::Prepare),
                newer.clone(),
                certified_announcement(
                    test_fence(10, &[1, 2], &[(1, 11), (2, 22)]),
                    proof.clone(),
                    Phase::Prepare,
                ),
            ],
            vec![
                certified_announcement(fence.clone(), proof.clone(), Phase::Commit),
                newer,
                certified_announcement(fence, proof, Phase::Abort),
            ],
        ];

        for history in cases {
            assert!(validate_scanned_announcements(history).is_err());
        }
    }

    #[tokio::test]
    async fn max_announced_rejects_malformed_history() {
        let malformed = kv(NodeId(1));
        malformed.seed(NodeId(2), ANNOUNCEMENT_KEY, "not-json".to_string());
        assert!(BarrierCoordinator::new(malformed)
            .max_announced()
            .await
            .is_err());
    }

    #[derive(Debug)]
    struct FailingScanKv;

    #[async_trait]
    impl ClusterKv for FailingScanKv {
        async fn write(&self, _key: &str, _value: String) {}

        async fn read_from(&self, _who: NodeId, _key: &str) -> Option<String> {
            None
        }

        async fn scan(&self, _key: &str) -> Vec<(NodeId, String)> {
            Vec::new()
        }

        async fn scan_checked(&self, _key: &str) -> Result<Vec<(NodeId, String)>, String> {
            Err("injected scan failure".to_string())
        }
    }

    #[tokio::test]
    async fn max_announced_propagates_scan_failure() {
        let coordinator = BarrierCoordinator::new(Arc::new(FailingScanKv));
        let error = coordinator.max_announced().await.unwrap_err();
        assert_eq!(error, "injected scan failure");
    }

    #[tokio::test]
    async fn quorum_reached_when_all_ack_success() {
        let k = kv(NodeId(1));
        let ack_json = serde_json::to_string(&BarrierAck {
            epoch: 7,
            checkpoint_id: 7,
            assignment_digest: None,
            ok: true,
            error: None,
            watermark: CheckpointWatermark::Uninitialized,
        })
        .unwrap();
        k.seed(NodeId(2), ACK_KEY, ack_json.clone());
        k.seed(NodeId(3), ACK_KEY, ack_json);

        let coord = BarrierCoordinator::new(k);
        let prepare = BarrierAnnouncement {
            epoch: 7,
            checkpoint_id: 7,
            assignment_fence: None,
            leader_proof: None,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        };
        let outcome = coord
            .wait_for_quorum(
                &prepare,
                &[NodeId(2), NodeId(3)],
                Duration::from_millis(200),
            )
            .await;
        match outcome {
            QuorumOutcome::Reached {
                mut acks,
                follower_watermark,
            } => {
                acks.sort_by_key(|n| n.0);
                assert_eq!(acks, vec![NodeId(2), NodeId(3)]);
                assert_eq!(follower_watermark, CheckpointWatermark::Uninitialized);
            }
            other => panic!("expected Reached, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn uninitialized_participant_blocks_cluster_watermark_advancement() {
        let k = kv(NodeId(1));
        for (node, watermark) in [
            (NodeId(2), CheckpointWatermark::Active(100)),
            (NodeId(3), CheckpointWatermark::Uninitialized),
        ] {
            k.seed(
                node,
                ACK_KEY,
                serde_json::to_string(&BarrierAck {
                    epoch: 7,
                    checkpoint_id: 7,
                    assignment_digest: None,
                    ok: true,
                    error: None,
                    watermark,
                })
                .unwrap(),
            );
        }

        let outcome = BarrierCoordinator::new(k)
            .wait_for_quorum(
                &BarrierAnnouncement {
                    epoch: 7,
                    checkpoint_id: 7,
                    assignment_fence: None,
                    leader_proof: None,
                    phase: Phase::Prepare,
                    flags: 0,
                    min_watermark_ms: None,
                },
                &[NodeId(2), NodeId(3)],
                Duration::from_millis(200),
            )
            .await;

        assert!(matches!(
            outcome,
            QuorumOutcome::Reached {
                follower_watermark: CheckpointWatermark::Uninitialized,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn idle_participant_is_excluded_from_cluster_watermark_minimum() {
        let k = kv(NodeId(1));
        for (node, watermark) in [
            (NodeId(2), CheckpointWatermark::Active(100)),
            (NodeId(3), CheckpointWatermark::Idle),
        ] {
            k.seed(
                node,
                ACK_KEY,
                serde_json::to_string(&BarrierAck {
                    epoch: 7,
                    checkpoint_id: 7,
                    assignment_digest: None,
                    ok: true,
                    error: None,
                    watermark,
                })
                .unwrap(),
            );
        }

        let outcome = BarrierCoordinator::new(k)
            .wait_for_quorum(
                &BarrierAnnouncement {
                    epoch: 7,
                    checkpoint_id: 7,
                    assignment_fence: None,
                    leader_proof: None,
                    phase: Phase::Prepare,
                    flags: 0,
                    min_watermark_ms: None,
                },
                &[NodeId(2), NodeId(3)],
                Duration::from_millis(200),
            )
            .await;

        assert!(matches!(
            outcome,
            QuorumOutcome::Reached {
                follower_watermark: CheckpointWatermark::Active(100),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn quorum_timeout_when_follower_silent() {
        let k = kv(NodeId(1));
        let ack_json = serde_json::to_string(&BarrierAck {
            epoch: 8,
            checkpoint_id: 8,
            assignment_digest: None,
            ok: true,
            error: None,
            watermark: CheckpointWatermark::Uninitialized,
        })
        .unwrap();
        k.seed(NodeId(2), ACK_KEY, ack_json);

        let coord = BarrierCoordinator::new(k);
        let prepare = BarrierAnnouncement {
            epoch: 8,
            checkpoint_id: 8,
            assignment_fence: None,
            leader_proof: None,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        };
        let outcome = coord
            .wait_for_quorum(
                &prepare,
                &[NodeId(2), NodeId(3)],
                Duration::from_millis(150),
            )
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
            checkpoint_id: 9,
            assignment_digest: None,
            ok: true,
            error: None,
            watermark: CheckpointWatermark::Uninitialized,
        })
        .unwrap();
        let bad = serde_json::to_string(&BarrierAck {
            epoch: 9,
            checkpoint_id: 9,
            assignment_digest: None,
            ok: false,
            error: Some("state snapshot failed: disk full".into()),
            watermark: CheckpointWatermark::Uninitialized,
        })
        .unwrap();
        k.seed(NodeId(2), ACK_KEY, good);
        k.seed(NodeId(3), ACK_KEY, bad);

        let coord = BarrierCoordinator::new(k);
        let prepare = BarrierAnnouncement {
            epoch: 9,
            checkpoint_id: 9,
            assignment_fence: None,
            leader_proof: None,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        };
        let outcome = coord
            .wait_for_quorum(&prepare, &[NodeId(2), NodeId(3)], Duration::from_secs(2))
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
        let stale = serde_json::to_string(&BarrierAck {
            epoch: 9,
            checkpoint_id: 9,
            assignment_digest: None,
            ok: true,
            error: None,
            watermark: CheckpointWatermark::Uninitialized,
        })
        .unwrap();
        k.seed(NodeId(2), ACK_KEY, stale);

        let coord = BarrierCoordinator::new(k);
        let prepare = BarrierAnnouncement {
            epoch: 10,
            checkpoint_id: 10,
            assignment_fence: None,
            leader_proof: None,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        };
        let outcome = coord
            .wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_millis(100))
            .await;
        assert!(
            matches!(outcome, QuorumOutcome::TimedOut { .. }),
            "stale-epoch ack must not satisfy quorum"
        );
    }

    #[tokio::test]
    async fn wrong_checkpoint_or_assignment_ack_is_ignored() {
        let expected_fence = test_fence(4, &[1, 2], &[(1, 11), (2, 22)]);
        let prepare = BarrierAnnouncement {
            epoch: 10,
            checkpoint_id: 100,
            assignment_fence: Some(expected_fence.clone()),
            leader_proof: None,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        };
        let wrong_fence = test_fence(4, &[1, 2], &[(1, 111), (2, 22)]);

        for ack in [
            BarrierAck {
                epoch: 10,
                checkpoint_id: 99,
                assignment_digest: Some(expected_fence.digest()),
                ok: true,
                error: None,
                watermark: CheckpointWatermark::Uninitialized,
            },
            BarrierAck {
                epoch: 10,
                checkpoint_id: 100,
                assignment_digest: Some(wrong_fence.digest()),
                ok: true,
                error: None,
                watermark: CheckpointWatermark::Uninitialized,
            },
        ] {
            let k = kv(NodeId(1));
            k.seed(NodeId(2), ACK_KEY, serde_json::to_string(&ack).unwrap());
            let coord = BarrierCoordinator::new(k);
            let outcome = coord
                .wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_millis(50))
                .await;
            assert!(
                matches!(outcome, QuorumOutcome::TimedOut { .. }),
                "ack for a different exact attempt/certificate must not satisfy quorum"
            );
        }
    }
}
