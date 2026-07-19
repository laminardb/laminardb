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

#[cfg(feature = "cluster")]
const BARRIER_ENDPOINT_VERSION: u8 = 1;

#[cfg(feature = "cluster")]
const MAX_BARRIER_ENDPOINT_BYTES: usize = 1_024;

/// Process identity attached to one control-plane endpoint. The durable process lease remains the
/// authority; this value prevents a stable node id from resolving to a different process boot.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BarrierProcessIdentity {
    node_id: u64,
    boot_incarnation: uuid::Uuid,
    process_term: u64,
}

#[cfg(feature = "cluster")]
impl BarrierProcessIdentity {
    fn from_process_lease(lease: &super::ProcessLease) -> Result<Self, String> {
        lease
            .validate(lease.node)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            node_id: lease.node.0,
            boot_incarnation: lease.owner,
            process_term: lease.term,
        })
    }

    const fn is_canonical(self) -> bool {
        self.node_id != 0 && !self.boot_incarnation.is_nil() && self.process_term != 0
    }

    fn matches_participant(self, participant: &crate::checkpoint::CheckpointParticipant) -> bool {
        self.node_id == participant.node_id && self.boot_incarnation == participant.boot_incarnation
    }
}

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BarrierEndpointRecord {
    version: u8,
    address: String,
    process: BarrierProcessIdentity,
}

#[cfg(feature = "cluster")]
impl BarrierEndpointRecord {
    fn new(address: String, process: BarrierProcessIdentity) -> Result<Self, String> {
        let record = Self {
            version: BARRIER_ENDPOINT_VERSION,
            address,
            process,
        };
        record.validate()?;
        Ok(record)
    }

    fn validate(&self) -> Result<(), String> {
        if self.version != BARRIER_ENDPOINT_VERSION {
            return Err(format!(
                "unsupported cluster control endpoint version {}",
                self.version
            ));
        }
        if self.address.is_empty() || self.address.len() > MAX_BARRIER_ENDPOINT_BYTES / 2 {
            return Err("cluster control endpoint address is empty or oversized".into());
        }
        if !self.process.is_canonical() {
            return Err("cluster control endpoint process identity is not canonical".into());
        }
        Ok(())
    }

    fn encode(&self) -> Result<String, String> {
        let encoded = serde_json::to_string(self).map_err(|error| error.to_string())?;
        if encoded.len() > MAX_BARRIER_ENDPOINT_BYTES {
            return Err("cluster control endpoint advertisement is oversized".into());
        }
        Ok(encoded)
    }
}

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy)]
struct ExpectedBarrierProcess {
    node_id: u64,
    boot_incarnation: uuid::Uuid,
    process_term: Option<u64>,
}

#[cfg(feature = "cluster")]
impl ExpectedBarrierProcess {
    const fn participant(node_id: u64, boot_incarnation: uuid::Uuid) -> Self {
        Self {
            node_id,
            boot_incarnation,
            process_term: None,
        }
    }

    const fn exact(process: &crate::checkpoint::LeaderProofOwner) -> Self {
        Self {
            node_id: process.node_id,
            boot_incarnation: process.boot_id,
            process_term: Some(process.process_term),
        }
    }

    fn matches(self, actual: BarrierProcessIdentity) -> bool {
        self.node_id == actual.node_id
            && self.boot_incarnation == actual.boot_incarnation
            && self
                .process_term
                .is_none_or(|term| term == actual.process_term)
    }
}

#[cfg(feature = "cluster")]
fn decode_barrier_endpoint(raw: &str) -> Result<(String, Option<BarrierProcessIdentity>), String> {
    if raw.len() > MAX_BARRIER_ENDPOINT_BYTES {
        return Err("cluster control endpoint advertisement is oversized".into());
    }
    if !raw.trim_start().starts_with('{') {
        if raw.is_empty() {
            return Err("cluster control endpoint address is empty".into());
        }
        return Ok((raw.to_string(), None));
    }
    let record: BarrierEndpointRecord = serde_json::from_str(raw)
        .map_err(|error| format!("invalid cluster control endpoint advertisement: {error}"))?;
    record.validate()?;
    Ok((record.address, Some(record.process)))
}

#[cfg(feature = "cluster")]
fn encode_barrier_endpoint(
    address: &str,
    process: Option<BarrierProcessIdentity>,
) -> Result<String, String> {
    if address.is_empty() || address.len() > MAX_BARRIER_ENDPOINT_BYTES / 2 {
        return Err("cluster control endpoint address is empty or oversized".into());
    }
    process.map_or_else(
        || Ok(address.to_string()),
        |process| BarrierEndpointRecord::new(address.to_string(), process)?.encode(),
    )
}

/// Upper bound for a non-Prepare phase notification round. The durable KV
/// announcement is authoritative; direct gRPC delivery is only the low-latency
/// path and must never hold the checkpoint coordinator indefinitely.
#[cfg(feature = "cluster")]
const PHASE_RPC_TIMEOUT: Duration = Duration::from_secs(3);

#[cfg(feature = "cluster")]
const PREPARE_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(10);

#[cfg(feature = "cluster")]
const PREPARE_RETRY_MAX_BACKOFF: Duration = Duration::from_millis(250);

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

/// Merge one exact attempt from a single history. A successor may settle a reversible phase under
/// a new assignment and leader proof, but reversible equivocation and opposing decisions fail
/// closed.
fn merge_history_exact(
    current: BarrierAnnouncement,
    incoming: BarrierAnnouncement,
    history: &str,
) -> Result<BarrierAnnouncement, String> {
    if current.flags != incoming.flags {
        return Err(format!(
            "conflicting {history} barrier flags for exact attempt ({}, {})",
            current.epoch, current.checkpoint_id
        ));
    }
    if is_terminal_phase(current.phase) && is_terminal_phase(incoming.phase) {
        if current.phase != incoming.phase {
            return Err(format!(
                "conflicting {history} terminal phases for exact attempt ({}, {})",
                current.epoch, current.checkpoint_id
            ));
        }
        if !same_announcement_identity(&current, &incoming) {
            return Err(format!(
                "conflicting {history} barrier certificates for exact attempt ({}, {})",
                current.epoch, current.checkpoint_id
            ));
        }
        return Ok(current);
    }
    if is_terminal_phase(current.phase) {
        return Ok(current);
    }
    if is_terminal_phase(incoming.phase) {
        return Ok(incoming);
    }
    if !same_announcement_identity(&current, &incoming) {
        return Err(format!(
            "conflicting {history} barrier certificates for exact attempt ({}, {})",
            current.epoch, current.checkpoint_id
        ));
    }
    if current.phase == Phase::Aligned && incoming.phase == Phase::Prepare {
        Ok(current)
    } else {
        Ok(incoming)
    }
}

/// Merge direct deliveries without allowing a delayed phase to regress the same exact attempt.
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
        CheckpointAttemptRelation::Exact => merge_history_exact(current, incoming, "direct"),
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
            if is_terminal_phase(durable.phase) {
                Ok(durable)
            } else if is_terminal_phase(grpc.phase) {
                Ok(grpc)
            } else if !same_announcement_identity(&grpc, &durable) {
                Err(format!(
                    "conflicting direct and durable certificates for exact attempt ({}, {})",
                    grpc.epoch, grpc.checkpoint_id
                ))
            } else if grpc.phase == Phase::Aligned && durable.phase == Phase::Prepare {
                Ok(grpc)
            } else {
                Ok(durable)
            }
        }
    }
}

/// Merge per-node durable histories for leader reclamation. A successor terminal may carry a new
/// certificate; reversible phases must retain one certificate and terminal outcomes must agree.
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
        CheckpointAttemptRelation::Exact => merge_history_exact(current, incoming, "durable"),
    }
}

fn validate_scanned_announcements(
    mut announcements: Vec<BarrierAnnouncement>,
) -> Result<Option<BarrierAnnouncement>, String> {
    // Group exact attempts and audit every reversible certificate before a terminal can absorb it.
    // Ordering authority remains `relation_to`.
    announcements.sort_unstable_by_key(|announcement| {
        (
            announcement.epoch,
            announcement.checkpoint_id,
            is_terminal_phase(announcement.phase),
        )
    });
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

/// Per-peer barrier gRPC client pool. A stable node id may be reused by a replacement process,
/// so every cached channel is bound to the boot incarnation certified by its assignment or
/// leader proof.
#[cfg(feature = "cluster")]
#[derive(Clone)]
struct BarrierClientEntry {
    process: Option<BarrierProcessIdentity>,
    client: barrier_v1::barrier_sync_client::BarrierSyncClient<tonic::transport::Channel>,
}

#[cfg(feature = "cluster")]
type BarrierClientPool = Arc<parking_lot::Mutex<FxHashMap<NodeId, BarrierClientEntry>>>;

#[cfg(feature = "cluster")]
#[derive(Debug)]
enum BarrierClientResolutionError {
    ProcessMismatch,
    Invalid(String),
}

#[cfg(feature = "cluster")]
impl std::fmt::Display for BarrierClientResolutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProcessMismatch => formatter.write_str("endpoint belongs to a different process"),
            Self::Invalid(error) => formatter.write_str(error),
        }
    }
}

#[cfg(feature = "cluster")]
fn barrier_client_process_matches(
    expected: Option<ExpectedBarrierProcess>,
    actual: Option<BarrierProcessIdentity>,
) -> bool {
    match (expected, actual) {
        (None, None) => true,
        (Some(expected), Some(actual)) => expected.matches(actual),
        (None, Some(_)) | (Some(_), None) => false,
    }
}

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
    /// The one clustered Prepare batch admitted after its durable announcement. Pending tasks are
    /// owned here; once claimed, `wait_for_quorum` owns their structured cancellation. The
    /// orchestration must finish or drop that future before publishing a terminal or successor.
    prepare_fanout: parking_lot::Mutex<Option<PrepareFanoutState>>,
    /// Serializes local durable publication with the corresponding Prepare fan-out transition.
    /// Non-Prepare network delivery runs after this lock is released.
    announcement_lock: tokio::sync::Mutex<()>,
    clients: BarrierClientPool,
    server_handle: Arc<parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    relay_handle: Arc<parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    advertise_addr: String,
    local_process: Arc<std::sync::OnceLock<BarrierProcessIdentity>>,
}

#[cfg(feature = "cluster")]
fn abort_grpc_tasks(state: &GrpcState) {
    if let Some(handle) = state.server_handle.lock().take() {
        handle.abort();
    }
    if let Some(handle) = state.relay_handle.lock().take() {
        handle.abort();
    }
}

#[cfg(feature = "cluster")]
type ActiveLeaderState = Option<(NodeId, watch::Receiver<Vec<NodeInfo>>, Arc<AtomicBool>)>;

#[cfg(feature = "cluster")]
fn leader_proof_challenge_from_wire(bytes: &[u8]) -> Result<uuid::Uuid, tonic::Status> {
    let challenge = uuid::Uuid::from_slice(bytes).map_err(|_| {
        tonic::Status::invalid_argument("Leader proof challenge must contain exactly 16 bytes")
    })?;
    if challenge.is_nil() {
        return Err(tonic::Status::invalid_argument(
            "Leader proof challenge must be nonzero",
        ));
    }
    Ok(challenge)
}

#[cfg(feature = "cluster")]
fn leader_proof_ack_matches(challenge: uuid::Uuid, acknowledged: &[u8]) -> bool {
    acknowledged == challenge.as_bytes()
}

#[cfg(feature = "cluster")]
pub(crate) type LocalLeaderProofProvider =
    Arc<dyn Fn() -> Option<super::LeaderProof> + Send + Sync>;

#[cfg(feature = "cluster")]
struct GrpcBarrierServer {
    incoming_tx: crossfire::MAsyncTx<BarrierFlavor>,
    prepare_acks: Arc<parking_lot::Mutex<PrepareAckState>>,
    leader_lease_store: Arc<parking_lot::Mutex<Option<Arc<super::LeaderLeaseStore>>>>,
    local_leader_proof: Arc<parking_lot::Mutex<Option<LocalLeaderProofProvider>>>,
    local_process: Arc<std::sync::OnceLock<BarrierProcessIdentity>>,
    process_lease_deadline: Arc<std::sync::OnceLock<Arc<super::LeaseDeadline>>>,
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
        partitioning_abi_version: crate::state::PARTITIONING_ABI_VERSION,
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
    fn require_live_process_lease(&self) -> Result<Option<&super::LeaseDeadline>, tonic::Status> {
        if self.local_process.get().is_none() {
            return Ok(None);
        }
        let deadline = self.process_lease_deadline.get().ok_or_else(|| {
            tonic::Status::failed_precondition("Process lease deadline is not installed")
        })?;
        if !deadline.is_live() {
            return Err(tonic::Status::failed_precondition(
                "Process lease deadline has expired",
            ));
        }
        Ok(Some(deadline))
    }

    fn require_exact_local_proof_process(
        &self,
        proof: &super::LeaderProof,
    ) -> Result<(), tonic::Status> {
        let local = self.local_process.get().copied().ok_or_else(|| {
            tonic::Status::failed_precondition(
                "Leader proof confirmation requires a process-bound endpoint",
            )
        })?;
        if local.node_id != proof.owner.node_id
            || local.boot_incarnation != proof.owner.boot_id
            || local.process_term != proof.owner.process_term
        {
            return Err(tonic::Status::failed_precondition(
                "Leader proof challenge does not target this exact process",
            ));
        }
        Ok(())
    }

    async fn enqueue_while_process_live(
        &self,
        announcement: BarrierAnnouncement,
    ) -> Result<(), tonic::Status> {
        let deadline = self.require_live_process_lease()?;
        let send_result = if let Some(deadline) = deadline {
            tokio::select! {
                biased;
                () = deadline.wait_until_expired() => {
                    return Err(tonic::Status::failed_precondition(
                        "Process lease deadline expired before barrier delivery",
                    ));
                }
                result = self.incoming_tx.send(announcement) => result,
            }
        } else {
            self.incoming_tx.send(announcement).await
        };
        if send_result.is_err() {
            return Err(tonic::Status::aborted("Follower coordinator shutdown"));
        }
        self.require_live_process_lease()?;
        Ok(())
    }

    async fn wait_for_prepare_ack(
        &self,
        rx: tokio::sync::oneshot::Receiver<BarrierAck>,
    ) -> Result<BarrierAck, tonic::Status> {
        let deadline = self.require_live_process_lease()?;
        let wait_for_ack = async {
            match tokio::time::timeout(PREPARE_RPC_TIMEOUT, rx).await {
                Ok(Ok(ack)) => Ok(ack),
                Ok(Err(_)) => Err(tonic::Status::internal("Ack sender dropped")),
                Err(_) => Err(tonic::Status::deadline_exceeded(
                    "Follower checkpoint prepare timed out",
                )),
            }
        };
        tokio::pin!(wait_for_ack);
        if let Some(deadline) = deadline {
            tokio::select! {
                biased;
                () = deadline.wait_until_expired() => Err(tonic::Status::failed_precondition(
                    "Process lease deadline expired while awaiting Prepare acknowledgement",
                )),
                result = &mut wait_for_ack => result,
            }
        } else {
            wait_for_ack.await
        }
    }

    fn require_local_assignment_process(
        &self,
        fence: Option<&super::CheckpointAssignmentFence>,
    ) -> Result<(), tonic::Status> {
        match (self.local_process.get().copied(), fence) {
            (None, None) => Ok(()),
            (Some(_), None) => Err(tonic::Status::failed_precondition(
                "Assignment-less barrier cannot target a process-bound cluster endpoint",
            )),
            (None, Some(_)) => Err(tonic::Status::failed_precondition(
                "Certified barrier cannot target a process-unbound control endpoint",
            )),
            (Some(local), Some(fence))
                if fence.participant_incarnation(local.node_id) == Some(local.boot_incarnation) =>
            {
                Ok(())
            }
            (Some(_), Some(_)) => Err(tonic::Status::failed_precondition(
                "Certified barrier does not target this exact process",
            )),
        }
    }

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
    async fn confirm_leader_proof(
        &self,
        request: tonic::Request<barrier_v1::LeaderProofChallenge>,
    ) -> Result<tonic::Response<barrier_v1::LeaderProofAck>, tonic::Status> {
        self.require_live_process_lease()?;
        let requested = request.into_inner();
        let expected = leader_proof_from_wire(requested.expected_proof)?
            .ok_or_else(|| tonic::Status::invalid_argument("Leader proof challenge is missing"))?;
        leader_proof_challenge_from_wire(&requested.challenge_id)?;
        self.require_exact_local_proof_process(&expected)?;
        let provider = self.local_leader_proof.lock().clone().ok_or_else(|| {
            tonic::Status::failed_precondition("Local leader proof provider is not installed")
        })?;
        let local = provider().ok_or_else(|| {
            tonic::Status::failed_precondition("No process-local leader grant is live")
        })?;
        if local != expected {
            return Err(tonic::Status::failed_precondition(
                "Live process-local leader grant does not match the durable proof challenge",
            ));
        }
        self.require_live_process_lease()?;
        Ok(tonic::Response::new(barrier_v1::LeaderProofAck {
            challenge_id: requested.challenge_id,
        }))
    }

    async fn prepare(
        &self,
        request: tonic::Request<barrier_v1::PrepareRequest>,
    ) -> Result<tonic::Response<barrier_v1::Ack>, tonic::Status> {
        self.require_live_process_lease()?;
        let req = request.into_inner();
        let assignment_fence = assignment_fence_from_wire(
            req.assignment_version,
            req.assignment_vnode_count,
            req.assignment_map_digest,
            req.assignment_participants,
        )?;
        self.require_local_assignment_process(assignment_fence.as_ref())?;
        let leader_proof = self
            .validate_reversible_leader(req.leader_proof.clone())
            .await?;
        let attempt = CheckpointAttempt::new(req.epoch, req.checkpoint_id);
        let assignment_digest = assignment_fence
            .as_ref()
            .map(super::CheckpointAssignmentFence::digest);
        let identity = BarrierIdentity {
            attempt,
            assignment_digest,
        };

        self.require_live_process_lease()?;
        let (waiter_id, rx) = {
            let mut state = self.prepare_acks.lock();
            state.record_receipt(identity);
            if let Some(ack) = state.completed.get(&identity) {
                self.require_live_process_lease()?;
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

        self.enqueue_while_process_live(ann).await?;

        let ack = self.wait_for_prepare_ack(rx).await?;
        if ack.checkpoint_id != attempt.checkpoint_id || ack.assignment_digest != assignment_digest
        {
            return Err(tonic::Status::failed_precondition(
                "Follower acknowledgement identity mismatch",
            ));
        }
        self.require_latest_proof(&leader_proof).await?;
        self.require_live_process_lease()?;
        Ok(tonic::Response::new(grpc_ack(ack)))
    }

    async fn aligned(
        &self,
        request: tonic::Request<barrier_v1::AlignedRequest>,
    ) -> Result<tonic::Response<barrier_v1::Ack>, tonic::Status> {
        self.require_live_process_lease()?;
        let req = request.into_inner();
        let assignment_fence = assignment_fence_from_wire(
            req.assignment_version,
            req.assignment_vnode_count,
            req.assignment_map_digest,
            req.assignment_participants,
        )?;
        self.require_local_assignment_process(assignment_fence.as_ref())?;
        let leader_proof = self
            .validate_reversible_leader(req.leader_proof.clone())
            .await?;

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
        self.enqueue_while_process_live(ann).await?;
        self.require_live_process_lease()?;
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
        self.require_live_process_lease()?;
        let req = request.into_inner();
        let leader_proof = leader_proof_from_wire(req.leader_proof.clone())?;
        let assignment_fence = assignment_fence_from_wire(
            req.assignment_version,
            req.assignment_vnode_count,
            req.assignment_map_digest,
            req.assignment_participants,
        )?;
        self.require_local_assignment_process(assignment_fence.as_ref())?;

        let ann = BarrierAnnouncement {
            epoch: req.epoch,
            checkpoint_id: req.checkpoint_id,
            assignment_fence: assignment_fence.clone(),
            leader_proof,
            phase: Phase::Commit,
            flags: req.flags,
            min_watermark_ms: req.min_watermark_ms,
        };
        self.enqueue_while_process_live(ann).await?;
        self.require_live_process_lease()?;
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
        self.require_live_process_lease()?;
        let req = request.into_inner();
        let leader_proof = leader_proof_from_wire(req.leader_proof.clone())?;
        let assignment_fence = assignment_fence_from_wire(
            req.assignment_version,
            req.assignment_vnode_count,
            req.assignment_map_digest,
            req.assignment_participants,
        )?;
        self.require_local_assignment_process(assignment_fence.as_ref())?;

        let ann = BarrierAnnouncement {
            epoch: req.epoch,
            checkpoint_id: req.checkpoint_id,
            assignment_fence: assignment_fence.clone(),
            leader_proof,
            phase: Phase::Abort,
            flags: req.flags,
            min_watermark_ms: None,
        };
        self.enqueue_while_process_live(ann).await?;
        self.require_live_process_lease()?;
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
    expected_process: Option<ExpectedBarrierProcess>,
    pool: &BarrierClientPool,
    kv: &Arc<dyn ClusterKv>,
) -> Result<
    Option<barrier_v1::barrier_sync_client::BarrierSyncClient<tonic::transport::Channel>>,
    BarrierClientResolutionError,
> {
    {
        let pool = pool.lock();
        if let Some(entry) = pool.get(&peer) {
            if barrier_client_process_matches(expected_process, entry.process) {
                return Ok(Some(entry.client.clone()));
            }
        }
    }

    let Some(raw_endpoint) = kv.read_from(peer, BARRIER_ADDR_KEY).await else {
        return Ok(None);
    };
    let (address, published_process) =
        decode_barrier_endpoint(&raw_endpoint).map_err(BarrierClientResolutionError::Invalid)?;
    if let Some(expected) = expected_process {
        let actual = published_process.ok_or(BarrierClientResolutionError::ProcessMismatch)?;
        if actual.node_id != peer.0 {
            return Err(BarrierClientResolutionError::Invalid(format!(
                "cluster control endpoint slot {} advertises node {}",
                peer.0, actual.node_id
            )));
        }
        if !expected.matches(actual) {
            return Err(BarrierClientResolutionError::ProcessMismatch);
        }
    } else if let Some(process) = published_process {
        if process.node_id != peer.0 {
            return Err(BarrierClientResolutionError::Invalid(format!(
                "cluster control endpoint slot {} advertises a different node",
                peer.0
            )));
        }
        return Err(BarrierClientResolutionError::ProcessMismatch);
    }
    let endpoint =
        super::tls::client_endpoint(&address).map_err(BarrierClientResolutionError::Invalid)?;
    let channel = endpoint.connect_lazy();
    let client = barrier_v1::barrier_sync_client::BarrierSyncClient::new(channel);

    let mut pool = pool.lock();
    if let Some(entry) = pool.get(&peer) {
        if barrier_client_process_matches(expected_process, entry.process) {
            return Ok(Some(entry.client.clone()));
        }
        if expected_process.is_none() && entry.process.is_some() {
            return Ok(Some(client));
        }
    }
    while pool.len() >= crate::checkpoint::MAX_CHECKPOINT_PARTICIPANTS && !pool.contains_key(&peer)
    {
        let victim = if expected_process.is_none() {
            pool.iter()
                .find_map(|(node, entry)| entry.process.is_none().then_some(*node))
        } else {
            pool.keys().next().copied()
        };
        let Some(victim) = victim else {
            break;
        };
        pool.remove(&victim);
    }
    if pool.len() >= crate::checkpoint::MAX_CHECKPOINT_PARTICIPANTS && !pool.contains_key(&peer) {
        return Ok(Some(client));
    }
    pool.insert(
        peer,
        BarrierClientEntry {
            process: published_process,
            client: client.clone(),
        },
    );
    Ok(Some(client))
}

#[cfg(feature = "cluster")]
fn evict_barrier_client(
    pool: &BarrierClientPool,
    peer: NodeId,
    expected_process: Option<ExpectedBarrierProcess>,
) {
    let mut pool = pool.lock();
    let remove = pool
        .get(&peer)
        .is_some_and(|entry| barrier_client_process_matches(expected_process, entry.process));
    if remove {
        pool.remove(&peer);
    }
}

#[cfg(feature = "cluster")]
async fn call_phase_rpc(
    client: &mut barrier_v1::barrier_sync_client::BarrierSyncClient<tonic::transport::Channel>,
    ann: &BarrierAnnouncement,
    request_timeout: Duration,
) -> Result<(), String> {
    let assignment = assignment_fence_to_wire(ann.assignment_fence.as_ref());
    match ann.phase {
        Phase::Aligned => {
            let mut request = tonic::Request::new(barrier_v1::AlignedRequest {
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
            request.set_timeout(request_timeout);
            client
                .aligned(request)
                .await
                .map_err(|error| error.to_string())
                .and_then(|response| validate_phase_ack(&response.into_inner(), ann))
        }
        Phase::Commit => {
            let mut request = tonic::Request::new(barrier_v1::CommitRequest {
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
            request.set_timeout(request_timeout);
            client
                .commit(request)
                .await
                .map_err(|error| error.to_string())
                .and_then(|response| validate_phase_ack(&response.into_inner(), ann))
        }
        Phase::Abort => {
            let mut request = tonic::Request::new(barrier_v1::AbortRequest {
                epoch: ann.epoch,
                checkpoint_id: ann.checkpoint_id,
                flags: ann.flags,
                assignment_version: assignment.version,
                assignment_participants: assignment.participants,
                assignment_vnode_count: assignment.vnode_count,
                assignment_map_digest: assignment.map_digest,
                leader_proof: leader_proof_to_wire(ann.leader_proof.as_ref()),
            });
            request.set_timeout(request_timeout);
            client
                .abort(request)
                .await
                .map_err(|error| error.to_string())
                .and_then(|response| validate_phase_ack(&response.into_inner(), ann))
        }
        Phase::Prepare => Err("Prepare cannot use the phase-notification RPC path".into()),
    }
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

    let expected_process = ann
        .assignment_fence
        .as_ref()
        .map(|fence| {
            fence
                .participant_incarnation(peer.0)
                .map(|boot| ExpectedBarrierProcess::participant(peer.0, boot))
                .ok_or_else(|| {
                    format!("{rpc} RPC peer {} is outside the assignment roster", peer.0)
                })
        })
        .transpose()?;
    let result = tokio::time::timeout_at(deadline, async {
        let mut client = get_barrier_client(peer, expected_process, &clients_pool, &kv)
            .await
            .map_err(|error| format!("failed to resolve peer {}: {error}", peer.0))?
            .ok_or_else(|| format!("failed to get client for peer {}", peer.0))?;
        let request_timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        call_phase_rpc(&mut client, &ann, request_timeout).await
    })
    .await;

    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            evict_barrier_client(&clients_pool, peer, expected_process);
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
            evict_barrier_client(&clients_pool, peer, expected_process);
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

/// Exact announcement and participant roster bound to one eager Prepare round.
#[cfg(feature = "cluster")]
struct PrepareFanoutBatch {
    announcement: BarrierAnnouncement,
    expected: Vec<NodeId>,
    // `JoinSet` aborts all remaining tasks when the batch or quorum future is dropped.
    tasks: tokio::task::JoinSet<Result<(NodeId, CheckpointWatermark), (NodeId, PeerFailure)>>,
}

#[cfg(feature = "cluster")]
#[derive(Clone, Copy)]
struct PrepareFanoutBudget {
    total: Duration,
    per_attempt: Duration,
}

#[cfg(feature = "cluster")]
fn prepare_fanout_budget(quorum_window: Duration) -> Result<PrepareFanoutBudget, String> {
    if quorum_window.is_zero() {
        return Err("Prepare quorum window must be greater than zero".into());
    }
    let per_attempt = quorum_window / 2;
    if per_attempt.is_zero() {
        return Err("Prepare quorum window is too small to divide into retry attempts".into());
    }
    Ok(PrepareFanoutBudget {
        total: PREPARE_RPC_TIMEOUT.max(quorum_window),
        per_attempt,
    })
}

#[cfg(feature = "cluster")]
enum PrepareFanoutState {
    Pending(PrepareFanoutBatch),
    Claimed(BarrierAnnouncement),
}

#[cfg(feature = "cluster")]
impl PrepareFanoutState {
    const fn announcement(&self) -> &BarrierAnnouncement {
        match self {
            Self::Pending(batch) => &batch.announcement,
            Self::Claimed(announcement) => announcement,
        }
    }
}

#[cfg(feature = "cluster")]
fn clustered_prepare_roster(prepare: &BarrierAnnouncement) -> Result<Option<Vec<NodeId>>, String> {
    if prepare.phase != Phase::Prepare {
        return Err("Prepare fan-out received a different barrier phase".into());
    }
    if prepare.min_watermark_ms.is_some() {
        return Err("Prepare fan-out cannot carry a committed watermark".into());
    }
    let Some(fence) = prepare.assignment_fence.as_ref() else {
        // A build with the cluster feature may still run embedded or single-node. Those modes
        // retain the existing KV/direct-on-wait behavior and do not install a clustered batch.
        return Ok(None);
    };
    if !fence.is_canonical() {
        return Err("clustered Prepare has a non-canonical assignment certificate".into());
    }
    let proof = prepare
        .leader_proof
        .as_ref()
        .filter(|proof| proof.is_canonical())
        .ok_or_else(|| "clustered Prepare has no canonical leader proof".to_string())?;
    if fence.participant_incarnation(proof.owner.node_id) != Some(proof.owner.boot_id) {
        return Err(
            "clustered Prepare leader proof is outside its exact assignment process roster".into(),
        );
    }

    Ok(Some(
        fence
            .participants
            .iter()
            .filter(|participant| participant.node_id != proof.owner.node_id)
            .map(|participant| NodeId(participant.node_id))
            .collect(),
    ))
}

#[cfg(feature = "cluster")]
fn clustered_phase_roster(
    announcement: &BarrierAnnouncement,
    local_process: Option<BarrierProcessIdentity>,
) -> Result<Option<Vec<NodeId>>, String> {
    if announcement.phase == Phase::Prepare {
        return Err("non-Prepare fan-out received a Prepare announcement".into());
    }
    let Some(fence) = announcement.assignment_fence.as_ref() else {
        return Ok(None);
    };
    if !fence.is_canonical() {
        return Err("clustered barrier has a non-canonical assignment certificate".into());
    }

    let local_process = local_process
        .ok_or_else(|| "assignment-certified phase has no local process identity".to_string())?;
    if announcement.phase == Phase::Aligned {
        let proof = announcement
            .leader_proof
            .as_ref()
            .filter(|proof| proof.is_canonical())
            .ok_or_else(|| "clustered Aligned has no canonical leader proof".to_string())?;
        if fence.participant_incarnation(proof.owner.node_id) != Some(proof.owner.boot_id) {
            return Err(
                "clustered Aligned leader proof is outside its exact assignment process roster"
                    .into(),
            );
        }
        if local_process.node_id != proof.owner.node_id
            || local_process.boot_incarnation != proof.owner.boot_id
            || local_process.process_term != proof.owner.process_term
        {
            return Err("clustered Aligned sender does not own its live leader proof".into());
        }
    }

    Ok(Some(
        fence
            .participants
            .iter()
            .filter(|participant| {
                if is_terminal_phase(announcement.phase) {
                    participant.node_id != local_process.node_id
                } else {
                    !local_process.matches_participant(participant)
                }
            })
            .map(|participant| NodeId(participant.node_id))
            .collect(),
    ))
}

#[cfg(feature = "cluster")]
fn prepare_fanout_plan(
    announcement: &BarrierAnnouncement,
    quorum_window: Option<Duration>,
) -> Result<(Option<Vec<NodeId>>, Option<PrepareFanoutBudget>), String> {
    if announcement.phase != Phase::Prepare {
        return Ok((None, None));
    }
    let roster = clustered_prepare_roster(announcement)?;
    let budget = roster
        .as_ref()
        .map(|_| {
            quorum_window.ok_or_else(|| {
                "assignment-certified Prepare has no quorum retry window".to_string()
            })
        })
        .transpose()?
        .map(prepare_fanout_budget)
        .transpose()?;
    Ok((roster, budget))
}

#[cfg(feature = "cluster")]
fn canonical_expected_roster(expected: &[NodeId]) -> Result<Vec<NodeId>, String> {
    let mut canonical = expected.to_vec();
    canonical.sort_unstable_by_key(|peer| peer.0);
    if canonical.iter().any(NodeId::is_unassigned)
        || canonical.windows(2).any(|pair| pair[0] == pair[1])
    {
        return Err("Prepare quorum participant roster is not canonical".into());
    }
    Ok(canonical)
}

#[cfg(feature = "cluster")]
fn install_prepare_fanout(
    state: &GrpcState,
    kv: &Arc<dyn ClusterKv>,
    prepare: &BarrierAnnouncement,
    expected: Vec<NodeId>,
    budget: PrepareFanoutBudget,
) {
    let mut pending = state.prepare_fanout.lock();
    let rpc_deadline = tokio::time::Instant::now() + budget.total;
    let mut tasks = tokio::task::JoinSet::new();
    for &peer in &expected {
        let clients_pool = Arc::clone(&state.clients);
        let kv = Arc::clone(kv);
        let prepare = prepare.clone();
        tasks.spawn(async move {
            prepare_peer_until_deadline(
                peer,
                clients_pool,
                kv,
                prepare,
                rpc_deadline,
                budget.per_attempt,
            )
            .await
        });
    }
    let incoming = PrepareFanoutBatch {
        announcement: prepare.clone(),
        expected,
        tasks,
    };
    *pending = Some(PrepareFanoutState::Pending(incoming));
}

#[cfg(feature = "cluster")]
fn preflight_prepare_fanout(
    state: &GrpcState,
    prepare: &BarrierAnnouncement,
) -> Result<bool, String> {
    let pending = state.prepare_fanout.lock();
    let Some(current) = pending.as_ref() else {
        return Ok(true);
    };
    match announcement_attempt(prepare).relation_to(announcement_attempt(current.announcement())) {
        CheckpointAttemptRelation::Newer => Ok(true),
        CheckpointAttemptRelation::Exact if current.announcement() == prepare => Ok(false),
        CheckpointAttemptRelation::Older => {
            Err("stale Prepare cannot replace a newer in-flight fan-out".into())
        }
        CheckpointAttemptRelation::Conflict => {
            Err("conflicting Prepare attempt cannot replace the in-flight fan-out".into())
        }
        CheckpointAttemptRelation::Exact => {
            Err("conflicting Prepare certificate cannot replace the in-flight fan-out".into())
        }
    }
}

#[cfg(feature = "cluster")]
fn retire_prepare_fanout(state: &GrpcState, announcement: &BarrierAnnouncement) {
    let mut pending = state.prepare_fanout.lock();
    let Some(current) = pending.as_ref() else {
        return;
    };
    match announcement_attempt(announcement)
        .relation_to(announcement_attempt(current.announcement()))
    {
        CheckpointAttemptRelation::Newer => {
            pending.take();
        }
        CheckpointAttemptRelation::Exact if announcement.phase != Phase::Prepare => {
            pending.take();
        }
        CheckpointAttemptRelation::Older
        | CheckpointAttemptRelation::Conflict
        | CheckpointAttemptRelation::Exact => {}
    }
}

#[cfg(feature = "cluster")]
fn retryable_prepare_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        // Tonic maps a client-service readiness failure to `Unknown`; it is
        // transport state, not a response from the follower. The remaining
        // codes represent a connection that cannot currently complete the
        // request. Fence and validation failures use distinct semantic codes.
        tonic::Code::Unknown
            | tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::Aborted
    )
}

#[cfg(feature = "cluster")]
async fn wait_for_prepare_retry(deadline: tokio::time::Instant, backoff: &mut Duration) -> bool {
    let now = tokio::time::Instant::now();
    if now >= deadline {
        return false;
    }

    tokio::time::sleep_until((now + *backoff).min(deadline)).await;
    *backoff = backoff.saturating_mul(2).min(PREPARE_RETRY_MAX_BACKOFF);
    tokio::time::Instant::now() < deadline
}

#[cfg(feature = "cluster")]
fn prepare_rpc_request(
    prepare: &BarrierAnnouncement,
    assignment: &WireAssignmentFence,
    leader_proof: Option<&barrier_v1::LeaderProof>,
    timeout: Duration,
) -> tonic::Request<barrier_v1::PrepareRequest> {
    let mut request = tonic::Request::new(barrier_v1::PrepareRequest {
        epoch: prepare.epoch,
        checkpoint_id: prepare.checkpoint_id,
        flags: prepare.flags,
        assignment_version: assignment.version,
        assignment_participants: assignment.participants.clone(),
        assignment_vnode_count: assignment.vnode_count,
        assignment_map_digest: assignment.map_digest.clone(),
        leader_proof: leader_proof.cloned(),
    });
    request.set_timeout(timeout);
    request
}

#[cfg(feature = "cluster")]
fn validate_prepare_ack(
    peer: NodeId,
    prepare: &BarrierAnnouncement,
    assignment_digest: Option<&[u8; 32]>,
    ack: barrier_v1::Ack,
) -> Result<(NodeId, CheckpointWatermark), (NodeId, PeerFailure)> {
    if ack.epoch != prepare.epoch
        || ack.checkpoint_id != prepare.checkpoint_id
        || ack.assignment_digest.as_slice()
            != assignment_digest.map_or(&[][..], <[u8; 32]>::as_slice)
    {
        return Err((
            peer,
            PeerFailure::Nack("Prepare acknowledgement identity mismatch".into()),
        ));
    }
    if !ack.ok {
        return Err((
            peer,
            PeerFailure::Nack(
                ack.error
                    .unwrap_or_else(|| "Unknown prepare failure".to_string()),
            ),
        ));
    }
    checkpoint_watermark_from_wire(ack.watermark_status, ack.local_watermark_ms)
        .map(|watermark| (peer, watermark))
        .map_err(|error| (peer, PeerFailure::Nack(error)))
}

#[cfg(feature = "cluster")]
fn prepare_expected_process(
    peer: NodeId,
    prepare: &BarrierAnnouncement,
) -> Result<Option<ExpectedBarrierProcess>, (NodeId, PeerFailure)> {
    let Some(fence) = prepare.assignment_fence.as_ref() else {
        return Ok(None);
    };
    let boot = fence.participant_incarnation(peer.0).ok_or_else(|| {
        (
            peer,
            PeerFailure::Nack("Prepare peer is outside the assignment process roster".into()),
        )
    })?;
    Ok(Some(ExpectedBarrierProcess::participant(peer.0, boot)))
}

#[cfg(feature = "cluster")]
async fn prepare_peer_until_deadline(
    peer: NodeId,
    clients_pool: BarrierClientPool,
    kv: Arc<dyn ClusterKv>,
    prepare: BarrierAnnouncement,
    deadline: tokio::time::Instant,
    max_attempt_duration: Duration,
) -> Result<(NodeId, CheckpointWatermark), (NodeId, PeerFailure)> {
    let assignment = assignment_fence_to_wire(prepare.assignment_fence.as_ref());
    let assignment_digest = prepare
        .assignment_fence
        .as_ref()
        .map(super::CheckpointAssignmentFence::digest);
    let leader_proof = leader_proof_to_wire(prepare.leader_proof.as_ref());
    let expected_process = prepare_expected_process(peer, &prepare)?;
    let mut backoff = PREPARE_RETRY_INITIAL_BACKOFF;

    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err((peer, PeerFailure::Unreachable));
        }

        let Ok(client) = tokio::time::timeout_at(
            deadline,
            get_barrier_client(peer, expected_process, &clients_pool, &kv),
        )
        .await
        else {
            return Err((peer, PeerFailure::Unreachable));
        };
        let client = match client {
            Ok(client) => client,
            Err(BarrierClientResolutionError::ProcessMismatch) => {
                if wait_for_prepare_retry(deadline, &mut backoff).await {
                    continue;
                }
                return Err((peer, PeerFailure::Unreachable));
            }
            Err(BarrierClientResolutionError::Invalid(error)) => {
                return Err((peer, PeerFailure::Nack(error)));
            }
        };
        let Some(mut client) = client else {
            if wait_for_prepare_retry(deadline, &mut backoff).await {
                continue;
            }
            return Err((peer, PeerFailure::Unreachable));
        };

        let now = tokio::time::Instant::now();
        let remaining = deadline.saturating_duration_since(now);
        if remaining.is_zero() {
            evict_barrier_client(&clients_pool, peer, expected_process);
            return Err((peer, PeerFailure::Unreachable));
        }
        // Leave part of the existing quorum budget available to evict and re-resolve a lazy
        // channel whose readiness check stalls. Prepare identities are idempotent, so a live
        // follower may safely complete an earlier attempt and serve its cached acknowledgement.
        let attempt_budget = (remaining / 2).min(max_attempt_duration);
        if attempt_budget.is_zero() {
            evict_barrier_client(&clients_pool, peer, expected_process);
            return Err((peer, PeerFailure::Unreachable));
        }
        let attempt_deadline = now + attempt_budget;

        let request =
            prepare_rpc_request(&prepare, &assignment, leader_proof.as_ref(), attempt_budget);

        match tokio::time::timeout_at(attempt_deadline, client.prepare(request)).await {
            Ok(Ok(response)) => {
                return validate_prepare_ack(
                    peer,
                    &prepare,
                    assignment_digest.as_ref(),
                    response.into_inner(),
                );
            }
            Ok(Err(status)) => {
                evict_barrier_client(&clients_pool, peer, expected_process);
                if retryable_prepare_status(&status) {
                    if wait_for_prepare_retry(deadline, &mut backoff).await {
                        continue;
                    }
                    return Err((peer, PeerFailure::Unreachable));
                }
                return Err((peer, PeerFailure::Nack(status.to_string())));
            }
            Err(_) => {
                evict_barrier_client(&clients_pool, peer, expected_process);
                if wait_for_prepare_retry(deadline, &mut backoff).await {
                    continue;
                }
                return Err((peer, PeerFailure::Unreachable));
            }
        }
    }
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
    #[cfg(feature = "cluster")]
    local_leader_proof: Arc<parking_lot::Mutex<Option<LocalLeaderProofProvider>>>,
    #[cfg(feature = "cluster")]
    local_process: Arc<std::sync::OnceLock<BarrierProcessIdentity>>,
    #[cfg(feature = "cluster")]
    unbound_endpoint_started: parking_lot::Mutex<bool>,
    #[cfg(feature = "cluster")]
    process_lease_deadline: Arc<std::sync::OnceLock<Arc<super::LeaseDeadline>>>,
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
                abort_grpc_tasks(&state);
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
            #[cfg(feature = "cluster")]
            local_leader_proof: Arc::new(parking_lot::Mutex::new(None)),
            #[cfg(feature = "cluster")]
            local_process: Arc::new(std::sync::OnceLock::new()),
            #[cfg(feature = "cluster")]
            unbound_endpoint_started: parking_lot::Mutex::new(false),
            #[cfg(feature = "cluster")]
            process_lease_deadline: Arc::new(std::sync::OnceLock::new()),
        }
    }

    #[cfg(feature = "cluster")]
    fn require_live_bound_process_lease(&self) -> Result<(), String> {
        if self.local_process.get().is_none() {
            return Ok(());
        }
        let deadline = self
            .process_lease_deadline
            .get()
            .ok_or_else(|| "process lease deadline is not installed".to_string())?;
        if !deadline.is_live() {
            return Err("process lease deadline has expired".into());
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn claim_endpoint_process(&self) -> Option<BarrierProcessIdentity> {
        let mut unbound_endpoint_started = self.unbound_endpoint_started.lock();
        let process = self.local_process.get().copied();
        if process.is_none() {
            *unbound_endpoint_started = true;
        }
        process
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn install_process_lease_deadline(
        &self,
        deadline: Arc<super::LeaseDeadline>,
    ) -> Result<(), String> {
        match self.process_lease_deadline.set(deadline) {
            Ok(()) => Ok(()),
            Err(deadline)
                if self
                    .process_lease_deadline
                    .get()
                    .is_some_and(|current| Arc::ptr_eq(current, &deadline)) =>
            {
                Ok(())
            }
            Err(_) => Err("process lease deadline is already installed".into()),
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn install_local_process_lease(
        &self,
        lease: &super::ProcessLease,
    ) -> Result<(), String> {
        let process = BarrierProcessIdentity::from_process_lease(lease)?;
        let unbound_endpoint_started = self.unbound_endpoint_started.lock();
        if *unbound_endpoint_started {
            return Err(
                "an assignment-less cluster control endpoint cannot be promoted in place".into(),
            );
        }
        match self.local_process.set(process) {
            Ok(()) => Ok(()),
            Err(_) if self.local_process.get() == Some(&process) => Ok(()),
            Err(_) => Err("cluster control endpoint process identity is already installed".into()),
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn set_local_leader_proof_provider(&self, provider: LocalLeaderProofProvider) {
        *self.local_leader_proof.lock() = Some(provider);
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
    ///
    /// # Errors
    /// Returns `NotConfigured` when durable cluster checkpoint authority is not installed.
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
        self.validate_announcement_leader(ann, proof).await
    }

    #[cfg(feature = "cluster")]
    async fn validate_announcement_leader(
        &self,
        ann: &BarrierAnnouncement,
        proof: &crate::checkpoint::LeaderProof,
    ) -> Result<(), String> {
        let local_proof = self
            .local_leader_proof
            .lock()
            .clone()
            .and_then(|provider| provider());
        if local_proof.as_ref() == Some(proof) {
            return Ok(());
        }
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

    #[cfg(feature = "cluster")]
    pub(super) async fn validate_checkpoint_prepare(
        &self,
        announcement: &BarrierAnnouncement,
    ) -> Result<(), String> {
        if announcement.phase != Phase::Prepare {
            return Err("checkpoint Prepare validation received a different barrier phase".into());
        }
        let proof = announcement.leader_proof.as_ref().ok_or_else(|| {
            format!(
                "clustered Prepare for checkpoint {}/{} is missing a durable leader proof",
                announcement.epoch, announcement.checkpoint_id
            )
        })?;
        self.validate_announcement_leader(announcement, proof).await
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
    ) -> Result<std::net::SocketAddr, String> {
        use barrier_v1::barrier_sync_server::BarrierSyncServer;
        use std::net::TcpListener;
        use tonic::transport::Server;

        self.require_live_bound_process_lease()?;
        let advertised_process = self.claim_endpoint_process();
        let listener = TcpListener::bind(bind_addr).map_err(|e| e.to_string())?;
        let local_addr = listener.local_addr().map_err(|e| e.to_string())?;
        listener.set_nonblocking(true).map_err(|e| e.to_string())?;
        let tokio_listener =
            tokio::net::TcpListener::from_std(listener).map_err(|e| e.to_string())?;
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
        let advertisement = encode_barrier_endpoint(&advertise_addr, advertised_process)?;

        let (incoming_tx, incoming_rx) = crossfire::mpsc::bounded_async::<BarrierAnnouncement>(128);
        let prepare_acks = Arc::new(parking_lot::Mutex::new(PrepareAckState::default()));
        let clients = Arc::new(parking_lot::Mutex::new(FxHashMap::default()));
        let local_process = Arc::clone(&self.local_process);

        let server_impl = GrpcBarrierServer {
            incoming_tx: incoming_tx.clone(),
            prepare_acks: Arc::clone(&prepare_acks),
            leader_lease_store: Arc::clone(&self.leader_lease_store),
            local_leader_proof: Arc::clone(&self.local_leader_proof),
            local_process: Arc::clone(&local_process),
            process_lease_deadline: Arc::clone(&self.process_lease_deadline),
        };

        // Apply TLS synchronously so a bad cert fails start_server (before
        // publishing BARRIER_ADDR_KEY) rather than silently never serving.
        let mut builder = Server::builder();
        if let Some(tls) = super::tls::server_tls() {
            builder = builder
                .tls_config(tls.clone())
                .map_err(|e| format!("cluster control-plane TLS config: {e}"))?;
        }
        let router = builder.add_service(BarrierSyncServer::new(server_impl));
        let server_task = tokio::spawn(async move {
            let incoming_stream = tokio_stream::wrappers::TcpListenerStream::new(tokio_listener);
            let _ = router.serve_with_incoming(incoming_stream).await;
        });

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
            prepare_fanout: parking_lot::Mutex::new(None),
            announcement_lock: tokio::sync::Mutex::new(()),
            clients,
            server_handle: Arc::new(parking_lot::Mutex::new(Some(server_task))),
            relay_handle: Arc::new(parking_lot::Mutex::new(Some(relay_task))),
            advertise_addr: advertise_addr.clone(),
            local_process: Arc::clone(&local_process),
        });

        if let Err(error) = self.require_live_bound_process_lease() {
            abort_grpc_tasks(&grpc_state);
            return Err(error);
        }
        if let Err(error) = self.kv.write_checked(BARRIER_ADDR_KEY, advertisement).await {
            abort_grpc_tasks(&grpc_state);
            return Err(format!(
                "publish cluster control endpoint advertisement: {error}"
            ));
        }

        *self.grpc.lock() = Some(grpc_state);

        Ok(local_addr)
    }

    /// Ask one exact remote process to confirm a proof already read from durable authority.
    ///
    /// The response echoes only a fresh challenge id. It never returns a process-local or durable
    /// fencing token.
    ///
    /// # Errors
    /// Fails when the proof, peer address, RPC, acknowledgement, or deadline is invalid.
    #[cfg(feature = "cluster")]
    pub async fn confirm_remote_leader_proof(
        &self,
        proof: &super::LeaderProof,
        deadline: tokio::time::Instant,
    ) -> Result<bool, String> {
        if !proof.is_canonical() {
            return Err("remote leader proof challenge is not canonical".into());
        }
        let peer = NodeId(proof.owner.node_id);
        let state = self
            .grpc
            .lock()
            .clone()
            .ok_or_else(|| "cluster control RPC server is not started".to_string())?;
        let clients = Arc::clone(&state.clients);
        let request_timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        let challenge = uuid::Uuid::new_v4();
        let expected_process = Some(ExpectedBarrierProcess::exact(&proof.owner));
        let result = tokio::time::timeout_at(deadline, async {
            let mut client =
                match get_barrier_client(peer, expected_process, &clients, &self.kv).await {
                    Ok(Some(client)) => client,
                    Ok(None) => {
                        return Err(format!(
                            "cluster control address for peer {} is unavailable",
                            peer.0
                        ));
                    }
                    Err(BarrierClientResolutionError::ProcessMismatch) => return Ok(false),
                    Err(BarrierClientResolutionError::Invalid(error)) => return Err(error),
                };
            let mut request = tonic::Request::new(barrier_v1::LeaderProofChallenge {
                expected_proof: leader_proof_to_wire(Some(proof)),
                challenge_id: challenge.as_bytes().to_vec(),
            });
            request.set_timeout(request_timeout);
            match client.confirm_leader_proof(request).await {
                Ok(response) => {
                    let acknowledged = response.into_inner().challenge_id;
                    if !leader_proof_ack_matches(challenge, &acknowledged) {
                        return Err("remote leader proof acknowledgement challenge mismatch".into());
                    }
                    Ok(true)
                }
                Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                    // The stable node id may now advertise a replacement process. Do not pin
                    // subsequent proof attempts to a still-responsive channel for the old boot.
                    evict_barrier_client(&clients, peer, expected_process);
                    Ok(false)
                }
                Err(status) => Err(status.to_string()),
            }
        })
        .await;
        match result {
            Ok(Ok(confirmed)) => Ok(confirmed),
            Ok(Err(error)) => {
                evict_barrier_client(&clients, peer, expected_process);
                Err(error)
            }
            Err(_) => {
                evict_barrier_client(&clients, peer, expected_process);
                Err(format!(
                    "remote leader proof request for peer {} timed out",
                    peer.0
                ))
            }
        }
    }

    /// Leader-side announcement for terminal, aligned, and assignment-less local/KV phases.
    ///
    /// # Errors
    /// Assignment-certified Prepare must use [`Self::announce_prepare`] so its retry cadence is
    /// derived from the configured quorum window. Other errors propagate validation, encoding,
    /// and publication failures.
    pub async fn announce(&self, ann: &BarrierAnnouncement) -> Result<(), String> {
        #[cfg(feature = "cluster")]
        if ann.phase == Phase::Prepare && ann.assignment_fence.is_some() {
            return Err(
                "assignment-certified Prepare requires an explicit quorum retry window".into(),
            );
        }
        self.announce_inner(ann, None).await
    }

    /// Durably publish one assignment-certified Prepare and immediately start its direct fan-out.
    ///
    /// # Errors
    /// Rejects a different phase, an assignment-less announcement, a zero/indivisible quorum
    /// window, malformed authority, conflicting in-flight Prepare state, or publication failure.
    #[cfg(feature = "cluster")]
    pub async fn announce_prepare(
        &self,
        ann: &BarrierAnnouncement,
        quorum_window: Duration,
    ) -> Result<(), String> {
        if ann.phase != Phase::Prepare || ann.assignment_fence.is_none() {
            return Err("explicit Prepare fan-out requires an assignment certificate".into());
        }
        self.announce_inner(ann, Some(quorum_window)).await
    }

    #[cfg(feature = "cluster")]
    async fn discover_assignment_less_phase_peers(&self, local_address: &str) -> Vec<NodeId> {
        let live: Option<FxHashSet<NodeId>> =
            self.leader_election
                .lock()
                .clone()
                .map(|(_, members_rx, _)| {
                    members_rx
                        .borrow()
                        .iter()
                        .filter(|member| {
                            matches!(member.state, NodeState::Active | NodeState::Draining)
                        })
                        .map(|member| member.id)
                        .collect()
                });
        let mut discovered = Vec::new();
        for (node_id, raw_endpoint) in self.kv.scan(BARRIER_ADDR_KEY).await {
            let address = match decode_barrier_endpoint(&raw_endpoint) {
                Ok((address, None)) => address,
                Ok((_, Some(_))) => continue,
                Err(error) => {
                    tracing::warn!(peer = node_id.0, %error, "ignoring invalid cluster control endpoint");
                    continue;
                }
            };
            if address == local_address {
                continue;
            }
            if live.as_ref().is_some_and(|live| !live.contains(&node_id)) {
                continue;
            }
            discovered.push(node_id);
        }
        discovered
    }

    async fn announce_inner(
        &self,
        ann: &BarrierAnnouncement,
        prepare_quorum_window: Option<Duration>,
    ) -> Result<(), String> {
        #[cfg(feature = "cluster")]
        {
            self.validate_reversible_announcement(ann).await?;
            let (prepare_roster, prepare_budget) = prepare_fanout_plan(ann, prepare_quorum_window)?;
            let grpc_opt = self.grpc.lock().clone();
            if let Some(state) = grpc_opt {
                let local_process = state.local_process.get().copied();
                let phase_roster = if ann.phase == Phase::Prepare {
                    None
                } else {
                    clustered_phase_roster(ann, local_process)?
                };
                let announcement_guard = state.announcement_lock.lock().await;
                let replace_prepare_fanout = if prepare_roster.is_some() {
                    // Reject stale/equivocating retries before they can overwrite the durable
                    // gossip slot. The announcement lock keeps this check and publication atomic
                    // with respect to every local phase transition.
                    preflight_prepare_fanout(&state, ann)?
                } else {
                    false
                };
                // Record the decision in KV before delivery, so a reclaiming
                // leader's `max_announced()` and a recovering peer's KV fallback
                // still see this epoch even if a peer RPC below fails and returns
                // early (the RPC receiver does not persist the announcement).
                let json = serde_json::to_string(ann).map_err(|e| e.to_string())?;
                self.kv
                    .write_checked(ANNOUNCEMENT_KEY, json)
                    .await
                    .map_err(|error| format!("publish barrier announcement: {error}"))?;
                if ann.phase == Phase::Prepare {
                    if let Some(expected) = prepare_roster.filter(|_| replace_prepare_fanout) {
                        // Start the exact assignment-complete batch only after Prepare is durable.
                        // Local source fencing, shuffle alignment, and state capture can now run
                        // concurrently with follower capture instead of delaying RPC delivery.
                        install_prepare_fanout(
                            &state,
                            &self.kv,
                            ann,
                            expected,
                            prepare_budget.expect("certified Prepare budget was validated"),
                        );
                    }
                    drop(announcement_guard);
                } else {
                    // Retire a still-pending batch. A claimed batch's tasks are owned by the
                    // quorum future; checkpoint orchestration completes or drops that future
                    // before it publishes this terminal/successor phase.
                    retire_prepare_fanout(&state, ann);
                    drop(announcement_guard);
                    let expected = if let Some(roster) = phase_roster {
                        // The checkpoint certificate, not mutable membership, is the phase roster.
                        // This also removes a discovery/object-store scan from the clustered hot
                        // path and excludes Active processes outside the frozen cut.
                        roster
                    } else {
                        // Feature-enabled embedded and single-node use remains assignment-less.
                        // Only that compatibility path discovers direct peers from membership.
                        self.discover_assignment_less_phase_peers(&state.advertise_addr)
                            .await
                    };

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

        #[cfg(not(feature = "cluster"))]
        let _ = prepare_quorum_window;
        let json = serde_json::to_string(ann).map_err(|e| e.to_string())?;
        self.kv
            .write_checked(ANNOUNCEMENT_KEY, json)
            .await
            .map_err(|error| format!("publish barrier announcement: {error}"))?;
        Ok(())
    }

    /// Watch over gRPC-delivered announcements, for push-driven waits
    /// (the decision wait and the Aligned resume gate). `None` until
    /// the gRPC server is started — gossip-KV-only deployments fall
    /// back to polling the merged gossip history.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn announcement_watch(&self) -> Option<watch::Receiver<Option<BarrierAnnouncement>>> {
        self.grpc.lock().as_ref().map(|s| s.latest_rx.clone())
    }

    /// Merge the latest direct and gossip announcements without consulting remote authority.
    /// Callers may inspect the result, but must validate a matching reversible phase before use.
    /// Observation is non-destructive, and direct plus gossip histories must remain related in
    /// both attempt dimensions. Terminal durable KV values remain the decision authority.
    ///
    /// # Errors
    /// Returns a string on transport, decode, or conflicting-history failure.
    pub(super) async fn observe_hint(
        &self,
        leader: NodeId,
    ) -> Result<Option<BarrierAnnouncement>, String> {
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
        Ok(observed)
    }

    #[cfg(feature = "cluster")]
    /// Validate one merged announcement immediately before a caller uses it.
    pub(super) async fn validate_observed(
        &self,
        announcement: &BarrierAnnouncement,
    ) -> Result<(), String> {
        self.validate_reversible_announcement(announcement).await
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
                let expected_roster = match canonical_expected_roster(expected) {
                    Ok(roster) => roster,
                    Err(error) => {
                        return QuorumOutcome::Failed {
                            failures: vec![(
                                expected.first().copied().unwrap_or(NodeId::UNASSIGNED),
                                error,
                            )],
                        };
                    }
                };
                let eager_batch = if prepare.assignment_fence.is_some() {
                    let mut pending = state.prepare_fanout.lock();
                    match pending.take() {
                        Some(PrepareFanoutState::Pending(batch))
                            if batch.announcement == *prepare
                                && batch.expected == expected_roster =>
                        {
                            *pending = Some(PrepareFanoutState::Claimed(prepare.clone()));
                            Some(batch)
                        }
                        Some(state @ PrepareFanoutState::Pending(_))
                            if state.announcement() != prepare =>
                        {
                            let claimed = state.announcement().clone();
                            *pending = Some(PrepareFanoutState::Claimed(claimed));
                            return QuorumOutcome::Failed {
                                failures: vec![(
                                    expected_roster
                                        .first()
                                        .copied()
                                        .unwrap_or(NodeId::UNASSIGNED),
                                    "Prepare quorum does not match the exact announced fan-out"
                                        .into(),
                                )],
                            };
                        }
                        Some(state @ PrepareFanoutState::Pending(_)) => {
                            let claimed = state.announcement().clone();
                            *pending = Some(PrepareFanoutState::Claimed(claimed));
                            return QuorumOutcome::Failed {
                                failures: vec![(
                                    expected_roster
                                        .first()
                                        .copied()
                                        .unwrap_or(NodeId::UNASSIGNED),
                                    "Prepare quorum roster does not match the announced assignment"
                                        .into(),
                                )],
                            };
                        }
                        Some(state @ PrepareFanoutState::Claimed(_)) => {
                            let exact = state.announcement() == prepare;
                            *pending = Some(state);
                            return QuorumOutcome::Failed {
                                failures: vec![(
                                    expected_roster
                                        .first()
                                        .copied()
                                        .unwrap_or(NodeId::UNASSIGNED),
                                    if exact {
                                        "clustered Prepare fan-out was already claimed"
                                    } else {
                                        "Prepare quorum does not match the claimed fan-out"
                                    }
                                    .into(),
                                )],
                            };
                        }
                        None => {
                            return QuorumOutcome::Failed {
                                failures: vec![(
                                    expected_roster
                                        .first()
                                        .copied()
                                        .unwrap_or(NodeId::UNASSIGNED),
                                    "clustered Prepare has no in-flight announced fan-out".into(),
                                )],
                            };
                        }
                    }
                } else {
                    None
                };

                let prepare_deadline = tokio::time::Instant::now() + deadline;
                let results = if let Some(mut batch) = eager_batch {
                    debug_assert_eq!(batch.tasks.len(), expected_roster.len());
                    let mut results = Vec::with_capacity(expected_roster.len());
                    loop {
                        match tokio::time::timeout_at(prepare_deadline, batch.tasks.join_next())
                            .await
                        {
                            Ok(Some(Ok(result))) => results.push(result),
                            Ok(Some(Err(error))) => results.push(Err((
                                NodeId::UNASSIGNED,
                                PeerFailure::Nack(format!("Prepare RPC task failed: {error}")),
                            ))),
                            Ok(None) | Err(_) => break,
                        }
                    }
                    results
                } else {
                    // Embedded/single-node use with the cluster feature and legacy direct tests
                    // have no assignment certificate. Keep their on-demand direct path intact.
                    let futures = expected.iter().map(|&peer| {
                        prepare_peer_until_deadline(
                            peer,
                            Arc::clone(&state.clients),
                            Arc::clone(&self.kv),
                            prepare.clone(),
                            prepare_deadline,
                            deadline / 2,
                        )
                    });
                    futures::future::join_all(futures).await
                };

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
                successful.sort_unstable_by_key(|peer| peer.0);
                failures.sort_unstable_by_key(|(peer, _)| peer.0);

                let completed: FxHashSet<NodeId> = successful
                    .iter()
                    .copied()
                    .chain(timed_out.iter().copied())
                    .chain(failures.iter().map(|(peer, _)| *peer))
                    .collect();
                for &peer in &expected_roster {
                    if !completed.contains(&peer) {
                        timed_out.push(peer);
                    }
                }
                timed_out.sort_unstable_by_key(|peer| peer.0);

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
                    missing.sort_unstable_by_key(|peer| peer.0);
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
    fn leader_proof_challenge_and_ack_require_one_exact_fresh_id() {
        let challenge = uuid::Uuid::from_u128(17);
        assert_eq!(
            leader_proof_challenge_from_wire(challenge.as_bytes()).unwrap(),
            challenge
        );
        assert!(leader_proof_ack_matches(challenge, challenge.as_bytes()));
        assert!(!leader_proof_ack_matches(
            challenge,
            uuid::Uuid::from_u128(18).as_bytes()
        ));
        assert!(!leader_proof_ack_matches(challenge, &[1; 15]));
        assert!(leader_proof_challenge_from_wire(&[0; 16]).is_err());
        assert!(leader_proof_challenge_from_wire(&[1; 15]).is_err());
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn process_bound_endpoint_advertisement_is_strict_and_bounded() {
        let process = BarrierProcessIdentity {
            node_id: 7,
            boot_incarnation: uuid::Uuid::from_u128(70),
            process_term: 9,
        };
        let encoded = BarrierEndpointRecord::new("127.0.0.1:9000".into(), process)
            .unwrap()
            .encode()
            .unwrap();
        assert_eq!(
            decode_barrier_endpoint(&encoded).unwrap(),
            ("127.0.0.1:9000".into(), Some(process))
        );
        assert_eq!(
            decode_barrier_endpoint("127.0.0.1:9001").unwrap(),
            ("127.0.0.1:9001".into(), None)
        );

        let mut wrong_version: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        wrong_version["version"] = serde_json::json!(2);
        assert!(decode_barrier_endpoint(&wrong_version.to_string()).is_err());

        let mut unknown_field: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        unknown_field["unexpected"] = serde_json::json!(true);
        assert!(decode_barrier_endpoint(&unknown_field.to_string()).is_err());

        let mut nil_boot: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        nil_boot["process"]["boot_incarnation"] = serde_json::json!(uuid::Uuid::nil());
        assert!(decode_barrier_endpoint(&nil_boot.to_string()).is_err());
        assert!(decode_barrier_endpoint(&"x".repeat(MAX_BARRIER_ENDPOINT_BYTES + 1)).is_err());
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
    fn prepare_retry_classification_keeps_fence_failures_semantic() {
        for code in [
            tonic::Code::Unknown,
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::Cancelled,
            tonic::Code::Aborted,
        ] {
            assert!(retryable_prepare_status(&tonic::Status::new(code, "")));
        }

        for code in [
            tonic::Code::PermissionDenied,
            tonic::Code::FailedPrecondition,
            tonic::Code::InvalidArgument,
            tonic::Code::ResourceExhausted,
            tonic::Code::Internal,
        ] {
            assert!(!retryable_prepare_status(&tonic::Status::new(code, "")));
        }
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn eager_prepare_retry_budget_tracks_the_configured_quorum_window() {
        let default = prepare_fanout_budget(Duration::from_secs(3)).unwrap();
        assert_eq!(default.per_attempt, Duration::from_millis(1_500));
        assert_eq!(default.total, PREPARE_RPC_TIMEOUT);

        let extended = prepare_fanout_budget(Duration::from_secs(40)).unwrap();
        assert_eq!(extended.per_attempt, Duration::from_secs(20));
        assert_eq!(extended.total, Duration::from_secs(40));
        assert!(prepare_fanout_budget(Duration::ZERO).is_err());
        assert!(prepare_fanout_budget(Duration::from_nanos(1)).is_err());
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
    fn successor_terminal_targets_historical_proof_owner_and_excludes_actual_sender() {
        let announcement = BarrierAnnouncement {
            epoch: 9,
            checkpoint_id: 90,
            assignment_fence: Some(test_fence(17, &[1, 2, 3], &[(1, 11), (2, 22), (3, 33)])),
            leader_proof: Some(crate::cluster::control::LeaderProof {
                owner: crate::checkpoint::LeaderProofOwner {
                    node_id: 1,
                    boot_id: uuid::Uuid::from_u128(11),
                    process_term: 7,
                },
                fencing_token: 9,
            }),
            phase: Phase::Commit,
            flags: 0,
            min_watermark_ms: None,
        };

        assert_eq!(
            clustered_phase_roster(
                &announcement,
                Some(BarrierProcessIdentity {
                    node_id: 3,
                    boot_incarnation: uuid::Uuid::from_u128(33),
                    process_term: 8,
                }),
            )
            .unwrap(),
            Some(vec![NodeId(1), NodeId(2)])
        );
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn restarted_same_node_terminal_skips_the_unaddressable_predecessor() {
        let announcement = BarrierAnnouncement {
            epoch: 9,
            checkpoint_id: 90,
            assignment_fence: Some(test_fence(17, &[1, 2], &[(1, 11), (2, 22)])),
            leader_proof: Some(crate::cluster::control::LeaderProof {
                owner: crate::checkpoint::LeaderProofOwner {
                    node_id: 1,
                    boot_id: uuid::Uuid::from_u128(11),
                    process_term: 7,
                },
                fencing_token: 9,
            }),
            phase: Phase::Commit,
            flags: 0,
            min_watermark_ms: None,
        };

        assert_eq!(
            clustered_phase_roster(
                &announcement,
                Some(BarrierProcessIdentity {
                    node_id: 1,
                    boot_incarnation: uuid::Uuid::from_u128(111),
                    process_term: 8,
                }),
            )
            .unwrap(),
            Some(vec![NodeId(2)])
        );
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn restarted_same_node_cannot_send_aligned_with_the_predecessor_proof() {
        let announcement = BarrierAnnouncement {
            epoch: 9,
            checkpoint_id: 90,
            assignment_fence: Some(test_fence(17, &[1, 2], &[(1, 11), (2, 22)])),
            leader_proof: Some(crate::cluster::control::LeaderProof {
                owner: crate::checkpoint::LeaderProofOwner {
                    node_id: 1,
                    boot_id: uuid::Uuid::from_u128(11),
                    process_term: 7,
                },
                fencing_token: 9,
            }),
            phase: Phase::Aligned,
            flags: 0,
            min_watermark_ms: None,
        };

        let error = clustered_phase_roster(
            &announcement,
            Some(BarrierProcessIdentity {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(111),
                process_term: 8,
            }),
        )
        .unwrap_err();
        assert!(
            error.contains("does not own its live leader proof"),
            "{error}"
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
        let original_lease = match store.begin_new_term(&original, 1).await.unwrap() {
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
            local_leader_proof: Arc::new(parking_lot::Mutex::new(None)),
            local_process: Arc::new(std::sync::OnceLock::new()),
            process_lease_deadline: Arc::new(std::sync::OnceLock::new()),
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
    #[tokio::test]
    async fn reversible_announcement_uses_only_an_exact_live_local_proof() {
        use object_store::memory::InMemory;

        let coordinator = BarrierCoordinator::new(kv(NodeId(1)));
        coordinator.set_leader_lease_store(Arc::new(super::super::LeaderLeaseStore::new(
            Arc::new(InMemory::new()),
            1_000,
        )));
        let proof = super::super::LeaderProof {
            owner: crate::checkpoint::LeaderProofOwner {
                node_id: 1,
                boot_id: uuid::Uuid::from_u128(11),
                process_term: 3,
            },
            fencing_token: 7,
        };
        let local = proof.clone();
        coordinator.set_local_leader_proof_provider(Arc::new(move || Some(local.clone())));
        let mut announcement = BarrierAnnouncement {
            epoch: 20,
            checkpoint_id: 200,
            assignment_fence: Some(test_fence(9, &[1], &[(1, 11)])),
            leader_proof: Some(proof),
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        };

        coordinator
            .validate_reversible_announcement(&announcement)
            .await
            .expect("the exact locally live proof must avoid a remote authority read");

        announcement.leader_proof.as_mut().unwrap().fencing_token += 1;
        let error = coordinator
            .validate_reversible_announcement(&announcement)
            .await
            .expect_err("a different token must fall through to durable validation");
        assert!(error.contains("no durable leader lease exists"), "{error}");
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
            let lease = match store.begin_new_term(&owner, 1).await.unwrap() {
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

        #[derive(Debug)]
        struct RejectAnnouncementKv {
            inner: Arc<InMemoryKv>,
        }

        #[async_trait]
        impl ClusterKv for RejectAnnouncementKv {
            async fn write(&self, key: &str, value: String) {
                let _ = self.write_checked(key, value).await;
            }

            async fn write_checked(&self, key: &str, value: String) -> Result<(), String> {
                if key == ANNOUNCEMENT_KEY {
                    return Err("injected durable write failure".into());
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

        fn test_process_lease(
            node_id: u64,
            boot: u128,
            term: u64,
        ) -> crate::cluster::control::ProcessLease {
            crate::cluster::control::ProcessLease {
                node: NodeId(node_id),
                owner: uuid::Uuid::from_u128(boot),
                term,
                seq: term,
                expires_at_ms: i64::MAX,
            }
        }

        fn bind_process(coordinator: &BarrierCoordinator, node_id: u64, boot: u128, term: u64) {
            coordinator
                .install_process_lease_deadline(Arc::new(
                    crate::cluster::control::LeaseDeadline::live_for(Duration::from_secs(60)),
                ))
                .unwrap();
            coordinator
                .install_local_process_lease(&test_process_lease(node_id, boot, term))
                .unwrap();
        }

        #[tokio::test]
        async fn process_bound_server_requires_a_live_shared_deadline() {
            let coordinator = BarrierCoordinator::new(kv(NodeId(2)));
            coordinator
                .install_local_process_lease(&test_process_lease(2, 22, 1))
                .unwrap();
            let error = coordinator
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap_err();
            assert!(error.contains("not installed"), "{error}");

            let deadline = Arc::new(crate::cluster::control::LeaseDeadline::fenced());
            coordinator
                .install_process_lease_deadline(Arc::clone(&deadline))
                .unwrap();
            coordinator
                .install_process_lease_deadline(Arc::clone(&deadline))
                .unwrap();
            assert!(coordinator
                .install_process_lease_deadline(Arc::new(
                    crate::cluster::control::LeaseDeadline::live_for(Duration::from_secs(60)),
                ))
                .is_err());
            let error = coordinator
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap_err();
            assert!(error.contains("expired"), "{error}");
        }

        #[tokio::test]
        async fn assignment_less_server_cannot_be_promoted_after_first_publication() {
            let control = kv(NodeId(2));
            let coordinator = BarrierCoordinator::new(control.clone());
            coordinator
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            let original = control
                .read_from(NodeId(2), BARRIER_ADDR_KEY)
                .await
                .unwrap();
            assert_eq!(decode_barrier_endpoint(&original).unwrap().1, None);

            let error = coordinator
                .install_local_process_lease(&test_process_lease(2, 22, 1))
                .unwrap_err();

            assert!(error.contains("cannot be promoted"), "{error}");
            assert_eq!(
                control.read_from(NodeId(2), BARRIER_ADDR_KEY).await,
                Some(original)
            );
        }

        #[tokio::test]
        async fn invalid_advertisement_fails_before_server_state_or_publication() {
            let control = kv(NodeId(2));
            let coordinator = BarrierCoordinator::new(control.clone());

            let error = coordinator
                .start_server(
                    "127.0.0.1:0".parse().unwrap(),
                    Some("x".repeat(MAX_BARRIER_ENDPOINT_BYTES)),
                )
                .await
                .unwrap_err();

            assert!(error.contains("oversized"), "{error}");
            assert!(coordinator.grpc.lock().is_none());
            assert!(control
                .read_from(NodeId(2), BARRIER_ADDR_KEY)
                .await
                .is_none());
        }

        fn endpoint_advertisement(
            address: SocketAddr,
            node_id: u64,
            boot: u128,
            term: u64,
        ) -> String {
            BarrierEndpointRecord::new(
                address.to_string(),
                BarrierProcessIdentity {
                    node_id,
                    boot_incarnation: uuid::Uuid::from_u128(boot),
                    process_term: term,
                },
            )
            .unwrap()
            .encode()
            .unwrap()
        }

        #[tokio::test]
        async fn process_bound_client_pool_is_bounded_and_eviction_is_incarnation_safe() {
            let kv = kv(NodeId(999));
            let kv_dyn: Arc<dyn ClusterKv> = kv.clone();
            let pool: BarrierClientPool = Arc::new(parking_lot::Mutex::new(FxHashMap::default()));
            let count = u64::try_from(crate::checkpoint::MAX_CHECKPOINT_PARTICIPANTS).unwrap() + 5;
            for node_id in 1..=count {
                let boot = u128::from(node_id) + 1_000;
                kv.seed(
                    NodeId(node_id),
                    BARRIER_ADDR_KEY,
                    endpoint_advertisement("127.0.0.1:1".parse().unwrap(), node_id, boot, 1),
                );
                assert!(get_barrier_client(
                    NodeId(node_id),
                    Some(ExpectedBarrierProcess::participant(
                        node_id,
                        uuid::Uuid::from_u128(boot),
                    )),
                    &pool,
                    &kv_dyn,
                )
                .await
                .unwrap()
                .is_some());
            }
            assert_eq!(
                pool.lock().len(),
                crate::checkpoint::MAX_CHECKPOINT_PARTICIPANTS
            );

            let peer = NodeId(count);
            let current = ExpectedBarrierProcess::participant(
                peer.0,
                uuid::Uuid::from_u128(u128::from(peer.0) + 1_000),
            );
            let predecessor = ExpectedBarrierProcess::participant(
                peer.0,
                uuid::Uuid::from_u128(u128::from(peer.0) + 999),
            );
            assert!(matches!(
                get_barrier_client(peer, None, &pool, &kv_dyn).await,
                Err(BarrierClientResolutionError::ProcessMismatch)
            ));
            evict_barrier_client(&pool, peer, None);
            assert!(pool.lock().contains_key(&peer));
            evict_barrier_client(&pool, peer, Some(predecessor));
            assert!(pool.lock().contains_key(&peer));
            evict_barrier_client(&pool, peer, Some(current));
            assert!(!pool.lock().contains_key(&peer));

            let mismatched_peer = NodeId(count + 1);
            kv.seed(
                mismatched_peer,
                BARRIER_ADDR_KEY,
                endpoint_advertisement(
                    "127.0.0.1:1".parse().unwrap(),
                    mismatched_peer.0 + 1,
                    9_999,
                    1,
                ),
            );
            let started = std::time::Instant::now();
            assert!(matches!(
                get_barrier_client(
                    mismatched_peer,
                    Some(ExpectedBarrierProcess::participant(
                        mismatched_peer.0,
                        uuid::Uuid::from_u128(9_999),
                    )),
                    &pool,
                    &kv_dyn,
                )
                .await,
                Err(BarrierClientResolutionError::Invalid(_))
            ));
            assert!(started.elapsed() < Duration::from_millis(100));
        }

        #[tokio::test]
        async fn every_certified_phase_rejects_a_wrong_recipient_before_mutation() {
            use barrier_v1::barrier_sync_server::BarrierSync;

            let (incoming_tx, incoming_rx) =
                crossfire::mpsc::bounded_async::<BarrierAnnouncement>(8);
            let local_process = Arc::new(std::sync::OnceLock::new());
            local_process
                .set(BarrierProcessIdentity {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::from_u128(22),
                    process_term: 1,
                })
                .unwrap();
            let process_lease_deadline = Arc::new(std::sync::OnceLock::new());
            process_lease_deadline
                .set(Arc::new(crate::cluster::control::LeaseDeadline::live_for(
                    Duration::from_secs(60),
                )))
                .unwrap();
            let prepare_acks = Arc::new(parking_lot::Mutex::new(PrepareAckState::default()));
            let server = GrpcBarrierServer {
                incoming_tx,
                prepare_acks: Arc::clone(&prepare_acks),
                leader_lease_store: Arc::new(parking_lot::Mutex::new(None)),
                local_leader_proof: Arc::new(parking_lot::Mutex::new(None)),
                local_process,
                process_lease_deadline,
            };
            let wrong =
                assignment_fence_to_wire(Some(&test_fence(1, &[1, 2], &[(1, 11), (2, 23)])));

            let status = server
                .prepare(tonic::Request::new(barrier_v1::PrepareRequest {
                    epoch: 1,
                    checkpoint_id: 1,
                    flags: 0,
                    assignment_version: wrong.version,
                    assignment_participants: wrong.participants.clone(),
                    assignment_vnode_count: wrong.vnode_count,
                    assignment_map_digest: wrong.map_digest.clone(),
                    leader_proof: None,
                }))
                .await
                .unwrap_err();
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);

            let status = server
                .aligned(tonic::Request::new(barrier_v1::AlignedRequest {
                    epoch: 1,
                    checkpoint_id: 1,
                    flags: 0,
                    min_watermark_ms: None,
                    assignment_version: wrong.version,
                    assignment_participants: wrong.participants.clone(),
                    assignment_vnode_count: wrong.vnode_count,
                    assignment_map_digest: wrong.map_digest.clone(),
                    leader_proof: None,
                }))
                .await
                .unwrap_err();
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);

            let status = server
                .commit(tonic::Request::new(barrier_v1::CommitRequest {
                    epoch: 1,
                    checkpoint_id: 1,
                    flags: 0,
                    min_watermark_ms: None,
                    assignment_version: wrong.version,
                    assignment_participants: wrong.participants.clone(),
                    assignment_vnode_count: wrong.vnode_count,
                    assignment_map_digest: wrong.map_digest.clone(),
                    leader_proof: None,
                }))
                .await
                .unwrap_err();
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);

            let status = server
                .abort(tonic::Request::new(barrier_v1::AbortRequest {
                    epoch: 1,
                    checkpoint_id: 1,
                    flags: 0,
                    assignment_version: wrong.version,
                    assignment_participants: wrong.participants,
                    assignment_vnode_count: wrong.vnode_count,
                    assignment_map_digest: wrong.map_digest,
                    leader_proof: None,
                }))
                .await
                .unwrap_err();
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            assert!(incoming_rx.try_recv().is_err());
            assert!(prepare_acks.lock().pending.is_empty());
            assert!(prepare_acks.lock().received_at.is_empty());
        }

        async fn assert_fenced_phase_rejections(
            server: &GrpcBarrierServer,
            assignment: WireAssignmentFence,
            proof: Option<barrier_v1::LeaderProof>,
            epoch: u64,
            checkpoint_id: u64,
        ) {
            use barrier_v1::barrier_sync_server::BarrierSync;

            let status = server
                .aligned(tonic::Request::new(barrier_v1::AlignedRequest {
                    epoch,
                    checkpoint_id,
                    flags: 0,
                    min_watermark_ms: None,
                    assignment_version: assignment.version,
                    assignment_participants: assignment.participants.clone(),
                    assignment_vnode_count: assignment.vnode_count,
                    assignment_map_digest: assignment.map_digest.clone(),
                    leader_proof: proof.clone(),
                }))
                .await
                .unwrap_err();
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            let status = server
                .commit(tonic::Request::new(barrier_v1::CommitRequest {
                    epoch,
                    checkpoint_id,
                    flags: 0,
                    min_watermark_ms: None,
                    assignment_version: assignment.version,
                    assignment_participants: assignment.participants.clone(),
                    assignment_vnode_count: assignment.vnode_count,
                    assignment_map_digest: assignment.map_digest.clone(),
                    leader_proof: proof.clone(),
                }))
                .await
                .unwrap_err();
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            let status = server
                .abort(tonic::Request::new(barrier_v1::AbortRequest {
                    epoch,
                    checkpoint_id,
                    flags: 0,
                    assignment_version: assignment.version,
                    assignment_participants: assignment.participants,
                    assignment_vnode_count: assignment.vnode_count,
                    assignment_map_digest: assignment.map_digest,
                    leader_proof: proof,
                }))
                .await
                .unwrap_err();
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        }

        #[tokio::test]
        async fn fenced_process_rejects_cached_prepare_and_every_phase_before_mutation() {
            use barrier_v1::barrier_sync_server::BarrierSync;

            let (store, proof) = lease_authority().await;
            let (incoming_tx, incoming_rx) =
                crossfire::mpsc::bounded_async::<BarrierAnnouncement>(8);
            let local_process = Arc::new(std::sync::OnceLock::new());
            local_process
                .set(BarrierProcessIdentity {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::from_u128(22),
                    process_term: 1,
                })
                .unwrap();
            let deadline = Arc::new(crate::cluster::control::LeaseDeadline::live_for(
                Duration::from_secs(60),
            ));
            let process_lease_deadline = Arc::new(std::sync::OnceLock::new());
            process_lease_deadline.set(Arc::clone(&deadline)).unwrap();

            let epoch = 7;
            let checkpoint_id = 70;
            let fence = test_fence(9, &[1, 2], &[(1, 1), (2, 22)]);
            let assignment_digest = Some(fence.digest());
            let identity = BarrierIdentity {
                attempt: CheckpointAttempt::new(epoch, checkpoint_id),
                assignment_digest,
            };
            let cached_ack = BarrierAck {
                epoch,
                checkpoint_id,
                assignment_digest,
                ok: true,
                error: None,
                watermark: CheckpointWatermark::Active(17),
            };
            let mut ack_state = PrepareAckState::default();
            ack_state.completed.insert(identity, cached_ack.clone());
            let prepare_acks = Arc::new(parking_lot::Mutex::new(ack_state));
            let server = GrpcBarrierServer {
                incoming_tx,
                prepare_acks: Arc::clone(&prepare_acks),
                leader_lease_store: Arc::new(parking_lot::Mutex::new(Some(store))),
                local_leader_proof: Arc::new(parking_lot::Mutex::new(None)),
                local_process,
                process_lease_deadline,
            };
            let assignment = assignment_fence_to_wire(Some(&fence));
            let proof = leader_proof_to_wire(Some(&proof));
            deadline.fence();

            let status = server
                .prepare(tonic::Request::new(barrier_v1::PrepareRequest {
                    epoch,
                    checkpoint_id,
                    flags: 0,
                    assignment_version: assignment.version,
                    assignment_participants: assignment.participants.clone(),
                    assignment_vnode_count: assignment.vnode_count,
                    assignment_map_digest: assignment.map_digest.clone(),
                    leader_proof: proof.clone(),
                }))
                .await
                .unwrap_err();
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);

            assert_fenced_phase_rejections(&server, assignment, proof, epoch, checkpoint_id).await;

            let state = prepare_acks.lock();
            assert_eq!(state.completed.get(&identity), Some(&cached_ack));
            assert!(state.pending.is_empty());
            assert!(state.received_at.is_empty());
            assert!(incoming_rx.try_recv().is_err());
        }

        #[tokio::test]
        async fn process_fence_wakes_an_in_flight_prepare_without_an_ack() {
            use barrier_v1::barrier_sync_server::BarrierSync;

            let (store, proof) = lease_authority().await;
            let (incoming_tx, incoming_rx) =
                crossfire::mpsc::bounded_async::<BarrierAnnouncement>(1);
            let local_process = Arc::new(std::sync::OnceLock::new());
            local_process
                .set(BarrierProcessIdentity {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::from_u128(22),
                    process_term: 1,
                })
                .unwrap();
            let deadline = Arc::new(crate::cluster::control::LeaseDeadline::live_for(
                Duration::from_secs(60),
            ));
            let process_lease_deadline = Arc::new(std::sync::OnceLock::new());
            process_lease_deadline.set(Arc::clone(&deadline)).unwrap();
            let prepare_acks = Arc::new(parking_lot::Mutex::new(PrepareAckState::default()));
            let server = GrpcBarrierServer {
                incoming_tx,
                prepare_acks: Arc::clone(&prepare_acks),
                leader_lease_store: Arc::new(parking_lot::Mutex::new(Some(store))),
                local_leader_proof: Arc::new(parking_lot::Mutex::new(None)),
                local_process,
                process_lease_deadline,
            };
            let fence = test_fence(9, &[1, 2], &[(1, 1), (2, 22)]);
            let assignment = assignment_fence_to_wire(Some(&fence));
            let request = tonic::Request::new(barrier_v1::PrepareRequest {
                epoch: 8,
                checkpoint_id: 80,
                flags: 0,
                assignment_version: assignment.version,
                assignment_participants: assignment.participants,
                assignment_vnode_count: assignment.vnode_count,
                assignment_map_digest: assignment.map_digest,
                leader_proof: leader_proof_to_wire(Some(&proof)),
            });
            let call = server.prepare(request);
            tokio::pin!(call);
            tokio::select! {
                result = &mut call => panic!("Prepare returned before fencing: {result:?}"),
                announcement = incoming_rx.recv() => {
                    assert_eq!(announcement.unwrap().phase, Phase::Prepare);
                }
            }

            deadline.fence();
            let status = tokio::time::timeout(Duration::from_secs(1), &mut call)
                .await
                .expect("fencing did not wake Prepare")
                .unwrap_err();
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            assert!(prepare_acks.lock().pending.is_empty());
            assert!(prepare_acks.lock().completed.is_empty());
        }

        fn proof(
            node_id: u64,
            boot: u128,
            process_term: u64,
            token: u64,
        ) -> crate::cluster::control::LeaderProof {
            crate::cluster::control::LeaderProof {
                owner: crate::checkpoint::LeaderProofOwner {
                    node_id,
                    boot_id: uuid::Uuid::from_u128(boot),
                    process_term,
                },
                fencing_token: token,
            }
        }

        #[tokio::test]
        async fn prepare_validation_precedes_transport_start_and_durable_publication() {
            let leader_kv = kv(NodeId(1));
            let (store, leader_proof) = lease_authority().await;
            let leader = coordinator(leader_kv.clone(), store);
            let valid = BarrierAnnouncement {
                epoch: 1,
                checkpoint_id: 40,
                assignment_fence: Some(test_fence(9, &[1, 2], &[(1, 1), (2, 22)])),
                leader_proof: Some(leader_proof.clone()),
                phase: Phase::Prepare,
                flags: 0,
                min_watermark_ms: None,
            };

            assert!(leader
                .announce_prepare(&valid, Duration::ZERO)
                .await
                .is_err());
            assert!(leader_kv
                .read_from(NodeId(1), ANNOUNCEMENT_KEY)
                .await
                .is_none());

            let leader_outside_roster = BarrierAnnouncement {
                epoch: 1,
                checkpoint_id: 41,
                assignment_fence: Some(test_fence(10, &[2], &[(2, 22)])),
                leader_proof: Some(leader_proof),
                ..valid
            };
            assert!(leader
                .announce_prepare(&leader_outside_roster, Duration::from_secs(1))
                .await
                .is_err());
            assert!(leader_kv
                .read_from(NodeId(1), ANNOUNCEMENT_KEY)
                .await
                .is_none());
        }

        #[tokio::test]
        async fn remote_proof_confirmation_acknowledges_only_the_exact_live_provider_value() {
            let caller_kv = kv(NodeId(1));
            let remote_kv = kv(NodeId(2));
            let caller = BarrierCoordinator::new(caller_kv.clone());
            let remote = BarrierCoordinator::new(remote_kv);
            let expected = proof(2, 22, 7, 41);
            bind_process(&remote, 2, 22, 7);

            caller
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            let remote_addr = remote
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            caller_kv.seed(
                NodeId(2),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(remote_addr, 2, 22, 7),
            );
            caller_kv.seed(
                NodeId(3),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(remote_addr, 3, 22, 7),
            );

            let deadline = || tokio::time::Instant::now() + std::time::Duration::from_secs(1);
            assert!(
                !caller
                    .confirm_remote_leader_proof(&expected, deadline())
                    .await
                    .unwrap(),
                "an absent provider must fail closed"
            );

            let live = Arc::new(parking_lot::Mutex::new(Some(expected.clone())));
            let provider = Arc::clone(&live);
            remote.set_local_leader_proof_provider(Arc::new(move || provider.lock().clone()));
            assert!(caller
                .confirm_remote_leader_proof(&expected, deadline())
                .await
                .unwrap());

            let mut wrong_token = expected.clone();
            wrong_token.fencing_token += 1;
            assert!(
                !caller
                    .confirm_remote_leader_proof(&wrong_token, deadline())
                    .await
                    .unwrap(),
                "the acknowledgement must bind the fencing token"
            );

            let mut wrong_process = expected.clone();
            wrong_process.owner.process_term += 1;
            caller_kv.seed(
                NodeId(2),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(remote_addr, 2, 22, 8),
            );
            assert!(
                !caller
                    .confirm_remote_leader_proof(&wrong_process, deadline())
                    .await
                    .unwrap(),
                "the acknowledgement must bind the process term"
            );
            caller_kv.seed(
                NodeId(2),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(remote_addr, 2, 22, 7),
            );

            let mut wrong_boot = expected.clone();
            wrong_boot.owner.boot_id = uuid::Uuid::from_u128(23);
            assert!(
                !caller
                    .confirm_remote_leader_proof(&wrong_boot, deadline())
                    .await
                    .unwrap(),
                "the acknowledgement must bind the boot incarnation"
            );

            let mut wrong_node = expected.clone();
            wrong_node.owner.node_id = 3;
            assert!(
                !caller
                    .confirm_remote_leader_proof(&wrong_node, deadline())
                    .await
                    .unwrap(),
                "the acknowledgement must bind the stable node identity"
            );

            *live.lock() = None;
            assert!(
                !caller
                    .confirm_remote_leader_proof(&expected, deadline())
                    .await
                    .unwrap(),
                "an expired process-local grant must fail closed"
            );
        }

        #[tokio::test]
        async fn proof_confirmation_rotates_a_same_node_endpoint_without_stale_eviction() {
            let caller_kv = kv(NodeId(1));
            let caller = BarrierCoordinator::new(caller_kv.clone());
            caller
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();

            let predecessor = BarrierCoordinator::new(kv(NodeId(2)));
            let successor = BarrierCoordinator::new(kv(NodeId(2)));
            let predecessor_proof = proof(2, 22, 7, 41);
            let successor_proof = proof(2, 23, 8, 42);
            bind_process(&predecessor, 2, 22, 7);
            bind_process(&successor, 2, 23, 8);
            let predecessor_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let successor_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let calls = Arc::clone(&predecessor_calls);
            let live = predecessor_proof.clone();
            predecessor.set_local_leader_proof_provider(Arc::new(move || {
                calls.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                Some(live.clone())
            }));
            let calls = Arc::clone(&successor_calls);
            let live = successor_proof.clone();
            successor.set_local_leader_proof_provider(Arc::new(move || {
                calls.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                Some(live.clone())
            }));
            let predecessor_addr = predecessor
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            let successor_addr = successor
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            caller_kv.seed(
                NodeId(2),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(predecessor_addr, 2, 22, 7),
            );
            let deadline = || tokio::time::Instant::now() + Duration::from_secs(1);
            assert!(caller
                .confirm_remote_leader_proof(&predecessor_proof, deadline())
                .await
                .unwrap());

            caller_kv.seed(
                NodeId(2),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(successor_addr, 2, 23, 8),
            );
            assert!(caller
                .confirm_remote_leader_proof(&successor_proof, deadline())
                .await
                .unwrap());
            assert_eq!(
                predecessor_calls.load(std::sync::atomic::Ordering::Acquire),
                1
            );
            assert_eq!(
                successor_calls.load(std::sync::atomic::Ordering::Acquire),
                1
            );

            assert!(!caller
                .confirm_remote_leader_proof(&predecessor_proof, deadline())
                .await
                .unwrap());
            assert_eq!(
                predecessor_calls.load(std::sync::atomic::Ordering::Acquire),
                1
            );
            assert_eq!(
                successor_calls.load(std::sync::atomic::Ordering::Acquire),
                1
            );
            let cached = caller.grpc.lock().clone().unwrap();
            assert_eq!(
                cached.clients.lock().get(&NodeId(2)).unwrap().process,
                Some(BarrierProcessIdentity {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::from_u128(23),
                    process_term: 8,
                })
            );
        }

        /// Observation is latest-wins (non-destructive), so wait for the
        /// expected phase specifically — earlier phases may linger.
        async fn wait_observe(
            coord: &BarrierCoordinator,
            leader: NodeId,
            phase: Phase,
        ) -> BarrierAnnouncement {
            for _ in 0..100 {
                if let Some(ann) = coord.observe_hint(leader).await.unwrap() {
                    if ann.phase == phase {
                        return ann;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            panic!("timed out waiting for {phase:?} announcement from leader {leader:?}");
        }

        async fn wait_observe_exact(
            coord: &BarrierCoordinator,
            leader: NodeId,
            expected: CheckpointAttempt,
            phase: Phase,
        ) -> BarrierAnnouncement {
            for _ in 0..100 {
                if let Some(announcement) = coord.observe_hint(leader).await.unwrap() {
                    if announcement_attempt(&announcement) == expected
                        && announcement.phase == phase
                    {
                        return announcement;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            panic!(
                "timed out waiting for {phase:?} announcement {expected:?} from leader {leader:?}"
            );
        }

        fn pending_prepare_waiters(
            coordinator: &BarrierCoordinator,
            prepare: &BarrierAnnouncement,
        ) -> usize {
            let identity = BarrierIdentity::from_announcement(prepare);
            let state = coordinator.grpc.lock().clone().unwrap();
            let waiters = state
                .prepare_acks
                .lock()
                .pending
                .get(&identity)
                .map_or(0, Vec::len);
            waiters
        }

        async fn wait_for_direct_prepare(
            coordinator: &BarrierCoordinator,
            prepare: &BarrierAnnouncement,
        ) {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if coordinator.prepare_received_at(prepare).is_some() {
                        return;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("direct Prepare was not delivered");
        }

        async fn started_barrier_pair() -> (
            BarrierCoordinator,
            BarrierCoordinator,
            crate::cluster::control::LeaderProof,
        ) {
            let leader_kv = kv(NodeId(1));
            let follower_kv = kv(NodeId(2));
            let (store, proof) = lease_authority().await;
            let leader = coordinator(leader_kv.clone(), Arc::clone(&store));
            let follower = coordinator(follower_kv, store);
            bind_process(&leader, 1, 1, 1);
            bind_process(&follower, 2, 22, 1);
            leader
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            let follower_addr = follower
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            leader_kv.seed(
                NodeId(2),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(follower_addr, 2, 22, 1),
            );
            (leader, follower, proof)
        }

        #[tokio::test]
        async fn failed_durable_prepare_publication_prevents_direct_delivery() {
            let leader_inner = kv(NodeId(1));
            let leader_kv = Arc::new(RejectAnnouncementKv {
                inner: Arc::clone(&leader_inner),
            });
            let follower_kv = kv(NodeId(2));
            let (store, proof) = lease_authority().await;
            let leader = coordinator(leader_kv, Arc::clone(&store));
            let follower = coordinator(follower_kv, store);
            bind_process(&leader, 1, 1, 1);
            bind_process(&follower, 2, 22, 1);
            leader
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            let follower_addr = follower
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            leader_inner.seed(
                NodeId(2),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(follower_addr, 2, 22, 1),
            );

            let prepare = BarrierAnnouncement {
                epoch: 1,
                checkpoint_id: 41,
                assignment_fence: Some(test_fence(9, &[1, 2], &[(1, 1), (2, 22)])),
                leader_proof: Some(proof),
                phase: Phase::Prepare,
                flags: crate::checkpoint::flags::FULL_SNAPSHOT,
                min_watermark_ms: None,
            };
            let mut follower_watch = follower.announcement_watch().unwrap();

            let error = leader
                .announce_prepare(&prepare, Duration::from_secs(1))
                .await
                .unwrap_err();

            assert!(error.contains("injected durable write failure"), "{error}");
            assert!(leader_inner
                .read_from(NodeId(1), ANNOUNCEMENT_KEY)
                .await
                .is_none());
            assert!(
                tokio::time::timeout(Duration::from_millis(100), follower_watch.changed())
                    .await
                    .is_err(),
                "failed durable publication must not deliver a direct announcement"
            );
            assert!(follower.prepare_received_at(&prepare).is_none());
        }

        #[tokio::test]
        async fn announce_starts_one_prepare_rpc_before_quorum_wait() {
            let (leader, follower, proof) = started_barrier_pair().await;

            let fence = test_fence(9, &[1, 2], &[(1, 1), (2, 22)]);
            let prepare = BarrierAnnouncement {
                epoch: 1,
                checkpoint_id: 41,
                assignment_fence: Some(fence.clone()),
                leader_proof: Some(proof),
                phase: Phase::Prepare,
                flags: crate::checkpoint::flags::FULL_SNAPSHOT,
                min_watermark_ms: None,
            };

            assert!(leader.announce(&prepare).await.is_err());
            assert!(
                leader
                    .kv
                    .read_from(NodeId(1), ANNOUNCEMENT_KEY)
                    .await
                    .is_none(),
                "generic announcement must reject certified Prepare before durable publication"
            );
            tokio::time::timeout(
                Duration::from_millis(500),
                leader.announce_prepare(&prepare, Duration::from_secs(1)),
            )
            .await
            .expect("announce must not wait for the follower acknowledgement")
            .unwrap();
            wait_for_direct_prepare(&follower, &prepare).await;
            let direct = wait_observe_exact(
                &follower,
                NodeId(1),
                CheckpointAttempt::new(prepare.epoch, prepare.checkpoint_id),
                Phase::Prepare,
            )
            .await;
            assert_eq!(direct, prepare);
            assert_eq!(pending_prepare_waiters(&follower, &prepare), 1);
            let accepted_json = leader
                .kv
                .read_from(NodeId(1), ANNOUNCEMENT_KEY)
                .await
                .unwrap();
            leader
                .announce_prepare(&prepare, Duration::from_secs(1))
                .await
                .unwrap();
            let conflicting = BarrierAnnouncement {
                flags: 0,
                ..prepare.clone()
            };
            assert!(leader
                .announce_prepare(&conflicting, Duration::from_secs(1))
                .await
                .is_err());
            assert_eq!(
                leader.kv.read_from(NodeId(1), ANNOUNCEMENT_KEY).await,
                Some(accepted_json),
                "rejected equivocation must not overwrite the durable announcement"
            );
            assert_eq!(
                pending_prepare_waiters(&follower, &prepare),
                1,
                "idempotent and conflicting publications must not issue another RPC"
            );

            let quorum = leader.wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_secs(1));
            tokio::pin!(quorum);
            tokio::select! {
                outcome = &mut quorum => panic!("silent follower completed quorum early: {outcome:?}"),
                () = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
            assert_eq!(
                pending_prepare_waiters(&follower, &prepare),
                1,
                "wait_for_quorum must consume the eager task instead of issuing a duplicate RPC"
            );

            follower
                .ack(&BarrierAck {
                    epoch: prepare.epoch,
                    checkpoint_id: prepare.checkpoint_id,
                    assignment_digest: Some(fence.digest()),
                    ok: true,
                    error: None,
                    watermark: CheckpointWatermark::Active(91),
                })
                .await
                .unwrap();
            assert!(matches!(
                quorum.await,
                QuorumOutcome::Reached {
                    acks,
                    follower_watermark: CheckpointWatermark::Active(91),
                } if acks == vec![NodeId(2)]
            ));
        }

        #[tokio::test]
        async fn eager_prepare_ack_before_quorum_wait_is_retained() {
            let (leader, follower, proof) = started_barrier_pair().await;
            let fence = test_fence(10, &[1, 2], &[(1, 1), (2, 22)]);
            let prepare = BarrierAnnouncement {
                epoch: 2,
                checkpoint_id: 42,
                assignment_fence: Some(fence.clone()),
                leader_proof: Some(proof),
                phase: Phase::Prepare,
                flags: 0,
                min_watermark_ms: None,
            };

            leader
                .announce_prepare(&prepare, Duration::from_secs(1))
                .await
                .unwrap();
            wait_for_direct_prepare(&follower, &prepare).await;
            follower
                .ack(&BarrierAck {
                    epoch: prepare.epoch,
                    checkpoint_id: prepare.checkpoint_id,
                    assignment_digest: Some(fence.digest()),
                    ok: true,
                    error: None,
                    watermark: CheckpointWatermark::Active(92),
                })
                .await
                .unwrap();

            let outcome = leader
                .wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_secs(1))
                .await;
            assert!(matches!(
                outcome,
                QuorumOutcome::Reached {
                    acks,
                    follower_watermark: CheckpointWatermark::Active(92),
                } if acks == vec![NodeId(2)]
            ));
        }

        #[tokio::test]
        async fn quorum_deadline_aborts_a_silent_eager_prepare_rpc() {
            let (leader, follower, proof) = started_barrier_pair().await;

            let fence = test_fence(10, &[1, 2], &[(1, 1), (2, 22)]);
            let prepare = BarrierAnnouncement {
                epoch: 2,
                checkpoint_id: 42,
                assignment_fence: Some(fence),
                leader_proof: Some(proof),
                phase: Phase::Prepare,
                flags: 0,
                min_watermark_ms: None,
            };
            leader
                .announce_prepare(&prepare, Duration::from_millis(50))
                .await
                .unwrap();
            wait_for_direct_prepare(&follower, &prepare).await;

            let started = std::time::Instant::now();
            let outcome = leader
                .wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_millis(50))
                .await;
            assert!(matches!(
                outcome,
                QuorumOutcome::TimedOut {
                    got,
                    missing,
                } if got.is_empty() && missing == vec![NodeId(2)]
            ));
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "the caller deadline must bound the eager task's longer transport deadline"
            );
            tokio::time::timeout(Duration::from_secs(1), async {
                while pending_prepare_waiters(&follower, &prepare) != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("caller timeout did not cancel the follower Prepare waiter");
        }

        #[tokio::test]
        async fn newer_and_terminal_announcements_retire_eager_prepare_tasks() {
            let (leader, follower, proof) = started_barrier_pair().await;
            let fence = test_fence(11, &[1, 2], &[(1, 1), (2, 22)]);
            let first = BarrierAnnouncement {
                epoch: 3,
                checkpoint_id: 43,
                assignment_fence: Some(fence.clone()),
                leader_proof: Some(proof.clone()),
                phase: Phase::Prepare,
                flags: 0,
                min_watermark_ms: None,
            };
            leader
                .announce_prepare(&first, Duration::from_secs(1))
                .await
                .unwrap();
            wait_for_direct_prepare(&follower, &first).await;
            assert_eq!(pending_prepare_waiters(&follower, &first), 1);

            let successor = BarrierAnnouncement {
                epoch: 4,
                checkpoint_id: 44,
                ..first.clone()
            };
            leader
                .announce_prepare(&successor, Duration::from_secs(1))
                .await
                .unwrap();
            wait_for_direct_prepare(&follower, &successor).await;
            tokio::time::timeout(Duration::from_secs(1), async {
                while pending_prepare_waiters(&follower, &first) != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the newer Prepare did not cancel its predecessor task");
            assert_eq!(pending_prepare_waiters(&follower, &successor), 1);

            leader
                .announce(&BarrierAnnouncement {
                    phase: Phase::Abort,
                    ..successor.clone()
                })
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(1), async {
                while pending_prepare_waiters(&follower, &successor) != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the terminal announcement did not cancel its Prepare task");
            assert!(
                leader
                    .grpc
                    .lock()
                    .as_ref()
                    .unwrap()
                    .prepare_fanout
                    .lock()
                    .is_none(),
                "the terminal announcement must retire the batch identity"
            );
        }

        #[tokio::test]
        async fn quorum_roster_mismatch_aborts_the_eager_batch() {
            let (leader, follower, proof) = started_barrier_pair().await;
            let fence = test_fence(12, &[1, 2], &[(1, 1), (2, 22)]);
            let prepare = BarrierAnnouncement {
                epoch: 5,
                checkpoint_id: 45,
                assignment_fence: Some(fence),
                leader_proof: Some(proof),
                phase: Phase::Prepare,
                flags: 0,
                min_watermark_ms: None,
            };
            leader
                .announce_prepare(&prepare, Duration::from_millis(100))
                .await
                .unwrap();
            wait_for_direct_prepare(&follower, &prepare).await;

            let outcome = leader
                .wait_for_quorum(&prepare, &[NodeId(3)], Duration::from_millis(100))
                .await;
            assert!(matches!(outcome, QuorumOutcome::Failed { .. }));
            tokio::time::timeout(Duration::from_secs(1), async {
                while pending_prepare_waiters(&follower, &prepare) != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("roster mismatch did not abort the eager Prepare task");

            let retry = leader
                .wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_millis(100))
                .await;
            assert!(
                matches!(retry, QuorumOutcome::Failed { .. }),
                "a rejected batch must not be silently recreated: {retry:?}"
            );
        }

        #[tokio::test]
        async fn test_grpc_barrier_flow() {
            let leader_kv = kv(NodeId(1));
            let follower_kv = kv(NodeId(2));
            let (store, proof) = lease_authority().await;
            let leader_coord = coordinator(leader_kv.clone(), Arc::clone(&store));
            let follower_coord = coordinator(follower_kv.clone(), store);
            bind_process(&leader_coord, 1, 1, 1);
            bind_process(&follower_coord, 2, 22, 1);

            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let leader_addr = leader_coord.start_server(addr, None).await.unwrap();
            let bound_addr = follower_coord.start_server(addr, None).await.unwrap();

            leader_kv.seed(
                NodeId(2),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(bound_addr, 2, 22, 1),
            );
            follower_kv.seed(
                NodeId(1),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(leader_addr, 1, 1, 1),
            );

            // Sequencing handshake: observation is latest-wins, so the
            // leader must not announce Commit until the follower has
            // observed Aligned (otherwise Commit may overwrite it).
            let (aligned_seen_tx, aligned_seen_rx) = tokio::sync::oneshot::channel::<()>();
            let assignment_fence = test_fence(9, &[1, 2, 1, 2], &[(1, 1), (2, 22)]);
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
            leader_coord
                .announce_prepare(&prepare, Duration::from_secs(5))
                .await
                .unwrap();

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
        async fn certified_phase_uses_frozen_roster_not_active_membership() {
            let leader_kv = kv(NodeId(1));
            let follower_kv = kv(NodeId(2));
            let outsider_kv = kv(NodeId(3));
            let (store, leader_proof) = lease_authority().await;
            let mut leader = coordinator(leader_kv.clone(), Arc::clone(&store));
            let follower = coordinator(follower_kv, Arc::clone(&store));
            let outsider = coordinator(outsider_kv, store);
            bind_process(&leader, 1, 1, 1);
            bind_process(&follower, 2, 22, 1);
            bind_process(&outsider, 3, 33, 1);

            let member = |node_id: u64, state| NodeInfo {
                id: NodeId(node_id),
                name: format!("node-{node_id}"),
                rpc_address: String::new(),
                raft_address: String::new(),
                state,
                metadata: crate::cluster::discovery::NodeMetadata::default(),
                last_heartbeat_ms: 0,
            };
            let (_members_tx, members_rx) = watch::channel(vec![
                member(1, NodeState::Active),
                member(2, NodeState::Draining),
                member(3, NodeState::Active),
            ]);
            leader.set_leader_election(NodeId(1), members_rx, Arc::new(AtomicBool::new(true)));

            leader
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            let follower_addr = follower
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            let outsider_addr = outsider
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            leader_kv.seed(
                NodeId(2),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(follower_addr, 2, 22, 1),
            );
            leader_kv.seed(
                NodeId(3),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(outsider_addr, 3, 33, 1),
            );

            let fence = test_fence(9, &[1, 2], &[(1, 1), (2, 22)]);
            let aligned = BarrierAnnouncement {
                epoch: 4,
                checkpoint_id: 44,
                assignment_fence: Some(fence),
                leader_proof: Some(leader_proof),
                phase: Phase::Aligned,
                flags: 0,
                min_watermark_ms: Some(100),
            };
            leader.announce(&aligned).await.unwrap();
            let observed = wait_observe_exact(
                &follower,
                NodeId(1),
                CheckpointAttempt::new(4, 44),
                Phase::Aligned,
            )
            .await;
            assert_eq!(observed, aligned);
            assert!(outsider
                .grpc
                .lock()
                .as_ref()
                .unwrap()
                .latest_rx
                .borrow()
                .is_none());
        }

        #[tokio::test]
        async fn prepare_reconnects_a_stale_client_within_the_same_quorum_deadline() {
            let leader_kv = kv(NodeId(1));
            let follower_kv = kv(NodeId(2));
            let (store, proof) = lease_authority().await;
            let leader_coord = coordinator(leader_kv.clone(), Arc::clone(&store));
            let follower_coord = coordinator(follower_kv.clone(), store);
            bind_process(&leader_coord, 1, 1, 1);
            bind_process(&follower_coord, 2, 22, 1);
            let leader_addr = leader_coord
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            let follower_addr = follower_coord
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            leader_kv.seed(
                NodeId(2),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(follower_addr, 2, 22, 1),
            );
            follower_kv.seed(
                NodeId(1),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(leader_addr, 1, 1, 1),
            );

            let dead_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let dead_addr = dead_listener.local_addr().unwrap();
            drop(dead_listener);
            let dead_channel =
                tonic::transport::Endpoint::from_shared(format!("http://{dead_addr}"))
                    .unwrap()
                    .connect_lazy();
            let state = leader_coord.grpc.lock().clone().unwrap();
            state.clients.lock().insert(
                NodeId(2),
                BarrierClientEntry {
                    process: Some(BarrierProcessIdentity {
                        node_id: 2,
                        boot_incarnation: uuid::Uuid::from_u128(22),
                        process_term: 1,
                    }),
                    client: barrier_v1::barrier_sync_client::BarrierSyncClient::new(dead_channel),
                },
            );

            let assignment_fence = test_fence(9, &[1, 2], &[(1, 1), (2, 22)]);
            let follower_fence = assignment_fence.clone();
            let prepare = BarrierAnnouncement {
                epoch: 2,
                checkpoint_id: 43,
                assignment_fence: Some(assignment_fence),
                leader_proof: Some(proof),
                phase: Phase::Prepare,
                flags: 0,
                min_watermark_ms: None,
            };
            leader_coord
                .announce_prepare(&prepare, Duration::from_secs(2))
                .await
                .unwrap();

            let follower = async {
                let announcement = wait_observe(&follower_coord, NodeId(1), Phase::Prepare).await;
                follower_coord
                    .ack(&BarrierAck {
                        epoch: announcement.epoch,
                        checkpoint_id: announcement.checkpoint_id,
                        assignment_digest: Some(follower_fence.digest()),
                        ok: true,
                        error: None,
                        watermark: CheckpointWatermark::Active(101),
                    })
                    .await
                    .unwrap();
            };
            let leader =
                leader_coord.wait_for_quorum(&prepare, &[NodeId(2)], Duration::from_secs(2));
            let (outcome, ()) = tokio::join!(leader, follower);

            assert!(
                matches!(
                    &outcome,
                    QuorumOutcome::Reached {
                        acks,
                        follower_watermark: CheckpointWatermark::Active(101),
                    } if acks.as_slice() == [NodeId(2)]
                ),
                "the stale transport client must be evicted and re-resolved: {outcome:?}"
            );
        }

        #[tokio::test]
        async fn terminal_hint_does_not_authorize_or_close_a_prepare_attempt() {
            let leader_kv = kv(NodeId(1));
            let follower_kv = kv(NodeId(2));
            let (store, proof) = lease_authority().await;
            let leader_coord = coordinator(leader_kv.clone(), Arc::clone(&store));
            let follower_coord = coordinator(follower_kv.clone(), store);
            let leader_addr = leader_coord
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            let follower_addr = follower_coord
                .start_server("127.0.0.1:0".parse().unwrap(), None)
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
            let leader_addr = leader_coord
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            let follower_addr = follower_coord
                .start_server("127.0.0.1:0".parse().unwrap(), None)
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
            bind_process(&leader_coord, 1, 1, 1);
            bind_process(&follower_coord, 2, 22, 1);
            let leader_addr = leader_coord
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            let follower_addr = follower_coord
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            leader_kv.seed(
                NodeId(2),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(follower_addr, 2, 22, 1),
            );
            follower_kv.seed(
                NodeId(1),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(leader_addr, 1, 1, 1),
            );

            let accepted_fence = test_fence(9, &[1, 2, 1, 2], &[(1, 1), (2, 22)]);
            let conflicting_fence = test_fence(9, &[2, 1, 1, 2], &[(1, 1), (2, 22)]);
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
            leader_coord
                .announce_prepare(&accepted, Duration::from_secs(2))
                .await
                .unwrap();

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
                matches!(conflicting_outcome, QuorumOutcome::Failed { .. }),
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
        async fn successor_abort_does_not_poison_the_grpc_relay() {
            let authority = Arc::new(crate::cluster::control::LeaderLeaseStore::new(
                Arc::new(InMemory::new()),
                1,
            ));
            let original_owner = crate::cluster::control::LeaderLeaseOwner {
                node: NodeId(1),
                boot: uuid::Uuid::from_u128(1),
                process_term: 1,
            };
            let successor_owner = crate::cluster::control::LeaderLeaseOwner {
                node: NodeId(3),
                boot: uuid::Uuid::from_u128(3),
                process_term: 2,
            };
            let original_lease = match authority.begin_new_term(&original_owner, 1).await.unwrap() {
                crate::cluster::control::LeaseOutcome::Acquired(lease) => lease,
                crate::cluster::control::LeaseOutcome::Held(_) => unreachable!(),
            };
            let takeover_observation = authority
                .observe_rival(&successor_owner, &original_lease)
                .unwrap();
            let successor_proof = proof(3, 3, 2, original_lease.token + 1);

            let leader_kv = kv(NodeId(1));
            let follower_kv = kv(NodeId(2));
            let successor_kv = kv(NodeId(3));
            let leader = coordinator(leader_kv.clone(), Arc::clone(&authority));
            let follower = coordinator(follower_kv.clone(), Arc::clone(&authority));
            let successor = coordinator(successor_kv.clone(), Arc::clone(&authority));
            bind_process(&leader, 1, 1, 1);
            bind_process(&follower, 2, 2, 1);
            bind_process(&successor, 3, 3, 2);
            leader
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            let follower_addr = follower
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            successor
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();
            leader_kv.seed(
                NodeId(2),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(follower_addr, 2, 2, 1),
            );
            successor_kv.seed(
                NodeId(2),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(follower_addr, 2, 2, 1),
            );

            let aligned = BarrierAnnouncement {
                epoch: 12,
                checkpoint_id: 12,
                assignment_fence: Some(test_fence(1, &[1, 2], &[(1, 1), (2, 2)])),
                leader_proof: Some(original_lease.proof()),
                phase: Phase::Aligned,
                flags: 0,
                min_watermark_ms: None,
            };
            let abort = BarrierAnnouncement {
                assignment_fence: Some(test_fence(2, &[2, 3], &[(2, 2), (3, 3)])),
                leader_proof: Some(successor_proof.clone()),
                phase: Phase::Abort,
                ..aligned.clone()
            };

            leader.announce(&aligned).await.unwrap();
            wait_observe_exact(
                &follower,
                NodeId(1),
                CheckpointAttempt::new(12, 12),
                Phase::Aligned,
            )
            .await;
            successor.announce(&abort).await.unwrap();
            wait_observe_exact(
                &follower,
                NodeId(1),
                CheckpointAttempt::new(12, 12),
                Phase::Abort,
            )
            .await;
            leader.announce(&aligned).await.unwrap();

            tokio::time::sleep(Duration::from_millis(2)).await;
            let successor_lease = match authority
                .try_takeover(&successor_owner, &takeover_observation, 2)
                .await
                .unwrap()
            {
                crate::cluster::control::LeaseOutcome::Acquired(lease) => lease,
                crate::cluster::control::LeaseOutcome::Held(_) => unreachable!(),
            };
            assert_eq!(successor_lease.proof(), successor_proof);

            let successor_aligned = BarrierAnnouncement {
                epoch: 13,
                checkpoint_id: 13,
                phase: Phase::Aligned,
                ..abort
            };
            successor.announce(&successor_aligned).await.unwrap();
            wait_observe_exact(
                &follower,
                NodeId(1),
                CheckpointAttempt::new(13, 13),
                Phase::Aligned,
            )
            .await;
            let state = follower.grpc.lock().clone().unwrap();
            assert!(state.merge_error.lock().is_none());
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
            let lease = match store.begin_new_term(&owner, 1).await.unwrap() {
                crate::cluster::control::LeaseOutcome::Acquired(lease) => lease,
                crate::cluster::control::LeaseOutcome::Held(_) => unreachable!(),
            };
            durable.config_mut(|config| config.wait_get_per_call = Duration::from_secs(5));

            let follower_coord = coordinator(kv(NodeId(2)), store);
            bind_process(&follower_coord, 2, 22, 1);
            let bound_addr = follower_coord
                .start_server("127.0.0.1:0".parse().unwrap(), None)
                .await
                .unwrap();

            let leader_kv = kv(NodeId(1));
            leader_kv.seed(
                NodeId(2),
                BARRIER_ADDR_KEY,
                endpoint_advertisement(bound_addr, 2, 22, 1),
            );
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

    #[cfg(feature = "cluster")]
    #[test]
    fn successor_terminal_supersedes_a_reversible_direct_certificate() {
        let (aligned, abort) = failover_aligned_and_abort();

        assert_eq!(
            merge_direct_announcement(aligned.clone(), abort.clone()).unwrap(),
            abort
        );
        assert_eq!(
            merge_direct_announcement(abort.clone(), aligned.clone()).unwrap(),
            abort
        );
        assert!(merge_direct_announcement(
            BarrierAnnouncement {
                phase: Phase::Abort,
                ..aligned.clone()
            },
            abort.clone(),
        )
        .is_err());
        assert!(merge_direct_announcement(
            aligned,
            BarrierAnnouncement {
                phase: Phase::Prepare,
                ..abort
            },
        )
        .is_err());
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

        let (aligned, abort) = failover_aligned_and_abort();
        assert_eq!(
            merge_observed_announcement(aligned.clone(), abort.clone()).unwrap(),
            abort
        );
        for direct in [Phase::Commit, Phase::Abort] {
            assert_eq!(
                merge_observed_announcement(
                    BarrierAnnouncement {
                        phase: direct,
                        ..aligned.clone()
                    },
                    abort.clone(),
                )
                .unwrap(),
                abort,
                "the durable terminal must override every exact direct hint"
            );
        }
        assert_eq!(
            merge_observed_announcement(abort.clone(), aligned).unwrap(),
            abort,
            "a delivered successor terminal must beat the predecessor's durable reversible phase"
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
                flags: crate::checkpoint::flags::FULL_SNAPSHOT,
                ..base
            },
        )
        .is_err());
    }

    /// The gRPC-vs-gossip merge in `observe`: a manually seeded gossip-only `Prepare` for a newer
    /// attempt supersedes an older gRPC value, while lagging gossip for the same exact attempt
    /// never masks terminal progress.
    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn observe_merges_grpc_and_gossip_by_epoch() {
        let leader_kv = kv(NodeId(1));
        let follower_kv = kv(NodeId(2));
        let leader_coord = BarrierCoordinator::new(leader_kv.clone());
        let follower_coord = BarrierCoordinator::new(follower_kv.clone());

        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let leader_addr = leader_coord.start_server(addr, None).await.unwrap();
        let bound_addr = follower_coord.start_server(addr, None).await.unwrap();
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
            if let Some(ann) = follower_coord.observe_hint(NodeId(1)).await.unwrap() {
                if ann.phase == Phase::Abort {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        // This uncertified local-mode Prepare is seeded in gossip KV only and must win the merge
        // over the stale watch value.
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
        let got = follower_coord
            .observe_hint(NodeId(1))
            .await
            .unwrap()
            .unwrap();
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
        let got = follower_coord
            .observe_hint(NodeId(1))
            .await
            .unwrap()
            .unwrap();
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
        let got = coord.observe_hint(NodeId(1)).await.unwrap().unwrap();
        assert_eq!(got.epoch, 5);
        assert_eq!(got.checkpoint_id, 42);
    }

    #[tokio::test]
    async fn observe_returns_none_when_leader_silent() {
        let k = kv(NodeId(1));
        let coord = BarrierCoordinator::new(k);
        assert!(coord.observe_hint(NodeId(1)).await.unwrap().is_none());
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

    fn failover_aligned_and_abort() -> (BarrierAnnouncement, BarrierAnnouncement) {
        let aligned = certified_announcement(
            test_fence(9, &[1, 2], &[(1, 11), (2, 22)]),
            crate::cluster::control::LeaderProof {
                owner: crate::checkpoint::LeaderProofOwner {
                    node_id: 2,
                    boot_id: uuid::Uuid::from_u128(22),
                    process_term: 3,
                },
                fencing_token: 7,
            },
            Phase::Aligned,
        );
        let abort = certified_announcement(
            test_fence(10, &[1], &[(1, 11)]),
            crate::cluster::control::LeaderProof {
                fencing_token: 8,
                ..test_leader_proof()
            },
            Phase::Abort,
        );
        (aligned, abort)
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
                certified_announcement(fence.clone(), proof.clone(), Phase::Abort),
                certified_announcement(
                    test_fence(10, &[1, 2], &[(1, 11), (2, 22)]),
                    proof.clone(),
                    Phase::Abort,
                ),
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

    #[tokio::test]
    async fn max_announced_accepts_successor_settlement_and_later_attempt() {
        let (aligned, abort) = failover_aligned_and_abort();
        let history = kv(NodeId(1));
        history.seed(
            NodeId(1),
            ANNOUNCEMENT_KEY,
            serde_json::to_string(&aligned).unwrap(),
        );
        history.seed(
            NodeId(2),
            ANNOUNCEMENT_KEY,
            serde_json::to_string(&abort).unwrap(),
        );
        let coordinator = BarrierCoordinator::new(history.clone());
        assert_eq!(
            coordinator.max_announced().await.unwrap(),
            Some(CheckpointAttempt::new(5, 50))
        );

        history.seed(NodeId(3), ANNOUNCEMENT_KEY, announcement_json(6, 60));
        assert_eq!(
            coordinator.max_announced().await.unwrap(),
            Some(CheckpointAttempt::new(6, 60))
        );
    }

    #[test]
    fn scanned_successor_terminal_cannot_hide_reversible_equivocation() {
        let (aligned, abort) = failover_aligned_and_abort();
        let conflicting = BarrierAnnouncement {
            phase: Phase::Prepare,
            ..abort.clone()
        };
        let records = [aligned, conflicting, abort];

        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            let history = order
                .into_iter()
                .map(|index| records[index].clone())
                .collect();
            assert!(validate_scanned_announcements(history).is_err());
        }
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
