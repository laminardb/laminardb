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

use crate::checkpoint::CheckpointAttempt;
use crate::checkpoint::CheckpointWatermark;
use crate::cluster::discovery::NodeId;
#[cfg(feature = "cluster")]
use crate::cluster::discovery::{NodeInfo, NodeState};
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
    /// (sink pre-commit, manifest, uploads) completes before the ack.
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
    /// Monotonic checkpoint ID retained in the wire field named `epoch`.
    pub epoch: u64,
    /// The same nonzero coordinator-assigned checkpoint ID.
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
}

fn announcement_attempt(ann: &BarrierAnnouncement) -> CheckpointAttempt {
    CheckpointAttempt::new(ann.epoch, ann.checkpoint_id)
}

fn validate_announcement_attempt(ann: &BarrierAnnouncement) -> Result<(), String> {
    if announcement_attempt(ann).is_canonical() {
        Ok(())
    } else {
        Err(format!(
            "barrier announcement must use one nonzero canonical checkpoint ID; received epoch {} and checkpoint ID {}",
            ann.epoch, ann.checkpoint_id
        ))
    }
}

fn validate_ack_attempt(ack: &BarrierAck) -> Result<(), String> {
    if CheckpointAttempt::new(ack.epoch, ack.checkpoint_id).is_canonical() {
        Ok(())
    } else {
        Err(format!(
            "barrier acknowledgement must use one nonzero canonical checkpoint ID; received epoch {} and checkpoint ID {}",
            ack.epoch, ack.checkpoint_id
        ))
    }
}

#[cfg(feature = "cluster")]
fn validate_wire_checkpoint_attempt(epoch: u64, checkpoint_id: u64) -> Result<(), tonic::Status> {
    if CheckpointAttempt::new(epoch, checkpoint_id).is_canonical() {
        Ok(())
    } else {
        Err(tonic::Status::invalid_argument(
            "Barrier request must use one nonzero canonical checkpoint ID",
        ))
    }
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
    if current.phase == incoming.phase && current != incoming {
        return Err(format!(
            "conflicting {history} {:?} payloads for exact attempt ({}, {})",
            current.phase, current.epoch, current.checkpoint_id
        ));
    }
    if is_terminal_phase(current.phase) && is_terminal_phase(incoming.phase) {
        if current.phase != incoming.phase {
            return Err(format!(
                "conflicting {history} terminal phases for exact attempt ({}, {})",
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
    validate_announcement_attempt(&current)?;
    validate_announcement_attempt(&incoming)?;
    match incoming.checkpoint_id.cmp(&current.checkpoint_id) {
        std::cmp::Ordering::Greater => Ok(incoming),
        std::cmp::Ordering::Less => Ok(current),
        std::cmp::Ordering::Equal => merge_history_exact(current, incoming, "direct"),
    }
}

/// Merge the low-latency direct value with the durable leader announcement.
/// At the same exact attempt a terminal KV value is the decision authority;
/// otherwise phase progress is monotonic while gossip catches up.
fn merge_observed_announcement(
    grpc: BarrierAnnouncement,
    durable: BarrierAnnouncement,
) -> Result<BarrierAnnouncement, String> {
    validate_announcement_attempt(&grpc)?;
    validate_announcement_attempt(&durable)?;
    match durable.checkpoint_id.cmp(&grpc.checkpoint_id) {
        std::cmp::Ordering::Greater => Ok(durable),
        std::cmp::Ordering::Less => Ok(grpc),
        std::cmp::Ordering::Equal => {
            if grpc.phase == durable.phase && !is_terminal_phase(grpc.phase) && grpc != durable {
                Err(format!(
                    "conflicting direct and durable {:?} payloads for exact attempt ({}, {})",
                    grpc.phase, grpc.epoch, grpc.checkpoint_id
                ))
            } else if is_terminal_phase(durable.phase) {
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
    validate_announcement_attempt(&current)?;
    validate_announcement_attempt(&incoming)?;
    match incoming.checkpoint_id.cmp(&current.checkpoint_id) {
        std::cmp::Ordering::Greater => Ok(incoming),
        std::cmp::Ordering::Less => Ok(current),
        std::cmp::Ordering::Equal => merge_history_exact(current, incoming, "durable"),
    }
}

fn validate_publication_order(
    current: &BarrierAnnouncement,
    incoming: &BarrierAnnouncement,
) -> Result<(), String> {
    validate_announcement_attempt(current)?;
    validate_announcement_attempt(incoming)?;
    match incoming.checkpoint_id.cmp(&current.checkpoint_id) {
        std::cmp::Ordering::Greater => Ok(()),
        std::cmp::Ordering::Less => Err(format!(
            "stale barrier publication ({}, {}) cannot replace newer admitted attempt ({}, {})",
            incoming.epoch, incoming.checkpoint_id, current.epoch, current.checkpoint_id
        )),
        std::cmp::Ordering::Equal if current == incoming => Ok(()),
        std::cmp::Ordering::Equal => {
            let merged =
                merge_history_exact(current.clone(), incoming.clone(), "local publication")?;
            if merged == *incoming {
                Ok(())
            } else {
                Err(format!(
                    "barrier publication cannot regress exact attempt ({}, {}) from {:?} to {:?}",
                    current.epoch, current.checkpoint_id, current.phase, incoming.phase
                ))
            }
        }
    }
}

fn validate_scanned_announcements(
    mut announcements: Vec<BarrierAnnouncement>,
) -> Result<Option<BarrierAnnouncement>, String> {
    for announcement in &announcements {
        validate_announcement_attempt(announcement)?;
    }
    // Group exact attempts and audit every reversible certificate before a terminal can absorb it.
    // Canonical attempt IDs are the only ordering authority.
    announcements.sort_unstable_by_key(|announcement| {
        (
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
    /// Canonical checkpoint ID retained in the wire field named `epoch`.
    pub epoch: u64,
    /// The same nonzero coordinator-assigned checkpoint ID being acknowledged.
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
    /// Local half of the same ordered announcement stream used by the gRPC server. Cluster
    /// leaders use it to observe their own reversible Aligned notification without polling the
    /// durable fallback.
    incoming_tx: crossfire::MAsyncTx<BarrierFlavor>,
    merge_error: Arc<parking_lot::Mutex<Option<String>>>,
    prepare_acks: Arc<parking_lot::Mutex<PrepareAckState>>,
    /// The one clustered Prepare admitted after durable publication. It progresses from pending
    /// fan-out through claimed quorum collection to an exact quorum-ready Aligned admission.
    prepare_fanout: parking_lot::Mutex<Option<PrepareFanoutState>>,
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
    if !CheckpointAttempt::new(ack.epoch, ack.checkpoint_id).is_canonical() {
        return Err("Barrier phase acknowledgement has a non-canonical checkpoint ID".into());
    }
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
        validate_wire_checkpoint_attempt(req.epoch, req.checkpoint_id)?;
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
        };

        self.enqueue_while_process_live(ann).await?;

        let ack = self.wait_for_prepare_ack(rx).await?;
        if ack.epoch != attempt.epoch
            || ack.checkpoint_id != attempt.checkpoint_id
            || ack.assignment_digest != assignment_digest
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
        validate_wire_checkpoint_attempt(req.epoch, req.checkpoint_id)?;
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
        validate_wire_checkpoint_attempt(req.epoch, req.checkpoint_id)?;
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
        validate_wire_checkpoint_attempt(req.epoch, req.checkpoint_id)?;
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

/// Deliver a non-Prepare phase to remote participants over the low-latency notification path.
#[cfg(feature = "cluster")]
async fn send_phase_notifications(
    state: &GrpcState,
    kv: &Arc<dyn ClusterKv>,
    ann: &BarrierAnnouncement,
    expected: Vec<NodeId>,
) -> Vec<Result<(), String>> {
    let deadline = tokio::time::Instant::now() + PHASE_RPC_TIMEOUT;
    futures::future::join_all(expected.into_iter().map(|peer| {
        send_phase_rpc(
            peer,
            Arc::clone(&state.clients),
            Arc::clone(kv),
            ann.clone(),
            deadline,
        )
    }))
    .await
}

#[cfg(feature = "cluster")]
fn send_local_phase_notification(
    state: &GrpcState,
    ann: &BarrierAnnouncement,
    process_lease: &super::LeaseDeadline,
) -> Result<(), String> {
    if !process_lease.is_live() {
        return Err("local process lease expired before barrier delivery".into());
    }
    match state.incoming_tx.try_send(ann.clone()) {
        Ok(()) => {}
        Err(crossfire::TrySendError::Full(_)) => {
            return Err("local barrier announcement relay is full".into());
        }
        Err(crossfire::TrySendError::Disconnected(_)) => {
            return Err("local barrier announcement relay is closed".into());
        }
    }
    if !process_lease.is_live() {
        return Err("local process lease expired during barrier delivery".into());
    }
    Ok(())
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
    QuorumReached(BarrierAnnouncement),
}

#[cfg(feature = "cluster")]
impl PrepareFanoutState {
    const fn announcement(&self) -> &BarrierAnnouncement {
        match self {
            Self::Pending(batch) => &batch.announcement,
            Self::Claimed(announcement) | Self::QuorumReached(announcement) => announcement,
        }
    }
}

#[cfg(feature = "cluster")]
fn clustered_prepare_roster(prepare: &BarrierAnnouncement) -> Result<Option<Vec<NodeId>>, String> {
    if prepare.phase != Phase::Prepare {
        return Err("Prepare fan-out received a different barrier phase".into());
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
    let mut pending = state.prepare_fanout.lock();
    let Some(current) = pending.as_ref() else {
        return Ok(true);
    };
    validate_announcement_attempt(prepare)?;
    validate_announcement_attempt(current.announcement())?;
    match prepare
        .checkpoint_id
        .cmp(&current.announcement().checkpoint_id)
    {
        std::cmp::Ordering::Greater => {
            // Admission has already rejected attempt regression. Cancel the obsolete structured
            // fan-out before cancellable durable I/O so it cannot complete after being superseded.
            pending.take();
            Ok(true)
        }
        std::cmp::Ordering::Equal if current.announcement() == prepare => match current {
            PrepareFanoutState::Pending(_) => Ok(false),
            PrepareFanoutState::Claimed(_) => {
                Err("Prepare cannot be republished while its quorum is being collected".into())
            }
            PrepareFanoutState::QuorumReached(_) => {
                Err("Prepare cannot regress an exact quorum-ready checkpoint".into())
            }
        },
        std::cmp::Ordering::Less => {
            Err("stale Prepare cannot replace a newer in-flight fan-out".into())
        }
        std::cmp::Ordering::Equal => {
            Err("conflicting Prepare certificate cannot replace the in-flight fan-out".into())
        }
    }
}

#[cfg(feature = "cluster")]
fn mark_prepare_quorum_reached(
    state: &GrpcState,
    prepare: &BarrierAnnouncement,
) -> Result<(), String> {
    let mut fanout = state.prepare_fanout.lock();
    match fanout.take() {
        Some(PrepareFanoutState::Claimed(claimed)) if claimed == *prepare => {
            *fanout = Some(PrepareFanoutState::QuorumReached(claimed));
            Ok(())
        }
        Some(other) => {
            *fanout = Some(other);
            Err("Prepare quorum completion lost its exact claimed fan-out".into())
        }
        None => Err("Prepare quorum completion has no claimed fan-out".into()),
    }
}

#[cfg(feature = "cluster")]
fn require_aligned_quorum(state: &GrpcState, aligned: &BarrierAnnouncement) -> Result<(), String> {
    let fanout = state.prepare_fanout.lock();
    let Some(PrepareFanoutState::QuorumReached(prepare)) = fanout.as_ref() else {
        return Err("clustered Aligned requires a successful exact Prepare quorum".into());
    };
    if !same_announcement_identity(prepare, aligned) {
        return Err("clustered Aligned does not match the exact reached Prepare quorum".into());
    }
    Ok(())
}

#[cfg(feature = "cluster")]
fn retire_prepare_fanout(state: &GrpcState) {
    state.prepare_fanout.lock().take();
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

#[derive(Default)]
struct AnnouncementPublicationState {
    initialized: bool,
    latest: Option<BarrierAnnouncement>,
}

/// Cross-instance barrier coordination.
pub struct BarrierCoordinator {
    kv: Arc<dyn ClusterKv>,
    /// Serializes local publication across every runtime mode. The latest admitted value advances
    /// before cancellable I/O so an ambiguous write result cannot reopen an older phase.
    publication: tokio::sync::Mutex<AnnouncementPublicationState>,
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
            publication: tokio::sync::Mutex::new(AnnouncementPublicationState::default()),
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
        validate_announcement_attempt(ann)?;
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
    fn require_exact_local_leader_proof(&self, ann: &BarrierAnnouncement) -> Result<(), String> {
        let expected = ann
            .leader_proof
            .as_ref()
            .ok_or_else(|| "assignment-certified barrier has no exact leader proof".to_string())?;
        let provider = self
            .local_leader_proof
            .lock()
            .clone()
            .ok_or_else(|| "local leader proof provider is not installed".to_string())?;
        if provider().as_ref() != Some(expected) {
            return Err(
                "assignment-certified barrier sender no longer owns its exact leader proof".into(),
            );
        }
        Ok(())
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
        validate_announcement_attempt(announcement)?;
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
            incoming_tx,
            merge_error,
            prepare_acks,
            prepare_fanout: parking_lot::Mutex::new(None),
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
    /// Assignment-certified Prepare must use [`Self::announce_prepare`]. Assignment-certified
    /// reversible phases require a started leased barrier server, and Aligned requires the exact
    /// Prepare to have completed [`Self::wait_for_quorum`]. Other errors propagate validation,
    /// encoding, and publication failures.
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
        validate_announcement_attempt(ann)?;
        #[cfg(feature = "cluster")]
        {
            self.validate_reversible_announcement(ann).await?;
            let (prepare_roster, prepare_budget) = prepare_fanout_plan(ann, prepare_quorum_window)?;
            let grpc_opt = self.grpc.lock().clone();
            let process_bound = self.local_process.get().is_some();
            match (process_bound, ann.assignment_fence.is_some()) {
                (true, false) => {
                    return Err(format!(
                        "process-bound cluster {:?} requires an assignment certificate",
                        ann.phase
                    ));
                }
                (false, true) => {
                    return Err(format!(
                        "assignment-certified {:?} requires a process-bound leased endpoint",
                        ann.phase
                    ));
                }
                (true, true) | (false, false) => {}
            }
            if ann.assignment_fence.is_some()
                && matches!(ann.phase, Phase::Prepare | Phase::Aligned)
                && grpc_opt.is_none()
            {
                return Err(format!(
                    "assignment-certified {:?} requires a started leased barrier server",
                    ann.phase
                ));
            }
            let phase_roster = if let Some(state) = grpc_opt.as_ref() {
                let local_process = state.local_process.get().copied();
                if ann.phase == Phase::Prepare {
                    None
                } else {
                    clustered_phase_roster(ann, local_process)?
                }
            } else {
                None
            };
            let json = serde_json::to_string(ann).map_err(|e| e.to_string())?;
            let mut publication = self.publication.lock().await;
            if !publication.initialized {
                publication.latest = self.scan_latest_announcement().await?;
                publication.initialized = true;
            }
            if let Some(current) = publication.latest.as_ref() {
                validate_publication_order(current, ann)?;
            }

            if let Some(state) = grpc_opt {
                let certified_prepare = ann.phase == Phase::Prepare && prepare_roster.is_some();
                if certified_prepare {
                    self.require_live_bound_process_lease()?;
                    self.require_exact_local_leader_proof(ann)?;
                }
                let replace_prepare_fanout = if prepare_roster.is_some() {
                    // Reject stale/equivocating retries before they can overwrite the durable
                    // gossip slot. The publication lock keeps this check and durable mutation
                    // atomic with respect to every local phase transition.
                    preflight_prepare_fanout(&state, ann)?
                } else {
                    false
                };
                let certified_aligned = ann.phase == Phase::Aligned && phase_roster.is_some();
                if certified_aligned {
                    // Recheck authority after lock contention and require the exact Prepare quorum
                    // before releasing any participant. Aligned remains reversible: it cannot
                    // advance sources, commit sinks, authorize recovery, or collect old state.
                    self.require_live_bound_process_lease()?;
                    self.require_exact_local_leader_proof(ann)?;
                    require_aligned_quorum(&state, ann)?;
                    let process_lease = self
                        .process_lease_deadline
                        .get()
                        .cloned()
                        .ok_or_else(|| "process lease deadline is not installed".to_string())?;
                    // Admission advances before cancellable I/O. An ambiguous durable error must
                    // not reopen Prepare or allow a terminal attempt to regress locally.
                    publication.latest = Some(ann.clone());

                    let expected =
                        phase_roster.expect("certified Aligned has an assignment roster");
                    let remote = send_phase_notifications(&state, &self.kv, ann, expected);
                    let local_result = send_local_phase_notification(&state, ann, &process_lease);
                    let durable = self.kv.write_checked(ANNOUNCEMENT_KEY, json);
                    tokio::pin!(remote);
                    tokio::pin!(durable);
                    let mut completed_remote = None;
                    let durable_result = tokio::select! {
                        result = &mut durable => result,
                        results = &mut remote => {
                            completed_remote = Some(results);
                            durable.await
                        }
                    };
                    let authority_result = self
                        .require_live_bound_process_lease()
                        .and_then(|()| self.require_exact_local_leader_proof(ann));
                    drop(publication);
                    authority_result?;

                    let direct_results = match completed_remote {
                        Some(results) => results,
                        None => remote.await,
                    };
                    if let Err(error) = local_result {
                        tracing::warn!(
                            epoch = ann.epoch,
                            error = %error,
                            "local aligned announcement delivery failed; resume falls back to durable observation or Commit"
                        );
                    }
                    for error in direct_results.into_iter().filter_map(Result::err) {
                        tracing::warn!(
                            epoch = ann.epoch,
                            error = %error,
                            "aligned announcement delivery failed; resume falls back to durable observation or Commit"
                        );
                    }
                    self.require_live_bound_process_lease()?;
                    self.require_exact_local_leader_proof(ann)?;
                    durable_result
                        .map_err(|error| format!("publish barrier announcement: {error}"))?;
                    return Ok(());
                }
                // Prepare, terminal, and assignment-less phases remain durable-first. Recovery
                // and irreversible decisions never depend on best-effort notification delivery.
                if is_terminal_phase(ann.phase) {
                    // Cancel superseded transport work at admission, before a cancellable durable
                    // write. Publication ordering retains the terminal identity if I/O is
                    // ambiguous, so the old Prepare must not remain actionable.
                    retire_prepare_fanout(&state);
                }
                publication.latest = Some(ann.clone());
                self.kv
                    .write_checked(ANNOUNCEMENT_KEY, json)
                    .await
                    .map_err(|error| format!("publish barrier announcement: {error}"))?;
                if ann.phase == Phase::Prepare {
                    if certified_prepare {
                        self.require_live_bound_process_lease()?;
                        self.require_exact_local_leader_proof(ann)?;
                    }
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
                    drop(publication);
                } else {
                    drop(publication);
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

                    let results = send_phase_notifications(&state, &self.kv, ann, expected).await;
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

            publication.latest = Some(ann.clone());
            self.kv
                .write_checked(ANNOUNCEMENT_KEY, json)
                .await
                .map_err(|error| format!("publish barrier announcement: {error}"))?;
            Ok(())
        }

        #[cfg(not(feature = "cluster"))]
        {
            let _ = prepare_quorum_window;
            let json = serde_json::to_string(ann).map_err(|e| e.to_string())?;
            let mut publication = self.publication.lock().await;
            if !publication.initialized {
                publication.latest = self.scan_latest_announcement().await?;
                publication.initialized = true;
            }
            if let Some(current) = publication.latest.as_ref() {
                validate_publication_order(current, ann)?;
            }
            publication.latest = Some(ann.clone());
            self.kv
                .write_checked(ANNOUNCEMENT_KEY, json)
                .await
                .map_err(|error| format!("publish barrier announcement: {error}"))?;
            Ok(())
        }
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
    /// Observation is non-destructive, and direct plus gossip histories must remain related by
    /// canonical checkpoint ID. Terminal durable KV values remain the decision authority.
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
        if let Some(announcement) = observed.as_ref() {
            validate_announcement_attempt(announcement)?;
        }
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

    async fn scan_latest_announcement(&self) -> Result<Option<BarrierAnnouncement>, String> {
        let mut announcements = Vec::new();
        for (node, json) in self.kv.scan_checked(ANNOUNCEMENT_KEY).await? {
            let announcement: BarrierAnnouncement = serde_json::from_str(&json)
                .map_err(|error| format!("malformed barrier announcement from {node}: {error}"))?;
            announcements.push(announcement);
        }
        validate_scanned_announcements(announcements)
    }

    /// Follower-side ack.
    ///
    /// # Errors
    /// Returns a string on JSON encode failure.
    pub async fn ack(&self, ack: &BarrierAck) -> Result<(), String> {
        validate_ack_attempt(ack)?;
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
        if let Err(error) = validate_announcement_attempt(prepare) {
            return QuorumOutcome::Failed {
                failures: vec![(
                    expected.first().copied().unwrap_or(NodeId::UNASSIGNED),
                    error,
                )],
            };
        }
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
                        Some(
                            state @ (PrepareFanoutState::Claimed(_)
                            | PrepareFanoutState::QuorumReached(_)),
                        ) => {
                            let exact = state.announcement() == prepare;
                            *pending = Some(state);
                            return QuorumOutcome::Failed {
                                failures: vec![(
                                    expected_roster
                                        .first()
                                        .copied()
                                        .unwrap_or(NodeId::UNASSIGNED),
                                    if exact {
                                        "clustered Prepare fan-out was already claimed or completed"
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

                if prepare.assignment_fence.is_some() {
                    if let Err(error) = mark_prepare_quorum_reached(&state, prepare) {
                        return QuorumOutcome::Failed {
                            failures: vec![(
                                expected_roster
                                    .first()
                                    .copied()
                                    .unwrap_or(NodeId::UNASSIGNED),
                                error,
                            )],
                        };
                    }
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
mod tests;
