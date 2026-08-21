//! Barrier identities, canonical announcement history, acknowledgements, and outcomes.

use super::{CheckpointAttempt, CheckpointWatermark, Deserialize, NodeId, Serialize};
#[cfg(feature = "cluster")]
use super::{Duration, BARRIER_ENDPOINT_VERSION, MAX_BARRIER_ENDPOINT_BYTES};

/// Process identity attached to one control-plane endpoint. The durable process lease remains the
/// authority; this value prevents a stable node id from resolving to a different process boot.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BarrierProcessIdentity {
    pub(super) node_id: u64,
    pub(super) boot_incarnation: uuid::Uuid,
    pub(super) process_term: u64,
}

#[cfg(feature = "cluster")]
impl BarrierProcessIdentity {
    pub(super) fn from_process_lease(lease: &super::super::ProcessLease) -> Result<Self, String> {
        lease
            .validate(lease.node)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            node_id: lease.node.0,
            boot_incarnation: lease.owner,
            process_term: lease.term,
        })
    }

    pub(super) const fn is_canonical(self) -> bool {
        self.node_id != 0 && !self.boot_incarnation.is_nil() && self.process_term != 0
    }

    pub(super) fn matches_participant(
        self,
        participant: &crate::checkpoint::CheckpointParticipant,
    ) -> bool {
        self.node_id == participant.node_id && self.boot_incarnation == participant.boot_incarnation
    }
}

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BarrierEndpointRecord {
    version: u8,
    address: String,
    process: BarrierProcessIdentity,
}

#[cfg(feature = "cluster")]
impl BarrierEndpointRecord {
    pub(super) fn new(address: String, process: BarrierProcessIdentity) -> Result<Self, String> {
        let record = Self {
            version: BARRIER_ENDPOINT_VERSION,
            address,
            process,
        };
        record.validate()?;
        Ok(record)
    }

    pub(super) fn validate(&self) -> Result<(), String> {
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

    pub(super) fn encode(&self) -> Result<String, String> {
        let encoded = serde_json::to_string(self).map_err(|error| error.to_string())?;
        if encoded.len() > MAX_BARRIER_ENDPOINT_BYTES {
            return Err("cluster control endpoint advertisement is oversized".into());
        }
        Ok(encoded)
    }
}

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy)]
pub(super) struct ExpectedBarrierProcess {
    node_id: u64,
    boot_incarnation: uuid::Uuid,
    process_term: Option<u64>,
}

#[cfg(feature = "cluster")]
impl ExpectedBarrierProcess {
    pub(super) const fn participant(node_id: u64, boot_incarnation: uuid::Uuid) -> Self {
        Self {
            node_id,
            boot_incarnation,
            process_term: None,
        }
    }

    pub(super) const fn exact(process: &crate::checkpoint::LeaderProofOwner) -> Self {
        Self {
            node_id: process.node_id,
            boot_incarnation: process.boot_id,
            process_term: Some(process.process_term),
        }
    }

    pub(super) fn matches(self, actual: BarrierProcessIdentity) -> bool {
        self.node_id == actual.node_id
            && self.boot_incarnation == actual.boot_incarnation
            && self
                .process_term
                .is_none_or(|term| term == actual.process_term)
    }
}

#[cfg(feature = "cluster")]
pub(super) fn decode_barrier_endpoint(
    raw: &str,
) -> Result<(String, Option<BarrierProcessIdentity>), String> {
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
pub(super) fn encode_barrier_endpoint(
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
pub(super) const PHASE_RPC_TIMEOUT: Duration = Duration::from_secs(3);

#[cfg(feature = "cluster")]
pub(super) const PREPARE_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(10);

#[cfg(feature = "cluster")]
pub(super) const PREPARE_RETRY_MAX_BACKOFF: Duration = Duration::from_millis(250);

/// Barrier phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    /// Align the shuffle, capture state locally, and transfer the immutable cut to a supervised
    /// tail before acknowledging capture. Durable preparation continues in that tail.
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

pub(super) const fn is_terminal_phase(phase: Phase) -> bool {
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
    pub assignment_fence: Option<super::super::CheckpointAssignmentFence>,
    /// Exact durable leader term that issued this announcement. Clustered reversible phases
    /// (`Prepare` and `Aligned`) are rejected unless this proof is present and still live.
    /// Terminal notifications carry it only for diagnostics; their authority comes from the
    /// immutable durable checkpoint outcome.
    pub leader_proof: Option<super::super::LeaderProof>,
    /// Phase this announcement signals.
    pub phase: Phase,
    /// Checkpoint behavior flags from [`crate::checkpoint::flags`].
    pub flags: u64,
}

pub(super) fn announcement_attempt(ann: &BarrierAnnouncement) -> CheckpointAttempt {
    CheckpointAttempt::new(ann.epoch, ann.checkpoint_id)
}

pub(super) fn validate_announcement_attempt(ann: &BarrierAnnouncement) -> Result<(), String> {
    if announcement_attempt(ann).is_canonical() {
        Ok(())
    } else {
        Err(format!(
            "barrier announcement must use one nonzero canonical checkpoint ID; received epoch {} and checkpoint ID {}",
            ann.epoch, ann.checkpoint_id
        ))
    }
}

pub(super) fn validate_ack_attempt(ack: &BarrierAck) -> Result<(), String> {
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
pub(super) fn validate_wire_checkpoint_attempt(
    epoch: u64,
    checkpoint_id: u64,
) -> Result<(), tonic::Status> {
    if CheckpointAttempt::new(epoch, checkpoint_id).is_canonical() {
        Ok(())
    } else {
        Err(tonic::Status::invalid_argument(
            "Barrier request must use one nonzero canonical checkpoint ID",
        ))
    }
}

pub(super) fn same_announcement_identity(
    left: &BarrierAnnouncement,
    right: &BarrierAnnouncement,
) -> bool {
    announcement_attempt(left) == announcement_attempt(right)
        && left.assignment_fence == right.assignment_fence
        && left.leader_proof == right.leader_proof
        && left.flags == right.flags
}

/// Merge one exact attempt from a single history. A successor may settle a reversible phase under
/// a new assignment and leader proof, but reversible equivocation and opposing decisions fail
/// closed.
pub(super) fn merge_history_exact(
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
pub(super) fn merge_direct_announcement(
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
pub(super) fn merge_observed_announcement(
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
pub(super) fn merge_scanned_announcement(
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

pub(super) fn validate_publication_order(
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

pub(super) fn validate_scanned_announcements(
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

/// Follower checkpoint-prepare disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BarrierAckDisposition {
    /// Legacy durable-prepare acknowledgement. It is deliberately not accepted as a capture
    /// acknowledgement without an explicit mixed-version capability negotiation.
    Prepared,
    /// Legacy durable-prepare acknowledgement retaining handoff replay work.
    PreparedWithReplay,
    /// Local checkpoint preparation failed and requires normal failure handling.
    Failed,
    /// Local alignment and capture completed, the sink epoch is sealed, and a supervised tail
    /// owns the exact immutable cut. Durable preparation may still be running.
    Captured,
    /// Capture ownership completed and the captured cut retains handoff replay work.
    CapturedWithReplay,
}

impl BarrierAckDisposition {
    pub(super) const fn precedence(self) -> u8 {
        match self {
            Self::Prepared => 0,
            Self::PreparedWithReplay => 1,
            Self::Captured => 2,
            Self::CapturedWithReplay => 3,
            Self::Failed => 4,
        }
    }
}

/// Follower acknowledgement for one exact announcement identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarrierAck {
    /// Canonical checkpoint ID retained in the wire field named `epoch`.
    pub epoch: u64,
    /// The same nonzero coordinator-assigned checkpoint ID being acknowledged.
    pub checkpoint_id: u64,
    /// SHA-256 binding of the announcement's assignment certificate.
    pub assignment_digest: Option<[u8; 32]>,
    /// Exact behavior flags echoed from the announcement.
    pub flags: u64,
    /// Typed local capture/prepare result.
    pub disposition: BarrierAckDisposition,
    /// Free-text reason; populated when preparation fails.
    pub error: Option<String>,
    /// Follower event-time state at ack time. Uninitialized inputs block advancement; only
    /// explicitly idle inputs are excluded from the active cluster minimum.
    pub watermark: CheckpointWatermark,
}

/// Outcome of `wait_for_quorum`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuorumOutcome {
    /// All expected peers prepared successfully.
    Reached {
        /// Peers that acked successfully.
        acks: Vec<NodeId>,
        /// Safe aggregate across all required followers.
        follower_watermark: CheckpointWatermark,
        /// At least one follower retained replay in the prepared handoff cut.
        handoff_replay_pending: bool,
    },
    /// Deadline expired with at least one peer silent.
    TimedOut {
        /// Peers that did ack.
        got: Vec<NodeId>,
        /// Peers that didn't.
        missing: Vec<NodeId>,
    },
    /// At least one peer reported a fatal prepare failure.
    Failed {
        /// `(peer, error_message)` for every failed ack.
        failures: Vec<(NodeId, String)>,
    },
}
