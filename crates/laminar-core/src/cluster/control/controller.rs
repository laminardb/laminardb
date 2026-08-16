//! Facade over `ClusterKv` + `BarrierCoordinator` + membership watch.
//! `None` on `CheckpointCoordinator` means single-instance mode.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use sha2::{Digest as _, Sha256};
use tokio::sync::watch;
use uuid::Uuid;

use super::barrier::{
    BarrierAck, BarrierAnnouncement, BarrierCoordinator, ClusterKv, QuorumOutcome,
};
use super::leader::leader_of;
use super::snapshot::AssignmentSnapshotStore;
use crate::checkpoint::{
    AssignmentDrainId, AssignmentDrainTransition, CheckpointAssignmentAdoption,
    CheckpointAssignmentFence, CheckpointParticipant, LeaderProof, MAX_CHECKPOINT_PARTICIPANTS,
};
use crate::cluster::discovery::{assignable_node_ids, NodeId, NodeInfo, NodeState};
use crate::state::Locality;

const RECOVERY_INCARNATION_KEY: &str = "control:recovery-incarnation";
const ADOPTED_ASSIGNMENT_KEY: &str = "control:adopted-assignment";
const DRAIN_ACK_KEY: &str = "control:drain-ack";
const DRAIN_ACK_PROTOCOL_VERSION: u16 = 1;
const RELEASE_READY_ACK_KEY: &str = "control:recovery-release-ready";
const RELEASE_READY_PROTOCOL_VERSION: u16 = 2;
const RECOVERY_STOPPED_REPORT_KEY: &str = "control:recovery-stopped";
/// Current wire version for a recovery stopped report.
const RECOVERY_STOPPED_REPORT_PROTOCOL_VERSION: u16 = 4;
/// Maximum encoded size of one recovery stopped report.
const MAX_RECOVERY_STOPPED_REPORT_BYTES: usize = 1_024;
/// Shared ceiling for mutable recovery intents and immutable release terminals.
pub(super) const MAX_RECOVERY_ANNOUNCEMENT_BYTES: usize = 256 * 1_024;
const RECOVERY_CONTROL_IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_ADOPTED_ASSIGNMENT_BYTES: usize = 1_024;
const MAX_DRAIN_ACK_BYTES: usize = 1_024;
const MAX_RELEASE_READY_ACK_BYTES: usize = 1_024;
const CONTROL_ROSTER_IO_CONCURRENCY: usize = 32;
const PENDING_RELEASE_FAULT_AUDIT_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(feature = "cluster")]
const CHECKPOINT_PREPARE_OBSERVATION_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(feature = "cluster")]
struct LeaderLeaseGate {
    lease: watch::Receiver<Option<super::LeaderLease>>,
    owner: super::LeaderLeaseOwner,
    deadline: watch::Receiver<Arc<super::LeaseDeadline>>,
}

/// One authority-validated clustered `Prepare` and its local assignment disposition.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointPrepareObservation {
    /// The announced assignment exactly matches the locally installed certified assignment.
    AssignmentReady(BarrierAnnouncement),
    /// Authority is valid, but the assignment must be rejected before barrier injection.
    AssignmentRejected {
        /// Leader announcement to identify the negative acknowledgement.
        announcement: BarrierAnnouncement,
        /// Local certification failure sent back to the leader.
        error: String,
    },
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

/// Frozen recovery round, assignment certificate, and durable driver term.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryRound {
    /// Unique round identity.
    pub id: RecoveryRoundId,
    /// Exact durable leader term that created and may advance this round.
    pub leader_proof: LeaderProof,
    /// Exact owner-complete assignment cut from which the quorum was frozen.
    pub assignment_fence: CheckpointAssignmentFence,
    /// Exact non-owner processes whose fault evidence must be inventoried before recovery starts.
    ///
    /// These participants stop and report Prepared checkpoint evidence, but do not join the
    /// assignment-owner restore or release quorum.
    pub evidence_participants: Vec<CheckpointParticipant>,
    /// Exact shared-authority fault inventory revision frozen for this round.
    fault_revision: u64,
    /// Canonical nonzero fault reports covered by this round's terminal `Release`.
    pub faults: Vec<RecoveryFault>,
}

/// One durable fault report covered by a coordinated recovery round.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryFault {
    /// Stable node slot that published the report.
    pub reporter: NodeId,
    /// Nonzero globally monotonic authority sequence observed by the recovery driver.
    pub sequence: u64,
    /// Whether automatic recovery may consume this fault.
    #[serde(
        default,
        skip_serializing_if = "RecoveryFaultDisposition::is_recoverable"
    )]
    pub disposition: RecoveryFaultDisposition,
}

/// Durable recovery policy attached to one fault authority slot.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryFaultDisposition {
    /// A coordinated rewind may consume the fault after a committed Release.
    #[default]
    Recoverable,
    /// The failure is deterministic and must remain fenced until an operator replaces the
    /// cluster authority namespace. Automatic recovery must never consume or downgrade it.
    Terminal,
}

impl RecoveryFaultDisposition {
    /// Whether the legacy automatic-recovery policy is in effect.
    #[must_use]
    pub const fn is_recoverable(&self) -> bool {
        matches!(self, Self::Recoverable)
    }
}

impl RecoveryFault {
    /// Whether this fault permanently disables automatic recovery.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self.disposition, RecoveryFaultDisposition::Terminal)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryFaultPublisher {
    pub(crate) participant: CheckpointParticipant,
    pub(crate) process_term: u64,
}

/// Opaque process-local identity for one recoverable failure notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryFaultRequest {
    sequence: std::num::NonZeroU64,
}

/// Durable disposition of one process-local recovery-fault request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryFaultReportOutcome {
    /// The exact request is the active fault for this stable node.
    Active,
    /// The exact request was already atomically covered by a committed recovery Release.
    AlreadyCleared,
    /// A newer request from the same process already superseded this request.
    CoveredByNewerRequest,
    /// A durable terminal fault already owns this stable-node slot. The attempted request was not
    /// admitted and automatic recovery remains permanently fenced.
    TerminalFenceActive,
}

impl RecoveryFaultRequest {
    /// Process-local ordinal used to retain the request across bounded retries.
    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.sequence.get()
    }
}

impl RecoveryFaultPublisher {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.participant.node_id == 0
            || self.participant.boot_incarnation.is_nil()
            || self.process_term == 0
        {
            return Err("recovery fault publisher identity is not canonical".into());
        }
        Ok(())
    }
}

/// One atomically observed shared-authority fault inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryFaultInventory {
    pub(crate) revision: u64,
    pub(crate) faults: Vec<RecoveryFault>,
}

impl RecoveryFaultInventory {
    /// Authority sequence that admitted this exact active set.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Canonical active fault reports in stable-node order.
    #[must_use]
    pub fn faults(&self) -> &[RecoveryFault] {
        &self.faults
    }

    /// Whether automatic recovery is durably disabled by any active report.
    #[must_use]
    pub fn has_terminal_fault(&self) -> bool {
        self.faults.iter().copied().any(RecoveryFault::is_terminal)
    }
}

/// One coherent recovery-admission view from the shared leader authority.
/// This is evidence, not authority to open intake; callers must revalidate it with the audited
/// leader proof immediately before activation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryAdmissionSnapshot {
    pub(crate) committed_release: Option<RecoveryAnnouncement>,
    pub(crate) fault_inventory: RecoveryFaultInventory,
    pub(crate) authority_sequence: u64,
    pub(crate) release_head: Option<super::leader_lease::RecoveryReleaseLink>,
}

impl RecoveryAdmissionSnapshot {
    /// Latest committed recovery release in this authority view.
    #[must_use]
    pub fn committed_release(&self) -> Option<&RecoveryAnnouncement> {
        self.committed_release.as_ref()
    }

    /// Active recovery faults from the same authority view.
    #[must_use]
    pub const fn fault_inventory(&self) -> &RecoveryFaultInventory {
        &self.fault_inventory
    }
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
        evidence_participants: Vec<CheckpointParticipant>,
        fault_revision: u64,
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
            evidence_participants,
            fault_revision,
            faults,
        };
        round.validate()?;
        Ok(round)
    }

    /// Whether `node` belongs to the immutable assignment-owner quorum.
    #[must_use]
    pub fn contains_owner(&self, node: NodeId) -> bool {
        self.assignment_fence.contains(node.0)
    }

    /// Frozen assignment owners as runtime node identifiers.
    #[must_use]
    pub fn owners(&self) -> Vec<NodeId> {
        self.assignment_fence
            .participants
            .iter()
            .map(|participant| NodeId(participant.node_id))
            .collect()
    }

    /// Whether this round carries a fault that automatic recovery must not consume.
    #[must_use]
    pub fn has_terminal_fault(&self) -> bool {
        self.faults.iter().copied().any(RecoveryFault::is_terminal)
    }

    /// Frozen assignment-owner boot identity for `node`.
    #[must_use]
    pub(crate) fn owner_incarnation(&self, node: NodeId) -> Option<Uuid> {
        self.assignment_fence.participant_incarnation(node.0)
    }

    /// Whether `node` must stop and report evidence for this round.
    #[must_use]
    pub fn contains_stopped_participant(&self, node: NodeId) -> bool {
        self.contains_owner(node)
            || self
                .evidence_participants
                .binary_search_by_key(&node.0, |participant| participant.node_id)
                .is_ok()
    }

    /// Frozen stopped roster as sorted runtime node identifiers.
    #[must_use]
    pub fn stopped_participants(&self) -> Vec<NodeId> {
        self.stopped_roster()
            .into_iter()
            .map(|participant| NodeId(participant.node_id))
            .collect()
    }

    /// Frozen stopped-roster boot identity for `node`.
    #[must_use]
    pub(crate) fn stopped_participant_incarnation(&self, node: NodeId) -> Option<Uuid> {
        self.assignment_fence
            .participant_incarnation(node.0)
            .or_else(|| {
                self.evidence_participants
                    .binary_search_by_key(&node.0, |participant| participant.node_id)
                    .ok()
                    .map(|index| self.evidence_participants[index].boot_incarnation)
            })
    }

    fn stopped_roster(&self) -> Vec<CheckpointParticipant> {
        let mut roster = Vec::with_capacity(
            self.assignment_fence.participants.len() + self.evidence_participants.len(),
        );
        roster.extend(self.assignment_fence.participants.iter().copied());
        roster.extend(self.evidence_participants.iter().copied());
        roster.sort_unstable_by_key(|participant| participant.node_id);
        roster
    }

    /// Exact fault sequence this round covers for `node`.
    #[must_use]
    pub fn fault_sequence(&self, node: NodeId) -> Option<u64> {
        self.faults
            .binary_search_by_key(&node, |fault| fault.reporter)
            .ok()
            .map(|index| self.faults[index].sequence)
    }

    /// Exact shared-authority fault inventory revision frozen into this round.
    #[must_use]
    pub const fn fault_revision(&self) -> u64 {
        self.fault_revision
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
        if !self.contains_owner(self.id.driver) {
            return Err("recovery driver is absent from the frozen quorum".into());
        }
        if self.owner_incarnation(self.id.driver) != Some(self.leader_proof.owner.boot_id) {
            return Err("recovery leader proof is not bound to the frozen driver process".into());
        }
        if self.fault_revision == 0 {
            return Err("recovery fault revision must be nonzero".into());
        }
        if self.faults.is_empty()
            || self.faults.iter().any(|fault| {
                fault.reporter.is_unassigned()
                    || fault.sequence == 0
                    || fault.sequence > self.fault_revision
            })
            || self
                .faults
                .windows(2)
                .any(|pair| pair[0].reporter >= pair[1].reporter)
        {
            return Err("recovery fault set is not canonical".into());
        }
        if self
            .evidence_participants
            .iter()
            .any(|participant| participant.node_id == 0 || participant.boot_incarnation.is_nil())
            || self
                .evidence_participants
                .windows(2)
                .any(|pair| pair[0].node_id >= pair[1].node_id)
        {
            return Err("recovery evidence participant roster is not canonical".into());
        }
        if self.assignment_fence.participants.len() + self.evidence_participants.len()
            > MAX_CHECKPOINT_PARTICIPANTS
        {
            return Err(format!(
                "recovery stopped roster has {} participants; maximum is {MAX_CHECKPOINT_PARTICIPANTS}",
                self.assignment_fence.participants.len() + self.evidence_participants.len()
            ));
        }
        if self.evidence_participants.iter().any(|participant| {
            self.assignment_fence.contains(participant.node_id)
                || self
                    .faults
                    .binary_search_by_key(&NodeId(participant.node_id), |fault| fault.reporter)
                    .is_err()
        }) {
            return Err("recovery evidence participants must be non-owner fault reporters".into());
        }
        let largest_terminal = RecoveryAnnouncement {
            round: self.clone(),
            phase: RecoverPhase::ReleaseCommitted { epoch: u64::MAX },
        };
        let encoded = serde_json::to_vec(&largest_terminal)
            .map_err(|error| format!("could not encode recovery round: {error}"))?;
        validate_recovery_announcement_size(encoded.len())?;
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
#[serde(deny_unknown_fields)]
pub struct RecoveryAnnouncement {
    /// Frozen round identity and quorum.
    pub round: RecoveryRound,
    /// Current phase; `Start` is valid only after the identical `Prepare`.
    pub phase: RecoverPhase,
}

impl RecoveryAnnouncement {
    pub(crate) fn validate(&self) -> Result<(), String> {
        self.round.validate()?;
        if self.round.has_terminal_fault() && self.phase != RecoverPhase::Prepare {
            return Err("a terminal recovery fault may only be retained in Prepare".into());
        }
        let encoded = serde_json::to_vec(self)
            .map_err(|error| format!("could not encode recovery announcement: {error}"))?;
        validate_recovery_announcement_size(encoded.len())
    }
}

/// Failure to obtain a trustworthy local-process authority view.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LocalProcessAuthorityEvidenceError {
    /// Authority is temporarily unavailable because startup, lease, identity, or bounded storage
    /// evidence is incomplete or changed during the read. Every checked storage read failure is
    /// classified here, including backends that report a malformed outer envelope as an I/O
    /// failure rather than returning its logical value.
    #[error("local process authority evidence is unavailable: {0}")]
    Unavailable(String),
    /// Logical payload bytes successfully returned by checked storage are malformed,
    /// non-canonical, or contradict their current-process slot or same-version audited fence.
    #[error("local process authority evidence is invalid: {0}")]
    Invalid(String),
}

/// One non-durable identity sample for the exact local process generation.
///
/// This value names process authority; it does not prove assignment adoption or grant future
/// authority. Callers that expose it must sample a live identity around their in-memory read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalProcessAuthorityIdentity {
    /// Stable node slot and boot incarnation.
    pub participant: CheckpointParticipant,
    /// Stable-node process term bound to `participant`.
    pub process_term: u64,
}

impl LocalProcessAuthorityIdentity {
    /// Whether every process-identity field has its canonical production shape.
    #[must_use]
    pub fn is_canonical(self) -> bool {
        self.participant.node_id != 0
            && !self.participant.boot_incarnation.is_nil()
            && self.process_term != 0
    }
}

/// One bounded, current-process view of locally retained cluster-control evidence.
///
/// A successful read samples the lease as live before and after the durable point read; it is not
/// a future authority grant.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalProcessAuthorityEvidence {
    /// Stable node slot and boot incarnation sampled for this view.
    pub participant: CheckpointParticipant,
    /// Stable-node process term bound to `participant` by durable lease publication.
    pub process_term: u64,
    /// Exact report durably published by this boot and matched to the sampled locally audited
    /// assignment fence.
    pub adopted_assignment: CheckpointAssignmentAdoption,
}

/// Boot-bound acknowledgement that one process quiesced for a frozen recovery round.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryStoppedReport {
    /// Wire format version. Only [`RECOVERY_STOPPED_REPORT_PROTOCOL_VERSION`] is accepted.
    protocol_version: u16,
    /// Compact identity of the frozen recovery round for which the publisher stopped.
    round_id: RecoveryRoundId,
    /// Canonical SHA-256 digest of the exact full recovery round.
    round_sha256: String,
    /// Stable node slot and boot incarnation that published this report.
    publisher: CheckpointParticipant,
}

impl RecoveryStoppedReport {
    /// Construct a canonical stopped report.
    ///
    /// # Errors
    /// Returns an error when the round, publisher, or encoded size is invalid.
    pub fn new(round: &RecoveryRound, publisher: CheckpointParticipant) -> Result<Self, String> {
        let report = Self {
            protocol_version: RECOVERY_STOPPED_REPORT_PROTOCOL_VERSION,
            round_id: round.id,
            round_sha256: recovery_round_sha256(round)?,
            publisher,
        };
        report.validate(round)?;
        Ok(report)
    }

    /// Exact recovery-round identity bound into this report.
    #[must_use]
    pub const fn round_id(&self) -> RecoveryRoundId {
        self.round_id
    }

    /// Exact process that published this report.
    #[must_use]
    pub const fn publisher(&self) -> CheckpointParticipant {
        self.publisher
    }

    /// Validate all semantic and encoded-size invariants of this report.
    ///
    /// # Errors
    /// Returns an error describing the first non-canonical invariant.
    pub fn validate(&self, round: &RecoveryRound) -> Result<(), String> {
        self.validate_semantics(round)?;
        let encoded = serde_json::to_vec(self)
            .map_err(|error| format!("could not encode recovery stopped report: {error}"))?;
        validate_stopped_report_size(encoded.len())
    }

    fn validate_shape(&self) -> Result<(), String> {
        if self.protocol_version != RECOVERY_STOPPED_REPORT_PROTOCOL_VERSION {
            return Err(format!(
                "unsupported recovery stopped report version {}; expected {RECOVERY_STOPPED_REPORT_PROTOCOL_VERSION}",
                self.protocol_version
            ));
        }
        if self.round_id.generation == 0
            || self.round_id.nonce.is_nil()
            || self.round_id.driver.is_unassigned()
        {
            return Err("recovery stopped report round identity is not canonical".into());
        }
        if !is_sha256_hex(&self.round_sha256) {
            return Err("recovery stopped report round digest is not canonical".into());
        }
        if self.publisher.node_id == 0 || self.publisher.boot_incarnation.is_nil() {
            return Err("recovery stopped report publisher is not canonical".into());
        }
        Ok(())
    }

    fn validate_semantics(&self, round: &RecoveryRound) -> Result<(), String> {
        self.validate_shape()?;
        round.validate()?;
        if self.round_id != round.id || self.round_sha256 != recovery_round_sha256(round)? {
            return Err("recovery stopped report does not match the exact frozen round".into());
        }
        if round.stopped_participant_incarnation(NodeId(self.publisher.node_id))
            != Some(self.publisher.boot_incarnation)
        {
            return Err("recovery stopped report publisher does not match the frozen round".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryAnnouncementAck {
    announcement: RecoveryAnnouncement,
    incarnation: Uuid,
}

#[derive(serde::Serialize)]
struct RecoveryStoppedRoundDigestInput<'a> {
    protocol: &'static str,
    round: &'a RecoveryRound,
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
            protocol: "laminardb-recovery-release-v2",
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

    #[cfg(feature = "cluster")]
    fn from_process_authority(error: super::ProcessLeaseError) -> Self {
        match error {
            super::ProcessLeaseError::Io(reason) | super::ProcessLeaseError::Deadline(reason) => {
                Self::Uncertain(reason)
            }
            super::ProcessLeaseError::Invalid(reason) => Self::Conflict(reason),
            super::ProcessLeaseError::Json(error) => Self::Conflict(error.to_string()),
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

/// Held after this process validates its atomically settled recovery fault and until its local
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
    /// Serialises every local authority grant with terminal process fencing.
    process_authority_transition: parking_lot::Mutex<()>,
    /// Process-unique recovery identity, published before this node becomes Active.
    recovery_incarnation: Uuid,
    /// Stable-node process term bound to `recovery_incarnation`.
    recovery_process_term: AtomicU64,
    /// Per-process request identity; shared authority assigns the cluster-visible fault sequence.
    recovery_fault_request_sequence: AtomicU64,
    /// Recovery-safe cluster watermark installed after an immutable Commit outcome or from a
    /// validated committed checkpoint. `i64::MIN` = uninitialised.
    cluster_min_watermark: Arc<AtomicI64>,
    /// Source-keyed decision frontiers installed from the same immutable committed channel cut.
    /// A temporal operator consumes these instead of the pipeline-wide minimum so an unrelated
    /// slow source cannot regress restored source-specific progress.
    committed_source_watermarks: parking_lot::RwLock<Arc<rustc_hash::FxHashMap<String, i64>>>,
    /// Serialises monotonic live publication with the exact replacement performed while recovery
    /// owns the pipeline fence. Without this lock, a recovering process could replace a stale cut
    /// while an in-flight normal publication concurrently reinstalls it.
    committed_watermark_publication: parking_lot::Mutex<()>,
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
    /// Coalescing-safe local candidacy generation. Every observed loss advances the generation
    /// synchronously, so a rapid loss/reacquire cannot reuse a prior leader token.
    leader_candidacy: watch::Sender<super::LeaderCandidacy>,
    /// Last certified assignment roster. Retained across transient certificate suspension so an
    /// ownerless worker cannot displace an available data owner. If every certified owner is
    /// unavailable, a durably lease-fenced idle worker may lead placement repair only.
    leadership_participants: parking_lot::RwLock<Option<(u64, Vec<u64>)>>,
    /// Serialises this node's recovery and fault-slot conditional writes.
    recovery_writes: tokio::sync::Mutex<()>,
    /// Coalesces non-authorizing fault audits while a Release still lacks readiness. The complete
    /// path never consults this cache.
    pending_release_fault_audit:
        tokio::sync::Mutex<Option<(RecoveryReleaseId, tokio::time::Instant)>>,
    /// Exact process incarnations that missed a capture quorum, keyed by stable node id. Entries
    /// remain quarantined until that process acknowledges or a different lease-bound boot appears;
    /// elapsed time is not evidence that a stalled owner became safe.
    unresponsive: Arc<parking_lot::Mutex<rustc_hash::FxHashMap<u64, Option<Uuid>>>>,
    /// This node's own failure-domain locality; peers carry theirs in `members_rx`.
    self_locality: parking_lot::RwLock<Locality>,
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

mod assignment;
mod authority;
mod barrier;
mod construction;
mod membership;
mod recovery_faults;
mod recovery_identity;
mod recovery_protocol;
mod wire;

use wire::{
    encode_drain_ack, encode_recovery_announcement, encode_recovery_stopped_report,
    encode_release_ready_ack, is_sha256_hex, parse_drain_ack, parse_local_adopted_assignment,
    parse_recovery_announcement, parse_recovery_announcement_ack,
    parse_recovery_stopped_report_shape, parse_release_ready_ack, recovery_round_sha256,
    sha256_hex, validate_recovery_announcement_size, validate_stopped_report_size, DrainAck,
};

#[cfg(test)]
use wire::parse_recovery_stopped_report;

#[cfg(test)]
mod tests;
