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
    CheckpointAssignmentFence, CheckpointParticipant, LeaderProof, PreparedCheckpointWitness,
    MAX_CHECKPOINT_PARTICIPANTS, MAX_PREPARED_CHECKPOINT_WITNESSES,
};
use crate::cluster::discovery::{assignable_node_ids, NodeId, NodeInfo, NodeState};
#[cfg(test)]
use crate::state::CheckpointAttempt;
use crate::state::Locality;

const RECOVERY_INCARNATION_KEY: &str = "control:recovery-incarnation";
const ADOPTED_ASSIGNMENT_KEY: &str = "control:adopted-assignment";
const DRAIN_ACK_KEY: &str = "control:drain-ack";
const DRAIN_ACK_PROTOCOL_VERSION: u16 = 1;
const RELEASE_READY_ACK_KEY: &str = "control:recovery-release-ready";
const RELEASE_READY_PROTOCOL_VERSION: u16 = 2;
const RECOVERY_STOPPED_REPORT_KEY: &str = "control:recovery-stopped";
/// Current wire version for a recovery stopped report.
const RECOVERY_STOPPED_REPORT_PROTOCOL_VERSION: u16 = 3;
/// Maximum encoded size of one recovery stopped report.
const MAX_RECOVERY_STOPPED_REPORT_BYTES: usize = 32 * 1_024;
/// Shared ceiling for mutable recovery intents and immutable release terminals.
pub(super) const MAX_RECOVERY_ANNOUNCEMENT_BYTES: usize = 256 * 1_024;
const RECOVERY_CONTROL_IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_ADOPTED_ASSIGNMENT_BYTES: usize = 1_024;
const MAX_DRAIN_ACK_BYTES: usize = 1_024;
const MAX_RELEASE_READY_ACK_BYTES: usize = 1_024;
const CONTROL_ROSTER_IO_CONCURRENCY: usize = 32;
const PENDING_RELEASE_FAULT_AUDIT_INTERVAL: Duration = Duration::from_secs(1);

#[cfg(feature = "cluster")]
struct LeaderLeaseGate {
    lease: watch::Receiver<Option<super::LeaderLease>>,
    owner: super::LeaderLeaseOwner,
    deadline: Arc<super::LeaseDeadline>,
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

/// Boot-bound phase-one acknowledgement and unresolved prepared-checkpoint inventory.
///
/// The report is recovery evidence, not a checkpoint outcome. Its exact round and publisher bind
/// it to one frozen process quorum; every prepared witness still requires an immutable outcome.
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
    /// Canonically ordered unresolved prepared-checkpoint evidence visible to the publisher.
    prepared_witnesses: Vec<PreparedCheckpointWitness>,
}

impl RecoveryStoppedReport {
    /// Construct a canonical stopped report.
    ///
    /// # Errors
    /// Returns an error when the round, publisher, inventory ordering, count, or encoded size is
    /// invalid.
    pub fn new(
        round: &RecoveryRound,
        publisher: CheckpointParticipant,
        prepared_witnesses: Vec<PreparedCheckpointWitness>,
    ) -> Result<Self, String> {
        let report = Self {
            protocol_version: RECOVERY_STOPPED_REPORT_PROTOCOL_VERSION,
            round_id: round.id,
            round_sha256: recovery_round_sha256(round)?,
            publisher,
            prepared_witnesses,
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

    /// Canonically ordered unresolved prepared-checkpoint evidence.
    #[must_use]
    pub fn prepared_witnesses(&self) -> &[PreparedCheckpointWitness] {
        &self.prepared_witnesses
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
        if self.prepared_witnesses.len() > MAX_PREPARED_CHECKPOINT_WITNESSES {
            return Err(format!(
                "recovery stopped report has {} prepared witnesses; maximum is {MAX_PREPARED_CHECKPOINT_WITNESSES}",
                self.prepared_witnesses.len()
            ));
        }
        for witness in &self.prepared_witnesses {
            witness.validate()?;
            if witness.participant_id != self.publisher.node_id {
                return Err(format!(
                    "prepared checkpoint witness participant {} does not match report publisher {}",
                    witness.participant_id, self.publisher.node_id
                ));
            }
        }
        if self
            .prepared_witnesses
            .windows(2)
            .any(|pair| pair[0].ordering_key() >= pair[1].ordering_key())
        {
            return Err(
                "recovery stopped report witnesses must be uniquely sorted by epoch, checkpoint ID, and participant"
                    .into(),
            );
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

fn sha256_hex(encoded: &[u8]) -> String {
    use std::fmt::Write as _;

    let digest = Sha256::digest(encoded);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        write!(&mut hex, "{byte:02x}").expect("writing to a String cannot fail");
    }
    hex
}

fn recovery_round_sha256(round: &RecoveryRound) -> Result<String, String> {
    let encoded = serde_json::to_vec(&RecoveryStoppedRoundDigestInput {
        protocol: "laminardb-recovery-stopped-round-v3",
        round,
    })
    .map_err(|error| format!("could not encode recovery stopped round identity: {error}"))?;
    Ok(sha256_hex(&encoded))
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
    validate_recovery_announcement_size(raw.len())?;
    let announcement: RecoveryAnnouncement = serde_json::from_str(raw)
        .map_err(|error| format!("invalid recovery announcement: {error}"))?;
    announcement.validate()?;
    if encode_recovery_announcement(&announcement)? != raw {
        return Err("recovery announcement is not canonically encoded".into());
    }
    Ok(Some(announcement))
}

fn validate_recovery_announcement_size(encoded_len: usize) -> Result<(), String> {
    if encoded_len == 0 || encoded_len > MAX_RECOVERY_ANNOUNCEMENT_BYTES {
        return Err(format!(
            "recovery announcement is {encoded_len} bytes; maximum is {MAX_RECOVERY_ANNOUNCEMENT_BYTES}"
        ));
    }
    Ok(())
}

fn encode_recovery_announcement(announcement: &RecoveryAnnouncement) -> Result<String, String> {
    announcement.round.validate()?;
    let encoded = serde_json::to_string(announcement)
        .map_err(|error| format!("could not encode recovery announcement: {error}"))?;
    validate_recovery_announcement_size(encoded.len())?;
    Ok(encoded)
}

fn validate_stopped_report_size(encoded_len: usize) -> Result<(), String> {
    if encoded_len > MAX_RECOVERY_STOPPED_REPORT_BYTES {
        return Err(format!(
            "recovery stopped report is {encoded_len} bytes; maximum is {MAX_RECOVERY_STOPPED_REPORT_BYTES}"
        ));
    }
    Ok(())
}

fn encode_recovery_stopped_report(
    report: &RecoveryStoppedReport,
    round: &RecoveryRound,
) -> Result<String, String> {
    report.validate_semantics(round)?;
    encode_recovery_stopped_report_shape(report)
}

fn encode_recovery_stopped_report_shape(report: &RecoveryStoppedReport) -> Result<String, String> {
    report.validate_shape()?;
    let encoded = serde_json::to_string(report)
        .map_err(|error| format!("could not encode recovery stopped report: {error}"))?;
    validate_stopped_report_size(encoded.len())?;
    Ok(encoded)
}

#[cfg(test)]
fn parse_recovery_stopped_report(
    raw: &str,
    publisher: NodeId,
    round: &RecoveryRound,
) -> Result<RecoveryStoppedReport, String> {
    let report = parse_recovery_stopped_report_shape(raw, publisher)?;
    report.validate_semantics(round)?;
    Ok(report)
}

fn parse_recovery_stopped_report_shape(
    raw: &str,
    publisher: NodeId,
) -> Result<RecoveryStoppedReport, String> {
    validate_stopped_report_size(raw.len())?;
    let report: RecoveryStoppedReport = serde_json::from_str(raw)
        .map_err(|error| format!("invalid recovery stopped report from {publisher}: {error}"))?;
    report.validate_shape()?;
    if report.publisher.node_id != publisher.0 {
        return Err(format!(
            "recovery stopped report from {publisher} names publisher {}",
            report.publisher.node_id
        ));
    }
    if encode_recovery_stopped_report_shape(&report)? != raw {
        return Err(format!(
            "recovery stopped report from {publisher} is not canonically encoded"
        ));
    }
    Ok(report)
}

fn parse_recovery_announcement_ack(
    raw: &str,
    publisher: NodeId,
) -> Result<RecoveryAnnouncement, String> {
    let ack: RecoveryAnnouncementAck = serde_json::from_str(raw)
        .map_err(|error| format!("invalid recovery phase acknowledgement: {error}"))?;
    ack.announcement.validate()?;
    if ack.announcement.round.owner_incarnation(publisher) != Some(ack.incarnation) {
        return Err(format!(
            "recovery phase acknowledgement from {publisher} has a stale process incarnation"
        ));
    }
    Ok(ack.announcement)
}

fn parse_local_adopted_assignment(
    raw: &str,
    participant: CheckpointParticipant,
) -> Result<Option<CheckpointAssignmentAdoption>, String> {
    if raw.is_empty() || raw.len() > MAX_ADOPTED_ASSIGNMENT_BYTES {
        return Err(format!(
            "local adopted assignment is {} bytes; expected 1..={MAX_ADOPTED_ASSIGNMENT_BYTES}",
            raw.len()
        ));
    }
    let adoption: CheckpointAssignmentAdoption = serde_json::from_str(raw)
        .map_err(|error| format!("invalid local adopted assignment: {error}"))?;
    if !adoption.is_canonical() || adoption.participant.node_id != participant.node_id {
        return Err("local adopted assignment has a non-canonical publisher".into());
    }
    let canonical = serde_json::to_string(&adoption).map_err(|error| {
        format!("could not canonically encode local adopted assignment: {error}")
    })?;
    if canonical != raw {
        return Err("local adopted assignment is not canonically encoded".into());
    }
    if adoption.participant.boot_incarnation != participant.boot_incarnation {
        return Ok(None);
    }
    Ok(Some(adoption))
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
    /// Serialises every local authority grant with terminal process fencing.
    process_authority_transition: parking_lot::Mutex<()>,
    /// Process-unique recovery identity, published before this node becomes Active.
    recovery_incarnation: Uuid,
    /// Stable-node process term bound to `recovery_incarnation`.
    recovery_process_term: AtomicU64,
    /// Per-process request identity; shared authority assigns the cluster-visible fault sequence.
    recovery_fault_request_sequence: AtomicU64,
    /// Recovery-safe cluster watermark installed after an immutable Commit outcome or from a
    /// validated recovery capsule. `i64::MIN` = uninitialised.
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
        let controller = Self {
            instance_id,
            barrier,
            kv,
            recovery_kv,
            snapshot,
            members_rx,
            // A new leader must not checkpoint until it proves exact assignment convergence.
            checkpoint_assignment_fence: watch::channel(None).0,
            checkpoint_drain_transition: watch::channel(None).0,
            process_authority_transition: parking_lot::Mutex::new(()),
            recovery_incarnation,
            recovery_process_term: AtomicU64::new(0),
            recovery_fault_request_sequence: AtomicU64::new(1),
            cluster_min_watermark: Arc::new(AtomicI64::new(i64::MIN)),
            draining: Arc::new(AtomicBool::new(false)),
            recovering: Arc::new(AtomicBool::new(false)),
            active: Arc::new(AtomicBool::new(true)),
            process_lease_live: Arc::new(AtomicBool::new(true)),
            process_lease_deadline: std::sync::OnceLock::new(),
            process_lease_authority: std::sync::OnceLock::new(),
            leader_eligible,
            leader_candidacy: watch::channel(super::LeaderCandidacy::initial(false)).0,
            leadership_participants: parking_lot::RwLock::new(None),
            recovery_writes: tokio::sync::Mutex::new(()),
            pending_release_fault_audit: tokio::sync::Mutex::new(None),
            unresponsive: Arc::new(parking_lot::Mutex::new(rustc_hash::FxHashMap::default())),
            self_locality: parking_lot::RwLock::new(Locality::default()),
            #[cfg(feature = "cluster")]
            leader_lease: std::sync::OnceLock::new(),
        };
        controller.notify_leader_eligibility_change();
        controller
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
    ///
    /// # Errors
    /// Returns [`super::ClusterCheckpointAuthorityError::NotConfigured`] when no authority exists.
    #[cfg(feature = "cluster")]
    pub fn checkpoint_authority(
        &self,
    ) -> Result<Arc<super::LeaderLeaseStore>, super::ClusterCheckpointAuthorityError> {
        self.barrier.checkpoint_authority()
    }

    /// Latest recovery-safe cluster watermark installed from an immutable Commit outcome or a
    /// validated recovery capsule.
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
        let eligible = self.is_leader_lease_candidate();
        self.leader_candidacy.send_if_modified(|published| {
            let next = published
                .transition(eligible)
                .unwrap_or_else(super::LeaderCandidacy::terminal);
            if next == *published {
                false
            } else {
                *published = next;
                true
            }
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
    pub fn leader_candidacy_watch(self: &Arc<Self>) -> watch::Receiver<super::LeaderCandidacy> {
        let mut members = self.members_rx.clone();
        members.borrow_and_update();
        self.notify_leader_eligibility_change();
        let candidacy = self.leader_candidacy.clone();
        let candidate_rx = candidacy.subscribe();
        let controller = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = candidacy.closed() => return,
                    changed = members.changed() => {
                        if changed.is_err() {
                            candidacy.send_replace(super::LeaderCandidacy::terminal());
                            return;
                        }
                    }
                }
                let Some(controller) = controller.upgrade() else {
                    return;
                };
                controller.notify_leader_eligibility_change();
            }
        });
        candidate_rx
    }

    /// Mark this node's active status.
    pub fn set_active(&self, active: bool) {
        let _transition = self.process_authority_transition.lock();
        let active = active && self.process_lease_is_live();
        self.active.store(active, Ordering::SeqCst);
        let eligible = active && !self.is_draining();
        if self.leader_eligible.swap(eligible, Ordering::SeqCst) != eligible {
            self.notify_leader_eligibility_change();
        }
    }

    /// Permanently fence controller mutations after stable-node lease loss.
    pub fn fence_process_lease(&self) {
        let _transition = self.process_authority_transition.lock();
        self.process_lease_live.store(false, Ordering::SeqCst);
        if let Some(deadline) = self.process_lease_deadline.get() {
            deadline.fence();
        }
        self.active.store(false, Ordering::SeqCst);
        self.recovering.store(true, Ordering::SeqCst);
        let roster_changed = self.leadership_participants.write().take().is_some();
        if self.leader_eligible.swap(false, Ordering::SeqCst) || roster_changed {
            self.notify_leader_eligibility_change();
        }
        self.checkpoint_assignment_fence.send_replace(None);
        self.checkpoint_drain_transition.send_replace(None);
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

    fn recovery_process_lease_is_live(&self) -> bool {
        self.process_lease_live.load(Ordering::Acquire)
            && self
                .process_lease_deadline
                .get()
                .is_some_and(|deadline| deadline.is_live())
    }

    /// Wait until the process-local lease deadline expires or is explicitly fenced.
    pub async fn wait_for_process_lease_loss(&self) {
        if !self.process_lease_live.load(Ordering::Acquire) {
            return;
        }
        let Some(deadline) = self.process_lease_deadline.get() else {
            std::future::pending::<()>().await;
            return;
        };
        deadline.wait_until_expired().await;
    }

    /// Install the stable-node lease deadline before the runtime starts.
    ///
    /// Reinstalling the same shared deadline is idempotent. A different deadline, or installation
    /// after terminal process fencing, is rejected so controller and data-plane gates cannot
    /// silently follow different lease clocks.
    ///
    /// # Errors
    /// Rejects installation after fencing or replacement with a different deadline.
    pub fn set_process_lease_deadline(
        &self,
        deadline: Arc<super::LeaseDeadline>,
    ) -> Result<(), String> {
        let _transition = self.process_authority_transition.lock();
        if !self.process_lease_live.load(Ordering::Acquire) {
            return Err("process lease authority is already terminally fenced".into());
        }
        if let Some(current) = self.process_lease_deadline.get() {
            if !Arc::ptr_eq(current, &deadline) {
                return Err("process lease deadline is already installed".into());
            }
            #[cfg(feature = "cluster")]
            self.barrier.install_process_lease_deadline(deadline)?;
            return Ok(());
        }
        #[cfg(feature = "cluster")]
        self.barrier
            .install_process_lease_deadline(Arc::clone(&deadline))?;
        self.process_lease_deadline
            .set(deadline)
            .map_err(|_| "process lease deadline is already installed".to_string())
    }

    /// Shared process-local lease deadline, when cluster runtime wiring is complete.
    #[must_use]
    pub fn process_lease_deadline(&self) -> Option<Arc<super::LeaseDeadline>> {
        self.process_lease_deadline.get().cloned()
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
        Box::pin(tokio::time::timeout_at(
            deadline,
            authority.record_assignment_recovery_decision(proof, decision),
        ))
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
        let _transition = self.process_authority_transition.lock();
        let recovering = recovering || !self.process_lease_is_live();
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

    async fn read_recovery_stopped_reports(
        &self,
        round: &RecoveryRound,
        roster: &[CheckpointParticipant],
    ) -> Result<Vec<RecoveryStoppedReport>, RecoveryControlError> {
        round.validate().map_err(RecoveryControlError::Conflict)?;
        if roster
            .windows(2)
            .any(|pair| pair[0].node_id >= pair[1].node_id)
            || roster.iter().any(|participant| {
                round.stopped_participant_incarnation(NodeId(participant.node_id))
                    != Some(participant.boot_incarnation)
            })
        {
            return Err(RecoveryControlError::Conflict(
                "recovery stopped-report read roster is not a canonical round subset".into(),
            ));
        }
        let reads = futures::stream::iter(roster.iter().copied().map(|participant| async move {
            let value = self
                .read_recovery_value(NodeId(participant.node_id), RECOVERY_STOPPED_REPORT_KEY)
                .await;
            (participant, value)
        }))
        .buffer_unordered(CONTROL_ROSTER_IO_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        let mut reports = Vec::new();
        for (participant, value) in reads {
            let Some(raw) = value.map_err(RecoveryControlError::Uncertain)? else {
                continue;
            };
            let publisher = NodeId(participant.node_id);
            let report = parse_recovery_stopped_report_shape(&raw, publisher)
                .map_err(RecoveryControlError::Conflict)?;
            if report.round_id.generation < round.id.generation {
                continue;
            }
            if report.round_id.generation > round.id.generation {
                // A slot alone is not authority: corroborate that its exact publishing boot
                // durably adopted at least this generation. A stale or partially written newer
                // value remains pending for the old round instead of forcing abandon loops.
                let adopted = self
                    .read_recovery_value(publisher, "control:recovery-gen")
                    .await
                    .map_err(RecoveryControlError::Uncertain)?;
                let incarnation = self
                    .read_recovery_value(publisher, RECOVERY_INCARNATION_KEY)
                    .await
                    .map_err(RecoveryControlError::Uncertain)?;
                let Some((adopted, incarnation)) = adopted.zip(incarnation) else {
                    continue;
                };
                let adopted = adopted.parse::<u64>().map_err(|error| {
                    RecoveryControlError::Conflict(format!(
                        "invalid replicated recovery generation from {publisher}: {error}"
                    ))
                })?;
                let incarnation = Uuid::parse_str(&incarnation).map_err(|error| {
                    RecoveryControlError::Conflict(format!(
                        "invalid recovery incarnation published by {publisher}: {error}"
                    ))
                })?;
                if adopted >= report.round_id.generation
                    && incarnation == report.publisher.boot_incarnation
                {
                    reports.push(report);
                }
                continue;
            }
            report.validate_semantics(round).map_err(|error| {
                RecoveryControlError::Conflict(format!(
                    "same-generation recovery stopped report from {publisher} conflicts with the exact frozen round: {error}"
                ))
            })?;
            reports.push(report);
        }
        reports.sort_unstable_by_key(|report| report.publisher.node_id);
        Ok(reports)
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

    /// Read this process's exact local assignment evidence with one bounded checked-KV operation.
    ///
    /// Only the local stable-node slot is read; this method never scans shared assignment or
    /// recovery records. The process identity and sampled lease are captured before the read and
    /// revalidated immediately afterward. The retained report is then validated against the current
    /// boot, and identity is revalidated again around the sampled assignment fence. A canonical
    /// report from an older boot makes the view unavailable rather than being attributed to this
    /// process. A returned adoption also matches the exact locally audited assignment fence sampled
    /// before and after the final identity revalidation.
    ///
    /// # Errors
    /// Fails closed when the process term is unpublished, the lease is not live, the bounded
    /// durable read fails or times out, retained bytes are malformed or non-canonical, a
    /// current-boot record contradicts the local identity, or identity changes during the read.
    /// Checked-storage errors are unavailable even when a backend internally conflates a malformed
    /// outer envelope with I/O. `Invalid` is reserved for logical values returned successfully to
    /// this method that then fail payload bounds, canonicality, current-slot validation, or the
    /// sampled same-version audited fence.
    pub async fn read_local_process_authority_evidence(
        &self,
    ) -> Result<LocalProcessAuthorityEvidence, LocalProcessAuthorityEvidenceError> {
        let before = self
            .capture_live_local_process_authority()
            .map_err(LocalProcessAuthorityEvidenceError::Unavailable)?;
        let node = NodeId(before.participant.node_id);
        let adopted_raw = self
            .read_recovery_value(node, ADOPTED_ASSIGNMENT_KEY)
            .await
            .map_err(LocalProcessAuthorityEvidenceError::Unavailable)?;

        let after_read = self
            .capture_live_local_process_authority()
            .map_err(LocalProcessAuthorityEvidenceError::Unavailable)?;
        if after_read != before {
            return Err(LocalProcessAuthorityEvidenceError::Unavailable(
                "local process authority changed while reading retained evidence".into(),
            ));
        }

        let adopted_raw = adopted_raw.ok_or_else(|| {
            LocalProcessAuthorityEvidenceError::Unavailable(
                "local process has no durable assignment adoption".into(),
            )
        })?;
        let adoption = parse_local_adopted_assignment(&adopted_raw, before.participant)
            .map_err(LocalProcessAuthorityEvidenceError::Invalid)?
            .ok_or_else(|| {
                LocalProcessAuthorityEvidenceError::Unavailable(
                    "durable assignment adoption belongs to a prior local boot".into(),
                )
            })?;
        let expected_fence = match self.checkpoint_assignment_fence(adoption.assignment_version) {
            Some(fence) if adoption.matches_fence(&fence) => fence,
            Some(_) => {
                return Err(LocalProcessAuthorityEvidenceError::Invalid(
                    "durable local adoption contradicts the same-version audited assignment fence"
                        .into(),
                ));
            }
            None => {
                return Err(LocalProcessAuthorityEvidenceError::Unavailable(
                    "matching locally audited assignment fence is unavailable".into(),
                ));
            }
        };

        let after = self
            .capture_live_local_process_authority()
            .map_err(LocalProcessAuthorityEvidenceError::Unavailable)?;
        if after != before {
            return Err(LocalProcessAuthorityEvidenceError::Unavailable(
                "local process authority changed while reading retained evidence".into(),
            ));
        }
        match self.checkpoint_assignment_fence(adoption.assignment_version) {
            Some(fence) if fence == expected_fence && adoption.matches_fence(&fence) => {}
            Some(_) => {
                return Err(LocalProcessAuthorityEvidenceError::Invalid(
                    "locally audited assignment fence changed or contradicted its durable adoption at the same version"
                        .into(),
                ));
            }
            None => {
                return Err(LocalProcessAuthorityEvidenceError::Unavailable(
                    "matching locally audited assignment fence changed during evidence capture"
                        .into(),
                ));
            }
        }

        Ok(LocalProcessAuthorityEvidence {
            participant: after.participant,
            process_term: after.process_term,
            adopted_assignment: adoption,
        })
    }

    /// Publish and read back this process's incarnation metadata.
    ///
    /// This does not grant recovery-write authority. Cluster startup must use
    /// [`Self::publish_leased_recovery_incarnation`] to bind the durable process term.
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
        lease
            .validate(self.instance_id)
            .map_err(|error| error.to_string())?;
        if lease.node != self.instance_id || lease.owner != self.recovery_incarnation {
            return Err("process lease does not bind this recovery incarnation".into());
        }
        if !self.recovery_process_lease_is_live() {
            return Err("live process lease deadline is not installed".into());
        }
        let publisher = RecoveryFaultPublisher {
            participant: CheckpointParticipant {
                node_id: lease.node.0,
                boot_incarnation: lease.owner,
            },
            process_term: lease.term,
        };
        if !self
            .recovery_fault_publisher_is_current(publisher)
            .await
            .map_err(|error| error.to_string())?
        {
            return Err("process lease is not the current durable stable-node term".into());
        }
        if !self.recovery_process_lease_is_live() {
            return Err("process lease changed before publishing recovery identity".into());
        }
        self.barrier.install_local_process_lease(lease)?;
        if let Err(error) = self.publish_recovery_incarnation().await {
            self.fence_process_lease();
            return Err(error);
        }
        if !self.recovery_process_lease_is_live() {
            self.fence_process_lease();
            return Err("process lease changed while publishing recovery identity".into());
        }
        match self.recovery_fault_publisher_is_current(publisher).await {
            Ok(true) if self.recovery_process_lease_is_live() => {}
            Ok(_) => {
                self.fence_process_lease();
                return Err("process lease changed while publishing recovery identity".into());
            }
            Err(error) => {
                self.fence_process_lease();
                return Err(format!(
                    "process lease authority became uncertain after recovery publication: {error}"
                ));
            }
        }
        self.recovery_process_term
            .store(lease.term, Ordering::Release);
        Ok(())
    }

    /// Allocate the next process-local recovery-fault request identity.
    ///
    /// Shared authority converts this ordinal into a cluster-wide fault sequence. Retries retain
    /// the original ordinal; exhaustion fails closed instead of wrapping.
    ///
    /// # Errors
    /// Returns an error if the process-local sequence is exhausted or becomes noncanonical.
    pub fn next_recovery_fault_request(&self) -> Result<RecoveryFaultRequest, String> {
        let sequence = self
            .recovery_fault_request_sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| "recovery fault request sequence exhausted".to_string())?;
        Ok(RecoveryFaultRequest {
            sequence: std::num::NonZeroU64::new(sequence)
                .ok_or_else(|| "recovery fault request allocator produced zero".to_string())?,
        })
    }

    /// Reconstitute one previously allocated request for a bounded retry.
    ///
    /// # Errors
    /// Rejects zero and ordinals that this controller process has not allocated.
    pub fn recovery_fault_request(&self, sequence: u64) -> Result<RecoveryFaultRequest, String> {
        let sequence = std::num::NonZeroU64::new(sequence)
            .ok_or_else(|| "recovery fault request sequence must be nonzero".to_string())?;
        if sequence.get() >= self.recovery_fault_request_sequence.load(Ordering::Acquire) {
            return Err("recovery fault request was not allocated by this process".into());
        }
        Ok(RecoveryFaultRequest { sequence })
    }

    fn recovery_fault_publisher(&self) -> Result<RecoveryFaultPublisher, String> {
        let publisher = RecoveryFaultPublisher {
            participant: CheckpointParticipant {
                node_id: self.instance_id.0,
                boot_incarnation: self.recovery_incarnation,
            },
            process_term: self.recovery_process_term.load(Ordering::Acquire),
        };
        publisher.validate()?;
        Ok(publisher)
    }

    fn capture_live_local_process_authority(&self) -> Result<RecoveryFaultPublisher, String> {
        let _transition = self.process_authority_transition.lock();
        if !self.recovery_process_lease_is_live() {
            return Err("local process lease authority is not live".into());
        }
        let publisher = self.recovery_fault_publisher()?;
        if !self.recovery_process_lease_is_live() {
            return Err("local process lease authority changed while sampling identity".into());
        }
        Ok(publisher)
    }

    async fn recovery_fault_publisher_is_current(
        &self,
        publisher: RecoveryFaultPublisher,
    ) -> Result<bool, RecoveryControlError> {
        if !self.recovery_process_lease_is_live() {
            return Ok(false);
        }
        let authority = self.process_lease_authority.get().ok_or_else(|| {
            RecoveryControlError::Conflict("process lease authority is not installed".into())
        })?;
        let deadline = tokio::time::Instant::now()
            .checked_add(RECOVERY_CONTROL_IO_TIMEOUT)
            .ok_or_else(|| {
                RecoveryControlError::Conflict(
                    "recovery fault process-lease deadline overflow".into(),
                )
            })?;
        let current = authority
            .verify_current_participant_term(
                publisher.participant,
                publisher.process_term,
                deadline,
            )
            .await
            .map_err(RecoveryControlError::from_process_authority)?;
        Ok(current && self.recovery_process_lease_is_live())
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

    /// Whether every current assignment-owner boot identity still equals the frozen round.
    ///
    /// # Errors
    /// Returns an error when the current incarnation roster is unavailable or malformed.
    pub async fn recovery_incarnations_match(&self, round: &RecoveryRound) -> Result<bool, String> {
        Ok(self
            .recovery_participant_incarnations(&round.assignment_fence.participant_ids())
            .await?
            == round.assignment_fence.participants)
    }

    /// Whether every owner and evidence reporter still has the boot identity frozen for stopping.
    ///
    /// This check belongs only to the Prepare/stopped-evidence boundary. Evidence reporters do not
    /// join restore or release liveness quorums after their stopped reports are durable.
    ///
    /// # Errors
    /// Returns an error when the current incarnation roster is unavailable or malformed.
    pub async fn recovery_stopped_incarnations_match(
        &self,
        round: &RecoveryRound,
    ) -> Result<bool, RecoveryControlError> {
        self.recovery_roster_incarnations_match_control(&round.stopped_roster())
            .await
    }

    async fn recovery_incarnations_match_control(
        &self,
        round: &RecoveryRound,
    ) -> Result<bool, RecoveryControlError> {
        self.recovery_roster_incarnations_match_control(&round.assignment_fence.participants)
            .await
    }

    async fn recovery_roster_incarnations_match_control(
        &self,
        roster: &[CheckpointParticipant],
    ) -> Result<bool, RecoveryControlError> {
        let expected: std::collections::BTreeSet<u64> = roster
            .iter()
            .map(|participant| participant.node_id)
            .collect();
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
        if reported.len() != roster.len() {
            return Ok(false);
        }
        Ok(roster.iter().all(|participant| {
            reported.get(&participant.node_id) == Some(&participant.boot_incarnation)
        }))
    }

    async fn read_recovery_fault_inventory_control(
        &self,
    ) -> Result<RecoveryFaultInventory, RecoveryControlError> {
        self.checkpoint_authority()
            .map_err(|error| RecoveryControlError::Conflict(error.to_string()))?
            .recovery_fault_inventory()
            .await
            .map_err(RecoveryControlError::from_authority)
    }

    /// Atomically observed shared-authority recovery-fault inventory.
    ///
    /// # Errors
    /// Returns an error when the leader authority is unavailable or malformed.
    pub async fn read_recovery_fault_inventory(&self) -> Result<RecoveryFaultInventory, String> {
        self.read_recovery_fault_inventory_control()
            .await
            .map_err(|error| error.to_string())
    }

    /// Coherent committed-release and fault view from shared recovery authority.
    ///
    /// # Errors
    /// Returns a classified error when the authority or immutable terminal cannot be validated.
    pub async fn read_recovery_admission_snapshot(
        &self,
    ) -> Result<RecoveryAdmissionSnapshot, RecoveryControlError> {
        self.checkpoint_authority()
            .map_err(|error| RecoveryControlError::Conflict(error.to_string()))?
            .recovery_admission_snapshot()
            .await
            .map_err(RecoveryControlError::from_authority)
    }

    /// Confirm this recovery view still has the same terminal, no active faults, and the exact
    /// audited leader term.
    ///
    /// # Errors
    /// Returns a classified error when the current shared authority cannot be validated.
    pub async fn recovery_admission_is_current(
        &self,
        snapshot: &RecoveryAdmissionSnapshot,
        leader_proof: &LeaderProof,
    ) -> Result<bool, RecoveryControlError> {
        self.checkpoint_authority()
            .map_err(|error| RecoveryControlError::Conflict(error.to_string()))?
            .recovery_admission_is_current(snapshot, leader_proof)
            .await
            .map_err(RecoveryControlError::from_authority)
    }

    /// Publish this process's fault request so the leader drives a recovery round.
    ///
    /// # Errors
    /// Fails when the process lease is stale or the request cannot be ordered in shared authority.
    pub async fn report_fault(
        &self,
        request: RecoveryFaultRequest,
    ) -> Result<RecoveryFaultReportOutcome, String> {
        let seq = request.sequence();
        let _guard = self.recovery_writes.lock().await;
        let publisher = self.recovery_fault_publisher()?;
        if !self
            .recovery_fault_publisher_is_current(publisher)
            .await
            .map_err(|error| error.to_string())?
        {
            return Err("stable node process lease is no longer current".into());
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|error| error.to_string())?;
        let result = Box::pin(authority.record_recovery_fault(publisher, seq))
            .await
            .map_err(|error| RecoveryControlError::from_authority(error).to_string())?;
        if !self
            .recovery_fault_publisher_is_current(publisher)
            .await
            .map_err(|error| error.to_string())?
        {
            return Err("stable node process lease changed while publishing recovery fault".into());
        }
        match result {
            super::leader_lease::RecordRecoveryFaultResult::Active => {
                Ok(RecoveryFaultReportOutcome::Active)
            }
            super::leader_lease::RecordRecoveryFaultResult::AlreadyCleared => {
                Ok(RecoveryFaultReportOutcome::AlreadyCleared)
            }
            super::leader_lease::RecordRecoveryFaultResult::CoveredByNewerRequest => {
                Ok(RecoveryFaultReportOutcome::CoveredByNewerRequest)
            }
            super::leader_lease::RecordRecoveryFaultResult::Superseded => {
                Err("recovery fault request was superseded by a newer local process".into())
            }
        }
    }

    /// Validate this process's atomically released fault while retaining the local fault-write
    /// fence. The caller must hold the returned guard through its source-gate transition.
    ///
    /// `Ok(None)` means the terminal is no longer current, a newer active fault exists anywhere,
    /// or the local active/tombstoned slot does not match it. An exact tombstone is idempotent
    /// success only while the shared fault inventory remains settled.
    ///
    /// # Errors
    /// Returns an error for a nonterminal release, malformed state, or failed durable I/O.
    pub async fn begin_recovery_release(
        &self,
        terminal: &RecoveryAnnouncement,
    ) -> Result<Option<RecoveryReleaseGuard<'_>>, RecoveryControlError> {
        terminal
            .validate()
            .map_err(RecoveryControlError::Conflict)?;
        if !matches!(terminal.phase, RecoverPhase::ReleaseCommitted { .. }) {
            return Err(RecoveryControlError::Conflict(
                "recovery fault consumption requires a terminal Release".into(),
            ));
        }
        let guard = self.recovery_writes.lock().await;
        let publisher = self
            .recovery_fault_publisher()
            .map_err(RecoveryControlError::Conflict)?;
        if !self.recovery_fault_publisher_is_current(publisher).await? {
            return Err(RecoveryControlError::Superseded(
                "stable node process lease is no longer current".into(),
            ));
        }
        let authorized = self
            .checkpoint_authority()
            .map_err(|error| RecoveryControlError::Conflict(error.to_string()))?
            .authorize_recovery_release(publisher, terminal)
            .await
            .map_err(RecoveryControlError::from_authority)?;
        if !authorized {
            return Ok(None);
        }
        if !self.recovery_fault_publisher_is_current(publisher).await? {
            return Err(RecoveryControlError::Superseded(
                "stable node process lease changed while authorizing recovery Release".into(),
            ));
        }
        Ok(Some(RecoveryReleaseGuard {
            _write_guard: guard,
        }))
    }

    /// This stable node's active durable fault sequence, when present.
    ///
    /// # Errors
    /// Returns an error when shared authority is unavailable or malformed.
    pub async fn read_local_fault_report_control(
        &self,
    ) -> Result<Option<u64>, RecoveryControlError> {
        Ok(self
            .read_recovery_fault_inventory_control()
            .await?
            .faults
            .into_iter()
            .find(|fault| fault.reporter == self.instance_id)
            .map(|fault| fault.sequence))
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

    /// Each active stable-node report and its globally monotonic authority sequence.
    ///
    /// # Errors
    /// Returns an error when shared authority is unavailable or malformed.
    pub async fn read_fault_reports(&self) -> Result<Vec<(NodeId, u64)>, String> {
        Ok(self
            .read_recovery_fault_inventory()
            .await?
            .faults
            .into_iter()
            .map(|fault| (fault.reporter, fault.sequence))
            .collect())
    }

    async fn audit_recovery_faults_control(
        &self,
        round: &RecoveryRound,
    ) -> Result<(), RecoveryControlError> {
        let inventory = self.read_recovery_fault_inventory_control().await?;
        if inventory.revision == round.fault_revision && inventory.faults == round.faults {
            Ok(())
        } else {
            Err(RecoveryControlError::Superseded(
                "recovery fault set changed before Release commit".into(),
            ))
        }
    }

    async fn audit_pending_release_faults_control(
        &self,
        release: &RecoveryAnnouncement,
    ) -> Result<(), RecoveryControlError> {
        let release_id =
            RecoveryReleaseId::for_pending(release).map_err(RecoveryControlError::Conflict)?;
        let mut cached = self.pending_release_fault_audit.lock().await;
        let now = tokio::time::Instant::now();
        if cached
            .as_ref()
            .is_some_and(|(audited, valid_until)| audited == &release_id && now < *valid_until)
        {
            return Ok(());
        }
        self.audit_recovery_faults_control(&release.round).await?;
        *cached = Some((
            release_id,
            tokio::time::Instant::now() + PENDING_RELEASE_FAULT_AUDIT_INTERVAL,
        ));
        Ok(())
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
        if !start.round.contains_owner(self.instance_id) {
            return Err("node outside recovery quorum cannot acknowledge restore".into());
        }
        if start.round.owner_incarnation(self.instance_id) != Some(self.recovery_incarnation) {
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
        if release.round.owner_incarnation(self.instance_id) != Some(self.recovery_incarnation) {
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
        let locally_audited =
            self.checkpoint_drain_transition.borrow().as_ref() == Some(transition);
        if !locally_audited
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
        self.write_recovery_value_exact(ADOPTED_ASSIGNMENT_KEY, encoded)
            .await
    }

    /// Each visible process's exact adopted assignment identity.
    ///
    /// # Errors
    /// Fails closed if any visible report is malformed or claims another publisher.
    pub async fn read_adopted_assignments(
        &self,
    ) -> Result<Vec<(NodeId, CheckpointAssignmentAdoption)>, String> {
        self.scan_recovery_values(ADOPTED_ASSIGNMENT_KEY)
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
        let _transition = self.process_authority_transition.lock();
        let fence = if self.process_lease_is_live() {
            fence
        } else {
            None
        };
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
        let _transition = self.process_authority_transition.lock();
        let transition = if self.process_lease_is_live() {
            transition
        } else {
            None
        };
        self.checkpoint_drain_transition.send_replace(transition);
    }

    /// Clear the locally audited drain transition only when it still equals `expected`.
    ///
    /// The comparison and clear share the process-authority transition lock with publication and
    /// terminal process fencing, so a newer transition cannot be erased by stale reconciliation.
    #[must_use]
    pub fn clear_checkpoint_drain_transition_if_matches(
        &self,
        expected: &AssignmentDrainTransition,
    ) -> bool {
        let _transition = self.process_authority_transition.lock();
        let matches = self.checkpoint_drain_transition.borrow().as_ref() == Some(expected);
        if matches {
            self.checkpoint_drain_transition.send_replace(None);
        }
        matches
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
        self.checkpoint_assignment_fence_after_authority_validation(fence, leader)
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_assignment_fence_after_authority_validation(
        &self,
        fence: CheckpointAssignmentFence,
        leader: &crate::checkpoint::LeaderProof,
    ) -> Option<CheckpointAssignmentFence> {
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
        self.barrier.start_server(bind_addr, advertise_host).await
    }

    /// Start a cluster control endpoint whose first advertisement is bound to an acquired
    /// stable-node process lease.
    ///
    /// # Errors
    /// Rejects a lease for another node or boot, a conflicting prior identity, or server start.
    #[cfg(feature = "cluster")]
    pub async fn start_leased_barrier_server(
        &self,
        bind_addr: std::net::SocketAddr,
        advertise_host: Option<String>,
        process_lease: &super::ProcessLease,
    ) -> Result<std::net::SocketAddr, String> {
        process_lease
            .validate(self.instance_id)
            .map_err(|error| error.to_string())?;
        if process_lease.owner != self.recovery_incarnation {
            return Err("control endpoint lease does not bind this process incarnation".into());
        }
        if !self.recovery_process_lease_is_live() {
            return Err("live process lease deadline is not installed".into());
        }
        self.barrier.install_local_process_lease(process_lease)?;
        self.barrier.start_server(bind_addr, advertise_host).await
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

    /// Leader-side assignment-certified Prepare publication with its configured quorum window.
    ///
    /// # Errors
    /// Propagates process-lease and [`BarrierCoordinator::announce_prepare`] errors.
    #[cfg(feature = "cluster")]
    pub async fn announce_prepare_barrier(
        &self,
        ann: &BarrierAnnouncement,
        quorum_window: Duration,
    ) -> Result<(), String> {
        if !self.process_lease_is_live() {
            return Err("stable node process lease is no longer live".into());
        }
        self.barrier.announce_prepare(ann, quorum_window).await
    }

    /// Observe the merged barrier history, validating durable authority only when `predicate`
    /// selects the announcement. Malformed or conflicting histories fail before filtering.
    ///
    /// # Errors
    /// Propagates merge, transport, and matching reversible-authority failures.
    pub async fn observe_barrier_matching<F>(
        &self,
        mut predicate: F,
    ) -> Result<Option<BarrierAnnouncement>, String>
    where
        F: FnMut(&BarrierAnnouncement) -> bool,
    {
        let Some(leader) = self.current_leader() else {
            return Ok(None);
        };
        let Some(announcement) = self.barrier.observe_hint(leader).await? else {
            return Ok(None);
        };
        if !predicate(&announcement) {
            return Ok(None);
        }
        #[cfg(feature = "cluster")]
        self.barrier.validate_observed(&announcement).await?;
        Ok(Some(announcement))
    }

    /// Observe a clustered `Prepare`, validate its leader with one durable-authority read, and
    /// report the local assignment disposition without consulting authority again.
    ///
    /// # Errors
    /// Rejects missing, stale, or conflicting authority and assignment certificates.
    #[cfg(feature = "cluster")]
    pub async fn observe_checkpoint_prepare(
        &self,
    ) -> Result<Option<CheckpointPrepareObservation>, String> {
        let Some(leader) = self.current_leader() else {
            return Ok(None);
        };
        let Some(announcement) = self.barrier.observe_hint(leader).await? else {
            return Ok(None);
        };
        if announcement.phase != super::Phase::Prepare {
            return Ok(None);
        }
        self.barrier
            .validate_checkpoint_prepare(&announcement)
            .await?;
        let proof = announcement
            .leader_proof
            .as_ref()
            .filter(|proof| proof.is_canonical())
            .ok_or_else(|| "leader Prepare omitted its canonical authority proof".to_string())?;
        let assignment_error = match announcement.assignment_fence.as_ref() {
            None => Some(
                "[LDB-6055] leader Prepare omitted its canonical assignment certificate"
                    .to_string(),
            ),
            Some(fence) if !fence.is_canonical() => Some(
                "[LDB-6055] leader Prepare carried a non-canonical assignment certificate"
                    .to_string(),
            ),
            Some(fence) => self
                .checkpoint_assignment_fence(fence.assignment_version)
                .and_then(|certified| {
                    self.checkpoint_assignment_fence_after_authority_validation(certified, proof)
                })
                .map_or_else(
                    || {
                        Some(format!(
                            "[LDB-6055] follower cannot certify leader Prepare assignment {}",
                            fence.assignment_version
                        ))
                    },
                    |certified| {
                        (certified != *fence).then(|| {
                            format!(
                                "[LDB-6055] follower assignment differs from leader Prepare assignment {}",
                                fence.assignment_version
                            )
                        })
                    },
                ),
        };
        Ok(Some(match assignment_error {
            Some(error) => CheckpointPrepareObservation::AssignmentRejected {
                announcement,
                error,
            },
            None => CheckpointPrepareObservation::AssignmentReady(announcement),
        }))
    }

    /// Subscribe to direct checkpoint announcements. Consumers must retain a bounded KV poll:
    /// the watch is a latency path, while the merged gossip history remains the fallback.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn checkpoint_announcement_watch(
        &self,
    ) -> Option<watch::Receiver<Option<BarrierAnnouncement>>> {
        self.barrier.announcement_watch()
    }

    /// Local monotonic receipt time for this exact direct Prepare, if gRPC delivered it.
    #[must_use]
    pub fn checkpoint_prepare_received_at(
        &self,
        prepare: &BarrierAnnouncement,
    ) -> Option<std::time::Instant> {
        self.barrier.prepare_received_at(prepare)
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

    async fn recovery_evidence_roster_matches(
        &self,
        round: &RecoveryRound,
    ) -> Result<bool, String> {
        let candidates = round
            .faults
            .iter()
            .filter(|fault| !round.assignment_fence.contains(fault.reporter.0))
            .map(|fault| fault.reporter.0)
            .collect::<Vec<_>>();
        let available = if candidates.is_empty() {
            Vec::new()
        } else {
            self.available_recovery_participant_incarnations(&candidates)
                .await?
        };
        Ok(available == round.evidence_participants)
    }

    /// Announce phase 1 with the immutable stopped/evidence roster.
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
        if !self.recovery_evidence_roster_matches(round).await? {
            return Err("available recovery evidence roster changed before Prepare".into());
        }
        if !self
            .recovery_stopped_incarnations_match(round)
            .await
            .map_err(|error| error.to_string())?
        {
            return Err("recovery stopped-process roster changed before Prepare".into());
        }
        let _guard = self.recovery_writes.lock().await;
        self.require_recovery_driver_proof(round, "Prepare publication")
            .await?;
        if !self.recovery_evidence_roster_matches(round).await? {
            return Err("available recovery evidence roster changed during Prepare".into());
        }
        let announcement = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Prepare,
        };
        let encoded = encode_recovery_announcement(&announcement)?;
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
        let encoded = encode_recovery_announcement(&announcement)?;
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
        let encoded = encode_recovery_announcement(&release)?;
        self.write_recovery_value_exact("control:recover", encoded)
            .await?;
        self.require_recovery_driver_proof(round, "Release read-back")
            .await
    }

    /// Commit a pending release after every frozen owner published its compact readiness record.
    ///
    /// A pending attempt audits the frozen fault set before returning incomplete readiness. Once
    /// complete, the process roster and fault set are validated under the driver's phase-transition
    /// mutex before admitting the content-addressed terminal into durable leader authority.
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
                self.audit_pending_release_faults_control(release).await?;
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
        self.audit_recovery_faults_control(round).await?;
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
        match Box::pin(authority.record_recovery_release_commit(&round.leader_proof, reference))
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
            super::leader_lease::RecordRecoveryReleaseCommitResult::FaultsChanged => {
                Err(RecoveryControlError::Superseded(
                    "recovery fault inventory changed at Release authority admission".into(),
                ))
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

    /// Whether the assignment-owner quorum names this exact process, not only its stable node id.
    #[must_use]
    pub fn recovery_round_contains_current_process(&self, round: &RecoveryRound) -> bool {
        round.owner_incarnation(self.instance_id) == Some(self.recovery_incarnation)
    }

    /// Whether this exact owner or evidence-reporter process must stop for the round's Prepare.
    #[must_use]
    pub fn recovery_round_requires_current_process_stop(&self, round: &RecoveryRound) -> bool {
        round.stopped_participant_incarnation(self.instance_id) == Some(self.recovery_incarnation)
    }

    /// Ack phase 1 for the exact frozen round.
    ///
    /// # Errors
    /// Returns an error for invalid state or when this node is outside the stopped roster.
    pub async fn announce_stopped(
        &self,
        round: &RecoveryRound,
        prepared_witnesses: Vec<PreparedCheckpointWitness>,
    ) -> Result<(), String> {
        round.validate()?;
        if !round.contains_stopped_participant(self.instance_id) {
            return Err("node outside recovery stopped roster cannot acknowledge Prepare".into());
        }
        if !self.recovery_round_requires_current_process_stop(round) {
            return Err("Prepare acknowledgement has a stale local process incarnation".into());
        }
        if !self.recovery_incarnation_is_current().await? {
            return Err("Prepare acknowledgement came from a superseded local process".into());
        }
        let report = RecoveryStoppedReport::new(
            round,
            CheckpointParticipant {
                node_id: self.instance_id.0,
                boot_incarnation: self.recovery_incarnation,
            },
            prepared_witnesses,
        )?;
        let encoded = encode_recovery_stopped_report(&report, round)?;
        self.write_recovery_value_exact(RECOVERY_STOPPED_REPORT_KEY, encoded)
            .await
    }

    /// Point-read only the still-missing members of an exact stopped roster.
    ///
    /// # Errors
    /// Returns a conflict for a noncanonical or out-of-round subset and preserves the same
    /// uncertainty/conflict/supersession classification used by recovery quorum polling.
    pub async fn read_stopped(
        &self,
        round: &RecoveryRound,
        participants: &[NodeId],
    ) -> Result<Vec<RecoveryStoppedReport>, RecoveryControlError> {
        if participants.windows(2).any(|pair| pair[0].0 >= pair[1].0)
            || participants.iter().any(NodeId::is_unassigned)
        {
            return Err(RecoveryControlError::Conflict(
                "recovery stopped-report subset requires canonical participants".into(),
            ));
        }
        let roster = participants
            .iter()
            .map(|node| {
                round
                    .stopped_participant_incarnation(*node)
                    .map(|boot_incarnation| CheckpointParticipant {
                        node_id: node.0,
                        boot_incarnation,
                    })
                    .ok_or_else(|| {
                        RecoveryControlError::Conflict(format!(
                            "node {node} is outside the recovery stopped roster"
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.read_recovery_stopped_reports(round, &roster).await
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

    /// Wait until the merged barrier history yields an announcement matching `pred`, or `timeout`
    /// expires (→ `Ok(None)`). Observation remains side-effect free; event-time progress must come
    /// from immutable checkpoint authority. Push-driven off the gRPC announcement watch when available; gossip-KV-only
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
            if let Some(ann) = self.observe_barrier_matching(&mut pred).await? {
                return Ok(Some(ann));
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
mod tests;
