//! Durable, append-only leader fencing.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::watch;
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::checkpoint::{
    AssignmentDrainId, AssignmentDrainTransition, CheckpointAssignmentFence,
    ClusterRecoveryCapsule, LeaderProof, LeaderProofOwner, RecoveryCapsuleRef,
    MAX_CHECKPOINT_PARTICIPANTS,
};
use crate::checkpoint_decision::{
    CheckpointDecisionStore, CheckpointOutcome, CheckpointScope, CheckpointVerdict, DecisionError,
    RecordOutcomeResult,
};
use crate::cluster::discovery::NodeId;

use super::catalog_manifest::{
    CatalogManifest, CatalogManifestError, CatalogManifestRef, CatalogSealOutcome,
};
use super::controller::{
    RecoverPhase, RecoveryAdmissionSnapshot, RecoveryAnnouncement, RecoveryFault,
    RecoveryFaultInventory, RecoveryFaultPublisher, RecoveryReleaseId,
    MAX_RECOVERY_ANNOUNCEMENT_BYTES,
};
use super::lease_deadline::LeaseDeadline;
use super::process_lease::{ProcessLease, ProcessLeaseFence};
use super::snapshot::{
    AssignmentSnapshotRef, AssignmentSnapshotStore, RotateOutcome, SnapshotError,
};

const LEASE_PREFIX: &str = "control/leader-lease/";
const AUTHORITY_HEAD_PATH: &str = "control/leader-lease-head/v1.json";
const RECOVERY_RELEASE_TERMINAL_PREFIX: &str = "control/recovery-release-terminals/v2/";
const AUTHORITY_RECORD_VERSION: u32 = 9;
const AUTHORITY_HEAD_VERSION: u32 = 1;
const MAX_AUTHORITY_RECORD_BYTES: u64 = 256 * 1024;
const MAX_AUTHORITY_HEAD_BYTES: u64 = 128;
const MAX_AUTHORITY_HEAD_DISCOVERY_RECORDS: usize = MAX_LIVE_AUTHORITY_LINKS * 3
    + LEADER_LEASE_PRUNE_BATCH_RECORDS * LEADER_LEASE_MAX_PRUNE_BATCHES
    + 2;
const MAX_RECOVERY_FAULT_SLOTS: usize = MAX_CHECKPOINT_PARTICIPANTS * 4;
const RECOVERY_FAULT_AUTHORITY_HEADROOM_BYTES: u64 = 32 * 1024;
const MAX_RECOVERY_RELEASE_TERMINAL_BYTES: u64 = MAX_RECOVERY_ANNOUNCEMENT_BYTES as u64;
const MAX_LEASE_HEAD_READ_ATTEMPTS: usize = 4;
const MAX_LIVE_AUTHORITY_LINKS: usize = 4096;
const OUTCOME_HISTORY_COMPACTION_TRIGGER: usize = 64;
const OUTCOME_HISTORY_RETAINED_LINKS: usize = 16;
const LEADER_LEASE_PRUNE_BATCH_RECORDS: usize = 256;
const LEADER_LEASE_MAX_PRUNE_BATCHES: usize = 4;
const RECOVERY_RELEASE_GC_BATCH_RECORDS: usize = 64;
const RECOVERY_RELEASE_GC_MAX_BATCHES: usize = 4;
const LEADER_LEASE_PRUNE_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const MAX_TEST_LEADER_LEASE_RECORDS: usize = 4096;

fn assignment_snapshot_error(context: &str, error: SnapshotError) -> LeaseError {
    match error {
        SnapshotError::Io(message) => LeaseError::Io(format!("{context}: {message}")),
        error => LeaseError::Invalid(format!("{context}: {error}")),
    }
}

fn lease_path(sequence: u64) -> OsPath {
    OsPath::from(format!("{LEASE_PREFIX}v{sequence:016}.json"))
}

fn authority_head_path() -> OsPath {
    OsPath::from(AUTHORITY_HEAD_PATH)
}

fn recovery_release_terminal_path(reference: &RecoveryReleaseTerminalRef) -> OsPath {
    OsPath::from(format!(
        "{RECOVERY_RELEASE_TERMINAL_PREFIX}generation={:020}/sha256={}.json",
        reference.release.generation(),
        reference.sha256
    ))
}

fn recovery_release_terminal_coordinates(path: &OsPath) -> Result<(u64, String), LeaseError> {
    let raw = path
        .as_ref()
        .strip_prefix(RECOVERY_RELEASE_TERMINAL_PREFIX)
        .and_then(|value| value.strip_prefix("generation="))
        .ok_or_else(|| {
            LeaseError::Invalid(format!("invalid recovery release terminal path {path}"))
        })?;
    let (generation, digest) = raw.split_once("/sha256=").ok_or_else(|| {
        LeaseError::Invalid(format!("invalid recovery release terminal path {path}"))
    })?;
    let digest = digest.strip_suffix(".json").ok_or_else(|| {
        LeaseError::Invalid(format!("invalid recovery release terminal path {path}"))
    })?;
    if generation.len() != 20
        || !generation.bytes().all(|byte| byte.is_ascii_digit())
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(LeaseError::Invalid(format!(
            "invalid recovery release terminal path {path}"
        )));
    }
    let generation = generation.parse::<u64>().map_err(|error| {
        LeaseError::Invalid(format!(
            "invalid recovery release terminal generation in {path}: {error}"
        ))
    })?;
    if generation == 0 {
        return Err(LeaseError::Invalid(format!(
            "invalid recovery release terminal path {path}"
        )));
    }
    Ok((generation, digest.to_owned()))
}

fn lease_sequence_from_path(path: &OsPath) -> Result<u64, LeaseError> {
    let raw = path
        .as_ref()
        .strip_prefix(LEASE_PREFIX)
        .and_then(|file| file.strip_prefix('v'))
        .and_then(|file| file.strip_suffix(".json"))
        .ok_or_else(|| LeaseError::Invalid(format!("invalid leader authority path {path}")))?;
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(LeaseError::Invalid(format!(
            "invalid leader authority sequence in {path}"
        )));
    }
    let sequence = raw.parse::<u64>().map_err(|error| {
        LeaseError::Invalid(format!(
            "invalid leader authority sequence in {path}: {error}"
        ))
    })?;
    if sequence == 0 || lease_path(sequence) != *path {
        return Err(LeaseError::Invalid(format!(
            "noncanonical leader authority path {path}"
        )));
    }
    Ok(sequence)
}

fn consume_live_authority_link(traversed: &mut usize) -> bool {
    if *traversed == MAX_LIVE_AUTHORITY_LINKS {
        return false;
    }
    *traversed += 1;
    true
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(i64::MAX)
}

async fn read_authority_record(
    store: &dyn ObjectStore,
    sequence: u64,
) -> Result<Option<LeaderAuthorityRecord>, LeaseError> {
    let result = match store.get(&lease_path(sequence)).await {
        Ok(result) => result,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(LeaseError::Io(error.to_string())),
    };
    if result.meta.size == 0 || result.meta.size > MAX_AUTHORITY_RECORD_BYTES {
        return Err(LeaseError::Invalid(format!(
            "leader authority record is {} bytes; maximum is {MAX_AUTHORITY_RECORD_BYTES}",
            result.meta.size
        )));
    }
    let bytes = result
        .bytes()
        .await
        .map_err(|error| LeaseError::Io(error.to_string()))?;
    let record: LeaderAuthorityRecord = serde_json::from_slice(&bytes)?;
    record.validate()?;
    if record.lease.seq != sequence {
        return Err(LeaseError::Invalid(
            "authority record sequence does not match its object name".into(),
        ));
    }
    let canonical = serde_json::to_vec(&record)?;
    if canonical.as_slice() != bytes.as_ref() {
        return Err(LeaseError::Invalid(format!(
            "leader authority record {sequence} does not use its canonical body"
        )));
    }
    Ok(Some(record))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorityHeadPointer {
    version: u32,
    sequence: u64,
    nonce: Uuid,
}

impl AuthorityHeadPointer {
    fn new(sequence: u64) -> Result<Self, LeaseError> {
        let pointer = Self {
            version: AUTHORITY_HEAD_VERSION,
            sequence,
            nonce: Uuid::new_v4(),
        };
        pointer.validate()?;
        Ok(pointer)
    }

    fn validate(self) -> Result<(), LeaseError> {
        if self.version != AUTHORITY_HEAD_VERSION || self.sequence == 0 || self.nonce.is_nil() {
            return Err(LeaseError::Invalid(
                "leader authority head has an unsupported version, zero sequence, or nil nonce"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct VersionedAuthorityHeadPointer {
    pointer: AuthorityHeadPointer,
    update_version: UpdateVersion,
}

struct PublishedAuthorityHead {
    record: LeaderAuthorityRecord,
    pointer: VersionedAuthorityHeadPointer,
}

async fn read_authority_head_pointer(
    store: &dyn ObjectStore,
) -> Result<Option<VersionedAuthorityHeadPointer>, LeaseError> {
    let result = match store.get(&authority_head_path()).await {
        Ok(result) => result,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(LeaseError::Io(error.to_string())),
    };
    if result.meta.size == 0 || result.meta.size > MAX_AUTHORITY_HEAD_BYTES {
        return Err(LeaseError::Invalid(format!(
            "leader authority head is {} bytes; maximum is {MAX_AUTHORITY_HEAD_BYTES}",
            result.meta.size
        )));
    }
    let update_version = UpdateVersion {
        e_tag: result.meta.e_tag.clone(),
        version: result.meta.version.clone(),
    };
    let bytes = result
        .bytes()
        .await
        .map_err(|error| LeaseError::Io(error.to_string()))?;
    let pointer: AuthorityHeadPointer = serde_json::from_slice(&bytes)?;
    pointer.validate()?;
    let canonical = serde_json::to_vec(&pointer)?;
    if canonical.as_slice() != bytes.as_ref() {
        return Err(LeaseError::Invalid(
            "leader authority head does not use its canonical body".into(),
        ));
    }
    Ok(Some(VersionedAuthorityHeadPointer {
        pointer,
        update_version,
    }))
}

fn encode_authority_head_pointer(sequence: u64) -> Result<Bytes, LeaseError> {
    let encoded = serde_json::to_vec(&AuthorityHeadPointer::new(sequence)?)?;
    let encoded_len = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
    if encoded.is_empty() || encoded_len > MAX_AUTHORITY_HEAD_BYTES {
        return Err(LeaseError::Invalid(format!(
            "encoded leader authority head is {} bytes; maximum is {MAX_AUTHORITY_HEAD_BYTES}",
            encoded.len()
        )));
    }
    Ok(Bytes::from(encoded))
}

fn encode_authority_record(record: &LeaderAuthorityRecord) -> Result<Bytes, LeaseError> {
    record.validate()?;
    let encoded = serde_json::to_vec(record)?;
    let encoded_len = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
    if encoded.is_empty() || encoded_len > MAX_AUTHORITY_RECORD_BYTES {
        return Err(LeaseError::Invalid(format!(
            "encoded leader authority record is {} bytes; maximum is {MAX_AUTHORITY_RECORD_BYTES}",
            encoded.len()
        )));
    }
    if record.recovery_fault_slots.iter().any(|slot| slot.active) {
        ensure_recovery_fault_authority_headroom(encoded_len)?;
    }
    Ok(Bytes::from(encoded))
}

fn ensure_recovery_fault_authority_headroom(encoded_len: u64) -> Result<(), LeaseError> {
    let limit = MAX_AUTHORITY_RECORD_BYTES
        .checked_sub(RECOVERY_FAULT_AUTHORITY_HEADROOM_BYTES)
        .expect("recovery fault headroom must fit the authority record");
    if encoded_len > limit {
        return Err(LeaseError::Invalid(format!(
            "recovery fault inventory leaves {RECOVERY_FAULT_AUTHORITY_HEADROOM_BYTES} bytes of mandatory authority headroom only up to {limit} bytes; candidate is {encoded_len} bytes"
        )));
    }
    Ok(())
}

/// Exact process incarnation eligible to hold the leader lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderLeaseOwner {
    /// Stable cluster node identity.
    pub node: NodeId,
    /// Boot-unique process identity.
    pub boot: Uuid,
    /// Durable stable-node process term.
    pub process_term: u64,
}

impl LeaderLeaseOwner {
    fn from_process_lease(process: &ProcessLease) -> Result<Self, LeaseError> {
        process
            .validate(process.node)
            .map_err(|error| LeaseError::Invalid(error.to_string()))?;
        Ok(Self {
            node: process.node,
            boot: process.owner,
            process_term: process.term,
        })
    }

    fn validate(&self) -> Result<(), LeaseError> {
        if self.node.is_unassigned() || self.boot.is_nil() || self.process_term == 0 {
            return Err(LeaseError::Invalid(
                "leader owner node, boot identity, and process term must be nonzero".into(),
            ));
        }
        Ok(())
    }

    fn proof_owner(&self) -> LeaderProofOwner {
        LeaderProofOwner {
            node_id: self.node.0,
            boot_id: self.boot,
            process_term: self.process_term,
        }
    }
}

/// Durable leader lease record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderLease {
    /// Append-only compare-and-set sequence.
    pub seq: u64,
    /// Monotonic liveness sequence, advanced only by an acquisition or renewal.
    pub renewal_sequence: u64,
    /// Fencing token, stable across uninterrupted renewals and advanced for each authority term.
    pub token: u64,
    /// Exact process incarnation holding the lease.
    pub owner: LeaderLeaseOwner,
    /// Owner-written wall-clock expiry for diagnostics only.
    pub expires_at_ms: i64,
    /// Immutable catalog content reference, once sealed for this control namespace.
    pub catalog_manifest: Option<CatalogManifestRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutcomeLink {
    sequence: u64,
    epoch: u64,
    checkpoint_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AssignmentDecisionLink {
    sequence: u64,
    target_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorityRecoveryFaultSlot {
    publisher: RecoveryFaultPublisher,
    request_sequence: u64,
    fault_sequence: u64,
    active: bool,
}

impl AuthorityRecoveryFaultSlot {
    fn validate(&self) -> Result<(), LeaseError> {
        self.publisher.validate().map_err(LeaseError::Invalid)?;
        if self.request_sequence == 0 || self.fault_sequence == 0 {
            return Err(LeaseError::Invalid(
                "recovery fault request and authority sequence must be nonzero".into(),
            ));
        }
        Ok(())
    }

    fn fault(&self) -> RecoveryFault {
        RecoveryFault {
            reporter: NodeId(self.publisher.participant.node_id),
            sequence: self.fault_sequence,
        }
    }

    fn matches_request(&self, publisher: RecoveryFaultPublisher, request_sequence: u64) -> bool {
        self.publisher == publisher && self.request_sequence == request_sequence
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryReleaseTerminalRef {
    release: RecoveryReleaseId,
    sha256: String,
    encoded_len: u64,
}

impl RecoveryReleaseTerminalRef {
    fn validate(&self) -> Result<(), LeaseError> {
        if !self.release.is_canonical()
            || self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || self.encoded_len == 0
            || self.encoded_len > MAX_RECOVERY_RELEASE_TERMINAL_BYTES
        {
            return Err(LeaseError::Invalid(
                "recovery release terminal reference is not canonical".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn generation(&self) -> u64 {
        self.release.generation()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorityRecoveryReleaseCommit {
    terminal: RecoveryReleaseTerminalRef,
    leader_proof: LeaderProof,
}

impl AuthorityRecoveryReleaseCommit {
    fn validate(&self) -> Result<(), LeaseError> {
        self.terminal.validate()?;
        if !self.leader_proof.is_canonical() {
            return Err(LeaseError::Invalid(
                "recovery release commit has no canonical leader proof".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryReleaseLink {
    sequence: u64,
    terminal: RecoveryReleaseTerminalRef,
}

impl RecoveryReleaseLink {
    fn validate(&self) -> Result<(), LeaseError> {
        if self.sequence == 0 {
            return Err(LeaseError::Invalid(
                "recovery release authority link has a zero sequence".into(),
            ));
        }
        self.terminal.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecordRecoveryReleaseCommitResult {
    Created(RecoveryReleaseTerminalRef),
    Unchanged(RecoveryReleaseTerminalRef),
    Conflict { winner: RecoveryReleaseTerminalRef },
    FaultsChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordRecoveryFaultResult {
    Active,
    AlreadyCleared,
    CoveredByNewerRequest,
    Superseded,
}

fn recovery_release_id_for_terminal(
    terminal: &RecoveryAnnouncement,
) -> Result<RecoveryReleaseId, LeaseError> {
    terminal.validate().map_err(LeaseError::Invalid)?;
    let RecoverPhase::ReleaseCommitted { epoch } = terminal.phase else {
        return Err(LeaseError::Invalid(
            "recovery release terminal must carry ReleaseCommitted".into(),
        ));
    };
    RecoveryReleaseId::for_pending(&RecoveryAnnouncement {
        round: terminal.round.clone(),
        phase: RecoverPhase::Release { epoch },
    })
    .map_err(LeaseError::Invalid)
}

fn encode_recovery_release_terminal(
    terminal: &RecoveryAnnouncement,
) -> Result<(Bytes, RecoveryReleaseTerminalRef), LeaseError> {
    let release = recovery_release_id_for_terminal(terminal)?;
    let encoded = serde_json::to_vec(terminal)?;
    let encoded_len = u64::try_from(encoded.len())
        .map_err(|_| LeaseError::Invalid("recovery release terminal is too large".into()))?;
    if encoded_len == 0 || encoded_len > MAX_RECOVERY_RELEASE_TERMINAL_BYTES {
        return Err(LeaseError::Invalid(format!(
            "recovery release terminal is {encoded_len} bytes; maximum is {MAX_RECOVERY_RELEASE_TERMINAL_BYTES}"
        )));
    }
    let reference = RecoveryReleaseTerminalRef {
        release,
        sha256: format!("{:x}", Sha256::digest(&encoded)),
        encoded_len,
    };
    reference.validate()?;
    Ok((Bytes::from(encoded), reference))
}

/// Immutable settlement of one exact assignment-drain transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssignmentDrainDecision {
    /// Exact predecessor-to-target transition being settled.
    pub transition: AssignmentDrainTransition,
    /// Durable leader term that decided the transition.
    pub leader_proof: LeaderProof,
    /// Whether to install the certified target or restore the predecessor map.
    pub verdict: AssignmentDrainVerdict,
}

impl AssignmentDrainDecision {
    /// Construct a decision for an exact canonical transition.
    ///
    /// # Errors
    /// Rejects a malformed transition.
    pub fn new(
        transition: &AssignmentDrainTransition,
        leader_proof: LeaderProof,
        verdict: AssignmentDrainVerdict,
    ) -> Result<Self, String> {
        if !transition.is_canonical()
            || !leader_proof.is_canonical()
            || (verdict == AssignmentDrainVerdict::Commit && leader_proof != transition.leader)
        {
            return Err("assignment drain decision requires a canonical transition".into());
        }
        Ok(Self {
            transition: transition.clone(),
            leader_proof,
            verdict,
        })
    }

    /// Compact identity used by source-drain receipts and authority lookups.
    #[must_use]
    pub fn round(&self) -> AssignmentDrainId {
        self.transition.id()
    }

    /// Target assignment version settled by this decision.
    #[must_use]
    pub fn target_version(&self) -> u64 {
        self.transition.target.assignment_version
    }

    fn validate(&self) -> Result<(), LeaseError> {
        if !self.transition.is_canonical()
            || !self.leader_proof.is_canonical()
            || (self.verdict == AssignmentDrainVerdict::Commit
                && self.leader_proof != self.transition.leader)
        {
            return Err(LeaseError::Invalid(
                "assignment drain decision is not canonical".into(),
            ));
        }
        Ok(())
    }
}

/// Terminal result for an assignment-drain transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentDrainVerdict {
    /// Install the transition's certified target assignment.
    Commit,
    /// Restore the predecessor owner map at the target version.
    Abort,
}

/// Result of admitting a drain decision through the shared authority sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordAssignmentDrainDecisionResult {
    /// This call created the immutable decision.
    Created(AssignmentDrainDecision),
    /// The same decision was already durable.
    Unchanged(AssignmentDrainDecision),
    /// Another terminal decision already won for this transition version.
    Conflict {
        /// Immutable winner.
        winner: AssignmentDrainDecision,
    },
}

/// Immutable authorization to install one staged failure-recovery assignment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssignmentRecoveryDecision {
    /// Exact assignment being replaced.
    pub predecessor: CheckpointAssignmentFence,
    /// Exact successor assignment authorized for installation.
    pub target: CheckpointAssignmentFence,
    /// Content-addressed staged snapshot whose assignment must equal `target`.
    pub proposal: AssignmentSnapshotRef,
    /// Exact process-lease takeover proofs for every predecessor process absent from `target`.
    pub removed_process_fences: Vec<ProcessLeaseFence>,
    /// Durable leader term that authorized this recovery.
    pub leader_proof: LeaderProof,
}

impl AssignmentRecoveryDecision {
    /// Construct one exact failure-recovery authorization.
    ///
    /// # Errors
    /// Rejects invalid fences, a non-successor proposal, or an incomplete process-removal proof.
    pub fn new(
        predecessor: CheckpointAssignmentFence,
        target: CheckpointAssignmentFence,
        proposal: AssignmentSnapshotRef,
        removed_process_fences: Vec<ProcessLeaseFence>,
        leader_proof: LeaderProof,
    ) -> Result<Self, String> {
        let decision = Self {
            predecessor,
            target,
            proposal,
            removed_process_fences,
            leader_proof,
        };
        decision.validate().map_err(|error| error.to_string())?;
        Ok(decision)
    }

    /// Target assignment version settled by this decision.
    #[must_use]
    pub fn target_version(&self) -> u64 {
        self.target.assignment_version
    }

    fn validate(&self) -> Result<(), LeaseError> {
        if !self.predecessor.is_canonical()
            || !self.target.is_canonical()
            || !self.leader_proof.is_canonical()
            || self.predecessor.vnode_count != self.target.vnode_count
            || self.predecessor.assignment_version.checked_add(1)
                != Some(self.target.assignment_version)
            || self.proposal.validate().is_err()
            || self.proposal.version != self.target.assignment_version
        {
            return Err(LeaseError::Invalid(
                "assignment recovery decision is not a canonical successor".into(),
            ));
        }

        let removed: Vec<_> = self
            .predecessor
            .participants
            .iter()
            .filter(|participant| {
                self.target.participant_incarnation(participant.node_id)
                    != Some(participant.boot_incarnation)
            })
            .collect();
        if removed.is_empty() || removed.len() != self.removed_process_fences.len() {
            return Err(LeaseError::Invalid(
                "assignment recovery decision requires the exact removed predecessor process set"
                    .into(),
            ));
        }
        for (participant, fence) in removed.iter().zip(&self.removed_process_fences) {
            if !fence.is_canonical()
                || fence.predecessor.node.0 != participant.node_id
                || fence.predecessor.owner != participant.boot_incarnation
            {
                return Err(LeaseError::Invalid(
                    "assignment recovery process fences are incomplete, unsorted, or unrelated"
                        .into(),
                ));
            }
        }
        Ok(())
    }
}

/// Result of admitting a recovery decision through the shared authority sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordAssignmentRecoveryDecisionResult {
    /// This call created the immutable decision.
    Created(AssignmentRecoveryDecision),
    /// The same decision was already durable.
    Unchanged(AssignmentRecoveryDecision),
    /// Another recovery decision already won for this target version.
    Conflict {
        /// Immutable winner.
        winner: AssignmentRecoveryDecision,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "decision",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum AuthorityAssignmentDecision {
    Drain(AssignmentDrainDecision),
    Recovery(AssignmentRecoveryDecision),
}

impl AuthorityAssignmentDecision {
    fn target_version(&self) -> u64 {
        match self {
            Self::Drain(decision) => decision.target_version(),
            Self::Recovery(decision) => decision.target_version(),
        }
    }

    fn leader_proof(&self) -> &LeaderProof {
        match self {
            Self::Drain(decision) => &decision.leader_proof,
            Self::Recovery(decision) => &decision.leader_proof,
        }
    }

    fn validate(&self) -> Result<(), LeaseError> {
        match self {
            Self::Drain(decision) => decision.validate(),
            Self::Recovery(decision) => decision.validate(),
        }
    }
}

enum RecordAuthorityAssignmentDecisionResult {
    Created(AuthorityAssignmentDecision),
    Unchanged(AuthorityAssignmentDecision),
    Conflict { winner: AuthorityAssignmentDecision },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorityOutcomeFloor {
    deployment_id: String,
    artifact_before_epoch: u64,
    authority_before_epoch: u64,
    terminal_anchor: Option<CheckpointOutcome>,
    terminal_anchor_link: Option<OutcomeLink>,
    committed_anchor: Option<CheckpointOutcome>,
    committed_anchor_link: Option<OutcomeLink>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorityAssignmentDecisionFloor {
    before_target_version: u64,
    terminal_anchor: Option<AuthorityAssignmentDecision>,
    terminal_anchor_link: Option<AssignmentDecisionLink>,
}

/// One immutable entry in the cluster's shared leadership and checkpoint-decision sequence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaderAuthorityRecord {
    version: u32,
    lease: LeaderLease,
    /// Present only on the sequence that admitted this terminal outcome.
    checkpoint_outcome: Option<CheckpointOutcome>,
    /// Link to the preceding admitted outcome, present only on an outcome-bearing record.
    previous_outcome: Option<OutcomeLink>,
    /// Latest admitted outcome. Renewals, takeovers, catalog seals, and floor advances preserve it.
    outcome_head: Option<OutcomeLink>,
    /// Link to the preceding admitted Commit, present only on a Commit-bearing record.
    previous_commit: Option<OutcomeLink>,
    /// Latest admitted Commit. Every authority mutation preserves it; Abort does not advance it.
    commit_head: Option<OutcomeLink>,
    /// Monotonic cluster outcome retention boundary and its continuity anchors.
    outcome_floor: Option<AuthorityOutcomeFloor>,
    /// Present only on the sequence that admitted an assignment decision.
    assignment_decision: Option<AuthorityAssignmentDecision>,
    /// Link to the preceding assignment decision, present only on a decision-bearing record.
    previous_assignment_decision: Option<AssignmentDecisionLink>,
    /// Latest admitted assignment decision. Every other authority mutation preserves it.
    assignment_decision_head: Option<AssignmentDecisionLink>,
    /// Monotonic assignment-decision retention boundary and its continuity anchor.
    assignment_decision_floor: Option<AuthorityAssignmentDecisionFloor>,
    /// Authority sequence that admitted the current recovery-fault slot state.
    recovery_fault_revision: u64,
    /// One active or tombstoned request identity per stable node.
    recovery_fault_slots: Vec<AuthorityRecoveryFaultSlot>,
    /// Present only on the sequence that admitted the latest recovery release.
    recovery_release_commit: Option<AuthorityRecoveryReleaseCommit>,
    /// Latest admitted recovery release, preserved by every later authority mutation.
    recovery_release_head: Option<RecoveryReleaseLink>,
}

impl LeaderLease {
    fn validate(&self) -> Result<(), LeaseError> {
        self.owner.validate()?;
        if self.seq == 0
            || self.renewal_sequence == 0
            || self.renewal_sequence > self.seq
            || self.token == 0
        {
            return Err(LeaseError::Invalid(
                "leader lease sequence and token must be nonzero and renewal sequence must be within 1..=sequence"
                    .into(),
            ));
        }
        if let Some(reference) = &self.catalog_manifest {
            reference
                .validate()
                .map_err(|error| LeaseError::Invalid(error.to_string()))?;
        }
        Ok(())
    }

    /// Exact feature-neutral proof for this ownership term.
    #[must_use]
    pub fn proof(&self) -> LeaderProof {
        LeaderProof {
            owner: self.owner.proof_owner(),
            fencing_token: self.token,
        }
    }

    /// Whether `proof` names this exact owner and fencing token.
    #[must_use]
    pub fn matches_proof(&self, proof: &LeaderProof) -> bool {
        proof.is_canonical()
            && proof.owner == self.owner.proof_owner()
            && proof.fencing_token == self.token
    }

    fn has_same_liveness_identity(&self, other: &Self) -> bool {
        self.owner == other.owner
            && self.token == other.token
            && self.renewal_sequence == other.renewal_sequence
    }
}

impl AuthorityOutcomeFloor {
    fn validate(&self) -> Result<(), LeaseError> {
        let deployment = Uuid::parse_str(&self.deployment_id)
            .map_err(|error| LeaseError::Invalid(format!("outcome floor deployment: {error}")))?;
        if deployment.is_nil()
            || deployment.to_string() != self.deployment_id
            || self.authority_before_epoch == 0
            || self.artifact_before_epoch > self.authority_before_epoch
        {
            return Err(LeaseError::Invalid(
                "outcome floor requires a canonical deployment and ordered authority/artifact horizons"
                    .into(),
            ));
        }
        for (name, anchor) in [
            ("terminal", self.terminal_anchor.as_ref()),
            ("committed", self.committed_anchor.as_ref()),
        ] {
            let Some(anchor) = anchor else { continue };
            anchor
                .validate_shape(anchor.epoch)
                .map_err(|error| LeaseError::Invalid(error.to_string()))?;
            if anchor.scope != CheckpointScope::Cluster
                || anchor.deployment_id != self.deployment_id
            {
                return Err(LeaseError::Invalid(format!(
                    "outcome floor has an invalid {name} anchor"
                )));
            }
        }
        match (self.terminal_anchor.as_ref(), self.terminal_anchor_link) {
            (Some(anchor), Some(link))
                if anchor.epoch < self.authority_before_epoch
                    && link.sequence != 0
                    && link.epoch == anchor.epoch
                    && link.checkpoint_id == anchor.checkpoint_id => {}
            (None, None) => {}
            _ => {
                return Err(LeaseError::Invalid(
                    "outcome floor terminal anchor does not match its exact authority link".into(),
                ));
            }
        }
        match (self.committed_anchor.as_ref(), self.committed_anchor_link) {
            (Some(anchor), Some(link))
                if self.artifact_before_epoch != 0
                    && anchor.is_commit()
                    && anchor.epoch < self.artifact_before_epoch
                    && link.sequence != 0
                    && link.epoch == anchor.epoch
                    && link.checkpoint_id == anchor.checkpoint_id => {}
            (None, None) => {}
            _ => {
                return Err(LeaseError::Invalid(
                    "outcome floor committed anchor does not match its exact Commit link".into(),
                ));
            }
        }
        if let (Some(committed), Some(terminal)) = (
            self.committed_anchor.as_ref(),
            self.terminal_anchor.as_ref(),
        ) {
            let ordered = if committed.epoch == terminal.epoch {
                committed == terminal
            } else {
                committed.epoch < terminal.epoch && committed.checkpoint_id < terminal.checkpoint_id
            };
            if !ordered {
                return Err(LeaseError::Invalid(
                    "outcome floor anchors are not monotonically ordered".into(),
                ));
            }
            let links_ordered = match (self.committed_anchor_link, self.terminal_anchor_link) {
                (Some(committed_link), Some(terminal_link)) if committed == terminal => {
                    committed_link == terminal_link
                }
                (Some(committed_link), Some(terminal_link)) => {
                    committed_link.sequence < terminal_link.sequence
                }
                _ => false,
            };
            if !links_ordered {
                return Err(LeaseError::Invalid(
                    "outcome floor anchor links are not ordered with their exact outcomes".into(),
                ));
            }
        } else if self.committed_anchor.is_some() {
            return Err(LeaseError::Invalid(
                "outcome floor has a committed anchor without terminal continuity".into(),
            ));
        }
        Ok(())
    }
}

impl AuthorityAssignmentDecisionFloor {
    fn validate(&self) -> Result<(), LeaseError> {
        if self.before_target_version == 0 {
            return Err(LeaseError::Invalid(
                "assignment decision floor requires a nonzero target version".into(),
            ));
        }
        match (self.terminal_anchor.as_ref(), self.terminal_anchor_link) {
            (Some(anchor), Some(link)) => {
                anchor.validate()?;
                if link.sequence == 0
                    || link.target_version != anchor.target_version()
                    || anchor.target_version() >= self.before_target_version
                {
                    return Err(LeaseError::Invalid(
                        "assignment decision floor anchor does not match its exact authority link"
                            .into(),
                    ));
                }
            }
            (None, None) => {}
            _ => {
                return Err(LeaseError::Invalid(
                    "assignment decision floor has an incomplete terminal anchor".into(),
                ));
            }
        }
        Ok(())
    }
}

impl LeaderAuthorityRecord {
    fn initial(lease: LeaderLease) -> Self {
        Self {
            version: AUTHORITY_RECORD_VERSION,
            lease,
            checkpoint_outcome: None,
            previous_outcome: None,
            outcome_head: None,
            previous_commit: None,
            commit_head: None,
            outcome_floor: None,
            assignment_decision: None,
            previous_assignment_decision: None,
            assignment_decision_head: None,
            assignment_decision_floor: None,
            recovery_fault_revision: 0,
            recovery_fault_slots: Vec::new(),
            recovery_release_commit: None,
            recovery_release_head: None,
        }
    }

    fn preserve_with_lease(&self, lease: LeaderLease) -> Self {
        Self {
            version: AUTHORITY_RECORD_VERSION,
            lease,
            checkpoint_outcome: None,
            previous_outcome: None,
            outcome_head: self.outcome_head,
            previous_commit: None,
            commit_head: self.commit_head,
            outcome_floor: self.outcome_floor.clone(),
            assignment_decision: None,
            previous_assignment_decision: None,
            assignment_decision_head: self.assignment_decision_head,
            assignment_decision_floor: self.assignment_decision_floor.clone(),
            recovery_fault_revision: self.recovery_fault_revision,
            recovery_fault_slots: self.recovery_fault_slots.clone(),
            recovery_release_commit: None,
            recovery_release_head: self.recovery_release_head.clone(),
        }
    }

    fn validate(&self) -> Result<(), LeaseError> {
        if self.version != AUTHORITY_RECORD_VERSION {
            return Err(LeaseError::Invalid(format!(
                "authority record version {} is unsupported",
                self.version
            )));
        }
        self.lease.validate()?;
        if let Some(floor) = &self.outcome_floor {
            floor.validate()?;
        }
        if let Some(floor) = &self.assignment_decision_floor {
            floor.validate()?;
        }
        if self.recovery_fault_slots.windows(2).any(|pair| {
            pair[0].publisher.participant.node_id >= pair[1].publisher.participant.node_id
        }) || (self.recovery_fault_revision == 0 && !self.recovery_fault_slots.is_empty())
            || (self.recovery_fault_revision != 0
                && self.recovery_fault_slots.is_empty()
                && self
                    .recovery_release_head
                    .as_ref()
                    .map(|head| head.sequence)
                    != Some(self.recovery_fault_revision))
            || self.recovery_fault_revision > self.lease.seq
            || self.recovery_fault_slots.len() > MAX_RECOVERY_FAULT_SLOTS
        {
            return Err(LeaseError::Invalid(
                "leader authority recovery-fault inventory is not canonical".into(),
            ));
        }
        let mut fault_sequences = BTreeSet::new();
        for slot in &self.recovery_fault_slots {
            slot.validate()?;
            if slot.fault_sequence > self.recovery_fault_revision
                || slot.fault_sequence > self.lease.seq
                || !fault_sequences.insert(slot.fault_sequence)
            {
                return Err(LeaseError::Invalid(
                    "leader authority recovery-fault sequences are not canonical".into(),
                ));
            }
        }
        if [
            self.checkpoint_outcome.is_some(),
            self.assignment_decision.is_some(),
            self.recovery_release_commit.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count()
            > 1
        {
            return Err(LeaseError::Invalid(
                "one authority sequence cannot admit two terminal domains".into(),
            ));
        }
        match self.checkpoint_outcome.as_ref() {
            Some(outcome) => {
                outcome
                    .validate_shape(outcome.epoch)
                    .map_err(|error| LeaseError::Invalid(error.to_string()))?;
                if outcome.scope != CheckpointScope::Cluster {
                    return Err(LeaseError::Invalid(
                        "leader authority can only admit cluster checkpoint outcomes".into(),
                    ));
                }
                let proof = outcome.leader_proof.as_ref().ok_or_else(|| {
                    LeaseError::Invalid("cluster outcome has no leader proof".into())
                })?;
                let current_link = OutcomeLink {
                    sequence: self.lease.seq,
                    epoch: outcome.epoch,
                    checkpoint_id: outcome.checkpoint_id,
                };
                if !self.lease.matches_proof(proof) || self.outcome_head != Some(current_link) {
                    return Err(LeaseError::Invalid(
                        "cluster outcome is not bound to its exact authority sequence and term"
                            .into(),
                    ));
                }
                if let Some(previous) = self.previous_outcome {
                    if previous.sequence >= self.lease.seq
                        || previous.epoch >= outcome.epoch
                        || previous.checkpoint_id >= outcome.checkpoint_id
                    {
                        return Err(LeaseError::Invalid(
                            "cluster outcome link does not move backward in sequence and epoch"
                                .into(),
                        ));
                    }
                }
                if outcome.is_commit() {
                    if self.commit_head != Some(current_link) {
                        return Err(LeaseError::Invalid(
                            "cluster Commit is not bound to its exact Commit-chain head".into(),
                        ));
                    }
                    if let Some(previous) = self.previous_commit {
                        if previous.sequence >= self.lease.seq
                            || previous.epoch >= outcome.epoch
                            || previous.checkpoint_id >= outcome.checkpoint_id
                        {
                            return Err(LeaseError::Invalid(
                                "cluster Commit link does not move backward in sequence and epoch"
                                    .into(),
                            ));
                        }
                        if self.outcome_floor.as_ref().is_some_and(|floor| {
                            previous.epoch < floor.artifact_before_epoch
                                && Some(previous) != floor.committed_anchor_link
                        }) {
                            return Err(LeaseError::Invalid(
                                "cluster Commit link crosses the artifact floor without its exact anchor"
                                    .into(),
                            ));
                        }
                    }
                } else if self.previous_commit.is_some() {
                    return Err(LeaseError::Invalid(
                        "cluster Abort carries a previous-Commit link".into(),
                    ));
                }
                if let Some(floor) = &self.outcome_floor {
                    if outcome.deployment_id != floor.deployment_id
                        || outcome.epoch < floor.authority_before_epoch
                    {
                        return Err(LeaseError::Invalid(
                            "cluster outcome is below or outside its durable authority floor"
                                .into(),
                        ));
                    }
                }
            }
            None => {
                if self.previous_outcome.is_some() || self.previous_commit.is_some() {
                    return Err(LeaseError::Invalid(
                        "non-outcome authority record carries an outcome-chain link".into(),
                    ));
                }
                if self.outcome_head.is_some_and(|head| {
                    head.sequence > self.lease.seq || head.epoch == 0 || head.checkpoint_id == 0
                }) {
                    return Err(LeaseError::Invalid(
                        "authority outcome head is outside the durable sequence".into(),
                    ));
                }
            }
        }
        if self.commit_head.is_some_and(|head| {
            head.sequence > self.lease.seq || head.epoch == 0 || head.checkpoint_id == 0
        }) {
            return Err(LeaseError::Invalid(
                "authority Commit head is outside the durable sequence".into(),
            ));
        }
        match (self.commit_head, self.outcome_head) {
            (Some(commit), Some(terminal)) => {
                let equal = commit == terminal;
                let strictly_older = commit.sequence < terminal.sequence
                    && commit.epoch < terminal.epoch
                    && commit.checkpoint_id < terminal.checkpoint_id;
                if !equal && !strictly_older {
                    return Err(LeaseError::Invalid(
                        "authority Commit head is not ordered behind the terminal head".into(),
                    ));
                }
                if self
                    .checkpoint_outcome
                    .as_ref()
                    .is_some_and(|outcome| !outcome.is_commit())
                    && !strictly_older
                {
                    return Err(LeaseError::Invalid(
                        "cluster Abort did not preserve a strictly older Commit head".into(),
                    ));
                }
            }
            (Some(_), None) => {
                return Err(LeaseError::Invalid(
                    "authority Commit head exists without terminal continuity".into(),
                ));
            }
            (None, _) => {}
        }
        if let Some(floor) = self.outcome_floor.as_ref() {
            if floor
                .terminal_anchor_link
                .is_some_and(|link| link.sequence >= self.lease.seq)
            {
                return Err(LeaseError::Invalid(
                    "outcome floor anchor is outside the authority sequence".into(),
                ));
            }
            if floor
                .committed_anchor_link
                .is_some_and(|link| link.sequence >= self.lease.seq)
            {
                return Err(LeaseError::Invalid(
                    "outcome floor Commit anchor is outside the authority sequence".into(),
                ));
            }
            if let Some(head) = self.outcome_head {
                if head.epoch < floor.authority_before_epoch
                    && Some(head) != floor.terminal_anchor_link
                {
                    return Err(LeaseError::Invalid(
                        "authority outcome head does not meet its durable compaction floor".into(),
                    ));
                }
            } else if floor.terminal_anchor_link.is_some() {
                return Err(LeaseError::Invalid(
                    "outcome floor anchor is disconnected from the authority head".into(),
                ));
            }
            if let Some(head) = self.commit_head {
                if head.epoch < floor.artifact_before_epoch
                    && Some(head) != floor.committed_anchor_link
                {
                    return Err(LeaseError::Invalid(
                        "authority Commit head does not meet its durable artifact floor".into(),
                    ));
                }
            } else if floor.committed_anchor_link.is_some() {
                return Err(LeaseError::Invalid(
                    "outcome floor Commit anchor is disconnected from the Commit head".into(),
                ));
            }
        }
        match self.assignment_decision.as_ref() {
            Some(decision) => {
                decision.validate()?;
                if self
                    .assignment_decision_floor
                    .as_ref()
                    .is_some_and(|floor| decision.target_version() < floor.before_target_version)
                {
                    return Err(LeaseError::Invalid(
                        "assignment decision is below its durable authority floor".into(),
                    ));
                }
                if !self.lease.matches_proof(decision.leader_proof())
                    || self.assignment_decision_head
                        != Some(AssignmentDecisionLink {
                            sequence: self.lease.seq,
                            target_version: decision.target_version(),
                        })
                {
                    return Err(LeaseError::Invalid(
                        "assignment decision is not bound to its exact authority sequence and term"
                            .into(),
                    ));
                }
                if let Some(previous) = self.previous_assignment_decision {
                    if previous.sequence >= self.lease.seq
                        || previous.target_version >= decision.target_version()
                    {
                        return Err(LeaseError::Invalid(
                            "assignment decision link does not move backward".into(),
                        ));
                    }
                }
            }
            None => {
                if self.previous_assignment_decision.is_some() {
                    return Err(LeaseError::Invalid(
                        "non-decision authority record carries a previous assignment-decision link"
                            .into(),
                    ));
                }
                if self
                    .assignment_decision_head
                    .is_some_and(|head| head.sequence > self.lease.seq || head.target_version == 0)
                {
                    return Err(LeaseError::Invalid(
                        "authority assignment-decision head is outside the durable sequence".into(),
                    ));
                }
            }
        }
        if let Some(floor) = self.assignment_decision_floor.as_ref() {
            if floor
                .terminal_anchor_link
                .is_some_and(|link| link.sequence >= self.lease.seq)
            {
                return Err(LeaseError::Invalid(
                    "assignment decision floor anchor is outside the authority sequence".into(),
                ));
            }
            if let Some(head) = self.assignment_decision_head {
                if head.target_version < floor.before_target_version
                    && Some(head) != floor.terminal_anchor_link
                {
                    return Err(LeaseError::Invalid(
                        "authority assignment-decision head does not meet its durable floor".into(),
                    ));
                }
            } else if floor.terminal_anchor_link.is_some() {
                return Err(LeaseError::Invalid(
                    "assignment decision floor anchor is disconnected from the authority head"
                        .into(),
                ));
            }
        }
        match self.recovery_release_commit.as_ref() {
            Some(commit) => {
                commit.validate()?;
                let expected = RecoveryReleaseLink {
                    sequence: self.lease.seq,
                    terminal: commit.terminal.clone(),
                };
                if !self.lease.matches_proof(&commit.leader_proof)
                    || self.recovery_release_head.as_ref() != Some(&expected)
                    || self.recovery_fault_revision != self.lease.seq
                    || self.recovery_fault_slots.iter().any(|slot| slot.active)
                {
                    return Err(LeaseError::Invalid(
                        "recovery release commit is not bound to its exact authority sequence, term, and settled fault inventory"
                            .into(),
                    ));
                }
            }
            None => {
                if let Some(head) = self.recovery_release_head.as_ref() {
                    head.validate()?;
                    if head.sequence >= self.lease.seq {
                        return Err(LeaseError::Invalid(
                            "recovery release head is outside the durable authority sequence"
                                .into(),
                        ));
                    }
                    if !self.recovery_fault_slots.iter().any(|slot| slot.active)
                        && self.recovery_fault_revision != head.sequence
                    {
                        return Err(LeaseError::Invalid(
                            "settled recovery fault inventory is not bound to its release head"
                                .into(),
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Result of an acquisition or renewal attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseOutcome {
    /// The caller's exact process incarnation owns the returned record.
    Acquired(LeaderLease),
    /// A rival exact process incarnation owns the returned record.
    Held(LeaderLease),
}

enum AuthorityCreateOutcome {
    Created,
    ExistingIdentical,
    Contended(LeaderAuthorityRecord),
}

#[derive(Clone, Copy)]
enum SameOwnerToken {
    #[cfg(test)]
    Preserve,
    Rotate,
    Exact(u64),
}

/// Candidate-local proof that one rival liveness identity remained current for a full TTL.
#[derive(Debug)]
pub struct LeaderLeaseObservation {
    lease: LeaderLease,
    started: Instant,
}

/// Coalescing-safe local candidacy state. A generation change means a published grant was lost
/// and must never reuse its fencing token, even when the current state is eligible again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaderCandidacy {
    eligible: bool,
    generation: u64,
}

impl LeaderCandidacy {
    pub(crate) const fn initial(eligible: bool) -> Self {
        Self {
            eligible,
            generation: 1,
        }
    }

    pub(crate) fn transition(self, eligible: bool) -> Option<Self> {
        let generation = if self.eligible && !eligible {
            self.generation.checked_add(1)?
        } else {
            self.generation
        };
        Some(Self {
            eligible,
            generation,
        })
    }

    pub(crate) const fn terminal() -> Self {
        Self {
            eligible: false,
            generation: u64::MAX,
        }
    }

    /// Whether this process is currently eligible to contend for the leader lease.
    #[must_use]
    pub const fn is_eligible(self) -> bool {
        self.eligible
    }
}

/// Leader lease storage or validation failure.
#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    /// Underlying object-store failure.
    #[error("object store I/O: {0}")]
    Io(String),
    /// Malformed configuration, owner, or durable record.
    #[error("invalid leader lease: {0}")]
    Invalid(String),
    /// A newer durable authority term superseded an exact renewal.
    #[error("leader lease fenced: {0}")]
    Fenced(String),
    /// JSON encoding or decoding failure.
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
}

/// Failure while using the leader sequence as the cluster checkpoint-decision authority.
#[derive(Debug, thiserror::Error)]
pub enum ClusterCheckpointAuthorityError {
    /// Cluster runtime did not wire the durable leader authority.
    #[error("cluster checkpoint authority is not installed")]
    NotConfigured,
    /// Shared append-only authority failed.
    #[error("leader authority: {0}")]
    Authority(#[from] LeaseError),
    /// Checkpoint metadata or content-addressed recovery state was invalid.
    #[error("checkpoint decision: {0}")]
    Decision(#[from] DecisionError),
    /// The supplied proof no longer names the exact durable leader term.
    #[error("cluster checkpoint decision was fenced by a different durable leader term")]
    Fenced,
}

/// Exact continuity retained when old cluster checkpoint outcomes are compacted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterOutcomeRetentionBoundary {
    /// Checkpoint artifacts below this epoch are eligible for garbage collection.
    pub artifact_before_epoch: u64,
    /// Terminal outcome links below this epoch have been compacted.
    pub terminal_before_epoch: u64,
    /// Greatest committed outcome below the artifact-retention horizon.
    pub committed_anchor: Option<CheckpointOutcome>,
    /// Greatest terminal outcome compacted from the authority chain, including aborts.
    pub terminal_anchor: Option<CheckpointOutcome>,
}

impl ClusterOutcomeRetentionBoundary {
    fn from_floor(floor: Option<&AuthorityOutcomeFloor>) -> Self {
        floor.map_or(
            Self {
                artifact_before_epoch: 0,
                terminal_before_epoch: 0,
                committed_anchor: None,
                terminal_anchor: None,
            },
            |floor| Self {
                artifact_before_epoch: floor.artifact_before_epoch,
                terminal_before_epoch: floor.authority_before_epoch,
                committed_anchor: floor.committed_anchor.clone(),
                terminal_anchor: floor.terminal_anchor.clone(),
            },
        )
    }
}

/// Live cluster outcomes and their retention horizons from one audited authority head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterOutcomeInventory {
    /// Outcomes at or above the artifact-retention horizon, in ascending epoch order.
    pub outcomes: Vec<CheckpointOutcome>,
    /// Artifact and terminal-history horizons paired with `outcomes`.
    pub retention_boundary: ClusterOutcomeRetentionBoundary,
}

/// Append-only object-store authority for the cluster leader.
pub struct LeaderLeaseStore {
    store: Arc<dyn ObjectStore>,
    ttl_ms: i64,
    prune_running: Arc<AtomicBool>,
    outcome_audit_cache: Mutex<Option<ClusterOutcomeAuditCache>>,
    outcome_audit_flights: Mutex<Vec<ClusterOutcomeAuditFlight>>,
}

#[derive(Clone, PartialEq, Eq)]
struct ClusterOutcomeAuditKey {
    terminal_head: Option<OutcomeLink>,
    commit_head: Option<OutcomeLink>,
    floor: Option<AuthorityOutcomeFloor>,
}

struct ClusterOutcomeAuditCache {
    key: ClusterOutcomeAuditKey,
    authority_sequence: u64,
    snapshot: ClusterOutcomeAuditSnapshot,
}

#[derive(Clone)]
struct ClusterOutcomeAuditSnapshot {
    outcomes: Arc<[CheckpointOutcome]>,
    terminal_links: Arc<[OutcomeLink]>,
    commit_links: Arc<[OutcomeLink]>,
}

struct ClusterOutcomeAuditFlight {
    key: ClusterOutcomeAuditKey,
    gate: Weak<tokio::sync::Mutex<()>>,
}

struct PruneLatchGuard(Arc<AtomicBool>);

impl Drop for PruneLatchGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl std::fmt::Debug for LeaderLeaseStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LeaderLeaseStore")
            .field("ttl_ms", &self.ttl_ms)
            .finish_non_exhaustive()
    }
}

impl LeaderLeaseStore {
    /// Create a leader lease authority.
    ///
    /// The store must provide linearizable `PutMode::Create`/`Update` and GET ETag or version
    /// metadata; unsupported conditional updates fail closed.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, ttl_ms: i64) -> Self {
        Self {
            store,
            ttl_ms,
            prune_running: Arc::new(AtomicBool::new(false)),
            outcome_audit_cache: Mutex::new(None),
            outcome_audit_flights: Mutex::new(Vec::new()),
        }
    }

    fn recovery_fault_inventory_from(record: &LeaderAuthorityRecord) -> RecoveryFaultInventory {
        RecoveryFaultInventory {
            revision: record.recovery_fault_revision,
            faults: record
                .recovery_fault_slots
                .iter()
                .filter(|slot| slot.active)
                .map(AuthorityRecoveryFaultSlot::fault)
                .collect(),
        }
    }

    pub(crate) async fn recovery_fault_inventory(
        &self,
    ) -> Result<RecoveryFaultInventory, ClusterCheckpointAuthorityError> {
        let record = self
            .load_record()
            .await?
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        Ok(Self::recovery_fault_inventory_from(&record))
    }

    pub(crate) async fn record_recovery_fault(
        &self,
        publisher: RecoveryFaultPublisher,
        request_sequence: u64,
    ) -> Result<RecordRecoveryFaultResult, ClusterCheckpointAuthorityError> {
        publisher.validate().map_err(LeaseError::Invalid)?;
        if request_sequence == 0 {
            return Err(LeaseError::Invalid(
                "recovery fault request sequence must be nonzero".into(),
            )
            .into());
        }

        loop {
            let current = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let node_id = publisher.participant.node_id;
            let slot_index = current
                .recovery_fault_slots
                .binary_search_by_key(&node_id, |slot| slot.publisher.participant.node_id);
            let (insert_at, replace) = match slot_index {
                Ok(index) => {
                    let slot = &current.recovery_fault_slots[index];
                    if slot.matches_request(publisher, request_sequence) {
                        return Ok(if slot.active {
                            RecordRecoveryFaultResult::Active
                        } else {
                            RecordRecoveryFaultResult::AlreadyCleared
                        });
                    }
                    if publisher.process_term < slot.publisher.process_term {
                        return Ok(RecordRecoveryFaultResult::Superseded);
                    }
                    if publisher.process_term == slot.publisher.process_term
                        && publisher.participant.boot_incarnation
                            != slot.publisher.participant.boot_incarnation
                    {
                        return Err(LeaseError::Invalid(format!(
                            "stable node {node_id} has two recovery fault publishers for process term {}",
                            publisher.process_term
                        ))
                        .into());
                    }
                    if publisher.process_term == slot.publisher.process_term
                        && request_sequence < slot.request_sequence
                    {
                        return Ok(RecordRecoveryFaultResult::CoveredByNewerRequest);
                    }
                    (index, true)
                }
                Err(index) => (index, false),
            };

            let sequence =
                current.lease.seq.checked_add(1).ok_or_else(|| {
                    LeaseError::Invalid("leader authority sequence exhausted".into())
                })?;
            let mut lease = current.lease.clone();
            lease.seq = sequence;
            let mut candidate = current.preserve_with_lease(lease);
            let slot = AuthorityRecoveryFaultSlot {
                publisher,
                request_sequence,
                // Authority sequence is globally monotonic even after an obsolete tombstone is
                // compacted. A delayed retry can therefore become a conservative fresh fault,
                // but can never alias a sequence already remembered by a recovery monitor.
                fault_sequence: sequence,
                active: true,
            };
            if replace {
                candidate.recovery_fault_slots[insert_at] = slot;
            } else {
                candidate.recovery_fault_slots.insert(insert_at, slot);
            }
            candidate.recovery_fault_revision = sequence;
            candidate.validate()?;

            match self.create_authority_record(&candidate).await? {
                AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                    return Ok(RecordRecoveryFaultResult::Active);
                }
                AuthorityCreateOutcome::Contended(_) => tokio::task::yield_now().await,
            }
        }
    }

    pub(crate) async fn authorize_recovery_release(
        &self,
        clearer: RecoveryFaultPublisher,
        terminal: &RecoveryAnnouncement,
    ) -> Result<bool, ClusterCheckpointAuthorityError> {
        clearer.validate().map_err(LeaseError::Invalid)?;
        let (_, terminal_reference) = encode_recovery_release_terminal(terminal)?;
        let reporter = NodeId(clearer.participant.node_id);
        let expected = terminal.round.fault_sequence(reporter);
        let current = self
            .load_record()
            .await?
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        let Some(release_head) = current.recovery_release_head.as_ref() else {
            return Ok(false);
        };
        if release_head.terminal != terminal_reference
            || current.recovery_fault_revision != release_head.sequence
        {
            return Ok(false);
        }
        if current.recovery_fault_slots.iter().any(|slot| slot.active) {
            return Ok(false);
        }
        let slot_index = current
            .recovery_fault_slots
            .binary_search_by_key(&reporter.0, |slot| slot.publisher.participant.node_id);
        let Some(expected_sequence) = expected else {
            return Ok(match slot_index {
                Ok(index) => !current.recovery_fault_slots[index].active,
                Err(_) => true,
            });
        };
        let Ok(index) = slot_index else {
            return Ok(false);
        };
        let slot = &current.recovery_fault_slots[index];
        Ok(slot.publisher == clearer && slot.fault_sequence == expected_sequence && !slot.active)
    }

    pub(crate) async fn stage_recovery_release_terminal(
        &self,
        terminal: &RecoveryAnnouncement,
    ) -> Result<RecoveryReleaseTerminalRef, ClusterCheckpointAuthorityError> {
        let (encoded, reference) = encode_recovery_release_terminal(terminal)?;
        let path = recovery_release_terminal_path(&reference);
        let put_error = match self
            .store
            .put_opts(
                &path,
                PutPayload::from(encoded),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
        {
            Ok(_)
            | Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => None,
            Err(error) => Some(error),
        };
        match self.load_recovery_release_terminal(&reference).await {
            Ok(stored) if stored == *terminal => Ok(reference),
            Ok(_) => Err(LeaseError::Invalid(format!(
                "recovery release terminal '{}' differs from the proposed content",
                reference.sha256
            ))
            .into()),
            Err(reconcile_error) => {
                if let Some(put_error) = put_error {
                    Err(LeaseError::Io(format!(
                        "recovery release terminal write failed ({put_error}); reconciliation failed ({reconcile_error})"
                    ))
                    .into())
                } else {
                    Err(reconcile_error)
                }
            }
        }
    }

    pub(crate) async fn load_recovery_release_terminal(
        &self,
        reference: &RecoveryReleaseTerminalRef,
    ) -> Result<RecoveryAnnouncement, ClusterCheckpointAuthorityError> {
        reference.validate()?;
        let result = match self
            .store
            .get(&recovery_release_terminal_path(reference))
            .await
        {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(LeaseError::Invalid(format!(
                    "recovery release terminal '{}' is missing",
                    reference.sha256
                ))
                .into());
            }
            Err(error) => return Err(LeaseError::Io(error.to_string()).into()),
        };
        if result.meta.size != reference.encoded_len {
            return Err(LeaseError::Invalid(format!(
                "recovery release terminal '{}' is {} bytes, expected {}",
                reference.sha256, result.meta.size, reference.encoded_len
            ))
            .into());
        }
        let bytes = result
            .bytes()
            .await
            .map_err(|error| LeaseError::Io(error.to_string()))?;
        if u64::try_from(bytes.len()).ok() != Some(reference.encoded_len) {
            return Err(LeaseError::Invalid(format!(
                "recovery release terminal '{}' payload length changed while reading",
                reference.sha256
            ))
            .into());
        }
        let terminal: RecoveryAnnouncement = serde_json::from_slice(&bytes).map_err(|error| {
            LeaseError::Invalid(format!(
                "recovery release terminal '{}': {error}",
                reference.sha256
            ))
        })?;
        let (canonical, actual_reference) = encode_recovery_release_terminal(&terminal)?;
        if &actual_reference != reference || canonical.as_ref() != bytes.as_ref() {
            return Err(LeaseError::Invalid(format!(
                "recovery release terminal '{}' does not match its content-addressed reference",
                reference.sha256
            ))
            .into());
        }
        Ok(terminal)
    }

    pub(crate) async fn record_recovery_release_commit(
        &self,
        proof: &LeaderProof,
        reference: RecoveryReleaseTerminalRef,
    ) -> Result<RecordRecoveryReleaseCommitResult, ClusterCheckpointAuthorityError> {
        reference.validate()?;
        let terminal = self.load_recovery_release_terminal(&reference).await?;
        if &terminal.round.leader_proof != proof || !proof.is_canonical() {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }

        loop {
            let current = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            if let Some(winner) = current.recovery_release_head.as_ref() {
                if winner.terminal == reference
                    || winner.terminal.generation() >= reference.generation()
                {
                    self.load_recovery_release_terminal(&winner.terminal)
                        .await?;
                    return if winner.terminal == reference {
                        Ok(RecordRecoveryReleaseCommitResult::Unchanged(reference))
                    } else {
                        Ok(RecordRecoveryReleaseCommitResult::Conflict {
                            winner: winner.terminal.clone(),
                        })
                    };
                }
            }
            if !current.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            let fault_inventory = Self::recovery_fault_inventory_from(&current);
            if fault_inventory.revision != terminal.round.fault_revision()
                || fault_inventory.faults != terminal.round.faults
            {
                return Ok(RecordRecoveryReleaseCommitResult::FaultsChanged);
            }

            let base_sequence = current.lease.seq;
            let sequence = base_sequence
                .checked_add(1)
                .ok_or_else(|| LeaseError::Invalid("leader authority sequence exhausted".into()))?;
            let mut candidate = current.preserve_with_lease(LeaderLease {
                seq: sequence,
                renewal_sequence: current.lease.renewal_sequence,
                token: current.lease.token,
                owner: current.lease.owner.clone(),
                expires_at_ms: current.lease.expires_at_ms,
                catalog_manifest: current.lease.catalog_manifest.clone(),
            });
            // Only a process frozen into the stopped roster may consume this terminal. Covered
            // unavailable publishers remain fenced and conservatively republish if they return.
            candidate.recovery_fault_slots.retain(|slot| {
                slot.active
                    && terminal
                        .round
                        .stopped_participant_incarnation(NodeId(slot.publisher.participant.node_id))
                        == Some(slot.publisher.participant.boot_incarnation)
            });
            for slot in &mut candidate.recovery_fault_slots {
                slot.active = false;
            }
            candidate.recovery_fault_revision = sequence;
            candidate.recovery_release_commit = Some(AuthorityRecoveryReleaseCommit {
                terminal: reference.clone(),
                leader_proof: proof.clone(),
            });
            candidate.recovery_release_head = Some(RecoveryReleaseLink {
                sequence,
                terminal: reference.clone(),
            });
            candidate.validate()?;

            match self.create_authority_record(&candidate).await? {
                AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                    return Ok(RecordRecoveryReleaseCommitResult::Created(reference));
                }
                AuthorityCreateOutcome::Contended(winner_head) => {
                    if let Some(winner) = winner_head.recovery_release_head.as_ref() {
                        if winner.terminal == reference
                            || winner.terminal.generation() >= reference.generation()
                        {
                            self.load_recovery_release_terminal(&winner.terminal)
                                .await?;
                            return if winner.terminal == reference {
                                Ok(RecordRecoveryReleaseCommitResult::Unchanged(reference))
                            } else {
                                Ok(RecordRecoveryReleaseCommitResult::Conflict {
                                    winner: winner.terminal.clone(),
                                })
                            };
                        }
                    }
                    if !winner_head.lease.matches_proof(proof) {
                        return Err(ClusterCheckpointAuthorityError::Fenced);
                    }
                    if winner_head.lease.seq <= base_sequence {
                        return Err(LeaseError::Invalid(
                            "recovery release authority contention did not advance the sequence"
                                .into(),
                        )
                        .into());
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    async fn recovery_release_terminal_from(
        &self,
        head: &LeaderAuthorityRecord,
        link: &RecoveryReleaseLink,
    ) -> Result<RecoveryAnnouncement, ClusterCheckpointAuthorityError> {
        let admission = if link.sequence == head.lease.seq {
            head.clone()
        } else {
            read_authority_record(self.store.as_ref(), link.sequence)
                .await?
                .ok_or_else(|| {
                    LeaseError::Invalid(
                        "recovery release admission is missing from retained authority history"
                            .into(),
                    )
                })?
        };
        let commit = admission
            .recovery_release_commit
            .as_ref()
            .filter(|commit| {
                commit.terminal == link.terminal
                    && admission.recovery_release_head.as_ref() == Some(link)
            })
            .ok_or_else(|| {
                LeaseError::Invalid(
                    "recovery release head does not match its admitting authority record".into(),
                )
            })?;
        let terminal = self.load_recovery_release_terminal(&link.terminal).await?;
        if terminal.round.leader_proof != commit.leader_proof {
            return Err(LeaseError::Invalid(
                "recovery release terminal does not match its admitting leader proof".into(),
            )
            .into());
        }
        Ok(terminal)
    }

    pub(crate) async fn recovery_admission_snapshot(
        &self,
    ) -> Result<RecoveryAdmissionSnapshot, ClusterCheckpointAuthorityError> {
        for _ in 0..MAX_LEASE_HEAD_READ_ATTEMPTS {
            let head = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let Some(link) = head.recovery_release_head.clone() else {
                return Ok(RecoveryAdmissionSnapshot {
                    committed_release: None,
                    fault_inventory: Self::recovery_fault_inventory_from(&head),
                    authority_sequence: head.lease.seq,
                    release_head: None,
                });
            };
            let terminal = self.recovery_release_terminal_from(&head, &link).await;
            let rechecked = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            if rechecked.lease.seq >= head.lease.seq
                && rechecked.recovery_release_head == head.recovery_release_head
                && rechecked.recovery_fault_revision == head.recovery_fault_revision
                && rechecked.recovery_fault_slots == head.recovery_fault_slots
            {
                return Ok(RecoveryAdmissionSnapshot {
                    committed_release: Some(terminal?),
                    fault_inventory: Self::recovery_fault_inventory_from(&rechecked),
                    authority_sequence: rechecked.lease.seq,
                    release_head: rechecked.recovery_release_head,
                });
            }
            tokio::task::yield_now().await;
        }
        Err(LeaseError::Io(format!(
            "recovery admission changed during {MAX_LEASE_HEAD_READ_ATTEMPTS} read attempts"
        ))
        .into())
    }

    pub(crate) async fn recovery_admission_is_current(
        &self,
        snapshot: &RecoveryAdmissionSnapshot,
        leader_proof: &LeaderProof,
    ) -> Result<bool, ClusterCheckpointAuthorityError> {
        let current = self
            .load_record()
            .await?
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        let inventory = Self::recovery_fault_inventory_from(&current);
        Ok(current.lease.matches_proof(leader_proof)
            && current.lease.seq >= snapshot.authority_sequence
            && current.recovery_release_head == snapshot.release_head
            && inventory == snapshot.fault_inventory
            && inventory.faults().is_empty())
    }

    pub(crate) async fn latest_recovery_release_terminal(
        &self,
    ) -> Result<Option<RecoveryAnnouncement>, ClusterCheckpointAuthorityError> {
        for _ in 0..MAX_LEASE_HEAD_READ_ATTEMPTS {
            let Some(head) = self.load_record().await? else {
                return Ok(None);
            };
            let Some(link) = head.recovery_release_head.clone() else {
                return Ok(None);
            };
            let terminal = self.recovery_release_terminal_from(&head, &link).await;
            let rechecked = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            if rechecked.recovery_release_head.as_ref() == Some(&link) {
                return Ok(Some(terminal?));
            }
            tokio::task::yield_now().await;
        }
        Err(LeaseError::Io(format!(
            "recovery release head changed during {MAX_LEASE_HEAD_READ_ATTEMPTS} read attempts"
        ))
        .into())
    }

    fn ttl(&self) -> Result<Duration, LeaseError> {
        let ttl = u64::try_from(self.ttl_ms)
            .map_err(|_| LeaseError::Invalid("lease TTL must be positive".into()))?;
        if ttl == 0 {
            return Err(LeaseError::Invalid("lease TTL must be positive".into()));
        }
        Ok(Duration::from_millis(ttl))
    }

    fn diagnostic_expiry(&self, now_ms: i64) -> Result<i64, LeaseError> {
        now_ms
            .checked_add(self.ttl_ms)
            .ok_or_else(|| LeaseError::Invalid("diagnostic lease expiry overflow".into()))
    }

    #[cfg(test)]
    async fn list_seqs(&self) -> Result<Vec<u64>, LeaseError> {
        let prefix = OsPath::from(LEASE_PREFIX);
        let mut entries = self.store.list(Some(&prefix));
        let mut sequences = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|error| LeaseError::Io(error.to_string()))?;
            if sequences.len() == MAX_TEST_LEADER_LEASE_RECORDS {
                return Err(LeaseError::Invalid(format!(
                    "test leader history exceeds {MAX_TEST_LEADER_LEASE_RECORDS} records"
                )));
            }
            sequences.push(lease_sequence_from_path(&entry.location)?);
        }
        sequences.sort_unstable();
        sequences.dedup();
        Ok(sequences)
    }

    fn schedule_history_prune(&self) {
        if self
            .prune_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.prune_running.store(false, Ordering::Release);
            return;
        };
        let store = Arc::clone(&self.store);
        let prune_running = Arc::clone(&self.prune_running);
        let grace_ms = self.ttl_ms.saturating_mul(2).max(1);
        runtime.spawn(async move {
            let _latch = PruneLatchGuard(prune_running);
            match tokio::time::timeout(
                LEADER_LEASE_PRUNE_TIMEOUT,
                Self::prune_history(&store, grace_ms),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(%error, "leader lease history prune failed");
                }
                Err(_) => {
                    tracing::warn!(
                        timeout = ?LEADER_LEASE_PRUNE_TIMEOUT,
                        "leader lease history prune timed out"
                    );
                }
            }
        });
    }

    async fn prune_history(store: &Arc<dyn ObjectStore>, grace_ms: i64) -> Result<(), LeaseError> {
        let authority = Self::new(Arc::clone(store), 1);
        let Some(head) = authority.load_record().await? else {
            return Ok(());
        };
        let head_sequence = head.lease.seq;
        let mut retained = BTreeSet::from([head_sequence]);
        if let Some(previous) = head_sequence
            .checked_sub(1)
            .filter(|sequence| *sequence != 0)
        {
            retained.insert(previous);
        }
        let floor = head
            .outcome_floor
            .as_ref()
            .map_or(0, |floor| floor.authority_before_epoch);
        let artifact_floor = head
            .outcome_floor
            .as_ref()
            .map_or(0, |floor| floor.artifact_before_epoch);
        let mut link = head.outcome_head;
        let mut outcome_links = 0;
        let mut terminal_commit_links = BTreeSet::new();
        let mut expected_commit_head = head.commit_head;
        while let Some(current) = link {
            if current.epoch < floor {
                if head
                    .outcome_floor
                    .as_ref()
                    .and_then(|floor| floor.terminal_anchor_link)
                    != Some(current)
                {
                    return Err(LeaseError::Invalid(
                        "outcome floor does not anchor the retained authority chain".into(),
                    ));
                }
                break;
            }
            if !consume_live_authority_link(&mut outcome_links) {
                return Err(LeaseError::Invalid(format!(
                    "live outcome retention exceeds the fixed {MAX_LIVE_AUTHORITY_LINKS}-link authority bound"
                )));
            }
            let outcome_record = read_authority_record(store.as_ref(), current.sequence)
                .await?
                .filter(|record| {
                    record.outcome_head == Some(current)
                        && record
                            .checkpoint_outcome
                            .as_ref()
                            .map(|outcome| (outcome.epoch, outcome.checkpoint_id))
                            == Some((current.epoch, current.checkpoint_id))
                })
                .ok_or_else(|| {
                    LeaseError::Invalid("retained outcome authority chain is broken".into())
                })?;
            if outcome_record.commit_head != expected_commit_head {
                return Err(LeaseError::Invalid(
                    "retained outcome chain did not preserve the exact Commit head".into(),
                ));
            }
            retained.insert(current.sequence);
            if outcome_record
                .checkpoint_outcome
                .as_ref()
                .is_some_and(CheckpointOutcome::is_commit)
            {
                terminal_commit_links.insert(current);
                expected_commit_head = outcome_record.previous_commit;
            } else {
                expected_commit_head = outcome_record.commit_head;
            }
            link = outcome_record.previous_outcome;
        }
        let mut commit_link = head.commit_head;
        let mut commit_links = 0;
        let mut retained_commit_links = BTreeSet::new();
        while let Some(current) = commit_link {
            if current.epoch < artifact_floor {
                break;
            }
            if !consume_live_authority_link(&mut commit_links) {
                return Err(LeaseError::Invalid(format!(
                    "live Commit retention exceeds the fixed {MAX_LIVE_AUTHORITY_LINKS}-link authority bound"
                )));
            }
            let commit_record = read_authority_record(store.as_ref(), current.sequence)
                .await?
                .filter(|record| {
                    record.commit_head == Some(current)
                        && record.checkpoint_outcome.as_ref().is_some_and(|outcome| {
                            outcome.is_commit()
                                && outcome.epoch == current.epoch
                                && outcome.checkpoint_id == current.checkpoint_id
                        })
                })
                .ok_or_else(|| {
                    LeaseError::Invalid("retained Commit authority chain is broken".into())
                })?;
            retained.insert(current.sequence);
            retained_commit_links.insert(current);
            commit_link = commit_record.previous_commit;
        }
        match (
            commit_link,
            head.outcome_floor
                .as_ref()
                .and_then(|floor| floor.committed_anchor_link),
            head.outcome_floor
                .as_ref()
                .and_then(|floor| floor.committed_anchor.as_ref()),
        ) {
            (Some(link), Some(anchor_link), Some(anchor))
                if link == anchor_link
                    && link.epoch == anchor.epoch
                    && link.checkpoint_id == anchor.checkpoint_id => {}
            (None, None, None) => {}
            _ => {
                return Err(LeaseError::Invalid(
                    "Commit chain does not meet the retained artifact-floor anchor".into(),
                ));
            }
        }
        let boundary_commit_head = head
            .outcome_floor
            .as_ref()
            .and_then(|floor| floor.terminal_anchor.as_ref())
            .and_then(|anchor| {
                retained_commit_links
                    .iter()
                    .rev()
                    .find(|link| link.epoch <= anchor.epoch)
                    .copied()
                    .or_else(|| {
                        head.outcome_floor
                            .as_ref()
                            .and_then(|floor| floor.committed_anchor_link)
                    })
            });
        if expected_commit_head != boundary_commit_head {
            return Err(LeaseError::Invalid(
                "retained terminal chain lost Commit continuity at its durable floor".into(),
            ));
        }
        if !terminal_commit_links.is_subset(&retained_commit_links) {
            return Err(LeaseError::Invalid(
                "terminal Commit records are not linked from the retained Commit chain".into(),
            ));
        }
        if let Some((anchor, anchor_link)) = head.outcome_floor.as_ref().and_then(|floor| {
            floor
                .terminal_anchor
                .as_ref()
                .zip(floor.terminal_anchor_link)
        }) {
            if anchor.is_commit() {
                let linked = if anchor.epoch >= artifact_floor {
                    retained_commit_links.contains(&anchor_link)
                } else {
                    head.outcome_floor.as_ref().is_some_and(|floor| {
                        floor.committed_anchor.as_ref() == Some(anchor)
                            && floor.committed_anchor_link == Some(anchor_link)
                    })
                };
                if !linked {
                    return Err(LeaseError::Invalid(
                        "terminal Commit anchor is not linked from the retained Commit chain"
                            .into(),
                    ));
                }
            }
        }
        let mut assignment_link = head.assignment_decision_head;
        let mut assignment_links = 0;
        while let Some(current) = assignment_link {
            if !consume_live_authority_link(&mut assignment_links) {
                return Err(LeaseError::Invalid(format!(
                    "live assignment-decision retention exceeds the fixed {MAX_LIVE_AUTHORITY_LINKS}-link authority bound"
                )));
            }
            if head
                .assignment_decision_floor
                .as_ref()
                .is_some_and(|floor| current.target_version < floor.before_target_version)
            {
                if head
                    .assignment_decision_floor
                    .as_ref()
                    .and_then(|floor| floor.terminal_anchor_link)
                    != Some(current)
                {
                    return Err(LeaseError::Invalid(
                        "assignment-decision floor does not anchor the retained authority chain"
                            .into(),
                    ));
                }
                break;
            }
            let decision_record = read_authority_record(store.as_ref(), current.sequence)
                .await?
                .filter(|record| {
                    record.assignment_decision_head == Some(current)
                        && record
                            .assignment_decision
                            .as_ref()
                            .map(AuthorityAssignmentDecision::target_version)
                            == Some(current.target_version)
                })
                .ok_or_else(|| {
                    LeaseError::Invalid(
                        "retained assignment-decision authority chain is broken".into(),
                    )
                })?;
            retained.insert(current.sequence);
            assignment_link = decision_record.previous_assignment_decision;
        }
        let retained_release = match head.recovery_release_head.as_ref() {
            Some(link) => {
                let admission = read_authority_record(store.as_ref(), link.sequence)
                    .await?
                    .filter(|record| {
                        record.recovery_release_head.as_ref() == Some(link)
                            && record
                                .recovery_release_commit
                                .as_ref()
                                .is_some_and(|commit| commit.terminal == link.terminal)
                    })
                    .ok_or_else(|| {
                        LeaseError::Invalid(
                            "retained recovery release authority link is broken".into(),
                        )
                    })?;
                if admission.lease.seq != link.sequence {
                    return Err(LeaseError::Invalid(
                        "recovery release admission sequence does not match its retained link"
                            .into(),
                    ));
                }
                let terminal = authority
                    .load_recovery_release_terminal(&link.terminal)
                    .await
                    .map_err(|error| LeaseError::Invalid(error.to_string()))?;
                if admission
                    .recovery_release_commit
                    .as_ref()
                    .is_none_or(|commit| terminal.round.leader_proof != commit.leader_proof)
                {
                    return Err(LeaseError::Invalid(
                        "retained recovery release terminal does not match its admission".into(),
                    ));
                }
                retained.insert(link.sequence);
                Some(link.terminal.clone())
            }
            None => None,
        };

        let mut history_exhausted = false;
        for _ in 0..LEADER_LEASE_MAX_PRUNE_BATCHES {
            let (candidates, exhausted) =
                Self::prune_candidates(store, &retained, head_sequence, grace_ms).await?;
            if candidates.is_empty() {
                history_exhausted = true;
                break;
            }
            let deletions =
                futures::stream::iter(candidates.into_iter().map(Ok::<_, object_store::Error>));
            let mut results = store.delete_stream(Box::pin(deletions));
            while let Some(result) = results.next().await {
                if let Err(error) = result {
                    if !matches!(error, object_store::Error::NotFound { .. }) {
                        return Err(LeaseError::Io(error.to_string()));
                    }
                }
            }
            if exhausted {
                history_exhausted = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        if !history_exhausted {
            return Err(LeaseError::Io(
                "leader lease history still exceeds the bounded prune budget".into(),
            ));
        }
        Self::prune_recovery_release_terminals(store, retained_release.as_ref(), grace_ms).await
    }

    async fn prune_recovery_release_terminals(
        store: &Arc<dyn ObjectStore>,
        retained: Option<&RecoveryReleaseTerminalRef>,
        grace_ms: i64,
    ) -> Result<(), LeaseError> {
        let Some(retained) = retained else {
            return Ok(());
        };
        retained.validate()?;
        for _ in 0..RECOVERY_RELEASE_GC_MAX_BATCHES {
            let prefix = OsPath::from(RECOVERY_RELEASE_TERMINAL_PREFIX);
            let mut listed = store.list(Some(&prefix));
            let now = now_millis();
            let mut candidates = Vec::with_capacity(RECOVERY_RELEASE_GC_BATCH_RECORDS);
            let mut exhausted = true;
            while let Some(entry) = listed.next().await {
                let entry = entry.map_err(|error| LeaseError::Io(error.to_string()))?;
                let (generation, digest) = recovery_release_terminal_coordinates(&entry.location)?;
                let canonical = OsPath::from(format!(
                    "{RECOVERY_RELEASE_TERMINAL_PREFIX}generation={generation:020}/sha256={digest}.json"
                ));
                if canonical != entry.location {
                    return Err(LeaseError::Invalid(format!(
                        "noncanonical recovery release terminal path {}",
                        entry.location
                    )));
                }
                if entry.location == recovery_release_terminal_path(retained)
                    || generation > retained.generation()
                    || now.saturating_sub(entry.last_modified.timestamp_millis()) < grace_ms
                {
                    continue;
                }
                candidates.push(entry.location);
                if candidates.len() == RECOVERY_RELEASE_GC_BATCH_RECORDS {
                    exhausted = false;
                    break;
                }
            }
            if candidates.is_empty() {
                return Ok(());
            }
            let deletions =
                futures::stream::iter(candidates.into_iter().map(Ok::<_, object_store::Error>));
            let mut results = store.delete_stream(Box::pin(deletions));
            while let Some(result) = results.next().await {
                if let Err(error) = result {
                    if !matches!(error, object_store::Error::NotFound { .. }) {
                        return Err(LeaseError::Io(error.to_string()));
                    }
                }
            }
            if exhausted {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    async fn prune_candidates(
        store: &Arc<dyn ObjectStore>,
        retained: &BTreeSet<u64>,
        snapshot_head_sequence: u64,
        grace_ms: i64,
    ) -> Result<(Vec<OsPath>, bool), LeaseError> {
        let prefix = OsPath::from(LEASE_PREFIX);
        let mut listed = store.list(Some(&prefix));
        let mut candidates = Vec::with_capacity(LEADER_LEASE_PRUNE_BATCH_RECORDS);
        let now = now_millis();
        let mut exhausted = true;
        while let Some(entry) = listed.next().await {
            let entry = entry.map_err(|error| LeaseError::Io(error.to_string()))?;
            let sequence = lease_sequence_from_path(&entry.location)?;
            if sequence >= snapshot_head_sequence
                || retained.contains(&sequence)
                || now.saturating_sub(entry.last_modified.timestamp_millis()) < grace_ms
            {
                continue;
            }
            candidates.push(entry.location);
            if candidates.len() == LEADER_LEASE_PRUNE_BATCH_RECORDS {
                exhausted = false;
                break;
            }
        }
        Ok((candidates, exhausted))
    }

    /// Load the highest durable sequence.
    ///
    /// # Errors
    /// Fails closed on object-store I/O or malformed durable state.
    pub async fn load(&self) -> Result<Option<LeaderLease>, LeaseError> {
        Ok(self.load_record().await?.map(|record| record.lease))
    }

    async fn load_record(&self) -> Result<Option<LeaderAuthorityRecord>, LeaseError> {
        Ok(self
            .load_published_authority_head()
            .await?
            .map(|head| head.record))
    }

    async fn load_published_authority_head(
        &self,
    ) -> Result<Option<PublishedAuthorityHead>, LeaseError> {
        for attempt in 0..MAX_LEASE_HEAD_READ_ATTEMPTS {
            let Some(pointer) = read_authority_head_pointer(self.store.as_ref()).await? else {
                let Some(discovered) = self.discover_authority_head().await? else {
                    if read_authority_head_pointer(self.store.as_ref())
                        .await?
                        .is_none()
                    {
                        return Ok(None);
                    }
                    if attempt + 1 < MAX_LEASE_HEAD_READ_ATTEMPTS {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    break;
                };
                self.publish_authority_head(discovered.lease.seq, None)
                    .await?;
                if attempt + 1 < MAX_LEASE_HEAD_READ_ATTEMPTS {
                    tokio::task::yield_now().await;
                    continue;
                }
                break;
            };

            let sequence = pointer.pointer.sequence;
            let successor_sequence = sequence.checked_add(1);
            let (record, successor) = if let Some(successor_sequence) = successor_sequence {
                tokio::try_join!(
                    read_authority_record(self.store.as_ref(), sequence),
                    read_authority_record(self.store.as_ref(), successor_sequence)
                )?
            } else {
                (
                    read_authority_record(self.store.as_ref(), sequence).await?,
                    None,
                )
            };
            let Some(record) = record else {
                let rechecked = read_authority_head_pointer(self.store.as_ref())
                    .await?
                    .ok_or_else(|| {
                        LeaseError::Invalid(
                            "leader authority head disappeared while reading its target".into(),
                        )
                    })?;
                if rechecked.pointer.sequence > sequence {
                    if attempt + 1 < MAX_LEASE_HEAD_READ_ATTEMPTS {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    break;
                }
                if rechecked.pointer.sequence < sequence {
                    return Err(LeaseError::Invalid(format!(
                        "leader authority head regressed from sequence {sequence} to {}",
                        rechecked.pointer.sequence
                    )));
                }
                return Err(LeaseError::Invalid(format!(
                    "leader authority head points ahead to missing sequence {sequence}"
                )));
            };
            let Some(successor_sequence) = successor_sequence else {
                return Ok(Some(PublishedAuthorityHead { record, pointer }));
            };
            if successor.is_none() {
                return Ok(Some(PublishedAuthorityHead { record, pointer }));
            }

            if let Some(after_successor) = successor_sequence.checked_add(1) {
                if read_authority_record(self.store.as_ref(), after_successor)
                    .await?
                    .is_some()
                {
                    let rechecked = read_authority_head_pointer(self.store.as_ref())
                        .await?
                        .ok_or_else(|| {
                            LeaseError::Invalid(
                                "leader authority head disappeared while checking pointer lag"
                                    .into(),
                            )
                        })?;
                    if rechecked.pointer.sequence == sequence {
                        return Err(LeaseError::Invalid(format!(
                            "leader authority head at sequence {sequence} lags by more than one record"
                        )));
                    }
                    if rechecked.pointer.sequence < sequence {
                        return Err(LeaseError::Invalid(format!(
                            "leader authority head regressed from sequence {sequence} to {}",
                            rechecked.pointer.sequence
                        )));
                    }
                    if attempt + 1 < MAX_LEASE_HEAD_READ_ATTEMPTS {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    break;
                }
            }

            self.publish_authority_head(successor_sequence, Some(&pointer))
                .await?;
            if attempt + 1 < MAX_LEASE_HEAD_READ_ATTEMPTS {
                tokio::task::yield_now().await;
                continue;
            }
            break;
        }
        Err(LeaseError::Io(format!(
            "leader authority head changed during {MAX_LEASE_HEAD_READ_ATTEMPTS} read attempts"
        )))
    }

    async fn discover_authority_head(&self) -> Result<Option<LeaderAuthorityRecord>, LeaseError> {
        let prefix = OsPath::from(LEASE_PREFIX);
        let mut listed = self.store.list(Some(&prefix));
        let mut discovered = 0usize;
        let mut maximum = None;
        while let Some(entry) = listed.next().await {
            let entry = entry.map_err(|error| LeaseError::Io(error.to_string()))?;
            discovered = discovered.checked_add(1).ok_or_else(|| {
                LeaseError::Invalid("leader authority discovery count overflowed".into())
            })?;
            if discovered > MAX_AUTHORITY_HEAD_DISCOVERY_RECORDS {
                return Err(LeaseError::Invalid(format!(
                    "leader authority head discovery exceeds the fixed {MAX_AUTHORITY_HEAD_DISCOVERY_RECORDS}-record bound"
                )));
            }
            let sequence = lease_sequence_from_path(&entry.location)?;
            maximum = Some(maximum.map_or(sequence, |current: u64| current.max(sequence)));
        }
        let Some(sequence) = maximum else {
            return Ok(None);
        };
        read_authority_record(self.store.as_ref(), sequence)
            .await?
            .map(Some)
            .ok_or_else(|| {
                LeaseError::Io(format!(
                    "discovered leader authority sequence {sequence} vanished before publication"
                ))
            })
    }

    async fn publish_authority_head(
        &self,
        sequence: u64,
        expected: Option<&VersionedAuthorityHeadPointer>,
    ) -> Result<u64, LeaseError> {
        if let Some(expected) = expected {
            if expected.pointer.sequence.checked_add(1) != Some(sequence) {
                return Err(LeaseError::Invalid(
                    "leader authority head update is not the exact successor".into(),
                ));
            }
            if expected.update_version.e_tag.is_none() && expected.update_version.version.is_none()
            {
                return Err(LeaseError::Invalid(
                    "leader authority store did not provide a native conditional update version"
                        .into(),
                ));
            }
        }

        let options = PutOptions {
            mode: expected.map_or(PutMode::Create, |expected| {
                PutMode::Update(expected.update_version.clone())
            }),
            ..PutOptions::default()
        };
        let result = self
            .store
            .put_opts(
                &authority_head_path(),
                PutPayload::from(encode_authority_head_pointer(sequence)?),
                options,
            )
            .await;
        if result.is_ok() {
            return Ok(sequence);
        }
        let write_error = result
            .expect_err("successful authority head writes return above")
            .to_string();
        let current = read_authority_head_pointer(self.store.as_ref())
            .await?
            .ok_or_else(|| {
                LeaseError::Io(format!(
                    "leader authority head write failed ({write_error}) and no pointer was durable"
                ))
            })?;
        if let Some(expected) = expected {
            if current.pointer.sequence < expected.pointer.sequence {
                return Err(LeaseError::Invalid(format!(
                    "leader authority head regressed from sequence {} to {}",
                    expected.pointer.sequence, current.pointer.sequence
                )));
            }
        }
        if current.pointer.sequence >= sequence {
            if current.pointer.sequence > sequence {
                read_authority_record(self.store.as_ref(), current.pointer.sequence)
                    .await?
                    .ok_or_else(|| {
                        LeaseError::Invalid(format!(
                            "leader authority head points ahead to missing sequence {}",
                            current.pointer.sequence
                        ))
                    })?;
            }
            return Ok(current.pointer.sequence);
        }
        Err(LeaseError::Io(format!(
            "leader authority head write failed ({write_error}) and remained at sequence {}",
            current.pointer.sequence
        )))
    }

    async fn read_catalog_manifest_blob(
        &self,
        reference: &CatalogManifestRef,
    ) -> Result<(CatalogManifest, Bytes), CatalogManifestError> {
        reference.validate()?;
        let path = reference.object_path();
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(CatalogManifestError::Invalid(format!(
                    "catalog manifest blob '{}' is missing",
                    reference.sha256
                )));
            }
            Err(error) => {
                return Err(CatalogManifestError::Authority(LeaseError::Io(
                    error.to_string(),
                )));
            }
        };
        if result.meta.size != reference.encoded_len {
            return Err(CatalogManifestError::Invalid(format!(
                "catalog manifest blob '{}' is {} bytes, expected {}",
                reference.sha256, result.meta.size, reference.encoded_len
            )));
        }
        let bytes = result
            .bytes()
            .await
            .map_err(|error| CatalogManifestError::Authority(LeaseError::Io(error.to_string())))?;
        if u64::try_from(bytes.len()).ok() != Some(reference.encoded_len) {
            return Err(CatalogManifestError::Invalid(format!(
                "catalog manifest blob '{}' payload length changed while reading",
                reference.sha256
            )));
        }
        let manifest: CatalogManifest = serde_json::from_slice(&bytes)?;
        let (canonical, actual_reference) = manifest.encode_and_reference()?;
        if actual_reference != *reference || canonical.as_slice() != bytes.as_ref() {
            return Err(CatalogManifestError::Invalid(format!(
                "catalog manifest blob '{}' does not match its sealed reference",
                reference.sha256
            )));
        }
        Ok((manifest, bytes))
    }

    pub(super) async fn load_catalog_manifest(
        &self,
        reference: &CatalogManifestRef,
    ) -> Result<CatalogManifest, CatalogManifestError> {
        self.read_catalog_manifest_blob(reference)
            .await
            .map(|(manifest, _)| manifest)
    }

    async fn ensure_catalog_manifest_blob(
        &self,
        encoded: &[u8],
        reference: &CatalogManifestRef,
    ) -> Result<(), CatalogManifestError> {
        reference.validate()?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let path = reference.object_path();
        let payload = PutPayload::from(Bytes::copy_from_slice(encoded));
        let put_error = match self.store.put_opts(&path, payload, options).await {
            Ok(_)
            | Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => None,
            Err(error) => Some(error),
        };

        match self.read_catalog_manifest_blob(reference).await {
            Ok((_, stored)) if stored.as_ref() == encoded => Ok(()),
            Ok(_) => Err(CatalogManifestError::Invalid(format!(
                "catalog manifest blob '{}' differs from the proposed content",
                reference.sha256
            ))),
            Err(error) => {
                if let Some(put_error) = put_error {
                    Err(CatalogManifestError::Authority(LeaseError::Io(format!(
                        "catalog manifest write failed ({put_error}); reconciliation failed ({error})"
                    ))))
                } else {
                    Err(error)
                }
            }
        }
    }

    async fn create_authority_record(
        &self,
        candidate: &LeaderAuthorityRecord,
    ) -> Result<AuthorityCreateOutcome, LeaseError> {
        let encoded = encode_authority_record(candidate)?;
        let expected = match self.load_published_authority_head().await? {
            None => {
                if candidate.lease.seq != 1 {
                    return Err(LeaseError::Invalid(format!(
                        "cannot append authority sequence {} to an empty published namespace",
                        candidate.lease.seq
                    )));
                }
                None
            }
            Some(head) if head.record.lease.seq == candidate.lease.seq => {
                return if head.record == *candidate {
                    Ok(AuthorityCreateOutcome::ExistingIdentical)
                } else {
                    Ok(AuthorityCreateOutcome::Contended(head.record))
                };
            }
            Some(head) if head.record.lease.seq > candidate.lease.seq => {
                return Ok(AuthorityCreateOutcome::Contended(head.record));
            }
            Some(head) if head.record.lease.seq.checked_add(1) == Some(candidate.lease.seq) => {
                Some(head.pointer)
            }
            Some(head) => {
                return Err(LeaseError::Invalid(format!(
                    "cannot append authority sequence {} after published sequence {}",
                    candidate.lease.seq, head.record.lease.seq
                )));
            }
        };
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let result = self
            .store
            .put_opts(
                &lease_path(candidate.lease.seq),
                PutPayload::from(encoded),
                options,
            )
            .await;
        let created = result.is_ok();
        if let Err(error) = result {
            let Some(at_sequence) =
                read_authority_record(self.store.as_ref(), candidate.lease.seq).await?
            else {
                return Err(LeaseError::Io(format!(
                    "authority record {} write failed ({error}) and no record was durable",
                    candidate.lease.seq
                )));
            };
            if at_sequence != *candidate {
                let winner = self.load_record().await?.ok_or_else(|| {
                    LeaseError::Io("authority contender was not published or readable".into())
                })?;
                return Ok(AuthorityCreateOutcome::Contended(winner));
            }
        }

        let published_sequence = self
            .publish_authority_head(candidate.lease.seq, expected.as_ref())
            .await?;
        if published_sequence > candidate.lease.seq {
            if created {
                self.schedule_history_prune();
            }
            let winner = self
                .load_record()
                .await?
                .ok_or_else(|| LeaseError::Io("newer published authority head vanished".into()))?;
            return Ok(AuthorityCreateOutcome::Contended(winner));
        }
        self.schedule_history_prune();
        if created {
            Ok(AuthorityCreateOutcome::Created)
        } else {
            Ok(AuthorityCreateOutcome::ExistingIdentical)
        }
    }

    /// Seal the complete catalog by appending it to the exact durable leader term.
    ///
    /// Renewals that race this operation are retried under the same proof. A takeover either
    /// observes and preserves the seal or wins the next sequence and fences this writer.
    ///
    /// # Errors
    /// Rejects malformed inventories, divergent existing inventories, stale proofs, and durable
    /// storage failures.
    pub(super) async fn seal_catalog(
        &self,
        proof: &LeaderProof,
        manifest: &CatalogManifest,
    ) -> Result<CatalogSealOutcome, CatalogManifestError> {
        if !proof.is_canonical() {
            return Err(CatalogManifestError::Fenced);
        }
        let (encoded, reference) = manifest.encode_and_reference()?;

        let current = self
            .load_record()
            .await?
            .ok_or(CatalogManifestError::Fenced)?;
        if let Some(sealed) = &current.lease.catalog_manifest {
            let durable = self.load_catalog_manifest(sealed).await?;
            return if durable == *manifest {
                Ok(CatalogSealOutcome::ExistingIdentical)
            } else {
                Err(CatalogManifestError::Conflict)
            };
        }
        if !current.lease.matches_proof(proof) {
            return Err(CatalogManifestError::Fenced);
        }
        self.ensure_catalog_manifest_blob(&encoded, &reference)
            .await?;

        loop {
            let current = self
                .load_record()
                .await?
                .ok_or(CatalogManifestError::Fenced)?;
            if let Some(sealed) = &current.lease.catalog_manifest {
                let durable = self.load_catalog_manifest(sealed).await?;
                return if durable == *manifest {
                    Ok(CatalogSealOutcome::ExistingIdentical)
                } else {
                    Err(CatalogManifestError::Conflict)
                };
            }
            if !current.lease.matches_proof(proof) {
                return Err(CatalogManifestError::Fenced);
            }

            let base_sequence = current.lease.seq;
            let candidate_lease = LeaderLease {
                seq: current.lease.seq.checked_add(1).ok_or_else(|| {
                    CatalogManifestError::Authority(LeaseError::Invalid(
                        "lease sequence exhausted".into(),
                    ))
                })?,
                renewal_sequence: current.lease.renewal_sequence,
                token: current.lease.token,
                owner: current.lease.owner.clone(),
                expires_at_ms: current.lease.expires_at_ms,
                catalog_manifest: Some(reference.clone()),
            };
            let candidate = current.preserve_with_lease(candidate_lease);
            match self.create_authority_record(&candidate).await? {
                AuthorityCreateOutcome::Created => return Ok(CatalogSealOutcome::Created),
                AuthorityCreateOutcome::ExistingIdentical => {
                    return Ok(CatalogSealOutcome::ExistingIdentical);
                }
                AuthorityCreateOutcome::Contended(winner) => {
                    if let Some(sealed) = &winner.lease.catalog_manifest {
                        let durable = self.load_catalog_manifest(sealed).await?;
                        return if durable == *manifest {
                            Ok(CatalogSealOutcome::ExistingIdentical)
                        } else {
                            Err(CatalogManifestError::Conflict)
                        };
                    }
                    if !winner.lease.matches_proof(proof) {
                        return Err(CatalogManifestError::Fenced);
                    }
                    if winner.lease.seq <= base_sequence {
                        return Err(CatalogManifestError::Authority(LeaseError::Invalid(
                            "catalog seal contention did not advance the authority sequence".into(),
                        )));
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    async fn audited_cluster_outcomes_from(
        &self,
        head: &LeaderAuthorityRecord,
    ) -> Result<ClusterOutcomeAuditSnapshot, ClusterCheckpointAuthorityError> {
        let floor = head.outcome_floor.as_ref();
        let authority_before_epoch = floor.map_or(0, |floor| floor.authority_before_epoch);
        let artifact_before_epoch = floor.map_or(0, |floor| floor.artifact_before_epoch);
        let mut terminal_records: BTreeMap<u64, LeaderAuthorityRecord> = BTreeMap::new();
        let mut terminal_newest_first = Vec::new();
        let mut terminal_links = Vec::new();
        let mut link = head.outcome_head;
        let mut expected_commit_head = head.commit_head;
        let mut traversed = 0;
        while let Some(current) = link {
            if current.epoch < authority_before_epoch {
                break;
            }
            if !consume_live_authority_link(&mut traversed) {
                return Err(DecisionError::Conflict(format!(
                    "live outcome retention exceeds the fixed {MAX_LIVE_AUTHORITY_LINKS}-link authority bound"
                ))
                .into());
            }
            let record = if let Some(record) = terminal_records.get(&current.sequence) {
                record.clone()
            } else {
                let record = read_authority_record(self.store.as_ref(), current.sequence)
                    .await?
                    .ok_or_else(|| {
                        DecisionError::InventoryChanged(format!(
                            "cluster outcome authority record {} disappeared during audit",
                            current.sequence
                        ))
                    })?;
                terminal_records.insert(current.sequence, record.clone());
                record
            };
            let outcome = record.checkpoint_outcome.clone().ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "cluster outcome head epoch {} points to non-outcome authority record {}",
                    current.epoch, current.sequence
                ))
            })?;
            if record.outcome_head != Some(current)
                || outcome.epoch != current.epoch
                || outcome.checkpoint_id != current.checkpoint_id
            {
                return Err(DecisionError::Conflict(format!(
                    "cluster outcome link epoch {} sequence {} does not match its authority record",
                    current.epoch, current.sequence
                ))
                .into());
            }
            if record.commit_head != expected_commit_head {
                return Err(DecisionError::Conflict(format!(
                    "cluster outcome epoch {} did not preserve the exact Commit head",
                    outcome.epoch
                ))
                .into());
            }
            expected_commit_head = if outcome.is_commit() {
                record.previous_commit
            } else {
                record.commit_head
            };
            terminal_newest_first.push(outcome);
            terminal_links.push(current);
            link = record.previous_outcome;
        }

        if let Some(floor) = floor {
            match (
                link,
                floor.terminal_anchor_link,
                floor.terminal_anchor.as_ref(),
            ) {
                (Some(link), Some(anchor_link), Some(anchor))
                    if link == anchor_link
                        && link.epoch == anchor.epoch
                        && link.checkpoint_id == anchor.checkpoint_id => {}
                (None, None, None) => {}
                _ => {
                    return Err(DecisionError::Conflict(format!(
                        "cluster outcome chain does not meet durable floor {} at its terminal anchor",
                        floor.authority_before_epoch
                    ))
                    .into());
                }
            }
        } else if link.is_some() {
            return Err(DecisionError::Conflict(
                "cluster outcome chain stopped without a durable retention floor".into(),
            )
            .into());
        }

        let mut commit_newest_first = Vec::new();
        let mut commit_links = Vec::new();
        link = head.commit_head;
        traversed = 0;
        while let Some(current) = link {
            if current.epoch < artifact_before_epoch {
                break;
            }
            if !consume_live_authority_link(&mut traversed) {
                return Err(DecisionError::Conflict(format!(
                    "live Commit retention exceeds the fixed {MAX_LIVE_AUTHORITY_LINKS}-link authority bound"
                ))
                .into());
            }
            let record = if let Some(record) = terminal_records.get(&current.sequence) {
                record.clone()
            } else {
                read_authority_record(self.store.as_ref(), current.sequence)
                    .await?
                    .ok_or_else(|| {
                        DecisionError::InventoryChanged(format!(
                            "cluster Commit authority record {} disappeared during audit",
                            current.sequence
                        ))
                    })?
            };
            let outcome = record.checkpoint_outcome.clone().ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "cluster Commit head epoch {} points to non-outcome authority record {}",
                    current.epoch, current.sequence
                ))
            })?;
            if !outcome.is_commit()
                || record.commit_head != Some(current)
                || outcome.epoch != current.epoch
                || outcome.checkpoint_id != current.checkpoint_id
            {
                return Err(DecisionError::Conflict(format!(
                    "cluster Commit link epoch {} sequence {} does not match its authority record",
                    current.epoch, current.sequence
                ))
                .into());
            }
            commit_newest_first.push(outcome);
            commit_links.push(current);
            link = record.previous_commit;
        }

        if let Some(floor) = floor {
            match (
                link,
                floor.committed_anchor_link,
                floor.committed_anchor.as_ref(),
            ) {
                (Some(link), Some(anchor_link), Some(anchor))
                    if link == anchor_link
                        && link.epoch == anchor.epoch
                        && link.checkpoint_id == anchor.checkpoint_id => {}
                (None, None, None) => {}
                _ => {
                    return Err(DecisionError::Conflict(format!(
                        "cluster Commit chain does not meet durable artifact floor {} at its committed anchor",
                        floor.artifact_before_epoch
                    ))
                    .into());
                }
            }
        } else if link.is_some() {
            return Err(DecisionError::Conflict(
                "cluster Commit chain stopped without a durable retention floor".into(),
            )
            .into());
        }

        terminal_newest_first.reverse();
        terminal_links.reverse();
        commit_newest_first.reverse();
        commit_links.reverse();
        let boundary_commit_head = floor
            .and_then(|floor| floor.terminal_anchor.as_ref())
            .and_then(|anchor| {
                commit_links
                    .iter()
                    .rev()
                    .find(|link| link.epoch <= anchor.epoch)
                    .copied()
                    .or_else(|| floor.and_then(|floor| floor.committed_anchor_link))
            });
        if expected_commit_head != boundary_commit_head {
            return Err(DecisionError::Conflict(
                "cluster terminal chain does not preserve Commit continuity at its durable floor"
                    .into(),
            )
            .into());
        }
        for (outcome, terminal_link) in terminal_newest_first.iter().zip(&terminal_links) {
            if !outcome.is_commit() {
                continue;
            }
            let commit_link = commit_links
                .binary_search_by_key(&outcome.epoch, |link| link.epoch)
                .ok()
                .map(|index| commit_links[index]);
            if commit_link != Some(*terminal_link) {
                return Err(DecisionError::Conflict(format!(
                    "cluster Commit epoch {} is not linked identically from both durable outcome heads",
                    outcome.epoch
                ))
                .into());
            }
        }
        if let Some((terminal_anchor, terminal_anchor_link)) = floor.and_then(|floor| {
            floor
                .terminal_anchor
                .as_ref()
                .zip(floor.terminal_anchor_link)
        }) {
            if terminal_anchor.is_commit() {
                let commit_link = if terminal_anchor.epoch >= artifact_before_epoch {
                    commit_links
                        .binary_search_by_key(&terminal_anchor.epoch, |link| link.epoch)
                        .ok()
                        .map(|index| commit_links[index])
                } else {
                    floor.and_then(|floor| {
                        (floor.committed_anchor.as_ref() == Some(terminal_anchor))
                            .then_some(floor.committed_anchor_link)
                            .flatten()
                    })
                };
                if commit_link != Some(terminal_anchor_link) {
                    return Err(DecisionError::Conflict(format!(
                        "cluster terminal Commit anchor epoch {} is not linked identically from the Commit chain",
                        terminal_anchor.epoch
                    ))
                    .into());
                }
            }
        }
        let mut outcomes = Vec::with_capacity(
            terminal_newest_first
                .len()
                .saturating_add(commit_newest_first.len())
                .saturating_add(2),
        );
        outcomes.extend(floor.and_then(|floor| floor.committed_anchor.clone()));
        outcomes.extend(floor.and_then(|floor| floor.terminal_anchor.clone()));
        outcomes.extend(terminal_newest_first);
        outcomes.extend(commit_newest_first);
        outcomes.sort_unstable_by_key(|outcome| outcome.epoch);
        let mut merged: Vec<CheckpointOutcome> = Vec::with_capacity(outcomes.len());
        for outcome in outcomes {
            if let Some(previous) = merged.last() {
                if previous.epoch == outcome.epoch {
                    if previous != &outcome {
                        return Err(DecisionError::Conflict(format!(
                            "cluster outcome epoch {} has conflicting terminal and Commit-chain records",
                            outcome.epoch
                        ))
                        .into());
                    }
                    continue;
                }
            }
            merged.push(outcome);
        }

        let expected_deployment = CheckpointDecisionStore::new(Arc::clone(&self.store))
            .load_or_create_deployment_id()
            .await?;
        for outcome in &merged {
            if outcome.deployment_id != expected_deployment {
                return Err(DecisionError::Conflict(format!(
                    "cluster outcome epoch {} belongs to deployment {}, current deployment is {}",
                    outcome.epoch, outcome.deployment_id, expected_deployment
                ))
                .into());
            }
        }
        for pair in merged.windows(2) {
            let previous = &pair[0];
            let current = &pair[1];
            if current.epoch <= previous.epoch || current.checkpoint_id <= previous.checkpoint_id {
                return Err(DecisionError::Conflict(format!(
                    "cluster outcomes regress from epoch {} checkpoint {} to epoch {} checkpoint {}",
                    previous.epoch, previous.checkpoint_id, current.epoch, current.checkpoint_id
                ))
                .into());
            }
        }
        Ok(ClusterOutcomeAuditSnapshot {
            outcomes: Arc::from(merged),
            terminal_links: Arc::from(terminal_links),
            commit_links: Arc::from(commit_links),
        })
    }

    fn cluster_outcome_audit_key(head: &LeaderAuthorityRecord) -> ClusterOutcomeAuditKey {
        ClusterOutcomeAuditKey {
            terminal_head: head.outcome_head,
            commit_head: head.commit_head,
            floor: head.outcome_floor.clone(),
        }
    }

    fn cached_cluster_outcome_audit(
        &self,
        key: &ClusterOutcomeAuditKey,
        authority_sequence: u64,
    ) -> Option<ClusterOutcomeAuditSnapshot> {
        let mut cache = self.outcome_audit_cache.lock();
        let cached = cache.as_mut().filter(|cached| cached.key == *key)?;
        cached.authority_sequence = cached.authority_sequence.max(authority_sequence);
        Some(cached.snapshot.clone())
    }

    fn install_cluster_outcome_audit(
        &self,
        key: ClusterOutcomeAuditKey,
        authority_sequence: u64,
        snapshot: ClusterOutcomeAuditSnapshot,
    ) {
        let mut cache = self.outcome_audit_cache.lock();
        if cache.as_ref().is_none_or(|cached| {
            cached.authority_sequence < authority_sequence
                || (cached.authority_sequence == authority_sequence && cached.key == key)
        }) {
            *cache = Some(ClusterOutcomeAuditCache {
                key,
                authority_sequence,
                snapshot,
            });
        }
    }

    fn cluster_outcome_audit_gate(
        &self,
        key: &ClusterOutcomeAuditKey,
    ) -> Arc<tokio::sync::Mutex<()>> {
        let mut flights = self.outcome_audit_flights.lock();
        flights.retain(|flight| flight.gate.strong_count() != 0);
        if let Some(gate) = flights
            .iter()
            .find(|flight| flight.key == *key)
            .and_then(|flight| flight.gate.upgrade())
        {
            return gate;
        }
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        flights.push(ClusterOutcomeAuditFlight {
            key: key.clone(),
            gate: Arc::downgrade(&gate),
        });
        gate
    }

    async fn cached_audited_cluster_outcomes_from(
        &self,
        head: &LeaderAuthorityRecord,
    ) -> Result<ClusterOutcomeAuditSnapshot, ClusterCheckpointAuthorityError> {
        let key = Self::cluster_outcome_audit_key(head);
        if let Some(snapshot) = self.cached_cluster_outcome_audit(&key, head.lease.seq) {
            return Ok(snapshot);
        }

        let gate = self.cluster_outcome_audit_gate(&key);
        let _guard = gate.lock().await;
        if let Some(snapshot) = self.cached_cluster_outcome_audit(&key, head.lease.seq) {
            return Ok(snapshot);
        }

        let snapshot = self.audited_cluster_outcomes_from(head).await?;
        self.install_cluster_outcome_audit(key, head.lease.seq, snapshot.clone());
        Ok(snapshot)
    }

    fn outcomes_retained_by_floor(
        floor: &AuthorityOutcomeFloor,
        snapshot: &ClusterOutcomeAuditSnapshot,
    ) -> ClusterOutcomeAuditSnapshot {
        let outcomes = &snapshot.outcomes;
        let mut retained = Vec::with_capacity(outcomes.len());
        if let Some(anchor) = floor.committed_anchor.as_ref() {
            retained.push(anchor.clone());
        }
        if let Some(anchor) = floor.terminal_anchor.as_ref() {
            if retained.last() != Some(anchor) {
                retained.push(anchor.clone());
            }
        }
        retained.extend(
            outcomes
                .iter()
                .filter(|outcome| {
                    outcome.epoch >= floor.authority_before_epoch
                        || (outcome.is_commit() && outcome.epoch >= floor.artifact_before_epoch)
                })
                .cloned(),
        );
        retained.sort_unstable_by_key(|outcome| outcome.epoch);
        retained.dedup();
        ClusterOutcomeAuditSnapshot {
            outcomes: Arc::from(retained),
            terminal_links: Arc::from(
                snapshot
                    .terminal_links
                    .iter()
                    .filter(|link| link.epoch >= floor.authority_before_epoch)
                    .copied()
                    .collect::<Vec<_>>(),
            ),
            commit_links: Arc::from(
                snapshot
                    .commit_links
                    .iter()
                    .filter(|link| link.epoch >= floor.artifact_before_epoch)
                    .copied()
                    .collect::<Vec<_>>(),
            ),
        }
    }

    async fn build_cluster_outcome_floor(
        &self,
        current: &LeaderAuthorityRecord,
        snapshot: &ClusterOutcomeAuditSnapshot,
        artifact_before_epoch: u64,
        authority_before_epoch: u64,
    ) -> Result<AuthorityOutcomeFloor, ClusterCheckpointAuthorityError> {
        let terminal_anchor = snapshot
            .outcomes
            .iter()
            .rev()
            .find(|outcome| outcome.epoch < authority_before_epoch)
            .cloned();
        let terminal_anchor_link = match terminal_anchor.as_ref() {
            Some(anchor)
                if current
                    .outcome_floor
                    .as_ref()
                    .is_some_and(|floor| floor.terminal_anchor.as_ref() == Some(anchor)) =>
            {
                current
                    .outcome_floor
                    .as_ref()
                    .and_then(|floor| floor.terminal_anchor_link)
            }
            Some(anchor) => {
                let index = snapshot
                    .terminal_links
                    .binary_search_by_key(&anchor.epoch, |link| link.epoch)
                    .map_err(|_| {
                        DecisionError::Conflict(format!(
                            "cluster outcome epoch {} has no audited authority link",
                            anchor.epoch
                        ))
                    })?;
                let link = snapshot.terminal_links[index];
                if link.checkpoint_id != anchor.checkpoint_id {
                    return Err(DecisionError::Conflict(format!(
                        "cluster outcome epoch {} checkpoint {} does not match audited authority link checkpoint {}",
                        anchor.epoch, anchor.checkpoint_id, link.checkpoint_id
                    ))
                    .into());
                }
                Some(link)
            }
            None => None,
        };
        let committed_anchor = snapshot
            .outcomes
            .iter()
            .rev()
            .find(|outcome| outcome.epoch < artifact_before_epoch && outcome.is_commit())
            .cloned();
        let committed_anchor_link = match committed_anchor.as_ref() {
            Some(anchor)
                if current
                    .outcome_floor
                    .as_ref()
                    .is_some_and(|floor| floor.committed_anchor.as_ref() == Some(anchor)) =>
            {
                current
                    .outcome_floor
                    .as_ref()
                    .and_then(|floor| floor.committed_anchor_link)
            }
            Some(anchor) => {
                let index = snapshot
                    .commit_links
                    .binary_search_by_key(&anchor.epoch, |link| link.epoch)
                    .map_err(|_| {
                        DecisionError::Conflict(format!(
                            "cluster Commit epoch {} has no audited Commit link",
                            anchor.epoch
                        ))
                    })?;
                let link = snapshot.commit_links[index];
                if link.checkpoint_id != anchor.checkpoint_id {
                    return Err(DecisionError::Conflict(format!(
                        "cluster Commit epoch {} checkpoint {} does not match audited Commit link checkpoint {}",
                        anchor.epoch, anchor.checkpoint_id, link.checkpoint_id
                    ))
                    .into());
                }
                Some(link)
            }
            None => None,
        };
        let floor = AuthorityOutcomeFloor {
            deployment_id: CheckpointDecisionStore::new(Arc::clone(&self.store))
                .load_or_create_deployment_id()
                .await?,
            artifact_before_epoch,
            authority_before_epoch,
            terminal_anchor,
            terminal_anchor_link,
            committed_anchor,
            committed_anchor_link,
        };
        floor.validate()?;
        Ok(floor)
    }

    async fn compact_cluster_outcome_history_before_append(
        &self,
        proof: &LeaderProof,
        current: &LeaderAuthorityRecord,
        snapshot: &ClusterOutcomeAuditSnapshot,
    ) -> Result<bool, ClusterCheckpointAuthorityError> {
        if snapshot.terminal_links.len() < OUTCOME_HISTORY_COMPACTION_TRIGGER {
            return Ok(false);
        }
        let authority_before_epoch = snapshot.terminal_links
            [snapshot.terminal_links.len() - OUTCOME_HISTORY_RETAINED_LINKS]
            .epoch;
        let artifact_before_epoch = current
            .outcome_floor
            .as_ref()
            .map_or(0, |floor| floor.artifact_before_epoch);
        let floor = self
            .build_cluster_outcome_floor(
                current,
                snapshot,
                artifact_before_epoch,
                authority_before_epoch,
            )
            .await?;
        // Floor construction may perform remote reads. Publish only if the audited chain and both
        // horizons are still the exact authority snapshot used to select the anchors.
        let rechecked = self
            .load_record()
            .await?
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        if !rechecked.lease.matches_proof(proof) {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        if rechecked.outcome_head != current.outcome_head
            || rechecked.commit_head != current.commit_head
            || rechecked.outcome_floor != current.outcome_floor
        {
            return Ok(true);
        }

        let sequence = rechecked
            .lease
            .seq
            .checked_add(1)
            .ok_or_else(|| LeaseError::Invalid("leader authority sequence exhausted".into()))?;
        let mut lease = rechecked.lease.clone();
        lease.seq = sequence;
        let mut next = rechecked.preserve_with_lease(lease);
        next.outcome_floor = Some(floor.clone());
        next.validate()?;
        match self.create_authority_record(&next).await? {
            AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                self.install_cluster_outcome_audit(
                    Self::cluster_outcome_audit_key(&next),
                    next.lease.seq,
                    Self::outcomes_retained_by_floor(&floor, snapshot),
                );
                Ok(true)
            }
            AuthorityCreateOutcome::Contended(winner) => {
                if !winner.lease.matches_proof(proof) {
                    return Err(ClusterCheckpointAuthorityError::Fenced);
                }
                if winner.lease.seq <= current.lease.seq {
                    return Err(LeaseError::Invalid(
                        "outcome history compaction contention did not advance the authority sequence"
                            .into(),
                    )
                    .into());
                }
                Ok(true)
            }
        }
    }

    async fn audited_cluster_outcomes(
        &self,
    ) -> Result<
        (Option<LeaderAuthorityRecord>, Arc<[CheckpointOutcome]>),
        ClusterCheckpointAuthorityError,
    > {
        const AUDIT_RETRIES: usize = 3;
        for attempt in 0..AUDIT_RETRIES {
            let Some(head) = self.load_record().await? else {
                *self.outcome_audit_cache.lock() = None;
                return Ok((None, Arc::from([])));
            };
            match self.cached_audited_cluster_outcomes_from(&head).await {
                Err(ClusterCheckpointAuthorityError::Decision(
                    DecisionError::InventoryChanged(_),
                )) if attempt + 1 < AUDIT_RETRIES => {
                    tokio::task::yield_now().await;
                }
                Ok(snapshot) => return Ok((Some(head), snapshot.outcomes)),
                Err(error) => return Err(error),
            }
        }
        Err(DecisionError::InventoryChanged(
            "cluster outcome audit exhausted stability retries".into(),
        )
        .into())
    }

    async fn audited_assignment_decisions_from(
        &self,
        head: &LeaderAuthorityRecord,
    ) -> Result<Vec<AuthorityAssignmentDecision>, ClusterCheckpointAuthorityError> {
        let mut newest_first = Vec::new();
        let floor = head.assignment_decision_floor.as_ref();
        let before_target_version = floor.map_or(0, |floor| floor.before_target_version);
        let mut stopped_at_anchor = false;
        let mut link = head.assignment_decision_head;
        let mut traversed = 0;
        while let Some(current) = link {
            if !consume_live_authority_link(&mut traversed) {
                return Err(DecisionError::Conflict(format!(
                    "live assignment-decision retention exceeds the fixed {MAX_LIVE_AUTHORITY_LINKS}-link authority bound"
                ))
                .into());
            }
            if current.target_version < before_target_version {
                if floor.and_then(|floor| floor.terminal_anchor_link) != Some(current) {
                    return Err(DecisionError::Conflict(format!(
                        "assignment decision chain does not meet durable floor {before_target_version} at its terminal anchor"
                    ))
                    .into());
                }
                stopped_at_anchor = true;
                break;
            }
            let record = read_authority_record(self.store.as_ref(), current.sequence)
                .await?
                .ok_or_else(|| {
                    DecisionError::InventoryChanged(format!(
                        "assignment decision authority record {} disappeared during audit",
                        current.sequence
                    ))
                })?;
            let decision = record.assignment_decision.clone().ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "assignment decision head version {} points to non-decision authority record {}",
                    current.target_version, current.sequence
                ))
            })?;
            if record.assignment_decision_head != Some(current)
                || decision.target_version() != current.target_version
            {
                return Err(DecisionError::Conflict(format!(
                    "assignment decision link version {} sequence {} does not match its authority record",
                    current.target_version, current.sequence
                ))
                .into());
            }
            newest_first.push(decision);
            link = record.previous_assignment_decision;
        }
        if floor.and_then(|floor| floor.terminal_anchor_link).is_some() && !stopped_at_anchor {
            return Err(DecisionError::Conflict(
                "assignment decision chain stopped without its durable retention anchor".into(),
            )
            .into());
        }
        newest_first.reverse();
        if let Some(pair) = newest_first
            .windows(2)
            .find(|pair| pair[0].target_version() >= pair[1].target_version())
        {
            return Err(DecisionError::Conflict(format!(
                "assignment decisions regress from version {} to {}",
                pair[0].target_version(),
                pair[1].target_version()
            ))
            .into());
        }
        Ok(newest_first)
    }

    async fn exact_assignment_decision_link(
        &self,
        head: &LeaderAuthorityRecord,
        decision: &AuthorityAssignmentDecision,
    ) -> Result<AssignmentDecisionLink, ClusterCheckpointAuthorityError> {
        if let Some(floor) = head.assignment_decision_floor.as_ref() {
            if floor.terminal_anchor.as_ref() == Some(decision) {
                return floor.terminal_anchor_link.ok_or_else(|| {
                    DecisionError::Conflict(
                        "assignment decision floor lost its terminal authority link".into(),
                    )
                    .into()
                });
            }
        }
        let before_target_version = head
            .assignment_decision_floor
            .as_ref()
            .map_or(0, |floor| floor.before_target_version);
        let mut link = head.assignment_decision_head;
        let mut traversed = 0;
        while let Some(current) = link {
            if !consume_live_authority_link(&mut traversed) {
                return Err(DecisionError::Conflict(format!(
                    "live assignment-decision retention exceeds the fixed {MAX_LIVE_AUTHORITY_LINKS}-link authority bound during exact lookup"
                ))
                .into());
            }
            if current.target_version < before_target_version {
                break;
            }
            let record = read_authority_record(self.store.as_ref(), current.sequence)
                .await?
                .ok_or_else(|| {
                    DecisionError::InventoryChanged(format!(
                        "assignment decision authority record {} disappeared during exact lookup",
                        current.sequence
                    ))
                })?;
            if record.assignment_decision.as_ref() == Some(decision) {
                return Ok(current);
            }
            link = record.previous_assignment_decision;
        }
        Err(DecisionError::Conflict(format!(
            "assignment decision version {} is not linked from the durable authority head",
            decision.target_version()
        ))
        .into())
    }

    /// Admit one cluster terminal outcome through the exact next leader-authority sequence.
    ///
    /// Renewals, takeovers, catalog seals, floor advances, and other decisions all contend on the
    /// same create-only object. An identical retry converges on the durable winner.
    ///
    /// # Errors
    /// Fails closed for a stale proof, non-monotonic or conflicting outcome, malformed recovery
    /// capsule, or object-store failure.
    pub async fn record_cluster_outcome(
        &self,
        proof: &LeaderProof,
        epoch: u64,
        checkpoint_id: u64,
        assignment_fence: CheckpointAssignmentFence,
        verdict: CheckpointVerdict,
        recovery_capsule: Option<RecoveryCapsuleRef>,
    ) -> Result<RecordOutcomeResult, ClusterCheckpointAuthorityError> {
        if !proof.is_canonical() {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        let initial = self
            .load_record()
            .await?
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        if !initial.lease.matches_proof(proof) {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        let candidate = CheckpointDecisionStore::new(Arc::clone(&self.store))
            .canonical_outcome(
                epoch,
                checkpoint_id,
                CheckpointScope::Cluster,
                Some(assignment_fence),
                Some(proof.clone()),
                verdict,
                recovery_capsule,
            )
            .await?;

        loop {
            let current = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            if !current.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            let snapshot = self.cached_audited_cluster_outcomes_from(&current).await?;
            let outcomes = &snapshot.outcomes;
            if let Some(winner) = outcomes
                .iter()
                .find(|outcome| outcome.epoch == candidate.epoch)
            {
                return if winner == &candidate {
                    Ok(RecordOutcomeResult::Unchanged(winner.clone()))
                } else {
                    Ok(RecordOutcomeResult::Conflict {
                        winner: winner.clone(),
                    })
                };
            }
            if let Some(last) = outcomes.last() {
                if candidate.epoch <= last.epoch || candidate.checkpoint_id <= last.checkpoint_id {
                    return Err(DecisionError::Conflict(format!(
                        "cluster outcome epoch {} checkpoint {} does not advance durable epoch {} checkpoint {}",
                        candidate.epoch, candidate.checkpoint_id, last.epoch, last.checkpoint_id
                    ))
                    .into());
                }
            }
            if candidate.is_commit() && snapshot.commit_links.len() >= MAX_LIVE_AUTHORITY_LINKS {
                return Err(DecisionError::Conflict(format!(
                    "live Commit retention reached the fixed {MAX_LIVE_AUTHORITY_LINKS}-link authority bound; advance the artifact-retention horizon before admitting another Commit"
                ))
                .into());
            }
            if let Some(floor) = current.outcome_floor.as_ref() {
                if candidate.deployment_id != floor.deployment_id
                    || candidate.epoch < floor.authority_before_epoch
                {
                    return Err(DecisionError::Conflict(format!(
                        "cluster outcome epoch {} is below or outside authority floor {}",
                        candidate.epoch, floor.authority_before_epoch
                    ))
                    .into());
                }
            }
            if self
                .compact_cluster_outcome_history_before_append(proof, &current, &snapshot)
                .await?
            {
                tokio::task::yield_now().await;
                continue;
            }

            let base_sequence = current.lease.seq;
            let sequence = base_sequence
                .checked_add(1)
                .ok_or_else(|| LeaseError::Invalid("leader authority sequence exhausted".into()))?;
            let mut next = current.preserve_with_lease(LeaderLease {
                seq: sequence,
                renewal_sequence: current.lease.renewal_sequence,
                token: current.lease.token,
                owner: current.lease.owner.clone(),
                expires_at_ms: current.lease.expires_at_ms,
                catalog_manifest: current.lease.catalog_manifest.clone(),
            });
            next.checkpoint_outcome = Some(candidate.clone());
            next.previous_outcome = current.outcome_head;
            let new_link = OutcomeLink {
                sequence,
                epoch: candidate.epoch,
                checkpoint_id: candidate.checkpoint_id,
            };
            next.outcome_head = Some(new_link);
            if candidate.is_commit() {
                next.previous_commit = current.commit_head;
                next.commit_head = Some(new_link);
            }
            next.validate()?;
            match self.create_authority_record(&next).await? {
                AuthorityCreateOutcome::Created => {
                    let mut appended = outcomes.to_vec();
                    appended.push(candidate.clone());
                    let mut terminal_links = snapshot.terminal_links.to_vec();
                    terminal_links.push(new_link);
                    let mut commit_links = snapshot.commit_links.to_vec();
                    if candidate.is_commit() {
                        commit_links.push(new_link);
                    }
                    self.install_cluster_outcome_audit(
                        Self::cluster_outcome_audit_key(&next),
                        next.lease.seq,
                        ClusterOutcomeAuditSnapshot {
                            outcomes: Arc::from(appended),
                            terminal_links: Arc::from(terminal_links),
                            commit_links: Arc::from(commit_links),
                        },
                    );
                    return Ok(RecordOutcomeResult::Created(candidate));
                }
                AuthorityCreateOutcome::ExistingIdentical => {
                    return Ok(RecordOutcomeResult::Unchanged(candidate));
                }
                AuthorityCreateOutcome::Contended(winner_head) => {
                    let winners = self
                        .cached_audited_cluster_outcomes_from(&winner_head)
                        .await?;
                    if let Some(winner) = winners
                        .outcomes
                        .iter()
                        .find(|outcome| outcome.epoch == candidate.epoch)
                    {
                        return if winner == &candidate {
                            Ok(RecordOutcomeResult::Unchanged(winner.clone()))
                        } else {
                            Ok(RecordOutcomeResult::Conflict {
                                winner: winner.clone(),
                            })
                        };
                    }
                    if !winner_head.lease.matches_proof(proof) {
                        return Err(ClusterCheckpointAuthorityError::Fenced);
                    }
                    if winner_head.lease.seq <= base_sequence {
                        return Err(LeaseError::Invalid(
                            "cluster outcome contention did not advance the authority sequence"
                                .into(),
                        )
                        .into());
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    async fn validate_assignment_recovery_snapshot(
        &self,
        decision: &AssignmentRecoveryDecision,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        let snapshots = AssignmentSnapshotStore::new(Arc::clone(&self.store));
        let predecessor = snapshots
            .load()
            .await
            .map_err(|error| {
                assignment_snapshot_error("load assignment recovery predecessor", error)
            })?
            .ok_or_else(|| {
                LeaseError::Invalid(
                    "assignment recovery requires a durable predecessor snapshot".into(),
                )
            })?;
        let predecessor_fence = predecessor.assignment_fence().map_err(|error| {
            LeaseError::Invalid(format!(
                "assignment recovery predecessor has no valid fence: {error}"
            ))
        })?;
        if predecessor.draining || predecessor_fence != decision.predecessor {
            return Err(LeaseError::Invalid(
                "assignment recovery predecessor is not the exact committed assignment head".into(),
            )
            .into());
        }
        let proposal = snapshots
            .load_recovery_proposal(&decision.proposal)
            .await
            .map_err(|error| {
                assignment_snapshot_error("load assignment recovery proposal", error)
            })?;
        let proposal_fence = proposal.assignment_fence().map_err(|error| {
            LeaseError::Invalid(format!(
                "assignment recovery proposal has no valid fence: {error}"
            ))
        })?;
        if proposal_fence != decision.target {
            return Err(LeaseError::Invalid(
                "assignment recovery proposal does not match its authorized target fence".into(),
            )
            .into());
        }
        Ok(())
    }

    async fn record_assignment_decision(
        &self,
        proof: &LeaderProof,
        decision: AuthorityAssignmentDecision,
    ) -> Result<RecordAuthorityAssignmentDecisionResult, ClusterCheckpointAuthorityError> {
        decision.validate()?;
        if decision.leader_proof() != proof || !proof.is_canonical() {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }

        loop {
            let current = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            if !current.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            if let Some(floor) = current.assignment_decision_floor.as_ref() {
                if decision.target_version() < floor.before_target_version {
                    return Err(DecisionError::Conflict(format!(
                        "assignment decision version {} is below durable retention floor {}",
                        decision.target_version(),
                        floor.before_target_version
                    ))
                    .into());
                }
            }
            let decisions = self.audited_assignment_decisions_from(&current).await?;
            if let Some(winner) = decisions
                .iter()
                .find(|winner| winner.target_version() == decision.target_version())
            {
                return if winner == &decision {
                    Ok(RecordAuthorityAssignmentDecisionResult::Unchanged(
                        winner.clone(),
                    ))
                } else {
                    Ok(RecordAuthorityAssignmentDecisionResult::Conflict {
                        winner: winner.clone(),
                    })
                };
            }
            if let Some(last) = decisions.last() {
                if decision.target_version() <= last.target_version() {
                    return Err(DecisionError::Conflict(format!(
                        "assignment decision version {} does not advance durable version {}",
                        decision.target_version(),
                        last.target_version()
                    ))
                    .into());
                }
            }
            if let AuthorityAssignmentDecision::Recovery(recovery) = &decision {
                self.validate_assignment_recovery_snapshot(recovery).await?;
            }

            let base_sequence = current.lease.seq;
            let sequence = base_sequence
                .checked_add(1)
                .ok_or_else(|| LeaseError::Invalid("leader authority sequence exhausted".into()))?;
            let mut candidate = current.preserve_with_lease(LeaderLease {
                seq: sequence,
                renewal_sequence: current.lease.renewal_sequence,
                token: current.lease.token,
                owner: current.lease.owner.clone(),
                expires_at_ms: current.lease.expires_at_ms,
                catalog_manifest: current.lease.catalog_manifest.clone(),
            });
            candidate.assignment_decision = Some(decision.clone());
            candidate.previous_assignment_decision = current.assignment_decision_head;
            candidate.assignment_decision_head = Some(AssignmentDecisionLink {
                sequence,
                target_version: decision.target_version(),
            });
            candidate.validate()?;

            match self.create_authority_record(&candidate).await? {
                AuthorityCreateOutcome::Created => {
                    return Ok(RecordAuthorityAssignmentDecisionResult::Created(decision));
                }
                AuthorityCreateOutcome::ExistingIdentical => {
                    return Ok(RecordAuthorityAssignmentDecisionResult::Unchanged(decision));
                }
                AuthorityCreateOutcome::Contended(winner_head) => {
                    let winners = self.audited_assignment_decisions_from(&winner_head).await?;
                    if let Some(winner) = winners
                        .iter()
                        .find(|winner| winner.target_version() == decision.target_version())
                    {
                        return if winner == &decision {
                            Ok(RecordAuthorityAssignmentDecisionResult::Unchanged(
                                winner.clone(),
                            ))
                        } else {
                            Ok(RecordAuthorityAssignmentDecisionResult::Conflict {
                                winner: winner.clone(),
                            })
                        };
                    }
                    if !winner_head.lease.matches_proof(proof) {
                        return Err(ClusterCheckpointAuthorityError::Fenced);
                    }
                    if winner_head.lease.seq > base_sequence {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    return Err(LeaseError::Invalid(
                        "assignment authority contention did not advance the sequence".into(),
                    )
                    .into());
                }
            }
        }
    }

    /// Admit one assignment-drain settlement through the exact next authority sequence.
    ///
    /// Lease renewals, takeovers, checkpoint outcomes, and other decisions contend on that same
    /// create-only sequence. An identical retry converges on the durable winner.
    ///
    /// # Errors
    /// Fails closed for a stale proof, malformed/non-monotonic decision, or storage failure.
    pub async fn record_assignment_drain_decision(
        &self,
        proof: &LeaderProof,
        decision: AssignmentDrainDecision,
    ) -> Result<RecordAssignmentDrainDecisionResult, ClusterCheckpointAuthorityError> {
        match self
            .record_assignment_decision(proof, AuthorityAssignmentDecision::Drain(decision))
            .await?
        {
            RecordAuthorityAssignmentDecisionResult::Created(
                AuthorityAssignmentDecision::Drain(decision),
            ) => Ok(RecordAssignmentDrainDecisionResult::Created(decision)),
            RecordAuthorityAssignmentDecisionResult::Unchanged(
                AuthorityAssignmentDecision::Drain(decision),
            ) => Ok(RecordAssignmentDrainDecisionResult::Unchanged(decision)),
            RecordAuthorityAssignmentDecisionResult::Conflict {
                winner: AuthorityAssignmentDecision::Drain(winner),
            } => Ok(RecordAssignmentDrainDecisionResult::Conflict { winner }),
            RecordAuthorityAssignmentDecisionResult::Conflict {
                winner: AuthorityAssignmentDecision::Recovery(winner),
            } => Err(DecisionError::Conflict(format!(
                "assignment recovery decision already settled target version {}",
                winner.target_version()
            ))
            .into()),
            RecordAuthorityAssignmentDecisionResult::Created(_)
            | RecordAuthorityAssignmentDecisionResult::Unchanged(_) => Err(LeaseError::Invalid(
                "assignment decision recorder returned the wrong decision kind".into(),
            )
            .into()),
        }
    }

    /// Admit one failure-recovery assignment through the exact next authority sequence.
    ///
    /// # Errors
    /// Fails closed for a stale proof, malformed/non-monotonic decision, or storage failure.
    pub(crate) async fn record_assignment_recovery_decision(
        &self,
        proof: &LeaderProof,
        decision: AssignmentRecoveryDecision,
    ) -> Result<RecordAssignmentRecoveryDecisionResult, ClusterCheckpointAuthorityError> {
        decision.validate()?;
        if &decision.leader_proof != proof || !proof.is_canonical() {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        let current = self
            .load_record()
            .await?
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        if !current.lease.matches_proof(proof) {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        if let Some(floor) = current.assignment_decision_floor.as_ref() {
            if decision.target_version() < floor.before_target_version {
                return Err(DecisionError::Conflict(format!(
                    "assignment decision version {} is below durable retention floor {}",
                    decision.target_version(),
                    floor.before_target_version
                ))
                .into());
            }
        }
        if let Some(winner) = self
            .audited_assignment_decisions_from(&current)
            .await?
            .into_iter()
            .find(|winner| winner.target_version() == decision.target_version())
        {
            return match winner {
                AuthorityAssignmentDecision::Recovery(winner) if winner == decision => {
                    Ok(RecordAssignmentRecoveryDecisionResult::Unchanged(winner))
                }
                AuthorityAssignmentDecision::Recovery(winner) => {
                    Ok(RecordAssignmentRecoveryDecisionResult::Conflict { winner })
                }
                AuthorityAssignmentDecision::Drain(winner) => {
                    Err(DecisionError::Conflict(format!(
                        "assignment drain decision already settled target version {}",
                        winner.target_version()
                    ))
                    .into())
                }
            };
        }
        match self
            .record_assignment_decision(proof, AuthorityAssignmentDecision::Recovery(decision))
            .await?
        {
            RecordAuthorityAssignmentDecisionResult::Created(
                AuthorityAssignmentDecision::Recovery(decision),
            ) => Ok(RecordAssignmentRecoveryDecisionResult::Created(decision)),
            RecordAuthorityAssignmentDecisionResult::Unchanged(
                AuthorityAssignmentDecision::Recovery(decision),
            ) => Ok(RecordAssignmentRecoveryDecisionResult::Unchanged(decision)),
            RecordAuthorityAssignmentDecisionResult::Conflict {
                winner: AuthorityAssignmentDecision::Recovery(winner),
            } => Ok(RecordAssignmentRecoveryDecisionResult::Conflict { winner }),
            RecordAuthorityAssignmentDecisionResult::Conflict {
                winner: AuthorityAssignmentDecision::Drain(winner),
            } => Err(DecisionError::Conflict(format!(
                "assignment drain decision already settled target version {}",
                winner.target_version()
            ))
            .into()),
            RecordAuthorityAssignmentDecisionResult::Created(_)
            | RecordAuthorityAssignmentDecisionResult::Unchanged(_) => Err(LeaseError::Invalid(
                "assignment decision recorder returned the wrong decision kind".into(),
            )
            .into()),
        }
    }

    async fn assignment_decision(
        &self,
        target_version: u64,
    ) -> Result<Option<AuthorityAssignmentDecision>, ClusterCheckpointAuthorityError> {
        if target_version == 0 {
            return Err(LeaseError::Invalid(
                "assignment decision target version must be nonzero".into(),
            )
            .into());
        }
        let Some(head) = self.load_record().await? else {
            return Ok(None);
        };
        if let Some(floor) = head.assignment_decision_floor.as_ref() {
            if target_version < floor.before_target_version {
                return Err(DecisionError::Conflict(format!(
                    "assignment decision version {target_version} is below durable retention floor {}",
                    floor.before_target_version
                ))
                .into());
            }
        }
        Ok(self
            .audited_assignment_decisions_from(&head)
            .await?
            .into_iter()
            .find(|decision| decision.target_version() == target_version))
    }

    /// Read the immutable settlement for one exact target assignment version.
    ///
    /// # Errors
    /// Fails closed on malformed or incomplete authority history.
    pub async fn assignment_drain_decision(
        &self,
        target_version: u64,
    ) -> Result<Option<AssignmentDrainDecision>, ClusterCheckpointAuthorityError> {
        match self.assignment_decision(target_version).await? {
            Some(AuthorityAssignmentDecision::Drain(decision)) => Ok(Some(decision)),
            Some(AuthorityAssignmentDecision::Recovery(_)) => Err(DecisionError::Conflict(
                format!("target version {target_version} was settled by assignment recovery"),
            )
            .into()),
            None => Ok(None),
        }
    }

    /// Read the immutable recovery authorization for one exact target assignment version.
    ///
    /// # Errors
    /// Fails closed on malformed, incomplete, or cross-kind authority history.
    pub async fn assignment_recovery_decision(
        &self,
        target_version: u64,
    ) -> Result<Option<AssignmentRecoveryDecision>, ClusterCheckpointAuthorityError> {
        match self.assignment_decision(target_version).await? {
            Some(AuthorityAssignmentDecision::Recovery(decision)) => Ok(Some(decision)),
            Some(AuthorityAssignmentDecision::Drain(_)) => Err(DecisionError::Conflict(format!(
                "target version {target_version} was settled by assignment drain"
            ))
            .into()),
            None => Ok(None),
        }
    }

    /// Materialize the immutable authority winner for one recovery target version.
    ///
    /// The caller supplies only the version; an arbitrary staged proposal can never bypass the
    /// shared leader-decision chain.
    ///
    /// # Errors
    /// Fails closed when no recovery decision exists, its proposal is invalid, or storage fails.
    pub async fn materialize_assignment_recovery(
        &self,
        target_version: u64,
    ) -> Result<RotateOutcome, ClusterCheckpointAuthorityError> {
        let decision = self
            .assignment_recovery_decision(target_version)
            .await?
            .ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "assignment recovery target {target_version} has no authority decision"
                ))
            })?;
        let snapshots = AssignmentSnapshotStore::new(Arc::clone(&self.store));
        let proposal = snapshots
            .load_recovery_proposal(&decision.proposal)
            .await
            .map_err(|error| {
                assignment_snapshot_error("load authorized assignment recovery proposal", error)
            })?;
        if proposal.assignment_fence().map_err(|error| {
            LeaseError::Invalid(format!(
                "authorized assignment recovery proposal has no valid fence: {error}"
            ))
        })? != decision.target
        {
            return Err(LeaseError::Invalid(
                "authorized assignment recovery proposal does not match its target".into(),
            )
            .into());
        }
        snapshots
            .materialize_recovery(&decision.proposal)
            .await
            .map_err(|error| {
                assignment_snapshot_error("materialize authorized assignment recovery", error)
                    .into()
            })
    }

    /// Advance the shared assignment-decision floor through the exact next authority sequence.
    ///
    /// The compatibility-shaped API retains both drain and recovery decisions on one ordered
    /// chain. The caller must first durably prune assignment snapshots below the same
    /// target-version horizon. Decision-bearing authority records below the durable floor then
    /// become eligible for best-effort deletion while the exact terminal anchor preserves chain
    /// continuity.
    pub async fn prune_assignment_drain_decisions_before(
        &self,
        proof: &LeaderProof,
        before_target_version: u64,
    ) -> Result<u64, ClusterCheckpointAuthorityError> {
        if before_target_version == 0 {
            return Ok(self
                .load_record()
                .await?
                .and_then(|head| head.assignment_decision_floor)
                .map_or(0, |floor| floor.before_target_version));
        }
        if !proof.is_canonical() {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        loop {
            let current = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            if !current.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            if let Some(floor) = current.assignment_decision_floor.as_ref() {
                if floor.before_target_version >= before_target_version {
                    self.schedule_history_prune();
                    return Ok(floor.before_target_version);
                }
            }

            let decisions = self.audited_assignment_decisions_from(&current).await?;
            let terminal_anchor = decisions
                .iter()
                .rev()
                .find(|decision| decision.target_version() < before_target_version)
                .cloned()
                .or_else(|| {
                    current
                        .assignment_decision_floor
                        .as_ref()
                        .and_then(|floor| floor.terminal_anchor.clone())
                });
            let terminal_anchor_link = match terminal_anchor.as_ref() {
                Some(anchor) => Some(
                    self.exact_assignment_decision_link(&current, anchor)
                        .await?,
                ),
                None => None,
            };
            let floor = AuthorityAssignmentDecisionFloor {
                before_target_version,
                terminal_anchor,
                terminal_anchor_link,
            };
            floor.validate()?;

            let base_sequence = current.lease.seq;
            let sequence = base_sequence
                .checked_add(1)
                .ok_or_else(|| LeaseError::Invalid("leader authority sequence exhausted".into()))?;
            let mut next = current.preserve_with_lease(LeaderLease {
                seq: sequence,
                renewal_sequence: current.lease.renewal_sequence,
                token: current.lease.token,
                owner: current.lease.owner.clone(),
                expires_at_ms: current.lease.expires_at_ms,
                catalog_manifest: current.lease.catalog_manifest.clone(),
            });
            next.assignment_decision_floor = Some(floor);
            next.validate()?;

            match self.create_authority_record(&next).await? {
                AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                    return Ok(before_target_version);
                }
                AuthorityCreateOutcome::Contended(winner) => {
                    if let Some(winner_floor) = winner.assignment_decision_floor.as_ref() {
                        if winner_floor.before_target_version >= before_target_version {
                            self.schedule_history_prune();
                            return Ok(winner_floor.before_target_version);
                        }
                    }
                    if !winner.lease.matches_proof(proof) {
                        return Err(ClusterCheckpointAuthorityError::Fenced);
                    }
                    if winner.lease.seq > base_sequence {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    return Err(LeaseError::Invalid(
                        "assignment decision floor contention did not advance the sequence".into(),
                    )
                    .into());
                }
            }
        }
    }

    async fn cluster_outcome_from_snapshot(
        &self,
        head: &LeaderAuthorityRecord,
        epoch: u64,
    ) -> Result<Option<CheckpointOutcome>, ClusterCheckpointAuthorityError> {
        let floor = head.outcome_floor.as_ref();
        let artifact_before_epoch = floor.map_or(0, |floor| floor.artifact_before_epoch);
        if epoch < artifact_before_epoch {
            return Ok(None);
        }
        let authority_before_epoch = floor.map_or(0, |floor| floor.authority_before_epoch);
        let cache_key = Self::cluster_outcome_audit_key(head);
        if let Some(snapshot) = self.cached_cluster_outcome_audit(&cache_key, head.lease.seq) {
            return Ok(snapshot
                .outcomes
                .binary_search_by_key(&epoch, |outcome| outcome.epoch)
                .ok()
                .map(|index| snapshot.outcomes[index].clone())
                .filter(|outcome| {
                    outcome.epoch >= authority_before_epoch
                        || outcome.is_commit()
                        || floor
                            .is_some_and(|floor| floor.terminal_anchor.as_ref() == Some(outcome))
                }));
        }
        if let Some(anchor) = floor
            .and_then(|floor| floor.terminal_anchor.as_ref())
            .filter(|anchor| anchor.epoch == epoch)
        {
            let expected_deployment = CheckpointDecisionStore::new(Arc::clone(&self.store))
                .load_or_create_deployment_id()
                .await?;
            if anchor.deployment_id != expected_deployment {
                return Err(DecisionError::Conflict(format!(
                    "cluster terminal anchor epoch {} does not belong to current deployment {}",
                    anchor.epoch, expected_deployment
                ))
                .into());
            }
            return Ok(Some(anchor.clone()));
        }

        let commit_only = epoch < authority_before_epoch;
        let before_epoch = if commit_only {
            artifact_before_epoch
        } else {
            authority_before_epoch
        };
        let mut current = if commit_only {
            head.commit_head
        } else {
            head.outcome_head
        };
        let expected_anchor = if commit_only {
            floor.and_then(|floor| floor.committed_anchor_link)
        } else {
            floor.and_then(|floor| floor.terminal_anchor_link)
        };
        let expected_anchor_outcome = if commit_only {
            floor.and_then(|floor| floor.committed_anchor.as_ref())
        } else {
            floor.and_then(|floor| floor.terminal_anchor.as_ref())
        };
        let mut traversed = 0;
        while let Some(link) = current {
            if link.epoch < before_epoch {
                match (expected_anchor, expected_anchor_outcome) {
                    (Some(anchor_link), Some(anchor))
                        if link == anchor_link
                            && link.epoch == anchor.epoch
                            && link.checkpoint_id == anchor.checkpoint_id =>
                    {
                        return Ok(None)
                    }
                    (None, None) => return Ok(None),
                    _ => {
                        return Err(DecisionError::Conflict(format!(
                            "cluster {} chain does not meet durable floor {before_epoch} at its exact anchor",
                            if commit_only { "Commit" } else { "terminal outcome" }
                        ))
                        .into());
                    }
                }
            }
            if link.epoch < epoch {
                return Ok(None);
            }
            if !consume_live_authority_link(&mut traversed) {
                return Err(DecisionError::Conflict(format!(
                    "live {} retention exceeds the fixed {MAX_LIVE_AUTHORITY_LINKS}-link authority bound during exact lookup",
                    if commit_only { "Commit" } else { "outcome" }
                ))
                .into());
            }

            let admission;
            let record = if link.sequence == head.lease.seq {
                head
            } else {
                admission = read_authority_record(self.store.as_ref(), link.sequence)
                    .await?
                    .ok_or_else(|| {
                        DecisionError::InventoryChanged(format!(
                            "cluster {} authority record {} disappeared during exact lookup",
                            if commit_only { "Commit" } else { "outcome" },
                            link.sequence
                        ))
                    })?;
                &admission
            };
            let outcome = record.checkpoint_outcome.as_ref().ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "cluster {} head epoch {} points to non-outcome authority record {}",
                    if commit_only { "Commit" } else { "outcome" },
                    link.epoch,
                    link.sequence
                ))
            })?;
            let record_head = if commit_only {
                record.commit_head
            } else {
                record.outcome_head
            };
            if record_head != Some(link)
                || (commit_only && !outcome.is_commit())
                || outcome.epoch != link.epoch
                || outcome.checkpoint_id != link.checkpoint_id
            {
                return Err(DecisionError::Conflict(format!(
                    "cluster {} link epoch {} sequence {} does not match its authority record",
                    if commit_only { "Commit" } else { "outcome" },
                    link.epoch,
                    link.sequence
                ))
                .into());
            }
            if outcome.epoch == epoch {
                let expected_deployment = CheckpointDecisionStore::new(Arc::clone(&self.store))
                    .load_or_create_deployment_id()
                    .await?;
                if floor.is_some_and(|floor| floor.deployment_id != expected_deployment)
                    || outcome.deployment_id != expected_deployment
                {
                    return Err(DecisionError::Conflict(format!(
                        "cluster outcome epoch {} does not belong to current deployment {}",
                        outcome.epoch, expected_deployment
                    ))
                    .into());
                }
                return Ok(Some(outcome.clone()));
            }
            current = if commit_only {
                record.previous_commit
            } else {
                record.previous_outcome
            };
        }
        if expected_anchor.is_some() {
            return Err(DecisionError::Conflict(format!(
                "cluster {} chain stopped without its durable floor anchor",
                if commit_only { "Commit" } else { "outcome" }
            ))
            .into());
        }
        Ok(None)
    }

    async fn stable_cluster_outcome(
        &self,
        epoch: u64,
    ) -> Result<Option<CheckpointOutcome>, ClusterCheckpointAuthorityError> {
        const LOOKUP_RETRIES: usize = 3;
        for attempt in 0..LOOKUP_RETRIES {
            let Some(head) = self.load_record().await? else {
                return Ok(None);
            };
            match self.cluster_outcome_from_snapshot(&head, epoch).await {
                Err(ClusterCheckpointAuthorityError::Decision(
                    DecisionError::InventoryChanged(_),
                )) if attempt + 1 < LOOKUP_RETRIES => {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        Err(DecisionError::InventoryChanged(
            "cluster outcome exact lookup exhausted stability retries".into(),
        )
        .into())
    }

    /// Read one live cluster outcome from the shared authority.
    pub async fn cluster_outcome(
        &self,
        epoch: u64,
    ) -> Result<Option<CheckpointOutcome>, ClusterCheckpointAuthorityError> {
        self.stable_cluster_outcome(epoch).await
    }

    /// Read one live cluster outcome together with its content-addressed recovery capsule.
    /// Commit always returns a validated capsule; Abort returns `None` for the capsule.
    pub async fn cluster_outcome_with_recovery_capsule(
        &self,
        epoch: u64,
    ) -> Result<
        Option<(CheckpointOutcome, Option<ClusterRecoveryCapsule>)>,
        ClusterCheckpointAuthorityError,
    > {
        let decisions = CheckpointDecisionStore::new(Arc::clone(&self.store));
        let Some(outcome) = self.stable_cluster_outcome(epoch).await? else {
            return Ok(None);
        };
        let capsule = if outcome.is_commit() {
            let reference = outcome.recovery_capsule.as_ref().ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "cluster Commit epoch {} checkpoint {} has no recovery capsule",
                    outcome.epoch, outcome.checkpoint_id
                ))
            })?;
            let capsule = decisions.load_recovery_capsule(reference).await?;
            if capsule.attempt.epoch != outcome.epoch
                || capsule.attempt.checkpoint_id != outcome.checkpoint_id
                || Some(&capsule.assignment_fence) != outcome.assignment_fence.as_ref()
                || capsule.deployment_id != outcome.deployment_id
            {
                return Err(DecisionError::Conflict(format!(
                    "cluster Commit epoch {} checkpoint {} does not match recovery capsule '{}'",
                    outcome.epoch, outcome.checkpoint_id, reference.sha256
                ))
                .into());
            }
            Some(capsule)
        } else {
            None
        };
        Ok(Some((outcome, capsule)))
    }

    /// Audit and return every live cluster outcome in ascending epoch order.
    pub async fn cluster_outcome_inventory(
        &self,
    ) -> Result<ClusterOutcomeInventory, ClusterCheckpointAuthorityError> {
        let (head, outcomes) = self.audited_cluster_outcomes().await?;
        Ok(Self::cluster_outcome_inventory_from_audit(
            head.as_ref(),
            &outcomes,
        ))
    }

    fn cluster_outcome_inventory_from_audit(
        head: Option<&LeaderAuthorityRecord>,
        outcomes: &[CheckpointOutcome],
    ) -> ClusterOutcomeInventory {
        let floor = head.and_then(|head| head.outcome_floor.as_ref());
        let artifact_before_epoch = floor.map_or(0, |floor| floor.artifact_before_epoch);
        ClusterOutcomeInventory {
            outcomes: outcomes
                .iter()
                .filter(|outcome| outcome.epoch >= artifact_before_epoch)
                .cloned()
                .collect(),
            retention_boundary: ClusterOutcomeRetentionBoundary::from_floor(floor),
        }
    }

    /// Audit and return every live cluster outcome in ascending epoch order.
    pub async fn cluster_outcomes(
        &self,
    ) -> Result<Vec<CheckpointOutcome>, ClusterCheckpointAuthorityError> {
        Ok(self.cluster_outcome_inventory().await?.outcomes)
    }

    /// Greatest live cluster commit recovery cut.
    pub async fn highest_cluster_committed_outcome(
        &self,
    ) -> Result<Option<CheckpointOutcome>, ClusterCheckpointAuthorityError> {
        Ok(self
            .cluster_outcomes()
            .await?
            .into_iter()
            .rev()
            .find(CheckpointOutcome::is_commit))
    }

    /// Greatest terminal cluster outcome, including the compacted continuity anchor.
    pub async fn highest_cluster_terminal_outcome(
        &self,
    ) -> Result<Option<CheckpointOutcome>, ClusterCheckpointAuthorityError> {
        Ok(self.audited_cluster_outcomes().await?.1.last().cloned())
    }

    /// Return the exact immutable outcome for `attempt`, or an audited terminal outcome
    /// that durably dominates it in both identity dimensions. Compacted continuity anchors are
    /// included in the audit.
    ///
    /// # Errors
    /// Returns an error for malformed attempt identity, conflicting outcome dimensions, or an
    /// unavailable or invalid durable authority chain.
    pub async fn cluster_attempt_settlement(
        &self,
        attempt: crate::state::CheckpointAttempt,
    ) -> Result<Option<CheckpointOutcome>, ClusterCheckpointAuthorityError> {
        use crate::state::CheckpointAttemptRelation;

        if attempt.epoch == 0 || attempt.checkpoint_id == 0 {
            return Err(DecisionError::Conflict(
                "cluster checkpoint settlement requires nonzero attempt dimensions".into(),
            )
            .into());
        }
        let outcomes = self.audited_cluster_outcomes().await?.1;
        if let Ok(index) = outcomes.binary_search_by_key(&attempt.epoch, |outcome| outcome.epoch) {
            let exact_epoch = &outcomes[index];
            if exact_epoch.checkpoint_id != attempt.checkpoint_id {
                return Err(DecisionError::Conflict(format!(
                    "cluster outcome epoch {} belongs to checkpoint {}, not pending checkpoint {}",
                    attempt.epoch, exact_epoch.checkpoint_id, attempt.checkpoint_id
                ))
                .into());
            }
            return Ok(Some(exact_epoch.clone()));
        }
        if let Ok(index) =
            outcomes.binary_search_by_key(&attempt.checkpoint_id, |outcome| outcome.checkpoint_id)
        {
            let exact_checkpoint = &outcomes[index];
            return Err(DecisionError::Conflict(format!(
                "cluster outcome checkpoint {} belongs to epoch {}, not pending epoch {}",
                attempt.checkpoint_id, exact_checkpoint.epoch, attempt.epoch
            ))
            .into());
        }
        let Some(highest) = outcomes.last() else {
            return Ok(None);
        };
        let highest_attempt =
            crate::state::CheckpointAttempt::new(highest.epoch, highest.checkpoint_id);
        match highest_attempt.relation_to(attempt) {
            CheckpointAttemptRelation::Newer => Ok(Some(highest.clone())),
            CheckpointAttemptRelation::Older => Ok(None),
            CheckpointAttemptRelation::Exact => unreachable!("exact epoch was handled above"),
            CheckpointAttemptRelation::Conflict => Err(DecisionError::Conflict(format!(
                "cluster terminal outcome epoch {} checkpoint {} conflicts with pending epoch {} checkpoint {}",
                highest.epoch, highest.checkpoint_id, attempt.epoch, attempt.checkpoint_id
            ))
            .into()),
        }
    }

    async fn validate_cluster_recovery_cut(
        &self,
        floor: &AuthorityOutcomeFloor,
        audited_outcomes: &[CheckpointOutcome],
    ) -> Result<Option<CheckpointOutcome>, ClusterCheckpointAuthorityError> {
        if floor.artifact_before_epoch == 0 {
            return Ok(None);
        }
        let recovery_cut = audited_outcomes
            .iter()
            .rev()
            .find(|outcome| outcome.epoch >= floor.artifact_before_epoch && outcome.is_commit())
            .ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "cluster outcome floor {} has no live commit recovery cut",
                    floor.artifact_before_epoch
                ))
            })?;
        CheckpointDecisionStore::new(Arc::clone(&self.store))
            .validate_recovery_capsule_for_outcome(recovery_cut)
            .await?;
        Ok(Some(recovery_cut.clone()))
    }

    async fn preflight_cluster_recovery_cut<V, Fut>(
        &self,
        floor: &AuthorityOutcomeFloor,
        audited_outcomes: &[CheckpointOutcome],
        validate_artifacts: &V,
    ) -> Result<Option<CheckpointOutcome>, ClusterCheckpointAuthorityError>
    where
        V: Fn(CheckpointOutcome) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let recovery_cut = self
            .validate_cluster_recovery_cut(floor, audited_outcomes)
            .await?;
        if let Some(recovery_cut) = recovery_cut.as_ref() {
            validate_artifacts(recovery_cut.clone())
                .await
                .map_err(|error| {
                    DecisionError::Conflict(format!(
                        "cluster recovery cut epoch {} checkpoint {} failed durable recovery metadata preflight: {error}",
                        recovery_cut.epoch, recovery_cut.checkpoint_id
                    ))
                })?;
        }
        Ok(recovery_cut)
    }

    /// Exact continuity boundary for cluster outcomes compacted from the authority history.
    pub async fn cluster_outcome_retention_boundary(
        &self,
    ) -> Result<ClusterOutcomeRetentionBoundary, ClusterCheckpointAuthorityError> {
        let head = self.load_record().await?;
        Ok(ClusterOutcomeRetentionBoundary::from_floor(
            head.as_ref().and_then(|head| head.outcome_floor.as_ref()),
        ))
    }

    /// Read an existing cluster retention boundary after auditing its outcome chain and selected
    /// recovery capsule, without invoking a caller-supplied state-artifact preflight.
    ///
    /// # Errors
    ///
    /// Returns an error when the authority history or selected recovery capsule is invalid.
    pub async fn audited_cluster_outcome_retention_boundary(
        &self,
    ) -> Result<ClusterOutcomeRetentionBoundary, ClusterCheckpointAuthorityError> {
        Ok(self
            .validated_cluster_outcome_inventory(|_| async { Ok(()) })
            .await?
            .retention_boundary)
    }

    /// Read live outcomes and their retention boundary only after the selected live Commit passes
    /// the caller's durable recovery metadata preflight and both outcome heads and the floor remain
    /// unchanged.
    pub async fn validated_cluster_outcome_inventory<V, Fut>(
        &self,
        validate_artifacts: V,
    ) -> Result<ClusterOutcomeInventory, ClusterCheckpointAuthorityError>
    where
        V: Fn(CheckpointOutcome) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        loop {
            let current = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let snapshot = self.cached_audited_cluster_outcomes_from(&current).await?;
            if let Some(floor) = current.outcome_floor.as_ref() {
                self.preflight_cluster_recovery_cut(floor, &snapshot.outcomes, &validate_artifacts)
                    .await?;
            }
            let rechecked = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            if rechecked.outcome_head == current.outcome_head
                && rechecked.commit_head == current.commit_head
                && rechecked.outcome_floor == current.outcome_floor
            {
                return Ok(Self::cluster_outcome_inventory_from_audit(
                    Some(&current),
                    &snapshot.outcomes,
                ));
            }
            tokio::task::yield_now().await;
        }
    }

    /// Run one bounded recovery-capsule cleanup step below the durable artifact horizon.
    ///
    /// This is deliberately independent of floor publication: cleanup failure cannot revoke an
    /// already-authorized manifest/state retention horizon.
    pub async fn maintain_cluster_recovery_capsules(
        &self,
    ) -> Result<crate::checkpoint_decision::RecoveryCapsuleGcStep, ClusterCheckpointAuthorityError>
    {
        let current = self
            .load_record()
            .await?
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        let audited = self.cached_audited_cluster_outcomes_from(&current).await?;
        let Some(floor) = current.outcome_floor.as_ref() else {
            return Ok(crate::checkpoint_decision::RecoveryCapsuleGcStep {
                examined: 0,
                deleted: 0,
                quarantined: 0,
                pending: false,
            });
        };
        if floor.artifact_before_epoch == 0 {
            return Ok(crate::checkpoint_decision::RecoveryCapsuleGcStep {
                examined: 0,
                deleted: 0,
                quarantined: 0,
                pending: false,
            });
        }
        let mut known_live_digests = BTreeSet::new();
        known_live_digests.extend(
            audited
                .outcomes
                .iter()
                .filter(|outcome| outcome.epoch >= floor.artifact_before_epoch)
                .filter_map(|outcome| outcome.recovery_capsule.as_ref())
                .map(|reference| reference.sha256.clone()),
        );
        CheckpointDecisionStore::new(Arc::clone(&self.store))
            .sweep_recovery_capsules_step(floor.artifact_before_epoch, &known_live_digests)
            .await
            .map_err(Into::into)
    }

    /// Advance the cluster outcome floor through the exact next authority sequence.
    ///
    /// At least one live commit remains at or above the requested horizon. Outcome-bearing
    /// records below the floor and unreferenced old recovery capsules become eligible for
    /// best-effort deletion only after the floor is durable.
    pub async fn prune_cluster_outcomes_before<V, Fut>(
        &self,
        proof: &LeaderProof,
        before_epoch: u64,
        validate_artifacts: V,
    ) -> Result<u64, ClusterCheckpointAuthorityError>
    where
        V: Fn(CheckpointOutcome) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        if before_epoch == 0 {
            return Ok(self
                .cluster_outcome_retention_boundary()
                .await?
                .artifact_before_epoch);
        }
        if !proof.is_canonical() {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        loop {
            let current = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            if !current.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            if let Some(floor) = current
                .outcome_floor
                .as_ref()
                .filter(|floor| floor.artifact_before_epoch >= before_epoch)
            {
                self.schedule_history_prune();
                return Ok(floor.artifact_before_epoch);
            }
            let snapshot = self.cached_audited_cluster_outcomes_from(&current).await?;
            if !snapshot
                .outcomes
                .iter()
                .any(|outcome| outcome.epoch >= before_epoch && outcome.is_commit())
            {
                return Err(DecisionError::Conflict(format!(
                    "cannot advance cluster outcome floor to {before_epoch}: no live commit recovery cut would remain"
                ))
                .into());
            }
            let authority_before_epoch = current
                .outcome_floor
                .as_ref()
                .map_or(before_epoch, |floor| {
                    floor.authority_before_epoch.max(before_epoch)
                });
            let floor = self
                .build_cluster_outcome_floor(
                    &current,
                    &snapshot,
                    before_epoch,
                    authority_before_epoch,
                )
                .await?;
            self.preflight_cluster_recovery_cut(&floor, &snapshot.outcomes, &validate_artifacts)
                .await?;

            // Lease renewals and catalog seals may advance the shared sequence while the complete
            // recovery metadata preflight performs remote reads. They are harmless only when both the
            // outcome heads and retention floor remain exactly the same.
            let rechecked = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            if !rechecked.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            if rechecked.outcome_head != current.outcome_head
                || rechecked.commit_head != current.commit_head
                || rechecked.outcome_floor != current.outcome_floor
            {
                tokio::task::yield_now().await;
                continue;
            }
            let base_sequence = rechecked.lease.seq;
            let sequence = base_sequence
                .checked_add(1)
                .ok_or_else(|| LeaseError::Invalid("leader authority sequence exhausted".into()))?;
            let mut next = rechecked.preserve_with_lease(LeaderLease {
                seq: sequence,
                renewal_sequence: rechecked.lease.renewal_sequence,
                token: rechecked.lease.token,
                owner: rechecked.lease.owner.clone(),
                expires_at_ms: rechecked.lease.expires_at_ms,
                catalog_manifest: rechecked.lease.catalog_manifest.clone(),
            });
            next.outcome_floor = Some(floor.clone());
            next.validate()?;
            match self.create_authority_record(&next).await? {
                AuthorityCreateOutcome::Created => {
                    self.install_cluster_outcome_audit(
                        Self::cluster_outcome_audit_key(&next),
                        next.lease.seq,
                        Self::outcomes_retained_by_floor(&floor, &snapshot),
                    );
                    return Ok(before_epoch);
                }
                AuthorityCreateOutcome::ExistingIdentical => {
                    let winner_floor = next.outcome_floor.as_ref().ok_or_else(|| {
                        LeaseError::Invalid(
                            "durable floor winner lost its retention boundary".into(),
                        )
                    })?;
                    let winner_snapshot = self.cached_audited_cluster_outcomes_from(&next).await?;
                    self.preflight_cluster_recovery_cut(
                        winner_floor,
                        &winner_snapshot.outcomes,
                        &validate_artifacts,
                    )
                    .await?;
                    let confirmed = self
                        .load_record()
                        .await?
                        .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
                    if !confirmed.lease.matches_proof(proof) {
                        return Err(ClusterCheckpointAuthorityError::Fenced);
                    }
                    if confirmed.outcome_head == next.outcome_head
                        && confirmed.commit_head == next.commit_head
                        && confirmed.outcome_floor == next.outcome_floor
                    {
                        return Ok(before_epoch);
                    }
                    tokio::task::yield_now().await;
                }
                AuthorityCreateOutcome::Contended(winner) => {
                    if !winner.lease.matches_proof(proof) {
                        return Err(ClusterCheckpointAuthorityError::Fenced);
                    }
                    if winner.lease.seq <= base_sequence {
                        return Err(LeaseError::Invalid(
                            "outcome floor contention did not advance the authority sequence"
                                .into(),
                        )
                        .into());
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    /// Acquire an empty authority as a new term, or rotate the fencing token when this exact
    /// process incarnation already owns the durable head. Rival wall clocks never authorize
    /// takeover.
    ///
    /// # Errors
    /// Fails closed on invalid input, object-store I/O, or arithmetic exhaustion.
    pub async fn begin_new_term(
        &self,
        owner: &LeaderLeaseOwner,
        now_ms: i64,
    ) -> Result<LeaseOutcome, LeaseError> {
        self.acquire_or_renew_current_term_for_test_inner(owner, now_ms, SameOwnerToken::Rotate)
            .await
    }

    /// Renew only the exact durable authority term identified by `token`.
    ///
    /// # Errors
    /// Returns [`LeaseError::Fenced`] if the durable head is absent or belongs to another owner or
    /// term. Also fails closed on invalid input, object-store I/O, or arithmetic exhaustion.
    pub async fn renew_exact(
        &self,
        owner: &LeaderLeaseOwner,
        token: u64,
        now_ms: i64,
    ) -> Result<LeaseOutcome, LeaseError> {
        self.acquire_or_renew_current_term_for_test_inner(
            owner,
            now_ms,
            SameOwnerToken::Exact(token),
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn acquire_or_renew_current_term_for_test(
        &self,
        owner: &LeaderLeaseOwner,
        now_ms: i64,
    ) -> Result<LeaseOutcome, LeaseError> {
        self.acquire_or_renew_current_term_for_test_inner(owner, now_ms, SameOwnerToken::Preserve)
            .await
    }

    async fn acquire_or_renew_current_term_for_test_inner(
        &self,
        owner: &LeaderLeaseOwner,
        now_ms: i64,
        same_owner_token: SameOwnerToken,
    ) -> Result<LeaseOutcome, LeaseError> {
        owner.validate()?;
        self.ttl()?;
        let expires_at_ms = self.diagnostic_expiry(now_ms)?;
        loop {
            let current = self.load_record().await?;
            let candidate = match current {
                None => {
                    if matches!(same_owner_token, SameOwnerToken::Exact(_)) {
                        return Err(LeaseError::Fenced(
                            "exact leader renewal lost the durable authority head".into(),
                        ));
                    }
                    LeaderAuthorityRecord::initial(LeaderLease {
                        seq: 1,
                        renewal_sequence: 1,
                        token: 1,
                        owner: owner.clone(),
                        expires_at_ms,
                        catalog_manifest: None,
                    })
                }
                Some(record) if record.lease.owner == *owner => {
                    let token = match same_owner_token {
                        #[cfg(test)]
                        SameOwnerToken::Preserve => record.lease.token,
                        SameOwnerToken::Rotate => {
                            record.lease.token.checked_add(1).ok_or_else(|| {
                                LeaseError::Invalid("leader fencing token exhausted".into())
                            })?
                        }
                        SameOwnerToken::Exact(expected) if expected == record.lease.token => {
                            expected
                        }
                        SameOwnerToken::Exact(_) => {
                            return Err(LeaseError::Fenced(
                                "exact leader renewal was superseded by a newer local term".into(),
                            ));
                        }
                    };
                    let lease = LeaderLease {
                        seq: record.lease.seq.checked_add(1).ok_or_else(|| {
                            LeaseError::Invalid("lease sequence exhausted".into())
                        })?,
                        renewal_sequence: record.lease.renewal_sequence.checked_add(1).ok_or_else(
                            || LeaseError::Invalid("lease renewal sequence exhausted".into()),
                        )?,
                        token,
                        owner: owner.clone(),
                        expires_at_ms,
                        catalog_manifest: record.lease.catalog_manifest.clone(),
                    };
                    record.preserve_with_lease(lease)
                }
                Some(record) => {
                    if matches!(same_owner_token, SameOwnerToken::Exact(_)) {
                        return Err(LeaseError::Fenced(
                            "exact leader renewal was superseded by a rival owner".into(),
                        ));
                    }
                    return Ok(LeaseOutcome::Held(record.lease));
                }
            };
            match self.create_authority_record(&candidate).await? {
                AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                    return Ok(LeaseOutcome::Acquired(candidate.lease));
                }
                AuthorityCreateOutcome::Contended(winner) if winner.lease.owner == *owner => {
                    if matches!(
                        same_owner_token,
                        SameOwnerToken::Exact(expected) if winner.lease.token != expected
                    ) {
                        return Err(LeaseError::Fenced(
                            "exact leader renewal lost a same-owner CAS race".into(),
                        ));
                    }
                    tokio::task::yield_now().await;
                }
                AuthorityCreateOutcome::Contended(winner) => {
                    if matches!(same_owner_token, SameOwnerToken::Exact(_)) {
                        return Err(LeaseError::Fenced(
                            "exact leader renewal lost a rival-owner CAS race".into(),
                        ));
                    }
                    return Ok(LeaseOutcome::Held(winner.lease));
                }
            }
        }
    }

    /// Start a candidate-local observation of a rival durable liveness identity.
    ///
    /// # Errors
    /// Rejects malformed state or an observation of the candidate itself.
    pub fn observe_rival(
        &self,
        owner: &LeaderLeaseOwner,
        lease: &LeaderLease,
    ) -> Result<LeaderLeaseObservation, LeaseError> {
        owner.validate()?;
        lease.validate()?;
        self.ttl()?;
        if lease.owner == *owner {
            return Err(LeaseError::Invalid(
                "leader takeover observation must belong to a rival".into(),
            ));
        }
        Ok(LeaderLeaseObservation {
            lease: lease.clone(),
            started: Instant::now(),
        })
    }

    /// Take over only after the rival's owner, fencing token, and renewal sequence remained
    /// current for a full TTL on the candidate's monotonic clock.
    ///
    /// # Errors
    /// Fails closed on early observation, invalid state, I/O, or arithmetic exhaustion.
    pub async fn try_takeover(
        &self,
        owner: &LeaderLeaseOwner,
        observation: &LeaderLeaseObservation,
        now_ms: i64,
    ) -> Result<LeaseOutcome, LeaseError> {
        owner.validate()?;
        observation.lease.validate()?;
        let ttl = self.ttl()?;
        if observation.lease.owner == *owner {
            return Err(LeaseError::Invalid(
                "leader takeover observation must belong to a rival".into(),
            ));
        }
        if observation.started.elapsed() < ttl {
            return Ok(LeaseOutcome::Held(observation.lease.clone()));
        }
        let expires_at_ms = self.diagnostic_expiry(now_ms)?;
        loop {
            let current = self
                .load_record()
                .await?
                .ok_or_else(|| LeaseError::Invalid("observed leader lease disappeared".into()))?;
            if !current.lease.has_same_liveness_identity(&observation.lease) {
                return Ok(LeaseOutcome::Held(current.lease));
            }
            let candidate_lease = LeaderLease {
                seq: current
                    .lease
                    .seq
                    .checked_add(1)
                    .ok_or_else(|| LeaseError::Invalid("lease sequence exhausted".into()))?,
                renewal_sequence: current.lease.renewal_sequence.checked_add(1).ok_or_else(
                    || LeaseError::Invalid("lease renewal sequence exhausted".into()),
                )?,
                token: current
                    .lease
                    .token
                    .checked_add(1)
                    .ok_or_else(|| LeaseError::Invalid("fencing token exhausted".into()))?,
                owner: owner.clone(),
                expires_at_ms,
                catalog_manifest: current.lease.catalog_manifest.clone(),
            };
            let candidate = current.preserve_with_lease(candidate_lease);
            match self.create_authority_record(&candidate).await? {
                AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                    return Ok(LeaseOutcome::Acquired(candidate.lease));
                }
                AuthorityCreateOutcome::Contended(winner)
                    if winner.lease.has_same_liveness_identity(&observation.lease) =>
                {
                    tokio::task::yield_now().await;
                }
                AuthorityCreateOutcome::Contended(winner) => {
                    return Ok(LeaseOutcome::Held(winner.lease));
                }
            }
        }
    }
}

/// Renewal timings for the leader lease.
#[derive(Debug, Clone, Copy)]
pub struct LeaderLeaseConfig {
    /// Lease lifetime.
    pub ttl: Duration,
    /// Renewal cadence, strictly below the lifetime.
    pub renew_interval: Duration,
}

impl Default for LeaderLeaseConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(5),
            renew_interval: Duration::from_secs(2),
        }
    }
}

/// Whether the exact owner has a durable record and a live process-local deadline.
#[must_use]
pub fn lease_grants_leadership(
    lease: &Option<LeaderLease>,
    owner: &LeaderLeaseOwner,
    deadline: &LeaseDeadline,
) -> bool {
    deadline.is_live() && matches!(lease, Some(lease) if lease.owner == *owner)
}

/// Whether a captured proof still matches the current exact grant and local deadline.
#[must_use]
pub fn lease_grants_proof(
    lease: &Option<LeaderLease>,
    owner: &LeaderLeaseOwner,
    deadline: &LeaseDeadline,
    proof: &LeaderProof,
) -> bool {
    deadline.is_live()
        && proof.owner == owner.proof_owner()
        && matches!(lease, Some(lease) if lease.owner == *owner && lease.matches_proof(proof))
}

/// Acquires and renews leadership while candidacy remains true.
pub struct LeaderLeaseManager {
    store: Arc<LeaderLeaseStore>,
    owner: LeaderLeaseOwner,
    config: LeaderLeaseConfig,
    lease_tx: watch::Sender<Option<LeaderLease>>,
    deadline: Arc<LeaseDeadline>,
}

#[cfg(feature = "cluster")]
enum LeaseOperationEvent {
    Shutdown,
    Candidacy(Result<(), watch::error::RecvError>),
    Deadline,
    Completed {
        result: Result<LeaseOutcome, LeaseError>,
        valid_until: tokio::time::Instant,
    },
}

#[cfg(feature = "cluster")]
async fn wait_for_deadline(deadline: Option<tokio::time::Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    } else {
        std::future::pending::<()>().await;
    }
}

impl std::fmt::Debug for LeaderLeaseManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LeaderLeaseManager")
            .field("owner", &self.owner)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl LeaderLeaseManager {
    /// Construct a manager bound to this boot's acquired stable-node process lease.
    ///
    /// # Errors
    /// Rejects invalid ownership or inconsistent renewal timings.
    pub fn new(
        store: Arc<LeaderLeaseStore>,
        process_lease: &ProcessLease,
        config: LeaderLeaseConfig,
    ) -> Result<Self, LeaseError> {
        let owner = LeaderLeaseOwner::from_process_lease(process_lease)?;
        let ttl_ms = i64::try_from(config.ttl.as_millis())
            .map_err(|_| LeaseError::Invalid("lease TTL exceeds diagnostic range".into()))?;
        let exact_ttl = u64::try_from(ttl_ms)
            .ok()
            .map(Duration::from_millis)
            .is_some_and(|ttl| ttl == config.ttl);
        if ttl_ms <= 0
            || !exact_ttl
            || config.renew_interval.is_zero()
            || config.renew_interval >= config.ttl
            || ttl_ms != store.ttl_ms
        {
            return Err(LeaseError::Invalid(
                "manager requires a renewal interval below the store's matching TTL".into(),
            ));
        }
        let (lease_tx, _lease_rx) = watch::channel(None);
        Ok(Self {
            store,
            owner,
            config,
            lease_tx,
            deadline: Arc::new(LeaseDeadline::uninitialized()),
        })
    }

    /// Exact process incarnation this manager may publish.
    #[must_use]
    pub fn owner(&self) -> &LeaderLeaseOwner {
        &self.owner
    }

    /// Subscribe to the locally authorized leader record.
    #[must_use]
    pub fn lease_watch(&self) -> watch::Receiver<Option<LeaderLease>> {
        self.lease_tx.subscribe()
    }

    /// Shared local-monotonic liveness gate for leader hot paths.
    #[must_use]
    pub fn deadline(&self) -> Arc<LeaseDeadline> {
        Arc::clone(&self.deadline)
    }

    #[cfg(feature = "cluster")]
    fn withdraw(&self) {
        self.deadline.withdraw();
        self.lease_tx.send_replace(None);
    }

    #[cfg(feature = "cluster")]
    fn fence(&self) {
        self.deadline.fence();
        self.lease_tx.send_replace(None);
    }

    #[cfg(feature = "cluster")]
    async fn attempt_lease(
        &self,
        shutdown: &tokio_util::sync::CancellationToken,
        candidate: &mut watch::Receiver<LeaderCandidacy>,
        valid_until: Option<tokio::time::Instant>,
        observation: Option<&LeaderLeaseObservation>,
        held_token: Option<u64>,
    ) -> LeaseOperationEvent {
        let Some(attempt_valid_until) = tokio::time::Instant::now().checked_add(self.config.ttl)
        else {
            return LeaseOperationEvent::Deadline;
        };
        let operation_deadline = valid_until.map_or(attempt_valid_until, |current| {
            current.min(attempt_valid_until)
        });
        let operation = async {
            if let Some(observation) = observation {
                self.store
                    .try_takeover(&self.owner, observation, now_millis())
                    .await
            } else if let Some(token) = held_token {
                self.store
                    .renew_exact(&self.owner, token, now_millis())
                    .await
            } else {
                self.store.begin_new_term(&self.owner, now_millis()).await
            }
        };
        tokio::select! {
            biased;
            () = shutdown.cancelled() => LeaseOperationEvent::Shutdown,
            changed = candidate.changed() => LeaseOperationEvent::Candidacy(changed),
            () = wait_for_deadline(Some(operation_deadline)) => LeaseOperationEvent::Deadline,
            result = operation => LeaseOperationEvent::Completed {
                result,
                valid_until: attempt_valid_until,
            },
        }
    }

    #[cfg(feature = "cluster")]
    async fn wait_for_candidacy_change(
        &self,
        shutdown: &tokio_util::sync::CancellationToken,
        candidate: &mut watch::Receiver<LeaderCandidacy>,
    ) -> bool {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                self.fence();
                false
            }
            changed = candidate.changed() => {
                if changed.is_err() {
                    self.fence();
                    return false;
                }
                true
            }
        }
    }

    #[cfg(feature = "cluster")]
    async fn run(
        self,
        shutdown: tokio_util::sync::CancellationToken,
        mut candidate: watch::Receiver<LeaderCandidacy>,
    ) {
        let mut ticker = tokio::time::interval(self.config.renew_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut valid_until: Option<tokio::time::Instant> = None;
        let mut observation: Option<LeaderLeaseObservation> = None;
        // A newly constructed manager and every locally withdrawn grant need a new durable
        // fencing token. Only uninterrupted renewals may preserve the current token.
        let mut held_token = None;
        let mut candidacy_generation = candidate.borrow().generation;

        loop {
            let candidacy = *candidate.borrow_and_update();
            if candidacy.generation != candidacy_generation {
                self.withdraw();
                observation = None;
                valid_until = None;
                held_token = None;
                candidacy_generation = candidacy.generation;
            }
            if !candidacy.eligible {
                self.withdraw();
                observation = None;
                valid_until = None;
                held_token = None;
                if !self
                    .wait_for_candidacy_change(&shutdown, &mut candidate)
                    .await
                {
                    return;
                }
                continue;
            }

            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    self.fence();
                    return;
                }
                changed = candidate.changed() => {
                    if changed.is_err() {
                        self.fence();
                        return;
                    }
                    continue;
                }
                () = wait_for_deadline(valid_until) => {
                    self.fence();
                    return;
                }
                _ = ticker.tick() => {}
            }

            let (result, attempt_valid_until) = match self
                .attempt_lease(
                    &shutdown,
                    &mut candidate,
                    valid_until,
                    observation.as_ref(),
                    held_token,
                )
                .await
            {
                LeaseOperationEvent::Shutdown | LeaseOperationEvent::Deadline => {
                    self.fence();
                    return;
                }
                LeaseOperationEvent::Candidacy(changed) => {
                    if changed.is_err() {
                        self.fence();
                        return;
                    }
                    continue;
                }
                LeaseOperationEvent::Completed {
                    result,
                    valid_until: attempt_valid_until,
                } => {
                    let response_at = tokio::time::Instant::now();
                    if response_at >= attempt_valid_until
                        || valid_until.is_some_and(|current| response_at >= current)
                    {
                        self.fence();
                        return;
                    }
                    (result, attempt_valid_until)
                }
            };

            match result {
                Ok(LeaseOutcome::Acquired(lease)) if lease.owner == self.owner => {
                    let publication_at = tokio::time::Instant::now();
                    if publication_at >= attempt_valid_until
                        || valid_until.is_some_and(|current| publication_at >= current)
                    {
                        self.fence();
                        return;
                    }
                    observation = None;
                    valid_until = Some(attempt_valid_until);
                    held_token = Some(lease.token);
                    self.deadline.extend_until(attempt_valid_until.into_std());
                    self.lease_tx.send_replace(Some(lease));
                }
                Ok(LeaseOutcome::Acquired(_)) => {
                    self.fence();
                    return;
                }
                Ok(LeaseOutcome::Held(rival)) => {
                    self.withdraw();
                    valid_until = None;
                    held_token = None;
                    let unchanged = observation
                        .as_ref()
                        .is_some_and(|observed| observed.lease.has_same_liveness_identity(&rival));
                    if !unchanged {
                        match self.store.observe_rival(&self.owner, &rival) {
                            Ok(new_observation) => observation = Some(new_observation),
                            Err(error) => {
                                tracing::warn!(%error, "leader lease observation rejected");
                                observation = None;
                            }
                        }
                    }
                }
                Err(LeaseError::Fenced(error)) => {
                    tracing::warn!(%error, "leader lease renewal was fenced");
                    self.fence();
                    return;
                }
                Err(error) => {
                    tracing::warn!(%error, "leader lease operation failed");
                }
            }
        }
    }

    /// Spawn the renewal loop. Loss of candidacy withdraws the current local grant so this
    /// manager can contend again later. Shutdown, a missed renewal, or an invalid lease outcome
    /// terminally fences the manager.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn spawn(
        self,
        shutdown: tokio_util::sync::CancellationToken,
        candidate: watch::Receiver<LeaderCandidacy>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(self.run(shutdown, candidate))
    }
}

#[cfg(test)]
mod tests {
    use super::super::controller::{RecoveryFault, RecoveryRound};
    use super::super::snapshot::AssignmentSnapshot;
    use super::*;
    use async_trait::async_trait;
    use futures::StreamExt as FuturesStreamExt;
    use object_store::memory::InMemory;
    use std::collections::BTreeMap;

    fn owner(node: u64, boot: u128, process_term: u64) -> LeaderLeaseOwner {
        LeaderLeaseOwner {
            node: NodeId(node),
            boot: Uuid::from_u128(boot),
            process_term,
        }
    }

    async fn accept_recovery_artifacts(_: CheckpointOutcome) -> Result<(), String> {
        Ok(())
    }

    fn process(owner: &LeaderLeaseOwner) -> ProcessLease {
        ProcessLease {
            node: owner.node,
            owner: owner.boot,
            term: owner.process_term,
            seq: 1,
            expires_at_ms: 1,
        }
    }

    fn store(ttl_ms: i64) -> LeaderLeaseStore {
        LeaderLeaseStore::new(Arc::new(InMemory::new()), ttl_ms)
    }

    #[test]
    fn live_authority_link_budget_is_exact() {
        let mut traversed = 0;
        for _ in 0..MAX_LIVE_AUTHORITY_LINKS {
            assert!(consume_live_authority_link(&mut traversed));
        }
        assert!(!consume_live_authority_link(&mut traversed));
        assert_eq!(traversed, MAX_LIVE_AUTHORITY_LINKS);
    }

    #[test]
    fn prune_latch_guard_releases_on_every_drop_path() {
        let latch = Arc::new(AtomicBool::new(true));
        {
            let _guard = PruneLatchGuard(Arc::clone(&latch));
        }
        assert!(!latch.load(Ordering::Acquire));
    }

    fn assignment_fence(owner: &LeaderLeaseOwner) -> CheckpointAssignmentFence {
        CheckpointAssignmentFence::from_owner_map(
            1,
            &[owner.node.0],
            vec![crate::checkpoint::CheckpointParticipant {
                node_id: owner.node.0,
                boot_incarnation: owner.boot,
            }],
        )
        .unwrap()
    }

    fn recovery_fault_publisher(
        node_id: u64,
        boot_incarnation: u128,
        process_term: u64,
    ) -> RecoveryFaultPublisher {
        RecoveryFaultPublisher {
            participant: crate::checkpoint::CheckpointParticipant {
                node_id,
                boot_incarnation: Uuid::from_u128(boot_incarnation),
            },
            process_term,
        }
    }

    fn owner_recovery_fault_publisher(owner: &LeaderLeaseOwner) -> RecoveryFaultPublisher {
        RecoveryFaultPublisher {
            participant: crate::checkpoint::CheckpointParticipant {
                node_id: owner.node.0,
                boot_incarnation: owner.boot,
            },
            process_term: owner.process_term,
        }
    }

    async fn recovery_release_terminal(
        store: &LeaderLeaseStore,
        lease: &LeaderLease,
        generation: u64,
        epoch: u64,
    ) -> RecoveryAnnouncement {
        let inventory = store.recovery_fault_inventory().await.unwrap();
        assert!(!inventory.faults().is_empty());
        let round = RecoveryRound::new(
            generation,
            lease.proof(),
            assignment_fence(&lease.owner),
            Vec::new(),
            inventory.revision(),
            inventory.faults().to_vec(),
        )
        .unwrap();
        RecoveryAnnouncement {
            round,
            phase: RecoverPhase::ReleaseCommitted { epoch },
        }
    }

    async fn recovery_release_terminal_after_owner_fault(
        store: &LeaderLeaseStore,
        lease: &LeaderLease,
        generation: u64,
        epoch: u64,
    ) -> RecoveryAnnouncement {
        assert_eq!(
            store
                .record_recovery_fault(owner_recovery_fault_publisher(&lease.owner), generation,)
                .await
                .unwrap(),
            RecordRecoveryFaultResult::Active
        );
        recovery_release_terminal(store, lease, generation, epoch).await
    }

    async fn commit_recovery_release(
        store: &LeaderLeaseStore,
        lease: &LeaderLease,
        terminal: &RecoveryAnnouncement,
    ) -> RecoveryReleaseTerminalRef {
        let reference = store
            .stage_recovery_release_terminal(terminal)
            .await
            .unwrap();
        assert_eq!(
            store
                .record_recovery_release_commit(&lease.proof(), reference.clone())
                .await
                .unwrap(),
            RecordRecoveryReleaseCommitResult::Created(reference.clone())
        );
        reference
    }

    fn assignment_drain_transition(
        owner: &LeaderLeaseOwner,
        leader_proof: LeaderProof,
    ) -> AssignmentDrainTransition {
        assignment_drain_transition_at(owner, leader_proof, 2)
    }

    fn assignment_drain_transition_at(
        owner: &LeaderLeaseOwner,
        leader_proof: LeaderProof,
        target_version: u64,
    ) -> AssignmentDrainTransition {
        assert!(target_version > 1);
        let predecessor = CheckpointAssignmentFence::from_owner_map(
            target_version - 1,
            &[owner.node.0],
            vec![crate::checkpoint::CheckpointParticipant {
                node_id: owner.node.0,
                boot_incarnation: owner.boot,
            }],
        )
        .unwrap();
        let target = CheckpointAssignmentFence::from_owner_map(
            target_version,
            &[owner.node.0],
            predecessor.participants.clone(),
        )
        .unwrap();
        AssignmentDrainTransition::new(predecessor, target, leader_proof).unwrap()
    }

    async fn assignment_recovery_decision(
        store: &LeaderLeaseStore,
        predecessor_version: u64,
        predecessor_processes: &[LeaderLeaseOwner],
        target_processes: &[LeaderLeaseOwner],
        leader_proof: LeaderProof,
        updated_at_ms: i64,
    ) -> AssignmentRecoveryDecision {
        assert!(!predecessor_processes.is_empty());
        assert!(!target_processes.is_empty());
        assert!(predecessor_processes
            .windows(2)
            .all(|pair| pair[0].node.0 < pair[1].node.0));
        assert!(target_processes
            .windows(2)
            .all(|pair| pair[0].node.0 < pair[1].node.0));
        let vnode_count = predecessor_processes.len().max(target_processes.len());
        let predecessor_owners: Vec<_> = (0..vnode_count)
            .map(|index| {
                predecessor_processes[index % predecessor_processes.len()]
                    .node
                    .0
            })
            .collect();
        let target_owners: Vec<_> = (0..vnode_count)
            .map(|index| target_processes[index % target_processes.len()].node.0)
            .collect();
        let participants = |processes: &[LeaderLeaseOwner]| {
            processes
                .iter()
                .map(|process| crate::checkpoint::CheckpointParticipant {
                    node_id: process.node.0,
                    boot_incarnation: process.boot,
                })
                .collect::<Vec<_>>()
        };
        let snapshots = AssignmentSnapshotStore::new(Arc::clone(&store.store));
        let mut durable_version = snapshots
            .load()
            .await
            .unwrap()
            .map_or(0, |snapshot| snapshot.version);
        while durable_version < predecessor_version {
            let version = durable_version.checked_add(1).unwrap();
            let snapshot = AssignmentSnapshot {
                version,
                partitioning_abi_version: crate::state::PARTITIONING_ABI_VERSION,
                vnodes: predecessor_owners
                    .iter()
                    .copied()
                    .enumerate()
                    .map(|(vnode, node)| (u32::try_from(vnode).unwrap(), NodeId(node)))
                    .collect(),
                participants: participants(predecessor_processes),
                updated_at_ms: i64::try_from(version).unwrap(),
                draining: false,
                drain_transition: None,
            };
            if version == 1 {
                let _ = snapshots.save_if_absent(&snapshot).await.unwrap();
            } else {
                let _ = snapshots
                    .save_if_version(&snapshot, durable_version)
                    .await
                    .unwrap();
            }
            durable_version = snapshots.load().await.unwrap().unwrap().version;
        }
        let predecessor_snapshot = snapshots.load().await.unwrap().unwrap();
        assert_eq!(predecessor_snapshot.version, predecessor_version);
        let predecessor = predecessor_snapshot.assignment_fence().unwrap();
        assert_eq!(
            predecessor,
            CheckpointAssignmentFence::from_owner_map(
                predecessor_version,
                &predecessor_owners,
                participants(predecessor_processes),
            )
            .unwrap()
        );
        let target_version = predecessor_version.checked_add(1).unwrap();
        let target_snapshot = AssignmentSnapshot {
            version: target_version,
            partitioning_abi_version: crate::state::PARTITIONING_ABI_VERSION,
            vnodes: target_owners
                .iter()
                .copied()
                .enumerate()
                .map(|(vnode, node)| (u32::try_from(vnode).unwrap(), NodeId(node)))
                .collect(),
            participants: participants(target_processes),
            updated_at_ms,
            draining: false,
            drain_transition: None,
        };
        let target = target_snapshot.assignment_fence().unwrap();
        let proposal = snapshots
            .stage_recovery_proposal(&target_snapshot)
            .await
            .unwrap();
        let removed_process_fences = predecessor_processes
            .iter()
            .enumerate()
            .filter(|(_, process)| {
                target.participant_incarnation(process.node.0) != Some(process.boot)
            })
            .map(|(index, process)| {
                let predecessor = ProcessLease {
                    node: process.node,
                    owner: process.boot,
                    term: process.process_term,
                    seq: u64::try_from(index).unwrap().saturating_add(10),
                    expires_at_ms: 1,
                };
                let successor_owner = target
                    .participant_incarnation(process.node.0)
                    .unwrap_or_else(|| Uuid::from_u128(10_000 + u128::from(process.node.0)));
                let successor = ProcessLease {
                    node: process.node,
                    owner: successor_owner,
                    term: predecessor.term.checked_add(1).unwrap(),
                    seq: predecessor.seq.checked_add(1).unwrap(),
                    expires_at_ms: 2,
                };
                ProcessLeaseFence::new(predecessor, successor).unwrap()
            })
            .collect();
        AssignmentRecoveryDecision::new(
            predecessor,
            target,
            proposal,
            removed_process_fences,
            leader_proof,
        )
        .unwrap()
    }

    fn digest(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }

    fn recovery_capsule_path(reference: &RecoveryCapsuleRef) -> OsPath {
        OsPath::from(format!(
            "checkpoint-recovery-capsules/epoch={:020}/checkpoint={:020}/sha256={}",
            reference.epoch, reference.checkpoint_id, reference.sha256
        ))
    }

    async fn recovery_capsule(
        store: &LeaderLeaseStore,
        fence: &CheckpointAssignmentFence,
        epoch: u64,
        checkpoint_id: u64,
    ) -> RecoveryCapsuleRef {
        let decisions = CheckpointDecisionStore::new(Arc::clone(&store.store));
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let portable_state_sha256 = digest(9);
        let capsule = crate::checkpoint::ClusterRecoveryCapsule {
            version: crate::checkpoint::CLUSTER_RECOVERY_CAPSULE_VERSION,
            attempt: crate::state::CheckpointAttempt::new(epoch, checkpoint_id),
            deployment_id,
            pipeline_identity: crate::checkpoint::PipelineIdentity::empty(),
            assignment_fence: fence.clone(),
            seal_inventory_sha256: digest(2),
            participants: vec![crate::checkpoint::ParticipantRecoveryRef {
                participant_id: fence.participants[0].node_id,
                readiness_sha256: digest(3),
                manifest_sha256: digest(4),
                portable_state_sha256: portable_state_sha256.clone(),
            }],
            source_offsets: std::collections::BTreeMap::new(),
            source_metadata: std::collections::BTreeMap::new(),
            source_assignment_versions: std::collections::BTreeMap::new(),
            source_watermarks: std::collections::BTreeMap::new(),
            cluster_watermark: crate::checkpoint::CheckpointWatermark::Uninitialized,
            recovery_watermark_frontier: None,
            portable_state_sha256,
        };
        decisions.create_recovery_capsule(&capsule).await.unwrap()
    }

    async fn record_commit(
        store: &LeaderLeaseStore,
        proof: &LeaderProof,
        fence: &CheckpointAssignmentFence,
        epoch: u64,
        checkpoint_id: u64,
    ) -> RecordOutcomeResult {
        let capsule = recovery_capsule(store, fence, epoch, checkpoint_id).await;
        store
            .record_cluster_outcome(
                proof,
                epoch,
                checkpoint_id,
                fence.clone(),
                CheckpointVerdict::Commit,
                Some(capsule),
            )
            .await
            .unwrap()
    }

    async fn retention_test_store(
        ttl_ms: i64,
    ) -> (Arc<LeaderLeaseStore>, LeaderLeaseOwner, LeaderProof) {
        let store = Arc::new(store(ttl_ms));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        record_commit(store.as_ref(), &proof, &fence, 1, 10).await;
        record_commit(store.as_ref(), &proof, &fence, 3, 30).await;
        (store, incumbent, proof)
    }

    async fn disable_history_pruning_for_test(store: &LeaderLeaseStore) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.prune_running.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        store.prune_running.store(true, Ordering::Release);
    }

    #[tokio::test]
    async fn exact_active_recovery_fault_retry_is_idempotent() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(_) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
            panic!("empty authority must be acquired");
        };
        let publisher = owner_recovery_fault_publisher(&incumbent);

        assert_eq!(
            store.record_recovery_fault(publisher, 7).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let admitted = store.load_record().await.unwrap().unwrap();
        assert_eq!(
            store.record_recovery_fault(publisher, 7).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );

        assert_eq!(store.load_record().await.unwrap().unwrap(), admitted);
        assert_eq!(
            store.recovery_fault_inventory().await.unwrap().faults(),
            &[RecoveryFault {
                reporter: incumbent.node,
                sequence: 2,
            }]
        );
    }

    #[tokio::test]
    async fn ambiguous_recovery_fault_create_reconciles_without_a_duplicate_sequence() {
        let (raw, store) = ambiguous_once_at(1_000, lease_path(2));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(_) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
            panic!("empty authority must be acquired");
        };
        let publisher = owner_recovery_fault_publisher(&incumbent);

        assert_eq!(
            store.record_recovery_fault(publisher, 7).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        assert!(raw
            .did_return_ambiguous
            .load(std::sync::atomic::Ordering::Acquire));
        let admitted = store.load_record().await.unwrap().unwrap();
        assert_eq!(admitted.lease.seq, 2);
        assert_eq!(admitted.recovery_fault_revision, 2);
        assert_eq!(admitted.recovery_fault_slots.len(), 1);
        assert_eq!(admitted.recovery_fault_slots[0].fault_sequence, 2);

        assert_eq!(
            store.record_recovery_fault(publisher, 7).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        assert_eq!(store.load_record().await.unwrap().unwrap(), admitted);
    }

    #[tokio::test]
    async fn exact_recovery_fault_retry_observes_a_terminal_bound_tombstone() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap()
        else {
            panic!("empty authority must be acquired");
        };
        let publisher = owner_recovery_fault_publisher(&incumbent);
        assert_eq!(
            store.record_recovery_fault(publisher, 7).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let terminal = recovery_release_terminal(&store, &lease, 1, 0).await;
        commit_recovery_release(&store, &lease, &terminal).await;
        let tombstone = store.load_record().await.unwrap().unwrap();
        assert_eq!(tombstone.recovery_fault_revision, tombstone.lease.seq);
        assert!(!tombstone.recovery_fault_slots[0].active);
        let mut stale_revision = tombstone.clone();
        stale_revision.recovery_fault_revision -= 1;
        assert!(stale_revision.validate().is_err());
        let mut active_slot = tombstone.clone();
        active_slot.recovery_fault_slots[0].active = true;
        assert!(active_slot.validate().is_err());
        let mut advanced_lease = tombstone.lease.clone();
        advanced_lease.seq += 1;
        let mut detached_revision = tombstone.preserve_with_lease(advanced_lease);
        detached_revision.recovery_fault_revision = detached_revision.lease.seq;
        assert!(detached_revision.validate().is_err());
        assert!(store
            .authorize_recovery_release(publisher, &terminal)
            .await
            .unwrap());
        assert!(!store
            .authorize_recovery_release(recovery_fault_publisher(1, 2, 2), &terminal)
            .await
            .unwrap());
        assert_eq!(store.load_record().await.unwrap().unwrap(), tombstone);

        assert_eq!(
            store.record_recovery_fault(publisher, 7).await.unwrap(),
            RecordRecoveryFaultResult::AlreadyCleared
        );
        assert_eq!(store.load_record().await.unwrap().unwrap(), tombstone);
        assert!(store
            .recovery_fault_inventory()
            .await
            .unwrap()
            .faults()
            .is_empty());
    }

    #[tokio::test]
    async fn a_new_fault_blocks_authorization_from_the_previous_release() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap()
        else {
            panic!("empty authority must be acquired");
        };
        let released = owner_recovery_fault_publisher(&incumbent);
        assert_eq!(
            store.record_recovery_fault(released, 1).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let terminal = recovery_release_terminal(&store, &lease, 1, 0).await;
        commit_recovery_release(&store, &lease, &terminal).await;

        let newer = recovery_fault_publisher(2, 2, 1);
        assert_eq!(
            store.record_recovery_fault(newer, 1).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        assert!(!store
            .authorize_recovery_release(released, &terminal)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn recovery_release_retains_only_exact_stopped_fault_publishers() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap()
        else {
            panic!("empty authority must be acquired");
        };
        let evidence = recovery_fault_publisher(2, 2, 1);
        let unavailable = recovery_fault_publisher(3, 3, 1);
        assert_eq!(
            store.record_recovery_fault(evidence, 1).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        assert_eq!(
            store.record_recovery_fault(unavailable, 1).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let inventory = store.recovery_fault_inventory().await.unwrap();
        let round = RecoveryRound::new(
            1,
            lease.proof(),
            assignment_fence(&incumbent),
            vec![evidence.participant],
            inventory.revision(),
            inventory.faults().to_vec(),
        )
        .unwrap();
        let terminal = RecoveryAnnouncement {
            round,
            phase: RecoverPhase::ReleaseCommitted { epoch: 0 },
        };

        commit_recovery_release(&store, &lease, &terminal).await;
        let head = store.load_record().await.unwrap().unwrap();
        assert_eq!(head.recovery_fault_slots.len(), 1);
        assert_eq!(head.recovery_fault_slots[0].publisher, evidence);
        assert!(!head.recovery_fault_slots[0].active);
        assert!(store
            .authorize_recovery_release(evidence, &terminal)
            .await
            .unwrap());
        assert!(!store
            .authorize_recovery_release(unavailable, &terminal)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn older_same_boot_recovery_fault_is_covered_by_the_newer_request() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(_) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
            panic!("empty authority must be acquired");
        };
        let publisher = owner_recovery_fault_publisher(&incumbent);
        assert_eq!(
            store.record_recovery_fault(publisher, 2).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let newer = store.load_record().await.unwrap().unwrap();

        assert_eq!(
            store.record_recovery_fault(publisher, 1).await.unwrap(),
            RecordRecoveryFaultResult::CoveredByNewerRequest
        );
        assert_eq!(store.load_record().await.unwrap().unwrap(), newer);
    }

    #[tokio::test]
    async fn lower_term_recovery_fault_is_superseded() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(_) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
            panic!("empty authority must be acquired");
        };
        let current = recovery_fault_publisher(1, 2, 2);
        assert_eq!(
            store.record_recovery_fault(current, 1).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let admitted = store.load_record().await.unwrap().unwrap();

        assert_eq!(
            store
                .record_recovery_fault(recovery_fault_publisher(1, 1, 1), u64::MAX)
                .await
                .unwrap(),
            RecordRecoveryFaultResult::Superseded
        );
        assert_eq!(store.load_record().await.unwrap().unwrap(), admitted);
    }

    #[tokio::test]
    async fn higher_term_recovery_fault_replaces_the_stable_node_slot() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(_) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
            panic!("empty authority must be acquired");
        };
        let first = recovery_fault_publisher(1, 1, 1);
        let replacement = recovery_fault_publisher(1, 2, 2);
        assert_eq!(
            store.record_recovery_fault(first, 10).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );

        assert_eq!(
            store.record_recovery_fault(replacement, 1).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let head = store.load_record().await.unwrap().unwrap();
        assert_eq!(head.recovery_fault_slots.len(), 1);
        assert_eq!(head.recovery_fault_slots[0].publisher, replacement);
        assert_eq!(head.recovery_fault_slots[0].request_sequence, 1);
        assert_eq!(head.recovery_fault_slots[0].fault_sequence, 3);
        assert!(head.recovery_fault_slots[0].active);
        assert_eq!(
            store.record_recovery_fault(first, u64::MAX).await.unwrap(),
            RecordRecoveryFaultResult::Superseded
        );
    }

    #[tokio::test]
    async fn same_term_different_boot_recovery_fault_is_rejected() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(_) = store.begin_new_term(&incumbent, 0).await.unwrap() else {
            panic!("empty authority must be acquired");
        };
        assert_eq!(
            store
                .record_recovery_fault(recovery_fault_publisher(1, 1, 1), 1)
                .await
                .unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let admitted = store.load_record().await.unwrap().unwrap();

        let error = store
            .record_recovery_fault(recovery_fault_publisher(1, 2, 1), 2)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ClusterCheckpointAuthorityError::Authority(LeaseError::Invalid(message))
                if message.contains("two recovery fault publishers")
        ));
        assert_eq!(store.load_record().await.unwrap().unwrap(), admitted);
    }

    #[tokio::test]
    async fn recovery_release_authorization_rejects_uncommitted_and_stale_terminals() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap()
        else {
            panic!("empty authority must be acquired");
        };
        let publisher = owner_recovery_fault_publisher(&incumbent);
        assert_eq!(
            store.record_recovery_fault(publisher, 1).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let first = recovery_release_terminal(&store, &lease, 1, 0).await;
        let first_reference = store.stage_recovery_release_terminal(&first).await.unwrap();

        assert!(!store
            .authorize_recovery_release(publisher, &first)
            .await
            .unwrap());
        assert_eq!(
            store
                .record_recovery_release_commit(&lease.proof(), first_reference.clone())
                .await
                .unwrap(),
            RecordRecoveryReleaseCommitResult::Created(first_reference)
        );
        assert!(store
            .authorize_recovery_release(publisher, &first)
            .await
            .unwrap());

        assert_eq!(
            store.record_recovery_fault(publisher, 2).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let second = recovery_release_terminal(&store, &lease, 2, 0).await;
        commit_recovery_release(&store, &lease, &second).await;

        assert!(!store
            .authorize_recovery_release(publisher, &first)
            .await
            .unwrap());
        assert!(store
            .authorize_recovery_release(publisher, &second)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn fault_report_before_recovery_release_cas_returns_faults_changed() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap()
        else {
            panic!("empty authority must be acquired");
        };
        let publisher = owner_recovery_fault_publisher(&incumbent);
        assert_eq!(
            store.record_recovery_fault(publisher, 1).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let terminal = recovery_release_terminal(&store, &lease, 1, 0).await;
        let reference = store
            .stage_recovery_release_terminal(&terminal)
            .await
            .unwrap();

        assert_eq!(
            store.record_recovery_fault(publisher, 2).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        assert_eq!(
            store
                .record_recovery_release_commit(&lease.proof(), reference)
                .await
                .unwrap(),
            RecordRecoveryReleaseCommitResult::FaultsChanged
        );
        assert_eq!(
            store.recovery_fault_inventory().await.unwrap().faults(),
            &[RecoveryFault {
                reporter: incumbent.node,
                sequence: 3,
            }]
        );
        assert_eq!(
            store.latest_recovery_release_terminal().await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn recovery_release_cas_before_fault_report_preserves_both_facts() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap()
        else {
            panic!("empty authority must be acquired");
        };
        let publisher = owner_recovery_fault_publisher(&incumbent);
        assert_eq!(
            store.record_recovery_fault(publisher, 1).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let terminal = recovery_release_terminal(&store, &lease, 1, 0).await;
        let reference = commit_recovery_release(&store, &lease, &terminal).await;

        assert_eq!(
            store.record_recovery_fault(publisher, 2).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        assert_eq!(
            store.latest_recovery_release_terminal().await.unwrap(),
            Some(terminal.clone())
        );
        assert_eq!(
            store.recovery_fault_inventory().await.unwrap().faults(),
            &[RecoveryFault {
                reporter: incumbent.node,
                sequence: 4,
            }]
        );
        assert!(!store
            .authorize_recovery_release(publisher, &terminal)
            .await
            .unwrap());
        assert_eq!(
            store
                .record_recovery_release_commit(&lease.proof(), reference.clone())
                .await
                .unwrap(),
            RecordRecoveryReleaseCommitResult::Unchanged(reference)
        );
    }

    #[tokio::test]
    async fn recovery_admission_snapshot_retries_when_faults_change_during_terminal_read() {
        let inner = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = inner.clone();
        let setup = LeaderLeaseStore::new(Arc::clone(&object_store), 1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(lease) = setup.begin_new_term(&incumbent, 0).await.unwrap()
        else {
            panic!("empty authority must be acquired");
        };
        let initial = setup.recovery_admission_snapshot().await.unwrap();
        assert_eq!(initial.committed_release(), None);
        assert!(initial.fault_inventory().faults().is_empty());
        assert!(setup
            .recovery_admission_is_current(&initial, &lease.proof())
            .await
            .unwrap());
        let publisher = owner_recovery_fault_publisher(&incumbent);
        let terminal = recovery_release_terminal_after_owner_fault(&setup, &lease, 7, 4).await;
        let reference = commit_recovery_release(&setup, &lease, &terminal).await;
        let settled = setup.recovery_admission_snapshot().await.unwrap();
        assert_eq!(settled.committed_release(), Some(&terminal));
        assert!(settled.fault_inventory().faults().is_empty());
        assert!(setup
            .recovery_admission_is_current(&settled, &lease.proof())
            .await
            .unwrap());

        let current = setup.load_record().await.unwrap().unwrap();
        let sequence = current.lease.seq + 1;
        let mut changed = current.preserve_with_lease(LeaderLease {
            seq: sequence,
            renewal_sequence: current.lease.renewal_sequence,
            token: current.lease.token,
            owner: current.lease.owner.clone(),
            expires_at_ms: current.lease.expires_at_ms,
            catalog_manifest: current.lease.catalog_manifest.clone(),
        });
        let slot = AuthorityRecoveryFaultSlot {
            publisher,
            request_sequence: 8,
            fault_sequence: sequence,
            active: true,
        };
        match changed
            .recovery_fault_slots
            .binary_search_by_key(&publisher.participant.node_id, |slot| {
                slot.publisher.participant.node_id
            }) {
            Ok(index) => changed.recovery_fault_slots[index] = slot,
            Err(index) => changed.recovery_fault_slots.insert(index, slot),
        }
        changed.recovery_fault_revision = sequence;
        changed.validate().unwrap();
        let changed_path = lease_path(changed.lease.seq);
        let changed_body = encode_authority_record(&changed).unwrap();

        let terminal_path = recovery_release_terminal_path(&reference);
        let (raw, store) = replacing_once_on_get(
            1_000,
            object_store,
            terminal_path.clone(),
            changed_path,
            changed_body,
            false,
        );
        let current = store.recovery_admission_snapshot().await.unwrap();

        assert!(raw.did_replace.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(raw.get_count(&terminal_path), 2);
        assert_eq!(current.committed_release(), Some(&terminal));
        assert_eq!(current.authority_sequence, changed.lease.seq);
        assert_eq!(current.fault_inventory().revision(), changed.lease.seq);
        assert_eq!(
            current.fault_inventory().faults(),
            &[RecoveryFault {
                reporter: incumbent.node,
                sequence: changed.lease.seq,
            }]
        );

        raw.clear_get_counts();
        assert!(!store
            .recovery_admission_is_current(&settled, &lease.proof())
            .await
            .unwrap());
        assert!(!store
            .recovery_admission_is_current(&current, &lease.proof())
            .await
            .unwrap());
        assert_eq!(raw.get_count(&terminal_path), 0);
    }

    #[tokio::test]
    async fn recovery_admission_revalidation_rejects_leader_takeover() {
        let store = store(1);
        let incumbent = owner(1, 1, 1);
        let rival = owner(2, 2, 1);
        let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap()
        else {
            panic!("empty authority must be acquired");
        };
        let terminal = recovery_release_terminal_after_owner_fault(&store, &lease, 7, 4).await;
        commit_recovery_release(&store, &lease, &terminal).await;
        let snapshot = store.recovery_admission_snapshot().await.unwrap();
        assert!(store
            .recovery_admission_is_current(&snapshot, &lease.proof())
            .await
            .unwrap());

        let observed = store.load().await.unwrap().unwrap();
        let observation = LeaderLeaseObservation {
            lease: observed,
            started: Instant::now()
                .checked_sub(Duration::from_millis(2))
                .unwrap(),
        };
        let LeaseOutcome::Acquired(takeover) =
            store.try_takeover(&rival, &observation, 2).await.unwrap()
        else {
            panic!("expired incumbent must be replaced");
        };

        let after = store.recovery_admission_snapshot().await.unwrap();
        assert_eq!(after.committed_release(), snapshot.committed_release());
        assert_eq!(after.fault_inventory(), snapshot.fault_inventory());
        assert!(!store
            .recovery_admission_is_current(&snapshot, &lease.proof())
            .await
            .unwrap());
        assert!(store
            .recovery_admission_is_current(&snapshot, &takeover.proof())
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn recovery_release_compacts_tombstones_without_reusing_fault_sequences() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(lease) = store.begin_new_term(&incumbent, 0).await.unwrap()
        else {
            panic!("empty authority must be acquired");
        };
        let first = recovery_fault_publisher(2, 2, 1);
        assert_eq!(
            store.record_recovery_fault(first, 1).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let first_sequence = store.recovery_fault_inventory().await.unwrap().faults()[0].sequence;
        let terminal = recovery_release_terminal(&store, &lease, 1, 0).await;
        commit_recovery_release(&store, &lease, &terminal).await;

        for offset in 0..=MAX_RECOVERY_FAULT_SLOTS {
            let ordinal = u64::try_from(offset).unwrap();
            let publisher =
                recovery_fault_publisher(10_000 + ordinal, 10_000 + u128::from(ordinal), 1);
            assert_eq!(
                store.record_recovery_fault(publisher, 1).await.unwrap(),
                RecordRecoveryFaultResult::Active
            );
            let terminal = recovery_release_terminal(&store, &lease, 2 + ordinal, 0).await;
            commit_recovery_release(&store, &lease, &terminal).await;
            let head = store.load_record().await.unwrap().unwrap();
            assert!(head.recovery_fault_slots.is_empty());
            assert_eq!(head.recovery_fault_revision, head.lease.seq);
        }

        assert_eq!(
            store.record_recovery_fault(first, 1).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let retried = store.recovery_fault_inventory().await.unwrap();
        assert_eq!(retried.faults().len(), 1);
        assert!(retried.faults()[0].sequence > first_sequence);
    }

    #[tokio::test]
    async fn full_unavailable_fault_inventory_is_compacted_before_new_admission() {
        let incumbent = owner(1, 1, 1);
        let sequence = u64::try_from(MAX_RECOVERY_FAULT_SLOTS).unwrap() + 1;
        let lease = LeaderLease {
            seq: sequence,
            renewal_sequence: 1,
            token: 1,
            owner: incumbent.clone(),
            expires_at_ms: i64::MAX,
            catalog_manifest: None,
        };
        let mut record = LeaderAuthorityRecord::initial(lease.clone());
        record.recovery_fault_revision = sequence;
        record.recovery_fault_slots = (0..MAX_RECOVERY_FAULT_SLOTS)
            .map(|index| {
                let ordinal = u64::try_from(index).unwrap() + 2;
                AuthorityRecoveryFaultSlot {
                    publisher: recovery_fault_publisher(ordinal, u128::from(ordinal), 1),
                    request_sequence: 1,
                    fault_sequence: ordinal,
                    active: true,
                }
            })
            .collect();
        let store = store(1_000);
        store
            .store
            .put(
                &lease_path(sequence),
                PutPayload::from(encode_authority_record(&record).unwrap()),
            )
            .await
            .unwrap();
        let inventory = store.recovery_fault_inventory().await.unwrap();
        let terminal = RecoveryAnnouncement {
            round: RecoveryRound::new(
                1,
                lease.proof(),
                assignment_fence(&incumbent),
                Vec::new(),
                inventory.revision(),
                inventory.faults().to_vec(),
            )
            .unwrap(),
            phase: RecoverPhase::ReleaseCommitted { epoch: 0 },
        };

        commit_recovery_release(&store, &lease, &terminal).await;
        let settled = store.load_record().await.unwrap().unwrap();
        assert!(settled.recovery_fault_slots.is_empty());
        assert_eq!(settled.recovery_fault_revision, settled.lease.seq);
        assert!(settled.validate().is_ok());

        let unseen = recovery_fault_publisher(sequence + 1, u128::from(sequence + 1), 1);
        assert_eq!(
            store.record_recovery_fault(unseen, 1).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let admitted = store.recovery_fault_inventory().await.unwrap();
        assert_eq!(admitted.faults().len(), 1);
        assert!(admitted.faults()[0].sequence > sequence);
    }

    #[test]
    fn recovery_fault_slot_capacity_preserves_authority_headroom() {
        fn authority_with_fault_slots(
            slot_count: usize,
            wide_values: bool,
        ) -> LeaderAuthorityRecord {
            let sequence = if wide_values {
                u64::MAX
            } else {
                u64::try_from(slot_count.max(1)).unwrap()
            };
            let mut record = LeaderAuthorityRecord::initial(LeaderLease {
                seq: sequence,
                renewal_sequence: 1,
                token: 1,
                owner: owner(1, 1, 1),
                expires_at_ms: i64::MAX,
                catalog_manifest: None,
            });
            record.recovery_fault_revision = sequence;
            record.recovery_fault_slots = (0..slot_count)
                .map(|index| {
                    let ordinal = u64::try_from(index).unwrap() + 1;
                    let node_id = if wide_values {
                        u64::MAX - u64::try_from(slot_count - 1 - index).unwrap()
                    } else {
                        ordinal
                    };
                    AuthorityRecoveryFaultSlot {
                        publisher: RecoveryFaultPublisher {
                            participant: crate::checkpoint::CheckpointParticipant {
                                node_id,
                                boot_incarnation: if wide_values {
                                    Uuid::from_u128(
                                        u128::MAX - u128::try_from(slot_count - 1 - index).unwrap(),
                                    )
                                } else {
                                    Uuid::from_u128(u128::from(ordinal))
                                },
                            },
                            process_term: sequence,
                        },
                        request_sequence: sequence,
                        fault_sequence: if wide_values {
                            sequence - u64::try_from(slot_count - 1 - index).unwrap()
                        } else {
                            ordinal
                        },
                        active: true,
                    }
                })
                .collect();
            record
        }

        let canonical_roster = authority_with_fault_slots(MAX_CHECKPOINT_PARTICIPANTS, true);
        let slot_bound = authority_with_fault_slots(MAX_RECOVERY_FAULT_SLOTS, true);
        for record in [&canonical_roster, &slot_bound] {
            record.validate().unwrap();
            let encoded = encode_authority_record(record).unwrap();
            let encoded_len = u64::try_from(encoded.len()).unwrap();
            assert!(
                encoded_len + RECOVERY_FAULT_AUTHORITY_HEADROOM_BYTES <= MAX_AUTHORITY_RECORD_BYTES,
                "{} fault slots encoded to {encoded_len} bytes",
                record.recovery_fault_slots.len()
            );
        }

        let overflow = authority_with_fault_slots(MAX_RECOVERY_FAULT_SLOTS + 1, true);
        assert!(overflow.validate().is_err());

        let mut future_fault = authority_with_fault_slots(1, false);
        future_fault.lease.seq = 2;
        future_fault.recovery_fault_slots[0].fault_sequence = 2;
        assert!(future_fault.validate().is_err());

        let mut orphaned_revision = LeaderAuthorityRecord::initial(LeaderLease {
            seq: 2,
            renewal_sequence: 1,
            token: 1,
            owner: owner(1, 1, 1),
            expires_at_ms: 1,
            catalog_manifest: None,
        });
        orphaned_revision.recovery_fault_revision = 2;
        assert!(orphaned_revision.validate().is_err());
    }

    #[tokio::test]
    async fn exact_owner_renews_without_advancing_token() {
        let store = store(1_000);
        let owner = owner(1, 1, 4);
        let LeaseOutcome::Acquired(first) = store.begin_new_term(&owner, 10).await.unwrap() else {
            panic!("empty authority must be acquired");
        };
        let LeaseOutcome::Acquired(second) =
            store.renew_exact(&owner, first.token, 500).await.unwrap()
        else {
            panic!("exact owner must renew");
        };
        assert_eq!((first.seq, first.renewal_sequence, first.token), (1, 1, 1));
        assert_eq!(
            (second.seq, second.renewal_sequence, second.token),
            (2, 2, 1)
        );
    }

    #[tokio::test]
    async fn acquisition_new_term_and_takeover_advance_the_renewal_sequence() {
        let store = store(10);
        let incumbent = owner(1, 1, 4);
        let rival = owner(2, 2, 1);
        let LeaseOutcome::Acquired(first) = store.begin_new_term(&incumbent, 10).await.unwrap()
        else {
            panic!("empty authority must be acquired");
        };
        let LeaseOutcome::Acquired(renewed) = store
            .renew_exact(&incumbent, first.token, 20)
            .await
            .unwrap()
        else {
            panic!("exact owner must renew");
        };
        let LeaseOutcome::Acquired(new_term) = store.begin_new_term(&incumbent, 30).await.unwrap()
        else {
            panic!("same owner must begin a new authority term");
        };
        let observation = store.observe_rival(&rival, &new_term).unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        let LeaseOutcome::Acquired(takeover) =
            store.try_takeover(&rival, &observation, 40).await.unwrap()
        else {
            panic!("expired rival must be replaced");
        };

        assert_eq!((first.seq, first.renewal_sequence, first.token), (1, 1, 1));
        assert_eq!(
            (renewed.seq, renewed.renewal_sequence, renewed.token),
            (2, 2, 1)
        );
        assert_eq!(
            (new_term.seq, new_term.renewal_sequence, new_term.token),
            (3, 3, 2)
        );
        assert_eq!(
            (
                takeover.seq,
                takeover.renewal_sequence,
                takeover.token,
                takeover.owner,
            ),
            (4, 4, 3, rival)
        );
    }

    #[test]
    fn lease_validation_rejects_an_invalid_renewal_sequence() {
        let mut lease = LeaderLease {
            seq: 2,
            renewal_sequence: 0,
            token: 1,
            owner: owner(1, 1, 1),
            expires_at_ms: 1,
            catalog_manifest: None,
        };
        assert!(lease.validate().is_err());
        lease.renewal_sequence = 3;
        assert!(lease.validate().is_err());
        lease.renewal_sequence = 2;
        assert!(lease.validate().is_ok());
    }

    #[tokio::test]
    async fn exact_renewal_rejects_a_missing_or_newer_authority_term() {
        let empty = store(1_000);
        let owner = owner(1, 1, 4);
        assert!(empty.renew_exact(&owner, 1, 10).await.is_err());

        let store = store(1_000);
        let LeaseOutcome::Acquired(first) = store.begin_new_term(&owner, 10).await.unwrap() else {
            panic!("empty authority must be acquired");
        };
        let LeaseOutcome::Acquired(new_term) = store.begin_new_term(&owner, 20).await.unwrap()
        else {
            panic!("same-owner reacquisition must rotate its term");
        };
        let error = store
            .renew_exact(&owner, first.token, 30)
            .await
            .unwrap_err();
        assert!(matches!(error, LeaseError::Fenced(_)));
        assert!(new_term.token > first.token);
    }

    #[tokio::test]
    async fn fast_rival_clock_cannot_steal() {
        let store = store(30);
        let incumbent = owner(1, 1, 1);
        let rival = owner(2, 2, 1);
        store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap();
        let LeaseOutcome::Held(current) = store
            .acquire_or_renew_current_term_for_test(&rival, i64::MAX - 30)
            .await
            .unwrap()
        else {
            panic!("wall time must not authorize a takeover");
        };
        let observation = store.observe_rival(&rival, &current).unwrap();
        let LeaseOutcome::Held(_) = store
            .try_takeover(&rival, &observation, i64::MAX - 30)
            .await
            .unwrap()
        else {
            panic!("a full local observation is mandatory");
        };
    }

    #[tokio::test]
    async fn renewal_invalidates_observation_despite_backward_owner_clock() {
        let store = store(20);
        let incumbent = owner(1, 1, 1);
        let rival = owner(2, 2, 1);
        store
            .acquire_or_renew_current_term_for_test(&incumbent, 10_000)
            .await
            .unwrap();
        let LeaseOutcome::Held(first) = store
            .acquire_or_renew_current_term_for_test(&rival, 0)
            .await
            .unwrap()
        else {
            panic!("rival must observe the incumbent");
        };
        let observation = store.observe_rival(&rival, &first).unwrap();
        store
            .acquire_or_renew_current_term_for_test(&incumbent, -10_000)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        let LeaseOutcome::Held(current) =
            store.try_takeover(&rival, &observation, 0).await.unwrap()
        else {
            panic!("renewal must invalidate the old observation");
        };
        assert_eq!(current.seq, 2);
        assert_eq!(current.renewal_sequence, 2);
        assert_eq!(current.owner, incumbent);
    }

    #[tokio::test]
    async fn recovery_fault_append_does_not_reset_takeover_observation() {
        let store = store(60);
        let incumbent = owner(1, 1, 1);
        let rival = owner(2, 2, 1);
        let LeaseOutcome::Acquired(first) = store.begin_new_term(&incumbent, 0).await.unwrap()
        else {
            panic!("initial leader acquisition");
        };
        let observation = store.observe_rival(&rival, &first).unwrap();

        tokio::time::sleep(Duration::from_millis(35)).await;
        assert_eq!(
            store
                .record_recovery_fault(owner_recovery_fault_publisher(&incumbent), 1)
                .await
                .unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let appended = store.load().await.unwrap().unwrap();
        assert_eq!(appended.seq, 2);
        assert_eq!(appended.renewal_sequence, first.renewal_sequence);
        tokio::time::sleep(Duration::from_millis(30)).await;

        let LeaseOutcome::Acquired(takeover) =
            store.try_takeover(&rival, &observation, 1).await.unwrap()
        else {
            panic!("recovery fault append reset the takeover observation");
        };
        assert_eq!(takeover.seq, 3);
        assert_eq!(takeover.renewal_sequence, 2);
        assert_eq!(takeover.owner, rival);
    }

    #[tokio::test]
    async fn repeated_recovery_fault_contention_cannot_starve_an_expired_takeover() {
        let (raw, store) = blocking_once_at(10, lease_path(2));
        let incumbent = owner(1, 1, 1);
        let rival = owner(2, 2, 1);
        let LeaseOutcome::Acquired(first) = store.begin_new_term(&incumbent, 0).await.unwrap()
        else {
            panic!("initial leader acquisition");
        };
        let publisher = owner_recovery_fault_publisher(&incumbent);
        let observation = store.observe_rival(&rival, &first).unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;

        let takeover_store = Arc::clone(&store);
        let takeover_owner = rival.clone();
        let takeover = tokio::spawn(async move {
            takeover_store
                .try_takeover(&takeover_owner, &observation, 1)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();

        assert_eq!(
            store.record_recovery_fault(publisher, 1).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        assert_eq!(
            store.record_recovery_fault(publisher, 2).await.unwrap(),
            RecordRecoveryFaultResult::Active
        );
        let contending_head = store.load().await.unwrap().unwrap();
        assert_eq!(contending_head.seq, 3);
        assert_eq!(contending_head.renewal_sequence, 1);
        raw.release.add_permits(1);

        let LeaseOutcome::Acquired(replacement) =
            tokio::time::timeout(Duration::from_secs(1), takeover)
                .await
                .expect("takeover was starved by recovery fault appends")
                .unwrap()
                .unwrap()
        else {
            panic!("unchanged liveness identity did not retry after CAS contention");
        };
        assert_eq!(replacement.seq, 4);
        assert_eq!(replacement.renewal_sequence, 2);
        assert_eq!(replacement.owner, rival);
    }

    #[tokio::test]
    async fn unchanged_liveness_observation_is_required_for_takeover() {
        let store = store(15);
        let incumbent = owner(1, 1, 1);
        let rival = owner(2, 2, 1);
        store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap();
        let LeaseOutcome::Held(current) = store
            .acquire_or_renew_current_term_for_test(&rival, 0)
            .await
            .unwrap()
        else {
            panic!("rival must be held");
        };
        let observation = store.observe_rival(&rival, &current).unwrap();
        assert!(matches!(
            store.try_takeover(&rival, &observation, 0).await.unwrap(),
            LeaseOutcome::Held(_)
        ));
        tokio::time::sleep(Duration::from_millis(20)).await;
        let LeaseOutcome::Acquired(lease) =
            store.try_takeover(&rival, &observation, 0).await.unwrap()
        else {
            panic!("unchanged rival may be replaced after a full TTL");
        };
        assert_eq!(
            (lease.seq, lease.renewal_sequence, lease.token, lease.owner,),
            (2, 2, 2, rival)
        );
    }

    #[tokio::test]
    async fn same_node_new_boot_is_a_rival_and_advances_token() {
        let store = store(10);
        let old = owner(7, 1, 3);
        let replacement = owner(7, 2, 4);
        store
            .acquire_or_renew_current_term_for_test(&old, 0)
            .await
            .unwrap();
        let LeaseOutcome::Held(current) = store
            .acquire_or_renew_current_term_for_test(&replacement, 0)
            .await
            .unwrap()
        else {
            panic!("new boot cannot renew an old boot's token");
        };
        let observation = store.observe_rival(&replacement, &current).unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        let LeaseOutcome::Acquired(lease) = store
            .try_takeover(&replacement, &observation, 0)
            .await
            .unwrap()
        else {
            panic!("replacement must acquire");
        };
        assert_eq!(lease.token, 2);
        assert_eq!(lease.owner, replacement);
    }

    #[tokio::test]
    async fn two_racers_have_one_winner() {
        let (raw, store) = blocking_store_at(1_000, lease_path(1));
        let left_owner = owner(1, 1, 1);
        let right_owner = owner(2, 2, 1);
        let left_store = Arc::clone(&store);
        let left = tokio::spawn(async move {
            left_store
                .acquire_or_renew_current_term_for_test(&left_owner, 0)
                .await
        });
        let right_store = Arc::clone(&store);
        let right = tokio::spawn(async move {
            right_store
                .acquire_or_renew_current_term_for_test(&right_owner, 0)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire_many(2))
            .await
            .unwrap()
            .unwrap()
            .forget();
        raw.release.add_permits(2);
        let (left, right) = tokio::join!(left, right);
        let left = left.unwrap().unwrap();
        let right = right.unwrap().unwrap();
        assert_eq!(
            usize::from(matches!(left, LeaseOutcome::Acquired(_)))
                + usize::from(matches!(right, LeaseOutcome::Acquired(_))),
            1
        );
        let durable = store.load().await.unwrap().unwrap();
        assert!(matches!(
            (&left, &right),
            (LeaseOutcome::Acquired(winner), LeaseOutcome::Held(held))
                | (LeaseOutcome::Held(held), LeaseOutcome::Acquired(winner))
                if winner == &durable && held == &durable
        ));
    }

    #[tokio::test]
    async fn exact_renewal_is_fenced_when_a_rival_wins_its_cas_sequence() {
        let (raw, store) = blocking_once_at(10, lease_path(2));
        let incumbent = owner(1, 1, 1);
        let rival = owner(2, 2, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            panic!("initial leader acquisition");
        };
        let observation = store.observe_rival(&rival, &first).unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        let renewing_store = Arc::clone(&store);
        let renewing_owner = incumbent.clone();
        let renewal = tokio::spawn(async move {
            renewing_store
                .renew_exact(&renewing_owner, first.token, 1)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();

        let LeaseOutcome::Acquired(replacement) =
            store.try_takeover(&rival, &observation, 2).await.unwrap()
        else {
            panic!("observed rival must win the blocked renewal sequence");
        };
        raw.release.add_permits(1);
        let error = renewal.await.unwrap().unwrap_err();

        assert!(matches!(error, LeaseError::Fenced(_)));
        assert_eq!(store.load().await.unwrap(), Some(replacement));
    }

    #[tokio::test]
    async fn shared_local_filesystem_rejects_authority_head_cas() {
        let temp = tempfile::tempdir().unwrap();
        let filesystem: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(temp.path()).unwrap());
        let store = LeaderLeaseStore::new(filesystem, 1_000);
        let owner = owner(1, 1, 1);
        assert!(matches!(
            store
                .acquire_or_renew_current_term_for_test(&owner, 0)
                .await
                .unwrap(),
            LeaseOutcome::Acquired(LeaderLease { seq: 1, .. })
        ));
        let error = store
            .acquire_or_renew_current_term_for_test(&owner, 1)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("PutMode::Update"), "{error}");
        assert_eq!(
            read_authority_head_pointer(store.store.as_ref())
                .await
                .unwrap()
                .unwrap()
                .pointer
                .sequence,
            1
        );
        assert!(read_authority_record(store.store.as_ref(), 2)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn renewal_history_pruning_has_a_reader_grace_period() {
        let store = store(1);
        let owner = owner(1, 1, 1);
        for now in 0..8 {
            assert!(matches!(
                store
                    .acquire_or_renew_current_term_for_test(&owner, now)
                    .await
                    .unwrap(),
                LeaseOutcome::Acquired(_)
            ));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(matches!(
            store
                .acquire_or_renew_current_term_for_test(&owner, 9)
                .await
                .unwrap(),
            LeaseOutcome::Acquired(_)
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if store.list_seqs().await.unwrap() == vec![8, 9] {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(store.load().await.unwrap().unwrap().seq, 9);
    }

    #[tokio::test]
    async fn prune_never_deletes_records_newer_than_its_head_snapshot() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for sequence in 1..=3 {
            object_store
                .put(
                    &lease_path(sequence),
                    PutPayload::from(Bytes::from_static(b"x")),
                )
                .await
                .unwrap();
        }
        let retained = BTreeSet::from([2, 3]);
        let snapshot_head_sequence = *retained.last().unwrap();

        object_store
            .put(
                &lease_path(snapshot_head_sequence + 1),
                PutPayload::from(Bytes::from_static(b"x")),
            )
            .await
            .unwrap();

        let (candidates, exhausted) =
            LeaderLeaseStore::prune_candidates(&object_store, &retained, snapshot_head_sequence, 0)
                .await
                .unwrap();
        assert!(exhausted);
        assert_eq!(candidates, vec![lease_path(1)]);
    }

    struct BlockingStore {
        inner: Arc<dyn ObjectStore>,
        blocked_path: OsPath,
        block_put: bool,
        block_once: bool,
        block_after_put: bool,
        block_get_once: bool,
        did_block: std::sync::atomic::AtomicBool,
        ambiguous_path: Option<OsPath>,
        did_return_ambiguous: std::sync::atomic::AtomicBool,
        replacement_on_get: Option<(OsPath, Bytes, bool)>,
        did_replace: std::sync::atomic::AtomicBool,
        entered: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
        get_counts: Arc<std::sync::Mutex<std::collections::BTreeMap<String, u64>>>,
        put_counts: Arc<std::sync::Mutex<std::collections::BTreeMap<(String, &'static str), u64>>>,
        list_count: std::sync::atomic::AtomicU64,
        fail_delete_once: Arc<std::sync::Mutex<Option<OsPath>>>,
        track_capsule_get_concurrency: std::sync::atomic::AtomicBool,
        active_capsule_gets: std::sync::atomic::AtomicUsize,
        max_capsule_gets: std::sync::atomic::AtomicUsize,
    }

    impl BlockingStore {
        fn clear_get_counts(&self) {
            self.get_counts.lock().unwrap().clear();
        }

        fn get_count(&self, location: &OsPath) -> u64 {
            self.get_counts
                .lock()
                .unwrap()
                .get(location.as_ref())
                .copied()
                .unwrap_or(0)
        }

        fn get_count_prefix(&self, prefix: &str) -> u64 {
            self.get_counts
                .lock()
                .unwrap()
                .iter()
                .filter(|(location, _)| location.starts_with(prefix))
                .map(|(_, count)| *count)
                .sum()
        }

        fn put_count(&self, location: &OsPath, mode: &'static str) -> u64 {
            self.put_counts
                .lock()
                .unwrap()
                .get(&(location.to_string(), mode))
                .copied()
                .unwrap_or(0)
        }

        fn list_count(&self) -> u64 {
            self.list_count.load(std::sync::atomic::Ordering::Acquire)
        }

        fn clear_authority_io_counts(&self) {
            self.clear_get_counts();
            self.put_counts.lock().unwrap().clear();
            self.list_count
                .store(0, std::sync::atomic::Ordering::Release);
        }

        fn fail_next_delete(&self, location: OsPath) {
            *self.fail_delete_once.lock().unwrap() = Some(location);
        }

        fn begin_capsule_get_concurrency_probe(&self) {
            self.active_capsule_gets
                .store(0, std::sync::atomic::Ordering::Release);
            self.max_capsule_gets
                .store(0, std::sync::atomic::Ordering::Release);
            self.track_capsule_get_concurrency
                .store(true, std::sync::atomic::Ordering::Release);
        }

        fn finish_capsule_get_concurrency_probe(&self) -> usize {
            self.track_capsule_get_concurrency
                .store(false, std::sync::atomic::Ordering::Release);
            self.max_capsule_gets
                .load(std::sync::atomic::Ordering::Acquire)
        }
    }

    impl std::fmt::Debug for BlockingStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("BlockingStore")
                .finish_non_exhaustive()
        }
    }

    impl std::fmt::Display for BlockingStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("BlockingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for BlockingStore {
        async fn put_opts(
            &self,
            location: &OsPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let mode = match &options.mode {
                PutMode::Overwrite => "overwrite",
                PutMode::Create => "create",
                PutMode::Update(_) => "update",
            };
            {
                let mut put_counts = self.put_counts.lock().unwrap();
                *put_counts.entry((location.to_string(), mode)).or_default() += 1;
            }
            let should_block = self.block_put
                && location == &self.blocked_path
                && (!self.block_once
                    || !self
                        .did_block
                        .swap(true, std::sync::atomic::Ordering::AcqRel));
            if should_block && !self.block_after_put {
                self.entered.add_permits(1);
                let permit =
                    self.release
                        .acquire()
                        .await
                        .map_err(|error| object_store::Error::Generic {
                            store: "BlockingStore",
                            source: Box::new(error),
                        })?;
                permit.forget();
            }
            let result = self.inner.put_opts(location, payload, options).await;
            if should_block && self.block_after_put {
                self.entered.add_permits(1);
                let permit =
                    self.release
                        .acquire()
                        .await
                        .map_err(|error| object_store::Error::Generic {
                            store: "BlockingStore",
                            source: Box::new(error),
                        })?;
                permit.forget();
            }
            if result.is_ok()
                && self.ambiguous_path.as_ref() == Some(location)
                && !self
                    .did_return_ambiguous
                    .swap(true, std::sync::atomic::Ordering::AcqRel)
            {
                return Err(object_store::Error::Generic {
                    store: "BlockingStore",
                    source: Box::new(std::io::Error::other("injected ambiguous create response")),
                });
            }
            result
        }

        async fn put_multipart_opts(
            &self,
            location: &OsPath,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &OsPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            {
                let mut get_counts = self.get_counts.lock().unwrap();
                *get_counts.entry(location.to_string()).or_default() += 1;
            }
            if self.block_get_once
                && location == &self.blocked_path
                && !self
                    .did_block
                    .swap(true, std::sync::atomic::Ordering::AcqRel)
            {
                self.entered.add_permits(1);
                let permit =
                    self.release
                        .acquire()
                        .await
                        .map_err(|error| object_store::Error::Generic {
                            store: "BlockingStore",
                            source: Box::new(error),
                        })?;
                permit.forget();
            }
            if location == &self.blocked_path
                && !self
                    .did_replace
                    .swap(true, std::sync::atomic::Ordering::AcqRel)
            {
                if let Some((replacement_path, replacement, remove_blocked)) =
                    &self.replacement_on_get
                {
                    self.inner
                        .put_opts(
                            replacement_path,
                            PutPayload::from(replacement.clone()),
                            PutOptions {
                                mode: PutMode::Create,
                                ..PutOptions::default()
                            },
                        )
                        .await?;
                    if *remove_blocked {
                        self.inner.delete(location).await?;
                    }
                }
            }
            let track_concurrency = location
                .as_ref()
                .starts_with("checkpoint-recovery-capsules/")
                && self
                    .track_capsule_get_concurrency
                    .load(std::sync::atomic::Ordering::Acquire);
            if track_concurrency {
                let active = self
                    .active_capsule_gets
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                    + 1;
                self.max_capsule_gets
                    .fetch_max(active, std::sync::atomic::Ordering::AcqRel);
                tokio::task::yield_now().await;
            }
            let result = self.inner.get_opts(location, options).await;
            if track_concurrency {
                self.active_capsule_gets
                    .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            }
            result
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
            let inner = Arc::clone(&self.inner);
            let fail_delete_once = Arc::clone(&self.fail_delete_once);
            FuturesStreamExt::boxed(FuturesStreamExt::then(locations, move |location| {
                let inner = Arc::clone(&inner);
                let fail_delete_once = Arc::clone(&fail_delete_once);
                async move {
                    let location = location?;
                    let inject_failure = {
                        let mut fail = fail_delete_once.lock().unwrap();
                        if fail.as_ref() == Some(&location) {
                            fail.take();
                            true
                        } else {
                            false
                        }
                    };
                    if inject_failure {
                        return Err(object_store::Error::Generic {
                            store: "BlockingStore",
                            source: Box::new(std::io::Error::other(
                                "injected one-shot delete failure",
                            )),
                        });
                    }
                    inner.delete(&location).await?;
                    Ok(location)
                }
            }))
        }

        fn list(
            &self,
            prefix: Option<&OsPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.list_count
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&OsPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &OsPath,
            to: &OsPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn blocking_store_at(
        ttl_ms: i64,
        blocked_path: OsPath,
    ) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
        let raw = Arc::new(BlockingStore {
            inner: Arc::new(InMemory::new()),
            blocked_path,
            block_put: true,
            block_once: false,
            block_after_put: false,
            block_get_once: false,
            did_block: std::sync::atomic::AtomicBool::new(false),
            ambiguous_path: None,
            did_return_ambiguous: std::sync::atomic::AtomicBool::new(false),
            replacement_on_get: None,
            did_replace: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            get_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            put_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            list_count: std::sync::atomic::AtomicU64::new(0),
            fail_delete_once: Arc::new(std::sync::Mutex::new(None)),
            track_capsule_get_concurrency: std::sync::atomic::AtomicBool::new(false),
            active_capsule_gets: std::sync::atomic::AtomicUsize::new(0),
            max_capsule_gets: std::sync::atomic::AtomicUsize::new(0),
        });
        let object_store: Arc<dyn ObjectStore> = raw.clone();
        let authority = Arc::new(LeaderLeaseStore::new(object_store, ttl_ms));
        (raw, authority)
    }

    fn blocking_get_once_at(
        ttl_ms: i64,
        blocked_path: OsPath,
    ) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
        blocking_get_once_with_inner(ttl_ms, Arc::new(InMemory::new()), blocked_path)
    }

    fn blocking_get_once_with_inner(
        ttl_ms: i64,
        inner: Arc<dyn ObjectStore>,
        blocked_path: OsPath,
    ) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
        let raw = Arc::new(BlockingStore {
            inner,
            blocked_path,
            block_put: false,
            block_once: true,
            block_after_put: false,
            block_get_once: true,
            did_block: std::sync::atomic::AtomicBool::new(false),
            ambiguous_path: None,
            did_return_ambiguous: std::sync::atomic::AtomicBool::new(false),
            replacement_on_get: None,
            did_replace: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            get_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            put_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            list_count: std::sync::atomic::AtomicU64::new(0),
            fail_delete_once: Arc::new(std::sync::Mutex::new(None)),
            track_capsule_get_concurrency: std::sync::atomic::AtomicBool::new(false),
            active_capsule_gets: std::sync::atomic::AtomicUsize::new(0),
            max_capsule_gets: std::sync::atomic::AtomicUsize::new(0),
        });
        let object_store: Arc<dyn ObjectStore> = raw.clone();
        let authority = Arc::new(LeaderLeaseStore::new(object_store, ttl_ms));
        (raw, authority)
    }

    fn replacing_once_on_get(
        ttl_ms: i64,
        inner: Arc<dyn ObjectStore>,
        blocked_path: OsPath,
        replacement_path: OsPath,
        replacement: Bytes,
        remove_blocked: bool,
    ) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
        let raw = Arc::new(BlockingStore {
            inner,
            blocked_path,
            block_put: true,
            block_once: true,
            block_after_put: false,
            block_get_once: false,
            did_block: std::sync::atomic::AtomicBool::new(false),
            ambiguous_path: None,
            did_return_ambiguous: std::sync::atomic::AtomicBool::new(false),
            replacement_on_get: Some((replacement_path, replacement, remove_blocked)),
            did_replace: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            get_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            put_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            list_count: std::sync::atomic::AtomicU64::new(0),
            fail_delete_once: Arc::new(std::sync::Mutex::new(None)),
            track_capsule_get_concurrency: std::sync::atomic::AtomicBool::new(false),
            active_capsule_gets: std::sync::atomic::AtomicUsize::new(0),
            max_capsule_gets: std::sync::atomic::AtomicUsize::new(0),
        });
        let object_store: Arc<dyn ObjectStore> = raw.clone();
        let authority = Arc::new(LeaderLeaseStore::new(object_store, ttl_ms));
        (raw, authority)
    }

    fn blocking_once_at(
        ttl_ms: i64,
        blocked_path: OsPath,
    ) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
        let raw = Arc::new(BlockingStore {
            inner: Arc::new(InMemory::new()),
            blocked_path,
            block_put: true,
            block_once: true,
            block_after_put: false,
            block_get_once: false,
            did_block: std::sync::atomic::AtomicBool::new(false),
            ambiguous_path: None,
            did_return_ambiguous: std::sync::atomic::AtomicBool::new(false),
            replacement_on_get: None,
            did_replace: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            get_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            put_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            list_count: std::sync::atomic::AtomicU64::new(0),
            fail_delete_once: Arc::new(std::sync::Mutex::new(None)),
            track_capsule_get_concurrency: std::sync::atomic::AtomicBool::new(false),
            active_capsule_gets: std::sync::atomic::AtomicUsize::new(0),
            max_capsule_gets: std::sync::atomic::AtomicUsize::new(0),
        });
        let object_store: Arc<dyn ObjectStore> = raw.clone();
        let authority = Arc::new(LeaderLeaseStore::new(object_store, ttl_ms));
        (raw, authority)
    }

    #[cfg(feature = "cluster")]
    fn delayed_response_once_at(
        ttl_ms: i64,
        blocked_path: OsPath,
    ) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
        delayed_response_once_at_with_ambiguity(ttl_ms, blocked_path, false)
    }

    fn delayed_ambiguous_response_once_at(
        ttl_ms: i64,
        blocked_path: OsPath,
    ) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
        delayed_response_once_at_with_ambiguity(ttl_ms, blocked_path, true)
    }

    fn delayed_response_once_at_with_ambiguity(
        ttl_ms: i64,
        blocked_path: OsPath,
        ambiguous: bool,
    ) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
        let raw = Arc::new(BlockingStore {
            inner: Arc::new(InMemory::new()),
            ambiguous_path: ambiguous.then(|| blocked_path.clone()),
            blocked_path,
            block_put: true,
            block_once: true,
            block_after_put: true,
            block_get_once: false,
            did_block: std::sync::atomic::AtomicBool::new(false),
            did_return_ambiguous: std::sync::atomic::AtomicBool::new(false),
            replacement_on_get: None,
            did_replace: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            get_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            put_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            list_count: std::sync::atomic::AtomicU64::new(0),
            fail_delete_once: Arc::new(std::sync::Mutex::new(None)),
            track_capsule_get_concurrency: std::sync::atomic::AtomicBool::new(false),
            active_capsule_gets: std::sync::atomic::AtomicUsize::new(0),
            max_capsule_gets: std::sync::atomic::AtomicUsize::new(0),
        });
        let object_store: Arc<dyn ObjectStore> = raw.clone();
        let authority = Arc::new(LeaderLeaseStore::new(object_store, ttl_ms));
        (raw, authority)
    }

    fn ambiguous_once_at(
        ttl_ms: i64,
        ambiguous_path: OsPath,
    ) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
        let raw = Arc::new(BlockingStore {
            inner: Arc::new(InMemory::new()),
            blocked_path: OsPath::from("control/never-block"),
            block_put: true,
            block_once: true,
            block_after_put: false,
            block_get_once: false,
            did_block: std::sync::atomic::AtomicBool::new(false),
            ambiguous_path: Some(ambiguous_path),
            did_return_ambiguous: std::sync::atomic::AtomicBool::new(false),
            replacement_on_get: None,
            did_replace: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            get_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            put_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            list_count: std::sync::atomic::AtomicU64::new(0),
            fail_delete_once: Arc::new(std::sync::Mutex::new(None)),
            track_capsule_get_concurrency: std::sync::atomic::AtomicBool::new(false),
            active_capsule_gets: std::sync::atomic::AtomicUsize::new(0),
            max_capsule_gets: std::sync::atomic::AtomicUsize::new(0),
        });
        let object_store: Arc<dyn ObjectStore> = raw.clone();
        let authority = Arc::new(LeaderLeaseStore::new(object_store, ttl_ms));
        (raw, authority)
    }

    fn bare_authority_record(owner: &LeaderLeaseOwner, sequence: u64) -> LeaderAuthorityRecord {
        LeaderAuthorityRecord::initial(LeaderLease {
            seq: sequence,
            renewal_sequence: sequence,
            token: 1,
            owner: owner.clone(),
            expires_at_ms: 1_000,
            catalog_manifest: None,
        })
    }

    async fn seed_authority_record(raw: &BlockingStore, record: &LeaderAuthorityRecord) {
        raw.inner
            .put_opts(
                &lease_path(record.lease.seq),
                PutPayload::from(encode_authority_record(record).unwrap()),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
            .unwrap();
    }

    async fn seed_authority_head(raw: &BlockingStore, sequence: u64) {
        raw.inner
            .put_opts(
                &authority_head_path(),
                PutPayload::from(encode_authority_head_pointer(sequence).unwrap()),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn missing_head_discovers_once_repairs_successor_and_healthy_reads_never_list() {
        let (empty_raw, empty) = blocking_store_at(
            1_000,
            OsPath::from("control/never-block-empty-authority-head"),
        );
        assert!(empty.load().await.unwrap().is_none());
        assert_eq!(empty_raw.list_count(), 1);
        assert_eq!(empty_raw.put_count(&authority_head_path(), "create"), 0);

        let (raw, store) = delayed_ambiguous_response_once_at(1_000, authority_head_path());
        let incumbent = owner(1, 1, 1);
        let stale = bare_authority_record(&incumbent, 1);
        let retained_previous = bare_authority_record(&incumbent, 6);
        let retained_head = bare_authority_record(&incumbent, 7);
        let orphan_successor = bare_authority_record(&incumbent, 8);
        seed_authority_record(&raw, &stale).await;
        seed_authority_record(&raw, &retained_previous).await;
        seed_authority_record(&raw, &retained_head).await;
        raw.clear_authority_io_counts();

        let recovery = {
            let store = Arc::clone(&store);
            tokio::spawn(async move { store.load().await })
        };
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        seed_authority_record(&raw, &orphan_successor).await;
        raw.release.add_permits(1);

        assert_eq!(
            recovery.await.unwrap().unwrap(),
            Some(orphan_successor.lease.clone())
        );
        assert!(raw
            .did_return_ambiguous
            .load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(raw.list_count(), 1);
        assert_eq!(raw.put_count(&authority_head_path(), "create"), 1);
        assert_eq!(raw.put_count(&authority_head_path(), "update"), 1);
        assert_eq!(
            read_authority_head_pointer(raw.inner.as_ref())
                .await
                .unwrap()
                .unwrap()
                .pointer
                .sequence,
            8
        );

        raw.clear_authority_io_counts();
        assert_eq!(store.load().await.unwrap(), Some(orphan_successor.lease));
        assert!(store.cluster_outcome(1).await.unwrap().is_none());
        assert_eq!(raw.list_count(), 0);
        assert_eq!(raw.put_count(&authority_head_path(), "create"), 0);
        assert_eq!(raw.put_count(&authority_head_path(), "update"), 0);
        assert_eq!(raw.get_count(&authority_head_path()), 2);
        assert_eq!(raw.get_count(&lease_path(8)), 2);
        assert_eq!(raw.get_count(&lease_path(9)), 2);
    }

    #[tokio::test]
    async fn pointer_update_without_a_native_version_fails_before_writing() {
        let (raw, store) = blocking_store_at(
            1_000,
            OsPath::from("control/never-block-versionless-authority-head"),
        );
        let first = bare_authority_record(&owner(1, 1, 1), 1);
        seed_authority_record(&raw, &first).await;
        seed_authority_head(&raw, 1).await;
        let before = read_authority_head_pointer(raw.inner.as_ref())
            .await
            .unwrap()
            .unwrap();
        let versionless = VersionedAuthorityHeadPointer {
            pointer: before.pointer,
            update_version: UpdateVersion {
                e_tag: None,
                version: None,
            },
        };
        raw.clear_authority_io_counts();

        let error = store
            .publish_authority_head(2, Some(&versionless))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("did not provide a native conditional update version"),
            "{error}"
        );
        assert_eq!(raw.put_count(&authority_head_path(), "update"), 0);
        assert_eq!(
            read_authority_head_pointer(raw.inner.as_ref())
                .await
                .unwrap()
                .unwrap()
                .pointer,
            before.pointer
        );
    }

    #[test]
    fn same_sequence_authority_heads_have_unique_nonce_bodies() {
        let first = encode_authority_head_pointer(7).unwrap();
        let second = encode_authority_head_pointer(7).unwrap();
        let first_pointer: AuthorityHeadPointer = serde_json::from_slice(&first).unwrap();
        let second_pointer: AuthorityHeadPointer = serde_json::from_slice(&second).unwrap();

        assert_eq!(first_pointer.sequence, second_pointer.sequence);
        assert_ne!(first_pointer.nonce, second_pointer.nonce);
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn record_before_pointer_crash_is_repaired_without_listing() {
        let (raw, store) = blocking_store_at(
            1_000,
            OsPath::from("control/never-block-record-before-pointer"),
        );
        let incumbent = owner(1, 1, 1);
        let first = bare_authority_record(&incumbent, 1);
        let second = first.preserve_with_lease(LeaderLease {
            seq: 2,
            renewal_sequence: 2,
            token: 1,
            owner: incumbent,
            expires_at_ms: 2_000,
            catalog_manifest: None,
        });
        seed_authority_record(&raw, &first).await;
        seed_authority_head(&raw, 1).await;
        seed_authority_record(&raw, &second).await;
        raw.clear_authority_io_counts();

        assert_eq!(store.load().await.unwrap(), Some(second.lease));
        assert_eq!(raw.list_count(), 0);
        assert_eq!(raw.put_count(&authority_head_path(), "update"), 1);
        assert_eq!(
            read_authority_head_pointer(raw.inner.as_ref())
                .await
                .unwrap()
                .unwrap()
                .pointer
                .sequence,
            2
        );
    }

    #[tokio::test]
    async fn stalled_reader_retries_when_the_pointer_target_was_pruned() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (raw, store) = blocking_get_once_with_inner(1_000, inner, lease_path(1));
        let incumbent = owner(1, 1, 1);
        let first = bare_authority_record(&incumbent, 1);
        let second = bare_authority_record(&incumbent, 2);
        let third = bare_authority_record(&incumbent, 3);
        seed_authority_record(&raw, &first).await;
        seed_authority_head(&raw, 1).await;
        let reader = {
            let store = Arc::clone(&store);
            tokio::spawn(async move { store.load().await })
        };
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();

        seed_authority_record(&raw, &second).await;
        seed_authority_record(&raw, &third).await;
        raw.inner.delete(&authority_head_path()).await.unwrap();
        seed_authority_head(&raw, 3).await;
        raw.inner.delete(&lease_path(1)).await.unwrap();
        raw.release.add_permits(1);

        assert_eq!(reader.await.unwrap().unwrap(), Some(third.lease));
        assert_eq!(raw.list_count(), 0);
    }

    #[tokio::test]
    async fn applied_but_ambiguous_pointer_update_is_reconciled() {
        let (raw, store) = ambiguous_once_at(1_000, authority_head_path());
        let incumbent = owner(1, 1, 1);
        let first = bare_authority_record(&incumbent, 1);
        seed_authority_record(&raw, &first).await;
        seed_authority_head(&raw, 1).await;
        store.prune_running.store(true, Ordering::Release);
        raw.clear_authority_io_counts();

        let LeaseOutcome::Acquired(renewed) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 1)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        assert_eq!(renewed.seq, 2);
        assert!(raw
            .did_return_ambiguous
            .load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(raw.put_count(&authority_head_path(), "update"), 1);
        assert_eq!(raw.list_count(), 0);
        assert_eq!(
            read_authority_head_pointer(raw.inner.as_ref())
                .await
                .unwrap()
                .unwrap()
                .pointer
                .sequence,
            2
        );
    }

    #[tokio::test]
    async fn contenders_and_stale_repair_cannot_regress_the_pointer() {
        let (raw, store) = blocking_store_at(
            1_000,
            OsPath::from("control/never-block-pointer-contenders"),
        );
        let incumbent = owner(1, 1, 1);
        let first = bare_authority_record(&incumbent, 1);
        seed_authority_record(&raw, &first).await;
        seed_authority_head(&raw, 1).await;
        store.prune_running.store(true, Ordering::Release);
        let stale = read_authority_head_pointer(raw.inner.as_ref())
            .await
            .unwrap()
            .unwrap();
        let first_candidate = first.preserve_with_lease(LeaderLease {
            seq: 2,
            renewal_sequence: 2,
            token: 1,
            owner: incumbent.clone(),
            expires_at_ms: 2_000,
            catalog_manifest: None,
        });
        let second_candidate = first.preserve_with_lease(LeaderLease {
            expires_at_ms: 3_000,
            ..first_candidate.lease.clone()
        });
        raw.clear_authority_io_counts();

        let (first_result, second_result) = tokio::join!(
            store.create_authority_record(&first_candidate),
            store.create_authority_record(&second_candidate)
        );
        let first_result = first_result.unwrap();
        let second_result = second_result.unwrap();
        assert_eq!(
            usize::from(matches!(&first_result, AuthorityCreateOutcome::Created))
                + usize::from(matches!(&second_result, AuthorityCreateOutcome::Created)),
            1
        );
        assert_eq!(
            usize::from(matches!(
                &first_result,
                AuthorityCreateOutcome::Contended(_)
            )) + usize::from(matches!(
                &second_result,
                AuthorityCreateOutcome::Contended(_)
            )),
            1
        );

        let winner = store.load_record().await.unwrap().unwrap();
        let third = winner.preserve_with_lease(LeaderLease {
            seq: 3,
            renewal_sequence: 3,
            token: 1,
            owner: incumbent,
            expires_at_ms: 4_000,
            catalog_manifest: None,
        });
        assert!(matches!(
            store.create_authority_record(&third).await.unwrap(),
            AuthorityCreateOutcome::Created
        ));
        let before_stale = read_authority_head_pointer(raw.inner.as_ref())
            .await
            .unwrap()
            .unwrap()
            .pointer;
        assert_eq!(
            store.publish_authority_head(2, Some(&stale)).await.unwrap(),
            3
        );
        let after_stale = read_authority_head_pointer(raw.inner.as_ref())
            .await
            .unwrap()
            .unwrap()
            .pointer;
        assert_eq!(after_stale, before_stale);
        assert_eq!(after_stale.sequence, 3);
        assert_eq!(raw.list_count(), 0);
    }

    #[tokio::test]
    async fn stalled_writer_recreating_a_pruned_sequence_is_contended() {
        let (raw, store) = blocking_store_at(1_000, lease_path(2));
        let incumbent = owner(1, 1, 1);
        let first = bare_authority_record(&incumbent, 1);
        seed_authority_record(&raw, &first).await;
        seed_authority_head(&raw, 1).await;
        store.prune_running.store(true, Ordering::Release);
        let stale_candidate = first.preserve_with_lease(LeaderLease {
            seq: 2,
            renewal_sequence: 2,
            token: 1,
            owner: incumbent.clone(),
            expires_at_ms: 2_000,
            catalog_manifest: None,
        });
        let stale_retry = stale_candidate.clone();
        let stale_store = Arc::clone(&store);
        let stalled =
            tokio::spawn(
                async move { stale_store.create_authority_record(&stale_candidate).await },
            );
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();

        let winner = first.preserve_with_lease(LeaderLease {
            seq: 2,
            renewal_sequence: 2,
            token: 1,
            owner: incumbent.clone(),
            expires_at_ms: 3_000,
            catalog_manifest: None,
        });
        seed_authority_record(&raw, &winner).await;
        let first_pointer = read_authority_head_pointer(raw.inner.as_ref())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .publish_authority_head(2, Some(&first_pointer))
                .await
                .unwrap(),
            2
        );
        let third = winner.preserve_with_lease(LeaderLease {
            seq: 3,
            renewal_sequence: 3,
            token: 1,
            owner: incumbent,
            expires_at_ms: 4_000,
            catalog_manifest: None,
        });
        seed_authority_record(&raw, &third).await;
        let second_pointer = read_authority_head_pointer(raw.inner.as_ref())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .publish_authority_head(3, Some(&second_pointer))
                .await
                .unwrap(),
            3
        );
        raw.inner.delete(&lease_path(2)).await.unwrap();

        raw.release.add_permits(1);
        let result = stalled.await.unwrap().unwrap();
        let AuthorityCreateOutcome::Contended(current) = result else {
            panic!("stale recreated sequence must be classified as contention");
        };
        assert_eq!(current.lease.seq, 3);
        assert_eq!(
            read_authority_head_pointer(raw.inner.as_ref())
                .await
                .unwrap()
                .unwrap()
                .pointer
                .sequence,
            3
        );
        assert!(read_authority_record(raw.inner.as_ref(), 2)
            .await
            .unwrap()
            .is_some());
        let AuthorityCreateOutcome::Contended(current) =
            store.create_authority_record(&stale_retry).await.unwrap()
        else {
            panic!("stale exact residue must remain contended on retry");
        };
        assert_eq!(current.lease.seq, 3);
    }

    #[tokio::test]
    async fn malformed_or_ahead_pointer_fails_closed_without_list_or_put() {
        let (malformed_raw, malformed_store) =
            blocking_store_at(1_000, OsPath::from("control/never-block-malformed-pointer"));
        malformed_raw
            .inner
            .put(
                &authority_head_path(),
                PutPayload::from(Bytes::from_static(b"{\"version\":1,\"sequence\":1}")),
            )
            .await
            .unwrap();
        malformed_raw.clear_authority_io_counts();
        assert!(malformed_store.load().await.is_err());
        assert_eq!(malformed_raw.list_count(), 0);
        assert_eq!(malformed_raw.put_count(&authority_head_path(), "create"), 0);
        assert_eq!(malformed_raw.put_count(&authority_head_path(), "update"), 0);

        let (ahead_raw, ahead_store) =
            blocking_store_at(1_000, OsPath::from("control/never-block-ahead-pointer"));
        let incumbent = owner(1, 1, 1);
        seed_authority_record(&ahead_raw, &bare_authority_record(&incumbent, 1)).await;
        seed_authority_head(&ahead_raw, 2).await;
        ahead_raw.clear_authority_io_counts();
        let error = ahead_store.load().await.unwrap_err();
        assert!(error.to_string().contains("points ahead"), "{error}");
        assert_eq!(ahead_raw.list_count(), 0);
        assert_eq!(ahead_raw.put_count(&authority_head_path(), "create"), 0);
        assert_eq!(ahead_raw.put_count(&authority_head_path(), "update"), 0);
    }

    #[tokio::test]
    async fn pruning_lists_once_and_preserves_pointer_and_recent_records() {
        let (raw, store) =
            blocking_store_at(1_000, OsPath::from("control/never-block-pointer-prune"));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(_) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        disable_history_pruning_for_test(&store).await;
        for now in 1..5 {
            assert!(matches!(
                store
                    .acquire_or_renew_current_term_for_test(&incumbent, now)
                    .await
                    .unwrap(),
                LeaseOutcome::Acquired(_)
            ));
        }
        raw.clear_authority_io_counts();

        LeaderLeaseStore::prune_history(&store.store, 0)
            .await
            .unwrap();
        assert_eq!(raw.list_count(), 1);
        assert!(raw.inner.get(&authority_head_path()).await.is_ok());
        assert!(raw.inner.get(&lease_path(4)).await.is_ok());
        assert!(raw.inner.get(&lease_path(5)).await.is_ok());
        assert_eq!(store.load().await.unwrap().unwrap().seq, 5);
        assert_eq!(raw.list_count(), 1);
    }

    fn catalog(name: &str) -> CatalogManifest {
        CatalogManifest::new(vec![super::super::CatalogManifestEntry {
            canonical_name: name.to_owned(),
            kind: crate::catalog::CatalogObjectKind::Source,
            ddl: format!("CREATE SOURCE {name} (id BIGINT)"),
        }])
        .unwrap()
    }

    #[tokio::test]
    async fn committed_recovery_release_survives_renewal_and_takeover() {
        let store = Arc::new(store(1));
        let incumbent = owner(1, 11, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let terminal = recovery_release_terminal_after_owner_fault(&store, &first, 7, 4).await;
        let reference = store
            .stage_recovery_release_terminal(&terminal)
            .await
            .unwrap();
        assert!(matches!(
            store
                .record_recovery_release_commit(&proof, reference.clone())
                .await
                .unwrap(),
            RecordRecoveryReleaseCommitResult::Created(_)
        ));
        let LeaseOutcome::Acquired(renewed) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 1)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let successor = owner(2, 22, 1);
        let observation = store.observe_rival(&successor, &renewed).unwrap();
        tokio::time::sleep(Duration::from_millis(3)).await;
        let LeaseOutcome::Acquired(_) = store
            .try_takeover(&successor, &observation, 10)
            .await
            .unwrap()
        else {
            panic!("successor must acquire the expired authority");
        };

        assert_eq!(
            store.latest_recovery_release_terminal().await.unwrap(),
            Some(terminal)
        );
        assert!(matches!(
            store
                .record_recovery_release_commit(&proof, reference)
                .await
                .unwrap(),
            RecordRecoveryReleaseCommitResult::Unchanged(_)
        ));
    }

    #[tokio::test]
    async fn takeover_before_recovery_release_append_fences_the_old_commit() {
        let (raw, store) = blocking_once_at(10, lease_path(3));
        let incumbent = owner(1, 11, 1);
        let successor = owner(2, 22, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let terminal = recovery_release_terminal_after_owner_fault(&store, &first, 7, 4).await;
        let reference = store
            .stage_recovery_release_terminal(&terminal)
            .await
            .unwrap();
        let observation = store.observe_rival(&successor, &first).unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;

        let committing = {
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                store
                    .record_recovery_release_commit(&proof, reference)
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        assert!(matches!(
            store
                .try_takeover(&successor, &observation, 20)
                .await
                .unwrap(),
            LeaseOutcome::Acquired(_)
        ));
        raw.release.add_permits(1);
        assert!(matches!(
            committing.await.unwrap(),
            Err(ClusterCheckpointAuthorityError::Fenced)
        ));
        assert_eq!(
            store.latest_recovery_release_terminal().await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn ambiguous_recovery_release_create_reconciles_the_exact_winner() {
        let (raw, store) = ambiguous_once_at(1_000, lease_path(3));
        let incumbent = owner(1, 11, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let terminal = recovery_release_terminal_after_owner_fault(&store, &first, 7, 4).await;
        let reference = store
            .stage_recovery_release_terminal(&terminal)
            .await
            .unwrap();
        assert!(matches!(
            store
                .record_recovery_release_commit(&first.proof(), reference)
                .await
                .unwrap(),
            RecordRecoveryReleaseCommitResult::Created(_)
        ));
        assert!(raw
            .did_return_ambiguous
            .load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(
            store.latest_recovery_release_terminal().await.unwrap(),
            Some(terminal)
        );
    }

    #[tokio::test]
    async fn recovery_release_generation_has_one_exact_winner() {
        let store = store(1_000);
        let incumbent = owner(1, 11, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let first_terminal =
            recovery_release_terminal_after_owner_fault(&store, &first, 7, 4).await;
        let first_reference = store
            .stage_recovery_release_terminal(&first_terminal)
            .await
            .unwrap();
        store
            .record_recovery_release_commit(&proof, first_reference)
            .await
            .unwrap();

        let mut divergent = first_terminal.clone();
        divergent.phase = RecoverPhase::ReleaseCommitted { epoch: 5 };
        let divergent_reference = store
            .stage_recovery_release_terminal(&divergent)
            .await
            .unwrap();
        assert!(matches!(
            store
                .record_recovery_release_commit(&proof, divergent_reference)
                .await
                .unwrap(),
            RecordRecoveryReleaseCommitResult::Conflict { .. }
        ));

        let newer = recovery_release_terminal_after_owner_fault(&store, &first, 8, 5).await;
        let newer_reference = store.stage_recovery_release_terminal(&newer).await.unwrap();
        assert!(matches!(
            store
                .record_recovery_release_commit(&proof, newer_reference)
                .await
                .unwrap(),
            RecordRecoveryReleaseCommitResult::Created(_)
        ));
        assert_eq!(
            store.latest_recovery_release_terminal().await.unwrap(),
            Some(newer)
        );
    }

    #[tokio::test]
    async fn retained_recovery_release_blob_is_revalidated_on_every_read() {
        let raw = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = raw.clone();
        let store = LeaderLeaseStore::new(Arc::clone(&object_store), 1_000);
        let incumbent = owner(1, 11, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let terminal = recovery_release_terminal_after_owner_fault(&store, &first, 7, 4).await;
        let (encoded, expected_reference) = encode_recovery_release_terminal(&terminal).unwrap();
        let reference = store
            .stage_recovery_release_terminal(&terminal)
            .await
            .unwrap();
        assert_eq!(reference, expected_reference);
        store
            .record_recovery_release_commit(&first.proof(), reference.clone())
            .await
            .unwrap();

        let path = recovery_release_terminal_path(&reference);
        raw.delete(&path).await.unwrap();
        let missing = store
            .latest_recovery_release_terminal()
            .await
            .unwrap_err()
            .to_string();
        assert!(missing.contains("is missing"), "{missing}");

        raw.put(&path, PutPayload::from(encoded)).await.unwrap();
        assert_eq!(
            store.latest_recovery_release_terminal().await.unwrap(),
            Some(terminal)
        );
        raw.put(&path, PutPayload::from(Bytes::from_static(b"broken")))
            .await
            .unwrap();
        let corrupt = store
            .latest_recovery_release_terminal()
            .await
            .unwrap_err()
            .to_string();
        assert!(corrupt.contains("bytes, expected"), "{corrupt}");
    }

    #[tokio::test]
    async fn existing_invalid_release_blob_is_a_validation_conflict() {
        let raw = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = raw.clone();
        let store = LeaderLeaseStore::new(object_store, 1_000);
        let incumbent = owner(1, 11, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let terminal = recovery_release_terminal_after_owner_fault(&store, &first, 7, 4).await;
        let (_, reference) = encode_recovery_release_terminal(&terminal).unwrap();
        raw.put(
            &recovery_release_terminal_path(&reference),
            PutPayload::from(Bytes::from_static(b"invalid")),
        )
        .await
        .unwrap();

        assert!(matches!(
            store.stage_recovery_release_terminal(&terminal).await,
            Err(ClusterCheckpointAuthorityError::Authority(
                LeaseError::Invalid(_)
            ))
        ));
    }

    #[tokio::test]
    async fn pruning_retains_latest_release_admission_and_collects_orphan_blobs() {
        let raw = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = raw.clone();
        let store = LeaderLeaseStore::new(Arc::clone(&object_store), 1_000);
        let incumbent = owner(1, 11, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let terminal = recovery_release_terminal_after_owner_fault(&store, &first, 7, 4).await;
        let retained = store
            .stage_recovery_release_terminal(&terminal)
            .await
            .unwrap();
        let mut orphan = terminal.clone();
        orphan.phase = RecoverPhase::ReleaseCommitted { epoch: 5 };
        let orphan = store
            .stage_recovery_release_terminal(&orphan)
            .await
            .unwrap();
        store
            .record_recovery_release_commit(&first.proof(), retained.clone())
            .await
            .unwrap();
        for now in 1..=4 {
            assert!(matches!(
                store
                    .acquire_or_renew_current_term_for_test(&incumbent, now)
                    .await
                    .unwrap(),
                LeaseOutcome::Acquired(_)
            ));
        }

        LeaderLeaseStore::prune_history(&object_store, 0)
            .await
            .unwrap();
        let sequences = store.list_seqs().await.unwrap();
        assert!(sequences.contains(&3), "{sequences:?}");
        assert!(raw
            .get(&recovery_release_terminal_path(&retained))
            .await
            .is_ok());
        assert!(matches!(
            raw.get(&recovery_release_terminal_path(&orphan)).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert_eq!(
            store.latest_recovery_release_terminal().await.unwrap(),
            Some(terminal)
        );
    }

    #[test]
    fn replacement_term_may_abort_but_cannot_commit_an_existing_drain() {
        let incumbent = owner(1, 1, 1);
        let incumbent_lease = LeaderLease {
            seq: 1,
            renewal_sequence: 1,
            token: 1,
            owner: incumbent.clone(),
            expires_at_ms: 1,
            catalog_manifest: None,
        };
        let transition = assignment_drain_transition(&incumbent, incumbent_lease.proof());
        let replacement = LeaderLease {
            seq: 2,
            renewal_sequence: 2,
            token: 2,
            owner: owner(2, 2, 1),
            expires_at_ms: 2,
            catalog_manifest: None,
        }
        .proof();

        assert!(AssignmentDrainDecision::new(
            &transition,
            replacement.clone(),
            AssignmentDrainVerdict::Commit,
        )
        .is_err());
        assert!(AssignmentDrainDecision::new(
            &transition,
            replacement,
            AssignmentDrainVerdict::Abort,
        )
        .is_ok());
    }

    #[tokio::test]
    async fn assignment_recovery_requires_exact_sorted_removals_and_matching_proposal() {
        let store = store(1_000);
        let incumbent = owner(1, 11, 1);
        let failed_two = owner(2, 22, 1);
        let failed_three = owner(3, 33, 1);
        let LeaseOutcome::Acquired(lease) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = lease.proof();
        let decision = assignment_recovery_decision(
            &store,
            1,
            &[incumbent.clone(), failed_two, failed_three],
            std::slice::from_ref(&incumbent),
            proof.clone(),
            1,
        )
        .await;
        assert!(decision.validate().is_ok());

        let mut missing = decision.clone();
        missing.removed_process_fences.pop();
        assert!(missing.validate().is_err());

        let mut unsorted = decision.clone();
        unsorted.removed_process_fences.swap(0, 1);
        assert!(unsorted.validate().is_err());

        let mut wrong_version = decision.clone();
        wrong_version.proposal.version = wrong_version.proposal.version.checked_add(1).unwrap();
        assert!(wrong_version.validate().is_err());

        let mut wrong_predecessor = decision.clone();
        wrong_predecessor.predecessor.assignment_digest[0] ^= 1;
        assert!(wrong_predecessor.validate().is_ok());
        assert!(matches!(
            store
                .record_assignment_recovery_decision(&proof, wrong_predecessor)
                .await,
            Err(ClusterCheckpointAuthorityError::Authority(
                LeaseError::Invalid(_)
            ))
        ));

        let mut wrong_target = decision;
        wrong_target.target.assignment_digest[0] ^= 1;
        assert!(wrong_target.validate().is_ok());
        assert!(matches!(
            store
                .record_assignment_recovery_decision(&proof, wrong_target)
                .await,
            Err(ClusterCheckpointAuthorityError::Authority(
                LeaseError::Invalid(_)
            ))
        ));
        assert_eq!(store.load().await.unwrap().unwrap().seq, 1);
    }

    #[tokio::test]
    async fn competing_assignment_recoveries_have_one_same_version_winner() {
        let (raw, store) = blocking_store_at(1_000, lease_path(2));
        let incumbent = owner(1, 11, 1);
        let failed = owner(2, 22, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let left = assignment_recovery_decision(
            &store,
            1,
            &[incumbent.clone(), failed.clone()],
            std::slice::from_ref(&incumbent),
            proof.clone(),
            1,
        )
        .await;
        let right = assignment_recovery_decision(
            &store,
            1,
            &[incumbent.clone(), failed],
            std::slice::from_ref(&incumbent),
            proof.clone(),
            2,
        )
        .await;
        assert_ne!(left.proposal, right.proposal);

        let left_store = Arc::clone(&store);
        let left_proof = proof.clone();
        let left_task = tokio::spawn(async move {
            left_store
                .record_assignment_recovery_decision(&left_proof, left)
                .await
        });
        let right_store = Arc::clone(&store);
        let right_proof = proof.clone();
        let right_task = tokio::spawn(async move {
            right_store
                .record_assignment_recovery_decision(&right_proof, right)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire_many(2))
            .await
            .unwrap()
            .unwrap()
            .forget();
        raw.release.add_permits(2);

        let (left_result, right_result) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(left_task, right_task)
        })
        .await
        .unwrap();
        let left_result = left_result.unwrap().unwrap();
        let right_result = right_result.unwrap().unwrap();
        assert_eq!(
            usize::from(matches!(
                &left_result,
                RecordAssignmentRecoveryDecisionResult::Created(_)
            )) + usize::from(matches!(
                &right_result,
                RecordAssignmentRecoveryDecisionResult::Created(_)
            )),
            1
        );
        assert_eq!(
            usize::from(matches!(
                &left_result,
                RecordAssignmentRecoveryDecisionResult::Conflict { .. }
            )) + usize::from(matches!(
                &right_result,
                RecordAssignmentRecoveryDecisionResult::Conflict { .. }
            )),
            1
        );
        let durable = store
            .assignment_recovery_decision(2)
            .await
            .unwrap()
            .unwrap();
        let snapshots = AssignmentSnapshotStore::new(Arc::clone(&store.store));
        let _ = store.materialize_assignment_recovery(2).await.unwrap();
        assert_eq!(snapshots.load().await.unwrap().unwrap().version, 2);
        assert_eq!(
            store
                .record_assignment_recovery_decision(&proof, durable.clone())
                .await
                .unwrap(),
            RecordAssignmentRecoveryDecisionResult::Unchanged(durable)
        );
    }

    #[tokio::test]
    async fn authorized_recovery_supersedes_a_delayed_same_version_drain_write() {
        let store = store(1_000);
        let incumbent = owner(1, 11, 1);
        let failed = owner(2, 22, 1);
        let LeaseOutcome::Acquired(lease) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = lease.proof();
        let recovery = assignment_recovery_decision(
            &store,
            1,
            &[incumbent.clone(), failed.clone()],
            std::slice::from_ref(&incumbent),
            proof.clone(),
            1,
        )
        .await;
        assert!(matches!(
            store
                .record_assignment_recovery_decision(&proof, recovery.clone())
                .await
                .unwrap(),
            RecordAssignmentRecoveryDecisionResult::Created(_)
        ));

        let snapshots = AssignmentSnapshotStore::new(Arc::clone(&store.store));
        let predecessor = snapshots.load().await.unwrap().unwrap();
        let delayed_drain = predecessor
            .next_draining(
                BTreeMap::from([(0, failed.node), (1, incumbent.node)]),
                predecessor.participants.clone(),
                proof,
            )
            .unwrap();
        assert!(matches!(
            snapshots
                .save_if_version(&delayed_drain, predecessor.version)
                .await
                .unwrap(),
            RotateOutcome::Rotated
        ));
        assert_eq!(snapshots.load().await.unwrap(), Some(delayed_drain));

        assert!(matches!(
            store.materialize_assignment_recovery(2).await.unwrap(),
            RotateOutcome::Rotated
        ));
        let authorized = snapshots
            .load_recovery_proposal(&recovery.proposal)
            .await
            .unwrap();
        assert_eq!(snapshots.load().await.unwrap(), Some(authorized));
        assert_eq!(snapshots.load_drain_transition(2).await.unwrap(), None);
    }

    #[tokio::test]
    async fn drain_and_recovery_share_one_ordered_retention_chain() {
        let store = store(1_000);
        let incumbent = owner(1, 11, 1);
        let failed = owner(2, 22, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let participants = vec![
            crate::checkpoint::CheckpointParticipant {
                node_id: incumbent.node.0,
                boot_incarnation: incumbent.boot,
            },
            crate::checkpoint::CheckpointParticipant {
                node_id: failed.node.0,
                boot_incarnation: failed.boot,
            },
        ];
        let predecessor = CheckpointAssignmentFence::from_owner_map(
            1,
            &[incumbent.node.0, failed.node.0],
            participants.clone(),
        )
        .unwrap();
        let target = CheckpointAssignmentFence::from_owner_map(
            2,
            &[incumbent.node.0, failed.node.0],
            participants,
        )
        .unwrap();
        let transition =
            AssignmentDrainTransition::new(predecessor, target, proof.clone()).unwrap();
        let drain = AssignmentDrainDecision::new(
            &transition,
            proof.clone(),
            AssignmentDrainVerdict::Commit,
        )
        .unwrap();
        assert!(matches!(
            store
                .record_assignment_drain_decision(&proof, drain.clone())
                .await
                .unwrap(),
            RecordAssignmentDrainDecisionResult::Created(_)
        ));

        let losing_recovery = assignment_recovery_decision(
            &store,
            1,
            &[incumbent.clone(), failed.clone()],
            std::slice::from_ref(&incumbent),
            proof.clone(),
            2,
        )
        .await;
        assert!(matches!(
            store
                .record_assignment_recovery_decision(&proof, losing_recovery)
                .await,
            Err(ClusterCheckpointAuthorityError::Decision(
                DecisionError::Conflict(_)
            ))
        ));

        let recovery = assignment_recovery_decision(
            &store,
            2,
            &[incumbent.clone(), failed],
            std::slice::from_ref(&incumbent),
            proof.clone(),
            3,
        )
        .await;
        assert!(matches!(
            store
                .record_assignment_recovery_decision(&proof, recovery.clone())
                .await
                .unwrap(),
            RecordAssignmentRecoveryDecisionResult::Created(_)
        ));

        let losing_drain_transition = assignment_drain_transition_at(&incumbent, proof.clone(), 3);
        let losing_drain = AssignmentDrainDecision::new(
            &losing_drain_transition,
            proof.clone(),
            AssignmentDrainVerdict::Commit,
        )
        .unwrap();
        assert!(matches!(
            store
                .record_assignment_drain_decision(&proof, losing_drain)
                .await,
            Err(ClusterCheckpointAuthorityError::Decision(
                DecisionError::Conflict(_)
            ))
        ));

        let head = store.load_record().await.unwrap().unwrap();
        assert!(matches!(
            head.assignment_decision,
            Some(AuthorityAssignmentDecision::Recovery(ref durable)) if durable == &recovery
        ));
        let previous = head.previous_assignment_decision.unwrap();
        assert_eq!(previous.target_version, 2);
        let previous_record = read_authority_record(store.store.as_ref(), previous.sequence)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            previous_record.assignment_decision,
            Some(AuthorityAssignmentDecision::Drain(ref durable)) if durable == &drain
        ));
        assert!(matches!(
            store.assignment_drain_decision(3).await,
            Err(ClusterCheckpointAuthorityError::Decision(
                DecisionError::Conflict(_)
            ))
        ));
        assert!(matches!(
            store.assignment_recovery_decision(2).await,
            Err(ClusterCheckpointAuthorityError::Decision(
                DecisionError::Conflict(_)
            ))
        ));

        assert_eq!(
            store
                .prune_assignment_drain_decisions_before(&proof, 3)
                .await
                .unwrap(),
            3
        );
        let floor = store
            .load_record()
            .await
            .unwrap()
            .unwrap()
            .assignment_decision_floor
            .unwrap();
        assert!(matches!(
            floor.terminal_anchor,
            Some(AuthorityAssignmentDecision::Drain(anchor)) if anchor == drain
        ));
        assert_eq!(
            store.assignment_recovery_decision(3).await.unwrap(),
            Some(recovery)
        );
    }

    #[tokio::test]
    async fn takeover_fences_a_delayed_assignment_recovery_decision() {
        let (raw, store) = blocking_once_at(10, lease_path(2));
        let incumbent = owner(1, 11, 1);
        let successor = owner(1, 12, 2);
        let failed = owner(2, 22, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let old_proof = first.proof();
        let old_decision = assignment_recovery_decision(
            &store,
            1,
            &[incumbent.clone(), failed.clone()],
            std::slice::from_ref(&incumbent),
            old_proof.clone(),
            1,
        )
        .await;
        let observation = store.observe_rival(&successor, &first).unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;

        let delayed_store = Arc::clone(&store);
        let delayed_proof = old_proof.clone();
        let delayed_task = tokio::spawn(async move {
            delayed_store
                .record_assignment_recovery_decision(&delayed_proof, old_decision)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let LeaseOutcome::Acquired(takeover) = store
            .try_takeover(&successor, &observation, 20)
            .await
            .unwrap()
        else {
            panic!("successor must win the authority sequence");
        };
        raw.release.add_permits(1);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), delayed_task)
                .await
                .unwrap()
                .unwrap(),
            Err(ClusterCheckpointAuthorityError::Fenced)
        ));

        let takeover_proof = takeover.proof();
        let winner = assignment_recovery_decision(
            &store,
            1,
            &[incumbent, failed],
            std::slice::from_ref(&successor),
            takeover_proof.clone(),
            2,
        )
        .await;
        assert!(matches!(
            store
                .record_assignment_recovery_decision(&takeover_proof, winner.clone())
                .await
                .unwrap(),
            RecordAssignmentRecoveryDecisionResult::Created(_)
        ));
        assert_eq!(
            store.assignment_recovery_decision(2).await.unwrap(),
            Some(winner)
        );
    }

    #[tokio::test]
    async fn competing_assignment_drain_decisions_have_one_immutable_winner() {
        let (raw, store) = blocking_store_at(1_000, lease_path(2));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let transition = assignment_drain_transition(&incumbent, proof.clone());
        let commit = AssignmentDrainDecision::new(
            &transition,
            proof.clone(),
            AssignmentDrainVerdict::Commit,
        )
        .unwrap();
        let abort =
            AssignmentDrainDecision::new(&transition, proof.clone(), AssignmentDrainVerdict::Abort)
                .unwrap();

        let commit_store = Arc::clone(&store);
        let commit_proof = proof.clone();
        let commit_task = tokio::spawn(async move {
            commit_store
                .record_assignment_drain_decision(&commit_proof, commit)
                .await
        });
        let abort_store = Arc::clone(&store);
        let abort_proof = proof.clone();
        let abort_task = tokio::spawn(async move {
            abort_store
                .record_assignment_drain_decision(&abort_proof, abort)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire_many(2))
            .await
            .unwrap()
            .unwrap()
            .forget();
        raw.release.add_permits(2);

        let (commit_result, abort_result) = tokio::join!(commit_task, abort_task);
        let commit_result = commit_result.unwrap().unwrap();
        let abort_result = abort_result.unwrap().unwrap();
        assert_eq!(
            usize::from(matches!(
                &commit_result,
                RecordAssignmentDrainDecisionResult::Created(_)
            )) + usize::from(matches!(
                &abort_result,
                RecordAssignmentDrainDecisionResult::Created(_)
            )),
            1
        );
        assert_eq!(
            usize::from(matches!(
                &commit_result,
                RecordAssignmentDrainDecisionResult::Conflict { .. }
            )) + usize::from(matches!(
                &abort_result,
                RecordAssignmentDrainDecisionResult::Conflict { .. }
            )),
            1
        );
        let durable = store.assignment_drain_decision(2).await.unwrap().unwrap();
        let retry = store
            .record_assignment_drain_decision(&proof, durable.clone())
            .await
            .unwrap();
        assert_eq!(
            retry,
            RecordAssignmentDrainDecisionResult::Unchanged(durable)
        );
    }

    #[tokio::test]
    async fn takeover_fences_delayed_drain_commit_and_can_abort_the_transition() {
        let (raw, store) = blocking_once_at(10, lease_path(2));
        let incumbent = owner(1, 1, 1);
        let successor = owner(2, 2, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let old_proof = first.proof();
        let transition = assignment_drain_transition(&incumbent, old_proof.clone());
        let observation = store.observe_rival(&successor, &first).unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;

        let delayed = AssignmentDrainDecision::new(
            &transition,
            old_proof.clone(),
            AssignmentDrainVerdict::Commit,
        )
        .unwrap();
        let delayed_store = Arc::clone(&store);
        let delayed_proof = old_proof.clone();
        let delayed_task = tokio::spawn(async move {
            delayed_store
                .record_assignment_drain_decision(&delayed_proof, delayed)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();

        let LeaseOutcome::Acquired(takeover) = store
            .try_takeover(&successor, &observation, 20)
            .await
            .unwrap()
        else {
            panic!("successor must win the authority sequence");
        };
        raw.release.add_permits(1);
        assert!(matches!(
            delayed_task.await.unwrap(),
            Err(ClusterCheckpointAuthorityError::Fenced)
        ));

        let takeover_proof = takeover.proof();
        let abort = AssignmentDrainDecision::new(
            &transition,
            takeover_proof.clone(),
            AssignmentDrainVerdict::Abort,
        )
        .unwrap();
        assert!(matches!(
            store
                .record_assignment_drain_decision(&takeover_proof, abort)
                .await
                .unwrap(),
            RecordAssignmentDrainDecisionResult::Created(_)
        ));
        assert_eq!(
            store
                .assignment_drain_decision(2)
                .await
                .unwrap()
                .unwrap()
                .verdict,
            AssignmentDrainVerdict::Abort
        );
    }

    #[tokio::test]
    async fn assignment_drain_floor_compacts_history_and_rejects_stale_versions() {
        let store = store(1);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        for target_version in 2..=5 {
            let transition =
                assignment_drain_transition_at(&incumbent, proof.clone(), target_version);
            let decision = AssignmentDrainDecision::new(
                &transition,
                proof.clone(),
                AssignmentDrainVerdict::Commit,
            )
            .unwrap();
            assert!(matches!(
                store
                    .record_assignment_drain_decision(&proof, decision)
                    .await
                    .unwrap(),
                RecordAssignmentDrainDecisionResult::Created(_)
            ));
        }

        let head = store.load_record().await.unwrap().unwrap();
        let mut by_target_version = std::collections::BTreeMap::new();
        let mut link = head.assignment_decision_head;
        while let Some(current) = link {
            by_target_version.insert(current.target_version, current.sequence);
            link = read_authority_record(store.store.as_ref(), current.sequence)
                .await
                .unwrap()
                .unwrap()
                .previous_assignment_decision;
        }

        assert_eq!(
            store
                .prune_assignment_drain_decisions_before(&proof, 4)
                .await
                .unwrap(),
            4
        );
        let floor = store
            .load_record()
            .await
            .unwrap()
            .unwrap()
            .assignment_decision_floor
            .unwrap();
        assert_eq!(floor.before_target_version, 4);
        assert_eq!(floor.terminal_anchor.unwrap().target_version(), 3);
        assert!(matches!(
            store.assignment_drain_decision(3).await,
            Err(ClusterCheckpointAuthorityError::Decision(
                DecisionError::Conflict(_)
            ))
        ));
        for target_version in [4, 5] {
            assert_eq!(
                store
                    .assignment_drain_decision(target_version)
                    .await
                    .unwrap()
                    .unwrap()
                    .target_version(),
                target_version
            );
        }

        let stale_transition = assignment_drain_transition_at(&incumbent, proof.clone(), 3);
        let stale = AssignmentDrainDecision::new(
            &stale_transition,
            proof.clone(),
            AssignmentDrainVerdict::Commit,
        )
        .unwrap();
        assert!(matches!(
            store.record_assignment_drain_decision(&proof, stale).await,
            Err(ClusterCheckpointAuthorityError::Decision(
                DecisionError::Conflict(_)
            ))
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let _ = store
                    .acquire_or_renew_current_term_for_test(&incumbent, 10)
                    .await
                    .unwrap();
                let mut compacted_absent = true;
                for target_version in [2, 3] {
                    if read_authority_record(
                        store.store.as_ref(),
                        by_target_version[&target_version],
                    )
                    .await
                    .unwrap()
                    .is_some()
                    {
                        compacted_absent = false;
                        break;
                    }
                }
                if compacted_absent {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();

        for target_version in [4, 5] {
            assert!(read_authority_record(
                store.store.as_ref(),
                by_target_version[&target_version]
            )
            .await
            .unwrap()
            .is_some());
        }
    }

    #[tokio::test]
    async fn assignment_drain_floor_rejects_a_rewritten_anchor_link() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        for target_version in [2, 4] {
            let transition =
                assignment_drain_transition_at(&incumbent, proof.clone(), target_version);
            let decision = AssignmentDrainDecision::new(
                &transition,
                proof.clone(),
                AssignmentDrainVerdict::Commit,
            )
            .unwrap();
            store
                .record_assignment_drain_decision(&proof, decision)
                .await
                .unwrap();
        }
        store
            .prune_assignment_drain_decisions_before(&proof, 4)
            .await
            .unwrap();

        let mut corrupt = store.load_record().await.unwrap().unwrap();
        corrupt
            .assignment_decision_floor
            .as_mut()
            .unwrap()
            .terminal_anchor_link
            .as_mut()
            .unwrap()
            .sequence += 1;
        store
            .store
            .put(
                &lease_path(corrupt.lease.seq),
                PutPayload::from(Bytes::from(serde_json::to_vec(&corrupt).unwrap())),
            )
            .await
            .unwrap();
        assert!(matches!(
            store.assignment_drain_decision(4).await,
            Err(ClusterCheckpointAuthorityError::Decision(
                DecisionError::Conflict(_)
            ))
        ));
    }

    #[tokio::test]
    async fn delayed_cluster_decision_is_fenced_when_takeover_wins_next_sequence() {
        let (raw, store) = blocking_once_at(10, lease_path(2));
        let incumbent = owner(1, 1, 1);
        let successor = owner(2, 2, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let observation = store.observe_rival(&successor, &first).unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        let decision_store = Arc::clone(&store);
        let decision = tokio::spawn(async move {
            decision_store
                .record_cluster_outcome(&proof, 1, 10, fence, CheckpointVerdict::Abort, None)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();

        let LeaseOutcome::Acquired(takeover) = store
            .try_takeover(&successor, &observation, 20)
            .await
            .unwrap()
        else {
            panic!("successor must win the unblocked next sequence");
        };
        assert_eq!(takeover.owner, successor);
        raw.release.add_permits(1);
        assert!(matches!(
            decision.await.unwrap(),
            Err(ClusterCheckpointAuthorityError::Fenced)
        ));
        assert!(store.cluster_outcomes().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delayed_cluster_decision_retries_after_renewal_wins_next_sequence() {
        let (raw, store) = blocking_once_at(1_000, lease_path(2));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        let decision_store = Arc::clone(&store);
        let decision = tokio::spawn(async move {
            decision_store
                .record_cluster_outcome(&proof, 1, 10, fence, CheckpointVerdict::Abort, None)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();

        let LeaseOutcome::Acquired(renewal) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 1)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        assert_eq!(renewal.seq, 2);
        raw.release.add_permits(1);
        assert!(matches!(
            decision.await.unwrap().unwrap(),
            RecordOutcomeResult::Created(_)
        ));
        assert_eq!(store.load().await.unwrap().unwrap().seq, 3);
        assert_eq!(store.cluster_outcomes().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn delayed_cluster_decision_retries_after_catalog_seal_wins_next_sequence() {
        let (raw, store) = blocking_once_at(1_000, lease_path(2));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        let decision_store = Arc::clone(&store);
        let decision_proof = proof.clone();
        let decision = tokio::spawn(async move {
            decision_store
                .record_cluster_outcome(
                    &decision_proof,
                    1,
                    10,
                    fence,
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();

        let manifest = catalog("events");
        assert_eq!(
            store.seal_catalog(&proof, &manifest).await.unwrap(),
            CatalogSealOutcome::Created
        );
        raw.release.add_permits(1);
        assert!(matches!(
            decision.await.unwrap().unwrap(),
            RecordOutcomeResult::Created(_)
        ));
        let head = store.load().await.unwrap().unwrap();
        assert_eq!(head.seq, 3);
        let reference = head.catalog_manifest.expect("catalog seal must survive");
        assert_eq!(
            store.load_catalog_manifest(&reference).await.unwrap(),
            manifest
        );
        assert_eq!(store.cluster_outcomes().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn delayed_cluster_decision_retries_after_floor_advance_wins_next_sequence() {
        let (raw, store) = blocking_once_at(1_000, lease_path(4));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        record_commit(&store, &proof, &fence, 1, 10).await;
        record_commit(&store, &proof, &fence, 3, 30).await;

        let decision_store = Arc::clone(&store);
        let decision_proof = proof.clone();
        let decision_fence = fence.clone();
        let decision = tokio::spawn(async move {
            decision_store
                .record_cluster_outcome(
                    &decision_proof,
                    4,
                    40,
                    decision_fence,
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();

        assert_eq!(
            store
                .prune_cluster_outcomes_before(&proof, 3, accept_recovery_artifacts)
                .await
                .unwrap(),
            3
        );
        raw.release.add_permits(1);
        assert!(matches!(
            decision.await.unwrap().unwrap(),
            RecordOutcomeResult::Created(_)
        ));
        assert_eq!(
            store
                .cluster_outcomes()
                .await
                .unwrap()
                .into_iter()
                .map(|outcome| outcome.epoch)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
        assert_eq!(
            store
                .cluster_outcome_retention_boundary()
                .await
                .unwrap()
                .artifact_before_epoch,
            3
        );
    }

    #[tokio::test]
    async fn ambiguous_cluster_decision_reconciles_exact_canonical_winner() {
        let (raw, store) = ambiguous_once_at(1_000, lease_path(2));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let result = store
            .record_cluster_outcome(
                &first.proof(),
                1,
                10,
                assignment_fence(&incumbent),
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
        assert!(raw
            .did_return_ambiguous
            .load(std::sync::atomic::Ordering::Acquire));
        assert!(matches!(result, RecordOutcomeResult::Unchanged(_)));
        let outcomes = store.cluster_outcomes().await.unwrap();
        assert_eq!(
            outcomes
                .iter()
                .map(|outcome| (outcome.epoch, outcome.checkpoint_id))
                .collect::<Vec<_>>(),
            vec![(1, 10)]
        );
    }

    #[tokio::test]
    async fn ambiguous_cluster_decision_compacted_before_reconciliation_fails_closed() {
        let (raw, store) = delayed_ambiguous_response_once_at(1_000, lease_path(2));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        disable_history_pruning_for_test(&store).await;
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        let delayed_store = Arc::clone(&store);
        let delayed_proof = proof.clone();
        let delayed_fence = fence.clone();
        let delayed = tokio::spawn(async move {
            delayed_store
                .record_cluster_outcome(
                    &delayed_proof,
                    1,
                    10,
                    delayed_fence,
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();

        for epoch in 2..=u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 1).unwrap() {
            store
                .record_cluster_outcome(
                    &proof,
                    epoch,
                    epoch * 10,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }
        let boundary = store.cluster_outcome_retention_boundary().await.unwrap();
        assert!(boundary.terminal_before_epoch > 1);
        assert!(store
            .cluster_outcomes()
            .await
            .unwrap()
            .iter()
            .all(|outcome| outcome.epoch != 1));

        raw.release.add_permits(1);
        let error = delayed.await.unwrap().unwrap_err();
        assert!(raw
            .did_return_ambiguous
            .load(std::sync::atomic::Ordering::Acquire));
        assert!(
            matches!(
                error,
                ClusterCheckpointAuthorityError::Decision(DecisionError::Conflict(_))
            ),
            "{error}"
        );
        assert_eq!(
            read_authority_record(raw.as_ref(), 2)
                .await
                .unwrap()
                .unwrap()
                .checkpoint_outcome
                .unwrap()
                .epoch,
            1
        );
    }

    #[tokio::test]
    async fn exact_cluster_outcome_bounds_latest_future_and_older_reads() {
        let (raw, store) = blocking_once_at(
            1_000,
            OsPath::from("control/never-block-exact-outcome-reads"),
        );
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        for epoch in 1..=4 {
            store
                .record_cluster_outcome(
                    &proof,
                    epoch,
                    epoch * 10,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.prune_running.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let deployment_path = OsPath::from("checkpoint-deployment/identity.json");

        raw.clear_get_counts();
        let latest = store.cluster_outcome(4).await.unwrap().unwrap();
        assert_eq!((latest.epoch, latest.checkpoint_id), (4, 40));
        assert_eq!(raw.get_count(&lease_path(5)), 1);
        assert_eq!(raw.get_count(&lease_path(4)), 0);
        assert_eq!(raw.get_count(&deployment_path), 0);

        raw.clear_get_counts();
        assert!(store.cluster_outcome(5).await.unwrap().is_none());
        assert_eq!(raw.get_count(&lease_path(5)), 1);
        assert_eq!(raw.get_count(&lease_path(4)), 0);
        assert_eq!(raw.get_count(&deployment_path), 0);

        raw.clear_get_counts();
        let older = store.cluster_outcome(2).await.unwrap().unwrap();
        assert_eq!((older.epoch, older.checkpoint_id), (2, 20));
        assert_eq!(raw.get_count(&lease_path(5)), 1);
        assert_eq!(raw.get_count(&lease_path(4)), 0);
        assert_eq!(raw.get_count(&lease_path(3)), 0);
        assert_eq!(raw.get_count(&lease_path(2)), 0);
        assert_eq!(raw.get_count(&deployment_path), 0);

        let restarted = LeaderLeaseStore::new(raw.clone(), 1_000);
        raw.clear_get_counts();
        let cold_older = restarted.cluster_outcome(2).await.unwrap().unwrap();
        assert_eq!((cold_older.epoch, cold_older.checkpoint_id), (2, 20));
        assert_eq!(raw.get_count(&lease_path(5)), 1);
        assert_eq!(raw.get_count(&lease_path(4)), 1);
        assert_eq!(raw.get_count(&lease_path(3)), 1);
        assert_eq!(raw.get_count(&lease_path(2)), 0);
        assert_eq!(raw.get_count(&deployment_path), 1);

        record_commit(store.as_ref(), &proof, &fence, 5, 50).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.prune_running.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let recovery_reference = store
            .load_record()
            .await
            .unwrap()
            .unwrap()
            .checkpoint_outcome
            .unwrap()
            .recovery_capsule
            .unwrap();
        raw.clear_get_counts();
        let (committed, capsule) = store
            .cluster_outcome_with_recovery_capsule(5)
            .await
            .unwrap()
            .unwrap();
        assert!(committed.is_commit());
        assert_eq!(capsule.unwrap().attempt.epoch, 5);
        assert_eq!(raw.get_count(&lease_path(6)), 1);
        assert_eq!(raw.get_count(&deployment_path), 1);
        assert_eq!(
            raw.get_count(&recovery_capsule_path(&recovery_reference)),
            1
        );
    }

    #[tokio::test]
    async fn exact_cluster_outcome_retries_a_disappearing_admission() {
        let (raw, store) = blocking_once_at(
            1_000,
            OsPath::from("control/never-block-exact-outcome-retry"),
        );
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        for epoch in 1..=3 {
            store
                .record_cluster_outcome(
                    &proof,
                    epoch,
                    epoch * 10,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.prune_running.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        raw.inner.delete(&lease_path(3)).await.unwrap();
        raw.clear_get_counts();
        let restarted = LeaderLeaseStore::new(raw.clone(), 1_000);

        assert!(matches!(
            restarted.cluster_outcome(1).await,
            Err(ClusterCheckpointAuthorityError::Decision(
                DecisionError::InventoryChanged(_)
            ))
        ));
        assert_eq!(raw.get_count(&lease_path(4)), 3);
        assert_eq!(raw.get_count(&lease_path(3)), 3);
        assert_eq!(
            raw.get_count(&OsPath::from("checkpoint-deployment/identity.json")),
            0
        );
    }

    #[tokio::test]
    async fn exact_cluster_outcome_rejects_a_rewritten_immutable_link() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        store
            .record_cluster_outcome(&proof, 1, 10, fence.clone(), CheckpointVerdict::Abort, None)
            .await
            .unwrap();
        store
            .acquire_or_renew_current_term_for_test(&incumbent, 1)
            .await
            .unwrap();
        store
            .record_cluster_outcome(&proof, 3, 30, fence, CheckpointVerdict::Abort, None)
            .await
            .unwrap();

        let mut corrupt = store.load_record().await.unwrap().unwrap();
        corrupt.previous_outcome = Some(OutcomeLink {
            sequence: 3,
            epoch: 1,
            checkpoint_id: 10,
        });
        store
            .store
            .put(
                &lease_path(corrupt.lease.seq),
                PutPayload::from(encode_authority_record(&corrupt).unwrap()),
            )
            .await
            .unwrap();
        let restarted = LeaderLeaseStore::new(Arc::clone(&store.store), 1_000);

        assert!(matches!(
            restarted.cluster_outcome(1).await,
            Err(ClusterCheckpointAuthorityError::Decision(
                DecisionError::Conflict(_)
            ))
        ));
    }

    #[tokio::test]
    async fn outcome_audit_rejects_a_commit_chain_link_to_abort() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        disable_history_pruning_for_test(&store).await;
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        record_commit(&store, &proof, &fence, 1, 10).await;
        store
            .record_cluster_outcome(&proof, 2, 20, fence.clone(), CheckpointVerdict::Abort, None)
            .await
            .unwrap();
        record_commit(&store, &proof, &fence, 3, 30).await;

        let mut corrupt = store.load_record().await.unwrap().unwrap();
        corrupt.previous_commit = Some(OutcomeLink {
            sequence: corrupt.lease.seq - 1,
            epoch: 2,
            checkpoint_id: 20,
        });
        store
            .store
            .put(
                &lease_path(corrupt.lease.seq),
                PutPayload::from(encode_authority_record(&corrupt).unwrap()),
            )
            .await
            .unwrap();
        *store.outcome_audit_cache.lock() = None;

        assert!(matches!(
            store.highest_cluster_terminal_outcome().await,
            Err(ClusterCheckpointAuthorityError::Decision(
                DecisionError::Conflict(_)
            ))
        ));
    }

    #[tokio::test]
    async fn cluster_attempt_settlement_returns_exact_or_strict_dominator() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        store
            .record_cluster_outcome(&proof, 1, 10, fence.clone(), CheckpointVerdict::Abort, None)
            .await
            .unwrap();
        store
            .record_cluster_outcome(&proof, 3, 30, fence, CheckpointVerdict::Abort, None)
            .await
            .unwrap();

        let exact = store
            .cluster_attempt_settlement(crate::state::CheckpointAttempt::new(1, 10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!((exact.epoch, exact.checkpoint_id), (1, 10));
        let dominator = store
            .cluster_attempt_settlement(crate::state::CheckpointAttempt::new(2, 20))
            .await
            .unwrap()
            .unwrap();
        assert_eq!((dominator.epoch, dominator.checkpoint_id), (3, 30));
        assert!(store
            .cluster_attempt_settlement(crate::state::CheckpointAttempt::new(4, 40))
            .await
            .unwrap()
            .is_none());
        assert!(matches!(
            store
                .cluster_attempt_settlement(crate::state::CheckpointAttempt::new(2, 35))
                .await,
            Err(ClusterCheckpointAuthorityError::Decision(
                DecisionError::Conflict(_)
            ))
        ));
        assert!(matches!(
            store
                .cluster_attempt_settlement(crate::state::CheckpointAttempt::new(2, 10))
                .await,
            Err(ClusterCheckpointAuthorityError::Decision(
                DecisionError::Conflict(_)
            ))
        ));
    }

    #[tokio::test]
    async fn cluster_attempt_settlement_preserves_fences_across_outcome_compaction() {
        let store = store(1);
        let incumbent = owner(1, 11, 7);
        let successor = owner(2, 22, 8);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let incumbent_proof = first.proof();
        let incumbent_fence = assignment_fence(&incumbent);
        store
            .record_cluster_outcome(
                &incumbent_proof,
                1,
                10,
                incumbent_fence.clone(),
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
        let compacted_sequence = store.load_record().await.unwrap().unwrap().lease.seq;

        let observation = store.observe_rival(&successor, &first).unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        let LeaseOutcome::Acquired(takeover) = store
            .try_takeover(&successor, &observation, 2)
            .await
            .unwrap()
        else {
            panic!("successor must acquire after a full observation");
        };
        let successor_proof = takeover.proof();
        let successor_fence = CheckpointAssignmentFence::from_owner_map(
            2,
            &[successor.node.0],
            vec![crate::checkpoint::CheckpointParticipant {
                node_id: successor.node.0,
                boot_incarnation: successor.boot,
            }],
        )
        .unwrap();
        record_commit(&store, &successor_proof, &successor_fence, 3, 30).await;

        assert_eq!(
            store
                .prune_cluster_outcomes_before(&successor_proof, 3, accept_recovery_artifacts,)
                .await
                .unwrap(),
            3
        );
        LeaderLeaseStore::prune_history(&store.store, 0)
            .await
            .unwrap();
        assert!(
            read_authority_record(store.store.as_ref(), compacted_sequence)
                .await
                .unwrap()
                .is_none(),
            "the exact outcome record must be physically pruned"
        );

        let exact_anchor = store
            .cluster_attempt_settlement(crate::state::CheckpointAttempt::new(1, 10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(exact_anchor.verdict, CheckpointVerdict::Abort);
        assert_eq!(
            exact_anchor.assignment_fence.as_ref(),
            Some(&incumbent_fence)
        );
        assert_eq!(exact_anchor.leader_proof.as_ref(), Some(&incumbent_proof));
        assert_eq!(
            exact_anchor
                .leader_proof
                .as_ref()
                .unwrap()
                .owner
                .process_term,
            incumbent.process_term
        );

        let strict_dominator = store
            .cluster_attempt_settlement(crate::state::CheckpointAttempt::new(2, 20))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (strict_dominator.epoch, strict_dominator.checkpoint_id),
            (3, 30)
        );
        assert_eq!(strict_dominator.verdict, CheckpointVerdict::Commit);
        assert_eq!(
            strict_dominator.assignment_fence.as_ref(),
            Some(&successor_fence)
        );
        assert_eq!(
            strict_dominator.leader_proof.as_ref(),
            Some(&successor_proof)
        );
        assert_eq!(
            strict_dominator
                .leader_proof
                .as_ref()
                .unwrap()
                .owner
                .process_term,
            successor.process_term
        );
    }

    #[tokio::test]
    async fn cluster_outcome_audit_cache_reuses_unchanged_head_and_reaudits_changed_head() {
        let (raw, store) = blocking_store_at(
            1_000,
            OsPath::from("control/never-block-outcome-audit-cache"),
        );
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        store
            .record_cluster_outcome(&proof, 1, 10, fence.clone(), CheckpointVerdict::Abort, None)
            .await
            .unwrap();
        store
            .record_cluster_outcome(&proof, 2, 20, fence.clone(), CheckpointVerdict::Abort, None)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.prune_running.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        *store.outcome_audit_cache.lock() = None;
        raw.clear_get_counts();
        let exact = store
            .cluster_attempt_settlement(crate::state::CheckpointAttempt::new(1, 10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!((exact.epoch, exact.checkpoint_id), (1, 10));
        assert_eq!(raw.get_count(&lease_path(2)), 1);

        raw.clear_get_counts();
        let highest = store
            .highest_cluster_terminal_outcome()
            .await
            .unwrap()
            .unwrap();
        assert_eq!((highest.epoch, highest.checkpoint_id), (2, 20));
        assert_eq!(raw.get_count(&lease_path(3)), 1);
        assert_eq!(raw.get_count(&lease_path(2)), 0);

        let external = LeaderLeaseStore::new(raw.clone(), 1_000);
        external
            .record_cluster_outcome(&proof, 3, 30, fence, CheckpointVerdict::Abort, None)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while external.prune_running.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        raw.clear_get_counts();
        let highest = store
            .highest_cluster_terminal_outcome()
            .await
            .unwrap()
            .unwrap();
        assert_eq!((highest.epoch, highest.checkpoint_id), (3, 30));
        assert_eq!(raw.get_count(&lease_path(3)), 1);
        assert_eq!(raw.get_count(&lease_path(2)), 1);
    }

    #[tokio::test]
    async fn concurrent_cold_outcome_audits_read_each_history_link_once() {
        let (raw, store) = blocking_get_once_at(1_000, lease_path(3));
        raw.did_block.store(true, Ordering::Release);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        for (epoch, checkpoint_id) in [(1, 10), (2, 20), (3, 30)] {
            store
                .record_cluster_outcome(
                    &proof,
                    epoch,
                    checkpoint_id,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.prune_running.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        *store.outcome_audit_cache.lock() = None;
        raw.clear_get_counts();
        raw.did_block.store(false, Ordering::Release);
        let first_store = Arc::clone(&store);
        let first_audit =
            tokio::spawn(async move { first_store.highest_cluster_terminal_outcome().await });
        raw.entered.acquire().await.unwrap().forget();

        let mut followers = Vec::new();
        for _ in 0..16 {
            let follower = Arc::clone(&store);
            followers.push(tokio::spawn(async move {
                follower.highest_cluster_terminal_outcome().await
            }));
        }
        tokio::task::yield_now().await;
        raw.release.add_permits(1);

        assert_eq!(first_audit.await.unwrap().unwrap().unwrap().epoch, 3);
        for follower in followers {
            assert_eq!(follower.await.unwrap().unwrap().unwrap().epoch, 3);
        }
        assert_eq!(raw.get_count(&lease_path(3)), 1);
        assert_eq!(raw.get_count(&lease_path(2)), 1);
    }

    #[tokio::test]
    async fn failed_outcome_audit_is_not_retained() {
        let (raw, store) = blocking_store_at(
            1_000,
            OsPath::from("control/never-block-failed-outcome-audit"),
        );
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        for (epoch, checkpoint_id) in [(1, 10), (2, 20)] {
            store
                .record_cluster_outcome(
                    &proof,
                    epoch,
                    checkpoint_id,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.prune_running.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let path = lease_path(2);
        let saved = raw.inner.get(&path).await.unwrap().bytes().await.unwrap();
        raw.inner.delete(&path).await.unwrap();
        *store.outcome_audit_cache.lock() = None;
        assert!(matches!(
            store.highest_cluster_terminal_outcome().await,
            Err(ClusterCheckpointAuthorityError::Decision(
                DecisionError::InventoryChanged(_)
            ))
        ));

        raw.inner.put(&path, PutPayload::from(saved)).await.unwrap();
        raw.clear_get_counts();
        assert_eq!(
            store
                .highest_cluster_terminal_outcome()
                .await
                .unwrap()
                .unwrap()
                .epoch,
            2
        );
        assert_eq!(raw.get_count(&path), 1);
    }

    #[tokio::test]
    async fn repeated_cluster_outcome_appends_do_not_reaudit_history() {
        let (raw, store) = blocking_store_at(
            1_000,
            OsPath::from("control/never-block-outcome-hot-appends"),
        );
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.prune_running.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        store.prune_running.store(true, Ordering::Release);
        raw.clear_get_counts();

        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        for epoch in 1..=8 {
            store
                .record_cluster_outcome(
                    &proof,
                    epoch,
                    epoch * 10,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }

        let counts = (1..=9)
            .map(|sequence| raw.get_count(&lease_path(sequence)))
            .collect::<Vec<_>>();
        assert_eq!(counts, vec![3, 6, 6, 6, 6, 6, 6, 6, 3]);
        assert_eq!(raw.get_count(&authority_head_path()), 24);
        store.prune_running.store(false, Ordering::Release);
    }

    #[tokio::test]
    async fn all_abort_history_compacts_without_advancing_artifact_retention() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        disable_history_pruning_for_test(&store).await;
        let empty = store.cluster_outcome_retention_boundary().await.unwrap();
        assert_eq!(
            (empty.artifact_before_epoch, empty.terminal_before_epoch),
            (0, 0)
        );
        assert!(empty.committed_anchor.is_none());

        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        for epoch in 1..=u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 1).unwrap() {
            store
                .record_cluster_outcome(
                    &proof,
                    epoch,
                    epoch * 10,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }

        let boundary = store.cluster_outcome_retention_boundary().await.unwrap();
        assert_eq!(boundary.artifact_before_epoch, 0);
        assert!(boundary.terminal_before_epoch > 1);
        assert!(boundary.committed_anchor.is_none());
        let terminal_anchor = boundary.terminal_anchor.unwrap();
        assert!(!terminal_anchor.is_commit());
        assert_eq!(terminal_anchor.epoch + 1, boundary.terminal_before_epoch);
        LeaderLeaseStore::prune_history(&store.store, 0)
            .await
            .unwrap();
        let restarted = LeaderLeaseStore::new(Arc::clone(&store.store), 1_000);
        assert!(restarted
            .highest_cluster_committed_outcome()
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            restarted
                .cluster_outcome(terminal_anchor.epoch)
                .await
                .unwrap(),
            Some(terminal_anchor.clone())
        );

        let compacted_attempt = restarted
            .cluster_attempt_settlement(crate::state::CheckpointAttempt::new(1, 10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            compacted_attempt.epoch,
            u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 1).unwrap()
        );
        let exact_anchor = restarted
            .cluster_attempt_settlement(crate::state::CheckpointAttempt::new(
                terminal_anchor.epoch,
                terminal_anchor.checkpoint_id,
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(exact_anchor, terminal_anchor);
    }

    #[tokio::test]
    async fn history_compaction_retains_lagged_commit_inventory() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        disable_history_pruning_for_test(&store).await;
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        record_commit(&store, &proof, &fence, 1, 10).await;
        for epoch in 2..=u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 1).unwrap() {
            store
                .record_cluster_outcome(
                    &proof,
                    epoch,
                    epoch * 10,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }

        let boundary = store
            .audited_cluster_outcome_retention_boundary()
            .await
            .unwrap();
        assert_eq!(boundary.artifact_before_epoch, 0);
        assert!(boundary.terminal_before_epoch > 1);
        assert!(boundary.committed_anchor.is_none());
        assert!(boundary.terminal_anchor.as_ref().unwrap().epoch > 1);
        LeaderLeaseStore::prune_history(&store.store, 0)
            .await
            .unwrap();
        let restarted = LeaderLeaseStore::new(Arc::clone(&store.store), 1_000);
        assert_eq!(
            restarted
                .highest_cluster_committed_outcome()
                .await
                .unwrap()
                .unwrap()
                .epoch,
            1
        );
    }

    #[tokio::test]
    async fn outcome_inventory_pairs_divergent_horizons_with_one_audited_head() {
        let (raw, store) = blocking_store_at(
            1_000,
            OsPath::from("control/never-block-paired-outcome-inventory"),
        );
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        disable_history_pruning_for_test(&store).await;
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        record_commit(&store, &proof, &fence, 1, 10).await;
        record_commit(&store, &proof, &fence, 3, 30).await;
        store
            .prune_cluster_outcomes_before(&proof, 3, accept_recovery_artifacts)
            .await
            .unwrap();
        let last_epoch = u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 3).unwrap();
        for epoch in 4..=last_epoch {
            store
                .record_cluster_outcome(
                    &proof,
                    epoch,
                    epoch * 10,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }

        raw.clear_get_counts();
        let inventory = store.cluster_outcome_inventory().await.unwrap();

        assert_eq!(inventory.retention_boundary.artifact_before_epoch, 3);
        assert!(inventory.retention_boundary.terminal_before_epoch > 3);
        assert_eq!(
            inventory
                .retention_boundary
                .committed_anchor
                .as_ref()
                .unwrap()
                .epoch,
            1
        );
        assert_eq!(
            inventory.outcomes.first().map(|outcome| outcome.epoch),
            Some(3)
        );
        assert!(inventory
            .outcomes
            .iter()
            .all(|outcome| outcome.epoch >= inventory.retention_boundary.artifact_before_epoch));
        assert_eq!(raw.get_count(&authority_head_path()), 1);
        assert_eq!(
            raw.get_count_prefix(LEASE_PREFIX),
            2,
            "paired inventory must use one head-and-successor snapshot for its boundary"
        );
    }

    #[tokio::test]
    async fn validated_outcome_inventory_retries_after_heads_and_floor_advance() {
        let store = Arc::new(store(1_000));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        disable_history_pruning_for_test(&store).await;
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        for (epoch, checkpoint_id) in [(1, 10), (3, 30), (5, 50)] {
            record_commit(&store, &proof, &fence, epoch, checkpoint_id).await;
        }
        store
            .prune_cluster_outcomes_before(&proof, 2, accept_recovery_artifacts)
            .await
            .unwrap();

        let mutated = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let preflighted = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mutation_store = Arc::clone(&store);
        let mutation_proof = proof.clone();
        let mutation_fence = fence.clone();
        let inventory = store
            .validated_cluster_outcome_inventory({
                let mutated = Arc::clone(&mutated);
                let preflighted = Arc::clone(&preflighted);
                move |outcome| {
                    let mutated = Arc::clone(&mutated);
                    let preflighted = Arc::clone(&preflighted);
                    let store = Arc::clone(&mutation_store);
                    let proof = mutation_proof.clone();
                    let fence = mutation_fence.clone();
                    async move {
                        preflighted.lock().unwrap().push(outcome.epoch);
                        if !mutated.swap(true, std::sync::atomic::Ordering::AcqRel) {
                            record_commit(&store, &proof, &fence, 7, 70).await;
                            store
                                .prune_cluster_outcomes_before(&proof, 5, accept_recovery_artifacts)
                                .await
                                .unwrap();
                        }
                        Ok(())
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(*preflighted.lock().unwrap(), vec![5, 7]);
        assert_eq!(inventory.retention_boundary.artifact_before_epoch, 5);
        assert_eq!(
            inventory
                .outcomes
                .iter()
                .map(|outcome| outcome.epoch)
                .collect::<Vec<_>>(),
            vec![5, 7]
        );
    }

    #[tokio::test]
    async fn mixed_history_keeps_lagged_commits_through_compaction_prune_and_restart() {
        let (raw, store) = blocking_store_at(
            1_000,
            OsPath::from("control/never-block-mixed-commit-history"),
        );
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        disable_history_pruning_for_test(&store).await;
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        let commit_epochs = [1, 17, 49, 97, 145, 193, 241, 257];
        for epoch in 1..=260 {
            if commit_epochs.contains(&epoch) {
                record_commit(&store, &proof, &fence, epoch, epoch * 10).await;
            } else {
                store
                    .record_cluster_outcome(
                        &proof,
                        epoch,
                        epoch * 10,
                        fence.clone(),
                        CheckpointVerdict::Abort,
                        None,
                    )
                    .await
                    .unwrap();
            }
        }

        let head = store.load_record().await.unwrap().unwrap();
        let snapshot = store
            .cached_audited_cluster_outcomes_from(&head)
            .await
            .unwrap();
        assert_eq!(
            snapshot
                .outcomes
                .iter()
                .filter(|outcome| outcome.is_commit())
                .map(|outcome| outcome.epoch)
                .collect::<Vec<_>>(),
            commit_epochs.to_vec()
        );
        assert!(head.outcome_floor.as_ref().unwrap().authority_before_epoch > 193);
        assert_eq!(snapshot.commit_links.len(), commit_epochs.len());

        *store.outcome_audit_cache.lock() = None;
        raw.clear_get_counts();
        assert_eq!(store.cluster_outcome(17).await.unwrap().unwrap().epoch, 17);
        assert!(
            raw.get_count_prefix(LEASE_PREFIX) <= u64::try_from(commit_epochs.len() + 1).unwrap(),
            "lagged exact lookup must traverse only the Commit chain"
        );

        raw.clear_get_counts();
        let cold = LeaderLeaseStore::new(raw.clone(), 1_000);
        let cold_outcomes = cold.cluster_outcomes().await.unwrap();
        assert_eq!(
            cold_outcomes
                .iter()
                .filter(|outcome| outcome.is_commit())
                .map(|outcome| outcome.epoch)
                .collect::<Vec<_>>(),
            commit_epochs.to_vec()
        );
        let cold_head = cold.load_record().await.unwrap().unwrap();
        let cold_snapshot = cold
            .cached_audited_cluster_outcomes_from(&cold_head)
            .await
            .unwrap();
        assert!(
            raw.get_count_prefix(LEASE_PREFIX)
                <= u64::try_from(
                    cold_snapshot.terminal_links.len() + cold_snapshot.commit_links.len() + 3,
                )
                .unwrap(),
            "cold audit must not read records shared by both chains twice"
        );

        assert_eq!(
            store
                .prune_cluster_outcomes_before(&proof, 194, accept_recovery_artifacts)
                .await
                .unwrap(),
            194
        );
        let pruned_head = store.load_record().await.unwrap().unwrap();
        let floor = pruned_head.outcome_floor.as_ref().unwrap();
        assert_eq!(floor.committed_anchor.as_ref().unwrap().epoch, 193);
        let compacted_commit_sequence = floor.committed_anchor_link.unwrap().sequence;
        let retained_commit_sequence = store
            .cached_audited_cluster_outcomes_from(&pruned_head)
            .await
            .unwrap()
            .commit_links
            .iter()
            .find(|link| link.epoch == 241)
            .unwrap()
            .sequence;

        LeaderLeaseStore::prune_history(&store.store, 0)
            .await
            .unwrap();
        assert!(
            read_authority_record(raw.as_ref(), compacted_commit_sequence)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            read_authority_record(raw.as_ref(), retained_commit_sequence)
                .await
                .unwrap()
                .is_some()
        );

        let restarted = LeaderLeaseStore::new(raw.clone(), 1_000);
        assert!(restarted.cluster_outcome(193).await.unwrap().is_none());
        assert_eq!(
            restarted.cluster_outcome(241).await.unwrap().unwrap().epoch,
            241
        );
        assert_eq!(
            restarted
                .cluster_outcomes()
                .await
                .unwrap()
                .into_iter()
                .filter(CheckpointOutcome::is_commit)
                .map(|outcome| outcome.epoch)
                .collect::<Vec<_>>(),
            vec![241, 257]
        );
        let boundary = restarted
            .audited_cluster_outcome_retention_boundary()
            .await
            .unwrap();
        assert_eq!(boundary.artifact_before_epoch, 194);
        assert_eq!(boundary.committed_anchor.unwrap().epoch, 193);
    }

    #[tokio::test]
    async fn next_commit_is_rejected_at_the_live_commit_capacity_before_sequence_creation() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        disable_history_pruning_for_test(&store).await;
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        let first_capsule = recovery_capsule(&store, &fence, 1, 10).await;
        store
            .record_cluster_outcome(
                &proof,
                1,
                10,
                fence.clone(),
                CheckpointVerdict::Commit,
                Some(first_capsule),
            )
            .await
            .unwrap();

        let maximum = u64::try_from(MAX_LIVE_AUTHORITY_LINKS).unwrap();
        let authority_before_epoch = maximum
            .checked_sub(u64::try_from(OUTCOME_HISTORY_RETAINED_LINKS).unwrap())
            .and_then(|epoch| epoch.checked_add(1))
            .unwrap();
        let terminal_anchor_epoch = authority_before_epoch - 1;
        let mut current = store.load_record().await.unwrap().unwrap();
        let template = current.checkpoint_outcome.clone().unwrap();
        let mut terminal_anchor = None;
        let mut terminal_anchor_link = None;
        for epoch in 2..=maximum {
            let sequence = current.lease.seq.checked_add(1).unwrap();
            let checkpoint_id = epoch.checked_mul(10).unwrap();
            let mut outcome = template.clone();
            outcome.epoch = epoch;
            outcome.checkpoint_id = checkpoint_id;
            let reference = outcome.recovery_capsule.as_mut().unwrap();
            reference.epoch = epoch;
            reference.checkpoint_id = checkpoint_id;
            let link = OutcomeLink {
                sequence,
                epoch,
                checkpoint_id,
            };
            let mut lease = current.lease.clone();
            lease.seq = sequence;
            let mut next = current.preserve_with_lease(lease);
            next.checkpoint_outcome = Some(outcome.clone());
            next.previous_outcome = current.outcome_head;
            next.outcome_head = Some(link);
            next.previous_commit = current.commit_head;
            next.commit_head = Some(link);
            store
                .store
                .put(
                    &lease_path(sequence),
                    PutPayload::from(encode_authority_record(&next).unwrap()),
                )
                .await
                .unwrap();
            if epoch == terminal_anchor_epoch {
                terminal_anchor = Some(outcome);
                terminal_anchor_link = Some(link);
            }
            current = next;
        }

        let floor = AuthorityOutcomeFloor {
            deployment_id: template.deployment_id,
            artifact_before_epoch: 0,
            authority_before_epoch,
            terminal_anchor,
            terminal_anchor_link,
            committed_anchor: None,
            committed_anchor_link: None,
        };
        let floor_sequence = current.lease.seq.checked_add(1).unwrap();
        let mut floor_lease = current.lease.clone();
        floor_lease.seq = floor_sequence;
        let mut floor_head = current.preserve_with_lease(floor_lease);
        floor_head.outcome_floor = Some(floor);
        store
            .store
            .put(
                &lease_path(floor_sequence),
                PutPayload::from(encode_authority_record(&floor_head).unwrap()),
            )
            .await
            .unwrap();
        store
            .store
            .put(
                &authority_head_path(),
                PutPayload::from(encode_authority_head_pointer(floor_sequence).unwrap()),
            )
            .await
            .unwrap();
        *store.outcome_audit_cache.lock() = None;

        let next_epoch = maximum.checked_add(1).unwrap();
        let next_checkpoint_id = next_epoch.checked_mul(10).unwrap();
        let next_capsule = recovery_capsule(&store, &fence, next_epoch, next_checkpoint_id).await;
        let error = store
            .record_cluster_outcome(
                &proof,
                next_epoch,
                next_checkpoint_id,
                fence,
                CheckpointVerdict::Commit,
                Some(next_capsule),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ClusterCheckpointAuthorityError::Decision(DecisionError::Conflict(message))
                if message.contains("live Commit retention reached")
        ));
        assert!(
            read_authority_record(store.store.as_ref(), floor_sequence + 1)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(store.load_record().await.unwrap().unwrap(), floor_head);
    }

    #[tokio::test]
    async fn hot_history_compaction_uses_the_cached_anchor_link() {
        let (raw, store) = blocking_store_at(
            1_000,
            OsPath::from("control/never-block-hot-history-compaction"),
        );
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        disable_history_pruning_for_test(&store).await;
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        for epoch in 1..=u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER).unwrap() {
            store
                .record_cluster_outcome(
                    &proof,
                    epoch,
                    epoch * 10,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }

        raw.clear_get_counts();
        let next_epoch = u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 1).unwrap();
        store
            .record_cluster_outcome(
                &proof,
                next_epoch,
                next_epoch * 10,
                fence,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            raw.get_count(&authority_head_path()),
            6,
            "hot compaction must use a fixed number of pointer reads"
        );
        assert_eq!(
            raw.get_count_prefix(LEASE_PREFIX),
            12,
            "hot compaction must use only bounded head and successor reads"
        );
    }

    #[tokio::test]
    async fn restarted_authority_compacts_before_append_with_bounded_terminal_reads() {
        let (raw, store) = blocking_store_at(
            1_000,
            OsPath::from("control/never-block-restarted-history-compaction"),
        );
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        disable_history_pruning_for_test(&store).await;
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        let second_trigger =
            OUTCOME_HISTORY_COMPACTION_TRIGGER * 2 - OUTCOME_HISTORY_RETAINED_LINKS;
        for epoch in 1..=u64::try_from(second_trigger).unwrap() {
            store
                .record_cluster_outcome(
                    &proof,
                    epoch,
                    epoch * 10,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }

        let restarted = LeaderLeaseStore::new(raw.clone(), 1_000);
        restarted.prune_running.store(true, Ordering::Release);
        raw.clear_get_counts();
        let next_epoch = u64::try_from(second_trigger + 1).unwrap();
        restarted
            .record_cluster_outcome(
                &proof,
                next_epoch,
                next_epoch * 10,
                fence,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();

        let head = restarted.load_record().await.unwrap().unwrap();
        let snapshot = restarted
            .cached_audited_cluster_outcomes_from(&head)
            .await
            .unwrap();
        assert!(snapshot.terminal_links.len() <= OUTCOME_HISTORY_COMPACTION_TRIGGER);
        assert_eq!(
            raw.get_count(&authority_head_path()),
            7,
            "cold compaction must use a fixed number of pointer reads"
        );
        assert_eq!(
            raw.get_count_prefix(LEASE_PREFIX),
            u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 14).unwrap(),
            "cold compaction must perform exactly one bounded authority-chain audit"
        );
        assert!(head.outcome_floor.as_ref().unwrap().authority_before_epoch > 1);
    }

    #[tokio::test]
    async fn corrupt_pending_commit_capsule_does_not_block_terminal_compaction() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        disable_history_pruning_for_test(&store).await;
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        let capsule = recovery_capsule(&store, &fence, 1, 10).await;
        store
            .record_cluster_outcome(
                &proof,
                1,
                10,
                fence.clone(),
                CheckpointVerdict::Commit,
                Some(capsule.clone()),
            )
            .await
            .unwrap();
        for epoch in 2..=u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER).unwrap() {
            store
                .record_cluster_outcome(
                    &proof,
                    epoch,
                    epoch * 10,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }
        store
            .store
            .put(
                &recovery_capsule_path(&capsule),
                PutPayload::from(Bytes::from_static(b"corrupt")),
            )
            .await
            .unwrap();

        let next_epoch = u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 1).unwrap();
        store
            .record_cluster_outcome(
                &proof,
                next_epoch,
                next_epoch * 10,
                fence,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
        let head = store.load_record().await.unwrap().unwrap();
        assert!(head.outcome_floor.is_some());
        assert_eq!(head.outcome_head.unwrap().epoch, next_epoch);
        assert_eq!(store.cluster_outcome(1).await.unwrap().unwrap().epoch, 1);
        assert!(matches!(
            store.cluster_outcome_with_recovery_capsule(1).await,
            Err(ClusterCheckpointAuthorityError::Decision(_))
        ));
        assert!(matches!(
            store
                .prune_cluster_outcomes_before(&proof, 1, accept_recovery_artifacts)
                .await,
            Err(ClusterCheckpointAuthorityError::Decision(_))
        ));
    }

    #[tokio::test]
    async fn obsolete_anchor_capsule_is_not_preflighted_and_is_garbage_collected() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        disable_history_pruning_for_test(&store).await;
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        let obsolete = recovery_capsule(&store, &fence, 1, 10).await;
        let live = recovery_capsule(&store, &fence, 3, 30).await;
        store
            .record_cluster_outcome(
                &proof,
                1,
                10,
                fence.clone(),
                CheckpointVerdict::Commit,
                Some(obsolete.clone()),
            )
            .await
            .unwrap();
        store
            .record_cluster_outcome(
                &proof,
                3,
                30,
                fence.clone(),
                CheckpointVerdict::Commit,
                Some(live),
            )
            .await
            .unwrap();
        store
            .prune_cluster_outcomes_before(&proof, 3, accept_recovery_artifacts)
            .await
            .unwrap();

        let obsolete_path = recovery_capsule_path(&obsolete);
        store
            .store
            .put(
                &obsolete_path,
                PutPayload::from(Bytes::from_static(b"corrupt")),
            )
            .await
            .unwrap();
        let boundary = store
            .audited_cluster_outcome_retention_boundary()
            .await
            .unwrap();
        assert_eq!(boundary.artifact_before_epoch, 3);
        assert_eq!(boundary.committed_anchor.unwrap().epoch, 1);

        let last_epoch = u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 3).unwrap();
        for epoch in 4..=last_epoch {
            store
                .record_cluster_outcome(
                    &proof,
                    epoch,
                    epoch * 10,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }
        let compacted = store
            .audited_cluster_outcome_retention_boundary()
            .await
            .unwrap();
        assert!(compacted.terminal_before_epoch > compacted.artifact_before_epoch);

        let maintenance = store.maintain_cluster_recovery_capsules().await.unwrap();
        assert!(maintenance.quarantined >= 1);
        assert!(matches!(
            store.store.head(&obsolete_path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert_eq!(
            store
                .audited_cluster_outcome_retention_boundary()
                .await
                .unwrap()
                .artifact_before_epoch,
            3
        );
    }

    #[tokio::test]
    async fn concurrent_artifact_floor_mutation_is_preserved_by_history_compaction() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let setup = LeaderLeaseStore::new(Arc::clone(&inner), 1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = setup
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        disable_history_pruning_for_test(&setup).await;
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        let capsule = recovery_capsule(&setup, &fence, 1, 10).await;
        setup
            .record_cluster_outcome(
                &proof,
                1,
                10,
                fence.clone(),
                CheckpointVerdict::Commit,
                Some(capsule.clone()),
            )
            .await
            .unwrap();
        record_commit(&setup, &proof, &fence, 3, 30).await;
        assert_eq!(
            setup
                .prune_cluster_outcomes_before(&proof, 2, accept_recovery_artifacts)
                .await
                .unwrap(),
            2
        );
        for epoch in 4..=u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 2).unwrap() {
            setup
                .record_cluster_outcome(
                    &proof,
                    epoch,
                    epoch * 10,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }

        let (raw, compactor) = blocking_get_once_with_inner(
            1_000,
            inner,
            OsPath::from("checkpoint-deployment/identity.json"),
        );
        compactor.prune_running.store(true, Ordering::Release);
        let compactor = Arc::clone(&compactor);
        let compact_proof = proof.clone();
        let compact_fence = fence.clone();
        let next_epoch = u64::try_from(OUTCOME_HISTORY_COMPACTION_TRIGGER + 3).unwrap();
        let append = tokio::spawn(async move {
            compactor
                .record_cluster_outcome(
                    &compact_proof,
                    next_epoch,
                    next_epoch * 10,
                    compact_fence,
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();

        let artifact_pruner = LeaderLeaseStore::new(raw.clone(), 1_000);
        artifact_pruner.prune_running.store(true, Ordering::Release);
        assert_eq!(
            artifact_pruner
                .prune_cluster_outcomes_before(&proof, 3, accept_recovery_artifacts)
                .await
                .unwrap(),
            3
        );
        raw.release.add_permits(1);
        append.await.unwrap().unwrap();

        let boundary = artifact_pruner
            .audited_cluster_outcome_retention_boundary()
            .await
            .unwrap();
        assert_eq!(boundary.artifact_before_epoch, 3);
        assert!(boundary.terminal_before_epoch > boundary.artifact_before_epoch);
        assert_eq!(boundary.committed_anchor.unwrap().epoch, 1);
        assert!(boundary.terminal_anchor.unwrap().epoch > 3);
    }

    #[tokio::test]
    async fn older_concurrent_outcome_audit_cannot_replace_a_newer_cache_entry() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let setup = Arc::new(LeaderLeaseStore::new(Arc::clone(&inner), 1_000));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = setup
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        for (epoch, checkpoint_id) in [(1, 10), (2, 20)] {
            setup
                .record_cluster_outcome(
                    &proof,
                    epoch,
                    checkpoint_id,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }

        let (raw, store) = blocking_get_once_with_inner(1_000, inner, lease_path(3));
        store.prune_running.store(true, Ordering::Release);
        let old_store = Arc::clone(&store);
        let old_audit = tokio::spawn(async move {
            old_store
                .cluster_attempt_settlement(crate::state::CheckpointAttempt::new(1, 10))
                .await
        });
        raw.entered.acquire().await.unwrap().forget();

        store
            .record_cluster_outcome(&proof, 3, 30, fence, CheckpointVerdict::Abort, None)
            .await
            .unwrap();
        let newest = store
            .highest_cluster_terminal_outcome()
            .await
            .unwrap()
            .unwrap();
        assert_eq!((newest.epoch, newest.checkpoint_id), (3, 30));

        raw.release.add_permits(1);
        let old = old_audit.await.unwrap().unwrap().unwrap();
        assert_eq!((old.epoch, old.checkpoint_id), (1, 10));
        let cache = store.outcome_audit_cache.lock();
        let cached = cache.as_ref().expect("newer audit must remain cached");
        assert_eq!(cached.authority_sequence, 4);
        assert_eq!(
            cached.snapshot.outcomes.last().map(|outcome| outcome.epoch),
            Some(3)
        );
        drop(cache);

        raw.clear_get_counts();
        assert_eq!(
            store
                .highest_cluster_terminal_outcome()
                .await
                .unwrap()
                .unwrap()
                .epoch,
            3
        );
        assert_eq!(raw.get_count(&lease_path(4)), 1);
        assert_eq!(raw.get_count(&lease_path(3)), 0);
    }

    #[tokio::test]
    async fn exact_cluster_outcome_obeys_and_validates_the_durable_floor_anchor() {
        let (raw, store) = blocking_once_at(
            1_000,
            OsPath::from("control/never-block-exact-outcome-floor"),
        );
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        record_commit(store.as_ref(), &proof, &fence, 1, 10).await;
        record_commit(store.as_ref(), &proof, &fence, 5, 50).await;
        store
            .prune_cluster_outcomes_before(&proof, 3, accept_recovery_artifacts)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.prune_running.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        raw.clear_get_counts();
        assert!(store.cluster_outcome(1).await.unwrap().is_none());
        assert_eq!(raw.get_count(&lease_path(4)), 1);
        assert_eq!(raw.get_count(&lease_path(3)), 0);
        assert_eq!(raw.get_count(&lease_path(2)), 0);
        assert_eq!(
            raw.get_count(&OsPath::from("checkpoint-deployment/identity.json")),
            0
        );

        raw.clear_get_counts();
        assert!(store.cluster_outcome(3).await.unwrap().is_none());
        assert_eq!(raw.get_count(&lease_path(4)), 1);
        assert_eq!(raw.get_count(&lease_path(3)), 0);
        assert_eq!(raw.get_count(&lease_path(2)), 0);

        let restarted = LeaderLeaseStore::new(raw.clone(), 1_000);
        raw.clear_get_counts();
        assert!(restarted.cluster_outcome(3).await.unwrap().is_none());
        assert_eq!(raw.get_count(&lease_path(4)), 1);
        assert_eq!(raw.get_count(&lease_path(3)), 1);
        assert_eq!(raw.get_count(&lease_path(2)), 0);

        let mut corrupt = store.load_record().await.unwrap().unwrap();
        corrupt
            .outcome_floor
            .as_mut()
            .unwrap()
            .terminal_anchor_link
            .as_mut()
            .unwrap()
            .sequence += 1;
        store
            .store
            .put(
                &lease_path(corrupt.lease.seq),
                PutPayload::from(Bytes::from(serde_json::to_vec(&corrupt).unwrap())),
            )
            .await
            .unwrap();
        let restarted = LeaderLeaseStore::new(raw.clone(), 1_000);
        assert!(matches!(
            restarted.cluster_outcome(3).await,
            Err(ClusterCheckpointAuthorityError::Authority(
                LeaseError::Invalid(_)
            ))
        ));
    }

    #[tokio::test]
    async fn cluster_decision_rejects_foreign_owner_and_fencing_token() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let fence = assignment_fence(&incumbent);
        let mut wrong_token = first.proof();
        wrong_token.fencing_token += 1;
        assert!(matches!(
            store
                .record_cluster_outcome(
                    &wrong_token,
                    1,
                    10,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await,
            Err(ClusterCheckpointAuthorityError::Fenced)
        ));
        let foreign = LeaderProof {
            owner: owner(2, 2, 1).proof_owner(),
            fencing_token: first.token,
        };
        assert!(matches!(
            store
                .record_cluster_outcome(&foreign, 1, 10, fence, CheckpointVerdict::Abort, None,)
                .await,
            Err(ClusterCheckpointAuthorityError::Fenced)
        ));
        assert!(store.cluster_outcomes().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delayed_catalog_seal_is_fenced_when_takeover_wins_the_sequence() {
        let (raw, store) = blocking_once_at(10, lease_path(2));
        let incumbent = owner(1, 1, 1);
        let successor = owner(2, 2, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let observation = store.observe_rival(&successor, &first).unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;

        let proof = first.proof();
        let manifest = catalog("events");
        let seal_store = Arc::clone(&store);
        let seal_manifest = manifest.clone();
        let seal =
            tokio::spawn(async move { seal_store.seal_catalog(&proof, &seal_manifest).await });
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();

        let LeaseOutcome::Acquired(takeover) = store
            .try_takeover(&successor, &observation, 20)
            .await
            .unwrap()
        else {
            panic!("successor must win the unblocked create-only sequence");
        };
        assert_eq!(takeover.owner, successor);
        assert!(takeover.catalog_manifest.is_none());
        raw.release.add_permits(1);
        assert!(matches!(
            seal.await.unwrap(),
            Err(CatalogManifestError::Fenced)
        ));
        assert!(store
            .load()
            .await
            .unwrap()
            .unwrap()
            .catalog_manifest
            .is_none());
    }

    #[tokio::test]
    async fn delayed_catalog_seal_retries_after_same_term_renewal_wins_the_sequence() {
        let (raw, store) = blocking_once_at(1_000, lease_path(2));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let manifest = catalog("events");
        let seal_store = Arc::clone(&store);
        let seal_manifest = manifest.clone();
        let seal =
            tokio::spawn(async move { seal_store.seal_catalog(&proof, &seal_manifest).await });
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();

        let LeaseOutcome::Acquired(renewal) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 1)
            .await
            .unwrap()
        else {
            panic!("same owner must renew through the unblocked sequence");
        };
        assert_eq!(renewal.seq, 2);
        assert!(renewal.catalog_manifest.is_none());
        raw.release.add_permits(1);
        assert_eq!(seal.await.unwrap().unwrap(), CatalogSealOutcome::Created);
        let sealed = store.load().await.unwrap().unwrap();
        assert_eq!(sealed.seq, 3);
        let reference = sealed
            .catalog_manifest
            .expect("catalog reference must be sealed");
        assert_eq!(
            store.load_catalog_manifest(&reference).await.unwrap(),
            manifest
        );
    }

    #[tokio::test]
    async fn takeover_preserves_a_catalog_sealed_before_it() {
        let store = store(10);
        let incumbent = owner(1, 1, 1);
        let successor = owner(2, 2, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let manifest = catalog("events");
        assert_eq!(
            store.seal_catalog(&first.proof(), &manifest).await.unwrap(),
            CatalogSealOutcome::Created
        );
        let sealed = store.load().await.unwrap().unwrap();
        let sealed_reference = sealed
            .catalog_manifest
            .clone()
            .expect("catalog reference must be sealed");
        let observation = store.observe_rival(&successor, &sealed).unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        let LeaseOutcome::Acquired(takeover) = store
            .try_takeover(&successor, &observation, 20)
            .await
            .unwrap()
        else {
            panic!("successor must acquire after a full observation");
        };
        assert_eq!(takeover.catalog_manifest, Some(sealed_reference.clone()));
        assert_eq!(
            store
                .load_catalog_manifest(&sealed_reference)
                .await
                .unwrap(),
            manifest
        );
    }

    async fn assert_invalid_selected_cut_blocks_prune(corrupt: bool) {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            panic!("empty authority must be acquired");
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        let first_capsule = recovery_capsule(&store, &fence, 1, 10).await;
        let selected_capsule = recovery_capsule(&store, &fence, 2, 20).await;
        for (epoch, checkpoint_id, capsule) in [
            (1, 10, first_capsule.clone()),
            (2, 20, selected_capsule.clone()),
        ] {
            assert!(matches!(
                store
                    .record_cluster_outcome(
                        &proof,
                        epoch,
                        checkpoint_id,
                        fence.clone(),
                        CheckpointVerdict::Commit,
                        Some(capsule),
                    )
                    .await
                    .unwrap(),
                RecordOutcomeResult::Created(_)
            ));
        }
        let old_orphan = recovery_capsule(&store, &fence, 1, 11).await;
        let old_orphan_path = recovery_capsule_path(&old_orphan);
        let selected_path = recovery_capsule_path(&selected_capsule);
        if corrupt {
            store
                .store
                .put(
                    &selected_path,
                    PutPayload::from(Bytes::from_static(b"corrupt")),
                )
                .await
                .unwrap();
        } else {
            store.store.delete(&selected_path).await.unwrap();
        }

        assert!(matches!(
            store
                .prune_cluster_outcomes_before(&proof, 2, accept_recovery_artifacts)
                .await,
            Err(ClusterCheckpointAuthorityError::Decision(
                DecisionError::Conflict(_)
            ))
        ));
        assert_eq!(
            store
                .cluster_outcome_retention_boundary()
                .await
                .unwrap()
                .artifact_before_epoch,
            0
        );
        store
            .store
            .head(&old_orphan_path)
            .await
            .expect("failed cut validation must prevent orphan pruning");
        store
            .store
            .head(&recovery_capsule_path(&first_capsule))
            .await
            .expect("failed cut validation must prevent authority-history pruning");
    }

    #[tokio::test]
    async fn missing_selected_live_cut_prevents_floor_advance_and_prune() {
        assert_invalid_selected_cut_blocks_prune(false).await;
    }

    #[tokio::test]
    async fn corrupt_selected_live_cut_prevents_floor_advance_and_prune() {
        assert_invalid_selected_cut_blocks_prune(true).await;
    }

    #[tokio::test]
    async fn failed_recovery_metadata_preflight_does_not_publish_a_new_floor() {
        let (store, _incumbent, proof) = retention_test_store(1_000).await;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed_calls = Arc::clone(&calls);

        let error = store
            .prune_cluster_outcomes_before(&proof, 3, move |_| {
                let calls = Arc::clone(&observed_calls);
                async move {
                    calls.fetch_add(1, Ordering::AcqRel);
                    Err("selected state replica is unreadable".to_owned())
                }
            })
            .await
            .expect_err("artifact failure must block a new durable floor");

        assert!(error
            .to_string()
            .contains("durable recovery metadata preflight"));
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert_eq!(
            store
                .cluster_outcome_retention_boundary()
                .await
                .unwrap()
                .artifact_before_epoch,
            0
        );
    }

    #[tokio::test]
    async fn covered_retention_horizon_does_not_repeat_artifact_preflight() {
        let (store, _incumbent, proof) = retention_test_store(1_000).await;
        assert_eq!(
            store
                .prune_cluster_outcomes_before(&proof, 3, accept_recovery_artifacts)
                .await
                .unwrap(),
            3
        );
        let sequence = store.load().await.unwrap().unwrap().seq;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed_calls = Arc::clone(&calls);

        assert_eq!(
            store
                .prune_cluster_outcomes_before(&proof, 2, move |_| {
                    let calls = Arc::clone(&observed_calls);
                    async move {
                        calls.fetch_add(1, Ordering::AcqRel);
                        Err("covered horizon must not run this callback".to_owned())
                    }
                })
                .await
                .unwrap(),
            3
        );
        assert_eq!(calls.load(Ordering::Acquire), 0);
        assert_eq!(store.load().await.unwrap().unwrap().seq, sequence);
    }

    #[tokio::test]
    async fn renewal_during_artifact_preflight_preserves_new_floor_authorization() {
        let (store, incumbent, proof) = retention_test_store(1_000).await;
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let pruning = {
            let store = Arc::clone(&store);
            let proof = proof.clone();
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move {
                store
                    .prune_cluster_outcomes_before(&proof, 3, move |_| {
                        let entered = Arc::clone(&entered);
                        let release = Arc::clone(&release);
                        let calls = Arc::clone(&calls);
                        async move {
                            if calls.fetch_add(1, Ordering::AcqRel) == 0 {
                                entered.add_permits(1);
                                release.acquire().await.unwrap().forget();
                            }
                            Ok(())
                        }
                    })
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();

        assert!(matches!(
            store
                .acquire_or_renew_current_term_for_test(&incumbent, 1)
                .await
                .unwrap(),
            LeaseOutcome::Acquired(_)
        ));
        release.add_permits(1);

        assert_eq!(pruning.await.unwrap().unwrap(), 3);
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert_eq!(
            store
                .cluster_outcome_retention_boundary()
                .await
                .unwrap()
                .artifact_before_epoch,
            3
        );
    }

    #[tokio::test]
    async fn changed_outcome_head_during_preflight_restarts_new_floor_authorization() {
        let (store, incumbent, proof) = retention_test_store(1_000).await;
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let pruning = {
            let store = Arc::clone(&store);
            let proof = proof.clone();
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move {
                store
                    .prune_cluster_outcomes_before(&proof, 3, move |_| {
                        let entered = Arc::clone(&entered);
                        let release = Arc::clone(&release);
                        let calls = Arc::clone(&calls);
                        async move {
                            if calls.fetch_add(1, Ordering::AcqRel) == 0 {
                                entered.add_permits(1);
                                release.acquire().await.unwrap().forget();
                            }
                            Ok(())
                        }
                    })
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();

        store
            .record_cluster_outcome(
                &proof,
                4,
                40,
                assignment_fence(&incumbent),
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
        release.add_permits(1);

        assert_eq!(pruning.await.unwrap().unwrap(), 3);
        assert_eq!(calls.load(Ordering::Acquire), 2);
        assert_eq!(
            store
                .cluster_outcome_retention_boundary()
                .await
                .unwrap()
                .artifact_before_epoch,
            3
        );
    }

    #[tokio::test]
    async fn takeover_during_artifact_preflight_fences_new_floor_publication() {
        let (store, _incumbent, proof) = retention_test_store(10).await;
        let successor = owner(2, 2, 1);
        let current = store.load().await.unwrap().unwrap();
        let observation = store.observe_rival(&successor, &current).unwrap();
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let pruning = {
            let store = Arc::clone(&store);
            let proof = proof.clone();
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                store
                    .prune_cluster_outcomes_before(&proof, 3, move |_| {
                        let entered = Arc::clone(&entered);
                        let release = Arc::clone(&release);
                        async move {
                            entered.add_permits(1);
                            release.acquire().await.unwrap().forget();
                            Ok(())
                        }
                    })
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();

        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(matches!(
            store
                .try_takeover(&successor, &observation, 20)
                .await
                .unwrap(),
            LeaseOutcome::Acquired(_)
        ));
        release.add_permits(1);

        assert!(matches!(
            pruning.await.unwrap(),
            Err(ClusterCheckpointAuthorityError::Fenced)
        ));
        assert_eq!(
            store
                .cluster_outcome_retention_boundary()
                .await
                .unwrap()
                .artifact_before_epoch,
            0
        );
    }

    #[tokio::test]
    async fn ambiguous_floor_create_revalidates_the_winner_cut() {
        let (raw, store) = ambiguous_once_at(1_000, lease_path(4));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            panic!("empty authority must be acquired");
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        let first_capsule = recovery_capsule(store.as_ref(), &fence, 1, 10).await;
        let selected_capsule = recovery_capsule(store.as_ref(), &fence, 2, 20).await;
        for (epoch, checkpoint_id, capsule) in [
            (1, 10, first_capsule.clone()),
            (2, 20, selected_capsule.clone()),
        ] {
            assert!(matches!(
                store
                    .record_cluster_outcome(
                        &proof,
                        epoch,
                        checkpoint_id,
                        fence.clone(),
                        CheckpointVerdict::Commit,
                        Some(capsule),
                    )
                    .await
                    .unwrap(),
                RecordOutcomeResult::Created(_)
            ));
        }

        raw.clear_get_counts();
        assert_eq!(
            store
                .prune_cluster_outcomes_before(&proof, 2, accept_recovery_artifacts)
                .await
                .unwrap(),
            2
        );
        assert!(raw
            .did_return_ambiguous
            .load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(raw.get_count(&recovery_capsule_path(&first_capsule)), 0);
        assert_eq!(raw.get_count(&recovery_capsule_path(&selected_capsule)), 2);
    }

    #[tokio::test]
    async fn capsule_cleanup_is_bounded_retryable_and_independent_of_floor_publication() {
        let (raw, store) =
            blocking_once_at(1_000, OsPath::from("control/never-block-capsule-sweep"));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            panic!("empty authority must be acquired");
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);

        let first_capsule = recovery_capsule(store.as_ref(), &fence, 1, 10).await;
        let second_capsule = recovery_capsule(store.as_ref(), &fence, 2, 20).await;
        let third_capsule = recovery_capsule(store.as_ref(), &fence, 3, 30).await;
        for (epoch, checkpoint_id, capsule) in [
            (1, 10, first_capsule.clone()),
            (2, 20, second_capsule.clone()),
            (3, 30, third_capsule.clone()),
        ] {
            assert!(matches!(
                store
                    .record_cluster_outcome(
                        &proof,
                        epoch,
                        checkpoint_id,
                        fence.clone(),
                        CheckpointVerdict::Commit,
                        Some(capsule),
                    )
                    .await
                    .unwrap(),
                RecordOutcomeResult::Created(_)
            ));
        }

        let old_orphan = recovery_capsule(store.as_ref(), &fence, 1, 11).await;
        let deletable_old_orphan = recovery_capsule(store.as_ref(), &fence, 1, 12).await;
        let another_old_orphan = recovery_capsule(store.as_ref(), &fence, 1, 14).await;
        let corrupt_old_orphan = recovery_capsule(store.as_ref(), &fence, 1, 13).await;
        let at_floor_unpublished = recovery_capsule(store.as_ref(), &fence, 2, 21).await;
        let above_floor_unpublished = recovery_capsule(store.as_ref(), &fence, 4, 41).await;
        let old_orphan_path = recovery_capsule_path(&old_orphan);
        let deletable_old_orphan_path = recovery_capsule_path(&deletable_old_orphan);
        let another_old_orphan_path = recovery_capsule_path(&another_old_orphan);
        let corrupt_old_orphan_path = recovery_capsule_path(&corrupt_old_orphan);
        let at_floor_path = recovery_capsule_path(&at_floor_unpublished);
        let above_floor_path = recovery_capsule_path(&above_floor_unpublished);
        let malformed_path =
            OsPath::from("checkpoint-recovery-capsules/epoch=00000000000000000001/malformed-junk");
        let known_paths = [
            recovery_capsule_path(&first_capsule),
            recovery_capsule_path(&second_capsule),
            recovery_capsule_path(&third_capsule),
        ];
        raw.inner
            .put(
                &corrupt_old_orphan_path,
                PutPayload::from(Bytes::from_static(b"corrupt")),
            )
            .await
            .unwrap();
        raw.inner
            .put(
                &malformed_path,
                PutPayload::from(Bytes::from_static(b"junk")),
            )
            .await
            .unwrap();

        raw.clear_get_counts();
        raw.fail_next_delete(old_orphan_path.clone());
        raw.begin_capsule_get_concurrency_probe();
        assert_eq!(
            store
                .prune_cluster_outcomes_before(&proof, 2, accept_recovery_artifacts)
                .await
                .unwrap(),
            2
        );
        raw.inner
            .head(&old_orphan_path)
            .await
            .expect("floor publication must not perform capsule cleanup inline");
        let first_step = store.maintain_cluster_recovery_capsules().await.unwrap();
        assert!(first_step.pending, "failed delete must remain retryable");
        assert!(raw.finish_capsule_get_concurrency_probe() <= 4);

        assert_eq!(raw.get_count(&known_paths[0]), 1);
        assert_eq!(raw.get_count(&known_paths[1]), 0);
        assert_eq!(
            raw.get_count(&known_paths[2]),
            1,
            "the highest retained commit capsule must be fully validated"
        );
        assert_eq!(raw.get_count(&old_orphan_path), 1);
        assert_eq!(raw.get_count(&deletable_old_orphan_path), 1);
        assert_eq!(raw.get_count(&another_old_orphan_path), 1);
        assert!(raw.get_count(&corrupt_old_orphan_path) >= 1);
        assert_eq!(raw.get_count(&at_floor_path), 0);
        assert_eq!(raw.get_count(&above_floor_path), 0);
        assert_eq!(raw.get_count(&malformed_path), 0);
        assert!(matches!(
            raw.inner.head(&known_paths[0]).await,
            Err(object_store::Error::NotFound { .. })
        ));
        raw.inner
            .head(&old_orphan_path)
            .await
            .expect("a failed best-effort delete remains retryable");
        assert!(matches!(
            raw.inner.head(&deletable_old_orphan_path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(matches!(
            raw.inner.head(&another_old_orphan_path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(matches!(
            raw.inner.head(&corrupt_old_orphan_path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(matches!(
            raw.inner.head(&malformed_path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        raw.inner
            .head(&at_floor_path)
            .await
            .expect("an unpublished capsule at the floor must be retained");
        raw.inner
            .head(&above_floor_path)
            .await
            .expect("an unpublished capsule above the floor must be retained");

        raw.clear_get_counts();
        let retry = store.maintain_cluster_recovery_capsules().await.unwrap();
        assert!(retry.pending);
        assert_eq!(raw.get_count(&old_orphan_path), 1);
        assert_eq!(raw.get_count(&deletable_old_orphan_path), 0);
        assert_eq!(raw.get_count(&another_old_orphan_path), 0);
        assert_eq!(raw.get_count(&corrupt_old_orphan_path), 0);
        assert_eq!(raw.get_count(&at_floor_path), 0);
        assert_eq!(raw.get_count(&above_floor_path), 0);
        assert_eq!(raw.get_count(&malformed_path), 0);
        assert!(matches!(
            raw.inner.head(&old_orphan_path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        raw.inner
            .head(&at_floor_path)
            .await
            .expect("an unpublished capsule at the floor must survive retries");
        raw.inner
            .head(&above_floor_path)
            .await
            .expect("an unpublished capsule above the floor must survive retries");

        raw.clear_get_counts();
        assert!(
            store
                .maintain_cluster_recovery_capsules()
                .await
                .unwrap()
                .pending
        );
        assert_eq!(raw.get_count(&malformed_path), 0);
        assert_eq!(raw.get_count(&corrupt_old_orphan_path), 0);
    }

    #[tokio::test]
    async fn renewal_catalog_seal_and_takeover_preserve_outcome_head_and_floor() {
        let store = store(10);
        let incumbent = owner(1, 1, 1);
        let successor = owner(2, 2, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        let decisions = CheckpointDecisionStore::new(Arc::clone(&store.store));
        let first_capsule = recovery_capsule(&store, &fence, 1, 10).await;
        let second_capsule = recovery_capsule(&store, &fence, 2, 20).await;
        let third_capsule = recovery_capsule(&store, &fence, 3, 30).await;
        for (epoch, checkpoint_id, capsule) in [
            (1, 10, first_capsule.clone()),
            (2, 20, second_capsule.clone()),
            (3, 30, third_capsule.clone()),
        ] {
            assert!(matches!(
                store
                    .record_cluster_outcome(
                        &proof,
                        epoch,
                        checkpoint_id,
                        fence.clone(),
                        CheckpointVerdict::Commit,
                        Some(capsule),
                    )
                    .await
                    .unwrap(),
                RecordOutcomeResult::Created(_)
            ));
        }
        assert_eq!(
            store
                .prune_cluster_outcomes_before(&proof, 3, accept_recovery_artifacts)
                .await
                .unwrap(),
            3
        );
        store
            .seal_catalog(&proof, &catalog("events"))
            .await
            .unwrap();
        let LeaseOutcome::Acquired(renewed) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 1)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let observation = store.observe_rival(&successor, &renewed).unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        let LeaseOutcome::Acquired(takeover) = store
            .try_takeover(&successor, &observation, 20)
            .await
            .unwrap()
        else {
            panic!("successor must acquire after a full observation");
        };

        assert_eq!(
            store
                .cluster_outcomes()
                .await
                .unwrap()
                .into_iter()
                .map(|outcome| (outcome.epoch, outcome.checkpoint_id))
                .collect::<Vec<_>>(),
            vec![(3, 30)]
        );
        let boundary = store.cluster_outcome_retention_boundary().await.unwrap();
        assert_eq!(boundary.artifact_before_epoch, 3);
        let committed_anchor = boundary.committed_anchor.unwrap();
        assert_eq!(
            (committed_anchor.epoch, committed_anchor.checkpoint_id),
            (2, 20)
        );
        assert_eq!(committed_anchor.leader_proof.as_ref(), Some(&proof));
        assert_eq!(boundary.terminal_anchor, Some(committed_anchor));
        assert!(matches!(
            store
                .record_cluster_outcome(&proof, 4, 40, fence, CheckpointVerdict::Abort, None,)
                .await,
            Err(ClusterCheckpointAuthorityError::Fenced)
        ));
        assert_eq!(takeover.token, first.token + 1);
        decisions
            .load_recovery_capsule(&first_capsule)
            .await
            .unwrap();
        let maintenance = store.maintain_cluster_recovery_capsules().await.unwrap();
        assert_eq!(maintenance.deleted, 2);
        assert_eq!(maintenance.quarantined, 0);
        assert!(maintenance.pending);
        assert!(decisions
            .load_recovery_capsule(&first_capsule)
            .await
            .is_err());
        assert!(decisions
            .load_recovery_capsule(&second_capsule)
            .await
            .is_err());
        decisions
            .load_recovery_capsule(&third_capsule)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn history_prune_keeps_live_outcome_chain_and_drops_only_compacted_records() {
        let store = store(1);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        for epoch in 1..=4 {
            record_commit(&store, &proof, &fence, epoch, epoch * 10).await;
        }
        let head = store.load_record().await.unwrap().unwrap();
        let mut by_epoch = std::collections::BTreeMap::new();
        let mut link = head.outcome_head;
        while let Some(current) = link {
            by_epoch.insert(current.epoch, current.sequence);
            link = read_authority_record(store.store.as_ref(), current.sequence)
                .await
                .unwrap()
                .unwrap()
                .previous_outcome;
        }
        assert_eq!(
            store
                .prune_cluster_outcomes_before(&proof, 3, accept_recovery_artifacts)
                .await
                .unwrap(),
            3
        );
        tokio::time::sleep(Duration::from_millis(5)).await;

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let _ = store
                    .acquire_or_renew_current_term_for_test(&incumbent, 10)
                    .await
                    .unwrap();
                let compacted_absent =
                    read_authority_record(store.store.as_ref(), *by_epoch.get(&1).unwrap())
                        .await
                        .unwrap()
                        .is_none()
                        && read_authority_record(store.store.as_ref(), *by_epoch.get(&2).unwrap())
                            .await
                            .unwrap()
                            .is_none();
                if compacted_absent {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();

        for epoch in [3, 4] {
            assert!(
                read_authority_record(store.store.as_ref(), *by_epoch.get(&epoch).unwrap())
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        assert_eq!(
            store
                .cluster_outcomes()
                .await
                .unwrap()
                .into_iter()
                .map(|outcome| outcome.epoch)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
        assert_eq!(
            store
                .highest_cluster_terminal_outcome()
                .await
                .unwrap()
                .unwrap()
                .epoch,
            4
        );
    }

    #[tokio::test]
    async fn floor_rejects_an_older_commit_anchor_with_a_nonolder_authority_sequence() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&incumbent, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        record_commit(&store, &proof, &fence, 1, 10).await;
        store
            .record_cluster_outcome(&proof, 2, 20, fence.clone(), CheckpointVerdict::Abort, None)
            .await
            .unwrap();
        record_commit(&store, &proof, &fence, 3, 30).await;
        store
            .prune_cluster_outcomes_before(&proof, 3, accept_recovery_artifacts)
            .await
            .unwrap();

        let mut corrupt = store.load_record().await.unwrap().unwrap();
        corrupt
            .outcome_floor
            .as_mut()
            .unwrap()
            .committed_anchor_link
            .as_mut()
            .unwrap()
            .sequence += 1;
        store
            .store
            .put(
                &lease_path(corrupt.lease.seq),
                PutPayload::from(Bytes::from(serde_json::to_vec(&corrupt).unwrap())),
            )
            .await
            .unwrap();
        assert!(matches!(
            store.cluster_outcomes().await,
            Err(ClusterCheckpointAuthorityError::Authority(
                LeaseError::Invalid(_)
            ))
        ));
        assert!(matches!(
            store.audited_cluster_outcome_retention_boundary().await,
            Err(ClusterCheckpointAuthorityError::Authority(
                LeaseError::Invalid(_)
            ))
        ));
    }

    #[tokio::test]
    async fn renewals_copy_only_the_bounded_catalog_reference() {
        let store = store(1_000);
        let owner = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&owner, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let manifest = CatalogManifest::new(vec![super::super::CatalogManifestEntry {
            canonical_name: "events".into(),
            kind: crate::catalog::CatalogObjectKind::Source,
            ddl: format!(
                "CREATE SOURCE events WITH ('description' = '{}')",
                "x".repeat(100_000)
            ),
        }])
        .unwrap();
        store.seal_catalog(&first.proof(), &manifest).await.unwrap();

        let LeaseOutcome::Acquired(renewed) = store
            .acquire_or_renew_current_term_for_test(&owner, 1)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let reference = renewed
            .catalog_manifest
            .clone()
            .expect("renewal must retain the catalog reference");
        assert!(serde_json::to_vec(&renewed).unwrap().len() < 512);
        assert_eq!(
            store.load_catalog_manifest(&reference).await.unwrap(),
            manifest
        );
    }

    #[tokio::test]
    async fn preexisting_manifest_blob_must_match_exact_content() {
        let store = store(1_000);
        let owner = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store
            .acquire_or_renew_current_term_for_test(&owner, 0)
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let manifest = catalog("events");
        let (_, reference) = manifest.encode_and_reference().unwrap();
        store
            .store
            .put(
                &reference.object_path(),
                PutPayload::from(Bytes::from_static(b"corrupt")),
            )
            .await
            .unwrap();

        assert!(matches!(
            store.seal_catalog(&first.proof(), &manifest).await,
            Err(CatalogManifestError::Invalid(_))
        ));
        assert!(store
            .load()
            .await
            .unwrap()
            .unwrap()
            .catalog_manifest
            .is_none());
    }

    #[cfg(feature = "cluster")]
    fn blocking_store(ttl_ms: i64) -> (Arc<BlockingStore>, Arc<LeaderLeaseStore>) {
        blocking_store_at(ttl_ms, lease_path(2))
    }

    #[cfg(feature = "cluster")]
    async fn wait_for_lease(lease: &mut watch::Receiver<Option<LeaderLease>>) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while lease.borrow_and_update().is_none() {
                lease.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
    }

    #[cfg(feature = "cluster")]
    fn candidacy_channel(
        eligible: bool,
    ) -> (
        watch::Sender<LeaderCandidacy>,
        watch::Receiver<LeaderCandidacy>,
    ) {
        watch::channel(LeaderCandidacy::initial(eligible))
    }

    #[cfg(feature = "cluster")]
    fn set_candidacy(candidate: &watch::Sender<LeaderCandidacy>, eligible: bool) {
        candidate.send_modify(|current| {
            *current = current
                .transition(eligible)
                .expect("leader candidacy generation");
        });
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn delayed_durable_acquisition_response_fails_closed_at_attempt_deadline() {
        let ttl = Duration::from_millis(40);
        let (raw, store) = delayed_response_once_at(40, lease_path(1));
        let owner = owner(1, 1, 1);
        let manager = LeaderLeaseManager::new(
            Arc::clone(&store),
            &process(&owner),
            LeaderLeaseConfig {
                ttl,
                renew_interval: Duration::from_millis(5),
            },
        )
        .unwrap();
        let deadline = manager.deadline();
        let lease = manager.lease_watch();
        let (_candidate_tx, candidate_rx) = candidacy_channel(true);
        let task = manager.spawn(tokio_util::sync::CancellationToken::new(), candidate_rx);

        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        assert!(matches!(
            store.load().await.unwrap(),
            Some(LeaderLease { owner: current, .. }) if current == owner
        ));
        tokio::time::timeout(ttl + Duration::from_millis(100), task)
            .await
            .expect("manager must not wait beyond the attempt's anchored TTL")
            .unwrap();

        assert!(lease.borrow().is_none());
        assert!(!deadline.is_live());
        raw.release.add_permits(1);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn candidacy_loss_interrupts_hung_renewal_and_withdraws_the_grant() {
        let (raw, store) = blocking_store(80);
        let owner = owner(1, 1, 1);
        let manager = LeaderLeaseManager::new(
            Arc::clone(&store),
            &process(&owner),
            LeaderLeaseConfig {
                ttl: Duration::from_millis(80),
                renew_interval: Duration::from_millis(10),
            },
        )
        .unwrap();
        let deadline = manager.deadline();
        let mut lease = manager.lease_watch();
        let (candidate_tx, candidate_rx) = candidacy_channel(true);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let task = manager.spawn(shutdown.clone(), candidate_rx);
        wait_for_lease(&mut lease).await;
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        set_candidacy(&candidate_tx, false);
        tokio::time::timeout(Duration::from_millis(40), lease.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(lease.borrow().is_none());
        assert!(!deadline.is_live());
        raw.release.add_permits(1);
        shutdown.cancel();
        task.await.unwrap();
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn candidacy_reacquisition_rotates_the_durable_fencing_token() {
        let owner = owner(1, 1, 1);
        let store = Arc::new(store(500));
        let manager = LeaderLeaseManager::new(
            Arc::clone(&store),
            &process(&owner),
            LeaderLeaseConfig {
                ttl: Duration::from_millis(500),
                renew_interval: Duration::from_millis(20),
            },
        )
        .unwrap();
        let deadline = manager.deadline();
        let mut lease = manager.lease_watch();
        let (candidate_tx, candidate_rx) = candidacy_channel(true);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let task = manager.spawn(shutdown.clone(), candidate_rx);

        wait_for_lease(&mut lease).await;
        let first = lease.borrow().clone().expect("initial leader grant");
        let stale_proof = first.proof();
        set_candidacy(&candidate_tx, false);
        tokio::time::timeout(Duration::from_secs(1), async {
            while lease.borrow_and_update().is_some() {
                lease.changed().await.unwrap();
            }
        })
        .await
        .expect("candidacy loss did not withdraw the local grant");
        assert!(!lease_grants_proof(
            &lease.borrow().clone(),
            &owner,
            &deadline,
            &stale_proof,
        ));

        set_candidacy(&candidate_tx, true);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if lease
                    .borrow_and_update()
                    .as_ref()
                    .is_some_and(|current| current.token > first.token)
                {
                    break;
                }
                lease.changed().await.unwrap();
            }
        })
        .await
        .expect("candidacy reacquisition did not publish a new fencing token");
        let reacquired = lease.borrow().clone().expect("reacquired leader grant");
        assert!(reacquired.token > first.token);
        assert_eq!(
            store
                .load()
                .await
                .unwrap()
                .expect("durable leader grant")
                .token,
            reacquired.token
        );
        assert!(!lease_grants_proof(
            &Some(reacquired),
            &owner,
            &deadline,
            &stale_proof,
        ));

        shutdown.cancel();
        task.await.unwrap();
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "current_thread")]
    async fn coalesced_candidacy_loss_still_rotates_the_fencing_token() {
        let owner = owner(1, 2, 1);
        let store = Arc::new(store(500));
        let manager = LeaderLeaseManager::new(
            Arc::clone(&store),
            &process(&owner),
            LeaderLeaseConfig {
                ttl: Duration::from_millis(500),
                renew_interval: Duration::from_millis(20),
            },
        )
        .unwrap();
        let deadline = manager.deadline();
        let mut lease = manager.lease_watch();
        let (candidate_tx, candidate_rx) = candidacy_channel(true);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let task = manager.spawn(shutdown.clone(), candidate_rx);
        wait_for_lease(&mut lease).await;
        let first = lease.borrow().clone().expect("initial leader grant");
        let stale_proof = first.proof();

        // No await between these updates: the receiver observes only the final eligible value.
        set_candidacy(&candidate_tx, false);
        set_candidacy(&candidate_tx, true);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if lease
                    .borrow_and_update()
                    .as_ref()
                    .is_some_and(|current| current.token > first.token)
                {
                    break;
                }
                lease.changed().await.unwrap();
            }
        })
        .await
        .expect("coalesced candidacy loss reused the old fencing token");
        let current = lease.borrow().clone().expect("rotated leader grant");
        assert!(!lease_grants_proof(
            &Some(current),
            &owner,
            &deadline,
            &stale_proof,
        ));

        shutdown.cancel();
        task.await.unwrap();
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn hung_renewal_fences_at_local_deadline() {
        let (raw, store) = blocking_store(40);
        let owner = owner(1, 1, 1);
        let manager = LeaderLeaseManager::new(
            store,
            &process(&owner),
            LeaderLeaseConfig {
                ttl: Duration::from_millis(40),
                renew_interval: Duration::from_millis(5),
            },
        )
        .unwrap();
        let deadline = manager.deadline();
        let mut lease = manager.lease_watch();
        let (_candidate_tx, candidate_rx) = candidacy_channel(true);
        let task = manager.spawn(tokio_util::sync::CancellationToken::new(), candidate_rx);
        wait_for_lease(&mut lease).await;
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        tokio::time::timeout(Duration::from_millis(150), task)
            .await
            .unwrap()
            .unwrap();
        assert!(lease.borrow().is_none());
        assert!(!deadline.is_live());
        raw.release.add_permits(1);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn shutdown_clears_published_grant_and_fences() {
        let config = LeaderLeaseConfig {
            ttl: Duration::from_millis(100),
            renew_interval: Duration::from_millis(20),
        };
        let owner = owner(1, 1, 1);
        let manager =
            LeaderLeaseManager::new(Arc::new(store(100)), &process(&owner), config).unwrap();
        let deadline = manager.deadline();
        let mut lease = manager.lease_watch();
        let (_candidate_tx, candidate_rx) = candidacy_channel(true);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let task = manager.spawn(shutdown.clone(), candidate_rx);
        wait_for_lease(&mut lease).await;
        shutdown.cancel();
        tokio::time::timeout(Duration::from_millis(50), task)
            .await
            .unwrap()
            .unwrap();
        assert!(lease.borrow().is_none());
        assert!(!deadline.is_live());
    }

    #[test]
    fn grant_requires_exact_owner_and_live_deadline() {
        let expected = owner(1, 1, 1);
        let lease = Some(LeaderLease {
            seq: 1,
            renewal_sequence: 1,
            token: 1,
            owner: expected.clone(),
            expires_at_ms: i64::MIN,
            catalog_manifest: None,
        });
        let deadline = LeaseDeadline::live_for(Duration::from_secs(1));
        assert!(lease_grants_leadership(&lease, &expected, &deadline));
        assert!(!lease_grants_leadership(&lease, &owner(1, 2, 2), &deadline));
        deadline.fence();
        assert!(!lease_grants_leadership(&lease, &expected, &deadline));
    }
}
