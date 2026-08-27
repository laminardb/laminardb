//! Durable, append-only leader fencing.

mod artifact_admission;
mod subscription_replay;

pub use subscription_replay::{
    SubscriptionReplayPin, SubscriptionReplayPinAcquire, SUBSCRIPTION_REPLAY_PIN_RENEW_INTERVAL,
};

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
    probe_object_store_conditional_update, AssignmentDrainId, AssignmentDrainTransition,
    CheckpointAssignmentFence, CheckpointStoreError, CommittedCheckpointIndex,
    CommittedCheckpointRef, LeaderProof, LeaderProofOwner, MAX_CHECKPOINT_PARTICIPANTS,
};
use crate::checkpoint_decision::{
    CheckpointArtifactInventory, CheckpointDecisionStore, CheckpointOutcome, CheckpointScope,
    CheckpointVerdict, DecisionError, RecordOutcomeResult,
};
use crate::cluster::discovery::NodeId;

use super::catalog_manifest::{
    CatalogManifest, CatalogManifestError, CatalogManifestRef, CatalogSealOutcome,
};
use super::controller::{
    RecoverPhase, RecoveryAdmissionSnapshot, RecoveryAnnouncement, RecoveryFault,
    RecoveryFaultDisposition, RecoveryFaultInventory, RecoveryFaultPublisher, RecoveryReleaseId,
    MAX_RECOVERY_ANNOUNCEMENT_BYTES,
};
use super::lease_deadline::LeaseDeadline;
use super::process_lease::{ProcessLease, ProcessLeaseFence};
use super::snapshot::{
    AssignmentSnapshotRef, AssignmentSnapshotStore, RotateOutcome, SnapshotError,
};

const LEASE_PREFIX: &str = "control/leader-lease/";
const AUTHORITY_HEAD_PATH: &str = "control/leader-lease-head/v1.json";
const STORE_CONTRACT_PROBE_PREFIX: &str = "control/object-store-contract-probes/v1/";
const RECOVERY_RELEASE_TERMINAL_PREFIX: &str = "control/recovery-release-terminals/v2/";
const AUTHORITY_RECORD_VERSION: u32 = 12;
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

impl OutcomeLink {
    fn validate(self) -> Result<(), LeaseError> {
        let attempt = crate::checkpoint::CheckpointAttempt::new(self.epoch, self.checkpoint_id);
        if self.sequence == 0 || !attempt.is_canonical() {
            return Err(LeaseError::Invalid(
                "checkpoint outcome link requires a nonzero authority sequence and one canonical checkpoint ID"
                    .into(),
            ));
        }
        Ok(())
    }
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
    #[serde(
        default,
        skip_serializing_if = "RecoveryFaultDisposition::is_recoverable"
    )]
    disposition: RecoveryFaultDisposition,
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
        if self.disposition == RecoveryFaultDisposition::Terminal && !self.active {
            return Err(LeaseError::Invalid(
                "terminal recovery fault authority cannot be tombstoned".into(),
            ));
        }
        Ok(())
    }

    fn fault(&self) -> RecoveryFault {
        RecoveryFault {
            reporter: NodeId(self.publisher.participant.node_id),
            sequence: self.fault_sequence,
            disposition: self.disposition,
        }
    }

    fn matches_request(
        &self,
        publisher: RecoveryFaultPublisher,
        request_sequence: u64,
        disposition: RecoveryFaultDisposition,
    ) -> bool {
        self.publisher == publisher
            && self.request_sequence == request_sequence
            && self.disposition == disposition
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
    TerminalFenceActive,
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
    /// Exact predecessor-fenced checkpoint used to hand moved vnodes to the target assignment.
    /// Present if and only if the drain commits.
    pub handoff_checkpoint: Option<CommittedCheckpointRef>,
}

impl AssignmentDrainDecision {
    /// Commit an exact canonical transition against its state handoff cut.
    ///
    /// # Errors
    /// Rejects malformed authority or checkpoint references.
    pub fn commit(
        transition: &AssignmentDrainTransition,
        leader_proof: LeaderProof,
        handoff_checkpoint: CommittedCheckpointRef,
    ) -> Result<Self, String> {
        handoff_checkpoint.validate()?;
        if !transition.is_canonical() || leader_proof != transition.leader {
            return Err("assignment drain decision requires a canonical transition".into());
        }
        Ok(Self {
            transition: transition.clone(),
            leader_proof,
            verdict: AssignmentDrainVerdict::Commit,
            handoff_checkpoint: Some(handoff_checkpoint),
        })
    }

    /// Abort an exact canonical transition under the deciding leader term.
    ///
    /// # Errors
    /// Rejects malformed transition or leader authority.
    pub fn abort(
        transition: &AssignmentDrainTransition,
        leader_proof: LeaderProof,
    ) -> Result<Self, String> {
        if !transition.is_canonical() || !leader_proof.is_canonical() {
            return Err("assignment drain decision requires a canonical transition".into());
        }
        Ok(Self {
            transition: transition.clone(),
            leader_proof,
            verdict: AssignmentDrainVerdict::Abort,
            handoff_checkpoint: None,
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
        match (self.verdict, self.handoff_checkpoint.as_ref()) {
            (AssignmentDrainVerdict::Commit, Some(reference)) => {
                reference.validate().map_err(LeaseError::Invalid)?;
            }
            (AssignmentDrainVerdict::Abort, None) => {}
            _ => {
                return Err(LeaseError::Invalid(
                    "assignment drain decision has an invalid handoff checkpoint".into(),
                ));
            }
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
    /// Exact committed state cut installed by this recovery.
    pub recovery_checkpoint: CommittedCheckpointRef,
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
        recovery_checkpoint: CommittedCheckpointRef,
        leader_proof: LeaderProof,
    ) -> Result<Self, String> {
        let decision = Self {
            predecessor,
            target,
            proposal,
            removed_process_fences,
            recovery_checkpoint,
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
            || self.recovery_checkpoint.validate().is_err()
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

    fn predecessor(&self) -> &CheckpointAssignmentFence {
        match self {
            Self::Drain(decision) => &decision.transition.predecessor,
            Self::Recovery(decision) => &decision.predecessor,
        }
    }

    fn materialized_target(&self) -> CheckpointAssignmentFence {
        match self {
            Self::Drain(decision) => match decision.verdict {
                AssignmentDrainVerdict::Commit => decision.transition.target.clone(),
                AssignmentDrainVerdict::Abort => {
                    let mut target = decision.transition.predecessor.clone();
                    target.assignment_version = decision.transition.target.assignment_version;
                    target
                }
            },
            Self::Recovery(decision) => decision.target.clone(),
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

/// Destructive phase authorized by the cluster leader authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterArtifactCleanupPhase {
    /// Delete state objects that are not referenced by the protected recovery cuts.
    DeleteData,
    /// Delete the exact participant manifests and committed-checkpoint index.
    DeleteMetadata,
}

/// Exact, crash-resumable cluster checkpoint artifact cleanup position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterArtifactCleanupCursor {
    /// Oldest live committed checkpoint protected by this retention segment.
    pub protected: CommittedCheckpointRef,
    /// Exact expired committed checkpoint being reclaimed.
    pub current: CommittedCheckpointRef,
    /// Exact older checkpoint to process after `current`, if any.
    pub next: Option<CommittedCheckpointRef>,
    /// Previously reclaimed checkpoint at which this segment must stop.
    pub stop_before: Option<CommittedCheckpointRef>,
    /// Exact participants whose manifests belong to `current`.
    pub participant_ids: Vec<u64>,
    /// Destructive operation durably authorized for `current`.
    pub phase: ClusterArtifactCleanupPhase,
}

impl ClusterArtifactCleanupCursor {
    fn validate(&self) -> Result<(), LeaseError> {
        for (name, reference) in [
            ("protected", Some(&self.protected)),
            ("current", Some(&self.current)),
            ("next", self.next.as_ref()),
            ("stop-before", self.stop_before.as_ref()),
        ] {
            if let Some(reference) = reference {
                reference.validate().map_err(|error| {
                    LeaseError::Invalid(format!(
                        "cluster artifact cleanup {name} reference: {error}"
                    ))
                })?;
            }
        }
        if self.current.epoch >= self.protected.epoch
            || self.current.checkpoint_id >= self.protected.checkpoint_id
        {
            return Err(LeaseError::Invalid(
                "cluster artifact cleanup current checkpoint is not older than its protected cut"
                    .into(),
            ));
        }
        if let Some(next) = self.next.as_ref() {
            if next.epoch >= self.current.epoch || next.checkpoint_id >= self.current.checkpoint_id
            {
                return Err(LeaseError::Invalid(
                    "cluster artifact cleanup next checkpoint is not older than its current checkpoint"
                        .into(),
                ));
            }
        }
        if let Some(stop) = self.stop_before.as_ref() {
            if stop.epoch >= self.current.epoch || stop.checkpoint_id >= self.current.checkpoint_id
            {
                return Err(LeaseError::Invalid(
                    "cluster artifact cleanup stop boundary is not older than its current checkpoint"
                        .into(),
                ));
            }
            let next = self.next.as_ref().ok_or_else(|| {
                LeaseError::Invalid(
                    "cluster artifact cleanup lost the path to its stop boundary".into(),
                )
            })?;
            if next.epoch < stop.epoch || next.checkpoint_id < stop.checkpoint_id {
                return Err(LeaseError::Invalid(
                    "cluster artifact cleanup next checkpoint crosses its stop boundary".into(),
                ));
            }
        }
        if self.participant_ids.is_empty()
            || self.participant_ids.len() > MAX_CHECKPOINT_PARTICIPANTS
            || self.participant_ids[0] == 0
            || !self
                .participant_ids
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        {
            return Err(LeaseError::Invalid(
                "cluster artifact cleanup participants are not canonical".into(),
            ));
        }
        Ok(())
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AssignmentHandoffPin {
    target: CheckpointAssignmentFence,
    checkpoint: CommittedCheckpointRef,
}

impl AssignmentHandoffPin {
    fn validate(&self) -> Result<(), LeaseError> {
        if !self.target.is_canonical() {
            return Err(LeaseError::Invalid(
                "assignment handoff pin has an invalid target fence".into(),
            ));
        }
        self.checkpoint.validate().map_err(LeaseError::Invalid)
    }
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
    /// Exact leader-fenced artifact cleanup position, preserved across leadership terms.
    artifact_cleanup: Option<ClusterArtifactCleanupCursor>,
    /// Latest replay-retention floor linearized through this authority sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    subscription_cleanup_commit: Option<subscription_replay::SubscriptionCleanupCommit>,
    /// Exact unresolved checkpoint attempt whose artifacts require a terminal decision or cleanup.
    active_checkpoint_artifacts: Option<CheckpointArtifactInventory>,
    /// Leader term that admitted `active_checkpoint_artifacts` before any artifact write.
    active_checkpoint_artifact_leader_proof: Option<LeaderProof>,
    /// Present only on the sequence that admitted an assignment decision.
    assignment_decision: Option<AuthorityAssignmentDecision>,
    /// Link to the preceding assignment decision, present only on a decision-bearing record.
    previous_assignment_decision: Option<AssignmentDecisionLink>,
    /// Latest admitted assignment decision. Every other authority mutation preserves it.
    assignment_decision_head: Option<AssignmentDecisionLink>,
    /// Monotonic assignment-decision retention boundary and its continuity anchor.
    assignment_decision_floor: Option<AuthorityAssignmentDecisionFloor>,
    /// State handoff cut retained until a complete checkpoint commits under its target fence.
    assignment_handoff_pin: Option<AssignmentHandoffPin>,
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
        for link in [self.terminal_anchor_link, self.committed_anchor_link]
            .into_iter()
            .flatten()
        {
            link.validate()?;
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
            artifact_cleanup: None,
            subscription_cleanup_commit: None,
            active_checkpoint_artifacts: None,
            active_checkpoint_artifact_leader_proof: None,
            assignment_decision: None,
            previous_assignment_decision: None,
            assignment_decision_head: None,
            assignment_decision_floor: None,
            assignment_handoff_pin: None,
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
            artifact_cleanup: self.artifact_cleanup.clone(),
            subscription_cleanup_commit: self.subscription_cleanup_commit.clone(),
            active_checkpoint_artifacts: self.active_checkpoint_artifacts.clone(),
            active_checkpoint_artifact_leader_proof: self
                .active_checkpoint_artifact_leader_proof
                .clone(),
            assignment_decision: None,
            previous_assignment_decision: None,
            assignment_decision_head: self.assignment_decision_head,
            assignment_decision_floor: self.assignment_decision_floor.clone(),
            assignment_handoff_pin: self.assignment_handoff_pin.clone(),
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
        for link in [
            self.previous_outcome,
            self.outcome_head,
            self.previous_commit,
            self.commit_head,
        ]
        .into_iter()
        .flatten()
        {
            link.validate()?;
        }
        self.subscription_cleanup_commit
            .iter()
            .try_for_each(subscription_replay::SubscriptionCleanupCommit::validate)?;
        self.validate_recovery_fault_inventory()?;
        self.validate_checkpoint_outcome_chain()?;
        self.validate_outcome_floor()?;
        self.validate_artifact_cleanup()?;
        self.validate_checkpoint_artifact_inventory()?;
        self.validate_assignment_decision_chain()?;
        self.validate_assignment_decision_floor()?;
        self.validate_assignment_handoff_pin()?;
        self.validate_recovery_release()?;
        Ok(())
    }

    fn validate_recovery_fault_inventory(&self) -> Result<(), LeaseError> {
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
        Ok(())
    }

    fn validate_checkpoint_outcome_chain(&self) -> Result<(), LeaseError> {
        if let Some(outcome) = self.checkpoint_outcome.as_ref() {
            outcome
                .validate_shape(outcome.epoch)
                .map_err(|error| LeaseError::Invalid(error.to_string()))?;
            if outcome.scope != CheckpointScope::Cluster {
                return Err(LeaseError::Invalid(
                    "leader authority can only admit cluster checkpoint outcomes".into(),
                ));
            }
            let proof = outcome
                .leader_proof
                .as_ref()
                .ok_or_else(|| LeaseError::Invalid("cluster outcome has no leader proof".into()))?;
            let current_link = OutcomeLink {
                sequence: self.lease.seq,
                epoch: outcome.epoch,
                checkpoint_id: outcome.checkpoint_id,
            };
            if !self.lease.matches_proof(proof) || self.outcome_head != Some(current_link) {
                return Err(LeaseError::Invalid(
                    "cluster outcome is not bound to its exact authority sequence and term".into(),
                ));
            }
            if let Some(previous) = self.previous_outcome {
                if previous.sequence >= self.lease.seq
                    || previous.epoch >= outcome.epoch
                    || previous.checkpoint_id >= outcome.checkpoint_id
                {
                    return Err(LeaseError::Invalid(
                        "cluster outcome link does not move backward in sequence and epoch".into(),
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
                        "cluster outcome is below or outside its durable authority floor".into(),
                    ));
                }
            }
        } else {
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
        Ok(())
    }

    fn validate_outcome_floor(&self) -> Result<(), LeaseError> {
        if let Some(floor) = self.outcome_floor.as_ref() {
            floor.validate()?;
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
        Ok(())
    }

    fn validate_artifact_cleanup(&self) -> Result<(), LeaseError> {
        let Some(cursor) = self.artifact_cleanup.as_ref() else {
            return Ok(());
        };
        cursor.validate()?;
        let floor = self.outcome_floor.as_ref().ok_or_else(|| {
            LeaseError::Invalid(
                "cluster artifact cleanup exists without an outcome retention floor".into(),
            )
        })?;
        if floor.artifact_before_epoch != cursor.protected.epoch
            || self
                .commit_head
                .is_none_or(|head| head.epoch < cursor.protected.epoch)
        {
            return Err(LeaseError::Invalid(
                "cluster artifact cleanup is not bound to its protected retention cut".into(),
            ));
        }
        Ok(())
    }

    fn validate_checkpoint_artifact_inventory(&self) -> Result<(), LeaseError> {
        let (Some(inventory), Some(admitting_proof)) = (
            self.active_checkpoint_artifacts.as_ref(),
            self.active_checkpoint_artifact_leader_proof.as_ref(),
        ) else {
            if self.active_checkpoint_artifacts.is_some()
                || self.active_checkpoint_artifact_leader_proof.is_some()
            {
                return Err(LeaseError::Invalid(
                    "cluster checkpoint artifact inventory has incomplete leader authority".into(),
                ));
            }
            return Ok(());
        };

        inventory.validate().map_err(|error| {
            LeaseError::Invalid(format!("cluster checkpoint artifact inventory: {error}"))
        })?;
        let assignment_fence = inventory.assignment_fence.as_ref().ok_or_else(|| {
            LeaseError::Invalid(
                "cluster checkpoint artifact inventory has no assignment fence".into(),
            )
        })?;
        if !admitting_proof.is_canonical()
            || assignment_fence.participant_incarnation(admitting_proof.owner.node_id)
                != Some(admitting_proof.owner.boot_id)
        {
            return Err(LeaseError::Invalid(
                "cluster checkpoint artifact inventory has no canonical admitting leader".into(),
            ));
        }
        if self
            .outcome_floor
            .as_ref()
            .is_some_and(|floor| floor.deployment_id != inventory.deployment_id)
        {
            return Err(LeaseError::Invalid(
                "cluster checkpoint artifact inventory belongs to a foreign deployment".into(),
            ));
        }
        if let Some(head) = self.outcome_head {
            let attempt = crate::checkpoint::CheckpointAttempt::new(head.epoch, head.checkpoint_id);
            if matches!(
                inventory.attempt.relation_to(attempt),
                crate::checkpoint::CheckpointAttemptRelation::Older
                    | crate::checkpoint::CheckpointAttemptRelation::Conflict
            ) {
                return Err(LeaseError::Invalid(
                    "cluster checkpoint artifact inventory is behind the terminal outcome head"
                        .into(),
                ));
            }
        }
        if let Some(outcome) = self.checkpoint_outcome.as_ref() {
            let exact = outcome.deployment_id == inventory.deployment_id
                && outcome.epoch == inventory.attempt.epoch
                && outcome.checkpoint_id == inventory.attempt.checkpoint_id
                && outcome.assignment_fence.as_ref() == inventory.assignment_fence.as_ref();
            if outcome.is_commit() || !exact {
                return Err(LeaseError::Invalid(
                    "terminal cluster outcome has inconsistent checkpoint artifact inventory"
                        .into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_assignment_decision_chain(&self) -> Result<(), LeaseError> {
        if let Some(decision) = self.assignment_decision.as_ref() {
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
        } else {
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
        Ok(())
    }

    fn validate_assignment_decision_floor(&self) -> Result<(), LeaseError> {
        if let Some(floor) = self.assignment_decision_floor.as_ref() {
            floor.validate()?;
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
        Ok(())
    }

    fn validate_assignment_handoff_pin(&self) -> Result<(), LeaseError> {
        let Some(pin) = self.assignment_handoff_pin.as_ref() else {
            return Ok(());
        };
        pin.validate()?;
        if self.commit_head.is_none_or(|head| {
            head.epoch != pin.checkpoint.epoch || head.checkpoint_id != pin.checkpoint.checkpoint_id
        }) || self
            .outcome_floor
            .as_ref()
            .is_some_and(|floor| floor.artifact_before_epoch > pin.checkpoint.epoch)
            || self
                .artifact_cleanup
                .as_ref()
                .is_some_and(|cursor| cursor.protected.epoch > pin.checkpoint.epoch)
        {
            return Err(LeaseError::Invalid(
                "assignment handoff pin is outside its live checkpoint retention range".into(),
            ));
        }
        Ok(())
    }

    fn validate_recovery_release(&self) -> Result<(), LeaseError> {
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
                    || self.recovery_fault_slots.iter().any(|slot| {
                        slot.active || slot.disposition == RecoveryFaultDisposition::Terminal
                    })
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
    Contended(Box<LeaderAuthorityRecord>),
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
    /// The store must provide linearizable `PutMode::Create`/`Update` and GET `ETag` or version
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

    /// Verify the conditional-write contract required by the mutable authority head.
    ///
    /// Validation and cleanup each receive the supplied timeout. The probe uses a unique path and
    /// cleanup is attempted after every validation outcome.
    ///
    /// # Errors
    /// Returns an error when the store does not enforce native conditional writes, omits update
    /// metadata, times out, or cannot remove the probe.
    pub async fn verify_store_contract(&self, timeout: Duration) -> Result<(), LeaseError> {
        probe_object_store_conditional_update(
            self.store.as_ref(),
            STORE_CONTRACT_PROBE_PREFIX,
            timeout,
        )
        .await
        .map_err(|error| match error {
            CheckpointStoreError::Invalid(message) => LeaseError::Invalid(message),
            error => LeaseError::Io(error.to_string()),
        })
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

    #[cfg(test)]
    pub(crate) async fn record_recovery_fault(
        &self,
        publisher: RecoveryFaultPublisher,
        request_sequence: u64,
    ) -> Result<RecordRecoveryFaultResult, ClusterCheckpointAuthorityError> {
        self.record_recovery_fault_with_disposition(
            publisher,
            request_sequence,
            RecoveryFaultDisposition::Recoverable,
        )
        .await
    }

    pub(crate) async fn record_recovery_fault_with_disposition(
        &self,
        publisher: RecoveryFaultPublisher,
        request_sequence: u64,
        disposition: RecoveryFaultDisposition,
    ) -> Result<RecordRecoveryFaultResult, ClusterCheckpointAuthorityError> {
        publisher.validate().map_err(LeaseError::Invalid)?;
        if request_sequence == 0 {
            return Err(LeaseError::Invalid(
                "recovery fault request sequence must be nonzero".into(),
            )
            .into());
        }

        loop {
            let head = self
                .load_published_authority_head()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let current = &head.record;
            let node_id = publisher.participant.node_id;
            let slot_index = current
                .recovery_fault_slots
                .binary_search_by_key(&node_id, |slot| slot.publisher.participant.node_id);
            let (insert_at, replace) = match slot_index {
                Ok(index) => {
                    let slot = &current.recovery_fault_slots[index];
                    if slot.matches_request(publisher, request_sequence, disposition) {
                        return Ok(if slot.active {
                            RecordRecoveryFaultResult::Active
                        } else {
                            RecordRecoveryFaultResult::AlreadyCleared
                        });
                    }
                    // A terminal slot is an operator-owned cluster fence. Neither a later request
                    // nor a replacement process may silently downgrade or tombstone it.
                    if slot.disposition == RecoveryFaultDisposition::Terminal {
                        return Ok(RecordRecoveryFaultResult::TerminalFenceActive);
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
                disposition,
                active: true,
            };
            if replace {
                candidate.recovery_fault_slots[insert_at] = slot;
            } else {
                candidate.recovery_fault_slots.insert(insert_at, slot);
            }
            candidate.recovery_fault_revision = sequence;
            candidate.validate()?;

            match self
                .create_authority_record(Some(&head), &candidate)
                .await?
            {
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
        if current
            .recovery_fault_slots
            .iter()
            .any(|slot| slot.active || slot.disposition == RecoveryFaultDisposition::Terminal)
        {
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
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let current = &published.record;
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
            let fault_inventory = Self::recovery_fault_inventory_from(current);
            if fault_inventory.revision != terminal.round.fault_revision()
                || fault_inventory.faults != terminal.round.faults
                || fault_inventory
                    .faults
                    .iter()
                    .copied()
                    .any(RecoveryFault::is_terminal)
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

            match self
                .create_authority_record(Some(&published), &candidate)
                .await?
            {
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
        let pointer = if let Some(pointer) =
            read_authority_head_pointer(self.store.as_ref()).await?
        {
            pointer
        } else {
            let Some(discovered) = self.discover_authority_head().await? else {
                let Some(pointer) = read_authority_head_pointer(self.store.as_ref()).await? else {
                    return Ok(None);
                };
                return self.load_authority_head_target(pointer).await.map(Some);
            };
            let discovered_sequence = discovered.lease.seq;
            let published_sequence = self
                .publish_authority_head(discovered_sequence, None)
                .await?;
            let pointer = self
                .reload_authority_head_pointer(published_sequence)
                .await?;
            if published_sequence > discovered_sequence
                || pointer.pointer.sequence > published_sequence
            {
                return self.load_authority_head_target(pointer).await.map(Some);
            }
            pointer
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
                return self
                    .reload_published_authority_snapshot(rechecked.pointer.sequence)
                    .await
                    .map(Some);
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
                            "leader authority head disappeared while checking pointer lag".into(),
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
                return self
                    .reload_published_authority_snapshot(rechecked.pointer.sequence)
                    .await
                    .map(Some);
            }
        }

        let published_sequence = self
            .publish_authority_head(successor_sequence, Some(&pointer))
            .await?;
        self.reload_published_authority_snapshot(published_sequence)
            .await
            .map(Some)
    }

    async fn reload_published_authority_snapshot(
        &self,
        minimum_sequence: u64,
    ) -> Result<PublishedAuthorityHead, LeaseError> {
        let pointer = self.reload_authority_head_pointer(minimum_sequence).await?;
        self.load_authority_head_target(pointer).await
    }

    async fn reload_authority_head_pointer(
        &self,
        minimum_sequence: u64,
    ) -> Result<VersionedAuthorityHeadPointer, LeaseError> {
        let pointer = read_authority_head_pointer(self.store.as_ref())
            .await?
            .ok_or_else(|| {
                LeaseError::Invalid(
                    "leader authority head disappeared while reloading its published snapshot"
                        .into(),
                )
            })?;
        if pointer.pointer.sequence < minimum_sequence {
            return Err(LeaseError::Invalid(format!(
                "leader authority head regressed from sequence {minimum_sequence} to {}",
                pointer.pointer.sequence
            )));
        }
        Ok(pointer)
    }

    async fn load_authority_head_target(
        &self,
        mut pointer: VersionedAuthorityHeadPointer,
    ) -> Result<PublishedAuthorityHead, LeaseError> {
        for attempt in 0..MAX_LEASE_HEAD_READ_ATTEMPTS {
            let sequence = pointer.pointer.sequence;
            if let Some(record) = read_authority_record(self.store.as_ref(), sequence).await? {
                return Ok(PublishedAuthorityHead { record, pointer });
            }

            let rechecked = read_authority_head_pointer(self.store.as_ref())
                .await?
                .ok_or_else(|| {
                    LeaseError::Invalid(
                        "leader authority head disappeared while reading its target".into(),
                    )
                })?;
            if rechecked.pointer.sequence < sequence {
                return Err(LeaseError::Invalid(format!(
                    "leader authority head regressed from sequence {sequence} to {}",
                    rechecked.pointer.sequence
                )));
            }
            if rechecked.pointer.sequence == sequence {
                return Err(LeaseError::Invalid(format!(
                    "leader authority head points ahead to missing sequence {sequence}"
                )));
            }
            pointer = rechecked;
            if attempt + 1 < MAX_LEASE_HEAD_READ_ATTEMPTS {
                tokio::task::yield_now().await;
            }
        }
        Err(LeaseError::Io(format!(
            "leader authority head target changed during {MAX_LEASE_HEAD_READ_ATTEMPTS} read attempts"
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
        expected: Option<&PublishedAuthorityHead>,
        candidate: &LeaderAuthorityRecord,
    ) -> Result<AuthorityCreateOutcome, LeaseError> {
        let encoded = encode_authority_record(candidate)?;
        let expected_pointer = match expected {
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
                    Ok(AuthorityCreateOutcome::Contended(Box::new(
                        head.record.clone(),
                    )))
                };
            }
            Some(head) if head.record.lease.seq > candidate.lease.seq => {
                return Ok(AuthorityCreateOutcome::Contended(Box::new(
                    head.record.clone(),
                )));
            }
            Some(head) if head.record.lease.seq.checked_add(1) == Some(candidate.lease.seq) => {
                Some(&head.pointer)
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
                return Ok(AuthorityCreateOutcome::Contended(Box::new(winner)));
            }
        }

        let published_sequence = self
            .publish_authority_head(candidate.lease.seq, expected_pointer)
            .await?;
        if published_sequence > candidate.lease.seq {
            if created {
                self.schedule_history_prune();
            }
            let winner = self
                .load_record()
                .await?
                .ok_or_else(|| LeaseError::Io("newer published authority head vanished".into()))?;
            return Ok(AuthorityCreateOutcome::Contended(Box::new(winner)));
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
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or(CatalogManifestError::Fenced)?;
            let current = &published.record;
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
            match self
                .create_authority_record(Some(&published), &candidate)
                .await?
            {
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
        let published = self
            .load_published_authority_head()
            .await?
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        let rechecked = &published.record;
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
        match self
            .create_authority_record(Some(&published), &next)
            .await?
        {
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

    async fn reject_consumed_checkpoint_assignment(
        &self,
        current: &LeaderAuthorityRecord,
        assignment_fence: &CheckpointAssignmentFence,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        let decisions = self.audited_assignment_decisions_from(current).await?;
        if let Some(floor) = current.assignment_decision_floor.as_ref() {
            // The floor is half-open: only target versions below it were compacted. Equality is
            // not evidence that the fence was consumed (notably for a bootstrap assignment),
            // while any decision at the boundary remains in `decisions` and is checked below.
            if assignment_fence.assignment_version < floor.before_target_version {
                return Err(DecisionError::Conflict(format!(
                    "checkpoint assignment version {} is below durable assignment-decision floor {}",
                    assignment_fence.assignment_version, floor.before_target_version
                ))
                .into());
            }
        }
        let Some(newest) = decisions.last() else {
            return Ok(());
        };
        if newest.target_version() > assignment_fence.assignment_version {
            return Err(DecisionError::Conflict(format!(
                "checkpoint assignment version {} was consumed by assignment decision version {}",
                assignment_fence.assignment_version,
                newest.target_version()
            ))
            .into());
        }
        if newest.target_version() == assignment_fence.assignment_version
            && newest.materialized_target() != *assignment_fence
        {
            return Err(DecisionError::Conflict(format!(
                "checkpoint assignment version {} does not match its materialized assignment decision",
                assignment_fence.assignment_version
            ))
            .into());
        }
        Ok(())
    }

    /// Admit one exact cluster checkpoint attempt before any participant writes artifacts.
    ///
    /// An identical retry returns the durable inventory. A later attempt cannot begin until the
    /// current attempt commits or its aborted artifacts are cleaned exactly.
    ///
    /// # Errors
    /// Fails for a stale proof, foreign deployment, malformed inventory, another active attempt,
    /// or object-store failure.
    pub async fn begin_cluster_checkpoint_artifacts(
        &self,
        proof: &LeaderProof,
        inventory: CheckpointArtifactInventory,
    ) -> Result<CheckpointArtifactInventory, ClusterCheckpointAuthorityError> {
        if !proof.is_canonical() {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        inventory.validate().map_err(DecisionError::Conflict)?;
        let assignment_fence = inventory.assignment_fence.as_ref().ok_or_else(|| {
            DecisionError::Conflict(
                "cluster checkpoint artifact inventory requires an assignment fence".into(),
            )
        })?;
        if assignment_fence.participant_incarnation(proof.owner.node_id)
            != Some(proof.owner.boot_id)
        {
            return Err(DecisionError::Conflict(
                "checkpoint artifact admitting leader is outside the assignment fence".into(),
            )
            .into());
        }
        let initial = self
            .load_record()
            .await?
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        if !initial.lease.matches_proof(proof) {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        let deployment_id = CheckpointDecisionStore::new(Arc::clone(&self.store))
            .load_or_create_deployment_id()
            .await?;
        if inventory.deployment_id != deployment_id {
            return Err(DecisionError::Conflict(format!(
                "checkpoint artifact inventory deployment {} does not match authority deployment {deployment_id}",
                inventory.deployment_id
            ))
            .into());
        }

        loop {
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let current = &published.record;
            if !current.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            self.reject_consumed_checkpoint_assignment(current, assignment_fence)
                .await?;
            if let Some(active) = current.active_checkpoint_artifacts.as_ref() {
                if active == &inventory
                    && current.active_checkpoint_artifact_leader_proof.as_ref() == Some(proof)
                    && current.outcome_head.is_none_or(|head| {
                        head.epoch != inventory.attempt.epoch
                            || head.checkpoint_id != inventory.attempt.checkpoint_id
                    })
                {
                    return Ok(active.clone());
                }
                return Err(DecisionError::Conflict(format!(
                    "checkpoint {} still has unresolved or inherited cluster artifacts",
                    active.attempt.checkpoint_id
                ))
                .into());
            }
            if let Some(head) = current.outcome_head {
                let terminal =
                    crate::checkpoint::CheckpointAttempt::new(head.epoch, head.checkpoint_id);
                if inventory.attempt.relation_to(terminal)
                    != crate::checkpoint::CheckpointAttemptRelation::Newer
                {
                    return Err(DecisionError::Conflict(format!(
                        "checkpoint {} does not advance terminal checkpoint {}",
                        inventory.attempt.checkpoint_id, head.checkpoint_id
                    ))
                    .into());
                }
            }

            let sequence =
                current.lease.seq.checked_add(1).ok_or_else(|| {
                    LeaseError::Invalid("leader authority sequence exhausted".into())
                })?;
            let mut lease = current.lease.clone();
            lease.seq = sequence;
            let mut next = current.preserve_with_lease(lease);
            next.active_checkpoint_artifacts = Some(inventory.clone());
            next.active_checkpoint_artifact_leader_proof = Some(proof.clone());
            next.validate()?;
            match self
                .create_authority_record(Some(&published), &next)
                .await?
            {
                AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                    return Ok(inventory);
                }
                AuthorityCreateOutcome::Contended(winner) => {
                    if !winner.lease.matches_proof(proof) {
                        return Err(ClusterCheckpointAuthorityError::Fenced);
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    /// Clear one exact retained inventory after its durable Abort artifact paths are sealed.
    ///
    /// # Errors
    /// Fails for a stale proof, a different active attempt, a missing matching Abort, malformed
    /// inventory, or object-store failure.
    pub async fn finish_cluster_checkpoint_artifact_cleanup(
        &self,
        proof: &LeaderProof,
        expected: &CheckpointArtifactInventory,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        if !proof.is_canonical() {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        expected.validate().map_err(DecisionError::Conflict)?;
        if expected.assignment_fence.is_none() {
            return Err(DecisionError::Conflict(
                "cluster checkpoint artifact inventory requires an assignment fence".into(),
            )
            .into());
        }

        loop {
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let current = &published.record;
            if !current.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            let snapshot = self.cached_audited_cluster_outcomes_from(current).await?;
            let matching_abort = snapshot.outcomes.iter().any(|outcome| {
                matches!(outcome.verdict, CheckpointVerdict::Abort)
                    && outcome.deployment_id == expected.deployment_id
                    && outcome.epoch == expected.attempt.epoch
                    && outcome.checkpoint_id == expected.attempt.checkpoint_id
                    && outcome.assignment_fence.as_ref() == expected.assignment_fence.as_ref()
            });
            match current.active_checkpoint_artifacts.as_ref() {
                None if matching_abort => return Ok(()),
                None => {
                    return Err(DecisionError::Conflict(format!(
                        "checkpoint {} has no retained cluster artifact inventory",
                        expected.attempt.checkpoint_id
                    ))
                    .into());
                }
                Some(active) if active != expected => {
                    return Err(DecisionError::Conflict(format!(
                        "checkpoint {} still has unresolved cluster artifacts",
                        active.attempt.checkpoint_id
                    ))
                    .into());
                }
                Some(_) if !matching_abort => {
                    return Err(DecisionError::Conflict(format!(
                        "checkpoint {} has no matching durable Abort",
                        expected.attempt.checkpoint_id
                    ))
                    .into());
                }
                Some(_) => {}
            }

            let sequence =
                current.lease.seq.checked_add(1).ok_or_else(|| {
                    LeaseError::Invalid("leader authority sequence exhausted".into())
                })?;
            let mut lease = current.lease.clone();
            lease.seq = sequence;
            let mut next = current.preserve_with_lease(lease);
            next.active_checkpoint_artifacts = None;
            next.active_checkpoint_artifact_leader_proof = None;
            next.validate()?;
            match self
                .create_authority_record(Some(&published), &next)
                .await?
            {
                AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                    return Ok(());
                }
                AuthorityCreateOutcome::Contended(winner) => {
                    if !winner.lease.matches_proof(proof) {
                        return Err(ClusterCheckpointAuthorityError::Fenced);
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    /// Admit one cluster terminal outcome through the exact next leader-authority sequence.
    ///
    /// Renewals, takeovers, catalog seals, floor advances, and other decisions all contend on the
    /// same create-only object. An identical retry converges on the durable winner.
    ///
    /// # Errors
    /// Fails closed for a stale proof, non-monotonic or conflicting outcome, malformed committed
    /// checkpoint index, or object-store failure.
    pub async fn record_cluster_outcome(
        &self,
        proof: &LeaderProof,
        epoch: u64,
        checkpoint_id: u64,
        assignment_fence: CheckpointAssignmentFence,
        verdict: CheckpointVerdict,
        committed_checkpoint: Option<CommittedCheckpointRef>,
    ) -> Result<RecordOutcomeResult, ClusterCheckpointAuthorityError> {
        if !proof.is_canonical() {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        let attempt = crate::checkpoint::CheckpointAttempt::new(epoch, checkpoint_id);
        if !attempt.is_canonical() {
            return Err(DecisionError::Conflict(
                "cluster checkpoint outcomes require one nonzero canonical checkpoint ID".into(),
            )
            .into());
        }
        let initial = self
            .load_record()
            .await?
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        if !initial.lease.matches_proof(proof) {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        let decisions = CheckpointDecisionStore::new(Arc::clone(&self.store));
        let (candidate, committed_index) = decisions
            .canonical_outcome_with_index(
                epoch,
                checkpoint_id,
                CheckpointScope::Cluster,
                Some(assignment_fence),
                Some(proof.clone()),
                verdict,
                committed_checkpoint,
            )
            .await?;

        loop {
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let current = &published.record;
            if !current.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            let snapshot = self.cached_audited_cluster_outcomes_from(current).await?;
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
            let active = match current.active_checkpoint_artifacts.as_ref() {
                Some(active)
                    if active.deployment_id == candidate.deployment_id
                        && active.attempt.epoch == candidate.epoch
                        && active.attempt.checkpoint_id == candidate.checkpoint_id
                        && active.assignment_fence.as_ref()
                            == candidate.assignment_fence.as_ref() =>
                {
                    Some(active)
                }
                Some(_) => {
                    return Err(DecisionError::Conflict(format!(
                        "cluster checkpoint {} does not match the active artifact inventory",
                        candidate.checkpoint_id
                    ))
                    .into());
                }
                None if candidate.is_commit() => {
                    return Err(DecisionError::Conflict(format!(
                        "cluster Commit checkpoint {} has no admitted artifact inventory",
                        candidate.checkpoint_id
                    ))
                    .into());
                }
                None => None,
            };
            if candidate.is_commit()
                && current.active_checkpoint_artifact_leader_proof.as_ref() != Some(proof)
            {
                return Err(DecisionError::Conflict(format!(
                    "takeover leader cannot Commit checkpoint {} admitted by an older leader term",
                    candidate.checkpoint_id
                ))
                .into());
            }
            if let Some(last) = outcomes.last() {
                if candidate.checkpoint_id <= last.checkpoint_id {
                    return Err(DecisionError::Conflict(format!(
                        "cluster checkpoint {} does not advance durable checkpoint {}",
                        candidate.checkpoint_id, last.checkpoint_id
                    ))
                    .into());
                }
            }
            if candidate.is_commit()
                && current
                    .assignment_handoff_pin
                    .as_ref()
                    .is_some_and(|pin| candidate.assignment_fence.as_ref() != Some(&pin.target))
            {
                return Err(DecisionError::Conflict(
                    "cluster Commit does not bind the active assignment handoff target".into(),
                )
                .into());
            }
            let (commit_index, expected_predecessor) = if candidate.is_commit() {
                let index = committed_index.as_ref().ok_or_else(|| {
                    DecisionError::Conflict(
                        "canonical cluster Commit is missing its committed checkpoint index".into(),
                    )
                })?;
                let expected_predecessor = outcomes
                    .iter()
                    .rev()
                    .find(|outcome| outcome.is_commit())
                    .and_then(|outcome| outcome.committed_checkpoint.clone());
                if index.predecessor != expected_predecessor {
                    return Err(DecisionError::Conflict(format!(
                        "cluster Commit checkpoint {} does not extend the authoritative Commit head",
                        candidate.checkpoint_id
                    ))
                    .into());
                }
                if active.is_none_or(|active| index.pipeline_identity != active.pipeline_identity) {
                    return Err(DecisionError::Conflict(format!(
                        "cluster Commit checkpoint {} does not match its admitted pipeline identity",
                        candidate.checkpoint_id
                    ))
                    .into());
                }
                (Some(index), expected_predecessor)
            } else {
                (None, None)
            };
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
            if Box::pin(
                self.compact_cluster_outcome_history_before_append(proof, current, &snapshot),
            )
            .await?
            {
                tokio::task::yield_now().await;
                continue;
            }
            if let (Some(index), Some(predecessor_ref)) =
                (commit_index, expected_predecessor.as_ref())
            {
                let predecessor = decisions.load_committed_checkpoint(predecessor_ref).await?;
                index
                    .validate_predecessor_index(&predecessor)
                    .map_err(DecisionError::Conflict)?;
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
                next.active_checkpoint_artifacts = None;
                next.active_checkpoint_artifact_leader_proof = None;
                if next
                    .assignment_handoff_pin
                    .as_ref()
                    .is_some_and(|pin| candidate.assignment_fence.as_ref() == Some(&pin.target))
                {
                    next.assignment_handoff_pin = None;
                }
            }
            next.validate()?;
            match self
                .create_authority_record(Some(&published), &next)
                .await?
            {
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

    async fn current_assignment_checkpoint(
        &self,
        current: &LeaderAuthorityRecord,
        predecessor: &CheckpointAssignmentFence,
        purpose: &str,
    ) -> Result<Option<CommittedCheckpointRef>, ClusterCheckpointAuthorityError> {
        let Some(commit_head) = current.commit_head else {
            return Ok(None);
        };
        let outcome = self
            .cluster_outcome_from_snapshot(current, commit_head.epoch)
            .await?
            .ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "{purpose} checkpoint {} is not live",
                    commit_head.checkpoint_id
                ))
            })?;
        if !outcome.is_commit()
            || outcome.checkpoint_id != commit_head.checkpoint_id
            || outcome.assignment_fence.as_ref() != Some(predecessor)
        {
            return Err(DecisionError::Conflict(format!(
                "{purpose} checkpoint {} does not bind its predecessor fence",
                commit_head.checkpoint_id
            ))
            .into());
        }
        let reference = outcome.committed_checkpoint.as_ref().ok_or_else(|| {
            DecisionError::Conflict(format!(
                "{purpose} checkpoint {} has no committed index reference",
                commit_head.checkpoint_id
            ))
        })?;
        let index = CheckpointDecisionStore::new(Arc::clone(&self.store))
            .validate_committed_checkpoint_for_outcome(&outcome)
            .await?;
        if index.assignment_fence.as_ref() != Some(predecessor) {
            return Err(DecisionError::Conflict(format!(
                "{purpose} checkpoint index {} does not bind its predecessor fence",
                commit_head.checkpoint_id
            ))
            .into());
        }
        Ok(Some(reference.clone()))
    }

    async fn validate_current_assignment_checkpoint(
        &self,
        current: &LeaderAuthorityRecord,
        reference: &CommittedCheckpointRef,
        predecessor: &CheckpointAssignmentFence,
        purpose: &str,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        let current_reference = self
            .current_assignment_checkpoint(current, predecessor, purpose)
            .await?
            .ok_or_else(|| {
                DecisionError::Conflict(format!("{purpose} has no authoritative checkpoint Commit"))
            })?;
        if current_reference != *reference {
            return Err(DecisionError::Conflict(format!(
                "{purpose} checkpoint {} is not the current Commit head {}",
                reference.checkpoint_id, current_reference.checkpoint_id
            ))
            .into());
        }
        Ok(())
    }

    fn aborted_handoff_target(
        transition: &AssignmentDrainTransition,
    ) -> Result<CheckpointAssignmentFence, ClusterCheckpointAuthorityError> {
        let mut target = transition.predecessor.clone();
        target.assignment_version = transition.target.assignment_version;
        if !target.is_canonical() {
            return Err(LeaseError::Invalid(
                "assignment drain abort produced an invalid handoff target".into(),
            )
            .into());
        }
        Ok(target)
    }

    async fn next_assignment_handoff_pin(
        &self,
        current: &LeaderAuthorityRecord,
        decision: &AuthorityAssignmentDecision,
    ) -> Result<Option<AssignmentHandoffPin>, ClusterCheckpointAuthorityError> {
        match decision {
            AuthorityAssignmentDecision::Drain(drain) => match drain.verdict {
                AssignmentDrainVerdict::Commit => {
                    if current.assignment_handoff_pin.is_some() {
                        return Err(DecisionError::Conflict(
                            "assignment decision cannot overtake an unresolved state handoff"
                                .into(),
                        )
                        .into());
                    }
                    let reference = drain.handoff_checkpoint.as_ref().ok_or_else(|| {
                        LeaseError::Invalid(
                            "committed assignment drain has no handoff checkpoint".into(),
                        )
                    })?;
                    self.validate_current_assignment_checkpoint(
                        current,
                        reference,
                        &drain.transition.predecessor,
                        "assignment drain handoff",
                    )
                    .await?;
                    Ok(Some(AssignmentHandoffPin {
                        target: drain.transition.target.clone(),
                        checkpoint: reference.clone(),
                    }))
                }
                AssignmentDrainVerdict::Abort => match current.assignment_handoff_pin.as_ref() {
                    None => {
                        let checkpoint = self
                            .current_assignment_checkpoint(
                                current,
                                &drain.transition.predecessor,
                                "assignment drain abort",
                            )
                            .await?;
                        match checkpoint {
                            Some(checkpoint) => Ok(Some(AssignmentHandoffPin {
                                target: Self::aborted_handoff_target(&drain.transition)?,
                                checkpoint,
                            })),
                            None => Ok(None),
                        }
                    }
                    Some(pin) if pin.target == drain.transition.predecessor => {
                        Ok(Some(AssignmentHandoffPin {
                            target: Self::aborted_handoff_target(&drain.transition)?,
                            checkpoint: pin.checkpoint.clone(),
                        }))
                    }
                    Some(_) => Err(DecisionError::Conflict(
                        "assignment decision cannot overtake an unresolved state handoff".into(),
                    )
                    .into()),
                },
            },
            AuthorityAssignmentDecision::Recovery(recovery) => {
                if let Some(pin) = current.assignment_handoff_pin.as_ref() {
                    if pin.target != recovery.predecessor
                        || pin.checkpoint != recovery.recovery_checkpoint
                    {
                        return Err(DecisionError::Conflict(
                            "assignment recovery does not continue the exact unresolved state handoff"
                                .into(),
                        )
                        .into());
                    }
                } else {
                    self.validate_current_assignment_checkpoint(
                        current,
                        &recovery.recovery_checkpoint,
                        &recovery.predecessor,
                        "assignment recovery",
                    )
                    .await?;
                }
                Ok(Some(AssignmentHandoffPin {
                    target: recovery.target.clone(),
                    checkpoint: recovery.recovery_checkpoint.clone(),
                }))
            }
        }
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
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let current = &published.record;
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
            let decisions = self.audited_assignment_decisions_from(current).await?;
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
            if matches!(&decision, AuthorityAssignmentDecision::Drain(_)) {
                let predecessor = decision.predecessor();
                if let Some(active) = current.active_checkpoint_artifacts.as_ref() {
                    let active_fence = active.assignment_fence.as_ref().ok_or_else(|| {
                        DecisionError::Conflict(
                            "active checkpoint artifact inventory lost its assignment fence".into(),
                        )
                    })?;
                    if active_fence.assignment_version == predecessor.assignment_version {
                        let detail = if active_fence == predecessor {
                            "matches"
                        } else {
                            "conflicts with"
                        };
                        return Err(DecisionError::Conflict(format!(
                            "assignment drain decision cannot overtake checkpoint {} whose assignment fence {detail} predecessor version {}",
                            active.attempt.checkpoint_id,
                            predecessor.assignment_version
                        ))
                        .into());
                    }
                }
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
            let assignment_handoff_pin =
                self.next_assignment_handoff_pin(current, &decision).await?;

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
            candidate.assignment_handoff_pin = assignment_handoff_pin;
            candidate.validate()?;

            match self
                .create_authority_record(Some(&published), &candidate)
                .await?
            {
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
        match Box::pin(
            self.record_assignment_decision(proof, AuthorityAssignmentDecision::Drain(decision)),
        )
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
        match Box::pin(
            self.record_assignment_decision(proof, AuthorityAssignmentDecision::Recovery(decision)),
        )
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

    /// Read the state handoff checkpoint pinned for one exact target assignment.
    ///
    /// # Errors
    /// Fails when `target` is invalid or the durable authority head is unavailable.
    pub async fn assignment_handoff_checkpoint(
        &self,
        target: &CheckpointAssignmentFence,
    ) -> Result<Option<CommittedCheckpointRef>, ClusterCheckpointAuthorityError> {
        if !target.is_canonical() {
            return Err(LeaseError::Invalid(
                "assignment handoff checkpoint lookup requires a canonical target fence".into(),
            )
            .into());
        }
        Ok(self.load_record().await?.and_then(|head| {
            head.assignment_handoff_pin
                .filter(|pin| pin.target == *target)
                .map(|pin| pin.checkpoint)
        }))
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
    ///
    /// # Errors
    ///
    /// Fails for a stale proof, invalid horizon, corrupt history, or storage failure.
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
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let current = &published.record;
            if !current.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            if let Some(floor) = current.assignment_decision_floor.as_ref() {
                if floor.before_target_version >= before_target_version {
                    self.schedule_history_prune();
                    return Ok(floor.before_target_version);
                }
            }

            let decisions = self.audited_assignment_decisions_from(current).await?;
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
                Some(anchor) => Some(self.exact_assignment_decision_link(current, anchor).await?),
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

            match self
                .create_authority_record(Some(&published), &next)
                .await?
            {
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
    ///
    /// # Errors
    ///
    /// Fails when the durable authority history is unavailable or invalid.
    pub async fn cluster_outcome(
        &self,
        epoch: u64,
    ) -> Result<Option<CheckpointOutcome>, ClusterCheckpointAuthorityError> {
        self.stable_cluster_outcome(epoch).await
    }

    /// Store one immutable committed-checkpoint index before publishing its Commit outcome.
    ///
    /// # Errors
    ///
    /// Fails when the durable authority cannot create the immutable index.
    pub async fn create_committed_checkpoint(
        &self,
        index: &CommittedCheckpointIndex,
    ) -> Result<CommittedCheckpointRef, ClusterCheckpointAuthorityError> {
        CheckpointDecisionStore::new(Arc::clone(&self.store))
            .create_committed_checkpoint(index)
            .await
            .map_err(Into::into)
    }

    /// Load one exact content-addressed committed-checkpoint index.
    ///
    /// # Errors
    ///
    /// Fails when the durable authority cannot load or validate the index.
    pub async fn load_committed_checkpoint(
        &self,
        reference: &CommittedCheckpointRef,
    ) -> Result<CommittedCheckpointIndex, ClusterCheckpointAuthorityError> {
        CheckpointDecisionStore::new(Arc::clone(&self.store))
            .load_committed_checkpoint(reference)
            .await
            .map_err(Into::into)
    }

    /// Read one live cluster outcome together with its committed checkpoint index.
    /// Commit always returns a validated index; Abort returns `None`.
    ///
    /// # Errors
    ///
    /// Fails when the authority history or selected committed index is unavailable or invalid.
    pub async fn cluster_outcome_with_committed_checkpoint(
        &self,
        epoch: u64,
    ) -> Result<
        Option<(CheckpointOutcome, Option<CommittedCheckpointIndex>)>,
        ClusterCheckpointAuthorityError,
    > {
        let decisions = CheckpointDecisionStore::new(Arc::clone(&self.store));
        let Some(outcome) = self.stable_cluster_outcome(epoch).await? else {
            return Ok(None);
        };
        let index = if outcome.is_commit() {
            let reference = outcome.committed_checkpoint.as_ref().ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "cluster Commit epoch {} checkpoint {} has no committed checkpoint index",
                    outcome.epoch, outcome.checkpoint_id
                ))
            })?;
            let index = decisions.load_committed_checkpoint(reference).await?;
            if index.epoch != outcome.epoch
                || index.checkpoint_id != outcome.checkpoint_id
                || index.scope != outcome.scope
                || index.assignment_fence.as_ref() != outcome.assignment_fence.as_ref()
                || index.deployment_id != outcome.deployment_id
            {
                return Err(DecisionError::Conflict(format!(
                    "cluster Commit epoch {} checkpoint {} does not match committed checkpoint '{}'",
                    outcome.epoch, outcome.checkpoint_id, reference.sha256
                ))
                .into());
            }
            Some(index)
        } else {
            None
        };
        Ok(Some((outcome, index)))
    }

    /// Audit and return every live cluster outcome in ascending epoch order.
    ///
    /// # Errors
    ///
    /// Fails when the durable authority history is unavailable or invalid.
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
    ///
    /// # Errors
    ///
    /// Fails when the durable authority history is unavailable or invalid.
    pub async fn cluster_outcomes(
        &self,
    ) -> Result<Vec<CheckpointOutcome>, ClusterCheckpointAuthorityError> {
        Ok(self.cluster_outcome_inventory().await?.outcomes)
    }

    /// Greatest live cluster commit recovery cut.
    ///
    /// # Errors
    ///
    /// Fails when the durable authority history is unavailable or invalid.
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
    ///
    /// # Errors
    ///
    /// Fails when the durable authority history is unavailable or invalid.
    pub async fn highest_cluster_terminal_outcome(
        &self,
    ) -> Result<Option<CheckpointOutcome>, ClusterCheckpointAuthorityError> {
        Ok(self.audited_cluster_outcomes().await?.1.last().cloned())
    }

    /// Return the exact immutable outcome for `attempt`, or the first audited terminal outcome
    /// known to close that older checkpoint. Compacted continuity anchors are included in the
    /// audit.
    ///
    /// # Errors
    /// Returns an error for a noncanonical attempt identity or an unavailable or invalid durable
    /// authority chain.
    pub async fn cluster_attempt_settlement(
        &self,
        attempt: crate::checkpoint::CheckpointAttempt,
    ) -> Result<Option<CheckpointOutcome>, ClusterCheckpointAuthorityError> {
        if !attempt.is_canonical() {
            return Err(DecisionError::Conflict(
                "cluster checkpoint settlement requires one nonzero canonical checkpoint ID".into(),
            )
            .into());
        }
        let outcomes = self.audited_cluster_outcomes().await?.1;
        if let Ok(index) = outcomes.binary_search_by_key(&attempt.epoch, |outcome| outcome.epoch) {
            return Ok(Some(outcomes[index].clone()));
        }
        let Some(highest) = outcomes.last() else {
            return Ok(None);
        };
        if highest.checkpoint_id > attempt.checkpoint_id {
            Ok(Some(highest.clone()))
        } else {
            Ok(None)
        }
    }

    fn cleanup_participant_ids(
        index: &CommittedCheckpointIndex,
    ) -> Result<Vec<u64>, ClusterCheckpointAuthorityError> {
        index.validate().map_err(DecisionError::Conflict)?;
        let participant_ids = index
            .participants
            .iter()
            .map(|participant| participant.participant_id)
            .collect::<Vec<_>>();
        if participant_ids.is_empty()
            || participant_ids.len() > MAX_CHECKPOINT_PARTICIPANTS
            || participant_ids[0] == 0
            || !participant_ids.windows(2).all(|pair| pair[0] < pair[1])
        {
            return Err(DecisionError::Conflict(
                "committed checkpoint participants are not canonical".into(),
            )
            .into());
        }
        Ok(participant_ids)
    }

    fn cleanup_stop_before(head: &LeaderAuthorityRecord) -> Option<CommittedCheckpointRef> {
        head.outcome_floor
            .as_ref()
            .filter(|floor| floor.artifact_before_epoch != 0)
            .and_then(|floor| floor.committed_anchor.as_ref())
            .and_then(|outcome| outcome.committed_checkpoint.clone())
    }

    /// Read the exact cluster checkpoint artifact cleanup position.
    ///
    /// The returned cursor was admitted through the shared leader-authority sequence. `None`
    /// means no destructive checkpoint cleanup is currently authorized.
    ///
    /// # Errors
    /// Fails when the durable authority head is unavailable or invalid.
    pub async fn cluster_artifact_cleanup(
        &self,
    ) -> Result<Option<ClusterArtifactCleanupCursor>, ClusterCheckpointAuthorityError> {
        Ok(self
            .load_record()
            .await?
            .and_then(|head| head.artifact_cleanup))
    }

    /// Atomically advance the artifact floor and authorize its first expired checkpoint.
    ///
    /// `protected` must name a live cluster Commit. Its exact predecessor becomes the first
    /// cleanup target. An already covered horizon or a protected genesis cut returns `None`.
    /// An identical retry returns the active cursor; a different active segment is rejected.
    ///
    /// # Errors
    /// Fails for a stale proof, invalid committed-index chain, failed protected-artifact
    /// validation, concurrent cleanup segment, or object-store failure.
    pub async fn begin_cluster_artifact_cleanup<V, Fut>(
        &self,
        proof: &LeaderProof,
        protected: CommittedCheckpointRef,
        validate_artifacts: V,
    ) -> Result<Option<ClusterArtifactCleanupCursor>, ClusterCheckpointAuthorityError>
    where
        V: Fn(CheckpointOutcome) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), String>> + Send + 'static,
    {
        if !proof.is_canonical() {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        protected.validate().map_err(DecisionError::Conflict)?;
        loop {
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let current = &published.record;
            if !current.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            if let Some(active) = current.artifact_cleanup.as_ref() {
                if active.protected == protected {
                    return Ok(Some(active.clone()));
                }
                return Err(DecisionError::Conflict(format!(
                    "cluster artifact cleanup for protected checkpoint {} is still active",
                    active.protected.checkpoint_id
                ))
                .into());
            }
            if current.assignment_handoff_pin.as_ref().is_some_and(|pin| {
                protected.epoch > pin.checkpoint.epoch
                    && protected.checkpoint_id > pin.checkpoint.checkpoint_id
            }) {
                return Ok(None);
            }
            if current
                .outcome_floor
                .as_ref()
                .is_some_and(|floor| floor.artifact_before_epoch >= protected.epoch)
            {
                return Ok(None);
            }

            let snapshot = self.cached_audited_cluster_outcomes_from(current).await?;
            let protected_outcome = snapshot
                .outcomes
                .iter()
                .find(|outcome| {
                    outcome.is_commit() && outcome.committed_checkpoint.as_ref() == Some(&protected)
                })
                .cloned()
                .ok_or_else(|| {
                    DecisionError::Conflict(format!(
                        "protected checkpoint {} is not a live cluster Commit",
                        protected.checkpoint_id
                    ))
                })?;
            let decisions = CheckpointDecisionStore::new(Arc::clone(&self.store));
            let protected_index = decisions
                .validate_committed_checkpoint_for_outcome(&protected_outcome)
                .await?;
            validate_artifacts(protected_outcome.clone())
                .await
                .map_err(|error| {
                    DecisionError::Conflict(format!(
                        "protected cluster checkpoint {} failed durable artifact preflight: {error}",
                        protected.checkpoint_id
                    ))
                })?;
            let Some(expired) = protected_index.predecessor.clone() else {
                return Ok(None);
            };
            let expired_index = decisions.load_committed_checkpoint(&expired).await?;
            protected_index
                .validate_predecessor_index(&expired_index)
                .map_err(DecisionError::Conflict)?;

            let authority_before_epoch = current
                .outcome_floor
                .as_ref()
                .map_or(protected.epoch, |floor| {
                    floor.authority_before_epoch.max(protected.epoch)
                });
            let floor = self
                .build_cluster_outcome_floor(
                    current,
                    &snapshot,
                    protected.epoch,
                    authority_before_epoch,
                )
                .await?;
            if floor
                .committed_anchor
                .as_ref()
                .and_then(|outcome| outcome.committed_checkpoint.as_ref())
                != Some(&expired)
            {
                return Err(DecisionError::Conflict(
                    "protected checkpoint predecessor is not the artifact-floor Commit anchor"
                        .into(),
                )
                .into());
            }
            let cursor = ClusterArtifactCleanupCursor {
                protected: protected.clone(),
                current: expired,
                next: expired_index.predecessor.clone(),
                stop_before: Self::cleanup_stop_before(current),
                participant_ids: Self::cleanup_participant_ids(&expired_index)?,
                phase: ClusterArtifactCleanupPhase::DeleteData,
            };
            cursor.validate()?;

            // Artifact preflight and index reads may race harmless authority mutations. Publish
            // only from the exact outcome/floor/cursor snapshot used to derive this segment.
            let rechecked = self
                .load_published_authority_head()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            if !rechecked.record.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            if rechecked.record.outcome_head != current.outcome_head
                || rechecked.record.commit_head != current.commit_head
                || rechecked.record.outcome_floor != current.outcome_floor
                || rechecked.record.artifact_cleanup != current.artifact_cleanup
            {
                tokio::task::yield_now().await;
                continue;
            }
            let sequence =
                rechecked.record.lease.seq.checked_add(1).ok_or_else(|| {
                    LeaseError::Invalid("leader authority sequence exhausted".into())
                })?;
            let mut lease = rechecked.record.lease.clone();
            lease.seq = sequence;
            let mut next = rechecked.record.preserve_with_lease(lease);
            next.outcome_floor = Some(floor.clone());
            next.artifact_cleanup = Some(cursor.clone());
            next.validate()?;
            match self
                .create_authority_record(Some(&rechecked), &next)
                .await?
            {
                AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                    self.install_cluster_outcome_audit(
                        Self::cluster_outcome_audit_key(&next),
                        next.lease.seq,
                        Self::outcomes_retained_by_floor(&floor, &snapshot),
                    );
                    return Ok(Some(cursor));
                }
                AuthorityCreateOutcome::Contended(winner) => {
                    if !winner.lease.matches_proof(proof) {
                        return Err(ClusterCheckpointAuthorityError::Fenced);
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    /// Durably authorize metadata deletion after the exact current data cleanup completes.
    ///
    /// # Errors
    /// Fails for a stale proof, stale or non-`DeleteData` cursor, or object-store failure.
    pub async fn mark_cluster_artifact_data_deleted(
        &self,
        proof: &LeaderProof,
        expected: &ClusterArtifactCleanupCursor,
    ) -> Result<ClusterArtifactCleanupCursor, ClusterCheckpointAuthorityError> {
        if !proof.is_canonical() {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        expected.validate()?;
        if expected.phase != ClusterArtifactCleanupPhase::DeleteData {
            return Err(DecisionError::Conflict(
                "cluster artifact data transition requires a DeleteData cursor".into(),
            )
            .into());
        }
        let mut replacement = expected.clone();
        replacement.phase = ClusterArtifactCleanupPhase::DeleteMetadata;
        loop {
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let current = &published.record;
            if !current.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            if current.artifact_cleanup.as_ref() == Some(&replacement) {
                return Ok(replacement);
            }
            if current.artifact_cleanup.as_ref() != Some(expected) {
                return Err(DecisionError::Conflict(
                    "cluster artifact cleanup cursor changed before data completion".into(),
                )
                .into());
            }
            let sequence =
                current.lease.seq.checked_add(1).ok_or_else(|| {
                    LeaseError::Invalid("leader authority sequence exhausted".into())
                })?;
            let mut lease = current.lease.clone();
            lease.seq = sequence;
            let mut next = current.preserve_with_lease(lease);
            next.artifact_cleanup = Some(replacement.clone());
            next.validate()?;
            match self
                .create_authority_record(Some(&published), &next)
                .await?
            {
                AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                    return Ok(replacement);
                }
                AuthorityCreateOutcome::Contended(winner) => {
                    if !winner.lease.matches_proof(proof) {
                        return Err(ClusterCheckpointAuthorityError::Fenced);
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    /// Complete metadata deletion and authorize the exact next expired checkpoint, if any.
    ///
    /// The transition clears the journal at genesis or immediately before the previously
    /// reclaimed stop boundary. Otherwise it loads the immutable next index and records its exact
    /// predecessor and participants before any deletion of that target is allowed.
    ///
    /// # Errors
    /// Fails for a stale proof, stale or non-`DeleteMetadata` cursor, a broken predecessor chain,
    /// or object-store failure.
    pub async fn mark_cluster_artifact_metadata_deleted(
        &self,
        proof: &LeaderProof,
        expected: &ClusterArtifactCleanupCursor,
    ) -> Result<Option<ClusterArtifactCleanupCursor>, ClusterCheckpointAuthorityError> {
        if !proof.is_canonical() {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        expected.validate()?;
        if expected.phase != ClusterArtifactCleanupPhase::DeleteMetadata {
            return Err(DecisionError::Conflict(
                "cluster artifact metadata transition requires a DeleteMetadata cursor".into(),
            )
            .into());
        }
        loop {
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let current = &published.record;
            if !current.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            if current.artifact_cleanup.as_ref() != Some(expected) {
                let completed = expected.next.is_none()
                    || expected.next.as_ref() == expected.stop_before.as_ref();
                if completed && current.artifact_cleanup.is_none() {
                    return Ok(None);
                }
                if let (Some(next_reference), Some(actual)) =
                    (expected.next.as_ref(), current.artifact_cleanup.as_ref())
                {
                    if actual.phase == ClusterArtifactCleanupPhase::DeleteData
                        && actual.protected == expected.protected
                        && &actual.current == next_reference
                        && actual.stop_before == expected.stop_before
                    {
                        return Ok(Some(actual.clone()));
                    }
                }
                return Err(DecisionError::Conflict(
                    "cluster artifact cleanup cursor changed before metadata completion".into(),
                )
                .into());
            }

            let replacement = match expected.next.as_ref() {
                None => None,
                Some(next_reference) if Some(next_reference) == expected.stop_before.as_ref() => {
                    None
                }
                Some(next_reference) => {
                    let decisions = CheckpointDecisionStore::new(Arc::clone(&self.store));
                    let index = decisions.load_committed_checkpoint(next_reference).await?;
                    if index.scope != CheckpointScope::Cluster {
                        return Err(DecisionError::Conflict(
                            "cluster artifact cleanup next checkpoint is not cluster-scoped".into(),
                        )
                        .into());
                    }
                    let replacement = ClusterArtifactCleanupCursor {
                        protected: expected.protected.clone(),
                        current: next_reference.clone(),
                        next: index.predecessor.clone(),
                        stop_before: expected.stop_before.clone(),
                        participant_ids: Self::cleanup_participant_ids(&index)?,
                        phase: ClusterArtifactCleanupPhase::DeleteData,
                    };
                    replacement.validate()?;
                    Some(replacement)
                }
            };

            let sequence =
                current.lease.seq.checked_add(1).ok_or_else(|| {
                    LeaseError::Invalid("leader authority sequence exhausted".into())
                })?;
            let mut lease = current.lease.clone();
            lease.seq = sequence;
            let mut next = current.preserve_with_lease(lease);
            next.artifact_cleanup = replacement.clone();
            next.validate()?;
            match self
                .create_authority_record(Some(&published), &next)
                .await?
            {
                AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                    return Ok(replacement);
                }
                AuthorityCreateOutcome::Contended(winner) => {
                    if !winner.lease.matches_proof(proof) {
                        return Err(ClusterCheckpointAuthorityError::Fenced);
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    /// Exact continuity boundary for cluster outcomes compacted from the authority history.
    ///
    /// # Errors
    ///
    /// Fails when the durable authority history is unavailable or invalid.
    pub async fn cluster_outcome_retention_boundary(
        &self,
    ) -> Result<ClusterOutcomeRetentionBoundary, ClusterCheckpointAuthorityError> {
        let head = self.load_record().await?;
        Ok(ClusterOutcomeRetentionBoundary::from_floor(
            head.as_ref().and_then(|head| head.outcome_floor.as_ref()),
        ))
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
        Box::pin(self.acquire_or_renew_current_term_for_test_inner(
            owner,
            now_ms,
            SameOwnerToken::Rotate,
        ))
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
        Box::pin(self.acquire_or_renew_current_term_for_test_inner(
            owner,
            now_ms,
            SameOwnerToken::Exact(token),
        ))
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
            let published = self.load_published_authority_head().await?;
            let candidate = match published.as_ref() {
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
                Some(head) if head.record.lease.owner == *owner => {
                    let record = &head.record;
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
                Some(head) => {
                    if matches!(same_owner_token, SameOwnerToken::Exact(_)) {
                        return Err(LeaseError::Fenced(
                            "exact leader renewal was superseded by a rival owner".into(),
                        ));
                    }
                    return Ok(LeaseOutcome::Held(head.record.lease.clone()));
                }
            };
            match self
                .create_authority_record(published.as_ref(), &candidate)
                .await?
            {
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
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or_else(|| LeaseError::Invalid("observed leader lease disappeared".into()))?;
            let current = &published.record;
            if !current.lease.has_same_liveness_identity(&observation.lease) {
                return Ok(LeaseOutcome::Held(current.lease.clone()));
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
            match self
                .create_authority_record(Some(&published), &candidate)
                .await?
            {
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
    deadline_tx: watch::Sender<Arc<LeaseDeadline>>,
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
struct LeaderLeaseExitGuard {
    shutdown: tokio_util::sync::CancellationToken,
    unexpected_exit: tokio_util::sync::CancellationToken,
    lease_tx: watch::Sender<Option<LeaderLease>>,
    deadline_tx: watch::Sender<Arc<LeaseDeadline>>,
}

#[cfg(feature = "cluster")]
impl Drop for LeaderLeaseExitGuard {
    fn drop(&mut self) {
        self.deadline_tx.borrow().fence();
        self.lease_tx.send_replace(None);
        if !self.shutdown.is_cancelled() {
            tracing::error!(
                "leader lease manager exited without intentional shutdown; fencing process authority"
            );
            self.unexpected_exit.cancel();
        }
    }
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
        let (deadline_tx, _deadline_rx) = watch::channel(Arc::new(LeaseDeadline::uninitialized()));
        Ok(Self {
            store,
            owner,
            config,
            lease_tx,
            deadline_tx,
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

    /// Snapshot the current local-monotonic deadline generation.
    ///
    /// This is useful before the manager starts and in fixed-generation tests. A running manager
    /// may replace it after any discontinuous grant; runtime consumers must subscribe through
    /// [`Self::deadline_watch`].
    #[must_use]
    pub fn deadline(&self) -> Arc<LeaseDeadline> {
        self.deadline_tx.borrow().clone()
    }

    /// Subscribe to generation-scoped local leader deadlines.
    ///
    /// A discontinuous grant permanently fences the previous deadline and publishes a fresh
    /// inactive one before another durable fencing term can become locally authoritative.
    #[must_use]
    pub fn deadline_watch(&self) -> watch::Receiver<Arc<LeaseDeadline>> {
        self.deadline_tx.subscribe()
    }

    #[cfg(feature = "cluster")]
    fn withdraw(&self) -> bool {
        let deadline = self.deadline_tx.borrow().clone();
        let had_grant = self.lease_tx.borrow().is_some() || deadline.is_live();
        // Always rotate the generation. An acquisition may have crossed its deadline after the
        // durable response but before local publication, leaving an expired, never-published
        // deadline that `is_live()` cannot distinguish from a fresh inactive one.
        deadline.fence();
        self.lease_tx.send_replace(None);
        self.deadline_tx
            .send_replace(Arc::new(LeaseDeadline::uninitialized()));
        had_grant
    }

    #[cfg(feature = "cluster")]
    fn fence(&self) {
        self.deadline_tx.borrow().fence();
        self.lease_tx.send_replace(None);
    }

    #[cfg(feature = "cluster")]
    fn withdraw_for_new_term(
        &self,
        ticker: &mut tokio::time::Interval,
        valid_until: &mut Option<tokio::time::Instant>,
        observation: &mut Option<LeaderLeaseObservation>,
        held_token: &mut Option<u64>,
        rotate_not_before: &mut Option<tokio::time::Instant>,
    ) {
        // Permanently fence the process-local grant before forgetting its fencing token. A later
        // acquisition runs through `begin_new_term` and a fresh deadline generation, so neither
        // half of the expired proof can become live again.
        let had_grant = self.withdraw();
        *valid_until = None;
        *observation = None;
        *held_token = None;
        let now = tokio::time::Instant::now();
        if had_grant {
            let retry_at = now
                .checked_add(self.config.ttl)
                .unwrap_or_else(tokio::time::Instant::now);
            // An old-proof checkpoint tail is still allowed to win the authority CAS while no
            // newer term exists. Preserve one full TTL for that bounded settlement before the
            // same process rotates the durable token.
            if rotate_not_before.is_none_or(|current| retry_at > current) {
                *rotate_not_before = Some(retry_at);
            }
        }
        if let Some(retry_at) = *rotate_not_before {
            if retry_at > now {
                ticker.reset_at(retry_at);
            } else {
                *rotate_not_before = None;
                ticker.reset_immediately();
            }
        } else {
            ticker.reset_immediately();
        }
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
                Box::pin(
                    self.store
                        .try_takeover(&self.owner, observation, now_millis()),
                )
                .await
            } else if let Some(token) = held_token {
                Box::pin(self.store.renew_exact(&self.owner, token, now_millis())).await
            } else {
                Box::pin(self.store.begin_new_term(&self.owner, now_millis())).await
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
        let mut rotate_not_before = None;
        let mut candidacy_generation = candidate.borrow().generation;

        loop {
            let candidacy = *candidate.borrow_and_update();
            if candidacy.generation != candidacy_generation {
                self.withdraw_for_new_term(
                    &mut ticker,
                    &mut valid_until,
                    &mut observation,
                    &mut held_token,
                    &mut rotate_not_before,
                );
                candidacy_generation = candidacy.generation;
            }
            if !candidacy.eligible {
                self.withdraw_for_new_term(
                    &mut ticker,
                    &mut valid_until,
                    &mut observation,
                    &mut held_token,
                    &mut rotate_not_before,
                );
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
                    tracing::warn!(
                        owner = ?self.owner,
                        "leader lease local deadline expired; withdrawing and rotating the fencing token"
                    );
                    self.withdraw_for_new_term(
                        &mut ticker,
                        &mut valid_until,
                        &mut observation,
                        &mut held_token,
                        &mut rotate_not_before,
                    );
                    continue;
                }
                _ = ticker.tick() => {}
            }

            let (result, attempt_valid_until) = match Box::pin(self.attempt_lease(
                &shutdown,
                &mut candidate,
                valid_until,
                observation.as_ref(),
                held_token,
            ))
            .await
            {
                LeaseOperationEvent::Shutdown => {
                    self.fence();
                    return;
                }
                LeaseOperationEvent::Deadline => {
                    tracing::warn!(
                        owner = ?self.owner,
                        "leader lease operation exceeded its local deadline; withdrawing and rotating the fencing token"
                    );
                    self.withdraw_for_new_term(
                        &mut ticker,
                        &mut valid_until,
                        &mut observation,
                        &mut held_token,
                        &mut rotate_not_before,
                    );
                    continue;
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
                        tracing::warn!(
                            owner = ?self.owner,
                            "leader lease response arrived after its local deadline; withdrawing and rotating the fencing token"
                        );
                        self.withdraw_for_new_term(
                            &mut ticker,
                            &mut valid_until,
                            &mut observation,
                            &mut held_token,
                            &mut rotate_not_before,
                        );
                        continue;
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
                        tracing::warn!(
                            owner = ?self.owner,
                            "leader lease publication crossed its local deadline; withdrawing and rotating the fencing token"
                        );
                        self.withdraw_for_new_term(
                            &mut ticker,
                            &mut valid_until,
                            &mut observation,
                            &mut held_token,
                            &mut rotate_not_before,
                        );
                        continue;
                    }
                    observation = None;
                    valid_until = Some(attempt_valid_until);
                    held_token = Some(lease.token);
                    rotate_not_before = None;
                    let deadline = self.deadline_tx.borrow().clone();
                    deadline.extend_until(attempt_valid_until.into_std());
                    if !deadline.is_live() {
                        tracing::warn!(
                            owner = ?self.owner,
                            "leader lease deadline expired during local publication; withdrawing and rotating the fencing token"
                        );
                        self.withdraw_for_new_term(
                            &mut ticker,
                            &mut valid_until,
                            &mut observation,
                            &mut held_token,
                            &mut rotate_not_before,
                        );
                        continue;
                    }
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
    /// manager can contend again later. A missed local deadline withdraws the grant and rotates
    /// its fencing token before reacquisition. Shutdown or an invalid lease outcome terminally
    /// fences the manager.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn spawn(
        self,
        shutdown: tokio_util::sync::CancellationToken,
        candidate: watch::Receiver<LeaderCandidacy>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(self.run(shutdown, candidate))
    }

    /// Spawn the renewal loop and signal any exit not preceded by intentional shutdown.
    ///
    /// The guard lives inside the same task as the manager future, so panic and forced task abort
    /// cannot detach a still-running inner lease writer.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn spawn_supervised(
        self,
        shutdown: tokio_util::sync::CancellationToken,
        candidate: watch::Receiver<LeaderCandidacy>,
        unexpected_exit: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let run_shutdown = shutdown.clone();
            let _exit_guard = LeaderLeaseExitGuard {
                shutdown,
                unexpected_exit,
                lease_tx: self.lease_tx.clone(),
                deadline_tx: self.deadline_tx.clone(),
            };
            self.run(run_shutdown, candidate).await;
        })
    }
}

#[cfg(test)]
mod tests;
