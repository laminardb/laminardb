//! Durable, append-only leader fencing.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::checkpoint::{
    AssignmentDrainId, AssignmentDrainTransition, CheckpointAssignmentFence, LeaderProof,
    LeaderProofOwner, RecoveryCapsuleRef,
};
use crate::checkpoint_decision::{
    CheckpointDecisionStore, CheckpointOutcome, CheckpointScope, CheckpointVerdict, DecisionError,
    RecordOutcomeResult,
};
use crate::cluster::discovery::NodeId;

use super::catalog_manifest::{
    CatalogManifest, CatalogManifestError, CatalogManifestRef, CatalogSealOutcome,
};
use super::lease_deadline::LeaseDeadline;
use super::process_lease::ProcessLease;

const LEASE_PREFIX: &str = "control/leader-lease/";
const AUTHORITY_RECORD_VERSION: u32 = 4;
const MAX_AUTHORITY_RECORD_BYTES: u64 = 256 * 1024;
const MAX_LEASE_HEAD_READ_ATTEMPTS: usize = 4;
const MAX_LIVE_AUTHORITY_LINKS: usize = 4096;
const LEADER_LEASE_HISTORY_TO_RETAIN: usize = 2;
const LEADER_LEASE_PRUNE_BATCH_RECORDS: usize = 256;
const LEADER_LEASE_MAX_PRUNE_BATCHES: usize = 4;
const LEADER_LEASE_PRUNE_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const MAX_TEST_LEADER_LEASE_RECORDS: usize = 4096;

fn lease_path(sequence: u64) -> OsPath {
    OsPath::from(format!("{LEASE_PREFIX}v{sequence:016}.json"))
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

fn encode_authority_record(record: &LeaderAuthorityRecord) -> Result<Bytes, LeaseError> {
    record.validate()?;
    let encoded = serde_json::to_vec(record)?;
    if encoded.is_empty()
        || u64::try_from(encoded.len()).unwrap_or(u64::MAX) > MAX_AUTHORITY_RECORD_BYTES
    {
        return Err(LeaseError::Invalid(format!(
            "encoded leader authority record is {} bytes; maximum is {MAX_AUTHORITY_RECORD_BYTES}",
            encoded.len()
        )));
    }
    Ok(Bytes::from(encoded))
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
    /// Fencing token, stable across renewals and advanced on takeover.
    pub token: u64,
    /// Exact process incarnation holding the lease.
    pub owner: LeaderLeaseOwner,
    /// Owner-written wall-clock expiry for diagnostics only.
    pub expires_at_ms: i64,
    /// Immutable catalog content reference, once sealed for this control namespace.
    pub catalog_manifest: Option<CatalogManifestRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutcomeLink {
    sequence: u64,
    epoch: u64,
    checkpoint_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AssignmentDrainDecisionLink {
    sequence: u64,
    target_version: u64,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorityOutcomeFloor {
    deployment_id: String,
    before_epoch: u64,
    terminal_anchor: Option<CheckpointOutcome>,
    terminal_anchor_link: Option<OutcomeLink>,
    committed_anchor: Option<CheckpointOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorityAssignmentDrainFloor {
    before_target_version: u64,
    terminal_anchor: Option<AssignmentDrainDecision>,
    terminal_anchor_link: Option<AssignmentDrainDecisionLink>,
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
    /// Monotonic cluster outcome retention boundary and its continuity anchors.
    outcome_floor: Option<AuthorityOutcomeFloor>,
    /// Present only on the sequence that admitted this assignment-drain decision.
    assignment_drain_decision: Option<AssignmentDrainDecision>,
    /// Link to the preceding drain decision, present only on a decision-bearing record.
    previous_assignment_drain_decision: Option<AssignmentDrainDecisionLink>,
    /// Latest admitted drain decision. Every other authority mutation preserves it.
    assignment_drain_decision_head: Option<AssignmentDrainDecisionLink>,
    /// Monotonic assignment-drain retention boundary and its continuity anchor.
    assignment_drain_floor: Option<AuthorityAssignmentDrainFloor>,
}

impl LeaderLease {
    fn validate(&self) -> Result<(), LeaseError> {
        self.owner.validate()?;
        if self.seq == 0 || self.token == 0 {
            return Err(LeaseError::Invalid(
                "leader lease sequence and token must be nonzero".into(),
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
}

impl AuthorityOutcomeFloor {
    fn validate(&self) -> Result<(), LeaseError> {
        let deployment = Uuid::parse_str(&self.deployment_id)
            .map_err(|error| LeaseError::Invalid(format!("outcome floor deployment: {error}")))?;
        if deployment.is_nil()
            || deployment.to_string() != self.deployment_id
            || self.before_epoch == 0
        {
            return Err(LeaseError::Invalid(
                "outcome floor requires a canonical deployment and nonzero horizon".into(),
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
                || anchor.epoch >= self.before_epoch
            {
                return Err(LeaseError::Invalid(format!(
                    "outcome floor has an invalid {name} anchor"
                )));
            }
        }
        if let Some(committed) = self.committed_anchor.as_ref() {
            if !committed.is_commit() {
                return Err(LeaseError::Invalid(
                    "outcome floor committed anchor is not a commit".into(),
                ));
            }
            let terminal = self.terminal_anchor.as_ref().ok_or_else(|| {
                LeaseError::Invalid(
                    "outcome floor has a committed anchor without a terminal anchor".into(),
                )
            })?;
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
        }
        if self
            .terminal_anchor
            .as_ref()
            .is_some_and(CheckpointOutcome::is_commit)
            && self.committed_anchor != self.terminal_anchor
        {
            return Err(LeaseError::Invalid(
                "outcome floor does not retain its terminal commit as committed anchor".into(),
            ));
        }
        match (self.terminal_anchor.as_ref(), self.terminal_anchor_link) {
            (Some(anchor), Some(link))
                if link.sequence != 0
                    && link.epoch == anchor.epoch
                    && link.checkpoint_id == anchor.checkpoint_id => {}
            (None, None) => {}
            _ => {
                return Err(LeaseError::Invalid(
                    "outcome floor terminal anchor does not match its exact authority link".into(),
                ));
            }
        }
        Ok(())
    }
}

impl AuthorityAssignmentDrainFloor {
    fn validate(&self) -> Result<(), LeaseError> {
        if self.before_target_version == 0 {
            return Err(LeaseError::Invalid(
                "assignment drain floor requires a nonzero target version".into(),
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
                        "assignment drain floor anchor does not match its exact authority link"
                            .into(),
                    ));
                }
            }
            (None, None) => {}
            _ => {
                return Err(LeaseError::Invalid(
                    "assignment drain floor has an incomplete terminal anchor".into(),
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
            outcome_floor: None,
            assignment_drain_decision: None,
            previous_assignment_drain_decision: None,
            assignment_drain_decision_head: None,
            assignment_drain_floor: None,
        }
    }

    fn preserve_with_lease(&self, lease: LeaderLease) -> Self {
        Self {
            version: AUTHORITY_RECORD_VERSION,
            lease,
            checkpoint_outcome: None,
            previous_outcome: None,
            outcome_head: self.outcome_head,
            outcome_floor: self.outcome_floor.clone(),
            assignment_drain_decision: None,
            previous_assignment_drain_decision: None,
            assignment_drain_decision_head: self.assignment_drain_decision_head,
            assignment_drain_floor: self.assignment_drain_floor.clone(),
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
        if let Some(floor) = &self.assignment_drain_floor {
            floor.validate()?;
        }
        if self.checkpoint_outcome.is_some() && self.assignment_drain_decision.is_some() {
            return Err(LeaseError::Invalid(
                "one authority sequence cannot admit two decision domains".into(),
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
                if !self.lease.matches_proof(proof)
                    || self.outcome_head
                        != Some(OutcomeLink {
                            sequence: self.lease.seq,
                            epoch: outcome.epoch,
                            checkpoint_id: outcome.checkpoint_id,
                        })
                {
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
                if let Some(floor) = &self.outcome_floor {
                    if outcome.deployment_id != floor.deployment_id
                        || outcome.epoch < floor.before_epoch
                    {
                        return Err(LeaseError::Invalid(
                            "cluster outcome is below or outside its durable authority floor"
                                .into(),
                        ));
                    }
                }
            }
            None => {
                if self.previous_outcome.is_some() {
                    return Err(LeaseError::Invalid(
                        "non-outcome authority record carries a previous-outcome link".into(),
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
        match self.assignment_drain_decision.as_ref() {
            Some(decision) => {
                decision.validate()?;
                if self
                    .assignment_drain_floor
                    .as_ref()
                    .is_some_and(|floor| decision.target_version() < floor.before_target_version)
                {
                    return Err(LeaseError::Invalid(
                        "assignment drain decision is below its durable authority floor".into(),
                    ));
                }
                if !self.lease.matches_proof(&decision.leader_proof)
                    || self.assignment_drain_decision_head
                        != Some(AssignmentDrainDecisionLink {
                            sequence: self.lease.seq,
                            target_version: decision.target_version(),
                        })
                {
                    return Err(LeaseError::Invalid(
                        "assignment drain decision is not bound to its exact authority sequence and term"
                            .into(),
                    ));
                }
                if let Some(previous) = self.previous_assignment_drain_decision {
                    if previous.sequence >= self.lease.seq
                        || previous.target_version >= decision.target_version()
                    {
                        return Err(LeaseError::Invalid(
                            "assignment drain decision link does not move backward".into(),
                        ));
                    }
                }
            }
            None => {
                if self.previous_assignment_drain_decision.is_some() {
                    return Err(LeaseError::Invalid(
                        "non-drain authority record carries a previous drain-decision link".into(),
                    ));
                }
                if self
                    .assignment_drain_decision_head
                    .is_some_and(|head| head.sequence > self.lease.seq || head.target_version == 0)
                {
                    return Err(LeaseError::Invalid(
                        "authority drain-decision head is outside the durable sequence".into(),
                    ));
                }
            }
        }
        if let Some(floor) = self.assignment_drain_floor.as_ref() {
            if floor
                .terminal_anchor_link
                .is_some_and(|link| link.sequence >= self.lease.seq)
            {
                return Err(LeaseError::Invalid(
                    "assignment drain floor anchor is outside the authority sequence".into(),
                ));
            }
            if let Some(head) = self.assignment_drain_decision_head {
                if head.target_version < floor.before_target_version
                    && Some(head) != floor.terminal_anchor_link
                {
                    return Err(LeaseError::Invalid(
                        "authority drain-decision head does not meet its durable floor".into(),
                    ));
                }
            } else if floor.terminal_anchor_link.is_some() {
                return Err(LeaseError::Invalid(
                    "assignment drain floor anchor is disconnected from the authority head".into(),
                ));
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
    Contended(LeaderAuthorityRecord),
}

/// Candidate-local proof that one exact rival record remained current for a full TTL.
#[derive(Debug)]
pub struct LeaderLeaseObservation {
    lease: LeaderLease,
    started: Instant,
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
    /// Outcomes below this epoch are continuity-only.
    pub before_epoch: u64,
    /// Greatest committed outcome compacted below the horizon.
    pub committed_anchor: Option<CheckpointOutcome>,
    /// Greatest terminal outcome compacted below the horizon, including aborts.
    pub terminal_anchor: Option<CheckpointOutcome>,
}

/// Append-only object-store authority for the cluster leader.
pub struct LeaderLeaseStore {
    store: Arc<dyn ObjectStore>,
    ttl_ms: i64,
    prune_running: Arc<AtomicBool>,
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
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, ttl_ms: i64) -> Self {
        Self {
            store,
            ttl_ms,
            prune_running: Arc::new(AtomicBool::new(false)),
        }
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

    async fn newest_sequences(&self, retain: usize) -> Result<Vec<u64>, LeaseError> {
        debug_assert!(retain > 0);
        let prefix = OsPath::from(LEASE_PREFIX);
        let mut entries = self.store.list(Some(&prefix));
        let mut sequences = Vec::with_capacity(retain);
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|error| LeaseError::Io(error.to_string()))?;
            let sequence = lease_sequence_from_path(&entry.location)?;
            match sequences.binary_search(&sequence) {
                Ok(_) => {}
                Err(index) if sequences.len() < retain => sequences.insert(index, sequence),
                Err(index) if index > 0 => {
                    sequences.remove(0);
                    let insertion = sequences.binary_search(&sequence).unwrap_err();
                    sequences.insert(insertion, sequence);
                }
                Err(_) => {}
            }
        }
        Ok(sequences)
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
        let newest = authority
            .newest_sequences(LEADER_LEASE_HISTORY_TO_RETAIN)
            .await?;
        let Some(head_sequence) = newest.last().copied() else {
            return Ok(());
        };
        let head = read_authority_record(store.as_ref(), head_sequence)
            .await?
            .ok_or_else(|| LeaseError::Io("leader authority head vanished during prune".into()))?;
        let mut retained: BTreeSet<u64> = newest.into_iter().collect();
        let floor = head
            .outcome_floor
            .as_ref()
            .map_or(0, |floor| floor.before_epoch);
        let mut link = head.outcome_head;
        let mut outcome_links = 0;
        while let Some(current) = link {
            if !consume_live_authority_link(&mut outcome_links) {
                return Err(LeaseError::Invalid(format!(
                    "live outcome retention exceeds the fixed {MAX_LIVE_AUTHORITY_LINKS}-link authority bound"
                )));
            }
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
            retained.insert(current.sequence);
            link = outcome_record.previous_outcome;
        }
        let mut drain_link = head.assignment_drain_decision_head;
        let mut drain_links = 0;
        while let Some(current) = drain_link {
            if !consume_live_authority_link(&mut drain_links) {
                return Err(LeaseError::Invalid(format!(
                    "live assignment-drain retention exceeds the fixed {MAX_LIVE_AUTHORITY_LINKS}-link authority bound"
                )));
            }
            if head
                .assignment_drain_floor
                .as_ref()
                .is_some_and(|floor| current.target_version < floor.before_target_version)
            {
                if head
                    .assignment_drain_floor
                    .as_ref()
                    .and_then(|floor| floor.terminal_anchor_link)
                    != Some(current)
                {
                    return Err(LeaseError::Invalid(
                        "assignment-drain floor does not anchor the retained authority chain"
                            .into(),
                    ));
                }
                break;
            }
            let decision_record = read_authority_record(store.as_ref(), current.sequence)
                .await?
                .filter(|record| {
                    record.assignment_drain_decision_head == Some(current)
                        && record
                            .assignment_drain_decision
                            .as_ref()
                            .map(AssignmentDrainDecision::target_version)
                            == Some(current.target_version)
                })
                .ok_or_else(|| {
                    LeaseError::Invalid(
                        "retained assignment-drain authority chain is broken".into(),
                    )
                })?;
            retained.insert(current.sequence);
            drain_link = decision_record.previous_assignment_drain_decision;
        }

        for _ in 0..LEADER_LEASE_MAX_PRUNE_BATCHES {
            let (candidates, exhausted) =
                Self::prune_candidates(store, &retained, head_sequence, grace_ms).await?;
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
        Err(LeaseError::Io(
            "leader lease history still exceeds the bounded prune budget".into(),
        ))
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
        let mut observed_head = false;
        for attempt in 0..MAX_LEASE_HEAD_READ_ATTEMPTS {
            let Some(sequence) = self.newest_sequences(1).await?.last().copied() else {
                if !observed_head {
                    return Ok(None);
                }
                if attempt + 1 < MAX_LEASE_HEAD_READ_ATTEMPTS {
                    tokio::task::yield_now().await;
                    continue;
                }
                break;
            };
            observed_head = true;
            match read_authority_record(self.store.as_ref(), sequence).await? {
                Some(record) => return Ok(Some(record)),
                None if attempt + 1 < MAX_LEASE_HEAD_READ_ATTEMPTS => {
                    tokio::task::yield_now().await;
                }
                None => break,
            }
        }
        Err(LeaseError::Io(format!(
            "leader authority head changed during {MAX_LEASE_HEAD_READ_ATTEMPTS} read attempts"
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
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let payload = PutPayload::from(encode_authority_record(candidate)?);
        match self
            .store
            .put_opts(&lease_path(candidate.lease.seq), payload, options)
            .await
        {
            Ok(_) => {
                self.schedule_history_prune();
                Ok(AuthorityCreateOutcome::Created)
            }
            Err(error) => {
                // Reconcile ambiguous writes at the exact create-only key before consulting a
                // possibly later head.
                if let Some(at_sequence) =
                    read_authority_record(self.store.as_ref(), candidate.lease.seq).await?
                {
                    if at_sequence == *candidate {
                        self.schedule_history_prune();
                        return Ok(AuthorityCreateOutcome::Created);
                    }
                    let winner = self.load_record().await?.unwrap_or(at_sequence);
                    return Ok(AuthorityCreateOutcome::Contended(winner));
                }
                if matches!(
                    error,
                    object_store::Error::AlreadyExists { .. }
                        | object_store::Error::Precondition { .. }
                ) {
                    let winner = self.load_record().await?.ok_or_else(|| {
                        LeaseError::Io(
                            "authority CAS conflict but the winner was not readable".into(),
                        )
                    })?;
                    return Ok(AuthorityCreateOutcome::Contended(winner));
                }
                Err(LeaseError::Io(error.to_string()))
            }
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
                token: current.lease.token,
                owner: current.lease.owner.clone(),
                expires_at_ms: current.lease.expires_at_ms,
                catalog_manifest: Some(reference.clone()),
            };
            let candidate = current.preserve_with_lease(candidate_lease);
            let options = PutOptions {
                mode: PutMode::Create,
                ..PutOptions::default()
            };
            let payload = PutPayload::from(encode_authority_record(&candidate)?);
            match self
                .store
                .put_opts(&lease_path(candidate.lease.seq), payload, options)
                .await
            {
                Ok(_) => {
                    self.schedule_history_prune();
                    return Ok(CatalogSealOutcome::Created);
                }
                Err(
                    object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. },
                ) => {
                    // A same-term renewal is harmless. Re-read the head and retry; a takeover or
                    // another seal is classified at the top of the loop.
                    tokio::task::yield_now().await;
                }
                Err(error) => {
                    // Object stores may report an indeterminate write result. Reconcile from the
                    // append-only head before returning the transport error.
                    match self.load().await {
                        Ok(Some(winner)) => {
                            if let Some(sealed) = &winner.catalog_manifest {
                                let durable = self.load_catalog_manifest(sealed).await?;
                                return if durable == *manifest {
                                    Ok(CatalogSealOutcome::ExistingIdentical)
                                } else {
                                    Err(CatalogManifestError::Conflict)
                                };
                            }
                            if !winner.matches_proof(proof) {
                                return Err(CatalogManifestError::Fenced);
                            }
                            if winner.seq > base_sequence {
                                tokio::task::yield_now().await;
                                continue;
                            }
                        }
                        Ok(None) => return Err(CatalogManifestError::Fenced),
                        Err(_) => {}
                    }
                    return Err(CatalogManifestError::Authority(LeaseError::Io(
                        error.to_string(),
                    )));
                }
            }
        }
    }

    async fn audited_cluster_outcomes_from(
        &self,
        head: &LeaderAuthorityRecord,
    ) -> Result<Vec<CheckpointOutcome>, ClusterCheckpointAuthorityError> {
        let floor = head.outcome_floor.as_ref();
        let before_epoch = floor.map_or(0, |floor| floor.before_epoch);
        let mut newest_first = Vec::new();
        let mut link = head.outcome_head;
        let mut traversed = 0;
        while let Some(current) = link {
            if !consume_live_authority_link(&mut traversed) {
                return Err(DecisionError::Conflict(format!(
                    "live outcome retention exceeds the fixed {MAX_LIVE_AUTHORITY_LINKS}-link authority bound"
                ))
                .into());
            }
            if current.epoch < before_epoch {
                break;
            }
            let record = read_authority_record(self.store.as_ref(), current.sequence)
                .await?
                .ok_or_else(|| {
                    DecisionError::InventoryChanged(format!(
                        "cluster outcome authority record {} disappeared during audit",
                        current.sequence
                    ))
                })?;
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
            newest_first.push(outcome);
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
                        floor.before_epoch
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

        newest_first.reverse();
        let mut outcomes = Vec::with_capacity(newest_first.len().saturating_add(2));
        if let Some(anchor) = floor.and_then(|floor| floor.committed_anchor.as_ref()) {
            outcomes.push(anchor.clone());
        }
        if let Some(anchor) = floor.and_then(|floor| floor.terminal_anchor.as_ref()) {
            if outcomes.last() != Some(anchor) {
                outcomes.push(anchor.clone());
            }
        }
        outcomes.extend(newest_first);

        let expected_deployment = CheckpointDecisionStore::new(Arc::clone(&self.store))
            .load_or_create_deployment_id()
            .await?;
        for outcome in &outcomes {
            if outcome.deployment_id != expected_deployment {
                return Err(DecisionError::Conflict(format!(
                    "cluster outcome epoch {} belongs to deployment {}, current deployment is {}",
                    outcome.epoch, outcome.deployment_id, expected_deployment
                ))
                .into());
            }
        }
        for pair in outcomes.windows(2) {
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
        Ok(outcomes)
    }

    async fn audited_cluster_outcomes(
        &self,
    ) -> Result<
        (Option<LeaderAuthorityRecord>, Vec<CheckpointOutcome>),
        ClusterCheckpointAuthorityError,
    > {
        const AUDIT_RETRIES: usize = 3;
        for attempt in 0..AUDIT_RETRIES {
            let Some(head) = self.load_record().await? else {
                return Ok((None, Vec::new()));
            };
            match self.audited_cluster_outcomes_from(&head).await {
                Err(ClusterCheckpointAuthorityError::Decision(
                    DecisionError::InventoryChanged(_),
                )) if attempt + 1 < AUDIT_RETRIES => {
                    tokio::task::yield_now().await;
                }
                result => return result.map(|outcomes| (Some(head), outcomes)),
            }
        }
        Err(DecisionError::InventoryChanged(
            "cluster outcome audit exhausted stability retries".into(),
        )
        .into())
    }

    async fn audited_assignment_drain_decisions_from(
        &self,
        head: &LeaderAuthorityRecord,
    ) -> Result<Vec<AssignmentDrainDecision>, ClusterCheckpointAuthorityError> {
        let mut newest_first = Vec::new();
        let floor = head.assignment_drain_floor.as_ref();
        let before_target_version = floor.map_or(0, |floor| floor.before_target_version);
        let mut stopped_at_anchor = false;
        let mut link = head.assignment_drain_decision_head;
        let mut traversed = 0;
        while let Some(current) = link {
            if !consume_live_authority_link(&mut traversed) {
                return Err(DecisionError::Conflict(format!(
                    "live assignment-drain retention exceeds the fixed {MAX_LIVE_AUTHORITY_LINKS}-link authority bound"
                ))
                .into());
            }
            if current.target_version < before_target_version {
                if floor.and_then(|floor| floor.terminal_anchor_link) != Some(current) {
                    return Err(DecisionError::Conflict(format!(
                        "assignment drain decision chain does not meet durable floor {before_target_version} at its terminal anchor"
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
                        "assignment drain decision authority record {} disappeared during audit",
                        current.sequence
                    ))
                })?;
            let decision = record.assignment_drain_decision.clone().ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "assignment drain decision head version {} points to non-decision authority record {}",
                    current.target_version, current.sequence
                ))
            })?;
            if record.assignment_drain_decision_head != Some(current)
                || decision.target_version() != current.target_version
            {
                return Err(DecisionError::Conflict(format!(
                    "assignment drain decision link version {} sequence {} does not match its authority record",
                    current.target_version, current.sequence
                ))
                .into());
            }
            newest_first.push(decision);
            link = record.previous_assignment_drain_decision;
        }
        if floor.and_then(|floor| floor.terminal_anchor_link).is_some() && !stopped_at_anchor {
            return Err(DecisionError::Conflict(
                "assignment drain decision chain stopped without its durable retention anchor"
                    .into(),
            )
            .into());
        }
        newest_first.reverse();
        if let Some(pair) = newest_first
            .windows(2)
            .find(|pair| pair[0].target_version() >= pair[1].target_version())
        {
            return Err(DecisionError::Conflict(format!(
                "assignment drain decisions regress from version {} to {}",
                pair[0].target_version(),
                pair[1].target_version()
            ))
            .into());
        }
        Ok(newest_first)
    }

    async fn exact_assignment_drain_decision_link(
        &self,
        head: &LeaderAuthorityRecord,
        decision: &AssignmentDrainDecision,
    ) -> Result<AssignmentDrainDecisionLink, ClusterCheckpointAuthorityError> {
        if let Some(floor) = head.assignment_drain_floor.as_ref() {
            if floor.terminal_anchor.as_ref() == Some(decision) {
                return floor.terminal_anchor_link.ok_or_else(|| {
                    DecisionError::Conflict(
                        "assignment drain floor lost its terminal authority link".into(),
                    )
                    .into()
                });
            }
        }
        let before_target_version = head
            .assignment_drain_floor
            .as_ref()
            .map_or(0, |floor| floor.before_target_version);
        let mut link = head.assignment_drain_decision_head;
        let mut traversed = 0;
        while let Some(current) = link {
            if !consume_live_authority_link(&mut traversed) {
                return Err(DecisionError::Conflict(format!(
                    "live assignment-drain retention exceeds the fixed {MAX_LIVE_AUTHORITY_LINKS}-link authority bound during exact lookup"
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
                        "assignment drain decision authority record {} disappeared during exact lookup",
                        current.sequence
                    ))
                })?;
            if record.assignment_drain_decision.as_ref() == Some(decision) {
                return Ok(current);
            }
            link = record.previous_assignment_drain_decision;
        }
        Err(DecisionError::Conflict(format!(
            "assignment drain decision version {} is not linked from the durable authority head",
            decision.target_version()
        ))
        .into())
    }

    async fn exact_outcome_link(
        &self,
        head: &LeaderAuthorityRecord,
        outcome: &CheckpointOutcome,
    ) -> Result<OutcomeLink, ClusterCheckpointAuthorityError> {
        if let Some(floor) = head.outcome_floor.as_ref() {
            if floor.terminal_anchor.as_ref() == Some(outcome) {
                return floor.terminal_anchor_link.ok_or_else(|| {
                    DecisionError::Conflict(
                        "cluster outcome floor lost its terminal authority link".into(),
                    )
                    .into()
                });
            }
        }
        let before_epoch = head
            .outcome_floor
            .as_ref()
            .map_or(0, |floor| floor.before_epoch);
        let mut link = head.outcome_head;
        let mut traversed = 0;
        while let Some(current) = link {
            if !consume_live_authority_link(&mut traversed) {
                return Err(DecisionError::Conflict(format!(
                    "live outcome retention exceeds the fixed {MAX_LIVE_AUTHORITY_LINKS}-link authority bound during exact lookup"
                ))
                .into());
            }
            if current.epoch < before_epoch {
                break;
            }
            if current.epoch == outcome.epoch && current.checkpoint_id == outcome.checkpoint_id {
                return Ok(current);
            }
            let record = read_authority_record(self.store.as_ref(), current.sequence)
                .await?
                .ok_or_else(|| {
                    DecisionError::InventoryChanged(format!(
                        "cluster outcome authority record {} disappeared while resolving its link",
                        current.sequence
                    ))
                })?;
            link = record.previous_outcome;
        }
        Err(DecisionError::Conflict(format!(
            "cluster outcome epoch {} checkpoint {} has no authority link",
            outcome.epoch, outcome.checkpoint_id
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
            let outcomes = self.audited_cluster_outcomes_from(&current).await?;
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
            if let Some(floor) = current.outcome_floor.as_ref() {
                if candidate.deployment_id != floor.deployment_id
                    || candidate.epoch < floor.before_epoch
                {
                    return Err(DecisionError::Conflict(format!(
                        "cluster outcome epoch {} is below or outside authority floor {}",
                        candidate.epoch, floor.before_epoch
                    ))
                    .into());
                }
            }

            let base_sequence = current.lease.seq;
            let sequence = base_sequence
                .checked_add(1)
                .ok_or_else(|| LeaseError::Invalid("leader authority sequence exhausted".into()))?;
            let mut next = current.preserve_with_lease(LeaderLease {
                seq: sequence,
                token: current.lease.token,
                owner: current.lease.owner.clone(),
                expires_at_ms: current.lease.expires_at_ms,
                catalog_manifest: current.lease.catalog_manifest.clone(),
            });
            next.checkpoint_outcome = Some(candidate.clone());
            next.previous_outcome = current.outcome_head;
            next.outcome_head = Some(OutcomeLink {
                sequence,
                epoch: candidate.epoch,
                checkpoint_id: candidate.checkpoint_id,
            });
            next.validate()?;
            let payload = PutPayload::from(encode_authority_record(&next)?);
            let options = PutOptions {
                mode: PutMode::Create,
                ..PutOptions::default()
            };
            match self
                .store
                .put_opts(&lease_path(sequence), payload, options)
                .await
            {
                Ok(_) => {
                    self.schedule_history_prune();
                    return Ok(RecordOutcomeResult::Created(candidate));
                }
                Err(error) => {
                    // Every failed create may be an acknowledged-late success. Reconcile from
                    // the canonical head before classifying the transport result.
                    if let Ok(Some(winner_head)) = self.load_record().await {
                        if let Ok(winners) = self.audited_cluster_outcomes_from(&winner_head).await
                        {
                            if let Some(winner) = winners
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
                        }
                        if !winner_head.lease.matches_proof(proof) {
                            return Err(ClusterCheckpointAuthorityError::Fenced);
                        }
                        if winner_head.lease.seq > base_sequence {
                            tokio::task::yield_now().await;
                            continue;
                        }
                    }
                    return Err(LeaseError::Io(error.to_string()).into());
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
        decision.validate()?;
        if &decision.leader_proof != proof || !proof.is_canonical() {
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
            if let Some(floor) = current.assignment_drain_floor.as_ref() {
                if decision.target_version() < floor.before_target_version {
                    return Err(DecisionError::Conflict(format!(
                        "assignment drain decision version {} is below durable retention floor {}",
                        decision.target_version(),
                        floor.before_target_version
                    ))
                    .into());
                }
            }
            let decisions = self
                .audited_assignment_drain_decisions_from(&current)
                .await?;
            if let Some(winner) = decisions
                .iter()
                .find(|winner| winner.target_version() == decision.target_version())
            {
                return if winner == &decision {
                    Ok(RecordAssignmentDrainDecisionResult::Unchanged(
                        winner.clone(),
                    ))
                } else {
                    Ok(RecordAssignmentDrainDecisionResult::Conflict {
                        winner: winner.clone(),
                    })
                };
            }
            if let Some(last) = decisions.last() {
                if decision.target_version() <= last.target_version() {
                    return Err(DecisionError::Conflict(format!(
                        "assignment drain decision version {} does not advance durable version {}",
                        decision.target_version(),
                        last.target_version()
                    ))
                    .into());
                }
            }

            let base_sequence = current.lease.seq;
            let sequence = base_sequence
                .checked_add(1)
                .ok_or_else(|| LeaseError::Invalid("leader authority sequence exhausted".into()))?;
            let mut candidate = current.preserve_with_lease(LeaderLease {
                seq: sequence,
                token: current.lease.token,
                owner: current.lease.owner.clone(),
                expires_at_ms: current.lease.expires_at_ms,
                catalog_manifest: current.lease.catalog_manifest.clone(),
            });
            candidate.assignment_drain_decision = Some(decision.clone());
            candidate.previous_assignment_drain_decision = current.assignment_drain_decision_head;
            candidate.assignment_drain_decision_head = Some(AssignmentDrainDecisionLink {
                sequence,
                target_version: decision.target_version(),
            });
            candidate.validate()?;

            match self.create_authority_record(&candidate).await? {
                AuthorityCreateOutcome::Created => {
                    return Ok(RecordAssignmentDrainDecisionResult::Created(decision));
                }
                AuthorityCreateOutcome::Contended(winner_head) => {
                    let winners = self
                        .audited_assignment_drain_decisions_from(&winner_head)
                        .await?;
                    if let Some(winner) = winners
                        .iter()
                        .find(|winner| winner.target_version() == decision.target_version())
                    {
                        return if winner == &decision {
                            Ok(RecordAssignmentDrainDecisionResult::Unchanged(
                                winner.clone(),
                            ))
                        } else {
                            Ok(RecordAssignmentDrainDecisionResult::Conflict {
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
                        "assignment drain authority contention did not advance the sequence".into(),
                    )
                    .into());
                }
            }
        }
    }

    /// Read the immutable settlement for one exact target assignment version.
    ///
    /// # Errors
    /// Fails closed on malformed or incomplete authority history.
    pub async fn assignment_drain_decision(
        &self,
        target_version: u64,
    ) -> Result<Option<AssignmentDrainDecision>, ClusterCheckpointAuthorityError> {
        if target_version == 0 {
            return Err(LeaseError::Invalid(
                "assignment drain decision target version must be nonzero".into(),
            )
            .into());
        }
        let Some(head) = self.load_record().await? else {
            return Ok(None);
        };
        if let Some(floor) = head.assignment_drain_floor.as_ref() {
            if target_version < floor.before_target_version {
                return Err(DecisionError::Conflict(format!(
                    "assignment drain decision version {target_version} is below durable retention floor {}",
                    floor.before_target_version
                ))
                .into());
            }
        }
        Ok(self
            .audited_assignment_drain_decisions_from(&head)
            .await?
            .into_iter()
            .find(|decision| decision.target_version() == target_version))
    }

    /// Advance the assignment-drain floor through the exact next authority sequence.
    ///
    /// The caller must first durably prune assignment snapshots below the same target-version
    /// horizon. Decision-bearing authority records below the durable floor then become eligible
    /// for best-effort deletion while the exact terminal anchor preserves chain continuity.
    pub async fn prune_assignment_drain_decisions_before(
        &self,
        proof: &LeaderProof,
        before_target_version: u64,
    ) -> Result<u64, ClusterCheckpointAuthorityError> {
        if before_target_version == 0 {
            return Ok(self
                .load_record()
                .await?
                .and_then(|head| head.assignment_drain_floor)
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
            if let Some(floor) = current.assignment_drain_floor.as_ref() {
                if floor.before_target_version >= before_target_version {
                    self.schedule_history_prune();
                    return Ok(floor.before_target_version);
                }
            }

            let decisions = self
                .audited_assignment_drain_decisions_from(&current)
                .await?;
            let terminal_anchor = decisions
                .iter()
                .rev()
                .find(|decision| decision.target_version() < before_target_version)
                .cloned()
                .or_else(|| {
                    current
                        .assignment_drain_floor
                        .as_ref()
                        .and_then(|floor| floor.terminal_anchor.clone())
                });
            let terminal_anchor_link = match terminal_anchor.as_ref() {
                Some(anchor) => Some(
                    self.exact_assignment_drain_decision_link(&current, anchor)
                        .await?,
                ),
                None => None,
            };
            let floor = AuthorityAssignmentDrainFloor {
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
                token: current.lease.token,
                owner: current.lease.owner.clone(),
                expires_at_ms: current.lease.expires_at_ms,
                catalog_manifest: current.lease.catalog_manifest.clone(),
            });
            next.assignment_drain_floor = Some(floor);
            next.validate()?;

            match self.create_authority_record(&next).await? {
                AuthorityCreateOutcome::Created => return Ok(before_target_version),
                AuthorityCreateOutcome::Contended(winner) => {
                    if let Some(winner_floor) = winner.assignment_drain_floor.as_ref() {
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
                        "assignment drain floor contention did not advance the sequence".into(),
                    )
                    .into());
                }
            }
        }
    }

    /// Read one live cluster outcome from the shared authority.
    pub async fn cluster_outcome(
        &self,
        epoch: u64,
    ) -> Result<Option<CheckpointOutcome>, ClusterCheckpointAuthorityError> {
        Ok(self
            .cluster_outcomes()
            .await?
            .into_iter()
            .find(|outcome| outcome.epoch == epoch))
    }

    /// Audit and return every live cluster outcome in ascending epoch order.
    pub async fn cluster_outcomes(
        &self,
    ) -> Result<Vec<CheckpointOutcome>, ClusterCheckpointAuthorityError> {
        let (head, mut outcomes) = self.audited_cluster_outcomes().await?;
        if let Some(floor) = head.and_then(|head| head.outcome_floor) {
            outcomes.retain(|outcome| outcome.epoch >= floor.before_epoch);
        }
        Ok(outcomes)
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
        Ok(self.audited_cluster_outcomes().await?.1.pop())
    }

    async fn validate_cluster_recovery_cut(
        &self,
        floor: &AuthorityOutcomeFloor,
        audited_outcomes: &[CheckpointOutcome],
    ) -> Result<CheckpointOutcome, ClusterCheckpointAuthorityError> {
        let recovery_cut = audited_outcomes
            .iter()
            .rev()
            .find(|outcome| outcome.epoch >= floor.before_epoch && outcome.is_commit())
            .ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "cluster outcome floor {} has no live commit recovery cut",
                    floor.before_epoch
                ))
            })?;
        CheckpointDecisionStore::new(Arc::clone(&self.store))
            .validate_recovery_capsule_for_outcome(recovery_cut)
            .await?;
        Ok(recovery_cut.clone())
    }

    async fn preflight_cluster_recovery_cut<V, Fut>(
        &self,
        floor: &AuthorityOutcomeFloor,
        audited_outcomes: &[CheckpointOutcome],
        validate_artifacts: &V,
    ) -> Result<CheckpointOutcome, ClusterCheckpointAuthorityError>
    where
        V: Fn(CheckpointOutcome) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let recovery_cut = self
            .validate_cluster_recovery_cut(floor, audited_outcomes)
            .await?;
        validate_artifacts(recovery_cut.clone())
            .await
            .map_err(|error| {
                DecisionError::Conflict(format!(
                    "cluster recovery cut epoch {} checkpoint {} failed complete artifact preflight: {error}",
                    recovery_cut.epoch, recovery_cut.checkpoint_id
                ))
            })?;
        Ok(recovery_cut)
    }

    /// Exact continuity boundary for cluster outcomes compacted from the authority history.
    pub async fn cluster_outcome_retention_boundary(
        &self,
    ) -> Result<ClusterOutcomeRetentionBoundary, ClusterCheckpointAuthorityError> {
        let Some(head) = self.load_record().await? else {
            return Ok(ClusterOutcomeRetentionBoundary {
                before_epoch: 0,
                committed_anchor: None,
                terminal_anchor: None,
            });
        };
        Ok(head.outcome_floor.map_or(
            ClusterOutcomeRetentionBoundary {
                before_epoch: 0,
                committed_anchor: None,
                terminal_anchor: None,
            },
            |floor| ClusterOutcomeRetentionBoundary {
                before_epoch: floor.before_epoch,
                committed_anchor: floor.committed_anchor,
                terminal_anchor: floor.terminal_anchor,
            },
        ))
    }

    /// Read an existing cluster retention boundary only after its selected live Commit passes the
    /// caller's complete artifact preflight and the outcome head remains unchanged.
    pub async fn validated_cluster_outcome_retention_boundary<V, Fut>(
        &self,
        validate_artifacts: V,
    ) -> Result<ClusterOutcomeRetentionBoundary, ClusterCheckpointAuthorityError>
    where
        V: Fn(CheckpointOutcome) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        loop {
            let current = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let Some(floor) = current.outcome_floor.as_ref() else {
                return Ok(ClusterOutcomeRetentionBoundary {
                    before_epoch: 0,
                    committed_anchor: None,
                    terminal_anchor: None,
                });
            };
            let outcomes = self.audited_cluster_outcomes_from(&current).await?;
            self.preflight_cluster_recovery_cut(floor, &outcomes, &validate_artifacts)
                .await?;
            let rechecked = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            if rechecked.outcome_head == current.outcome_head
                && rechecked.outcome_floor == current.outcome_floor
            {
                return Ok(ClusterOutcomeRetentionBoundary {
                    before_epoch: floor.before_epoch,
                    committed_anchor: floor.committed_anchor.clone(),
                    terminal_anchor: floor.terminal_anchor.clone(),
                });
            }
            tokio::task::yield_now().await;
        }
    }

    /// Run one bounded recovery-capsule cleanup step below the durable authority floor.
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
        let audited_outcomes = self.audited_cluster_outcomes_from(&current).await?;
        let Some(floor) = current.outcome_floor.as_ref() else {
            return Ok(crate::checkpoint_decision::RecoveryCapsuleGcStep {
                examined: 0,
                deleted: 0,
                quarantined: 0,
                pending: false,
            });
        };
        let mut known_live_digests = BTreeSet::new();
        for anchor in [
            floor.terminal_anchor.as_ref(),
            floor.committed_anchor.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if let Some(reference) = anchor.recovery_capsule.as_ref() {
                known_live_digests.insert(reference.sha256.clone());
            }
        }
        known_live_digests.extend(
            audited_outcomes
                .iter()
                .filter(|outcome| outcome.epoch >= floor.before_epoch)
                .filter_map(|outcome| outcome.recovery_capsule.as_ref())
                .map(|reference| reference.sha256.clone()),
        );
        CheckpointDecisionStore::new(Arc::clone(&self.store))
            .sweep_recovery_capsules_step(floor.before_epoch, &known_live_digests)
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
                .before_epoch);
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
            let outcomes = self.audited_cluster_outcomes_from(&current).await?;
            let floor_is_existing = current
                .outcome_floor
                .as_ref()
                .is_some_and(|floor| floor.before_epoch >= before_epoch);
            let floor = if floor_is_existing {
                current
                    .outcome_floor
                    .clone()
                    .expect("existing floor checked above")
            } else {
                if !outcomes
                    .iter()
                    .any(|outcome| outcome.epoch >= before_epoch && outcome.is_commit())
                {
                    return Err(DecisionError::Conflict(format!(
                        "cannot advance cluster outcome floor to {before_epoch}: no live commit recovery cut would remain"
                    ))
                    .into());
                }
                let deployment_id = CheckpointDecisionStore::new(Arc::clone(&self.store))
                    .load_or_create_deployment_id()
                    .await?;
                let terminal_anchor = outcomes
                    .iter()
                    .rev()
                    .find(|outcome| outcome.epoch < before_epoch)
                    .cloned();
                let terminal_anchor_link = match terminal_anchor.as_ref() {
                    Some(anchor) => Some(self.exact_outcome_link(&current, anchor).await?),
                    None => None,
                };
                AuthorityOutcomeFloor {
                    deployment_id,
                    before_epoch,
                    terminal_anchor,
                    terminal_anchor_link,
                    committed_anchor: outcomes
                        .iter()
                        .rev()
                        .find(|outcome| outcome.epoch < before_epoch && outcome.is_commit())
                        .cloned(),
                }
            };
            floor.validate()?;
            self.preflight_cluster_recovery_cut(&floor, &outcomes, &validate_artifacts)
                .await?;

            // Lease renewals and catalog seals may advance the shared sequence while the complete
            // artifact preflight performs remote reads. They are harmless only when both the
            // outcome head and retention floor remain exactly the same.
            let rechecked = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            if !rechecked.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            if rechecked.outcome_head != current.outcome_head
                || rechecked.outcome_floor != current.outcome_floor
            {
                tokio::task::yield_now().await;
                continue;
            }
            if floor_is_existing {
                self.schedule_history_prune();
                return Ok(floor.before_epoch);
            }

            let base_sequence = rechecked.lease.seq;
            let sequence = base_sequence
                .checked_add(1)
                .ok_or_else(|| LeaseError::Invalid("leader authority sequence exhausted".into()))?;
            let mut next = rechecked.preserve_with_lease(LeaderLease {
                seq: sequence,
                token: rechecked.lease.token,
                owner: rechecked.lease.owner.clone(),
                expires_at_ms: rechecked.lease.expires_at_ms,
                catalog_manifest: rechecked.lease.catalog_manifest.clone(),
            });
            next.outcome_floor = Some(floor.clone());
            next.validate()?;
            let payload = PutPayload::from(encode_authority_record(&next)?);
            let options = PutOptions {
                mode: PutMode::Create,
                ..PutOptions::default()
            };
            match self
                .store
                .put_opts(&lease_path(sequence), payload, options)
                .await
            {
                Ok(_) => {
                    self.schedule_history_prune();
                    return Ok(before_epoch);
                }
                Err(error) => {
                    if let Ok(Some(winner)) = self.load_record().await {
                        if winner == next {
                            let winner_floor = winner.outcome_floor.as_ref().ok_or_else(|| {
                                LeaseError::Invalid(
                                    "durable floor winner lost its retention boundary".into(),
                                )
                            })?;
                            let winner_outcomes =
                                self.audited_cluster_outcomes_from(&winner).await?;
                            self.preflight_cluster_recovery_cut(
                                winner_floor,
                                &winner_outcomes,
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
                            if confirmed.outcome_head == winner.outcome_head
                                && confirmed.outcome_floor == winner.outcome_floor
                            {
                                self.schedule_history_prune();
                                return Ok(before_epoch);
                            }
                            tokio::task::yield_now().await;
                            continue;
                        }
                        if !winner.lease.matches_proof(proof) {
                            return Err(ClusterCheckpointAuthorityError::Fenced);
                        }
                        if winner.lease.seq > base_sequence {
                            tokio::task::yield_now().await;
                            continue;
                        }
                    }
                    return Err(LeaseError::Io(error.to_string()).into());
                }
            }
        }
    }

    /// Acquire an empty authority or renew an exact owner. Rival wall clocks never authorize
    /// takeover.
    ///
    /// # Errors
    /// Fails closed on invalid input, object-store I/O, or arithmetic exhaustion.
    pub async fn try_acquire(
        &self,
        owner: &LeaderLeaseOwner,
        now_ms: i64,
    ) -> Result<LeaseOutcome, LeaseError> {
        owner.validate()?;
        self.ttl()?;
        let expires_at_ms = self.diagnostic_expiry(now_ms)?;
        loop {
            let current = self.load_record().await?;
            let candidate = match current {
                None => LeaderAuthorityRecord::initial(LeaderLease {
                    seq: 1,
                    token: 1,
                    owner: owner.clone(),
                    expires_at_ms,
                    catalog_manifest: None,
                }),
                Some(record) if record.lease.owner == *owner => {
                    let lease = LeaderLease {
                        seq: record.lease.seq.checked_add(1).ok_or_else(|| {
                            LeaseError::Invalid("lease sequence exhausted".into())
                        })?,
                        token: record.lease.token,
                        owner: owner.clone(),
                        expires_at_ms,
                        catalog_manifest: record.lease.catalog_manifest.clone(),
                    };
                    record.preserve_with_lease(lease)
                }
                Some(record) => return Ok(LeaseOutcome::Held(record.lease)),
            };
            match self.create_authority_record(&candidate).await? {
                AuthorityCreateOutcome::Created => {
                    return Ok(LeaseOutcome::Acquired(candidate.lease));
                }
                AuthorityCreateOutcome::Contended(winner) if winner.lease.owner == *owner => {
                    tokio::task::yield_now().await;
                }
                AuthorityCreateOutcome::Contended(winner) => {
                    return Ok(LeaseOutcome::Held(winner.lease));
                }
            }
        }
    }

    /// Start a candidate-local observation of a rival durable record.
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

    /// Take over only after the exact rival owner and sequence remained current for a full TTL on
    /// the candidate's monotonic clock.
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
        let current = self
            .load_record()
            .await?
            .ok_or_else(|| LeaseError::Invalid("observed leader lease disappeared".into()))?;
        if current.lease != observation.lease {
            return Ok(LeaseOutcome::Held(current.lease));
        }
        let candidate_lease = LeaderLease {
            seq: current
                .lease
                .seq
                .checked_add(1)
                .ok_or_else(|| LeaseError::Invalid("lease sequence exhausted".into()))?,
            token: current
                .lease
                .token
                .checked_add(1)
                .ok_or_else(|| LeaseError::Invalid("fencing token exhausted".into()))?,
            owner: owner.clone(),
            expires_at_ms: self.diagnostic_expiry(now_ms)?,
            catalog_manifest: current.lease.catalog_manifest.clone(),
        };
        let candidate = current.preserve_with_lease(candidate_lease);
        match self.create_authority_record(&candidate).await? {
            AuthorityCreateOutcome::Created => Ok(LeaseOutcome::Acquired(candidate.lease)),
            AuthorityCreateOutcome::Contended(winner) => Ok(LeaseOutcome::Held(winner.lease)),
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
    Completed(Result<LeaseOutcome, LeaseError>),
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
            deadline: Arc::new(LeaseDeadline::fenced()),
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
    fn fence(&self) {
        self.deadline.fence();
        self.lease_tx.send_replace(None);
    }

    #[cfg(feature = "cluster")]
    async fn attempt_lease(
        &self,
        shutdown: &tokio_util::sync::CancellationToken,
        candidate: &mut watch::Receiver<bool>,
        valid_until: Option<tokio::time::Instant>,
        observation: Option<&LeaderLeaseObservation>,
    ) -> LeaseOperationEvent {
        let operation = async {
            if let Some(observation) = observation {
                self.store
                    .try_takeover(&self.owner, observation, now_millis())
                    .await
            } else {
                self.store.try_acquire(&self.owner, now_millis()).await
            }
        };
        tokio::select! {
            biased;
            () = shutdown.cancelled() => LeaseOperationEvent::Shutdown,
            changed = candidate.changed() => LeaseOperationEvent::Candidacy(changed),
            () = wait_for_deadline(valid_until) => LeaseOperationEvent::Deadline,
            result = operation => LeaseOperationEvent::Completed(result),
        }
    }

    #[cfg(feature = "cluster")]
    async fn wait_for_candidacy_change(
        &self,
        shutdown: &tokio_util::sync::CancellationToken,
        candidate: &mut watch::Receiver<bool>,
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
        mut candidate: watch::Receiver<bool>,
    ) {
        let mut ticker = tokio::time::interval(self.config.renew_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut valid_until: Option<tokio::time::Instant> = None;
        let mut observation: Option<LeaderLeaseObservation> = None;

        loop {
            if !*candidate.borrow_and_update() {
                self.fence();
                observation = None;
                valid_until = None;
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
                    if !*candidate.borrow_and_update() {
                        self.fence();
                    }
                    continue;
                }
                () = wait_for_deadline(valid_until) => {
                    self.fence();
                    return;
                }
                _ = ticker.tick() => {}
            }

            let result = match self
                .attempt_lease(&shutdown, &mut candidate, valid_until, observation.as_ref())
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
                    if !*candidate.borrow_and_update() {
                        self.fence();
                    }
                    continue;
                }
                LeaseOperationEvent::Completed(result) => result,
            };

            match result {
                Ok(LeaseOutcome::Acquired(lease)) if lease.owner == self.owner => {
                    let Some(next_deadline) =
                        tokio::time::Instant::now().checked_add(self.config.ttl)
                    else {
                        self.fence();
                        return;
                    };
                    observation = None;
                    valid_until = Some(next_deadline);
                    self.deadline.extend(self.config.ttl);
                    self.lease_tx.send_replace(Some(lease));
                }
                Ok(LeaseOutcome::Acquired(_)) => {
                    self.fence();
                    return;
                }
                Ok(LeaseOutcome::Held(rival)) => {
                    self.fence();
                    valid_until = None;
                    let unchanged = observation
                        .as_ref()
                        .is_some_and(|observed| observed.lease == rival);
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
                Err(error) => {
                    tracing::warn!(%error, "leader lease operation failed");
                }
            }
        }
    }

    /// Spawn the renewal loop. Loss of candidacy or shutdown fences synchronously with the
    /// corresponding watch/cancellation notification. A missed renewal fences at the last local
    /// monotonic deadline and terminates the manager.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn spawn(
        self,
        shutdown: tokio_util::sync::CancellationToken,
        candidate: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(self.run(shutdown, candidate))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::StreamExt as FuturesStreamExt;
    use object_store::memory::InMemory;

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

    #[tokio::test]
    async fn exact_owner_renews_without_advancing_token() {
        let store = store(1_000);
        let owner = owner(1, 1, 4);
        let LeaseOutcome::Acquired(first) = store.try_acquire(&owner, 10).await.unwrap() else {
            panic!("empty authority must be acquired");
        };
        let LeaseOutcome::Acquired(second) = store.try_acquire(&owner, 500).await.unwrap() else {
            panic!("exact owner must renew");
        };
        assert_eq!((first.seq, first.token), (1, 1));
        assert_eq!((second.seq, second.token), (2, 1));
    }

    #[tokio::test]
    async fn fast_rival_clock_cannot_steal() {
        let store = store(30);
        let incumbent = owner(1, 1, 1);
        let rival = owner(2, 2, 1);
        store.try_acquire(&incumbent, 0).await.unwrap();
        let LeaseOutcome::Held(current) = store.try_acquire(&rival, i64::MAX - 30).await.unwrap()
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
        store.try_acquire(&incumbent, 10_000).await.unwrap();
        let LeaseOutcome::Held(first) = store.try_acquire(&rival, 0).await.unwrap() else {
            panic!("rival must observe the incumbent");
        };
        let observation = store.observe_rival(&rival, &first).unwrap();
        store.try_acquire(&incumbent, -10_000).await.unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        let LeaseOutcome::Held(current) =
            store.try_takeover(&rival, &observation, 0).await.unwrap()
        else {
            panic!("renewal must invalidate the old observation");
        };
        assert_eq!(current.seq, 2);
        assert_eq!(current.owner, incumbent);
    }

    #[tokio::test]
    async fn full_unchanged_observation_is_required_for_takeover() {
        let store = store(15);
        let incumbent = owner(1, 1, 1);
        let rival = owner(2, 2, 1);
        store.try_acquire(&incumbent, 0).await.unwrap();
        let LeaseOutcome::Held(current) = store.try_acquire(&rival, 0).await.unwrap() else {
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
        assert_eq!((lease.seq, lease.token, lease.owner), (2, 2, rival));
    }

    #[tokio::test]
    async fn same_node_new_boot_is_a_rival_and_advances_token() {
        let store = store(10);
        let old = owner(7, 1, 3);
        let replacement = owner(7, 2, 4);
        store.try_acquire(&old, 0).await.unwrap();
        let LeaseOutcome::Held(current) = store.try_acquire(&replacement, 0).await.unwrap() else {
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
        let left = tokio::spawn(async move { left_store.try_acquire(&left_owner, 0).await });
        let right_store = Arc::clone(&store);
        let right = tokio::spawn(async move { right_store.try_acquire(&right_owner, 0).await });
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
    async fn local_filesystem_supports_create_only_renewal() {
        let temp = tempfile::tempdir().unwrap();
        let filesystem: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(temp.path()).unwrap());
        let store = LeaderLeaseStore::new(filesystem, 1_000);
        let owner = owner(1, 1, 1);
        assert!(matches!(
            store.try_acquire(&owner, 0).await.unwrap(),
            LeaseOutcome::Acquired(LeaderLease { seq: 1, .. })
        ));
        assert!(matches!(
            store.try_acquire(&owner, 1).await.unwrap(),
            LeaseOutcome::Acquired(LeaderLease { seq: 2, .. })
        ));
    }

    #[tokio::test]
    async fn renewal_history_pruning_has_a_reader_grace_period() {
        let store = store(1);
        let owner = owner(1, 1, 1);
        for now in 0..8 {
            assert!(matches!(
                store.try_acquire(&owner, now).await.unwrap(),
                LeaseOutcome::Acquired(_)
            ));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(matches!(
            store.try_acquire(&owner, 9).await.unwrap(),
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
        let authority = LeaderLeaseStore::new(Arc::clone(&object_store), 1);
        let retained: BTreeSet<_> = authority
            .newest_sequences(LEADER_LEASE_HISTORY_TO_RETAIN)
            .await
            .unwrap()
            .into_iter()
            .collect();
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

    #[tokio::test]
    async fn load_relists_when_the_selected_head_is_pruned() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let owner = owner(1, 1, 1);
        let first_lease = LeaderLease {
            seq: 1,
            token: 1,
            owner: owner.clone(),
            expires_at_ms: 1_000,
            catalog_manifest: None,
        };
        let first = LeaderAuthorityRecord::initial(first_lease);
        let second_lease = LeaderLease {
            seq: 2,
            token: 1,
            owner,
            expires_at_ms: 2_000,
            catalog_manifest: None,
        };
        let second = first.preserve_with_lease(second_lease.clone());
        inner
            .put(
                &lease_path(1),
                PutPayload::from(Bytes::from(serde_json::to_vec(&first).unwrap())),
            )
            .await
            .unwrap();
        let raw = Arc::new(BlockingStore {
            inner,
            blocked_path: lease_path(1),
            block_once: true,
            did_block: std::sync::atomic::AtomicBool::new(false),
            ambiguous_path: None,
            did_return_ambiguous: std::sync::atomic::AtomicBool::new(false),
            replacement_on_get: Some((
                lease_path(2),
                Bytes::from(serde_json::to_vec(&second).unwrap()),
            )),
            did_replace: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            get_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            fail_delete_once: Arc::new(std::sync::Mutex::new(None)),
            track_capsule_get_concurrency: std::sync::atomic::AtomicBool::new(false),
            active_capsule_gets: std::sync::atomic::AtomicUsize::new(0),
            max_capsule_gets: std::sync::atomic::AtomicUsize::new(0),
        });
        let object_store: Arc<dyn ObjectStore> = raw.clone();
        let store = LeaderLeaseStore::new(object_store, 1_000);

        assert_eq!(store.load().await.unwrap(), Some(second_lease));
        assert!(raw.did_replace.load(std::sync::atomic::Ordering::Acquire));
    }

    struct BlockingStore {
        inner: Arc<dyn ObjectStore>,
        blocked_path: OsPath,
        block_once: bool,
        did_block: std::sync::atomic::AtomicBool,
        ambiguous_path: Option<OsPath>,
        did_return_ambiguous: std::sync::atomic::AtomicBool,
        replacement_on_get: Option<(OsPath, Bytes)>,
        did_replace: std::sync::atomic::AtomicBool,
        entered: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
        get_counts: Arc<std::sync::Mutex<std::collections::BTreeMap<String, u64>>>,
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
            let should_block = location == &self.blocked_path
                && (!self.block_once
                    || !self
                        .did_block
                        .swap(true, std::sync::atomic::Ordering::AcqRel));
            if should_block {
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
            if location == &self.blocked_path
                && !self
                    .did_replace
                    .swap(true, std::sync::atomic::Ordering::AcqRel)
            {
                if let Some((replacement_path, replacement)) = &self.replacement_on_get {
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
                    self.inner.delete(location).await?;
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
            block_once: false,
            did_block: std::sync::atomic::AtomicBool::new(false),
            ambiguous_path: None,
            did_return_ambiguous: std::sync::atomic::AtomicBool::new(false),
            replacement_on_get: None,
            did_replace: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            get_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
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
            block_once: true,
            did_block: std::sync::atomic::AtomicBool::new(false),
            ambiguous_path: None,
            did_return_ambiguous: std::sync::atomic::AtomicBool::new(false),
            replacement_on_get: None,
            did_replace: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            get_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
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
            block_once: true,
            did_block: std::sync::atomic::AtomicBool::new(false),
            ambiguous_path: Some(ambiguous_path),
            did_return_ambiguous: std::sync::atomic::AtomicBool::new(false),
            replacement_on_get: None,
            did_replace: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            get_counts: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            fail_delete_once: Arc::new(std::sync::Mutex::new(None)),
            track_capsule_get_concurrency: std::sync::atomic::AtomicBool::new(false),
            active_capsule_gets: std::sync::atomic::AtomicUsize::new(0),
            max_capsule_gets: std::sync::atomic::AtomicUsize::new(0),
        });
        let object_store: Arc<dyn ObjectStore> = raw.clone();
        let authority = Arc::new(LeaderLeaseStore::new(object_store, ttl_ms));
        (raw, authority)
    }

    fn catalog(name: &str) -> CatalogManifest {
        CatalogManifest::new(vec![super::super::CatalogManifestEntry {
            canonical_name: name.to_owned(),
            kind: crate::catalog::CatalogObjectKind::Source,
            ddl: format!("CREATE SOURCE {name} (id BIGINT)"),
        }])
        .unwrap()
    }

    #[test]
    fn replacement_term_may_abort_but_cannot_commit_an_existing_drain() {
        let incumbent = owner(1, 1, 1);
        let incumbent_lease = LeaderLease {
            seq: 1,
            token: 1,
            owner: incumbent.clone(),
            expires_at_ms: 1,
            catalog_manifest: None,
        };
        let transition = assignment_drain_transition(&incumbent, incumbent_lease.proof());
        let replacement = LeaderLease {
            seq: 2,
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
    async fn competing_assignment_drain_decisions_have_one_immutable_winner() {
        let (raw, store) = blocking_store_at(1_000, lease_path(2));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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
        let mut link = head.assignment_drain_decision_head;
        while let Some(current) = link {
            by_target_version.insert(current.target_version, current.sequence);
            link = read_authority_record(store.store.as_ref(), current.sequence)
                .await
                .unwrap()
                .unwrap()
                .previous_assignment_drain_decision;
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
            .assignment_drain_floor
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
                let _ = store.try_acquire(&incumbent, 10).await.unwrap();
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
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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
            .assignment_drain_floor
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
                PutPayload::from(encode_authority_record(&corrupt).unwrap()),
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
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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

        let LeaseOutcome::Acquired(renewal) = store.try_acquire(&incumbent, 1).await.unwrap()
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
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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
                .before_epoch,
            3
        );
    }

    #[tokio::test]
    async fn ambiguous_cluster_decision_reconciles_exact_canonical_winner() {
        let (raw, store) = ambiguous_once_at(1_000, lease_path(2));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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
    async fn cluster_decision_rejects_foreign_owner_and_fencing_token() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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

        let LeaseOutcome::Acquired(renewal) = store.try_acquire(&incumbent, 1).await.unwrap()
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
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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
                .before_epoch,
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
    async fn ambiguous_floor_create_revalidates_the_winner_cut() {
        let (raw, store) = ambiguous_once_at(1_000, lease_path(4));
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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

        assert_eq!(raw.get_count(&known_paths[0]), 0);
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
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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
        let LeaseOutcome::Acquired(renewed) = store.try_acquire(&incumbent, 1).await.unwrap()
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
        assert_eq!(boundary.before_epoch, 3);
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
        assert_eq!(maintenance.deleted, 1);
        assert_eq!(maintenance.quarantined, 0);
        assert!(maintenance.pending);
        assert!(decisions
            .load_recovery_capsule(&first_capsule)
            .await
            .is_err());
        decisions
            .load_recovery_capsule(&second_capsule)
            .await
            .unwrap();
        decisions
            .load_recovery_capsule(&third_capsule)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn history_prune_keeps_live_outcome_chain_and_drops_only_compacted_records() {
        let store = store(1);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
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
                let _ = store.try_acquire(&incumbent, 10).await.unwrap();
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
    async fn floor_anchor_rejects_same_attempt_with_a_different_authority_sequence() {
        let store = store(1_000);
        let incumbent = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store.try_acquire(&incumbent, 0).await.unwrap() else {
            unreachable!()
        };
        let proof = first.proof();
        let fence = assignment_fence(&incumbent);
        record_commit(&store, &proof, &fence, 1, 10).await;
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
            .terminal_anchor_link
            .as_mut()
            .unwrap()
            .sequence += 1;
        store
            .store
            .put(
                &lease_path(corrupt.lease.seq),
                PutPayload::from(encode_authority_record(&corrupt).unwrap()),
            )
            .await
            .unwrap();
        assert!(matches!(
            store.cluster_outcomes().await,
            Err(ClusterCheckpointAuthorityError::Decision(
                DecisionError::Conflict(_)
            ))
        ));
    }

    #[tokio::test]
    async fn renewals_copy_only_the_bounded_catalog_reference() {
        let store = store(1_000);
        let owner = owner(1, 1, 1);
        let LeaseOutcome::Acquired(first) = store.try_acquire(&owner, 0).await.unwrap() else {
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

        let LeaseOutcome::Acquired(renewed) = store.try_acquire(&owner, 1).await.unwrap() else {
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
        let LeaseOutcome::Acquired(first) = store.try_acquire(&owner, 0).await.unwrap() else {
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
    #[tokio::test]
    async fn candidacy_loss_interrupts_hung_renewal_and_fences() {
        let (raw, store) = blocking_store(80);
        let owner = owner(1, 1, 1);
        let manager = LeaderLeaseManager::new(
            store,
            &process(&owner),
            LeaderLeaseConfig {
                ttl: Duration::from_millis(80),
                renew_interval: Duration::from_millis(10),
            },
        )
        .unwrap();
        let deadline = manager.deadline();
        let mut lease = manager.lease_watch();
        let (candidate_tx, candidate_rx) = watch::channel(true);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let task = manager.spawn(shutdown.clone(), candidate_rx);
        wait_for_lease(&mut lease).await;
        tokio::time::timeout(Duration::from_secs(1), raw.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        candidate_tx.send(false).unwrap();
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
        let (_candidate_tx, candidate_rx) = watch::channel(true);
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
        let (_candidate_tx, candidate_rx) = watch::channel(true);
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
