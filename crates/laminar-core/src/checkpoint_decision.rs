//! Durable checkpoint identity, ID allocation, and immutable terminal outcomes.

#[cfg(feature = "cluster")]
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
#[cfg(feature = "cluster")]
use sha2::{Digest, Sha256};

use crate::checkpoint::{
    CheckpointAssignmentFence, ClusterRecoveryCapsule, LeaderProof, RecoveryCapsuleRef,
};

/// Durable checkpoint metadata store.
pub struct CheckpointDecisionStore {
    store: Arc<dyn ObjectStore>,
    /// Serializes candidates from this instance so its durable creates cannot reorder.
    reservation_lock: tokio::sync::Mutex<()>,
    /// Highest reservation observed or attempted by this store instance.
    reservation_hint: AtomicU64,
    deployment_id: tokio::sync::OnceCell<String>,
}

impl std::fmt::Debug for CheckpointDecisionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointDecisionStore")
            .finish_non_exhaustive()
    }
}

/// Errors raised by [`CheckpointDecisionStore`] operations.
#[derive(Debug, thiserror::Error)]
pub enum DecisionError {
    /// Underlying object-store I/O failure.
    #[error("object store I/O: {0}")]
    Io(String),
    /// A persisted decision is malformed or conflicts with the requested cut.
    #[error("checkpoint decision conflict: {0}")]
    Conflict(String),
    /// The immutable inventory changed while it was being enumerated, normally because GC
    /// retired a resolved pair. Callers may retry the complete audit from a fresh LIST.
    #[error("checkpoint decision inventory changed during audit: {0}")]
    InventoryChanged(String),
}

/// Runtime scope of a durable checkpoint outcome.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointScope {
    /// Embedded or standalone recovery domain.
    Local,
    /// Multi-participant cluster recovery domain.
    Cluster,
}

/// Irrevocable terminal verdict for one concrete checkpoint attempt.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointVerdict {
    /// Every participant prepared successfully and recovery must finish commit.
    Commit,
    /// Recovery must roll back this exact checkpoint attempt.
    Abort,
}

/// Single create-once terminal outcome for one epoch.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct CheckpointOutcome {
    /// Outcome payload format.
    pub version: u32,
    /// Recovery domain that created the outcome.
    pub scope: CheckpointScope,
    /// Terminal epoch, matching the object path.
    pub epoch: u64,
    /// Exact checkpoint attempt resolved for the epoch.
    pub checkpoint_id: u64,
    /// Durable deployment incarnation that owns the outcome.
    pub deployment_id: String,
    /// Exact assignment certificate for a cluster outcome.
    pub assignment_fence: Option<CheckpointAssignmentFence>,
    /// Exact leader authority that selected a cluster outcome.
    pub leader_proof: Option<LeaderProof>,
    /// Exact global recovery image selected by a cluster Commit.
    pub recovery_capsule: Option<RecoveryCapsuleRef>,
    /// Create-once terminal verdict.
    pub verdict: CheckpointVerdict,
}

/// Result of attempting to create a terminal checkpoint outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordOutcomeResult {
    /// This call durably created the outcome.
    Created(CheckpointOutcome),
    /// An identical outcome was already durable.
    Unchanged(CheckpointOutcome),
    /// A different outcome won the create-once race for the epoch.
    Conflict {
        /// Durable winner that callers must obey.
        winner: CheckpointOutcome,
    },
}

/// Scalar continuity boundary for outcome retention.
///
/// This is not a checkpoint recovery target: it deliberately carries neither an assignment fence,
/// leader proof, verdict, nor manifest owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutcomeRetentionBoundary {
    /// Outcomes below this epoch are continuity-only and their raw records may be absent.
    pub before_epoch: u64,
    /// Greatest committed checkpoint ID compacted below the horizon, if any.
    pub committed_checkpoint_id: Option<u64>,
    /// Greatest terminal epoch compacted below the horizon, including aborts.
    pub highest_closed_epoch: Option<u64>,
}

const CHECKPOINT_OUTCOME_VERSION: u32 = 2;
#[cfg(feature = "cluster")]
const RECOVERY_CAPSULE_GC_BATCH_SIZE: usize = 64;

impl CheckpointOutcome {
    pub(crate) fn validate_shape(&self, path_epoch: u64) -> Result<(), DecisionError> {
        if self.version != CHECKPOINT_OUTCOME_VERSION {
            return Err(DecisionError::Conflict(format!(
                "outcome for epoch {path_epoch} has unsupported version {}; expected \
                 {CHECKPOINT_OUTCOME_VERSION}",
                self.version
            )));
        }
        if self.epoch == 0 || self.epoch != path_epoch {
            return Err(DecisionError::Conflict(format!(
                "outcome path epoch {path_epoch} does not match non-zero payload epoch {}",
                self.epoch
            )));
        }
        if self.checkpoint_id == 0 {
            return Err(DecisionError::Conflict(format!(
                "outcome for epoch {path_epoch} has checkpoint ID 0"
            )));
        }

        let deployment = uuid::Uuid::parse_str(&self.deployment_id).map_err(|error| {
            DecisionError::Conflict(format!(
                "outcome for epoch {path_epoch} has invalid deployment identity: {error}"
            ))
        })?;
        if deployment.is_nil() || deployment.to_string() != self.deployment_id {
            return Err(DecisionError::Conflict(format!(
                "outcome for epoch {path_epoch} must use a canonical non-nil deployment identity"
            )));
        }

        match (
            self.scope,
            self.assignment_fence.as_ref(),
            self.leader_proof.as_ref(),
        ) {
            (CheckpointScope::Local, None, None) => {}
            (CheckpointScope::Local, _, _) => {
                return Err(DecisionError::Conflict(format!(
                    "local outcome for epoch {path_epoch} cannot carry an assignment fence or \
                     leader proof"
                )));
            }
            (CheckpointScope::Cluster, Some(fence), Some(proof))
                if fence.is_canonical()
                    && proof.is_canonical()
                    && fence.participant_incarnation(proof.owner.node_id)
                        == Some(proof.owner.boot_id) => {}
            (CheckpointScope::Cluster, Some(fence), Some(proof))
                if !fence.is_canonical() || !proof.is_canonical() =>
            {
                return Err(DecisionError::Conflict(format!(
                    "cluster outcome for epoch {path_epoch} has a non-canonical assignment fence \
                     or leader proof"
                )));
            }
            (CheckpointScope::Cluster, Some(_), Some(proof)) => {
                return Err(DecisionError::Conflict(format!(
                    "cluster outcome for epoch {path_epoch} leader node {} boot {} is absent from \
                     the assignment fence",
                    proof.owner.node_id, proof.owner.boot_id
                )));
            }
            (CheckpointScope::Cluster, _, _) => {
                return Err(DecisionError::Conflict(format!(
                    "cluster outcome for epoch {path_epoch} requires an assignment fence and \
                     leader proof"
                )));
            }
        }

        match (self.scope, &self.verdict, self.recovery_capsule.as_ref()) {
            (CheckpointScope::Local, _, None)
            | (CheckpointScope::Cluster, CheckpointVerdict::Abort, None) => {}
            (CheckpointScope::Cluster, CheckpointVerdict::Commit, Some(reference)) => {
                reference.validate().map_err(|error| {
                    DecisionError::Conflict(format!(
                        "cluster commit outcome for epoch {path_epoch} has an invalid recovery capsule reference: {error}"
                    ))
                })?;
            }
            (CheckpointScope::Local, _, Some(_)) => {
                return Err(DecisionError::Conflict(format!(
                    "local outcome for epoch {path_epoch} cannot carry a recovery capsule"
                )));
            }
            (CheckpointScope::Cluster, CheckpointVerdict::Commit, None) => {
                return Err(DecisionError::Conflict(format!(
                    "cluster commit outcome for epoch {path_epoch} requires a recovery capsule"
                )));
            }
            (CheckpointScope::Cluster, CheckpointVerdict::Abort, Some(_)) => {
                return Err(DecisionError::Conflict(format!(
                    "cluster abort outcome for epoch {path_epoch} cannot carry a recovery capsule"
                )));
            }
        }
        Ok(())
    }

    /// Whether this outcome irrevocably selected commit.
    #[must_use]
    pub fn is_commit(&self) -> bool {
        matches!(self.verdict, CheckpointVerdict::Commit)
    }
}

/// Durable allocator state for globally unique checkpoint IDs.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct CheckpointIdReservation {
    /// Reservation payload format.
    version: u32,
    /// Reserved checkpoint ID, matching the object name.
    checkpoint_id: u64,
}

const CHECKPOINT_ID_RESERVATION_VERSION: u32 = 1;

/// Create-once identity for one durable checkpoint/decision-store incarnation.
/// A storage reset deliberately creates a new value so surviving external sinks cannot mistake
/// a restarted checkpoint-id sequence for their prior writer.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct DeploymentIdentity {
    version: u32,
    id: String,
}

const DEPLOYMENT_IDENTITY_VERSION: u32 = 1;

/// Monotonic tombstone for terminal outcomes below `before_epoch`.
///
/// The terminal and committed anchors are continuity metadata only. Recovery must select a live
/// commit outcome at or above the floor rather than treating an anchor whose checkpoint artifacts
/// may have been deleted as a recovery cut. The canonical object is advanced with compare-and-swap
/// so concurrent retention workers cannot regress either the horizon or its anchors.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct OutcomeGcFloor {
    version: u32,
    deployment_id: String,
    before_epoch: u64,
    terminal_anchor: Option<CheckpointOutcome>,
    committed_anchor: Option<CheckpointOutcome>,
}

const OUTCOME_GC_FLOOR_VERSION: u32 = 4;

#[derive(Debug)]
struct VersionedOutcomeGcFloor {
    floor: OutcomeGcFloor,
    update_version: UpdateVersion,
}

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RecoveryCapsuleGcCursor {
    version: u32,
    deployment_id: String,
    /// Last path examined in the current pass. `None` starts a new pass from the prefix root.
    offset: Option<String>,
}

#[cfg(feature = "cluster")]
const RECOVERY_CAPSULE_GC_CURSOR_VERSION: u32 = 1;

#[cfg(feature = "cluster")]
#[derive(Debug)]
struct RecoveryCapsuleListCandidate {
    location: String,
    meta: object_store::ObjectMeta,
}

#[cfg(feature = "cluster")]
impl PartialEq for RecoveryCapsuleListCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.location == other.location
    }
}

#[cfg(feature = "cluster")]
impl Eq for RecoveryCapsuleListCandidate {}

#[cfg(feature = "cluster")]
impl PartialOrd for RecoveryCapsuleListCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(feature = "cluster")]
impl Ord for RecoveryCapsuleListCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.location.cmp(&other.location)
    }
}

#[cfg(feature = "cluster")]
fn retain_lexically_oldest_recovery_capsule(
    oldest: &mut BinaryHeap<RecoveryCapsuleListCandidate>,
    meta: object_store::ObjectMeta,
) {
    let candidate = RecoveryCapsuleListCandidate {
        location: meta.location.to_string(),
        meta,
    };
    if oldest.len() < RECOVERY_CAPSULE_GC_BATCH_SIZE {
        oldest.push(candidate);
    } else if oldest.peek().is_some_and(|largest| &candidate < largest) {
        oldest.pop();
        oldest.push(candidate);
    }
}

#[cfg(feature = "cluster")]
#[derive(Debug)]
struct VersionedRecoveryCapsuleGcCursor {
    cursor: RecoveryCapsuleGcCursor,
    update_version: UpdateVersion,
}

/// Result of one bounded cluster recovery-capsule maintenance step.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryCapsuleGcStep {
    /// Number of active-prefix entries selected and processed in this step.
    pub examined: usize,
    /// Number of old unreferenced capsules deleted.
    pub deleted: usize,
    /// Number of malformed or corrupt objects moved out of the active prefix.
    pub quarantined: usize,
    /// Whether an existing floor requires periodic rescans for delayed or previously failed work.
    pub pending: bool,
}

impl CheckpointDecisionStore {
    /// Wrap an existing object store.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            reservation_lock: tokio::sync::Mutex::new(()),
            reservation_hint: AtomicU64::new(0),
            deployment_id: tokio::sync::OnceCell::new(),
        }
    }

    fn outcome_root() -> OsPath {
        OsPath::from("checkpoint-outcomes/")
    }

    fn outcome_path(epoch: u64) -> OsPath {
        OsPath::from(format!("checkpoint-outcomes/epoch={epoch}/outcome"))
    }

    fn recovery_capsule_path(reference: &RecoveryCapsuleRef) -> OsPath {
        OsPath::from(format!(
            "checkpoint-recovery-capsules/epoch={:020}/checkpoint={:020}/sha256={}",
            reference.epoch, reference.checkpoint_id, reference.sha256
        ))
    }

    #[cfg(feature = "cluster")]
    fn recovery_capsule_root() -> OsPath {
        OsPath::from("checkpoint-recovery-capsules/")
    }

    #[cfg(feature = "cluster")]
    fn recovery_capsule_coordinates_from_path(location: &str) -> Option<(u64, u64, &str)> {
        let suffix = location.strip_prefix("checkpoint-recovery-capsules/epoch=")?;
        let (epoch, suffix) = suffix.split_once("/checkpoint=")?;
        let (checkpoint_id, digest) = suffix.split_once("/sha256=")?;
        if epoch.len() != 20
            || checkpoint_id.len() != 20
            || digest.len() != 64
            || digest.contains('/')
        {
            return None;
        }
        Some((epoch.parse().ok()?, checkpoint_id.parse().ok()?, digest))
    }

    #[cfg(feature = "cluster")]
    fn recovery_capsule_gc_cursor_path(deployment_id: &str) -> OsPath {
        OsPath::from(format!(
            "checkpoint-recovery-capsule-gc/deployment={deployment_id}/cursor"
        ))
    }

    #[cfg(feature = "cluster")]
    fn recovery_capsule_quarantine_path(location: &str) -> OsPath {
        let digest = format!("{:x}", Sha256::digest(location.as_bytes()));
        OsPath::from(format!(
            "checkpoint-recovery-capsule-quarantine/path-sha256={digest}"
        ))
    }

    fn outcome_gc_floor_path(deployment_id: &str) -> OsPath {
        OsPath::from(format!(
            "checkpoint-outcome-gc/deployment={deployment_id}/floor"
        ))
    }

    fn reservation_root() -> OsPath {
        OsPath::from("checkpoint-id-reservations/")
    }

    fn reservation_path(checkpoint_id: u64) -> OsPath {
        OsPath::from(format!("checkpoint-id-reservations/id={checkpoint_id}"))
    }

    fn deployment_identity_path() -> OsPath {
        OsPath::from("checkpoint-deployment/identity.json")
    }

    #[cfg(feature = "cluster")]
    async fn read_recovery_capsule_gc_cursor(
        &self,
        deployment_id: &str,
    ) -> Result<Option<VersionedRecoveryCapsuleGcCursor>, DecisionError> {
        let path = Self::recovery_capsule_gc_cursor_path(deployment_id);
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(DecisionError::Io(error.to_string())),
        };
        let update_version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let bytes = result
            .bytes()
            .await
            .map_err(|error| DecisionError::Io(error.to_string()))?;
        let cursor: RecoveryCapsuleGcCursor = serde_json::from_slice(&bytes).map_err(|error| {
            DecisionError::Conflict(format!("recovery capsule GC cursor: {error}"))
        })?;
        if cursor.version != RECOVERY_CAPSULE_GC_CURSOR_VERSION
            || cursor.deployment_id != deployment_id
            || cursor.offset.as_ref().is_some_and(|offset| {
                !offset.starts_with("checkpoint-recovery-capsules/")
                    || OsPath::parse(offset).is_err()
            })
        {
            return Err(DecisionError::Conflict(
                "recovery capsule GC cursor does not match its path, deployment, or progress"
                    .into(),
            ));
        }
        let canonical = serde_json::to_vec(&cursor)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(DecisionError::Conflict(
                "recovery capsule GC cursor does not use its canonical body".into(),
            ));
        }
        Ok(Some(VersionedRecoveryCapsuleGcCursor {
            cursor,
            update_version,
        }))
    }

    #[cfg(feature = "cluster")]
    async fn compare_and_swap_recovery_capsule_gc_cursor(
        &self,
        cursor: &RecoveryCapsuleGcCursor,
        expected: Option<UpdateVersion>,
    ) -> Result<bool, DecisionError> {
        let path = Self::recovery_capsule_gc_cursor_path(&cursor.deployment_id);
        let payload = serde_json::to_vec(cursor)
            .map(Bytes::from)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        let options = PutOptions {
            mode: expected.map_or(PutMode::Create, PutMode::Update),
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(&path, PutPayload::from(payload), options)
            .await
        {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. }
                | object_store::Error::NotFound { .. },
            ) => Ok(false),
            Err(error) => match self
                .read_recovery_capsule_gc_cursor(&cursor.deployment_id)
                .await?
            {
                Some(current) if current.cursor == *cursor => Ok(true),
                _ => Err(DecisionError::Io(error.to_string())),
            },
        }
    }

    /// Create the canonical content-addressed body for a cluster recovery capsule.
    ///
    /// Identical retries converge on the existing immutable body. The returned reference is safe
    /// to publish only after this method succeeds.
    ///
    /// # Errors
    /// Object-store I/O, malformed capsule content, deployment mismatch, or a conflicting body.
    pub async fn create_recovery_capsule(
        &self,
        capsule: &ClusterRecoveryCapsule,
    ) -> Result<RecoveryCapsuleRef, DecisionError> {
        let (encoded, reference) = capsule
            .encode_and_reference()
            .map_err(DecisionError::Conflict)?;
        let deployment_id = self.load_or_create_deployment_id().await?;
        if capsule.deployment_id != deployment_id {
            return Err(DecisionError::Conflict(format!(
                "recovery capsule belongs to deployment {}, current deployment is {deployment_id}",
                capsule.deployment_id
            )));
        }

        let path = Self::recovery_capsule_path(&reference);
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let put_error = self
            .store
            .put_opts(
                &path,
                PutPayload::from(Bytes::copy_from_slice(&encoded)),
                options,
            )
            .await
            .err();

        match self.load_recovery_capsule(&reference).await {
            Ok(stored) if stored == *capsule => Ok(reference),
            Ok(_) => Err(DecisionError::Conflict(format!(
                "recovery capsule '{}' differs from the proposed content",
                reference.sha256
            ))),
            Err(reconcile_error) => {
                if let Some(put_error) = put_error {
                    Err(DecisionError::Io(format!(
                        "recovery capsule write failed ({put_error}); reconciliation failed ({reconcile_error})"
                    )))
                } else {
                    Err(reconcile_error)
                }
            }
        }
    }

    /// Load and verify one exact content-addressed recovery capsule.
    ///
    /// Verification covers the object path, recorded and observed lengths, canonical JSON body,
    /// SHA-256 reference, deployment identity, and capsule-internal invariants.
    ///
    /// # Errors
    /// Object-store I/O, a missing object, malformed content, or any reference mismatch.
    pub async fn load_recovery_capsule(
        &self,
        reference: &RecoveryCapsuleRef,
    ) -> Result<ClusterRecoveryCapsule, DecisionError> {
        reference.validate().map_err(DecisionError::Conflict)?;
        let result = match self
            .store
            .get(&Self::recovery_capsule_path(reference))
            .await
        {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(DecisionError::Conflict(format!(
                    "recovery capsule '{}' is missing",
                    reference.sha256
                )));
            }
            Err(error) => return Err(DecisionError::Io(error.to_string())),
        };
        if result.meta.size != reference.len {
            return Err(DecisionError::Conflict(format!(
                "recovery capsule '{}' is {} bytes, expected {}",
                reference.sha256, result.meta.size, reference.len
            )));
        }
        let bytes = result
            .bytes()
            .await
            .map_err(|error| DecisionError::Io(error.to_string()))?;
        if u64::try_from(bytes.len()).ok() != Some(reference.len) {
            return Err(DecisionError::Conflict(format!(
                "recovery capsule '{}' payload length changed while reading",
                reference.sha256
            )));
        }
        let capsule: ClusterRecoveryCapsule = serde_json::from_slice(&bytes).map_err(|error| {
            DecisionError::Conflict(format!("recovery capsule '{}': {error}", reference.sha256))
        })?;
        let (canonical, actual_reference) = capsule
            .encode_and_reference()
            .map_err(DecisionError::Conflict)?;
        if actual_reference != *reference || canonical.as_slice() != bytes.as_ref() {
            return Err(DecisionError::Conflict(format!(
                "recovery capsule '{}' does not match its content-addressed reference",
                reference.sha256
            )));
        }
        let deployment_id = self.load_or_create_deployment_id().await?;
        if capsule.deployment_id != deployment_id {
            return Err(DecisionError::Conflict(format!(
                "recovery capsule belongs to deployment {}, current deployment is {deployment_id}",
                capsule.deployment_id
            )));
        }
        Ok(capsule)
    }

    pub(crate) async fn validate_recovery_capsule_for_outcome(
        &self,
        outcome: &CheckpointOutcome,
    ) -> Result<(), DecisionError> {
        outcome.validate_shape(outcome.epoch)?;
        let reference = outcome.recovery_capsule.as_ref().ok_or_else(|| {
            DecisionError::Conflict(format!(
                "cluster commit outcome for epoch {} requires a recovery capsule",
                outcome.epoch
            ))
        })?;
        let capsule = self.load_recovery_capsule(reference).await?;
        if capsule.attempt.epoch != outcome.epoch
            || capsule.attempt.checkpoint_id != outcome.checkpoint_id
            || Some(&capsule.assignment_fence) != outcome.assignment_fence.as_ref()
            || capsule.deployment_id != outcome.deployment_id
        {
            return Err(DecisionError::Conflict(format!(
                "cluster commit outcome for epoch {} does not match recovery capsule '{}'",
                outcome.epoch, reference.sha256
            )));
        }
        Ok(())
    }

    fn decode_deployment_identity(bytes: &[u8]) -> Result<String, DecisionError> {
        let identity: DeploymentIdentity = serde_json::from_slice(bytes)
            .map_err(|error| DecisionError::Conflict(format!("deployment identity: {error}")))?;
        if identity.version != DEPLOYMENT_IDENTITY_VERSION {
            return Err(DecisionError::Conflict(format!(
                "deployment identity version {} is unsupported (expected \
                 {DEPLOYMENT_IDENTITY_VERSION})",
                identity.version
            )));
        }
        let parsed = uuid::Uuid::parse_str(&identity.id).map_err(|error| {
            DecisionError::Conflict(format!("deployment identity is not a UUID: {error}"))
        })?;
        let canonical = parsed.to_string();
        if canonical != identity.id || parsed.is_nil() {
            return Err(DecisionError::Conflict(
                "deployment identity must be a canonical non-nil UUID".into(),
            ));
        }
        Ok(canonical)
    }

    async fn read_deployment_identity(&self) -> Result<Option<String>, DecisionError> {
        let result = match self.store.get(&Self::deployment_identity_path()).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(DecisionError::Io(error.to_string())),
        };
        let bytes = result
            .bytes()
            .await
            .map_err(|error| DecisionError::Io(error.to_string()))?;
        Self::decode_deployment_identity(&bytes).map(Some)
    }

    /// Load the checkpoint namespace's create-once deployment incarnation, creating it when the
    /// durable store is empty. Concurrent cluster members converge through object-store CAS.
    ///
    /// # Errors
    /// Object-store I/O or a malformed/conflicting persisted identity.
    pub async fn load_or_create_deployment_id(&self) -> Result<String, DecisionError> {
        if let Some(identity) = self.deployment_id.get() {
            return Ok(identity.clone());
        }
        let _guard = self.reservation_lock.lock().await;
        if let Some(identity) = self.deployment_id.get() {
            return Ok(identity.clone());
        }
        if let Some(identity) = self.read_deployment_identity().await? {
            let _ = self.deployment_id.set(identity.clone());
            return Ok(identity);
        }

        let identity = DeploymentIdentity {
            version: DEPLOYMENT_IDENTITY_VERSION,
            id: uuid::Uuid::now_v7().to_string(),
        };
        let payload = serde_json::to_vec(&identity)
            .map(Bytes::from)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(
                &Self::deployment_identity_path(),
                PutPayload::from(payload),
                options,
            )
            .await
        {
            Ok(_) => {
                let _ = self.deployment_id.set(identity.id.clone());
                Ok(identity.id)
            }
            Err(
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. },
            ) => {
                let stored = self.read_deployment_identity().await?.ok_or_else(|| {
                    DecisionError::Conflict(
                        "deployment identity disappeared after create conflict".into(),
                    )
                })?;
                let _ = self.deployment_id.set(stored.clone());
                Ok(stored)
            }
            Err(error) => Err(DecisionError::Io(error.to_string())),
        }
    }

    fn reservation_id(location: &str) -> Result<u64, DecisionError> {
        let value = location
            .strip_prefix("checkpoint-id-reservations/id=")
            .ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "malformed checkpoint ID reservation path: {location}"
                ))
            })?;
        if value.is_empty() || value.contains('/') {
            return Err(DecisionError::Conflict(format!(
                "malformed checkpoint ID reservation path: {location}"
            )));
        }
        let checkpoint_id = value.parse::<u64>().map_err(|_| {
            DecisionError::Conflict(format!(
                "malformed checkpoint ID reservation path: {location}"
            ))
        })?;
        if checkpoint_id == 0 {
            return Err(DecisionError::Conflict(
                "checkpoint ID reservation cannot contain ID 0".to_owned(),
            ));
        }
        Ok(checkpoint_id)
    }

    fn encode_reservation(reservation: CheckpointIdReservation) -> Result<Bytes, DecisionError> {
        serde_json::to_vec(&reservation)
            .map(Bytes::from)
            .map_err(|e| DecisionError::Conflict(e.to_string()))
    }

    async fn initialize_reservation_hint(&self) -> Result<(), DecisionError> {
        if self.reservation_hint.load(Ordering::Acquire) != 0 {
            return Ok(());
        }

        let root = Self::reservation_root();
        let mut entries = self.store.list(Some(&root));
        let mut highest = 0_u64;
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|e| DecisionError::Io(e.to_string()))?;
            highest = highest.max(Self::reservation_id(entry.location.as_ref())?);
        }
        self.reservation_hint.fetch_max(highest, Ordering::AcqRel);
        Ok(())
    }

    fn next_reservation_candidate(&self) -> Result<u64, DecisionError> {
        let prior = self
            .reservation_hint
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |last| {
                last.checked_add(1)
            })
            .map_err(|_| DecisionError::Conflict("checkpoint ID space exhausted u64".to_owned()))?;
        Ok(prior + 1)
    }

    /// Allocate a globally unique, monotonically increasing checkpoint ID.
    ///
    /// Each allocation is an immutable, versioned reservation object. A store
    /// instance lists the durable reservations once to seed a local hint, then
    /// uses `PutMode::Create` to atomically claim candidates. Create races retry
    /// with the next candidate, so concurrent coordinators cannot return the
    /// same ID. IDs may have gaps when a caller crashes after allocation. These
    /// reservation objects are permanent: deleting them can permit ID reuse.
    ///
    /// # Errors
    /// Object-store I/O, a malformed durable reservation path, or exhaustion
    /// of the `u64` ID space.
    pub async fn allocate_checkpoint_id(&self) -> Result<u64, DecisionError> {
        let _guard = self.reservation_lock.lock().await;
        self.initialize_reservation_hint().await?;

        loop {
            let checkpoint_id = self.next_reservation_candidate()?;
            let reservation = CheckpointIdReservation {
                version: CHECKPOINT_ID_RESERVATION_VERSION,
                checkpoint_id,
            };
            let opts = PutOptions {
                mode: PutMode::Create,
                ..PutOptions::default()
            };
            match self
                .store
                .put_opts(
                    &Self::reservation_path(checkpoint_id),
                    PutPayload::from(Self::encode_reservation(reservation)?),
                    opts,
                )
                .await
            {
                Ok(_) => return Ok(checkpoint_id),
                Err(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. },
                ) => {
                    tokio::task::yield_now().await;
                }
                Err(e) => return Err(DecisionError::Io(e.to_string())),
            }
        }
    }

    /// Epoch segment of a canonical create-once terminal outcome object.
    fn outcome_epoch_segment(loc: &str) -> Option<&str> {
        loc.strip_prefix("checkpoint-outcomes/")?
            .strip_suffix("/outcome")?
            .strip_prefix("epoch=")
    }

    pub(crate) async fn canonical_outcome(
        &self,
        epoch: u64,
        checkpoint_id: u64,
        scope: CheckpointScope,
        assignment_fence: Option<CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        verdict: CheckpointVerdict,
        recovery_capsule: Option<RecoveryCapsuleRef>,
    ) -> Result<CheckpointOutcome, DecisionError> {
        let outcome = CheckpointOutcome {
            version: CHECKPOINT_OUTCOME_VERSION,
            scope,
            epoch,
            checkpoint_id,
            deployment_id: self.load_or_create_deployment_id().await?,
            assignment_fence,
            leader_proof,
            recovery_capsule,
            verdict,
        };
        if outcome.recovery_capsule.is_some() {
            self.validate_recovery_capsule_for_outcome(&outcome).await?;
        } else {
            outcome.validate_shape(epoch)?;
        }
        Ok(outcome)
    }

    async fn read_outcome_record(
        &self,
        path: &OsPath,
        epoch: u64,
    ) -> Result<Option<CheckpointOutcome>, DecisionError> {
        let result = match self.store.get(path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(DecisionError::Io(error.to_string())),
        };
        let bytes = result
            .bytes()
            .await
            .map_err(|error| DecisionError::Io(error.to_string()))?;
        let outcome: CheckpointOutcome = serde_json::from_slice(&bytes)
            .map_err(|error| DecisionError::Conflict(format!("outcome epoch {epoch}: {error}")))?;
        outcome.validate_shape(epoch)?;
        if outcome.scope == CheckpointScope::Cluster {
            return Err(DecisionError::Conflict(format!(
                "cluster outcome epoch {epoch} is stored outside the shared leader authority"
            )));
        }
        let canonical = serde_json::to_vec(&outcome)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(DecisionError::Conflict(format!(
                "outcome for epoch {epoch} does not use the canonical body"
            )));
        }
        let expected_deployment = self.load_or_create_deployment_id().await?;
        if outcome.deployment_id != expected_deployment {
            return Err(DecisionError::Conflict(format!(
                "outcome for epoch {epoch} belongs to deployment {}, current deployment is \
                 {expected_deployment}",
                outcome.deployment_id
            )));
        }
        Ok(Some(outcome))
    }

    async fn create_outcome(
        &self,
        candidate: CheckpointOutcome,
    ) -> Result<RecordOutcomeResult, DecisionError> {
        let path = Self::outcome_path(candidate.epoch);
        let payload = serde_json::to_vec(&candidate)
            .map(Bytes::from)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let Err(create_error) = self
            .store
            .put_opts(&path, PutPayload::from(payload), options)
            .await
        else {
            return Ok(RecordOutcomeResult::Created(candidate));
        };

        // A failed create response may be ambiguous: read the create-once key before deciding
        // whether the call failed. Recovery will not proceed past a valid Prepared witness until
        // this exact epoch has a terminal winner.
        if let Some(winner) = self.read_outcome_record(&path, candidate.epoch).await? {
            return if winner == candidate {
                Ok(RecordOutcomeResult::Unchanged(winner))
            } else {
                Ok(RecordOutcomeResult::Conflict { winner })
            };
        }

        match create_error {
            object_store::Error::Precondition { .. }
            | object_store::Error::AlreadyExists { .. } => Err(DecisionError::Conflict(format!(
                "outcome for epoch {} disappeared after create conflict",
                candidate.epoch
            ))),
            error => Err(DecisionError::Io(error.to_string())),
        }
    }

    async fn read_outcome_gc_floor(
        &self,
        deployment_id: &str,
    ) -> Result<Option<VersionedOutcomeGcFloor>, DecisionError> {
        let path = Self::outcome_gc_floor_path(deployment_id);
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(DecisionError::Io(error.to_string())),
        };
        let update_version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let bytes = result
            .bytes()
            .await
            .map_err(|error| DecisionError::Io(error.to_string()))?;
        let floor: OutcomeGcFloor = serde_json::from_slice(&bytes)
            .map_err(|error| DecisionError::Conflict(format!("outcome GC floor: {error}")))?;
        let before_epoch = floor.before_epoch;
        if floor.version != OUTCOME_GC_FLOOR_VERSION
            || floor.before_epoch == 0
            || floor.deployment_id != deployment_id
        {
            return Err(DecisionError::Conflict(
                "outcome GC floor does not match its canonical path, deployment, and version"
                    .to_owned(),
            ));
        }
        if let Some(anchor) = floor.terminal_anchor.as_ref() {
            anchor.validate_shape(anchor.epoch)?;
            if anchor.epoch >= floor.before_epoch || anchor.deployment_id != floor.deployment_id {
                return Err(DecisionError::Conflict(format!(
                    "outcome GC floor {before_epoch} has invalid terminal anchor epoch {} checkpoint {}",
                    anchor.epoch, anchor.checkpoint_id
                )));
            }
        }
        if let Some(anchor) = floor.committed_anchor.as_ref() {
            anchor.validate_shape(anchor.epoch)?;
            if !anchor.is_commit()
                || anchor.epoch >= floor.before_epoch
                || anchor.deployment_id != floor.deployment_id
            {
                return Err(DecisionError::Conflict(format!(
                    "outcome GC floor {before_epoch} has invalid committed anchor epoch {} checkpoint {}",
                    anchor.epoch, anchor.checkpoint_id
                )));
            }
            let terminal = floor.terminal_anchor.as_ref().ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "outcome GC floor {before_epoch} has a committed anchor without a terminal anchor"
                ))
            })?;
            let ordered = if anchor.epoch == terminal.epoch {
                anchor == terminal
            } else {
                anchor.epoch < terminal.epoch && anchor.checkpoint_id < terminal.checkpoint_id
            };
            if !ordered {
                return Err(DecisionError::Conflict(format!(
                    "outcome GC floor {before_epoch} committed anchor epoch {} checkpoint {} is not ordered before terminal anchor epoch {} checkpoint {}",
                    anchor.epoch,
                    anchor.checkpoint_id,
                    terminal.epoch,
                    terminal.checkpoint_id
                )));
            }
        }
        if floor
            .terminal_anchor
            .as_ref()
            .is_some_and(CheckpointOutcome::is_commit)
            && floor.committed_anchor != floor.terminal_anchor
        {
            return Err(DecisionError::Conflict(format!(
                "outcome GC floor {before_epoch} does not retain its terminal commit as the committed anchor"
            )));
        }
        let canonical = serde_json::to_vec(&floor)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(DecisionError::Conflict(format!(
                "outcome GC floor {before_epoch} does not use its canonical body"
            )));
        }
        Ok(Some(VersionedOutcomeGcFloor {
            floor,
            update_version,
        }))
    }

    async fn current_outcome_gc_floor(&self) -> Result<Option<OutcomeGcFloor>, DecisionError> {
        let deployment_id = self.load_or_create_deployment_id().await?;
        Ok(self
            .read_outcome_gc_floor(&deployment_id)
            .await?
            .map(|versioned| versioned.floor))
    }

    async fn ensure_outcome_not_tombstoned(
        &self,
        outcome: &CheckpointOutcome,
    ) -> Result<(), DecisionError> {
        if let Some(floor) = self.current_outcome_gc_floor().await? {
            if outcome.epoch < floor.before_epoch {
                return Err(DecisionError::Conflict(format!(
                    "outcome epoch {} checkpoint {} is below durable outcome GC horizon {}",
                    outcome.epoch, outcome.checkpoint_id, floor.before_epoch
                )));
            }
        }
        Ok(())
    }

    /// Advance the canonical floor if `expected` still names its current object version.
    ///
    /// `false` is ordinary CAS contention and requires the caller to rebuild its candidate from
    /// the winner. Ambiguous write failures are reconciled by reading the canonical object.
    async fn compare_and_swap_outcome_gc_floor(
        &self,
        floor: &OutcomeGcFloor,
        expected: Option<UpdateVersion>,
    ) -> Result<bool, DecisionError> {
        let path = Self::outcome_gc_floor_path(&floor.deployment_id);
        let payload = serde_json::to_vec(floor)
            .map(Bytes::from)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        let options = PutOptions {
            mode: expected.map_or(PutMode::Create, PutMode::Update),
            ..PutOptions::default()
        };
        let result = self
            .store
            .put_opts(&path, PutPayload::from(payload), options)
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. }
                | object_store::Error::NotFound { .. },
            ) => Ok(false),
            Err(error) => match self.read_outcome_gc_floor(&floor.deployment_id).await? {
                Some(current) if current.floor.before_epoch >= floor.before_epoch => Ok(true),
                _ => Err(DecisionError::Io(error.to_string())),
            },
        }
    }

    /// Create or read the one local terminal outcome allowed for an epoch.
    ///
    /// Identical retries return [`RecordOutcomeResult::Unchanged`]. A different checkpoint,
    /// authority, assignment, recovery image, or verdict returns the durable winner in
    /// [`RecordOutcomeResult::Conflict`]; it never overwrites that winner.
    ///
    /// # Errors
    /// Object-store I/O, malformed/non-canonical metadata, or any cluster-scoped proposal.
    pub async fn record_outcome(
        &self,
        epoch: u64,
        checkpoint_id: u64,
        scope: CheckpointScope,
        assignment_fence: Option<CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        verdict: CheckpointVerdict,
        recovery_capsule: Option<RecoveryCapsuleRef>,
    ) -> Result<RecordOutcomeResult, DecisionError> {
        if scope == CheckpointScope::Cluster {
            return Err(DecisionError::Conflict(
                "cluster outcomes must be admitted through the shared leader authority".into(),
            ));
        }
        let candidate = self
            .canonical_outcome(
                epoch,
                checkpoint_id,
                scope,
                assignment_fence,
                leader_proof,
                verdict,
                recovery_capsule,
            )
            .await?;
        self.ensure_outcome_not_tombstoned(&candidate).await?;
        let result = self.create_outcome(candidate.clone()).await?;
        // A floor published while the create was in flight wins. The late raw object is inert and
        // remains eligible for a later best-effort sweep.
        self.ensure_outcome_not_tombstoned(&candidate).await?;
        Ok(result)
    }

    /// Load the standalone/local terminal outcome for `epoch`.
    ///
    /// `None` means unresolved; it is never evidence of abort.
    ///
    /// # Errors
    /// Object-store I/O or a malformed/conflicting outcome body.
    pub async fn outcome(&self, epoch: u64) -> Result<Option<CheckpointOutcome>, DecisionError> {
        const FLOOR_RETRIES: usize = 3;
        for attempt in 0..FLOOR_RETRIES {
            let floor_before = self.current_outcome_gc_floor().await?;
            if floor_before
                .as_ref()
                .is_some_and(|floor| epoch < floor.before_epoch)
            {
                let floor_after = self.current_outcome_gc_floor().await?;
                if floor_before == floor_after {
                    return Ok(None);
                }
            } else {
                let outcome = self
                    .read_outcome_record(&Self::outcome_path(epoch), epoch)
                    .await?;
                let floor_after = self.current_outcome_gc_floor().await?;
                if floor_before == floor_after {
                    return Ok(outcome.filter(|outcome| {
                        floor_after
                            .as_ref()
                            .is_none_or(|floor| outcome.epoch >= floor.before_epoch)
                    }));
                }
            }
            if attempt + 1 < FLOOR_RETRIES {
                tokio::task::yield_now().await;
            }
        }
        Err(DecisionError::InventoryChanged(
            "outcome GC floor kept advancing during exact lookup".into(),
        ))
    }

    /// Whether the durable outcome for `epoch` is commit.
    ///
    /// `false` can mean either abort or unresolved. Call [`Self::outcome`] when that distinction
    /// matters.
    ///
    /// # Errors
    /// Object-store I/O or a malformed/conflicting outcome body.
    pub async fn outcome_is_committed(&self, epoch: u64) -> Result<bool, DecisionError> {
        Ok(self
            .outcome(epoch)
            .await?
            .is_some_and(|outcome| outcome.is_commit()))
    }

    async fn list_outcome_epochs(&self) -> Result<Vec<u64>, DecisionError> {
        let mut entries = self.store.list(Some(&Self::outcome_root()));
        let mut epochs = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|error| DecisionError::Io(error.to_string()))?;
            let location = entry.location.as_ref();
            let epoch = Self::outcome_epoch_segment(location)
                .and_then(|segment| segment.parse::<u64>().ok())
                .filter(|epoch| *epoch != 0)
                .ok_or_else(|| {
                    DecisionError::Conflict(format!(
                        "malformed checkpoint outcome path: {location}"
                    ))
                })?;
            epochs.push(epoch);
        }
        epochs.sort_unstable();
        if epochs.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(DecisionError::Conflict(
                "checkpoint outcome inventory contains a duplicate epoch".into(),
            ));
        }
        Ok(epochs)
    }

    /// Load every live standalone/local outcome in ascending epoch order.
    ///
    /// Continuity-only GC anchors are audited internally but never returned to callers because
    /// their checkpoint artifacts may already have been deleted.
    ///
    /// # Errors
    /// Object-store I/O, a malformed object name/body, or an inventory that changed during read.
    pub async fn outcomes(&self) -> Result<Vec<CheckpointOutcome>, DecisionError> {
        const FLOOR_RETRIES: usize = 3;
        for attempt in 0..FLOOR_RETRIES {
            let floor_before = self.current_outcome_gc_floor().await?;
            let mut outcomes = self.audited_outcomes().await?;
            let floor_after = self.current_outcome_gc_floor().await?;
            if floor_before == floor_after {
                if let Some(floor) = floor_after {
                    outcomes.retain(|outcome| outcome.epoch >= floor.before_epoch);
                }
                return Ok(outcomes);
            }
            if attempt + 1 < FLOOR_RETRIES {
                tokio::task::yield_now().await;
            }
        }
        Err(DecisionError::InventoryChanged(
            "outcome GC floor kept advancing while selecting live outcomes".into(),
        ))
    }

    /// Anchor-inclusive inventory used for monotonic-history audits and retention metadata.
    async fn audited_outcomes(&self) -> Result<Vec<CheckpointOutcome>, DecisionError> {
        const INVENTORY_RETRIES: usize = 3;
        for attempt in 0..INVENTORY_RETRIES {
            match self.outcomes_once().await {
                Err(DecisionError::InventoryChanged(_)) if attempt + 1 < INVENTORY_RETRIES => {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        Err(DecisionError::InventoryChanged(
            "checkpoint outcome inventory exhausted stability retries".into(),
        ))
    }

    async fn outcomes_once(&self) -> Result<Vec<CheckpointOutcome>, DecisionError> {
        let floor_before = self.current_outcome_gc_floor().await?;
        let min_epoch = floor_before.as_ref().map_or(0, |floor| floor.before_epoch);
        let epochs = self
            .list_outcome_epochs()
            .await?
            .into_iter()
            .filter(|epoch| *epoch >= min_epoch);

        let mut outcomes = Vec::new();
        if let Some(anchor) = floor_before
            .as_ref()
            .and_then(|floor| floor.committed_anchor.as_ref())
        {
            outcomes.push(anchor.clone());
        }
        if let Some(anchor) = floor_before
            .as_ref()
            .and_then(|floor| floor.terminal_anchor.as_ref())
        {
            if outcomes.last() != Some(anchor) {
                outcomes.push(anchor.clone());
            }
        }
        for epoch in epochs {
            let outcome = self
                .read_outcome_record(&Self::outcome_path(epoch), epoch)
                .await?
                .ok_or_else(|| {
                    DecisionError::InventoryChanged(format!(
                        "checkpoint outcome for epoch {epoch} disappeared during inventory"
                    ))
                })?;
            outcomes.push(outcome);
        }
        let floor_after = self.current_outcome_gc_floor().await?;
        if floor_before != floor_after {
            return Err(DecisionError::InventoryChanged(
                "outcome GC floor advanced during outcome inventory".into(),
            ));
        }
        for pair in outcomes.windows(2) {
            let previous = &pair[0];
            let current = &pair[1];
            if current.epoch <= previous.epoch || current.checkpoint_id <= previous.checkpoint_id {
                return Err(DecisionError::Conflict(format!(
                    "checkpoint outcomes regress from epoch {} checkpoint {} to epoch {} checkpoint {}",
                    previous.epoch,
                    previous.checkpoint_id,
                    current.epoch,
                    current.checkpoint_id
                )));
            }
        }
        Ok(outcomes)
    }

    /// Greatest committed terminal outcome, ignoring durable aborts.
    ///
    /// # Errors
    /// Object-store I/O or malformed/conflicting outcome inventory.
    pub async fn highest_committed_outcome(
        &self,
    ) -> Result<Option<CheckpointOutcome>, DecisionError> {
        Ok(self
            .outcomes()
            .await?
            .into_iter()
            .rev()
            .find(CheckpointOutcome::is_commit))
    }

    /// Greatest terminal outcome, including the continuity anchor when it is the newest closed
    /// epoch retained by the store.
    ///
    /// # Errors
    /// Object-store I/O or malformed/conflicting outcome inventory.
    pub async fn highest_terminal_outcome(
        &self,
    ) -> Result<Option<CheckpointOutcome>, DecisionError> {
        Ok(self.audited_outcomes().await?.pop())
    }

    /// Greatest durable outcome GC horizon for the current deployment, or zero before pruning.
    ///
    /// # Errors
    /// Object-store I/O or malformed/conflicting floor metadata.
    pub async fn outcome_gc_floor_horizon(&self) -> Result<u64, DecisionError> {
        Ok(self
            .current_outcome_gc_floor()
            .await?
            .map_or(0, |floor| floor.before_epoch))
    }

    /// Read scalar continuity metadata for compacted terminal outcomes.
    ///
    /// The returned boundary can fence external cursor rollback, but it cannot be used to recover
    /// a checkpoint. Recovery must use a live commit from [`Self::outcomes`] or
    /// [`Self::highest_committed_outcome`].
    ///
    /// # Errors
    /// Object-store I/O or malformed/conflicting floor metadata.
    pub async fn outcome_retention_boundary(
        &self,
    ) -> Result<OutcomeRetentionBoundary, DecisionError> {
        let floor = self.current_outcome_gc_floor().await?;
        Ok(floor.map_or(
            OutcomeRetentionBoundary {
                before_epoch: 0,
                committed_checkpoint_id: None,
                highest_closed_epoch: None,
            },
            |floor| OutcomeRetentionBoundary {
                before_epoch: floor.before_epoch,
                committed_checkpoint_id: floor.committed_anchor.map(|anchor| anchor.checkpoint_id),
                highest_closed_epoch: floor.terminal_anchor.map(|anchor| anchor.epoch),
            },
        ))
    }

    #[cfg(feature = "cluster")]
    async fn quarantine_recovery_capsule_object(
        &self,
        location: &OsPath,
    ) -> Result<(), DecisionError> {
        let quarantine = Self::recovery_capsule_quarantine_path(location.as_ref());
        match self.store.rename(location, &quarantine).await {
            Ok(()) => Ok(()),
            Err(error) => match self.store.head(location).await {
                Err(object_store::Error::NotFound { .. }) => Ok(()),
                Ok(_) => Err(DecisionError::Io(format!(
                    "failed to quarantine recovery capsule '{location}': {error}"
                ))),
                Err(head_error) => Err(DecisionError::Io(format!(
                    "failed to quarantine recovery capsule '{location}' ({error}); reconciliation failed ({head_error})"
                ))),
            },
        }
    }

    /// Perform one bounded-memory cleanup step for capsule epochs below a durable cluster floor.
    ///
    /// Each step full-scans the unordered listing and processes the lexically oldest bounded batch
    /// after the cursor. The cursor wraps so delayed creates and failed paths are retried.
    #[cfg(feature = "cluster")]
    pub(crate) async fn sweep_recovery_capsules_step(
        &self,
        before_epoch: u64,
        known_live_digests: &std::collections::BTreeSet<String>,
    ) -> Result<RecoveryCapsuleGcStep, DecisionError> {
        if before_epoch <= 1 {
            return Ok(RecoveryCapsuleGcStep {
                examined: 0,
                deleted: 0,
                quarantined: 0,
                pending: false,
            });
        }

        let deployment_id = self.load_or_create_deployment_id().await?;
        let observed = self.read_recovery_capsule_gc_cursor(&deployment_id).await?;
        let (cursor, update_version) = observed.map_or_else(
            || {
                (
                    RecoveryCapsuleGcCursor {
                        version: RECOVERY_CAPSULE_GC_CURSOR_VERSION,
                        deployment_id: deployment_id.clone(),
                        offset: None,
                    },
                    None,
                )
            },
            |observed| (observed.cursor, Some(observed.update_version)),
        );

        let root = Self::recovery_capsule_root();
        let mut listed = self.store.list(Some(&root));
        let mut oldest = BinaryHeap::with_capacity(RECOVERY_CAPSULE_GC_BATCH_SIZE);
        let mut eligible = 0usize;
        while let Some(entry) = listed.next().await {
            let entry = entry.map_err(|error| DecisionError::Io(error.to_string()))?;
            if cursor
                .offset
                .as_deref()
                .is_some_and(|offset| entry.location.as_ref() <= offset)
            {
                continue;
            }
            eligible = eligible.saturating_add(1);
            retain_lexically_oldest_recovery_capsule(&mut oldest, entry);
        }
        let list_exhausted = eligible <= RECOVERY_CAPSULE_GC_BATCH_SIZE;
        let entries = oldest
            .into_sorted_vec()
            .into_iter()
            .map(|candidate| candidate.meta)
            .collect::<Vec<_>>();
        let examined = entries.len();

        enum ObjectWork {
            Retained,
            Deleted,
            Quarantined,
            Failed,
        }

        let work = futures::stream::iter(entries.iter().cloned())
            .map(|entry| async move {
                let Some((epoch, checkpoint_id, digest)) =
                    Self::recovery_capsule_coordinates_from_path(entry.location.as_ref())
                else {
                    return match self
                        .quarantine_recovery_capsule_object(&entry.location)
                        .await
                    {
                        Ok(()) => ObjectWork::Quarantined,
                        Err(error) => {
                            tracing::warn!(path = %entry.location, %error, "recovery capsule quarantine failed; retrying on the next scan pass");
                            ObjectWork::Failed
                        }
                    };
                };
                if epoch >= before_epoch {
                    return ObjectWork::Retained;
                }
                let reference = RecoveryCapsuleRef {
                    epoch,
                    checkpoint_id,
                    sha256: digest.to_owned(),
                    len: entry.size,
                };
                if reference.validate().is_err() {
                    return match self
                        .quarantine_recovery_capsule_object(&entry.location)
                        .await
                    {
                        Ok(()) => ObjectWork::Quarantined,
                        Err(error) => {
                            tracing::warn!(path = %entry.location, %error, "recovery capsule quarantine failed; retrying on the next scan pass");
                            ObjectWork::Failed
                        }
                    };
                }
                if known_live_digests.contains(&reference.sha256) {
                    return ObjectWork::Retained;
                }
                match self.load_recovery_capsule(&reference).await {
                    Ok(_) => match self.store.delete(&entry.location).await {
                        Ok(()) | Err(object_store::Error::NotFound { .. }) => ObjectWork::Deleted,
                        Err(error) => {
                            tracing::warn!(epoch, checkpoint_id, %error, "recovery capsule delete failed; retrying on the next scan pass");
                            ObjectWork::Failed
                        }
                    },
                    Err(DecisionError::Conflict(error)) => {
                        if matches!(
                            self.store.head(&entry.location).await,
                            Err(object_store::Error::NotFound { .. })
                        ) {
                            return ObjectWork::Deleted;
                        }
                        match self
                            .quarantine_recovery_capsule_object(&entry.location)
                            .await
                        {
                            Ok(()) => {
                                tracing::warn!(path = %entry.location, %error, "corrupt recovery capsule quarantined");
                                ObjectWork::Quarantined
                            }
                            Err(quarantine_error) => {
                                tracing::warn!(path = %entry.location, %error, %quarantine_error, "corrupt recovery capsule quarantine failed; retrying on the next scan pass");
                                ObjectWork::Failed
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(path = %entry.location, %error, "recovery capsule read failed; retrying on the next scan pass");
                        ObjectWork::Failed
                    }
                }
            })
            .buffer_unordered(4)
            .collect::<Vec<_>>()
            .await;

        let deleted = work
            .iter()
            .filter(|result| matches!(result, ObjectWork::Deleted))
            .count();
        let quarantined = work
            .iter()
            .filter(|result| matches!(result, ObjectWork::Quarantined))
            .count();

        let mut updated = cursor.clone();
        updated.offset = if list_exhausted {
            None
        } else {
            entries.iter().map(|entry| entry.location.to_string()).max()
        };
        let cursor_was_absent = update_version.is_none();
        if updated != cursor
            && !self
                .compare_and_swap_recovery_capsule_gc_cursor(&updated, update_version)
                .await?
        {
            return Ok(RecoveryCapsuleGcStep {
                examined,
                deleted,
                quarantined,
                pending: true,
            });
        }
        if cursor_was_absent && updated == cursor {
            let _ = self
                .compare_and_swap_recovery_capsule_gc_cursor(&updated, None)
                .await?;
        }

        Ok(RecoveryCapsuleGcStep {
            examined,
            deleted,
            quarantined,
            // Maintenance intentionally keeps cycling while a floor exists so delayed creates
            // and previously failed paths cannot be stranded after a quiet pass.
            pending: true,
        })
    }

    /// Advance the outcome GC floor for `epoch < before`, then best-effort delete raw
    /// tombstoned outcomes.
    ///
    /// At least one live commit must remain at or above the effective horizon. The embedded anchors
    /// preserve terminal and committed-cursor continuity but are never eligible as recovery cuts.
    /// Concurrent higher floors supersede this request and are included in the same best-effort
    /// sweep.
    ///
    /// # Errors
    /// Object-store I/O, malformed/conflicting inventory, or a horizon that would remove the last
    /// recoverable commit outcome.
    pub async fn prune_outcomes_before(&self, before: u64) -> Result<u64, DecisionError> {
        if before == 0 {
            return self.outcome_gc_floor_horizon().await;
        }
        let deployment_id = self.load_or_create_deployment_id().await?;

        // A failed CAS means another worker advanced the floor. Rebuild from that winner instead
        // of publishing anchors derived from an older view: reusing a stale candidate could
        // discard a terminal outcome admitted between the two floor generations.
        loop {
            let observed = self.read_outcome_gc_floor(&deployment_id).await?;
            if observed
                .as_ref()
                .is_some_and(|versioned| versioned.floor.before_epoch >= before)
            {
                // The floor may have become durable immediately before its publisher crashed.
                // Re-enter the idempotent sweep so retries repair any raw outcomes or capsules
                // left behind by that incomplete retention pass.
                break;
            }

            let outcomes = match self.audited_outcomes().await {
                Err(DecisionError::InventoryChanged(_)) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                result => result?,
            };
            if !outcomes
                .iter()
                .any(|outcome| outcome.epoch >= before && outcome.is_commit())
            {
                return Err(DecisionError::Conflict(format!(
                    "cannot advance outcome GC floor to {before}: no live commit recovery cut would remain"
                )));
            }
            let floor = OutcomeGcFloor {
                version: OUTCOME_GC_FLOOR_VERSION,
                deployment_id: deployment_id.clone(),
                before_epoch: before,
                terminal_anchor: outcomes
                    .iter()
                    .rev()
                    .find(|outcome| outcome.epoch < before)
                    .cloned(),
                committed_anchor: outcomes
                    .iter()
                    .rev()
                    .find(|outcome| outcome.epoch < before && outcome.is_commit())
                    .cloned(),
            };
            let expected = observed.map(|versioned| versioned.update_version);
            if self
                .compare_and_swap_outcome_gc_floor(&floor, expected)
                .await?
            {
                break;
            }
            tokio::task::yield_now().await;
        }

        let mut swept_horizon = 0;
        loop {
            let effective = self.current_outcome_gc_floor().await?.ok_or_else(|| {
                DecisionError::InventoryChanged(
                    "outcome GC floor disappeared immediately after publication".into(),
                )
            })?;
            if effective.before_epoch < before || effective.before_epoch < swept_horizon {
                return Err(DecisionError::InventoryChanged(format!(
                    "outcome GC floor regressed to {} after publishing {before}",
                    effective.before_epoch
                )));
            }
            if effective.before_epoch > swept_horizon {
                let raw_epochs = self.list_outcome_epochs().await?;
                for epoch in raw_epochs
                    .into_iter()
                    .filter(|epoch| *epoch < effective.before_epoch)
                {
                    match self.store.delete(&Self::outcome_path(epoch)).await {
                        Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                        Err(error) => tracing::warn!(
                            epoch,
                            %error,
                            "outcome prune: tombstoned outcome delete failed"
                        ),
                    }
                }
                swept_horizon = effective.before_epoch;
            }
            let current = self.current_outcome_gc_floor().await?.ok_or_else(|| {
                DecisionError::InventoryChanged(
                    "outcome GC floor disappeared during best-effort sweep".into(),
                )
            })?;
            if current.before_epoch == swept_horizon {
                return Ok(swept_horizon);
            }
            tokio::task::yield_now().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    fn store_in(dir: &std::path::Path) -> CheckpointDecisionStore {
        let fs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
        CheckpointDecisionStore::new(fs)
    }

    struct DeferredPutStore {
        inner: Arc<dyn ObjectStore>,
        target: std::sync::Mutex<Option<OsPath>>,
        intercepted: std::sync::atomic::AtomicBool,
        pending: std::sync::Mutex<Option<(OsPath, PutPayload, PutOptions)>>,
        reverse_lists: bool,
    }

    impl DeferredPutStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                target: std::sync::Mutex::new(None),
                intercepted: std::sync::atomic::AtomicBool::new(false),
                pending: std::sync::Mutex::new(None),
                reverse_lists: false,
            }
        }

        #[cfg(feature = "cluster")]
        fn with_reversed_lists(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                target: std::sync::Mutex::new(None),
                intercepted: std::sync::atomic::AtomicBool::new(false),
                pending: std::sync::Mutex::new(None),
                reverse_lists: true,
            }
        }

        fn intercept(&self, target: OsPath) {
            *self.target.lock().unwrap() = Some(target);
            self.intercepted
                .store(false, std::sync::atomic::Ordering::Release);
            *self.pending.lock().unwrap() = None;
        }

        async fn apply_pending(&self) -> object_store::Result<object_store::PutResult> {
            let (location, payload, options) =
                self.pending.lock().unwrap().take().expect("deferred put");
            self.inner.put_opts(&location, payload, options).await
        }
    }

    impl std::fmt::Debug for DeferredPutStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("DeferredPutStore")
                .finish_non_exhaustive()
        }
    }

    impl std::fmt::Display for DeferredPutStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("DeferredPutStore")
        }
    }

    #[async_trait]
    impl ObjectStore for DeferredPutStore {
        async fn put_opts(
            &self,
            location: &OsPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let target = self.target.lock().unwrap().clone();
            if target.as_ref() == Some(location)
                && !self
                    .intercepted
                    .swap(true, std::sync::atomic::Ordering::AcqRel)
            {
                *self.pending.lock().unwrap() =
                    Some((location.clone(), payload.clone(), options.clone()));
                return Err(object_store::Error::Generic {
                    store: "DeferredPutStore",
                    source: Box::new(std::io::Error::other(
                        "injected response loss before remote visibility",
                    )),
                });
            }
            self.inner.put_opts(location, payload, options).await
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
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&OsPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            let listed = self.inner.list(prefix);
            if !self.reverse_lists {
                return listed;
            }
            futures::stream::once(async move {
                let mut entries = listed.collect::<Vec<_>>().await;
                entries.sort_by(|left, right| match (left, right) {
                    (Ok(left), Ok(right)) => right.location.as_ref().cmp(left.location.as_ref()),
                    (Err(_), Ok(_)) => std::cmp::Ordering::Less,
                    (Ok(_), Err(_)) => std::cmp::Ordering::Greater,
                    (Err(_), Err(_)) => std::cmp::Ordering::Equal,
                });
                futures::stream::iter(entries)
            })
            .flatten()
            .boxed()
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

    fn assignment_fence(
        assignment_version: u64,
        participant_ids: &[u64],
    ) -> CheckpointAssignmentFence {
        let participants = participant_ids
            .iter()
            .map(|node_id| crate::checkpoint::CheckpointParticipant {
                node_id: *node_id,
                boot_incarnation: uuid::Uuid::from_u128(u128::from(*node_id) + 1_000),
            })
            .collect();
        CheckpointAssignmentFence::from_owner_map(assignment_version, participant_ids, participants)
            .unwrap()
    }

    fn leader_proof(
        fence: &CheckpointAssignmentFence,
        node_id: u64,
        process_term: u64,
        fencing_token: u64,
    ) -> LeaderProof {
        LeaderProof {
            owner: crate::checkpoint::LeaderProofOwner {
                node_id,
                boot_id: fence.participant_incarnation(node_id).unwrap(),
                process_term,
            },
            fencing_token,
        }
    }

    fn digest(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }

    async fn test_capsule(
        store: &CheckpointDecisionStore,
        epoch: u64,
        checkpoint_id: u64,
        fence: &CheckpointAssignmentFence,
    ) -> ClusterRecoveryCapsule {
        let deployment_id = store.load_or_create_deployment_id().await.unwrap();
        let portable_state_sha256 = digest(9);
        let participants = fence
            .participant_ids()
            .into_iter()
            .map(|participant_id| crate::checkpoint::ParticipantRecoveryRef {
                participant_id,
                readiness_sha256: digest(3),
                manifest_sha256: digest(4),
                portable_state_sha256: portable_state_sha256.clone(),
            })
            .collect();
        ClusterRecoveryCapsule {
            version: crate::checkpoint::CLUSTER_RECOVERY_CAPSULE_VERSION,
            attempt: crate::state::CheckpointAttempt::new(epoch, checkpoint_id),
            deployment_id,
            pipeline_identity: crate::checkpoint::PipelineIdentity::empty(),
            assignment_fence: fence.clone(),
            seal_inventory_sha256: digest(2),
            participants,
            source_offsets: std::collections::BTreeMap::new(),
            source_metadata: std::collections::BTreeMap::new(),
            source_watermarks: std::collections::BTreeMap::new(),
            cluster_watermark: crate::checkpoint::CheckpointWatermark::Uninitialized,
            recovery_watermark_frontier: None,
            portable_state_sha256,
        }
    }

    async fn create_capsule_ref(
        store: &CheckpointDecisionStore,
        epoch: u64,
        checkpoint_id: u64,
        fence: &CheckpointAssignmentFence,
    ) -> RecoveryCapsuleRef {
        let capsule = test_capsule(store, epoch, checkpoint_id, fence).await;
        store.create_recovery_capsule(&capsule).await.unwrap()
    }

    #[test]
    fn inventory_paths_require_canonical_protocol_names() {
        assert_eq!(
            CheckpointDecisionStore::outcome_epoch_segment("checkpoint-outcomes/epoch=5/outcome"),
            Some("5")
        );
        assert_eq!(
            CheckpointDecisionStore::outcome_epoch_segment("checkpoint-outcomes/epoch=5/other"),
            None
        );
    }

    #[tokio::test]
    async fn recovery_capsule_create_is_idempotent_and_load_verifies_reference() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let fence = assignment_fence(12, &[2, 7]);
        let capsule = test_capsule(&store, 4, 40, &fence).await;

        let reference = store.create_recovery_capsule(&capsule).await.unwrap();
        assert_eq!(
            store.create_recovery_capsule(&capsule).await.unwrap(),
            reference
        );
        assert_eq!(
            store.load_recovery_capsule(&reference).await.unwrap(),
            capsule
        );

        let mut wrong_length = reference.clone();
        wrong_length.len += 1;
        assert!(store.load_recovery_capsule(&wrong_length).await.is_err());

        let mut wrong_digest = reference;
        wrong_digest.sha256 = digest(0xaa);
        assert!(store.load_recovery_capsule(&wrong_digest).await.is_err());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cyclic_capsule_cleanup_finds_a_create_visible_after_client_failure() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
        let object_store: Arc<dyn ObjectStore> = fault.clone();
        let store = CheckpointDecisionStore::new(object_store);
        let fence = assignment_fence(12, &[2, 7]);
        let capsule = test_capsule(&store, 1, 40, &fence).await;
        let (_, reference) = capsule.encode_and_reference().unwrap();
        let path = CheckpointDecisionStore::recovery_capsule_path(&reference);
        fault.intercept(path.clone());

        let error = store
            .create_recovery_capsule(&capsule)
            .await
            .expect_err("the client must observe the injected ambiguous failure");
        assert!(matches!(error, DecisionError::Io(_)));
        assert!(matches!(
            inner.head(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));

        let live = std::collections::BTreeSet::new();
        assert!(
            store
                .sweep_recovery_capsules_step(2, &live)
                .await
                .unwrap()
                .pending
        );
        fault.apply_pending().await.unwrap();
        inner
            .head(&path)
            .await
            .expect("deferred server-side create became visible");

        let step = store.sweep_recovery_capsules_step(2, &live).await.unwrap();
        assert_eq!(step.deleted, 1);
        assert!(step.pending);
        assert!(matches!(
            inner.head(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn capsule_cleanup_progress_is_independent_of_list_order() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let reversed: Arc<dyn ObjectStore> =
            Arc::new(DeferredPutStore::with_reversed_lists(Arc::clone(&inner)));
        let store = CheckpointDecisionStore::new(reversed);
        let fence = assignment_fence(12, &[2, 7]);
        let oldest = create_capsule_ref(&store, 1, 10, &fence).await;
        let oldest_path = CheckpointDecisionStore::recovery_capsule_path(&oldest);
        let mut newest_path = None;
        for epoch in 2..=70u64 {
            let reference = RecoveryCapsuleRef {
                epoch,
                checkpoint_id: epoch * 10,
                sha256: digest(u8::try_from(epoch).unwrap()),
                len: 1,
            };
            let path = CheckpointDecisionStore::recovery_capsule_path(&reference);
            inner
                .put(&path, PutPayload::from(Bytes::from_static(b"x")))
                .await
                .unwrap();
            newest_path = Some(path);
        }

        let step = store
            .sweep_recovery_capsules_step(2, &std::collections::BTreeSet::new())
            .await
            .unwrap();
        assert_eq!(step.examined, RECOVERY_CAPSULE_GC_BATCH_SIZE);
        assert_eq!(step.deleted, 1);
        assert!(matches!(
            inner.head(&oldest_path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        inner
            .head(&newest_path.unwrap())
            .await
            .expect("newer retained capsule must survive unordered maintenance");
    }

    #[tokio::test]
    async fn recovery_capsule_load_rejects_tampered_body() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let fence = assignment_fence(12, &[2, 7]);
        let capsule = test_capsule(&store, 4, 40, &fence).await;
        let reference = store.create_recovery_capsule(&capsule).await.unwrap();
        let path = CheckpointDecisionStore::recovery_capsule_path(&reference);
        let mut encoded = crate::checkpoint::canonical_json_bytes(&capsule).unwrap();
        let position = encoded
            .iter()
            .position(|byte| *byte == b'4')
            .expect("test capsule contains a digit");
        encoded[position] = b'5';
        object_store
            .put(&path, PutPayload::from(Bytes::from(encoded)))
            .await
            .unwrap();

        let error = store.load_recovery_capsule(&reference).await.unwrap_err();
        assert!(matches!(error, DecisionError::Conflict(_)));
    }

    #[tokio::test]
    async fn outcome_shape_requires_capsule_only_for_cluster_commit() {
        let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        let fence = assignment_fence(12, &[2, 7]);
        let proof = leader_proof(&fence, 2, 3, 4);
        let reference = create_capsule_ref(&store, 4, 40, &fence).await;

        let missing = store
            .canonical_outcome(
                4,
                40,
                CheckpointScope::Cluster,
                Some(fence.clone()),
                Some(proof.clone()),
                CheckpointVerdict::Commit,
                None,
            )
            .await
            .unwrap_err();
        assert!(missing.to_string().contains("requires a recovery capsule"));

        let abort_with_capsule = store
            .canonical_outcome(
                4,
                40,
                CheckpointScope::Cluster,
                Some(fence.clone()),
                Some(proof),
                CheckpointVerdict::Abort,
                Some(reference.clone()),
            )
            .await
            .unwrap_err();
        assert!(abort_with_capsule
            .to_string()
            .contains("abort outcome for epoch 4 cannot carry"));

        let local_with_capsule = store
            .canonical_outcome(
                4,
                40,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                Some(reference),
            )
            .await
            .unwrap_err();
        assert!(local_with_capsule
            .to_string()
            .contains("local outcome for epoch 4 cannot carry"));
    }

    #[tokio::test]
    async fn outcome_rejects_capsule_for_a_different_attempt() {
        let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        let fence = assignment_fence(12, &[2, 7]);
        let proof = leader_proof(&fence, 2, 3, 4);
        let reference = create_capsule_ref(&store, 4, 40, &fence).await;

        let error = store
            .canonical_outcome(
                5,
                50,
                CheckpointScope::Cluster,
                Some(fence),
                Some(proof),
                CheckpointVerdict::Commit,
                Some(reference),
            )
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("does not match recovery capsule"));
    }

    #[tokio::test]
    async fn standalone_outcome_objects_reject_cluster_authority() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let fence = assignment_fence(12, &[2, 7]);
        let proof = leader_proof(&fence, 2, 3, 4);
        let error = store
            .record_outcome(
                4,
                40,
                CheckpointScope::Cluster,
                Some(fence.clone()),
                Some(proof.clone()),
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("shared leader authority"));

        let forged = store
            .canonical_outcome(
                4,
                40,
                CheckpointScope::Cluster,
                Some(fence),
                Some(proof),
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
        object_store
            .put(
                &CheckpointDecisionStore::outcome_path(4),
                PutPayload::from(Bytes::from(serde_json::to_vec(&forged).unwrap())),
            )
            .await
            .unwrap();
        let error = store.outcome(4).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("outside the shared leader authority"));
    }

    #[tokio::test]
    async fn terminal_outcome_retry_is_idempotent_and_conflict_returns_winner() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let commit = CheckpointVerdict::Commit;

        let created = store
            .record_outcome(
                7,
                70,
                CheckpointScope::Local,
                None,
                None,
                commit.clone(),
                None,
            )
            .await
            .unwrap();
        let RecordOutcomeResult::Created(winner) = created else {
            panic!("first create must win");
        };

        assert_eq!(
            store
                .record_outcome(7, 70, CheckpointScope::Local, None, None, commit, None)
                .await
                .unwrap(),
            RecordOutcomeResult::Unchanged(winner.clone())
        );
        assert_eq!(
            store
                .record_outcome(
                    7,
                    71,
                    CheckpointScope::Local,
                    None,
                    None,
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap(),
            RecordOutcomeResult::Conflict {
                winner: winner.clone()
            }
        );
        assert_eq!(store.outcome(7).await.unwrap(), Some(winner));
    }

    #[tokio::test]
    async fn recovery_abort_wins_against_a_delayed_commit_create() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
        let object_store: Arc<dyn ObjectStore> = fault.clone();
        let store = CheckpointDecisionStore::new(object_store);
        fault.intercept(CheckpointDecisionStore::outcome_path(2));

        let error = store
            .record_outcome(
                2,
                20,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                None,
            )
            .await
            .expect_err("the delayed create is not yet durably visible");
        assert!(matches!(error, DecisionError::Io(_)));

        let RecordOutcomeResult::Created(abort) = store
            .record_outcome(
                2,
                20,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap()
        else {
            panic!("recovery Abort must win the create-once race");
        };
        assert!(matches!(
            fault.apply_pending().await,
            Err(object_store::Error::Precondition { .. })
                | Err(object_store::Error::AlreadyExists { .. })
        ));
        assert_eq!(store.outcome(2).await.unwrap(), Some(abort));
    }

    #[tokio::test]
    async fn delayed_commit_wins_before_recovery_abort_create() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
        let object_store: Arc<dyn ObjectStore> = fault.clone();
        let store = CheckpointDecisionStore::new(object_store);
        fault.intercept(CheckpointDecisionStore::outcome_path(2));

        let error = store
            .record_outcome(
                2,
                20,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                None,
            )
            .await
            .expect_err("the delayed create is not yet durably visible");
        assert!(matches!(error, DecisionError::Io(_)));
        fault.apply_pending().await.unwrap();

        let RecordOutcomeResult::Conflict { winner } = store
            .record_outcome(
                2,
                20,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap()
        else {
            panic!("the visible Commit must remain the terminal winner");
        };
        assert!(winner.is_commit());
        assert_eq!(store.outcome(2).await.unwrap(), Some(winner));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_commit_and_abort_converge_on_one_terminal_winner() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let commit_store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let commit = tokio::spawn(async move {
            commit_store
                .record_outcome(
                    8,
                    80,
                    CheckpointScope::Local,
                    None,
                    None,
                    CheckpointVerdict::Commit,
                    None,
                )
                .await
                .unwrap()
        });

        let abort_store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let abort = tokio::spawn(async move {
            abort_store
                .record_outcome(
                    8,
                    80,
                    CheckpointScope::Local,
                    None,
                    None,
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap()
        });

        let results = [commit.await.unwrap(), abort.await.unwrap()];
        let restarted = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let winner = restarted.outcome(8).await.unwrap().unwrap();
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, RecordOutcomeResult::Created(_)))
                .count(),
            1
        );
        for result in results {
            match result {
                RecordOutcomeResult::Created(observed)
                | RecordOutcomeResult::Unchanged(observed)
                | RecordOutcomeResult::Conflict { winner: observed } => {
                    assert_eq!(observed, winner);
                }
            }
        }
    }

    #[tokio::test]
    async fn terminal_outcomes_survive_restart_and_absence_is_not_abort() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        store
            .record_outcome(
                4,
                40,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                None,
            )
            .await
            .unwrap();
        store
            .record_outcome(
                5,
                50,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
        drop(store);

        let restarted = CheckpointDecisionStore::new(object_store);
        assert!(matches!(
            restarted.outcome(5).await.unwrap().unwrap().verdict,
            CheckpointVerdict::Abort
        ));
        assert_eq!(restarted.outcome(6).await.unwrap(), None);
        assert!(!restarted.outcome_is_committed(5).await.unwrap());
        assert!(!restarted.outcome_is_committed(6).await.unwrap());
        assert_eq!(
            restarted
                .highest_committed_outcome()
                .await
                .unwrap()
                .unwrap()
                .epoch,
            4
        );
    }

    #[tokio::test]
    async fn abort_after_commit_advances_terminal_without_replacing_live_commit() {
        let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        store
            .record_outcome(
                4,
                40,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                None,
            )
            .await
            .unwrap();
        store
            .record_outcome(
                5,
                50,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            store
                .highest_terminal_outcome()
                .await
                .unwrap()
                .unwrap()
                .epoch,
            5
        );
        assert_eq!(
            store
                .highest_committed_outcome()
                .await
                .unwrap()
                .unwrap()
                .epoch,
            4
        );
    }

    #[tokio::test]
    async fn outcome_floor_rejects_late_create_and_survives_restart() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        for (epoch, checkpoint_id, verdict) in [
            (1, 10, CheckpointVerdict::Commit),
            (2, 20, CheckpointVerdict::Abort),
            (5, 50, CheckpointVerdict::Commit),
        ] {
            store
                .record_outcome(
                    epoch,
                    checkpoint_id,
                    CheckpointScope::Local,
                    None,
                    None,
                    verdict,
                    None,
                )
                .await
                .unwrap();
        }
        assert_eq!(store.prune_outcomes_before(4).await.unwrap(), 4);

        let error = store
            .record_outcome(
                3,
                30,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, DecisionError::Conflict(_)));
        assert!(error
            .to_string()
            .contains("below durable outcome GC horizon 4"));
        assert_eq!(store.outcome(3).await.unwrap(), None);
        drop(store);

        let restarted = CheckpointDecisionStore::new(object_store);
        assert_eq!(restarted.outcome_gc_floor_horizon().await.unwrap(), 4);
        let live = restarted.outcomes().await.unwrap();
        assert_eq!(
            live.iter()
                .map(|outcome| (outcome.epoch, outcome.checkpoint_id))
                .collect::<Vec<_>>(),
            vec![(5, 50)]
        );
        let continuity = restarted.audited_outcomes().await.unwrap();
        assert_eq!(
            continuity
                .iter()
                .map(|outcome| (outcome.epoch, outcome.checkpoint_id))
                .collect::<Vec<_>>(),
            vec![(1, 10), (2, 20), (5, 50)]
        );
        assert!(matches!(&continuity[1].verdict, CheckpointVerdict::Abort));
        assert_eq!(
            restarted
                .highest_committed_outcome()
                .await
                .unwrap()
                .unwrap()
                .epoch,
            5
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_outcome_floor_advancement_is_monotonic() {
        const LAST_EPOCH: u64 = 32;

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = CheckpointDecisionStore::new(Arc::clone(&object_store));
        for epoch in 1..=LAST_EPOCH {
            writer
                .record_outcome(
                    epoch,
                    epoch * 10,
                    CheckpointScope::Local,
                    None,
                    None,
                    CheckpointVerdict::Commit,
                    None,
                )
                .await
                .unwrap();
        }

        let mut tasks = tokio::task::JoinSet::new();
        for before in 2..=LAST_EPOCH {
            let object_store = Arc::clone(&object_store);
            tasks.spawn(async move {
                let store = CheckpointDecisionStore::new(object_store);
                (before, store.prune_outcomes_before(before).await)
            });
        }
        while let Some(result) = tasks.join_next().await {
            let (requested, horizon) = result.unwrap();
            assert!(
                horizon.unwrap() >= requested,
                "a concurrent floor winner may supersede but never regress a request"
            );
        }

        let restarted = CheckpointDecisionStore::new(object_store);
        assert_eq!(
            restarted.outcome_gc_floor_horizon().await.unwrap(),
            LAST_EPOCH
        );
        assert_eq!(
            restarted.outcome_retention_boundary().await.unwrap(),
            OutcomeRetentionBoundary {
                before_epoch: LAST_EPOCH,
                committed_checkpoint_id: Some((LAST_EPOCH - 1) * 10),
                highest_closed_epoch: Some(LAST_EPOCH - 1),
            }
        );
        assert_eq!(
            restarted
                .outcomes()
                .await
                .unwrap()
                .into_iter()
                .map(|outcome| outcome.epoch)
                .collect::<Vec<_>>(),
            vec![LAST_EPOCH]
        );
    }

    #[tokio::test]
    async fn outcome_floor_object_count_is_bounded_across_many_horizons() {
        const LAST_EPOCH: u64 = 64;

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let deployment_id = store.load_or_create_deployment_id().await.unwrap();
        for epoch in 1..=LAST_EPOCH {
            store
                .record_outcome(
                    epoch,
                    epoch * 10,
                    CheckpointScope::Local,
                    None,
                    None,
                    CheckpointVerdict::Commit,
                    None,
                )
                .await
                .unwrap();
        }
        for before in 2..=LAST_EPOCH {
            assert_eq!(store.prune_outcomes_before(before).await.unwrap(), before);
        }

        let mut entries = object_store.list(Some(&OsPath::from("checkpoint-outcome-gc/")));
        let mut locations = Vec::new();
        while let Some(entry) = entries.next().await {
            locations.push(entry.unwrap().location);
        }
        assert_eq!(
            locations,
            vec![CheckpointDecisionStore::outcome_gc_floor_path(
                &deployment_id
            )],
            "retention must overwrite one canonical floor instead of leaking horizon history"
        );
    }

    #[tokio::test]
    async fn outcome_retention_boundary_preserves_commit_cursor_and_closed_epoch() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        for (epoch, checkpoint_id, verdict) in [
            (1, 10, CheckpointVerdict::Commit),
            (2, 20, CheckpointVerdict::Abort),
            (3, 30, CheckpointVerdict::Commit),
        ] {
            store
                .record_outcome(
                    epoch,
                    checkpoint_id,
                    CheckpointScope::Local,
                    None,
                    None,
                    verdict,
                    None,
                )
                .await
                .unwrap();
        }
        assert_eq!(store.prune_outcomes_before(3).await.unwrap(), 3);
        drop(store);

        let restarted = CheckpointDecisionStore::new(object_store);
        assert_eq!(
            restarted.outcome_retention_boundary().await.unwrap(),
            OutcomeRetentionBoundary {
                before_epoch: 3,
                committed_checkpoint_id: Some(10),
                highest_closed_epoch: Some(2),
            }
        );
        assert_eq!(
            restarted
                .outcomes()
                .await
                .unwrap()
                .into_iter()
                .map(|outcome| (outcome.epoch, outcome.checkpoint_id))
                .collect::<Vec<_>>(),
            vec![(3, 30)]
        );
    }

    #[tokio::test]
    async fn outcome_inventory_rejects_checkpoint_order_regression() {
        let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        store
            .record_outcome(
                8,
                80,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
        store
            .record_outcome(
                10,
                100,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                None,
            )
            .await
            .unwrap();
        store.prune_outcomes_before(9).await.unwrap();
        store
            .record_outcome(
                9,
                79,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
        let error = store
            .outcomes()
            .await
            .expect_err("the audited inventory must reject checkpoint-order regression");
        assert!(matches!(error, DecisionError::Conflict(_)));
        assert!(error.to_string().contains("epoch 8 checkpoint 80"));
        assert!(error.to_string().contains("epoch 9 checkpoint 79"));
    }

    #[tokio::test]
    async fn outcome_prune_cannot_remove_last_live_commit() {
        let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        store
            .record_outcome(
                4,
                40,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                None,
            )
            .await
            .unwrap();
        store
            .record_outcome(
                5,
                50,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();

        let error = store.prune_outcomes_before(5).await.unwrap_err();
        assert!(matches!(error, DecisionError::Conflict(_)));
        assert!(error.to_string().contains("no live commit recovery cut"));
        assert_eq!(store.outcome_gc_floor_horizon().await.unwrap(), 0);
        assert_eq!(
            store
                .highest_committed_outcome()
                .await
                .unwrap()
                .unwrap()
                .epoch,
            4
        );
    }

    #[tokio::test]
    async fn deployment_identity_is_create_once_across_store_instances() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let second = CheckpointDecisionStore::new(object_store);

        let first_id = first.load_or_create_deployment_id().await.unwrap();
        let second_id = second.load_or_create_deployment_id().await.unwrap();

        assert_eq!(first_id, second_id);
        assert!(!uuid::Uuid::parse_str(&first_id).unwrap().is_nil());
    }

    #[tokio::test]
    async fn independent_decision_stores_get_distinct_deployment_identities() {
        let first = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        let second = CheckpointDecisionStore::new(Arc::new(InMemory::new()));

        assert_ne!(
            first.load_or_create_deployment_id().await.unwrap(),
            second.load_or_create_deployment_id().await.unwrap()
        );
    }

    #[tokio::test]
    async fn checkpoint_ids_start_at_one_and_increase() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        assert_eq!(s.allocate_checkpoint_id().await.unwrap(), 1);
        assert_eq!(s.allocate_checkpoint_id().await.unwrap(), 2);
        drop(s);

        let restarted = store_in(dir.path());
        assert_eq!(restarted.allocate_checkpoint_id().await.unwrap(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_checkpoint_id_allocations_are_unique_and_monotonic() {
        const ALLOCATIONS: u64 = 64;

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..ALLOCATIONS {
            let object_store = Arc::clone(&object_store);
            tasks.spawn(async move {
                CheckpointDecisionStore::new(object_store)
                    .allocate_checkpoint_id()
                    .await
                    .unwrap()
            });
        }

        let mut allocated = Vec::with_capacity(usize::try_from(ALLOCATIONS).unwrap());
        while let Some(result) = tasks.join_next().await {
            allocated.push(result.unwrap());
        }
        allocated.sort_unstable();

        assert_eq!(allocated, (1..=ALLOCATIONS).collect::<Vec<_>>());
        assert!(allocated.iter().all(|id| *id != 0));
    }
}
