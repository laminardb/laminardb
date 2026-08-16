//! Durable checkpoint identity, ID allocation, and immutable terminal outcomes.

use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, OnceLock, Weak};

use crate::checkpoint::CheckpointAttempt;
pub use crate::checkpoint::CheckpointScope;
use crate::checkpoint::{
    CheckpointAssignmentFence, CommittedCheckpointIndex, CommittedCheckpointRef, LeaderProof,
    PipelineIdentity, MAX_COMMITTED_CHECKPOINT_INDEX_BYTES,
};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use object_store::path::Path as OsPath;
use object_store::{
    GetOptions, GetRange, GetResult, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    UpdateVersion,
};

/// Durable checkpoint metadata store.
pub struct CheckpointDecisionStore {
    store: Arc<dyn ObjectStore>,
    update_mode: DecisionStoreUpdateMode,
    /// Serializes deployment creation and checkpoint-ID allocation from this instance.
    metadata_write_lock: tokio::sync::Mutex<()>,
    /// Serializes local read/compare/overwrite transitions across every store instance that owns
    /// this namespace. Shared stores use native object-store CAS instead.
    local_metadata_rmw_lock: Option<Arc<tokio::sync::Mutex<()>>>,
    /// Last checkpoint-ID head observed by this instance. Shared-store CAS detects stale entries.
    checkpoint_id_head: parking_lot::Mutex<Option<VersionedCheckpointIdHead>>,
    /// IDs already covered by the current local durable reservation.
    local_reservation: parking_lot::Mutex<LocalReservation>,
    deployment_id: tokio::sync::OnceCell<String>,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DecisionStoreUpdateMode {
    NativeCas,
    LocalSingleWriter,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum LocalMetadataNamespace {
    /// Generic local stores share an authority by cloning the same `Arc<dyn ObjectStore>`.
    #[cfg(test)]
    StoreAuthority(usize),
    /// Filesystem constructors can identify independently opened stores by canonical root.
    Filesystem(PathBuf),
}

fn shared_local_metadata_rmw_lock(
    namespace: LocalMetadataNamespace,
) -> Arc<tokio::sync::Mutex<()>> {
    static LOCKS: OnceLock<
        parking_lot::Mutex<
            rustc_hash::FxHashMap<LocalMetadataNamespace, Weak<tokio::sync::Mutex<()>>>,
        >,
    > = OnceLock::new();

    let mut locks = LOCKS
        .get_or_init(|| parking_lot::Mutex::new(rustc_hash::FxHashMap::default()))
        .lock();
    locks.retain(|_, lock| lock.strong_count() != 0);
    if let Some(lock) = locks.get(&namespace).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    locks.insert(namespace, Arc::downgrade(&lock));
    lock
}

impl std::fmt::Debug for CheckpointDecisionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointDecisionStore")
            .field("update_mode", &self.update_mode)
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
#[serde(deny_unknown_fields)]
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
    /// Exact global recovery index selected by a Commit.
    pub committed_checkpoint: Option<CommittedCheckpointRef>,
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

/// Exact position of a crash-resumable local checkpoint cleanup.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CheckpointRetentionCursor {
    /// Recovery cut that must remain intact throughout this cleanup.
    pub protected: CommittedCheckpointRef,
    /// Expired cut currently being retired.
    pub current: CommittedCheckpointRef,
    /// Exact predecessor of `current`.
    pub next: Option<CommittedCheckpointRef>,
    /// Exclusive lower boundary of this cleanup.
    pub stop_before: Option<CommittedCheckpointRef>,
}

/// Durable phase of local checkpoint retention.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "phase")]
pub enum CheckpointRetentionState {
    /// Only this recovery cut is retained and no deletion is pending.
    Idle {
        /// Authoritative retained recovery cut.
        protected: CommittedCheckpointRef,
    },
    /// Referenced node-data objects for `current` may be deleted.
    DeleteData {
        /// Exact bounded cleanup position.
        cursor: CheckpointRetentionCursor,
    },
    /// Node data is settled; the manifest and then index may be deleted.
    DeleteMetadata {
        /// Exact bounded cleanup position.
        cursor: CheckpointRetentionCursor,
    },
}

/// Result of a conditional retention-head transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointRetentionUpdateResult {
    /// This call durably changed the retention head.
    Applied(CheckpointRetentionState),
    /// The requested state was already durable or an active cleanup must resume first.
    Unchanged(CheckpointRetentionState),
    /// Another writer changed the retention head.
    Conflict {
        /// Durable state observed after the failed transition.
        current: Option<CheckpointRetentionState>,
    },
}

const CHECKPOINT_OUTCOME_VERSION: u32 = 3;
const CHECKPOINT_DECISION_HEAD_VERSION: u32 = 2;
const CHECKPOINT_DECISION_HEAD_MAX_BYTES: u64 = 128 * 1_024;
const CHECKPOINT_RETENTION_HEAD_VERSION: u32 = 1;
const CHECKPOINT_RETENTION_HEAD_MAX_BYTES: u64 = 64 * 1_024;
const ABORTED_COMMITTED_CHECKPOINT_SEAL_VERSION: u32 = 1;

#[derive(Debug, serde::Serialize)]
struct AbortedCommittedCheckpointSeal<'a> {
    version: u32,
    deployment_id: &'a str,
    candidate: &'a CommittedCheckpointRef,
}

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
        if self.epoch != self.checkpoint_id {
            return Err(DecisionError::Conflict(format!(
                "outcome for epoch {path_epoch} has non-canonical checkpoint ID {}; runtime \
                 outcomes require epoch == checkpoint ID",
                self.checkpoint_id
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
                if fence.is_canonical() && proof.is_canonical() => {}
            (CheckpointScope::Cluster, Some(fence), Some(proof))
                if !fence.is_canonical() || !proof.is_canonical() =>
            {
                return Err(DecisionError::Conflict(format!(
                    "cluster outcome for epoch {path_epoch} has a non-canonical assignment fence \
                     or leader proof"
                )));
            }
            (CheckpointScope::Cluster, _, _) => {
                return Err(DecisionError::Conflict(format!(
                    "cluster outcome for epoch {path_epoch} requires an assignment fence and \
                     leader proof"
                )));
            }
        }
        if self.is_commit()
            && self.scope == CheckpointScope::Cluster
            && self
                .assignment_fence
                .as_ref()
                .zip(self.leader_proof.as_ref())
                .is_some_and(|(fence, proof)| {
                    fence.participant_incarnation(proof.owner.node_id) != Some(proof.owner.boot_id)
                })
        {
            return Err(DecisionError::Conflict(format!(
                "cluster Commit for epoch {path_epoch} requires its leader proof in the assignment fence"
            )));
        }

        match (&self.verdict, self.committed_checkpoint.as_ref()) {
            (CheckpointVerdict::Commit, Some(reference)) => {
                reference.validate().map_err(|error| {
                    DecisionError::Conflict(format!(
                        "commit outcome for epoch {path_epoch} has an invalid committed checkpoint reference: {error}"
                    ))
                })?;
                if reference.epoch != self.epoch || reference.checkpoint_id != self.checkpoint_id {
                    return Err(DecisionError::Conflict(format!(
                        "commit outcome for epoch {path_epoch} names a different committed checkpoint"
                    )));
                }
            }
            (CheckpointVerdict::Commit, None) => {
                return Err(DecisionError::Conflict(format!(
                    "commit outcome for epoch {path_epoch} requires a committed checkpoint reference"
                )));
            }
            (CheckpointVerdict::Abort, Some(_)) => {
                return Err(DecisionError::Conflict(format!(
                    "abort outcome for epoch {path_epoch} cannot carry a committed checkpoint reference"
                )));
            }
            (CheckpointVerdict::Abort, None) => {}
        }
        Ok(())
    }

    /// Whether this outcome irrevocably selected commit.
    #[must_use]
    pub fn is_commit(&self) -> bool {
        matches!(self.verdict, CheckpointVerdict::Commit)
    }
}

/// Durable checkpoint namespace identity and its shared-store allocation head.
///
/// `id` never changes. Deleting this single authority creates a new deployment identity before
/// checkpoint IDs restart, so surviving external sinks cannot confuse the new sequence with the
/// previous writer. `allocation_id` identifies the last CAS proposal for lost-response recovery.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DeploymentIdentity {
    version: u32,
    id: String,
    allocator_mode: DecisionStoreUpdateMode,
    checkpoint_id: u64,
    allocation_id: String,
}

const DEPLOYMENT_IDENTITY_VERSION: u32 = 3;
const DEPLOYMENT_IDENTITY_MAX_BYTES: u64 = 1_024;

/// Durable proof that every named checkpoint-committable sink may have opened this attempt.
///
/// The witness is created before any external begin call and remains live until the exact attempt
/// reaches a terminal outcome or every named sink confirms rollback. Recovery must reconcile the
/// attempt before opening a later one.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CheckpointSinkOpenWitness {
    version: u32,
    /// Durable deployment incarnation that owns this witness.
    pub deployment_id: String,
    /// Logical pipeline and recovery-state ABI identity.
    pub pipeline_identity: PipelineIdentity,
    /// Runtime participant that owns the named sink handles (`0` in embedded/local mode).
    pub participant_id: u64,
    /// Exact canonical attempt that may be externally open.
    pub attempt: CheckpointAttempt,
    /// Canonically sorted, unique checkpoint-committable sink names.
    pub committable_sinks: Vec<String>,
    /// Unique create proposal used to reconcile an ambiguous object-store response.
    create_token: String,
}

const CHECKPOINT_SINK_OPEN_WITNESS_VERSION: u32 = 1;
const CHECKPOINT_SINK_OPEN_WITNESS_MAX_BYTES: u64 = 64 * 1_024;
const CHECKPOINT_SINK_OPEN_WITNESS_MAX_SINKS: usize = 1_024;

/// Versioned singleton that never returns to an absent state after its first open transition.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CheckpointSinkOpenWitnessSlot {
    version: u32,
    state: CheckpointSinkOpenWitnessSlotState,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum CheckpointSinkOpenWitnessSlotState {
    Open {
        witness: CheckpointSinkOpenWitness,
    },
    Closed {
        witness: CheckpointSinkOpenWitness,
        close_token: String,
    },
}

impl CheckpointSinkOpenWitnessSlot {
    fn open(witness: CheckpointSinkOpenWitness) -> Self {
        Self {
            version: CHECKPOINT_SINK_OPEN_WITNESS_SLOT_VERSION,
            state: CheckpointSinkOpenWitnessSlotState::Open { witness },
        }
    }

    fn closed(witness: CheckpointSinkOpenWitness) -> Self {
        Self {
            version: CHECKPOINT_SINK_OPEN_WITNESS_SLOT_VERSION,
            state: CheckpointSinkOpenWitnessSlotState::Closed {
                witness,
                close_token: uuid::Uuid::now_v7().to_string(),
            },
        }
    }

    const fn witness(&self) -> &CheckpointSinkOpenWitness {
        match &self.state {
            CheckpointSinkOpenWitnessSlotState::Open { witness }
            | CheckpointSinkOpenWitnessSlotState::Closed { witness, .. } => witness,
        }
    }
}

const CHECKPOINT_SINK_OPEN_WITNESS_SLOT_VERSION: u32 = 1;

#[derive(Debug)]
struct VersionedCheckpointSinkOpenWitnessSlot {
    slot: CheckpointSinkOpenWitnessSlot,
    update_version: UpdateVersion,
}

#[derive(Debug, Clone)]
struct VersionedCheckpointIdHead {
    head: DeploymentIdentity,
    update_version: UpdateVersion,
}

#[derive(Debug, Default)]
struct LocalReservation {
    next_id: Option<u64>,
    end: u64,
}

const LOCAL_RESERVATION_SIZE: u64 = 65_536;

/// Durable inventory for one checkpoint attempt that may have written artifacts.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CheckpointArtifactInventory {
    /// Durable deployment incarnation that owns the artifacts.
    pub deployment_id: String,
    /// Logical pipeline and recovery-state ABI identity.
    pub pipeline_identity: PipelineIdentity,
    /// Exact canonical checkpoint attempt.
    pub attempt: CheckpointAttempt,
    /// Exact cluster assignment, absent for local checkpoints.
    pub assignment_fence: Option<CheckpointAssignmentFence>,
}

impl CheckpointArtifactInventory {
    /// Validate the canonical persisted shape.
    ///
    /// # Errors
    /// Returns an error for a foreign format or non-canonical identity.
    pub fn validate(&self) -> Result<(), String> {
        let deployment = uuid::Uuid::parse_str(&self.deployment_id)
            .map_err(|error| format!("invalid deployment identity: {error}"))?;
        if deployment.is_nil() || deployment.to_string() != self.deployment_id {
            return Err("deployment identity must be a canonical non-nil UUID".into());
        }
        if !self.pipeline_identity.is_canonical() {
            return Err("pipeline identity is not canonical".into());
        }
        if !self.attempt.is_canonical() {
            return Err("checkpoint attempt is not canonical".into());
        }
        if self
            .assignment_fence
            .as_ref()
            .is_some_and(|fence| !fence.is_canonical())
        {
            return Err("checkpoint assignment fence is not canonical".into());
        }
        Ok(())
    }
}

/// Result of a conditional checkpoint artifact-inventory transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointArtifactInventoryUpdateResult {
    /// This call durably changed the decision head.
    Applied,
    /// The exact requested state was already durable.
    Unchanged,
    /// Another attempt or terminal transition changed the decision head.
    Conflict {
        /// Active inventory observed after the failed transition.
        current: Option<CheckpointArtifactInventory>,
    },
}

/// Exact local recovery cursor and unresolved artifact inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointDecisionHead {
    /// Greatest terminal local outcome.
    pub latest_terminal: Option<CheckpointOutcome>,
    /// Greatest committed local outcome, including its exact committed-index reference.
    pub latest_commit: Option<CheckpointOutcome>,
    /// Exact unresolved attempt whose artifacts require a terminal decision or cleanup.
    pub active_artifacts: Option<CheckpointArtifactInventory>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DurableCheckpointDecisionHead {
    version: u32,
    deployment_id: String,
    latest_terminal: Option<CheckpointOutcome>,
    latest_commit: Option<CheckpointOutcome>,
    active_artifacts: Option<CheckpointArtifactInventory>,
}

#[derive(Debug)]
struct VersionedCheckpointDecisionHead {
    head: DurableCheckpointDecisionHead,
    update_version: UpdateVersion,
}

#[derive(Debug)]
enum DecisionHeadCasResult {
    Applied,
    Unchanged,
    Conflict(Option<Box<DurableCheckpointDecisionHead>>),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DurableCheckpointRetentionHead {
    version: u32,
    deployment_id: String,
    state: CheckpointRetentionState,
}

#[derive(Debug)]
struct VersionedCheckpointRetentionHead {
    head: DurableCheckpointRetentionHead,
    update_version: UpdateVersion,
}

impl CheckpointDecisionStore {
    fn ensure_control_record_size(
        record: &str,
        size: u64,
        maximum: u64,
    ) -> Result<(), DecisionError> {
        if size > maximum {
            return Err(DecisionError::Conflict(format!(
                "{record} is {size} bytes; maximum is {maximum}"
            )));
        }
        Ok(())
    }

    fn encode_control_record<T: serde::Serialize>(
        record: &str,
        value: &T,
        maximum: u64,
    ) -> Result<Bytes, DecisionError> {
        let payload = serde_json::to_vec(value)
            .map(Bytes::from)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        let size = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        Self::ensure_control_record_size(record, size, maximum)?;
        Ok(payload)
    }

    async fn read_control_record_bytes(
        result: GetResult,
        record: &str,
        maximum: u64,
        expected_size: Option<u64>,
    ) -> Result<Bytes, DecisionError> {
        Self::ensure_control_record_size(record, result.meta.size, maximum)?;
        if let Some(expected_size) = expected_size {
            if result.meta.size != expected_size {
                return Err(DecisionError::Conflict(format!(
                    "{record} is {} bytes, expected {expected_size}",
                    result.meta.size
                )));
            }
        }
        let metadata_size = result.meta.size;
        if result.range.start != 0 || result.range.end != metadata_size {
            return Err(DecisionError::Conflict(format!(
                "{record} returned byte range {}..{}, expected 0..{metadata_size}",
                result.range.start, result.range.end
            )));
        }
        let capacity = usize::try_from(metadata_size).map_err(|_| {
            DecisionError::Conflict(format!(
                "{record} length {metadata_size} exceeds this process address space"
            ))
        })?;
        let mut bytes = BytesMut::with_capacity(capacity);
        let mut stream = result.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| DecisionError::Io(error.to_string()))?;
            let next_len = bytes.len().checked_add(chunk.len()).ok_or_else(|| {
                DecisionError::Conflict(format!("{record} payload length overflow"))
            })?;
            if next_len > capacity {
                return Err(DecisionError::Conflict(format!(
                    "{record} payload exceeded its advertised {metadata_size}-byte length"
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() != capacity {
            return Err(DecisionError::Conflict(format!(
                "{record} payload length changed while reading"
            )));
        }
        Ok(bytes.freeze())
    }

    async fn get_control_record(
        &self,
        path: &OsPath,
        record: &str,
        maximum: u64,
    ) -> Result<Option<GetResult>, DecisionError> {
        let request_end = maximum.checked_add(1).ok_or_else(|| {
            DecisionError::Conflict(format!("{record} maximum cannot be range-bounded"))
        })?;
        let result = match self
            .store
            .get_opts(
                path,
                GetOptions {
                    range: Some(GetRange::Bounded(0..request_end)),
                    ..GetOptions::default()
                },
            )
            .await
        {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(DecisionError::Io(error.to_string())),
        };
        Self::ensure_control_record_size(record, result.meta.size, maximum)?;
        if result.range.start != 0 || result.range.end != result.meta.size {
            return Err(DecisionError::Conflict(format!(
                "{record} returned byte range {}..{}, inconsistent with advertised size {}",
                result.range.start, result.range.end, result.meta.size
            )));
        }
        Ok(Some(result))
    }

    fn require_native_cas_token(
        &self,
        record: &str,
        update_version: &UpdateVersion,
    ) -> Result<(), DecisionError> {
        if self.update_mode == DecisionStoreUpdateMode::NativeCas
            && update_version.e_tag.is_none()
            && update_version.version.is_none()
        {
            return Err(DecisionError::Conflict(format!(
                "shared {record} has neither an ETag nor an object version for CAS"
            )));
        }
        Ok(())
    }

    /// Wrap shared storage that must provide native conditional updates.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self::with_update_mode(store, DecisionStoreUpdateMode::NativeCas, None)
    }

    #[cfg(test)]
    fn local_single_writer(store: Arc<dyn ObjectStore>) -> Self {
        let authority = Arc::as_ptr(&store).cast::<()>() as usize;
        let lock =
            shared_local_metadata_rmw_lock(LocalMetadataNamespace::StoreAuthority(authority));
        Self::with_update_mode(
            store,
            DecisionStoreUpdateMode::LocalSingleWriter,
            Some(lock),
        )
    }

    /// Open crash-durable checkpoint metadata in a caller-owned local directory.
    /// The caller must retain its exclusive namespace lease for the store's write lifetime.
    ///
    /// # Errors
    /// Returns an I/O error when the directory cannot be created, synchronized, or opened.
    pub fn local_filesystem(root: impl AsRef<FsPath>) -> Result<Self, DecisionError> {
        let root = root.as_ref();
        let store = crate::checkpoint::object_store_builder::durable_local_object_store(root)
            .map_err(|error| DecisionError::Io(error.to_string()))?;
        let canonical_root =
            std::fs::canonicalize(root).map_err(|error| DecisionError::Io(error.to_string()))?;
        let lock =
            shared_local_metadata_rmw_lock(LocalMetadataNamespace::Filesystem(canonical_root));
        Ok(Self::with_update_mode(
            store,
            DecisionStoreUpdateMode::LocalSingleWriter,
            Some(lock),
        ))
    }

    fn with_update_mode(
        store: Arc<dyn ObjectStore>,
        update_mode: DecisionStoreUpdateMode,
        local_metadata_rmw_lock: Option<Arc<tokio::sync::Mutex<()>>>,
    ) -> Self {
        debug_assert_eq!(
            update_mode == DecisionStoreUpdateMode::LocalSingleWriter,
            local_metadata_rmw_lock.is_some()
        );
        Self {
            store,
            update_mode,
            metadata_write_lock: tokio::sync::Mutex::new(()),
            local_metadata_rmw_lock,
            checkpoint_id_head: parking_lot::Mutex::new(None),
            local_reservation: parking_lot::Mutex::new(LocalReservation::default()),
            deployment_id: tokio::sync::OnceCell::new(),
        }
    }

    fn decision_head_path(deployment_id: &str) -> OsPath {
        OsPath::from(format!(
            "checkpoint-decisions/deployment={deployment_id}/head"
        ))
    }

    fn retention_head_path(deployment_id: &str) -> OsPath {
        OsPath::from(format!(
            "checkpoint-retention/deployment={deployment_id}/head"
        ))
    }

    fn committed_checkpoint_path(reference: &CommittedCheckpointRef) -> OsPath {
        OsPath::from(format!(
            "committed-checkpoints/epoch={:020}/checkpoint={:020}/sha256={}",
            reference.epoch, reference.checkpoint_id, reference.sha256
        ))
    }

    fn deployment_identity_path() -> OsPath {
        OsPath::from("checkpoint-deployment/identity.json")
    }

    fn sink_open_witness_path() -> OsPath {
        OsPath::from("checkpoint-sink-open-witness/witness.json")
    }
}

mod allocation;
mod committed;
mod deployment;
mod outcome;
mod retention;
mod sink_witness;

#[cfg(test)]
mod tests;
