//! Durable checkpoint identity, ID allocation, and immutable terminal outcomes.

#[cfg(feature = "cluster")]
use std::collections::BinaryHeap;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, OnceLock, Weak};

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use object_store::path::Path as OsPath;
use object_store::{
    GetOptions, GetRange, GetResult, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    UpdateVersion,
};
#[cfg(feature = "cluster")]
use sha2::{Digest, Sha256};

use crate::checkpoint::{
    CheckpointAssignmentFence, ClusterRecoveryCapsule, LeaderProof, PipelineIdentity,
    RecoveryCapsuleRef,
};
use crate::state::CheckpointAttempt;

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
    /// Active immutable reservation block for the certified local single writer.
    local_reservation: parking_lot::Mutex<LocalReservationState>,
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
const CHECKPOINT_OUTCOME_MAX_BYTES: u64 = 64 * 1_024;
const OUTCOME_GC_FLOOR_MAX_BYTES: u64 = 256 * 1_024;
#[cfg(feature = "cluster")]
const RECOVERY_CAPSULE_GC_BATCH_SIZE: usize = 64;
#[cfg(feature = "cluster")]
const RECOVERY_CAPSULE_GC_CURSOR_MAX_BYTES: u64 = 1_024;

impl CheckpointOutcome {
    #[allow(clippy::too_many_lines)]
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

const DEPLOYMENT_IDENTITY_VERSION: u32 = 2;
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

/// In-memory cursor within a durable immutable local ID block. A restarted process always claims
/// a later block and therefore burns any IDs the previous process did not consume.
#[derive(Debug, Default)]
struct LocalReservationState {
    initialized: bool,
    highest_block: Option<u64>,
    next_id: Option<u64>,
    block_end: u64,
}

const LOCAL_RESERVATION_BLOCK_SIZE: u64 = 65_536;

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
enum RecoveryCapsuleObjectWork {
    Retained,
    Deleted,
    Quarantined,
    Failed,
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
        let store: Arc<dyn ObjectStore> = Arc::new(
            crate::durable_local_store::DurableLocalObjectStore::new(root)
                .map_err(|error| DecisionError::Io(error.to_string()))?,
        );
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
            local_reservation: parking_lot::Mutex::new(LocalReservationState::default()),
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

    fn local_reservation_root() -> OsPath {
        OsPath::from("checkpoint-id-blocks/")
    }

    fn local_reservation_path(block: u64) -> OsPath {
        OsPath::from(format!("checkpoint-id-blocks/block={block:020}"))
    }

    fn deployment_identity_path() -> OsPath {
        OsPath::from("checkpoint-deployment/identity.json")
    }

    fn sink_open_witness_path() -> OsPath {
        OsPath::from("checkpoint-sink-open-witness/witness.json")
    }

    #[cfg(feature = "cluster")]
    async fn read_recovery_capsule_gc_cursor(
        &self,
        deployment_id: &str,
    ) -> Result<Option<VersionedRecoveryCapsuleGcCursor>, DecisionError> {
        let path = Self::recovery_capsule_gc_cursor_path(deployment_id);
        let Some(result) = self
            .get_control_record(
                &path,
                "recovery capsule GC cursor",
                RECOVERY_CAPSULE_GC_CURSOR_MAX_BYTES,
            )
            .await?
        else {
            return Ok(None);
        };
        let update_version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        self.require_native_cas_token("recovery capsule GC cursor", &update_version)?;
        let bytes = Self::read_control_record_bytes(
            result,
            "recovery capsule GC cursor",
            RECOVERY_CAPSULE_GC_CURSOR_MAX_BYTES,
            None,
        )
        .await?;
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
        let payload = Self::encode_control_record(
            "recovery capsule GC cursor",
            cursor,
            RECOVERY_CAPSULE_GC_CURSOR_MAX_BYTES,
        )?;
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
        match self
            .store
            .put_opts(&path, PutPayload::from(Bytes::from(encoded)), options)
            .await
        {
            Ok(_) => Ok(reference),
            Err(put_error) => match self.load_recovery_capsule(&reference).await {
                Ok(stored) if stored == *capsule => Ok(reference),
                Ok(_) => Err(DecisionError::Conflict(format!(
                    "recovery capsule '{}' differs from the proposed content",
                    reference.sha256
                ))),
                Err(reconcile_error) => Err(DecisionError::Io(format!(
                    "recovery capsule write failed ({put_error}); reconciliation failed ({reconcile_error})"
                ))),
            },
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
        let record = format!("recovery capsule '{}'", reference.sha256);
        let result = self
            .get_control_record(
                &Self::recovery_capsule_path(reference),
                &record,
                u64::try_from(crate::checkpoint::MAX_RECOVERY_CAPSULE_BYTES).unwrap_or(u64::MAX),
            )
            .await?
            .ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "recovery capsule '{}' is missing",
                    reference.sha256
                ))
            })?;
        let bytes = Self::read_control_record_bytes(
            result,
            &record,
            u64::try_from(crate::checkpoint::MAX_RECOVERY_CAPSULE_BYTES).unwrap_or(u64::MAX),
            Some(reference.len),
        )
        .await?;
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

    fn validate_deployment_identity(
        identity: &DeploymentIdentity,
    ) -> Result<String, DecisionError> {
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
        let allocation_id = uuid::Uuid::parse_str(&identity.allocation_id).map_err(|error| {
            DecisionError::Conflict(format!(
                "deployment identity has invalid allocation identity: {error}"
            ))
        })?;
        if allocation_id.is_nil() || allocation_id.to_string() != identity.allocation_id {
            return Err(DecisionError::Conflict(
                "deployment identity must use a canonical non-nil allocation identity".into(),
            ));
        }
        Ok(canonical)
    }

    async fn read_deployment_identity(
        &self,
    ) -> Result<Option<VersionedCheckpointIdHead>, DecisionError> {
        let Some(result) = self
            .get_control_record(
                &Self::deployment_identity_path(),
                "deployment identity",
                DEPLOYMENT_IDENTITY_MAX_BYTES,
            )
            .await?
        else {
            return Ok(None);
        };
        let update_version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        self.require_native_cas_token("deployment identity", &update_version)?;
        let bytes = Self::read_control_record_bytes(
            result,
            "deployment identity",
            DEPLOYMENT_IDENTITY_MAX_BYTES,
            None,
        )
        .await?;
        let identity: DeploymentIdentity = serde_json::from_slice(&bytes)
            .map_err(|error| DecisionError::Conflict(format!("deployment identity: {error}")))?;
        Self::validate_deployment_identity(&identity)?;
        if identity.allocator_mode != self.update_mode {
            return Err(DecisionError::Conflict(format!(
                "deployment identity allocator mode {:?} cannot be opened as {:?}",
                identity.allocator_mode, self.update_mode
            )));
        }
        let canonical = serde_json::to_vec(&identity)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(DecisionError::Conflict(
                "deployment identity does not use its canonical body".into(),
            ));
        }
        Ok(Some(VersionedCheckpointIdHead {
            head: identity,
            update_version,
        }))
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
        let _guard = self.metadata_write_lock.lock().await;
        if let Some(identity) = self.deployment_id.get() {
            return Ok(identity.clone());
        }
        if let Some(stored) = self.read_deployment_identity().await? {
            let identity = stored.head.id.clone();
            self.cache_checkpoint_id_head(Some(stored));
            let _ = self.deployment_id.set(identity.clone());
            return Ok(identity);
        }

        let identity = DeploymentIdentity {
            version: DEPLOYMENT_IDENTITY_VERSION,
            id: uuid::Uuid::now_v7().to_string(),
            allocator_mode: self.update_mode,
            checkpoint_id: 0,
            allocation_id: uuid::Uuid::now_v7().to_string(),
        };
        let payload = Self::encode_control_record(
            "deployment identity",
            &identity,
            DEPLOYMENT_IDENTITY_MAX_BYTES,
        )?;
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
            Ok(put_result) => {
                let update_version: UpdateVersion = put_result.into();
                if self.update_mode == DecisionStoreUpdateMode::NativeCas
                    && update_version.e_tag.is_none()
                    && update_version.version.is_none()
                {
                    self.cache_checkpoint_id_head(None);
                } else {
                    self.cache_checkpoint_id_head(Some(VersionedCheckpointIdHead {
                        head: identity.clone(),
                        update_version,
                    }));
                }
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
                let identity = stored.head.id.clone();
                self.cache_checkpoint_id_head(Some(stored));
                let _ = self.deployment_id.set(identity.clone());
                Ok(identity)
            }
            Err(error) => match self.read_deployment_identity().await? {
                Some(stored) => {
                    let identity = stored.head.id.clone();
                    self.cache_checkpoint_id_head(Some(stored));
                    let _ = self.deployment_id.set(identity.clone());
                    Ok(identity)
                }
                None => Err(DecisionError::Io(error.to_string())),
            },
        }
    }

    fn local_reservation_block(location: &str) -> Result<u64, DecisionError> {
        let value = location
            .strip_prefix("checkpoint-id-blocks/block=")
            .ok_or_else(|| {
                DecisionError::Conflict(format!("malformed checkpoint ID block path: {location}"))
            })?;
        if value.len() != 20 || value.contains('/') {
            return Err(DecisionError::Conflict(format!(
                "malformed checkpoint ID block path: {location}"
            )));
        }
        let block = value.parse::<u64>().map_err(|_| {
            DecisionError::Conflict(format!("malformed checkpoint ID block path: {location}"))
        })?;
        if Self::local_reservation_path(block).as_ref() != location {
            return Err(DecisionError::Conflict(format!(
                "checkpoint ID block path is not canonical: {location}"
            )));
        }
        Self::local_reservation_block_bounds(block)?;
        Ok(block)
    }

    const fn local_reservation_block_for(checkpoint_id: u64) -> u64 {
        (checkpoint_id - 1) / LOCAL_RESERVATION_BLOCK_SIZE
    }

    fn local_reservation_block_bounds(block: u64) -> Result<(u64, u64), DecisionError> {
        let start = block
            .checked_mul(LOCAL_RESERVATION_BLOCK_SIZE)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| {
                DecisionError::Conflict("checkpoint ID space exhausted u64".to_owned())
            })?;
        let end = start.saturating_add(LOCAL_RESERVATION_BLOCK_SIZE - 1);
        Ok((start, end))
    }

    async fn initialize_local_reservation(&self) -> Result<(), DecisionError> {
        if self.local_reservation.lock().initialized {
            return Ok(());
        }

        let mut entries = self.store.list(Some(&Self::local_reservation_root()));
        let mut highest = None;
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|error| DecisionError::Io(error.to_string()))?;
            let block = Self::local_reservation_block(entry.location.as_ref())?;
            highest = Some(highest.map_or(block, |current: u64| current.max(block)));
        }
        let mut state = self.local_reservation.lock();
        state.initialized = true;
        state.highest_block = highest;
        state.next_id = None;
        state.block_end = 0;
        Ok(())
    }

    fn consume_local_reservation(&self, minimum: u64) -> Option<u64> {
        let mut state = self.local_reservation.lock();
        let next_id = state.next_id?;
        let checkpoint_id = next_id.max(minimum);
        if checkpoint_id > state.block_end {
            state.next_id = None;
            return None;
        }
        state.next_id = checkpoint_id
            .checked_add(1)
            .filter(|next| *next <= state.block_end);
        Some(checkpoint_id)
    }

    async fn allocate_local_checkpoint_id_at_least(
        &self,
        minimum: u64,
    ) -> Result<u64, DecisionError> {
        self.initialize_local_reservation().await?;
        if let Some(checkpoint_id) = self.consume_local_reservation(minimum) {
            return Ok(checkpoint_id);
        }

        let minimum_block = Self::local_reservation_block_for(minimum);
        let highest_block = self.local_reservation.lock().highest_block;
        let mut candidate = match highest_block {
            Some(block) => block
                .checked_add(1)
                .map(|next| next.max(minimum_block))
                .ok_or_else(|| {
                    DecisionError::Conflict("checkpoint ID space exhausted u64".to_owned())
                })?,
            None => minimum_block,
        };

        loop {
            let (start, end) = Self::local_reservation_block_bounds(candidate)?;
            let result = self
                .store
                .put_opts(
                    &Self::local_reservation_path(candidate),
                    PutPayload::from(Bytes::new()),
                    PutOptions {
                        mode: PutMode::Create,
                        ..PutOptions::default()
                    },
                )
                .await;
            match result {
                Ok(_) => {
                    let checkpoint_id = start.max(minimum);
                    let mut state = self.local_reservation.lock();
                    state.highest_block = Some(
                        state
                            .highest_block
                            .map_or(candidate, |current| current.max(candidate)),
                    );
                    state.block_end = end;
                    state.next_id = checkpoint_id
                        .checked_add(1)
                        .filter(|next| *next <= state.block_end);
                    return Ok(checkpoint_id);
                }
                Err(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. },
                ) => {
                    candidate = candidate.checked_add(1).ok_or_else(|| {
                        DecisionError::Conflict("checkpoint ID space exhausted u64".to_owned())
                    })?;
                    self.local_reservation.lock().highest_block = Some(candidate - 1);
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(DecisionError::Io(error.to_string())),
            }
        }
    }

    async fn read_checkpoint_id_head(
        &self,
        deployment_id: &str,
    ) -> Result<Option<VersionedCheckpointIdHead>, DecisionError> {
        let observed = self.read_deployment_identity().await?;
        if let Some(observed) = observed.as_ref() {
            if observed.head.id != deployment_id {
                return Err(DecisionError::Conflict(format!(
                    "checkpoint ID head belongs to deployment {}, current deployment is {deployment_id}",
                    observed.head.id
                )));
            }
        }
        Ok(observed)
    }

    fn cache_checkpoint_id_head(&self, head: Option<VersionedCheckpointIdHead>) {
        *self.checkpoint_id_head.lock() = head;
    }

    fn validate_checkpoint_id_head_progress(
        prior: Option<&VersionedCheckpointIdHead>,
        observed: Option<&VersionedCheckpointIdHead>,
    ) -> Result<(), DecisionError> {
        match (prior, observed) {
            (Some(prior), None) => Err(DecisionError::Conflict(format!(
                "checkpoint ID head disappeared after durable ID {}",
                prior.head.checkpoint_id
            ))),
            (Some(prior), Some(observed))
                if observed.head.checkpoint_id < prior.head.checkpoint_id =>
            {
                Err(DecisionError::Conflict(format!(
                    "checkpoint ID head regressed from {} to {}",
                    prior.head.checkpoint_id, observed.head.checkpoint_id
                )))
            }
            (Some(prior), Some(observed))
                if observed.head.checkpoint_id == prior.head.checkpoint_id
                    && observed.head != prior.head =>
            {
                Err(DecisionError::Conflict(format!(
                    "checkpoint ID {} changed allocation identity without advancing",
                    prior.head.checkpoint_id
                )))
            }
            _ => Ok(()),
        }
    }

    async fn allocate_shared_checkpoint_id_at_least(
        &self,
        deployment_id: &str,
        minimum: u64,
    ) -> Result<u64, DecisionError> {
        let allocation_id = uuid::Uuid::now_v7().to_string();
        let mut current = self.checkpoint_id_head.lock().clone();
        if current.is_none() {
            current = self.read_checkpoint_id_head(deployment_id).await?;
            if current.is_none() {
                return Err(DecisionError::Conflict(format!(
                    "checkpoint ID authority for deployment {deployment_id} disappeared"
                )));
            }
            self.cache_checkpoint_id_head(current.clone());
        }

        loop {
            let versioned = current.as_ref().ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "checkpoint ID authority for deployment {deployment_id} disappeared"
                ))
            })?;
            let checkpoint_id = versioned
                .head
                .checkpoint_id
                .checked_add(1)
                .map(|next| next.max(minimum))
                .ok_or_else(|| {
                    DecisionError::Conflict("checkpoint ID space exhausted u64".to_owned())
                })?;
            let head = DeploymentIdentity {
                version: DEPLOYMENT_IDENTITY_VERSION,
                id: deployment_id.to_owned(),
                allocator_mode: self.update_mode,
                checkpoint_id,
                allocation_id: allocation_id.clone(),
            };
            let payload = serde_json::to_vec(&head)
                .map(Bytes::from)
                .map_err(|error| DecisionError::Conflict(error.to_string()))?;
            let mode = PutMode::Update(versioned.update_version.clone());
            let result = self
                .store
                .put_opts(
                    &Self::deployment_identity_path(),
                    PutPayload::from(payload),
                    PutOptions {
                        mode,
                        ..PutOptions::default()
                    },
                )
                .await;
            match result {
                Ok(put_result) => {
                    let update_version: UpdateVersion = put_result.into();
                    if update_version.e_tag.is_none() && update_version.version.is_none() {
                        // The create/update itself is authoritative, but this response cannot
                        // safely seed the next CAS. Force a metadata read on the next allocation.
                        self.cache_checkpoint_id_head(None);
                    } else {
                        self.cache_checkpoint_id_head(Some(VersionedCheckpointIdHead {
                            head,
                            update_version,
                        }));
                    }
                    return Ok(checkpoint_id);
                }
                Err(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. }
                    | object_store::Error::NotFound { .. },
                ) => {
                    let observed = self.read_checkpoint_id_head(deployment_id).await?;
                    Self::validate_checkpoint_id_head_progress(
                        current.as_ref(),
                        observed.as_ref(),
                    )?;
                    current = observed;
                    self.cache_checkpoint_id_head(current.clone());
                    tokio::task::yield_now().await;
                }
                Err(error) => {
                    let observed = self.read_checkpoint_id_head(deployment_id).await?;
                    Self::validate_checkpoint_id_head_progress(
                        current.as_ref(),
                        observed.as_ref(),
                    )?;
                    if observed.as_ref().is_some_and(|value| value.head == head) {
                        self.cache_checkpoint_id_head(observed);
                        return Ok(checkpoint_id);
                    }
                    if observed
                        .as_ref()
                        .is_some_and(|value| value.head.checkpoint_id >= checkpoint_id)
                    {
                        current = observed;
                        self.cache_checkpoint_id_head(current.clone());
                        tokio::task::yield_now().await;
                        continue;
                    }
                    self.cache_checkpoint_id_head(observed);
                    return Err(DecisionError::Io(error.to_string()));
                }
            }
        }
    }

    /// Allocate the next globally ordered checkpoint ID at or above `minimum`.
    ///
    /// Shared stores advance one fixed-size durable high-water object with native compare-and-
    /// swap. The allocation identity in each proposal reconciles a lost write response without
    /// returning an ID won by another coordinator. Node-local filesystems claim immutable blocks
    /// and allocate from the active block in memory. They are cancellation-safe under the
    /// constructor's exclusive single-writer namespace contract, remove durable I/O from the hot
    /// path, and burn the unused block tail on restart. Gaps are valid and IDs are never reused.
    ///
    /// # Errors
    /// Object-store I/O, malformed or foreign durable state, a shared store without conditional
    /// update support, or exhaustion of the `u64` ID space.
    pub async fn allocate_checkpoint_id_at_least(
        &self,
        minimum: u64,
    ) -> Result<u64, DecisionError> {
        if minimum == 0 {
            return Err(DecisionError::Conflict(
                "minimum checkpoint ID must be nonzero".to_owned(),
            ));
        }

        let deployment_id = self.load_or_create_deployment_id().await?;
        let _guard = self.metadata_write_lock.lock().await;
        match self.update_mode {
            DecisionStoreUpdateMode::NativeCas => {
                self.allocate_shared_checkpoint_id_at_least(&deployment_id, minimum)
                    .await
            }
            DecisionStoreUpdateMode::LocalSingleWriter => {
                self.allocate_local_checkpoint_id_at_least(minimum).await
            }
        }
    }

    #[cfg(test)]
    /// Allocate from the default floor in unit tests.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable allocator is unavailable or invalid.
    pub async fn allocate_checkpoint_id(&self) -> Result<u64, DecisionError> {
        self.allocate_checkpoint_id_at_least(1).await
    }

    fn validate_sink_open_witness_shape(
        witness: &CheckpointSinkOpenWitness,
    ) -> Result<(), DecisionError> {
        if witness.version != CHECKPOINT_SINK_OPEN_WITNESS_VERSION {
            return Err(DecisionError::Conflict(format!(
                "sink-open witness version {} is unsupported (expected \
                 {CHECKPOINT_SINK_OPEN_WITNESS_VERSION})",
                witness.version
            )));
        }
        let deployment = uuid::Uuid::parse_str(&witness.deployment_id).map_err(|error| {
            DecisionError::Conflict(format!(
                "sink-open witness has invalid deployment identity: {error}"
            ))
        })?;
        if deployment.is_nil() || deployment.to_string() != witness.deployment_id {
            return Err(DecisionError::Conflict(
                "sink-open witness must use a canonical non-nil deployment identity".into(),
            ));
        }
        if let Some(error) = witness.pipeline_identity.validation_error() {
            return Err(DecisionError::Conflict(format!(
                "sink-open witness has an invalid pipeline identity: {error}"
            )));
        }
        if !witness.attempt.is_canonical() {
            return Err(DecisionError::Conflict(
                "sink-open witness must use one nonzero canonical checkpoint ID".into(),
            ));
        }
        if witness.committable_sinks.is_empty()
            || witness.committable_sinks.len() > CHECKPOINT_SINK_OPEN_WITNESS_MAX_SINKS
        {
            return Err(DecisionError::Conflict(format!(
                "sink-open witness must name between 1 and \
                 {CHECKPOINT_SINK_OPEN_WITNESS_MAX_SINKS} committable sinks"
            )));
        }
        if witness
            .committable_sinks
            .iter()
            .any(|name| name.is_empty() || name.trim() != name)
        {
            return Err(DecisionError::Conflict(
                "sink-open witness contains a non-canonical sink name".into(),
            ));
        }
        if witness
            .committable_sinks
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(DecisionError::Conflict(
                "sink-open witness sink names must be strictly sorted and unique".into(),
            ));
        }
        let create_token = uuid::Uuid::parse_str(&witness.create_token).map_err(|error| {
            DecisionError::Conflict(format!(
                "sink-open witness has invalid create token: {error}"
            ))
        })?;
        if create_token.is_nil() || create_token.to_string() != witness.create_token {
            return Err(DecisionError::Conflict(
                "sink-open witness must use a canonical non-nil create token".into(),
            ));
        }
        Ok(())
    }

    fn validate_sink_open_witness_slot_shape(
        slot: &CheckpointSinkOpenWitnessSlot,
    ) -> Result<(), DecisionError> {
        if slot.version != CHECKPOINT_SINK_OPEN_WITNESS_SLOT_VERSION {
            return Err(DecisionError::Conflict(format!(
                "sink-open witness slot version {} is unsupported (expected \
                 {CHECKPOINT_SINK_OPEN_WITNESS_SLOT_VERSION})",
                slot.version
            )));
        }
        Self::validate_sink_open_witness_shape(slot.witness())?;
        if let CheckpointSinkOpenWitnessSlotState::Closed { close_token, .. } = &slot.state {
            let token = uuid::Uuid::parse_str(close_token).map_err(|error| {
                DecisionError::Conflict(format!(
                    "sink-open witness slot has invalid close token: {error}"
                ))
            })?;
            if token.is_nil() || token.to_string() != *close_token {
                return Err(DecisionError::Conflict(
                    "sink-open witness slot must use a canonical non-nil close token".into(),
                ));
            }
        }
        Ok(())
    }

    fn encode_sink_open_witness_slot(
        slot: &CheckpointSinkOpenWitnessSlot,
    ) -> Result<Bytes, DecisionError> {
        Self::validate_sink_open_witness_slot_shape(slot)?;
        Self::encode_control_record(
            "sink-open witness slot",
            slot,
            CHECKPOINT_SINK_OPEN_WITNESS_MAX_BYTES,
        )
    }

    async fn read_sink_open_witness_record(
        &self,
    ) -> Result<Option<VersionedCheckpointSinkOpenWitnessSlot>, DecisionError> {
        let Some(result) = self
            .get_control_record(
                &Self::sink_open_witness_path(),
                "sink-open witness",
                CHECKPOINT_SINK_OPEN_WITNESS_MAX_BYTES,
            )
            .await?
        else {
            return Ok(None);
        };
        let update_version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        self.require_native_cas_token("sink-open witness slot", &update_version)?;
        let bytes = Self::read_control_record_bytes(
            result,
            "sink-open witness",
            CHECKPOINT_SINK_OPEN_WITNESS_MAX_BYTES,
            None,
        )
        .await?;
        let slot: CheckpointSinkOpenWitnessSlot = serde_json::from_slice(&bytes)
            .map_err(|error| DecisionError::Conflict(format!("sink-open witness slot: {error}")))?;
        Self::validate_sink_open_witness_slot_shape(&slot)?;
        let canonical = serde_json::to_vec(&slot)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(DecisionError::Conflict(format!(
                "sink-open witness slot for checkpoint {} does not use its canonical body",
                slot.witness().attempt.checkpoint_id
            )));
        }
        Ok(Some(VersionedCheckpointSinkOpenWitnessSlot {
            slot,
            update_version,
        }))
    }

    fn validate_sink_open_witness_slot_deployment(
        slot: &CheckpointSinkOpenWitnessSlot,
        deployment_id: &str,
    ) -> Result<(), DecisionError> {
        if slot.witness().deployment_id == deployment_id {
            return Ok(());
        }
        Err(DecisionError::Conflict(format!(
            "sink-open witness belongs to deployment {}, current deployment is {deployment_id}",
            slot.witness().deployment_id
        )))
    }

    fn sink_open_witness_put_mode(&self, expected: Option<UpdateVersion>) -> PutMode {
        match (self.update_mode, expected) {
            (_, None) => PutMode::Create,
            (DecisionStoreUpdateMode::NativeCas, Some(version)) => PutMode::Update(version),
            (DecisionStoreUpdateMode::LocalSingleWriter, Some(_)) => PutMode::Overwrite,
        }
    }

    async fn put_sink_open_witness_slot(
        &self,
        slot: &CheckpointSinkOpenWitnessSlot,
        expected: Option<UpdateVersion>,
    ) -> Result<(), object_store::Error> {
        let payload = Self::encode_sink_open_witness_slot(slot).map_err(|error| {
            object_store::Error::Generic {
                store: "CheckpointDecisionStore",
                source: Box::new(error),
            }
        })?;
        self.store
            .put_opts(
                &Self::sink_open_witness_path(),
                PutPayload::from(payload),
                PutOptions {
                    mode: self.sink_open_witness_put_mode(expected),
                    ..PutOptions::default()
                },
            )
            .await
            .map(|_| ())
    }

    /// Read the singleton sink-open owner record.
    ///
    /// # Errors
    /// Object-store I/O, malformed/non-canonical metadata, or foreign deployment state.
    pub async fn sink_open_witness(
        &self,
    ) -> Result<Option<CheckpointSinkOpenWitness>, DecisionError> {
        let deployment_id = self.load_or_create_deployment_id().await?;
        let Some(versioned) = self.read_sink_open_witness_record().await? else {
            return Ok(None);
        };
        Self::validate_sink_open_witness_slot_deployment(&versioned.slot, &deployment_id)?;
        match versioned.slot.state {
            CheckpointSinkOpenWitnessSlotState::Open { witness } => Ok(Some(witness)),
            CheckpointSinkOpenWitnessSlotState::Closed { .. } => Ok(None),
        }
    }

    /// Create the durable witness before invoking any checkpoint-committable sink begin call.
    ///
    /// `committable_sinks` must already be strictly sorted and unique so duplicate runtime names
    /// cannot be silently collapsed into one recovery participant.
    ///
    /// # Errors
    /// Object-store I/O, invalid input, or any malformed, foreign, or conflicting live witness.
    pub async fn create_sink_open_witness(
        &self,
        pipeline_identity: PipelineIdentity,
        participant_id: u64,
        attempt: CheckpointAttempt,
        committable_sinks: Vec<String>,
    ) -> Result<CheckpointSinkOpenWitness, DecisionError> {
        let candidate = CheckpointSinkOpenWitness {
            version: CHECKPOINT_SINK_OPEN_WITNESS_VERSION,
            deployment_id: self.load_or_create_deployment_id().await?,
            pipeline_identity,
            participant_id,
            attempt,
            committable_sinks,
            create_token: uuid::Uuid::now_v7().to_string(),
        };
        Self::validate_sink_open_witness_shape(&candidate)?;
        Self::encode_sink_open_witness_slot(&CheckpointSinkOpenWitnessSlot::open(
            candidate.clone(),
        ))?;
        // An accepted open must always have enough room for its mandatory close tombstone.
        Self::encode_sink_open_witness_slot(&CheckpointSinkOpenWitnessSlot::closed(
            candidate.clone(),
        ))?;

        match self.update_mode {
            DecisionStoreUpdateMode::NativeCas => {
                self.create_sink_open_witness_inner(candidate).await
            }
            DecisionStoreUpdateMode::LocalSingleWriter => {
                let local_lock = self.local_metadata_rmw_lock.as_ref().ok_or_else(|| {
                    DecisionError::Conflict(
                        "local decision store is missing its namespace write lock".to_owned(),
                    )
                })?;
                let _guard = local_lock.lock().await;
                self.create_sink_open_witness_inner(candidate).await
            }
        }
    }

    async fn create_sink_open_witness_inner(
        &self,
        candidate: CheckpointSinkOpenWitness,
    ) -> Result<CheckpointSinkOpenWitness, DecisionError> {
        let current = self.read_sink_open_witness_record().await?;
        let (expected, prior_slot) = match current {
            None => (None, None),
            Some(versioned) => {
                Self::validate_sink_open_witness_slot_deployment(
                    &versioned.slot,
                    &candidate.deployment_id,
                )?;
                match &versioned.slot.state {
                    CheckpointSinkOpenWitnessSlotState::Open { witness } => {
                        return Err(DecisionError::Conflict(format!(
                            "sink-open witness create for checkpoint {} observed conflicting \
                             checkpoint {}",
                            candidate.attempt.checkpoint_id, witness.attempt.checkpoint_id
                        )));
                    }
                    CheckpointSinkOpenWitnessSlotState::Closed { witness, .. }
                        if candidate.attempt.checkpoint_id <= witness.attempt.checkpoint_id =>
                    {
                        return Err(DecisionError::Conflict(format!(
                            "sink-open witness checkpoint {} does not advance closed checkpoint {}",
                            candidate.attempt.checkpoint_id, witness.attempt.checkpoint_id
                        )));
                    }
                    CheckpointSinkOpenWitnessSlotState::Closed { .. } => {}
                }
                (Some(versioned.update_version), Some(versioned.slot))
            }
        };
        let candidate_slot = CheckpointSinkOpenWitnessSlot::open(candidate.clone());
        let create_error = match self
            .put_sink_open_witness_slot(&candidate_slot, expected)
            .await
        {
            Ok(()) => return Ok(candidate),
            Err(error) => error,
        };

        // Only this proposal's exact create token proves that an ambiguous open transition won.
        if let Some(observed) = self.read_sink_open_witness_record().await? {
            Self::validate_sink_open_witness_slot_deployment(
                &observed.slot,
                &candidate.deployment_id,
            )?;
            if observed.slot == candidate_slot {
                return Ok(candidate);
            }
            let conditional_conflict = matches!(
                &create_error,
                object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. }
                    | object_store::Error::NotFound { .. }
            );
            if !conditional_conflict
                && prior_slot
                    .as_ref()
                    .is_some_and(|prior| prior == &observed.slot)
            {
                return Err(DecisionError::Io(create_error.to_string()));
            }
            return Err(DecisionError::Conflict(format!(
                "sink-open witness create for checkpoint {} observed conflicting checkpoint {}",
                candidate.attempt.checkpoint_id,
                observed.slot.witness().attempt.checkpoint_id
            )));
        }

        match create_error {
            object_store::Error::Precondition { .. }
            | object_store::Error::AlreadyExists { .. }
            | object_store::Error::NotFound { .. } => Err(DecisionError::Conflict(format!(
                "sink-open witness for checkpoint {} disappeared after create conflict",
                candidate.attempt.checkpoint_id
            ))),
            error => Err(DecisionError::Io(error.to_string())),
        }
    }

    /// Close exactly the supplied witness after its attempt is terminal or fully rolled back.
    ///
    /// Closure durably replaces the open state. The tombstone makes an old conditional write
    /// harmless after a successor opens and gives ambiguous responses an exact state to reconcile.
    ///
    /// # Errors
    /// Object-store I/O or a malformed, foreign, or different live witness.
    pub async fn clear_sink_open_witness(
        &self,
        expected: &CheckpointSinkOpenWitness,
    ) -> Result<(), DecisionError> {
        Self::validate_sink_open_witness_shape(expected)?;
        let deployment_id = self.load_or_create_deployment_id().await?;
        if expected.deployment_id != deployment_id {
            return Err(DecisionError::Conflict(format!(
                "cannot clear sink-open witness from deployment {}; current deployment is \
                 {deployment_id}",
                expected.deployment_id
            )));
        }
        match self.update_mode {
            DecisionStoreUpdateMode::NativeCas => {
                self.clear_sink_open_witness_inner(expected).await
            }
            DecisionStoreUpdateMode::LocalSingleWriter => {
                let local_lock = self.local_metadata_rmw_lock.as_ref().ok_or_else(|| {
                    DecisionError::Conflict(
                        "local decision store is missing its namespace write lock".to_owned(),
                    )
                })?;
                let _guard = local_lock.lock().await;
                self.clear_sink_open_witness_inner(expected).await
            }
        }
    }

    async fn clear_sink_open_witness_inner(
        &self,
        expected: &CheckpointSinkOpenWitness,
    ) -> Result<(), DecisionError> {
        let Some(current) = self.read_sink_open_witness_record().await? else {
            return Err(DecisionError::Conflict(format!(
                "sink-open witness slot for checkpoint {} is missing",
                expected.attempt.checkpoint_id
            )));
        };
        Self::validate_sink_open_witness_slot_deployment(&current.slot, &expected.deployment_id)?;
        match &current.slot.state {
            CheckpointSinkOpenWitnessSlotState::Closed { witness, .. } if witness == expected => {
                return Ok(());
            }
            CheckpointSinkOpenWitnessSlotState::Open { witness } if witness == expected => {}
            _ => {
                return Err(DecisionError::Conflict(format!(
                    "cannot clear sink-open witness for checkpoint {}; current slot names \
                     checkpoint {} with a different create identity",
                    expected.attempt.checkpoint_id,
                    current.slot.witness().attempt.checkpoint_id
                )));
            }
        }

        let closed = CheckpointSinkOpenWitnessSlot::closed(expected.clone());
        let close_error = match self
            .put_sink_open_witness_slot(&closed, Some(current.update_version))
            .await
        {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        match self.read_sink_open_witness_record().await? {
            Some(observed) if observed.slot == closed => Ok(()),
            Some(observed) => {
                Self::validate_sink_open_witness_slot_deployment(
                    &observed.slot,
                    &expected.deployment_id,
                )?;
                match &observed.slot.state {
                    CheckpointSinkOpenWitnessSlotState::Closed { witness, .. }
                        if witness == expected =>
                    {
                        // Another exact close is equivalent even though its token differs.
                        Ok(())
                    }
                    CheckpointSinkOpenWitnessSlotState::Open { witness } if witness == expected => {
                        Err(DecisionError::Io(close_error.to_string()))
                    }
                    _ => {
                        // A different valid generation can only follow a successful close CAS.
                        // The stale transition cannot touch it because its object version differs.
                        Ok(())
                    }
                }
            }
            None => Err(DecisionError::Conflict(format!(
                "sink-open witness slot disappeared while closing checkpoint {}: {close_error}",
                expected.attempt.checkpoint_id
            ))),
        }
    }

    /// Epoch segment of a canonical create-once terminal outcome object.
    fn outcome_epoch_segment(loc: &str) -> Option<&str> {
        let segment = loc
            .strip_prefix("checkpoint-outcomes/")?
            .strip_suffix("/outcome")?
            .strip_prefix("epoch=")?;
        let epoch = segment.parse::<u64>().ok()?;
        (epoch != 0 && Self::outcome_path(epoch).as_ref() == loc).then_some(segment)
    }

    #[allow(clippy::too_many_arguments)]
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
        let record = format!("checkpoint outcome for epoch {epoch}");
        let Some(result) = self
            .get_control_record(path, &record, CHECKPOINT_OUTCOME_MAX_BYTES)
            .await?
        else {
            return Ok(None);
        };
        let bytes =
            Self::read_control_record_bytes(result, &record, CHECKPOINT_OUTCOME_MAX_BYTES, None)
                .await?;
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
        let payload = Self::encode_control_record(
            "checkpoint outcome",
            &candidate,
            CHECKPOINT_OUTCOME_MAX_BYTES,
        )?;
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
        let Some(result) = self
            .get_control_record(&path, "outcome GC floor", OUTCOME_GC_FLOOR_MAX_BYTES)
            .await?
        else {
            return Ok(None);
        };
        let update_version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        self.require_native_cas_token("outcome GC floor", &update_version)?;
        let bytes = Self::read_control_record_bytes(
            result,
            "outcome GC floor",
            OUTCOME_GC_FLOOR_MAX_BYTES,
            None,
        )
        .await?;
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
        let payload =
            Self::encode_control_record("outcome GC floor", floor, OUTCOME_GC_FLOOR_MAX_BYTES)?;
        let options = PutOptions {
            mode: expected.clone().map_or(PutMode::Create, PutMode::Update),
            ..PutOptions::default()
        };
        let result = self
            .store
            .put_opts(&path, PutPayload::from(payload.clone()), options)
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. }
                | object_store::Error::NotFound { .. },
            ) => Ok(false),
            Err(object_store::Error::NotImplemented { .. })
                if expected.is_some()
                    && self.update_mode == DecisionStoreUpdateMode::LocalSingleWriter =>
            {
                // LocalFileSystem atomically replaces a file but cannot condition that replace on
                // an ETag. The runtime topology guarantees one process writer for this namespace;
                // serialize its read/compare/replace so concurrent maintenance tasks cannot
                // regress the floor. Shared stores never enter this path.
                let _guard = self
                    .local_metadata_rmw_lock
                    .as_ref()
                    .expect("local decision store has a namespace RMW lock")
                    .lock()
                    .await;
                let Some(current) = self.read_outcome_gc_floor(&floor.deployment_id).await? else {
                    return Ok(false);
                };
                if current.floor.before_epoch >= floor.before_epoch {
                    return Ok(true);
                }
                if Some(&current.update_version) != expected.as_ref() {
                    return Ok(false);
                }
                let overwrite = PutOptions {
                    mode: PutMode::Overwrite,
                    ..PutOptions::default()
                };
                match self
                    .store
                    .put_opts(&path, PutPayload::from(payload), overwrite)
                    .await
                {
                    Ok(_) => Ok(true),
                    Err(error) => match self.read_outcome_gc_floor(&floor.deployment_id).await? {
                        Some(current) if current.floor.before_epoch >= floor.before_epoch => {
                            Ok(true)
                        }
                        _ => Err(DecisionError::Io(error.to_string())),
                    },
                }
            }
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
    #[allow(clippy::too_many_arguments)]
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
    /// a checkpoint. Recovery must select a live commit from [`Self::outcomes`].
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
    #[allow(clippy::too_many_lines)]
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

        let work = futures::stream::iter(entries.iter().cloned())
            .map(|entry| async move {
                let Some((epoch, checkpoint_id, digest)) =
                    Self::recovery_capsule_coordinates_from_path(entry.location.as_ref())
                else {
                    return match self
                        .quarantine_recovery_capsule_object(&entry.location)
                        .await
                    {
                        Ok(()) => RecoveryCapsuleObjectWork::Quarantined,
                        Err(error) => {
                            tracing::warn!(path = %entry.location, %error, "recovery capsule quarantine failed; retrying on the next scan pass");
                            RecoveryCapsuleObjectWork::Failed
                        }
                    };
                };
                if epoch >= before_epoch {
                    return RecoveryCapsuleObjectWork::Retained;
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
                        Ok(()) => RecoveryCapsuleObjectWork::Quarantined,
                        Err(error) => {
                            tracing::warn!(path = %entry.location, %error, "recovery capsule quarantine failed; retrying on the next scan pass");
                            RecoveryCapsuleObjectWork::Failed
                        }
                    };
                }
                if known_live_digests.contains(&reference.sha256) {
                    return RecoveryCapsuleObjectWork::Retained;
                }
                match self.load_recovery_capsule(&reference).await {
                    Ok(_) => match self.store.delete(&entry.location).await {
                        Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                            RecoveryCapsuleObjectWork::Deleted
                        }
                        Err(error) => {
                            tracing::warn!(epoch, checkpoint_id, %error, "recovery capsule delete failed; retrying on the next scan pass");
                            RecoveryCapsuleObjectWork::Failed
                        }
                    },
                    Err(DecisionError::Conflict(error)) => {
                        if matches!(
                            self.store.head(&entry.location).await,
                            Err(object_store::Error::NotFound { .. })
                        ) {
                            return RecoveryCapsuleObjectWork::Deleted;
                        }
                        match self
                            .quarantine_recovery_capsule_object(&entry.location)
                            .await
                        {
                            Ok(()) => {
                                tracing::warn!(path = %entry.location, %error, "corrupt recovery capsule quarantined");
                                RecoveryCapsuleObjectWork::Quarantined
                            }
                            Err(quarantine_error) => {
                                tracing::warn!(path = %entry.location, %error, %quarantine_error, "corrupt recovery capsule quarantine failed; retrying on the next scan pass");
                                RecoveryCapsuleObjectWork::Failed
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(path = %entry.location, %error, "recovery capsule read failed; retrying on the next scan pass");
                        RecoveryCapsuleObjectWork::Failed
                    }
                }
            })
            .buffer_unordered(4)
            .collect::<Vec<_>>()
            .await;

        let deleted = work
            .iter()
            .filter(|result| matches!(result, RecoveryCapsuleObjectWork::Deleted))
            .count();
        let quarantined = work
            .iter()
            .filter(|result| matches!(result, RecoveryCapsuleObjectWork::Quarantined))
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
    use futures::TryStreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    fn store_in(dir: &std::path::Path) -> CheckpointDecisionStore {
        CheckpointDecisionStore::local_filesystem(dir).unwrap()
    }

    async fn record_local_commits(store: &CheckpointDecisionStore, last_epoch: u64) {
        for epoch in 1..=last_epoch {
            store
                .record_outcome(
                    epoch,
                    epoch,
                    CheckpointScope::Local,
                    None,
                    None,
                    CheckpointVerdict::Commit,
                    None,
                )
                .await
                .unwrap();
        }
    }

    fn highest_commit_epoch(outcomes: &[CheckpointOutcome]) -> Option<u64> {
        outcomes
            .iter()
            .rev()
            .find(|outcome| outcome.is_commit())
            .map(|outcome| outcome.epoch)
    }

    struct DeferredPutStore {
        inner: Arc<dyn ObjectStore>,
        target: std::sync::Mutex<Option<OsPath>>,
        intercepted: std::sync::atomic::AtomicBool,
        apply_before_error: std::sync::atomic::AtomicBool,
        pending: std::sync::Mutex<Option<(OsPath, PutPayload, PutOptions)>>,
        reverse_lists: bool,
        reject_updates: std::sync::atomic::AtomicBool,
        blocked_overwrite: std::sync::Mutex<Option<OsPath>>,
        overwrite_entered: tokio::sync::Semaphore,
        overwrite_release: tokio::sync::Semaphore,
        strip_cas_tokens: std::sync::atomic::AtomicBool,
        forged_get: std::sync::Mutex<Option<(OsPath, ForgedGet)>>,
        requested_range_end: std::sync::atomic::AtomicU64,
        get_count: std::sync::atomic::AtomicU64,
    }

    #[derive(Clone, Copy)]
    enum ForgedGet {
        InconsistentRange,
        OversizedStream,
    }

    impl DeferredPutStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                target: std::sync::Mutex::new(None),
                intercepted: std::sync::atomic::AtomicBool::new(false),
                apply_before_error: std::sync::atomic::AtomicBool::new(false),
                pending: std::sync::Mutex::new(None),
                reverse_lists: false,
                reject_updates: std::sync::atomic::AtomicBool::new(false),
                blocked_overwrite: std::sync::Mutex::new(None),
                overwrite_entered: tokio::sync::Semaphore::new(0),
                overwrite_release: tokio::sync::Semaphore::new(0),
                strip_cas_tokens: std::sync::atomic::AtomicBool::new(false),
                forged_get: std::sync::Mutex::new(None),
                requested_range_end: std::sync::atomic::AtomicU64::new(0),
                get_count: std::sync::atomic::AtomicU64::new(0),
            }
        }

        #[cfg(feature = "cluster")]
        fn with_reversed_lists(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                target: std::sync::Mutex::new(None),
                intercepted: std::sync::atomic::AtomicBool::new(false),
                apply_before_error: std::sync::atomic::AtomicBool::new(false),
                pending: std::sync::Mutex::new(None),
                reverse_lists: true,
                reject_updates: std::sync::atomic::AtomicBool::new(false),
                blocked_overwrite: std::sync::Mutex::new(None),
                overwrite_entered: tokio::sync::Semaphore::new(0),
                overwrite_release: tokio::sync::Semaphore::new(0),
                strip_cas_tokens: std::sync::atomic::AtomicBool::new(false),
                forged_get: std::sync::Mutex::new(None),
                requested_range_end: std::sync::atomic::AtomicU64::new(0),
                get_count: std::sync::atomic::AtomicU64::new(0),
            }
        }

        fn intercept(&self, target: OsPath) {
            *self.target.lock().unwrap() = Some(target);
            self.intercepted
                .store(false, std::sync::atomic::Ordering::Release);
            self.apply_before_error
                .store(false, std::sync::atomic::Ordering::Release);
            *self.pending.lock().unwrap() = None;
        }

        fn intercept_after_apply(&self, target: OsPath) {
            self.intercept(target);
            self.apply_before_error
                .store(true, std::sync::atomic::Ordering::Release);
        }

        async fn apply_pending(&self) -> object_store::Result<object_store::PutResult> {
            let (location, payload, options) =
                self.pending.lock().unwrap().take().expect("deferred put");
            self.inner.put_opts(&location, payload, options).await
        }

        fn reject_conditional_updates(&self) {
            self.reject_updates
                .store(true, std::sync::atomic::Ordering::Release);
        }

        fn block_next_overwrite(&self, target: OsPath) {
            *self.blocked_overwrite.lock().unwrap() = Some(target);
        }

        async fn wait_for_blocked_overwrite(&self) {
            self.overwrite_entered
                .acquire()
                .await
                .expect("overwrite gate is open")
                .forget();
        }

        fn release_blocked_overwrite(&self) {
            self.overwrite_release.add_permits(1);
        }

        fn strip_cas_tokens(&self) {
            self.strip_cas_tokens
                .store(true, std::sync::atomic::Ordering::Release);
        }

        fn forge_get(&self, target: OsPath, forged: ForgedGet) {
            *self.forged_get.lock().unwrap() = Some((target, forged));
            self.requested_range_end
                .store(0, std::sync::atomic::Ordering::Release);
        }

        fn requested_range_end(&self) -> u64 {
            self.requested_range_end
                .load(std::sync::atomic::Ordering::Acquire)
        }

        fn reset_get_count(&self) {
            self.get_count
                .store(0, std::sync::atomic::Ordering::Release);
        }

        fn get_count(&self) -> u64 {
            self.get_count.load(std::sync::atomic::Ordering::Acquire)
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
            if self
                .reject_updates
                .load(std::sync::atomic::Ordering::Acquire)
                && matches!(&options.mode, PutMode::Update(_))
            {
                return Err(object_store::Error::NotImplemented {
                    operation: "put_opts with Update".into(),
                    implementer: "DeferredPutStore".into(),
                });
            }
            let block_overwrite = matches!(&options.mode, PutMode::Overwrite)
                && self.blocked_overwrite.lock().unwrap().as_ref() == Some(location);
            if block_overwrite {
                *self.blocked_overwrite.lock().unwrap() = None;
                self.overwrite_entered.add_permits(1);
                self.overwrite_release
                    .acquire()
                    .await
                    .expect("overwrite release gate is open")
                    .forget();
            }
            let target = self.target.lock().unwrap().clone();
            if target.as_ref() == Some(location)
                && !self
                    .intercepted
                    .swap(true, std::sync::atomic::Ordering::AcqRel)
            {
                if self
                    .apply_before_error
                    .load(std::sync::atomic::Ordering::Acquire)
                {
                    self.inner.put_opts(location, payload, options).await?;
                    return Err(object_store::Error::Generic {
                        store: "DeferredPutStore",
                        source: Box::new(std::io::Error::other(
                            "injected response loss after remote visibility",
                        )),
                    });
                }
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
            self.get_count
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            if let Some(GetRange::Bounded(range)) = options.range.as_ref() {
                self.requested_range_end
                    .store(range.end, std::sync::atomic::Ordering::Release);
            }
            let mut result = self.inner.get_opts(location, options).await?;
            if self
                .strip_cas_tokens
                .load(std::sync::atomic::Ordering::Acquire)
            {
                result.meta.e_tag = None;
                result.meta.version = None;
            }
            let forged = self
                .forged_get
                .lock()
                .unwrap()
                .as_ref()
                .filter(|(target, _)| target == location)
                .map(|(_, forged)| *forged);
            match forged {
                Some(ForgedGet::InconsistentRange) => {
                    result.meta.size = 1;
                    result.range = 0..2;
                }
                Some(ForgedGet::OversizedStream) => {
                    result.meta.size = 1;
                    result.range = 0..1;
                    result.payload = object_store::GetResultPayload::Stream(
                        futures::stream::iter([
                            Ok::<Bytes, object_store::Error>(Bytes::from_static(b"x")),
                            Ok::<Bytes, object_store::Error>(Bytes::from_static(b"y")),
                        ])
                        .boxed(),
                    );
                }
                None => {}
            }
            Ok(result)
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

    fn test_sink_open_witness(
        deployment_id: String,
        checkpoint_id: u64,
        create_token: u128,
    ) -> CheckpointSinkOpenWitness {
        CheckpointSinkOpenWitness {
            version: CHECKPOINT_SINK_OPEN_WITNESS_VERSION,
            deployment_id,
            pipeline_identity: PipelineIdentity::empty(),
            participant_id: 0,
            attempt: CheckpointAttempt::canonical(checkpoint_id),
            committable_sinks: vec!["sink-a".into(), "sink-b".into()],
            create_token: uuid::Uuid::from_u128(create_token).to_string(),
        }
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
            source_assignment_versions: std::collections::BTreeMap::new(),
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
        for malformed in [
            "checkpoint-outcomes/epoch=0/outcome",
            "checkpoint-outcomes/epoch=05/outcome",
            "checkpoint-outcomes/epoch=+5/outcome",
            "checkpoint-outcomes/epoch=5//outcome",
        ] {
            assert_eq!(
                CheckpointDecisionStore::outcome_epoch_segment(malformed),
                None,
                "accepted noncanonical outcome path {malformed}"
            );
        }
    }

    async fn put_oversized_control_record(
        store: &Arc<dyn ObjectStore>,
        path: &OsPath,
        maximum: u64,
    ) {
        let size = usize::try_from(maximum + 1).unwrap();
        store
            .put(path, PutPayload::from(Bytes::from(vec![b'x'; size])))
            .await
            .unwrap();
    }

    fn assert_oversized_control_record(error: &DecisionError, record: &str, maximum: u64) {
        let message = error.to_string();
        assert!(message.contains(record), "{message}");
        assert!(
            message.contains(&format!("maximum is {maximum}")),
            "{message}"
        );
    }

    #[tokio::test]
    async fn control_record_reads_reject_oversized_objects() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));

        put_oversized_control_record(
            &object_store,
            &CheckpointDecisionStore::deployment_identity_path(),
            DEPLOYMENT_IDENTITY_MAX_BYTES,
        )
        .await;
        let error = store.read_deployment_identity().await.unwrap_err();
        assert_oversized_control_record(
            &error,
            "deployment identity",
            DEPLOYMENT_IDENTITY_MAX_BYTES,
        );

        put_oversized_control_record(
            &object_store,
            &CheckpointDecisionStore::sink_open_witness_path(),
            CHECKPOINT_SINK_OPEN_WITNESS_MAX_BYTES,
        )
        .await;
        let error = store.read_sink_open_witness_record().await.unwrap_err();
        assert_oversized_control_record(
            &error,
            "sink-open witness",
            CHECKPOINT_SINK_OPEN_WITNESS_MAX_BYTES,
        );

        let outcome_path = CheckpointDecisionStore::outcome_path(1);
        put_oversized_control_record(&object_store, &outcome_path, CHECKPOINT_OUTCOME_MAX_BYTES)
            .await;
        let error = store
            .read_outcome_record(&outcome_path, 1)
            .await
            .unwrap_err();
        assert_oversized_control_record(&error, "checkpoint outcome", CHECKPOINT_OUTCOME_MAX_BYTES);

        let deployment_id = uuid::Uuid::from_u128(1).to_string();
        let floor_path = CheckpointDecisionStore::outcome_gc_floor_path(&deployment_id);
        put_oversized_control_record(&object_store, &floor_path, OUTCOME_GC_FLOOR_MAX_BYTES).await;
        let error = store
            .read_outcome_gc_floor(&deployment_id)
            .await
            .unwrap_err();
        assert_oversized_control_record(&error, "outcome GC floor", OUTCOME_GC_FLOOR_MAX_BYTES);

        let reference = RecoveryCapsuleRef {
            epoch: 1,
            checkpoint_id: 1,
            sha256: digest(1),
            len: 1,
        };
        let capsule_path = CheckpointDecisionStore::recovery_capsule_path(&reference);
        let capsule_maximum = u64::try_from(crate::checkpoint::MAX_RECOVERY_CAPSULE_BYTES).unwrap();
        put_oversized_control_record(&object_store, &capsule_path, capsule_maximum).await;
        let error = store.load_recovery_capsule(&reference).await.unwrap_err();
        assert_oversized_control_record(&error, "recovery capsule", capsule_maximum);
    }

    #[tokio::test]
    async fn control_record_reads_bound_the_requested_range_and_streamed_body() {
        for (forged, expected_error) in [
            (ForgedGet::InconsistentRange, "inconsistent with advertised"),
            (ForgedGet::OversizedStream, "exceeded its advertised"),
        ] {
            let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let path = CheckpointDecisionStore::deployment_identity_path();
            inner
                .put(&path, PutPayload::from_static(b"x"))
                .await
                .unwrap();
            let fault = Arc::new(DeferredPutStore::new(inner));
            fault.forge_get(path, forged);
            let object_store: Arc<dyn ObjectStore> = fault.clone();
            let store = CheckpointDecisionStore::new(object_store);

            let error = store.read_deployment_identity().await.unwrap_err();
            assert!(error.to_string().contains(expected_error), "{error}");
            assert_eq!(
                fault.requested_range_end(),
                DEPLOYMENT_IDENTITY_MAX_BYTES + 1,
                "control metadata must never request an unbounded body"
            );
        }
    }

    #[tokio::test]
    async fn native_cas_metadata_requires_an_update_authority() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let deployment_id = uuid::Uuid::from_u128(1).to_string();
        let floor = OutcomeGcFloor {
            version: OUTCOME_GC_FLOOR_VERSION,
            deployment_id: deployment_id.clone(),
            before_epoch: 1,
            terminal_anchor: None,
            committed_anchor: None,
        };
        inner
            .put(
                &CheckpointDecisionStore::outcome_gc_floor_path(&deployment_id),
                PutPayload::from(Bytes::from(serde_json::to_vec(&floor).unwrap())),
            )
            .await
            .unwrap();
        let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
        fault.strip_cas_tokens();
        let object_store: Arc<dyn ObjectStore> = fault.clone();
        let store = CheckpointDecisionStore::new(object_store);

        let error = store
            .read_outcome_gc_floor(&deployment_id)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("outcome GC floor"), "{error}");
        assert!(error.to_string().contains("neither an ETag"), "{error}");

        #[cfg(feature = "cluster")]
        {
            let cursor = RecoveryCapsuleGcCursor {
                version: RECOVERY_CAPSULE_GC_CURSOR_VERSION,
                deployment_id: deployment_id.clone(),
                offset: None,
            };
            inner
                .put(
                    &CheckpointDecisionStore::recovery_capsule_gc_cursor_path(&deployment_id),
                    PutPayload::from(Bytes::from(serde_json::to_vec(&cursor).unwrap())),
                )
                .await
                .unwrap();
            let error = store
                .read_recovery_capsule_gc_cursor(&deployment_id)
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("recovery capsule GC cursor"),
                "{error}"
            );
            assert!(error.to_string().contains("neither an ETag"), "{error}");
        }
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn recovery_capsule_cursor_read_rejects_oversized_object() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let deployment_id = uuid::Uuid::from_u128(1).to_string();
        put_oversized_control_record(
            &object_store,
            &CheckpointDecisionStore::recovery_capsule_gc_cursor_path(&deployment_id),
            RECOVERY_CAPSULE_GC_CURSOR_MAX_BYTES,
        )
        .await;

        let error = store
            .read_recovery_capsule_gc_cursor(&deployment_id)
            .await
            .unwrap_err();
        assert_oversized_control_record(
            &error,
            "recovery capsule GC cursor",
            RECOVERY_CAPSULE_GC_CURSOR_MAX_BYTES,
        );
    }

    #[tokio::test]
    async fn recovery_capsule_create_is_idempotent_and_load_verifies_reference() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fault = Arc::new(DeferredPutStore::new(inner));
        let object_store: Arc<dyn ObjectStore> = fault.clone();
        let store = CheckpointDecisionStore::new(object_store);
        let fence = assignment_fence(12, &[2, 7]);
        let capsule = test_capsule(&store, 4, 4, &fence).await;

        fault.reset_get_count();
        let reference = store.create_recovery_capsule(&capsule).await.unwrap();
        assert_eq!(
            fault.get_count(),
            0,
            "a confirmed immutable create must not read back its body"
        );

        fault.reset_get_count();
        assert_eq!(
            store.create_recovery_capsule(&capsule).await.unwrap(),
            reference
        );
        assert!(
            fault.get_count() > 0,
            "an existing immutable body must be reconciled after create conflict"
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

    #[tokio::test]
    async fn recovery_capsule_create_reconciles_a_lost_success_response() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
        let object_store: Arc<dyn ObjectStore> = fault.clone();
        let store = CheckpointDecisionStore::new(object_store);
        let fence = assignment_fence(12, &[2, 7]);
        let capsule = test_capsule(&store, 4, 4, &fence).await;
        let (_, expected) = capsule.encode_and_reference().unwrap();
        fault.intercept_after_apply(CheckpointDecisionStore::recovery_capsule_path(&expected));

        fault.reset_get_count();
        let observed = store.create_recovery_capsule(&capsule).await.unwrap();

        assert_eq!(observed, expected);
        assert!(
            fault.get_count() > 0,
            "an ambiguous create response must reconcile the remote body"
        );
        inner
            .head(&CheckpointDecisionStore::recovery_capsule_path(&observed))
            .await
            .expect("the lost response followed a remotely visible create");
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cyclic_capsule_cleanup_finds_a_create_visible_after_client_failure() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
        let object_store: Arc<dyn ObjectStore> = fault.clone();
        let store = CheckpointDecisionStore::new(object_store);
        let fence = assignment_fence(12, &[2, 7]);
        let capsule = test_capsule(&store, 1, 1, &fence).await;
        let (_, reference) = capsule.encode_and_reference().unwrap();
        let path = CheckpointDecisionStore::recovery_capsule_path(&reference);
        fault.intercept(path.clone());

        fault.reset_get_count();
        let error = store
            .create_recovery_capsule(&capsule)
            .await
            .expect_err("the client must observe the injected ambiguous failure");
        assert!(matches!(error, DecisionError::Io(_)));
        assert!(
            fault.get_count() > 0,
            "an ambiguous failed create must reconcile before returning"
        );
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
        let oldest = create_capsule_ref(&store, 1, 1, &fence).await;
        let oldest_path = CheckpointDecisionStore::recovery_capsule_path(&oldest);
        let mut newest_path = None;
        for epoch in 2..=70u64 {
            let reference = RecoveryCapsuleRef {
                epoch,
                checkpoint_id: epoch,
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
        let capsule = test_capsule(&store, 4, 4, &fence).await;
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
        let reference = create_capsule_ref(&store, 4, 4, &fence).await;

        let missing = store
            .canonical_outcome(
                4,
                4,
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
                4,
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
                4,
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
        let reference = create_capsule_ref(&store, 4, 4, &fence).await;

        let error = store
            .canonical_outcome(
                5,
                5,
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
                4,
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
                4,
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
                7,
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
                .record_outcome(7, 7, CheckpointScope::Local, None, None, commit, None)
                .await
                .unwrap(),
            RecordOutcomeResult::Unchanged(winner.clone())
        );
        assert_eq!(
            store
                .record_outcome(
                    7,
                    7,
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
                2,
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
                2,
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
            Err(object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. })
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
                2,
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
                2,
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
                    8,
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
                    8,
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
                4,
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
                5,
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
        let outcomes = restarted.outcomes().await.unwrap();
        assert_eq!(highest_commit_epoch(&outcomes), Some(4));
    }

    #[tokio::test]
    async fn abort_after_commit_advances_terminal_without_replacing_live_commit() {
        let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        store
            .record_outcome(
                4,
                4,
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
                5,
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
        let outcomes = store.outcomes().await.unwrap();
        assert_eq!(highest_commit_epoch(&outcomes), Some(4));
    }

    #[tokio::test]
    async fn outcome_floor_rejects_late_create_and_survives_restart() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        for (epoch, checkpoint_id, verdict) in [
            (1, 1, CheckpointVerdict::Commit),
            (2, 2, CheckpointVerdict::Abort),
            (5, 5, CheckpointVerdict::Commit),
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
                3,
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
            vec![(5, 5)]
        );
        let continuity = restarted.audited_outcomes().await.unwrap();
        assert_eq!(
            continuity
                .iter()
                .map(|outcome| (outcome.epoch, outcome.checkpoint_id))
                .collect::<Vec<_>>(),
            vec![(1, 1), (2, 2), (5, 5)]
        );
        assert!(matches!(&continuity[1].verdict, CheckpointVerdict::Abort));
        assert_eq!(highest_commit_epoch(&live), Some(5));
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
                    epoch,
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
                committed_checkpoint_id: Some(LAST_EPOCH - 1),
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
    async fn local_filesystem_outcome_floor_advances_repeatedly_and_survives_restart() {
        const LAST_EPOCH: u64 = 24;

        let dir = tempdir().unwrap();
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let store = CheckpointDecisionStore::local_single_writer(Arc::clone(&object_store));
        let deployment_id = store.load_or_create_deployment_id().await.unwrap();
        record_local_commits(&store, LAST_EPOCH).await;

        for before in 2..=LAST_EPOCH {
            assert_eq!(store.prune_outcomes_before(before).await.unwrap(), before);
        }
        assert_eq!(store.outcome_gc_floor_horizon().await.unwrap(), LAST_EPOCH);
        assert_eq!(
            store
                .outcomes()
                .await
                .unwrap()
                .into_iter()
                .map(|outcome| outcome.epoch)
                .collect::<Vec<_>>(),
            vec![LAST_EPOCH]
        );

        let mut floors = object_store.list(Some(&OsPath::from("checkpoint-outcome-gc/")));
        let mut floor_paths = Vec::new();
        while let Some(entry) = floors.next().await {
            floor_paths.push(entry.unwrap().location);
        }
        assert_eq!(
            floor_paths,
            vec![CheckpointDecisionStore::outcome_gc_floor_path(
                &deployment_id
            )]
        );

        drop(store);
        let restarted = CheckpointDecisionStore::local_single_writer(object_store);
        assert_eq!(
            restarted.outcome_gc_floor_horizon().await.unwrap(),
            LAST_EPOCH
        );
        let outcomes = restarted.outcomes().await.unwrap();
        assert_eq!(highest_commit_epoch(&outcomes), Some(LAST_EPOCH));
    }

    #[test]
    fn independently_opened_local_filesystem_stores_share_the_namespace_rmw_lock() {
        let dir = tempdir().unwrap();
        let first = CheckpointDecisionStore::local_filesystem(dir.path()).unwrap();
        let second = CheckpointDecisionStore::local_filesystem(dir.path()).unwrap();

        assert!(Arc::ptr_eq(
            first.local_metadata_rmw_lock.as_ref().unwrap(),
            second.local_metadata_rmw_lock.as_ref().unwrap()
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn local_filesystem_concurrent_floor_advancement_is_monotonic() {
        const LAST_EPOCH: u64 = 16;

        let dir = tempdir().unwrap();
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let store = Arc::new(CheckpointDecisionStore::local_single_writer(object_store));
        record_local_commits(store.as_ref(), LAST_EPOCH).await;

        let mut tasks = tokio::task::JoinSet::new();
        for before in 2..=LAST_EPOCH {
            let store = Arc::clone(&store);
            tasks.spawn(async move { (before, store.prune_outcomes_before(before).await) });
        }
        while let Some(result) = tasks.join_next().await {
            let (requested, horizon) = result.unwrap();
            assert!(
                horizon.unwrap() >= requested,
                "a serialized local winner may supersede but never regress a request"
            );
        }
        assert_eq!(store.outcome_gc_floor_horizon().await.unwrap(), LAST_EPOCH);
        assert_eq!(
            store
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
    async fn local_floor_rmw_is_ordered_across_store_instances() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fault = Arc::new(DeferredPutStore::new(inner));
        let object_store: Arc<dyn ObjectStore> = fault.clone();
        let first_store = Arc::new(CheckpointDecisionStore::local_single_writer(Arc::clone(
            &object_store,
        )));
        let second_store = Arc::new(CheckpointDecisionStore::local_single_writer(object_store));
        assert!(Arc::ptr_eq(
            first_store.local_metadata_rmw_lock.as_ref().unwrap(),
            second_store.local_metadata_rmw_lock.as_ref().unwrap()
        ));
        record_local_commits(first_store.as_ref(), 5).await;
        assert_eq!(first_store.prune_outcomes_before(2).await.unwrap(), 2);

        fault.reject_conditional_updates();
        let deployment_id = first_store.load_or_create_deployment_id().await.unwrap();
        fault.block_next_overwrite(CheckpointDecisionStore::outcome_gc_floor_path(
            &deployment_id,
        ));
        let advancing_store = Arc::clone(&first_store);
        let advance = tokio::spawn(async move { advancing_store.prune_outcomes_before(3).await });
        fault.wait_for_blocked_overwrite().await;
        assert!(
            second_store
                .local_metadata_rmw_lock
                .as_ref()
                .unwrap()
                .try_lock()
                .is_err(),
            "a second store instance entered a local floor RMW transition"
        );
        fault.release_blocked_overwrite();
        assert!(advance.await.unwrap().unwrap() >= 3);

        assert_eq!(second_store.prune_outcomes_before(4).await.unwrap(), 4);
        assert_eq!(first_store.outcome_gc_floor_horizon().await.unwrap(), 4);
    }

    #[tokio::test]
    async fn shared_local_filesystem_requires_native_conditional_floor_updates() {
        let dir = tempdir().unwrap();
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let store = CheckpointDecisionStore::new(object_store);
        record_local_commits(&store, 4).await;

        assert_eq!(store.prune_outcomes_before(2).await.unwrap(), 2);
        let error = store.prune_outcomes_before(3).await.unwrap_err();
        assert!(error.to_string().contains("PutMode::Update"), "{error}");
        assert_eq!(store.outcome_gc_floor_horizon().await.unwrap(), 2);
        assert_eq!(
            store
                .outcomes()
                .await
                .unwrap()
                .into_iter()
                .map(|outcome| outcome.epoch)
                .collect::<Vec<_>>(),
            vec![2, 3, 4],
            "a failed shared update must not delete outcomes beyond the durable floor"
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
                    epoch,
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
            (1, 1, CheckpointVerdict::Commit),
            (2, 2, CheckpointVerdict::Abort),
            (3, 3, CheckpointVerdict::Commit),
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
                committed_checkpoint_id: Some(1),
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
            vec![(3, 3)]
        );
    }

    #[tokio::test]
    async fn outcome_inventory_rejects_noncanonical_attempt_identity() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let forged = CheckpointOutcome {
            version: CHECKPOINT_OUTCOME_VERSION,
            scope: CheckpointScope::Local,
            epoch: 9,
            checkpoint_id: 79,
            deployment_id: store.load_or_create_deployment_id().await.unwrap(),
            assignment_fence: None,
            leader_proof: None,
            recovery_capsule: None,
            verdict: CheckpointVerdict::Abort,
        };
        object_store
            .put(
                &CheckpointDecisionStore::outcome_path(9),
                PutPayload::from(Bytes::from(serde_json::to_vec(&forged).unwrap())),
            )
            .await
            .unwrap();

        let error = store.outcomes().await.unwrap_err();
        assert!(matches!(error, DecisionError::Conflict(_)));
        assert!(error.to_string().contains("non-canonical checkpoint ID 79"));
    }

    #[tokio::test]
    async fn outcome_prune_cannot_remove_last_live_commit() {
        let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        store
            .record_outcome(
                4,
                4,
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
                5,
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
        let outcomes = store.outcomes().await.unwrap();
        assert_eq!(highest_commit_epoch(&outcomes), Some(4));
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
    async fn sink_open_witness_roundtrips_across_restart_and_clears_exactly() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let witness = store
            .create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(7),
                vec!["sink-a".into(), "sink-b".into()],
            )
            .await
            .unwrap();
        assert_eq!(
            store.sink_open_witness().await.unwrap(),
            Some(witness.clone())
        );
        drop(store);

        let restarted = CheckpointDecisionStore::new(object_store);
        assert_eq!(
            restarted.sink_open_witness().await.unwrap(),
            Some(witness.clone())
        );
        restarted.clear_sink_open_witness(&witness).await.unwrap();
        assert_eq!(restarted.sink_open_witness().await.unwrap(), None);
        let tombstone = restarted
            .read_sink_open_witness_record()
            .await
            .unwrap()
            .expect("closed slot remains durable");
        assert!(matches!(
            tombstone.slot.state,
            CheckpointSinkOpenWitnessSlotState::Closed {
                witness: ref closed,
                ..
            } if closed == &witness
        ));
        restarted.clear_sink_open_witness(&witness).await.unwrap();
    }

    #[tokio::test]
    async fn sink_open_witness_rejects_malformed_body_and_input() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let deployment_id = store.load_or_create_deployment_id().await.unwrap();
        let witness = test_sink_open_witness(deployment_id, 8, 8);
        let mut body = serde_json::to_value(CheckpointSinkOpenWitnessSlot::open(witness)).unwrap();
        body.as_object_mut()
            .unwrap()
            .insert("unexpected".into(), serde_json::Value::Bool(true));
        object_store
            .put(
                &CheckpointDecisionStore::sink_open_witness_path(),
                PutPayload::from(Bytes::from(serde_json::to_vec(&body).unwrap())),
            )
            .await
            .unwrap();

        let error = store.sink_open_witness().await.unwrap_err();
        assert!(error.to_string().contains("unknown field"), "{error}");

        let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        let error = store
            .create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(9),
                vec!["sink-b".into(), "sink-a".into()],
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("strictly sorted"), "{error}");
    }

    #[tokio::test]
    async fn sink_open_witness_rejects_oversized_body_before_create() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let error = store
            .create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(10),
                vec!["x".repeat(CHECKPOINT_SINK_OPEN_WITNESS_MAX_BYTES as usize)],
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("maximum is"), "{error}");
        assert!(object_store
            .head(&CheckpointDecisionStore::sink_open_witness_path())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn sink_open_witness_rejects_foreign_deployment() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let current = store.load_or_create_deployment_id().await.unwrap();
        let foreign = test_sink_open_witness(uuid::Uuid::from_u128(0x1234).to_string(), 11, 0x5678);
        let foreign_slot = CheckpointSinkOpenWitnessSlot::closed(foreign.clone());
        object_store
            .put(
                &CheckpointDecisionStore::sink_open_witness_path(),
                PutPayload::from(Bytes::from(serde_json::to_vec(&foreign_slot).unwrap())),
            )
            .await
            .unwrap();

        let error = store.sink_open_witness().await.unwrap_err();
        assert!(
            error.to_string().contains(&foreign.deployment_id),
            "{error}"
        );
        assert!(error.to_string().contains(&current), "{error}");
    }

    #[tokio::test]
    async fn sink_open_witness_singleton_linearizes_concurrent_attempts() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let second = CheckpointDecisionStore::new(object_store);
        let (left, right) = tokio::join!(
            first.create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(12),
                vec!["sink-a".into()],
            ),
            second.create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(13),
                vec!["sink-a".into()],
            )
        );
        assert_ne!(left.is_ok(), right.is_ok());
        let winner = left.or(right).unwrap();
        assert_eq!(first.sink_open_witness().await.unwrap(), Some(winner));
    }

    #[tokio::test]
    async fn sink_open_witness_does_not_adopt_another_proposals_token() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let second = CheckpointDecisionStore::new(object_store);
        let winner = first
            .create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(14),
                vec!["sink-a".into()],
            )
            .await
            .unwrap();

        let error = second
            .create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(14),
                vec!["sink-a".into()],
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("conflicting checkpoint 14"),
            "{error}"
        );
        assert_eq!(first.sink_open_witness().await.unwrap(), Some(winner));
    }

    #[tokio::test]
    async fn sink_open_witness_reconciles_lost_create_response_by_token() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
        let object_store: Arc<dyn ObjectStore> = fault.clone();
        let store = CheckpointDecisionStore::new(object_store);
        store.load_or_create_deployment_id().await.unwrap();
        let attempt = CheckpointAttempt::canonical(14);
        fault.intercept_after_apply(CheckpointDecisionStore::sink_open_witness_path());

        let witness = store
            .create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                attempt,
                vec!["sink-a".into(), "sink-b".into()],
            )
            .await
            .unwrap();
        assert_eq!(witness.attempt, attempt);
        assert_eq!(store.sink_open_witness().await.unwrap(), Some(witness));
    }

    #[tokio::test]
    async fn sink_open_witness_reconciles_lost_close_response_by_tombstone() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let attempt = CheckpointAttempt::canonical(15);
        let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
        let object_store: Arc<dyn ObjectStore> = fault.clone();
        let store = CheckpointDecisionStore::new(object_store);
        let witness = store
            .create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                attempt,
                vec!["sink-a".into(), "sink-b".into()],
            )
            .await
            .unwrap();

        fault.intercept_after_apply(CheckpointDecisionStore::sink_open_witness_path());
        store.clear_sink_open_witness(&witness).await.unwrap();
        assert_eq!(store.sink_open_witness().await.unwrap(), None);
        assert!(inner
            .head(&CheckpointDecisionStore::sink_open_witness_path())
            .await
            .is_ok());
        let tombstone = store
            .read_sink_open_witness_record()
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            tombstone.slot.state,
            CheckpointSinkOpenWitnessSlotState::Closed {
                witness: ref closed,
                ..
            } if closed == &witness
        ));
    }

    #[tokio::test]
    async fn stale_sink_open_witness_close_cannot_erase_a_successor() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
        let object_store: Arc<dyn ObjectStore> = fault.clone();
        let stale = CheckpointDecisionStore::new(object_store);
        let first = stale
            .create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(16),
                vec!["sink-a".into()],
            )
            .await
            .unwrap();

        fault.intercept(CheckpointDecisionStore::sink_open_witness_path());
        let error = stale.clear_sink_open_witness(&first).await.unwrap_err();
        assert!(matches!(&error, DecisionError::Io(_)), "{error}");

        let current = CheckpointDecisionStore::new(Arc::clone(&inner));
        current.clear_sink_open_witness(&first).await.unwrap();
        let successor = current
            .create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(17),
                vec!["sink-a".into()],
            )
            .await
            .unwrap();

        assert!(
            fault.apply_pending().await.is_err(),
            "the stale close must fail its old object-version precondition"
        );
        assert_eq!(current.sink_open_witness().await.unwrap(), Some(successor));
    }

    #[tokio::test]
    async fn closed_sink_open_witness_slot_linearizes_successor_open() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let owner = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let first = owner
            .create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(18),
                vec!["sink-a".into()],
            )
            .await
            .unwrap();
        owner.clear_sink_open_witness(&first).await.unwrap();

        let left_store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let right_store = CheckpointDecisionStore::new(object_store);
        let (left, right) = tokio::join!(
            left_store.create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(19),
                vec!["sink-a".into()],
            ),
            right_store.create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(20),
                vec!["sink-a".into()],
            )
        );
        assert_ne!(left.is_ok(), right.is_ok());
        let winner = left.or(right).unwrap();
        assert_eq!(owner.sink_open_witness().await.unwrap(), Some(winner));
    }

    #[tokio::test]
    async fn local_single_writer_reuses_closed_sink_open_witness_slot() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::local_single_writer(object_store);
        let first = store
            .create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(21),
                vec!["sink-a".into()],
            )
            .await
            .unwrap();
        store.clear_sink_open_witness(&first).await.unwrap();
        let error = store
            .create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(21),
                vec!["sink-a".into()],
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("does not advance"), "{error}");
        let successor = store
            .create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(22),
                vec!["sink-a".into()],
            )
            .await
            .unwrap();
        assert_eq!(store.sink_open_witness().await.unwrap(), Some(successor));
    }

    #[tokio::test]
    async fn local_sink_witness_rmw_is_ordered_across_store_instances() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fault = Arc::new(DeferredPutStore::new(inner));
        let object_store: Arc<dyn ObjectStore> = fault.clone();
        let first_store = Arc::new(CheckpointDecisionStore::local_single_writer(Arc::clone(
            &object_store,
        )));
        let second_store = Arc::new(CheckpointDecisionStore::local_single_writer(object_store));
        assert!(Arc::ptr_eq(
            first_store.local_metadata_rmw_lock.as_ref().unwrap(),
            second_store.local_metadata_rmw_lock.as_ref().unwrap()
        ));

        let first = first_store
            .create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(23),
                vec!["sink-a".into()],
            )
            .await
            .unwrap();
        fault.block_next_overwrite(CheckpointDecisionStore::sink_open_witness_path());
        let closing_store = Arc::clone(&first_store);
        let closing_witness = first.clone();
        let close = tokio::spawn(async move {
            closing_store
                .clear_sink_open_witness(&closing_witness)
                .await
        });
        fault.wait_for_blocked_overwrite().await;
        assert!(
            second_store
                .local_metadata_rmw_lock
                .as_ref()
                .unwrap()
                .try_lock()
                .is_err(),
            "a second store instance entered a local witness RMW transition"
        );
        fault.release_blocked_overwrite();
        close.await.unwrap().unwrap();

        second_store.clear_sink_open_witness(&first).await.unwrap();
        let successor = second_store
            .create_sink_open_witness(
                PipelineIdentity::empty(),
                0,
                CheckpointAttempt::canonical(24),
                vec!["sink-a".into()],
            )
            .await
            .unwrap();
        assert_eq!(
            first_store.sink_open_witness().await.unwrap(),
            Some(successor)
        );
    }

    #[tokio::test]
    async fn legacy_deployment_identity_is_rejected() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        object_store
            .put(
                &CheckpointDecisionStore::deployment_identity_path(),
                PutPayload::from_bytes(Bytes::from_static(
                    br#"{"version":1,"id":"018f0000-0000-7000-8000-000000000001","allocator_mode":"native_cas","checkpoint_id":0,"allocation_id":"018f0000-0000-7000-8000-000000000002"}"#,
                )),
            )
            .await
            .unwrap();

        let store = CheckpointDecisionStore::new(object_store);
        let error = store.load_or_create_deployment_id().await.unwrap_err();
        assert!(
            error.to_string().contains("version 1 is unsupported"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn allocator_protocol_cannot_change_inside_a_deployment_namespace() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shared = CheckpointDecisionStore::new(Arc::clone(&object_store));
        assert_eq!(shared.allocate_checkpoint_id().await.unwrap(), 1);
        drop(shared);

        let local = CheckpointDecisionStore::local_single_writer(object_store);
        let error = local.allocate_checkpoint_id().await.unwrap_err();
        assert!(error.to_string().contains("allocator mode"), "{error}");
        assert!(error.to_string().contains("NativeCas"), "{error}");
        assert!(error.to_string().contains("LocalSingleWriter"), "{error}");
    }

    #[tokio::test]
    async fn local_allocator_protocol_cannot_be_reopened_as_shared_cas() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let local = CheckpointDecisionStore::local_single_writer(Arc::clone(&object_store));
        assert_eq!(local.allocate_checkpoint_id().await.unwrap(), 1);
        drop(local);

        let shared = CheckpointDecisionStore::new(object_store);
        let error = shared.allocate_checkpoint_id().await.unwrap_err();
        assert!(error.to_string().contains("allocator mode"), "{error}");
        assert!(error.to_string().contains("LocalSingleWriter"), "{error}");
        assert!(error.to_string().contains("NativeCas"), "{error}");
    }

    #[tokio::test]
    async fn checkpoint_ids_start_at_one_and_increase() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        assert_eq!(s.allocate_checkpoint_id().await.unwrap(), 1);
        assert_eq!(s.allocate_checkpoint_id().await.unwrap(), 2);
        assert_eq!(s.allocate_checkpoint_id_at_least(10).await.unwrap(), 10);
        assert_eq!(s.allocate_checkpoint_id().await.unwrap(), 11);
        drop(s);

        let restarted = store_in(dir.path());
        assert_eq!(
            restarted.allocate_checkpoint_id().await.unwrap(),
            LOCAL_RESERVATION_BLOCK_SIZE + 1,
            "restart must burn the unconsumed tail of the prior durable block"
        );
        assert!(restarted.allocate_checkpoint_id_at_least(0).await.is_err());
    }

    #[tokio::test]
    async fn local_checkpoint_allocation_uses_one_durable_object_per_large_block() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::local_single_writer(Arc::clone(&object_store));

        for expected in 1..=4_096 {
            assert_eq!(store.allocate_checkpoint_id().await.unwrap(), expected);
        }

        let mut locations = object_store
            .list(None)
            .map_ok(|meta| meta.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        locations.sort_unstable();
        assert_eq!(
            locations,
            vec![
                "checkpoint-deployment/identity.json".to_owned(),
                "checkpoint-id-blocks/block=00000000000000000000".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn local_checkpoint_floor_jump_claims_a_nonoverlapping_block() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::local_single_writer(object_store);

        assert_eq!(store.allocate_checkpoint_id().await.unwrap(), 1);
        let minimum = LOCAL_RESERVATION_BLOCK_SIZE * 3 + 17;
        assert_eq!(
            store
                .allocate_checkpoint_id_at_least(minimum)
                .await
                .unwrap(),
            minimum
        );
        assert_eq!(store.allocate_checkpoint_id().await.unwrap(), minimum + 1);
    }

    #[tokio::test]
    async fn stale_shared_counter_cache_cannot_allocate_below_a_floor_jump() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let stale = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let jumper = CheckpointDecisionStore::new(object_store);

        assert_eq!(stale.allocate_checkpoint_id().await.unwrap(), 1);
        assert_eq!(
            jumper.allocate_checkpoint_id_at_least(100).await.unwrap(),
            100
        );
        assert_eq!(
            stale.allocate_checkpoint_id().await.unwrap(),
            101,
            "a stale cached ETag must reload the durable winner before returning"
        );
    }

    #[tokio::test]
    async fn shared_counter_object_count_is_bounded() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));

        for expected in 1..=256 {
            assert_eq!(store.allocate_checkpoint_id().await.unwrap(), expected);
        }

        let mut locations = object_store
            .list(None)
            .map_ok(|meta| meta.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        locations.sort_unstable();
        assert_eq!(
            locations,
            vec!["checkpoint-deployment/identity.json".to_owned()]
        );
    }

    #[tokio::test]
    async fn shared_counter_authority_deletion_changes_deployment_before_id_reuse() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let old_deployment = store.load_or_create_deployment_id().await.unwrap();
        assert_eq!(store.allocate_checkpoint_id().await.unwrap(), 1);

        object_store
            .delete(&CheckpointDecisionStore::deployment_identity_path())
            .await
            .unwrap();
        let error = store.allocate_checkpoint_id().await.unwrap_err();
        assert!(error.to_string().contains("head disappeared"), "{error}");
        drop(store);

        let restarted = CheckpointDecisionStore::new(object_store);
        let new_deployment = restarted.load_or_create_deployment_id().await.unwrap();
        assert_ne!(new_deployment, old_deployment);
        assert_eq!(restarted.allocate_checkpoint_id().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn missing_shared_authority_is_not_recreated_from_a_cached_deployment_id() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let deployment = store.load_or_create_deployment_id().await.unwrap();
        store.cache_checkpoint_id_head(None);
        object_store
            .delete(&CheckpointDecisionStore::deployment_identity_path())
            .await
            .unwrap();

        let error = store.allocate_checkpoint_id().await.unwrap_err();
        assert!(error.to_string().contains(&deployment), "{error}");
        assert!(error.to_string().contains("disappeared"), "{error}");
        assert!(object_store
            .head(&CheckpointDecisionStore::deployment_identity_path())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn shared_counter_reconciles_a_lost_success_response_by_token() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
        let object_store: Arc<dyn ObjectStore> = fault.clone();
        let store = CheckpointDecisionStore::new(object_store);
        store.load_or_create_deployment_id().await.unwrap();
        fault.intercept_after_apply(CheckpointDecisionStore::deployment_identity_path());

        assert_eq!(store.allocate_checkpoint_id().await.unwrap(), 1);
        assert_eq!(store.allocate_checkpoint_id().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn uncertain_shared_counter_write_is_burned_after_late_visibility() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fault = Arc::new(DeferredPutStore::new(Arc::clone(&inner)));
        let object_store: Arc<dyn ObjectStore> = fault.clone();
        let store = CheckpointDecisionStore::new(object_store);
        store.load_or_create_deployment_id().await.unwrap();
        fault.intercept(CheckpointDecisionStore::deployment_identity_path());

        let error = store.allocate_checkpoint_id().await.unwrap_err();
        assert!(matches!(error, DecisionError::Io(_)));
        fault.apply_pending().await.unwrap();
        drop(store);

        let restarted = CheckpointDecisionStore::new(inner);
        assert_eq!(
            restarted.allocate_checkpoint_id().await.unwrap(),
            2,
            "the uncertain ID must never be returned again after it becomes visible"
        );
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
