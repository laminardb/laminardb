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
    GetOptions, GetRange, GetResult, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion,
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

const CHECKPOINT_OUTCOME_VERSION: u32 = 3;
const CHECKPOINT_DECISION_HEAD_VERSION: u32 = 1;
const CHECKPOINT_DECISION_HEAD_MAX_BYTES: u64 = 128 * 1_024;

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

/// Exact local recovery cursor published as one authoritative terminal decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointDecisionHead {
    /// Greatest terminal local outcome.
    pub latest_terminal: CheckpointOutcome,
    /// Greatest committed local outcome, including its exact committed-index reference.
    pub latest_commit: Option<CheckpointOutcome>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DurableCheckpointDecisionHead {
    version: u32,
    deployment_id: String,
    latest_terminal: CheckpointOutcome,
    latest_commit: Option<CheckpointOutcome>,
}

#[derive(Debug)]
struct VersionedCheckpointDecisionHead {
    head: DurableCheckpointDecisionHead,
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

    /// Create the canonical content-addressed body for a committed checkpoint index.
    ///
    /// Identical retries converge on the existing immutable body. The returned reference is safe
    /// to publish only after this method succeeds.
    ///
    /// # Errors
    /// Object-store I/O, malformed index content, deployment mismatch, or a conflicting body.
    pub async fn create_committed_checkpoint(
        &self,
        index: &CommittedCheckpointIndex,
    ) -> Result<CommittedCheckpointRef, DecisionError> {
        let (encoded, reference) = index
            .encode_and_reference()
            .map_err(DecisionError::Conflict)?;
        let deployment_id = self.load_or_create_deployment_id().await?;
        if index.deployment_id != deployment_id {
            return Err(DecisionError::Conflict(format!(
                "committed checkpoint belongs to deployment {}, current deployment is {deployment_id}",
                index.deployment_id
            )));
        }

        let path = Self::committed_checkpoint_path(&reference);
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
            Err(put_error) => match self.load_committed_checkpoint(&reference).await {
                Ok(stored) if stored == *index => Ok(reference),
                Ok(_) => Err(DecisionError::Conflict(format!(
                    "committed checkpoint '{}' differs from the proposed content",
                    reference.sha256
                ))),
                Err(reconcile_error) => Err(DecisionError::Io(format!(
                    "committed checkpoint write failed ({put_error}); reconciliation failed ({reconcile_error})"
                ))),
            },
        }
    }

    /// Load and verify one exact content-addressed committed checkpoint index.
    ///
    /// Verification covers the object path, recorded and observed lengths, canonical JSON body,
    /// SHA-256 reference, deployment identity, and committed-index invariants.
    ///
    /// # Errors
    /// Object-store I/O, a missing object, malformed content, or any reference mismatch.
    pub async fn load_committed_checkpoint(
        &self,
        reference: &CommittedCheckpointRef,
    ) -> Result<CommittedCheckpointIndex, DecisionError> {
        reference.validate().map_err(DecisionError::Conflict)?;
        let record = format!("committed checkpoint '{}'", reference.sha256);
        let result = self
            .get_control_record(
                &Self::committed_checkpoint_path(reference),
                &record,
                u64::try_from(MAX_COMMITTED_CHECKPOINT_INDEX_BYTES).unwrap_or(u64::MAX),
            )
            .await?
            .ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "committed checkpoint '{}' is missing",
                    reference.sha256
                ))
            })?;
        let bytes = Self::read_control_record_bytes(
            result,
            &record,
            u64::try_from(MAX_COMMITTED_CHECKPOINT_INDEX_BYTES).unwrap_or(u64::MAX),
            Some(reference.len),
        )
        .await?;
        let index: CommittedCheckpointIndex = serde_json::from_slice(&bytes).map_err(|error| {
            DecisionError::Conflict(format!(
                "committed checkpoint '{}': {error}",
                reference.sha256
            ))
        })?;
        let (canonical, actual_reference) = index
            .encode_and_reference()
            .map_err(DecisionError::Conflict)?;
        if actual_reference != *reference || canonical.as_slice() != bytes.as_ref() {
            return Err(DecisionError::Conflict(format!(
                "committed checkpoint '{}' does not match its content-addressed reference",
                reference.sha256
            )));
        }
        let deployment_id = self.load_or_create_deployment_id().await?;
        if index.deployment_id != deployment_id {
            return Err(DecisionError::Conflict(format!(
                "committed checkpoint belongs to deployment {}, current deployment is {deployment_id}",
                index.deployment_id
            )));
        }
        Ok(index)
    }

    pub(crate) async fn validate_committed_checkpoint_for_outcome(
        &self,
        outcome: &CheckpointOutcome,
    ) -> Result<CommittedCheckpointIndex, DecisionError> {
        outcome.validate_shape(outcome.epoch)?;
        let reference = outcome.committed_checkpoint.as_ref().ok_or_else(|| {
            DecisionError::Conflict(format!(
                "commit outcome for epoch {} requires a committed checkpoint",
                outcome.epoch
            ))
        })?;
        let index = self.load_committed_checkpoint(reference).await?;
        if index.epoch != outcome.epoch
            || index.checkpoint_id != outcome.checkpoint_id
            || index.scope != outcome.scope
            || index.assignment_fence.as_ref() != outcome.assignment_fence.as_ref()
            || index.deployment_id != outcome.deployment_id
        {
            return Err(DecisionError::Conflict(format!(
                "commit outcome for epoch {} does not match committed checkpoint '{}'",
                outcome.epoch, reference.sha256
            )));
        }
        Ok(index)
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

    fn consume_local_reservation(&self, minimum: u64) -> Option<u64> {
        let mut reservation = self.local_reservation.lock();
        let checkpoint_id = reservation.next_id?.max(minimum);
        if checkpoint_id > reservation.end {
            reservation.next_id = None;
            return None;
        }
        reservation.next_id = checkpoint_id
            .checked_add(1)
            .filter(|next| *next <= reservation.end);
        Some(checkpoint_id)
    }

    fn install_local_reservation(&self, first: u64, end: u64) {
        let mut reservation = self.local_reservation.lock();
        reservation.end = end;
        reservation.next_id = first.checked_add(1).filter(|next| *next <= end);
    }

    async fn allocate_local_checkpoint_id_at_least(
        &self,
        deployment_id: &str,
        minimum: u64,
    ) -> Result<u64, DecisionError> {
        if let Some(checkpoint_id) = self.consume_local_reservation(minimum) {
            return Ok(checkpoint_id);
        }
        let lock = self.local_metadata_rmw_lock.as_ref().ok_or_else(|| {
            DecisionError::Conflict(
                "local decision store is missing its namespace write lock".into(),
            )
        })?;
        let _guard = lock.lock().await;
        let current = self
            .read_checkpoint_id_head(deployment_id)
            .await?
            .ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "checkpoint ID authority for deployment {deployment_id} disappeared"
                ))
            })?;
        let checkpoint_id = current
            .head
            .checkpoint_id
            .checked_add(1)
            .map(|next| next.max(minimum))
            .ok_or_else(|| DecisionError::Conflict("checkpoint ID space exhausted u64".into()))?;
        let reservation_end = checkpoint_id.saturating_add(LOCAL_RESERVATION_SIZE - 1);
        let head = DeploymentIdentity {
            version: DEPLOYMENT_IDENTITY_VERSION,
            id: deployment_id.to_owned(),
            allocator_mode: self.update_mode,
            checkpoint_id: reservation_end,
            allocation_id: uuid::Uuid::now_v7().to_string(),
        };
        let payload = Self::encode_control_record(
            "deployment identity",
            &head,
            DEPLOYMENT_IDENTITY_MAX_BYTES,
        )?;
        let result = self
            .store
            .put_opts(
                &Self::deployment_identity_path(),
                PutPayload::from(payload),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..PutOptions::default()
                },
            )
            .await;
        match result {
            Ok(put_result) => {
                self.cache_checkpoint_id_head(Some(VersionedCheckpointIdHead {
                    head,
                    update_version: put_result.into(),
                }));
                self.install_local_reservation(checkpoint_id, reservation_end);
                Ok(checkpoint_id)
            }
            Err(error) => {
                let observed = self.read_checkpoint_id_head(deployment_id).await?;
                if observed.as_ref().is_some_and(|value| value.head == head) {
                    self.cache_checkpoint_id_head(observed);
                    self.install_local_reservation(checkpoint_id, reservation_end);
                    Ok(checkpoint_id)
                } else {
                    self.cache_checkpoint_id_head(observed);
                    Err(DecisionError::Io(error.to_string()))
                }
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
    /// Shared stores advance exactly one ID with native compare-and-swap. A certified local single
    /// writer reserves a durable range in the deployment singleton, consumes it in memory, and
    /// burns any unused suffix after restart.
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
                self.allocate_local_checkpoint_id_at_least(&deployment_id, minimum)
                    .await
            }
        }
    }

    #[cfg(test)]
    async fn checkpoint_id_reservation_high_watermark(&self) -> Result<u64, DecisionError> {
        let deployment_id = self.load_or_create_deployment_id().await?;
        self.read_checkpoint_id_head(&deployment_id)
            .await?
            .map(|head| head.head.checkpoint_id)
            .ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "checkpoint ID authority for deployment {deployment_id} disappeared"
                ))
            })
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

    fn validate_decision_head_shape(
        head: &DurableCheckpointDecisionHead,
        deployment_id: &str,
    ) -> Result<(), DecisionError> {
        if head.version != CHECKPOINT_DECISION_HEAD_VERSION || head.deployment_id != deployment_id {
            return Err(DecisionError::Conflict(
                "checkpoint decision head has a foreign deployment or unsupported version".into(),
            ));
        }
        head.latest_terminal
            .validate_shape(head.latest_terminal.epoch)?;
        if head.latest_terminal.scope != CheckpointScope::Local
            || head.latest_terminal.deployment_id != deployment_id
        {
            return Err(DecisionError::Conflict(
                "checkpoint decision head contains a non-local terminal outcome".into(),
            ));
        }
        match head.latest_commit.as_ref() {
            Some(commit) => {
                commit.validate_shape(commit.epoch)?;
                if commit.scope != CheckpointScope::Local
                    || commit.deployment_id != deployment_id
                    || !commit.is_commit()
                    || commit.epoch > head.latest_terminal.epoch
                    || (!head.latest_terminal.is_commit()
                        && commit.epoch == head.latest_terminal.epoch)
                {
                    return Err(DecisionError::Conflict(
                        "checkpoint decision head contains an invalid latest Commit".into(),
                    ));
                }
                if head.latest_terminal.is_commit() && commit != &head.latest_terminal {
                    return Err(DecisionError::Conflict(
                        "terminal Commit does not match the decision head's latest Commit".into(),
                    ));
                }
            }
            None if head.latest_terminal.is_commit() => {
                return Err(DecisionError::Conflict(
                    "checkpoint decision head lost its terminal Commit".into(),
                ));
            }
            None => {}
        }
        Ok(())
    }

    async fn read_decision_head(
        &self,
        deployment_id: &str,
    ) -> Result<Option<VersionedCheckpointDecisionHead>, DecisionError> {
        let path = Self::decision_head_path(deployment_id);
        let Some(result) = self
            .get_control_record(
                &path,
                "checkpoint decision head",
                CHECKPOINT_DECISION_HEAD_MAX_BYTES,
            )
            .await?
        else {
            return Ok(None);
        };
        let update_version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        self.require_native_cas_token("checkpoint decision head", &update_version)?;
        let bytes = Self::read_control_record_bytes(
            result,
            "checkpoint decision head",
            CHECKPOINT_DECISION_HEAD_MAX_BYTES,
            None,
        )
        .await?;
        let head: DurableCheckpointDecisionHead =
            serde_json::from_slice(&bytes).map_err(|error| {
                DecisionError::Conflict(format!("checkpoint decision head: {error}"))
            })?;
        Self::validate_decision_head_shape(&head, deployment_id)?;
        let canonical = serde_json::to_vec(&head)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(DecisionError::Conflict(
                "checkpoint decision head does not use its canonical body".into(),
            ));
        }
        Ok(Some(VersionedCheckpointDecisionHead {
            head,
            update_version,
        }))
    }

    pub(crate) async fn canonical_outcome_with_index(
        &self,
        epoch: u64,
        checkpoint_id: u64,
        scope: CheckpointScope,
        assignment_fence: Option<CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        verdict: CheckpointVerdict,
        committed_checkpoint: Option<CommittedCheckpointRef>,
    ) -> Result<(CheckpointOutcome, Option<CommittedCheckpointIndex>), DecisionError> {
        let outcome = CheckpointOutcome {
            version: CHECKPOINT_OUTCOME_VERSION,
            scope,
            epoch,
            checkpoint_id,
            deployment_id: self.load_or_create_deployment_id().await?,
            assignment_fence,
            leader_proof,
            committed_checkpoint,
            verdict,
        };
        outcome.validate_shape(epoch)?;
        let index = if outcome.is_commit() {
            Some(
                self.validate_committed_checkpoint_for_outcome(&outcome)
                    .await?,
            )
        } else {
            None
        };
        Ok((outcome, index))
    }

    async fn record_outcome_inner(
        &self,
        candidate: CheckpointOutcome,
        committed_index: Option<CommittedCheckpointIndex>,
    ) -> Result<RecordOutcomeResult, DecisionError> {
        let observed = self.read_decision_head(&candidate.deployment_id).await?;
        if let Some(current) = observed.as_ref() {
            if current.head.latest_terminal.epoch == candidate.epoch {
                return if current.head.latest_terminal == candidate {
                    Ok(RecordOutcomeResult::Unchanged(candidate))
                } else {
                    Ok(RecordOutcomeResult::Conflict {
                        winner: current.head.latest_terminal.clone(),
                    })
                };
            }
            if current.head.latest_terminal.epoch > candidate.epoch {
                return Ok(RecordOutcomeResult::Conflict {
                    winner: current.head.latest_terminal.clone(),
                });
            }
        }

        if candidate.is_commit() {
            let index = committed_index.as_ref().ok_or_else(|| {
                DecisionError::Conflict("Commit has no validated committed checkpoint".into())
            })?;
            let expected_predecessor = observed.as_ref().and_then(|current| {
                current
                    .head
                    .latest_commit
                    .as_ref()
                    .and_then(|commit| commit.committed_checkpoint.clone())
            });
            if index.predecessor != expected_predecessor {
                return Err(DecisionError::Conflict(format!(
                    "Commit epoch {} does not extend the authoritative committed checkpoint",
                    candidate.epoch
                )));
            }
            if let Some(predecessor_ref) = expected_predecessor.as_ref() {
                let predecessor = self.load_committed_checkpoint(predecessor_ref).await?;
                index
                    .validate_predecessor_index(&predecessor)
                    .map_err(DecisionError::Conflict)?;
            }
        }

        let head = DurableCheckpointDecisionHead {
            version: CHECKPOINT_DECISION_HEAD_VERSION,
            deployment_id: candidate.deployment_id.clone(),
            latest_commit: if candidate.is_commit() {
                Some(candidate.clone())
            } else {
                observed
                    .as_ref()
                    .and_then(|current| current.head.latest_commit.clone())
            },
            latest_terminal: candidate.clone(),
        };
        Self::validate_decision_head_shape(&head, &candidate.deployment_id)?;
        let expected = observed.map(|current| current.update_version);
        let payload = Self::encode_control_record(
            "checkpoint decision head",
            &head,
            CHECKPOINT_DECISION_HEAD_MAX_BYTES,
        )?;
        let mode = match (self.update_mode, expected) {
            (_, None) => PutMode::Create,
            (DecisionStoreUpdateMode::NativeCas, Some(version)) => PutMode::Update(version),
            (DecisionStoreUpdateMode::LocalSingleWriter, Some(_)) => PutMode::Overwrite,
        };
        let result = self
            .store
            .put_opts(
                &Self::decision_head_path(&head.deployment_id),
                PutPayload::from(payload),
                PutOptions {
                    mode,
                    ..PutOptions::default()
                },
            )
            .await
            .map(|_| ());
        if result.is_ok() {
            return Ok(RecordOutcomeResult::Created(candidate));
        }

        let winner = self.read_decision_head(&head.deployment_id).await?;
        if let Some(winner) = winner {
            if winner.head == head {
                return Ok(RecordOutcomeResult::Unchanged(candidate));
            }
            if winner.head.latest_terminal.epoch >= candidate.epoch {
                return if winner.head.latest_terminal == candidate {
                    Ok(RecordOutcomeResult::Unchanged(candidate))
                } else {
                    Ok(RecordOutcomeResult::Conflict {
                        winner: winner.head.latest_terminal,
                    })
                };
            }
        }

        let error = result.expect_err("failed decision-head write has an error");
        match error {
            object_store::Error::Precondition { .. }
            | object_store::Error::AlreadyExists { .. }
            | object_store::Error::NotFound { .. } => Err(DecisionError::Conflict(format!(
                "checkpoint decision head contention did not publish epoch {}",
                candidate.epoch
            ))),
            error => Err(DecisionError::Io(error.to_string())),
        }
    }

    /// Publish the authoritative local terminal outcome.
    ///
    /// The singleton CAS is the decision: a crash before it leaves the attempt unresolved, while a
    /// crash after it leaves both the latest terminal and latest Commit directly recoverable.
    /// Equal retries converge; stale epochs and conflicting outcomes return the durable winner.
    ///
    /// # Errors
    /// Object-store I/O, malformed metadata, a forked Commit predecessor, or cluster authority.
    pub async fn record_outcome(
        &self,
        epoch: u64,
        checkpoint_id: u64,
        scope: CheckpointScope,
        assignment_fence: Option<CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        verdict: CheckpointVerdict,
        committed_checkpoint: Option<CommittedCheckpointRef>,
    ) -> Result<RecordOutcomeResult, DecisionError> {
        if scope == CheckpointScope::Cluster {
            return Err(DecisionError::Conflict(
                "cluster outcomes must be admitted through the shared leader authority".into(),
            ));
        }
        let (candidate, committed_index) = self
            .canonical_outcome_with_index(
                epoch,
                checkpoint_id,
                scope,
                assignment_fence,
                leader_proof,
                verdict,
                committed_checkpoint,
            )
            .await?;
        if self.update_mode == DecisionStoreUpdateMode::LocalSingleWriter {
            let lock = self.local_metadata_rmw_lock.as_ref().ok_or_else(|| {
                DecisionError::Conflict(
                    "local decision store is missing its namespace write lock".into(),
                )
            })?;
            let _guard = lock.lock().await;
            self.record_outcome_inner(candidate, committed_index).await
        } else {
            self.record_outcome_inner(candidate, committed_index).await
        }
    }

    /// Read the exact authoritative local decision head without listing storage.
    ///
    /// # Errors
    /// Object-store I/O or malformed/foreign head metadata.
    pub async fn checkpoint_decision_head(
        &self,
    ) -> Result<Option<CheckpointDecisionHead>, DecisionError> {
        let deployment_id = self.load_or_create_deployment_id().await?;
        Ok(self
            .read_decision_head(&deployment_id)
            .await?
            .map(|versioned| CheckpointDecisionHead {
                latest_terminal: versioned.head.latest_terminal,
                latest_commit: versioned.head.latest_commit,
            }))
    }

    /// Read the latest authoritative local terminal outcome without listing storage.
    ///
    /// # Errors
    /// Object-store I/O or malformed/foreign head metadata.
    pub async fn latest_terminal_outcome(
        &self,
    ) -> Result<Option<CheckpointOutcome>, DecisionError> {
        Ok(self
            .checkpoint_decision_head()
            .await?
            .map(|head| head.latest_terminal))
    }

    /// Read the latest authoritative local Commit without listing storage.
    ///
    /// # Errors
    /// Object-store I/O or malformed/foreign head metadata.
    pub async fn latest_committed_outcome(
        &self,
    ) -> Result<Option<CheckpointOutcome>, DecisionError> {
        Ok(self
            .checkpoint_decision_head()
            .await?
            .and_then(|head| head.latest_commit))
    }
}

#[cfg(test)]
mod tests;
