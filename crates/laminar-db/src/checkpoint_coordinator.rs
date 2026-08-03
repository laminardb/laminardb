//! Checkpoint capture and participant persistence.

#![allow(clippy::disallowed_types)] // checkpoint control path

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::connector::{
    CoordinatedCommitBatch, CoordinatedCommitCursor, CoordinatedCommitNamespace,
    CoordinatedCommitPayload, MAX_COORDINATED_COMMIT_BATCH_BYTES,
    MAX_COORDINATED_COMMIT_PAYLOAD_BYTES,
};
#[cfg(feature = "cluster")]
use laminar_core::checkpoint::CheckpointAttemptRelation;
use laminar_core::checkpoint::{
    checkpoint_descriptor_sha256, checkpoint_manifest_bytes, checkpoint_sha256,
    classify_channel_progress, ByteRange, ChannelProgress, CheckpointAttempt, CheckpointManifest,
    CheckpointScope, CheckpointStore, CheckpointWatermark, CommittedCheckpointIndex,
    CommittedCheckpointRef, CommittedParticipantRef, ConnectorCheckpoint, LeaderProof,
    PipelineIdentity, PreparedSinkDescriptor, ReferencedStateChunk, StateChunkId, StateFrame,
    StateFrameKey, COMMITTED_CHECKPOINT_INDEX_VERSION, PREPARED_SINK_DESCRIPTOR_VERSION,
};
#[cfg(feature = "cluster")]
use laminar_core::cluster::control::{BarrierAnnouncement, Phase};
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::error::DbError;

const MAX_SINK_PHASE_ONE_CONCURRENCY: usize = 8;
const MAX_RETENTION_IO_CONCURRENCY: usize = 8;
const RETENTION_RETRY_DELAY: Duration = Duration::from_secs(30);
#[cfg(feature = "cluster")]
const FOLLOWER_DECISION_POLL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    pub checkpoint_timeout: Duration,
    pub(crate) cleanup_timeout: Duration,
    pub(crate) quorum_timeout: Duration,
    pub max_node_data_bytes: u64,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_timeout: Duration::from_secs(120),
            cleanup_timeout: Duration::from_secs(30),
            quorum_timeout: Duration::from_secs(3),
            max_node_data_bytes:
                laminar_core::checkpoint::checkpoint_store::DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CheckpointRequest {
    pub assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
    pub state_frames: Vec<CapturedStateFrame>,
    pub channel_progress: Vec<ChannelProgress>,
    pub source_offset_overrides: HashMap<String, ConnectorCheckpoint>,
}

#[derive(Debug, Clone)]
pub struct CapturedStateFrame {
    pub key: StateFrameKey,
    pub state: Option<Bytes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum CheckpointPhase {
    Idle,
    PreCommitting,
    Persisting,
    Deciding,
}

impl std::fmt::Display for CheckpointPhase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => formatter.write_str("Idle"),
            Self::PreCommitting => formatter.write_str("PreCommitting"),
            Self::Persisting => formatter.write_str("Persisting"),
            Self::Deciding => formatter.write_str("Deciding"),
        }
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointFailureDisposition {
    Retryable,
    RequiresRecovery,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CheckpointResult {
    pub success: bool,
    pub checkpoint_id: u64,
    pub epoch: u64,
    pub duration: Duration,
    pub error: Option<String>,
    pub failure_disposition: Option<CheckpointFailureDisposition>,
}

impl CheckpointResult {
    #[must_use]
    pub fn continuation_error(&self) -> Option<&str> {
        self.success.then_some(self.error.as_deref()).flatten()
    }

    #[must_use]
    pub fn requires_recovery(&self) -> bool {
        !self.success
            && self.failure_disposition == Some(CheckpointFailureDisposition::RequiresRecovery)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkEpochReservation {
    Opening(CheckpointAttempt),
    Ready(CheckpointAttempt),
    InDoubt(CheckpointAttempt),
}

impl SinkEpochReservation {
    const fn attempt(self) -> CheckpointAttempt {
        match self {
            Self::Opening(attempt) | Self::Ready(attempt) | Self::InDoubt(attempt) => attempt,
        }
    }
}

#[derive(Debug)]
pub(crate) struct EpochAllocator {
    next_id_floor: std::sync::atomic::AtomicU64,
    observed_id_floor: std::sync::atomic::AtomicU64,
    allocation_lock: tokio::sync::Mutex<()>,
    sink_epoch_reservation: parking_lot::Mutex<Option<SinkEpochReservation>>,
    decision_store:
        std::sync::OnceLock<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
}

fn checked_successor_epoch(epoch: u64, context: &str) -> Result<u64, DbError> {
    epoch.checked_add(1).ok_or_else(|| {
        DbError::Checkpoint(format!(
            "checkpoint epoch space exhausted at {epoch} while {context}"
        ))
    })
}

fn require_canonical_attempt(
    attempt: CheckpointAttempt,
    context: &str,
) -> Result<CheckpointAttempt, DbError> {
    if attempt.is_canonical() {
        Ok(attempt)
    } else {
        Err(DbError::Checkpoint(format!(
            "{context} requires one nonzero canonical checkpoint ID; received epoch {} and checkpoint ID {}",
            attempt.epoch, attempt.checkpoint_id
        )))
    }
}

impl EpochAllocator {
    fn new() -> Self {
        Self {
            next_id_floor: std::sync::atomic::AtomicU64::new(1),
            observed_id_floor: std::sync::atomic::AtomicU64::new(0),
            allocation_lock: tokio::sync::Mutex::new(()),
            sink_epoch_reservation: parking_lot::Mutex::new(None),
            decision_store: std::sync::OnceLock::new(),
        }
    }

    fn bind_decision_store(
        &self,
        store: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<(), DbError> {
        if let Some(bound) = self.decision_store.get() {
            return if Arc::ptr_eq(bound, &store) {
                Ok(())
            } else {
                Err(DbError::Checkpoint(
                    "checkpoint allocator decision store is already bound".into(),
                ))
            };
        }
        self.decision_store.set(store).map_err(|_| {
            DbError::Checkpoint("checkpoint allocator decision store is already bound".into())
        })
    }

    async fn allocate_fresh_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        use std::sync::atomic::Ordering;

        let store = self.decision_store.get().ok_or_else(|| {
            DbError::Checkpoint("checkpoint ID allocation requires a decision store".into())
        })?;
        loop {
            let minimum = self.next_id_floor.load(Ordering::Acquire).max(1);
            let checkpoint_id =
                tokio::time::timeout_at(deadline, store.allocate_checkpoint_id_at_least(minimum))
                    .await
                    .map_err(|_| DbError::Checkpoint("checkpoint ID allocation timed out".into()))?
                    .map_err(|error| {
                        DbError::Checkpoint(format!("checkpoint ID allocation failed: {error}"))
                    })?;
            let successor = checked_successor_epoch(checkpoint_id, "advancing allocation")?;
            let mut floor = self.next_id_floor.load(Ordering::Acquire);
            loop {
                if checkpoint_id < floor {
                    break;
                }
                match self.next_id_floor.compare_exchange_weak(
                    floor,
                    successor.max(floor),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Ok(CheckpointAttempt::canonical(checkpoint_id)),
                    Err(observed) => floor = observed,
                }
            }
            tokio::task::yield_now().await;
        }
    }

    pub(crate) async fn allocate_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let _guard = tokio::time::timeout_at(deadline, self.allocation_lock.lock())
            .await
            .map_err(|_| DbError::Checkpoint("checkpoint allocator lock timed out".into()))?;
        if let Some(reservation) = *self.sink_epoch_reservation.lock() {
            return Err(DbError::Checkpoint(format!(
                "sink epoch {} must be consumed before allocating another checkpoint",
                reservation.attempt().epoch
            )));
        }
        self.allocate_fresh_until(deadline).await
    }

    async fn reserve_sink_epoch_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let _guard = tokio::time::timeout_at(deadline, self.allocation_lock.lock())
            .await
            .map_err(|_| DbError::Checkpoint("sink epoch allocator lock timed out".into()))?;
        if let Some(reservation) = *self.sink_epoch_reservation.lock() {
            return Err(DbError::Checkpoint(format!(
                "sink epoch {} is already reserved",
                reservation.attempt().epoch
            )));
        }
        let attempt = self.allocate_fresh_until(deadline).await?;
        *self.sink_epoch_reservation.lock() = Some(SinkEpochReservation::Opening(attempt));
        Ok(attempt)
    }

    fn mark_sink_epoch_ready(&self, attempt: CheckpointAttempt) -> Result<(), DbError> {
        let mut reservation = self.sink_epoch_reservation.lock();
        match *reservation {
            Some(SinkEpochReservation::Opening(current)) if current == attempt => {
                *reservation = Some(SinkEpochReservation::Ready(attempt));
                Ok(())
            }
            current => Err(DbError::Checkpoint(format!(
                "sink epoch reservation mismatch for {attempt:?}: {current:?}"
            ))),
        }
    }

    fn mark_sink_epoch_in_doubt(&self, attempt: CheckpointAttempt) {
        *self.sink_epoch_reservation.lock() = Some(SinkEpochReservation::InDoubt(attempt));
    }

    pub(crate) async fn consume_sink_epoch_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let _guard = tokio::time::timeout_at(deadline, self.allocation_lock.lock())
            .await
            .map_err(|_| DbError::Checkpoint("checkpoint allocator lock timed out".into()))?;
        let mut reservation = self.sink_epoch_reservation.lock();
        match *reservation {
            Some(SinkEpochReservation::Ready(attempt)) => {
                reservation.take();
                Ok(attempt)
            }
            Some(SinkEpochReservation::Opening(attempt)) => Err(DbError::Checkpoint(format!(
                "sink epoch {} is still opening",
                attempt.epoch
            ))),
            Some(SinkEpochReservation::InDoubt(attempt)) => Err(DbError::Checkpoint(format!(
                "sink epoch {} requires recovery",
                attempt.epoch
            ))),
            None => Err(DbError::Checkpoint(
                "checkpoint-committable sinks have no open epoch".into(),
            )),
        }
    }

    fn clear_sink_epoch(&self, attempt: CheckpointAttempt) {
        let mut reservation = self.sink_epoch_reservation.lock();
        if reservation.is_some_and(|current| current.attempt() == attempt) {
            reservation.take();
        }
    }

    pub(crate) fn peek_epoch(&self) -> u64 {
        use std::sync::atomic::Ordering;
        if let Some(reservation) = *self.sink_epoch_reservation.lock() {
            reservation.attempt().epoch
        } else {
            self.next_id_floor.load(Ordering::Acquire)
        }
    }

    pub(crate) fn advance_epoch_to(&self, epoch: u64) {
        use std::sync::atomic::Ordering;
        self.next_id_floor.fetch_max(epoch, Ordering::AcqRel);
        self.observed_id_floor.fetch_max(epoch, Ordering::AcqRel);
    }
}

#[cfg(feature = "cluster")]
pub(crate) type QuorumPeer = laminar_core::cluster::discovery::NodeId;
#[cfg(not(feature = "cluster"))]
pub(crate) type QuorumPeer = u64;

#[derive(Debug, Clone)]
pub(crate) enum QuorumStage {
    RunInline,
    Done {
        cluster_watermark: laminar_core::checkpoint::CheckpointWatermark,
        participants: Vec<QuorumPeer>,
        #[cfg(feature = "cluster")]
        leader_proof: LeaderProof,
    },
}

#[cfg(feature = "cluster")]
pub(crate) struct PrepareQuorum<'a> {
    attempt: CheckpointAttempt,
    local_watermark: laminar_core::checkpoint::CheckpointWatermark,
    assignment_fence: &'a laminar_core::checkpoint::CheckpointAssignmentFence,
    leader_proof: &'a LeaderProof,
    announce_prepare: bool,
}

#[cfg(feature = "cluster")]
impl<'a> PrepareQuorum<'a> {
    pub(crate) const fn new(
        attempt: CheckpointAttempt,
        local_watermark: laminar_core::checkpoint::CheckpointWatermark,
        assignment_fence: &'a laminar_core::checkpoint::CheckpointAssignmentFence,
        leader_proof: &'a LeaderProof,
        announce_prepare: bool,
    ) -> Self {
        Self {
            attempt,
            local_watermark,
            assignment_fence,
            leader_proof,
            announce_prepare,
        }
    }
}

pub(crate) struct RegisteredSink {
    name: String,
    handle: crate::sink_task::SinkTaskHandle,
}

struct PackedCheckpoint {
    manifest: CheckpointManifest,
    node_data: Vec<Bytes>,
}

#[derive(Clone)]
enum GcAuthority {
    Local,
    #[cfg(feature = "cluster")]
    Cluster {
        authority: Arc<laminar_core::cluster::control::LeaderLeaseStore>,
        proof: LeaderProof,
        controller: std::sync::Weak<laminar_core::cluster::control::ClusterController>,
    },
}

impl GcAuthority {
    fn can_retry(&self) -> bool {
        match self {
            Self::Local => true,
            #[cfg(feature = "cluster")]
            Self::Cluster {
                proof, controller, ..
            } => controller
                .upgrade()
                .is_some_and(|controller| controller.proof_is_live(proof)),
        }
    }
}

#[derive(Clone)]
struct GcRequest {
    requested: Option<CommittedCheckpointIndex>,
    decision_store: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    authority: GcAuthority,
}

async fn load_index_manifests(
    store: &dyn CheckpointStore,
    index: &CommittedCheckpointIndex,
) -> Result<Vec<CheckpointManifest>, DbError> {
    let checkpoint_id = index.checkpoint_id;
    let reads = index
        .participants
        .clone()
        .into_iter()
        .map(|participant| async move {
            let manifest = store
                .load_manifest_verified(
                    participant.participant_id,
                    checkpoint_id,
                    participant.manifest_len,
                    &participant.manifest_sha256,
                )
                .await
                .map_err(DbError::from)?
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "checkpoint {} participant {} manifest is missing",
                        checkpoint_id, participant.participant_id
                    ))
                })?;
            let encoded = checkpoint_manifest_bytes(&manifest).map_err(|error| {
                DbError::Checkpoint(format!("encode checkpoint manifest: {error}"))
            })?;
            participant
                .verify_manifest(&manifest, &encoded)
                .map_err(DbError::Checkpoint)?;
            Ok::<_, DbError>((participant.participant_id, manifest, encoded))
        });
    let mut loaded = futures::stream::iter(reads)
        .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    loaded.sort_unstable_by_key(|(participant_id, _, _)| *participant_id);
    if let Some((participant_id, _, _)) = loaded.iter().find(|(_, manifest, _)| {
        manifest.epoch != index.epoch
            || manifest.checkpoint_id != index.checkpoint_id
            || manifest.deployment_id != index.deployment_id
            || manifest.pipeline_identity != index.pipeline_identity
            || manifest.vnode_count != index.vnode_count
            || manifest.assignment_fence != index.assignment_fence
    }) {
        return Err(DbError::Checkpoint(format!(
            "checkpoint {} participant {} manifest belongs to a different committed cut",
            index.checkpoint_id, participant_id
        )));
    }
    let views = loaded
        .iter()
        .map(|(_, manifest, bytes)| (manifest, bytes.as_slice()))
        .collect::<Vec<_>>();
    index
        .validate_participant_manifests(&views)
        .map_err(DbError::Checkpoint)?;
    Ok(loaded
        .into_iter()
        .map(|(_, manifest, _)| manifest)
        .collect())
}

struct LiveChunkInventory {
    references: BTreeSet<StateChunkId>,
    pinned: BTreeSet<StateChunkId>,
}

fn live_chunk_inventory(manifests: &[CheckpointManifest]) -> LiveChunkInventory {
    let mut references = BTreeSet::new();
    let mut pinned = BTreeSet::new();
    for manifest in manifests {
        pinned.insert(manifest.node_data.chunk);
        for reference in &manifest.referenced_chunks {
            references.insert(reference.chunk);
        }
    }
    LiveChunkInventory { references, pinned }
}

async fn delete_retired_data(
    store: &dyn CheckpointStore,
    manifests: &[CheckpointManifest],
    live: &LiveChunkInventory,
) -> Result<(), DbError> {
    let mut candidates = BTreeSet::new();
    for manifest in manifests {
        candidates.insert(manifest.node_data.chunk);
        candidates.extend(
            manifest
                .referenced_chunks
                .iter()
                .map(|reference| reference.chunk),
        );
    }
    let deletions = candidates
        .into_iter()
        .filter(|chunk| !live.pinned.contains(chunk) && !live.references.contains(chunk));
    let results = futures::stream::iter(deletions)
        .map(|chunk| async move { store.delete_node_data(chunk).await })
        .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    for result in results {
        result.map_err(DbError::from)?;
    }
    Ok(())
}

async fn delete_retired_manifests(
    store: &dyn CheckpointStore,
    checkpoint_id: u64,
    participant_ids: &[u64],
) -> Result<(), DbError> {
    let results = futures::stream::iter(participant_ids.to_vec())
        .map(|participant_id| async move {
            store
                .delete_manifest(StateChunkId {
                    participant_id,
                    checkpoint_id,
                })
                .await
        })
        .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    for result in results {
        result.map_err(DbError::from)?;
    }
    Ok(())
}

struct ProtectedCheckpoint {
    index: CommittedCheckpointIndex,
    live: LiveChunkInventory,
}

async fn load_protected_checkpoint(
    store: &dyn CheckpointStore,
    decisions: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    reference: &CommittedCheckpointRef,
) -> Result<ProtectedCheckpoint, DbError> {
    let index = decisions
        .load_committed_checkpoint(reference)
        .await
        .map_err(|error| DbError::Checkpoint(format!("load retained checkpoint index: {error}")))?;
    let manifests = load_index_manifests(store, &index).await?;
    let live = live_chunk_inventory(&manifests);
    Ok(ProtectedCheckpoint { index, live })
}

async fn load_cleanup_target(
    store: &dyn CheckpointStore,
    decisions: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    protected: &CommittedCheckpointIndex,
    current: &CommittedCheckpointRef,
    next: Option<&CommittedCheckpointRef>,
    participant_ids: Option<&[u64]>,
) -> Result<(CommittedCheckpointIndex, Vec<CheckpointManifest>), DbError> {
    let index = decisions
        .load_committed_checkpoint(current)
        .await
        .map_err(|error| DbError::Checkpoint(format!("load retired checkpoint index: {error}")))?;
    if index.deployment_id != protected.deployment_id
        || index.pipeline_identity != protected.pipeline_identity
        || index.scope != protected.scope
        || index.vnode_count != protected.vnode_count
        || index.epoch >= protected.epoch
        || index.predecessor.as_ref() != next
    {
        return Err(DbError::Checkpoint(format!(
            "checkpoint {} retention cursor breaks committed-cut continuity",
            current.checkpoint_id
        )));
    }
    if let Some(expected) = participant_ids {
        let actual = index
            .participants
            .iter()
            .map(|participant| participant.participant_id)
            .collect::<Vec<_>>();
        if actual != expected {
            return Err(DbError::Checkpoint(format!(
                "checkpoint {} retention cursor has a different participant roster",
                current.checkpoint_id
            )));
        }
    }
    let manifests = load_index_manifests(store, &index).await?;
    Ok((index, manifests))
}

fn local_retention_update_state(
    result: laminar_core::checkpoint_decision::CheckpointRetentionUpdateResult,
) -> Result<laminar_core::checkpoint_decision::CheckpointRetentionState, DbError> {
    use laminar_core::checkpoint_decision::CheckpointRetentionUpdateResult;
    match result {
        CheckpointRetentionUpdateResult::Applied(state)
        | CheckpointRetentionUpdateResult::Unchanged(state)
        | CheckpointRetentionUpdateResult::Conflict {
            current: Some(state),
        } => Ok(state),
        CheckpointRetentionUpdateResult::Conflict { current: None } => Err(DbError::Checkpoint(
            "checkpoint retention head disappeared during a conditional update".into(),
        )),
    }
}

async fn run_local_gc_request(
    store: &dyn CheckpointStore,
    request: &GcRequest,
) -> Result<(), DbError> {
    use laminar_core::checkpoint_decision::CheckpointRetentionState;

    let requested = request.requested.as_ref().ok_or_else(|| {
        DbError::Checkpoint("local checkpoint retention requires a committed cut".into())
    })?;
    let (_, requested) = requested
        .encode_and_reference()
        .map_err(DbError::Checkpoint)?;
    let mut state = local_retention_update_state(
        request
            .decision_store
            .begin_checkpoint_retention(&requested)
            .await
            .map_err(|error| DbError::Checkpoint(format!("begin checkpoint retention: {error}")))?,
    )?;
    let mut protected = None::<(CommittedCheckpointRef, ProtectedCheckpoint)>;

    loop {
        match &state {
            CheckpointRetentionState::Idle {
                protected: retained,
            } if retained == &requested || retained.epoch > requested.epoch => return Ok(()),
            CheckpointRetentionState::Idle { .. } => {
                state = local_retention_update_state(
                    request
                        .decision_store
                        .begin_checkpoint_retention(&requested)
                        .await
                        .map_err(|error| {
                            DbError::Checkpoint(format!("begin checkpoint retention: {error}"))
                        })?,
                )?;
            }
            CheckpointRetentionState::DeleteData { cursor } => {
                if protected
                    .as_ref()
                    .is_none_or(|(reference, _)| reference != &cursor.protected)
                {
                    protected = Some((
                        cursor.protected.clone(),
                        load_protected_checkpoint(
                            store,
                            request.decision_store.as_ref(),
                            &cursor.protected,
                        )
                        .await?,
                    ));
                }
                let retained = &protected
                    .as_ref()
                    .expect("retained checkpoint was loaded")
                    .1;
                let (_, manifests) = load_cleanup_target(
                    store,
                    request.decision_store.as_ref(),
                    &retained.index,
                    &cursor.current,
                    cursor.next.as_ref(),
                    Some(&[laminar_core::state::LOCAL_NODE_ID.0]),
                )
                .await?;
                delete_retired_data(store, &manifests, &retained.live).await?;
                state = local_retention_update_state(
                    request
                        .decision_store
                        .advance_checkpoint_retention(&state)
                        .await
                        .map_err(|error| {
                            DbError::Checkpoint(format!("advance checkpoint retention: {error}"))
                        })?,
                )?;
            }
            CheckpointRetentionState::DeleteMetadata { cursor } => {
                delete_retired_manifests(
                    store,
                    cursor.current.checkpoint_id,
                    &[laminar_core::state::LOCAL_NODE_ID.0],
                )
                .await?;
                request
                    .decision_store
                    .delete_committed_checkpoint(&cursor.current)
                    .await
                    .map_err(|error| {
                        DbError::Checkpoint(format!("delete retired checkpoint index: {error}"))
                    })?;
                state = local_retention_update_state(
                    request
                        .decision_store
                        .advance_checkpoint_retention(&state)
                        .await
                        .map_err(|error| {
                            DbError::Checkpoint(format!("advance checkpoint retention: {error}"))
                        })?,
                )?;
            }
        }
    }
}

#[cfg(feature = "cluster")]
async fn begin_cluster_cleanup(
    store: Arc<dyn CheckpointStore>,
    decisions: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    authority: &laminar_core::cluster::control::LeaderLeaseStore,
    proof: &LeaderProof,
    protected: CommittedCheckpointRef,
) -> Result<Option<laminar_core::cluster::control::ClusterArtifactCleanupCursor>, DbError> {
    authority
        .begin_cluster_artifact_cleanup(proof, protected, move |outcome| {
            let store = Arc::clone(&store);
            let decisions = Arc::clone(&decisions);
            async move {
                let reference = outcome
                    .committed_checkpoint
                    .as_ref()
                    .ok_or_else(|| "retained Commit has no checkpoint index".to_owned())?;
                load_protected_checkpoint(store.as_ref(), decisions.as_ref(), reference)
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            }
        })
        .await
        .map_err(|error| DbError::Checkpoint(format!("begin cluster retention: {error}")))
}

#[cfg(feature = "cluster")]
async fn run_cluster_gc_request(
    store: Arc<dyn CheckpointStore>,
    request: &GcRequest,
    authority: Arc<laminar_core::cluster::control::LeaderLeaseStore>,
    proof: LeaderProof,
) -> Result<(), DbError> {
    use laminar_core::cluster::control::ClusterArtifactCleanupPhase;

    let requested = request
        .requested
        .as_ref()
        .map(CommittedCheckpointIndex::encode_and_reference)
        .transpose()
        .map_err(DbError::Checkpoint)?
        .map(|(_, reference)| reference);
    let mut cursor = authority
        .cluster_artifact_cleanup()
        .await
        .map_err(|error| DbError::Checkpoint(format!("load cluster retention: {error}")))?;
    if cursor.is_none() {
        let Some(requested) = requested.as_ref() else {
            return Ok(());
        };
        cursor = begin_cluster_cleanup(
            Arc::clone(&store),
            Arc::clone(&request.decision_store),
            authority.as_ref(),
            &proof,
            requested.clone(),
        )
        .await?;
    }
    let mut protected = None::<(CommittedCheckpointRef, ProtectedCheckpoint)>;

    loop {
        let Some(current) = cursor.clone() else {
            return Ok(());
        };
        match current.phase {
            ClusterArtifactCleanupPhase::DeleteData => {
                if protected
                    .as_ref()
                    .is_none_or(|(reference, _)| reference != &current.protected)
                {
                    protected = Some((
                        current.protected.clone(),
                        load_protected_checkpoint(
                            store.as_ref(),
                            request.decision_store.as_ref(),
                            &current.protected,
                        )
                        .await?,
                    ));
                }
                let retained = &protected
                    .as_ref()
                    .expect("retained checkpoint was loaded")
                    .1;
                let (_, manifests) = load_cleanup_target(
                    store.as_ref(),
                    request.decision_store.as_ref(),
                    &retained.index,
                    &current.current,
                    current.next.as_ref(),
                    Some(&current.participant_ids),
                )
                .await?;
                delete_retired_data(store.as_ref(), &manifests, &retained.live).await?;
                cursor = Some(
                    authority
                        .mark_cluster_artifact_data_deleted(&proof, &current)
                        .await
                        .map_err(|error| {
                            DbError::Checkpoint(format!(
                                "advance cluster retention data phase: {error}"
                            ))
                        })?,
                );
            }
            ClusterArtifactCleanupPhase::DeleteMetadata => {
                delete_retired_manifests(
                    store.as_ref(),
                    current.current.checkpoint_id,
                    &current.participant_ids,
                )
                .await?;
                request
                    .decision_store
                    .delete_committed_checkpoint(&current.current)
                    .await
                    .map_err(|error| {
                        DbError::Checkpoint(format!("delete retired checkpoint index: {error}"))
                    })?;
                let completed = current.protected.clone();
                cursor = authority
                    .mark_cluster_artifact_metadata_deleted(&proof, &current)
                    .await
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "advance cluster retention metadata phase: {error}"
                        ))
                    })?;
                if cursor.is_none() {
                    let Some(requested) = requested.as_ref() else {
                        return Ok(());
                    };
                    if completed.epoch >= requested.epoch {
                        return Ok(());
                    }
                    cursor = begin_cluster_cleanup(
                        Arc::clone(&store),
                        Arc::clone(&request.decision_store),
                        authority.as_ref(),
                        &proof,
                        requested.clone(),
                    )
                    .await?;
                }
            }
        }
    }
}

async fn run_gc_request(
    store: Arc<dyn CheckpointStore>,
    request: GcRequest,
) -> Result<(), DbError> {
    match request.authority.clone() {
        GcAuthority::Local
            if request
                .requested
                .as_ref()
                .is_some_and(|index| index.scope == CheckpointScope::Local) =>
        {
            run_local_gc_request(store.as_ref(), &request).await
        }
        #[cfg(feature = "cluster")]
        GcAuthority::Cluster {
            authority, proof, ..
        } if request
            .requested
            .as_ref()
            .is_none_or(|index| index.scope == CheckpointScope::Cluster) =>
        {
            run_cluster_gc_request(store, &request, authority, proof).await
        }
        _ => Err(DbError::Checkpoint(
            "checkpoint retention authority does not match the committed scope".into(),
        )),
    }
}

#[cfg(test)]
mod artifact_tests {
    use super::*;
    use laminar_core::checkpoint::ObjectStoreCheckpointStore;
    use laminar_core::checkpoint_decision::{
        CheckpointDecisionStore, CheckpointRetentionState, CheckpointRetentionUpdateResult,
    };
    use laminar_core::state::KeyGroupCount;
    use object_store::memory::InMemory;

    fn manifest(
        checkpoint_id: u64,
        deployment_id: &str,
        retained_chunk: Option<(StateChunkId, u64, String)>,
    ) -> (CheckpointManifest, Bytes) {
        let key_groups = KeyGroupCount::try_from(1_u16).unwrap();
        let mut manifest =
            CheckpointManifest::new_with_key_group_count(checkpoint_id, checkpoint_id, key_groups);
        manifest.deployment_id = deployment_id.into();
        let payload;
        let frame_chunk;
        if let Some((chunk, object_length, sha256)) = retained_chunk {
            payload = Bytes::new();
            frame_chunk = chunk;
            manifest.referenced_chunks.push(ReferencedStateChunk {
                chunk,
                object_length,
                sha256,
                ref_count: NonZeroU32::new(1).unwrap(),
            });
        } else {
            payload = Bytes::from(vec![u8::try_from(checkpoint_id).unwrap()]);
            frame_chunk = manifest.node_data.chunk;
        }
        manifest.state_frames.push(StateFrame {
            key: StateFrameKey::Vnode {
                operator_id: "join".into(),
                vnode: 0,
            },
            chunk: frame_chunk,
            range: ByteRange {
                offset: 0,
                length: 1,
            },
            sha256: checkpoint_sha256(&[u8::try_from(frame_chunk.checkpoint_id).unwrap()]),
        });
        manifest.node_data.object_length = payload.len() as u64;
        manifest.node_data.sha256 = checkpoint_sha256(&payload);
        (manifest, payload)
    }

    fn retention_state(result: CheckpointRetentionUpdateResult) -> CheckpointRetentionState {
        match result {
            CheckpointRetentionUpdateResult::Applied(state)
            | CheckpointRetentionUpdateResult::Unchanged(state) => state,
            result => panic!("unexpected retention update: {result:?}"),
        }
    }

    #[tokio::test]
    async fn retention_reclaims_last_referenced_chunk_and_keeps_latest_cut() {
        use laminar_core::checkpoint_decision::CheckpointVerdict;

        let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let key_groups = KeyGroupCount::try_from(1_u16).unwrap();
        let store: Arc<dyn CheckpointStore> = Arc::new(
            ObjectStoreCheckpointStore::new(Arc::clone(&objects), "gc")
                .with_key_group_count(key_groups),
        );
        let decisions = Arc::new(CheckpointDecisionStore::new(objects));
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

        let mut predecessor = None;
        let mut latest = None;
        let mut checkpoint_three = None;
        for checkpoint_id in 1..=6 {
            let retained_chunk = (checkpoint_id == 6)
                .then(|| checkpoint_three.clone().expect("checkpoint three metadata"));
            let (manifest, payload) = manifest(checkpoint_id, &deployment_id, retained_chunk);
            store
                .save_checkpoint(&manifest, std::slice::from_ref(&payload))
                .await
                .unwrap();
            if checkpoint_id == 3 {
                checkpoint_three = Some((
                    manifest.node_data.chunk,
                    manifest.node_data.object_length,
                    manifest.node_data.sha256.clone(),
                ));
            }
            let manifest_bytes = checkpoint_manifest_bytes(&manifest).unwrap();
            let participant =
                CommittedParticipantRef::from_manifest(&manifest, &manifest_bytes).unwrap();
            let index = CommittedCheckpointIndex {
                version: COMMITTED_CHECKPOINT_INDEX_VERSION,
                deployment_id: deployment_id.clone(),
                pipeline_identity: manifest.pipeline_identity.clone(),
                epoch: checkpoint_id,
                checkpoint_id,
                scope: CheckpointScope::Local,
                vnode_count: 1,
                assignment_fence: None,
                predecessor,
                participants: vec![participant],
                source_offsets: BTreeMap::new(),
                channel_progress: Vec::new(),
                checkpoint_watermark: None,
            };
            let reference = decisions.create_committed_checkpoint(&index).await.unwrap();
            decisions
                .record_outcome(
                    checkpoint_id,
                    checkpoint_id,
                    CheckpointScope::Local,
                    None,
                    None,
                    CheckpointVerdict::Commit,
                    Some(reference.clone()),
                )
                .await
                .unwrap();
            predecessor = Some(reference);
            latest = Some(index);
        }

        let protected = predecessor.clone().unwrap();
        let interrupted = retention_state(
            decisions
                .begin_checkpoint_retention(&protected)
                .await
                .unwrap(),
        );
        let CheckpointRetentionState::DeleteData { cursor } = interrupted else {
            panic!("retention did not enter its data phase");
        };
        assert_eq!(cursor.current.checkpoint_id, 5);
        store
            .delete_node_data(StateChunkId {
                participant_id: 1,
                checkpoint_id: 5,
            })
            .await
            .unwrap();

        run_gc_request(
            Arc::clone(&store),
            GcRequest {
                requested: Some(latest.take().unwrap()),
                decision_store: Arc::clone(&decisions),
                authority: GcAuthority::Local,
            },
        )
        .await
        .unwrap();

        for checkpoint_id in [1, 2, 4, 5] {
            let chunk = StateChunkId {
                participant_id: 1,
                checkpoint_id,
            };
            assert_eq!(store.node_data_len(chunk).await.unwrap(), None);
        }
        assert!(store
            .node_data_len(StateChunkId {
                participant_id: 1,
                checkpoint_id: 3,
            })
            .await
            .unwrap()
            .is_some());
        for checkpoint_id in 1..=5 {
            assert_eq!(
                store
                    .load_manifest_for_participant(1, checkpoint_id)
                    .await
                    .unwrap(),
                None
            );
        }
        assert!(store
            .load_manifest_for_participant(1, 6)
            .await
            .unwrap()
            .is_some());

        let (manifest, payload) = manifest(7, &deployment_id, None);
        store
            .save_checkpoint(&manifest, std::slice::from_ref(&payload))
            .await
            .unwrap();
        let encoded = checkpoint_manifest_bytes(&manifest).unwrap();
        let index = CommittedCheckpointIndex {
            version: COMMITTED_CHECKPOINT_INDEX_VERSION,
            deployment_id,
            pipeline_identity: manifest.pipeline_identity.clone(),
            epoch: 7,
            checkpoint_id: 7,
            scope: CheckpointScope::Local,
            vnode_count: 1,
            assignment_fence: None,
            predecessor,
            participants: vec![CommittedParticipantRef::from_manifest(&manifest, &encoded).unwrap()],
            source_offsets: BTreeMap::new(),
            channel_progress: Vec::new(),
            checkpoint_watermark: None,
        };
        let reference = decisions.create_committed_checkpoint(&index).await.unwrap();
        decisions
            .record_outcome(
                7,
                7,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                Some(reference.clone()),
            )
            .await
            .unwrap();

        let delete_data = retention_state(
            decisions
                .begin_checkpoint_retention(&reference)
                .await
                .unwrap(),
        );
        let CheckpointRetentionState::DeleteData { cursor } = &delete_data else {
            panic!("retention did not enter its data phase");
        };
        let retained = load_protected_checkpoint(store.as_ref(), decisions.as_ref(), &reference)
            .await
            .unwrap();
        let (_, retired) = load_cleanup_target(
            store.as_ref(),
            decisions.as_ref(),
            &retained.index,
            &cursor.current,
            cursor.next.as_ref(),
            Some(&[1]),
        )
        .await
        .unwrap();
        delete_retired_data(store.as_ref(), &retired, &retained.live)
            .await
            .unwrap();
        let delete_metadata = retention_state(
            decisions
                .advance_checkpoint_retention(&delete_data)
                .await
                .unwrap(),
        );
        assert!(matches!(
            delete_metadata,
            CheckpointRetentionState::DeleteMetadata { .. }
        ));
        store
            .delete_manifest(StateChunkId {
                participant_id: 1,
                checkpoint_id: 6,
            })
            .await
            .unwrap();
        run_gc_request(
            Arc::clone(&store),
            GcRequest {
                requested: Some(index),
                decision_store: decisions,
                authority: GcAuthority::Local,
            },
        )
        .await
        .unwrap();

        for checkpoint_id in [3, 6] {
            assert_eq!(
                store
                    .node_data_len(StateChunkId {
                        participant_id: 1,
                        checkpoint_id,
                    })
                    .await
                    .unwrap(),
                None
            );
        }
        assert!(store
            .load_manifest_for_participant(1, 7)
            .await
            .unwrap()
            .is_some());
    }

    async fn coordinator_with_store(
        objects: Arc<dyn object_store::ObjectStore>,
    ) -> (CheckpointCoordinator, Arc<CheckpointDecisionStore>, String) {
        let decisions = Arc::new(CheckpointDecisionStore::new(Arc::clone(&objects)));
        let store = ObjectStoreCheckpointStore::new(objects, "aborted-artifacts")
            .with_key_group_count(KeyGroupCount::try_from(1_u16).unwrap());
        let mut coordinator =
            CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
        coordinator
            .bind_durable_decision_store(Arc::clone(&decisions))
            .await
            .unwrap();
        let deployment_id = coordinator.expected_deployment_id().unwrap().to_owned();
        (coordinator, decisions, deployment_id)
    }

    async fn save_prepared(
        coordinator: &mut CheckpointCoordinator,
        checkpoint_id: u64,
        deployment_id: &str,
    ) -> (CheckpointManifest, Bytes) {
        let (manifest, payload) = manifest(checkpoint_id, deployment_id, None);
        let encoded = coordinator
            .store
            .save_checkpoint(&manifest, std::slice::from_ref(&payload))
            .await
            .unwrap();
        coordinator.prepared.insert(
            CheckpointAttempt::canonical(checkpoint_id),
            (Arc::new(manifest.clone()), encoded.clone()),
        );
        (manifest, encoded)
    }

    #[tokio::test]
    async fn durable_abort_deletes_only_its_exact_prepared_artifact_and_is_idempotent() {
        let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let (mut coordinator, _decisions, deployment_id) = coordinator_with_store(objects).await;
        let (aborted, _) = save_prepared(&mut coordinator, 1, &deployment_id).await;
        let (unrelated, unrelated_payload) = manifest(2, &deployment_id, None);
        coordinator
            .store
            .save_checkpoint(&unrelated, std::slice::from_ref(&unrelated_payload))
            .await
            .unwrap();

        let attempt = CheckpointAttempt::canonical(1);
        coordinator
            .abort_attempt_until(
                attempt,
                None,
                None,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        coordinator
            .abort_attempt_until(
                attempt,
                None,
                None,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(
            coordinator
                .store
                .load_manifest_for_participant(1, 1)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            coordinator
                .store
                .node_data_len(aborted.node_data.chunk)
                .await
                .unwrap(),
            None
        );
        assert!(coordinator
            .store
            .load_manifest_for_participant(1, 2)
            .await
            .unwrap()
            .is_some());
        assert!(coordinator
            .store
            .node_data_len(unrelated.node_data.chunk)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn recovery_reclaims_artifact_left_after_durable_abort() {
        use laminar_core::checkpoint_decision::CheckpointVerdict;

        let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let (mut interrupted, decisions, deployment_id) =
            coordinator_with_store(Arc::clone(&objects)).await;
        let (manifest, _) = save_prepared(&mut interrupted, 1, &deployment_id).await;
        decisions
            .record_outcome(
                1,
                1,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
        drop(interrupted);

        let (mut restarted, _, _) = coordinator_with_store(objects).await;
        assert!(restarted.recover().await.unwrap().is_none());
        assert!(restarted
            .store
            .load_manifest_for_participant(1, 1)
            .await
            .unwrap()
            .is_none());
        assert!(restarted
            .store
            .node_data_len(manifest.node_data.chunk)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn commit_winner_prevents_prepared_artifact_deletion() {
        use laminar_core::checkpoint_decision::CheckpointVerdict;

        let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let (mut coordinator, decisions, deployment_id) = coordinator_with_store(objects).await;
        let (manifest, encoded) = save_prepared(&mut coordinator, 1, &deployment_id).await;
        let participant = CommittedParticipantRef::from_manifest(&manifest, &encoded).unwrap();
        let index = CommittedCheckpointIndex {
            version: COMMITTED_CHECKPOINT_INDEX_VERSION,
            deployment_id,
            pipeline_identity: manifest.pipeline_identity.clone(),
            epoch: 1,
            checkpoint_id: 1,
            scope: CheckpointScope::Local,
            vnode_count: 1,
            assignment_fence: None,
            predecessor: None,
            participants: vec![participant],
            source_offsets: BTreeMap::new(),
            channel_progress: Vec::new(),
            checkpoint_watermark: None,
        };
        let reference = decisions.create_committed_checkpoint(&index).await.unwrap();
        decisions
            .record_outcome(
                1,
                1,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                Some(reference),
            )
            .await
            .unwrap();

        assert!(coordinator
            .abort_attempt_until(
                CheckpointAttempt::canonical(1),
                None,
                None,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .is_err());
        assert!(coordinator
            .store
            .load_manifest_for_participant(1, 1)
            .await
            .unwrap()
            .is_some());
        assert!(coordinator
            .store
            .node_data_len(manifest.node_data.chunk)
            .await
            .unwrap()
            .is_some());
    }
}

async fn run_gc_worker(
    store: Arc<dyn CheckpointStore>,
    mut requests: tokio::sync::watch::Receiver<Option<GcRequest>>,
) {
    while requests.changed().await.is_ok() {
        let Some(mut request) = requests.borrow_and_update().clone() else {
            continue;
        };
        loop {
            match run_gc_request(Arc::clone(&store), request.clone()).await {
                Ok(()) => break,
                Err(error) => {
                    warn!(%error, retry_delay = ?RETENTION_RETRY_DELAY, "checkpoint retention paused at its durable cursor");
                }
            }
            if !request.authority.can_retry() {
                break;
            }
            tokio::select! {
                changed = requests.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let Some(next) = requests.borrow_and_update().clone() else {
                        break;
                    };
                    request = next;
                }
                () = tokio::time::sleep(RETENTION_RETRY_DELAY) => {}
            }
        }
    }
}

pub struct CheckpointCoordinator {
    config: CheckpointConfig,
    store: Arc<dyn CheckpointStore>,
    allocator: Arc<EpochAllocator>,
    phase: CheckpointPhase,
    sinks: Vec<RegisteredSink>,
    assignment_version: u64,
    assignment_scoped_sources: HashSet<String>,
    owned_vnodes: Vec<u32>,
    pipeline_identity: Option<PipelineIdentity>,
    deployment_id: Option<String>,
    decision_store: Option<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
    active_sink_witness: Option<laminar_core::checkpoint_decision::CheckpointSinkOpenWitness>,
    prepared: HashMap<CheckpointAttempt, (Arc<CheckpointManifest>, Bytes)>,
    last_committed_manifest: Option<Arc<CheckpointManifest>>,
    last_committed_ref: Option<CommittedCheckpointRef>,
    failure_requires_recovery: bool,
    local_watermark: laminar_core::checkpoint::CheckpointWatermark,
    #[cfg(feature = "cluster")]
    cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    checkpoints_completed: u64,
    checkpoints_failed: u64,
    last_checkpoint_duration: Option<Duration>,
    duration_histogram: DurationHistogram,
    total_bytes_written: u64,
    prom: Option<Arc<crate::engine_metrics::EngineMetrics>>,
    gc_requests: tokio::sync::watch::Sender<Option<GcRequest>>,
    gc_task: tokio::task::JoinHandle<()>,
}

impl CheckpointCoordinator {
    pub fn new(config: CheckpointConfig, store: Box<dyn CheckpointStore>) -> Result<Self, DbError> {
        laminar_core::checkpoint::checkpoint_store::validate_max_checkpoint_node_data_bytes(
            config.max_node_data_bytes,
        )
        .map_err(|error| DbError::Config(format!("checkpoint.max_node_data_bytes: {error}")))?;
        if store.max_node_data_bytes() != config.max_node_data_bytes {
            return Err(DbError::Config(format!(
                "checkpoint store node-data limit {} does not match checkpoint.max_node_data_bytes {}",
                store.max_node_data_bytes(),
                config.max_node_data_bytes
            )));
        }
        if store.participant_id() == 0 {
            return Err(DbError::Config(
                "checkpoint participant ID must be nonzero".into(),
            ));
        }
        let vnode_count = store.key_group_count().get();
        let store: Arc<dyn CheckpointStore> = Arc::from(store);
        let (gc_requests, gc_receiver) = tokio::sync::watch::channel(None);
        let gc_task = tokio::spawn(run_gc_worker(Arc::clone(&store), gc_receiver));
        Ok(Self {
            allocator: Arc::new(EpochAllocator::new()),
            config,
            store,
            phase: CheckpointPhase::Idle,
            sinks: Vec::new(),
            assignment_version: 0,
            assignment_scoped_sources: HashSet::new(),
            owned_vnodes: (0..u32::from(vnode_count)).collect(),
            pipeline_identity: None,
            deployment_id: None,
            decision_store: None,
            active_sink_witness: None,
            prepared: HashMap::new(),
            last_committed_manifest: None,
            last_committed_ref: None,
            failure_requires_recovery: false,
            local_watermark: laminar_core::checkpoint::CheckpointWatermark::Uninitialized,
            #[cfg(feature = "cluster")]
            cluster_controller: None,
            checkpoints_completed: 0,
            checkpoints_failed: 0,
            last_checkpoint_duration: None,
            duration_histogram: DurationHistogram::new(),
            total_bytes_written: 0,
            prom: None,
            gc_requests,
            gc_task,
        })
    }

    pub fn set_decision_store(
        &mut self,
        store: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<(), DbError> {
        self.allocator.bind_decision_store(Arc::clone(&store))?;
        self.decision_store = Some(store);
        Ok(())
    }

    pub async fn bind_durable_decision_store(
        &mut self,
        store: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<(), DbError> {
        let deployment_id = store
            .load_or_create_deployment_id()
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!("load checkpoint deployment identity: {error}"))
            })?;
        self.set_decision_store(store)?;
        self.bind_deployment_id(deployment_id)
    }

    pub(crate) fn bind_pipeline_identity(
        &mut self,
        identity: PipelineIdentity,
    ) -> Result<(), DbError> {
        match self.pipeline_identity.as_ref() {
            None => {
                self.pipeline_identity = Some(identity);
                Ok(())
            }
            Some(current) if current == &identity => Ok(()),
            Some(_) => Err(DbError::Checkpoint(
                "pipeline identity cannot change while checkpointing is active".into(),
            )),
        }
    }

    pub(crate) fn bound_pipeline_identity(&self) -> Result<PipelineIdentity, DbError> {
        self.pipeline_identity.clone().ok_or_else(|| {
            DbError::Checkpoint("checkpoint coordinator has no pipeline identity".into())
        })
    }

    pub(crate) fn bind_deployment_id(&mut self, deployment_id: String) -> Result<(), DbError> {
        let canonical = uuid::Uuid::parse_str(&deployment_id)
            .is_ok_and(|id| !id.is_nil() && id.to_string() == deployment_id);
        if !canonical {
            return Err(DbError::Checkpoint(
                "checkpoint deployment identity must be a canonical non-nil UUID".into(),
            ));
        }
        match self.deployment_id.as_ref() {
            None => {
                self.deployment_id = Some(deployment_id);
                Ok(())
            }
            Some(current) if current == &deployment_id => Ok(()),
            Some(_) => Err(DbError::Checkpoint(
                "deployment identity cannot change while checkpointing is active".into(),
            )),
        }
    }

    fn expected_pipeline_identity(&self) -> Result<PipelineIdentity, DbError> {
        self.bound_pipeline_identity()
    }

    fn expected_deployment_id(&self) -> Result<&str, DbError> {
        self.deployment_id.as_deref().ok_or_else(|| {
            DbError::Checkpoint("checkpoint deployment identity is not bound".into())
        })
    }

    #[cfg(feature = "cluster")]
    pub fn set_cluster_controller(
        &mut self,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
    ) {
        self.cluster_controller = Some(controller);
    }

    #[cfg(feature = "cluster")]
    #[must_use]
    pub(crate) fn participant_id(&self) -> u64 {
        self.store.participant_id()
    }

    pub fn set_assignment_version(&mut self, version: u64) {
        self.assignment_version = version;
    }

    pub(crate) fn set_assignment_scoped_sources(
        &mut self,
        sources: impl IntoIterator<Item = String>,
    ) {
        self.assignment_scoped_sources = sources.into_iter().collect();
    }

    pub fn set_vnode_set(&mut self, mut vnodes: Vec<u32>) {
        vnodes.sort_unstable();
        vnodes.dedup();
        self.owned_vnodes = vnodes;
    }

    pub fn set_local_watermark(
        &mut self,
        watermark: laminar_core::checkpoint::CheckpointWatermark,
    ) {
        self.local_watermark = watermark;
    }

    pub fn set_metrics(&mut self, prom: Arc<crate::engine_metrics::EngineMetrics>) {
        self.prom = Some(prom);
    }

    pub(crate) fn register_sink(
        &mut self,
        name: impl Into<String>,
        handle: crate::sink_task::SinkTaskHandle,
    ) {
        self.sinks.push(RegisteredSink {
            name: name.into(),
            handle,
        });
    }

    pub(crate) fn committable_sink_names(&self) -> Result<Vec<String>, DbError> {
        let mut names = self
            .sinks
            .iter()
            .filter(|sink| sink.handle.checkpoint_committable())
            .map(|sink| sink.name.clone())
            .collect::<Vec<_>>();
        names.sort_unstable();
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(DbError::Checkpoint(
                "checkpoint-committable sink names must be unique".into(),
            ));
        }
        Ok(names)
    }

    fn sorted_sink_names(&self) -> Result<Vec<String>, DbError> {
        let mut names = self
            .sinks
            .iter()
            .map(|sink| sink.name.clone())
            .collect::<Vec<_>>();
        names.sort_unstable();
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(DbError::Checkpoint("sink names must be unique".into()));
        }
        Ok(names)
    }

    fn has_checkpoint_committable_sinks(&self) -> bool {
        self.sinks
            .iter()
            .any(|sink| sink.handle.checkpoint_committable())
    }

    pub(crate) fn clear_sinks(&mut self) -> Result<(), DbError> {
        if self.active_sink_witness.is_some()
            || self.allocator.sink_epoch_reservation.lock().is_some()
        {
            return Err(DbError::Checkpoint(
                "cannot clear sinks while a sink epoch remains open".into(),
            ));
        }
        self.sinks.clear();
        Ok(())
    }

    fn schedule_retention(
        &self,
        current: CommittedCheckpointIndex,
        leader_proof: Option<LeaderProof>,
    ) {
        let Some(decision_store) = self.decision_store.as_ref() else {
            return;
        };
        let authority = match current.scope {
            CheckpointScope::Local => GcAuthority::Local,
            CheckpointScope::Cluster => {
                #[cfg(feature = "cluster")]
                {
                    let Some(proof) = leader_proof else {
                        warn!("cluster checkpoint retention has no live leader proof");
                        return;
                    };
                    let Some(controller) = self.cluster_controller.as_ref() else {
                        warn!("cluster checkpoint retention has no cluster controller");
                        return;
                    };
                    let authority = match controller.checkpoint_authority() {
                        Ok(authority) => authority,
                        Err(error) => {
                            warn!(%error, "cluster checkpoint retention authority is unavailable");
                            return;
                        }
                    };
                    GcAuthority::Cluster {
                        authority,
                        proof,
                        controller: Arc::downgrade(controller),
                    }
                }
                #[cfg(not(feature = "cluster"))]
                {
                    let _ = leader_proof;
                    warn!("cluster checkpoint retention requires the cluster feature");
                    return;
                }
            }
        };
        if self
            .gc_requests
            .send(Some(GcRequest {
                requested: Some(current),
                decision_store: Arc::clone(decision_store),
                authority,
            }))
            .is_err()
        {
            warn!("checkpoint retention worker is unavailable");
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn schedule_cluster_retention_resume(
        &self,
        proof: LeaderProof,
    ) -> Result<(), DbError> {
        let decision_store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("cluster checkpoint retention has no decision store".into())
        })?;
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint("cluster checkpoint retention has no controller".into())
        })?;
        if !controller.proof_is_live(&proof) {
            return Err(DbError::Checkpoint(
                "cluster checkpoint retention leader proof is no longer live".into(),
            ));
        }
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!("cluster checkpoint retention authority: {error}"))
        })?;
        let requested = self
            .gc_requests
            .borrow()
            .as_ref()
            .and_then(|request| request.requested.clone());
        self.gc_requests
            .send(Some(GcRequest {
                requested,
                decision_store: Arc::clone(decision_store),
                authority: GcAuthority::Cluster {
                    authority,
                    proof,
                    controller: Arc::downgrade(controller),
                },
            }))
            .map_err(|_| DbError::Checkpoint("checkpoint retention worker is unavailable".into()))
    }

    #[must_use]
    pub fn phase(&self) -> CheckpointPhase {
        self.phase
    }

    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.allocator.peek_epoch()
    }

    #[must_use]
    pub fn config(&self) -> &CheckpointConfig {
        &self.config
    }

    #[must_use]
    pub(crate) fn epoch_allocator(&self) -> Arc<EpochAllocator> {
        Arc::clone(&self.allocator)
    }

    #[must_use]
    pub fn store(&self) -> &dyn CheckpointStore {
        self.store.as_ref()
    }

    fn recovery_scope(&self) -> CheckpointScope {
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            return CheckpointScope::Cluster;
        }
        CheckpointScope::Local
    }

    fn manifest_watermark(
        manifest: Option<&CheckpointManifest>,
    ) -> Result<laminar_core::checkpoint::CheckpointWatermark, DbError> {
        let Some(manifest) = manifest else {
            return Ok(laminar_core::checkpoint::CheckpointWatermark::Uninitialized);
        };
        classify_channel_progress(&manifest.channel_progress)
            .map_err(|error| DbError::Checkpoint(format!("recovered channel progress: {error}")))
    }

    async fn install_recovered_cut(
        &mut self,
        outcome: laminar_core::checkpoint_decision::CheckpointOutcome,
        committed: CommittedCheckpointIndex,
        deadline: tokio::time::Instant,
    ) -> Result<crate::recovery_manager::RecoveredState, DbError> {
        let pipeline_identity = self.expected_pipeline_identity()?;
        let deployment_id = self.expected_deployment_id()?.to_owned();
        let recovered = {
            let manager = crate::recovery_manager::RecoveryManager::new(
                self.store.as_ref(),
                &pipeline_identity,
                &deployment_id,
                self.recovery_scope(),
            );
            tokio::time::timeout_at(deadline, manager.recover_committed(&outcome, &committed))
                .await
                .map_err(|_| DbError::Checkpoint("checkpoint recovery timed out".into()))??
        };

        #[cfg(feature = "cluster")]
        let continuation_proof = if committed.scope == CheckpointScope::Cluster {
            self.cluster_controller
                .as_ref()
                .and_then(|controller| controller.capture_leader_proof())
        } else {
            None
        };
        #[cfg(not(feature = "cluster"))]
        let continuation_proof: Option<LeaderProof> = None;
        let continuation_fencing_token = match committed.scope {
            CheckpointScope::Local => Some(1),
            CheckpointScope::Cluster => {
                #[cfg(feature = "cluster")]
                {
                    continuation_proof.as_ref().map(|proof| proof.fencing_token)
                }
                #[cfg(not(feature = "cluster"))]
                {
                    None
                }
            }
        };
        if let Some(fencing_token) = continuation_fencing_token {
            let manifests = recovered.manifests.iter().collect::<Vec<_>>();
            self.commit_external_sinks_until(
                CheckpointAttempt::canonical(committed.checkpoint_id),
                &manifests,
                fencing_token,
                committed
                    .predecessor
                    .as_ref()
                    .map_or(0, |reference| reference.checkpoint_id),
                deadline,
            )
            .await?;
            self.schedule_retention(committed.clone(), continuation_proof);
        }

        let reference = outcome.committed_checkpoint.clone().ok_or_else(|| {
            DbError::Checkpoint("Commit outcome has no committed checkpoint reference".into())
        })?;
        let local_manifest = recovered
            .manifests
            .iter()
            .find(|manifest| manifest.participant_id == self.store.participant_id())
            .cloned()
            .map(Arc::new);
        self.local_watermark = Self::manifest_watermark(local_manifest.as_deref())?;
        self.last_committed_manifest = local_manifest;
        self.last_committed_ref = Some(reference);
        self.prepared.clear();
        self.allocator.advance_epoch_to(checked_successor_epoch(
            committed.epoch,
            "installing recovered checkpoint",
        )?);
        self.failure_requires_recovery = false;
        self.phase = CheckpointPhase::Idle;
        #[cfg(feature = "cluster")]
        if let (Some(controller), Some(watermark)) = (
            self.cluster_controller.as_ref(),
            committed.checkpoint_watermark,
        ) {
            controller.publish_cluster_min_watermark(watermark);
        }
        Ok(recovered)
    }

    pub async fn recover(
        &mut self,
    ) -> Result<Option<crate::recovery_manager::RecoveredState>, DbError> {
        if self.phase != CheckpointPhase::Idle {
            return Err(DbError::Checkpoint(
                "cannot recover while a checkpoint is in progress".into(),
            ));
        }
        let deadline = tokio::time::Instant::now() + self.config.checkpoint_timeout;

        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.as_ref() {
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
            })?;
            let latest_terminal =
                tokio::time::timeout_at(deadline, authority.highest_cluster_terminal_outcome())
                    .await
                    .map_err(|_| DbError::Checkpoint("cluster terminal read timed out".into()))?
                    .map_err(|error| {
                        DbError::Checkpoint(format!("cluster terminal read failed: {error}"))
                    })?;
            if let Some(outcome) = latest_terminal.filter(|outcome| !outcome.is_commit()) {
                self.delete_prepared_artifact_until(
                    CheckpointAttempt::new(outcome.epoch, outcome.checkpoint_id),
                    deadline,
                )
                .await?;
            }
            let Some(selected) =
                tokio::time::timeout_at(deadline, authority.highest_cluster_committed_outcome())
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint("cluster recovery selection timed out".into())
                    })?
                    .map_err(|error| {
                        DbError::Checkpoint(format!("cluster recovery selection: {error}"))
                    })?
            else {
                return Ok(None);
            };
            let (outcome, committed) = tokio::time::timeout_at(
                deadline,
                authority.cluster_outcome_with_committed_checkpoint(selected.epoch),
            )
            .await
            .map_err(|_| DbError::Checkpoint("cluster checkpoint read timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("cluster checkpoint read: {error}")))?
            .ok_or_else(|| DbError::Checkpoint("selected cluster checkpoint disappeared".into()))?;
            if outcome != selected {
                return Err(DbError::Checkpoint(
                    "cluster recovery selection changed during exact read".into(),
                ));
            }
            let committed = committed.ok_or_else(|| {
                DbError::Checkpoint("selected cluster Commit has no checkpoint index".into())
            })?;
            return self
                .install_recovered_cut(outcome, committed, deadline)
                .await
                .map(Some);
        }

        let store = Arc::clone(self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("checkpoint recovery requires a decision store".into())
        })?);
        let head = tokio::time::timeout_at(deadline, store.checkpoint_decision_head())
            .await
            .map_err(|_| DbError::Checkpoint("checkpoint recovery selection timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("checkpoint recovery selection: {error}"))
            })?;
        if let Some(outcome) = head
            .as_ref()
            .map(|head| &head.latest_terminal)
            .filter(|outcome| !outcome.is_commit())
        {
            self.delete_prepared_artifact_until(
                CheckpointAttempt::new(outcome.epoch, outcome.checkpoint_id),
                deadline,
            )
            .await?;
        }
        let Some(outcome) = head.and_then(|head| head.latest_commit) else {
            return Ok(None);
        };
        let reference = outcome.committed_checkpoint.as_ref().ok_or_else(|| {
            DbError::Checkpoint("selected Commit has no checkpoint index reference".into())
        })?;
        let committed =
            tokio::time::timeout_at(deadline, store.load_committed_checkpoint(reference))
                .await
                .map_err(|_| DbError::Checkpoint("committed checkpoint read timed out".into()))?
                .map_err(|error| {
                    DbError::Checkpoint(format!("committed checkpoint read: {error}"))
                })?;
        self.install_recovered_cut(outcome, committed, deadline)
            .await
            .map(Some)
    }

    #[cfg(feature = "cluster")]
    pub async fn recover_to_epoch(
        &mut self,
        epoch: u64,
    ) -> Result<Option<crate::recovery_manager::RecoveredState>, DbError> {
        if epoch == 0 {
            return Ok(None);
        }
        if self.phase != CheckpointPhase::Idle {
            return Err(DbError::Checkpoint(
                "cannot recover while a checkpoint is in progress".into(),
            ));
        }
        let deadline = tokio::time::Instant::now() + self.config.checkpoint_timeout;
        if let Some(controller) = self.cluster_controller.as_ref() {
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
            })?;
            let (outcome, committed) = tokio::time::timeout_at(
                deadline,
                authority.cluster_outcome_with_committed_checkpoint(epoch),
            )
            .await
            .map_err(|_| DbError::Checkpoint("cluster checkpoint read timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("cluster checkpoint read: {error}")))?
            .ok_or_else(|| DbError::Checkpoint(format!("cluster epoch {epoch} has no outcome")))?;
            let committed = committed.ok_or_else(|| {
                DbError::Checkpoint(format!("cluster epoch {epoch} is not committed"))
            })?;
            return self
                .install_recovered_cut(outcome, committed, deadline)
                .await
                .map(Some);
        }

        Err(DbError::Checkpoint(
            "epoch-targeted recovery requires cluster checkpoint authority".into(),
        ))
    }

    async fn create_sink_witness_until(
        &self,
        attempt: CheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Result<Option<laminar_core::checkpoint_decision::CheckpointSinkOpenWitness>, DbError> {
        let names = self.committable_sink_names()?;
        if names.is_empty() {
            return Ok(None);
        }
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            return Ok(None);
        }
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("committable sinks require a decision store".into())
        })?;
        tokio::time::timeout_at(
            deadline,
            store.create_sink_open_witness(
                self.expected_pipeline_identity()?,
                self.store.participant_id(),
                attempt,
                names,
            ),
        )
        .await
        .map_err(|_| DbError::Checkpoint("sink-open witness create timed out".into()))?
        .map(Some)
        .map_err(|error| DbError::Checkpoint(format!("sink-open witness create: {error}")))
    }

    async fn clear_sink_witness_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let Some(witness) = self.active_sink_witness.clone() else {
            return Ok(());
        };
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("sink-open witness cleanup requires a decision store".into())
        })?;
        tokio::time::timeout_at(deadline, store.clear_sink_open_witness(&witness))
            .await
            .map_err(|_| DbError::Checkpoint("sink-open witness cleanup timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("sink-open witness cleanup failed: {error}"))
            })?;
        self.active_sink_witness = None;
        Ok(())
    }

    async fn begin_sink_epoch_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        if !self.has_checkpoint_committable_sinks() {
            return Ok(());
        }
        let attempt = self.allocator.reserve_sink_epoch_until(deadline).await?;
        let witness = match self.create_sink_witness_until(attempt, deadline).await {
            Ok(witness) => witness,
            Err(error) => {
                self.allocator.clear_sink_epoch(attempt);
                return Err(error);
            }
        };
        self.active_sink_witness = witness;

        let results = futures::future::join_all(
            self.sinks
                .iter()
                .filter(|sink| sink.handle.checkpoint_committable())
                .map(|sink| {
                    let name = sink.name.clone();
                    let handle = sink.handle.clone();
                    async move {
                        (
                            name,
                            handle.begin_epoch_until(attempt.epoch, deadline).await,
                        )
                    }
                }),
        )
        .await;
        let failures = results
            .into_iter()
            .filter_map(|(name, result)| result.err().map(|error| format!("{name}: {error}")))
            .collect::<Vec<_>>();
        if failures.is_empty() {
            self.allocator.mark_sink_epoch_ready(attempt)?;
            return Ok(());
        }

        if let Err(rollback) = self.rollback_sinks_until(attempt.epoch, deadline).await {
            self.allocator.mark_sink_epoch_in_doubt(attempt);
            self.failure_requires_recovery = true;
            return Err(DbError::Checkpoint(format!(
                "sink epoch {} failed to open ({}) and rollback failed ({rollback})",
                attempt.epoch,
                failures.join("; ")
            )));
        }
        self.clear_sink_witness_until(deadline).await?;
        self.allocator.clear_sink_epoch(attempt);
        Err(DbError::Checkpoint(format!(
            "sink epoch {} failed to open: {}",
            attempt.epoch,
            failures.join("; ")
        )))
    }

    pub async fn begin_initial_epoch(&mut self) -> Result<(), DbError> {
        let deadline = tokio::time::Instant::now() + self.config.checkpoint_timeout;
        self.begin_sink_epoch_until(deadline).await
    }

    async fn allocate_attempt_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        if self.has_checkpoint_committable_sinks() {
            self.allocator.consume_sink_epoch_until(deadline).await
        } else {
            self.allocator.allocate_until(deadline).await
        }
    }

    async fn rollback_sinks_until(
        &self,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let results = futures::future::join_all(
            self.sinks
                .iter()
                .filter(|sink| sink.handle.checkpoint_committable())
                .map(|sink| {
                    let name = sink.name.clone();
                    let handle = sink.handle.clone();
                    async move { (name, handle.rollback_epoch_until(epoch, deadline).await) }
                }),
        )
        .await;
        let failures = results
            .into_iter()
            .filter_map(|(name, result)| result.err().map(|error| format!("{name}: {error}")))
            .collect::<Vec<_>>();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(DbError::Checkpoint(format!(
                "sink rollback failed: {}",
                failures.join("; ")
            )))
        }
    }

    pub(crate) async fn reconcile_sink_open_witness(&mut self) -> Result<(), DbError> {
        self.reconcile_sink_open_witness_until(
            tokio::time::Instant::now() + self.config.cleanup_timeout,
        )
        .await
    }

    pub(crate) async fn reconcile_sink_open_witness_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            return Ok(());
        }
        let Some(store) = self.decision_store.as_ref() else {
            return Ok(());
        };
        let Some(witness) = tokio::time::timeout_at(deadline, store.sink_open_witness())
            .await
            .map_err(|_| DbError::Checkpoint("sink-open witness read timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("sink-open witness read: {error}")))?
        else {
            return Ok(());
        };
        if witness.pipeline_identity != self.expected_pipeline_identity()?
            || witness.deployment_id != self.expected_deployment_id()?
            || witness.participant_id != self.store.participant_id()
            || witness.committable_sinks != self.committable_sink_names()?
        {
            return Err(DbError::Checkpoint(
                "sink-open witness does not match the running checkpoint namespace".into(),
            ));
        }
        let head = tokio::time::timeout_at(deadline, store.checkpoint_decision_head())
            .await
            .map_err(|_| DbError::Checkpoint("sink-open outcome read timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("sink-open outcome read: {error}")))?;
        match head.map(|head| head.latest_terminal) {
            Some(outcome) if outcome.epoch > witness.attempt.epoch => {
                return Err(DbError::Checkpoint(
                    "sink-open witness remained open past a newer terminal outcome".into(),
                ));
            }
            Some(outcome) if outcome.epoch == witness.attempt.epoch => {
                if outcome.checkpoint_id != witness.attempt.checkpoint_id {
                    return Err(DbError::Checkpoint(
                        "sink-open witness conflicts with its terminal outcome".into(),
                    ));
                }
                if !outcome.is_commit() {
                    self.rollback_sinks_until(witness.attempt.epoch, deadline)
                        .await?;
                }
            }
            Some(_) | None => {
                self.rollback_sinks_until(witness.attempt.epoch, deadline)
                    .await?;
            }
        }
        tokio::time::timeout_at(deadline, store.clear_sink_open_witness(&witness))
            .await
            .map_err(|_| DbError::Checkpoint("sink-open witness cleanup timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("sink-open witness cleanup: {error}")))?;
        self.allocator.advance_epoch_to(checked_successor_epoch(
            witness.attempt.epoch,
            "reconciling sink-open ownership",
        )?);
        self.failure_requires_recovery = false;
        Ok(())
    }

    async fn pre_commit_sinks_until(
        &self,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<BTreeMap<String, Option<Vec<u8>>>, DbError> {
        let mut pending = self.sinks.iter();
        let mut active = FuturesUnordered::new();
        for sink in pending.by_ref().take(MAX_SINK_PHASE_ONE_CONCURRENCY) {
            active.push(Self::sink_phase_one(sink, epoch, deadline));
        }
        let mut descriptors = BTreeMap::new();
        let mut descriptor_bytes = 0usize;
        let mut first_error = None;
        while let Some(result) = active.next().await {
            match result {
                Ok(Some((name, payload))) if first_error.is_none() => {
                    descriptor_bytes = descriptor_bytes
                        .checked_add(payload.as_ref().map_or(0, Vec::len))
                        .ok_or_else(|| {
                            DbError::Checkpoint("sink descriptor byte count overflow".into())
                        })?;
                    if descriptor_bytes > MAX_COORDINATED_COMMIT_BATCH_BYTES {
                        first_error = Some(DbError::Checkpoint(format!(
                            "sink descriptors exceed {MAX_COORDINATED_COMMIT_BATCH_BYTES} bytes"
                        )));
                    } else {
                        descriptors.insert(name, payload);
                        if let Some(sink) = pending.next() {
                            active.push(Self::sink_phase_one(sink, epoch, deadline));
                        }
                    }
                }
                Ok(None) if first_error.is_none() => {
                    if let Some(sink) = pending.next() {
                        active.push(Self::sink_phase_one(sink, epoch, deadline));
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        first_error.map_or(Ok(descriptors), Err)
    }

    async fn sink_phase_one(
        sink: &RegisteredSink,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<Option<(String, Option<Vec<u8>>)>, DbError> {
        if sink.handle.checkpoint_committable() {
            let payload = sink
                .handle
                .pre_commit_until(epoch, deadline)
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!("sink '{}' pre-commit failed: {error}", sink.name))
                })?;
            if payload
                .as_ref()
                .is_some_and(|payload| payload.len() > MAX_COORDINATED_COMMIT_PAYLOAD_BYTES)
            {
                return Err(DbError::Checkpoint(format!(
                    "sink '{}' descriptor exceeds {MAX_COORDINATED_COMMIT_PAYLOAD_BYTES} bytes",
                    sink.name
                )));
            }
            Ok(Some((sink.name.clone(), payload)))
        } else {
            sink.handle.flush_until(deadline).await.map_err(|error| {
                DbError::Checkpoint(format!("sink '{}' flush failed: {error}", sink.name))
            })?;
            Ok(None)
        }
    }

    fn validate_request(&self, request: &CheckpointRequest) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        let vnode_count = u32::from(self.store.key_group_count().get());
        #[cfg(feature = "cluster")]
        match (
            self.cluster_controller.as_ref(),
            request.assignment_fence.as_ref(),
        ) {
            (None, None) => {}
            (None, Some(_)) => {
                return Err(DbError::Checkpoint(
                    "local checkpoint received an assignment fence".into(),
                ));
            }
            (Some(_), None) => {
                return Err(DbError::Checkpoint(
                    "cluster checkpoint is missing its assignment fence".into(),
                ));
            }
            (Some(controller), Some(fence)) => {
                if !fence.is_canonical()
                    || !fence.contains(self.store.participant_id())
                    || fence.vnode_count != vnode_count
                    || fence.assignment_version != self.assignment_version
                    || controller
                        .checkpoint_assignment_fence(fence.assignment_version)
                        .as_ref()
                        != Some(fence)
                {
                    return Err(DbError::Checkpoint(
                        "checkpoint assignment fence is stale or incompatible".into(),
                    ));
                }
            }
        }
        #[cfg(not(feature = "cluster"))]
        if request.assignment_fence.is_some() {
            return Err(DbError::Checkpoint(
                "local checkpoint received an assignment fence".into(),
            ));
        }

        for source in &self.assignment_scoped_sources {
            let checkpoint = request.source_offset_overrides.get(source).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "assignment-scoped source '{source}' has no captured offset"
                ))
            })?;
            let expected = request
                .assignment_fence
                .as_ref()
                .map_or(self.assignment_version, |fence| fence.assignment_version);
            if checkpoint
                .source_assignment_version
                .map(std::num::NonZeroU64::get)
                != Some(expected)
            {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' offset is not bound to assignment {expected}"
                )));
            }
        }
        if let Some((source, version)) = request
            .source_offset_overrides
            .iter()
            .filter(|(source, _)| !self.assignment_scoped_sources.contains(*source))
            .find_map(|(source, checkpoint)| {
                checkpoint
                    .source_assignment_version
                    .map(|version| (source, version))
            })
        {
            return Err(DbError::Checkpoint(format!(
                "non-assignment source '{source}' carries assignment version {version}"
            )));
        }
        Ok(())
    }

    fn prior_chunk_metadata(
        prior: &CheckpointManifest,
        chunk: StateChunkId,
    ) -> Option<(u64, String)> {
        if prior.node_data.chunk == chunk {
            return Some((
                prior.node_data.object_length,
                prior.node_data.sha256.clone(),
            ));
        }
        prior
            .referenced_chunks
            .iter()
            .find(|reference| reference.chunk == chunk)
            .map(|reference| (reference.object_length, reference.sha256.clone()))
    }

    fn validate_capture_roster(&self, captures: &[CapturedStateFrame]) -> Result<(), DbError> {
        if captures.windows(2).any(|pair| pair[0].key >= pair[1].key) {
            return Err(DbError::Checkpoint(
                "captured state frames must be strictly ordered and unique".into(),
            ));
        }
        if captures.iter().any(|capture| {
            matches!(capture.key, StateFrameKey::OperatorWhole { .. }) && capture.state.is_none()
        }) {
            return Err(DbError::Checkpoint(
                "whole-operator state must carry its current payload".into(),
            ));
        }
        Ok(())
    }

    async fn pack_checkpoint(
        &self,
        attempt: CheckpointAttempt,
        mut request: CheckpointRequest,
        sink_payloads: BTreeMap<String, Option<Vec<u8>>>,
    ) -> Result<PackedCheckpoint, DbError> {
        self.validate_request(&request)?;
        request
            .state_frames
            .sort_unstable_by(|left, right| left.key.cmp(&right.key));
        self.validate_capture_roster(&request.state_frames)?;
        for channel in &mut request.channel_progress {
            channel.participant_id = self.store.participant_id();
        }
        request.channel_progress.sort_unstable_by(|left, right| {
            (left.participant_id, left.channel_id.as_str())
                .cmp(&(right.participant_id, right.channel_id.as_str()))
        });
        if request.channel_progress.windows(2).any(|pair| {
            pair[0].participant_id == pair[1].participant_id
                && pair[0].channel_id == pair[1].channel_id
        }) {
            return Err(DbError::Checkpoint(
                "channel progress contains duplicate channel identities".into(),
            ));
        }

        let expected_sinks = self.committable_sink_names()?;
        if !sink_payloads
            .keys()
            .map(String::as_str)
            .eq(expected_sinks.iter().map(String::as_str))
        {
            return Err(DbError::Checkpoint(
                "phase one did not produce exactly one descriptor per committable sink".into(),
            ));
        }

        let current_chunk = StateChunkId {
            participant_id: self.store.participant_id(),
            checkpoint_id: attempt.checkpoint_id,
        };
        let mut node_data = Vec::new();
        let mut object_length = 0;
        let mut frames = Vec::new();
        let mut current_frame_chunks = Vec::new();
        let mut referenced = BTreeMap::<StateChunkId, (u64, String, u32)>::new();

        for capture in request.state_frames {
            if let Some(bytes) = capture.state {
                let length = u64::try_from(bytes.len()).map_err(|_| {
                    DbError::Checkpoint(format!("state frame {:?} length exceeds u64", capture.key))
                })?;
                if length == 0 {
                    return Err(DbError::Checkpoint(format!(
                        "state frame {:?} has an empty payload",
                        capture.key
                    )));
                }
                let range = ByteRange {
                    offset: object_length,
                    length,
                };
                object_length = range.end().ok_or_else(|| {
                    DbError::Checkpoint("checkpoint node-data length overflow".into())
                })?;
                node_data.push(bytes);
                let node_data_index = node_data.len() - 1;
                let frame_index = frames.len();
                frames.push(StateFrame {
                    key: capture.key,
                    chunk: current_chunk,
                    range,
                    sha256: String::new(),
                });
                current_frame_chunks.push((frame_index, node_data_index));
            } else {
                let prior = self.last_committed_manifest.as_ref().ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "unchanged state frame {:?} has no committed predecessor",
                        capture.key
                    ))
                })?;
                let frame = prior
                    .state_frames
                    .iter()
                    .find(|frame| frame.key == capture.key)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "unchanged state frame {:?} is absent from its committed predecessor",
                            capture.key
                        ))
                    })?
                    .clone();
                let (length, digest) =
                    Self::prior_chunk_metadata(prior, frame.chunk).ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "predecessor frame {:?} references untracked object {:?}",
                            frame.key, frame.chunk
                        ))
                    })?;
                let entry = referenced
                    .entry(frame.chunk)
                    .or_insert((length, digest.clone(), 0));
                if entry.0 != length || entry.1 != digest {
                    return Err(DbError::Checkpoint(format!(
                        "conflicting metadata for referenced object {:?}",
                        frame.chunk
                    )));
                }
                entry.2 = entry
                    .2
                    .checked_add(1)
                    .ok_or_else(|| DbError::Checkpoint("referenced frame count overflow".into()))?;
                frames.push(frame);
            }
        }

        let mut prepared_sinks = Vec::with_capacity(sink_payloads.len());
        let mut prepared_sink_chunks = Vec::new();
        for (sink_name, payload) in &sink_payloads {
            let (range, digest) = match payload {
                None => (None, checkpoint_descriptor_sha256(None)),
                Some(payload) => {
                    let length = u64::try_from(payload.len()).map_err(|_| {
                        DbError::Checkpoint(format!(
                            "sink '{sink_name}' descriptor length exceeds u64"
                        ))
                    })?;
                    let range = ByteRange {
                        offset: object_length,
                        length,
                    };
                    object_length = range.end().ok_or_else(|| {
                        DbError::Checkpoint("checkpoint node-data length overflow".into())
                    })?;
                    node_data.push(Bytes::copy_from_slice(payload));
                    prepared_sink_chunks.push((prepared_sinks.len(), node_data.len() - 1));
                    (Some(range), String::new())
                }
            };
            prepared_sinks.push(PreparedSinkDescriptor {
                sink_name: sink_name.clone(),
                format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
                payload: range,
                sha256: digest,
            });
        }

        if object_length > self.config.max_node_data_bytes {
            return Err(DbError::Checkpoint(format!(
                "checkpoint node data is {object_length} bytes; limit is {}",
                self.config.max_node_data_bytes
            )));
        }
        let digest_chunks = node_data.clone();
        let (object_sha256, frame_digests, sink_digests) = tokio::task::spawn_blocking(move || {
            let mut object_digest = Sha256::new();
            for bytes in &digest_chunks {
                object_digest.update(bytes);
            }
            let frame_digests = current_frame_chunks
                .into_iter()
                .map(|(frame, chunk)| (frame, checkpoint_sha256(&digest_chunks[chunk])))
                .collect::<Vec<_>>();
            let sink_digests = prepared_sink_chunks
                .into_iter()
                .map(|(sink, chunk)| {
                    (
                        sink,
                        checkpoint_descriptor_sha256(Some(&digest_chunks[chunk])),
                    )
                })
                .collect::<Vec<_>>();
            (
                format!("{:x}", object_digest.finalize()),
                frame_digests,
                sink_digests,
            )
        })
        .await
        .map_err(|error| DbError::Checkpoint(format!("checkpoint digest task failed: {error}")))?;
        for (frame, digest) in frame_digests {
            frames[frame].sha256 = digest;
        }
        for (sink, digest) in sink_digests {
            prepared_sinks[sink].sha256 = digest;
        }

        let mut manifest = CheckpointManifest::new_with_key_group_count(
            attempt.checkpoint_id,
            attempt.epoch,
            self.store.key_group_count(),
        );
        manifest.bind_participant(self.store.participant_id());
        manifest.pipeline_identity = self.expected_pipeline_identity()?;
        self.expected_deployment_id()?
            .clone_into(&mut manifest.deployment_id);
        manifest.assignment_fence = request.assignment_fence;
        manifest.owned_vnodes = self
            .owned_vnodes
            .iter()
            .map(|vnode| {
                u16::try_from(*vnode).map_err(|_| {
                    DbError::Checkpoint(format!(
                        "owned vnode {vnode} exceeds the configured vnode ID space"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        manifest.source_offsets = request.source_offset_overrides;
        manifest.source_names = manifest.source_offsets.keys().cloned().collect();
        manifest.source_names.sort_unstable();
        manifest.sink_names = self.sorted_sink_names()?;
        manifest.channel_progress = request.channel_progress;
        manifest.checkpoint_watermark = classify_channel_progress(&manifest.channel_progress)
            .map_err(DbError::Checkpoint)?
            .active_value();
        manifest.node_data.object_length = object_length;
        manifest.node_data.sha256 = object_sha256;
        manifest.state_frames = frames;
        manifest.prepared_sinks = prepared_sinks;
        manifest.referenced_chunks = referenced
            .into_iter()
            .map(|(chunk, (object_length, sha256, count))| {
                Ok(ReferencedStateChunk {
                    chunk,
                    object_length,
                    sha256,
                    ref_count: NonZeroU32::new(count).ok_or_else(|| {
                        DbError::Checkpoint("referenced object has zero frame references".into())
                    })?,
                })
            })
            .collect::<Result<Vec<_>, DbError>>()?;
        let errors = manifest.validate(self.store.key_group_count());
        if !errors.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "checkpoint manifest validation: {}",
                errors
                    .into_iter()
                    .map(|error| error.to_string())
                    .collect::<Vec<_>>()
                    .join("; ")
            )));
        }
        Ok(PackedCheckpoint {
            manifest,
            node_data,
        })
    }

    async fn persist_checkpoint_until(
        &mut self,
        packed: &PackedCheckpoint,
        deadline: tokio::time::Instant,
    ) -> Result<Bytes, DbError> {
        self.phase = CheckpointPhase::Persisting;
        let persisted = tokio::time::timeout_at(
            deadline,
            self.store
                .save_checkpoint(&packed.manifest, &packed.node_data),
        )
        .await;
        let manifest_bytes = match persisted {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(error)) => {
                self.retain_ambiguous_prepared(packed)?;
                return Err(DbError::from(error));
            }
            Err(_) => {
                self.retain_ambiguous_prepared(packed)?;
                return Err(DbError::Checkpoint(
                    "checkpoint persistence timed out".into(),
                ));
            }
        };
        self.total_bytes_written = self
            .total_bytes_written
            .saturating_add(packed.manifest.node_data.object_length);
        self.prepared.insert(
            CheckpointAttempt::canonical(packed.manifest.checkpoint_id),
            (Arc::new(packed.manifest.clone()), manifest_bytes.clone()),
        );
        Ok(manifest_bytes)
    }

    fn retain_ambiguous_prepared(&mut self, packed: &PackedCheckpoint) -> Result<(), DbError> {
        let manifest_bytes = Bytes::from(checkpoint_manifest_bytes(&packed.manifest).map_err(
            |error| DbError::Checkpoint(format!("encode checkpoint manifest: {error}")),
        )?);
        self.prepared
            .entry(CheckpointAttempt::canonical(packed.manifest.checkpoint_id))
            .or_insert_with(|| (Arc::new(packed.manifest.clone()), manifest_bytes));
        Ok(())
    }

    fn prepared_chunk(&self, attempt: CheckpointAttempt) -> Result<StateChunkId, DbError> {
        let attempt = require_canonical_attempt(attempt, "prepared artifact cleanup")?;
        let expected = StateChunkId {
            participant_id: self.store.participant_id(),
            checkpoint_id: attempt.checkpoint_id,
        };
        if let Some((manifest, _)) = self.prepared.get(&attempt) {
            if manifest.epoch != attempt.epoch
                || manifest.checkpoint_id != attempt.checkpoint_id
                || manifest.participant_id != expected.participant_id
                || manifest.node_data.chunk != expected
            {
                return Err(DbError::Checkpoint(format!(
                    "prepared checkpoint {} does not match its local artifact identity",
                    attempt.checkpoint_id
                )));
            }
        }
        Ok(expected)
    }

    async fn delete_prepared_artifact_until(
        &mut self,
        attempt: CheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let chunk = self.prepared_chunk(attempt)?;
        tokio::time::timeout_at(deadline, self.store.delete_manifest(chunk))
            .await
            .map_err(|_| DbError::Checkpoint("prepared manifest cleanup timed out".into()))?
            .map_err(DbError::from)?;
        tokio::time::timeout_at(deadline, self.store.delete_node_data(chunk))
            .await
            .map_err(|_| DbError::Checkpoint("prepared node-data cleanup timed out".into()))?
            .map_err(DbError::from)?;
        self.prepared.remove(&attempt);
        Ok(())
    }

    async fn load_prepared_participant_manifests(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&laminar_core::checkpoint::CheckpointAssignmentFence>,
        local: (CheckpointManifest, Bytes),
        deadline: tokio::time::Instant,
    ) -> Result<Vec<(CheckpointManifest, Bytes)>, DbError> {
        let participant_ids = assignment_fence.map_or_else(
            || vec![self.store.participant_id()],
            laminar_core::checkpoint::CheckpointAssignmentFence::participant_ids,
        );
        if local.0.participant_id != self.store.participant_id()
            || !participant_ids.contains(&local.0.participant_id)
        {
            return Err(DbError::Checkpoint(
                "local participant is absent from the checkpoint assignment".into(),
            ));
        }
        let mut loaded = BTreeMap::from([(local.0.participant_id, local)]);
        let mut reads = participant_ids
            .into_iter()
            .filter(|participant_id| *participant_id != self.store.participant_id())
            .map(|participant_id| {
                let store = Arc::clone(&self.store);
                async move {
                    let manifest = tokio::time::timeout_at(
                        deadline,
                        store.load_manifest_for_participant(participant_id, attempt.checkpoint_id),
                    )
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(format!(
                            "participant {participant_id} manifest read timed out"
                        ))
                    })?
                    .map_err(DbError::from)?
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "durably prepared participant {participant_id} manifest is missing"
                        ))
                    })?;
                    let encoded =
                        Bytes::from(checkpoint_manifest_bytes(&manifest).map_err(|error| {
                            DbError::Checkpoint(format!(
                                "encode participant {participant_id} manifest: {error}"
                            ))
                        })?);
                    Ok::<_, DbError>((participant_id, manifest, encoded))
                }
            })
            .collect::<FuturesUnordered<_>>();
        while let Some(result) = reads.next().await {
            let (participant_id, manifest, encoded) = result?;
            loaded.insert(participant_id, (manifest, encoded));
        }
        Ok(loaded.into_values().collect())
    }

    fn merge_source_checkpoint(
        source: &str,
        destination: &mut ConnectorCheckpoint,
        incoming: &ConnectorCheckpoint,
    ) -> Result<(), DbError> {
        match (
            destination.source_assignment_version,
            incoming.source_assignment_version,
        ) {
            (None, None) => {}
            (Some(left), Some(right)) if left == right => {}
            _ => {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' participant offsets disagree on assignment version"
                )));
            }
        }
        for (key, value) in &incoming.offsets {
            if destination
                .offsets
                .insert(key.clone(), value.clone())
                .is_some_and(|previous| previous != *value)
            {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' has conflicting offset '{key}'"
                )));
            }
        }
        for (key, value) in &incoming.metadata {
            if destination
                .metadata
                .insert(key.clone(), value.clone())
                .is_some_and(|previous| previous != *value)
            {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' has conflicting metadata '{key}'"
                )));
            }
        }
        Ok(())
    }

    fn build_committed_index(
        &self,
        attempt: CheckpointAttempt,
        scope: CheckpointScope,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        manifests: &[(CheckpointManifest, Bytes)],
        quorum_watermark: Option<CheckpointWatermark>,
    ) -> Result<CommittedCheckpointIndex, DbError> {
        let mut participants = Vec::with_capacity(manifests.len());
        let mut source_offsets = BTreeMap::<String, ConnectorCheckpoint>::new();
        let mut channels = BTreeMap::<(u64, String), ChannelProgress>::new();
        for (manifest, encoded) in manifests {
            participants.push(
                CommittedParticipantRef::from_manifest(manifest, encoded)
                    .map_err(DbError::Checkpoint)?,
            );
            for (source, checkpoint) in &manifest.source_offsets {
                match source_offsets.entry(source.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(checkpoint.clone());
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        Self::merge_source_checkpoint(source, entry.get_mut(), checkpoint)?;
                    }
                }
            }
            for channel in &manifest.channel_progress {
                if channels
                    .insert(
                        (channel.participant_id, channel.channel_id.clone()),
                        channel.clone(),
                    )
                    .is_some()
                {
                    return Err(DbError::Checkpoint(format!(
                        "participant {} channel '{}' appears more than once",
                        channel.participant_id, channel.channel_id
                    )));
                }
            }
        }
        participants.sort_unstable_by_key(|participant| participant.participant_id);
        let channel_progress = channels.into_values().collect::<Vec<_>>();
        let classification =
            classify_channel_progress(&channel_progress).map_err(DbError::Checkpoint)?;
        if let Some(quorum_watermark) = quorum_watermark {
            if quorum_watermark != classification {
                return Err(DbError::Checkpoint(format!(
                    "checkpoint quorum watermark {quorum_watermark:?} does not match merged channel progress {classification:?}"
                )));
            }
        }
        let checkpoint_watermark = classification.active_value();
        let index = CommittedCheckpointIndex {
            version: COMMITTED_CHECKPOINT_INDEX_VERSION,
            deployment_id: self.expected_deployment_id()?.to_owned(),
            pipeline_identity: self.expected_pipeline_identity()?,
            epoch: attempt.epoch,
            checkpoint_id: attempt.checkpoint_id,
            scope,
            vnode_count: self.store.key_group_count().get(),
            assignment_fence,
            predecessor: self.last_committed_ref.clone(),
            participants,
            source_offsets,
            channel_progress,
            checkpoint_watermark,
        };
        let manifest_views = manifests
            .iter()
            .map(|(manifest, encoded)| (manifest, encoded.as_ref()))
            .collect::<Vec<_>>();
        index
            .validate_participant_manifests(&manifest_views)
            .map_err(DbError::Checkpoint)?;
        Ok(index)
    }

    async fn commit_external_sinks_until(
        &self,
        attempt: CheckpointAttempt,
        manifests: &[&CheckpointManifest],
        fencing_token: u64,
        predecessor_checkpoint_id: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        if !self.has_checkpoint_committable_sinks() {
            return Ok(());
        }
        if fencing_token == 0 {
            return Err(DbError::Checkpoint(
                "external checkpoint publication requires a nonzero fencing token".into(),
            ));
        }
        let identity = self.expected_pipeline_identity()?;
        let deployment_id = self.expected_deployment_id()?.to_owned();
        for sink in self
            .sinks
            .iter()
            .filter(|sink| sink.handle.checkpoint_committable())
        {
            let namespace = CoordinatedCommitNamespace::try_new(
                identity.clone(),
                deployment_id.clone(),
                sink.name.clone(),
            )
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
            let cursor =
                tokio::time::timeout_at(deadline, sink.handle.committed_cursor(namespace.clone()))
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(format!(
                            "sink '{}' committed-cursor read timed out",
                            sink.name
                        ))
                    })?
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "sink '{}' committed-cursor read failed: {error}",
                            sink.name
                        ))
                    })?;
            if let Some(cursor) = cursor {
                if cursor.checkpoint_id == attempt.checkpoint_id {
                    if cursor.fencing_token != fencing_token {
                        return Err(DbError::Checkpoint(format!(
                            "sink '{}' checkpoint {} was committed under fencing token {}, expected {fencing_token}",
                            sink.name, attempt.checkpoint_id, cursor.fencing_token
                        )));
                    }
                    continue;
                }
            }
            let expected_predecessor = if predecessor_checkpoint_id == 0 {
                CoordinatedCommitCursor {
                    checkpoint_id: 0,
                    fencing_token: 0,
                }
            } else {
                let cursor = cursor.ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "sink '{}' has no external cursor for committed predecessor {predecessor_checkpoint_id}",
                        sink.name
                    ))
                })?;
                if cursor.checkpoint_id != predecessor_checkpoint_id {
                    return Err(DbError::Checkpoint(format!(
                        "sink '{}' external cursor {} trails committed predecessor {predecessor_checkpoint_id}; recovery must publish the missing cut first",
                        sink.name, cursor.checkpoint_id
                    )));
                }
                cursor
            };

            let mut entries = Vec::with_capacity(manifests.len());
            for manifest in manifests {
                let descriptor = manifest
                    .prepared_sinks
                    .binary_search_by(|descriptor| descriptor.sink_name.cmp(&sink.name))
                    .ok()
                    .map(|index| &manifest.prepared_sinks[index])
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "participant {} has no prepared descriptor for sink '{}'",
                            manifest.participant_id, sink.name
                        ))
                    })?;
                let payload = tokio::time::timeout_at(
                    deadline,
                    self.store
                        .load_prepared_sink_descriptor(manifest, descriptor),
                )
                .await
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "sink '{}' descriptor read timed out for participant {}",
                        sink.name, manifest.participant_id
                    ))
                })?
                .map_err(DbError::from)?
                .map(|bytes| bytes.to_vec());
                entries.push(CoordinatedCommitPayload {
                    attempt,
                    participant_id: manifest.participant_id,
                    payload,
                });
            }
            entries.sort_unstable_by_key(|entry| entry.participant_id);
            let batch = CoordinatedCommitBatch {
                namespace,
                expected_predecessor,
                fencing_token,
                target: attempt,
                entries,
            };
            batch.validate_shape().map_err(|error| {
                DbError::Checkpoint(format!("sink '{}' commit batch: {error}", sink.name))
            })?;
            tokio::time::timeout_at(deadline, sink.handle.commit_aggregated(batch))
                .await
                .map_err(|_| {
                    DbError::Checkpoint(format!("sink '{}' external commit timed out", sink.name))
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "sink '{}' external commit failed: {error}",
                        sink.name
                    ))
                })?;
        }
        Ok(())
    }

    async fn create_committed_index_until(
        &self,
        index: &CommittedCheckpointIndex,
        deadline: tokio::time::Instant,
    ) -> Result<CommittedCheckpointRef, DbError> {
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("committed checkpoint publication requires a decision store".into())
        })?;
        tokio::time::timeout_at(deadline, store.create_committed_checkpoint(index))
            .await
            .map_err(|_| DbError::Checkpoint("committed checkpoint index create timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("committed checkpoint index create failed: {error}"))
            })
    }

    async fn record_outcome_until(
        &self,
        attempt: CheckpointAttempt,
        verdict: laminar_core::checkpoint_decision::CheckpointVerdict,
        committed_checkpoint: Option<CommittedCheckpointRef>,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        deadline: tokio::time::Instant,
    ) -> Result<laminar_core::checkpoint_decision::CheckpointOutcome, DbError> {
        use laminar_core::checkpoint_decision::RecordOutcomeResult;

        #[cfg(feature = "cluster")]
        let result = if let Some(controller) = self.cluster_controller.as_ref() {
            let proof = leader_proof
                .as_ref()
                .ok_or_else(|| DbError::Checkpoint("cluster outcome has no leader proof".into()))?;
            let fence = assignment_fence.clone().ok_or_else(|| {
                DbError::Checkpoint("cluster outcome has no assignment fence".into())
            })?;
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
            })?;
            tokio::time::timeout_at(
                deadline,
                authority.record_cluster_outcome(
                    proof,
                    attempt.epoch,
                    attempt.checkpoint_id,
                    fence,
                    verdict.clone(),
                    committed_checkpoint.clone(),
                ),
            )
            .await
            .map_err(|_| DbError::Checkpoint("cluster outcome create timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("cluster outcome create: {error}")))?
        } else {
            let store = self.decision_store.as_ref().ok_or_else(|| {
                DbError::Checkpoint("checkpoint outcome requires a decision store".into())
            })?;
            tokio::time::timeout_at(
                deadline,
                store.record_outcome(
                    attempt.epoch,
                    attempt.checkpoint_id,
                    CheckpointScope::Local,
                    None,
                    None,
                    verdict.clone(),
                    committed_checkpoint.clone(),
                ),
            )
            .await
            .map_err(|_| DbError::Checkpoint("checkpoint outcome create timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("checkpoint outcome create: {error}")))?
        };
        #[cfg(not(feature = "cluster"))]
        let result = {
            let store = self.decision_store.as_ref().ok_or_else(|| {
                DbError::Checkpoint("checkpoint outcome requires a decision store".into())
            })?;
            tokio::time::timeout_at(
                deadline,
                store.record_outcome(
                    attempt.epoch,
                    attempt.checkpoint_id,
                    CheckpointScope::Local,
                    None,
                    None,
                    verdict.clone(),
                    committed_checkpoint.clone(),
                ),
            )
            .await
            .map_err(|_| DbError::Checkpoint("checkpoint outcome create timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("checkpoint outcome create: {error}")))?
        };

        let winner = match result {
            RecordOutcomeResult::Created(outcome) | RecordOutcomeResult::Unchanged(outcome) => {
                outcome
            }
            RecordOutcomeResult::Conflict { winner } => winner,
        };
        if winner.epoch != attempt.epoch
            || winner.checkpoint_id != attempt.checkpoint_id
            || winner.verdict != verdict
            || winner.committed_checkpoint != committed_checkpoint
            || winner.assignment_fence != assignment_fence
            || winner.leader_proof != leader_proof
            || winner.deployment_id != self.expected_deployment_id()?
        {
            return Err(DbError::Checkpoint(format!(
                "checkpoint {} lost its immutable outcome race",
                attempt.checkpoint_id
            )));
        }
        Ok(winner)
    }

    async fn abort_attempt_until(
        &mut self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        self.phase = CheckpointPhase::Deciding;
        self.record_outcome_until(
            attempt,
            laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
            None,
            assignment_fence,
            leader_proof,
            deadline,
        )
        .await?;
        let cleanup_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        let rollback = self
            .rollback_sinks_until(attempt.epoch, cleanup_deadline)
            .await;
        let witness_cleanup = if rollback.is_ok() {
            self.clear_sink_witness_until(cleanup_deadline).await
        } else {
            Ok(())
        };
        let artifact_cleanup = self
            .delete_prepared_artifact_until(attempt, cleanup_deadline)
            .await;
        rollback?;
        witness_cleanup?;
        artifact_cleanup?;
        self.allocator.advance_epoch_to(checked_successor_epoch(
            attempt.epoch,
            "closing an aborted checkpoint",
        )?);
        self.phase = CheckpointPhase::Idle;
        Ok(())
    }

    fn failed_result(
        &mut self,
        attempt: CheckpointAttempt,
        started: Instant,
        error: String,
        disposition: CheckpointFailureDisposition,
    ) -> CheckpointResult {
        let duration = started.elapsed();
        self.phase = CheckpointPhase::Idle;
        self.checkpoints_failed = self.checkpoints_failed.saturating_add(1);
        self.last_checkpoint_duration = Some(duration);
        self.duration_histogram.record(duration);
        if disposition == CheckpointFailureDisposition::RequiresRecovery {
            self.failure_requires_recovery = true;
        }
        CheckpointResult {
            success: false,
            checkpoint_id: attempt.checkpoint_id,
            epoch: attempt.epoch,
            duration,
            error: Some(error),
            failure_disposition: Some(disposition),
        }
    }

    async fn fail_before_commit(
        &mut self,
        attempt: CheckpointAttempt,
        started: Instant,
        error: DbError,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        deadline: tokio::time::Instant,
    ) -> CheckpointResult {
        let message = error.to_string();
        match self
            .abort_attempt_until(
                attempt,
                assignment_fence.clone(),
                leader_proof.clone(),
                deadline,
            )
            .await
        {
            Ok(()) => {
                #[cfg(feature = "cluster")]
                if let Some(controller) = self.cluster_controller.as_ref() {
                    use laminar_core::cluster::control::{BarrierAnnouncement, Phase};
                    let _ = controller
                        .announce_barrier(&BarrierAnnouncement {
                            epoch: attempt.epoch,
                            checkpoint_id: attempt.checkpoint_id,
                            assignment_fence,
                            leader_proof,
                            phase: Phase::Abort,
                            flags: 0,
                        })
                        .await;
                }
                let successor = if self.has_checkpoint_committable_sinks() {
                    self.begin_sink_epoch_until(deadline).await.err()
                } else {
                    None
                };
                let error = match successor {
                    Some(successor) => {
                        format!("{message}; successor sink epoch failed: {successor}")
                    }
                    None => message,
                };
                self.failed_result(
                    attempt,
                    started,
                    error,
                    CheckpointFailureDisposition::Retryable,
                )
            }
            Err(abort) => self.failed_result(
                attempt,
                started,
                format!("{message}; durable Abort or rollback failed: {abort}"),
                CheckpointFailureDisposition::RequiresRecovery,
            ),
        }
    }

    async fn run_checkpoint_attempt(
        &mut self,
        request: CheckpointRequest,
        attempt: CheckpointAttempt,
        quorum: QuorumStage,
        started: Instant,
    ) -> Result<CheckpointResult, DbError> {
        require_canonical_attempt(attempt, "checkpoint admission")?;
        if self.failure_requires_recovery {
            return Ok(self.failed_result(
                attempt,
                started,
                "a prior checkpoint has unresolved durable or sink state".into(),
                CheckpointFailureDisposition::RequiresRecovery,
            ));
        }
        let deadline = tokio::time::Instant::from_std(started) + self.config.checkpoint_timeout;
        if tokio::time::Instant::now() >= deadline {
            return Ok(self.failed_result(
                attempt,
                started,
                "checkpoint deadline expired before durable work".into(),
                CheckpointFailureDisposition::Retryable,
            ));
        }
        let assignment_fence = request.assignment_fence.clone();
        #[cfg(feature = "cluster")]
        let validation_proof = match &quorum {
            QuorumStage::Done { leader_proof, .. } => Some(leader_proof.clone()),
            QuorumStage::RunInline => self
                .cluster_controller
                .as_ref()
                .and_then(|controller| controller.capture_leader_proof()),
        };
        #[cfg(not(feature = "cluster"))]
        let validation_proof = None;
        if let Err(error) = self.validate_request(&request) {
            return Ok(self
                .fail_before_commit(
                    attempt,
                    started,
                    error,
                    assignment_fence,
                    validation_proof,
                    deadline,
                )
                .await);
        }

        let assignment_fence = request.assignment_fence.clone();
        #[cfg(feature = "cluster")]
        let (scope, leader_proof, quorum_watermark) =
            if let Some(controller) = self.cluster_controller.clone() {
                let fence = assignment_fence.as_ref().ok_or_else(|| {
                    DbError::Checkpoint("cluster checkpoint has no assignment fence".into())
                })?;
                let (proof, participants, cluster_watermark) = match quorum {
                    QuorumStage::RunInline => {
                        let proof = controller.capture_leader_proof().ok_or_else(|| {
                            DbError::Checkpoint("no live leader proof for checkpoint".into())
                        })?;
                        let (watermark, participants) = Self::run_prepare_quorum(
                            &controller,
                            self.config.quorum_timeout,
                            PrepareQuorum::new(attempt, self.local_watermark, fence, &proof, true),
                        )
                        .await
                        .map_err(DbError::Checkpoint)?;
                        controller
                            .announce_barrier(&BarrierAnnouncement {
                                epoch: attempt.epoch,
                                checkpoint_id: attempt.checkpoint_id,
                                assignment_fence: Some(fence.clone()),
                                leader_proof: Some(proof.clone()),
                                phase: Phase::Aligned,
                                flags: 0,
                            })
                            .await
                            .map_err(|error| {
                                DbError::Checkpoint(format!(
                                    "checkpoint Aligned publication failed: {error}"
                                ))
                            })?;
                        (proof, participants, watermark)
                    }
                    QuorumStage::Done {
                        cluster_watermark,
                        participants,
                        leader_proof,
                    } => (leader_proof, participants, cluster_watermark),
                };
                let mut expected = fence
                    .participant_ids()
                    .into_iter()
                    .filter(|participant| *participant != self.store.participant_id())
                    .collect::<Vec<_>>();
                let mut actual = participants
                    .into_iter()
                    .map(|participant| participant.0)
                    .collect::<Vec<_>>();
                expected.sort_unstable();
                actual.sort_unstable();
                if actual != expected || !controller.proof_is_live(&proof) {
                    return Ok(self
                        .fail_before_commit(
                            attempt,
                            started,
                            DbError::Checkpoint(
                                "checkpoint quorum does not match its assignment or leader proof"
                                    .into(),
                            ),
                            assignment_fence,
                            Some(proof),
                            deadline,
                        )
                        .await);
                }
                (
                    CheckpointScope::Cluster,
                    Some(proof),
                    Some(cluster_watermark),
                )
            } else {
                let _ = quorum;
                (CheckpointScope::Local, None, None)
            };
        #[cfg(not(feature = "cluster"))]
        let (scope, leader_proof, quorum_watermark) = {
            let _ = quorum;
            (CheckpointScope::Local, None, None)
        };

        self.phase = CheckpointPhase::PreCommitting;
        let descriptors = match self.pre_commit_sinks_until(attempt.epoch, deadline).await {
            Ok(descriptors) => descriptors,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        assignment_fence,
                        leader_proof,
                        deadline,
                    )
                    .await);
            }
        };
        let packed = match self.pack_checkpoint(attempt, request, descriptors).await {
            Ok(packed) => packed,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        assignment_fence,
                        leader_proof,
                        deadline,
                    )
                    .await);
            }
        };
        let local_manifest_bytes = match self.persist_checkpoint_until(&packed, deadline).await {
            Ok(bytes) => bytes,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        assignment_fence,
                        leader_proof,
                        deadline,
                    )
                    .await);
            }
        };

        let manifests = match self
            .load_prepared_participant_manifests(
                attempt,
                assignment_fence.as_ref(),
                (packed.manifest.clone(), local_manifest_bytes),
                deadline,
            )
            .await
        {
            Ok(manifests) => manifests,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        assignment_fence,
                        leader_proof,
                        deadline,
                    )
                    .await);
            }
        };
        let index = match self.build_committed_index(
            attempt,
            scope,
            assignment_fence.clone(),
            &manifests,
            quorum_watermark,
        ) {
            Ok(index) => index,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        assignment_fence,
                        leader_proof,
                        deadline,
                    )
                    .await);
            }
        };
        let reference = match self.create_committed_index_until(&index, deadline).await {
            Ok(reference) => reference,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        assignment_fence,
                        leader_proof,
                        deadline,
                    )
                    .await);
            }
        };

        self.phase = CheckpointPhase::Deciding;
        let outcome = self
            .record_outcome_until(
                attempt,
                laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
                Some(reference.clone()),
                assignment_fence.clone(),
                leader_proof.clone(),
                deadline,
            )
            .await;
        if let Err(error) = outcome {
            return Ok(self.failed_result(
                attempt,
                started,
                format!("commit outcome is in-doubt: {error}"),
                CheckpointFailureDisposition::RequiresRecovery,
            ));
        }

        let predecessor_checkpoint_id = self
            .last_committed_ref
            .as_ref()
            .map_or(0, |reference| reference.checkpoint_id);
        self.last_committed_ref = Some(reference);
        self.last_committed_manifest = manifests
            .iter()
            .find(|(manifest, _)| manifest.participant_id == self.store.participant_id())
            .map(|(manifest, _)| Arc::new(manifest.clone()));
        self.prepared.remove(&attempt);
        self.allocator.advance_epoch_to(checked_successor_epoch(
            attempt.epoch,
            "closing a committed checkpoint",
        )?);
        #[cfg(feature = "cluster")]
        if let (Some(controller), Some(watermark)) =
            (self.cluster_controller.as_ref(), index.checkpoint_watermark)
        {
            controller.publish_cluster_min_watermark(watermark);
        }
        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.as_ref() {
            use laminar_core::cluster::control::{BarrierAnnouncement, Phase};
            let _ = controller
                .announce_barrier(&BarrierAnnouncement {
                    epoch: attempt.epoch,
                    checkpoint_id: attempt.checkpoint_id,
                    assignment_fence: assignment_fence.clone(),
                    leader_proof: leader_proof.clone(),
                    phase: Phase::Commit,
                    flags: 0,
                })
                .await;
        }

        let continuation_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        let fencing_token = leader_proof.as_ref().map_or(1, |proof| proof.fencing_token);
        let participant_manifests = manifests
            .iter()
            .map(|(manifest, _)| manifest)
            .collect::<Vec<_>>();
        let continuation = self
            .commit_external_sinks_until(
                attempt,
                &participant_manifests,
                fencing_token,
                predecessor_checkpoint_id,
                continuation_deadline,
            )
            .await;
        let continuation = match continuation {
            Ok(()) => {
                self.schedule_retention(index.clone(), leader_proof.clone());
                if let Err(error) = self.clear_sink_witness_until(continuation_deadline).await {
                    Err(error)
                } else if self.has_checkpoint_committable_sinks() {
                    self.begin_sink_epoch_until(continuation_deadline).await
                } else {
                    Ok(())
                }
            }
            Err(error) => Err(error),
        };

        let duration = started.elapsed();
        self.phase = CheckpointPhase::Idle;
        self.checkpoints_completed = self.checkpoints_completed.saturating_add(1);
        self.last_checkpoint_duration = Some(duration);
        self.duration_histogram.record(duration);
        let continuation_error = continuation.err().map(|error| {
            self.failure_requires_recovery = true;
            format!(
                "checkpoint {} committed, but sink continuation requires recovery: {error}",
                attempt.checkpoint_id
            )
        });
        Ok(CheckpointResult {
            success: true,
            checkpoint_id: attempt.checkpoint_id,
            epoch: attempt.epoch,
            duration,
            error: continuation_error,
            failure_disposition: None,
        })
    }

    pub async fn checkpoint(
        &mut self,
        request: CheckpointRequest,
    ) -> Result<CheckpointResult, DbError> {
        let started = Instant::now();
        let deadline = tokio::time::Instant::from_std(started) + self.config.checkpoint_timeout;
        let attempt = self.allocate_attempt_until(deadline).await?;
        self.run_checkpoint_attempt(request, attempt, QuorumStage::RunInline, started)
            .await
    }

    pub async fn checkpoint_with_offsets(
        &mut self,
        request: CheckpointRequest,
    ) -> Result<CheckpointResult, DbError> {
        self.checkpoint(request).await
    }

    pub(crate) async fn checkpoint_preallocated_started(
        &mut self,
        request: CheckpointRequest,
        attempt: CheckpointAttempt,
        quorum: QuorumStage,
        started: Instant,
    ) -> Result<CheckpointResult, DbError> {
        self.run_checkpoint_attempt(request, attempt, quorum, started)
            .await
    }

    pub(crate) async fn abandon_epoch_until(
        &mut self,
        checkpoint_id: u64,
        epoch: u64,
        error: String,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointResult, DbError> {
        let attempt = require_canonical_attempt(
            CheckpointAttempt::new(epoch, checkpoint_id),
            "checkpoint abandonment",
        )?;
        let started = Instant::now();
        Ok(self
            .fail_before_commit(
                attempt,
                started,
                DbError::Checkpoint(error),
                assignment_fence,
                leader_proof,
                deadline,
            )
            .await)
    }

    #[cfg(feature = "cluster")]
    fn validate_cluster_watermark_candidate(
        controller: &laminar_core::cluster::control::ClusterController,
        observed: laminar_core::checkpoint::CheckpointWatermark,
    ) -> Result<laminar_core::checkpoint::CheckpointWatermark, String> {
        observed
            .validate()
            .map_err(|error| format!("invalid checkpoint watermark: {error}"))?;
        match (controller.cluster_min_watermark(), observed) {
            (Some(current), laminar_core::checkpoint::CheckpointWatermark::Active(watermark))
                if watermark < current =>
            {
                Err(format!(
                    "cluster watermark {watermark} regresses committed frontier {current}"
                ))
            }
            (Some(current), laminar_core::checkpoint::CheckpointWatermark::Uninitialized) => Err(
                format!("uninitialized watermark cannot replace committed frontier {current}"),
            ),
            _ => Ok(observed),
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) async fn run_prepare_quorum(
        controller: &Arc<laminar_core::cluster::control::ClusterController>,
        quorum_timeout: Duration,
        request: PrepareQuorum<'_>,
    ) -> Result<
        (
            laminar_core::checkpoint::CheckpointWatermark,
            Vec<laminar_core::cluster::discovery::NodeId>,
        ),
        String,
    > {
        use laminar_core::cluster::control::{BarrierAnnouncement, Phase, QuorumOutcome};

        let PrepareQuorum {
            attempt,
            local_watermark,
            assignment_fence,
            leader_proof,
            announce_prepare,
        } = request;
        if !controller.proof_is_live(leader_proof) {
            return Err("leader proof is stale before checkpoint Prepare".into());
        }
        let announcement = BarrierAnnouncement {
            epoch: attempt.epoch,
            checkpoint_id: attempt.checkpoint_id,
            assignment_fence: Some(assignment_fence.clone()),
            leader_proof: Some(leader_proof.clone()),
            phase: Phase::Prepare,
            flags: 0,
        };
        if announce_prepare {
            controller
                .announce_prepare_barrier(&announcement, quorum_timeout)
                .await
                .map_err(|error| format!("checkpoint Prepare publication failed: {error}"))?;
        }
        let mut followers = assignment_fence
            .participants
            .iter()
            .map(|participant| laminar_core::cluster::discovery::NodeId(participant.node_id))
            .filter(|participant| *participant != controller.instance_id())
            .collect::<Vec<_>>();
        followers.sort_unstable_by_key(|participant| participant.0);

        let outcome = controller
            .wait_for_quorum(&announcement, &followers, quorum_timeout)
            .await;
        if !controller.proof_is_live(leader_proof) {
            return Err("leader proof expired during checkpoint Prepare".into());
        }
        match outcome {
            QuorumOutcome::Reached {
                follower_watermark,
                ref acks,
            } => {
                controller.note_responsive(acks);
                let watermark = local_watermark.cluster_min(follower_watermark);
                Ok((
                    Self::validate_cluster_watermark_candidate(controller, watermark)?,
                    followers,
                ))
            }
            QuorumOutcome::TimedOut { missing, .. } => {
                controller.note_unresponsive(&missing);
                Err(format!(
                    "checkpoint Prepare timed out waiting for {} participants",
                    missing.len()
                ))
            }
            QuorumOutcome::Failed { failures } => Err(format!(
                "checkpoint Prepare failed on {} participants: {}",
                failures.len(),
                failures
                    .first()
                    .map_or("unknown", |(_, message)| message.as_str())
            )),
        }
    }

    #[cfg(feature = "cluster")]
    async fn validate_follower_prepare_context(
        controller: &laminar_core::cluster::control::ClusterController,
        request: &CheckpointRequest,
        announcement: &laminar_core::cluster::control::BarrierAnnouncement,
    ) -> Result<
        (
            laminar_core::checkpoint::CheckpointAssignmentFence,
            LeaderProof,
        ),
        DbError,
    > {
        use laminar_core::cluster::control::Phase;

        require_canonical_attempt(
            CheckpointAttempt::new(announcement.epoch, announcement.checkpoint_id),
            "follower Prepare",
        )?;
        if announcement.phase != Phase::Prepare {
            return Err(DbError::Checkpoint(
                "follower checkpoint did not originate from Prepare".into(),
            ));
        }
        let fence = request.assignment_fence.as_ref().ok_or_else(|| {
            DbError::Checkpoint("follower checkpoint has no assignment fence".into())
        })?;
        let proof = announcement
            .leader_proof
            .as_ref()
            .ok_or_else(|| DbError::Checkpoint("follower checkpoint has no leader proof".into()))?;
        if announcement.assignment_fence.as_ref() != Some(fence)
            || !fence.contains(controller.instance_id().0)
            || fence.participant_incarnation(proof.owner.node_id) != Some(proof.owner.boot_id)
            || controller
                .checkpoint_assignment_fence_for_leader(fence.assignment_version, proof)
                .await
                .as_ref()
                != Some(fence)
        {
            return Err(DbError::Checkpoint(
                "follower Prepare does not match the certified assignment".into(),
            ));
        }
        Ok((fence.clone(), proof.clone()))
    }

    #[cfg(feature = "cluster")]
    pub(crate) async fn follower_prepare_acked_until(
        &mut self,
        request: CheckpointRequest,
        leader_proof: LeaderProof,
        epoch: u64,
        checkpoint_id: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        use laminar_core::cluster::control::BarrierAck;

        let attempt = require_canonical_attempt(
            CheckpointAttempt::new(epoch, checkpoint_id),
            "follower Prepare",
        )?;
        let controller = self.cluster_controller.clone().ok_or_else(|| {
            DbError::Checkpoint("follower Prepare has no cluster controller".into())
        })?;
        let fence = request.assignment_fence.clone().ok_or_else(|| {
            DbError::Checkpoint("follower Prepare has no assignment fence".into())
        })?;
        if controller
            .checkpoint_assignment_fence_for_leader(fence.assignment_version, &leader_proof)
            .await
            .as_ref()
            != Some(&fence)
        {
            return Err(DbError::Checkpoint(
                "follower Prepare authority is no longer current".into(),
            ));
        }
        let was_prepared = self.prepared.contains_key(&attempt);
        self.allocator.advance_epoch_to(epoch);
        self.phase = CheckpointPhase::PreCommitting;
        let descriptors = self.pre_commit_sinks_until(epoch, deadline).await;
        let prepared = match descriptors {
            Ok(descriptors) => match self.pack_checkpoint(attempt, request, descriptors).await {
                Ok(packed) => self
                    .persist_checkpoint_until(&packed, deadline)
                    .await
                    .map(|_| ()),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };
        if let Err(error) = prepared {
            let _ = controller
                .ack_barrier(&BarrierAck {
                    epoch,
                    checkpoint_id,
                    assignment_digest: Some(fence.digest()),
                    ok: false,
                    error: Some(error.to_string()),
                    watermark: self.local_watermark,
                })
                .await;
            let rollback = self.rollback_sinks_until(epoch, deadline).await;
            // This invocation never returned success, so its newly-created artifact cannot have
            // contributed a positive Prepare acknowledgement. An older acknowledged retry must
            // remain until the authoritative decision is observed.
            let artifact_cleanup = if was_prepared {
                Ok(())
            } else {
                self.delete_prepared_artifact_until(attempt, deadline).await
            };
            if let Err(rollback) = rollback {
                self.failure_requires_recovery = true;
                return Err(DbError::Checkpoint(format!(
                    "follower Prepare failed ({error}); rollback also failed ({rollback})"
                )));
            }
            if let Err(cleanup) = artifact_cleanup {
                self.failure_requires_recovery = true;
                return Err(DbError::Checkpoint(format!(
                    "follower Prepare failed ({error}); prepared artifact cleanup also failed ({cleanup})"
                )));
            }
            self.phase = CheckpointPhase::Idle;
            return Err(error);
        }
        self.phase = CheckpointPhase::Idle;
        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub async fn follower_checkpoint(
        &mut self,
        request: CheckpointRequest,
        announcement: laminar_core::cluster::control::BarrierAnnouncement,
        decision_timeout: Duration,
    ) -> Result<bool, DbError> {
        use laminar_core::cluster::control::BarrierAck;

        let controller = self.cluster_controller.clone().ok_or_else(|| {
            DbError::Checkpoint("follower checkpoint has no cluster controller".into())
        })?;
        let (fence, proof) =
            Self::validate_follower_prepare_context(&controller, &request, &announcement).await?;
        let deadline = tokio::time::Instant::now() + self.config.checkpoint_timeout;
        self.follower_prepare_acked_until(
            request,
            proof,
            announcement.epoch,
            announcement.checkpoint_id,
            deadline,
        )
        .await?;
        controller
            .ack_barrier(&BarrierAck {
                epoch: announcement.epoch,
                checkpoint_id: announcement.checkpoint_id,
                assignment_digest: Some(fence.digest()),
                ok: true,
                error: None,
                watermark: self.local_watermark,
            })
            .await
            .map_err(|error| DbError::Checkpoint(format!("follower prepared ack: {error}")))?;
        let committed = Self::await_follower_decision(
            &controller,
            announcement.epoch,
            announcement.checkpoint_id,
            &fence,
            decision_timeout,
        )
        .await?;
        self.follower_finish(announcement.epoch, announcement.checkpoint_id, committed)
            .await
    }

    #[cfg(feature = "cluster")]
    pub(crate) async fn await_follower_decision(
        controller: &laminar_core::cluster::control::ClusterController,
        epoch: u64,
        checkpoint_id: u64,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        decision_timeout: Duration,
    ) -> Result<bool, DbError> {
        use laminar_core::checkpoint_decision::CheckpointVerdict;
        let attempt = require_canonical_attempt(
            CheckpointAttempt::new(epoch, checkpoint_id),
            "follower decision",
        )?;
        if !assignment_fence.is_canonical()
            || !assignment_fence.contains(controller.instance_id().0)
        {
            return Err(DbError::Checkpoint(
                "follower decision has an invalid assignment fence".into(),
            ));
        }
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!("follower checkpoint authority: {error}"))
        })?;
        let deadline = tokio::time::Instant::now() + decision_timeout;
        loop {
            let settlement =
                tokio::time::timeout_at(deadline, authority.cluster_attempt_settlement(attempt))
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(format!(
                            "follower decision timed out for checkpoint {checkpoint_id}"
                        ))
                    })?
                    .map_err(|error| {
                        DbError::Checkpoint(format!("follower decision read failed: {error}"))
                    })?;
            if let Some(settlement) = settlement {
                let settled = CheckpointAttempt::new(settlement.epoch, settlement.checkpoint_id);
                match settled.relation_to(attempt) {
                    CheckpointAttemptRelation::Exact
                        if settlement.verdict == CheckpointVerdict::Abort =>
                    {
                        return Ok(false);
                    }
                    CheckpointAttemptRelation::Exact => {
                        let exact = tokio::time::timeout_at(
                            deadline,
                            authority.cluster_outcome_with_committed_checkpoint(epoch),
                        )
                        .await
                        .map_err(|_| {
                            DbError::Checkpoint("follower committed-index read timed out".into())
                        })?
                        .map_err(|error| {
                            DbError::Checkpoint(format!(
                                "follower committed-index read failed: {error}"
                            ))
                        })?
                        .ok_or_else(|| {
                            DbError::Checkpoint(
                                "Commit outcome has no committed checkpoint index".into(),
                            )
                        })?;
                        let (outcome, index) = exact;
                        let index = index.ok_or_else(|| {
                            DbError::Checkpoint(
                                "Commit outcome has no committed checkpoint body".into(),
                            )
                        })?;
                        if outcome != settlement
                            || outcome.assignment_fence.as_ref() != Some(assignment_fence)
                            || index.epoch != epoch
                            || index.checkpoint_id != checkpoint_id
                            || index.assignment_fence.as_ref() != Some(assignment_fence)
                            || !index.participants.iter().any(|participant| {
                                participant.participant_id == controller.instance_id().0
                            })
                        {
                            return Err(DbError::Checkpoint(
                                "follower Commit does not match its prepared participant cut"
                                    .into(),
                            ));
                        }
                        index.validate().map_err(DbError::Checkpoint)?;
                        if let Some(watermark) = index.checkpoint_watermark {
                            controller.publish_cluster_min_watermark(watermark);
                        }
                        return Ok(true);
                    }
                    CheckpointAttemptRelation::Newer => return Ok(false),
                    CheckpointAttemptRelation::Older | CheckpointAttemptRelation::Conflict => {
                        return Err(DbError::Checkpoint(
                            "follower observed an incompatible terminal checkpoint".into(),
                        ));
                    }
                }
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(DbError::Checkpoint(format!(
                    "follower decision timed out for checkpoint {checkpoint_id}"
                )));
            }
            tokio::time::sleep(FOLLOWER_DECISION_POLL.min(remaining)).await;
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) async fn follower_finish(
        &mut self,
        epoch: u64,
        checkpoint_id: u64,
        committed: bool,
    ) -> Result<bool, DbError> {
        let attempt = require_canonical_attempt(
            CheckpointAttempt::new(epoch, checkpoint_id),
            "follower completion",
        )?;
        let deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        if committed {
            let (manifest, manifest_bytes) =
                self.prepared.get(&attempt).cloned().ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "follower checkpoint {checkpoint_id} has no prepared manifest"
                    ))
                })?;
            let controller = self.cluster_controller.as_ref().ok_or_else(|| {
                DbError::Checkpoint("follower completion has no cluster controller".into())
            })?;
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!("follower checkpoint authority: {error}"))
            })?;
            let (outcome, index) = tokio::time::timeout_at(
                deadline,
                authority.cluster_outcome_with_committed_checkpoint(epoch),
            )
            .await
            .map_err(|_| DbError::Checkpoint("follower Commit verification timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("follower Commit verification failed: {error}"))
            })?
            .ok_or_else(|| DbError::Checkpoint("follower Commit disappeared".into()))?;
            let index = index.ok_or_else(|| {
                DbError::Checkpoint("follower Commit has no committed checkpoint index".into())
            })?;
            let reference = outcome.committed_checkpoint.clone().ok_or_else(|| {
                DbError::Checkpoint("follower Commit has no committed checkpoint reference".into())
            })?;
            let participant = index
                .participants
                .iter()
                .find(|participant| participant.participant_id == self.store.participant_id())
                .ok_or_else(|| {
                    DbError::Checkpoint("follower is absent from committed participant set".into())
                })?;
            participant
                .verify_manifest(manifest.as_ref(), &manifest_bytes)
                .map_err(DbError::Checkpoint)?;
            self.last_committed_ref = Some(reference);
            self.last_committed_manifest = Some(manifest);
            self.prepared.remove(&attempt);
            self.checkpoints_completed = self.checkpoints_completed.saturating_add(1);
        } else {
            use laminar_core::checkpoint_decision::CheckpointVerdict;

            let controller = self.cluster_controller.as_ref().ok_or_else(|| {
                DbError::Checkpoint("follower completion has no cluster controller".into())
            })?;
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!("follower checkpoint authority: {error}"))
            })?;
            let settlement =
                tokio::time::timeout_at(deadline, authority.cluster_attempt_settlement(attempt))
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint("follower Abort verification timed out".into())
                    })?
                    .map_err(|error| {
                        DbError::Checkpoint(format!("follower Abort verification failed: {error}"))
                    })?
                    .ok_or_else(|| DbError::Checkpoint("follower Abort is unresolved".into()))?;
            let settled = CheckpointAttempt::new(settlement.epoch, settlement.checkpoint_id);
            match settled.relation_to(attempt) {
                CheckpointAttemptRelation::Exact
                    if settlement.verdict == CheckpointVerdict::Abort => {}
                CheckpointAttemptRelation::Newer => {}
                _ => {
                    return Err(DbError::Checkpoint(
                        "follower cannot discard a checkpoint without an authoritative Abort or superseding terminal outcome"
                            .into(),
                    ));
                }
            }
            let rollback = self.rollback_sinks_until(epoch, deadline).await;
            let artifact_cleanup = self.delete_prepared_artifact_until(attempt, deadline).await;
            rollback?;
            artifact_cleanup?;
            self.checkpoints_failed = self.checkpoints_failed.saturating_add(1);
        }
        self.allocator.advance_epoch_to(checked_successor_epoch(
            epoch,
            "closing a follower checkpoint",
        )?);
        if self.has_checkpoint_committable_sinks() {
            self.begin_sink_epoch_until(deadline).await?;
        }
        self.phase = CheckpointPhase::Idle;
        Ok(committed)
    }
}

impl std::fmt::Debug for CheckpointCoordinator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CheckpointCoordinator")
            .field("phase", &self.phase)
            .field("participant", &self.store.participant_id())
            .field("sinks", &self.sinks.len())
            .field("completed", &self.checkpoints_completed)
            .field("failed", &self.checkpoints_failed)
            .finish_non_exhaustive()
    }
}

impl Drop for CheckpointCoordinator {
    fn drop(&mut self) {
        self.gc_task.abort();
    }
}

struct DurationHistogram {
    samples: Box<[u64; Self::CAPACITY]>,
    cursor: usize,
    count: usize,
}

impl DurationHistogram {
    const CAPACITY: usize = 100;

    fn new() -> Self {
        Self {
            samples: Box::new([0; Self::CAPACITY]),
            cursor: 0,
            count: 0,
        }
    }

    fn record(&mut self, duration: Duration) {
        self.samples[self.cursor] = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
        self.cursor = (self.cursor + 1) % Self::CAPACITY;
        self.count = self.count.saturating_add(1).min(Self::CAPACITY);
    }

    fn percentiles(&self) -> (u64, u64, u64) {
        if self.count == 0 {
            return (0, 0, 0);
        }
        let mut values = self.samples[..self.count].to_vec();
        values.sort_unstable();
        let at = |numerator: usize| {
            let index = (self.count.saturating_sub(1) * numerator).div_ceil(100);
            values[index]
        };
        (at(50), at(95), at(99))
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CheckpointStats {
    pub completed: u64,
    pub failed: u64,
    pub last_duration: Option<Duration>,
    pub duration_p50_ms: u64,
    pub duration_p95_ms: u64,
    pub duration_p99_ms: u64,
    pub total_bytes_written: u64,
    pub current_phase: CheckpointPhase,
    pub current_epoch: u64,
}

impl CheckpointCoordinator {
    pub(crate) fn last_committed_manifest(&self) -> Option<&CheckpointManifest> {
        self.last_committed_manifest.as_deref()
    }

    #[must_use]
    pub fn stats(&self) -> CheckpointStats {
        let (p50, p95, p99) = self.duration_histogram.percentiles();
        CheckpointStats {
            completed: self.checkpoints_completed,
            failed: self.checkpoints_failed,
            last_duration: self.last_checkpoint_duration,
            duration_p50_ms: p50 / 1_000,
            duration_p95_ms: p95 / 1_000,
            duration_p99_ms: p99 / 1_000,
            total_bytes_written: self.total_bytes_written,
            current_phase: self.phase,
            current_epoch: self.allocator.peek_epoch(),
        }
    }
}

#[must_use]
pub(crate) fn source_to_connector_checkpoint(cp: &SourceCheckpoint) -> ConnectorCheckpoint {
    ConnectorCheckpoint {
        offsets: cp.durable_offsets(),
        metadata: cp.metadata().clone(),
        source_assignment_version: cp.assignment_version(),
    }
}

#[must_use]
pub(crate) fn connector_to_source_checkpoint(cp: &ConnectorCheckpoint) -> SourceCheckpoint {
    let mut source = SourceCheckpoint::with_offsets(cp.offsets.clone());
    for (key, value) in &cp.metadata {
        source.set_metadata(key.clone(), value.clone());
    }
    if let Some(version) = cp.source_assignment_version {
        source.bind_assignment_version(version);
    }
    source
}
