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
    channel_progress_frontiers_by_source, checkpoint_artifact_identity_sha256,
    checkpoint_descriptor_sha256, checkpoint_manifest_bytes, checkpoint_sha256,
    classify_channel_progress, ByteRange, ChannelProgress, CheckpointAttempt, CheckpointManifest,
    CheckpointScope, CheckpointStore, CheckpointWatermark, CommittedCheckpointIndex,
    CommittedCheckpointRef, CommittedParticipantRef, ConnectorCheckpoint, LeaderProof,
    PipelineIdentity, PreparedSinkDescriptor, ReferencedStateChunk, StateChunkId, StateFrame,
    StateFrameKey, COMMITTED_CHECKPOINT_INDEX_VERSION, PREPARED_SINK_DESCRIPTOR_VERSION,
};
use laminar_core::checkpoint_decision::{
    CheckpointArtifactInventory, CheckpointArtifactInventoryUpdateResult,
};
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::error::DbError;
#[cfg(feature = "cluster")]
use crate::recovery_manager::{
    load_verified_state_frames, RecoveredStateFrame, VerifiedStateFramePlan,
};

const MAX_SINK_PHASE_ONE_CONCURRENCY: usize = 8;
const MAX_RETENTION_IO_CONCURRENCY: usize = 8;
const REFERENCED_CHUNK_REBASE_THRESHOLD: usize = 64;
const RETENTION_RETRY_DELAY: Duration = Duration::from_secs(30);
#[cfg(feature = "cluster")]
const FOLLOWER_DECISION_POLL: Duration = Duration::from_millis(250);
const PARTICIPANT_MANIFEST_POLL_INITIAL: Duration = Duration::from_millis(10);
const PARTICIPANT_MANIFEST_POLL_MAX: Duration = Duration::from_millis(250);

async fn await_participant_manifest_until<F, Fut>(
    participant_id: u64,
    attempt: CheckpointAttempt,
    deadline: tokio::time::Instant,
    mut load: F,
) -> Result<CheckpointManifest, DbError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<CheckpointManifest>, DbError>>,
{
    let mut backoff = PARTICIPANT_MANIFEST_POLL_INITIAL;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(DbError::Checkpoint(format!(
                "participant {participant_id} checkpoint {} epoch {} manifest readiness timed out",
                attempt.checkpoint_id, attempt.epoch
            )));
        }
        match tokio::time::timeout_at(deadline, load()).await {
            Ok(Ok(Some(manifest))) => {
                if manifest.participant_id != participant_id
                    || manifest.checkpoint_id != attempt.checkpoint_id
                    || manifest.epoch != attempt.epoch
                {
                    return Err(DbError::Checkpoint(format!(
                        "participant {participant_id} published an invalid manifest readiness marker for checkpoint {} epoch {}",
                        attempt.checkpoint_id, attempt.epoch
                    )));
                }
                return Ok(manifest);
            }
            Ok(Ok(None)) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(DbError::Checkpoint(format!(
                    "participant {participant_id} checkpoint {} epoch {} manifest read timed out",
                    attempt.checkpoint_id, attempt.epoch
                )));
            }
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(DbError::Checkpoint(format!(
                "participant {participant_id} checkpoint {} epoch {} manifest readiness timed out",
                attempt.checkpoint_id, attempt.epoch
            )));
        }
        tokio::time::sleep_until((now + backoff).min(deadline)).await;
        backoff = backoff.saturating_mul(2).min(PARTICIPANT_MANIFEST_POLL_MAX);
    }
}

#[cfg(test)]
mod participant_manifest_readiness_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(start_paused = true)]
    async fn missing_manifest_is_retried_until_it_becomes_ready() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let mut manifest = CheckpointManifest::new(7, 7);
        manifest.bind_participant(2);
        let loaded = await_participant_manifest_until(
            2,
            CheckpointAttempt::canonical(7),
            tokio::time::Instant::now() + Duration::from_secs(1),
            {
                let attempts = Arc::clone(&attempts);
                let manifest = manifest.clone();
                move || {
                    let attempts = Arc::clone(&attempts);
                    let manifest = manifest.clone();
                    async move {
                        if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                            Ok(None)
                        } else {
                            Ok(Some(manifest))
                        }
                    }
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(loaded.checkpoint_id, 7);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn wrong_manifest_identity_fails_without_polling() {
        for (manifest, field) in [
            (CheckpointManifest::new(7, 7), "participant"),
            (
                {
                    let mut manifest = CheckpointManifest::new(8, 7);
                    manifest.bind_participant(2);
                    manifest
                },
                "checkpoint",
            ),
            (
                {
                    let mut manifest = CheckpointManifest::new(7, 8);
                    manifest.bind_participant(2);
                    manifest
                },
                "epoch",
            ),
        ] {
            let attempts = Arc::new(AtomicUsize::new(0));
            let started = tokio::time::Instant::now();
            let error = await_participant_manifest_until(
                2,
                CheckpointAttempt::canonical(7),
                started + Duration::from_secs(1),
                {
                    let attempts = Arc::clone(&attempts);
                    move || {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        let manifest = manifest.clone();
                        async move { Ok(Some(manifest)) }
                    }
                },
            )
            .await
            .unwrap_err();

            assert!(
                error
                    .to_string()
                    .contains("invalid manifest readiness marker"),
                "wrong {field} returned {error}"
            );
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
            assert_eq!(tokio::time::Instant::now(), started);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_manifest_read_fails_without_polling() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let started = tokio::time::Instant::now();
        let error = await_participant_manifest_until(
            2,
            CheckpointAttempt::canonical(7),
            started + Duration::from_secs(1),
            {
                let attempts = Arc::clone(&attempts);
                move || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async {
                        Err(DbError::Checkpoint(
                            "invalid participant manifest readiness marker".into(),
                        ))
                    }
                }
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("invalid participant manifest"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(tokio::time::Instant::now(), started);
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_missing_and_blocked_reads_share_the_exact_deadline() {
        let started = tokio::time::Instant::now();
        let deadline = started + Duration::from_millis(100);
        let missing = await_participant_manifest_until(
            2,
            CheckpointAttempt::canonical(7),
            deadline,
            || async { Ok(None) },
        );
        let blocked =
            await_participant_manifest_until(3, CheckpointAttempt::canonical(7), deadline, || {
                std::future::pending::<Result<Option<CheckpointManifest>, DbError>>()
            });

        let (missing, blocked) = tokio::join!(missing, blocked);

        assert!(missing
            .unwrap_err()
            .to_string()
            .contains("readiness timed out"));
        assert!(blocked.unwrap_err().to_string().contains("read timed out"));
        assert_eq!(tokio::time::Instant::now(), deadline);
    }
}

#[cfg(feature = "cluster")]
async fn publish_terminal_hint_until<F>(deadline: tokio::time::Instant, hint: F)
where
    F: std::future::Future<Output = Result<(), String>>,
{
    // Terminal hints accelerate observation but do not own the already-immutable durable verdict.
    let _ = tokio::time::timeout_at(deadline, hint).await;
}

#[cfg(all(test, feature = "cluster"))]
#[tokio::test]
async fn terminal_checkpoint_hint_cannot_outlive_its_cleanup_deadline() {
    tokio::time::timeout(
        Duration::from_secs(1),
        publish_terminal_hint_until(
            tokio::time::Instant::now(),
            std::future::pending::<Result<(), String>>(),
        ),
    )
    .await
    .expect("an unresponsive terminal hint must be released at its private cleanup deadline");
}

#[cfg(all(debug_assertions, feature = "cluster"))]
async fn checkpoint_kill_gate(
    role: &'static str,
    attempt: CheckpointAttempt,
    predecessor: Option<(u64, u64)>,
) {
    static GATE_FILE: std::sync::OnceLock<Option<std::path::PathBuf>> = std::sync::OnceLock::new();
    let Some(gate_file) = GATE_FILE
        .get_or_init(|| std::env::var_os("LAMINAR_CHECKPOINT_KILL_GATE_FILE").map(Into::into))
        .as_ref()
    else {
        return;
    };
    if std::fs::read_to_string(gate_file)
        .ok()
        .is_none_or(|requested| requested.trim() != role)
    {
        return;
    }
    let Some((predecessor_id, predecessor_epoch)) = predecessor else {
        return;
    };

    let ready_file = gate_file.with_extension("ready");
    let evidence = format!(
        "{role} {} {} {predecessor_id} {predecessor_epoch}",
        attempt.checkpoint_id, attempt.epoch
    );
    if std::fs::write(&ready_file, evidence).is_err() {
        return;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while gate_file.is_file() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let _ = std::fs::remove_file(ready_file);
}

#[cfg(feature = "cluster")]
fn handoff_error(message: impl Into<String>) -> DbError {
    DbError::Checkpoint(format!("[LDB-6050] {}", message.into()))
}

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
    pub flags: u64,
    pub handoff_replay_pending: bool,
    /// Capture-time proof that this cut can be restored under a different vnode assignment.
    pub reassignment_portable: bool,
    pub assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
    pub state_frames: Vec<CapturedStateFrame>,
    pub(crate) managed_vnode_operators: Vec<ManagedVnodeOperator>,
    pub source_names: Vec<String>,
    pub channel_progress: Vec<ChannelProgress>,
    pub source_offset_overrides: HashMap<String, ConnectorCheckpoint>,
}

#[derive(Debug, Clone)]
pub struct CapturedStateFrame {
    pub key: StateFrameKey,
    pub state: Option<Bytes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ManagedVnodePlacement {
    GlobalSingleton,
    VnodeKeyed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedVnodeOperator {
    pub(crate) operator_id: String,
    pub(crate) placement: ManagedVnodePlacement,
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

/// Determines who publishes a prepared successor sink epoch as writable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SinkEpochPublication {
    /// Startup and direct coordinator APIs have no callback-owned transition guard.
    Immediate,
    /// A spawned pipeline tail publishes only after its terminal result is known successful.
    DeferredToTail,
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

#[derive(Debug, Clone)]
pub(crate) enum QuorumStage {
    RunInline,
    #[cfg(feature = "cluster")]
    Captured {
        cluster_watermark: laminar_core::checkpoint::CheckpointWatermark,
        participants: Vec<QuorumPeer>,
        leader_proof: LeaderProof,
    },
}

/// Follower-local durability state after immutable capture ownership has been acknowledged.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FollowerPrepareOutcome {
    /// Manifest persistence completed with an acknowledgement.
    Prepared,
    /// Manifest Create may be visible even though its acknowledgement was lost. The captured
    /// phase-one state must remain intact until an authoritative Commit or Abort is observed.
    InDoubt,
}

#[cfg(feature = "cluster")]
pub(crate) struct PrepareQuorum<'a> {
    attempt: CheckpointAttempt,
    local_watermark: laminar_core::checkpoint::CheckpointWatermark,
    assignment_fence: &'a laminar_core::checkpoint::CheckpointAssignmentFence,
    leader_proof: &'a LeaderProof,
    flags: u64,
}

#[cfg(feature = "cluster")]
impl<'a> PrepareQuorum<'a> {
    pub(crate) const fn new(
        attempt: CheckpointAttempt,
        local_watermark: laminar_core::checkpoint::CheckpointWatermark,
        assignment_fence: &'a laminar_core::checkpoint::CheckpointAssignmentFence,
        leader_proof: &'a LeaderProof,
        flags: u64,
    ) -> Self {
        Self {
            attempt,
            local_watermark,
            assignment_fence,
            leader_proof,
            flags,
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
    #[cfg(feature = "cluster")]
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::checkpoint_decision::{
        CheckpointDecisionStore, CheckpointRetentionState, CheckpointRetentionUpdateResult,
    };
    #[cfg(feature = "cluster")]
    use laminar_core::cluster::control::{
        BarrierAck, BarrierAckDisposition, ClusterController, ClusterKv, InMemoryKv,
        LeaderLeaseOwner, LeaderLeaseStore, LeaseOutcome, ACK_KEY,
    };
    #[cfg(feature = "cluster")]
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
    use laminar_core::state::KeyGroupCount;
    use object_store::memory::InMemory;

    #[cfg(feature = "cluster")]
    struct ManifestCommitThenIoStore {
        inner: Arc<dyn object_store::ObjectStore>,
        fail_manifest_create: std::sync::atomic::AtomicBool,
        block_get: std::sync::atomic::AtomicBool,
    }

    #[cfg(feature = "cluster")]
    impl std::fmt::Debug for ManifestCommitThenIoStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("ManifestCommitThenIoStore")
                .finish_non_exhaustive()
        }
    }

    #[cfg(feature = "cluster")]
    impl std::fmt::Display for ManifestCommitThenIoStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("ManifestCommitThenIoStore")
        }
    }

    #[cfg(feature = "cluster")]
    #[async_trait::async_trait]
    impl object_store::ObjectStore for ManifestCommitThenIoStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let lose_ack = location.to_string().ends_with("/manifest.json")
                && matches!(&options.mode, object_store::PutMode::Create)
                && self
                    .fail_manifest_create
                    .swap(false, std::sync::atomic::Ordering::AcqRel);
            let result = self.inner.put_opts(location, payload, options).await?;
            if lose_ack {
                return Err(object_store::Error::Generic {
                    store: "ManifestCommitThenIoStore",
                    source: Box::new(std::io::Error::other(
                        "injected manifest acknowledgement loss after create",
                    )),
                });
            }
            Ok(result)
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if self.block_get.load(std::sync::atomic::Ordering::Acquire) {
                return std::future::pending().await;
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[cfg(feature = "cluster")]
    struct AmbiguousFollowerSink {
        rollbacks: Arc<std::sync::atomic::AtomicU64>,
        schema: arrow::datatypes::SchemaRef,
    }

    #[cfg(feature = "cluster")]
    #[async_trait::async_trait]
    impl laminar_connectors::connector::SinkConnector for AmbiguousFollowerSink {
        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), laminar_connectors::error::ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &arrow::record_batch::RecordBatch,
        ) -> Result<
            laminar_connectors::connector::WriteResult,
            laminar_connectors::error::ConnectorError,
        > {
            Ok(laminar_connectors::connector::WriteResult {
                records_written: 0,
                bytes_written: 0,
            })
        }

        async fn rollback_epoch(
            &mut self,
            _epoch: u64,
        ) -> Result<(), laminar_connectors::error::ConnectorError> {
            self.rollbacks
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            Ok(())
        }

        async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(1)
        }
    }

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

    #[cfg(feature = "cluster")]
    fn cluster_manifest(
        checkpoint_id: u64,
        participant_id: u64,
        vnode: u16,
        deployment_id: &str,
        fence: &CheckpointAssignmentFence,
        key_groups: KeyGroupCount,
    ) -> (CheckpointManifest, Bytes) {
        let payload = Bytes::from(vec![
            u8::try_from(checkpoint_id).unwrap(),
            u8::try_from(participant_id).unwrap(),
        ]);
        let mut manifest =
            CheckpointManifest::new_with_key_group_count(checkpoint_id, checkpoint_id, key_groups);
        manifest.bind_participant(participant_id);
        manifest.deployment_id = deployment_id.into();
        manifest.assignment_fence = Some(fence.clone());
        manifest.reassignment_portable = true;
        manifest.owned_vnodes = vec![vnode];
        manifest.state_frames.push(StateFrame {
            key: StateFrameKey::Vnode {
                operator_id: "join".into(),
                vnode,
            },
            chunk: manifest.node_data.chunk,
            range: ByteRange {
                offset: 0,
                length: payload.len() as u64,
            },
            sha256: checkpoint_sha256(&payload),
        });
        manifest.node_data.object_length = payload.len() as u64;
        manifest.node_data.sha256 = checkpoint_sha256(&payload);
        (manifest, payload)
    }

    #[cfg(feature = "cluster")]
    async fn save_cluster_manifests(
        objects: Arc<dyn object_store::ObjectStore>,
        prefix: &str,
        checkpoint_id: u64,
        deployment_id: &str,
        fence: &CheckpointAssignmentFence,
        key_groups: KeyGroupCount,
    ) -> Vec<(CheckpointManifest, Bytes)> {
        let mut manifests = Vec::with_capacity(2);
        for (participant_id, vnode) in [(1, 0), (2, 1)] {
            let store = ObjectStoreCheckpointStore::new(Arc::clone(&objects), prefix)
                .with_key_group_count(key_groups)
                .with_participant_id(participant_id);
            let (manifest, payload) = cluster_manifest(
                checkpoint_id,
                participant_id,
                vnode,
                deployment_id,
                fence,
                key_groups,
            );
            let encoded = store
                .save_checkpoint(&manifest, std::slice::from_ref(&payload))
                .await
                .unwrap();
            manifests.push((manifest, encoded));
        }
        manifests
    }

    fn retention_state(result: CheckpointRetentionUpdateResult) -> CheckpointRetentionState {
        match result {
            CheckpointRetentionUpdateResult::Applied(state)
            | CheckpointRetentionUpdateResult::Unchanged(state) => state,
            result => panic!("unexpected retention update: {result:?}"),
        }
    }

    async fn node_data_exists(
        store: &dyn CheckpointStore,
        chunk: StateChunkId,
        object_length: u64,
    ) -> bool {
        store
            .load_node_data_ranges(chunk, object_length, &[])
            .await
            .unwrap()
            .is_some()
    }

    async fn admit_local_artifacts(
        decisions: &CheckpointDecisionStore,
        deployment_id: &str,
        checkpoint_id: u64,
        pipeline_identity: PipelineIdentity,
    ) {
        let result = decisions
            .begin_checkpoint_artifact_inventory(CheckpointArtifactInventory {
                deployment_id: deployment_id.to_owned(),
                pipeline_identity,
                attempt: CheckpointAttempt::canonical(checkpoint_id),
                assignment_fence: None,
            })
            .await
            .unwrap();
        assert!(matches!(
            result,
            CheckpointArtifactInventoryUpdateResult::Applied
                | CheckpointArtifactInventoryUpdateResult::Unchanged
        ));
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
            admit_local_artifacts(
                decisions.as_ref(),
                &deployment_id,
                checkpoint_id,
                PipelineIdentity::empty(),
            )
            .await;
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
                reassignment_portable: false,
                predecessor,
                participants: vec![participant],
                source_names: Vec::new(),
                source_offsets: BTreeMap::new(),
                channel_progress: Vec::new(),
                source_watermarks: BTreeMap::new(),
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
            assert!(!node_data_exists(store.as_ref(), chunk, 1).await);
        }
        assert!(
            node_data_exists(
                store.as_ref(),
                StateChunkId {
                    participant_id: 1,
                    checkpoint_id: 3,
                },
                1,
            )
            .await
        );
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

        admit_local_artifacts(
            decisions.as_ref(),
            &deployment_id,
            7,
            PipelineIdentity::empty(),
        )
        .await;
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
            reassignment_portable: false,
            predecessor,
            participants: vec![CommittedParticipantRef::from_manifest(&manifest, &encoded).unwrap()],
            source_names: Vec::new(),
            source_offsets: BTreeMap::new(),
            channel_progress: Vec::new(),
            source_watermarks: BTreeMap::new(),
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
            let object_length = u64::from(checkpoint_id == 3);
            assert!(
                !node_data_exists(
                    store.as_ref(),
                    StateChunkId {
                        participant_id: 1,
                        checkpoint_id,
                    },
                    object_length,
                )
                .await
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
        coordinator
            .bind_pipeline_identity(PipelineIdentity::empty())
            .unwrap();
        let deployment_id = coordinator.expected_deployment_id().unwrap().to_owned();
        (coordinator, decisions, deployment_id)
    }

    #[tokio::test]
    async fn initial_committed_index_derives_an_empty_inventory_source_cut_from_its_marker() {
        let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let (coordinator, decisions, deployment_id) = coordinator_with_store(objects).await;
        let (mut manifest, _) = manifest(1, &deployment_id, None);
        manifest.source_names = vec!["orders".into()];
        manifest.source_offsets.insert(
            "orders".into(),
            ConnectorCheckpoint {
                input_channels: Some(Vec::new()),
                ..ConnectorCheckpoint::default()
            },
        );
        manifest.channel_progress = vec![ChannelProgress {
            participant_id: laminar_core::state::LOCAL_NODE_ID.0,
            source_name: "orders".into(),
            input_channel: laminar_core::checkpoint::SINGLETON_WATERMARK_CHANNEL.to_vec(),
            watermark: Some(900),
            idle: true,
        }];
        let encoded = Bytes::from(checkpoint_manifest_bytes(&manifest).unwrap());

        let index = coordinator
            .build_committed_index(
                CheckpointAttempt::canonical(1),
                CheckpointScope::Local,
                None,
                None,
                &BTreeMap::new(),
                &[(manifest, encoded)],
                None,
            )
            .unwrap();
        assert_eq!(index.version, COMMITTED_CHECKPOINT_INDEX_VERSION);
        assert!(index.predecessor.is_none());
        assert_eq!(index.source_watermarks.get("orders"), Some(&900));
        assert_eq!(index.checkpoint_watermark, None);
        assert!(index.source_offsets["orders"]
            .input_channels
            .as_ref()
            .is_some_and(Vec::is_empty));
        index.validate().unwrap();

        let reference = decisions.create_committed_checkpoint(&index).await.unwrap();
        let restored = decisions
            .load_committed_checkpoint(&reference)
            .await
            .unwrap();
        assert_eq!(restored.source_watermarks.get("orders"), Some(&900));
        assert_eq!(restored, index);
    }

    #[tokio::test]
    async fn committed_index_retains_a_predecessor_cut_for_an_empty_source_inventory() {
        let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let (coordinator, decisions, deployment_id) = coordinator_with_store(objects).await;
        let (mut predecessor_manifest, _) = manifest(1, &deployment_id, None);
        predecessor_manifest.source_names = vec!["orders".into()];
        predecessor_manifest.channel_progress = vec![ChannelProgress {
            participant_id: laminar_core::state::LOCAL_NODE_ID.0,
            source_name: "orders".into(),
            input_channel: laminar_core::checkpoint::SINGLETON_WATERMARK_CHANNEL.to_vec(),
            watermark: Some(900),
            idle: false,
        }];
        predecessor_manifest.checkpoint_watermark = Some(900);
        let predecessor_bytes =
            Bytes::from(checkpoint_manifest_bytes(&predecessor_manifest).unwrap());
        let predecessor_index = coordinator
            .build_committed_index(
                CheckpointAttempt::canonical(1),
                CheckpointScope::Local,
                None,
                None,
                &BTreeMap::new(),
                &[(predecessor_manifest, predecessor_bytes)],
                None,
            )
            .unwrap();
        let predecessor = decisions
            .create_committed_checkpoint(&predecessor_index)
            .await
            .unwrap();
        let retained = coordinator
            .predecessor_source_watermarks_until(
                Some(&predecessor),
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();

        let (mut manifest, _) = manifest(2, &deployment_id, None);
        manifest.source_names = vec!["orders".into()];
        let encoded = Bytes::from(checkpoint_manifest_bytes(&manifest).unwrap());

        let index = coordinator
            .build_committed_index(
                CheckpointAttempt::canonical(2),
                CheckpointScope::Local,
                None,
                Some(predecessor),
                &retained,
                &[(manifest, encoded)],
                None,
            )
            .unwrap();
        let reference = decisions.create_committed_checkpoint(&index).await.unwrap();
        let restored = decisions
            .load_committed_checkpoint(&reference)
            .await
            .unwrap();

        assert!(restored.channel_progress.is_empty());
        assert_eq!(restored.source_watermarks.get("orders"), Some(&900));
        restored
            .validate_predecessor_index(&predecessor_index)
            .unwrap();
    }

    async fn save_prepared(
        coordinator: &mut CheckpointCoordinator,
        checkpoint_id: u64,
        deployment_id: &str,
    ) -> (CheckpointManifest, Bytes) {
        coordinator
            .begin_checkpoint_artifacts_until(
                CheckpointAttempt::canonical(checkpoint_id),
                None,
                None,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
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
    async fn durable_abort_seals_only_its_exact_prepared_artifact_and_is_idempotent() {
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

        assert!(coordinator
            .store
            .load_manifest_for_participant(1, 1)
            .await
            .is_err());
        assert!(coordinator
            .store
            .load_node_data_ranges(
                aborted.node_data.chunk,
                aborted.node_data.object_length,
                &[],
            )
            .await
            .is_err());
        assert!(coordinator
            .store
            .load_manifest_for_participant(1, 2)
            .await
            .unwrap()
            .is_some());
        assert!(
            node_data_exists(
                coordinator.store.as_ref(),
                unrelated.node_data.chunk,
                unrelated.node_data.object_length,
            )
            .await
        );
    }

    #[tokio::test]
    async fn recovery_aborts_and_seals_unresolved_candidate_index() {
        let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let (mut interrupted, decisions, deployment_id) =
            coordinator_with_store(Arc::clone(&objects)).await;
        let committed = interrupted
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(committed.success, "{committed:?}");
        let predecessor = interrupted.last_committed_ref.clone().unwrap();
        let (manifest, encoded) = save_prepared(&mut interrupted, 2, &deployment_id).await;
        let index = CommittedCheckpointIndex {
            version: COMMITTED_CHECKPOINT_INDEX_VERSION,
            deployment_id,
            pipeline_identity: manifest.pipeline_identity.clone(),
            epoch: 2,
            checkpoint_id: 2,
            scope: CheckpointScope::Local,
            vnode_count: 1,
            assignment_fence: None,
            reassignment_portable: false,
            predecessor: Some(predecessor.clone()),
            participants: vec![CommittedParticipantRef::from_manifest(&manifest, &encoded).unwrap()],
            source_names: Vec::new(),
            source_offsets: BTreeMap::new(),
            channel_progress: Vec::new(),
            source_watermarks: BTreeMap::new(),
            checkpoint_watermark: None,
        };
        let candidate = decisions.create_committed_checkpoint(&index).await.unwrap();
        drop(interrupted);

        let (mut restarted, _, _) = coordinator_with_store(objects).await;
        assert_eq!(restarted.recover().await.unwrap().unwrap().epoch(), 1);
        assert!(decisions
            .load_committed_checkpoint(&candidate)
            .await
            .is_err());
        assert!(decisions
            .load_committed_checkpoint(&predecessor)
            .await
            .is_ok());
        let head = decisions.checkpoint_decision_head().await.unwrap().unwrap();
        assert!(head.active_artifacts.is_none());
        assert!(head
            .latest_terminal
            .is_some_and(|outcome| !outcome.is_commit() && outcome.checkpoint_id == 2));
        assert!(restarted
            .store
            .load_manifest_for_participant(1, 2)
            .await
            .is_err());
        assert!(restarted
            .store
            .load_node_data_ranges(
                manifest.node_data.chunk,
                manifest.node_data.object_length,
                &[],
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn commit_winner_prevents_prepared_artifact_sealing() {
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
            reassignment_portable: false,
            predecessor: None,
            participants: vec![participant],
            source_names: Vec::new(),
            source_offsets: BTreeMap::new(),
            channel_progress: Vec::new(),
            source_watermarks: BTreeMap::new(),
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
        assert!(
            node_data_exists(
                coordinator.store.as_ref(),
                manifest.node_data.chunk,
                manifest.node_data.object_length,
            )
            .await
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(start_paused = true)]
    async fn follower_assignment_authority_validation_is_bounded_by_attempt_deadline() {
        let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let blocked = Arc::new(ManifestCommitThenIoStore {
            inner,
            fail_manifest_create: std::sync::atomic::AtomicBool::new(false),
            block_get: std::sync::atomic::AtomicBool::new(false),
        });
        let authority_objects: Arc<dyn object_store::ObjectStore> = blocked.clone();
        let authority = Arc::new(LeaderLeaseStore::new(authority_objects, 1_000));
        let leader_boot = uuid::Uuid::from_u128(1);
        let follower_boot = uuid::Uuid::from_u128(2);
        let owner = LeaderLeaseOwner {
            node: NodeId(1),
            boot: leader_boot,
            process_term: 1,
        };
        let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap()
        else {
            panic!("empty checkpoint authority must grant its first leader term");
        };
        let proof = lease.proof();
        let fence = CheckpointAssignmentFence::from_owner_map(
            1,
            &[1, 2],
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: leader_boot,
                },
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: follower_boot,
                },
            ],
        )
        .unwrap();
        let local_kv = Arc::new(InMemoryKv::new(NodeId(2)));
        let control_kv: Arc<dyn ClusterKv> = local_kv;
        let (_members_tx, members_rx) = tokio::sync::watch::channel(vec![NodeInfo {
            id: NodeId(1),
            name: "leader".into(),
            rpc_address: String::new(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        }]);
        let controller = ClusterController::new_with_recovery_incarnation(
            NodeId(2),
            Arc::clone(&control_kv),
            control_kv,
            None,
            members_rx,
            follower_boot,
        );
        controller.set_leader_lease_store(authority);
        controller.publish_checkpoint_assignment_fence(Some(fence.clone()));

        blocked
            .block_get
            .store(true, std::sync::atomic::Ordering::Release);
        let started = tokio::time::Instant::now();
        let deadline = started + Duration::from_secs(5);
        let validation = CheckpointCoordinator::certify_follower_assignment_until(
            &controller,
            &fence,
            &proof,
            deadline,
            "follower Prepare",
        );
        tokio::pin!(validation);
        tokio::select! {
            result = &mut validation => panic!("authority validation completed before its deadline: {result:?}"),
            () = tokio::task::yield_now() => {}
        }
        tokio::time::advance(Duration::from_secs(5)).await;
        let error = validation
            .await
            .expect_err("a stalled authority read must expire at the attempt deadline");
        assert_eq!(tokio::time::Instant::now(), deadline);
        assert!(error
            .to_string()
            .contains("follower Prepare authority validation timed out"));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn follower_manifest_ack_loss_preserves_prepared_sink_until_authoritative_commit() {
        use laminar_connectors::connector::{
            SinkConsistency, SinkContract, SinkInputMode, SinkTopology,
        };

        let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let flaky = Arc::new(ManifestCommitThenIoStore {
            inner: Arc::clone(&objects),
            fail_manifest_create: std::sync::atomic::AtomicBool::new(false),
            block_get: std::sync::atomic::AtomicBool::new(false),
        });
        let checkpoint_objects: Arc<dyn object_store::ObjectStore> = flaky.clone();
        let decisions = Arc::new(CheckpointDecisionStore::new(Arc::clone(&objects)));
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&objects), 1_000));
        let leader_boot = uuid::Uuid::from_u128(1);
        let follower_boot = uuid::Uuid::from_u128(2);
        let owner = LeaderLeaseOwner {
            node: NodeId(1),
            boot: leader_boot,
            process_term: 1,
        };
        let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap()
        else {
            panic!("empty checkpoint authority must grant its first leader term");
        };
        let proof = lease.proof();
        let key_groups = KeyGroupCount::try_from(2_u16).unwrap();
        let fence = CheckpointAssignmentFence::from_owner_map(
            1,
            &[1, 2],
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: leader_boot,
                },
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: follower_boot,
                },
            ],
        )
        .unwrap();

        let local_kv = Arc::new(InMemoryKv::new(NodeId(2)));
        let control_kv: Arc<dyn ClusterKv> = local_kv.clone();
        let (_members_tx, members_rx) = tokio::sync::watch::channel(vec![NodeInfo {
            id: NodeId(1),
            name: "leader".into(),
            rpc_address: String::new(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        }]);
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            NodeId(2),
            Arc::clone(&control_kv),
            control_kv,
            None,
            members_rx,
            follower_boot,
        ));
        controller.set_leader_lease_store(Arc::clone(&authority));
        controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
        assert_eq!(
            controller
                .checkpoint_assignment_fence_for_leader(fence.assignment_version, &proof)
                .await,
            Some(fence.clone()),
            "fixture must certify the follower's exact leader and assignment fence"
        );

        let prefix = "follower-manifest-ack-loss";
        let store = ObjectStoreCheckpointStore::new(checkpoint_objects, prefix)
            .with_key_group_count(key_groups)
            .with_participant_id(2);
        let mut coordinator =
            CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
        coordinator
            .bind_durable_decision_store(Arc::clone(&decisions))
            .await
            .unwrap();
        coordinator
            .bind_pipeline_identity(PipelineIdentity::empty())
            .unwrap();
        coordinator.set_assignment_version(fence.assignment_version);
        coordinator.set_vnode_set(vec![1]);
        coordinator.set_cluster_controller(Arc::clone(&controller));

        let rollbacks = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let sink = AmbiguousFollowerSink {
            rollbacks: Arc::clone(&rollbacks),
            schema: Arc::new(arrow::datatypes::Schema::empty()),
        };
        let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
            crate::sink_task::SinkEvent,
        >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
        let sink_handle =
            crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
                name: "probe".into(),
                sink_id: Arc::from("probe"),
                connector: Box::new(sink),
                contract: SinkContract::new(
                    SinkConsistency::CheckpointCommittable,
                    SinkTopology::MultiWriter,
                    SinkInputMode::AppendOnly,
                ),
                requires_recovery_on_error: true,
                channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
                flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
                write_timeout: Duration::from_secs(1),
                event_tx,
                terminal_tasks: None,
                process_authority: None,
            });
        coordinator.register_sink("probe", sink_handle.clone());

        let attempt = CheckpointAttempt::canonical(7);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        coordinator
            .begin_checkpoint_artifacts_until(attempt, Some(fence.clone()), Some(&proof), deadline)
            .await
            .unwrap();
        sink_handle
            .begin_epoch_until(attempt.epoch, deadline)
            .await
            .unwrap();
        let admission = sink_handle
            .begun_epoch_admission(attempt.epoch)
            .expect("sink epoch must be begun before publication");
        sink_handle.publish_open_epoch(admission).unwrap();
        sink_handle
            .seal_epoch_for_protocol_until(attempt.epoch, deadline)
            .await
            .unwrap();
        controller
            .ack_barrier(&BarrierAck {
                epoch: attempt.epoch,
                checkpoint_id: attempt.checkpoint_id,
                assignment_digest: Some(fence.digest()),
                flags: 0,
                disposition: BarrierAckDisposition::Captured,
                error: None,
                watermark: CheckpointWatermark::Uninitialized,
            })
            .await
            .unwrap();

        flaky
            .fail_manifest_create
            .store(true, std::sync::atomic::Ordering::Release);
        let outcome = coordinator
            .follower_prepare_acked_until(
                CheckpointRequest {
                    assignment_fence: Some(fence.clone()),
                    reassignment_portable: true,
                    ..CheckpointRequest::default()
                },
                proof.clone(),
                attempt.epoch,
                attempt.checkpoint_id,
                deadline,
            )
            .await
            .unwrap();
        assert_eq!(outcome, FollowerPrepareOutcome::InDoubt);
        assert_eq!(coordinator.phase, CheckpointPhase::Idle);
        assert_eq!(rollbacks.load(std::sync::atomic::Ordering::Acquire), 0);

        let cached_ack = local_kv
            .read_from_checked(NodeId(2), ACK_KEY)
            .await
            .unwrap()
            .expect("captured acknowledgement must remain cached");
        let cached_ack: BarrierAck = serde_json::from_str(&cached_ack).unwrap();
        assert_eq!(cached_ack.disposition, BarrierAckDisposition::Captured);
        assert!(cached_ack.error.is_none());

        let (follower_manifest, follower_manifest_bytes) = coordinator
            .prepared
            .get(&attempt)
            .cloned()
            .expect("acknowledgement loss must retain the exact prepared candidate");
        assert_eq!(
            coordinator
                .store
                .load_manifest_for_participant(2, attempt.checkpoint_id)
                .await
                .unwrap()
                .as_ref(),
            Some(follower_manifest.as_ref()),
            "the manifest Create succeeded even though its acknowledgement was lost"
        );

        let leader_store = ObjectStoreCheckpointStore::new(Arc::clone(&objects), prefix)
            .with_key_group_count(key_groups)
            .with_participant_id(1);
        let (mut leader_manifest, leader_payload) = cluster_manifest(
            attempt.checkpoint_id,
            1,
            0,
            &deployment_id,
            &fence,
            key_groups,
        );
        leader_manifest.sink_names = vec!["probe".into()];
        leader_manifest.prepared_sinks = vec![PreparedSinkDescriptor {
            sink_name: "probe".into(),
            format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
            payload: None,
            sha256: checkpoint_descriptor_sha256(None),
        }];
        let leader_manifest_bytes = leader_store
            .save_checkpoint(&leader_manifest, std::slice::from_ref(&leader_payload))
            .await
            .unwrap();
        let manifests = vec![
            (leader_manifest, leader_manifest_bytes),
            ((*follower_manifest).clone(), follower_manifest_bytes),
        ];
        let index = coordinator
            .build_committed_index(
                attempt,
                CheckpointScope::Cluster,
                Some(fence.clone()),
                None,
                &BTreeMap::new(),
                &manifests,
                None,
            )
            .unwrap();
        let reference = decisions.create_committed_checkpoint(&index).await.unwrap();
        authority
            .record_cluster_outcome(
                &proof,
                attempt.epoch,
                attempt.checkpoint_id,
                fence,
                laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
                Some(reference.clone()),
            )
            .await
            .unwrap();

        assert!(coordinator
            .follower_finish_deferred(attempt.epoch, attempt.checkpoint_id, true, Instant::now(),)
            .await
            .unwrap());
        assert!(!coordinator.prepared.contains_key(&attempt));
        assert_eq!(coordinator.last_committed_ref(), Some(&reference));
        assert_eq!(
            rollbacks.load(std::sync::atomic::Ordering::Acquire),
            0,
            "a durable Commit must never follow unilateral follower rollback"
        );
        sink_handle.close().await.unwrap();
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_settlement_resumes_exact_seals_and_rejects_a_genesis_fork() {
        let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let decisions = Arc::new(CheckpointDecisionStore::new(Arc::clone(&objects)));
        let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
        let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&objects), 1_000));
        let leader_boot = uuid::Uuid::from_u128(1);
        let owner = LeaderLeaseOwner {
            node: NodeId(1),
            boot: leader_boot,
            process_term: 1,
        };
        let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap()
        else {
            panic!("empty cluster authority must grant its first leader term");
        };
        let proof = lease.proof();
        let key_groups = KeyGroupCount::try_from(2_u16).unwrap();
        let fence = CheckpointAssignmentFence::from_owner_map(
            1,
            &[1, 2],
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: leader_boot,
                },
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::from_u128(2),
                },
            ],
        )
        .unwrap();

        let control_kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(NodeId(1)));
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
        let controller = Arc::new(ClusterController::new(
            NodeId(1),
            control_kv,
            None,
            members_rx,
        ));
        controller.set_leader_lease_store(Arc::clone(&authority));

        let prefix = "cluster-aborted-artifacts";
        let store = ObjectStoreCheckpointStore::new(Arc::clone(&objects), prefix)
            .with_key_group_count(key_groups)
            .with_participant_id(1);
        let mut coordinator =
            CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
        coordinator
            .bind_durable_decision_store(Arc::clone(&decisions))
            .await
            .unwrap();
        coordinator
            .bind_pipeline_identity(PipelineIdentity::empty())
            .unwrap();
        coordinator.set_cluster_controller(Arc::clone(&controller));

        let nonportable_request = CheckpointRequest {
            assignment_fence: Some(fence.clone()),
            ..CheckpointRequest::default()
        };
        let error = coordinator
            .validate_request(&nonportable_request)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cluster checkpoint requires a vnode-reassignment-portable capture"),
            "{error}"
        );

        let predecessor_attempt = CheckpointAttempt::canonical(1);
        let predecessor_inventory = CheckpointArtifactInventory {
            deployment_id: deployment_id.clone(),
            pipeline_identity: PipelineIdentity::empty(),
            attempt: predecessor_attempt,
            assignment_fence: Some(fence.clone()),
        };
        authority
            .begin_cluster_checkpoint_artifacts(&proof, predecessor_inventory)
            .await
            .unwrap();
        let predecessor_manifests = save_cluster_manifests(
            Arc::clone(&objects),
            prefix,
            1,
            &deployment_id,
            &fence,
            key_groups,
        )
        .await;
        let mut nonportable_manifests = predecessor_manifests.clone();
        nonportable_manifests[1].0.reassignment_portable = false;
        nonportable_manifests[1].1 =
            Bytes::from(checkpoint_manifest_bytes(&nonportable_manifests[1].0).unwrap());
        let error = coordinator
            .build_committed_index(
                predecessor_attempt,
                CheckpointScope::Cluster,
                Some(fence.clone()),
                None,
                &BTreeMap::new(),
                &nonportable_manifests,
                None,
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("must be proven portable across vnode reassignment"),
            "{error}"
        );
        let predecessor_index = coordinator
            .build_committed_index(
                predecessor_attempt,
                CheckpointScope::Cluster,
                Some(fence.clone()),
                None,
                &BTreeMap::new(),
                &predecessor_manifests,
                None,
            )
            .unwrap();
        assert!(predecessor_index.reassignment_portable);
        let predecessor = decisions
            .create_committed_checkpoint(&predecessor_index)
            .await
            .unwrap();
        authority
            .record_cluster_outcome(
                &proof,
                1,
                1,
                fence.clone(),
                laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
                Some(predecessor.clone()),
            )
            .await
            .unwrap();
        assert!(coordinator.last_committed_ref.is_none());
        assert_eq!(
            coordinator
                .authoritative_committed_predecessor_until(
                    CheckpointScope::Cluster,
                    tokio::time::Instant::now() + Duration::from_secs(2),
                )
                .await
                .unwrap(),
            Some(predecessor.clone())
        );

        let attempt = CheckpointAttempt::canonical(2);
        let inventory = CheckpointArtifactInventory {
            deployment_id: deployment_id.clone(),
            pipeline_identity: PipelineIdentity::empty(),
            attempt,
            assignment_fence: Some(fence.clone()),
        };
        authority
            .begin_cluster_checkpoint_artifacts(&proof, inventory.clone())
            .await
            .unwrap();
        let manifests = save_cluster_manifests(
            Arc::clone(&objects),
            prefix,
            2,
            &deployment_id,
            &fence,
            key_groups,
        )
        .await;
        let candidate_index = coordinator
            .build_committed_index(
                attempt,
                CheckpointScope::Cluster,
                Some(fence.clone()),
                Some(predecessor.clone()),
                &predecessor_index.source_watermarks,
                &manifests,
                None,
            )
            .unwrap();
        let candidate = decisions
            .create_committed_checkpoint(&candidate_index)
            .await
            .unwrap();
        authority
            .record_cluster_outcome(
                &proof,
                2,
                2,
                fence,
                laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();

        coordinator.prepared.insert(
            attempt,
            (Arc::new(manifests[0].0.clone()), manifests[0].1.clone()),
        );
        coordinator.failure_requires_recovery = true;
        coordinator.local_watermark = CheckpointWatermark::Active(42);
        coordinator.allocator.advance_epoch_to(9);
        let allocator_epoch = coordinator.allocator.peek_epoch();
        let genesis_error = coordinator.recover_to_epoch(0).await.unwrap_err();
        assert!(
            genesis_error
                .to_string()
                .contains("checkpoint artifacts remain"),
            "{genesis_error}"
        );
        assert!(coordinator.prepared.contains_key(&attempt));
        assert!(coordinator.failure_requires_recovery);
        assert_eq!(coordinator.local_watermark, CheckpointWatermark::Active(42));
        assert_eq!(coordinator.allocator.peek_epoch(), allocator_epoch);

        let first_chunk = manifests[0].0.node_data.chunk;
        let first_identity = checkpoint_artifact_identity_sha256(&inventory, first_chunk).unwrap();
        let sealer = ObjectStoreCheckpointStore::new(Arc::clone(&objects), prefix)
            .with_key_group_count(key_groups)
            .with_participant_id(1);
        assert!(sealer
            .seal_aborted_manifest(first_chunk, &first_identity)
            .await
            .unwrap()
            .is_some());

        assert!(coordinator
            .settle_cluster_checkpoint_artifacts_until(
                &proof,
                tokio::time::Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap());
        assert!(authority
            .cluster_checkpoint_artifacts()
            .await
            .unwrap()
            .is_none());
        assert!(decisions
            .load_committed_checkpoint(&candidate)
            .await
            .is_err());
        assert_eq!(
            decisions
                .load_committed_checkpoint(&predecessor)
                .await
                .unwrap(),
            predecessor_index
        );
        for (manifest, _) in &manifests {
            assert!(coordinator
                .store
                .load_manifest_for_participant(manifest.participant_id, attempt.checkpoint_id)
                .await
                .is_err());
            assert!(coordinator
                .store
                .load_node_data_ranges(
                    manifest.node_data.chunk,
                    manifest.node_data.object_length,
                    &[],
                )
                .await
                .is_err());
        }
        assert!(!coordinator
            .settle_cluster_checkpoint_artifacts_until(
                &proof,
                tokio::time::Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap());

        coordinator.prepared.insert(
            attempt,
            (Arc::new(manifests[0].0.clone()), manifests[0].1.clone()),
        );
        coordinator.last_committed_manifest = Some(Arc::new(predecessor_manifests[0].0.clone()));
        coordinator.last_committed_ref = Some(predecessor);
        coordinator.failure_requires_recovery = true;
        coordinator.local_watermark = CheckpointWatermark::Active(42);
        let genesis_error = coordinator.recover_to_epoch(0).await.unwrap_err();
        assert!(
            genesis_error
                .to_string()
                .contains("cannot replace authoritative committed checkpoint"),
            "{genesis_error}"
        );
        assert!(coordinator.prepared.contains_key(&attempt));
        assert!(coordinator.last_committed_manifest.is_some());
        assert!(coordinator.last_committed_ref.is_some());
        assert!(coordinator.failure_requires_recovery);
        assert_eq!(coordinator.local_watermark, CheckpointWatermark::Active(42));
        assert_eq!(coordinator.allocator.peek_epoch(), allocator_epoch);
    }
}

#[cfg(test)]
mod outcome_tests {
    use super::*;
    use laminar_core::checkpoint::ObjectStoreCheckpointStore;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn outcome_recording_keeps_internal_and_prometheus_counts_in_lockstep() {
        let store = ObjectStoreCheckpointStore::new(Arc::new(InMemory::new()), "metrics");
        let mut coordinator =
            CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
        let registry = prometheus::Registry::new();
        let metrics = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
        coordinator.set_metrics(Arc::clone(&metrics));

        coordinator.record_checkpoint_outcome(
            true,
            CheckpointAttempt::canonical(1),
            Duration::from_millis(10),
            Some(37),
        );
        coordinator.record_checkpoint_outcome(
            false,
            CheckpointAttempt::canonical(2),
            Duration::from_millis(20),
            None,
        );

        let stats = coordinator.stats();
        assert_eq!((stats.completed, stats.failed), (1, 1));
        assert_eq!(stats.last_duration, Some(Duration::from_millis(20)));
        assert_eq!(metrics.checkpoints_completed.get(), 1);
        assert_eq!(metrics.checkpoints_failed.get(), 1);
        assert_eq!(metrics.checkpoint_duration.get_sample_count(), 2);
        assert_eq!(metrics.checkpoint_epoch.get(), 2);
        assert_eq!(metrics.checkpoint_size_bytes.get(), 37);
    }
}

#[cfg(test)]
mod sparse_capture_tests {
    use super::*;
    use laminar_core::checkpoint::{checkpoint_sha256, ObjectStoreCheckpointStore};
    use laminar_core::state::KeyGroupCount;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn local_request_cannot_claim_cluster_reassignment_portability() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(InMemory::new()),
            "local-portability-validation",
        );
        let coordinator =
            CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
        let request = CheckpointRequest {
            reassignment_portable: true,
            ..CheckpointRequest::default()
        };

        let error = coordinator.validate_request(&request).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("local checkpoint cannot claim vnode reassignment portability"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn sparse_capture_carries_only_live_owned_frames_and_refcounts_chunks() {
        let key_groups = KeyGroupCount::try_from(3_u16).unwrap();
        let store = ObjectStoreCheckpointStore::new(Arc::new(InMemory::new()), "sparse-capture")
            .with_key_group_count(key_groups);
        let mut coordinator =
            CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
        coordinator
            .bind_pipeline_identity(PipelineIdentity::empty())
            .unwrap();
        coordinator
            .bind_deployment_id(uuid::Uuid::from_u128(1).to_string())
            .unwrap();

        let mut prior = CheckpointManifest::new_with_key_group_count(1, 1, key_groups);
        prior.bind_participant(coordinator.store.participant_id());
        prior.deployment_id = uuid::Uuid::from_u128(1).to_string();
        prior.owned_vnodes = vec![0, 1, 2];
        let mut prior_bytes = Vec::new();
        for (operator_id, vnode) in [
            ("graph:dropped", 0),
            ("graph:dropped", 1),
            ("graph:dropped", 2),
            ("graph:global", 0),
            ("graph:keep", 0),
            ("graph:keep", 1),
            ("graph:keep", 2),
        ] {
            let payload = format!("{operator_id}-{vnode}").into_bytes();
            let offset = prior_bytes.len() as u64;
            prior_bytes.extend_from_slice(&payload);
            prior.state_frames.push(StateFrame {
                key: StateFrameKey::Vnode {
                    operator_id: operator_id.into(),
                    vnode,
                },
                chunk: prior.node_data.chunk,
                range: ByteRange {
                    offset,
                    length: payload.len() as u64,
                },
                sha256: checkpoint_sha256(&payload),
            });
        }
        prior.node_data.object_length = prior_bytes.len() as u64;
        prior.node_data.sha256 = checkpoint_sha256(&prior_bytes);
        coordinator.last_committed_manifest = Some(Arc::new(prior));
        let request = || CheckpointRequest {
            state_frames: vec![CapturedStateFrame {
                key: StateFrameKey::Vnode {
                    operator_id: "graph:keep".into(),
                    vnode: 2,
                },
                state: Some(Bytes::from_static(b"new-two")),
            }],
            managed_vnode_operators: vec![
                ManagedVnodeOperator {
                    operator_id: "graph:keep".into(),
                    placement: ManagedVnodePlacement::VnodeKeyed,
                },
                ManagedVnodeOperator {
                    operator_id: "graph:global".into(),
                    placement: ManagedVnodePlacement::GlobalSingleton,
                },
            ],
            ..CheckpointRequest::default()
        };

        coordinator.set_vnode_set(vec![0, 2]);
        let mut reassigned = request();
        reassigned
            .state_frames
            .sort_unstable_by(|left, right| left.key.cmp(&right.key));
        coordinator
            .complete_sparse_vnode_captures(&mut reassigned)
            .unwrap();
        assert!(reassigned
            .state_frames
            .iter()
            .all(|capture| match &capture.key {
                StateFrameKey::Vnode { operator_id, vnode } => {
                    operator_id != "graph:dropped" && *vnode != 1
                }
                StateFrameKey::OperatorWhole { .. } => true,
            }));

        coordinator.set_vnode_set(vec![0, 1, 2]);
        let packed = coordinator
            .pack_checkpoint(
                CheckpointAttempt::canonical(2),
                request(),
                BTreeMap::new(),
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();

        let keys = packed
            .manifest
            .state_frames
            .iter()
            .map(|frame| frame.key.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                StateFrameKey::Vnode {
                    operator_id: "graph:global".into(),
                    vnode: 0,
                },
                StateFrameKey::Vnode {
                    operator_id: "graph:keep".into(),
                    vnode: 0,
                },
                StateFrameKey::Vnode {
                    operator_id: "graph:keep".into(),
                    vnode: 1,
                },
                StateFrameKey::Vnode {
                    operator_id: "graph:keep".into(),
                    vnode: 2,
                },
            ]
        );
        assert_eq!(packed.manifest.referenced_chunks.len(), 1);
        assert_eq!(packed.manifest.referenced_chunks[0].ref_count.get(), 3);
        assert_eq!(packed.node_data, vec![Bytes::from_static(b"new-two")]);
        assert!(!packed.manifest.reassignment_portable);
    }

    #[tokio::test]
    async fn committed_manifest_rebases_at_referenced_chunk_threshold() {
        let key_groups = KeyGroupCount::try_from(1_u16).unwrap();
        let mut coordinator = CheckpointCoordinator::new(
            CheckpointConfig::default(),
            Box::new(
                ObjectStoreCheckpointStore::new(Arc::new(InMemory::new()), "chunk-threshold")
                    .with_key_group_count(key_groups),
            ),
        )
        .unwrap();
        let mut manifest = CheckpointManifest::new_with_key_group_count(65, 65, key_groups);
        manifest.deployment_id = uuid::Uuid::from_u128(1).to_string();
        manifest.referenced_chunks = (1..=REFERENCED_CHUNK_REBASE_THRESHOLD)
            .map(|checkpoint_id| ReferencedStateChunk {
                chunk: StateChunkId {
                    participant_id: 1,
                    checkpoint_id: u64::try_from(checkpoint_id).unwrap(),
                },
                object_length: 1,
                sha256: checkpoint_sha256(b"x"),
                ref_count: NonZeroU32::new(1).unwrap(),
            })
            .collect();
        manifest.state_frames = (1..=REFERENCED_CHUNK_REBASE_THRESHOLD)
            .map(|checkpoint_id| StateFrame {
                key: StateFrameKey::Vnode {
                    operator_id: format!("graph:{checkpoint_id:020}"),
                    vnode: 0,
                },
                chunk: StateChunkId {
                    participant_id: 1,
                    checkpoint_id: u64::try_from(checkpoint_id).unwrap(),
                },
                range: ByteRange {
                    offset: 0,
                    length: 1,
                },
                sha256: checkpoint_sha256(b"x"),
            })
            .collect();
        assert!(manifest.validate(key_groups).is_empty());
        coordinator.last_committed_manifest = Some(Arc::new(manifest));

        assert!(coordinator.committed_manifest_needs_vnode_rebase(CheckpointAttempt::canonical(65)));
        assert!(
            !coordinator.committed_manifest_needs_vnode_rebase(CheckpointAttempt::canonical(66))
        );
        Arc::make_mut(
            coordinator
                .last_committed_manifest
                .as_mut()
                .expect("installed manifest"),
        )
        .referenced_chunks
        .pop();
        assert!(
            !coordinator.committed_manifest_needs_vnode_rebase(CheckpointAttempt::canonical(65))
        );
    }
}

#[cfg(all(test, feature = "cluster"))]
#[path = "checkpoint_coordinator_handoff_tests.rs"]
mod handoff_tests;

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
    last_committed_source_watermarks: Option<(CommittedCheckpointRef, BTreeMap<String, i64>)>,
    failure_requires_recovery: bool,
    local_watermark: laminar_core::checkpoint::CheckpointWatermark,
    #[cfg(feature = "cluster")]
    cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    #[cfg(feature = "cluster")]
    recovery_graph_payload_limit: usize,
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
        #[cfg(feature = "cluster")]
        let recovery_graph_payload_limit =
            usize::try_from(config.max_node_data_bytes).unwrap_or(usize::MAX);
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
            last_committed_source_watermarks: None,
            failure_requires_recovery: false,
            local_watermark: laminar_core::checkpoint::CheckpointWatermark::Uninitialized,
            #[cfg(feature = "cluster")]
            cluster_controller: None,
            #[cfg(feature = "cluster")]
            recovery_graph_payload_limit,
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

    fn checkpoint_artifact_inventory(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
    ) -> Result<CheckpointArtifactInventory, DbError> {
        if assignment_fence.is_none()
            && self.store.participant_id() != laminar_core::state::LOCAL_NODE_ID.0
        {
            return Err(DbError::Checkpoint(
                "local checkpoint artifacts require the singleton local participant".into(),
            ));
        }
        let inventory = CheckpointArtifactInventory {
            deployment_id: self.expected_deployment_id()?.to_owned(),
            pipeline_identity: self.expected_pipeline_identity()?,
            attempt: require_canonical_attempt(attempt, "checkpoint artifact admission")?,
            assignment_fence,
        };
        inventory.validate().map_err(DbError::Checkpoint)?;
        Ok(inventory)
    }

    fn validate_checkpoint_artifact_inventory(
        &self,
        inventory: &CheckpointArtifactInventory,
    ) -> Result<(), DbError> {
        inventory.validate().map_err(DbError::Checkpoint)?;
        if inventory.deployment_id != self.expected_deployment_id()?
            || inventory.pipeline_identity != self.expected_pipeline_identity()?
        {
            return Err(DbError::Checkpoint(
                "checkpoint artifact inventory does not belong to this pipeline deployment".into(),
            ));
        }
        Ok(())
    }

    async fn authoritative_committed_predecessor_until(
        &self,
        scope: CheckpointScope,
        deadline: tokio::time::Instant,
    ) -> Result<Option<CommittedCheckpointRef>, DbError> {
        #[cfg(feature = "cluster")]
        if scope == CheckpointScope::Cluster {
            let controller = self.cluster_controller.as_ref().ok_or_else(|| {
                DbError::Checkpoint("cluster checkpoint has no cluster controller".into())
            })?;
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
            })?;
            let outcome =
                tokio::time::timeout_at(deadline, authority.highest_cluster_committed_outcome())
                    .await
                    .map_err(|_| DbError::Checkpoint("cluster predecessor read timed out".into()))?
                    .map_err(|error| {
                        DbError::Checkpoint(format!("cluster predecessor read failed: {error}"))
                    })?;
            return outcome
                .map(|outcome| {
                    outcome.committed_checkpoint.ok_or_else(|| {
                        DbError::Checkpoint(
                            "cluster predecessor Commit has no checkpoint reference".into(),
                        )
                    })
                })
                .transpose();
        }

        if scope != CheckpointScope::Local {
            return Err(DbError::Checkpoint(
                "cluster checkpointing requires the cluster feature".into(),
            ));
        }
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("local predecessor read requires a decision store".into())
        })?;
        let head = tokio::time::timeout_at(deadline, store.checkpoint_decision_head())
            .await
            .map_err(|_| DbError::Checkpoint("local predecessor read timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("local predecessor read failed: {error}"))
            })?;
        head.and_then(|head| head.latest_commit)
            .map(|outcome| {
                outcome.committed_checkpoint.ok_or_else(|| {
                    DbError::Checkpoint(
                        "local predecessor Commit has no checkpoint reference".into(),
                    )
                })
            })
            .transpose()
    }

    async fn predecessor_source_watermarks_until(
        &self,
        predecessor: Option<&CommittedCheckpointRef>,
        deadline: tokio::time::Instant,
    ) -> Result<BTreeMap<String, i64>, DbError> {
        let Some(predecessor) = predecessor else {
            return Ok(BTreeMap::new());
        };
        if let Some((cached_reference, source_watermarks)) =
            self.last_committed_source_watermarks.as_ref()
        {
            if cached_reference == predecessor {
                return Ok(source_watermarks.clone());
            }
        }
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "committed predecessor source-watermark read requires a decision store".into(),
            )
        })?;
        let committed =
            tokio::time::timeout_at(deadline, store.load_committed_checkpoint(predecessor))
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "committed predecessor source-watermark read timed out".into(),
                    )
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "committed predecessor source-watermark read failed: {error}"
                    ))
                })?;
        committed
            .effective_source_watermarks()
            .map_err(DbError::Checkpoint)
    }

    pub(crate) async fn begin_checkpoint_artifacts_until(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<&LeaderProof>,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let inventory = self.checkpoint_artifact_inventory(attempt, assignment_fence)?;

        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.as_ref() {
            let proof = leader_proof.ok_or_else(|| {
                DbError::Checkpoint("cluster artifact admission has no leader proof".into())
            })?;
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
            })?;
            let admitted = tokio::time::timeout_at(
                deadline,
                authority.begin_cluster_checkpoint_artifacts(proof, inventory.clone()),
            )
            .await
            .map_err(|_| DbError::Checkpoint("cluster artifact admission timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("cluster artifact admission failed: {error}"))
            })?;
            if admitted != inventory {
                return Err(DbError::Checkpoint(
                    "cluster artifact admission returned a different inventory".into(),
                ));
            }
            return Ok(());
        }

        if inventory.assignment_fence.is_some() || leader_proof.is_some() {
            return Err(DbError::Checkpoint(
                "local artifact admission cannot carry cluster authority".into(),
            ));
        }
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("checkpoint artifact admission requires a decision store".into())
        })?;
        let result = tokio::time::timeout_at(
            deadline,
            store.begin_checkpoint_artifact_inventory(inventory),
        )
        .await
        .map_err(|_| DbError::Checkpoint("checkpoint artifact admission timed out".into()))?
        .map_err(|error| {
            DbError::Checkpoint(format!("checkpoint artifact admission failed: {error}"))
        })?;
        match result {
            CheckpointArtifactInventoryUpdateResult::Applied
            | CheckpointArtifactInventoryUpdateResult::Unchanged => Ok(()),
            CheckpointArtifactInventoryUpdateResult::Conflict { current } => {
                Err(DbError::Checkpoint(format!(
                    "checkpoint artifact admission conflicts with {current:?}"
                )))
            }
        }
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

    fn emit_checkpoint_metrics(
        &self,
        success: bool,
        attempt: CheckpointAttempt,
        duration: Duration,
        checkpoint_bytes: Option<u64>,
    ) {
        let Some(metrics) = self.prom.as_ref() else {
            return;
        };
        if success {
            metrics.checkpoints_completed.inc();
            if let Some(checkpoint_bytes) = checkpoint_bytes {
                metrics
                    .checkpoint_size_bytes
                    .set(i64::try_from(checkpoint_bytes).unwrap_or(i64::MAX));
            }
        } else {
            metrics.checkpoints_failed.inc();
            warn!(
                checkpoint_id = attempt.checkpoint_id,
                epoch = attempt.epoch,
                "checkpoint failure metric recorded"
            );
        }
        metrics
            .checkpoint_epoch
            .set(i64::try_from(attempt.epoch).unwrap_or(i64::MAX));
        metrics.checkpoint_duration.observe(duration.as_secs_f64());
    }

    fn record_checkpoint_outcome(
        &mut self,
        success: bool,
        attempt: CheckpointAttempt,
        duration: Duration,
        checkpoint_bytes: Option<u64>,
    ) {
        if success {
            self.checkpoints_completed = self.checkpoints_completed.saturating_add(1);
        } else {
            self.checkpoints_failed = self.checkpoints_failed.saturating_add(1);
        }
        self.last_checkpoint_duration = Some(duration);
        self.duration_histogram.record(duration);
        self.emit_checkpoint_metrics(success, attempt, duration, checkpoint_bytes);
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
        leader_proof: Option<&LeaderProof>,
    ) {
        let Some(decision_store) = self.decision_store.as_ref() else {
            return;
        };
        let authority = match current.scope {
            CheckpointScope::Local => GcAuthority::Local,
            CheckpointScope::Cluster => {
                #[cfg(feature = "cluster")]
                {
                    let Some(proof) = leader_proof.cloned() else {
                        warn!("cluster checkpoint retention has no live leader proof");
                        return;
                    };
                    let Some(controller) = self.cluster_controller.as_ref() else {
                        warn!("cluster checkpoint retention has no cluster controller");
                        return;
                    };
                    if controller.checkpoint_drain_transition().is_some() {
                        return;
                    }
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
        if controller.checkpoint_drain_transition().is_some() {
            return Ok(());
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

    #[cfg(feature = "cluster")]
    pub(crate) fn set_recovery_graph_payload_limit(&mut self, bytes: usize) {
        debug_assert_ne!(bytes, 0);
        self.recovery_graph_payload_limit = bytes;
    }

    #[must_use]
    pub(crate) fn epoch_allocator(&self) -> Arc<EpochAllocator> {
        Arc::clone(&self.allocator)
    }

    #[must_use]
    pub fn store(&self) -> &dyn CheckpointStore {
        self.store.as_ref()
    }

    #[cfg(feature = "cluster")]
    pub(crate) async fn load_handoff_state_frames(
        &self,
        pinned: &CommittedCheckpointRef,
        predecessor: &laminar_core::checkpoint::CheckpointAssignmentFence,
        predecessor_owners: &[laminar_core::state::NodeId],
        acquired_vnodes: &[u32],
        include_whole: bool,
        max_payload_bytes: usize,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<RecoveredStateFrame>, DbError> {
        if tokio::time::Instant::now() >= deadline {
            return Err(handoff_error("vnode handoff read timed out"));
        }
        let owner_ids = predecessor_owners
            .iter()
            .map(|owner| owner.0)
            .collect::<Vec<_>>();
        let key_group_count = self.store.key_group_count();
        if !predecessor.is_canonical()
            || predecessor.vnode_count != u32::from(key_group_count)
            || owner_ids.len() != usize::from(key_group_count.get())
            || !predecessor.matches_owner_map(&owner_ids)
        {
            return Err(handoff_error(
                "predecessor fence does not match the exact vnode owner map",
            ));
        }
        if acquired_vnodes.is_empty()
            || acquired_vnodes.windows(2).any(|pair| pair[0] >= pair[1])
            || acquired_vnodes
                .iter()
                .any(|vnode| *vnode >= predecessor.vnode_count)
        {
            return Err(handoff_error(
                "acquired vnode roster must be nonempty, canonical, and in range",
            ));
        }

        let decision_store = self.decision_store.as_ref().ok_or_else(|| {
            handoff_error("vnode handoff requires a durable checkpoint decision store")
        })?;
        let committed =
            tokio::time::timeout_at(deadline, decision_store.load_committed_checkpoint(pinned))
                .await
                .map_err(|_| handoff_error("committed handoff checkpoint read timed out"))?
                .map_err(|error| {
                    handoff_error(format!("committed handoff checkpoint read failed: {error}"))
                })?;
        let pipeline_identity = self.expected_pipeline_identity()?;
        let deployment_id = self.expected_deployment_id()?.to_owned();
        if committed.pipeline_identity != pipeline_identity {
            return Err(handoff_error(
                "handoff checkpoint pipeline identity does not match the active pipeline",
            ));
        }
        if committed.deployment_id != deployment_id {
            return Err(handoff_error(
                "handoff checkpoint deployment does not match the active deployment",
            ));
        }
        if committed.scope != CheckpointScope::Cluster {
            return Err(handoff_error(
                "vnode handoff requires a cluster-scoped committed checkpoint",
            ));
        }
        if !committed.reassignment_portable
            || committed.assignment_fence.as_ref() != Some(predecessor)
            || u32::from(committed.vnode_count) != predecessor.vnode_count
        {
            return Err(handoff_error(
                "handoff checkpoint is not portable or does not cover the exact predecessor assignment",
            ));
        }

        let mut requested_by_donor = BTreeMap::<u64, Vec<u16>>::new();
        for &vnode in acquired_vnodes {
            let vnode16 = u16::try_from(vnode)
                .map_err(|_| handoff_error(format!("acquired vnode {vnode} exceeds u16")))?;
            let donor = predecessor_owners[vnode as usize].0;
            requested_by_donor.entry(donor).or_default().push(vnode16);
        }
        let mut expected_by_donor = requested_by_donor
            .keys()
            .copied()
            .map(|donor| (donor, Vec::new()))
            .collect::<BTreeMap<_, Vec<u16>>>();
        for (vnode, owner) in predecessor_owners.iter().enumerate() {
            if let Some(expected) = expected_by_donor.get_mut(&owner.0) {
                expected.push(u16::try_from(vnode).map_err(|_| {
                    handoff_error("predecessor vnode owner map exceeds the checkpoint ABI")
                })?);
            }
        }

        let donors = requested_by_donor
            .into_iter()
            .map(|(participant_id, requested)| {
                let participant = committed
                    .participants
                    .binary_search_by_key(&participant_id, |entry| entry.participant_id)
                    .ok()
                    .map(|index| committed.participants[index].clone())
                    .ok_or_else(|| {
                        handoff_error(format!(
                            "vnode donor {participant_id} is absent from the committed checkpoint"
                        ))
                    })?;
                let expected = expected_by_donor.remove(&participant_id).ok_or_else(|| {
                    handoff_error(format!(
                        "vnode donor {participant_id} has no predecessor ownership roster"
                    ))
                })?;
                Ok((participant, requested, expected))
            })
            .collect::<Result<Vec<_>, DbError>>()?;

        let manifest_bytes = donors.iter().try_fold(0usize, |total, donor| {
            let bytes = usize::try_from(donor.0.manifest_len).map_err(|_| {
                DbError::ManagedStateBudgetExceeded {
                    context: "[LDB-6050] vnode handoff manifests".into(),
                    accounted_bytes: usize::MAX,
                    limit_bytes: max_payload_bytes,
                }
            })?;
            total
                .checked_add(bytes)
                .ok_or_else(|| DbError::ManagedStateBudgetExceeded {
                    context: "[LDB-6050] vnode handoff manifests".into(),
                    accounted_bytes: usize::MAX,
                    limit_bytes: max_payload_bytes,
                })
        })?;
        if manifest_bytes > max_payload_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: "[LDB-6050] vnode handoff manifests".into(),
                accounted_bytes: manifest_bytes,
                limit_bytes: max_payload_bytes,
            });
        }

        let checkpoint_id = committed.checkpoint_id;
        let checkpoint_epoch = committed.epoch;
        let committed_vnode_count = committed.vnode_count;
        let manifest_reads = donors
            .into_iter()
            .map(|(participant, requested, expected)| {
                let store = Arc::clone(&self.store);
                let pipeline_identity = &pipeline_identity;
                let deployment_id = &deployment_id;
                async move {
                    let participant_id = participant.participant_id;
                    let manifest = tokio::time::timeout_at(
                        deadline,
                        store.load_manifest_verified(
                            participant_id,
                            checkpoint_id,
                            participant.manifest_len,
                            &participant.manifest_sha256,
                        ),
                    )
                    .await
                    .map_err(|_| {
                        handoff_error(format!(
                            "participant {participant_id} handoff manifest read timed out"
                        ))
                    })?
                    .map_err(|error| {
                        handoff_error(format!(
                            "participant {participant_id} handoff manifest read failed: {error}"
                        ))
                    })?
                    .ok_or_else(|| {
                        handoff_error(format!(
                            "participant {participant_id} handoff manifest is missing"
                        ))
                    })?;
                    if manifest.participant_id != participant_id
                        || manifest.node_data.chunk.participant_id != participant_id
                        || manifest.node_data.object_length != participant.node_data_len
                        || manifest.node_data.sha256 != participant.node_data_sha256
                        || manifest.epoch != checkpoint_epoch
                        || manifest.checkpoint_id != checkpoint_id
                        || manifest.deployment_id != deployment_id.as_str()
                        || &manifest.pipeline_identity != pipeline_identity
                        || manifest.vnode_count != committed_vnode_count
                        || manifest.assignment_fence.as_ref() != Some(predecessor)
                        || manifest.owned_vnodes != expected
                    {
                        return Err(handoff_error(format!(
                        "participant {participant_id} manifest does not match the exact handoff cut"
                    )));
                    }

                    let selected = manifest
                        .state_frames
                        .iter()
                        .filter(|frame| match &frame.key {
                            StateFrameKey::OperatorWhole { operator_id } => {
                                include_whole
                                    && operator_id
                                        .strip_prefix("graph:")
                                        .is_some_and(|suffix| !suffix.is_empty())
                            }
                            StateFrameKey::Vnode { operator_id, vnode } => {
                                operator_id
                                    .strip_prefix("graph:")
                                    .is_some_and(|suffix| !suffix.is_empty())
                                    && requested.binary_search(vnode).is_ok()
                            }
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    let selected_bytes = selected.iter().try_fold(0usize, |total, frame| {
                        let bytes = usize::try_from(frame.range.length).map_err(|_| {
                            DbError::ManagedStateBudgetExceeded {
                                context: "[LDB-6050] vnode handoff payload".into(),
                                accounted_bytes: usize::MAX,
                                limit_bytes: max_payload_bytes,
                            }
                        })?;
                        total.checked_add(bytes).ok_or_else(|| {
                            DbError::ManagedStateBudgetExceeded {
                                context: "[LDB-6050] vnode handoff payload".into(),
                                accounted_bytes: usize::MAX,
                                limit_bytes: max_payload_bytes,
                            }
                        })
                    })?;
                    let plan = VerifiedStateFramePlan::new(&manifest, &selected)?;
                    Ok((plan, selected_bytes))
                }
            });
        let loaded = futures::stream::iter(manifest_reads)
            .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY)
            .try_collect::<Vec<(VerifiedStateFramePlan, usize)>>()
            .await?;

        let payload_bytes = loaded.iter().try_fold(0usize, |total, (_, bytes)| {
            total
                .checked_add(*bytes)
                .ok_or_else(|| DbError::ManagedStateBudgetExceeded {
                    context: "[LDB-6050] vnode handoff payload".into(),
                    accounted_bytes: usize::MAX,
                    limit_bytes: max_payload_bytes,
                })
        })?;
        if payload_bytes > max_payload_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: "[LDB-6050] vnode handoff payload".into(),
                accounted_bytes: payload_bytes,
                limit_bytes: max_payload_bytes,
            });
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(handoff_error("vnode handoff read timed out"));
        }

        let plans = loaded.into_iter().map(|(plan, _)| plan).collect();
        tokio::time::timeout_at(
            deadline,
            load_verified_state_frames(self.store.as_ref(), plans),
        )
        .await
        .map_err(|_| handoff_error("vnode handoff frame read timed out"))?
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
        let recovery_scope = self.recovery_scope();
        #[cfg(feature = "cluster")]
        let cluster_target = if recovery_scope == CheckpointScope::Cluster {
            let controller = self.cluster_controller.as_ref().ok_or_else(|| {
                DbError::Checkpoint("cluster recovery has no cluster controller".into())
            })?;
            let assignment = controller
                .checkpoint_assignment_fence(self.assignment_version)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "cluster recovery has no active assignment fence for version {}",
                        self.assignment_version
                    ))
                })?;
            Some(crate::recovery_manager::ClusterRecoveryTarget {
                assignment,
                owned_vnodes: self.owned_vnodes.clone(),
                max_graph_payload_bytes: self.recovery_graph_payload_limit,
            })
        } else {
            None
        };
        #[cfg(not(feature = "cluster"))]
        let cluster_target = None;
        #[cfg(feature = "cluster")]
        if let Some(target) = cluster_target.as_ref() {
            let predecessor = committed.assignment_fence.as_ref().ok_or_else(|| {
                DbError::Checkpoint("cluster recovery checkpoint has no assignment fence".into())
            })?;
            if predecessor != &target.assignment {
                if predecessor.assignment_version >= target.assignment.assignment_version
                    || predecessor.vnode_count != target.assignment.vnode_count
                    || predecessor.partitioning_abi_version
                        != target.assignment.partitioning_abi_version
                    || !committed.reassignment_portable
                {
                    return Err(DbError::Checkpoint(format!(
                        "recovery target assignment {} is not a compatible newer bootstrap target for committed assignment {}",
                        target.assignment.assignment_version, predecessor.assignment_version
                    )));
                }
                let expected = outcome.committed_checkpoint.as_ref().ok_or_else(|| {
                    DbError::Checkpoint(
                        "cluster Commit outcome has no committed checkpoint reference".into(),
                    )
                })?;
                let controller = self.cluster_controller.as_ref().ok_or_else(|| {
                    DbError::Checkpoint("cluster recovery has no cluster controller".into())
                })?;
                let authority = controller.checkpoint_authority().map_err(|error| {
                    DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
                })?;
                let pinned = tokio::time::timeout_at(
                    deadline,
                    authority.assignment_handoff_checkpoint(&target.assignment),
                )
                .await
                .map_err(|_| {
                    DbError::Checkpoint("assignment handoff checkpoint lookup timed out".into())
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "assignment handoff checkpoint lookup failed: {error}"
                    ))
                })?;
                if pinned.as_ref() != Some(expected) {
                    return Err(DbError::Checkpoint(
                        "recovery checkpoint is not the durable handoff pin for the active assignment"
                            .into(),
                    ));
                }
            }
        }
        let recovered = {
            let manager = crate::recovery_manager::RecoveryManager::new(
                self.store.as_ref(),
                &pipeline_identity,
                &deployment_id,
                recovery_scope,
            );
            tokio::time::timeout_at(
                deadline,
                manager.recover_committed_for_target(&outcome, &committed, cluster_target),
            )
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
            self.schedule_retention(committed.clone(), continuation_proof.as_ref());
        }

        let reference = outcome.committed_checkpoint.clone().ok_or_else(|| {
            DbError::Checkpoint("Commit outcome has no committed checkpoint reference".into())
        })?;
        let committed_source_watermarks = committed
            .effective_source_watermarks()
            .map_err(DbError::Checkpoint)?;
        let local_manifest = (!recovered.reassigned)
            .then(|| {
                recovered
                    .manifests
                    .iter()
                    .find(|manifest| manifest.participant_id == self.store.participant_id())
                    .cloned()
                    .map(Arc::new)
            })
            .flatten();
        self.local_watermark = Self::manifest_watermark(local_manifest.as_deref())?;
        self.last_committed_manifest = local_manifest;
        self.last_committed_ref = Some(reference.clone());
        self.last_committed_source_watermarks =
            Some((reference, committed_source_watermarks.clone()));
        self.prepared.clear();
        self.allocator.advance_epoch_to(checked_successor_epoch(
            committed.epoch,
            "installing recovered checkpoint",
        )?);
        self.failure_requires_recovery = false;
        self.phase = CheckpointPhase::Idle;
        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.as_ref() {
            controller
                .replace_recovered_checkpoint_progress(
                    &committed.channel_progress,
                    &committed_source_watermarks,
                )
                .map_err(DbError::Checkpoint)?;
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
        let mut head = tokio::time::timeout_at(deadline, store.checkpoint_decision_head())
            .await
            .map_err(|_| DbError::Checkpoint("checkpoint recovery selection timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("checkpoint recovery selection: {error}"))
            })?;
        if let Some(inventory) = head.as_ref().and_then(|head| head.active_artifacts.clone()) {
            self.validate_checkpoint_artifact_inventory(&inventory)?;
            if inventory.assignment_fence.is_some() {
                return Err(DbError::Checkpoint(
                    "local recovery found cluster artifact inventory".into(),
                ));
            }
            let exact_abort = head
                .as_ref()
                .and_then(|head| head.latest_terminal.as_ref())
                .is_some_and(|outcome| {
                    !outcome.is_commit()
                        && outcome.epoch == inventory.attempt.epoch
                        && outcome.checkpoint_id == inventory.attempt.checkpoint_id
                });
            if !exact_abort {
                if head
                    .as_ref()
                    .and_then(|head| head.latest_terminal.as_ref())
                    .is_some_and(|outcome| outcome.epoch >= inventory.attempt.epoch)
                {
                    return Err(DbError::Checkpoint(format!(
                        "checkpoint {} has incompatible terminal authority while artifacts remain",
                        inventory.attempt.checkpoint_id
                    )));
                }
                self.record_outcome_until(
                    inventory.attempt,
                    laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
                    None,
                    None,
                    None,
                    deadline,
                )
                .await?;
            }
            self.cleanup_local_checkpoint_artifacts_until(inventory.attempt, deadline)
                .await?;
            head = tokio::time::timeout_at(deadline, store.checkpoint_decision_head())
                .await
                .map_err(|_| DbError::Checkpoint("checkpoint recovery selection timed out".into()))?
                .map_err(|error| {
                    DbError::Checkpoint(format!("checkpoint recovery selection: {error}"))
                })?;
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
        if self.phase != CheckpointPhase::Idle {
            return Err(DbError::Checkpoint(
                "cannot recover while a checkpoint is in progress".into(),
            ));
        }
        let deadline = tokio::time::Instant::now() + self.config.checkpoint_timeout;
        if epoch == 0 {
            let controller = self.cluster_controller.as_ref().ok_or_else(|| {
                DbError::Checkpoint(
                    "epoch-targeted recovery requires cluster checkpoint authority".into(),
                )
            })?;
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
            })?;
            if tokio::time::timeout_at(deadline, authority.cluster_checkpoint_artifacts())
                .await
                .map_err(|_| {
                    DbError::Checkpoint("genesis recovery artifact audit timed out".into())
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!("genesis recovery artifact audit failed: {error}"))
                })?
                .is_some()
            {
                return Err(DbError::Checkpoint(
                    "genesis recovery cannot install while checkpoint artifacts remain".into(),
                ));
            }
            if let Some(reference) = self
                .authoritative_committed_predecessor_until(CheckpointScope::Cluster, deadline)
                .await?
            {
                return Err(DbError::Checkpoint(format!(
                    "genesis recovery cannot replace authoritative committed checkpoint {}",
                    reference.checkpoint_id
                )));
            }
            self.prepared.clear();
            self.last_committed_manifest = None;
            self.last_committed_ref = None;
            self.last_committed_source_watermarks = None;
            self.local_watermark = CheckpointWatermark::Uninitialized;
            self.failure_requires_recovery = false;
            self.phase = CheckpointPhase::Idle;
            controller
                .replace_recovered_checkpoint_progress(&[], &BTreeMap::new())
                .map_err(DbError::Checkpoint)?;
            return Ok(None);
        }
        let authoritative = self
            .authoritative_committed_predecessor_until(CheckpointScope::Cluster, deadline)
            .await?;
        let authoritative = authoritative.ok_or_else(|| {
            DbError::Checkpoint(format!(
                "cluster epoch {epoch} cannot be recovered because no authoritative Commit exists"
            ))
        })?;
        if authoritative.epoch != epoch || authoritative.checkpoint_id != epoch {
            return Err(DbError::Checkpoint(format!(
                "cluster epoch {epoch} is not the authoritative committed recovery head {}",
                authoritative.checkpoint_id
            )));
        }
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
            if outcome.committed_checkpoint.as_ref() != Some(&authoritative) {
                return Err(DbError::Checkpoint(
                    "cluster recovery outcome does not match the authoritative committed head"
                        .into(),
                ));
            }
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
        publication: SinkEpochPublication,
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
            let admissions = self
                .sinks
                .iter()
                .filter(|sink| sink.handle.checkpoint_committable())
                .map(|sink| {
                    (
                        sink.name.as_str(),
                        sink.handle.begun_epoch_admission(attempt.epoch),
                    )
                })
                .collect::<Vec<_>>();
            let expected = admissions.first().and_then(|(_, admission)| *admission);
            let invalid = admissions
                .iter()
                .filter_map(|(name, admission)| {
                    (*admission != expected || admission.is_none()).then_some(*name)
                })
                .collect::<Vec<_>>();
            if !invalid.is_empty() {
                for sink in self
                    .sinks
                    .iter()
                    .filter(|sink| sink.handle.checkpoint_committable())
                {
                    sink.handle.fail_epoch_gate();
                }
                self.allocator.mark_sink_epoch_in_doubt(attempt);
                self.failure_requires_recovery = true;
                return Err(DbError::Checkpoint(format!(
                    "sink epoch {} begin acknowledgement did not leave every gate Begun: {}",
                    attempt.epoch,
                    invalid.join(", ")
                )));
            }
            let admission = expected.ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "sink epoch {} has no committable gate admission",
                    attempt.epoch
                ))
            })?;
            if let Err(error) = self.allocator.mark_sink_epoch_ready(attempt) {
                for sink in self
                    .sinks
                    .iter()
                    .filter(|sink| sink.handle.checkpoint_committable())
                {
                    sink.handle.fail_epoch_gate();
                }
                self.failure_requires_recovery = true;
                return Err(error);
            }
            if publication == SinkEpochPublication::Immediate {
                let mut publication_error = None;
                for sink in self
                    .sinks
                    .iter()
                    .filter(|sink| sink.handle.checkpoint_committable())
                {
                    if let Err(error) = sink.handle.publish_open_epoch(admission) {
                        publication_error = Some(DbError::Checkpoint(format!(
                            "sink '{}' epoch {} publication failed: {error}",
                            sink.name, attempt.epoch
                        )));
                        break;
                    }
                }
                if let Some(error) = publication_error {
                    for sink in self
                        .sinks
                        .iter()
                        .filter(|sink| sink.handle.checkpoint_committable())
                    {
                        sink.handle.fail_epoch_gate();
                    }
                    self.failure_requires_recovery = true;
                    return Err(error);
                }
            }
            return Ok(());
        }

        for sink in self
            .sinks
            .iter()
            .filter(|sink| sink.handle.checkpoint_committable())
        {
            sink.handle.fail_epoch_gate();
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
        self.begin_sink_epoch_until(deadline, SinkEpochPublication::Immediate)
            .await
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
            for sink in self
                .sinks
                .iter()
                .filter(|sink| sink.handle.checkpoint_committable())
            {
                sink.handle.fail_epoch_gate();
            }
            Err(DbError::Checkpoint(format!(
                "sink rollback failed: {}",
                failures.join("; ")
            )))
        }
    }

    async fn seal_sink_epoch_until(
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
                    async move {
                        (
                            name,
                            handle.seal_epoch_for_protocol_until(epoch, deadline).await,
                        )
                    }
                }),
        )
        .await;
        let mut admission = None;
        let mut failures = Vec::new();
        for (name, result) in results {
            match result {
                Ok(Some(current)) if admission.is_none_or(|expected| expected == current) => {
                    admission = Some(current);
                }
                Ok(Some(current)) => failures.push(format!(
                    "{name}: mismatched sink transition admission {current:?}"
                )),
                Ok(None) => {}
                Err(error) => failures.push(format!("{name}: {error}")),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            for sink in self
                .sinks
                .iter()
                .filter(|sink| sink.handle.checkpoint_committable())
            {
                sink.handle.fail_epoch_gate();
            }
            Err(DbError::Checkpoint(format!(
                "sink epoch {epoch} seal failed: {}",
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
        match head.and_then(|head| head.latest_terminal) {
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
        // Phase one is a group boundary: no connector may enter PreCommit while a peer can still
        // admit an epoch write. Each handle repeats this seal idempotently at command admission.
        self.seal_sink_epoch_until(epoch, deadline).await?;
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
        let unsupported_flags = request.flags & !laminar_core::checkpoint::flags::HANDOFF;
        if unsupported_flags != 0 {
            return Err(DbError::Checkpoint(format!(
                "checkpoint request carries unsupported flags {unsupported_flags:#x}"
            )));
        }
        if request.handoff_replay_pending
            && request.flags & laminar_core::checkpoint::flags::HANDOFF == 0
        {
            return Err(DbError::Checkpoint(
                "aligned replay may only qualify an assignment handoff checkpoint".into(),
            ));
        }
        if request.handoff_replay_pending && request.reassignment_portable {
            return Err(DbError::Checkpoint(
                "a checkpoint with aligned replay pending cannot claim vnode reassignment portability"
                    .into(),
            ));
        }
        #[cfg(feature = "cluster")]
        if request.flags & laminar_core::checkpoint::flags::HANDOFF != 0
            && self.cluster_controller.is_none()
        {
            return Err(DbError::Checkpoint(
                "assignment handoff checkpoint requires a cluster runtime".into(),
            ));
        }
        #[cfg(not(feature = "cluster"))]
        if request.flags != 0 {
            return Err(DbError::Checkpoint(
                "assignment handoff checkpoint requires cluster support".into(),
            ));
        }

        #[cfg(feature = "cluster")]
        let vnode_count = u32::from(self.store.key_group_count().get());
        #[cfg(feature = "cluster")]
        match (
            self.cluster_controller.as_ref(),
            request.assignment_fence.as_ref(),
        ) {
            (None, None) if !request.reassignment_portable => {}
            (None, None) => {
                return Err(DbError::Checkpoint(
                    "local checkpoint cannot claim vnode reassignment portability".into(),
                ));
            }
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
                if !request.reassignment_portable {
                    return Err(DbError::Checkpoint(
                        "cluster checkpoint requires a vnode-reassignment-portable capture".into(),
                    ));
                }
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
        #[cfg(not(feature = "cluster"))]
        if request.reassignment_portable {
            return Err(DbError::Checkpoint(
                "local checkpoint cannot claim vnode reassignment portability".into(),
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
            .binary_search_by_key(&chunk, |reference| reference.chunk)
            .ok()
            .map(|index| &prior.referenced_chunks[index])
            .map(|reference| (reference.object_length, reference.sha256.clone()))
    }

    fn managed_vnode_is_required(
        &self,
        operators: &[ManagedVnodeOperator],
        operator_id: &str,
        vnode: u16,
    ) -> bool {
        let Ok(index) =
            operators.binary_search_by(|operator| operator.operator_id.as_str().cmp(operator_id))
        else {
            return false;
        };
        let vnode = u32::from(vnode);
        if self.owned_vnodes.binary_search(&vnode).is_err() {
            return false;
        }
        match operators[index].placement {
            ManagedVnodePlacement::GlobalSingleton => vnode == 0,
            ManagedVnodePlacement::VnodeKeyed => true,
        }
    }

    fn complete_sparse_vnode_captures(
        &self,
        request: &mut CheckpointRequest,
    ) -> Result<(), DbError> {
        request
            .managed_vnode_operators
            .sort_unstable_by(|left, right| left.operator_id.cmp(&right.operator_id));
        if request
            .managed_vnode_operators
            .iter()
            .any(|operator| operator.operator_id.is_empty())
            || request
                .managed_vnode_operators
                .windows(2)
                .any(|pair| pair[0].operator_id == pair[1].operator_id)
        {
            return Err(DbError::Checkpoint(
                "managed vnode operator inventory must have non-empty, unique identifiers".into(),
            ));
        }

        for capture in &request.state_frames {
            if let StateFrameKey::Vnode { operator_id, vnode } = &capture.key {
                if !self.managed_vnode_is_required(
                    &request.managed_vnode_operators,
                    operator_id,
                    *vnode,
                ) {
                    return Err(DbError::Checkpoint(format!(
                        "captured vnode frame {:?} is outside the current managed-state inventory or ownership roster",
                        capture.key
                    )));
                }
            }
        }

        let expected_vnodes =
            request
                .managed_vnode_operators
                .iter()
                .try_fold(0usize, |total, operator| {
                    let count = match operator.placement {
                        ManagedVnodePlacement::GlobalSingleton => {
                            usize::from(self.owned_vnodes.first() == Some(&0))
                        }
                        ManagedVnodePlacement::VnodeKeyed => self.owned_vnodes.len(),
                    };
                    total.checked_add(count).ok_or_else(|| {
                        DbError::Checkpoint("managed vnode frame count overflowed usize".into())
                    })
                })?;
        let current_vnodes = request
            .state_frames
            .iter()
            .filter(|capture| matches!(capture.key, StateFrameKey::Vnode { .. }))
            .count();
        if current_vnodes < expected_vnodes {
            if let Some(prior) = self.last_committed_manifest.as_ref() {
                let current_whole_frames = request.state_frames.len() - current_vnodes;
                let merged_capacity = current_whole_frames
                    .checked_add(expected_vnodes)
                    .ok_or_else(|| {
                        DbError::Checkpoint(
                            "managed vnode checkpoint frame count overflowed usize".into(),
                        )
                    })?;
                let mut merged = Vec::new();
                merged.try_reserve_exact(merged_capacity).map_err(|error| {
                    DbError::Checkpoint(format!(
                        "managed vnode checkpoint roster reservation failed: {error}"
                    ))
                })?;

                let current = std::mem::take(&mut request.state_frames);
                let mut current = current.into_iter().peekable();
                let mut inherited = prior
                    .state_frames
                    .iter()
                    .filter(|frame| {
                        let StateFrameKey::Vnode { operator_id, vnode } = &frame.key else {
                            return false;
                        };
                        self.managed_vnode_is_required(
                            &request.managed_vnode_operators,
                            operator_id,
                            *vnode,
                        )
                    })
                    .peekable();

                loop {
                    match (current.peek(), inherited.peek()) {
                        (Some(current_frame), Some(inherited_frame)) => {
                            match current_frame.key.cmp(&inherited_frame.key) {
                                std::cmp::Ordering::Less => {
                                    merged.push(current.next().expect("peeked current frame"));
                                }
                                std::cmp::Ordering::Equal => {
                                    merged.push(current.next().expect("peeked current frame"));
                                    inherited.next();
                                }
                                std::cmp::Ordering::Greater => {
                                    let frame = inherited.next().expect("peeked inherited frame");
                                    merged.push(CapturedStateFrame {
                                        key: frame.key.clone(),
                                        state: None,
                                    });
                                }
                            }
                        }
                        (Some(_), None) => {
                            merged.extend(current);
                            break;
                        }
                        (None, Some(_)) => {
                            merged.extend(inherited.map(|frame| CapturedStateFrame {
                                key: frame.key.clone(),
                                state: None,
                            }));
                            break;
                        }
                        (None, None) => break,
                    }
                }
                request.state_frames = merged;
            }
        }
        self.validate_capture_roster(&request.state_frames)?;

        let actual_vnodes = request
            .state_frames
            .iter()
            .filter(|capture| matches!(capture.key, StateFrameKey::Vnode { .. }))
            .count();
        if actual_vnodes != expected_vnodes {
            return Err(DbError::Checkpoint(format!(
                "managed vnode checkpoint is incomplete: captured {actual_vnodes} logical frames, expected {expected_vnodes}"
            )));
        }
        Ok(())
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
        deadline: tokio::time::Instant,
    ) -> Result<PackedCheckpoint, DbError> {
        if tokio::time::Instant::now() >= deadline {
            return Err(DbError::Checkpoint(
                "checkpoint packing exceeded its end-to-end deadline".into(),
            ));
        }
        self.validate_request(&request)?;
        request
            .state_frames
            .sort_unstable_by(|left, right| left.key.cmp(&right.key));
        self.validate_capture_roster(&request.state_frames)?;
        self.complete_sparse_vnode_captures(&mut request)?;
        for channel in &mut request.channel_progress {
            channel.participant_id = self.store.participant_id();
        }
        request.channel_progress.sort_unstable_by(|left, right| {
            (
                left.participant_id,
                left.source_name.as_str(),
                left.input_channel.as_slice(),
            )
                .cmp(&(
                    right.participant_id,
                    right.source_name.as_str(),
                    right.input_channel.as_slice(),
                ))
        });
        if request.channel_progress.windows(2).any(|pair| {
            pair[0].participant_id == pair[1].participant_id
                && pair[0].source_name == pair[1].source_name
                && pair[0].input_channel == pair[1].input_channel
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

        for CapturedStateFrame { key, state } in request.state_frames {
            if let Some(bytes) = state {
                let length = u64::try_from(bytes.len()).map_err(|_| {
                    DbError::Checkpoint(format!("state frame {key:?} length exceeds u64"))
                })?;
                if length == 0 {
                    return Err(DbError::Checkpoint(format!(
                        "state frame {key:?} has an empty payload"
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
                    key,
                    chunk: current_chunk,
                    range,
                    sha256: String::new(),
                });
                current_frame_chunks.push((frame_index, node_data_index));
            } else {
                let prior = self.last_committed_manifest.as_ref().ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "unchanged state frame {key:?} has no committed predecessor"
                    ))
                })?;
                let frame_index = prior
                    .state_frames
                    .binary_search_by(|frame| frame.key.cmp(&key))
                    .ok()
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "unchanged state frame {key:?} is absent from its committed predecessor"
                        ))
                    })?;
                let prior_frame = &prior.state_frames[frame_index];
                let (length, digest) = Self::prior_chunk_metadata(prior, prior_frame.chunk)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "predecessor frame {:?} references untracked object {:?}",
                            prior_frame.key, prior_frame.chunk
                        ))
                    })?;
                let entry =
                    referenced
                        .entry(prior_frame.chunk)
                        .or_insert((length, digest.clone(), 0));
                if entry.0 != length || entry.1 != digest {
                    return Err(DbError::Checkpoint(format!(
                        "conflicting metadata for referenced object {:?}",
                        prior_frame.chunk
                    )));
                }
                entry.2 = entry
                    .2
                    .checked_add(1)
                    .ok_or_else(|| DbError::Checkpoint("referenced frame count overflow".into()))?;
                frames.push(StateFrame {
                    key,
                    chunk: prior_frame.chunk,
                    range: prior_frame.range,
                    sha256: prior_frame.sha256.clone(),
                });
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
        let digest_task = tokio::task::spawn_blocking(move || {
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
        });
        let (object_sha256, frame_digests, sink_digests) =
            tokio::time::timeout_at(deadline, digest_task)
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "checkpoint digest task exceeded its end-to-end deadline".into(),
                    )
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!("checkpoint digest task failed: {error}"))
                })?;
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
        manifest.reassignment_portable = request.reassignment_portable;
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
        manifest.source_names = request.source_names;
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
        if tokio::time::Instant::now() >= deadline {
            self.retain_ambiguous_prepared(packed)?;
            return Err(DbError::Checkpoint(
                "checkpoint persistence timed out".into(),
            ));
        }
        let persisted = tokio::time::timeout_at(
            deadline,
            self.store
                .save_checkpoint(&packed.manifest, &packed.node_data),
        )
        .await;
        let manifest_bytes = match persisted {
            Err(_) => {
                self.retain_ambiguous_prepared(packed)?;
                return Err(DbError::Checkpoint(
                    "checkpoint persistence timed out".into(),
                ));
            }
            Ok(Err(error)) => {
                self.retain_ambiguous_prepared(packed)?;
                return Err(DbError::from(error));
            }
            Ok(Ok(bytes)) => bytes,
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

    async fn seal_checkpoint_artifacts_until(
        &mut self,
        inventory: &CheckpointArtifactInventory,
        predecessor: Option<CommittedCheckpointRef>,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        self.validate_checkpoint_artifact_inventory(inventory)?;
        let participant_ids = inventory.assignment_fence.as_ref().map_or_else(
            || vec![laminar_core::state::LOCAL_NODE_ID.0],
            laminar_core::checkpoint::CheckpointAssignmentFence::participant_ids,
        );
        let mut manifest_seals = futures::stream::iter(participant_ids.iter().copied())
            .map(|participant_id| {
                let store = Arc::clone(&self.store);
                let chunk = StateChunkId {
                    participant_id,
                    checkpoint_id: inventory.attempt.checkpoint_id,
                };
                let artifact_identity =
                    checkpoint_artifact_identity_sha256(inventory, chunk).map_err(DbError::from);
                async move {
                    let artifact_identity = artifact_identity?;
                    let manifest = tokio::time::timeout_at(deadline, async {
                        store.seal_aborted_manifest(chunk, &artifact_identity).await
                    })
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(format!(
                            "participant {participant_id} artifact manifest seal timed out"
                        ))
                    })?
                    .map_err(DbError::from)?;
                    Ok::<_, DbError>((participant_id, manifest))
                }
            })
            .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY);
        let mut loaded = BTreeMap::new();
        while let Some(result) = manifest_seals.next().await {
            let (participant_id, manifest) = result?;
            if let Some((manifest, encoded)) = manifest {
                if manifest.deployment_id != inventory.deployment_id
                    || manifest.pipeline_identity != inventory.pipeline_identity
                    || manifest.epoch != inventory.attempt.epoch
                    || manifest.checkpoint_id != inventory.attempt.checkpoint_id
                    || manifest.participant_id != participant_id
                    || manifest.vnode_count != self.store.key_group_count().get()
                    || manifest.node_data.chunk
                        != (StateChunkId {
                            participant_id,
                            checkpoint_id: inventory.attempt.checkpoint_id,
                        })
                    || manifest.assignment_fence != inventory.assignment_fence
                {
                    return Err(DbError::Checkpoint(format!(
                        "participant {participant_id} manifest does not match the active artifact inventory"
                    )));
                }
                loaded.insert(participant_id, (manifest, encoded));
            }
        }

        if loaded.len() == participant_ids.len() {
            let manifests = loaded.into_values().collect::<Vec<_>>();
            let scope = if inventory.assignment_fence.is_some() {
                CheckpointScope::Cluster
            } else {
                CheckpointScope::Local
            };
            let predecessor_source_watermarks = self
                .predecessor_source_watermarks_until(predecessor.as_ref(), deadline)
                .await?;
            let candidate = self.build_committed_index(
                inventory.attempt,
                scope,
                inventory.assignment_fence.clone(),
                predecessor,
                &predecessor_source_watermarks,
                &manifests,
                None,
            )?;
            let decisions = self.decision_store.as_ref().ok_or_else(|| {
                DbError::Checkpoint("artifact sealing requires a decision store".into())
            })?;
            tokio::time::timeout_at(
                deadline,
                decisions.seal_aborted_committed_checkpoint_candidate(&candidate),
            )
            .await
            .map_err(|_| DbError::Checkpoint("candidate checkpoint index seal timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("candidate checkpoint index seal failed: {error}"))
            })?;
        }

        let mut data_seals = futures::stream::iter(participant_ids.iter().copied())
            .map(|participant_id| {
                let store = Arc::clone(&self.store);
                let chunk = StateChunkId {
                    participant_id,
                    checkpoint_id: inventory.attempt.checkpoint_id,
                };
                let artifact_identity =
                    checkpoint_artifact_identity_sha256(inventory, chunk).map_err(DbError::from);
                async move {
                    let artifact_identity = artifact_identity?;
                    tokio::time::timeout_at(deadline, async {
                        store
                            .seal_aborted_node_data(chunk, &artifact_identity)
                            .await
                    })
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(format!(
                            "participant {participant_id} node-data seal timed out"
                        ))
                    })?
                    .map_err(DbError::from)
                }
            })
            .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY);
        while let Some(result) = data_seals.next().await {
            result?;
        }
        self.prepared.remove(&inventory.attempt);
        Ok(())
    }

    async fn cleanup_local_checkpoint_artifacts_until(
        &mut self,
        attempt: CheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let store = Arc::clone(self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("local artifact cleanup requires a decision store".into())
        })?);
        let Some(head) = tokio::time::timeout_at(deadline, store.checkpoint_decision_head())
            .await
            .map_err(|_| DbError::Checkpoint("local artifact cleanup read timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("local artifact cleanup read failed: {error}"))
            })?
        else {
            return Ok(());
        };
        let Some(inventory) = head.active_artifacts else {
            return Ok(());
        };
        if inventory.attempt != attempt || inventory.assignment_fence.is_some() {
            return Err(DbError::Checkpoint(format!(
                "checkpoint {} does not match the active local artifact inventory",
                attempt.checkpoint_id
            )));
        }
        if !head.latest_terminal.as_ref().is_some_and(|outcome| {
            !outcome.is_commit()
                && outcome.epoch == attempt.epoch
                && outcome.checkpoint_id == attempt.checkpoint_id
        }) {
            return Err(DbError::Checkpoint(format!(
                "checkpoint {} artifacts cannot be cleaned without its durable Abort",
                attempt.checkpoint_id
            )));
        }
        let predecessor = head
            .latest_commit
            .and_then(|outcome| outcome.committed_checkpoint);
        self.seal_checkpoint_artifacts_until(&inventory, predecessor, deadline)
            .await?;
        let result = tokio::time::timeout_at(
            deadline,
            store.complete_checkpoint_artifact_cleanup(&inventory),
        )
        .await
        .map_err(|_| DbError::Checkpoint("local artifact inventory cleanup timed out".into()))?
        .map_err(|error| {
            DbError::Checkpoint(format!("local artifact inventory cleanup failed: {error}"))
        })?;
        match result {
            CheckpointArtifactInventoryUpdateResult::Applied
            | CheckpointArtifactInventoryUpdateResult::Unchanged => Ok(()),
            CheckpointArtifactInventoryUpdateResult::Conflict { current } => {
                Err(DbError::Checkpoint(format!(
                    "local artifact inventory cleanup conflicted with {current:?}"
                )))
            }
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) async fn settle_cluster_checkpoint_artifacts_until(
        &mut self,
        proof: &LeaderProof,
        deadline: tokio::time::Instant,
    ) -> Result<bool, DbError> {
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint("cluster artifact cleanup has no cluster controller".into())
        })?;
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
        })?;
        let Some(inventory) =
            tokio::time::timeout_at(deadline, authority.cluster_checkpoint_artifacts())
                .await
                .map_err(|_| {
                    DbError::Checkpoint("cluster artifact inventory read timed out".into())
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!("cluster artifact inventory read failed: {error}"))
                })?
        else {
            return Ok(false);
        };
        self.validate_checkpoint_artifact_inventory(&inventory)?;
        let assignment_fence = inventory.assignment_fence.clone().ok_or_else(|| {
            DbError::Checkpoint("cluster artifact inventory has no assignment fence".into())
        })?;
        let settlement = tokio::time::timeout_at(
            deadline,
            authority.cluster_attempt_settlement(inventory.attempt),
        )
        .await
        .map_err(|_| DbError::Checkpoint("cluster artifact settlement read timed out".into()))?
        .map_err(|error| {
            DbError::Checkpoint(format!("cluster artifact settlement read failed: {error}"))
        })?;
        match settlement {
            None => {
                self.record_outcome_until(
                    inventory.attempt,
                    laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
                    None,
                    Some(assignment_fence),
                    Some(proof.clone()),
                    deadline,
                )
                .await?;
            }
            Some(outcome)
                if outcome.epoch == inventory.attempt.epoch
                    && outcome.checkpoint_id == inventory.attempt.checkpoint_id
                    && !outcome.is_commit()
                    && outcome.deployment_id == inventory.deployment_id
                    && outcome.scope == CheckpointScope::Cluster
                    && outcome.assignment_fence.as_ref() == inventory.assignment_fence.as_ref() => {
            }
            Some(_) => {
                return Err(DbError::Checkpoint(format!(
                    "checkpoint {} has incompatible terminal authority while artifacts remain",
                    inventory.attempt.checkpoint_id
                )));
            }
        }
        let latest_commit =
            tokio::time::timeout_at(deadline, authority.highest_cluster_committed_outcome())
                .await
                .map_err(|_| {
                    DbError::Checkpoint("cluster committed predecessor read timed out".into())
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "cluster committed predecessor read failed: {error}"
                    ))
                })?;
        if latest_commit
            .as_ref()
            .is_some_and(|outcome| outcome.epoch >= inventory.attempt.epoch)
        {
            return Err(DbError::Checkpoint(format!(
                "checkpoint {} artifact cleanup does not follow the committed head",
                inventory.attempt.checkpoint_id
            )));
        }
        let predecessor = latest_commit.and_then(|outcome| outcome.committed_checkpoint);
        self.seal_checkpoint_artifacts_until(&inventory, predecessor, deadline)
            .await?;
        tokio::time::timeout_at(
            deadline,
            authority.finish_cluster_checkpoint_artifact_cleanup(proof, &inventory),
        )
        .await
        .map_err(|_| DbError::Checkpoint("cluster artifact inventory cleanup timed out".into()))?
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "cluster artifact inventory cleanup failed: {error}"
            ))
        })?;
        Ok(true)
    }

    async fn await_prepared_participant_manifests(
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
        let expected_assignment_fence = assignment_fence.cloned();
        let expected_deployment_id = self.expected_deployment_id()?.to_owned();
        let expected_pipeline_identity = self.expected_pipeline_identity()?;
        if local.0.participant_id != self.store.participant_id()
            || !participant_ids.contains(&local.0.participant_id)
            || local.0.checkpoint_id != attempt.checkpoint_id
            || local.0.epoch != attempt.epoch
            || local.0.assignment_fence != expected_assignment_fence
            || local.0.deployment_id != expected_deployment_id
            || local.0.pipeline_identity != expected_pipeline_identity
        {
            return Err(DbError::Checkpoint(format!(
                "local participant published an invalid manifest readiness marker for checkpoint {} epoch {}",
                attempt.checkpoint_id, attempt.epoch
            )));
        }
        let mut loaded = BTreeMap::from([(local.0.participant_id, local)]);
        let mut reads = participant_ids
            .into_iter()
            .filter(|participant_id| *participant_id != self.store.participant_id())
            .map(|participant_id| {
                let store = Arc::clone(&self.store);
                let expected_assignment_fence = expected_assignment_fence.clone();
                let expected_deployment_id = expected_deployment_id.clone();
                let expected_pipeline_identity = expected_pipeline_identity.clone();
                async move {
                    let manifest = await_participant_manifest_until(
                        participant_id,
                        attempt,
                        deadline,
                        || {
                            let store = Arc::clone(&store);
                            async move {
                                store
                                    .load_manifest_for_participant(
                                        participant_id,
                                        attempt.checkpoint_id,
                                    )
                                    .await
                                    .map_err(DbError::from)
                            }
                        },
                    )
                    .await?;
                    if manifest.epoch != attempt.epoch
                        || manifest.assignment_fence != expected_assignment_fence
                        || manifest.deployment_id != expected_deployment_id
                        || manifest.pipeline_identity != expected_pipeline_identity
                    {
                        return Err(DbError::Checkpoint(format!(
                            "participant {participant_id} published an invalid manifest readiness marker for checkpoint {} epoch {}",
                            attempt.checkpoint_id, attempt.epoch
                        )));
                    }
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
        match (&mut destination.input_channels, &incoming.input_channels) {
            (None, None) => {}
            (Some(destination), Some(incoming)) => {
                if incoming
                    .iter()
                    .any(|channel| destination.binary_search(channel).is_ok())
                {
                    return Err(DbError::Checkpoint(format!(
                        "source '{source}' input channel is owned by multiple participants"
                    )));
                }
                destination.extend(incoming.iter().cloned());
                destination.sort_unstable();
            }
            _ => {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' participant checkpoints disagree on whether input channels are declared"
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
        predecessor: Option<CommittedCheckpointRef>,
        predecessor_source_watermarks: &BTreeMap<String, i64>,
        manifests: &[(CheckpointManifest, Bytes)],
        quorum_watermark: Option<CheckpointWatermark>,
    ) -> Result<CommittedCheckpointIndex, DbError> {
        let mut participants = Vec::with_capacity(manifests.len());
        let mut source_offsets = BTreeMap::<String, ConnectorCheckpoint>::new();
        let mut channels = BTreeMap::<(u64, String, Vec<u8>), ChannelProgress>::new();
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
                        (
                            channel.participant_id,
                            channel.source_name.clone(),
                            channel.input_channel.clone(),
                        ),
                        channel.clone(),
                    )
                    .is_some()
                {
                    return Err(DbError::Checkpoint(format!(
                        "participant {} source '{}' input channel appears more than once",
                        channel.participant_id, channel.source_name
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
        let source_names = manifests
            .first()
            .map_or_else(Vec::new, |(manifest, _)| manifest.source_names.clone());
        let mut source_watermarks = predecessor_source_watermarks
            .iter()
            .filter(|(source, _)| source_names.binary_search(source).is_ok())
            .map(|(source, watermark)| (source.clone(), *watermark))
            .collect::<BTreeMap<_, _>>();
        for (source, frontier) in
            channel_progress_frontiers_by_source(&channel_progress).map_err(DbError::Checkpoint)?
        {
            let Some(frontier) = frontier else {
                continue;
            };
            if source_watermarks
                .get(source)
                .is_some_and(|predecessor| *predecessor > frontier)
            {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' decision watermark regressed below its committed predecessor"
                )));
            }
            source_watermarks.insert(source.to_owned(), frontier);
        }
        let reassignment_portable = manifests
            .first()
            .is_some_and(|(manifest, _)| manifest.reassignment_portable);
        let index = CommittedCheckpointIndex {
            version: COMMITTED_CHECKPOINT_INDEX_VERSION,
            deployment_id: self.expected_deployment_id()?.to_owned(),
            pipeline_identity: self.expected_pipeline_identity()?,
            epoch: attempt.epoch,
            checkpoint_id: attempt.checkpoint_id,
            scope,
            vnode_count: self.store.key_group_count().get(),
            assignment_fence,
            reassignment_portable,
            predecessor,
            participants,
            source_names,
            source_offsets,
            channel_progress,
            source_watermarks,
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
        if tokio::time::Instant::now() >= deadline {
            return Err(DbError::Checkpoint(
                "committed checkpoint index create timed out".into(),
            ));
        }
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
        #[cfg(feature = "cluster")]
        let cluster_scope = self.cluster_controller.is_some();
        #[cfg(not(feature = "cluster"))]
        let cluster_scope = false;
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
        let artifact_cleanup = if cluster_scope {
            self.failure_requires_recovery = true;
            Ok(())
        } else {
            self.cleanup_local_checkpoint_artifacts_until(attempt, cleanup_deadline)
                .await
        };
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
        self.record_checkpoint_outcome(false, attempt, duration, None);
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
        flags: u64,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        _attempt_deadline: tokio::time::Instant,
        sink_epoch_publication: SinkEpochPublication,
    ) -> CheckpointResult {
        #[cfg(not(feature = "cluster"))]
        let _ = flags;
        // Once an exact attempt has been reserved, failure settlement owns a private cleanup
        // budget. The attempt deadline fences new capture/durable work, but must not cancel the
        // durable Abort and rollback that make the attempt terminal.
        let cleanup_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        let mut message = error.to_string();
        if let Err(seal) = self
            .seal_sink_epoch_until(attempt.epoch, cleanup_deadline)
            .await
        {
            self.failure_requires_recovery = true;
            message = format!("{message}; {seal}");
        }
        match self
            .abort_attempt_until(
                attempt,
                assignment_fence.clone(),
                leader_proof.clone(),
                cleanup_deadline,
            )
            .await
        {
            Ok(()) => {
                // Durable Abort is already terminal. Bound its best-effort cluster hint under a
                // fresh, explicit cleanup window rather than reviving the expired attempt deadline
                // or allowing notification I/O to hang.
                #[cfg(feature = "cluster")]
                if let Some(controller) = self.cluster_controller.as_ref() {
                    use laminar_core::cluster::control::{BarrierAnnouncement, Phase};
                    let notification_deadline =
                        tokio::time::Instant::now() + self.config.cleanup_timeout;
                    publish_terminal_hint_until(
                        notification_deadline,
                        controller.announce_barrier(&BarrierAnnouncement {
                            epoch: attempt.epoch,
                            checkpoint_id: attempt.checkpoint_id,
                            assignment_fence,
                            leader_proof,
                            phase: Phase::Abort,
                            flags,
                        }),
                    )
                    .await;
                }
                // A slow best-effort hint must not consume the local successor epoch's required
                // continuation budget.
                let continuation_deadline =
                    tokio::time::Instant::now() + self.config.cleanup_timeout;
                let requires_recovery = self.failure_requires_recovery;
                let successor = if !requires_recovery && self.has_checkpoint_committable_sinks() {
                    self.begin_sink_epoch_until(continuation_deadline, sink_epoch_publication)
                        .await
                        .err()
                } else {
                    None
                };
                let (error, disposition) = match (requires_recovery, successor) {
                    (true, _) => (message, CheckpointFailureDisposition::RequiresRecovery),
                    (false, Some(successor)) => (
                        format!("{message}; successor sink epoch failed: {successor}"),
                        CheckpointFailureDisposition::RequiresRecovery,
                    ),
                    (false, None) => (message, CheckpointFailureDisposition::Retryable),
                };
                self.failed_result(attempt, started, error, disposition)
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
        deadline: tokio::time::Instant,
        sink_epoch_publication: SinkEpochPublication,
    ) -> Result<CheckpointResult, DbError> {
        require_canonical_attempt(attempt, "checkpoint admission")?;
        let flags = request.flags;
        let assignment_fence = request.assignment_fence.clone();
        #[cfg(feature = "cluster")]
        let validation_proof = match &quorum {
            QuorumStage::Captured { leader_proof, .. } => Some(leader_proof.clone()),
            QuorumStage::RunInline => self
                .cluster_controller
                .as_ref()
                .and_then(|controller| controller.capture_leader_proof()),
        };
        #[cfg(not(feature = "cluster"))]
        let validation_proof = None;
        if self.failure_requires_recovery {
            return Ok(self
                .fail_before_commit(
                    attempt,
                    started,
                    DbError::Checkpoint(
                        "a prior checkpoint has unresolved durable or sink state".into(),
                    ),
                    flags,
                    assignment_fence,
                    validation_proof,
                    deadline,
                    sink_epoch_publication,
                )
                .await);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(self
                .fail_before_commit(
                    attempt,
                    started,
                    DbError::Checkpoint("checkpoint deadline expired before durable work".into()),
                    flags,
                    assignment_fence,
                    validation_proof,
                    deadline,
                    sink_epoch_publication,
                )
                .await);
        }
        let request_validation = self.validate_request(&request);
        if let Err(error) = request_validation {
            return Ok(self
                .fail_before_commit(
                    attempt,
                    started,
                    error,
                    flags,
                    assignment_fence,
                    validation_proof,
                    deadline,
                    sink_epoch_publication,
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
                        return Ok(self
                            .fail_before_commit(
                                attempt,
                                started,
                                DbError::Checkpoint(
                                    "cluster checkpoint reached durable execution without a \
                                 precomputed certified quorum"
                                        .into(),
                                ),
                                flags,
                                assignment_fence,
                                validation_proof,
                                deadline,
                                sink_epoch_publication,
                            )
                            .await);
                    }
                    QuorumStage::Captured {
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
                            flags,
                            assignment_fence,
                            Some(proof),
                            deadline,
                            sink_epoch_publication,
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
                        flags,
                        assignment_fence,
                        leader_proof,
                        deadline,
                        sink_epoch_publication,
                    )
                    .await);
            }
        };
        let packed = match self
            .pack_checkpoint(attempt, request, descriptors, deadline)
            .await
        {
            Ok(packed) => packed,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        flags,
                        assignment_fence,
                        leader_proof,
                        deadline,
                        sink_epoch_publication,
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
                        flags,
                        assignment_fence,
                        leader_proof,
                        deadline,
                        sink_epoch_publication,
                    )
                    .await);
            }
        };

        let manifests = match self
            .await_prepared_participant_manifests(
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
                        flags,
                        assignment_fence,
                        leader_proof,
                        deadline,
                        sink_epoch_publication,
                    )
                    .await);
            }
        };
        let predecessor = match self
            .authoritative_committed_predecessor_until(scope, deadline)
            .await
        {
            Ok(predecessor) => predecessor,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        flags,
                        assignment_fence,
                        leader_proof,
                        deadline,
                        sink_epoch_publication,
                    )
                    .await);
            }
        };
        let predecessor_source_watermarks = match self
            .predecessor_source_watermarks_until(predecessor.as_ref(), deadline)
            .await
        {
            Ok(source_watermarks) => source_watermarks,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        flags,
                        assignment_fence,
                        leader_proof,
                        deadline,
                        sink_epoch_publication,
                    )
                    .await);
            }
        };
        let index = match self.build_committed_index(
            attempt,
            scope,
            assignment_fence.clone(),
            predecessor.clone(),
            &predecessor_source_watermarks,
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
                        flags,
                        assignment_fence,
                        leader_proof,
                        deadline,
                        sink_epoch_publication,
                    )
                    .await);
            }
        };
        #[cfg(all(debug_assertions, feature = "cluster"))]
        checkpoint_kill_gate(
            "leader",
            attempt,
            predecessor
                .as_ref()
                .map(|reference| (reference.checkpoint_id, reference.epoch)),
        )
        .await;
        let reference = match self.create_committed_index_until(&index, deadline).await {
            Ok(reference) => reference,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        flags,
                        assignment_fence,
                        leader_proof,
                        deadline,
                        sink_epoch_publication,
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

        let predecessor_checkpoint_id = index
            .predecessor
            .as_ref()
            .map_or(0, |reference| reference.checkpoint_id);
        self.last_committed_ref = Some(reference.clone());
        self.last_committed_source_watermarks = Some((reference, index.source_watermarks.clone()));
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
        if let Some(controller) = self.cluster_controller.as_ref() {
            controller
                .publish_committed_checkpoint_progress(
                    &index.channel_progress,
                    &index.source_watermarks,
                )
                .map_err(DbError::Checkpoint)?;
        }
        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.as_ref() {
            use laminar_core::cluster::control::{BarrierAnnouncement, Phase};
            // The durable Commit is already immutable. Its cluster hint is best-effort and must
            // not delay sink continuation or the terminal caller reply without bound.
            let notification_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
            publish_terminal_hint_until(
                notification_deadline,
                controller.announce_barrier(&BarrierAnnouncement {
                    epoch: attempt.epoch,
                    checkpoint_id: attempt.checkpoint_id,
                    assignment_fence: assignment_fence.clone(),
                    leader_proof: leader_proof.clone(),
                    phase: Phase::Commit,
                    flags,
                }),
            )
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
                self.schedule_retention(index.clone(), leader_proof.as_ref());
                if let Err(error) = self.clear_sink_witness_until(continuation_deadline).await {
                    Err(error)
                } else if self.has_checkpoint_committable_sinks() {
                    self.begin_sink_epoch_until(continuation_deadline, sink_epoch_publication)
                        .await
                } else {
                    Ok(())
                }
            }
            Err(error) => Err(error),
        };

        let duration = started.elapsed();
        self.phase = CheckpointPhase::Idle;
        let checkpoint_bytes = self
            .last_committed_manifest
            .as_ref()
            .map(|manifest| manifest.node_data.object_length);
        self.record_checkpoint_outcome(true, attempt, duration, checkpoint_bytes);
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
        #[cfg(feature = "cluster")]
        let local = self.cluster_controller.is_none();
        #[cfg(not(feature = "cluster"))]
        let local = true;
        if !local {
            return Err(DbError::Checkpoint(
                "cluster checkpoints require reserved pipeline admission and certified Prepare"
                    .into(),
            ));
        }
        let attempt = self.allocate_attempt_until(deadline).await?;
        if let Err(error) = self
            .begin_checkpoint_artifacts_until(attempt, None, None, deadline)
            .await
        {
            return Ok(self
                .fail_before_commit(
                    attempt,
                    started,
                    error,
                    request.flags,
                    request.assignment_fence.clone(),
                    None,
                    deadline,
                    SinkEpochPublication::Immediate,
                )
                .await);
        }
        self.run_checkpoint_attempt(
            request,
            attempt,
            QuorumStage::RunInline,
            started,
            deadline,
            SinkEpochPublication::Immediate,
        )
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
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointResult, DbError> {
        self.run_checkpoint_attempt(
            request,
            attempt,
            quorum,
            started,
            deadline,
            SinkEpochPublication::DeferredToTail,
        )
        .await
    }

    pub(crate) async fn abandon_epoch_until(
        &mut self,
        checkpoint_id: u64,
        epoch: u64,
        error: String,
        flags: u64,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        deadline: tokio::time::Instant,
        sink_epoch_publication: SinkEpochPublication,
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
                flags,
                assignment_fence,
                leader_proof,
                deadline,
                sink_epoch_publication,
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
            bool,
        ),
        String,
    > {
        use laminar_core::cluster::control::{BarrierAnnouncement, Phase, QuorumOutcome};

        let PrepareQuorum {
            attempt,
            local_watermark,
            assignment_fence,
            leader_proof,
            flags,
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
            flags,
        };
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
                handoff_replay_pending,
            } => {
                controller.note_responsive(acks);
                let watermark = if followers.is_empty() {
                    local_watermark
                } else {
                    local_watermark.cluster_min(follower_watermark)
                };
                Ok((
                    Self::validate_cluster_watermark_candidate(controller, watermark)?,
                    followers,
                    handoff_replay_pending,
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
    async fn certify_follower_assignment_until(
        controller: &laminar_core::cluster::control::ClusterController,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        proof: &LeaderProof,
        deadline: tokio::time::Instant,
        context: &'static str,
    ) -> Result<(), DbError> {
        let certified = tokio::time::timeout_at(
            deadline,
            controller.checkpoint_assignment_fence_for_leader(fence.assignment_version, proof),
        )
        .await
        .map_err(|_| DbError::Checkpoint(format!("{context} authority validation timed out")))?;
        if certified.as_ref() != Some(fence) {
            return Err(DbError::Checkpoint(format!(
                "{context} authority is no longer current"
            )));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    async fn validate_follower_prepare_context(
        controller: &laminar_core::cluster::control::ClusterController,
        request: &CheckpointRequest,
        announcement: &laminar_core::cluster::control::BarrierAnnouncement,
        deadline: tokio::time::Instant,
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
            || announcement.flags != request.flags
            || !fence.contains(controller.instance_id().0)
            || fence.participant_incarnation(proof.owner.node_id) != Some(proof.owner.boot_id)
        {
            return Err(DbError::Checkpoint(
                "follower Prepare does not match the certified assignment".into(),
            ));
        }
        Self::certify_follower_assignment_until(
            controller,
            fence,
            proof,
            deadline,
            "follower Prepare",
        )
        .await?;
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
    ) -> Result<FollowerPrepareOutcome, DbError> {
        use laminar_core::cluster::control::{BarrierAck, BarrierAckDisposition};

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
        Self::certify_follower_assignment_until(
            &controller,
            &fence,
            &leader_proof,
            deadline,
            "follower Prepare",
        )
        .await?;
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!("follower checkpoint authority: {error}"))
        })?;
        let expected_inventory =
            self.checkpoint_artifact_inventory(attempt, Some(fence.clone()))?;
        let active_inventory =
            tokio::time::timeout_at(deadline, authority.cluster_checkpoint_artifacts())
                .await
                .map_err(|_| {
                    DbError::Checkpoint("follower artifact admission read timed out".into())
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!("follower artifact admission read failed: {error}"))
                })?;
        if active_inventory.as_ref() != Some(&expected_inventory) {
            return Err(DbError::Checkpoint(
                "follower checkpoint has no exact durable artifact admission".into(),
            ));
        }
        let flags = request.flags;
        self.allocator.advance_epoch_to(epoch);
        self.phase = CheckpointPhase::PreCommitting;
        let descriptors = self.pre_commit_sinks_until(epoch, deadline).await;
        let (prepared, persistence_in_doubt) = match descriptors {
            Ok(descriptors) => match self
                .pack_checkpoint(attempt, request, descriptors, deadline)
                .await
            {
                Ok(packed) => (
                    self.persist_checkpoint_until(&packed, deadline)
                        .await
                        .map(|_| ()),
                    true,
                ),
                Err(error) => (Err(error), false),
            },
            Err(error) => (Err(error), false),
        };
        if let Err(error) = prepared {
            if persistence_in_doubt {
                // A timed-out/failed Create may already be visible. After Captured quorum the
                // leader is permitted to prove Commit from that exact manifest, so rolling back
                // phase-one sink state or superseding the cached Captured acknowledgement here
                // could contradict the authoritative outcome. Keep the retained prepared image
                // and let the normal decision path commit or abort it.
                tracing::warn!(
                    checkpoint_id,
                    epoch,
                    %error,
                    "follower manifest persistence is in doubt; awaiting authoritative decision"
                );
                self.phase = CheckpointPhase::Idle;
                return Ok(FollowerPrepareOutcome::InDoubt);
            }
            let acknowledgement = BarrierAck {
                epoch,
                checkpoint_id,
                assignment_digest: Some(fence.digest()),
                flags,
                disposition: BarrierAckDisposition::Failed,
                error: Some(error.to_string()),
                watermark: self.local_watermark,
            };
            // Once local phase one has failed before persistence starts, rollback is both safe
            // and required.  A slow best-effort Failed acknowledgement must not consume the
            // attempt's remaining budget and strand the coordinator in PreCommitting, so give
            // rollback its private cleanup window and run the notification alongside it.
            let cleanup_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
            let notify = async {
                let _ = tokio::time::timeout_at(
                    cleanup_deadline,
                    controller.ack_barrier(&acknowledgement),
                )
                .await;
            };
            let rollback = self.rollback_sinks_until(epoch, cleanup_deadline);
            let ((), rollback) = tokio::join!(notify, rollback);
            self.phase = CheckpointPhase::Idle;
            if let Err(rollback) = rollback {
                self.failure_requires_recovery = true;
                return Err(DbError::Checkpoint(format!(
                    "follower Prepare failed ({error}); rollback also failed ({rollback})"
                )));
            }
            // The durable active inventory owns every ambiguous Create until coordinated Abort
            // replaces the exact paths with permanent seals. Deleting here would reopen a path
            // for a late writer and discard manifest evidence needed to locate a candidate index.
            return Err(error);
        }
        #[cfg(all(debug_assertions, feature = "cluster"))]
        checkpoint_kill_gate(
            "follower",
            attempt,
            self.last_committed_ref
                .as_ref()
                .map(|reference| (reference.checkpoint_id, reference.epoch)),
        )
        .await;
        self.phase = CheckpointPhase::Idle;
        Ok(FollowerPrepareOutcome::Prepared)
    }

    /// Legacy direct follower entry point.
    ///
    /// This API owns no callback-supervised immutable capture tail, so it deliberately publishes
    /// `Captured` only after local phase-one packing/persistence has returned. The streaming
    /// runtime uses its early-capture path instead and acknowledges immediately after transferring
    /// the sealed capture into the supervised follower tail.
    #[cfg(feature = "cluster")]
    pub async fn follower_checkpoint(
        &mut self,
        request: CheckpointRequest,
        announcement: laminar_core::cluster::control::BarrierAnnouncement,
        decision_timeout: Duration,
    ) -> Result<bool, DbError> {
        use laminar_core::cluster::control::{BarrierAck, BarrierAckDisposition};

        let started = Instant::now();
        let deadline = tokio::time::Instant::from_std(started) + self.config.checkpoint_timeout;
        let controller = self.cluster_controller.clone().ok_or_else(|| {
            DbError::Checkpoint("follower checkpoint has no cluster controller".into())
        })?;
        let (fence, proof) =
            Self::validate_follower_prepare_context(&controller, &request, &announcement, deadline)
                .await?;
        let handoff_replay_pending = request.handoff_replay_pending;
        let prepare_outcome = self
            .follower_prepare_acked_until(
                request,
                proof,
                announcement.epoch,
                announcement.checkpoint_id,
                deadline,
            )
            .await?;
        let captured_ack_error = match tokio::time::timeout_at(
            deadline,
            controller.ack_barrier(&BarrierAck {
                epoch: announcement.epoch,
                checkpoint_id: announcement.checkpoint_id,
                assignment_digest: Some(fence.digest()),
                flags: announcement.flags,
                disposition: if handoff_replay_pending {
                    BarrierAckDisposition::CapturedWithReplay
                } else {
                    BarrierAckDisposition::Captured
                },
                error: None,
                watermark: self.local_watermark,
            }),
        )
        .await
        {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(format!("follower captured ack failed: {error}")),
            Err(_) => Some("follower captured ack timed out".to_string()),
        };
        if let Some(error) = captured_ack_error.as_deref() {
            // The local prepared image and any phase-one sink state cannot be discarded merely
            // because the best-effort Captured notification was ambiguous.  The leader may have
            // observed it and committed, so continue through exact terminal settlement.
            tracing::warn!(
                checkpoint_id = announcement.checkpoint_id,
                epoch = announcement.epoch,
                %error,
                "follower Captured acknowledgement was not confirmed; awaiting authority"
            );
        }
        let required_settlement_deadline = deadline
            .checked_add(self.config.cleanup_timeout)
            .ok_or_else(|| DbError::Checkpoint("follower settlement deadline overflowed".into()))?;
        let decision_timeout = decision_timeout.max(
            required_settlement_deadline.saturating_duration_since(tokio::time::Instant::now()),
        );
        if prepare_outcome == FollowerPrepareOutcome::InDoubt {
            tracing::debug!(
                checkpoint_id = announcement.checkpoint_id,
                epoch = announcement.epoch,
                "preserving in-doubt follower preparation through terminal observation"
            );
        }
        let committed = match Self::await_follower_decision(
            &controller,
            announcement.epoch,
            announcement.checkpoint_id,
            &fence,
            decision_timeout,
        )
        .await
        {
            Ok(committed) => committed,
            Err(settlement) => {
                self.failure_requires_recovery = true;
                let message = captured_ack_error.map_or_else(
                    || settlement.to_string(),
                    |ack| format!("{ack}; terminal settlement failed: {settlement}"),
                );
                return Err(DbError::Checkpoint(message));
            }
        };
        let result = self
            .follower_finish(
                announcement.epoch,
                announcement.checkpoint_id,
                committed,
                started,
            )
            .await;
        if result.is_err() {
            self.failure_requires_recovery = true;
        }
        result
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
                        let source_watermarks = index
                            .effective_source_watermarks()
                            .map_err(DbError::Checkpoint)?;
                        controller
                            .publish_committed_checkpoint_progress(
                                &index.channel_progress,
                                &source_watermarks,
                            )
                            .map_err(DbError::Checkpoint)?;
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
        started: Instant,
    ) -> Result<bool, DbError> {
        self.follower_finish_with_publication(
            epoch,
            checkpoint_id,
            committed,
            started,
            SinkEpochPublication::Immediate,
        )
        .await
    }

    #[cfg(feature = "cluster")]
    pub(crate) async fn follower_finish_deferred(
        &mut self,
        epoch: u64,
        checkpoint_id: u64,
        committed: bool,
        started: Instant,
    ) -> Result<bool, DbError> {
        self.follower_finish_with_publication(
            epoch,
            checkpoint_id,
            committed,
            started,
            SinkEpochPublication::DeferredToTail,
        )
        .await
    }

    #[cfg(feature = "cluster")]
    async fn follower_finish_with_publication(
        &mut self,
        epoch: u64,
        checkpoint_id: u64,
        committed: bool,
        started: Instant,
        sink_epoch_publication: SinkEpochPublication,
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
            let source_watermarks = index
                .effective_source_watermarks()
                .map_err(DbError::Checkpoint)?;
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
            self.last_committed_ref = Some(reference.clone());
            self.last_committed_source_watermarks = Some((reference, source_watermarks));
            self.last_committed_manifest = Some(manifest);
            self.prepared.remove(&attempt);
        } else {
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
                    if settlement.verdict
                        == laminar_core::checkpoint_decision::CheckpointVerdict::Abort => {}
                CheckpointAttemptRelation::Newer => {}
                _ => {
                    return Err(DbError::Checkpoint(
                        "follower cannot discard a checkpoint without an authoritative Abort or superseding terminal outcome"
                            .into(),
                    ));
                }
            }
            let rollback = self.rollback_sinks_until(epoch, deadline).await;
            rollback?;
            self.failure_requires_recovery = true;
        }
        self.allocator.advance_epoch_to(checked_successor_epoch(
            epoch,
            "closing a follower checkpoint",
        )?);
        let continuation =
            if !self.failure_requires_recovery && self.has_checkpoint_committable_sinks() {
                self.begin_sink_epoch_until(deadline, sink_epoch_publication)
                    .await
            } else {
                Ok(())
            };
        let duration = started.elapsed();
        self.phase = CheckpointPhase::Idle;
        let checkpoint_bytes = if committed {
            self.last_committed_manifest
                .as_ref()
                .map(|manifest| manifest.node_data.object_length)
        } else {
            None
        };
        self.record_checkpoint_outcome(committed, attempt, duration, checkpoint_bytes);
        if continuation.is_err() {
            self.failure_requires_recovery = true;
        }
        continuation?;
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
    #[cfg(any(feature = "cluster", test))]
    pub(crate) fn last_committed_ref(&self) -> Option<&CommittedCheckpointRef> {
        self.last_committed_ref.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn set_last_committed_ref_for_test(&mut self, reference: CommittedCheckpointRef) {
        self.last_committed_source_watermarks = None;
        self.last_committed_ref = Some(reference);
    }

    pub(crate) fn last_committed_manifest(&self) -> Option<&CheckpointManifest> {
        self.last_committed_manifest.as_deref()
    }

    pub(crate) fn committed_manifest_needs_vnode_rebase(&self, attempt: CheckpointAttempt) -> bool {
        self.last_committed_manifest
            .as_ref()
            .is_some_and(|manifest| {
                manifest.checkpoint_id == attempt.checkpoint_id
                    && manifest.epoch == attempt.epoch
                    && manifest.referenced_chunks.len() >= REFERENCED_CHUNK_REBASE_THRESHOLD
            })
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
        input_channels: cp.input_channels().map(<[Vec<u8>]>::to_vec),
        source_assignment_version: cp.assignment_version(),
    }
}

#[must_use]
pub(crate) fn connector_to_source_checkpoint(cp: &ConnectorCheckpoint) -> SourceCheckpoint {
    let mut source = SourceCheckpoint::with_offsets(cp.offsets.clone());
    for (key, value) in &cp.metadata {
        source.set_metadata(key.clone(), value.clone());
    }
    if let Some(channels) = &cp.input_channels {
        source
            .set_input_channels(channels.clone())
            .expect("validated connector checkpoint input-channel inventory");
    }
    if let Some(version) = cp.source_assignment_version {
        source.bind_assignment_version(version);
    }
    source
}
