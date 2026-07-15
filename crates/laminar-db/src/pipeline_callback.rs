//! Production `PipelineCallback` bridging coordinator to sinks, checkpoints, and watermarks.
#![allow(clippy::disallowed_types)] // cold path

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::connector::SinkContract;
use laminar_core::checkpoint::CheckpointWatermark;
use laminar_core::state::CheckpointAttempt;
#[cfg(feature = "cluster")]
use laminar_core::state::CheckpointAttemptRelation;
use laminar_core::storage::checkpoint_manifest::ConnectorCheckpoint;
use rustc_hash::FxHashMap;

use crate::db::{filter_late_rows, SourceWatermarkState};
use crate::error::DbError;
use crate::pipeline::CheckpointCompletion;

/// Resolution state of a sink WHERE filter.
#[derive(Clone)]
pub(crate) enum SinkFilter {
    Pending,
    Compiled(Arc<dyn PhysicalExpr>),
    Rejected,
}

/// Per-cycle dispatch chosen from `SinkFilter`. `Rejected` is fail-closed.
enum SinkFilterDispatch {
    Compiled(Arc<dyn PhysicalExpr>),
    Rejected,
    None,
}

/// Bridges the coordinator to the rest of the database (sinks, watermarks, checkpoints).
// Throttled WARN so silent watermark drops are diagnosable.
fn warn_late_drops(source: &str, column: &str, watermark_ms: i64, dropped: usize) {
    static THROTTLE: crate::log_throttle::LogThrottle =
        crate::log_throttle::LogThrottle::every(Duration::from_secs(10));
    if !THROTTLE.allow() {
        return;
    }
    tracing::warn!(
        source,
        time_column = column,
        watermark_ms,
        dropped,
        "dropping rows older than the event-time watermark; a future-dated \
         timestamp can advance the watermark and starve the stream"
    );
}

/// Terminal failure reporting is cleanup, not part of the durable attempt. A failure discovered
/// at the attempt deadline must still release its manual caller and exact-attempt bookkeeping.
const CHECKPOINT_FAILURE_REPORT_TIMEOUT: Duration = Duration::from_secs(1);

/// RAII guard that releases an epoch's admission slot and staged-byte budget on drop.
struct EpochInFlightGuard {
    in_flight: Arc<std::sync::atomic::AtomicU64>,
    staged_bytes: Arc<std::sync::atomic::AtomicU64>,
    bytes: u64,
}

impl EpochInFlightGuard {
    /// Claim one admission slot and `bytes` of staged budget.
    fn claim(
        in_flight: &Arc<std::sync::atomic::AtomicU64>,
        staged_bytes: &Arc<std::sync::atomic::AtomicU64>,
        bytes: u64,
    ) -> Self {
        in_flight.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        staged_bytes.fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
        Self {
            in_flight: Arc::clone(in_flight),
            staged_bytes: Arc::clone(staged_bytes),
            bytes,
        }
    }
}

impl Drop for EpochInFlightGuard {
    fn drop(&mut self) {
        self.in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        self.staged_bytes
            .fetch_sub(self.bytes, std::sync::atomic::Ordering::AcqRel);
    }
}

/// State for the leader's spawned durable tail.
struct LeaderTail {
    _in_flight: EpochInFlightGuard,
    coordinator:
        Arc<tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>>,
    complete_tx: crossfire::MAsyncTx<crossfire::mpsc::Array<CheckpointCompletion>>,
    request: crate::checkpoint_coordinator::CheckpointRequest,
    operator_state: Option<CapturedOperatorState>,
    operator_state_encoded_budget: u64,
    mutable_operator_capture_guard: Option<MutableCheckpointCaptureGuard>,
    #[allow(clippy::disallowed_types)]
    vnode_states: crate::checkpoint_coordinator::StagedVnodeStates,
    fan_out: FxHashMap<String, SourceCheckpoint>,
    local_watermark: CheckpointWatermark,
    attempt: CheckpointAttempt,
    attempt_started: std::time::Instant,
    checkpoint_timeout: Duration,
    serialization_timeout: Duration,
    checkpoint_cleanup_timeout: Duration,
    fault_on_failure: bool,
    checkpoint_fault: Arc<parking_lot::Mutex<Option<String>>>,
    #[cfg(feature = "cluster")]
    controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    /// Exact durable authority captured before this attempt's `Prepare` publication.
    #[cfg(feature = "cluster")]
    leader_proof: Option<laminar_core::cluster::control::LeaderProof>,
    #[cfg(feature = "cluster")]
    quorum_timeout: Duration,
    #[cfg(feature = "cluster")]
    delta_rebase_needed: Arc<std::sync::atomic::AtomicBool>,
}

fn checkpoint_failure_requires_pipeline_fault(
    result: &crate::checkpoint_coordinator::CheckpointResult,
    fault_on_retryable_failure: bool,
) -> bool {
    result.requires_recovery() || fault_on_retryable_failure
}

#[cfg(feature = "cluster")]
fn vnode_capture_requires_full_rebase(previous_epoch: Option<u64>, epoch: u64) -> bool {
    match previous_epoch {
        Some(previous) => previous.checked_add(1) != Some(epoch),
        None => true,
    }
}

/// Captured follower state and the runtime handles that own its decision-led durable tail.
#[cfg(feature = "cluster")]
struct FollowerDurableTail {
    _in_flight: EpochInFlightGuard,
    coordinator:
        Arc<tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>>,
    state: Arc<FollowerTailState>,
    complete_tx: crossfire::MAsyncTx<crossfire::mpsc::Array<CheckpointCompletion>>,
    controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    delta_rebase_needed: Arc<std::sync::atomic::AtomicBool>,
    checkpoint_fault: Arc<parking_lot::Mutex<Option<String>>>,
    request: crate::checkpoint_coordinator::CheckpointRequest,
    operator_state: Option<CapturedOperatorState>,
    operator_state_encoded_budget: u64,
    mutable_operator_capture_guard: Option<MutableCheckpointCaptureGuard>,
    assignment_fence: laminar_core::cluster::control::CheckpointAssignmentFence,
    identity: CertifiedCheckpointAttempt,
    vnode_states: crate::checkpoint_coordinator::StagedVnodeStates,
    fan_out: FxHashMap<String, SourceCheckpoint>,
    local_watermark: CheckpointWatermark,
    attempt: CheckpointAttempt,
    attempt_started: std::time::Instant,
    checkpoint_timeout: Duration,
    serialization_timeout: Duration,
}

fn set_checkpoint_fault(slot: &parking_lot::Mutex<Option<String>>, reason: impl Into<String>) {
    let mut fault = slot.lock();
    if fault.is_none() {
        *fault = Some(reason.into());
    }
}

fn mutable_checkpoint_capture_failure(component: &str, error: &str) -> String {
    format!(
        "{component} checkpoint capture failed; recovery from the last committed checkpoint \
         is required: {error}"
    )
}

/// Cancellation guard for a checkpoint image that has already consumed mutable operator state.
/// An outer attempt deadline may drop the async capture future while its non-abortable blocking
/// encoder is still running; that must fault the pipeline instead of retrying from drained state.
struct MutableCheckpointCaptureGuard {
    checkpoint_fault: Arc<parking_lot::Mutex<Option<String>>>,
    armed: bool,
}

impl MutableCheckpointCaptureGuard {
    fn new(checkpoint_fault: Arc<parking_lot::Mutex<Option<String>>>) -> Self {
        Self {
            checkpoint_fault,
            armed: true,
        }
    }

    fn fail(&mut self, error: &str) -> String {
        let reason = mutable_checkpoint_capture_failure("operator state", error);
        set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
        self.armed = false;
        reason
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for MutableCheckpointCaptureGuard {
    fn drop(&mut self) {
        if self.armed {
            set_checkpoint_fault(
                &self.checkpoint_fault,
                mutable_checkpoint_capture_failure(
                    "operator state",
                    "capture was cancelled after mutable state was consumed",
                ),
            );
        }
    }
}

fn fail_after_mutable_capture(
    guard: &mut Option<MutableCheckpointCaptureGuard>,
    error: String,
) -> String {
    guard
        .as_mut()
        .map_or(error.clone(), |guard| guard.fail(&error))
}

async fn deliver_checkpoint_completion(
    tx: &crossfire::MAsyncTx<crossfire::mpsc::Array<CheckpointCompletion>>,
    completion: CheckpointCompletion,
    deadline: tokio::time::Instant,
) -> bool {
    matches!(
        tokio::time::timeout_at(deadline, tx.send(completion)).await,
        Ok(Ok(()))
    )
}

async fn deliver_checkpoint_failure(
    tx: &crossfire::MAsyncTx<crossfire::mpsc::Array<CheckpointCompletion>>,
    attempt: CheckpointAttempt,
    error: impl Into<String>,
    checkpoint_fault: &parking_lot::Mutex<Option<String>>,
) {
    let error = error.into();
    let deadline = tokio::time::Instant::now() + CHECKPOINT_FAILURE_REPORT_TIMEOUT;
    if !deliver_checkpoint_completion(
        tx,
        CheckpointCompletion::failed(attempt, error.clone()),
        deadline,
    )
    .await
    {
        set_checkpoint_fault(
            checkpoint_fault,
            format!(
                "checkpoint {} epoch {} failed ({error}), but its terminal failure could not be \
                 reported within {:?}",
                attempt.checkpoint_id, attempt.epoch, CHECKPOINT_FAILURE_REPORT_TIMEOUT
            ),
        );
    }
}

async fn cleanup_reserved_attempt_until(
    coordinator: &tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>,
    attempt: CheckpointAttempt,
    reason: String,
    assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    leader_proof: Option<laminar_core::checkpoint::LeaderProof>,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    tokio::time::timeout_at(deadline, async {
        let mut guard = coordinator.lock().await;
        let coordinator = guard.as_mut().ok_or_else(|| {
            format!(
                "checkpoint {} epoch {} has no initialized coordinator for cleanup",
                attempt.checkpoint_id, attempt.epoch
            )
        })?;
        coordinator
            .abandon_epoch_until(
                attempt.checkpoint_id,
                attempt.epoch,
                reason,
                assignment_fence,
                leader_proof,
                deadline,
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|_| {
        format!(
            "checkpoint {} epoch {} cleanup exceeded its end-to-end deadline",
            attempt.checkpoint_id, attempt.epoch
        )
    })?
}

/// Materialize immutable source snapshots at the durability boundary.
///
/// `SourceCheckpoint` clones share persistent offset trees, so moving this conversion into a
/// blocking worker keeps an `O(N)` file inventory traversal off Tokio workers and the pipeline
/// callback. The resulting map owns the exact bytes persisted in the checkpoint manifest.
fn materialize_source_checkpoint_map(
    checkpoints: FxHashMap<String, SourceCheckpoint>,
) -> HashMap<String, ConnectorCheckpoint> {
    checkpoints
        .into_iter()
        .map(|(name, checkpoint)| {
            (
                name,
                crate::checkpoint_coordinator::source_to_connector_checkpoint(&checkpoint),
            )
        })
        .collect()
}

/// Run source-offset materialization without refreshing the attempt's absolute deadline.
async fn materialize_source_checkpoints_until(
    checkpoints: FxHashMap<String, SourceCheckpoint>,
    attempt: CheckpointAttempt,
    deadline: tokio::time::Instant,
) -> Result<HashMap<String, ConnectorCheckpoint>, String> {
    if tokio::time::Instant::now() >= deadline {
        return Err(format!(
            "checkpoint {} epoch {} exhausted its end-to-end deadline before source-offset \
             materialization",
            attempt.checkpoint_id, attempt.epoch
        ));
    }

    let materialization =
        tokio::task::spawn_blocking(move || materialize_source_checkpoint_map(checkpoints));
    match tokio::time::timeout_at(deadline, materialization).await {
        Ok(Ok(offsets)) => Ok(offsets),
        Ok(Err(error)) => Err(format!(
            "checkpoint {} epoch {} source-offset materialization worker failed: {error}",
            attempt.checkpoint_id, attempt.epoch
        )),
        Err(_) => Err(format!(
            "checkpoint {} epoch {} source-offset materialization exceeded its end-to-end \
             deadline",
            attempt.checkpoint_id, attempt.epoch
        )),
    }
}

/// Fail a leader tail before the coordinator has taken ownership of the attempt.
///
/// Terminal reporting, cluster Abort publication, and exact-attempt abandonment run
/// concurrently. Cleanup therefore starts immediately even when the completion channel or
/// cluster control plane is slow, while every cleanup operation remains bounded by one private
/// runtime-owned deadline.
async fn fail_reserved_leader_attempt(
    tail: &LeaderTail,
    terminal_error: String,
    cleanup_reason: String,
) {
    let attempt = tail.attempt;
    if tail.fault_on_failure {
        set_checkpoint_fault(&tail.checkpoint_fault, terminal_error.clone());
    }
    #[cfg(feature = "cluster")]
    tail.delta_rebase_needed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let cleanup_deadline = tokio::time::Instant::now() + tail.checkpoint_cleanup_timeout;
    let report = deliver_checkpoint_failure(
        &tail.complete_tx,
        attempt,
        terminal_error,
        &tail.checkpoint_fault,
    );
    #[cfg(feature = "cluster")]
    let leader_proof = tail.leader_proof.clone();
    #[cfg(not(feature = "cluster"))]
    let leader_proof = None;
    let cleanup = cleanup_reserved_attempt_until(
        tail.coordinator.as_ref(),
        attempt,
        cleanup_reason,
        tail.request.assignment_fence.clone(),
        leader_proof,
        cleanup_deadline,
    );

    let ((), cleanup_result) = tokio::join!(report, cleanup);

    let mut cleanup_errors = Vec::new();
    if let Err(error) = cleanup_result {
        cleanup_errors.push(error);
    }
    if !cleanup_errors.is_empty() {
        let cleanup_fault = format!(
            "checkpoint {} epoch {} pre-execution cleanup incomplete: {}",
            attempt.checkpoint_id,
            attempt.epoch,
            cleanup_errors.join("; ")
        );
        tracing::error!(%cleanup_fault, "checkpoint cleanup faulted the pipeline");
        set_checkpoint_fault(&tail.checkpoint_fault, cleanup_fault);
    }
}

#[allow(clippy::disallowed_types)]
fn combine_operator_checkpoint_states<I>(
    graph_state: Option<bytes::Bytes>,
    mv_states: I,
) -> std::collections::HashMap<String, bytes::Bytes>
where
    I: IntoIterator<Item = (String, bytes::Bytes)>,
{
    let mut states = std::collections::HashMap::with_capacity(2);
    if let Some(bytes) = graph_state {
        states.insert("operator_graph".to_string(), bytes);
    }
    states.extend(mv_states);
    states
}

const GRAPH_CHECKPOINT_CAPTURE_OVERHEAD: u64 = 256;
const GRAPH_CHECKPOINT_ENTRY_OVERHEAD: u64 = 128;

fn graph_checkpoint_capture_estimated_bytes(
    checkpoint: Option<&crate::operator_graph::GraphCheckpoint>,
) -> Result<u64, DbError> {
    let Some(checkpoint) = checkpoint else {
        return Ok(0);
    };
    checkpoint.operators.iter().try_fold(
        GRAPH_CHECKPOINT_CAPTURE_OVERHEAD,
        |total, (name, data)| {
            let name_bytes = u64::try_from(name.len()).map_err(|_| {
                DbError::Checkpoint("operator checkpoint name size does not fit u64".into())
            })?;
            let data_bytes = u64::try_from(data.len()).map_err(|_| {
                DbError::Checkpoint("operator checkpoint state size does not fit u64".into())
            })?;
            total
                .checked_add(GRAPH_CHECKPOINT_ENTRY_OVERHEAD)
                .and_then(|bytes| bytes.checked_add(name_bytes))
                .and_then(|bytes| bytes.checked_add(data_bytes))
                .ok_or_else(|| {
                    DbError::Checkpoint("operator checkpoint capture size overflowed u64".into())
                })
        },
    )
}

struct OperatorStateCapture {
    graph: Option<crate::operator_graph::GraphCheckpoint>,
    materialized_views: crate::mv_store::MvCheckpointCapture,
    reference_tables: Option<crate::table_store::ReferenceTableCheckpointCapture>,
    serialization_permit: tokio::sync::OwnedSemaphorePermit,
}

impl OperatorStateCapture {
    fn encode(
        self,
        max_encoded_bytes: u64,
    ) -> Result<std::collections::HashMap<String, bytes::Bytes>, DbError> {
        let Self {
            graph,
            materialized_views,
            reference_tables,
            serialization_permit,
        } = self;
        let mut remaining = max_encoded_bytes;

        let graph = graph
            .as_ref()
            .map(|checkpoint| {
                crate::operator_graph::OperatorGraph::serialize_checkpoint_bounded(
                    checkpoint, remaining,
                )
            })
            .transpose()?
            .map(|state| {
                let retained = u64::try_from(state.capacity()).map_err(|_| {
                    DbError::Checkpoint(
                        "operator graph checkpoint capacity does not fit u64".into(),
                    )
                })?;
                remaining = remaining.checked_sub(retained).ok_or_else(|| {
                    DbError::Checkpoint(
                        "operator graph checkpoint exceeded its staged-state budget".into(),
                    )
                })?;
                Ok::<_, DbError>(bytes::Bytes::from(state))
            })
            .transpose()?;

        let (materialized_views, mv_retained_bytes) =
            materialized_views.encode(remaining)?.into_parts();
        remaining = remaining.checked_sub(mv_retained_bytes).ok_or_else(|| {
            DbError::Checkpoint("MV checkpoint exceeded the remaining staged-state budget".into())
        })?;

        let reference_tables = reference_tables
            .map(|capture| capture.encode(remaining))
            .transpose()?;
        let mut states = combine_operator_checkpoint_states(graph, materialized_views);
        if let Some(reference_tables) = reference_tables {
            if states
                .insert(
                    crate::table_store::REFERENCE_TABLE_CHECKPOINT_KEY.to_string(),
                    reference_tables,
                )
                .is_some()
            {
                return Err(DbError::Checkpoint(
                    "reserved reference-table checkpoint key collision".into(),
                ));
            }
        }

        // The permit is deliberately owned by the non-abortable worker. If its async waiter times
        // out, another checkpoint cannot capture a second image until this worker actually exits.
        drop(serialization_permit);
        Ok(states)
    }
}

/// Immutable operator image captured at the aligned cut. Encoding is deliberately deferred to
/// the durable tail so at-least-once sources can resume while Arrow IPC and rkyv run off-thread.
struct CapturedOperatorState {
    image: OperatorStateCapture,
    estimated_bytes: u64,
    mutable_capture_guard: Option<MutableCheckpointCaptureGuard>,
}

struct SerializedOperatorState {
    states: std::collections::HashMap<String, bytes::Bytes>,
    mutable_capture_guard: Option<MutableCheckpointCaptureGuard>,
}

impl SerializedOperatorState {
    #[cfg(test)]
    fn accept_for_test(mut self) -> std::collections::HashMap<String, bytes::Bytes> {
        if let Some(guard) = self.mutable_capture_guard.as_mut() {
            guard.disarm();
        }
        self.states
    }
}

impl CapturedOperatorState {
    const fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    async fn serialize_until(
        self,
        max_encoded_bytes: u64,
        serialization_timeout: Duration,
        attempt_deadline: tokio::time::Instant,
    ) -> Result<SerializedOperatorState, String> {
        let Self {
            image,
            estimated_bytes: _,
            mut mutable_capture_guard,
        } = self;
        let serialization_deadline =
            attempt_deadline.min(tokio::time::Instant::now() + serialization_timeout);
        let worker = tokio::task::spawn_blocking(move || image.encode(max_encoded_bytes));
        let states = match tokio::time::timeout_at(serialization_deadline, worker).await {
            Err(_) => {
                let error = format!(
                    "[LDB-6017] checkpoint state serialization timed out ({serialization_timeout:?})"
                );
                return Err(fail_after_mutable_capture(
                    &mut mutable_capture_guard,
                    error,
                ));
            }
            Ok(Err(error)) => {
                let error = format!("checkpoint state serialization join failed: {error}");
                return Err(fail_after_mutable_capture(
                    &mut mutable_capture_guard,
                    error,
                ));
            }
            Ok(Ok(Err(error))) => {
                let error = format!("checkpoint state serialization failed: {error}");
                return Err(fail_after_mutable_capture(
                    &mut mutable_capture_guard,
                    error,
                ));
            }
            Ok(Ok(Ok(states))) => states,
        };
        Ok(SerializedOperatorState {
            states,
            mutable_capture_guard,
        })
    }
}

/// Bytes held in memory by a pending checkpoint (operator states + per-vnode slices).
#[allow(clippy::disallowed_types)]
fn staged_request_bytes(
    request: &crate::checkpoint_coordinator::CheckpointRequest,
    vnode_states: &crate::checkpoint_coordinator::StagedVnodeStates,
) -> u64 {
    let ops = request
        .operator_states
        .values()
        .fold(0_u64, |total, bytes| {
            total.saturating_add(bytes.len() as u64)
        });
    let vnodes = vnode_states
        .values()
        .flat_map(|m| m.values())
        .fold(0_u64, |total, slice| {
            let slice_bytes = match slice {
                crate::checkpoint_coordinator::StagedSlice::Bytes(b) => b.len() as u64,
                crate::checkpoint_coordinator::StagedSlice::Delta(changed) => changed.len() as u64,
            };
            total.saturating_add(slice_bytes)
        });
    ops.saturating_add(vnodes)
}

#[allow(clippy::disallowed_types)]
fn staged_vnode_bytes(vnode_states: &crate::checkpoint_coordinator::StagedVnodeStates) -> u64 {
    staged_request_bytes(
        &crate::checkpoint_coordinator::CheckpointRequest::default(),
        vnode_states,
    )
}

fn encoded_operator_state_budget(
    staged_cap_bytes: u64,
    capture_bytes: u64,
    vnode_bytes: u64,
) -> Result<u64, String> {
    staged_cap_bytes
        .checked_sub(capture_bytes)
        .and_then(|remaining| remaining.checked_sub(vnode_bytes))
        .ok_or_else(|| {
            format!(
                "checkpoint immutable capture ({capture_bytes} operator bytes and {vnode_bytes} vnode bytes) exceeds the staged-state cap of {staged_cap_bytes} bytes"
            )
        })
}

/// Exact checkpoint identity retained across follower admission, durable-tail execution, and
/// terminal resume. The certificate and leader proof distinguish same-attempt equivocation.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct CertifiedCheckpointAttempt {
    attempt: CheckpointAttempt,
    assignment_digest: [u8; 32],
    leader_proof: laminar_core::cluster::control::LeaderProof,
}

/// A generous fail-closed ceiling for follower identities retained by one pipeline. The normal
/// in-flight depth is four; keeping 256 exact epoch bindings covers deep test/soak configurations
/// while preventing a faulty leader from growing follower control-plane memory without bound.
#[cfg(feature = "cluster")]
const MAX_RETAINED_FOLLOWER_IDENTITIES: usize = 256;

#[cfg(feature = "cluster")]
#[derive(Debug, Clone)]
struct RetainedFollowerIdentity {
    identity: CertifiedCheckpointAttempt,
    in_flight: bool,
}

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FollowerAdmission {
    Reserved,
    Covered,
}

#[cfg(feature = "cluster")]
enum FollowerPrepareAdmission {
    Idle,
    Started(CheckpointAttempt),
    CaptureNow(laminar_core::cluster::control::BarrierAnnouncement),
    Failed {
        attempt: CheckpointAttempt,
        error: String,
    },
}

#[cfg(feature = "cluster")]
#[derive(Debug, Default)]
struct FollowerTailProgress {
    /// Exact identity binding for every retained epoch. Failed and aborted attempts stay here so
    /// an exact retry is allowed but a same-epoch checkpoint/certificate rebind is rejected.
    attempts: std::collections::BTreeMap<u64, RetainedFollowerIdentity>,
    committed: Option<CertifiedCheckpointAttempt>,
}

/// Follower durable-tail bookkeeping bound to exact attempt and assignment identities.
#[cfg(feature = "cluster")]
#[derive(Debug, Default)]
pub(crate) struct FollowerTailState {
    progress: parking_lot::Mutex<FollowerTailProgress>,
}

#[cfg(feature = "cluster")]
impl FollowerTailState {
    /// Atomically validate and reserve one certified follower attempt.
    ///
    /// Exact inactive identities are retries and may be reserved again. Exact active identities,
    /// committed identities, and stale epochs are already covered. A different identity for any
    /// retained epoch is equivocation and fails closed.
    fn reserve(&self, announced: CertifiedCheckpointAttempt) -> Result<FollowerAdmission, String> {
        let mut progress = self.progress.lock();
        let epoch = announced.attempt.epoch;

        if let Some(committed) = progress.committed.as_ref() {
            if Self::highwater_covers(committed, &announced)? {
                return Ok(FollowerAdmission::Covered);
            }
        }

        if let Some(retained) = progress.attempts.get(&epoch).cloned() {
            if retained.identity != announced {
                return Err(Self::identity_conflict(&retained.identity, &announced));
            }
            if retained.in_flight {
                return Ok(FollowerAdmission::Covered);
            }
            if let Some((_, latest)) = progress
                .attempts
                .last_key_value()
                .filter(|(&latest_epoch, _)| latest_epoch > epoch)
            {
                if Self::highwater_covers(&latest.identity, &announced)? {
                    return Ok(FollowerAdmission::Covered);
                }
            }
            progress.attempts.insert(
                epoch,
                RetainedFollowerIdentity {
                    identity: announced.clone(),
                    in_flight: true,
                },
            );
            return Ok(FollowerAdmission::Reserved);
        }

        if let Some((_, latest)) = progress.attempts.last_key_value() {
            if Self::highwater_covers(&latest.identity, &announced)? {
                return Ok(FollowerAdmission::Covered);
            }
        }

        while progress.attempts.len() >= MAX_RETAINED_FOLLOWER_IDENTITIES {
            let evictable = progress
                .attempts
                .iter()
                .find_map(|(&retained_epoch, retained)| {
                    (!retained.in_flight).then_some(retained_epoch)
                });
            let Some(evictable) = evictable else {
                return Err(format!(
                    "[LDB-6055] follower identity capacity {MAX_RETAINED_FOLLOWER_IDENTITIES} \
                     exhausted by concurrent checkpoint tails"
                ));
            };
            progress.attempts.remove(&evictable);
        }

        progress.attempts.insert(
            epoch,
            RetainedFollowerIdentity {
                identity: announced,
                in_flight: true,
            },
        );
        Ok(FollowerAdmission::Reserved)
    }

    /// Record one exact tail outcome without allowing a stale or equivocal terminal to mutate a
    /// different attempt. Failure and abort release the slot but retain the epoch's identity.
    fn finish(&self, identity: &CertifiedCheckpointAttempt, committed: bool) -> Result<(), String> {
        let mut progress = self.progress.lock();
        let Some(retained) = progress.attempts.get(&identity.attempt.epoch) else {
            return Err(format!(
                "[LDB-6055] follower terminal has no reserved identity for epoch {}",
                identity.attempt.epoch
            ));
        };
        if retained.identity != *identity {
            return Err(Self::identity_conflict(&retained.identity, identity));
        }
        if !retained.in_flight {
            return Err(format!(
                "[LDB-6055] follower terminal repeated inactive checkpoint {} epoch {}",
                identity.attempt.checkpoint_id, identity.attempt.epoch
            ));
        }

        let advance_committed = if committed {
            match progress.committed.as_ref() {
                None => true,
                Some(current) => match current.attempt.relation_to(identity.attempt) {
                    CheckpointAttemptRelation::Older => true,
                    CheckpointAttemptRelation::Exact if current != identity => {
                        return Err(Self::identity_conflict(current, identity));
                    }
                    CheckpointAttemptRelation::Conflict => {
                        return Err(Self::attempt_conflict(current, identity));
                    }
                    CheckpointAttemptRelation::Exact | CheckpointAttemptRelation::Newer => false,
                },
            }
        } else {
            false
        };

        // Validate the terminal and committed high-watermark before releasing the admission
        // fence. A rejected terminal remains in flight for recovery to resolve.
        progress
            .attempts
            .get_mut(&identity.attempt.epoch)
            .expect("validated follower identity must remain retained")
            .in_flight = false;
        if advance_committed {
            progress.committed = Some(identity.clone());
        }
        Ok(())
    }

    /// Release an admission identity only after the durable tail applied an authoritative
    /// Commit or Abort. An in-doubt/error result deliberately keeps the identity in flight so a
    /// same-attempt retry cannot reuse prepared state before recovery resolves it.
    fn finish_resolved(
        &self,
        identity: &CertifiedCheckpointAttempt,
        outcome: &Result<bool, DbError>,
    ) -> Result<Option<bool>, String> {
        let Ok(committed) = outcome else {
            return Ok(None);
        };
        self.finish(identity, *committed)?;
        Ok(Some(*committed))
    }

    fn identity_conflict(
        retained: &CertifiedCheckpointAttempt,
        announced: &CertifiedCheckpointAttempt,
    ) -> String {
        format!(
            "[LDB-6055] conflicting checkpoint identity for epoch {}: retained id {} digest {:?} \
             authority {:?}, announced id {} digest {:?} authority {:?}",
            announced.attempt.epoch,
            retained.attempt.checkpoint_id,
            retained.assignment_digest,
            retained.leader_proof,
            announced.attempt.checkpoint_id,
            announced.assignment_digest,
            announced.leader_proof,
        )
    }

    fn attempt_conflict(
        retained: &CertifiedCheckpointAttempt,
        announced: &CertifiedCheckpointAttempt,
    ) -> String {
        format!(
            "[LDB-6055] conflicting checkpoint progress: retained epoch {} id {}, announced epoch \
             {} id {}",
            retained.attempt.epoch,
            retained.attempt.checkpoint_id,
            announced.attempt.epoch,
            announced.attempt.checkpoint_id
        )
    }

    fn highwater_covers(
        retained: &CertifiedCheckpointAttempt,
        announced: &CertifiedCheckpointAttempt,
    ) -> Result<bool, String> {
        match retained.attempt.relation_to(announced.attempt) {
            CheckpointAttemptRelation::Exact if retained == announced => Ok(true),
            CheckpointAttemptRelation::Exact => Err(Self::identity_conflict(retained, announced)),
            CheckpointAttemptRelation::Newer => Ok(true),
            CheckpointAttemptRelation::Older => Ok(false),
            CheckpointAttemptRelation::Conflict => Err(Self::attempt_conflict(retained, announced)),
        }
    }

    #[cfg(test)]
    fn in_flight(&self) -> Vec<CertifiedCheckpointAttempt> {
        self.progress
            .lock()
            .attempts
            .values()
            .filter(|retained| retained.in_flight)
            .map(|retained| retained.identity.clone())
            .collect()
    }

    #[cfg(test)]
    fn committed(&self) -> Option<CertifiedCheckpointAttempt> {
        self.progress.lock().committed.clone()
    }
}

fn classify_checkpoint_watermark(
    source_count: usize,
    active_source_count: usize,
    pipeline_watermark: i64,
) -> CheckpointWatermark {
    if source_count == 0 || active_source_count == 0 {
        CheckpointWatermark::Idle
    } else if pipeline_watermark == i64::MIN {
        CheckpointWatermark::Uninitialized
    } else {
        CheckpointWatermark::Active(pipeline_watermark)
    }
}

#[allow(clippy::struct_excessive_bools)] // config/state flags, not a state machine
pub(crate) struct ConnectorPipelineCallback {
    pub(crate) graph: crate::operator_graph::OperatorGraph,
    pub(crate) stream_entries: Vec<Arc<crate::catalog::StreamEntry>>,
    #[allow(clippy::type_complexity)]
    pub(crate) sinks: Vec<(
        String,
        crate::sink_task::SinkTaskHandle,
        Option<String>,
        String, // input stream name (FROM clause target)
        SinkContract,
    )>,
    pub(crate) owned_sink_handles: Arc<parking_lot::Mutex<Vec<crate::sink_task::SinkTaskHandle>>>,
    pub(crate) watermark_states: FxHashMap<String, SourceWatermarkState>,
    pub(crate) source_entries_for_wm: FxHashMap<String, Arc<crate::catalog::SourceEntry>>,
    pub(crate) source_ids: FxHashMap<String, usize>,
    pub(crate) source_name_arcs: FxHashMap<usize, Arc<str>>,
    pub(crate) source_wms_buf: FxHashMap<Arc<str>, i64>,
    pub(crate) tracker: Option<laminar_core::time::WatermarkTracker>,
    pub(crate) prom: Arc<crate::engine_metrics::EngineMetrics>,
    pub(crate) pipeline_watermark: Arc<std::sync::atomic::AtomicI64>,
    pub(crate) coordinator:
        Arc<tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>>,
    pub(crate) table_store: Arc<parking_lot::RwLock<crate::table_store::TableStore>>,
    pub(crate) mv_store: Arc<parking_lot::RwLock<crate::mv_store::MvStore>>,
    /// Mirrors `MvStore::has_any` so the per-cycle check skips the write lock.
    pub(crate) mv_store_has_any: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) filter_ctx: SessionContext,
    pub(crate) compiled_sink_filters: Vec<SinkFilter>,
    pub(crate) pending_sink_filter_compiles: usize,
    pub(crate) delivery_guarantee: laminar_connectors::connector::DeliveryGuarantee,
    pub(crate) serialization_timeout: Duration,
    pub(crate) checkpoint_state_cap_bytes: u64,
    pub(crate) checkpoint_serialization_gate: Arc<tokio::sync::Semaphore>,
    /// One semantic deadline spanning sink fence, capture, quorum, and durable publication.
    pub(crate) checkpoint_timeout: Duration,
    /// Runtime-owned budget for Abort publication, coordinator acquisition and sink cleanup.
    pub(crate) checkpoint_cleanup_timeout: Duration,
    pub(crate) sink_event_rx: laminar_core::streaming::AsyncConsumer<crate::sink_task::SinkEvent>,
    /// Set when a best-effort sink write is dropped; suppresses checkpoint admission while the
    /// handle's sticky poison prevents later durable publication.
    pub(crate) sink_timed_out: bool,
    /// Set when an exactly-once sink fails (poisoned epoch); the coordinator polls it via
    /// `take_pipeline_fault` and faults for recovery so the dropped rows are replayed (CP-4).
    pub(crate) sink_fault: Option<String>,
    /// Fault raised by capture or a spawned checkpoint tail. Kept separate from `sink_fault`
    /// because durable decision waits run outside the callback.
    pub(crate) checkpoint_fault: Arc<parking_lot::Mutex<Option<String>>>,
    pub(crate) shutdown_signal: Arc<tokio::sync::Notify>,
    #[cfg(feature = "cluster")]
    pub(crate) cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    /// Frames a peer shuffled to us that never arrived (CL-2). Read before every seal.
    #[cfg(feature = "cluster")]
    pub(crate) shuffle_delivery_loss_incidents: Option<Arc<std::sync::atomic::AtomicU64>>,
    /// Cumulative loss cutoff repaired by a completed coordinated rewind.
    #[cfg(feature = "cluster")]
    pub(crate) shuffle_recovered_delivery_loss_incidents: Option<Arc<std::sync::atomic::AtomicU64>>,
    #[cfg(feature = "cluster")]
    pub(crate) shuffle_delivery_loss_incidents_seen: u64,
    /// Current assignment source for validating the leader's version-bound checkpoint fence.
    /// `None` only outside a clustered vnode runtime.
    #[cfg(feature = "cluster")]
    pub(crate) vnode_registry: Option<Arc<laminar_core::state::VnodeRegistry>>,
    /// Assignment version that last installed an exact source recovery cut.
    /// Carry-only assignment publications must not rewind live watermark state.
    #[cfg(feature = "cluster")]
    pub(crate) reconciled_source_handoff_version: Option<u64>,
    /// Bounded exact-identity bindings and overlapping durable tails for follower admission.
    #[cfg(feature = "cluster")]
    pub(crate) follower_tail: Arc<FollowerTailState>,
    #[cfg(feature = "cluster")]
    pub(crate) barrier_injectors: Vec<crate::pipeline::callback::SourceBarrierControl>,
    #[cfg(feature = "cluster")]
    pub(crate) pending_follower_checkpoint:
        Option<laminar_core::cluster::control::BarrierAnnouncement>,
    /// Exact authority retained between successful `Prepare` publication and durable-tail handoff.
    #[cfg(feature = "cluster")]
    pub(crate) checkpoint_leader_proofs:
        FxHashMap<CheckpointAttempt, laminar_core::cluster::control::LeaderProof>,
    pub(crate) subscription_registry: Arc<crate::subscription::SubscriptionRegistry>,
    pub(crate) named_stream_names: rustc_hash::FxHashSet<Arc<str>>,
    pub(crate) checkpoint_complete_tx:
        crossfire::MAsyncTx<crossfire::mpsc::Array<CheckpointCompletion>>,
    /// Every asynchronous ALO checkpoint tail. `JoinSet` provides structured cancellation and
    /// prevents shutdown from racing detached state/sink work.
    pub(crate) checkpoint_tail_tasks: tokio::task::JoinSet<()>,
    /// In-flight epoch count; the coordinator serializes durable checkpoint tails.
    pub(crate) checkpoint_in_flight: Arc<std::sync::atomic::AtomicU64>,
    pub(crate) staged_bytes: Arc<std::sync::atomic::AtomicU64>,
    /// Set by a checkpoint tail on epoch failure; the next capture consumes it to force a FULL
    /// re-base. Serialized checkpoint tails ensure the flag is observed before the next capture.
    #[cfg(feature = "cluster")]
    pub(crate) delta_rebase_needed: Arc<std::sync::atomic::AtomicBool>,
    /// Last epoch whose vnode snapshot completed in this callback. Leader high-watermark adoption
    /// can jump epochs without a local failure, which must force a FULL before delta capture.
    #[cfg(feature = "cluster")]
    pub(crate) last_vnode_capture_epoch: Option<u64>,
    /// Lock-free id allocator shared with the coordinator so barrier admission doesn't
    /// queue behind an earlier epoch's durable tail holding the coordinator mutex.
    pub(crate) epoch_allocator: Option<Arc<crate::checkpoint_coordinator::EpochAllocator>>,
    #[cfg(feature = "cluster")]
    pub(crate) quorum_timeout: Duration,
    /// When true, durable tails run inline so post-barrier rows cannot enter an epoch-N open
    /// transaction or staged descriptor.
    pub(crate) checkpoint_committable_sinks: bool,
    /// Cluster startup/recovery fence. While set, neither source nor shuffle input may be folded.
    pub(crate) intake_gate: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(feature = "cluster")]
fn observe_unrecovered_delivery_loss_incidents(
    incidents: u64,
    recovered: u64,
    seen: &mut u64,
    recovery_active: bool,
) -> Option<u64> {
    *seen = (*seen).max(recovered);
    if recovery_active || incidents <= *seen {
        return None;
    }
    let new_incidents = incidents - *seen;
    *seen = incidents;
    Some(new_incidents)
}

impl ConnectorPipelineCallback {
    #[cfg(feature = "cluster")]
    fn reconcile_source_handoff_watermarks(&mut self) -> Result<(), String> {
        let Some(registry) = self.vnode_registry.as_ref() else {
            return Ok(());
        };
        let published = registry.versioned_snapshot();
        let Some(installed_version) = published.source_handoff_installed_version() else {
            return Ok(());
        };
        if self
            .reconciled_source_handoff_version
            .is_some_and(|reconciled| reconciled >= installed_version)
        {
            return Ok(());
        }
        let handoff = published.committed_source_handoff().ok_or_else(|| {
            format!(
                "assignment {installed_version} names an installed source handoff without its committed cut"
            )
        })?;

        for (source_name, _) in handoff.sources() {
            if !self.source_entries_for_wm.contains_key(source_name) {
                return Err(format!(
                    "committed checkpoint {:?} names unknown source '{source_name}'",
                    handoff.attempt()
                ));
            }
        }

        let combined_watermark = handoff.recovery_watermark_frontier();

        let mut tracker_watermarks = self.tracker.as_ref().map(|tracker| {
            (0..tracker.num_sources())
                .map(|source_id| {
                    tracker
                        .source_watermark(source_id)
                        .filter(|watermark| *watermark != i64::MIN)
                })
                .collect::<Vec<_>>()
        });
        let mut tracker_idle = self.tracker.as_ref().map(|tracker| {
            (0..tracker.num_sources())
                .map(|source_id| tracker.is_idle(source_id))
                .collect::<Vec<_>>()
        });
        if let (Some(watermarks), Some(idle)) = (tracker_watermarks.as_mut(), tracker_idle.as_mut())
        {
            for source_id in 0..watermarks.len() {
                let source_name = self.source_name_arcs.get(&source_id).ok_or_else(|| {
                    format!("watermark tracker source {source_id} has no canonical source name")
                })?;
                let recovered = handoff.source(source_name).ok_or_else(|| {
                    format!(
                        "committed checkpoint {:?} has no watermark state for tracked source '{}'",
                        handoff.attempt(),
                        source_name
                    )
                })?;
                // A missing per-source value represents an idle source in an
                // otherwise initialized cut. The committed cluster frontier is
                // still a durable lower bound when that source reactivates.
                watermarks[source_id] = recovered.watermark().or(combined_watermark);
                idle[source_id] = handoff.cluster_watermark() == CheckpointWatermark::Idle;
            }
        }
        if let Some(tracker) = self.tracker.as_mut() {
            tracker
                .restore_for_recovery(
                    tracker_watermarks
                        .as_deref()
                        .expect("tracker recovery watermarks were constructed"),
                    tracker_idle
                        .as_deref()
                        .expect("tracker recovery idle states were constructed"),
                    combined_watermark,
                )
                .map_err(|error| error.to_string())?;
        } else if handoff.cluster_watermark() != CheckpointWatermark::Idle {
            return Err(format!(
                "committed checkpoint {:?} has {:?} watermark state but the pipeline tracks no event-time sources",
                handoff.attempt(),
                handoff.cluster_watermark()
            ));
        }

        for (source_name, recovered) in handoff.sources() {
            let watermark = recovered
                .watermark()
                .or(combined_watermark)
                .unwrap_or(i64::MIN);
            self.source_entries_for_wm
                .get(source_name)
                .expect("source handoff names were validated above")
                .source
                .restore_watermark_for_recovery(watermark);
            if let Some(state) = self.watermark_states.get_mut(source_name) {
                state.generator.restore_watermark_for_recovery(watermark);
            }
        }
        self.pipeline_watermark.store(
            combined_watermark.unwrap_or(i64::MIN),
            std::sync::atomic::Ordering::Release,
        );
        self.reconciled_source_handoff_version = Some(installed_version);
        tracing::info!(
            assignment_version = published.version(),
            handoff_installed_version = installed_version,
            attempt = ?handoff.attempt(),
            sources = handoff.source_count(),
            watermark = ?handoff.cluster_watermark(),
            "installed committed source watermark handoff before intake"
        );
        Ok(())
    }

    fn reap_checkpoint_tail_tasks(&mut self) {
        while let Some(result) = self.checkpoint_tail_tasks.try_join_next() {
            if let Err(error) = result {
                set_checkpoint_fault(
                    &self.checkpoint_fault,
                    format!("checkpoint durable tail terminated unexpectedly: {error}"),
                );
            }
        }
    }

    fn spawn_checkpoint_tail(
        &mut self,
        tail: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        self.reap_checkpoint_tail_tasks();
        self.checkpoint_tail_tasks.spawn(tail);
    }
    /// Classify a graph error. Terminal errors signal shutdown before returning `Halt`.
    fn map_graph_error(
        err: &crate::error::DbError,
        shutdown: &tokio::sync::Notify,
    ) -> crate::pipeline::CycleError {
        use crate::pipeline::CycleError;
        if err.requires_pipeline_halt() {
            match err {
                crate::error::DbError::BackpressureFail(msg) => tracing::error!(
                    reason = %msg,
                    "backpressure_policy=Fail tripped; halting pipeline"
                ),
                crate::error::DbError::ShuffleTerminal(msg) => tracing::error!(
                    reason = %msg,
                    "permanent shuffle routing failure; halting pipeline"
                ),
                _ => unreachable!("requires_pipeline_halt returned true for a non-terminal error"),
            }
            shutdown.notify_one();
            return CycleError::Halt(format!("{err}"));
        }
        if err.requires_pipeline_recovery() {
            return CycleError::Recovery(format!("{err}"));
        }
        CycleError::Fatal(format!("{err}"))
    }

    /// Checkpoint drains recover ordinary graph faults, but a permanent error must retain its halt
    /// disposition or the coordinator would restart forever on the same input.
    fn map_checkpoint_drain_error(
        err: &crate::error::DbError,
        shutdown: &tokio::sync::Notify,
    ) -> crate::pipeline::CycleError {
        use crate::pipeline::CycleError;
        match Self::map_graph_error(err, shutdown) {
            CycleError::Halt(message) => {
                CycleError::Halt(format!("checkpoint graph drain halted: {message}"))
            }
            CycleError::Recovery(_) | CycleError::Fatal(_) => {
                CycleError::Recovery(format!("checkpoint graph drain failed: {err}"))
            }
        }
    }

    /// Cap a local watermark by the decision-bound cluster frontier.
    /// Before the first committed cluster cut, event time remains uninitialized.
    #[cfg(feature = "cluster")]
    fn cap_watermark_by_cluster_min(watermark: i64, cluster_wm: Option<i64>) -> i64 {
        cluster_wm.map_or(i64::MIN, |cluster_wm| watermark.min(cluster_wm))
    }

    #[cfg(feature = "cluster")]
    fn cap_source_watermarks_by_cluster_min(
        source_wms: &mut FxHashMap<Arc<str>, i64>,
        cluster_wm: Option<i64>,
    ) {
        for wm in source_wms.values_mut() {
            *wm = Self::cap_watermark_by_cluster_min(*wm, cluster_wm);
        }
    }

    fn effective_pipeline_watermark(&self) -> i64 {
        let local = self
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire);
        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.as_ref() {
            return Self::cap_watermark_by_cluster_min(local, controller.cluster_min_watermark());
        }
        local
    }

    #[cfg(feature = "cluster")]
    fn certified_announcement(
        announcement: &laminar_core::cluster::control::BarrierAnnouncement,
    ) -> Option<CertifiedCheckpointAttempt> {
        Some(CertifiedCheckpointAttempt {
            attempt: CheckpointAttempt::new(announcement.epoch, announcement.checkpoint_id),
            assignment_digest: announcement.assignment_fence.as_ref()?.digest(),
            leader_proof: announcement.leader_proof.as_ref()?.clone(),
        })
    }

    /// Release a pre-tail reservation while preserving its exact epoch binding for retry.
    #[cfg(feature = "cluster")]
    fn release_follower_reservation(&self, identity: &CertifiedCheckpointAttempt) {
        if let Err(error) = self.follower_tail.finish(identity, false) {
            set_checkpoint_fault(&self.checkpoint_fault, error);
        }
    }

    /// Leader durable tail: quorum + `Aligned` pre-mutex, then durable writes under the FIFO mutex.
    async fn run_leader_tail(tail: LeaderTail) {
        let attempt = tail.attempt;
        let remaining = tail
            .checkpoint_timeout
            .saturating_sub(tail.attempt_started.elapsed());
        let deadline = tokio::time::Instant::now() + remaining;
        if remaining.is_zero() {
            let error = format!(
                "checkpoint {} epoch {} exhausted its {:?} end-to-end deadline before the durable tail",
                attempt.checkpoint_id, attempt.epoch, tail.checkpoint_timeout
            );
            fail_reserved_leader_attempt(&tail, error.clone(), error).await;
            return;
        }

        #[cfg(feature = "cluster")]
        let Some(quorum) = Self::prepare_leader_quorum(&tail, deadline).await
        else {
            return;
        };
        #[cfg(not(feature = "cluster"))]
        let quorum = crate::checkpoint_coordinator::QuorumStage::RunInline;

        Self::execute_leader_tail(tail, quorum, deadline).await;
    }

    #[cfg(feature = "cluster")]
    async fn prepare_leader_quorum(
        tail: &LeaderTail,
        deadline: tokio::time::Instant,
    ) -> Option<crate::checkpoint_coordinator::QuorumStage> {
        use crate::checkpoint_coordinator::{CheckpointCoordinator, PrepareQuorum, QuorumStage};
        use laminar_core::cluster::control::{BarrierAnnouncement, Phase};

        let Some(controller) = tail.controller.as_ref() else {
            return Some(QuorumStage::RunInline);
        };
        let Some(assignment_fence) = tail.request.assignment_fence.as_ref() else {
            Self::handle_leader_quorum_failure(
                tail,
                "[LDB-6055] clustered durable tail lost its assignment certificate".into(),
            )
            .await;
            return None;
        };
        let Some(leader_proof) = tail.leader_proof.as_ref() else {
            Self::handle_leader_quorum_failure(
                tail,
                "clustered durable tail lost its exact leader proof".into(),
            )
            .await;
            return None;
        };
        if !controller.proof_is_live(leader_proof) {
            Self::handle_leader_quorum_failure(
                tail,
                "leadership changed before checkpoint quorum".into(),
            )
            .await;
            return None;
        }
        let attempt = tail.attempt;
        let (epoch, checkpoint_id) = (attempt.epoch, attempt.checkpoint_id);
        let quorum_timeout = tail.quorum_timeout.min(
            tail.checkpoint_timeout
                .saturating_sub(tail.attempt_started.elapsed()),
        );
        let quorum_result = tokio::time::timeout_at(
            deadline,
            CheckpointCoordinator::run_prepare_quorum(
                controller,
                quorum_timeout,
                PrepareQuorum::new(
                    attempt,
                    tail.local_watermark,
                    assignment_fence,
                    leader_proof,
                    false,
                ),
            ),
        )
        .await
        .map_err(|_| {
            format!(
                "capture quorum exhausted the {:?} end-to-end checkpoint deadline",
                tail.checkpoint_timeout
            )
        })
        .and_then(|result| result);

        match quorum_result {
            Ok((cluster_watermark, participants)) => {
                if let Ok(Err(error)) = tokio::time::timeout_at(
                    deadline,
                    controller.announce_barrier(&BarrierAnnouncement {
                        epoch,
                        checkpoint_id,
                        assignment_fence: Some(assignment_fence.clone()),
                        leader_proof: Some(leader_proof.clone()),
                        phase: Phase::Aligned,
                        flags: 0,
                        min_watermark_ms: cluster_watermark.active_value(),
                    }),
                )
                .await
                {
                    tracing::warn!(
                        epoch, %error,
                        "[LDB-6031] aligned announcement failed; peers resume on Commit"
                    );
                }
                Some(QuorumStage::Done {
                    cluster_watermark,
                    participants,
                    leader_proof: leader_proof.clone(),
                })
            }
            Err(message) => {
                Self::handle_leader_quorum_failure(tail, message).await;
                None
            }
        }
    }

    #[cfg(feature = "cluster")]
    async fn handle_leader_quorum_failure(tail: &LeaderTail, message: String) {
        let attempt = tail.attempt;
        let (epoch, checkpoint_id) = (attempt.epoch, attempt.checkpoint_id);
        tracing::error!(checkpoint_id, epoch, error = %message, "[LDB-6032] quorum miss");
        let terminal_error =
            format!("checkpoint {checkpoint_id} epoch {epoch} quorum failed: {message}");
        fail_reserved_leader_attempt(tail, terminal_error, message).await;
    }

    async fn execute_leader_tail(
        mut tail: LeaderTail,
        quorum: crate::checkpoint_coordinator::QuorumStage,
        deadline: tokio::time::Instant,
    ) {
        let attempt = tail.attempt;
        let Some(operator_state) = tail.operator_state.take() else {
            let error = format!(
                "checkpoint {} epoch {} lost its captured operator image",
                attempt.checkpoint_id, attempt.epoch
            );
            fail_reserved_leader_attempt(&tail, error.clone(), error).await;
            return;
        };
        let serialized_operator_state = match operator_state
            .serialize_until(
                tail.operator_state_encoded_budget,
                tail.serialization_timeout,
                deadline,
            )
            .await
        {
            Ok(states) => states,
            Err(error) => {
                fail_reserved_leader_attempt(&tail, error.clone(), error).await;
                return;
            }
        };
        tail.request.operator_states = serialized_operator_state.states;
        tail.mutable_operator_capture_guard = serialized_operator_state.mutable_capture_guard;

        let source_offsets =
            match materialize_source_checkpoints_until(tail.fan_out.clone(), attempt, deadline)
                .await
            {
                Ok(offsets) => offsets,
                Err(error) => {
                    fail_reserved_leader_attempt(&tail, error.clone(), error).await;
                    return;
                }
            };
        tail.request.source_offset_overrides = source_offsets;

        let remaining = tail
            .checkpoint_timeout
            .saturating_sub(tail.attempt_started.elapsed());
        let Ok(mut guard) = tokio::time::timeout(remaining, tail.coordinator.lock()).await else {
            let error = format!(
                "checkpoint {} epoch {} exceeded its {:?} end-to-end deadline waiting for the coordinator",
                attempt.checkpoint_id, attempt.epoch, tail.checkpoint_timeout
            );
            fail_reserved_leader_attempt(&tail, error.clone(), error).await;
            return;
        };

        let Some(coordinator) = guard.as_mut() else {
            drop(guard);
            let error = format!(
                "checkpoint {} epoch {} coordinator disappeared before the durable tail",
                attempt.checkpoint_id, attempt.epoch
            );
            fail_reserved_leader_attempt(&tail, error.clone(), error).await;
            return;
        };
        coordinator.set_pending_vnode_states(std::mem::take(&mut tail.vnode_states));
        coordinator.set_local_watermark(tail.local_watermark);
        let result = coordinator
            .checkpoint_preallocated_started(
                std::mem::take(&mut tail.request),
                attempt,
                quorum,
                tail.attempt_started,
            )
            .await;
        // Completion delivery may wait on a bounded channel; it must not hold
        // the FIFO checkpoint coordinator lock while doing so.
        drop(guard);
        Self::handle_leader_result(&mut tail, result, deadline).await;
    }

    async fn handle_leader_result(
        tail: &mut LeaderTail,
        result: Result<crate::checkpoint_coordinator::CheckpointResult, DbError>,
        deadline: tokio::time::Instant,
    ) {
        let attempt = tail.attempt;
        match result {
            Ok(result) if result.success => {
                Self::complete_successful_leader_tail(tail, result, deadline).await;
            }
            Ok(result) => {
                #[cfg(feature = "cluster")]
                tail.delta_rebase_needed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                tracing::warn!(
                    epoch = result.epoch,
                    error = ?result.error,
                    "Barrier-aligned checkpoint failed"
                );
                let terminal_error = format!(
                    "checkpoint {} epoch {} failed: {}",
                    result.checkpoint_id,
                    result.epoch,
                    result
                        .error
                        .as_deref()
                        .unwrap_or("unknown checkpoint failure")
                );
                if checkpoint_failure_requires_pipeline_fault(&result, tail.fault_on_failure) {
                    set_checkpoint_fault(&tail.checkpoint_fault, terminal_error.clone());
                }
                deliver_checkpoint_failure(
                    &tail.complete_tx,
                    attempt,
                    terminal_error,
                    &tail.checkpoint_fault,
                )
                .await;
            }
            Err(error) => {
                #[cfg(feature = "cluster")]
                tail.delta_rebase_needed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                tracing::warn!(%error, "Barrier-aligned checkpoint error");
                let terminal_error = error.to_string();
                if tail.fault_on_failure {
                    set_checkpoint_fault(&tail.checkpoint_fault, terminal_error.clone());
                }
                deliver_checkpoint_failure(
                    &tail.complete_tx,
                    attempt,
                    terminal_error,
                    &tail.checkpoint_fault,
                )
                .await;
            }
        }
    }

    async fn complete_successful_leader_tail(
        tail: &mut LeaderTail,
        result: crate::checkpoint_coordinator::CheckpointResult,
        deadline: tokio::time::Instant,
    ) {
        let continuation_error = result.continuation_error().map(str::to_owned);
        match CheckpointCompletion::validated(tail.attempt, result, tail.fan_out.clone()) {
            Ok(completion) => {
                if let Some(guard) = tail.mutable_operator_capture_guard.as_mut() {
                    guard.disarm();
                }
                if !deliver_checkpoint_completion(&tail.complete_tx, completion, deadline).await {
                    set_checkpoint_fault(
                        &tail.checkpoint_fault,
                        format!(
                            "checkpoint {} epoch {} committed but its completion missed the \
                             end-to-end deadline",
                            tail.attempt.checkpoint_id, tail.attempt.epoch
                        ),
                    );
                    return;
                }
                // Completion is enqueued first so the durable source cut is acknowledged before a
                // successor-epoch continuation fault fences further writes.
                if let Some(error) = continuation_error {
                    set_checkpoint_fault(&tail.checkpoint_fault, error);
                }
            }
            Err(reason) => {
                tracing::error!(
                    error = %reason,
                    "[LDB-6048] refusing mismatched checkpoint completion"
                );
                set_checkpoint_fault(&tail.checkpoint_fault, reason.clone());
                deliver_checkpoint_failure(
                    &tail.complete_tx,
                    tail.attempt,
                    reason,
                    &tail.checkpoint_fault,
                )
                .await;
            }
        }
    }

    /// Build the follower's durable tail future (ack → prepare → decision wait → 2PC).
    ///
    /// Spawned for at-least-once (resumes on `Aligned`) or awaited inline for exactly-once.
    #[cfg(feature = "cluster")]
    fn follower_tail_future(
        &mut self,
        request: crate::checkpoint_coordinator::CheckpointRequest,
        operator_state: CapturedOperatorState,
        identity: CertifiedCheckpointAttempt,
        fan_out: FxHashMap<String, SourceCheckpoint>,
        attempt_started: std::time::Instant,
    ) -> Result<impl std::future::Future<Output = ()> + Send + 'static, String> {
        let assignment_fence = request.assignment_fence.clone().ok_or_else(|| {
            "[LDB-6055] follower durable tail has no assignment certificate".to_string()
        })?;
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            "[LDB-6055] follower durable tail has no cluster controller".to_string()
        })?;
        if !assignment_fence.is_canonical()
            || !assignment_fence.contains(controller.instance_id().0)
            || controller
                .checkpoint_assignment_fence(assignment_fence.assignment_version)
                .as_ref()
                != Some(&assignment_fence)
        {
            return Err(
                "[LDB-6055] follower durable tail has an invalid assignment certificate".into(),
            );
        }
        let vnode_states = self.capture_vnode_states(identity.attempt.epoch)?;
        let operator_state_encoded_budget = encoded_operator_state_budget(
            self.checkpoint_state_cap_bytes,
            operator_state.estimated_bytes(),
            staged_vnode_bytes(&vnode_states),
        )?;
        let local_watermark = self.checkpoint_watermark();
        let attempt = identity.attempt;
        if identity.assignment_digest != assignment_fence.digest()
            || !identity.leader_proof.is_canonical()
            || assignment_fence.participant_incarnation(identity.leader_proof.owner.node_id)
                != Some(identity.leader_proof.owner.boot_id)
        {
            return Err(
                "[LDB-6055] follower durable tail has an invalid certified authority binding"
                    .into(),
            );
        }

        // Charge followers too; otherwise their capture-to-upload memory is unaccounted.
        let in_flight = EpochInFlightGuard::claim(
            &self.checkpoint_in_flight,
            &self.staged_bytes,
            self.checkpoint_state_cap_bytes,
        );
        let tail = FollowerDurableTail {
            _in_flight: in_flight,
            coordinator: Arc::clone(&self.coordinator),
            state: Arc::clone(&self.follower_tail),
            complete_tx: self.checkpoint_complete_tx.clone(),
            controller: self.cluster_controller.clone(),
            delta_rebase_needed: Arc::clone(&self.delta_rebase_needed),
            checkpoint_fault: Arc::clone(&self.checkpoint_fault),
            request,
            operator_state: Some(operator_state),
            operator_state_encoded_budget,
            mutable_operator_capture_guard: None,
            assignment_fence,
            identity,
            vnode_states,
            fan_out,
            local_watermark,
            attempt,
            attempt_started,
            checkpoint_timeout: self.checkpoint_timeout,
            serialization_timeout: self.serialization_timeout,
        };
        Ok(Self::run_follower_tail(tail))
    }

    #[cfg(feature = "cluster")]
    async fn run_follower_tail(mut tail: FollowerDurableTail) {
        let remaining = tail
            .checkpoint_timeout
            .saturating_sub(tail.attempt_started.elapsed());
        let deadline = tokio::time::Instant::now() + remaining;

        // The immutable source snapshot is already captured, so this acknowledgement may release
        // `Aligned`. The leader's exact-attempt restorable gate still requires this participant's
        // partial before a decision and therefore fences the materialization/prepare tail below.
        Self::acknowledge_follower_capture(&tail, deadline).await;
        let Some(operator_state) = tail.operator_state.take() else {
            Self::fail_follower_tail_before_prepare(
                &tail,
                format!(
                    "checkpoint {} epoch {} lost its captured operator image",
                    tail.attempt.checkpoint_id, tail.attempt.epoch
                ),
                deadline,
            )
            .await;
            return;
        };
        let serialized_operator_state = match operator_state
            .serialize_until(
                tail.operator_state_encoded_budget,
                tail.serialization_timeout,
                deadline,
            )
            .await
        {
            Ok(states) => states,
            Err(error) => {
                Self::fail_follower_tail_before_prepare(&tail, error, deadline).await;
                return;
            }
        };
        tail.request.operator_states = serialized_operator_state.states;
        tail.mutable_operator_capture_guard = serialized_operator_state.mutable_capture_guard;

        let source_offsets = match materialize_source_checkpoints_until(
            tail.fan_out.clone(),
            tail.attempt,
            deadline,
        )
        .await
        {
            Ok(offsets) => offsets,
            Err(error) => {
                Self::fail_follower_tail_before_prepare(&tail, error, deadline).await;
                return;
            }
        };
        tail.request.source_offset_overrides = source_offsets;

        let prepared = Self::prepare_follower_tail_until(&mut tail, deadline).await;
        // Decision observation deliberately happens outside the coordinator mutex so a successor
        // epoch can prepare without queuing behind this attempt's control-plane wait.
        let outcome = Self::apply_follower_decision_until(&tail, prepared, deadline).await;
        Self::complete_follower_tail(tail, outcome, deadline).await;
    }

    #[cfg(feature = "cluster")]
    async fn fail_follower_tail_before_prepare(
        tail: &FollowerDurableTail,
        error: String,
        deadline: tokio::time::Instant,
    ) {
        let rejected = Self::reject_follower_capture(
            tail.controller.as_deref(),
            tail.checkpoint_fault.as_ref(),
            tail.attempt,
            Some(tail.assignment_fence.digest()),
            error,
            deadline,
        )
        .await;
        if rejected.is_ok() {
            if let Err(finish_error) = tail.state.finish(&tail.identity, false) {
                set_checkpoint_fault(&tail.checkpoint_fault, finish_error);
            }
        }
        tail.delta_rebase_needed
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(feature = "cluster")]
    async fn acknowledge_follower_capture(
        tail: &FollowerDurableTail,
        deadline: tokio::time::Instant,
    ) {
        let Some(controller) = tail.controller.as_ref() else {
            return;
        };
        let _ = tokio::time::timeout_at(
            deadline,
            controller.ack_barrier(&laminar_core::cluster::control::BarrierAck {
                epoch: tail.attempt.epoch,
                checkpoint_id: tail.attempt.checkpoint_id,
                assignment_digest: Some(tail.assignment_fence.digest()),
                ok: true,
                error: None,
                watermark: tail.local_watermark,
            }),
        )
        .await;
    }

    #[cfg(feature = "cluster")]
    async fn prepare_follower_tail_until(
        tail: &mut FollowerDurableTail,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let request = std::mem::take(&mut tail.request);
        let vnode_states = std::mem::take(&mut tail.vnode_states);
        let attempt = tail.attempt;
        let result = tokio::time::timeout_at(deadline, async {
            let mut guard = tail.coordinator.lock().await;
            let coordinator = guard.as_mut().ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6045] checkpoint coordinator disappeared before follower prepare".into(),
                )
            })?;
            coordinator.set_pending_vnode_states(vnode_states);
            coordinator.set_local_watermark(tail.local_watermark);
            coordinator
                .follower_prepare_acked_until(
                    request,
                    tail.identity.leader_proof.clone(),
                    attempt.epoch,
                    attempt.checkpoint_id,
                    deadline,
                )
                .await?;
            Ok::<_, DbError>(())
        })
        .await;

        result.unwrap_or_else(|_| {
            Err(DbError::Checkpoint(format!(
                "[LDB-6046] follower checkpoint {} epoch {} exceeded its {:?} end-to-end \
                 deadline during prepare",
                attempt.checkpoint_id, attempt.epoch, tail.checkpoint_timeout
            )))
        })
    }

    #[cfg(feature = "cluster")]
    async fn apply_follower_decision_until(
        tail: &FollowerDurableTail,
        prepared: Result<(), DbError>,
        deadline: tokio::time::Instant,
    ) -> Result<bool, DbError> {
        use crate::checkpoint_coordinator::CheckpointCoordinator;

        prepared?;
        let controller = tail.controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6045] follower durable tail lost its decision dependencies".into(),
            )
        })?;
        let attempt = tail.attempt;
        let decision_timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        let verdict = CheckpointCoordinator::await_follower_decision(
            controller,
            attempt.epoch,
            attempt.checkpoint_id,
            &tail.assignment_fence,
            decision_timeout,
        )
        .await?;

        tokio::time::timeout_at(deadline, async {
            let mut guard = tail.coordinator.lock().await;
            let coordinator = guard.as_mut().ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6045] checkpoint coordinator disappeared while a follower decision \
                     was pending"
                        .into(),
                )
            })?;
            coordinator
                .follower_finish(attempt.epoch, attempt.checkpoint_id, verdict)
                .await
        })
        .await
        .unwrap_or_else(|_| {
            Err(DbError::Checkpoint(format!(
                "[LDB-6046] follower checkpoint {} epoch {} exceeded its {:?} end-to-end \
                 deadline while applying the decision",
                attempt.checkpoint_id, attempt.epoch, tail.checkpoint_timeout
            )))
        })
    }

    #[cfg(feature = "cluster")]
    async fn complete_follower_tail(
        mut tail: FollowerDurableTail,
        outcome: Result<bool, DbError>,
        deadline: tokio::time::Instant,
    ) {
        let attempt = tail.attempt;
        let committed = match tail.state.finish_resolved(&tail.identity, &outcome) {
            Ok(Some(committed)) => committed,
            Ok(None) => {
                let Err(error) = outcome else {
                    set_checkpoint_fault(
                        &tail.checkpoint_fault,
                        "follower terminal bookkeeping lost an authoritative outcome",
                    );
                    return;
                };
                tracing::error!(
                    epoch = attempt.epoch,
                    checkpoint_id = attempt.checkpoint_id,
                    %error,
                    "follower checkpoint is in-doubt; faulting pipeline",
                );
                set_checkpoint_fault(&tail.checkpoint_fault, error.to_string());
                return;
            }
            Err(error) => {
                set_checkpoint_fault(&tail.checkpoint_fault, error);
                return;
            }
        };
        if committed {
            if let Some(guard) = tail.mutable_operator_capture_guard.as_mut() {
                guard.disarm();
            }
            tracing::info!(epoch = attempt.epoch, "follower checkpoint committed");
        } else {
            tracing::warn!(
                epoch = attempt.epoch,
                "follower checkpoint aborted by leader"
            );
        }
        let completion = if committed {
            CheckpointCompletion::new(attempt, tail.fan_out)
        } else {
            CheckpointCompletion::failed(attempt, "checkpoint aborted by the cluster leader")
        };
        if !deliver_checkpoint_completion(&tail.complete_tx, completion, deadline).await {
            set_checkpoint_fault(
                &tail.checkpoint_fault,
                format!(
                    "follower checkpoint {} epoch {} reached an authoritative terminal outcome \
                     but its completion could not be delivered before the end-to-end deadline",
                    attempt.checkpoint_id, attempt.epoch
                ),
            );
        }
        if !committed {
            // Follower capture is destructive; every uncommitted outcome must re-base FULL next.
            tail.delta_rebase_needed
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Reject a follower capture or pre-prepare step under the attempt deadline.
    ///
    /// When capture readiness was already acknowledged, the negative acknowledgement retracts it.
    /// If quorum raced ahead, the exact-attempt restorable gate still refuses to seal without this
    /// participant's partial.
    #[cfg(feature = "cluster")]
    async fn reject_follower_capture(
        controller: Option<&laminar_core::cluster::control::ClusterController>,
        checkpoint_fault: &parking_lot::Mutex<Option<String>>,
        attempt: CheckpointAttempt,
        assignment_digest: Option<[u8; 32]>,
        error: String,
        deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        tracing::warn!(
            epoch = attempt.epoch,
            checkpoint_id = attempt.checkpoint_id,
            error = %error,
            "follower checkpoint capture failed; rejecting attempt"
        );
        let Some(controller) = controller else {
            let error = format!(
                "checkpoint {} epoch {} capture failed without a cluster controller to \
                 publish its negative acknowledgement",
                attempt.checkpoint_id, attempt.epoch
            );
            set_checkpoint_fault(checkpoint_fault, error.clone());
            return Err(error);
        };
        let rejection = laminar_core::cluster::control::BarrierAck {
            epoch: attempt.epoch,
            checkpoint_id: attempt.checkpoint_id,
            assignment_digest,
            ok: false,
            error: Some(error),
            watermark: CheckpointWatermark::Uninitialized,
        };
        let acknowledgement = controller.ack_barrier(&rejection);
        match tokio::time::timeout_at(deadline, acknowledgement).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(ack_error)) => {
                let error = format!(
                    "checkpoint {} epoch {} capture failed and its negative acknowledgement \
                     could not be published: {ack_error}",
                    attempt.checkpoint_id, attempt.epoch
                );
                set_checkpoint_fault(checkpoint_fault, error.clone());
                Err(error)
            }
            Err(_) => {
                let error = format!(
                    "checkpoint {} epoch {} capture failed and its negative acknowledgement \
                     missed the end-to-end attempt deadline",
                    attempt.checkpoint_id, attempt.epoch
                );
                set_checkpoint_fault(checkpoint_fault, error.clone());
                Err(error)
            }
        }
    }

    /// Hold the pipeline until the leader announces `Aligned` (or `Commit`/`Abort`/newer epoch).
    ///
    /// Prevents epoch-N+1 shuffle rows from reaching a peer still snapshotting epoch-N.
    /// No-op without a cross-node shuffle; bounded — on timeout the epoch aborts via the leader.
    #[cfg(feature = "cluster")]
    async fn wait_for_aligned_resume(
        has_cluster_shuffle: bool,
        controller: &laminar_core::cluster::control::ClusterController,
        identity: CertifiedCheckpointAttempt,
        assignment_fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
        quorum_timeout: std::time::Duration,
    ) -> Result<(), String> {
        use laminar_core::cluster::control::Phase;

        // The gate must outlast the leader's quorum wait: a slow-but-successful alignment that lands
        // `Aligned` AFTER the follower resumes would let epoch-N+1 shuffle rows cross a peer's
        // closed epoch-N channel while that peer is still capturing. Derive the gate from
        // quorum_timeout (default 3s → 10s) so a
        // user-raised quorum_timeout can never invert the gate > quorum relation (CL-6).
        let resume_gate_timeout = std::time::Duration::from_secs(10)
            .max(quorum_timeout + std::time::Duration::from_secs(5));

        if !has_cluster_shuffle {
            return Ok(());
        }
        if !assignment_fence.is_canonical()
            || assignment_fence.digest() != identity.assignment_digest
        {
            return Err(format!(
                "[LDB-6055] refusing shuffle resume for checkpoint {} epoch {} with a mismatched \
                 assignment certificate",
                identity.attempt.checkpoint_id, identity.attempt.epoch
            ));
        }
        let released = controller
            .wait_for_barrier(
                |a| {
                    let candidate = CheckpointAttempt::new(a.epoch, a.checkpoint_id);
                    match candidate.relation_to(identity.attempt) {
                        CheckpointAttemptRelation::Newer => true,
                        CheckpointAttemptRelation::Exact => {
                            a.assignment_fence.as_ref() == Some(assignment_fence)
                                && match a.phase {
                                    Phase::Aligned => {
                                        a.leader_proof.as_ref() == Some(&identity.leader_proof)
                                    }
                                    // Terminal records are only wake-up hints; durable outcome
                                    // validation owns their authority.
                                    Phase::Commit | Phase::Abort => true,
                                    Phase::Prepare => false,
                                }
                        }
                        CheckpointAttemptRelation::Older | CheckpointAttemptRelation::Conflict => {
                            false
                        }
                    }
                },
                resume_gate_timeout,
            )
            .await
            .map_err(|error| {
                format!(
                    "aligned resume gate observation failed for checkpoint {} epoch {}; pipeline \
                     remains fenced: {error}",
                    identity.attempt.checkpoint_id, identity.attempt.epoch
                )
            })?;
        let Some(released) = released else {
            return Err(format!(
                "aligned resume gate timed out for checkpoint {} epoch {}; pipeline remains fenced",
                identity.attempt.checkpoint_id, identity.attempt.epoch
            ));
        };

        // Observation is side-effect free. Validate the exact attempt and certificate before the
        // controller sees the phase; only Commit advances its recovery-safe watermark. A newer
        // epoch may supersede this gate, but its watermark belongs to its own validation path.
        if released.epoch == identity.attempt.epoch
            && released.checkpoint_id == identity.attempt.checkpoint_id
            && matches!(released.phase, Phase::Aligned | Phase::Commit)
            && !controller.accept_barrier_watermark(
                &released,
                identity.attempt.epoch,
                identity.attempt.checkpoint_id,
                assignment_fence,
            )
        {
            return Err(format!(
                "[LDB-6055] exact shuffle resume for checkpoint {} epoch {} failed watermark \
                 certification",
                identity.attempt.checkpoint_id, identity.attempt.epoch
            ));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    async fn wait_for_aligned_resume_until(
        has_shuffle: bool,
        controller: &laminar_core::cluster::control::ClusterController,
        identity: CertifiedCheckpointAttempt,
        assignment_fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
        resume_gate_timeout: std::time::Duration,
        attempt_deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        let attempt = identity.attempt;
        tokio::time::timeout_at(
            attempt_deadline,
            Self::wait_for_aligned_resume(
                has_shuffle,
                controller,
                identity,
                assignment_fence,
                resume_gate_timeout,
            ),
        )
        .await
        .map_err(|_| {
            format!(
                "checkpoint {} epoch {} exhausted its end-to-end deadline while waiting for aligned resume",
                attempt.checkpoint_id, attempt.epoch
            )
        })?
    }

    /// Observe and inject a leader `Prepare`, returning only attempts ready for immediate capture.
    #[cfg(feature = "cluster")]
    async fn admit_follower_prepare(
        &mut self,
        controller: &laminar_core::cluster::control::ClusterController,
    ) -> FollowerPrepareAdmission {
        use laminar_core::cluster::control::Phase;

        let announcement = match controller.observe_barrier().await {
            Ok(Some(announcement)) if announcement.phase == Phase::Prepare => announcement,
            _ => return FollowerPrepareAdmission::Idle,
        };
        let attempt = CheckpointAttempt::new(announcement.epoch, announcement.checkpoint_id);
        if let Some(error) = self
            .follower_prepare_assignment_error(controller, &announcement)
            .await
        {
            Self::reject_uncertified_follower_prepare(controller, &announcement, error.clone())
                .await;
            return FollowerPrepareAdmission::Failed { attempt, error };
        }
        let Some(announced_identity) = Self::certified_announcement(&announcement) else {
            return FollowerPrepareAdmission::Failed {
                attempt,
                error: "certified follower Prepare lost its exact identity".into(),
            };
        };
        if self
            .barrier_injectors
            .iter()
            .any(|control| !control.can_trigger())
        {
            tracing::debug!(
                checkpoint_id = announcement.checkpoint_id,
                epoch = announcement.epoch,
                "follower barrier injection deferred while a prior command is pending"
            );
            return FollowerPrepareAdmission::Idle;
        }
        match self
            .reserve_follower_prepare(controller, &announcement, announced_identity.clone())
            .await
        {
            Ok(FollowerAdmission::Reserved) => {}
            Ok(FollowerAdmission::Covered) => return FollowerPrepareAdmission::Idle,
            Err(error) => return FollowerPrepareAdmission::Failed { attempt, error },
        }
        self.pending_follower_checkpoint = Some(announcement.clone());
        if let Err(error) = self.inject_follower_prepare_barriers(&announcement) {
            let cleanup =
                <Self as crate::pipeline::PipelineCallback>::cancel_source_barrier_attempt(
                    self, attempt, &error,
                )
                .await;
            if cleanup.is_ok() {
                for control in &self.barrier_injectors {
                    control.release_exact(attempt);
                }
            }
            let error = match cleanup {
                Ok(()) => error,
                Err(cleanup) => format!("{error}; follower cleanup failed: {cleanup}"),
            };
            return FollowerPrepareAdmission::Failed { attempt, error };
        }

        if self.barrier_injectors.is_empty() {
            return FollowerPrepareAdmission::CaptureNow(announcement);
        }
        tracing::info!(
            checkpoint_id = announcement.checkpoint_id,
            epoch = announcement.epoch,
            "follower deferring checkpoint alignment until source barriers flow through"
        );
        FollowerPrepareAdmission::Started(attempt)
    }

    #[cfg(feature = "cluster")]
    async fn follower_prepare_assignment_error(
        &mut self,
        controller: &laminar_core::cluster::control::ClusterController,
        announcement: &laminar_core::cluster::control::BarrierAnnouncement,
    ) -> Option<String> {
        let leader_proof = match announcement.leader_proof.as_ref() {
            None => return Some("leader Prepare omitted its durable authority proof".into()),
            Some(proof) if !proof.is_canonical() => {
                return Some("leader Prepare carried a non-canonical authority proof".into());
            }
            Some(proof) => proof,
        };
        let Some(fence) = announcement.assignment_fence.as_ref() else {
            return Some("[LDB-6055] leader Prepare omitted its assignment certificate".into());
        };
        if !fence.is_canonical() {
            return Some(
                "[LDB-6055] leader Prepare carried a non-canonical assignment certificate".into(),
            );
        }
        if self
            .vnode_registry
            .as_ref()
            .is_none_or(|registry| registry.assignment_version() != fence.assignment_version)
        {
            return Some(format!(
                "[LDB-6055] follower assignment does not match leader Prepare version {}",
                fence.assignment_version
            ));
        }
        if self.vnode_registry.as_ref().is_some_and(|registry| {
            let publication = registry.versioned_snapshot();
            publication.version() == fence.assignment_version
                && publication
                    .source_handoff_installed_version()
                    .is_some_and(|installed| {
                        self.reconciled_source_handoff_version
                            .is_none_or(|reconciled| reconciled < installed)
                    })
        }) {
            return Some(format!(
                "[LDB-6055] follower assignment {} source handoff is not installed",
                fence.assignment_version
            ));
        }
        if !fence.contains(controller.instance_id().0) {
            return Some(
                "[LDB-6055] follower is absent from the certified checkpoint roster".into(),
            );
        }
        if fence.participant_incarnation(leader_proof.owner.node_id)
            != Some(leader_proof.owner.boot_id)
        {
            return Some(
                "[LDB-6055] leader authority owner does not match the checkpoint roster".into(),
            );
        }
        if controller
            .checkpoint_assignment_fence_for_leader(fence.assignment_version, leader_proof)
            .await
            .as_ref()
            != Some(fence)
        {
            return Some(format!(
                "[LDB-6055] follower cannot certify leader Prepare assignment {}",
                fence.assignment_version
            ));
        }
        None
    }

    #[cfg(feature = "cluster")]
    async fn reject_uncertified_follower_prepare(
        controller: &laminar_core::cluster::control::ClusterController,
        announcement: &laminar_core::cluster::control::BarrierAnnouncement,
        error: String,
    ) {
        let _ = controller
            .ack_barrier(&laminar_core::cluster::control::BarrierAck {
                epoch: announcement.epoch,
                checkpoint_id: announcement.checkpoint_id,
                assignment_digest: announcement
                    .assignment_fence
                    .as_ref()
                    .map(laminar_core::cluster::control::CheckpointAssignmentFence::digest),
                ok: false,
                error: Some(error.clone()),
                watermark: CheckpointWatermark::Uninitialized,
            })
            .await;
        tracing::warn!(%error, "rejecting follower Prepare before barrier injection");
    }

    #[cfg(feature = "cluster")]
    async fn reserve_follower_prepare(
        &mut self,
        controller: &laminar_core::cluster::control::ClusterController,
        announcement: &laminar_core::cluster::control::BarrierAnnouncement,
        announced_identity: CertifiedCheckpointAttempt,
    ) -> Result<FollowerAdmission, String> {
        match self.follower_tail.reserve(announced_identity.clone()) {
            Ok(admission) => Ok(admission),
            Err(error) => {
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                let _ = controller
                    .ack_barrier(&laminar_core::cluster::control::BarrierAck {
                        epoch: announcement.epoch,
                        checkpoint_id: announcement.checkpoint_id,
                        assignment_digest: Some(announced_identity.assignment_digest),
                        ok: false,
                        error: Some(error.clone()),
                        watermark: CheckpointWatermark::Uninitialized,
                    })
                    .await;
                tracing::error!(%error, "rejecting equivocal follower Prepare");
                Err(error)
            }
        }
    }

    #[cfg(feature = "cluster")]
    fn inject_follower_prepare_barriers(
        &mut self,
        announcement: &laminar_core::cluster::control::BarrierAnnouncement,
    ) -> Result<(), String> {
        let barrier = laminar_core::checkpoint::CheckpointBarrier {
            checkpoint_id: announcement.checkpoint_id,
            epoch: announcement.epoch,
            flags: announcement.flags,
        };
        for (source_idx, control) in self.barrier_injectors.iter().enumerate() {
            tracing::debug!(
                source_idx,
                checkpoint_id = announcement.checkpoint_id,
                "follower injecting checkpoint barrier"
            );
            if !control.trigger(barrier) {
                let error = format!(
                    "follower rejected source barrier for checkpoint {} epoch {}",
                    announcement.checkpoint_id, announcement.epoch
                );
                return Err(error);
            }
        }
        Ok(())
    }

    /// Align the follower's shuffle contribution under the attempt's absolute deadline.
    #[cfg(feature = "cluster")]
    async fn align_follower_shuffle_until(
        &mut self,
        controller: &laminar_core::cluster::control::ClusterController,
        attempt: CheckpointAttempt,
        assignment_fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let watermark = self.effective_pipeline_watermark();
        self.graph
            .align_shuffle_barriers(
                attempt,
                watermark,
                assignment_fence,
                deadline,
                Some(controller),
            )
            .await
    }

    #[cfg(feature = "cluster")]
    async fn fence_follower_sinks_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        let result =
            match tokio::time::timeout_at(deadline, self.sync_sinks_and_drain_events(deadline))
                .await
            {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(format!("follower sink fence failed: {error}")),
                Err(_) => {
                    Err("follower sink fence exhausted the end-to-end checkpoint deadline".into())
                }
            };
        if let Err(error) = result {
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            return Err(error);
        }
        if let Some(error) = self.sink_fault.clone() {
            return Err(error);
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    async fn fail_pending_follower_control(
        &mut self,
        attempt: CheckpointAttempt,
        error: String,
    ) -> crate::pipeline::CheckpointControlOutcome {
        let cleanup = <Self as crate::pipeline::PipelineCallback>::cancel_source_barrier_attempt(
            self, attempt, &error,
        )
        .await;
        let error = match cleanup {
            Ok(()) => error,
            Err(cleanup) => format!("{error}; follower cleanup failed: {cleanup}"),
        };
        crate::pipeline::CheckpointControlOutcome::Failed { attempt, error }
    }

    #[cfg(feature = "cluster")]
    fn clear_pending_follower_checkpoint(
        &mut self,
        attempt: CheckpointAttempt,
    ) -> Result<(), String> {
        if self
            .pending_follower_checkpoint
            .as_ref()
            .is_some_and(|pending| {
                pending.epoch == attempt.epoch && pending.checkpoint_id == attempt.checkpoint_id
            })
        {
            self.pending_follower_checkpoint = None;
            Ok(())
        } else {
            Err(format!(
                "follower checkpoint {} epoch {} lost its pending control identity",
                attempt.checkpoint_id, attempt.epoch
            ))
        }
    }

    #[cfg(feature = "cluster")]
    async fn maybe_follower_checkpoint(
        &mut self,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
        source_offsets: FxHashMap<String, SourceCheckpoint>,
    ) -> crate::pipeline::CheckpointControlOutcome {
        use crate::pipeline::CheckpointControlOutcome;

        let ann = match self.admit_follower_prepare(&controller).await {
            FollowerPrepareAdmission::Idle => return CheckpointControlOutcome::Idle,
            FollowerPrepareAdmission::Started(attempt) => {
                return CheckpointControlOutcome::Started {
                    attempt,
                    captured: false,
                };
            }
            FollowerPrepareAdmission::Failed { attempt, error } => {
                return CheckpointControlOutcome::Failed { attempt, error };
            }
            FollowerPrepareAdmission::CaptureNow(announcement) => announcement,
        };
        let attempt = CheckpointAttempt::new(ann.epoch, ann.checkpoint_id);
        let Some(identity) = Self::certified_announcement(&ann) else {
            return self
                .fail_pending_follower_control(
                    attempt,
                    "admitted follower checkpoint lost its exact identity".into(),
                )
                .await;
        };

        // The histogram timer records on drop, including every early-return failure below.
        // Alignment and capture stop the pipeline just as surely as the durable tail does.
        let attempt_started = controller
            .checkpoint_prepare_received_at(&ann)
            .unwrap_or_else(std::time::Instant::now);
        let attempt_deadline =
            tokio::time::Instant::from_std(attempt_started) + self.checkpoint_timeout;
        let _stall_timer = self.prom.checkpoint_pipeline_stall_duration.start_timer();
        let Some(assignment_fence) = ann.assignment_fence.as_ref() else {
            let error = "admitted follower checkpoint lost its assignment certificate";
            tracing::error!(error);
            return self
                .fail_pending_follower_control(attempt, error.into())
                .await;
        };
        if let Err(error) = self.validate_checkpoint_assignment(Some(assignment_fence)) {
            return self.fail_pending_follower_control(attempt, error).await;
        }
        if let Err(error) = self
            .drain_checkpoint_edges_until_inner(attempt_deadline)
            .await
        {
            return self
                .fail_pending_follower_control(attempt, error.to_string())
                .await;
        }
        if let Err(error) = self.fence_follower_sinks_until(attempt_deadline).await {
            return self.fail_pending_follower_control(attempt, error).await;
        }

        if let Err(error) = self
            .align_follower_shuffle_until(&controller, attempt, assignment_fence, attempt_deadline)
            .await
        {
            tracing::warn!(%error, "follower shuffle alignment failed — skipping");
            let error = error.to_string();
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            return self.fail_pending_follower_control(attempt, error).await;
        }

        // Alignment above is where a peer's barrier reveals trailing loss; capturing now would
        // hand the leader a gapped snapshot to seal. Leave the flag for `take_pipeline_fault`.
        self.check_shuffle_delivery_loss();
        if self.sink_fault.is_some() {
            let error = self
                .sink_fault
                .clone()
                .unwrap_or_else(|| "follower shuffle loss before capture".into());
            tracing::warn!("follower: shuffle loss before capture; failing the epoch for replay");
            return self.fail_pending_follower_control(attempt, error).await;
        }

        // Vnode partials cover only explicitly sharded operators. The common builder captures
        // aggregate channel replay and every non-vnode operator for both follower entry paths.
        let (request, operator_state) = match self
            .build_follower_checkpoint_request_until(assignment_fence, attempt_deadline)
            .await
        {
            Ok(request) => request,
            Err(error) => {
                return self.fail_pending_follower_control(attempt, error).await;
            }
        };

        let has_shuffle = self.graph.cluster_shuffle_config().is_some();
        let tail = match self.follower_tail_future(
            request,
            operator_state,
            identity.clone(),
            source_offsets,
            attempt_started,
        ) {
            Ok(tail) => tail,
            Err(error) => {
                return self.fail_pending_follower_control(attempt, error).await;
            }
        };
        if let Err(error) = self.clear_pending_follower_checkpoint(attempt) {
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            return CheckpointControlOutcome::Failed { attempt, error };
        }
        if self.checkpoint_committable_sinks {
            tail.await;
        } else {
            self.spawn_checkpoint_tail(tail);
            if let Err(error) = Self::wait_for_aligned_resume_until(
                has_shuffle,
                &controller,
                identity,
                assignment_fence,
                self.quorum_timeout,
                attempt_deadline,
            )
            .await
            {
                set_checkpoint_fault(&self.checkpoint_fault, error);
            }
        }
        CheckpointControlOutcome::Started {
            attempt,
            captured: true,
        }
    }

    #[cfg(feature = "cluster")]
    async fn run_follower_checkpoint_deferred(
        &mut self,
        ann: laminar_core::cluster::control::BarrierAnnouncement,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
        attempt_started: std::time::Instant,
    ) -> crate::pipeline::BarrierOutcome {
        use crate::pipeline::BarrierOutcome;

        let Some(identity) = Self::certified_announcement(&ann) else {
            return BarrierOutcome::Failed;
        };
        let attempt = identity.attempt;
        let Some(controller) = self.cluster_controller.clone() else {
            return BarrierOutcome::Failed;
        };
        let attempt_started = controller
            .checkpoint_prepare_received_at(&ann)
            .map_or(attempt_started, |received_at| {
                received_at.min(attempt_started)
            });

        // Record the complete pipeline pause. The timer observes on drop so alignment or
        // serialization failures are visible instead of disappearing from latency telemetry.
        let attempt_deadline =
            tokio::time::Instant::from_std(attempt_started) + self.checkpoint_timeout;
        let _stall_timer = self.prom.checkpoint_pipeline_stall_duration.start_timer();
        let Some(assignment_fence) = ann.assignment_fence.as_ref() else {
            tracing::warn!("follower deferred checkpoint lost its assignment certificate");
            return BarrierOutcome::Failed;
        };
        if let Err(error) = self.fence_follower_sinks_until(attempt_deadline).await {
            tracing::warn!(%error, "follower deferred checkpoint sink fence failed");
            return BarrierOutcome::Failed;
        }
        if !self
            .deferred_follower_capture_is_aligned(
                &controller,
                attempt,
                assignment_fence,
                attempt_deadline,
            )
            .await
        {
            return BarrierOutcome::Failed;
        }

        let (request, operator_state) = match self
            .build_follower_checkpoint_request_until(assignment_fence, attempt_deadline)
            .await
        {
            Ok(request) => request,
            Err(error) => {
                tracing::warn!(%error, "follower deferred checkpoint state capture failed");
                return BarrierOutcome::Failed;
            }
        };

        let has_shuffle = self.graph.cluster_shuffle_config().is_some();
        let tail = match self.follower_tail_future(
            request,
            operator_state,
            identity.clone(),
            source_checkpoints,
            attempt_started,
        ) {
            Ok(tail) => tail,
            Err(error) => {
                tracing::warn!(%error, "follower deferred checkpoint tail construction failed");
                return BarrierOutcome::Failed;
            }
        };
        if self.checkpoint_committable_sinks {
            tail.await;
        } else {
            self.spawn_checkpoint_tail(tail);
            if let Err(error) = Self::wait_for_aligned_resume_until(
                has_shuffle,
                &controller,
                identity,
                assignment_fence,
                self.quorum_timeout,
                attempt_deadline,
            )
            .await
            {
                set_checkpoint_fault(&self.checkpoint_fault, error);
            }
        }
        BarrierOutcome::Async
    }

    #[cfg(feature = "cluster")]
    async fn deferred_follower_capture_is_aligned(
        &mut self,
        controller: &laminar_core::cluster::control::ClusterController,
        attempt: CheckpointAttempt,
        assignment_fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
        deadline: tokio::time::Instant,
    ) -> bool {
        // Capture only after peers' pre-checkpoint rows have crossed the shuffle barrier.
        if let Err(error) = self
            .align_follower_shuffle_until(controller, attempt, assignment_fence, deadline)
            .await
        {
            tracing::warn!(%error, "follower shuffle alignment failed — skipping");
            set_checkpoint_fault(&self.checkpoint_fault, error.to_string());
            return false;
        }

        // A trailing shuffle gap discovered during alignment must not be sealed cluster-wide.
        self.check_shuffle_delivery_loss();
        if self.sink_fault.is_some() {
            tracing::warn!("follower: shuffle loss before capture; failing the epoch for replay");
            return false;
        }
        true
    }

    #[cfg(feature = "cluster")]
    async fn build_follower_checkpoint_request_until(
        &mut self,
        assignment_fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
        deadline: tokio::time::Instant,
    ) -> Result<
        (
            crate::checkpoint_coordinator::CheckpointRequest,
            CapturedOperatorState,
        ),
        String,
    > {
        if tokio::time::Instant::now() >= deadline {
            return Err(
                "follower operator-state capture exhausted the checkpoint deadline".to_string(),
            );
        }
        let operator_state =
            tokio::time::timeout_at(deadline, self.capture_operator_state_until(deadline))
                .await
                .map_err(|_| {
                    "follower operator-state capture exhausted the checkpoint deadline".to_string()
                })??;
        let mut request = self.build_checkpoint_request();
        request.assignment_fence = Some(assignment_fence.clone());
        Ok((request, operator_state))
    }

    /// Reserve one durable attempt before any source or shuffle barrier is admitted.
    async fn reserve_attempt(
        &mut self,
        attempt_started: std::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let allocator = self.epoch_allocator.clone().ok_or_else(|| {
            DbError::Checkpoint("checkpoint attempt allocator is not initialized".into())
        })?;
        let deadline = tokio::time::Instant::from_std(attempt_started) + self.checkpoint_timeout;

        #[cfg(feature = "cluster")]
        if let Some(cc) = self.cluster_controller.clone() {
            if !cc.is_leader() {
                return Err(DbError::Checkpoint(
                    "only the cluster leader may reserve checkpoint attempts".into(),
                ));
            }
            // A reclaiming leader can lag an in-flight announced epoch. Checkpoint IDs come from
            // the shared durable allocator; only the execution epoch needs a local high-watermark.
            let max_announced = tokio::time::timeout_at(deadline, cc.max_announced_epoch())
                .await
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "checkpoint admission exhausted its {:?} end-to-end deadline while \
                         reading the cluster epoch high-watermark",
                        self.checkpoint_timeout
                    ))
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "cannot reserve a checkpoint from conflicting cluster announcement \
                         history: {error}"
                    ))
                })?;
            if let Some(max_announced) = max_announced {
                allocator.advance_past(
                    max_announced.epoch,
                    "advancing past the cluster announcement high-watermark",
                )?;
            }
        }

        allocator.allocate_until(deadline).await
    }

    async fn abandon_reserved_attempt(
        &mut self,
        attempt: CheckpointAttempt,
        reason: String,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) -> Result<(), String> {
        #[cfg(feature = "cluster")]
        let leader_proof = self.checkpoint_leader_proofs.remove(&attempt);
        #[cfg(not(feature = "cluster"))]
        let leader_proof = None;
        let deadline = tokio::time::Instant::now() + self.checkpoint_cleanup_timeout;
        let mut cleanup_errors = Vec::new();
        let cleanup_result = cleanup_reserved_attempt_until(
            self.coordinator.as_ref(),
            attempt,
            reason,
            assignment_fence,
            leader_proof,
            deadline,
        );
        let cleanup_result = cleanup_result.await;
        if let Err(error) = cleanup_result {
            cleanup_errors.push(error);
        }
        if cleanup_errors.is_empty() {
            Ok(())
        } else {
            let error = format!(
                "checkpoint {} epoch {} abandonment incomplete: {}",
                attempt.checkpoint_id,
                attempt.epoch,
                cleanup_errors.join("; ")
            );
            tracing::error!(%error, "checkpoint cleanup faulted the pipeline");
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            Err(error)
        }
    }

    /// Align the cross-node shuffle for an already announced exact attempt.
    #[cfg(feature = "cluster")]
    fn validate_checkpoint_assignment(
        &self,
        assignment_fence: Option<&laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) -> Result<Option<laminar_core::cluster::control::CheckpointAssignmentFence>, String> {
        match (self.cluster_controller.as_ref(), assignment_fence) {
            (None, None) => Ok(None),
            (None, Some(_)) => {
                Err("cluster assignment certificate supplied to a local checkpoint runtime".into())
            }
            (Some(_), None) => {
                Err("[LDB-6056] clustered checkpoint has no assignment certificate".into())
            }
            (Some(controller), Some(admitted)) => {
                let registry = self.vnode_registry.as_ref().ok_or_else(|| {
                    "[LDB-6056] clustered checkpoint has no vnode registry".to_string()
                })?;
                if registry.assignment_version() != admitted.assignment_version {
                    return Err(format!(
                        "[LDB-6055] checkpoint assignment changed after admission: admitted {}, current {}",
                        admitted.assignment_version,
                        registry.assignment_version()
                    ));
                }
                let publication = registry.versioned_snapshot();
                if publication
                    .source_handoff_installed_version()
                    .is_some_and(|installed| {
                        self.reconciled_source_handoff_version
                            .is_none_or(|reconciled| reconciled < installed)
                    })
                {
                    return Err(format!(
                        "[LDB-6055] assignment {} source handoff was not installed before checkpoint capture",
                        admitted.assignment_version
                    ));
                }
                let current = controller
                    .checkpoint_assignment_fence(admitted.assignment_version)
                    .ok_or_else(|| {
                        format!(
                            "[LDB-6056] assignment {} is no longer checkpoint-ready",
                            admitted.assignment_version
                        )
                    })?;
                if current != *admitted {
                    return Err(format!(
                        "[LDB-6055] checkpoint participant roster changed after assignment {} admission",
                        admitted.assignment_version
                    ));
                }
                Ok(Some(admitted.clone()))
            }
        }
    }

    #[cfg(feature = "cluster")]
    async fn align_shuffle_for_leader(
        &mut self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&laminar_core::cluster::control::CheckpointAssignmentFence>,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        // A binary compiled with cluster support may still run embedded or single-node without a
        // controller. Those runtimes have no cross-node shuffle to align.
        let Some(cc) = self.cluster_controller.clone() else {
            return Ok(());
        };
        if !cc.is_leader() {
            return Err(DbError::Checkpoint(
                "only the cluster leader may align checkpoint shuffles".into(),
            ));
        }
        let assignment_fence = assignment_fence.ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6055] clustered shuffle alignment lost its assignment certificate".into(),
            )
        })?;
        let wm = self.effective_pipeline_watermark();
        self.graph
            .align_shuffle_barriers(attempt, wm, assignment_fence, deadline, Some(&*cc))
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "shuffle alignment failed for checkpoint {} epoch {}: {error}",
                    attempt.checkpoint_id, attempt.epoch
                ))
            })
    }

    /// Build the callback-owned portion of a `CheckpointRequest`.
    ///
    /// Source offset overrides remain empty here. The immutable source snapshot is already owned
    /// by the durable tail, which materializes it on a blocking worker after the pipeline resumes.
    fn build_checkpoint_request(&self) -> crate::checkpoint_coordinator::CheckpointRequest {
        let recovery_watermark_frontier = self.effective_pipeline_watermark();
        let mut per_source_watermarks = HashMap::with_capacity(self.watermark_states.len());
        for (name, wm_state) in &self.watermark_states {
            let wm = wm_state.generator.current_watermark();
            if wm > i64::MIN {
                per_source_watermarks.insert(name.clone(), wm);
            }
        }
        crate::checkpoint_coordinator::CheckpointRequest {
            assignment_fence: None,
            operator_states: std::collections::HashMap::new(),
            // Numeric recovery progress is distinct from Active/Idle status. In particular, an
            // idle cut retains the last initialized frontier instead of erasing it on restart.
            watermark: (recovery_watermark_frontier != i64::MIN)
                .then_some(recovery_watermark_frontier),
            table_store_checkpoint_path: None,
            // Reference-table rows are not yet part of the atomic state snapshot. Persisting a
            // source cursor without those rows can skip the startup snapshot after recovery.
            extra_table_offsets: HashMap::new(),
            source_watermarks: per_source_watermarks,
            source_offset_overrides: HashMap::new(),
        }
    }

    fn checkpoint_watermark(&self) -> CheckpointWatermark {
        let Some(tracker) = self.tracker.as_ref() else {
            return CheckpointWatermark::Idle;
        };
        classify_checkpoint_watermark(
            tracker.num_sources(),
            tracker.active_source_count(),
            self.pipeline_watermark
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    fn fault_mutable_checkpoint_capture(&self, component: &str, error: &str) -> String {
        let reason = mutable_checkpoint_capture_failure(component, error);
        set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
        reason
    }

    async fn capture_operator_state_until(
        &mut self,
        attempt_deadline: tokio::time::Instant,
    ) -> Result<CapturedOperatorState, String> {
        if tokio::time::Instant::now() >= attempt_deadline {
            return Err(
                "[LDB-6017] checkpoint deadline expired before operator-state capture".into(),
            );
        }
        let timeout = self.serialization_timeout;
        let gate_deadline = attempt_deadline.min(tokio::time::Instant::now() + timeout);
        let serialization_permit = tokio::time::timeout_at(
            gate_deadline,
            Arc::clone(&self.checkpoint_serialization_gate).acquire_owned(),
        )
        .await
        .map_err(|_| {
            format!("[LDB-6017] prior checkpoint serialization did not exit within {timeout:?}")
        })?
        .map_err(|_| "checkpoint serialization gate was closed".to_string())?;

        let graph = match self.graph.snapshot_state() {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                let error = format!("snapshot failed: {error}");
                return Err(self.fault_mutable_checkpoint_capture("operator state", &error));
            }
        };
        let mut mutable_capture_guard = graph
            .as_ref()
            .map(|_| MutableCheckpointCaptureGuard::new(Arc::clone(&self.checkpoint_fault)));

        let capture = (|| -> Result<(OperatorStateCapture, u64), DbError> {
            let graph_estimate = graph_checkpoint_capture_estimated_bytes(graph.as_ref())?;
            let mv_budget = self
                .checkpoint_state_cap_bytes
                .checked_sub(graph_estimate)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "operator checkpoint capture estimate {graph_estimate} bytes exceeds the staged-state cap of {} bytes",
                        self.checkpoint_state_cap_bytes
                    ))
                })?;
            let materialized_views = self.mv_store.read().capture_checkpoint(mv_budget)?;
            let table_budget = mv_budget
                .checked_sub(materialized_views.estimated_bytes())
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "MV checkpoint capture estimate exceeded its admitted budget".into(),
                    )
                })?;
            let reference_tables = self.table_store.read().capture_checkpoint(table_budget)?;
            let reference_table_estimate = reference_tables.as_ref().map_or(
                0,
                crate::table_store::ReferenceTableCheckpointCapture::estimated_bytes,
            );
            table_budget
                .checked_sub(reference_table_estimate)
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "reference-table checkpoint capture estimate exceeded its admitted budget"
                            .into(),
                    )
                })?;
            let estimated_bytes = graph_estimate
                .checked_add(materialized_views.estimated_bytes())
                .and_then(|bytes| bytes.checked_add(reference_table_estimate))
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "operator checkpoint capture estimate overflowed u64".into(),
                    )
                })?;
            Ok((
                OperatorStateCapture {
                    graph,
                    materialized_views,
                    reference_tables,
                    serialization_permit,
                },
                estimated_bytes,
            ))
        })();
        let (image, estimated_bytes) = match capture {
            Ok(capture) => capture,
            Err(error) => {
                let error = format!("checkpoint image capture failed: {error}");
                return Err(fail_after_mutable_capture(
                    &mut mutable_capture_guard,
                    error,
                ));
            }
        };
        Ok(CapturedOperatorState {
            image,
            estimated_bytes,
            mutable_capture_guard,
        })
    }

    #[cfg(test)]
    async fn capture_and_serialize_operator_state(
        &mut self,
    ) -> Result<std::collections::HashMap<String, bytes::Bytes>, String> {
        let deadline = tokio::time::Instant::now() + self.serialization_timeout;
        let capture = self.capture_operator_state_until(deadline).await?;
        let encoded_budget = encoded_operator_state_budget(
            self.checkpoint_state_cap_bytes,
            capture.estimated_bytes(),
            0,
        )?;
        capture
            .serialize_until(encoded_budget, self.serialization_timeout, deadline)
            .await
            .map(SerializedOperatorState::accept_for_test)
    }

    /// Capture per-vnode operator state for the in-flight checkpoint.
    ///
    /// Empty outside cluster mode. A cluster capture failure faults the runtime because an empty
    /// map is a valid stateless snapshot and cannot encode partially consumed live state.
    #[allow(clippy::unused_self, clippy::disallowed_types)] // matches the coordinator/graph map shape
    #[cfg_attr(
        not(feature = "cluster"),
        allow(clippy::unnecessary_wraps) // one cross-feature API; cluster capture is fallible
    )]
    fn capture_vnode_states(
        &mut self,
        epoch: u64,
    ) -> Result<crate::checkpoint_coordinator::StagedVnodeStates, String> {
        #[cfg(feature = "cluster")]
        {
            // This proactive re-base avoids sacrificing an attempt after an allocator jump. The
            // coordinator separately requires a sealed, immediately preceding parent before it
            // persists any delta manifest or marker.
            let failed_capture = self
                .delta_rebase_needed
                .swap(false, std::sync::atomic::Ordering::SeqCst);
            if failed_capture
                || vnode_capture_requires_full_rebase(self.last_vnode_capture_epoch, epoch)
            {
                self.graph.force_full_rebase();
            }
            match self.graph.snapshot_state_by_vnode() {
                Ok(states) => {
                    self.last_vnode_capture_epoch = Some(epoch);
                    Ok(states)
                }
                Err(error) => {
                    self.delta_rebase_needed
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    let error = format!("per-vnode state snapshot failed: {error}");
                    Err(self.fault_mutable_checkpoint_capture("per-vnode state", &error))
                }
            }
        }
        #[cfg(not(feature = "cluster"))]
        {
            let _ = epoch;
            Ok(std::collections::HashMap::new())
        }
    }

    /// Sync all sinks and drain their events; `sink_timed_out` is current after this returns.
    /// A failed fence aborts this checkpoint: sealing offsets while queued writes are unknown
    /// would violate both ALO and EO delivery.
    async fn sync_sinks_and_drain_events(
        &mut self,
        attempt_deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        let sync_futures = self.sinks.iter().map(|(name, handle, _, _, _)| {
            let name = name.clone();
            let handle = handle.clone();
            async move { (name, handle.sync_until(attempt_deadline).await) }
        });
        let results = futures::future::join_all(sync_futures).await;
        self.drain_sink_events();
        let failures: Vec<String> = results
            .into_iter()
            .filter_map(|(name, result)| {
                result
                    .err()
                    .map(|error| format!("sink '{name}' sync barrier failed: {error}"))
            })
            .collect();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    /// Cluster pipelines always fault (rather than drop) on fatal errors: coordinated
    /// recovery replays them, and a swallowed fault would desync the cross-node cut.
    #[cfg_attr(not(feature = "cluster"), allow(clippy::unused_self))]
    fn in_cluster(&self) -> bool {
        #[cfg(feature = "cluster")]
        {
            self.cluster_controller.is_some()
        }
        #[cfg(not(feature = "cluster"))]
        {
            false
        }
    }

    /// A shuffle frame a peer sent never arrived, so this node's state is missing records that
    /// only exist upstream. Sealing here would commit the gap permanently — the rewind target
    /// would sit at or above the corrupt epoch. Fault instead: the round rewinds to the last
    /// committed cut and the replay regenerates them (CL-2).
    #[cfg(feature = "cluster")]
    fn check_shuffle_delivery_loss(&mut self) {
        let Some(ref counter) = self.shuffle_delivery_loss_incidents else {
            return;
        };
        let recovered = self
            .shuffle_recovered_delivery_loss_incidents
            .as_ref()
            .map_or(0, |counter| {
                counter.load(std::sync::atomic::Ordering::Acquire)
            });
        // A replacement callback is live while coordinated recovery still holds source intake.
        // The exact Release promotes the captured loss cutoff before clearing this flag. Until
        // then, re-faulting the old cumulative count can keep a successfully restored node in an
        // endless recovery loop; advancing `seen` here would instead hide a failed rewind.
        let recovery_active = self
            .cluster_controller
            .as_ref()
            .is_some_and(|controller| controller.is_recovering());
        let incidents = counter.load(std::sync::atomic::Ordering::Acquire);
        if let Some(incidents) = observe_unrecovered_delivery_loss_incidents(
            incidents,
            recovered,
            &mut self.shuffle_delivery_loss_incidents_seen,
            recovery_active,
        ) {
            self.prom
                .shuffle_delivery_loss_incidents_total
                .inc_by(incidents);
            self.sink_fault.get_or_insert_with(|| {
                format!(
                    "{incidents} shuffle delivery-loss incident(s); replaying from the last checkpoint"
                )
            });
        }
    }

    #[cfg(not(feature = "cluster"))]
    #[allow(clippy::unused_self)]
    fn check_shuffle_delivery_loss(&mut self) {}

    fn record_dropped_sink_write(&mut self, reason: String) {
        let requires_replay = self.checkpoint_committable_sinks
            || self.delivery_guarantee
                != laminar_connectors::connector::DeliveryGuarantee::BestEffort
            || self.in_cluster();
        if requires_replay {
            self.sink_fault.get_or_insert(reason);
        } else {
            self.sink_timed_out = true;
        }
    }

    fn drain_sink_events(&mut self) {
        self.check_shuffle_delivery_loss();
        while let Ok(event) = self.sink_event_rx.try_recv() {
            tracing::debug!(?event, "sink event");
            let reason = match &event {
                crate::sink_task::SinkEvent::FlushError {
                    sink_id,
                    epoch,
                    operation,
                    error,
                } => {
                    self.prom.sink_write_failures.inc();
                    format!("sink '{sink_id}' {operation} failed at epoch {epoch}: {error}")
                }
                crate::sink_task::SinkEvent::WriteError {
                    sink_id,
                    epoch,
                    rows,
                    error,
                } => {
                    self.prom.sink_write_failures.inc();
                    format!(
                        "sink '{sink_id}' write error for {rows} rows at epoch {epoch}: {error}"
                    )
                }
                crate::sink_task::SinkEvent::WriteTimeout {
                    sink_id,
                    epoch,
                    rows,
                    timeout,
                } => {
                    self.prom.sink_write_timeouts.inc();
                    format!(
                        "sink '{sink_id}' write timeout for {rows} rows at epoch {epoch} \
                         after {timeout:?}"
                    )
                }
                crate::sink_task::SinkEvent::WriteEnqueueTimeout {
                    sink_id,
                    rows,
                    timeout,
                } => {
                    self.prom.sink_write_timeouts.inc();
                    format!(
                        "sink '{sink_id}' write enqueue timeout for {rows} rows after {timeout:?}"
                    )
                }
                crate::sink_task::SinkEvent::ChannelClosed { sink_id } => {
                    self.prom.sink_task_channel_closed.inc();
                    format!("sink '{sink_id}' task channel closed")
                }
            };
            // Any replay-guaranteed mode must fault so recovery replays the dropped rows.
            // Best-effort mode may continue, but the handle's sticky poison still prevents a
            // later checkpoint from claiming the dropped batch.
            self.record_dropped_sink_write(reason);
        }
    }

    async fn compile_pending_sink_filters(
        &mut self,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) {
        if self.pending_sink_filter_compiles == 0 {
            return;
        }

        while self.compiled_sink_filters.len() < self.sinks.len() {
            self.compiled_sink_filters.push(SinkFilter::Pending);
        }

        for (i, (sink_name, _, filter_sql, sink_input, _)) in self.sinks.iter().enumerate() {
            if filter_sql.is_none() || !matches!(self.compiled_sink_filters[i], SinkFilter::Pending)
            {
                continue;
            }
            let Some(batches) = results.get(sink_input.as_str()) else {
                continue;
            };
            let Some(batch) = batches.first() else {
                continue;
            };
            let schema = batch.schema();
            let sql = filter_sql.as_deref().unwrap();
            match crate::filter_compile::compile(&self.filter_ctx, sql, &schema).await {
                Ok(compiled) => {
                    self.compiled_sink_filters[i] = SinkFilter::Compiled(compiled);
                }
                Err(e) => {
                    tracing::error!(
                        sink = %sink_name,
                        filter = %sql,
                        error = %e,
                        "[LDB-1100] sink filter did not compile; fail-closed: \
                         ALL rows from this stream will be dropped for this sink. \
                         Track via sink_filter_rejected_rows_total."
                    );
                    self.compiled_sink_filters[i] = SinkFilter::Rejected;
                }
            }
            self.pending_sink_filter_compiles = self.pending_sink_filter_compiles.saturating_sub(1);
        }
    }

    fn refresh_source_watermarks(&mut self) {
        self.source_wms_buf.clear();
        if let Some(ref tracker) = self.tracker {
            for (&sid, name) in &self.source_name_arcs {
                if let Some(watermark) = tracker.source_watermark(sid) {
                    self.source_wms_buf.insert(Arc::clone(name), watermark);
                }
            }
        }

        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.as_ref() {
            Self::cap_source_watermarks_by_cluster_min(
                &mut self.source_wms_buf,
                controller.cluster_min_watermark(),
            );
        }
    }

    async fn drain_checkpoint_edges_until_inner(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), crate::pipeline::CycleError> {
        self.refresh_source_watermarks();
        let watermark = self.effective_pipeline_watermark();
        while !self.graph.checkpoint_is_quiescent() {
            if tokio::time::Instant::now() >= deadline {
                let error = format!(
                    "checkpoint graph drain exhausted its end-to-end deadline with {} buffered bytes",
                    self.graph.checkpoint_pending_input_bytes()
                );
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(crate::pipeline::CycleError::Recovery(error));
            }

            let source_watermarks = if self.source_wms_buf.is_empty() {
                None
            } else {
                Some(&self.source_wms_buf)
            };
            let pass_started = std::time::Instant::now();
            let results = match self
                .graph
                .execute_checkpoint_drain_cycle(watermark, source_watermarks)
                .await
            {
                Ok(results) => results,
                Err(error) => {
                    let mapped = Self::map_checkpoint_drain_error(&error, &self.shutdown_signal);
                    if let crate::pipeline::CycleError::Recovery(reason) = &mapped {
                        set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
                    }
                    return Err(mapped);
                }
            };
            let (any_failed, _) = self.graph.take_cycle_failures();
            if any_failed {
                let error = "checkpoint graph drain encountered a partial operator-domain failure"
                    .to_string();
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(crate::pipeline::CycleError::Recovery(error));
            }
            let (any_deferred, _) = self.graph.take_cycle_deferrals();
            if any_deferred && self.graph.checkpoint_is_quiescent() {
                let error =
                    "checkpoint graph reported deferred work without a pending edge".to_string();
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(crate::pipeline::CycleError::Recovery(error));
            }

            if let Err(error) =
                <Self as crate::pipeline::PipelineCallback>::update_mv_stores(self, &results)
            {
                let error = format!(
                    "checkpoint graph drain could not publish materialized-view output: {error}"
                );
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(crate::pipeline::CycleError::Recovery(error));
            }
            if let Err(error) =
                <Self as crate::pipeline::PipelineCallback>::push_to_streams(self, &results)
            {
                let error =
                    format!("checkpoint graph drain could not publish stream output: {error}");
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(crate::pipeline::CycleError::Recovery(error));
            }
            <Self as crate::pipeline::PipelineCallback>::write_to_sinks(self, &results).await;
            #[allow(clippy::cast_possible_truncation)]
            <Self as crate::pipeline::PipelineCallback>::record_cycle(
                self,
                0,
                0,
                pass_started.elapsed().as_nanos() as u64,
            );

            if tokio::time::Instant::now() >= deadline && !self.graph.checkpoint_is_quiescent() {
                let error = format!(
                    "checkpoint graph drain exhausted its end-to-end deadline with {} buffered bytes",
                    self.graph.checkpoint_pending_input_bytes()
                );
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(crate::pipeline::CycleError::Recovery(error));
            }
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn admit_control_stream(
    graph: &mut crate::operator_graph::OperatorGraph,
    name: String,
    sql: String,
    emit_clause: Option<laminar_sql::parser::EmitClause>,
    window_config: Option<laminar_sql::translator::WindowOperatorConfig>,
    order_config: Option<laminar_sql::translator::OrderOperatorConfig>,
    join_config: Option<Vec<laminar_sql::translator::JoinOperatorConfig>>,
    incremental: bool,
) -> Result<(), DbError> {
    graph.add_query(
        name.clone(),
        sql,
        emit_clause,
        window_config,
        order_config,
        join_config,
        incremental,
    );
    let admitted = graph.take_build_errors();
    if admitted.is_err() {
        graph.remove_query(&name);
    }
    admitted
}

pub(crate) fn apply_control_to_graph(
    graph: &mut crate::operator_graph::OperatorGraph,
    msg: crate::pipeline::ControlMsg,
) {
    match msg.into_kind() {
        crate::pipeline::ControlMsgKind::AddStream {
            name,
            sql,
            emit_clause,
            window_config,
            order_config,
            join_config,
            incremental,
            reply,
            mutation,
        } => {
            if mutation.state() == crate::pipeline::ControlMutationState::Cancelled {
                let _ = reply.send(Err(DbError::Pipeline(format!(
                    "CREATE for '{name}' was cancelled before graph admission"
                ))));
                return;
            }
            let result = match admit_control_stream(
                graph,
                name.clone(),
                sql,
                emit_clause,
                window_config,
                order_config,
                join_config,
                incremental,
            ) {
                Ok(()) if mutation.try_apply() => {
                    tracing::info!(stream = %name, "Stream added via control channel");
                    Ok(())
                }
                Ok(()) => {
                    graph.remove_query(&name);
                    Err(DbError::Pipeline(format!(
                        "CREATE for '{name}' was cancelled before graph admission committed"
                    )))
                }
                Err(error) => {
                    mutation.cancel();
                    Err(error)
                }
            };
            let _ = reply.send(result);
        }
        crate::pipeline::ControlMsgKind::DropStreams {
            names,
            reply,
            mutation,
        } => {
            let result = if mutation.try_apply() {
                for name in &names {
                    graph.remove_query(name);
                }
                tracing::info!(streams = ?names, "Streams removed via control channel");
                Ok(())
            } else {
                Err(DbError::Pipeline(
                    "DROP was cancelled before graph removal committed".to_string(),
                ))
            };
            let _ = reply.send(result);
        }
    }
}

#[allow(clippy::too_many_lines)]
impl crate::pipeline::PipelineCallback for ConnectorPipelineCallback {
    fn prepare_source_intake(&mut self) -> Result<(), String> {
        #[cfg(feature = "cluster")]
        self.reconcile_source_handoff_watermarks()?;
        Ok(())
    }

    async fn execute_cycle(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        watermark: i64,
    ) -> Result<crate::pipeline::CycleOutcome, crate::pipeline::CycleError> {
        // Test-only one-shot fault injector for the recovery soak (inert in release / when
        // unset). The harness creates a per-process trigger file only after the cluster has
        // reached steady state and it has observed the node's actual leader/follower role.
        #[cfg(all(debug_assertions, feature = "cluster"))]
        {
            use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
            use std::sync::OnceLock;
            use std::time::Instant;
            static TRIGGER_FILE: OnceLock<Option<std::path::PathBuf>> = OnceLock::new();
            static POLL_START: OnceLock<Instant> = OnceLock::new();
            static NEXT_POLL_MS: AtomicU64 = AtomicU64::new(0);
            static FIRED: AtomicBool = AtomicBool::new(false);
            if let Some(path) = TRIGGER_FILE
                .get_or_init(|| {
                    std::env::var_os("LAMINAR_FAULT_INJECT_TRIGGER_FILE").map(Into::into)
                })
                .as_ref()
            {
                let now_ms =
                    u64::try_from(POLL_START.get_or_init(Instant::now).elapsed().as_millis())
                        .unwrap_or(u64::MAX);
                let next_poll = NEXT_POLL_MS.load(Ordering::Relaxed);
                if now_ms >= next_poll
                    && NEXT_POLL_MS
                        .compare_exchange(
                            next_poll,
                            now_ms.saturating_add(25),
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                {
                    let requested_role = std::fs::read_to_string(path).ok();
                    let role_matches = requested_role.as_deref().is_some_and(|role| {
                        self.cluster_controller.as_ref().is_some_and(|controller| {
                            match role.trim() {
                                "leader" => controller.is_leader(),
                                "follower" => {
                                    !controller.is_leader() && controller.current_leader().is_some()
                                }
                                _ => false,
                            }
                        })
                    });
                    if role_matches
                        && !FIRED.load(Ordering::Relaxed)
                        && FIRED
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                            .is_ok()
                    {
                        if std::fs::remove_file(path).is_ok() {
                            return Err(crate::pipeline::CycleError::Fatal(
                                "injected fault for coordinated-recovery soak \
                                 (LAMINAR_FAULT_INJECT_TRIGGER_FILE)"
                                    .into(),
                            ));
                        }
                        FIRED.store(false, Ordering::Release);
                    }
                }
            }
        }
        self.refresh_source_watermarks();

        let swm_ref = if self.source_wms_buf.is_empty() {
            None
        } else {
            Some(&self.source_wms_buf)
        };
        let results = self
            .graph
            .execute_cycle(source_batches, watermark, swm_ref)
            .await
            .map_err(|e| Self::map_graph_error(&e, &self.shutdown_signal))?;
        let (any_failed, failed_sources) = self.graph.take_cycle_failures();
        let (any_deferred, deferred_sources) = self.graph.take_cycle_deferrals();
        Ok(crate::pipeline::CycleOutcome {
            results,
            any_failed,
            failed_sources,
            any_deferred,
            deferred_sources,
        })
    }

    async fn drain_checkpoint_edges_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), crate::pipeline::CycleError> {
        self.drain_checkpoint_edges_until_inner(deadline).await
    }

    fn push_to_streams(
        &self,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<(), crate::pipeline::CycleError> {
        for entry in &self.stream_entries {
            if let Some(batches) = results.get(entry.name.as_str()) {
                for batch in batches {
                    if batch.num_rows() > 0 {
                        #[allow(clippy::cast_possible_truncation)]
                        let row_count = batch.num_rows() as u64;
                        self.subscription_registry
                            .send_batch(&entry.name, batch.clone())
                            .map_err(|error| {
                                let reason = format!(
                                    "subscription publication for stream '{}' failed: {error}",
                                    entry.name
                                );
                                set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
                                crate::pipeline::CycleError::Recovery(reason)
                            })?;
                        entry.record_emitted_rows(row_count);
                        self.prom.events_emitted.inc_by(row_count);
                    }
                }
            }
        }

        // Ephemeral streams (console live queries) aren't in `stream_entries`; push them here.
        // MVs reach subscribers via their own path below.
        let mv_has_any = self
            .mv_store_has_any
            .load(std::sync::atomic::Ordering::Acquire);
        let mv_read = if mv_has_any {
            Some(self.mv_store.read())
        } else {
            None
        };
        for (stream_name, batches) in results {
            let is_named = self.named_stream_names.contains(stream_name);
            let has_mv = mv_read
                .as_ref()
                .is_some_and(|r| r.has_mv(stream_name.as_ref()));
            if is_named || has_mv {
                continue;
            }
            for batch in batches {
                if batch.num_rows() > 0 {
                    self.subscription_registry
                        .send_batch(stream_name, batch.clone())
                        .map_err(|error| {
                            let reason = format!(
                                "subscription publication for ephemeral stream '{stream_name}' failed: {error}"
                            );
                            set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
                            crate::pipeline::CycleError::Recovery(reason)
                        })?;
                }
            }
        }
        Ok(())
    }

    fn update_mv_stores(
        &self,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<(), crate::pipeline::CycleError> {
        if results.is_empty() {
            return Ok(());
        }
        if !self
            .mv_store_has_any
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(());
        }
        let mut store = self.mv_store.write();
        let mut updates = 0u64;
        // Snapshot broadcast is deferred past the write lock (rematerialize is O(rows) and would
        // otherwise block SELECT readers on the store-wide lock).
        let mut changelog_broadcasts: Vec<Arc<str>> = Vec::new();
        for (stream_name, batches) in results {
            if !store.has_mv(stream_name) {
                continue;
            }
            // A changelog MV (batches carry `__weight`) must not put the raw Z-set changelog on the
            // SUBSCRIBE wire — subscribers get plain rows. Apply to the store, then broadcast the
            // consolidated snapshot instead (only when someone is listening). A full-emit MV keeps
            // forwarding its per-cycle batch verbatim (that batch already IS its full state).
            let changelog = batches.iter().any(|b| {
                b.num_rows() > 0
                    && b.schema()
                        .index_of(laminar_core::changelog::WEIGHT_COLUMN)
                        .is_ok()
            });
            // Apply the whole cycle's output in one call: an Aggregate-mode MV replaces its
            // result set per cycle, so a per-batch update would keep only the last chunk of a
            // multi-batch (>8192-row) output (EX-1).
            #[allow(clippy::cast_possible_truncation)]
            let row_batches = batches.iter().filter(|b| b.num_rows() > 0).count() as u64;
            if row_batches > 0 {
                store.update_cycle(stream_name, batches).map_err(|error| {
                    let reason = format!(
                        "materialized-view state update for '{stream_name}' failed: {error}"
                    );
                    set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
                    crate::pipeline::CycleError::Recovery(reason)
                })?;
                updates += row_batches;
                if !changelog {
                    for batch in batches {
                        if batch.num_rows() > 0 {
                            self.subscription_registry
                                .send_batch(stream_name, batch.clone())
                                .map_err(|error| {
                                    let reason = format!(
                                        "materialized-view subscription publication for '{stream_name}' failed: {error}"
                                    );
                                    set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
                                    crate::pipeline::CycleError::Recovery(reason)
                                })?;
                        }
                    }
                }
            }
            if changelog && self.subscription_registry.subscriber_count(stream_name) > 0 {
                changelog_broadcasts.push(Arc::clone(stream_name));
            }
        }
        if updates > 0 {
            self.prom.mv_updates.inc_by(updates);
            #[allow(clippy::cast_possible_truncation)]
            let bytes = store.total_bytes() as u64;
            #[allow(clippy::cast_possible_wrap)]
            self.prom.mv_bytes_stored.set(bytes as i64);
        }
        drop(store);

        if !changelog_broadcasts.is_empty() {
            let store = self.mv_store.read();
            for stream_name in changelog_broadcasts {
                match store.to_record_batch(&stream_name) {
                    Ok(Some(snap)) => self
                        .subscription_registry
                        .send_batch(&stream_name, snap)
                        .map_err(|error| {
                            let reason = format!(
                                "materialized-view snapshot publication for '{stream_name}' failed: {error}"
                            );
                            set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
                            crate::pipeline::CycleError::Recovery(reason)
                        })?,
                    Ok(None) => {}
                    Err(error) => {
                        let reason = format!(
                            "materialized-view snapshot materialization for '{stream_name}' failed: {error}"
                        );
                        set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
                        return Err(crate::pipeline::CycleError::Recovery(reason));
                    }
                }
            }
        }
        Ok(())
    }

    async fn close_sinks(&mut self) -> Result<(), String> {
        let mut failures = Vec::new();

        // Close is itself an ordered command: each actor processes every previously queued write
        // before acknowledging connector flush/close. Its bounded enqueue, acknowledgement, and
        // join replace the old unbounded pre-close Sync round trip.
        // Register ownership before the first await. If the callback runtime is cancelled while
        // close is enqueuing or awaiting an unsafe connector, replacement startup still sees and
        // drives every actor to a terminal state.
        {
            let mut registered = self.owned_sink_handles.lock();
            for (_, handle, _, _, _) in &self.sinks {
                if !registered.iter().any(|known| known.same_actor(handle)) {
                    registered.push(handle.clone());
                }
            }
        }
        let close_results =
            futures::future::join_all(self.sinks.iter().map(|(name, handle, _, _, _)| {
                let name = name.clone();
                let handle = handle.clone();
                async move { (name, handle.close().await) }
            }))
            .await;
        for (name, result) in close_results {
            if let Err(error) = result {
                failures.push(format!("sink '{name}' shutdown close failed: {error}"));
            }
        }
        self.owned_sink_handles
            .lock()
            .retain(crate::sink_task::SinkTaskHandle::has_unresolved_task);
        self.drain_sink_events();
        if let Some(reason) = self.sink_fault.take() {
            failures.push(reason);
        } else if self.sink_timed_out {
            failures.push("one or more best-effort sink writes failed before shutdown".to_string());
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    async fn write_to_sinks(&mut self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        self.compile_pending_sink_filters(results).await;

        // Shared Arc per stream so multiple sinks don't each clone the Vec.
        let mut shared_inputs: FxHashMap<&str, Arc<[RecordBatch]>> = FxHashMap::default();

        let sink_futures: Vec<_> = self
            .sinks
            .iter()
            .enumerate()
            .filter_map(
                |(sink_idx, (sink_name, handle, _filter_sql, sink_input, contract))| {
                    let batches = results.get(sink_input.as_str())?;
                    if batches.is_empty() {
                        return None;
                    }
                    let shared = shared_inputs
                        .entry(sink_input.as_str())
                        .or_insert_with(|| Arc::<[RecordBatch]>::from(batches.as_slice()))
                        .clone();
                    let sink_name = sink_name.clone();
                    let handle = handle.clone();
                    let filter_state = match self.compiled_sink_filters.get(sink_idx).cloned() {
                        Some(SinkFilter::Compiled(phys)) => SinkFilterDispatch::Compiled(phys),
                        Some(SinkFilter::Rejected) => SinkFilterDispatch::Rejected,
                        Some(SinkFilter::Pending) | None => SinkFilterDispatch::None,
                    };
                    let accepts_full_changelog = contract.accepts_full_changelog();
                    let prom = Arc::clone(&self.prom);
                    Some(async move {
                        for batch in shared.iter() {
                            let filtered: Cow<RecordBatch> = match &filter_state {
                                SinkFilterDispatch::Compiled(phys) => {
                                    match crate::filter_compile::apply(batch, phys.as_ref()) {
                                        Ok(Some(fb)) => Cow::Owned(fb),
                                        Ok(None) => continue,
                                        Err(e) => {
                                            tracing::warn!(
                                                sink = %sink_name,
                                                error = %e,
                                                "Compiled sink filter error"
                                            );
                                            continue;
                                        }
                                    }
                                }
                                SinkFilterDispatch::Rejected => {
                                    #[allow(clippy::cast_possible_truncation)]
                                    let dropped = batch.num_rows() as u64;
                                    prom.sink_filter_rejected_rows
                                        .with_label_values(&[sink_name.as_str()])
                                        .inc_by(dropped);
                                    continue;
                                }
                                SinkFilterDispatch::None => Cow::Borrowed(batch),
                            };

                            let prepared = crate::changelog_filter::prepare_for_sink(
                                &filtered,
                                accepts_full_changelog,
                            );
                            if prepared.num_rows() == 0 {
                                continue;
                            }
                            if let Err(e) = handle.write_batch(prepared.into_owned()).await {
                                tracing::warn!(
                                    sink = %sink_name,
                                    error = %e,
                                    "Sink write could not be enqueued"
                                );
                                return Some(format!(
                                    "sink '{sink_name}' write enqueue failed: {e}"
                                ));
                            }
                        }
                        None
                    })
                },
            )
            .collect();
        let direct_failures = futures::future::join_all(sink_futures).await;
        for reason in direct_failures.into_iter().flatten() {
            // Do not depend on the bounded event channel for correctness. In particular, a full
            // event channel must not turn an enqueue timeout into a checkpointable lost write.
            self.record_dropped_sink_write(reason);
        }

        // Opportunistic; the strict barrier runs in the checkpoint path.
        self.drain_sink_events();
    }

    fn extract_watermark(&mut self, source_name: &str, batch: &RecordBatch) {
        if let Some(wm_state) = self.watermark_states.get_mut(source_name) {
            if let Some(entry) = self.source_entries_for_wm.get(source_name) {
                let external_wm = entry.source.current_watermark();
                if let Some(wm) = wm_state.generator.advance_watermark(external_wm) {
                    self.prom
                        .source_watermark_ms
                        .with_label_values(&[source_name])
                        .set(wm.timestamp());
                    if let Some(ref mut trk) = self.tracker {
                        if let Some(sid) = self.source_ids.get(source_name) {
                            if let Some(global_wm) = trk.update_source(*sid, wm.timestamp()) {
                                self.pipeline_watermark.store(
                                    global_wm.timestamp(),
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                            }
                        }
                    }
                }
            }

            if let Ok(max_ts) = wm_state.extractor.extract(batch) {
                if let Some(wm) = wm_state.generator.on_event(max_ts) {
                    if let Some(entry) = self.source_entries_for_wm.get(source_name) {
                        entry.source.watermark(wm.timestamp());
                    }
                    self.prom
                        .source_watermark_ms
                        .with_label_values(&[source_name])
                        .set(wm.timestamp());
                    if let Some(ref mut trk) = self.tracker {
                        if let Some(sid) = self.source_ids.get(source_name) {
                            if let Some(global_wm) = trk.update_source(*sid, wm.timestamp()) {
                                self.pipeline_watermark.store(
                                    global_wm.timestamp(),
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                            }
                        }
                    }
                }
            }
        }

        #[allow(clippy::cast_possible_truncation)]
        let row_count = batch.num_rows() as u64;
        self.prom.events_ingested.inc_by(row_count);
        self.prom.batches.inc();
    }

    fn filter_late_rows(&self, source_name: &str, batch: &RecordBatch) -> Option<RecordBatch> {
        if let Some(wm_state) = self.watermark_states.get(source_name) {
            // Processing-time watermarks are wall-clock; filtering would drop every real row.
            if wm_state.generator.is_processing_time() {
                return Some(batch.clone());
            }
            let current_wm = wm_state.generator.current_watermark();
            #[cfg(feature = "cluster")]
            let current_wm = self
                .cluster_controller
                .as_ref()
                .map_or(current_wm, |controller| {
                    Self::cap_watermark_by_cluster_min(
                        current_wm,
                        controller.cluster_min_watermark(),
                    )
                });
            if current_wm > i64::MIN {
                let before = batch.num_rows();
                // Null timestamps are data-quality, not lateness — count separately.
                let null_ts = batch
                    .column_by_name(&wm_state.column)
                    .map_or(0, |c| c.null_count());
                match filter_late_rows(batch, &wm_state.column, current_wm) {
                    Ok(out) => {
                        let after = out.as_ref().map_or(0, arrow_array::RecordBatch::num_rows);
                        let dropped = before.saturating_sub(after);
                        let late = dropped.saturating_sub(null_ts);
                        if null_ts > 0 {
                            self.prom
                                .events_null_timestamp
                                .inc_by(u64::try_from(null_ts).unwrap_or(u64::MAX));
                        }
                        if late > 0 {
                            self.prom
                                .events_dropped
                                .inc_by(u64::try_from(late).unwrap_or(u64::MAX));
                            warn_late_drops(source_name, &wm_state.column, current_wm, late);
                        }
                        return out;
                    }
                    Err(e) => {
                        // Schema drift, not lateness.
                        tracing::error!(
                            source = source_name,
                            column = %wm_state.column,
                            error = %e,
                            "filter_late_rows: dropping batch (schema drift)"
                        );
                        return None;
                    }
                }
            }
        }
        Some(batch.clone())
    }

    fn current_watermark(&self) -> i64 {
        self.effective_pipeline_watermark()
    }

    fn is_leader(&self) -> bool {
        #[cfg(feature = "cluster")]
        {
            if let Some(ref cc) = self.cluster_controller {
                return cc.is_leader();
            }
        }
        true
    }

    fn is_recovering(&self) -> bool {
        #[cfg(feature = "cluster")]
        {
            if let Some(ref cc) = self.cluster_controller {
                return cc.is_recovering();
            }
        }
        false
    }

    fn fault_on_cycle_error(&self) -> bool {
        use laminar_connectors::connector::DeliveryGuarantee;
        self.delivery_guarantee != DeliveryGuarantee::BestEffort || self.in_cluster()
    }

    fn take_pipeline_fault(&mut self) -> Option<String> {
        self.reap_checkpoint_tail_tasks();
        self.sink_fault
            .take()
            .or_else(|| self.checkpoint_fault.lock().take())
    }

    async fn settle_checkpoint_tail_tasks(&mut self, abort: bool) -> Result<(), String> {
        if abort {
            self.checkpoint_tail_tasks.abort_all();
            // Cancellation is cooperative and a tail may be inside blocking storage code. Drop
            // the JoinSet after requesting abort instead of defeating the shutdown deadline by
            // awaiting it. Exact attempt IDs make any detached ambiguous write unusable unless
            // its matching durable decision exists.
            self.checkpoint_tail_tasks = tokio::task::JoinSet::new();
            return Ok(());
        }
        let mut failures = Vec::new();
        while let Some(result) = self.checkpoint_tail_tasks.join_next().await {
            match result {
                Ok(()) => {}
                Err(error) => failures.push(error.to_string()),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "checkpoint durable tail task failure: {}",
                failures.join("; ")
            ))
        }
    }

    fn record_checkpoint_failure(&mut self, checkpoint_id: u64, reason: &str) {
        if self.delivery_guarantee == laminar_connectors::connector::DeliveryGuarantee::ExactlyOnce
        {
            set_checkpoint_fault(
                &self.checkpoint_fault,
                format!("checkpoint {checkpoint_id}: {reason}"),
            );
        }
    }

    fn record_checkpoint_continuation_fault(&mut self, attempt: CheckpointAttempt, reason: &str) {
        set_checkpoint_fault(
            &self.checkpoint_fault,
            format!(
                "checkpoint {} epoch {} committed, but continuation failed: {reason}",
                attempt.checkpoint_id, attempt.epoch
            ),
        );
    }

    fn record_checkpoint_admission_failure(&mut self, reason: &str) {
        if self.delivery_guarantee == laminar_connectors::connector::DeliveryGuarantee::ExactlyOnce
        {
            set_checkpoint_fault(
                &self.checkpoint_fault,
                format!("checkpoint admission: {reason}"),
            );
        }
    }

    async fn reserve_checkpoint_attempt(
        &mut self,
        attempt_started: std::time::Instant,
    ) -> Result<CheckpointAttempt, String> {
        self.reserve_attempt(attempt_started)
            .await
            .map_err(|error| error.to_string())
    }

    async fn publish_checkpoint_prepare(
        &mut self,
        attempt: CheckpointAttempt,
        attempt_started: std::time::Instant,
        admitted_assignment_fence: Option<
            laminar_core::cluster::control::CheckpointAssignmentFence,
        >,
    ) -> Result<(), String> {
        #[cfg(feature = "cluster")]
        {
            use laminar_core::cluster::control::{BarrierAnnouncement, Phase};

            let assignment_fence =
                self.validate_checkpoint_assignment(admitted_assignment_fence.as_ref())?;
            let Some(controller) = self.cluster_controller.as_ref() else {
                return Ok(());
            };
            if !controller.is_leader() {
                return Err("leadership changed before checkpoint Prepare publication".into());
            }
            let leader_proof = controller.capture_leader_proof().ok_or_else(|| {
                "no exact durable leader proof is live for checkpoint Prepare".to_string()
            })?;
            if !leader_proof.is_canonical() || !controller.proof_is_live(&leader_proof) {
                return Err("checkpoint Prepare captured an invalid leader proof".into());
            }
            if self.checkpoint_leader_proofs.contains_key(&attempt) {
                return Err(format!(
                    "checkpoint {} epoch {} already owns a leader proof",
                    attempt.checkpoint_id, attempt.epoch
                ));
            }
            let assignment_fence = assignment_fence.ok_or_else(|| {
                "[LDB-6055] clustered checkpoint lost its assignment certificate before Prepare"
                    .to_string()
            })?;
            let deadline =
                tokio::time::Instant::from_std(attempt_started) + self.checkpoint_timeout;
            // Publication failure is ambiguous: retain the proof before issuing I/O so cleanup
            // can resolve this exact attempt instead of assuming Prepare was absent.
            self.checkpoint_leader_proofs
                .insert(attempt, leader_proof.clone());
            tokio::time::timeout_at(
                deadline,
                controller.announce_barrier(&BarrierAnnouncement {
                    epoch: attempt.epoch,
                    checkpoint_id: attempt.checkpoint_id,
                    assignment_fence: Some(assignment_fence),
                    leader_proof: Some(leader_proof.clone()),
                    phase: Phase::Prepare,
                    flags: 0,
                    min_watermark_ms: None,
                }),
            )
            .await
            .map_err(|_| {
                format!(
                    "checkpoint Prepare publication exhausted the {:?} end-to-end deadline",
                    self.checkpoint_timeout
                )
            })?
            .map_err(|error| format!("checkpoint Prepare publication failed: {error}"))?;
            Ok(())
        }

        #[cfg(not(feature = "cluster"))]
        {
            let _ = (attempt, attempt_started);
            if admitted_assignment_fence.is_some() {
                return Err(
                    "cluster assignment certificate supplied to a local checkpoint runtime".into(),
                );
            }
            Ok(())
        }
    }

    async fn abandon_checkpoint_attempt(
        &mut self,
        attempt: CheckpointAttempt,
        reason: &str,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) -> Result<(), String> {
        self.abandon_reserved_attempt(attempt, reason.to_owned(), assignment_fence)
            .await
    }

    #[cfg(feature = "cluster")]
    async fn checkpoint_assignment_for_admission(
        &mut self,
    ) -> Result<Option<laminar_core::cluster::control::CheckpointAssignmentFence>, String> {
        self.reconcile_source_handoff_watermarks()?;
        let Some(controller) = self.cluster_controller.as_ref() else {
            return Ok(None);
        };
        let Some(registry) = self.vnode_registry.as_ref() else {
            tracing::error!(
                "cluster checkpoint admission has no vnode registry; failing assignment fence"
            );
            return Err("cluster checkpoint admission has no vnode registry".into());
        };
        let publication = registry.versioned_snapshot();
        if publication
            .source_handoff_installed_version()
            .is_some_and(|installed| {
                self.reconciled_source_handoff_version
                    .is_none_or(|reconciled| reconciled < installed)
            })
        {
            return Err(format!(
                "assignment {} source handoff is not installed",
                publication.version()
            ));
        }
        // The snapshot watcher performs the gossip scan off the hot path. Retain the exact
        // certificate so later capture/quorum/durable phases cannot silently switch generations.
        let fence = controller
            .checkpoint_assignment_fence(publication.version())
            .ok_or_else(|| {
                format!(
                    "assignment {} is not checkpoint-ready",
                    publication.version()
                )
            })?;
        let verified = registry.versioned_snapshot();
        if verified.version() != publication.version()
            || verified.source_handoff_installed_version()
                != publication.source_handoff_installed_version()
        {
            return Err("assignment changed while checkpoint admission was being certified".into());
        }
        Ok(Some(fence))
    }

    fn tick_idle_watermark(&mut self) {
        let Some(ref mut trk) = self.tracker else {
            return;
        };
        for (name, state) in &mut self.watermark_states {
            if let Some(wm) = state.generator.on_periodic() {
                if let Some(&id) = self.source_ids.get(name) {
                    if let Some(global) = trk.update_source(id, wm.timestamp()) {
                        self.pipeline_watermark
                            .store(global.timestamp(), std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
            if let Some(entry) = self.source_entries_for_wm.get(name) {
                let external = entry.source.current_watermark();
                if external > i64::MIN {
                    if let Some(wm) = state.generator.advance_watermark(external) {
                        if let Some(&id) = self.source_ids.get(name) {
                            if let Some(global) = trk.update_source(id, wm.timestamp()) {
                                self.pipeline_watermark.store(
                                    global.timestamp(),
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                            }
                        }
                    }
                }
            }
        }
        if let Some(global) = trk.check_idle_sources() {
            self.pipeline_watermark
                .store(global.timestamp(), std::sync::atomic::Ordering::Relaxed);
            tracing::info!(
                watermark_ms = global.timestamp(),
                "pipeline watermark advanced via idle-source detection"
            );
        }
        for (name, &id) in &self.source_ids {
            self.prom
                .source_idle
                .with_label_values(&[name.as_str()])
                .set(i64::from(trk.is_idle(id)));
        }
    }

    /// Service follower announcements observed from the cluster leader.
    /// All local checkpoint admission is owned exclusively by `StreamingCoordinator`.
    async fn service_checkpoint_control(
        &mut self,
        source_offsets: FxHashMap<String, SourceCheckpoint>,
    ) -> crate::pipeline::CheckpointControlOutcome {
        // Followers respond only to a leader-published PREPARE. Leaders and local runtimes return
        // control to the streaming coordinator, which is the sole admission owner.
        #[cfg(feature = "cluster")]
        if let Some(cc) = self.cluster_controller.clone() {
            if let Err(error) = self.reconcile_source_handoff_watermarks() {
                set_checkpoint_fault(
                    &self.checkpoint_fault,
                    format!("follower source handoff reconciliation failed: {error}"),
                );
                return crate::pipeline::CheckpointControlOutcome::Idle;
            }
            return if cc.is_leader() {
                crate::pipeline::CheckpointControlOutcome::Idle
            } else {
                self.maybe_follower_checkpoint(cc, source_offsets).await
            };
        }

        let _ = source_offsets;
        crate::pipeline::CheckpointControlOutcome::Idle
    }

    async fn checkpoint_with_barrier(
        &mut self,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
        attempt: CheckpointAttempt,
        attempt_started: std::time::Instant,
        admitted_assignment_fence: Option<
            laminar_core::cluster::control::CheckpointAssignmentFence,
        >,
    ) -> crate::pipeline::BarrierOutcome {
        use crate::pipeline::{BarrierOutcome, SkipReason};

        #[cfg(feature = "cluster")]
        if let Some(cc) = self.cluster_controller.clone() {
            if !cc.is_leader() {
                if let Some(ann) = self.pending_follower_checkpoint.as_ref().cloned() {
                    // A slow round can finish after its epoch was abandoned; never attribute
                    // captured offsets to a newer announcement.
                    if ann.checkpoint_id != attempt.checkpoint_id || ann.epoch != attempt.epoch {
                        tracing::warn!(
                            round_checkpoint_id = attempt.checkpoint_id,
                            round_epoch = attempt.epoch,
                            pending_checkpoint_id = ann.checkpoint_id,
                            pending_epoch = ann.epoch,
                            "stale follower barrier round — its epoch was abandoned"
                        );
                        return BarrierOutcome::Failed;
                    }
                    let outcome = self
                        .run_follower_checkpoint_deferred(ann, source_checkpoints, attempt_started)
                        .await;
                    if matches!(outcome, BarrierOutcome::Async) {
                        if let Err(error) = self.clear_pending_follower_checkpoint(attempt) {
                            set_checkpoint_fault(&self.checkpoint_fault, error);
                        }
                    }
                    return outcome;
                }
                tracing::warn!(
                    "follower received checkpoint_with_barrier but pending_follower_checkpoint is None"
                );
                return BarrierOutcome::Failed;
            }
        }

        if self.prom.cycles.get() == 0 {
            return BarrierOutcome::Skipped(SkipReason::NoCyclesSinceLastCheckpoint);
        }

        #[cfg(feature = "cluster")]
        let assignment_fence =
            match self.validate_checkpoint_assignment(admitted_assignment_fence.as_ref()) {
                Ok(fence) => fence,
                Err(error) => {
                    tracing::warn!(%error, "checkpoint assignment changed before capture");
                    return BarrierOutcome::Failed;
                }
            };
        #[cfg(not(feature = "cluster"))]
        let assignment_fence = {
            let _ = admitted_assignment_fence;
            None
        };

        let attempt_deadline =
            tokio::time::Instant::from_std(attempt_started) + self.checkpoint_timeout;
        // Sink fencing, shuffle alignment, and state capture all pause the pipeline. A
        // drop-observing timer also records early failures rather than hiding their latency.
        let _stall_timer = self.prom.checkpoint_pipeline_stall_duration.start_timer();

        match tokio::time::timeout_at(
            attempt_deadline,
            self.sync_sinks_and_drain_events(attempt_deadline),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::error!(%error, "checkpoint sink write fence failed");
                return BarrierOutcome::Failed;
            }
            Err(_) => {
                tracing::error!(
                    timeout = ?self.checkpoint_timeout,
                    "checkpoint sink write fence exhausted the end-to-end attempt deadline"
                );
                return BarrierOutcome::Failed;
            }
        }

        // A pending exactly-once sink fault means the coordinator is about to fault for recovery;
        // don't seal this epoch past the dropped rows (CP-4). Leave the flag for `take_pipeline_fault`.
        if self.sink_fault.is_some() {
            return BarrierOutcome::Failed;
        }

        // Clear after one suppression; the timer path is unreachable under barrier checkpointing.
        if self.sink_timed_out {
            self.sink_timed_out = false;
            return BarrierOutcome::Skipped(SkipReason::PreservingReplayWindowAfterSinkTimeout);
        }

        // Align the shuffle before capture so peers' pre-barrier rows enter the snapshot.
        #[cfg(feature = "cluster")]
        match self
            .align_shuffle_for_leader(attempt, assignment_fence.as_ref(), attempt_deadline)
            .await
        {
            Ok(()) => {}
            Err(error) => {
                tracing::warn!(%error, "shuffle barrier alignment failed");
                set_checkpoint_fault(&self.checkpoint_fault, error.to_string());
                return BarrierOutcome::Failed;
            }
        }

        // Trailing loss — a peer's last frames of the epoch, which no data-gap check can see —
        // surfaces only when that peer's barrier lands, i.e. inside the alignment above, after the
        // fence at the top of this function. Re-check before capture: a snapshot taken now would
        // seal the gap, and the rewind target would then sit at or above the corrupt epoch (CL-2).
        #[cfg(feature = "cluster")]
        {
            self.check_shuffle_delivery_loss();
            if self.sink_fault.is_some() {
                return BarrierOutcome::Failed;
            }
        }

        let operator_state = match tokio::time::timeout_at(
            attempt_deadline,
            self.capture_operator_state_until(attempt_deadline),
        )
        .await
        {
            Ok(Ok(capture)) => capture,
            Ok(Err(error)) => {
                tracing::warn!(%error, "Stream executor barrier checkpoint failed");
                return BarrierOutcome::Failed;
            }
            Err(_) => {
                tracing::warn!(
                    timeout = ?self.checkpoint_timeout,
                    "state capture exhausted the end-to-end checkpoint deadline"
                );
                return BarrierOutcome::Failed;
            }
        };

        let vnode_states = match self.capture_vnode_states(attempt.epoch) {
            Ok(states) => states,
            Err(error) => {
                tracing::warn!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    error = %error,
                    "barrier checkpoint vnode capture failed"
                );
                return BarrierOutcome::Failed;
            }
        };
        let operator_state_encoded_budget = match encoded_operator_state_budget(
            self.checkpoint_state_cap_bytes,
            operator_state.estimated_bytes(),
            staged_vnode_bytes(&vnode_states),
        ) {
            Ok(budget) => budget,
            Err(error) => {
                tracing::warn!(%error, "checkpoint immutable image exceeded staged-state budget");
                return BarrierOutcome::Failed;
            }
        };
        #[cfg(feature = "cluster")]
        let leader_proof = if let Some(controller) = self.cluster_controller.as_ref() {
            let Some(proof) = self.checkpoint_leader_proofs.get(&attempt) else {
                tracing::error!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "cluster checkpoint lost the leader proof captured for Prepare"
                );
                return BarrierOutcome::Failed;
            };
            if !controller.proof_is_live(proof) {
                tracing::warn!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "leadership changed before checkpoint durable-tail handoff"
                );
                return BarrierOutcome::Failed;
            }
            self.checkpoint_leader_proofs.remove(&attempt)
        } else {
            None
        };
        let mut request = self.build_checkpoint_request();
        #[cfg(feature = "cluster")]
        let resume_certificate =
            assignment_fence
                .as_ref()
                .zip(leader_proof.as_ref())
                .map(|(fence, proof)| {
                    (
                        CertifiedCheckpointAttempt {
                            attempt,
                            assignment_digest: fence.digest(),
                            leader_proof: proof.clone(),
                        },
                        fence.clone(),
                    )
                });
        request.assignment_fence = assignment_fence;

        let in_flight = EpochInFlightGuard::claim(
            &self.checkpoint_in_flight,
            &self.staged_bytes,
            self.checkpoint_state_cap_bytes,
        );
        let tail = LeaderTail {
            _in_flight: in_flight,
            coordinator: Arc::clone(&self.coordinator),
            complete_tx: self.checkpoint_complete_tx.clone(),
            request,
            operator_state: Some(operator_state),
            operator_state_encoded_budget,
            mutable_operator_capture_guard: None,
            vnode_states,
            fan_out: source_checkpoints.clone(),
            local_watermark: self.checkpoint_watermark(),
            attempt,
            attempt_started,
            checkpoint_timeout: self.checkpoint_timeout,
            serialization_timeout: self.serialization_timeout,
            checkpoint_cleanup_timeout: self.checkpoint_cleanup_timeout,
            fault_on_failure: self.delivery_guarantee
                == laminar_connectors::connector::DeliveryGuarantee::ExactlyOnce,
            checkpoint_fault: Arc::clone(&self.checkpoint_fault),
            #[cfg(feature = "cluster")]
            controller: self.cluster_controller.clone(),
            #[cfg(feature = "cluster")]
            leader_proof,
            #[cfg(feature = "cluster")]
            quorum_timeout: self.quorum_timeout,
            #[cfg(feature = "cluster")]
            delta_rebase_needed: Arc::clone(&self.delta_rebase_needed),
        };
        if self.checkpoint_committable_sinks {
            Self::run_leader_tail(tail).await;
        } else {
            self.spawn_checkpoint_tail(Self::run_leader_tail(tail));

            #[cfg(feature = "cluster")]
            if let (Some(cc), Some((identity, assignment_fence))) =
                (self.cluster_controller.clone(), resume_certificate)
            {
                let has_shuffle = self.graph.cluster_shuffle_config().is_some();
                if let Err(error) = Self::wait_for_aligned_resume_until(
                    has_shuffle,
                    &cc,
                    identity,
                    &assignment_fence,
                    self.quorum_timeout,
                    attempt_deadline,
                )
                .await
                {
                    set_checkpoint_fault(&self.checkpoint_fault, error);
                }
            }
        }
        BarrierOutcome::Async
    }

    fn record_cycle(&self, events_ingested: u64, _batches: u64, elapsed_ns: u64) {
        let _ = events_ingested; // counted in extract_watermark
        self.prom.cycles.inc();
        #[allow(clippy::cast_precision_loss)]
        self.prom
            .cycle_duration
            .observe(elapsed_ns as f64 / 1_000_000_000.0);
    }

    fn note_cycle_error(&self) {
        self.prom.pipeline_cycle_errors_total.inc();
    }

    fn apply_control(&mut self, msg: crate::pipeline::ControlMsg) {
        apply_control_to_graph(&mut self.graph, msg);
    }

    fn is_backpressured(&self) -> bool {
        let bp = self.graph.input_buf_pressure() > 0.8;
        if bp {
            self.prom.cycles_backpressured.inc();
        }
        bp
    }

    fn intake_paused(&self) -> bool {
        if self.intake_gate.load(std::sync::atomic::Ordering::Acquire) {
            return true;
        }
        #[cfg(feature = "cluster")]
        if self
            .cluster_controller
            .as_ref()
            .is_some_and(|controller| !controller.process_lease_is_live())
        {
            return true;
        }
        false
    }

    fn reserve_subscription_cut(&self, attempt: CheckpointAttempt) -> Result<(), String> {
        if self.in_cluster() {
            return Ok(());
        }
        self.subscription_registry.reserve_cut(attempt)
    }

    fn abort_subscription_cut(&self, attempt: CheckpointAttempt) {
        if self.in_cluster() {
            return;
        }
        self.subscription_registry.abort_cut(attempt);
    }

    fn publish_barrier(&self, attempt: CheckpointAttempt) -> Result<(), String> {
        if self.in_cluster() {
            return Ok(());
        }
        self.subscription_registry.commit_cut(attempt)
    }

    fn invalidate_subscriptions(&self, reason: &str) {
        if !self.in_cluster() {
            self.subscription_registry.invalidate_all(reason);
        }
    }

    fn has_deferred_input(&self) -> bool {
        // In cluster mode, always return true so the coordinator runs `execute_cycle` each idle
        // tick; without this a follower with no local sources never drains the shuffle receiver.
        #[cfg(feature = "cluster")]
        {
            if self.cluster_controller.is_some() {
                return true;
            }
        }
        self.graph.has_pending_input()
    }

    async fn cancel_source_barrier_attempt(
        &mut self,
        attempt: CheckpointAttempt,
        reason: &str,
    ) -> Result<(), String> {
        #[cfg(feature = "cluster")]
        {
            let Some(controller) = self.cluster_controller.clone() else {
                let error = format!(
                    "follower checkpoint {} epoch {} has no cluster controller for cancellation",
                    attempt.checkpoint_id, attempt.epoch
                );
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(error);
            };
            if controller.is_leader() {
                return Err("leader cannot use follower source-barrier cancellation".into());
            }
            let Some(announcement) = self.pending_follower_checkpoint.as_ref() else {
                let error = format!(
                    "follower checkpoint {} epoch {} has no pending announcement to reject",
                    attempt.checkpoint_id, attempt.epoch
                );
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(error);
            };
            if CheckpointAttempt::new(announcement.epoch, announcement.checkpoint_id) != attempt {
                let error = format!(
                    "follower cancellation identity epoch={} id={} does not match pending epoch={} id={}",
                    attempt.epoch,
                    attempt.checkpoint_id,
                    announcement.epoch,
                    announcement.checkpoint_id
                );
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(error);
            }
            let announcement = announcement.clone();
            let barrier = laminar_core::checkpoint::CheckpointBarrier {
                checkpoint_id: announcement.checkpoint_id,
                epoch: announcement.epoch,
                flags: announcement.flags,
            };
            for control in &self.barrier_injectors {
                control.cancel_exact(barrier);
            }

            let Some(identity) = Self::certified_announcement(&announcement) else {
                let error = format!(
                    "follower checkpoint {} epoch {} lost its certified identity during cancellation",
                    attempt.checkpoint_id, attempt.epoch
                );
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(error);
            };
            let cleanup_deadline = tokio::time::Instant::now() + self.checkpoint_cleanup_timeout;
            Self::reject_follower_capture(
                Some(controller.as_ref()),
                self.checkpoint_fault.as_ref(),
                attempt,
                Some(identity.assignment_digest),
                reason.to_owned(),
                cleanup_deadline,
            )
            .await?;

            if self.graph.cluster_shuffle_config().is_some() {
                use laminar_core::cluster::control::Phase;

                let released = tokio::time::timeout_at(
                    cleanup_deadline,
                    controller.wait_for_barrier(
                        |candidate| {
                            let candidate_attempt =
                                CheckpointAttempt::new(candidate.epoch, candidate.checkpoint_id);
                            match candidate_attempt.relation_to(attempt) {
                                CheckpointAttemptRelation::Newer => true,
                                CheckpointAttemptRelation::Exact => candidate.phase == Phase::Abort,
                                CheckpointAttemptRelation::Older
                                | CheckpointAttemptRelation::Conflict => false,
                            }
                        },
                        self.checkpoint_cleanup_timeout,
                    ),
                )
                .await;
                let released = match released {
                    Ok(Ok(released)) => released,
                    Ok(Err(observation_error)) => {
                        let error = format!(
                            "follower checkpoint {} epoch {} cleanup observation failed; \
                             reservation remains fenced: {observation_error}",
                            attempt.checkpoint_id, attempt.epoch
                        );
                        set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                        return Err(error);
                    }
                    Err(_) => None,
                };
                if released.is_none() {
                    let error = format!(
                        "follower checkpoint {} epoch {} rejection was published but no Abort or newer epoch arrived before cleanup deadline",
                        attempt.checkpoint_id, attempt.epoch
                    );
                    set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                    return Err(error);
                }
            }

            self.release_follower_reservation(&identity);
            if self
                .pending_follower_checkpoint
                .as_ref()
                .is_some_and(|pending| {
                    pending.epoch == attempt.epoch && pending.checkpoint_id == attempt.checkpoint_id
                })
            {
                self.pending_follower_checkpoint = None;
                Ok(())
            } else {
                let error = format!(
                    "follower checkpoint {} epoch {} changed during exact cancellation",
                    attempt.checkpoint_id, attempt.epoch
                );
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                Err(error)
            }
        }
        #[cfg(not(feature = "cluster"))]
        {
            let _ = (attempt, reason);
            Ok(())
        }
    }

    fn set_barrier_injectors(
        &mut self,
        injectors: Vec<crate::pipeline::callback::SourceBarrierControl>,
    ) {
        #[cfg(feature = "cluster")]
        {
            self.barrier_injectors = injectors;
        }
        #[cfg(not(feature = "cluster"))]
        {
            let _ = injectors;
        }
    }
}

/// Rebuild a versioned lookup state over `batch`, reusing the prior state's key
/// and version columns. `None` if the version column is absent or the index
/// fails to build.
pub(crate) fn rebuild_versioned_state(
    prior: &laminar_sql::datafusion::VersionedLookupState,
    batch: RecordBatch,
) -> Option<laminar_sql::datafusion::VersionedLookupState> {
    let key_indices: Vec<usize> = prior
        .key_columns
        .iter()
        .filter_map(|k| batch.schema().index_of(k).ok())
        .collect();
    let version_col_idx = batch.schema().index_of(&prior.version_column).ok()?;
    let index = laminar_sql::datafusion::lookup_join_exec::VersionedIndex::build(
        &batch,
        &key_indices,
        version_col_idx,
        prior.max_versions_per_key,
    )
    .ok()?;
    Some(laminar_sql::datafusion::VersionedLookupState {
        batch,
        index: Arc::new(index),
        key_columns: prior.key_columns.clone(),
        version_column: prior.version_column.clone(),
        stream_time_column: prior.stream_time_column.clone(),
        max_versions_per_key: prior.max_versions_per_key,
    })
}

/// Encode an Arrow schema as a hex-encoded IPC flatbuffer.
pub(crate) fn encode_arrow_schema(schema: &arrow_schema::Schema) -> String {
    laminar_connectors::config::encode_arrow_schema_ipc(schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DrainingCaptureFailureOperator {
        live_rows: Arc<std::sync::atomic::AtomicUsize>,
        fail_whole_capture: Arc<std::sync::atomic::AtomicBool>,
        #[cfg(feature = "cluster")]
        fail_vnode_capture: Arc<std::sync::atomic::AtomicBool>,
    }

    #[async_trait::async_trait]
    impl crate::operator_graph::GraphOperator for DrainingCaptureFailureOperator {
        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(
            &mut self,
        ) -> Result<Option<crate::operator_graph::OperatorCheckpoint>, DbError> {
            if self
                .fail_whole_capture
                .load(std::sync::atomic::Ordering::Acquire)
            {
                self.live_rows
                    .store(0, std::sync::atomic::Ordering::Release);
                return Err(DbError::Checkpoint(
                    "injected failure after draining whole-operator state".into(),
                ));
            }
            Ok(None)
        }

        #[cfg(feature = "cluster")]
        fn checkpoint_by_vnode(
            &mut self,
            _vnode_count: u32,
        ) -> Result<
            Option<std::collections::HashMap<u32, crate::checkpoint_coordinator::StagedSlice>>,
            DbError,
        > {
            if self
                .fail_vnode_capture
                .load(std::sync::atomic::Ordering::Acquire)
            {
                self.live_rows
                    .store(0, std::sync::atomic::Ordering::Release);
                return Err(DbError::Checkpoint(
                    "injected failure after draining vnode state".into(),
                ));
            }
            Ok(None)
        }
    }

    struct DrainingCaptureOperator {
        live_rows: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl crate::operator_graph::GraphOperator for DrainingCaptureOperator {
        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(
            &mut self,
        ) -> Result<Option<crate::operator_graph::OperatorCheckpoint>, DbError> {
            let rows = self.live_rows.swap(0, std::sync::atomic::Ordering::AcqRel);
            Ok(Some(crate::operator_graph::OperatorCheckpoint {
                data: rows.to_le_bytes().to_vec(),
            }))
        }
    }

    #[cfg(feature = "cluster")]
    struct FollowerCheckpointEvidenceOperator;

    #[cfg(feature = "cluster")]
    #[async_trait::async_trait]
    impl crate::operator_graph::GraphOperator for FollowerCheckpointEvidenceOperator {
        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(
            &mut self,
        ) -> Result<Option<crate::operator_graph::OperatorCheckpoint>, DbError> {
            Ok(Some(crate::operator_graph::OperatorCheckpoint {
                data: b"follower-whole-operator-state".to_vec(),
            }))
        }
    }

    fn empty_callback_fixture() -> ConnectorPipelineCallback {
        let (_sink_event_tx, sink_event_rx) =
            laminar_core::streaming::channel::channel::<crate::sink_task::SinkEvent>(1);
        let (checkpoint_complete_tx, _checkpoint_complete_rx) =
            crossfire::mpsc::bounded_async::<crate::pipeline::CheckpointCompletion>(1);
        let mv_store = crate::mv_store::MvStore::new();
        let mv_store_has_any = mv_store.has_any_handle();
        ConnectorPipelineCallback {
            graph: crate::operator_graph::OperatorGraph::new(SessionContext::new()),
            stream_entries: Vec::new(),
            sinks: Vec::new(),
            owned_sink_handles: Arc::new(parking_lot::Mutex::new(Vec::new())),
            watermark_states: FxHashMap::default(),
            source_entries_for_wm: FxHashMap::default(),
            source_ids: FxHashMap::default(),
            source_name_arcs: FxHashMap::default(),
            source_wms_buf: FxHashMap::default(),
            tracker: None,
            prom: Arc::new(crate::engine_metrics::EngineMetrics::new(
                &prometheus::Registry::new(),
            )),
            pipeline_watermark: Arc::new(std::sync::atomic::AtomicI64::new(i64::MIN)),
            coordinator: Arc::new(tokio::sync::Mutex::new(None)),
            table_store: Arc::new(parking_lot::RwLock::new(
                crate::table_store::TableStore::new(),
            )),
            mv_store: Arc::new(parking_lot::RwLock::new(mv_store)),
            mv_store_has_any,
            filter_ctx: SessionContext::new(),
            compiled_sink_filters: Vec::new(),
            pending_sink_filter_compiles: 0,
            delivery_guarantee: laminar_connectors::connector::DeliveryGuarantee::AtLeastOnce,
            serialization_timeout: Duration::from_secs(1),
            checkpoint_state_cap_bytes: 512 * 1024 * 1024,
            checkpoint_serialization_gate: Arc::new(tokio::sync::Semaphore::new(1)),
            checkpoint_timeout: Duration::from_secs(1),
            checkpoint_cleanup_timeout: Duration::from_secs(1),
            sink_event_rx,
            sink_timed_out: false,
            sink_fault: None,
            checkpoint_fault: Arc::new(parking_lot::Mutex::new(None)),
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            #[cfg(feature = "cluster")]
            cluster_controller: None,
            #[cfg(feature = "cluster")]
            shuffle_delivery_loss_incidents: None,
            #[cfg(feature = "cluster")]
            shuffle_recovered_delivery_loss_incidents: None,
            #[cfg(feature = "cluster")]
            shuffle_delivery_loss_incidents_seen: 0,
            #[cfg(feature = "cluster")]
            vnode_registry: None,
            #[cfg(feature = "cluster")]
            reconciled_source_handoff_version: None,
            #[cfg(feature = "cluster")]
            follower_tail: Arc::default(),
            #[cfg(feature = "cluster")]
            barrier_injectors: Vec::new(),
            #[cfg(feature = "cluster")]
            pending_follower_checkpoint: None,
            #[cfg(feature = "cluster")]
            checkpoint_leader_proofs: FxHashMap::default(),
            subscription_registry: Arc::new(crate::subscription::SubscriptionRegistry::new()),
            named_stream_names: rustc_hash::FxHashSet::default(),
            checkpoint_complete_tx,
            checkpoint_tail_tasks: tokio::task::JoinSet::new(),
            checkpoint_in_flight: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            staged_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(feature = "cluster")]
            delta_rebase_needed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(feature = "cluster")]
            last_vnode_capture_epoch: None,
            epoch_allocator: None,
            #[cfg(feature = "cluster")]
            quorum_timeout: Duration::from_secs(1),
            checkpoint_committable_sinks: false,
            intake_gate: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    #[cfg(feature = "cluster")]
    struct ClusterCallbackFixture {
        callback: ConnectorPipelineCallback,
        source: Arc<crate::catalog::SourceEntry>,
    }

    #[cfg(feature = "cluster")]
    fn digest(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }

    #[cfg(feature = "cluster")]
    fn local_controller() -> Arc<laminar_core::cluster::control::ClusterController> {
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::{NodeId, NodeInfo};

        let node_id = NodeId(1);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
        Arc::new(laminar_core::cluster::control::ClusterController::new(
            node_id, kv, None, members_rx,
        ))
    }

    #[cfg(feature = "cluster")]
    fn local_assignment_fence(
        controller: &laminar_core::cluster::control::ClusterController,
        version: u64,
    ) -> laminar_core::checkpoint::CheckpointAssignmentFence {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

        CheckpointAssignmentFence::from_owner_map(
            version,
            &[1],
            vec![CheckpointParticipant {
                node_id: 1,
                boot_incarnation: controller.recovery_incarnation(),
            }],
        )
        .unwrap()
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn follower_capture_request_includes_whole_operator_graph_state() {
        let controller = local_controller();
        let assignment_fence = local_assignment_fence(&controller, 17);
        let mut callback = empty_callback_fixture();
        callback.graph.push_test_node(
            "follower-checkpoint-evidence",
            Box::new(FollowerCheckpointEvidenceOperator),
        );

        // This is the request builder used by both the no-source-barrier CaptureNow branch and
        // deferred source-barrier capture.
        let (mut request, operator_state) = callback
            .build_follower_checkpoint_request_until(
                &assignment_fence,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert!(
            request.operator_states.is_empty(),
            "aligned capture must defer serialization to the durable tail"
        );
        request.operator_states = operator_state
            .serialize_until(
                callback.checkpoint_state_cap_bytes,
                callback.serialization_timeout,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap()
            .accept_for_test();

        assert_eq!(request.assignment_fence.as_ref(), Some(&assignment_fence));
        let bytes = request
            .operator_states
            .get("operator_graph")
            .expect("whole operator graph must be present in the follower request");
        let graph_checkpoint =
            rkyv::from_bytes::<crate::operator_graph::GraphCheckpoint, rkyv::rancor::Error>(bytes)
                .unwrap();
        assert_eq!(
            graph_checkpoint
                .operators
                .get("follower-checkpoint-evidence")
                .map(Vec::as_slice),
            Some(b"follower-whole-operator-state".as_slice())
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn follower_capture_request_rejects_an_expired_deadline() {
        let controller = local_controller();
        let assignment_fence = local_assignment_fence(&controller, 18);
        let mut callback = empty_callback_fixture();

        let error = callback
            .build_follower_checkpoint_request_until(
                &assignment_fence,
                tokio::time::Instant::now() - Duration::from_millis(1),
            )
            .await
            .err()
            .expect("an expired deadline must reject follower capture");

        assert_eq!(
            error,
            "follower operator-state capture exhausted the checkpoint deadline"
        );
    }

    #[cfg(feature = "cluster")]
    fn committed_source_handoff(
        assignment_fence: laminar_core::checkpoint::CheckpointAssignmentFence,
        source_watermark: Option<i64>,
        cluster_watermark: CheckpointWatermark,
        recovery_watermark_frontier: Option<i64>,
    ) -> Arc<laminar_core::checkpoint::CommittedSourceHandoff> {
        use std::collections::BTreeMap;

        use laminar_core::checkpoint::{
            ClusterRecoveryCapsule, ParticipantRecoveryRef, PipelineIdentity,
            CLUSTER_RECOVERY_CAPSULE_VERSION, PIPELINE_IDENTITY_VERSION,
        };

        let source_watermarks = source_watermark
            .map(|watermark| BTreeMap::from([("orders".to_string(), watermark)]))
            .unwrap_or_default();
        let portable_state_sha256 = digest(6);
        let capsule = ClusterRecoveryCapsule {
            version: CLUSTER_RECOVERY_CAPSULE_VERSION,
            attempt: CheckpointAttempt::new(7, assignment_fence.assignment_version),
            deployment_id: "00000000-0000-0000-0000-000000000007".into(),
            pipeline_identity: PipelineIdentity {
                canonical_version: PIPELINE_IDENTITY_VERSION,
                sha256: digest(1),
            },
            participants: vec![ParticipantRecoveryRef {
                participant_id: 1,
                readiness_sha256: digest(3),
                manifest_sha256: digest(4),
                portable_state_sha256: portable_state_sha256.clone(),
            }],
            assignment_fence,
            seal_inventory_sha256: digest(2),
            source_offsets: BTreeMap::from([("orders".into(), BTreeMap::new())]),
            source_metadata: BTreeMap::from([("orders".into(), BTreeMap::new())]),
            source_assignment_versions: BTreeMap::new(),
            source_watermarks,
            cluster_watermark,
            recovery_watermark_frontier,
            portable_state_sha256,
        };
        Arc::new(laminar_core::checkpoint::CommittedSourceHandoff::try_from(&capsule).unwrap())
    }

    #[cfg(feature = "cluster")]
    fn cluster_callback_fixture(
        registry: Arc<laminar_core::state::VnodeRegistry>,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
        reconciled_source_handoff_version: Option<u64>,
        startup_watermark: Option<i64>,
    ) -> ClusterCallbackFixture {
        use arrow_schema::{DataType, Field, Schema, TimeUnit};
        use laminar_core::streaming::BackpressureStrategy;

        let catalog = crate::catalog::SourceCatalog::new(16, BackpressureStrategy::Block);
        let source = catalog
            .register_source(
                "orders",
                Arc::new(Schema::new(vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                )])),
                Some("ts".into()),
                Some(Duration::ZERO),
                None,
                None,
            )
            .unwrap();
        source
            .source
            .restore_watermark_for_recovery(startup_watermark.unwrap_or(i64::MIN));

        let mut generator = laminar_core::time::BoundedOutOfOrdernessGenerator::new(0);
        laminar_core::time::WatermarkGenerator::restore_watermark_for_recovery(
            &mut generator,
            startup_watermark.unwrap_or(i64::MIN),
        );
        let watermark_states = FxHashMap::from_iter([(
            "orders".into(),
            SourceWatermarkState {
                extractor: laminar_core::time::EventTimeExtractor::from_column("ts"),
                generator: Box::new(generator),
                column: "ts".into(),
            },
        )]);
        let source_entries_for_wm = FxHashMap::from_iter([("orders".into(), Arc::clone(&source))]);
        let source_ids = FxHashMap::from_iter([("orders".into(), 0)]);
        let source_name_arcs = FxHashMap::from_iter([(0, Arc::<str>::from("orders"))]);
        let mut tracker = laminar_core::time::WatermarkTracker::new(1);
        tracker
            .restore_for_recovery(&[startup_watermark], &[false], startup_watermark)
            .unwrap();

        let (_sink_event_tx, sink_event_rx) =
            laminar_core::streaming::channel::channel::<crate::sink_task::SinkEvent>(1);
        let (checkpoint_complete_tx, _checkpoint_complete_rx) =
            crossfire::mpsc::bounded_async::<crate::pipeline::CheckpointCompletion>(1);
        let mv_store = crate::mv_store::MvStore::new();
        let mv_store_has_any = mv_store.has_any_handle();
        let pipeline_watermark = Arc::new(std::sync::atomic::AtomicI64::new(
            startup_watermark.unwrap_or(i64::MIN),
        ));

        ClusterCallbackFixture {
            callback: ConnectorPipelineCallback {
                graph: crate::operator_graph::OperatorGraph::new(SessionContext::new()),
                stream_entries: Vec::new(),
                sinks: Vec::new(),
                owned_sink_handles: Arc::new(parking_lot::Mutex::new(Vec::new())),
                watermark_states,
                source_entries_for_wm,
                source_ids,
                source_name_arcs,
                source_wms_buf: FxHashMap::default(),
                tracker: Some(tracker),
                prom: Arc::new(crate::engine_metrics::EngineMetrics::new(
                    &prometheus::Registry::new(),
                )),
                pipeline_watermark,
                coordinator: Arc::new(tokio::sync::Mutex::new(None)),
                table_store: Arc::new(parking_lot::RwLock::new(
                    crate::table_store::TableStore::new(),
                )),
                mv_store: Arc::new(parking_lot::RwLock::new(mv_store)),
                mv_store_has_any,
                filter_ctx: SessionContext::new(),
                compiled_sink_filters: Vec::new(),
                pending_sink_filter_compiles: 0,
                delivery_guarantee: laminar_connectors::connector::DeliveryGuarantee::AtLeastOnce,
                serialization_timeout: Duration::from_secs(1),
                checkpoint_state_cap_bytes: 512 * 1024 * 1024,
                checkpoint_serialization_gate: Arc::new(tokio::sync::Semaphore::new(1)),
                checkpoint_timeout: Duration::from_secs(1),
                checkpoint_cleanup_timeout: Duration::from_secs(1),
                sink_event_rx,
                sink_timed_out: false,
                sink_fault: None,
                checkpoint_fault: Arc::new(parking_lot::Mutex::new(None)),
                shutdown_signal: Arc::new(tokio::sync::Notify::new()),
                cluster_controller: Some(controller),
                shuffle_delivery_loss_incidents: None,
                shuffle_recovered_delivery_loss_incidents: None,
                shuffle_delivery_loss_incidents_seen: 0,
                vnode_registry: Some(registry),
                reconciled_source_handoff_version,
                follower_tail: Arc::default(),
                barrier_injectors: Vec::new(),
                pending_follower_checkpoint: None,
                checkpoint_leader_proofs: FxHashMap::default(),
                subscription_registry: Arc::new(crate::subscription::SubscriptionRegistry::new()),
                named_stream_names: rustc_hash::FxHashSet::default(),
                checkpoint_complete_tx,
                checkpoint_tail_tasks: tokio::task::JoinSet::new(),
                checkpoint_in_flight: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                staged_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                delta_rebase_needed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                last_vnode_capture_epoch: None,
                epoch_allocator: None,
                quorum_timeout: Duration::from_secs(1),
                checkpoint_committable_sinks: false,
                intake_gate: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            },
            source,
        }
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn rebuilt_callback_does_not_replay_a_carried_old_source_handoff() {
        use laminar_core::state::{NodeId, VnodeRegistry};

        let controller = local_controller();
        let registry = Arc::new(VnodeRegistry::new_unassigned(1));
        let version_one_fence = local_assignment_fence(&controller, 1);
        registry.set_assignment_and_version_with_source_handoff(
            vec![NodeId(1)].into(),
            1,
            committed_source_handoff(
                version_one_fence,
                Some(1_000),
                CheckpointWatermark::Active(1_000),
                Some(1_000),
            ),
        );
        registry.set_assignment_and_version_carrying_source_handoff(vec![NodeId(1)].into(), 2);

        let version_two_fence = local_assignment_fence(&controller, 2);
        controller.publish_checkpoint_assignment_fence(Some(version_two_fence));
        let mut fixture = cluster_callback_fixture(
            Arc::clone(&registry),
            Arc::clone(&controller),
            Some(1),
            Some(2_000),
        );

        fixture
            .callback
            .reconcile_source_handoff_watermarks()
            .unwrap();

        assert_eq!(fixture.callback.reconciled_source_handoff_version, Some(1));
        assert_eq!(
            fixture
                .callback
                .tracker
                .as_ref()
                .unwrap()
                .source_watermark(0),
            Some(2_000)
        );
        assert_eq!(
            fixture.callback.watermark_states["orders"]
                .generator
                .current_watermark(),
            2_000
        );
        assert_eq!(fixture.source.source.current_watermark(), 2_000);
        assert_eq!(
            fixture
                .callback
                .pipeline_watermark
                .load(std::sync::atomic::Ordering::Acquire),
            2_000
        );

        let version_three_fence = local_assignment_fence(&controller, 3);
        registry.set_assignment_and_version_with_source_handoff(
            vec![NodeId(1)].into(),
            3,
            committed_source_handoff(
                version_three_fence.clone(),
                Some(2_500),
                CheckpointWatermark::Active(2_500),
                Some(2_500),
            ),
        );
        controller.publish_checkpoint_assignment_fence(Some(version_three_fence));

        fixture
            .callback
            .reconcile_source_handoff_watermarks()
            .unwrap();

        assert_eq!(fixture.callback.reconciled_source_handoff_version, Some(3));
        assert_eq!(
            fixture
                .callback
                .tracker
                .as_ref()
                .unwrap()
                .source_watermark(0),
            Some(2_500)
        );
        assert_eq!(
            fixture.callback.watermark_states["orders"]
                .generator
                .current_watermark(),
            2_500
        );
        assert_eq!(fixture.source.source.current_watermark(), 2_500);
        assert_eq!(
            fixture
                .callback
                .pipeline_watermark
                .load(std::sync::atomic::Ordering::Acquire),
            2_500
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn active_handoff_frontier_fills_a_missing_source_watermark() {
        use arrow_array::TimestampMillisecondArray;
        use arrow_schema::{DataType, Field, Schema, TimeUnit};
        use laminar_core::state::{NodeId, VnodeRegistry};

        let controller = local_controller();
        controller.publish_cluster_min_watermark(1_500);
        let registry = Arc::new(VnodeRegistry::new_unassigned(1));
        let fence = local_assignment_fence(&controller, 1);
        registry.set_assignment_and_version_with_source_handoff(
            vec![NodeId(1)].into(),
            1,
            committed_source_handoff(
                fence.clone(),
                None,
                CheckpointWatermark::Active(1_500),
                Some(1_500),
            ),
        );
        controller.publish_checkpoint_assignment_fence(Some(fence));
        let mut fixture =
            cluster_callback_fixture(registry, Arc::clone(&controller), None, Some(2_000));

        fixture
            .callback
            .reconcile_source_handoff_watermarks()
            .unwrap();

        let tracker = fixture.callback.tracker.as_ref().unwrap();
        assert_eq!(tracker.source_watermark(0), Some(1_500));
        assert_eq!(tracker.current_watermark().unwrap().timestamp(), 1_500);
        assert_eq!(
            fixture.callback.watermark_states["orders"]
                .generator
                .current_watermark(),
            1_500
        );
        assert_eq!(fixture.source.source.current_watermark(), 1_500);
        assert_eq!(
            fixture
                .callback
                .pipeline_watermark
                .load(std::sync::atomic::Ordering::Acquire),
            1_500
        );

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            )])),
            vec![Arc::new(TimestampMillisecondArray::from(vec![
                1_400, 1_700,
            ]))],
        )
        .unwrap();
        let retained = crate::pipeline::PipelineCallback::filter_late_rows(
            &fixture.callback,
            "orders",
            &batch,
        )
        .expect("the post-frontier row must survive");
        assert_eq!(retained.num_rows(), 1);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn idle_handoff_restores_its_retained_frontier_on_a_fresh_controller() {
        use laminar_core::state::{NodeId, VnodeRegistry};

        let controller = local_controller();
        assert_eq!(controller.cluster_min_watermark(), None);
        let registry = Arc::new(VnodeRegistry::new_unassigned(1));
        let fence = local_assignment_fence(&controller, 1);
        registry.set_assignment_and_version_with_source_handoff(
            vec![NodeId(1)].into(),
            1,
            committed_source_handoff(fence.clone(), None, CheckpointWatermark::Idle, Some(1_500)),
        );
        controller.publish_checkpoint_assignment_fence(Some(fence));
        let mut fixture =
            cluster_callback_fixture(registry, Arc::clone(&controller), None, Some(2_000));

        fixture
            .callback
            .reconcile_source_handoff_watermarks()
            .unwrap();

        let tracker = fixture.callback.tracker.as_ref().unwrap();
        assert_eq!(tracker.source_watermark(0), Some(1_500));
        assert_eq!(tracker.current_watermark().unwrap().timestamp(), 1_500);
        assert!(tracker.is_idle(0));
        assert_eq!(
            fixture.callback.watermark_states["orders"]
                .generator
                .current_watermark(),
            1_500
        );
        assert_eq!(fixture.source.source.current_watermark(), 1_500);
        assert_eq!(
            fixture
                .callback
                .pipeline_watermark
                .load(std::sync::atomic::Ordering::Acquire),
            1_500
        );
        assert_eq!(
            controller.cluster_min_watermark(),
            None,
            "reconciliation must use the durable handoff frontier, not controller residue"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn checkpoint_capture_rejects_an_installed_unreconciled_source_handoff() {
        use laminar_core::state::{NodeId, VnodeRegistry};

        let controller = local_controller();
        let registry = Arc::new(VnodeRegistry::new_unassigned(1));
        let fence = local_assignment_fence(&controller, 1);
        registry.set_assignment_and_version_with_source_handoff(
            vec![NodeId(1)].into(),
            1,
            committed_source_handoff(
                fence.clone(),
                Some(1_500),
                CheckpointWatermark::Active(1_500),
                Some(1_500),
            ),
        );
        controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
        let fixture = cluster_callback_fixture(registry, controller, None, None);

        let error = fixture
            .callback
            .validate_checkpoint_assignment(Some(&fence))
            .unwrap_err();

        assert!(error.contains("[LDB-6055]"), "unexpected error: {error}");
        assert!(
            error.contains("source handoff was not installed before checkpoint capture"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn checkpoint_watermark_distinguishes_idle_from_uninitialized_inputs() {
        assert_eq!(
            classify_checkpoint_watermark(0, 0, i64::MIN),
            CheckpointWatermark::Idle
        );
        assert_eq!(
            classify_checkpoint_watermark(2, 0, 10),
            CheckpointWatermark::Idle
        );
        assert_eq!(
            classify_checkpoint_watermark(2, 2, i64::MIN),
            CheckpointWatermark::Uninitialized
        );
        assert_eq!(
            classify_checkpoint_watermark(2, 1, 10),
            CheckpointWatermark::Active(10)
        );
    }

    #[cfg(feature = "cluster")]
    fn assignment_fence(
        version: u64,
        participants: &[u64],
    ) -> laminar_core::checkpoint::CheckpointAssignmentFence {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

        let participants = participants
            .iter()
            .map(|node_id| CheckpointParticipant {
                node_id: *node_id,
                boot_incarnation: format!("00000000-0000-0000-0000-{node_id:012x}")
                    .parse()
                    .unwrap(),
            })
            .collect::<Vec<_>>();
        let owners = participants
            .iter()
            .map(|participant| participant.node_id)
            .collect::<Vec<_>>();
        CheckpointAssignmentFence::from_owner_map(version, &owners, participants).unwrap()
    }

    #[cfg(feature = "cluster")]
    fn leader_proof(fencing_token: u64) -> laminar_core::cluster::control::LeaderProof {
        laminar_core::cluster::control::LeaderProof {
            owner: laminar_core::cluster::control::LeaderProofOwner {
                node_id: 1,
                boot_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
                process_term: 1,
            },
            fencing_token,
        }
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn allocator_epoch_jump_requires_full_vnode_capture() {
        assert!(vnode_capture_requires_full_rebase(None, 1));
        assert!(!vnode_capture_requires_full_rebase(Some(7), 8));
        assert!(vnode_capture_requires_full_rebase(Some(7), 1_000));
        assert!(vnode_capture_requires_full_rebase(Some(u64::MAX), 0));
    }

    #[tokio::test]
    async fn destructive_operator_capture_failure_faults_at_least_once_runtime() {
        let live_rows = Arc::new(std::sync::atomic::AtomicUsize::new(17));
        let fail_whole_capture = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut callback = empty_callback_fixture();
        callback.graph.push_test_node(
            "draining-capture",
            Box::new(DrainingCaptureFailureOperator {
                live_rows: Arc::clone(&live_rows),
                fail_whole_capture,
                #[cfg(feature = "cluster")]
                fail_vnode_capture: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            }),
        );

        let error = callback
            .capture_and_serialize_operator_state()
            .await
            .unwrap_err();

        assert_eq!(live_rows.load(std::sync::atomic::Ordering::Acquire), 0);
        assert!(error.contains("recovery from the last committed checkpoint is required"));
        let fault = crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback)
            .expect("mutable capture failure must become a runtime fault");
        assert_eq!(fault, error);
    }

    #[tokio::test]
    async fn failure_after_destructive_operator_capture_faults_runtime() {
        let live_rows = Arc::new(std::sync::atomic::AtomicUsize::new(17));
        let mut callback = empty_callback_fixture();
        callback.checkpoint_state_cap_bytes = 1;
        callback.graph.push_test_node(
            "draining-capture",
            Box::new(DrainingCaptureOperator {
                live_rows: Arc::clone(&live_rows),
            }),
        );

        let error = callback
            .capture_and_serialize_operator_state()
            .await
            .unwrap_err();

        assert_eq!(live_rows.load(std::sync::atomic::Ordering::Acquire), 0);
        assert!(error.contains("recovery from the last committed checkpoint is required"));
        assert!(error.contains("staged-state cap"));
        let fault = crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback)
            .expect("a post-capture failure must become a runtime fault");
        assert_eq!(fault, error);
    }

    #[tokio::test]
    async fn busy_serialization_gate_times_out_before_mutating_operator_state() {
        let live_rows = Arc::new(std::sync::atomic::AtomicUsize::new(17));
        let mut callback = empty_callback_fixture();
        callback.serialization_timeout = Duration::from_millis(10);
        callback.graph.push_test_node(
            "draining-capture",
            Box::new(DrainingCaptureOperator {
                live_rows: Arc::clone(&live_rows),
            }),
        );
        let held_permit = Arc::clone(&callback.checkpoint_serialization_gate)
            .acquire_owned()
            .await
            .unwrap();

        let error = callback
            .capture_operator_state_until(tokio::time::Instant::now() + Duration::from_millis(100))
            .await
            .err()
            .expect("a busy serialization gate must reject a second capture");

        assert!(error.contains("[LDB-6017]"), "{error}");
        assert_eq!(live_rows.load(std::sync::atomic::Ordering::Acquire), 17);
        assert!(
            crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback).is_none(),
            "waiting for an earlier encoder must fail before mutable capture"
        );
        drop(held_permit);
    }

    #[tokio::test]
    async fn dropping_serialized_destructive_image_before_commit_faults_runtime() {
        let live_rows = Arc::new(std::sync::atomic::AtomicUsize::new(17));
        let mut callback = empty_callback_fixture();
        callback.graph.push_test_node(
            "draining-capture",
            Box::new(DrainingCaptureOperator {
                live_rows: Arc::clone(&live_rows),
            }),
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let capture = callback
            .capture_operator_state_until(deadline)
            .await
            .unwrap();
        let budget = encoded_operator_state_budget(
            callback.checkpoint_state_cap_bytes,
            capture.estimated_bytes(),
            0,
        )
        .unwrap();
        let serialized = capture
            .serialize_until(budget, callback.serialization_timeout, deadline)
            .await
            .unwrap();

        assert_eq!(live_rows.load(std::sync::atomic::Ordering::Acquire), 0);
        drop(serialized);
        let fault = crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback)
            .expect("an uncommitted destructive image must require recovery");
        assert!(fault.contains("recovery from the last committed checkpoint is required"));
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn destructive_vnode_capture_failure_faults_runtime() {
        let live_rows = Arc::new(std::sync::atomic::AtomicUsize::new(23));
        let mut callback = empty_callback_fixture();
        callback.graph.set_test_vnode_count(4);
        callback.graph.push_test_node(
            "draining-vnode-capture",
            Box::new(DrainingCaptureFailureOperator {
                live_rows: Arc::clone(&live_rows),
                fail_whole_capture: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                fail_vnode_capture: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            }),
        );

        let error = callback.capture_vnode_states(41).unwrap_err();

        assert_eq!(live_rows.load(std::sync::atomic::Ordering::Acquire), 0);
        assert!(error.contains("recovery from the last committed checkpoint is required"));
        let fault = crate::pipeline::PipelineCallback::take_pipeline_fault(&mut callback)
            .expect("mutable vnode capture failure must become a runtime fault");
        assert_eq!(fault, error);
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn shuffle_delivery_loss_incidents_are_forgiven_only_after_recovery_completion() {
        let mut seen = 0;

        assert_eq!(
            observe_unrecovered_delivery_loss_incidents(2, 0, &mut seen, true),
            None,
            "an active rewind must not re-fault the replacement callback"
        );
        assert_eq!(seen, 0, "an incomplete rewind must not hide the loss");

        assert_eq!(
            observe_unrecovered_delivery_loss_incidents(2, 2, &mut seen, false),
            None,
            "the completed rewind covers its captured loss cutoff"
        );
        assert_eq!(seen, 2);

        assert_eq!(
            observe_unrecovered_delivery_loss_incidents(3, 2, &mut seen, false),
            Some(1),
            "loss after the captured cutoff still requires recovery"
        );
        assert_eq!(seen, 3);
    }

    #[test]
    fn ambiguous_decision_faults_at_least_once_pipeline() {
        let result = crate::checkpoint_coordinator::CheckpointResult {
            success: false,
            checkpoint_id: 22,
            epoch: 7,
            duration: std::time::Duration::ZERO,
            error: Some("decision outcome is in doubt".into()),
            failure_disposition: Some(
                crate::checkpoint_coordinator::CheckpointFailureDisposition::RequiresRecovery,
            ),
        };

        assert!(checkpoint_failure_requires_pipeline_fault(&result, false));
    }
    use crate::error::DbError;

    #[test]
    fn source_checkpoint_map_materializes_offsets_and_metadata() {
        use laminar_connectors::checkpoint::PersistentOffset;

        let mut inventory = PersistentOffset::new("[", ",", "]");
        inventory.push_fragment(r#""first.parquet""#);
        inventory.push_fragment(r#""second.parquet""#);
        let mut files = SourceCheckpoint::new();
        files.set_offset("row", "17");
        files.set_persistent_offset("manifest", inventory);
        files.set_metadata("connector", "file");
        files.set_metadata("schema_sha256", "abc123");

        let mut kafka = SourceCheckpoint::new();
        kafka.set_offset("orders:0", "42");
        let mut snapshots = FxHashMap::default();
        snapshots.insert("files".to_string(), files.clone());
        snapshots.insert("orders".to_string(), kafka);

        let materialized = materialize_source_checkpoint_map(snapshots);
        let files_durable = materialized.get("files").expect("files checkpoint");
        assert_eq!(
            files_durable.offsets.get("manifest").map(String::as_str),
            Some(r#"["first.parquet","second.parquet"]"#)
        );
        assert_eq!(
            files_durable.offsets.get("row").map(String::as_str),
            Some("17")
        );
        assert_eq!(
            files_durable
                .metadata
                .get("schema_sha256")
                .map(String::as_str),
            Some("abc123")
        );
        assert_eq!(
            materialized
                .get("orders")
                .and_then(|checkpoint| checkpoint.offsets.get("orders:0"))
                .map(String::as_str),
            Some("42")
        );
    }

    #[tokio::test]
    async fn source_checkpoint_materialization_rejects_expired_deadline() {
        let attempt = CheckpointAttempt::new(7, 11);
        let result = materialize_source_checkpoints_until(
            FxHashMap::default(),
            attempt,
            tokio::time::Instant::now(),
        )
        .await;
        let Err(error) = result else {
            panic!("an expired absolute deadline must reject materialization");
        };
        assert!(error.contains("checkpoint 11 epoch 7"));
        assert!(error.contains("before source-offset materialization"));
    }

    #[test]
    fn mv_state_is_retained_when_graph_has_no_snapshot() {
        let mv_bytes = bytes::Bytes::from_static(b"materialized-view-state");
        let states = combine_operator_checkpoint_states(
            None,
            [("mv:test_view".to_string(), mv_bytes.clone())],
        );

        assert_eq!(states.get("mv:test_view"), Some(&mv_bytes));
        assert!(!states.contains_key("operator_graph"));
    }

    #[tokio::test]
    async fn test_backpressure_fail_notifies_shutdown() {
        use crate::pipeline::CycleError;
        let notify = Arc::new(tokio::sync::Notify::new());
        let err = DbError::BackpressureFail("downstream of 'q'".into());
        let mapped = ConnectorPipelineCallback::map_graph_error(&err, &notify);
        assert!(
            matches!(&mapped, CycleError::Halt(m) if m.contains("Backpressure fail")),
            "unexpected: {mapped:?}"
        );

        tokio::time::timeout(Duration::from_millis(50), notify.notified())
            .await
            .expect("shutdown should have been notified");
    }

    #[tokio::test]
    async fn terminal_shuffle_routing_halts_and_notifies_shutdown() {
        use crate::pipeline::CycleError;
        let notify = Arc::new(tokio::sync::Notify::new());
        let error = DbError::ShuffleTerminal("oversized routed row".into());

        let mapped = ConnectorPipelineCallback::map_graph_error(&error, &notify);

        assert!(matches!(
            &mapped,
            CycleError::Halt(message) if message.contains("oversized routed row")
        ));
        tokio::time::timeout(Duration::from_millis(50), notify.notified())
            .await
            .expect("terminal routing must notify shutdown");
    }

    #[tokio::test]
    async fn partial_shuffle_send_requires_recovery_without_shutdown() {
        use crate::pipeline::CycleError;
        let notify = Arc::new(tokio::sync::Notify::new());
        let error = DbError::ShufflePartialSend("peer accepted an earlier frame".into());

        let mapped = ConnectorPipelineCallback::map_graph_error(&error, &notify);

        assert!(matches!(
            &mapped,
            CycleError::Recovery(message) if message.contains("peer accepted an earlier frame")
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), notify.notified())
                .await
                .is_err(),
            "recovery is coordinator-owned and must not signal terminal shutdown"
        );
    }

    #[tokio::test]
    async fn checkpoint_drain_preserves_terminal_shuffle_halt() {
        use crate::pipeline::CycleError;
        let notify = Arc::new(tokio::sync::Notify::new());
        let error = DbError::ShuffleTerminal("invalid routing structure".into());

        let mapped = ConnectorPipelineCallback::map_checkpoint_drain_error(&error, &notify);

        assert!(matches!(
            &mapped,
            CycleError::Halt(message)
                if message.contains("checkpoint graph drain halted")
                    && message.contains("invalid routing structure")
        ));
        tokio::time::timeout(Duration::from_millis(50), notify.notified())
            .await
            .expect("checkpoint drain terminal error must notify shutdown");
    }

    #[tokio::test]
    async fn test_non_backpressure_error_does_not_notify() {
        use crate::pipeline::CycleError;
        let notify = Arc::new(tokio::sync::Notify::new());
        let err = DbError::Pipeline("unrelated".into());
        let mapped = ConnectorPipelineCallback::map_graph_error(&err, &notify);
        assert!(
            matches!(mapped, CycleError::Fatal(_)),
            "non-Fail errors must classify as Fatal"
        );

        let got = tokio::time::timeout(Duration::from_millis(50), notify.notified()).await;
        assert!(got.is_err(), "non-Fail errors must not trigger shutdown");
    }

    #[tokio::test]
    async fn vnode_rehydration_checkpoint_error_requires_whole_graph_recovery() {
        use crate::pipeline::CycleError;
        let notify = Arc::new(tokio::sync::Notify::new());
        let error = DbError::Checkpoint("vnode apply failed after a partial graph swap".into());

        let mapped = ConnectorPipelineCallback::map_graph_error(&error, &notify);

        assert!(matches!(mapped, CycleError::Recovery(_)));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), notify.notified())
                .await
                .is_err(),
            "the coordinator owns recovery; this is not a graceful shutdown"
        );
    }

    #[tokio::test]
    async fn reserved_attempt_cleanup_deadline_includes_coordinator_lock() {
        let coordinator = tokio::sync::Mutex::new(None);
        let lock = coordinator.lock().await;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
        let started = std::time::Instant::now();

        let error = cleanup_reserved_attempt_until(
            &coordinator,
            CheckpointAttempt::new(7, 11),
            "injected admission failure".into(),
            None,
            None,
            deadline,
        )
        .await
        .unwrap_err();

        assert!(error.contains("cleanup exceeded its end-to-end deadline"));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "coordinator lock contention must not refresh or bypass the cleanup deadline"
        );
        drop(lock);
    }

    /// Rejected must drop, not passthrough.
    #[test]
    fn rejected_filter_dispatches_to_drop_not_passthrough() {
        let filters = [SinkFilter::Rejected];
        let dispatch = match filters.first().cloned() {
            Some(SinkFilter::Compiled(phys)) => SinkFilterDispatch::Compiled(phys),
            Some(SinkFilter::Rejected) => SinkFilterDispatch::Rejected,
            Some(SinkFilter::Pending) | None => SinkFilterDispatch::None,
        };
        assert!(
            matches!(dispatch, SinkFilterDispatch::Rejected),
            "Rejected filter must map to Rejected dispatch (drop), not None (passthrough)"
        );
    }

    /// Pending / absent → no filter (compilation runs before the dispatch loop).
    #[test]
    fn pending_and_absent_filters_dispatch_to_passthrough() {
        for filter in [Some(SinkFilter::Pending), None] {
            let dispatch = match filter.clone() {
                Some(SinkFilter::Compiled(phys)) => SinkFilterDispatch::Compiled(phys),
                Some(SinkFilter::Rejected) => SinkFilterDispatch::Rejected,
                Some(SinkFilter::Pending) | None => SinkFilterDispatch::None,
            };
            assert!(matches!(dispatch, SinkFilterDispatch::None));
        }
    }

    /// A clustered runtime cannot finalize event time before its first
    /// globally committed frontier.
    #[cfg(feature = "cluster")]
    #[test]
    fn cap_source_watermarks_freezes_before_first_cluster_commit() {
        let mut wms: FxHashMap<Arc<str>, i64> = FxHashMap::default();
        wms.insert(Arc::from("a"), 1_000);
        wms.insert(Arc::from("b"), 500);

        ConnectorPipelineCallback::cap_source_watermarks_by_cluster_min(&mut wms, None);

        assert_eq!(wms.get(&Arc::<str>::from("a")).copied(), Some(i64::MIN));
        assert_eq!(wms.get(&Arc::<str>::from("b")).copied(), Some(i64::MIN));
    }

    /// When a cluster-wide minimum is published, sources that have
    /// advanced past it get pulled back to it; sources at or below
    /// the cap are left alone (cap must not push watermarks forward).
    #[cfg(feature = "cluster")]
    #[test]
    fn cap_source_watermarks_lowers_only_sources_above_cluster_min() {
        let mut wms: FxHashMap<Arc<str>, i64> = FxHashMap::default();
        wms.insert(Arc::from("ahead"), 2_000);
        wms.insert(Arc::from("at"), 1_500);
        wms.insert(Arc::from("behind"), 800);

        ConnectorPipelineCallback::cap_source_watermarks_by_cluster_min(&mut wms, Some(1_500));

        assert_eq!(
            wms.get(&Arc::<str>::from("ahead")).copied(),
            Some(1_500),
            "source above cluster min must be capped down",
        );
        assert_eq!(
            wms.get(&Arc::<str>::from("at")).copied(),
            Some(1_500),
            "source at cluster min unchanged",
        );
        assert_eq!(
            wms.get(&Arc::<str>::from("behind")).copied(),
            Some(800),
            "source below cluster min must NOT be advanced by the cap",
        );
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn committed_cluster_watermark_keeps_replayed_rows_from_a_faster_local_frontier() {
        use arrow_array::TimestampMillisecondArray;
        use arrow_schema::{DataType, Field, Schema, TimeUnit};

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            )])),
            vec![Arc::new(TimestampMillisecondArray::from(vec![
                1_400, 1_700,
            ]))],
        )
        .unwrap();
        let effective = ConnectorPipelineCallback::cap_watermark_by_cluster_min(2_000, Some(1_500));
        let retained = filter_late_rows(&batch, "ts", effective)
            .unwrap()
            .expect("the row after the committed frontier must survive replay");

        assert_eq!(effective, 1_500);
        assert_eq!(retained.num_rows(), 1);
    }

    #[cfg(feature = "cluster")]
    fn follower_identity(epoch: u64, checkpoint_id: u64, digest: u8) -> CertifiedCheckpointAttempt {
        CertifiedCheckpointAttempt {
            attempt: CheckpointAttempt::new(epoch, checkpoint_id),
            assignment_digest: [digest; 32],
            leader_proof: leader_proof(1),
        }
    }

    /// Failure releases an attempt for an exact retry but permanently binds that retained epoch
    /// to its first checkpoint/certificate identity.
    #[cfg(feature = "cluster")]
    #[test]
    fn follower_admission_allows_only_exact_retry_after_failure() {
        let state = FollowerTailState::default();
        let exact = follower_identity(5, 50, 1);
        let wrong_checkpoint = follower_identity(5, 51, 1);
        let wrong_certificate = follower_identity(5, 50, 2);
        let mut wrong_authority = exact.clone();
        wrong_authority.leader_proof = leader_proof(2);

        assert_eq!(
            state.reserve(exact.clone()),
            Ok(FollowerAdmission::Reserved)
        );
        assert_eq!(state.reserve(exact.clone()), Ok(FollowerAdmission::Covered));
        assert!(state.reserve(wrong_checkpoint.clone()).is_err());
        assert!(state.reserve(wrong_certificate.clone()).is_err());
        assert!(state.reserve(wrong_authority.clone()).is_err());

        assert_eq!(state.finish(&exact, false), Ok(()));
        assert_eq!(
            state.reserve(exact.clone()),
            Ok(FollowerAdmission::Reserved)
        );
        assert!(state.reserve(wrong_checkpoint).is_err());
        assert!(state.reserve(wrong_certificate).is_err());
        assert!(state.reserve(wrong_authority).is_err());

        assert_eq!(state.finish(&exact, false), Ok(()));
        let newer = follower_identity(6, 60, 1);
        assert_eq!(
            state.reserve(newer.clone()),
            Ok(FollowerAdmission::Reserved)
        );
        assert_eq!(state.finish(&newer, false), Ok(()));
        assert_eq!(
            state.reserve(exact),
            Ok(FollowerAdmission::Covered),
            "an exact retry becomes stale after a newer epoch is observed"
        );
    }

    /// An atomic commit leaves no admission window in which the same attempt can be reserved a
    /// second time: admission sees either the active reservation or its committed terminal.
    #[cfg(feature = "cluster")]
    #[test]
    fn follower_commit_and_admission_are_atomic() {
        let state = Arc::new(FollowerTailState::default());
        let identity = follower_identity(8, 80, 3);
        assert_eq!(
            state.reserve(identity.clone()),
            Ok(FollowerAdmission::Reserved)
        );

        let start = Arc::new(std::sync::Barrier::new(3));
        let finishing_state = Arc::clone(&state);
        let finishing_start = Arc::clone(&start);
        let finishing_identity = identity.clone();
        let finish = std::thread::spawn(move || {
            finishing_start.wait();
            finishing_state.finish(&finishing_identity, true)
        });
        let admitting_state = Arc::clone(&state);
        let admitting_start = Arc::clone(&start);
        let admitting_identity = identity.clone();
        let admission = std::thread::spawn(move || {
            admitting_start.wait();
            admitting_state.reserve(admitting_identity)
        });
        start.wait();

        assert_eq!(finish.join().unwrap(), Ok(()));
        assert_eq!(admission.join().unwrap(), Ok(FollowerAdmission::Covered));
        assert_eq!(state.committed(), Some(identity));
        assert!(state.in_flight().is_empty());
    }

    /// Multiple durable tails retain independent slots; a newer admission never overwrites an
    /// older tail, and either tail may finish first.
    #[cfg(feature = "cluster")]
    #[test]
    fn follower_tail_tracks_multiple_in_flight_identities() {
        let state = FollowerTailState::default();
        let five = follower_identity(5, 50, 1);
        let seven = follower_identity(7, 70, 1);

        assert_eq!(state.reserve(five.clone()), Ok(FollowerAdmission::Reserved));
        assert_eq!(
            state.reserve(seven.clone()),
            Ok(FollowerAdmission::Reserved)
        );
        assert_eq!(state.in_flight(), vec![five.clone(), seven.clone()]);

        assert_eq!(state.finish(&seven, true), Ok(()));
        assert_eq!(state.in_flight(), vec![five.clone()]);
        assert_eq!(state.finish(&five, true), Ok(()));
        assert!(state.in_flight().is_empty());
        assert_eq!(
            state.committed(),
            Some(seven),
            "an older tail finishing late must not regress committed identity"
        );
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn follower_committed_highwater_rejects_conflicting_attempt_dimensions() {
        let state = FollowerTailState::default();
        let committed = follower_identity(10, 100, 1);
        assert_eq!(
            state.reserve(committed.clone()),
            Ok(FollowerAdmission::Reserved)
        );
        assert_eq!(state.finish(&committed, true), Ok(()));

        for conflicting in [
            follower_identity(10, 101, 1),
            follower_identity(11, 100, 1),
            follower_identity(9, 101, 1),
            follower_identity(11, 99, 1),
        ] {
            let error = state.reserve(conflicting).unwrap_err();
            assert!(error.contains("conflicting checkpoint"), "{error}");
        }
        assert_eq!(state.committed(), Some(committed));
        assert!(state.in_flight().is_empty());
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn follower_rejected_terminal_keeps_its_admission_fence() {
        let terminal = follower_identity(11, 99, 1);
        for committed in [follower_identity(10, 100, 1), follower_identity(11, 99, 2)] {
            let state = FollowerTailState::default();
            assert_eq!(
                state.reserve(terminal.clone()),
                Ok(FollowerAdmission::Reserved)
            );
            state.progress.lock().committed = Some(committed.clone());

            let error = state.finish(&terminal, true).unwrap_err();
            assert!(error.contains("conflicting checkpoint"), "{error}");
            assert_eq!(state.in_flight(), vec![terminal.clone()]);
            assert_eq!(state.committed(), Some(committed));
        }
    }

    /// A terminal carrying another certificate for the same epoch/checkpoint cannot release or
    /// commit the reserved identity.
    #[cfg(feature = "cluster")]
    #[test]
    fn follower_terminal_rejects_same_attempt_with_different_certificate() {
        let state = FollowerTailState::default();
        let exact = follower_identity(9, 90, 1);
        let wrong_certificate = follower_identity(9, 90, 2);

        assert_eq!(
            state.reserve(exact.clone()),
            Ok(FollowerAdmission::Reserved)
        );
        assert!(state.finish(&wrong_certificate, true).is_err());
        assert_eq!(state.in_flight(), vec![exact.clone()]);
        assert_eq!(state.committed(), None);
        assert_eq!(state.finish(&exact, true), Ok(()));
        assert_eq!(state.committed(), Some(exact));
    }

    /// Missing/outage/conflict errors are all in-doubt after prepare and must retain the exact
    /// admission identity until recovery observes an immutable outcome.
    #[cfg(feature = "cluster")]
    #[test]
    fn follower_in_doubt_completion_retains_in_flight_identity() {
        let state = FollowerTailState::default();
        let identity = follower_identity(10, 100, 4);
        assert_eq!(
            state.reserve(identity.clone()),
            Ok(FollowerAdmission::Reserved)
        );

        let missing = Err(DbError::Checkpoint(
            "follower outcome remained unresolved".into(),
        ));
        assert_eq!(state.finish_resolved(&identity, &missing), Ok(None));
        assert_eq!(state.in_flight(), vec![identity.clone()]);

        let read_error = Err(DbError::Checkpoint(
            "durable outcome store read failed".into(),
        ));
        assert_eq!(state.finish_resolved(&identity, &read_error), Ok(None));
        assert_eq!(state.in_flight(), vec![identity.clone()]);

        let abort = Ok(false);
        assert_eq!(state.finish_resolved(&identity, &abort), Ok(Some(false)));
        assert!(state.in_flight().is_empty());
    }

    #[cfg(feature = "cluster")]
    fn resume_identity(
        epoch: u64,
        checkpoint_id: u64,
    ) -> (
        laminar_core::cluster::control::CheckpointAssignmentFence,
        CertifiedCheckpointAttempt,
    ) {
        let fence = assignment_fence(1, &[1, 7]);
        let identity = CertifiedCheckpointAttempt {
            attempt: CheckpointAttempt::new(epoch, checkpoint_id),
            assignment_digest: fence.digest(),
            leader_proof: leader_proof(1),
        };
        (fence, identity)
    }

    /// Build a follower-side controller whose `current_leader()` is a
    /// seeded peer, for resume-gate tests. The caller holds the
    /// returned membership sender alive for the test's duration.
    #[cfg(feature = "cluster")]
    async fn gate_controller() -> (
        Arc<laminar_core::cluster::control::InMemoryKv>,
        laminar_core::cluster::control::ClusterController,
        laminar_core::cluster::discovery::NodeId,
        tokio::sync::watch::Sender<Vec<laminar_core::cluster::discovery::NodeInfo>>,
    ) {
        use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};

        let leader_id = NodeId(1);
        let follower_id = NodeId(7);
        let kv = Arc::new(InMemoryKv::new(follower_id));
        let kv_trait: Arc<dyn ClusterKv> = kv.clone();
        let leader_info = NodeInfo {
            id: leader_id,
            name: "leader".into(),
            rpc_address: String::new(),
            raft_address: String::new(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        };
        let (tx, rx) = tokio::sync::watch::channel(vec![leader_info]);
        let controller = ClusterController::new(follower_id, kv_trait, None, rx);
        let lease_store = Arc::new(laminar_core::cluster::control::LeaderLeaseStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            1_000,
        ));
        lease_store
            .try_acquire(
                &laminar_core::cluster::control::LeaderLeaseOwner {
                    node: leader_id,
                    boot: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
                    process_term: 1,
                },
                0,
            )
            .await
            .unwrap();
        controller.set_leader_lease_store(lease_store);
        (kv, controller, leader_id, tx)
    }

    /// The resume gate releases on the leader's `Aligned` announcement.
    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn aligned_resume_gate_releases_on_aligned() {
        use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

        let (kv, controller, leader_id, _members_tx) = gate_controller().await;
        let (fence, identity) = resume_identity(3, 3);
        let aligned = serde_json::to_string(&BarrierAnnouncement {
            epoch: 3,
            checkpoint_id: 3,
            assignment_fence: Some(fence.clone()),
            leader_proof: Some(identity.leader_proof.clone()),
            phase: Phase::Aligned,
            flags: 0,
            min_watermark_ms: Some(42),
        })
        .unwrap();
        kv.seed(leader_id, ANNOUNCEMENT_KEY, aligned);

        tokio::time::timeout(
            Duration::from_secs(2),
            ConnectorPipelineCallback::wait_for_aligned_resume(
                true,
                &controller,
                identity,
                &fence,
                std::time::Duration::from_secs(3),
            ),
        )
        .await
        .expect("gate must release on Aligned")
        .expect("Aligned certificate must validate");
        assert_eq!(
            controller.cluster_min_watermark(),
            None,
            "Aligned may still abort and cannot advance the recovery-safe watermark"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn exact_commit_resume_gate_publishes_the_recovery_safe_watermark() {
        use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

        let (kv, controller, leader_id, _members_tx) = gate_controller().await;
        let (fence, identity) = resume_identity(3, 3);
        kv.seed(
            leader_id,
            ANNOUNCEMENT_KEY,
            serde_json::to_string(&BarrierAnnouncement {
                epoch: 3,
                checkpoint_id: 3,
                assignment_fence: Some(fence.clone()),
                leader_proof: Some(identity.leader_proof.clone()),
                phase: Phase::Commit,
                flags: 0,
                min_watermark_ms: Some(42),
            })
            .unwrap(),
        );

        ConnectorPipelineCallback::wait_for_aligned_resume(
            true,
            &controller,
            identity,
            &fence,
            Duration::from_secs(1),
        )
        .await
        .expect("exact Commit must release the resume gate");
        assert_eq!(controller.cluster_min_watermark(), Some(42));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn aligned_resume_gate_timeout_fails_closed() {
        let (_kv, controller, _leader_id, _members_tx) = gate_controller().await;
        let (fence, identity) = resume_identity(3, 3);
        let error = ConnectorPipelineCallback::wait_for_aligned_resume(
            true,
            &controller,
            identity,
            &fence,
            Duration::from_millis(20),
        )
        .await
        .expect_err("missing Aligned must not reopen the pipeline");
        assert!(error.contains("pipeline remains fenced"), "{error}");
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn aligned_resume_gate_rejects_wrong_attempt_or_certificate() {
        use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

        for wrong in [
            BarrierAnnouncement {
                epoch: 3,
                checkpoint_id: 4,
                assignment_fence: Some(resume_identity(3, 3).0),
                leader_proof: None,
                phase: Phase::Abort,
                flags: 0,
                min_watermark_ms: None,
            },
            BarrierAnnouncement {
                epoch: 3,
                checkpoint_id: 3,
                assignment_fence: Some(assignment_fence(2, &[1, 7])),
                leader_proof: Some(leader_proof(1)),
                phase: Phase::Aligned,
                flags: 0,
                min_watermark_ms: Some(99),
            },
            BarrierAnnouncement {
                epoch: 3,
                checkpoint_id: 3,
                assignment_fence: Some(resume_identity(3, 3).0),
                leader_proof: Some(leader_proof(2)),
                phase: Phase::Aligned,
                flags: 0,
                min_watermark_ms: Some(99),
            },
        ] {
            let (kv, controller, leader_id, _members_tx) = gate_controller().await;
            let (expected_fence, identity) = resume_identity(3, 3);
            kv.seed(
                leader_id,
                ANNOUNCEMENT_KEY,
                serde_json::to_string(&wrong).unwrap(),
            );

            let outcome = tokio::time::timeout(
                Duration::from_millis(100),
                ConnectorPipelineCallback::wait_for_aligned_resume(
                    true,
                    &controller,
                    identity,
                    &expected_fence,
                    std::time::Duration::from_secs(3),
                ),
            )
            .await;
            assert!(
                !matches!(outcome, Ok(Ok(()))),
                "wrong exact identity must not release the shuffle resume gate"
            );
            assert_eq!(
                controller.cluster_min_watermark(),
                None,
                "rejected identity must not advance the cluster watermark"
            );
        }
    }

    /// A newer epoch's announcement supersedes the awaited one
    /// (latest-wins observation can overwrite Aligned/Commit).
    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn aligned_resume_gate_releases_on_newer_epoch() {
        use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

        let (kv, controller, leader_id, _members_tx) = gate_controller().await;
        let (fence, identity) = resume_identity(3, 3);
        let newer = serde_json::to_string(&BarrierAnnouncement {
            epoch: 4,
            checkpoint_id: 4,
            assignment_fence: Some(assignment_fence(2, &[1, 7])),
            leader_proof: Some(leader_proof(1)),
            phase: Phase::Aligned,
            flags: 0,
            min_watermark_ms: Some(77),
        })
        .unwrap();
        kv.seed(leader_id, ANNOUNCEMENT_KEY, newer);

        tokio::time::timeout(
            Duration::from_secs(2),
            ConnectorPipelineCallback::wait_for_aligned_resume(
                true,
                &controller,
                identity,
                &fence,
                std::time::Duration::from_secs(3),
            ),
        )
        .await
        .expect("gate must release when a newer epoch is announced")
        .expect("newer-epoch release must preserve the awaited certificate");
        assert_eq!(
            controller.cluster_min_watermark(),
            None,
            "a newer epoch may release the gate but cannot publish its watermark here"
        );
    }

    /// Without a cross-node shuffle there is no in-flight-row invariant
    /// to protect — the gate is a no-op even with no announcement.
    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn aligned_resume_gate_skips_without_shuffle() {
        let (_kv, controller, _leader_id, _members_tx) = gate_controller().await;
        let (fence, identity) = resume_identity(3, 3);
        tokio::time::timeout(
            Duration::from_millis(100),
            ConnectorPipelineCallback::wait_for_aligned_resume(
                false,
                &controller,
                identity,
                &fence,
                std::time::Duration::from_secs(3),
            ),
        )
        .await
        .expect("gate must be a no-op without a cluster shuffle")
        .expect("no-shuffle gate must accept a valid certificate");
    }

    /// Retention is bounded and fails closed when every retained identity is active. Once one tail
    /// finishes, its inactive slot can be evicted for a newer epoch.
    #[cfg(feature = "cluster")]
    #[test]
    fn follower_identity_history_is_bounded() {
        let state = FollowerTailState::default();
        for epoch in 1..=MAX_RETAINED_FOLLOWER_IDENTITIES as u64 {
            assert_eq!(
                state.reserve(follower_identity(epoch, epoch * 10, 1)),
                Ok(FollowerAdmission::Reserved)
            );
        }
        assert!(
            state
                .reserve(follower_identity(
                    MAX_RETAINED_FOLLOWER_IDENTITIES as u64 + 1,
                    9_999,
                    1,
                ))
                .is_err(),
            "all-active capacity must fail closed"
        );

        let oldest = follower_identity(1, 10, 1);
        assert_eq!(state.finish(&oldest, false), Ok(()));
        let newest = follower_identity(MAX_RETAINED_FOLLOWER_IDENTITIES as u64 + 1, 9_999, 1);
        assert_eq!(state.reserve(newest), Ok(FollowerAdmission::Reserved));
        assert_eq!(state.in_flight().len(), MAX_RETAINED_FOLLOWER_IDENTITIES);
    }
}
