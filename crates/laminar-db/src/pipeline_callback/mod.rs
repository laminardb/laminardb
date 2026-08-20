//! Production `PipelineCallback` bridging coordinator to sinks, checkpoints, and watermarks.
#![allow(clippy::disallowed_types)] // cold path

mod checkpoint_tail;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::connector::SinkContract;
#[cfg(feature = "cluster")]
use laminar_core::checkpoint::CheckpointAttemptRelation;
use laminar_core::checkpoint::{
    classify_channel_progress, CheckpointAttempt, CheckpointWatermark, ConnectorCheckpoint,
    SINGLETON_WATERMARK_CHANNEL,
};
use rustc_hash::FxHashMap;

use crate::db::{filter_late_rows, SourceWatermarkState};
use crate::error::DbError;
use crate::operator_graph::InputFrontier;
#[cfg(feature = "cluster")]
use crate::operator_graph::ShuffleAlignmentOutcome;
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

async fn await_sink_publication_until<T>(
    deadline: Option<tokio::time::Instant>,
    boundary: &str,
    future: impl std::future::Future<Output = T>,
) -> Result<T, String> {
    let Some(deadline) = deadline else {
        return Ok(future.await);
    };
    if tokio::time::Instant::now() >= deadline {
        return Err(format!("{boundary} exhausted the checkpoint deadline"));
    }
    tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| format!("{boundary} exhausted the checkpoint deadline"))
}

#[cfg(feature = "cluster")]
async fn await_sink_publication<T>(
    controller: Option<&laminar_core::cluster::control::ClusterController>,
    deadline: Option<tokio::time::Instant>,
    boundary: &str,
    future: impl std::future::Future<Output = T>,
) -> Result<T, String> {
    let Some(controller) = controller else {
        return await_sink_publication_until(deadline, boundary, future).await;
    };
    if !controller.process_lease_is_live() {
        return Err(format!("cluster process lease expired before {boundary}"));
    }
    let operation = await_sink_publication_until(deadline, boundary, future);
    tokio::pin!(operation);
    tokio::select! {
        biased;
        () = controller.wait_for_process_lease_loss() => {
            Err(format!("cluster process lease expired before {boundary}"))
        }
        result = &mut operation => {
            let value = result?;
            if controller.process_lease_is_live() {
                Ok(value)
            } else {
                Err(format!("cluster process lease expired before {boundary}"))
            }
        }
    }
}

#[cfg(not(feature = "cluster"))]
async fn await_sink_publication<T>(
    deadline: Option<tokio::time::Instant>,
    boundary: &str,
    future: impl std::future::Future<Output = T>,
) -> Result<T, String> {
    await_sink_publication_until(deadline, boundary, future).await
}

/// Terminal failure reporting is cleanup, not part of the durable attempt. A failure discovered
/// at the attempt deadline must still release its manual caller and exact-attempt bookkeeping.
const CHECKPOINT_FAILURE_REPORT_TIMEOUT: Duration = Duration::from_secs(1);

struct SinkEpochTransition {
    handles: Vec<(
        crate::sink_task::SinkTaskHandle,
        crate::sink_task::SinkEpochAdmission,
    )>,
}

impl SinkEpochTransition {
    fn capture_open(
        handles: impl IntoIterator<Item = crate::sink_task::SinkTaskHandle>,
        epoch: u64,
    ) -> Result<Option<Self>, String> {
        let sink_handles = handles.into_iter().collect::<Vec<_>>();
        let mut captured = Vec::with_capacity(sink_handles.len());
        for handle in &sink_handles {
            match handle.open_epoch_admission(epoch) {
                Ok(admission) => captured.push((handle.clone(), admission)),
                Err(error) => {
                    for handle in &sink_handles {
                        handle.fail_epoch_gate();
                    }
                    return Err(format!("sink '{}' epoch capture: {error}", handle.name()));
                }
            }
        }
        let Some((_, expected)) = captured.first() else {
            return Ok(None);
        };
        if captured.iter().any(|(_, admission)| admission != expected) {
            for handle in &sink_handles {
                handle.fail_epoch_gate();
            }
            return Err(format!(
                "checkpoint epoch {epoch} sink gates have mismatched transition generations"
            ));
        }
        Ok(Some(Self { handles: captured }))
    }

    async fn seal_until(&mut self, deadline: tokio::time::Instant) -> Result<(), String> {
        // Pipeline publication owns `&mut PipelineCallback`, so no normal producer can race the
        // earlier sink Sync/capture. Admission still linearizes direct handle users with each
        // exact close, and the actor's Prepared phase is the final protocol backstop.
        for (handle, admission) in &mut self.handles {
            *admission = handle
                .seal_epoch_until(*admission, deadline)
                .await
                .map_err(|error| format!("sink '{}' epoch seal failed: {error}", handle.name()))?;
        }
        let expected = self.handles.first().map(|(_, admission)| *admission);
        if self
            .handles
            .iter()
            .any(|(_, admission)| Some(*admission) != expected)
        {
            return Err("sink epoch seals produced mismatched transition generations".into());
        }
        Ok(())
    }

    fn publish_successor(&mut self) -> Result<(), String> {
        let admissions = self
            .handles
            .iter()
            .map(|(handle, sealed)| {
                let begun = handle.current_begun_epoch_admission().ok_or_else(|| {
                    format!(
                        "sink '{}' has no prepared successor for sealed epoch {}",
                        handle.name(),
                        sealed.epoch
                    )
                })?;
                if begun.generation != sealed.generation {
                    return Err(format!(
                        "sink '{}' successor generation {} does not match sealed generation {}",
                        handle.name(),
                        begun.generation,
                        sealed.generation
                    ));
                }
                Ok(begun)
            })
            .collect::<Result<Vec<_>, String>>()?;
        let expected = admissions
            .first()
            .copied()
            .ok_or_else(|| "checkpoint-committable sink transition has no handles".to_string())?;
        if admissions.iter().any(|admission| *admission != expected) {
            return Err("prepared successor sink gates do not share one exact admission".into());
        }
        for ((handle, _), admission) in self.handles.iter().zip(admissions) {
            handle.publish_open_epoch(admission).map_err(|error| {
                format!("sink '{}' successor publication: {error}", handle.name())
            })?;
        }
        Ok(())
    }

    fn fail(&self) {
        for (handle, admission) in &self.handles {
            handle.fail_epoch_transition(*admission);
        }
    }
}

/// RAII guard that fails an unresolved sink transition before releasing epoch admission.
struct EpochInFlightGuard {
    in_flight: Arc<std::sync::atomic::AtomicU64>,
    sink_transition: Option<SinkEpochTransition>,
    checkpoint_fault: Arc<parking_lot::Mutex<Option<String>>>,
    attempt: CheckpointAttempt,
}

impl EpochInFlightGuard {
    /// Claim one admission slot.
    fn claim(
        in_flight: &Arc<std::sync::atomic::AtomicU64>,
        checkpoint_fault: &Arc<parking_lot::Mutex<Option<String>>>,
        attempt: CheckpointAttempt,
        sink_handles: impl IntoIterator<Item = crate::sink_task::SinkTaskHandle>,
    ) -> Result<Self, String> {
        let sink_transition = SinkEpochTransition::capture_open(sink_handles, attempt.epoch)?;
        in_flight.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(Self {
            in_flight: Arc::clone(in_flight),
            sink_transition,
            checkpoint_fault: Arc::clone(checkpoint_fault),
            attempt,
        })
    }

    async fn seal_sink_epoch_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        if let Some(transition) = self.sink_transition.as_mut() {
            transition.seal_until(deadline).await?;
        }
        Ok(())
    }

    fn publish_successor(&mut self) -> Result<(), String> {
        let Some(transition) = self.sink_transition.as_mut() else {
            return Ok(());
        };
        if let Err(error) = transition.publish_successor() {
            transition.fail();
            return Err(error);
        }
        Ok(())
    }

    fn disarm_sink_epoch(&mut self) {
        self.sink_transition = None;
    }

    fn fail_sink_epoch(&mut self, reason: impl Into<String>) {
        if let Some(transition) = self.sink_transition.take() {
            transition.fail();
            set_checkpoint_fault(&self.checkpoint_fault, reason);
        }
    }
}

impl Drop for EpochInFlightGuard {
    fn drop(&mut self) {
        if let Some(transition) = self.sink_transition.take() {
            transition.fail();
            set_checkpoint_fault(
                &self.checkpoint_fault,
                format!(
                    "checkpoint {} epoch {} ended without publishing a writable successor sink epoch",
                    self.attempt.checkpoint_id, self.attempt.epoch
                ),
            );
        }
        self.in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// State for the leader's spawned durable tail.
struct LeaderTail {
    in_flight: EpochInFlightGuard,
    coordinator:
        Arc<tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>>,
    complete_tx: crossfire::MAsyncTx<crossfire::mpsc::Array<CheckpointCompletion>>,
    request: crate::checkpoint_coordinator::CheckpointRequest,
    operator_state: Option<CapturedOperatorState>,
    operator_state_staged_cap_bytes: u64,
    mutable_operator_capture_guard: Option<MutableCheckpointCaptureGuard>,
    fan_out: FxHashMap<String, SourceCheckpoint>,
    local_watermark: CheckpointWatermark,
    handoff: HandoffCapture,
    attempt: CheckpointAttempt,
    attempt_started: std::time::Instant,
    attempt_deadline: tokio::time::Instant,
    checkpoint_timeout: Duration,
    serialization_timeout: Duration,
    checkpoint_cleanup_timeout: Duration,
    fault_on_retryable_failure: bool,
    fault_on_unclassified_error: bool,
    checkpoint_fault: Arc<parking_lot::Mutex<Option<String>>>,
    #[cfg(feature = "cluster")]
    controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    /// Exact durable authority captured before this attempt's `Prepare` publication.
    #[cfg(feature = "cluster")]
    leader_proof: Option<laminar_core::cluster::control::LeaderProof>,
    full_vnode_capture_needed: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Debug, Clone, Copy, Default)]
struct HandoffCapture {
    flags: u64,
    replay_pending: bool,
}

impl HandoffCapture {
    const fn new(flags: u64, replay_pending: bool) -> Self {
        Self {
            flags,
            replay_pending,
        }
    }

    fn bind_request(
        self,
        request: &mut crate::checkpoint_coordinator::CheckpointRequest,
        reassignment_portable: bool,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) {
        request.flags = self.flags;
        request.handoff_replay_pending = self.replay_pending;
        request.reassignment_portable = reassignment_portable;
        request.assignment_fence = assignment_fence;
    }

    const fn terminal(self) -> bool {
        crate::checkpoint_coordinator::sink_epoch_admission::is_terminal_handoff(
            self.flags,
            self.replay_pending,
        )
    }
}

fn checkpoint_failure_requires_pipeline_fault(
    result: &crate::checkpoint_coordinator::CheckpointResult,
    fault_on_retryable_failure: bool,
) -> bool {
    result.requires_recovery() || fault_on_retryable_failure
}

fn validate_durable_source_checkpoint_roster(
    expected: &[String],
    checkpoints: &FxHashMap<String, SourceCheckpoint>,
) -> Result<(), String> {
    if expected.len() == checkpoints.len()
        && expected
            .iter()
            .all(|source| checkpoints.contains_key(source))
    {
        return Ok(());
    }

    let missing = expected
        .iter()
        .filter(|source| !checkpoints.contains_key(*source))
        .cloned()
        .collect::<Vec<_>>();
    let mut unexpected = checkpoints
        .keys()
        .filter(|source| !expected.iter().any(|expected| expected == *source))
        .cloned()
        .collect::<Vec<_>>();
    unexpected.sort_unstable();
    Err(format!(
        "durable checkpoint source roster mismatch: missing {missing:?}, unexpected {unexpected:?}"
    ))
}

/// Captured follower state and the runtime handles that own its decision-led durable tail.
#[cfg(feature = "cluster")]
struct FollowerDurableTail {
    in_flight: EpochInFlightGuard,
    coordinator:
        Arc<tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>>,
    state: Arc<FollowerTailState>,
    complete_tx: crossfire::MAsyncTx<crossfire::mpsc::Array<CheckpointCompletion>>,
    controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    full_vnode_capture_needed: Arc<std::sync::atomic::AtomicBool>,
    checkpoint_fault: Arc<parking_lot::Mutex<Option<String>>>,
    request: crate::checkpoint_coordinator::CheckpointRequest,
    operator_state: Option<CapturedOperatorState>,
    operator_state_staged_cap_bytes: u64,
    mutable_operator_capture_guard: Option<MutableCheckpointCaptureGuard>,
    assignment_fence: laminar_core::cluster::control::CheckpointAssignmentFence,
    identity: CertifiedCheckpointAttempt,
    fan_out: FxHashMap<String, SourceCheckpoint>,
    local_watermark: CheckpointWatermark,
    handoff_replay_pending: bool,
    attempt: CheckpointAttempt,
    attempt_started: std::time::Instant,
    attempt_deadline: tokio::time::Instant,
    checkpoint_timeout: Duration,
    serialization_timeout: Duration,
    checkpoint_cleanup_timeout: Duration,
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
    match guard.as_mut() {
        Some(guard) => guard.fail(&error),
        None => error,
    }
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
    flags: u64,
    assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    leader_proof: Option<laminar_core::checkpoint::LeaderProof>,
    deadline: tokio::time::Instant,
    sink_epoch_publication: crate::checkpoint_coordinator::SinkEpochPublication,
) -> Result<(), String> {
    tokio::time::timeout_at(deadline, async {
        let mut guard = coordinator.lock().await;
        let coordinator = guard.as_mut().ok_or_else(|| {
            format!(
                "checkpoint {} epoch {} has no initialized coordinator for cleanup",
                attempt.checkpoint_id, attempt.epoch
            )
        })?;
        let result = coordinator
            .abandon_epoch_until(
                attempt.checkpoint_id,
                attempt.epoch,
                reason,
                flags,
                assignment_fence,
                leader_proof,
                deadline,
                sink_epoch_publication,
            )
            .await
            .map_err(|error| error.to_string())?;
        let completed = CheckpointAttempt::new(result.epoch, result.checkpoint_id);
        if result.success
            || completed != attempt
            || result.failure_disposition
                != Some(crate::checkpoint_coordinator::CheckpointFailureDisposition::Retryable)
        {
            return Err(result.error.unwrap_or_else(|| {
                "checkpoint abandonment did not reach a clean durable Abort".into()
            }));
        }
        Ok(())
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
/// Exact-attempt abandonment and cluster Abort publication must finish (or exhaust their one
/// private runtime-owned deadline) before terminal reporting. Otherwise a manual checkpoint
/// waiter can observe failure while the reserved attempt is still live and race later lifecycle
/// work against its rollback.
async fn fail_reserved_leader_attempt(
    tail: &mut LeaderTail,
    terminal_error: String,
    cleanup_reason: String,
) {
    let attempt = tail.attempt;
    tail.in_flight.fail_sink_epoch(terminal_error.clone());
    if tail.fault_on_retryable_failure {
        set_checkpoint_fault(&tail.checkpoint_fault, terminal_error.clone());
    }
    tail.full_vnode_capture_needed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let coordinator = Arc::clone(&tail.coordinator);
    let complete_tx = tail.complete_tx.clone();
    let checkpoint_fault = Arc::clone(&tail.checkpoint_fault);
    let flags = tail.request.flags;
    let assignment_fence = tail.request.assignment_fence.clone();
    let checkpoint_cleanup_timeout = tail.checkpoint_cleanup_timeout;

    let cleanup_deadline = tokio::time::Instant::now() + checkpoint_cleanup_timeout;
    #[cfg(feature = "cluster")]
    let leader_proof = tail.leader_proof.clone();
    #[cfg(not(feature = "cluster"))]
    let leader_proof = None;
    let cleanup_result = cleanup_reserved_attempt_until(
        coordinator.as_ref(),
        attempt,
        cleanup_reason,
        flags,
        assignment_fence,
        leader_proof,
        cleanup_deadline,
        crate::checkpoint_coordinator::SinkEpochPublication::DeferredToTail,
    )
    .await;

    let reported_error = if let Err(error) = cleanup_result {
        let cleanup_fault = format!(
            "checkpoint {} epoch {} pre-execution cleanup incomplete; recovery required: {error}",
            attempt.checkpoint_id, attempt.epoch,
        );
        tracing::error!(%cleanup_fault, "checkpoint cleanup faulted the pipeline");
        set_checkpoint_fault(&checkpoint_fault, cleanup_fault.clone());
        format!("{terminal_error}; {cleanup_fault}")
    } else {
        terminal_error
    };

    deliver_checkpoint_failure(&complete_tx, attempt, reported_error, &checkpoint_fault).await;
}

struct OperatorStateCapture {
    graph: crate::operator_graph::GraphStateCapture,
    materialized_views: crate::mv_store::MvCheckpointCapture,
    reference_tables: Option<crate::table_store::ReferenceTableCheckpointCapture>,
    serialization_permit: tokio::sync::OwnedSemaphorePermit,
}

struct EncodedOperatorState {
    frames: Vec<crate::checkpoint_coordinator::CapturedStateFrame>,
    managed_vnode_operators: Vec<crate::checkpoint_coordinator::ManagedVnodeOperator>,
}

fn graph_capture_needs_mutable_guard(graph: &crate::operator_graph::GraphStateCapture) -> bool {
    !graph.whole.is_empty()
        || graph
            .vnodes
            .iter()
            .any(|(_, captured)| captured.state.is_some())
}

impl OperatorStateCapture {
    fn encode(
        self,
        max_staged_bytes: u64,
        mut staged_bytes: u64,
    ) -> Result<EncodedOperatorState, DbError> {
        let Self {
            graph,
            materialized_views,
            reference_tables,
            serialization_permit,
        } = self;
        let managed_vnode_operators = graph
            .managed_vnode_operators
            .into_iter()
            .map(|(operator_id, placement)| {
                let placement = match placement {
                    crate::operator::capability::OperatorStateClass::GlobalSingleton => {
                        crate::checkpoint_coordinator::ManagedVnodePlacement::GlobalSingleton
                    }
                    crate::operator::capability::OperatorStateClass::VnodeKeyed => {
                        crate::checkpoint_coordinator::ManagedVnodePlacement::VnodeKeyed
                    }
                    unsupported => {
                        return Err(DbError::Checkpoint(format!(
                            "managed operator '{operator_id}' has unsupported placement {unsupported:?}"
                        )));
                    }
                };
                Ok(crate::checkpoint_coordinator::ManagedVnodeOperator {
                    operator_id: format!("graph:{operator_id}"),
                    placement,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut frames = Vec::with_capacity(graph.whole.len() + graph.vnodes.len() + 1);
        for state in graph.whole {
            let encoded = state
                .state
                .materialize(&mut staged_bytes, max_staged_bytes)?;
            frames.push(crate::checkpoint_coordinator::CapturedStateFrame {
                key: laminar_core::checkpoint::StateFrameKey::OperatorWhole {
                    operator_id: format!("graph:{}", state.operator_id),
                },
                state: Some(encoded),
            });
        }
        for (operator_id, captured) in graph.vnodes {
            let vnode = u16::try_from(captured.vnode).map_err(|_| {
                DbError::Checkpoint(format!(
                    "operator '{operator_id}' captured vnode {} outside the checkpoint ABI",
                    captured.vnode
                ))
            })?;
            let encoded = captured
                .state
                .map(|state| state.materialize(&mut staged_bytes, max_staged_bytes))
                .transpose()?;
            frames.push(crate::checkpoint_coordinator::CapturedStateFrame {
                key: laminar_core::checkpoint::StateFrameKey::Vnode {
                    operator_id: format!("graph:{operator_id}"),
                    vnode,
                },
                state: encoded,
            });
        }

        let mv_capture_bytes = materialized_views.estimated_bytes();
        let mv_headroom = max_staged_bytes.checked_sub(staged_bytes).ok_or_else(|| {
            DbError::Checkpoint("MV capture exceeded the staged-state budget".into())
        })?;
        let (materialized_views, mv_retained_bytes) =
            materialized_views.encode(mv_headroom)?.into_parts();
        staged_bytes = staged_bytes
            .checked_sub(mv_capture_bytes)
            .and_then(|bytes| bytes.checked_add(mv_retained_bytes))
            .filter(|bytes| *bytes <= max_staged_bytes)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "MV state ownership transfer exceeded its staged-state budget".into(),
                )
            })?;

        let reference_tables = reference_tables
            .map(|capture| {
                let capture_bytes = capture.estimated_bytes();
                let headroom = max_staged_bytes.checked_sub(staged_bytes).ok_or_else(|| {
                    DbError::Checkpoint(
                        "reference-table capture exceeded the staged-state budget".into(),
                    )
                })?;
                let (encoded, encoded_retained_bytes) = capture.encode(headroom)?;
                staged_bytes = staged_bytes
                    .checked_sub(capture_bytes)
                    .and_then(|bytes| bytes.checked_add(encoded_retained_bytes))
                    .filter(|bytes| *bytes <= max_staged_bytes)
                    .ok_or_else(|| {
                        DbError::Checkpoint(
                            "reference-table state ownership transfer exceeded its staged-state budget"
                                .into(),
                        )
                    })?;
                Ok::<bytes::Bytes, DbError>(encoded)
            })
            .transpose()?;
        frames.extend(materialized_views.into_iter().map(|(operator_id, state)| {
            crate::checkpoint_coordinator::CapturedStateFrame {
                key: laminar_core::checkpoint::StateFrameKey::OperatorWhole { operator_id },
                state: Some(state),
            }
        }));
        if let Some(reference_tables) = reference_tables {
            frames.push(crate::checkpoint_coordinator::CapturedStateFrame {
                key: laminar_core::checkpoint::StateFrameKey::OperatorWhole {
                    operator_id: crate::table_store::REFERENCE_TABLE_CHECKPOINT_KEY.to_string(),
                },
                state: Some(reference_tables),
            });
        }

        frames.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        if frames.windows(2).any(|pair| pair[0].key == pair[1].key) {
            return Err(DbError::Checkpoint(
                "checkpoint capture produced a duplicate logical state frame".into(),
            ));
        }

        // The permit is deliberately owned by the non-abortable worker. If its async waiter times
        // out, another checkpoint cannot capture a second image until this worker actually exits.
        drop(serialization_permit);
        Ok(EncodedOperatorState {
            frames,
            managed_vnode_operators,
        })
    }
}

/// Immutable operator image captured at the aligned cut. Encoding is deliberately deferred to
/// the spawned durable tail so the callback can resume while Arrow IPC and rkyv run off-thread.
struct CapturedOperatorState {
    image: OperatorStateCapture,
    estimated_bytes: u64,
    mutable_capture_guard: Option<MutableCheckpointCaptureGuard>,
}

struct SerializedOperatorState {
    frames: Vec<crate::checkpoint_coordinator::CapturedStateFrame>,
    managed_vnode_operators: Vec<crate::checkpoint_coordinator::ManagedVnodeOperator>,
    mutable_capture_guard: Option<MutableCheckpointCaptureGuard>,
}

impl SerializedOperatorState {
    #[cfg(test)]
    fn accept_for_test(mut self) -> Vec<crate::checkpoint_coordinator::CapturedStateFrame> {
        if let Some(guard) = self.mutable_capture_guard.as_mut() {
            guard.disarm();
        }
        self.frames
    }
}

impl CapturedOperatorState {
    async fn serialize_until(
        self,
        max_staged_bytes: u64,
        serialization_timeout: Duration,
        attempt_deadline: tokio::time::Instant,
    ) -> Result<SerializedOperatorState, String> {
        let Self {
            image,
            estimated_bytes,
            mut mutable_capture_guard,
        } = self;
        let serialization_deadline =
            attempt_deadline.min(tokio::time::Instant::now() + serialization_timeout);
        let worker =
            tokio::task::spawn_blocking(move || image.encode(max_staged_bytes, estimated_bytes));
        let encoded = match tokio::time::timeout_at(serialization_deadline, worker).await {
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
            Ok(Ok(Ok(encoded))) => encoded,
        };
        Ok(SerializedOperatorState {
            frames: encoded.frames,
            managed_vnode_operators: encoded.managed_vnode_operators,
            mutable_capture_guard,
        })
    }
}

/// Exact checkpoint identity retained across follower admission, durable-tail execution, and
/// terminal resume. The certificate and leader proof distinguish same-attempt equivocation.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct CertifiedCheckpointAttempt {
    attempt: CheckpointAttempt,
    assignment_digest: [u8; 32],
    flags: u64,
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
    Started {
        attempt: CheckpointAttempt,
        flags: u64,
    },
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
             flags {:#x} authority {:?}, announced id {} digest {:?} flags {:#x} authority {:?}",
            announced.attempt.epoch,
            retained.attempt.checkpoint_id,
            retained.assignment_digest,
            retained.flags,
            retained.leader_proof,
            announced.attempt.checkpoint_id,
            announced.assignment_digest,
            announced.flags,
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
        bool, // admitted input is a changelog and must carry canonical weight
    )>,
    pub(crate) owned_sink_handles: Arc<parking_lot::Mutex<Vec<crate::sink_task::SinkTaskHandle>>>,
    pub(crate) watermark_states: FxHashMap<String, SourceWatermarkState>,
    pub(crate) source_entries_for_wm: FxHashMap<String, Arc<crate::catalog::SourceEntry>>,
    pub(crate) source_ids: FxHashMap<String, usize>,
    pub(crate) source_name_arcs: FxHashMap<usize, Arc<str>>,
    pub(crate) checkpoint_source_names: Vec<String>,
    pub(crate) source_frontiers_buf: FxHashMap<Arc<str>, InputFrontier>,
    /// Decision-bound source frontiers pinned once before coordinator intake. Source filtering
    /// and managed temporal execution must share this exact snapshot across the whole cycle.
    #[cfg(feature = "cluster")]
    pub(crate) committed_source_watermarks_snapshot: Arc<FxHashMap<String, i64>>,
    pub(crate) tracker: Option<laminar_core::time::WatermarkTracker>,
    pub(crate) prom: Arc<crate::engine_metrics::EngineMetrics>,
    #[cfg(feature = "cluster")]
    pub(crate) checkpoint_barrier_timings:
        Arc<crate::checkpoint_timing::CheckpointBarrierTimingLedger>,
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
    /// Deterministic record/shuffle failure that cannot be repaired by checkpoint recovery.
    /// Checkpoint alignment sometimes has to return a disposition-only `Failed` outcome, so the
    /// structured halt is retained here for the coordinator to arbitrate before recovery faults.
    pub(crate) pipeline_halt: Option<String>,
    /// Last admission failure already reported; cleared by the next successful admission path.
    pub(crate) last_checkpoint_admission_failure: Option<String>,
    pub(crate) checkpoint_admission_recovering: bool,
    pub(crate) shutdown_signal: Arc<tokio::sync::Notify>,
    #[cfg(feature = "cluster")]
    pub(crate) cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    /// Shared assignment/checkpoint admission boundary. The coordinator carries an owned guard
    /// from its exact assignment audit through durable Prepare and source-barrier installation.
    #[cfg(feature = "cluster")]
    pub(crate) assignment_adoption_lock: Arc<tokio::sync::Mutex<()>>,
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
    /// Existing database-owned I/O runtime used by spawned checkpoint tails.
    pub(crate) checkpoint_tail_runtime: tokio::runtime::Handle,
    /// Every spawned checkpoint tail. `JoinSet` keeps shutdown from racing state/sink work that
    /// has not reached a terminal result.
    pub(crate) checkpoint_tail_tasks: tokio::task::JoinSet<()>,
    /// In-flight epoch count; the coordinator serializes durable checkpoint tails.
    pub(crate) checkpoint_in_flight: Arc<std::sync::atomic::AtomicU64>,
    /// Set when a captured image is not committed. The next capture must include every owned
    /// vnode because dirty flags were consumed by the abandoned attempt.
    pub(crate) full_vnode_capture_needed: Arc<std::sync::atomic::AtomicBool>,
    /// Lock-free id allocator shared with the coordinator so barrier admission doesn't
    /// queue behind an earlier epoch's durable tail holding the coordinator mutex.
    pub(crate) epoch_allocator: Option<Arc<crate::checkpoint_coordinator::EpochAllocator>>,
    #[cfg(feature = "cluster")]
    pub(crate) quorum_timeout: Duration,
    /// Whether checkpoint attempts consume a committable sink epoch reservation.
    pub(crate) checkpoint_committable_sinks: bool,
    /// Cluster startup/recovery fence. While set, neither source nor shuffle input may be folded.
    pub(crate) intake_gate: Arc<std::sync::atomic::AtomicBool>,
}

/// Freeze graph intake at the exact portable handoff cut.
///
/// A HANDOFF checkpoint with retained replay is only an intermediate cut: the coordinator must
/// remain open so the replay can reach the fixed point. Once the same capture proves that no
/// replay remains, closing this gate before mutable state capture prevents any predecessor-scoped
/// channel work from being staged between the portable cut and assignment publication.
#[cfg(feature = "cluster")]
pub(crate) fn fence_intake_after_terminal_handoff_capture(
    intake_gate: &std::sync::atomic::AtomicBool,
    flags: u64,
    handoff_replay_pending: bool,
) -> bool {
    let terminal_handoff = crate::checkpoint_coordinator::sink_epoch_admission::is_terminal_handoff(
        flags,
        handoff_replay_pending,
    );
    if terminal_handoff {
        intake_gate.store(true, std::sync::atomic::Ordering::Release);
    }
    terminal_handoff
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
    fn checkpoint_recovery_active(&self) -> bool {
        self.cluster_controller
            .as_ref()
            .is_some_and(|controller| controller.is_recovering())
    }

    fn mark_checkpoint_admission_failure(&mut self, reason: &str) -> bool {
        if self.last_checkpoint_admission_failure.as_deref() == Some(reason) {
            return false;
        }
        self.last_checkpoint_admission_failure = Some(reason.to_owned());
        true
    }

    fn observe_checkpoint_recovery_state(&mut self, recovering: bool) -> bool {
        if self.checkpoint_admission_recovering != recovering {
            self.checkpoint_admission_recovering = recovering;
            self.last_checkpoint_admission_failure = None;
        }
        recovering
    }

    #[cfg(feature = "cluster")]
    fn require_process_authority(&self, boundary: &str) -> Result<(), crate::pipeline::CycleError> {
        if self
            .cluster_controller
            .as_ref()
            .is_none_or(|controller| controller.process_lease_is_live())
        {
            return Ok(());
        }
        let error = format!("cluster process lease expired before {boundary}");
        set_checkpoint_fault(&self.checkpoint_fault, error.clone());
        Err(crate::pipeline::CycleError::Recovery(error))
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_barrier_timing_context(
        controller: &laminar_core::cluster::control::ClusterController,
        attempt: CheckpointAttempt,
        role: crate::checkpoint_timing::CheckpointBarrierRole,
        assignment_fence: Option<&laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) -> Option<crate::checkpoint_timing::CheckpointBarrierTimingContext> {
        let process = controller
            .try_live_local_process_authority_identity()
            .ok()?;
        let assignment_fence = assignment_fence?;
        if !process.is_canonical()
            || !attempt.is_canonical()
            || !assignment_fence.is_canonical()
            || assignment_fence.participant_incarnation(process.participant.node_id)
                != Some(process.participant.boot_incarnation)
        {
            return None;
        }
        Some(crate::checkpoint_timing::CheckpointBarrierTimingContext {
            process,
            attempt,
            role,
            assignment_version: assignment_fence.assignment_version,
            assignment_digest: assignment_fence.digest(),
        })
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
        self.checkpoint_tail_tasks
            .spawn_on(tail, &self.checkpoint_tail_runtime);
    }

    #[cfg(feature = "cluster")]
    fn record_checkpoint_alignment_error(&mut self, error: &crate::error::DbError) {
        let reason = error.to_string();
        if error.requires_pipeline_halt() {
            if self.pipeline_halt.is_none() {
                self.pipeline_halt = Some(reason);
            }
            self.shutdown_signal.notify_one();
        } else {
            set_checkpoint_fault(&self.checkpoint_fault, reason);
        }
    }

    fn record_pipeline_halt(&mut self, error: &crate::pipeline::CycleError) {
        if matches!(error, crate::pipeline::CycleError::Halt(_)) && self.pipeline_halt.is_none() {
            self.pipeline_halt = Some(error.to_string());
            self.shutdown_signal.notify_one();
        }
    }

    /// Classify a graph error. Terminal errors signal shutdown before returning `Halt`.
    fn map_graph_error(
        err: &crate::error::DbError,
        shutdown: &tokio::sync::Notify,
    ) -> crate::pipeline::CycleError {
        use crate::pipeline::CycleError;
        if err.requires_pipeline_halt() {
            match err {
                crate::error::DbError::PipelineTerminal(msg) => tracing::error!(
                    reason = %msg,
                    "deterministic record-path failure; halting pipeline"
                ),
                crate::error::DbError::BackpressureFail(msg) => tracing::error!(
                    reason = %msg,
                    "backpressure_policy=Fail tripped; halting pipeline"
                ),
                crate::error::DbError::ShuffleTerminal(msg) => tracing::error!(
                    reason = %msg,
                    "permanent shuffle routing failure; halting pipeline"
                ),
                crate::error::DbError::ManagedStateBudgetExceeded {
                    context,
                    accounted_bytes,
                    limit_bytes,
                } => tracing::error!(
                    context,
                    accounted_bytes,
                    limit_bytes,
                    "managed working-state budget exceeded; halting pipeline"
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
    fn cap_source_frontiers_by_cluster_min(
        source_frontiers: &mut FxHashMap<Arc<str>, InputFrontier>,
        cluster_wm: Option<i64>,
    ) {
        for frontier in source_frontiers.values_mut() {
            frontier.watermark = frontier.watermark.and_then(|watermark| {
                let capped = Self::cap_watermark_by_cluster_min(watermark, cluster_wm);
                (capped != i64::MIN).then_some(capped)
            });
        }
    }

    #[cfg(feature = "cluster")]
    fn source_frontier_is_idle(&self, source_name: &str) -> bool {
        self.source_ids
            .get(source_name)
            .and_then(|source_id| {
                self.tracker
                    .as_ref()
                    .map(|tracker| tracker.is_idle(*source_id))
            })
            .unwrap_or(false)
    }

    /// Select the decision-bound lateness/activation floor for one source.
    ///
    /// Active local progress is capped by the committed cut so speculative progress cannot drop
    /// replay. An idle source has already been excluded by that durable cut, so its revival must
    /// resume at the cut rather than below an irreversible temporal output frontier.
    #[cfg(feature = "cluster")]
    fn decision_bound_source_floor(local_floor: i64, committed: Option<i64>, idle: bool) -> i64 {
        match committed {
            None => i64::MIN,
            Some(committed) if idle => committed,
            Some(committed) => local_floor.min(committed),
        }
    }

    #[cfg(feature = "cluster")]
    fn decision_bound_source_admission_floor(&self, source_name: &str, local_floor: i64) -> i64 {
        Self::decision_bound_source_floor(
            local_floor,
            self.committed_source_watermarks_snapshot
                .get(source_name)
                .copied(),
            self.source_frontier_is_idle(source_name),
        )
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
            flags: announcement.flags,
            leader_proof: announcement.leader_proof.as_ref()?.clone(),
        })
    }

    /// Release an exact pre-capture follower reservation after cluster control resolved it.
    #[cfg(feature = "cluster")]
    fn finish_pending_follower_attempt(
        &mut self,
        attempt: CheckpointAttempt,
    ) -> Result<(), String> {
        let Some(announcement) = self.pending_follower_checkpoint.as_ref() else {
            let error = format!(
                "follower checkpoint {} epoch {} has no pending announcement to resolve",
                attempt.checkpoint_id, attempt.epoch
            );
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            return Err(error);
        };
        if CheckpointAttempt::new(announcement.epoch, announcement.checkpoint_id) != attempt {
            let error = format!(
                "follower resolution identity epoch={} id={} does not match pending epoch={} id={}",
                attempt.epoch,
                attempt.checkpoint_id,
                announcement.epoch,
                announcement.checkpoint_id
            );
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            return Err(error);
        }
        let flags = announcement.flags;
        let Some(identity) = Self::certified_announcement(announcement) else {
            let error = format!(
                "follower checkpoint {} epoch {} lost its certified identity during resolution",
                attempt.checkpoint_id, attempt.epoch
            );
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            return Err(error);
        };
        if let Err(error) = self.follower_tail.finish(&identity, false) {
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            return Err(error);
        }
        let barrier = laminar_core::checkpoint::CheckpointBarrier {
            checkpoint_id: attempt.checkpoint_id,
            epoch: attempt.epoch,
            flags,
        };
        for control in &self.barrier_injectors {
            control.cancel_exact(barrier);
        }
        self.pending_follower_checkpoint = None;
        Ok(())
    }

    /// Leader durable tail: quorum + `Aligned` pre-mutex, then durable writes under the FIFO mutex.
    async fn run_leader_tail(mut tail: LeaderTail) {
        let attempt = tail.attempt;
        let deadline = tail.attempt_deadline;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            let error = format!(
                "checkpoint {} epoch {} exhausted its {:?} end-to-end deadline before the durable tail",
                attempt.checkpoint_id, attempt.epoch, tail.checkpoint_timeout
            );
            fail_reserved_leader_attempt(&mut tail, error.clone(), error).await;
            return;
        }
        if tail.operator_state.is_none() {
            let error = format!(
                "checkpoint {} epoch {} lost its captured operator image before capture quorum",
                attempt.checkpoint_id, attempt.epoch
            );
            fail_reserved_leader_attempt(&mut tail, error.clone(), error).await;
            return;
        }

        #[cfg(feature = "cluster")]
        let Some(quorum) = Self::capture_leader_quorum(&mut tail, deadline).await
        else {
            return;
        };
        #[cfg(not(feature = "cluster"))]
        let quorum = crate::checkpoint_coordinator::QuorumStage::RunInline;

        Self::execute_leader_tail(tail, quorum, deadline).await;
    }

    #[cfg(feature = "cluster")]
    async fn wait_for_capture_quorum_until(
        controller: &Arc<laminar_core::cluster::control::ClusterController>,
        deadline: tokio::time::Instant,
        checkpoint_timeout: Duration,
        prepare: crate::checkpoint_coordinator::PrepareQuorum<'_>,
    ) -> Result<
        (
            CheckpointWatermark,
            Vec<laminar_core::cluster::discovery::NodeId>,
            bool,
        ),
        String,
    > {
        let prepared_wait_timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        tokio::time::timeout_at(
            deadline,
            crate::checkpoint_coordinator::CheckpointCoordinator::run_prepare_quorum(
                controller,
                prepared_wait_timeout,
                prepare,
            ),
        )
        .await
        .map_err(|_| {
            format!(
                "capture quorum exhausted the {checkpoint_timeout:?} end-to-end checkpoint deadline"
            )
        })?
    }

    #[cfg(feature = "cluster")]
    async fn capture_leader_quorum(
        tail: &mut LeaderTail,
        deadline: tokio::time::Instant,
    ) -> Option<crate::checkpoint_coordinator::QuorumStage> {
        use crate::checkpoint_coordinator::{PrepareQuorum, QuorumStage};
        use laminar_core::cluster::control::{BarrierAnnouncement, Phase};

        let Some(controller) = tail.controller.as_ref() else {
            return Some(QuorumStage::RunInline);
        };
        let Some(assignment_fence) = tail.request.assignment_fence.as_ref() else {
            Self::handle_leader_pre_tail_failure(
                tail,
                "[LDB-6055] clustered durable tail lost its assignment certificate".into(),
            )
            .await;
            return None;
        };
        let Some(leader_proof) = tail.leader_proof.as_ref() else {
            Self::handle_leader_pre_tail_failure(
                tail,
                "clustered durable tail lost its exact leader proof".into(),
            )
            .await;
            return None;
        };
        if !controller.proof_is_live(leader_proof) {
            Self::handle_leader_pre_tail_failure(
                tail,
                "leadership changed before checkpoint quorum".into(),
            )
            .await;
            return None;
        }
        let attempt = tail.attempt;
        let (epoch, checkpoint_id) = (attempt.epoch, attempt.checkpoint_id);
        let quorum_result = Self::wait_for_capture_quorum_until(
            controller,
            deadline,
            tail.checkpoint_timeout,
            PrepareQuorum::new(
                attempt,
                tail.local_watermark,
                assignment_fence,
                leader_proof,
                tail.request.flags,
            ),
        )
        .await;

        match quorum_result {
            Ok((cluster_watermark, participants, follower_replay_pending)) => {
                tail.handoff.replay_pending |= follower_replay_pending;
                tail.request.handoff_replay_pending = tail.handoff.replay_pending;
                if tail.handoff.replay_pending {
                    tail.request.reassignment_portable = false;
                }
                let aligned_result = tokio::time::timeout_at(
                    deadline,
                    controller.announce_barrier(&BarrierAnnouncement {
                        epoch,
                        checkpoint_id,
                        assignment_fence: Some(assignment_fence.clone()),
                        leader_proof: Some(leader_proof.clone()),
                        phase: Phase::Aligned,
                        flags: tail.request.flags,
                    }),
                )
                .await;
                match aligned_result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) if !controller.proof_is_live(leader_proof) => {
                        Self::handle_leader_pre_tail_failure(
                            tail,
                            format!(
                                "leadership changed while publishing checkpoint Aligned: {error}"
                            ),
                        )
                        .await;
                        return None;
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(
                            epoch, %error,
                            "[LDB-6031] aligned announcement failed; peers resume on Commit"
                        );
                    }
                    Err(_) => {
                        Self::handle_leader_pre_tail_failure(
                            tail,
                            format!(
                                "Aligned publication exhausted the {:?} end-to-end checkpoint deadline",
                                tail.checkpoint_timeout
                            ),
                        )
                        .await;
                        return None;
                    }
                }
                if !controller.proof_is_live(leader_proof) {
                    Self::handle_leader_pre_tail_failure(
                        tail,
                        "leadership changed after checkpoint Aligned publication".into(),
                    )
                    .await;
                    return None;
                }
                Some(QuorumStage::Captured {
                    cluster_watermark,
                    participants,
                    leader_proof: leader_proof.clone(),
                })
            }
            Err(message) => {
                Self::handle_leader_pre_tail_failure(tail, message).await;
                None
            }
        }
    }

    #[cfg(feature = "cluster")]
    async fn handle_leader_pre_tail_failure(tail: &mut LeaderTail, message: String) {
        let attempt = tail.attempt;
        let (epoch, checkpoint_id) = (attempt.epoch, attempt.checkpoint_id);
        tracing::error!(
            checkpoint_id,
            epoch,
            error = %message,
            "[LDB-6032] checkpoint failed before its durable tail"
        );
        let terminal_error = format!(
            "checkpoint {checkpoint_id} epoch {epoch} failed before its durable tail: {message}"
        );
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
            fail_reserved_leader_attempt(&mut tail, error.clone(), error).await;
            return;
        };
        let serialized_operator_state = match operator_state
            .serialize_until(
                tail.operator_state_staged_cap_bytes,
                tail.serialization_timeout,
                deadline,
            )
            .await
        {
            Ok(states) => states,
            Err(error) => {
                fail_reserved_leader_attempt(&mut tail, error.clone(), error).await;
                return;
            }
        };
        tail.request.state_frames = serialized_operator_state.frames;
        tail.request.managed_vnode_operators = serialized_operator_state.managed_vnode_operators;
        tail.mutable_operator_capture_guard = serialized_operator_state.mutable_capture_guard;

        let source_offsets =
            match materialize_source_checkpoints_until(tail.fan_out.clone(), attempt, deadline)
                .await
            {
                Ok(offsets) => offsets,
                Err(error) => {
                    fail_reserved_leader_attempt(&mut tail, error.clone(), error).await;
                    return;
                }
            };
        tail.request.source_offset_overrides = source_offsets;

        let Ok(mut guard) = tokio::time::timeout_at(deadline, tail.coordinator.lock()).await else {
            let error = format!(
                "checkpoint {} epoch {} exceeded its {:?} end-to-end deadline waiting for the coordinator",
                attempt.checkpoint_id, attempt.epoch, tail.checkpoint_timeout
            );
            fail_reserved_leader_attempt(&mut tail, error.clone(), error).await;
            return;
        };

        let Some(coordinator) = guard.as_mut() else {
            drop(guard);
            let error = format!(
                "checkpoint {} epoch {} coordinator disappeared before the durable tail",
                attempt.checkpoint_id, attempt.epoch
            );
            fail_reserved_leader_attempt(&mut tail, error.clone(), error).await;
            return;
        };
        coordinator.set_local_watermark(tail.local_watermark);
        let result = coordinator
            .checkpoint_preallocated_started(
                std::mem::take(&mut tail.request),
                attempt,
                quorum,
                tail.attempt_started,
                deadline,
            )
            .await;
        if result.as_ref().is_ok_and(|result| result.success)
            && coordinator.committed_manifest_needs_vnode_rebase(attempt)
        {
            tail.full_vnode_capture_needed
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        // Completion delivery may wait on a bounded channel; it must not hold
        // the FIFO checkpoint coordinator lock while doing so.
        drop(guard);
        Self::handle_leader_result(&mut tail, result).await;
    }

    async fn handle_leader_result(
        tail: &mut LeaderTail,
        result: Result<crate::checkpoint_coordinator::CheckpointResult, DbError>,
    ) {
        let attempt = tail.attempt;
        match result {
            Ok(result) if result.success => {
                Self::complete_successful_leader_tail(tail, result).await;
            }
            Ok(result) => {
                tail.full_vnode_capture_needed
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
                tail.in_flight.fail_sink_epoch(terminal_error.clone());
                if checkpoint_failure_requires_pipeline_fault(
                    &result,
                    tail.fault_on_retryable_failure,
                ) {
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
                tail.full_vnode_capture_needed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                tracing::warn!(%error, "Barrier-aligned checkpoint error");
                let terminal_error = error.to_string();
                tail.in_flight.fail_sink_epoch(terminal_error.clone());
                if tail.fault_on_unclassified_error {
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

    /// Build the follower's durable tail (ack → prepare → decision wait → 2PC).
    #[cfg(feature = "cluster")]
    fn follower_tail_future(
        &mut self,
        request: crate::checkpoint_coordinator::CheckpointRequest,
        operator_state: CapturedOperatorState,
        identity: CertifiedCheckpointAttempt,
        fan_out: FxHashMap<String, SourceCheckpoint>,
        attempt_started: std::time::Instant,
        attempt_deadline: tokio::time::Instant,
    ) -> Result<FollowerDurableTail, String> {
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
        let operator_state_staged_cap_bytes = self.checkpoint_state_cap_bytes;
        let local_watermark = classify_channel_progress(&request.channel_progress)?;
        let handoff_replay_pending = request.handoff_replay_pending;
        let attempt = identity.attempt;
        if identity.assignment_digest != assignment_fence.digest()
            || identity.flags != request.flags
            || !identity.leader_proof.is_canonical()
            || assignment_fence.participant_incarnation(identity.leader_proof.owner.node_id)
                != Some(identity.leader_proof.owner.boot_id)
        {
            return Err(
                "[LDB-6055] follower durable tail has an invalid certified authority binding"
                    .into(),
            );
        }

        let in_flight = EpochInFlightGuard::claim(
            &self.checkpoint_in_flight,
            &self.checkpoint_fault,
            attempt,
            self.sinks
                .iter()
                .filter(|(_, handle, _, _, _, _)| handle.checkpoint_committable())
                .map(|(_, handle, _, _, _, _)| handle.clone()),
        )?;
        let tail = FollowerDurableTail {
            in_flight,
            coordinator: Arc::clone(&self.coordinator),
            state: Arc::clone(&self.follower_tail),
            complete_tx: self.checkpoint_complete_tx.clone(),
            controller: self.cluster_controller.clone(),
            full_vnode_capture_needed: Arc::clone(&self.full_vnode_capture_needed),
            checkpoint_fault: Arc::clone(&self.checkpoint_fault),
            request,
            operator_state: Some(operator_state),
            operator_state_staged_cap_bytes,
            mutable_operator_capture_guard: None,
            assignment_fence,
            identity,
            fan_out,
            local_watermark,
            handoff_replay_pending,
            attempt,
            attempt_started,
            attempt_deadline,
            checkpoint_timeout: self.checkpoint_timeout,
            serialization_timeout: self.serialization_timeout,
            checkpoint_cleanup_timeout: self.checkpoint_cleanup_timeout,
        };
        Ok(tail)
    }

    #[cfg(feature = "cluster")]
    async fn run_follower_tail(mut tail: FollowerDurableTail) {
        let deadline = tail.attempt_deadline;

        if tail.operator_state.is_none() {
            let error = format!(
                "checkpoint {} epoch {} lost its captured operator image",
                tail.attempt.checkpoint_id, tail.attempt.epoch
            );
            Self::fail_follower_tail_before_prepare(&mut tail, error, deadline).await;
            return;
        }
        let captured_acknowledgement = laminar_core::cluster::control::BarrierAck {
            epoch: tail.attempt.epoch,
            checkpoint_id: tail.attempt.checkpoint_id,
            assignment_digest: Some(tail.assignment_fence.digest()),
            flags: tail.identity.flags,
            disposition: if tail.handoff_replay_pending {
                laminar_core::cluster::control::BarrierAckDisposition::CapturedWithReplay
            } else {
                laminar_core::cluster::control::BarrierAckDisposition::Captured
            },
            error: None,
            watermark: tail.local_watermark,
        };
        if let Err(error) = Self::acknowledge_follower_captured(
            tail.controller.clone(),
            captured_acknowledgement,
            deadline,
        )
        .await
        {
            // The acknowledgement result is ambiguous: the leader may already have consumed the
            // exact Captured proof before the response was lost. Retain the attempt identity and
            // fault through authoritative recovery instead of reopening the same ID for a second
            // capture.
            Self::fail_follower_tail_after_capture(tail, error.to_string(), deadline).await;
            return;
        }
        let operator_state = tail
            .operator_state
            .take()
            .expect("captured follower image was checked before its acknowledgement");
        let serialized_operator_state = match operator_state
            .serialize_until(
                tail.operator_state_staged_cap_bytes,
                tail.serialization_timeout,
                deadline,
            )
            .await
        {
            Ok(states) => states,
            Err(error) => {
                Self::fail_follower_tail_after_capture(tail, error, deadline).await;
                return;
            }
        };
        tail.request.state_frames = serialized_operator_state.frames;
        tail.request.managed_vnode_operators = serialized_operator_state.managed_vnode_operators;
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
                Self::fail_follower_tail_after_capture(tail, error, deadline).await;
                return;
            }
        };
        tail.request.source_offset_overrides = source_offsets;

        let prepared = match Self::prepare_follower_tail_until(&mut tail, deadline).await {
            Ok(prepared) => prepared,
            Err(error) => {
                Self::fail_follower_tail_after_capture(tail, error.to_string(), deadline).await;
                return;
            }
        };
        // Decision observation deliberately happens outside the coordinator mutex so a successor
        // epoch can prepare without queuing behind this attempt's control-plane wait.
        let outcome = Self::apply_follower_decision_until(&mut tail, Ok(prepared), deadline).await;
        Self::complete_follower_tail(tail, outcome).await;
    }

    #[cfg(feature = "cluster")]
    async fn fail_follower_tail_after_capture(
        tail: FollowerDurableTail,
        error: String,
        deadline: tokio::time::Instant,
    ) {
        tail.full_vnode_capture_needed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = Self::reject_follower_capture(
            tail.controller.as_deref(),
            tail.checkpoint_fault.as_ref(),
            tail.attempt,
            Some(tail.assignment_fence.digest()),
            tail.identity.flags,
            error.clone(),
            deadline,
        )
        .await;
        Self::complete_follower_tail(tail, Err(DbError::Checkpoint(error))).await;
    }

    #[cfg(feature = "cluster")]
    async fn fail_follower_tail_before_prepare(
        tail: &mut FollowerDurableTail,
        error: String,
        deadline: tokio::time::Instant,
    ) {
        tail.in_flight.fail_sink_epoch(error.clone());
        let controller = tail.controller.clone();
        let checkpoint_fault = Arc::clone(&tail.checkpoint_fault);
        let state = Arc::clone(&tail.state);
        let identity = tail.identity.clone();
        let full_vnode_capture_needed = Arc::clone(&tail.full_vnode_capture_needed);
        let attempt = tail.attempt;
        let assignment_digest = tail.assignment_fence.digest();
        let flags = tail.identity.flags;
        let rejected = Self::reject_follower_capture(
            controller.as_deref(),
            checkpoint_fault.as_ref(),
            attempt,
            Some(assignment_digest),
            flags,
            error,
            deadline,
        )
        .await;
        if rejected.is_ok() {
            if let Err(finish_error) = state.finish(&identity, false) {
                set_checkpoint_fault(&checkpoint_fault, finish_error);
            }
        }
        full_vnode_capture_needed.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(feature = "cluster")]
    async fn acknowledge_follower_captured(
        controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
        acknowledgement: laminar_core::cluster::control::BarrierAck,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let controller = controller.ok_or_else(|| {
            DbError::Checkpoint("follower captured without a cluster controller".into())
        })?;
        tokio::time::timeout_at(deadline, controller.ack_barrier(&acknowledgement))
            .await
            .map_err(|_| DbError::Checkpoint("follower captured acknowledgement timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("follower captured acknowledgement failed: {error}"))
            })
    }

    #[cfg(feature = "cluster")]
    async fn prepare_follower_tail_until(
        tail: &mut FollowerDurableTail,
        deadline: tokio::time::Instant,
    ) -> Result<crate::checkpoint_coordinator::FollowerPrepareOutcome, DbError> {
        let request = std::mem::take(&mut tail.request);
        let attempt = tail.attempt;
        let mut guard = tokio::time::timeout_at(deadline, tail.coordinator.lock())
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "[LDB-6046] follower checkpoint {} epoch {} exceeded its {:?} end-to-end \
                     deadline while waiting for checkpoint coordinator ownership",
                    attempt.checkpoint_id, attempt.epoch, tail.checkpoint_timeout
                ))
            })?;
        let coordinator = guard.as_mut().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6045] checkpoint coordinator disappeared before follower prepare".into(),
            )
        })?;
        coordinator.set_local_watermark(tail.local_watermark);
        coordinator
            .follower_prepare_acked_until(
                request,
                tail.identity.leader_proof.clone(),
                attempt.epoch,
                attempt.checkpoint_id,
                deadline,
            )
            .await
    }

    #[cfg(feature = "cluster")]
    async fn apply_follower_decision_until(
        tail: &mut FollowerDurableTail,
        prepared: Result<crate::checkpoint_coordinator::FollowerPrepareOutcome, DbError>,
        deadline: tokio::time::Instant,
    ) -> Result<bool, DbError> {
        use crate::checkpoint_coordinator::{CheckpointCoordinator, FollowerPrepareOutcome};

        let local_prepare = prepared?;
        let controller = tail.controller.clone().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6045] follower durable tail lost its decision dependencies".into(),
            )
        })?;
        let attempt = tail.attempt;
        let assignment_fence = tail.assignment_fence.clone();
        let coordinator = Arc::clone(&tail.coordinator);
        let full_vnode_capture_needed = Arc::clone(&tail.full_vnode_capture_needed);
        let attempt_started = tail.attempt_started;
        let checkpoint_cleanup_timeout = tail.checkpoint_cleanup_timeout;
        let terminal_handoff =
            crate::checkpoint_coordinator::sink_epoch_admission::is_terminal_handoff(
                tail.identity.flags,
                tail.handoff_replay_pending,
            );
        if local_prepare == FollowerPrepareOutcome::InDoubt {
            tracing::debug!(
                checkpoint_id = attempt.checkpoint_id,
                epoch = attempt.epoch,
                "preserving in-doubt follower preparation through terminal observation"
            );
        }
        // Capture/persistence authority still ends at `deadline`. Terminal observation gets one
        // separate cleanup window because the leader may consume the full attempt deadline
        // proving manifest readiness and only then publish its durable Abort.
        let decision_deadline = deadline
            .checked_add(checkpoint_cleanup_timeout)
            .ok_or_else(|| {
                DbError::Checkpoint("follower terminal observation deadline overflowed".into())
            })?;
        let decision_timeout =
            decision_deadline.saturating_duration_since(tokio::time::Instant::now());
        let committed = CheckpointCoordinator::await_follower_decision(
            &controller,
            attempt.epoch,
            attempt.checkpoint_id,
            &assignment_fence,
            decision_timeout,
        )
        .await?;

        let cleanup_deadline = tokio::time::Instant::now() + checkpoint_cleanup_timeout;
        tokio::time::timeout_at(cleanup_deadline, async {
            let mut guard = coordinator.lock().await;
            let coordinator = guard.as_mut().ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6045] checkpoint coordinator disappeared while a follower decision \
                     was pending"
                        .into(),
                )
            })?;
            let committed = coordinator
                .follower_finish_deferred(
                    attempt.epoch,
                    attempt.checkpoint_id,
                    committed,
                    attempt_started,
                    terminal_handoff,
                )
                .await?;
            if committed && coordinator.committed_manifest_needs_vnode_rebase(attempt) {
                full_vnode_capture_needed.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            Ok(committed)
        })
        .await
        .unwrap_or_else(|_| {
            Err(DbError::Checkpoint(format!(
                "[LDB-6046] follower checkpoint {} epoch {} exceeded its {:?} cleanup deadline \
                 while applying the durable decision",
                attempt.checkpoint_id, attempt.epoch, checkpoint_cleanup_timeout
            )))
        })
    }

    #[cfg(feature = "cluster")]
    async fn complete_follower_tail(mut tail: FollowerDurableTail, outcome: Result<bool, DbError>) {
        let attempt = tail.attempt;
        let committed = match tail.state.finish_resolved(&tail.identity, &outcome) {
            Ok(Some(committed)) => committed,
            Ok(None) => {
                let Err(error) = outcome else {
                    tail.in_flight.fail_sink_epoch(
                        "follower terminal bookkeeping lost an authoritative outcome",
                    );
                    set_checkpoint_fault(
                        &tail.checkpoint_fault,
                        "follower terminal bookkeeping lost an authoritative outcome",
                    );
                    tail.full_vnode_capture_needed
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    return;
                };
                tracing::error!(
                    epoch = attempt.epoch,
                    checkpoint_id = attempt.checkpoint_id,
                    %error,
                    "follower checkpoint is in-doubt; faulting pipeline",
                );
                tail.in_flight.fail_sink_epoch(error.to_string());
                set_checkpoint_fault(&tail.checkpoint_fault, error.to_string());
                tail.full_vnode_capture_needed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                return;
            }
            Err(error) => {
                tail.in_flight.fail_sink_epoch(error.clone());
                set_checkpoint_fault(&tail.checkpoint_fault, error);
                tail.full_vnode_capture_needed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                return;
            }
        };
        let terminal_handoff =
            crate::checkpoint_coordinator::sink_epoch_admission::is_terminal_handoff(
                tail.identity.flags,
                tail.handoff_replay_pending,
            );
        let sink_transition_resolved = if committed {
            if terminal_handoff {
                true
            } else {
                match tail.in_flight.publish_successor() {
                    Ok(()) => true,
                    Err(error) => {
                        set_checkpoint_fault(
                        &tail.checkpoint_fault,
                        format!(
                            "follower checkpoint {} epoch {} committed, but successor sink publication failed: {error}",
                            attempt.checkpoint_id, attempt.epoch
                        ),
                    );
                        false
                    }
                }
            }
        } else {
            tail.in_flight.fail_sink_epoch(format!(
                "follower checkpoint {} epoch {} aborted",
                attempt.checkpoint_id, attempt.epoch
            ));
            false
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
            CheckpointCompletion::new(attempt, tail.fan_out, tail.handoff_replay_pending)
        } else {
            CheckpointCompletion::failed(attempt, "checkpoint aborted by the cluster leader")
        };
        let report_deadline = tokio::time::Instant::now() + CHECKPOINT_FAILURE_REPORT_TIMEOUT;
        let reported =
            deliver_checkpoint_completion(&tail.complete_tx, completion, report_deadline).await;
        if !reported {
            set_checkpoint_fault(
                &tail.checkpoint_fault,
                format!(
                    "follower checkpoint {} epoch {} reached an authoritative terminal outcome \
                     but its completion could not be reported within {:?}",
                    attempt.checkpoint_id, attempt.epoch, CHECKPOINT_FAILURE_REPORT_TIMEOUT,
                ),
            );
        }
        if reported && sink_transition_resolved {
            tail.in_flight.disarm_sink_epoch();
        }
        if !committed {
            // Follower capture is destructive; every uncommitted outcome must re-base FULL next.
            tail.full_vnode_capture_needed
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Reject a follower capture or pre-prepare step under the attempt deadline.
    ///
    /// A negative acknowledgement prevents the leader from admitting this participant.
    #[cfg(feature = "cluster")]
    async fn reject_follower_capture(
        controller: Option<&laminar_core::cluster::control::ClusterController>,
        checkpoint_fault: &parking_lot::Mutex<Option<String>>,
        attempt: CheckpointAttempt,
        assignment_digest: Option<[u8; 32]>,
        flags: u64,
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
            flags,
            disposition: laminar_core::cluster::control::BarrierAckDisposition::Failed,
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

    #[cfg(feature = "cluster")]
    async fn await_rejected_follower_settlement(
        controller: &laminar_core::cluster::control::ClusterController,
        attempt: CheckpointAttempt,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        deadline: tokio::time::Instant,
    ) -> Result<(CheckpointAttempt, [u8; 32]), String> {
        use laminar_core::cluster::control::Phase;

        const AUDIT_FALLBACK_INITIAL: Duration = Duration::from_millis(250);
        const AUDIT_FALLBACK_MAX: Duration = Duration::from_secs(2);

        let authority = controller.checkpoint_authority().map_err(|error| {
            format!("rejected follower checkpoint {attempt:?} has no durable authority: {error}")
        })?;
        let mut audit_fallback = AUDIT_FALLBACK_INITIAL;
        let mut terminal_hint_consumed = false;
        loop {
            if !controller.process_lease_is_live() {
                return Err(format!(
                    "rejected follower checkpoint {attempt:?} lost process authority during durable settlement"
                ));
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(format!(
                    "rejected follower checkpoint {attempt:?} has no immutable Abort or newer terminal outcome"
                ));
            }
            let settlement = tokio::select! {
                biased;
                () = controller.wait_for_process_lease_loss() => {
                    return Err(format!(
                        "rejected follower checkpoint {attempt:?} lost process authority during durable settlement"
                    ));
                }
                result = tokio::time::timeout_at(
                    deadline,
                    authority.cluster_attempt_settlement(attempt),
                ) => result,
            }
            .map_err(|_| {
                format!(
                    "durable settlement audit for rejected follower checkpoint {attempt:?} timed out"
                )
            })?
            .map_err(|error| {
                format!(
                    "durable settlement audit for rejected follower checkpoint {attempt:?} failed: {error}"
                )
            })?;
            let Some(outcome) = settlement else {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    continue;
                }
                let wait = audit_fallback.min(remaining);
                if terminal_hint_consumed {
                    tokio::select! {
                        biased;
                        () = controller.wait_for_process_lease_loss() => {
                            return Err(format!(
                                "rejected follower checkpoint {attempt:?} lost process authority during durable settlement"
                            ));
                        }
                        () = tokio::time::sleep(wait) => {}
                    }
                } else {
                    let observed = tokio::select! {
                        biased;
                        () = controller.wait_for_process_lease_loss() => {
                            return Err(format!(
                                "rejected follower checkpoint {attempt:?} lost process authority during durable settlement"
                            ));
                        }
                        result = controller.wait_for_barrier(
                                |candidate| {
                                    let candidate_attempt = CheckpointAttempt::new(
                                        candidate.epoch,
                                        candidate.checkpoint_id,
                                    );
                                    match candidate_attempt.relation_to(attempt) {
                                        CheckpointAttemptRelation::Newer => {
                                            matches!(candidate.phase, Phase::Commit | Phase::Abort)
                                        }
                                        CheckpointAttemptRelation::Exact => {
                                            candidate.phase == Phase::Abort
                                        }
                                        CheckpointAttemptRelation::Older
                                        | CheckpointAttemptRelation::Conflict => false,
                                    }
                                },
                                wait,
                            ) => result,
                    }
                    .map_err(|error| {
                            format!(
                                "terminal hint observation for rejected follower checkpoint {attempt:?} failed: {error}"
                            )
                        })?;
                    terminal_hint_consumed = observed.is_some();
                }
                audit_fallback = audit_fallback.saturating_mul(2).min(AUDIT_FALLBACK_MAX);
                continue;
            };
            let settled = CheckpointAttempt::new(outcome.epoch, outcome.checkpoint_id);
            match settled.relation_to(attempt) {
                CheckpointAttemptRelation::Exact => {
                    if outcome.assignment_fence.as_ref() != Some(assignment_fence) {
                        return Err(format!(
                            "durable outcome for rejected follower checkpoint {attempt:?} has a different assignment certificate"
                        ));
                    }
                    if outcome.is_commit() {
                        return Err(format!(
                            "rejected follower checkpoint {attempt:?} conflicts with its durable Commit"
                        ));
                    }
                    return Ok((settled, assignment_fence.digest()));
                }
                CheckpointAttemptRelation::Newer => {
                    let fence = outcome.assignment_fence.as_ref().ok_or_else(|| {
                        format!(
                            "newer durable settlement {settled:?} has no assignment certificate"
                        )
                    })?;
                    return Ok((settled, fence.digest()));
                }
                CheckpointAttemptRelation::Older | CheckpointAttemptRelation::Conflict => {
                    return Err(format!(
                        "durable settlement {settled:?} does not close rejected follower checkpoint {attempt:?}"
                    ));
                }
            }
        }
    }

    /// Hold the pipeline until the leader announces `Aligned` (or `Commit`/`Abort`/newer epoch).
    ///
    /// Prevents epoch-N+1 shuffle rows from reaching a peer still snapshotting epoch-N.
    /// No-op without a cross-node shuffle; bounded — on timeout the epoch aborts via the leader.
    #[cfg(feature = "cluster")]
    async fn wait_for_newer_terminal_outcome(
        controller: &laminar_core::cluster::control::ClusterController,
        attempt: CheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        let authority = controller.checkpoint_authority().map_err(|error| {
            format!("newer checkpoint {attempt:?} has no durable authority: {error}")
        })?;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(format!(
                    "newer checkpoint {attempt:?} has no immutable terminal outcome; pipeline \
                     remains fenced"
                ));
            }
            let observed =
                tokio::time::timeout(remaining, authority.cluster_attempt_settlement(attempt))
                    .await
                    .map_err(|_| {
                        format!(
                    "durable outcome read for newer checkpoint {attempt:?} exhausted the shuffle \
                     resume deadline"
                )
                    })?
                    .map_err(|error| {
                        format!(
                            "durable outcome read for newer checkpoint {attempt:?} failed: {error}"
                        )
                    })?;
            if let Some(outcome) = observed {
                let settled = CheckpointAttempt::new(outcome.epoch, outcome.checkpoint_id);
                match settled.relation_to(attempt) {
                    CheckpointAttemptRelation::Exact | CheckpointAttemptRelation::Newer => {
                        return Ok(());
                    }
                    CheckpointAttemptRelation::Older | CheckpointAttemptRelation::Conflict => {
                        return Err(format!(
                            "durable settlement {settled:?} does not close newer checkpoint \
                             {attempt:?}"
                        ));
                    }
                }
            }
            tokio::time::sleep(
                Duration::from_millis(250)
                    .min(deadline.saturating_duration_since(tokio::time::Instant::now())),
            )
            .await;
        }
    }

    #[cfg(feature = "cluster")]
    async fn wait_for_aligned_resume(
        has_cluster_shuffle: bool,
        controller: &laminar_core::cluster::control::ClusterController,
        identity: CertifiedCheckpointAttempt,
        assignment_fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
        prepared_wait_timeout: std::time::Duration,
    ) -> Result<(), String> {
        use laminar_core::cluster::control::Phase;

        // The gate must outlast the leader's quorum wait: a slow-but-successful alignment that lands
        // `Aligned` AFTER the follower resumes would let epoch-N+1 shuffle rows cross a peer's
        // closed epoch-N channel while that peer is still capturing. Derive the gate from
        // the durable-Prepared wait so the gate can never expire first (CL-6).
        let resume_gate_timeout = std::time::Duration::from_secs(10)
            .max(prepared_wait_timeout + std::time::Duration::from_secs(5));
        let resume_gate_deadline = tokio::time::Instant::now() + resume_gate_timeout;

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
                        CheckpointAttemptRelation::Newer => match a.phase {
                            Phase::Aligned => a
                                .assignment_fence
                                .as_ref()
                                .zip(a.leader_proof.as_ref())
                                .is_some_and(|(fence, proof)| {
                                    fence.is_canonical()
                                        && proof.is_canonical()
                                        && fence.participant_incarnation(proof.owner.node_id)
                                            == Some(proof.owner.boot_id)
                                }),
                            Phase::Commit | Phase::Abort => true,
                            Phase::Prepare => false,
                        },
                        CheckpointAttemptRelation::Exact => match a.phase {
                            // A successor may durably abort an attempt prepared by the old
                            // leader. The terminal record is only a wake-up hint; durable outcome
                            // validation owns its authority and performs the rollback.
                            Phase::Abort => a.flags == identity.flags,
                            Phase::Aligned => {
                                a.flags == identity.flags
                                    && a.assignment_fence.as_ref() == Some(assignment_fence)
                                    && a.leader_proof.as_ref() == Some(&identity.leader_proof)
                            }
                            Phase::Commit => {
                                a.flags == identity.flags
                                    && a.assignment_fence.as_ref() == Some(assignment_fence)
                            }
                            Phase::Prepare => false,
                        },
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

        if released.epoch == identity.attempt.epoch
            && released.checkpoint_id == identity.attempt.checkpoint_id
            && matches!(released.phase, Phase::Commit | Phase::Abort)
        {
            let remaining =
                resume_gate_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(format!(
                    "checkpoint {} epoch {} exhausted its shuffle resume deadline before the \
                     terminal outcome could be verified",
                    identity.attempt.checkpoint_id, identity.attempt.epoch
                ));
            }
            crate::checkpoint_coordinator::CheckpointCoordinator::await_follower_decision(
                controller,
                identity.attempt.epoch,
                identity.attempt.checkpoint_id,
                assignment_fence,
                remaining,
            )
            .await
            .map_err(|error| {
                format!(
                    "shuffle resume for checkpoint {} epoch {} could not verify its immutable \
                     terminal outcome: {error}",
                    identity.attempt.checkpoint_id, identity.attempt.epoch
                )
            })?;
        } else if CheckpointAttempt::new(released.epoch, released.checkpoint_id)
            .relation_to(identity.attempt)
            == CheckpointAttemptRelation::Newer
            && matches!(released.phase, Phase::Commit | Phase::Abort)
        {
            Self::wait_for_newer_terminal_outcome(
                controller,
                CheckpointAttempt::new(released.epoch, released.checkpoint_id),
                resume_gate_deadline,
            )
            .await?;
        } else if CheckpointAttempt::new(released.epoch, released.checkpoint_id)
            .relation_to(identity.attempt)
            == CheckpointAttemptRelation::Newer
            && released.phase == Phase::Aligned
        {
            let successor_fence = released.assignment_fence.as_ref().ok_or_else(|| {
                "newer Aligned announcement lost its assignment certificate".to_string()
            })?;
            let successor_proof = released
                .leader_proof
                .as_ref()
                .ok_or_else(|| "newer Aligned announcement lost its leader proof".to_string())?;
            if !successor_fence.contains(controller.instance_id().0) {
                return Err(format!(
                    "newer checkpoint epoch {} excludes this process from its certified \
                     assignment; the old pipeline remains fenced",
                    released.epoch
                ));
            }
            let remaining =
                resume_gate_deadline.saturating_duration_since(tokio::time::Instant::now());
            let certified = tokio::time::timeout(
                remaining,
                controller.checkpoint_assignment_fence_for_leader(
                    successor_fence.assignment_version,
                    successor_proof,
                ),
            )
            .await
            .map_err(|_| {
                format!(
                    "newer checkpoint epoch {} exhausted its shuffle resume deadline while \
                     certifying assignment {}",
                    released.epoch, successor_fence.assignment_version
                )
            })?;
            if certified.as_ref() != Some(successor_fence) {
                return Err(format!(
                    "newer checkpoint epoch {} has no locally certified assignment {}; the old \
                     pipeline remains fenced",
                    released.epoch, successor_fence.assignment_version
                ));
            }
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    async fn wait_for_aligned_resume_until(
        has_shuffle: bool,
        controller: &laminar_core::cluster::control::ClusterController,
        identity: CertifiedCheckpointAttempt,
        assignment_fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
        attempt_deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        let attempt = identity.attempt;
        let remaining_attempt =
            attempt_deadline.saturating_duration_since(tokio::time::Instant::now());
        tokio::time::timeout_at(
            attempt_deadline,
            Self::wait_for_aligned_resume(
                has_shuffle,
                controller,
                identity,
                assignment_fence,
                remaining_attempt,
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
        let announcement = match controller
            .observe_checkpoint_prepare_until(self.checkpoint_timeout)
            .await
        {
            Ok(Some(
                laminar_core::cluster::control::CheckpointPrepareObservation::AssignmentReady(
                    announcement,
                ),
            )) => announcement,
            Ok(Some(
                laminar_core::cluster::control::CheckpointPrepareObservation::AssignmentRejected {
                    announcement,
                    error,
                },
            )) => {
                let attempt =
                    CheckpointAttempt::new(announcement.epoch, announcement.checkpoint_id);
                let deadline = Self::follower_prepare_deadline(
                    controller,
                    &announcement,
                    self.checkpoint_timeout,
                );
                Self::reject_uncertified_follower_prepare(
                    controller,
                    &announcement,
                    error.clone(),
                    deadline,
                )
                .await;
                return FollowerPrepareAdmission::Failed { attempt, error };
            }
            Ok(None) => return FollowerPrepareAdmission::Idle,
            Err(error) => {
                set_checkpoint_fault(
                    &self.checkpoint_fault,
                    format!("follower checkpoint control observation failed: {error}"),
                );
                return FollowerPrepareAdmission::Idle;
            }
        };
        let attempt = CheckpointAttempt::new(announcement.epoch, announcement.checkpoint_id);
        let attempt_deadline =
            Self::follower_prepare_deadline(controller, &announcement, self.checkpoint_timeout);
        if let Some(error) = self.follower_prepare_assignment_error(controller, &announcement) {
            Self::reject_uncertified_follower_prepare(
                controller,
                &announcement,
                error.clone(),
                attempt_deadline,
            )
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
            .reserve_follower_prepare(
                controller,
                &announcement,
                announced_identity.clone(),
                attempt_deadline,
            )
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
        FollowerPrepareAdmission::Started {
            attempt,
            flags: announcement.flags,
        }
    }

    #[cfg(feature = "cluster")]
    fn follower_prepare_deadline(
        controller: &laminar_core::cluster::control::ClusterController,
        announcement: &laminar_core::cluster::control::BarrierAnnouncement,
        checkpoint_timeout: Duration,
    ) -> tokio::time::Instant {
        let started = controller
            .checkpoint_prepare_received_at(announcement)
            .unwrap_or_else(std::time::Instant::now);
        tokio::time::Instant::from_std(started) + checkpoint_timeout
    }

    #[cfg(feature = "cluster")]
    fn follower_prepare_assignment_error(
        &self,
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
        let unsupported = announcement.flags & !laminar_core::checkpoint::flags::HANDOFF;
        if unsupported != 0 {
            return Some(format!(
                "leader Prepare carried unsupported checkpoint flags {unsupported:#x}"
            ));
        }
        if announcement.flags & laminar_core::checkpoint::flags::HANDOFF != 0 {
            let Some(transition) = controller.checkpoint_drain_transition() else {
                return Some("handoff Prepare has no active assignment drain".into());
            };
            if transition.predecessor != *fence || transition.leader != *leader_proof {
                return Some("handoff Prepare does not match the active assignment drain".into());
            }
        }
        None
    }

    #[cfg(feature = "cluster")]
    async fn reject_uncertified_follower_prepare(
        controller: &laminar_core::cluster::control::ClusterController,
        announcement: &laminar_core::cluster::control::BarrierAnnouncement,
        error: String,
        deadline: tokio::time::Instant,
    ) {
        let acknowledgement = laminar_core::cluster::control::BarrierAck {
            epoch: announcement.epoch,
            checkpoint_id: announcement.checkpoint_id,
            assignment_digest: announcement
                .assignment_fence
                .as_ref()
                .map(laminar_core::cluster::control::CheckpointAssignmentFence::digest),
            flags: announcement.flags,
            disposition: laminar_core::cluster::control::BarrierAckDisposition::Failed,
            error: Some(error.clone()),
            watermark: CheckpointWatermark::Uninitialized,
        };
        let _ = tokio::time::timeout_at(deadline, controller.ack_barrier(&acknowledgement)).await;
        tracing::warn!(%error, "rejecting follower Prepare before barrier injection");
    }

    #[cfg(feature = "cluster")]
    async fn reserve_follower_prepare(
        &mut self,
        controller: &laminar_core::cluster::control::ClusterController,
        announcement: &laminar_core::cluster::control::BarrierAnnouncement,
        announced_identity: CertifiedCheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Result<FollowerAdmission, String> {
        match self.follower_tail.reserve(announced_identity.clone()) {
            Ok(admission) => Ok(admission),
            Err(error) => {
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                let acknowledgement = laminar_core::cluster::control::BarrierAck {
                    epoch: announcement.epoch,
                    checkpoint_id: announcement.checkpoint_id,
                    assignment_digest: Some(announced_identity.assignment_digest),
                    flags: announcement.flags,
                    disposition: laminar_core::cluster::control::BarrierAckDisposition::Failed,
                    error: Some(error.clone()),
                    watermark: CheckpointWatermark::Uninitialized,
                };
                let _ = tokio::time::timeout_at(deadline, controller.ack_barrier(&acknowledgement))
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
    ) -> Result<ShuffleAlignmentOutcome, DbError> {
        self.flush_cluster_shuffle_until(controller, attempt, assignment_fence, deadline)
            .await
    }

    /// Reach a distributed fixed point while every source remains held at the admitted cut.
    ///
    /// Each wave first drains local retained work to handoff quiescence, then publishes an ordered
    /// marker carrying whether that drain did any work. Wave zero is never terminal: it closes
    /// transport backlog that was not represented by a preceding activity marker. For later
    /// waves, every participant observes the same sender activity vector, so a completely idle
    /// vector plus local post-alignment quiescence proves no pre-cut row remains in transit or in
    /// operator channel state.
    #[cfg(feature = "cluster")]
    fn shuffle_flush_attempt_advanced(
        wave: u64,
        local_activity: bool,
        aligned: crate::operator_graph::ShuffleFlushWaveOutcome,
    ) -> bool {
        wave != 0 || local_activity || aligned.graph_state_staged
    }

    #[cfg(feature = "cluster")]
    fn log_checkpoint_barrier_phase_completed(
        attempt: CheckpointAttempt,
        role: &'static str,
        phase: &'static str,
        attempt_started: std::time::Instant,
    ) {
        tracing::info!(
            checkpoint_id = attempt.checkpoint_id,
            epoch = attempt.epoch,
            role,
            phase,
            elapsed = ?attempt_started.elapsed(),
            "checkpoint barrier phase completed"
        );
    }

    #[cfg(feature = "cluster")]
    async fn flush_cluster_shuffle_until(
        &mut self,
        controller: &laminar_core::cluster::control::ClusterController,
        attempt: CheckpointAttempt,
        assignment_fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
        deadline: tokio::time::Instant,
    ) -> Result<ShuffleAlignmentOutcome, DbError> {
        let flush_started_at = std::time::Instant::now();
        let mut wave = 0_u64;
        let mut saw_local_activity = false;
        let mut saw_peer_activity = false;
        let mut saw_graph_staging = false;
        tracing::info!(
            checkpoint_id = attempt.checkpoint_id,
            epoch = attempt.epoch,
            "checkpoint distributed shuffle fixed-point flush started"
        );
        loop {
            let local_activity = self
                .drain_handoff_edges_until_inner(deadline)
                .await
                .map_err(|error| match error {
                    crate::pipeline::CycleError::Halt(reason) => DbError::PipelineTerminal(
                        format!("shuffle flush wave {wave} graph drain halted: {reason}"),
                    ),
                    crate::pipeline::CycleError::Fatal(reason)
                    | crate::pipeline::CycleError::Recovery(reason) => DbError::Checkpoint(
                        format!("shuffle flush wave {wave} graph drain failed: {reason}"),
                    ),
                })?;
            let watermark = self.effective_pipeline_watermark();
            let aligned = self
                .graph
                .align_shuffle_flush_wave(
                    attempt,
                    wave,
                    local_activity,
                    watermark,
                    assignment_fence,
                    deadline,
                    Some(controller),
                )
                .await?;
            saw_local_activity |= local_activity;
            saw_peer_activity |= aligned.peer_activity;
            saw_graph_staging |= aligned.graph_state_staged;
            tracing::debug!(
                checkpoint_id = attempt.checkpoint_id,
                epoch = attempt.epoch,
                wave,
                local_activity,
                peer_activity = aligned.peer_activity,
                graph_state_staged = aligned.graph_state_staged,
                elapsed = ?flush_started_at.elapsed(),
                "checkpoint distributed shuffle fixed-point wave settled"
            );
            if aligned.outcome != ShuffleAlignmentOutcome::Aligned {
                if Self::shuffle_flush_attempt_advanced(wave, local_activity, aligned) {
                    return Err(DbError::Checkpoint(format!(
                        "shuffle flush observed {:?} after distributed replay had already advanced; coordinated recovery is required",
                        aligned.outcome
                    )));
                }
                return Ok(aligned.outcome);
            }
            if wave != 0 && !local_activity && !aligned.peer_activity {
                if aligned.graph_state_staged || !self.graph.handoff_is_quiescent() {
                    return Err(DbError::Checkpoint(format!(
                        "shuffle flush wave {wave} received an all-idle activity vector but staged new replay; coordinated recovery is required"
                    )));
                }
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    waves = wave + 1,
                    saw_local_activity,
                    saw_peer_activity,
                    saw_graph_staging,
                    elapsed = ?flush_started_at.elapsed(),
                    "checkpoint distributed shuffle fixed-point flush completed"
                );
                return Ok(ShuffleAlignmentOutcome::Aligned);
            }
            wave = wave.checked_add(1).ok_or_else(|| {
                DbError::Checkpoint("shuffle flush wave counter overflowed u64".into())
            })?;
            if wave > laminar_core::checkpoint::barrier::MAX_SHUFFLE_FLUSH_WAVE {
                return Err(DbError::Checkpoint(
                    "shuffle flush exhausted its reserved barrier wave field".into(),
                ));
            }
        }
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
    async fn cancel_pending_follower_control(
        &mut self,
        attempt: CheckpointAttempt,
    ) -> crate::pipeline::CheckpointControlOutcome {
        let reason = "shuffle scope was cancelled before follower checkpoint staging";
        match <Self as crate::pipeline::PipelineCallback>::cancel_source_barrier_attempt(
            self, attempt, reason,
        )
        .await
        {
            Ok(()) => crate::pipeline::CheckpointControlOutcome::Cancelled { attempt },
            Err(error) => {
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                crate::pipeline::CheckpointControlOutcome::Failed { attempt, error }
            }
        }
    }

    #[cfg(feature = "cluster")]
    fn abort_pending_follower_control(
        &mut self,
        attempt: CheckpointAttempt,
    ) -> crate::pipeline::CheckpointControlOutcome {
        let reason = "checkpoint was aborted by the cluster leader during shuffle alignment";
        match <Self as crate::pipeline::PipelineCallback>::resolve_authoritative_follower_abort(
            self, attempt,
        ) {
            Ok(()) => crate::pipeline::CheckpointControlOutcome::Aborted { attempt },
            Err(cleanup) => crate::pipeline::CheckpointControlOutcome::Failed {
                attempt,
                error: format!("{reason}; follower cleanup failed: {cleanup}"),
            },
        }
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

        if self
            .require_process_authority("follower checkpoint admission")
            .is_err()
        {
            return CheckpointControlOutcome::Idle;
        }

        let ann = match self.admit_follower_prepare(&controller).await {
            FollowerPrepareAdmission::Idle => return CheckpointControlOutcome::Idle,
            FollowerPrepareAdmission::Started { attempt, flags } => {
                if let Err(error) =
                    self.require_process_authority("follower source-barrier admission")
                {
                    return self
                        .fail_pending_follower_control(attempt, error.to_string())
                        .await;
                }
                return CheckpointControlOutcome::Started {
                    attempt,
                    captured: false,
                    flags,
                };
            }
            FollowerPrepareAdmission::Failed { attempt, error } => {
                return CheckpointControlOutcome::Failed { attempt, error };
            }
            FollowerPrepareAdmission::CaptureNow(announcement) => announcement,
        };
        let attempt = CheckpointAttempt::new(ann.epoch, ann.checkpoint_id);
        if self.delivery_guarantee != laminar_connectors::connector::DeliveryGuarantee::BestEffort {
            if let Err(error) = validate_durable_source_checkpoint_roster(
                &self.checkpoint_source_names,
                &source_offsets,
            ) {
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return self.fail_pending_follower_control(attempt, error).await;
            }
        }
        if let Err(error) = self.require_process_authority("follower checkpoint capture") {
            return self
                .fail_pending_follower_control(attempt, error.to_string())
                .await;
        }
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
        let mut barrier_timing =
            crate::checkpoint_timing::CheckpointBarrierTimingGuard::start_with_context(
                || {
                    Self::checkpoint_barrier_timing_context(
                        &controller,
                        attempt,
                        crate::checkpoint_timing::CheckpointBarrierRole::Follower,
                        ann.assignment_fence.as_ref(),
                    )
                },
                self.prom.as_ref(),
                &self.checkpoint_barrier_timings,
                attempt_deadline,
            );
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
            self.record_pipeline_halt(&error);
            return self
                .fail_pending_follower_control(attempt, error.to_string())
                .await;
        }
        if self.initial_checkpoint_sink_fence_required() {
            if let Err(error) = self.fence_follower_sinks_until(attempt_deadline).await {
                return self.fail_pending_follower_control(attempt, error).await;
            }
        }
        let checkpoint_rotation_guard = match self
            .checkpoint_capture_rotation_guard_until(Some(assignment_fence), attempt_deadline)
            .await
        {
            Ok(guard) => guard,
            Err(error) => {
                tracing::info!(%error, "follower checkpoint capture was superseded before shuffle staging");
                return self.cancel_pending_follower_control(attempt).await;
            }
        };
        // Fixed-point graph drains acquire this fair rotation fence themselves. Avoid a nested
        // read acquisition that can deadlock behind a queued assignment writer.
        drop(checkpoint_rotation_guard);
        if let Err(error) = self.require_process_authority("follower shuffle alignment") {
            return self
                .fail_pending_follower_control(attempt, error.to_string())
                .await;
        }

        match self
            .align_follower_shuffle_until(&controller, attempt, assignment_fence, attempt_deadline)
            .await
        {
            Ok(ShuffleAlignmentOutcome::Aligned) => {}
            Ok(ShuffleAlignmentOutcome::Aborted) => {
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "follower shuffle alignment observed the leader's checkpoint Abort"
                );
                return self.abort_pending_follower_control(attempt);
            }
            Ok(ShuffleAlignmentOutcome::ScopeCancelledBeforeStaging) => {
                if let Err(error) = self.require_process_authority("follower shuffle cancellation")
                {
                    return self
                        .fail_pending_follower_control(attempt, error.to_string())
                        .await;
                }
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "follower shuffle scope closed before checkpoint staging"
                );
                return self.cancel_pending_follower_control(attempt).await;
            }
            Err(error) => {
                tracing::warn!(%error, "follower shuffle alignment failed — skipping");
                self.record_checkpoint_alignment_error(&error);
                let error = error.to_string();
                return self.fail_pending_follower_control(attempt, error).await;
            }
        }
        if let Err(error) = self.validate_checkpoint_assignment(Some(assignment_fence)) {
            let error = format!(
                "follower assignment changed after shuffle staging and before state capture: \
                 {error}"
            );
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            return self.fail_pending_follower_control(attempt, error).await;
        }
        if let Err(error) = self.require_process_authority("follower state capture") {
            return self
                .fail_pending_follower_control(attempt, error.to_string())
                .await;
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

        // The fixed-point drains can enqueue sink output. Fence those writes after the final
        // idle wave, then reacquire the assignment read fence and prove the portable cut still
        // holds before mutable state capture.
        if let Err(error) = self.fence_follower_sinks_until(attempt_deadline).await {
            return self.fail_pending_follower_control(attempt, error).await;
        }
        let checkpoint_rotation_guard = match self
            .handoff_capture_rotation_guard_until(Some(assignment_fence), attempt_deadline)
            .await
        {
            Ok(guard) => guard,
            Err(error) => {
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return self.fail_pending_follower_control(attempt, error).await;
            }
        };
        let reassignment_portable = checkpoint_rotation_guard.is_some();
        if let Err(error) = self.require_process_authority("follower post-fence state capture") {
            drop(checkpoint_rotation_guard);
            return self
                .fail_pending_follower_control(attempt, error.to_string())
                .await;
        }

        // Capture the complete local node image after shuffle alignment so follower entry paths
        // cannot omit channel replay or non-keyed operator state.
        let (mut request, operator_state) = match self.build_follower_checkpoint_request_until(
            assignment_fence,
            ann.flags,
            attempt_deadline,
        ) {
            Ok(request) => request,
            Err(error) => {
                drop(checkpoint_rotation_guard);
                return self.fail_pending_follower_control(attempt, error).await;
            }
        };
        request.reassignment_portable = reassignment_portable;
        if let Err(error) = self.validate_checkpoint_assignment(Some(assignment_fence)) {
            let error =
                format!("follower assignment changed during mutable state capture: {error}");
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            drop(checkpoint_rotation_guard);
            return self.fail_pending_follower_control(attempt, error).await;
        }
        if let Err(error) = self.require_process_authority("follower durable-tail handoff") {
            drop(checkpoint_rotation_guard);
            return self
                .fail_pending_follower_control(attempt, error.to_string())
                .await;
        }

        let has_shuffle = self.graph.cluster_shuffle_config().is_some();
        let mut tail = match self.follower_tail_future(
            request,
            operator_state,
            identity.clone(),
            source_offsets,
            attempt_started,
            attempt_deadline,
        ) {
            Ok(tail) => tail,
            Err(error) => {
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                drop(checkpoint_rotation_guard);
                return self.fail_pending_follower_control(attempt, error).await;
            }
        };
        if let Err(error) = tail.in_flight.seal_sink_epoch_until(attempt_deadline).await {
            drop(checkpoint_rotation_guard);
            Self::fail_follower_tail_before_prepare(
                &mut tail,
                format!("follower sink epoch seal failed: {error}"),
                attempt_deadline,
            )
            .await;
            return CheckpointControlOutcome::Failed { attempt, error };
        }
        drop(checkpoint_rotation_guard);
        if let Err(error) = self.clear_pending_follower_checkpoint(attempt) {
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            return CheckpointControlOutcome::Failed { attempt, error };
        }
        barrier_timing.finish_local_barrier_with_handoff();
        self.spawn_checkpoint_tail(Self::run_follower_tail(tail));
        if has_shuffle {
            barrier_timing.begin_aligned_resume();
        }
        let aligned = Self::wait_for_aligned_resume_until(
            has_shuffle,
            &controller,
            identity,
            assignment_fence,
            attempt_deadline,
        )
        .await;
        if has_shuffle {
            barrier_timing.finish_aligned_resume();
        }
        if let Err(error) = aligned {
            set_checkpoint_fault(&self.checkpoint_fault, error);
        }
        CheckpointControlOutcome::Started {
            attempt,
            captured: true,
            flags: ann.flags,
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
        if self
            .require_process_authority("deferred follower checkpoint")
            .is_err()
        {
            return BarrierOutcome::Failed;
        }
        let attempt_started = controller
            .checkpoint_prepare_received_at(&ann)
            .map_or(attempt_started, |received_at| {
                received_at.min(attempt_started)
            });

        // Record the complete pipeline pause. The timer observes on drop so alignment or
        // serialization failures are visible instead of disappearing from latency telemetry.
        let attempt_deadline =
            tokio::time::Instant::from_std(attempt_started) + self.checkpoint_timeout;
        let mut barrier_timing =
            crate::checkpoint_timing::CheckpointBarrierTimingGuard::start_with_context(
                || {
                    Self::checkpoint_barrier_timing_context(
                        &controller,
                        attempt,
                        crate::checkpoint_timing::CheckpointBarrierRole::Follower,
                        ann.assignment_fence.as_ref(),
                    )
                },
                self.prom.as_ref(),
                &self.checkpoint_barrier_timings,
                attempt_deadline,
            );
        let Some(assignment_fence) = ann.assignment_fence.as_ref() else {
            tracing::warn!("follower deferred checkpoint lost its assignment certificate");
            return BarrierOutcome::Failed;
        };
        if self.initial_checkpoint_sink_fence_required() {
            if let Err(error) = self.fence_follower_sinks_until(attempt_deadline).await {
                tracing::warn!(%error, "follower deferred checkpoint sink fence failed");
                return BarrierOutcome::Failed;
            }
            Self::log_checkpoint_barrier_phase_completed(
                attempt,
                "follower",
                "initial_sink_fence",
                attempt_started,
            );
        }
        let checkpoint_rotation_guard = match self
            .checkpoint_capture_rotation_guard_until(Some(assignment_fence), attempt_deadline)
            .await
        {
            Ok(guard) => guard,
            Err(error) => {
                tracing::info!(%error, "deferred follower capture was superseded before shuffle staging");
                return BarrierOutcome::CancelledBeforeCapture;
            }
        };
        // Fixed-point graph drains acquire this fair fence themselves.
        drop(checkpoint_rotation_guard);
        if self
            .require_process_authority("deferred follower shuffle alignment")
            .is_err()
        {
            return BarrierOutcome::Failed;
        }
        match self
            .align_deferred_follower_capture(
                &controller,
                attempt,
                assignment_fence,
                attempt_deadline,
            )
            .await
        {
            Ok(ShuffleAlignmentOutcome::Aligned) => {}
            Ok(ShuffleAlignmentOutcome::Aborted) => return BarrierOutcome::Aborted,
            Ok(ShuffleAlignmentOutcome::ScopeCancelledBeforeStaging) => {
                if self
                    .require_process_authority("deferred follower shuffle cancellation")
                    .is_err()
                {
                    return BarrierOutcome::Failed;
                }
                return BarrierOutcome::CancelledBeforeCapture;
            }
            Err(error) => {
                tracing::warn!(%error, "follower deferred shuffle alignment failed");
                self.record_checkpoint_alignment_error(&error);
                return BarrierOutcome::Failed;
            }
        }
        if let Err(error) = self.validate_checkpoint_assignment(Some(assignment_fence)) {
            let error = format!(
                "deferred follower assignment changed after shuffle staging and before state \
                 capture: {error}"
            );
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            tracing::warn!(%error);
            return BarrierOutcome::Failed;
        }
        if self
            .require_process_authority("deferred follower state capture")
            .is_err()
        {
            return BarrierOutcome::Failed;
        }

        if let Err(error) = self.fence_follower_sinks_until(attempt_deadline).await {
            tracing::warn!(%error, "deferred follower post-flush sink fence failed");
            return BarrierOutcome::Failed;
        }
        Self::log_checkpoint_barrier_phase_completed(
            attempt,
            "follower",
            "final_sink_fence",
            attempt_started,
        );
        let checkpoint_rotation_guard = match self
            .handoff_capture_rotation_guard_until(Some(assignment_fence), attempt_deadline)
            .await
        {
            Ok(guard) => guard,
            Err(error) => {
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                tracing::warn!(%error, "deferred follower lost its portable cut after sink fencing");
                return BarrierOutcome::Failed;
            }
        };
        let reassignment_portable = checkpoint_rotation_guard.is_some();

        let (mut request, operator_state) = match self.build_follower_checkpoint_request_until(
            assignment_fence,
            ann.flags,
            attempt_deadline,
        ) {
            Ok(request) => request,
            Err(error) => {
                tracing::warn!(%error, "follower deferred checkpoint state capture failed");
                return BarrierOutcome::Failed;
            }
        };
        request.reassignment_portable = reassignment_portable;
        if let Err(error) = self.validate_checkpoint_assignment(Some(assignment_fence)) {
            let error = format!(
                "deferred follower assignment changed during mutable state capture: {error}"
            );
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            tracing::warn!(%error);
            return BarrierOutcome::Failed;
        }
        if self
            .require_process_authority("deferred follower durable-tail handoff")
            .is_err()
        {
            return BarrierOutcome::Failed;
        }

        let has_shuffle = self.graph.cluster_shuffle_config().is_some();
        let mut tail = match self.follower_tail_future(
            request,
            operator_state,
            identity.clone(),
            source_checkpoints,
            attempt_started,
            attempt_deadline,
        ) {
            Ok(tail) => tail,
            Err(error) => {
                tracing::warn!(%error, "follower deferred checkpoint tail construction failed");
                set_checkpoint_fault(&self.checkpoint_fault, error);
                return BarrierOutcome::Failed;
            }
        };
        if let Err(error) = tail.in_flight.seal_sink_epoch_until(attempt_deadline).await {
            drop(checkpoint_rotation_guard);
            Self::fail_follower_tail_before_prepare(
                &mut tail,
                format!("deferred follower sink epoch seal failed: {error}"),
                attempt_deadline,
            )
            .await;
            return BarrierOutcome::Failed;
        }
        drop(checkpoint_rotation_guard);
        barrier_timing.finish_local_barrier_with_handoff();
        self.spawn_checkpoint_tail(Self::run_follower_tail(tail));
        if has_shuffle {
            barrier_timing.begin_aligned_resume();
        }
        let aligned = Self::wait_for_aligned_resume_until(
            has_shuffle,
            &controller,
            identity,
            assignment_fence,
            attempt_deadline,
        )
        .await;
        if has_shuffle {
            barrier_timing.finish_aligned_resume();
        }
        if let Err(error) = aligned {
            set_checkpoint_fault(&self.checkpoint_fault, error);
        }
        BarrierOutcome::Async
    }

    #[cfg(feature = "cluster")]
    async fn align_deferred_follower_capture(
        &mut self,
        controller: &laminar_core::cluster::control::ClusterController,
        attempt: CheckpointAttempt,
        assignment_fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
        deadline: tokio::time::Instant,
    ) -> Result<ShuffleAlignmentOutcome, DbError> {
        // Capture only after peers' pre-checkpoint rows have crossed the shuffle barrier.
        let outcome = self
            .align_follower_shuffle_until(controller, attempt, assignment_fence, deadline)
            .await?;
        match outcome {
            ShuffleAlignmentOutcome::Aligned => {}
            ShuffleAlignmentOutcome::Aborted => {
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "follower shuffle alignment observed the leader's checkpoint Abort"
                );
                return Ok(outcome);
            }
            ShuffleAlignmentOutcome::ScopeCancelledBeforeStaging => return Ok(outcome),
        }

        // A trailing shuffle gap discovered during alignment must not be sealed cluster-wide.
        self.check_shuffle_delivery_loss();
        if let Some(error) = self.sink_fault.clone() {
            return Err(DbError::Checkpoint(error));
        }
        Ok(ShuffleAlignmentOutcome::Aligned)
    }

    #[cfg(feature = "cluster")]
    fn build_follower_checkpoint_request_until(
        &mut self,
        assignment_fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
        flags: u64,
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
        let handoff_replay_pending = flags & laminar_core::checkpoint::flags::HANDOFF != 0
            && !self.graph.handoff_is_quiescent();
        if fence_intake_after_terminal_handoff_capture(
            &self.intake_gate,
            flags,
            handoff_replay_pending,
        ) {
            tracing::info!(
                "terminal HANDOFF capture fenced graph intake until assignment transition"
            );
        }
        let operator_state = self.capture_operator_state_until(deadline)?;
        let mut request = self.build_checkpoint_request()?;
        (request.flags, request.handoff_replay_pending) = (flags, handoff_replay_pending);
        request.assignment_fence = Some(assignment_fence.clone());
        Ok((request, operator_state))
    }

    /// Reserve one durable attempt before any source or shuffle barrier is admitted.
    async fn reserve_attempt(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let allocator = self.epoch_allocator.clone().ok_or_else(|| {
            DbError::Checkpoint("checkpoint attempt allocator is not initialized".into())
        })?;
        #[cfg(feature = "cluster")]
        if let Some(cc) = self.cluster_controller.clone() {
            if !cc.is_leader() {
                return Err(DbError::Checkpoint(
                    "only the cluster leader may reserve checkpoint attempts".into(),
                ));
            }
        }

        if self.checkpoint_committable_sinks {
            allocator.consume_sink_epoch_until(deadline).await
        } else {
            allocator.allocate_until(deadline).await
        }
    }

    async fn abandon_reserved_attempt(
        &mut self,
        attempt: CheckpointAttempt,
        reason: String,
        flags: u64,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) -> Result<(), String> {
        #[cfg(feature = "cluster")]
        let leader_proof = self.checkpoint_leader_proofs.remove(&attempt);
        #[cfg(not(feature = "cluster"))]
        let leader_proof = None;
        #[cfg(feature = "cluster")]
        let retirement_digest = assignment_fence
            .as_ref()
            .map(laminar_core::checkpoint::CheckpointAssignmentFence::digest);
        let deadline = tokio::time::Instant::now() + self.checkpoint_cleanup_timeout;
        let mut cleanup_errors = Vec::new();
        let cleanup_result = cleanup_reserved_attempt_until(
            self.coordinator.as_ref(),
            attempt,
            reason,
            flags,
            assignment_fence,
            leader_proof,
            deadline,
            crate::checkpoint_coordinator::SinkEpochPublication::Immediate,
        );
        let cleanup_result = cleanup_result.await;
        if let Err(error) = cleanup_result {
            cleanup_errors.push(error);
        }
        #[cfg(feature = "cluster")]
        if cleanup_errors.is_empty() {
            if let Err(error) = self.retire_shuffle_checkpoint_barriers(attempt, retirement_digest)
            {
                cleanup_errors.push(error);
            }
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

    #[cfg(feature = "cluster")]
    fn retire_shuffle_checkpoint_barriers(
        &self,
        attempt: CheckpointAttempt,
        assignment_digest: Option<[u8; 32]>,
    ) -> Result<(), String> {
        let Some(shuffle) = self.graph.cluster_shuffle_config() else {
            return Ok(());
        };
        let assignment_digest = assignment_digest.ok_or_else(|| {
            let error = format!(
                "checkpoint {} epoch {} barrier retirement has no durable assignment digest",
                attempt.checkpoint_id, attempt.epoch
            );
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            error
        })?;
        shuffle
            .receiver
            .retire_checkpoint_barriers(attempt, assignment_digest)
            .map_err(|error| {
                let error = format!(
                    "checkpoint {} epoch {} barrier retirement failed: {error}",
                    attempt.checkpoint_id, attempt.epoch
                );
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                error
            })
    }

    #[cfg(feature = "cluster")]
    async fn reconcile_terminal_shuffle_barriers(
        &mut self,
        controller: &laminar_core::cluster::control::ClusterController,
    ) -> Result<(), String> {
        let has_staged_barrier = self
            .graph
            .cluster_shuffle_config()
            .is_some_and(|shuffle| shuffle.receiver.stage_checkpointed_inbound());
        if !has_staged_barrier {
            return Ok(());
        }
        let authority = controller.checkpoint_authority().map_err(|error| {
            format!("shuffle terminal reconciliation has no authority: {error}")
        })?;
        let terminal = tokio::time::timeout(
            self.checkpoint_cleanup_timeout,
            authority.highest_cluster_terminal_outcome(),
        )
        .await
        .map_err(|_| "shuffle terminal authority audit timed out".to_string())?
        .map_err(|error| format!("shuffle terminal authority audit failed: {error}"))?;
        if let Some(terminal) = terminal {
            let attempt = CheckpointAttempt::new(terminal.epoch, terminal.checkpoint_id);
            let assignment_digest = terminal
                .assignment_fence
                .as_ref()
                .map(laminar_core::checkpoint::CheckpointAssignmentFence::digest);
            self.retire_shuffle_checkpoint_barriers(attempt, assignment_digest)?;
        }
        Ok(())
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
    async fn checkpoint_flags_for_assignment(
        controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
        deadline: tokio::time::Instant,
    ) -> Result<u64, String> {
        let Some(controller) = controller else {
            return if assignment_fence.is_none() {
                Ok(laminar_core::checkpoint::flags::NONE)
            } else {
                Err("local checkpoint received a cluster assignment fence".into())
            };
        };
        let Some(transition) = controller.checkpoint_drain_transition() else {
            return Ok(laminar_core::checkpoint::flags::NONE);
        };
        let fence = assignment_fence.as_ref().ok_or_else(|| {
            "active assignment drain has no admitted predecessor fence".to_string()
        })?;
        let leader = controller
            .capture_leader_proof()
            .ok_or_else(|| "active assignment drain has no live leader proof".to_string())?;
        if transition.predecessor != *fence || transition.leader != leader {
            return Err("checkpoint admission does not match the active assignment drain".into());
        }
        let quorum_ready =
            tokio::time::timeout_at(deadline, controller.drain_ack_quorum_reached(&transition))
                .await
                .map_err(|_| "HANDOFF readiness audit timed out".to_string())?
                .map_err(|error| format!("HANDOFF readiness audit failed: {error}"))?;
        if !quorum_ready {
            return Err("active assignment drain is not HANDOFF-ready".into());
        }
        // The readiness audit performs durable I/O. Re-read the process-local transition and
        // lease afterward so a concurrent watcher clear or leadership change cannot authorize a
        // checkpoint from the stale observation.
        if controller.checkpoint_drain_transition().as_ref() != Some(&transition)
            || controller.capture_leader_proof().as_ref() != Some(&transition.leader)
            || !controller.proof_is_live(&transition.leader)
        {
            return Err("assignment drain authority changed during HANDOFF readiness audit".into());
        }
        Ok(laminar_core::checkpoint::flags::HANDOFF)
    }

    /// Acquire the existing graph/assignment read fence only for shuffle alignment and mutable
    /// state capture. Sink fencing runs before this token and encoding/durable checkpoint-tail I/O
    /// runs after it. Alignment remains inside the token and may perform bounded transport and
    /// authority-settlement reads because its staged channel state belongs to the same cut.
    #[cfg(feature = "cluster")]
    async fn checkpoint_capture_rotation_guard_until(
        &mut self,
        assignment_fence: Option<&laminar_core::cluster::control::CheckpointAssignmentFence>,
        deadline: tokio::time::Instant,
    ) -> Result<Option<tokio::sync::OwnedRwLockReadGuard<()>>, String> {
        let guard = self
            .graph
            .checkpoint_rotation_guard_until(deadline)
            .await
            .map_err(|error| error.to_string())?;
        if assignment_fence.is_some() && guard.is_none() {
            return Err(
                "[LDB-6051] clustered checkpoint capture has no graph assignment-rotation fence"
                    .into(),
            );
        }
        self.validate_checkpoint_assignment(assignment_fence)?;
        if !self.graph.checkpoint_is_quiescent() {
            return Err(
                "[LDB-6051] checkpoint capture found graph input or a vnode assignment \
                 transition that was not drained"
                    .into(),
            );
        }
        Ok(guard)
    }

    /// Reacquire the assignment fence after the final sink sync and prove that no portable
    /// channel replay appeared while the rotation lock was released.
    #[cfg(feature = "cluster")]
    async fn handoff_capture_rotation_guard_until(
        &mut self,
        assignment_fence: Option<&laminar_core::cluster::control::CheckpointAssignmentFence>,
        deadline: tokio::time::Instant,
    ) -> Result<Option<tokio::sync::OwnedRwLockReadGuard<()>>, String> {
        let guard = self
            .checkpoint_capture_rotation_guard_until(assignment_fence, deadline)
            .await?;
        if !self.graph.handoff_is_quiescent() {
            return Err(
                "[LDB-6051] checkpoint capture found retained shuffle replay after its final sink fence"
                    .into(),
            );
        }
        Ok(guard)
    }

    #[cfg(feature = "cluster")]
    async fn align_shuffle_for_leader(
        &mut self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&laminar_core::cluster::control::CheckpointAssignmentFence>,
        deadline: tokio::time::Instant,
    ) -> Result<ShuffleAlignmentOutcome, DbError> {
        // A binary compiled with cluster support may still run embedded or single-node without a
        // controller. Those runtimes have no cross-node shuffle to align.
        let Some(cc) = self.cluster_controller.clone() else {
            return Ok(ShuffleAlignmentOutcome::Aligned);
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
        self.flush_cluster_shuffle_until(&cc, attempt, assignment_fence, deadline)
            .await
            .map_err(|error| {
                if error.requires_pipeline_halt() {
                    error
                } else {
                    DbError::Checkpoint(format!(
                        "shuffle alignment failed for checkpoint {} epoch {}: {error}",
                        attempt.checkpoint_id, attempt.epoch
                    ))
                }
            })
    }

    /// Build the callback-owned portion of a `CheckpointRequest`.
    ///
    /// Source offset overrides remain empty here. The immutable source snapshot is already owned
    /// by the durable tail, which materializes it on a blocking worker after the pipeline resumes.
    fn build_checkpoint_request(
        &self,
    ) -> Result<crate::checkpoint_coordinator::CheckpointRequest, String> {
        let mut channel_progress = Vec::new();
        if let Some(tracker) = self.tracker.as_ref() {
            channel_progress.reserve(tracker.num_sources());
            for source_id in 0..tracker.num_sources() {
                let source_name = self.source_name_arcs.get(&source_id).ok_or_else(|| {
                    format!(
                        "watermark tracker source {source_id} has no stable checkpoint channel identity"
                    )
                })?;
                let state = self
                    .watermark_states
                    .get(source_name.as_ref())
                    .ok_or_else(|| {
                        format!("watermark source '{source_name}' has no runtime state")
                    })?;
                match state.input_channel_progress()? {
                    Some(input_channels) if !input_channels.is_empty() => {
                        channel_progress.extend(input_channels.into_iter().map(|channel| {
                            laminar_core::checkpoint::ChannelProgress {
                                participant_id: 0,
                                source_name: source_name.to_string(),
                                input_channel: channel.input_channel,
                                watermark: channel.watermark,
                                idle: channel.idle,
                            }
                        }));
                    }
                    // An installed empty inventory is a real partitioned-source decision, not
                    // an absence of watermark evidence. Preserve that decision with the same
                    // participant-local logical marker used by non-partitioned sources.
                    Some(_) | None => {
                        channel_progress.push(laminar_core::checkpoint::ChannelProgress {
                            participant_id: 0,
                            source_name: source_name.to_string(),
                            input_channel: SINGLETON_WATERMARK_CHANNEL.to_vec(),
                            watermark: tracker
                                .source_watermark(source_id)
                                .filter(|watermark| *watermark != i64::MIN),
                            idle: tracker.is_idle(source_id),
                        });
                    }
                }
            }
            channel_progress.sort_unstable_by(|left, right| {
                (&left.source_name, &left.input_channel)
                    .cmp(&(&right.source_name, &right.input_channel))
            });
        }
        Ok(crate::checkpoint_coordinator::CheckpointRequest {
            flags: laminar_core::checkpoint::flags::NONE,
            handoff_replay_pending: false,
            reassignment_portable: false,
            assignment_fence: None,
            state_frames: Vec::new(),
            managed_vnode_operators: Vec::new(),
            source_names: self.checkpoint_source_names.clone(),
            channel_progress,
            source_offset_overrides: HashMap::new(),
        })
    }

    fn fault_mutable_checkpoint_capture(&self, component: &str, error: &str) -> String {
        let reason = mutable_checkpoint_capture_failure(component, error);
        set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
        reason
    }

    fn capture_operator_state_until(
        &mut self,
        attempt_deadline: tokio::time::Instant,
    ) -> Result<CapturedOperatorState, String> {
        if tokio::time::Instant::now() >= attempt_deadline {
            return Err(
                "[LDB-6017] checkpoint deadline expired before operator-state capture".into(),
            );
        }
        // Healthy admission permits only one checkpoint tail. Contention therefore means a
        // non-abortable encoder from a failed attempt still owns the image, or that the admission
        // invariant was breached. Waiting while the assignment read fence is held can exhaust a
        // rebalance writer's shorter deadline, so reject before touching mutable operator state.
        let serialization_permit = Arc::clone(&self.checkpoint_serialization_gate)
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => {
                    "[LDB-6017] prior checkpoint serialization is still active; refusing an \
                     overlapping mutable capture"
                        .to_string()
                }
                tokio::sync::TryAcquireError::Closed => {
                    "checkpoint serialization gate was closed".to_string()
                }
            })?;

        let capture_timer = self.prom.checkpoint_state_capture_duration.start_timer();
        if self
            .full_vnode_capture_needed
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.graph.force_full_vnode_capture();
        }
        let graph = match self.graph.capture_state(self.checkpoint_state_cap_bytes) {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                self.full_vnode_capture_needed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                let reason = format!("snapshot failed: {error}");
                if error.requires_pipeline_halt() {
                    if self.pipeline_halt.is_none() {
                        self.pipeline_halt = Some(reason.clone());
                    }
                    self.shutdown_signal.notify_one();
                    return Err(reason);
                }
                return Err(self.fault_mutable_checkpoint_capture("operator state", &reason));
            }
        };
        let mut mutable_capture_guard = graph_capture_needs_mutable_guard(&graph)
            .then(|| MutableCheckpointCaptureGuard::new(Arc::clone(&self.checkpoint_fault)));

        let capture = (|| -> Result<(OperatorStateCapture, u64), DbError> {
            let graph_estimate = graph.retained_bytes();
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
        capture_timer.observe_duration();
        let (image, estimated_bytes) = match capture {
            Ok(capture) => capture,
            Err(error) => {
                self.full_vnode_capture_needed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
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
    ) -> Result<Vec<crate::checkpoint_coordinator::CapturedStateFrame>, String> {
        let deadline = tokio::time::Instant::now() + self.serialization_timeout;
        let capture = self.capture_operator_state_until(deadline)?;
        capture
            .serialize_until(
                self.checkpoint_state_cap_bytes,
                self.serialization_timeout,
                deadline,
            )
            .await
            .map(SerializedOperatorState::accept_for_test)
    }

    /// Sync all sinks and drain their events; `sink_timed_out` is current after this returns.
    /// A failed fence aborts this checkpoint: sealing offsets while queued writes are unknown
    /// would violate both ALO and EO delivery.
    async fn sync_sinks_and_drain_events(
        &mut self,
        attempt_deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        let sync_futures = self.sinks.iter().map(|(name, handle, _, _, _, _)| {
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

    fn sink_publication_requires_replay(&self) -> bool {
        self.checkpoint_committable_sinks
            || self.delivery_guarantee
                != laminar_connectors::connector::DeliveryGuarantee::BestEffort
            || self.in_cluster()
    }

    /// Cluster capture has a final FIFO sink fence after the distributed fixed-point drain, so
    /// the earlier fence is needed only when a checkpoint-committable sink must seal its current
    /// transaction before shuffle work begins. Non-cluster builds have no second fence and retain
    /// the original behavior unconditionally.
    fn initial_checkpoint_sink_fence_required(&self) -> bool {
        #[cfg(feature = "cluster")]
        {
            self.checkpoint_committable_sinks
        }
        #[cfg(not(feature = "cluster"))]
        {
            true
        }
    }

    fn record_dropped_sink_write(&mut self, reason: String) {
        if self.sink_publication_requires_replay() {
            self.sink_fault.get_or_insert(reason);
        } else {
            self.sink_timed_out = true;
        }
    }

    fn drain_sink_events(&mut self) {
        #[cfg(feature = "cluster")]
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
    ) -> Result<(), String> {
        if self.pending_sink_filter_compiles == 0 {
            return Ok(());
        }

        let requires_replay = self.sink_publication_requires_replay();

        while self.compiled_sink_filters.len() < self.sinks.len() {
            self.compiled_sink_filters.push(SinkFilter::Pending);
        }

        for (i, (sink_name, _, filter_sql, sink_input, _, expects_changelog)) in
            self.sinks.iter().enumerate()
        {
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
            let weight = laminar_core::changelog::WEIGHT_COLUMN;
            let weighted_input = *expects_changelog
                || batches.iter().any(|batch| {
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .any(|field| field.name().eq_ignore_ascii_case(weight))
                });
            let compiled = if weighted_input {
                if crate::sql_analysis::predicate_references_weight(sql) {
                    Err(DbError::Pipeline(format!(
                        "filter '{sql}' must not reference engine-owned changelog column '{weight}'"
                    )))
                } else {
                    crate::filter_compile::compile_replay_immutable(&self.filter_ctx, sql, &schema)
                        .await
                }
            } else {
                crate::filter_compile::compile(&self.filter_ctx, sql, &schema).await
            };
            match compiled {
                Ok(compiled) => {
                    self.compiled_sink_filters[i] = SinkFilter::Compiled(compiled);
                }
                Err(e) => {
                    let reason = format!("sink '{sink_name}' filter compilation failed: {e}");
                    tracing::error!(
                        sink = %sink_name,
                        filter = %sql,
                        error = %e,
                        "[LDB-1100] sink filter did not compile; fail-closed: \
                         ALL rows from this stream will be dropped for this sink. \
                         Track via sink_filter_rejected_rows_total."
                    );
                    self.compiled_sink_filters[i] = SinkFilter::Rejected;
                    self.pending_sink_filter_compiles =
                        self.pending_sink_filter_compiles.saturating_sub(1);
                    if requires_replay || weighted_input {
                        return Err(reason);
                    }
                    continue;
                }
            }
            self.pending_sink_filter_compiles = self.pending_sink_filter_compiles.saturating_sub(1);
        }
        Ok(())
    }

    fn refresh_source_frontiers(&mut self) {
        self.source_frontiers_buf.clear();
        if let Some(ref tracker) = self.tracker {
            for (&sid, name) in &self.source_name_arcs {
                let frontier = InputFrontier {
                    watermark: tracker
                        .source_watermark(sid)
                        .filter(|watermark| *watermark != i64::MIN),
                    idle: tracker.is_idle(sid),
                };
                self.source_frontiers_buf.insert(Arc::clone(name), frontier);
            }
        }

        #[cfg(feature = "cluster")]
        self.graph
            .set_local_source_frontiers(&self.source_frontiers_buf);
        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.as_ref() {
            self.graph.cap_temporal_source_frontiers(|source_name| {
                self.committed_source_watermarks_snapshot
                    .get(source_name)
                    .copied()
            });
            Self::cap_source_frontiers_by_cluster_min(
                &mut self.source_frontiers_buf,
                controller.cluster_min_watermark(),
            );
        }
    }

    async fn drain_checkpoint_edges_until_inner(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), crate::pipeline::CycleError> {
        self.drain_checkpoint_edges_until_mode(deadline, false)
            .await
            .map(|_| ())
    }

    #[cfg(feature = "cluster")]
    async fn drain_handoff_edges_until_inner(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<bool, crate::pipeline::CycleError> {
        self.drain_checkpoint_edges_until_mode(deadline, true).await
    }

    async fn drain_checkpoint_edges_until_mode(
        &mut self,
        deadline: tokio::time::Instant,
        handoff: bool,
    ) -> Result<bool, crate::pipeline::CycleError> {
        #[cfg(feature = "cluster")]
        self.require_process_authority("checkpoint graph drain")?;
        self.refresh_source_frontiers();
        let watermark = self.effective_pipeline_watermark();
        #[cfg(feature = "cluster")]
        let shuffle_work_wake = self
            .graph
            .cluster_shuffle_config()
            .map(|shuffle| shuffle.receiver.work_ready_notify());
        #[cfg(not(feature = "cluster"))]
        let shuffle_work_wake: Option<Arc<tokio::sync::Notify>> = None;
        let mut completed_pass = false;
        let mut activity = false;
        while {
            #[cfg(feature = "cluster")]
            {
                if handoff {
                    !self.graph.handoff_is_quiescent()
                } else {
                    !self.graph.checkpoint_is_quiescent()
                }
            }
            #[cfg(not(feature = "cluster"))]
            {
                let _ = handoff;
                !self.graph.checkpoint_is_quiescent()
            }
        } {
            #[cfg(feature = "cluster")]
            self.require_process_authority("checkpoint graph execution")?;
            if tokio::time::Instant::now() >= deadline {
                let error = format!(
                    "checkpoint graph drain exhausted its end-to-end deadline with {} buffered bytes",
                    self.graph.checkpoint_pending_input_bytes()
                );
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(crate::pipeline::CycleError::Recovery(error));
            }

            #[cfg(feature = "cluster")]
            let shuffle_work_ready = self
                .graph
                .cluster_shuffle_config()
                .is_some_and(|shuffle| shuffle.receiver.queued_work_ready());
            #[cfg(not(feature = "cluster"))]
            let shuffle_work_ready = false;
            if completed_pass && !shuffle_work_ready && !self.graph.has_runnable_deferred_work() {
                tokio::select! {
                    biased;
                    // Do not start another graph pass once the attempt deadline has elapsed.
                    // Continuing re-enters the deadline check above and preserves its diagnostic.
                    () = tokio::time::sleep_until(deadline) => continue,
                    () = async {
                        match shuffle_work_wake.as_ref() {
                            Some(wake) => wake.notified().await,
                            None => std::future::pending().await,
                        }
                    } => {}
                    () = tokio::time::sleep(
                        crate::pipeline::streaming_coordinator::IDLE_TIMEOUT,
                    ) => {}
                }
                // A pending shuffle send deliberately remains non-runnable until its completion
                // is polled by an operator pass. Both the receiver notification and the fallback
                // timer are therefore progress hints, not proof that
                // `has_runnable_deferred_work` has changed. Fall through and poll once.
            }

            let source_frontiers = if self.source_frontiers_buf.is_empty() {
                None
            } else {
                Some(&self.source_frontiers_buf)
            };
            let results = match tokio::time::timeout_at(
                deadline,
                self.graph
                    .execute_checkpoint_drain_cycle(watermark, source_frontiers),
            )
            .await
            {
                Ok(Ok(results)) => results,
                Ok(Err(error)) => {
                    let mapped = Self::map_checkpoint_drain_error(&error, &self.shutdown_signal);
                    self.record_pipeline_halt(&mapped);
                    if let crate::pipeline::CycleError::Recovery(reason) = &mapped {
                        set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
                    }
                    return Err(mapped);
                }
                Err(_) => {
                    let error =
                        "checkpoint graph drain exceeded its absolute attempt deadline".to_string();
                    set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                    return Err(crate::pipeline::CycleError::Recovery(error));
                }
            };
            let (any_failed, _) = self.graph.take_cycle_failures();
            if any_failed {
                let error = "checkpoint graph drain encountered a partial operator-domain failure"
                    .to_string();
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(crate::pipeline::CycleError::Recovery(error));
            }
            // Consume this drain pass's normal-cycle deferral report. Checkpoint quiescence
            // deliberately permits barrier-aligned shuffle replay: it is captured in operator
            // channel state and blocks vnode handoff, not an ordinary checkpoint snapshot.
            let _ = self.graph.take_cycle_deferrals();

            #[cfg(feature = "cluster")]
            self.require_process_authority("checkpoint materialized-view publication")?;
            if let Err(error) =
                <Self as crate::pipeline::PipelineCallback>::update_mv_stores(self, &results)
            {
                let error = format!(
                    "checkpoint graph drain could not publish materialized-view output: {error}"
                );
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(crate::pipeline::CycleError::Recovery(error));
            }
            #[cfg(feature = "cluster")]
            self.require_process_authority("checkpoint stream publication")?;
            if let Err(error) =
                <Self as crate::pipeline::PipelineCallback>::push_to_streams(self, &results)
            {
                let error =
                    format!("checkpoint graph drain could not publish stream output: {error}");
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(crate::pipeline::CycleError::Recovery(error));
            }
            #[cfg(feature = "cluster")]
            self.require_process_authority("checkpoint sink publication")?;
            if let Err(error) = <Self as crate::pipeline::PipelineCallback>::write_to_sinks(
                self,
                &results,
                Some(deadline),
            )
            .await
            {
                if let crate::pipeline::CycleError::Recovery(reason) = &error {
                    set_checkpoint_fault(&self.checkpoint_fault, reason.clone());
                }
                return Err(error);
            }
            #[cfg(feature = "cluster")]
            self.require_process_authority("checkpoint graph drain continuation")?;
            // Checkpoint drains run while the pipeline is paused and belong to whole-attempt
            // checkpoint-duration accounting. Mixing them into the normal processing-cycle
            // histogram makes that hot-path signal report checkpoint latency instead.
            completed_pass = true;
            activity = true;

            if tokio::time::Instant::now() >= deadline {
                let error = format!(
                    "checkpoint graph drain exhausted its end-to-end deadline with {} buffered bytes",
                    self.graph.checkpoint_pending_input_bytes()
                );
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(crate::pipeline::CycleError::Recovery(error));
            }
            tokio::task::yield_now().await;
        }
        if tokio::time::Instant::now() >= deadline {
            let error = format!(
                "checkpoint graph drain exhausted its end-to-end deadline with {} buffered bytes",
                self.graph.checkpoint_pending_input_bytes()
            );
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            return Err(crate::pipeline::CycleError::Recovery(error));
        }
        #[cfg(feature = "cluster")]
        self.require_process_authority("checkpoint graph drain completion")?;
        Ok(activity)
    }
}

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
    let rollback_name = name.clone();
    graph.add_query(
        name,
        sql,
        emit_clause,
        window_config,
        order_config,
        join_config,
        incremental,
    );
    let admitted = graph.take_build_errors();
    if admitted.is_err() {
        graph.remove_query(&rollback_name);
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

impl ConnectorPipelineCallback {
    #[cfg(feature = "cluster")]
    async fn route_follower_checkpoint_barrier(
        &mut self,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
        attempt: CheckpointAttempt,
        attempt_started: std::time::Instant,
        flags: u64,
    ) -> Result<FxHashMap<String, SourceCheckpoint>, crate::pipeline::BarrierOutcome> {
        use crate::pipeline::BarrierOutcome;

        let Some(controller) = self.cluster_controller.clone() else {
            return Ok(source_checkpoints);
        };
        if let Some(announcement) = self.pending_follower_checkpoint.clone() {
            if announcement.checkpoint_id != attempt.checkpoint_id
                || announcement.epoch != attempt.epoch
                || announcement.flags != flags
            {
                let error = format!(
                    "retained follower checkpoint {} epoch {} does not match source barrier checkpoint {} epoch {}",
                    announcement.checkpoint_id,
                    announcement.epoch,
                    attempt.checkpoint_id,
                    attempt.epoch
                );
                tracing::warn!(
                    round_checkpoint_id = attempt.checkpoint_id,
                    round_epoch = attempt.epoch,
                    pending_checkpoint_id = announcement.checkpoint_id,
                    pending_epoch = announcement.epoch,
                    "stale follower barrier round — its epoch was abandoned"
                );
                set_checkpoint_fault(&self.checkpoint_fault, error);
                return Err(BarrierOutcome::Failed);
            }

            let outcome = self
                .run_follower_checkpoint_deferred(announcement, source_checkpoints, attempt_started)
                .await;
            if matches!(outcome, BarrierOutcome::Async) {
                if let Err(error) = self.clear_pending_follower_checkpoint(attempt) {
                    set_checkpoint_fault(&self.checkpoint_fault, error);
                }
            }
            return Err(outcome);
        }

        if !controller.is_leader() {
            tracing::warn!(
                "follower received checkpoint_with_barrier but pending_follower_checkpoint is None"
            );
            return Err(BarrierOutcome::Failed);
        }
        Ok(source_checkpoints)
    }

    async fn fence_checkpoint_sinks(
        &mut self,
        attempt_deadline: tokio::time::Instant,
    ) -> Result<(), crate::pipeline::BarrierOutcome> {
        use crate::pipeline::{BarrierOutcome, SkipReason};

        match tokio::time::timeout_at(
            attempt_deadline,
            self.sync_sinks_and_drain_events(attempt_deadline),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::error!(%error, "checkpoint sink write fence failed");
                return Err(BarrierOutcome::Failed);
            }
            Err(_) => {
                tracing::error!(
                    timeout = ?self.checkpoint_timeout,
                    "checkpoint sink write fence exhausted the end-to-end attempt deadline"
                );
                return Err(BarrierOutcome::Failed);
            }
        }

        #[cfg(feature = "cluster")]
        if self
            .require_process_authority("checkpoint shuffle alignment")
            .is_err()
        {
            return Err(BarrierOutcome::Failed);
        }

        if self.sink_fault.is_some() {
            return Err(BarrierOutcome::Failed);
        }
        if self.sink_timed_out {
            self.sink_timed_out = false;
            return Err(BarrierOutcome::Skipped(
                SkipReason::PreservingReplayWindowAfterSinkTimeout,
            ));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    async fn align_leader_shuffle(
        &mut self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&laminar_core::cluster::control::CheckpointAssignmentFence>,
        attempt_deadline: tokio::time::Instant,
    ) -> Result<(), crate::pipeline::BarrierOutcome> {
        use crate::pipeline::BarrierOutcome;

        match self
            .align_shuffle_for_leader(attempt, assignment_fence, attempt_deadline)
            .await
        {
            Ok(ShuffleAlignmentOutcome::Aligned) => {}
            Ok(ShuffleAlignmentOutcome::Aborted) => {
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "leader shuffle alignment observed its checkpoint Abort"
                );
                return Err(BarrierOutcome::Aborted);
            }
            Ok(ShuffleAlignmentOutcome::ScopeCancelledBeforeStaging) => {
                if self
                    .require_process_authority("checkpoint shuffle cancellation")
                    .is_err()
                {
                    return Err(BarrierOutcome::Failed);
                }
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "leader shuffle scope closed before checkpoint staging"
                );
                return Err(BarrierOutcome::CancelledBeforeCapture);
            }
            Err(error) => {
                tracing::warn!(%error, "shuffle barrier alignment failed");
                self.record_checkpoint_alignment_error(&error);
                return Err(BarrierOutcome::Failed);
            }
        }

        if self
            .require_process_authority("checkpoint state capture")
            .is_err()
        {
            return Err(BarrierOutcome::Failed);
        }
        self.check_shuffle_delivery_loss();
        if self.sink_fault.is_some() {
            return Err(BarrierOutcome::Failed);
        }
        Ok(())
    }

    fn capture_leader_checkpoint_state(
        &mut self,
        _attempt: CheckpointAttempt,
        attempt_deadline: tokio::time::Instant,
    ) -> Result<(CapturedOperatorState, u64), crate::pipeline::BarrierOutcome> {
        use crate::pipeline::BarrierOutcome;

        let operator_state = match self.capture_operator_state_until(attempt_deadline) {
            Ok(capture) => capture,
            Err(error) => {
                tracing::warn!(%error, "Stream executor barrier checkpoint failed");
                return Err(BarrierOutcome::Failed);
            }
        };

        #[cfg(feature = "cluster")]
        if self
            .require_process_authority("checkpoint durable-tail handoff")
            .is_err()
        {
            return Err(BarrierOutcome::Failed);
        }

        Ok((operator_state, self.checkpoint_state_cap_bytes))
    }

    #[cfg(feature = "cluster")]
    fn take_checkpoint_leader_proof(
        &mut self,
        attempt: CheckpointAttempt,
    ) -> Result<Option<laminar_core::cluster::control::LeaderProof>, crate::pipeline::BarrierOutcome>
    {
        use crate::pipeline::BarrierOutcome;

        let Some(controller) = self.cluster_controller.as_ref() else {
            return Ok(None);
        };
        let Some(proof) = self.checkpoint_leader_proofs.get(&attempt) else {
            tracing::error!(
                checkpoint_id = attempt.checkpoint_id,
                epoch = attempt.epoch,
                "cluster checkpoint lost the leader proof captured for Prepare"
            );
            return Err(BarrierOutcome::Failed);
        };
        if !controller.proof_is_live(proof) {
            tracing::warn!(
                checkpoint_id = attempt.checkpoint_id,
                epoch = attempt.epoch,
                "leadership changed before checkpoint durable-tail handoff"
            );
            return Err(BarrierOutcome::Failed);
        }
        Ok(self.checkpoint_leader_proofs.remove(&attempt))
    }
}

impl crate::pipeline::PipelineCallback for ConnectorPipelineCallback {
    fn prepare_source_intake(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn pin_source_frontiers_for_new_cycle(&mut self) -> Result<(), String> {
        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.as_ref() {
            self.committed_source_watermarks_snapshot =
                controller.committed_source_watermarks_snapshot();
        }
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
        self.refresh_source_frontiers();

        let source_frontiers = if self.source_frontiers_buf.is_empty() {
            None
        } else {
            Some(&self.source_frontiers_buf)
        };
        let results = match self
            .graph
            .execute_cycle(source_batches, watermark, source_frontiers)
            .await
        {
            Ok(results) => results,
            Err(error) => {
                let error = Self::map_graph_error(&error, &self.shutdown_signal);
                self.record_pipeline_halt(&error);
                return Err(error);
            }
        };
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

    async fn complete_pending_vnode_transition(
        &mut self,
    ) -> Result<bool, crate::pipeline::CycleError> {
        #[cfg(feature = "cluster")]
        {
            match self.graph.complete_pending_vnode_transition().await {
                Ok(completed) => Ok(completed),
                Err(error) if error.is_shuffle_not_ready() => Ok(false),
                Err(error) => {
                    let error = Self::map_graph_error(&error, &self.shutdown_signal);
                    self.record_pipeline_halt(&error);
                    Err(error)
                }
            }
        }

        #[cfg(not(feature = "cluster"))]
        {
            Ok(false)
        }
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
            let bytes = i64::try_from(store.total_bytes()).unwrap_or(i64::MAX);
            self.prom.mv_bytes_stored.set(bytes);
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

    async fn settle_sink_epoch_for_shutdown(&mut self) -> Result<(), String> {
        let mut coordinator = self.coordinator.lock().await;
        let Some(coordinator) = coordinator.as_mut() else {
            return Ok(());
        };
        coordinator
            .reconcile_sink_open_witness()
            .await
            .map_err(|error| error.to_string())
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
            for (_, handle, _, _, _, _) in &self.sinks {
                if !registered.iter().any(|known| known.same_actor(handle)) {
                    registered.push(handle.clone());
                }
            }
        }
        let close_results =
            futures::future::join_all(self.sinks.iter().map(|(name, handle, _, _, _, _)| {
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

    async fn write_to_sinks(
        &mut self,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<(), crate::pipeline::CycleError> {
        let sink_input_violation = self.sinks.iter().find_map(
            |(
                sink_name,
                _handle,
                _filter_sql,
                sink_input,
                contract,
                expects_changelog,
            )| {
                let batches = results.get(sink_input.as_str())?;
                batches.iter().enumerate().find_map(|(batch_index, batch)| {
                    crate::changelog_filter::validate_sink_input(
                        batch,
                        contract.accepts_full_changelog(),
                        *expects_changelog,
                    )
                    .err()
                    .map(|error| {
                        format!(
                            "sink '{sink_name}' rejected input '{sink_input}' batch {batch_index}: {error}"
                        )
                    })
                })
            },
        );
        if let Some(error) = sink_input_violation {
            // This invariant is independent of delivery mode. Validate the complete publication
            // before an epoch gate opens or any concurrent sink can observe a partial cycle.
            self.record_dropped_sink_write(error.clone());
            return Err(crate::pipeline::CycleError::Recovery(error));
        }
        let weighted_publication = self.sinks.iter().any(|(_, _, _, sink_input, _, _)| {
            results.get(sink_input.as_str()).is_some_and(|batches| {
                batches.iter().any(|batch| {
                    batch.schema().fields().iter().any(|field| {
                        field
                            .name()
                            .eq_ignore_ascii_case(laminar_core::changelog::WEIGHT_COLUMN)
                    })
                })
            })
        });

        #[cfg(feature = "cluster")]
        let controller = self.cluster_controller.clone();

        let compile = self.compile_pending_sink_filters(results);
        let compile_result = await_sink_publication(
            #[cfg(feature = "cluster")]
            controller.as_deref(),
            deadline,
            "sink filter compilation",
            compile,
        )
        .await;
        let compile_error = match compile_result {
            Ok(Ok(())) => None,
            Ok(Err(error)) | Err(error) => Some(error),
        };
        if let Some(error) = compile_error {
            self.record_dropped_sink_write(error.clone());
            return Err(crate::pipeline::CycleError::Recovery(error));
        }

        // Filter every sink-bound batch before opening any epoch gate or enqueueing any write.
        // RecordBatch clones are shallow; owned filtered batches are reused by the write phase.
        let requires_replay = self.sink_publication_requires_replay() || weighted_publication;
        let mut preflighted_inputs: Vec<Option<Arc<[RecordBatch]>>> =
            Vec::with_capacity(self.sinks.len());
        let mut shared_unfiltered: FxHashMap<String, Arc<[RecordBatch]>> = FxHashMap::default();
        for sink_idx in 0..self.sinks.len() {
            let (sink_name, sink_input, filter_state) = {
                let (sink_name, _, _, sink_input, _, _) = &self.sinks[sink_idx];
                let filter_state = match self.compiled_sink_filters.get(sink_idx).cloned() {
                    Some(SinkFilter::Compiled(phys)) => SinkFilterDispatch::Compiled(phys),
                    Some(SinkFilter::Rejected) => SinkFilterDispatch::Rejected,
                    Some(SinkFilter::Pending) | None => SinkFilterDispatch::None,
                };
                (sink_name.clone(), sink_input.clone(), filter_state)
            };
            let Some(batches) = results.get(sink_input.as_str()) else {
                preflighted_inputs.push(None);
                continue;
            };
            if matches!(filter_state, SinkFilterDispatch::None) {
                let shared = shared_unfiltered
                    .entry(sink_input)
                    .or_insert_with(|| Arc::from(batches.as_slice()))
                    .clone();
                preflighted_inputs.push(Some(shared));
                continue;
            }
            let mut ready = Vec::with_capacity(batches.len());
            for batch in batches {
                match &filter_state {
                    SinkFilterDispatch::Compiled(phys) => {
                        match crate::filter_compile::apply(batch, phys.as_ref()) {
                            Ok(Some(filtered)) => ready.push(filtered),
                            Ok(None) => {}
                            Err(error) => {
                                self.prom
                                    .sink_filter_rejected_rows
                                    .with_label_values(&[sink_name.as_str()])
                                    .inc_by(batch.num_rows() as u64);
                                tracing::warn!(
                                    sink = %sink_name,
                                    error = %error,
                                    "Compiled sink filter error"
                                );
                                if requires_replay {
                                    let reason = format!(
                                        "sink '{sink_name}' filter application failed: {error}"
                                    );
                                    self.record_dropped_sink_write(reason.clone());
                                    return Err(crate::pipeline::CycleError::Recovery(reason));
                                }
                            }
                        }
                    }
                    SinkFilterDispatch::Rejected => {
                        self.prom
                            .sink_filter_rejected_rows
                            .with_label_values(&[sink_name.as_str()])
                            .inc_by(batch.num_rows() as u64);
                        if requires_replay {
                            let reason = format!(
                                "sink '{sink_name}' filter is rejected for a recovery-required publication"
                            );
                            self.record_dropped_sink_write(reason.clone());
                            return Err(crate::pipeline::CycleError::Recovery(reason));
                        }
                    }
                    SinkFilterDispatch::None => unreachable!("handled by shared input fast path"),
                }
            }
            preflighted_inputs.push(Some(Arc::from(ready)));
        }

        let has_committable_output =
            self.sinks
                .iter()
                .enumerate()
                .any(|(sink_idx, (_, handle, _, _, _, _))| {
                    handle.checkpoint_committable()
                        && preflighted_inputs
                            .get(sink_idx)
                            .and_then(Option::as_deref)
                            .is_some_and(|batches| !batches.is_empty())
                });
        if has_committable_output {
            let gate_result: Result<(), String> = 'gate: {
                let mut group_admission = None;
                for (sink_name, handle, _, _, _, _) in self
                    .sinks
                    .iter()
                    .filter(|(_, handle, _, _, _, _)| handle.checkpoint_committable())
                {
                    let gate = handle.wait_for_write_gate_until(deadline);
                    let observed = await_sink_publication(
                        #[cfg(feature = "cluster")]
                        controller.as_deref(),
                        deadline,
                        "coordinated sink epoch gate",
                        gate,
                    )
                    .await;
                    let admission = match observed {
                        Ok(Ok(Some(admission))) => admission,
                        Ok(Ok(None)) => {
                            break 'gate Err(format!(
                                "checkpoint-committable sink '{sink_name}' has no epoch gate"
                            ))
                        }
                        Ok(Err(error)) => {
                            break 'gate Err(format!(
                                "sink '{sink_name}' epoch gate failed: {error}"
                            ))
                        }
                        Err(error) => break 'gate Err(error),
                    };
                    if group_admission.is_some_and(|expected| expected != admission) {
                        break 'gate Err(format!(
                            "checkpoint-committable sink '{sink_name}' opened admission {admission:?}, which does not match group admission {group_admission:?}"
                        ));
                    }
                    group_admission = Some(admission);
                }
                Ok(())
            };
            if let Err(error) = gate_result {
                self.record_dropped_sink_write(error.clone());
                return Err(crate::pipeline::CycleError::Recovery(error));
            }
        }
        let sink_futures: Vec<_> = self
            .sinks
            .iter()
            .enumerate()
            .filter_map(|(sink_idx, (sink_name, handle, _, _, _, _))| {
                let batches = preflighted_inputs.get(sink_idx)?.as_ref()?.clone();
                if batches.is_empty() {
                    return None;
                }
                let sink_name = sink_name.clone();
                let handle = handle.clone();
                #[cfg(feature = "cluster")]
                let controller = controller.clone();
                Some(async move {
                    for batch in batches.iter() {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        let boundary = format!("sink '{sink_name}' write enqueue");
                        let batch = batch.clone();
                        let write = async {
                            match deadline {
                                Some(deadline) => handle.write_batch_until(batch, deadline).await,
                                None => handle.write_batch(batch).await,
                            }
                        };
                        let enqueue = await_sink_publication(
                            #[cfg(feature = "cluster")]
                            controller.as_deref(),
                            deadline,
                            &boundary,
                            write,
                        )
                        .await;
                        match enqueue {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => {
                                tracing::warn!(
                                    sink = %sink_name,
                                    %error,
                                    "Sink write could not be enqueued"
                                );
                                return Some(format!(
                                    "sink '{sink_name}' write enqueue failed: {error}"
                                ));
                            }
                            Err(error) => return Some(error),
                        }
                    }
                    None
                })
            })
            .collect();
        let direct_failures = futures::future::join_all(sink_futures)
            .await
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        for reason in &direct_failures {
            // Do not depend on the bounded event channel for correctness. In particular, a full
            // event channel must not turn an enqueue timeout into a checkpointable lost write.
            self.record_dropped_sink_write(reason.clone());
        }

        // Opportunistic; the strict barrier runs in the checkpoint path.
        self.drain_sink_events();
        if requires_replay {
            if let Some(error) = direct_failures.first() {
                return Err(crate::pipeline::CycleError::Recovery(error.clone()));
            }
            if let Some(error) = self.sink_fault.clone() {
                return Err(crate::pipeline::CycleError::Recovery(error));
            }
        }
        Ok(())
    }

    fn extract_watermark(
        &mut self,
        source_name: &str,
        batch: &RecordBatch,
        admission_floor: i64,
    ) -> Result<(), crate::pipeline::CycleError> {
        #[cfg(feature = "cluster")]
        let admission_floor = self
            .cluster_controller
            .as_ref()
            .map_or(admission_floor, |_| {
                self.decision_bound_source_admission_floor(source_name, admission_floor)
            });
        if let Some(wm_state) = self.watermark_states.get_mut(source_name) {
            #[cfg(feature = "cluster")]
            if self.cluster_controller.is_some() && admission_floor > i64::MIN {
                let _ = wm_state.install_committed_watermark_floor(admission_floor);
            }
            if let Some(entry) = self.source_entries_for_wm.get(source_name) {
                let external_wm = entry.source.current_watermark();
                wm_state.advance_external_watermark(external_wm);
            }
            let advanced = wm_state
                .observe_input_channels(batch, admission_floor)
                .map_err(|error| {
                    crate::pipeline::CycleError::Recovery(format!(
                    "source '{source_name}' watermark extraction failed for column '{}': {error}",
                    wm_state.column
                ))
                })?;
            let source_watermark = wm_state.generator.current_watermark();
            if let Some(watermark) = advanced {
                if let Some(entry) = self.source_entries_for_wm.get(source_name) {
                    entry.source.watermark(watermark);
                }
            }
            if source_watermark > i64::MIN {
                self.prom
                    .source_watermark_ms
                    .with_label_values(&[source_name])
                    .set(source_watermark);
            }
            if let Some(ref mut tracker) = self.tracker {
                if let Some(source_id) = self.source_ids.get(source_name) {
                    if let Some(global) = tracker.update_source(*source_id, source_watermark) {
                        self.pipeline_watermark
                            .store(global.timestamp(), std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        }

        let row_count = batch.num_rows() as u64;
        self.prom.events_ingested.inc_by(row_count);
        self.prom.batches.inc();
        Ok(())
    }

    fn reconcile_source_input_channels(
        &mut self,
        source_name: &str,
        input_channels: Option<Arc<[Vec<u8>]>>,
    ) -> Result<(), crate::pipeline::CycleError> {
        let admission_floor = self.effective_pipeline_watermark();
        #[cfg(feature = "cluster")]
        let admission_floor = self
            .cluster_controller
            .as_ref()
            .map_or(admission_floor, |_| {
                self.decision_bound_source_admission_floor(source_name, admission_floor)
            });
        let Some(state) = self.watermark_states.get_mut(source_name) else {
            return Ok(());
        };
        if !state.is_partitioned() {
            return Ok(());
        }
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() && admission_floor > i64::MIN {
            let _ = state.install_committed_watermark_floor(admission_floor);
        }
        let changed = state
            .install_input_channels(input_channels, admission_floor)
            .map_err(|error| {
                crate::pipeline::CycleError::Recovery(format!(
                    "source '{source_name}' input-channel inventory is invalid: {error}"
                ))
            })?;
        if !changed {
            return Ok(());
        }
        if let (Some(tracker), Some(source_id)) =
            (self.tracker.as_mut(), self.source_ids.get(source_name))
        {
            let advanced = tracker.update_source(*source_id, state.generator.current_watermark());
            if state.input_channels_all_idle() == Some(true) {
                if let Some(global) = tracker.mark_idle(*source_id).or(advanced) {
                    self.pipeline_watermark
                        .store(global.timestamp(), std::sync::atomic::Ordering::Relaxed);
                }
            } else if let Some(global) = advanced {
                self.pipeline_watermark
                    .store(global.timestamp(), std::sync::atomic::Ordering::Relaxed);
            }
        }
        Ok(())
    }

    fn filter_late_rows(
        &self,
        source_name: &str,
        batch: &RecordBatch,
    ) -> Result<Option<RecordBatch>, crate::pipeline::CycleError> {
        if let Some(wm_state) = self.watermark_states.get(source_name) {
            // Processing-time watermarks are wall-clock; filtering would drop every real row.
            if wm_state.generator.is_processing_time() {
                return Ok(Some(batch.clone()));
            }
            // The source frontier closes its own windows and join input. The pipeline frontier
            // is also a floor because an idle source may resume after the other inputs advanced.
            let current_wm = wm_state.generator.current_watermark().max(
                self.pipeline_watermark
                    .load(std::sync::atomic::Ordering::Acquire),
            );
            #[cfg(feature = "cluster")]
            let current_wm = self.cluster_controller.as_ref().map_or(current_wm, |_| {
                self.decision_bound_source_admission_floor(source_name, current_wm)
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
                        return Ok(out);
                    }
                    Err(e) => {
                        return Err(crate::pipeline::CycleError::Recovery(format!(
                            "source '{source_name}' late-row preparation failed for column '{}': {e}",
                            wm_state.column
                        )));
                    }
                }
            }
        }
        Ok(Some(batch.clone()))
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

    fn is_recovering(&mut self) -> bool {
        #[cfg(feature = "cluster")]
        let recovering = self.checkpoint_recovery_active();
        #[cfg(not(feature = "cluster"))]
        let recovering = false;
        self.observe_checkpoint_recovery_state(recovering)
    }

    fn fault_on_cycle_error(&self) -> bool {
        use laminar_connectors::connector::DeliveryGuarantee;
        self.delivery_guarantee != DeliveryGuarantee::BestEffort || self.in_cluster()
    }

    fn take_pipeline_halt(&mut self) -> Option<String> {
        self.pipeline_halt.take()
    }

    fn take_pipeline_fault(&mut self) -> Option<String> {
        self.reap_checkpoint_tail_tasks();
        self.sink_fault
            .take()
            .or_else(|| self.checkpoint_fault.lock().take())
            .or_else(|| self.graph.execution_poison_reason().map(str::to_owned))
    }

    async fn settle_checkpoint_tail_tasks(&mut self) -> Result<(), String> {
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
        tracing::warn!(checkpoint_id, reason = %reason, "checkpoint attempt failed");
        if self.delivery_guarantee == laminar_connectors::connector::DeliveryGuarantee::ExactlyOnce
        {
            set_checkpoint_fault(
                &self.checkpoint_fault,
                format!("checkpoint {checkpoint_id}: {reason}"),
            );
        }
    }

    fn record_checkpoint_continuation_fault(&mut self, attempt: CheckpointAttempt, reason: &str) {
        tracing::error!(
            checkpoint_id = attempt.checkpoint_id,
            epoch = attempt.epoch,
            reason = %reason,
            "checkpoint continuation failed"
        );
        set_checkpoint_fault(
            &self.checkpoint_fault,
            format!(
                "checkpoint {} epoch {} committed, but continuation failed: {reason}",
                attempt.checkpoint_id, attempt.epoch
            ),
        );
    }

    fn record_checkpoint_admission_failure(&mut self, reason: &str) {
        #[cfg(feature = "cluster")]
        let recovering = self.checkpoint_recovery_active();
        #[cfg(not(feature = "cluster"))]
        let recovering = false;
        self.observe_checkpoint_recovery_state(recovering);
        if self.mark_checkpoint_admission_failure(reason) {
            tracing::error!(reason = %reason, "checkpoint admission failed");
        }
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
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, String> {
        let attempt = self
            .reserve_attempt(deadline)
            .await
            .map_err(|error| error.to_string())?;
        self.last_checkpoint_admission_failure = None;
        Ok(attempt)
    }

    async fn publish_checkpoint_prepare(
        &mut self,
        attempt: CheckpointAttempt,
        _attempt_started: std::time::Instant,
        deadline: tokio::time::Instant,
        flags: u64,
        admitted_assignment_fence: Option<
            laminar_core::cluster::control::CheckpointAssignmentFence,
        >,
    ) -> Result<(), String> {
        #[cfg(feature = "cluster")]
        {
            use laminar_core::cluster::control::{BarrierAnnouncement, Phase};

            let assignment_fence =
                self.validate_checkpoint_assignment(admitted_assignment_fence.as_ref())?;
            let expected_flags = Self::checkpoint_flags_for_assignment(
                self.cluster_controller.clone(),
                assignment_fence.clone(),
                deadline,
            )
            .await?;
            if flags != expected_flags {
                return Err(format!(
                    "checkpoint flags {flags:#x} no longer match admission {expected_flags:#x}"
                ));
            }
            let Some(controller) = self.cluster_controller.clone() else {
                let mut guard = tokio::time::timeout_at(deadline, self.coordinator.lock())
                    .await
                    .map_err(|_| "local checkpoint artifact admission timed out".to_string())?;
                let coordinator = guard.as_mut().ok_or_else(|| {
                    "local checkpoint artifact admission has no coordinator".to_string()
                })?;
                coordinator
                    .begin_checkpoint_artifacts_until(attempt, None, None, deadline)
                    .await
                    .map_err(|error| error.to_string())?;
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
            let quorum_window = self
                .quorum_timeout
                .min(deadline.saturating_duration_since(tokio::time::Instant::now()));
            if quorum_window.is_zero() {
                return Err("checkpoint Prepare has no remaining quorum window".into());
            }
            // Publication failure is ambiguous: retain the proof before issuing I/O so cleanup
            // can resolve this exact attempt instead of assuming Prepare was absent.
            self.checkpoint_leader_proofs
                .insert(attempt, leader_proof.clone());
            {
                let mut guard = tokio::time::timeout_at(deadline, self.coordinator.lock())
                    .await
                    .map_err(|_| "cluster checkpoint artifact admission timed out".to_string())?;
                let coordinator = guard.as_mut().ok_or_else(|| {
                    "cluster checkpoint artifact admission has no coordinator".to_string()
                })?;
                coordinator
                    .begin_checkpoint_artifacts_until(
                        attempt,
                        Some(assignment_fence.clone()),
                        Some(&leader_proof),
                        deadline,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
            }
            let publish_flags = Self::checkpoint_flags_for_assignment(
                Some(Arc::clone(&controller)),
                Some(assignment_fence.clone()),
                deadline,
            )
            .await?;
            if flags != publish_flags {
                return Err(format!(
                    "checkpoint flags {flags:#x} changed before Prepare publication to {publish_flags:#x}"
                ));
            }
            self.validate_checkpoint_assignment(Some(&assignment_fence))?;
            tokio::time::timeout_at(
                deadline,
                controller.announce_prepare_barrier_until(
                    &BarrierAnnouncement {
                        epoch: attempt.epoch,
                        checkpoint_id: attempt.checkpoint_id,
                        assignment_fence: Some(assignment_fence),
                        leader_proof: Some(leader_proof.clone()),
                        phase: Phase::Prepare,
                        flags,
                    },
                    deadline,
                    quorum_window,
                ),
            )
            .await
            .map_err(|_| {
                "checkpoint Prepare publication exhausted its end-to-end deadline".to_string()
            })?
            .map_err(|error| format!("checkpoint Prepare publication failed: {error}"))?;
            Ok(())
        }

        #[cfg(not(feature = "cluster"))]
        {
            let _ = flags;
            if admitted_assignment_fence.is_some() {
                return Err(
                    "cluster assignment certificate supplied to a local checkpoint runtime".into(),
                );
            }
            let mut guard = tokio::time::timeout_at(deadline, self.coordinator.lock())
                .await
                .map_err(|_| "local checkpoint artifact admission timed out".to_string())?;
            let coordinator = guard.as_mut().ok_or_else(|| {
                "local checkpoint artifact admission has no coordinator".to_string()
            })?;
            coordinator
                .begin_checkpoint_artifacts_until(attempt, None, None, deadline)
                .await
                .map_err(|error| error.to_string())?;
            Ok(())
        }
    }

    async fn abandon_checkpoint_attempt(
        &mut self,
        attempt: CheckpointAttempt,
        reason: &str,
        flags: u64,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) -> Result<(), String> {
        self.abandon_reserved_attempt(attempt, reason.to_owned(), flags, assignment_fence)
            .await
    }

    #[cfg(feature = "cluster")]
    async fn checkpoint_assignment_for_admission(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> crate::pipeline::CheckpointAssignmentAdmission {
        use crate::pipeline::CheckpointAssignmentAdmission;

        let Some(controller) = self.cluster_controller.clone() else {
            return CheckpointAssignmentAdmission::Ready {
                assignment_fence: None,
                flags: laminar_core::checkpoint::flags::NONE,
                assignment_guard: None,
            };
        };
        let Ok(assignment_guard) = tokio::time::timeout_at(
            deadline,
            Arc::clone(&self.assignment_adoption_lock).lock_owned(),
        )
        .await
        else {
            return CheckpointAssignmentAdmission::Deferred(
                "checkpoint admission timed out waiting for assignment serialization".into(),
            );
        };
        let Some(registry) = self.vnode_registry.clone() else {
            tracing::error!(
                "cluster checkpoint admission has no vnode registry; failing assignment fence"
            );
            return CheckpointAssignmentAdmission::Fault(
                "cluster checkpoint admission has no vnode registry".into(),
            );
        };
        let publication = registry.versioned_snapshot();
        // The snapshot watcher performs the gossip scan off the hot path. Retain the exact
        // certificate so later capture/quorum/durable phases cannot silently switch generations.
        let Some(fence) = controller.checkpoint_assignment_fence(publication.version()) else {
            return CheckpointAssignmentAdmission::Deferred(format!(
                "assignment {} is not checkpoint-ready",
                publication.version()
            ));
        };
        let verified = registry.versioned_snapshot();
        if verified.version() != publication.version() {
            return CheckpointAssignmentAdmission::Deferred(
                "assignment changed while checkpoint admission was being certified".into(),
            );
        }
        let drain_was_active = controller.checkpoint_drain_transition().is_some();
        let flags = match Self::checkpoint_flags_for_assignment(
            Some(Arc::clone(&controller)),
            Some(fence.clone()),
            deadline,
        )
        .await
        {
            Ok(flags) => flags,
            Err(error)
                if drain_was_active || controller.checkpoint_drain_transition().is_some() =>
            {
                return CheckpointAssignmentAdmission::Deferred(error);
            }
            Err(error) => return CheckpointAssignmentAdmission::Fault(error),
        };
        if registry.assignment_version() != publication.version()
            || controller
                .checkpoint_assignment_fence(publication.version())
                .as_ref()
                != Some(&fence)
        {
            return CheckpointAssignmentAdmission::Deferred(
                "assignment changed during HANDOFF readiness audit".into(),
            );
        }
        CheckpointAssignmentAdmission::Ready {
            assignment_fence: Some(fence),
            flags,
            assignment_guard: Some(assignment_guard),
        }
    }

    fn checkpoint_control_wake(&self) -> Option<crate::pipeline::callback::CheckpointControlWake> {
        #[cfg(feature = "cluster")]
        {
            self.cluster_controller.as_ref().map(|controller| {
                crate::pipeline::callback::CheckpointControlWake::new(
                    controller.checkpoint_announcement_watch(),
                )
            })
        }
        #[cfg(not(feature = "cluster"))]
        {
            None
        }
    }

    fn shuffle_work_wake(&self) -> Option<Arc<tokio::sync::Notify>> {
        #[cfg(feature = "cluster")]
        {
            self.graph
                .cluster_shuffle_config()
                .map(|shuffle| shuffle.receiver.work_ready_notify())
        }
        #[cfg(not(feature = "cluster"))]
        {
            None
        }
    }

    fn tick_idle_watermark(&mut self) {
        #[cfg(feature = "cluster")]
        let cluster_frontiers = self
            .cluster_controller
            .as_ref()
            .map(|_| Arc::clone(&self.committed_source_watermarks_snapshot));
        let Some(ref mut trk) = self.tracker else {
            return;
        };
        for (name, state) in &mut self.watermark_states {
            let source_id = self.source_ids.get(name).copied();
            #[cfg(feature = "cluster")]
            let was_idle = source_id.is_some_and(|id| trk.is_idle(id));
            // Installing the durable floor is not source activity. Only a real external or
            // periodic advance below may reactivate a non-partitioned source.
            #[cfg(feature = "cluster")]
            if was_idle {
                if let Some(frontiers) = cluster_frontiers.as_ref() {
                    let floor = Self::decision_bound_source_floor(
                        i64::MIN,
                        frontiers.get(name).copied(),
                        true,
                    );
                    if floor > i64::MIN {
                        let _ = state.install_committed_watermark_floor(floor);
                    }
                }
            }
            let mut advanced = None;
            if let Some(entry) = self.source_entries_for_wm.get(name) {
                let external = entry.source.current_watermark();
                if external > i64::MIN {
                    advanced = state.advance_external_watermark(external);
                }
            }
            let (periodic, all_input_channels_idle) = state.tick_input_channel_idleness();
            #[cfg(feature = "cluster")]
            if all_input_channels_idle {
                if let Some(frontiers) = cluster_frontiers.as_ref() {
                    let floor = Self::decision_bound_source_floor(
                        i64::MIN,
                        frontiers.get(name).copied(),
                        true,
                    );
                    if floor > i64::MIN {
                        let _ = state.install_committed_watermark_floor(floor);
                    }
                }
            }
            advanced = periodic.or(advanced);
            if let Some(id) = source_id {
                #[cfg(feature = "cluster")]
                let visible_watermark = cluster_frontiers.as_ref().map_or_else(
                    || state.generator.current_watermark(),
                    |frontiers| {
                        Self::decision_bound_source_floor(
                            state.generator.current_watermark(),
                            frontiers.get(name).copied(),
                            was_idle || all_input_channels_idle,
                        )
                    },
                );
                #[cfg(not(feature = "cluster"))]
                let visible_watermark = state.generator.current_watermark();
                let global = if state.is_partitioned() {
                    let advanced = trk.update_source(id, visible_watermark);
                    if all_input_channels_idle {
                        trk.mark_idle(id).or(advanced)
                    } else {
                        advanced
                    }
                } else if advanced.is_some() {
                    trk.update_source(id, visible_watermark)
                } else {
                    None
                };
                if let Some(global) = global {
                    self.pipeline_watermark
                        .store(global.timestamp(), std::sync::atomic::Ordering::Relaxed);
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
            if let Err(error) = self.require_process_authority("follower checkpoint control") {
                return crate::pipeline::CheckpointControlOutcome::AdmissionFailed {
                    error: error.to_string(),
                };
            }
            if let Err(error) = self.reconcile_terminal_shuffle_barriers(cc.as_ref()).await {
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return crate::pipeline::CheckpointControlOutcome::AdmissionFailed { error };
            }
            self.last_checkpoint_admission_failure = None;
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
        attempt_deadline: tokio::time::Instant,
        flags: u64,
        admitted_assignment_fence: Option<
            laminar_core::cluster::control::CheckpointAssignmentFence,
        >,
    ) -> crate::pipeline::BarrierOutcome {
        use crate::pipeline::BarrierOutcome;

        if self.delivery_guarantee != laminar_connectors::connector::DeliveryGuarantee::BestEffort {
            if let Err(error) = validate_durable_source_checkpoint_roster(
                &self.checkpoint_source_names,
                &source_checkpoints,
            ) {
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                tracing::warn!(%error, "checkpoint source cut is incomplete");
                return BarrierOutcome::Failed;
            }
        }

        #[cfg(feature = "cluster")]
        let source_checkpoints = match self
            .route_follower_checkpoint_barrier(source_checkpoints, attempt, attempt_started, flags)
            .await
        {
            Ok(source_checkpoints) => source_checkpoints,
            Err(outcome) => return outcome,
        };

        #[cfg(feature = "cluster")]
        if self
            .require_process_authority("checkpoint sink fencing")
            .is_err()
        {
            return BarrierOutcome::Failed;
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

        // Sink fencing, shuffle alignment, and state capture all pause the pipeline. A
        // drop-observing timer also records early failures rather than hiding their latency.
        #[cfg(feature = "cluster")]
        let mut barrier_timing = self.cluster_controller.as_ref().map(|controller| {
            crate::checkpoint_timing::CheckpointBarrierTimingGuard::start_with_context(
                || {
                    Self::checkpoint_barrier_timing_context(
                        controller,
                        attempt,
                        crate::checkpoint_timing::CheckpointBarrierRole::Leader,
                        assignment_fence.as_ref(),
                    )
                },
                self.prom.as_ref(),
                &self.checkpoint_barrier_timings,
                attempt_deadline,
            )
        });
        #[cfg(feature = "cluster")]
        let _stall_timer = barrier_timing
            .is_none()
            .then(|| self.prom.checkpoint_pipeline_stall_duration.start_timer());
        #[cfg(feature = "cluster")]
        let mut local_barrier_timer = barrier_timing
            .is_none()
            .then(|| self.prom.checkpoint_barrier_local_duration.start_timer());
        #[cfg(not(feature = "cluster"))]
        let _stall_timer = self.prom.checkpoint_pipeline_stall_duration.start_timer();
        #[cfg(not(feature = "cluster"))]
        let local_barrier_timer = self.prom.checkpoint_barrier_local_duration.start_timer();

        if self.initial_checkpoint_sink_fence_required() {
            if let Err(outcome) = self.fence_checkpoint_sinks(attempt_deadline).await {
                return outcome;
            }
            #[cfg(feature = "cluster")]
            Self::log_checkpoint_barrier_phase_completed(
                attempt,
                "leader",
                "initial_sink_fence",
                attempt_started,
            );
        }

        #[cfg(feature = "cluster")]
        let checkpoint_rotation_guard = match self
            .checkpoint_capture_rotation_guard_until(assignment_fence.as_ref(), attempt_deadline)
            .await
        {
            Ok(guard) => guard,
            Err(error) => {
                tracing::info!(%error, "checkpoint capture was superseded before shuffle staging");
                return BarrierOutcome::CancelledBeforeCapture;
            }
        };
        // The fixed-point drain executes normal graph cycles, which acquire this same fair read
        // fence. Do not nest the read acquisition: a queued assignment writer between the two
        // reads would otherwise deadlock behind this guard while blocking the inner read.
        #[cfg(feature = "cluster")]
        drop(checkpoint_rotation_guard);

        #[cfg(feature = "cluster")]
        if let Err(outcome) = self
            .align_leader_shuffle(attempt, assignment_fence.as_ref(), attempt_deadline)
            .await
        {
            return outcome;
        }

        #[cfg(feature = "cluster")]
        if let Err(error) = self.validate_checkpoint_assignment(assignment_fence.as_ref()) {
            let error = format!(
                "checkpoint assignment changed after shuffle staging and before state capture: \
                 {error}"
            );
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            tracing::warn!(%error);
            return BarrierOutcome::Failed;
        }

        // Flush drains may publish additional sink rows. Release the assignment read fence for
        // the final sink sync, then reacquire it and revalidate both topology and portable graph
        // quiescence before consuming mutable checkpoint state.
        #[cfg(feature = "cluster")]
        if let Err(outcome) = self.fence_checkpoint_sinks(attempt_deadline).await {
            return outcome;
        }
        #[cfg(feature = "cluster")]
        Self::log_checkpoint_barrier_phase_completed(
            attempt,
            "leader",
            "final_sink_fence",
            attempt_started,
        );
        #[cfg(feature = "cluster")]
        let checkpoint_rotation_guard = match self
            .handoff_capture_rotation_guard_until(assignment_fence.as_ref(), attempt_deadline)
            .await
        {
            Ok(guard) => guard,
            Err(error) => {
                tracing::warn!(%error, "checkpoint lost its portable cut after final sink fencing");
                set_checkpoint_fault(&self.checkpoint_fault, error);
                return BarrierOutcome::Failed;
            }
        };
        #[cfg(feature = "cluster")]
        let reassignment_portable =
            assignment_fence.is_some() && checkpoint_rotation_guard.is_some();
        #[cfg(not(feature = "cluster"))]
        let reassignment_portable = false;

        #[cfg(feature = "cluster")]
        let handoff_replay_pending = flags & laminar_core::checkpoint::flags::HANDOFF != 0
            && !self.graph.handoff_is_quiescent();
        #[cfg(not(feature = "cluster"))]
        let handoff_replay_pending = false;
        let handoff = HandoffCapture::new(flags, handoff_replay_pending);

        #[cfg(feature = "cluster")]
        if fence_intake_after_terminal_handoff_capture(
            &self.intake_gate,
            flags,
            handoff_replay_pending,
        ) {
            tracing::info!(
                "terminal HANDOFF capture fenced graph intake until assignment transition"
            );
        }

        let mut request = match self.build_checkpoint_request() {
            Ok(request) => request,
            Err(error) => {
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                tracing::warn!(%error, "checkpoint channel-state capture failed");
                return BarrierOutcome::Failed;
            }
        };
        let local_watermark = match classify_channel_progress(&request.channel_progress) {
            Ok(watermark) => watermark,
            Err(error) => {
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                tracing::warn!(%error, "checkpoint channel-state classification failed");
                return BarrierOutcome::Failed;
            }
        };
        let (operator_state, operator_state_staged_cap_bytes) =
            match self.capture_leader_checkpoint_state(attempt, attempt_deadline) {
                Ok(capture) => capture,
                Err(outcome) => return outcome,
            };
        #[cfg(feature = "cluster")]
        if let Err(error) = self.validate_checkpoint_assignment(assignment_fence.as_ref()) {
            let error =
                format!("checkpoint assignment changed during mutable state capture: {error}");
            set_checkpoint_fault(&self.checkpoint_fault, error.clone());
            tracing::warn!(%error);
            return BarrierOutcome::Failed;
        }
        let in_flight = match EpochInFlightGuard::claim(
            &self.checkpoint_in_flight,
            &self.checkpoint_fault,
            attempt,
            self.sinks
                .iter()
                .filter(|(_, handle, _, _, _, _)| handle.checkpoint_committable())
                .map(|(_, handle, _, _, _, _)| handle.clone()),
        ) {
            Ok(guard) => guard,
            Err(error) => {
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                let _ = self
                    .abandon_reserved_attempt(attempt, error, flags, assignment_fence.clone())
                    .await;
                return BarrierOutcome::Failed;
            }
        };
        #[cfg(feature = "cluster")]
        drop(checkpoint_rotation_guard);
        #[cfg(feature = "cluster")]
        let leader_proof = match self.take_checkpoint_leader_proof(attempt) {
            Ok(proof) => proof,
            Err(outcome) => return outcome,
        };
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
                            flags,
                            leader_proof: proof.clone(),
                        },
                        fence.clone(),
                    )
                });
        // Capture-time authority is bound only after the final assignment proof is consumed.
        handoff.bind_request(&mut request, reassignment_portable, assignment_fence);
        let mut tail = LeaderTail {
            in_flight,
            coordinator: Arc::clone(&self.coordinator),
            complete_tx: self.checkpoint_complete_tx.clone(),
            request,
            operator_state: Some(operator_state),
            operator_state_staged_cap_bytes,
            mutable_operator_capture_guard: None,
            fan_out: source_checkpoints.clone(),
            local_watermark,
            handoff,
            attempt,
            attempt_started,
            attempt_deadline,
            checkpoint_timeout: attempt_deadline
                .saturating_duration_since(tokio::time::Instant::from_std(attempt_started)),
            serialization_timeout: self.serialization_timeout,
            checkpoint_cleanup_timeout: self.checkpoint_cleanup_timeout,
            fault_on_retryable_failure: self.delivery_guarantee
                == laminar_connectors::connector::DeliveryGuarantee::ExactlyOnce,
            fault_on_unclassified_error: self.delivery_guarantee
                != laminar_connectors::connector::DeliveryGuarantee::BestEffort,
            checkpoint_fault: Arc::clone(&self.checkpoint_fault),
            #[cfg(feature = "cluster")]
            controller: self.cluster_controller.clone(),
            #[cfg(feature = "cluster")]
            leader_proof,
            full_vnode_capture_needed: Arc::clone(&self.full_vnode_capture_needed),
        };
        if let Err(error) = tail.in_flight.seal_sink_epoch_until(attempt_deadline).await {
            let error = format!("leader sink epoch seal failed: {error}");
            fail_reserved_leader_attempt(&mut tail, error.clone(), error).await;
            return BarrierOutcome::Failed;
        }
        #[cfg(feature = "cluster")]
        if let Some(timing) = barrier_timing.as_mut() {
            timing.finish_local_barrier_with_handoff();
        } else {
            drop(local_barrier_timer.take());
        }
        #[cfg(not(feature = "cluster"))]
        drop(local_barrier_timer);
        self.spawn_checkpoint_tail(Self::run_leader_tail(tail));

        #[cfg(feature = "cluster")]
        if let (Some(cc), Some((identity, assignment_fence))) =
            (self.cluster_controller.clone(), resume_certificate)
        {
            let has_shuffle = self.graph.cluster_shuffle_config().is_some();
            if has_shuffle {
                if let Some(timing) = barrier_timing.as_mut() {
                    timing.begin_aligned_resume();
                }
            }
            let resume_timer = (has_shuffle && barrier_timing.is_none())
                .then(|| self.prom.checkpoint_aligned_resume_wait.start_timer());
            let aligned = Self::wait_for_aligned_resume_until(
                has_shuffle,
                &cc,
                identity,
                &assignment_fence,
                attempt_deadline,
            )
            .await;
            if has_shuffle {
                if let Some(timing) = barrier_timing.as_mut() {
                    timing.finish_aligned_resume();
                }
            }
            drop(resume_timer);
            if let Err(error) = aligned {
                set_checkpoint_fault(&self.checkpoint_fault, error);
            }
        }
        BarrierOutcome::Async
    }

    fn record_cycle(&self, events_ingested: u64, _batches: u64, elapsed_ns: u64) {
        let _ = events_ingested; // counted in extract_watermark
        self.prom.cycles.inc();
        self.prom
            .cycle_duration
            .observe(Duration::from_nanos(elapsed_ns).as_secs_f64());
    }

    fn record_cycle_phases(&self, execute_ns: u64, output_store_ns: u64, sink_enqueue_ns: u64) {
        self.prom
            .cycle_execute_duration
            .observe(Duration::from_nanos(execute_ns).as_secs_f64());
        self.prom
            .cycle_output_store_duration
            .observe(Duration::from_nanos(output_store_ns).as_secs_f64());
        self.prom
            .cycle_sink_enqueue_duration
            .observe(Duration::from_nanos(sink_enqueue_ns).as_secs_f64());
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
        // A shuffle wake schedules the cycle; this gate lets that cycle drain the receiver.
        #[cfg(feature = "cluster")]
        {
            if self.graph.cluster_shuffle_config().is_some() {
                return true;
            }
        }
        self.graph.has_deferred_work()
    }

    fn has_runnable_deferred_input(&self) -> bool {
        #[cfg(feature = "cluster")]
        if self
            .graph
            .cluster_shuffle_config()
            .is_some_and(|shuffle| shuffle.receiver.queued_work_ready())
        {
            return true;
        }
        self.graph.has_runnable_deferred_work()
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
            let Some(identity) = Self::certified_announcement(announcement) else {
                let error = format!(
                    "follower checkpoint {} epoch {} lost its certified identity during cancellation",
                    attempt.checkpoint_id, attempt.epoch
                );
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(error);
            };
            let Some(assignment_fence) = announcement.assignment_fence.clone() else {
                let error = format!(
                    "follower checkpoint {} epoch {} lost its assignment certificate during cancellation",
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
                identity.flags,
                reason.to_owned(),
                cleanup_deadline,
            )
            .await?;

            let retirement = if self.graph.cluster_shuffle_config().is_some() {
                match Self::await_rejected_follower_settlement(
                    controller.as_ref(),
                    attempt,
                    &assignment_fence,
                    cleanup_deadline,
                )
                .await
                {
                    Ok(retirement) => Some(retirement),
                    Err(observation_error) => {
                        let error = format!(
                            "follower checkpoint {} epoch {} rejection has no verified durable settlement; \
                             reservation remains fenced: {observation_error}",
                            attempt.checkpoint_id, attempt.epoch,
                        );
                        set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                        return Err(error);
                    }
                }
            } else {
                None
            };

            if let Err(authority_error) =
                self.require_process_authority("follower cancellation settlement")
            {
                let error = authority_error.to_string();
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(error);
            }
            if let Some((settled, assignment_digest)) = retirement {
                self.retire_shuffle_checkpoint_barriers(settled, Some(assignment_digest))?;
            }
            if let Err(authority_error) =
                self.require_process_authority("follower cancellation release")
            {
                let error = authority_error.to_string();
                set_checkpoint_fault(&self.checkpoint_fault, error.clone());
                return Err(error);
            }
            self.finish_pending_follower_attempt(attempt)
        }
        #[cfg(not(feature = "cluster"))]
        {
            let _ = (attempt, reason);
            Ok(())
        }
    }

    fn resolve_authoritative_follower_abort(
        &mut self,
        attempt: CheckpointAttempt,
    ) -> Result<(), String> {
        #[cfg(feature = "cluster")]
        {
            let assignment_digest = self
                .pending_follower_checkpoint
                .as_ref()
                .filter(|announcement| {
                    CheckpointAttempt::new(announcement.epoch, announcement.checkpoint_id)
                        == attempt
                })
                .and_then(|announcement| announcement.assignment_fence.as_ref())
                .map(laminar_core::checkpoint::CheckpointAssignmentFence::digest);
            self.retire_shuffle_checkpoint_barriers(attempt, assignment_digest)?;
            self.finish_pending_follower_attempt(attempt)
        }
        #[cfg(not(feature = "cluster"))]
        {
            let _ = attempt;
            Err("authoritative follower Abort cleanup requires cluster mode".into())
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

/// Encode an Arrow schema as a hex-encoded IPC flatbuffer.
pub(crate) fn encode_arrow_schema(schema: &arrow_schema::Schema) -> String {
    laminar_connectors::config::encode_arrow_schema_ipc(schema)
}

#[cfg(test)]
mod tests;
