//! Pipeline callback trait and source registration types.
//!
//! Decouples the pipeline coordinator from `db.rs` internals so the
//! TPC coordinator can drive SQL cycles, sink writes, and checkpoints
//! through a narrow interface.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{SourceConnector, SourcePosition};
use laminar_core::checkpoint::{CheckpointAttempt, CheckpointAttemptRelation};
use laminar_core::checkpoint::{CheckpointBarrier, CheckpointBarrierInjector};
use laminar_core::cluster::control::CheckpointAssignmentFence;
use rustc_hash::{FxHashMap, FxHashSet};

#[cfg(feature = "cluster")]
const CHECKPOINT_CONTROL_DIRECT_FALLBACK: Duration = Duration::from_millis(250);
const CHECKPOINT_CONTROL_POLL_FALLBACK: Duration = Duration::from_millis(25);

/// Push wake plus bounded KV fallback for clustered checkpoint control.
#[cfg(feature = "cluster")]
#[doc(hidden)]
pub struct CheckpointControlWake {
    announcements: Option<
        tokio::sync::watch::Receiver<Option<laminar_core::cluster::control::BarrierAnnouncement>>,
    >,
    fallback: Duration,
}

#[cfg(not(feature = "cluster"))]
#[doc(hidden)]
pub struct CheckpointControlWake {
    _private: (),
}

#[cfg(feature = "cluster")]
impl CheckpointControlWake {
    #[must_use]
    pub(crate) fn new(
        announcements: Option<
            tokio::sync::watch::Receiver<
                Option<laminar_core::cluster::control::BarrierAnnouncement>,
            >,
        >,
    ) -> Self {
        let fallback = if announcements.is_some() {
            CHECKPOINT_CONTROL_DIRECT_FALLBACK
        } else {
            CHECKPOINT_CONTROL_POLL_FALLBACK
        };
        Self {
            announcements,
            fallback,
        }
    }

    pub(crate) async fn wait_until(&mut self, fallback_at: tokio::time::Instant) {
        let Some(announcements) = self.announcements.as_mut() else {
            tokio::time::sleep_until(fallback_at).await;
            return;
        };
        tokio::select! {
            biased;
            changed = announcements.changed() => {
                if changed.is_err() {
                    self.announcements = None;
                    self.fallback = CHECKPOINT_CONTROL_POLL_FALLBACK;
                }
            }
            () = tokio::time::sleep_until(fallback_at) => {}
        }
    }

    #[must_use]
    pub(crate) const fn fallback(&self) -> Duration {
        self.fallback
    }

    #[must_use]
    pub(crate) const fn capacity_retry() -> Duration {
        CHECKPOINT_CONTROL_POLL_FALLBACK
    }
}

#[cfg(not(feature = "cluster"))]
impl CheckpointControlWake {
    pub(crate) async fn wait_until(&mut self, fallback_at: tokio::time::Instant) {
        tokio::time::sleep_until(fallback_at).await;
    }

    #[must_use]
    pub(crate) const fn fallback() -> Duration {
        CHECKPOINT_CONTROL_POLL_FALLBACK
    }

    #[must_use]
    pub(crate) const fn capacity_retry() -> Duration {
        CHECKPOINT_CONTROL_POLL_FALLBACK
    }
}

/// Retained source-task command for an exact barrier attempt.
///
/// A release is identity-bound so an old checkpoint cannot resume a source held at a newer cut.
/// `Stop` lets shutdown terminate a held source without briefly reopening data intake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceBarrierSignal {
    Release(CheckpointAttempt),
    Stop,
}

/// Coordinator/callback control for one source's exact barrier command and hold.
#[derive(Clone)]
#[doc(hidden)]
pub struct SourceBarrierControl {
    injector: CheckpointBarrierInjector,
    release_tx: tokio::sync::watch::Sender<Option<SourceBarrierSignal>>,
}

impl SourceBarrierControl {
    pub(crate) fn new(
        injector: CheckpointBarrierInjector,
        release_tx: tokio::sync::watch::Sender<Option<SourceBarrierSignal>>,
    ) -> Self {
        Self {
            injector,
            release_tx,
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn can_trigger(&self) -> bool {
        self.injector.can_trigger()
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn trigger(&self, barrier: CheckpointBarrier) -> bool {
        self.injector.trigger(barrier)
    }

    /// Remove this exact command if it has not been claimed by the source yet.
    pub(crate) fn cancel_exact(&self, barrier: CheckpointBarrier) -> bool {
        self.injector.cancel_exact(barrier)
    }

    /// Release a source that has already emitted this exact attempt's barrier.
    pub(crate) fn release_exact(&self, attempt: CheckpointAttempt) {
        if !attempt.is_canonical() {
            return;
        }
        self.release_tx.send_if_modified(|signal| match *signal {
            Some(SourceBarrierSignal::Stop) => false,
            Some(SourceBarrierSignal::Release(released)) => match released.relation_to(attempt) {
                CheckpointAttemptRelation::Older => {
                    *signal = Some(SourceBarrierSignal::Release(attempt));
                    true
                }
                // Preserve exact/newer releases. Conflicting dimensions are equivocation and
                // deliberately cannot overwrite or authorize either exact hold.
                CheckpointAttemptRelation::Exact
                | CheckpointAttemptRelation::Newer
                | CheckpointAttemptRelation::Conflict => false,
            },
            None => {
                *signal = Some(SourceBarrierSignal::Release(attempt));
                true
            }
        });
    }

    pub(crate) fn stop_hold(&self) {
        let _ = self.release_tx.send(Some(SourceBarrierSignal::Stop));
    }
}

#[cfg(test)]
mod source_barrier_tests {
    use super::*;

    #[test]
    fn noncanonical_release_cannot_publish_or_advance_source_release() {
        let injector = CheckpointBarrierInjector::new();
        let (release_tx, release_rx) = tokio::sync::watch::channel(None);
        let control = SourceBarrierControl::new(injector, release_tx);
        let invalid = CheckpointAttempt::new(6, 7);

        control.release_exact(invalid);
        assert_eq!(*release_rx.borrow(), None);

        let canonical = CheckpointAttempt::canonical(5);
        control.release_exact(canonical);
        assert_eq!(
            *release_rx.borrow(),
            Some(SourceBarrierSignal::Release(canonical))
        );

        control.release_exact(invalid);
        assert_eq!(
            *release_rx.borrow(),
            Some(SourceBarrierSignal::Release(canonical))
        );
    }
}

/// Why a barrier checkpoint was deliberately skipped, as opposed to
/// attempted-and-failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// A sink write timed out; skip to keep the replay window intact.
    PreservingReplayWindowAfterSinkTimeout,
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SkipReason::PreservingReplayWindowAfterSinkTimeout => {
                "preserving_replay_window_after_sink_timeout"
            }
        })
    }
}

/// Outcome of a barrier-aligned checkpoint attempt.
#[derive(Debug)]
pub enum BarrierOutcome {
    /// Checkpoint committed at the given epoch.
    Committed(u64),
    /// Checkpoint is processing asynchronously in the background.
    Async,
    /// Deliberately skipped (see `SkipReason`).
    Skipped(SkipReason),
    /// Topology authority closed before state capture; retry after a stable assignment.
    CancelledBeforeCapture,
    /// The exact attempt was terminated by authoritative cluster control before capture.
    Aborted,
    /// Attempted and failed; retry on the next interval.
    Failed,
}

/// Durable completion of one exact checkpoint attempt.
///
/// The checkpoint ID cannot be reconstructed from the execution epoch: durable ID
/// reservations may be burned when an earlier attempt is abandoned. Keeping the exact
/// attempt on the completion channel prevents downstream barriers and source commits from
/// being attributed to a different checkpoint timeline.
#[derive(Debug)]
pub(crate) enum CheckpointCompletion {
    /// The exact attempt reached its durable commit point.
    Committed {
        /// Exact attempt admitted before capture.
        attempt: CheckpointAttempt,
        /// Coordinator result for the same attempt.
        result: crate::checkpoint_coordinator::CheckpointResult,
        /// Per-source positions persisted by that exact attempt.
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
        /// The committed handoff cut retained aligned replay and is not yet the final cut.
        handoff_replay_pending: bool,
    },
    /// The admitted attempt terminated without a durable commit.
    Failed {
        /// Exact attempt that failed.
        attempt: CheckpointAttempt,
        /// Stable user-facing failure reason.
        error: String,
    },
}

/// Result of servicing authoritative cluster checkpoint control on a follower.
///
/// Exact attempt identity lets the coordinator adopt a leader-started checkpoint for state
/// pressure without mistaking a stale completion for the current baseline.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointControlOutcome {
    /// No new authoritative checkpoint command was observed.
    Idle,
    /// Follower admission failed before an exact leader-prepared attempt could be identified.
    AdmissionFailed { error: String },
    /// The exact leader-prepared attempt was admitted locally.
    Started {
        attempt: CheckpointAttempt,
        captured: bool,
        flags: u64,
    },
    /// The exact leader-prepared attempt was authoritatively aborted before capture.
    Aborted { attempt: CheckpointAttempt },
    /// The exact attempt was cleanly rejected after its shuffle scope closed before capture.
    Cancelled { attempt: CheckpointAttempt },
    /// The exact leader-prepared attempt was rejected before it could remain in flight.
    Failed {
        attempt: CheckpointAttempt,
        error: String,
    },
}

/// Assignment gate result before a checkpoint attempt ID is reserved.
#[doc(hidden)]
#[derive(Debug)]
pub enum CheckpointAssignmentAdmission {
    /// Admission may continue with a local cut or the exact certified cluster assignment.
    Ready {
        assignment_fence: Option<CheckpointAssignmentFence>,
        flags: u64,
        /// Retains the cluster assignment linearization boundary until the exact Prepare and
        /// source-barrier commands are installed. `None` outside a clustered runtime.
        assignment_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    },
    /// Topology is transitioning; retry later without faulting or reserving an attempt.
    Deferred(String),
    /// Assignment authority is invalid or unavailable and the pipeline must fail closed.
    Fault(String),
}

impl CheckpointCompletion {
    /// Create a completion for a path whose exact attempt is already authoritative.
    #[cfg(any(feature = "cluster", test))]
    #[must_use]
    pub(crate) fn new(
        attempt: CheckpointAttempt,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
        handoff_replay_pending: bool,
    ) -> Self {
        Self::Committed {
            attempt,
            result: crate::checkpoint_coordinator::CheckpointResult {
                success: true,
                checkpoint_id: attempt.checkpoint_id,
                epoch: attempt.epoch,
                duration: std::time::Duration::ZERO,
                error: None,
                failure_disposition: None,
            },
            source_checkpoints,
            handoff_replay_pending,
        }
    }

    /// Build a completion only when the coordinator result belongs to the admitted attempt.
    pub(crate) fn validated(
        admitted: CheckpointAttempt,
        result: crate::checkpoint_coordinator::CheckpointResult,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
        handoff_replay_pending: bool,
    ) -> Result<Self, String> {
        let completed = CheckpointAttempt::new(result.epoch, result.checkpoint_id);
        if completed != admitted {
            return Err(format!(
                "checkpoint completion identity mismatch: admitted epoch={} id={}, \
                 coordinator completed epoch={} id={}",
                admitted.epoch, admitted.checkpoint_id, completed.epoch, completed.checkpoint_id,
            ));
        }
        Ok(Self::Committed {
            attempt: admitted,
            result,
            source_checkpoints,
            handoff_replay_pending,
        })
    }

    /// Create a terminal failure for an already-admitted exact attempt.
    pub(crate) fn failed(attempt: CheckpointAttempt, error: impl Into<String>) -> Self {
        Self::Failed {
            attempt,
            error: error.into(),
        }
    }

    /// Exact attempt that reached a terminal outcome.
    #[must_use]
    pub(crate) const fn attempt(&self) -> CheckpointAttempt {
        match self {
            Self::Committed { attempt, .. } | Self::Failed { attempt, .. } => *attempt,
        }
    }
}

/// How a failed `execute_cycle` should be handled by the coordinator.
#[derive(Debug, thiserror::Error)]
pub enum CycleError {
    /// Non-deferrable error: `ExactlyOnce` recovers from checkpoint, `AtLeastOnce` drops it.
    #[error("{0}")]
    Fatal(String),
    /// Shared runtime infrastructure failed; all delivery modes recover from a durable cut.
    #[error("{0}")]
    Recovery(String),
    /// `backpressure_policy=Fail` (shutdown already signaled); stop, don't recover.
    #[error("{0}")]
    Halt(String),
}

/// Result of a pipeline cycle. Per-domain failure isolation lets healthy failure domains
/// commit and advance while a faulted domain's sources are held back for replay.
pub struct CycleOutcome {
    /// Output of the domains that succeeded this cycle.
    pub results: FxHashMap<Arc<str>, Vec<RecordBatch>>,
    /// At least one failure domain faulted (it may have no *local* source, e.g. a cluster
    /// follower reading a remote shuffle), so this can be set with `failed_sources` empty.
    pub any_failed: bool,
    /// Names of sources whose domain faulted; the coordinator must not commit their offsets.
    pub failed_sources: FxHashSet<Arc<str>>,
    /// At least one operator retained work for an exact local retry. This may be set with
    /// `deferred_sources` empty on a cluster worker processing remote shuffle input.
    pub any_deferred: bool,
    /// Names of local sources whose input is retained in the graph. Their staged cursors must
    /// remain uncommitted until a later cycle consumes all retained work.
    pub deferred_sources: FxHashSet<Arc<str>>,
}

impl CycleOutcome {
    /// A fully-successful cycle: every domain committed.
    #[must_use]
    pub fn clean(results: FxHashMap<Arc<str>, Vec<RecordBatch>>) -> Self {
        Self {
            results,
            any_failed: false,
            failed_sources: FxHashSet::default(),
            any_deferred: false,
            deferred_sources: FxHashSet::default(),
        }
    }
}

/// A registered source with its name and config.
pub struct SourceRegistration {
    /// Source name.
    pub name: String,
    /// The connector (owned).
    pub connector: Box<dyn SourceConnector>,
    /// Connector config included in the atomic startup request.
    pub config: ConnectorConfig,
    /// The runtime installed cluster vnode ownership for this source instance.
    ///
    /// This is an engine-owned admission fact, not a connector capability switch.
    pub assignment_scoped: bool,
    /// Exact position to install atomically when the source starts.
    pub position: SourcePosition,
}

/// Callback trait for the coordinator to interact with the rest of the DB.
/// Trait exists for test seam; production impl is `ConnectorPipelineCallback`.
#[trait_variant::make(Send)]
pub trait PipelineCallback: Send + 'static {
    /// Install any newly published recovery cut before the coordinator removes
    /// another source message from its FIFO. The default is a no-op outside a
    /// clustered source-handoff runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if a pending recovery cut cannot be installed.
    fn prepare_source_intake(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Pin decision-bound source frontiers before a new source-admission cycle begins.
    ///
    /// The coordinator does not call this while graph-owned deferred input is replaying, so one
    /// snapshot covers source filtering, initial execution, and every retained replay pass.
    ///
    /// # Errors
    /// Returns an error when the frontier snapshot cannot be installed.
    fn pin_source_frontiers_for_new_cycle(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Execute a SQL cycle over the accumulated source batches. `Err` is a whole-cycle
    /// failure (all domains, or a backpressure halt); per-domain faults surface in
    /// [`CycleOutcome`] so healthy domains still commit.
    async fn execute_cycle(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        watermark: i64,
    ) -> Result<CycleOutcome, CycleError>;

    /// Complete assignment-scoped vnode lifecycle work without admitting source input.
    ///
    /// Recovery can close intake before a predecessor transition has run. Implementations return
    /// `true` only when pending work completed; `false` means idle or waiting for its exact
    /// assignment certificate.
    async fn complete_pending_vnode_transition(&mut self) -> Result<bool, CycleError>;

    /// Drain every graph input that belongs to the frozen checkpoint cut.
    ///
    /// Implementations must deliver each drain pass's outputs before returning. The returned
    /// future is one owner transaction: its caller must await an explicit result or destroy the
    /// complete callback/coordinator generation. It must not drop the future while retaining that
    /// generation. Operators may temporarily own graph input across an await, and a completed graph
    /// pass may have partially published materialized-view, stream, or sink output.
    ///
    /// An implementation may cancel a nested operation for its absolute deadline or process lease
    /// only when it regains control and records or returns a disposition that prevents checkpoint
    /// capture from passing incomplete publication. A future caller that needs an outer timeout,
    /// `select!`, or task abort must first add a coordinator-owned attempt transaction covering
    /// graph/output publication, source-barrier ownership, and attempt cleanup. The current
    /// implementation checks its absolute deadline between complete graph passes and inside
    /// sink-publication awaits whose outcomes are consumed before capture.
    async fn drain_checkpoint_edges_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), CycleError>;

    /// Push cycle results to stream subscriptions.
    ///
    /// # Errors
    ///
    /// Returns an error if subscription delivery rejects the cycle output.
    fn push_to_streams(
        &self,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<(), CycleError> {
        let _ = results;
        Ok(())
    }

    /// Update materialized view stores with cycle results.
    ///
    /// # Errors
    ///
    /// Returns an error if materialized state cannot apply the cycle output.
    fn update_mv_stores(
        &self,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<(), CycleError> {
        let _ = results;
        Ok(())
    }

    /// Write cycle results to sinks, bounded by `deadline` when this is a checkpoint drain.
    async fn write_to_sinks(
        &mut self,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<(), CycleError>;

    /// Extract watermark from a batch for a given source.
    ///
    /// # Errors
    ///
    /// Returns an error when the source batch has invalid event-time metadata.
    fn extract_watermark(
        &mut self,
        source_name: &str,
        batch: &RecordBatch,
        admission_floor: i64,
    ) -> Result<(), CycleError>;

    /// Install the exact input-channel inventory carried by a source cursor.
    ///
    /// # Errors
    ///
    /// Returns an error when the inventory is invalid or conflicts with recovered progress.
    fn reconcile_source_input_channels(
        &mut self,
        source_name: &str,
        input_channels: Option<Arc<[Vec<u8>]>>,
    ) -> Result<(), CycleError>;

    /// Filter late rows while preserving any validated hidden source metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when hidden metadata is malformed or the batch cannot be filtered.
    fn filter_late_rows(
        &self,
        source_name: &str,
        batch: &RecordBatch,
    ) -> Result<Option<RecordBatch>, CycleError>;

    /// Current pipeline watermark.
    fn current_watermark(&self) -> i64;

    /// `true` if this node is the cluster leader, or in single-node mode.
    fn is_leader(&self) -> bool {
        true
    }

    /// `true` while a coordinated restart is in flight; the checkpoint admission gate holds.
    /// Default `false`.
    fn is_recovering(&mut self) -> bool {
        false
    }

    /// `true` if a fatal cycle error should fault for recovery rather than drop-and-continue
    /// (exactly-once, or coordinated recovery). Default `false` (at-least-once drops).
    fn fault_on_cycle_error(&self) -> bool {
        false
    }

    /// Take a pending deterministic pipeline halt. A halt is permanent for the current
    /// deployment and therefore takes precedence over consistency faults that would otherwise
    /// trigger recovery of the same poison input.
    fn take_pipeline_halt(&mut self) -> Option<String> {
        None
    }

    /// Take a pending consistency fault from checkpointing or a poisoned sink epoch. The
    /// coordinator stops intake so recovery can replay from the last committed cut.
    fn take_pipeline_fault(&mut self) -> Option<String> {
        None
    }

    /// Record a checkpoint failure observed by the coordinator. Exactly-once implementations
    /// fault for recovery; weaker guarantees may retain retry-on-next-interval behaviour.
    fn record_checkpoint_failure(&mut self, _checkpoint_id: u64, _reason: &str) {}

    /// Record an invariant failure after the checkpoint itself became durable.
    fn record_checkpoint_continuation_fault(&mut self, _attempt: CheckpointAttempt, _reason: &str) {
    }

    /// Record a failure before an exact checkpoint attempt could be reserved.
    fn record_checkpoint_admission_failure(&mut self, _reason: &str) {}

    /// Join tracked asynchronous checkpoint tails before connector teardown.
    fn settle_checkpoint_tail_tasks(
        &mut self,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send {
        std::future::ready(Ok(()))
    }

    /// Durably reserve the exact attempt before barriers are admitted to sources.
    ///
    /// Implementations must never synthesize an in-memory checkpoint ID. A successful
    /// reservation may be abandoned, but its ID is permanently burned.
    fn reserve_checkpoint_attempt(
        &mut self,
        _deadline: tokio::time::Instant,
    ) -> impl std::future::Future<Output = Result<CheckpointAttempt, String>> + Send {
        std::future::ready(Err(
            "checkpoint coordinator has no durable attempt allocator".into(),
        ))
    }

    /// Durably admit exact checkpoint artifacts, then publish the certified cluster `Prepare`,
    /// before any source or shuffle barrier is injected.
    fn publish_checkpoint_prepare(
        &mut self,
        _attempt: CheckpointAttempt,
        _attempt_started: std::time::Instant,
        _deadline: tokio::time::Instant,
        _flags: u64,
        _assignment_fence: Option<CheckpointAssignmentFence>,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send {
        std::future::ready(Ok(()))
    }

    /// Abandon a reserved attempt that cannot reach the durable checkpoint tail.
    /// Transactional sinks must roll back that epoch before the next attempt begins.
    fn abandon_checkpoint_attempt(
        &mut self,
        _attempt: CheckpointAttempt,
        _reason: &str,
        _flags: u64,
        _assignment_fence: Option<CheckpointAssignmentFence>,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;

    /// Cancel an exact follower source-barrier attempt before capture.
    ///
    /// An attempt admitted with follower ownership releases its exact local reservation and
    /// publishes a negative barrier acknowledgement, even if this process changes role while the
    /// attempt is active. Originator-owned attempts use `abandon_checkpoint_attempt`.
    fn cancel_source_barrier_attempt(
        &mut self,
        _attempt: CheckpointAttempt,
        _reason: &str,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;

    /// Resolve exact local follower state after an authoritative pre-capture
    /// [`BarrierOutcome::Aborted`]. This operation must not publish control traffic or wait on the
    /// network because cluster authority has already terminated the attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if local follower state cannot be resolved exactly.
    fn resolve_authoritative_follower_abort(
        &mut self,
        _attempt: CheckpointAttempt,
    ) -> Result<(), String> {
        Err("authoritative follower Abort cleanup is not implemented by this callback".into())
    }

    /// Capture the exact assignment certificate for a new attempt.
    fn checkpoint_assignment_for_admission(
        &mut self,
        _deadline: tokio::time::Instant,
    ) -> impl std::future::Future<Output = CheckpointAssignmentAdmission> + Send {
        std::future::ready(CheckpointAssignmentAdmission::Ready {
            assignment_fence: None,
            flags: laminar_core::checkpoint::flags::NONE,
            assignment_guard: None,
        })
    }

    /// Wake the coordinator for leader-originated checkpoint control. `None` keeps local
    /// runtimes free of cluster polling.
    fn checkpoint_control_wake(&self) -> Option<CheckpointControlWake> {
        None
    }

    /// Wake the coordinator when inbound data or deferred shuffle work becomes ready.
    fn shuffle_work_wake(&self) -> Option<Arc<tokio::sync::Notify>> {
        None
    }

    /// Demote sources idle past their timeout so a quiet input doesn't pin the combined watermark.
    fn tick_idle_watermark(&mut self) {}

    /// Service a cluster follower announcement observed from the leader.
    ///
    /// Periodic, manual, and shutdown admission belongs exclusively to the streaming coordinator.
    /// This control seam must never originate a local checkpoint.
    async fn service_checkpoint_control(
        &mut self,
        source_offsets: FxHashMap<String, SourceCheckpoint>,
    ) -> CheckpointControlOutcome;

    /// Called when all sources have aligned on a barrier.
    async fn checkpoint_with_barrier(
        &mut self,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
        attempt: CheckpointAttempt,
        attempt_started: std::time::Instant,
        deadline: tokio::time::Instant,
        flags: u64,
        assignment_fence: Option<CheckpointAssignmentFence>,
    ) -> BarrierOutcome;

    /// Record cycle metrics.
    fn record_cycle(&self, events_ingested: u64, batches: u64, elapsed_ns: u64);

    /// Record the three timed phases of one successfully published normal cycle. Checkpoint graph
    /// drains use a separate execution path and must not call this hook.
    fn record_cycle_phases(&self, _execute_ns: u64, _output_store_ns: u64, _sink_enqueue_ns: u64) {}

    /// Count a fatal cycle error that was dropped-and-continued (at-least-once only).
    fn note_cycle_error(&self) {}

    /// Apply a DDL control message (add/drop stream) to the running pipeline.
    fn apply_control(&mut self, msg: super::ControlMsg);

    /// `true` when internal buffers are near capacity.
    fn is_backpressured(&self) -> bool {
        false
    }

    /// `true` while the runtime must not fold source or shuffle input into operator state.
    /// Cluster startup and coordinated recovery use this stronger fence; ordinary backpressure
    /// only pauses source polling.
    fn intake_paused(&self) -> bool {
        false
    }

    /// `true` when deferred operators have pending input to drain.
    fn has_deferred_input(&self) -> bool {
        false
    }

    /// `true` when retained work can run now without waiting for an external wake.
    fn has_runnable_deferred_input(&self) -> bool {
        self.has_deferred_input()
    }

    /// Reserve each subscription log's cursor at the aligned checkpoint cut.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription cut cannot be reserved atomically.
    fn reserve_subscription_cut(&self, _attempt: CheckpointAttempt) -> Result<(), String> {
        Ok(())
    }

    /// Discard an unresolved subscription cut after checkpoint failure.
    fn abort_subscription_cut(&self, _attempt: CheckpointAttempt) {}

    /// Resolve the exact cut for external SUBSCRIBE consumers after durable commit.
    ///
    /// # Errors
    ///
    /// Returns an error if the committed cut cannot be published atomically.
    fn publish_barrier(&self, _attempt: CheckpointAttempt) -> Result<(), String> {
        Ok(())
    }

    /// Terminate provisional subscription delivery before shutdown or recovery replay.
    fn invalidate_subscriptions(&self, _reason: &str) {}

    /// Resolve durable ownership of any open checkpoint-committable sink epoch while its actor is
    /// still live. A failure must leave the actors open so lifecycle teardown can retry.
    fn settle_sink_epoch_for_shutdown(
        &mut self,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send {
        std::future::ready(Ok(()))
    }

    /// Gracefully close sinks on shutdown (abort open transactions, flush) so a restart
    /// re-initialises cleanly. Every sink must be attempted; the result aggregates failures.
    fn close_sinks(&mut self) -> impl std::future::Future<Output = Result<(), String>> + Send {
        std::future::ready(Ok(()))
    }

    /// Register the local source barrier injectors.
    fn set_barrier_injectors(&mut self, injectors: Vec<SourceBarrierControl>) {
        let _ = injectors;
    }
}

#[cfg(all(test, feature = "cluster"))]
mod tests {
    use super::*;

    async fn assert_quiet_wake_at(wake: &mut CheckpointControlWake, delay: Duration) {
        let deadline = tokio::time::Instant::now() + delay;
        assert!(tokio::time::timeout(
            delay.saturating_sub(Duration::from_millis(1)),
            wake.wait_until(deadline),
        )
        .await
        .is_err());
        tokio::time::timeout(Duration::from_millis(2), wake.wait_until(deadline))
            .await
            .expect("checkpoint control fallback exceeded its configured deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn checkpoint_control_wake_uses_bounded_direct_and_poll_fallbacks() {
        let (_direct_tx, direct_rx) = tokio::sync::watch::channel(None);
        let mut direct = CheckpointControlWake::new(Some(direct_rx));
        assert_eq!(direct.fallback(), Duration::from_millis(250));
        assert_quiet_wake_at(&mut direct, Duration::from_millis(250)).await;

        let mut poll = CheckpointControlWake::new(None);
        assert_eq!(poll.fallback(), Duration::from_millis(25));
        assert_quiet_wake_at(&mut poll, Duration::from_millis(25)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn checkpoint_control_wake_degrades_when_direct_delivery_closes() {
        let (direct_tx, direct_rx) = tokio::sync::watch::channel(None);
        let mut wake = CheckpointControlWake::new(Some(direct_rx));
        drop(direct_tx);

        wake.wait_until(tokio::time::Instant::now() + Duration::from_millis(250))
            .await;
        assert_eq!(wake.fallback(), Duration::from_millis(25));
        assert_quiet_wake_at(&mut wake, Duration::from_millis(25)).await;
    }
}
