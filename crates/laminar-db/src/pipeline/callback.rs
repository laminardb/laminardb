//! Pipeline callback trait and source registration types.
//!
//! Decouples the pipeline coordinator from `db.rs` internals so the
//! TPC coordinator can drive SQL cycles, sink writes, and checkpoints
//! through a narrow interface.

use std::sync::Arc;

use arrow_array::RecordBatch;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{SourceConnector, SourceContract, SourcePosition};
use laminar_core::state::CheckpointAttempt;
use rustc_hash::{FxHashMap, FxHashSet};

/// Why a barrier checkpoint was deliberately skipped, as opposed to
/// attempted-and-failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// No execution cycles ran since the last checkpoint.
    NoCyclesSinceLastCheckpoint,
    /// A sink write timed out; skip to keep the replay window intact.
    PreservingReplayWindowAfterSinkTimeout,
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SkipReason::NoCyclesSinceLastCheckpoint => "no_cycles_since_last_checkpoint",
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
    },
    /// The admitted attempt terminated without a durable commit.
    Failed {
        /// Exact attempt that failed.
        attempt: CheckpointAttempt,
        /// Stable user-facing failure reason.
        error: String,
    },
}

impl CheckpointCompletion {
    /// Create a completion for a path whose exact attempt is already authoritative.
    #[cfg(any(feature = "cluster", test))]
    #[must_use]
    pub(crate) fn new(
        attempt: CheckpointAttempt,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
    ) -> Self {
        Self::Committed {
            attempt,
            result: crate::checkpoint_coordinator::CheckpointResult {
                success: true,
                checkpoint_id: attempt.checkpoint_id,
                epoch: attempt.epoch,
                duration: std::time::Duration::ZERO,
                error: None,
            },
            source_checkpoints,
        }
    }

    /// Build a completion only when the coordinator result belongs to the admitted attempt.
    pub(crate) fn validated(
        admitted: CheckpointAttempt,
        result: crate::checkpoint_coordinator::CheckpointResult,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
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
}

impl CycleOutcome {
    /// A fully-successful cycle: every domain committed.
    #[must_use]
    pub fn clean(results: FxHashMap<Arc<str>, Vec<RecordBatch>>) -> Self {
        Self {
            results,
            any_failed: false,
            failed_sources: FxHashSet::default(),
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
    /// Durability and placement semantics resolved from the connector configuration.
    pub contract: SourceContract,
    /// Exact position to install atomically when the source starts.
    pub position: SourcePosition,
}

/// Callback trait for the coordinator to interact with the rest of the DB.
/// Trait exists for test seam; production impl is `ConnectorPipelineCallback`.
#[trait_variant::make(Send)]
pub trait PipelineCallback: Send + 'static {
    /// Execute a SQL cycle over the accumulated source batches. `Err` is a whole-cycle
    /// failure (all domains, or a backpressure halt); per-domain faults surface in
    /// [`CycleOutcome`] so healthy domains still commit.
    async fn execute_cycle(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        watermark: i64,
    ) -> Result<CycleOutcome, CycleError>;

    /// Push cycle results to stream subscriptions.
    fn push_to_streams(&self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>);

    /// Update materialized view stores with cycle results.
    fn update_mv_stores(&self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        let _ = results;
    }

    /// Write cycle results to sinks.
    async fn write_to_sinks(&mut self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>);

    /// Extract watermark from a batch for a given source.
    fn extract_watermark(&mut self, source_name: &str, batch: &RecordBatch);

    /// Filter late rows from a batch.
    fn filter_late_rows(&self, source_name: &str, batch: &RecordBatch) -> Option<RecordBatch>;

    /// Current pipeline watermark.
    fn current_watermark(&self) -> i64;

    /// `true` if this node is the cluster leader, or in single-node mode.
    fn is_leader(&self) -> bool {
        true
    }

    /// `true` while a coordinated restart is in flight; the checkpoint admission gate holds.
    /// Default `false`.
    fn is_recovering(&self) -> bool {
        false
    }

    /// `true` if a fatal cycle error should fault for recovery rather than drop-and-continue
    /// (exactly-once, or coordinated recovery). Default `false` (at-least-once drops).
    fn fault_on_cycle_error(&self) -> bool {
        false
    }

    /// Take a pending exactly-once sink failure, if any. A poisoned sink epoch aborts its
    /// transaction; escalating to a pipeline fault (like a cycle error) is the only path that
    /// replays the dropped rows, so the coordinator polls this and faults for recovery (CP-4).
    fn take_sink_fault(&mut self) -> Option<String> {
        None
    }

    /// Record a checkpoint failure observed by the coordinator. Exactly-once implementations
    /// fault for recovery; weaker guarantees may retain retry-on-next-interval behaviour.
    fn record_checkpoint_failure(&mut self, _checkpoint_id: u64, _reason: &str) {}

    /// Record a failure before an exact checkpoint attempt could be reserved.
    fn record_checkpoint_admission_failure(&mut self, _reason: &str) {}

    /// Join tracked asynchronous checkpoint tails before connector teardown. When `abort` is
    /// true, cancel them first because the bounded graceful-drain budget has expired.
    fn settle_checkpoint_tail_tasks(
        &mut self,
        _abort: bool,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send {
        std::future::ready(Ok(()))
    }

    /// Durably reserve the exact attempt before barriers are admitted to sources.
    ///
    /// Implementations must never synthesize an in-memory checkpoint ID. A successful
    /// reservation may be abandoned, but its ID is permanently burned.
    fn reserve_checkpoint_attempt(
        &mut self,
        _attempt_started: std::time::Instant,
    ) -> impl std::future::Future<Output = Result<CheckpointAttempt, String>> + Send {
        std::future::ready(Err(
            "checkpoint coordinator has no durable attempt allocator".into(),
        ))
    }

    /// Abandon a reserved attempt that cannot reach the durable checkpoint tail.
    /// Transactional sinks must roll back that epoch before the next attempt begins.
    fn abandon_checkpoint_attempt(
        &mut self,
        _attempt: CheckpointAttempt,
        _reason: &str,
    ) -> impl std::future::Future<Output = ()> + Send {
        std::future::ready(())
    }

    /// `true` when the cluster is converged enough for the leader to checkpoint; the
    /// cluster impl reads a locally-published verdict, no gossip. Default `true`
    /// (single-node). `impl Future` (not `async fn`) preserves the `trait_variant`
    /// default; `&mut self` keeps the future `Send`.
    fn assignment_ready_for_checkpoint(
        &mut self,
    ) -> impl std::future::Future<Output = bool> + Send {
        std::future::ready(true)
    }

    /// Demote sources idle past their timeout so a quiet input doesn't pin the combined watermark.
    fn tick_idle_watermark(&mut self) {}

    /// Service a cluster follower announcement observed from the leader.
    ///
    /// Periodic, connector-request, manual, and shutdown admission belongs exclusively to the
    /// streaming coordinator. This control seam must never originate a local checkpoint.
    async fn service_checkpoint_control(
        &mut self,
        source_offsets: FxHashMap<String, SourceCheckpoint>,
    ) -> Option<u64>;

    /// Called when all sources have aligned on a barrier.
    async fn checkpoint_with_barrier(
        &mut self,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
        attempt: CheckpointAttempt,
        attempt_started: std::time::Instant,
    ) -> BarrierOutcome;

    /// Record cycle metrics.
    fn record_cycle(&self, events_ingested: u64, batches: u64, elapsed_ns: u64);

    /// Count a fatal cycle error that was dropped-and-continued (at-least-once only).
    fn note_cycle_error(&self) {}

    /// Poll table sources for incremental CDC changes.
    async fn poll_tables(&mut self);

    /// Apply a DDL control message (add/drop stream) to the running pipeline.
    fn apply_control(&mut self, msg: super::ControlMsg);

    /// `true` when internal buffers are near capacity.
    fn is_backpressured(&self) -> bool {
        false
    }

    /// `true` while total operator state exceeds the configured memory budget.
    ///
    /// When over budget, the coordinator throttles intake to one message per cycle
    /// and skips idle-watermark ticking so a paused source is not treated as idle.
    fn state_over_budget(&mut self) -> bool {
        false
    }

    /// Shed idle vnode slices to the cold tier when state approaches the memory budget.
    ///
    /// Runs in the maintenance phase; no-op without a tier or budget. `ready` (not
    /// `async {}`) preserves the `trait_variant` `impl Future` rewrite.
    fn maybe_demote_state(&mut self) -> impl std::future::Future<Output = ()> + Send {
        std::future::ready(())
    }

    /// `true` when deferred operators have pending input to drain.
    fn has_deferred_input(&self) -> bool {
        false
    }

    /// Forward a committed epoch to external SUBSCRIBE consumers.
    fn publish_barrier(&self, epoch: u64, checkpoint_id: u64) {
        let _ = (epoch, checkpoint_id);
    }

    /// Gracefully close sinks on shutdown (abort open transactions, flush) so a restart
    /// re-initialises cleanly. Every sink must be attempted; the result aggregates failures.
    fn close_sinks(&mut self) -> impl std::future::Future<Output = Result<(), String>> + Send {
        std::future::ready(Ok(()))
    }

    /// Register the local source barrier injectors.
    fn set_barrier_injectors(
        &mut self,
        injectors: Vec<(
            Arc<str>,
            laminar_core::checkpoint::CheckpointBarrierInjector,
        )>,
    ) {
        let _ = injectors;
    }
}
