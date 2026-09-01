//! Source startup, drain, polling, recovery, and assignment protocol.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tokio::sync::Notify;

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

use super::{
    ConnectorCancellationPolicy, ConnectorTaskTracker, DeliveryGuarantee, SourceBatch,
    SourceContract,
};

/// Intake behavior when a source temporarily has no exact checkpoint cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceCheckpointUnavailablePolicy {
    /// Hold intake until control-plane reconciliation makes the cursor available.
    HoldIntake,
    /// Poll at most one batch before retrying a retained barrier.
    ///
    /// Sources using this policy must bind every returned batch to its exact
    /// current cursor and stop at the first completed replay-unit boundary.
    PollToReplayBoundary,
}

/// Atomic startup position for a source connector.
///
/// A resume request carries both the durable checkpoint attempt and the
/// connector checkpoint captured by that attempt. Connectors must install the
/// position before `start` returns and before `poll_batch` can emit records.
#[derive(Debug, Clone)]
pub enum SourcePosition {
    /// Start from the connector's configured deterministic initial position.
    Initial,
    /// Resume from an exact durable engine checkpoint.
    Resume {
        /// Checkpoint attempt that owns the connector state.
        attempt: laminar_core::checkpoint::CheckpointAttempt,
        /// Connector cursor captured by `attempt`.
        checkpoint: SourceCheckpoint,
    },
}

/// Complete source startup request.
///
/// Startup is intentionally a single operation so a connector cannot become
/// externally active between opening resources and restoring its position.
#[derive(Debug, Clone)]
pub struct SourceStart {
    /// Fully resolved connector configuration.
    config: ConnectorConfig,
    /// Initial or exact recovery position.
    position: SourcePosition,
    /// Pipeline-wide delivery guarantee used for fail-closed cursor policy.
    delivery: DeliveryGuarantee,
}

impl SourceStart {
    /// Construct a source startup request before any connector I/O.
    ///
    /// # Errors
    /// Returns a configuration error when a resume attempt is zero or split across two identities.
    pub fn new(
        config: ConnectorConfig,
        position: SourcePosition,
        delivery: DeliveryGuarantee,
    ) -> Result<Self, ConnectorError> {
        if matches!(
            &position,
            SourcePosition::Resume { attempt, .. } if !attempt.is_canonical()
        ) {
            return Err(ConnectorError::ConfigurationError(
                "source resume must use one nonzero canonical checkpoint ID".into(),
            ));
        }
        Ok(Self {
            config,
            position,
            delivery,
        })
    }

    /// Consume the request into connector-owned startup inputs.
    #[must_use]
    pub fn into_parts(self) -> (ConnectorConfig, SourcePosition, DeliveryGuarantee) {
        (self.config, self.position, self.delivery)
    }
}

/// Exact cluster transition for which a source must stop advancing input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceDrainRequest {
    /// Compact predecessor/target/leader identity.
    pub round: laminar_core::checkpoint::AssignmentDrainId,
}

impl SourceDrainRequest {
    /// Construct a canonical source drain request.
    ///
    /// # Errors
    /// Returns an error when the transition identity is not canonical.
    pub fn new(round: laminar_core::checkpoint::AssignmentDrainId) -> Result<Self, ConnectorError> {
        if !round.is_canonical() {
            return Err(ConnectorError::ConfigurationError(
                "source drain round is not canonical".into(),
            ));
        }
        Ok(Self { round })
    }
}

/// Terminal resolution of one exact source drain round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceDrainOutcome {
    /// The target assignment committed and the source may adopt its target input ownership.
    Commit,
    /// The transition aborted and the source must resume from the predecessor cut.
    Abort,
}

/// Exact round resolution delivered to a source connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceDrainResolution {
    /// Round being resolved.
    pub round: laminar_core::checkpoint::AssignmentDrainId,
    /// Durable transition outcome.
    pub outcome: SourceDrainOutcome,
}

/// Trait for source connectors that read data from external systems.
///
/// Source connectors operate in Ring 1 and push data into Ring 0 via
/// the streaming `Source<ArrowRecord>::push_arrow()` API.
///
/// # Lifecycle
///
/// 1. `start()` — atomically install the configured or recovered cursor and initialize the reader
/// 2. `poll_batch()` — read batches in a loop
/// 3. `checkpoint()` — capture the current connector cursor
/// 4. `close()` — clean shutdown
#[async_trait]
pub trait SourceConnector: Send {
    /// Deadline behavior required by the underlying client implementation.
    ///
    /// Retirement is the conservative default: a new connector must not be
    /// reused after cancellation until every lifecycle future has been audited.
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::RetireConnector
    }

    /// Observe detached tasks whose lifetime may outlast this connector value.
    ///
    /// A connector that spawns detached work must retain the matching
    /// [`crate::connector::ConnectorTaskOwner`] and move a guard into every task. The runtime can
    /// then wait for true terminal completion after dropping the connector.
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        None
    }

    /// Opens the source and establishes its initial or resumed position as one
    /// indivisible lifecycle transition.
    ///
    /// Implementations must not emit records or expose an externally active
    /// consumer before the requested position has been applied successfully.
    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError>;

    /// `Ok(None)` = no data currently available; runtime retries after a delay.
    /// `max_records` is the normal batching target. A source may exceed it only
    /// when one upstream atomic replay unit cannot be split without making its
    /// checkpoint cursor invalid. Such sources must enforce independent hard
    /// record and byte limits and fail before retained data can grow unbounded.
    ///
    /// The runtime may cancel this future at a shutdown or authority deadline.
    /// [`ConnectorCancellationPolicy::CancelSafe`] implementations must not
    /// advance external or checkpoint-visible position across an `.await`
    /// unless dropping the future there preserves replay of every
    /// not-yet-returned record. Stage work privately, then advance the cursor
    /// and return without another cancellation point. The conservative
    /// [`ConnectorCancellationPolicy::RetireConnector`] policy instead makes
    /// the complete connector generation terminal after cancellation.
    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError>;

    /// Resolve the source schema from the connector and format properties before
    /// DDL reaches the planner. Implementations that perform network I/O must
    /// bound it with a timeout. Return `Err(ConnectorError::…)` on failure so
    /// the runtime can surface the cause to DDL — do not log and swallow.
    async fn discover_schema(
        &mut self,
        _properties: &std::collections::HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Arrow schema of records this source produces.
    fn schema(&self) -> SchemaRef;

    /// Returned checkpoint must contain enough info to resume from the
    /// current position after a restart.
    fn checkpoint(&self) -> SourceCheckpoint;

    /// Atomically attempt to capture a source cursor. `Ok(None)` means a
    /// transient control-plane publication is not reconciled yet; the runtime
    /// retains the barrier and retries without advancing it.
    ///
    /// # Errors
    /// Returns a connector error when the source cannot produce a valid cursor for the current
    /// ownership/configuration state.
    fn try_checkpoint(&self) -> Result<Option<SourceCheckpoint>, ConnectorError> {
        Ok(Some(self.checkpoint()))
    }

    /// Choose how the runtime progresses after [`Self::try_checkpoint`] returns `Ok(None)`.
    ///
    /// The default holds intake because polling across an unreconciled
    /// control-plane cursor could move data past the retained barrier.
    /// The result must remain stable after the connector starts.
    fn checkpoint_unavailable_policy(&self) -> SourceCheckpointUnavailablePolicy {
        SourceCheckpointUnavailablePolicy::HoldIntake
    }

    /// Whether this source's data-plane cursor is reconciled with the current
    /// control-plane ownership publication. The runtime does not poll records
    /// or admit checkpoint barriers while this is false.
    ///
    /// Non-partitioned and local sources are always ready. Cluster-aware
    /// sources override this with a lock-free version fence.
    ///
    /// # Errors
    /// Returns a connector error when control-plane reconciliation detects invalid or lost
    /// ownership state that cannot be retried safely.
    fn checkpoint_ready(&self) -> Result<bool, ConnectorError> {
        Ok(true)
    }

    /// Start or advance connector-side control-plane work needed to become
    /// [`checkpoint_ready`](Self::checkpoint_ready). Called even while data
    /// polling and barriers are fenced.
    fn drive_control_plane(&mut self) {}

    /// Stop advancing external input for an exact cluster transition.
    ///
    /// Implementations start the provider operation without blocking and later expose readiness
    /// through [`Self::poll_drain_ready`]. `deadline` is the engine-owned absolute deadline for
    /// this drain attempt; provider retries must not continue beyond it. Actor-only sources are
    /// fenced by the engine and never call this hook; assignment-scoped sources must implement it
    /// explicitly.
    ///
    /// # Errors
    /// Returns an error when the provider cannot start this exact drain round.
    fn begin_drain(
        &mut self,
        _request: &SourceDrainRequest,
        _deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::ConfigurationError(
            "assignment-scoped source does not implement provider drain".into(),
        ))
    }

    /// Whether the exact provider FIFO boundary has been consumed.
    ///
    /// Returns `false` while the reader is still pausing or while pre-boundary payloads remain.
    ///
    /// # Errors
    /// Returns an error when drain progress cannot be observed safely.
    fn poll_drain_ready(
        &mut self,
        _round: laminar_core::checkpoint::AssignmentDrainId,
    ) -> Result<bool, ConnectorError> {
        Err(ConnectorError::ConfigurationError(
            "assignment-scoped source does not expose provider drain readiness".into(),
        ))
    }

    /// Resolve an exact drain after target commit or abort.
    ///
    /// An abort must rewind any client-delivered but engine-unaccepted records before resuming.
    /// A commit must reconcile target ownership before clearing its post-cut filter.
    /// `deadline` is the engine-owned absolute deadline for the complete resolution; provider
    /// retries and blocking client calls must be bounded by its remaining time.
    async fn finish_drain(
        &mut self,
        _resolution: SourceDrainResolution,
        _deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::ConfigurationError(
            "assignment-scoped source does not implement provider drain resolution".into(),
        ))
    }

    /// Install the cluster vnode assignment for a source that advertises
    /// [`crate::connector::SourceTopology::Splittable`]. The source identity is the stable,
    /// canonical catalog object name and must be part of any external split
    /// mapping ABI.
    ///
    /// Embedded, single-node, singleton, and node-local sources are not sent
    /// this hook. The default fails closed so an extension cannot advertise
    /// splittable placement while every cluster node reads the full input.
    ///
    /// # Errors
    /// Returns a configuration error unless the connector implements exact
    /// vnode-scoped input ownership.
    fn set_vnode_assignment(
        &mut self,
        _source_identity: &str,
        _registry: Arc<laminar_core::state::VnodeRegistry>,
        _self_id: laminar_core::state::NodeId,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::ConfigurationError(
            "source advertises splittable placement but does not implement vnode assignment".into(),
        ))
    }

    /// Close the connection and release resources.
    async fn close(&mut self) -> Result<(), ConnectorError>;

    /// Returns a [`Notify`] handle that is signalled when new data is available.
    ///
    /// When `Some`, the pipeline coordinator awaits the notification instead of
    /// polling on a timer, eliminating idle CPU usage. Push-driven sources should
    /// return `Some` and call `notify.notify_one()` when data arrives.
    ///
    /// The default implementation returns `None`, which causes the pipeline to
    /// fall back to timer-based polling (suitable for batch/file sources).
    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        None
    }

    /// Declare recovery and placement semantics for this exact configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the concrete configuration cannot provide a valid
    /// recovery, placement, or input contract.
    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError>;

    /// Return connector-owned semantic options for durable recovery identity.
    ///
    /// The hook must be deterministic, configuration-only, and free of external
    /// I/O. `Some` replaces the raw property map in the pipeline identity;
    /// `None` asks the runtime to use its conservative sanitized-property
    /// fallback. A connector may omit operational endpoints or credentials only
    /// when its checkpoint independently binds the exact external object.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the semantic identity cannot be
    /// derived from the supplied source configuration.
    fn recovery_identity_options(
        &self,
        _config: &ConnectorConfig,
    ) -> Result<Option<std::collections::BTreeMap<String, String>>, ConnectorError> {
        Ok(None)
    }

    /// Acknowledge that `epoch` has been durably committed.
    ///
    /// Called after the manifest and exact engine commit decision are durable. Coordinated
    /// external sink publication may complete asynchronously afterward. The `checkpoint` is the exact
    /// per-source `SourceCheckpoint` that was persisted into the manifest
    /// for this epoch — sources can rely on it to advance external offset
    /// state (broker group offsets, lookup-DB cursors, ack tokens) using
    /// values that match what's durable.
    ///
    /// May be called with an empty `checkpoint` for timer-driven commits
    /// where no per-source state was captured; implementations should
    /// treat that as a no-op for any externally-visible advancement.
    ///
    /// Idempotent — a retry after cancellation is legal.
    ///
    /// # Errors
    ///
    /// The epoch is already durable and cannot be rolled back. During normal processing an error
    /// faults replay-capable pipelines so recovery retries the advisory upstream commit; during
    /// shutdown it is logged and the durable `LaminarDB` checkpoint remains authoritative.
    async fn notify_epoch_committed(
        &mut self,
        _epoch: u64,
        _checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }
}
