//! Sink lifecycle, write, flush, rollback, and coordinated-commit protocol.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

use super::{
    ConnectorCancellationPolicy, ConnectorTaskTracker, CoordinatedCommitter, SinkContract,
};

/// Durable runtime identity bound to checkpoint-committable sink staging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkRuntimeContext {
    /// Create-once deployment UUID shared by checkpoint recovery.
    pub deployment_id: String,
    /// Stable sink registration name within the pipeline.
    pub sink_id: String,
    /// Stable nonzero checkpoint participant identifier.
    pub participant_id: u64,
}

/// Summary of a successful `write_batch` call.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Records accepted by the sink.
    pub records_written: usize,
    /// Bytes written to the underlying transport (may be estimated).
    pub bytes_written: u64,
}

impl WriteResult {
    /// Construct with raw counts.
    #[must_use]
    pub fn new(records_written: usize, bytes_written: u64) -> Self {
        Self {
            records_written,
            bytes_written,
        }
    }
}

/// Trait for sink connectors that write data to external systems.
///
/// Sink connectors operate in Ring 1, receiving data from Ring 0 and
/// writing to external systems. Implementations whose contract is
/// [`crate::connector::SinkConsistency::CheckpointCommittable`] prepare checkpoint-owned
/// committables with `begin_epoch`/`pre_commit`, expose a
/// [`CoordinatedCommitter`] for the single external commit, and implement
/// `rollback_epoch`; the runtime drives them via the checkpoint coordinator.
///
/// All sinks follow `open()` → `write_batch()`/`flush()` → `close()`.
/// Checkpoint-committable sinks additionally loop over `begin_epoch()`, staged
/// writes, `pre_commit()`, and coordinated commit (or `rollback_epoch()` on a
/// proven pre-decision failure).
#[async_trait]
pub trait SinkConnector: Send {
    /// Deadline behavior required by the underlying client implementation.
    ///
    /// Retirement is the conservative default: a new connector must not be
    /// reused after cancellation until every lifecycle future has been audited.
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::RetireConnector
    }

    /// Bind the checkpoint runtime identity before `open`.
    ///
    /// The default is a no-op for sinks whose object naming does not depend on
    /// checkpoint identity.
    ///
    /// # Errors
    ///
    /// Implementations return an error when the supplied identity is invalid
    /// or cannot be applied in the connector's current lifecycle state.
    fn bind_runtime_context(&mut self, _context: SinkRuntimeContext) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Observe detached tasks whose lifetime may outlast this connector value.
    ///
    /// A connector that spawns detached work must retain the matching
    /// [`crate::connector::ConnectorTaskOwner`] and move a guard into every task. The runtime can
    /// then wait for true terminal completion after dropping the connector.
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        None
    }

    /// Declare durability, placement, and input semantics for this exact
    /// configuration without opening files, sockets, clients, or transactions.
    ///
    /// The fail-closed default is an ephemeral append-only singleton. Durable
    /// or distributed behaviour must be opted into explicitly.
    ///
    /// # Errors
    ///
    /// Returns an error when the concrete configuration cannot provide a valid
    /// durability, placement, or input contract. The default implementation never fails.
    fn contract(&self, _config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        Ok(SinkContract::default())
    }

    /// Open the connection and prepare to accept writes.
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError>;

    /// Implementations using [`ConnectorCancellationPolicy::CancelSafe`] must
    /// remain valid when this future is dropped at a deadline. For
    /// [`ConnectorCancellationPolicy::RetireConnector`], cancellation makes the
    /// complete connector instance terminal before later work can be processed.
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError>;

    /// Expected Arrow schema of input batches.
    fn schema(&self) -> SchemaRef;

    /// Begin checkpoint-owned staging. Called only for an admitted
    /// checkpoint-committable contract; weaker sinks use the no-op default.
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Flush + prepare, but do not finalize externally. The runtime persists
    /// the checkpoint decision before a designated committer finalizes the
    /// collected descriptors; on failure it calls `rollback_epoch`.
    ///
    /// Returns an opaque commit descriptor for checkpoint-committable sinks (the
    /// committables the designated committer will aggregate), else `None`.
    /// Default delegates to `flush()` and returns `None`.
    ///
    /// # Errors
    /// Returns `ConfigurationError` if the sink exposes a coordinated committer
    /// yet relies on this default — it would finalize epochs with no external commit.
    async fn pre_commit(&mut self, _epoch: u64) -> Result<Option<Vec<u8>>, ConnectorError> {
        if self.as_coordinated_committer().is_some() {
            return Err(ConnectorError::ConfigurationError(
                "sink exposes a coordinated committer but does not override pre_commit".into(),
            ));
        }
        self.flush().await?;
        Ok(None)
    }

    /// Must be idempotent. The runtime calls this on every
    /// checkpoint-committable sink after proving a pre-decision failure,
    /// including sinks that never completed `pre_commit`.
    async fn rollback_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Default per-call `write_batch` I/O timeout. Users can override this via
    /// the `sink.write.timeout.ms` connector property.
    fn suggested_write_timeout(&self) -> std::time::Duration;

    /// Maximum residence time for a non-empty sink buffer before the runtime
    /// invokes [`flush`](Self::flush). Checkpoint-committable sinks ignore the
    /// periodic timer and flush only through their checkpoint protocol.
    fn flush_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(5)
    }

    /// Must be internally bounded — the sink task's periodic timer
    /// calls this on every tick. Thorough drains belong in `pre_commit`
    /// / coordinated commit / `close`, not here.
    async fn flush(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Close the sink and release resources.
    async fn close(&mut self) -> Result<(), ConnectorError>;

    /// Leader-side committer for a checkpoint-committable contract; `None`
    /// for every weaker contract.
    fn as_coordinated_committer(&self) -> Option<&dyn CoordinatedCommitter> {
        None
    }
}
