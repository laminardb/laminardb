//! Pipeline callback trait and source registration types.
//!
//! Decouples the pipeline coordinator from `db.rs` internals so the
//! TPC coordinator can drive SQL cycles, sink writes, and checkpoints
//! through a narrow interface.

use std::sync::Arc;

use arrow_array::RecordBatch;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::SourceConnector;
use rustc_hash::FxHashMap;

/// A registered source with its name and config.
pub struct SourceRegistration {
    /// Source name.
    pub name: String,
    /// The connector (owned).
    pub connector: Box<dyn SourceConnector>,
    /// Connector config (for open).
    pub config: ConnectorConfig,
    /// Whether this source supports replay from a checkpointed position.
    pub supports_replay: bool,
    /// Checkpoint to restore on startup (set during recovery).
    ///
    /// When `Some`, the source adapter calls `connector.restore()` after
    /// `open()` to seek to the checkpointed position. This is how Kafka
    /// sources resume from their last checkpoint offset on recovery.
    pub restore_checkpoint: Option<SourceCheckpoint>,
}

/// Callback trait for the coordinator to interact with the rest of the DB.
/// Trait exists for test seam; production impl is `ConnectorPipelineCallback`.
#[trait_variant::make(Send)]
pub trait PipelineCallback: Send + 'static {
    /// Called with accumulated source batches to execute a SQL cycle.
    async fn execute_cycle(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        watermark: i64,
    ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, String>;

    /// Called with results to push to stream subscriptions.
    fn push_to_streams(&self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>);

    /// Update materialized view stores with cycle results.
    fn update_mv_stores(&self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        let _ = results;
    }

    /// Called with results to write to sinks.
    async fn write_to_sinks(&mut self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>);

    /// Extract watermark from a batch for a given source.
    fn extract_watermark(&mut self, source_name: &str, batch: &RecordBatch);

    /// Filter late rows from a batch. Returns the filtered batch (or None if all late).
    fn filter_late_rows(&self, source_name: &str, batch: &RecordBatch) -> Option<RecordBatch>;

    /// Get the current pipeline watermark.
    fn current_watermark(&self) -> i64;

    /// Perform a periodic (timer-based) checkpoint. At-least-once semantics.
    /// For exactly-once, use [`checkpoint_with_barrier`].
    ///
    /// [`checkpoint_with_barrier`]: PipelineCallback::checkpoint_with_barrier
    async fn maybe_checkpoint(
        &mut self,
        force: bool,
        source_offsets: FxHashMap<String, SourceCheckpoint>,
    ) -> Option<u64>;

    /// Called when all sources have aligned on a barrier.
    async fn checkpoint_with_barrier(
        &mut self,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
    ) -> Option<u64>;

    /// Record cycle metrics.
    fn record_cycle(&self, events_ingested: u64, batches: u64, elapsed_ns: u64);

    /// Poll table sources for incremental CDC changes.
    async fn poll_tables(&mut self);

    /// Apply a DDL control message (add/drop stream) to the running pipeline.
    fn apply_control(&mut self, msg: super::ControlMsg);

    /// Returns `true` when internal buffers are near capacity.
    fn is_backpressured(&self) -> bool {
        false
    }

    /// Returns `true` when deferred operators have pending input to drain.
    fn has_deferred_input(&self) -> bool {
        false
    }
}
