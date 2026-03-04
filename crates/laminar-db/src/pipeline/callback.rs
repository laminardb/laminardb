//! Pipeline callback trait and source registration types.
//!
//! Decouples the pipeline coordinator from `db.rs` internals so the
//! TPC coordinator can drive SQL cycles, sink writes, and checkpoints
//! through a narrow interface.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::SourceConnector;

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
}

/// Callback trait for the coordinator to interact with the rest of the DB.
///
/// This decouples the pipeline module from db.rs internals.
#[async_trait::async_trait]
pub trait PipelineCallback: Send + 'static {
    /// Called with accumulated source batches to execute a SQL cycle.
    async fn execute_cycle(
        &mut self,
        source_batches: &HashMap<String, Vec<RecordBatch>>,
        watermark: i64,
    ) -> Result<HashMap<String, Vec<RecordBatch>>, String>;

    /// Called with results to push to stream subscriptions.
    fn push_to_streams(&self, results: &HashMap<String, Vec<RecordBatch>>);

    /// Called with results to write to sinks.
    async fn write_to_sinks(&mut self, results: &HashMap<String, Vec<RecordBatch>>);

    /// Extract watermark from a batch for a given source.
    fn extract_watermark(&mut self, source_name: &str, batch: &RecordBatch);

    /// Filter late rows from a batch. Returns the filtered batch (or None if all late).
    fn filter_late_rows(&self, source_name: &str, batch: &RecordBatch) -> Option<RecordBatch>;

    /// Get the current pipeline watermark.
    fn current_watermark(&self) -> i64;

    /// Perform a periodic checkpoint. Returns true if checkpoint was triggered.
    async fn maybe_checkpoint(&mut self, force: bool) -> bool;

    /// Called when all sources have aligned on a barrier.
    ///
    /// Receives source checkpoints captured at the barrier point (consistent).
    /// The callback should snapshot operator state and persist the checkpoint.
    /// Returns true if the checkpoint succeeded.
    async fn checkpoint_with_barrier(
        &mut self,
        source_checkpoints: HashMap<String, SourceCheckpoint>,
    ) -> bool;

    /// Record cycle metrics.
    fn record_cycle(&self, events_ingested: u64, batches: u64, elapsed_ns: u64);

    /// Poll table sources for incremental CDC changes.
    async fn poll_tables(&mut self);
}
