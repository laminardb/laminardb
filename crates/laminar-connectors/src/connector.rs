//! Core connector traits.
//!
//! Defines the async traits that all source and sink connectors implement:
//! - `SourceConnector`: Reads data from external systems
//! - `SinkConnector`: Writes data to external systems
//!
//! These traits operate in Ring 1 (background) and communicate with Ring 0
//! through the streaming API (`Source<T>::push_arrow()` and subscriptions).

use std::fmt;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

/// A batch of records read from a source connector.
#[derive(Debug, Clone)]
pub struct SourceBatch {
    /// The records as an Arrow `RecordBatch`.
    pub records: RecordBatch,

    /// The partition this batch came from, if applicable.
    pub partition: Option<PartitionInfo>,
}

impl SourceBatch {
    /// Creates a new source batch.
    #[must_use]
    pub fn new(records: RecordBatch) -> Self {
        Self {
            records,
            partition: None,
        }
    }

    /// Creates a new source batch from a specific partition.
    #[must_use]
    pub fn with_partition(records: RecordBatch, partition: PartitionInfo) -> Self {
        Self {
            records,
            partition: Some(partition),
        }
    }

    /// Returns the number of records in the batch.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.records.num_rows()
    }
}

/// Information about a source partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionInfo {
    /// Partition identifier (e.g., Kafka partition number, CDC slot name).
    pub id: String,

    /// Current offset within this partition.
    pub offset: String,
}

impl PartitionInfo {
    /// Creates a new partition info.
    #[must_use]
    pub fn new(id: impl Into<String>, offset: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            offset: offset.into(),
        }
    }
}

impl fmt::Display for PartitionInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.offset)
    }
}

/// Result of writing a batch to a sink connector.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Number of records successfully written.
    pub records_written: usize,

    /// Number of bytes written.
    pub bytes_written: u64,
}

impl WriteResult {
    /// Creates a new write result.
    #[must_use]
    pub fn new(records_written: usize, bytes_written: u64) -> Self {
        Self {
            records_written,
            bytes_written,
        }
    }
}

/// Capabilities declared by a sink connector.
///
/// This is distinct from `laminar_core::sink::SinkCapabilities` which is for
/// the core reactor sink trait. `SinkConnectorCapabilities` describes the
/// capabilities of an external sink connector implementation.
#[derive(Debug, Clone, Default)]
#[allow(clippy::struct_excessive_bools)]
pub struct SinkConnectorCapabilities {
    /// Whether the sink supports exactly-once semantics via epochs.
    pub exactly_once: bool,

    /// Whether the sink supports idempotent writes.
    pub idempotent: bool,

    /// Whether the sink supports upsert (insert-or-update) writes.
    pub upsert: bool,

    /// Whether the sink can handle changelog/retraction records.
    pub changelog: bool,

    /// Whether the sink supports two-phase commit (pre-commit + commit).
    pub two_phase_commit: bool,

    /// Whether the sink supports schema evolution.
    pub schema_evolution: bool,

    /// Whether the sink supports partitioned writes.
    pub partitioned: bool,
}

impl SinkConnectorCapabilities {
    /// Creates capabilities with exactly-once support.
    #[must_use]
    pub fn with_exactly_once(mut self) -> Self {
        self.exactly_once = true;
        self
    }

    /// Creates capabilities with idempotent write support.
    #[must_use]
    pub fn with_idempotent(mut self) -> Self {
        self.idempotent = true;
        self
    }

    /// Creates capabilities with upsert support.
    #[must_use]
    pub fn with_upsert(mut self) -> Self {
        self.upsert = true;
        self
    }

    /// Creates capabilities with changelog support.
    #[must_use]
    pub fn with_changelog(mut self) -> Self {
        self.changelog = true;
        self
    }

    /// Creates capabilities with two-phase commit support (pre-commit + commit).
    #[must_use]
    pub fn with_two_phase_commit(mut self) -> Self {
        self.two_phase_commit = true;
        self
    }

    /// Creates capabilities with schema evolution support.
    #[must_use]
    pub fn with_schema_evolution(mut self) -> Self {
        self.schema_evolution = true;
        self
    }

    /// Creates capabilities with partitioned write support.
    #[must_use]
    pub fn with_partitioned(mut self) -> Self {
        self.partitioned = true;
        self
    }
}

/// Trait for source connectors that read data from external systems.
///
/// Source connectors operate in Ring 1 and push data into Ring 0 via
/// the streaming `Source<ArrowRecord>::push_arrow()` API.
///
/// # Lifecycle
///
/// 1. `open()` - Initialize connection, discover schema
/// 2. `poll_batch()` - Read batches in a loop
/// 3. `checkpoint()` / `restore()` - Manage offsets
/// 4. `close()` - Clean shutdown
///
/// # Example
///
/// ```rust,ignore
/// struct MySource { /* ... */ }
///
/// #[async_trait]
/// impl SourceConnector for MySource {
///     async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
///         // Connect to external system
///         Ok(())
///     }
///
///     async fn poll_batch(&mut self, max_records: usize) -> Result<Option<SourceBatch>, ConnectorError> {
///         // Read up to max_records from external system
///         Ok(None) // None = no data available yet
///     }
///
///     // ... other methods
/// }
/// ```
#[async_trait]
pub trait SourceConnector: Send {
    /// Opens the connector and initializes the connection.
    ///
    /// Called once before any polling begins. The connector should establish
    /// connections, discover the schema, and prepare for reading.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if connection or initialization fails.
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError>;

    /// Polls for the next batch of records.
    ///
    /// Returns `Ok(Some(batch))` when records are available, or `Ok(None)` when
    /// no data is currently available (the runtime will poll again after a delay).
    ///
    /// The `max_records` parameter is a hint; implementations may return fewer.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` on read failure.
    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError>;

    /// Returns the schema of records produced by this source.
    fn schema(&self) -> SchemaRef;

    /// Creates a checkpoint of the current source position.
    ///
    /// The returned checkpoint contains enough information to resume
    /// reading from this position after a restart.
    fn checkpoint(&self) -> SourceCheckpoint;

    /// Restores the source to a previously checkpointed position.
    ///
    /// Called during recovery before polling resumes.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the checkpoint is invalid or the
    /// seek operation fails.
    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError>;

    /// Returns the current health status of the connector.
    fn health_check(&self) -> HealthStatus {
        HealthStatus::Healthy
    }

    /// Returns current metrics from the connector.
    fn metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics::default()
    }

    /// Closes the connector and releases all resources.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if cleanup fails.
    async fn close(&mut self) -> Result<(), ConnectorError>;

    /// Returns this connector as a [`SchemaProvider`](crate::schema::SchemaProvider), if supported.
    fn as_schema_provider(&self) -> Option<&dyn crate::schema::SchemaProvider> {
        None
    }

    /// Returns this connector as a [`SchemaInferable`](crate::schema::SchemaInferable), if supported.
    fn as_schema_inferable(&self) -> Option<&dyn crate::schema::SchemaInferable> {
        None
    }

    /// Returns this connector as a [`SchemaRegistryAware`](crate::schema::SchemaRegistryAware), if supported.
    fn as_schema_registry_aware(&self) -> Option<&dyn crate::schema::SchemaRegistryAware> {
        None
    }

    /// Returns this connector as a [`SchemaEvolvable`](crate::schema::SchemaEvolvable), if supported.
    fn as_schema_evolvable(&self) -> Option<&dyn crate::schema::SchemaEvolvable> {
        None
    }
}

/// Trait for sink connectors that write data to external systems.
///
/// Sink connectors operate in Ring 1, receiving data from Ring 0 via
/// subscriptions and writing to external systems.
///
/// # Exactly-Once Support
///
/// Sinks that support exactly-once semantics implement the epoch-based
/// methods (`begin_epoch`, `commit_epoch`, `rollback_epoch`). The runtime
/// calls these in coordination with the checkpoint manager.
///
/// # Lifecycle
///
/// 1. `open()` - Initialize connection
/// 2. For each epoch:
///    a. `begin_epoch()` - Start transaction
///    b. `write_batch()` - Write records (may be called multiple times)
///    c. `commit_epoch()` - Commit transaction
/// 3. `close()` - Clean shutdown
#[async_trait]
pub trait SinkConnector: Send {
    /// Opens the connector and initializes the connection.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if connection or initialization fails.
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError>;

    /// Writes a batch of records to the external system.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` on write failure.
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError>;

    /// Returns the expected input schema for this sink.
    fn schema(&self) -> SchemaRef;

    /// Begins a new epoch for exactly-once processing.
    ///
    /// Called by the runtime when a new checkpoint epoch starts.
    /// Default implementation does nothing (at-least-once semantics).
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the epoch cannot be started.
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Pre-commits the current epoch (phase 1 of two-phase commit).
    ///
    /// Called after all writes for this epoch are complete but before the
    /// checkpoint manifest is persisted. The sink should flush any buffered
    /// data and prepare for commit, but must NOT finalize the transaction.
    ///
    /// The protocol is:
    /// 1. `pre_commit(epoch)` — flush/prepare (this method)
    /// 2. Manifest persisted to disk
    /// 3. `commit_epoch(epoch)` — finalize transaction
    /// 4. On failure: `rollback_epoch(epoch)`
    ///
    /// Default implementation delegates to `flush()`.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the pre-commit fails.
    async fn pre_commit(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        self.flush().await
    }

    /// Commits the current epoch (phase 2 of two-phase commit).
    ///
    /// Called by the runtime after the checkpoint manifest is successfully
    /// persisted. The sink should finalize any pending transactions.
    /// Default implementation does nothing (at-least-once semantics).
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the commit fails.
    async fn commit_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Rolls back the current epoch.
    ///
    /// Called by the runtime when a checkpoint fails.
    /// Default implementation does nothing.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the rollback fails.
    async fn rollback_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Returns the current health status of the connector.
    fn health_check(&self) -> HealthStatus {
        HealthStatus::Healthy
    }

    /// Returns current metrics from the connector.
    fn metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics::default()
    }

    /// Returns the capabilities of this sink connector.
    fn capabilities(&self) -> SinkConnectorCapabilities {
        SinkConnectorCapabilities::default()
    }

    /// Flushes any buffered data to the external system.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the flush fails.
    async fn flush(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Closes the connector and releases all resources.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if cleanup fails.
    async fn close(&mut self) -> Result<(), ConnectorError>;

    /// Returns this connector as a [`SchemaRegistryAware`](crate::schema::SchemaRegistryAware), if supported.
    fn as_schema_registry_aware(&self) -> Option<&dyn crate::schema::SchemaRegistryAware> {
        None
    }

    /// Returns this connector as a [`SchemaEvolvable`](crate::schema::SchemaEvolvable), if supported.
    fn as_schema_evolvable(&self) -> Option<&dyn crate::schema::SchemaEvolvable> {
        None
    }
}

#[cfg(test)]
#[allow(clippy::cast_possible_wrap)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn test_batch(n: usize) -> RecordBatch {
        #[allow(clippy::cast_possible_wrap)]
        let ids: Vec<i64> = (0..n as i64).collect();
        RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(ids))]).unwrap()
    }

    #[test]
    fn test_source_batch() {
        let batch = SourceBatch::new(test_batch(10));
        assert_eq!(batch.num_rows(), 10);
        assert!(batch.partition.is_none());
    }

    #[test]
    fn test_source_batch_with_partition() {
        let partition = PartitionInfo::new("0", "1234");
        let batch = SourceBatch::with_partition(test_batch(5), partition);
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.partition.as_ref().unwrap().id, "0");
        assert_eq!(batch.partition.as_ref().unwrap().offset, "1234");
    }

    #[test]
    fn test_partition_info_display() {
        let p = PartitionInfo::new("3", "42");
        assert_eq!(p.to_string(), "3@42");
    }

    #[test]
    fn test_write_result() {
        let result = WriteResult::new(100, 5000);
        assert_eq!(result.records_written, 100);
        assert_eq!(result.bytes_written, 5000);
    }

    #[test]
    fn test_sink_capabilities_builder() {
        let caps = SinkConnectorCapabilities::default()
            .with_exactly_once()
            .with_changelog()
            .with_partitioned();

        assert!(caps.exactly_once);
        assert!(!caps.idempotent);
        assert!(!caps.upsert);
        assert!(caps.changelog);
        assert!(!caps.schema_evolution);
        assert!(caps.partitioned);
    }
}
