//! Connector traits — async `SourceConnector` / `SinkConnector`.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tokio::sync::Notify;

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

/// Delivery guarantee level for the pipeline.
///
/// Configures the expected end-to-end delivery semantics. The pipeline
/// validates at startup that all sources and sinks meet the requirements
/// for the chosen guarantee level.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
pub enum DeliveryGuarantee {
    /// At-least-once: records may be replayed on recovery. Requires
    /// checkpointing but tolerates non-replayable sources (with degradation).
    #[default]
    AtLeastOnce,
    /// Exactly-once: no duplicates or losses. Requires all sources to
    /// support replay, all sinks to support exactly-once, and checkpoint
    /// to be enabled.
    ExactlyOnce,
}

impl std::fmt::Display for DeliveryGuarantee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeliveryGuarantee::AtLeastOnce => write!(f, "at-least-once"),
            DeliveryGuarantee::ExactlyOnce => write!(f, "exactly-once"),
        }
    }
}

impl FromStr for DeliveryGuarantee {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace('-', "_").as_str() {
            "at_least_once" | "atleastonce" => Ok(Self::AtLeastOnce),
            "exactly_once" | "exactlyonce" => Ok(Self::ExactlyOnce),
            other => Err(format!("unknown delivery guarantee: '{other}'")),
        }
    }
}

/// SSL connection mode for `PostgreSQL`-compatible connectors.
///
/// Shared by the `PostgreSQL` sink and `PostgreSQL` CDC source. Variant names
/// follow the `libpq` `sslmode` parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PostgresSslMode {
    /// No SSL.
    Disable,
    /// Try SSL, fall back to unencrypted.
    #[default]
    Prefer,
    /// Require SSL.
    Require,
    /// Require SSL and verify CA certificate.
    VerifyCa,
    /// Require SSL, verify certificate and hostname.
    VerifyFull,
}

impl std::fmt::Display for PostgresSslMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disable => write!(f, "disable"),
            Self::Prefer => write!(f, "prefer"),
            Self::Require => write!(f, "require"),
            Self::VerifyCa => write!(f, "verify-ca"),
            Self::VerifyFull => write!(f, "verify-full"),
        }
    }
}

impl FromStr for PostgresSslMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace('-', "_").as_str() {
            "disable" | "off" => Ok(Self::Disable),
            "prefer" => Ok(Self::Prefer),
            "require" => Ok(Self::Require),
            "verify_ca" | "verifyca" => Ok(Self::VerifyCa),
            "verify_full" | "verifyfull" => Ok(Self::VerifyFull),
            other => Err(format!("unknown SSL mode: '{other}'")),
        }
    }
}

/// A batch of records read from a source connector.
#[derive(Debug, Clone)]
pub struct SourceBatch {
    /// Arrow batch carrying the records.
    pub records: RecordBatch,
    /// The partition this batch came from, if the source is partitioned.
    pub partition: Option<PartitionInfo>,
}

impl SourceBatch {
    /// Construct without partition metadata.
    #[must_use]
    pub fn new(records: RecordBatch) -> Self {
        Self {
            records,
            partition: None,
        }
    }

    /// Construct with partition metadata attached.
    #[must_use]
    pub fn with_partition(records: RecordBatch, partition: PartitionInfo) -> Self {
        Self {
            records,
            partition: Some(partition),
        }
    }

    /// Record count in the batch.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.records.num_rows()
    }
}

/// Source partition identity + current offset (Kafka partition number,
/// CDC slot name, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionInfo {
    /// Partition id — free-form string (Kafka partition number as string,
    /// CDC slot name, file path, …).
    pub id: String,
    /// Current offset — interpretation is connector-specific (Kafka offset
    /// as string, CDC LSN, etc.).
    pub offset: String,
}

impl PartitionInfo {
    /// Construct from id/offset strings or anything that converts.
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

/// Capabilities declared by a sink connector.
#[derive(Debug, Clone)]
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

    /// Default per-call `write_batch` I/O timeout. Users can override via
    /// the `sink.write.timeout.ms` connector property.
    pub suggested_write_timeout: std::time::Duration,
}

impl SinkConnectorCapabilities {
    /// All booleans default to `false`; flip via `with_*` below or by
    /// assigning the fields directly (they're `pub`).
    #[must_use]
    pub fn new(suggested_write_timeout: std::time::Duration) -> Self {
        Self {
            exactly_once: false,
            idempotent: false,
            upsert: false,
            changelog: false,
            two_phase_commit: false,
            schema_evolution: false,
            partitioned: false,
            suggested_write_timeout,
        }
    }

    /// Enable exactly-once semantics (requires epoch + 2PC impl).
    #[must_use]
    pub fn with_exactly_once(mut self) -> Self {
        self.exactly_once = true;
        self
    }
    /// Enable idempotent writes (safe re-delivery on retry).
    #[must_use]
    pub fn with_idempotent(mut self) -> Self {
        self.idempotent = true;
        self
    }
    /// Enable upsert (key-based insert-or-update).
    #[must_use]
    pub fn with_upsert(mut self) -> Self {
        self.upsert = true;
        self
    }
    /// Enable changelog/retraction records.
    #[must_use]
    pub fn with_changelog(mut self) -> Self {
        self.changelog = true;
        self
    }
    /// Enable two-phase commit (`pre_commit` + `commit_epoch`).
    #[must_use]
    pub fn with_two_phase_commit(mut self) -> Self {
        self.two_phase_commit = true;
        self
    }
    /// Enable additive schema evolution.
    #[must_use]
    pub fn with_schema_evolution(mut self) -> Self {
        self.schema_evolution = true;
        self
    }
    /// Enable partitioned writes.
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
/// 1. `open()` — initialize connection, discover schema
/// 2. `poll_batch()` — read batches in a loop
/// 3. `checkpoint()` / `restore()` — manage offsets
/// 4. `close()` — clean shutdown
#[async_trait]
pub trait SourceConnector: Send {
    /// Called once before polling begins.
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError>;

    /// `Ok(None)` = no data currently available; runtime retries after a delay.
    /// `max_records` is a hint — implementations may return fewer.
    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError>;

    /// Resolve the source schema from the `WITH (...)` properties before
    /// DDL reaches the planner. Implementations that hit the network
    /// (e.g. Kafka fetching an Avro schema from a Schema Registry) must
    /// bound their I/O with a timeout and leave the schema empty on
    /// failure rather than hang.
    async fn discover_schema(&mut self, _properties: &std::collections::HashMap<String, String>) {}

    /// Arrow schema of records this source produces.
    fn schema(&self) -> SchemaRef;

    /// Returned checkpoint must contain enough info to resume from the
    /// current position after a restart.
    fn checkpoint(&self) -> SourceCheckpoint;

    /// Called during recovery before polling resumes.
    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError>;

    /// Defaults to `Unknown`; connectors should override.
    fn health_check(&self) -> HealthStatus {
        HealthStatus::Unknown
    }

    /// Current connector metrics snapshot.
    fn metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics::default()
    }

    /// Close the connection and release resources.
    async fn close(&mut self) -> Result<(), ConnectorError>;

    /// Returns a [`Notify`] handle that is signalled when new data is available.
    ///
    /// When `Some`, the pipeline coordinator awaits the notification instead of
    /// polling on a timer, eliminating idle CPU usage. Sources that receive data
    /// asynchronously (WebSocket, CDC replication streams, Kafka) should return
    /// `Some` and call `notify.notify_one()` when data arrives.
    ///
    /// The default implementation returns `None`, which causes the pipeline to
    /// fall back to timer-based polling (suitable for batch/file sources).
    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        None
    }

    /// Returns this connector as a [`SchemaProvider`](crate::schema::SchemaProvider), if supported.
    fn as_schema_provider(&self) -> Option<&dyn crate::schema::SchemaProvider> {
        None
    }

    /// Returns this connector as a [`SchemaRegistryAware`](crate::schema::SchemaRegistryAware), if supported.
    fn as_schema_registry_aware(&self) -> Option<&dyn crate::schema::SchemaRegistryAware> {
        None
    }

    /// Whether this source supports replay from a checkpointed position.
    ///
    /// Sources that return `false` (e.g., WebSocket, raw TCP) cannot seek
    /// back to a previous offset on recovery. Checkpointing still captures
    /// their state for best-effort recovery, but exactly-once semantics
    /// are degraded to at-most-once for events from this source.
    ///
    /// The default implementation returns `true` because most durable
    /// sources (Kafka, CDC, files) support replay.
    fn supports_replay(&self) -> bool {
        true
    }

    /// Returns a shared flag that the source sets to `true` when it
    /// requests an immediate checkpoint.
    ///
    /// This is used by sources that detect external state changes requiring
    /// a checkpoint before proceeding — for example, Kafka consumer group
    /// rebalance (partition revocation). The pipeline coordinator polls
    /// this flag each cycle and clears it after triggering the checkpoint.
    ///
    /// The default returns `None` (no source-initiated checkpoints).
    fn checkpoint_requested(&self) -> Option<Arc<std::sync::atomic::AtomicBool>> {
        None
    }

    /// Acknowledge that `epoch` has been durably committed.
    ///
    /// Called after the manifest is persisted and every exactly-once sink
    /// committed the epoch. Idempotent — a retry after cancellation is
    /// legal.
    ///
    /// # Errors
    ///
    /// Errors are logged; they do not roll back the committed epoch.
    async fn notify_epoch_committed(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Trait for sink connectors that write data to external systems.
///
/// Sink connectors operate in Ring 1, receiving data from Ring 0 and
/// writing to external systems. Implementations that advertise
/// `exactly_once` also implement `begin_epoch`/`pre_commit`/`commit_epoch`/
/// `rollback_epoch`; the runtime drives them via the checkpoint coordinator.
///
/// Lifecycle: `open()` → loop over epochs of `begin_epoch()`,
/// `write_batch()*`, `pre_commit()`, `commit_epoch()` (or `rollback_epoch()`
/// on failure) → `close()`.
#[async_trait]
pub trait SinkConnector: Send {
    /// Open the connection and prepare to accept writes.
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError>;

    /// Must be cancellation-safe: the runtime wraps this in
    /// `tokio::time::timeout`. Don't split a `&mut self` mutation
    /// across an `.await`. In-flight transactional state may remain open
    /// after cancellation; the caller will `rollback_epoch` it.
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError>;

    /// Expected Arrow schema of input batches.
    fn schema(&self) -> SchemaRef;

    /// Default: no-op (at-least-once semantics).
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Phase 1 of 2PC: flush + prepare, do NOT finalize the txn. The
    /// runtime persists the manifest between `pre_commit` and
    /// `commit_epoch`; on failure it calls `rollback_epoch`. Default
    /// delegates to `flush()`.
    async fn pre_commit(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        self.flush().await
    }

    /// Phase 2 of 2PC: finalize the txn. Called after the manifest is
    /// durable. Default: no-op (at-least-once semantics).
    async fn commit_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Must be idempotent: the runtime calls this on every exactly-once
    /// sink after a `pre_commit` failure, including sinks that never
    /// `pre_commit`ed.
    async fn rollback_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Defaults to `Unknown`; connectors should override.
    fn health_check(&self) -> HealthStatus {
        HealthStatus::Unknown
    }

    /// Current sink metrics snapshot.
    fn metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics::default()
    }

    /// Required (no default) so every implementation declares
    /// `suggested_write_timeout`.
    fn capabilities(&self) -> SinkConnectorCapabilities;

    /// Must be internally bounded — the sink task's periodic timer
    /// calls this on every tick. Thorough drains belong in `pre_commit`
    /// / `commit_epoch` / `close`, not here.
    async fn flush(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Close the sink and release resources.
    async fn close(&mut self) -> Result<(), ConnectorError>;

    /// Return a [`SchemaRegistryAware`](crate::schema::SchemaRegistryAware)
    /// view, if the sink speaks a schema registry protocol.
    fn as_schema_registry_aware(&self) -> Option<&dyn crate::schema::SchemaRegistryAware> {
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
        let caps = SinkConnectorCapabilities::new(std::time::Duration::from_secs(5))
            .with_exactly_once()
            .with_changelog()
            .with_partitioned();

        assert!(caps.exactly_once);
        assert!(!caps.idempotent);
        assert!(!caps.upsert);
        assert!(caps.changelog);
        assert!(!caps.schema_evolution);
        assert!(caps.partitioned);
        assert_eq!(
            caps.suggested_write_timeout,
            std::time::Duration::from_secs(5)
        );
    }
}
