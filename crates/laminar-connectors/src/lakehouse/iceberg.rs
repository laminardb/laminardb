//! Apache Iceberg sink connector implementation.
//!
//! [`IcebergSink`] implements [`SinkConnector`], writing Arrow `RecordBatch`
//! data to Apache Iceberg tables with snapshot isolation and exactly-once
//! semantics.
//!
//! # Write Strategies
//!
//! - **Append mode**: Arrow-to-Parquet zero-copy writes for immutable streams
//! - **Upsert mode**: CDC via Iceberg v2 equality delete files + data files
//!
//! Exactly-once semantics use epoch-to-Iceberg-snapshot mapping: each `LaminarDB`
//! epoch maps to exactly one Iceberg snapshot via snapshot summary properties.
//!
//! # Ring Architecture
//!
//! - **Ring 0**: No sink code. Data arrives via SPSC channel (~5ns push).
//! - **Ring 1**: Batch buffering, Parquet writes, Iceberg catalog commits.
//! - **Ring 2**: Schema management, configuration, health checks.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, RecordBatch};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tracing::{debug, info, warn};

use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{SinkConnector, SinkConnectorCapabilities, WriteResult};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::iceberg_config::{DeliveryGuarantee, IcebergSinkConfig, IcebergWriteMode};
use super::iceberg_metrics::IcebergSinkMetrics;

/// Apache Iceberg sink connector.
///
/// Writes Arrow `RecordBatch` to Iceberg tables with snapshot isolation,
/// exactly-once semantics, hidden partitioning, and background maintenance.
///
/// # Lifecycle
///
/// ```text
/// new() -> open() -> [begin_epoch() -> write_batch()* -> commit_epoch()] -> close()
///                          |                                    |
///                          +--- rollback_epoch() (on failure) --+
/// ```
///
/// # Exactly-Once Semantics
///
/// Each `LaminarDB` epoch maps to exactly one Iceberg snapshot. On recovery,
/// the sink checks snapshot summary properties for the last committed epoch
/// via `laminardb.writer-id` and `laminardb.epoch`. If an epoch was already
/// committed, it is skipped (idempotent commit).
pub struct IcebergSink {
    /// Sink configuration.
    config: IcebergSinkConfig,
    /// Arrow schema for input batches (set on first write or from existing table).
    schema: Option<SchemaRef>,
    /// Connector lifecycle state.
    state: ConnectorState,
    /// Current epoch being written.
    current_epoch: u64,
    /// Last successfully committed epoch.
    last_committed_epoch: u64,
    /// `RecordBatch` buffer for the current epoch.
    buffer: Vec<RecordBatch>,
    /// Total rows buffered in current epoch.
    buffered_rows: usize,
    /// Total bytes buffered (estimated) in current epoch.
    buffered_bytes: u64,
    /// Number of data files pending commit in current epoch.
    pending_data_files: usize,
    /// Number of delete files pending commit in current epoch (upsert mode).
    pending_delete_files: usize,
    /// Current Iceberg snapshot ID.
    snapshot_id: i64,
    /// Current table version (sequential commit count).
    table_version: u64,
    /// Time when the current buffer started accumulating.
    buffer_start_time: Option<Instant>,
    /// Sink metrics.
    metrics: IcebergSinkMetrics,
}

impl IcebergSink {
    /// Creates a new Iceberg sink with the given configuration.
    #[must_use]
    pub fn new(config: IcebergSinkConfig) -> Self {
        Self {
            config,
            schema: None,
            state: ConnectorState::Created,
            current_epoch: 0,
            last_committed_epoch: 0,
            buffer: Vec::new(),
            buffered_rows: 0,
            buffered_bytes: 0,
            pending_data_files: 0,
            pending_delete_files: 0,
            snapshot_id: 0,
            table_version: 0,
            buffer_start_time: None,
            metrics: IcebergSinkMetrics::new(),
        }
    }

    /// Creates a new Iceberg sink with an explicit schema.
    #[must_use]
    pub fn with_schema(config: IcebergSinkConfig, schema: SchemaRef) -> Self {
        let mut sink = Self::new(config);
        sink.schema = Some(schema);
        sink
    }

    /// Returns the current connector state.
    #[must_use]
    pub fn state(&self) -> ConnectorState {
        self.state
    }

    /// Returns the current epoch.
    #[must_use]
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch
    }

    /// Returns the last committed epoch.
    #[must_use]
    pub fn last_committed_epoch(&self) -> u64 {
        self.last_committed_epoch
    }

    /// Returns the number of buffered rows pending flush.
    #[must_use]
    pub fn buffered_rows(&self) -> usize {
        self.buffered_rows
    }

    /// Returns the estimated buffered bytes.
    #[must_use]
    pub fn buffered_bytes(&self) -> u64 {
        self.buffered_bytes
    }

    /// Returns the current Iceberg snapshot ID.
    #[must_use]
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Returns the current table version.
    #[must_use]
    pub fn table_version(&self) -> u64 {
        self.table_version
    }

    /// Returns a reference to the sink metrics.
    #[must_use]
    pub fn sink_metrics(&self) -> &IcebergSinkMetrics {
        &self.metrics
    }

    /// Returns the sink configuration.
    #[must_use]
    pub fn config(&self) -> &IcebergSinkConfig {
        &self.config
    }

    /// Checks if a buffer flush is needed based on size or time thresholds.
    #[must_use]
    pub fn should_flush(&self) -> bool {
        if self.buffered_rows >= self.config.max_buffer_records {
            return true;
        }
        if self.buffered_bytes >= self.config.target_file_size as u64 {
            return true;
        }
        if let Some(start) = self.buffer_start_time {
            if start.elapsed() >= self.config.max_buffer_duration {
                return true;
            }
        }
        false
    }

    /// Estimates the byte size of a `RecordBatch`.
    #[must_use]
    pub fn estimate_batch_size(batch: &RecordBatch) -> u64 {
        batch
            .columns()
            .iter()
            .map(|col| col.get_array_memory_size() as u64)
            .sum()
    }

    /// Flushes the internal buffer by writing Parquet data files to Iceberg.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::UnsupportedOperation` because the Iceberg
    /// write path requires iceberg-rust which is not yet wired.
    #[allow(clippy::unused_self)] // will use self when iceberg-rust is wired
    fn flush_buffer_local(&mut self) -> Result<WriteResult, ConnectorError> {
        Err(ConnectorError::UnsupportedOperation(
            "Iceberg sink write path is not yet implemented — \
             flush_buffer_local requires iceberg-rust writer for Parquet file creation"
                .into(),
        ))
    }

    /// Commits pending files as an Iceberg snapshot.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::UnsupportedOperation` because the Iceberg
    /// catalog commit path requires iceberg-rust which is not yet wired.
    #[allow(clippy::unused_self)] // will use self when iceberg-rust is wired
    fn commit_local(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        Err(ConnectorError::UnsupportedOperation(
            "Iceberg sink commit is not yet implemented — \
             commit_local requires iceberg-rust catalog for snapshot creation"
                .into(),
        ))
    }

    /// Splits a changelog `RecordBatch` into insert and delete batches.
    ///
    /// Uses the `_op` metadata column:
    /// - `"I"` (insert), `"+I"`, `"c"` (create), `"r"` (read/snapshot),
    ///   `"+U"` (update-after) -> insert batch (data files)
    /// - `"D"` (delete), `"-D"`, `"-U"` (update-before) -> delete batch
    ///   (equality delete files)
    ///
    /// The returned batches exclude metadata columns (those starting with `_`).
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the `_op` column is
    /// missing or not a string type.
    pub fn split_changelog_batch(
        batch: &RecordBatch,
    ) -> Result<(RecordBatch, RecordBatch), ConnectorError> {
        let op_idx = batch.schema().index_of("_op").map_err(|_| {
            ConnectorError::ConfigurationError(
                "upsert mode requires '_op' column in input schema".into(),
            )
        })?;

        let op_array = batch
            .column(op_idx)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                ConnectorError::ConfigurationError("'_op' column must be String (Utf8) type".into())
            })?;

        let mut insert_indices = Vec::new();
        let mut delete_indices = Vec::new();

        for i in 0..op_array.len() {
            if op_array.is_null(i) {
                continue;
            }
            match op_array.value(i) {
                "I" | "+I" | "c" | "r" | "+U" | "U" => {
                    insert_indices.push(u32::try_from(i).unwrap_or(u32::MAX));
                }
                "D" | "-D" | "-U" => {
                    delete_indices.push(u32::try_from(i).unwrap_or(u32::MAX));
                }
                _ => {} // Skip unknown ops
            }
        }

        let insert_batch = filter_batch_by_indices(batch, &insert_indices)?;
        let delete_batch = filter_batch_by_indices(batch, &delete_indices)?;

        Ok((insert_batch, delete_batch))
    }

    /// Processes a changelog batch in upsert mode.
    ///
    /// Splits into inserts (data files) and deletes (equality delete files).
    fn process_changelog_batch(&mut self, batch: &RecordBatch) -> Result<(), ConnectorError> {
        let (insert_batch, delete_batch) = Self::split_changelog_batch(batch)?;

        // Buffer inserts for data files.
        if insert_batch.num_rows() > 0 {
            self.buffer.push(insert_batch);
        }

        // Record delete count for equality delete files.
        // In production, iceberg-rust writes equality delete files here.
        if delete_batch.num_rows() > 0 {
            self.pending_delete_files += 1;
            self.metrics.record_delete_files(1);
            self.metrics.record_deletes(delete_batch.num_rows() as u64);
        }

        Ok(())
    }
}

#[async_trait]
impl SinkConnector for IcebergSink {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Initializing;

        // Re-parse config if properties provided.
        if !config.properties().is_empty() {
            self.config = IcebergSinkConfig::from_config(config)?;
        }

        info!(
            warehouse = %self.config.warehouse,
            namespace = ?self.config.namespace,
            table = %self.config.table_name,
            catalog_type = %self.config.catalog_type,
            mode = %self.config.write_mode,
            guarantee = %self.config.delivery_guarantee,
            "opening Iceberg sink connector"
        );

        return Err(ConnectorError::UnsupportedOperation(
            "Iceberg sink is not yet implemented — \
             requires iceberg-rust catalog connection (REST/Glue/Hive), \
             table loading, epoch recovery, and schema resolution"
                .into(),
        ));
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: "Running".into(),
                actual: self.state.to_string(),
            });
        }

        if batch.num_rows() == 0 {
            return Ok(WriteResult::new(0, 0));
        }

        // Handle schema on first write.
        if self.schema.is_none() {
            self.schema = Some(batch.schema());
        }

        let num_rows = batch.num_rows();
        let estimated_bytes = Self::estimate_batch_size(batch);

        // Handle upsert mode (changelog splitting).
        if self.config.write_mode == IcebergWriteMode::Upsert {
            self.process_changelog_batch(batch)?;
            self.buffered_rows += num_rows;
            self.buffered_bytes += estimated_bytes;
            if self.buffer_start_time.is_none() {
                self.buffer_start_time = Some(Instant::now());
            }
        } else {
            // Append mode: buffer the batch directly.
            if self.buffer_start_time.is_none() {
                self.buffer_start_time = Some(Instant::now());
            }
            self.buffer.push(batch.clone());
            self.buffered_rows += num_rows;
            self.buffered_bytes += estimated_bytes;
        }

        // Flush if buffer threshold reached.
        if self.should_flush() {
            let result = self.flush_buffer_local()?;
            return Ok(result);
        }

        Ok(WriteResult::new(0, 0))
    }

    fn schema(&self) -> SchemaRef {
        self.schema
            .clone()
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()))
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // For exactly-once, skip epochs already committed.
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && epoch <= self.last_committed_epoch
        {
            warn!(
                epoch,
                last_committed = self.last_committed_epoch,
                "Iceberg: skipping already-committed epoch"
            );
            return Ok(());
        }

        self.current_epoch = epoch;
        self.buffer.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.pending_data_files = 0;
        self.pending_delete_files = 0;
        self.buffer_start_time = None;

        debug!(epoch, "Iceberg: began epoch");
        Ok(())
    }

    async fn pre_commit(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Skip if already committed (exactly-once idempotency).
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && epoch <= self.last_committed_epoch
        {
            return Ok(());
        }

        if !self.buffer.is_empty() {
            self.flush_buffer_local()?;
        }

        debug!(epoch, "Iceberg: pre-committed (data files written)");
        Ok(())
    }

    async fn commit_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Skip if already committed (exactly-once idempotency).
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && epoch <= self.last_committed_epoch
        {
            return Ok(());
        }

        if self.pending_data_files > 0 || self.pending_delete_files > 0 {
            self.commit_local(epoch)?;
        }

        self.last_committed_epoch = epoch;

        info!(
            epoch,
            snapshot_id = self.snapshot_id,
            table_version = self.table_version,
            "Iceberg: committed epoch"
        );

        Ok(())
    }

    async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Discard buffered data and pending files.
        // In production, orphan files (uncommitted data files) will be
        // cleaned up by Iceberg's orphan file cleanup during maintenance.
        self.buffer.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.pending_data_files = 0;
        self.pending_delete_files = 0;
        self.buffer_start_time = None;

        self.metrics.record_rollback();
        warn!(epoch, "Iceberg: rolled back epoch");
        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        match self.state {
            ConnectorState::Running => HealthStatus::Healthy,
            ConnectorState::Created | ConnectorState::Initializing => HealthStatus::Unknown,
            ConnectorState::Paused => HealthStatus::Degraded("connector paused".into()),
            ConnectorState::Recovering => HealthStatus::Degraded("recovering".into()),
            ConnectorState::Closed => HealthStatus::Unhealthy("closed".into()),
            ConnectorState::Failed => HealthStatus::Unhealthy("failed".into()),
        }
    }

    fn metrics(&self) -> ConnectorMetrics {
        self.metrics.to_connector_metrics()
    }

    fn capabilities(&self) -> SinkConnectorCapabilities {
        let mut caps = SinkConnectorCapabilities::default().with_idempotent();

        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            caps = caps.with_exactly_once().with_two_phase_commit();
        }
        if self.config.write_mode == IcebergWriteMode::Upsert {
            caps = caps.with_upsert().with_changelog();
        }
        if self.config.schema_evolution {
            caps = caps.with_schema_evolution();
        }
        if !self.config.partition_spec.is_empty() {
            caps = caps.with_partitioned();
        }

        caps
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        if !self.buffer.is_empty() {
            self.flush_buffer_local()?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing Iceberg sink connector");

        if !self.buffer.is_empty() {
            self.flush_buffer_local()?;
            if self.pending_data_files > 0 || self.pending_delete_files > 0 {
                self.commit_local(self.current_epoch)?;
                self.last_committed_epoch = self.current_epoch;
            }
        }

        self.state = ConnectorState::Closed;

        info!(
            warehouse = %self.config.warehouse,
            table = %self.config.full_table_name(),
            snapshot_id = self.snapshot_id,
            table_version = self.table_version,
            "Iceberg sink connector closed"
        );

        Ok(())
    }
}

impl std::fmt::Debug for IcebergSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSink")
            .field("state", &self.state)
            .field("warehouse", &self.config.warehouse)
            .field("namespace", &self.config.namespace)
            .field("table_name", &self.config.table_name)
            .field("catalog_type", &self.config.catalog_type)
            .field("mode", &self.config.write_mode)
            .field("guarantee", &self.config.delivery_guarantee)
            .field("current_epoch", &self.current_epoch)
            .field("last_committed_epoch", &self.last_committed_epoch)
            .field("buffered_rows", &self.buffered_rows)
            .field("snapshot_id", &self.snapshot_id)
            .field("table_version", &self.table_version)
            .finish_non_exhaustive()
    }
}

// ── Helper functions ────────────────────────────────────────────────

/// Filters a `RecordBatch` to include only rows at the given indices.
///
/// Also strips metadata columns (those starting with `_`) from the output.
fn filter_batch_by_indices(
    batch: &RecordBatch,
    indices: &[u32],
) -> Result<RecordBatch, ConnectorError> {
    let user_schema = Arc::new(arrow_schema::Schema::new(
        batch
            .schema()
            .fields()
            .iter()
            .filter(|f| !f.name().starts_with('_'))
            .cloned()
            .collect::<Vec<_>>(),
    ));

    if indices.is_empty() {
        return Ok(RecordBatch::new_empty(user_schema));
    }

    let indices_array = arrow_array::UInt32Array::from(indices.to_vec());

    let filtered_columns: Vec<Arc<dyn arrow_array::Array>> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !f.name().starts_with('_'))
        .map(|(i, _)| {
            arrow_select::take::take(batch.column(i), &indices_array, None)
                .map_err(|e| ConnectorError::Internal(format!("arrow take failed: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(user_schema, filtered_columns)
        .map_err(|e| ConnectorError::Internal(format!("batch construction failed: {e}")))
}

#[cfg(test)]
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_precision_loss)]
#[allow(clippy::float_cmp)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn test_config() -> IcebergSinkConfig {
        IcebergSinkConfig::new("/tmp/warehouse", "analytics", "trades")
    }

    fn upsert_config() -> IcebergSinkConfig {
        let mut cfg = test_config();
        cfg.write_mode = IcebergWriteMode::Upsert;
        cfg.equality_delete_columns = vec!["id".to_string()];
        cfg
    }

    fn test_batch(n: usize) -> RecordBatch {
        let ids: Vec<i64> = (0..n as i64).collect();
        let names: Vec<&str> = (0..n).map(|_| "test").collect();
        let values: Vec<f64> = (0..n).map(|i| i as f64 * 1.5).collect();

        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    // ── Constructor tests ──

    #[test]
    fn test_new_defaults() {
        let sink = IcebergSink::new(test_config());
        assert_eq!(sink.state(), ConnectorState::Created);
        assert_eq!(sink.current_epoch(), 0);
        assert_eq!(sink.last_committed_epoch(), 0);
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.buffered_bytes(), 0);
        assert_eq!(sink.snapshot_id(), 0);
        assert_eq!(sink.table_version(), 0);
        assert!(sink.schema.is_none());
    }

    #[test]
    fn test_with_schema() {
        let schema = test_schema();
        let sink = IcebergSink::with_schema(test_config(), schema.clone());
        assert_eq!(sink.schema(), schema);
    }

    #[test]
    fn test_schema_empty_when_none() {
        let sink = IcebergSink::new(test_config());
        let schema = sink.schema();
        assert_eq!(schema.fields().len(), 0);
    }

    // ── Batch size estimation ──

    #[test]
    fn test_estimate_batch_size() {
        let batch = test_batch(100);
        let size = IcebergSink::estimate_batch_size(&batch);
        assert!(size > 0);
    }

    #[test]
    fn test_estimate_batch_size_empty() {
        let batch = RecordBatch::new_empty(test_schema());
        let size = IcebergSink::estimate_batch_size(&batch);
        assert!(size < 1024);
    }

    // ── Should flush tests ──

    #[test]
    fn test_should_flush_by_rows() {
        let mut config = test_config();
        config.max_buffer_records = 100;
        let mut sink = IcebergSink::new(config);
        sink.buffered_rows = 99;
        assert!(!sink.should_flush());
        sink.buffered_rows = 100;
        assert!(sink.should_flush());
    }

    #[test]
    fn test_should_flush_by_bytes() {
        let mut config = test_config();
        config.target_file_size = 1000;
        let mut sink = IcebergSink::new(config);
        sink.buffered_bytes = 999;
        assert!(!sink.should_flush());
        sink.buffered_bytes = 1000;
        assert!(sink.should_flush());
    }

    #[test]
    fn test_should_flush_empty() {
        let sink = IcebergSink::new(test_config());
        assert!(!sink.should_flush());
    }

    // ── Batch buffering tests ──

    #[tokio::test]
    async fn test_write_batch_buffering() {
        let mut config = test_config();
        config.max_buffer_records = 100;
        let mut sink = IcebergSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(10);
        let result = sink.write_batch(&batch).await.unwrap();

        // Should buffer, not flush (10 < 100)
        assert_eq!(result.records_written, 0);
        assert_eq!(sink.buffered_rows(), 10);
        assert!(sink.buffered_bytes() > 0);
    }

    #[tokio::test]
    async fn test_write_batch_auto_flush_returns_error() {
        let mut config = test_config();
        config.max_buffer_records = 10;
        let mut sink = IcebergSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(15);
        let result = sink.write_batch(&batch).await;

        // Flush triggers UnsupportedOperation because write path is not implemented
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not yet implemented"));
    }

    #[tokio::test]
    async fn test_write_batch_empty() {
        let mut sink = IcebergSink::new(test_config());
        sink.state = ConnectorState::Running;

        let batch = test_batch(0);
        let result = sink.write_batch(&batch).await.unwrap();
        assert_eq!(result.records_written, 0);
        assert_eq!(sink.buffered_rows(), 0);
    }

    #[tokio::test]
    async fn test_write_batch_not_running() {
        let mut sink = IcebergSink::new(test_config());
        // state is Created, not Running

        let batch = test_batch(10);
        let result = sink.write_batch(&batch).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_batch_sets_schema() {
        let mut sink = IcebergSink::new(test_config());
        sink.state = ConnectorState::Running;
        assert!(sink.schema.is_none());

        let batch = test_batch(5);
        sink.write_batch(&batch).await.unwrap();
        assert!(sink.schema.is_some());
        assert_eq!(sink.schema.unwrap().fields().len(), 3);
    }

    #[tokio::test]
    async fn test_multiple_write_batches_accumulate() {
        let mut config = test_config();
        config.max_buffer_records = 100;
        let mut sink = IcebergSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(10);
        sink.write_batch(&batch).await.unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.write_batch(&batch).await.unwrap();

        assert_eq!(sink.buffered_rows(), 30);
    }

    // ── Epoch lifecycle tests ──

    #[tokio::test]
    async fn test_epoch_lifecycle_pre_commit_fails() {
        let mut sink = IcebergSink::new(test_config());
        sink.state = ConnectorState::Running;

        sink.begin_epoch(1).await.unwrap();
        assert_eq!(sink.current_epoch(), 1);

        let batch = test_batch(10);
        sink.write_batch(&batch).await.unwrap();

        // pre_commit triggers flush which is not implemented
        let result = sink.pre_commit(1).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not yet implemented"));
    }

    #[tokio::test]
    async fn test_epoch_skip_already_committed() {
        let mut config = test_config();
        config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
        let mut sink = IcebergSink::new(config);
        sink.state = ConnectorState::Running;

        // Simulate having already committed epoch 1
        sink.last_committed_epoch = 1;

        // begin_epoch(1) should skip for exactly-once (epoch <= last_committed)
        sink.begin_epoch(1).await.unwrap();

        // commit_epoch(1) should also skip
        sink.commit_epoch(1).await.unwrap();
        assert_eq!(sink.last_committed_epoch(), 1);
    }

    #[tokio::test]
    async fn test_epoch_at_least_once_no_skip() {
        let mut config = test_config();
        config.delivery_guarantee = DeliveryGuarantee::AtLeastOnce;
        let mut sink = IcebergSink::new(config);
        sink.state = ConnectorState::Running;

        // At-least-once doesn't skip even if epoch <= last_committed
        sink.last_committed_epoch = 1;
        sink.begin_epoch(1).await.unwrap();
        assert_eq!(sink.current_epoch(), 1);
        assert_eq!(sink.buffered_rows(), 0);
    }

    #[tokio::test]
    async fn test_rollback_clears_buffer() {
        let mut config = test_config();
        config.max_buffer_records = 1000;
        let mut sink = IcebergSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(50);
        sink.write_batch(&batch).await.unwrap();
        assert_eq!(sink.buffered_rows(), 50);

        sink.rollback_epoch(0).await.unwrap();
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.buffered_bytes(), 0);
        assert_eq!(sink.pending_data_files, 0);
        assert_eq!(sink.pending_delete_files, 0);
    }

    #[tokio::test]
    async fn test_commit_empty_epoch() {
        let mut sink = IcebergSink::new(test_config());
        sink.state = ConnectorState::Running;

        sink.begin_epoch(1).await.unwrap();
        // No writes
        sink.commit_epoch(1).await.unwrap();
        assert_eq!(sink.last_committed_epoch(), 1);
        assert_eq!(sink.table_version(), 0); // No version bump (no files)
    }

    #[tokio::test]
    async fn test_sequential_epochs_pre_commit_fails() {
        let mut sink = IcebergSink::new(test_config());
        sink.state = ConnectorState::Running;

        sink.begin_epoch(1).await.unwrap();
        let batch = test_batch(10);
        sink.write_batch(&batch).await.unwrap();

        // pre_commit flushes, which is not implemented
        let result = sink.pre_commit(1).await;
        assert!(result.is_err());
    }

    // ── Flush tests ──

    #[tokio::test]
    async fn test_explicit_flush_returns_error() {
        let mut config = test_config();
        config.max_buffer_records = 1000;
        let mut sink = IcebergSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(20);
        sink.write_batch(&batch).await.unwrap();
        assert_eq!(sink.buffered_rows(), 20);

        let result = sink.flush().await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not yet implemented"));
    }

    // ── Open and close tests ──

    #[tokio::test]
    async fn test_open_returns_unsupported() {
        let mut sink = IcebergSink::new(test_config());

        let connector_config = ConnectorConfig::new("iceberg");
        let result = sink.open(&connector_config).await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not yet implemented"));
    }

    #[tokio::test]
    async fn test_open_with_properties_returns_unsupported() {
        let mut sink = IcebergSink::new(IcebergSinkConfig::default());

        let mut connector_config = ConnectorConfig::new("iceberg");
        connector_config.set("warehouse", "/data/new_warehouse");
        connector_config.set("namespace", "db.schema");
        connector_config.set("table.name", "new_table");
        connector_config.set("catalog.type", "glue");
        connector_config.set("write.mode", "upsert");
        connector_config.set("equality.delete.columns", "id");

        let result = sink.open(&connector_config).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not yet implemented"));
    }

    #[tokio::test]
    async fn test_close() {
        let mut sink = IcebergSink::new(test_config());
        sink.state = ConnectorState::Running;

        sink.close().await.unwrap();
        assert_eq!(sink.state(), ConnectorState::Closed);
    }

    #[tokio::test]
    async fn test_close_with_buffered_data_returns_error() {
        let mut config = test_config();
        config.max_buffer_records = 1000;
        let mut sink = IcebergSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(30);
        sink.write_batch(&batch).await.unwrap();
        assert_eq!(sink.buffered_rows(), 30);

        // close() tries to flush remaining data, which fails
        let result = sink.close().await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not yet implemented"));
    }

    // ── Health check tests ──

    #[test]
    fn test_health_check_created() {
        let sink = IcebergSink::new(test_config());
        assert_eq!(sink.health_check(), HealthStatus::Unknown);
    }

    #[test]
    fn test_health_check_running() {
        let mut sink = IcebergSink::new(test_config());
        sink.state = ConnectorState::Running;
        assert_eq!(sink.health_check(), HealthStatus::Healthy);
    }

    #[test]
    fn test_health_check_closed() {
        let mut sink = IcebergSink::new(test_config());
        sink.state = ConnectorState::Closed;
        assert!(matches!(sink.health_check(), HealthStatus::Unhealthy(_)));
    }

    #[test]
    fn test_health_check_failed() {
        let mut sink = IcebergSink::new(test_config());
        sink.state = ConnectorState::Failed;
        assert!(matches!(sink.health_check(), HealthStatus::Unhealthy(_)));
    }

    #[test]
    fn test_health_check_paused() {
        let mut sink = IcebergSink::new(test_config());
        sink.state = ConnectorState::Paused;
        assert!(matches!(sink.health_check(), HealthStatus::Degraded(_)));
    }

    // ── Capabilities tests ──

    #[test]
    fn test_capabilities_append_exactly_once() {
        let mut config = test_config();
        config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
        let sink = IcebergSink::new(config);
        let caps = sink.capabilities();
        assert!(caps.exactly_once);
        assert!(caps.idempotent);
        assert!(!caps.upsert);
        assert!(!caps.changelog);
        assert!(!caps.schema_evolution);
        assert!(!caps.partitioned);
    }

    #[test]
    fn test_capabilities_upsert() {
        let sink = IcebergSink::new(upsert_config());
        let caps = sink.capabilities();
        assert!(caps.upsert);
        assert!(caps.changelog);
        assert!(caps.idempotent);
    }

    #[test]
    fn test_capabilities_schema_evolution() {
        let mut config = test_config();
        config.schema_evolution = true;
        let sink = IcebergSink::new(config);
        let caps = sink.capabilities();
        assert!(caps.schema_evolution);
    }

    #[test]
    fn test_capabilities_partitioned() {
        use super::super::iceberg_config::{IcebergPartitionField, IcebergTransform};
        let mut config = test_config();
        config.partition_spec = vec![IcebergPartitionField::new(
            "event_time",
            IcebergTransform::Day,
        )];
        let sink = IcebergSink::new(config);
        let caps = sink.capabilities();
        assert!(caps.partitioned);
    }

    #[test]
    fn test_capabilities_at_least_once() {
        let mut config = test_config();
        config.delivery_guarantee = DeliveryGuarantee::AtLeastOnce;
        let sink = IcebergSink::new(config);
        let caps = sink.capabilities();
        assert!(!caps.exactly_once);
        assert!(caps.idempotent);
    }

    // ── Metrics tests ──

    #[test]
    fn test_metrics_initial() {
        let sink = IcebergSink::new(test_config());
        let m = sink.metrics();
        assert_eq!(m.records_total, 0);
        assert_eq!(m.bytes_total, 0);
        assert_eq!(m.errors_total, 0);
    }

    #[tokio::test]
    async fn test_metrics_after_writes_flush_fails() {
        let mut config = test_config();
        config.max_buffer_records = 5;
        let mut sink = IcebergSink::new(config);
        sink.state = ConnectorState::Running;

        // 10 > max_buffer_records(5), so write_batch triggers flush which fails
        let batch = test_batch(10);
        let result = sink.write_batch(&batch).await;
        assert!(result.is_err());
    }

    // ── Changelog splitting tests ──

    fn changelog_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("_op", DataType::Utf8, false),
            Field::new("_ts_ms", DataType::Int64, false),
        ]))
    }

    fn changelog_batch() -> RecordBatch {
        RecordBatch::try_new(
            changelog_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(StringArray::from(vec!["I", "+U", "D", "+I", "-D"])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_split_changelog_batch() {
        let batch = changelog_batch();
        let (inserts, deletes) = IcebergSink::split_changelog_batch(&batch).unwrap();

        // Inserts: rows 0 (I), 1 (+U), 3 (+I) = 3 rows
        assert_eq!(inserts.num_rows(), 3);
        // Deletes: rows 2 (D), 4 (-D) = 2 rows
        assert_eq!(deletes.num_rows(), 2);

        // Metadata columns should be stripped
        assert_eq!(inserts.num_columns(), 2); // id, name only
        assert_eq!(deletes.num_columns(), 2);

        // Verify insert values
        let insert_ids = inserts
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(insert_ids.value(0), 1);
        assert_eq!(insert_ids.value(1), 2);
        assert_eq!(insert_ids.value(2), 4);

        // Verify delete values
        let delete_ids = deletes
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(delete_ids.value(0), 3);
        assert_eq!(delete_ids.value(1), 5);
    }

    #[test]
    fn test_split_changelog_all_inserts() {
        let schema = changelog_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(StringArray::from(vec!["I", "I"])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap();

        let (inserts, deletes) = IcebergSink::split_changelog_batch(&batch).unwrap();
        assert_eq!(inserts.num_rows(), 2);
        assert_eq!(deletes.num_rows(), 0);
    }

    #[test]
    fn test_split_changelog_all_deletes() {
        let schema = changelog_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(StringArray::from(vec!["D", "D"])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap();

        let (inserts, deletes) = IcebergSink::split_changelog_batch(&batch).unwrap();
        assert_eq!(inserts.num_rows(), 0);
        assert_eq!(deletes.num_rows(), 2);
    }

    #[test]
    fn test_split_changelog_missing_op_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();

        let result = IcebergSink::split_changelog_batch(&batch);
        assert!(result.is_err());
    }

    #[test]
    fn test_split_changelog_snapshot_read() {
        let schema = changelog_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(StringArray::from(vec!["r"])), // snapshot read
                Arc::new(Int64Array::from(vec![100])),
            ],
        )
        .unwrap();

        let (inserts, deletes) = IcebergSink::split_changelog_batch(&batch).unwrap();
        assert_eq!(inserts.num_rows(), 1);
        assert_eq!(deletes.num_rows(), 0);
    }

    #[test]
    fn test_split_changelog_create_op() {
        let schema = changelog_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(StringArray::from(vec!["c"])), // create
                Arc::new(Int64Array::from(vec![100])),
            ],
        )
        .unwrap();

        let (inserts, deletes) = IcebergSink::split_changelog_batch(&batch).unwrap();
        assert_eq!(inserts.num_rows(), 1);
        assert_eq!(deletes.num_rows(), 0);
    }

    // ── Upsert mode tests ──

    #[tokio::test]
    async fn test_upsert_mode_splits_changelog() {
        let mut sink = IcebergSink::new(upsert_config());
        sink.state = ConnectorState::Running;

        let batch = changelog_batch();
        sink.write_batch(&batch).await.unwrap();

        // Should have recorded delete operations
        let m = sink.metrics();
        let deletes = m
            .custom
            .iter()
            .find(|(k, _)| k == "iceberg.changelog_deletes");
        assert_eq!(deletes.unwrap().1, 2.0); // 2 delete rows
    }

    // ── Debug output test ──

    #[test]
    fn test_debug_output() {
        let sink = IcebergSink::new(test_config());
        let debug = format!("{sink:?}");
        assert!(debug.contains("IcebergSink"));
        assert!(debug.contains("/tmp/warehouse"));
        assert!(debug.contains("analytics"));
        assert!(debug.contains("trades"));
    }
}
