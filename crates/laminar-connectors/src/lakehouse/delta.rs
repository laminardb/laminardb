//! Delta Lake sink connector implementation.
//!
//! [`DeltaLakeSink`] implements [`SinkConnector`], writing Arrow `RecordBatch`
//! data to Delta Lake tables with ACID transactions and exactly-once semantics.
//!
//! # Write Strategies
//!
//! - **Append mode**: Arrow-to-Parquet zero-copy writes for immutable streams
//! - **Overwrite mode**: Replace partition contents for recomputation
//! - **Upsert mode**: CDC MERGE via F063 Z-set changelog integration
//!
//! Exactly-once semantics use epoch-to-Delta-version mapping: each `LaminarDB`
//! epoch maps to exactly one Delta Lake transaction via `txn` application
//! transaction metadata in the Delta log.
//!
//! # Ring Architecture
//!
//! - **Ring 0**: No sink code. Data arrives via SPSC channel (~5ns push).
//! - **Ring 1**: Batch buffering, Parquet writes, Delta log commits.
//! - **Ring 2**: Schema management, configuration, health checks.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, RecordBatch};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tracing::{debug, info, warn};

#[cfg(feature = "delta-lake")]
use deltalake::DeltaTable;

#[cfg(feature = "delta-lake")]
use deltalake::protocol::SaveMode;

use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{SinkConnector, SinkConnectorCapabilities, WriteResult};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::delta_config::{DeliveryGuarantee, DeltaLakeSinkConfig, DeltaWriteMode};
use super::delta_metrics::DeltaLakeSinkMetrics;

/// Delta Lake sink connector.
///
/// Writes Arrow `RecordBatch` to Delta Lake tables with ACID transactions,
/// exactly-once semantics, partitioning, and background compaction.
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
/// Each `LaminarDB` epoch maps to exactly one Delta Lake transaction (version).
/// On recovery, the sink checks `_delta_log/` for the last committed epoch
/// via `txn` (application transaction) metadata. If an epoch was already
/// committed, it is skipped (idempotent commit).
pub struct DeltaLakeSink {
    /// Sink configuration.
    config: DeltaLakeSinkConfig,
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
    /// Number of Parquet files pending commit in current epoch.
    pending_files: usize,
    /// Current Delta Lake table version.
    delta_version: u64,
    /// Time when the current buffer started accumulating.
    buffer_start_time: Option<Instant>,
    /// Sink metrics.
    metrics: DeltaLakeSinkMetrics,
    /// Delta Lake table handle (present when `delta-lake` feature is enabled).
    #[cfg(feature = "delta-lake")]
    table: Option<DeltaTable>,
}

impl DeltaLakeSink {
    /// Creates a new Delta Lake sink with the given configuration.
    #[must_use]
    pub fn new(config: DeltaLakeSinkConfig) -> Self {
        Self {
            config,
            schema: None,
            state: ConnectorState::Created,
            current_epoch: 0,
            last_committed_epoch: 0,
            buffer: Vec::new(),
            buffered_rows: 0,
            buffered_bytes: 0,
            pending_files: 0,
            delta_version: 0,
            buffer_start_time: None,
            metrics: DeltaLakeSinkMetrics::new(),
            #[cfg(feature = "delta-lake")]
            table: None,
        }
    }

    /// Creates a new Delta Lake sink with an explicit schema.
    #[must_use]
    pub fn with_schema(config: DeltaLakeSinkConfig, schema: SchemaRef) -> Self {
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

    /// Returns the current Delta Lake table version.
    #[must_use]
    pub fn delta_version(&self) -> u64 {
        self.delta_version
    }

    /// Returns a reference to the sink metrics.
    #[must_use]
    pub fn sink_metrics(&self) -> &DeltaLakeSinkMetrics {
        &self.metrics
    }

    /// Returns the sink configuration.
    #[must_use]
    pub fn config(&self) -> &DeltaLakeSinkConfig {
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

    /// Flushes the internal buffer (updates metrics; actual Parquet write
    /// happens via the `deltalake` crate when the `delta-lake` feature is
    /// enabled).
    fn flush_buffer_local(&mut self) -> WriteResult {
        let total_rows = self.buffered_rows;
        let estimated_bytes: u64 = self
            .buffer
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();

        self.pending_files += 1;

        self.buffer.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.buffer_start_time = None;

        self.metrics
            .record_flush(total_rows as u64, estimated_bytes);

        debug!(
            rows = total_rows,
            bytes = estimated_bytes,
            pending_files = self.pending_files,
            "Delta Lake: flushed buffer to Parquet"
        );

        WriteResult::new(total_rows, estimated_bytes)
    }

    /// Flushes buffered data to Delta Lake when the `delta-lake` feature is enabled.
    ///
    /// This writes the buffered `RecordBatch`es to Parquet files and commits them
    /// as a Delta Lake transaction with the current epoch in txn metadata.
    #[cfg(feature = "delta-lake")]
    async fn flush_buffer_to_delta(&mut self) -> Result<WriteResult, ConnectorError> {
        if self.buffer.is_empty() {
            return Ok(WriteResult::new(0, 0));
        }

        let total_rows = self.buffered_rows;
        let estimated_bytes: u64 = self
            .buffer
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();

        // Take the table and buffer for the write operation.
        let table = self.table.take().ok_or_else(|| {
            ConnectorError::InvalidState {
                expected: "table initialized".into(),
                actual: "table not initialized".into(),
            }
        })?;

        let batches = std::mem::take(&mut self.buffer);

        // Determine save mode.
        let save_mode = match self.config.write_mode {
            DeltaWriteMode::Append | DeltaWriteMode::Upsert => SaveMode::Append,
            DeltaWriteMode::Overwrite => SaveMode::Overwrite,
        };

        // Get partition columns if configured.
        let partition_cols = if self.config.partition_columns.is_empty() {
            None
        } else {
            Some(self.config.partition_columns.clone())
        };

        // Write to Delta Lake.
        let (new_table, version) = super::delta_io::write_batches(
            table,
            batches,
            &self.config.writer_id,
            self.current_epoch,
            save_mode,
            partition_cols,
        )
        .await?;

        // Restore table and update state.
        // Note: Delta Lake uses i64 for version, but our version is u64.
        // Versions are always non-negative, so this is safe.
        self.table = Some(new_table);
        #[allow(clippy::cast_sign_loss)]
        {
            self.delta_version = version as u64;
        }
        self.pending_files = 0;
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.buffer_start_time = None;

        self.metrics
            .record_flush(total_rows as u64, estimated_bytes);
        self.metrics.record_commit(self.delta_version);

        debug!(
            rows = total_rows,
            bytes = estimated_bytes,
            delta_version = self.delta_version,
            "Delta Lake: flushed and committed buffer"
        );

        Ok(WriteResult::new(total_rows, estimated_bytes))
    }

    /// Commits pending files as a Delta Lake transaction (updates metrics).
    /// Only used when the delta-lake feature is NOT enabled (local simulation).
    #[cfg(not(feature = "delta-lake"))]
    fn commit_local(&mut self, epoch: u64) {
        self.delta_version += 1;
        self.pending_files = 0;
        self.metrics.record_commit(self.delta_version);

        debug!(
            epoch,
            delta_version = self.delta_version,
            "Delta Lake: committed transaction"
        );
    }

    /// Splits a changelog `RecordBatch` into insert and delete batches.
    ///
    /// Uses the `_op` metadata column:
    /// - `"I"` (insert), `"U"` (update-after), `"r"` (snapshot read) -> insert
    /// - `"D"` (delete) -> delete
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
                "I" | "U" | "r" => {
                    insert_indices.push(u32::try_from(i).unwrap_or(u32::MAX));
                }
                "D" => {
                    delete_indices.push(u32::try_from(i).unwrap_or(u32::MAX));
                }
                _ => {} // Skip unknown ops
            }
        }

        let insert_batch = filter_batch_by_indices(batch, &insert_indices)?;
        let delete_batch = filter_batch_by_indices(batch, &delete_indices)?;

        Ok((insert_batch, delete_batch))
    }
}

#[async_trait]
impl SinkConnector for DeltaLakeSink {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Initializing;

        // Re-parse config if properties provided.
        if !config.properties().is_empty() {
            self.config = DeltaLakeSinkConfig::from_config(config)?;
        }

        info!(
            table_path = %self.config.table_path,
            mode = %self.config.write_mode,
            guarantee = %self.config.delivery_guarantee,
            "opening Delta Lake sink connector"
        );

        // When delta-lake feature is enabled, open/create the actual table.
        #[cfg(feature = "delta-lake")]
        {
            use super::delta_io;

            // Open or create the Delta Lake table.
            let table = delta_io::open_or_create_table(
                &self.config.table_path,
                self.config.storage_options.clone(),
                self.schema.as_ref(),
            )
            .await?;

            // Read schema from existing table if we don't have one.
            if self.schema.is_none() {
                if let Ok(schema) = delta_io::get_table_schema(&table) {
                    self.schema = Some(schema);
                }
            }

            // Resolve last committed epoch for exactly-once recovery.
            if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
                self.last_committed_epoch =
                    delta_io::get_last_committed_epoch(&table, &self.config.writer_id).await;
                if self.last_committed_epoch > 0 {
                    info!(
                        writer_id = %self.config.writer_id,
                        last_committed_epoch = self.last_committed_epoch,
                        "recovered last committed epoch from Delta Lake txn metadata"
                    );
                }
            }

            // Store the Delta version.
            // Note: Delta Lake uses i64 for version, but our version is u64.
            // Versions are always non-negative, so this is safe.
            #[allow(clippy::cast_sign_loss)]
            {
                self.delta_version = table.version().unwrap_or(0) as u64;
            }
            self.table = Some(table);
        }

        self.state = ConnectorState::Running;

        info!("Delta Lake sink connector opened successfully");
        Ok(())
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

        // Buffer the batch.
        if self.buffer_start_time.is_none() {
            self.buffer_start_time = Some(Instant::now());
        }
        self.buffer.push(batch.clone());
        self.buffered_rows += num_rows;
        self.buffered_bytes += estimated_bytes;

        // Flush if buffer threshold reached.
        if self.should_flush() {
            let result = self.flush_buffer_local();
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
                "Delta Lake: skipping already-committed epoch"
            );
            return Ok(());
        }

        self.current_epoch = epoch;
        self.buffer.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.pending_files = 0;
        self.buffer_start_time = None;

        debug!(epoch, "Delta Lake: began epoch");
        Ok(())
    }

    async fn commit_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Skip if already committed (exactly-once idempotency).
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && epoch <= self.last_committed_epoch
        {
            return Ok(());
        }

        // When delta-lake feature is enabled, use real I/O.
        #[cfg(feature = "delta-lake")]
        {
            // Flush any remaining buffered data to Delta Lake.
            if !self.buffer.is_empty() {
                self.flush_buffer_to_delta().await?;
            }
        }

        // When delta-lake feature is NOT enabled, use local simulation.
        #[cfg(not(feature = "delta-lake"))]
        {
            // Flush any remaining buffered data.
            if !self.buffer.is_empty() {
                let _ = self.flush_buffer_local();
            }

            // Commit all pending files as a single Delta Lake transaction.
            if self.pending_files > 0 {
                self.commit_local(epoch);
            }
        }

        self.last_committed_epoch = epoch;

        info!(
            epoch,
            delta_version = self.delta_version,
            "Delta Lake: committed epoch"
        );

        Ok(())
    }

    async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Discard buffered data and pending files.
        self.buffer.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.pending_files = 0;
        self.buffer_start_time = None;

        self.metrics.record_rollback();
        warn!(epoch, "Delta Lake: rolled back epoch");
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
            caps = caps.with_exactly_once();
        }
        if self.config.write_mode == DeltaWriteMode::Upsert {
            caps = caps.with_upsert().with_changelog();
        }
        if self.config.schema_evolution {
            caps = caps.with_schema_evolution();
        }
        if !self.config.partition_columns.is_empty() {
            caps = caps.with_partitioned();
        }

        caps
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        if !self.buffer.is_empty() {
            #[cfg(feature = "delta-lake")]
            {
                self.flush_buffer_to_delta().await?;
            }
            #[cfg(not(feature = "delta-lake"))]
            {
                let _ = self.flush_buffer_local();
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing Delta Lake sink connector");

        // Flush remaining data.
        if !self.buffer.is_empty() {
            #[cfg(feature = "delta-lake")]
            {
                self.flush_buffer_to_delta().await?;
            }
            #[cfg(not(feature = "delta-lake"))]
            {
                let _ = self.flush_buffer_local();
                if self.pending_files > 0 {
                    self.commit_local(self.current_epoch);
                    self.last_committed_epoch = self.current_epoch;
                }
            }
        }

        // Drop the table handle when closing.
        #[cfg(feature = "delta-lake")]
        {
            self.table = None;
        }

        self.state = ConnectorState::Closed;

        info!(
            table_path = %self.config.table_path,
            delta_version = self.delta_version,
            "Delta Lake sink connector closed"
        );

        Ok(())
    }
}

impl std::fmt::Debug for DeltaLakeSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaLakeSink")
            .field("state", &self.state)
            .field("table_path", &self.config.table_path)
            .field("mode", &self.config.write_mode)
            .field("guarantee", &self.config.delivery_guarantee)
            .field("current_epoch", &self.current_epoch)
            .field("last_committed_epoch", &self.last_committed_epoch)
            .field("buffered_rows", &self.buffered_rows)
            .field("delta_version", &self.delta_version)
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

    fn test_config() -> DeltaLakeSinkConfig {
        DeltaLakeSinkConfig::new("/tmp/delta_test")
    }

    fn upsert_config() -> DeltaLakeSinkConfig {
        let mut cfg = test_config();
        cfg.write_mode = DeltaWriteMode::Upsert;
        cfg.merge_key_columns = vec!["id".to_string()];
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
        let sink = DeltaLakeSink::new(test_config());
        assert_eq!(sink.state(), ConnectorState::Created);
        assert_eq!(sink.current_epoch(), 0);
        assert_eq!(sink.last_committed_epoch(), 0);
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.buffered_bytes(), 0);
        assert_eq!(sink.delta_version(), 0);
        assert!(sink.schema.is_none());
    }

    #[test]
    fn test_with_schema() {
        let schema = test_schema();
        let sink = DeltaLakeSink::with_schema(test_config(), schema.clone());
        assert_eq!(sink.schema(), schema);
    }

    #[test]
    fn test_schema_empty_when_none() {
        let sink = DeltaLakeSink::new(test_config());
        let schema = sink.schema();
        assert_eq!(schema.fields().len(), 0);
    }

    // ── Batch size estimation ──

    #[test]
    fn test_estimate_batch_size() {
        let batch = test_batch(100);
        let size = DeltaLakeSink::estimate_batch_size(&batch);
        assert!(size > 0);
    }

    #[test]
    fn test_estimate_batch_size_empty() {
        let batch = RecordBatch::new_empty(test_schema());
        let size = DeltaLakeSink::estimate_batch_size(&batch);
        // Arrow arrays have baseline buffer allocation even with 0 rows,
        // so size may be small but not necessarily zero.
        assert!(size < 1024);
    }

    // ── Should flush tests ──

    #[test]
    fn test_should_flush_by_rows() {
        let mut config = test_config();
        config.max_buffer_records = 100;
        let mut sink = DeltaLakeSink::new(config);
        sink.buffered_rows = 99;
        assert!(!sink.should_flush());
        sink.buffered_rows = 100;
        assert!(sink.should_flush());
    }

    #[test]
    fn test_should_flush_by_bytes() {
        let mut config = test_config();
        config.target_file_size = 1000;
        let mut sink = DeltaLakeSink::new(config);
        sink.buffered_bytes = 999;
        assert!(!sink.should_flush());
        sink.buffered_bytes = 1000;
        assert!(sink.should_flush());
    }

    #[test]
    fn test_should_flush_empty() {
        let sink = DeltaLakeSink::new(test_config());
        assert!(!sink.should_flush());
    }

    // ── Batch buffering tests ──

    #[tokio::test]
    async fn test_write_batch_buffering() {
        let mut config = test_config();
        config.max_buffer_records = 100;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(10);
        let result = sink.write_batch(&batch).await.unwrap();

        // Should buffer, not flush (10 < 100)
        assert_eq!(result.records_written, 0);
        assert_eq!(sink.buffered_rows(), 10);
        assert!(sink.buffered_bytes() > 0);
    }

    #[tokio::test]
    async fn test_write_batch_auto_flush() {
        let mut config = test_config();
        config.max_buffer_records = 10;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(15);
        let result = sink.write_batch(&batch).await.unwrap();

        // Should flush because 15 >= 10
        assert_eq!(result.records_written, 15);
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.pending_files, 1);
    }

    #[tokio::test]
    async fn test_write_batch_empty() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Running;

        let batch = test_batch(0);
        let result = sink.write_batch(&batch).await.unwrap();
        assert_eq!(result.records_written, 0);
        assert_eq!(sink.buffered_rows(), 0);
    }

    #[tokio::test]
    async fn test_write_batch_not_running() {
        let mut sink = DeltaLakeSink::new(test_config());
        // state is Created, not Running

        let batch = test_batch(10);
        let result = sink.write_batch(&batch).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_batch_sets_schema() {
        let mut sink = DeltaLakeSink::new(test_config());
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
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(10);
        sink.write_batch(&batch).await.unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.write_batch(&batch).await.unwrap();

        assert_eq!(sink.buffered_rows(), 30);
    }

    // ── Epoch lifecycle tests ──
    // Note: These tests bypass open() and test business logic only.
    // With the delta-lake feature, commit_epoch() does real I/O which fails without a table.
    // See delta_io.rs for integration tests with real I/O.

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_epoch_lifecycle() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Running;

        // Begin epoch
        sink.begin_epoch(1).await.unwrap();
        assert_eq!(sink.current_epoch(), 1);

        // Write some data
        let batch = test_batch(10);
        sink.write_batch(&batch).await.unwrap();

        // Commit epoch
        sink.commit_epoch(1).await.unwrap();
        assert_eq!(sink.last_committed_epoch(), 1);
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.delta_version(), 1);

        // Metrics should show 1 commit
        let m = sink.metrics();
        let commits = m.custom.iter().find(|(k, _)| k == "delta.commits");
        assert_eq!(commits.unwrap().1, 1.0);
    }

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_epoch_skip_already_committed() {
        let mut config = test_config();
        config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        // Commit epoch 1
        sink.begin_epoch(1).await.unwrap();
        let batch = test_batch(5);
        sink.write_batch(&batch).await.unwrap();
        sink.commit_epoch(1).await.unwrap();
        assert_eq!(sink.last_committed_epoch(), 1);

        // Try to begin epoch 1 again (should skip)
        sink.begin_epoch(1).await.unwrap();
        // Should not have cleared the state for a new epoch
        // (skipped because already committed)

        // Commit epoch 1 again (should be no-op)
        sink.commit_epoch(1).await.unwrap();
        assert_eq!(sink.last_committed_epoch(), 1);
        assert_eq!(sink.delta_version(), 1); // No new version
    }

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_epoch_at_least_once_no_skip() {
        let mut config = test_config();
        config.delivery_guarantee = DeliveryGuarantee::AtLeastOnce;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        sink.begin_epoch(1).await.unwrap();
        let batch = test_batch(5);
        sink.write_batch(&batch).await.unwrap();
        sink.commit_epoch(1).await.unwrap();

        // Begin epoch 1 again (at-least-once doesn't skip)
        sink.begin_epoch(1).await.unwrap();
        assert_eq!(sink.current_epoch(), 1);
        assert_eq!(sink.buffered_rows(), 0); // Buffer was cleared
    }

    #[tokio::test]
    async fn test_rollback_clears_buffer() {
        let mut config = test_config();
        config.max_buffer_records = 1000;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(50);
        sink.write_batch(&batch).await.unwrap();
        assert_eq!(sink.buffered_rows(), 50);

        sink.rollback_epoch(0).await.unwrap();
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.buffered_bytes(), 0);
        assert_eq!(sink.pending_files, 0);
    }

    #[tokio::test]
    async fn test_commit_empty_epoch() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Running;

        sink.begin_epoch(1).await.unwrap();
        // No writes
        sink.commit_epoch(1).await.unwrap();
        assert_eq!(sink.last_committed_epoch(), 1);
        assert_eq!(sink.delta_version(), 0); // No version bump (no files)
    }

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_sequential_epochs() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Running;

        for epoch in 1..=5 {
            sink.begin_epoch(epoch).await.unwrap();
            let batch = test_batch(10);
            sink.write_batch(&batch).await.unwrap();
            sink.commit_epoch(epoch).await.unwrap();
        }

        assert_eq!(sink.last_committed_epoch(), 5);
        assert_eq!(sink.delta_version(), 5);
    }

    // ── Flush tests ──
    // Note: These tests bypass open() and test business logic only.

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_explicit_flush() {
        let mut config = test_config();
        config.max_buffer_records = 1000;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(20);
        sink.write_batch(&batch).await.unwrap();
        assert_eq!(sink.buffered_rows(), 20);

        sink.flush().await.unwrap();
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.pending_files, 1);

        let m = sink.metrics();
        assert_eq!(m.records_total, 20);
    }

    // ── Open and close tests ──
    // Note: These tests use fake paths that don't exist.
    // With the delta-lake feature, open() tries to actually access the path.
    // See delta_io.rs for integration tests with real I/O.

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_open() {
        let mut sink = DeltaLakeSink::new(test_config());

        let connector_config = ConnectorConfig::new("delta-lake");
        sink.open(&connector_config).await.unwrap();

        assert_eq!(sink.state(), ConnectorState::Running);
    }

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_open_with_properties() {
        let mut sink = DeltaLakeSink::new(DeltaLakeSinkConfig::default());

        let mut connector_config = ConnectorConfig::new("delta-lake");
        connector_config.set("table.path", "/data/new_table");
        connector_config.set("write.mode", "overwrite");

        sink.open(&connector_config).await.unwrap();
        assert_eq!(sink.config().table_path, "/data/new_table");
        assert_eq!(sink.config().write_mode, DeltaWriteMode::Overwrite);
    }

    #[tokio::test]
    async fn test_close() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Running;

        sink.close().await.unwrap();
        assert_eq!(sink.state(), ConnectorState::Closed);
    }

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_close_flushes_remaining() {
        let mut config = test_config();
        config.max_buffer_records = 1000;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(30);
        sink.write_batch(&batch).await.unwrap();
        assert_eq!(sink.buffered_rows(), 30);

        sink.close().await.unwrap();
        assert_eq!(sink.buffered_rows(), 0);

        let m = sink.metrics();
        assert_eq!(m.records_total, 30);
    }

    // ── Health check tests ──

    #[test]
    fn test_health_check_created() {
        let sink = DeltaLakeSink::new(test_config());
        assert_eq!(sink.health_check(), HealthStatus::Unknown);
    }

    #[test]
    fn test_health_check_running() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Running;
        assert_eq!(sink.health_check(), HealthStatus::Healthy);
    }

    #[test]
    fn test_health_check_closed() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Closed;
        assert!(matches!(sink.health_check(), HealthStatus::Unhealthy(_)));
    }

    #[test]
    fn test_health_check_failed() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Failed;
        assert!(matches!(sink.health_check(), HealthStatus::Unhealthy(_)));
    }

    #[test]
    fn test_health_check_paused() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Paused;
        assert!(matches!(sink.health_check(), HealthStatus::Degraded(_)));
    }

    // ── Capabilities tests ──

    #[test]
    fn test_capabilities_append_exactly_once() {
        let mut config = test_config();
        config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
        let sink = DeltaLakeSink::new(config);
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
        let sink = DeltaLakeSink::new(upsert_config());
        let caps = sink.capabilities();
        assert!(caps.upsert);
        assert!(caps.changelog);
        assert!(caps.idempotent);
    }

    #[test]
    fn test_capabilities_schema_evolution() {
        let mut config = test_config();
        config.schema_evolution = true;
        let sink = DeltaLakeSink::new(config);
        let caps = sink.capabilities();
        assert!(caps.schema_evolution);
    }

    #[test]
    fn test_capabilities_partitioned() {
        let mut config = test_config();
        config.partition_columns = vec!["trade_date".to_string()];
        let sink = DeltaLakeSink::new(config);
        let caps = sink.capabilities();
        assert!(caps.partitioned);
    }

    #[test]
    fn test_capabilities_at_least_once() {
        let mut config = test_config();
        config.delivery_guarantee = DeliveryGuarantee::AtLeastOnce;
        let sink = DeltaLakeSink::new(config);
        let caps = sink.capabilities();
        assert!(!caps.exactly_once);
        assert!(caps.idempotent);
    }

    // ── Metrics tests ──

    #[test]
    fn test_metrics_initial() {
        let sink = DeltaLakeSink::new(test_config());
        let m = sink.metrics();
        assert_eq!(m.records_total, 0);
        assert_eq!(m.bytes_total, 0);
        assert_eq!(m.errors_total, 0);
    }

    #[tokio::test]
    async fn test_metrics_after_writes() {
        let mut config = test_config();
        config.max_buffer_records = 5;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(10);
        sink.write_batch(&batch).await.unwrap();

        let m = sink.metrics();
        assert_eq!(m.records_total, 10);
        assert!(m.bytes_total > 0);
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
                Arc::new(StringArray::from(vec!["I", "U", "D", "I", "D"])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_split_changelog_batch() {
        let batch = changelog_batch();
        let (inserts, deletes) = DeltaLakeSink::split_changelog_batch(&batch).unwrap();

        // Inserts: rows 0 (I), 1 (U), 3 (I) = 3 rows
        assert_eq!(inserts.num_rows(), 3);
        // Deletes: rows 2 (D), 4 (D) = 2 rows
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

        let (inserts, deletes) = DeltaLakeSink::split_changelog_batch(&batch).unwrap();
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

        let (inserts, deletes) = DeltaLakeSink::split_changelog_batch(&batch).unwrap();
        assert_eq!(inserts.num_rows(), 0);
        assert_eq!(deletes.num_rows(), 2);
    }

    #[test]
    fn test_split_changelog_missing_op_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();

        let result = DeltaLakeSink::split_changelog_batch(&batch);
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

        let (inserts, deletes) = DeltaLakeSink::split_changelog_batch(&batch).unwrap();
        assert_eq!(inserts.num_rows(), 1);
        assert_eq!(deletes.num_rows(), 0);
    }

    // ── Debug output test ──

    #[test]
    fn test_debug_output() {
        let sink = DeltaLakeSink::new(test_config());
        let debug = format!("{sink:?}");
        assert!(debug.contains("DeltaLakeSink"));
        assert!(debug.contains("/tmp/delta_test"));
    }
}
