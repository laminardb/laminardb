//! Delta Lake source connector implementation.
//!
//! [`DeltaSource`] implements [`SourceConnector`], reading Arrow `RecordBatch`
//! data from Delta Lake tables by polling for new versions.
//!
//! # Polling Strategy
//!
//! The source maintains a `current_version` cursor. On each `poll_batch()`:
//! 1. Check if the table has a newer version than `current_version`
//! 2. If yes, load the new version and scan for record batches
//! 3. Buffer the results and return them incrementally
//! 4. If no new data, return `None`
//!
//! # Checkpoint / Recovery
//!
//! The checkpoint stores `current_version` so that on recovery the source
//! resumes from the correct Delta Lake version.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
#[cfg(feature = "delta-lake")]
use tracing::debug;
use tracing::info;

#[cfg(feature = "delta-lake")]
use deltalake::DeltaTable;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{SourceBatch, SourceConnector};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::delta_source_config::DeltaSourceConfig;

/// Delta Lake source connector.
///
/// Reads Arrow `RecordBatch` data from Delta Lake tables by polling for
/// new table versions.
///
/// # Lifecycle
///
/// ```text
/// new() -> open() -> [poll_batch()]* -> close()
///                          |
///                 checkpoint() / restore()
/// ```
pub struct DeltaSource {
    /// Source configuration.
    config: DeltaSourceConfig,
    /// Connector lifecycle state.
    state: ConnectorState,
    /// Arrow schema (set from table metadata on open).
    schema: Option<SchemaRef>,
    /// Current Delta Lake version cursor.
    current_version: i64,
    /// Buffered batches from the last version load.
    pending_batches: VecDeque<RecordBatch>,
    /// Total records read so far.
    records_read: u64,
    /// Delta Lake table handle.
    #[cfg(feature = "delta-lake")]
    table: Option<DeltaTable>,
}

impl DeltaSource {
    /// Creates a new Delta Lake source with the given configuration.
    #[must_use]
    pub fn new(config: DeltaSourceConfig) -> Self {
        Self {
            config,
            state: ConnectorState::Created,
            schema: None,
            current_version: -1,
            pending_batches: VecDeque::new(),
            records_read: 0,
            #[cfg(feature = "delta-lake")]
            table: None,
        }
    }

    /// Returns the current connector state.
    #[must_use]
    pub fn state(&self) -> ConnectorState {
        self.state
    }

    /// Returns the current Delta Lake version cursor.
    #[must_use]
    pub fn current_version(&self) -> i64 {
        self.current_version
    }

    /// Returns the source configuration.
    #[must_use]
    pub fn config(&self) -> &DeltaSourceConfig {
        &self.config
    }
}

#[async_trait]
impl SourceConnector for DeltaSource {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Initializing;

        // Re-parse config if properties provided.
        if !config.properties().is_empty() {
            self.config = DeltaSourceConfig::from_config(config)?;
        }

        info!(
            table_path = %self.config.table_path,
            starting_version = ?self.config.starting_version,
            "opening Delta Lake source connector"
        );

        #[cfg(feature = "delta-lake")]
        {
            use super::delta_io;

            // Open the existing table (source requires the table to exist).
            let table = delta_io::open_or_create_table(
                &self.config.table_path,
                self.config.storage_options.clone(),
                None,
            )
            .await?;

            // Read schema from table.
            if let Ok(schema) = delta_io::get_table_schema(&table) {
                self.schema = Some(schema);
            }

            // Set starting version.
            let table_version = table.version().unwrap_or(0);
            #[allow(clippy::cast_possible_wrap)]
            if let Some(start) = self.config.starting_version {
                self.current_version = start;
            } else {
                self.current_version = table_version;
            }

            info!(
                table_path = %self.config.table_path,
                table_version,
                current_version = self.current_version,
                "Delta Lake source: resolved starting version"
            );

            self.table = Some(table);
        }

        #[cfg(not(feature = "delta-lake"))]
        {
            // Without the delta-lake feature, we can still set up in Created state
            // for testing business logic.
            if let Some(start) = self.config.starting_version {
                self.current_version = start;
            }
        }

        self.state = ConnectorState::Running;
        info!("Delta Lake source connector opened successfully");
        Ok(())
    }

    #[allow(unused_variables)]
    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: "Running".into(),
                actual: self.state.to_string(),
            });
        }

        // Return buffered batches first.
        if let Some(batch) = self.pending_batches.pop_front() {
            self.records_read += batch.num_rows() as u64;
            return Ok(Some(SourceBatch::new(batch)));
        }

        // Check for new versions.
        #[cfg(feature = "delta-lake")]
        {
            use super::delta_io;

            let table = self
                .table
                .as_mut()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "table initialized".into(),
                    actual: "table not initialized".into(),
                })?;

            let latest_version = delta_io::get_latest_version(table).await?;

            if latest_version <= self.current_version {
                return Ok(None); // No new data
            }

            debug!(
                current_version = self.current_version,
                latest_version, "Delta Lake source: new version(s) available"
            );

            // Load the next version and scan batches.
            let next_version = self.current_version + 1;
            let batches =
                delta_io::read_batches_at_version(table, next_version, max_records).await?;

            // Update schema if needed.
            if self.schema.is_none() {
                if let Some(first) = batches.first() {
                    self.schema = Some(first.schema());
                }
            }

            self.current_version = next_version;

            // Buffer all batches, return the first one.
            for batch in batches {
                if batch.num_rows() > 0 {
                    self.pending_batches.push_back(batch);
                }
            }

            if let Some(batch) = self.pending_batches.pop_front() {
                self.records_read += batch.num_rows() as u64;
                return Ok(Some(SourceBatch::new(batch)));
            }
        }

        Ok(None)
    }

    fn schema(&self) -> SchemaRef {
        self.schema
            .clone()
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()))
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new(0);
        cp.set_offset("delta_version", self.current_version.to_string());
        cp
    }

    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        if let Some(version_str) = checkpoint.get_offset("delta_version") {
            self.current_version = version_str.parse::<i64>().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid delta_version in checkpoint: '{version_str}'"
                ))
            })?;
            info!(
                restored_version = self.current_version,
                "Delta Lake source: restored from checkpoint"
            );
        }
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
        ConnectorMetrics {
            records_total: self.records_read,
            ..ConnectorMetrics::default()
        }
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing Delta Lake source connector");

        #[cfg(feature = "delta-lake")]
        {
            self.table = None;
        }

        self.pending_batches.clear();
        self.state = ConnectorState::Closed;

        info!(
            table_path = %self.config.table_path,
            current_version = self.current_version,
            records_read = self.records_read,
            "Delta Lake source connector closed"
        );

        Ok(())
    }
}

impl std::fmt::Debug for DeltaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaSource")
            .field("state", &self.state)
            .field("table_path", &self.config.table_path)
            .field("current_version", &self.current_version)
            .field("pending_batches", &self.pending_batches.len())
            .field("records_read", &self.records_read)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn test_config() -> DeltaSourceConfig {
        DeltaSourceConfig::new("/tmp/delta_source_test")
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    #[allow(clippy::cast_precision_loss)]
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

    #[test]
    fn test_new_defaults() {
        let source = DeltaSource::new(test_config());
        assert_eq!(source.state(), ConnectorState::Created);
        assert_eq!(source.current_version(), -1);
        assert!(source.schema.is_none());
    }

    #[test]
    fn test_checkpoint_roundtrip() {
        let mut source = DeltaSource::new(test_config());
        source.current_version = 42;

        let cp = source.checkpoint();
        assert_eq!(cp.get_offset("delta_version"), Some("42"));
    }

    #[tokio::test]
    async fn test_restore_from_checkpoint() {
        let mut source = DeltaSource::new(test_config());
        assert_eq!(source.current_version(), -1);

        let mut cp = SourceCheckpoint::new(0);
        cp.set_offset("delta_version", "10");
        source.restore(&cp).await.unwrap();

        assert_eq!(source.current_version(), 10);
    }

    #[test]
    fn test_health_check() {
        let mut source = DeltaSource::new(test_config());
        assert_eq!(source.health_check(), HealthStatus::Unknown);

        source.state = ConnectorState::Running;
        assert_eq!(source.health_check(), HealthStatus::Healthy);

        source.state = ConnectorState::Closed;
        assert!(matches!(source.health_check(), HealthStatus::Unhealthy(_)));
    }

    #[test]
    fn test_schema_empty_when_none() {
        let source = DeltaSource::new(test_config());
        let schema = source.schema();
        assert_eq!(schema.fields().len(), 0);
    }

    #[tokio::test]
    async fn test_poll_not_running() {
        let mut source = DeltaSource::new(test_config());
        // state is Created, not Running
        let result = source.poll_batch(100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_poll_returns_buffered_batches() {
        let mut source = DeltaSource::new(test_config());
        source.state = ConnectorState::Running;

        // Manually buffer some batches.
        source.pending_batches.push_back(test_batch(5));
        source.pending_batches.push_back(test_batch(3));

        let batch1 = source.poll_batch(100).await.unwrap();
        assert!(batch1.is_some());
        assert_eq!(batch1.unwrap().records.num_rows(), 5);

        let batch2 = source.poll_batch(100).await.unwrap();
        assert!(batch2.is_some());
        assert_eq!(batch2.unwrap().records.num_rows(), 3);

        assert_eq!(source.records_read, 8);
    }

    #[test]
    fn test_debug_output() {
        let source = DeltaSource::new(test_config());
        let debug = format!("{source:?}");
        assert!(debug.contains("DeltaSource"));
        assert!(debug.contains("/tmp/delta_source_test"));
    }

    #[tokio::test]
    async fn test_close() {
        let mut source = DeltaSource::new(test_config());
        source.state = ConnectorState::Running;
        source.pending_batches.push_back(test_batch(5));

        source.close().await.unwrap();
        assert_eq!(source.state(), ConnectorState::Closed);
        assert!(source.pending_batches.is_empty());
    }
}
