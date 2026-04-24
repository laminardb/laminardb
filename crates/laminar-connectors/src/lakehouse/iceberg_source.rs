//! Apache Iceberg source connector implementation.
//!
//! [`IcebergSource`] implements [`SourceConnector`] for polling Iceberg
//! snapshots. It is a **reference/lookup table** source — Iceberg tables
//! are snapshot-based with no push mechanism.
//!
//! On each poll cycle the source checks for a newer snapshot. If one exists,
//! only the newly added data files are read via manifest-level diff
//! (see `iceberg_incremental::scan_incremental`). The first read is a full scan.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tracing::debug;
#[cfg(feature = "iceberg")]
use tracing::info;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{SourceBatch, SourceConnector};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::iceberg_config::IcebergSourceConfig;

/// Apache Iceberg source connector.
///
/// Polls for new snapshots on a configurable interval and emits
/// `RecordBatch` data. Supports pinning to a specific snapshot.
#[allow(dead_code)] // Fields used by feature-gated I/O methods.
pub struct IcebergSource {
    /// Source configuration — reparsed from `ConnectorConfig` in `open()`.
    config: IcebergSourceConfig,
    /// Discovered Arrow schema.
    schema: Option<SchemaRef>,
    /// Connector lifecycle state.
    state: ConnectorState,
    /// Buffered batches from the most recent scan.
    buffer: VecDeque<RecordBatch>,
    /// Last fully-ingested snapshot ID.
    last_snapshot_id: Option<i64>,
    /// Time of last snapshot poll.
    last_poll_time: Option<Instant>,
    /// Current epoch for checkpoint.
    epoch: u64,
    /// Cached catalog connection.
    #[cfg(feature = "iceberg")]
    catalog: Option<Arc<dyn iceberg::Catalog>>,
    /// Cached table handle.
    #[cfg(feature = "iceberg")]
    table: Option<iceberg::table::Table>,
}

impl IcebergSource {
    /// Creates a new Iceberg source with the given configuration.
    #[must_use]
    pub fn new(config: IcebergSourceConfig, _registry: Option<&prometheus::Registry>) -> Self {
        Self {
            config,
            schema: None,
            state: ConnectorState::Created,
            buffer: VecDeque::new(),
            last_snapshot_id: None,
            last_poll_time: None,
            epoch: 0,
            #[cfg(feature = "iceberg")]
            catalog: None,
            #[cfg(feature = "iceberg")]
            table: None,
        }
    }

    /// Checks for a new snapshot and loads data if available.
    #[cfg(feature = "iceberg")]
    async fn refresh(&mut self) -> Result<(), ConnectorError> {
        if let Some(last) = self.last_poll_time {
            if last.elapsed() < self.config.poll_interval {
                return Ok(());
            }
        }
        self.last_poll_time = Some(Instant::now());

        // Reload table metadata to see new snapshots.
        let catalog = self
            .catalog
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open".into(),
                actual: "catalog not initialized".into(),
            })?;
        let table = super::iceberg_io::load_table(
            catalog.as_ref(),
            &self.config.catalog.namespace,
            &self.config.catalog.table_name,
        )
        .await?;
        self.table = Some(table);

        let table = self.table.as_ref().unwrap();
        let current_snap = super::iceberg_io::current_snapshot_id(table);

        // If pinned to a specific snapshot, only load once.
        if let Some(pinned) = self.config.snapshot_id {
            if self.last_snapshot_id.is_some() {
                return Ok(());
            }
            let batches =
                super::iceberg_io::scan_table(table, Some(pinned), &self.config.select_columns)
                    .await?;
            self.buffer.extend(batches);
            self.last_snapshot_id = Some(pinned);
            return Ok(());
        }

        if current_snap == self.last_snapshot_id {
            return Ok(());
        }

        // Incremental read if we have a previous snapshot, full scan otherwise.
        let batches = if let (Some(old), Some(new)) = (self.last_snapshot_id, current_snap) {
            super::iceberg_incremental::scan_incremental(
                table,
                old,
                new,
                &self.config.select_columns,
            )
            .await?
        } else {
            super::iceberg_io::scan_table(table, current_snap, &self.config.select_columns).await?
        };

        if self.schema.is_none() {
            if let Some(first) = batches.first() {
                self.schema = Some(first.schema());
            }
        }

        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        debug!(snapshot = ?current_snap, rows, "iceberg source loaded snapshot");

        self.buffer.extend(batches);
        self.last_snapshot_id = current_snap;

        Ok(())
    }
}

#[async_trait]
impl SourceConnector for IcebergSource {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        // Re-parse config from runtime ConnectorConfig (not factory defaults).
        if !config.properties().is_empty() {
            self.config = IcebergSourceConfig::from_config(config)?;
        }

        #[cfg(feature = "iceberg")]
        {
            let catalog = super::iceberg_io::build_catalog(&self.config.catalog).await?;
            let table = super::iceberg_io::load_table(
                catalog.as_ref(),
                &self.config.catalog.namespace,
                &self.config.catalog.table_name,
            )
            .await?;

            let iceberg_schema = table.current_schema_ref();
            let arrow_schema =
                iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).map_err(|e| {
                    ConnectorError::SchemaMismatch(format!("iceberg→arrow schema: {e}"))
                })?;
            self.schema = Some(Arc::new(arrow_schema));

            info!(
                table = self.config.catalog.table_name,
                namespace = self.config.catalog.namespace,
                "iceberg source connected"
            );

            self.catalog = Some(catalog);
            self.table = Some(table);
            self.state = ConnectorState::Running;
            return Ok(());
        }

        #[cfg(not(feature = "iceberg"))]
        {
            self.state = ConnectorState::Failed;
            Err(ConnectorError::ConfigurationError(
                "Apache Iceberg requires the 'iceberg' feature".into(),
            ))
        }
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if let Some(batch) = self.buffer.pop_front() {
            if batch.num_rows() <= max_records {
                return Ok(Some(SourceBatch::new(batch)));
            }
            let take = batch.slice(0, max_records);
            let remainder = batch.slice(max_records, batch.num_rows() - max_records);
            self.buffer.push_front(remainder);
            return Ok(Some(SourceBatch::new(take)));
        }

        #[cfg(feature = "iceberg")]
        self.refresh().await?;

        Ok(self.buffer.pop_front().map(SourceBatch::new))
    }

    fn schema(&self) -> SchemaRef {
        self.schema
            .clone()
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()))
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new(self.epoch);
        if let Some(sid) = self.last_snapshot_id {
            cp.set_offset("snapshot_id", sid.to_string());
        }
        cp.set_metadata("connector_type", "iceberg");
        cp
    }

    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        self.epoch = checkpoint.epoch();
        if let Some(sid) = checkpoint.get_offset("snapshot_id") {
            self.last_snapshot_id = Some(sid.parse().map_err(|_| {
                ConnectorError::Internal(format!("invalid snapshot_id in checkpoint: '{sid}'"))
            })?);
            debug!(snapshot_id = ?self.last_snapshot_id, "iceberg source restored");
        }
        Ok(())
    }

    fn supports_replay(&self) -> bool {
        false
    }

    fn health_check(&self) -> HealthStatus {
        match self.state {
            ConnectorState::Running => HealthStatus::Healthy,
            ConnectorState::Failed => HealthStatus::Unhealthy("source failed".into()),
            _ => HealthStatus::Unknown,
        }
    }

    fn metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics::default()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        #[cfg(feature = "iceberg")]
        {
            self.catalog = None;
            self.table = None;
        }
        self.state = ConnectorState::Closed;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConnectorConfig;

    fn test_source_config() -> IcebergSourceConfig {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", "http://localhost:8181");
        config.set("warehouse", "s3://test/wh");
        config.set("namespace", "test");
        config.set("table.name", "dim_customers");
        IcebergSourceConfig::from_config(&config).unwrap()
    }

    #[test]
    fn test_new_source() {
        let source = IcebergSource::new(test_source_config(), None);
        assert!(source.schema.is_none());
        assert!(source.last_snapshot_id.is_none());
        assert!(source.buffer.is_empty());
    }

    #[test]
    fn test_checkpoint_round_trip() {
        let mut source = IcebergSource::new(test_source_config(), None);
        source.last_snapshot_id = Some(42);
        source.epoch = 5;

        let cp = source.checkpoint();
        assert_eq!(cp.epoch(), 5);
        assert_eq!(cp.get_offset("snapshot_id"), Some("42"));
        assert_eq!(cp.get_metadata("connector_type"), Some("iceberg"));
    }

    #[tokio::test]
    async fn test_restore_from_checkpoint() {
        let mut source = IcebergSource::new(test_source_config(), None);
        let mut cp = SourceCheckpoint::new(10);
        cp.set_offset("snapshot_id", "123");

        source.restore(&cp).await.unwrap();
        assert_eq!(source.epoch, 10);
        assert_eq!(source.last_snapshot_id, Some(123));
    }

    #[test]
    fn test_supports_replay_false() {
        let source = IcebergSource::new(test_source_config(), None);
        assert!(!source.supports_replay());
    }
}
