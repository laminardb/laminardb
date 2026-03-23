//! Iceberg reference table source for lookup/enrichment joins.
//!
//! Implements [`ReferenceTableSource`] to populate a dimension table from
//! an Iceberg table. Delivers snapshot data as batches, then polls for
//! new snapshots to deliver incremental changes via manifest diff.

use std::collections::VecDeque;
#[cfg(feature = "iceberg")]
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use async_trait::async_trait;
#[cfg(feature = "iceberg")]
use tracing::{debug, info};

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::reference::ReferenceTableSource;

use super::iceberg_config::IcebergSourceConfig;

/// Iceberg reference table source for lookup/enrichment joins.
#[allow(dead_code)] // Fields used by feature-gated I/O methods.
pub struct IcebergReferenceTableSource {
    /// Source configuration.
    config: IcebergSourceConfig,
    /// Buffered snapshot batches (drained via `poll_snapshot`).
    snapshot_batches: VecDeque<RecordBatch>,
    /// Whether the initial snapshot has been fully delivered.
    snapshot_complete: bool,
    /// Buffered change batches (from newer snapshots).
    change_batches: VecDeque<RecordBatch>,
    /// Snapshot ID that has been loaded (may still have undelivered batches).
    loaded_snapshot_id: Option<i64>,
    /// Snapshot ID whose batches have been fully delivered — safe to checkpoint.
    delivered_snapshot_id: Option<i64>,
    /// Time of last change poll.
    last_poll_time: Option<Instant>,
    /// Current epoch.
    epoch: u64,
    /// Cached catalog connection.
    #[cfg(feature = "iceberg")]
    catalog: Option<Arc<dyn iceberg::Catalog>>,
}

impl IcebergReferenceTableSource {
    /// Creates a new reference table source.
    #[must_use]
    pub fn new(config: IcebergSourceConfig) -> Self {
        Self {
            config,
            snapshot_batches: VecDeque::new(),
            snapshot_complete: false,
            change_batches: VecDeque::new(),
            loaded_snapshot_id: None,
            delivered_snapshot_id: None,
            last_poll_time: None,
            epoch: 0,
            #[cfg(feature = "iceberg")]
            catalog: None,
        }
    }

    /// Creates from a `ConnectorConfig` (SQL WITH clause).
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` on invalid configuration.
    pub fn from_connector_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let cfg = IcebergSourceConfig::from_config(config)?;
        Ok(Self::new(cfg))
    }

    /// Loads the initial snapshot from the Iceberg table.
    #[cfg(feature = "iceberg")]
    async fn load_initial_snapshot(&mut self) -> Result<(), ConnectorError> {
        let catalog = super::iceberg_io::build_catalog(&self.config.catalog).await?;
        let table = super::iceberg_io::load_table(
            catalog.as_ref(),
            &self.config.catalog.namespace,
            &self.config.catalog.table_name,
        )
        .await?;

        let snap_id = self
            .config
            .snapshot_id
            .or_else(|| super::iceberg_io::current_snapshot_id(&table));

        let batches =
            super::iceberg_io::scan_table(&table, snap_id, &self.config.select_columns).await?;

        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        info!(snapshot = ?snap_id, rows, "iceberg reference: initial snapshot loaded");

        self.snapshot_batches.extend(batches);
        self.loaded_snapshot_id = snap_id;
        self.catalog = Some(catalog);

        Ok(())
    }

    /// Checks for newer snapshots and loads change data.
    #[cfg(feature = "iceberg")]
    async fn poll_for_changes(&mut self) -> Result<(), ConnectorError> {
        if let Some(last) = self.last_poll_time {
            if last.elapsed() < self.config.poll_interval {
                return Ok(());
            }
        }
        self.last_poll_time = Some(Instant::now());

        let catalog = self
            .catalog
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "snapshot loaded".into(),
                actual: "catalog not initialized".into(),
            })?;

        let table = super::iceberg_io::load_table(
            catalog.as_ref(),
            &self.config.catalog.namespace,
            &self.config.catalog.table_name,
        )
        .await?;

        let current_snap = super::iceberg_io::current_snapshot_id(&table);

        if current_snap == self.loaded_snapshot_id {
            return Ok(());
        }

        // Incremental read if we have a previous snapshot, full scan otherwise.
        let batches = if let (Some(old), Some(new)) = (self.loaded_snapshot_id, current_snap) {
            super::iceberg_incremental::scan_incremental(
                &table,
                old,
                new,
                &self.config.select_columns,
            )
            .await?
        } else {
            super::iceberg_io::scan_table(&table, current_snap, &self.config.select_columns).await?
        };

        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        debug!(
            old_snapshot = ?self.loaded_snapshot_id,
            new_snapshot = ?current_snap,
            rows,
            "iceberg reference: change snapshot loaded"
        );

        self.change_batches.extend(batches);
        self.loaded_snapshot_id = current_snap;

        Ok(())
    }
}

#[async_trait]
impl ReferenceTableSource for IcebergReferenceTableSource {
    async fn poll_snapshot(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if !self.snapshot_complete
            && self.snapshot_batches.is_empty()
            && self.loaded_snapshot_id.is_none()
        {
            #[cfg(feature = "iceberg")]
            self.load_initial_snapshot().await?;
        }

        if let Some(batch) = self.snapshot_batches.pop_front() {
            return Ok(Some(batch));
        }

        // All snapshot batches delivered — safe to checkpoint this snapshot.
        self.delivered_snapshot_id = self.loaded_snapshot_id;
        self.snapshot_complete = true;
        Ok(None)
    }

    fn is_snapshot_complete(&self) -> bool {
        self.snapshot_complete
    }

    async fn poll_changes(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if let Some(batch) = self.change_batches.pop_front() {
            return Ok(Some(batch));
        }

        // All change batches from the previous snapshot have been delivered.
        self.delivered_snapshot_id = self.loaded_snapshot_id;

        #[cfg(feature = "iceberg")]
        self.poll_for_changes().await?;

        Ok(self.change_batches.pop_front())
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new(self.epoch);
        if let Some(sid) = self.delivered_snapshot_id {
            cp.set_offset("snapshot_id", sid.to_string());
        }
        cp.set_metadata("connector_type", "iceberg-reference");
        cp
    }

    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        self.epoch = checkpoint.epoch();
        if let Some(sid) = checkpoint.get_offset("snapshot_id") {
            let snap: i64 = sid.parse().map_err(|_| {
                ConnectorError::CheckpointError(format!(
                    "invalid snapshot_id in checkpoint: '{sid}'"
                ))
            })?;
            // Both cursors restored to the last fully-delivered snapshot.
            self.delivered_snapshot_id = Some(snap);
            self.loaded_snapshot_id = Some(snap);
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        #[cfg(feature = "iceberg")]
        {
            self.catalog = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_source_config() -> IcebergSourceConfig {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", "http://localhost:8181");
        config.set("warehouse", "s3://test/wh");
        config.set("namespace", "test");
        config.set("table.name", "dim_customers");
        IcebergSourceConfig::from_config(&config).unwrap()
    }

    #[test]
    fn test_reference_source_new() {
        let source = IcebergReferenceTableSource::new(test_source_config());
        assert!(!source.snapshot_complete);
        assert!(source.snapshot_batches.is_empty());
        assert!(source.loaded_snapshot_id.is_none());
    }

    #[tokio::test]
    async fn test_reference_checkpoint_round_trip() {
        let mut source = IcebergReferenceTableSource::new(test_source_config());
        // Simulate fully-delivered snapshot.
        source.delivered_snapshot_id = Some(99);
        source.loaded_snapshot_id = Some(99);
        source.epoch = 7;

        let cp = source.checkpoint();
        assert_eq!(cp.get_offset("snapshot_id"), Some("99"));
        assert_eq!(cp.get_metadata("connector_type"), Some("iceberg-reference"));

        let mut restored = IcebergReferenceTableSource::new(test_source_config());
        restored.restore(&cp).await.unwrap();
        assert_eq!(restored.delivered_snapshot_id, Some(99));
        assert_eq!(restored.loaded_snapshot_id, Some(99));
        assert_eq!(restored.epoch, 7);
    }

    #[tokio::test]
    async fn test_checkpoint_only_after_delivery() {
        let mut source = IcebergReferenceTableSource::new(test_source_config());
        // Loaded but not yet delivered — checkpoint should NOT include snapshot_id.
        source.loaded_snapshot_id = Some(42);
        let cp = source.checkpoint();
        assert_eq!(cp.get_offset("snapshot_id"), None);
    }

    #[tokio::test]
    async fn test_reference_empty_snapshot_completes() {
        let mut source = IcebergReferenceTableSource::new(test_source_config());
        source.loaded_snapshot_id = Some(1);
        let result = source.poll_snapshot().await.unwrap();
        assert!(result.is_none());
        assert!(source.is_snapshot_complete());
    }

    #[test]
    fn test_from_connector_config() {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", "http://localhost:8181");
        config.set("warehouse", "s3://test/wh");
        config.set("namespace", "test");
        config.set("table.name", "dim_customers");

        let source = IcebergReferenceTableSource::from_connector_config(&config);
        assert!(source.is_ok());
    }
}
