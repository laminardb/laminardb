//! Delta Lake reference table source for lookup/enrichment joins.
//!
//! Implements [`ReferenceTableSource`] to populate lookup tables from Delta
//! Lake tables. Supports catalog resolution (Unity, Glue) via the existing
//! [`delta_io::resolve_catalog_options`] function.

use std::collections::VecDeque;

use arrow_array::RecordBatch;
use tracing::info;

#[cfg(feature = "delta-lake")]
use std::time::Instant;

#[cfg(feature = "delta-lake")]
use tracing::{debug, warn};

#[cfg(feature = "delta-lake")]
use deltalake::DeltaTable;

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::lakehouse::delta_source_config::DeltaSourceConfig;
#[cfg(feature = "delta-lake")]
use crate::lakehouse::delta_source_config::SchemaEvolutionAction;
use crate::reference::ReferenceTableSource;

/// Lifecycle phase.
#[allow(dead_code)] // Variants used only with delta-lake feature
enum Phase {
    Init,
    Snapshot,
    Changes,
    Closed,
}

/// Delta Lake reference table source for `CREATE LOOKUP TABLE`.
///
/// Reads a Delta Lake table as a dimension/reference table for enrichment
/// joins. The table is loaded as a full snapshot on first access, then
/// incrementally refreshed as new Delta versions appear.
pub struct DeltaReferenceTableSource {
    #[allow(dead_code)] // read in feature-gated methods
    config: DeltaSourceConfig,
    phase: Phase,
    #[cfg(feature = "delta-lake")]
    table: Option<DeltaTable>,
    current_version: i64,
    pending_batches: VecDeque<RecordBatch>,
    #[cfg(feature = "delta-lake")]
    last_version_check: Option<Instant>,
    #[cfg(feature = "delta-lake")]
    known_schema: Option<arrow_schema::SchemaRef>,
}

impl DeltaReferenceTableSource {
    /// Creates a new Delta Lake reference table source from a pre-parsed config.
    #[must_use]
    pub fn from_source_config(config: DeltaSourceConfig) -> Self {
        Self {
            config,
            phase: Phase::Init,
            #[cfg(feature = "delta-lake")]
            table: None,
            current_version: -1,
            pending_batches: VecDeque::new(),
            #[cfg(feature = "delta-lake")]
            last_version_check: None,
            #[cfg(feature = "delta-lake")]
            known_schema: None,
        }
    }

    /// Creates a new source from a [`ConnectorConfig`] (SQL WITH clause).
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if required config keys are missing.
    pub fn from_connector_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let source_config = DeltaSourceConfig::from_config(config)?;
        Ok(Self::from_source_config(source_config))
    }

    #[cfg(feature = "delta-lake")]
    async fn open_table(&mut self) -> Result<(), ConnectorError> {
        use crate::lakehouse::delta_io;

        let (resolved_path, resolved_opts) = delta_io::resolve_catalog_options(
            &self.config.catalog_type,
            self.config.catalog_database.as_deref(),
            self.config.catalog_name.as_deref(),
            self.config.catalog_schema.as_deref(),
            &self.config.table_path,
            &self.config.storage_options,
        )
        .await?;

        info!(
            table_path = %self.config.table_path,
            resolved_path = %resolved_path,
            catalog = %self.config.catalog_type,
            "delta lookup: resolved catalog"
        );

        let table = delta_io::open_or_create_table(&resolved_path, resolved_opts, None).await?;

        info!(
            resolved_path = %resolved_path,
            table_version = table.version().unwrap_or(0),
            "delta lookup: table opened"
        );

        self.table = Some(table);
        Ok(())
    }

    /// Loads all batches at the latest version into `pending_batches`.
    /// When `partition_filter` is set, applies it as a WHERE clause.
    #[cfg(feature = "delta-lake")]
    async fn load_snapshot(&mut self) -> Result<(), ConnectorError> {
        use crate::lakehouse::delta_io;

        let table = self
            .table
            .as_mut()
            .ok_or_else(|| ConnectorError::Internal("table not opened".into()))?;

        let latest = delta_io::get_latest_version(table).await?;

        let batches = if self.config.partition_filter.is_some() {
            self.load_snapshot_filtered(latest).await?
        } else {
            let (b, _) = delta_io::read_batches_at_version(table, latest, usize::MAX).await?;
            b
        };

        if let Some(first) = batches.first() {
            self.known_schema = Some(first.schema());
        }

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        info!(
            version = latest,
            batches = batches.len(),
            rows = total_rows,
            partition_filter = ?self.config.partition_filter,
            "delta lookup: snapshot loaded"
        );

        self.pending_batches = VecDeque::from(batches);
        self.current_version = latest;
        self.last_version_check = Some(Instant::now());
        Ok(())
    }

    /// Loads snapshot with partition filter via `DataFusion` WHERE clause.
    #[cfg(feature = "delta-lake")]
    async fn load_snapshot_filtered(
        &mut self,
        version: i64,
    ) -> Result<Vec<RecordBatch>, ConnectorError> {
        use tokio_stream::StreamExt;

        let table = self
            .table
            .as_mut()
            .ok_or_else(|| ConnectorError::Internal("table not opened".into()))?;

        table
            .load_version(version)
            .await
            .map_err(|e| ConnectorError::ReadError(format!("load version {version}: {e}")))?;

        let provider = table
            .table_provider()
            .build()
            .await
            .map_err(|e| ConnectorError::ReadError(format!("build table provider: {e}")))?;

        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_table("delta_lookup_scan", std::sync::Arc::new(provider))
            .map_err(|e| ConnectorError::ReadError(format!("register scan: {e}")))?;

        // Caller guarantees partition_filter is Some.
        let filter = self.config.partition_filter.as_deref().unwrap_or("1=1");
        let sql = format!("SELECT * FROM delta_lookup_scan WHERE {filter}");

        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| ConnectorError::ReadError(format!("filtered scan: {e}")))?;

        let mut stream = df
            .execute_stream()
            .await
            .map_err(|e| ConnectorError::ReadError(format!("stream: {e}")))?;

        let mut batches = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.map_err(|e| ConnectorError::ReadError(format!("batch: {e}")))?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }

        Ok(batches)
    }

    /// Checks for new Delta versions and loads diffs, capped at
    /// `MAX_VERSIONS_PER_POLL` per call to avoid blocking the pipeline.
    #[cfg(feature = "delta-lake")]
    async fn check_for_changes(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        use crate::lakehouse::delta_io;

        /// Max versions to read per `poll_changes()` call.
        const MAX_VERSIONS_PER_POLL: i64 = 10;

        // Throttle version checks to poll_interval.
        if let Some(last) = self.last_version_check {
            if last.elapsed() < self.config.poll_interval {
                return Ok(None);
            }
        }

        let table = self
            .table
            .as_mut()
            .ok_or_else(|| ConnectorError::Internal("table not opened".into()))?;

        let latest = delta_io::get_latest_version(table).await?;

        if latest <= self.current_version {
            self.last_version_check = Some(Instant::now());
            return Ok(None);
        }

        let target = latest.min(self.current_version + MAX_VERSIONS_PER_POLL);

        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for v in (self.current_version + 1)..=target {
            let (batches, _) = delta_io::read_version_diff(
                table,
                v,
                usize::MAX,
                self.config.partition_filter.as_deref(),
            )
            .await?;

            if let (Some(known), Some(first)) = (&self.known_schema, batches.first()) {
                let new_schema = first.schema();
                if known.as_ref() != new_schema.as_ref() {
                    match self.config.schema_evolution_action {
                        SchemaEvolutionAction::Warn => {
                            warn!(version = v, "delta lookup: schema changed between versions");
                            self.known_schema = Some(new_schema);
                        }
                        SchemaEvolutionAction::Error => {
                            if v > self.current_version + 1 {
                                self.current_version = v - 1;
                            }
                            return Err(ConnectorError::Internal(format!(
                                "delta lookup: schema evolution detected at version {v} \
                                 (action=error)"
                            )));
                        }
                    }
                }
            }

            all_batches.extend(batches);
        }

        let total_rows: usize = all_batches.iter().map(RecordBatch::num_rows).sum();
        if total_rows > 0 {
            debug!(
                from = self.current_version + 1,
                to = target,
                latest,
                rows = total_rows,
                "delta lookup: version diff loaded"
            );
        }

        self.current_version = target;

        // If more versions remain, skip throttle so next poll re-enters immediately.
        if target < latest {
            self.last_version_check = None;
        } else {
            self.last_version_check = Some(Instant::now());
        }

        let mut batch_iter = all_batches.into_iter();
        let first = batch_iter.next();
        self.pending_batches.extend(batch_iter);
        Ok(first)
    }
}

#[async_trait::async_trait]
impl ReferenceTableSource for DeltaReferenceTableSource {
    async fn poll_snapshot(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if matches!(self.phase, Phase::Init) {
            #[cfg(feature = "delta-lake")]
            {
                self.open_table().await?;
                self.load_snapshot().await?;
                self.phase = Phase::Snapshot;
            }

            #[cfg(not(feature = "delta-lake"))]
            {
                return Err(ConnectorError::ConfigurationError(
                    "Delta Lake lookup requires the 'delta-lake' feature. \
                     Build with: cargo build --features delta-lake"
                        .into(),
                ));
            }
        }

        if !matches!(self.phase, Phase::Snapshot) {
            return Ok(None);
        }

        if let Some(batch) = self.pending_batches.pop_front() {
            return Ok(Some(batch));
        }

        self.phase = Phase::Changes;
        Ok(None)
    }

    fn is_snapshot_complete(&self) -> bool {
        matches!(self.phase, Phase::Changes | Phase::Closed)
    }

    async fn poll_changes(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if !matches!(self.phase, Phase::Changes) {
            return Ok(None);
        }

        if let Some(batch) = self.pending_batches.pop_front() {
            return Ok(Some(batch));
        }

        #[cfg(feature = "delta-lake")]
        {
            return self.check_for_changes().await;
        }

        #[cfg(not(feature = "delta-lake"))]
        Ok(None)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new(0);
        cp.set_offset("delta_version", self.current_version.to_string());
        cp
    }

    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        if let Some(v) = checkpoint.get_offset("delta_version") {
            self.current_version = v.parse().map_err(|_| {
                ConnectorError::CheckpointError(format!(
                    "invalid delta_version in checkpoint: '{v}'"
                ))
            })?;
            info!(
                version = self.current_version,
                "delta lookup: restored from checkpoint"
            );
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.phase = Phase::Closed;
        #[cfg(feature = "delta-lake")]
        {
            self.table = None;
        }
        self.pending_batches.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lakehouse::delta_source_config::DeltaSourceConfig;

    #[test]
    fn test_from_source_config() {
        let config = DeltaSourceConfig::new("/tmp/test_delta");
        let src = DeltaReferenceTableSource::from_source_config(config);
        assert!(!src.is_snapshot_complete());
        assert_eq!(src.current_version, -1);
    }

    #[test]
    fn test_from_connector_config() {
        let mut config = ConnectorConfig::new("delta-lake");
        config.set("table.path", "/tmp/test_delta");
        let src = DeltaReferenceTableSource::from_connector_config(&config).unwrap();
        assert!(!src.is_snapshot_complete());
    }

    #[test]
    fn test_from_connector_config_missing_path() {
        let config = ConnectorConfig::new("delta-lake");
        assert!(DeltaReferenceTableSource::from_connector_config(&config).is_err());
    }

    #[test]
    fn test_checkpoint_round_trip() {
        let config = DeltaSourceConfig::new("/tmp/test");
        let mut src = DeltaReferenceTableSource::from_source_config(config);
        src.current_version = 42;
        let cp = src.checkpoint();
        assert_eq!(cp.get_offset("delta_version"), Some("42"));
    }

    #[tokio::test]
    async fn test_restore_from_checkpoint() {
        let config = DeltaSourceConfig::new("/tmp/test");
        let mut src = DeltaReferenceTableSource::from_source_config(config);
        let mut cp = SourceCheckpoint::new(0);
        cp.set_offset("delta_version", "17");
        src.restore(&cp).await.unwrap();
        assert_eq!(src.current_version, 17);
    }

    #[tokio::test]
    async fn test_restore_invalid_version() {
        let config = DeltaSourceConfig::new("/tmp/test");
        let mut src = DeltaReferenceTableSource::from_source_config(config);
        let mut cp = SourceCheckpoint::new(0);
        cp.set_offset("delta_version", "not_a_number");
        assert!(src.restore(&cp).await.is_err());
    }

    #[tokio::test]
    async fn test_close_sets_phase() {
        let config = DeltaSourceConfig::new("/tmp/test");
        let mut src = DeltaReferenceTableSource::from_source_config(config);
        src.close().await.unwrap();
        assert!(src.is_snapshot_complete());
    }

    #[tokio::test]
    async fn test_poll_changes_before_snapshot_returns_none() {
        let config = DeltaSourceConfig::new("/tmp/test");
        let mut src = DeltaReferenceTableSource::from_source_config(config);
        assert!(src.poll_changes().await.unwrap().is_none());
    }

    #[cfg(feature = "delta-lake")]
    mod integration {
        use super::*;
        use arrow_array::{Int64Array, StringArray};
        use arrow_schema::{DataType, Field, Schema, SchemaRef};
        use std::collections::HashMap;
        use std::sync::Arc;
        use tempfile::TempDir;

        fn test_schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ]))
        }

        fn test_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
            RecordBatch::try_new(
                test_schema(),
                vec![
                    Arc::new(Int64Array::from(ids.to_vec())),
                    Arc::new(StringArray::from(names.to_vec())),
                ],
            )
            .unwrap()
        }

        async fn write_delta_version(path: &str, batches: Vec<RecordBatch>, epoch: i64) -> i64 {
            use crate::lakehouse::delta_io;
            use deltalake::protocol::SaveMode;

            let schema = test_schema();
            let table = delta_io::open_or_create_table(path, HashMap::new(), Some(&schema))
                .await
                .unwrap();

            let (_table, version) = delta_io::write_batches(
                table,
                batches,
                "test-writer",
                epoch,
                SaveMode::Append,
                None,
                false,
                None,
                false,
                None,
            )
            .await
            .unwrap();
            version
        }

        #[tokio::test]
        async fn test_snapshot_lifecycle() {
            let temp_dir = TempDir::new().unwrap();
            let table_path = temp_dir.path().to_str().unwrap();

            let batch = test_batch(&[1, 2, 3], &["Alice", "Bob", "Carol"]);
            write_delta_version(table_path, vec![batch], 1).await;

            let config = DeltaSourceConfig::new(table_path);
            let mut src = DeltaReferenceTableSource::from_source_config(config);

            assert!(!src.is_snapshot_complete());

            let mut total_rows = 0;
            while let Some(batch) = src.poll_snapshot().await.unwrap() {
                total_rows += batch.num_rows();
            }
            assert_eq!(total_rows, 3);
            assert!(src.is_snapshot_complete());
            assert!(src.poll_snapshot().await.unwrap().is_none());
            assert!(src.poll_changes().await.unwrap().is_none());

            src.close().await.unwrap();
        }

        #[tokio::test]
        async fn test_checkpoint_preserves_version() {
            let temp_dir = TempDir::new().unwrap();
            let table_path = temp_dir.path().to_str().unwrap();

            write_delta_version(table_path, vec![test_batch(&[1], &["Alice"])], 1).await;

            let config = DeltaSourceConfig::new(table_path);
            let mut src = DeltaReferenceTableSource::from_source_config(config);
            while src.poll_snapshot().await.unwrap().is_some() {}

            let cp = src.checkpoint();
            assert!(src.current_version >= 0);
            assert_eq!(
                cp.get_offset("delta_version"),
                Some(src.current_version.to_string().as_str())
            );
            src.close().await.unwrap();
        }

        #[tokio::test]
        async fn test_poll_changes_picks_up_new_version() {
            let temp_dir = TempDir::new().unwrap();
            let table_path = temp_dir.path().to_str().unwrap();

            // Version 1.
            write_delta_version(table_path, vec![test_batch(&[1, 2], &["Alice", "Bob"])], 1).await;

            // Snapshot.
            let mut config = DeltaSourceConfig::new(table_path);
            config.poll_interval = std::time::Duration::from_millis(0);
            let mut src = DeltaReferenceTableSource::from_source_config(config);
            while src.poll_snapshot().await.unwrap().is_some() {}
            let v1 = src.current_version;

            // Write version 2.
            write_delta_version(table_path, vec![test_batch(&[3], &["Carol"])], 2).await;

            // poll_changes should pick it up.
            let mut change_rows = 0;
            loop {
                match src.poll_changes().await.unwrap() {
                    Some(batch) => change_rows += batch.num_rows(),
                    None => break,
                }
            }
            assert_eq!(change_rows, 1);
            assert!(src.current_version > v1);
            src.close().await.unwrap();
        }

        #[tokio::test]
        async fn test_from_connector_config_with_catalog_none() {
            let temp_dir = TempDir::new().unwrap();
            let table_path = temp_dir.path().to_str().unwrap();

            write_delta_version(table_path, vec![test_batch(&[10], &["Test"])], 1).await;

            let mut config = ConnectorConfig::new("delta-lake");
            config.set("table.path", table_path);
            config.set("catalog.type", "none");

            let mut src = DeltaReferenceTableSource::from_connector_config(&config).unwrap();

            let mut total_rows = 0;
            while let Some(batch) = src.poll_snapshot().await.unwrap() {
                total_rows += batch.num_rows();
            }
            assert_eq!(total_rows, 1);
            assert!(src.is_snapshot_complete());
            src.close().await.unwrap();
        }
    }
}
