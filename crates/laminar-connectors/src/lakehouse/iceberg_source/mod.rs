//! Replayable Apache Iceberg snapshot and append source.

#[cfg(feature = "iceberg-core")]
mod append_lineage;
#[cfg(feature = "iceberg-core")]
mod cursor;
#[cfg(feature = "iceberg-core")]
mod metrics;
#[cfg(feature = "iceberg-core")]
mod planner;

use std::sync::Arc;
#[cfg(feature = "iceberg-core")]
use std::time::Instant;

#[cfg(feature = "iceberg-core")]
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
#[cfg(feature = "iceberg-core")]
use iceberg::spec::SnapshotRef;
#[cfg(feature = "iceberg-core")]
use tracing::info;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceInputMode,
    SourcePosition, SourceStart, SourceTopology,
};
use crate::error::ConnectorError;

#[cfg(feature = "iceberg-core")]
use super::iceberg_config::IcebergReadBootstrap;
use super::iceberg_config::{IcebergReadMode, IcebergSourceConfig};
#[cfg(feature = "iceberg-core")]
use metrics::IcebergSourceMetrics;
#[cfg(feature = "iceberg-core")]
use planner::{ScanOutput, ScanTask};

#[cfg(feature = "iceberg-core")]
pub use cursor::IcebergSourceCursorV1;

#[cfg(feature = "iceberg-core")]
struct PendingBatch {
    batch: RecordBatch,
    completed_cursor: Option<IcebergSourceCursorV1>,
}

/// Apache Iceberg source with bounded scan-to-ingestion backpressure.
pub struct IcebergSource {
    config: IcebergSourceConfig,
    schema: Option<SchemaRef>,
    state: ConnectorState,
    checkpoint: SourceCheckpoint,
    #[cfg(feature = "iceberg-core")]
    cursor: Option<IcebergSourceCursorV1>,
    #[cfg(feature = "iceberg-core")]
    pending: Option<PendingBatch>,
    #[cfg(feature = "iceberg-core")]
    scan: Option<ScanTask>,
    #[cfg(feature = "iceberg-core")]
    last_poll_time: Option<Instant>,
    #[cfg(feature = "iceberg-core")]
    bounded_snapshot_complete: bool,
    #[cfg(feature = "iceberg-core")]
    current_sequence_number: Option<i64>,
    #[cfg(feature = "iceberg-core")]
    pending_snapshots: usize,
    #[cfg(feature = "iceberg-core")]
    metrics: IcebergSourceMetrics,
    #[cfg(feature = "iceberg-core")]
    catalog: Option<Arc<dyn iceberg::Catalog>>,
    #[cfg(feature = "iceberg-core")]
    table: Option<iceberg::table::Table>,
}

impl IcebergSource {
    /// Creates an Iceberg source from parsed configuration.
    #[must_use]
    pub fn new(config: IcebergSourceConfig, registry: Option<&prometheus::Registry>) -> Self {
        #[cfg(not(feature = "iceberg-core"))]
        let _ = registry;
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_metadata("connector_type", "iceberg");
        Self {
            config,
            schema: None,
            state: ConnectorState::Created,
            checkpoint,
            #[cfg(feature = "iceberg-core")]
            cursor: None,
            #[cfg(feature = "iceberg-core")]
            pending: None,
            #[cfg(feature = "iceberg-core")]
            scan: None,
            #[cfg(feature = "iceberg-core")]
            last_poll_time: None,
            #[cfg(feature = "iceberg-core")]
            bounded_snapshot_complete: false,
            #[cfg(feature = "iceberg-core")]
            current_sequence_number: None,
            #[cfg(feature = "iceberg-core")]
            pending_snapshots: 0,
            #[cfg(feature = "iceberg-core")]
            metrics: IcebergSourceMetrics::new(registry),
            #[cfg(feature = "iceberg-core")]
            catalog: None,
            #[cfg(feature = "iceberg-core")]
            table: None,
        }
    }

    #[cfg(feature = "iceberg-core")]
    fn install_cursor(&mut self, cursor: IcebergSourceCursorV1) -> Result<(), ConnectorError> {
        self.metrics.processed_snapshot_id.set(cursor.snapshot_id);
        self.pending_snapshots = self.pending_snapshots.saturating_sub(1);
        self.metrics
            .snapshot_lag
            .set(i64::try_from(self.pending_snapshots).unwrap_or(i64::MAX));
        if let Some(current) = self.current_sequence_number {
            self.metrics
                .sequence_lag
                .set(current.saturating_sub(cursor.sequence_number));
        }
        self.checkpoint = cursor.to_checkpoint()?;
        self.cursor = Some(cursor);
        Ok(())
    }

    #[cfg(feature = "iceberg-core")]
    fn observe_table_head(&mut self, snapshot: Option<(i64, i64)>) {
        if let Some((snapshot_id, sequence_number)) = snapshot {
            self.metrics.current_snapshot_id.set(snapshot_id);
            self.current_sequence_number = Some(sequence_number);
            if let Some(cursor) = &self.cursor {
                self.metrics
                    .sequence_lag
                    .set(sequence_number.saturating_sub(cursor.sequence_number));
            }
        }
    }

    #[cfg(feature = "iceberg-core")]
    fn set_pending_snapshots(&mut self, pending: usize) {
        self.pending_snapshots = pending;
        self.metrics
            .snapshot_lag
            .set(i64::try_from(pending).unwrap_or(i64::MAX));
    }

    #[cfg(feature = "iceberg-core")]
    fn emit_pending(&mut self, max_records: usize) -> Result<Option<SourceBatch>, ConnectorError> {
        let Some(mut pending) = self.pending.take() else {
            return Ok(None);
        };
        if pending.batch.num_rows() > max_records {
            let emitted = pending.batch.slice(0, max_records);
            pending.batch = pending
                .batch
                .slice(max_records, pending.batch.num_rows() - max_records);
            self.pending = Some(pending);
            return Ok(Some(SourceBatch::new(emitted)));
        }
        if let Some(cursor) = pending.completed_cursor {
            self.install_cursor(cursor)?;
        }
        Ok(Some(SourceBatch::new(pending.batch)))
    }

    #[cfg(feature = "iceberg-core")]
    fn selected_snapshot(
        table: &iceberg::table::Table,
        config: &IcebergSourceConfig,
    ) -> Result<Option<SnapshotRef>, ConnectorError> {
        if let Some(snapshot_id) = config.snapshot_id {
            return table
                .metadata()
                .snapshot_by_id(snapshot_id)
                .cloned()
                .map(Some)
                .ok_or_else(|| {
                    ConnectorError::ReadError(format!(
                        "[LDB-ICEBERG-SNAPSHOT-MISSING] snapshot {snapshot_id} does not exist"
                    ))
                });
        }
        if let Some(snapshot) = table.metadata().snapshot_for_ref(&config.table_ref) {
            return Ok(Some(snapshot.clone()));
        }
        if config.table_ref == "main" && table.metadata().current_snapshot().is_none() {
            return Ok(None);
        }
        Err(ConnectorError::ReadError(format!(
            "[LDB-ICEBERG-REF-MISSING] table ref '{}' does not exist",
            config.table_ref
        )))
    }

    #[cfg(feature = "iceberg-core")]
    fn start_initial_scan(&mut self) -> Result<(), ConnectorError> {
        let table = self
            .table
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "loaded Iceberg table".into(),
                actual: "table unavailable".into(),
            })?
            .clone();
        let Some(snapshot) = Self::selected_snapshot(&table, &self.config)? else {
            self.bounded_snapshot_complete = self.config.read_mode == IcebergReadMode::Snapshot;
            return Ok(());
        };
        let cursor = IcebergSourceCursorV1::from_snapshot(&self.config, &table, &snapshot);
        if self.config.read_mode == IcebergReadMode::Append {
            append_lineage::validate_cursor_lineage(
                &table,
                &cursor,
                self.config.max_snapshots_per_poll,
            )?;
        }
        if self.config.read_mode == IcebergReadMode::Append
            && self.config.bootstrap == IcebergReadBootstrap::None
        {
            return self.install_cursor(cursor);
        }
        self.set_pending_snapshots(1);
        self.scan = Some(planner::full_snapshot_task(
            table,
            &self.config,
            snapshot.snapshot_id(),
        )?);
        Ok(())
    }

    #[cfg(feature = "iceberg-core")]
    async fn refresh_append(&mut self) -> Result<(), ConnectorError> {
        if let Some(last_poll) = self.last_poll_time {
            if last_poll.elapsed() < self.config.poll_interval {
                return Ok(());
            }
        }
        self.last_poll_time = Some(Instant::now());
        let poll_started = Instant::now();
        let catalog = self
            .catalog
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "started Iceberg source".into(),
                actual: "catalog unavailable".into(),
            })?;
        let table = super::iceberg_io::load_table_with_timeout(
            catalog.as_ref(),
            &self.config.catalog.namespace,
            &self.config.catalog.table_name,
            self.config.catalog.request_timeout,
        )
        .await?;
        let head = table
            .metadata()
            .snapshot_for_ref(&self.config.table_ref)
            .map(|snapshot| (snapshot.snapshot_id(), snapshot.sequence_number()));
        self.observe_table_head(head);
        self.metrics
            .observe_table(table.metadata(), &self.config.table_ref);

        if let Some(cursor) = self.cursor.as_ref() {
            cursor.validate_binding(&self.config, &table)?;
            let planning_started = Instant::now();
            let plans = tokio::time::timeout(
                self.config.storage.request_timeout,
                append_lineage::plan_appends(
                    &table,
                    cursor,
                    self.config.max_snapshots_per_poll,
                    self.config.max_planned_files,
                ),
            )
            .await
            .map_err(|_| {
                ConnectorError::ReadError(
                    "[LDB-ICEBERG-APPEND-PLANNING-TIMEOUT] append manifest planning exceeded storage.request_timeout"
                        .into(),
                )
            })??;
            self.metrics
                .planning_duration
                .observe(planning_started.elapsed().as_secs_f64());
            let planned_files = plans.iter().fold(0_u64, |total, plan| {
                total.saturating_add(u64::try_from(plan.added_file_paths.len()).unwrap_or(u64::MAX))
            });
            let planned_manifests = plans.iter().fold(0_u64, |total, plan| {
                total.saturating_add(u64::try_from(plan.manifest_count).unwrap_or(u64::MAX))
            });
            self.metrics.planned_files.inc_by(planned_files);
            self.metrics.planned_manifests.inc_by(planned_manifests);
            self.set_pending_snapshots(plans.len());
            self.scan = planner::append_task(table.clone(), &self.config, plans)?;
        } else if let Some(snapshot) = Self::selected_snapshot(&table, &self.config)? {
            if self.config.bootstrap == IcebergReadBootstrap::Initial {
                self.scan = Some(planner::full_snapshot_task(
                    table.clone(),
                    &self.config,
                    snapshot.snapshot_id(),
                )?);
            } else {
                self.install_cursor(IcebergSourceCursorV1::from_snapshot(
                    &self.config,
                    &table,
                    &snapshot,
                ))?;
            }
        }
        self.table = Some(table);
        self.metrics
            .poll_duration
            .observe(poll_started.elapsed().as_secs_f64());
        Ok(())
    }

    #[cfg(feature = "iceberg-core")]
    async fn finish_scan(&mut self) -> Result<(), ConnectorError> {
        let Some(scan) = self.scan.take() else {
            return Ok(());
        };
        scan.handle.await.map_err(|error| {
            ConnectorError::Internal(format!(
                "Iceberg scan task terminated unexpectedly: {error}"
            ))
        })?;
        self.metrics
            .read_duration
            .observe(scan.started_at.elapsed().as_secs_f64());
        if self.config.read_mode == IcebergReadMode::Snapshot {
            self.bounded_snapshot_complete = true;
        }
        Ok(())
    }

    #[cfg(feature = "iceberg-core")]
    async fn next_scan_output(&mut self) -> Result<Option<ScanOutput>, ConnectorError> {
        let result = match self.scan.as_mut() {
            Some(scan) => scan.receiver.recv().await,
            None => return Ok(None),
        };
        if let Some(result) = result {
            result.map(Some)
        } else {
            self.finish_scan().await?;
            Ok(None)
        }
    }
}

#[async_trait]
impl SourceConnector for IcebergSource {
    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (config, position, _) = request.into_parts();
        if !config.properties().is_empty() {
            self.config = IcebergSourceConfig::from_config(&config)?;
        }
        super::iceberg::capabilities::validate_source(&self.config)?;
        if matches!(&position, SourcePosition::Resume { .. })
            && self.config.read_mode != IcebergReadMode::Append
        {
            return Err(ConnectorError::ConfigurationError(
                "only Iceberg read.mode=append has a replay cursor".into(),
            ));
        }

        #[cfg(feature = "iceberg-core")]
        {
            let catalog =
                super::iceberg_io::build_catalog(&self.config.catalog, &self.config.storage)
                    .await?;
            let table = super::iceberg_io::load_table_with_timeout(
                catalog.as_ref(),
                &self.config.catalog.namespace,
                &self.config.catalog.table_name,
                self.config.catalog.request_timeout,
            )
            .await?;
            let arrow_schema = iceberg::arrow::schema_to_arrow_schema(&table.current_schema_ref())
                .map_err(|error| {
                    ConnectorError::SchemaMismatch(format!("Iceberg to Arrow schema: {error}"))
                })?;
            self.schema = Some(Arc::new(arrow_schema));
            self.catalog = Some(catalog);
            let head = table
                .metadata()
                .snapshot_for_ref(&self.config.table_ref)
                .map(|snapshot| (snapshot.snapshot_id(), snapshot.sequence_number()));
            self.observe_table_head(head);
            self.metrics
                .observe_table(table.metadata(), &self.config.table_ref);
            self.table = Some(table);

            match position {
                SourcePosition::Initial => self.start_initial_scan()?,
                SourcePosition::Resume { checkpoint, .. } => {
                    let cursor = IcebergSourceCursorV1::from_checkpoint(&checkpoint)?;
                    let table =
                        self.table
                            .as_ref()
                            .ok_or_else(|| ConnectorError::InvalidState {
                                expected: "loaded Iceberg table".into(),
                                actual: "table unavailable".into(),
                            })?;
                    cursor.validate_binding(&self.config, table)?;
                    append_lineage::validate_cursor_lineage(
                        table,
                        &cursor,
                        self.config.max_snapshots_per_poll,
                    )?;
                    self.install_cursor(cursor)?;
                }
            }
            self.state = ConnectorState::Running;
            info!(
                table = self.config.catalog.table_name,
                namespace = self.config.catalog.namespace,
                mode = %self.config.read_mode,
                "iceberg source connected"
            );
            return Ok(());
        }

        #[cfg(not(feature = "iceberg-core"))]
        {
            self.state = ConnectorState::Failed;
            Err(ConnectorError::FeatureUnsupported(
                "Apache Iceberg requires the 'iceberg' feature".into(),
            ))
        }
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if max_records == 0 {
            return Err(ConnectorError::ConfigurationError(
                "Iceberg source max_records must be greater than zero".into(),
            ));
        }
        #[cfg(feature = "iceberg-core")]
        {
            loop {
                if let Some(batch) = self.emit_pending(max_records)? {
                    return Ok(Some(batch));
                }
                if let Some(output) = self.next_scan_output().await? {
                    match output {
                        ScanOutput::Batch {
                            batch,
                            completed_cursor,
                        } => {
                            self.metrics.observe_batch(&batch);
                            self.pending = Some(PendingBatch {
                                batch,
                                completed_cursor,
                            });
                        }
                        ScanOutput::Cursor(cursor) => self.install_cursor(cursor)?,
                        ScanOutput::ReadMetrics {
                            files,
                            storage_bytes,
                        } => self.metrics.observe_completed_read(files, storage_bytes),
                    }
                    continue;
                }
                if self.bounded_snapshot_complete {
                    return Ok(None);
                }
                if self.config.read_mode == IcebergReadMode::Append {
                    self.refresh_append().await?;
                    if self.scan.is_some() {
                        continue;
                    }
                }
                return Ok(None);
            }
        }
        #[cfg(not(feature = "iceberg-core"))]
        {
            Err(ConnectorError::FeatureUnsupported(
                "Apache Iceberg requires the 'iceberg' feature".into(),
            ))
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema
            .clone()
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()))
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        self.checkpoint.clone()
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        let parsed = if config.properties().is_empty() {
            self.config.clone()
        } else {
            IcebergSourceConfig::from_config(config)?
        };
        super::iceberg::capabilities::validate_source(&parsed)?;
        Ok(SourceContract::new(
            if parsed.read_mode == IcebergReadMode::Append {
                SourceConsistency::Replayable
            } else {
                SourceConsistency::Ephemeral
            },
            SourceTopology::Singleton,
            SourceInputMode::AppendOnly,
        ))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        #[cfg(feature = "iceberg-core")]
        {
            if let Some(scan) = self.scan.take() {
                drop(scan.receiver);
                scan.handle.abort();
                let _ = scan.handle.await;
            }
            self.catalog = None;
            self.table = None;
            self.pending = None;
        }
        self.state = ConnectorState::Closed;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
