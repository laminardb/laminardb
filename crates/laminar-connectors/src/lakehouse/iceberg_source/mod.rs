//! Replayable Apache Iceberg snapshot and append source.

#[cfg(feature = "iceberg-core")]
mod append_lineage;
#[cfg(feature = "iceberg-core")]
mod cursor;
#[cfg(feature = "iceberg-core")]
mod metrics;
#[cfg(feature = "iceberg-core")]
mod planner;
#[cfg(feature = "iceberg-core")]
mod read_schema;

use std::sync::Arc;
#[cfg(feature = "iceberg-core")]
use std::time::Instant;

#[cfg(feature = "iceberg-core")]
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
#[cfg(feature = "iceberg-core")]
use iceberg::expr::Predicate;
#[cfg(feature = "iceberg-core")]
use iceberg::spec::SnapshotRef;
#[cfg(feature = "iceberg-core")]
use tracing::info;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    SourceBatch, SourceCheckpointUnavailablePolicy, SourceConnector, SourceConsistency,
    SourceContract, SourceInputMode, SourcePosition, SourceStart, SourceTopology,
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
use read_schema::ReadSchemaBinding;

#[cfg(feature = "iceberg-core")]
pub use cursor::{IcebergSourceCursorOriginV1, IcebergSourceCursorV1};

#[cfg(feature = "iceberg-core")]
const PARTIAL_SCAN_ERROR: &str = "[LDB-ICEBERG-PARTIAL-SCAN] Iceberg scan failed after partial snapshot emission; recovery from the last durable cursor is required";

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
    replay_unit_in_progress: bool,
    #[cfg(feature = "iceberg-core")]
    scan_failed: bool,
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
    #[cfg(feature = "iceberg-core")]
    read_schema: Option<ReadSchemaBinding>,
    #[cfg(feature = "iceberg-core")]
    filter_predicate: Option<Predicate>,
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
            replay_unit_in_progress: false,
            #[cfg(feature = "iceberg-core")]
            scan_failed: false,
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
            #[cfg(feature = "iceberg-core")]
            read_schema: None,
            #[cfg(feature = "iceberg-core")]
            filter_predicate: None,
        }
    }

    #[cfg(feature = "iceberg-core")]
    fn bind_read_schema(
        &mut self,
        root: &iceberg::spec::Schema,
        declared: Option<SchemaRef>,
    ) -> Result<(), ConnectorError> {
        let predicate = super::iceberg_scan::parse_and_bind_filter(
            self.config.filter.as_deref(),
            Arc::new(root.clone()),
        )?;
        let binding = ReadSchemaBinding::bind(root, &self.config.select_columns, declared)?;
        self.schema = Some(binding.output_schema());
        self.read_schema = Some(binding);
        self.filter_predicate = predicate;
        Ok(())
    }

    #[cfg(feature = "iceberg-core")]
    fn read_schema(&self) -> Result<&ReadSchemaBinding, ConnectorError> {
        self.read_schema
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "Iceberg source bound to a retained snapshot schema".into(),
                actual: "read schema is unavailable".into(),
            })
    }

    #[cfg(feature = "iceberg-core")]
    fn install_cursor(&mut self, cursor: IcebergSourceCursorV1) -> Result<(), ConnectorError> {
        self.replay_unit_in_progress = false;
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
            self.replay_unit_in_progress = true;
            return Ok(Some(
                SourceBatch::new(emitted).with_checkpoint(self.checkpoint.clone()),
            ));
        }
        if let Some(cursor) = pending.completed_cursor {
            self.install_cursor(cursor)?;
        } else {
            self.replay_unit_in_progress = true;
        }
        Ok(Some(
            SourceBatch::new(pending.batch).with_checkpoint(self.checkpoint.clone()),
        ))
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
        let selected = Self::selected_snapshot(&table, &self.config)?;
        if self.read_schema.is_none() {
            let root_schema = match selected.as_ref() {
                Some(snapshot) => snapshot.schema(table.metadata()).map_err(|error| {
                    super::iceberg_scan::connector_scan_error(
                        "resolve retained Iceberg snapshot schema",
                        &error,
                    )
                })?,
                None => table.current_schema_ref(),
            };
            self.bind_read_schema(&root_schema, None)?;
        }
        let Some(snapshot) = selected else {
            if self.config.read_mode == IcebergReadMode::Snapshot {
                self.bounded_snapshot_complete = true;
                return Ok(());
            }
            let cursor = IcebergSourceCursorV1::from_empty_table(
                &self.config,
                &table,
                self.read_schema()?.schema_id(),
            )?;
            return self.install_cursor(cursor);
        };
        let cursor = IcebergSourceCursorV1::from_snapshot(
            &self.config,
            &table,
            &snapshot,
            self.read_schema()?.schema_id(),
        );
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
            self.read_schema()?,
            self.filter_predicate.clone(),
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
            let deadline = tokio::time::Instant::now() + self.config.storage.request_timeout;
            let plans = tokio::time::timeout_at(
                deadline,
                append_lineage::plan_appends(&table, cursor, &self.config, deadline),
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
                total.saturating_add(u64::try_from(plan.added_files.len()).unwrap_or(u64::MAX))
            });
            let planned_manifests = plans.iter().fold(0_u64, |total, plan| {
                total.saturating_add(u64::try_from(plan.manifest_count).unwrap_or(u64::MAX))
            });
            self.metrics.planned_files.inc_by(planned_files);
            self.metrics.planned_manifests.inc_by(planned_manifests);
            self.set_pending_snapshots(plans.len());
            self.scan = planner::append_task(
                table.clone(),
                &self.config,
                self.read_schema()?,
                self.filter_predicate.clone(),
                plans,
            );
        } else if let Some(snapshot) = Self::selected_snapshot(&table, &self.config)? {
            if self.config.bootstrap == IcebergReadBootstrap::Initial {
                self.scan = Some(planner::full_snapshot_task(
                    table.clone(),
                    &self.config,
                    self.read_schema()?,
                    self.filter_predicate.clone(),
                    snapshot.snapshot_id(),
                )?);
            } else {
                let cursor = IcebergSourceCursorV1::from_snapshot(
                    &self.config,
                    &table,
                    &snapshot,
                    self.read_schema()?.schema_id(),
                );
                self.install_cursor(cursor)?;
            }
        }
        self.table = Some(table);
        self.last_poll_time = Some(Instant::now());
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
        let failed = std::mem::take(&mut self.scan_failed);
        scan.handle.await.map_err(|error| {
            ConnectorError::Internal(format!(
                "Iceberg scan task terminated unexpectedly: {error}"
            ))
        })?;
        self.metrics
            .read_duration
            .observe(scan.started_at.elapsed().as_secs_f64());
        if self.config.read_mode == IcebergReadMode::Snapshot && !failed {
            self.bounded_snapshot_complete = true;
        }
        Ok(())
    }

    #[cfg(feature = "iceberg-core")]
    fn fail_partial_scan(&mut self) -> ConnectorError {
        self.state = ConnectorState::Failed;
        ConnectorError::TransactionError(PARTIAL_SCAN_ERROR.into())
    }

    #[cfg(feature = "iceberg-core")]
    async fn next_scan_output(&mut self) -> Result<Option<ScanOutput>, ConnectorError> {
        let result = match self.scan.as_mut() {
            Some(scan) => scan.receiver.recv().await,
            None => return Ok(None),
        };
        if let Some(result) = result {
            match result {
                Err(error) => {
                    self.scan_failed = true;
                    if self.replay_unit_in_progress {
                        Err(self.fail_partial_scan())
                    } else {
                        Err(error)
                    }
                }
                Ok(output) => Ok(Some(output)),
            }
        } else {
            match self.finish_scan().await {
                Ok(()) => Ok(None),
                Err(_) if self.replay_unit_in_progress => Err(self.fail_partial_scan()),
                Err(error) => Err(error),
            }
        }
    }
}

#[async_trait]
impl SourceConnector for IcebergSource {
    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (config, position, _) = request.into_parts();
        #[cfg(feature = "iceberg-core")]
        let declared_schema = if config.get("_arrow_schema").is_some() {
            let schema = config.arrow_schema().ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "invalid Iceberg source '_arrow_schema' encoding".into(),
                )
            })?;
            if schema.fields().is_empty() {
                return Err(ConnectorError::ConfigurationError(
                    "Iceberg source '_arrow_schema' must declare at least one column".into(),
                ));
            }
            Some(schema)
        } else {
            None
        };
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
            let recovered_cursor = match &position {
                SourcePosition::Initial => None,
                SourcePosition::Resume { checkpoint, .. } => {
                    Some(IcebergSourceCursorV1::from_checkpoint(checkpoint)?)
                }
            };
            let catalog = super::iceberg_io::build_catalog_for_access_with_metrics(
                &self.config.catalog,
                &self.config.storage,
                super::iceberg_io::CatalogAccess::Read,
                Some(self.metrics.credential_refresh_failures.clone()),
            )
            .await?
            .catalog;
            let table = super::iceberg_io::load_table_with_timeout(
                catalog.as_ref(),
                &self.config.catalog.namespace,
                &self.config.catalog.table_name,
                self.config.catalog.request_timeout,
            )
            .await?;
            let root_schema = if let Some(cursor) = recovered_cursor.as_ref() {
                cursor.validate_binding(&self.config, &table)?;
                append_lineage::validate_cursor_lineage(
                    &table,
                    cursor,
                    self.config.max_snapshots_per_poll,
                )?;
                cursor.retained_schema(&table)?
            } else {
                match Self::selected_snapshot(&table, &self.config)? {
                    Some(snapshot) => snapshot.schema(table.metadata()).map_err(|error| {
                        super::iceberg_scan::connector_scan_error(
                            "resolve initial Iceberg snapshot schema",
                            &error,
                        )
                    })?,
                    None => table.current_schema_ref(),
                }
            };
            self.bind_read_schema(&root_schema, declared_schema)?;
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
                SourcePosition::Resume { .. } => {
                    self.install_cursor(recovered_cursor.ok_or_else(|| {
                        ConnectorError::Internal(
                            "Iceberg resume cursor was not retained during startup".into(),
                        )
                    })?)?;
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
            if self.state == ConnectorState::Failed {
                return if self.replay_unit_in_progress {
                    Err(ConnectorError::TransactionError(PARTIAL_SCAN_ERROR.into()))
                } else {
                    Err(ConnectorError::InvalidState {
                        expected: "Running".into(),
                        actual: self.state.to_string(),
                    })
                };
            }
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
                if self.config.read_mode == IcebergReadMode::Snapshot {
                    self.start_initial_scan()?;
                    if self.scan.is_some() {
                        continue;
                    }
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

    fn try_checkpoint(&self) -> Result<Option<SourceCheckpoint>, ConnectorError> {
        #[cfg(feature = "iceberg-core")]
        if self.replay_unit_in_progress {
            return Ok(None);
        }
        Ok(Some(self.checkpoint()))
    }

    fn checkpoint_unavailable_policy(&self) -> SourceCheckpointUnavailablePolicy {
        SourceCheckpointUnavailablePolicy::PollToReplayBoundary
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
            self.read_schema = None;
            self.filter_predicate = None;
            self.pending = None;
            self.replay_unit_in_progress = false;
            self.scan_failed = false;
        }
        self.state = ConnectorState::Closed;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
