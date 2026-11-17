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
#[cfg(feature = "iceberg-core")]
mod split;
#[cfg(feature = "iceberg-core")]
mod start_position;

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
use crate::connector::split_ownership::SPLIT_ROUTE;
#[cfg(feature = "iceberg-core")]
use crate::connector::split_ownership::{OwnershipChange, SplitAssignment};
use crate::connector::{
    SourceBatch, SourceChannelPlacement, SourceCheckpointUnavailablePolicy, SourceConnector,
    SourceConsistency, SourceContract, SourceInputMode, SourcePosition, SourceStart,
    SourceTopology,
};
use crate::error::ConnectorError;

#[cfg(feature = "iceberg-core")]
use super::iceberg_config::IcebergReadBootstrap;
use super::iceberg_config::{IcebergReadMode, IcebergSourceConfig};
#[cfg(feature = "iceberg-core")]
use metrics::IcebergSourceMetrics;
#[cfg(feature = "iceberg-core")]
use planner::{Completion, ScanOutput, ScanTask};
#[cfg(feature = "iceberg-core")]
use read_schema::ReadSchemaBinding;

#[cfg(feature = "iceberg-core")]
pub use cursor::{IcebergSourceCursorOriginV1, IcebergSourceCursorV1};

#[cfg(feature = "iceberg-core")]
const PARTIAL_SCAN_ERROR: &str = "[LDB-ICEBERG-PARTIAL-SCAN] Iceberg scan failed after partial snapshot emission; recovery from the last durable cursor is required";

#[cfg(feature = "iceberg-core")]
struct PendingBatch {
    batch: RecordBatch,
    completed: Option<Completion>,
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
    /// Set by `read.splits`; replaces `cursor`.
    #[cfg(feature = "iceberg-core")]
    split: Option<split::IcebergSplitInput>,
    /// Cluster ownership, installed before `start`.
    #[cfg(feature = "iceberg-core")]
    assignment: Option<SplitAssignment>,
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
            #[cfg(feature = "iceberg-core")]
            split: None,
            #[cfg(feature = "iceberg-core")]
            assignment: None,
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
        let checkpoint = cursor.to_checkpoint()?;
        self.replay_unit_in_progress = false;
        self.observe_processed(cursor.snapshot_id, cursor.sequence_number);
        self.set_pending_snapshots(self.pending_snapshots.saturating_sub(1));
        self.checkpoint = checkpoint;
        self.cursor = Some(cursor);
        Ok(())
    }

    #[cfg(feature = "iceberg-core")]
    fn observe_processed(&mut self, snapshot_id: i64, sequence_number: i64) {
        self.metrics.processed_snapshot_id.set(snapshot_id);
        if let Some(current) = self.current_sequence_number {
            self.metrics
                .sequence_lag
                .set(current.saturating_sub(sequence_number));
        }
    }

    /// Run append planning under `storage.request_timeout` and observe how long it took.
    #[cfg(feature = "iceberg-core")]
    async fn plan_within_request_timeout<T, F>(
        &self,
        plan: impl FnOnce(tokio::time::Instant) -> F,
    ) -> Result<T, ConnectorError>
    where
        F: std::future::Future<Output = Result<T, ConnectorError>>,
    {
        let started = Instant::now();
        let deadline = super::iceberg_io::checked_deadline(
            self.config.storage.request_timeout,
            "storage.request_timeout",
        )?;
        let planned = tokio::time::timeout_at(deadline, plan(deadline))
            .await
            .map_err(|_| {
                ConnectorError::ReadError(
                    "[LDB-ICEBERG-APPEND-PLANNING-TIMEOUT] append manifest planning exceeded storage.request_timeout"
                        .into(),
                )
            })??;
        self.metrics
            .planning_duration
            .observe(started.elapsed().as_secs_f64());
        Ok(planned)
    }

    #[cfg(feature = "iceberg-core")]
    fn observe_planned(&mut self, files: usize, manifests: usize, snapshots: usize) {
        let count = |value: usize| u64::try_from(value).unwrap_or(u64::MAX);
        self.metrics.planned_files.inc_by(count(files));
        self.metrics.planned_manifests.inc_by(count(manifests));
        self.set_pending_snapshots(snapshots);
    }

    #[cfg(feature = "iceberg-core")]
    fn install_completion(&mut self, completion: Completion) -> Result<(), ConnectorError> {
        match completion {
            Completion::Snapshot(cursor) => self.install_cursor(cursor),
            Completion::Channel { channel, position } => {
                self.install_channel_position(channel, position)
            }
        }
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
        if let Some(completion) = pending.completed {
            self.install_completion(completion)?;
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
    fn loaded_table(&self) -> Result<&iceberg::table::Table, ConnectorError> {
        self.table
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "loaded Iceberg table".into(),
                actual: "table unavailable".into(),
            })
    }

    /// Load the table's current metadata and observe its head.
    #[cfg(feature = "iceberg-core")]
    async fn reload_table(&mut self) -> Result<iceberg::table::Table, ConnectorError> {
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
        self.observe_loaded_table(&table);
        Ok(table)
    }

    #[cfg(feature = "iceberg-core")]
    fn observe_loaded_table(&mut self, table: &iceberg::table::Table) {
        let head = table
            .metadata()
            .snapshot_for_ref(&self.config.table_ref)
            .map(|snapshot| (snapshot.snapshot_id(), snapshot.sequence_number()));
        self.observe_table_head(head);
        self.metrics
            .observe_table(table.metadata(), &self.config.table_ref);
    }

    #[cfg(feature = "iceberg-core")]
    fn start_initial_scan(&mut self) -> Result<(), ConnectorError> {
        let table = self.loaded_table()?.clone();
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
        )?;
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
        let table = self.reload_table().await?;
        if let Some(cursor) = self.cursor.as_ref() {
            cursor.validate_binding(&self.config, &table)?;
            let plans = self
                .plan_within_request_timeout(|deadline| {
                    append_lineage::plan_appends(&table, cursor, &self.config, deadline)
                })
                .await?;
            let files = plans.iter().map(|plan| plan.added_files.len()).sum();
            let manifests = plans.iter().map(|plan| plan.manifest_count).sum();
            self.observe_planned(files, manifests, plans.len());
            self.scan = planner::append_task(
                table.clone(),
                &self.config,
                self.read_schema()?,
                self.filter_predicate.clone(),
                plans,
            )?;
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
                )?;
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
        // A split scan completes its snapshots channel by channel, so they stay pending until the
        // scan ends.
        if self.split.is_some() {
            self.set_pending_snapshots(0);
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
    async fn resolve_schema(
        &mut self,
        config: &ConnectorConfig,
        explicit: Option<SchemaRef>,
    ) -> Result<crate::schema::resolution::SchemaBinding, ConnectorError> {
        #[cfg(feature = "iceberg-core")]
        {
            super::schema_resolution::iceberg_source(config, explicit).await
        }
        #[cfg(not(feature = "iceberg-core"))]
        {
            let _ = (config, explicit);
            Err(ConnectorError::FeatureUnsupported(
                "Iceberg schema discovery requires the iceberg feature".into(),
            ))
        }
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (config, position, _) = request.into_parts();
        if matches!(&position, SourcePosition::Initialized { .. }) {
            return Err(ConnectorError::ConfigurationError(
                "Iceberg has no sealed topology startup contract".into(),
            ));
        }
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
            let source_name = config
                .get("laminar.source.name")
                .filter(|name| !name.is_empty())
                .unwrap_or("iceberg")
                .to_string();
            self.reset_read_state();
            let start = self.decode_start(&position, &source_name)?;
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
            let schema_cursor = start.schema_cursor(&table)?;
            let root_schema = self.resolve_root_schema(&config, &table, schema_cursor.as_ref())?;
            self.bind_read_schema(&root_schema, declared_schema)?;
            self.catalog = Some(catalog);
            self.observe_loaded_table(&table);
            self.table = Some(table);
            self.install_start(start, &source_name)?;
            self.state = ConnectorState::Running;
            info!(
                operation_class = "connector-open",
                storage_provider = %self.config.storage.diagnostic_provider(&self.config.catalog.warehouse),
                storage_endpoint_class = %self.config.storage.diagnostic_endpoint_class(&self.config.catalog.warehouse),
                storage_auth_source = %self.config.storage.diagnostic_auth_source(&self.config.catalog.warehouse),
                request_timeout_ms = self.config.storage.request_timeout.as_millis(),
                success = true,
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
                if !self.split_may_read()? {
                    return Ok(None);
                }
                if let Some(batch) = self.emit_pending(max_records)? {
                    return Ok(Some(batch));
                }
                if let Some(output) = self.next_scan_output().await? {
                    match output {
                        ScanOutput::Batch { batch, completed } => {
                            self.metrics.observe_batch(&batch);
                            self.pending = Some(PendingBatch { batch, completed });
                        }
                        ScanOutput::Completed(completion) => self.install_completion(completion)?,
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
                    if self.split.is_some() {
                        self.refresh_split().await?;
                    } else {
                        self.refresh_append().await?;
                    }
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
        if self.replay_unit_in_progress || !self.checkpoint_ready()? {
            return Ok(None);
        }
        Ok(Some(self.checkpoint()))
    }

    fn checkpoint_ready(&self) -> Result<bool, ConnectorError> {
        #[cfg(feature = "iceberg-core")]
        if let Some(split) = &self.split {
            return split.ready();
        }
        Ok(true)
    }

    #[cfg(feature = "iceberg-core")]
    fn drive_control_plane(&mut self) {
        let change = self
            .split
            .as_mut()
            .map_or(OwnershipChange::Unchanged, split::IcebergSplitInput::drive);
        self.follow_split_ownership(change);
    }

    #[cfg(feature = "iceberg-core")]
    fn begin_drain(
        &mut self,
        request: &crate::connector::SourceDrainRequest,
        _deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        self.split_mut()?.begin_drain(request.round)
    }

    // A drained split source stops at the end of the replay unit it is reading.
    #[cfg(feature = "iceberg-core")]
    fn poll_drain_ready(
        &mut self,
        round: laminar_core::checkpoint::AssignmentDrainId,
    ) -> Result<bool, ConnectorError> {
        self.split_mut()?.ensure_active_drain(round)?;
        Ok(!self.replay_unit_in_progress)
    }

    #[cfg(feature = "iceberg-core")]
    async fn finish_drain(
        &mut self,
        settlement: crate::connector::SourceDrainSettlement,
        _deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        let change = self.split_mut()?.finish_drain(&settlement)?;
        self.follow_split_ownership(change);
        Ok(())
    }

    #[cfg(feature = "iceberg-core")]
    fn set_vnode_assignment(
        &mut self,
        source_identity: &str,
        registry: Arc<laminar_core::state::VnodeRegistry>,
        self_id: laminar_core::state::NodeId,
    ) -> Result<(), ConnectorError> {
        self.assignment = Some(SplitAssignment::new(source_identity, registry, self_id)?);
        Ok(())
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
        if let Some(channels) = parsed.splits {
            return Ok(SourceContract::new(
                SourceConsistency::Replayable,
                SourceTopology::Splittable,
                SourceInputMode::AppendOnly,
            )
            .with_placement(SourceChannelPlacement {
                channels,
                route: SPLIT_ROUTE,
            }));
        }
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
