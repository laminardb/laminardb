//! Delta Lake change-data-feed and snapshot source.
//!
//! Rows are read through the Delta kernel in replay units — one file of a pinned snapshot, or
//! one commit of the change data feed — and the source checkpoints only between units. Operator
//! state recovered with a cursor therefore holds every earlier unit completely and nothing of the
//! units replay delivers again.

mod cursor;
#[cfg(feature = "delta-lake")]
mod reader;
#[cfg(feature = "delta-lake")]
mod reader_contract;

use std::sync::Arc;

#[cfg(feature = "delta-lake")]
use arrow_array::{BinaryArray, RecordBatch, UInt32Array};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
#[cfg(feature = "delta-lake")]
use std::time::Instant;
use tracing::info;

#[cfg(feature = "delta-lake")]
use deltalake::DeltaTable;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    ConnectorTaskOwner, ConnectorTaskTracker, SourceBatch, SourceCheckpointUnavailablePolicy,
    SourceConnector, SourceConsistency, SourceContract, SourceInputMode, SourcePosition,
    SourceRowPositionCapability, SourceStart, SourceTopology,
};
use crate::error::ConnectorError;

use super::delta_source_config::DeltaSourceConfig;
#[cfg(feature = "delta-lake")]
use super::delta_source_config::{DeltaReadMode, DeltaStartingVersion};
use cursor::DeltaCursor;
#[cfg(feature = "delta-lake")]
use cursor::DeltaPhase;

/// Commits planned by one change-feed scan; bounds the log work behind one kernel read.
#[cfg(feature = "delta-lake")]
const MAX_COMMITS_PER_SCAN: u64 = 64;

const PARTIAL_UNIT_ERROR: &str = "[LDB-DELTA-PARTIAL-UNIT] Delta read failed after part of a \
                                  commit or snapshot file was emitted; recovery from the last \
                                  durable cursor is required";

/// Delta Lake source connector.
///
/// Emits a full changelog (`__weight` = +1 for inserts and update post-images, -1 for deletes
/// and update pre-images) from either a pinned snapshot followed by the change data feed, the
/// change data feed from an explicit version, or one snapshot that then completes.
///
/// # Lifecycle
///
/// ```text
/// new() -> start() -> [poll_batch()]* -> close()
///                           |
///                      checkpoint()
/// ```
pub struct DeltaSource {
    config: DeltaSourceConfig,
    state: ConnectorState,
    /// Arrow schema emitted to the pipeline, set from the bound reader contract on start.
    schema: Option<SchemaRef>,
    #[cfg(feature = "delta-lake")]
    schema_binding: Option<crate::schema::resolution::SchemaBinding>,
    #[cfg(feature = "delta-lake")]
    task_owner: ConnectorTaskOwner,
    task_tracker: ConnectorTaskTracker,
    /// Cursor after the last complete replay unit; the only position ever checkpointed.
    cursor: Option<DeltaCursor>,
    /// Replay unit whose rows are partly emitted.
    #[cfg(feature = "delta-lake")]
    open_unit: Option<OpenUnit>,
    #[cfg(feature = "delta-lake")]
    reader: Option<Reader>,
    records_read: u64,
}

#[cfg(feature = "delta-lake")]
#[derive(Debug, Clone, Copy)]
struct OpenUnit {
    unit: reader::UnitKey,
    rows_emitted: u32,
}

#[cfg(feature = "delta-lake")]
struct Pending {
    batch: RecordBatch,
    unit: reader::UnitKey,
    completes: Option<DeltaPhase>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

/// Open table and in-flight read of a started source.
#[cfg(feature = "delta-lake")]
struct Reader {
    /// Metadata-only handle used to discover new commits and confirm the table incarnation.
    table: DeltaTable,
    /// Catalog resolution in use, including vended credentials and their expiry.
    resolved: super::delta_io::ResolvedCatalogTable,
    context: reader::ReadContext,
    scan: Option<reader::ScanTask>,
    pending: Option<Pending>,
    last_version_check: Option<Instant>,
}

impl DeltaSource {
    /// Creates a new Delta Lake source with the given configuration.
    #[must_use]
    pub fn new(config: DeltaSourceConfig, _registry: Option<&prometheus::Registry>) -> Self {
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        #[cfg(not(feature = "delta-lake"))]
        drop(task_owner);
        Self {
            config,
            state: ConnectorState::Created,
            schema: None,
            #[cfg(feature = "delta-lake")]
            schema_binding: None,
            #[cfg(feature = "delta-lake")]
            task_owner,
            task_tracker,
            cursor: None,
            #[cfg(feature = "delta-lake")]
            open_unit: None,
            #[cfg(feature = "delta-lake")]
            reader: None,
            records_read: 0,
        }
    }

    /// Returns the current connector state.
    #[must_use]
    pub fn state(&self) -> ConnectorState {
        self.state
    }

    /// Returns the source configuration.
    #[must_use]
    pub fn config(&self) -> &DeltaSourceConfig {
        &self.config
    }
}

#[cfg(feature = "delta-lake")]
impl DeltaSource {
    async fn open(
        &mut self,
        config: &ConnectorConfig,
        recovered: Option<DeltaCursor>,
    ) -> Result<(), ConnectorError> {
        use super::delta_io;

        let resolved = self.resolve_table().await?;
        let resolved_storage = crate::storage::StorageCredentialResolver::resolve(
            &resolved.location,
            &resolved.storage_options,
        );
        info!(
            operation_class = "connector-open",
            storage_provider = %resolved_storage.provider,
            storage_endpoint_class = %resolved_storage.endpoint_class(),
            storage_auth_source = %resolved_storage.auth_source,
            read_mode = ?self.config.read_mode,
            starting_version = ?self.config.starting_version,
            resumed = recovered.is_some(),
            "opening Delta Lake source connector"
        );
        let table =
            delta_io::open_table_metadata(&resolved.location, resolved.storage_options.clone())
                .await?;
        self.install_reader_contract(config, &table)?;
        let snapshot = table.snapshot().map_err(|error| {
            delta_io::classify_delta_metadata_error("read Delta table metadata", &error)
        })?;
        let table_id = snapshot.metadata().id().to_string();
        self.check_read_features(snapshot.metadata().configuration())?;
        let latest = snapshot.version();
        let cursor = match recovered {
            Some(cursor) => self.validate_recovered(cursor, &table_id)?,
            None => self.initial_cursor(table_id, latest)?,
        };

        let log_store = table.log_store();
        let mut table_root = log_store.root_url().clone();
        if !table_root.path().ends_with('/') {
            table_root.set_path(&format!("{}/", table_root.path()));
        }
        let output_schema = self
            .schema
            .clone()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "bound Delta reader schema".into(),
                actual: "schema missing".into(),
            })?;
        let context = reader::ReadContext {
            engine: log_store.engine(None),
            table_root,
            output_schema,
            budget: Arc::new(tokio::sync::Semaphore::new(
                reader::MAX_QUEUED_BYTES as usize,
            )),
        };
        info!(
            latest_version = latest,
            cursor = ?cursor.phase,
            "Delta Lake source: installed read position"
        );
        self.cursor = Some(cursor);
        self.reader = Some(Reader {
            table,
            resolved,
            context,
            scan: None,
            pending: None,
            last_version_check: None,
        });
        Ok(())
    }

    /// Resolve the table through its catalog for reading. Environment credential fallbacks are
    /// resolved again each time rather than retained from the first start.
    async fn resolve_table(&self) -> Result<super::delta_io::ResolvedCatalogTable, ConnectorError> {
        let mut resolved = super::delta_io::resolve_catalog_options(
            &self.config.catalog_type,
            self.config.catalog_database.as_deref(),
            self.config.catalog_name.as_deref(),
            self.config.catalog_schema.as_deref(),
            &self.config.table_path,
            &self.config.stable_storage_options(),
            super::delta_io::CatalogAccess::Read,
        )
        .await?;
        resolved.storage_options = crate::storage::StorageCredentialResolver::resolve(
            &resolved.location,
            &resolved.storage_options,
        )
        .options;
        Ok(resolved)
    }

    /// Replace expiring vended credentials before the next read. Only the storage access
    /// changes; the table must keep its location and incarnation.
    async fn renew_credentials(&mut self) -> Result<(), ConnectorError> {
        let resolved = self.resolve_table().await?;
        let table_id = self.cursor()?.table_id.clone();
        let reader = self.reader_mut()?;
        if resolved.location != reader.resolved.location {
            return Err(ConnectorError::ConfigurationError(
                "the catalog moved the Delta table to another storage location; reset the source"
                    .into(),
            ));
        }
        let table = super::delta_io::open_table_metadata(
            &resolved.location,
            resolved.storage_options.clone(),
        )
        .await?;
        ensure_same_table(&table, &table_id)?;
        reader.context.engine = table.log_store().engine(None);
        reader.table = table;
        reader.resolved = resolved;
        info!("Delta Lake source: renewed vended storage credentials");
        Ok(())
    }

    /// The change data feed needs CDF enabled and, in the kernel, no column mapping. Snapshot
    /// reads support both column mapping and deletion vectors.
    fn check_read_features(
        &self,
        configuration: &std::collections::HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        if self.config.read_mode == DeltaReadMode::Snapshot {
            return Ok(());
        }
        let enabled = configuration
            .get("delta.enableChangeDataFeed")
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        if !enabled {
            return Err(ConnectorError::ConfigurationError(
                "Delta read.mode=changes requires the table property \
                 delta.enableChangeDataFeed=true; enable it or use read.mode=snapshot"
                    .into(),
            ));
        }
        if configuration
            .get("delta.columnMapping.mode")
            .is_some_and(|mode| !mode.eq_ignore_ascii_case("none"))
        {
            return Err(ConnectorError::ConfigurationError(
                "the Delta kernel change data feed does not support column-mapped tables; use \
                 read.mode=snapshot"
                    .into(),
            ));
        }
        Ok(())
    }

    fn initial_cursor(&self, table_id: String, latest: u64) -> Result<DeltaCursor, ConnectorError> {
        let after_latest = latest
            .checked_add(1)
            .ok_or_else(|| ConnectorError::ReadError("Delta version overflowed".into()))?;
        let phase = match (self.config.read_mode, self.config.starting_version) {
            (DeltaReadMode::Snapshot, _)
            | (DeltaReadMode::Changes, DeltaStartingVersion::Snapshot) => DeltaPhase::Snapshot {
                version: latest,
                files_done: 0,
                last_file: None,
            },
            (DeltaReadMode::Changes, DeltaStartingVersion::Latest) => DeltaPhase::Changes {
                next_version: after_latest,
            },
            (DeltaReadMode::Changes, DeltaStartingVersion::Version(version)) => {
                if version > after_latest {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "starting.version {version} is beyond the table's next version \
                         {after_latest}"
                    )));
                }
                DeltaPhase::Changes {
                    next_version: version,
                }
            }
        };
        Ok(DeltaCursor { table_id, phase })
    }

    fn validate_recovered(
        &self,
        cursor: DeltaCursor,
        table_id: &str,
    ) -> Result<DeltaCursor, ConnectorError> {
        if cursor.table_id != table_id {
            return Err(ConnectorError::ConfigurationError(
                "the Delta table was replaced since this checkpoint (its table id changed); \
                 the old cursor is not valid for the new table — reset the source"
                    .into(),
            ));
        }
        let compatible = match (&cursor.phase, self.config.read_mode) {
            (DeltaPhase::Snapshot { .. }, _)
            | (DeltaPhase::Changes { .. }, DeltaReadMode::Changes)
            | (DeltaPhase::Complete { .. }, DeltaReadMode::Snapshot) => true,
            (DeltaPhase::Changes { .. }, DeltaReadMode::Snapshot)
            | (DeltaPhase::Complete { .. }, DeltaReadMode::Changes) => false,
        };
        if !compatible {
            return Err(ConnectorError::ConfigurationError(format!(
                "checkpointed Delta position {:?} does not belong to read.mode={:?}",
                cursor.phase, self.config.read_mode
            )));
        }
        Ok(cursor)
    }

    fn after_snapshot(&self, version: u64) -> Result<DeltaPhase, ConnectorError> {
        Ok(match self.config.read_mode {
            DeltaReadMode::Snapshot => DeltaPhase::Complete { version },
            DeltaReadMode::Changes => DeltaPhase::Changes {
                next_version: version
                    .checked_add(1)
                    .ok_or_else(|| ConnectorError::ReadError("Delta version overflowed".into()))?,
            },
        })
    }

    fn reader_mut(&mut self) -> Result<&mut Reader, ConnectorError> {
        self.reader
            .as_mut()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "started Delta source".into(),
                actual: "reader not open".into(),
            })
    }

    fn cursor(&self) -> Result<&DeltaCursor, ConnectorError> {
        self.cursor
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "installed Delta cursor".into(),
                actual: "cursor missing".into(),
            })
    }

    /// Emit at most `max_records` rows of the received batch. Every fallible step precedes the
    /// cursor and unit bookkeeping, so a failed emission leaves the checkpointable state intact.
    fn emit_pending(&mut self, max_records: usize) -> Result<Option<SourceBatch>, ConnectorError> {
        let Some(mut pending) = self.reader_mut()?.pending.take() else {
            return Ok(None);
        };
        let first_row = match self.open_unit {
            Some(open) if open.unit == pending.unit => open.rows_emitted,
            Some(_) => {
                return Err(ConnectorError::Internal(
                    "Delta reader started a replay unit before ending the previous one".into(),
                ))
            }
            None => 0,
        };
        let rows = pending.batch.num_rows();
        let take = rows.min(max_records);
        let emitted = positioned(pending.batch.slice(0, take), pending.unit, first_row)?;
        let rows_emitted = u32::try_from(take)
            .ok()
            .and_then(|take| first_row.checked_add(take))
            .ok_or_else(|| {
                ConnectorError::ReadError(
                    "one Delta commit or file exceeds the u32 row-position range".into(),
                )
            })?;

        if take < rows {
            pending.batch = pending.batch.slice(take, rows - take);
            self.open_unit = Some(OpenUnit {
                unit: pending.unit,
                rows_emitted,
            });
            self.reader_mut()?.pending = Some(pending);
        } else if let Some(phase) = pending.completes.take() {
            self.complete_unit(phase)?;
        } else {
            self.open_unit = Some(OpenUnit {
                unit: pending.unit,
                rows_emitted,
            });
        }
        self.records_read = self.records_read.saturating_add(take as u64);
        Ok(Some(emitted.with_checkpoint(self.checkpoint())))
    }

    fn complete_unit(&mut self, phase: DeltaPhase) -> Result<(), ConnectorError> {
        let cursor = self
            .cursor
            .as_mut()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "installed Delta cursor".into(),
                actual: "cursor missing".into(),
            })?;
        cursor.phase = phase;
        self.open_unit = None;
        Ok(())
    }

    /// The next reader message, or `None` when no read is running. A failure after a unit's
    /// rows were partly emitted retires this generation; earlier failures may be retried.
    async fn next_output(&mut self) -> Result<Option<reader::ScanOutput>, ConnectorError> {
        let reader = self.reader_mut()?;
        let Some(scan) = reader.scan.as_mut() else {
            return Ok(None);
        };
        let received = scan.receiver.recv().await;
        if !matches!(received, Some(Ok(_))) {
            reader.scan = None;
        }
        match received {
            Some(Ok(output)) => Ok(Some(output)),
            Some(Err(error)) => Err(self.fail_read(error)),
            None if self.open_unit.is_some() => Err(self.fail_read(ConnectorError::ReadError(
                "Delta reader stopped inside a replay unit".into(),
            ))),
            None => Ok(None),
        }
    }

    fn fail_read(&mut self, error: ConnectorError) -> ConnectorError {
        if self.open_unit.is_none() {
            return error;
        }
        self.state = ConnectorState::Failed;
        ConnectorError::TransactionError(format!("{PARTIAL_UNIT_ERROR}: {error}"))
    }

    /// Start reading the unit(s) after the cursor. Returns `false` when there is nothing to read.
    async fn start_next_scan(&mut self) -> Result<bool, ConnectorError> {
        let phase = self.cursor()?.phase.clone();
        if !matches!(phase, DeltaPhase::Complete { .. })
            && super::delta_io::credentials_need_refresh(
                self.reader_mut()?.resolved.credentials_expire_at,
            )
        {
            self.renew_credentials().await?;
        }
        let guard = self.task_owner.track().ok_or(ConnectorError::Closed)?;
        match phase {
            DeltaPhase::Complete { .. } => Ok(false),
            DeltaPhase::Snapshot {
                version,
                files_done,
                last_file,
            } => {
                let after = self.after_snapshot(version)?;
                let reader = self.reader_mut()?;
                reader.scan = Some(reader::spawn_snapshot(
                    reader.context.clone(),
                    guard,
                    version,
                    files_done,
                    last_file,
                    after,
                ));
                Ok(true)
            }
            DeltaPhase::Changes { next_version } => {
                let poll_interval = self.config.poll_interval;
                if self
                    .reader_mut()?
                    .last_version_check
                    .is_some_and(|checked| checked.elapsed() < poll_interval)
                {
                    return Ok(false);
                }
                let latest = self.refresh_table(next_version).await?;
                if latest < next_version {
                    return Ok(false);
                }
                let end = next_version
                    .saturating_add(MAX_COMMITS_PER_SCAN - 1)
                    .min(latest);
                let reader = self.reader_mut()?;
                reader.scan = Some(reader::spawn_changes(
                    reader.context.clone(),
                    guard,
                    next_version,
                    end,
                ));
                Ok(true)
            }
        }
    }

    /// Discover the latest committed version and, when it moved, confirm the table is the same
    /// incarnation with the bound schema before any of its new commits are read.
    async fn refresh_table(&mut self, next_version: u64) -> Result<u64, ConnectorError> {
        use super::delta_io;

        let table_id = self.cursor()?.table_id.clone();
        let reader = self.reader_mut()?;
        reader.last_version_check = Some(Instant::now());
        // The cheap incremental listing only answers "is there anything new"; it cannot see a
        // table recreated at the same location, whose log restarts at version 0.
        let listed = delta_io::get_latest_version(&mut reader.table).await;
        if let Ok(latest) = &listed {
            let latest = delta_io::to_delta_version(*latest)?;
            if latest < next_version {
                return Ok(latest);
            }
        }
        // New commits (or an unreadable listing): load the metadata from scratch, so a
        // replacement's own metaData action is the one compared with the cursor.
        let fresh = delta_io::open_table_metadata(
            &reader.resolved.location,
            reader.resolved.storage_options.clone(),
        )
        .await?;
        ensure_same_table(&fresh, &table_id)?;
        listed?;
        let latest = fresh.version().ok_or_else(|| {
            ConnectorError::ReadError("opened Delta table has no committed version".into())
        })?;
        if latest
            .checked_add(1)
            .is_none_or(|after| after < next_version)
        {
            return Err(ConnectorError::ConfigurationError(format!(
                "the Delta log regressed to version {latest} behind the cursor at {next_version}; \
                 the table was replaced or its history rewritten — reset the source"
            )));
        }
        reader.table = fresh;
        if latest < next_version {
            return Ok(latest);
        }
        let current = super::schema_resolution::delta_read_schema(&delta_io::get_table_schema(
            &reader.table,
        )?);
        let bound = self
            .schema_binding
            .as_ref()
            .and_then(|binding| binding.external.as_ref());
        if bound.is_some_and(|bound| bound != current.as_ref()) {
            return Err(ConnectorError::SchemaMismatch(format!(
                "the Delta table schema changed by version {latest}; the source binding is \
                 fixed — migrate the pipeline (DROP/CREATE) to read the new schema"
            )));
        }
        Ok(latest)
    }
}

#[cfg(feature = "delta-lake")]
fn ensure_same_table(table: &DeltaTable, table_id: &str) -> Result<(), ConnectorError> {
    let snapshot = table.snapshot().map_err(|error| {
        super::delta_io::classify_delta_metadata_error("read Delta table metadata", &error)
    })?;
    if snapshot.metadata().id() != table_id {
        return Err(ConnectorError::ConfigurationError(
            "the Delta table was replaced at its location (its table id changed); reset the \
             source to read the new table"
                .into(),
        ));
    }
    Ok(())
}

/// Attach replay positions: an empty partition (one channel), the unit's order key, and the
/// row's ordinal within its unit.
#[cfg(feature = "delta-lake")]
fn positioned(
    batch: RecordBatch,
    unit: reader::UnitKey,
    first_row: u32,
) -> Result<SourceBatch, ConnectorError> {
    let rows = batch.num_rows();
    let end = u32::try_from(rows)
        .ok()
        .and_then(|rows| first_row.checked_add(rows))
        .ok_or_else(|| {
            ConnectorError::ReadError(
                "one Delta commit or file exceeds the u32 row-position range".into(),
            )
        })?;
    let key = unit.order_key();
    let positions = crate::connector::SourceRowPositions::try_new(
        BinaryArray::from_iter_values(std::iter::repeat_n(&[][..], rows)),
        BinaryArray::from_iter_values(std::iter::repeat_n(&key[..], rows)),
        UInt32Array::from_iter_values(first_row..end),
    )?;
    SourceBatch::positioned(batch, positions)
}

#[async_trait]
impl SourceConnector for DeltaSource {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        if config.properties().is_empty() {
            self.config.validate()?;
        } else {
            DeltaSourceConfig::from_config(config)?.validate()?;
        }
        // Replay re-reads complete commits or snapshot files from the checkpointed cursor in
        // the kernel's sequential order; nothing is acknowledged upstream.
        Ok(SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Singleton,
            SourceInputMode::FullChangelog,
        )
        .with_row_positions(SourceRowPositionCapability::OrderedDeterministic))
    }

    async fn resolve_schema(
        &mut self,
        config: &ConnectorConfig,
        explicit: Option<SchemaRef>,
    ) -> Result<crate::schema::resolution::SchemaBinding, ConnectorError> {
        #[cfg(feature = "delta-lake")]
        {
            super::schema_resolution::delta_source(config, explicit).await
        }
        #[cfg(not(feature = "delta-lake"))]
        {
            let _ = (config, explicit);
            Err(ConnectorError::FeatureUnsupported(
                "Delta schema discovery requires the delta-lake feature".into(),
            ))
        }
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (config, position, _) = request.into_parts();
        let recovered = match &position {
            SourcePosition::Initialized { .. } => {
                return Err(ConnectorError::ConfigurationError(
                    "Delta Lake has no sealed topology startup contract".into(),
                ));
            }
            SourcePosition::Initial => None,
            SourcePosition::Resume { checkpoint, .. } => {
                Some(DeltaCursor::from_checkpoint(checkpoint)?)
            }
        };
        if !config.properties().is_empty() {
            self.config = DeltaSourceConfig::from_config(&config)?;
        }
        self.config.validate()?;
        self.state = ConnectorState::Initializing;

        #[cfg(feature = "delta-lake")]
        {
            if let Err(error) = self.open(&config, recovered).await {
                self.state = ConnectorState::Failed;
                return Err(error);
            }
            self.state = ConnectorState::Running;
            Ok(())
        }

        #[cfg(not(feature = "delta-lake"))]
        {
            let _ = (config, recovered);
            self.state = ConnectorState::Failed;
            Err(ConnectorError::ConfigurationError(
                "Delta Lake source requires the 'delta-lake' feature to be enabled. \
                 Build with: cargo build --features delta-lake"
                    .into(),
            ))
        }
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if max_records == 0 {
            return Err(ConnectorError::ConfigurationError(
                "Delta source max_records must be greater than zero".into(),
            ));
        }
        match self.state {
            ConnectorState::Running => {}
            ConnectorState::Failed if cfg!(feature = "delta-lake") => {
                return Err(ConnectorError::TransactionError(PARTIAL_UNIT_ERROR.into()));
            }
            state => {
                return Err(ConnectorError::InvalidState {
                    expected: "Running".into(),
                    actual: state.to_string(),
                });
            }
        }

        #[cfg(feature = "delta-lake")]
        loop {
            if let Some(batch) = self.emit_pending(max_records)? {
                return Ok(Some(batch));
            }
            if let Some(output) = self.next_output().await? {
                match output {
                    reader::ScanOutput::Batch {
                        batch,
                        unit,
                        permit,
                        completes,
                    } => {
                        self.reader_mut()?.pending = Some(Pending {
                            batch,
                            unit,
                            completes,
                            _permit: permit,
                        });
                    }
                    reader::ScanOutput::UnitEnd(phase) => {
                        if self.open_unit.is_some() {
                            return Err(ConnectorError::Internal(
                                "Delta reader ended a replay unit without its final rows".into(),
                            ));
                        }
                        self.complete_unit(phase)?;
                    }
                }
                continue;
            }
            if !self.start_next_scan().await? {
                return Ok(None);
            }
        }

        #[cfg(not(feature = "delta-lake"))]
        Err(ConnectorError::FeatureUnsupported(
            "Delta Lake source requires the 'delta-lake' feature".into(),
        ))
    }

    fn schema(&self) -> SchemaRef {
        self.schema
            .clone()
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()))
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        self.cursor
            .as_ref()
            .map_or_else(SourceCheckpoint::new, DeltaCursor::to_checkpoint)
    }

    fn try_checkpoint(&self) -> Result<Option<SourceCheckpoint>, ConnectorError> {
        #[cfg(feature = "delta-lake")]
        if self.open_unit.is_some() {
            return Ok(None);
        }
        Ok(Some(self.checkpoint()))
    }

    fn checkpoint_unavailable_policy(&self) -> SourceCheckpointUnavailablePolicy {
        SourceCheckpointUnavailablePolicy::PollToReplayBoundary
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing Delta Lake source connector");

        #[cfg(feature = "delta-lake")]
        if let Some(reader) = self.reader.take() {
            // Closing the budget wakes a reader blocked on it; dropping the receiver ends the
            // read at its next hand-off. The task tracker observes its termination.
            reader.context.budget.close();
            drop(reader);
            self.open_unit = None;
        }
        self.state = ConnectorState::Closed;
        info!(
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
            .field("table_path", &"<configured>")
            .field("cursor", &self.cursor)
            .field("records_read", &self.records_read)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
