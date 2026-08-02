//! Delta Lake source connector.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
#[cfg(feature = "delta-lake")]
use std::time::Instant;
#[cfg(feature = "delta-lake")]
use tracing::debug;
use tracing::info;
#[cfg(feature = "delta-lake")]
use tracing::warn;

#[cfg(feature = "delta-lake")]
use deltalake::DeltaTable;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceInputMode,
    SourceTopology,
};
use crate::connector::{SourcePosition, SourceStart};
use crate::error::ConnectorError;

use super::delta_source_config::DeltaSourceConfig;

#[cfg(feature = "delta-lake")]
const MAX_CDF_COMMIT_ROWS: usize = 262_144;
#[cfg(feature = "delta-lake")]
const MAX_CDF_COMMIT_BYTES: usize = 64 * 1024 * 1024;

/// Delta Lake source connector.
///
/// Reads incremental Change Data Feed commits from Delta Lake tables.
///
/// # Lifecycle
///
/// ```text
/// new() -> start() -> [poll_batch()]* -> close()
///                           |
///                      checkpoint()
/// ```
pub struct DeltaSource {
    /// Source configuration.
    config: DeltaSourceConfig,
    /// Connector lifecycle state.
    state: ConnectorState,
    /// Arrow schema (set from table metadata on start).
    schema: Option<SchemaRef>,
    /// Current Delta Lake version cursor — the last *fully consumed* version.
    /// Only advanced after all buffered batches for a version are drained.
    current_version: i64,
    /// The version currently being drained. While `pending_batches` is
    /// non-empty this holds the version they came from. Once drained,
    /// `current_version` is advanced to this value and the field is cleared.
    #[cfg(feature = "delta-lake")]
    inflight_version: Option<i64>,
    /// The latest version known at the table. Used in incremental mode to
    /// walk versions one-by-one without re-calling `get_latest_version` for
    /// each step.
    #[cfg(feature = "delta-lake")]
    known_latest_version: i64,
    /// Buffered batches from the last version load.
    pending_batches: VecDeque<RecordBatch>,
    /// Total records read so far.
    records_read: u64,
    /// Delta Lake table handle.
    #[cfg(feature = "delta-lake")]
    table: Option<DeltaTable>,
    /// Catalog-resolved location used by initial open and subsequent reopens.
    #[cfg(feature = "delta-lake")]
    resolved_table_path: String,
    /// Explicit options paired with the catalog-resolved location.
    #[cfg(feature = "delta-lake")]
    stable_storage_options: std::collections::HashMap<String, String>,
    /// Last time we checked for new Delta versions. Used to throttle
    /// `get_latest_version()` calls to `poll_interval` instead of
    /// hammering every source-adapter tick (10ms).
    #[cfg(feature = "delta-lake")]
    last_version_check: Option<Instant>,
}

#[cfg(feature = "delta-lake")]
fn initial_current_version(starting_version: Option<i64>, latest_version: i64) -> i64 {
    starting_version.map_or(latest_version, |first_version| first_version - 1)
}

#[cfg(feature = "delta-lake")]
fn cdf_output_matches(expected: &SchemaRef, batch: &RecordBatch) -> bool {
    use laminar_core::changelog::WEIGHT_COLUMN;

    let actual = batch.schema();
    actual.fields().len() == expected.fields().len() + 1
        && actual
            .fields()
            .iter()
            .take(expected.fields().len())
            .eq(expected.fields().iter())
        && actual
            .fields()
            .last()
            .is_some_and(|field| field.name() == WEIGHT_COLUMN)
}

impl DeltaSource {
    /// Creates a new Delta Lake source with the given configuration.
    #[must_use]
    pub fn new(config: DeltaSourceConfig, _registry: Option<&prometheus::Registry>) -> Self {
        Self {
            config,
            state: ConnectorState::Created,
            schema: None,
            current_version: -1,
            #[cfg(feature = "delta-lake")]
            inflight_version: None,
            #[cfg(feature = "delta-lake")]
            known_latest_version: -1,
            pending_batches: VecDeque::new(),
            records_read: 0,
            #[cfg(feature = "delta-lake")]
            table: None,
            #[cfg(feature = "delta-lake")]
            resolved_table_path: String::new(),
            #[cfg(feature = "delta-lake")]
            stable_storage_options: std::collections::HashMap::new(),
            #[cfg(feature = "delta-lake")]
            last_version_check: None,
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

    /// Re-opens the Delta Lake table (e.g., after a connection failure).
    #[cfg(feature = "delta-lake")]
    async fn reopen_table(&mut self) -> Result<(), ConnectorError> {
        use super::delta_io;

        if self.resolved_table_path.is_empty() {
            return Err(ConnectorError::InvalidState {
                expected: "catalog-resolved table location".into(),
                actual: "table location not resolved".into(),
            });
        }
        let storage_options = crate::storage::StorageCredentialResolver::resolve(
            &self.resolved_table_path,
            &self.stable_storage_options,
        )
        .options;
        let table =
            delta_io::open_or_create_table(&self.resolved_table_path, storage_options, None)
                .await?;

        self.table = Some(table);
        Ok(())
    }
}

#[async_trait]
#[allow(clippy::too_many_lines)]
impl SourceConnector for DeltaSource {
    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        if config.properties().is_empty() {
            self.config.validate()?;
        } else {
            DeltaSourceConfig::from_config(config)?.validate()?;
        }

        Ok(SourceContract::new(
            SourceConsistency::Ephemeral,
            SourceTopology::Singleton,
            SourceInputMode::FullChangelog,
        ))
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (config, position, _) = request.into_parts();
        if let SourcePosition::Resume { attempt, .. } = position {
            return Err(ConnectorError::ConfigurationError(format!(
                "Delta Lake is an ephemeral source and cannot resume checkpoint attempt {attempt:?}"
            )));
        }
        let config = &config;

        if !config.properties().is_empty() {
            self.config = DeltaSourceConfig::from_config(config)?;
        }
        self.config.validate()?;
        self.state = ConnectorState::Initializing;

        info!(
            table_path = %self.config.table_path,
            starting_version = ?self.config.starting_version,
            "opening Delta Lake source connector"
        );

        #[cfg(feature = "delta-lake")]
        {
            use super::delta_io;

            let stable_options = self.config.stable_storage_options();
            let (resolved_path, stable_options) = delta_io::resolve_catalog_options(
                &self.config.catalog_type,
                self.config.catalog_database.as_deref(),
                self.config.catalog_name.as_deref(),
                self.config.catalog_schema.as_deref(),
                &self.config.table_path,
                &stable_options,
            )
            .await?;
            let resolved_options =
                crate::storage::StorageCredentialResolver::resolve(&resolved_path, &stable_options)
                    .options;

            let table =
                delta_io::open_or_create_table(&resolved_path, resolved_options, None).await?;

            self.schema = Some(delta_io::get_table_schema(&table)?);
            let table_version = table.version().ok_or_else(|| {
                ConnectorError::ReadError("opened Delta table has no committed version".into())
            })?;
            self.current_version =
                initial_current_version(self.config.starting_version, table_version);
            self.known_latest_version = table_version;
            self.last_version_check = Some(Instant::now());
            self.resolved_table_path = resolved_path;
            self.stable_storage_options = stable_options;

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
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ConfigurationError(
                "Delta Lake source requires the 'delta-lake' feature to be enabled. \
                 Build with: cargo build --features delta-lake"
                    .into(),
            ));
        }

        #[cfg(feature = "delta-lake")]
        {
            self.state = ConnectorState::Running;
            info!("Delta Lake source connector opened successfully");
            Ok(())
        }
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

        // Return buffered batches first. When the buffer drains
        // completely, advance current_version to the inflight version
        // so that checkpoint() reports the fully-consumed position.
        if let Some(batch) = self.pending_batches.pop_front() {
            self.records_read += batch.num_rows() as u64;

            #[cfg(feature = "delta-lake")]
            if self.pending_batches.is_empty() {
                if let Some(v) = self.inflight_version.take() {
                    self.current_version = v;
                }
            }

            return Ok(Some(SourceBatch::new(batch)));
        }

        // Check for new versions, throttled by poll_interval.
        #[cfg(feature = "delta-lake")]
        {
            use super::delta_io;

            // Recover from lost table handle (e.g., connection failure).
            if self.table.is_none() {
                match self.reopen_table().await {
                    Ok(()) => {
                        info!("Delta Lake source: re-opened table after lost handle");
                    }
                    Err(e) => {
                        warn!(error = %e, "Delta Lake source: reopen failed, will retry");
                        return Ok(None);
                    }
                }
            }

            // Throttle version checks: skip if less than poll_interval has
            // elapsed since the last check. This prevents hammering
            // get_latest_version() on every source-adapter tick (10ms).
            // In incremental mode, skip the throttle if we already know
            // there are more versions to process (catch-up).
            let needs_refresh = self.known_latest_version <= self.current_version;
            if needs_refresh {
                if let Some(last_check) = self.last_version_check {
                    if last_check.elapsed() < self.config.poll_interval {
                        return Ok(None);
                    }
                }
                self.last_version_check = Some(Instant::now());

                let table = self
                    .table
                    .as_mut()
                    .ok_or_else(|| ConnectorError::InvalidState {
                        expected: "table initialized".into(),
                        actual: "table not initialized".into(),
                    })?;
                let latest_version = match delta_io::get_latest_version(table).await {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(error = %e, "Delta Lake source: version check failed, will retry");
                        return Ok(None);
                    }
                };
                self.known_latest_version = latest_version;

                if latest_version <= self.current_version {
                    return Ok(None); // No new data
                }

                debug!(
                    current_version = self.current_version,
                    latest_version, "Delta Lake source: new version(s) available"
                );
            }

            let target_version = self.current_version.checked_add(1).ok_or_else(|| {
                ConnectorError::ConfigurationError("Delta source version cursor overflowed".into())
            })?;

            {
                let table = self
                    .table
                    .as_ref()
                    .ok_or_else(|| ConnectorError::InvalidState {
                        expected: "table initialized".into(),
                        actual: "table not initialized".into(),
                    })?;
                let log_store = table.log_store();
                match log_store.read_commit_entry(target_version).await {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        return Err(ConnectorError::ConfigurationError(format!(
                            "Delta commit {target_version} is unavailable; incremental streaming cannot fall back to a snapshot"
                        )));
                    }
                    Err(error) => {
                        return Err(ConnectorError::ReadError(format!(
                            "failed to verify Delta commit {target_version}: {error}"
                        )));
                    }
                }
            }

            let scan_table = self
                .table
                .as_ref()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "table initialized".into(),
                    actual: "table not initialized".into(),
                })?
                .clone();
            let cdf_batches = delta_io::read_cdf_batches(
                scan_table,
                target_version,
                target_version,
                MAX_CDF_COMMIT_ROWS,
                MAX_CDF_COMMIT_BYTES,
            )
            .await?;

            let expected_schema =
                self.schema
                    .as_ref()
                    .ok_or_else(|| ConnectorError::InvalidState {
                        expected: "source schema initialized".into(),
                        actual: "source schema missing".into(),
                    })?;
            let mut batches = Vec::with_capacity(cdf_batches.len());
            for batch in cdf_batches {
                let mapped = delta_io::map_cdf_to_changelog(&batch)?;
                if !cdf_output_matches(expected_schema, &mapped) {
                    return Err(ConnectorError::SchemaMismatch(format!(
                        "Delta CDF schema evolved at version {target_version}"
                    )));
                }
                batches.push(mapped);
            }

            // Buffer all batches. Do NOT advance current_version yet —
            // it is only safe to checkpoint this version after the
            // buffer is fully drained. Store it as inflight_version.
            for batch in batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                self.pending_batches.push_back(batch);
            }

            if self.pending_batches.is_empty() {
                // Version fully consumed with no data rows (metadata-only).
                self.current_version = target_version;
            } else {
                // Version fully consumed, batches buffered. Advance after drain.
                self.inflight_version = Some(target_version);
            }

            if let Some(batch) = self.pending_batches.pop_front() {
                self.records_read += batch.num_rows() as u64;

                // Single-batch version: buffer is already empty, advance now.
                if self.pending_batches.is_empty() {
                    if let Some(v) = self.inflight_version.take() {
                        self.current_version = v;
                    }
                }

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
        let mut cp = SourceCheckpoint::new();
        cp.set_offset("delta_version", self.current_version.to_string());
        cp
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
        let source = DeltaSource::new(test_config(), None);
        assert_eq!(source.state(), ConnectorState::Created);
        assert_eq!(source.current_version(), -1);
        assert!(source.schema.is_none());
    }

    #[test]
    fn cdf_contract_is_full_changelog() {
        let source = DeltaSource::new(test_config(), None);
        assert_eq!(
            source
                .contract(&ConnectorConfig::new("delta-lake"))
                .unwrap()
                .input_mode,
            SourceInputMode::FullChangelog
        );

        let mut config = ConnectorConfig::new("delta-lake");
        config.set("table.path", "/tmp/delta_source_test");
        config.set("read.mode", "snapshot");
        let error = source.contract(&config).unwrap_err();
        assert!(error.to_string().contains("read.mode"));
    }

    #[tokio::test]
    async fn start_rejects_removed_option_before_opening_table() {
        let mut source = DeltaSource::new(test_config(), None);
        let mut config = ConnectorConfig::new("delta-lake");
        config.set("table.path", "/tmp/delta_source_test");
        config.set("cdf.enabled", "true");
        let error = source
            .start(
                SourceStart::new(
                    config,
                    SourcePosition::Initial,
                    crate::connector::DeliveryGuarantee::BestEffort,
                )
                .unwrap(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("cdf.enabled"));
        assert_eq!(source.state(), ConnectorState::Created);
    }

    #[cfg(all(
        feature = "delta-lake",
        not(feature = "delta-lake-glue"),
        not(feature = "delta-lake-unity")
    ))]
    #[tokio::test]
    async fn start_routes_catalog_locations_through_the_resolver() {
        use super::super::delta_config::DeltaCatalogType;

        let mut glue_config = test_config();
        glue_config.catalog_type = DeltaCatalogType::Glue;
        glue_config.catalog_database = Some("analytics".into());
        let mut glue_source = DeltaSource::new(glue_config, None);
        let glue_error = glue_source
            .start(
                SourceStart::new(
                    ConnectorConfig::new("delta-lake"),
                    SourcePosition::Initial,
                    crate::connector::DeliveryGuarantee::BestEffort,
                )
                .unwrap(),
            )
            .await
            .unwrap_err();
        assert!(glue_error.to_string().contains("delta-lake-glue"));

        let mut unity_config = DeltaSourceConfig::new("uc://catalog.schema.events");
        unity_config.catalog_type = DeltaCatalogType::Unity {
            workspace_url: "https://workspace.example".into(),
            access_token: "test-token".into(),
        };
        unity_config.catalog_name = Some("catalog".into());
        unity_config.catalog_schema = Some("schema".into());
        let mut unity_source = DeltaSource::new(unity_config, None);
        let unity_error = unity_source
            .start(
                SourceStart::new(
                    ConnectorConfig::new("delta-lake"),
                    SourcePosition::Initial,
                    crate::connector::DeliveryGuarantee::BestEffort,
                )
                .unwrap(),
            )
            .await
            .unwrap_err();
        assert!(unity_error.to_string().contains("delta-lake-unity"));
    }

    #[cfg(feature = "delta-lake")]
    #[tokio::test]
    async fn reopen_uses_the_location_resolved_at_start() {
        use std::collections::HashMap;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let table_path = temp_dir.path().to_string_lossy().into_owned();
        super::super::delta_io::open_or_create_table(
            &table_path,
            HashMap::new(),
            Some(&test_schema()),
        )
        .await
        .unwrap();

        let mut source = DeltaSource::new(DeltaSourceConfig::new(&table_path), None);
        source
            .start(
                SourceStart::new(
                    ConnectorConfig::new("delta-lake"),
                    SourcePosition::Initial,
                    crate::connector::DeliveryGuarantee::BestEffort,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(source.resolved_table_path, table_path);

        source.table = None;
        source.config.table_path = temp_dir
            .path()
            .join("unresolved-location")
            .to_string_lossy()
            .into_owned();
        source.reopen_table().await.unwrap();
        assert_eq!(source.table.as_ref().and_then(DeltaTable::version), Some(0));
    }

    #[cfg(feature = "delta-lake")]
    #[tokio::test]
    async fn cdf_scan_retains_the_live_table_handle() {
        use deltalake::kernel::engine::arrow_conversion::TryIntoKernel as _;
        use deltalake::TableProperty;

        let schema = test_schema();
        let delta_schema: deltalake::kernel::StructType =
            schema.as_ref().try_into_kernel().unwrap();
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(delta_schema.fields().cloned())
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await
            .unwrap()
            .write(vec![test_batch(2)])
            .await
            .unwrap();
        assert_eq!(table.version(), Some(1));

        let mut source = DeltaSource::new(test_config(), None);
        source.state = ConnectorState::Running;
        source.schema = Some(super::super::delta_io::get_table_schema(&table).unwrap());
        source.current_version = 0;
        source.known_latest_version = 1;
        source.table = Some(table);

        let batch = source.poll_batch(100).await.unwrap().unwrap();
        assert_eq!(batch.records.num_rows(), 2);
        assert_eq!(source.table.as_ref().and_then(DeltaTable::version), Some(1));
    }

    #[cfg(feature = "delta-lake")]
    #[test]
    fn starting_version_is_the_first_version_read() {
        assert_eq!(initial_current_version(Some(5), 9), 4);
        assert_eq!(initial_current_version(None, 9), 9);
    }

    #[cfg(feature = "delta-lake")]
    #[tokio::test]
    async fn multi_batch_commit_drains_before_advancing_the_cursor() {
        let mut source = DeltaSource::new(test_config(), None);
        source.state = ConnectorState::Running;
        source.current_version = 6;
        source.inflight_version = Some(7);
        source.pending_batches.push_back(test_batch(1));
        source.pending_batches.push_back(test_batch(2));
        source.pending_batches.push_back(test_batch(3));

        assert!(source.checkpoint_ready().unwrap());

        assert_eq!(
            source
                .poll_batch(100)
                .await
                .unwrap()
                .unwrap()
                .records
                .num_rows(),
            1
        );
        assert_eq!(source.current_version(), 6);
        assert_eq!(source.checkpoint().get_offset("delta_version"), Some("6"));

        assert_eq!(
            source
                .poll_batch(100)
                .await
                .unwrap()
                .unwrap()
                .records
                .num_rows(),
            2
        );
        assert_eq!(source.current_version(), 6);

        assert_eq!(
            source
                .poll_batch(100)
                .await
                .unwrap()
                .unwrap()
                .records
                .num_rows(),
            3
        );
        assert_eq!(source.current_version(), 7);
        assert!(source.inflight_version.is_none());
        assert!(source.pending_batches.is_empty());
        assert_eq!(source.checkpoint().get_offset("delta_version"), Some("7"));
    }

    #[test]
    fn test_checkpoint_roundtrip() {
        let mut source = DeltaSource::new(test_config(), None);
        source.current_version = 42;

        let cp = source.checkpoint();
        assert_eq!(cp.get_offset("delta_version"), Some("42"));
    }

    #[tokio::test]
    async fn resume_fails_before_opening_the_ephemeral_source() {
        let mut source = DeltaSource::new(test_config(), None);
        let error = source
            .start(
                SourceStart::new(
                    ConnectorConfig::new("delta-lake"),
                    SourcePosition::Resume {
                        attempt: laminar_core::state::CheckpointAttempt::canonical(11),
                        checkpoint: SourceCheckpoint::new(),
                    },
                    crate::connector::DeliveryGuarantee::BestEffort,
                )
                .unwrap(),
            )
            .await
            .expect_err("ephemeral Delta source must reject recovery");
        assert!(error.to_string().contains("ephemeral"));
        assert_eq!(source.state(), ConnectorState::Created);
    }

    #[test]
    fn test_schema_empty_when_none() {
        let source = DeltaSource::new(test_config(), None);
        let schema = source.schema();
        assert_eq!(schema.fields().len(), 0);
    }

    #[tokio::test]
    async fn test_poll_not_running() {
        let mut source = DeltaSource::new(test_config(), None);
        // state is Created, not Running
        let result = source.poll_batch(100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_poll_returns_buffered_batches() {
        let mut source = DeltaSource::new(test_config(), None);
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
    fn test_poll_interval_is_stored() {
        let mut config = test_config();
        config.poll_interval = std::time::Duration::from_millis(500);
        let source = DeltaSource::new(config, None);
        assert_eq!(
            source.config().poll_interval,
            std::time::Duration::from_millis(500)
        );
    }

    #[test]
    fn test_debug_output() {
        let source = DeltaSource::new(test_config(), None);
        let debug = format!("{source:?}");
        assert!(debug.contains("DeltaSource"));
        assert!(debug.contains("/tmp/delta_source_test"));
    }

    #[tokio::test]
    async fn test_close() {
        let mut source = DeltaSource::new(test_config(), None);
        source.state = ConnectorState::Running;
        source.pending_batches.push_back(test_batch(5));

        source.close().await.unwrap();
        assert_eq!(source.state(), ConnectorState::Closed);
        assert!(source.pending_batches.is_empty());
    }

    /// D020: Source `start()` must error without delta-lake feature.
    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_open_requires_feature() {
        let mut source = DeltaSource::new(test_config(), None);
        let connector_config = crate::config::ConnectorConfig::new("delta-lake");
        let result = source
            .start(
                SourceStart::new(
                    connector_config,
                    SourcePosition::Initial,
                    crate::connector::DeliveryGuarantee::BestEffort,
                )
                .unwrap(),
            )
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("delta-lake"), "error: {err}");
    }
}
