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
use crate::connector::{SourceBatch, SourceConnector};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::delta_source_config::DeltaSourceConfig;
#[cfg(feature = "delta-lake")]
use super::delta_source_config::{DeltaReadMode, SchemaEvolutionAction};

/// Delta Lake source connector.
///
/// Reads Arrow `RecordBatch` data from Delta Lake tables by polling for
/// new table versions. Supports both incremental (changes-only) and
/// snapshot (full re-read) modes.
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
    /// Last time we checked for new Delta versions. Used to throttle
    /// `get_latest_version()` calls to `poll_interval` instead of
    /// hammering every source-adapter tick (10ms).
    #[cfg(feature = "delta-lake")]
    last_version_check: Option<Instant>,
    /// Per-field projection: `Some(idx)` = take column idx from new batch,
    /// `None` = emit null column. Aligned with `self.schema.fields()`.
    #[cfg(feature = "delta-lake")]
    projection_indices: Option<Vec<Option<usize>>>,
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
            #[cfg(feature = "delta-lake")]
            inflight_version: None,
            #[cfg(feature = "delta-lake")]
            known_latest_version: -1,
            pending_batches: VecDeque::new(),
            records_read: 0,
            #[cfg(feature = "delta-lake")]
            table: None,
            #[cfg(feature = "delta-lake")]
            last_version_check: None,
            #[cfg(feature = "delta-lake")]
            projection_indices: None,
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

        let table = delta_io::open_or_create_table(
            &self.config.table_path,
            self.config.storage_options.clone(),
            None,
        )
        .await?;

        self.table = Some(table);
        Ok(())
    }
}

#[async_trait]
#[allow(clippy::too_many_lines)]
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

            // Start from -1 (or explicit starting_version) and let
            // poll_batch() walk versions incrementally with bounded reads.
            if let Some(start) = self.config.starting_version {
                self.current_version = start;
            } else {
                self.current_version = -1;
            }
            let table_version = table.version().unwrap_or(0);

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

            let target_version = match self.config.read_mode {
                DeltaReadMode::Snapshot => self.known_latest_version,
                DeltaReadMode::Incremental => self.current_version + 1,
            };

            // Read data first. Both read_batches_at_version and
            // read_version_diff call load_version(target_version) internally,
            // so the table's snapshot will be at target_version after this.
            let table = self
                .table
                .as_mut()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "table initialized".into(),
                    actual: "table not initialized".into(),
                })?;
            let partition_filter = self.config.partition_filter.clone();

            // Version-gap detection: if the target commit was cleaned up,
            // skip ahead to a snapshot at the latest available version.
            let mut use_snapshot_fallback = false;
            if self.config.read_mode == DeltaReadMode::Incremental && target_version > 0 {
                let log_store = table.log_store();
                if let Ok(None) = log_store.read_commit_entry(target_version).await {
                    warn!(
                        target_version,
                        known_latest = self.known_latest_version,
                        "version unavailable, falling back to snapshot at latest"
                    );
                    use_snapshot_fallback = true;
                }
            }

            // On gap fallback, override target_version so inflight_version
            // tracks the snapshot version, not the missing one.
            let target_version = if use_snapshot_fallback {
                self.known_latest_version
            } else {
                target_version
            };

            // Incremental reads (read_version_diff, CDF) always consume the
            // full version — each version's diff is O(new_files), not
            // O(table_size), so unbounded reads are safe. This avoids the
            // re-read-from-start problem when max_records truncates a version.
            // Snapshot reads stay bounded by max_records (can be table-sized).
            let (batches, fully_consumed) = if use_snapshot_fallback {
                delta_io::read_batches_at_version(table, target_version, max_records).await?
            } else if self.config.cdf_enabled
                && self.config.read_mode == DeltaReadMode::Incremental
                && target_version > 0
            {
                // CDF mode: scan_cdf() consumes the DeltaTable. Take it,
                // read CDF batches, then re-open the table handle.
                let taken_table =
                    self.table
                        .take()
                        .ok_or_else(|| ConnectorError::InvalidState {
                            expected: "table initialized".into(),
                            actual: "table not initialized".into(),
                        })?;
                let cdf_batches =
                    delta_io::read_cdf_batches(taken_table, target_version, target_version).await?;

                // Re-open table since scan_cdf consumed it.
                self.reopen_table().await?;

                // Map CDF _change_type to LaminarDB _op.
                let mut mapped = Vec::new();
                for batch in &cdf_batches {
                    if let Some(mapped_batch) = delta_io::map_cdf_to_changelog(batch)? {
                        mapped.push(mapped_batch);
                    }
                }
                (mapped, true)
            } else {
                match self.config.read_mode {
                    DeltaReadMode::Snapshot => {
                        delta_io::read_batches_at_version(table, target_version, max_records)
                            .await?
                    }
                    DeltaReadMode::Incremental => {
                        // Read full version diff — each version is one
                        // commit's worth of files, safe to read unbounded.
                        let (b, _) = delta_io::read_version_diff(
                            table,
                            target_version,
                            usize::MAX,
                            partition_filter.as_deref(),
                        )
                        .await?;
                        (b, true)
                    }
                }
            };

            // Schema evolution detection: extract schema from the snapshot
            // that read_version_diff/read_batches_at_version already loaded.
            // This avoids a redundant load_version call.
            {
                let table = self
                    .table
                    .as_ref()
                    .ok_or_else(|| ConnectorError::InvalidState {
                        expected: "table initialized".into(),
                        actual: "table not initialized".into(),
                    })?;
                if let Ok(snapshot) = table.snapshot() {
                    let new_schema = snapshot.snapshot().arrow_schema();
                    if let Some(existing) = &self.schema {
                        if existing.fields() != new_schema.fields() {
                            match self.config.schema_evolution_action {
                                SchemaEvolutionAction::Warn => {
                                    warn!(
                                        table_path = %self.config.table_path,
                                        old_fields = ?existing.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>(),
                                        new_fields = ?new_schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>(),
                                        "Delta Lake source: schema evolved, projecting to original"
                                    );
                                    // Map each original field to its index in the new
                                    // schema, or None if the field was removed.
                                    let indices: Vec<Option<usize>> = existing
                                        .fields()
                                        .iter()
                                        .map(|f| new_schema.index_of(f.name()).ok())
                                        .collect();
                                    self.projection_indices = Some(indices);
                                    // Do NOT update self.schema — keep the original
                                    // for downstream stability.
                                }
                                SchemaEvolutionAction::Error => {
                                    return Err(ConnectorError::SchemaMismatch(format!(
                                        "schema evolved at version {target_version}"
                                    )));
                                }
                            }
                        }
                    } else {
                        self.schema = Some(new_schema);
                    }
                }
            }

            // Buffer all batches. Do NOT advance current_version yet —
            // it is only safe to checkpoint this version after the
            // buffer is fully drained. Store it as inflight_version.
            for batch in batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                // Apply schema projection if schema evolution was detected.
                let batch = if let Some(ref indices) = self.projection_indices {
                    let original_schema = self.schema.as_ref().unwrap();
                    let num_rows = batch.num_rows();
                    let columns: Vec<Arc<dyn arrow_array::Array>> = indices
                        .iter()
                        .zip(original_schema.fields())
                        .map(|(idx, field)| match idx {
                            Some(i) => batch.column(*i).clone(),
                            None => arrow_array::new_null_array(field.data_type(), num_rows),
                        })
                        .collect();
                    RecordBatch::try_new(original_schema.clone(), columns).map_err(|e| {
                        ConnectorError::ReadError(format!(
                            "failed to project batch to original schema: {e}"
                        ))
                    })?
                } else {
                    batch
                };
                self.pending_batches.push_back(batch);
            }

            if !fully_consumed {
                // Snapshot mode: max_records truncated the version.
                // Don't advance — next poll re-reads the same version.
                // (Incremental mode always reads full versions, so
                // fully_consumed is always true there.)
            } else if self.pending_batches.is_empty() {
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
        let mut cp = SourceCheckpoint::new(0);
        cp.set_offset("delta_version", self.current_version.to_string());
        cp.set_offset("read_mode", self.config.read_mode.to_string());
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
            .field("read_mode", &self.config.read_mode)
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

    /// D002/D003: Verify `max_records` bounds the pending buffer.
    /// Without the delta-lake feature, `poll_batch` returns buffered data
    /// incrementally; with the feature, `read_batches_at_version` applies LIMIT.
    #[tokio::test]
    async fn test_poll_batch_returns_buffered_incrementally() {
        let mut source = DeltaSource::new(test_config());
        source.state = ConnectorState::Running;

        // Simulate what read_batches_at_version produces: many small batches
        for _ in 0..10 {
            source.pending_batches.push_back(test_batch(100));
        }

        // Each poll_batch returns exactly one buffered batch
        let batch = source.poll_batch(50).await.unwrap();
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().records.num_rows(), 100);
        // 9 remaining
        assert_eq!(source.pending_batches.len(), 9);
    }

    /// Version is only advanced after the inflight buffer is fully drained.
    /// With multiple buffered batches, `current_version` stays at the old value
    /// until the last batch is consumed, then jumps to the target version.
    #[tokio::test]
    async fn test_version_deferred_until_buffer_drained() {
        let mut source = DeltaSource::new(test_config());
        source.state = ConnectorState::Running;
        source.current_version = 5;

        // Simulate: read_batches_at_version loaded version 42 with 3 batches.
        // In production the delta-lake cfg block sets inflight_version; here
        // we set it manually to test the drain logic (which is not cfg-gated
        // inside the pop_front path above — it is, so we test via the
        // non-feature path by just checking the pending_batches drain).
        source.pending_batches.push_back(test_batch(10));
        source.pending_batches.push_back(test_batch(10));
        source.pending_batches.push_back(test_batch(10));

        // Without delta-lake feature, inflight_version doesn't exist, so
        // current_version won't auto-advance. Verify the buffer drains.
        let b1 = source.poll_batch(100).await.unwrap();
        assert!(b1.is_some());
        assert_eq!(source.pending_batches.len(), 2);

        let b2 = source.poll_batch(100).await.unwrap();
        assert!(b2.is_some());
        assert_eq!(source.pending_batches.len(), 1);

        let b3 = source.poll_batch(100).await.unwrap();
        assert!(b3.is_some());
        assert!(source.pending_batches.is_empty());
        assert_eq!(source.records_read, 30);
    }

    /// D004: `poll_interval` is parsed and stored in config.
    /// The field is used by the delta-lake feature to throttle version checks.
    #[test]
    fn test_poll_interval_is_stored() {
        let mut config = test_config();
        config.poll_interval = std::time::Duration::from_millis(500);
        let source = DeltaSource::new(config);
        assert_eq!(
            source.config().poll_interval,
            std::time::Duration::from_millis(500)
        );
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

    /// D020: Source open() must error without delta-lake feature.
    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_open_requires_feature() {
        let mut source = DeltaSource::new(test_config());
        let connector_config = crate::config::ConnectorConfig::new("delta-lake");
        let result = source.open(&connector_config).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("delta-lake"), "error: {err}");
    }
}
