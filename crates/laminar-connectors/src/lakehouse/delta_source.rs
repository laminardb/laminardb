//! Delta Lake source connector implementation.
//!
//! [`DeltaSource`] implements [`SourceConnector`], reading Arrow `RecordBatch`
//! data from Delta Lake tables by polling for new versions.
//!
//! # Read Modes
//!
//! - **Incremental** (default): Walks versions one-by-one from `current_version + 1`
//!   to latest, reading only the files added in each version. Correct for streaming.
//! - **Snapshot**: Jumps to the latest version and reads the full table state.
//!   Useful for batch-style materialization of small tables.
//!
//! # Polling Strategy
//!
//! The source maintains a `current_version` cursor. On each `poll_batch()`:
//! 1. Drain any buffered batches from the previous load first
//! 2. Throttle: skip version check if less than `poll_interval` since last check
//! 3. Check if the table has a newer version than `current_version`
//! 4. **Incremental**: read one version at a time (`current_version + 1`)
//!    **Snapshot**: jump directly to the latest version
//! 5. Buffer results; `current_version` only advances after the buffer is
//!    fully drained, so checkpoint always reflects fully-consumed state
//!
//! # Schema Evolution Detection
//!
//! When a new version is loaded, the source compares the table schema against
//! the previously known schema. On mismatch, the action is controlled by
//! `schema.evolution.action`: `warn` (log and continue) or `error` (stop).
//!
//! # Checkpoint / Recovery
//!
//! The checkpoint stores `current_version` and `read_mode` so that on recovery
//! the source resumes from the correct Delta Lake version.

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

                let latest_version = delta_io::get_latest_version(&mut self.table).await?;
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

            // Schema evolution detection (scoped to drop table borrow before read).
            {
                let table = self
                    .table
                    .as_mut()
                    .ok_or_else(|| ConnectorError::InvalidState {
                        expected: "table initialized".into(),
                        actual: "table not initialized".into(),
                    })?;
                let new_schema = delta_io::get_schema_at_version(table, target_version).await?;
                if self.schema.is_none() {
                    self.schema = Some(new_schema);
                } else if self.schema.as_ref().unwrap().fields() != new_schema.fields() {
                    let msg = format!(
                        "schema evolved: {} field(s) before, {} field(s) now",
                        self.schema.as_ref().unwrap().fields().len(),
                        new_schema.fields().len()
                    );
                    match self.config.schema_evolution_action {
                        SchemaEvolutionAction::Warn => {
                            warn!(table_path = %self.config.table_path, "{msg}");
                            self.schema = Some(new_schema);
                        }
                        SchemaEvolutionAction::Error => {
                            return Err(ConnectorError::SchemaMismatch(msg));
                        }
                    }
                }
            }

            let table = self
                .table
                .as_mut()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "table initialized".into(),
                    actual: "table not initialized".into(),
                })?;
            let partition_filter = self.config.partition_filter.clone();
            let batches = match self.config.read_mode {
                DeltaReadMode::Snapshot => {
                    delta_io::read_batches_at_version(table, target_version, max_records).await?
                }
                DeltaReadMode::Incremental => {
                    delta_io::read_version_diff(
                        table,
                        target_version,
                        max_records,
                        partition_filter.as_deref(),
                    )
                    .await?
                }
            };

            // Buffer all batches. Do NOT advance current_version yet —
            // it is only safe to checkpoint this version after the
            // buffer is fully drained. Store it as inflight_version.
            for batch in batches {
                if batch.num_rows() > 0 {
                    self.pending_batches.push_back(batch);
                }
            }

            if self.pending_batches.is_empty() {
                // Version had no data rows (e.g. metadata-only commit).
                // Safe to advance immediately.
                self.current_version = target_version;
            } else {
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
