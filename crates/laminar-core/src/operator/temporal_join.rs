//! # Temporal Join Operators (F021)
//!
//! Join streaming events with versioned tables using point-in-time lookups.
//! Temporal joins return the table value that was valid at the event's timestamp,
//! enabling consistent enrichment with time-varying dimension data.
//!
//! ## Use Cases
//!
//! - Currency rate lookup at transaction time
//! - Product price lookup at order time
//! - User tier lookup at event time
//! - Regulatory compliance (audit trail)
//!
//! ## Join Types
//!
//! - **Event-Time**: Deterministic lookup based on event timestamp (`FOR SYSTEM_TIME AS OF`)
//! - **Process-Time**: Non-deterministic lookup based on processing time (latest value)
//!
//! ## Table Characteristics
//!
//! - **Append-Only**: Only inserts, no updates/deletes. Optimized: no stream-side state needed.
//! - **Non-Append-Only**: Has updates/deletes. Requires state to track join results for retractions.
//!
//! ## Example
//!
//! ```rust,no_run
//! use laminar_core::operator::temporal_join::{
//!     TemporalJoinOperator, TemporalJoinConfig, TemporalJoinSemantics,
//!     TableCharacteristics, TemporalJoinType,
//! };
//!
//! // Join orders with currency rates valid at order time
//! let config = TemporalJoinConfig::builder()
//!     .stream_key_column("currency".to_string())
//!     .table_key_column("currency".to_string())
//!     .table_version_column("valid_from".to_string())
//!     .semantics(TemporalJoinSemantics::EventTime)
//!     .table_characteristics(TableCharacteristics::AppendOnly)
//!     .join_type(TemporalJoinType::Inner)
//!     .build();
//!
//! let operator = TemporalJoinOperator::new(config);
//! ```
//!
//! ## SQL Syntax (Future)
//!
//! ```sql
//! -- Event-time temporal join
//! SELECT o.*, r.rate
//! FROM orders o
//! JOIN currency_rates FOR SYSTEM_TIME AS OF o.order_time r
//!     ON o.currency = r.currency;
//!
//! -- Process-time temporal join (latest value)
//! SELECT o.*, c.tier
//! FROM orders o
//! JOIN customers FOR SYSTEM_TIME AS OF PROCTIME() c
//!     ON o.customer_id = c.id;
//! ```

use super::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output, OutputVec, Timer,
    TimerKey,
};
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use fxhash::FxHashMap;
use rkyv::{
    rancor::Error as RkyvError, Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize,
};
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Type of temporal join semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TemporalJoinSemantics {
    /// Event-time: Lookup value valid at event timestamp.
    /// Deterministic, requires versioned table.
    #[default]
    EventTime,

    /// Process-time: Lookup current value at processing time.
    /// Non-deterministic, simpler but less predictable.
    ProcessTime,
}

/// Table update characteristics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TableCharacteristics {
    /// Append-only: Only inserts, no updates/deletes.
    /// Optimization: No state needed on streaming side.
    #[default]
    AppendOnly,

    /// Non-append-only: Has updates and/or deletes.
    /// Requires state to track which rows were joined.
    NonAppendOnly,
}

/// Type of temporal join to perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TemporalJoinType {
    /// Inner join - only emit when a match is found.
    #[default]
    Inner,
    /// Left outer join - emit all stream events, with nulls for unmatched.
    Left,
}

impl TemporalJoinType {
    /// Returns true if unmatched stream events should be emitted.
    #[must_use]
    pub fn emits_unmatched(&self) -> bool {
        matches!(self, TemporalJoinType::Left)
    }
}

/// Configuration for a temporal join operator.
#[derive(Debug, Clone)]
pub struct TemporalJoinConfig {
    /// Column name in the stream to use as lookup key.
    pub stream_key_column: String,
    /// Column name in the table that matches the stream key.
    pub table_key_column: String,
    /// Column name for the version timestamp in the table.
    pub table_version_column: String,
    /// Join semantics (event-time or process-time).
    pub semantics: TemporalJoinSemantics,
    /// Table characteristics (append-only or non-append-only).
    pub table_characteristics: TableCharacteristics,
    /// Type of join to perform.
    pub join_type: TemporalJoinType,
    /// Operator ID for checkpointing.
    pub operator_id: Option<String>,
    /// Maximum number of versions to retain per key (0 = unlimited).
    pub max_versions_per_key: usize,
}

impl TemporalJoinConfig {
    /// Creates a new builder for temporal join configuration.
    #[must_use]
    pub fn builder() -> TemporalJoinConfigBuilder {
        TemporalJoinConfigBuilder::default()
    }
}

/// Builder for [`TemporalJoinConfig`].
#[derive(Debug, Default)]
pub struct TemporalJoinConfigBuilder {
    stream_key_column: Option<String>,
    table_key_column: Option<String>,
    table_version_column: Option<String>,
    semantics: Option<TemporalJoinSemantics>,
    table_characteristics: Option<TableCharacteristics>,
    join_type: Option<TemporalJoinType>,
    operator_id: Option<String>,
    max_versions_per_key: Option<usize>,
}

impl TemporalJoinConfigBuilder {
    /// Sets the stream key column name.
    #[must_use]
    pub fn stream_key_column(mut self, column: String) -> Self {
        self.stream_key_column = Some(column);
        self
    }

    /// Sets the table key column name.
    #[must_use]
    pub fn table_key_column(mut self, column: String) -> Self {
        self.table_key_column = Some(column);
        self
    }

    /// Sets the table version column name.
    #[must_use]
    pub fn table_version_column(mut self, column: String) -> Self {
        self.table_version_column = Some(column);
        self
    }

    /// Sets the join semantics.
    #[must_use]
    pub fn semantics(mut self, semantics: TemporalJoinSemantics) -> Self {
        self.semantics = Some(semantics);
        self
    }

    /// Sets the table characteristics.
    #[must_use]
    pub fn table_characteristics(mut self, characteristics: TableCharacteristics) -> Self {
        self.table_characteristics = Some(characteristics);
        self
    }

    /// Sets the join type.
    #[must_use]
    pub fn join_type(mut self, join_type: TemporalJoinType) -> Self {
        self.join_type = Some(join_type);
        self
    }

    /// Sets a custom operator ID.
    #[must_use]
    pub fn operator_id(mut self, id: String) -> Self {
        self.operator_id = Some(id);
        self
    }

    /// Sets the maximum number of versions to retain per key.
    #[must_use]
    pub fn max_versions_per_key(mut self, max: usize) -> Self {
        self.max_versions_per_key = Some(max);
        self
    }

    /// Builds the configuration.
    ///
    /// # Panics
    ///
    /// Panics if required fields are not set.
    #[must_use]
    pub fn build(self) -> TemporalJoinConfig {
        TemporalJoinConfig {
            stream_key_column: self
                .stream_key_column
                .expect("stream_key_column is required"),
            table_key_column: self
                .table_key_column
                .expect("table_key_column is required"),
            table_version_column: self
                .table_version_column
                .expect("table_version_column is required"),
            semantics: self.semantics.unwrap_or_default(),
            table_characteristics: self.table_characteristics.unwrap_or_default(),
            join_type: self.join_type.unwrap_or_default(),
            operator_id: self.operator_id,
            max_versions_per_key: self.max_versions_per_key.unwrap_or(0),
        }
    }
}

/// Timer key prefix for version cleanup.
const TEMPORAL_TIMER_PREFIX: u8 = 0x60;

/// Static counter for generating unique operator IDs.
static TEMPORAL_OPERATOR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A stored table row for temporal joining.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct TableRow {
    /// Version timestamp (when this row became valid).
    pub version_timestamp: i64,
    /// Serialized key value.
    pub key_value: Vec<u8>,
    /// Serialized record batch data.
    pub data: Vec<u8>,
}

impl TableRow {
    /// Creates a new table row from a record batch.
    fn new(
        version_timestamp: i64,
        key_value: Vec<u8>,
        batch: &RecordBatch,
    ) -> Result<Self, OperatorError> {
        let data = Self::serialize_batch(batch)?;
        Ok(Self {
            version_timestamp,
            key_value,
            data,
        })
    }

    /// Serializes a record batch to bytes.
    fn serialize_batch(batch: &RecordBatch) -> Result<Vec<u8>, OperatorError> {
        let mut buf = Vec::new();
        {
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
                .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;
            writer
                .write(batch)
                .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;
            writer
                .finish()
                .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;
        }
        Ok(buf)
    }

    /// Deserializes a record batch from bytes.
    fn deserialize_batch(data: &[u8]) -> Result<RecordBatch, OperatorError> {
        let cursor = std::io::Cursor::new(data);
        let mut reader = arrow_ipc::reader::StreamReader::try_new(cursor, None)
            .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;
        reader
            .next()
            .ok_or_else(|| OperatorError::SerializationFailed("Empty batch data".to_string()))?
            .map_err(|e| OperatorError::SerializationFailed(e.to_string()))
    }

    /// Converts this row back to a record batch.
    ///
    /// # Errors
    ///
    /// Returns `OperatorError::SerializationFailed` if the batch data is invalid.
    pub fn to_batch(&self) -> Result<RecordBatch, OperatorError> {
        Self::deserialize_batch(&self.data)
    }
}

/// Record of a joined event for retraction tracking (non-append-only tables).
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct JoinedEventRecord {
    /// Original stream event timestamp.
    pub event_timestamp: i64,
    /// Serialized stream event data.
    pub event_data: Vec<u8>,
    /// Table row version that was joined.
    pub table_version: i64,
    /// The key value used for joining.
    pub key_value: Vec<u8>,
}

/// Per-key versioned table state.
#[derive(Debug, Clone, Default)]
pub struct VersionedKeyState {
    /// Rows indexed by version timestamp.
    /// Multiple rows at the same timestamp stored in a vector.
    pub versions: BTreeMap<i64, SmallVec<[TableRow; 1]>>,
    /// Minimum version timestamp.
    pub min_version: i64,
    /// Maximum version timestamp.
    pub max_version: i64,
}

impl VersionedKeyState {
    /// Creates a new empty key state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            versions: BTreeMap::new(),
            min_version: i64::MAX,
            max_version: i64::MIN,
        }
    }

    /// Inserts a table row.
    pub fn insert(&mut self, row: TableRow) {
        let version = row.version_timestamp;
        self.versions.entry(version).or_default().push(row);
        self.min_version = self.min_version.min(version);
        self.max_version = self.max_version.max(version);
    }

    /// Returns the number of rows in this key's state.
    #[must_use]
    pub fn len(&self) -> usize {
        self.versions.values().map(SmallVec::len).sum()
    }

    /// Returns true if this key has no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.versions.is_empty()
    }

    /// Finds the row valid at the given timestamp.
    /// Returns the row with the largest `version_ts` <= `event_ts`.
    #[must_use]
    pub fn lookup_at_time(&self, timestamp: i64) -> Option<&TableRow> {
        let (_, rows) = self.versions.range(..=timestamp).next_back()?;
        rows.last()
    }

    /// Finds the latest row (for process-time lookups).
    #[must_use]
    pub fn lookup_latest(&self) -> Option<&TableRow> {
        let (_, rows) = self.versions.iter().next_back()?;
        rows.last()
    }

    /// Removes versions before the given timestamp.
    pub fn cleanup_before(&mut self, threshold: i64) {
        self.versions = self.versions.split_off(&threshold);
        self.min_version = self.versions.keys().next().copied().unwrap_or(i64::MAX);
    }

    /// Removes a specific version (for non-append-only table deletes).
    pub fn remove_version(&mut self, version: i64) -> Option<SmallVec<[TableRow; 1]>> {
        let removed = self.versions.remove(&version);
        if removed.is_some() {
            self.min_version = self.versions.keys().next().copied().unwrap_or(i64::MAX);
            self.max_version = self.versions.keys().next_back().copied().unwrap_or(i64::MIN);
        }
        removed
    }

    /// Limits the number of versions (keeps most recent).
    pub fn limit_versions(&mut self, max_versions: usize) {
        if max_versions == 0 || self.versions.len() <= max_versions {
            return;
        }

        let to_remove = self.versions.len() - max_versions;
        let keys_to_remove: Vec<_> = self.versions.keys().take(to_remove).copied().collect();
        for key in keys_to_remove {
            self.versions.remove(&key);
        }
        self.min_version = self.versions.keys().next().copied().unwrap_or(i64::MAX);
    }
}

/// Serializable version of `VersionedKeyState` for checkpointing.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct SerializableVersionedKeyState {
    versions: Vec<(i64, Vec<TableRow>)>,
    min_version: i64,
    max_version: i64,
}

impl From<&VersionedKeyState> for SerializableVersionedKeyState {
    fn from(state: &VersionedKeyState) -> Self {
        Self {
            versions: state
                .versions
                .iter()
                .map(|(ts, rows)| (*ts, rows.to_vec()))
                .collect(),
            min_version: state.min_version,
            max_version: state.max_version,
        }
    }
}

impl From<SerializableVersionedKeyState> for VersionedKeyState {
    fn from(state: SerializableVersionedKeyState) -> Self {
        let mut versions = BTreeMap::new();
        for (ts, rows) in state.versions {
            versions.insert(ts, SmallVec::from_vec(rows));
        }
        Self {
            versions,
            min_version: state.min_version,
            max_version: state.max_version,
        }
    }
}

/// Table change event for non-append-only tables.
#[derive(Debug, Clone)]
pub enum TableChange {
    /// Insert a new row.
    Insert(TableRow),
    /// Update an existing row (provides old and new versions).
    Update {
        /// The old row being replaced.
        old: TableRow,
        /// The new row.
        new: TableRow,
    },
    /// Delete a row.
    Delete(TableRow),
}

/// Metrics for tracking temporal join operations.
#[derive(Debug, Clone, Default)]
pub struct TemporalJoinMetrics {
    /// Number of stream events processed.
    pub stream_events: u64,
    /// Number of table inserts processed.
    pub table_inserts: u64,
    /// Number of table updates processed.
    pub table_updates: u64,
    /// Number of table deletes processed.
    pub table_deletes: u64,
    /// Number of successful matches.
    pub matches: u64,
    /// Number of unmatched stream events.
    pub unmatched: u64,
    /// Number of retractions emitted.
    pub retractions: u64,
    /// Number of state cleanup operations.
    pub state_cleanups: u64,
}

impl TemporalJoinMetrics {
    /// Creates new metrics.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Resets all counters.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Temporal join operator.
///
/// Performs point-in-time lookups against a versioned table. Supports both
/// append-only tables (optimized, no stream-side state) and non-append-only
/// tables (with retraction support).
pub struct TemporalJoinOperator {
    /// Configuration.
    config: TemporalJoinConfig,
    /// Operator ID.
    operator_id: String,
    /// Versioned table state: key -> versions.
    table_state: FxHashMap<Vec<u8>, VersionedKeyState>,
    /// Stream event state for retraction tracking (non-append-only only).
    /// key -> list of joined event records.
    stream_state: FxHashMap<Vec<u8>, Vec<JoinedEventRecord>>,
    /// Current watermark.
    watermark: i64,
    /// Metrics.
    metrics: TemporalJoinMetrics,
    /// Output schema (lazily initialized).
    output_schema: Option<SchemaRef>,
    /// Stream schema.
    stream_schema: Option<SchemaRef>,
    /// Table schema.
    table_schema: Option<SchemaRef>,
}

impl TemporalJoinOperator {
    /// Creates a new temporal join operator.
    #[must_use]
    pub fn new(config: TemporalJoinConfig) -> Self {
        let operator_id = config.operator_id.clone().unwrap_or_else(|| {
            let num = TEMPORAL_OPERATOR_COUNTER.fetch_add(1, Ordering::Relaxed);
            format!("temporal_join_{num}")
        });

        Self {
            config,
            operator_id,
            table_state: FxHashMap::default(),
            stream_state: FxHashMap::default(),
            watermark: i64::MIN,
            metrics: TemporalJoinMetrics::new(),
            output_schema: None,
            stream_schema: None,
            table_schema: None,
        }
    }

    /// Creates a new temporal join operator with explicit ID.
    #[must_use]
    pub fn with_id(mut config: TemporalJoinConfig, operator_id: String) -> Self {
        config.operator_id = Some(operator_id);
        Self::new(config)
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &TemporalJoinConfig {
        &self.config
    }

    /// Returns the metrics.
    #[must_use]
    pub fn metrics(&self) -> &TemporalJoinMetrics {
        &self.metrics
    }

    /// Resets the metrics.
    pub fn reset_metrics(&mut self) {
        self.metrics.reset();
    }

    /// Returns the current watermark.
    #[must_use]
    pub fn watermark(&self) -> i64 {
        self.watermark
    }

    /// Returns the total number of table rows in state.
    #[must_use]
    pub fn table_state_size(&self) -> usize {
        self.table_state.values().map(VersionedKeyState::len).sum()
    }

    /// Returns the total number of tracked stream events (non-append-only only).
    #[must_use]
    pub fn stream_state_size(&self) -> usize {
        self.stream_state.values().map(Vec::len).sum()
    }

    /// Processes a stream event (probe side).
    pub fn process_stream(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec {
        self.metrics.stream_events += 1;

        // Capture stream schema
        if self.stream_schema.is_none() {
            self.stream_schema = Some(event.data.schema());
            self.update_output_schema();
        }

        let mut output = OutputVec::new();

        // Extract join key
        let Some(key_value) = Self::extract_key(&event.data, &self.config.stream_key_column) else {
            return output;
        };

        // Determine lookup timestamp
        let lookup_ts = match self.config.semantics {
            TemporalJoinSemantics::EventTime => event.timestamp,
            TemporalJoinSemantics::ProcessTime => ctx.processing_time,
        };

        // Find matching table row
        if let Some(table_row) = self.lookup_table(&key_value, lookup_ts) {
            self.metrics.matches += 1;

            // Track join for potential retraction (non-append-only)
            if self.config.table_characteristics == TableCharacteristics::NonAppendOnly {
                if let Ok(event_data) = TableRow::serialize_batch(&event.data) {
                    let record = JoinedEventRecord {
                        event_timestamp: event.timestamp,
                        event_data,
                        table_version: table_row.version_timestamp,
                        key_value: key_value.clone(),
                    };
                    self.stream_state.entry(key_value).or_default().push(record);
                }
            }

            // Create joined output
            if let Some(joined) = self.create_joined_event(event, &table_row) {
                output.push(Output::Event(joined));
            }
        } else {
            self.metrics.unmatched += 1;
            if self.config.join_type.emits_unmatched() {
                if let Some(unmatched) = self.create_unmatched_event(event) {
                    output.push(Output::Event(unmatched));
                }
            }
        }

        output
    }

    /// Processes a table insert (append-only or non-append-only).
    pub fn process_table_insert(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        self.metrics.table_inserts += 1;

        // Capture table schema
        if self.table_schema.is_none() {
            self.table_schema = Some(event.data.schema());
            self.update_output_schema();
        }

        // Extract key and version
        let Some(key_value) = Self::extract_key(&event.data, &self.config.table_key_column) else {
            return OutputVec::new();
        };

        let version_ts =
            Self::extract_timestamp(&event.data, &self.config.table_version_column)
                .unwrap_or(event.timestamp);

        // Create and store table row
        let Ok(row) = TableRow::new(version_ts, key_value.clone(), &event.data) else {
            return OutputVec::new();
        };

        let key_state = self.table_state.entry(key_value).or_default();
        key_state.insert(row);

        // Limit versions if configured
        if self.config.max_versions_per_key > 0 {
            key_state.limit_versions(self.config.max_versions_per_key);
        }

        OutputVec::new()
    }

    /// Processes a table change (non-append-only tables).
    /// Returns retractions and new join results for affected stream events.
    pub fn process_table_change(
        &mut self,
        change: &TableChange,
        _ctx: &mut OperatorContext,
    ) -> OutputVec {
        if self.config.table_characteristics != TableCharacteristics::NonAppendOnly {
            // For append-only tables, only inserts are valid
            if let TableChange::Insert(row) = change {
                let key_state = self.table_state.entry(row.key_value.clone()).or_default();
                key_state.insert(row.clone());
            }
            return OutputVec::new();
        }

        let mut output = OutputVec::new();

        match change {
            TableChange::Insert(row) => {
                self.metrics.table_inserts += 1;
                let key_state = self.table_state.entry(row.key_value.clone()).or_default();
                key_state.insert(row.clone());
            }
            TableChange::Update { old, new } => {
                self.metrics.table_updates += 1;

                // Emit retractions for events that joined with old version
                self.emit_retractions_for_version(&old.key_value, old.version_timestamp, &mut output);

                // Update table state
                if let Some(key_state) = self.table_state.get_mut(&old.key_value) {
                    key_state.remove_version(old.version_timestamp);
                }
                let key_state = self.table_state.entry(new.key_value.clone()).or_default();
                key_state.insert(new.clone());

                // Re-emit joins for affected events with new version
                self.rejoin_affected_events(&new.key_value, new.version_timestamp, &mut output);
            }
            TableChange::Delete(row) => {
                self.metrics.table_deletes += 1;

                // Emit retractions for events that joined with deleted version
                self.emit_retractions_for_version(&row.key_value, row.version_timestamp, &mut output);

                // Remove from table state
                if let Some(key_state) = self.table_state.get_mut(&row.key_value) {
                    key_state.remove_version(row.version_timestamp);
                }
            }
        }

        output
    }

    /// Emits retractions for all stream events that joined with a specific table version.
    fn emit_retractions_for_version(
        &mut self,
        key: &[u8],
        version: i64,
        output: &mut OutputVec,
    ) {
        let Some(records) = self.stream_state.get(key) else {
            return;
        };

        for record in records {
            if record.table_version == version {
                // Emit retraction (reconstruct the joined event and mark as retraction)
                if let Ok(event_batch) = TableRow::deserialize_batch(&record.event_data) {
                    let event = Event {
                        timestamp: record.event_timestamp,
                        data: event_batch,
                    };

                    // Look up the old table row to reconstruct the join
                    if let Some(key_state) = self.table_state.get(key) {
                        if let Some((_, rows)) = key_state.versions.get_key_value(&version) {
                            if let Some(table_row) = rows.last() {
                                if let Some(joined) = self.create_joined_event(&event, table_row) {
                                    // Emit as late event with retraction semantic
                                    // In a full implementation, this would be Output::Retraction
                                    output.push(Output::LateEvent(joined));
                                    self.metrics.retractions += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Re-joins affected stream events with a new table version.
    fn rejoin_affected_events(&mut self, key: &[u8], new_version: i64, output: &mut OutputVec) {
        // Collect events that need re-joining
        let events_to_rejoin: Vec<(i64, Vec<u8>)> = {
            let Some(records) = self.stream_state.get(key) else {
                return;
            };
            let Some(key_state) = self.table_state.get(key) else {
                return;
            };

            records
                .iter()
                .filter_map(|record| {
                    let lookup_ts = record.event_timestamp;
                    if let Some(new_row) = key_state.lookup_at_time(lookup_ts) {
                        if new_row.version_timestamp == new_version {
                            return Some((record.event_timestamp, record.event_data.clone()));
                        }
                    }
                    None
                })
                .collect()
        };

        // Now emit the rejoined events
        if let Some(key_state) = self.table_state.get(key) {
            for (event_ts, event_data) in &events_to_rejoin {
                if let Ok(event_batch) = TableRow::deserialize_batch(event_data) {
                    let event = Event {
                        timestamp: *event_ts,
                        data: event_batch,
                    };
                    if let Some(new_row) = key_state.lookup_at_time(*event_ts) {
                        if let Some(joined) = self.create_joined_event(&event, new_row) {
                            output.push(Output::Event(joined));
                        }
                    }
                }
            }
        }

        // Update tracked versions
        if let Some(records) = self.stream_state.get_mut(key) {
            for record in records.iter_mut() {
                if events_to_rejoin.iter().any(|(ts, _)| *ts == record.event_timestamp) {
                    record.table_version = new_version;
                }
            }
        }
    }

    /// Looks up a table row for the given key and timestamp.
    fn lookup_table(&self, key: &[u8], timestamp: i64) -> Option<TableRow> {
        let key_state = self.table_state.get(key)?;

        match self.config.semantics {
            TemporalJoinSemantics::EventTime => key_state.lookup_at_time(timestamp).cloned(),
            TemporalJoinSemantics::ProcessTime => key_state.lookup_latest().cloned(),
        }
    }

    /// Handles watermark updates and triggers cleanup.
    pub fn on_watermark(&mut self, watermark: i64, _ctx: &mut OperatorContext) -> OutputVec {
        self.watermark = watermark;

        // Cleanup old stream state for non-append-only
        if self.config.table_characteristics == TableCharacteristics::NonAppendOnly {
            self.cleanup_stream_state(watermark);
        }

        OutputVec::new()
    }

    /// Cleans up stream state for events that can no longer receive retractions.
    fn cleanup_stream_state(&mut self, watermark: i64) {
        let initial_count: usize = self.stream_state.values().map(Vec::len).sum();

        for records in self.stream_state.values_mut() {
            records.retain(|r| r.event_timestamp >= watermark);
        }
        self.stream_state.retain(|_, v| !v.is_empty());

        let final_count: usize = self.stream_state.values().map(Vec::len).sum();
        if final_count < initial_count {
            self.metrics.state_cleanups += (initial_count - final_count) as u64;
        }
    }

    /// Extracts the key value from a record batch.
    fn extract_key(batch: &RecordBatch, column_name: &str) -> Option<Vec<u8>> {
        let column_index = batch.schema().index_of(column_name).ok()?;
        let column = batch.column(column_index);

        if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
            if string_array.is_empty() || string_array.is_null(0) {
                return None;
            }
            return Some(string_array.value(0).as_bytes().to_vec());
        }

        if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>() {
            if int_array.is_empty() || int_array.is_null(0) {
                return None;
            }
            return Some(int_array.value(0).to_le_bytes().to_vec());
        }

        None
    }

    /// Extracts a timestamp value from a record batch.
    fn extract_timestamp(batch: &RecordBatch, column_name: &str) -> Option<i64> {
        let column_index = batch.schema().index_of(column_name).ok()?;
        let column = batch.column(column_index);

        if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>() {
            if int_array.is_empty() || int_array.is_null(0) {
                return None;
            }
            return Some(int_array.value(0));
        }

        None
    }

    /// Creates a timer key for cleanup.
    /// Reserved for future use when timer-based cleanup is implemented.
    #[allow(dead_code)]
    fn make_cleanup_timer_key(key_suffix: &[u8]) -> TimerKey {
        let mut key = TimerKey::new();
        key.push(TEMPORAL_TIMER_PREFIX);
        key.extend_from_slice(key_suffix);
        key
    }

    /// Updates the output schema when both input schemas are known.
    fn update_output_schema(&mut self) {
        if let (Some(stream), Some(table)) = (&self.stream_schema, &self.table_schema) {
            let mut fields: Vec<Field> =
                stream.fields().iter().map(|f| f.as_ref().clone()).collect();

            // Add table fields, prefixing duplicates
            for field in table.fields() {
                let name = if stream.field_with_name(field.name()).is_ok() {
                    format!("table_{}", field.name())
                } else {
                    field.name().clone()
                };
                fields.push(Field::new(
                    name,
                    field.data_type().clone(),
                    true, // Nullable for outer joins
                ));
            }

            self.output_schema = Some(Arc::new(Schema::new(fields)));
        }
    }

    /// Creates a joined event from stream event and table row.
    fn create_joined_event(&self, stream_event: &Event, table_row: &TableRow) -> Option<Event> {
        let schema = self.output_schema.as_ref()?;
        let table_batch = table_row.to_batch().ok()?;

        let mut columns: Vec<ArrayRef> = stream_event.data.columns().to_vec();
        for column in table_batch.columns() {
            columns.push(Arc::clone(column));
        }

        let joined_batch = RecordBatch::try_new(Arc::clone(schema), columns).ok()?;

        Some(Event {
            timestamp: stream_event.timestamp,
            data: joined_batch,
        })
    }

    /// Creates an unmatched event for left outer joins (with null table columns).
    fn create_unmatched_event(&self, stream_event: &Event) -> Option<Event> {
        let schema = self.output_schema.as_ref()?;
        let table_schema = self.table_schema.as_ref()?;

        let num_rows = stream_event.data.num_rows();
        let mut columns: Vec<ArrayRef> = stream_event.data.columns().to_vec();

        // Add null columns for table side
        for field in table_schema.fields() {
            columns.push(Self::create_null_array(field.data_type(), num_rows));
        }

        let joined_batch = RecordBatch::try_new(Arc::clone(schema), columns).ok()?;

        Some(Event {
            timestamp: stream_event.timestamp,
            data: joined_batch,
        })
    }

    /// Creates a null array of the given type and length.
    fn create_null_array(data_type: &DataType, num_rows: usize) -> ArrayRef {
        match data_type {
            DataType::Utf8 => Arc::new(StringArray::from(vec![None::<&str>; num_rows])) as ArrayRef,
            DataType::Float64 => {
                use arrow_array::Float64Array;
                Arc::new(Float64Array::from(vec![None; num_rows])) as ArrayRef
            }
            _ => Arc::new(Int64Array::from(vec![None; num_rows])) as ArrayRef,
        }
    }
}

impl Operator for TemporalJoinOperator {
    fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec {
        // Default to processing as stream event
        self.process_stream(event, ctx)
    }

    fn on_timer(&mut self, timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        if timer.key.first() == Some(&TEMPORAL_TIMER_PREFIX) {
            // Cleanup triggered
            self.cleanup_stream_state(timer.timestamp);
        }
        OutputVec::new()
    }

    fn checkpoint(&self) -> OperatorState {
        // Serialize table state
        let table_entries: Vec<(Vec<u8>, SerializableVersionedKeyState)> = self
            .table_state
            .iter()
            .map(|(k, v)| (k.clone(), v.into()))
            .collect();

        // Serialize stream state (for non-append-only)
        let stream_entries: Vec<(Vec<u8>, Vec<JoinedEventRecord>)> = self
            .stream_state
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let checkpoint_data = (
            self.watermark,
            self.metrics.stream_events,
            self.metrics.table_inserts,
            self.metrics.matches,
            self.metrics.unmatched,
            self.metrics.retractions,
            table_entries,
            stream_entries,
        );

        let data = rkyv::to_bytes::<RkyvError>(&checkpoint_data)
            .map(|v| v.to_vec())
            .unwrap_or_default();

        OperatorState {
            operator_id: self.operator_id.clone(),
            data,
        }
    }

    fn restore(&mut self, state: OperatorState) -> Result<(), OperatorError> {
        type CheckpointData = (
            i64,
            u64,
            u64,
            u64,
            u64,
            u64,
            Vec<(Vec<u8>, SerializableVersionedKeyState)>,
            Vec<(Vec<u8>, Vec<JoinedEventRecord>)>,
        );

        if state.operator_id != self.operator_id {
            return Err(OperatorError::StateAccessFailed(format!(
                "Operator ID mismatch: expected {}, got {}",
                self.operator_id, state.operator_id
            )));
        }

        let archived = rkyv::access::<rkyv::Archived<CheckpointData>, RkyvError>(&state.data)
            .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;
        let (
            watermark,
            stream_events,
            table_inserts,
            matches,
            unmatched,
            retractions,
            table_entries,
            stream_entries,
        ) = rkyv::deserialize::<CheckpointData, RkyvError>(archived)
            .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;

        self.watermark = watermark;
        self.metrics.stream_events = stream_events;
        self.metrics.table_inserts = table_inserts;
        self.metrics.matches = matches;
        self.metrics.unmatched = unmatched;
        self.metrics.retractions = retractions;

        // Restore table state
        self.table_state.clear();
        for (key, serializable) in table_entries {
            self.table_state.insert(key, serializable.into());
        }

        // Restore stream state
        self.stream_state.clear();
        for (key, records) in stream_entries {
            self.stream_state.insert(key, records);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{InMemoryStore, StateStore};
    use crate::time::{BoundedOutOfOrdernessGenerator, TimerService, WatermarkGenerator};
    use arrow_array::Float64Array;
    use arrow_schema::{DataType, Field, Schema};

    /// Creates an order event for testing.
    fn create_order_event(timestamp: i64, currency: &str, amount: f64) -> Event {
        let schema = Arc::new(Schema::new(vec![
            Field::new("currency", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![currency])),
                Arc::new(Float64Array::from(vec![amount])),
            ],
        )
        .unwrap();
        Event { timestamp, data: batch }
    }

    /// Creates a currency rate event for testing.
    fn create_rate_event(timestamp: i64, currency: &str, rate: f64, valid_from: i64) -> Event {
        let schema = Arc::new(Schema::new(vec![
            Field::new("currency", DataType::Utf8, false),
            Field::new("rate", DataType::Float64, false),
            Field::new("valid_from", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![currency])),
                Arc::new(Float64Array::from(vec![rate])),
                Arc::new(Int64Array::from(vec![valid_from])),
            ],
        )
        .unwrap();
        Event { timestamp, data: batch }
    }

    fn create_test_context<'a>(
        timers: &'a mut TimerService,
        state: &'a mut dyn StateStore,
        watermark_gen: &'a mut dyn WatermarkGenerator,
    ) -> OperatorContext<'a> {
        OperatorContext {
            event_time: 0,
            processing_time: 0,
            timers,
            state,
            watermark_generator: watermark_gen,
            operator_index: 0,
        }
    }

    #[test]
    fn test_temporal_join_semantics_default() {
        assert_eq!(
            TemporalJoinSemantics::default(),
            TemporalJoinSemantics::EventTime
        );
    }

    #[test]
    fn test_table_characteristics_default() {
        assert_eq!(
            TableCharacteristics::default(),
            TableCharacteristics::AppendOnly
        );
    }

    #[test]
    fn test_temporal_join_type_properties() {
        assert!(!TemporalJoinType::Inner.emits_unmatched());
        assert!(TemporalJoinType::Left.emits_unmatched());
    }

    #[test]
    fn test_config_builder() {
        let config = TemporalJoinConfig::builder()
            .stream_key_column("currency".to_string())
            .table_key_column("currency".to_string())
            .table_version_column("valid_from".to_string())
            .semantics(TemporalJoinSemantics::EventTime)
            .table_characteristics(TableCharacteristics::AppendOnly)
            .join_type(TemporalJoinType::Left)
            .max_versions_per_key(100)
            .operator_id("test_temporal".to_string())
            .build();

        assert_eq!(config.stream_key_column, "currency");
        assert_eq!(config.table_key_column, "currency");
        assert_eq!(config.table_version_column, "valid_from");
        assert_eq!(config.semantics, TemporalJoinSemantics::EventTime);
        assert_eq!(config.table_characteristics, TableCharacteristics::AppendOnly);
        assert_eq!(config.join_type, TemporalJoinType::Left);
        assert_eq!(config.max_versions_per_key, 100);
    }

    #[test]
    fn test_event_time_temporal_join_basic() {
        let config = TemporalJoinConfig::builder()
            .stream_key_column("currency".to_string())
            .table_key_column("currency".to_string())
            .table_version_column("valid_from".to_string())
            .semantics(TemporalJoinSemantics::EventTime)
            .join_type(TemporalJoinType::Inner)
            .build();

        let mut operator = TemporalJoinOperator::with_id(config, "test_temporal".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Insert rate versions
        // Rate 1.1 valid from t=500
        let rate1 = create_rate_event(500, "USD", 1.1, 500);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_insert(&rate1, &mut ctx);
        }

        // Rate 1.2 valid from t=800
        let rate2 = create_rate_event(800, "USD", 1.2, 800);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_insert(&rate2, &mut ctx);
        }

        // Rate 1.3 valid from t=1200
        let rate3 = create_rate_event(1200, "USD", 1.3, 1200);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_insert(&rate3, &mut ctx);
        }

        // Order at t=1000 should join with rate 1.2 (valid from t=800)
        let order = create_order_event(1000, "USD", 100.0);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_stream(&order, &mut ctx)
        };

        assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);
        assert_eq!(operator.metrics().matches, 1);

        // Verify output has both order and rate columns
        if let Some(Output::Event(event)) = outputs.first() {
            assert_eq!(event.data.num_columns(), 5); // 2 order + 3 rate
        }
    }

    #[test]
    fn test_event_time_multiple_versions() {
        let config = TemporalJoinConfig::builder()
            .stream_key_column("currency".to_string())
            .table_key_column("currency".to_string())
            .table_version_column("valid_from".to_string())
            .semantics(TemporalJoinSemantics::EventTime)
            .join_type(TemporalJoinType::Inner)
            .build();

        let mut operator = TemporalJoinOperator::with_id(config, "test_temporal".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Insert multiple rate versions
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_insert(&create_rate_event(100, "USD", 1.0, 100), &mut ctx);
            operator.process_table_insert(&create_rate_event(200, "USD", 1.1, 200), &mut ctx);
            operator.process_table_insert(&create_rate_event(300, "USD", 1.2, 300), &mut ctx);
        }

        // Order at t=150 should join with rate 1.0 (valid from t=100)
        let order1 = create_order_event(150, "USD", 100.0);
        let outputs1 = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_stream(&order1, &mut ctx)
        };
        assert_eq!(outputs1.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);

        // Order at t=250 should join with rate 1.1 (valid from t=200)
        let order2 = create_order_event(250, "USD", 100.0);
        let outputs2 = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_stream(&order2, &mut ctx)
        };
        assert_eq!(outputs2.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);

        // Order at t=350 should join with rate 1.2 (valid from t=300)
        let order3 = create_order_event(350, "USD", 100.0);
        let outputs3 = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_stream(&order3, &mut ctx)
        };
        assert_eq!(outputs3.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);

        assert_eq!(operator.metrics().matches, 3);
    }

    #[test]
    fn test_no_match_before_first_version() {
        let config = TemporalJoinConfig::builder()
            .stream_key_column("currency".to_string())
            .table_key_column("currency".to_string())
            .table_version_column("valid_from".to_string())
            .semantics(TemporalJoinSemantics::EventTime)
            .join_type(TemporalJoinType::Inner)
            .build();

        let mut operator = TemporalJoinOperator::with_id(config, "test_temporal".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Insert rate valid from t=500
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_insert(&create_rate_event(500, "USD", 1.1, 500), &mut ctx);
        }

        // Order at t=400 (before any rate is valid)
        let order = create_order_event(400, "USD", 100.0);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_stream(&order, &mut ctx)
        };

        // Inner join - no output
        assert_eq!(outputs.len(), 0);
        assert_eq!(operator.metrics().unmatched, 1);
    }

    #[test]
    fn test_left_join_no_match() {
        let config = TemporalJoinConfig::builder()
            .stream_key_column("currency".to_string())
            .table_key_column("currency".to_string())
            .table_version_column("valid_from".to_string())
            .semantics(TemporalJoinSemantics::EventTime)
            .join_type(TemporalJoinType::Left)
            .build();

        let mut operator = TemporalJoinOperator::with_id(config, "test_temporal".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // First, establish schemas by processing a matched event
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_insert(&create_rate_event(500, "USD", 1.1, 500), &mut ctx);
        }
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_stream(&create_order_event(600, "USD", 100.0), &mut ctx);
        }

        // Order for different currency (no matching rate)
        let order = create_order_event(700, "EUR", 100.0);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_stream(&order, &mut ctx)
        };

        // Left join should emit with nulls
        assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);
        assert_eq!(operator.metrics().unmatched, 1);

        if let Some(Output::Event(event)) = outputs.first() {
            assert_eq!(event.data.num_columns(), 5); // Order cols + null rate cols
        }
    }

    #[test]
    fn test_process_time_semantics() {
        let config = TemporalJoinConfig::builder()
            .stream_key_column("currency".to_string())
            .table_key_column("currency".to_string())
            .table_version_column("valid_from".to_string())
            .semantics(TemporalJoinSemantics::ProcessTime) // Process time
            .join_type(TemporalJoinType::Inner)
            .build();

        let mut operator = TemporalJoinOperator::with_id(config, "test_temporal".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Insert rate versions
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_insert(&create_rate_event(100, "USD", 1.0, 100), &mut ctx);
            operator.process_table_insert(&create_rate_event(200, "USD", 1.1, 200), &mut ctx);
            operator.process_table_insert(&create_rate_event(300, "USD", 1.2, 300), &mut ctx);
        }

        // Process-time lookup always gets latest version
        // Order at t=150 should still get rate 1.2 (latest)
        let order = create_order_event(150, "USD", 100.0);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            ctx.processing_time = 1000; // Processing time doesn't matter for latest
            operator.process_stream(&order, &mut ctx)
        };

        assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);
    }

    #[test]
    fn test_append_only_no_stream_state() {
        let config = TemporalJoinConfig::builder()
            .stream_key_column("currency".to_string())
            .table_key_column("currency".to_string())
            .table_version_column("valid_from".to_string())
            .table_characteristics(TableCharacteristics::AppendOnly)
            .build();

        let mut operator = TemporalJoinOperator::with_id(config, "test_temporal".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Insert rate
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_insert(&create_rate_event(500, "USD", 1.1, 500), &mut ctx);
        }

        // Process order
        let order = create_order_event(600, "USD", 100.0);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_stream(&order, &mut ctx);
        }

        // Append-only should not track stream events
        assert_eq!(operator.stream_state_size(), 0);
    }

    #[test]
    fn test_non_append_only_tracks_stream_state() {
        let config = TemporalJoinConfig::builder()
            .stream_key_column("currency".to_string())
            .table_key_column("currency".to_string())
            .table_version_column("valid_from".to_string())
            .table_characteristics(TableCharacteristics::NonAppendOnly)
            .build();

        let mut operator = TemporalJoinOperator::with_id(config, "test_temporal".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Insert rate
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_insert(&create_rate_event(500, "USD", 1.1, 500), &mut ctx);
        }

        // Process order
        let order = create_order_event(600, "USD", 100.0);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_stream(&order, &mut ctx);
        }

        // Non-append-only should track stream events
        assert_eq!(operator.stream_state_size(), 1);
    }

    #[test]
    fn test_table_delete_emits_retraction() {
        let config = TemporalJoinConfig::builder()
            .stream_key_column("currency".to_string())
            .table_key_column("currency".to_string())
            .table_version_column("valid_from".to_string())
            .table_characteristics(TableCharacteristics::NonAppendOnly)
            .build();

        let mut operator = TemporalJoinOperator::with_id(config, "test_temporal".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Insert rate and process order
        let rate = create_rate_event(500, "USD", 1.1, 500);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_insert(&rate, &mut ctx);
        }

        let order = create_order_event(600, "USD", 100.0);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_stream(&order, &mut ctx);
        }

        // Create table row for delete
        let table_row = TableRow::new(500, b"USD".to_vec(), &rate.data).unwrap();

        // Delete the rate
        let change = TableChange::Delete(table_row);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_change(&change, &mut ctx)
        };

        // Should emit retraction
        assert_eq!(operator.metrics().table_deletes, 1);
        assert!(operator.metrics().retractions >= 1 || outputs.iter().any(|o| matches!(o, Output::LateEvent(_))));
    }

    #[test]
    fn test_multiple_keys() {
        let config = TemporalJoinConfig::builder()
            .stream_key_column("currency".to_string())
            .table_key_column("currency".to_string())
            .table_version_column("valid_from".to_string())
            .build();

        let mut operator = TemporalJoinOperator::with_id(config, "test_temporal".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Insert rates for different currencies
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_insert(&create_rate_event(500, "USD", 1.1, 500), &mut ctx);
            operator.process_table_insert(&create_rate_event(500, "EUR", 0.9, 500), &mut ctx);
            operator.process_table_insert(&create_rate_event(500, "GBP", 0.8, 500), &mut ctx);
        }

        // Orders for different currencies
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            let outputs1 = operator.process_stream(&create_order_event(600, "USD", 100.0), &mut ctx);
            assert_eq!(outputs1.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);
        }

        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            let outputs2 = operator.process_stream(&create_order_event(600, "EUR", 100.0), &mut ctx);
            assert_eq!(outputs2.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);
        }

        assert_eq!(operator.metrics().matches, 2);
    }

    #[test]
    fn test_max_versions_per_key() {
        let config = TemporalJoinConfig::builder()
            .stream_key_column("currency".to_string())
            .table_key_column("currency".to_string())
            .table_version_column("valid_from".to_string())
            .max_versions_per_key(2) // Only keep 2 versions
            .build();

        let mut operator = TemporalJoinOperator::with_id(config, "test_temporal".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Insert 5 versions
        for i in 0..5 {
            let rate = create_rate_event(100 * (i + 1), "USD", 1.0 + (i as f64) * 0.1, 100 * (i + 1));
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_insert(&rate, &mut ctx);
        }

        // Should only have 2 versions (most recent)
        assert_eq!(operator.table_state_size(), 2);

        // Should have versions for t=400 and t=500 only
        let key_state = operator.table_state.get(&b"USD".to_vec()).unwrap();
        assert!(key_state.lookup_at_time(400).is_some());
        assert!(key_state.lookup_at_time(500).is_some());
        // Earlier versions should be gone
        assert!(key_state.lookup_at_time(100).is_none());
    }

    #[test]
    fn test_checkpoint_restore() {
        let config = TemporalJoinConfig::builder()
            .stream_key_column("currency".to_string())
            .table_key_column("currency".to_string())
            .table_version_column("valid_from".to_string())
            .table_characteristics(TableCharacteristics::NonAppendOnly)
            .build();

        let mut operator = TemporalJoinOperator::with_id(config.clone(), "test_temporal".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Add some state
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_insert(&create_rate_event(500, "USD", 1.1, 500), &mut ctx);
            operator.process_table_insert(&create_rate_event(600, "USD", 1.2, 600), &mut ctx);
        }

        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_stream(&create_order_event(550, "USD", 100.0), &mut ctx);
        }

        operator.watermark = 500;
        operator.metrics.matches = 10;
        operator.metrics.retractions = 2;

        // Checkpoint
        let checkpoint = operator.checkpoint();

        // Restore to new operator
        let mut restored = TemporalJoinOperator::with_id(config, "test_temporal".to_string());
        restored.restore(checkpoint).unwrap();

        // Verify state restored
        assert_eq!(restored.watermark(), 500);
        assert_eq!(restored.metrics().matches, 10);
        assert_eq!(restored.metrics().retractions, 2);
        assert_eq!(restored.table_state_size(), 2);
        assert_eq!(restored.stream_state_size(), 1);
    }

    #[test]
    fn test_schema_composition() {
        let config = TemporalJoinConfig::builder()
            .stream_key_column("currency".to_string())
            .table_key_column("currency".to_string())
            .table_version_column("valid_from".to_string())
            .build();

        let mut operator = TemporalJoinOperator::with_id(config, "test_temporal".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process table to capture schema
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_insert(&create_rate_event(500, "USD", 1.1, 500), &mut ctx);
        }

        // Process stream to capture schema and produce output
        let order = create_order_event(600, "USD", 100.0);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_stream(&order, &mut ctx)
        };

        assert_eq!(outputs.len(), 1);

        if let Some(Output::Event(event)) = outputs.first() {
            let schema = event.data.schema();

            // Check stream columns (order)
            assert!(schema.field_with_name("amount").is_ok());

            // Check table columns (rate) - currency is duplicated so prefixed
            assert!(schema.field_with_name("table_currency").is_ok());
            assert!(schema.field_with_name("rate").is_ok());
            assert!(schema.field_with_name("valid_from").is_ok());
        }
    }

    #[test]
    fn test_metrics_tracking() {
        let config = TemporalJoinConfig::builder()
            .stream_key_column("currency".to_string())
            .table_key_column("currency".to_string())
            .table_version_column("valid_from".to_string())
            .build();

        let mut operator = TemporalJoinOperator::with_id(config, "test_temporal".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process events
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_insert(&create_rate_event(500, "USD", 1.1, 500), &mut ctx);
            operator.process_table_insert(&create_rate_event(600, "USD", 1.2, 600), &mut ctx);
        }

        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_stream(&create_order_event(550, "USD", 100.0), &mut ctx);
            operator.process_stream(&create_order_event(650, "USD", 200.0), &mut ctx);
        }

        assert_eq!(operator.metrics().table_inserts, 2);
        assert_eq!(operator.metrics().stream_events, 2);
        assert_eq!(operator.metrics().matches, 2);
    }

    #[test]
    fn test_versioned_key_state_operations() {
        let mut key_state = VersionedKeyState::new();
        assert!(key_state.is_empty());

        // Insert some rows
        let row1 = TableRow {
            version_timestamp: 100,
            key_value: b"test".to_vec(),
            data: vec![],
        };
        let row2 = TableRow {
            version_timestamp: 200,
            key_value: b"test".to_vec(),
            data: vec![],
        };
        let row3 = TableRow {
            version_timestamp: 300,
            key_value: b"test".to_vec(),
            data: vec![],
        };

        key_state.insert(row1);
        key_state.insert(row2);
        key_state.insert(row3);

        assert_eq!(key_state.len(), 3);
        assert_eq!(key_state.min_version, 100);
        assert_eq!(key_state.max_version, 300);

        // Lookup at different times
        assert!(key_state.lookup_at_time(50).is_none()); // Before first version
        assert_eq!(key_state.lookup_at_time(150).unwrap().version_timestamp, 100);
        assert_eq!(key_state.lookup_at_time(250).unwrap().version_timestamp, 200);
        assert_eq!(key_state.lookup_at_time(350).unwrap().version_timestamp, 300);

        // Latest lookup
        assert_eq!(key_state.lookup_latest().unwrap().version_timestamp, 300);

        // Cleanup before 200
        key_state.cleanup_before(200);
        assert_eq!(key_state.len(), 2);
        assert_eq!(key_state.min_version, 200);

        // Remove specific version
        key_state.remove_version(200);
        assert_eq!(key_state.len(), 1);
    }

    #[test]
    fn test_version_limiting() {
        let mut key_state = VersionedKeyState::new();

        // Insert 10 versions
        for i in 0..10 {
            key_state.insert(TableRow {
                version_timestamp: 100 * (i + 1),
                key_value: b"test".to_vec(),
                data: vec![],
            });
        }

        assert_eq!(key_state.len(), 10);

        // Limit to 3 versions
        key_state.limit_versions(3);
        assert_eq!(key_state.len(), 3);

        // Should have versions 800, 900, 1000
        assert!(key_state.lookup_at_time(700).is_none());
        assert!(key_state.lookup_at_time(800).is_some());
    }

    #[test]
    fn test_metrics_reset() {
        let mut metrics = TemporalJoinMetrics::new();
        metrics.stream_events = 100;
        metrics.matches = 50;
        metrics.retractions = 5;

        metrics.reset();

        assert_eq!(metrics.stream_events, 0);
        assert_eq!(metrics.matches, 0);
        assert_eq!(metrics.retractions, 0);
    }

    #[test]
    fn test_table_row_serialization() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("currency", DataType::Utf8, false),
            Field::new("rate", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["USD"])),
                Arc::new(Float64Array::from(vec![1.25])),
            ],
        )
        .unwrap();

        let row = TableRow::new(1000, b"USD".to_vec(), &batch).unwrap();

        // Verify round-trip
        let restored_batch = row.to_batch().unwrap();
        assert_eq!(restored_batch.num_rows(), 1);
        assert_eq!(restored_batch.num_columns(), 2);
    }

    #[test]
    fn test_stream_state_cleanup() {
        let config = TemporalJoinConfig::builder()
            .stream_key_column("currency".to_string())
            .table_key_column("currency".to_string())
            .table_version_column("valid_from".to_string())
            .table_characteristics(TableCharacteristics::NonAppendOnly)
            .build();

        let mut operator = TemporalJoinOperator::with_id(config, "test_temporal".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Add state
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_table_insert(&create_rate_event(500, "USD", 1.1, 500), &mut ctx);
        }

        // Process some orders at different timestamps
        for i in 0..5 {
            let order = create_order_event(600 + i * 100, "USD", 100.0);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_stream(&order, &mut ctx);
        }

        assert_eq!(operator.stream_state_size(), 5);

        // Advance watermark past some events
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.on_watermark(900, &mut ctx);
        }

        // Old events should be cleaned up
        assert!(operator.stream_state_size() < 5);
        assert!(operator.metrics().state_cleanups > 0);
    }
}
