//! # ASOF Join Operators
//!
//! Implementation of temporal proximity joins that match events based on
//! closest timestamp rather than exact equality.
//!
//! ASOF joins are essential for financial and time-series applications where
//! you need to enrich events with the most recent prior data (e.g., enriching
//! trades with the most recent quote).
//!
//! ## Join Directions
//!
//! - **Backward**: Match with the most recent prior event (default for finance)
//! - **Forward**: Match with the next future event
//! - **Nearest**: Match with the closest event by absolute time difference
//!
//! ## Example
//!
//! ```rust,no_run
//! use laminar_core::operator::asof_join::{
//!     AsofJoinOperator, AsofJoinConfig, AsofDirection, AsofJoinType,
//! };
//! use std::time::Duration;
//!
//! // Join trades with the most recent quote within 5 seconds
//! let config = AsofJoinConfig {
//!     key_column: "symbol".to_string(),
//!     left_time_column: "trade_time".to_string(),
//!     right_time_column: "quote_time".to_string(),
//!     direction: AsofDirection::Backward,
//!     tolerance: Some(Duration::from_secs(5)),
//!     join_type: AsofJoinType::Inner,
//!     operator_id: Some("trade_quote_join".to_string()),
//! };
//!
//! let operator = AsofJoinOperator::new(config);
//! ```
//!
//! ## SQL Syntax (Future)
//!
//! ```sql
//! SELECT t.*, q.bid, q.ask
//! FROM trades t
//! ASOF JOIN quotes q
//!     ON t.symbol = q.symbol
//!     AND t.trade_time >= q.quote_time
//!     AND t.trade_time - q.quote_time <= INTERVAL '5' SECOND;
//! ```
//!
//! ## State Management
//!
//! Right-side events are stored in per-key `BTreeMap` structures for O(log n)
//! temporal lookups. State is cleaned up based on watermark progress.

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
use std::time::Duration;

/// Direction for ASOF matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AsofDirection {
    /// Match with the most recent prior event (timestamp <= left timestamp).
    /// This is the default and most common for financial applications.
    #[default]
    Backward,
    /// Match with the next future event (timestamp >= left timestamp).
    Forward,
    /// Match with the closest event by absolute time difference.
    Nearest,
}

/// Type of ASOF join to perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AsofJoinType {
    /// Inner join - only emit when a match is found.
    #[default]
    Inner,
    /// Left outer join - emit all left events, with nulls for unmatched.
    Left,
}

impl AsofJoinType {
    /// Returns true if unmatched left events should be emitted.
    #[must_use]
    pub fn emits_unmatched(&self) -> bool {
        matches!(self, AsofJoinType::Left)
    }
}

/// Configuration for an ASOF join operator.
#[derive(Debug, Clone)]
pub struct AsofJoinConfig {
    /// Column name used as the join key (must match in both streams).
    pub key_column: String,
    /// Column name for the timestamp in left stream events.
    pub left_time_column: String,
    /// Column name for the timestamp in right stream events.
    pub right_time_column: String,
    /// Direction for temporal matching.
    pub direction: AsofDirection,
    /// Maximum time difference for matching (None = unlimited).
    pub tolerance: Option<Duration>,
    /// Type of join to perform.
    pub join_type: AsofJoinType,
    /// Operator ID for checkpointing.
    pub operator_id: Option<String>,
}

impl AsofJoinConfig {
    /// Creates a new builder for ASOF join configuration.
    #[must_use]
    pub fn builder() -> AsofJoinConfigBuilder {
        AsofJoinConfigBuilder::default()
    }

    /// Returns the tolerance in milliseconds, or `i64::MAX` if unlimited.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn tolerance_ms(&self) -> i64 {
        self.tolerance
            .map_or(i64::MAX, |d| d.as_millis() as i64)
    }
}

/// Builder for [`AsofJoinConfig`].
#[derive(Debug, Default)]
pub struct AsofJoinConfigBuilder {
    key_column: Option<String>,
    left_time_column: Option<String>,
    right_time_column: Option<String>,
    direction: Option<AsofDirection>,
    tolerance: Option<Duration>,
    join_type: Option<AsofJoinType>,
    operator_id: Option<String>,
}

impl AsofJoinConfigBuilder {
    /// Sets the join key column name.
    #[must_use]
    pub fn key_column(mut self, column: String) -> Self {
        self.key_column = Some(column);
        self
    }

    /// Sets the left timestamp column name.
    #[must_use]
    pub fn left_time_column(mut self, column: String) -> Self {
        self.left_time_column = Some(column);
        self
    }

    /// Sets the right timestamp column name.
    #[must_use]
    pub fn right_time_column(mut self, column: String) -> Self {
        self.right_time_column = Some(column);
        self
    }

    /// Sets the ASOF direction.
    #[must_use]
    pub fn direction(mut self, direction: AsofDirection) -> Self {
        self.direction = Some(direction);
        self
    }

    /// Sets the tolerance for matching.
    #[must_use]
    pub fn tolerance(mut self, tolerance: Duration) -> Self {
        self.tolerance = Some(tolerance);
        self
    }

    /// Sets the join type.
    #[must_use]
    pub fn join_type(mut self, join_type: AsofJoinType) -> Self {
        self.join_type = Some(join_type);
        self
    }

    /// Sets a custom operator ID.
    #[must_use]
    pub fn operator_id(mut self, id: String) -> Self {
        self.operator_id = Some(id);
        self
    }

    /// Builds the configuration.
    ///
    /// # Errors
    ///
    /// Returns `OperatorError::ConfigError` if required fields
    /// (`key_column`, `left_time_column`, `right_time_column`) are not set.
    pub fn build(self) -> Result<AsofJoinConfig, OperatorError> {
        Ok(AsofJoinConfig {
            key_column: self.key_column.ok_or_else(|| {
                OperatorError::ConfigError("key_column is required".into())
            })?,
            left_time_column: self.left_time_column.ok_or_else(|| {
                OperatorError::ConfigError("left_time_column is required".into())
            })?,
            right_time_column: self.right_time_column.ok_or_else(|| {
                OperatorError::ConfigError("right_time_column is required".into())
            })?,
            direction: self.direction.unwrap_or_default(),
            tolerance: self.tolerance,
            join_type: self.join_type.unwrap_or_default(),
            operator_id: self.operator_id,
        })
    }
}

/// Timer key prefix for state cleanup.
const ASOF_TIMER_PREFIX: u8 = 0x50;

/// Static counter for generating unique operator IDs.
static ASOF_OPERATOR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A stored right-side event for ASOF matching.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct AsofRow {
    /// Event timestamp in milliseconds.
    pub timestamp: i64,
    /// Serialized record batch data.
    pub data: Vec<u8>,
}

impl AsofRow {
    /// Creates a new ASOF row from an event timestamp and batch.
    fn new(timestamp: i64, batch: &RecordBatch) -> Result<Self, OperatorError> {
        let data = Self::serialize_batch(batch)?;
        Ok(Self { timestamp, data })
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

/// Per-key state for ASOF joining.
///
/// Uses `BTreeMap` for O(log n) range queries on timestamps.
#[derive(Debug, Clone, Default)]
pub struct KeyState {
    /// Events indexed by timestamp for efficient range queries.
    /// Multiple events at the same timestamp are stored in a vector.
    pub events: BTreeMap<i64, SmallVec<[AsofRow; 1]>>,
    /// Minimum timestamp in this key's state.
    pub min_timestamp: i64,
    /// Maximum timestamp in this key's state.
    pub max_timestamp: i64,
}

impl KeyState {
    /// Creates a new empty key state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            events: BTreeMap::new(),
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
        }
    }

    /// Inserts an event into the state.
    pub fn insert(&mut self, row: AsofRow) {
        let ts = row.timestamp;
        self.events.entry(ts).or_default().push(row);
        self.min_timestamp = self.min_timestamp.min(ts);
        self.max_timestamp = self.max_timestamp.max(ts);
    }

    /// Returns the number of events in this key's state.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.values().map(SmallVec::len).sum()
    }

    /// Returns true if this key has no events.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Removes events with timestamps before the given threshold.
    pub fn cleanup_before(&mut self, threshold: i64) {
        self.events = self.events.split_off(&threshold);
        self.min_timestamp = self.events.keys().next().copied().unwrap_or(i64::MAX);
    }
}

/// Serializable version of `KeyState` for checkpointing.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct SerializableKeyState {
    events: Vec<(i64, Vec<AsofRow>)>,
    min_timestamp: i64,
    max_timestamp: i64,
}

impl From<&KeyState> for SerializableKeyState {
    fn from(state: &KeyState) -> Self {
        Self {
            events: state
                .events
                .iter()
                .map(|(ts, rows)| (*ts, rows.to_vec()))
                .collect(),
            min_timestamp: state.min_timestamp,
            max_timestamp: state.max_timestamp,
        }
    }
}

impl From<SerializableKeyState> for KeyState {
    fn from(state: SerializableKeyState) -> Self {
        let mut events = BTreeMap::new();
        for (ts, rows) in state.events {
            events.insert(ts, SmallVec::from_vec(rows));
        }
        Self {
            events,
            min_timestamp: state.min_timestamp,
            max_timestamp: state.max_timestamp,
        }
    }
}

/// Metrics for tracking ASOF join operations.
#[derive(Debug, Clone, Default)]
pub struct AsofJoinMetrics {
    /// Number of left events processed.
    pub left_events: u64,
    /// Number of right events processed.
    pub right_events: u64,
    /// Number of matches found.
    pub matches: u64,
    /// Number of unmatched left events (for left join).
    pub unmatched_left: u64,
    /// Number of matches within tolerance.
    pub within_tolerance: u64,
    /// Number of matches rejected due to tolerance.
    pub outside_tolerance: u64,
    /// Number of late events dropped.
    pub late_events: u64,
    /// Number of state cleanup operations.
    pub state_cleanups: u64,
}

impl AsofJoinMetrics {
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

/// ASOF join operator.
///
/// Performs temporal proximity joins between two event streams. Left events
/// probe the right-side state for the closest matching timestamp.
///
/// # State Management
///
/// Right-side events are stored in memory in per-key `BTreeMap` structures.
/// State is persisted to the state store on checkpoint and cleaned up
/// based on watermark progress.
///
/// # Performance Characteristics
///
/// - Matching: O(log n) per left event (`BTreeMap` range query)
/// - State size: Bounded by right-side events within tolerance + watermark lag
/// - Memory: Linear in right-side event count per key
pub struct AsofJoinOperator {
    /// Configuration.
    config: AsofJoinConfig,
    /// Operator ID.
    operator_id: String,
    /// Per-key right-side state.
    right_state: FxHashMap<Vec<u8>, KeyState>,
    /// Current watermark.
    watermark: i64,
    /// Metrics.
    metrics: AsofJoinMetrics,
    /// Output schema (lazily initialized).
    output_schema: Option<SchemaRef>,
    /// Left schema (captured from first left event).
    left_schema: Option<SchemaRef>,
    /// Right schema (captured from first right event).
    right_schema: Option<SchemaRef>,
}

impl AsofJoinOperator {
    /// Creates a new ASOF join operator.
    #[must_use]
    pub fn new(config: AsofJoinConfig) -> Self {
        let operator_id = config.operator_id.clone().unwrap_or_else(|| {
            let num = ASOF_OPERATOR_COUNTER.fetch_add(1, Ordering::Relaxed);
            format!("asof_join_{num}")
        });

        Self {
            config,
            operator_id,
            right_state: FxHashMap::default(),
            watermark: i64::MIN,
            metrics: AsofJoinMetrics::new(),
            output_schema: None,
            left_schema: None,
            right_schema: None,
        }
    }

    /// Creates a new ASOF join operator with explicit ID.
    #[must_use]
    pub fn with_id(mut config: AsofJoinConfig, operator_id: String) -> Self {
        config.operator_id = Some(operator_id);
        Self::new(config)
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &AsofJoinConfig {
        &self.config
    }

    /// Returns the metrics.
    #[must_use]
    pub fn metrics(&self) -> &AsofJoinMetrics {
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

    /// Returns the total number of right-side events in state.
    #[must_use]
    pub fn state_size(&self) -> usize {
        self.right_state.values().map(KeyState::len).sum()
    }

    /// Processes a left-side event (probe side).
    pub fn process_left(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        self.metrics.left_events += 1;

        // Capture left schema on first event
        if self.left_schema.is_none() {
            self.left_schema = Some(event.data.schema());
            self.update_output_schema();
        }

        let mut output = OutputVec::new();

        // Extract join key
        let Some(key_value) = Self::extract_key(&event.data, &self.config.key_column) else {
            return output;
        };

        // Extract timestamp from left event
        let left_timestamp = event.timestamp;

        // Find matching right event
        let match_result = self.find_match(&key_value, left_timestamp);

        match match_result {
            Some(matched_row) => {
                self.metrics.matches += 1;
                self.metrics.within_tolerance += 1;

                // Create joined output
                if let Some(joined) = self.create_joined_event(event, &matched_row) {
                    output.push(Output::Event(joined));
                }
            }
            None => {
                if self.config.join_type.emits_unmatched() {
                    self.metrics.unmatched_left += 1;
                    if let Some(unmatched) = self.create_unmatched_event(event) {
                        output.push(Output::Event(unmatched));
                    }
                }
            }
        }

        output
    }

    /// Processes a right-side event (build side).
    pub fn process_right(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec {
        self.metrics.right_events += 1;

        // Capture right schema on first event
        if self.right_schema.is_none() {
            self.right_schema = Some(event.data.schema());
            self.update_output_schema();
        }

        let output = OutputVec::new();

        // Extract join key
        let Some(key_value) = Self::extract_key(&event.data, &self.config.key_column) else {
            return output;
        };

        // Check if event is too late
        if self.watermark > i64::MIN && event.timestamp < self.watermark {
            self.metrics.late_events += 1;
            // Still store it as it might match future left events for Backward direction
        }

        // Create row and store in state (key is stored in HashMap, not in row)
        let Ok(row) = AsofRow::new(event.timestamp, &event.data) else {
            return output;
        };

        // Calculate cleanup time before borrowing state
        let cleanup_time = self.calculate_cleanup_time(event.timestamp);

        let key_state = self.right_state.entry(key_value).or_default();
        key_state.insert(row);

        // Register cleanup timer based on tolerance and direction
        let timer_key = Self::make_cleanup_timer_key(&key_state.max_timestamp.to_be_bytes());
        ctx.timers
            .register_timer(cleanup_time, Some(timer_key), Some(ctx.operator_index));

        output
    }

    /// Finds a matching right-side event for the given left timestamp.
    fn find_match(&self, key: &[u8], left_timestamp: i64) -> Option<AsofRow> {
        let key_state = self.right_state.get(key)?;

        match self.config.direction {
            AsofDirection::Backward => self.find_backward_match(key_state, left_timestamp),
            AsofDirection::Forward => self.find_forward_match(key_state, left_timestamp),
            AsofDirection::Nearest => self.find_nearest_match(key_state, left_timestamp),
        }
    }

    /// Finds the most recent prior event (timestamp <= `left_timestamp`).
    fn find_backward_match(&self, key_state: &KeyState, left_timestamp: i64) -> Option<AsofRow> {
        // Use range(..=left_timestamp).last() to find the most recent prior event
        let (ts, rows) = key_state.events.range(..=left_timestamp).next_back()?;

        // Check tolerance
        let diff = left_timestamp - ts;
        if diff > self.config.tolerance_ms() {
            return None;
        }

        // Return the last row at this timestamp (most recent)
        rows.last().cloned()
    }

    /// Finds the next future event (timestamp >= `left_timestamp`).
    fn find_forward_match(&self, key_state: &KeyState, left_timestamp: i64) -> Option<AsofRow> {
        // Use range(left_timestamp..).first() to find the next future event
        let (ts, rows) = key_state.events.range(left_timestamp..).next()?;

        // Check tolerance
        let diff = ts - left_timestamp;
        if diff > self.config.tolerance_ms() {
            return None;
        }

        // Return the first row at this timestamp
        rows.first().cloned()
    }

    /// Finds the closest event by absolute time difference.
    fn find_nearest_match(&self, key_state: &KeyState, left_timestamp: i64) -> Option<AsofRow> {
        let before = key_state.events.range(..=left_timestamp).next_back();
        let after = key_state.events.range(left_timestamp..).next();

        let candidate = match (before, after) {
            (Some((ts_before, rows_before)), Some((ts_after, rows_after))) => {
                let diff_before = left_timestamp - ts_before;
                let diff_after = ts_after - left_timestamp;
                if diff_before <= diff_after {
                    Some((diff_before, rows_before.last()?.clone()))
                } else {
                    Some((diff_after, rows_after.first()?.clone()))
                }
            }
            (Some((ts, rows)), None) => {
                let diff = left_timestamp - ts;
                Some((diff, rows.last()?.clone()))
            }
            (None, Some((ts, rows))) => {
                let diff = ts - left_timestamp;
                Some((diff, rows.first()?.clone()))
            }
            (None, None) => None,
        };

        let (diff, row) = candidate?;

        // Check tolerance
        if diff > self.config.tolerance_ms() {
            return None;
        }

        Some(row)
    }

    /// Calculates when state for a given timestamp can be cleaned up.
    fn calculate_cleanup_time(&self, timestamp: i64) -> i64 {
        let tolerance_ms = self.config.tolerance_ms();
        match self.config.direction {
            // For Backward and Nearest, we need to keep state longer
            // because future left events may match with these right events
            AsofDirection::Backward | AsofDirection::Nearest => {
                if tolerance_ms == i64::MAX {
                    i64::MAX
                } else {
                    timestamp.saturating_add(tolerance_ms)
                }
            }
            // For Forward, we can clean up more aggressively
            AsofDirection::Forward => timestamp,
        }
    }

    /// Handles watermark updates and triggers state cleanup.
    pub fn on_watermark(&mut self, watermark: i64, _ctx: &mut OperatorContext) -> OutputVec {
        self.watermark = watermark;
        self.cleanup_state(watermark);
        OutputVec::new()
    }

    /// Cleans up state that can no longer produce matches.
    fn cleanup_state(&mut self, watermark: i64) {
        let tolerance_ms = self.config.tolerance_ms();

        let threshold = match self.config.direction {
            AsofDirection::Backward | AsofDirection::Nearest => {
                if tolerance_ms == i64::MAX {
                    i64::MIN // Never clean up
                } else {
                    watermark.saturating_sub(tolerance_ms)
                }
            }
            AsofDirection::Forward => watermark,
        };

        if threshold == i64::MIN {
            return;
        }

        let initial_count: usize = self.right_state.values().map(KeyState::len).sum();

        for key_state in self.right_state.values_mut() {
            key_state.cleanup_before(threshold);
        }

        // Remove empty key states
        self.right_state.retain(|_, v| !v.is_empty());

        let final_count: usize = self.right_state.values().map(KeyState::len).sum();
        if final_count < initial_count {
            self.metrics.state_cleanups += (initial_count - final_count) as u64;
        }
    }

    /// Extracts the join key value from a record batch.
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

    /// Creates a timer key for state cleanup.
    fn make_cleanup_timer_key(key_suffix: &[u8]) -> TimerKey {
        let mut key = TimerKey::new();
        key.push(ASOF_TIMER_PREFIX);
        key.extend_from_slice(key_suffix);
        key
    }

    /// Updates the output schema when both input schemas are known.
    fn update_output_schema(&mut self) {
        if let (Some(left), Some(right)) = (&self.left_schema, &self.right_schema) {
            let mut fields: Vec<Field> =
                left.fields().iter().map(|f| f.as_ref().clone()).collect();

            // Add right fields, prefixing duplicates
            for field in right.fields() {
                let name = if left.field_with_name(field.name()).is_ok() {
                    format!("right_{}", field.name())
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

    /// Creates a joined event from left event and matched right row.
    fn create_joined_event(&self, left_event: &Event, right_row: &AsofRow) -> Option<Event> {
        let schema = self.output_schema.as_ref()?;
        let right_batch = right_row.to_batch().ok()?;

        let mut columns: Vec<ArrayRef> = left_event.data.columns().to_vec();
        for column in right_batch.columns() {
            columns.push(Arc::clone(column));
        }

        let joined_batch = RecordBatch::try_new(Arc::clone(schema), columns).ok()?;

        Some(Event::new(left_event.timestamp, joined_batch))
    }

    /// Creates an unmatched event for left outer joins (with null right columns).
    fn create_unmatched_event(&self, left_event: &Event) -> Option<Event> {
        let schema = self.output_schema.as_ref()?;
        let right_schema = self.right_schema.as_ref()?;

        let num_rows = left_event.data.num_rows();
        let mut columns: Vec<ArrayRef> = left_event.data.columns().to_vec();

        // Add null columns for right side
        for field in right_schema.fields() {
            columns.push(Self::create_null_array(field.data_type(), num_rows));
        }

        let joined_batch = RecordBatch::try_new(Arc::clone(schema), columns).ok()?;

        Some(Event::new(left_event.timestamp, joined_batch))
    }

    /// Creates a null array of the given type and length.
    fn create_null_array(data_type: &DataType, num_rows: usize) -> ArrayRef {
        match data_type {
            DataType::Utf8 => Arc::new(StringArray::from(vec![None::<&str>; num_rows])) as ArrayRef,
            DataType::Float64 => {
                use arrow_array::Float64Array;
                Arc::new(Float64Array::from(vec![None; num_rows])) as ArrayRef
            }
            // Default to Int64 for numeric and other types
            _ => Arc::new(Int64Array::from(vec![None; num_rows])) as ArrayRef,
        }
    }

}

impl Operator for AsofJoinOperator {
    fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec {
        // Default to processing as left event
        self.process_left(event, ctx)
    }

    fn on_timer(&mut self, timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        // Timers trigger state cleanup
        if timer.key.first() == Some(&ASOF_TIMER_PREFIX) {
            self.cleanup_state(timer.timestamp);
        }
        OutputVec::new()
    }

    fn checkpoint(&self) -> OperatorState {
        // Serialize metrics and state summary
        let state_entries: Vec<(Vec<u8>, SerializableKeyState)> = self
            .right_state
            .iter()
            .map(|(k, v)| (k.clone(), v.into()))
            .collect();

        let checkpoint_data = (
            self.watermark,
            self.metrics.left_events,
            self.metrics.right_events,
            self.metrics.matches,
            self.metrics.unmatched_left,
            state_entries,
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
        type CheckpointData = (i64, u64, u64, u64, u64, Vec<(Vec<u8>, SerializableKeyState)>);

        if state.operator_id != self.operator_id {
            return Err(OperatorError::StateAccessFailed(format!(
                "Operator ID mismatch: expected {}, got {}",
                self.operator_id, state.operator_id
            )));
        }

        let archived = rkyv::access::<rkyv::Archived<CheckpointData>, RkyvError>(&state.data)
            .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;
        let (watermark, left_events, right_events, matches, unmatched_left, state_entries) =
            rkyv::deserialize::<CheckpointData, RkyvError>(archived)
                .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;

        self.watermark = watermark;
        self.metrics.left_events = left_events;
        self.metrics.right_events = right_events;
        self.metrics.matches = matches;
        self.metrics.unmatched_left = unmatched_left;

        // Restore state
        self.right_state.clear();
        for (key, serializable) in state_entries {
            self.right_state.insert(key, serializable.into());
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

    /// Creates a trade event for testing.
    fn create_trade_event(timestamp: i64, symbol: &str, price: f64) -> Event {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![symbol])),
                Arc::new(Float64Array::from(vec![price])),
            ],
        )
        .unwrap();
        Event::new(timestamp, batch)
    }

    /// Creates a quote event for testing.
    fn create_quote_event(timestamp: i64, symbol: &str, bid: f64, ask: f64) -> Event {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("bid", DataType::Float64, false),
            Field::new("ask", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![symbol])),
                Arc::new(Float64Array::from(vec![bid])),
                Arc::new(Float64Array::from(vec![ask])),
            ],
        )
        .unwrap();
        Event::new(timestamp, batch)
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
    fn test_asof_direction_default() {
        assert_eq!(AsofDirection::default(), AsofDirection::Backward);
    }

    #[test]
    fn test_asof_join_type_properties() {
        assert!(!AsofJoinType::Inner.emits_unmatched());
        assert!(AsofJoinType::Left.emits_unmatched());
    }

    #[test]
    fn test_config_builder() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Backward)
            .tolerance(Duration::from_secs(5))
            .join_type(AsofJoinType::Left)
            .operator_id("test_op".to_string())
            .build()
            .unwrap();

        assert_eq!(config.key_column, "symbol");
        assert_eq!(config.left_time_column, "trade_time");
        assert_eq!(config.right_time_column, "quote_time");
        assert_eq!(config.direction, AsofDirection::Backward);
        assert_eq!(config.tolerance, Some(Duration::from_secs(5)));
        assert_eq!(config.join_type, AsofJoinType::Left);
        assert_eq!(config.tolerance_ms(), 5000);
    }

    #[test]
    fn test_backward_asof_basic() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Backward)
            .tolerance(Duration::from_secs(10))
            .join_type(AsofJoinType::Inner)
            .build()
            .unwrap();

        let mut operator = AsofJoinOperator::with_id(config, "test_asof".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Store quote at t=900
        let quote = create_quote_event(900, "AAPL", 150.0, 151.0);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_right(&quote, &mut ctx);
        }

        // Store quote at t=950
        let quote2 = create_quote_event(950, "AAPL", 152.0, 153.0);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_right(&quote2, &mut ctx);
        }

        // Trade at t=1000 should match quote at t=950 (most recent prior)
        let trade = create_trade_event(1000, "AAPL", 152.5);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_left(&trade, &mut ctx)
        };

        assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);
        assert_eq!(operator.metrics().matches, 1);

        // Verify output has both trade and quote columns
        if let Some(Output::Event(event)) = outputs.first() {
            assert_eq!(event.data.num_columns(), 5); // 2 trade + 3 quote
        }
    }

    #[test]
    fn test_forward_asof_basic() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Forward)
            .tolerance(Duration::from_secs(10))
            .join_type(AsofJoinType::Inner)
            .build()
            .unwrap();

        let mut operator = AsofJoinOperator::with_id(config, "test_asof".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Store quotes after the trade time
        let quote1 = create_quote_event(1050, "AAPL", 150.0, 151.0);
        let quote2 = create_quote_event(1100, "AAPL", 152.0, 153.0);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_right(&quote1, &mut ctx);
            operator.process_right(&quote2, &mut ctx);
        }

        // Trade at t=1000 should match quote at t=1050 (next future)
        let trade = create_trade_event(1000, "AAPL", 150.5);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_left(&trade, &mut ctx)
        };

        assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);
        assert_eq!(operator.metrics().matches, 1);
    }

    #[test]
    fn test_nearest_asof() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Nearest)
            .tolerance(Duration::from_secs(10))
            .join_type(AsofJoinType::Inner)
            .build()
            .unwrap();

        let mut operator = AsofJoinOperator::with_id(config, "test_asof".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Store quotes before and after
        let quote_before = create_quote_event(990, "AAPL", 150.0, 151.0);
        let quote_after = create_quote_event(1020, "AAPL", 152.0, 153.0);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_right(&quote_before, &mut ctx);
            operator.process_right(&quote_after, &mut ctx);
        }

        // Trade at t=1000 - quote_before is 10ms away, quote_after is 20ms away
        // Should match quote_before (closer)
        let trade = create_trade_event(1000, "AAPL", 150.5);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_left(&trade, &mut ctx)
        };

        assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);
    }

    #[test]
    fn test_tolerance_exceeded() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Backward)
            .tolerance(Duration::from_millis(50)) // 50ms tolerance
            .join_type(AsofJoinType::Inner)
            .build()
            .unwrap();

        let mut operator = AsofJoinOperator::with_id(config, "test_asof".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Store quote at t=900
        let quote = create_quote_event(900, "AAPL", 150.0, 151.0);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_right(&quote, &mut ctx);
        }

        // Trade at t=1000 - 100ms after quote, exceeds 50ms tolerance
        let trade = create_trade_event(1000, "AAPL", 150.5);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_left(&trade, &mut ctx)
        };

        // No match due to tolerance exceeded
        assert_eq!(outputs.len(), 0);
        assert_eq!(operator.metrics().matches, 0);
    }

    #[test]
    fn test_tolerance_within() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Backward)
            .tolerance(Duration::from_millis(100)) // 100ms tolerance
            .join_type(AsofJoinType::Inner)
            .build()
            .unwrap();

        let mut operator = AsofJoinOperator::with_id(config, "test_asof".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Store quote at t=950
        let quote = create_quote_event(950, "AAPL", 150.0, 151.0);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_right(&quote, &mut ctx);
        }

        // Trade at t=1000 - 50ms after quote, within 100ms tolerance
        let trade = create_trade_event(1000, "AAPL", 150.5);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_left(&trade, &mut ctx)
        };

        assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);
        assert_eq!(operator.metrics().within_tolerance, 1);
    }

    #[test]
    fn test_no_match_empty_state() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Backward)
            .join_type(AsofJoinType::Inner)
            .build()
            .unwrap();

        let mut operator = AsofJoinOperator::with_id(config, "test_asof".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Trade with no quotes in state
        let trade = create_trade_event(1000, "AAPL", 150.5);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_left(&trade, &mut ctx)
        };

        assert_eq!(outputs.len(), 0);
        assert_eq!(operator.metrics().matches, 0);
    }

    #[test]
    fn test_multiple_keys() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Backward)
            .tolerance(Duration::from_secs(10))
            .join_type(AsofJoinType::Inner)
            .build()
            .unwrap();

        let mut operator = AsofJoinOperator::with_id(config, "test_asof".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Store quotes for different symbols
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_right(&create_quote_event(950, "AAPL", 150.0, 151.0), &mut ctx);
            operator.process_right(&create_quote_event(960, "GOOG", 2800.0, 2801.0), &mut ctx);
        }

        // Trade for AAPL should match AAPL quote, not GOOG
        let trade = create_trade_event(1000, "AAPL", 150.5);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_left(&trade, &mut ctx)
        };

        assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);

        // Trade for GOOG should match GOOG quote
        let trade2 = create_trade_event(1000, "GOOG", 2800.5);
        let outputs2 = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_left(&trade2, &mut ctx)
        };

        assert_eq!(outputs2.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);
        assert_eq!(operator.metrics().matches, 2);
    }

    #[test]
    fn test_multiple_events_same_timestamp() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Backward)
            .tolerance(Duration::from_secs(10))
            .join_type(AsofJoinType::Inner)
            .build()
            .unwrap();

        let mut operator = AsofJoinOperator::with_id(config, "test_asof".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Store multiple quotes at the same timestamp
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_right(&create_quote_event(950, "AAPL", 150.0, 151.0), &mut ctx);
            operator.process_right(&create_quote_event(950, "AAPL", 150.5, 151.5), &mut ctx); // Same ts
        }

        // Trade should match (last quote at that timestamp for Backward)
        let trade = create_trade_event(1000, "AAPL", 150.5);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_left(&trade, &mut ctx)
        };

        assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);
    }

    #[test]
    fn test_left_outer_join() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Backward)
            .tolerance(Duration::from_millis(50))
            .join_type(AsofJoinType::Left) // Left outer join
            .build()
            .unwrap();

        let mut operator = AsofJoinOperator::with_id(config, "test_asof".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // First, process a matched event to establish right schema
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_right(&create_quote_event(990, "AAPL", 150.0, 151.0), &mut ctx);
        }
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_left(&create_trade_event(1000, "AAPL", 150.5), &mut ctx);
        }

        // Now process trade with no matching quote (different symbol)
        let trade = create_trade_event(2000, "GOOG", 2800.5);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_left(&trade, &mut ctx)
        };

        // Left join should emit with nulls
        assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);
        assert_eq!(operator.metrics().unmatched_left, 1);

        if let Some(Output::Event(event)) = outputs.first() {
            // Should have both left and right columns (with nulls)
            assert_eq!(event.data.num_columns(), 5);
        }
    }

    #[test]
    fn test_inner_join_no_output() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Backward)
            .tolerance(Duration::from_millis(50))
            .join_type(AsofJoinType::Inner)
            .build()
            .unwrap();

        let mut operator = AsofJoinOperator::with_id(config, "test_asof".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Trade with no matching quote
        let trade = create_trade_event(1000, "AAPL", 150.5);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_left(&trade, &mut ctx)
        };

        // Inner join should emit nothing when no match
        assert_eq!(outputs.len(), 0);
    }

    #[test]
    fn test_state_cleanup() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Backward)
            .tolerance(Duration::from_millis(100))
            .join_type(AsofJoinType::Inner)
            .build()
            .unwrap();

        let mut operator = AsofJoinOperator::with_id(config, "test_asof".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Store quotes
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_right(&create_quote_event(900, "AAPL", 150.0, 151.0), &mut ctx);
            operator.process_right(&create_quote_event(950, "AAPL", 152.0, 153.0), &mut ctx);
        }

        assert_eq!(operator.state_size(), 2);

        // Advance watermark past old quotes
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.on_watermark(1100, &mut ctx);
        }

        // Old quotes should be cleaned up
        assert!(operator.state_size() < 2 || operator.metrics().state_cleanups > 0);
    }

    #[test]
    fn test_late_event_still_stored() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Backward)
            .tolerance(Duration::from_secs(10))
            .join_type(AsofJoinType::Inner)
            .build()
            .unwrap();

        let mut operator = AsofJoinOperator::with_id(config, "test_asof".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Set watermark
        operator.watermark = 1000;

        // Process late quote
        let quote = create_quote_event(500, "AAPL", 150.0, 151.0);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_right(&quote, &mut ctx);
        }

        assert_eq!(operator.metrics().late_events, 1);
        // Late events are still stored (for Backward joins)
        assert_eq!(operator.state_size(), 1);
    }

    #[test]
    fn test_checkpoint_restore() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Backward)
            .tolerance(Duration::from_secs(10))
            .join_type(AsofJoinType::Inner)
            .build()
            .unwrap();

        let mut operator = AsofJoinOperator::with_id(config.clone(), "test_asof".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Add some state
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_right(&create_quote_event(900, "AAPL", 150.0, 151.0), &mut ctx);
            operator.process_right(&create_quote_event(950, "AAPL", 152.0, 153.0), &mut ctx);
        }

        // Record metrics
        operator.metrics.left_events = 10;
        operator.metrics.matches = 5;
        operator.watermark = 800;

        // Checkpoint
        let checkpoint = operator.checkpoint();

        // Restore to new operator
        let mut restored = AsofJoinOperator::with_id(config, "test_asof".to_string());
        restored.restore(checkpoint).unwrap();

        // Verify state restored
        assert_eq!(restored.metrics().left_events, 10);
        assert_eq!(restored.metrics().matches, 5);
        assert_eq!(restored.watermark(), 800);
        assert_eq!(restored.state_size(), 2);
    }

    #[test]
    fn test_schema_composition() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Backward)
            .tolerance(Duration::from_secs(10))
            .join_type(AsofJoinType::Inner)
            .build()
            .unwrap();

        let mut operator = AsofJoinOperator::with_id(config, "test_asof".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process right to capture schema
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_right(&create_quote_event(950, "AAPL", 150.0, 151.0), &mut ctx);
        }

        // Process left to capture schema and produce output
        let trade = create_trade_event(1000, "AAPL", 150.5);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_left(&trade, &mut ctx)
        };

        assert_eq!(outputs.len(), 1);

        if let Some(Output::Event(event)) = outputs.first() {
            let schema = event.data.schema();

            // Check left columns (trade)
            assert!(schema.field_with_name("price").is_ok());

            // Check right columns (quote) - symbol is duplicated, so prefixed
            assert!(schema.field_with_name("right_symbol").is_ok());
            assert!(schema.field_with_name("bid").is_ok());
            assert!(schema.field_with_name("ask").is_ok());
        }
    }

    #[test]
    fn test_metrics_tracking() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Backward)
            .tolerance(Duration::from_secs(10))
            .join_type(AsofJoinType::Inner)
            .build()
            .unwrap();

        let mut operator = AsofJoinOperator::with_id(config, "test_asof".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process some events
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_right(&create_quote_event(900, "AAPL", 150.0, 151.0), &mut ctx);
            operator.process_right(&create_quote_event(950, "AAPL", 152.0, 153.0), &mut ctx);
        }

        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_left(&create_trade_event(1000, "AAPL", 150.5), &mut ctx);
            operator.process_left(&create_trade_event(1100, "AAPL", 151.5), &mut ctx);
        }

        assert_eq!(operator.metrics().right_events, 2);
        assert_eq!(operator.metrics().left_events, 2);
        assert_eq!(operator.metrics().matches, 2);
        assert_eq!(operator.metrics().within_tolerance, 2);
    }

    #[test]
    fn test_key_state_operations() {
        let mut key_state = KeyState::new();
        assert!(key_state.is_empty());

        // Insert some rows
        let row1 = AsofRow {
            timestamp: 100,
            data: vec![],
        };
        let row2 = AsofRow {
            timestamp: 200,
            data: vec![],
        };

        key_state.insert(row1);
        key_state.insert(row2);

        assert_eq!(key_state.len(), 2);
        assert_eq!(key_state.min_timestamp, 100);
        assert_eq!(key_state.max_timestamp, 200);

        // Cleanup before 150
        key_state.cleanup_before(150);
        assert_eq!(key_state.len(), 1);
        assert_eq!(key_state.min_timestamp, 200);
    }

    #[test]
    fn test_asof_row_serialization() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL"])),
                Arc::new(Float64Array::from(vec![150.5])),
            ],
        )
        .unwrap();

        let row = AsofRow::new(1000, &batch).unwrap();

        // Verify round-trip
        let restored_batch = row.to_batch().unwrap();
        assert_eq!(restored_batch.num_rows(), 1);
        assert_eq!(restored_batch.num_columns(), 2);
    }

    #[test]
    fn test_metrics_reset() {
        let mut metrics = AsofJoinMetrics::new();
        metrics.left_events = 100;
        metrics.matches = 50;

        metrics.reset();

        assert_eq!(metrics.left_events, 0);
        assert_eq!(metrics.matches, 0);
    }

    #[test]
    fn test_unlimited_tolerance() {
        let config = AsofJoinConfig::builder()
            .key_column("symbol".to_string())
            .left_time_column("trade_time".to_string())
            .right_time_column("quote_time".to_string())
            .direction(AsofDirection::Backward)
            // No tolerance set - unlimited
            .join_type(AsofJoinType::Inner)
            .build()
            .unwrap();

        assert_eq!(config.tolerance_ms(), i64::MAX);

        let mut operator = AsofJoinOperator::with_id(config, "test_asof".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Store very old quote
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_right(&create_quote_event(100, "AAPL", 150.0, 151.0), &mut ctx);
        }

        // Trade much later should still match with unlimited tolerance
        let trade = create_trade_event(1000000, "AAPL", 150.5);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_left(&trade, &mut ctx)
        };

        assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);
    }
}
