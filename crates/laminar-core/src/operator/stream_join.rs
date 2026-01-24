//! # Stream-Stream Join Operators
//!
//! Implementation of time-bounded joins between two event streams.
//!
//! Stream-stream joins match events from two streams based on a join key
//! and a time bound. Events are matched if they share a key and their
//! timestamps fall within the specified time window.
//!
//! ## Join Types
//!
//! - **Inner**: Only emit matched pairs
//! - **Left**: Emit all left events, with right match if exists
//! - **Right**: Emit all right events, with left match if exists
//! - **Full**: Emit all events, with matches where they exist
//!
//! ## Example
//!
//! ```rust,no_run
//! use laminar_core::operator::stream_join::{
//!     StreamJoinOperator, JoinType, JoinSide,
//! };
//! use std::time::Duration;
//!
//! // Join orders with payments within 1 hour, matching on order_id
//! let operator = StreamJoinOperator::new(
//!     "order_id".to_string(),  // left key column
//!     "order_id".to_string(),  // right key column
//!     Duration::from_secs(3600), // 1 hour time bound
//!     JoinType::Inner,
//! );
//! ```
//!
//! ## SQL Syntax
//!
//! ```sql
//! SELECT o.*, p.status
//! FROM orders o
//! JOIN payments p
//!     ON o.order_id = p.order_id
//!     AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR;
//! ```
//!
//! ## State Management
//!
//! Events are stored in state with keys formatted as:
//! - `sjl:<key_hash>:<timestamp>:<event_id>` for left events
//! - `sjr:<key_hash>:<timestamp>:<event_id>` for right events
//!
//! State is automatically cleaned up when watermark passes
//! `event_timestamp + time_bound`.

use super::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output, OutputVec, Timer,
    TimerKey,
};
use crate::state::{StateStore, StateStoreExt};
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, Int64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rkyv::{
    rancor::Error as RkyvError,
    Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Join type for stream-stream joins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JoinType {
    /// Inner join - only emit matched pairs.
    #[default]
    Inner,
    /// Left outer join - emit all left events, with right match if exists.
    Left,
    /// Right outer join - emit all right events, with left match if exists.
    Right,
    /// Full outer join - emit all events, with matches where they exist.
    Full,
}

impl JoinType {
    /// Returns true if unmatched left events should be emitted.
    #[must_use]
    pub fn emits_unmatched_left(&self) -> bool {
        matches!(self, JoinType::Left | JoinType::Full)
    }

    /// Returns true if unmatched right events should be emitted.
    #[must_use]
    pub fn emits_unmatched_right(&self) -> bool {
        matches!(self, JoinType::Right | JoinType::Full)
    }
}

/// Identifies which side of the join an event came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSide {
    /// Left side of the join.
    Left,
    /// Right side of the join.
    Right,
}

/// State key prefixes for join state.
const LEFT_STATE_PREFIX: &[u8; 4] = b"sjl:";
const RIGHT_STATE_PREFIX: &[u8; 4] = b"sjr:";

/// Timer key prefix for left-side cleanup.
const LEFT_TIMER_PREFIX: u8 = 0x10;
/// Timer key prefix for right-side cleanup.
const RIGHT_TIMER_PREFIX: u8 = 0x20;
/// Timer key prefix for unmatched event emission.
const UNMATCHED_TIMER_PREFIX: u8 = 0x30;

/// Static counter for generating unique operator IDs.
static JOIN_OPERATOR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Static counter for generating unique event IDs within an operator.
static EVENT_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A stored join row containing serialized event data.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct JoinRow {
    /// Event timestamp in milliseconds.
    pub timestamp: i64,
    /// Serialized key value (as bytes).
    pub key_value: Vec<u8>,
    /// Serialized record batch data.
    pub data: Vec<u8>,
    /// Whether this row has been matched (for outer joins).
    pub matched: bool,
}

impl JoinRow {
    /// Creates a new join row from an event and extracted key.
    fn new(timestamp: i64, key_value: Vec<u8>, batch: &RecordBatch) -> Result<Self, OperatorError> {
        // Serialize the record batch using Arrow IPC
        let data = Self::serialize_batch(batch)?;
        Ok(Self {
            timestamp,
            key_value,
            data,
            matched: false,
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

    /// Converts this join row back to a record batch.
    ///
    /// # Errors
    ///
    /// Returns `OperatorError::SerializationFailed` if the batch data is invalid.
    pub fn to_batch(&self) -> Result<RecordBatch, OperatorError> {
        Self::deserialize_batch(&self.data)
    }
}

/// Metrics for tracking join operations.
#[derive(Debug, Clone, Default)]
pub struct JoinMetrics {
    /// Number of left events processed.
    pub left_events: u64,
    /// Number of right events processed.
    pub right_events: u64,
    /// Number of join matches produced.
    pub matches: u64,
    /// Number of unmatched left events emitted (left/full joins).
    pub unmatched_left: u64,
    /// Number of unmatched right events emitted (right/full joins).
    pub unmatched_right: u64,
    /// Number of late events dropped.
    pub late_events: u64,
    /// Number of state entries cleaned up.
    pub state_cleanups: u64,
}

impl JoinMetrics {
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

/// Stream-stream join operator.
///
/// Joins events from two streams based on a key column and time bound.
/// Events are matched if they share a key value and their timestamps
/// are within the specified time window.
///
/// # State Management
///
/// Events from both sides are stored in state until they can no longer
/// produce matches (watermark passes `timestamp + time_bound`). State
/// is automatically cleaned up via timers.
///
/// # Performance Considerations
///
/// - State grows linearly with the number of events within the time window
/// - For high-cardinality joins, consider using shorter time bounds
/// - Inner joins use less state than outer joins (no unmatched tracking)
pub struct StreamJoinOperator {
    /// Left stream key column name.
    left_key_column: String,
    /// Right stream key column name.
    right_key_column: String,
    /// Time bound for matching (events match if within this duration).
    time_bound_ms: i64,
    /// Type of join to perform.
    join_type: JoinType,
    /// Operator ID for checkpointing.
    operator_id: String,
    /// Metrics for monitoring.
    metrics: JoinMetrics,
    /// Output schema (lazily initialized).
    output_schema: Option<SchemaRef>,
    /// Left schema (captured from first left event).
    left_schema: Option<SchemaRef>,
    /// Right schema (captured from first right event).
    right_schema: Option<SchemaRef>,
}

impl StreamJoinOperator {
    /// Creates a new stream join operator.
    ///
    /// # Arguments
    ///
    /// * `left_key_column` - Name of the key column in left stream events
    /// * `right_key_column` - Name of the key column in right stream events
    /// * `time_bound` - Maximum time difference for matching events
    /// * `join_type` - Type of join to perform
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(
        left_key_column: String,
        right_key_column: String,
        time_bound: Duration,
        join_type: JoinType,
    ) -> Self {
        let operator_num = JOIN_OPERATOR_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self {
            left_key_column,
            right_key_column,
            time_bound_ms: time_bound.as_millis() as i64,
            join_type,
            operator_id: format!("stream_join_{operator_num}"),
            metrics: JoinMetrics::new(),
            output_schema: None,
            left_schema: None,
            right_schema: None,
        }
    }

    /// Creates a new stream join operator with a custom operator ID.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn with_id(
        left_key_column: String,
        right_key_column: String,
        time_bound: Duration,
        join_type: JoinType,
        operator_id: String,
    ) -> Self {
        Self {
            left_key_column,
            right_key_column,
            time_bound_ms: time_bound.as_millis() as i64,
            join_type,
            operator_id,
            metrics: JoinMetrics::new(),
            output_schema: None,
            left_schema: None,
            right_schema: None,
        }
    }

    /// Returns the join type.
    #[must_use]
    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    /// Returns the time bound in milliseconds.
    #[must_use]
    pub fn time_bound_ms(&self) -> i64 {
        self.time_bound_ms
    }

    /// Returns the metrics.
    #[must_use]
    pub fn metrics(&self) -> &JoinMetrics {
        &self.metrics
    }

    /// Resets the metrics.
    pub fn reset_metrics(&mut self) {
        self.metrics.reset();
    }

    /// Processes an event from either the left or right side.
    ///
    /// This is the main entry point for the join operator. Call this with
    /// the appropriate `JoinSide` to indicate which stream the event came from.
    pub fn process_side(
        &mut self,
        event: &Event,
        side: JoinSide,
        ctx: &mut OperatorContext,
    ) -> OutputVec {
        match side {
            JoinSide::Left => self.process_left(event, ctx),
            JoinSide::Right => self.process_right(event, ctx),
        }
    }

    /// Processes a left-side event.
    fn process_left(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec {
        self.metrics.left_events += 1;

        // Capture left schema on first event
        if self.left_schema.is_none() {
            self.left_schema = Some(event.data.schema());
            self.update_output_schema();
        }

        self.process_event(event, JoinSide::Left, ctx)
    }

    /// Processes a right-side event.
    fn process_right(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec {
        self.metrics.right_events += 1;

        // Capture right schema on first event
        if self.right_schema.is_none() {
            self.right_schema = Some(event.data.schema());
            self.update_output_schema();
        }

        self.process_event(event, JoinSide::Right, ctx)
    }

    /// Updates the output schema when both input schemas are known.
    fn update_output_schema(&mut self) {
        if let (Some(left), Some(right)) = (&self.left_schema, &self.right_schema) {
            let mut fields: Vec<Field> = left.fields().iter().map(|f| f.as_ref().clone()).collect();

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

    /// Core event processing logic.
    fn process_event(
        &mut self,
        event: &Event,
        side: JoinSide,
        ctx: &mut OperatorContext,
    ) -> OutputVec {
        let mut output = OutputVec::new();
        let event_time = event.timestamp;

        // Update watermark
        let emitted_watermark = ctx.watermark_generator.on_event(event_time);

        // Check if event is too late
        let current_wm = ctx.watermark_generator.current_watermark();
        if current_wm > i64::MIN && event_time + self.time_bound_ms < current_wm {
            self.metrics.late_events += 1;
            output.push(Output::LateEvent(event.clone()));
            return output;
        }

        // Extract join key
        let key_column = match side {
            JoinSide::Left => &self.left_key_column,
            JoinSide::Right => &self.right_key_column,
        };
        let Some(key_value) = Self::extract_key(&event.data, key_column) else {
            // Can't extract key, skip this event
            return output;
        };

        // Create join row
        let Ok(join_row) = JoinRow::new(event_time, key_value.clone(), &event.data) else {
            return output;
        };

        // Store the event in state
        let state_key = Self::make_state_key(side, &key_value, event_time);
        if ctx.state.put_typed(&state_key, &join_row).is_err() {
            return output;
        }

        // Register cleanup timer
        let cleanup_time = event_time + self.time_bound_ms;
        let timer_key = Self::make_timer_key(side, &state_key);
        ctx.timers.register_timer(cleanup_time, Some(timer_key), Some(ctx.operator_index));

        // For outer joins, register unmatched emission timer
        if (side == JoinSide::Left && self.join_type.emits_unmatched_left())
            || (side == JoinSide::Right && self.join_type.emits_unmatched_right())
        {
            let unmatched_timer_key = Self::make_unmatched_timer_key(side, &state_key);
            ctx.timers
                .register_timer(cleanup_time, Some(unmatched_timer_key), Some(ctx.operator_index));
        }

        // Probe the opposite side for matches
        let matches = self.probe_opposite_side(side, &key_value, event_time, ctx.state);

        // Emit join results
        for (other_row_key, mut other_row) in matches {
            self.metrics.matches += 1;

            // Mark this row as matched in state
            other_row.matched = true;
            let _ = ctx.state.put_typed(&other_row_key, &other_row);

            // Also mark our row as matched
            if let Ok(Some(mut our_row)) = ctx.state.get_typed::<JoinRow>(&state_key) {
                our_row.matched = true;
                let _ = ctx.state.put_typed(&state_key, &our_row);
            }

            // Create joined output
            if let Some(joined_event) = self.create_joined_event(
                side,
                &join_row,
                &other_row,
                std::cmp::max(event_time, other_row.timestamp),
            ) {
                output.push(Output::Event(joined_event));
            }
        }

        // Emit watermark if generated
        if let Some(wm) = emitted_watermark {
            output.push(Output::Watermark(wm.timestamp()));
        }

        output
    }

    /// Extracts the join key value from a record batch.
    fn extract_key(batch: &RecordBatch, column_name: &str) -> Option<Vec<u8>> {
        let column_index = batch.schema().index_of(column_name).ok()?;
        let column = batch.column(column_index);

        // Handle different column types
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

        // For other types, use the raw bytes if available
        // This is a fallback - in practice, keys should be string or integer
        None
    }

    /// Creates a state key for storing a join row.
    #[allow(clippy::cast_sign_loss)]
    fn make_state_key(side: JoinSide, key_value: &[u8], timestamp: i64) -> Vec<u8> {
        let prefix = match side {
            JoinSide::Left => LEFT_STATE_PREFIX,
            JoinSide::Right => RIGHT_STATE_PREFIX,
        };

        let event_id = EVENT_ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        // Key format: prefix (4) + key_hash (8) + timestamp (8) + event_id (8) = 28 bytes
        let mut key = Vec::with_capacity(28);
        key.extend_from_slice(prefix);

        // Use FxHash for the key value
        let key_hash = fxhash::hash64(key_value);
        key.extend_from_slice(&key_hash.to_be_bytes());
        key.extend_from_slice(&timestamp.to_be_bytes());
        key.extend_from_slice(&event_id.to_be_bytes());

        key
    }

    /// Creates a timer key for cleanup.
    fn make_timer_key(side: JoinSide, state_key: &[u8]) -> TimerKey {
        let prefix = match side {
            JoinSide::Left => LEFT_TIMER_PREFIX,
            JoinSide::Right => RIGHT_TIMER_PREFIX,
        };

        let mut key = TimerKey::new();
        key.push(prefix);
        key.extend_from_slice(state_key);
        key
    }

    /// Creates a timer key for unmatched event emission.
    fn make_unmatched_timer_key(side: JoinSide, state_key: &[u8]) -> TimerKey {
        let side_byte = match side {
            JoinSide::Left => 0x01,
            JoinSide::Right => 0x02,
        };

        let mut key = TimerKey::new();
        key.push(UNMATCHED_TIMER_PREFIX);
        key.push(side_byte);
        key.extend_from_slice(state_key);
        key
    }

    /// Probes the opposite side for matching events.
    fn probe_opposite_side(
        &self,
        current_side: JoinSide,
        key_value: &[u8],
        timestamp: i64,
        state: &dyn StateStore,
    ) -> Vec<(Vec<u8>, JoinRow)> {
        let mut matches = Vec::new();

        let prefix = match current_side {
            JoinSide::Left => RIGHT_STATE_PREFIX,
            JoinSide::Right => LEFT_STATE_PREFIX,
        };

        // Build prefix for scanning: prefix + key_hash
        let key_hash = fxhash::hash64(key_value);
        let mut scan_prefix = Vec::with_capacity(12);
        scan_prefix.extend_from_slice(prefix);
        scan_prefix.extend_from_slice(&key_hash.to_be_bytes());

        // Scan for matching keys
        for (state_key, value) in state.prefix_scan(&scan_prefix) {
            // Deserialize the join row
            let Ok(row) = rkyv::access::<rkyv::Archived<JoinRow>, RkyvError>(&value)
                .and_then(rkyv::deserialize::<JoinRow, RkyvError>)
            else {
                continue;
            };

            // Check if timestamps are within time bound
            let time_diff = (timestamp - row.timestamp).abs();
            if time_diff <= self.time_bound_ms {
                // Verify key matches (in case of hash collision)
                if row.key_value == key_value {
                    matches.push((state_key.to_vec(), row));
                }
            }
        }

        matches
    }

    /// Creates a joined event from two matching rows.
    fn create_joined_event(
        &self,
        current_side: JoinSide,
        current_row: &JoinRow,
        other_row: &JoinRow,
        output_timestamp: i64,
    ) -> Option<Event> {
        let (left_row, right_row) = match current_side {
            JoinSide::Left => (current_row, other_row),
            JoinSide::Right => (other_row, current_row),
        };

        let left_batch = left_row.to_batch().ok()?;
        let right_batch = right_row.to_batch().ok()?;

        let joined_batch = self.concat_batches(&left_batch, &right_batch)?;

        Some(Event {
            timestamp: output_timestamp,
            data: joined_batch,
        })
    }

    /// Concatenates two batches horizontally.
    fn concat_batches(&self, left: &RecordBatch, right: &RecordBatch) -> Option<RecordBatch> {
        let schema = self.output_schema.as_ref()?;

        let mut columns: Vec<ArrayRef> = left.columns().to_vec();

        // Add right columns
        for column in right.columns() {
            columns.push(Arc::clone(column));
        }

        RecordBatch::try_new(Arc::clone(schema), columns).ok()
    }

    /// Creates an unmatched event for outer joins.
    fn create_unmatched_event(
        &self,
        side: JoinSide,
        row: &JoinRow,
    ) -> Option<Event> {
        let batch = row.to_batch().ok()?;
        let schema = self.output_schema.as_ref()?;

        let num_rows = batch.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::new();

        match side {
            JoinSide::Left => {
                // Left columns are populated, right columns are null
                columns.extend(batch.columns().iter().cloned());

                // Add null columns for right side
                if let Some(right_schema) = &self.right_schema {
                    for field in right_schema.fields() {
                        columns.push(Self::create_null_array(field.data_type(), num_rows));
                    }
                }
            }
            JoinSide::Right => {
                // Left columns are null, right columns are populated
                if let Some(left_schema) = &self.left_schema {
                    for field in left_schema.fields() {
                        columns.push(Self::create_null_array(field.data_type(), num_rows));
                    }
                }

                columns.extend(batch.columns().iter().cloned());
            }
        }

        let joined_batch = RecordBatch::try_new(Arc::clone(schema), columns).ok()?;

        Some(Event {
            timestamp: row.timestamp,
            data: joined_batch,
        })
    }

    /// Creates a null array of the given type and length.
    fn create_null_array(data_type: &DataType, num_rows: usize) -> ArrayRef {
        match data_type {
            DataType::Int64 => {
                Arc::new(Int64Array::from(vec![None; num_rows])) as ArrayRef
            }
            DataType::Utf8 => {
                Arc::new(StringArray::from(vec![None::<&str>; num_rows])) as ArrayRef
            }
            // Add more types as needed
            _ => Arc::new(Int64Array::from(vec![None; num_rows])) as ArrayRef,
        }
    }

    /// Handles cleanup timer expiration.
    fn handle_cleanup_timer(
        &mut self,
        _side: JoinSide,
        state_key: &[u8],
        ctx: &mut OperatorContext,
    ) -> OutputVec {
        let output = OutputVec::new();

        // Delete the state entry
        if ctx.state.delete(state_key).is_ok() {
            self.metrics.state_cleanups += 1;
        }

        output
    }

    /// Handles unmatched timer expiration for outer joins.
    fn handle_unmatched_timer(
        &mut self,
        side: JoinSide,
        state_key: &[u8],
        ctx: &mut OperatorContext,
    ) -> OutputVec {
        let mut output = OutputVec::new();

        // Get the join row
        let Ok(Some(row)) = ctx.state.get_typed::<JoinRow>(state_key) else {
            return output;
        };

        // Only emit if not matched
        if !row.matched {
            match side {
                JoinSide::Left if self.join_type.emits_unmatched_left() => {
                    self.metrics.unmatched_left += 1;
                    if let Some(event) = self.create_unmatched_event(side, &row) {
                        output.push(Output::Event(event));
                    }
                }
                JoinSide::Right if self.join_type.emits_unmatched_right() => {
                    self.metrics.unmatched_right += 1;
                    if let Some(event) = self.create_unmatched_event(side, &row) {
                        output.push(Output::Event(event));
                    }
                }
                _ => {}
            }
        }

        output
    }

    /// Parses a timer key to determine its type and extract the state key.
    fn parse_timer_key(key: &[u8]) -> Option<(TimerKeyType, JoinSide, Vec<u8>)> {
        if key.is_empty() {
            return None;
        }

        match key[0] {
            LEFT_TIMER_PREFIX => {
                let state_key = key[1..].to_vec();
                Some((TimerKeyType::Cleanup, JoinSide::Left, state_key))
            }
            RIGHT_TIMER_PREFIX => {
                let state_key = key[1..].to_vec();
                Some((TimerKeyType::Cleanup, JoinSide::Right, state_key))
            }
            UNMATCHED_TIMER_PREFIX => {
                if key.len() < 2 {
                    return None;
                }
                let side = match key[1] {
                    0x01 => JoinSide::Left,
                    0x02 => JoinSide::Right,
                    _ => return None,
                };
                let state_key = key[2..].to_vec();
                Some((TimerKeyType::Unmatched, side, state_key))
            }
            _ => None,
        }
    }
}

/// Type of timer key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimerKeyType {
    /// Cleanup timer - delete state entry.
    Cleanup,
    /// Unmatched timer - emit unmatched event for outer joins.
    Unmatched,
}

impl Operator for StreamJoinOperator {
    fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec {
        // Default to left side - in practice, users should call process_side directly
        self.process_left(event, ctx)
    }

    fn on_timer(&mut self, timer: Timer, ctx: &mut OperatorContext) -> OutputVec {
        let Some((timer_type, side, state_key)) = Self::parse_timer_key(&timer.key) else {
            return OutputVec::new();
        };

        match timer_type {
            TimerKeyType::Cleanup => self.handle_cleanup_timer(side, &state_key, ctx),
            TimerKeyType::Unmatched => self.handle_unmatched_timer(side, &state_key, ctx),
        }
    }

    fn checkpoint(&self) -> OperatorState {
        // Checkpoint the metrics and configuration
        let checkpoint_data = (
            self.left_key_column.clone(),
            self.right_key_column.clone(),
            self.time_bound_ms,
            self.metrics.left_events,
            self.metrics.right_events,
            self.metrics.matches,
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
        // Type alias for checkpoint data tuple
        type CheckpointData = (String, String, i64, u64, u64, u64);

        if state.operator_id != self.operator_id {
            return Err(OperatorError::StateAccessFailed(format!(
                "Operator ID mismatch: expected {}, got {}",
                self.operator_id, state.operator_id
            )));
        }

        // Restore metrics from checkpoint
        let archived = rkyv::access::<rkyv::Archived<CheckpointData>, RkyvError>(&state.data)
            .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;
        let (_, _, _, left_events, right_events, matches) =
            rkyv::deserialize::<CheckpointData, RkyvError>(archived)
                .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;

        self.metrics.left_events = left_events;
        self.metrics.right_events = right_events;
        self.metrics.matches = matches;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::InMemoryStore;
    use crate::time::{BoundedOutOfOrdernessGenerator, TimerService, WatermarkGenerator};
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn create_order_event(timestamp: i64, order_id: &str, amount: i64) -> Event {
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![order_id])),
                Arc::new(Int64Array::from(vec![amount])),
            ],
        )
        .unwrap();
        Event { timestamp, data: batch }
    }

    fn create_payment_event(timestamp: i64, order_id: &str, status: &str) -> Event {
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![order_id])),
                Arc::new(StringArray::from(vec![status])),
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
    fn test_join_type_properties() {
        assert!(!JoinType::Inner.emits_unmatched_left());
        assert!(!JoinType::Inner.emits_unmatched_right());

        assert!(JoinType::Left.emits_unmatched_left());
        assert!(!JoinType::Left.emits_unmatched_right());

        assert!(!JoinType::Right.emits_unmatched_left());
        assert!(JoinType::Right.emits_unmatched_right());

        assert!(JoinType::Full.emits_unmatched_left());
        assert!(JoinType::Full.emits_unmatched_right());
    }

    #[test]
    fn test_join_operator_creation() {
        let operator = StreamJoinOperator::new(
            "order_id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(3600),
            JoinType::Inner,
        );

        assert_eq!(operator.join_type(), JoinType::Inner);
        assert_eq!(operator.time_bound_ms(), 3600_000);
    }

    #[test]
    fn test_join_operator_with_id() {
        let operator = StreamJoinOperator::with_id(
            "order_id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(3600),
            JoinType::Left,
            "test_join".to_string(),
        );

        assert_eq!(operator.operator_id, "test_join");
        assert_eq!(operator.join_type(), JoinType::Left);
    }

    #[test]
    fn test_inner_join_basic() {
        let mut operator = StreamJoinOperator::with_id(
            "order_id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(3600),
            JoinType::Inner,
            "test_join".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process left event (order)
        let order = create_order_event(1000, "order_1", 100);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            let outputs = operator.process_side(&order, JoinSide::Left, &mut ctx);
            // No match yet, should produce no output
            assert!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count() == 0);
        }

        // Process right event (payment) - should produce a match
        let payment = create_payment_event(2000, "order_1", "paid");
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            let outputs = operator.process_side(&payment, JoinSide::Right, &mut ctx);
            // Should have one match
            assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);
        }

        assert_eq!(operator.metrics().matches, 1);
        assert_eq!(operator.metrics().left_events, 1);
        assert_eq!(operator.metrics().right_events, 1);
    }

    #[test]
    fn test_inner_join_no_match_different_key() {
        let mut operator = StreamJoinOperator::with_id(
            "order_id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(3600),
            JoinType::Inner,
            "test_join".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process left event
        let order = create_order_event(1000, "order_1", 100);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_side(&order, JoinSide::Left, &mut ctx);
        }

        // Process right event with different key
        let payment = create_payment_event(2000, "order_2", "paid");
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            let outputs = operator.process_side(&payment, JoinSide::Right, &mut ctx);
            // No match due to different key
            assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 0);
        }

        assert_eq!(operator.metrics().matches, 0);
    }

    #[test]
    fn test_inner_join_no_match_outside_time_bound() {
        let mut operator = StreamJoinOperator::with_id(
            "order_id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(1), // 1 second time bound
            JoinType::Inner,
            "test_join".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process left event at t=1000
        let order = create_order_event(1000, "order_1", 100);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_side(&order, JoinSide::Left, &mut ctx);
        }

        // Process right event at t=5000 (4 seconds later, outside 1s bound)
        let payment = create_payment_event(5000, "order_1", "paid");
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            let outputs = operator.process_side(&payment, JoinSide::Right, &mut ctx);
            // No match due to time bound
            assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 0);
        }

        assert_eq!(operator.metrics().matches, 0);
    }

    #[test]
    fn test_join_multiple_matches() {
        let mut operator = StreamJoinOperator::with_id(
            "order_id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(3600),
            JoinType::Inner,
            "test_join".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process two left events with same key
        for ts in [1000, 2000] {
            let order = create_order_event(ts, "order_1", 100);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_side(&order, JoinSide::Left, &mut ctx);
        }

        // Process right event - should match both
        let payment = create_payment_event(1500, "order_1", "paid");
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            let outputs = operator.process_side(&payment, JoinSide::Right, &mut ctx);
            // Should have two matches
            assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 2);
        }

        assert_eq!(operator.metrics().matches, 2);
    }

    #[test]
    fn test_join_late_event() {
        let mut operator = StreamJoinOperator::with_id(
            "order_id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(1),
            JoinType::Inner,
            "test_join".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

        // Advance watermark significantly
        let future_order = create_order_event(10000, "order_2", 200);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_side(&future_order, JoinSide::Left, &mut ctx);
        }

        // Process very late event
        let late_payment = create_payment_event(100, "order_1", "paid");
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            let outputs = operator.process_side(&late_payment, JoinSide::Right, &mut ctx);
            // Should be marked as late
            assert!(outputs.iter().any(|o| matches!(o, Output::LateEvent(_))));
        }

        assert_eq!(operator.metrics().late_events, 1);
    }

    #[test]
    fn test_join_row_serialization() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["test"])),
                Arc::new(Int64Array::from(vec![42])),
            ],
        )
        .unwrap();

        let row = JoinRow::new(1000, b"key".to_vec(), &batch).unwrap();

        // Verify we can deserialize back
        let restored_batch = row.to_batch().unwrap();
        assert_eq!(restored_batch.num_rows(), 1);
        assert_eq!(restored_batch.num_columns(), 2);
    }

    #[test]
    fn test_cleanup_timer() {
        let mut operator = StreamJoinOperator::with_id(
            "order_id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(1),
            JoinType::Inner,
            "test_join".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process an event
        let order = create_order_event(1000, "order_1", 100);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_side(&order, JoinSide::Left, &mut ctx);
        }

        // State should have one entry
        assert!(state.len() > 0);
        let initial_state_len = state.len();

        // Get the registered timers and fire them
        let registered_timers = timers.poll_timers(2001); // After cleanup time
        assert!(!registered_timers.is_empty());

        // Fire the cleanup timer - convert TimerRegistration to Timer
        for timer_reg in registered_timers {
            let timer = Timer {
                key: timer_reg.key.unwrap_or_default(),
                timestamp: timer_reg.timestamp,
            };
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.on_timer(timer, &mut ctx);
        }

        // Verify cleanup happened (state decreased)
        assert!(state.len() < initial_state_len || operator.metrics().state_cleanups > 0);
    }

    #[test]
    fn test_checkpoint_restore() {
        let mut operator = StreamJoinOperator::with_id(
            "order_id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(3600),
            JoinType::Inner,
            "test_join".to_string(),
        );

        // Simulate some activity
        operator.metrics.left_events = 10;
        operator.metrics.right_events = 5;
        operator.metrics.matches = 3;

        // Checkpoint
        let checkpoint = operator.checkpoint();

        // Create new operator and restore
        let mut restored = StreamJoinOperator::with_id(
            "order_id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(3600),
            JoinType::Inner,
            "test_join".to_string(),
        );

        restored.restore(checkpoint).unwrap();

        assert_eq!(restored.metrics().left_events, 10);
        assert_eq!(restored.metrics().right_events, 5);
        assert_eq!(restored.metrics().matches, 3);
    }

    #[test]
    fn test_metrics_reset() {
        let mut operator = StreamJoinOperator::new(
            "order_id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(3600),
            JoinType::Inner,
        );

        operator.metrics.left_events = 10;
        operator.metrics.matches = 5;

        operator.reset_metrics();

        assert_eq!(operator.metrics().left_events, 0);
        assert_eq!(operator.metrics().matches, 0);
    }

    #[test]
    fn test_bidirectional_join() {
        let mut operator = StreamJoinOperator::with_id(
            "order_id".to_string(),
            "order_id".to_string(),
            Duration::from_secs(3600),
            JoinType::Inner,
            "test_join".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Right event arrives first
        let payment = create_payment_event(1000, "order_1", "paid");
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            let outputs = operator.process_side(&payment, JoinSide::Right, &mut ctx);
            assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 0);
        }

        // Left event arrives and matches
        let order = create_order_event(1500, "order_1", 100);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            let outputs = operator.process_side(&order, JoinSide::Left, &mut ctx);
            assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);
        }

        assert_eq!(operator.metrics().matches, 1);
    }

    #[test]
    fn test_integer_key_join() {
        fn create_int_key_event(timestamp: i64, key: i64, value: i64) -> Event {
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![key])),
                    Arc::new(Int64Array::from(vec![value])),
                ],
            )
            .unwrap();
            Event { timestamp, data: batch }
        }

        let mut operator = StreamJoinOperator::with_id(
            "key".to_string(),
            "key".to_string(),
            Duration::from_secs(3600),
            JoinType::Inner,
            "test_join".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process left with integer key
        let left = create_int_key_event(1000, 42, 100);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process_side(&left, JoinSide::Left, &mut ctx);
        }

        // Process right with same integer key
        let right = create_int_key_event(1500, 42, 200);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            let outputs = operator.process_side(&right, JoinSide::Right, &mut ctx);
            assert_eq!(outputs.iter().filter(|o| matches!(o, Output::Event(_))).count(), 1);
        }

        assert_eq!(operator.metrics().matches, 1);
    }
}
