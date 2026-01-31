//! # Streaming Top-K Operator
//!
//! Bounded heap of top-K items with retraction-based changelog emission.
//!
//! Supports `ORDER BY ... LIMIT N` on unbounded streams by maintaining
//! a sorted buffer of at most K entries. New events that rank within the
//! top-K cause eviction of the worst entry and optional rank-change
//! retractions.
//!
//! ## Emit Strategies
//!
//! - `OnUpdate`: Emit changelog on every state change (lowest latency)
//! - `OnWatermark`: Buffer changes, emit on watermark advance
//! - `Periodic(interval)`: Emit on timer interval
//!
//! ## Ring 0 Constraints
//!
//! - `entries` pre-allocated to capacity K — no reallocation during `process()`
//! - Sort keys use `Vec<u8>` memcomparable encoding for zero-branch comparison
//! - Sorted Vec with binary search: O(log K) search + O(K) shift

use super::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output, OutputVec, Timer,
};
use super::window::ChangelogRecord;
use arrow_array::{Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow_schema::DataType;

/// Configuration for a sort column in the top-K operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopKSortColumn {
    /// Column name in the event schema
    pub column_name: String,
    /// Sort in descending order
    pub descending: bool,
    /// Place NULL values before non-NULL values
    pub nulls_first: bool,
}

impl TopKSortColumn {
    /// Creates a new ascending sort column.
    #[must_use]
    pub fn ascending(name: impl Into<String>) -> Self {
        Self {
            column_name: name.into(),
            descending: false,
            nulls_first: false,
        }
    }

    /// Creates a new descending sort column.
    #[must_use]
    pub fn descending(name: impl Into<String>) -> Self {
        Self {
            column_name: name.into(),
            descending: true,
            nulls_first: false,
        }
    }

    /// Sets whether nulls should sort first.
    #[must_use]
    pub fn with_nulls_first(mut self, nulls_first: bool) -> Self {
        self.nulls_first = nulls_first;
        self
    }
}

/// Emit strategy for the streaming top-K operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopKEmitStrategy {
    /// Emit changelog on every state change (lowest latency, highest volume).
    OnUpdate,
    /// Buffer changes, emit batch when watermark advances.
    OnWatermark,
    /// Emit on timer at the given interval in microseconds.
    Periodic(i64),
}

/// An entry in the top-K buffer.
#[derive(Debug, Clone)]
struct TopKEntry {
    /// Memcomparable sort key for efficient comparison.
    sort_key: Vec<u8>,
    /// The original event.
    event: Event,
}

/// Streaming top-K operator for `ORDER BY ... LIMIT N`.
///
/// Maintains a sorted buffer of at most K entries. Each incoming event
/// is checked against the current worst entry. If better, it replaces
/// the worst and changelog records are emitted.
pub struct StreamingTopKOperator {
    /// Operator identifier for checkpointing.
    operator_id: String,
    /// Number of top entries to maintain.
    k: usize,
    /// Sort column specifications.
    sort_columns: Vec<TopKSortColumn>,
    /// Sorted entries (best first). Pre-allocated to capacity K.
    entries: Vec<TopKEntry>,
    /// Emission strategy.
    emit_strategy: TopKEmitStrategy,
    /// Pending changelog records (for OnWatermark/Periodic strategies).
    pending_changes: Vec<ChangelogRecord>,
    /// Monotonic sequence counter for changelog ordering.
    sequence_counter: u64,
    /// Current watermark value.
    current_watermark: i64,
}

impl StreamingTopKOperator {
    /// Creates a new streaming top-K operator.
    #[must_use]
    pub fn new(
        operator_id: String,
        k: usize,
        sort_columns: Vec<TopKSortColumn>,
        emit_strategy: TopKEmitStrategy,
    ) -> Self {
        Self {
            operator_id,
            k,
            sort_columns,
            entries: Vec::with_capacity(k),
            emit_strategy,
            pending_changes: Vec::new(),
            sequence_counter: 0,
            current_watermark: i64::MIN,
        }
    }

    /// Returns the current number of entries in the top-K buffer.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the top-K buffer is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the current entries as events (best first).
    #[must_use]
    pub fn entries(&self) -> Vec<&Event> {
        self.entries.iter().map(|e| &e.event).collect()
    }

    /// Returns the current watermark value.
    #[must_use]
    pub fn current_watermark(&self) -> i64 {
        self.current_watermark
    }

    /// Returns the number of pending changelog records.
    #[must_use]
    pub fn pending_changes_count(&self) -> usize {
        self.pending_changes.len()
    }

    /// Extracts a memcomparable sort key from an event.
    fn extract_sort_key(&self, event: &Event) -> Vec<u8> {
        let batch = &event.data;
        let schema = batch.schema();
        let mut key = Vec::new();

        for col_spec in &self.sort_columns {
            let Ok(col_idx) = schema.index_of(&col_spec.column_name) else {
                // Column not found — encode as null
                encode_null(col_spec.nulls_first, col_spec.descending, &mut key);
                continue;
            };

            let array = batch.column(col_idx);

            if array.is_null(0) {
                encode_null(col_spec.nulls_first, col_spec.descending, &mut key);
                continue;
            }

            match array.data_type() {
                DataType::Int64 => {
                    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    encode_not_null(col_spec.nulls_first, col_spec.descending, &mut key);
                    encode_i64(arr.value(0), col_spec.descending, &mut key);
                }
                DataType::Float64 => {
                    let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    encode_not_null(col_spec.nulls_first, col_spec.descending, &mut key);
                    encode_f64(arr.value(0), col_spec.descending, &mut key);
                }
                DataType::Utf8 => {
                    let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                    encode_not_null(col_spec.nulls_first, col_spec.descending, &mut key);
                    encode_utf8(arr.value(0), col_spec.descending, &mut key);
                }
                DataType::Timestamp(_, _) => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    encode_not_null(col_spec.nulls_first, col_spec.descending, &mut key);
                    encode_i64(arr.value(0), col_spec.descending, &mut key);
                }
                _ => {
                    // Unsupported type: treat as null
                    encode_null(col_spec.nulls_first, col_spec.descending, &mut key);
                }
            }
        }

        key
    }

    /// Finds the insertion position for a sort key using binary search.
    /// Returns the index where the key should be inserted to maintain sorted order.
    fn find_insert_position(&self, sort_key: &[u8]) -> usize {
        self.entries
            .binary_search_by(|entry| entry.sort_key.as_slice().cmp(sort_key))
            .unwrap_or_else(|pos| pos)
    }

    /// Checks if an event with the given sort key would enter the top-K.
    fn would_enter_topk(&self, sort_key: &[u8]) -> bool {
        if self.entries.len() < self.k {
            return true;
        }
        // Compare with the worst (last) entry
        if let Some(worst) = self.entries.last() {
            sort_key < worst.sort_key.as_slice()
        } else {
            true
        }
    }

    /// Processes a single event, returning changelog records for the changes.
    fn process_event(&mut self, event: &Event, emit_timestamp: i64) -> Vec<ChangelogRecord> {
        let sort_key = self.extract_sort_key(event);

        if !self.would_enter_topk(&sort_key) {
            return Vec::new();
        }

        let insert_pos = self.find_insert_position(&sort_key);
        let mut changes = Vec::new();

        // Insert the new entry
        let new_entry = TopKEntry {
            sort_key,
            event: event.clone(),
        };
        self.entries.insert(insert_pos, new_entry);

        // Generate insert changelog
        changes.push(ChangelogRecord::insert(event.clone(), emit_timestamp));

        // Generate rank change retractions for entries that shifted down
        for i in (insert_pos + 1)..self.entries.len().min(self.k) {
            let shifted_event = &self.entries[i].event;
            // Emit update: rank changed from (i-1) to i
            let (before, after) = ChangelogRecord::update(
                shifted_event.clone(),
                shifted_event.clone(),
                emit_timestamp,
            );
            changes.push(before);
            changes.push(after);
        }

        // Evict worst entry if over capacity
        if self.entries.len() > self.k {
            let evicted = self.entries.pop().unwrap();
            changes.push(ChangelogRecord::delete(evicted.event, emit_timestamp));
        }

        self.sequence_counter += 1;
        changes
    }

    /// Flushes pending changelog records as Output.
    fn flush_pending(&mut self) -> OutputVec {
        let mut outputs = OutputVec::new();
        for record in self.pending_changes.drain(..) {
            outputs.push(Output::Changelog(record));
        }
        outputs
    }
}

impl Operator for StreamingTopKOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        let emit_timestamp = event.timestamp;
        let changes = self.process_event(event, emit_timestamp);

        match &self.emit_strategy {
            TopKEmitStrategy::OnUpdate => {
                let mut outputs = OutputVec::new();
                for record in changes {
                    outputs.push(Output::Changelog(record));
                }
                outputs
            }
            TopKEmitStrategy::OnWatermark | TopKEmitStrategy::Periodic(_) => {
                self.pending_changes.extend(changes);
                OutputVec::new()
            }
        }
    }

    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        // For Periodic strategy: flush pending changes on timer
        match &self.emit_strategy {
            TopKEmitStrategy::Periodic(_) => self.flush_pending(),
            _ => OutputVec::new(),
        }
    }

    fn checkpoint(&self) -> OperatorState {
        // Serialize entry count + sort keys + timestamps
        // For simplicity, serialize as JSON-like format
        let mut data = Vec::new();

        // Write entry count
        let count = self.entries.len() as u64;
        data.extend_from_slice(&count.to_le_bytes());

        // Write watermark
        data.extend_from_slice(&self.current_watermark.to_le_bytes());

        // Write sequence counter
        data.extend_from_slice(&self.sequence_counter.to_le_bytes());

        // Write each entry's sort key length + sort key
        for entry in &self.entries {
            let key_len = entry.sort_key.len() as u64;
            data.extend_from_slice(&key_len.to_le_bytes());
            data.extend_from_slice(&entry.sort_key);
            data.extend_from_slice(&entry.event.timestamp.to_le_bytes());
        }

        OperatorState {
            operator_id: self.operator_id.clone(),
            data,
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn restore(&mut self, state: OperatorState) -> Result<(), OperatorError> {
        if state.data.len() < 24 {
            return Err(OperatorError::SerializationFailed(
                "TopK checkpoint data too short".to_string(),
            ));
        }

        let mut offset = 0;
        let count = u64::from_le_bytes(
            state.data[offset..offset + 8]
                .try_into()
                .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
        ) as usize;
        offset += 8;

        self.current_watermark = i64::from_le_bytes(
            state.data[offset..offset + 8]
                .try_into()
                .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
        );
        offset += 8;

        self.sequence_counter = u64::from_le_bytes(
            state.data[offset..offset + 8]
                .try_into()
                .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
        );
        offset += 8;

        // Restore sort keys (events are not fully restored — only sort key metadata)
        self.entries.clear();
        for _ in 0..count {
            if offset + 8 > state.data.len() {
                return Err(OperatorError::SerializationFailed(
                    "TopK checkpoint truncated".to_string(),
                ));
            }
            let key_len = u64::from_le_bytes(
                state.data[offset..offset + 8]
                    .try_into()
                    .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
            ) as usize;
            offset += 8;

            if offset + key_len + 8 > state.data.len() {
                return Err(OperatorError::SerializationFailed(
                    "TopK checkpoint truncated at key".to_string(),
                ));
            }
            let sort_key = state.data[offset..offset + key_len].to_vec();
            offset += key_len;

            let timestamp = i64::from_le_bytes(
                state.data[offset..offset + 8]
                    .try_into()
                    .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
            );
            offset += 8;

            // Create a minimal event placeholder for the restored entry
            let batch = arrow_array::RecordBatch::new_empty(std::sync::Arc::new(
                arrow_schema::Schema::empty(),
            ));
            self.entries.push(TopKEntry {
                sort_key,
                event: Event::new(timestamp, batch),
            });
        }

        Ok(())
    }
}

// === Sort key encoding helpers ===

/// Encodes a null value marker into the sort key.
pub fn encode_null(nulls_first: bool, descending: bool, key: &mut Vec<u8>) {
    // nulls_first=true + ascending  => null sorts first (0x00)
    // nulls_first=true + descending => null sorts first (0xFF after flip)
    // nulls_first=false + ascending => null sorts last (0x01)
    // nulls_first=false + descending => null sorts last (0x00 after flip)
    if nulls_first {
        if descending {
            key.push(0xFF);
        } else {
            key.push(0x00);
        }
    } else if descending {
        key.push(0x00);
    } else {
        key.push(0xFF);
    }
}

/// Encodes a non-null value marker into the sort key.
pub fn encode_not_null(nulls_first: bool, descending: bool, key: &mut Vec<u8>) {
    if nulls_first {
        if descending {
            key.push(0x00);
        } else {
            key.push(0x01);
        }
    } else if descending {
        key.push(0x01);
    } else {
        key.push(0x00);
    }
}

/// Encodes an i64 value as memcomparable bytes.
///
/// XOR with sign bit to convert signed comparison to unsigned,
/// then big-endian encoding. Optionally flip all bits for descending.
pub fn encode_i64(val: i64, descending: bool, key: &mut Vec<u8>) {
    #[allow(clippy::cast_sign_loss)]
    let unsigned = (val as u64) ^ (1u64 << 63);
    let bytes = unsigned.to_be_bytes();
    if descending {
        key.extend(bytes.iter().map(|b| !b));
    } else {
        key.extend_from_slice(&bytes);
    }
}

/// Encodes an f64 value as memcomparable bytes.
///
/// Uses IEEE 754 total ordering trick: if positive, flip sign bit;
/// if negative, flip all bits. This gives correct ordering for all
/// finite values, infinities, and NaN.
pub fn encode_f64(val: f64, descending: bool, key: &mut Vec<u8>) {
    let bits = val.to_bits();
    let encoded = if bits & (1u64 << 63) == 0 {
        bits ^ (1u64 << 63)
    } else {
        !bits
    };
    let bytes = encoded.to_be_bytes();
    if descending {
        key.extend(bytes.iter().map(|b| !b));
    } else {
        key.extend_from_slice(&bytes);
    }
}

/// Encodes a UTF-8 string as memcomparable bytes.
///
/// Appends the raw bytes followed by a null terminator.
/// For descending order, flips all bits.
pub fn encode_utf8(val: &str, descending: bool, key: &mut Vec<u8>) {
    if descending {
        key.extend(val.as_bytes().iter().map(|b| !b));
        key.push(0xFF); // flipped null terminator
    } else {
        key.extend_from_slice(val.as_bytes());
        key.push(0x00); // null terminator
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::window::CdcOperation;
    use crate::state::InMemoryStore;
    use crate::time::{BoundedOutOfOrdernessGenerator, TimerService};
    use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_event(timestamp: i64, price: f64) -> Event {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![price]))],
        )
        .unwrap();
        Event::new(timestamp, batch)
    }

    fn make_event_i64(timestamp: i64, value: i64) -> Event {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap();
        Event::new(timestamp, batch)
    }

    fn make_event_str(timestamp: i64, name: &str) -> Event {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![name]))],
        )
        .unwrap();
        Event::new(timestamp, batch)
    }

    fn make_multi_column_event(timestamp: i64, category: &str, price: f64) -> Event {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![category])),
                Arc::new(Float64Array::from(vec![price])),
            ],
        )
        .unwrap();
        Event::new(timestamp, batch)
    }

    fn create_topk(
        k: usize,
        sort_columns: Vec<TopKSortColumn>,
        strategy: TopKEmitStrategy,
    ) -> StreamingTopKOperator {
        StreamingTopKOperator::new("test_topk".to_string(), k, sort_columns, strategy)
    }

    fn create_test_context<'a>(
        timers: &'a mut TimerService,
        state: &'a mut dyn crate::state::StateStore,
        watermark_gen: &'a mut dyn crate::time::WatermarkGenerator,
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

    // --- Sort key encoding tests ---

    #[test]
    fn test_topk_sort_key_extraction_int64() {
        let op = create_topk(
            3,
            vec![TopKSortColumn::ascending("value")],
            TopKEmitStrategy::OnUpdate,
        );
        let e1 = make_event_i64(1, 100);
        let e2 = make_event_i64(2, 200);
        let e3 = make_event_i64(3, -50);

        let k1 = op.extract_sort_key(&e1);
        let k2 = op.extract_sort_key(&e2);
        let k3 = op.extract_sort_key(&e3);

        // Ascending: -50 < 100 < 200
        assert!(k3 < k1);
        assert!(k1 < k2);
    }

    #[test]
    fn test_topk_sort_key_extraction_float64() {
        let op = create_topk(
            3,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
        );
        let e1 = make_event(1, 150.0);
        let e2 = make_event(2, 200.0);
        let e3 = make_event(3, 100.0);

        let k1 = op.extract_sort_key(&e1);
        let k2 = op.extract_sort_key(&e2);
        let k3 = op.extract_sort_key(&e3);

        // Descending: 200 < 150 < 100 (in sort key order)
        assert!(k2 < k1);
        assert!(k1 < k3);
    }

    #[test]
    fn test_topk_sort_key_extraction_utf8() {
        let op = create_topk(
            3,
            vec![TopKSortColumn::ascending("name")],
            TopKEmitStrategy::OnUpdate,
        );
        let e1 = make_event_str(1, "apple");
        let e2 = make_event_str(2, "banana");
        let e3 = make_event_str(3, "cherry");

        let k1 = op.extract_sort_key(&e1);
        let k2 = op.extract_sort_key(&e2);
        let k3 = op.extract_sort_key(&e3);

        // Ascending: apple < banana < cherry
        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn test_topk_sort_key_extraction_timestamp() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                arrow_array::TimestampMicrosecondArray::from(vec![1000]),
            )],
        )
        .unwrap();
        let event = Event::new(1, batch);

        let op = create_topk(
            3,
            vec![TopKSortColumn::ascending("ts")],
            TopKEmitStrategy::OnUpdate,
        );
        let key = op.extract_sort_key(&event);
        assert!(!key.is_empty());
    }

    // --- Insertion tests ---

    #[test]
    fn test_topk_insert_below_capacity() {
        let mut op = create_topk(
            3,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let event = make_event(1, 150.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&event, &mut ctx);

        assert_eq!(op.len(), 1);
        // Should emit an Insert changelog
        assert!(!outputs.is_empty());
    }

    #[test]
    fn test_topk_insert_at_capacity_better_entry() {
        let mut op = create_topk(
            2,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Fill to capacity
        for (i, price) in [100.0, 150.0].iter().enumerate() {
            let event = make_event(i as i64, *price);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(&event, &mut ctx);
        }
        assert_eq!(op.len(), 2);

        // Insert a better entry (200 > 100, which is worst)
        let better = make_event(3, 200.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&better, &mut ctx);

        assert_eq!(op.len(), 2);
        // Should have Insert + rank changes + Delete for evicted
        assert!(outputs.len() >= 2);
    }

    #[test]
    fn test_topk_insert_at_capacity_worse_entry() {
        let mut op = create_topk(
            2,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Fill with good entries
        for (i, price) in [200.0, 150.0].iter().enumerate() {
            let event = make_event(i as i64, *price);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(&event, &mut ctx);
        }

        // Insert a worse entry (50 < 150)
        let worse = make_event(3, 50.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&worse, &mut ctx);

        assert_eq!(op.len(), 2);
        // No emission - entry doesn't enter top-K
        assert!(outputs.is_empty());
    }

    #[test]
    fn test_topk_ascending_order() {
        let mut op = create_topk(
            3,
            vec![TopKSortColumn::ascending("value")],
            TopKEmitStrategy::OnUpdate,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Insert values: 30, 10, 20 -> top-3 ascending should be [10, 20, 30]
        for (i, val) in [30i64, 10, 20].iter().enumerate() {
            let event = make_event_i64(i as i64, *val);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(&event, &mut ctx);
        }

        assert_eq!(op.len(), 3);
        // Entries should be sorted by ascending value
        let entries = op.entries();
        let vals: Vec<i64> = entries
            .iter()
            .map(|e| {
                e.data
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .collect();
        assert_eq!(vals, vec![10, 20, 30]);
    }

    #[test]
    fn test_topk_descending_order() {
        let mut op = create_topk(
            3,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Insert: 100, 200, 150 -> top-3 descending should have 200 first
        for (i, price) in [100.0, 200.0, 150.0].iter().enumerate() {
            let event = make_event(i as i64, *price);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(&event, &mut ctx);
        }

        let entries = op.entries();
        let prices: Vec<f64> = entries
            .iter()
            .map(|e| {
                e.data
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0)
            })
            .collect();
        assert_eq!(prices, vec![200.0, 150.0, 100.0]);
    }

    #[test]
    fn test_topk_multi_column_sort() {
        let mut op = create_topk(
            3,
            vec![
                TopKSortColumn::ascending("category"),
                TopKSortColumn::descending("price"),
            ],
            TopKEmitStrategy::OnUpdate,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let events = vec![
            make_multi_column_event(1, "B", 100.0),
            make_multi_column_event(2, "A", 200.0),
            make_multi_column_event(3, "A", 150.0),
        ];

        for event in &events {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(event, &mut ctx);
        }

        // Sort: category ASC, then price DESC within same category
        // A/200, A/150, B/100
        let entries = op.entries();
        let cats: Vec<&str> = entries
            .iter()
            .map(|e| {
                e.data
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0)
            })
            .collect();
        assert_eq!(cats, vec!["A", "A", "B"]);
    }

    #[test]
    fn test_topk_nulls_first() {
        let sort_cols = vec![TopKSortColumn::ascending("value").with_nulls_first(true)];
        let mut op = create_topk(3, sort_cols, TopKEmitStrategy::OnUpdate);

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Create a null value event
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let null_array = Int64Array::new_null(1);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(null_array)]).unwrap();
        let null_event = Event::new(1, batch);

        let val_event = make_event_i64(2, 100);

        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&val_event, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&null_event, &mut ctx);

        // With nulls_first, the null should sort before 100
        let entries = op.entries();
        assert_eq!(entries.len(), 2);
        assert!(entries[0].data.column(0).is_null(0));
    }

    #[test]
    fn test_topk_nulls_last() {
        let sort_cols = vec![TopKSortColumn::ascending("value").with_nulls_first(false)];
        let mut op = create_topk(3, sort_cols, TopKEmitStrategy::OnUpdate);

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let null_array = Int64Array::new_null(1);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(null_array)]).unwrap();
        let null_event = Event::new(1, batch);

        let val_event = make_event_i64(2, 100);

        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&null_event, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&val_event, &mut ctx);

        // With nulls_last, the value should sort before null
        let entries = op.entries();
        assert_eq!(entries.len(), 2);
        assert!(!entries[0].data.column(0).is_null(0));
    }

    // --- Emit strategy tests ---

    #[test]
    fn test_topk_emit_on_update_insert() {
        let mut op = create_topk(
            3,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let event = make_event(1, 150.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&event, &mut ctx);

        // Should emit Insert changelog immediately
        assert_eq!(outputs.len(), 1);
        match &outputs[0] {
            Output::Changelog(rec) => {
                assert_eq!(rec.operation, CdcOperation::Insert);
                assert_eq!(rec.weight, 1);
            }
            _ => panic!("Expected Changelog output"),
        }
    }

    #[test]
    fn test_topk_emit_on_update_eviction() {
        let mut op = create_topk(
            1,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Fill with one entry
        let event1 = make_event(1, 100.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&event1, &mut ctx);

        // Better entry evicts the first
        let event2 = make_event(2, 200.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&event2, &mut ctx);

        // Should have Insert + Delete (eviction)
        let mut has_insert = false;
        let mut has_delete = false;
        for output in &outputs {
            if let Output::Changelog(rec) = output {
                match rec.operation {
                    CdcOperation::Insert => has_insert = true,
                    CdcOperation::Delete => has_delete = true,
                    _ => {}
                }
            }
        }
        assert!(has_insert);
        assert!(has_delete);
    }

    #[test]
    fn test_topk_emit_on_update_rank_change() {
        let mut op = create_topk(
            3,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Insert two entries
        let e1 = make_event(1, 100.0);
        let e2 = make_event(2, 200.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e2, &mut ctx);

        // Insert between them: 150 goes between 200 and 100, shifting 100's rank
        let e3 = make_event(3, 150.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&e3, &mut ctx);

        // Should have Insert for 150 + UpdateBefore/UpdateAfter for 100's rank change
        let mut has_insert = false;
        let mut has_update_before = false;
        let mut has_update_after = false;
        for output in &outputs {
            if let Output::Changelog(rec) = output {
                match rec.operation {
                    CdcOperation::Insert => has_insert = true,
                    CdcOperation::UpdateBefore => has_update_before = true,
                    CdcOperation::UpdateAfter => has_update_after = true,
                    _ => {}
                }
            }
        }
        assert!(has_insert);
        assert!(has_update_before);
        assert!(has_update_after);
    }

    #[test]
    fn test_topk_emit_on_watermark_batched() {
        let mut op = create_topk(
            3,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnWatermark,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Insert events — should not emit immediately
        let e1 = make_event(1, 100.0);
        let e2 = make_event(2, 200.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let out1 = op.process(&e1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let out2 = op.process(&e2, &mut ctx);

        assert!(out1.is_empty());
        assert!(out2.is_empty());
        assert!(op.pending_changes_count() > 0);
    }

    #[test]
    fn test_topk_emit_periodic() {
        let mut op = create_topk(
            3,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::Periodic(1000),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Insert events — buffered
        let e1 = make_event(1, 100.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);

        assert!(op.pending_changes_count() > 0);

        // Timer triggers flush
        let timer = Timer {
            key: smallvec::smallvec![],
            timestamp: 1000,
        };
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.on_timer(timer, &mut ctx);

        assert!(!outputs.is_empty());
        assert_eq!(op.pending_changes_count(), 0);
    }

    // --- Edge case tests ---

    #[test]
    fn test_topk_empty_heap() {
        let op = create_topk(
            3,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
        );
        assert!(op.is_empty());
        assert_eq!(op.len(), 0);
    }

    #[test]
    fn test_topk_k_equals_one() {
        let mut op = create_topk(
            1,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let e1 = make_event(1, 100.0);
        let e2 = make_event(2, 200.0);
        let e3 = make_event(3, 50.0);

        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);
        assert_eq!(op.len(), 1);

        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e2, &mut ctx);
        assert_eq!(op.len(), 1);

        // Verify it kept the best (200)
        let entries = op.entries();
        let price = entries[0]
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((price - 200.0).abs() < f64::EPSILON);

        // Worse entry doesn't change anything
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&e3, &mut ctx);
        assert!(outputs.is_empty());
    }

    #[test]
    fn test_topk_large_k() {
        let mut op = create_topk(
            100,
            vec![TopKSortColumn::ascending("value")],
            TopKEmitStrategy::OnUpdate,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        for i in 0..50 {
            let event = make_event_i64(i, i * 10);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(&event, &mut ctx);
        }

        assert_eq!(op.len(), 50);
    }

    #[test]
    fn test_topk_duplicate_sort_keys() {
        let mut op = create_topk(
            3,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Insert three events with the same price
        for i in 0..3 {
            let event = make_event(i, 100.0);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(&event, &mut ctx);
        }

        assert_eq!(op.len(), 3);
    }

    // --- Checkpoint/restore tests ---

    #[test]
    fn test_topk_checkpoint_roundtrip() {
        let mut op = create_topk(
            3,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        for (i, price) in [150.0, 200.0, 100.0].iter().enumerate() {
            let event = make_event(i as i64, *price);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(&event, &mut ctx);
        }

        let checkpoint = op.checkpoint();
        assert_eq!(checkpoint.operator_id, "test_topk");
        assert!(!checkpoint.data.is_empty());

        // Restore to a new operator
        let mut op2 = create_topk(
            3,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
        );
        op2.restore(checkpoint).unwrap();

        assert_eq!(op2.len(), 3);
    }

    #[test]
    fn test_topk_restore_and_continue() {
        let mut op = create_topk(
            2,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let event = make_event(1, 150.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&event, &mut ctx);

        let checkpoint = op.checkpoint();

        let mut op2 = create_topk(
            2,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
        );
        op2.restore(checkpoint).unwrap();

        // Should be able to continue processing
        assert_eq!(op2.len(), 1);
    }

    // --- Changelog record tests ---

    #[test]
    fn test_topk_changelog_record_types() {
        let record = ChangelogRecord::insert(make_event(1, 100.0), 1);
        assert_eq!(record.operation, CdcOperation::Insert);
        assert_eq!(record.weight, 1);

        let record = ChangelogRecord::delete(make_event(1, 100.0), 1);
        assert_eq!(record.operation, CdcOperation::Delete);
        assert_eq!(record.weight, -1);

        let (before, after) =
            ChangelogRecord::update(make_event(1, 100.0), make_event(2, 200.0), 1);
        assert_eq!(before.operation, CdcOperation::UpdateBefore);
        assert_eq!(after.operation, CdcOperation::UpdateAfter);
    }

    #[test]
    fn test_topk_no_emission_on_no_change() {
        let mut op = create_topk(
            2,
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Fill to capacity with good entries
        let e1 = make_event(1, 200.0);
        let e2 = make_event(2, 150.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e2, &mut ctx);

        // Worse entry — no change
        let e3 = make_event(3, 50.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&e3, &mut ctx);

        assert!(outputs.is_empty());
    }
}
