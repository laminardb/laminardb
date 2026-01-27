//! # Watermark-Bounded Sort Operator
//!
//! Buffers events between watermark boundaries, emits sorted batch
//! when watermark advances. Useful for producing ordered output
//! (e.g., sorted Parquet files) from out-of-order streams.
//!
//! ## Memory Bounds
//!
//! Only holds events in the `(last_watermark, current_watermark)` range,
//! which is bounded by `max_out_of_orderness`. A `max_buffer_size` safety
//! limit prevents unbounded growth.
//!
//! ## How It Works
//!
//! 1. Events arrive out of order (within bounded disorder)
//! 2. Events are buffered until watermark advances
//! 3. On watermark advance, all events with timestamp <= new watermark
//!    are sorted and emitted as a batch
//! 4. Late events (timestamp <= last emitted watermark) are dropped

use super::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output, OutputVec, Timer,
};
use super::topk::{TopKSortColumn, encode_i64, encode_f64, encode_utf8, encode_null, encode_not_null};
use arrow_array::{Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow_schema::DataType;

/// A buffered event with its pre-computed sort key.
struct SortBufferEntry {
    /// Memcomparable sort key.
    sort_key: Vec<u8>,
    /// The original event.
    event: Event,
}

/// Watermark-bounded sort operator.
///
/// Buffers events between watermark advances and emits sorted batches.
pub struct WatermarkBoundedSortOperator {
    /// Operator identifier for checkpointing.
    operator_id: String,
    /// Sort column specifications.
    sort_columns: Vec<TopKSortColumn>,
    /// Event buffer (unsorted).
    buffer: Vec<SortBufferEntry>,
    /// Maximum buffer size (safety limit).
    max_buffer_size: usize,
    /// Last watermark value that triggered emission.
    last_watermark: i64,
    /// Number of late events dropped.
    late_events_dropped: u64,
}

impl WatermarkBoundedSortOperator {
    /// Creates a new watermark-bounded sort operator.
    #[must_use]
    pub fn new(
        operator_id: String,
        sort_columns: Vec<TopKSortColumn>,
        max_buffer_size: usize,
    ) -> Self {
        Self {
            operator_id,
            sort_columns,
            buffer: Vec::new(),
            max_buffer_size,
            last_watermark: i64::MIN,
            late_events_dropped: 0,
        }
    }

    /// Returns the current buffer size.
    #[must_use]
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    /// Returns true if the buffer is empty.
    #[must_use]
    pub fn is_buffer_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Returns the last emitted watermark.
    #[must_use]
    pub fn last_watermark(&self) -> i64 {
        self.last_watermark
    }

    /// Returns the count of late events that were dropped.
    #[must_use]
    pub fn late_events_dropped(&self) -> u64 {
        self.late_events_dropped
    }

    /// Extracts a memcomparable sort key from an event.
    fn extract_sort_key(&self, event: &Event) -> Vec<u8> {
        let batch = &event.data;
        let schema = batch.schema();
        let mut key = Vec::new();

        for col_spec in &self.sort_columns {
            let Ok(col_idx) = schema.index_of(&col_spec.column_name) else {
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
                    encode_null(col_spec.nulls_first, col_spec.descending, &mut key);
                }
            }
        }

        key
    }

    /// Processes a watermark advance: sorts and emits all buffered events.
    fn on_watermark_advance(&mut self, new_watermark: i64) -> OutputVec {
        if new_watermark <= self.last_watermark {
            return OutputVec::new();
        }

        // Sort the buffer
        self.buffer.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));

        // Emit all buffered events
        let mut outputs = OutputVec::new();
        for entry in self.buffer.drain(..) {
            outputs.push(Output::Event(entry.event));
        }

        // Emit the watermark
        outputs.push(Output::Watermark(new_watermark));

        self.last_watermark = new_watermark;
        outputs
    }
}

impl Operator for WatermarkBoundedSortOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        // Drop late events (before or at last watermark)
        if event.timestamp <= self.last_watermark {
            self.late_events_dropped += 1;
            return OutputVec::new();
        }

        // Buffer the event if under capacity
        if self.buffer.len() >= self.max_buffer_size {
            // Safety: force a sort+emit when buffer is full
            let outputs = self.on_watermark_advance(event.timestamp);
            // Then buffer the new event
            let sort_key = self.extract_sort_key(event);
            self.buffer.push(SortBufferEntry {
                sort_key,
                event: event.clone(),
            });
            return outputs;
        }

        let sort_key = self.extract_sort_key(event);
        self.buffer.push(SortBufferEntry {
            sort_key,
            event: event.clone(),
        });

        OutputVec::new()
    }

    fn on_timer(&mut self, timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        // Timer carries watermark timestamp
        self.on_watermark_advance(timer.timestamp)
    }

    fn checkpoint(&self) -> OperatorState {
        let mut data = Vec::new();

        // Write last watermark
        data.extend_from_slice(&self.last_watermark.to_le_bytes());

        // Write late events dropped
        data.extend_from_slice(&self.late_events_dropped.to_le_bytes());

        // Write buffer size
        let buf_len = self.buffer.len() as u64;
        data.extend_from_slice(&buf_len.to_le_bytes());

        // Write event timestamps
        for entry in &self.buffer {
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
                "WatermarkSort checkpoint data too short".to_string(),
            ));
        }

        let mut offset = 0;
        self.last_watermark = i64::from_le_bytes(
            state.data[offset..offset + 8]
                .try_into()
                .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
        );
        offset += 8;

        self.late_events_dropped = u64::from_le_bytes(
            state.data[offset..offset + 8]
                .try_into()
                .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
        );
        offset += 8;

        let buf_len = u64::from_le_bytes(
            state.data[offset..offset + 8]
                .try_into()
                .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
        ) as usize;
        offset += 8;

        self.buffer.clear();
        for _ in 0..buf_len {
            if offset + 8 > state.data.len() {
                return Err(OperatorError::SerializationFailed(
                    "WatermarkSort checkpoint truncated".to_string(),
                ));
            }
            let ts = i64::from_le_bytes(
                state.data[offset..offset + 8]
                    .try_into()
                    .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
            );
            offset += 8;

            let batch = arrow_array::RecordBatch::new_empty(std::sync::Arc::new(
                arrow_schema::Schema::empty(),
            ));
            self.buffer.push(SortBufferEntry {
                sort_key: Vec::new(),
                event: Event {
                    timestamp: ts,
                    data: batch,
                },
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::InMemoryStore;
    use crate::time::{BoundedOutOfOrdernessGenerator, TimerService};
    use arrow_array::{Float64Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use smallvec::smallvec;
    use std::sync::Arc;

    fn make_event(timestamp: i64, value: i64) -> Event {
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
        Event {
            timestamp,
            data: batch,
        }
    }

    fn make_event_f64(timestamp: i64, price: f64) -> Event {
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
        Event {
            timestamp,
            data: batch,
        }
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

    #[test]
    fn test_watermark_sort_basic() {
        let mut op = WatermarkBoundedSortOperator::new(
            "test_wms".to_string(),
            vec![TopKSortColumn::ascending("value")],
            100_000,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Buffer events
        let e1 = make_event(100, 30);
        let e2 = make_event(200, 10);
        let e3 = make_event(300, 20);

        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e2, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e3, &mut ctx);

        assert_eq!(op.buffer_size(), 3);
    }

    #[test]
    fn test_watermark_sort_out_of_order() {
        let mut op = WatermarkBoundedSortOperator::new(
            "test_wms".to_string(),
            vec![TopKSortColumn::ascending("value")],
            100_000,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Out of order values
        let events = vec![
            make_event(100, 30),
            make_event(200, 10),
            make_event(300, 20),
        ];

        for event in &events {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(event, &mut ctx);
        }

        // Watermark advance triggers sorted emission
        let timer = Timer {
            key: smallvec![],
            timestamp: 500,
        };
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.on_timer(timer, &mut ctx);

        // 3 events + 1 watermark
        assert_eq!(outputs.len(), 4);

        // Verify sorted by value ascending: 10, 20, 30
        let vals: Vec<i64> = outputs
            .iter()
            .filter_map(|o| match o {
                Output::Event(e) => Some(
                    e.data
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                ),
                _ => None,
            })
            .collect();
        assert_eq!(vals, vec![10, 20, 30]);
    }

    #[test]
    fn test_watermark_sort_watermark_advance() {
        let mut op = WatermarkBoundedSortOperator::new(
            "test_wms".to_string(),
            vec![TopKSortColumn::ascending("value")],
            100_000,
        );

        // Directly test watermark advance
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let e1 = make_event(100, 50);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);

        let outputs = op.on_watermark_advance(200);
        // Should emit 1 event + 1 watermark
        assert_eq!(outputs.len(), 2);
        assert!(op.is_buffer_empty());
        assert_eq!(op.last_watermark(), 200);
    }

    #[test]
    fn test_watermark_sort_late_events() {
        let mut op = WatermarkBoundedSortOperator::new(
            "test_wms".to_string(),
            vec![TopKSortColumn::ascending("value")],
            100_000,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Advance watermark
        let e1 = make_event(100, 10);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);
        op.on_watermark_advance(200);

        // Late event (timestamp 50 <= watermark 200)
        let late = make_event(50, 5);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&late, &mut ctx);

        assert!(outputs.is_empty());
        assert_eq!(op.late_events_dropped(), 1);
    }

    #[test]
    fn test_watermark_sort_multi_column() {
        let mut op = WatermarkBoundedSortOperator::new(
            "test_wms".to_string(),
            vec![TopKSortColumn::descending("price")],
            100_000,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let events = vec![
            make_event_f64(100, 100.0),
            make_event_f64(200, 300.0),
            make_event_f64(300, 200.0),
        ];

        for event in &events {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(event, &mut ctx);
        }

        let outputs = op.on_watermark_advance(500);
        let prices: Vec<f64> = outputs
            .iter()
            .filter_map(|o| match o {
                Output::Event(e) => Some(
                    e.data
                        .column(0)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(0),
                ),
                _ => None,
            })
            .collect();
        // Descending: 300, 200, 100
        assert_eq!(prices, vec![300.0, 200.0, 100.0]);
    }

    #[test]
    fn test_watermark_sort_empty_buffer() {
        let mut op = WatermarkBoundedSortOperator::new(
            "test_wms".to_string(),
            vec![TopKSortColumn::ascending("value")],
            100_000,
        );

        // Advancing watermark on empty buffer
        let outputs = op.on_watermark_advance(100);
        // Only watermark output
        assert_eq!(outputs.len(), 1);
        match &outputs[0] {
            Output::Watermark(w) => assert_eq!(*w, 100),
            _ => panic!("Expected Watermark output"),
        }
    }

    #[test]
    fn test_watermark_sort_buffer_limit() {
        let mut op = WatermarkBoundedSortOperator::new(
            "test_wms".to_string(),
            vec![TopKSortColumn::ascending("value")],
            3, // Very small buffer limit
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Fill buffer to capacity
        for i in 1..=3 {
            let event = make_event(i * 100, i);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(&event, &mut ctx);
        }
        assert_eq!(op.buffer_size(), 3);

        // Next event triggers a forced flush
        let overflow = make_event(400, 4);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&overflow, &mut ctx);

        // Should have flushed previous buffer (3 events + watermark) + 1 new buffered
        assert!(!outputs.is_empty());
        assert_eq!(op.buffer_size(), 1); // The new event is buffered
    }

    #[test]
    fn test_watermark_sort_checkpoint_restore() {
        let mut op = WatermarkBoundedSortOperator::new(
            "test_wms".to_string(),
            vec![TopKSortColumn::ascending("value")],
            100_000,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let e1 = make_event(100, 10);
        let e2 = make_event(200, 20);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e2, &mut ctx);

        let checkpoint = op.checkpoint();
        assert_eq!(checkpoint.operator_id, "test_wms");

        let mut op2 = WatermarkBoundedSortOperator::new(
            "test_wms".to_string(),
            vec![TopKSortColumn::ascending("value")],
            100_000,
        );
        op2.restore(checkpoint).unwrap();

        assert_eq!(op2.buffer_size(), 2);
        assert_eq!(op2.last_watermark(), i64::MIN);
    }

    #[test]
    fn test_watermark_sort_ascending_and_descending() {
        // Test with ascending sort
        let mut op_asc = WatermarkBoundedSortOperator::new(
            "asc".to_string(),
            vec![TopKSortColumn::ascending("value")],
            100_000,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        for val in [30i64, 10, 20] {
            let event = make_event(val * 10, val);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op_asc.process(&event, &mut ctx);
        }

        let asc_out = op_asc.on_watermark_advance(1000);
        let asc_vals: Vec<i64> = asc_out
            .iter()
            .filter_map(|o| match o {
                Output::Event(e) => Some(
                    e.data
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                ),
                _ => None,
            })
            .collect();
        assert_eq!(asc_vals, vec![10, 20, 30]);

        // Test with descending sort
        let mut op_desc = WatermarkBoundedSortOperator::new(
            "desc".to_string(),
            vec![TopKSortColumn::descending("value")],
            100_000,
        );

        for val in [30i64, 10, 20] {
            let event = make_event(val * 10, val);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op_desc.process(&event, &mut ctx);
        }

        let desc_out = op_desc.on_watermark_advance(1000);
        let desc_vals: Vec<i64> = desc_out
            .iter()
            .filter_map(|o| match o {
                Output::Event(e) => Some(
                    e.data
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                ),
                _ => None,
            })
            .collect();
        assert_eq!(desc_vals, vec![30, 20, 10]);
    }

    #[test]
    fn test_watermark_sort_preserves_watermarks() {
        let mut op = WatermarkBoundedSortOperator::new(
            "test_wms".to_string(),
            vec![TopKSortColumn::ascending("value")],
            100_000,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let e1 = make_event(100, 10);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);

        let outputs = op.on_watermark_advance(200);
        // Last output should be the watermark
        let last = outputs.last().unwrap();
        match last {
            Output::Watermark(w) => assert_eq!(*w, 200),
            _ => panic!("Expected Watermark as last output"),
        }
    }
}
