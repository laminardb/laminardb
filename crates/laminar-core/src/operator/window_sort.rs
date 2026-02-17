//! # Window-Local Sort Operator
//!
//! ORDER BY within windowed aggregations. Bounded by window size.
//!
//! Sits downstream of a window operator. Buffers output events per window.
//! On watermark advance past window end, sorts the buffer and emits.
//! Optionally applies a LIMIT for top-N within each window.
//!
//! ## Memory Bounds
//!
//! Memory is bounded by the maximum number of events per window.
//! Each window buffer is released immediately after sorting and emission.

use super::topk::{
    encode_f64, encode_i64, encode_not_null, encode_null, encode_utf8, TopKSortColumn,
};
use super::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output, OutputVec, Timer,
};
use arrow_array::{Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow_schema::DataType;
use rustc_hash::FxHashMap;

/// Window-local sort operator.
///
/// Buffers events per window, sorts on window close (watermark advance),
/// and emits sorted events. Optionally applies a LIMIT.
pub struct WindowLocalSortOperator {
    /// Operator identifier for checkpointing.
    operator_id: String,
    /// Sort column specifications.
    sort_columns: Vec<TopKSortColumn>,
    /// Per-window event buffers, keyed by window start timestamp.
    window_buffers: FxHashMap<i64, Vec<BufferedEvent>>,
    /// Current watermark value.
    current_watermark: i64,
    /// Optional LIMIT for top-N within each window.
    limit: Option<usize>,
}

/// A buffered event with its pre-computed sort key.
struct BufferedEvent {
    /// Memcomparable sort key.
    sort_key: Vec<u8>,
    /// The original event.
    event: Event,
}

impl WindowLocalSortOperator {
    /// Creates a new window-local sort operator.
    #[must_use]
    pub fn new(
        operator_id: String,
        sort_columns: Vec<TopKSortColumn>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            operator_id,
            sort_columns,
            window_buffers: FxHashMap::default(),
            current_watermark: i64::MIN,
            limit,
        }
    }

    /// Returns the number of active window buffers.
    #[must_use]
    pub fn active_window_count(&self) -> usize {
        self.window_buffers.len()
    }

    /// Returns the total number of buffered events across all windows.
    #[must_use]
    pub fn total_buffered_events(&self) -> usize {
        self.window_buffers.values().map(Vec::len).sum()
    }

    /// Returns the current watermark value.
    #[must_use]
    pub fn current_watermark(&self) -> i64 {
        self.current_watermark
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

    /// Adds an event to the buffer for the given window.
    fn buffer_event(&mut self, window_start: i64, event: &Event) {
        let sort_key = self.extract_sort_key(event);
        let buffered = BufferedEvent {
            sort_key,
            event: event.clone(),
        };
        self.window_buffers
            .entry(window_start)
            .or_default()
            .push(buffered);
    }

    /// Sorts and emits a window buffer, applying optional LIMIT.
    fn sort_and_emit_window(&mut self, window_start: i64) -> OutputVec {
        let mut outputs = OutputVec::new();

        if let Some(mut buffer) = self.window_buffers.remove(&window_start) {
            // Sort by memcomparable key
            buffer.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));

            // Apply LIMIT if configured
            let limit = self.limit.unwrap_or(buffer.len());
            for buffered in buffer.into_iter().take(limit) {
                outputs.push(Output::Event(buffered.event));
            }
        }

        outputs
    }

    /// Checks all windows and emits any that are closed by the current watermark.
    fn check_and_emit_closed_windows(&mut self) -> OutputVec {
        let mut outputs = OutputVec::new();

        // Collect window starts that are closed by the watermark
        let closed_windows: Vec<i64> = self
            .window_buffers
            .keys()
            .filter(|&&ws| ws < self.current_watermark)
            .copied()
            .collect();

        // Sort for deterministic output
        let mut closed_windows = closed_windows;
        closed_windows.sort_unstable();

        for window_start in closed_windows {
            let window_outputs = self.sort_and_emit_window(window_start);
            outputs.extend(window_outputs);
        }

        outputs
    }
}

impl Operator for WindowLocalSortOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        // Use the event timestamp as the window start key.
        // In practice, the upstream window operator tags events with window_start.
        // Here we use the event timestamp as a simple window key.
        let window_start = event.timestamp;
        self.buffer_event(window_start, event);
        OutputVec::new()
    }

    fn on_timer(&mut self, timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        // Timer carries window_start as the timestamp.
        // Update watermark and emit closed windows.
        self.current_watermark = timer.timestamp;
        self.check_and_emit_closed_windows()
    }

    fn checkpoint(&self) -> OperatorState {
        let mut data = Vec::new();

        // Write watermark
        data.extend_from_slice(&self.current_watermark.to_le_bytes());

        // Write number of windows
        let num_windows = self.window_buffers.len() as u64;
        data.extend_from_slice(&num_windows.to_le_bytes());

        // Write each window: start + event count
        for (&window_start, buffer) in &self.window_buffers {
            data.extend_from_slice(&window_start.to_le_bytes());
            let count = buffer.len() as u64;
            data.extend_from_slice(&count.to_le_bytes());
            // Write event timestamps
            for be in buffer {
                data.extend_from_slice(&be.event.timestamp.to_le_bytes());
            }
        }

        OperatorState {
            operator_id: self.operator_id.clone(),
            data,
        }
    }

    #[allow(clippy::cast_possible_truncation)] // Checkpoint wire format uses u64 for counts
    fn restore(&mut self, state: OperatorState) -> Result<(), OperatorError> {
        if state.data.len() < 16 {
            return Err(OperatorError::SerializationFailed(
                "WindowLocalSort checkpoint data too short".to_string(),
            ));
        }

        let mut offset = 0;
        self.current_watermark = i64::from_le_bytes(
            state.data[offset..offset + 8]
                .try_into()
                .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
        );
        offset += 8;

        let num_windows = u64::from_le_bytes(
            state.data[offset..offset + 8]
                .try_into()
                .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
        ) as usize;
        offset += 8;

        self.window_buffers.clear();
        for _ in 0..num_windows {
            if offset + 16 > state.data.len() {
                return Err(OperatorError::SerializationFailed(
                    "WindowLocalSort checkpoint truncated".to_string(),
                ));
            }
            let window_start = i64::from_le_bytes(
                state.data[offset..offset + 8]
                    .try_into()
                    .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
            );
            offset += 8;

            let count = u64::from_le_bytes(
                state.data[offset..offset + 8]
                    .try_into()
                    .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
            ) as usize;
            offset += 8;

            let mut buffer = Vec::with_capacity(count);
            for _ in 0..count {
                if offset + 8 > state.data.len() {
                    return Err(OperatorError::SerializationFailed(
                        "WindowLocalSort checkpoint truncated at events".to_string(),
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
                buffer.push(BufferedEvent {
                    sort_key: Vec::new(),
                    event: Event::new(ts, batch),
                });
            }
            self.window_buffers.insert(window_start, buffer);
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

    fn make_event(timestamp: i64, price: f64) -> Event {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Float64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![price]))]).unwrap();
        Event::new(timestamp, batch)
    }

    fn make_event_i64(timestamp: i64, value: i64) -> Event {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap();
        Event::new(timestamp, batch)
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
    fn test_window_sort_single_window() {
        let mut op = WindowLocalSortOperator::new(
            "test_ws".to_string(),
            vec![TopKSortColumn::ascending("price")],
            None,
        );

        // Buffer events in window_start=1000
        op.buffer_event(1000, &make_event(1000, 300.0));
        op.buffer_event(1000, &make_event(1001, 100.0));
        op.buffer_event(1000, &make_event(1002, 200.0));

        // Sort and emit
        let outputs = op.sort_and_emit_window(1000);
        assert_eq!(outputs.len(), 3);

        // Should be sorted ascending by price: 100, 200, 300
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
        assert_eq!(prices, vec![100.0, 200.0, 300.0]);
    }

    #[test]
    fn test_window_sort_multiple_windows() {
        let mut op = WindowLocalSortOperator::new(
            "test_ws".to_string(),
            vec![TopKSortColumn::ascending("price")],
            None,
        );

        // Buffer events in two windows
        op.buffer_event(1000, &make_event(1000, 300.0));
        op.buffer_event(1000, &make_event(1001, 100.0));
        op.buffer_event(2000, &make_event(2000, 250.0));
        op.buffer_event(2000, &make_event(2001, 50.0));

        assert_eq!(op.active_window_count(), 2);
        assert_eq!(op.total_buffered_events(), 4);

        // Emit first window
        let out1 = op.sort_and_emit_window(1000);
        assert_eq!(out1.len(), 2);
        assert_eq!(op.active_window_count(), 1);

        // Emit second window
        let out2 = op.sort_and_emit_window(2000);
        assert_eq!(out2.len(), 2);
        assert_eq!(op.active_window_count(), 0);
    }

    #[test]
    fn test_window_sort_multi_column() {
        let mut op = WindowLocalSortOperator::new(
            "test_ws".to_string(),
            vec![TopKSortColumn::ascending("value")],
            None,
        );

        op.buffer_event(1000, &make_event_i64(1000, 30));
        op.buffer_event(1000, &make_event_i64(1001, 10));
        op.buffer_event(1000, &make_event_i64(1002, 20));

        let outputs = op.sort_and_emit_window(1000);
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
    fn test_window_sort_with_limit() {
        let mut op = WindowLocalSortOperator::new(
            "test_ws".to_string(),
            vec![TopKSortColumn::ascending("price")],
            Some(2),
        );

        op.buffer_event(1000, &make_event(1000, 300.0));
        op.buffer_event(1000, &make_event(1001, 100.0));
        op.buffer_event(1000, &make_event(1002, 200.0));

        let outputs = op.sort_and_emit_window(1000);
        // LIMIT 2: only first 2 sorted events
        assert_eq!(outputs.len(), 2);

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
        assert_eq!(prices, vec![100.0, 200.0]);
    }

    #[test]
    fn test_window_sort_empty_window() {
        let mut op = WindowLocalSortOperator::new(
            "test_ws".to_string(),
            vec![TopKSortColumn::ascending("price")],
            None,
        );

        // No events buffered for this window
        let outputs = op.sort_and_emit_window(1000);
        assert!(outputs.is_empty());
    }

    #[test]
    fn test_window_sort_watermark_triggers_emit() {
        let mut op = WindowLocalSortOperator::new(
            "test_ws".to_string(),
            vec![TopKSortColumn::ascending("price")],
            None,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Buffer events in window_start=1000
        op.buffer_event(1000, &make_event(1000, 300.0));
        op.buffer_event(1000, &make_event(1001, 100.0));

        // Timer with watermark past window end triggers emission
        let timer = Timer {
            key: smallvec![],
            timestamp: 2000, // watermark advances past 1000
        };
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.on_timer(timer, &mut ctx);

        assert_eq!(outputs.len(), 2);
        assert_eq!(op.active_window_count(), 0);
    }

    #[test]
    fn test_window_sort_preserves_other_outputs() {
        // Ensure window buffers for different windows are independent
        let mut op = WindowLocalSortOperator::new(
            "test_ws".to_string(),
            vec![TopKSortColumn::ascending("price")],
            None,
        );

        op.buffer_event(1000, &make_event(1000, 100.0));
        op.buffer_event(2000, &make_event(2000, 200.0));

        // Only emit window 1000
        let outputs = op.sort_and_emit_window(1000);
        assert_eq!(outputs.len(), 1);
        // Window 2000 still active
        assert_eq!(op.active_window_count(), 1);
        assert_eq!(op.total_buffered_events(), 1);
    }

    #[test]
    fn test_window_sort_checkpoint_restore() {
        let mut op = WindowLocalSortOperator::new(
            "test_ws".to_string(),
            vec![TopKSortColumn::ascending("price")],
            Some(5),
        );

        op.buffer_event(1000, &make_event(1000, 100.0));
        op.buffer_event(2000, &make_event(2000, 200.0));

        let checkpoint = op.checkpoint();
        assert_eq!(checkpoint.operator_id, "test_ws");

        let mut op2 = WindowLocalSortOperator::new(
            "test_ws".to_string(),
            vec![TopKSortColumn::ascending("price")],
            Some(5),
        );
        op2.restore(checkpoint).unwrap();

        assert_eq!(op2.active_window_count(), 2);
    }

    #[test]
    fn test_window_sort_descending() {
        let mut op = WindowLocalSortOperator::new(
            "test_ws".to_string(),
            vec![TopKSortColumn::descending("price")],
            None,
        );

        op.buffer_event(1000, &make_event(1000, 100.0));
        op.buffer_event(1000, &make_event(1001, 300.0));
        op.buffer_event(1000, &make_event(1002, 200.0));

        let outputs = op.sort_and_emit_window(1000);
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
    fn test_window_sort_large_window() {
        let mut op = WindowLocalSortOperator::new(
            "test_ws".to_string(),
            vec![TopKSortColumn::ascending("value")],
            None,
        );

        // Buffer 100 events in reverse order
        for i in (0..100).rev() {
            op.buffer_event(1000, &make_event_i64(1000 + i, i));
        }

        let outputs = op.sort_and_emit_window(1000);
        assert_eq!(outputs.len(), 100);

        // Verify sorted ascending
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
        let expected: Vec<i64> = (0..100).collect();
        assert_eq!(vals, expected);
    }
}
