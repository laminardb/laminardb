//! # LAG/LEAD Operator
//!
//! Per-row analytic window functions for streaming event processing.
//!
//! ## Streaming Semantics
//!
//! - **LAG(col, offset, default)**: Look back `offset` events in the partition's
//!   history buffer. Emit immediately since history is always available.
//! - **LEAD(col, offset, default)**: Buffer current event and wait for `offset`
//!   future events in the partition. Flush remaining with defaults on watermark.
//!
//! ## Memory Bounds
//!
//! Memory is O(P * max(`lag_offset`, `lead_offset`)) where P = distinct partitions.
//! A `max_partitions` limit prevents unbounded partition growth.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::{
    Array, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema};
use rustc_hash::FxHashMap;

use super::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output, OutputVec, Timer,
};

/// Configuration for a LAG/LEAD operator.
#[derive(Debug, Clone)]
pub struct LagLeadConfig {
    /// Operator identifier for checkpointing.
    pub operator_id: String,
    /// Individual function specifications.
    pub functions: Vec<LagLeadFunctionSpec>,
    /// Partition key columns.
    pub partition_columns: Vec<String>,
    /// Maximum number of partitions (memory safety).
    pub max_partitions: usize,
}

/// Specification for a single LAG or LEAD function.
#[derive(Debug, Clone)]
pub struct LagLeadFunctionSpec {
    /// True for LAG, false for LEAD.
    pub is_lag: bool,
    /// Source column to read values from.
    pub source_column: String,
    /// Offset (number of rows to look back/ahead).
    pub offset: usize,
    /// Default value when no row is available (as f64 for simplicity).
    pub default_value: Option<f64>,
    /// Output column name.
    pub output_column: String,
}

/// Per-partition state for LAG/LEAD processing.
#[derive(Debug, Clone)]
struct PartitionState {
    /// History buffer for LAG lookback (most recent at back).
    lag_history: VecDeque<f64>,
    /// Pending events for LEAD (waiting for future events).
    lead_pending: VecDeque<PendingLeadEvent>,
}

/// A pending event waiting for LEAD resolution.
#[derive(Debug, Clone)]
struct PendingLeadEvent {
    /// Original event.
    event: Event,
    /// Remaining events needed before this can be emitted.
    remaining: usize,
    /// The value from the source column at this event's position.
    value: f64,
}

/// Metrics for LAG/LEAD operator.
#[derive(Debug, Default)]
pub struct LagLeadMetrics {
    /// Total events processed.
    pub events_processed: u64,
    /// LAG lookups performed.
    pub lag_lookups: u64,
    /// LEAD events buffered.
    pub lead_buffered: u64,
    /// LEAD events flushed (resolved or watermark).
    pub lead_flushed: u64,
    /// Active partition count.
    pub partitions_active: u64,
}

/// LAG/LEAD streaming operator.
///
/// Implements per-partition LAG and LEAD analytic functions for streaming
/// event processing with checkpoint/restore support.
pub struct LagLeadOperator {
    /// Operator identifier for checkpointing.
    operator_id: String,
    /// Function specifications.
    functions: Vec<LagLeadFunctionSpec>,
    /// Partition key columns.
    partition_columns: Vec<String>,
    /// Per-partition state.
    partitions: FxHashMap<Vec<u8>, PartitionState>,
    /// Maximum number of partitions.
    max_partitions: usize,
    /// Operator metrics.
    metrics: LagLeadMetrics,
    // ── Pre-allocated scratch buffers (avoid per-event allocations) ──
    /// Reusable partition key buffer.
    key_buf: Vec<u8>,
    /// Cached LEAD function (offset, default) pairs — computed once, reused.
    lead_specs_cache: Vec<(usize, Option<f64>)>,
    /// Cached LEAD default values — computed once, reused.
    lead_defaults_cache: Vec<f64>,
    /// Cached LEAD output column names — computed once, reused.
    lead_columns_cache: Vec<String>,
    /// Cached column indices for partition columns — resolved on first event.
    cached_partition_indices: Vec<Option<usize>>,
    /// Cached column index for the LAG source column — resolved on first event.
    cached_lag_col_index: Option<usize>,
    /// Cached column index for the LEAD source column — resolved on first event.
    cached_lead_col_index: Option<usize>,
}

impl LagLeadOperator {
    /// Creates a new LAG/LEAD operator from configuration.
    #[must_use]
    pub fn new(config: LagLeadConfig) -> Self {
        // Pre-compute LEAD caches once at construction time.
        let lead_specs_cache: Vec<(usize, Option<f64>)> = config
            .functions
            .iter()
            .filter(|f| !f.is_lag)
            .map(|f| (f.offset, f.default_value))
            .collect();
        let lead_defaults_cache: Vec<f64> = config
            .functions
            .iter()
            .filter(|f| !f.is_lag)
            .map(|f| f.default_value.unwrap_or(f64::NAN))
            .collect();
        let lead_columns_cache: Vec<String> = config
            .functions
            .iter()
            .filter(|f| !f.is_lag)
            .map(|f| f.output_column.clone())
            .collect();

        let num_partition_cols = config.partition_columns.len();
        Self {
            operator_id: config.operator_id,
            functions: config.functions,
            partition_columns: config.partition_columns,
            partitions: FxHashMap::default(),
            max_partitions: config.max_partitions,
            metrics: LagLeadMetrics::default(),
            key_buf: Vec::with_capacity(64),
            lead_specs_cache,
            lead_defaults_cache,
            lead_columns_cache,
            cached_partition_indices: vec![None; num_partition_cols],
            cached_lag_col_index: None,
            cached_lead_col_index: None,
        }
    }

    /// Returns the number of active partitions.
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Returns a reference to the metrics.
    #[must_use]
    pub fn metrics(&self) -> &LagLeadMetrics {
        &self.metrics
    }

    /// Extracts the partition key from an event into `self.key_buf`.
    ///
    /// Reuses the internal buffer to avoid per-event allocation.
    fn fill_partition_key(&mut self, event: &Event) {
        self.key_buf.clear();
        let batch = &event.data;

        for (i, col_name) in self.partition_columns.iter().enumerate() {
            let col_idx = if let Some(idx) = self.cached_partition_indices[i] {
                idx
            } else {
                let Ok(idx) = batch.schema().index_of(col_name) else {
                    self.key_buf.push(0x00);
                    continue;
                };
                self.cached_partition_indices[i] = Some(idx);
                idx
            };

            let array = batch.column(col_idx);

            if array.is_null(0) {
                self.key_buf.push(0x00);
                continue;
            }

            self.key_buf.push(0x01); // non-null marker

            match array.data_type() {
                DataType::Int64 => {
                    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    self.key_buf.extend_from_slice(&arr.value(0).to_le_bytes());
                }
                DataType::Utf8 => {
                    let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                    self.key_buf.extend_from_slice(arr.value(0).as_bytes());
                    self.key_buf.push(0x00); // null terminator
                }
                DataType::Float64 => {
                    let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    self.key_buf
                        .extend_from_slice(&arr.value(0).to_bits().to_le_bytes());
                }
                _ => {
                    self.key_buf.push(0x00);
                }
            }
        }
    }

    /// Extracts a f64 value from a column in the event.
    ///
    /// Caches the column index on first call to avoid per-event schema lookups.
    fn extract_column_value(
        event: &Event,
        column: &str,
        cached_index: &mut Option<usize>,
    ) -> f64 {
        let batch = &event.data;
        let col_idx = if let Some(idx) = *cached_index {
            idx
        } else {
            let Ok(idx) = batch.schema().index_of(column) else {
                return f64::NAN;
            };
            *cached_index = Some(idx);
            idx
        };

        let array = batch.column(col_idx);
        if array.is_null(0) {
            return f64::NAN;
        }

        match array.data_type() {
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                arr.value(0)
            }
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                #[allow(clippy::cast_precision_loss)]
                {
                    arr.value(0) as f64
                }
            }
            DataType::Timestamp(_, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                #[allow(clippy::cast_precision_loss)]
                {
                    arr.value(0) as f64
                }
            }
            _ => f64::NAN,
        }
    }

    /// Computes LAG values for a partition.
    fn compute_lag_values(functions: &[LagLeadFunctionSpec], state: &PartitionState) -> Vec<f64> {
        functions
            .iter()
            .filter(|f| f.is_lag)
            .map(|func| {
                let history = &state.lag_history;
                if history.len() >= func.offset {
                    let idx = history.len() - func.offset;
                    history[idx]
                } else {
                    func.default_value.unwrap_or(f64::NAN)
                }
            })
            .collect()
    }

    /// Builds an output event with computed values (static to avoid borrow issues).
    fn build_output(
        functions: &[LagLeadFunctionSpec],
        event: &Event,
        lag_values: &[f64],
        lead_values: &[f64],
    ) -> Event {
        let original_batch = &event.data;
        let num_original = original_batch.num_columns();
        let num_functions = functions.len();
        let mut fields: Vec<Field> = Vec::with_capacity(num_original + num_functions);
        fields.extend(
            original_batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone()),
        );
        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(num_original + num_functions);
        columns.extend((0..num_original).map(|i| original_batch.column(i).clone()));

        let mut lag_idx = 0;
        let mut lead_idx = 0;

        for func in functions {
            let value = if func.is_lag {
                let v = lag_values.get(lag_idx).copied().unwrap_or(f64::NAN);
                lag_idx += 1;
                v
            } else {
                let v = lead_values.get(lead_idx).copied().unwrap_or(f64::NAN);
                lead_idx += 1;
                v
            };

            fields.push(Field::new(&func.output_column, DataType::Float64, true));
            columns.push(Arc::new(Float64Array::from(vec![value])));
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns)
            .unwrap_or_else(|_| RecordBatch::new_empty(Arc::new(Schema::empty())));
        Event::new(event.timestamp, batch)
    }

    /// Processes an event: computes LAG values, buffers LEAD events, resolves
    /// pending LEAD events that now have enough future rows.
    #[allow(clippy::too_many_lines)]
    fn process_event(&mut self, event: &Event) -> OutputVec {
        self.fill_partition_key(event);
        let partition_key = self.key_buf.clone();

        // Check max partitions
        if !self.partitions.contains_key(&partition_key)
            && self.partitions.len() >= self.max_partitions
        {
            return OutputVec::new();
        }

        let has_lag = self.functions.iter().any(|f| f.is_lag);
        let has_lead = !self.lead_specs_cache.is_empty();

        // Pre-compute constants from functions to avoid borrowing self later
        let max_lag_offset = self
            .functions
            .iter()
            .filter(|f| f.is_lag)
            .map(|f| f.offset)
            .max()
            .unwrap_or(1);
        let max_lead_offset = self
            .functions
            .iter()
            .filter(|f| !f.is_lag)
            .map(|f| f.offset)
            .max()
            .unwrap_or(1);
        let lag_source_col = self
            .functions
            .iter()
            .find(|f| f.is_lag)
            .map(|f| f.source_column.clone());
        let lead_source_col = self
            .functions
            .iter()
            .find(|f| !f.is_lag)
            .map(|f| f.source_column.clone());

        // Get or create partition state
        let state = self
            .partitions
            .entry(partition_key)
            .or_insert_with(|| PartitionState {
                lag_history: VecDeque::new(),
                lead_pending: VecDeque::new(),
            });

        let mut outputs = OutputVec::new();

        // Process LAG: look back in history
        let lag_values = if has_lag {
            Self::compute_lag_values(&self.functions, state)
        } else {
            vec![]
        };

        // Update LAG history
        if has_lag {
            if let Some(col) = &lag_source_col {
                let value = Self::extract_column_value(event, col, &mut self.cached_lag_col_index);
                state.lag_history.push_back(value);
                while state.lag_history.len() > max_lag_offset {
                    state.lag_history.pop_front();
                }
            }
        }

        if has_lead {
            // Buffer this event for LEAD resolution
            let value = if let Some(col) = &lead_source_col {
                Self::extract_column_value(event, col, &mut self.cached_lead_col_index)
            } else {
                f64::NAN
            };

            // Decrement remaining on all pending events
            for pending in &mut state.lead_pending {
                pending.remaining = pending.remaining.saturating_sub(1);
            }

            state.lead_pending.push_back(PendingLeadEvent {
                event: event.clone(),
                remaining: max_lead_offset,
                value,
            });
            self.metrics.lead_buffered += 1;

            // Emit resolved LEAD events (remaining == 0)
            // Use cached lead_specs to avoid per-event collection.
            let mut resolved_events = Vec::new();
            while state.lead_pending.front().is_some_and(|p| p.remaining == 0) {
                let resolved = state.lead_pending.pop_front().unwrap();
                let lead_values: Vec<f64> = self
                    .lead_specs_cache
                    .iter()
                    .map(|(offset, default)| {
                        if *offset <= state.lead_pending.len() {
                            state.lead_pending[*offset - 1].value
                        } else {
                            default.unwrap_or(f64::NAN)
                        }
                    })
                    .collect();
                resolved_events.push((resolved, lead_values));
            }

            for (resolved, lead_values) in resolved_events {
                let output =
                    Self::build_output(&self.functions, &resolved.event, &lag_values, &lead_values);
                outputs.push(Output::Event(output));
                self.metrics.lead_flushed += 1;
            }
        } else {
            // No LEAD functions: emit immediately with LAG values
            let output = Self::build_output(&self.functions, event, &lag_values, &[]);
            outputs.push(Output::Event(output));
        }

        self.metrics.events_processed += 1;
        if has_lag {
            self.metrics.lag_lookups += 1;
        }
        self.metrics.partitions_active = self.partitions.len() as u64;

        outputs
    }

    /// Flushes all pending LEAD events with default values.
    /// Called on watermark advance.
    fn flush_pending_leads(&mut self) -> OutputVec {
        let mut outputs = OutputVec::new();

        // Use cached defaults and column names (built once at construction).
        let lead_defaults = &self.lead_defaults_cache;
        let lead_output_columns = &self.lead_columns_cache;

        let mut flushed_count = 0u64;

        for state in self.partitions.values_mut() {
            while let Some(pending) = state.lead_pending.pop_front() {
                let original_batch = &pending.event.data;
                let num_original = original_batch.num_columns();
                let num_lead = lead_output_columns.len();
                let mut fields: Vec<Field> = Vec::with_capacity(num_original + num_lead);
                fields.extend(
                    original_batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.as_ref().clone()),
                );
                let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(num_original + num_lead);
                columns.extend((0..num_original).map(|i| original_batch.column(i).clone()));

                for (col_name, &default) in lead_output_columns.iter().zip(lead_defaults.iter()) {
                    fields.push(Field::new(col_name, DataType::Float64, true));
                    columns.push(Arc::new(Float64Array::from(vec![default])));
                }

                let schema = Arc::new(Schema::new(fields));
                if let Ok(batch) = RecordBatch::try_new(schema, columns) {
                    let output_event = Event::new(pending.event.timestamp, batch);
                    outputs.push(Output::Event(output_event));
                    flushed_count += 1;
                }
            }
        }

        self.metrics.lead_flushed += flushed_count;
        outputs
    }
}

impl Operator for LagLeadOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        self.process_event(event)
    }

    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        // Flush pending LEAD events on watermark/timer
        self.flush_pending_leads()
    }

    fn checkpoint(&self) -> OperatorState {
        let mut data = Vec::new();

        // Write partition count
        let num_partitions = self.partitions.len() as u64;
        data.extend_from_slice(&num_partitions.to_le_bytes());

        // Write each partition
        for (key, state) in &self.partitions {
            // Partition key
            let key_len = key.len() as u64;
            data.extend_from_slice(&key_len.to_le_bytes());
            data.extend_from_slice(key);

            // LAG history
            let history_len = state.lag_history.len() as u64;
            data.extend_from_slice(&history_len.to_le_bytes());
            for &val in &state.lag_history {
                data.extend_from_slice(&val.to_le_bytes());
            }

            // LEAD pending count
            let pending_len = state.lead_pending.len() as u64;
            data.extend_from_slice(&pending_len.to_le_bytes());
            for pending in &state.lead_pending {
                data.extend_from_slice(&pending.event.timestamp.to_le_bytes());
                data.extend_from_slice(&(pending.remaining as u64).to_le_bytes());
                data.extend_from_slice(&pending.value.to_le_bytes());
            }
        }

        OperatorState {
            operator_id: self.operator_id.clone(),
            data,
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn restore(&mut self, state: OperatorState) -> Result<(), OperatorError> {
        if state.data.len() < 8 {
            return Err(OperatorError::SerializationFailed(
                "LagLead checkpoint data too short".to_string(),
            ));
        }

        let mut offset = 0;

        let num_partitions = u64::from_le_bytes(
            state.data[offset..offset + 8]
                .try_into()
                .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
        ) as usize;
        offset += 8;

        self.partitions.clear();

        for _ in 0..num_partitions {
            if offset + 8 > state.data.len() {
                return Err(OperatorError::SerializationFailed(
                    "LagLead checkpoint truncated".to_string(),
                ));
            }

            // Read partition key
            let key_len = u64::from_le_bytes(
                state.data[offset..offset + 8]
                    .try_into()
                    .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
            ) as usize;
            offset += 8;

            let partition_key = state.data[offset..offset + key_len].to_vec();
            offset += key_len;

            // Read LAG history
            let history_len = u64::from_le_bytes(
                state.data[offset..offset + 8]
                    .try_into()
                    .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
            ) as usize;
            offset += 8;

            let mut lag_history = VecDeque::with_capacity(history_len);
            for _ in 0..history_len {
                let val = f64::from_le_bytes(
                    state.data[offset..offset + 8]
                        .try_into()
                        .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
                );
                offset += 8;
                lag_history.push_back(val);
            }

            // Read LEAD pending
            let pending_len = u64::from_le_bytes(
                state.data[offset..offset + 8]
                    .try_into()
                    .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
            ) as usize;
            offset += 8;

            let mut lead_pending = VecDeque::with_capacity(pending_len);
            for _ in 0..pending_len {
                let timestamp = i64::from_le_bytes(
                    state.data[offset..offset + 8]
                        .try_into()
                        .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
                );
                offset += 8;

                let remaining = u64::from_le_bytes(
                    state.data[offset..offset + 8]
                        .try_into()
                        .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
                ) as usize;
                offset += 8;

                let value = f64::from_le_bytes(
                    state.data[offset..offset + 8]
                        .try_into()
                        .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
                );
                offset += 8;

                let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
                lead_pending.push_back(PendingLeadEvent {
                    event: Event::new(timestamp, batch),
                    remaining,
                    value,
                });
            }

            self.partitions.insert(
                partition_key,
                PartitionState {
                    lag_history,
                    lead_pending,
                },
            );
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp)]
mod tests {
    use super::*;
    use crate::operator::TimerKey;
    use crate::state::InMemoryStore;
    use crate::time::{BoundedOutOfOrdernessGenerator, TimerService};

    fn make_trade(timestamp: i64, symbol: &str, price: f64) -> Event {
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

    fn lag_config(offset: usize) -> LagLeadConfig {
        LagLeadConfig {
            operator_id: "test_lag".to_string(),
            functions: vec![LagLeadFunctionSpec {
                is_lag: true,
                source_column: "price".to_string(),
                offset,
                default_value: None,
                output_column: "prev_price".to_string(),
            }],
            partition_columns: vec!["symbol".to_string()],
            max_partitions: 100,
        }
    }

    fn lead_config(offset: usize) -> LagLeadConfig {
        LagLeadConfig {
            operator_id: "test_lead".to_string(),
            functions: vec![LagLeadFunctionSpec {
                is_lag: false,
                source_column: "price".to_string(),
                offset,
                default_value: Some(0.0),
                output_column: "next_price".to_string(),
            }],
            partition_columns: vec!["symbol".to_string()],
            max_partitions: 100,
        }
    }

    #[test]
    fn test_lag_first_event_returns_nan() {
        let mut op = LagLeadOperator::new(lag_config(1));
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let event = make_trade(1, "AAPL", 150.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&event, &mut ctx);

        assert_eq!(outputs.len(), 1);
        if let Output::Event(e) = &outputs[0] {
            let arr = e
                .data
                .column_by_name("prev_price")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert!(arr.value(0).is_nan());
        } else {
            panic!("Expected Event output");
        }
    }

    #[test]
    fn test_lag_second_event_returns_previous() {
        let mut op = LagLeadOperator::new(lag_config(1));
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let e1 = make_trade(1, "AAPL", 150.0);
        let e2 = make_trade(2, "AAPL", 155.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&e2, &mut ctx);

        if let Output::Event(e) = &outputs[0] {
            let arr = e
                .data
                .column_by_name("prev_price")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(arr.value(0), 150.0);
        }
    }

    #[test]
    fn test_lag_with_default() {
        let mut op = LagLeadOperator::new(LagLeadConfig {
            operator_id: "test".to_string(),
            functions: vec![LagLeadFunctionSpec {
                is_lag: true,
                source_column: "price".to_string(),
                offset: 1,
                default_value: Some(-1.0),
                output_column: "prev_price".to_string(),
            }],
            partition_columns: vec!["symbol".to_string()],
            max_partitions: 100,
        });
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let event = make_trade(1, "AAPL", 150.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&event, &mut ctx);

        if let Output::Event(e) = &outputs[0] {
            let arr = e
                .data
                .column_by_name("prev_price")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(arr.value(0), -1.0);
        }
    }

    #[test]
    fn test_lag_offset_2() {
        let mut op = LagLeadOperator::new(lag_config(2));
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let events = [
            make_trade(1, "AAPL", 100.0),
            make_trade(2, "AAPL", 110.0),
            make_trade(3, "AAPL", 120.0),
        ];

        for e in &events[..2] {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(e, &mut ctx);
        }

        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&events[2], &mut ctx);

        if let Output::Event(e) = &outputs[0] {
            let arr = e
                .data
                .column_by_name("prev_price")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(arr.value(0), 100.0); // 2 positions back
        }
    }

    #[test]
    fn test_lag_separate_partitions() {
        let mut op = LagLeadOperator::new(lag_config(1));
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // AAPL events
        let a1 = make_trade(1, "AAPL", 150.0);
        let a2 = make_trade(3, "AAPL", 155.0);
        // GOOG events
        let g1 = make_trade(2, "GOOG", 2800.0);

        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&a1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&g1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&a2, &mut ctx);

        // AAPL should see previous AAPL price (150.0), not GOOG
        if let Output::Event(e) = &outputs[0] {
            let arr = e
                .data
                .column_by_name("prev_price")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(arr.value(0), 150.0);
        }
        assert_eq!(op.partition_count(), 2);
    }

    #[test]
    fn test_lead_buffers_events() {
        let mut op = LagLeadOperator::new(lead_config(1));
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let e1 = make_trade(1, "AAPL", 150.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&e1, &mut ctx);

        // First event should be buffered (no future event yet)
        assert!(outputs.is_empty());
    }

    #[test]
    fn test_lead_resolves_on_next_event() {
        let mut op = LagLeadOperator::new(lead_config(1));
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let e1 = make_trade(1, "AAPL", 150.0);
        let e2 = make_trade(2, "AAPL", 155.0);

        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&e2, &mut ctx);

        // First event should now be emitted with LEAD = 155.0
        assert_eq!(outputs.len(), 1);
        if let Output::Event(e) = &outputs[0] {
            let arr = e
                .data
                .column_by_name("next_price")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(arr.value(0), 155.0);
        }
    }

    #[test]
    fn test_lead_flush_on_watermark() {
        let mut op = LagLeadOperator::new(lead_config(1));
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let e1 = make_trade(1, "AAPL", 150.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);

        // Flush on timer/watermark
        let timer = Timer {
            key: TimerKey::default(),
            timestamp: 100,
        };
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.on_timer(timer, &mut ctx);

        // Should emit with default value (0.0)
        assert_eq!(outputs.len(), 1);
        if let Output::Event(e) = &outputs[0] {
            let arr = e
                .data
                .column_by_name("next_price")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(arr.value(0), 0.0);
        }
    }

    #[test]
    fn test_max_partitions() {
        let mut op = LagLeadOperator::new(LagLeadConfig {
            operator_id: "test".to_string(),
            functions: vec![LagLeadFunctionSpec {
                is_lag: true,
                source_column: "price".to_string(),
                offset: 1,
                default_value: None,
                output_column: "prev_price".to_string(),
            }],
            partition_columns: vec!["symbol".to_string()],
            max_partitions: 2,
        });
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let e1 = make_trade(1, "AAPL", 150.0);
        let e2 = make_trade(2, "GOOG", 2800.0);
        let e3 = make_trade(3, "MSFT", 300.0);

        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e2, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&e3, &mut ctx);

        assert!(outputs.is_empty()); // MSFT rejected
        assert_eq!(op.partition_count(), 2);
    }

    #[test]
    fn test_checkpoint_restore() {
        let mut op = LagLeadOperator::new(lag_config(1));
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let events = vec![
            make_trade(1, "AAPL", 100.0),
            make_trade(2, "AAPL", 110.0),
            make_trade(3, "GOOG", 2800.0),
        ];
        for e in &events {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(e, &mut ctx);
        }

        let checkpoint = op.checkpoint();
        assert_eq!(checkpoint.operator_id, "test_lag");

        let mut op2 = LagLeadOperator::new(lag_config(1));
        op2.restore(checkpoint).unwrap();
        assert_eq!(op2.partition_count(), 2);
    }

    #[test]
    fn test_metrics() {
        let mut op = LagLeadOperator::new(lag_config(1));
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let e1 = make_trade(1, "AAPL", 150.0);
        let e2 = make_trade(2, "AAPL", 155.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e2, &mut ctx);

        assert_eq!(op.metrics().events_processed, 2);
        assert_eq!(op.metrics().lag_lookups, 2);
    }

    #[test]
    fn test_lead_separate_partitions() {
        let mut op = LagLeadOperator::new(lead_config(1));
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let a1 = make_trade(1, "AAPL", 150.0);
        let g1 = make_trade(2, "GOOG", 2800.0);
        let a2 = make_trade(3, "AAPL", 155.0);

        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&a1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&g1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&a2, &mut ctx);

        // AAPL's first event should resolve with next AAPL value
        assert_eq!(outputs.len(), 1);
        if let Output::Event(e) = &outputs[0] {
            let arr = e
                .data
                .column_by_name("next_price")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(arr.value(0), 155.0);
        }
    }

    #[test]
    fn test_empty_operator() {
        let op = LagLeadOperator::new(lag_config(1));
        assert_eq!(op.partition_count(), 0);
        assert_eq!(op.metrics().events_processed, 0);
    }
}
