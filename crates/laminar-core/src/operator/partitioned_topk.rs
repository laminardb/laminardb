//! # Partitioned Top-K Operator
//!
//! Per-group top-K supporting the `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) WHERE rn <= N`
//! pattern. Each partition key gets an independent top-K heap.
//!
//! ## Memory Bounds
//!
//! Memory is O(P * K) where P = distinct partitions, K = per-partition limit.
//! A `max_partitions` safety limit prevents unbounded partition growth.
//!
//! ## Emit Strategies
//!
//! Same as the global top-K: `OnUpdate`, `OnWatermark`, or `Periodic`.
//! Changelog records are emitted per-partition.

use super::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output, OutputVec, Timer,
};
use super::topk::{TopKEmitStrategy, TopKSortColumn, encode_i64, encode_f64, encode_utf8, encode_null, encode_not_null};
use super::window::ChangelogRecord;
use arrow_array::{Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow_schema::DataType;
use fxhash::FxHashMap;

/// Configuration for a partition key column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionColumn {
    /// Column name in the event schema.
    pub column_name: String,
}

impl PartitionColumn {
    /// Creates a new partition column.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            column_name: name.into(),
        }
    }
}

/// An entry in a per-partition top-K heap.
#[derive(Debug, Clone)]
struct PartitionEntry {
    /// Memcomparable sort key.
    sort_key: Vec<u8>,
    /// The original event.
    event: Event,
}

/// Partitioned top-K operator.
///
/// Maintains independent top-K heaps per partition key.
/// Supports the `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) WHERE rn <= N` pattern.
pub struct PartitionedTopKOperator {
    /// Operator identifier for checkpointing.
    operator_id: String,
    /// Number of top entries per partition.
    k: usize,
    /// Partition key columns.
    partition_columns: Vec<PartitionColumn>,
    /// Sort column specifications.
    sort_columns: Vec<TopKSortColumn>,
    /// Per-partition top-K heaps, keyed by partition key bytes.
    partitions: FxHashMap<Vec<u8>, Vec<PartitionEntry>>,
    /// Emission strategy.
    emit_strategy: TopKEmitStrategy,
    /// Pending changelog records (for OnWatermark/Periodic strategies).
    pending_changes: Vec<ChangelogRecord>,
    /// Monotonic sequence counter.
    sequence_counter: u64,
    /// Maximum number of partitions (memory safety).
    max_partitions: usize,
}

impl PartitionedTopKOperator {
    /// Creates a new partitioned top-K operator.
    #[must_use]
    pub fn new(
        operator_id: String,
        k: usize,
        partition_columns: Vec<PartitionColumn>,
        sort_columns: Vec<TopKSortColumn>,
        emit_strategy: TopKEmitStrategy,
        max_partitions: usize,
    ) -> Self {
        Self {
            operator_id,
            k,
            partition_columns,
            sort_columns,
            partitions: FxHashMap::default(),
            emit_strategy,
            pending_changes: Vec::new(),
            sequence_counter: 0,
            max_partitions,
        }
    }

    /// Returns the number of active partitions.
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Returns the total number of entries across all partitions.
    #[must_use]
    pub fn total_entries(&self) -> usize {
        self.partitions.values().map(Vec::len).sum()
    }

    /// Returns the number of entries in a specific partition.
    #[must_use]
    pub fn partition_size(&self, partition_key: &[u8]) -> usize {
        self.partitions
            .get(partition_key)
            .map_or(0, Vec::len)
    }

    /// Returns the number of pending changelog records.
    #[must_use]
    pub fn pending_changes_count(&self) -> usize {
        self.pending_changes.len()
    }

    /// Extracts the partition key from an event.
    fn extract_partition_key(&self, event: &Event) -> Vec<u8> {
        let batch = &event.data;
        let schema = batch.schema();
        let mut key = Vec::new();

        for col in &self.partition_columns {
            let Ok(col_idx) = schema.index_of(&col.column_name) else {
                // Missing column: use null marker
                key.push(0x00);
                continue;
            };

            let array = batch.column(col_idx);

            if array.is_null(0) {
                key.push(0x00); // null marker
                continue;
            }

            key.push(0x01); // non-null marker

            match array.data_type() {
                DataType::Int64 => {
                    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    key.extend_from_slice(&arr.value(0).to_le_bytes());
                }
                DataType::Utf8 => {
                    let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                    let val = arr.value(0);
                    key.extend_from_slice(val.as_bytes());
                    key.push(0x00); // null terminator
                }
                DataType::Float64 => {
                    let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    key.extend_from_slice(&arr.value(0).to_bits().to_le_bytes());
                }
                _ => {
                    key.push(0x00); // unsupported type marker
                }
            }
        }

        key
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

    /// Processes an event for a specific partition, returning changelog records.
    fn process_partition(
        &mut self,
        partition_key: Vec<u8>,
        event: &Event,
        emit_timestamp: i64,
    ) -> Vec<ChangelogRecord> {
        let sort_key = self.extract_sort_key(event);

        let entries = self.partitions.entry(partition_key).or_default();

        // Check if event enters this partition's top-K
        if entries.len() >= self.k {
            if let Some(worst) = entries.last() {
                if sort_key >= worst.sort_key {
                    return Vec::new(); // Doesn't enter top-K
                }
            }
        }

        // Find insertion position (binary search)
        let insert_pos = entries
            .binary_search_by(|entry| entry.sort_key.as_slice().cmp(&sort_key))
            .unwrap_or_else(|pos| pos);

        let new_entry = PartitionEntry {
            sort_key,
            event: event.clone(),
        };
        entries.insert(insert_pos, new_entry);

        let mut changes = Vec::new();

        // Generate insert changelog
        changes.push(ChangelogRecord::insert(event.clone(), emit_timestamp));

        // Generate rank change retractions for shifted entries
        for entry in entries.iter().take(entries.len().min(self.k)).skip(insert_pos + 1) {
            let shifted_event = &entry.event;
            let (before, after) = ChangelogRecord::update(
                shifted_event.clone(),
                shifted_event.clone(),
                emit_timestamp,
            );
            changes.push(before);
            changes.push(after);
        }

        // Evict worst entry if over capacity
        if entries.len() > self.k {
            let evicted = entries.pop().unwrap();
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

impl Operator for PartitionedTopKOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        let partition_key = self.extract_partition_key(event);

        // Check max partitions limit
        if !self.partitions.contains_key(&partition_key)
            && self.partitions.len() >= self.max_partitions
        {
            // Reject: too many partitions
            return OutputVec::new();
        }

        let emit_timestamp = event.timestamp;
        let changes = self.process_partition(partition_key, event, emit_timestamp);

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
        match &self.emit_strategy {
            TopKEmitStrategy::Periodic(_) => self.flush_pending(),
            _ => OutputVec::new(),
        }
    }

    fn checkpoint(&self) -> OperatorState {
        let mut data = Vec::new();

        // Write partition count
        let num_partitions = self.partitions.len() as u64;
        data.extend_from_slice(&num_partitions.to_le_bytes());

        // Write sequence counter
        data.extend_from_slice(&self.sequence_counter.to_le_bytes());

        // Write each partition
        for (key, entries) in &self.partitions {
            // Partition key length + bytes
            let key_len = key.len() as u64;
            data.extend_from_slice(&key_len.to_le_bytes());
            data.extend_from_slice(key);

            // Entry count
            let entry_count = entries.len() as u64;
            data.extend_from_slice(&entry_count.to_le_bytes());

            // Each entry: sort_key_len + sort_key + timestamp
            for entry in entries {
                let sk_len = entry.sort_key.len() as u64;
                data.extend_from_slice(&sk_len.to_le_bytes());
                data.extend_from_slice(&entry.sort_key);
                data.extend_from_slice(&entry.event.timestamp.to_le_bytes());
            }
        }

        OperatorState {
            operator_id: self.operator_id.clone(),
            data,
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn restore(&mut self, state: OperatorState) -> Result<(), OperatorError> {
        if state.data.len() < 16 {
            return Err(OperatorError::SerializationFailed(
                "PartitionedTopK checkpoint data too short".to_string(),
            ));
        }

        let mut offset = 0;

        let num_partitions = u64::from_le_bytes(
            state.data[offset..offset + 8]
                .try_into()
                .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
        ) as usize;
        offset += 8;

        self.sequence_counter = u64::from_le_bytes(
            state.data[offset..offset + 8]
                .try_into()
                .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
        );
        offset += 8;

        self.partitions.clear();

        for _ in 0..num_partitions {
            if offset + 8 > state.data.len() {
                return Err(OperatorError::SerializationFailed(
                    "PartitionedTopK checkpoint truncated".to_string(),
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
                    "PartitionedTopK checkpoint truncated at key".to_string(),
                ));
            }
            let partition_key = state.data[offset..offset + key_len].to_vec();
            offset += key_len;

            let entry_count = u64::from_le_bytes(
                state.data[offset..offset + 8]
                    .try_into()
                    .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
            ) as usize;
            offset += 8;

            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                if offset + 8 > state.data.len() {
                    return Err(OperatorError::SerializationFailed(
                        "PartitionedTopK checkpoint truncated at entry".to_string(),
                    ));
                }
                let sk_len = u64::from_le_bytes(
                    state.data[offset..offset + 8]
                        .try_into()
                        .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
                ) as usize;
                offset += 8;

                if offset + sk_len + 8 > state.data.len() {
                    return Err(OperatorError::SerializationFailed(
                        "PartitionedTopK checkpoint truncated at sort key".to_string(),
                    ));
                }
                let sort_key = state.data[offset..offset + sk_len].to_vec();
                offset += sk_len;

                let timestamp = i64::from_le_bytes(
                    state.data[offset..offset + 8]
                        .try_into()
                        .map_err(|e| OperatorError::SerializationFailed(format!("{e}")))?,
                );
                offset += 8;

                let batch = arrow_array::RecordBatch::new_empty(std::sync::Arc::new(
                    arrow_schema::Schema::empty(),
                ));
                entries.push(PartitionEntry {
                    sort_key,
                    event: Event::new(timestamp, batch),
                });
            }

            self.partitions.insert(partition_key, entries);
        }

        Ok(())
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

    fn make_trade(timestamp: i64, category: &str, price: f64) -> Event {
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

    fn make_trade_int(timestamp: i64, category: &str, value: i64) -> Event {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![category])),
                Arc::new(Int64Array::from(vec![value])),
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

    fn create_partitioned_topk(
        k: usize,
        max_partitions: usize,
    ) -> PartitionedTopKOperator {
        PartitionedTopKOperator::new(
            "test_ptopk".to_string(),
            k,
            vec![PartitionColumn::new("category")],
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnUpdate,
            max_partitions,
        )
    }

    #[test]
    fn test_partitioned_topk_single_partition() {
        let mut op = create_partitioned_topk(3, 100);
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let trades = vec![
            make_trade(1, "A", 100.0),
            make_trade(2, "A", 200.0),
            make_trade(3, "A", 150.0),
        ];

        for trade in &trades {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(trade, &mut ctx);
        }

        assert_eq!(op.partition_count(), 1);
        assert_eq!(op.total_entries(), 3);
    }

    #[test]
    fn test_partitioned_topk_multiple_partitions() {
        let mut op = create_partitioned_topk(2, 100);
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let trades = vec![
            make_trade(1, "A", 100.0),
            make_trade(2, "B", 200.0),
            make_trade(3, "A", 150.0),
            make_trade(4, "B", 250.0),
        ];

        for trade in &trades {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(trade, &mut ctx);
        }

        assert_eq!(op.partition_count(), 2);
        assert_eq!(op.total_entries(), 4);
    }

    #[test]
    fn test_partitioned_topk_eviction_in_partition() {
        let mut op = create_partitioned_topk(2, 100);
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Fill partition "A" to capacity
        let e1 = make_trade(1, "A", 200.0);
        let e2 = make_trade(2, "A", 150.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e2, &mut ctx);

        // Better entry evicts worst in partition
        let e3 = make_trade(3, "A", 300.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&e3, &mut ctx);

        // Should evict price=150 and keep 300, 200
        assert_eq!(op.total_entries(), 2);
        assert!(!outputs.is_empty());

        // Verify we have Insert + Delete among outputs
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
    fn test_partitioned_topk_no_cross_partition_eviction() {
        let mut op = create_partitioned_topk(2, 100);
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Fill partition "A" to capacity
        let e1 = make_trade(1, "A", 200.0);
        let e2 = make_trade(2, "A", 150.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e2, &mut ctx);

        // New entry in partition "B" does NOT evict from "A"
        let e3 = make_trade(3, "B", 50.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e3, &mut ctx);

        assert_eq!(op.partition_count(), 2);
        assert_eq!(op.total_entries(), 3); // 2 in A + 1 in B
    }

    #[test]
    fn test_partitioned_topk_emit_on_update() {
        let mut op = create_partitioned_topk(3, 100);
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let trade = make_trade(1, "A", 100.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&trade, &mut ctx);

        // OnUpdate: should emit immediately
        assert!(!outputs.is_empty());
        match &outputs[0] {
            Output::Changelog(rec) => {
                assert_eq!(rec.operation, CdcOperation::Insert);
            }
            _ => panic!("Expected Changelog output"),
        }
    }

    #[test]
    fn test_partitioned_topk_emit_on_watermark() {
        let mut op = PartitionedTopKOperator::new(
            "test_ptopk".to_string(),
            2,
            vec![PartitionColumn::new("category")],
            vec![TopKSortColumn::descending("price")],
            TopKEmitStrategy::OnWatermark,
            100,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let trade = make_trade(1, "A", 100.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&trade, &mut ctx);

        // OnWatermark: should buffer, not emit
        assert!(outputs.is_empty());
        assert!(op.pending_changes_count() > 0);
    }

    #[test]
    fn test_partitioned_topk_empty_partition() {
        let op = create_partitioned_topk(3, 100);
        assert_eq!(op.partition_count(), 0);
        assert_eq!(op.total_entries(), 0);
    }

    #[test]
    fn test_partitioned_topk_max_partitions() {
        let mut op = create_partitioned_topk(2, 2); // max 2 partitions
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Create 2 partitions
        let e1 = make_trade(1, "A", 100.0);
        let e2 = make_trade(2, "B", 200.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e2, &mut ctx);

        // Third partition rejected
        let e3 = make_trade(3, "C", 300.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&e3, &mut ctx);

        assert!(outputs.is_empty());
        assert_eq!(op.partition_count(), 2);
    }

    #[test]
    fn test_partitioned_topk_k_equals_one() {
        let mut op = create_partitioned_topk(1, 100);
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let e1 = make_trade(1, "A", 100.0);
        let e2 = make_trade(2, "A", 200.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e2, &mut ctx);

        // Only best entry kept per partition
        assert_eq!(op.total_entries(), 1);
    }

    #[test]
    fn test_partitioned_topk_multi_column_partition_key() {
        let mut op = PartitionedTopKOperator::new(
            "test_ptopk".to_string(),
            3,
            vec![
                PartitionColumn::new("category"),
                PartitionColumn::new("value"),
            ],
            vec![TopKSortColumn::descending("value")],
            TopKEmitStrategy::OnUpdate,
            100,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let e1 = make_trade_int(1, "A", 100);
        let e2 = make_trade_int(2, "A", 200);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e2, &mut ctx);

        // Each (category, value) combo is a unique partition
        assert_eq!(op.partition_count(), 2);
    }

    #[test]
    fn test_partitioned_topk_multi_column_sort() {
        let mut op = PartitionedTopKOperator::new(
            "test_ptopk".to_string(),
            3,
            vec![PartitionColumn::new("category")],
            vec![
                TopKSortColumn::descending("price"),
            ],
            TopKEmitStrategy::OnUpdate,
            100,
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let trades = vec![
            make_trade(1, "A", 100.0),
            make_trade(2, "A", 300.0),
            make_trade(3, "A", 200.0),
        ];

        for trade in &trades {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(trade, &mut ctx);
        }

        assert_eq!(op.total_entries(), 3);
    }

    #[test]
    fn test_partitioned_topk_checkpoint_restore() {
        let mut op = create_partitioned_topk(3, 100);
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let trades = vec![
            make_trade(1, "A", 100.0),
            make_trade(2, "B", 200.0),
            make_trade(3, "A", 150.0),
        ];

        for trade in &trades {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(trade, &mut ctx);
        }

        let checkpoint = op.checkpoint();
        assert_eq!(checkpoint.operator_id, "test_ptopk");

        let mut op2 = create_partitioned_topk(3, 100);
        op2.restore(checkpoint).unwrap();

        assert_eq!(op2.partition_count(), 2);
        assert_eq!(op2.total_entries(), 3);
    }

    #[test]
    fn test_partitioned_topk_rank_changes() {
        let mut op = create_partitioned_topk(3, 100);
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Insert two entries
        let e1 = make_trade(1, "A", 100.0);
        let e2 = make_trade(2, "A", 200.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e1, &mut ctx);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&e2, &mut ctx);

        // Insert between them causes rank change for price=100
        let e3 = make_trade(3, "A", 150.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let outputs = op.process(&e3, &mut ctx);

        // Should have Insert + UpdateBefore + UpdateAfter
        let mut has_update_before = false;
        let mut has_update_after = false;
        for output in &outputs {
            if let Output::Changelog(rec) = output {
                match rec.operation {
                    CdcOperation::UpdateBefore => has_update_before = true,
                    CdcOperation::UpdateAfter => has_update_after = true,
                    _ => {}
                }
            }
        }
        assert!(has_update_before);
        assert!(has_update_after);
    }

    #[test]
    fn test_partitioned_topk_row_number_pattern() {
        // Simulates ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) WHERE rn <= 2
        let mut op = create_partitioned_topk(2, 100);
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let trades = vec![
            make_trade(1, "tech", 100.0),
            make_trade(2, "tech", 200.0),
            make_trade(3, "tech", 150.0),  // evicts 100
            make_trade(4, "finance", 300.0),
            make_trade(5, "finance", 250.0),
        ];

        for trade in &trades {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(trade, &mut ctx);
        }

        assert_eq!(op.partition_count(), 2);
        assert_eq!(op.total_entries(), 4); // 2 in tech + 2 in finance
    }

    #[test]
    fn test_partitioned_topk_string_partition_key() {
        let mut op = create_partitioned_topk(3, 100);
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        let trades = vec![
            make_trade(1, "electronics", 100.0),
            make_trade(2, "clothing", 200.0),
            make_trade(3, "electronics", 150.0),
        ];

        for trade in &trades {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
            op.process(trade, &mut ctx);
        }

        assert_eq!(op.partition_count(), 2);
    }

    #[test]
    fn test_partitioned_topk_null_partition_key() {
        let mut op = create_partitioned_topk(3, 100);
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Create event with null category
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, true),
            Field::new("price", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::new_null(1)),
                Arc::new(Float64Array::from(vec![100.0])),
            ],
        )
        .unwrap();
        let null_event = Event::new(1, batch);

        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        op.process(&null_event, &mut ctx);

        // Null partition key should still create a partition
        assert_eq!(op.partition_count(), 1);
    }

    #[test]
    fn test_partitioned_topk_changelog_per_partition() {
        let mut op = create_partitioned_topk(2, 100);
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Insert in partition A
        let e1 = make_trade(1, "A", 100.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let out_a = op.process(&e1, &mut ctx);

        // Insert in partition B
        let e2 = make_trade(2, "B", 200.0);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
        let out_b = op.process(&e2, &mut ctx);

        // Both should independently emit Insert changelog
        assert_eq!(out_a.len(), 1);
        assert_eq!(out_b.len(), 1);
    }

    #[test]
    fn test_partitioned_topk_large_partitions() {
        let mut op = create_partitioned_topk(5, 1000);
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut wm = BoundedOutOfOrdernessGenerator::new(0);

        // Create many partitions with a few entries each
        for i in 0..50 {
            let category = format!("cat_{}", i);
            for j in 0..3 {
                let trade = make_trade(i * 100 + j, &category, j as f64 * 10.0);
                let mut ctx = create_test_context(&mut timers, &mut state, &mut wm);
                op.process(&trade, &mut ctx);
            }
        }

        assert_eq!(op.partition_count(), 50);
        assert_eq!(op.total_entries(), 150); // 50 partitions * 3 entries each
    }
}
