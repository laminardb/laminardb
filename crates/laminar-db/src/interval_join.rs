#![deny(clippy::disallowed_types)]

//! Stream-stream interval join execution.
//!
//! Implements a stateful interval join that buffers rows from both sides
//! across execution cycles, matching any pair where
//! `|left_ts - right_ts| <= time_bound_ms`. Expired rows are evicted
//! when the watermark advances beyond `time_bound_ms`.
//!
//! The join state is checkpointed via Arrow IPC serialization.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
    UInt32Array,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use rustc_hash::FxHashMap;

use laminar_sql::translator::StreamJoinConfig;

use crate::aggregate_state::JoinStateCheckpoint;
use crate::error::DbError;

// ── Key helpers (same pattern as asof_batch.rs) ────────────────────────────

/// A borrowed reference to a key column, avoiding per-row String allocations.
enum KeyColumn<'a> {
    Utf8(&'a StringArray),
    Int64(&'a Int64Array),
}

impl KeyColumn<'_> {
    fn is_null(&self, i: usize) -> bool {
        match self {
            KeyColumn::Utf8(a) => a.is_null(i),
            KeyColumn::Int64(a) => a.is_null(i),
        }
    }

    fn hash_at(&self, i: usize) -> Option<u64> {
        if self.is_null(i) {
            return None;
        }
        let mut hasher = DefaultHasher::new();
        match self {
            KeyColumn::Utf8(a) => a.value(i).hash(&mut hasher),
            KeyColumn::Int64(a) => a.value(i).hash(&mut hasher),
        }
        Some(hasher.finish())
    }

    fn keys_equal(&self, i: usize, other: &KeyColumn<'_>, j: usize) -> bool {
        if self.is_null(i) || other.is_null(j) {
            return false;
        }
        match (self, other) {
            (KeyColumn::Utf8(a), KeyColumn::Utf8(b)) => a.value(i) == b.value(j),
            (KeyColumn::Int64(a), KeyColumn::Int64(b)) => a.value(i) == b.value(j),
            _ => false,
        }
    }
}

fn extract_key_column<'a>(
    batch: &'a RecordBatch,
    col_name: &str,
) -> Result<KeyColumn<'a>, DbError> {
    let col_idx = batch
        .schema()
        .index_of(col_name)
        .map_err(|_| DbError::Pipeline(format!("Column '{col_name}' not found")))?;
    let array = batch.column(col_idx);
    match array.data_type() {
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DbError::Pipeline(format!("Column '{col_name}' is not Utf8")))?;
            Ok(KeyColumn::Utf8(a))
        }
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DbError::Pipeline(format!("Column '{col_name}' is not Int64")))?;
            Ok(KeyColumn::Int64(a))
        }
        other => Err(DbError::Pipeline(format!(
            "Unsupported key column type: {other}"
        ))),
    }
}

fn extract_column_as_timestamps(batch: &RecordBatch, col_name: &str) -> Result<Vec<i64>, DbError> {
    let col_idx = batch
        .schema()
        .index_of(col_name)
        .map_err(|_| DbError::Pipeline(format!("Timestamp column '{col_name}' not found")))?;
    let array = batch.column(col_idx);
    match array.data_type() {
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DbError::Pipeline(format!("Column '{col_name}' is not Int64")))?;
            Ok(a.values().to_vec())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    DbError::Pipeline(format!("Column '{col_name}' is not TimestampMillisecond"))
                })?;
            Ok(a.values().to_vec())
        }
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DbError::Pipeline(format!("Column '{col_name}' is not Float64")))?;
            #[allow(clippy::cast_possible_truncation)]
            Ok(a.values().iter().map(|v| *v as i64).collect())
        }
        other => Err(DbError::Pipeline(format!(
            "Unsupported timestamp column type for '{col_name}': {other}"
        ))),
    }
}

// ── Per-side state ─────────────────────────────────────────────────────────

/// Compact when accumulated batch count exceeds this threshold.
const COMPACTION_THRESHOLD: usize = 32;

/// Index type: `key_hash` → sorted `timestamp` → list of `(batch_idx, row_idx)`.
type SideIndex = FxHashMap<u64, BTreeMap<i64, Vec<(usize, usize)>>>;

/// Per-side state for interval join, persisted across cycles.
pub(crate) struct SideState {
    /// Accumulated batches from previous cycles, compacted periodically.
    batches: Vec<RecordBatch>,
    /// Index: `key_hash` → `BTreeMap<timestamp, Vec<(batch_idx, row_idx)>>`
    index: SideIndex,
    /// Total buffered rows.
    row_count: usize,
}

impl SideState {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
            index: FxHashMap::default(),
            row_count: 0,
        }
    }

    /// Add a batch and index its rows.
    fn add_batch(
        &mut self,
        batch: &RecordBatch,
        key_col_name: &str,
        time_col_name: &str,
    ) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let batch_idx = self.batches.len();
        let keys = extract_key_column(batch, key_col_name)?;
        let timestamps = extract_column_as_timestamps(batch, time_col_name)?;

        let mut indexed_rows = 0usize;
        for (row_idx, &ts) in timestamps.iter().enumerate() {
            if let Some(key_hash) = keys.hash_at(row_idx) {
                self.index
                    .entry(key_hash)
                    .or_default()
                    .entry(ts)
                    .or_default()
                    .push((batch_idx, row_idx));
                indexed_rows += 1;
            }
            // Null keys are skipped per SQL three-valued logic
        }
        self.row_count += indexed_rows;
        self.batches.push(batch.clone());
        Ok(())
    }

    /// Evict all rows with `ts < cutoff`, then compact if batch count exceeds threshold.
    fn evict_before(&mut self, cutoff: i64, key_col: &str, time_col: &str) -> Result<(), DbError> {
        // Collect entries to remove from each btree
        for btree in self.index.values_mut() {
            // BTreeMap::split_off returns entries >= cutoff; we keep those.
            let keep = btree.split_off(&cutoff);
            // Count evicted rows
            for entries in btree.values() {
                self.row_count = self.row_count.saturating_sub(entries.len());
            }
            *btree = keep;
        }
        // Remove empty btrees
        self.index.retain(|_, btree| !btree.is_empty());

        // Compact when too many batches have accumulated (dead rows waste memory)
        if self.batches.len() > COMPACTION_THRESHOLD {
            self.compact(key_col, time_col)?;
        }
        Ok(())
    }

    /// Compact all live rows into a single batch, freeing dead batch memory.
    fn compact(&mut self, key_col: &str, time_col: &str) -> Result<(), DbError> {
        // Collect all live (batch_idx, row_idx) from the index
        let mut live_rows: Vec<(usize, usize)> = Vec::with_capacity(self.row_count);
        for btree in self.index.values() {
            for entries in btree.values() {
                live_rows.extend_from_slice(entries);
            }
        }

        if live_rows.is_empty() {
            self.batches.clear();
            return Ok(());
        }

        // Sort by (batch_idx, row_idx) for sequential access
        live_rows.sort_unstable();

        // Group rows by batch and use `take` per batch instead of per-row slicing
        let schema = self.batches[0].schema();
        let compacted = take_rows_from_batches(
            &self.batches,
            &live_rows,
            &schema,
            "interval join (compact)",
        )?;

        // Replace batches and rebuild index
        self.batches = vec![compacted];
        self.index.clear();

        let keys = extract_key_column(&self.batches[0], key_col)?;
        let timestamps = extract_column_as_timestamps(&self.batches[0], time_col)?;
        for (row_idx, &ts) in timestamps.iter().enumerate() {
            if let Some(key_hash) = keys.hash_at(row_idx) {
                self.index
                    .entry(key_hash)
                    .or_default()
                    .entry(ts)
                    .or_default()
                    .push((0, row_idx));
            }
        }
        self.row_count = self.batches[0].num_rows();
        Ok(())
    }
}

// ── Complete join state ────────────────────────────────────────────────────

/// Complete interval join state for one query.
pub(crate) struct IntervalJoinState {
    left: SideState,
    right: SideState,
    /// Last watermark used for eviction.
    evicted_watermark: i64,
    /// Output schema (left cols + right cols).
    output_schema: Option<SchemaRef>,
}

impl IntervalJoinState {
    /// Create empty state.
    pub(crate) fn new() -> Self {
        Self {
            left: SideState::new(),
            right: SideState::new(),
            evicted_watermark: i64::MIN,
            output_schema: None,
        }
    }

    /// Estimated total memory usage in bytes.
    pub(crate) fn estimated_size_bytes(&self) -> usize {
        let mut size = 0usize;
        for b in &self.left.batches {
            size += b.get_array_memory_size();
        }
        for b in &self.right.batches {
            size += b.get_array_memory_size();
        }
        // Rough estimate: index overhead ~64 bytes per entry
        let index_entries: usize = self.left.index.values().map(BTreeMap::len).sum::<usize>()
            + self.right.index.values().map(BTreeMap::len).sum::<usize>();
        size += index_entries * 64;
        size
    }

    /// Serialize to checkpoint. Compacts both sides first so that only live
    /// rows are serialized (dead/evicted rows in old batches are discarded).
    pub(crate) fn snapshot_checkpoint(
        &mut self,
        left_key: &str,
        left_time: &str,
        right_key: &str,
        right_time: &str,
    ) -> Result<JoinStateCheckpoint, DbError> {
        // Compact before serialization to avoid checkpointing dead rows
        if !self.left.batches.is_empty() {
            self.left.compact(left_key, left_time)?;
        }
        if !self.right.batches.is_empty() {
            self.right.compact(right_key, right_time)?;
        }

        let mut left_batches_ipc = Vec::with_capacity(self.left.batches.len());
        for batch in &self.left.batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let ipc = laminar_core::serialization::serialize_batch_stream(batch).map_err(|e| {
                DbError::Pipeline(format!("interval join left batch serialization: {e}"))
            })?;
            left_batches_ipc.push(ipc);
        }

        let mut right_batches_ipc = Vec::with_capacity(self.right.batches.len());
        for batch in &self.right.batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let ipc = laminar_core::serialization::serialize_batch_stream(batch).map_err(|e| {
                DbError::Pipeline(format!("interval join right batch serialization: {e}"))
            })?;
            right_batches_ipc.push(ipc);
        }

        Ok(JoinStateCheckpoint {
            left_buffer_rows: self.left.row_count as u64,
            right_buffer_rows: self.right.row_count as u64,
            left_batches: left_batches_ipc,
            right_batches: right_batches_ipc,
            last_evicted_watermark: self.evicted_watermark,
        })
    }

    /// Restore from a checkpoint. The index is rebuilt from the deserialized
    /// batches using the provided key/time column names.
    pub(crate) fn from_checkpoint(
        cp: &JoinStateCheckpoint,
        left_key_col: &str,
        left_time_col: &str,
        right_key_col: &str,
        right_time_col: &str,
    ) -> Result<Self, DbError> {
        let mut state = Self::new();
        state.evicted_watermark = cp.last_evicted_watermark;

        for ipc_bytes in &cp.left_batches {
            let batch =
                laminar_core::serialization::deserialize_batch_stream(ipc_bytes).map_err(|e| {
                    DbError::Pipeline(format!("interval join left batch deserialization: {e}"))
                })?;
            state.left.add_batch(&batch, left_key_col, left_time_col)?;
        }

        for ipc_bytes in &cp.right_batches {
            let batch =
                laminar_core::serialization::deserialize_batch_stream(ipc_bytes).map_err(|e| {
                    DbError::Pipeline(format!("interval join right batch deserialization: {e}"))
                })?;
            state
                .right
                .add_batch(&batch, right_key_col, right_time_col)?;
        }

        Ok(state)
    }
}

// ── Core join execution ────────────────────────────────────────────────────

/// Build the merged output schema from left and right schemas.
///
/// All right-side columns are suffixed with `_{right_table}` to avoid
/// ambiguity. Only INNER JOIN is supported (LEFT/RIGHT/FULL are rejected
/// at detection time).
fn build_output_schema(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    config: &StreamJoinConfig,
) -> SchemaRef {
    let mut fields: Vec<Field> = left_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();

    for field in right_schema.fields() {
        let f = field.as_ref().clone();
        let suffixed = format!("{}_{}", f.name(), config.right_table);
        fields.push(f.with_name(suffixed));
    }

    Arc::new(Schema::new(fields))
}

/// Probe one side's index for matches against a single row from the other side.
///
/// Returns all `(batch_idx, row_idx)` where `|probe_ts - candidate_ts| <= bound_ms`.
fn probe_index(
    index: &SideIndex,
    key_hash: u64,
    probe_ts: i64,
    bound_ms: i64,
) -> Vec<(usize, usize)> {
    let Some(btree) = index.get(&key_hash) else {
        return Vec::new();
    };
    let low = probe_ts.saturating_sub(bound_ms);
    let high = probe_ts.saturating_add(bound_ms);
    let mut results = Vec::new();
    for (_, entries) in btree.range(low..=high) {
        results.extend_from_slice(entries);
    }
    results
}

/// Gather rows from multiple batches using `arrow::compute::take` per batch,
/// then concatenate. Replaces the per-row `slice(row, 1)` + `concat` pattern
/// with O(B) take operations where B is the number of distinct source batches.
///
/// Works correctly for any ordering — output row order matches `rows` order.
/// Most effective when rows are batch-sorted (consecutive runs from the same
/// batch are gathered in a single `take` call).
fn take_rows_from_batches(
    batches: &[RecordBatch],
    rows: &[(usize, usize)],
    schema: &SchemaRef,
    context: &str,
) -> Result<RecordBatch, DbError> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    // If all rows come from one batch, fast-path: single take
    let first_batch = rows[0].0;
    let single_batch = rows.iter().all(|&(b, _)| b == first_batch);

    if single_batch {
        #[allow(clippy::cast_possible_truncation)]
        let indices = UInt32Array::from(rows.iter().map(|&(_, r)| r as u32).collect::<Vec<_>>());
        let columns: Vec<ArrayRef> = (0..batches[first_batch].num_columns())
            .map(|col_idx| {
                arrow::compute::take(batches[first_batch].column(col_idx), &indices, None)
                    .map_err(|e| DbError::query_pipeline_arrow(context, &e))
            })
            .collect::<Result<_, _>>()?;
        return RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| DbError::query_pipeline_arrow(context, &e));
    }

    // Multi-batch: group consecutive runs by batch_idx, take per group, concat.
    // We preserve output ordering by processing rows in their original order.
    let mut segments: Vec<RecordBatch> = Vec::new();
    let mut seg_start = 0;

    while seg_start < rows.len() {
        let batch_idx = rows[seg_start].0;
        let mut seg_end = seg_start + 1;
        while seg_end < rows.len() && rows[seg_end].0 == batch_idx {
            seg_end += 1;
        }

        #[allow(clippy::cast_possible_truncation)]
        let indices = UInt32Array::from(
            rows[seg_start..seg_end]
                .iter()
                .map(|&(_, r)| r as u32)
                .collect::<Vec<_>>(),
        );
        let columns: Vec<ArrayRef> = (0..batches[batch_idx].num_columns())
            .map(|col_idx| {
                arrow::compute::take(batches[batch_idx].column(col_idx), &indices, None)
                    .map_err(|e| DbError::query_pipeline_arrow(context, &e))
            })
            .collect::<Result<_, _>>()?;
        let segment = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| DbError::query_pipeline_arrow(context, &e))?;
        segments.push(segment);

        seg_start = seg_end;
    }

    if segments.len() == 1 {
        return Ok(segments.into_iter().next().unwrap());
    }

    concat_batches(schema, &segments).map_err(|e| DbError::query_pipeline_arrow(context, &e))
}

/// Execute one cycle of an interval join (INNER only).
///
/// Bounds are inclusive: `|left_ts - right_ts| <= bound_ms`. NULL keys are
/// skipped. New left rows probe all right; new right rows probe only old left
/// (avoids double-emit). State is compacted on eviction when batch count
/// exceeds `COMPACTION_THRESHOLD`.
#[allow(clippy::too_many_lines)]
pub(crate) fn execute_interval_join_cycle(
    state: &mut IntervalJoinState,
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    config: &StreamJoinConfig,
    watermark: i64,
) -> Result<Vec<RecordBatch>, DbError> {
    let bound_ms = i64::try_from(config.time_bound.as_millis()).unwrap_or(i64::MAX);

    // Concat incoming batches for each side
    let new_left = if left_batches.is_empty() {
        None
    } else {
        let schema = left_batches[0].schema();
        Some(
            concat_batches(&schema, left_batches)
                .map_err(|e| DbError::query_pipeline_arrow("interval join (left concat)", &e))?,
        )
    };

    let new_right = if right_batches.is_empty() {
        None
    } else {
        let schema = right_batches[0].schema();
        Some(
            concat_batches(&schema, right_batches)
                .map_err(|e| DbError::query_pipeline_arrow("interval join (right concat)", &e))?,
        )
    };

    // Record the boundary between old and new batches on each side
    let left_old_count = state.left.batches.len();
    let right_old_count = state.right.batches.len();

    // Step 1: Buffer new right into state first (so new left can probe it)
    if let Some(ref rb) = new_right {
        state
            .right
            .add_batch(rb, &config.right_key, &config.right_time_column)?;
    }

    // Collect match pairs: (left_batch_idx, left_row_idx, right_batch_idx, right_row_idx)
    let mut match_pairs: Vec<(usize, usize, usize, usize)> = Vec::new();

    // Step 2: Probe new left rows against all right (old + new)
    if let Some(ref lb) = new_left {
        let left_keys = extract_key_column(lb, &config.left_key)?;
        let left_timestamps = extract_column_as_timestamps(lb, &config.left_time_column)?;

        // The new left batch will be added at batch_idx = left_old_count
        let new_left_batch_idx = left_old_count;

        for (row_idx, &left_ts) in left_timestamps.iter().enumerate() {
            let Some(key_hash) = left_keys.hash_at(row_idx) else {
                continue; // Skip null keys
            };
            let candidates = probe_index(&state.right.index, key_hash, left_ts, bound_ms);

            for (r_batch, r_row) in candidates {
                let r_key_col =
                    extract_key_column(&state.right.batches[r_batch], &config.right_key)?;
                if left_keys.keys_equal(row_idx, &r_key_col, r_row) {
                    match_pairs.push((new_left_batch_idx, row_idx, r_batch, r_row));
                }
            }
        }
    }

    // Step 3: Probe new right rows against OLD left rows only (avoid double-emit)
    if let Some(ref rb) = new_right {
        let right_keys = extract_key_column(rb, &config.right_key)?;
        let right_timestamps = extract_column_as_timestamps(rb, &config.right_time_column)?;

        let new_right_batch_idx = right_old_count;

        for (row_idx, &right_ts) in right_timestamps.iter().enumerate() {
            let Some(key_hash) = right_keys.hash_at(row_idx) else {
                continue; // Skip null keys
            };
            let candidates = probe_index(&state.left.index, key_hash, right_ts, bound_ms);

            for (l_batch, l_row) in candidates {
                if l_batch < left_old_count {
                    let l_key_col =
                        extract_key_column(&state.left.batches[l_batch], &config.left_key)?;
                    if right_keys.keys_equal(row_idx, &l_key_col, l_row) {
                        match_pairs.push((l_batch, l_row, new_right_batch_idx, row_idx));
                    }
                }
            }
        }
    }

    // Step 4: Buffer new left rows into state (after probing to get correct indices)
    if let Some(ref lb) = new_left {
        state
            .left
            .add_batch(lb, &config.left_key, &config.left_time_column)?;
    }

    // Determine or cache output schema (now both sides are buffered)
    if state.output_schema.is_none() {
        let left_schema = state.left.batches.first().map(RecordBatch::schema);
        let right_schema = state.right.batches.first().map(RecordBatch::schema);
        if let (Some(ls), Some(rs)) = (left_schema, right_schema) {
            state.output_schema = Some(build_output_schema(&ls, &rs, config));
        }
    }

    // Step 5: Build output from match_pairs
    let result = if match_pairs.is_empty() {
        Vec::new()
    } else {
        let output_schema = state.output_schema.as_ref().ok_or_else(|| {
            DbError::Pipeline("interval join: output schema not available".to_string())
        })?;
        let left_schema = state
            .left
            .batches
            .first()
            .map_or_else(|| Arc::new(Schema::empty()), RecordBatch::schema);
        let right_schema = state
            .right
            .batches
            .first()
            .map_or_else(|| Arc::new(Schema::empty()), RecordBatch::schema);

        // Extract left and right (batch_idx, row_idx) pairs from match_pairs
        let (left_pairs, right_pairs): (Vec<_>, Vec<_>) = match_pairs
            .iter()
            .map(|&(lb, lr, rb, rr)| ((lb, lr), (rb, rr)))
            .unzip();

        // Build output columns using take-based gathering
        let mut columns: Vec<ArrayRef> =
            Vec::with_capacity(left_schema.fields().len() + right_schema.fields().len());

        let left_gathered = take_rows_from_batches(
            &state.left.batches,
            &left_pairs,
            &left_schema,
            "interval join (left)",
        )?;
        for col_idx in 0..left_schema.fields().len() {
            columns.push(left_gathered.column(col_idx).clone());
        }

        let right_gathered = take_rows_from_batches(
            &state.right.batches,
            &right_pairs,
            &right_schema,
            "interval join (right)",
        )?;
        for col_idx in 0..right_schema.fields().len() {
            columns.push(right_gathered.column(col_idx).clone());
        }

        let batch = RecordBatch::try_new(output_schema.clone(), columns)
            .map_err(|e| DbError::query_pipeline_arrow("interval join (result)", &e))?;
        if batch.num_rows() > 0 {
            vec![batch]
        } else {
            Vec::new()
        }
    };

    // Step 6: Evict expired rows (and compact if batch count exceeds threshold)
    let cutoff = watermark.saturating_sub(bound_ms);
    if cutoff > state.evicted_watermark {
        state
            .left
            .evict_before(cutoff, &config.left_key, &config.left_time_column)?;
        state
            .right
            .evict_before(cutoff, &config.right_key, &config.right_time_column)?;
        state.evicted_watermark = cutoff;
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use laminar_sql::translator::StreamJoinType;
    use std::time::Duration;

    fn make_config() -> StreamJoinConfig {
        StreamJoinConfig {
            left_key: "id".to_string(),
            right_key: "id".to_string(),
            left_time_column: "ts".to_string(),
            right_time_column: "ts".to_string(),
            left_table: "left_stream".to_string(),
            right_table: "right_stream".to_string(),
            time_bound: Duration::from_millis(100),
            join_type: StreamJoinType::Inner,
        }
    }

    fn left_batch(ids: &[&str], timestamps: &[i64], values: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn right_batch(ids: &[&str], timestamps: &[i64], amounts: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(amounts.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_basic_inner_join_same_cycle() {
        let config = make_config();
        let mut state = IntervalJoinState::new();

        let left = left_batch(&["A", "B"], &[100, 200], &[10.0, 20.0]);
        let right = right_batch(&["A", "B"], &[110, 250], &[1.0, 2.0]);

        let result =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0).unwrap();

        // A: |100 - 110| = 10 <= 100 → match
        // B: |200 - 250| = 50 <= 100 → match
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
        assert_eq!(result[0].num_columns(), 6); // 3 left + 3 right
    }

    #[test]
    fn test_cross_cycle_matching() {
        let config = make_config();
        let mut state = IntervalJoinState::new();

        // Cycle 1: only left data
        let left = left_batch(&["A"], &[100], &[10.0]);
        let result = execute_interval_join_cycle(&mut state, &[left], &[], &config, 0).unwrap();
        assert!(result.is_empty()); // No right data yet

        // Cycle 2: right data arrives, should match the buffered left
        let right = right_batch(&["A"], &[150], &[1.0]);
        let result = execute_interval_join_cycle(&mut state, &[], &[right], &config, 0).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1); // |100 - 150| = 50 <= 100
    }

    #[test]
    fn test_time_bound_enforcement() {
        let config = make_config(); // time_bound = 100ms
        let mut state = IntervalJoinState::new();

        let left = left_batch(&["A"], &[100], &[10.0]);
        let right = right_batch(&["A"], &[300], &[1.0]); // |100 - 300| = 200 > 100

        let result =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0).unwrap();
        assert!(result.is_empty()); // Outside time bound
    }

    #[test]
    fn test_eviction_on_watermark_advance() {
        let config = make_config(); // time_bound = 100ms
        let mut state = IntervalJoinState::new();

        // Cycle 1: buffer left row at ts=100
        let left = left_batch(&["A"], &[100], &[10.0]);
        let _ = execute_interval_join_cycle(&mut state, &[left], &[], &config, 0).unwrap();
        assert_eq!(state.left.row_count, 1);

        // Cycle 2: advance watermark to 300 → cutoff = 300 - 100 = 200
        // Row at ts=100 < 200, should be evicted
        let _ = execute_interval_join_cycle(&mut state, &[], &[], &config, 300).unwrap();
        assert_eq!(state.left.row_count, 0);
    }

    #[test]
    fn test_multiple_keys() {
        let config = make_config();
        let mut state = IntervalJoinState::new();

        let left = left_batch(&["A", "B"], &[100, 100], &[10.0, 20.0]);
        let right = right_batch(&["B", "A"], &[110, 110], &[1.0, 2.0]);

        let result =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0).unwrap();

        // A@100 matches A@110 (|100-110|=10 <= 100) ✓
        // B@100 matches B@110 (|100-110|=10 <= 100) ✓
        // A@100 does NOT match B@110 (different keys)
        // B@100 does NOT match A@110 (different keys)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[test]
    fn test_no_double_emit() {
        let config = make_config();
        let mut state = IntervalJoinState::new();

        // Both sides in same cycle — each match should appear exactly once
        let left = left_batch(&["A"], &[100], &[10.0]);
        let right = right_batch(&["A"], &[110], &[1.0]);

        let result =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1); // Exactly one match, not two
    }

    #[test]
    fn test_empty_inputs() {
        let config = make_config();
        let mut state = IntervalJoinState::new();

        let result = execute_interval_join_cycle(&mut state, &[], &[], &config, 0).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_checkpoint_roundtrip() {
        let config = make_config();
        let mut state = IntervalJoinState::new();

        let left = left_batch(&["A"], &[100], &[10.0]);
        let right = right_batch(&["A"], &[110], &[1.0]);
        let _ = execute_interval_join_cycle(&mut state, &[left], &[right], &config, 50).unwrap();

        // Checkpoint (compacts before serializing)
        let cp = state
            .snapshot_checkpoint(
                &config.left_key,
                &config.left_time_column,
                &config.right_key,
                &config.right_time_column,
            )
            .unwrap();
        assert!(cp.left_buffer_rows > 0);
        assert!(cp.right_buffer_rows > 0);

        // Restore
        let mut restored = IntervalJoinState::from_checkpoint(
            &cp,
            &config.left_key,
            &config.left_time_column,
            &config.right_key,
            &config.right_time_column,
        )
        .unwrap();

        // New right data should still match the restored left
        let right2 = right_batch(&["A"], &[120], &[2.0]);
        let result =
            execute_interval_join_cycle(&mut restored, &[], &[right2], &config, 50).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1); // Matches restored A@100
    }

    fn left_batch_nullable(
        ids: &[Option<&str>],
        timestamps: &[i64],
        values: &[f64],
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn right_batch_nullable(
        ids: &[Option<&str>],
        timestamps: &[i64],
        amounts: &[f64],
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(amounts.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_null_key_no_match() {
        let config = make_config();
        let mut state = IntervalJoinState::new();

        // Left has a null key row, right has a matching timestamp
        let left = left_batch_nullable(&[Some("A"), None], &[100, 100], &[10.0, 20.0]);
        let right = right_batch_nullable(&[Some("A"), None], &[110, 110], &[1.0, 2.0]);

        let result =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0).unwrap();

        // Only A matches A — null keys never match (SQL three-valued logic)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
    }

    #[test]
    fn test_compaction_frees_batches() {
        let config = make_config(); // time_bound = 100ms
        let mut state = IntervalJoinState::new();

        // Add 40+ single-row batches to left side
        for i in 0i64..40 {
            let ts = i * 10 + 1000;
            #[allow(clippy::cast_precision_loss)]
            let left = left_batch(&["A"], &[ts], &[i as f64]);
            let _ = execute_interval_join_cycle(&mut state, &[left], &[], &config, 0).unwrap();
        }
        assert!(state.left.batches.len() >= 40);

        // Evict the first half (ts < 1200). Watermark = 1300 → cutoff = 1300 - 100 = 1200
        let _ = execute_interval_join_cycle(&mut state, &[], &[], &config, 1300).unwrap();

        // After compaction (triggered because batch count > COMPACTION_THRESHOLD),
        // should have exactly 1 batch with only live rows
        assert_eq!(state.left.batches.len(), 1);
        assert!(state.left.row_count > 0);

        // Verify live rows are still accessible by probing with a right-side match
        let right = right_batch(&["A"], &[1350], &[99.0]);
        let result = execute_interval_join_cycle(&mut state, &[], &[right], &config, 1300).unwrap();
        // Should match rows within [1250, 1450] — rows at ts=1300..1390 should be live
        assert!(!result.is_empty());
    }
}
