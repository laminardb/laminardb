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
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use rustc_hash::{FxHashMap, FxHashSet};

use laminar_sql::translator::{StreamJoinConfig, StreamJoinType};

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

    /// Remove entries matching `(key_hash, timestamp)` from the index.
    fn remove_by_key_ts(&mut self, key_hash: u64, ts: i64) {
        if let Some(btree) = self.index.get_mut(&key_hash) {
            if let Some(entries) = btree.remove(&ts) {
                self.row_count = self.row_count.saturating_sub(entries.len());
            }
            if btree.is_empty() {
                self.index.remove(&key_hash);
            }
        }
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

        // Slice each live row and collect
        let mut slices: Vec<RecordBatch> = Vec::with_capacity(live_rows.len());
        for &(batch_idx, row_idx) in &live_rows {
            slices.push(self.batches[batch_idx].slice(row_idx, 1));
        }

        let schema = self.batches[0].schema();
        let compacted = concat_batches(&schema, &slices)
            .map_err(|e| DbError::query_pipeline_arrow("interval join (compact)", &e))?;

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
    /// Last cutoff used for left-side eviction (derived from right watermark).
    left_evicted_cutoff: i64,
    /// Last cutoff used for right-side eviction (derived from left watermark).
    right_evicted_cutoff: i64,
    /// Output schema (left cols + right cols).
    output_schema: Option<SchemaRef>,
}

impl IntervalJoinState {
    /// Create empty state.
    pub(crate) fn new() -> Self {
        Self {
            left: SideState::new(),
            right: SideState::new(),
            left_evicted_cutoff: i64::MIN,
            right_evicted_cutoff: i64::MIN,
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
            last_evicted_watermark: self.left_evicted_cutoff,
            last_evicted_watermark_right: self.right_evicted_cutoff,
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
        state.left_evicted_cutoff = cp.last_evicted_watermark;
        state.right_evicted_cutoff = cp.last_evicted_watermark_right;

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
fn build_output_schema(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    config: &StreamJoinConfig,
) -> SchemaRef {
    let left_nullable = matches!(
        config.join_type,
        StreamJoinType::Right | StreamJoinType::Full
    );
    let right_nullable = matches!(
        config.join_type,
        StreamJoinType::Left | StreamJoinType::Full
    );

    let mut fields: Vec<Field> = left_schema
        .fields()
        .iter()
        .map(|f| {
            let mut field = f.as_ref().clone();
            if left_nullable {
                field = field.with_nullable(true);
            }
            field
        })
        .collect();

    // Semi/Anti: left columns only
    if matches!(
        config.join_type,
        StreamJoinType::LeftSemi | StreamJoinType::LeftAnti
    ) {
        return Arc::new(Schema::new(fields));
    }

    for field in right_schema.fields() {
        let mut f = field.as_ref().clone();
        if right_nullable {
            f = f.with_nullable(true);
        }
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
    left_watermark: i64,
    right_watermark: i64,
) -> Result<Vec<RecordBatch>, DbError> {
    let bound_ms = i64::try_from(config.time_bound.as_millis()).unwrap_or(i64::MAX);

    // Separate positive (I/U+) and negative (D/U-) CDC events.
    // Positive events are added to state; negative events remove from state.
    let left_pos: Vec<RecordBatch> = left_batches
        .iter()
        .map(crate::changelog_filter::filter_positive_events)
        .collect::<Result<Vec<_>, _>>()?;
    let right_pos: Vec<RecordBatch> = right_batches
        .iter()
        .map(crate::changelog_filter::filter_positive_events)
        .collect::<Result<Vec<_>, _>>()?;

    // Apply deletes: remove matching (key_hash, ts) from state
    for raw_batch in left_batches {
        if let Some(neg) = crate::changelog_filter::extract_negative_events(raw_batch)? {
            let keys = extract_key_column(&neg, &config.left_key)?;
            let timestamps = extract_column_as_timestamps(&neg, &config.left_time_column)?;
            for (i, &ts) in timestamps.iter().enumerate() {
                if let Some(kh) = keys.hash_at(i) {
                    state.left.remove_by_key_ts(kh, ts);
                }
            }
        }
    }
    for raw_batch in right_batches {
        if let Some(neg) = crate::changelog_filter::extract_negative_events(raw_batch)? {
            let keys = extract_key_column(&neg, &config.right_key)?;
            let timestamps = extract_column_as_timestamps(&neg, &config.right_time_column)?;
            for (i, &ts) in timestamps.iter().enumerate() {
                if let Some(kh) = keys.hash_at(i) {
                    state.right.remove_by_key_ts(kh, ts);
                }
            }
        }
    }

    let left_batches = &left_pos[..];
    let right_batches = &right_pos[..];

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
    let is_semi = config.join_type == StreamJoinType::LeftSemi;
    // For Semi: track which new left rows already matched (at most one per left row)
    let mut semi_matched: FxHashSet<usize> = FxHashSet::default();

    // Step 2: Probe new left rows against all right (old + new)
    if let Some(ref lb) = new_left {
        let left_keys = extract_key_column(lb, &config.left_key)?;
        let left_timestamps = extract_column_as_timestamps(lb, &config.left_time_column)?;

        // The new left batch will be added at batch_idx = left_old_count
        let new_left_batch_idx = left_old_count;

        for (row_idx, &left_ts) in left_timestamps.iter().enumerate() {
            if is_semi && semi_matched.contains(&row_idx) {
                continue;
            }
            let Some(key_hash) = left_keys.hash_at(row_idx) else {
                continue; // Skip null keys
            };
            let candidates = probe_index(&state.right.index, key_hash, left_ts, bound_ms);

            for (r_batch, r_row) in candidates {
                if is_semi && semi_matched.contains(&row_idx) {
                    break;
                }
                let r_key_col =
                    extract_key_column(&state.right.batches[r_batch], &config.right_key)?;
                if left_keys.keys_equal(row_idx, &r_key_col, r_row) {
                    match_pairs.push((new_left_batch_idx, row_idx, r_batch, r_row));
                    if is_semi {
                        semi_matched.insert(row_idx);
                    }
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

    // Determine or update output schema. Rebuild when both sides become available
    // to ensure right-column suffixing is correct.
    {
        let left_schema = state.left.batches.first().map(RecordBatch::schema);
        let right_schema = state.right.batches.first().map(RecordBatch::schema);
        match (left_schema, right_schema) {
            (Some(ls), Some(rs)) => {
                // Always set when both sides available (may upgrade a partial schema)
                state.output_schema = Some(build_output_schema(&ls, &rs, config));
            }
            (Some(ls), None)
                if state.output_schema.is_none()
                    && matches!(
                        config.join_type,
                        StreamJoinType::LeftSemi | StreamJoinType::LeftAnti
                    ) =>
            {
                state.output_schema =
                    Some(build_output_schema(&ls, &Arc::new(Schema::empty()), config));
            }
            (None, Some(rs))
                if state.output_schema.is_none() && config.join_type == StreamJoinType::Right =>
            {
                state.output_schema =
                    Some(build_output_schema(&Arc::new(Schema::empty()), &rs, config));
            }
            _ => {}
        }
    }

    let left_only = matches!(
        config.join_type,
        StreamJoinType::LeftSemi | StreamJoinType::LeftAnti
    );

    // Step 5: Build output from match_pairs (Anti emits nothing here — only at eviction)
    let mut result = if match_pairs.is_empty() || config.join_type == StreamJoinType::LeftAnti {
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

        let num_rows = match_pairs.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());

        // Build left columns
        for col_idx in 0..left_schema.fields().len() {
            let mut builder = Vec::with_capacity(num_rows);
            for &(l_batch, l_row, _, _) in &match_pairs {
                let array = state.left.batches[l_batch].column(col_idx);
                let sliced = array.slice(l_row, 1);
                builder.push(sliced);
            }
            let refs: Vec<&dyn Array> = builder.iter().map(AsRef::as_ref).collect();
            let concatenated = arrow::compute::concat(&refs)
                .map_err(|e| DbError::query_pipeline_arrow("interval join (left concat)", &e))?;
            columns.push(concatenated);
        }

        // Build right columns (skip for Semi/Anti)
        if !left_only {
            let right_schema = state
                .right
                .batches
                .first()
                .map_or_else(|| Arc::new(Schema::empty()), RecordBatch::schema);
            for col_idx in 0..right_schema.fields().len() {
                let mut builder = Vec::with_capacity(num_rows);
                for &(_, _, r_batch, r_row) in &match_pairs {
                    let array = state.right.batches[r_batch].column(col_idx);
                    let sliced = array.slice(r_row, 1);
                    builder.push(sliced);
                }
                let refs: Vec<&dyn Array> = builder.iter().map(AsRef::as_ref).collect();
                let concatenated = arrow::compute::concat(&refs).map_err(|e| {
                    DbError::query_pipeline_arrow("interval join (right concat)", &e)
                })?;
                columns.push(concatenated);
            }
        }

        let batch = RecordBatch::try_new(output_schema.clone(), columns)
            .map_err(|e| DbError::query_pipeline_arrow("interval join (result)", &e))?;
        if batch.num_rows() > 0 {
            vec![batch]
        } else {
            Vec::new()
        }
    };

    // Step 5.5: For LEFT/RIGHT/FULL/ANTI, emit unmatched rows about to be evicted.
    // Probe the opposite side's state — runs BEFORE eviction so all matches are available.
    if matches!(
        config.join_type,
        StreamJoinType::Left | StreamJoinType::Full | StreamJoinType::LeftAnti
    ) {
        let left_cutoff = right_watermark.saturating_sub(bound_ms);
        if left_cutoff > state.left_evicted_cutoff && !state.left.batches.is_empty() {
            let unmatched = emit_unmatched_left_rows(state, config, left_cutoff, bound_ms)?;
            if let Some(batch) = unmatched {
                result.push(batch);
            }
        }
    }
    if matches!(
        config.join_type,
        StreamJoinType::Right | StreamJoinType::Full
    ) {
        let right_cutoff = left_watermark.saturating_sub(bound_ms);
        if right_cutoff > state.right_evicted_cutoff && !state.right.batches.is_empty() {
            let unmatched = emit_unmatched_right_rows(state, config, right_cutoff, bound_ms)?;
            if let Some(batch) = unmatched {
                result.push(batch);
            }
        }
    }

    // Step 6: Evict expired rows independently per side.
    // A left row at ts can match right rows with ts in [left_ts - bound, left_ts + bound].
    // Once the right watermark passes left_ts + bound, no future right row can match.
    // So: evict left rows where ts < right_watermark - bound (and vice versa).
    let left_cutoff = right_watermark.saturating_sub(bound_ms);
    if left_cutoff > state.left_evicted_cutoff {
        state
            .left
            .evict_before(left_cutoff, &config.left_key, &config.left_time_column)?;
        state.left_evicted_cutoff = left_cutoff;
    }
    let right_cutoff = left_watermark.saturating_sub(bound_ms);
    if right_cutoff > state.right_evicted_cutoff {
        state
            .right
            .evict_before(right_cutoff, &config.right_key, &config.right_time_column)?;
        state.right_evicted_cutoff = right_cutoff;
    }

    Ok(result)
}

/// For LEFT/ANTI joins: emit left rows about to be evicted that have no match
/// in the current right state. Called BEFORE right-side eviction.
fn emit_unmatched_left_rows(
    state: &IntervalJoinState,
    config: &StreamJoinConfig,
    left_cutoff: i64,
    bound_ms: i64,
) -> Result<Option<RecordBatch>, DbError> {
    let Some(output_schema) = state.output_schema.as_ref() else {
        return Ok(None);
    };

    let left_only = matches!(
        config.join_type,
        StreamJoinType::LeftSemi | StreamJoinType::LeftAnti
    );

    let mut unmatched_left: Vec<(usize, usize)> = Vec::new();

    for (&key_hash, btree) in &state.left.index {
        for (&ts, entries) in btree.range(..left_cutoff) {
            for &(batch_idx, row_idx) in entries {
                let candidates = probe_index(&state.right.index, key_hash, ts, bound_ms);
                let left_key =
                    extract_key_column(&state.left.batches[batch_idx], &config.left_key)?;
                let has_match = candidates.iter().any(|&(rb, rr)| {
                    extract_key_column(&state.right.batches[rb], &config.right_key)
                        .is_ok_and(|rk| left_key.keys_equal(row_idx, &rk, rr))
                });
                if !has_match {
                    unmatched_left.push((batch_idx, row_idx));
                }
            }
        }
    }

    if unmatched_left.is_empty() {
        return Ok(None);
    }

    let left_schema = state
        .left
        .batches
        .first()
        .map_or_else(|| Arc::new(Schema::empty()), RecordBatch::schema);

    let num_rows = unmatched_left.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());

    // Left columns
    for col_idx in 0..left_schema.fields().len() {
        let mut builder = Vec::with_capacity(num_rows);
        for &(batch_idx, row_idx) in &unmatched_left {
            let array = state.left.batches[batch_idx].column(col_idx);
            builder.push(array.slice(row_idx, 1));
        }
        let refs: Vec<&dyn Array> = builder.iter().map(AsRef::as_ref).collect();
        let concatenated = arrow::compute::concat(&refs)
            .map_err(|e| DbError::query_pipeline_arrow("interval join (unmatched left)", &e))?;
        columns.push(concatenated);
    }

    // Right columns: NULLs (skip for Anti)
    if !left_only {
        let right_schema = state
            .right
            .batches
            .first()
            .map_or_else(|| Arc::new(Schema::empty()), RecordBatch::schema);
        for col_idx in 0..right_schema.fields().len() {
            let dt = output_schema
                .field(left_schema.fields().len() + col_idx)
                .data_type();
            columns.push(arrow::array::new_null_array(dt, num_rows));
        }
    }

    RecordBatch::try_new(output_schema.clone(), columns)
        .map(|b| if b.num_rows() > 0 { Some(b) } else { None })
        .map_err(|e| DbError::query_pipeline_arrow("interval join (unmatched result)", &e))
}

/// For RIGHT/FULL joins: emit right rows about to be evicted that have no match
/// in the current left state. Symmetric to `emit_unmatched_left_rows`.
fn emit_unmatched_right_rows(
    state: &IntervalJoinState,
    config: &StreamJoinConfig,
    right_cutoff: i64,
    bound_ms: i64,
) -> Result<Option<RecordBatch>, DbError> {
    let Some(output_schema) = state.output_schema.as_ref() else {
        return Ok(None);
    };

    let mut unmatched_right: Vec<(usize, usize)> = Vec::new();

    for (&key_hash, btree) in &state.right.index {
        for (&ts, entries) in btree.range(..right_cutoff) {
            for &(batch_idx, row_idx) in entries {
                let candidates = probe_index(&state.left.index, key_hash, ts, bound_ms);
                let right_key =
                    extract_key_column(&state.right.batches[batch_idx], &config.right_key)?;
                let has_match = candidates.iter().any(|&(lb, lr)| {
                    extract_key_column(&state.left.batches[lb], &config.left_key)
                        .is_ok_and(|lk| right_key.keys_equal(row_idx, &lk, lr))
                });
                if !has_match {
                    unmatched_right.push((batch_idx, row_idx));
                }
            }
        }
    }

    if unmatched_right.is_empty() {
        return Ok(None);
    }

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

    let num_rows = unmatched_right.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());

    // Left columns: NULLs
    for col_idx in 0..left_schema.fields().len() {
        let dt = output_schema.field(col_idx).data_type();
        columns.push(arrow::array::new_null_array(dt, num_rows));
    }

    // Right columns: actual data
    for col_idx in 0..right_schema.fields().len() {
        let mut builder = Vec::with_capacity(num_rows);
        for &(batch_idx, row_idx) in &unmatched_right {
            let array = state.right.batches[batch_idx].column(col_idx);
            builder.push(array.slice(row_idx, 1));
        }
        let refs: Vec<&dyn Array> = builder.iter().map(AsRef::as_ref).collect();
        let concatenated = arrow::compute::concat(&refs)
            .map_err(|e| DbError::query_pipeline_arrow("interval join (unmatched right)", &e))?;
        columns.push(concatenated);
    }

    RecordBatch::try_new(output_schema.clone(), columns)
        .map(|b| if b.num_rows() > 0 { Some(b) } else { None })
        .map_err(|e| DbError::query_pipeline_arrow("interval join (unmatched right result)", &e))
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
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();

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
        let result = execute_interval_join_cycle(&mut state, &[left], &[], &config, 0, 0).unwrap();
        assert!(result.is_empty()); // No right data yet

        // Cycle 2: right data arrives, should match the buffered left
        let right = right_batch(&["A"], &[150], &[1.0]);
        let result = execute_interval_join_cycle(&mut state, &[], &[right], &config, 0, 0).unwrap();
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
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();
        assert!(result.is_empty()); // Outside time bound
    }

    #[test]
    fn test_eviction_on_watermark_advance() {
        let config = make_config(); // time_bound = 100ms
        let mut state = IntervalJoinState::new();

        // Cycle 1: buffer left row at ts=100
        let left = left_batch(&["A"], &[100], &[10.0]);
        let _ = execute_interval_join_cycle(&mut state, &[left], &[], &config, 0, 0).unwrap();
        assert_eq!(state.left.row_count, 1);

        // Cycle 2: advance watermark to 300 → cutoff = 300 - 100 = 200
        // Row at ts=100 < 200, should be evicted
        let _ = execute_interval_join_cycle(&mut state, &[], &[], &config, 300, 300).unwrap();
        assert_eq!(state.left.row_count, 0);
    }

    #[test]
    fn test_multiple_keys() {
        let config = make_config();
        let mut state = IntervalJoinState::new();

        let left = left_batch(&["A", "B"], &[100, 100], &[10.0, 20.0]);
        let right = right_batch(&["B", "A"], &[110, 110], &[1.0, 2.0]);

        let result =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();

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
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1); // Exactly one match, not two
    }

    #[test]
    fn test_empty_inputs() {
        let config = make_config();
        let mut state = IntervalJoinState::new();

        let result = execute_interval_join_cycle(&mut state, &[], &[], &config, 0, 0).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_checkpoint_roundtrip() {
        let config = make_config();
        let mut state = IntervalJoinState::new();

        let left = left_batch(&["A"], &[100], &[10.0]);
        let right = right_batch(&["A"], &[110], &[1.0]);
        let _ =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 50, 50).unwrap();

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
            execute_interval_join_cycle(&mut restored, &[], &[right2], &config, 50, 50).unwrap();
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
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();

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
            let _ = execute_interval_join_cycle(&mut state, &[left], &[], &config, 0, 0).unwrap();
        }
        assert!(state.left.batches.len() >= 40);

        // Evict the first half (ts < 1200). Watermark = 1300 → cutoff = 1300 - 100 = 1200
        let _ = execute_interval_join_cycle(&mut state, &[], &[], &config, 1300, 1300).unwrap();

        // After compaction (triggered because batch count > COMPACTION_THRESHOLD),
        // should have exactly 1 batch with only live rows
        assert_eq!(state.left.batches.len(), 1);
        assert!(state.left.row_count > 0);

        // Verify live rows are still accessible by probing with a right-side match
        let right = right_batch(&["A"], &[1350], &[99.0]);
        let result =
            execute_interval_join_cycle(&mut state, &[], &[right], &config, 1300, 1300).unwrap();
        // Should match rows within [1250, 1450] — rows at ts=1300..1390 should be live
        assert!(!result.is_empty());
    }

    fn make_left_config() -> StreamJoinConfig {
        StreamJoinConfig {
            left_key: "id".to_string(),
            right_key: "id".to_string(),
            left_time_column: "ts".to_string(),
            right_time_column: "ts".to_string(),
            left_table: "left_stream".to_string(),
            right_table: "right_stream".to_string(),
            time_bound: Duration::from_millis(100),
            join_type: StreamJoinType::Left,
        }
    }

    #[test]
    fn test_left_join_unmatched_emitted_with_nulls() {
        let config = make_left_config();
        let mut state = IntervalJoinState::new();

        // Left at ts=100, right at ts=500 (outside bound, no match)
        let left = left_batch(&["A"], &[100], &[10.0]);
        let right = right_batch(&["B"], &[100], &[1.0]); // Different key, just to populate right schema
        let result =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();
        assert!(result.is_empty()); // No match (different keys)

        // Advance right watermark to 300 → left cutoff = 300 - 100 = 200
        // Left row at ts=100 < 200, deadline passed, no match → emit with NULLs
        let result = execute_interval_join_cycle(&mut state, &[], &[], &config, 0, 300).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
        // Left columns present, right columns null
        assert_eq!(result[0].num_columns(), 6); // 3 left + 3 right (suffixed)
        assert!(result[0].column(3).is_null(0)); // right ts null
    }

    #[test]
    fn test_left_join_matched_not_re_emitted() {
        let config = make_left_config();
        let mut state = IntervalJoinState::new();

        let left = left_batch(&["A"], &[100], &[10.0]);
        let right = right_batch(&["A"], &[110], &[1.0]);
        let result =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1); // Matched

        // Advance watermark past deadline
        let result = execute_interval_join_cycle(&mut state, &[], &[], &config, 0, 300).unwrap();
        // Should NOT re-emit as unmatched (probe finds match in right state)
        assert!(result.is_empty());
    }

    #[test]
    fn test_right_join_unmatched_emitted() {
        let mut config = make_config();
        config.join_type = StreamJoinType::Right;
        let mut state = IntervalJoinState::new();

        // Right at ts=100 (unmatched key), left at ts=100 (different key, for schema)
        let left = left_batch(&["B"], &[100], &[99.0]);
        let right = right_batch(&["A"], &[100], &[1.0]);
        let result =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();
        assert!(result.is_empty()); // No match (different keys)

        // Advance left watermark → right cutoff = 300 - 100 = 200
        let result = execute_interval_join_cycle(&mut state, &[], &[], &config, 300, 0).unwrap();
        // Right A@100 unmatched → emitted with NULL left columns
        let total_rows: usize = result.iter().map(RecordBatch::num_rows).sum();
        assert!(total_rows >= 1);
        // First batch should have NULL left columns
        assert!(result[0].column(0).is_null(0)); // left id null
    }

    #[test]
    fn test_full_join_unmatched_both_sides() {
        let mut config = make_config();
        config.join_type = StreamJoinType::Full;
        let mut state = IntervalJoinState::new();

        // Left at ts=100, Right at ts=500 (outside bound, no match)
        let left = left_batch(&["A"], &[100], &[10.0]);
        let right = right_batch(&["A"], &[500], &[1.0]);
        let _ = execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();

        // Advance both watermarks past both deadlines
        let result = execute_interval_join_cycle(&mut state, &[], &[], &config, 700, 700).unwrap();
        // Both unmatched: one from left, one from right
        let total_rows: usize = result.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_semi_join_dedup() {
        let mut config = make_config();
        config.join_type = StreamJoinType::LeftSemi;
        let mut state = IntervalJoinState::new();

        // Left A@100, two right matches: A@110, A@120
        let left = left_batch(&["A"], &[100], &[10.0]);
        let right = right_batch(&["A", "A"], &[110, 120], &[1.0, 2.0]);
        let result =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1); // Only one output per left row
        assert_eq!(result[0].num_columns(), 3); // Left columns only
    }

    #[test]
    fn test_anti_join_unmatched_only() {
        let mut config = make_config();
        config.join_type = StreamJoinType::LeftAnti;
        let mut state = IntervalJoinState::new();

        // A@100 has match, B@200 has no match
        let left = left_batch(&["A", "B"], &[100, 200], &[10.0, 20.0]);
        let right = right_batch(&["A"], &[110], &[1.0]);
        let result =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();
        // Anti emits nothing during matching — only at eviction
        assert!(result.is_empty());

        // Advance watermark past deadline for both
        let result = execute_interval_join_cycle(&mut state, &[], &[], &config, 0, 400).unwrap();
        // B@200 is unmatched → emitted. A@100 has match → not emitted.
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
        assert_eq!(result[0].num_columns(), 3); // Left columns only
    }

    #[test]
    fn test_cdc_delete_removes_from_state() {
        let config = make_config();
        let mut state = IntervalJoinState::new();

        // Insert left row
        let left = left_batch(&["A"], &[100], &[10.0]);
        let _ = execute_interval_join_cycle(&mut state, &[left], &[], &config, 0, 0).unwrap();
        assert_eq!(state.left.row_count, 1);

        // Send CDC delete for same key+ts
        let del_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
            Field::new("_op", DataType::Utf8, false),
        ]));
        let del_batch = RecordBatch::try_new(
            del_schema,
            vec![
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Float64Array::from(vec![10.0])),
                Arc::new(StringArray::from(vec!["D"])),
            ],
        )
        .unwrap();
        let _ = execute_interval_join_cycle(&mut state, &[del_batch], &[], &config, 0, 0).unwrap();
        assert_eq!(state.left.row_count, 0); // Deleted
    }
}
