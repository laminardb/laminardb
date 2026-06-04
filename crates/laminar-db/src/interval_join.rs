#![deny(clippy::disallowed_types)]

//! Stream-stream interval join execution.
//!
//! Implements a stateful interval join that buffers rows from both sides
//! across execution cycles, matching any pair where
//! `|left_ts - right_ts| <= time_bound_ms`. Expired rows are evicted
//! when the watermark advances beyond `time_bound_ms`.
//!
//! The join state is checkpointed via Arrow IPC serialization.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::compute::concat_batches;
use arrow::datatypes::{Field, Schema, SchemaRef};
use rustc_hash::{FxHashMap, FxHashSet};

use laminar_sql::translator::{StreamJoinConfig, StreamJoinType};

use crate::aggregate_state::JoinStateCheckpoint;
use crate::error::DbError;
use crate::key_column::{extract_column_as_timestamps, extract_key_column, KeyColumn};

/// Compact when accumulated batch count exceeds this threshold.
const COMPACTION_THRESHOLD: usize = 32;

/// Flush partial output once the match buffer reaches this many pairs.
/// Bounds memory on cross-product shapes.
const EMIT_THRESHOLD: usize = 65_536;

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
    pub(crate) fn add_batch(
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

    /// Remove entries matching `(key_hash, timestamp)` with verified key equality.
    pub(crate) fn remove_by_key_ts(
        &mut self,
        key_hash: u64,
        ts: i64,
        delete_key: &KeyColumn<'_>,
        delete_row: usize,
        key_col_name: &str,
    ) -> Result<(), DbError> {
        let Some(btree) = self.index.get_mut(&key_hash) else {
            return Ok(());
        };
        let Some(entries) = btree.get_mut(&ts) else {
            return Ok(());
        };

        // `Vec::retain` can't surface errors, so do it by hand.
        let mut kept: Vec<(usize, usize)> = Vec::with_capacity(entries.len());
        for &(batch_idx, row_idx) in entries.iter() {
            let stored_key = extract_key_column(&self.batches[batch_idx], key_col_name)?;
            if !delete_key.keys_equal(delete_row, &stored_key, row_idx) {
                kept.push((batch_idx, row_idx));
            }
        }
        let removed = entries.len() - kept.len();
        *entries = kept;
        self.row_count = self.row_count.saturating_sub(removed);
        if entries.is_empty() {
            btree.remove(&ts);
        }
        if btree.is_empty() {
            self.index.remove(&key_hash);
        }
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

        // One `take` per source batch over the contiguous run of live rows.
        let mut taken: Vec<RecordBatch> = Vec::new();
        let mut i = 0;
        while i < live_rows.len() {
            let batch_idx = live_rows[i].0;
            let mut j = i + 1;
            while j < live_rows.len() && live_rows[j].0 == batch_idx {
                j += 1;
            }
            #[allow(clippy::cast_possible_truncation)]
            let indices = arrow::array::UInt32Array::from_iter_values(
                live_rows[i..j].iter().map(|&(_, row)| row as u32),
            );
            let src = &self.batches[batch_idx];
            let cols: Result<Vec<ArrayRef>, _> = src
                .columns()
                .iter()
                .map(|c| arrow::compute::take(c.as_ref(), &indices, None))
                .collect();
            let cols = cols
                .map_err(|e| DbError::query_pipeline_arrow("interval join (compact take)", &e))?;
            taken.push(
                RecordBatch::try_new(src.schema(), cols).map_err(|e| {
                    DbError::query_pipeline_arrow("interval join (compact build)", &e)
                })?,
            );
            i = j;
        }

        let schema = self.batches[0].schema();
        let compacted = concat_batches(&schema, &taken)
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

/// Complete interval join state for one query.
pub(crate) struct IntervalJoinState {
    pub(crate) left: SideState,
    pub(crate) right: SideState,
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

/// Build the merged output schema from left and right schemas.
/// The joined output schema: left fields, then right fields suffixed with
/// `_{right_table}`. Shared with the processing-time join so both produce the
/// column names the residual projection (built by `build_stream_join_projection_sql`)
/// expects.
pub(crate) fn build_output_schema(
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

/// Drain `match_pairs` into one output batch via `arrow::compute::interleave` —
/// one call per column, no per-row slice + concat.
fn flush_match_pairs(
    match_pairs: &mut Vec<(usize, usize, usize, usize)>,
    output_schema: &SchemaRef,
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    left_only: bool,
    out: &mut Vec<RecordBatch>,
) -> Result<(), DbError> {
    if match_pairs.is_empty() {
        return Ok(());
    }

    let left_indices: Vec<(usize, usize)> =
        match_pairs.iter().map(|&(b, r, _, _)| (b, r)).collect();
    let right_indices: Vec<(usize, usize)> = if left_only {
        Vec::new()
    } else {
        match_pairs.iter().map(|&(_, _, b, r)| (b, r)).collect()
    };

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());

    if let Some(first) = left_batches.first() {
        for col_idx in 0..first.num_columns() {
            let arrays: Vec<&dyn Array> = left_batches
                .iter()
                .map(|b| b.column(col_idx).as_ref())
                .collect();
            let arr = arrow::compute::interleave(&arrays, &left_indices).map_err(|e| {
                DbError::query_pipeline_arrow("interval join (interleave left)", &e)
            })?;
            columns.push(arr);
        }
    }

    if !left_only {
        if let Some(first) = right_batches.first() {
            for col_idx in 0..first.num_columns() {
                let arrays: Vec<&dyn Array> = right_batches
                    .iter()
                    .map(|b| b.column(col_idx).as_ref())
                    .collect();
                let arr = arrow::compute::interleave(&arrays, &right_indices).map_err(|e| {
                    DbError::query_pipeline_arrow("interval join (interleave right)", &e)
                })?;
                columns.push(arr);
            }
        }
    }

    let batch = RecordBatch::try_new(output_schema.clone(), columns)
        .map_err(|e| DbError::query_pipeline_arrow("interval join (result)", &e))?;
    if batch.num_rows() > 0 {
        out.push(batch);
    }
    match_pairs.clear();
    Ok(())
}

/// Execute one cycle of an interval join.
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

    let left_pos: Vec<RecordBatch> = left_batches
        .iter()
        .map(crate::changelog_filter::filter_positive_events)
        .collect::<Result<Vec<_>, _>>()?;
    let right_pos: Vec<RecordBatch> = right_batches
        .iter()
        .map(crate::changelog_filter::filter_positive_events)
        .collect::<Result<Vec<_>, _>>()?;

    for raw_batch in left_batches {
        if let Some(neg) = crate::changelog_filter::extract_negative_events(raw_batch)? {
            let keys = extract_key_column(&neg, &config.left_key)?;
            let timestamps = extract_column_as_timestamps(&neg, &config.left_time_column)?;
            for (i, &ts) in timestamps.iter().enumerate() {
                if let Some(kh) = keys.hash_at(i) {
                    state
                        .left
                        .remove_by_key_ts(kh, ts, &keys, i, &config.left_key)?;
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
                    state
                        .right
                        .remove_by_key_ts(kh, ts, &keys, i, &config.right_key)?;
                }
            }
        }
    }

    // A pure-delete CDC batch yields zero positive rows; treat it as None
    // so new_*_batch_idx never points past the end of state.
    let concat_nonempty =
        |slices: &[RecordBatch], side: &str| -> Result<Option<RecordBatch>, DbError> {
            if slices.is_empty() {
                return Ok(None);
            }
            let schema = slices[0].schema();
            let b = concat_batches(&schema, slices)
                .map_err(|e| DbError::query_pipeline_arrow(side, &e))?;
            Ok((b.num_rows() > 0).then_some(b))
        };
    let new_left = concat_nonempty(&left_pos, "interval join (left concat)")?;
    let new_right = concat_nonempty(&right_pos, "interval join (right concat)")?;

    // Buffer before probing so every (batch_idx, row_idx) we produce already
    // points into state.batches — lets flush_match_pairs run mid-probe
    // without juggling in-flight batch references.
    let left_old_count = state.left.batches.len();
    let right_old_count = state.right.batches.len();
    if let Some(ref rb) = new_right {
        state
            .right
            .add_batch(rb, &config.right_key, &config.right_time_column)?;
    }
    if let Some(ref lb) = new_left {
        state
            .left
            .add_batch(lb, &config.left_key, &config.left_time_column)?;
    }
    let new_left_batch_idx = left_old_count;
    let new_right_batch_idx = right_old_count;

    let left_only = matches!(
        config.join_type,
        StreamJoinType::LeftSemi | StreamJoinType::LeftAnti
    );

    // Refresh the output schema once both sides have been seen.
    {
        let left_schema = state.left.batches.first().map(RecordBatch::schema);
        let right_schema = state.right.batches.first().map(RecordBatch::schema);
        match (left_schema, right_schema) {
            (Some(ls), Some(rs)) => {
                state.output_schema = Some(build_output_schema(&ls, &rs, config));
            }
            (Some(ls), None) if state.output_schema.is_none() && left_only => {
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

    let is_semi = config.join_type == StreamJoinType::LeftSemi;
    let is_anti = config.join_type == StreamJoinType::LeftAnti;
    let mut result: Vec<RecordBatch> = Vec::new();

    // Borrow batches immutably for the probe; eviction below needs &mut so
    // the cached key columns must drop first.
    {
        // One KeyColumn per buffered batch — saves a schema lookup +
        // downcast per candidate inside the inner probe loops.
        let left_key_cols: Vec<KeyColumn<'_>> = state
            .left
            .batches
            .iter()
            .map(|b| extract_key_column(b, &config.left_key))
            .collect::<Result<_, _>>()?;
        let right_key_cols: Vec<KeyColumn<'_>> = state
            .right
            .batches
            .iter()
            .map(|b| extract_key_column(b, &config.right_key))
            .collect::<Result<_, _>>()?;

        // Anti emits only at eviction; don't bother accumulating pairs we'll discard.
        let collect_pairs = !is_anti;
        let mut match_pairs: Vec<(usize, usize, usize, usize)> = Vec::new();
        let mut semi_matched: FxHashSet<usize> = FxHashSet::default();

        let flush = |pairs: &mut Vec<_>, result: &mut Vec<RecordBatch>| {
            if pairs.is_empty() {
                return Ok(());
            }
            let schema = state.output_schema.as_ref().ok_or_else(|| {
                DbError::Pipeline("interval join: output schema not available".to_string())
            })?;
            flush_match_pairs(
                pairs,
                schema,
                &state.left.batches,
                &state.right.batches,
                left_only,
                result,
            )
        };

        // Step 1: probe new left rows against all right (old + new).
        if new_left.is_some() {
            let lb_kc = &left_key_cols[new_left_batch_idx];
            let lb_ts = extract_column_as_timestamps(
                &state.left.batches[new_left_batch_idx],
                &config.left_time_column,
            )?;
            for (row_idx, &left_ts) in lb_ts.iter().enumerate() {
                if is_semi && semi_matched.contains(&row_idx) {
                    continue;
                }
                let Some(key_hash) = lb_kc.hash_at(row_idx) else {
                    continue;
                };
                for (r_batch, r_row) in probe_index(&state.right.index, key_hash, left_ts, bound_ms)
                {
                    if is_semi && semi_matched.contains(&row_idx) {
                        break;
                    }
                    if !lb_kc.keys_equal(row_idx, &right_key_cols[r_batch], r_row) {
                        continue;
                    }
                    if collect_pairs {
                        match_pairs.push((new_left_batch_idx, row_idx, r_batch, r_row));
                        if match_pairs.len() >= EMIT_THRESHOLD {
                            flush(&mut match_pairs, &mut result)?;
                        }
                    }
                    if is_semi {
                        semi_matched.insert(row_idx);
                    }
                }
            }
        }

        // Step 2: probe new right rows against OLD left rows only — new_left ×
        // new_right pairs were already produced in step 1.
        if new_right.is_some() {
            let rb_kc = &right_key_cols[new_right_batch_idx];
            let rb_ts = extract_column_as_timestamps(
                &state.right.batches[new_right_batch_idx],
                &config.right_time_column,
            )?;
            for (row_idx, &right_ts) in rb_ts.iter().enumerate() {
                let Some(key_hash) = rb_kc.hash_at(row_idx) else {
                    continue;
                };
                for (l_batch, l_row) in probe_index(&state.left.index, key_hash, right_ts, bound_ms)
                {
                    if l_batch >= left_old_count {
                        continue;
                    }
                    if !rb_kc.keys_equal(row_idx, &left_key_cols[l_batch], l_row) {
                        continue;
                    }
                    if collect_pairs {
                        match_pairs.push((l_batch, l_row, new_right_batch_idx, row_idx));
                        if match_pairs.len() >= EMIT_THRESHOLD {
                            flush(&mut match_pairs, &mut result)?;
                        }
                    }
                }
            }
        }

        flush(&mut match_pairs, &mut result)?;

        // Step 3: emit unmatched rows about to be evicted (LEFT/RIGHT/FULL/ANTI).
        // Runs before eviction so the opposite side's state is still complete.
        if matches!(
            config.join_type,
            StreamJoinType::Left | StreamJoinType::Full | StreamJoinType::LeftAnti
        ) {
            let left_cutoff = right_watermark.saturating_sub(bound_ms);
            if left_cutoff > state.left_evicted_cutoff && !state.left.batches.is_empty() {
                emit_unmatched_left_rows(
                    state,
                    &left_key_cols,
                    &right_key_cols,
                    config,
                    left_cutoff,
                    bound_ms,
                    &mut result,
                )?;
            }
        }
        if matches!(
            config.join_type,
            StreamJoinType::Right | StreamJoinType::Full
        ) {
            let right_cutoff = left_watermark.saturating_sub(bound_ms);
            if right_cutoff > state.right_evicted_cutoff && !state.right.batches.is_empty() {
                emit_unmatched_right_rows(
                    state,
                    &left_key_cols,
                    &right_key_cols,
                    right_cutoff,
                    bound_ms,
                    &mut result,
                )?;
            }
        }
    }

    // Step 4: evict. A left row at ts can only still match right rows with
    // ts in [left_ts - bound, left_ts + bound], so once the right watermark
    // passes left_ts + bound it's safe to drop. Symmetric for right.
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

/// LEFT/ANTI: emit pre-eviction left rows with no match in current right state.
/// Output is chunked at `EMIT_THRESHOLD` rows.
fn emit_unmatched_left_rows(
    state: &IntervalJoinState,
    left_key_cols: &[KeyColumn<'_>],
    right_key_cols: &[KeyColumn<'_>],
    config: &StreamJoinConfig,
    left_cutoff: i64,
    bound_ms: i64,
    out: &mut Vec<RecordBatch>,
) -> Result<(), DbError> {
    let Some(output_schema) = state.output_schema.as_ref() else {
        return Ok(());
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
                let left_key = &left_key_cols[batch_idx];
                let has_match = candidates.iter().any(|&(rb, rr)| {
                    let rk = &right_key_cols[rb];
                    left_key.keys_equal(row_idx, rk, rr)
                });
                if !has_match {
                    unmatched_left.push((batch_idx, row_idx));
                }
            }
        }
    }

    if unmatched_left.is_empty() {
        return Ok(());
    }

    let left_field_count = state
        .left
        .batches
        .first()
        .map_or(0, RecordBatch::num_columns);
    let right_field_count = if left_only {
        0
    } else {
        state
            .right
            .batches
            .first()
            .map_or(0, RecordBatch::num_columns)
    };

    for chunk in unmatched_left.chunks(EMIT_THRESHOLD) {
        let num_rows = chunk.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());

        for col_idx in 0..left_field_count {
            let arrays: Vec<&dyn Array> = state
                .left
                .batches
                .iter()
                .map(|b| b.column(col_idx).as_ref())
                .collect();
            let arr = arrow::compute::interleave(&arrays, chunk).map_err(|e| {
                DbError::query_pipeline_arrow("interval join (unmatched left interleave)", &e)
            })?;
            columns.push(arr);
        }

        for col_idx in 0..right_field_count {
            let dt = output_schema.field(left_field_count + col_idx).data_type();
            columns.push(arrow::array::new_null_array(dt, num_rows));
        }

        let batch = RecordBatch::try_new(output_schema.clone(), columns).map_err(|e| {
            DbError::query_pipeline_arrow("interval join (unmatched left result)", &e)
        })?;
        if batch.num_rows() > 0 {
            out.push(batch);
        }
    }
    Ok(())
}

/// RIGHT/FULL: symmetric to `emit_unmatched_left_rows`.
fn emit_unmatched_right_rows(
    state: &IntervalJoinState,
    left_key_cols: &[KeyColumn<'_>],
    right_key_cols: &[KeyColumn<'_>],
    right_cutoff: i64,
    bound_ms: i64,
    out: &mut Vec<RecordBatch>,
) -> Result<(), DbError> {
    let Some(output_schema) = state.output_schema.as_ref() else {
        return Ok(());
    };

    let mut unmatched_right: Vec<(usize, usize)> = Vec::new();

    for (&key_hash, btree) in &state.right.index {
        for (&ts, entries) in btree.range(..right_cutoff) {
            for &(batch_idx, row_idx) in entries {
                let candidates = probe_index(&state.left.index, key_hash, ts, bound_ms);
                let right_key = &right_key_cols[batch_idx];
                let has_match = candidates.iter().any(|&(lb, lr)| {
                    let lk = &left_key_cols[lb];
                    right_key.keys_equal(row_idx, lk, lr)
                });
                if !has_match {
                    unmatched_right.push((batch_idx, row_idx));
                }
            }
        }
    }

    if unmatched_right.is_empty() {
        return Ok(());
    }

    let left_field_count = state
        .left
        .batches
        .first()
        .map_or(0, RecordBatch::num_columns);
    let right_field_count = state
        .right
        .batches
        .first()
        .map_or(0, RecordBatch::num_columns);

    for chunk in unmatched_right.chunks(EMIT_THRESHOLD) {
        let num_rows = chunk.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());

        for col_idx in 0..left_field_count {
            let dt = output_schema.field(col_idx).data_type();
            columns.push(arrow::array::new_null_array(dt, num_rows));
        }

        for col_idx in 0..right_field_count {
            let arrays: Vec<&dyn Array> = state
                .right
                .batches
                .iter()
                .map(|b| b.column(col_idx).as_ref())
                .collect();
            let arr = arrow::compute::interleave(&arrays, chunk).map_err(|e| {
                DbError::query_pipeline_arrow("interval join (unmatched right interleave)", &e)
            })?;
            columns.push(arr);
        }

        let batch = RecordBatch::try_new(output_schema.clone(), columns).map_err(|e| {
            DbError::query_pipeline_arrow("interval join (unmatched right result)", &e)
        })?;
        if batch.num_rows() > 0 {
            out.push(batch);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, TimeUnit};
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

    /// Regression: equi-join on a `Timestamp(_)` key column must not
    /// abort the cycle. Pre-fix the operator rejected the key with
    /// "Unsupported key column type ... Timestamp(ms)" and every cycle
    /// dropped its output.
    #[test]
    fn timestamp_key_does_not_abort_cycle() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "window_start",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Float64, false),
        ]));
        let mk = |w: &[i64], v: &[f64]| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(w.to_vec())),
                    Arc::new(Int64Array::from(w.to_vec())),
                    Arc::new(Float64Array::from(v.to_vec())),
                ],
            )
            .unwrap()
        };
        let config = StreamJoinConfig {
            left_key: "window_start".into(),
            right_key: "window_start".into(),
            left_time_column: "ts".into(),
            right_time_column: "ts".into(),
            left_table: "l".into(),
            right_table: "r".into(),
            time_bound: Duration::from_millis(1000),
            join_type: StreamJoinType::Inner,
        };
        let mut state = IntervalJoinState::new();
        let left = mk(&[1_714_478_400_000, 1_714_478_401_000], &[10.0, 20.0]);
        let right = mk(&[1_714_478_400_000, 1_714_478_401_000], &[1.0, 2.0]);
        let result =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
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
    fn test_remove_by_key_ts_propagates_extraction_error() {
        // Buffer one row, then call remove_by_key_ts with a key column name
        // that does not exist on the buffered batch. Pre-fix this returned
        // Ok(()) silently because `map_or(true, ...)` retained the row on
        // extraction failure, leaving phantom state.
        let mut side = SideState::new();
        let batch = left_batch(&["A"], &[100], &[1.0]);
        side.add_batch(&batch, "id", "ts").unwrap();

        // Probe key extracted from a "delete" batch that happens to have an `id` column.
        let del = left_batch(&["A"], &[100], &[1.0]);
        let del_keys = extract_key_column(&del, "id").unwrap();
        let kh = del_keys.hash_at(0).unwrap();

        // Buffered batch has no column called "missing" — extraction must fail.
        let err = side.remove_by_key_ts(kh, 100, &del_keys, 0, "missing");
        assert!(err.is_err(), "extraction failure must propagate");
    }

    #[test]
    #[allow(clippy::cast_possible_wrap, clippy::cast_precision_loss)]
    fn test_match_pairs_bounded_partial_emit_on_cross_product() {
        // Adversarial shape: every left × every right matches (single key,
        // wide bound, all timestamps within tolerance). Must emit multiple
        // batches each ≤ EMIT_THRESHOLD rows, never accumulate all M·N pairs.
        let config = StreamJoinConfig {
            left_key: "id".to_string(),
            right_key: "id".to_string(),
            left_time_column: "ts".to_string(),
            right_time_column: "ts".to_string(),
            left_table: "left_stream".to_string(),
            right_table: "right_stream".to_string(),
            time_bound: Duration::from_millis(1_000_000),
            join_type: StreamJoinType::Inner,
        };
        let mut state = IntervalJoinState::new();

        // 300 × 300 = 90,000 pairs > 65,536 threshold → at least 2 output batches.
        let m = 300usize;
        let ids_l: Vec<&str> = (0..m).map(|_| "K").collect();
        let ts_l: Vec<i64> = (0..m).map(|i| i as i64).collect();
        let v_l: Vec<f64> = (0..m).map(|i| i as f64).collect();
        let left = left_batch(&ids_l, &ts_l, &v_l);

        let ids_r: Vec<&str> = (0..m).map(|_| "K").collect();
        let ts_r: Vec<i64> = (0..m).map(|i| i as i64).collect();
        let v_r: Vec<f64> = (0..m).map(|i| i as f64).collect();
        let right = right_batch(&ids_r, &ts_r, &v_r);

        let result =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();

        let total: usize = result.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, m * m, "every pair must appear exactly once");
        assert!(
            result.len() >= 2,
            "expected partial emits across multiple batches, got {}",
            result.len()
        );
        for b in &result {
            assert!(
                b.num_rows() <= EMIT_THRESHOLD,
                "partial batch exceeded EMIT_THRESHOLD: {}",
                b.num_rows()
            );
        }
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
