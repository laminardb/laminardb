#![deny(clippy::disallowed_types)]

//! Stream-stream interval join: buffers both sides and matches pairs where
//! `|left_ts - right_ts| <= time_bound_ms`. Evicts expired rows on watermark advance.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{Field, Schema, SchemaRef};
use rustc_hash::FxHashMap;

use laminar_sql::translator::{StreamJoinConfig, StreamJoinType};

use crate::aggregate_state::JoinStateCheckpoint;
use crate::error::DbError;
use crate::key_column::{extract_column_as_timestamps, extract_key_column, KeyColumn};

const COMPACTION_THRESHOLD: usize = 32;

/// Caps memory on cross-product shapes.
const EMIT_THRESHOLD: usize = 65_536;

type SideIndex = FxHashMap<u64, BTreeMap<i64, Vec<(usize, usize)>>>;

pub(crate) struct SideState {
    batches: Vec<RecordBatch>,
    index: SideIndex,
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

    pub(crate) fn add_batch(
        &mut self,
        batch: &RecordBatch,
        key_col_name: &str,
        time_col_name: &str,
    ) -> Result<bool, DbError> {
        if let Some(retained) = self.batches.first() {
            if retained.schema().as_ref() != batch.schema().as_ref() {
                return Err(DbError::SchemaMismatch(
                    "interval join side schema changed while rows were retained".to_string(),
                ));
            }
        }
        if batch.num_rows() == 0 {
            return Ok(false);
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
            // null keys never match (SQL three-valued logic)
        }
        if indexed_rows == 0 {
            return Ok(false);
        }
        self.row_count += indexed_rows;
        self.batches.push(batch.clone());
        Ok(true)
    }

    fn evict_before(&mut self, cutoff: i64, key_col: &str, time_col: &str) -> Result<(), DbError> {
        for btree in self.index.values_mut() {
            let keep = btree.split_off(&cutoff);
            for entries in btree.values() {
                self.row_count = self.row_count.saturating_sub(entries.len());
            }
            *btree = keep;
        }
        self.index.retain(|_, btree| !btree.is_empty());

        if self.row_count == 0 {
            self.batches.clear();
            return Ok(());
        }

        if self.batches.len() > COMPACTION_THRESHOLD {
            self.compact(key_col, time_col)?;
        }
        Ok(())
    }

    fn compact(&mut self, key_col: &str, time_col: &str) -> Result<(), DbError> {
        let mut live_rows: Vec<(usize, usize)> = Vec::with_capacity(self.row_count);
        for btree in self.index.values() {
            for entries in btree.values() {
                live_rows.extend_from_slice(entries);
            }
        }

        if live_rows.is_empty() {
            self.batches.clear();
            self.index.clear();
            self.row_count = 0;
            return Ok(());
        }

        live_rows.sort_unstable();

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
        let (replacement_index, replacement_rows) = {
            let keys = extract_key_column(&compacted, key_col)?;
            let timestamps = extract_column_as_timestamps(&compacted, time_col)?;
            let mut index = FxHashMap::default();
            let mut rows = 0usize;
            for (row_idx, &ts) in timestamps.iter().enumerate() {
                if let Some(key_hash) = keys.hash_at(row_idx) {
                    index
                        .entry(key_hash)
                        .or_insert_with(BTreeMap::new)
                        .entry(ts)
                        .or_insert_with(Vec::new)
                        .push((0, row_idx));
                    rows += 1;
                }
            }
            (index, rows)
        };
        if replacement_rows != live_rows.len() {
            return Err(DbError::Pipeline(format!(
                "interval join compaction lost indexed rows: expected {}, rebuilt {replacement_rows}",
                live_rows.len()
            )));
        }

        self.batches = vec![compacted];
        self.index = replacement_index;
        self.row_count = replacement_rows;
        Ok(())
    }
}

/// Per-query interval join state.
pub(crate) struct IntervalJoinState {
    pub(crate) left: SideState,
    pub(crate) right: SideState,
    left_evicted_cutoff: i64,
    right_evicted_cutoff: i64,
    output_schema: Option<SchemaRef>,
}

impl IntervalJoinState {
    pub(crate) fn new() -> Self {
        Self {
            left: SideState::new(),
            right: SideState::new(),
            left_evicted_cutoff: i64::MIN,
            right_evicted_cutoff: i64::MIN,
            output_schema: None,
        }
    }

    /// Compacts both sides before serialization to avoid checkpointing dead rows.
    pub(crate) fn snapshot_checkpoint(
        &mut self,
        left_key: &str,
        left_time: &str,
        right_key: &str,
        right_time: &str,
    ) -> Result<JoinStateCheckpoint, DbError> {
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

    /// Restores from a checkpoint, rebuilding the index from deserialized batches.
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
                    DbError::Checkpoint(format!("interval join left batch deserialization: {e}"))
                })?;
            let _ = state
                .left
                .add_batch(&batch, left_key_col, left_time_col)
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "interval join left checkpoint index rebuild: {error}"
                    ))
                })?;
        }

        for ipc_bytes in &cp.right_batches {
            let batch =
                laminar_core::serialization::deserialize_batch_stream(ipc_bytes).map_err(|e| {
                    DbError::Checkpoint(format!("interval join right batch deserialization: {e}"))
                })?;
            let _ = state
                .right
                .add_batch(&batch, right_key_col, right_time_col)
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "interval join right checkpoint index rebuild: {error}"
                    ))
                })?;
        }

        let left_rows = u64::try_from(state.left.row_count).map_err(|_| {
            DbError::Checkpoint("interval join left row count does not fit u64".to_string())
        })?;
        let right_rows = u64::try_from(state.right.row_count).map_err(|_| {
            DbError::Checkpoint("interval join right row count does not fit u64".to_string())
        })?;
        if left_rows != cp.left_buffer_rows || right_rows != cp.right_buffer_rows {
            return Err(DbError::Checkpoint(format!(
                "interval join checkpoint row-count mismatch: metadata=({}, {}), decoded=({left_rows}, {right_rows})",
                cp.left_buffer_rows, cp.right_buffer_rows
            )));
        }

        Ok(state)
    }
}

/// Left fields then right fields suffixed with `_{right_table}`.
pub(crate) fn build_output_schema(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    config: &StreamJoinConfig,
) -> SchemaRef {
    let mut fields: Vec<Field> = left_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();

    for field in right_schema.fields() {
        let f = field.as_ref().clone();
        let suffixed = format!("{}_{}", f.name(), config.right_table);
        fields.push(f.with_name(suffixed));
    }

    Arc::new(Schema::new(fields))
}

/// All `(batch_idx, row_idx)` where `|probe_ts - candidate_ts| <= bound_ms`.
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

fn flush_match_pairs(
    match_pairs: &mut Vec<(usize, usize, usize, usize)>,
    output_schema: &SchemaRef,
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    out: &mut Vec<RecordBatch>,
) -> Result<(), DbError> {
    if match_pairs.is_empty() {
        return Ok(());
    }

    let left_indices: Vec<(usize, usize)> =
        match_pairs.iter().map(|&(b, r, _, _)| (b, r)).collect();
    let right_indices: Vec<(usize, usize)> =
        match_pairs.iter().map(|&(_, _, b, r)| (b, r)).collect();

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

    let batch = RecordBatch::try_new(output_schema.clone(), columns)
        .map_err(|e| DbError::query_pipeline_arrow("interval join (result)", &e))?;
    if batch.num_rows() > 0 {
        out.push(batch);
    }
    match_pairs.clear();
    Ok(())
}

fn validate_append_only_input(
    side: &str,
    batches: &[RecordBatch],
    key_column: &str,
    time_column: &str,
    closed_cutoff: i64,
) -> Result<(), DbError> {
    for batch in batches {
        if let Ok(index) = batch.schema().index_of("_op") {
            let operations = batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DbError::Pipeline(format!(
                        "interval join ({side}): _op must be Utf8 for append-only validation"
                    ))
                })?;
            if let Some((row, operation)) = operations
                .iter()
                .enumerate()
                .find(|(_, operation)| *operation != Some("I"))
            {
                return Err(DbError::InvalidOperation(format!(
                    "interval join ({side}) accepts append-only input; row {row} has _op {}",
                    operation.unwrap_or("NULL")
                )));
            }
        }

        if let Ok(index) = batch
            .schema()
            .index_of(laminar_core::changelog::WEIGHT_COLUMN)
        {
            let weights = batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DbError::Pipeline(format!(
                        "interval join ({side}): {} must be Int64 for append-only validation",
                        laminar_core::changelog::WEIGHT_COLUMN
                    ))
                })?;
            if let Some((row, weight)) = weights
                .iter()
                .enumerate()
                .find(|(_, weight)| *weight != Some(1))
            {
                return Err(DbError::InvalidOperation(format!(
                    "interval join ({side}) accepts only +1 weights; row {row} has weight {}",
                    weight.map_or_else(|| "NULL".to_string(), |value| value.to_string())
                )));
            }
        }

        // Preflight key/time extraction for every batch before either side mutates state.
        let _ = extract_key_column(batch, key_column)?;
        let timestamps = extract_column_as_timestamps(batch, time_column)?;
        if let Some((row, timestamp)) = timestamps
            .iter()
            .copied()
            .enumerate()
            .find(|(_, timestamp)| *timestamp < closed_cutoff)
        {
            return Err(DbError::InvalidOperation(format!(
                "interval join ({side}) received late row {row} at {timestamp} below closed cutoff {closed_cutoff}"
            )));
        }
    }
    Ok(())
}

fn validate_input_schemas(
    side: &str,
    retained: &SideState,
    batches: &[RecordBatch],
) -> Result<Option<SchemaRef>, DbError> {
    let expected = retained
        .batches
        .first()
        .map(RecordBatch::schema)
        .or_else(|| batches.first().map(RecordBatch::schema));
    let Some(expected) = expected else {
        return Ok(None);
    };

    for (batch_index, batch) in batches.iter().enumerate() {
        if batch.schema().as_ref() != expected.as_ref() {
            return Err(DbError::SchemaMismatch(format!(
                "interval join {side} batch {batch_index} does not match the retained side schema"
            )));
        }
    }
    Ok(Some(expected))
}

fn partial_apply(error: DbError) -> DbError {
    if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
        error
    } else {
        DbError::StatefulOperatorPartialApply(format!(
            "interval join admitted input before the cycle failed: {error}"
        ))
    }
}

/// One cycle: new left rows probe all right; new right rows probe only old left (avoids double-emit).
#[allow(clippy::too_many_lines)]
pub(crate) fn execute_interval_join_cycle(
    state: &mut IntervalJoinState,
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    config: &StreamJoinConfig,
    left_watermark: i64,
    right_watermark: i64,
) -> Result<Vec<RecordBatch>, DbError> {
    if config.join_type != StreamJoinType::Inner {
        return Err(DbError::InvalidOperation(format!(
            "interval join supports only INNER joins; {} requires durable per-row matched metadata",
            config.join_type
        )));
    }
    let bound_ms = i64::try_from(config.time_bound.as_millis()).map_err(|_| {
        DbError::InvalidOperation(
            "interval join time bound exceeds the supported millisecond range".to_string(),
        )
    })?;
    if bound_ms == 0 {
        return Err(DbError::InvalidOperation(
            "interval join requires a positive finite time bound".to_string(),
        ));
    }

    let left_schema = validate_input_schemas("left", &state.left, left_batches)?;
    let right_schema = validate_input_schemas("right", &state.right, right_batches)?;

    validate_append_only_input(
        "left",
        left_batches,
        &config.left_key,
        &config.left_time_column,
        state.left_evicted_cutoff,
    )?;
    validate_append_only_input(
        "right",
        right_batches,
        &config.right_key,
        &config.right_time_column,
        state.right_evicted_cutoff,
    )?;

    let concat_nonempty =
        |slices: &[RecordBatch], side: &str| -> Result<Option<RecordBatch>, DbError> {
            if slices.is_empty() {
                return Ok(None);
            }
            let schema = slices[0].schema();
            let batch = concat_batches(&schema, slices)
                .map_err(|e| DbError::query_pipeline_arrow(side, &e))?;
            Ok((batch.num_rows() > 0).then_some(batch))
        };
    let new_left = concat_nonempty(left_batches, "interval join (left concat)")?;
    let new_right = concat_nonempty(right_batches, "interval join (right concat)")?;

    // Buffer first so every (batch_idx, row_idx) already points into state.batches,
    // letting flush_match_pairs run mid-probe without juggling in-flight references.
    let left_old_count = state.left.batches.len();
    let right_old_count = state.right.batches.len();
    let has_new_right = if let Some(rb) = new_right {
        state
            .right
            .add_batch(&rb, &config.right_key, &config.right_time_column)?
    } else {
        false
    };
    let has_new_left = if let Some(lb) = new_left {
        match state
            .left
            .add_batch(&lb, &config.left_key, &config.left_time_column)
        {
            Ok(added) => added,
            Err(error) if has_new_right => return Err(partial_apply(error)),
            Err(error) => return Err(error),
        }
    } else {
        false
    };
    let new_left_batch_idx = left_old_count;
    let new_right_batch_idx = right_old_count;

    if let (Some(left_schema), Some(right_schema)) = (left_schema, right_schema) {
        state.output_schema = Some(build_output_schema(&left_schema, &right_schema, config));
    }

    let admitted = (|| -> Result<Vec<RecordBatch>, DbError> {
        let mut result: Vec<RecordBatch> = Vec::new();

        // One KeyColumn per buffered batch — avoids a schema lookup + downcast per candidate.
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

        let mut match_pairs: Vec<(usize, usize, usize, usize)> = Vec::new();

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
                result,
            )
        };

        // Probe new left against all right (old + new).
        if has_new_left {
            let lb_kc = &left_key_cols[new_left_batch_idx];
            let lb_ts = extract_column_as_timestamps(
                &state.left.batches[new_left_batch_idx],
                &config.left_time_column,
            )?;
            for (row_idx, &left_ts) in lb_ts.iter().enumerate() {
                let Some(key_hash) = lb_kc.hash_at(row_idx) else {
                    continue;
                };
                for (r_batch, r_row) in probe_index(&state.right.index, key_hash, left_ts, bound_ms)
                {
                    if !lb_kc.keys_equal(row_idx, &right_key_cols[r_batch], r_row) {
                        continue;
                    }
                    match_pairs.push((new_left_batch_idx, row_idx, r_batch, r_row));
                    if match_pairs.len() >= EMIT_THRESHOLD {
                        flush(&mut match_pairs, &mut result)?;
                    }
                }
            }
        }

        // Probe new right against OLD left only — new_left × new_right already covered above.
        if has_new_right {
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
                    match_pairs.push((l_batch, l_row, new_right_batch_idx, row_idx));
                    if match_pairs.len() >= EMIT_THRESHOLD {
                        flush(&mut match_pairs, &mut result)?;
                    }
                }
            }
        }

        flush(&mut match_pairs, &mut result)?;
        // A left row at ts is evictable once the right watermark passes ts + bound. Symmetric for
        // right.
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
    })();

    admitted.map_err(partial_apply)
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

    #[test]
    fn checkpoint_restore_rejects_row_count_mismatch() {
        let config = make_config();
        let mut state = IntervalJoinState::new();
        execute_interval_join_cycle(
            &mut state,
            &[left_batch(&["A"], &[100], &[1.0])],
            &[],
            &config,
            0,
            0,
        )
        .unwrap();
        let mut checkpoint = state
            .snapshot_checkpoint(
                &config.left_key,
                &config.left_time_column,
                &config.right_key,
                &config.right_time_column,
            )
            .unwrap();
        checkpoint.left_buffer_rows += 1;

        let error = IntervalJoinState::from_checkpoint(
            &checkpoint,
            &config.left_key,
            &config.left_time_column,
            &config.right_key,
            &config.right_time_column,
        )
        .err()
        .expect("corrupt row-count metadata must fail restore");
        assert!(error.to_string().contains("row-count mismatch"));
    }

    #[test]
    fn compaction_failure_leaves_original_state_intact() {
        let mut side = SideState::new();
        side.add_batch(&left_batch(&["A"], &[100], &[1.0]), "id", "ts")
            .unwrap();
        let before_index = side.index.clone();
        let before_batch = side.batches[0].clone();

        let error = side.compact("missing", "ts").unwrap_err();
        assert!(error.to_string().contains("missing"));
        assert_eq!(side.row_count, 1);
        assert_eq!(side.index, before_index);
        assert_eq!(side.batches.len(), 1);
        assert!(Arc::ptr_eq(
            side.batches[0].column(0),
            before_batch.column(0)
        ));
    }

    #[test]
    fn schema_fault_is_rejected_before_either_side_changes() {
        let config = make_config();
        let mut state = IntervalJoinState::new();
        execute_interval_join_cycle(
            &mut state,
            &[left_batch(&["seed"], &[100], &[1.0])],
            &[right_batch(&["seed"], &[100], &[1.0])],
            &config,
            0,
            0,
        )
        .unwrap();

        let checkpoint_bytes = |state: &mut IntervalJoinState| {
            let checkpoint = state
                .snapshot_checkpoint(
                    &config.left_key,
                    &config.left_time_column,
                    &config.right_key,
                    &config.right_time_column,
                )
                .unwrap();
            rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
                .unwrap()
                .to_vec()
        };
        let before = checkpoint_bytes(&mut state);

        let incompatible_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("price", DataType::Int64, false),
        ]));
        let incompatible = RecordBatch::try_new(
            incompatible_schema,
            vec![
                Arc::new(StringArray::from(vec!["new"])),
                Arc::new(Int64Array::from(vec![110])),
                Arc::new(Int64Array::from(vec![2])),
            ],
        )
        .unwrap();

        let error = execute_interval_join_cycle(
            &mut state,
            &[left_batch(&["new"], &[110], &[2.0]), incompatible],
            &[right_batch(&["new"], &[110], &[2.0])],
            &config,
            0,
            0,
        )
        .unwrap_err();
        assert!(matches!(error, DbError::SchemaMismatch(_)));
        assert_eq!(checkpoint_bytes(&mut state), before);
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
    fn all_null_keys_are_not_retained() {
        let config = make_config();
        let mut state = IntervalJoinState::new();
        let result = execute_interval_join_cycle(
            &mut state,
            &[left_batch_nullable(&[None], &[100], &[1.0])],
            &[right_batch_nullable(&[None], &[100], &[1.0])],
            &config,
            0,
            0,
        )
        .unwrap();
        assert!(result.is_empty());
        assert!(state.left.batches.is_empty());
        assert!(state.right.batches.is_empty());
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

    #[test]
    fn unsupported_join_types_fail_before_mutating_state() {
        for join_type in [
            StreamJoinType::Left,
            StreamJoinType::Right,
            StreamJoinType::Full,
            StreamJoinType::LeftSemi,
            StreamJoinType::LeftAnti,
        ] {
            let mut config = make_config();
            config.join_type = join_type;
            let mut state = IntervalJoinState::new();
            let error = execute_interval_join_cycle(
                &mut state,
                &[left_batch(&["A"], &[100], &[1.0])],
                &[right_batch(&["A"], &[100], &[1.0])],
                &config,
                0,
                0,
            )
            .unwrap_err();
            assert!(error.to_string().contains("only INNER"));
            assert_eq!(state.left.row_count, 0);
            assert_eq!(state.right.row_count, 0);
        }
    }

    #[test]
    fn retracting_cdc_fails_before_either_side_mutates() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("_op", DataType::Utf8, false),
        ]));
        let delete = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(StringArray::from(vec!["D"])),
            ],
        )
        .unwrap();
        let mut state = IntervalJoinState::new();
        let error = execute_interval_join_cycle(
            &mut state,
            &[left_batch(&["A"], &[100], &[1.0])],
            &[delete],
            &make_config(),
            0,
            0,
        )
        .unwrap_err();
        assert!(error.to_string().contains("append-only"));
        assert_eq!(state.left.row_count, 0);
        assert_eq!(state.right.row_count, 0);
        assert!(state.left.batches.is_empty());
        assert!(state.right.batches.is_empty());
    }

    #[test]
    fn negative_weight_fails_before_state_mutation() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
            Field::new(
                laminar_core::changelog::WEIGHT_COLUMN,
                DataType::Int64,
                false,
            ),
        ]));
        let retraction = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(Int64Array::from(vec![-1])),
            ],
        )
        .unwrap();
        let mut state = IntervalJoinState::new();
        let error =
            execute_interval_join_cycle(&mut state, &[retraction], &[], &make_config(), 0, 0)
                .unwrap_err();
        assert!(error.to_string().contains("only +1 weights"));
        assert_eq!(state.left.row_count, 0);
    }

    #[test]
    fn row_below_closed_cutoff_is_rejected_without_retention() {
        let config = make_config();
        let mut state = IntervalJoinState::new();
        execute_interval_join_cycle(&mut state, &[], &[], &config, 300, 300).unwrap();
        assert_eq!(state.left_evicted_cutoff, 200);

        let error = execute_interval_join_cycle(
            &mut state,
            &[left_batch(&["late"], &[199], &[1.0])],
            &[],
            &config,
            300,
            300,
        )
        .unwrap_err();
        assert!(error.to_string().contains("below closed cutoff 200"));
        assert_eq!(state.left.row_count, 0);
        assert!(state.left.batches.is_empty());
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
}
