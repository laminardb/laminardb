#![deny(clippy::disallowed_types)]

//! Batch-level ASOF join execution on `RecordBatch`es.
//!
//! Implements the ASOF join algorithm for batch data, matching each left row
//! to the closest right row by timestamp within the same key partition.

use std::collections::BTreeMap;
use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::concat_batches;
use arrow::datatypes::{Field, Schema, SchemaRef};

use laminar_sql::parser::join_parser::AsofSqlDirection;
use laminar_sql::translator::{AsofJoinTranslatorConfig, AsofSqlJoinType};

use crate::error::DbError;
use crate::key_column::{
    extract_column_as_timestamps, extract_key_column, take_with_nulls, KeyColumn,
};

/// Execute an ASOF join on two sets of `RecordBatch`es.
///
/// Matches each left row to the closest right row by timestamp, partitioned
/// by key column, according to the direction and tolerance in `config`.
///
/// # Errors
///
/// Returns `DbError::Pipeline` if schemas are invalid or column extraction fails.
pub(crate) fn execute_asof_join_batch(
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    config: &AsofJoinTranslatorConfig,
) -> Result<RecordBatch, DbError> {
    if left_batches.is_empty() {
        let schema = if right_batches.is_empty() {
            Arc::new(Schema::empty())
        } else {
            build_output_schema(
                &Arc::new(Schema::empty()),
                &right_batches[0].schema(),
                config,
            )
        };
        return Ok(RecordBatch::new_empty(schema));
    }

    let left_schema = left_batches[0].schema();
    let left = concat_batches(&left_schema, left_batches)
        .map_err(|e| DbError::query_pipeline_arrow("ASOF join (left)", &e))?;

    let right_schema = if right_batches.is_empty() {
        // Build a schema with the same structure but no rows
        Arc::new(Schema::empty())
    } else {
        right_batches[0].schema()
    };

    let right = if right_batches.is_empty() {
        RecordBatch::new_empty(right_schema.clone())
    } else {
        concat_batches(&right_schema, right_batches)
            .map_err(|e| DbError::query_pipeline_arrow("ASOF join (right)", &e))?
    };

    let output_schema = build_output_schema(&left_schema, &right_schema, config);

    // Build right-side index: key_hash -> BTreeMap<timestamp, row_index>
    // Keyed by hash to avoid per-row String allocations.
    let mut right_index: FxHashMap<u64, BTreeMap<i64, Vec<usize>>> =
        FxHashMap::with_capacity_and_hasher(right.num_rows(), rustc_hash::FxBuildHasher);
    let right_keys_col;
    if right.num_rows() > 0 {
        right_keys_col = Some(extract_key_column(&right, &config.key_column)?);
        let right_timestamps = extract_column_as_timestamps(&right, &config.right_time_column)?;
        let rk = right_keys_col.as_ref().unwrap();

        for (i, &ts) in right_timestamps.iter().enumerate() {
            if let Some(key_hash) = rk.hash_at(i) {
                right_index
                    .entry(key_hash)
                    .or_default()
                    .entry(ts)
                    .or_default()
                    .push(i);
            }
            // Null keys are skipped — they can never match per SQL three-valued logic
        }
    } else {
        right_keys_col = None;
    }

    // Extract left key and timestamp columns (zero-alloc borrow)
    let left_keys_col = extract_key_column(&left, &config.key_column)?;
    let left_timestamps = extract_column_as_timestamps(&left, &config.left_time_column)?;

    let tolerance_ms = config
        .tolerance
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));

    // For each left row, find matching right row
    let mut left_indices: Vec<usize> = Vec::with_capacity(left.num_rows());
    let mut right_indices: Vec<Option<usize>> = Vec::with_capacity(left.num_rows());

    for (left_idx, &left_ts) in left_timestamps.iter().enumerate() {
        let Some(left_hash) = left_keys_col.hash_at(left_idx) else {
            // Null left key: Left join emits with null right, Inner join skips
            if config.join_type == AsofSqlJoinType::Left {
                left_indices.push(left_idx);
                right_indices.push(None);
            }
            continue;
        };

        // Walk timestamps in direction order, verifying key equality at each.
        // This handles hash collisions: if a different key occupies the closest
        // timestamp, we continue to the next timestamp rather than giving up.
        let matched_right = right_index.get(&left_hash).and_then(|btree| {
            if let Some(ref rk) = right_keys_col {
                find_verified_match(
                    btree,
                    left_ts,
                    config.direction,
                    tolerance_ms,
                    &left_keys_col,
                    left_idx,
                    rk,
                )
            } else {
                None
            }
        });

        match (&config.join_type, matched_right) {
            (_, Some(right_idx)) => {
                left_indices.push(left_idx);
                right_indices.push(Some(right_idx));
            }
            (AsofSqlJoinType::Left, None) => {
                left_indices.push(left_idx);
                right_indices.push(None);
            }
            (AsofSqlJoinType::Inner, None) => {
                // Skip unmatched rows for inner join
            }
        }
    }

    // Build output columns
    build_output_batch(
        &left,
        &right,
        &left_indices,
        &right_indices,
        &output_schema,
        config,
    )
}

/// Walk timestamps in direction order, returning the first row index where the
/// key matches. Handles hash collisions by continuing to the next-best timestamp
/// when no key match is found at the closest one.
fn find_verified_match(
    btree: &BTreeMap<i64, Vec<usize>>,
    left_ts: i64,
    direction: AsofSqlDirection,
    tolerance_ms: Option<i64>,
    left_keys: &KeyColumn<'_>,
    left_idx: usize,
    right_keys: &KeyColumn<'_>,
) -> Option<usize> {
    match direction {
        AsofSqlDirection::Backward => {
            for (&ts, indices) in btree.range(..=left_ts).rev() {
                if let Some(tol) = tolerance_ms {
                    if left_ts - ts > tol {
                        break;
                    }
                }
                for &idx in indices {
                    if left_keys.keys_equal(left_idx, right_keys, idx) {
                        return Some(idx);
                    }
                }
            }
            None
        }
        AsofSqlDirection::Forward => {
            for (&ts, indices) in btree.range(left_ts..) {
                if let Some(tol) = tolerance_ms {
                    if ts - left_ts > tol {
                        break;
                    }
                }
                for &idx in indices {
                    if left_keys.keys_equal(left_idx, right_keys, idx) {
                        return Some(idx);
                    }
                }
            }
            None
        }
        AsofSqlDirection::Nearest => {
            let mut back = btree.range(..=left_ts).rev().peekable();
            let mut fwd = btree.range(left_ts.saturating_add(1)..).peekable();
            loop {
                let b_dist = back.peek().map(|(&ts, _)| left_ts - ts);
                let f_dist = fwd.peek().map(|(&ts, _)| ts - left_ts);
                match (b_dist, f_dist) {
                    (None, None) => return None,
                    (Some(bd), f) if f.is_none_or(|fd| bd <= fd) => {
                        if let Some(tol) = tolerance_ms {
                            if bd > tol {
                                return None;
                            }
                        }
                        let (_, indices) = back.next().unwrap();
                        for &idx in indices {
                            if left_keys.keys_equal(left_idx, right_keys, idx) {
                                return Some(idx);
                            }
                        }
                    }
                    (_, Some(fd)) => {
                        if let Some(tol) = tolerance_ms {
                            if fd > tol {
                                return None;
                            }
                        }
                        let (_, indices) = fwd.next().unwrap();
                        for &idx in indices {
                            if left_keys.keys_equal(left_idx, right_keys, idx) {
                                return Some(idx);
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}

/// Build the merged output schema from left and right schemas.
///
/// Right-side columns are made nullable for Left joins. Duplicate column
/// names (collisions between left and right) are disambiguated by appending
/// `_{right_table}` to the right-side field.
fn build_output_schema(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    config: &AsofJoinTranslatorConfig,
) -> SchemaRef {
    let mut fields: Vec<Field> = left_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();

    let left_names: FxHashSet<&str> = left_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let make_nullable = config.join_type == AsofSqlJoinType::Left;
    for field in right_schema.fields() {
        // Skip duplicate key column (already in left side)
        if field.name() == &config.key_column {
            continue;
        }
        let mut f = field.as_ref().clone();
        if make_nullable {
            f = f.with_nullable(true);
        }
        // Disambiguate duplicate names by appending _{right_table}
        if left_names.contains(f.name().as_str()) {
            let suffixed_name = format!("{}_{}", f.name(), config.right_table);
            f = f.with_name(suffixed_name);
        }
        fields.push(f);
    }

    Arc::new(Schema::new(fields))
}

/// Build the output `RecordBatch` from matched indices.
fn build_output_batch(
    left: &RecordBatch,
    right: &RecordBatch,
    left_indices: &[usize],
    right_indices: &[Option<usize>],
    output_schema: &SchemaRef,
    config: &AsofJoinTranslatorConfig,
) -> Result<RecordBatch, DbError> {
    let num_rows = left_indices.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(left.num_columns() + right.num_columns());

    // Left-side columns: take selected rows
    #[allow(clippy::cast_possible_truncation)]
    let left_idx_array =
        arrow::array::UInt32Array::from(left_indices.iter().map(|&i| i as u32).collect::<Vec<_>>());
    for col_idx in 0..left.num_columns() {
        let array = left.column(col_idx);
        let taken = arrow::compute::take(array, &left_idx_array, None)
            .map_err(|e| DbError::query_pipeline_arrow("ASOF join (left take)", &e))?;
        columns.push(taken);
    }

    // Right-side columns: take selected rows (with nulls for unmatched)
    let right_schema = right.schema();
    for col_idx in 0..right.num_columns() {
        let field_name = right_schema.field(col_idx).name();
        // Skip duplicate key column
        if field_name == &config.key_column {
            continue;
        }

        let array = right.column(col_idx);
        let taken = take_with_nulls(array, right_indices, num_rows)?;
        columns.push(taken);
    }

    RecordBatch::try_new(output_schema.clone(), columns)
        .map_err(|e| DbError::query_pipeline_arrow("ASOF join (result)", &e))
}

const ASOF_COMPACTION_THRESHOLD: u32 = 32;

/// Right-side index: `key_hash → BTreeMap<timestamp, Vec<row_index>>`.
type RightIndex = FxHashMap<u64, BTreeMap<i64, Vec<usize>>>;

/// Right-side state for streaming ASOF joins. Persists across execution cycles.
#[derive(Default)]
pub(crate) struct AsofRightBuffer {
    index: RightIndex,
    right_concat: Option<RecordBatch>,
    ingest_count: u32,
}

impl AsofRightBuffer {
    /// Ingest new right-side batches, appending to the concatenated batch and
    /// updating the index.
    pub fn ingest(
        &mut self,
        batches: &[RecordBatch],
        key_col: &str,
        time_col: &str,
    ) -> Result<(), DbError> {
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(());
        }

        // Filter out CDC negative events (D, U-) before buffering
        let filtered: Vec<RecordBatch> = batches
            .iter()
            .map(crate::changelog_filter::filter_positive_events)
            .collect::<Result<Vec<_>, _>>()?;
        let batches = &filtered[..];
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(());
        }

        let schema = batches[0].schema();
        let new_batch = arrow::compute::concat_batches(&schema, batches)
            .map_err(|e| DbError::query_pipeline_arrow("ASOF right buffer concat", &e))?;
        if new_batch.num_rows() == 0 {
            return Ok(());
        }

        let timestamps = extract_column_as_timestamps(&new_batch, time_col)?;
        // Pre-compute hashes before new_batch is moved into concat.
        let key_hashes: Vec<Option<u64>> = {
            let keys = extract_key_column(&new_batch, key_col)?;
            (0..new_batch.num_rows()).map(|i| keys.hash_at(i)).collect()
        };

        let (merged, offset) = if let Some(ref existing) = self.right_concat {
            let offset = existing.num_rows();
            let merged = arrow::compute::concat_batches(&schema, &[existing.clone(), new_batch])
                .map_err(|e| DbError::query_pipeline_arrow("ASOF right buffer merge", &e))?;
            (merged, offset)
        } else {
            (new_batch, 0)
        };

        for (i, &ts) in timestamps.iter().enumerate() {
            if let Some(key_hash) = key_hashes[i] {
                self.index
                    .entry(key_hash)
                    .or_default()
                    .entry(ts)
                    .or_default()
                    .push(offset + i);
            }
        }

        self.right_concat = Some(merged);
        self.ingest_count += 1;
        Ok(())
    }

    /// Evict all rows with `ts < cutoff`.
    pub fn evict_before(&mut self, cutoff: i64) -> Result<(), DbError> {
        for btree in self.index.values_mut() {
            let keep = btree.split_off(&cutoff);
            *btree = keep;
        }
        self.index.retain(|_, btree| !btree.is_empty());

        if self.ingest_count >= ASOF_COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    /// Keep, per key, the latest right row at or below `watermark` plus every
    /// newer row; drop the rest. A future left (`ts >= watermark`) backward- or
    /// nearest-matches the latest right `<= its ts`, which is never older than
    /// the latest right `<= watermark`, so older rows are unreachable. Unlike
    /// [`Self::evict_before`] this retains the boundary row, making it safe with
    /// no tolerance.
    pub fn evict_superseded(&mut self, watermark: i64) -> Result<(), DbError> {
        for btree in self.index.values_mut() {
            // Greatest ts <= watermark; keep it and everything newer, drop the
            // rest. `split_off(&keep_ts)` returns the `>= keep_ts` tail.
            if let Some((&keep_ts, _)) = btree.range(..=watermark).next_back() {
                *btree = btree.split_off(&keep_ts);
            }
        }
        self.index.retain(|_, btree| !btree.is_empty());

        if self.ingest_count >= ASOF_COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    fn compact(&mut self) -> Result<(), DbError> {
        let Some(ref batch) = self.right_concat else {
            return Ok(());
        };

        let mut live_rows: Vec<usize> = Vec::new();
        for btree in self.index.values() {
            for indices in btree.values() {
                live_rows.extend_from_slice(indices);
            }
        }

        if live_rows.is_empty() {
            self.right_concat = None;
            self.index.clear();
            self.ingest_count = 0;
            return Ok(());
        }

        live_rows.sort_unstable();
        live_rows.dedup();

        let mut idx_map: FxHashMap<usize, usize> = FxHashMap::default();
        for (new_idx, &old_idx) in live_rows.iter().enumerate() {
            idx_map.insert(old_idx, new_idx);
        }

        #[allow(clippy::cast_possible_truncation)]
        let take_indices = arrow::array::UInt32Array::from(
            live_rows.iter().map(|&i| i as u32).collect::<Vec<_>>(),
        );
        let schema = batch.schema();
        let columns: Result<Vec<ArrayRef>, _> = (0..batch.num_columns())
            .map(|col| arrow::compute::take(batch.column(col), &take_indices, None))
            .collect();
        let columns =
            columns.map_err(|e| DbError::query_pipeline_arrow("ASOF right buffer compact", &e))?;
        let compacted = RecordBatch::try_new(schema, columns)
            .map_err(|e| DbError::query_pipeline_arrow("ASOF right buffer compact batch", &e))?;

        for btree in self.index.values_mut() {
            for indices in btree.values_mut() {
                for idx in indices.iter_mut() {
                    *idx = idx_map[idx];
                }
            }
        }

        self.right_concat = Some(compacted);
        self.ingest_count = 0;
        Ok(())
    }

    pub fn estimated_size_bytes(&self) -> usize {
        let index_size: usize = self
            .index
            .values()
            .map(|btree| btree.len() * (8 + 8 + 24))
            .sum();
        let batch_size = self
            .right_concat
            .as_ref()
            .map_or(0, RecordBatch::get_array_memory_size);
        index_size + batch_size
    }
}

/// Execute an ASOF join of left batches against a stateful right buffer.
/// The right buffer must have been pre-populated via `AsofRightBuffer::ingest`.
pub(crate) fn execute_asof_join_with_state(
    left_batches: &[RecordBatch],
    right_buffer: &AsofRightBuffer,
    config: &AsofJoinTranslatorConfig,
    right_schema_hint: Option<&SchemaRef>,
) -> Result<RecordBatch, DbError> {
    if left_batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }

    let left_schema = left_batches[0].schema();
    let left = concat_batches(&left_schema, left_batches)
        .map_err(|e| DbError::query_pipeline_arrow("ASOF join (left)", &e))?;

    let Some(right) = right_buffer.right_concat.clone() else {
        // No right data buffered. A Left join emits left rows with null right
        // columns — but only once the right schema is known (captured from an
        // earlier batch). Keeping those columns is essential: a downstream
        // projection that references a right column (e.g. a spike ratio over a
        // slow-warming baseline) fails to plan if they're dropped. Until the
        // right schema is known, emit nothing rather than a malformed batch.
        if config.join_type == AsofSqlJoinType::Left {
            if let Some(right_schema) = right_schema_hint {
                let output_schema = build_output_schema(&left_schema, right_schema, config);
                let left_indices: Vec<usize> = (0..left.num_rows()).collect();
                let right_indices: Vec<Option<usize>> = vec![None; left.num_rows()];
                return build_output_batch(
                    &left,
                    &RecordBatch::new_empty(right_schema.clone()),
                    &left_indices,
                    &right_indices,
                    &output_schema,
                    config,
                );
            }
        }
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    };

    let right_schema = right.schema();
    let output_schema = build_output_schema(&left_schema, &right_schema, config);

    let left_keys_col = extract_key_column(&left, &config.key_column)?;
    let left_timestamps = extract_column_as_timestamps(&left, &config.left_time_column)?;
    let right_keys_col = extract_key_column(&right, &config.key_column)?;

    let tolerance_ms = config
        .tolerance
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));

    let mut left_indices: Vec<usize> = Vec::with_capacity(left.num_rows());
    let mut right_indices: Vec<Option<usize>> = Vec::with_capacity(left.num_rows());

    for (left_idx, &left_ts) in left_timestamps.iter().enumerate() {
        let Some(left_hash) = left_keys_col.hash_at(left_idx) else {
            if config.join_type == AsofSqlJoinType::Left {
                left_indices.push(left_idx);
                right_indices.push(None);
            }
            continue;
        };

        let matched_right = right_buffer.index.get(&left_hash).and_then(|btree| {
            find_verified_match(
                btree,
                left_ts,
                config.direction,
                tolerance_ms,
                &left_keys_col,
                left_idx,
                &right_keys_col,
            )
        });

        match (&config.join_type, matched_right) {
            (_, Some(right_idx)) => {
                left_indices.push(left_idx);
                right_indices.push(Some(right_idx));
            }
            (AsofSqlJoinType::Left, None) => {
                left_indices.push(left_idx);
                right_indices.push(None);
            }
            (AsofSqlJoinType::Inner, None) => {}
        }
    }

    build_output_batch(
        &left,
        &right,
        &left_indices,
        &right_indices,
        &output_schema,
        config,
    )
}

/// Serializable checkpoint for `AsofRightBuffer`.
///
/// Row indices are `u32` because they index into a single `RecordBatch`
/// whose row count is itself `u32`-bounded; using `u32` halves the
/// in-memory footprint of the index-entries vec compared to `usize`.
#[derive(
    serde::Serialize, serde::Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub(crate) struct AsofBufferCheckpoint {
    #[serde(default)]
    pub right_buffer_ipc: Vec<u8>,
    #[serde(default)]
    pub index_entries: Vec<(u64, i64, Vec<u32>)>,
    #[serde(default = "default_evicted_watermark")]
    pub last_evicted_watermark: i64,
}

fn default_evicted_watermark() -> i64 {
    i64::MIN
}

impl AsofRightBuffer {
    pub fn snapshot_checkpoint(
        &mut self,
        last_evicted_watermark: i64,
    ) -> Result<AsofBufferCheckpoint, DbError> {
        self.compact()?;

        let right_buffer_ipc = if let Some(ref batch) = self.right_concat {
            if batch.num_rows() > 0 {
                laminar_core::serialization::serialize_batch_stream(batch).map_err(|e| {
                    DbError::Pipeline(format!("ASOF checkpoint right buffer serialization: {e}"))
                })?
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        let mut index_entries = Vec::new();
        for (&key_hash, btree) in &self.index {
            for (&ts, indices) in btree {
                // Compact via u32. `compact()` was just called above, so indices
                // are bounded by the row count of `right_concat`, which is itself
                // u32-bounded.
                #[allow(clippy::cast_possible_truncation)]
                let narrow: Vec<u32> = indices.iter().map(|&i| i as u32).collect();
                index_entries.push((key_hash, ts, narrow));
            }
        }

        Ok(AsofBufferCheckpoint {
            right_buffer_ipc,
            index_entries,
            last_evicted_watermark,
        })
    }

    pub fn from_checkpoint(cp: &AsofBufferCheckpoint) -> Result<(Self, i64), DbError> {
        let right_concat = if cp.right_buffer_ipc.is_empty() {
            None
        } else {
            Some(
                laminar_core::serialization::deserialize_batch_stream(&cp.right_buffer_ipc)
                    .map_err(|e| {
                        DbError::Pipeline(format!(
                            "ASOF checkpoint right buffer deserialization: {e}"
                        ))
                    })?,
            )
        };

        let mut index: RightIndex = FxHashMap::default();
        for (key_hash, ts, indices) in &cp.index_entries {
            let widened: Vec<usize> = indices.iter().map(|&i| i as usize).collect();
            index.entry(*key_hash).or_default().insert(*ts, widened);
        }

        Ok((
            Self {
                index,
                right_concat,
                ingest_count: 0,
            },
            cp.last_evicted_watermark,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use std::time::Duration;

    fn trades_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("trade_ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "AAPL", "GOOG", "AAPL"])),
                Arc::new(Int64Array::from(vec![100, 200, 150, 300])),
                Arc::new(Float64Array::from(vec![150.0, 152.0, 2800.0, 155.0])),
            ],
        )
        .unwrap()
    }

    fn quotes_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("quote_ts", DataType::Int64, false),
            Field::new("bid", DataType::Float64, false),
            Field::new("ask", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "AAPL", "AAPL", "GOOG", "AAPL", "GOOG",
                ])),
                Arc::new(Int64Array::from(vec![90, 180, 140, 250, 160])),
                Arc::new(Float64Array::from(vec![
                    149.0, 151.0, 2790.0, 153.0, 2795.0,
                ])),
                Arc::new(Float64Array::from(vec![
                    150.0, 152.0, 2800.0, 154.0, 2805.0,
                ])),
            ],
        )
        .unwrap()
    }

    fn backward_config() -> AsofJoinTranslatorConfig {
        AsofJoinTranslatorConfig {
            left_table: "trades".to_string(),
            right_table: "quotes".to_string(),
            key_column: "symbol".to_string(),
            left_time_column: "trade_ts".to_string(),
            right_time_column: "quote_ts".to_string(),
            direction: AsofSqlDirection::Backward,
            tolerance: None,
            join_type: AsofSqlJoinType::Left,
        }
    }

    #[test]
    fn test_backward_join_basic() {
        let config = backward_config();
        let result =
            execute_asof_join_batch(&[trades_batch()], &[quotes_batch()], &config).unwrap();

        // 4 left rows → 4 output rows (Left join)
        assert_eq!(result.num_rows(), 4);
        // Output should have: symbol, trade_ts, price, quote_ts, bid, ask
        assert_eq!(result.num_columns(), 6);

        // Verify AAPL trade at ts=100 matches quote at ts=90 (backward: 90 <= 100)
        let quote_ts = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(quote_ts.value(0), 90); // trade@100 → quote@90
        assert_eq!(quote_ts.value(1), 180); // trade@200 → quote@180
    }

    #[test]
    fn asof_empty_right_buffer_keeps_right_columns() {
        let config = backward_config(); // Left join
        let empty = AsofRightBuffer::default();

        // Warm-up: right schema unknown yet -> emit nothing, not a left-only
        // batch (which would break a downstream projection over a right column).
        let out = execute_asof_join_with_state(&[trades_batch()], &empty, &config, None).unwrap();
        assert_eq!(out.num_rows(), 0);

        // Right schema known -> emit left rows with the right columns all null,
        // so a projection referencing e.g. `quote_ts` still resolves.
        let right_schema = quotes_batch().schema();
        let out =
            execute_asof_join_with_state(&[trades_batch()], &empty, &config, Some(&right_schema))
                .unwrap();
        assert_eq!(out.num_rows(), trades_batch().num_rows());
        let qts = out
            .column_by_name("quote_ts")
            .expect("right column must be kept");
        assert_eq!(qts.null_count(), out.num_rows(), "unmatched right is null");
    }

    #[test]
    fn test_forward_join_basic() {
        let mut config = backward_config();
        config.direction = AsofSqlDirection::Forward;

        let result =
            execute_asof_join_batch(&[trades_batch()], &[quotes_batch()], &config).unwrap();

        assert_eq!(result.num_rows(), 4);
        // AAPL trade at ts=100 → forward match is quote@180 (earliest >= 100)
        let quote_ts = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(quote_ts.value(0), 180); // trade@100 → quote@180 (forward)
        assert_eq!(quote_ts.value(1), 250); // trade@200 → quote@250 (earliest >= 200)
    }

    #[test]
    fn test_left_join_emits_unmatched_with_nulls() {
        // Create trades with a symbol that has no quotes
        let trades_schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("trade_ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        let trades = RecordBatch::try_new(
            trades_schema,
            vec![
                Arc::new(StringArray::from(vec!["MSFT"])),
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Float64Array::from(vec![300.0])),
            ],
        )
        .unwrap();

        let config = backward_config();
        let result = execute_asof_join_batch(&[trades], &[quotes_batch()], &config).unwrap();

        // Left join: MSFT has no match, should still emit with nulls
        assert_eq!(result.num_rows(), 1);
        assert!(result.column(3).is_null(0)); // quote_ts is null
    }

    #[test]
    fn test_inner_join_skips_unmatched() {
        let trades_schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("trade_ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        let trades = RecordBatch::try_new(
            trades_schema,
            vec![
                Arc::new(StringArray::from(vec!["MSFT", "AAPL"])),
                Arc::new(Int64Array::from(vec![100, 200])),
                Arc::new(Float64Array::from(vec![300.0, 152.0])),
            ],
        )
        .unwrap();

        let mut config = backward_config();
        config.join_type = AsofSqlJoinType::Inner;

        let result = execute_asof_join_batch(&[trades], &[quotes_batch()], &config).unwrap();

        // Inner join: MSFT skipped, only AAPL matches
        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn test_tolerance_filtering() {
        let mut config = backward_config();
        config.tolerance = Some(Duration::from_millis(15));

        let result =
            execute_asof_join_batch(&[trades_batch()], &[quotes_batch()], &config).unwrap();

        // AAPL trade@100 → quote@90 (diff=10, within 15ms tolerance) ✓
        // AAPL trade@200 → quote@180 (diff=20, exceeds 15ms) → null (Left join)
        // GOOG trade@150 → quote@140 (diff=10, within 15ms) ✓
        // AAPL trade@300 → quote@250 (diff=50, exceeds 15ms) → null
        assert_eq!(result.num_rows(), 4); // Left join, all left rows emitted
        let quote_ts = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(quote_ts.value(0), 90); // matched
        assert!(result.column(3).is_null(1)); // no match within tolerance
        assert_eq!(quote_ts.value(2), 140); // matched
        assert!(result.column(3).is_null(3)); // no match within tolerance
    }

    #[test]
    fn test_empty_left_input() {
        let config = backward_config();
        let result = execute_asof_join_batch(&[], &[quotes_batch()], &config).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_empty_right_input() {
        let config = backward_config();
        let result = execute_asof_join_batch(&[trades_batch()], &[], &config).unwrap();

        // Left join with no right data: all rows emitted with nulls
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn test_multiple_keys() {
        // Both AAPL and GOOG trades should match their respective quotes
        let config = backward_config();
        let result =
            execute_asof_join_batch(&[trades_batch()], &[quotes_batch()], &config).unwrap();

        assert_eq!(result.num_rows(), 4);

        // Check GOOG trade@150 matches GOOG quote@140 (not an AAPL quote)
        let symbols = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let quote_ts = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Row 2 is GOOG
        assert_eq!(symbols.value(2), "GOOG");
        assert_eq!(quote_ts.value(2), 140); // GOOG quote, not AAPL
    }

    #[test]
    fn test_multiple_right_matches_picks_closest() {
        // For backward: AAPL trade@200 should pick quote@180 (closest), not quote@90
        let config = backward_config();
        let result =
            execute_asof_join_batch(&[trades_batch()], &[quotes_batch()], &config).unwrap();

        let quote_ts = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // AAPL trade@200: backward match picks 180 (closest <= 200), not 90
        assert_eq!(quote_ts.value(1), 180);
    }

    #[test]
    fn test_nearest_join() {
        // Trades: AAPL@100, AAPL@200, GOOG@150, AAPL@300
        // Quotes: AAPL@90, AAPL@180, GOOG@140, AAPL@250, GOOG@160
        // Nearest should pick closest by absolute time difference:
        //   AAPL@100 → quote@90 (diff=10) vs quote@180 (diff=80) → 90
        //   AAPL@200 → quote@180 (diff=20) vs quote@250 (diff=50) → 180
        //   GOOG@150 → quote@140 (diff=10) vs quote@160 (diff=10) → 140 (tie: backward wins)
        //   AAPL@300 → quote@250 (diff=50) → 250
        let mut config = backward_config();
        config.direction = AsofSqlDirection::Nearest;

        let result =
            execute_asof_join_batch(&[trades_batch()], &[quotes_batch()], &config).unwrap();

        assert_eq!(result.num_rows(), 4);
        let quote_ts = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(quote_ts.value(0), 90); // AAPL@100 → nearest is 90
        assert_eq!(quote_ts.value(1), 180); // AAPL@200 → nearest is 180
        assert_eq!(quote_ts.value(2), 140); // GOOG@150 → tie, backward wins
        assert_eq!(quote_ts.value(3), 250); // AAPL@300 → only 250 nearby
    }

    #[test]
    fn test_hash_collision_different_keys() {
        // Two different keys at the same timestamp should both match correctly,
        // even if they happen to share the same hash bucket.
        let trades_schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("trade_ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        let trades = RecordBatch::try_new(
            trades_schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(Int64Array::from(vec![100, 100])), // same timestamp
                Arc::new(Float64Array::from(vec![150.0, 2800.0])),
            ],
        )
        .unwrap();

        let quotes_schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("quote_ts", DataType::Int64, false),
            Field::new("bid", DataType::Float64, false),
        ]));
        let quotes = RecordBatch::try_new(
            quotes_schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(Int64Array::from(vec![100, 100])), // same timestamp as trades
                Arc::new(Float64Array::from(vec![149.0, 2790.0])),
            ],
        )
        .unwrap();

        let config = backward_config();
        let result = execute_asof_join_batch(&[trades], &[quotes], &config).unwrap();

        // Both rows should match their respective keys
        assert_eq!(result.num_rows(), 2);

        let symbols = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let bids = result
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // AAPL trade should match AAPL quote (bid=149.0)
        assert_eq!(symbols.value(0), "AAPL");
        assert!((bids.value(0) - 149.0).abs() < f64::EPSILON);

        // GOOG trade should match GOOG quote (bid=2790.0), not be lost
        assert_eq!(symbols.value(1), "GOOG");
        assert!((bids.value(1) - 2790.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_null_key_no_match() {
        // Null-keyed rows should produce no matches
        let trades_schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, true),
            Field::new("trade_ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        let trades = RecordBatch::try_new(
            trades_schema,
            vec![
                Arc::new(StringArray::from(vec![Some("AAPL"), None])),
                Arc::new(Int64Array::from(vec![100, 100])),
                Arc::new(Float64Array::from(vec![150.0, 200.0])),
            ],
        )
        .unwrap();

        let mut config = backward_config();
        config.join_type = AsofSqlJoinType::Inner;

        let result = execute_asof_join_batch(&[trades], &[quotes_batch()], &config).unwrap();

        // Only AAPL matches; null key row is skipped for inner join
        assert_eq!(result.num_rows(), 1);
        let symbols = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(symbols.value(0), "AAPL");
    }

    #[test]
    fn test_null_key_left_join_emits_nulls() {
        // Left join: null-key rows emit with null right columns
        let trades_schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, true),
            Field::new("trade_ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        let trades = RecordBatch::try_new(
            trades_schema,
            vec![
                Arc::new(StringArray::from(vec![Some("AAPL"), None])),
                Arc::new(Int64Array::from(vec![100, 100])),
                Arc::new(Float64Array::from(vec![150.0, 200.0])),
            ],
        )
        .unwrap();

        let config = backward_config(); // Left join by default

        let result = execute_asof_join_batch(&[trades], &[quotes_batch()], &config).unwrap();

        // Both rows emitted: AAPL matched, null-key row with null right cols
        assert_eq!(result.num_rows(), 2);
        let symbols = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(symbols.value(0), "AAPL");
        assert!(result.column(0).is_null(1)); // null key row
        assert!(result.column(3).is_null(1)); // right-side quote_ts is null
    }

    /// Single-key quote row matching `quotes_batch`'s schema, at timestamp `ts`.
    fn quote_at(symbol: &str, ts: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("quote_ts", DataType::Int64, false),
            Field::new("bid", DataType::Float64, false),
            Field::new("ask", DataType::Float64, false),
        ]));
        #[allow(clippy::cast_precision_loss)]
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![symbol])),
                Arc::new(Int64Array::from(vec![ts])),
                Arc::new(Float64Array::from(vec![ts as f64])),
                Arc::new(Float64Array::from(vec![ts as f64 + 1.0])),
            ],
        )
        .unwrap()
    }

    /// Single-key trade row matching `trades_batch`'s schema, at timestamp `ts`.
    fn trade_at(symbol: &str, ts: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("trade_ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        #[allow(clippy::cast_precision_loss)]
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![symbol])),
                Arc::new(Int64Array::from(vec![ts])),
                Arc::new(Float64Array::from(vec![ts as f64])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn asof_no_tolerance_evicts_superseded_right() {
        // Backward, no tolerance. Three right rows for one key at t=1,2,3.
        let config = backward_config();
        let mut buf = AsofRightBuffer::default();
        for ts in [1_i64, 2, 3] {
            buf.ingest(&[quote_at("AAPL", ts)], "symbol", "quote_ts")
                .unwrap();
        }
        assert_eq!(buf.index.values().next().unwrap().len(), 3);

        // Advance the left watermark to 3: any future left L >= 3 backward-
        // matches the latest right <= L, which is >= the latest right <= 3.
        // So only t=3 is reachable; t=1 and t=2 are superseded.
        buf.evict_superseded(3).unwrap();
        let btree = buf.index.values().next().unwrap();
        assert_eq!(btree.len(), 1, "only the latest <= watermark is kept");
        assert!(btree.contains_key(&3));

        // A future left at t=5 still backward-matches the retained right at t=3.
        let out =
            execute_asof_join_with_state(&[trade_at("AAPL", 5)], &buf, &config, None).unwrap();
        let quote_ts = out
            .column_by_name("quote_ts")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(quote_ts.value(0), 3);
    }

    #[test]
    fn asof_evict_superseded_then_compact_remaps_offsets() {
        // The riskiest path: sparse per-key eviction followed by compaction,
        // which rebuilds `right_concat` and must remap the surviving index
        // offsets. Ingest past ASOF_COMPACTION_THRESHOLD so eviction compacts.
        let mut buf = AsofRightBuffer::default();
        for ts in 1..=40_i64 {
            buf.ingest(&[quote_at("AAPL", ts)], "symbol", "quote_ts")
                .unwrap();
        }

        // Keep only t=39 (latest <= 39) and t=40 (newer than the watermark).
        buf.evict_superseded(39).unwrap();
        assert!(
            buf.ingest_count < ASOF_COMPACTION_THRESHOLD,
            "compact() should have run and reset ingest_count"
        );
        let btree = buf.index.values().next().unwrap();
        assert_eq!(btree.len(), 2);
        assert_eq!(
            buf.right_concat.as_ref().unwrap().num_rows(),
            2,
            "data buffer shrinks with the index"
        );

        // After remapping, matches still resolve against the compacted buffer.
        let config = backward_config();
        let out =
            execute_asof_join_with_state(&[trade_at("AAPL", 100)], &buf, &config, None).unwrap();
        let quote_ts = out
            .column_by_name("quote_ts")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(quote_ts.value(0), 40); // backward match = latest <= 100
    }
}
