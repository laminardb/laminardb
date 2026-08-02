#![deny(clippy::disallowed_types)]

//! Stream-stream interval join for
//! `right_ts BETWEEN left_ts AND left_ts + time_bound`. Evicts expired rows on watermark advance.

use std::collections::BTreeMap;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::array::{
    new_null_array, Array, ArrayRef, BinaryArray, BinaryViewArray, Int64Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray, StringViewArray,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use rustc_hash::{FxHashMap, FxHasher};

use laminar_sql::parser::join_parser::JoinType;
use laminar_sql::translator::StreamJoinConfig;

use crate::error::DbError;
use crate::key_column::{extract_column_as_timestamps, extract_key_column, KeyColumn};

const COMPACTION_THRESHOLD: usize = 32;
const MAX_RETAINED_BATCHES: usize = 256;

/// Caps memory on cross-product shapes.
const EMIT_THRESHOLD: usize = 8_192;
const MAX_CYCLE_OUTPUT_ROWS: usize = 262_144;
const MAX_CYCLE_OUTPUT_BYTES: usize = 64 * 1024 * 1024;
const ROW_MATCHED: u8 = 1;
const ROW_EMITTED: u8 = 2;
// Null tuples never probe. Keeping them in a regular bucket lets eviction and compaction retain
// one state path for every row; a real hash collision remains safe because tuple equality follows.
const NULL_TUPLE_HASH: u64 = u64::MAX;

#[derive(Default)]
pub(crate) struct IntervalJoinOutputBudget {
    emitted_rows: usize,
    emitted_bytes: usize,
}

// Conservative allocator-independent charges. The B-tree charge covers a sparsely populated
// internal node (including spare key/value slots and child edges), while the hash charge covers
// bucket slack and control bytes. These are budget charges, not an allocator/RSS measurement.
const HEAP_ALLOCATION_CHARGE: usize = 32;
const HASH_BUCKET_CHARGE: usize = 128;
const BTREE_TIMESTAMP_CHARGE: usize = 512;
const BATCH_METADATA_CHARGE: usize = 256;
const ARRAY_METADATA_CHARGE: usize = 128;
// A restored row can be the first position for a unique timestamp and hash. `HashMap`'s smallest
// table has three usable buckets, so three charged buckets per row also covers every growth step.
// `Vec::push` currently reserves four position slots for the first element. This intentionally
// rejects a checkpoint unless its worst supported index shape fits before index construction.
const WORST_CASE_ROW_NON_HASH_CHARGE: usize = std::mem::size_of::<usize>()
    + std::mem::size_of::<u8>()
    + BTREE_TIMESTAMP_CHARGE
    + HEAP_ALLOCATION_CHARGE
    + 4 * std::mem::size_of::<(usize, usize)>();
const RESTORE_WORST_CASE_ROW_CHARGE: usize =
    WORST_CASE_ROW_NON_HASH_CHARGE + 3 * HASH_BUCKET_CHARGE;

type SideIndex = FxHashMap<u64, BTreeMap<i64, Vec<(usize, usize)>>>;

fn batch_metadata_charge(batch: &RecordBatch) -> usize {
    batch
        .num_columns()
        .saturating_mul(ARRAY_METADATA_CHARGE)
        .saturating_add(BATCH_METADATA_CHARGE)
        .saturating_add(batch.schema().fields().iter().fold(0usize, |bytes, field| {
            bytes.saturating_add(field.name().len())
        }))
}

fn position_vector_charge(capacity: usize) -> usize {
    HEAP_ALLOCATION_CHARGE
        .saturating_add(capacity.saturating_mul(std::mem::size_of::<(usize, usize)>()))
}

fn insert_index_position(
    index: &mut SideIndex,
    index_entry_bytes: &mut usize,
    key_hash: u64,
    timestamp: i64,
    position: (usize, usize),
) {
    use std::collections::btree_map::Entry;

    let timestamps = index.entry(key_hash).or_default();
    match timestamps.entry(timestamp) {
        Entry::Vacant(entry) => {
            let mut positions = Vec::new();
            positions.push(position);
            *index_entry_bytes = index_entry_bytes
                .saturating_add(BTREE_TIMESTAMP_CHARGE)
                .saturating_add(position_vector_charge(positions.capacity()));
            entry.insert(positions);
        }
        Entry::Occupied(mut entry) => {
            let positions = entry.get_mut();
            let previous_capacity = positions.capacity();
            positions.push(position);
            *index_entry_bytes = index_entry_bytes.saturating_add(
                positions
                    .capacity()
                    .saturating_sub(previous_capacity)
                    .saturating_mul(std::mem::size_of::<(usize, usize)>()),
            );
        }
    }
}

fn logical_row_bytes(batch: &RecordBatch) -> Result<Vec<usize>, DbError> {
    let mut bytes = vec![0usize; batch.num_rows()];
    for column in batch.columns() {
        let fixed = match column.data_type() {
            DataType::Null => Some(0),
            DataType::Boolean => Some(2),
            DataType::FixedSizeBinary(width) => usize::try_from(*width)
                .ok()
                .and_then(|width| width.checked_add(1)),
            data_type => data_type
                .primitive_width()
                .and_then(|width| width.checked_add(1)),
        };
        if let Some(charge) = fixed {
            for row in &mut bytes {
                *row = row.saturating_add(charge);
            }
            continue;
        }

        let lengths: Box<dyn Iterator<Item = usize> + '_> = match column.data_type() {
            DataType::Utf8 => Box::new(
                column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 column has StringArray representation")
                    .iter()
                    .map(|value| value.map_or(0, str::len)),
            ),
            DataType::LargeUtf8 => Box::new(
                column
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .expect("LargeUtf8 column has LargeStringArray representation")
                    .iter()
                    .map(|value| value.map_or(0, str::len)),
            ),
            DataType::Binary => Box::new(
                column
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("Binary column has BinaryArray representation")
                    .iter()
                    .map(|value| value.map_or(0, <[u8]>::len)),
            ),
            DataType::LargeBinary => Box::new(
                column
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .expect("LargeBinary column has LargeBinaryArray representation")
                    .iter()
                    .map(|value| value.map_or(0, <[u8]>::len)),
            ),
            DataType::Utf8View => Box::new(
                column
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .expect("Utf8View column has StringViewArray representation")
                    .iter()
                    .map(|value| value.map_or(0, str::len)),
            ),
            DataType::BinaryView => Box::new(
                column
                    .as_any()
                    .downcast_ref::<BinaryViewArray>()
                    .expect("BinaryView column has BinaryViewArray representation")
                    .iter()
                    .map(|value| value.map_or(0, <[u8]>::len)),
            ),
            _ => {
                return (0..batch.num_rows())
                    .map(|row| {
                        laminar_core::shuffle::logical_batch_bytes(&batch.slice(row, 1)).map_err(
                            |error| {
                                DbError::query_pipeline_arrow("interval join row sizing", &error)
                            },
                        )
                    })
                    .collect();
            }
        };
        let offset_bytes = match column.data_type() {
            DataType::LargeUtf8 | DataType::LargeBinary => 16,
            DataType::Utf8View | DataType::BinaryView => 16,
            _ => 8,
        };
        for (row, length) in bytes.iter_mut().zip(lengths) {
            *row = row
                .saturating_add(length)
                .saturating_add(offset_bytes)
                .saturating_add(1);
        }
    }
    Ok(bytes)
}

fn extract_key_columns<'a>(
    batch: &'a RecordBatch,
    names: &[String],
) -> Result<Vec<KeyColumn<'a>>, DbError> {
    if names.is_empty() {
        return Err(DbError::InvalidOperation(
            "interval join requires at least one equality key".into(),
        ));
    }
    names
        .iter()
        .map(|name| extract_key_column(batch, name))
        .collect()
}

fn tuple_hash_at(keys: &[KeyColumn<'_>], row: usize) -> Option<u64> {
    if keys.iter().any(|key| key.is_null(row)) {
        return None;
    }
    let mut hasher = FxHasher::default();
    hasher.write_usize(keys.len());
    for (position, key) in keys.iter().enumerate() {
        hasher.write_usize(position);
        key.hash_into(row, &mut hasher);
    }
    Some(hasher.finish())
}

fn tuples_equal(
    left: &[KeyColumn<'_>],
    left_row: usize,
    right: &[KeyColumn<'_>],
    right_row: usize,
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.keys_equal(left_row, right, right_row))
}

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct JoinStateCheckpoint {
    pub join_type: u8,
    pub left_keys: Vec<String>,
    pub right_keys: Vec<String>,
    pub left_time_column: String,
    pub right_time_column: String,
    pub left_table: String,
    pub right_table: String,
    pub bound_ms: i64,
    pub left_buffer_rows: u64,
    pub right_buffer_rows: u64,
    pub left_batches: Vec<Vec<u8>>,
    pub right_batches: Vec<Vec<u8>>,
    pub left_evicted_cutoff: i64,
    pub right_evicted_cutoff: i64,
    pub left_row_flags: Vec<Vec<u8>>,
    pub right_row_flags: Vec<Vec<u8>>,
}

impl JoinStateCheckpoint {
    pub(crate) fn retained_ipc_bytes(&self) -> Result<usize, DbError> {
        let payload = self
            .left_batches
            .iter()
            .chain(&self.right_batches)
            .chain(&self.left_row_flags)
            .chain(&self.right_row_flags)
            .try_fold(0usize, |total, batch| {
                total.checked_add(batch.capacity()).ok_or_else(|| {
                    DbError::Checkpoint(
                        "interval join checkpoint retained IPC accounting overflow".into(),
                    )
                })
            })?;
        self.left_keys
            .iter()
            .chain(&self.right_keys)
            .map(String::capacity)
            .chain([
                self.left_time_column.capacity(),
                self.right_time_column.capacity(),
                self.left_table.capacity(),
                self.right_table.capacity(),
            ])
            .try_fold(payload, |total, bytes| {
                total.checked_add(bytes).ok_or_else(|| {
                    DbError::Checkpoint(
                        "interval join checkpoint configuration accounting overflow".into(),
                    )
                })
            })
    }
}

pub(crate) const fn join_type_tag(join_type: JoinType) -> u8 {
    match join_type {
        JoinType::Inner => 0,
        JoinType::Left => 1,
        JoinType::Right => 2,
        JoinType::Full => 3,
        JoinType::LeftSemi => 4,
        JoinType::LeftAnti => 5,
        JoinType::RightSemi => 6,
        JoinType::RightAnti => 7,
    }
}

#[derive(Clone)]
pub(crate) struct SideState {
    batches: Vec<RecordBatch>,
    index: SideIndex,
    row_count: usize,
    retained_rows: usize,
    retained_batch_bytes: usize,
    retained_batch_metadata_bytes: usize,
    row_bytes: Vec<Vec<usize>>,
    row_flags: Vec<Vec<u8>>,
    row_size_vector_bytes: usize,
    index_entry_bytes: usize,
}

impl SideState {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
            index: FxHashMap::default(),
            row_count: 0,
            retained_rows: 0,
            retained_batch_bytes: 0,
            retained_batch_metadata_bytes: 0,
            row_bytes: Vec::new(),
            row_flags: Vec::new(),
            row_size_vector_bytes: 0,
            index_entry_bytes: 0,
        }
    }

    pub(crate) fn add_batch(
        &mut self,
        batch: &RecordBatch,
        key_col_names: &[String],
        time_col_name: &str,
        retain_null_tuples: bool,
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
        let keys = extract_key_columns(batch, key_col_names)?;
        let timestamps = extract_column_as_timestamps(batch, time_col_name)?;
        let mut indexed_rows = 0usize;
        for (row_idx, &ts) in timestamps.iter().enumerate() {
            let key_hash = match tuple_hash_at(&keys, row_idx) {
                Some(hash) => hash,
                None if retain_null_tuples => NULL_TUPLE_HASH,
                None => continue,
            };
            insert_index_position(
                &mut self.index,
                &mut self.index_entry_bytes,
                key_hash,
                ts,
                (batch_idx, row_idx),
            );
            indexed_rows += 1;
        }
        if indexed_rows == 0 {
            return Ok(false);
        }
        let row_bytes = logical_row_bytes(batch)?;
        self.row_count += indexed_rows;
        self.retained_rows = self.retained_rows.saturating_add(batch.num_rows());
        self.retained_batch_bytes = self
            .retained_batch_bytes
            .saturating_add(batch.get_array_memory_size());
        self.retained_batch_metadata_bytes = self
            .retained_batch_metadata_bytes
            .saturating_add(batch_metadata_charge(batch));
        self.row_size_vector_bytes = self
            .row_size_vector_bytes
            .saturating_add(position_vector_charge(row_bytes.capacity()));
        self.batches.push(batch.clone());
        self.row_bytes.push(row_bytes);
        self.row_flags.push(vec![0; batch.num_rows()]);
        Ok(true)
    }

    fn evict_before(
        &mut self,
        cutoff: i64,
        key_cols: &[String],
        time_col: &str,
    ) -> Result<(), DbError> {
        for btree in self.index.values_mut() {
            let keep = btree.split_off(&cutoff);
            for entries in btree.values() {
                self.row_count = self.row_count.saturating_sub(entries.len());
                self.index_entry_bytes = self
                    .index_entry_bytes
                    .saturating_sub(BTREE_TIMESTAMP_CHARGE)
                    .saturating_sub(position_vector_charge(entries.capacity()));
            }
            *btree = keep;
        }
        self.index.retain(|_, btree| !btree.is_empty());

        if self.row_count == 0 {
            self.batches.clear();
            self.retained_rows = 0;
            self.retained_batch_bytes = 0;
            self.retained_batch_metadata_bytes = 0;
            self.row_bytes.clear();
            self.row_flags.clear();
            self.row_size_vector_bytes = 0;
            self.index_entry_bytes = 0;
            return Ok(());
        }

        if self.batches.len() > COMPACTION_THRESHOLD
            && (self.retained_rows != self.row_count || self.batches.len() > MAX_RETAINED_BATCHES)
        {
            self.compact(key_cols, time_col)?;
        }
        Ok(())
    }

    fn unmatched_positions_before(
        &self,
        cutoff: i64,
        limit: usize,
    ) -> Result<Vec<(usize, usize)>, DbError> {
        let mut positions = Vec::new();
        for position in self
            .index
            .values()
            .flat_map(|timestamps| timestamps.range(..cutoff))
            .flat_map(|(_, positions)| positions.iter().copied())
        {
            let flags = self.row_flags[position.0][position.1];
            if flags & (ROW_MATCHED | ROW_EMITTED) != 0 {
                continue;
            }
            if positions.len() == limit {
                return Err(DbError::BackpressureFail(format!(
                    "interval join cycle exceeded {MAX_CYCLE_OUTPUT_ROWS} output rows while finalizing unmatched state"
                )));
            }
            positions.push(position);
        }
        positions.sort_unstable();
        Ok(positions)
    }

    fn compact(&mut self, key_cols: &[String], time_col: &str) -> Result<(), DbError> {
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
            self.retained_rows = 0;
            self.retained_batch_bytes = 0;
            self.retained_batch_metadata_bytes = 0;
            self.row_bytes.clear();
            self.row_flags.clear();
            self.row_size_vector_bytes = 0;
            self.index_entry_bytes = 0;
            return Ok(());
        }

        live_rows.sort_unstable();
        let replacement_flags: Vec<u8> = live_rows
            .iter()
            .map(|&(batch, row)| self.row_flags[batch][row])
            .collect();

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
        let (replacement_index, replacement_rows, replacement_index_entry_bytes) = {
            let keys = extract_key_columns(&compacted, key_cols)?;
            let timestamps = extract_column_as_timestamps(&compacted, time_col)?;
            let mut index = FxHashMap::default();
            let mut rows = 0usize;
            let mut index_entry_bytes = 0usize;
            for (row_idx, &ts) in timestamps.iter().enumerate() {
                insert_index_position(
                    &mut index,
                    &mut index_entry_bytes,
                    tuple_hash_at(&keys, row_idx).unwrap_or(NULL_TUPLE_HASH),
                    ts,
                    (0, row_idx),
                );
                rows += 1;
            }
            (index, rows, index_entry_bytes)
        };
        if replacement_rows != live_rows.len() {
            return Err(DbError::Pipeline(format!(
                "interval join compaction lost indexed rows: expected {}, rebuilt {replacement_rows}",
                live_rows.len()
            )));
        }

        let row_bytes = logical_row_bytes(&compacted)?;
        self.retained_rows = replacement_rows;
        self.retained_batch_bytes = compacted.get_array_memory_size();
        self.retained_batch_metadata_bytes = batch_metadata_charge(&compacted);
        self.row_size_vector_bytes = position_vector_charge(row_bytes.capacity());
        self.batches = vec![compacted];
        self.row_bytes = vec![row_bytes];
        self.row_flags = vec![replacement_flags];
        self.index = replacement_index;
        self.index_entry_bytes = replacement_index_entry_bytes;
        self.row_count = replacement_rows;
        Ok(())
    }

    fn accounted_state_bytes(&self) -> usize {
        self.retained_batch_bytes
            .saturating_add(self.retained_batch_metadata_bytes)
            .saturating_add(
                self.batches
                    .capacity()
                    .saturating_mul(std::mem::size_of::<RecordBatch>()),
            )
            .saturating_add(
                usize::from(self.batches.capacity() > 0).saturating_mul(HEAP_ALLOCATION_CHARGE),
            )
            .saturating_add(
                self.row_bytes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Vec<usize>>()),
            )
            .saturating_add(
                usize::from(self.row_bytes.capacity() > 0).saturating_mul(HEAP_ALLOCATION_CHARGE),
            )
            .saturating_add(self.row_size_vector_bytes)
            .saturating_add(
                self.row_flags
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Vec<u8>>()),
            )
            .saturating_add(
                usize::from(self.row_flags.capacity() > 0).saturating_mul(HEAP_ALLOCATION_CHARGE),
            )
            .saturating_add(self.row_flags.iter().fold(0usize, |bytes, flags| {
                bytes.saturating_add(flags.capacity())
            }))
            .saturating_add(self.index.capacity().saturating_mul(HASH_BUCKET_CHARGE))
            .saturating_add(
                usize::from(self.index.capacity() > 0).saturating_mul(HEAP_ALLOCATION_CHARGE),
            )
            .saturating_add(self.index_entry_bytes)
    }

    fn worst_case_input_growth(&self, batches: &[RecordBatch]) -> Result<usize, DbError> {
        let (rows, batch_bytes) = batches.iter().try_fold(
            (0usize, 0usize),
            |(rows, bytes), batch| -> Result<_, DbError> {
                let rows = rows.checked_add(batch.num_rows()).ok_or_else(|| {
                    DbError::BackpressureFail("interval join input row accounting overflow".into())
                })?;
                let bytes = bytes
                    .checked_add(batch.get_array_memory_size())
                    .and_then(|total| total.checked_add(batch_metadata_charge(batch)))
                    .and_then(|total| total.checked_add(batch.num_rows()))
                    .and_then(|total| total.checked_add(std::mem::size_of::<Vec<u8>>()))
                    .and_then(|total| total.checked_add(HEAP_ALLOCATION_CHARGE))
                    .ok_or_else(|| {
                        DbError::BackpressureFail(
                            "interval join input byte accounting overflow".into(),
                        )
                    })?;
                Ok((rows, bytes))
            },
        )?;
        if rows == 0 {
            return Ok(0);
        }
        let final_hash_entries = self.index.len().checked_add(rows).ok_or_else(|| {
            DbError::BackpressureFail("interval join hash entry accounting overflow".into())
        })?;
        let worst_hash_bytes = final_hash_entries
            .checked_mul(3 * HASH_BUCKET_CHARGE)
            .ok_or_else(|| {
                DbError::BackpressureFail("interval join hash index accounting overflow".into())
            })?;
        let current_hash_bytes = self.index.capacity().saturating_mul(HASH_BUCKET_CHARGE);
        let hash_growth = worst_hash_bytes.saturating_sub(current_hash_bytes);
        let row_growth = rows
            .checked_mul(WORST_CASE_ROW_NON_HASH_CHARGE)
            .ok_or_else(|| {
                DbError::BackpressureFail("interval join row index accounting overflow".into())
            })?;
        batch_bytes
            .checked_add(hash_growth)
            .and_then(|bytes| bytes.checked_add(row_growth))
            .ok_or_else(|| {
                DbError::BackpressureFail("interval join state growth accounting overflow".into())
            })
    }

    const fn is_compact(&self) -> bool {
        self.retained_rows == self.row_count
    }
}

/// Per-query interval join state.
#[derive(Clone)]
pub(crate) struct IntervalJoinState {
    pub(crate) left: SideState,
    pub(crate) right: SideState,
    left_evicted_cutoff: i64,
    right_evicted_cutoff: i64,
    left_schema: Option<SchemaRef>,
    right_schema: Option<SchemaRef>,
    output_schema: Option<SchemaRef>,
}

impl IntervalJoinState {
    pub(crate) fn new() -> Self {
        Self {
            left: SideState::new(),
            right: SideState::new(),
            left_evicted_cutoff: i64::MIN,
            right_evicted_cutoff: i64::MIN,
            left_schema: None,
            right_schema: None,
            output_schema: None,
        }
    }

    pub(crate) fn accounted_state_bytes(&self) -> usize {
        let schema_bytes = |schema: &SchemaRef| {
            BATCH_METADATA_CHARGE.saturating_add(schema.fields().iter().fold(
                0usize,
                |bytes, field| {
                    bytes
                        .saturating_add(ARRAY_METADATA_CHARGE)
                        .saturating_add(field.name().len())
                },
            ))
        };
        std::mem::size_of::<Self>()
            .saturating_add(HEAP_ALLOCATION_CHARGE)
            .saturating_add(self.left.accounted_state_bytes())
            .saturating_add(self.right.accounted_state_bytes())
            .saturating_add(self.left_schema.as_ref().map_or(0, schema_bytes))
            .saturating_add(self.right_schema.as_ref().map_or(0, schema_bytes))
            .saturating_add(self.output_schema.as_ref().map_or(0, schema_bytes))
    }

    pub(crate) fn preflight_input_growth(
        &self,
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
        max_state_bytes: usize,
    ) -> Result<(), DbError> {
        let growth = self
            .left
            .worst_case_input_growth(left_batches)?
            .checked_add(self.right.worst_case_input_growth(right_batches)?)
            .ok_or_else(|| {
                DbError::BackpressureFail("interval join state growth accounting overflow".into())
            })?;
        let projected = self
            .accounted_state_bytes()
            .checked_add(growth)
            .ok_or_else(|| {
                DbError::BackpressureFail("interval join state budget accounting overflow".into())
            })?;
        if projected > max_state_bytes {
            return Err(DbError::BackpressureFail(format!(
                "interval join input has a worst-case retained-state charge of {projected} bytes; limit is {max_state_bytes} bytes"
            )));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) const fn buffered_rows(&self) -> (usize, usize) {
        (self.left.row_count, self.right.row_count)
    }

    pub(crate) fn seed_input_schemas(
        &mut self,
        left: SchemaRef,
        right: SchemaRef,
        config: &StreamJoinConfig,
    ) -> Result<(), DbError> {
        for (side, current, declared) in [
            ("left", self.left_schema.as_ref(), &left),
            ("right", self.right_schema.as_ref(), &right),
        ] {
            if current.is_some_and(|schema| schema.as_ref() != declared.as_ref()) {
                return Err(DbError::SchemaMismatch(format!(
                    "interval join {side} declared schema changed"
                )));
            }
        }
        self.left_schema = Some(left);
        self.right_schema = Some(right);
        self.cache_input_schemas(None, None, config)
    }

    fn cache_input_schemas(
        &mut self,
        left: Option<SchemaRef>,
        right: Option<SchemaRef>,
        config: &StreamJoinConfig,
    ) -> Result<(), DbError> {
        if let Some(left) = left {
            if self
                .left_schema
                .as_ref()
                .is_some_and(|schema| schema.as_ref() != left.as_ref())
            {
                return Err(DbError::SchemaMismatch(
                    "interval join left schema changed".into(),
                ));
            }
            self.left_schema = Some(left);
        }
        if let Some(right) = right {
            if self
                .right_schema
                .as_ref()
                .is_some_and(|schema| schema.as_ref() != right.as_ref())
            {
                return Err(DbError::SchemaMismatch(
                    "interval join right schema changed".into(),
                ));
            }
            self.right_schema = Some(right);
        }
        if let (Some(left), Some(right)) = (&self.left_schema, &self.right_schema) {
            for (left_key, right_key) in config.left_keys.iter().zip(&config.right_keys) {
                let left_field = left.field_with_name(left_key).map_err(|error| {
                    DbError::SchemaMismatch(format!(
                        "interval join left key '{left_key}' is missing: {error}"
                    ))
                })?;
                let right_field = right.field_with_name(right_key).map_err(|error| {
                    DbError::SchemaMismatch(format!(
                        "interval join right key '{right_key}' is missing: {error}"
                    ))
                })?;
                if left_field.data_type() != right_field.data_type() {
                    return Err(DbError::SchemaMismatch(format!(
                        "interval join key types differ for '{left_key}' and '{right_key}': {} versus {}",
                        left_field.data_type(),
                        right_field.data_type()
                    )));
                }
                if !matches!(left_field.data_type(), DataType::Utf8 | DataType::Int64) {
                    return Err(DbError::SchemaMismatch(format!(
                        "interval join key '{left_key}' has unsupported type {}",
                        left_field.data_type()
                    )));
                }
            }
            self.output_schema = Some(build_output_schema(left, right, config));
        }
        Ok(())
    }

    fn finalize_closed_rows(
        &mut self,
        config: &StreamJoinConfig,
        left_watermark: i64,
        right_watermark: i64,
        force: bool,
        output_budget: &mut IntervalJoinOutputBudget,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let bound_ms = i64::try_from(config.time_bound.as_millis()).map_err(|_| {
            DbError::InvalidOperation(
                "interval join time bound exceeds the supported millisecond range".into(),
            )
        })?;
        let left_cutoff = self
            .left_evicted_cutoff
            .max(right_watermark.saturating_sub(bound_ms));
        let right_cutoff = self.right_evicted_cutoff.max(left_watermark);
        let close_left = force || left_cutoff > self.left_evicted_cutoff;
        let close_right = force || right_cutoff > self.right_evicted_cutoff;

        let emit_left = matches!(
            config.join_type,
            JoinType::Left | JoinType::Full | JoinType::LeftAnti
        );
        let emit_right = matches!(
            config.join_type,
            JoinType::Right | JoinType::Full | JoinType::RightAnti
        );
        let mut remaining_rows = MAX_CYCLE_OUTPUT_ROWS.saturating_sub(output_budget.emitted_rows);
        let left_positions = if close_left && emit_left {
            self.left
                .unmatched_positions_before(left_cutoff, remaining_rows)?
        } else {
            Vec::new()
        };
        remaining_rows = remaining_rows.saturating_sub(left_positions.len());
        let right_positions = if close_right && emit_right {
            self.right
                .unmatched_positions_before(right_cutoff, remaining_rows)?
        } else {
            Vec::new()
        };

        let mut rows = Vec::new();
        let mut emitted_left = Vec::new();
        let mut emitted_right = Vec::new();
        if emit_left {
            for &(batch, row) in &left_positions {
                let flags = self.left.row_flags[batch][row];
                if flags & (ROW_MATCHED | ROW_EMITTED) == 0 {
                    rows.push(JoinOutputRow {
                        left: Some((batch, row)),
                        right: None,
                    });
                    emitted_left.push((batch, row));
                }
            }
        }
        if emit_right {
            for &(batch, row) in &right_positions {
                let flags = self.right.row_flags[batch][row];
                if flags & (ROW_MATCHED | ROW_EMITTED) == 0 {
                    rows.push(JoinOutputRow {
                        left: None,
                        right: Some((batch, row)),
                    });
                    emitted_right.push((batch, row));
                }
            }
        }

        let mut output = Vec::new();
        if !rows.is_empty() {
            let left_schema = self.left_schema.as_ref().ok_or_else(|| {
                DbError::SchemaMismatch(
                    "interval join cannot finalize rows before the left input schema is known"
                        .into(),
                )
            })?;
            let right_schema = self.right_schema.as_ref().ok_or_else(|| {
                DbError::SchemaMismatch(
                    "interval join cannot finalize rows before the right input schema is known"
                        .into(),
                )
            })?;
            let output_schema = self.output_schema.as_ref().ok_or_else(|| {
                DbError::SchemaMismatch("interval join output schema is not initialized".into())
            })?;
            flush_output_rows(
                &mut rows,
                output_schema,
                left_schema,
                right_schema,
                config.join_type,
                &self.left.batches,
                &self.right.batches,
                &self.left.row_bytes,
                &self.right.row_bytes,
                &mut output,
                output_budget,
            )?;
            for (batch, row) in emitted_left {
                self.left.row_flags[batch][row] |= ROW_EMITTED;
            }
            for (batch, row) in emitted_right {
                self.right.row_flags[batch][row] |= ROW_EMITTED;
            }
        }

        self.evict_closed_rows(config, left_watermark, right_watermark, force)?;
        Ok(output)
    }

    fn evict_closed_rows(
        &mut self,
        config: &StreamJoinConfig,
        left_watermark: i64,
        right_watermark: i64,
        force: bool,
    ) -> Result<(), DbError> {
        let bound_ms = i64::try_from(config.time_bound.as_millis()).map_err(|_| {
            DbError::InvalidOperation(
                "interval join time bound exceeds the supported millisecond range".to_string(),
            )
        })?;
        let left_cutoff = right_watermark.saturating_sub(bound_ms);
        if force || left_cutoff > self.left_evicted_cutoff {
            let left_cutoff = left_cutoff.max(self.left_evicted_cutoff);
            self.left
                .evict_before(left_cutoff, &config.left_keys, &config.left_time_column)?;
            self.left_evicted_cutoff = left_cutoff;
        }
        let right_cutoff = left_watermark;
        if force || right_cutoff > self.right_evicted_cutoff {
            let right_cutoff = right_cutoff.max(self.right_evicted_cutoff);
            self.right
                .evict_before(right_cutoff, &config.right_keys, &config.right_time_column)?;
            self.right_evicted_cutoff = right_cutoff;
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn validate_vnode(
        &self,
        vnode: u32,
        vnode_count: u32,
        config: &StreamJoinConfig,
    ) -> Result<(), DbError> {
        for (side, batches, key_names) in [
            ("left", &self.left.batches, config.left_keys.as_slice()),
            ("right", &self.right.batches, config.right_keys.as_slice()),
        ] {
            for batch in batches {
                let key_indices = key_names
                    .iter()
                    .map(|key_name| {
                        batch.schema().index_of(key_name).map_err(|error| {
                            DbError::Checkpoint(format!(
                                "interval join vnode {vnode} {side} key '{key_name}' is missing: {error}"
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let routed = laminar_core::shuffle::row_vnodes(batch, &key_indices, vnode_count)
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "interval join vnode {vnode} {side} key validation failed: {error}"
                        ))
                    })?;
                if routed.iter().any(|actual| *actual != vnode) {
                    return Err(DbError::Checkpoint(format!(
                        "interval join vnode {vnode} {side} checkpoint contains a row for another vnode"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Removes dead rows before serialization; already-dense batches are encoded in place.
    pub(crate) fn snapshot_checkpoint(
        &mut self,
        config: &StreamJoinConfig,
        max_encoded_bytes: usize,
    ) -> Result<JoinStateCheckpoint, DbError> {
        if !self.left.batches.is_empty() && !self.left.is_compact() {
            self.left
                .compact(&config.left_keys, &config.left_time_column)?;
        }
        if !self.right.batches.is_empty() && !self.right.is_compact() {
            self.right
                .compact(&config.right_keys, &config.right_time_column)?;
        }

        fn encode_side(
            side: &str,
            batches: &[RecordBatch],
            remaining: &mut usize,
        ) -> Result<Vec<Vec<u8>>, DbError> {
            let mut encoded = Vec::new();
            encoded.try_reserve_exact(batches.len()).map_err(|_| {
                DbError::Checkpoint(format!(
                    "interval join {side} checkpoint batch roster cannot be reserved"
                ))
            })?;
            for batch in batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                let ipc = laminar_core::serialization::serialize_batches_stream_bounded(
                    batch.schema().as_ref(),
                    std::iter::once(batch),
                    *remaining,
                )
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "interval join {side} batch serialization within the cumulative checkpoint limit: {error}"
                    ))
                })?;
                *remaining = remaining.checked_sub(ipc.capacity()).ok_or_else(|| {
                    DbError::Checkpoint(
                        "interval join checkpoint encoded byte accounting overflow".into(),
                    )
                })?;
                encoded.push(ipc);
            }
            Ok(encoded)
        }

        let config_bytes = config
            .left_keys
            .iter()
            .chain(&config.right_keys)
            .map(String::capacity)
            .chain([
                config.left_time_column.capacity(),
                config.right_time_column.capacity(),
                config.left_table.capacity(),
                config.right_table.capacity(),
            ])
            .try_fold(0usize, |total, bytes| total.checked_add(bytes))
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "interval join checkpoint configuration accounting overflow".into(),
                )
            })?;
        let flag_bytes = self
            .left
            .row_flags
            .iter()
            .chain(&self.right.row_flags)
            .try_fold(0usize, |total, flags| total.checked_add(flags.capacity()))
            .ok_or_else(|| {
                DbError::Checkpoint("interval join checkpoint flag accounting overflow".into())
            })?;
        let retained_metadata = config_bytes.checked_add(flag_bytes).ok_or_else(|| {
            DbError::Checkpoint("interval join checkpoint metadata accounting overflow".into())
        })?;
        let mut remaining = max_encoded_bytes.checked_sub(retained_metadata).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join retained metadata exceeds its {max_encoded_bytes}-byte checkpoint limit"
            ))
        })?;
        let bound_ms = i64::try_from(config.time_bound.as_millis()).map_err(|_| {
            DbError::InvalidOperation(
                "interval join time bound exceeds the supported millisecond range".into(),
            )
        })?;
        let left_batches_ipc = encode_side("left", &self.left.batches, &mut remaining)?;
        let right_batches_ipc = encode_side("right", &self.right.batches, &mut remaining)?;

        let left_buffer_rows = u64::try_from(self.left.row_count).map_err(|_| {
            DbError::Checkpoint("interval join left row count does not fit u64".into())
        })?;
        let right_buffer_rows = u64::try_from(self.right.row_count).map_err(|_| {
            DbError::Checkpoint("interval join right row count does not fit u64".into())
        })?;
        Ok(JoinStateCheckpoint {
            join_type: join_type_tag(config.join_type),
            left_keys: config.left_keys.clone(),
            right_keys: config.right_keys.clone(),
            left_time_column: config.left_time_column.clone(),
            right_time_column: config.right_time_column.clone(),
            left_table: config.left_table.clone(),
            right_table: config.right_table.clone(),
            bound_ms,
            left_buffer_rows,
            right_buffer_rows,
            left_batches: left_batches_ipc,
            right_batches: right_batches_ipc,
            left_evicted_cutoff: self.left_evicted_cutoff,
            right_evicted_cutoff: self.right_evicted_cutoff,
            left_row_flags: self.left.row_flags.clone(),
            right_row_flags: self.right.row_flags.clone(),
        })
    }

    /// Restores from a checkpoint, rebuilding the index from deserialized batches.
    pub(crate) fn from_checkpoint(
        cp: &JoinStateCheckpoint,
        config: &StreamJoinConfig,
        max_state_bytes: usize,
    ) -> Result<Self, DbError> {
        if max_state_bytes == 0 {
            return Err(DbError::Checkpoint(
                "interval join restore state limit must be greater than zero".into(),
            ));
        }
        let bound_ms = i64::try_from(config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(
                "interval join configured time bound exceeds the supported millisecond range"
                    .into(),
            )
        })?;
        if cp.join_type != join_type_tag(config.join_type)
            || cp.left_keys != config.left_keys
            || cp.right_keys != config.right_keys
            || cp.left_time_column != config.left_time_column
            || cp.right_time_column != config.right_time_column
            || cp.left_table != config.left_table
            || cp.right_table != config.right_table
            || cp.bound_ms != bound_ms
        {
            return Err(DbError::Checkpoint(
                "interval join checkpoint configuration does not match the restored operator"
                    .into(),
            ));
        }
        let expected_left = usize::try_from(cp.left_buffer_rows).map_err(|_| {
            DbError::Checkpoint("interval join left row count does not fit usize".into())
        })?;
        let expected_right = usize::try_from(cp.right_buffer_rows).map_err(|_| {
            DbError::Checkpoint("interval join right row count does not fit usize".into())
        })?;
        let expected_rows = expected_left.checked_add(expected_right).ok_or_else(|| {
            DbError::Checkpoint("interval join checkpoint row-count overflow".into())
        })?;
        let worst_case_row_bytes = expected_rows
            .checked_mul(RESTORE_WORST_CASE_ROW_CHARGE)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "interval join checkpoint worst-case row accounting overflow".into(),
                )
            })?;
        if worst_case_row_bytes > max_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join checkpoint declares {expected_rows} rows whose worst-case decoded index charge exceeds the {max_state_bytes}-byte restore limit"
            )));
        }
        for (side, batch_count, expected) in [
            ("left", cp.left_batches.len(), expected_left),
            ("right", cp.right_batches.len(), expected_right),
        ] {
            if batch_count > expected {
                return Err(DbError::Checkpoint(format!(
                    "interval join {side} checkpoint has {batch_count} non-empty batches for {expected} rows"
                )));
            }
        }

        fn decode_side(
            side: &str,
            ipc_batches: &[Vec<u8>],
            row_flags: &[Vec<u8>],
            expected_rows: usize,
            decoded_charge: &mut usize,
            max_state_bytes: usize,
        ) -> Result<Vec<RecordBatch>, DbError> {
            if row_flags.len() != ipc_batches.len() {
                return Err(DbError::Checkpoint(format!(
                    "interval join {side} checkpoint flag roster does not match its batch roster"
                )));
            }
            let mut decoded = Vec::new();
            decoded.try_reserve_exact(ipc_batches.len()).map_err(|_| {
                DbError::Checkpoint(format!(
                    "interval join {side} checkpoint batch roster cannot be reserved"
                ))
            })?;
            let mut rows = 0usize;
            let mut schema: Option<SchemaRef> = None;
            for (ipc_bytes, flags) in ipc_batches.iter().zip(row_flags) {
                let batch = laminar_core::serialization::deserialize_batch_stream(ipc_bytes)
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "interval join {side} batch deserialization: {error}"
                        ))
                    })?;
                if batch.num_rows() == 0 {
                    return Err(DbError::Checkpoint(format!(
                        "interval join {side} checkpoint contains an empty batch"
                    )));
                }
                if flags.len() != batch.num_rows() || flags.iter().any(|flag| flag & !3 != 0) {
                    return Err(DbError::Checkpoint(format!(
                        "interval join {side} checkpoint has invalid per-row match state"
                    )));
                }
                if let Some(expected_schema) = schema.as_ref() {
                    if batch.schema().as_ref() != expected_schema.as_ref() {
                        return Err(DbError::Checkpoint(format!(
                            "interval join {side} checkpoint contains mixed schemas"
                        )));
                    }
                } else {
                    schema = Some(batch.schema());
                }
                rows = rows.checked_add(batch.num_rows()).ok_or_else(|| {
                    DbError::Checkpoint(format!("interval join {side} decoded row-count overflow"))
                })?;
                if rows > expected_rows {
                    return Err(DbError::Checkpoint(format!(
                        "interval join {side} checkpoint decoded more than its {expected_rows} declared rows"
                    )));
                }
                let row_charge = batch
                    .num_rows()
                    .checked_mul(RESTORE_WORST_CASE_ROW_CHARGE)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join {side} decoded row accounting overflow"
                        ))
                    })?;
                *decoded_charge = decoded_charge
                    .checked_add(batch.get_array_memory_size())
                    .and_then(|bytes| bytes.checked_add(batch_metadata_charge(&batch)))
                    .and_then(|bytes| bytes.checked_add(row_charge))
                    .ok_or_else(|| {
                        DbError::Checkpoint(
                            "interval join cumulative decoded byte accounting overflow".into(),
                        )
                    })?;
                if *decoded_charge > max_state_bytes {
                    return Err(DbError::Checkpoint(format!(
                        "interval join cumulative decoded state exceeds the {max_state_bytes}-byte restore limit before index reconstruction"
                    )));
                }
                decoded.push(batch);
            }
            if rows != expected_rows {
                return Err(DbError::Checkpoint(format!(
                    "interval join {side} checkpoint row-count mismatch: metadata={expected_rows}, decoded={rows}"
                )));
            }
            Ok(decoded)
        }

        let mut decoded_charge = std::mem::size_of::<Self>();
        if decoded_charge > max_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join state header exceeds the {max_state_bytes}-byte restore limit"
            )));
        }
        let left_batches = decode_side(
            "left",
            &cp.left_batches,
            &cp.left_row_flags,
            expected_left,
            &mut decoded_charge,
            max_state_bytes,
        )?;
        let right_batches = decode_side(
            "right",
            &cp.right_batches,
            &cp.right_row_flags,
            expected_right,
            &mut decoded_charge,
            max_state_bytes,
        )?;

        let mut state = Self::new();
        state.left_evicted_cutoff = cp.left_evicted_cutoff;
        state.right_evicted_cutoff = cp.right_evicted_cutoff;

        for (batch, flags) in left_batches.into_iter().zip(cp.left_row_flags.iter()) {
            let _ = state
                .left
                .add_batch(
                    &batch,
                    &config.left_keys,
                    &config.left_time_column,
                    retain_left_null_tuples(config.join_type),
                )
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "interval join left checkpoint index rebuild: {error}"
                    ))
                })?;
            *state.left.row_flags.last_mut().ok_or_else(|| {
                DbError::Checkpoint("interval join left row flags lost during restore".into())
            })? = flags.clone();
            state.left_schema = Some(batch.schema());
            if state.accounted_state_bytes() > max_state_bytes {
                return Err(DbError::Checkpoint(format!(
                    "interval join restored state exceeds the {max_state_bytes}-byte limit while rebuilding the left index"
                )));
            }
        }

        for (batch, flags) in right_batches.into_iter().zip(cp.right_row_flags.iter()) {
            let _ = state
                .right
                .add_batch(
                    &batch,
                    &config.right_keys,
                    &config.right_time_column,
                    retain_right_null_tuples(config.join_type),
                )
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "interval join right checkpoint index rebuild: {error}"
                    ))
                })?;
            *state.right.row_flags.last_mut().ok_or_else(|| {
                DbError::Checkpoint("interval join right row flags lost during restore".into())
            })? = flags.clone();
            state.right_schema = Some(batch.schema());
            if state.accounted_state_bytes() > max_state_bytes {
                return Err(DbError::Checkpoint(format!(
                    "interval join restored state exceeds the {max_state_bytes}-byte limit while rebuilding the right index"
                )));
            }
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

        if state.accounted_state_bytes() > max_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join restored state exceeds the {max_state_bytes}-byte limit"
            )));
        }

        Ok(state)
    }
}

fn output_sides(join_type: JoinType) -> (bool, bool) {
    match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti => (true, false),
        JoinType::RightSemi | JoinType::RightAnti => (false, true),
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => (true, true),
    }
}

fn retain_left_null_tuples(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left | JoinType::Full | JoinType::LeftAnti
    )
}

fn retain_right_null_tuples(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Right | JoinType::Full | JoinType::RightAnti
    )
}

/// Pair rows use suffixed right names; preserved-side-only rows keep their source names.
pub(crate) fn build_output_schema(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    config: &StreamJoinConfig,
) -> SchemaRef {
    let (include_left, include_right) = output_sides(config.join_type);
    let mut fields = Vec::new();
    if include_left {
        fields.extend(left_schema.fields().iter().map(|field| {
            let field = field.as_ref().clone();
            if matches!(config.join_type, JoinType::Right | JoinType::Full) {
                field.with_nullable(true)
            } else {
                field
            }
        }));
    }
    if include_right {
        fields.extend(right_schema.fields().iter().map(|field| {
            let field = field.as_ref().clone();
            if !include_left {
                return field;
            }
            let field = if matches!(config.join_type, JoinType::Left | JoinType::Full) {
                field.with_nullable(true)
            } else {
                field
            };
            let name = format!("{}_{}", field.name(), config.right_table);
            field.with_name(name)
        }));
    }
    Arc::new(Schema::new(fields))
}

#[derive(Clone, Copy)]
struct JoinOutputRow {
    left: Option<(usize, usize)>,
    right: Option<(usize, usize)>,
}

#[allow(clippy::too_many_arguments)]
fn flush_output_rows(
    rows: &mut Vec<JoinOutputRow>,
    output_schema: &SchemaRef,
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    join_type: JoinType,
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    left_row_bytes: &[Vec<usize>],
    right_row_bytes: &[Vec<usize>],
    out: &mut Vec<RecordBatch>,
    output_budget: &mut IntervalJoinOutputBudget,
) -> Result<(), DbError> {
    if rows.is_empty() {
        return Ok(());
    }

    let next_rows = output_budget.emitted_rows.saturating_add(rows.len());
    if next_rows > MAX_CYCLE_OUTPUT_ROWS {
        return Err(DbError::BackpressureFail(format!(
            "interval join cycle exceeded {MAX_CYCLE_OUTPUT_ROWS} output rows; narrow the event-time bound or reduce hot-key fanout"
        )));
    }
    let logical_bytes = rows.iter().try_fold(0usize, |total, row| {
        let left = row.left.map_or(Ok(0), |(batch, index)| {
            left_row_bytes
                .get(batch)
                .and_then(|rows| rows.get(index))
                .copied()
                .ok_or_else(|| {
                    DbError::Pipeline("interval join left output position is invalid".into())
                })
        })?;
        let right = row.right.map_or(Ok(0), |(batch, index)| {
            right_row_bytes
                .get(batch)
                .and_then(|rows| rows.get(index))
                .copied()
                .ok_or_else(|| {
                    DbError::Pipeline("interval join right output position is invalid".into())
                })
        })?;
        Ok::<_, DbError>(total.saturating_add(left).saturating_add(right))
    })?;
    let allocation_charge = logical_bytes.saturating_mul(2).saturating_add(
        rows.len()
            .saturating_mul(output_schema.fields().len())
            .saturating_mul(16),
    );
    if output_budget
        .emitted_bytes
        .saturating_add(allocation_charge)
        > MAX_CYCLE_OUTPUT_BYTES
    {
        return Err(DbError::BackpressureFail(format!(
            "interval join cycle would exceed {} MiB of output; narrow the event-time bound or reduce hot-key fanout",
            MAX_CYCLE_OUTPUT_BYTES / (1024 * 1024)
        )));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());
    let mut append_side = |side: &str,
                           schema: &SchemaRef,
                           batches: &[RecordBatch],
                           indices: &[(usize, usize)]|
     -> Result<(), DbError> {
        for (column, field) in schema.fields().iter().enumerate() {
            let nulls = new_null_array(field.data_type(), 1);
            let mut arrays: Vec<&dyn Array> = batches
                .iter()
                .map(|batch| batch.column(column).as_ref())
                .collect();
            arrays.push(nulls.as_ref());
            let array = arrow::compute::interleave(&arrays, indices).map_err(|error| {
                DbError::query_pipeline_arrow(&format!("interval join (interleave {side})"), &error)
            })?;
            columns.push(array);
        }
        Ok(())
    };
    let (include_left, include_right) = output_sides(join_type);
    if include_left {
        let null_batch = left_batches.len();
        let indices = rows
            .iter()
            .map(|row| row.left.unwrap_or((null_batch, 0)))
            .collect::<Vec<_>>();
        append_side("left", left_schema, left_batches, &indices)?;
    }
    if include_right {
        let null_batch = right_batches.len();
        let indices = rows
            .iter()
            .map(|row| row.right.unwrap_or((null_batch, 0)))
            .collect::<Vec<_>>();
        append_side("right", right_schema, right_batches, &indices)?;
    }

    let batch = RecordBatch::try_new(output_schema.clone(), columns)
        .map_err(|e| DbError::query_pipeline_arrow("interval join (result)", &e))?;
    if batch.num_rows() > 0 {
        let batch_bytes = batch.get_array_memory_size();
        let next_bytes = output_budget.emitted_bytes.saturating_add(batch_bytes);
        if next_bytes > MAX_CYCLE_OUTPUT_BYTES {
            return Err(DbError::BackpressureFail(format!(
                "interval join cycle exceeded {} MiB of output; narrow the event-time bound or reduce hot-key fanout",
                MAX_CYCLE_OUTPUT_BYTES / (1024 * 1024)
            )));
        }
        output_budget.emitted_rows = next_rows;
        output_budget.emitted_bytes = next_bytes;
        out.push(batch);
    }
    rows.clear();
    Ok(())
}

fn validate_append_only_input(
    side: &str,
    batches: &[RecordBatch],
    key_columns: &[String],
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
        let _ = extract_key_columns(batch, key_columns)?;
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
    cached: Option<&SchemaRef>,
    batches: &[RecordBatch],
) -> Result<Option<SchemaRef>, DbError> {
    let expected = cached
        .cloned()
        .or_else(|| retained.batches.first().map(RecordBatch::schema))
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

/// One cycle: new left rows probe all right; new right rows probe only old left.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(crate) fn execute_interval_join_cycle(
    state: &mut IntervalJoinState,
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    config: &StreamJoinConfig,
    left_admission_watermark: i64,
    right_admission_watermark: i64,
    left_watermark: i64,
    right_watermark: i64,
    max_state_bytes: usize,
    output_budget: &mut IntervalJoinOutputBudget,
) -> Result<Vec<RecordBatch>, DbError> {
    if config.left_keys.is_empty()
        || config.left_keys.len() != config.right_keys.len()
        || config.left_time_column.is_empty()
        || config.right_time_column.is_empty()
    {
        return Err(DbError::InvalidOperation(
            "interval join requires equal non-empty ordered key vectors and both event-time columns"
                .into(),
        ));
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

    let left_schema = validate_input_schemas(
        "left",
        &state.left,
        state.left_schema.as_ref(),
        left_batches,
    )?;
    let right_schema = validate_input_schemas(
        "right",
        &state.right,
        state.right_schema.as_ref(),
        right_batches,
    )?;

    validate_append_only_input(
        "left",
        left_batches,
        &config.left_keys,
        &config.left_time_column,
        left_admission_watermark,
    )?;
    validate_append_only_input(
        "right",
        right_batches,
        &config.right_keys,
        &config.right_time_column,
        right_admission_watermark,
    )?;
    state.cache_input_schemas(left_schema, right_schema, config)?;

    let has_left_input = left_batches.iter().any(|batch| batch.num_rows() > 0);
    let has_right_input = right_batches.iter().any(|batch| batch.num_rows() > 0);
    if !has_left_input && !has_right_input {
        return state
            .finalize_closed_rows(
                config,
                left_watermark,
                right_watermark,
                false,
                output_budget,
            )
            .map_err(partial_apply);
    }

    state
        .preflight_input_growth(left_batches, right_batches, max_state_bytes)
        .map_err(partial_apply)?;

    let mut result = Vec::new();
    let admitted = (|| -> Result<(), DbError> {
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

        // Buffer first so every output position points into retained state.
        let left_old_count = state.left.batches.len();
        let right_old_count = state.right.batches.len();
        let has_new_right = if let Some(rb) = new_right {
            state.right.add_batch(
                &rb,
                &config.right_keys,
                &config.right_time_column,
                retain_right_null_tuples(config.join_type),
            )?
        } else {
            false
        };
        let has_new_left = if let Some(lb) = new_left {
            state.left.add_batch(
                &lb,
                &config.left_keys,
                &config.left_time_column,
                retain_left_null_tuples(config.join_type),
            )?
        } else {
            false
        };
        let new_left_batch_idx = left_old_count;
        let new_right_batch_idx = right_old_count;

        let left_key_cols: Vec<Vec<KeyColumn<'_>>> = state
            .left
            .batches
            .iter()
            .map(|batch| extract_key_columns(batch, &config.left_keys))
            .collect::<Result<_, _>>()?;
        let right_key_cols: Vec<Vec<KeyColumn<'_>>> = state
            .right
            .batches
            .iter()
            .map(|batch| extract_key_columns(batch, &config.right_keys))
            .collect::<Result<_, _>>()?;
        let empty_left = Arc::new(Schema::empty());
        let empty_right = Arc::new(Schema::empty());
        let left_schema = state.left_schema.as_ref().unwrap_or(&empty_left);
        let right_schema = state.right_schema.as_ref().unwrap_or(&empty_right);
        let fallback_output = state
            .output_schema
            .is_none()
            .then(|| build_output_schema(left_schema, right_schema, config));
        let output_schema = state
            .output_schema
            .as_ref()
            .or(fallback_output.as_ref())
            .expect("interval join fallback output schema constructed");
        let mut output_rows = Vec::new();

        macro_rules! admit_match {
            ($left_batch:expr, $left_row:expr, $right_batch:expr, $right_row:expr) => {{
                state.left.row_flags[$left_batch][$left_row] |= ROW_MATCHED;
                state.right.row_flags[$right_batch][$right_row] |= ROW_MATCHED;
                match config.join_type {
                    JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                        output_rows.push(JoinOutputRow {
                            left: Some(($left_batch, $left_row)),
                            right: Some(($right_batch, $right_row)),
                        });
                    }
                    JoinType::LeftSemi => {
                        if state.left.row_flags[$left_batch][$left_row] & ROW_EMITTED == 0 {
                            state.left.row_flags[$left_batch][$left_row] |= ROW_EMITTED;
                            output_rows.push(JoinOutputRow {
                                left: Some(($left_batch, $left_row)),
                                right: None,
                            });
                        }
                    }
                    JoinType::RightSemi => {
                        if state.right.row_flags[$right_batch][$right_row] & ROW_EMITTED == 0 {
                            state.right.row_flags[$right_batch][$right_row] |= ROW_EMITTED;
                            output_rows.push(JoinOutputRow {
                                left: None,
                                right: Some(($right_batch, $right_row)),
                            });
                        }
                    }
                    JoinType::LeftAnti | JoinType::RightAnti => {}
                }
                if output_rows.len() >= EMIT_THRESHOLD {
                    flush_output_rows(
                        &mut output_rows,
                        output_schema,
                        left_schema,
                        right_schema,
                        config.join_type,
                        &state.left.batches,
                        &state.right.batches,
                        &state.left.row_bytes,
                        &state.right.row_bytes,
                        &mut result,
                        output_budget,
                    )?;
                }
            }};
        }

        if has_new_left {
            let lb_kc = &left_key_cols[new_left_batch_idx];
            let lb_ts = extract_column_as_timestamps(
                &state.left.batches[new_left_batch_idx],
                &config.left_time_column,
            )?;
            for (row_idx, &left_ts) in lb_ts.iter().enumerate() {
                let Some(key_hash) = tuple_hash_at(lb_kc, row_idx) else {
                    continue;
                };
                let low = left_ts;
                let high = left_ts.saturating_add(bound_ms);
                if let Some(times) = state.right.index.get(&key_hash) {
                    'left_row_matches: for entries in
                        times.range(low..=high).map(|(_, entries)| entries)
                    {
                        for &(r_batch, r_row) in entries {
                            if matches!(config.join_type, JoinType::RightSemi | JoinType::RightAnti)
                                && state.right.row_flags[r_batch][r_row] & ROW_MATCHED != 0
                            {
                                continue;
                            }
                            if !tuples_equal(lb_kc, row_idx, &right_key_cols[r_batch], r_row) {
                                continue;
                            }
                            admit_match!(new_left_batch_idx, row_idx, r_batch, r_row);
                            if matches!(config.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
                                break 'left_row_matches;
                            }
                        }
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
                let Some(key_hash) = tuple_hash_at(rb_kc, row_idx) else {
                    continue;
                };
                let low = right_ts.saturating_sub(bound_ms);
                let high = right_ts;
                if let Some(times) = state.left.index.get(&key_hash) {
                    'right_row_matches: for entries in
                        times.range(low..=high).map(|(_, entries)| entries)
                    {
                        for &(l_batch, l_row) in entries {
                            if l_batch >= left_old_count {
                                continue;
                            }
                            if matches!(config.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
                                && state.left.row_flags[l_batch][l_row] & ROW_MATCHED != 0
                            {
                                continue;
                            }
                            if !tuples_equal(&left_key_cols[l_batch], l_row, rb_kc, row_idx) {
                                continue;
                            }
                            admit_match!(l_batch, l_row, new_right_batch_idx, row_idx);
                            if matches!(config.join_type, JoinType::RightSemi | JoinType::RightAnti)
                            {
                                break 'right_row_matches;
                            }
                        }
                    }
                }
            }
        }

        flush_output_rows(
            &mut output_rows,
            output_schema,
            left_schema,
            right_schema,
            config.join_type,
            &state.left.batches,
            &state.right.batches,
            &state.left.row_bytes,
            &state.right.row_bytes,
            &mut result,
            output_budget,
        )?;
        Ok(())
    })();
    admitted.map_err(partial_apply)?;
    let closed = state
        .finalize_closed_rows(config, left_watermark, right_watermark, true, output_budget)
        .map_err(partial_apply)?;
    result.extend(closed);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use std::time::Duration;

    fn execute_interval_join_cycle(
        state: &mut IntervalJoinState,
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
        config: &StreamJoinConfig,
        left_watermark: i64,
        right_watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let mut output_budget = IntervalJoinOutputBudget::default();
        super::execute_interval_join_cycle(
            state,
            left_batches,
            right_batches,
            config,
            left_watermark,
            right_watermark,
            left_watermark,
            right_watermark,
            usize::MAX,
            &mut output_budget,
        )
    }

    fn make_config() -> StreamJoinConfig {
        StreamJoinConfig {
            join_type: JoinType::Inner,
            left_keys: vec!["id".to_string()],
            right_keys: vec!["id".to_string()],
            left_time_column: "ts".to_string(),
            right_time_column: "ts".to_string(),
            left_table: "left_stream".to_string(),
            right_table: "right_stream".to_string(),
            time_bound: Duration::from_millis(100),
        }
    }

    fn left_batch(ids: &[&str], timestamps: &[i64], values: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn right_batch(ids: &[&str], timestamps: &[i64], amounts: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("amount", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(amounts.to_vec())),
            ],
        )
        .unwrap()
    }

    fn composite_batch(
        right: bool,
        ids: &[Option<&str>],
        regions: &[Option<i64>],
        timestamps: &[i64],
    ) -> RecordBatch {
        let value_name = if right { "amount" } else { "price" };
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("region", DataType::Int64, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(value_name, DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(Int64Array::from(regions.to_vec())),
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(vec![1.0; ids.len()])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn all_bounded_join_kinds_have_final_watermark_semantics() {
        for (join_type, expected_rows, expected_columns) in [
            (JoinType::Inner, 1, 6),
            (JoinType::Left, 2, 6),
            (JoinType::Right, 2, 6),
            (JoinType::Full, 3, 6),
            (JoinType::LeftSemi, 1, 3),
            (JoinType::RightSemi, 1, 3),
            (JoinType::LeftAnti, 1, 3),
            (JoinType::RightAnti, 1, 3),
        ] {
            let mut config = make_config();
            config.join_type = join_type;
            let left = left_batch(&["A", "B"], &[100, 100], &[1.0, 2.0]);
            let right = right_batch(&["A", "C"], &[110, 110], &[3.0, 4.0]);
            let mut state = IntervalJoinState::new();
            state
                .seed_input_schemas(left.schema(), right.schema(), &config)
                .unwrap();

            let mut output =
                execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();
            output.extend(
                execute_interval_join_cycle(&mut state, &[], &[], &config, 1_000, 1_000).unwrap(),
            );

            assert_eq!(
                output.iter().map(RecordBatch::num_rows).sum::<usize>(),
                expected_rows,
                "{join_type:?}"
            );
            assert!(
                output
                    .iter()
                    .all(|batch| batch.num_columns() == expected_columns),
                "{join_type:?}"
            );
            assert_eq!(state.buffered_rows(), (0, 0), "{join_type:?}");

            let schema = build_output_schema(
                state.left_schema.as_ref().unwrap(),
                state.right_schema.as_ref().unwrap(),
                &config,
            );
            if matches!(join_type, JoinType::Left | JoinType::Full) {
                assert!(schema.fields()[3..].iter().all(|field| field.is_nullable()));
            }
            if matches!(join_type, JoinType::Right | JoinType::Full) {
                assert!(schema.fields()[..3].iter().all(|field| field.is_nullable()));
            }
        }
    }

    #[test]
    fn current_batch_rows_are_admitted_before_its_watermark_closes_them() {
        let config = make_config();
        let mut state = IntervalJoinState::new();
        let output = super::execute_interval_join_cycle(
            &mut state,
            &[left_batch(&["A"], &[100], &[1.0])],
            &[right_batch(&["A"], &[110], &[2.0])],
            &config,
            i64::MIN,
            i64::MIN,
            300,
            300,
            usize::MAX,
            &mut IntervalJoinOutputBudget::default(),
        )
        .unwrap();

        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(state.buffered_rows(), (0, 0));

        let error = super::execute_interval_join_cycle(
            &mut state,
            &[left_batch(&["late"], &[100], &[1.0])],
            &[],
            &config,
            300,
            300,
            300,
            300,
            usize::MAX,
            &mut IntervalJoinOutputBudget::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("below closed cutoff 300"));
        assert_eq!(state.buffered_rows(), (0, 0));
    }

    #[test]
    fn lagging_input_uses_its_own_admission_watermark_before_cross_side_closure() {
        let mut config = make_config();
        config.join_type = JoinType::Left;
        let mut state = IntervalJoinState::new();
        state
            .seed_input_schemas(
                left_batch(&["schema"], &[0], &[0.0]).schema(),
                right_batch(&["schema"], &[0], &[0.0]).schema(),
                &config,
            )
            .unwrap();
        execute_interval_join_cycle(&mut state, &[], &[], &config, 0, 300).unwrap();
        assert_eq!(state.left_evicted_cutoff, 200);

        let output = super::execute_interval_join_cycle(
            &mut state,
            &[left_batch(&["valid"], &[150], &[1.0])],
            &[],
            &config,
            0,
            300,
            0,
            300,
            usize::MAX,
            &mut IntervalJoinOutputBudget::default(),
        )
        .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(state.buffered_rows(), (0, 0));
    }

    #[test]
    fn composite_keys_match_in_order_and_null_tuples_never_match() {
        let mut config = make_config();
        config.left_keys = vec!["id".into(), "region".into()];
        config.right_keys = config.left_keys.clone();
        let left = composite_batch(
            false,
            &[Some("A"), Some("A"), None, Some("B")],
            &[Some(1), Some(2), Some(1), None],
            &[100; 4],
        );
        let right = composite_batch(
            true,
            &[Some("A"), Some("A"), None, Some("B")],
            &[Some(1), Some(3), Some(1), None],
            &[110; 4],
        );

        let mut inner = IntervalJoinState::new();
        inner
            .seed_input_schemas(left.schema(), right.schema(), &config)
            .unwrap();
        let output = execute_interval_join_cycle(
            &mut inner,
            std::slice::from_ref(&left),
            std::slice::from_ref(&right),
            &config,
            0,
            0,
        )
        .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

        config.join_type = JoinType::LeftAnti;
        let mut anti = IntervalJoinState::new();
        anti.seed_input_schemas(left.schema(), right.schema(), &config)
            .unwrap();
        assert!(
            execute_interval_join_cycle(&mut anti, &[left], &[right], &config, 0, 0,)
                .unwrap()
                .is_empty()
        );
        let output =
            execute_interval_join_cycle(&mut anti, &[], &[], &config, 1_000, 1_000).unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    }

    #[test]
    fn checkpoint_preserves_semi_first_match_emission() {
        let mut config = make_config();
        config.join_type = JoinType::LeftSemi;
        let mut state = IntervalJoinState::new();
        let first = execute_interval_join_cycle(
            &mut state,
            &[left_batch(&["A"], &[100], &[1.0])],
            &[right_batch(&["A"], &[110], &[1.0])],
            &config,
            0,
            0,
        )
        .unwrap();
        assert_eq!(first.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let checkpoint = state
            .snapshot_checkpoint(&config, crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
            .unwrap();
        let mut incompatible = config.clone();
        incompatible.join_type = JoinType::RightSemi;
        let error = IntervalJoinState::from_checkpoint(
            &checkpoint,
            &incompatible,
            crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        )
        .err()
        .expect("join-state semantics must be checkpoint-bound");
        assert!(error.to_string().contains("configuration does not match"));
        let mut restored = IntervalJoinState::from_checkpoint(
            &checkpoint,
            &config,
            crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        )
        .unwrap();
        let repeated = execute_interval_join_cycle(
            &mut restored,
            &[],
            &[right_batch(&["A"], &[120], &[2.0])],
            &config,
            0,
            0,
        )
        .unwrap();
        assert!(repeated.is_empty());
    }

    #[test]
    fn test_basic_inner_join_same_cycle() {
        let config = make_config();
        let mut state = IntervalJoinState::new();

        let left = left_batch(&["A", "B"], &[100, 200], &[10.0, 20.0]);
        let right = right_batch(&["A", "B"], &[110, 250], &[1.0, 2.0]);

        let result =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();

        // Both right timestamps fall between the matching left timestamp and left + 100ms.
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
        assert_eq!(result[0].num_rows(), 1);
    }

    #[test]
    fn test_time_bound_enforcement() {
        let config = make_config(); // time_bound = 100ms
        let mut state = IntervalJoinState::new();

        let left = left_batch(&["A"], &[100], &[10.0]);
        let right = right_batch(&["A", "A"], &[50, 300], &[1.0, 2.0]);

        let result =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();
        assert!(result.is_empty()); // Before the left timestamp and after left + bound.
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

        // A@100 matches A@110 and B@100 matches B@110.
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
            .snapshot_checkpoint(&config, crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
            .unwrap();
        assert!(cp.left_buffer_rows > 0);
        assert!(cp.right_buffer_rows > 0);

        // Restore
        let mut restored = IntervalJoinState::from_checkpoint(
            &cp,
            &config,
            crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
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
            .snapshot_checkpoint(&config, crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
            .unwrap();
        checkpoint.left_buffer_rows += 1;

        let error = IntervalJoinState::from_checkpoint(
            &checkpoint,
            &config,
            crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        )
        .err()
        .expect("corrupt row-count metadata must fail restore");
        assert!(error.to_string().contains("row-count mismatch"));
    }

    #[test]
    fn restored_frontier_rejects_a_genuinely_late_outer_row() {
        let mut config = make_config();
        config.join_type = JoinType::Left;
        let mut state = IntervalJoinState::new();
        execute_interval_join_cycle(&mut state, &[], &[], &config, 300, 300).unwrap();
        let checkpoint = state
            .snapshot_checkpoint(&config, crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
            .unwrap();
        let mut restored = IntervalJoinState::from_checkpoint(
            &checkpoint,
            &config,
            crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        )
        .unwrap();
        restored
            .seed_input_schemas(
                left_batch(&["schema"], &[300], &[1.0]).schema(),
                right_batch(&["schema"], &[300], &[1.0]).schema(),
                &config,
            )
            .unwrap();

        let error = super::execute_interval_join_cycle(
            &mut restored,
            &[left_batch(&["late"], &[150], &[1.0])],
            &[],
            &config,
            300,
            300,
            0,
            0,
            usize::MAX,
            &mut IntervalJoinOutputBudget::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("below closed cutoff 300"));
        assert_eq!(restored.buffered_rows(), (0, 0));
    }

    #[test]
    fn managed_accounting_charges_index_topology() {
        let mut shared_timestamp = SideState::new();
        shared_timestamp
            .add_batch(
                &left_batch(&["A", "A"], &[100, 100], &[1.0, 2.0]),
                &["id".to_string()],
                "ts",
                false,
            )
            .unwrap();
        let mut distinct_topology = SideState::new();
        distinct_topology
            .add_batch(
                &left_batch(&["A", "B"], &[100, 200], &[1.0, 2.0]),
                &["id".to_string()],
                "ts",
                false,
            )
            .unwrap();

        assert!(
            distinct_topology.accounted_state_bytes() > shared_timestamp.accounted_state_bytes()
        );
    }

    #[test]
    fn dense_checkpoint_skips_copy_and_obeys_encoder_limit() {
        let config = make_config();
        let mut state = IntervalJoinState::new();
        for (key, timestamp) in [("A", 100), ("B", 200)] {
            execute_interval_join_cycle(
                &mut state,
                &[left_batch(&[key], &[timestamp], &[1.0])],
                &[],
                &config,
                0,
                0,
            )
            .unwrap();
        }
        let first_column = state.left.batches[0].column(0).clone();

        let error = state
            .snapshot_checkpoint(&config, 1)
            .err()
            .expect("a one-byte checkpoint budget must reject Arrow IPC");
        assert!(error.to_string().contains("checkpoint limit"));
        assert_eq!(state.left.batches.len(), 2);
        assert!(Arc::ptr_eq(&first_column, state.left.batches[0].column(0)));

        let checkpoint = state
            .snapshot_checkpoint(&config, crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
            .unwrap();
        assert_eq!(checkpoint.left_batches.len(), 2);
        assert!(Arc::ptr_eq(&first_column, state.left.batches[0].column(0)));
    }

    #[test]
    fn restore_cardinality_preflight_precedes_ipc_decode() {
        let checkpoint = JoinStateCheckpoint {
            join_type: join_type_tag(JoinType::Inner),
            left_keys: vec!["id".into()],
            right_keys: vec!["id".into()],
            left_time_column: "ts".into(),
            right_time_column: "ts".into(),
            left_table: "left_stream".into(),
            right_table: "right_stream".into(),
            bound_ms: 100,
            left_buffer_rows: 2,
            right_buffer_rows: 0,
            left_batches: vec![vec![0xff]],
            right_batches: Vec::new(),
            left_evicted_cutoff: i64::MIN,
            right_evicted_cutoff: i64::MIN,
            left_row_flags: vec![vec![0, 0]],
            right_row_flags: Vec::new(),
        };
        let error = IntervalJoinState::from_checkpoint(
            &checkpoint,
            &make_config(),
            RESTORE_WORST_CASE_ROW_CHARGE,
        )
        .err()
        .expect("oversized restore cardinality must fail before IPC decode");
        assert!(error
            .to_string()
            .contains("worst-case decoded index charge"));
        assert!(!error.to_string().contains("deserialization"));
    }

    #[test]
    fn input_growth_preflight_rejects_before_mutation() {
        let state = IntervalJoinState::new();
        let error = state
            .preflight_input_growth(
                &[left_batch(&["A"], &[100], &[1.0])],
                &[],
                state.accounted_state_bytes(),
            )
            .unwrap_err();
        assert!(matches!(error, DbError::BackpressureFail(_)));
        assert_eq!(state.buffered_rows(), (0, 0));
    }

    #[test]
    fn watermark_only_cycle_frees_state_for_later_input_growth() {
        let config = make_config();
        let mut state = IntervalJoinState::new();
        let old_key = "old".repeat(4 * 1024);
        execute_interval_join_cycle(
            &mut state,
            &[left_batch(&[old_key.as_str()], &[100], &[1.0])],
            &[],
            &config,
            0,
            0,
        )
        .unwrap();
        let incoming = left_batch(&["new"], &[1_000], &[2.0]);
        let limit = state.accounted_state_bytes();
        let before_eviction = state
            .accounted_state_bytes()
            .checked_add(
                state
                    .left
                    .worst_case_input_growth(std::slice::from_ref(&incoming))
                    .unwrap(),
            )
            .unwrap();
        assert!(before_eviction > limit);

        let error = super::execute_interval_join_cycle(
            &mut state,
            std::slice::from_ref(&incoming),
            &[],
            &config,
            0,
            0,
            0,
            1_000,
            limit,
            &mut IntervalJoinOutputBudget::default(),
        )
        .unwrap_err();
        assert!(matches!(error, DbError::BackpressureFail(_)));
        assert_eq!(state.buffered_rows(), (1, 0));

        execute_interval_join_cycle(&mut state, &[], &[], &config, 0, 1_000).unwrap();
        super::execute_interval_join_cycle(
            &mut state,
            &[incoming],
            &[],
            &config,
            0,
            1_000,
            0,
            1_000,
            limit,
            &mut IntervalJoinOutputBudget::default(),
        )
        .unwrap();
        assert_eq!(state.buffered_rows(), (1, 0));
    }

    #[test]
    fn hot_key_preflight_does_not_recharge_historical_hash_rows() {
        let mut state = IntervalJoinState::new();
        let keys = vec!["hot"; 1_024];
        let timestamps = vec![100; keys.len()];
        let values = vec![1.0; keys.len()];
        state
            .left
            .add_batch(
                &left_batch(&keys, &timestamps, &values),
                &["id".to_string()],
                "ts",
                false,
            )
            .unwrap();

        state
            .preflight_input_growth(
                &[left_batch(&["hot"], &[100], &[1.0])],
                &[],
                state.accounted_state_bytes().saturating_add(64 * 1024),
            )
            .unwrap();
    }

    #[test]
    fn compaction_failure_leaves_original_state_intact() {
        let mut side = SideState::new();
        side.add_batch(
            &left_batch(&["A"], &[100], &[1.0]),
            &["id".to_string()],
            "ts",
            false,
        )
        .unwrap();
        let before_index = side.index.clone();
        let before_batch = side.batches[0].clone();

        let error = side.compact(&["missing".to_string()], "ts").unwrap_err();
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
                .snapshot_checkpoint(&config, crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
                .unwrap();
            rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
                .unwrap()
                .to_vec()
        };
        let before = checkpoint_bytes(&mut state);

        let incompatible_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("price", DataType::Int64, false),
        ]));
        let incompatible = RecordBatch::try_new(
            incompatible_schema,
            vec![
                Arc::new(StringArray::from(vec!["new"])),
                Arc::new(TimestampMillisecondArray::from(vec![110])),
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
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
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
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("amount", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
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
        // A right row at 1350 matches left rows within [1250, 1350].
        assert!(!result.is_empty());
    }

    #[test]
    fn retracting_cdc_fails_before_either_side_mutates() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("amount", DataType::Float64, false),
            Field::new("_op", DataType::Utf8, false),
        ]));
        let delete = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(TimestampMillisecondArray::from(vec![100])),
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
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
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
                Arc::new(TimestampMillisecondArray::from(vec![100])),
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
    fn row_below_prior_input_watermark_is_rejected_without_retention() {
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
        assert!(error.to_string().contains("below closed cutoff 300"));
        assert_eq!(state.left.row_count, 0);
        assert!(state.left.batches.is_empty());
    }

    #[test]
    #[allow(clippy::cast_possible_wrap, clippy::cast_precision_loss)]
    fn test_match_pairs_bounded_partial_emit_on_cross_product() {
        // Adversarial shape: every left × every right matches (single key,
        // wide bound, all timestamps within tolerance). The candidate buffer must
        // flush into batches no larger than EMIT_THRESHOLD.
        let config = StreamJoinConfig {
            join_type: JoinType::Inner,
            left_keys: vec!["id".to_string()],
            right_keys: vec!["id".to_string()],
            left_time_column: "ts".to_string(),
            right_time_column: "ts".to_string(),
            left_table: "left_stream".to_string(),
            right_table: "right_stream".to_string(),
            time_bound: Duration::from_millis(1_000_000),
        };
        let mut state = IntervalJoinState::new();

        // 300 × 300 pairs exceed the emit threshold, so output spans multiple batches.
        let m = 300usize;
        let ids_l: Vec<&str> = (0..m).map(|_| "K").collect();
        let ts_l = vec![0; m];
        let v_l: Vec<f64> = (0..m).map(|i| i as f64).collect();
        let left = left_batch(&ids_l, &ts_l, &v_l);

        let ids_r: Vec<&str> = (0..m).map(|_| "K").collect();
        let ts_r = vec![0; m];
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
    fn hot_key_output_budget_halts_before_unbounded_allocation() {
        let config = StreamJoinConfig {
            join_type: JoinType::Inner,
            left_keys: vec!["id".into()],
            right_keys: vec!["id".into()],
            left_time_column: "ts".into(),
            right_time_column: "ts".into(),
            left_table: "left_stream".into(),
            right_table: "right_stream".into(),
            time_bound: Duration::from_millis(1),
        };
        let rows = 513usize;
        let ids = vec!["K"; rows];
        let timestamps = vec![0; rows];
        let values = vec![1.0; rows];
        let mut state = IntervalJoinState::new();

        let error = execute_interval_join_cycle(
            &mut state,
            &[left_batch(&ids, &timestamps, &values)],
            &[right_batch(&ids, &timestamps, &values)],
            &config,
            0,
            0,
        )
        .unwrap_err();
        assert!(matches!(error, DbError::BackpressureFail(_)));
    }

    #[test]
    fn output_budget_is_shared_across_shards() {
        let config = make_config();
        let mut output_budget = IntervalJoinOutputBudget {
            emitted_rows: MAX_CYCLE_OUTPUT_ROWS - 1,
            emitted_bytes: 0,
        };
        let mut first = IntervalJoinState::new();
        let output = super::execute_interval_join_cycle(
            &mut first,
            &[left_batch(&["A"], &[100], &[1.0])],
            &[right_batch(&["A"], &[100], &[1.0])],
            &config,
            0,
            0,
            0,
            0,
            usize::MAX,
            &mut output_budget,
        )
        .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

        let mut second = IntervalJoinState::new();
        let error = super::execute_interval_join_cycle(
            &mut second,
            &[left_batch(&["B"], &[100], &[1.0])],
            &[right_batch(&["B"], &[100], &[1.0])],
            &config,
            0,
            0,
            0,
            0,
            usize::MAX,
            &mut output_budget,
        )
        .unwrap_err();
        assert!(matches!(error, DbError::BackpressureFail(_)));
    }
}
