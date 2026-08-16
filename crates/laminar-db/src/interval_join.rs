#![deny(clippy::disallowed_types)]

//! Stream-stream interval join for
//! `right_ts BETWEEN left_ts AND left_ts + time_bound`. Evicts expired rows on watermark advance.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::array::{
    new_null_array, Array, ArrayRef, BinaryArray, BinaryViewArray, Int64Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray, StringViewArray,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use rustc_hash::{FxHashMap, FxHasher};

use laminar_sql::parser::join_parser::JoinType;
use laminar_sql::translator::StreamJoinConfig;

use crate::error::DbError;
use crate::key_column::{extract_column_as_timestamps, extract_key_column, KeyColumn};

const COMPACTION_THRESHOLD: usize = 32;
const MAX_RETAINED_BATCHES: usize = 256;

/// Caps memory on cross-product shapes.
const EMIT_THRESHOLD: usize = 8_192;
/// Hard row bound shared by join emission and its private mutable-input normalization.
pub(crate) const MAX_CYCLE_OUTPUT_ROWS: usize = 262_144;
/// Hard transient byte bound shared by join emission and mutable-input normalization.
pub(crate) const MAX_CYCLE_OUTPUT_BYTES: usize = 64 * 1024 * 1024;
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
pub(crate) const HEAP_ALLOCATION_CHARGE: usize = 32;
const HASH_BUCKET_CHARGE: usize = 128;
const BTREE_TIMESTAMP_CHARGE: usize = 512;
const BATCH_METADATA_CHARGE: usize = 256;
const ARRAY_METADATA_CHARGE: usize = 128;
// A restored row can be the first position for a unique timestamp and hash. `HashMap`'s smallest
// table has three usable buckets, so three charged buckets per row also covers every growth step.
// `Vec::push` currently reserves four position slots for the first element. This intentionally
// rejects a checkpoint unless its worst supported index shape fits before index construction.
const WORST_CASE_ROW_NON_HASH_CHARGE: usize = std::mem::size_of::<usize>()
    + BTREE_TIMESTAMP_CHARGE
    + HEAP_ALLOCATION_CHARGE
    + 4 * std::mem::size_of::<(usize, usize)>();
const RESTORE_WORST_CASE_ROW_CHARGE: usize =
    WORST_CASE_ROW_NON_HASH_CHARGE + 3 * HASH_BUCKET_CHARGE;

const fn charged_allocation(bytes: usize) -> usize {
    bytes.saturating_add(if bytes == 0 {
        0
    } else {
        HEAP_ALLOCATION_CHARGE
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinExecutionMode {
    AppendOnly,
    Weighted,
}

impl JoinExecutionMode {
    const fn is_weighted(self) -> bool {
        matches!(self, Self::Weighted)
    }
}

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

fn shallow_batch_clone_charge(
    batches: &[RecordBatch],
    outer_capacity: usize,
) -> Result<usize, DbError> {
    let outer = outer_capacity
        .checked_mul(std::mem::size_of::<RecordBatch>())
        .map(charged_allocation)
        .ok_or_else(|| {
            DbError::Checkpoint("interval join batch-clone roster accounting overflow".into())
        })?;
    batches.iter().try_fold(outer, |bytes, batch| {
        batch
            .num_columns()
            .checked_mul(std::mem::size_of::<ArrayRef>())
            .map(charged_allocation)
            .and_then(|columns| bytes.checked_add(columns))
            .ok_or_else(|| {
                DbError::Checkpoint("interval join batch-clone roster accounting overflow".into())
            })
    })
}

fn position_vector_charge(capacity: usize) -> usize {
    HEAP_ALLOCATION_CHARGE
        .saturating_add(capacity.saturating_mul(std::mem::size_of::<(usize, usize)>()))
}

fn tracks_left_matches(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left | JoinType::Full | JoinType::LeftSemi | JoinType::LeftAnti
    )
}

fn tracks_right_matches(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Right | JoinType::Full | JoinType::RightSemi | JoinType::RightAnti
    )
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
            let positions = vec![position];
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

/// Compute checked logical payload bytes for every row without materializing row slices.
pub(crate) fn logical_row_bytes(batch: &RecordBatch) -> Result<Vec<usize>, DbError> {
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
            DataType::LargeUtf8
            | DataType::LargeBinary
            | DataType::Utf8View
            | DataType::BinaryView => 16,
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
    pub weighted: bool,
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
    pub left_row_weights: Vec<Vec<i64>>,
    pub right_row_weights: Vec<Vec<i64>>,
    pub left_match_flags: Vec<Vec<u8>>,
    pub right_match_flags: Vec<Vec<u8>>,
    pub left_match_weights: Vec<Vec<i64>>,
    pub right_match_weights: Vec<Vec<i64>>,
}

impl JoinStateCheckpoint {
    pub(crate) fn retained_ipc_bytes(&self) -> Result<usize, DbError> {
        let roster_bytes = [
            self.left_batches
                .capacity()
                .checked_mul(std::mem::size_of::<Vec<u8>>()),
            self.right_batches
                .capacity()
                .checked_mul(std::mem::size_of::<Vec<u8>>()),
            self.left_row_weights
                .capacity()
                .checked_mul(std::mem::size_of::<Vec<i64>>()),
            self.right_row_weights
                .capacity()
                .checked_mul(std::mem::size_of::<Vec<i64>>()),
            self.left_match_weights
                .capacity()
                .checked_mul(std::mem::size_of::<Vec<i64>>()),
            self.right_match_weights
                .capacity()
                .checked_mul(std::mem::size_of::<Vec<i64>>()),
            self.left_match_flags
                .capacity()
                .checked_mul(std::mem::size_of::<Vec<u8>>()),
            self.right_match_flags
                .capacity()
                .checked_mul(std::mem::size_of::<Vec<u8>>()),
            self.left_keys
                .capacity()
                .checked_mul(std::mem::size_of::<String>()),
            self.right_keys
                .capacity()
                .checked_mul(std::mem::size_of::<String>()),
        ]
        .into_iter()
        .try_fold(0usize, |total, bytes| {
            let bytes = bytes.ok_or_else(|| {
                DbError::Checkpoint("interval join checkpoint roster accounting overflow".into())
            })?;
            total.checked_add(charged_allocation(bytes)).ok_or_else(|| {
                DbError::Checkpoint("interval join checkpoint roster accounting overflow".into())
            })
        })?;
        let payload = self
            .left_batches
            .iter()
            .chain(&self.right_batches)
            .try_fold(roster_bytes, |total, batch| {
                total
                    .checked_add(charged_allocation(batch.capacity()))
                    .ok_or_else(|| {
                        DbError::Checkpoint(
                            "interval join checkpoint retained IPC accounting overflow".into(),
                        )
                    })
            })?;
        let payload = self
            .left_match_flags
            .iter()
            .chain(&self.right_match_flags)
            .try_fold(payload, |total, flags| {
                total
                    .checked_add(charged_allocation(flags.capacity()))
                    .ok_or_else(|| {
                        DbError::Checkpoint(
                            "interval join checkpoint retained match-flag accounting overflow"
                                .into(),
                        )
                    })
            })?;
        let payload = self
            .left_row_weights
            .iter()
            .chain(&self.right_row_weights)
            .chain(&self.left_match_weights)
            .chain(&self.right_match_weights)
            .try_fold(payload, |total, batch| {
                let bytes = batch
                    .capacity()
                    .checked_mul(std::mem::size_of::<i64>())
                    .ok_or_else(|| {
                        DbError::Checkpoint(
                            "interval join checkpoint retained weight accounting overflow".into(),
                        )
                    })?;
                total.checked_add(charged_allocation(bytes)).ok_or_else(|| {
                    DbError::Checkpoint(
                        "interval join checkpoint retained weight accounting overflow".into(),
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
                total.checked_add(charged_allocation(bytes)).ok_or_else(|| {
                    DbError::Checkpoint(
                        "interval join checkpoint configuration accounting overflow".into(),
                    )
                })
            })
    }
}

pub(crate) struct IntervalJoinCheckpointCapture {
    execution_mode: JoinExecutionMode,
    config: StreamJoinConfig,
    bound_ms: i64,
    left_buffer_rows: u64,
    right_buffer_rows: u64,
    left_batches: Vec<RecordBatch>,
    right_batches: Vec<RecordBatch>,
    left_evicted_cutoff: i64,
    right_evicted_cutoff: i64,
    left_row_weights: Vec<Arc<[i64]>>,
    right_row_weights: Vec<Arc<[i64]>>,
    left_match_flags: Vec<Arc<[u8]>>,
    right_match_flags: Vec<Arc<[u8]>>,
    left_match_weights: Vec<Arc<[i64]>>,
    right_match_weights: Vec<Arc<[i64]>>,
    left_needs_compaction: bool,
    right_needs_compaction: bool,
    retained_bytes: usize,
}

impl IntervalJoinCheckpointCapture {
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn calculate_retained_bytes(&self) -> usize {
        fn roster<T>(capacity: usize) -> usize {
            charged_allocation(capacity.saturating_mul(std::mem::size_of::<T>()))
        }

        fn batch_side(
            batches: &[RecordBatch],
            batch_capacity: usize,
            row_weights: &[Arc<[i64]>],
            row_weight_capacity: usize,
            match_flags: &[Arc<[u8]>],
            match_flag_capacity: usize,
            match_weights: &[Arc<[i64]>],
            match_weight_capacity: usize,
        ) -> usize {
            batches
                .iter()
                .fold(roster::<RecordBatch>(batch_capacity), |bytes, batch| {
                    bytes
                        .saturating_add(batch.get_array_memory_size())
                        .saturating_add(batch_metadata_charge(batch))
                })
                .saturating_add(roster::<Arc<[i64]>>(row_weight_capacity))
                .saturating_add(row_weights.iter().fold(0usize, |bytes, weights| {
                    bytes.saturating_add(charged_allocation(
                        weights.len().saturating_mul(std::mem::size_of::<i64>()),
                    ))
                }))
                .saturating_add(roster::<Arc<[i64]>>(match_weight_capacity))
                .saturating_add(match_weights.iter().fold(0usize, |bytes, weights| {
                    bytes.saturating_add(charged_allocation(
                        weights.len().saturating_mul(std::mem::size_of::<i64>()),
                    ))
                }))
                .saturating_add(roster::<Arc<[u8]>>(match_flag_capacity))
                .saturating_add(match_flags.iter().fold(0usize, |bytes, flags| {
                    bytes.saturating_add(charged_allocation(flags.len()))
                }))
        }

        fn string_vector(values: &[String], capacity: usize) -> usize {
            values
                .iter()
                .fold(roster::<String>(capacity), |bytes, value| {
                    bytes.saturating_add(charged_allocation(value.capacity()))
                })
        }

        let mut bytes = std::mem::size_of::<Self>();
        bytes = bytes
            .saturating_add(string_vector(
                &self.config.left_keys,
                self.config.left_keys.capacity(),
            ))
            .saturating_add(string_vector(
                &self.config.right_keys,
                self.config.right_keys.capacity(),
            ));
        for value in [
            &self.config.left_time_column,
            &self.config.right_time_column,
            &self.config.left_table,
            &self.config.right_table,
        ] {
            bytes = bytes.saturating_add(charged_allocation(value.capacity()));
        }
        bytes
            .saturating_add(batch_side(
                &self.left_batches,
                self.left_batches.capacity(),
                &self.left_row_weights,
                self.left_row_weights.capacity(),
                &self.left_match_flags,
                self.left_match_flags.capacity(),
                &self.left_match_weights,
                self.left_match_weights.capacity(),
            ))
            .saturating_add(batch_side(
                &self.right_batches,
                self.right_batches.capacity(),
                &self.right_row_weights,
                self.right_row_weights.capacity(),
                &self.right_match_flags,
                self.right_match_flags.capacity(),
                &self.right_match_weights,
                self.right_match_weights.capacity(),
            ))
    }

    pub(crate) fn encode(self, max_encoded_bytes: usize) -> Result<JoinStateCheckpoint, DbError> {
        type EncodedSide = (Vec<Vec<u8>>, Vec<Vec<i64>>, Vec<Vec<u8>>, Vec<Vec<i64>>);

        #[derive(Clone, Copy)]
        struct EncodeSideOptions {
            track_matches: bool,
            execution_mode: JoinExecutionMode,
            needs_compaction: bool,
            retain_null_tuples: bool,
        }

        fn encode_side(
            side: &str,
            batches: &[RecordBatch],
            captured_row_weights: &[Arc<[i64]>],
            captured_match_flags: &[Arc<[u8]>],
            captured_match_weights: &[Arc<[i64]>],
            options: EncodeSideOptions,
            key_columns: &[String],
            time_column: &str,
            evicted_cutoff: i64,
            expected_rows: u64,
            remaining: &mut usize,
        ) -> Result<EncodedSide, DbError> {
            fn weight_allocation_bytes(
                rows: &Vec<i64>,
                flags: &Vec<u8>,
                matches: &Vec<i64>,
            ) -> Result<usize, DbError> {
                [
                    rows.capacity().checked_mul(std::mem::size_of::<i64>()),
                    Some(flags.capacity()),
                    matches.capacity().checked_mul(std::mem::size_of::<i64>()),
                ]
                .into_iter()
                .try_fold(0usize, |total, bytes| {
                    let bytes = bytes.ok_or_else(|| {
                        DbError::Checkpoint(
                            "interval join checkpoint weight accounting overflow".into(),
                        )
                    })?;
                    total.checked_add(charged_allocation(bytes)).ok_or_else(|| {
                        DbError::Checkpoint(
                            "interval join checkpoint weight accounting overflow".into(),
                        )
                    })
                })
            }

            let EncodeSideOptions {
                track_matches,
                execution_mode,
                needs_compaction,
                retain_null_tuples,
            } = options;
            let weighted = execution_mode.is_weighted();

            if (weighted && batches.len() != captured_row_weights.len())
                || (!weighted && !captured_row_weights.is_empty())
                || (track_matches && !weighted && batches.len() != captured_match_flags.len())
                || (track_matches && weighted && batches.len() != captured_match_weights.len())
                || (!track_matches
                    && (!captured_match_flags.is_empty() || !captured_match_weights.is_empty()))
                || (weighted && !captured_match_flags.is_empty())
                || (!weighted && !captured_match_weights.is_empty())
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join {side} checkpoint weight rosters do not match its batch roster"
                )));
            }
            let requested_roster_bytes = [
                batches.len().checked_mul(std::mem::size_of::<Vec<u8>>()),
                captured_row_weights
                    .len()
                    .checked_mul(std::mem::size_of::<Vec<i64>>()),
                captured_match_flags
                    .len()
                    .checked_mul(std::mem::size_of::<Vec<u8>>()),
                captured_match_weights
                    .len()
                    .checked_mul(std::mem::size_of::<Vec<i64>>()),
            ]
            .into_iter()
            .try_fold(0usize, |total, bytes| {
                let bytes = bytes.ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join {side} checkpoint roster accounting overflow"
                    ))
                })?;
                total.checked_add(charged_allocation(bytes)).ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join {side} checkpoint roster accounting overflow"
                    ))
                })
            })?;
            *remaining = remaining
                .checked_sub(requested_roster_bytes)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join {side} checkpoint roster exceeded its cumulative checkpoint byte limit"
                    ))
                })?;

            let mut encoded = Vec::new();
            encoded.try_reserve_exact(batches.len()).map_err(|_| {
                DbError::Checkpoint(format!(
                    "interval join {side} checkpoint batch roster cannot be reserved"
                ))
            })?;
            let mut row_weights = Vec::new();
            row_weights
                .try_reserve_exact(captured_row_weights.len())
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "interval join {side} checkpoint row-weight roster cannot be reserved"
                    ))
                })?;
            let mut match_weights = Vec::new();
            match_weights
                .try_reserve_exact(captured_match_weights.len())
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "interval join {side} checkpoint match-weight roster cannot be reserved"
                    ))
                })?;
            let mut match_flags = Vec::new();
            match_flags
                .try_reserve_exact(captured_match_flags.len())
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "interval join {side} checkpoint match-flag roster cannot be reserved"
                    ))
                })?;
            let roster_bytes = [
                encoded
                    .capacity()
                    .checked_mul(std::mem::size_of::<Vec<u8>>()),
                row_weights
                    .capacity()
                    .checked_mul(std::mem::size_of::<Vec<i64>>()),
                match_flags
                    .capacity()
                    .checked_mul(std::mem::size_of::<Vec<u8>>()),
                match_weights
                    .capacity()
                    .checked_mul(std::mem::size_of::<Vec<i64>>()),
            ]
            .into_iter()
            .try_fold(0usize, |total, bytes| {
                let bytes = bytes.ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join {side} checkpoint roster accounting overflow"
                    ))
                })?;
                total.checked_add(charged_allocation(bytes)).ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join {side} checkpoint roster accounting overflow"
                    ))
                })
            })?;
            if roster_bytes > requested_roster_bytes {
                *remaining = remaining
                    .checked_sub(roster_bytes - requested_roster_bytes)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join {side} checkpoint roster exceeded its cumulative checkpoint byte limit"
                        ))
                    })?;
            } else {
                *remaining = remaining
                    .checked_add(requested_roster_bytes - roster_bytes)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join {side} checkpoint roster accounting overflow"
                        ))
                    })?;
            }

            let mut encoded_rows = 0_u64;
            for (batch_index, batch) in batches.iter().enumerate() {
                let captured_row_weights =
                    weighted.then(|| captured_row_weights[batch_index].as_ref());
                let captured_match_flags = (track_matches && !weighted)
                    .then(|| captured_match_flags[batch_index].as_ref());
                let captured_match_weights = (track_matches && weighted)
                    .then(|| &captured_match_weights[batch_index])
                    .map(AsRef::as_ref);
                if batch.num_rows() == 0
                    || captured_row_weights.is_some_and(|weights| weights.len() != batch.num_rows())
                    || captured_match_weights
                        .is_some_and(|weights| weights.len() != batch.num_rows())
                    || captured_match_flags.is_some_and(|flags| flags.len() != batch.num_rows())
                {
                    return Err(DbError::Checkpoint(format!(
                        "interval join {side} checkpoint batch shape is invalid"
                    )));
                }
                if captured_row_weights.is_some_and(|weights| weights.contains(&0))
                    || captured_match_weights
                        .is_some_and(|weights| weights.iter().any(|weight| *weight < 0))
                    || captured_match_flags.is_some_and(|flags| flags.iter().any(|flag| *flag > 1))
                {
                    return Err(DbError::Checkpoint(format!(
                        "interval join {side} checkpoint has invalid row or match weights"
                    )));
                }
                let (compacted, rows, flags, matches) = if needs_compaction {
                    let scan_bytes = batch
                        .num_rows()
                        .checked_mul(2 * std::mem::size_of::<i64>() + std::mem::size_of::<u32>())
                        .and_then(|bytes| {
                            bytes.checked_add(
                                key_columns
                                    .len()
                                    .checked_mul(std::mem::size_of::<KeyColumn<'_>>())?,
                            )
                        })
                        .and_then(|bytes| bytes.checked_add(3 * HEAP_ALLOCATION_CHARGE))
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "interval join {side} checkpoint compaction accounting overflow"
                            ))
                        })?;
                    if scan_bytes > *remaining {
                        return Err(DbError::Checkpoint(format!(
                            "interval join {side} checkpoint compaction requires {scan_bytes} bytes; remaining limit is {} bytes",
                            *remaining
                        )));
                    }
                    let timestamps =
                        extract_column_as_timestamps(batch, time_column).map_err(|error| {
                            DbError::Checkpoint(format!(
                                "interval join {side} checkpoint time column: {error}"
                            ))
                        })?;
                    let keys = extract_key_columns(batch, key_columns).map_err(|error| {
                        DbError::Checkpoint(format!(
                            "interval join {side} checkpoint key columns: {error}"
                        ))
                    })?;
                    let mut selected = Vec::<u32>::new();
                    selected.try_reserve_exact(batch.num_rows()).map_err(|_| {
                        DbError::Checkpoint(format!(
                            "interval join {side} checkpoint selection cannot be reserved"
                        ))
                    })?;
                    for (row, &timestamp) in timestamps.iter().enumerate() {
                        if timestamp >= evicted_cutoff
                            && (retain_null_tuples || keys.iter().all(|key| !key.is_null(row)))
                        {
                            selected.push(u32::try_from(row).map_err(|_| {
                                DbError::Checkpoint(format!(
                                    "interval join {side} checkpoint row index exceeds u32"
                                ))
                            })?);
                        }
                    }
                    if selected.is_empty() {
                        continue;
                    }
                    let scan_retained = timestamps
                        .capacity()
                        .checked_mul(std::mem::size_of::<i64>())
                        .and_then(|bytes| {
                            bytes.checked_add(
                                selected
                                    .capacity()
                                    .checked_mul(std::mem::size_of::<u32>())?,
                            )
                        })
                        .and_then(|bytes| {
                            bytes.checked_add(
                                keys.capacity()
                                    .checked_mul(std::mem::size_of::<KeyColumn<'_>>())?,
                            )
                        })
                        .and_then(|bytes| bytes.checked_add(3 * HEAP_ALLOCATION_CHARGE))
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "interval join {side} checkpoint scan accounting overflow"
                            ))
                        })?;
                    let weight_headroom =
                        remaining.checked_sub(scan_retained).ok_or_else(|| {
                            DbError::Checkpoint(format!(
                            "interval join {side} checkpoint scan exceeded its remaining byte limit"
                        ))
                        })?;
                    let selected_i64_bytes = selected
                        .len()
                        .checked_mul(std::mem::size_of::<i64>())
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "interval join {side} checkpoint weight accounting overflow"
                            ))
                        })?;
                    let selected_weight_bytes = [
                        weighted.then(|| charged_allocation(selected_i64_bytes)),
                        (track_matches && weighted).then(|| charged_allocation(selected_i64_bytes)),
                        (track_matches && !weighted).then(|| charged_allocation(selected.len())),
                    ]
                    .into_iter()
                    .flatten()
                    .try_fold(0usize, usize::checked_add)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join {side} checkpoint weight accounting overflow"
                        ))
                    })?;
                    if selected_weight_bytes > weight_headroom {
                        return Err(DbError::Checkpoint(format!(
                            "interval join {side} checkpoint weights exceed the remaining byte limit"
                        )));
                    }
                    let mut rows = Vec::new();
                    if weighted {
                        rows.try_reserve_exact(selected.len()).map_err(|_| {
                            DbError::Checkpoint(format!(
                                "interval join {side} checkpoint row weights cannot be reserved"
                            ))
                        })?;
                    }
                    let mut matches = Vec::new();
                    if track_matches && weighted {
                        matches.try_reserve_exact(selected.len()).map_err(|_| {
                            DbError::Checkpoint(format!(
                                "interval join {side} checkpoint match weights cannot be reserved"
                            ))
                        })?;
                    }
                    let mut flags = Vec::new();
                    if track_matches && !weighted {
                        flags.try_reserve_exact(selected.len()).map_err(|_| {
                            DbError::Checkpoint(format!(
                                "interval join {side} checkpoint match flags cannot be reserved"
                            ))
                        })?;
                    }
                    let scratch_headroom = remaining
                        .checked_sub(weight_allocation_bytes(&rows, &flags, &matches)?)
                        .and_then(|bytes| bytes.checked_sub(scan_retained))
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "interval join {side} checkpoint scan exceeded its remaining byte limit"
                            ))
                        })?;
                    if let Some(captured) = captured_row_weights {
                        rows.extend(selected.iter().map(|row| captured[*row as usize]));
                    }
                    if let Some(captured) = captured_match_weights {
                        matches.extend(selected.iter().map(|row| captured[*row as usize]));
                    }
                    if let Some(captured) = captured_match_flags {
                        flags.extend(selected.iter().map(|row| captured[*row as usize]));
                    }
                    let compacted = if selected.len() == batch.num_rows() {
                        None
                    } else {
                        let upper_bound = batch
                            .get_array_memory_size()
                            .checked_add(batch_metadata_charge(batch))
                            .ok_or_else(|| {
                                DbError::Checkpoint(format!(
                                    "interval join {side} checkpoint compaction accounting overflow"
                                ))
                            })?;
                        if upper_bound > scratch_headroom {
                            return Err(DbError::Checkpoint(format!(
                                "interval join {side} checkpoint compaction requires up to {upper_bound} bytes; scratch headroom is {scratch_headroom} bytes"
                            )));
                        }
                        let indices = arrow::array::UInt32Array::from(selected);
                        let columns = batch
                            .columns()
                            .iter()
                            .map(|column| arrow::compute::take(column.as_ref(), &indices, None))
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|error| {
                                DbError::query_pipeline_arrow(
                                    "interval join checkpoint compaction",
                                    &error,
                                )
                            })?;
                        Some(
                            RecordBatch::try_new(batch.schema(), columns).map_err(|error| {
                                DbError::query_pipeline_arrow(
                                    "interval join checkpoint compacted batch",
                                    &error,
                                )
                            })?,
                        )
                    };
                    (compacted, rows, flags, matches)
                } else {
                    let weight_bytes = [
                        captured_row_weights.map(|weights| {
                            charged_allocation(
                                weights.len().saturating_mul(std::mem::size_of::<i64>()),
                            )
                        }),
                        captured_match_weights.map(|weights| {
                            charged_allocation(
                                weights.len().saturating_mul(std::mem::size_of::<i64>()),
                            )
                        }),
                        captured_match_flags.map(|flags| charged_allocation(flags.len())),
                    ]
                    .into_iter()
                    .flatten()
                    .try_fold(0usize, usize::checked_add)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join {side} checkpoint weight accounting overflow"
                        ))
                    })?;
                    if weight_bytes > *remaining {
                        return Err(DbError::Checkpoint(format!(
                            "interval join {side} checkpoint weights exceed the remaining byte limit"
                        )));
                    }
                    let mut rows = Vec::new();
                    rows.try_reserve_exact(captured_row_weights.map_or(0, <[i64]>::len))
                        .map_err(|_| {
                            DbError::Checkpoint(format!(
                                "interval join {side} checkpoint row weights cannot be reserved"
                            ))
                        })?;
                    if let Some(captured) = captured_row_weights {
                        rows.extend_from_slice(captured);
                    }
                    let mut matches = Vec::new();
                    if let Some(captured) = captured_match_weights {
                        matches.try_reserve_exact(captured.len()).map_err(|_| {
                            DbError::Checkpoint(format!(
                                "interval join {side} checkpoint match weights cannot be reserved"
                            ))
                        })?;
                        matches.extend_from_slice(captured);
                    }
                    let mut flags = Vec::new();
                    if let Some(captured) = captured_match_flags {
                        flags.try_reserve_exact(captured.len()).map_err(|_| {
                            DbError::Checkpoint(format!(
                                "interval join {side} checkpoint match flags cannot be reserved"
                            ))
                        })?;
                        flags.extend_from_slice(captured);
                    }
                    (None, rows, flags, matches)
                };
                let weight_bytes = weight_allocation_bytes(&rows, &flags, &matches)?;
                *remaining = remaining.checked_sub(weight_bytes).ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join {side} checkpoint weight accounting overflow"
                    ))
                })?;
                let encoded_batch_rows = compacted
                    .as_ref()
                    .map_or(batch.num_rows(), RecordBatch::num_rows);
                encoded_rows = encoded_rows
                    .checked_add(u64::try_from(encoded_batch_rows).map_err(|_| {
                        DbError::Checkpoint(format!(
                            "interval join {side} checkpoint row count exceeds u64"
                        ))
                    })?)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join {side} checkpoint row-count overflow"
                        ))
                    })?;
                let batch = compacted.as_ref().unwrap_or(batch);
                let scratch_bytes = compacted.as_ref().map_or(0, |batch| {
                    batch
                        .get_array_memory_size()
                        .saturating_add(batch_metadata_charge(batch))
                });
                let serialization_limit = remaining
                    .checked_sub(scratch_bytes)
                    .and_then(|bytes| bytes.checked_sub(HEAP_ALLOCATION_CHARGE))
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join {side} checkpoint compacted batch exceeds its remaining byte limit"
                        ))
                    })?;
                let ipc = laminar_core::serialization::serialize_batches_stream_bounded(
                    batch.schema().as_ref(),
                    std::iter::once(batch),
                    serialization_limit,
                )
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "interval join {side} batch serialization within the cumulative checkpoint limit: {error}"
                    ))
                })?;
                *remaining = remaining
                    .checked_sub(charged_allocation(ipc.capacity()))
                    .ok_or_else(|| {
                        DbError::Checkpoint(
                            "interval join checkpoint encoded byte accounting overflow".into(),
                        )
                    })?;
                encoded.push(ipc);
                if weighted {
                    row_weights.push(rows);
                }
                if track_matches && weighted {
                    match_weights.push(matches);
                }
                if track_matches && !weighted {
                    match_flags.push(flags);
                }
            }
            if encoded_rows != expected_rows {
                return Err(DbError::Checkpoint(format!(
                    "interval join {side} checkpoint row-count mismatch: expected {expected_rows}, encoded {encoded_rows}"
                )));
            }
            Ok((encoded, row_weights, match_flags, match_weights))
        }

        let config_bytes = self
            .config
            .left_keys
            .iter()
            .chain(&self.config.right_keys)
            .map(|value| charged_allocation(value.capacity()))
            .chain([
                charged_allocation(self.config.left_time_column.capacity()),
                charged_allocation(self.config.right_time_column.capacity()),
                charged_allocation(self.config.left_table.capacity()),
                charged_allocation(self.config.right_table.capacity()),
            ])
            .try_fold(0usize, usize::checked_add)
            .and_then(|total| {
                total.checked_add(charged_allocation(
                    self.config
                        .left_keys
                        .capacity()
                        .checked_mul(std::mem::size_of::<String>())?,
                ))
            })
            .and_then(|total| {
                total.checked_add(charged_allocation(
                    self.config
                        .right_keys
                        .capacity()
                        .checked_mul(std::mem::size_of::<String>())?,
                ))
            })
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "interval join checkpoint configuration accounting overflow".into(),
                )
            })?;
        let mut remaining = max_encoded_bytes.checked_sub(config_bytes).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join retained configuration exceeds its {max_encoded_bytes}-byte checkpoint limit"
            ))
        })?;
        let (left_batches, left_row_weights, left_match_flags, left_match_weights) = encode_side(
            "left",
            &self.left_batches,
            &self.left_row_weights,
            &self.left_match_flags,
            &self.left_match_weights,
            EncodeSideOptions {
                track_matches: tracks_left_matches(self.config.join_type),
                execution_mode: self.execution_mode,
                needs_compaction: self.left_needs_compaction,
                retain_null_tuples: retain_left_null_tuples(self.config.join_type),
            },
            &self.config.left_keys,
            &self.config.left_time_column,
            self.left_evicted_cutoff,
            self.left_buffer_rows,
            &mut remaining,
        )?;
        let (right_batches, right_row_weights, right_match_flags, right_match_weights) =
            encode_side(
                "right",
                &self.right_batches,
                &self.right_row_weights,
                &self.right_match_flags,
                &self.right_match_weights,
                EncodeSideOptions {
                    track_matches: tracks_right_matches(self.config.join_type),
                    execution_mode: self.execution_mode,
                    needs_compaction: self.right_needs_compaction,
                    retain_null_tuples: retain_right_null_tuples(self.config.join_type),
                },
                &self.config.right_keys,
                &self.config.right_time_column,
                self.right_evicted_cutoff,
                self.right_buffer_rows,
                &mut remaining,
            )?;

        Ok(JoinStateCheckpoint {
            weighted: self.execution_mode.is_weighted(),
            join_type: join_type_tag(self.config.join_type),
            left_keys: self.config.left_keys,
            right_keys: self.config.right_keys,
            left_time_column: self.config.left_time_column,
            right_time_column: self.config.right_time_column,
            left_table: self.config.left_table,
            right_table: self.config.right_table,
            bound_ms: self.bound_ms,
            left_buffer_rows: self.left_buffer_rows,
            right_buffer_rows: self.right_buffer_rows,
            left_batches,
            right_batches,
            left_evicted_cutoff: self.left_evicted_cutoff,
            right_evicted_cutoff: self.right_evicted_cutoff,
            left_row_weights,
            right_row_weights,
            left_match_flags,
            right_match_flags,
            left_match_weights,
            right_match_weights,
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
    row_weights: Vec<Arc<[i64]>>,
    match_flags: Vec<Arc<[u8]>>,
    match_weights: Vec<Arc<[i64]>>,
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
            row_weights: Vec::new(),
            match_flags: Vec::new(),
            match_weights: Vec::new(),
            row_size_vector_bytes: 0,
            index_entry_bytes: 0,
        }
    }

    fn add_batch(
        &mut self,
        batch: &RecordBatch,
        row_weights: Option<Arc<[i64]>>,
        key_col_names: &[String],
        time_col_name: &str,
        retain_null_tuples: bool,
        track_matches: bool,
        execution_mode: JoinExecutionMode,
    ) -> Result<bool, DbError> {
        if let Some(retained) = self.batches.first() {
            if retained.schema().as_ref() != batch.schema().as_ref() {
                return Err(DbError::SchemaMismatch(
                    "interval join side schema changed while rows were retained".to_string(),
                ));
            }
        }
        let valid_weight_roster = match (execution_mode, row_weights.as_deref()) {
            (JoinExecutionMode::AppendOnly, None) => true,
            (JoinExecutionMode::Weighted, Some(weights)) => {
                weights.len() == batch.num_rows() && weights.iter().all(|weight| *weight != 0)
            }
            _ => false,
        };
        if !valid_weight_roster {
            return Err(DbError::PipelineTerminal(
                "interval join admitted an invalid row-weight roster".into(),
            ));
        }
        if batch.num_rows() == 0 {
            return Ok(false);
        }
        let batch_idx = self.batches.len();
        let keys = extract_key_columns(batch, key_col_names)?;
        let timestamps = extract_column_as_timestamps(batch, time_col_name)?;
        let row_bytes = logical_row_bytes(batch)?;
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
        if let Some(row_weights) = row_weights {
            self.row_weights.push(row_weights);
        }
        if track_matches {
            if execution_mode == JoinExecutionMode::AppendOnly {
                self.match_flags
                    .push(Arc::from(vec![0_u8; batch.num_rows()]));
            } else {
                self.match_weights
                    .push(Arc::from(vec![0_i64; batch.num_rows()]));
            }
        }
        Ok(true)
    }

    fn row_weight(&self, batch: usize, row: usize) -> Result<i64, DbError> {
        if self.row_weights.is_empty() {
            return self
                .batches
                .get(batch)
                .filter(|batch| row < batch.num_rows())
                .map(|_| 1)
                .ok_or_else(|| {
                    DbError::PipelineTerminal("interval join row position is invalid".into())
                });
        }
        self.row_weights
            .get(batch)
            .and_then(|weights| weights.get(row))
            .copied()
            .ok_or_else(|| {
                DbError::PipelineTerminal("interval join row-weight position is invalid".into())
            })
    }

    fn match_weight(
        &self,
        batch: usize,
        row: usize,
        execution_mode: JoinExecutionMode,
    ) -> Result<i64, DbError> {
        if execution_mode == JoinExecutionMode::AppendOnly {
            self.match_flags
                .get(batch)
                .and_then(|flags| flags.get(row))
                .copied()
                .map(i64::from)
                .ok_or_else(|| {
                    DbError::PipelineTerminal("interval join match-flag position is invalid".into())
                })
        } else {
            self.match_weights
                .get(batch)
                .and_then(|weights| weights.get(row))
                .copied()
                .ok_or_else(|| {
                    DbError::PipelineTerminal(
                        "interval join match-weight position is invalid".into(),
                    )
                })
        }
    }

    fn set_match_weight(
        match_flags: &mut [Arc<[u8]>],
        match_weights: &mut [Arc<[i64]>],
        batch: usize,
        row: usize,
        weight: i64,
        execution_mode: JoinExecutionMode,
    ) -> Result<(), DbError> {
        if execution_mode == JoinExecutionMode::AppendOnly {
            let weight = u8::try_from(weight).map_err(|_| {
                DbError::PipelineTerminal("interval join match flag exceeds u8".into())
            })?;
            let flags = match_flags.get_mut(batch).ok_or_else(|| {
                DbError::PipelineTerminal("interval join match-flag batch is missing".into())
            })?;
            let current = Arc::make_mut(flags).get_mut(row).ok_or_else(|| {
                DbError::PipelineTerminal("interval join match-flag row is missing".into())
            })?;
            *current = weight;
        } else {
            let weights = match_weights.get_mut(batch).ok_or_else(|| {
                DbError::PipelineTerminal("interval join match-weight batch is missing".into())
            })?;
            let current = Arc::make_mut(weights).get_mut(row).ok_or_else(|| {
                DbError::PipelineTerminal("interval join match-weight row is missing".into())
            })?;
            *current = weight;
        }
        Ok(())
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
            self.row_weights.clear();
            self.match_flags.clear();
            self.match_weights.clear();
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
        execution_mode: JoinExecutionMode,
    ) -> Result<Vec<(usize, usize)>, DbError> {
        let mut positions = Vec::new();
        for position in self
            .index
            .values()
            .flat_map(|timestamps| timestamps.range(..cutoff))
            .flat_map(|(_, positions)| positions.iter().copied())
        {
            if self.match_weight(position.0, position.1, execution_mode)? != 0 {
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
            self.row_weights.clear();
            self.match_flags.clear();
            self.match_weights.clear();
            self.row_size_vector_bytes = 0;
            self.index_entry_bytes = 0;
            return Ok(());
        }

        live_rows.sort_unstable();
        let replacement_row_weights: Vec<i64> = if self.row_weights.is_empty() {
            Vec::new()
        } else {
            live_rows
                .iter()
                .map(|&(batch, row)| self.row_weights[batch][row])
                .collect()
        };
        let replacement_match_weights: Vec<i64> = if self.match_weights.is_empty() {
            Vec::new()
        } else {
            live_rows
                .iter()
                .map(|&(batch, row)| self.match_weights[batch][row])
                .collect()
        };
        let replacement_match_flags: Vec<u8> = if self.match_flags.is_empty() {
            Vec::new()
        } else {
            live_rows
                .iter()
                .map(|&(batch, row)| self.match_flags[batch][row])
                .collect()
        };

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
        self.row_weights = if replacement_row_weights.is_empty() {
            Vec::new()
        } else {
            vec![Arc::from(replacement_row_weights)]
        };
        self.match_weights = if replacement_match_weights.is_empty() {
            Vec::new()
        } else {
            vec![Arc::from(replacement_match_weights)]
        };
        self.match_flags = if replacement_match_flags.is_empty() {
            Vec::new()
        } else {
            vec![Arc::from(replacement_match_flags)]
        };
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
                self.row_weights
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Arc<[i64]>>()),
            )
            .saturating_add(
                usize::from(self.row_weights.capacity() > 0).saturating_mul(HEAP_ALLOCATION_CHARGE),
            )
            .saturating_add(self.row_weights.iter().fold(0usize, |bytes, weights| {
                bytes.saturating_add(charged_allocation(
                    weights.len().saturating_mul(std::mem::size_of::<i64>()),
                ))
            }))
            .saturating_add(
                self.match_flags
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Arc<[u8]>>()),
            )
            .saturating_add(
                usize::from(self.match_flags.capacity() > 0).saturating_mul(HEAP_ALLOCATION_CHARGE),
            )
            .saturating_add(self.match_flags.iter().fold(0usize, |bytes, flags| {
                bytes.saturating_add(charged_allocation(flags.len()))
            }))
            .saturating_add(
                self.match_weights
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Arc<[i64]>>()),
            )
            .saturating_add(
                usize::from(self.match_weights.capacity() > 0)
                    .saturating_mul(HEAP_ALLOCATION_CHARGE),
            )
            .saturating_add(self.match_weights.iter().fold(0usize, |bytes, weights| {
                bytes.saturating_add(charged_allocation(
                    weights.len().saturating_mul(std::mem::size_of::<i64>()),
                ))
            }))
            .saturating_add(self.index.capacity().saturating_mul(HASH_BUCKET_CHARGE))
            .saturating_add(
                usize::from(self.index.capacity() > 0).saturating_mul(HEAP_ALLOCATION_CHARGE),
            )
            .saturating_add(self.index_entry_bytes)
    }

    fn worst_case_input_growth(
        &self,
        batches: &[RecordBatch],
        track_matches: bool,
        execution_mode: JoinExecutionMode,
    ) -> Result<usize, DbError> {
        let (rows, batch_bytes) = batches.iter().try_fold(
            (0usize, 0usize),
            |(rows, bytes), batch| -> Result<_, DbError> {
                let rows = rows.checked_add(batch.num_rows()).ok_or_else(|| {
                    DbError::BackpressureFail("interval join input row accounting overflow".into())
                })?;
                let bytes = bytes
                    .checked_add(batch.get_array_memory_size())
                    .and_then(|total| total.checked_add(batch_metadata_charge(batch)))
                    .and_then(|total| {
                        if execution_mode == JoinExecutionMode::Weighted {
                            total
                                .checked_add(charged_allocation(
                                    batch.num_rows().checked_mul(std::mem::size_of::<i64>())?,
                                ))?
                                .checked_add(std::mem::size_of::<Arc<[i64]>>())
                        } else {
                            Some(total)
                        }
                    })
                    .and_then(|total| {
                        if track_matches {
                            let support_bytes = if execution_mode == JoinExecutionMode::AppendOnly {
                                batch.num_rows()
                            } else {
                                batch.num_rows().checked_mul(std::mem::size_of::<i64>())?
                            };
                            let roster_entry_bytes =
                                if execution_mode == JoinExecutionMode::AppendOnly {
                                    std::mem::size_of::<Arc<[u8]>>()
                                } else {
                                    std::mem::size_of::<Arc<[i64]>>()
                                };
                            total
                                .checked_add(charged_allocation(support_bytes))?
                                .checked_add(roster_entry_bytes)
                        } else {
                            Some(total)
                        }
                    })
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
    execution_mode: JoinExecutionMode,
}

impl IntervalJoinState {
    pub(crate) fn new() -> Self {
        Self::new_with_mode(JoinExecutionMode::AppendOnly)
    }

    #[cfg(test)]
    pub(crate) fn new_weighted() -> Self {
        Self::new_with_mode(JoinExecutionMode::Weighted)
    }

    pub(crate) fn new_weighted_at_frontiers(
        left_watermark: i64,
        right_watermark: i64,
        bound_ms: i64,
    ) -> Self {
        let mut state = Self::new_with_mode(JoinExecutionMode::Weighted);
        state.left_evicted_cutoff = right_watermark.saturating_sub(bound_ms);
        state.right_evicted_cutoff = left_watermark;
        state
    }

    pub(crate) fn weighted_empty_state_preflight(
        left: &Schema,
        right: &Schema,
    ) -> Result<usize, DbError> {
        let schema_bytes = |schema: &Schema| -> Result<usize, DbError> {
            schema
                .fields()
                .iter()
                .try_fold(BATCH_METADATA_CHARGE, |bytes, field| {
                    bytes
                        .checked_add(ARRAY_METADATA_CHARGE)
                        .and_then(|bytes| bytes.checked_add(field.name().len()))
                        .ok_or_else(|| {
                            DbError::BackpressureFail(
                                "interval join schema preflight accounting overflow".into(),
                            )
                        })
                })
        };
        let left_bytes = schema_bytes(left)?;
        let right_bytes = schema_bytes(right)?;
        let output_fields = left
            .fields()
            .len()
            .checked_add(right.fields().len())
            .and_then(|fields| fields.checked_add(1))
            .ok_or_else(|| {
                DbError::BackpressureFail("interval join output schema preflight overflow".into())
            })?;
        let output_names = left
            .fields()
            .iter()
            .chain(right.fields())
            .try_fold(
                laminar_core::changelog::WEIGHT_COLUMN.len(),
                |bytes, field| bytes.checked_add(field.name().len()),
            )
            .ok_or_else(|| {
                DbError::BackpressureFail("interval join output schema preflight overflow".into())
            })?;
        let output_bytes = output_fields
            .checked_mul(ARRAY_METADATA_CHARGE)
            .and_then(|bytes| bytes.checked_add(BATCH_METADATA_CHARGE))
            .and_then(|bytes| bytes.checked_add(output_names))
            .ok_or_else(|| {
                DbError::BackpressureFail("interval join output schema preflight overflow".into())
            })?;
        std::mem::size_of::<Self>()
            .checked_add(HEAP_ALLOCATION_CHARGE)
            .and_then(|bytes| bytes.checked_add(left_bytes))
            .and_then(|bytes| bytes.checked_add(right_bytes))
            .and_then(|bytes| bytes.checked_add(output_bytes))
            .ok_or_else(|| {
                DbError::BackpressureFail(
                    "interval join weighted construction preflight overflow".into(),
                )
            })
    }

    fn new_with_mode(execution_mode: JoinExecutionMode) -> Self {
        Self {
            left: SideState::new(),
            right: SideState::new(),
            left_evicted_cutoff: i64::MIN,
            right_evicted_cutoff: i64::MIN,
            left_schema: None,
            right_schema: None,
            output_schema: None,
            execution_mode,
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

    fn preflight_input_growth(
        &self,
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
        join_type: JoinType,
        execution_mode: JoinExecutionMode,
        max_state_bytes: usize,
    ) -> Result<(), DbError> {
        let growth = self
            .left
            .worst_case_input_growth(left_batches, tracks_left_matches(join_type), execution_mode)?
            .checked_add(self.right.worst_case_input_growth(
                right_batches,
                tracks_right_matches(join_type),
                execution_mode,
            )?)
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

    #[cfg(any(test, feature = "cluster"))]
    pub(crate) const fn buffered_rows(&self) -> (usize, usize) {
        (self.left.row_count, self.right.row_count)
    }

    pub(crate) const fn evicted_cutoffs(&self) -> (i64, i64) {
        (self.left_evicted_cutoff, self.right_evicted_cutoff)
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
        self.cache_input_schemas(None, None, config, self.execution_mode)
    }

    fn cache_input_schemas(
        &mut self,
        left: Option<SchemaRef>,
        right: Option<SchemaRef>,
        config: &StreamJoinConfig,
        execution_mode: JoinExecutionMode,
    ) -> Result<(), DbError> {
        if self.execution_mode != execution_mode {
            return Err(DbError::InvalidOperation(
                "interval join execution mode changed while state was retained".into(),
            ));
        }
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
            self.output_schema = Some(build_output_schema_for_mode(
                left,
                right,
                config,
                execution_mode,
            ));
        }
        Ok(())
    }

    fn finalize_closed_rows(
        &mut self,
        config: &StreamJoinConfig,
        left_watermark: i64,
        right_watermark: i64,
        force: bool,
        execution_mode: JoinExecutionMode,
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
                .unmatched_positions_before(left_cutoff, remaining_rows, execution_mode)?
        } else {
            Vec::new()
        };
        remaining_rows = remaining_rows.saturating_sub(left_positions.len());
        let right_positions = if close_right && emit_right {
            self.right
                .unmatched_positions_before(right_cutoff, remaining_rows, execution_mode)?
        } else {
            Vec::new()
        };

        let mut rows = Vec::new();
        let mut weights = Vec::new();
        if emit_left {
            for &(batch, row) in &left_positions {
                rows.push(JoinOutputRow {
                    left: Some((batch, row)),
                    right: None,
                });
                if execution_mode.is_weighted() {
                    weights.push(self.left.row_weight(batch, row)?);
                }
            }
        }
        if emit_right {
            for &(batch, row) in &right_positions {
                rows.push(JoinOutputRow {
                    left: None,
                    right: Some((batch, row)),
                });
                if execution_mode.is_weighted() {
                    weights.push(self.right.row_weight(batch, row)?);
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
                &mut weights,
                output_schema,
                left_schema,
                right_schema,
                config.join_type,
                &self.left.batches,
                &self.right.batches,
                &self.left.row_bytes,
                &self.right.row_bytes,
                execution_mode,
                &mut output,
                output_budget,
            )?;
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

    fn capture_preflight_bytes(
        &self,
        config: &StreamJoinConfig,
    ) -> Result<(usize, usize, usize), DbError> {
        fn roster<T>(len: usize) -> Result<usize, DbError> {
            len.checked_mul(std::mem::size_of::<T>())
                .map(charged_allocation)
                .ok_or_else(|| {
                    DbError::Checkpoint("interval join capture roster accounting overflow".into())
                })
        }

        let config_capture_bytes = config
            .left_keys
            .iter()
            .chain(&config.right_keys)
            .map(|value| charged_allocation(value.len()))
            .chain([
                charged_allocation(config.left_time_column.len()),
                charged_allocation(config.right_time_column.len()),
                charged_allocation(config.left_table.len()),
                charged_allocation(config.right_table.len()),
            ])
            .try_fold(0usize, usize::checked_add)
            .and_then(|bytes| {
                roster::<String>(config.left_keys.len())
                    .ok()
                    .and_then(|left| bytes.checked_add(left))
            })
            .and_then(|bytes| {
                roster::<String>(config.right_keys.len())
                    .ok()
                    .and_then(|right| bytes.checked_add(right))
            })
            .ok_or_else(|| {
                DbError::Checkpoint("interval join capture config accounting overflow".into())
            })?;
        let left_batch_clones =
            shallow_batch_clone_charge(&self.left.batches, self.left.batches.len())?;
        let right_batch_clones =
            shallow_batch_clone_charge(&self.right.batches, self.right.batches.len())?;
        let non_batch_side_rosters = [
            roster::<Arc<[i64]>>(self.left.row_weights.len())?,
            roster::<Arc<[u8]>>(self.left.match_flags.len())?,
            roster::<Arc<[i64]>>(self.left.match_weights.len())?,
            roster::<Arc<[i64]>>(self.right.row_weights.len())?,
            roster::<Arc<[u8]>>(self.right.match_flags.len())?,
            roster::<Arc<[i64]>>(self.right.match_weights.len())?,
        ]
        .into_iter()
        .try_fold(0usize, usize::checked_add)
        .ok_or_else(|| {
            DbError::Checkpoint("interval join capture roster accounting overflow".into())
        })?;
        let fixed_capture_bytes = self
            .accounted_state_bytes()
            .checked_add(std::mem::size_of::<IntervalJoinCheckpointCapture>())
            .and_then(|bytes| bytes.checked_add(config_capture_bytes))
            .and_then(|bytes| bytes.checked_add(non_batch_side_rosters))
            .ok_or_else(|| {
                DbError::Checkpoint("interval join capture accounting overflow".into())
            })?;
        Ok((fixed_capture_bytes, left_batch_clones, right_batch_clones))
    }

    pub(crate) fn capture_checkpoint(
        &self,
        config: &StreamJoinConfig,
        max_capture_bytes: usize,
    ) -> Result<IntervalJoinCheckpointCapture, DbError> {
        type CapturedSide = (
            Vec<RecordBatch>,
            Vec<Arc<[i64]>>,
            Vec<Arc<[u8]>>,
            Vec<Arc<[i64]>>,
            u64,
            usize,
        );

        fn capture_side(
            side: &str,
            state: &SideState,
            track_matches: bool,
            execution_mode: JoinExecutionMode,
            max_batch_clone_bytes: usize,
        ) -> Result<CapturedSide, DbError> {
            if (execution_mode == JoinExecutionMode::Weighted
                && state.batches.len() != state.row_weights.len())
                || (execution_mode == JoinExecutionMode::AppendOnly
                    && !state.row_weights.is_empty())
                || (track_matches
                    && execution_mode == JoinExecutionMode::AppendOnly
                    && state.batches.len() != state.match_flags.len())
                || (track_matches
                    && execution_mode == JoinExecutionMode::Weighted
                    && state.batches.len() != state.match_weights.len())
                || (!track_matches
                    && (!state.match_flags.is_empty() || !state.match_weights.is_empty()))
                || (execution_mode == JoinExecutionMode::AppendOnly
                    && !state.match_weights.is_empty())
                || (execution_mode == JoinExecutionMode::Weighted && !state.match_flags.is_empty())
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join {side} checkpoint weight rosters do not match its batch roster"
                )));
            }
            let mut retained_rows = 0usize;
            for (batch_index, batch) in state.batches.iter().enumerate() {
                let row_weights = execution_mode
                    .is_weighted()
                    .then(|| &state.row_weights[batch_index]);
                if batch.num_rows() == 0 {
                    return Err(DbError::Checkpoint(format!(
                        "interval join {side} checkpoint contains an empty batch"
                    )));
                }
                if row_weights.is_some_and(|weights| {
                    weights.len() != batch.num_rows() || weights.contains(&0)
                }) || (track_matches
                    && execution_mode == JoinExecutionMode::AppendOnly
                    && (state.match_flags[batch_index].len() != batch.num_rows()
                        || state.match_flags[batch_index].iter().any(|flag| *flag > 1)))
                    || (track_matches
                        && execution_mode == JoinExecutionMode::Weighted
                        && (state.match_weights[batch_index].len() != batch.num_rows()
                            || state.match_weights[batch_index]
                                .iter()
                                .any(|weight| *weight < 0)))
                {
                    return Err(DbError::Checkpoint(format!(
                        "interval join {side} checkpoint contains invalid row or match weights"
                    )));
                }
                retained_rows = retained_rows.checked_add(batch.num_rows()).ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join {side} checkpoint row-count overflow"
                    ))
                })?;
            }
            if retained_rows != state.retained_rows || state.row_count > retained_rows {
                return Err(DbError::Checkpoint(format!(
                    "interval join {side} checkpoint row-count metadata is inconsistent"
                )));
            }
            let rows = u64::try_from(state.row_count).map_err(|_| {
                DbError::Checkpoint(format!("interval join {side} row count does not fit u64"))
            })?;
            let mut batches = Vec::new();
            batches
                .try_reserve_exact(state.batches.len())
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "interval join {side} checkpoint batch roster cannot be reserved"
                    ))
                })?;
            let batch_clone_bytes = shallow_batch_clone_charge(&state.batches, batches.capacity())?;
            if batch_clone_bytes > max_batch_clone_bytes {
                return Err(DbError::Checkpoint(format!(
                    "interval join {side} checkpoint batch clones require {batch_clone_bytes} bytes; remaining shallow-clone limit is {max_batch_clone_bytes} bytes"
                )));
            }
            batches.extend(state.batches.iter().cloned());
            let mut row_weights = Vec::new();
            row_weights
                .try_reserve_exact(state.row_weights.len())
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "interval join {side} checkpoint row-weight roster cannot be reserved"
                    ))
                })?;
            row_weights.extend(state.row_weights.iter().cloned());
            let mut match_flags = Vec::new();
            match_flags
                .try_reserve_exact(state.match_flags.len())
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "interval join {side} checkpoint match-flag roster cannot be reserved"
                    ))
                })?;
            match_flags.extend(state.match_flags.iter().cloned());
            let mut match_weights = Vec::new();
            match_weights
                .try_reserve_exact(state.match_weights.len())
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "interval join {side} checkpoint match-weight roster cannot be reserved"
                    ))
                })?;
            match_weights.extend(state.match_weights.iter().cloned());
            Ok((
                batches,
                row_weights,
                match_flags,
                match_weights,
                rows,
                batch_clone_bytes,
            ))
        }

        let (fixed_capture_bytes, left_batch_clones, right_batch_clones) =
            self.capture_preflight_bytes(config)?;
        let projected_capture_bytes = fixed_capture_bytes
            .checked_add(left_batch_clones)
            .and_then(|bytes| bytes.checked_add(right_batch_clones))
            .ok_or_else(|| {
                DbError::Checkpoint("interval join capture accounting overflow".into())
            })?;
        if projected_capture_bytes > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join capture requires {projected_capture_bytes} bytes; remaining capture limit is {max_capture_bytes} bytes"
            )));
        }

        let bound_ms = i64::try_from(config.time_bound.as_millis()).map_err(|_| {
            DbError::InvalidOperation(
                "interval join time bound exceeds the supported millisecond range".into(),
            )
        })?;
        let execution_mode = self.execution_mode;
        let (
            left_batches,
            left_row_weights,
            left_match_flags,
            left_match_weights,
            left_buffer_rows,
            left_batch_clone_bytes,
        ) = capture_side(
            "left",
            &self.left,
            tracks_left_matches(config.join_type),
            execution_mode,
            max_capture_bytes
                .checked_sub(fixed_capture_bytes)
                .and_then(|bytes| bytes.checked_sub(right_batch_clones))
                .expect("capture preflight validated left shallow-clone headroom"),
        )?;
        let (
            right_batches,
            right_row_weights,
            right_match_flags,
            right_match_weights,
            right_buffer_rows,
            _right_batch_clone_bytes,
        ) = capture_side(
            "right",
            &self.right,
            tracks_right_matches(config.join_type),
            execution_mode,
            max_capture_bytes
                .checked_sub(fixed_capture_bytes)
                .and_then(|bytes| bytes.checked_sub(left_batch_clone_bytes))
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "interval join left shallow capture exhausted right-side headroom".into(),
                    )
                })?,
        )?;
        let mut capture = IntervalJoinCheckpointCapture {
            execution_mode,
            config: config.clone(),
            bound_ms,
            left_buffer_rows,
            right_buffer_rows,
            left_batches,
            right_batches,
            left_evicted_cutoff: self.left_evicted_cutoff,
            right_evicted_cutoff: self.right_evicted_cutoff,
            left_row_weights,
            right_row_weights,
            left_match_flags,
            right_match_flags,
            left_match_weights,
            right_match_weights,
            left_needs_compaction: !self.left.is_compact(),
            right_needs_compaction: !self.right.is_compact(),
            retained_bytes: 0,
        };
        capture.retained_bytes = capture.calculate_retained_bytes();
        if capture.retained_bytes > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join capture retains {} bytes; remaining capture limit is {max_capture_bytes} bytes",
                capture.retained_bytes
            )));
        }
        Ok(capture)
    }

    #[cfg(test)]
    pub(crate) fn snapshot_checkpoint(
        &mut self,
        config: &StreamJoinConfig,
        max_encoded_bytes: usize,
    ) -> Result<JoinStateCheckpoint, DbError> {
        self.capture_checkpoint(config, max_encoded_bytes)?
            .encode(max_encoded_bytes)
    }

    /// Restores from a checkpoint, rebuilding the index from deserialized batches.
    pub(crate) fn from_checkpoint(
        cp: &JoinStateCheckpoint,
        config: &StreamJoinConfig,
        max_state_bytes: usize,
    ) -> Result<Self, DbError> {
        Self::from_checkpoint_with_mode(cp, config, max_state_bytes, JoinExecutionMode::AppendOnly)
    }

    /// Restore the private differential kernel used only behind ordered-input normalization.
    pub(crate) fn from_weighted_checkpoint(
        cp: &JoinStateCheckpoint,
        config: &StreamJoinConfig,
        max_state_bytes: usize,
    ) -> Result<Self, DbError> {
        Self::from_checkpoint_with_mode(cp, config, max_state_bytes, JoinExecutionMode::Weighted)
    }

    fn from_checkpoint_with_mode(
        cp: &JoinStateCheckpoint,
        config: &StreamJoinConfig,
        max_state_bytes: usize,
        execution_mode: JoinExecutionMode,
    ) -> Result<Self, DbError> {
        fn preflight_side_metadata(
            side: &str,
            batch_count: usize,
            expected_rows: usize,
            row_weights: &[Vec<i64>],
            match_flags: &[Vec<u8>],
            match_weights: &[Vec<i64>],
            track_matches: bool,
            weighted: bool,
        ) -> Result<(), DbError> {
            if batch_count > expected_rows
                || (weighted && row_weights.len() != batch_count)
                || (!weighted && !row_weights.is_empty())
                || (track_matches && weighted && match_weights.len() != batch_count)
                || (track_matches && !weighted && match_flags.len() != batch_count)
                || (!track_matches && (!match_flags.is_empty() || !match_weights.is_empty()))
                || (weighted && !match_flags.is_empty())
                || (!weighted && !match_weights.is_empty())
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join {side} checkpoint has invalid weight rosters"
                )));
            }
            let mut metadata_rows = None;
            if weighted {
                let mut rows = 0usize;
                for weights in row_weights {
                    if weights.is_empty() || weights.contains(&0) {
                        return Err(DbError::Checkpoint(format!(
                            "interval join {side} checkpoint has invalid row weights"
                        )));
                    }
                    rows = rows.checked_add(weights.len()).ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join {side} checkpoint row-count overflow"
                        ))
                    })?;
                }
                metadata_rows = Some(rows);
            }
            if track_matches && weighted {
                let mut rows = 0usize;
                for (batch_index, weights) in match_weights.iter().enumerate() {
                    if weights.is_empty()
                        || weights.iter().any(|weight| *weight < 0)
                        || weights.len() != row_weights[batch_index].len()
                    {
                        return Err(DbError::Checkpoint(format!(
                            "interval join {side} checkpoint has invalid match weights"
                        )));
                    }
                    rows = rows.checked_add(weights.len()).ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join {side} checkpoint row-count overflow"
                        ))
                    })?;
                }
                if metadata_rows.is_some_and(|expected| expected != rows) {
                    return Err(DbError::Checkpoint(format!(
                        "interval join {side} checkpoint weight row counts differ"
                    )));
                }
                metadata_rows = Some(rows);
            }
            if track_matches && !weighted {
                let mut rows = 0usize;
                for flags in match_flags {
                    if flags.is_empty() || flags.iter().any(|flag| *flag > 1) {
                        return Err(DbError::Checkpoint(format!(
                            "interval join {side} checkpoint has invalid match flags"
                        )));
                    }
                    rows = rows.checked_add(flags.len()).ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join {side} checkpoint row-count overflow"
                        ))
                    })?;
                }
                metadata_rows = Some(rows);
            }
            if metadata_rows.is_some_and(|rows| rows != expected_rows) {
                return Err(DbError::Checkpoint(format!(
                    "interval join {side} checkpoint row-count metadata is inconsistent"
                )));
            }
            Ok(())
        }

        fn decode_side(
            side: &str,
            ipc_batches: &[Vec<u8>],
            row_weights: &[Vec<i64>],
            match_flags: &[Vec<u8>],
            match_weights: &[Vec<i64>],
            track_matches: bool,
            weighted: bool,
            expected_rows: usize,
            decoded_charge: &mut usize,
            max_state_bytes: usize,
        ) -> Result<Vec<RecordBatch>, DbError> {
            let mut decoded = Vec::new();
            decoded.try_reserve_exact(ipc_batches.len()).map_err(|_| {
                DbError::Checkpoint(format!(
                    "interval join {side} checkpoint batch roster cannot be reserved"
                ))
            })?;
            *decoded_charge = decoded_charge
                .checked_add(charged_allocation(
                    decoded
                        .capacity()
                        .checked_mul(std::mem::size_of::<RecordBatch>())
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "interval join {side} decoded roster accounting overflow"
                            ))
                        })?,
                ))
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join {side} decoded roster accounting overflow"
                    ))
                })?;
            if *decoded_charge > max_state_bytes {
                return Err(DbError::Checkpoint(format!(
                    "interval join cumulative decoded state exceeds the {max_state_bytes}-byte restore limit before batch decode"
                )));
            }
            let mut rows = 0usize;
            let mut schema: Option<SchemaRef> = None;
            for (batch_index, ipc_bytes) in ipc_batches.iter().enumerate() {
                let row_weights = weighted.then(|| &row_weights[batch_index]);
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
                let match_flags = (track_matches && !weighted).then(|| &match_flags[batch_index]);
                let match_weights =
                    (track_matches && weighted).then(|| &match_weights[batch_index]);
                if row_weights.is_some_and(|weights| weights.len() != batch.num_rows())
                    || match_weights.is_some_and(|weights| weights.len() != batch.num_rows())
                    || match_flags.is_some_and(|flags| flags.len() != batch.num_rows())
                {
                    return Err(DbError::Checkpoint(format!(
                        "interval join {side} checkpoint row and weight shapes differ"
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
                    .and_then(|bytes| {
                        bytes.checked_add(charged_allocation(
                            row_weights
                                .map_or(0, Vec::len)
                                .checked_mul(std::mem::size_of::<i64>())?,
                        ))
                    })
                    .and_then(|bytes| {
                        bytes.checked_add(charged_allocation(
                            match_weights
                                .map_or(0, Vec::len)
                                .checked_mul(std::mem::size_of::<i64>())?,
                        ))
                    })
                    .and_then(|bytes| {
                        bytes.checked_add(charged_allocation(match_flags.map_or(0, Vec::len)))
                    })
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

        if max_state_bytes == 0 {
            return Err(DbError::Checkpoint(
                "interval join restore state limit must be greater than zero".into(),
            ));
        }
        if cp.weighted != execution_mode.is_weighted() {
            return Err(DbError::Checkpoint(
                "interval join checkpoint execution mode does not match the restored operator"
                    .into(),
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
        preflight_side_metadata(
            "left",
            cp.left_batches.len(),
            expected_left,
            &cp.left_row_weights,
            &cp.left_match_flags,
            &cp.left_match_weights,
            tracks_left_matches(config.join_type),
            execution_mode.is_weighted(),
        )?;
        preflight_side_metadata(
            "right",
            cp.right_batches.len(),
            expected_right,
            &cp.right_row_weights,
            &cp.right_match_flags,
            &cp.right_match_weights,
            tracks_right_matches(config.join_type),
            execution_mode.is_weighted(),
        )?;
        let retained_checkpoint_bytes = cp.retained_ipc_bytes()?;
        if retained_checkpoint_bytes > max_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join checkpoint retains {retained_checkpoint_bytes} bytes before decode; restore limit is {max_state_bytes} bytes"
            )));
        }
        let worst_case_row_bytes = expected_rows
            .checked_mul(RESTORE_WORST_CASE_ROW_CHARGE)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "interval join checkpoint worst-case row accounting overflow".into(),
                )
            })?;
        let predecode_projection = std::mem::size_of::<Self>()
            .checked_add(retained_checkpoint_bytes)
            .and_then(|bytes| bytes.checked_add(worst_case_row_bytes))
            .ok_or_else(|| {
                DbError::Checkpoint("interval join checkpoint restore projection overflow".into())
            })?;
        if predecode_projection > max_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join checkpoint declares {expected_rows} rows whose retained checkpoint plus worst-case decoded index charge is {predecode_projection} bytes; restore limit is {max_state_bytes} bytes"
            )));
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
            &cp.left_row_weights,
            &cp.left_match_flags,
            &cp.left_match_weights,
            tracks_left_matches(config.join_type),
            execution_mode.is_weighted(),
            expected_left,
            &mut decoded_charge,
            max_state_bytes,
        )?;
        let right_batches = decode_side(
            "right",
            &cp.right_batches,
            &cp.right_row_weights,
            &cp.right_match_flags,
            &cp.right_match_weights,
            tracks_right_matches(config.join_type),
            execution_mode.is_weighted(),
            expected_right,
            &mut decoded_charge,
            max_state_bytes,
        )?;

        let mut state = Self::new_with_mode(execution_mode);
        state.left_evicted_cutoff = cp.left_evicted_cutoff;
        state.right_evicted_cutoff = cp.right_evicted_cutoff;

        for (batch_index, batch) in left_batches.into_iter().enumerate() {
            let row_weights = if execution_mode.is_weighted() {
                Some(Arc::from(cp.left_row_weights[batch_index].clone()))
            } else {
                None
            };
            let _ = state
                .left
                .add_batch(
                    &batch,
                    row_weights,
                    &config.left_keys,
                    &config.left_time_column,
                    retain_left_null_tuples(config.join_type),
                    tracks_left_matches(config.join_type),
                    execution_mode,
                )
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "interval join left checkpoint index rebuild: {error}"
                    ))
                })?;
            if tracks_left_matches(config.join_type) {
                if execution_mode == JoinExecutionMode::AppendOnly {
                    *state.left.match_flags.last_mut().ok_or_else(|| {
                        DbError::Checkpoint(
                            "interval join left match flags lost during restore".into(),
                        )
                    })? = Arc::from(cp.left_match_flags[batch_index].clone());
                } else {
                    *state.left.match_weights.last_mut().ok_or_else(|| {
                        DbError::Checkpoint(
                            "interval join left match weights lost during restore".into(),
                        )
                    })? = Arc::from(cp.left_match_weights[batch_index].clone());
                }
            }
            state.left_schema = Some(batch.schema());
            if state.accounted_state_bytes() > max_state_bytes {
                return Err(DbError::Checkpoint(format!(
                    "interval join restored state exceeds the {max_state_bytes}-byte limit while rebuilding the left index"
                )));
            }
        }

        for (batch_index, batch) in right_batches.into_iter().enumerate() {
            let row_weights = if execution_mode.is_weighted() {
                Some(Arc::from(cp.right_row_weights[batch_index].clone()))
            } else {
                None
            };
            let _ = state
                .right
                .add_batch(
                    &batch,
                    row_weights,
                    &config.right_keys,
                    &config.right_time_column,
                    retain_right_null_tuples(config.join_type),
                    tracks_right_matches(config.join_type),
                    execution_mode,
                )
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "interval join right checkpoint index rebuild: {error}"
                    ))
                })?;
            if tracks_right_matches(config.join_type) {
                if execution_mode == JoinExecutionMode::AppendOnly {
                    *state.right.match_flags.last_mut().ok_or_else(|| {
                        DbError::Checkpoint(
                            "interval join right match flags lost during restore".into(),
                        )
                    })? = Arc::from(cp.right_match_flags[batch_index].clone());
                } else {
                    *state.right.match_weights.last_mut().ok_or_else(|| {
                        DbError::Checkpoint(
                            "interval join right match weights lost during restore".into(),
                        )
                    })? = Arc::from(cp.right_match_weights[batch_index].clone());
                }
            }
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

fn build_output_schema_for_mode(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    config: &StreamJoinConfig,
    execution_mode: JoinExecutionMode,
) -> SchemaRef {
    let plain = build_output_schema(left_schema, right_schema, config);
    if execution_mode == JoinExecutionMode::AppendOnly {
        return plain;
    }
    let mut fields = plain.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        laminar_core::changelog::WEIGHT_COLUMN,
        DataType::Int64,
        false,
    )));
    Arc::new(Schema::new_with_metadata(fields, plain.metadata().clone()))
}

/// Exact private-kernel output schema used to validate a weighted post-projection before intake.
pub(crate) fn build_weighted_output_schema(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    config: &StreamJoinConfig,
) -> SchemaRef {
    build_output_schema_for_mode(
        left_schema,
        right_schema,
        config,
        JoinExecutionMode::Weighted,
    )
}

#[derive(Clone, Copy)]
struct JoinOutputRow {
    left: Option<(usize, usize)>,
    right: Option<(usize, usize)>,
}

#[allow(clippy::too_many_arguments)]
fn flush_output_rows(
    rows: &mut Vec<JoinOutputRow>,
    weights: &mut Vec<i64>,
    output_schema: &SchemaRef,
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    join_type: JoinType,
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    left_row_bytes: &[Vec<usize>],
    right_row_bytes: &[Vec<usize>],
    execution_mode: JoinExecutionMode,
    out: &mut Vec<RecordBatch>,
    output_budget: &mut IntervalJoinOutputBudget,
) -> Result<(), DbError> {
    if (execution_mode.is_weighted() && weights.len() != rows.len())
        || (!execution_mode.is_weighted() && !weights.is_empty())
    {
        return Err(DbError::PipelineTerminal(
            "interval join output weight roster is inconsistent with its execution mode".into(),
        ));
    }
    if rows.is_empty() {
        return Ok(());
    }

    let next_rows = output_budget.emitted_rows.saturating_add(rows.len());
    if next_rows > MAX_CYCLE_OUTPUT_ROWS {
        return Err(DbError::BackpressureFail(format!(
            "interval join cycle exceeded {MAX_CYCLE_OUTPUT_ROWS} output rows; narrow the event-time bound or reduce hot-key fanout"
        )));
    }
    let payload_bytes = rows.iter().try_fold(0usize, |total, row| {
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
    let weight_bytes = if execution_mode == JoinExecutionMode::Weighted {
        rows.len()
            .checked_mul(std::mem::size_of::<i64>())
            .and_then(|bytes| bytes.checked_add(rows.len().saturating_add(7) / 8))
            .ok_or_else(|| {
                DbError::BackpressureFail(
                    "interval join weighted output byte accounting overflow".into(),
                )
            })?
    } else {
        0
    };
    let logical_bytes = payload_bytes.checked_add(weight_bytes).ok_or_else(|| {
        DbError::BackpressureFail("interval join output byte accounting overflow".into())
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
                DbError::query_pipeline_arrow(format!("interval join (interleave {side})"), &error)
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
    if execution_mode == JoinExecutionMode::Weighted {
        columns.push(Arc::new(Int64Array::from_iter_values(
            weights.iter().copied(),
        )));
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
    weights.clear();
    Ok(())
}

struct NormalizedJoinInput<'a> {
    batches: Cow<'a, [RecordBatch]>,
    row_weights: Vec<Arc<[i64]>>,
}

fn validate_normalized_join_batch(
    side: &str,
    batch: &RecordBatch,
    key_columns: &[String],
    time_column: &str,
    closed_cutoff: i64,
    execution_mode: JoinExecutionMode,
) -> Result<(), DbError> {
    if execution_mode == JoinExecutionMode::AppendOnly {
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
    }

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
    Ok(())
}

fn normalize_join_input<'a>(
    side: &str,
    batches: &'a [RecordBatch],
    key_columns: &[String],
    time_column: &str,
    closed_cutoff: i64,
    execution_mode: JoinExecutionMode,
) -> Result<NormalizedJoinInput<'a>, DbError> {
    if execution_mode == JoinExecutionMode::AppendOnly
        && batches.iter().all(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .all(|field| field.name() != laminar_core::changelog::WEIGHT_COLUMN)
        })
    {
        for batch in batches {
            validate_normalized_join_batch(
                side,
                batch,
                key_columns,
                time_column,
                closed_cutoff,
                execution_mode,
            )?;
        }
        return Ok(NormalizedJoinInput {
            batches: Cow::Borrowed(batches),
            row_weights: Vec::new(),
        });
    }

    let mut normalized = Vec::new();
    let mut row_weights = Vec::new();
    normalized.try_reserve_exact(batches.len()).map_err(|_| {
        DbError::BackpressureFail(format!(
            "interval join ({side}) input roster cannot be reserved"
        ))
    })?;
    if execution_mode.is_weighted() {
        row_weights.try_reserve_exact(batches.len()).map_err(|_| {
            DbError::BackpressureFail(format!(
                "interval join ({side}) weight roster cannot be reserved"
            ))
        })?;
    }

    for batch in batches {
        let schema = batch.schema();
        let weight_indices = schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                (field.name() == laminar_core::changelog::WEIGHT_COLUMN).then_some(index)
            })
            .collect::<Vec<_>>();
        let (plain, weights): (RecordBatch, Option<Arc<[i64]>>) = match weight_indices.as_slice() {
            [] if execution_mode == JoinExecutionMode::AppendOnly => (batch.clone(), None),
            [] => {
                return Err(DbError::InvalidOperation(format!(
                    "interval join ({side}) weighted input is missing trailing {}",
                    laminar_core::changelog::WEIGHT_COLUMN
                )));
            }
            [index] if *index == batch.num_columns().saturating_sub(1) => {
                let field = schema.field(*index);
                if field.data_type() != &DataType::Int64 || field.is_nullable() {
                    return Err(DbError::SchemaMismatch(format!(
                        "interval join ({side}) {} must be non-null Int64",
                        laminar_core::changelog::WEIGHT_COLUMN
                    )));
                }
                let weights = batch
                    .column(*index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DbError::Pipeline(format!(
                            "interval join ({side}) {} has an invalid Arrow representation",
                            laminar_core::changelog::WEIGHT_COLUMN
                        ))
                    })?;
                if let Some((row, weight)) = weights.iter().enumerate().find(|(_, weight)| {
                    execution_mode == JoinExecutionMode::AppendOnly && *weight != Some(1)
                        || execution_mode == JoinExecutionMode::Weighted
                            && weight.is_none_or(|weight| weight == 0)
                }) {
                    let requirement = if execution_mode == JoinExecutionMode::AppendOnly {
                        "+1"
                    } else {
                        "nonzero"
                    };
                    return Err(DbError::InvalidOperation(format!(
                        "interval join ({side}) requires {requirement} weights; row {row} has weight {}",
                        weight.map_or_else(|| "NULL".to_string(), |value| value.to_string())
                    )));
                }
                let plain_schema = Arc::new(Schema::new_with_metadata(
                    schema.fields()[..*index].to_vec(),
                    schema.metadata().clone(),
                ));
                let plain = RecordBatch::try_new(plain_schema, batch.columns()[..*index].to_vec())
                    .map_err(|error| {
                        DbError::query_pipeline_arrow(
                            format!("interval join ({side}) strip weight"),
                            &error,
                        )
                    })?;
                let retained_weights = if execution_mode.is_weighted() {
                    Some(Arc::from(
                        weights.iter().map(Option::unwrap).collect::<Vec<_>>(),
                    ))
                } else {
                    None
                };
                (plain, retained_weights)
            }
            _ => {
                return Err(DbError::SchemaMismatch(format!(
                    "interval join ({side}) requires a sole trailing {} column",
                    laminar_core::changelog::WEIGHT_COLUMN
                )));
            }
        };

        validate_normalized_join_batch(
            side,
            &plain,
            key_columns,
            time_column,
            closed_cutoff,
            execution_mode,
        )?;
        normalized.push(plain);
        if let Some(weights) = weights {
            row_weights.push(weights);
        }
    }
    Ok(NormalizedJoinInput {
        batches: Cow::Owned(normalized),
        row_weights,
    })
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

fn next_match_support(
    current: i64,
    delta: i64,
    execution_mode: JoinExecutionMode,
) -> Result<i64, DbError> {
    let next = if execution_mode == JoinExecutionMode::AppendOnly {
        if delta != 1 || !(0..=1).contains(&current) {
            return Err(DbError::PipelineTerminal(
                "interval join append-only match support is corrupt".into(),
            ));
        }
        1
    } else {
        current.checked_add(delta).ok_or_else(|| {
            DbError::PipelineTerminal("interval join match-support overflow".into())
        })?
    };
    if next < 0 {
        return Err(DbError::PipelineTerminal(format!(
            "interval join match support became negative ({current} + {delta})"
        )));
    }
    Ok(next)
}

fn semi_transition_weight(
    row_weight: i64,
    previous_support: i64,
    next_support: i64,
) -> Result<Option<i64>, DbError> {
    match (previous_support > 0, next_support > 0) {
        (false, true) => Ok(Some(row_weight)),
        (true, false) => row_weight.checked_neg().map(Some).ok_or_else(|| {
            DbError::PipelineTerminal("interval join semi-join retraction overflow".into())
        }),
        _ => Ok(None),
    }
}

/// Append-only cycle: new left rows probe all right, then new right rows probe old left.
#[allow(clippy::too_many_arguments)]
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
    execute_interval_join_cycle_with_mode(
        state,
        left_batches,
        right_batches,
        config,
        left_admission_watermark,
        right_admission_watermark,
        left_watermark,
        right_watermark,
        max_state_bytes,
        output_budget,
        JoinExecutionMode::AppendOnly,
    )
}

/// Differential cycle for rows already admitted by ordered-input normalizers.
///
/// The normalizers own replay and prior-cutoff validation, so this narrow wrapper deliberately
/// disables the generic kernel's late-row filter while retaining weighted schema validation.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_weighted_interval_join_cycle(
    state: &mut IntervalJoinState,
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    config: &StreamJoinConfig,
    left_watermark: i64,
    right_watermark: i64,
    max_state_bytes: usize,
    output_budget: &mut IntervalJoinOutputBudget,
) -> Result<Vec<RecordBatch>, DbError> {
    execute_interval_join_cycle_with_mode(
        state,
        left_batches,
        right_batches,
        config,
        i64::MIN,
        i64::MIN,
        left_watermark,
        right_watermark,
        max_state_bytes,
        output_budget,
        JoinExecutionMode::Weighted,
    )
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
fn execute_interval_join_cycle_with_mode(
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
    execution_mode: JoinExecutionMode,
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

    let left_input = normalize_join_input(
        "left",
        left_batches,
        &config.left_keys,
        &config.left_time_column,
        left_admission_watermark,
        execution_mode,
    )?;
    let right_input = normalize_join_input(
        "right",
        right_batches,
        &config.right_keys,
        &config.right_time_column,
        right_admission_watermark,
        execution_mode,
    )?;
    let left_schema = validate_input_schemas(
        "left",
        &state.left,
        state.left_schema.as_ref(),
        left_input.batches.as_ref(),
    )?;
    let right_schema = validate_input_schemas(
        "right",
        &state.right,
        state.right_schema.as_ref(),
        right_input.batches.as_ref(),
    )?;
    state.cache_input_schemas(left_schema, right_schema, config, execution_mode)?;

    let has_left_input = left_input.batches.iter().any(|batch| batch.num_rows() > 0);
    let has_right_input = right_input.batches.iter().any(|batch| batch.num_rows() > 0);
    if !has_left_input && !has_right_input {
        return state
            .finalize_closed_rows(
                config,
                left_watermark,
                right_watermark,
                false,
                execution_mode,
                output_budget,
            )
            .map_err(partial_apply);
    }

    state
        .preflight_input_growth(
            left_input.batches.as_ref(),
            right_input.batches.as_ref(),
            config.join_type,
            execution_mode,
            max_state_bytes,
        )
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
        let new_left = concat_nonempty(left_input.batches.as_ref(), "interval join (left concat)")?;
        let new_right =
            concat_nonempty(right_input.batches.as_ref(), "interval join (right concat)")?;
        let concat_weights = |weights: &[Arc<[i64]>], side: &str| -> Result<Arc<[i64]>, DbError> {
            let row_count = weights.iter().try_fold(0usize, |rows, weights| {
                rows.checked_add(weights.len()).ok_or_else(|| {
                    DbError::BackpressureFail(format!(
                        "interval join ({side}) weight row-count overflow"
                    ))
                })
            })?;
            let mut concatenated = Vec::new();
            concatenated.try_reserve_exact(row_count).map_err(|_| {
                DbError::BackpressureFail(format!(
                    "interval join ({side}) weights cannot be reserved"
                ))
            })?;
            for weights in weights {
                concatenated.extend_from_slice(weights);
            }
            Ok(Arc::from(concatenated))
        };
        let (new_left_weights, new_right_weights) = if execution_mode.is_weighted() {
            (
                Some(concat_weights(&left_input.row_weights, "left")?),
                Some(concat_weights(&right_input.row_weights, "right")?),
            )
        } else {
            (None, None)
        };

        // Buffer first so every output position points into retained state.
        let left_old_count = state.left.batches.len();
        let right_old_count = state.right.batches.len();
        let has_new_right = if let Some(rb) = new_right {
            state.right.add_batch(
                &rb,
                new_right_weights,
                &config.right_keys,
                &config.right_time_column,
                retain_right_null_tuples(config.join_type),
                tracks_right_matches(config.join_type),
                execution_mode,
            )?
        } else {
            false
        };
        let has_new_left = if let Some(lb) = new_left {
            state.left.add_batch(
                &lb,
                new_left_weights,
                &config.left_keys,
                &config.left_time_column,
                retain_left_null_tuples(config.join_type),
                tracks_left_matches(config.join_type),
                execution_mode,
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
        let fallback_output = state.output_schema.is_none().then(|| {
            build_output_schema_for_mode(left_schema, right_schema, config, execution_mode)
        });
        let output_schema = state
            .output_schema
            .as_ref()
            .or(fallback_output.as_ref())
            .expect("interval join fallback output schema constructed");
        let mut output_rows = Vec::new();
        let mut output_weights = Vec::new();

        macro_rules! flush_if_needed {
            () => {{
                if output_rows.len() >= EMIT_THRESHOLD {
                    flush_output_rows(
                        &mut output_rows,
                        &mut output_weights,
                        output_schema,
                        left_schema,
                        right_schema,
                        config.join_type,
                        &state.left.batches,
                        &state.right.batches,
                        &state.left.row_bytes,
                        &state.right.row_bytes,
                        execution_mode,
                        &mut result,
                        output_budget,
                    )?;
                }
            }};
        }

        macro_rules! push_output {
            ($row:expr, $weight:expr) => {{
                output_rows.push($row);
                if execution_mode.is_weighted() {
                    output_weights.push($weight);
                }
            }};
        }

        macro_rules! admit_match {
            ($left_batch:expr, $left_row:expr, $right_batch:expr, $right_row:expr,
             $left_initial:expr, $right_initial:expr) => {{
                let emits_pair = matches!(
                    config.join_type,
                    JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
                );
                let (left_weight, right_weight) = if execution_mode == JoinExecutionMode::AppendOnly
                {
                    (1, 1)
                } else {
                    (
                        state.left.row_weight($left_batch, $left_row)?,
                        state.right.row_weight($right_batch, $right_row)?,
                    )
                };
                let pair_weight = if emits_pair {
                    if execution_mode == JoinExecutionMode::AppendOnly {
                        Some(1)
                    } else {
                        Some(left_weight.checked_mul(right_weight).ok_or_else(|| {
                            DbError::PipelineTerminal(
                                "interval join pair-weight multiplication overflow".into(),
                            )
                        })?)
                    }
                } else {
                    None
                };
                let left_support = if tracks_left_matches(config.join_type) {
                    let previous =
                        state
                            .left
                            .match_weight($left_batch, $left_row, execution_mode)?;
                    let next = next_match_support(previous, right_weight, execution_mode)?;
                    let transition = if config.join_type == JoinType::LeftSemi && !$left_initial {
                        semi_transition_weight(left_weight, previous, next)?
                    } else {
                        None
                    };
                    Some((next, transition))
                } else {
                    None
                };
                let right_support = if tracks_right_matches(config.join_type) {
                    let previous =
                        state
                            .right
                            .match_weight($right_batch, $right_row, execution_mode)?;
                    let next = next_match_support(previous, left_weight, execution_mode)?;
                    let transition = if config.join_type == JoinType::RightSemi && !$right_initial {
                        semi_transition_weight(right_weight, previous, next)?
                    } else {
                        None
                    };
                    Some((next, transition))
                } else {
                    None
                };

                if let Some((next, _)) = left_support {
                    SideState::set_match_weight(
                        &mut state.left.match_flags,
                        &mut state.left.match_weights,
                        $left_batch,
                        $left_row,
                        next,
                        execution_mode,
                    )?;
                }
                if let Some((next, _)) = right_support {
                    SideState::set_match_weight(
                        &mut state.right.match_flags,
                        &mut state.right.match_weights,
                        $right_batch,
                        $right_row,
                        next,
                        execution_mode,
                    )?;
                }
                if let Some(weight) = pair_weight {
                    push_output!(
                        JoinOutputRow {
                            left: Some(($left_batch, $left_row)),
                            right: Some(($right_batch, $right_row)),
                        },
                        weight
                    );
                }
                if let Some(weight) = left_support.and_then(|(_, transition)| transition) {
                    push_output!(
                        JoinOutputRow {
                            left: Some(($left_batch, $left_row)),
                            right: None,
                        },
                        weight
                    );
                }
                if let Some(weight) = right_support.and_then(|(_, transition)| transition) {
                    push_output!(
                        JoinOutputRow {
                            left: None,
                            right: Some(($right_batch, $right_row)),
                        },
                        weight
                    );
                }
                flush_if_needed!();
            }};
        }

        macro_rules! probe_new_right {
            ($initial:expr) => {{
                if has_new_right {
                    let rb_kc = &right_key_cols[new_right_batch_idx];
                    let rb_ts = extract_column_as_timestamps(
                        &state.right.batches[new_right_batch_idx],
                        &config.right_time_column,
                    )?;
                    for (row_idx, &right_ts) in rb_ts.iter().enumerate() {
                        if let Some(key_hash) = tuple_hash_at(rb_kc, row_idx) {
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
                                        if execution_mode == JoinExecutionMode::AppendOnly
                                            && matches!(
                                                config.join_type,
                                                JoinType::LeftSemi | JoinType::LeftAnti
                                            )
                                            && state.left.match_weight(
                                                l_batch,
                                                l_row,
                                                execution_mode,
                                            )? != 0
                                        {
                                            continue;
                                        }
                                        if !tuples_equal(
                                            &left_key_cols[l_batch],
                                            l_row,
                                            rb_kc,
                                            row_idx,
                                        ) {
                                            continue;
                                        }
                                        admit_match!(
                                            l_batch,
                                            l_row,
                                            new_right_batch_idx,
                                            row_idx,
                                            false,
                                            $initial
                                        );
                                        if execution_mode == JoinExecutionMode::AppendOnly
                                            && matches!(
                                                config.join_type,
                                                JoinType::RightSemi | JoinType::RightAnti
                                            )
                                        {
                                            break 'right_row_matches;
                                        }
                                    }
                                }
                            }
                        }
                        if execution_mode == JoinExecutionMode::Weighted
                            && config.join_type == JoinType::RightSemi
                            && state.right.match_weight(
                                new_right_batch_idx,
                                row_idx,
                                execution_mode,
                            )? > 0
                        {
                            push_output!(
                                JoinOutputRow {
                                    left: None,
                                    right: Some((new_right_batch_idx, row_idx)),
                                },
                                state.right.row_weight(new_right_batch_idx, row_idx)?
                            );
                            flush_if_needed!();
                        }
                    }
                }
            }};
        }

        macro_rules! probe_new_left {
            ($initial:expr) => {{
                if has_new_left {
                    let lb_kc = &left_key_cols[new_left_batch_idx];
                    let lb_ts = extract_column_as_timestamps(
                        &state.left.batches[new_left_batch_idx],
                        &config.left_time_column,
                    )?;
                    for (row_idx, &left_ts) in lb_ts.iter().enumerate() {
                        if let Some(key_hash) = tuple_hash_at(lb_kc, row_idx) {
                            let low = left_ts;
                            let high = left_ts.saturating_add(bound_ms);
                            if let Some(times) = state.right.index.get(&key_hash) {
                                'left_row_matches: for entries in
                                    times.range(low..=high).map(|(_, entries)| entries)
                                {
                                    for &(r_batch, r_row) in entries {
                                        if execution_mode == JoinExecutionMode::AppendOnly
                                            && matches!(
                                                config.join_type,
                                                JoinType::RightSemi | JoinType::RightAnti
                                            )
                                            && state.right.match_weight(
                                                r_batch,
                                                r_row,
                                                execution_mode,
                                            )? != 0
                                        {
                                            continue;
                                        }
                                        if !tuples_equal(
                                            lb_kc,
                                            row_idx,
                                            &right_key_cols[r_batch],
                                            r_row,
                                        ) {
                                            continue;
                                        }
                                        admit_match!(
                                            new_left_batch_idx,
                                            row_idx,
                                            r_batch,
                                            r_row,
                                            $initial,
                                            false
                                        );
                                        if execution_mode == JoinExecutionMode::AppendOnly
                                            && matches!(
                                                config.join_type,
                                                JoinType::LeftSemi | JoinType::LeftAnti
                                            )
                                        {
                                            break 'left_row_matches;
                                        }
                                    }
                                }
                            }
                        }
                        if execution_mode == JoinExecutionMode::Weighted
                            && config.join_type == JoinType::LeftSemi
                            && state.left.match_weight(
                                new_left_batch_idx,
                                row_idx,
                                execution_mode,
                            )? > 0
                        {
                            push_output!(
                                JoinOutputRow {
                                    left: Some((new_left_batch_idx, row_idx)),
                                    right: None,
                                },
                                state.left.row_weight(new_left_batch_idx, row_idx)?
                            );
                            flush_if_needed!();
                        }
                    }
                }
            }};
        }

        if execution_mode == JoinExecutionMode::AppendOnly {
            // Preserve the production kernel's established output order and saturated support path.
            probe_new_left!(false);
            probe_new_right!(false);
        } else {
            // Canonical differential fold: dR probes L_old before dL sees R_old + dR.
            probe_new_right!(true);
            probe_new_left!(true);
        }

        flush_output_rows(
            &mut output_rows,
            &mut output_weights,
            output_schema,
            left_schema,
            right_schema,
            config.join_type,
            &state.left.batches,
            &state.right.batches,
            &state.left.row_bytes,
            &state.right.row_bytes,
            execution_mode,
            &mut result,
            output_budget,
        )?;
        Ok(())
    })();
    admitted.map_err(partial_apply)?;
    let closed = state
        .finalize_closed_rows(
            config,
            left_watermark,
            right_watermark,
            true,
            execution_mode,
            output_budget,
        )
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

    fn execute_weighted_cycle(
        state: &mut IntervalJoinState,
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
        config: &StreamJoinConfig,
        left_watermark: i64,
        right_watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let mut output_budget = IntervalJoinOutputBudget::default();
        execute_interval_join_cycle_with_mode(
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
            JoinExecutionMode::Weighted,
        )
    }

    fn weighted_batch(batch: RecordBatch, weights: &[i64]) -> RecordBatch {
        assert_eq!(batch.num_rows(), weights.len());
        let input_schema = batch.schema();
        let mut fields = input_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.push(Field::new(
            laminar_core::changelog::WEIGHT_COLUMN,
            DataType::Int64,
            false,
        ));
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(Int64Array::from(weights.to_vec())));
        RecordBatch::try_new(schema, columns).unwrap()
    }

    fn emitted_weights(output: &[RecordBatch]) -> Vec<i64> {
        output
            .iter()
            .flat_map(|batch| {
                let weight_index = batch.num_columns() - 1;
                assert_eq!(
                    batch.schema().field(weight_index).name(),
                    laminar_core::changelog::WEIGHT_COLUMN
                );
                assert!(!batch.schema().field(weight_index).is_nullable());
                assert_eq!(
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .filter(|field| { field.name() == laminar_core::changelog::WEIGHT_COLUMN })
                        .count(),
                    1
                );
                batch
                    .column(weight_index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect()
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
        assert!(!checkpoint.weighted);
        assert!(checkpoint.left_row_weights.is_empty());
        assert_eq!(checkpoint.left_match_flags, vec![vec![1]]);
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

        // Snapshot and serialize the retained cut.
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
                None,
                &["id".to_string()],
                "ts",
                false,
                false,
                JoinExecutionMode::AppendOnly,
            )
            .unwrap();
        let mut distinct_topology = SideState::new();
        distinct_topology
            .add_batch(
                &left_batch(&["A", "B"], &[100, 200], &[1.0, 2.0]),
                None,
                &["id".to_string()],
                "ts",
                false,
                false,
                JoinExecutionMode::AppendOnly,
            )
            .unwrap();

        assert!(
            distinct_topology.accounted_state_bytes() > shared_timestamp.accounted_state_bytes()
        );
    }

    #[test]
    fn checkpoint_capture_is_shallow_and_does_not_compact_live_state() {
        let config = make_config();
        let mut state = IntervalJoinState::new();
        execute_interval_join_cycle(
            &mut state,
            &[left_batch_nullable(
                &[Some("A"), None],
                &[100, 200],
                &[1.0, 2.0],
            )],
            &[],
            &config,
            0,
            0,
        )
        .unwrap();
        assert!(!state.left.is_compact());
        let first_column = state.left.batches[0].column(0).clone();

        let error = state
            .capture_checkpoint(&config, usize::MAX)
            .unwrap()
            .encode(1)
            .err()
            .expect("a one-byte checkpoint budget must reject Arrow IPC");
        assert!(error.to_string().contains("checkpoint limit"));
        assert_eq!(state.left.batches.len(), 1);
        assert!(!state.left.is_compact());
        assert!(Arc::ptr_eq(&first_column, state.left.batches[0].column(0)));

        let capture = state.capture_checkpoint(&config, usize::MAX).unwrap();
        assert!(Arc::ptr_eq(
            &first_column,
            capture.left_batches[0].column(0)
        ));
        let checkpoint = capture
            .encode(crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
            .unwrap();
        assert_eq!(checkpoint.left_buffer_rows, 1);
        assert!(checkpoint.left_row_weights.is_empty());
        assert!(checkpoint.left_match_flags.is_empty());
        assert!(Arc::ptr_eq(&first_column, state.left.batches[0].column(0)));
        assert!(!state.left.is_compact());

        let mut multi_batch = IntervalJoinState::new();
        execute_interval_join_cycle(
            &mut multi_batch,
            &[left_batch(&["A"], &[100], &[1.0])],
            &[],
            &config,
            0,
            0,
        )
        .unwrap();
        execute_interval_join_cycle(
            &mut multi_batch,
            &[left_batch(&["B"], &[101], &[2.0])],
            &[],
            &config,
            0,
            0,
        )
        .unwrap();
        assert_eq!(multi_batch.left.batches.len(), 2);
        let (fixed_capture_bytes, left_batch_clones, right_batch_clones) =
            multi_batch.capture_preflight_bytes(&config).unwrap();
        let expected_batch_clones = charged_allocation(
            2_usize
                .checked_mul(std::mem::size_of::<RecordBatch>())
                .unwrap(),
        )
        .checked_add(
            2_usize
                .checked_mul(charged_allocation(
                    3_usize
                        .checked_mul(std::mem::size_of::<ArrayRef>())
                        .unwrap(),
                ))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(left_batch_clones, expected_batch_clones);
        assert_eq!(right_batch_clones, 0);
        let exact_preflight = fixed_capture_bytes
            .checked_add(left_batch_clones)
            .and_then(|bytes| bytes.checked_add(right_batch_clones))
            .unwrap();
        let error = multi_batch
            .capture_checkpoint(&config, exact_preflight - 1)
            .err()
            .expect("one byte below the shallow-clone preflight must reject capture");
        assert!(error.to_string().contains("capture requires"));
    }

    #[test]
    fn post_cut_support_mutation_preserves_captured_support() {
        let mut config = make_config();
        config.join_type = JoinType::LeftSemi;
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
        let capture = state.capture_checkpoint(&config, usize::MAX).unwrap();

        execute_interval_join_cycle(
            &mut state,
            &[],
            &[right_batch(&["A"], &[110], &[2.0])],
            &config,
            0,
            0,
        )
        .unwrap();
        assert_eq!(state.left.match_flags[0][0], 1);
        assert!(!Arc::ptr_eq(
            &capture.left_match_flags[0],
            &state.left.match_flags[0]
        ));

        let checkpoint = capture
            .encode(crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
            .unwrap();
        assert_eq!(checkpoint.left_match_flags, vec![vec![0]]);
    }

    #[test]
    fn restore_cardinality_preflight_precedes_ipc_decode() {
        let checkpoint = JoinStateCheckpoint {
            weighted: false,
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
            left_row_weights: Vec::new(),
            right_row_weights: Vec::new(),
            left_match_flags: Vec::new(),
            right_match_flags: Vec::new(),
            left_match_weights: Vec::new(),
            right_match_weights: Vec::new(),
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
                JoinType::Inner,
                JoinExecutionMode::AppendOnly,
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
                    .worst_case_input_growth(
                        std::slice::from_ref(&incoming),
                        false,
                        JoinExecutionMode::AppendOnly,
                    )
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
                None,
                &["id".to_string()],
                "ts",
                false,
                false,
                JoinExecutionMode::AppendOnly,
            )
            .unwrap();

        state
            .preflight_input_growth(
                &[left_batch(&["hot"], &[100], &[1.0])],
                &[],
                JoinType::Inner,
                JoinExecutionMode::AppendOnly,
                state.accounted_state_bytes().saturating_add(64 * 1024),
            )
            .unwrap();
    }

    #[test]
    fn compaction_failure_leaves_original_state_intact() {
        let mut side = SideState::new();
        side.add_batch(
            &left_batch(&["A"], &[100], &[1.0]),
            None,
            &["id".to_string()],
            "ts",
            false,
            false,
            JoinExecutionMode::AppendOnly,
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
    fn append_normalization_borrows_plain_batches_and_owns_only_weight_strips() {
        let config = make_config();
        let plain_input = [left_batch(&["A"], &[100], &[1.0])];
        let plain = normalize_join_input(
            "left",
            &plain_input,
            &config.left_keys,
            &config.left_time_column,
            0,
            JoinExecutionMode::AppendOnly,
        )
        .unwrap();
        assert!(plain.row_weights.is_empty());
        assert!(matches!(&plain.batches, Cow::Borrowed(_)));

        let weighted_input = [weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[1])];
        let stripped = normalize_join_input(
            "left",
            &weighted_input,
            &config.left_keys,
            &config.left_time_column,
            0,
            JoinExecutionMode::AppendOnly,
        )
        .unwrap();
        assert!(stripped.row_weights.is_empty());
        assert!(matches!(&stripped.batches, Cow::Owned(_)));
        assert_eq!(stripped.batches[0].num_columns(), 3);
        assert_eq!(
            std::mem::size_of::<JoinOutputRow>(),
            std::mem::size_of::<(Option<(usize, usize)>, Option<(usize, usize)>,)>()
        );
    }

    #[test]
    fn side_admission_requires_mode_exact_weight_rosters() {
        let batch = left_batch(&["A"], &[100], &[1.0]);
        let keys = ["id".to_string()];

        let mut append = SideState::new();
        append
            .add_batch(
                &batch,
                None,
                &keys,
                "ts",
                false,
                false,
                JoinExecutionMode::AppendOnly,
            )
            .unwrap();
        assert!(append.row_weights.is_empty());

        let append_error = SideState::new()
            .add_batch(
                &batch,
                Some(Arc::<[i64]>::from([1_i64])),
                &keys,
                "ts",
                false,
                false,
                JoinExecutionMode::AppendOnly,
            )
            .unwrap_err();
        assert!(append_error.to_string().contains("row-weight roster"));

        let weighted_error = SideState::new()
            .add_batch(
                &batch,
                None,
                &keys,
                "ts",
                false,
                false,
                JoinExecutionMode::Weighted,
            )
            .unwrap_err();
        assert!(weighted_error.to_string().contains("row-weight roster"));
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
        assert!(error.to_string().contains("requires +1 weights"));
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

    #[test]
    fn weighted_deltas_cover_all_interval_join_kinds() {
        let cases = [
            (JoinType::Inner, vec![6, -6]),
            (JoinType::Left, vec![6, -6, 2]),
            (JoinType::Right, vec![6, -6]),
            (JoinType::Full, vec![6, -6, 2]),
            (JoinType::LeftSemi, vec![2, -2]),
            (JoinType::LeftAnti, vec![2]),
            (JoinType::RightSemi, vec![3, -3]),
            (JoinType::RightAnti, Vec::new()),
        ];

        for (join_type, expected) in cases {
            let mut config = make_config();
            config.join_type = join_type;
            let mut state = IntervalJoinState::new_weighted();
            let mut output = execute_weighted_cycle(
                &mut state,
                &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[2])],
                &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[3])],
                &config,
                0,
                0,
            )
            .unwrap();
            output.extend(
                execute_weighted_cycle(
                    &mut state,
                    &[],
                    &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[-3])],
                    &config,
                    0,
                    0,
                )
                .unwrap(),
            );
            output.extend(
                execute_weighted_cycle(&mut state, &[], &[], &config, 1_000, 1_000).unwrap(),
            );
            assert_eq!(emitted_weights(&output), expected, "{join_type:?}");
        }
    }

    #[test]
    fn execution_mode_is_bound_even_while_state_is_empty() {
        let config = make_config();
        let mut append_only = IntervalJoinState::new();
        let error = execute_weighted_cycle(&mut append_only, &[], &[], &config, 0, 0)
            .expect_err("append-only state must reject weighted execution");
        assert!(error.to_string().contains("execution mode changed"));
        assert_eq!(append_only.buffered_rows(), (0, 0));

        let mut weighted = IntervalJoinState::new_weighted();
        let error = execute_interval_join_cycle(&mut weighted, &[], &[], &config, 0, 0)
            .expect_err("weighted state must reject append-only execution");
        assert!(error.to_string().contains("execution mode changed"));
        assert_eq!(weighted.buffered_rows(), (0, 0));
    }

    #[test]
    fn weighted_semi_tracks_full_support_and_canonical_right_delta_order() {
        let mut left_semi = make_config();
        left_semi.join_type = JoinType::LeftSemi;
        let mut state = IntervalJoinState::new_weighted();
        let first = execute_weighted_cycle(
            &mut state,
            &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[5])],
            &[weighted_batch(
                right_batch(&["A", "A"], &[110, 110], &[10.0, 20.0]),
                &[2, 3],
            )],
            &left_semi,
            0,
            0,
        )
        .unwrap();
        assert_eq!(emitted_weights(&first), vec![5]);
        let still_matched = execute_weighted_cycle(
            &mut state,
            &[],
            &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[-2])],
            &left_semi,
            0,
            0,
        )
        .unwrap();
        assert!(still_matched.is_empty());
        let becomes_unmatched = execute_weighted_cycle(
            &mut state,
            &[],
            &[weighted_batch(right_batch(&["A"], &[110], &[20.0]), &[-3])],
            &left_semi,
            0,
            0,
        )
        .unwrap();
        assert_eq!(emitted_weights(&becomes_unmatched), vec![-5]);

        let mut right_semi = make_config();
        right_semi.join_type = JoinType::RightSemi;
        let mut ordered = IntervalJoinState::new_weighted();
        execute_weighted_cycle(
            &mut ordered,
            &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[1])],
            &[],
            &right_semi,
            0,
            0,
        )
        .unwrap();
        let output = execute_weighted_cycle(
            &mut ordered,
            &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[-1])],
            &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[1])],
            &right_semi,
            0,
            0,
        )
        .unwrap();
        assert_eq!(emitted_weights(&output), vec![1, -1]);
    }

    #[test]
    fn weighted_cross_term_is_emitted_once() {
        let config = make_config();
        let mut state = IntervalJoinState::new_weighted();
        execute_weighted_cycle(
            &mut state,
            &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[2])],
            &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[3])],
            &config,
            0,
            0,
        )
        .unwrap();
        let output = execute_weighted_cycle(
            &mut state,
            &[weighted_batch(left_batch(&["A"], &[100], &[2.0]), &[5])],
            &[weighted_batch(right_batch(&["A"], &[110], &[20.0]), &[7])],
            &config,
            0,
            0,
        )
        .unwrap();
        assert_eq!(emitted_weights(&output), vec![14, 15, 35]);
    }

    #[test]
    fn weighted_checkpoint_is_shallow_and_restores_exact_support() {
        let mut config = make_config();
        config.join_type = JoinType::LeftSemi;
        let mut state = IntervalJoinState::new_weighted();
        execute_weighted_cycle(
            &mut state,
            &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[4])],
            &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[2])],
            &config,
            0,
            0,
        )
        .unwrap();
        let row_weights = Arc::clone(&state.left.row_weights[0]);
        let match_weights = Arc::clone(&state.left.match_weights[0]);
        let capture = state.capture_checkpoint(&config, usize::MAX).unwrap();
        assert!(Arc::ptr_eq(&row_weights, &capture.left_row_weights[0]));
        assert!(Arc::ptr_eq(&match_weights, &capture.left_match_weights[0]));

        let checkpoint = capture
            .encode(crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
            .unwrap();
        assert!(checkpoint.weighted);
        let mode_error = IntervalJoinState::from_checkpoint(
            &checkpoint,
            &config,
            crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        )
        .err()
        .expect("weighted checkpoint must not restore into append-only state");
        assert!(mode_error.to_string().contains("execution mode"));
        let mut restored = IntervalJoinState::from_checkpoint_with_mode(
            &checkpoint,
            &config,
            crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
            JoinExecutionMode::Weighted,
        )
        .unwrap();
        let output = execute_weighted_cycle(
            &mut restored,
            &[],
            &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[-2])],
            &config,
            0,
            0,
        )
        .unwrap();
        assert_eq!(emitted_weights(&output), vec![-4]);

        execute_weighted_cycle(
            &mut state,
            &[],
            &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[-2])],
            &config,
            0,
            0,
        )
        .unwrap();
        assert!(!Arc::ptr_eq(&match_weights, &state.left.match_weights[0]));
        assert!(Arc::ptr_eq(&row_weights, &state.left.row_weights[0]));
    }

    #[test]
    fn weighted_checkpoint_compaction_keeps_payload_and_weight_rosters_aligned() {
        let mut config = make_config();
        config.join_type = JoinType::LeftSemi;
        let mut state = IntervalJoinState::new_weighted();
        execute_weighted_cycle(
            &mut state,
            &[weighted_batch(
                left_batch_nullable(&[Some("A"), None], &[100, 200], &[1.0, 2.0]),
                &[7, -2],
            )],
            &[],
            &config,
            0,
            0,
        )
        .unwrap();
        assert!(!state.left.is_compact());

        let checkpoint = state
            .capture_checkpoint(&config, usize::MAX)
            .unwrap()
            .encode(crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
            .unwrap();
        assert_eq!(checkpoint.left_row_weights, vec![vec![7]]);
        assert_eq!(checkpoint.left_match_weights, vec![vec![0]]);
        let decoded =
            laminar_core::serialization::deserialize_batch_stream(&checkpoint.left_batches[0])
                .unwrap();
        assert_eq!(decoded.num_rows(), 1);
        assert_eq!(
            decoded
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "A"
        );
    }

    #[test]
    fn weighted_checkpoint_keeps_support_after_opposite_eviction() {
        let mut config = make_config();
        config.join_type = JoinType::Left;
        let mut state = IntervalJoinState::new_weighted();
        execute_weighted_cycle(
            &mut state,
            &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[1])],
            &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[1])],
            &config,
            0,
            0,
        )
        .unwrap();
        execute_weighted_cycle(&mut state, &[], &[], &config, 200, 150).unwrap();
        assert_eq!(state.buffered_rows(), (1, 0));

        let checkpoint = state
            .capture_checkpoint(&config, usize::MAX)
            .unwrap()
            .encode(crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
            .unwrap();
        let mut restored = IntervalJoinState::from_checkpoint_with_mode(
            &checkpoint,
            &config,
            crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
            JoinExecutionMode::Weighted,
        )
        .unwrap();
        let output = execute_weighted_cycle(&mut restored, &[], &[], &config, 200, 300).unwrap();
        assert!(
            output.is_empty(),
            "matched left row became falsely unmatched"
        );
    }

    #[test]
    fn weighted_arithmetic_and_late_deltas_fail_closed() {
        let config = make_config();
        let mut overflow = IntervalJoinState::new_weighted();
        let error = execute_weighted_cycle(
            &mut overflow,
            &[weighted_batch(
                left_batch(&["A"], &[100], &[1.0]),
                &[i64::MAX],
            )],
            &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[2])],
            &config,
            0,
            0,
        )
        .unwrap_err();
        assert!(error.requires_pipeline_halt());
        assert!(error.to_string().contains("multiplication overflow"));

        let mut left_semi = make_config();
        left_semi.join_type = JoinType::LeftSemi;
        let mut underflow = IntervalJoinState::new_weighted();
        execute_weighted_cycle(
            &mut underflow,
            &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[1])],
            &[],
            &left_semi,
            0,
            0,
        )
        .unwrap();
        let error = execute_weighted_cycle(
            &mut underflow,
            &[],
            &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[-1])],
            &left_semi,
            0,
            0,
        )
        .unwrap_err();
        assert!(error.requires_pipeline_halt());
        assert!(error.to_string().contains("became negative"));
        assert_eq!(underflow.left.match_weights[0][0], 0);

        let mut support_overflow = IntervalJoinState::new_weighted();
        execute_weighted_cycle(
            &mut support_overflow,
            &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[1])],
            &[weighted_batch(
                right_batch(&["A"], &[110], &[10.0]),
                &[i64::MAX],
            )],
            &left_semi,
            0,
            0,
        )
        .unwrap();
        let error = execute_weighted_cycle(
            &mut support_overflow,
            &[],
            &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[1])],
            &left_semi,
            0,
            0,
        )
        .unwrap_err();
        assert!(error.requires_pipeline_halt());
        assert!(error.to_string().contains("match-support overflow"));
        assert_eq!(support_overflow.left.match_weights[0][0], i64::MAX);

        let mut negate_overflow = IntervalJoinState::new_weighted();
        execute_weighted_cycle(
            &mut negate_overflow,
            &[weighted_batch(
                left_batch(&["A"], &[100], &[1.0]),
                &[i64::MIN],
            )],
            &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[1])],
            &left_semi,
            0,
            0,
        )
        .unwrap();
        let error = execute_weighted_cycle(
            &mut negate_overflow,
            &[],
            &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[-1])],
            &left_semi,
            0,
            0,
        )
        .unwrap_err();
        assert!(error.requires_pipeline_halt());
        assert!(error.to_string().contains("semi-join retraction overflow"));
        assert_eq!(negate_overflow.left.match_weights[0][0], 1);

        let mut late = IntervalJoinState::new_weighted();
        let error = execute_interval_join_cycle_with_mode(
            &mut late,
            &[weighted_batch(left_batch(&["A"], &[99], &[1.0]), &[-1])],
            &[],
            &config,
            100,
            0,
            100,
            0,
            usize::MAX,
            &mut IntervalJoinOutputBudget::default(),
            JoinExecutionMode::Weighted,
        )
        .unwrap_err();
        assert!(error.to_string().contains("late row"));
        assert_eq!(late.buffered_rows(), (0, 0));
    }

    #[test]
    fn weighted_output_budget_accounts_for_the_trailing_weight_before_build() {
        let config = make_config();
        let initial_bytes = MAX_CYCLE_OUTPUT_BYTES - 225;
        let mut append_only = IntervalJoinState::new();
        let mut append_budget = IntervalJoinOutputBudget {
            emitted_rows: 0,
            emitted_bytes: initial_bytes,
        };
        let append_result = super::execute_interval_join_cycle(
            &mut append_only,
            &[left_batch(&["A"], &[100], &[1.0])],
            &[right_batch(&["A"], &[110], &[10.0])],
            &config,
            0,
            0,
            0,
            0,
            usize::MAX,
            &mut append_budget,
        );
        match append_result {
            Ok(output) => assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1),
            Err(error) => {
                assert!(error.to_string().contains("exceeded"));
                assert!(!error.to_string().contains("would exceed"));
            }
        }

        let mut state = IntervalJoinState::new_weighted();
        let mut output_budget = IntervalJoinOutputBudget {
            emitted_rows: 0,
            emitted_bytes: initial_bytes,
        };
        let error = execute_interval_join_cycle_with_mode(
            &mut state,
            &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[1])],
            &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[1])],
            &config,
            0,
            0,
            0,
            0,
            usize::MAX,
            &mut output_budget,
            JoinExecutionMode::Weighted,
        )
        .unwrap_err();
        assert!(matches!(error, DbError::BackpressureFail(_)));
        assert!(error.to_string().contains("would exceed"));
        assert_eq!(output_budget.emitted_bytes, initial_bytes);
    }

    #[test]
    fn weighted_restore_rejects_corrupt_support_before_ipc_decode() {
        let mut config = make_config();
        config.join_type = JoinType::LeftSemi;
        let checkpoint = JoinStateCheckpoint {
            weighted: true,
            join_type: join_type_tag(config.join_type),
            left_keys: config.left_keys.clone(),
            right_keys: config.right_keys.clone(),
            left_time_column: config.left_time_column.clone(),
            right_time_column: config.right_time_column.clone(),
            left_table: config.left_table.clone(),
            right_table: config.right_table.clone(),
            bound_ms: 100,
            left_buffer_rows: 1,
            right_buffer_rows: 0,
            left_batches: vec![vec![0xff]],
            right_batches: Vec::new(),
            left_evicted_cutoff: i64::MIN,
            right_evicted_cutoff: i64::MIN,
            left_row_weights: vec![vec![1]],
            right_row_weights: Vec::new(),
            left_match_flags: Vec::new(),
            right_match_flags: Vec::new(),
            left_match_weights: vec![vec![-1]],
            right_match_weights: Vec::new(),
        };
        let error = IntervalJoinState::from_checkpoint_with_mode(
            &checkpoint,
            &config,
            crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
            JoinExecutionMode::Weighted,
        )
        .err()
        .expect("corrupt weighted support must fail before IPC decode");
        assert!(error.to_string().contains("invalid match weights"));
        assert!(!error.to_string().contains("deserialization"));
    }
}
