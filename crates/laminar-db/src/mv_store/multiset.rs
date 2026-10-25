//! Counted MV rows with live quotas and cached snapshot expansion admission, split into vnode
//! slices placed by an inherited hash key or by the full row.

use super::admission::{size_overflow, MvLimits};
use super::slices::VnodeSlices;
use super::staging::StagingBudget;
use super::weight_and_plain_cols;
use crate::error::DbError;
use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::SchemaRef;
use arrow::row::{OwnedRow, Row, RowConverter, Rows, SortField};
use laminar_core::state::KeyGroupCount;
use std::collections::HashMap;
use std::sync::Arc;

use super::{
    DEFAULT_MAX_BYTES, MAX_MULTISET_MATERIALIZED_ROWS, MULTISET_MATERIALIZATION_ROW_OVERHEAD,
};

pub(super) type MultisetSlice = HashMap<OwnedRow, i64>;

/// Z-set multiset from a `__weight` changelog: full output row keyed to an integer multiplicity.
pub(super) struct MultisetState {
    pub(super) row_converter: Arc<RowConverter>,
    /// The inherited hash key's columns and encoder; `None` places each row by its full encoding.
    placement: Option<(Vec<usize>, RowConverter)>,
    pub(super) slices: VnodeSlices<MultisetSlice>,
    rows: usize,
    pub(super) approx_bytes: usize,
    expansion: Expansion,
}

#[derive(Clone, Copy, Default)]
struct Expansion {
    rows: u128,
    bytes: u128,
}

impl Expansion {
    fn contribution(key: &OwnedRow, count: i64) -> Result<Self, DbError> {
        let rows = u128::try_from(count).map_err(|_| size_overflow())?;
        let bytes = (key.as_ref().len() as u128 + MULTISET_MATERIALIZATION_ROW_OVERHEAD as u128)
            .checked_mul(rows)
            .ok_or_else(size_overflow)?;
        Ok(Self { rows, bytes })
    }

    fn replace(&mut self, key: &OwnedRow, old: i64, new: i64) -> Result<(), DbError> {
        let old = Self::contribution(key, old)?;
        let new = Self::contribution(key, new)?;
        self.rows = self
            .rows
            .checked_sub(old.rows)
            .and_then(|n| n.checked_add(new.rows))
            .ok_or_else(size_overflow)?;
        self.bytes = self
            .bytes
            .checked_sub(old.bytes)
            .and_then(|n| n.checked_add(new.bytes))
            .ok_or_else(size_overflow)?;
        Ok(())
    }

    fn validate(self) -> Result<usize, DbError> {
        if self.rows > MAX_MULTISET_MATERIALIZED_ROWS as u128 {
            return Err(DbError::Storage(format!("multiset MV materialization exceeds the safe row limit of {MAX_MULTISET_MATERIALIZED_ROWS}")));
        }
        if self.bytes > DEFAULT_MAX_BYTES as u128 {
            return Err(DbError::Storage(format!(
                "multiset MV materialization exceeds the safe byte limit of {DEFAULT_MAX_BYTES}"
            )));
        }
        usize::try_from(self.rows).map_err(|_| size_overflow())
    }
}

pub(super) struct MultisetDelta {
    counts: Vec<(OwnedRow, i64, u32)>,
    rows: usize,
    bytes: usize,
    expansion: Expansion,
}

struct StagedCount {
    delta: i128,
    vnode: u32,
}

const STAGED_ROW_OVERHEAD: usize =
    std::mem::size_of::<(OwnedRow, StagedCount)>() - std::mem::size_of::<(OwnedRow, i64)>();

/// The rows, bytes and snapshot expansion one slice holds.
#[cfg(feature = "cluster")]
fn slice_charge(slice: &MultisetSlice) -> (usize, usize, Expansion) {
    slice.iter().fold(
        (0, 0, Expansion::default()),
        |(rows, bytes, expansion), (key, &count)| {
            let added = Expansion::contribution(key, count).unwrap_or(Expansion {
                rows: u128::MAX,
                bytes: u128::MAX,
            });
            (
                rows + 1,
                bytes.saturating_add(row_size(key).unwrap_or(usize::MAX)),
                Expansion {
                    rows: expansion.rows.saturating_add(added.rows),
                    bytes: expansion.bytes.saturating_add(added.bytes),
                },
            )
        },
    )
}

fn row_size(key: &OwnedRow) -> Result<usize, DbError> {
    std::mem::size_of::<(OwnedRow, i64)>()
        .checked_add(key.as_ref().len())
        .ok_or_else(size_overflow)
}

pub(super) fn decode_rows<'a>(
    converter: &RowConverter,
    schema: &SchemaRef,
    rows: impl IntoIterator<Item = Row<'a>>,
) -> Result<Vec<ArrayRef>, DbError> {
    let mut arrays = converter
        .convert_rows(rows)
        .map_err(|e| DbError::Storage(format!("multiset MV row conversion: {e}")))?;
    for (array, field) in arrays.iter_mut().zip(schema.fields()) {
        // Arrow hydrates dictionaries, including nested ones, when decoding rows.
        // Restore the declared type without turning an unrepresentable value into null.
        if array.data_type() != field.data_type() {
            *array = cast_with_options(
                array,
                field.data_type(),
                &CastOptions {
                    safe: false,
                    ..CastOptions::default()
                },
            )
            .map_err(|e| DbError::Storage(format!("multiset MV column '{}': {e}", field.name())))?;
        }
    }
    Ok(arrays)
}

fn converter(
    schema: &SchemaRef,
    columns: impl Iterator<Item = usize>,
) -> Result<RowConverter, DbError> {
    let sort_fields: Vec<SortField> = columns
        .map(|c| SortField::new(schema.field(c).data_type().clone()))
        .collect();
    RowConverter::new(sort_fields)
        .map_err(|e| DbError::Storage(format!("multiset MV row converter: {e}")))
}

impl MultisetState {
    /// `placement` names the inherited hash-key columns; empty places rows by the full row.
    pub(super) fn new(
        schema: &SchemaRef,
        placement: &[usize],
        key_groups: KeyGroupCount,
    ) -> Result<Self, DbError> {
        if placement.iter().any(|&c| c >= schema.fields().len()) {
            return Err(DbError::Storage(
                "multiset MV placement column is outside its schema".into(),
            ));
        }
        let placement = if placement.is_empty() {
            None
        } else {
            Some((
                placement.to_vec(),
                converter(schema, placement.iter().copied())?,
            ))
        };
        Ok(Self {
            row_converter: Arc::new(converter(schema, 0..schema.fields().len())?),
            placement,
            slices: VnodeSlices::new(key_groups),
            rows: 0,
            approx_bytes: 0,
            expansion: Expansion::default(),
        })
    }

    #[cfg(feature = "cluster")]
    pub(super) const fn row_count(&self) -> usize {
        self.rows
    }

    /// The rows and bytes the slices of `vnodes` hold.
    #[cfg(feature = "cluster")]
    pub(super) fn slices_charge(&self, vnodes: &[u32]) -> (usize, usize) {
        vnodes.iter().fold((0, 0), |(rows, bytes), &vnode| {
            let (slice_rows, slice_bytes, _) = slice_charge(self.slices.slot(vnode));
            (rows + slice_rows, bytes.saturating_add(slice_bytes))
        })
    }

    /// Empty the slices of `revoked` and move in those of `acquired` from `staged`, with their
    /// charge. Preparation checked the result against the view limits, so this saturates
    /// rather than fails. The next capture writes every owned vnode.
    #[cfg(feature = "cluster")]
    pub(super) fn exchange_slices(&mut self, revoked: &[u32], staged: &mut Self, acquired: &[u32]) {
        for &vnode in revoked.iter().chain(acquired) {
            let (rows, bytes, expansion) =
                slice_charge(&std::mem::take(self.slices.slot_mut(vnode)));
            self.rows = self.rows.saturating_sub(rows);
            self.approx_bytes = self.approx_bytes.saturating_sub(bytes);
            self.expansion.rows = self.expansion.rows.saturating_sub(expansion.rows);
            self.expansion.bytes = self.expansion.bytes.saturating_sub(expansion.bytes);
        }
        for &vnode in acquired {
            let slice = std::mem::take(staged.slices.slot_mut(vnode));
            let (rows, bytes, expansion) = slice_charge(&slice);
            self.rows = self.rows.saturating_add(rows);
            self.approx_bytes = self.approx_bytes.saturating_add(bytes);
            self.expansion.rows = self.expansion.rows.saturating_add(expansion.rows);
            self.expansion.bytes = self.expansion.bytes.saturating_add(expansion.bytes);
            *self.slices.slot_mut(vnode) = slice;
        }
        self.slices.require_full_capture();
    }

    /// Encoded placement keys when rows are placed by an inherited hash key. `columns[i]` is
    /// the batch column holding view column `i`.
    fn placement_keys(
        &self,
        batch: &RecordBatch,
        columns: &[usize],
    ) -> Result<Option<Rows>, DbError> {
        let Some((placement, converter)) = &self.placement else {
            return Ok(None);
        };
        let arrays: Vec<ArrayRef> = placement
            .iter()
            .map(|&c| Arc::clone(batch.column(columns[c])))
            .collect();
        converter
            .convert_columns(&arrays)
            .map(Some)
            .map_err(|e| DbError::Storage(format!("multiset MV placement conversion: {e}")))
    }

    fn vnode_of(&self, placement: Option<&Rows>, row: &OwnedRow, row_idx: usize) -> u32 {
        match placement {
            Some(keys) => self.slices.vnode_for(keys.row(row_idx).as_ref()),
            None => self.slices.vnode_for(row.as_ref()),
        }
    }

    fn stage_batch(
        &self,
        name: &str,
        batch: &RecordBatch,
        deltas: &mut HashMap<OwnedRow, StagedCount>,
        budget: &mut StagingBudget,
    ) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let (weights, plain_indices) = weight_and_plain_cols(batch)?;
        budget.validate_input(name, batch)?;
        let plain_cols: Vec<ArrayRef> = plain_indices
            .iter()
            .map(|&c| Arc::clone(batch.column(c)))
            .collect();
        let rows = self
            .row_converter
            .convert_columns(&plain_cols)
            .map_err(|e| DbError::Storage(format!("multiset MV row conversion: {e}")))?;
        let placement = self.placement_keys(batch, &plain_indices)?;

        for row_idx in 0..batch.num_rows() {
            if weights.is_null(row_idx) {
                return Err(DbError::Storage(format!(
                    "multiset MV weight is null at row {row_idx}"
                )));
            }
            let w = weights.value(row_idx);
            if w == 0 {
                continue;
            }
            let key = rows.row(row_idx).owned();
            let bytes = row_size(&key)?
                .checked_add(STAGED_ROW_OVERHEAD)
                .ok_or_else(size_overflow)?;
            match deltas.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let delta = entry
                        .get()
                        .delta
                        .checked_add(i128::from(w))
                        .ok_or_else(|| {
                            DbError::Storage("multiset MV staged multiplicity overflow".into())
                        })?;
                    if delta == 0 {
                        budget.replace(name, Some(bytes), None)?;
                        entry.remove();
                    } else {
                        entry.get_mut().delta = delta;
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    budget.replace(name, None, Some(bytes))?;
                    let vnode = self.vnode_of(placement.as_ref(), entry.key(), row_idx);
                    entry.insert(StagedCount {
                        delta: i128::from(w),
                        vnode,
                    });
                }
            }
        }
        Ok(())
    }

    /// Apply a cycle's Z-set deltas only after every touched row has a valid final multiplicity.
    pub(super) fn prepare_cycle(
        &self,
        name: &str,
        batches: &[RecordBatch],
        limits: MvLimits,
        snapshot: bool,
    ) -> Result<MultisetDelta, DbError> {
        let mut deltas = HashMap::new();
        let mut budget = StagingBudget::new(limits, STAGED_ROW_OVERHEAD);
        for batch in batches {
            self.stage_batch(name, batch, &mut deltas, &mut budget)?;
        }

        let mut resolved = Vec::with_capacity(deltas.len());
        let (mut rows, mut removed, mut added) = (self.rows, 0usize, 0usize);
        let mut expansion = self.expansion;
        for (key, StagedCount { delta, vnode }) in deltas {
            let old = self.slices.slot(vnode).get(&key).copied().unwrap_or(0);
            let current = i128::from(old);
            let next = current
                .checked_add(delta)
                .ok_or_else(|| DbError::Storage("multiset MV multiplicity overflow".into()))?;
            if next < 0 {
                return Err(DbError::Storage(
                    "multiset MV retraction produced a negative multiplicity".into(),
                ));
            }
            let next = i64::try_from(next)
                .map_err(|_| DbError::Storage("multiset MV multiplicity overflow".into()))?;
            if old > 0 {
                removed = removed
                    .checked_add(row_size(&key)?)
                    .ok_or_else(size_overflow)?;
                rows -= 1;
            }
            if next > 0 {
                added = added
                    .checked_add(row_size(&key)?)
                    .ok_or_else(size_overflow)?;
                rows = rows.checked_add(1).ok_or_else(size_overflow)?;
            }
            expansion.replace(&key, old, next)?;
            resolved.push((key, next, vnode));
        }

        let bytes = self
            .approx_bytes
            .checked_sub(removed)
            .and_then(|bytes| bytes.checked_add(added))
            .ok_or_else(size_overflow)?;
        limits.validate(name, rows, bytes)?;
        if snapshot {
            expansion.validate()?;
        }
        Ok(MultisetDelta {
            counts: resolved,
            rows,
            bytes,
            expansion,
        })
    }

    pub(super) fn apply_prepared(&mut self, delta: MultisetDelta) {
        for (key, count, vnode) in delta.counts {
            let slice = self.slices.slot_mut(vnode);
            if count == 0 {
                slice.remove(&key);
            } else {
                slice.insert(key, count);
            }
        }
        self.rows = delta.rows;
        self.approx_bytes = delta.bytes;
        self.expansion = delta.expansion;
    }

    /// Restore one vnode's counted frame. A frame holds exactly one row per distinct value, each
    /// placed on `vnode`; anything else is corruption rather than an alternate encoding.
    pub(super) fn restore_slice(
        &mut self,
        name: &str,
        vnode: u32,
        batch: &RecordBatch,
        limits: MvLimits,
    ) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let count_idx = batch.num_columns().checked_sub(1).ok_or_else(|| {
            DbError::Storage("multiset MV checkpoint is missing its count column".into())
        })?;
        let counts = batch
            .column(count_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DbError::Storage("multiset MV checkpoint count column is not Int64".into())
            })?;
        let rows = self
            .row_converter
            .convert_columns(&batch.columns()[..count_idx])
            .map_err(|e| DbError::Storage(format!("multiset MV restore conversion: {e}")))?;
        let columns: Vec<usize> = (0..count_idx).collect();
        let placement = self.placement_keys(batch, &columns)?;
        for row_idx in 0..batch.num_rows() {
            if counts.is_null(row_idx) {
                return Err(DbError::Storage(format!(
                    "multiset MV checkpoint count is null at row {row_idx}"
                )));
            }
            let count = counts.value(row_idx);
            if count <= 0 {
                return Err(DbError::Storage(format!(
                    "multiset MV checkpoint count must be positive at row {row_idx}"
                )));
            }
            let key = rows.row(row_idx).owned();
            let placed = self.vnode_of(placement.as_ref(), &key, row_idx);
            if placed != vnode {
                return Err(DbError::Storage(format!(
                    "multiset MV frame for vnode {vnode} holds a row placed on vnode {placed}"
                )));
            }
            if self.slices.slot(vnode).contains_key(&key) {
                return Err(DbError::Storage(format!(
                    "multiset MV checkpoint contains a duplicate value at row {row_idx}"
                )));
            }
            let bytes = self
                .approx_bytes
                .checked_add(row_size(&key)?)
                .ok_or_else(size_overflow)?;
            let next_rows = self.rows.checked_add(1).ok_or_else(size_overflow)?;
            limits.validate(name, next_rows, bytes)?;
            self.expansion.replace(&key, 0, count)?;
            self.slices.slot_mut(vnode).insert(key, count);
            self.rows = next_rows;
            self.approx_bytes = bytes;
        }
        Ok(())
    }

    pub(super) fn to_record_batch(&self, schema: &SchemaRef) -> Result<RecordBatch, DbError> {
        if self.rows == 0 {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        self.expansion.validate()?;
        let rows = self
            .slices
            .slots()
            .flat_map(|(_, slice)| slice.iter())
            .flat_map(|(key, &count)| {
                std::iter::repeat_n(key.row(), usize::try_from(count).unwrap_or(0))
            });
        let arrays = decode_rows(&self.row_converter, schema, rows)?;
        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| DbError::Storage(format!("multiset MV batch assembly: {e}")))
    }
}
