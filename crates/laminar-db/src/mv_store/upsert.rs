//! Keyed MV snapshots with quota-checked replacement and deletion deltas, split into vnode
//! slices placed by the view key.

use super::admission::{size_overflow, MvLimits};
use super::slices::VnodeSlices;
use super::staging::StagingBudget;
use super::weight_and_plain_cols;
use crate::aggregate_state::global_aggregate_key;
use crate::error::DbError;
use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow::row::{OwnedRow, RowConverter, Rows, SortField};
use laminar_core::state::KeyGroupCount;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::ScalarValue;

pub(super) type UpsertSlice = HashMap<OwnedRow, Vec<ScalarValue>>;

/// Keyed running snapshot from a `__weight` changelog; stored rows omit the weight column.
pub(super) struct UpsertState {
    key_cols: Vec<usize>,
    /// `None` for a global aggregate, whose single row has a constant key on vnode 0.
    key_converter: Option<RowConverter>,
    pub(super) slices: VnodeSlices<UpsertSlice>,
    pub(super) rows: usize,
    pub(super) approx_bytes: usize,
}

pub(super) struct UpsertDelta {
    rows: HashMap<OwnedRow, StagedRow>,
    row_count: usize,
    bytes: usize,
}

struct StagedRow {
    values: Option<Vec<ScalarValue>>,
    bytes: usize,
    vnode: u32,
}

const STAGED_ROW_OVERHEAD: usize =
    std::mem::size_of::<StagedRow>() - std::mem::size_of::<Vec<ScalarValue>>();

impl UpsertState {
    pub(super) fn new(
        schema: &SchemaRef,
        key_cols: &[usize],
        key_groups: KeyGroupCount,
    ) -> Result<Self, DbError> {
        if key_cols
            .iter()
            .any(|&column| column >= schema.fields().len())
        {
            return Err(DbError::Storage(
                "upsert MV key column is outside its schema".into(),
            ));
        }
        let key_converter = if key_cols.is_empty() {
            None
        } else {
            let sort_fields: Vec<SortField> = key_cols
                .iter()
                .map(|&c| SortField::new(schema.field(c).data_type().clone()))
                .collect();
            Some(
                RowConverter::new(sort_fields)
                    .map_err(|e| DbError::Storage(format!("upsert MV key converter: {e}")))?,
            )
        };
        Ok(Self {
            key_cols: key_cols.to_vec(),
            key_converter,
            slices: VnodeSlices::new(key_groups),
            rows: 0,
            approx_bytes: 0,
        })
    }

    fn keys(&self, batch: &RecordBatch, plain_cols: &[usize]) -> Result<Option<Rows>, DbError> {
        let Some(converter) = &self.key_converter else {
            return Ok(None);
        };
        let key_arrays: Vec<ArrayRef> = self
            .key_cols
            .iter()
            .map(|&c| Arc::clone(batch.column(plain_cols[c])))
            .collect();
        converter
            .convert_columns(&key_arrays)
            .map(Some)
            .map_err(|e| DbError::Storage(format!("upsert MV key conversion: {e}")))
    }

    fn key_at(keys: Option<&Rows>, row_idx: usize) -> OwnedRow {
        keys.map_or_else(global_aggregate_key, |keys| keys.row(row_idx).owned())
    }

    /// The rows and bytes the slices of `vnodes` hold.
    #[cfg(feature = "cluster")]
    pub(super) fn slices_charge(&self, vnodes: &[u32]) -> (usize, usize) {
        vnodes.iter().fold((0, 0), |(rows, bytes), &vnode| {
            let (slice_rows, slice_bytes) = Self::slice_charge(self.slices.slot(vnode));
            (rows + slice_rows, bytes.saturating_add(slice_bytes))
        })
    }

    #[cfg(feature = "cluster")]
    fn slice_charge(slice: &UpsertSlice) -> (usize, usize) {
        let bytes = slice
            .iter()
            .map(|(key, values)| Self::row_size(key, values).unwrap_or(usize::MAX))
            .fold(0, usize::saturating_add);
        (slice.len(), bytes)
    }

    /// Empty the slices of `revoked` and move in those of `acquired` from `staged`, with their
    /// charge. Preparation checked the result against the view limits, so this saturates
    /// rather than fails. The next capture writes every owned vnode.
    #[cfg(feature = "cluster")]
    pub(super) fn exchange_slices(&mut self, revoked: &[u32], staged: &mut Self, acquired: &[u32]) {
        for &vnode in revoked.iter().chain(acquired) {
            let (rows, bytes) = Self::slice_charge(&std::mem::take(self.slices.slot_mut(vnode)));
            self.rows = self.rows.saturating_sub(rows);
            self.approx_bytes = self.approx_bytes.saturating_sub(bytes);
        }
        for &vnode in acquired {
            let slice = std::mem::take(staged.slices.slot_mut(vnode));
            let (rows, bytes) = Self::slice_charge(&slice);
            self.rows = self.rows.saturating_add(rows);
            self.approx_bytes = self.approx_bytes.saturating_add(bytes);
            *self.slices.slot_mut(vnode) = slice;
        }
        self.slices.require_full_capture();
    }

    fn vnode_of(&self, key: &OwnedRow) -> u32 {
        if self.key_converter.is_some() {
            self.slices.vnode_for(key.as_ref())
        } else {
            0
        }
    }

    pub(super) fn row_size(key: &OwnedRow, values: &Vec<ScalarValue>) -> Result<usize, DbError> {
        let spare = (values.capacity() - values.len())
            .checked_mul(std::mem::size_of::<ScalarValue>())
            .ok_or_else(size_overflow)?;
        let base = std::mem::size_of::<(OwnedRow, Vec<ScalarValue>)>()
            .checked_add(key.as_ref().len())
            .and_then(|bytes| bytes.checked_add(spare))
            .ok_or_else(size_overflow)?;
        values.iter().try_fold(base, |bytes, value| {
            bytes.checked_add(value.size()).ok_or_else(size_overflow)
        })
    }

    fn row_values(
        batch: &RecordBatch,
        columns: impl Iterator<Item = usize>,
        row_idx: usize,
    ) -> Result<Vec<ScalarValue>, DbError> {
        columns
            .map(|c| {
                ScalarValue::try_from_array(batch.column(c), row_idx)
                    .map_err(|e| DbError::Storage(format!("upsert MV scalar: {e}")))
            })
            .collect()
    }

    /// Stage `+weight` upserts and `-weight` deletes without touching live state.
    fn stage_batch(
        &self,
        name: &str,
        batch: &RecordBatch,
        staged: &mut HashMap<OwnedRow, StagedRow>,
        budget: &mut StagingBudget,
    ) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let (weights, plain_cols) = weight_and_plain_cols(batch)?;
        budget.validate_input(name, batch)?;
        let keys = self.keys(batch, &plain_cols)?;

        for row_idx in 0..batch.num_rows() {
            if weights.is_null(row_idx) {
                return Err(DbError::Storage(format!(
                    "upsert MV weight is null at row {row_idx}"
                )));
            }
            let w = weights.value(row_idx);
            if w == 0 {
                continue;
            }
            let key = Self::key_at(keys.as_ref(), row_idx);
            let values = if w > 0 {
                Some(Self::row_values(
                    batch,
                    plain_cols.iter().copied(),
                    row_idx,
                )?)
            } else {
                None
            };
            let bytes = match &values {
                Some(values) => Self::row_size(&key, values)?
                    .checked_add(STAGED_ROW_OVERHEAD)
                    .ok_or_else(size_overflow)?,
                None => std::mem::size_of::<(OwnedRow, StagedRow)>()
                    .checked_add(key.as_ref().len())
                    .ok_or_else(size_overflow)?,
            };
            // A changelog update retracts and re-inserts one key, so place each key once.
            match staged.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    budget.replace(name, Some(entry.get().bytes), Some(bytes))?;
                    let vnode = entry.get().vnode;
                    entry.insert(StagedRow {
                        values,
                        bytes,
                        vnode,
                    });
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    budget.replace(name, None, Some(bytes))?;
                    let vnode = self.vnode_of(entry.key());
                    entry.insert(StagedRow {
                        values,
                        bytes,
                        vnode,
                    });
                }
            }
        }
        Ok(())
    }

    pub(super) fn prepare_cycle(
        &self,
        name: &str,
        batches: &[RecordBatch],
        limits: MvLimits,
    ) -> Result<UpsertDelta, DbError> {
        let mut staged = HashMap::new();
        let mut budget = StagingBudget::new(limits, STAGED_ROW_OVERHEAD);
        for batch in batches {
            self.stage_batch(name, batch, &mut staged, &mut budget)?;
        }
        let (mut rows, mut removed, mut added) = (self.rows, 0usize, 0usize);
        for (key, replacement) in &staged {
            if let Some(old) = self.slices.slot(replacement.vnode).get(key) {
                removed = removed
                    .checked_add(Self::row_size(key, old)?)
                    .ok_or_else(size_overflow)?;
                rows -= 1;
            }
            if replacement.values.is_some() {
                added = added
                    .checked_add(replacement.bytes - STAGED_ROW_OVERHEAD)
                    .ok_or_else(size_overflow)?;
                rows = rows.checked_add(1).ok_or_else(size_overflow)?;
            }
        }
        let bytes = self
            .approx_bytes
            .checked_sub(removed)
            .and_then(|bytes| bytes.checked_add(added))
            .ok_or_else(size_overflow)?;
        limits.validate(name, rows, bytes)?;
        Ok(UpsertDelta {
            rows: staged,
            row_count: rows,
            bytes,
        })
    }

    pub(super) fn apply_prepared(&mut self, delta: UpsertDelta) {
        for (key, replacement) in delta.rows {
            let slice = self.slices.slot_mut(replacement.vnode);
            if let Some(values) = replacement.values {
                slice.insert(key, values);
            } else {
                slice.remove(&key);
            }
        }
        self.rows = delta.row_count;
        self.approx_bytes = delta.bytes;
    }

    /// Restore one vnode's committed rows (no weight column). Every row must belong to `vnode`.
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
        let plain_cols: Vec<usize> = (0..batch.num_columns()).collect();
        let keys = self.keys(batch, &plain_cols)?;
        for row_idx in 0..batch.num_rows() {
            let key = Self::key_at(keys.as_ref(), row_idx);
            let placed = self.vnode_of(&key);
            if placed != vnode {
                return Err(DbError::Storage(format!(
                    "upsert MV frame for vnode {vnode} holds a row placed on vnode {placed}"
                )));
            }
            let values = Self::row_values(batch, 0..batch.num_columns(), row_idx)?;
            if self.slices.slot(vnode).contains_key(&key) {
                return Err(DbError::Storage(format!(
                    "upsert MV frame for vnode {vnode} repeats a key at row {row_idx}"
                )));
            }
            let bytes = self
                .approx_bytes
                .checked_add(Self::row_size(&key, &values)?)
                .ok_or_else(size_overflow)?;
            let rows = self.rows.checked_add(1).ok_or_else(size_overflow)?;
            limits.validate(name, rows, bytes)?;
            self.slices.slot_mut(vnode).insert(key, values);
            self.rows = rows;
            self.approx_bytes = bytes;
        }
        Ok(())
    }

    pub(super) fn to_record_batch(&self, schema: &SchemaRef) -> Result<RecordBatch, DbError> {
        if self.rows == 0 {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        // One pass over the row maps assembling all columns, rather than one full scan per column.
        let ncols = schema.fields().len();
        let mut columns: Vec<Vec<ScalarValue>> =
            (0..ncols).map(|_| Vec::with_capacity(self.rows)).collect();
        for row in self.slices.slots().flat_map(|(_, slice)| slice.values()) {
            for (c, col) in columns.iter_mut().enumerate() {
                col.push(row[c].clone());
            }
        }
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(ncols);
        for col in columns {
            arrays.push(
                ScalarValue::iter_to_array(col)
                    .map_err(|e| DbError::Storage(format!("upsert MV column build: {e}")))?,
            );
        }
        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| DbError::Storage(format!("upsert MV batch assembly: {e}")))
    }
}
