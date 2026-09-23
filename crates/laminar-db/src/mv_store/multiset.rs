//! Counted MV rows with live quotas and cached snapshot expansion admission.

use super::admission::{size_overflow, MvLimits};
use super::staging::StagingBudget;
use super::weight_and_plain_cols;
use crate::error::DbError;
use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::SchemaRef;
use arrow::row::{OwnedRow, Row, RowConverter, SortField};
use std::collections::HashMap;
use std::sync::Arc;

use super::{
    DEFAULT_MAX_BYTES, MAX_MULTISET_MATERIALIZED_ROWS, MULTISET_MATERIALIZATION_ROW_OVERHEAD,
};

/// Z-set multiset from a `__weight` changelog: full output row keyed to an integer multiplicity.
pub(super) struct MultisetState {
    pub(super) row_converter: Arc<RowConverter>,
    pub(super) counts: HashMap<OwnedRow, i64>,
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
    counts: Vec<(OwnedRow, i64)>,
    bytes: usize,
    expansion: Expansion,
}

const STAGED_ROW_OVERHEAD: usize =
    std::mem::size_of::<(OwnedRow, i128)>() - std::mem::size_of::<(OwnedRow, i64)>();

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

impl MultisetState {
    pub(super) fn new(schema: &SchemaRef) -> Result<Self, DbError> {
        let sort_fields: Vec<SortField> = schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect();
        let row_converter = Arc::new(
            RowConverter::new(sort_fields)
                .map_err(|e| DbError::Storage(format!("multiset MV row converter: {e}")))?,
        );
        Ok(Self {
            row_converter,
            counts: HashMap::new(),
            approx_bytes: 0,
            expansion: Expansion::default(),
        })
    }

    fn stage_batch(
        &self,
        name: &str,
        batch: &RecordBatch,
        deltas: &mut HashMap<OwnedRow, i128>,
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
                    let delta = entry.get().checked_add(i128::from(w)).ok_or_else(|| {
                        DbError::Storage("multiset MV staged multiplicity overflow".into())
                    })?;
                    if delta == 0 {
                        budget.replace(name, Some(bytes), None)?;
                        entry.remove();
                    } else {
                        entry.insert(delta);
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    budget.replace(name, None, Some(bytes))?;
                    entry.insert(i128::from(w));
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
        let (mut rows, mut removed, mut added) = (self.counts.len(), 0usize, 0usize);
        let mut expansion = self.expansion;
        for (key, delta) in deltas {
            let old = self.counts.get(&key).copied().unwrap_or(0);
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
            resolved.push((key, next));
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
            bytes,
            expansion,
        })
    }

    pub(super) fn apply_prepared(&mut self, delta: MultisetDelta) {
        for (key, count) in delta.counts {
            if count == 0 {
                self.counts.remove(&key);
            } else {
                self.counts.insert(key, count);
            }
        }
        self.approx_bytes = delta.bytes;
        self.expansion = delta.expansion;
    }

    /// Restore a counted checkpoint batch. Checkpoints contain exactly one row per distinct
    /// value; duplicates are corruption rather than an alternate encoding.
    pub(super) fn load_counted_snapshot(
        &mut self,
        name: &str,
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
            if self.counts.contains_key(&key) {
                return Err(DbError::Storage(format!(
                    "multiset MV checkpoint contains a duplicate value at row {row_idx}"
                )));
            }
            let bytes = self
                .approx_bytes
                .checked_add(row_size(&key)?)
                .ok_or_else(size_overflow)?;
            limits.validate(name, self.counts.len() + 1, bytes)?;
            self.expansion.replace(&key, 0, count)?;
            self.counts.insert(key, count);
            self.approx_bytes = bytes;
        }
        Ok(())
    }

    pub(super) fn to_record_batch(&self, schema: &SchemaRef) -> Result<RecordBatch, DbError> {
        if self.counts.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        self.expansion.validate()?;
        let rows = self.counts.iter().flat_map(|(key, &count)| {
            std::iter::repeat_n(key.row(), usize::try_from(count).unwrap_or(0))
        });
        let arrays = decode_rows(&self.row_converter, schema, rows)?;
        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| DbError::Storage(format!("multiset MV batch assembly: {e}")))
    }
}
