//! What a view's subscribers receive: the view's rows when they attach, then each change to
//! them. A changelog view's rows carry `__weight`: `+1` in the snapshot, and in a change the
//! amount by which it changed the row's count. An append view's rows are plain; rows leaving
//! its retention are not changes.

use std::sync::Arc;

#[cfg(feature = "cluster")]
use arrow::array::Array;
use arrow::array::{Int64Array, RecordBatch};
#[cfg(feature = "cluster")]
use arrow::compute::interleave;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
#[cfg(feature = "cluster")]
use arrow::row::{RowConverter, SortField};
use laminar_core::changelog::WEIGHT_COLUMN;
#[cfg(feature = "cluster")]
use rustc_hash::FxHashMap;

use super::admission::PreparedUpdate;
use super::{MvEntry, MvStorageMode, MvStore, ViewRead};
use crate::error::DbError;

/// The schema subscribers of a view stored as `mode` with rows of `schema` receive.
pub(super) fn subscription_schema(schema: &SchemaRef, mode: &MvStorageMode) -> SchemaRef {
    match mode {
        MvStorageMode::AppendLog { .. } => Arc::clone(schema),
        MvStorageMode::Changelog { .. } => {
            let mut fields = schema.fields().to_vec();
            fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
        }
    }
}

impl MvEntry {
    fn subscription_schema(&self) -> SchemaRef {
        subscription_schema(&self.schema, &self.mode)
    }

    /// `update`'s change to this view, as its subscribers receive it.
    pub(super) fn changes(&self, update: &PreparedUpdate<'_>) -> Result<Vec<RecordBatch>, DbError> {
        let change = match (update, &self.upsert, &self.multiset) {
            (PreparedUpdate::Unchanged, ..) => None,
            (PreparedUpdate::Log(update), ..) => return Ok(update.appended().cloned().collect()),
            (PreparedUpdate::Upsert(delta), Some(state), _) => {
                state.changes(delta, &self.subscription_schema())?
            }
            (PreparedUpdate::Multiset(delta), _, Some(state)) => {
                state.changes(delta, &self.subscription_schema())?
            }
            _ => {
                return Err(DbError::Storage(
                    "MV change does not match its result state".into(),
                ))
            }
        };
        Ok(change.into_iter().collect())
    }
}

impl MvStore {
    /// The schema a subscription to view `name` receives.
    pub(crate) fn subscription_schema(&self, name: &str) -> Option<SchemaRef> {
        self.entries.get(name).map(MvEntry::subscription_schema)
    }

    /// View `name`'s rows as its subscribers first receive them, captured under the caller's
    /// store read guard; an append view's committed segments load after the guard.
    pub(crate) fn subscription_snapshot(&self, name: &str) -> Result<Option<ViewRead>, DbError> {
        let Some(entry) = self.entries.get(name) else {
            return Ok(None);
        };
        Ok(Some(match self.read(name)? {
            Some(ViewRead::Ready(rows)) if entry.log.is_none() => {
                ViewRead::Ready(with_unit_weights(&rows, entry.subscription_schema())?)
            }
            Some(read) => read,
            None => return Ok(None),
        }))
    }
}

/// `rows` of a changelog view, each inserted once.
pub(super) fn with_unit_weights(
    rows: &RecordBatch,
    schema: SchemaRef,
) -> Result<RecordBatch, DbError> {
    let mut columns = rows.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(vec![1; rows.num_rows()])));
    RecordBatch::try_new(schema, columns)
        .map_err(|e| DbError::Storage(format!("MV snapshot weights: {e}")))
}

/// The change from the rows of `older` to those of `newer`: each distinct row whose count
/// differs, weighted by the difference, in `schema` (the rows' columns, then `__weight`).
#[cfg(feature = "cluster")]
pub(super) fn row_diff(
    schema: &SchemaRef,
    older: &[RecordBatch],
    newer: &[RecordBatch],
) -> Result<Option<RecordBatch>, DbError> {
    let error = |e: arrow::error::ArrowError| DbError::Storage(format!("MV change: {e}"));
    let columns = schema.fields().len() - 1;
    let converter = RowConverter::new(
        schema.fields()[..columns]
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )
    .map_err(error)?;
    let sides: Vec<(&RecordBatch, i64)> = older
        .iter()
        .map(|batch| (batch, -1))
        .chain(newer.iter().map(|batch| (batch, 1)))
        .collect();
    let encoded = sides
        .iter()
        .map(|(batch, _)| converter.convert_columns(batch.columns()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(error)?;
    let mut counts = FxHashMap::default();
    let mut first_seen = Vec::new();
    for (side, rows) in encoded.iter().enumerate() {
        for (row_index, row) in rows.iter().enumerate() {
            let slot = *counts.entry(row).or_insert_with(|| {
                first_seen.push(((side, row_index), 0_i64));
                first_seen.len() - 1
            });
            first_seen[slot].1 += sides[side].1;
        }
    }
    let (indices, weights): (Vec<_>, Vec<_>) = first_seen
        .into_iter()
        .filter(|(_, weight)| *weight != 0)
        .unzip();
    if indices.is_empty() {
        return Ok(None);
    }
    let mut arrays = (0..columns)
        .map(|column| {
            let sources: Vec<&dyn Array> = sides
                .iter()
                .map(|(batch, _)| batch.column(column).as_ref())
                .collect();
            interleave(&sources, &indices)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(error)?;
    arrays.push(Arc::new(Int64Array::from(weights)));
    RecordBatch::try_new(Arc::clone(schema), arrays)
        .map(Some)
        .map_err(error)
}
