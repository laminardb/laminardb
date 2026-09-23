//! In-memory reference/dimension table rows.
#![allow(clippy::disallowed_types)] // checkpoint-size accounting scratch map

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayData, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow::row::OwnedRow;
use rustc_hash::FxHashMap;

use crate::error::DbError;

mod retention;
pub(crate) use retention::TableLimits;
use retention::{BatchRetention, BufferUsage, RetentionDelta};

struct StoredRow {
    batch: RecordBatch,
    retention: Arc<BatchRetention>,
}

#[derive(Clone, Copy)]
pub(crate) enum TableUpdate {
    Upsert,
    Snapshot,
}

pub(crate) struct TableRows {
    rows: FxHashMap<OwnedRow, StoredRow>,
    buffers: FxHashMap<usize, BufferUsage>,
    retained_bytes: usize,
}

impl TableRows {
    pub fn new() -> Self {
        Self {
            rows: FxHashMap::default(),
            buffers: FxHashMap::default(),
            retained_bytes: 0,
        }
    }

    /// Preflight only affected keys and allocations, then commit the final delta.
    pub fn apply_batch(
        &mut self,
        name: &str,
        batch: &RecordBatch,
        primary_key_index: usize,
        key_converter: &arrow::row::RowConverter,
        limits: TableLimits,
        update: TableUpdate,
    ) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let keys = key_converter
            .convert_columns(&[Arc::clone(batch.column(primary_key_index))])
            .map_err(|error| {
                DbError::InsertError(format!(
                    "failed to encode primary key for table '{name}': {error}"
                ))
            })?;
        // Last occurrence wins, including when a batch repeats an existing key.
        let mut updates = FxHashMap::default();
        for i in 0..batch.num_rows() {
            updates.insert(keys.row(i).owned(), i);
        }
        if matches!(update, TableUpdate::Snapshot)
            && (updates.len() != batch.num_rows()
                || updates.keys().any(|key| self.rows.contains_key(key)))
        {
            return Err(DbError::Storage(format!(
                "reference-table '{name}' replacement contains duplicate primary keys"
            )));
        }
        let retention = Arc::new(BatchRetention::new(batch)?);
        let mut delta = RetentionDelta::new(self.retained_bytes);
        let mut row_count = self.rows.len();
        let mut retired = FxHashMap::<usize, (&BatchRetention, usize)>::default();
        for key in updates.keys() {
            if let Some(old) = self.rows.get(key) {
                delta.remove_row(key.as_ref().len(), old.retention.row_bytes)?;
                let entry = retired
                    .entry(Arc::as_ptr(&old.retention) as usize)
                    .or_insert((&old.retention, 0));
                entry.1 += 1;
            } else {
                row_count = row_count
                    .checked_add(1)
                    .ok_or_else(retention::size_overflow)?;
            }
            delta.add_row(key.as_ref().len(), retention.row_bytes)?;
        }
        for (storage, references) in retired.into_values() {
            delta.remove_batch(storage, references)?;
        }
        delta.add_batch(&retention, updates.len())?;
        let (retained_bytes, buffers) = delta.prepare(&self.buffers)?;
        limits.validate(name, row_count, retained_bytes)?;

        // INVARIANT: every fallible validation precedes live row/ledger mutation.
        for (key, index) in updates {
            self.rows.insert(
                key,
                StoredRow {
                    batch: batch.slice(index, 1),
                    retention: Arc::clone(&retention),
                },
            );
        }
        for (identity, usage) in buffers {
            if usage.references == 0 {
                self.buffers.remove(&identity);
            } else {
                self.buffers.insert(identity, usage);
            }
        }
        self.retained_bytes = retained_bytes;
        Ok(())
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub fn checkpoint_capture_estimated_bytes(&self) -> Result<u64, DbError> {
        let mut bytes = 0u64;
        let mut variadic_buffers = HashMap::<usize, usize>::new();
        for (key, row) in &self.rows {
            let batch = &row.batch;
            add_checkpoint_capture_bytes(
                &mut bytes,
                std::mem::size_of::<(Vec<u8>, RecordBatch)>(),
            )?;
            add_checkpoint_capture_bytes(&mut bytes, key.as_ref().len())?;
            for column in batch.columns() {
                let data = column.to_data();
                let logical_bytes = data.get_slice_memory_size().map_err(|error| {
                    DbError::Checkpoint(format!(
                        "reference-table checkpoint size estimation failed: {error}"
                    ))
                })?;
                add_checkpoint_capture_bytes(&mut bytes, logical_bytes)?;
                collect_variadic_buffers(&data, &mut variadic_buffers)?;
            }
        }
        for buffer_bytes in variadic_buffers.into_values() {
            add_checkpoint_capture_bytes(&mut bytes, buffer_bytes)?;
        }
        // A capture pins live backing allocations even when its serialized rows are tiny.
        let retained =
            u64::try_from(self.retained_bytes).map_err(|_| checkpoint_capture_size_overflow())?;
        Ok(bytes.max(retained))
    }

    /// Clone owned keys and shallow Arrow row slices for off-lock encoding.
    pub fn checkpoint_rows(&self) -> Vec<(Vec<u8>, RecordBatch)> {
        self.rows
            .iter()
            .map(|(key, row)| (key.as_ref().to_vec(), row.batch.clone()))
            .collect()
    }

    /// Build one replacement from a complete sequence of snapshot batches.
    /// One row map covers every batch, so duplicate primary keys are rejected
    /// across batch boundaries as well as within a batch.
    pub fn from_batches(
        name: &str,
        batches: &[RecordBatch],
        primary_key_index: usize,
        key_converter: &arrow::row::RowConverter,
        limits: TableLimits,
    ) -> Result<Self, DbError> {
        let mut rows = Self::new();
        for batch in batches {
            rows.apply_batch(
                name,
                batch,
                primary_key_index,
                key_converter,
                limits,
                TableUpdate::Snapshot,
            )?;
        }
        Ok(rows)
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn to_record_batch(&self, schema: &SchemaRef) -> Result<Option<RecordBatch>, DbError> {
        if self.rows.is_empty() {
            return Ok(Some(RecordBatch::new_empty(schema.clone())));
        }
        let batches: Vec<&RecordBatch> = self.rows.values().map(|row| &row.batch).collect();
        arrow::compute::concat_batches(schema, batches.iter().copied())
            .map(Some)
            .map_err(|e| DbError::Storage(format!("concat batches: {e}")))
    }
}

fn add_checkpoint_capture_bytes(total: &mut u64, bytes: usize) -> Result<(), DbError> {
    let bytes = u64::try_from(bytes).map_err(|_| checkpoint_capture_size_overflow())?;
    *total = total
        .checked_add(bytes)
        .ok_or_else(checkpoint_capture_size_overflow)?;
    Ok(())
}

fn checkpoint_capture_size_overflow() -> DbError {
    DbError::Checkpoint("reference-table checkpoint capture size overflow".into())
}

fn collect_variadic_buffers(
    data: &ArrayData,
    buffers: &mut HashMap<usize, usize>,
) -> Result<(), DbError> {
    if matches!(
        data.data_type(),
        arrow::datatypes::DataType::Utf8View | arrow::datatypes::DataType::BinaryView
    ) {
        for buffer in data.buffers().iter().skip(1) {
            let end = buffer
                .ptr_offset()
                .checked_add(buffer.len())
                .ok_or_else(checkpoint_capture_size_overflow)?;
            let retained_bytes = buffer.capacity().max(end);
            buffers
                .entry(buffer.data_ptr().as_ptr() as usize)
                .and_modify(|bytes| *bytes = (*bytes).max(retained_bytes))
                .or_insert(retained_bytes);
        }
    }
    for child in data.child_data() {
        collect_variadic_buffers(child, buffers)?;
    }
    Ok(())
}
