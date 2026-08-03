//! In-memory reference/dimension table rows.
#![allow(clippy::disallowed_types)] // checkpoint-size accounting scratch map

use std::collections::HashMap;

use arrow::array::{Array, ArrayData, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow::row::OwnedRow;
use rustc_hash::FxHashMap;

use crate::error::DbError;

pub(crate) struct TableRows {
    rows: FxHashMap<OwnedRow, RecordBatch>,
}

impl TableRows {
    pub fn new() -> Self {
        Self {
            rows: FxHashMap::default(),
        }
    }

    /// Insert or update a row; returns `true` if the key existed.
    pub fn put(&mut self, key: OwnedRow, batch: RecordBatch) -> bool {
        self.rows.insert(key, batch).is_some()
    }

    pub fn checkpoint_capture_estimated_bytes(&self) -> Result<u64, DbError> {
        let mut bytes = 0u64;
        let mut variadic_buffers = HashMap::<usize, usize>::new();
        for (key, batch) in &self.rows {
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
        Ok(bytes)
    }

    /// Clone owned keys and shallow Arrow row slices for off-lock encoding.
    pub fn checkpoint_rows(&self) -> Vec<(Vec<u8>, RecordBatch)> {
        self.rows
            .iter()
            .map(|(key, batch)| (key.as_ref().to_vec(), batch.clone()))
            .collect()
    }

    /// Build one replacement from a complete sequence of snapshot batches.
    /// One row map covers every batch, so duplicate primary keys are rejected
    /// across batch boundaries as well as within a batch.
    pub fn from_batches(
        batches: &[RecordBatch],
        primary_key_index: usize,
        key_converter: &arrow::row::RowConverter,
    ) -> Result<Self, DbError> {
        let mut rows = Self::new();
        for batch in batches {
            let primary_key = batch.column(primary_key_index);
            let keys = key_converter
                .convert_columns(&[std::sync::Arc::clone(primary_key)])
                .map_err(|error| {
                    DbError::Storage(format!(
                        "failed to encode reference-table primary keys: {error}"
                    ))
                })?;
            for row_index in 0..batch.num_rows() {
                if rows.put(keys.row(row_index).owned(), batch.slice(row_index, 1)) {
                    return Err(DbError::Storage(
                        "reference-table replacement contains duplicate primary keys".into(),
                    ));
                }
            }
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
        let batches: Vec<&RecordBatch> = self.rows.values().collect();
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
