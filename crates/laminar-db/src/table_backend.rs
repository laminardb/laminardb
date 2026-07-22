//! Backend storage for reference/dimension table rows.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;

use arrow::array::{Array, ArrayData, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow::row::OwnedRow;

use crate::error::DbError;

pub(crate) enum TableBackend {
    InMemory {
        rows: HashMap<OwnedRow, RecordBatch>,
    },
}

impl TableBackend {
    pub fn in_memory() -> Self {
        Self::InMemory {
            rows: HashMap::new(),
        }
    }

    /// Insert or update a row; returns `true` if the key existed.
    pub fn put(&mut self, key: OwnedRow, batch: RecordBatch) -> bool {
        match self {
            Self::InMemory { rows } => rows.insert(key, batch).is_some(),
        }
    }

    pub fn checkpoint_capture_estimated_bytes(&self) -> Result<u64, DbError> {
        let Self::InMemory { rows } = self;
        let mut bytes = 0u64;
        let mut variadic_buffers = HashMap::<usize, usize>::new();
        for (key, batch) in rows {
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
        match self {
            Self::InMemory { rows } => rows
                .iter()
                .map(|(key, batch)| (key.as_ref().to_vec(), batch.clone()))
                .collect(),
        }
    }

    /// Build one replacement from a complete sequence of snapshot batches.
    /// The same backend is used for every batch so duplicates across batch
    /// boundaries are rejected as well as duplicates within one batch.
    pub fn from_batches(
        batches: &[RecordBatch],
        primary_key_index: usize,
        key_converter: &arrow::row::RowConverter,
    ) -> Result<Self, DbError> {
        let mut backend = Self::in_memory();
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
                if backend.put(keys.row(row_index).owned(), batch.slice(row_index, 1)) {
                    return Err(DbError::Storage(
                        "reference-table replacement contains duplicate primary keys".into(),
                    ));
                }
            }
        }
        Ok(backend)
    }

    pub fn row_count(&self) -> usize {
        match self {
            Self::InMemory { rows } => rows.len(),
        }
    }

    pub fn to_record_batch(&self, schema: &SchemaRef) -> Result<Option<RecordBatch>, DbError> {
        match self {
            Self::InMemory { rows } => {
                if rows.is_empty() {
                    return Ok(Some(RecordBatch::new_empty(schema.clone())));
                }
                let batches: Vec<&RecordBatch> = rows.values().collect();
                arrow::compute::concat_batches(schema, batches.iter().copied())
                    .map(Some)
                    .map_err(|e| DbError::Storage(format!("concat batches: {e}")))
            }
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::row::{RowConverter, SortField};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
        ]))
    }

    fn make_batch(id: i32, name: &str, price: f64) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int32Array::from(vec![id])),
                Arc::new(StringArray::from(vec![name])),
                Arc::new(Float64Array::from(vec![price])),
            ],
        )
        .unwrap()
    }

    fn key(id: i32) -> OwnedRow {
        RowConverter::new(vec![SortField::new(DataType::Int32)])
            .unwrap()
            .convert_columns(&[Arc::new(Int32Array::from(vec![id]))])
            .unwrap()
            .row(0)
            .owned()
    }

    #[test]
    fn in_memory_backend_round_trips_the_used_api() {
        let mut backend = TableBackend::in_memory();
        // Insert (new) then update (existing) report the right prior state.
        assert!(!backend.put(key(1), make_batch(1, "A", 1.0)));
        assert!(backend.put(key(1), make_batch(1, "B", 2.0)));

        // Empty schema-only batch when present; row when populated.
        let schema = test_schema();
        backend.put(key(2), make_batch(2, "C", 3.0));
        assert_eq!(
            backend
                .to_record_batch(&schema)
                .unwrap()
                .unwrap()
                .num_rows(),
            2
        );
    }
}
