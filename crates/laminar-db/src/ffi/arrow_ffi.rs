//! Arrow C Data Interface for zero-copy data exchange.

use arrow::array::{Array, RecordBatch, StructArray};
use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};

use super::error::{
    clear_last_error, set_last_error, LAMINAR_ERR_INTERNAL, LAMINAR_ERR_NULL_POINTER, LAMINAR_OK,
};
use super::query::LaminarRecordBatch;
use super::schema::LaminarSchema;
use crate::api::ApiError;

/// Export a `RecordBatch` to the Arrow C Data Interface.
///
/// # Safety
///
/// `batch`, `out_array`, and `out_schema` must be valid non-null pointers;
/// caller must call the release callbacks on both output structs.
#[no_mangle]
pub unsafe extern "C" fn laminar_batch_export(
    batch: *mut LaminarRecordBatch,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> i32 {
    clear_last_error();

    if batch.is_null() || out_array.is_null() || out_schema.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let batch_ref = unsafe { &(*batch) };
    let record_batch = batch_ref.inner();

    let struct_array: StructArray = record_batch.clone().into();
    let data = struct_array.into_data();

    match to_ffi(&data) {
        Ok((array, schema)) => {
            unsafe {
                std::ptr::write(out_array, array);
                std::ptr::write(out_schema, schema);
            }
            LAMINAR_OK
        }
        Err(e) => {
            set_last_error(ApiError::internal(format!("Arrow FFI export failed: {e}")));
            LAMINAR_ERR_INTERNAL
        }
    }
}

/// Export just the schema to the Arrow C Data Interface.
///
/// # Safety
///
/// `schema` and `out_schema` must be valid non-null pointers; caller must call the release callback.
#[no_mangle]
pub unsafe extern "C" fn laminar_schema_export(
    schema: *mut LaminarSchema,
    out_schema: *mut FFI_ArrowSchema,
) -> i32 {
    clear_last_error();

    if schema.is_null() || out_schema.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let schema_ref = unsafe { (*schema).schema() };

    match FFI_ArrowSchema::try_from(schema_ref.as_ref()) {
        Ok(ffi_schema) => {
            unsafe {
                std::ptr::write(out_schema, ffi_schema);
            }
            LAMINAR_OK
        }
        Err(e) => {
            set_last_error(ApiError::internal(format!(
                "Arrow FFI schema export failed: {e}"
            )));
            LAMINAR_ERR_INTERNAL
        }
    }
}

/// Export a single column from a `RecordBatch` to the Arrow C Data Interface.
///
/// # Safety
///
/// `batch`, `out_array`, and `out_schema` must be valid non-null pointers;
/// `column_index` must be in range; caller must call the release callbacks.
#[no_mangle]
pub unsafe extern "C" fn laminar_batch_export_column(
    batch: *mut LaminarRecordBatch,
    column_index: usize,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> i32 {
    clear_last_error();

    if batch.is_null() || out_array.is_null() || out_schema.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let batch_ref = unsafe { &(*batch) };
    let record_batch = batch_ref.inner();

    if column_index >= record_batch.num_columns() {
        set_last_error(ApiError::internal(format!(
            "Column index {column_index} out of bounds (batch has {} columns)",
            record_batch.num_columns()
        )));
        return LAMINAR_ERR_NULL_POINTER;
    }

    let column = record_batch.column(column_index);
    let data = column.to_data();

    match to_ffi(&data) {
        Ok((array, schema)) => {
            unsafe {
                std::ptr::write(out_array, array);
                std::ptr::write(out_schema, schema);
            }
            LAMINAR_OK
        }
        Err(e) => {
            set_last_error(ApiError::internal(format!(
                "Arrow FFI column export failed: {e}"
            )));
            LAMINAR_ERR_INTERNAL
        }
    }
}

/// Import a `RecordBatch` from the Arrow C Data Interface, taking ownership of both structs.
///
/// # Safety
///
/// `array`, `schema`, and `out` must be valid non-null pointers;
/// ownership of `array` and `schema` is transferred and they are released by this function.
#[no_mangle]
pub unsafe extern "C" fn laminar_batch_import(
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    out: *mut *mut LaminarRecordBatch,
) -> i32 {
    clear_last_error();

    if array.is_null() || schema.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let ffi_array = unsafe { std::ptr::read(array) };
    let ffi_schema = unsafe { std::ptr::read(schema) };

    // Zero the originals to prevent double-free; callers must not use them after this.
    unsafe {
        std::ptr::write_bytes(array, 0, 1);
        std::ptr::write_bytes(schema, 0, 1);
    }

    match from_ffi(ffi_array, &ffi_schema) {
        Ok(data) => {
            let struct_array = StructArray::from(data);
            let batch = RecordBatch::from(struct_array);

            let handle = Box::new(LaminarRecordBatch::new(batch));
            unsafe { *out = Box::into_raw(handle) };
            LAMINAR_OK
        }
        Err(e) => {
            set_last_error(ApiError::internal(format!("Arrow FFI import failed: {e}")));
            LAMINAR_ERR_INTERNAL
        }
    }
}

/// Alias for [`laminar_batch_import`] for use when creating data for writing.
///
/// # Safety
///
/// Same requirements as [`laminar_batch_import`].
#[no_mangle]
pub unsafe extern "C" fn laminar_batch_create(
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    out: *mut *mut LaminarRecordBatch,
) -> i32 {
    laminar_batch_import(array, schema, out)
}

#[cfg(test)]
#[allow(clippy::borrow_as_ptr)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), None])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_export_import_roundtrip() {
        let batch = create_test_batch();
        let mut ffi_batch = LaminarRecordBatch::new(batch.clone());

        // Export
        let mut out_array = FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();

        let rc = unsafe { laminar_batch_export(&mut ffi_batch, &mut out_array, &mut out_schema) };
        assert_eq!(rc, LAMINAR_OK);

        // Import
        let mut imported: *mut LaminarRecordBatch = std::ptr::null_mut();
        let rc = unsafe { laminar_batch_import(&mut out_array, &mut out_schema, &mut imported) };
        assert_eq!(rc, LAMINAR_OK);
        assert!(!imported.is_null());

        // Verify data matches
        let imported_batch = unsafe { (*imported).inner() };
        assert_eq!(batch.num_rows(), imported_batch.num_rows());
        assert_eq!(batch.num_columns(), imported_batch.num_columns());

        // Clean up
        unsafe {
            super::super::query::laminar_batch_free(imported);
        }
    }

    #[test]
    fn test_export_column() {
        let batch = create_test_batch();
        let mut ffi_batch = LaminarRecordBatch::new(batch);

        let mut out_array = FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();

        // Export first column
        let rc = unsafe {
            laminar_batch_export_column(&mut ffi_batch, 0, &mut out_array, &mut out_schema)
        };
        assert_eq!(rc, LAMINAR_OK);

        // Import and verify
        let data = unsafe { from_ffi(out_array, &out_schema) }.unwrap();
        let array = Int64Array::from(data);
        assert_eq!(array.len(), 3);
        assert_eq!(array.value(0), 1);
        assert_eq!(array.value(1), 2);
        assert_eq!(array.value(2), 3);
    }

    #[test]
    fn test_export_column_out_of_bounds() {
        let batch = create_test_batch();
        let mut ffi_batch = LaminarRecordBatch::new(batch);

        let mut out_array = FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();

        // Try to export non-existent column
        let rc = unsafe {
            laminar_batch_export_column(&mut ffi_batch, 99, &mut out_array, &mut out_schema)
        };
        assert_eq!(rc, LAMINAR_ERR_NULL_POINTER);
    }

    #[test]
    fn test_schema_export() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let mut ffi_schema_handle = LaminarSchema::new(schema);
        let mut out_schema = FFI_ArrowSchema::empty();

        let rc = unsafe { laminar_schema_export(&mut ffi_schema_handle, &mut out_schema) };
        assert_eq!(rc, LAMINAR_OK);

        // The schema is released when dropped
        drop(out_schema);
    }

    #[test]
    fn test_null_pointer_checks() {
        let mut out_array = FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();
        let mut out: *mut LaminarRecordBatch = std::ptr::null_mut();

        // Export with null batch
        let rc =
            unsafe { laminar_batch_export(std::ptr::null_mut(), &mut out_array, &mut out_schema) };
        assert_eq!(rc, LAMINAR_ERR_NULL_POINTER);

        // Import with null array
        let rc = unsafe { laminar_batch_import(std::ptr::null_mut(), &mut out_schema, &mut out) };
        assert_eq!(rc, LAMINAR_ERR_NULL_POINTER);

        // Schema export with null schema
        let rc = unsafe { laminar_schema_export(std::ptr::null_mut(), &mut out_schema) };
        assert_eq!(rc, LAMINAR_ERR_NULL_POINTER);
    }
}
