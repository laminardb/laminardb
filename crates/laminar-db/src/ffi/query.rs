//! FFI query result functions.

use std::ptr;

use arrow::array::RecordBatch;

use crate::api::{QueryResult, QueryStream};

use super::error::{clear_last_error, set_last_error, LAMINAR_ERR_NULL_POINTER, LAMINAR_OK};
use super::schema::LaminarSchema;

/// Opaque handle for materialized query results.
#[repr(C)]
pub struct LaminarQueryResult {
    inner: QueryResult,
}

impl LaminarQueryResult {
    pub(crate) fn new(result: QueryResult) -> Self {
        Self { inner: result }
    }
}

/// Opaque handle for streaming query results.
#[repr(C)]
pub struct LaminarQueryStream {
    inner: QueryStream,
}

impl LaminarQueryStream {
    pub(crate) fn new(stream: QueryStream) -> Self {
        Self { inner: stream }
    }
}

/// Opaque Arrow `RecordBatch` handle. Free with `laminar_batch_free`.
#[repr(C)]
pub struct LaminarRecordBatch {
    inner: RecordBatch,
}

impl LaminarRecordBatch {
    pub(crate) fn new(batch: RecordBatch) -> Self {
        Self { inner: batch }
    }

    pub(crate) fn into_inner(self) -> RecordBatch {
        self.inner
    }

    pub(crate) fn inner(&self) -> &RecordBatch {
        &self.inner
    }
}

/// Get the schema from a query result.
///
/// # Safety
///
/// `result` and `out` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn laminar_result_schema(
    result: *mut LaminarQueryResult,
    out: *mut *mut LaminarSchema,
) -> i32 {
    clear_last_error();

    if result.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let schema = unsafe { (*result).inner.schema() };
    let handle = Box::new(LaminarSchema::new(schema));

    unsafe { *out = Box::into_raw(handle) };
    LAMINAR_OK
}

/// Get the total row count from a query result.
///
/// # Safety
///
/// `result` and `out` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn laminar_result_num_rows(
    result: *mut LaminarQueryResult,
    out: *mut usize,
) -> i32 {
    clear_last_error();

    if result.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    unsafe {
        *out = (*result).inner.num_rows();
    }
    LAMINAR_OK
}

/// Get the number of batches in a query result.
///
/// # Safety
///
/// `result` and `out` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn laminar_result_num_batches(
    result: *mut LaminarQueryResult,
    out: *mut usize,
) -> i32 {
    clear_last_error();

    if result.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    unsafe {
        *out = (*result).inner.num_batches();
    }
    LAMINAR_OK
}

/// Get a batch by index from a query result.
///
/// # Safety
///
/// `result` and `out` must be valid non-null pointers; `index` must be in range.
#[no_mangle]
pub unsafe extern "C" fn laminar_result_get_batch(
    result: *mut LaminarQueryResult,
    index: usize,
    out: *mut *mut LaminarRecordBatch,
) -> i32 {
    clear_last_error();

    if result.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let result_ref = unsafe { &(*result).inner };

    if let Some(batch) = result_ref.batch(index) {
        let handle = Box::new(LaminarRecordBatch::new(batch.clone()));
        unsafe { *out = Box::into_raw(handle) };
        LAMINAR_OK
    } else {
        unsafe { *out = ptr::null_mut() };
        LAMINAR_ERR_NULL_POINTER
    }
}

/// Free a query result handle.
///
/// # Safety
///
/// `result` must be a valid handle from a laminar function, or NULL.
#[no_mangle]
pub unsafe extern "C" fn laminar_result_free(result: *mut LaminarQueryResult) {
    if !result.is_null() {
        drop(unsafe { Box::from_raw(result) });
    }
}

/// Get the schema from a query stream.
///
/// # Safety
///
/// `stream` and `out` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn laminar_stream_schema(
    stream: *mut LaminarQueryStream,
    out: *mut *mut LaminarSchema,
) -> i32 {
    clear_last_error();

    if stream.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let schema = unsafe { (*stream).inner.schema() };
    let handle = Box::new(LaminarSchema::new(schema));

    unsafe { *out = Box::into_raw(handle) };
    LAMINAR_OK
}

/// Get the next batch from a query stream (blocking). Sets `*out` to NULL when exhausted.
///
/// # Safety
///
/// `stream` and `out` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn laminar_stream_next(
    stream: *mut LaminarQueryStream,
    out: *mut *mut LaminarRecordBatch,
) -> i32 {
    clear_last_error();

    if stream.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let stream_ref = unsafe { &mut (*stream).inner };

    match stream_ref.next() {
        Ok(Some(batch)) => {
            let handle = Box::new(LaminarRecordBatch::new(batch));
            unsafe { *out = Box::into_raw(handle) };
            LAMINAR_OK
        }
        Ok(None) => {
            unsafe { *out = ptr::null_mut() };
            LAMINAR_OK
        }
        Err(e) => {
            unsafe { *out = ptr::null_mut() };
            let code = e.code();
            set_last_error(e);
            code
        }
    }
}

/// Try to get the next batch from a query stream (non-blocking). Sets `*out` to NULL if none available.
///
/// # Safety
///
/// `stream` and `out` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn laminar_stream_try_next(
    stream: *mut LaminarQueryStream,
    out: *mut *mut LaminarRecordBatch,
) -> i32 {
    clear_last_error();

    if stream.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let stream_ref = unsafe { &mut (*stream).inner };

    match stream_ref.try_next() {
        Ok(Some(batch)) => {
            let handle = Box::new(LaminarRecordBatch::new(batch));
            unsafe { *out = Box::into_raw(handle) };
            LAMINAR_OK
        }
        Ok(None) => {
            unsafe { *out = ptr::null_mut() };
            LAMINAR_OK
        }
        Err(e) => {
            unsafe { *out = ptr::null_mut() };
            let code = e.code();
            set_last_error(e);
            code
        }
    }
}

/// Check if a query stream is still active.
///
/// # Safety
///
/// `stream` and `out` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn laminar_stream_is_active(
    stream: *mut LaminarQueryStream,
    out: *mut bool,
) -> i32 {
    clear_last_error();

    if stream.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    unsafe {
        *out = (*stream).inner.is_active();
    }
    LAMINAR_OK
}

/// Cancel a query stream.
///
/// # Safety
///
/// `stream` must be a valid query stream handle.
#[no_mangle]
pub unsafe extern "C" fn laminar_stream_cancel(stream: *mut LaminarQueryStream) -> i32 {
    clear_last_error();

    if stream.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    unsafe {
        (*stream).inner.cancel();
    }
    LAMINAR_OK
}

/// Free a query stream handle.
///
/// # Safety
///
/// `stream` must be a valid handle from a laminar function, or NULL.
#[no_mangle]
pub unsafe extern "C" fn laminar_stream_free(stream: *mut LaminarQueryStream) {
    if !stream.is_null() {
        drop(unsafe { Box::from_raw(stream) });
    }
}

/// Get the number of rows in a record batch.
///
/// # Safety
///
/// `batch` and `out` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn laminar_batch_num_rows(
    batch: *mut LaminarRecordBatch,
    out: *mut usize,
) -> i32 {
    clear_last_error();

    if batch.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    unsafe {
        *out = (*batch).inner.num_rows();
    }
    LAMINAR_OK
}

/// Get the number of columns in a record batch.
///
/// # Safety
///
/// `batch` and `out` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn laminar_batch_num_columns(
    batch: *mut LaminarRecordBatch,
    out: *mut usize,
) -> i32 {
    clear_last_error();

    if batch.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    unsafe {
        *out = (*batch).inner.num_columns();
    }
    LAMINAR_OK
}

/// Free a record batch handle.
///
/// # Safety
///
/// `batch` must be a valid handle from a laminar function, or NULL.
#[no_mangle]
pub unsafe extern "C" fn laminar_batch_free(batch: *mut LaminarRecordBatch) {
    if !batch.is_null() {
        drop(unsafe { Box::from_raw(batch) });
    }
}

#[cfg(test)]
#[allow(clippy::borrow_as_ptr)]
mod tests {
    use super::*;
    use crate::ffi::connection::{laminar_close, laminar_open, laminar_query};
    use crate::ffi::schema::laminar_schema_free;

    #[test]
    fn test_result_schema() {
        let mut conn: *mut super::super::connection::LaminarConnection = ptr::null_mut();
        let mut result: *mut LaminarQueryResult = ptr::null_mut();
        let mut schema: *mut LaminarSchema = ptr::null_mut();

        // SAFETY: Test code with valid pointers
        unsafe {
            laminar_open(&mut conn);

            // Create table (not source) for point-in-time queries
            let create_sql = b"CREATE TABLE query_test (id BIGINT, val DOUBLE)\0";
            crate::ffi::connection::laminar_execute(
                conn,
                create_sql.as_ptr().cast(),
                ptr::null_mut(),
            );

            let query_sql = b"SELECT * FROM query_test\0";
            let rc = laminar_query(conn, query_sql.as_ptr().cast(), &mut result);
            assert_eq!(rc, LAMINAR_OK);

            // Get schema
            let rc = laminar_result_schema(result, &mut schema);
            assert_eq!(rc, LAMINAR_OK);
            assert!(!schema.is_null());

            laminar_schema_free(schema);
            laminar_result_free(result);
            laminar_close(conn);
        }
    }

    #[test]
    fn test_result_counts() {
        let mut conn: *mut super::super::connection::LaminarConnection = ptr::null_mut();
        let mut result: *mut LaminarQueryResult = ptr::null_mut();

        // SAFETY: Test code with valid pointers
        unsafe {
            laminar_open(&mut conn);

            // Create table (not source) for point-in-time queries
            let create_sql = b"CREATE TABLE count_test (id BIGINT)\0";
            crate::ffi::connection::laminar_execute(
                conn,
                create_sql.as_ptr().cast(),
                ptr::null_mut(),
            );

            let query_sql = b"SELECT * FROM count_test\0";
            laminar_query(conn, query_sql.as_ptr().cast(), &mut result);

            let mut num_rows: usize = 999;
            let rc = laminar_result_num_rows(result, &mut num_rows);
            assert_eq!(rc, LAMINAR_OK);
            assert_eq!(num_rows, 0); // Empty table

            let mut num_batches: usize = 999;
            let rc = laminar_result_num_batches(result, &mut num_batches);
            assert_eq!(rc, LAMINAR_OK);

            laminar_result_free(result);
            laminar_close(conn);
        }
    }

    #[test]
    fn test_batch_free_null() {
        // SAFETY: Testing null handling
        unsafe {
            laminar_batch_free(ptr::null_mut());
        }
        // Should not crash
    }
}
