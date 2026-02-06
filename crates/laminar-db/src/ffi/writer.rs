//! FFI writer functions.
//!
//! Provides `extern "C"` wrappers for data ingestion operations.

use std::ffi::{c_char, CStr};

use crate::api::Writer;

use super::connection::LaminarConnection;
use super::error::{
    clear_last_error, set_last_error, LAMINAR_ERR_INVALID_UTF8, LAMINAR_ERR_NULL_POINTER,
    LAMINAR_OK,
};
use super::query::LaminarRecordBatch;

/// Opaque writer handle for FFI.
#[repr(C)]
pub struct LaminarWriter {
    pub(super) inner: Option<Writer>,
}

impl LaminarWriter {
    /// Create from Writer.
    fn new(writer: Writer) -> Self {
        Self {
            inner: Some(writer),
        }
    }
}

/// Create a writer for a source.
///
/// # Arguments
///
/// * `conn` - Database connection
/// * `source_name` - Null-terminated source name
/// * `out` - Pointer to receive writer handle
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// * `conn` must be a valid connection handle
/// * `source_name` must be a valid null-terminated UTF-8 string
/// * `out` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn laminar_writer_create(
    conn: *mut LaminarConnection,
    source_name: *const c_char,
    out: *mut *mut LaminarWriter,
) -> i32 {
    clear_last_error();

    if conn.is_null() || source_name.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: source_name is non-null (checked above)
    let Ok(name_str) = (unsafe { CStr::from_ptr(source_name) }).to_str() else {
        return LAMINAR_ERR_INVALID_UTF8;
    };

    // SAFETY: conn is non-null (checked above)
    let conn_ref = unsafe { &(*conn).inner };

    match conn_ref.writer(name_str) {
        Ok(writer) => {
            let handle = Box::new(LaminarWriter::new(writer));
            // SAFETY: out is non-null (checked above)
            unsafe { *out = Box::into_raw(handle) };
            LAMINAR_OK
        }
        Err(e) => {
            let code = e.code();
            set_last_error(e);
            code
        }
    }
}

/// Write a record batch to the source.
///
/// # Arguments
///
/// * `writer` - Writer handle
/// * `batch` - Record batch to write (ownership is transferred)
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// * `writer` must be a valid writer handle
/// * `batch` must be a valid record batch handle (will be consumed)
#[no_mangle]
pub unsafe extern "C" fn laminar_writer_write(
    writer: *mut LaminarWriter,
    batch: *mut LaminarRecordBatch,
) -> i32 {
    clear_last_error();

    if writer.is_null() || batch.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: batch is non-null (checked above), take ownership
    let batch_box = unsafe { Box::from_raw(batch) };
    let record_batch = batch_box.into_inner();

    // SAFETY: writer is non-null (checked above)
    let writer_ref = unsafe { &mut (*writer).inner };

    if let Some(w) = writer_ref.as_mut() {
        match w.write(record_batch) {
            Ok(()) => LAMINAR_OK,
            Err(e) => {
                let code = e.code();
                set_last_error(e);
                code
            }
        }
    } else {
        set_last_error(crate::api::ApiError::internal("Writer already closed"));
        LAMINAR_ERR_NULL_POINTER
    }
}

/// Flush buffered data.
///
/// # Arguments
///
/// * `writer` - Writer handle
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// `writer` must be a valid writer handle.
#[no_mangle]
pub unsafe extern "C" fn laminar_writer_flush(writer: *mut LaminarWriter) -> i32 {
    clear_last_error();

    if writer.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: writer is non-null (checked above)
    let writer_ref = unsafe { &mut (*writer).inner };

    if let Some(w) = writer_ref.as_mut() {
        match w.flush() {
            Ok(()) => LAMINAR_OK,
            Err(e) => {
                let code = e.code();
                set_last_error(e);
                code
            }
        }
    } else {
        set_last_error(crate::api::ApiError::internal("Writer already closed"));
        LAMINAR_ERR_NULL_POINTER
    }
}

/// Close the writer and release resources.
///
/// After calling this, the writer handle can still be freed with `laminar_writer_free`,
/// but no more writes are allowed.
///
/// # Arguments
///
/// * `writer` - Writer handle
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// `writer` must be a valid writer handle.
#[no_mangle]
pub unsafe extern "C" fn laminar_writer_close(writer: *mut LaminarWriter) -> i32 {
    clear_last_error();

    if writer.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: writer is non-null (checked above)
    let writer_ref = unsafe { &mut (*writer).inner };

    match writer_ref.take() {
        Some(w) => match w.close() {
            Ok(()) => LAMINAR_OK,
            Err(e) => {
                let code = e.code();
                set_last_error(e);
                code
            }
        },
        None => {
            // Already closed, that's fine
            LAMINAR_OK
        }
    }
}

/// Free a writer handle.
///
/// # Arguments
///
/// * `writer` - Writer handle to free
///
/// # Safety
///
/// `writer` must be a valid handle from `laminar_writer_create`, or NULL.
#[no_mangle]
pub unsafe extern "C" fn laminar_writer_free(writer: *mut LaminarWriter) {
    if !writer.is_null() {
        // SAFETY: writer is non-null and was allocated by Box
        drop(unsafe { Box::from_raw(writer) });
    }
}

#[cfg(test)]
#[allow(clippy::borrow_as_ptr)]
mod tests {
    use super::*;
    use crate::ffi::connection::{laminar_close, laminar_execute, laminar_open};
    use std::ptr;

    #[test]
    fn test_writer_create() {
        let mut conn: *mut LaminarConnection = ptr::null_mut();
        let mut writer: *mut LaminarWriter = ptr::null_mut();

        // SAFETY: Test code with valid pointers
        unsafe {
            laminar_open(&mut conn);

            // Create source
            let sql = b"CREATE SOURCE writer_test (id BIGINT)\0";
            laminar_execute(conn, sql.as_ptr().cast(), ptr::null_mut());

            // Create writer
            let name = b"writer_test\0";
            let rc = laminar_writer_create(conn, name.as_ptr().cast(), &mut writer);
            assert_eq!(rc, LAMINAR_OK);
            assert!(!writer.is_null());

            // Close writer
            let rc = laminar_writer_close(writer);
            assert_eq!(rc, LAMINAR_OK);

            laminar_writer_free(writer);
            laminar_close(conn);
        }
    }

    #[test]
    fn test_writer_null_pointer() {
        // SAFETY: Testing null pointer handling
        let rc = unsafe { laminar_writer_flush(ptr::null_mut()) };
        assert_eq!(rc, LAMINAR_ERR_NULL_POINTER);
    }

    #[test]
    fn test_writer_source_not_found() {
        let mut conn: *mut LaminarConnection = ptr::null_mut();
        let mut writer: *mut LaminarWriter = ptr::null_mut();

        // SAFETY: Test code with valid pointers
        unsafe {
            laminar_open(&mut conn);

            let name = b"nonexistent_source\0";
            let rc = laminar_writer_create(conn, name.as_ptr().cast(), &mut writer);
            assert_ne!(rc, LAMINAR_OK);
            assert!(writer.is_null() || rc != LAMINAR_OK);

            laminar_close(conn);
        }
    }
}
