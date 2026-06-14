//! FFI connection functions.

use std::ffi::{c_char, CStr};
use std::ptr;

use crate::api::Connection;

use super::error::{
    clear_last_error, set_last_error, LAMINAR_ERR_INVALID_UTF8, LAMINAR_ERR_NULL_POINTER,
    LAMINAR_OK,
};
use super::query::{LaminarQueryResult, LaminarQueryStream};

/// Opaque connection handle. Create with `laminar_open`, free with `laminar_close`.
#[repr(C)]
pub struct LaminarConnection {
    pub(crate) inner: Connection,
}

/// Open a new database connection.
///
/// # Safety
///
/// `out` must be a valid pointer to a `*mut LaminarConnection`.
#[no_mangle]
pub unsafe extern "C" fn laminar_open(out: *mut *mut LaminarConnection) -> i32 {
    clear_last_error();

    if out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    match Connection::open() {
        Ok(conn) => {
            let handle = Box::new(LaminarConnection { inner: conn });
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

/// Close a database connection and free all associated resources.
///
/// # Safety
///
/// `conn` must be a valid handle from `laminar_open` that has not been closed.
#[no_mangle]
pub unsafe extern "C" fn laminar_close(conn: *mut LaminarConnection) -> i32 {
    clear_last_error();

    if conn.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let handle = unsafe { Box::from_raw(conn) };
    match handle.inner.close() {
        Ok(()) => LAMINAR_OK,
        Err(e) => {
            let code = e.code();
            set_last_error(e);
            code
        }
    }
}

/// Execute a SQL statement. For DDL, `out` may be NULL.
///
/// # Safety
///
/// `conn` and `sql` must be valid non-null pointers; `sql` must be null-terminated UTF-8;
/// if `out` is non-null it must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn laminar_execute(
    conn: *mut LaminarConnection,
    sql: *const c_char,
    out: *mut *mut LaminarQueryResult,
) -> i32 {
    clear_last_error();

    if conn.is_null() || sql.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let Ok(sql_str) = (unsafe { CStr::from_ptr(sql) }).to_str() else {
        return LAMINAR_ERR_INVALID_UTF8;
    };

    let conn_ref = unsafe { &(*conn).inner };

    match conn_ref.execute(sql_str) {
        Ok(result) => {
            use crate::api::ExecuteResult;
            match result {
                ExecuteResult::Query(stream) => match stream.collect() {
                    Ok(query_result) => {
                        if !out.is_null() {
                            let handle = Box::new(LaminarQueryResult::new(query_result));
                            unsafe { *out = Box::into_raw(handle) };
                        }
                        LAMINAR_OK
                    }
                    Err(e) => {
                        let code = e.code();
                        set_last_error(e);
                        code
                    }
                },
                ExecuteResult::Metadata(batch) => {
                    if !out.is_null() {
                        let query_result = crate::api::QueryResult::from_batch(batch);
                        let handle = Box::new(LaminarQueryResult::new(query_result));
                        unsafe { *out = Box::into_raw(handle) };
                    }
                    LAMINAR_OK
                }
                ExecuteResult::Ddl(_) | ExecuteResult::RowsAffected(_) => {
                    if !out.is_null() {
                        unsafe { *out = ptr::null_mut() };
                    }
                    LAMINAR_OK
                }
            }
        }
        Err(e) => {
            let code = e.code();
            set_last_error(e);
            code
        }
    }
}

/// Execute a query and collect all results into memory.
///
/// # Safety
///
/// `conn`, `sql`, and `out` must be valid non-null pointers; `sql` must be null-terminated UTF-8.
#[no_mangle]
pub unsafe extern "C" fn laminar_query(
    conn: *mut LaminarConnection,
    sql: *const c_char,
    out: *mut *mut LaminarQueryResult,
) -> i32 {
    clear_last_error();

    if conn.is_null() || sql.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let Ok(sql_str) = (unsafe { CStr::from_ptr(sql) }).to_str() else {
        return LAMINAR_ERR_INVALID_UTF8;
    };

    let conn_ref = unsafe { &(*conn).inner };

    match conn_ref.query(sql_str) {
        Ok(result) => {
            let handle = Box::new(LaminarQueryResult::new(result));
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

/// Execute a query with streaming results.
///
/// # Safety
///
/// `conn`, `sql`, and `out` must be valid non-null pointers; `sql` must be null-terminated UTF-8.
#[no_mangle]
pub unsafe extern "C" fn laminar_query_stream(
    conn: *mut LaminarConnection,
    sql: *const c_char,
    out: *mut *mut LaminarQueryStream,
) -> i32 {
    clear_last_error();

    if conn.is_null() || sql.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let Ok(sql_str) = (unsafe { CStr::from_ptr(sql) }).to_str() else {
        return LAMINAR_ERR_INVALID_UTF8;
    };

    let conn_ref = unsafe { &(*conn).inner };

    match conn_ref.query_stream(sql_str) {
        Ok(stream) => {
            let handle = Box::new(LaminarQueryStream::new(stream));
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

/// Start the streaming pipeline.
///
/// # Safety
///
/// `conn` must be a valid connection handle.
#[no_mangle]
pub unsafe extern "C" fn laminar_start(conn: *mut LaminarConnection) -> i32 {
    clear_last_error();

    if conn.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let conn_ref = unsafe { &(*conn).inner };

    match conn_ref.start() {
        Ok(()) => LAMINAR_OK,
        Err(e) => {
            let code = e.code();
            set_last_error(e);
            code
        }
    }
}

/// Check if the connection is closed.
///
/// # Safety
///
/// `conn` and `out` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn laminar_is_closed(conn: *mut LaminarConnection, out: *mut bool) -> i32 {
    clear_last_error();

    if conn.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    unsafe {
        *out = (*conn).inner.is_closed();
    }
    LAMINAR_OK
}

#[cfg(test)]
#[allow(clippy::borrow_as_ptr)]
mod tests {
    use super::*;

    #[test]
    fn test_open_close() {
        let mut conn: *mut LaminarConnection = ptr::null_mut();
        // SAFETY: Test code with valid pointer
        let rc = unsafe { laminar_open(&mut conn) };
        assert_eq!(rc, LAMINAR_OK);
        assert!(!conn.is_null());

        // SAFETY: conn is valid from laminar_open
        let rc = unsafe { laminar_close(conn) };
        assert_eq!(rc, LAMINAR_OK);
    }

    #[test]
    fn test_open_null_pointer() {
        // SAFETY: Testing null pointer handling
        let rc = unsafe { laminar_open(ptr::null_mut()) };
        assert_eq!(rc, LAMINAR_ERR_NULL_POINTER);
    }

    #[test]
    fn test_close_null_pointer() {
        // SAFETY: Testing null pointer handling
        let rc = unsafe { laminar_close(ptr::null_mut()) };
        assert_eq!(rc, LAMINAR_ERR_NULL_POINTER);
    }

    #[test]
    fn test_execute_create_source() {
        let mut conn: *mut LaminarConnection = ptr::null_mut();
        // SAFETY: Test code with valid pointers
        unsafe {
            let rc = laminar_open(&mut conn);
            assert_eq!(rc, LAMINAR_OK);

            let sql = b"CREATE SOURCE ffi_test (id BIGINT, name VARCHAR)\0";
            let rc = laminar_execute(conn, sql.as_ptr().cast(), ptr::null_mut());
            assert_eq!(rc, LAMINAR_OK);

            laminar_close(conn);
        }
    }

    #[test]
    fn test_is_closed() {
        let mut conn: *mut LaminarConnection = ptr::null_mut();
        let mut is_closed = true;

        // SAFETY: Test code with valid pointers
        unsafe {
            laminar_open(&mut conn);
            let rc = laminar_is_closed(conn, &mut is_closed);
            assert_eq!(rc, LAMINAR_OK);
            assert!(!is_closed);

            laminar_close(conn);
        }
    }
}
