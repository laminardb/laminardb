//! Thread-local error storage for FFI; callers retrieve the last message via `laminar_last_error`.

use std::cell::RefCell;
use std::ffi::{c_char, CString};
use std::ptr;

use crate::api::ApiError;

/// Success return code.
pub const LAMINAR_OK: i32 = 0;
/// Null pointer was passed to a function.
pub const LAMINAR_ERR_NULL_POINTER: i32 = -1;
/// Invalid UTF-8 string.
pub const LAMINAR_ERR_INVALID_UTF8: i32 = -2;
/// Connection error.
pub const LAMINAR_ERR_CONNECTION: i32 = 100;
/// Table/source not found.
pub const LAMINAR_ERR_TABLE_NOT_FOUND: i32 = 200;
/// Table/source already exists.
pub const LAMINAR_ERR_TABLE_EXISTS: i32 = 201;
/// Schema mismatch error.
pub const LAMINAR_ERR_SCHEMA_MISMATCH: i32 = 202;
/// Data ingestion error.
pub const LAMINAR_ERR_INGESTION: i32 = 300;
/// Query execution error.
pub const LAMINAR_ERR_QUERY: i32 = 400;
/// Subscription error.
pub const LAMINAR_ERR_SUBSCRIPTION: i32 = 500;
/// Internal error.
pub const LAMINAR_ERR_INTERNAL: i32 = 900;
/// Database is shutting down.
pub const LAMINAR_ERR_SHUTDOWN: i32 = 901;

thread_local! {
    static LAST_ERROR: RefCell<Option<StoredError>> = const { RefCell::new(None) };
}

struct StoredError {
    error: ApiError,
    c_message: CString,
}

pub(crate) fn set_last_error(err: ApiError) {
    let c_message = CString::new(err.message())
        .unwrap_or_else(|_| CString::new("Error message contained null byte").unwrap());
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = Some(StoredError {
            error: err,
            c_message,
        });
    });
}

pub(crate) fn clear_last_error() {
    LAST_ERROR.with(|e| *e.borrow_mut() = None);
}

#[must_use]
pub(crate) fn last_error_code() -> i32 {
    LAST_ERROR.with(|e| {
        e.borrow()
            .as_ref()
            .map_or(LAMINAR_OK, |stored| api_error_to_ffi_code(&stored.error))
    })
}

fn api_error_to_ffi_code(err: &ApiError) -> i32 {
    use crate::api::codes;
    let code = err.code();
    match code {
        codes::CONNECTION_FAILED => LAMINAR_ERR_CONNECTION,
        codes::TABLE_NOT_FOUND => LAMINAR_ERR_TABLE_NOT_FOUND,
        codes::TABLE_EXISTS => LAMINAR_ERR_TABLE_EXISTS,
        codes::SCHEMA_MISMATCH => LAMINAR_ERR_SCHEMA_MISMATCH,
        codes::INGESTION_FAILED => LAMINAR_ERR_INGESTION,
        codes::QUERY_FAILED => LAMINAR_ERR_QUERY,
        codes::SUBSCRIPTION_FAILED | codes::SUBSCRIPTION_CLOSED | codes::SUBSCRIPTION_TIMEOUT => {
            LAMINAR_ERR_SUBSCRIPTION
        }
        codes::SHUTDOWN => LAMINAR_ERR_SHUTDOWN,
        _ => LAMINAR_ERR_INTERNAL,
    }
}

/// Get the last error message as a null-terminated string, or null if no error.
///
/// # Safety
///
/// The returned pointer is valid until the next laminar_* call on this thread.
#[no_mangle]
pub extern "C" fn laminar_last_error() -> *const c_char {
    LAST_ERROR.with(|e| match &*e.borrow() {
        Some(stored) => stored.c_message.as_ptr(),
        None => ptr::null(),
    })
}

/// Get the last error code, or `LAMINAR_OK` if none.
#[no_mangle]
pub extern "C" fn laminar_last_error_code() -> i32 {
    last_error_code()
}

/// Clear the last error.
#[no_mangle]
pub extern "C" fn laminar_clear_error() {
    clear_last_error();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_error_returns_null() {
        clear_last_error();
        assert!(laminar_last_error().is_null());
        assert_eq!(laminar_last_error_code(), LAMINAR_OK);
    }

    #[test]
    fn test_set_and_get_error() {
        let err = ApiError::table_not_found("test_table");
        set_last_error(err);

        let code = laminar_last_error_code();
        assert_eq!(code, LAMINAR_ERR_TABLE_NOT_FOUND);

        let error_ptr = laminar_last_error();
        assert!(!error_ptr.is_null());

        // SAFETY: We just set this error, pointer is valid
        let error_cstr = unsafe { std::ffi::CStr::from_ptr(error_ptr) };
        let message = error_cstr.to_str().unwrap();
        assert!(message.contains("test_table"));
    }

    #[test]
    fn test_clear_error() {
        set_last_error(ApiError::internal("test"));
        assert!(!laminar_last_error().is_null());

        laminar_clear_error();
        assert!(laminar_last_error().is_null());
        assert_eq!(laminar_last_error_code(), LAMINAR_OK);
    }

    #[test]
    fn test_error_code_mapping() {
        // Connection error
        set_last_error(ApiError::connection("conn fail"));
        assert_eq!(laminar_last_error_code(), LAMINAR_ERR_CONNECTION);

        // Query error
        set_last_error(ApiError::Query {
            code: crate::api::codes::QUERY_FAILED,
            message: "query fail".into(),
        });
        assert_eq!(laminar_last_error_code(), LAMINAR_ERR_QUERY);

        // Shutdown
        set_last_error(ApiError::shutdown());
        assert_eq!(laminar_last_error_code(), LAMINAR_ERR_SHUTDOWN);
    }
}
