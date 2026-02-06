//! FFI memory management functions.
//!
//! Provides `extern "C"` functions for freeing memory allocated by `LaminarDB`.

use std::ffi::{c_char, CString};

/// Transfer ownership of a `CString` to a raw pointer.
///
/// The caller is responsible for freeing this with `laminar_string_free`.
pub(crate) fn take_ownership_string(s: CString) -> *mut c_char {
    s.into_raw()
}

/// Free a string allocated by `LaminarDB`.
///
/// # Arguments
///
/// * `s` - String pointer to free, or NULL
///
/// # Safety
///
/// `s` must be a pointer returned by a laminar function, or NULL.
/// After calling this function, the pointer is invalid.
#[no_mangle]
pub unsafe extern "C" fn laminar_string_free(s: *mut c_char) {
    if !s.is_null() {
        // SAFETY: s is non-null and was allocated by CString::into_raw
        drop(unsafe { CString::from_raw(s) });
    }
}

/// Get the `LaminarDB` library version.
///
/// # Returns
///
/// A pointer to a null-terminated version string. This is a static string
/// and should NOT be freed.
///
/// # Safety
///
/// The returned pointer is valid for the lifetime of the process.
#[no_mangle]
pub extern "C" fn laminar_version() -> *const c_char {
    // Static string, never freed
    static VERSION: &[u8] = concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes();
    VERSION.as_ptr().cast()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    #[test]
    fn test_string_free_null() {
        // Should not crash
        // SAFETY: Testing null handling
        unsafe {
            laminar_string_free(std::ptr::null_mut());
        }
    }

    #[test]
    fn test_string_round_trip() {
        let original = "Hello, FFI!";
        let c_string = CString::new(original).unwrap();
        let ptr = take_ownership_string(c_string);

        // SAFETY: ptr was just created
        let retrieved = unsafe { CStr::from_ptr(ptr).to_str().unwrap() };
        assert_eq!(retrieved, original);

        // SAFETY: ptr is valid
        unsafe { laminar_string_free(ptr) };
    }

    #[test]
    fn test_version() {
        let ptr = laminar_version();
        assert!(!ptr.is_null());

        // SAFETY: ptr points to a static string
        let version = unsafe { CStr::from_ptr(ptr).to_str().unwrap() };
        assert!(!version.is_empty());
        // Should contain version number pattern
        assert!(version.chars().any(|c| c.is_ascii_digit()));
    }
}
