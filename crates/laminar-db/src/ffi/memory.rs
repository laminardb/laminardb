//! FFI memory management functions.

use std::ffi::{c_char, CString};

/// Transfer ownership of a `CString` to a raw pointer; free with `laminar_string_free`.
pub(crate) fn take_ownership_string(s: CString) -> *mut c_char {
    s.into_raw()
}

/// Free a string allocated by a laminar function.
///
/// # Safety
///
/// `s` must be a pointer returned by a laminar function, or NULL.
#[no_mangle]
pub unsafe extern "C" fn laminar_string_free(s: *mut c_char) {
    if !s.is_null() {
        drop(unsafe { CString::from_raw(s) });
    }
}

/// Return the library version as a static null-terminated string (do not free).
///
/// # Safety
///
/// The returned pointer is valid for the lifetime of the process.
#[no_mangle]
pub extern "C" fn laminar_version() -> *const c_char {
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
