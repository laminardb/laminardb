//! FFI schema inspection functions.
//!
//! Provides `extern "C"` wrappers for schema inspection operations.

use std::ffi::{c_char, CStr, CString};

use arrow::datatypes::SchemaRef;

use super::connection::LaminarConnection;
use super::error::{
    clear_last_error, set_last_error, LAMINAR_ERR_INVALID_UTF8, LAMINAR_ERR_NULL_POINTER,
    LAMINAR_OK,
};
use super::memory::take_ownership_string;

/// Opaque schema handle for FFI.
#[repr(C)]
pub struct LaminarSchema {
    inner: SchemaRef,
}

impl LaminarSchema {
    /// Create from Arrow schema.
    pub(crate) fn new(schema: SchemaRef) -> Self {
        Self { inner: schema }
    }

    /// Get inner schema reference.
    #[allow(dead_code)] // Used for Arrow C Data Interface export
    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.inner
    }
}

/// Get schema for a source.
///
/// # Arguments
///
/// * `conn` - Database connection
/// * `name` - Null-terminated source name
/// * `out` - Pointer to receive schema handle
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// * `conn` must be a valid connection handle
/// * `name` must be a valid null-terminated UTF-8 string
/// * `out` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn laminar_get_schema(
    conn: *mut LaminarConnection,
    name: *const c_char,
    out: *mut *mut LaminarSchema,
) -> i32 {
    clear_last_error();

    if conn.is_null() || name.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: name is non-null (checked above)
    let Ok(name_str) = (unsafe { CStr::from_ptr(name) }).to_str() else {
        return LAMINAR_ERR_INVALID_UTF8;
    };

    // SAFETY: conn is non-null (checked above)
    let conn_ref = unsafe { &(*conn).inner };

    match conn_ref.get_schema(name_str) {
        Ok(schema) => {
            let handle = Box::new(LaminarSchema::new(schema));
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

/// List all sources as JSON array.
///
/// Returns a JSON array like `["source1", "source2"]`.
///
/// # Arguments
///
/// * `conn` - Database connection
/// * `out` - Pointer to receive JSON string (caller frees with `laminar_string_free`)
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// * `conn` must be a valid connection handle
/// * `out` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn laminar_list_sources(
    conn: *mut LaminarConnection,
    out: *mut *mut c_char,
) -> i32 {
    clear_last_error();

    if conn.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: conn is non-null (checked above)
    let conn_ref = unsafe { &(*conn).inner };

    let sources = conn_ref.list_sources();
    let json = format!(
        "[{}]",
        sources
            .iter()
            .map(|s| format!("\"{s}\""))
            .collect::<Vec<_>>()
            .join(", ")
    );

    match CString::new(json) {
        Ok(c_str) => {
            // SAFETY: out is non-null (checked above)
            unsafe { *out = take_ownership_string(c_str) };
            LAMINAR_OK
        }
        Err(_) => LAMINAR_ERR_INVALID_UTF8,
    }
}

/// Get the number of fields in a schema.
///
/// # Arguments
///
/// * `schema` - Schema handle
/// * `out` - Pointer to receive field count
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// * `schema` must be a valid schema handle
/// * `out` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn laminar_schema_num_fields(
    schema: *mut LaminarSchema,
    out: *mut usize,
) -> i32 {
    clear_last_error();

    if schema.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: schema and out are non-null (checked above)
    unsafe {
        *out = (*schema).inner.fields().len();
    }
    LAMINAR_OK
}

/// Get the name of a field by index.
///
/// # Arguments
///
/// * `schema` - Schema handle
/// * `index` - Field index (0-based)
/// * `out` - Pointer to receive field name (caller frees with `laminar_string_free`)
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// * `schema` must be a valid schema handle
/// * `index` must be less than the number of fields
/// * `out` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn laminar_schema_field_name(
    schema: *mut LaminarSchema,
    index: usize,
    out: *mut *mut c_char,
) -> i32 {
    clear_last_error();

    if schema.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: schema is non-null (checked above)
    let schema_ref = unsafe { &(*schema).inner };

    if index >= schema_ref.fields().len() {
        return LAMINAR_ERR_NULL_POINTER; // Index out of bounds
    }

    let name = schema_ref.field(index).name();
    match CString::new(name.as_str()) {
        Ok(c_str) => {
            // SAFETY: out is non-null (checked above)
            unsafe { *out = take_ownership_string(c_str) };
            LAMINAR_OK
        }
        Err(_) => LAMINAR_ERR_INVALID_UTF8,
    }
}

/// Get the type of a field by index.
///
/// Returns the Arrow data type as a string (e.g., "Int64", "Utf8", "Float64").
///
/// # Arguments
///
/// * `schema` - Schema handle
/// * `index` - Field index (0-based)
/// * `out` - Pointer to receive type name (caller frees with `laminar_string_free`)
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// * `schema` must be a valid schema handle
/// * `index` must be less than the number of fields
/// * `out` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn laminar_schema_field_type(
    schema: *mut LaminarSchema,
    index: usize,
    out: *mut *mut c_char,
) -> i32 {
    clear_last_error();

    if schema.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: schema is non-null (checked above)
    let schema_ref = unsafe { &(*schema).inner };

    if index >= schema_ref.fields().len() {
        return LAMINAR_ERR_NULL_POINTER; // Index out of bounds
    }

    let data_type = schema_ref.field(index).data_type();
    let type_str = format!("{data_type:?}");
    match CString::new(type_str) {
        Ok(c_str) => {
            // SAFETY: out is non-null (checked above)
            unsafe { *out = take_ownership_string(c_str) };
            LAMINAR_OK
        }
        Err(_) => LAMINAR_ERR_INVALID_UTF8,
    }
}

/// Free a schema handle.
///
/// # Arguments
///
/// * `schema` - Schema handle to free
///
/// # Safety
///
/// `schema` must be a valid handle from a laminar function, or NULL.
#[no_mangle]
pub unsafe extern "C" fn laminar_schema_free(schema: *mut LaminarSchema) {
    if !schema.is_null() {
        // SAFETY: schema is non-null and was allocated by Box
        drop(unsafe { Box::from_raw(schema) });
    }
}

#[cfg(test)]
#[allow(clippy::borrow_as_ptr)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::ffi::connection::laminar_open;
    use crate::ffi::memory::laminar_string_free;

    #[test]
    fn test_list_sources_empty() {
        let mut conn: *mut LaminarConnection = ptr::null_mut();
        let mut sources: *mut c_char = ptr::null_mut();

        // SAFETY: Test code with valid pointers
        unsafe {
            laminar_open(&mut conn);
            let rc = laminar_list_sources(conn, &mut sources);
            assert_eq!(rc, LAMINAR_OK);
            assert!(!sources.is_null());

            let sources_str = CStr::from_ptr(sources).to_str().unwrap();
            assert_eq!(sources_str, "[]");

            laminar_string_free(sources);
            crate::ffi::connection::laminar_close(conn);
        }
    }

    #[test]
    fn test_get_schema() {
        let mut conn: *mut LaminarConnection = ptr::null_mut();
        let mut schema: *mut LaminarSchema = ptr::null_mut();

        // SAFETY: Test code with valid pointers
        unsafe {
            laminar_open(&mut conn);

            // Create a source
            let sql = b"CREATE SOURCE schema_ffi_test (id BIGINT, name VARCHAR)\0";
            crate::ffi::connection::laminar_execute(conn, sql.as_ptr().cast(), ptr::null_mut());

            // Get schema
            let name = b"schema_ffi_test\0";
            let rc = laminar_get_schema(conn, name.as_ptr().cast(), &mut schema);
            assert_eq!(rc, LAMINAR_OK);
            assert!(!schema.is_null());

            // Check field count
            let mut num_fields: usize = 0;
            let rc = laminar_schema_num_fields(schema, &mut num_fields);
            assert_eq!(rc, LAMINAR_OK);
            assert_eq!(num_fields, 2);

            // Check field names
            let mut field_name: *mut c_char = ptr::null_mut();
            laminar_schema_field_name(schema, 0, &mut field_name);
            assert_eq!(CStr::from_ptr(field_name).to_str().unwrap(), "id");
            laminar_string_free(field_name);

            laminar_schema_field_name(schema, 1, &mut field_name);
            assert_eq!(CStr::from_ptr(field_name).to_str().unwrap(), "name");
            laminar_string_free(field_name);

            laminar_schema_free(schema);
            crate::ffi::connection::laminar_close(conn);
        }
    }

    #[test]
    fn test_schema_null_pointer() {
        let mut num_fields: usize = 0;
        // SAFETY: Testing null pointer handling
        let rc = unsafe { laminar_schema_num_fields(ptr::null_mut(), &mut num_fields) };
        assert_eq!(rc, LAMINAR_ERR_NULL_POINTER);
    }
}
