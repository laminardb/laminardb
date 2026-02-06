# F-FFI-002: C Header Generation

## Feature Specification v1.0

**Target Phase:** Phase 3 (Connectors & Integration)
**Priority:** P0
**Estimated Complexity:** Medium (2-3 days)
**Prerequisites:** F-FFI-001 (API Module)

---

## Executive Summary

This specification defines the C FFI layer for LaminarDB: `extern "C"` functions with `#[no_mangle]` that expose the F-FFI-001 API module to C and any language with C FFI support (Python via ctypes/cffi, Java via JNI/JNA, Node.js via node-ffi, .NET via P/Invoke).

The design uses opaque handles (pointers to boxed Rust types), explicit memory management (`*_free` functions), and error codes with out-parameters. Headers are generated automatically via `cbindgen`.

---

## 1. Design Principles

1. **Opaque handles** - C sees pointers, not struct layouts
2. **Explicit memory management** - Caller frees with `*_free()` functions
3. **Error codes** - All functions return `i32` (0 = success, negative = error)
4. **Out-parameters** - Results returned via pointer arguments
5. **Null safety** - All functions check for null pointers
6. **Thread safety** - All handles are Send + Sync (from F-FFI-001)

---

## 2. C API Design

### 2.1 Handle Types

```c
// Opaque handle types
typedef struct LaminarConnection LaminarConnection;
typedef struct LaminarWriter LaminarWriter;
typedef struct LaminarQueryResult LaminarQueryResult;
typedef struct LaminarQueryStream LaminarQueryStream;
typedef struct LaminarSubscription LaminarSubscription;
typedef struct LaminarSchema LaminarSchema;
typedef struct LaminarRecordBatch LaminarRecordBatch;
```

### 2.2 Error Handling

```c
// Error codes (matching F-FFI-001 ApiError codes)
#define LAMINAR_OK 0
#define LAMINAR_ERR_NULL_POINTER -1
#define LAMINAR_ERR_INVALID_UTF8 -2
#define LAMINAR_ERR_CONNECTION 100
#define LAMINAR_ERR_TABLE_NOT_FOUND 200
#define LAMINAR_ERR_TABLE_EXISTS 201
#define LAMINAR_ERR_SCHEMA_MISMATCH 202
#define LAMINAR_ERR_INGESTION 300
#define LAMINAR_ERR_QUERY 400
#define LAMINAR_ERR_SUBSCRIPTION 500
#define LAMINAR_ERR_INTERNAL 900
#define LAMINAR_ERR_SHUTDOWN 901

// Get last error message (thread-local)
const char* laminar_last_error(void);
int32_t laminar_last_error_code(void);
```

### 2.3 Connection Functions

```c
// Open database
int32_t laminar_open(LaminarConnection** out);
int32_t laminar_close(LaminarConnection* conn);

// Execute SQL
int32_t laminar_execute(
    LaminarConnection* conn,
    const char* sql,
    LaminarQueryResult** out  // May be NULL for DDL
);

// Query with materialized results
int32_t laminar_query(
    LaminarConnection* conn,
    const char* sql,
    LaminarQueryResult** out
);

// Query with streaming results
int32_t laminar_query_stream(
    LaminarConnection* conn,
    const char* sql,
    LaminarQueryStream** out
);

// Start pipeline
int32_t laminar_start(LaminarConnection* conn);

// Check if closed
int32_t laminar_is_closed(LaminarConnection* conn, bool* out);
```

### 2.4 Schema Functions

```c
// Get schema for a source
int32_t laminar_get_schema(
    LaminarConnection* conn,
    const char* name,
    LaminarSchema** out
);

// List sources (returns JSON array)
int32_t laminar_list_sources(
    LaminarConnection* conn,
    char** out  // Caller frees with laminar_string_free
);

// Schema inspection
int32_t laminar_schema_num_fields(LaminarSchema* schema, size_t* out);
int32_t laminar_schema_field_name(LaminarSchema* schema, size_t index, char** out);
int32_t laminar_schema_field_type(LaminarSchema* schema, size_t index, char** out);
void laminar_schema_free(LaminarSchema* schema);
```

### 2.5 Writer Functions

```c
// Create writer for a source
int32_t laminar_writer_create(
    LaminarConnection* conn,
    const char* source_name,
    LaminarWriter** out
);

// Write a record batch
int32_t laminar_writer_write(
    LaminarWriter* writer,
    LaminarRecordBatch* batch
);

// Flush and close
int32_t laminar_writer_flush(LaminarWriter* writer);
int32_t laminar_writer_close(LaminarWriter* writer);
void laminar_writer_free(LaminarWriter* writer);
```

### 2.6 Query Result Functions

```c
// Get schema
int32_t laminar_result_schema(LaminarQueryResult* result, LaminarSchema** out);

// Get row count
int32_t laminar_result_num_rows(LaminarQueryResult* result, size_t* out);

// Get batch count
int32_t laminar_result_num_batches(LaminarQueryResult* result, size_t* out);

// Get batch by index
int32_t laminar_result_get_batch(
    LaminarQueryResult* result,
    size_t index,
    LaminarRecordBatch** out
);

void laminar_result_free(LaminarQueryResult* result);
```

### 2.7 Query Stream Functions

```c
// Get schema
int32_t laminar_stream_schema(LaminarQueryStream* stream, LaminarSchema** out);

// Get next batch (blocking)
int32_t laminar_stream_next(
    LaminarQueryStream* stream,
    LaminarRecordBatch** out  // NULL when exhausted
);

// Try get next batch (non-blocking)
int32_t laminar_stream_try_next(
    LaminarQueryStream* stream,
    LaminarRecordBatch** out  // NULL if none available
);

// Check if active
int32_t laminar_stream_is_active(LaminarQueryStream* stream, bool* out);

// Cancel
int32_t laminar_stream_cancel(LaminarQueryStream* stream);

void laminar_stream_free(LaminarQueryStream* stream);
```

### 2.8 Memory Management

```c
// Free string returned by laminar functions
void laminar_string_free(char* s);

// Free record batch
void laminar_batch_free(LaminarRecordBatch* batch);
```

---

## 3. Implementation

### 3.1 Module Structure

```
laminar-db/src/
├── api/           # F-FFI-001 Rust API
├── ffi/           # NEW: C FFI layer
│   ├── mod.rs     # Module root, error handling
│   ├── connection.rs
│   ├── schema.rs
│   ├── writer.rs
│   ├── query.rs
│   └── memory.rs
└── lib.rs
```

### 3.2 Error Handling Pattern

```rust
// Thread-local last error storage
thread_local! {
    static LAST_ERROR: RefCell<Option<ApiError>> = RefCell::new(None);
}

fn set_last_error(err: ApiError) {
    LAST_ERROR.with(|e| *e.borrow_mut() = Some(err));
}

fn clear_last_error() {
    LAST_ERROR.with(|e| *e.borrow_mut() = None);
}

/// Get last error message.
#[no_mangle]
pub extern "C" fn laminar_last_error() -> *const c_char {
    LAST_ERROR.with(|e| {
        match &*e.borrow() {
            Some(err) => {
                // Return static or leaked string
            }
            None => std::ptr::null(),
        }
    })
}
```

### 3.3 Handle Pattern

```rust
/// Opaque connection handle.
pub struct LaminarConnection {
    inner: Connection,
}

#[no_mangle]
pub extern "C" fn laminar_open(out: *mut *mut LaminarConnection) -> i32 {
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
            set_last_error(e);
            e.code()
        }
    }
}

#[no_mangle]
pub extern "C" fn laminar_close(conn: *mut LaminarConnection) -> i32 {
    if conn.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let handle = unsafe { Box::from_raw(conn) };
    match handle.inner.close() {
        Ok(()) => LAMINAR_OK,
        Err(e) => {
            set_last_error(e);
            e.code()
        }
    }
}
```

---

## 4. cbindgen Configuration

### 4.1 cbindgen.toml

```toml
language = "C"
header = "/* LaminarDB C API - Auto-generated by cbindgen */"
include_guard = "LAMINAR_H"
no_includes = true
sys_includes = ["stdint.h", "stdbool.h", "stddef.h"]

[defines]
"feature = ffi" = "LAMINAR_FFI"

[export]
prefix = "Laminar"
include = ["LaminarConnection", "LaminarWriter", "LaminarQueryResult", "LaminarQueryStream", "LaminarSchema", "LaminarRecordBatch"]

[fn]
prefix = "laminar_"

[enum]
prefix_with_name = true
```

### 4.2 Build Script

```rust
// build.rs
fn main() {
    #[cfg(feature = "ffi")]
    {
        let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        cbindgen::generate(&crate_dir)
            .expect("Failed to generate C bindings")
            .write_to_file("include/laminar.h");
    }
}
```

---

## 5. Success Criteria

| Criterion | Validation |
|-----------|------------|
| All FFI functions are `#[no_mangle] extern "C"` | Code review |
| All handles use opaque pointers | cbindgen output |
| All functions return error codes | API inspection |
| Header generates without errors | `cargo build --features ffi` |
| No memory leaks in handle lifecycle | Valgrind/ASAN testing |
| Thread-local error storage works | Unit tests |

---

## 6. Testing

```c
// test_ffi.c
#include "laminar.h"
#include <assert.h>
#include <stdio.h>

int main() {
    LaminarConnection* conn = NULL;
    int32_t rc;

    // Open
    rc = laminar_open(&conn);
    assert(rc == LAMINAR_OK);
    assert(conn != NULL);

    // Execute DDL
    rc = laminar_execute(conn, "CREATE SOURCE test (id BIGINT)", NULL);
    assert(rc == LAMINAR_OK);

    // List sources
    char* sources = NULL;
    rc = laminar_list_sources(conn, &sources);
    assert(rc == LAMINAR_OK);
    printf("Sources: %s\n", sources);
    laminar_string_free(sources);

    // Close
    rc = laminar_close(conn);
    assert(rc == LAMINAR_OK);

    printf("All tests passed!\n");
    return 0;
}
```

