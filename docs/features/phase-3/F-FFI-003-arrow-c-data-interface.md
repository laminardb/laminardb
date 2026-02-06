# F-FFI-003: Arrow C Data Interface

## Feature Specification v1.0

**Target Phase:** Phase 3 (Connectors & Integration)
**Priority:** P0
**Estimated Complexity:** Medium (2-3 days)
**Prerequisites:** F-FFI-001 (API Module), F-FFI-002 (C Headers)

---

## Executive Summary

This specification defines zero-copy data exchange between LaminarDB and language bindings via the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html). This allows Python (via PyArrow), Java, Node.js, and other languages to receive query results without copying data.

---

## 1. Design Principles

1. **Zero-copy export** - Data buffers are shared, not copied
2. **Ownership transfer** - Consumer takes ownership and calls release callback
3. **Standard compliance** - Uses Arrow's official `FFI_ArrowArray` and `FFI_ArrowSchema`
4. **Memory safety** - Proper release callbacks prevent leaks

---

## 2. C API Design

### 2.1 Arrow C Data Interface Structures

These are defined by Arrow (not by us), but we reference them:

```c
// From Arrow C Data Interface specification
struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};
```

### 2.2 Export Functions

```c
// Export a record batch to Arrow C Data Interface
// The schema and array are exported separately (Arrow convention)
int32_t laminar_batch_export(
    LaminarRecordBatch* batch,
    struct ArrowArray* out_array,   // Caller-allocated, filled by function
    struct ArrowSchema* out_schema  // Caller-allocated, filled by function
);

// Export just the schema (useful for consumers that need schema first)
int32_t laminar_schema_export(
    LaminarSchema* schema,
    struct ArrowSchema* out_schema  // Caller-allocated, filled by function
);

// Export a single array/column from a batch
int32_t laminar_batch_export_column(
    LaminarRecordBatch* batch,
    size_t column_index,
    struct ArrowArray* out_array,
    struct ArrowSchema* out_schema
);
```

### 2.3 Import Functions

```c
// Import a record batch from Arrow C Data Interface
// Takes ownership of the ArrowArray (will call release)
int32_t laminar_batch_import(
    struct ArrowArray* array,       // Ownership transferred
    struct ArrowSchema* schema,     // Ownership transferred
    LaminarRecordBatch** out        // Receives new batch handle
);

// Create a record batch from imported data for writing
int32_t laminar_batch_create(
    struct ArrowArray* array,
    struct ArrowSchema* schema,
    LaminarRecordBatch** out
);
```

---

## 3. Implementation

### 3.1 Module Structure

```
laminar-db/src/ffi/
├── mod.rs
├── arrow_ffi.rs    # NEW: Arrow C Data Interface functions
└── ...
```

### 3.2 Export Implementation

```rust
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, to_ffi};

/// Export a RecordBatch to Arrow C Data Interface.
///
/// # Safety
///
/// * `batch` must be a valid batch handle
/// * `out_array` and `out_schema` must be valid pointers to uninitialized structs
/// * Caller must eventually call the release callbacks on both structs
#[no_mangle]
pub unsafe extern "C" fn laminar_batch_export(
    batch: *mut LaminarRecordBatch,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> i32 {
    if batch.is_null() || out_array.is_null() || out_schema.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let batch_ref = &(*batch).inner;

    // Convert RecordBatch to StructArray for export
    let struct_array: StructArray = batch_ref.clone().into();
    let data = struct_array.into_data();

    match to_ffi(&data) {
        Ok((array, schema)) => {
            std::ptr::write(out_array, array);
            std::ptr::write(out_schema, schema);
            LAMINAR_OK
        }
        Err(e) => {
            set_last_error(ApiError::internal(e.to_string()));
            LAMINAR_ERR_INTERNAL
        }
    }
}
```

### 3.3 Import Implementation

```rust
use arrow::ffi::from_ffi;

/// Import a RecordBatch from Arrow C Data Interface.
///
/// # Safety
///
/// * `array` and `schema` must be valid Arrow C Data Interface structs
/// * Ownership is transferred - this function will call release
/// * `out` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn laminar_batch_import(
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    out: *mut *mut LaminarRecordBatch,
) -> i32 {
    if array.is_null() || schema.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // Take ownership of the FFI structs
    let ffi_array = std::ptr::read(array);
    let ffi_schema = std::ptr::read(schema);

    match from_ffi(ffi_array, &ffi_schema) {
        Ok(data) => {
            // Convert ArrayData back to RecordBatch
            let struct_array = StructArray::from(data);
            match RecordBatch::from(struct_array) {
                batch => {
                    let handle = Box::new(LaminarRecordBatch::new(batch));
                    *out = Box::into_raw(handle);
                    LAMINAR_OK
                }
            }
        }
        Err(e) => {
            set_last_error(ApiError::internal(e.to_string()));
            LAMINAR_ERR_INTERNAL
        }
    }
}
```

---

## 4. Usage Examples

### 4.1 Python (PyArrow)

```python
import pyarrow as pa
from laminardb import Connection

conn = Connection()
conn.execute("CREATE TABLE test (id BIGINT, name VARCHAR)")
conn.execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")

result = conn.query("SELECT * FROM test")

# Zero-copy export to PyArrow
arrow_array = pa.Array._import_from_c(
    result._export_array_ptr(),
    result._export_schema_ptr()
)

# Now arrow_array is a PyArrow array with zero copies
df = arrow_array.to_pandas()
```

### 4.2 Node.js (apache-arrow)

```javascript
const { Connection } = require('laminardb');
const { tableFromIPC } = require('apache-arrow');

const conn = new Connection();
conn.execute("SELECT * FROM events");
const result = conn.query("SELECT * FROM events");

// Get Arrow C Data Interface pointers
const [arrayPtr, schemaPtr] = result.exportArrow();

// Import into JS Arrow
const table = tableFromIPC(arrayPtr, schemaPtr);
```

---

## 5. Success Criteria

| Criterion | Validation |
|-----------|------------|
| Export produces valid Arrow C Data Interface | Unit tests with re-import |
| Import consumes Arrow C Data Interface | Unit tests |
| Zero-copy verified | Memory comparison tests |
| Release callbacks work | Valgrind/ASAN tests |
| Works with PyArrow | Integration test |

---

## 6. Testing

```rust
#[test]
fn test_export_import_roundtrip() {
    // Create a batch
    let batch = create_test_batch();
    let mut ffi_batch = LaminarRecordBatch::new(batch.clone());

    // Export
    let mut out_array = FFI_ArrowArray::empty();
    let mut out_schema = FFI_ArrowSchema::empty();

    unsafe {
        let rc = laminar_batch_export(
            &mut ffi_batch,
            &mut out_array,
            &mut out_schema,
        );
        assert_eq!(rc, LAMINAR_OK);
    }

    // Import
    let mut imported: *mut LaminarRecordBatch = std::ptr::null_mut();
    unsafe {
        let rc = laminar_batch_import(
            &mut out_array,
            &mut out_schema,
            &mut imported,
        );
        assert_eq!(rc, LAMINAR_OK);
    }

    // Verify data matches
    let imported_batch = unsafe { &(*imported).inner };
    assert_eq!(batch.num_rows(), imported_batch.num_rows());
    assert_eq!(batch.schema(), imported_batch.schema());
}
```

---

## 7. References

- [Arrow C Data Interface Specification](https://arrow.apache.org/docs/format/CDataInterface.html)
- [arrow-rs FFI Module](https://docs.rs/arrow/latest/arrow/ffi/index.html)
- [PyArrow C Data Interface](https://arrow.apache.org/docs/python/integration/python_java.html)
