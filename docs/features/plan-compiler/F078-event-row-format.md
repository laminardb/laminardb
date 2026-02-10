# F078: Event Row Format

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F078 |
| **Status** | ✅ Done |
| **Priority** | P0 |
| **Phase** | 2.5 (Plan Compiler) |
| **Effort** | S (1-2 days) |
| **Dependencies** | None |
| **Owner** | TBD |

## Summary

Define a fixed-layout, row-oriented in-memory format (`EventRow`) that compiled pipelines operate on. The format bridges Arrow's columnar model with event-at-a-time Ring 0 processing, supporting zero-copy extraction from incoming bytes and zero-copy conversion back to Arrow `RecordBatch` for Ring 1 handoff.

## Motivation

LaminarDB's Ring 0 hot path processes events one at a time through compiled pipelines, but Arrow's `RecordBatch` is columnar — optimized for batch analytics, not single-event access. Extracting a single row from a `RecordBatch` requires computing column offsets and handling variable-length data across scattered memory. A dedicated row format enables:

1. **Direct field access** — compiled code emits a single memory offset load per field (< 10ns)
2. **Zero-copy ingestion** — Kafka/CDC events can be deserialized directly into `EventRow` layout
3. **Predictable layout** — schema is known at compile time, so Cranelift code hardcodes offsets
4. **Efficient Ring 0 → Ring 1 handoff** — accumulate rows into Arrow arrays without per-event allocation

```sql
-- For a query like:
SELECT symbol, price * quantity AS notional
FROM trades
WHERE price > 100.0

-- The compiled pipeline needs a row format where:
-- 1. 'symbol' is at a known offset (e.g., bytes 8..16)
-- 2. 'price' is at a known offset (e.g., bytes 16..24 as f64)
-- 3. 'quantity' is at a known offset (e.g., bytes 24..32 as i64)
-- No Arrow array indirection, no dynamic dispatch.
```

## Goals

1. `EventRow` — fixed-layout row format with null bitmap, fixed-size fields inline, variable-length fields at tail
2. `RowSchema` — compile-time schema descriptor that generates field accessor metadata (offsets, types, null positions)
3. `RowBatchBridge` — accumulates `EventRow`s into Arrow `RecordBatch` for Ring 1 handoff
4. Zero-copy fast path for all-fixed-width schemas (most streaming use cases)
5. Arena-compatible layout — rows can be allocated from `bumpalo` arenas in Ring 0

## Non-Goals

- Replacing Arrow as the interchange format (Arrow stays for Ring 1/Ring 2)
- Supporting nested/complex types in Phase 1 (Struct, List, Map)
- Compression or encoding (rows are transient, not stored)
- Variable-length string interning (future optimization)

## Technical Design

### EventRow Layout

```
┌─────────────────────────────────────────────────────────┐
│                      EventRow Layout                     │
├──────────┬──────────┬───────────────┬───────────────────┤
│  Header  │  Null    │  Fixed-Size   │  Variable-Length   │
│ (8 bytes)│  Bitmap  │  Fields       │  Fields            │
│          │ (N bits) │  (inline)     │  (offset + data)   │
├──────────┼──────────┼───────────────┼───────────────────┤
│ schema_id│ b0 b1 .. │ i64 f64 i32  │ len|data len|data  │
│ row_len  │          │ ...          │ ...                │
└──────────┴──────────┴───────────────┴───────────────────┘
```

```rust
/// Compile-time schema descriptor for an EventRow layout.
///
/// Generated once per query at compilation time. Stores field offsets
/// so compiled code uses constant memory loads.
#[derive(Debug, Clone)]
pub struct RowSchema {
    /// Unique identifier for this schema (hash of field types + names).
    pub schema_id: u32,
    /// Total fixed-region size in bytes (header + bitmap + fixed fields).
    pub fixed_size: usize,
    /// Per-field metadata, ordered by field index.
    pub fields: Vec<FieldLayout>,
    /// Byte offset where the null bitmap starts.
    pub null_bitmap_offset: usize,
    /// Number of fields (determines bitmap size).
    pub field_count: usize,
    /// Whether all fields are fixed-width (enables fast path).
    pub all_fixed: bool,
}

/// Layout metadata for a single field within the row.
#[derive(Debug, Clone, Copy)]
pub struct FieldLayout {
    /// Byte offset from row start to this field's data.
    pub offset: usize,
    /// Physical size in bytes (0 for variable-length).
    pub size: usize,
    /// Arrow DataType for conversion.
    pub data_type: FieldType,
    /// Bit position in the null bitmap.
    pub null_bit: usize,
    /// True if this field is variable-length (String, Binary).
    pub is_variable: bool,
}

/// Supported field types for compiled access.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    /// Stored as i64 microseconds since epoch.
    TimestampMicros,
    /// Variable-length UTF-8 string (offset + length in fixed region).
    Utf8,
    /// Variable-length binary (offset + length in fixed region).
    Binary,
}
```

### EventRow Access API

```rust
/// A single event row in the compiled pipeline's row format.
///
/// This is a thin wrapper around a byte slice. The `RowSchema` provides
/// the offsets needed to extract fields. Compiled code bypasses this API
/// entirely and uses hardcoded offsets.
pub struct EventRow<'a> {
    data: &'a [u8],
    schema: &'a RowSchema,
}

impl<'a> EventRow<'a> {
    /// Read a non-null i64 field at the given field index.
    #[inline(always)]
    pub fn get_i64(&self, field_idx: usize) -> Option<i64> {
        if self.is_null(field_idx) {
            return None;
        }
        let offset = self.schema.fields[field_idx].offset;
        let bytes = &self.data[offset..offset + 8];
        Some(i64::from_le_bytes(bytes.try_into().unwrap()))
    }

    /// Read a non-null f64 field at the given field index.
    #[inline(always)]
    pub fn get_f64(&self, field_idx: usize) -> Option<f64> {
        if self.is_null(field_idx) {
            return None;
        }
        let offset = self.schema.fields[field_idx].offset;
        let bytes = &self.data[offset..offset + 8];
        Some(f64::from_le_bytes(bytes.try_into().unwrap()))
    }

    /// Read a variable-length UTF-8 string field.
    #[inline(always)]
    pub fn get_str(&self, field_idx: usize) -> Option<&'a str> {
        if self.is_null(field_idx) {
            return None;
        }
        let offset = self.schema.fields[field_idx].offset;
        // Variable-length: fixed region stores (data_offset: u32, length: u32)
        let data_offset = u32::from_le_bytes(
            self.data[offset..offset + 4].try_into().unwrap()
        ) as usize;
        let length = u32::from_le_bytes(
            self.data[offset + 4..offset + 8].try_into().unwrap()
        ) as usize;
        let bytes = &self.data[data_offset..data_offset + length];
        Some(std::str::from_utf8(bytes).unwrap())
    }

    /// Check if a field is null via the bitmap.
    #[inline(always)]
    pub fn is_null(&self, field_idx: usize) -> bool {
        let byte_idx = self.schema.null_bitmap_offset + (field_idx >> 3);
        let bit_idx = field_idx & 7;
        (self.data[byte_idx] & (1 << bit_idx)) != 0
    }
}
```

### MutableEventRow (Ring 0 Writer)

```rust
/// A writable event row backed by arena-allocated memory.
///
/// Used by compiled pipelines to construct output rows without
/// heap allocation. The backing memory comes from a `bumpalo::Bump` arena.
pub struct MutableEventRow<'a> {
    data: &'a mut [u8],
    schema: &'a RowSchema,
    /// Current write position for variable-length data (grows from end).
    var_offset: usize,
}

impl<'a> MutableEventRow<'a> {
    /// Allocate a new row from the arena with the given schema.
    pub fn new_in(arena: &'a bumpalo::Bump, schema: &'a RowSchema, var_capacity: usize) -> Self {
        let total = schema.fixed_size + var_capacity;
        let data = arena.alloc_slice_fill_default(total);
        Self {
            data,
            schema,
            var_offset: schema.fixed_size,
        }
    }

    /// Write an i64 value to the given field.
    #[inline(always)]
    pub fn set_i64(&mut self, field_idx: usize, value: i64) {
        let offset = self.schema.fields[field_idx].offset;
        self.data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        self.clear_null(field_idx);
    }

    /// Write an f64 value to the given field.
    #[inline(always)]
    pub fn set_f64(&mut self, field_idx: usize, value: f64) {
        let offset = self.schema.fields[field_idx].offset;
        self.data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        self.clear_null(field_idx);
    }

    /// Write a string value (appends to variable-length region).
    #[inline(always)]
    pub fn set_str(&mut self, field_idx: usize, value: &str) {
        let offset = self.schema.fields[field_idx].offset;
        let var_start = self.var_offset;
        // Write data to variable region
        self.data[var_start..var_start + value.len()].copy_from_slice(value.as_bytes());
        // Write (offset, length) to fixed region
        self.data[offset..offset + 4].copy_from_slice(&(var_start as u32).to_le_bytes());
        self.data[offset + 4..offset + 8].copy_from_slice(&(value.len() as u32).to_le_bytes());
        self.var_offset += value.len();
        self.clear_null(field_idx);
    }

    /// Mark a field as null in the bitmap.
    #[inline(always)]
    pub fn set_null(&mut self, field_idx: usize) {
        let byte_idx = self.schema.null_bitmap_offset + (field_idx >> 3);
        let bit_idx = field_idx & 7;
        self.data[byte_idx] |= 1 << bit_idx;
    }

    #[inline(always)]
    fn clear_null(&mut self, field_idx: usize) {
        let byte_idx = self.schema.null_bitmap_offset + (field_idx >> 3);
        let bit_idx = field_idx & 7;
        self.data[byte_idx] &= !(1 << bit_idx);
    }
}
```

### RowBatchBridge (Ring 0 → Ring 1)

```rust
/// Accumulates EventRows into an Arrow RecordBatch.
///
/// Sits at the Ring 0/Ring 1 boundary. Ring 0 compiled pipelines write
/// EventRows; when the batch reaches the threshold (count or time),
/// it flushes as a RecordBatch for Ring 1 consumption.
pub struct RowBatchBridge {
    schema: Arc<arrow::datatypes::Schema>,
    row_schema: Arc<RowSchema>,
    /// Column builders — one per field.
    builders: Vec<Box<dyn arrow::array::ArrayBuilder>>,
    /// Current row count in the batch.
    row_count: usize,
    /// Flush threshold (number of rows).
    batch_size: usize,
}

impl RowBatchBridge {
    /// Append an EventRow to the batch builders.
    ///
    /// Returns `Some(RecordBatch)` if the batch is full and was flushed.
    #[inline]
    pub fn append(&mut self, row: &EventRow<'_>) -> Option<RecordBatch> {
        for (i, builder) in self.builders.iter_mut().enumerate() {
            if row.is_null(i) {
                builder.append_null();
            } else {
                // Type-specialized append via FieldType dispatch
                append_field(builder, row, i, &self.row_schema);
            }
        }
        self.row_count += 1;
        if self.row_count >= self.batch_size {
            Some(self.flush())
        } else {
            None
        }
    }

    /// Force-flush the current batch (called on time threshold or watermark).
    pub fn flush(&mut self) -> RecordBatch {
        let arrays: Vec<Arc<dyn arrow::array::Array>> = self.builders
            .iter_mut()
            .map(|b| b.finish())
            .collect();
        self.row_count = 0;
        RecordBatch::try_new(self.schema.clone(), arrays)
            .expect("schema mismatch in RowBatchBridge")
    }
}
```

### RowSchema from Arrow Schema

```rust
impl RowSchema {
    /// Build a RowSchema from an Arrow Schema.
    ///
    /// Computes field offsets assuming 8-byte alignment for fixed fields
    /// and (offset, length) pairs for variable-length fields.
    pub fn from_arrow(schema: &arrow::datatypes::Schema) -> Self {
        let field_count = schema.fields().len();
        let null_bitmap_bytes = (field_count + 7) / 8;

        // Header: 4 bytes schema_id + 4 bytes row_len = 8 bytes
        let header_size = 8;
        let null_bitmap_offset = header_size;
        let mut current_offset = header_size + null_bitmap_bytes;

        // Align to 8 bytes after bitmap
        current_offset = (current_offset + 7) & !7;

        let mut fields = Vec::with_capacity(field_count);
        let mut all_fixed = true;

        for (i, field) in schema.fields().iter().enumerate() {
            let (field_type, size, is_variable) = match field.data_type() {
                DataType::Boolean => (FieldType::Bool, 1, false),
                DataType::Int8 => (FieldType::Int8, 1, false),
                DataType::Int16 => (FieldType::Int16, 2, false),
                DataType::Int32 => (FieldType::Int32, 4, false),
                DataType::Int64 => (FieldType::Int64, 8, false),
                DataType::UInt8 => (FieldType::UInt8, 1, false),
                DataType::UInt16 => (FieldType::UInt16, 2, false),
                DataType::UInt32 => (FieldType::UInt32, 4, false),
                DataType::UInt64 => (FieldType::UInt64, 8, false),
                DataType::Float32 => (FieldType::Float32, 4, false),
                DataType::Float64 => (FieldType::Float64, 8, false),
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    (FieldType::TimestampMicros, 8, false)
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    all_fixed = false;
                    (FieldType::Utf8, 8, true) // 4-byte offset + 4-byte length
                }
                DataType::Binary | DataType::LargeBinary => {
                    all_fixed = false;
                    (FieldType::Binary, 8, true)
                }
                other => panic!("Unsupported type for EventRow: {:?}", other),
            };

            // Align offset to field's natural alignment
            let align = size.min(8);
            current_offset = (current_offset + align - 1) & !(align - 1);

            fields.push(FieldLayout {
                offset: current_offset,
                size,
                data_type: field_type,
                null_bit: i,
                is_variable,
            });
            current_offset += size;
        }

        // Align total fixed size to 8 bytes
        let fixed_size = (current_offset + 7) & !7;

        Self {
            schema_id: fxhash::hash32(schema.to_string().as_bytes()),
            fixed_size,
            fields,
            null_bitmap_offset,
            field_count,
            all_fixed,
        }
    }
}
```

## Performance Targets

| Metric | Target |
|--------|--------|
| Field access (fixed) | < 5ns (single memory load) |
| Field access (variable) | < 15ns (two memory loads) |
| Null check | < 3ns (bitmap bit test) |
| Row construction (arena) | < 50ns for 5-field fixed-width row |
| Batch flush (1000 rows) | < 50µs |
| Memory overhead vs raw struct | < 10% (bitmap + header) |

## Testing

| Module | Tests | What |
|--------|-------|------|
| `RowSchema` | 8 | from_arrow conversion, offset computation, alignment, all_fixed detection |
| `EventRow` | 10 | get_i64/f64/str, null handling, boundary values, empty row |
| `MutableEventRow` | 8 | set_i64/f64/str, null toggle, arena allocation, variable overflow |
| `RowBatchBridge` | 6 | append + flush, threshold trigger, time-based flush, empty batch |
| Integration | 4 | Arrow → RowSchema → write rows → flush → verify RecordBatch |

## Files

- `crates/laminar-core/src/compiler/row.rs` — NEW: `RowSchema`, `EventRow`, `MutableEventRow`
- `crates/laminar-core/src/compiler/bridge.rs` — NEW: `RowBatchBridge`
- `crates/laminar-core/src/compiler/mod.rs` — NEW: Module root
- `crates/laminar-core/src/lib.rs` — Add `pub mod compiler`
