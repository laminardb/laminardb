//! Fixed-layout row format for compiled Ring 0 event processing.
//!
//! [`EventRow`] provides a row-oriented, fixed-layout in-memory format that compiled
//! expressions operate on via direct pointer arithmetic. Unlike Arrow's columnar
//! `RecordBatch` (optimal for batch processing in Ring 1), `EventRow` gives compiled
//! Ring 0 pipelines direct pointer access to fields without column indirection.
//!
//! # Memory Layout
//!
//! ```text
//! [header: 8 bytes][null_bitmap: padded to 8B][fixed region][variable data region]
//! ```
//!
//! - **Header** (8 bytes): `[field_count: u16][flags: u16][var_region_offset: u32]`
//! - **Null bitmap**: `ceil(n_fields / 8)` bytes, padded to 8-byte alignment
//! - **Fixed region**: Fields in declaration order, naturally aligned
//! - **Variable data region**: Actual bytes for `Utf8`/`Binary` fields
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//! use arrow_schema::{DataType, Field, Schema};
//! use bumpalo::Bump;
//! use laminar_core::compiler::row::{RowSchema, MutableEventRow};
//!
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("ts", DataType::Int64, false),
//!     Field::new("value", DataType::Float64, true),
//!     Field::new("name", DataType::Utf8, true),
//! ]));
//! let row_schema = RowSchema::from_arrow(&schema).unwrap();
//!
//! let arena = Bump::new();
//! let mut row = MutableEventRow::new_in(&arena, &row_schema, 256);
//! row.set_i64(0, 1_000_000);
//! row.set_f64(1, 3.14);
//! row.set_str(2, "hello");
//!
//! let row = row.freeze();
//! assert_eq!(row.get_i64(0), 1_000_000);
//! assert!((row.get_f64(1) - 3.14).abs() < f64::EPSILON);
//! assert_eq!(row.get_str(2), "hello");
//! ```

use std::sync::Arc;

use arrow_schema::{DataType, SchemaRef, TimeUnit};
use bumpalo::Bump;

/// Size of the row header in bytes.
const HEADER_SIZE: usize = 8;

/// Supported scalar types for compiled row access.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    /// Boolean (1 byte inline).
    Bool,
    /// Signed 8-bit integer.
    Int8,
    /// Signed 16-bit integer.
    Int16,
    /// Signed 32-bit integer.
    Int32,
    /// Signed 64-bit integer.
    Int64,
    /// Unsigned 8-bit integer.
    UInt8,
    /// Unsigned 16-bit integer.
    UInt16,
    /// Unsigned 32-bit integer.
    UInt32,
    /// Unsigned 64-bit integer.
    UInt64,
    /// 32-bit floating point.
    Float32,
    /// 64-bit floating point.
    Float64,
    /// Timestamp in microseconds since epoch (stored as `i64`).
    TimestampMicros,
    /// Variable-length UTF-8 string (8-byte inline: `u32` offset + `u32` length).
    Utf8,
    /// Variable-length binary data (8-byte inline: `u32` offset + `u32` length).
    Binary,
}

impl FieldType {
    /// Converts an Arrow [`DataType`] to a [`FieldType`].
    ///
    /// Returns `None` for unsupported types.
    #[must_use]
    pub fn from_arrow(dt: &DataType) -> Option<Self> {
        match dt {
            DataType::Boolean => Some(Self::Bool),
            DataType::Int8 => Some(Self::Int8),
            DataType::Int16 => Some(Self::Int16),
            DataType::Int32 => Some(Self::Int32),
            DataType::Int64 => Some(Self::Int64),
            DataType::UInt8 => Some(Self::UInt8),
            DataType::UInt16 => Some(Self::UInt16),
            DataType::UInt32 => Some(Self::UInt32),
            DataType::UInt64 => Some(Self::UInt64),
            DataType::Float32 => Some(Self::Float32),
            DataType::Float64 => Some(Self::Float64),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Some(Self::TimestampMicros),
            DataType::Utf8 => Some(Self::Utf8),
            DataType::Binary => Some(Self::Binary),
            _ => None,
        }
    }

    /// Converts back to the corresponding Arrow [`DataType`].
    #[must_use]
    pub fn to_arrow(self) -> DataType {
        match self {
            Self::Bool => DataType::Boolean,
            Self::Int8 => DataType::Int8,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::UInt8 => DataType::UInt8,
            Self::UInt16 => DataType::UInt16,
            Self::UInt32 => DataType::UInt32,
            Self::UInt64 => DataType::UInt64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::TimestampMicros => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::Utf8 => DataType::Utf8,
            Self::Binary => DataType::Binary,
        }
    }

    /// Returns the inline byte size for this type.
    ///
    /// Fixed types return their native size. Variable-length types ([`Utf8`](Self::Utf8),
    /// [`Binary`](Self::Binary)) return 8 (a `u32` offset + `u32` length pair).
    #[must_use]
    pub const fn inline_size(self) -> usize {
        match self {
            Self::Bool | Self::Int8 | Self::UInt8 => 1,
            Self::Int16 | Self::UInt16 => 2,
            Self::Int32 | Self::UInt32 | Self::Float32 => 4,
            Self::Int64
            | Self::UInt64
            | Self::Float64
            | Self::TimestampMicros
            | Self::Utf8
            | Self::Binary => 8,
        }
    }

    /// Returns the natural alignment requirement in bytes.
    #[must_use]
    pub const fn alignment(self) -> usize {
        match self {
            Self::Bool | Self::Int8 | Self::UInt8 => 1,
            Self::Int16 | Self::UInt16 => 2,
            // Inline slot for Utf8/Binary contains two u32 values, so 4-byte alignment suffices.
            Self::Int32 | Self::UInt32 | Self::Float32 | Self::Utf8 | Self::Binary => 4,
            Self::Int64 | Self::UInt64 | Self::Float64 | Self::TimestampMicros => 8,
        }
    }

    /// Returns `true` if this type stores data in the variable region.
    #[must_use]
    pub const fn is_variable(self) -> bool {
        matches!(self, Self::Utf8 | Self::Binary)
    }
}

/// Per-field layout descriptor: absolute byte offset, size, null bitmap position.
#[derive(Debug, Clone, Copy)]
pub struct FieldLayout {
    /// The scalar type of this field.
    pub field_type: FieldType,
    /// Absolute byte offset from the row start to this field's inline data.
    pub offset: usize,
    /// Inline size in bytes.
    pub size: usize,
    /// Bit index in the null bitmap (0-based).
    pub null_bit: usize,
    /// Whether this field stores data in the variable region.
    pub is_variable: bool,
}

/// Errors from row schema construction.
#[derive(Debug, thiserror::Error)]
pub enum RowError {
    /// An Arrow field uses a data type not supported by the row format.
    #[error("unsupported Arrow data type for field '{name}': {data_type}")]
    UnsupportedType {
        /// Name of the unsupported field.
        name: String,
        /// The unsupported Arrow data type.
        data_type: DataType,
    },
}

/// Pre-computed row layout derived from an Arrow schema.
///
/// Converts an Arrow [`SchemaRef`] into a fixed layout suitable for compiled
/// field access via pointer arithmetic. The layout is computed once and reused
/// across all rows sharing the same schema.
#[derive(Debug, Clone)]
pub struct RowSchema {
    fields: Vec<FieldLayout>,
    arrow_schema: SchemaRef,
    null_bitmap_size: usize,
    fixed_region_offset: usize,
    min_row_size: usize,
}

impl RowSchema {
    /// Creates a [`RowSchema`] from an Arrow schema.
    ///
    /// # Errors
    ///
    /// Returns [`RowError::UnsupportedType`] if any field has an unsupported [`DataType`].
    pub fn from_arrow(schema: &SchemaRef) -> Result<Self, RowError> {
        let field_count = schema.fields().len();
        let null_bitmap_bytes = field_count.div_ceil(8);
        // Pad null bitmap to 8-byte alignment so the fixed region starts aligned.
        let padded_bitmap = (null_bitmap_bytes + 7) & !7;
        let fixed_region_offset = HEADER_SIZE + padded_bitmap;

        let mut fields = Vec::with_capacity(field_count);
        let mut current_offset = fixed_region_offset;

        for (i, arrow_field) in schema.fields().iter().enumerate() {
            let field_type = FieldType::from_arrow(arrow_field.data_type()).ok_or_else(|| {
                RowError::UnsupportedType {
                    name: arrow_field.name().clone(),
                    data_type: arrow_field.data_type().clone(),
                }
            })?;

            let align = field_type.alignment();
            current_offset = (current_offset + align - 1) & !(align - 1);

            fields.push(FieldLayout {
                field_type,
                offset: current_offset,
                size: field_type.inline_size(),
                null_bit: i,
                is_variable: field_type.is_variable(),
            });

            current_offset += field_type.inline_size();
        }

        Ok(Self {
            fields,
            arrow_schema: Arc::clone(schema),
            null_bitmap_size: padded_bitmap,
            fixed_region_offset,
            min_row_size: current_offset,
        })
    }

    /// Returns the header size in bytes (always 8).
    #[must_use]
    pub const fn header_size() -> usize {
        HEADER_SIZE
    }

    /// Returns the padded null bitmap size in bytes.
    #[must_use]
    pub fn null_bitmap_size(&self) -> usize {
        self.null_bitmap_size
    }

    /// Returns the byte offset where the fixed data region begins.
    #[must_use]
    pub fn fixed_region_offset(&self) -> usize {
        self.fixed_region_offset
    }

    /// Returns the minimum row size in bytes (header + bitmap + fixed region, no variable data).
    #[must_use]
    pub fn min_row_size(&self) -> usize {
        self.min_row_size
    }

    /// Returns the number of fields in the schema.
    #[must_use]
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// Returns the layout for the field at the given index.
    ///
    /// # Panics
    ///
    /// Panics if `idx >= field_count()`.
    #[must_use]
    pub fn field(&self, idx: usize) -> &FieldLayout {
        &self.fields[idx]
    }

    /// Returns all field layouts.
    #[must_use]
    pub fn fields(&self) -> &[FieldLayout] {
        &self.fields
    }

    /// Returns the Arrow schema this row layout was derived from.
    #[must_use]
    pub fn arrow_schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }
}

/// Zero-copy, read-only accessor for a fixed-layout event row.
///
/// All field accessors are `#[inline]` for compiled-code performance.
/// Type correctness is validated with `debug_assert` (zero overhead in release builds).
#[derive(Debug)]
pub struct EventRow<'a> {
    data: &'a [u8],
    schema: &'a RowSchema,
}

#[allow(clippy::missing_panics_doc)]
impl<'a> EventRow<'a> {
    /// Creates an [`EventRow`] from a byte slice and schema.
    ///
    /// # Panics
    ///
    /// Debug-asserts that `data.len() >= schema.min_row_size()`.
    #[inline]
    #[must_use]
    pub fn new(data: &'a [u8], schema: &'a RowSchema) -> Self {
        debug_assert!(
            data.len() >= schema.min_row_size(),
            "data too short: {} < {}",
            data.len(),
            schema.min_row_size()
        );
        Self { data, schema }
    }

    /// Returns the underlying byte slice.
    #[inline]
    #[must_use]
    pub fn data(&self) -> &'a [u8] {
        self.data
    }

    /// Returns the schema.
    #[inline]
    #[must_use]
    pub fn schema(&self) -> &'a RowSchema {
        self.schema
    }

    /// Returns `true` if the field at `field_idx` is null.
    ///
    /// Null bitmap convention: bit = 1 means null, bit = 0 means valid.
    #[inline]
    #[must_use]
    pub fn is_null(&self, field_idx: usize) -> bool {
        let bit = self.schema.fields[field_idx].null_bit;
        let byte_idx = HEADER_SIZE + bit / 8;
        let bit_idx = bit % 8;
        (self.data[byte_idx] & (1 << bit_idx)) != 0
    }

    /// Reads a `bool` field.
    #[inline]
    #[must_use]
    pub fn get_bool(&self, field_idx: usize) -> bool {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::Bool);
        let offset = self.schema.fields[field_idx].offset;
        self.data[offset] != 0
    }

    /// Reads an `i8` field.
    #[inline]
    #[must_use]
    pub fn get_i8(&self, field_idx: usize) -> i8 {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::Int8);
        let offset = self.schema.fields[field_idx].offset;
        i8::from_le_bytes([self.data[offset]])
    }

    /// Reads an `i16` field.
    #[inline]
    #[must_use]
    pub fn get_i16(&self, field_idx: usize) -> i16 {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::Int16);
        let offset = self.schema.fields[field_idx].offset;
        i16::from_le_bytes([self.data[offset], self.data[offset + 1]])
    }

    /// Reads an `i32` field.
    #[inline]
    #[must_use]
    pub fn get_i32(&self, field_idx: usize) -> i32 {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::Int32);
        let offset = self.schema.fields[field_idx].offset;
        let bytes: [u8; 4] = self.data[offset..offset + 4].try_into().unwrap();
        i32::from_le_bytes(bytes)
    }

    /// Reads an `i64` field. Also accepts [`FieldType::TimestampMicros`].
    #[inline]
    #[must_use]
    pub fn get_i64(&self, field_idx: usize) -> i64 {
        let ft = self.schema.fields[field_idx].field_type;
        debug_assert!(
            ft == FieldType::Int64 || ft == FieldType::TimestampMicros,
            "expected Int64 or TimestampMicros, got {ft:?}"
        );
        let offset = self.schema.fields[field_idx].offset;
        let bytes: [u8; 8] = self.data[offset..offset + 8].try_into().unwrap();
        i64::from_le_bytes(bytes)
    }

    /// Reads a `u8` field.
    #[inline]
    #[must_use]
    pub fn get_u8(&self, field_idx: usize) -> u8 {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::UInt8);
        self.data[self.schema.fields[field_idx].offset]
    }

    /// Reads a `u16` field.
    #[inline]
    #[must_use]
    pub fn get_u16(&self, field_idx: usize) -> u16 {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::UInt16);
        let offset = self.schema.fields[field_idx].offset;
        u16::from_le_bytes([self.data[offset], self.data[offset + 1]])
    }

    /// Reads a `u32` field.
    #[inline]
    #[must_use]
    pub fn get_u32(&self, field_idx: usize) -> u32 {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::UInt32);
        let offset = self.schema.fields[field_idx].offset;
        let bytes: [u8; 4] = self.data[offset..offset + 4].try_into().unwrap();
        u32::from_le_bytes(bytes)
    }

    /// Reads a `u64` field.
    #[inline]
    #[must_use]
    pub fn get_u64(&self, field_idx: usize) -> u64 {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::UInt64);
        let offset = self.schema.fields[field_idx].offset;
        let bytes: [u8; 8] = self.data[offset..offset + 8].try_into().unwrap();
        u64::from_le_bytes(bytes)
    }

    /// Reads an `f32` field.
    #[inline]
    #[must_use]
    pub fn get_f32(&self, field_idx: usize) -> f32 {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::Float32);
        let offset = self.schema.fields[field_idx].offset;
        let bytes: [u8; 4] = self.data[offset..offset + 4].try_into().unwrap();
        f32::from_le_bytes(bytes)
    }

    /// Reads an `f64` field.
    #[inline]
    #[must_use]
    pub fn get_f64(&self, field_idx: usize) -> f64 {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::Float64);
        let offset = self.schema.fields[field_idx].offset;
        let bytes: [u8; 8] = self.data[offset..offset + 8].try_into().unwrap();
        f64::from_le_bytes(bytes)
    }

    /// Reads a variable-length UTF-8 string field.
    #[inline]
    #[must_use]
    pub fn get_str(&self, field_idx: usize) -> &'a str {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::Utf8);
        let offset = self.schema.fields[field_idx].offset;
        let (var_off, var_len) = read_var_descriptor(self.data, offset);
        if var_len == 0 {
            return "";
        }
        std::str::from_utf8(&self.data[var_off..var_off + var_len])
            .expect("EventRow: invalid UTF-8 in Utf8 field")
    }

    /// Reads a variable-length binary field.
    #[inline]
    #[must_use]
    pub fn get_bytes(&self, field_idx: usize) -> &'a [u8] {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::Binary);
        let offset = self.schema.fields[field_idx].offset;
        let (var_off, var_len) = read_var_descriptor(self.data, offset);
        if var_len == 0 {
            return &[];
        }
        &self.data[var_off..var_off + var_len]
    }

    /// Returns a raw pointer to the field's inline data for JIT code.
    #[inline]
    #[must_use]
    pub fn field_ptr(&self, field_idx: usize) -> *const u8 {
        let offset = self.schema.fields[field_idx].offset;
        self.data[offset..].as_ptr()
    }

    /// Convenience: reads field 0 as `i64` (convention: field 0 is event time).
    #[inline]
    #[must_use]
    pub fn timestamp(&self) -> i64 {
        self.get_i64(0)
    }
}

/// Reads the `(offset, length)` variable-region descriptor at the given byte offset.
#[inline]
fn read_var_descriptor(data: &[u8], offset: usize) -> (usize, usize) {
    let off_bytes: [u8; 4] = data[offset..offset + 4].try_into().unwrap();
    let len_bytes: [u8; 4] = data[offset + 4..offset + 8].try_into().unwrap();
    (
        u32::from_le_bytes(off_bytes) as usize,
        u32::from_le_bytes(len_bytes) as usize,
    )
}

/// Arena-allocated, mutable writer for constructing an [`EventRow`].
///
/// The backing buffer is allocated from a [`bumpalo::Bump`] arena, ensuring
/// zero heap allocations on the hot path.
#[derive(Debug)]
pub struct MutableEventRow<'a> {
    data: &'a mut [u8],
    schema: &'a RowSchema,
    var_offset: usize,
}

impl<'a> MutableEventRow<'a> {
    /// Allocates a new row from the given arena.
    ///
    /// `var_capacity` is the number of extra bytes reserved for variable-length data
    /// (strings, binary). The total allocation is `schema.min_row_size() + var_capacity`.
    #[allow(clippy::cast_possible_truncation)]
    pub fn new_in(arena: &'a Bump, schema: &'a RowSchema, var_capacity: usize) -> Self {
        let total = schema.min_row_size() + var_capacity;
        let data: &'a mut [u8] = arena.alloc_slice_fill_default(total);

        // Write header: [field_count: u16][flags: u16][var_region_offset: u32]
        let field_count = schema.field_count() as u16;
        data[0..2].copy_from_slice(&field_count.to_le_bytes());
        // flags = 0 (already zeroed)
        let var_start = schema.min_row_size() as u32;
        data[4..8].copy_from_slice(&var_start.to_le_bytes());

        Self {
            data,
            schema,
            var_offset: schema.min_row_size(),
        }
    }

    /// Sets or clears the null flag for a field.
    ///
    /// When `is_null` is `true`, the field is marked as null in the bitmap.
    #[inline]
    pub fn set_null(&mut self, field_idx: usize, is_null: bool) {
        let bit = self.schema.fields[field_idx].null_bit;
        let byte_idx = HEADER_SIZE + bit / 8;
        let bit_idx = bit % 8;
        if is_null {
            self.data[byte_idx] |= 1 << bit_idx;
        } else {
            self.data[byte_idx] &= !(1 << bit_idx);
        }
    }

    /// Writes a `bool` field.
    #[inline]
    pub fn set_bool(&mut self, field_idx: usize, value: bool) {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::Bool);
        let offset = self.schema.fields[field_idx].offset;
        self.data[offset] = u8::from(value);
    }

    /// Writes an `i8` field.
    #[inline]
    pub fn set_i8(&mut self, field_idx: usize, value: i8) {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::Int8);
        let offset = self.schema.fields[field_idx].offset;
        self.data[offset..=offset].copy_from_slice(&value.to_le_bytes());
    }

    /// Writes an `i16` field.
    #[inline]
    pub fn set_i16(&mut self, field_idx: usize, value: i16) {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::Int16);
        let offset = self.schema.fields[field_idx].offset;
        self.data[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    /// Writes an `i32` field.
    #[inline]
    pub fn set_i32(&mut self, field_idx: usize, value: i32) {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::Int32);
        let offset = self.schema.fields[field_idx].offset;
        self.data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    /// Writes an `i64` field. Also accepts [`FieldType::TimestampMicros`].
    #[inline]
    pub fn set_i64(&mut self, field_idx: usize, value: i64) {
        let ft = self.schema.fields[field_idx].field_type;
        debug_assert!(
            ft == FieldType::Int64 || ft == FieldType::TimestampMicros,
            "expected Int64 or TimestampMicros, got {ft:?}"
        );
        let offset = self.schema.fields[field_idx].offset;
        self.data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    /// Writes a `u8` field.
    #[inline]
    pub fn set_u8(&mut self, field_idx: usize, value: u8) {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::UInt8);
        self.data[self.schema.fields[field_idx].offset] = value;
    }

    /// Writes a `u16` field.
    #[inline]
    pub fn set_u16(&mut self, field_idx: usize, value: u16) {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::UInt16);
        let offset = self.schema.fields[field_idx].offset;
        self.data[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    /// Writes a `u32` field.
    #[inline]
    pub fn set_u32(&mut self, field_idx: usize, value: u32) {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::UInt32);
        let offset = self.schema.fields[field_idx].offset;
        self.data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    /// Writes a `u64` field.
    #[inline]
    pub fn set_u64(&mut self, field_idx: usize, value: u64) {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::UInt64);
        let offset = self.schema.fields[field_idx].offset;
        self.data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    /// Writes an `f32` field.
    #[inline]
    pub fn set_f32(&mut self, field_idx: usize, value: f32) {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::Float32);
        let offset = self.schema.fields[field_idx].offset;
        self.data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    /// Writes an `f64` field.
    #[inline]
    pub fn set_f64(&mut self, field_idx: usize, value: f64) {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::Float64);
        let offset = self.schema.fields[field_idx].offset;
        self.data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    /// Writes a variable-length UTF-8 string field.
    ///
    /// Appends the string bytes to the variable region and writes the
    /// `(offset, length)` descriptor pair in the field's inline slot.
    ///
    /// # Panics
    ///
    /// Panics if the variable region overflows the allocated capacity.
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    pub fn set_str(&mut self, field_idx: usize, value: &str) {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::Utf8);
        self.write_variable(field_idx, value.as_bytes());
    }

    /// Writes a variable-length binary field.
    ///
    /// Appends the bytes to the variable region and writes the
    /// `(offset, length)` descriptor pair in the field's inline slot.
    ///
    /// # Panics
    ///
    /// Panics if the variable region overflows the allocated capacity.
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    pub fn set_bytes(&mut self, field_idx: usize, value: &[u8]) {
        debug_assert_eq!(self.schema.fields[field_idx].field_type, FieldType::Binary);
        self.write_variable(field_idx, value);
    }

    /// Borrows the current row state as a read-only [`EventRow`].
    ///
    /// The returned row's lifetime is tied to the borrow of `self`.
    #[inline]
    #[must_use]
    pub fn as_event_row(&self) -> EventRow<'_> {
        EventRow {
            data: &self.data[..self.var_offset],
            schema: self.schema,
        }
    }

    /// Consumes the mutable row, returning a read-only [`EventRow`] with the arena lifetime.
    #[inline]
    #[must_use]
    pub fn freeze(self) -> EventRow<'a> {
        let len = self.var_offset;
        let data: &'a [u8] = self.data;
        EventRow {
            data: &data[..len],
            schema: self.schema,
        }
    }

    /// Internal: appends variable-length bytes and writes the inline descriptor.
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    fn write_variable(&mut self, field_idx: usize, value: &[u8]) {
        let start = self.var_offset;
        let len = value.len();
        self.data[start..start + len].copy_from_slice(value);
        self.var_offset += len;

        let field_offset = self.schema.fields[field_idx].offset;
        self.data[field_offset..field_offset + 4].copy_from_slice(&(start as u32).to_le_bytes());
        self.data[field_offset + 4..field_offset + 8].copy_from_slice(&(len as u32).to_le_bytes());
    }
}

#[cfg(test)]
#[allow(
    clippy::float_cmp,
    clippy::approx_constant,
    clippy::modulo_one,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn make_schema(fields: Vec<(&str, DataType, bool)>) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dt, nullable)| Field::new(name, dt, nullable))
                .collect::<Vec<_>>(),
        ))
    }

    // ---- RowSchema tests ----

    #[test]
    fn schema_from_arrow_basic() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Float64, true),
            ("c", DataType::Boolean, false),
        ]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        assert_eq!(rs.field_count(), 3);
        assert_eq!(rs.field(0).field_type, FieldType::Int64);
        assert_eq!(rs.field(1).field_type, FieldType::Float64);
        assert_eq!(rs.field(2).field_type, FieldType::Bool);
    }

    #[test]
    fn schema_from_arrow_all_types() {
        let schema = make_schema(vec![
            ("f0", DataType::Boolean, true),
            ("f1", DataType::Int8, false),
            ("f2", DataType::Int16, false),
            ("f3", DataType::Int32, false),
            ("f4", DataType::Int64, false),
            ("f5", DataType::UInt8, false),
            ("f6", DataType::UInt16, false),
            ("f7", DataType::UInt32, false),
            ("f8", DataType::UInt64, false),
            ("f9", DataType::Float32, false),
            ("f10", DataType::Float64, false),
            (
                "f11",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            ("f12", DataType::Utf8, true),
            ("f13", DataType::Binary, true),
        ]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        assert_eq!(rs.field_count(), 14);
        assert_eq!(rs.field(11).field_type, FieldType::TimestampMicros);
        assert!(rs.field(12).is_variable);
        assert!(rs.field(13).is_variable);
    }

    #[test]
    fn schema_from_arrow_empty() {
        let schema = make_schema(vec![]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        assert_eq!(rs.field_count(), 0);
        assert_eq!(rs.min_row_size(), HEADER_SIZE);
        assert_eq!(rs.null_bitmap_size(), 0);
    }

    #[test]
    fn schema_from_arrow_unsupported() {
        let schema = make_schema(vec![(
            "bad",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            false,
        )]);
        let err = RowSchema::from_arrow(&schema).unwrap_err();
        assert!(err.to_string().contains("unsupported"));
    }

    #[test]
    fn schema_null_bitmap_sizes() {
        // 1 field: bitmap = 1 byte, padded to 8
        let s1 = make_schema(vec![("a", DataType::Int64, false)]);
        assert_eq!(RowSchema::from_arrow(&s1).unwrap().null_bitmap_size(), 8);

        // 8 fields: bitmap = 1 byte, padded to 8
        let s8 = make_schema(
            (0..8)
                .map(|i| {
                    (
                        Box::leak(format!("f{i}").into_boxed_str()) as &str,
                        DataType::Int32,
                        false,
                    )
                })
                .collect(),
        );
        assert_eq!(RowSchema::from_arrow(&s8).unwrap().null_bitmap_size(), 8);

        // 9 fields: bitmap = 2 bytes, padded to 8
        let s9 = make_schema(
            (0..9)
                .map(|i| {
                    (
                        Box::leak(format!("f{i}").into_boxed_str()) as &str,
                        DataType::Int32,
                        false,
                    )
                })
                .collect(),
        );
        assert_eq!(RowSchema::from_arrow(&s9).unwrap().null_bitmap_size(), 8);

        // 65 fields: bitmap = 9 bytes, padded to 16
        let s65 = make_schema(
            (0..65)
                .map(|i| {
                    (
                        Box::leak(format!("f{i}").into_boxed_str()) as &str,
                        DataType::Int32,
                        false,
                    )
                })
                .collect(),
        );
        assert_eq!(RowSchema::from_arrow(&s65).unwrap().null_bitmap_size(), 16);
    }

    #[test]
    fn schema_field_alignment() {
        // Bool (1B align) then Int64 (8B align) — should be padded.
        let schema = make_schema(vec![
            ("flag", DataType::Boolean, false),
            ("ts", DataType::Int64, false),
        ]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        // Bool at fixed_region_offset (16), size 1.
        // Int64 at next 8-byte boundary.
        let bool_off = rs.field(0).offset;
        let i64_off = rs.field(1).offset;
        assert_eq!(bool_off % 1, 0); // trivially aligned
        assert_eq!(i64_off % 8, 0); // 8-byte aligned
        assert!(i64_off > bool_off);
    }

    #[test]
    fn schema_mixed_fixed_variable() {
        let schema = make_schema(vec![
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
            ("score", DataType::Float64, false),
            ("data", DataType::Binary, true),
        ]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        assert_eq!(rs.field_count(), 4);
        assert!(!rs.field(0).is_variable);
        assert!(rs.field(1).is_variable);
        assert_eq!(rs.field(1).size, 8); // inline descriptor
        assert!(!rs.field(2).is_variable);
        assert!(rs.field(3).is_variable);
    }

    #[test]
    fn schema_var_region_offset_header() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Utf8, true),
        ]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        // min_row_size is where variable data would begin.
        assert!(rs.min_row_size() > rs.fixed_region_offset());
    }

    // ---- EventRow / MutableEventRow roundtrip tests ----

    #[test]
    fn roundtrip_bool() {
        let schema = make_schema(vec![("flag", DataType::Boolean, false)]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_bool(0, true);
        let row = row.freeze();
        assert!(row.get_bool(0));
    }

    #[test]
    fn roundtrip_integers() {
        let schema = make_schema(vec![
            ("a", DataType::Int8, false),
            ("b", DataType::Int16, false),
            ("c", DataType::Int32, false),
            ("d", DataType::Int64, false),
        ]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_i8(0, -42);
        row.set_i16(1, -1000);
        row.set_i32(2, 100_000);
        row.set_i64(3, i64::MAX);
        let row = row.freeze();
        assert_eq!(row.get_i8(0), -42);
        assert_eq!(row.get_i16(1), -1000);
        assert_eq!(row.get_i32(2), 100_000);
        assert_eq!(row.get_i64(3), i64::MAX);
    }

    #[test]
    fn roundtrip_unsigned() {
        let schema = make_schema(vec![
            ("a", DataType::UInt8, false),
            ("b", DataType::UInt16, false),
            ("c", DataType::UInt32, false),
            ("d", DataType::UInt64, false),
        ]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_u8(0, 255);
        row.set_u16(1, 60_000);
        row.set_u32(2, u32::MAX);
        row.set_u64(3, u64::MAX);
        let row = row.freeze();
        assert_eq!(row.get_u8(0), 255);
        assert_eq!(row.get_u16(1), 60_000);
        assert_eq!(row.get_u32(2), u32::MAX);
        assert_eq!(row.get_u64(3), u64::MAX);
    }

    #[test]
    fn roundtrip_floats() {
        let schema = make_schema(vec![
            ("a", DataType::Float32, false),
            ("b", DataType::Float64, false),
        ]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_f32(0, std::f32::consts::PI);
        row.set_f64(1, std::f64::consts::E);
        let row = row.freeze();
        assert!((row.get_f32(0) - std::f32::consts::PI).abs() < f32::EPSILON);
        assert!((row.get_f64(1) - std::f64::consts::E).abs() < f64::EPSILON);
    }

    #[test]
    fn roundtrip_timestamp() {
        let schema = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        let ts = 1_706_000_000_000_000_i64;
        row.set_i64(0, ts);
        let row = row.freeze();
        assert_eq!(row.get_i64(0), ts);
        assert_eq!(row.timestamp(), ts);
    }

    #[test]
    fn roundtrip_str() {
        let schema = make_schema(vec![("name", DataType::Utf8, true)]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 256);
        row.set_str(0, "hello world");
        let row = row.freeze();
        assert_eq!(row.get_str(0), "hello world");
    }

    #[test]
    fn roundtrip_bytes() {
        let schema = make_schema(vec![("data", DataType::Binary, true)]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 256);
        row.set_bytes(0, &[0xDE, 0xAD, 0xBE, 0xEF]);
        let row = row.freeze();
        assert_eq!(row.get_bytes(0), &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn roundtrip_all_types() {
        let schema = make_schema(vec![
            (
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            ("flag", DataType::Boolean, false),
            ("i8", DataType::Int8, false),
            ("i16", DataType::Int16, false),
            ("i32", DataType::Int32, false),
            ("i64", DataType::Int64, false),
            ("u8", DataType::UInt8, false),
            ("u16", DataType::UInt16, false),
            ("u32", DataType::UInt32, false),
            ("u64", DataType::UInt64, false),
            ("f32", DataType::Float32, false),
            ("f64", DataType::Float64, false),
            ("name", DataType::Utf8, true),
            ("data", DataType::Binary, true),
        ]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 512);

        row.set_i64(0, 999_999);
        row.set_bool(1, true);
        row.set_i8(2, -1);
        row.set_i16(3, 1000);
        row.set_i32(4, -50_000);
        row.set_i64(5, 42);
        row.set_u8(6, 200);
        row.set_u16(7, 50_000);
        row.set_u32(8, 3_000_000);
        row.set_u64(9, 18_000_000_000);
        row.set_f32(10, 1.5);
        row.set_f64(11, 2.718);
        row.set_str(12, "test");
        row.set_bytes(13, &[1, 2, 3]);

        let row = row.freeze();

        assert_eq!(row.timestamp(), 999_999);
        assert!(row.get_bool(1));
        assert_eq!(row.get_i8(2), -1);
        assert_eq!(row.get_i16(3), 1000);
        assert_eq!(row.get_i32(4), -50_000);
        assert_eq!(row.get_i64(5), 42);
        assert_eq!(row.get_u8(6), 200);
        assert_eq!(row.get_u16(7), 50_000);
        assert_eq!(row.get_u32(8), 3_000_000);
        assert_eq!(row.get_u64(9), 18_000_000_000);
        assert!((row.get_f32(10) - 1.5).abs() < f32::EPSILON);
        assert!((row.get_f64(11) - 2.718).abs() < f64::EPSILON);
        assert_eq!(row.get_str(12), "test");
        assert_eq!(row.get_bytes(13), &[1, 2, 3]);
    }

    #[test]
    fn null_bitmap_set_and_check() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, true),
            ("b", DataType::Float64, true),
            ("c", DataType::Utf8, true),
        ]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 64);

        // Initially all valid (not null).
        let view = row.as_event_row();
        assert!(!view.is_null(0));
        assert!(!view.is_null(1));
        assert!(!view.is_null(2));

        row.set_null(1, true);
        let view = row.as_event_row();
        assert!(!view.is_null(0));
        assert!(view.is_null(1));
        assert!(!view.is_null(2));

        // Clear it again.
        row.set_null(1, false);
        let view = row.as_event_row();
        assert!(!view.is_null(1));
    }

    #[test]
    fn all_null_row() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, true),
            ("b", DataType::Float64, true),
            ("c", DataType::Utf8, true),
        ]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 64);

        for i in 0..rs.field_count() {
            row.set_null(i, true);
        }
        let row = row.freeze();
        for i in 0..rs.field_count() {
            assert!(row.is_null(i));
        }
    }

    #[test]
    fn freeze_returns_correct_length() {
        let schema = make_schema(vec![
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 256);
        row.set_i64(0, 42);
        row.set_str(1, "hello");
        let frozen = row.freeze();
        // Data length = min_row_size + 5 ("hello")
        assert_eq!(frozen.data().len(), rs.min_row_size() + 5);
    }

    #[test]
    fn as_event_row_borrow() {
        let schema = make_schema(vec![("x", DataType::Int32, false)]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_i32(0, 77);
        // Borrow as EventRow, then continue mutating.
        assert_eq!(row.as_event_row().get_i32(0), 77);
        row.set_i32(0, 88);
        assert_eq!(row.as_event_row().get_i32(0), 88);
    }

    #[test]
    fn field_ptr_access() {
        let schema = make_schema(vec![("val", DataType::Int64, false)]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_i64(0, 12345);
        let frozen = row.freeze();
        let ptr = frozen.field_ptr(0);
        // Read i64 from raw pointer.
        let bytes = unsafe { std::slice::from_raw_parts(ptr, 8) };
        let val = i64::from_le_bytes(bytes.try_into().unwrap());
        assert_eq!(val, 12345);
    }

    #[test]
    fn timestamp_convenience() {
        let schema = make_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, false),
        ]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_i64(0, 42_000);
        row.set_f64(1, 1.0);
        let row = row.freeze();
        assert_eq!(row.timestamp(), 42_000);
    }

    #[test]
    fn empty_string_roundtrip() {
        let schema = make_schema(vec![("s", DataType::Utf8, true)]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 64);
        row.set_str(0, "");
        let row = row.freeze();
        assert_eq!(row.get_str(0), "");
    }

    #[test]
    fn large_binary_roundtrip() {
        let schema = make_schema(vec![("data", DataType::Binary, true)]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let blob: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
        let mut row = MutableEventRow::new_in(&arena, &rs, blob.len());
        row.set_bytes(0, &blob);
        let row = row.freeze();
        assert_eq!(row.get_bytes(0), &blob[..]);
    }

    #[test]
    fn multiple_variable_fields() {
        let schema = make_schema(vec![
            ("a", DataType::Utf8, true),
            ("b", DataType::Binary, true),
            ("c", DataType::Utf8, true),
        ]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 512);
        row.set_str(0, "first");
        row.set_bytes(1, &[10, 20, 30]);
        row.set_str(2, "third");
        let row = row.freeze();
        assert_eq!(row.get_str(0), "first");
        assert_eq!(row.get_bytes(1), &[10, 20, 30]);
        assert_eq!(row.get_str(2), "third");
    }

    #[test]
    fn roundtrip_max_values() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Float64, false),
            ("c", DataType::UInt64, false),
        ]);
        let rs = RowSchema::from_arrow(&schema).unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &rs, 0);
        row.set_i64(0, i64::MIN);
        row.set_f64(1, f64::MAX);
        row.set_u64(2, u64::MAX);
        let row = row.freeze();
        assert_eq!(row.get_i64(0), i64::MIN);
        assert_eq!(row.get_f64(1), f64::MAX);
        assert_eq!(row.get_u64(2), u64::MAX);
    }

    #[test]
    fn field_type_from_arrow_all() {
        assert_eq!(
            FieldType::from_arrow(&DataType::Boolean),
            Some(FieldType::Bool)
        );
        assert_eq!(
            FieldType::from_arrow(&DataType::Int8),
            Some(FieldType::Int8)
        );
        assert_eq!(
            FieldType::from_arrow(&DataType::Int16),
            Some(FieldType::Int16)
        );
        assert_eq!(
            FieldType::from_arrow(&DataType::Int32),
            Some(FieldType::Int32)
        );
        assert_eq!(
            FieldType::from_arrow(&DataType::Int64),
            Some(FieldType::Int64)
        );
        assert_eq!(
            FieldType::from_arrow(&DataType::UInt8),
            Some(FieldType::UInt8)
        );
        assert_eq!(
            FieldType::from_arrow(&DataType::UInt16),
            Some(FieldType::UInt16)
        );
        assert_eq!(
            FieldType::from_arrow(&DataType::UInt32),
            Some(FieldType::UInt32)
        );
        assert_eq!(
            FieldType::from_arrow(&DataType::UInt64),
            Some(FieldType::UInt64)
        );
        assert_eq!(
            FieldType::from_arrow(&DataType::Float32),
            Some(FieldType::Float32)
        );
        assert_eq!(
            FieldType::from_arrow(&DataType::Float64),
            Some(FieldType::Float64)
        );
        assert_eq!(
            FieldType::from_arrow(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            Some(FieldType::TimestampMicros)
        );
        assert_eq!(
            FieldType::from_arrow(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            )),
            Some(FieldType::TimestampMicros)
        );
        assert_eq!(
            FieldType::from_arrow(&DataType::Utf8),
            Some(FieldType::Utf8)
        );
        assert_eq!(
            FieldType::from_arrow(&DataType::Binary),
            Some(FieldType::Binary)
        );
        // Unsupported types
        assert_eq!(FieldType::from_arrow(&DataType::Float16), None);
        assert_eq!(FieldType::from_arrow(&DataType::LargeUtf8), None);
        assert_eq!(
            FieldType::from_arrow(&DataType::Timestamp(TimeUnit::Nanosecond, None)),
            None
        );
    }

    #[test]
    fn field_type_to_arrow_roundtrip() {
        let types = [
            FieldType::Bool,
            FieldType::Int8,
            FieldType::Int16,
            FieldType::Int32,
            FieldType::Int64,
            FieldType::UInt8,
            FieldType::UInt16,
            FieldType::UInt32,
            FieldType::UInt64,
            FieldType::Float32,
            FieldType::Float64,
            FieldType::TimestampMicros,
            FieldType::Utf8,
            FieldType::Binary,
        ];
        for ft in types {
            let arrow_dt = ft.to_arrow();
            let back = FieldType::from_arrow(&arrow_dt).unwrap();
            assert_eq!(ft, back, "roundtrip failed for {ft:?}");
        }
    }
}
