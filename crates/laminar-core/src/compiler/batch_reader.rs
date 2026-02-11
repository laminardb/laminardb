//! Arrow `RecordBatch` → [`EventRow`] bridge for Ring 0 input.
//!
//! [`BatchRowReader`] decomposes columnar Arrow data into row-oriented
//! [`EventRow`]s allocated in a bump arena. This is the reverse of
//! [`RowBatchBridge`](super::bridge::RowBatchBridge), which converts
//! rows *back* to Arrow `RecordBatch`.
//!
//! # Usage
//!
//! ```
//! use std::sync::Arc;
//! use arrow_array::{Int64Array, Float64Array, RecordBatch};
//! use arrow_schema::{DataType, Field, Schema};
//! use bumpalo::Bump;
//! use laminar_core::compiler::row::RowSchema;
//! use laminar_core::compiler::batch_reader::BatchRowReader;
//!
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("ts", DataType::Int64, false),
//!     Field::new("val", DataType::Float64, true),
//! ]));
//! let batch = RecordBatch::try_new(
//!     Arc::clone(&schema),
//!     vec![
//!         Arc::new(Int64Array::from(vec![1000, 2000])),
//!         Arc::new(Float64Array::from(vec![Some(1.5), None])),
//!     ],
//! ).unwrap();
//!
//! let row_schema = RowSchema::from_arrow(&schema).unwrap();
//! let reader = BatchRowReader::new(&batch, &row_schema);
//! assert_eq!(reader.row_count(), 2);
//!
//! let arena = Bump::new();
//! let row0 = reader.read_row(0, &arena);
//! assert_eq!(row0.get_i64(0), 1000);
//! assert!(!row0.is_null(1));
//!
//! let row1 = reader.read_row(1, &arena);
//! assert!(row1.is_null(1));
//! ```

use arrow_array::cast::AsArray;
use arrow_array::types::TimestampMicrosecondType;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};

use bumpalo::Bump;

use super::row::{EventRow, FieldType, MutableEventRow, RowSchema};

/// Pre-downcast column accessor to avoid per-row `downcast_ref` overhead.
enum ColumnAccessor<'a> {
    Bool(&'a BooleanArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    TimestampMicros(&'a arrow_array::TimestampMicrosecondArray),
    Utf8(&'a StringArray),
    Binary(&'a BinaryArray),
}

/// Reads rows from an Arrow [`RecordBatch`] into [`EventRow`] format.
///
/// Each call to [`read_row`](Self::read_row) decomposes a single row from the
/// columnar batch into a row-oriented [`EventRow`] allocated in a bump arena.
/// Column downcasts are performed once at construction for amortized overhead.
pub struct BatchRowReader<'a> {
    batch: &'a RecordBatch,
    schema: &'a RowSchema,
    columns: Vec<ColumnAccessor<'a>>,
    /// Pre-computed variable-length capacity estimate (64 bytes per var-len field).
    var_capacity: usize,
}

impl<'a> BatchRowReader<'a> {
    /// Creates a reader for the given batch.
    ///
    /// # Panics
    ///
    /// Panics if the batch schema doesn't match the [`RowSchema`] field types
    /// (field count mismatch or incompatible data types).
    #[must_use]
    pub fn new(batch: &'a RecordBatch, schema: &'a RowSchema) -> Self {
        assert_eq!(
            batch.num_columns(),
            schema.field_count(),
            "BatchRowReader: column count mismatch: batch has {}, schema has {}",
            batch.num_columns(),
            schema.field_count()
        );

        let columns: Vec<ColumnAccessor<'a>> = (0..schema.field_count())
            .map(|i| downcast_column(batch, schema, i))
            .collect();

        let var_capacity = schema.fields().iter().filter(|f| f.is_variable).count() * 64;

        Self {
            batch,
            schema,
            columns,
            var_capacity,
        }
    }

    /// Returns the number of rows in the batch.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.batch.num_rows()
    }

    /// Reads row `row_idx` into an [`EventRow`] allocated in the given arena.
    ///
    /// # Panics
    ///
    /// Panics if `row_idx >= row_count()`.
    #[must_use]
    pub fn read_row<'b>(&self, row_idx: usize, arena: &'b Bump) -> EventRow<'b>
    where
        'a: 'b,
    {
        debug_assert!(
            row_idx < self.batch.num_rows(),
            "row index {row_idx} out of bounds (batch has {} rows)",
            self.batch.num_rows()
        );

        let mut row = MutableEventRow::new_in(arena, self.schema, self.var_capacity);

        for (field_idx, col) in self.columns.iter().enumerate() {
            read_field(&mut row, col, field_idx, row_idx);
        }

        row.freeze()
    }
}

/// Downcasts a batch column to the typed `ColumnAccessor` matching the schema.
fn downcast_column<'a>(
    batch: &'a RecordBatch,
    schema: &RowSchema,
    col_idx: usize,
) -> ColumnAccessor<'a> {
    let field_type = schema.field(col_idx).field_type;
    let col = batch.column(col_idx);

    match field_type {
        FieldType::Bool => ColumnAccessor::Bool(col.as_boolean()),
        FieldType::Int8 => ColumnAccessor::Int8(col.as_primitive::<arrow_array::types::Int8Type>()),
        FieldType::Int16 => {
            ColumnAccessor::Int16(col.as_primitive::<arrow_array::types::Int16Type>())
        }
        FieldType::Int32 => {
            ColumnAccessor::Int32(col.as_primitive::<arrow_array::types::Int32Type>())
        }
        FieldType::Int64 => {
            ColumnAccessor::Int64(col.as_primitive::<arrow_array::types::Int64Type>())
        }
        FieldType::UInt8 => {
            ColumnAccessor::UInt8(col.as_primitive::<arrow_array::types::UInt8Type>())
        }
        FieldType::UInt16 => {
            ColumnAccessor::UInt16(col.as_primitive::<arrow_array::types::UInt16Type>())
        }
        FieldType::UInt32 => {
            ColumnAccessor::UInt32(col.as_primitive::<arrow_array::types::UInt32Type>())
        }
        FieldType::UInt64 => {
            ColumnAccessor::UInt64(col.as_primitive::<arrow_array::types::UInt64Type>())
        }
        FieldType::Float32 => {
            ColumnAccessor::Float32(col.as_primitive::<arrow_array::types::Float32Type>())
        }
        FieldType::Float64 => {
            ColumnAccessor::Float64(col.as_primitive::<arrow_array::types::Float64Type>())
        }
        FieldType::TimestampMicros => {
            ColumnAccessor::TimestampMicros(col.as_primitive::<TimestampMicrosecondType>())
        }
        FieldType::Utf8 => ColumnAccessor::Utf8(col.as_string::<i32>()),
        FieldType::Binary => ColumnAccessor::Binary(col.as_binary::<i32>()),
    }
}

/// Reads one field from a column array into a `MutableEventRow`.
#[allow(clippy::too_many_lines)]
fn read_field(
    row: &mut MutableEventRow<'_>,
    col: &ColumnAccessor<'_>,
    field_idx: usize,
    row_idx: usize,
) {
    match col {
        ColumnAccessor::Bool(arr) => {
            if arr.is_null(row_idx) {
                row.set_null(field_idx, true);
            } else {
                row.set_bool(field_idx, arr.value(row_idx));
            }
        }
        ColumnAccessor::Int8(arr) => {
            if arr.is_null(row_idx) {
                row.set_null(field_idx, true);
            } else {
                row.set_i8(field_idx, arr.value(row_idx));
            }
        }
        ColumnAccessor::Int16(arr) => {
            if arr.is_null(row_idx) {
                row.set_null(field_idx, true);
            } else {
                row.set_i16(field_idx, arr.value(row_idx));
            }
        }
        ColumnAccessor::Int32(arr) => {
            if arr.is_null(row_idx) {
                row.set_null(field_idx, true);
            } else {
                row.set_i32(field_idx, arr.value(row_idx));
            }
        }
        ColumnAccessor::Int64(arr) => {
            if arr.is_null(row_idx) {
                row.set_null(field_idx, true);
            } else {
                row.set_i64(field_idx, arr.value(row_idx));
            }
        }
        ColumnAccessor::UInt8(arr) => {
            if arr.is_null(row_idx) {
                row.set_null(field_idx, true);
            } else {
                row.set_u8(field_idx, arr.value(row_idx));
            }
        }
        ColumnAccessor::UInt16(arr) => {
            if arr.is_null(row_idx) {
                row.set_null(field_idx, true);
            } else {
                row.set_u16(field_idx, arr.value(row_idx));
            }
        }
        ColumnAccessor::UInt32(arr) => {
            if arr.is_null(row_idx) {
                row.set_null(field_idx, true);
            } else {
                row.set_u32(field_idx, arr.value(row_idx));
            }
        }
        ColumnAccessor::UInt64(arr) => {
            if arr.is_null(row_idx) {
                row.set_null(field_idx, true);
            } else {
                row.set_u64(field_idx, arr.value(row_idx));
            }
        }
        ColumnAccessor::Float32(arr) => {
            if arr.is_null(row_idx) {
                row.set_null(field_idx, true);
            } else {
                row.set_f32(field_idx, arr.value(row_idx));
            }
        }
        ColumnAccessor::Float64(arr) => {
            if arr.is_null(row_idx) {
                row.set_null(field_idx, true);
            } else {
                row.set_f64(field_idx, arr.value(row_idx));
            }
        }
        ColumnAccessor::TimestampMicros(arr) => {
            if arr.is_null(row_idx) {
                row.set_null(field_idx, true);
            } else {
                row.set_i64(field_idx, arr.value(row_idx));
            }
        }
        ColumnAccessor::Utf8(arr) => {
            if arr.is_null(row_idx) {
                row.set_null(field_idx, true);
            } else {
                row.set_str(field_idx, arr.value(row_idx));
            }
        }
        ColumnAccessor::Binary(arr) => {
            if arr.is_null(row_idx) {
                row.set_null(field_idx, true);
            } else {
                row.set_bytes(field_idx, arr.value(row_idx));
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;
    use crate::compiler::bridge::RowBatchBridge;
    use arrow_array::builder::TimestampMicrosecondBuilder;
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn make_schema(fields: Vec<(&str, DataType, bool)>) -> Arc<Schema> {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dt, nullable)| Field::new(name, dt, nullable))
                .collect::<Vec<_>>(),
        ))
    }

    // ── Construction tests ──────────────────────────────────────────

    #[test]
    fn new_valid_schema() {
        let schema = make_schema(vec![
            ("id", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)])),
            ],
        )
        .unwrap();
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let reader = BatchRowReader::new(&batch, &row_schema);
        assert_eq!(reader.row_count(), 3);
    }

    #[test]
    #[should_panic(expected = "column count mismatch")]
    fn new_column_count_mismatch() {
        let batch_schema = make_schema(vec![("a", DataType::Int64, false)]);
        let row_schema_arrow = make_schema(vec![
            ("a", DataType::Int64, false),
            ("b", DataType::Int64, false),
        ]);
        let batch =
            RecordBatch::try_new(batch_schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let row_schema = RowSchema::from_arrow(&row_schema_arrow).unwrap();
        let _ = BatchRowReader::new(&batch, &row_schema);
    }

    #[test]
    fn row_count_empty_batch() {
        let schema = make_schema(vec![("x", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap();
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let reader = BatchRowReader::new(&batch, &row_schema);
        assert_eq!(reader.row_count(), 0);
    }

    // ── Fixed-type round-trip tests ─────────────────────────────────

    #[test]
    fn roundtrip_int64_float64() {
        let schema = make_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1000, 2000])),
                Arc::new(Float64Array::from(vec![3.14, 2.718])),
            ],
        )
        .unwrap();
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let reader = BatchRowReader::new(&batch, &row_schema);
        let arena = Bump::new();

        let row0 = reader.read_row(0, &arena);
        assert_eq!(row0.get_i64(0), 1000);
        assert!((row0.get_f64(1) - 3.14).abs() < f64::EPSILON);

        let row1 = reader.read_row(1, &arena);
        assert_eq!(row1.get_i64(0), 2000);
        assert!((row1.get_f64(1) - 2.718).abs() < f64::EPSILON);
    }

    #[test]
    fn roundtrip_all_integer_types() {
        let schema = make_schema(vec![
            ("a", DataType::Int8, false),
            ("b", DataType::Int16, false),
            ("c", DataType::Int32, false),
            ("d", DataType::Int64, false),
            ("e", DataType::UInt8, false),
            ("f", DataType::UInt16, false),
            ("g", DataType::UInt32, false),
            ("h", DataType::UInt64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int8Array::from(vec![-42])),
                Arc::new(Int16Array::from(vec![-1000])),
                Arc::new(Int32Array::from(vec![100_000])),
                Arc::new(Int64Array::from(vec![i64::MAX])),
                Arc::new(UInt8Array::from(vec![255])),
                Arc::new(UInt16Array::from(vec![60_000])),
                Arc::new(UInt32Array::from(vec![u32::MAX])),
                Arc::new(UInt64Array::from(vec![u64::MAX])),
            ],
        )
        .unwrap();
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let reader = BatchRowReader::new(&batch, &row_schema);
        let arena = Bump::new();

        let row = reader.read_row(0, &arena);
        assert_eq!(row.get_i8(0), -42);
        assert_eq!(row.get_i16(1), -1000);
        assert_eq!(row.get_i32(2), 100_000);
        assert_eq!(row.get_i64(3), i64::MAX);
        assert_eq!(row.get_u8(4), 255);
        assert_eq!(row.get_u16(5), 60_000);
        assert_eq!(row.get_u32(6), u32::MAX);
        assert_eq!(row.get_u64(7), u64::MAX);
    }

    #[test]
    fn roundtrip_bool() {
        let schema = make_schema(vec![("flag", DataType::Boolean, false)]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BooleanArray::from(vec![true, false, true]))],
        )
        .unwrap();
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let reader = BatchRowReader::new(&batch, &row_schema);
        let arena = Bump::new();

        assert!(reader.read_row(0, &arena).get_bool(0));
        assert!(!reader.read_row(1, &arena).get_bool(0));
        assert!(reader.read_row(2, &arena).get_bool(0));
    }

    #[test]
    fn roundtrip_float32() {
        let schema = make_schema(vec![("f", DataType::Float32, false)]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float32Array::from(vec![std::f32::consts::PI]))],
        )
        .unwrap();
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let reader = BatchRowReader::new(&batch, &row_schema);
        let arena = Bump::new();

        let row = reader.read_row(0, &arena);
        assert!((row.get_f32(0) - std::f32::consts::PI).abs() < f32::EPSILON);
    }

    #[test]
    fn roundtrip_timestamp_micros() {
        let schema = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]);
        let ts_val = 1_706_000_000_000_000_i64;
        let mut builder = TimestampMicrosecondBuilder::with_capacity(1);
        builder.append_value(ts_val);
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(builder.finish())]).unwrap();
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let reader = BatchRowReader::new(&batch, &row_schema);
        let arena = Bump::new();

        let row = reader.read_row(0, &arena);
        assert_eq!(row.get_i64(0), ts_val);
    }

    // ── Variable-length type tests ──────────────────────────────────

    #[test]
    fn roundtrip_utf8() {
        let schema = make_schema(vec![("name", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["hello", "world"]))],
        )
        .unwrap();
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let reader = BatchRowReader::new(&batch, &row_schema);
        let arena = Bump::new();

        assert_eq!(reader.read_row(0, &arena).get_str(0), "hello");
        assert_eq!(reader.read_row(1, &arena).get_str(0), "world");
    }

    #[test]
    fn roundtrip_binary() {
        let schema = make_schema(vec![("data", DataType::Binary, false)]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BinaryArray::from_vec(vec![
                &[0xDE, 0xAD],
                &[0xBE, 0xEF],
            ]))],
        )
        .unwrap();
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let reader = BatchRowReader::new(&batch, &row_schema);
        let arena = Bump::new();

        assert_eq!(reader.read_row(0, &arena).get_bytes(0), &[0xDE, 0xAD]);
        assert_eq!(reader.read_row(1, &arena).get_bytes(0), &[0xBE, 0xEF]);
    }

    // ── Null handling tests ─────────────────────────────────────────

    #[test]
    fn null_fields_preserved() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, true),
            ("b", DataType::Float64, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![Some(42), None])),
                Arc::new(Float64Array::from(vec![None, Some(3.14)])),
            ],
        )
        .unwrap();
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let reader = BatchRowReader::new(&batch, &row_schema);
        let arena = Bump::new();

        let row0 = reader.read_row(0, &arena);
        assert!(!row0.is_null(0));
        assert_eq!(row0.get_i64(0), 42);
        assert!(row0.is_null(1));

        let row1 = reader.read_row(1, &arena);
        assert!(row1.is_null(0));
        assert!(!row1.is_null(1));
        assert!((row1.get_f64(1) - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn mixed_null_non_null() {
        let schema = make_schema(vec![
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
            ("score", DataType::Float64, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("alice"), None])),
                Arc::new(Float64Array::from(vec![None, Some(95.5)])),
            ],
        )
        .unwrap();
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let reader = BatchRowReader::new(&batch, &row_schema);
        let arena = Bump::new();

        let row0 = reader.read_row(0, &arena);
        assert_eq!(row0.get_i64(0), 1);
        assert_eq!(row0.get_str(1), "alice");
        assert!(row0.is_null(2));

        let row1 = reader.read_row(1, &arena);
        assert_eq!(row1.get_i64(0), 2);
        assert!(row1.is_null(1));
        assert!((row1.get_f64(2) - 95.5).abs() < f64::EPSILON);
    }

    // ── Round-trip: batch → row → bridge → batch ────────────────────

    #[test]
    fn full_roundtrip_fixed_types() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, true),
            ("b", DataType::Float64, true),
        ]);
        let original = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![Some(10), None, Some(30)])),
                Arc::new(Float64Array::from(vec![Some(1.1), Some(2.2), None])),
            ],
        )
        .unwrap();

        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let reader = BatchRowReader::new(&original, &row_schema);
        let mut bridge = RowBatchBridge::new(schema, 16).unwrap();

        let arena = Bump::new();
        for i in 0..reader.row_count() {
            let row = reader.read_row(i, &arena);
            bridge.append_row(&row).unwrap();
        }
        let restored = bridge.flush();

        // Compare column by column.
        assert_eq!(restored.num_rows(), 3);
        let col_a = restored
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let col_b = restored
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert!(!col_a.is_null(0));
        assert_eq!(col_a.value(0), 10);
        assert!(col_a.is_null(1));
        assert!(!col_a.is_null(2));
        assert_eq!(col_a.value(2), 30);

        assert!(!col_b.is_null(0));
        assert!((col_b.value(0) - 1.1).abs() < f64::EPSILON);
        assert!(!col_b.is_null(1));
        assert!((col_b.value(1) - 2.2).abs() < f64::EPSILON);
        assert!(col_b.is_null(2));
    }

    #[test]
    fn full_roundtrip_variable_types() {
        let schema = make_schema(vec![
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
            ("data", DataType::Binary, true),
        ]);
        let original = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("hello"), None])),
                Arc::new(BinaryArray::from_opt_vec(vec![None, Some(&[0xCA, 0xFE])])),
            ],
        )
        .unwrap();

        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let reader = BatchRowReader::new(&original, &row_schema);
        let mut bridge = RowBatchBridge::new(Arc::clone(original.schema_ref()), 16).unwrap();

        let arena = Bump::new();
        for i in 0..reader.row_count() {
            let row = reader.read_row(i, &arena);
            bridge.append_row(&row).unwrap();
        }
        let restored = bridge.flush();

        assert_eq!(restored.num_rows(), 2);
        let col_name = restored
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col_name.value(0), "hello");
        assert!(col_name.is_null(1));

        let col_data = restored
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert!(col_data.is_null(0));
        assert_eq!(col_data.value(1), &[0xCA, 0xFE]);
    }

    // ── Edge cases ──────────────────────────────────────────────────

    #[test]
    fn empty_batch_no_rows() {
        let schema = make_schema(vec![("x", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap();
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let reader = BatchRowReader::new(&batch, &row_schema);
        assert_eq!(reader.row_count(), 0);
    }

    #[test]
    fn single_row_batch() {
        let schema = make_schema(vec![("x", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![42]))],
        )
        .unwrap();
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let reader = BatchRowReader::new(&batch, &row_schema);
        assert_eq!(reader.row_count(), 1);

        let arena = Bump::new();
        let row = reader.read_row(0, &arena);
        assert_eq!(row.get_i64(0), 42);
    }

    #[test]
    fn empty_string_in_batch() {
        let schema = make_schema(vec![("s", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["", "nonempty"]))],
        )
        .unwrap();
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let reader = BatchRowReader::new(&batch, &row_schema);
        let arena = Bump::new();

        assert_eq!(reader.read_row(0, &arena).get_str(0), "");
        assert_eq!(reader.read_row(1, &arena).get_str(0), "nonempty");
    }
}
