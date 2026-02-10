//! Bridge between row-oriented Ring 0 events and columnar Arrow `RecordBatch`.
//!
//! [`RowBatchBridge`] accumulates [`EventRow`]s and flushes them as Arrow
//! `RecordBatch` for operators that need columnar data (windows, joins,
//! aggregations, or Ring 1 sinks).

use arrow_array::builder::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, Int8Builder, StringBuilder, TimestampMicrosecondBuilder,
    UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, DataType, SchemaRef, TimeUnit};

use super::row::{EventRow, FieldType, RowError, RowSchema};

/// Errors from the [`RowBatchBridge`].
#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    /// The bridge's row capacity is full; flush before appending more rows.
    #[error("bridge is full (capacity: {0})")]
    Full(usize),
    /// Row schema conversion failed.
    #[error("row schema error: {0}")]
    Schema(#[from] RowError),
    /// Arrow operation failed.
    #[error("arrow error: {0}")]
    Arrow(#[from] ArrowError),
}

/// Accumulates [`EventRow`]s into Arrow `RecordBatch` for Ring 0 → Ring 1 handoff.
///
/// Each column is backed by an Arrow `ArrayBuilder` matching the field type.
/// When the bridge is full (or explicitly flushed), the accumulated rows are
/// materialized into a `RecordBatch`.
pub struct RowBatchBridge {
    schema: SchemaRef,
    row_schema: RowSchema,
    builders: Vec<Box<dyn ArrayBuilder>>,
    row_count: usize,
    capacity: usize,
}

impl RowBatchBridge {
    /// Creates a new bridge for the given Arrow schema with the specified row capacity.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::Schema`] if the schema contains unsupported data types.
    pub fn new(schema: SchemaRef, capacity: usize) -> Result<Self, BridgeError> {
        let row_schema = RowSchema::from_arrow(&schema)?;
        let builders = create_builders(&schema, capacity);
        Ok(Self {
            schema,
            row_schema,
            builders,
            row_count: 0,
            capacity,
        })
    }

    /// Decomposes the row's fields into the column builders.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::Full`] if the bridge has reached capacity.
    pub fn append_row(&mut self, row: &EventRow) -> Result<(), BridgeError> {
        if self.row_count >= self.capacity {
            return Err(BridgeError::Full(self.capacity));
        }
        for (i, layout) in self.row_schema.fields().iter().enumerate() {
            append_field(&mut self.builders[i], layout.field_type, row, i);
        }
        self.row_count += 1;
        Ok(())
    }

    /// Returns `true` if the bridge has reached its row capacity.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.row_count >= self.capacity
    }

    /// Finalizes all builders into a `RecordBatch` and resets the bridge.
    ///
    /// # Panics
    ///
    /// Panics if the builders produce arrays incompatible with the schema
    /// (indicates a bug in the bridge, not user error).
    pub fn flush(&mut self) -> RecordBatch {
        let arrays: Vec<_> = self.builders.iter_mut().map(ArrayBuilder::finish).collect();
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)
            .expect("RowBatchBridge: schema/array mismatch in flush");
        self.builders = create_builders(&self.schema, self.capacity);
        self.row_count = 0;
        batch
    }

    /// Returns the number of rows currently accumulated.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Returns the row capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Creates one `ArrayBuilder` per schema field, matching the Arrow `DataType`.
fn create_builders(schema: &SchemaRef, capacity: usize) -> Vec<Box<dyn ArrayBuilder>> {
    schema
        .fields()
        .iter()
        .map(|f| create_builder(f.data_type(), capacity))
        .collect()
}

fn create_builder(dt: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    match dt {
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Int8 => Box::new(Int8Builder::with_capacity(capacity)),
        DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::UInt8 => Box::new(UInt8Builder::with_capacity(capacity)),
        DataType::UInt16 => Box::new(UInt16Builder::with_capacity(capacity)),
        DataType::UInt32 => Box::new(UInt32Builder::with_capacity(capacity)),
        DataType::UInt64 => Box::new(UInt64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let builder = TimestampMicrosecondBuilder::with_capacity(capacity)
                .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, tz.clone()));
            Box::new(builder)
        }
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, capacity * 32)),
        other => unreachable!(
            "unsupported data type in RowBatchBridge: {other} (should be caught by RowSchema::from_arrow)"
        ),
    }
}

/// Appends one field from an `EventRow` to the corresponding column builder.
#[allow(clippy::too_many_lines)]
fn append_field(
    builder: &mut Box<dyn ArrayBuilder>,
    field_type: FieldType,
    row: &EventRow,
    field_idx: usize,
) {
    let is_null = row.is_null(field_idx);
    match field_type {
        FieldType::Bool => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap();
            if is_null {
                b.append_null();
            } else {
                b.append_value(row.get_bool(field_idx));
            }
        }
        FieldType::Int8 => {
            let b = builder.as_any_mut().downcast_mut::<Int8Builder>().unwrap();
            if is_null {
                b.append_null();
            } else {
                b.append_value(row.get_i8(field_idx));
            }
        }
        FieldType::Int16 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int16Builder>()
                .unwrap();
            if is_null {
                b.append_null();
            } else {
                b.append_value(row.get_i16(field_idx));
            }
        }
        FieldType::Int32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .unwrap();
            if is_null {
                b.append_null();
            } else {
                b.append_value(row.get_i32(field_idx));
            }
        }
        FieldType::Int64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .unwrap();
            if is_null {
                b.append_null();
            } else {
                b.append_value(row.get_i64(field_idx));
            }
        }
        FieldType::UInt8 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<UInt8Builder>()
                .unwrap();
            if is_null {
                b.append_null();
            } else {
                b.append_value(row.get_u8(field_idx));
            }
        }
        FieldType::UInt16 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<UInt16Builder>()
                .unwrap();
            if is_null {
                b.append_null();
            } else {
                b.append_value(row.get_u16(field_idx));
            }
        }
        FieldType::UInt32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<UInt32Builder>()
                .unwrap();
            if is_null {
                b.append_null();
            } else {
                b.append_value(row.get_u32(field_idx));
            }
        }
        FieldType::UInt64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<UInt64Builder>()
                .unwrap();
            if is_null {
                b.append_null();
            } else {
                b.append_value(row.get_u64(field_idx));
            }
        }
        FieldType::Float32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .unwrap();
            if is_null {
                b.append_null();
            } else {
                b.append_value(row.get_f32(field_idx));
            }
        }
        FieldType::Float64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap();
            if is_null {
                b.append_null();
            } else {
                b.append_value(row.get_f64(field_idx));
            }
        }
        FieldType::TimestampMicros => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap();
            if is_null {
                b.append_null();
            } else {
                b.append_value(row.get_i64(field_idx));
            }
        }
        FieldType::Utf8 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            if is_null {
                b.append_null();
            } else {
                b.append_value(row.get_str(field_idx));
            }
        }
        FieldType::Binary => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BinaryBuilder>()
                .unwrap();
            if is_null {
                b.append_null();
            } else {
                b.append_value(row.get_bytes(field_idx));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::row::MutableEventRow;
    use arrow_array::{
        Array, BooleanArray, Float64Array, Int64Array, StringArray, UInt32Array,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use bumpalo::Bump;
    use std::sync::Arc;

    fn make_schema(fields: Vec<(&str, DataType, bool)>) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dt, nullable)| Field::new(name, dt, nullable))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn bridge_single_row() {
        let schema = make_schema(vec![
            ("id", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let mut bridge = RowBatchBridge::new(schema, 16).unwrap();

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &row_schema, 0);
        row.set_i64(0, 100);
        row.set_f64(1, 3.14);
        let row = row.freeze();

        bridge.append_row(&row).unwrap();
        assert_eq!(bridge.row_count(), 1);

        let batch = bridge.flush();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);

        let col0 = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(col0.value(0), 100);

        let col1 = batch.column(0 + 1).as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((col1.value(0) - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn bridge_batch_accumulation() {
        let schema = make_schema(vec![("x", DataType::Int64, false)]);
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let mut bridge = RowBatchBridge::new(schema, 100).unwrap();

        let arena = Bump::new();
        for i in 0..10 {
            let mut row = MutableEventRow::new_in(&arena, &row_schema, 0);
            row.set_i64(0, i);
            let row = row.freeze();
            bridge.append_row(&row).unwrap();
        }
        assert_eq!(bridge.row_count(), 10);
        assert!(!bridge.is_full());

        let batch = bridge.flush();
        assert_eq!(batch.num_rows(), 10);

        let col = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..10 {
            assert_eq!(col.value(i), i as i64);
        }

        // After flush, bridge is reset.
        assert_eq!(bridge.row_count(), 0);
    }

    #[test]
    fn bridge_capacity_full_error() {
        let schema = make_schema(vec![("x", DataType::Int64, false)]);
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let mut bridge = RowBatchBridge::new(schema, 2).unwrap();

        let arena = Bump::new();
        for i in 0..2 {
            let mut row = MutableEventRow::new_in(&arena, &row_schema, 0);
            row.set_i64(0, i);
            bridge.append_row(&row.freeze()).unwrap();
        }
        assert!(bridge.is_full());

        // Third row should fail.
        let mut row = MutableEventRow::new_in(&arena, &row_schema, 0);
        row.set_i64(0, 999);
        let err = bridge.append_row(&row.freeze()).unwrap_err();
        assert!(matches!(err, BridgeError::Full(2)));
    }

    #[test]
    fn bridge_mixed_types() {
        let schema = make_schema(vec![
            ("flag", DataType::Boolean, false),
            ("count", DataType::UInt32, false),
            ("name", DataType::Utf8, true),
            ("ts", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]);
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let mut bridge = RowBatchBridge::new(schema, 16).unwrap();

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &row_schema, 64);
        row.set_bool(0, true);
        row.set_u32(1, 42);
        row.set_str(2, "test");
        row.set_i64(3, 1_000_000);
        bridge.append_row(&row.freeze()).unwrap();

        let batch = bridge.flush();
        assert_eq!(batch.num_rows(), 1);

        let bools = batch.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bools.value(0));

        let uints = batch.column(1).as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(uints.value(0), 42);

        let strs = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strs.value(0), "test");
    }

    #[test]
    fn bridge_null_propagation() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, true),
            ("b", DataType::Utf8, true),
        ]);
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let mut bridge = RowBatchBridge::new(schema, 16).unwrap();

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &row_schema, 64);
        row.set_null(0, true);
        row.set_str(1, "hello");
        bridge.append_row(&row.freeze()).unwrap();

        let mut row2 = MutableEventRow::new_in(&arena, &row_schema, 64);
        row2.set_i64(0, 99);
        row2.set_null(1, true);
        bridge.append_row(&row2.freeze()).unwrap();

        let batch = bridge.flush();
        assert_eq!(batch.num_rows(), 2);

        let ints = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(ints.is_null(0));
        assert_eq!(ints.value(1), 99);

        let strs = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strs.value(0), "hello");
        assert!(strs.is_null(1));
    }

    #[test]
    fn bridge_flush_resets() {
        let schema = make_schema(vec![("x", DataType::Int64, false)]);
        let row_schema = RowSchema::from_arrow(&schema).unwrap();
        let mut bridge = RowBatchBridge::new(schema, 4).unwrap();

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &row_schema, 0);
        row.set_i64(0, 1);
        bridge.append_row(&row.freeze()).unwrap();

        let batch1 = bridge.flush();
        assert_eq!(batch1.num_rows(), 1);
        assert_eq!(bridge.row_count(), 0);
        assert!(!bridge.is_full());

        // Second batch.
        let mut row = MutableEventRow::new_in(&arena, &row_schema, 0);
        row.set_i64(0, 2);
        bridge.append_row(&row.freeze()).unwrap();

        let batch2 = bridge.flush();
        assert_eq!(batch2.num_rows(), 1);
        let col = batch2.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(col.value(0), 2);
    }

    #[test]
    fn bridge_empty_flush() {
        let schema = make_schema(vec![("x", DataType::Int64, false)]);
        let mut bridge = RowBatchBridge::new(schema, 4).unwrap();

        let batch = bridge.flush();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 1);
    }
}
