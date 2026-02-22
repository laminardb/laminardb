//! JSON serialization and deserialization.
//!
//! Converts between JSON records and Arrow `RecordBatch` using `serde_json`.

use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    Int8Builder, StringBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use serde_json::Value;

use super::{Format, RecordDeserializer, RecordSerializer};
use crate::error::SerdeError;

/// JSON record deserializer.
///
/// Parses JSON objects and maps fields to Arrow columns based on the
/// provided schema. Supports:
/// - All integer types (i8-i64, u8-u64)
/// - Float types (f32, f64)
/// - Boolean
/// - Utf8 (String)
/// - Nullable fields
#[derive(Debug, Clone)]
pub struct JsonDeserializer {
    _private: (),
}

impl JsonDeserializer {
    /// Creates a new JSON deserializer.
    #[must_use]
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Default for JsonDeserializer {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonDeserializer {
    /// Deserializes a pre-parsed JSON [`Value`] into a [`RecordBatch`].
    ///
    /// Avoids the serialize-then-reparse overhead when the caller already
    /// holds a parsed `Value` (e.g. Debezium envelope extraction).
    ///
    /// # Errors
    ///
    /// Returns `SerdeError` if the value is not a JSON object, a required
    /// field is missing, or Arrow array construction fails.
    pub fn deserialize_value(
        &self,
        value: &Value,
        schema: &SchemaRef,
    ) -> Result<RecordBatch, SerdeError> {
        let obj = value
            .as_object()
            .ok_or_else(|| SerdeError::MalformedInput("expected JSON object".into()))?;

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        for field in schema.fields() {
            let json_val = obj.get(field.name());

            let is_null = json_val.is_none() || json_val == Some(&Value::Null);

            if is_null && !field.is_nullable() {
                return Err(SerdeError::MissingField(field.name().clone()));
            }

            let array = build_array_from_json(field.data_type(), json_val, field.name())?;
            columns.push(array);
        }

        RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| SerdeError::MalformedInput(format!("failed to create RecordBatch: {e}")))
    }
}

impl RecordDeserializer for JsonDeserializer {
    fn deserialize(&self, data: &[u8], schema: &SchemaRef) -> Result<RecordBatch, SerdeError> {
        let value: Value = serde_json::from_slice(data)?;
        self.deserialize_value(&value, schema)
    }

    fn deserialize_batch(
        &self,
        records: &[&[u8]],
        schema: &SchemaRef,
    ) -> Result<RecordBatch, SerdeError> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }

        // Parse all JSON values first
        let values: Vec<Value> = records
            .iter()
            .map(|data| serde_json::from_slice(data).map_err(SerdeError::from))
            .collect::<Result<Vec<_>, _>>()?;

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        for field in schema.fields() {
            let array = build_column_from_json_values(
                field.data_type(),
                field.name(),
                &values,
                field.is_nullable(),
            )?;
            columns.push(array);
        }

        RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| SerdeError::MalformedInput(format!("failed to create RecordBatch: {e}")))
    }

    fn format(&self) -> Format {
        Format::Json
    }
}

/// JSON record serializer.
///
/// Converts Arrow `RecordBatch` rows to JSON objects.
#[derive(Debug, Clone)]
pub struct JsonSerializer {
    _private: (),
}

impl JsonSerializer {
    /// Creates a new JSON serializer.
    #[must_use]
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Default for JsonSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordSerializer for JsonSerializer {
    fn serialize(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SerdeError> {
        let mut records = Vec::with_capacity(batch.num_rows());
        let schema = batch.schema();
        // Reuse a single Map across all rows (clear instead of re-allocate)
        let mut obj = serde_json::Map::with_capacity(schema.fields().len());

        for row in 0..batch.num_rows() {
            obj.clear();

            for (col_idx, field) in schema.fields().iter().enumerate() {
                let column = batch.column(col_idx);

                if column.is_null(row) {
                    obj.insert(field.name().clone(), Value::Null);
                    continue;
                }

                let value = arrow_column_to_json(column, row, field.data_type())?;
                obj.insert(field.name().clone(), value);
            }

            let json_bytes = serde_json::to_vec(&obj)
                .map_err(|e| SerdeError::Json(e.to_string()))?;
            records.push(json_bytes);
        }

        Ok(records)
    }

    fn serialize_batch(&self, batch: &RecordBatch) -> Result<Vec<u8>, SerdeError> {
        let schema = batch.schema();
        // Estimate ~256 bytes per row for capacity hint
        let mut buf = Vec::with_capacity(batch.num_rows() * 256);
        let mut obj = serde_json::Map::with_capacity(schema.fields().len());
        // Reusable per-row write buffer
        let mut row_buf: Vec<u8> = Vec::with_capacity(256);

        for row in 0..batch.num_rows() {
            obj.clear();

            for (col_idx, field) in schema.fields().iter().enumerate() {
                let column = batch.column(col_idx);

                if column.is_null(row) {
                    obj.insert(field.name().clone(), Value::Null);
                    continue;
                }

                let value = arrow_column_to_json(column, row, field.data_type())?;
                obj.insert(field.name().clone(), value);
            }

            row_buf.clear();
            serde_json::to_writer(&mut row_buf, &obj)
                .map_err(|e| SerdeError::Json(e.to_string()))?;
            buf.extend_from_slice(&row_buf);
            buf.push(b'\n');
        }

        Ok(buf)
    }

    fn format(&self) -> Format {
        Format::Json
    }
}

/// Builds a single-element Arrow array from a JSON value.
fn build_array_from_json(
    data_type: &DataType,
    value: Option<&Value>,
    field_name: &str,
) -> Result<ArrayRef, SerdeError> {
    match data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(1);
            match value {
                Some(Value::Bool(b)) => builder.append_value(*b),
                Some(Value::Null) | None => builder.append_null(),
                _ => {
                    return Err(SerdeError::TypeConversion {
                        field: field_name.into(),
                        expected: "Boolean".into(),
                        message: format!("got {value:?}"),
                    })
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int8 => build_int_array::<Int8Builder>(value, field_name, "Int8"),
        DataType::Int16 => build_int_array::<Int16Builder>(value, field_name, "Int16"),
        DataType::Int32 => build_int_array::<Int32Builder>(value, field_name, "Int32"),
        DataType::Int64 => build_int_array::<Int64Builder>(value, field_name, "Int64"),
        DataType::UInt8 => build_uint_array::<UInt8Builder>(value, field_name, "UInt8"),
        DataType::UInt16 => build_uint_array::<UInt16Builder>(value, field_name, "UInt16"),
        DataType::UInt32 => build_uint_array::<UInt32Builder>(value, field_name, "UInt32"),
        DataType::UInt64 => build_uint_array::<UInt64Builder>(value, field_name, "UInt64"),
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(1);
            match value {
                Some(Value::Number(n)) => {
                    let v = n.as_f64().ok_or_else(|| SerdeError::TypeConversion {
                        field: field_name.into(),
                        expected: "Float32".into(),
                        message: format!("cannot convert {n}"),
                    })?;
                    #[allow(clippy::cast_possible_truncation)]
                    // f64 → f32: precision loss acceptable for Float32 columns
                    builder.append_value(v as f32);
                }
                Some(Value::Null) | None => builder.append_null(),
                _ => {
                    return Err(SerdeError::TypeConversion {
                        field: field_name.into(),
                        expected: "Float32".into(),
                        message: format!("got {value:?}"),
                    })
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(1);
            match value {
                Some(Value::Number(n)) => {
                    let v = n.as_f64().ok_or_else(|| SerdeError::TypeConversion {
                        field: field_name.into(),
                        expected: "Float64".into(),
                        message: format!("cannot convert {n}"),
                    })?;
                    builder.append_value(v);
                }
                Some(Value::Null) | None => builder.append_null(),
                _ => {
                    return Err(SerdeError::TypeConversion {
                        field: field_name.into(),
                        expected: "Float64".into(),
                        message: format!("got {value:?}"),
                    })
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(1, 64);
            match value {
                Some(Value::String(s)) => builder.append_value(s),
                Some(Value::Null) | None => builder.append_null(),
                // Coerce non-string values to string representation
                Some(other) => builder.append_value(other.to_string()),
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(SerdeError::UnsupportedFormat(format!(
            "unsupported Arrow type for JSON deserialization: {other}"
        ))),
    }
}

/// Helper trait for building integer arrays from JSON.
trait IntBuilder: Default {
    type Native: TryFrom<i64>;
    fn append_value(&mut self, v: Self::Native);
    fn append_null(&mut self);
    fn finish_array(self) -> ArrayRef;
}

macro_rules! impl_int_builder {
    ($builder:ty, $native:ty) => {
        impl IntBuilder for $builder {
            type Native = $native;
            fn append_value(&mut self, v: Self::Native) {
                <$builder>::append_value(self, v);
            }
            fn append_null(&mut self) {
                <$builder>::append_null(self);
            }
            fn finish_array(mut self) -> ArrayRef {
                Arc::new(self.finish())
            }
        }
    };
}

impl_int_builder!(Int8Builder, i8);
impl_int_builder!(Int16Builder, i16);
impl_int_builder!(Int32Builder, i32);
impl_int_builder!(Int64Builder, i64);

trait UintBuilder: Default {
    type Native: TryFrom<u64>;
    fn append_value(&mut self, v: Self::Native);
    fn append_null(&mut self);
    fn finish_array(self) -> ArrayRef;
}

macro_rules! impl_uint_builder {
    ($builder:ty, $native:ty) => {
        impl UintBuilder for $builder {
            type Native = $native;
            fn append_value(&mut self, v: Self::Native) {
                <$builder>::append_value(self, v);
            }
            fn append_null(&mut self) {
                <$builder>::append_null(self);
            }
            fn finish_array(mut self) -> ArrayRef {
                Arc::new(self.finish())
            }
        }
    };
}

impl_uint_builder!(UInt8Builder, u8);
impl_uint_builder!(UInt16Builder, u16);
impl_uint_builder!(UInt32Builder, u32);
impl_uint_builder!(UInt64Builder, u64);

fn build_int_array<B: IntBuilder>(
    value: Option<&Value>,
    field_name: &str,
    type_name: &str,
) -> Result<ArrayRef, SerdeError>
where
    <B::Native as TryFrom<i64>>::Error: std::fmt::Display,
{
    let mut builder = B::default();
    match value {
        Some(Value::Number(n)) => {
            let i = n.as_i64().ok_or_else(|| SerdeError::TypeConversion {
                field: field_name.into(),
                expected: type_name.into(),
                message: format!("cannot convert {n} to i64"),
            })?;
            let native = B::Native::try_from(i).map_err(|e| SerdeError::TypeConversion {
                field: field_name.into(),
                expected: type_name.into(),
                message: format!("{e}"),
            })?;
            builder.append_value(native);
        }
        Some(Value::Null) | None => builder.append_null(),
        _ => {
            return Err(SerdeError::TypeConversion {
                field: field_name.into(),
                expected: type_name.into(),
                message: format!("got {value:?}"),
            })
        }
    }
    Ok(builder.finish_array())
}

fn build_uint_array<B: UintBuilder>(
    value: Option<&Value>,
    field_name: &str,
    type_name: &str,
) -> Result<ArrayRef, SerdeError>
where
    <B::Native as TryFrom<u64>>::Error: std::fmt::Display,
{
    let mut builder = B::default();
    match value {
        Some(Value::Number(n)) => {
            let u = n.as_u64().ok_or_else(|| SerdeError::TypeConversion {
                field: field_name.into(),
                expected: type_name.into(),
                message: format!("cannot convert {n} to u64"),
            })?;
            let native = B::Native::try_from(u).map_err(|e| SerdeError::TypeConversion {
                field: field_name.into(),
                expected: type_name.into(),
                message: format!("{e}"),
            })?;
            builder.append_value(native);
        }
        Some(Value::Null) | None => builder.append_null(),
        _ => {
            return Err(SerdeError::TypeConversion {
                field: field_name.into(),
                expected: type_name.into(),
                message: format!("got {value:?}"),
            })
        }
    }
    Ok(builder.finish_array())
}

/// Builds a multi-row column from a slice of JSON values.
#[allow(clippy::too_many_lines)]
fn build_column_from_json_values(
    data_type: &DataType,
    field_name: &str,
    values: &[Value],
    nullable: bool,
) -> Result<ArrayRef, SerdeError> {
    match data_type {
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(values.len());
            for value in values {
                let obj = value.as_object();
                let field_val = obj.and_then(|o| o.get(field_name));
                match field_val {
                    Some(Value::Number(n)) => {
                        let i = n.as_i64().ok_or_else(|| SerdeError::TypeConversion {
                            field: field_name.into(),
                            expected: "Int64".into(),
                            message: format!("cannot convert {n}"),
                        })?;
                        builder.append_value(i);
                    }
                    Some(Value::Null) | None if nullable => builder.append_null(),
                    Some(Value::Null) | None => {
                        return Err(SerdeError::MissingField(field_name.into()))
                    }
                    other => {
                        return Err(SerdeError::TypeConversion {
                            field: field_name.into(),
                            expected: "Int64".into(),
                            message: format!("got {other:?}"),
                        })
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(values.len());
            for value in values {
                let obj = value.as_object();
                let field_val = obj.and_then(|o| o.get(field_name));
                match field_val {
                    Some(Value::Number(n)) => {
                        let f = n.as_f64().ok_or_else(|| SerdeError::TypeConversion {
                            field: field_name.into(),
                            expected: "Float64".into(),
                            message: format!("cannot convert {n}"),
                        })?;
                        builder.append_value(f);
                    }
                    Some(Value::Null) | None if nullable => builder.append_null(),
                    Some(Value::Null) | None => {
                        return Err(SerdeError::MissingField(field_name.into()))
                    }
                    other => {
                        return Err(SerdeError::TypeConversion {
                            field: field_name.into(),
                            expected: "Float64".into(),
                            message: format!("got {other:?}"),
                        })
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 32);
            for value in values {
                let obj = value.as_object();
                let field_val = obj.and_then(|o| o.get(field_name));
                match field_val {
                    Some(Value::String(s)) => builder.append_value(s),
                    Some(Value::Null) | None if nullable => builder.append_null(),
                    Some(Value::Null) | None => {
                        return Err(SerdeError::MissingField(field_name.into()))
                    }
                    Some(other) => builder.append_value(other.to_string()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for value in values {
                let obj = value.as_object();
                let field_val = obj.and_then(|o| o.get(field_name));
                match field_val {
                    Some(Value::Bool(b)) => builder.append_value(*b),
                    Some(Value::Null) | None if nullable => builder.append_null(),
                    Some(Value::Null) | None => {
                        return Err(SerdeError::MissingField(field_name.into()))
                    }
                    other => {
                        return Err(SerdeError::TypeConversion {
                            field: field_name.into(),
                            expected: "Boolean".into(),
                            message: format!("got {other:?}"),
                        })
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        // Fall back to per-row deserialization for other types
        _ => {
            let arrays: Result<Vec<ArrayRef>, _> = values
                .iter()
                .map(|v| {
                    let field_val = v.as_object().and_then(|o| o.get(field_name));
                    build_array_from_json(data_type, field_val, field_name)
                })
                .collect();
            let arrays = arrays?;
            if arrays.len() == 1 {
                return Ok(arrays.into_iter().next().unwrap());
            }
            Err(SerdeError::UnsupportedFormat(format!(
                "batch deserialization not supported for type {data_type}"
            )))
        }
    }
}

/// Converts an Arrow column value at `row` to a JSON value.
fn arrow_column_to_json(
    column: &ArrayRef,
    row: usize,
    data_type: &DataType,
) -> Result<Value, SerdeError> {
    use arrow_array::{
        BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };

    match data_type {
        DataType::Boolean => {
            let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(Value::Bool(arr.value(row)))
        }
        DataType::Int8 => {
            let arr = column.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(Value::Number(i64::from(arr.value(row)).into()))
        }
        DataType::Int16 => {
            let arr = column.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(Value::Number(i64::from(arr.value(row)).into()))
        }
        DataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(Value::Number(i64::from(arr.value(row)).into()))
        }
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(Value::Number(arr.value(row).into()))
        }
        DataType::UInt8 => {
            let arr = column.as_any().downcast_ref::<UInt8Array>().unwrap();
            Ok(Value::Number(u64::from(arr.value(row)).into()))
        }
        DataType::UInt16 => {
            let arr = column.as_any().downcast_ref::<UInt16Array>().unwrap();
            Ok(Value::Number(u64::from(arr.value(row)).into()))
        }
        DataType::UInt32 => {
            let arr = column.as_any().downcast_ref::<UInt32Array>().unwrap();
            Ok(Value::Number(u64::from(arr.value(row)).into()))
        }
        DataType::UInt64 => {
            let arr = column.as_any().downcast_ref::<UInt64Array>().unwrap();
            Ok(Value::Number(arr.value(row).into()))
        }
        DataType::Float32 => {
            let arr = column.as_any().downcast_ref::<Float32Array>().unwrap();
            let v = f64::from(arr.value(row));
            Ok(serde_json::Number::from_f64(v).map_or(Value::Null, Value::Number))
        }
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(serde_json::Number::from_f64(arr.value(row)).map_or(Value::Null, Value::Number))
        }
        DataType::Utf8 => {
            let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(Value::String(arr.value(row).to_string()))
        }
        other => Err(SerdeError::UnsupportedFormat(format!(
            "unsupported Arrow type for JSON serialization: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, true),
        ]))
    }

    #[test]
    fn test_json_deserialize_basic() {
        let deser = JsonDeserializer::new();
        let schema = test_schema();
        let data = br#"{"id": 1, "name": "Alice", "score": 95.5}"#;

        let batch = deser.deserialize(data, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
    }

    #[test]
    fn test_json_deserialize_null_field() {
        let deser = JsonDeserializer::new();
        let schema = test_schema();
        let data = br#"{"id": 2, "name": "Bob", "score": null}"#;

        let batch = deser.deserialize(data, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column(2).is_null(0));
    }

    #[test]
    fn test_json_deserialize_missing_required() {
        let deser = JsonDeserializer::new();
        let schema = test_schema();
        let data = br#"{"id": 3, "score": 80.0}"#;

        let result = deser.deserialize(data, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_json_serialize_roundtrip() {
        let deser = JsonDeserializer::new();
        let ser = JsonSerializer::new();
        let schema = test_schema();

        let data = br#"{"id": 42, "name": "Charlie", "score": 88.5}"#;
        let batch = deser.deserialize(data, &schema).unwrap();

        let serialized = ser.serialize(&batch).unwrap();
        assert_eq!(serialized.len(), 1);

        let roundtrip: Value = serde_json::from_slice(&serialized[0]).unwrap();
        assert_eq!(roundtrip["id"], 42);
        assert_eq!(roundtrip["name"], "Charlie");
    }

    #[test]
    fn test_json_deserialize_batch() {
        let deser = JsonDeserializer::new();
        let schema = test_schema();

        let r1 = br#"{"id": 1, "name": "A", "score": 10.0}"#;
        let r2 = br#"{"id": 2, "name": "B", "score": 20.0}"#;
        let records: Vec<&[u8]> = vec![r1, r2];

        let batch = deser.deserialize_batch(&records, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_json_serialize_batch_ndjson() {
        let deser = JsonDeserializer::new();
        let ser = JsonSerializer::new();
        let schema = test_schema();

        let r1 = br#"{"id": 1, "name": "A", "score": 10.0}"#;
        let r2 = br#"{"id": 2, "name": "B", "score": 20.0}"#;
        let records: Vec<&[u8]> = vec![r1, r2];
        let batch = deser.deserialize_batch(&records, &schema).unwrap();

        let ndjson = ser.serialize_batch(&batch).unwrap();
        let lines: Vec<&str> = std::str::from_utf8(&ndjson)
            .unwrap()
            .lines()
            .filter(|l| !l.is_empty())
            .collect();
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn test_json_type_coercion() {
        let deser = JsonDeserializer::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));

        // Numbers should be coerced to string
        let data = br#"{"value": 42}"#;
        let batch = deser.deserialize(data, &schema).unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "42");
    }
}
