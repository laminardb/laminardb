//! JSON format encoder implementing [`FormatEncoder`].
//!
//! Converts Arrow `RecordBatch`es into JSON byte payloads (one JSON
//! object per row).

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef, TimeUnit};

use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::traits::FormatEncoder;

/// Encodes Arrow `RecordBatch`es into JSON byte records.
///
/// Each row becomes one JSON object serialized as UTF-8 bytes.
#[derive(Debug)]
pub struct JsonEncoder {
    schema: SchemaRef,
}

impl JsonEncoder {
    /// Creates a new JSON encoder for the given schema.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl FormatEncoder for JsonEncoder {
    fn input_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn encode_batch(&self, batch: &RecordBatch) -> SchemaResult<Vec<Vec<u8>>> {
        let num_rows = batch.num_rows();
        let mut output = Vec::with_capacity(num_rows);

        for row in 0..num_rows {
            let mut obj = serde_json::Map::with_capacity(batch.num_columns());

            for (col_idx, field) in self.schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let value = if col.is_null(row) {
                    serde_json::Value::Null
                } else {
                    column_value_to_json(col, row, field.data_type())?
                };
                obj.insert(field.name().clone(), value);
            }

            let bytes = serde_json::to_vec(&serde_json::Value::Object(obj))
                .map_err(|e| SchemaError::DecodeError(format!("JSON encode error: {e}")))?;
            output.push(bytes);
        }

        Ok(output)
    }

    fn format_name(&self) -> &'static str {
        "json"
    }
}

fn column_value_to_json(
    col: &Arc<dyn arrow_array::Array>,
    row: usize,
    data_type: &DataType,
) -> SchemaResult<serde_json::Value> {
    Ok(match data_type {
        DataType::Boolean => serde_json::Value::Bool(col.as_boolean().value(row)),
        DataType::Int32 => serde_json::Value::Number(
            col.as_primitive::<arrow_array::types::Int32Type>()
                .value(row)
                .into(),
        ),
        DataType::Int64 => serde_json::Value::Number(
            col.as_primitive::<arrow_array::types::Int64Type>()
                .value(row)
                .into(),
        ),
        DataType::Float32 => {
            let f = f64::from(
                col.as_primitive::<arrow_array::types::Float32Type>()
                    .value(row),
            );
            serde_json::Number::from_f64(f)
                .map_or(serde_json::Value::Null, serde_json::Value::Number)
        }
        DataType::Float64 => {
            let f = col
                .as_primitive::<arrow_array::types::Float64Type>()
                .value(row);
            serde_json::Number::from_f64(f)
                .map_or(serde_json::Value::Null, serde_json::Value::Number)
        }
        DataType::Utf8 => serde_json::Value::String(col.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => {
            serde_json::Value::String(col.as_string::<i64>().value(row).to_string())
        }
        DataType::LargeBinary => {
            // Attempt to parse as JSON; fallback to opaque binary string.
            let bytes = col.as_binary::<i64>().value(row);
            match serde_json::from_slice::<serde_json::Value>(bytes) {
                Ok(v) => v,
                Err(_) => serde_json::Value::String(format!("<binary:{} bytes>", bytes.len())),
            }
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let nanos = col
                .as_primitive::<arrow_array::types::TimestampNanosecondType>()
                .value(row);
            // Convert nanos to RFC 3339 string.
            // `rem_euclid` guarantees a non-negative remainder in [0, 1_000_000_000),
            // which always fits in u32.
            let secs = nanos.div_euclid(1_000_000_000);
            #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
            let nsec = nanos.rem_euclid(1_000_000_000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsec) {
                serde_json::Value::String(dt.to_rfc3339())
            } else {
                serde_json::Value::Number(nanos.into())
            }
        }
        // Fallback: use Arrow's string representation.
        _ => {
            let arr_str = arrow_cast::display::ArrayFormatter::try_new(
                col.as_ref(),
                &arrow_cast::display::FormatOptions::default(),
            )
            .map_err(|e| SchemaError::DecodeError(format!("format error: {e}")))?;
            serde_json::Value::String(arr_str.value(row).to_string())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::*;
    use arrow_schema::{Field, Schema};

    fn make_schema(fields: Vec<(&str, DataType, bool)>) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dt, nullable)| Field::new(name, dt, nullable))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn test_encode_basic() {
        let schema = make_schema(vec![
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, false),
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            ],
        )
        .unwrap();

        let encoder = JsonEncoder::new(schema);
        let records = encoder.encode_batch(&batch).unwrap();

        assert_eq!(records.len(), 2);

        let v0: serde_json::Value = serde_json::from_slice(&records[0]).unwrap();
        assert_eq!(v0["id"], 1);
        assert_eq!(v0["name"], "Alice");

        let v1: serde_json::Value = serde_json::from_slice(&records[1]).unwrap();
        assert_eq!(v1["id"], 2);
        assert_eq!(v1["name"], "Bob");
    }

    #[test]
    fn test_encode_nulls() {
        let schema = make_schema(vec![("value", DataType::Int64, true)]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();

        let encoder = JsonEncoder::new(schema);
        let records = encoder.encode_batch(&batch).unwrap();

        let v0: serde_json::Value = serde_json::from_slice(&records[0]).unwrap();
        assert_eq!(v0["value"], 1);

        let v1: serde_json::Value = serde_json::from_slice(&records[1]).unwrap();
        assert!(v1["value"].is_null());
    }

    #[test]
    fn test_encode_all_types() {
        let schema = make_schema(vec![
            ("bool_col", DataType::Boolean, false),
            ("int_col", DataType::Int64, false),
            ("float_col", DataType::Float64, false),
            ("str_col", DataType::Utf8, false),
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(Float64Array::from(vec![3.14])),
                Arc::new(StringArray::from(vec!["hello"])),
            ],
        )
        .unwrap();

        let encoder = JsonEncoder::new(schema);
        let records = encoder.encode_batch(&batch).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&records[0]).unwrap();

        assert_eq!(v["bool_col"], true);
        assert_eq!(v["int_col"], 42);
        assert!((v["float_col"].as_f64().unwrap() - 3.14).abs() < f64::EPSILON);
        assert_eq!(v["str_col"], "hello");
    }

    #[test]
    fn test_encode_empty_batch() {
        let schema = make_schema(vec![("x", DataType::Int64, false)]);
        let batch = RecordBatch::new_empty(schema.clone());
        let encoder = JsonEncoder::new(schema);
        let records = encoder.encode_batch(&batch).unwrap();
        assert!(records.is_empty());
    }
}
