//! JSON format encoder implementing [`FormatEncoder`].

use std::io::Write as _;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use arrow_json::writer::{
    Encoder, EncoderFactory, EncoderOptions, LineDelimited, NullableEncoder, WriterBuilder,
};
use arrow_schema::{ArrowError, DataType, FieldRef, SchemaRef};

use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::traits::FormatEncoder;

/// Encodes Arrow `RecordBatch`es into one JSON object per row via
/// `arrow_json::writer`. `LargeBinary` columns are inlined as JSON
/// when valid (JSONB passthrough), matching `CollectExtra` semantics.
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
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }

        let mut buf = Vec::new();
        {
            let mut writer = WriterBuilder::new()
                .with_explicit_nulls(true)
                .with_encoder_factory(Arc::new(JsonbPassthroughFactory))
                .build::<_, LineDelimited>(&mut buf);
            writer
                .write(batch)
                .map_err(|e| SchemaError::DecodeError(format!("JSON encode error: {e}")))?;
            writer
                .finish()
                .map_err(|e| SchemaError::DecodeError(format!("JSON finish error: {e}")))?;
        }

        let output: Vec<Vec<u8>> = buf
            .split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
            .map(<[u8]>::to_vec)
            .collect();

        Ok(output)
    }

    fn format_name(&self) -> &'static str {
        "json"
    }
}

/// Inlines `LargeBinary` as raw JSON when valid; falls through otherwise.
#[derive(Debug)]
struct JsonbPassthroughFactory;

impl EncoderFactory for JsonbPassthroughFactory {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a FieldRef,
        array: &'a dyn Array,
        _options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        if *array.data_type() != DataType::LargeBinary {
            return Ok(None);
        }
        let binary_array = array.as_binary::<i64>();
        let nulls = binary_array.nulls().cloned();
        let encoder = LargeBinaryJsonbEncoder {
            array: binary_array,
        };
        Ok(Some(NullableEncoder::new(Box::new(encoder), nulls)))
    }
}

struct LargeBinaryJsonbEncoder<'a> {
    array: &'a arrow_array::LargeBinaryArray,
}

impl Encoder for LargeBinaryJsonbEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let bytes = self.array.value(idx);
        if serde_json::from_slice::<serde_json::Value>(bytes).is_ok() {
            out.extend_from_slice(bytes);
        } else {
            write!(out, "\"<binary:{} bytes>\"", bytes.len()).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::*;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

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
    fn test_encode_nulls_explicit() {
        let schema = make_schema(vec![
            ("a", DataType::Int64, true),
            ("b", DataType::Utf8, true),
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec![None, Some("x")])),
            ],
        )
        .unwrap();

        let encoder = JsonEncoder::new(schema);
        let records = encoder.encode_batch(&batch).unwrap();

        // Row 0: a=1, b=null — null key must be present.
        let v0: serde_json::Value = serde_json::from_slice(&records[0]).unwrap();
        assert_eq!(v0["a"], 1);
        assert!(
            v0.get("b").unwrap().is_null(),
            "null key 'b' must be present"
        );

        // Row 1: a=null, b="x" — null key must be present.
        let v1: serde_json::Value = serde_json::from_slice(&records[1]).unwrap();
        assert!(
            v1.get("a").unwrap().is_null(),
            "null key 'a' must be present"
        );
        assert_eq!(v1["b"], "x");
    }

    #[test]
    fn test_encode_large_binary_jsonb() {
        let schema = make_schema(vec![
            ("name", DataType::Utf8, false),
            ("extra", DataType::LargeBinary, true),
        ]);

        let json_bytes = br#"{"nested":"value","count":42}"#;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                Arc::new(LargeBinaryArray::from(vec![
                    Some(json_bytes.as_ref()),
                    None,
                ])),
            ],
        )
        .unwrap();

        let encoder = JsonEncoder::new(schema);
        let records = encoder.encode_batch(&batch).unwrap();

        // Row 0: extra must be inlined as a JSON object, not hex-encoded.
        let v0: serde_json::Value = serde_json::from_slice(&records[0]).unwrap();
        assert_eq!(v0["name"], "Alice");
        assert!(v0["extra"].is_object(), "JSONB must be inlined as object");
        assert_eq!(v0["extra"]["nested"], "value");
        assert_eq!(v0["extra"]["count"], 42);

        // Row 1: null extra.
        let v1: serde_json::Value = serde_json::from_slice(&records[1]).unwrap();
        assert!(v1["extra"].is_null());
    }

    #[test]
    fn test_encode_large_binary_non_json() {
        let schema = make_schema(vec![("data", DataType::LargeBinary, false)]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(LargeBinaryArray::from(vec![
                b"\x00\x01\x02".as_ref()
            ]))],
        )
        .unwrap();

        let encoder = JsonEncoder::new(schema);
        let records = encoder.encode_batch(&batch).unwrap();

        let v: serde_json::Value = serde_json::from_slice(&records[0]).unwrap();
        assert_eq!(v["data"], "<binary:3 bytes>");
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

    #[test]
    fn test_encode_timestamp() {
        let schema = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            false,
        )]);

        let nanos = 1_736_936_600_000_000_000_i64;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                TimestampNanosecondArray::from(vec![nanos]).with_timezone("+00:00"),
            )],
        )
        .unwrap();

        let encoder = JsonEncoder::new(schema);
        let records = encoder.encode_batch(&batch).unwrap();
        assert_eq!(records.len(), 1);

        let v: serde_json::Value = serde_json::from_slice(&records[0]).unwrap();
        assert!(v["ts"].is_string());
    }
}
