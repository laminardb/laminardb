//! Bridge adapters between legacy serde traits and the new schema traits.
//!
//! - [`DeserializerDecoder`] wraps a [`RecordDeserializer`] into a [`FormatDecoder`]
//! - [`SerializerEncoder`] wraps a [`RecordSerializer`] into a [`FormatEncoder`]
//!
//! This enables gradual migration: existing connectors that already have
//! `RecordDeserializer`/`RecordSerializer` implementations can be used
//! through the new schema framework without rewriting.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use super::error::{SchemaError, SchemaResult};
use super::traits::{FormatDecoder, FormatEncoder};
use super::types::RawRecord;
use crate::serde::{RecordDeserializer, RecordSerializer};

/// Adapts a [`RecordDeserializer`] to the [`FormatDecoder`] interface.
///
/// Wraps an existing `RecordDeserializer` and a schema so it can be used
/// wherever a `FormatDecoder` is expected.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_connectors::serde::json::JsonDeserializer;
/// use laminar_connectors::schema::bridge::DeserializerDecoder;
///
/// let decoder = DeserializerDecoder::new(
///     Box::new(JsonDeserializer::new()),
///     my_schema,
///     "json",
/// );
/// let batch = decoder.decode_batch(&records)?;
/// ```
pub struct DeserializerDecoder {
    inner: Box<dyn RecordDeserializer>,
    schema: SchemaRef,
    format: String,
}

impl DeserializerDecoder {
    /// Creates a new bridge decoder.
    pub fn new(
        deserializer: Box<dyn RecordDeserializer>,
        schema: SchemaRef,
        format: impl Into<String>,
    ) -> Self {
        Self {
            inner: deserializer,
            schema,
            format: format.into(),
        }
    }
}

impl std::fmt::Debug for DeserializerDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeserializerDecoder")
            .field("format", &self.format)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl FormatDecoder for DeserializerDecoder {
    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let slices: Vec<&[u8]> = records.iter().map(|r| r.value.as_slice()).collect();
        self.inner
            .deserialize_batch(&slices, &self.schema)
            .map_err(|e| SchemaError::DecodeError(e.to_string()))
    }

    fn decode_one(&self, record: &RawRecord) -> SchemaResult<RecordBatch> {
        self.inner
            .deserialize(&record.value, &self.schema)
            .map_err(|e| SchemaError::DecodeError(e.to_string()))
    }

    fn format_name(&self) -> &str {
        &self.format
    }
}

/// Adapts a [`RecordSerializer`] to the [`FormatEncoder`] interface.
///
/// Wraps an existing `RecordSerializer` and a schema so it can be used
/// wherever a `FormatEncoder` is expected.
pub struct SerializerEncoder {
    inner: Box<dyn RecordSerializer>,
    schema: SchemaRef,
    format: String,
}

impl SerializerEncoder {
    /// Creates a new bridge encoder.
    pub fn new(
        serializer: Box<dyn RecordSerializer>,
        schema: SchemaRef,
        format: impl Into<String>,
    ) -> Self {
        Self {
            inner: serializer,
            schema,
            format: format.into(),
        }
    }
}

impl std::fmt::Debug for SerializerEncoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerializerEncoder")
            .field("format", &self.format)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl FormatEncoder for SerializerEncoder {
    fn input_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn encode_batch(&self, batch: &RecordBatch) -> SchemaResult<Vec<Vec<u8>>> {
        self.inner
            .serialize(batch)
            .map_err(|e| SchemaError::DecodeError(e.to_string()))
    }

    fn format_name(&self) -> &str {
        &self.format
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::json::{JsonDeserializer, JsonSerializer};
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    // ── DeserializerDecoder tests ──────────────────────────────

    #[test]
    fn test_decoder_output_schema() {
        let schema = test_schema();
        let decoder =
            DeserializerDecoder::new(Box::new(JsonDeserializer::new()), schema.clone(), "json");
        assert_eq!(decoder.output_schema(), schema);
        assert_eq!(decoder.format_name(), "json");
    }

    #[test]
    fn test_decoder_debug() {
        let decoder =
            DeserializerDecoder::new(Box::new(JsonDeserializer::new()), test_schema(), "json");
        let dbg = format!("{decoder:?}");
        assert!(dbg.contains("DeserializerDecoder"));
        assert!(dbg.contains("json"));
    }

    #[test]
    fn test_decoder_decode_one() {
        let schema = test_schema();
        let decoder = DeserializerDecoder::new(Box::new(JsonDeserializer::new()), schema, "json");

        let record = RawRecord::new(br#"{"id": 42, "name": "Alice"}"#.to_vec());
        let batch = decoder.decode_one(&record).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_decoder_decode_batch() {
        let schema = test_schema();
        let decoder = DeserializerDecoder::new(Box::new(JsonDeserializer::new()), schema, "json");

        let records = vec![
            RawRecord::new(br#"{"id": 1, "name": "A"}"#.to_vec()),
            RawRecord::new(br#"{"id": 2, "name": "B"}"#.to_vec()),
        ];

        let batch = decoder.decode_batch(&records).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_decoder_decode_empty() {
        let schema = test_schema();
        let decoder =
            DeserializerDecoder::new(Box::new(JsonDeserializer::new()), schema.clone(), "json");

        let batch = decoder.decode_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), schema);
    }

    #[test]
    fn test_decoder_decode_error() {
        let schema = test_schema();
        let decoder = DeserializerDecoder::new(Box::new(JsonDeserializer::new()), schema, "json");

        let record = RawRecord::new(b"not json".to_vec());
        let result = decoder.decode_one(&record);
        assert!(result.is_err());
    }

    // ── SerializerEncoder tests ────────────────────────────────

    #[test]
    fn test_encoder_input_schema() {
        let schema = test_schema();
        let encoder =
            SerializerEncoder::new(Box::new(JsonSerializer::new()), schema.clone(), "json");
        assert_eq!(encoder.input_schema(), schema);
        assert_eq!(encoder.format_name(), "json");
    }

    #[test]
    fn test_encoder_debug() {
        let encoder =
            SerializerEncoder::new(Box::new(JsonSerializer::new()), test_schema(), "json");
        let dbg = format!("{encoder:?}");
        assert!(dbg.contains("SerializerEncoder"));
    }

    #[test]
    fn test_encoder_encode_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let encoder = SerializerEncoder::new(Box::new(JsonSerializer::new()), schema, "json");

        let output = encoder.encode_batch(&batch).unwrap();
        assert_eq!(output.len(), 3);

        // Each encoded record should be valid JSON.
        for bytes in &output {
            let _: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        }
    }

    // ── Round-trip test ────────────────────────────────────────

    #[test]
    fn test_roundtrip_through_bridge() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));

        // Encode.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20]))],
        )
        .unwrap();

        let encoder =
            SerializerEncoder::new(Box::new(JsonSerializer::new()), schema.clone(), "json");
        let output = encoder.encode_batch(&batch).unwrap();

        // Decode.
        let decoder = DeserializerDecoder::new(Box::new(JsonDeserializer::new()), schema, "json");
        let records: Vec<RawRecord> = output.into_iter().map(RawRecord::new).collect();
        let result_batch = decoder.decode_batch(&records).unwrap();

        assert_eq!(result_batch.num_rows(), 2);
        let col = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 10);
        assert_eq!(col.value(1), 20);
    }
}
