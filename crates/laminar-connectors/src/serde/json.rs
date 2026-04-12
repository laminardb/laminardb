//! JSON serialization and deserialization.
//!
//! Implements [`RecordDeserializer`] / [`RecordSerializer`] by delegating
//! to [`JsonDecoder`] and [`JsonEncoder`].

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use serde_json::Value;

use super::{Format, RecordDeserializer, RecordSerializer};
use crate::error::SerdeError;
use crate::schema::json::decoder::JsonDecoder;
use crate::schema::json::encoder::JsonEncoder;
use crate::schema::traits::{FormatDecoder, FormatEncoder};
use crate::schema::types::RawRecord;

/// JSON record deserializer. Delegates to [`JsonDecoder`].
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

    /// Deserializes a pre-parsed JSON [`Value`] into a [`RecordBatch`].
    ///
    /// Used by [`DebeziumDeserializer`](super::debezium::DebeziumDeserializer)
    /// to avoid double-parsing the envelope.
    ///
    /// # Errors
    ///
    /// Returns `SerdeError` if the value cannot be decoded.
    pub fn deserialize_value(
        &self,
        value: &Value,
        schema: &SchemaRef,
    ) -> Result<RecordBatch, SerdeError> {
        let bytes = serde_json::to_vec(value).map_err(|e| SerdeError::Json(e.to_string()))?;
        self.deserialize(&bytes, schema)
    }
}

impl Default for JsonDeserializer {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordDeserializer for JsonDeserializer {
    fn deserialize(&self, data: &[u8], schema: &SchemaRef) -> Result<RecordBatch, SerdeError> {
        let decoder = JsonDecoder::new(schema.clone());
        let record = RawRecord::new(data.to_vec());
        decoder
            .decode_one(&record)
            .map_err(|e| SerdeError::Json(e.to_string()))
    }

    fn deserialize_batch(
        &self,
        records: &[&[u8]],
        schema: &SchemaRef,
    ) -> Result<RecordBatch, SerdeError> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        let decoder = JsonDecoder::new(schema.clone());
        let raw_records: Vec<RawRecord> =
            records.iter().map(|r| RawRecord::new(r.to_vec())).collect();
        decoder
            .decode_batch(&raw_records)
            .map_err(|e| SerdeError::Json(e.to_string()))
    }

    fn format(&self) -> Format {
        Format::Json
    }
}

/// JSON record serializer. Delegates to [`JsonEncoder`].
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
        let encoder = JsonEncoder::new(batch.schema());
        encoder
            .encode_batch(batch)
            .map_err(|e| SerdeError::Json(e.to_string()))
    }

    fn format(&self) -> Format {
        Format::Json
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

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
    fn test_json_deserialize_coercion() {
        let deser = JsonDeserializer::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("qty", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));

        let data = br#"{"qty": "100", "price": "187.52"}"#;
        let batch = deser.deserialize(data, &schema).unwrap();

        let qty = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(qty.value(0), 100);
    }
}
