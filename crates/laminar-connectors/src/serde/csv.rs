//! CSV serialization and deserialization.
//!
//! Implements [`RecordDeserializer`] / [`RecordSerializer`] by delegating
//! to [`CsvDecoder`] and [`CsvEncoder`].

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use super::{Format, RecordDeserializer, RecordSerializer};
use crate::error::SerdeError;
use crate::schema::csv::{CsvDecoder, CsvDecoderConfig, CsvEncoder, CsvEncoderConfig};
use crate::schema::traits::{FormatDecoder, FormatEncoder};
use crate::schema::types::RawRecord;

/// CSV record deserializer. Delegates to [`CsvDecoder`].
#[derive(Debug, Clone)]
pub struct CsvDeserializer {
    delimiter: u8,
}

impl CsvDeserializer {
    /// Creates a new CSV deserializer with comma delimiter.
    #[must_use]
    pub fn new() -> Self {
        Self { delimiter: b',' }
    }

    /// Creates a CSV deserializer with a custom delimiter.
    #[must_use]
    pub fn with_delimiter(delimiter: u8) -> Self {
        Self { delimiter }
    }
}

impl Default for CsvDeserializer {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordDeserializer for CsvDeserializer {
    fn deserialize(&self, data: &[u8], schema: &SchemaRef) -> Result<RecordBatch, SerdeError> {
        let config = CsvDecoderConfig {
            delimiter: self.delimiter,
            has_header: false,
            ..CsvDecoderConfig::default()
        };
        let decoder = CsvDecoder::with_config(schema.clone(), config);
        let record = RawRecord::new(data.to_vec());
        decoder
            .decode_one(&record)
            .map_err(|e| SerdeError::Csv(e.to_string()))
    }

    fn format(&self) -> Format {
        Format::Csv
    }
}

/// CSV record serializer. Delegates to [`CsvEncoder`].
#[derive(Debug, Clone)]
pub struct CsvSerializer {
    delimiter: u8,
}

impl CsvSerializer {
    /// Creates a new CSV serializer with comma delimiter.
    #[must_use]
    pub fn new() -> Self {
        Self { delimiter: b',' }
    }

    /// Creates a CSV serializer with a custom delimiter.
    #[must_use]
    pub fn with_delimiter(delimiter: u8) -> Self {
        Self { delimiter }
    }
}

impl Default for CsvSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordSerializer for CsvSerializer {
    fn serialize(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SerdeError> {
        let config = CsvEncoderConfig {
            delimiter: self.delimiter,
            has_header: false,
        };
        let encoder = CsvEncoder::with_config(batch.schema(), config);
        encoder
            .encode_batch(batch)
            .map_err(|e| SerdeError::Csv(e.to_string()))
    }

    fn format(&self) -> Format {
        Format::Csv
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
    fn test_csv_deserialize_basic() {
        let deser = CsvDeserializer::new();
        let schema = test_schema();
        let data = b"1,Alice,95.5";

        let batch = deser.deserialize(data, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
    }

    #[test]
    fn test_csv_serialize_roundtrip() {
        let deser = CsvDeserializer::new();
        let ser = CsvSerializer::new();
        let schema = test_schema();

        let data = b"42,Charlie,88.5";
        let batch = deser.deserialize(data, &schema).unwrap();

        let serialized = ser.serialize(&batch).unwrap();
        assert_eq!(serialized.len(), 1);

        let line = std::str::from_utf8(&serialized[0]).unwrap();
        assert!(line.contains("42"));
        assert!(line.contains("Charlie"));
    }

    #[test]
    fn test_csv_null_handling() {
        let deser = CsvDeserializer::new();
        let schema = test_schema();
        let data = b"1,Bob,";

        let batch = deser.deserialize(data, &schema).unwrap();
        assert!(batch.column(2).is_null(0));
    }

    #[test]
    fn test_csv_quoted_fields() {
        let deser = CsvDeserializer::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("desc", DataType::Utf8, false),
        ]));
        let data = br#"1,"hello, world""#;

        let batch = deser.deserialize(data, &schema).unwrap();
        let descs = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(descs.value(0), "hello, world");
    }
}
