//! Raw bytes pass-through serialization.
//!
//! Treats each record as an opaque byte array. The schema must have
//! a single `Binary` or `Utf8` column.

use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::{Format, RecordDeserializer, RecordSerializer};
use crate::error::SerdeError;

/// The default schema for raw bytes: a single `Utf8` column named "value".
fn raw_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]))
}

/// Raw bytes deserializer.
///
/// Stores each record's bytes as a single `Utf8` value in a `RecordBatch`
/// with one column named "value". The provided schema is ignored in favor
/// of the raw schema.
#[derive(Debug, Clone)]
pub struct RawBytesDeserializer {
    _private: (),
}

impl RawBytesDeserializer {
    /// Creates a new raw bytes deserializer.
    #[must_use]
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Default for RawBytesDeserializer {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordDeserializer for RawBytesDeserializer {
    fn deserialize(&self, data: &[u8], _schema: &SchemaRef) -> Result<RecordBatch, SerdeError> {
        let s = std::str::from_utf8(data)
            .map_err(|e| SerdeError::MalformedInput(format!("invalid UTF-8: {e}")))?;
        let mut builder = StringBuilder::with_capacity(1, data.len());
        builder.append_value(s);

        let schema = raw_schema();
        RecordBatch::try_new(schema, vec![Arc::new(builder.finish())])
            .map_err(|e| SerdeError::MalformedInput(format!("failed to create batch: {e}")))
    }

    fn deserialize_batch(
        &self,
        records: &[&[u8]],
        _schema: &SchemaRef,
    ) -> Result<RecordBatch, SerdeError> {
        let total_bytes: usize = records.iter().map(|r| r.len()).sum();
        let mut builder = StringBuilder::with_capacity(records.len(), total_bytes);

        for data in records {
            let s = std::str::from_utf8(data)
                .map_err(|e| SerdeError::MalformedInput(format!("invalid UTF-8: {e}")))?;
            builder.append_value(s);
        }

        let schema = raw_schema();
        RecordBatch::try_new(schema, vec![Arc::new(builder.finish())])
            .map_err(|e| SerdeError::MalformedInput(format!("failed to create batch: {e}")))
    }

    fn format(&self) -> Format {
        Format::Raw
    }
}

/// Raw bytes serializer.
///
/// Extracts the first column of each row as UTF-8 bytes.
#[derive(Debug, Clone)]
pub struct RawBytesSerializer {
    _private: (),
}

impl RawBytesSerializer {
    /// Creates a new raw bytes serializer.
    #[must_use]
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Default for RawBytesSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordSerializer for RawBytesSerializer {
    fn serialize(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SerdeError> {
        if batch.num_columns() == 0 {
            return Ok(Vec::new());
        }

        let column = batch.column(0);
        let string_arr = column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                SerdeError::UnsupportedFormat("raw serializer expects Utf8 column".into())
            })?;

        let mut records = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            if string_arr.is_null(i) {
                records.push(Vec::new());
            } else {
                records.push(string_arr.value(i).as_bytes().to_vec());
            }
        }
        Ok(records)
    }

    fn format(&self) -> Format {
        Format::Raw
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raw_deserialize() {
        let deser = RawBytesDeserializer::new();
        let schema = raw_schema();
        let data = b"hello world";

        let batch = deser.deserialize(data, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "hello world");
    }

    #[test]
    fn test_raw_roundtrip() {
        let deser = RawBytesDeserializer::new();
        let ser = RawBytesSerializer::new();
        let schema = raw_schema();

        let data = b"test data";
        let batch = deser.deserialize(data, &schema).unwrap();
        let serialized = ser.serialize(&batch).unwrap();

        assert_eq!(serialized.len(), 1);
        assert_eq!(serialized[0], b"test data");
    }

    #[test]
    fn test_raw_batch() {
        let deser = RawBytesDeserializer::new();
        let schema = raw_schema();

        let records: Vec<&[u8]> = vec![b"line1", b"line2", b"line3"];
        let batch = deser.deserialize_batch(&records, &schema).unwrap();
        assert_eq!(batch.num_rows(), 3);
    }
}
