//! Avro format encoder implementing [`FormatEncoder`].
//!
//! Provides [`AvroFormatEncoder`] for encoding Arrow `RecordBatch`es into
//! Confluent wire-format Avro payloads.

use arrow_array::RecordBatch;
use arrow_avro::schema::FingerprintStrategy;
use arrow_avro::writer::format::AvroSoeFormat;
use arrow_avro::writer::WriterBuilder;
use arrow_schema::SchemaRef;

use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::traits::FormatEncoder;

/// Confluent wire format magic byte.
const CONFLUENT_MAGIC: u8 = 0x00;

/// Size of the Confluent wire format header (1 magic + 4 schema ID).
const CONFLUENT_HEADER_SIZE: usize = 5;

/// Avro format encoder implementing [`FormatEncoder`].
///
/// Encodes Arrow `RecordBatch`es into per-row Confluent wire-format Avro
/// payloads: `0x00` + schema ID (4-byte BE) + Avro binary body.
///
/// Uses `arrow-avro` for native Arrow-to-Avro encoding.
pub struct AvroFormatEncoder {
    /// The expected input Arrow schema.
    input_schema: SchemaRef,
    /// The Confluent schema ID to embed in each record's header.
    schema_id: u32,
}

impl AvroFormatEncoder {
    /// Creates a new Avro encoder.
    ///
    /// Each encoded record is prefixed with the Confluent wire-format
    /// header containing the given `schema_id`.
    #[must_use]
    pub fn new(input_schema: SchemaRef, schema_id: u32) -> Self {
        Self {
            input_schema,
            schema_id,
        }
    }

    /// Returns the configured schema ID.
    #[must_use]
    pub fn schema_id(&self) -> u32 {
        self.schema_id
    }

    /// Updates the schema ID (e.g., after registering with the registry).
    pub fn set_schema_id(&mut self, id: u32) {
        self.schema_id = id;
    }
}

impl std::fmt::Debug for AvroFormatEncoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroFormatEncoder")
            .field("schema_id", &self.schema_id)
            .finish_non_exhaustive()
    }
}

impl FormatEncoder for AvroFormatEncoder {
    fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }

    fn encode_batch(&self, batch: &RecordBatch) -> SchemaResult<Vec<Vec<u8>>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }

        let mut buf = Vec::new();
        let arrow_schema = (*self.input_schema).clone();

        let mut writer = WriterBuilder::new(arrow_schema)
            .with_fingerprint_strategy(FingerprintStrategy::Id(self.schema_id))
            .build::<_, AvroSoeFormat>(&mut buf)
            .map_err(|e| {
                SchemaError::DecodeError(format!("failed to build Avro writer: {e}"))
            })?;

        writer
            .write(batch)
            .map_err(|e| SchemaError::DecodeError(format!("Avro encode error: {e}")))?;

        writer
            .finish()
            .map_err(|e| SchemaError::DecodeError(format!("Avro flush error: {e}")))?;

        split_confluent_records(&buf, batch.num_rows())
    }

    fn format_name(&self) -> &'static str {
        "confluent-avro"
    }
}

/// Splits a buffer of concatenated Confluent-format Avro records.
///
/// Each record starts with `CONFLUENT_MAGIC` + 4-byte schema ID.
fn split_confluent_records(buf: &[u8], expected: usize) -> SchemaResult<Vec<Vec<u8>>> {
    let mut records = Vec::with_capacity(expected);
    let mut offset = 0;

    while offset < buf.len() {
        if buf[offset] != CONFLUENT_MAGIC {
            return Err(SchemaError::DecodeError(format!(
                "expected Confluent magic at offset {offset}, got 0x{:02x}",
                buf[offset]
            )));
        }

        // Find next record boundary.
        let schema_id_bytes = buf
            .get(offset + 1..offset + CONFLUENT_HEADER_SIZE)
            .ok_or_else(|| {
                SchemaError::DecodeError("truncated Confluent header".into())
            })?;

        let next_start = find_next_record(
            &buf[offset + CONFLUENT_HEADER_SIZE..],
            schema_id_bytes,
        )
        .map_or(buf.len(), |pos| offset + CONFLUENT_HEADER_SIZE + pos);

        records.push(buf[offset..next_start].to_vec());
        offset = next_start;
    }

    if records.len() != expected {
        return Err(SchemaError::DecodeError(format!(
            "expected {expected} records, got {}",
            records.len()
        )));
    }

    Ok(records)
}

/// Finds the next Confluent record boundary after the current header.
fn find_next_record(buf: &[u8], schema_id_bytes: &[u8]) -> Option<usize> {
    let mut pos = 0;
    while pos + CONFLUENT_HEADER_SIZE <= buf.len() {
        if buf[pos] == CONFLUENT_MAGIC
            && buf[pos + 1..pos + CONFLUENT_HEADER_SIZE] == *schema_id_bytes
        {
            return Some(pos);
        }
        pos += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn test_batch(n: usize) -> RecordBatch {
        #[allow(clippy::cast_possible_wrap)]
        let ids: Vec<i64> = (0..n as i64).collect();
        let names: Vec<String> = (0..n).map(|i| format!("name-{i}")).collect();
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_new_encoder() {
        let enc = AvroFormatEncoder::new(test_schema(), 42);
        assert_eq!(enc.schema_id(), 42);
        assert_eq!(enc.format_name(), "confluent-avro");
    }

    #[test]
    fn test_set_schema_id() {
        let mut enc = AvroFormatEncoder::new(test_schema(), 1);
        enc.set_schema_id(99);
        assert_eq!(enc.schema_id(), 99);
    }

    #[test]
    fn test_encode_empty_batch() {
        let enc = AvroFormatEncoder::new(test_schema(), 1);
        let batch = RecordBatch::new_empty(test_schema());
        let result = enc.encode_batch(&batch).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_encode_produces_confluent_records() {
        let enc = AvroFormatEncoder::new(test_schema(), 7);
        let batch = test_batch(3);
        let records = enc.encode_batch(&batch).unwrap();
        assert_eq!(records.len(), 3);

        for record in &records {
            assert!(record.len() >= CONFLUENT_HEADER_SIZE);
            assert_eq!(record[0], CONFLUENT_MAGIC);
            assert_eq!(&record[1..5], &7u32.to_be_bytes());
        }
    }

    #[test]
    fn test_input_schema() {
        let schema = test_schema();
        let enc = AvroFormatEncoder::new(Arc::clone(&schema), 1);
        assert_eq!(enc.input_schema(), schema);
    }

    #[test]
    fn test_debug_output() {
        let enc = AvroFormatEncoder::new(test_schema(), 42);
        let debug = format!("{enc:?}");
        assert!(debug.contains("AvroFormatEncoder"));
        assert!(debug.contains("42"));
    }
}
