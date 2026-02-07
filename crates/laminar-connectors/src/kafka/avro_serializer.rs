//! Avro serialization using `arrow-avro` with Confluent Schema Registry.
//!
//! `AvroSerializer` implements `RecordSerializer` by wrapping the
//! `arrow-avro` `Writer` with SOE format, producing per-record payloads
//! with the Confluent wire format prefix (`0x00` + 4-byte BE schema ID
//! + Avro body).

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_avro::schema::FingerprintStrategy;
use arrow_avro::writer::format::AvroSoeFormat;
use arrow_avro::writer::WriterBuilder;
use arrow_schema::SchemaRef;
use tokio::sync::Mutex;

use crate::error::SerdeError;
use crate::kafka::schema_registry::SchemaRegistryClient;
use crate::serde::{Format, RecordSerializer};

/// Confluent wire format header size (1 magic + 4 schema ID).
const CONFLUENT_HEADER_SIZE: usize = 5;

/// Confluent wire format magic byte.
const CONFLUENT_MAGIC: u8 = 0x00;

/// Avro serializer backed by `arrow-avro` with optional Schema Registry.
///
/// Produces per-row byte payloads in the Confluent wire format suitable
/// for individual Kafka producer messages.
pub struct AvroSerializer {
    /// Schema ID to embed in the Confluent wire format prefix.
    schema_id: u32,
    /// Arrow schema for the records being serialized.
    schema: SchemaRef,
    /// Optional Schema Registry client for schema registration.
    schema_registry: Option<Arc<Mutex<SchemaRegistryClient>>>,
}

impl AvroSerializer {
    /// Creates a new Avro serializer with a known schema ID.
    ///
    /// Each serialized record is prefixed with `0x00` + `schema_id` (4-byte BE).
    #[must_use]
    pub fn new(schema: SchemaRef, schema_id: u32) -> Self {
        Self {
            schema_id,
            schema,
            schema_registry: None,
        }
    }

    /// Creates a new Avro serializer with Schema Registry integration.
    ///
    /// The schema ID is obtained from the registry. Call
    /// `set_schema_id` after registering the schema.
    #[must_use]
    pub fn with_schema_registry(
        schema: SchemaRef,
        schema_id: u32,
        registry: Arc<Mutex<SchemaRegistryClient>>,
    ) -> Self {
        Self {
            schema_id,
            schema,
            schema_registry: Some(registry),
        }
    }

    /// Updates the schema ID (e.g., after registering with the registry).
    pub fn set_schema_id(&mut self, id: u32) {
        self.schema_id = id;
    }

    /// Returns the current schema ID.
    #[must_use]
    pub fn schema_id(&self) -> u32 {
        self.schema_id
    }

    /// Returns whether a Schema Registry client is configured.
    #[must_use]
    pub fn has_schema_registry(&self) -> bool {
        self.schema_registry.is_some()
    }

    /// Serializes a `RecordBatch` into per-row Avro payloads with
    /// Confluent wire format prefix.
    ///
    /// Each output `Vec<u8>` is: `0x00` | `schema_id` (4-byte BE) | Avro body.
    fn serialize_with_confluent_prefix(
        &self,
        batch: &RecordBatch,
    ) -> Result<Vec<Vec<u8>>, SerdeError> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }

        // Serialize all rows into a single buffer using SOE format.
        let mut buf = Vec::new();
        let arrow_schema = (*self.schema).clone();

        let mut writer = WriterBuilder::new(arrow_schema)
            .with_fingerprint_strategy(FingerprintStrategy::Id(self.schema_id))
            .build::<_, AvroSoeFormat>(&mut buf)
            .map_err(|e| SerdeError::MalformedInput(format!("failed to build Avro writer: {e}")))?;

        writer
            .write(batch)
            .map_err(|e| SerdeError::MalformedInput(format!("Avro encode error: {e}")))?;

        writer
            .finish()
            .map_err(|e| SerdeError::MalformedInput(format!("Avro flush error: {e}")))?;

        // Split the buffer into per-record payloads.
        // Each record starts with CONFLUENT_MAGIC (0x00) + 4-byte schema ID.
        split_confluent_records(&buf, batch.num_rows())
    }
}

impl RecordSerializer for AvroSerializer {
    fn serialize(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SerdeError> {
        self.serialize_with_confluent_prefix(batch)
    }

    fn serialize_batch(&self, batch: &RecordBatch) -> Result<Vec<u8>, SerdeError> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }

        let mut buf = Vec::new();
        let arrow_schema = (*self.schema).clone();

        let mut writer = WriterBuilder::new(arrow_schema)
            .with_fingerprint_strategy(FingerprintStrategy::Id(self.schema_id))
            .build::<_, AvroSoeFormat>(&mut buf)
            .map_err(|e| SerdeError::MalformedInput(format!("failed to build Avro writer: {e}")))?;

        writer
            .write(batch)
            .map_err(|e| SerdeError::MalformedInput(format!("Avro encode error: {e}")))?;

        writer
            .finish()
            .map_err(|e| SerdeError::MalformedInput(format!("Avro flush error: {e}")))?;

        Ok(buf)
    }

    fn format(&self) -> Format {
        Format::Avro
    }
}

impl std::fmt::Debug for AvroSerializer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroSerializer")
            .field("schema_id", &self.schema_id)
            .field("has_registry", &self.schema_registry.is_some())
            .finish_non_exhaustive()
    }
}

/// Splits a buffer of concatenated Confluent-format Avro records into
/// individual per-record payloads.
///
/// Each record starts with `0x00` + 4-byte BE schema ID + Avro body.
fn split_confluent_records(buf: &[u8], expected_rows: usize) -> Result<Vec<Vec<u8>>, SerdeError> {
    let mut records = Vec::with_capacity(expected_rows);
    let mut offset = 0;

    while offset < buf.len() {
        // Validate magic byte.
        if buf[offset] != CONFLUENT_MAGIC {
            return Err(SerdeError::InvalidConfluentHeader {
                expected: CONFLUENT_MAGIC,
                got: buf[offset],
            });
        }

        // Find the start of the next record (next occurrence of magic byte
        // followed by the same schema ID).
        let next_start = if offset + CONFLUENT_HEADER_SIZE < buf.len() {
            let schema_id_bytes = &buf[offset + 1..offset + CONFLUENT_HEADER_SIZE];
            find_next_record(&buf[offset + CONFLUENT_HEADER_SIZE..], schema_id_bytes)
                .map_or(buf.len(), |pos| offset + CONFLUENT_HEADER_SIZE + pos)
        } else {
            buf.len()
        };

        records.push(buf[offset..next_start].to_vec());
        offset = next_start;
    }

    if records.len() != expected_rows {
        return Err(SerdeError::RecordCountMismatch {
            expected: expected_rows,
            got: records.len(),
        });
    }

    Ok(records)
}

/// Finds the next Confluent record boundary in a buffer.
///
/// Looks for `0x00` followed by the expected schema ID bytes.
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
    fn test_new_serializer() {
        let ser = AvroSerializer::new(test_schema(), 42);
        assert_eq!(ser.schema_id(), 42);
        assert!(!ser.has_schema_registry());
        assert_eq!(ser.format(), Format::Avro);
    }

    #[test]
    fn test_set_schema_id() {
        let mut ser = AvroSerializer::new(test_schema(), 1);
        assert_eq!(ser.schema_id(), 1);
        ser.set_schema_id(99);
        assert_eq!(ser.schema_id(), 99);
    }

    #[test]
    fn test_serialize_empty_batch() {
        let ser = AvroSerializer::new(test_schema(), 1);
        let batch = RecordBatch::new_empty(test_schema());
        let result = ser.serialize(&batch).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_serialize_batch_produces_records() {
        let ser = AvroSerializer::new(test_schema(), 7);
        let batch = test_batch(3);
        let records = ser.serialize(&batch).unwrap();
        assert_eq!(records.len(), 3);

        // Each record should start with Confluent wire format
        for record in &records {
            assert!(record.len() >= CONFLUENT_HEADER_SIZE);
            assert_eq!(record[0], CONFLUENT_MAGIC);
            // Schema ID = 7 in big-endian
            assert_eq!(&record[1..5], &7u32.to_be_bytes());
        }
    }

    #[test]
    fn test_serialize_batch_to_single_buffer() {
        let ser = AvroSerializer::new(test_schema(), 1);
        let batch = test_batch(2);
        let buf = ser.serialize_batch(&batch).unwrap();
        assert!(!buf.is_empty());
        // Should contain Confluent prefix
        assert_eq!(buf[0], CONFLUENT_MAGIC);
    }

    #[test]
    fn test_debug_output() {
        let ser = AvroSerializer::new(test_schema(), 42);
        let debug = format!("{ser:?}");
        assert!(debug.contains("AvroSerializer"));
        assert!(debug.contains("42"));
    }
}
