//! Avro serialization using `arrow-avro` with Confluent Schema Registry.
//!
//! `AvroSerializer` implements `RecordSerializer` by wrapping the
//! `arrow-avro` `Writer` with SOE format, producing per-record payloads
//! with the Confluent wire format prefix (`0x00` + 4-byte BE schema ID
//! + Avro body).

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_avro::schema::FingerprintStrategy;
use arrow_avro::writer::format::AvroSoeFormat;
use arrow_avro::writer::WriterBuilder;
use arrow_schema::SchemaRef;

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
    /// Schema ID shared with `KafkaSink` for post-registration updates.
    schema_id: Arc<AtomicU32>,
    /// Arrow schema for the records being serialized.
    schema: SchemaRef,
    /// Optional Schema Registry client for schema registration.
    schema_registry: Option<Arc<SchemaRegistryClient>>,
}

impl AvroSerializer {
    /// Creates a new Avro serializer with a known schema ID.
    ///
    /// Each serialized record is prefixed with `0x00` + `schema_id` (4-byte BE).
    #[must_use]
    pub fn new(schema: SchemaRef, schema_id: u32) -> Self {
        Self {
            schema_id: Arc::new(AtomicU32::new(schema_id)),
            schema,
            schema_registry: None,
        }
    }

    /// Creates a new Avro serializer with a shared schema ID handle.
    ///
    /// The `KafkaSink` retains a clone of the `Arc<AtomicU32>` so it can
    /// update the schema ID after registration without downcasting.
    #[must_use]
    pub fn with_shared_schema_id(
        schema: SchemaRef,
        schema_id: Arc<AtomicU32>,
        registry: Option<Arc<SchemaRegistryClient>>,
    ) -> Self {
        Self {
            schema_id,
            schema,
            schema_registry: registry,
        }
    }

    /// Creates a new Avro serializer with Schema Registry integration.
    #[must_use]
    pub fn with_schema_registry(
        schema: SchemaRef,
        schema_id: u32,
        registry: Arc<SchemaRegistryClient>,
    ) -> Self {
        Self {
            schema_id: Arc::new(AtomicU32::new(schema_id)),
            schema,
            schema_registry: Some(registry),
        }
    }

    /// Returns a shared handle to the schema ID for external updates.
    #[must_use]
    pub fn schema_id_handle(&self) -> Arc<AtomicU32> {
        Arc::clone(&self.schema_id)
    }

    /// Returns the current schema ID.
    #[must_use]
    pub fn schema_id(&self) -> u32 {
        self.schema_id.load(Ordering::Relaxed)
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

        let id = self.schema_id.load(Ordering::Relaxed);
        let mut writer = WriterBuilder::new(arrow_schema)
            .with_fingerprint_strategy(FingerprintStrategy::Id(id))
            .build::<_, AvroSoeFormat>(&mut buf)
            .map_err(|e| SerdeError::MalformedInput(format!("failed to build Avro writer: {e}")))?;

        writer
            .write(batch)
            .map_err(|e| SerdeError::MalformedInput(format!("Avro encode error: {e}")))?;

        writer
            .finish()
            .map_err(|e| SerdeError::MalformedInput(format!("Avro flush error: {e}")))?;

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
        let id = self.schema_id.load(Ordering::Relaxed);

        let mut writer = WriterBuilder::new(arrow_schema)
            .with_fingerprint_strategy(FingerprintStrategy::Id(id))
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
            .field("schema_id", &self.schema_id.load(Ordering::Relaxed))
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
    fn test_shared_schema_id() {
        let ser = AvroSerializer::new(test_schema(), 1);
        assert_eq!(ser.schema_id(), 1);
        let handle = ser.schema_id_handle();
        handle.store(99, std::sync::atomic::Ordering::Relaxed);
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
