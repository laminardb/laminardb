//! Avro deserialization using `arrow-avro` with Confluent Schema Registry.
//!
//! [`AvroDeserializer`] implements [`RecordDeserializer`] by wrapping the
//! `arrow-avro` push-based [`Decoder`](arrow_avro::reader::Decoder), which
//! natively supports the Confluent wire format (`0x00` + 4-byte BE schema ID
//! + Avro payload).

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_avro::reader::ReaderBuilder;
use arrow_avro::schema::{AvroSchema, Fingerprint, FingerprintAlgorithm, SchemaStore};
use arrow_schema::SchemaRef;

use crate::error::SerdeError;
use crate::kafka::schema_registry::SchemaRegistryClient;
use crate::serde::{Format, RecordDeserializer};

/// Confluent wire format magic byte.
const CONFLUENT_MAGIC: u8 = 0x00;

/// Size of the Confluent wire format header (1 magic + 4 schema ID).
const CONFLUENT_HEADER_SIZE: usize = 5;

/// Avro deserializer backed by `arrow-avro` with optional Schema Registry.
///
/// Supports both raw Avro and the Confluent wire format. When a Schema
/// Registry client is provided, unknown schema IDs are fetched and
/// registered automatically.
pub struct AvroDeserializer {
    /// Schema store shared with the Decoder.
    schema_store: SchemaStore,
    /// Optional Schema Registry client for resolving unknown IDs.
    schema_registry: Option<Arc<SchemaRegistryClient>>,
    /// Set of schema IDs already registered in the store.
    known_ids: HashSet<i32>,
}

impl AvroDeserializer {
    /// Creates a new Avro deserializer without Schema Registry integration.
    ///
    /// The caller must register schemas manually via [`register_schema`](Self::register_schema).
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema_store: SchemaStore::new_with_type(FingerprintAlgorithm::Id),
            schema_registry: None,
            known_ids: HashSet::new(),
        }
    }

    /// Creates a new Avro deserializer with Schema Registry integration.
    ///
    /// Unknown schema IDs encountered in the Confluent wire format will
    /// be fetched from the registry automatically.
    #[must_use]
    pub fn with_schema_registry(registry: Arc<SchemaRegistryClient>) -> Self {
        Self {
            schema_store: SchemaStore::new_with_type(FingerprintAlgorithm::Id),
            schema_registry: Some(registry),
            known_ids: HashSet::new(),
        }
    }

    /// Registers an Avro schema with a Confluent schema ID.
    ///
    /// # Errors
    ///
    /// Returns `SerdeError` if the fingerprint cannot be set.
    #[allow(clippy::cast_sign_loss)]
    pub fn register_schema(
        &mut self,
        schema_id: i32,
        avro_schema_json: &str,
    ) -> Result<(), SerdeError> {
        let avro_schema = AvroSchema::new(avro_schema_json.to_string());
        // Use Fingerprint::Id directly — NOT load_fingerprint_id which
        // applies from_be byte-swap meant for raw wire bytes.
        let fp = Fingerprint::Id(schema_id as u32);
        self.schema_store
            .set(fp, avro_schema)
            .map_err(|e| SerdeError::MalformedInput(format!("failed to register schema: {e}")))?;
        self.known_ids.insert(schema_id);
        Ok(())
    }

    /// Ensures a schema ID is registered, fetching from SR if needed.
    ///
    /// Called by the Kafka source connector when an unknown schema ID is
    /// encountered in the Confluent wire format during poll.
    ///
    /// # Errors
    ///
    /// Returns `SerdeError` if the schema cannot be fetched or registered.
    pub async fn ensure_schema_registered(&mut self, schema_id: i32) -> Result<(), SerdeError> {
        if self.known_ids.contains(&schema_id) {
            return Ok(());
        }

        let registry = self
            .schema_registry
            .as_ref()
            .ok_or(SerdeError::SchemaNotFound { schema_id })?;

        let cached = registry
            .resolve_confluent_id(schema_id)
            .await
            .map_err(|_| SerdeError::SchemaNotFound { schema_id })?;

        self.register_schema(schema_id, &cached.schema_str)?;
        Ok(())
    }

    /// Extracts the Confluent schema ID from a wire-format message.
    ///
    /// Returns `None` if the message is not in Confluent wire format.
    #[must_use]
    pub fn extract_confluent_id(data: &[u8]) -> Option<i32> {
        if data.len() < CONFLUENT_HEADER_SIZE || data[0] != CONFLUENT_MAGIC {
            return None;
        }
        let id = i32::from_be_bytes([data[1], data[2], data[3], data[4]]);
        Some(id)
    }
}

impl Default for AvroDeserializer {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordDeserializer for AvroDeserializer {
    fn deserialize(&self, data: &[u8], schema: &SchemaRef) -> Result<RecordBatch, SerdeError> {
        self.deserialize_batch(&[data], schema)
    }

    fn deserialize_batch(
        &self,
        records: &[&[u8]],
        schema: &SchemaRef,
    ) -> Result<RecordBatch, SerdeError> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }

        let mut decoder = ReaderBuilder::new()
            .with_batch_size(records.len())
            .with_writer_schema_store(self.schema_store.clone())
            .build_decoder()
            .map_err(|e| SerdeError::MalformedInput(format!("failed to build decoder: {e}")))?;

        for record in records {
            let mut offset = 0;
            while offset < record.len() {
                let consumed = decoder
                    .decode(&record[offset..])
                    .map_err(|e| SerdeError::MalformedInput(format!("Avro decode error: {e}")))?;
                if consumed == 0 {
                    break;
                }
                offset += consumed;
            }
        }

        decoder
            .flush()
            .map_err(|e| SerdeError::MalformedInput(format!("Avro flush error: {e}")))?
            .ok_or_else(|| SerdeError::MalformedInput("no records decoded".into()))
    }

    fn format(&self) -> Format {
        Format::Avro
    }
}

impl std::fmt::Debug for AvroDeserializer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroDeserializer")
            .field("known_ids", &self.known_ids)
            .field("has_registry", &self.schema_registry.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    #[allow(dead_code)]
    const TEST_AVRO_SCHEMA: &str = r#"{
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
        ]
    }"#;

    fn test_arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    #[test]
    fn test_extract_confluent_id() {
        // Valid: 0x00 + 4-byte BE schema ID
        let data = [0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03];
        assert_eq!(AvroDeserializer::extract_confluent_id(&data), Some(1));

        let data = [0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x03];
        assert_eq!(AvroDeserializer::extract_confluent_id(&data), Some(256));
    }

    #[test]
    fn test_extract_confluent_id_not_confluent() {
        let data = [0x01, 0x00, 0x00, 0x00, 0x01];
        assert_eq!(AvroDeserializer::extract_confluent_id(&data), None);
    }

    #[test]
    fn test_extract_confluent_id_too_short() {
        let data = [0x00, 0x00];
        assert_eq!(AvroDeserializer::extract_confluent_id(&data), None);
    }

    #[test]
    fn test_new_deserializer() {
        let deser = AvroDeserializer::new();
        assert!(deser.schema_registry.is_none());
        assert!(deser.known_ids.is_empty());
    }

    #[test]
    fn test_register_schema() {
        let mut deser = AvroDeserializer::new();
        let result = deser.register_schema(1, TEST_AVRO_SCHEMA);
        assert!(result.is_ok());
        assert!(deser.known_ids.contains(&1));
    }

    #[test]
    fn test_format() {
        let deser = AvroDeserializer::new();
        assert_eq!(deser.format(), Format::Avro);
    }

    #[test]
    fn test_deserialize_empty_batch() {
        let deser = AvroDeserializer::new();
        let schema = test_arrow_schema();
        let result = deser.deserialize_batch(&[], &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().num_rows(), 0);
    }

    #[test]
    fn test_extract_confluent_id_large() {
        // Schema ID 42
        let mut data = vec![0x00u8];
        data.extend_from_slice(&42i32.to_be_bytes());
        data.push(0xFF);
        assert_eq!(AvroDeserializer::extract_confluent_id(&data), Some(42));
    }

    #[test]
    fn test_extract_confluent_id_edge_cases() {
        // Empty slice
        assert_eq!(AvroDeserializer::extract_confluent_id(&[]), None);

        // Magic byte only (too short)
        assert_eq!(AvroDeserializer::extract_confluent_id(&[0x00]), None);

        // 4 bytes with magic (too short)
        assert_eq!(
            AvroDeserializer::extract_confluent_id(&[0x00, 0x00, 0x00, 0x00]),
            None
        );

        // Exactly 5 bytes (boundary of CONFLUENT_HEADER_SIZE)
        let data = [0x00, 0x00, 0x00, 0x00, 0x05];
        assert_eq!(AvroDeserializer::extract_confluent_id(&data), Some(5));

        // i32::MAX ID
        let mut data = vec![0x00];
        data.extend_from_slice(&i32::MAX.to_be_bytes());
        assert_eq!(
            AvroDeserializer::extract_confluent_id(&data),
            Some(i32::MAX)
        );

        // i32::MIN ID
        let mut data = vec![0x00];
        data.extend_from_slice(&i32::MIN.to_be_bytes());
        assert_eq!(
            AvroDeserializer::extract_confluent_id(&data),
            Some(i32::MIN)
        );
    }
}
