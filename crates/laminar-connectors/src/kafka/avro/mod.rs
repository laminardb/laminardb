//! Avro deserialization using `arrow-avro` with Confluent Schema Registry.
//!
//! [`AvroDeserializer`] implements [`RecordDeserializer`] by wrapping the
//! `arrow-avro` push-based [`Decoder`], which
//! natively supports the Confluent wire format (`0x00` + 4-byte BE schema ID
//! + Avro payload).

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_avro::reader::{Decoder, ReaderBuilder};
use arrow_avro::schema::{AvroSchema, Fingerprint, FingerprintAlgorithm, SchemaStore};
use arrow_schema::SchemaRef;
use parking_lot::Mutex;

use crate::error::{ConnectorError, SerdeError};
use crate::kafka::schema_registry::SchemaRegistryClient;
use crate::serde::{Format, RecordDeserializer};

const DECODER_BATCH_CAPACITY: usize = 8192;

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
    /// Reused across batches; rebuilt when `register_schema` runs.
    decoder: Mutex<Option<Decoder>>,
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
            decoder: Mutex::new(None),
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
            decoder: Mutex::new(None),
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
        // Schema store changed — drop the cached decoder so it rebuilds
        // against the new store on the next deserialize_batch call.
        *self.decoder.lock() = None;
        Ok(())
    }

    /// Ensures a schema ID is registered, fetching from SR if needed.
    ///
    /// Called by the Kafka source connector when an unknown schema ID is
    /// encountered in the Confluent wire format during poll.
    ///
    /// # Errors
    ///
    /// Preserves Schema Registry `ConnectorError` classification for remote
    /// resolution failures and returns `ConnectorError::Serde` for local
    /// decoder registration failures.
    /// Returns `Ok(true)` if this was a newly registered schema ID,
    /// `Ok(false)` if already known.
    pub async fn ensure_schema_registered(
        &mut self,
        schema_id: i32,
    ) -> Result<bool, ConnectorError> {
        if self.known_ids.contains(&schema_id) {
            return Ok(false);
        }

        let registry = self.schema_registry.as_ref().ok_or(ConnectorError::Serde(
            SerdeError::SchemaNotFound { schema_id },
        ))?;

        let cached = registry.resolve_confluent_id(schema_id).await?;

        self.register_schema(schema_id, &cached.schema_str)
            .map_err(ConnectorError::Serde)?;
        Ok(true)
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

        let mut guard = self.decoder.lock();
        let decoder = if let Some(d) = guard.as_mut() {
            d
        } else {
            let d = ReaderBuilder::new()
                .with_batch_size(DECODER_BATCH_CAPACITY)
                .with_writer_schema_store(self.schema_store.clone())
                .build_decoder()
                .map_err(|e| SerdeError::MalformedInput(format!("failed to build decoder: {e}")))?;
            guard.insert(d)
        };

        let mut partials: Vec<RecordBatch> = Vec::new();
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
                if decoder.batch_is_full() {
                    if let Some(b) = decoder
                        .flush()
                        .map_err(|e| SerdeError::MalformedInput(format!("Avro flush: {e}")))?
                    {
                        partials.push(b);
                    }
                }
            }
        }
        if let Some(b) = decoder
            .flush()
            .map_err(|e| SerdeError::MalformedInput(format!("Avro flush: {e}")))?
        {
            partials.push(b);
        }

        match partials.len() {
            0 => Err(SerdeError::MalformedInput("no records decoded".into())),
            1 => Ok(partials.pop().unwrap()),
            _ => arrow_select::concat::concat_batches(schema, &partials)
                .map_err(|e| SerdeError::MalformedInput(format!("concat: {e}"))),
        }
    }

    fn format(&self) -> Format {
        Format::Avro
    }

    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
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
mod tests;
