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
use arrow_avro::writer::{Encoder, WriterBuilder};
use arrow_schema::SchemaRef;
use parking_lot::Mutex;

use crate::error::SerdeError;
use crate::kafka::schema_registry::SchemaRegistryClient;
use crate::serde::{Format, RecordSerializer};

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
    encoder: Mutex<Option<(u32, Encoder)>>,
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
            encoder: Mutex::new(None),
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
            encoder: Mutex::new(None),
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
            encoder: Mutex::new(None),
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

    /// Prepare the native writer once, before activation.
    ///
    /// # Errors
    /// Rejects invalid wire IDs and native schemas unsupported by the installed codec.
    pub fn prepare(&self) -> Result<(), SerdeError> {
        let id = self.schema_id();
        let mut prepared = self.encoder.lock();
        if prepared
            .as_ref()
            .is_some_and(|(existing, _)| *existing == id)
        {
            return Ok(());
        }
        if id == 0 || id > i32::MAX as u32 {
            return Err(SerdeError::MalformedInput(
                "Avro writer requires a positive signed schema ID".into(),
            ));
        }
        let encoder = WriterBuilder::new(self.schema.as_ref().clone())
            .with_fingerprint_strategy(FingerprintStrategy::Id(id))
            .build_encoder::<AvroSoeFormat>()
            .map_err(|error| {
                SerdeError::MalformedInput(format!("unsupported Avro writer: {error}"))
            })?;
        *prepared = Some((id, encoder));
        Ok(())
    }

    fn encode(&self, batch: &RecordBatch) -> Result<arrow_avro::writer::EncodedRows, SerdeError> {
        self.prepare()?;
        let mut prepared = self.encoder.lock();
        let (_, encoder) = prepared
            .as_mut()
            .ok_or_else(|| SerdeError::MalformedInput("Avro writer was not prepared".into()))?;
        let result = encoder.encode(batch);
        // Drain partial output on failure so a later call cannot publish bytes from a failed batch.
        let rows = encoder.flush();
        result
            .map_err(|error| SerdeError::MalformedInput(format!("Avro encode error: {error}")))?;
        Ok(rows)
    }
}

impl RecordSerializer for AvroSerializer {
    fn serialize(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SerdeError> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }
        Ok(self.encode(batch)?.iter().map(|row| row.to_vec()).collect())
    }

    fn serialize_batch(&self, batch: &RecordBatch) -> Result<Vec<u8>, SerdeError> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }
        let rows = self.encode(batch)?;
        let mut output = Vec::new();
        for row in rows.iter() {
            output.extend_from_slice(&row);
        }
        Ok(output)
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

#[cfg(test)]
mod tests;
