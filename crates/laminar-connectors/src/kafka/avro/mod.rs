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
    reader_schema: Option<AvroSchema>,
    projection: Option<Vec<usize>>,
    schema_metrics: Option<super::KafkaSourceMetrics>,
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
            reader_schema: None,
            projection: None,
            schema_metrics: None,
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
            reader_schema: None,
            projection: None,
            schema_metrics: None,
        }
    }

    /// Bind the immutable native reader and its SQL projection before consuming records.
    ///
    /// # Errors
    /// Rejects missing native content, invalid IDs, and unsupported projections.
    pub fn bind_reader(
        &mut self,
        binding: &crate::schema::resolution::SchemaBinding,
        logical: &SchemaRef,
    ) -> Result<(), ConnectorError> {
        let native = binding.value.as_ref().ok_or_else(|| {
            ConnectorError::SchemaMismatch("Avro reader has no native schema".into())
        })?;
        let resolved = native
            .definition
            .get("resolved")
            .ok_or_else(|| {
                ConnectorError::SchemaMismatch("Avro reader has no resolved native schema".into())
            })?
            .to_string();
        let external = crate::kafka::schema_registry::avro_to_arrow_schema(&resolved)?;
        let projection = logical
            .fields()
            .iter()
            .map(|field| {
                external.index_of(field.name()).map_err(|_| {
                    ConnectorError::SchemaMismatch(format!(
                        "reader field '{}' is absent from the native record",
                        field.name()
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let id = crate::kafka::schema_resolution::contract_schema_id(binding)?;
        let id = i32::try_from(id).map_err(|_| {
            ConnectorError::SchemaMismatch("Avro schema ID exceeds the supported wire range".into())
        })?;
        self.register_schema(id, &resolved)
            .map_err(ConnectorError::Serde)?;
        self.reader_schema = Some(AvroSchema::new(resolved));
        self.projection = Some(projection);
        Ok(())
    }

    pub(crate) fn set_schema_metrics(&mut self, metrics: super::KafkaSourceMetrics) {
        self.schema_metrics = Some(metrics);
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
        if schema_id <= 0 || avro_schema_json.len() > 1024 * 1024 {
            return Err(SerdeError::MalformedInput(
                "invalid schema ID or schema exceeds 1 MiB".into(),
            ));
        }
        if !self.known_ids.contains(&schema_id) && self.known_ids.len() >= 64 {
            return Err(SerdeError::MalformedInput(
                "writer schema cache limit (64) reached; controlled restart is required".into(),
            ));
        }
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

        let mut observation = WriterFetchObservation::new(self.schema_metrics.clone());
        let cached = registry.resolve_confluent_id(schema_id).await?;
        observation.succeeded = true;

        self.register_schema(schema_id, &cached.resolved_schema_str)
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
            let mut builder = ReaderBuilder::new()
                .with_batch_size(DECODER_BATCH_CAPACITY)
                .with_writer_schema_store(self.schema_store.clone());
            if let Some(reader) = &self.reader_schema {
                builder = builder.with_reader_schema(reader.clone());
            }
            if let Some(projection) = &self.projection {
                builder = builder.with_projection(projection.clone());
            }
            let d = builder
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
            1 => {
                let batch = partials.remove(0);
                if batch.schema().as_ref() == schema.as_ref() {
                    return Ok(batch);
                }
                RecordBatch::try_new(Arc::clone(schema), batch.columns().to_vec()).map_err(
                    |error| {
                        SerdeError::MalformedInput(format!("committed reader mismatch: {error}"))
                    },
                )
            }
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

struct WriterFetchObservation {
    metrics: Option<super::KafkaSourceMetrics>,
    started: std::time::Instant,
    succeeded: bool,
}

impl WriterFetchObservation {
    fn new(metrics: Option<super::KafkaSourceMetrics>) -> Self {
        if let Some(metrics) = &metrics {
            metrics.schema_cache_misses.inc();
            metrics.schema_unresolved_records.inc();
        }
        Self {
            metrics,
            started: std::time::Instant::now(),
            succeeded: false,
        }
    }
}

impl Drop for WriterFetchObservation {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.schema_unresolved_records.dec();
            metrics
                .schema_fetch_microseconds
                .inc_by(u64::try_from(self.started.elapsed().as_micros()).unwrap_or(u64::MAX));
            if !self.succeeded {
                metrics.schema_fetch_failures.inc();
            }
        }
    }
}
