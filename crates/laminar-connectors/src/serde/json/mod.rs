//! JSON serialization and deserialization.
//!
//! Implements [`RecordDeserializer`] / [`RecordSerializer`] by delegating
//! to [`JsonDecoder`] and [`JsonEncoder`].

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parking_lot::Mutex;
use serde_json::Value;

use super::{Format, RecordDeserializer, RecordSerializer};
use crate::error::SerdeError;
use crate::schema::json::decoder::JsonDecoder;
use crate::schema::json::encoder::JsonEncoder;
use crate::schema::traits::FormatEncoder;

/// JSON record deserializer. Delegates to a cached [`JsonDecoder`].
#[derive(Debug, Clone)]
pub struct JsonDeserializer {
    #[allow(clippy::type_complexity)]
    decoder: Arc<Mutex<Option<(SchemaRef, Arc<JsonDecoder>)>>>,
}

impl JsonDeserializer {
    /// Creates a new JSON deserializer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            decoder: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns a cached [`JsonDecoder`] for `schema`, rebuilding it only when
    /// the cached schema differs.
    fn decoder_for(&self, schema: &SchemaRef) -> Arc<JsonDecoder> {
        let mut cache = self.decoder.lock();
        if let Some((cached_schema, cached)) = cache.as_ref() {
            if Arc::ptr_eq(cached_schema, schema) || cached_schema == schema {
                return cached.clone();
            }
        }
        let decoder = Arc::new(JsonDecoder::new(schema.clone()));
        *cache = Some((schema.clone(), decoder.clone()));
        decoder
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
        let d = self.decoder_for(schema);
        d.decode_slices(&[data])
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
        let d = self.decoder_for(schema);
        d.decode_slices(records)
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
mod tests;
