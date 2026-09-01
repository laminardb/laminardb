//! Arrow `RecordBatch` → WebSocket message serialization.
//!
//! Converts Arrow data into JSON records for delivery to WebSocket clients.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::error::{ConnectorError, SerdeError};
use crate::schema::json::JsonEncoder;
use crate::schema::traits::FormatEncoder;

/// Serializes Arrow `RecordBatch` data into WebSocket message payloads.
pub struct BatchSerializer {
    encoder: JsonEncoder,
}

impl BatchSerializer {
    /// Creates a JSON serializer bound to the declared sink schema.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            encoder: JsonEncoder::new(schema),
        }
    }

    /// Serializes a `RecordBatch` into canonical per-row JSON bytes.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the batch schema differs from the declared
    /// sink schema or JSON encoding fails.
    pub fn serialize_rows(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, ConnectorError> {
        let expected = self.encoder.input_schema();
        if batch.schema().as_ref() != expected.as_ref() {
            return Err(ConnectorError::SchemaMismatch(format!(
                "WebSocket sink expected schema {expected:?}, got {:?}",
                batch.schema()
            )));
        }

        self.encoder.encode_batch(batch).map_err(|error| {
            ConnectorError::Serde(SerdeError::Json(format!(
                "failed to encode WebSocket sink batch: {error}"
            )))
        })
    }
}

#[cfg(test)]
mod tests;
