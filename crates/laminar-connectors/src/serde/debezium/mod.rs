//! Debezium CDC envelope format deserialization.
//!
//! Parses Debezium JSON change events into Arrow `RecordBatch`.
//!
//! ## Debezium Envelope Format
//!
//! ```json
//! {
//!   "before": { ... },       // null for inserts
//!   "after":  { ... },       // null for deletes
//!   "source": { ... },       // source metadata
//!   "op": "c|u|d|r",         // operation: create, update, delete, read (snapshot)
//!   "ts_ms": 1234567890      // timestamp
//! }
//! ```
//!
//! The deserializer extracts the `after` payload for inserts/updates
//! and the `before` payload for deletes, adding `__op` and `__ts_ms`
//! metadata columns.

use std::sync::Arc;

use arrow_array::builder::{Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde_json::Value;

use super::json::JsonDeserializer;
use super::{Format, RecordDeserializer};
use crate::error::SerdeError;

/// Debezium operation types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DebeziumOp {
    /// Create (insert).
    Create,
    /// Update.
    Update,
    /// Delete.
    Delete,
    /// Read (snapshot).
    Read,
}

impl DebeziumOp {
    /// Parses an operation from the Debezium `op` field.
    fn from_str(s: &str) -> Result<Self, SerdeError> {
        match s {
            "c" => Ok(DebeziumOp::Create),
            "u" => Ok(DebeziumOp::Update),
            "d" => Ok(DebeziumOp::Delete),
            "r" => Ok(DebeziumOp::Read),
            other => Err(SerdeError::MalformedInput(format!(
                "unknown Debezium op: {other}"
            ))),
        }
    }

    /// Returns the operation as a string.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            DebeziumOp::Create => "c",
            DebeziumOp::Update => "u",
            DebeziumOp::Delete => "d",
            DebeziumOp::Read => "r",
        }
    }
}

/// Debezium CDC envelope deserializer.
///
/// Extracts the data payload from a Debezium change event and converts
/// it to an Arrow `RecordBatch`. Two metadata columns are appended:
/// - `__op`: The operation type (c/u/d/r)
/// - `__ts_ms`: The event timestamp in milliseconds
///
/// The provided schema should describe the data columns only (without
/// `__op` and `__ts_ms`); these are added automatically.
#[derive(Debug, Clone)]
pub struct DebeziumDeserializer {
    json_deser: JsonDeserializer,
}

impl DebeziumDeserializer {
    /// Creates a new Debezium deserializer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            json_deser: JsonDeserializer::new(),
        }
    }
}

impl Default for DebeziumDeserializer {
    fn default() -> Self {
        Self::new()
    }
}

fn output_schema(schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new("__op", DataType::Utf8, false)));
    fields.push(Arc::new(Field::new("__ts_ms", DataType::Int64, false)));
    Arc::new(Schema::new(fields))
}

impl RecordDeserializer for DebeziumDeserializer {
    fn deserialize(&self, data: &[u8], schema: &SchemaRef) -> Result<RecordBatch, SerdeError> {
        let envelope: Value = serde_json::from_slice(data)?;
        let obj = envelope
            .as_object()
            .ok_or_else(|| SerdeError::MalformedInput("expected JSON object".into()))?;

        // Extract operation
        let op_str = obj
            .get("op")
            .and_then(Value::as_str)
            .ok_or_else(|| SerdeError::MissingField("op".into()))?;
        let op = DebeziumOp::from_str(op_str)?;

        // Extract timestamp
        let ts_ms = obj.get("ts_ms").and_then(Value::as_i64).unwrap_or(0);

        // Extract payload: `after` for create/update/read, `before` for delete
        let payload = match op {
            DebeziumOp::Create | DebeziumOp::Update | DebeziumOp::Read => obj
                .get("after")
                .ok_or_else(|| SerdeError::MissingField("after".into()))?,
            DebeziumOp::Delete => obj
                .get("before")
                .ok_or_else(|| SerdeError::MissingField("before".into()))?,
        };

        if payload.is_null() {
            return Err(SerdeError::MalformedInput(format!(
                "payload is null for op={op_str}"
            )));
        }

        // Deserialize the payload directly from the parsed Value (avoids double parse)
        let data_batch = self.json_deser.deserialize_value(payload, schema)?;

        // Append __op and __ts_ms columns
        let mut columns: Vec<ArrayRef> = data_batch.columns().to_vec();

        let mut op_builder = StringBuilder::with_capacity(1, 1);
        op_builder.append_value(op.as_str());
        columns.push(Arc::new(op_builder.finish()));

        let mut ts_builder = Int64Builder::with_capacity(1);
        ts_builder.append_value(ts_ms);
        columns.push(Arc::new(ts_builder.finish()));

        RecordBatch::try_new(output_schema(schema), columns)
            .map_err(|e| SerdeError::MalformedInput(format!("failed to create RecordBatch: {e}")))
    }

    fn deserialize_batch(
        &self,
        records: &[&[u8]],
        schema: &SchemaRef,
    ) -> Result<RecordBatch, SerdeError> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(output_schema(schema)));
        }
        let batches = records
            .iter()
            .map(|record| self.deserialize(record, schema))
            .collect::<Result<Vec<_>, _>>()?;
        let schema = batches[0].schema();
        arrow_select::concat::concat_batches(&schema, &batches)
            .map_err(|error| SerdeError::MalformedInput(format!("failed to concat batch: {error}")))
    }

    fn format(&self) -> Format {
        Format::Debezium
    }
}

#[cfg(test)]
mod tests;
