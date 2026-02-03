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

        // Deserialize the payload as a JSON record
        let payload_bytes =
            serde_json::to_vec(payload).map_err(|e| SerdeError::Json(e.to_string()))?;
        let data_batch = self.json_deser.deserialize(&payload_bytes, schema)?;

        // Append __op and __ts_ms columns
        let mut columns: Vec<ArrayRef> = data_batch.columns().to_vec();

        let mut op_builder = StringBuilder::with_capacity(1, 1);
        op_builder.append_value(op.as_str());
        columns.push(Arc::new(op_builder.finish()));

        let mut ts_builder = Int64Builder::with_capacity(1);
        ts_builder.append_value(ts_ms);
        columns.push(Arc::new(ts_builder.finish()));

        // Build extended schema with metadata columns
        let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new("__op", DataType::Utf8, false)));
        fields.push(Arc::new(Field::new("__ts_ms", DataType::Int64, false)));
        let extended_schema = Arc::new(Schema::new(fields));

        RecordBatch::try_new(extended_schema, columns)
            .map_err(|e| SerdeError::MalformedInput(format!("failed to create RecordBatch: {e}")))
    }

    fn format(&self) -> Format {
        Format::Debezium
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::Field;

    fn user_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    #[test]
    fn test_debezium_insert() {
        let deser = DebeziumDeserializer::new();
        let schema = user_schema();

        let data = br#"{
            "before": null,
            "after": {"id": 1, "name": "Alice"},
            "op": "c",
            "ts_ms": 1700000000000
        }"#;

        let batch = deser.deserialize(data, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        // data columns + __op + __ts_ms
        assert_eq!(batch.num_columns(), 4);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);

        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");

        let ops = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ops.value(0), "c");

        let ts = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 1_700_000_000_000);
    }

    #[test]
    fn test_debezium_update() {
        let deser = DebeziumDeserializer::new();
        let schema = user_schema();

        let data = br#"{
            "before": {"id": 1, "name": "Alice"},
            "after": {"id": 1, "name": "Alicia"},
            "op": "u",
            "ts_ms": 1700000001000
        }"#;

        let batch = deser.deserialize(data, &schema).unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alicia");

        let ops = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ops.value(0), "u");
    }

    #[test]
    fn test_debezium_delete() {
        let deser = DebeziumDeserializer::new();
        let schema = user_schema();

        let data = br#"{
            "before": {"id": 1, "name": "Alice"},
            "after": null,
            "op": "d",
            "ts_ms": 1700000002000
        }"#;

        let batch = deser.deserialize(data, &schema).unwrap();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);

        let ops = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ops.value(0), "d");
    }

    #[test]
    fn test_debezium_invalid_op() {
        let deser = DebeziumDeserializer::new();
        let schema = user_schema();

        let data = br#"{
            "before": null,
            "after": {"id": 1, "name": "Alice"},
            "op": "x",
            "ts_ms": 1700000000000
        }"#;

        let result = deser.deserialize(data, &schema);
        assert!(result.is_err());
    }
}
