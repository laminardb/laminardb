//! Arrow `RecordBatch` → WebSocket message serialization.
//!
//! Converts Arrow data into JSON or binary formats for delivery
//! to connected WebSocket clients.

use arrow_array::RecordBatch;
use arrow_cast::display::{ArrayFormatter, FormatOptions};

use crate::error::ConnectorError;

use super::sink_config::SinkFormat;

/// Serializes Arrow `RecordBatch` data into WebSocket message payloads.
pub struct BatchSerializer {
    /// Output format.
    format: SinkFormat,
}

impl BatchSerializer {
    /// Creates a new serializer for the given format.
    #[must_use]
    pub fn new(format: SinkFormat) -> Self {
        Self { format }
    }

    /// Returns the configured format.
    #[must_use]
    pub fn format(&self) -> &SinkFormat {
        &self.format
    }

    /// Serializes a `RecordBatch` into a JSON value suitable for the subscription protocol.
    ///
    /// Returns a `serde_json::Value` array where each element is a JSON object
    /// representing one row.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if serialization fails.
    pub fn serialize_to_json(
        &self,
        batch: &RecordBatch,
    ) -> Result<serde_json::Value, ConnectorError> {
        let schema = batch.schema();
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        let formatters: Vec<ArrayFormatter<'_>> = (0..num_cols)
            .map(|i| {
                ArrayFormatter::try_new(batch.column(i), &FormatOptions::default()).map_err(|e| {
                    ConnectorError::Internal(format!(
                        "failed to create formatter for column {i}: {e}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut rows = Vec::with_capacity(num_rows);

        for row in 0..num_rows {
            let mut obj = serde_json::Map::with_capacity(num_cols);
            for (col, formatter) in formatters.iter().enumerate() {
                let field = schema.field(col);
                if batch.column(col).is_null(row) {
                    obj.insert(field.name().clone(), serde_json::Value::Null);
                } else {
                    let value_str = formatter.value(row).to_string();
                    // Try to parse as number or boolean for cleaner JSON
                    let json_val = if let Ok(n) = value_str.parse::<i64>() {
                        serde_json::Value::Number(n.into())
                    } else if let Ok(n) = value_str.parse::<f64>() {
                        serde_json::Number::from_f64(n).map_or_else(
                            || serde_json::Value::String(value_str.clone()),
                            serde_json::Value::Number,
                        )
                    } else if value_str == "true" {
                        serde_json::Value::Bool(true)
                    } else if value_str == "false" {
                        serde_json::Value::Bool(false)
                    } else {
                        serde_json::Value::String(value_str)
                    };
                    obj.insert(field.name().clone(), json_val);
                }
            }
            rows.push(serde_json::Value::Object(obj));
        }

        Ok(serde_json::Value::Array(rows))
    }

    /// Serializes a `RecordBatch` into per-row JSON strings.
    ///
    /// Each returned `String` is a complete JSON object for one row.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if serialization fails.
    pub fn serialize_rows(&self, batch: &RecordBatch) -> Result<Vec<String>, ConnectorError> {
        match self.format {
            SinkFormat::Json | SinkFormat::JsonLines => {
                let json_val = self.serialize_to_json(batch)?;
                match json_val {
                    serde_json::Value::Array(rows) => rows
                        .into_iter()
                        .map(|v| {
                            serde_json::to_string(&v).map_err(|e| {
                                ConnectorError::Serde(crate::error::SerdeError::Json(e.to_string()))
                            })
                        })
                        .collect(),
                    _ => Err(ConnectorError::Internal(
                        "expected array from serialize_to_json".into(),
                    )),
                }
            }
            SinkFormat::ArrowIpc | SinkFormat::Binary => {
                // For binary formats, serialize the entire batch as one message
                let json_val = self.serialize_to_json(batch)?;
                let s = serde_json::to_string(&json_val).map_err(|e| {
                    ConnectorError::Serde(crate::error::SerdeError::Json(e.to_string()))
                })?;
                Ok(vec![s])
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_serialize_to_json() {
        let serializer = BatchSerializer::new(SinkFormat::Json);
        let batch = test_batch();
        let json = serializer.serialize_to_json(&batch).unwrap();

        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0]["id"], 1);
        assert_eq!(arr[0]["name"], "Alice");
        assert_eq!(arr[2]["name"], "Charlie");
    }

    #[test]
    fn test_serialize_rows() {
        let serializer = BatchSerializer::new(SinkFormat::Json);
        let batch = test_batch();
        let rows = serializer.serialize_rows(&batch).unwrap();

        assert_eq!(rows.len(), 3);
        let parsed: serde_json::Value = serde_json::from_str(&rows[0]).unwrap();
        assert_eq!(parsed["id"], 1);
        assert_eq!(parsed["name"], "Alice");
    }

    #[test]
    fn test_serialize_empty_batch() {
        let serializer = BatchSerializer::new(SinkFormat::Json);
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::new_empty(schema);
        let json = serializer.serialize_to_json(&batch).unwrap();
        assert_eq!(json.as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_serialize_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec![Some("Alice"), None])),
            ],
        )
        .unwrap();

        let serializer = BatchSerializer::new(SinkFormat::Json);
        let json = serializer.serialize_to_json(&batch).unwrap();
        let arr = json.as_array().unwrap();
        assert!(arr[1]["id"].is_null());
        assert!(arr[1]["name"].is_null());
    }
}
