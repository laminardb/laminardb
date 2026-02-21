//! Message parsing: WebSocket frames → Arrow `RecordBatch`.
//!
//! Converts incoming WebSocket text/binary messages into Arrow
//! `RecordBatch` rows for ingestion into Ring 0.

use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, StringBuilder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::error::ConnectorError;

use super::source_config::MessageFormat;

/// Parses raw WebSocket messages into Arrow `RecordBatch` data.
pub struct MessageParser {
    /// The output schema.
    schema: SchemaRef,
    /// The message format.
    format: MessageFormat,
}

impl MessageParser {
    /// Creates a new parser for the given schema and format.
    #[must_use]
    pub fn new(schema: SchemaRef, format: MessageFormat) -> Self {
        Self { schema, format }
    }

    /// Returns the output schema.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Parses a batch of raw message payloads into a `RecordBatch`.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::Serde` if parsing fails.
    pub fn parse_batch(&self, messages: &[&[u8]]) -> Result<RecordBatch, ConnectorError> {
        if messages.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        match &self.format {
            MessageFormat::Json | MessageFormat::JsonLines => self.parse_json_batch(messages),
            MessageFormat::Binary => self.parse_binary_batch(messages),
            MessageFormat::Csv { delimiter, .. } => self.parse_csv_batch(messages, *delimiter),
        }
    }

    /// Parses JSON messages into a `RecordBatch`.
    ///
    /// Each message is expected to be a JSON object whose keys map to the
    /// schema field names. Missing fields become null.
    fn parse_json_batch(&self, messages: &[&[u8]]) -> Result<RecordBatch, ConnectorError> {
        let mut builders: Vec<StringBuilder> = self
            .schema
            .fields()
            .iter()
            .map(|_| StringBuilder::with_capacity(messages.len(), messages.len() * 64))
            .collect();

        for msg in messages {
            let text = std::str::from_utf8(msg).map_err(|e| {
                ConnectorError::Serde(crate::error::SerdeError::MalformedInput(format!(
                    "invalid UTF-8 in JSON message: {e}"
                )))
            })?;

            let value: serde_json::Value = serde_json::from_str(text).map_err(|e| {
                ConnectorError::Serde(crate::error::SerdeError::Json(e.to_string()))
            })?;

            let obj = value.as_object().ok_or_else(|| {
                ConnectorError::Serde(crate::error::SerdeError::MalformedInput(
                    "expected JSON object".into(),
                ))
            })?;

            for (i, field) in self.schema.fields().iter().enumerate() {
                match obj.get(field.name()) {
                    Some(serde_json::Value::Null) | None => builders[i].append_null(),
                    Some(v) => {
                        let s = match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        builders[i].append_value(&s);
                    }
                }
            }
        }

        let arrays: Vec<Arc<dyn arrow_array::Array>> = builders
            .into_iter()
            .map(|mut b| Arc::new(b.finish()) as _)
            .collect();

        RecordBatch::try_new(self.schema.clone(), arrays).map_err(|e| {
            ConnectorError::Serde(crate::error::SerdeError::Json(format!(
                "failed to build RecordBatch: {e}"
            )))
        })
    }

    /// Parses binary messages — each message becomes a single row with a
    /// `LargeBinary` column named "payload".
    #[allow(clippy::unused_self)]
    fn parse_binary_batch(&self, messages: &[&[u8]]) -> Result<RecordBatch, ConnectorError> {
        let mut builder =
            BinaryBuilder::with_capacity(messages.len(), messages.iter().map(|m| m.len()).sum());
        for msg in messages {
            builder.append_value(msg);
        }

        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            false,
        )]));
        let arrays: Vec<Arc<dyn arrow_array::Array>> = vec![Arc::new(builder.finish())];

        RecordBatch::try_new(schema, arrays).map_err(|e| {
            ConnectorError::Serde(crate::error::SerdeError::MalformedInput(format!(
                "failed to build binary RecordBatch: {e}"
            )))
        })
    }

    /// Parses CSV text messages into a `RecordBatch`.
    ///
    /// Each message is treated as one or more CSV rows.
    fn parse_csv_batch(
        &self,
        messages: &[&[u8]],
        delimiter: char,
    ) -> Result<RecordBatch, ConnectorError> {
        let mut builders: Vec<StringBuilder> = self
            .schema
            .fields()
            .iter()
            .map(|_| StringBuilder::with_capacity(messages.len(), messages.len() * 32))
            .collect();

        let num_fields = self.schema.fields().len();

        for msg in messages {
            let text = std::str::from_utf8(msg).map_err(|e| {
                ConnectorError::Serde(crate::error::SerdeError::MalformedInput(format!(
                    "invalid UTF-8 in CSV message: {e}"
                )))
            })?;

            for line in text.lines() {
                if line.trim().is_empty() {
                    continue;
                }

                let fields: Vec<&str> = line.split(delimiter).collect();
                for (i, builder) in builders.iter_mut().enumerate().take(num_fields) {
                    if i < fields.len() {
                        builder.append_value(fields[i].trim());
                    } else {
                        builder.append_null();
                    }
                }
            }
        }

        let arrays: Vec<Arc<dyn arrow_array::Array>> = builders
            .into_iter()
            .map(|mut b| Arc::new(b.finish()) as _)
            .collect();

        RecordBatch::try_new(self.schema.clone(), arrays).map_err(|e| {
            ConnectorError::Serde(crate::error::SerdeError::Csv(format!(
                "failed to build CSV RecordBatch: {e}"
            )))
        })
    }
}

/// Creates a default schema for JSON messages when no explicit schema is provided.
///
/// Uses schema inference from the first message.
///
/// # Errors
///
/// Returns `ConnectorError::Serde` if the sample is not valid UTF-8 or valid JSON,
/// or if the top-level value is not a JSON object.
pub fn infer_schema_from_json(sample: &[u8]) -> Result<SchemaRef, ConnectorError> {
    let text = std::str::from_utf8(sample).map_err(|e| {
        ConnectorError::Serde(crate::error::SerdeError::MalformedInput(format!(
            "invalid UTF-8: {e}"
        )))
    })?;

    let value: serde_json::Value = serde_json::from_str(text)
        .map_err(|e| ConnectorError::Serde(crate::error::SerdeError::Json(e.to_string())))?;

    let obj = value.as_object().ok_or_else(|| {
        ConnectorError::Serde(crate::error::SerdeError::MalformedInput(
            "schema inference requires a JSON object".into(),
        ))
    })?;

    let fields: Vec<Field> = obj
        .iter()
        .map(|(key, val)| {
            let dt = match val {
                serde_json::Value::Bool(_) => DataType::Boolean,
                serde_json::Value::Number(n) => {
                    if n.is_f64() {
                        DataType::Float64
                    } else {
                        DataType::Int64
                    }
                }
                _ => DataType::Utf8,
            };
            Field::new(key, dt, true)
        })
        .collect();

    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn test_parse_json_batch() {
        let parser = MessageParser::new(json_schema(), MessageFormat::Json);
        let messages: Vec<&[u8]> = vec![
            br#"{"id": "1", "value": "hello"}"#,
            br#"{"id": "2", "value": "world"}"#,
        ];

        let batch = parser.parse_batch(&messages).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_parse_json_missing_field() {
        let parser = MessageParser::new(json_schema(), MessageFormat::Json);
        let messages: Vec<&[u8]> = vec![br#"{"id": "1"}"#];

        let batch = parser.parse_batch(&messages).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column(1).is_null(0));
    }

    #[test]
    fn test_parse_json_numeric_values() {
        let parser = MessageParser::new(json_schema(), MessageFormat::Json);
        let messages: Vec<&[u8]> = vec![br#"{"id": "1", "value": 42}"#];

        let batch = parser.parse_batch(&messages).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_parse_binary_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            false,
        )]));
        let parser = MessageParser::new(schema, MessageFormat::Binary);
        let messages: Vec<&[u8]> = vec![b"hello", b"world"];

        let batch = parser.parse_batch(&messages).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_parse_csv_batch() {
        let parser = MessageParser::new(
            json_schema(),
            MessageFormat::Csv {
                delimiter: ',',
                has_header: false,
            },
        );
        let messages: Vec<&[u8]> = vec![b"1,hello", b"2,world"];

        let batch = parser.parse_batch(&messages).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_parse_empty() {
        let parser = MessageParser::new(json_schema(), MessageFormat::Json);
        let messages: Vec<&[u8]> = vec![];

        let batch = parser.parse_batch(&messages).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_parse_invalid_json() {
        let parser = MessageParser::new(json_schema(), MessageFormat::Json);
        let messages: Vec<&[u8]> = vec![b"not json"];

        assert!(parser.parse_batch(&messages).is_err());
    }

    #[test]
    fn test_infer_schema() {
        let sample = br#"{"name": "Alice", "age": 30, "active": true, "score": 99.5}"#;
        let schema = infer_schema_from_json(sample).unwrap();

        assert_eq!(schema.fields().len(), 4);
        let age_field = schema.field_with_name("age").unwrap();
        assert_eq!(age_field.data_type(), &DataType::Int64);
        let active_field = schema.field_with_name("active").unwrap();
        assert_eq!(active_field.data_type(), &DataType::Boolean);
        let score_field = schema.field_with_name("score").unwrap();
        assert_eq!(score_field.data_type(), &DataType::Float64);
    }
}
