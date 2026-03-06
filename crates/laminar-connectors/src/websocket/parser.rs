//! Message parsing: WebSocket frames → Arrow `RecordBatch`.
//!
//! Converts incoming WebSocket text/binary messages into Arrow
//! `RecordBatch` rows for ingestion into Ring 0.

use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, StringBuilder};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::error::ConnectorError;
use crate::schema::json::decoder::{JsonDecoder, JsonDecoderConfig};
use crate::schema::traits::FormatDecoder;
use crate::schema::types::RawRecord;

use super::source_config::MessageFormat;

/// Parses raw WebSocket messages into Arrow `RecordBatch` data.
pub struct MessageParser {
    /// The output schema.
    schema: SchemaRef,
    /// The message format.
    format: MessageFormat,
    /// Type-aware JSON decoder (set for JSON/JsonLines formats).
    json_decoder: Option<JsonDecoder>,
}

impl MessageParser {
    /// Creates a new parser for the given schema, format, and JSON decoder config.
    #[must_use]
    pub fn new(
        schema: SchemaRef,
        format: MessageFormat,
        decoder_config: JsonDecoderConfig,
    ) -> Self {
        let json_decoder = match &format {
            MessageFormat::Json | MessageFormat::JsonLines => {
                Some(JsonDecoder::with_config(schema.clone(), decoder_config))
            }
            _ => None,
        };
        Self {
            schema,
            format,
            json_decoder,
        }
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
    /// Uses the type-aware [`JsonDecoder`] to coerce JSON values to the
    /// Arrow types declared in the schema.
    fn parse_json_batch(&self, messages: &[&[u8]]) -> Result<RecordBatch, ConnectorError> {
        let decoder = self.json_decoder.as_ref().ok_or_else(|| {
            ConnectorError::Internal("json_decoder not initialized for JSON format".into())
        })?;
        let records: Vec<RawRecord> = messages
            .iter()
            .map(|m| RawRecord::new(m.to_vec()))
            .collect();
        decoder.decode_batch(&records).map_err(ConnectorError::from)
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

/// Extracts the maximum event time (as epoch milliseconds) from a named column.
///
/// Supports `Int64` and `TimestampMillisecond` column types. Returns `None`
/// if the column is missing, has an unsupported type, or is entirely null.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn extract_max_event_time(batch: &RecordBatch, field: &str) -> Option<i64> {
    let col_idx = batch.schema().index_of(field).ok()?;
    let col = batch.column(col_idx);

    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
        (0..arr.len())
            .filter(|&i| !arr.is_null(i))
            .map(|i| arr.value(i))
            .max()
    } else if let Some(arr) = col
        .as_any()
        .downcast_ref::<arrow_array::TimestampMillisecondArray>()
    {
        (0..arr.len())
            .filter(|&i| !arr.is_null(i))
            .map(|i| arr.value(i))
            .max()
    } else {
        None
    }
}

/// Creates a default schema for JSON messages when no explicit schema is provided.
///
/// Uses schema inference from the first message. If `json_path` is provided,
/// navigates into the object before inferring fields.
///
/// # Errors
///
/// Returns `ConnectorError::Serde` if the sample is not valid UTF-8 or valid JSON,
/// or if the top-level value is not a JSON object.
pub fn infer_schema_from_json(sample: &[u8]) -> Result<SchemaRef, ConnectorError> {
    infer_schema_from_json_with_path(sample, None)
}

/// Like [`infer_schema_from_json`] but navigates a `json.path` first.
///
/// # Errors
///
/// Returns `ConnectorError::Serde` if the sample is not valid UTF-8 or valid JSON,
/// if a path segment is not found, or if the target is not a JSON object.
pub fn infer_schema_from_json_with_path(
    sample: &[u8],
    json_path: Option<&[String]>,
) -> Result<SchemaRef, ConnectorError> {
    let text = std::str::from_utf8(sample).map_err(|e| {
        ConnectorError::Serde(crate::error::SerdeError::MalformedInput(format!(
            "invalid UTF-8: {e}"
        )))
    })?;

    let value: serde_json::Value = serde_json::from_str(text)
        .map_err(|e| ConnectorError::Serde(crate::error::SerdeError::Json(e.to_string())))?;

    let target = if let Some(path) = json_path {
        let mut current = &value;
        for segment in path {
            current = current.get(segment.as_str()).ok_or_else(|| {
                ConnectorError::Serde(crate::error::SerdeError::MalformedInput(format!(
                    "json.path segment '{segment}' not found during inference"
                )))
            })?;
        }
        current
    } else {
        &value
    };

    let obj = target.as_object().ok_or_else(|| {
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
        let parser = MessageParser::new(
            json_schema(),
            MessageFormat::Json,
            JsonDecoderConfig::default(),
        );
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
        let parser = MessageParser::new(
            json_schema(),
            MessageFormat::Json,
            JsonDecoderConfig::default(),
        );
        let messages: Vec<&[u8]> = vec![br#"{"id": "1"}"#];

        let batch = parser.parse_batch(&messages).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column(1).is_null(0));
    }

    #[test]
    fn test_parse_json_numeric_values() {
        let parser = MessageParser::new(
            json_schema(),
            MessageFormat::Json,
            JsonDecoderConfig::default(),
        );
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
        let parser =
            MessageParser::new(schema, MessageFormat::Binary, JsonDecoderConfig::default());
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
            JsonDecoderConfig::default(),
        );
        let messages: Vec<&[u8]> = vec![b"1,hello", b"2,world"];

        let batch = parser.parse_batch(&messages).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_parse_empty() {
        let parser = MessageParser::new(
            json_schema(),
            MessageFormat::Json,
            JsonDecoderConfig::default(),
        );
        let messages: Vec<&[u8]> = vec![];

        let batch = parser.parse_batch(&messages).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_parse_invalid_json() {
        let parser = MessageParser::new(
            json_schema(),
            MessageFormat::Json,
            JsonDecoderConfig::default(),
        );
        let messages: Vec<&[u8]> = vec![b"not json"];

        assert!(parser.parse_batch(&messages).is_err());
    }

    #[test]
    fn test_parse_json_typed_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let parser = MessageParser::new(schema, MessageFormat::Json, JsonDecoderConfig::default());
        let messages: Vec<&[u8]> = vec![
            br#"{"id": 1, "price": 99.5, "name": "Widget"}"#,
            br#"{"id": 2, "price": 10.0, "name": "Gadget"}"#,
        ];

        let batch = parser.parse_batch(&messages).unwrap();
        assert_eq!(batch.num_rows(), 2);

        // Columns should have the declared types, not Utf8.
        assert_eq!(batch.column(0).data_type(), &DataType::Int64);
        assert_eq!(batch.column(1).data_type(), &DataType::Float64);
        assert_eq!(batch.column(2).data_type(), &DataType::Utf8);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
    }

    #[test]
    fn test_parse_json_coerces_string_numbers() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Float64,
            false,
        )]));
        let parser = MessageParser::new(schema, MessageFormat::Json, JsonDecoderConfig::default());
        let messages: Vec<&[u8]> = vec![br#"{"price": "187.52"}"#];

        let batch = parser.parse_batch(&messages).unwrap();
        assert_eq!(batch.column(0).data_type(), &DataType::Float64);
        let prices = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        assert!((prices.value(0) - 187.52).abs() < f64::EPSILON);
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

    #[test]
    fn test_extract_max_event_time_int64() {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let ts = arrow_array::Int64Array::from(vec![1000, 3000, 2000]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ts)]).unwrap();

        assert_eq!(extract_max_event_time(&batch, "ts"), Some(3000));
    }

    #[test]
    fn test_extract_max_event_time_missing_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ids = arrow_array::Int64Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ids)]).unwrap();

        assert_eq!(extract_max_event_time(&batch, "ts"), None);
    }

    #[test]
    fn test_extract_max_event_time_with_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
        let ts = arrow_array::Int64Array::from(vec![Some(1000), None, Some(3000), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ts)]).unwrap();

        assert_eq!(extract_max_event_time(&batch, "ts"), Some(3000));
    }

    #[test]
    fn test_extract_max_event_time_unsupported_type() {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Utf8, false)]));
        let ts = arrow_array::StringArray::from(vec!["2026-01-01"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ts)]).unwrap();

        assert_eq!(extract_max_event_time(&batch, "ts"), None);
    }
}
