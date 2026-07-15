//! Message parsing: WebSocket frames → Arrow `RecordBatch`.
//!
//! Converts incoming WebSocket text/binary messages into Arrow
//! `RecordBatch` rows for ingestion into Ring 0.

use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, LargeBinaryBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};

use crate::error::ConnectorError;
use crate::schema::csv::{CsvDecoder, CsvDecoderConfig};
use crate::schema::json::decoder::{JsonDecoder, JsonDecoderConfig};

use super::source_config::MessageFormat;

/// Parses raw WebSocket messages into Arrow `RecordBatch` data.
pub struct MessageParser {
    /// The output schema.
    schema: SchemaRef,
    /// The message format.
    format: MessageFormat,
    /// Type-aware JSON decoder (set for JSON format).
    json_decoder: Option<JsonDecoder>,
    /// Type-aware CSV decoder (set for CSV format).
    csv_decoder: Option<CsvDecoder>,
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
            MessageFormat::Json => Some(JsonDecoder::with_config(schema.clone(), decoder_config)),
            _ => None,
        };
        let csv_decoder = match &format {
            MessageFormat::Csv {
                delimiter,
                has_header,
            } => {
                // `parse_format` hard-codes the CSV delimiter to ',', so this
                // is ASCII by construction. If the delimiter ever becomes
                // user-configurable, validate `is_ascii()` at config parse.
                #[allow(clippy::cast_possible_truncation)]
                let csv_config = CsvDecoderConfig {
                    delimiter: *delimiter as u8,
                    has_header: *has_header,
                    ..CsvDecoderConfig::default()
                };
                Some(CsvDecoder::with_config(schema.clone(), csv_config))
            }
            _ => None,
        };
        Self {
            schema,
            format,
            json_decoder,
            csv_decoder,
        }
    }

    /// Validates format-specific requirements against the declared schema.
    ///
    /// Binary messages map one frame to one row, so the schema must contain
    /// exactly one `Binary` or `LargeBinary` field.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::SchemaMismatch` when the schema cannot
    /// represent the configured message format without coercion.
    pub fn validate_format_schema(
        schema: &SchemaRef,
        format: &MessageFormat,
    ) -> Result<(), ConnectorError> {
        if !matches!(format, MessageFormat::Binary) {
            return Ok(());
        }

        if schema.fields().len() != 1 {
            return Err(ConnectorError::SchemaMismatch(format!(
                "WebSocket binary format requires exactly one Binary or LargeBinary field, got {} fields",
                schema.fields().len()
            )));
        }

        let field = schema.field(0);
        if !matches!(field.data_type(), DataType::Binary | DataType::LargeBinary) {
            return Err(ConnectorError::SchemaMismatch(format!(
                "WebSocket binary field '{}' must be Binary or LargeBinary, got {}",
                field.name(),
                field.data_type()
            )));
        }

        Ok(())
    }

    /// Parses a batch of raw message payloads into a `RecordBatch`.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::Serde` if parsing fails.
    #[cfg(test)]
    pub fn parse_batch(&self, messages: &[&[u8]]) -> Result<RecordBatch, ConnectorError> {
        self.parse_batch_bounded(messages, usize::MAX)
    }

    /// Parses messages while bounding the number of produced Arrow rows.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::Serde` if parsing fails or JSON expansion
    /// exceeds `max_rows`.
    pub fn parse_batch_bounded(
        &self,
        messages: &[&[u8]],
        max_rows: usize,
    ) -> Result<RecordBatch, ConnectorError> {
        Self::validate_format_schema(&self.schema, &self.format)?;

        if messages.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        match &self.format {
            MessageFormat::Json => self.parse_json_batch(messages, max_rows),
            MessageFormat::Binary => self.parse_binary_batch(messages),
            MessageFormat::Csv { .. } => self.parse_csv_batch(messages),
        }
    }

    /// Parses JSON messages into a `RecordBatch`.
    ///
    /// Uses the type-aware [`JsonDecoder`] to coerce JSON values to the
    /// Arrow types declared in the schema.
    fn parse_json_batch(
        &self,
        messages: &[&[u8]],
        max_rows: usize,
    ) -> Result<RecordBatch, ConnectorError> {
        let decoder = self.json_decoder.as_ref().ok_or_else(|| {
            ConnectorError::Internal("json_decoder not initialized for JSON format".into())
        })?;
        decoder
            .decode_slices_bounded(messages, max_rows)
            .map_err(ConnectorError::from)
    }

    /// Parses binary messages into the single declared binary field.
    fn parse_binary_batch(&self, messages: &[&[u8]]) -> Result<RecordBatch, ConnectorError> {
        let total_bytes = messages.iter().try_fold(0_usize, |total, message| {
            total.checked_add(message.len()).ok_or_else(|| {
                ConnectorError::Serde(crate::error::SerdeError::MalformedInput(
                    "WebSocket binary batch size overflow".into(),
                ))
            })
        })?;

        let array: ArrayRef = match self.schema.field(0).data_type() {
            DataType::Binary => {
                if total_bytes > i32::MAX as usize {
                    return Err(ConnectorError::Serde(
                        crate::error::SerdeError::MalformedInput(format!(
                            "WebSocket binary batch contains {total_bytes} bytes, exceeding the Binary offset limit; declare LargeBinary"
                        )),
                    ));
                }
                let mut builder = BinaryBuilder::with_capacity(messages.len(), total_bytes);
                for message in messages {
                    builder.append_value(message);
                }
                Arc::new(builder.finish())
            }
            DataType::LargeBinary => {
                let mut builder = LargeBinaryBuilder::with_capacity(messages.len(), total_bytes);
                for message in messages {
                    builder.append_value(message);
                }
                Arc::new(builder.finish())
            }
            _ => {
                return Err(ConnectorError::Internal(
                    "binary schema was not validated before parsing".into(),
                ));
            }
        };

        RecordBatch::try_new(self.schema.clone(), vec![array]).map_err(|e| {
            ConnectorError::Serde(crate::error::SerdeError::MalformedInput(format!(
                "failed to build binary RecordBatch: {e}"
            )))
        })
    }

    /// Parses CSV text messages into a `RecordBatch`.
    ///
    /// Delegates to [`CsvDecoder`] for schema-directed type coercion.
    fn parse_csv_batch(&self, messages: &[&[u8]]) -> Result<RecordBatch, ConnectorError> {
        let decoder = self.csv_decoder.as_ref().ok_or_else(|| {
            ConnectorError::Internal("csv_decoder not initialized for CSV format".into())
        })?;
        decoder
            .decode_slices(messages)
            .map_err(ConnectorError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_schema::{Field, Schema};

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
    fn json_explode_cannot_exceed_the_actor_row_budget() {
        let parser = MessageParser::new(
            json_schema(),
            MessageFormat::Json,
            JsonDecoderConfig {
                json_explode: Some(vec!["id".into(), "value".into()]),
                ..JsonDecoderConfig::default()
            },
        );
        let message = br#"[["1","a"],["2","b"],["3","c"]]"#;

        let error = parser
            .parse_batch_bounded(&[message], 2)
            .unwrap_err()
            .to_string();

        assert!(error.contains("2-row batch limit"), "{error}");
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
            "frame",
            DataType::Binary,
            false,
        )]));
        let parser = MessageParser::new(
            schema.clone(),
            MessageFormat::Binary,
            JsonDecoderConfig::default(),
        );
        let messages: Vec<&[u8]> = vec![b"hello", b"world"];

        let batch = parser.parse_batch(&messages).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema(), schema);
        let frames = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();
        assert_eq!(frames.value(0), b"hello");
        assert_eq!(frames.value(1), b"world");
    }

    #[test]
    fn test_parse_large_binary_preserves_declared_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "packet",
            DataType::LargeBinary,
            true,
        )]));
        let parser = MessageParser::new(
            schema.clone(),
            MessageFormat::Binary,
            JsonDecoderConfig::default(),
        );
        let messages: Vec<&[u8]> = vec![b"one", b"two"];

        let batch = parser.parse_batch(&messages).unwrap();

        assert_eq!(batch.schema(), schema);
        let packets = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .unwrap();
        assert_eq!(packets.value(0), b"one");
        assert_eq!(packets.value(1), b"two");
    }

    #[test]
    fn test_binary_format_rejects_ambiguous_or_non_binary_schemas() {
        let schemas = [
            Arc::new(Schema::empty()),
            Arc::new(Schema::new(vec![
                Field::new("first", DataType::Binary, false),
                Field::new("second", DataType::Binary, false),
            ])),
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Utf8,
                false,
            )])),
        ];

        for schema in schemas {
            let error =
                MessageParser::validate_format_schema(&schema, &MessageFormat::Binary).unwrap_err();
            assert!(matches!(error, ConnectorError::SchemaMismatch(_)));
        }
    }

    #[test]
    fn test_invalid_binary_schema_fails_even_for_empty_input() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )]));
        let parser =
            MessageParser::new(schema, MessageFormat::Binary, JsonDecoderConfig::default());

        assert!(matches!(
            parser.parse_batch(&[]),
            Err(ConnectorError::SchemaMismatch(_))
        ));
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
}
