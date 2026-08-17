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
mod tests;
