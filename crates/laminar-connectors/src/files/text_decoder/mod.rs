//! Plain text line decoder implementing [`FormatDecoder`].
//!
//! Splits raw bytes by newlines and produces a single-column `RecordBatch`
//! with column `line: Utf8`.

use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::traits::FormatDecoder;
use crate::schema::types::RawRecord;

/// Decodes raw bytes as newline-delimited text into a single `line` column.
#[derive(Debug)]
pub struct TextLineDecoder {
    schema: SchemaRef,
}

impl TextLineDecoder {
    /// Creates a new text line decoder.
    #[must_use]
    pub fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new("line", DataType::Utf8, false)]));
        Self { schema }
    }
}

impl Default for TextLineDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl FormatDecoder for TextLineDecoder {
    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        // Validate UTF-8 upfront, then collect &str slices to avoid per-line allocation.
        let mut texts = Vec::with_capacity(records.len());
        for record in records {
            let text = std::str::from_utf8(&record.value).map_err(|e| {
                SchemaError::DecodeError(format!("invalid UTF-8 in text file: {e}"))
            })?;
            texts.push(text);
        }

        let lines: Vec<&str> = texts
            .iter()
            .flat_map(|t| t.lines())
            .filter(|l| !l.is_empty())
            .collect();
        let array = StringArray::from_iter_values(lines);
        RecordBatch::try_new(self.schema.clone(), vec![Arc::new(array)])
            .map_err(|e| SchemaError::DecodeError(format!("batch construction error: {e}")))
    }

    fn format_name(&self) -> &'static str {
        "text"
    }
}
#[cfg(test)]
mod tests;
