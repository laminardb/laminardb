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

    fn format_name(&self) -> &str {
        "text"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;

    #[test]
    fn test_empty_input() {
        let decoder = TextLineDecoder::new();
        let batch = decoder.decode_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().fields().len(), 1);
        assert_eq!(batch.schema().field(0).name(), "line");
    }

    #[test]
    fn test_single_record_multiple_lines() {
        let decoder = TextLineDecoder::new();
        let record = RawRecord::new(b"hello\nworld\nfoo".to_vec());
        let batch = decoder.decode_batch(&[record]).unwrap();
        assert_eq!(batch.num_rows(), 3);
        let col = batch.column(0).as_string::<i32>();
        assert_eq!(col.value(0), "hello");
        assert_eq!(col.value(1), "world");
        assert_eq!(col.value(2), "foo");
    }

    #[test]
    fn test_skips_empty_lines() {
        let decoder = TextLineDecoder::new();
        let record = RawRecord::new(b"a\n\nb\n".to_vec());
        let batch = decoder.decode_batch(&[record]).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_multiple_records() {
        let decoder = TextLineDecoder::new();
        let r1 = RawRecord::new(b"line1\nline2".to_vec());
        let r2 = RawRecord::new(b"line3".to_vec());
        let batch = decoder.decode_batch(&[r1, r2]).unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_format_name() {
        let decoder = TextLineDecoder::new();
        assert_eq!(decoder.format_name(), "text");
    }

    #[test]
    fn test_schema() {
        let decoder = TextLineDecoder::new();
        let schema = decoder.output_schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "line");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    }
}
