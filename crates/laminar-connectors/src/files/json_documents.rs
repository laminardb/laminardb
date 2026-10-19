//! JSON file payloads: one or more top-level JSON objects per file, written one per
//! line (JSON Lines) or concatenated.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use serde::de::IgnoredAny;

use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::traits::FormatDecoder;
use crate::schema::types::RawRecord;
use crate::schema::JsonDecoder;

/// Decodes every top-level JSON object in a file as one row.
pub(crate) struct JsonDocumentsDecoder {
    rows: JsonDecoder,
}

impl JsonDocumentsDecoder {
    pub(crate) fn new(rows: JsonDecoder) -> Self {
        Self { rows }
    }
}

impl FormatDecoder for JsonDocumentsDecoder {
    fn output_schema(&self) -> SchemaRef {
        self.rows.output_schema()
    }

    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch> {
        let mut documents = Vec::new();
        for record in records {
            documents.extend(split_documents(&record.value)?);
        }
        self.rows.decode_slices(&documents)
    }

    fn format_name(&self) -> &'static str {
        self.rows.format_name()
    }
}

/// Returns the byte range of each top-level JSON value in `file`.
fn split_documents(file: &[u8]) -> SchemaResult<Vec<&[u8]>> {
    let mut stream = serde_json::Deserializer::from_slice(file).into_iter::<IgnoredAny>();
    let mut documents = Vec::new();
    let mut start = 0;
    while let Some(value) = stream.next() {
        value.map_err(|error| SchemaError::DecodeError(format!("invalid JSON: {error}")))?;
        let end = stream.byte_offset();
        let document = file[start..end].trim_ascii_start();
        if document.first() == Some(&b'[') {
            return Err(SchemaError::DecodeError(
                "top-level JSON arrays are not supported; write one object per line".into(),
            ));
        }
        documents.push(document);
        start = end;
    }
    Ok(documents)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn decoder() -> JsonDocumentsDecoder {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        JsonDocumentsDecoder::new(JsonDecoder::new(schema))
    }

    fn ids(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column(0)
            .as_primitive::<Int64Type>()
            .values()
            .to_vec()
    }

    #[test]
    fn test_decode_json_lines_file_yields_row_per_line() {
        let file =
            RawRecord::new(b"{\"id\":1,\"name\":\"a\"}\n{\"id\":2}\n\n{\"id\":3}\n".to_vec());
        let batch = decoder().decode_batch(&[file]).unwrap();
        assert_eq!(ids(&batch), [1, 2, 3]);
        assert!(batch.column(1).is_null(1));
    }

    #[test]
    fn test_decode_pretty_printed_single_object() {
        let file = RawRecord::new(b"{\n  \"id\": 7,\n  \"name\": \"b\"\n}\n".to_vec());
        let batch = decoder().decode_batch(&[file]).unwrap();
        assert_eq!(ids(&batch), [7]);
    }

    #[test]
    fn test_decode_concatenated_objects() {
        let file = RawRecord::new(b"{\"id\":1} {\"id\":2}{\"id\":3}".to_vec());
        let batch = decoder().decode_batch(&[file]).unwrap();
        assert_eq!(ids(&batch), [1, 2, 3]);
    }

    #[test]
    fn test_decode_empty_file_yields_no_rows() {
        let batch = decoder()
            .decode_batch(&[RawRecord::new(b" \n".to_vec())])
            .unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_decode_rejects_top_level_array() {
        let file = RawRecord::new(b"[{\"id\":1},{\"id\":2}]".to_vec());
        let error = decoder().decode_batch(&[file]).unwrap_err();
        assert!(
            error.to_string().contains("top-level JSON arrays"),
            "{error}"
        );
    }

    #[test]
    fn test_decode_reports_invalid_line_position() {
        let file = RawRecord::new(b"{\"id\":1}\n{\"id\":}\n".to_vec());
        let error = decoder().decode_batch(&[file]).unwrap_err();
        assert!(error.to_string().contains("line 2"), "{error}");
    }
}
