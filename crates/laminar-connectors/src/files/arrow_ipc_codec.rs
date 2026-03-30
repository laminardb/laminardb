//! Arrow IPC file format decoder and encoder.

use std::io::Cursor;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::traits::{FormatDecoder, FormatEncoder};
use crate::schema::types::RawRecord;

/// Decodes Arrow IPC file bytes into `RecordBatch`es.
///
/// The constructor schema is used for `output_schema()` and empty-batch
/// returns. Actual decoded batches carry the file's embedded schema
/// (same contract as `ParquetDecoder`).
pub struct ArrowIpcDecoder {
    schema: SchemaRef,
}

impl ArrowIpcDecoder {
    /// Creates a decoder with the given declared schema.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl std::fmt::Debug for ArrowIpcDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowIpcDecoder")
            .field("schema", &self.schema)
            .finish()
    }
}

impl FormatDecoder for ArrowIpcDecoder {
    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let mut combined = Vec::with_capacity(records.iter().map(|r| r.value.len()).sum());
        for record in records {
            combined.extend_from_slice(&record.value);
        }

        let cursor = Cursor::new(&combined);
        let reader = arrow_ipc::reader::FileReader::try_new(cursor, None)
            .map_err(|e| SchemaError::DecodeError(format!("Arrow IPC read error: {e}")))?;

        let file_schema = reader.schema();

        let mut batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| SchemaError::DecodeError(format!("Arrow IPC batch error: {e}")))?;
            batches.push(batch);
        }

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(file_schema));
        }

        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }

        arrow_select::concat::concat_batches(&file_schema, &batches)
            .map_err(|e| SchemaError::DecodeError(format!("Arrow IPC concat error: {e}")))
    }

    fn format_name(&self) -> &str {
        "arrow_ipc"
    }
}

/// Encodes `RecordBatch`es into Arrow IPC file format bytes.
#[derive(Debug)]
pub struct ArrowIpcEncoder {
    schema: SchemaRef,
}

impl ArrowIpcEncoder {
    /// Creates a new Arrow IPC encoder for the given schema.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl FormatEncoder for ArrowIpcEncoder {
    fn input_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn encode_batch(&self, batch: &RecordBatch) -> SchemaResult<Vec<Vec<u8>>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }

        let mut buf = Vec::new();
        {
            let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut buf, &batch.schema())
                .map_err(|e| SchemaError::DecodeError(format!("Arrow IPC writer init: {e}")))?;
            writer
                .write(batch)
                .map_err(|e| SchemaError::DecodeError(format!("Arrow IPC write error: {e}")))?;
            writer
                .finish()
                .map_err(|e| SchemaError::DecodeError(format!("Arrow IPC finish error: {e}")))?;
        }

        Ok(vec![buf])
    }

    fn format_name(&self) -> &str {
        "arrow_ipc"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn test_batch(schema: &SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let schema = test_schema();
        let batch = test_batch(&schema);

        // Encode
        let encoder = ArrowIpcEncoder::new(schema.clone());
        let encoded = encoder.encode_batch(&batch).unwrap();
        assert_eq!(encoded.len(), 1);

        // Decode
        let decoder = ArrowIpcDecoder::new(schema);
        let record = RawRecord::new(encoded.into_iter().next().unwrap());
        let decoded = decoder.decode_batch(&[record]).unwrap();

        assert_eq!(decoded.num_rows(), 3);
        assert_eq!(decoded.num_columns(), 2);
        assert_eq!(
            decoded
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        assert!(decoded.column(1).is_null(1));
    }

    #[test]
    fn test_encode_empty_batch() {
        let schema = test_schema();
        let batch = RecordBatch::new_empty(schema.clone());
        let encoder = ArrowIpcEncoder::new(schema);
        let encoded = encoder.encode_batch(&batch).unwrap();
        assert!(encoded.is_empty());
    }

    #[test]
    fn test_decode_empty_records() {
        let schema = test_schema();
        let decoder = ArrowIpcDecoder::new(schema.clone());
        let batch = decoder.decode_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), schema);
    }
}
