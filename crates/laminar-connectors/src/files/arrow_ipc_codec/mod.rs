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

    fn format_name(&self) -> &'static str {
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

    fn format_name(&self) -> &'static str {
        "arrow_ipc"
    }
}
#[cfg(test)]
mod tests;
