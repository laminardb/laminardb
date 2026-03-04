//! Shared Arrow IPC serialization helpers.
//!
//! Centralizes `RecordBatch` ↔ bytes conversion using the Arrow IPC stream format,
//! eliminating copy-pasted implementations across join operators and backends.

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;

/// Serializes a single [`RecordBatch`] to Arrow IPC stream bytes.
///
/// # Errors
///
/// Returns [`arrow_schema::ArrowError`] if IPC encoding fails.
pub fn serialize_batch_stream(batch: &RecordBatch) -> Result<Vec<u8>, arrow_schema::ArrowError> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Deserializes a single [`RecordBatch`] from Arrow IPC stream bytes.
///
/// # Errors
///
/// Returns [`arrow_schema::ArrowError`] if the bytes are invalid or contain no batches.
pub fn deserialize_batch_stream(bytes: &[u8]) -> Result<RecordBatch, arrow_schema::ArrowError> {
    let cursor = std::io::Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None)?;
    reader.next().ok_or_else(|| {
        arrow_schema::ArrowError::IpcError("no record batch in IPC stream".to_string())
    })?
}
