//! Arrow IPC codec for gRPC message payloads.
//!
//! Wraps the shared `crate::serialization` helpers to convert between
//! `RecordBatch` and the `bytes` fields in protobuf messages.

use arrow_array::RecordBatch;

use crate::serialization::serialize_batch_stream;

/// Encode a [`RecordBatch`] to Arrow IPC bytes for embedding in a protobuf message.
///
/// # Errors
///
/// Returns [`arrow_schema::ArrowError`] if IPC encoding fails.
pub fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, arrow_schema::ArrowError> {
    serialize_batch_stream(batch)
}

/// Decode Arrow IPC bytes from a protobuf message into a [`RecordBatch`].
///
/// Rejects payloads containing more than one batch to enforce the
/// single-batch-per-message protocol invariant.
///
/// # Errors
///
/// Returns [`arrow_schema::ArrowError`] if the bytes are invalid IPC or
/// if the payload contains more than one batch.
pub fn decode_batch(bytes: &[u8]) -> Result<RecordBatch, arrow_schema::ArrowError> {
    let cursor = std::io::Cursor::new(bytes);
    let mut reader = arrow_ipc::reader::StreamReader::try_new(cursor, None)?;
    let batch = reader.next().ok_or_else(|| {
        arrow_schema::ArrowError::IpcError("no record batch in IPC stream".to_string())
    })??;
    if reader.next().is_some() {
        return Err(arrow_schema::ArrowError::IpcError(
            "expected exactly one batch in IPC payload, found multiple".to_string(),
        ));
    }
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn round_trip() {
        let original = test_batch();
        let encoded = encode_batch(&original).unwrap();
        let decoded = decode_batch(&encoded).unwrap();
        assert_eq!(original.num_rows(), decoded.num_rows());
        assert_eq!(original.schema(), decoded.schema());
    }

    #[test]
    fn invalid_bytes() {
        assert!(decode_batch(b"not valid ipc").is_err());
    }

    #[test]
    fn reject_multi_batch_payload() {
        let batch = test_batch();
        // Write two batches into a single IPC stream.
        let mut buf = Vec::new();
        {
            let mut writer =
                arrow_ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
            writer.write(&batch).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        let err = decode_batch(&buf).unwrap_err();
        assert!(
            err.to_string().contains("multiple"),
            "expected multi-batch error, got: {err}"
        );
    }
}
