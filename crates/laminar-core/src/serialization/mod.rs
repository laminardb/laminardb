//! Shared serialization helpers.
//!
//! - Arrow IPC: `RecordBatch` ↔ bytes conversion using the Arrow IPC stream format.
//! - `jsonb_tags`: Canonical JSONB binary format type tag constants.

/// Canonical JSONB binary format type tag constants.
pub mod jsonb_tags;

use arrow::buffer::Buffer;
use arrow_array::RecordBatch;
use arrow_ipc::reader::{StreamDecoder, StreamReader};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{ArrowError, Schema};

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

/// Incremental Arrow IPC stream encoder that writes the schema **once**.
///
/// When one logical result is split across many transport chunks (e.g. a gRPC
/// server-streaming response), [`serialize_batch_stream`] re-emits the full
/// schema with *every* batch — bloating the payload and wasting CPU on a schema
/// that never changes. This encoder instead treats the whole sequence as a
/// single Arrow IPC stream: the first [`encode`](Self::encode) call prepends the
/// schema message, and every later call emits only that batch's IPC message(s).
/// Concatenating the returned blobs in call order yields one valid Arrow IPC
/// stream, decodable by a single [`BatchStreamDecoder`].
pub struct BatchStreamEncoder {
    /// Underlying stream writer; its sink is drained after each batch so each
    /// `encode` returns only the bytes written since the previous call.
    writer: StreamWriter<Vec<u8>>,
}

impl BatchStreamEncoder {
    /// Creates an encoder for `schema`. The schema message is buffered up front
    /// and flushed out with the first encoded batch.
    ///
    /// # Errors
    ///
    /// Returns [`ArrowError`] if the schema header cannot be IPC-encoded.
    pub fn new(schema: &Schema) -> Result<Self, ArrowError> {
        Ok(Self {
            writer: StreamWriter::try_new(Vec::new(), schema)?,
        })
    }

    /// Encodes one [`RecordBatch`], returning the bytes produced since the last
    /// call. The first call also carries the schema message; later calls carry
    /// only the batch (and any dictionary) message(s). The batch's schema must
    /// match the one passed to [`new`](Self::new).
    ///
    /// # Errors
    ///
    /// Returns [`ArrowError`] if IPC encoding fails.
    pub fn encode(&mut self, batch: &RecordBatch) -> Result<Vec<u8>, ArrowError> {
        self.writer.write(batch)?;
        Ok(std::mem::take(self.writer.get_mut()))
    }

    /// Finishes the stream, returning the trailing bytes (the Arrow IPC
    /// end-of-stream marker). Append these after the last [`encode`](Self::encode)
    /// output so the concatenated blobs form a complete, well-formed IPC stream.
    ///
    /// Sending the marker is also what lets a [`BatchStreamDecoder`] flush a
    /// trailing zero-row batch: the push decoder needs at least one more byte to
    /// emit a batch whose body is empty, and the marker supplies it. No more
    /// batches may be encoded after this call.
    ///
    /// # Errors
    ///
    /// Returns [`ArrowError`] if writing the end-of-stream marker fails.
    pub fn finish(&mut self) -> Result<Vec<u8>, ArrowError> {
        self.writer.finish()?;
        Ok(std::mem::take(self.writer.get_mut()))
    }
}

/// Incremental decoder for a stream produced by [`BatchStreamEncoder`].
///
/// Feed each transport chunk (in order) to [`decode_chunk`](Self::decode_chunk).
/// The decoder remembers the schema seen in the first chunk and uses it to
/// decode every later schema-less chunk. A single chunk may yield zero or more
/// batches depending on how IPC messages fall across chunk boundaries, so always
/// drain the returned `Vec`.
#[derive(Debug, Default)]
pub struct BatchStreamDecoder {
    /// Stream-wide decoder state (schema + dictionaries + any partial message).
    decoder: StreamDecoder,
}

impl BatchStreamDecoder {
    /// Creates an empty decoder that has not yet seen a schema.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Decodes every complete [`RecordBatch`] contained in one transport chunk.
    ///
    /// Bytes that only partially complete a batch straddling a chunk boundary are
    /// buffered internally until the rest arrives, so feeding chunks in order
    /// reconstructs the original batch sequence exactly.
    ///
    /// # Errors
    ///
    /// Returns [`ArrowError`] if the bytes are not a valid continuation of the
    /// stream (e.g. a batch arrives before any schema).
    pub fn decode_chunk(&mut self, bytes: Vec<u8>) -> Result<Vec<RecordBatch>, ArrowError> {
        let mut buffer = Buffer::from_vec(bytes);
        let mut batches = Vec::new();
        // `decode` consumes from the front of `buffer`, returning a batch each
        // time one completes and `None` once it needs more bytes; loop until the
        // chunk is drained so a multi-message chunk yields all of its batches.
        while !buffer.is_empty() {
            if let Some(batch) = self.decoder.decode(&mut buffer)? {
                batches.push(batch);
            }
        }
        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field};
    use std::sync::Arc;

    fn batch(values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))]).unwrap()
    }

    // The schema rides only in the first chunk: an equal-sized later batch encodes
    // to fewer bytes than the first, and to fewer bytes than a standalone
    // schema-carrying serialization of the same batch.
    #[test]
    fn stream_encoder_emits_schema_once() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let mut encoder = BatchStreamEncoder::new(&schema).unwrap();

        let first = encoder.encode(&batch(&[1, 2, 3])).unwrap();
        let second = encoder.encode(&batch(&[4, 5, 6])).unwrap();

        // Same-width batches, yet the first is larger because it also carries the
        // one-time schema message.
        assert!(first.len() > second.len());

        // A standalone (schema + batch) serialization of the equal-sized batch is
        // larger than the schema-less chunk, proving the duplicate schema is gone.
        let standalone = serialize_batch_stream(&batch(&[4, 5, 6])).unwrap();
        assert!(second.len() < standalone.len());
    }

    // Encoding a batch sequence then feeding the chunks to a single decoder
    // round-trips every batch, in order, including an empty (zero-row) batch.
    #[test]
    fn stream_encode_decode_roundtrip() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let mut encoder = BatchStreamEncoder::new(&schema).unwrap();
        // The trailing batch is empty (zero rows): it round-trips only because the
        // end-of-stream marker from `finish` lets the push decoder flush it.
        let inputs = [batch(&[1, 2]), batch(&[3, 4, 5]), batch(&[])];
        let mut chunks: Vec<Vec<u8>> = inputs.iter().map(|b| encoder.encode(b).unwrap()).collect();
        chunks.push(encoder.finish().unwrap());

        let mut decoder = BatchStreamDecoder::new();
        let mut out = Vec::new();
        for chunk in chunks {
            out.extend(decoder.decode_chunk(chunk).unwrap());
        }

        assert_eq!(out, inputs);
    }
}
