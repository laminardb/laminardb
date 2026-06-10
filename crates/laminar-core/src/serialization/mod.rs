//! Shared serialization helpers.
//!
//! - Arrow IPC: `RecordBatch` ↔ bytes conversion using the Arrow IPC stream format.
//! - `jsonb_tags`: Canonical JSONB binary format type tag constants.

/// Canonical JSONB binary format type tag constants.
pub mod jsonb_tags;

use std::sync::Arc;

use arrow::buffer::Buffer;
use arrow_array::RecordBatch;
use arrow_ipc::reader::{StreamDecoder, StreamReader};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{ArrowError, Schema, SchemaRef};

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

/// Incremental Arrow IPC encoder that writes the schema once: concatenating the
/// per-call blobs in order yields one IPC stream for [`BatchStreamDecoder`].
pub struct BatchStreamEncoder {
    writer: StreamWriter<Vec<u8>>,
    schema: SchemaRef,
}

impl BatchStreamEncoder {
    /// Encoder for `schema`; the schema message flushes out with the first batch.
    ///
    /// # Errors
    /// [`ArrowError`] if the schema header can't be IPC-encoded.
    pub fn new(schema: &Schema) -> Result<Self, ArrowError> {
        Ok(Self {
            writer: StreamWriter::try_new(Vec::new(), schema)?,
            schema: Arc::new(schema.clone()),
        })
    }

    /// Schema this encoder was created with; every encoded batch must match it.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Encode one batch, returning the bytes written since the last call (the
    /// first call also carries the schema).
    ///
    /// # Errors
    /// [`ArrowError`] if IPC encoding fails.
    pub fn encode(&mut self, batch: &RecordBatch) -> Result<Vec<u8>, ArrowError> {
        self.writer.write(batch)?;
        Ok(std::mem::take(self.writer.get_mut()))
    }

    /// Finish the stream, returning the end-of-stream marker to append after the
    /// last [`encode`](Self::encode). Also lets the decoder flush a trailing
    /// zero-row batch; no batches may be encoded after this.
    ///
    /// # Errors
    /// [`ArrowError`] if writing the marker fails.
    pub fn finish(&mut self) -> Result<Vec<u8>, ArrowError> {
        self.writer.finish()?;
        Ok(std::mem::take(self.writer.get_mut()))
    }
}

/// Decoder for a stream produced by [`BatchStreamEncoder`]: feed each chunk in
/// order; the first chunk's schema decodes all later schema-less chunks.
#[derive(Debug, Default)]
pub struct BatchStreamDecoder {
    decoder: StreamDecoder,
}

impl BatchStreamDecoder {
    /// Creates an empty decoder that has not yet seen a schema.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Decode every complete batch in one chunk; a batch straddling a chunk
    /// boundary is buffered until the rest arrives, preserving order.
    ///
    /// # Errors
    /// [`ArrowError`] if the bytes aren't a valid continuation (e.g. a batch
    /// before any schema).
    pub fn decode_chunk(&mut self, bytes: Vec<u8>) -> Result<Vec<RecordBatch>, ArrowError> {
        let mut buffer = Buffer::from_vec(bytes);
        let mut batches = Vec::new();
        // Drain the chunk: `decode` yields a batch each time one completes.
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
