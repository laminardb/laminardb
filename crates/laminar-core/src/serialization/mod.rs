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

/// A growable byte writer that rejects payloads and retained capacities above a fixed limit.
/// This is shared by Arrow IPC and archive encoders at checkpoint/shuffle boundaries.
pub struct BoundedBytesWriter {
    bytes: Vec<u8>,
    limit: usize,
}

impl BoundedBytesWriter {
    /// Create an empty bounded writer.
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self::with_capacity(limit, 0)
    }

    fn with_capacity(limit: usize, initial_capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(initial_capacity.min(limit)),
            limit,
        }
    }

    /// Consume the writer and return its retained bytes without copying.
    #[must_use]
    pub fn into_vec(self) -> Vec<u8> {
        self.bytes
    }
}

impl std::io::Write for BoundedBytesWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let next_len = self
            .bytes
            .len()
            .checked_add(buf.len())
            .filter(|next_len| *next_len <= self.limit)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    "serialized payload exceeds its configured bound",
                )
            })?;
        if next_len > self.bytes.capacity() {
            self.bytes
                .try_reserve_exact(next_len - self.bytes.len())
                .map_err(std::io::Error::other)?;
            if self.bytes.capacity() < next_len || self.bytes.capacity() > self.limit {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    "serialized allocation exceeds its configured bound",
                ));
            }
        }
        self.bytes.extend_from_slice(buf);
        debug_assert!(self.bytes.capacity() <= self.limit);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

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

/// Test encoder for exercising incrementally chunked Arrow IPC streams.
#[cfg(test)]
pub(crate) struct BatchStreamEncoder {
    writer: StreamWriter<Vec<u8>>,
}

#[cfg(test)]
impl BatchStreamEncoder {
    /// Encoder for `schema`; the schema message flushes out with the first batch.
    ///
    /// # Errors
    /// [`ArrowError`] if the schema header can't be IPC-encoded.
    pub(crate) fn new(schema: &Schema) -> Result<Self, ArrowError> {
        Ok(Self {
            writer: StreamWriter::try_new(Vec::new(), schema)?,
        })
    }

    /// Encode one batch, returning the bytes written since the last call (the
    /// first call also carries the schema).
    ///
    /// # Errors
    /// [`ArrowError`] if IPC encoding fails.
    pub(crate) fn encode(&mut self, batch: &RecordBatch) -> Result<Vec<u8>, ArrowError> {
        self.writer.write(batch)?;
        Ok(std::mem::take(self.writer.get_mut()))
    }

    /// Finish the stream, returning the end-of-stream marker to append after the
    /// last [`encode`](Self::encode). Also lets the decoder flush a trailing
    /// zero-row batch; no batches may be encoded after this.
    ///
    /// # Errors
    /// [`ArrowError`] if writing the marker fails.
    pub(crate) fn finish(&mut self) -> Result<Vec<u8>, ArrowError> {
        self.writer.finish()?;
        Ok(std::mem::take(self.writer.get_mut()))
    }
}

/// Decoder for an incrementally chunked Arrow IPC stream. The first chunk's
/// schema decodes all later schema-less chunks.
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

    /// Verify that the last chunk ended between IPC messages rather than in a
    /// buffered header, metadata block, or body. This does not finish or reset
    /// the decoder.
    #[cfg(feature = "cluster")]
    pub(crate) fn ensure_message_boundary(&mut self) -> Result<(), ArrowError> {
        self.decoder.finish()
    }
}

/// Serializes batches as one Arrow IPC stream without allowing the writer to allocate beyond
/// `max_bytes`.
///
/// # Errors
///
/// Returns [`arrow_schema::ArrowError`] if encoding fails or crosses the byte bound.
pub fn serialize_batches_stream_bounded<'a, I>(
    schema: &Schema,
    batches: I,
    max_bytes: usize,
) -> Result<Vec<u8>, arrow_schema::ArrowError>
where
    I: IntoIterator<Item = &'a RecordBatch>,
{
    serialize_batches_stream_bounded_with_capacity(schema, batches, max_bytes, 0)
}

fn serialize_batches_stream_bounded_with_capacity<'a, I>(
    schema: &Schema,
    batches: I,
    max_bytes: usize,
    initial_capacity: usize,
) -> Result<Vec<u8>, arrow_schema::ArrowError>
where
    I: IntoIterator<Item = &'a RecordBatch>,
{
    let mut bounded = BoundedBytesWriter::with_capacity(max_bytes, initial_capacity);
    {
        let mut writer = StreamWriter::try_new(&mut bounded, schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    Ok(bounded.bytes)
}

#[cfg(feature = "cluster")]
/// Single-batch adapter that preserves the shuffle path's measured initial-capacity hint.
pub(crate) fn serialize_batch_stream_bounded(
    batch: &RecordBatch,
    max_bytes: usize,
    initial_capacity: usize,
) -> Result<Vec<u8>, arrow_schema::ArrowError> {
    serialize_batches_stream_bounded_with_capacity(
        batch.schema().as_ref(),
        std::iter::once(batch),
        max_bytes,
        initial_capacity,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field};
    use std::io::Write as _;
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

    #[test]
    fn bounded_writer_never_grows_past_its_limit() {
        let mut writer = BoundedBytesWriter::with_capacity(8, 4);
        writer.write_all(&[1, 2, 3]).unwrap();
        writer.write_all(&[4, 5, 6, 7, 8]).unwrap();
        assert_eq!(writer.bytes, vec![1, 2, 3, 4, 5, 6, 7, 8]);
        assert!(writer.bytes.capacity() <= 8);

        let error = writer.write_all(&[9]).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::OutOfMemory);
        assert_eq!(writer.bytes.len(), 8);
        assert!(writer.bytes.capacity() <= 8);
    }

    #[test]
    fn bounded_batch_stream_round_trips_multiple_batches_and_fails_at_the_bound() {
        let inputs = [batch(&[1, 2]), batch(&[3, 4, 5])];
        let schema = inputs[0].schema();
        let encoded =
            serialize_batches_stream_bounded(schema.as_ref(), inputs.iter(), usize::MAX).unwrap();
        let decoded = StreamReader::try_new(std::io::Cursor::new(&encoded), None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(decoded, inputs);

        let error =
            serialize_batches_stream_bounded(schema.as_ref(), inputs.iter(), encoded.len() - 1)
                .unwrap_err();
        assert!(error.to_string().contains("configured bound"));

        let tiny_error =
            serialize_batches_stream_bounded(schema.as_ref(), inputs.iter(), 1).unwrap_err();
        assert!(tiny_error.to_string().contains("configured bound"));
    }
}
