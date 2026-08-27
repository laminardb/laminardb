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
fn bounded_writer_grows_geometrically_without_changing_bytes() {
    const LIMIT: usize = 4096;
    let expected: Vec<u8> = (0..LIMIT).map(|value| (value % 251) as u8).collect();
    let mut writer = BoundedBytesWriter::new(LIMIT);
    let mut capacity = writer.bytes.capacity();
    let mut capacity_changes = 0;

    for byte in &expected {
        writer.write_all(std::slice::from_ref(byte)).unwrap();
        if writer.bytes.capacity() != capacity {
            capacity = writer.bytes.capacity();
            capacity_changes += 1;
        }
        assert!(capacity <= LIMIT);
    }

    assert_eq!(writer.bytes, expected);
    assert!(
        capacity_changes <= 16,
        "capacity changed {capacity_changes} times"
    );
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

    let error = serialize_batches_stream_bounded(schema.as_ref(), inputs.iter(), encoded.len() - 1)
        .unwrap_err();
    assert!(error.to_string().contains("configured bound"));

    let tiny_error =
        serialize_batches_stream_bounded(schema.as_ref(), inputs.iter(), 1).unwrap_err();
    assert!(tiny_error.to_string().contains("configured bound"));
}

#[test]
fn bounded_lz4_stream_is_deterministic_compact_and_round_trips() {
    let input = batch(&vec![7; 16_384]);
    let schema = input.schema();
    let plain = serialize_batches_stream_bounded(schema.as_ref(), [&input], usize::MAX).unwrap();
    let compressed =
        serialize_batches_stream_lz4_bounded(schema.as_ref(), [&input], usize::MAX).unwrap();
    let repeated =
        serialize_batches_stream_lz4_bounded(schema.as_ref(), [&input], usize::MAX).unwrap();

    assert_eq!(compressed, repeated);
    assert!(compressed.len() < plain.len() / 4);
    let decoded = StreamReader::try_new(std::io::Cursor::new(&compressed), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(decoded, [input]);

    let error =
        serialize_batches_stream_lz4_bounded(schema.as_ref(), decoded.iter(), compressed.len() - 1)
            .unwrap_err();
    assert!(error.to_string().contains("configured bound"));
}
