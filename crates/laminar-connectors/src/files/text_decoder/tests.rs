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
