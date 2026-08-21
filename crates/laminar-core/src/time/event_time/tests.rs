use super::*;
use arrow::array::{
    ArrayRef, Int64Builder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    TimestampNanosecondBuilder, TimestampSecondBuilder,
};
use arrow::datatypes::{Field, TimeUnit};
use std::sync::Arc;

fn make_ms_batch(values: &[Option<i64>]) -> RecordBatch {
    let mut b = TimestampMillisecondBuilder::new();
    for v in values {
        match v {
            Some(val) => b.append_value(*val),
            None => b.append_null(),
        }
    }
    let array: ArrayRef = Arc::new(b.finish());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        true,
    )]));
    RecordBatch::try_new(schema, vec![array]).unwrap()
}

fn make_ns_batch(values: &[Option<i64>]) -> RecordBatch {
    let mut b = TimestampNanosecondBuilder::new();
    for v in values {
        match v {
            Some(val) => b.append_value(*val),
            None => b.append_null(),
        }
    }
    let array: ArrayRef = Arc::new(b.finish());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    )]));
    RecordBatch::try_new(schema, vec![array]).unwrap()
}

fn make_us_batch(values: &[Option<i64>]) -> RecordBatch {
    let mut b = TimestampMicrosecondBuilder::new();
    for v in values {
        match v {
            Some(val) => b.append_value(*val),
            None => b.append_null(),
        }
    }
    let array: ArrayRef = Arc::new(b.finish());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    )]));
    RecordBatch::try_new(schema, vec![array]).unwrap()
}

fn make_s_batch(values: &[Option<i64>]) -> RecordBatch {
    let mut b = TimestampSecondBuilder::new();
    for v in values {
        match v {
            Some(val) => b.append_value(*val),
            None => b.append_null(),
        }
    }
    let array: ArrayRef = Arc::new(b.finish());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Second, None),
        true,
    )]));
    RecordBatch::try_new(schema, vec![array]).unwrap()
}

#[test]
fn test_extract_millis() {
    let batch = make_ms_batch(&[Some(1_705_312_200_000)]);
    let mut extractor = EventTimeExtractor::from_column("ts");
    assert_eq!(extractor.extract(&batch).unwrap(), 1_705_312_200_000);
}

#[test]
fn test_extract_nanos_is_rescaled_to_millis() {
    let batch = make_ns_batch(&[Some(1_705_312_200_000_000_000)]);
    let mut extractor = EventTimeExtractor::from_column("ts");
    assert_eq!(extractor.extract(&batch).unwrap(), 1_705_312_200_000);
}

#[test]
fn test_extract_micros_is_rescaled_to_millis() {
    let batch = make_us_batch(&[Some(1_705_312_200_000_000)]);
    let mut extractor = EventTimeExtractor::from_column("ts");
    assert_eq!(extractor.extract(&batch).unwrap(), 1_705_312_200_000);
}

#[test]
fn test_extract_millis_array_rescales_micros() {
    let batch = make_us_batch(&[Some(1_500), None, Some(2_500)]);
    let mut extractor = EventTimeExtractor::from_column("ts");
    let milliseconds = extractor.extract_millis_array(&batch).unwrap();

    assert_eq!(
        milliseconds.iter().collect::<Vec<_>>(),
        vec![Some(1), None, Some(2)]
    );
}

#[test]
fn test_extract_seconds_is_rescaled_to_millis() {
    let batch = make_s_batch(&[Some(1_705_312_200)]);
    let mut extractor = EventTimeExtractor::from_column("ts");
    assert_eq!(extractor.extract(&batch).unwrap(), 1_705_312_200_000);
}

#[test]
fn test_mode_first() {
    let batch = make_ms_batch(&[Some(100), Some(200), Some(150)]);
    let mut extractor = EventTimeExtractor::from_column("ts").with_mode(ExtractionMode::First);
    assert_eq!(extractor.extract(&batch).unwrap(), 100);
}

#[test]
fn test_mode_last() {
    let batch = make_ms_batch(&[Some(100), Some(200), Some(150)]);
    let mut extractor = EventTimeExtractor::from_column("ts").with_mode(ExtractionMode::Last);
    assert_eq!(extractor.extract(&batch).unwrap(), 150);
}

#[test]
fn test_mode_max() {
    let batch = make_ms_batch(&[Some(100), Some(200), Some(150)]);
    let mut extractor = EventTimeExtractor::from_column("ts").with_mode(ExtractionMode::Max);
    assert_eq!(extractor.extract(&batch).unwrap(), 200);
}

#[test]
fn test_mode_min() {
    let batch = make_ms_batch(&[Some(100), Some(200), Some(150)]);
    let mut extractor = EventTimeExtractor::from_column("ts").with_mode(ExtractionMode::Min);
    assert_eq!(extractor.extract(&batch).unwrap(), 100);
}

#[test]
fn test_max_skips_nulls() {
    let batch = make_ms_batch(&[Some(100), None, Some(200), Some(150)]);
    let mut extractor = EventTimeExtractor::from_column("ts").with_mode(ExtractionMode::Max);
    assert_eq!(extractor.extract(&batch).unwrap(), 200);
}

#[test]
fn test_column_not_found() {
    let batch = make_ms_batch(&[Some(100)]);
    let mut extractor = EventTimeExtractor::from_column("missing");
    assert!(matches!(
        extractor.extract(&batch),
        Err(EventTimeError::ColumnNotFound(_))
    ));
}

#[test]
fn test_non_timestamp_column_is_rejected() {
    let mut b = Int64Builder::new();
    b.append_value(100);
    let array: ArrayRef = Arc::new(b.finish());
    let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

    let mut extractor = EventTimeExtractor::from_column("ts");
    assert!(matches!(
        extractor.extract(&batch),
        Err(EventTimeError::IncompatibleType { .. })
    ));
}

#[test]
fn test_empty_batch() {
    let batch = make_ms_batch(&[]);
    let mut extractor = EventTimeExtractor::from_column("ts");
    assert!(matches!(
        extractor.extract(&batch),
        Err(EventTimeError::EmptyBatch)
    ));
}

#[test]
fn test_null_first_row() {
    let batch = make_ms_batch(&[None, Some(100)]);
    let mut extractor = EventTimeExtractor::from_column("ts").with_mode(ExtractionMode::First);
    assert!(matches!(
        extractor.extract(&batch),
        Err(EventTimeError::NullTimestamp { row: 0 })
    ));
}

#[test]
fn test_column_index_caching() {
    let batch = make_ms_batch(&[Some(100)]);
    let mut extractor = EventTimeExtractor::from_column("ts");

    assert!(extractor.cached_index.is_none());
    let _ = extractor.extract(&batch).unwrap();
    assert_eq!(extractor.cached_index, Some(0));
    assert_eq!(extractor.extract(&batch).unwrap(), 100);
}

#[test]
fn test_from_index_skips_name_lookup() {
    let batch = make_ms_batch(&[Some(100)]);
    let mut extractor = EventTimeExtractor::from_index(0);
    assert_eq!(extractor.cached_index, Some(0));
    assert_eq!(extractor.extract(&batch).unwrap(), 100);
}

#[test]
fn test_validate_schema_ok() {
    let schema = Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        true,
    )]);
    let extractor = EventTimeExtractor::from_column("ts");
    assert!(extractor.validate_schema(&schema).is_ok());
}

#[test]
fn test_validate_schema_rejects_non_timestamp() {
    let schema = Schema::new(vec![Field::new("ts", DataType::Int64, true)]);
    let extractor = EventTimeExtractor::from_column("ts");
    assert!(matches!(
        extractor.validate_schema(&schema),
        Err(EventTimeError::IncompatibleType { .. })
    ));
}
