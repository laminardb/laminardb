use super::*;
use arrow::array::{
    Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};

#[test]
fn passthrough_when_already_millis() {
    let arr = TimestampMillisecondArray::from(vec![1, 2, 3]);
    let out = cast_to_millis_array(&arr).unwrap();
    assert_eq!(out.values(), &[1, 2, 3]);
}

#[test]
fn rescales_nanos() {
    let arr = TimestampNanosecondArray::from(vec![1_500_000, 2_500_000]);
    let out = cast_to_millis_array(&arr).unwrap();
    assert_eq!(out.values(), &[1, 2]);
}

#[test]
fn rescales_micros() {
    let arr = TimestampMicrosecondArray::from(vec![1_500, 2_500]);
    let out = cast_to_millis_array(&arr).unwrap();
    assert_eq!(out.values(), &[1, 2]);
}

#[test]
fn rescales_seconds() {
    let arr = TimestampSecondArray::from(vec![1, 2]);
    let out = cast_to_millis_array(&arr).unwrap();
    assert_eq!(out.values(), &[1_000, 2_000]);
}

#[test]
fn non_timestamp_errors() {
    let arr = Int64Array::from(vec![1, 2]);
    assert!(cast_to_millis_array(&arr).is_err());
}
