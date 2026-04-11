//! Shared helper for casting a `Timestamp(_)` array to
//! `TimestampMillisecondArray`.
//!
//! Used by the window UDFs, the event-time extractor, the interval-join
//! key helper, and the WebSocket parser — anywhere we need a uniform
//! millisecond representation regardless of the column's declared
//! precision.

use std::fmt;

use arrow::array::{Array, TimestampMillisecondArray};
use arrow::datatypes::{DataType, TimeUnit};

/// Error returned when a column isn't a `Timestamp(_)` type or Arrow's
/// cast kernel fails.
#[derive(Debug)]
pub struct CastError(pub String);

impl fmt::Display for CastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for CastError {}

/// Cast any `Timestamp(_)` array to `TimestampMillisecond`. The
/// already-millis case is zero-copy; other precisions go through
/// `arrow::compute::cast`.
///
/// # Errors
///
/// Returns [`CastError`] if the column isn't `Timestamp(_)` or if
/// Arrow's cast kernel fails.
pub fn cast_to_millis_array(array: &dyn Array) -> Result<TimestampMillisecondArray, CastError> {
    if !matches!(array.data_type(), DataType::Timestamp(_, _)) {
        return Err(CastError(format!(
            "event-time column must be Timestamp(_), found {:?}",
            array.data_type()
        )));
    }
    let cast = arrow::compute::cast(array, &DataType::Timestamp(TimeUnit::Millisecond, None))
        .map_err(|e| CastError(e.to_string()))?;
    cast.as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .cloned()
        .ok_or_else(|| CastError("arrow cast did not yield TimestampMillisecond".into()))
}

#[cfg(test)]
mod tests {
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
}
