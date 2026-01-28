//! Watermark UDF for `DataFusion` integration (F005B)
//!
//! Provides a `watermark()` scalar function that returns the current
//! watermark timestamp from Ring 0 via a shared `Arc<AtomicI64>`.
//!
//! The watermark represents the boundary below which all events are
//! assumed to have arrived. Queries can use `WHERE event_time > watermark()`
//! to filter stale data.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

/// No-watermark sentinel value. When the atomic holds this value,
/// `watermark()` returns `NULL`.
pub const NO_WATERMARK: i64 = -1;

/// Scalar UDF that returns the current watermark timestamp.
///
/// The watermark is stored in a shared `Arc<AtomicI64>` that Ring 0
/// updates as events are processed. This UDF reads it with relaxed
/// ordering (appropriate since watermarks are monotonically advancing
/// and a slightly stale read is acceptable).
///
/// Returns `NULL` when no watermark has been set (value < 0).
#[derive(Debug)]
pub struct WatermarkUdf {
    signature: Signature,
    watermark_ms: Arc<AtomicI64>,
}

impl WatermarkUdf {
    /// Creates a new watermark UDF backed by the given atomic value.
    ///
    /// # Arguments
    ///
    /// * `watermark_ms` - Shared atomic holding the current watermark
    ///   in milliseconds since epoch. Values < 0 mean "no watermark".
    pub fn new(watermark_ms: Arc<AtomicI64>) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Nullary, Volatility::Volatile),
            watermark_ms,
        }
    }

    /// Creates a watermark UDF that always returns `NULL`.
    #[must_use]
    pub fn unset() -> Self {
        Self::new(Arc::new(AtomicI64::new(NO_WATERMARK)))
    }

    /// Returns a reference to the underlying atomic watermark value.
    #[must_use]
    pub fn watermark_ref(&self) -> &Arc<AtomicI64> {
        &self.watermark_ms
    }
}

impl PartialEq for WatermarkUdf {
    fn eq(&self, _other: &Self) -> bool {
        true // All watermark UDF instances serve the same purpose
    }
}

impl Eq for WatermarkUdf {}

impl Hash for WatermarkUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "watermark".hash(state);
    }
}

impl ScalarUDFImpl for WatermarkUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "watermark"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let wm = self.watermark_ms.load(Ordering::Relaxed);
        if wm < 0 {
            Ok(ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                None, None,
            )))
        } else {
            Ok(ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                Some(wm),
                None,
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit;
    use arrow_schema::Field;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::ScalarUDF;

    fn make_args() -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new(
                "output",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    #[test]
    fn test_watermark_default_null() {
        let udf = WatermarkUdf::unset();
        let result = udf.invoke_with_args(make_args()).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(None, _)) => {}
            other => panic!("Expected NULL when no watermark set, got: {other:?}"),
        }
    }

    #[test]
    fn test_watermark_fixed_value() {
        let wm = Arc::new(AtomicI64::new(1_000_000));
        let udf = WatermarkUdf::new(Arc::clone(&wm));

        let result = udf.invoke_with_args(make_args()).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(Some(v), _)) => {
                assert_eq!(v, 1_000_000);
            }
            other => panic!("Expected TimestampMillisecond(1_000_000), got: {other:?}"),
        }
    }

    #[test]
    fn test_watermark_atomic_update() {
        let wm = Arc::new(AtomicI64::new(NO_WATERMARK));
        let udf = WatermarkUdf::new(Arc::clone(&wm));

        // Initially NULL
        let result = udf.invoke_with_args(make_args()).unwrap();
        assert!(matches!(
            result,
            ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(None, _))
        ));

        // Update watermark
        wm.store(500_000, Ordering::Relaxed);
        let result = udf.invoke_with_args(make_args()).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(Some(v), _)) => {
                assert_eq!(v, 500_000);
            }
            other => panic!("Expected updated watermark, got: {other:?}"),
        }
    }

    #[test]
    fn test_watermark_registration() {
        let udf = ScalarUDF::new_from_impl(WatermarkUdf::unset());
        assert_eq!(udf.name(), "watermark");
    }
}
