//! Processing-time UDF for `DataFusion` integration
//!
//! Provides a `proctime()` scalar function that returns the current
//! wall-clock timestamp. Used to declare processing-time watermarks:
//!
//! ```sql
//! CREATE SOURCE events (
//!     data VARCHAR,
//!     ts TIMESTAMP,
//!     WATERMARK FOR ts AS PROCTIME()
//! );
//! ```

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

/// Scalar UDF that returns the current processing time.
///
/// `proctime()` takes no arguments and returns a `TimestampMillisecond`
/// representing the current wall-clock time. Unlike `watermark()`, this
/// function is volatile — it returns a different value on each invocation.
#[derive(Debug)]
pub struct ProcTimeUdf {
    signature: Signature,
}

impl ProcTimeUdf {
    /// Creates a new processing-time UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Nullary, Volatility::Volatile),
        }
    }
}

impl Default for ProcTimeUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for ProcTimeUdf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for ProcTimeUdf {}

impl Hash for ProcTimeUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "proctime".hash(state);
    }
}

impl ScalarUDFImpl for ProcTimeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "proctime"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    #[allow(clippy::cast_possible_truncation)]
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        Ok(ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
            Some(now_ms),
            None,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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
    fn test_proctime_returns_timestamp() {
        let udf = ProcTimeUdf::new();
        let result = udf.invoke_with_args(make_args()).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(Some(v), _)) => {
                // Should be a reasonable timestamp (after 2020-01-01)
                assert!(v > 1_577_836_800_000, "timestamp too old: {v}");
            }
            other => panic!("Expected TimestampMillisecond, got: {other:?}"),
        }
    }

    #[test]
    fn test_proctime_registration() {
        let udf = ScalarUDF::new_from_impl(ProcTimeUdf::new());
        assert_eq!(udf.name(), "proctime");
    }

    #[test]
    fn test_proctime_volatile() {
        let udf = ProcTimeUdf::new();
        assert_eq!(udf.signature().volatility, Volatility::Volatile);
    }

    #[test]
    fn test_proctime_return_type() {
        let udf = ProcTimeUdf::new();
        let rt = udf.return_type(&[]).unwrap();
        assert_eq!(rt, DataType::Timestamp(TimeUnit::Millisecond, None));
    }
}
