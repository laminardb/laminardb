//! Window function UDFs for `DataFusion` integration (F005B)
//!
//! Provides scalar UDFs that compute window start timestamps for
//! streaming window operations:
//!
//! - [`TumbleWindowStart`] — `tumble(timestamp, interval)` — fixed-size non-overlapping windows
//! - [`HopWindowStart`] — `hop(timestamp, slide, size)` — fixed-size overlapping windows
//! - [`SessionWindowStart`] — `session(timestamp, gap)` — pass-through for Ring 0 sessions
//!
//! These UDFs allow `DataFusion` to execute `GROUP BY TUMBLE(...)` style queries
//! by computing the window start as a per-row scalar value.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Int64Type, TimeUnit, TimestampMillisecondType};
use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, TimestampMillisecondArray};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

// ─── TumbleWindowStart ───────────────────────────────────────────────────────

/// Computes the tumbling window start for a given timestamp.
///
/// `tumble(timestamp, interval)` returns `floor(ts / interval) * interval`,
/// which is the start of the non-overlapping window that contains `ts`.
///
/// # Arguments
///
/// * Arg 0: Timestamp column or scalar (`TimestampMillisecond` or `Int64` ms)
/// * Arg 1: Window size as an interval scalar
///
/// # Returns
///
/// `TimestampMillisecond` representing the window start.
#[derive(Debug)]
pub struct TumbleWindowStart {
    signature: Signature,
}

impl TumbleWindowStart {
    /// Creates a new tumble window start UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl Default for TumbleWindowStart {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for TumbleWindowStart {
    fn eq(&self, _other: &Self) -> bool {
        true // All instances are identical
    }
}

impl Eq for TumbleWindowStart {}

impl Hash for TumbleWindowStart {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "tumble".hash(state);
    }
}

impl ScalarUDFImpl for TumbleWindowStart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "tumble"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() != 2 {
            return Err(DataFusionError::Plan(
                "tumble() requires exactly 2 arguments: (timestamp, interval)".to_string(),
            ));
        }
        let interval_ms = extract_interval_ms(&args[1])?;
        if interval_ms <= 0 {
            return Err(DataFusionError::Plan(
                "tumble() interval must be positive".to_string(),
            ));
        }
        compute_tumble(&args[0], interval_ms)
    }
}

// ─── HopWindowStart ──────────────────────────────────────────────────────────

/// Computes the earliest hopping window start for a given timestamp.
///
/// `hop(timestamp, slide, size)` returns the start of the earliest window
/// (of the given `size`, sliding by `slide`) that contains `ts`.
///
/// # Limitation
///
/// This returns only the *earliest* window start. Full multi-window
/// assignment (one row per window) is handled by Ring 0 operators.
///
/// # Arguments
///
/// * Arg 0: Timestamp column or scalar
/// * Arg 1: Slide interval scalar
/// * Arg 2: Window size interval scalar
#[derive(Debug)]
pub struct HopWindowStart {
    signature: Signature,
}

impl HopWindowStart {
    /// Creates a new hop window start UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

impl Default for HopWindowStart {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for HopWindowStart {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for HopWindowStart {}

impl Hash for HopWindowStart {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "hop".hash(state);
    }
}

impl ScalarUDFImpl for HopWindowStart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "hop"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() != 3 {
            return Err(DataFusionError::Plan(
                "hop() requires exactly 3 arguments: (timestamp, slide, size)".to_string(),
            ));
        }
        let slide_ms = extract_interval_ms(&args[1])?;
        let size_ms = extract_interval_ms(&args[2])?;
        if slide_ms <= 0 || size_ms <= 0 {
            return Err(DataFusionError::Plan(
                "hop() slide and size must be positive".to_string(),
            ));
        }
        compute_hop(&args[0], slide_ms, size_ms)
    }
}

// ─── SessionWindowStart ──────────────────────────────────────────────────────

/// Pass-through UDF for session window compatibility.
///
/// `session(timestamp, gap)` returns the input timestamp unchanged.
/// Session windows are data-dependent (gap-based grouping) and cannot
/// be computed as a per-row scalar. The actual session assignment is
/// handled by Ring 0 operators.
///
/// This UDF exists so that `GROUP BY SESSION(ts, gap)` is syntactically
/// valid in `DataFusion` queries, with real session logic deferred to
/// the streaming engine.
#[derive(Debug)]
pub struct SessionWindowStart {
    signature: Signature,
}

impl SessionWindowStart {
    /// Creates a new session window start UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl Default for SessionWindowStart {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for SessionWindowStart {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SessionWindowStart {}

impl Hash for SessionWindowStart {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "session".hash(state);
    }
}

impl ScalarUDFImpl for SessionWindowStart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "session"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() != 2 {
            return Err(DataFusionError::Plan(
                "session() requires exactly 2 arguments: (timestamp, gap)".to_string(),
            ));
        }
        // Pass-through: return the input timestamp as-is
        match &args[0] {
            ColumnarValue::Array(array) => {
                let result = convert_to_timestamp_ms_array(array)?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Scalar(scalar) => {
                let ts_ms = scalar_to_timestamp_ms(scalar)?;
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                    ts_ms, None,
                )))
            }
        }
    }
}

// ─── Helper Functions ────────────────────────────────────────────────────────

/// Extracts an interval value in milliseconds from a `ColumnarValue`.
///
/// Only scalar intervals are supported (array intervals would require
/// per-row window sizes, which is not a valid streaming pattern).
fn extract_interval_ms(value: &ColumnarValue) -> Result<i64> {
    match value {
        ColumnarValue::Scalar(scalar) => scalar_interval_to_ms(scalar),
        ColumnarValue::Array(_) => Err(DataFusionError::NotImplemented(
            "Array interval arguments not supported for window functions".to_string(),
        )),
    }
}

/// Converts a scalar interval to milliseconds.
fn scalar_interval_to_ms(scalar: &ScalarValue) -> Result<i64> {
    match scalar {
        ScalarValue::IntervalDayTime(Some(v)) => {
            Ok(i64::from(v.days) * 86_400_000 + i64::from(v.milliseconds))
        }
        ScalarValue::IntervalMonthDayNano(Some(v)) => {
            if v.months != 0 {
                return Err(DataFusionError::NotImplemented(
                    "Month-based intervals not supported for window functions \
                     (use days/hours/minutes/seconds)"
                        .to_string(),
                ));
            }
            Ok(i64::from(v.days) * 86_400_000 + v.nanoseconds / 1_000_000)
        }
        ScalarValue::IntervalYearMonth(_) => Err(DataFusionError::NotImplemented(
            "Year-month intervals not supported for window functions".to_string(),
        )),
        ScalarValue::Int64(Some(ms)) => Ok(*ms),
        _ => Err(DataFusionError::Plan(format!(
            "Expected interval argument for window function, got: {scalar:?}"
        ))),
    }
}

/// Converts a scalar value to a timestamp in milliseconds.
fn scalar_to_timestamp_ms(scalar: &ScalarValue) -> Result<Option<i64>> {
    match scalar {
        ScalarValue::TimestampMillisecond(v, _) | ScalarValue::Int64(v) => Ok(*v),
        ScalarValue::TimestampMicrosecond(v, _) => Ok(v.map(|v| v / 1_000)),
        ScalarValue::TimestampNanosecond(v, _) => Ok(v.map(|v| v / 1_000_000)),
        ScalarValue::TimestampSecond(v, _) => Ok(v.map(|v| v * 1_000)),
        _ => Err(DataFusionError::Plan(format!(
            "Expected timestamp argument for window function, got: {scalar:?}"
        ))),
    }
}

/// Computes tumble window start for a `ColumnarValue`.
fn compute_tumble(value: &ColumnarValue, interval_ms: i64) -> Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(array) => {
            let result = compute_tumble_array(array, interval_ms)?;
            Ok(ColumnarValue::Array(result))
        }
        ColumnarValue::Scalar(scalar) => {
            let ts_ms = scalar_to_timestamp_ms(scalar)?;
            let window_start = ts_ms.map(|ts| ts - ts.rem_euclid(interval_ms));
            Ok(ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                window_start,
                None,
            )))
        }
    }
}

/// Computes tumble window start for an array of timestamps.
fn compute_tumble_array(array: &ArrayRef, interval_ms: i64) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let input = array.as_primitive::<TimestampMillisecondType>();
            let result: TimestampMillisecondArray = input
                .iter()
                .map(|opt_ts| opt_ts.map(|ts| ts - ts.rem_euclid(interval_ms)))
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Int64 => {
            let input = array.as_primitive::<Int64Type>();
            let result: TimestampMillisecondArray = input
                .iter()
                .map(|opt_ts| opt_ts.map(|ts| ts - ts.rem_euclid(interval_ms)))
                .collect();
            Ok(Arc::new(result))
        }
        other => Err(DataFusionError::Plan(format!(
            "Unsupported timestamp type for tumble(): {other:?}. \
             Use TimestampMillisecond or Int64."
        ))),
    }
}

/// Computes hop (earliest) window start for a `ColumnarValue`.
fn compute_hop(value: &ColumnarValue, slide_ms: i64, size_ms: i64) -> Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(array) => {
            let result = compute_hop_array(array, slide_ms, size_ms)?;
            Ok(ColumnarValue::Array(result))
        }
        ColumnarValue::Scalar(scalar) => {
            let ts_ms = scalar_to_timestamp_ms(scalar)?;
            let window_start = ts_ms.map(|ts| hop_earliest_start(ts, slide_ms, size_ms));
            Ok(ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                window_start,
                None,
            )))
        }
    }
}

/// Computes hop window start for an array of timestamps.
fn compute_hop_array(array: &ArrayRef, slide_ms: i64, size_ms: i64) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let input = array.as_primitive::<TimestampMillisecondType>();
            let result: TimestampMillisecondArray = input
                .iter()
                .map(|opt_ts| opt_ts.map(|ts| hop_earliest_start(ts, slide_ms, size_ms)))
                .collect();
            Ok(Arc::new(result))
        }
        DataType::Int64 => {
            let input = array.as_primitive::<Int64Type>();
            let result: TimestampMillisecondArray = input
                .iter()
                .map(|opt_ts| opt_ts.map(|ts| hop_earliest_start(ts, slide_ms, size_ms)))
                .collect();
            Ok(Arc::new(result))
        }
        other => Err(DataFusionError::Plan(format!(
            "Unsupported timestamp type for hop(): {other:?}. \
             Use TimestampMillisecond or Int64."
        ))),
    }
}

/// Computes the earliest window start for a hopping window containing `ts`.
///
/// Windows of `size_ms` slide by `slide_ms`. The earliest window that
/// contains `ts` starts at `floor((ts - size + slide) / slide) * slide`.
#[inline]
fn hop_earliest_start(ts: i64, slide_ms: i64, size_ms: i64) -> i64 {
    let adjusted = ts - size_ms + slide_ms;
    adjusted - adjusted.rem_euclid(slide_ms)
}

/// Converts a timestamp array to `TimestampMillisecond` for consistent output.
fn convert_to_timestamp_ms_array(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(Arc::clone(array)),
        DataType::Int64 => {
            let input = array.as_primitive::<Int64Type>();
            let result: TimestampMillisecondArray = input.iter().collect();
            Ok(Arc::new(result))
        }
        other => Err(DataFusionError::Plan(format!(
            "Unsupported timestamp type for session(): {other:?}. \
             Use TimestampMillisecond or Int64."
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{IntervalDayTime, IntervalMonthDayNano};
    use arrow_array::Array;
    use arrow_schema::Field;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::ScalarUDF;

    fn interval_dt(days: i32, ms: i32) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(
            days, ms,
        ))))
    }

    fn ts_ms(ms: Option<i64>) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(ms, None))
    }

    fn expect_ts_ms(result: ColumnarValue) -> Option<i64> {
        match result {
            ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(v, _)) => v,
            other => panic!("Expected TimestampMillisecond scalar, got: {other:?}"),
        }
    }

    fn make_args(args: Vec<ColumnarValue>, rows: usize) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(Field::new(
                "output",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    // ── Tumble tests ─────────────────────────────────────────────────────

    #[test]
    fn test_tumble_basic() {
        let udf = TumbleWindowStart::new();
        // 5-minute interval = 300_000 ms, timestamp at 7 min
        let result = udf
            .invoke_with_args(make_args(
                vec![ts_ms(Some(420_000)), interval_dt(0, 300_000)],
                1,
            ))
            .unwrap();
        assert_eq!(expect_ts_ms(result), Some(300_000));
    }

    #[test]
    fn test_tumble_exact_boundary() {
        let udf = TumbleWindowStart::new();
        let result = udf
            .invoke_with_args(make_args(
                vec![ts_ms(Some(300_000)), interval_dt(0, 300_000)],
                1,
            ))
            .unwrap();
        assert_eq!(expect_ts_ms(result), Some(300_000));
    }

    #[test]
    fn test_tumble_zero_timestamp() {
        let udf = TumbleWindowStart::new();
        let result = udf
            .invoke_with_args(make_args(vec![ts_ms(Some(0)), interval_dt(0, 300_000)], 1))
            .unwrap();
        assert_eq!(expect_ts_ms(result), Some(0));
    }

    #[test]
    fn test_tumble_null_handling() {
        let udf = TumbleWindowStart::new();
        let result = udf
            .invoke_with_args(make_args(vec![ts_ms(None), interval_dt(0, 300_000)], 1))
            .unwrap();
        assert_eq!(expect_ts_ms(result), None);
    }

    #[test]
    fn test_tumble_array_input() {
        let udf = TumbleWindowStart::new();
        let ts_array = TimestampMillisecondArray::from(vec![
            Some(0),
            Some(150_000),
            Some(300_000),
            Some(420_000),
            None,
        ]);
        let ts = ColumnarValue::Array(Arc::new(ts_array));
        let interval = interval_dt(0, 300_000);

        let result = udf
            .invoke_with_args(make_args(vec![ts, interval], 5))
            .unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let r = arr.as_primitive::<TimestampMillisecondType>();
                assert_eq!(r.value(0), 0);
                assert_eq!(r.value(1), 0);
                assert_eq!(r.value(2), 300_000);
                assert_eq!(r.value(3), 300_000);
                assert!(r.is_null(4));
            }
            ColumnarValue::Scalar(_) => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_tumble_month_day_nano_interval() {
        let udf = TumbleWindowStart::new();
        // 1 hour = 3_600_000_000_000 nanoseconds
        let interval = ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(
            IntervalMonthDayNano::new(0, 0, 3_600_000_000_000),
        )));
        // 90 minutes = 5_400_000 ms
        let result = udf
            .invoke_with_args(make_args(vec![ts_ms(Some(5_400_000)), interval], 1))
            .unwrap();
        assert_eq!(expect_ts_ms(result), Some(3_600_000));
    }

    #[test]
    fn test_tumble_rejects_zero_interval() {
        let udf = TumbleWindowStart::new();
        let result = udf.invoke_with_args(make_args(vec![ts_ms(Some(1000)), interval_dt(0, 0)], 1));
        assert!(result.is_err());
    }

    #[test]
    fn test_tumble_rejects_wrong_arg_count() {
        let udf = TumbleWindowStart::new();
        let result = udf.invoke_with_args(make_args(vec![ts_ms(Some(1000))], 1));
        assert!(result.is_err());
    }

    // ── Hop tests ────────────────────────────────────────────────────────

    #[test]
    fn test_hop_basic() {
        let udf = HopWindowStart::new();
        // slide=5min, size=10min, ts=7min
        let result = udf
            .invoke_with_args(make_args(
                vec![
                    ts_ms(Some(420_000)),
                    interval_dt(0, 300_000),
                    interval_dt(0, 600_000),
                ],
                1,
            ))
            .unwrap();
        // Earliest 10-min window (sliding 5min) containing 420_000:
        // adjusted = 420_000 - 600_000 + 300_000 = 120_000
        // 120_000 - (120_000 % 300_000) = 120_000 - 120_000 = 0
        assert_eq!(expect_ts_ms(result), Some(0));
    }

    #[test]
    fn test_hop_at_boundary() {
        let udf = HopWindowStart::new();
        // slide=5min, size=10min, ts=exactly 5min
        let result = udf
            .invoke_with_args(make_args(
                vec![
                    ts_ms(Some(300_000)),
                    interval_dt(0, 300_000),
                    interval_dt(0, 600_000),
                ],
                1,
            ))
            .unwrap();
        // adjusted = 300_000 - 600_000 + 300_000 = 0
        // 0 - (0 % 300_000) = 0
        assert_eq!(expect_ts_ms(result), Some(0));
    }

    #[test]
    fn test_hop_rejects_wrong_arg_count() {
        let udf = HopWindowStart::new();
        let result = udf.invoke_with_args(make_args(
            vec![ts_ms(Some(1000)), interval_dt(0, 300_000)],
            1,
        ));
        assert!(result.is_err());
    }

    // ── Session tests ────────────────────────────────────────────────────

    #[test]
    fn test_session_passthrough_scalar() {
        let udf = SessionWindowStart::new();
        let result = udf
            .invoke_with_args(make_args(
                vec![ts_ms(Some(42_000)), interval_dt(0, 60_000)],
                1,
            ))
            .unwrap();
        assert_eq!(expect_ts_ms(result), Some(42_000));
    }

    #[test]
    fn test_session_passthrough_null() {
        let udf = SessionWindowStart::new();
        let result = udf
            .invoke_with_args(make_args(vec![ts_ms(None), interval_dt(0, 60_000)], 1))
            .unwrap();
        assert_eq!(expect_ts_ms(result), None);
    }

    // ── Registration & signature tests ───────────────────────────────────

    #[test]
    fn test_udf_registration() {
        let tumble = ScalarUDF::new_from_impl(TumbleWindowStart::new());
        assert_eq!(tumble.name(), "tumble");

        let hop = ScalarUDF::new_from_impl(HopWindowStart::new());
        assert_eq!(hop.name(), "hop");

        let session = ScalarUDF::new_from_impl(SessionWindowStart::new());
        assert_eq!(session.name(), "session");
    }

    #[test]
    fn test_udf_signatures_immutable() {
        assert_eq!(
            TumbleWindowStart::new().signature().volatility,
            Volatility::Immutable
        );
        assert_eq!(
            HopWindowStart::new().signature().volatility,
            Volatility::Immutable
        );
        assert_eq!(
            SessionWindowStart::new().signature().volatility,
            Volatility::Immutable
        );
    }

    #[test]
    fn test_tumble_return_type() {
        let udf = TumbleWindowStart::new();
        let rt = udf.return_type(&[]).unwrap();
        assert_eq!(rt, DataType::Timestamp(TimeUnit::Millisecond, None));
    }
}
