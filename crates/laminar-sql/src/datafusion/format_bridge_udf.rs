//! Format bridge scalar UDFs (F-SCHEMA-014).
//!
//! SQL-callable functions for inline format conversion within queries:
//!
//! - [`ParseEpochUdf`] — `parse_epoch(number, unit) -> timestamp`
//! - [`ParseTimestampUdf`] — `parse_timestamp(string, format, timezone) -> timestamp`
//! - [`ToJsonUdf`] — `to_json(value) -> text`
//! - [`FromJsonUdf`] — `from_json(string) -> jsonb`

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use arrow_array::{
    builder::{LargeBinaryBuilder, StringBuilder, TimestampMicrosecondBuilder},
    Array, ArrayRef, LargeBinaryArray, StringArray, TimestampMicrosecondArray,
};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::json_types;

// ── Helpers ──────────────────────────────────────────────────────

/// Expand all args to arrays of the same length.
fn expand_args(args: &[ColumnarValue]) -> Result<Vec<ArrayRef>> {
    let len = args
        .iter()
        .find_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            ColumnarValue::Scalar(_) => None,
        })
        .unwrap_or(1);

    args.iter()
        .map(|a| match a {
            ColumnarValue::Array(arr) => Ok(Arc::clone(arr)),
            ColumnarValue::Scalar(s) => s.to_array_of_size(len),
        })
        .collect()
}

// ══════════════════════════════════════════════════════════════════
// parse_epoch(number, unit) -> timestamp
// ══════════════════════════════════════════════════════════════════

/// `parse_epoch(number, unit) -> timestamp`
///
/// Converts an epoch number to a timestamp. The unit parameter specifies
/// the time unit: 'seconds', 'milliseconds', 'microseconds', 'nanoseconds'.
///
/// Returns `TimestampMicrosecond` (microsecond precision, no timezone).
/// Executes in Ring 0 — pure arithmetic, no allocation.
///
/// # Examples
///
/// ```sql
/// SELECT parse_epoch(1708528800, 'seconds') AS ts;
/// SELECT parse_epoch(1708528800000, 'milliseconds') AS ts;
/// ```
#[derive(Debug)]
pub struct ParseEpochUdf {
    signature: Signature,
}

impl ParseEpochUdf {
    /// Creates a new `parse_epoch` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Int64, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for ParseEpochUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for ParseEpochUdf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for ParseEpochUdf {}

impl Hash for ParseEpochUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "parse_epoch".hash(state);
    }
}

impl ScalarUDFImpl for ParseEpochUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "parse_epoch"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let val_arr = expanded[0]
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "parse_epoch: first arg must be Int64".into(),
                )
            })?;
        let unit_arr = expanded[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "parse_epoch: second arg must be Utf8".into(),
                )
            })?;

        let mut builder = TimestampMicrosecondBuilder::with_capacity(val_arr.len());
        for i in 0..val_arr.len() {
            if val_arr.is_null(i) || unit_arr.is_null(i) {
                builder.append_null();
            } else {
                let value = val_arr.value(i);
                let unit = unit_arr.value(i);
                let micros = epoch_to_micros(value, unit)?;
                builder.append_value(micros);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Convert an epoch value with the given unit string to microseconds.
fn epoch_to_micros(value: i64, unit: &str) -> Result<i64> {
    match unit.to_ascii_lowercase().as_str() {
        "seconds" | "s" => Ok(value.saturating_mul(1_000_000)),
        "milliseconds" | "ms" => Ok(value.saturating_mul(1_000)),
        "microseconds" | "us" => Ok(value),
        "nanoseconds" | "ns" => Ok(value / 1_000),
        _ => Err(datafusion_common::DataFusionError::Execution(format!(
            "parse_epoch: invalid unit '{unit}'. \
             Expected: seconds, milliseconds, microseconds, nanoseconds"
        ))),
    }
}

// ══════════════════════════════════════════════════════════════════
// parse_timestamp(string, format, timezone) -> timestamp
// ══════════════════════════════════════════════════════════════════

/// `parse_timestamp(string, format) -> timestamp`
///
/// Parses a timestamp string using the given chrono format string.
/// Use `'iso8601'` as a shortcut for ISO 8601 parsing.
///
/// Returns `TimestampMicrosecond`.
///
/// # Examples
///
/// ```sql
/// SELECT parse_timestamp('2026-02-21 14:30:00', '%Y-%m-%d %H:%M:%S');
/// SELECT parse_timestamp('2026-02-21T14:30:00Z', 'iso8601');
/// ```
#[derive(Debug)]
pub struct ParseTimestampUdf {
    signature: Signature,
}

impl ParseTimestampUdf {
    /// Creates a new `parse_timestamp` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for ParseTimestampUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for ParseTimestampUdf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for ParseTimestampUdf {}

impl Hash for ParseTimestampUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "parse_timestamp".hash(state);
    }
}

impl ScalarUDFImpl for ParseTimestampUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "parse_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let str_arr = expanded[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "parse_timestamp: first arg must be Utf8".into(),
                )
            })?;
        let fmt_arr = expanded[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "parse_timestamp: second arg must be Utf8".into(),
                )
            })?;

        let mut builder = TimestampMicrosecondBuilder::with_capacity(str_arr.len());
        for i in 0..str_arr.len() {
            if str_arr.is_null(i) || fmt_arr.is_null(i) {
                builder.append_null();
            } else {
                let ts_str = str_arr.value(i);
                let fmt = fmt_arr.value(i);
                match parse_ts_string(ts_str, fmt) {
                    Ok(micros) => builder.append_value(micros),
                    Err(_) => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Parse a timestamp string with the given format to microseconds since epoch.
fn parse_ts_string(ts_str: &str, fmt: &str) -> std::result::Result<i64, String> {
    use chrono::NaiveDateTime;

    if fmt.eq_ignore_ascii_case("iso8601") {
        // Try RFC 3339 / ISO 8601
        let dt = ts_str
            .parse::<chrono::DateTime<chrono::Utc>>()
            .or_else(|_| {
                NaiveDateTime::parse_from_str(ts_str, "%Y-%m-%dT%H:%M:%S%.f")
                    .map(|ndt| ndt.and_utc())
            })
            .or_else(|_| {
                NaiveDateTime::parse_from_str(ts_str, "%Y-%m-%dT%H:%M:%S").map(|ndt| ndt.and_utc())
            })
            .map_err(|e| e.to_string())?;
        return Ok(dt.timestamp_micros());
    }

    let ndt = NaiveDateTime::parse_from_str(ts_str, fmt).map_err(|e| e.to_string())?;
    Ok(ndt.and_utc().timestamp_micros())
}

// ══════════════════════════════════════════════════════════════════
// to_json(value) -> text
// ══════════════════════════════════════════════════════════════════

/// `to_json(value) -> text`
///
/// Converts any SQL value to a JSON text string.
/// Works with all Arrow data types — strings, numbers, booleans, structs.
///
/// # Examples
///
/// ```sql
/// SELECT to_json(42);           -- Returns: '42'
/// SELECT to_json('hello');      -- Returns: '"hello"'
/// ```
#[derive(Debug)]
pub struct ToJsonUdf {
    signature: Signature,
}

impl ToJsonUdf {
    /// Creates a new `to_json` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl Default for ToJsonUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for ToJsonUdf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for ToJsonUdf {}

impl Hash for ToJsonUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "to_json".hash(state);
    }
}

impl ScalarUDFImpl for ToJsonUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "to_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let arr = &expanded[0];
        let len = arr.len();

        let mut builder = StringBuilder::with_capacity(len, 256);
        for row in 0..len {
            if arr.is_null(row) {
                builder.append_value("null");
            } else {
                let val = arrow_value_to_json(arr, row);
                builder.append_value(val.to_string());
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Convert an Arrow array value at a given row to a `serde_json::Value`.
fn arrow_value_to_json(arr: &ArrayRef, row: usize) -> serde_json::Value {
    if arr.is_null(row) {
        return serde_json::Value::Null;
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return serde_json::Value::String(a.value(row).to_owned());
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow_array::Int64Array>() {
        return serde_json::Value::Number(a.value(row).into());
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow_array::Int32Array>() {
        return serde_json::Value::Number(i64::from(a.value(row)).into());
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow_array::Float64Array>() {
        if let Some(n) = serde_json::Number::from_f64(a.value(row)) {
            return serde_json::Value::Number(n);
        }
        return serde_json::Value::Null;
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow_array::BooleanArray>() {
        return serde_json::Value::Bool(a.value(row));
    }
    // JSONB passthrough
    if let Some(a) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
        if let Some(text) = json_types::jsonb_to_text(a.value(row)) {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
                return val;
            }
            return serde_json::Value::String(text);
        }
        return serde_json::Value::Null;
    }
    // Timestamp types
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        let micros = a.value(row);
        let secs = micros / 1_000_000;
        let sub_micros = (micros % 1_000_000).unsigned_abs() * 1_000;
        #[allow(clippy::cast_possible_truncation)]
        let nsecs = sub_micros as u32;
        if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
            return serde_json::Value::String(dt.to_rfc3339());
        }
        return serde_json::Value::Number(micros.into());
    }
    // Fallback via ScalarValue display
    let sv = datafusion_common::ScalarValue::try_from_array(arr, row).ok();
    match sv {
        Some(s) => serde_json::Value::String(s.to_string()),
        None => serde_json::Value::Null,
    }
}

// ══════════════════════════════════════════════════════════════════
// from_json(string) -> jsonb
// ══════════════════════════════════════════════════════════════════

/// `from_json(string) -> jsonb`
///
/// Parses a JSON string and returns the JSONB binary representation.
/// Returns NULL for invalid JSON.
///
/// # Examples
///
/// ```sql
/// SELECT from_json('{"name": "Alice", "age": 30}');
/// SELECT json_typeof(from_json('42'));  -- Returns: 'number'
/// ```
#[derive(Debug)]
pub struct FromJsonUdf {
    signature: Signature,
}

impl FromJsonUdf {
    /// Creates a new `from_json` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for FromJsonUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for FromJsonUdf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for FromJsonUdf {}

impl Hash for FromJsonUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "from_json".hash(state);
    }
}

impl ScalarUDFImpl for FromJsonUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "from_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary) // Returns JSONB
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let expanded = expand_args(&args.args)?;
        let str_arr = expanded[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal("from_json: arg must be Utf8".into())
            })?;

        let mut builder = LargeBinaryBuilder::with_capacity(str_arr.len(), 256);
        for i in 0..str_arr.len() {
            if str_arr.is_null(i) {
                builder.append_null();
            } else {
                let json_str = str_arr.value(i);
                match serde_json::from_str::<serde_json::Value>(json_str) {
                    Ok(val) => {
                        let jsonb = json_types::encode_jsonb(&val);
                        builder.append_value(&jsonb);
                    }
                    Err(_) => builder.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// ══════════════════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion_common::config::ConfigOptions;

    fn make_args_2(a: ArrayRef, b: ArrayRef) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(a), ColumnarValue::Array(b)],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new(
                "output",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            )),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    fn make_args_1(a: ArrayRef) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(a)],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    // ── parse_epoch tests ────────────────────────────────────

    #[test]
    fn test_parse_epoch_seconds() {
        let udf = ParseEpochUdf::new();
        let vals = Arc::new(arrow_array::Int64Array::from(vec![1_708_528_800])) as ArrayRef;
        let units = Arc::new(StringArray::from(vec!["seconds"])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_2(vals, units)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let ts = arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_708_528_800_000_000);
    }

    #[test]
    fn test_parse_epoch_milliseconds() {
        let udf = ParseEpochUdf::new();
        let vals = Arc::new(arrow_array::Int64Array::from(vec![1_708_528_800_000])) as ArrayRef;
        let units = Arc::new(StringArray::from(vec!["milliseconds"])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_2(vals, units)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let ts = arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_708_528_800_000_000);
    }

    #[test]
    fn test_parse_epoch_microseconds() {
        let udf = ParseEpochUdf::new();
        let vals = Arc::new(arrow_array::Int64Array::from(vec![1_708_528_800_000_000])) as ArrayRef;
        let units = Arc::new(StringArray::from(vec!["microseconds"])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_2(vals, units)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let ts = arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_708_528_800_000_000);
    }

    #[test]
    fn test_parse_epoch_nanoseconds() {
        let udf = ParseEpochUdf::new();
        let vals = Arc::new(arrow_array::Int64Array::from(vec![
            1_708_528_800_000_000_000,
        ])) as ArrayRef;
        let units = Arc::new(StringArray::from(vec!["nanoseconds"])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_2(vals, units)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let ts = arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_708_528_800_000_000);
    }

    #[test]
    fn test_parse_epoch_short_units() {
        let udf = ParseEpochUdf::new();
        for (val, unit, expected) in [
            (100i64, "s", 100_000_000i64),
            (100_000, "ms", 100_000_000),
            (100_000_000, "us", 100_000_000),
            (100_000_000_000, "ns", 100_000_000),
        ] {
            let vals = Arc::new(arrow_array::Int64Array::from(vec![val])) as ArrayRef;
            let units = Arc::new(StringArray::from(vec![unit])) as ArrayRef;
            let result = udf.invoke_with_args(make_args_2(vals, units)).unwrap();
            let arr = match result {
                ColumnarValue::Array(a) => a,
                _ => panic!("expected array"),
            };
            let ts = arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            assert_eq!(ts.value(0), expected, "Failed for unit '{unit}'");
        }
    }

    #[test]
    fn test_parse_epoch_invalid_unit() {
        let udf = ParseEpochUdf::new();
        let vals = Arc::new(arrow_array::Int64Array::from(vec![100])) as ArrayRef;
        let units = Arc::new(StringArray::from(vec!["invalid"])) as ArrayRef;
        assert!(udf.invoke_with_args(make_args_2(vals, units)).is_err());
    }

    #[test]
    fn test_parse_epoch_null_handling() {
        let udf = ParseEpochUdf::new();
        let vals = Arc::new(arrow_array::Int64Array::from(vec![
            Some(100),
            None,
            Some(200),
        ])) as ArrayRef;
        let units = Arc::new(StringArray::from(vec![
            Some("seconds"),
            Some("seconds"),
            Some("seconds"),
        ])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_2(vals, units)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let ts = arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert!(!ts.is_null(0));
        assert!(ts.is_null(1));
        assert!(!ts.is_null(2));
    }

    // ── parse_timestamp tests ────────────────────────────────

    #[test]
    fn test_parse_timestamp_custom_format() {
        let udf = ParseTimestampUdf::new();
        let strs = Arc::new(StringArray::from(vec!["2026-02-21 14:30:00"])) as ArrayRef;
        let fmts = Arc::new(StringArray::from(vec!["%Y-%m-%d %H:%M:%S"])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_2(strs, fmts)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let ts = arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert!(!ts.is_null(0));
        // 2026-02-21 14:30:00 UTC = 1771684200 seconds since epoch
        let expected = 1_771_684_200_000_000i64;
        assert_eq!(ts.value(0), expected);
    }

    #[test]
    fn test_parse_timestamp_iso8601() {
        let udf = ParseTimestampUdf::new();
        let strs = Arc::new(StringArray::from(vec!["2026-02-21T14:30:00Z"])) as ArrayRef;
        let fmts = Arc::new(StringArray::from(vec!["iso8601"])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_2(strs, fmts)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let ts = arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert!(!ts.is_null(0));
    }

    #[test]
    fn test_parse_timestamp_invalid_returns_null() {
        let udf = ParseTimestampUdf::new();
        let strs = Arc::new(StringArray::from(vec!["not-a-timestamp"])) as ArrayRef;
        let fmts = Arc::new(StringArray::from(vec!["%Y-%m-%d %H:%M:%S"])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_2(strs, fmts)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let ts = arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert!(ts.is_null(0));
    }

    // ── to_json tests ────────────────────────────────────────

    #[test]
    fn test_to_json_int() {
        let udf = ToJsonUdf::new();
        let vals = Arc::new(arrow_array::Int64Array::from(vec![42])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_1(vals)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "42");
    }

    #[test]
    fn test_to_json_string() {
        let udf = ToJsonUdf::new();
        let vals = Arc::new(StringArray::from(vec!["hello"])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_1(vals)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "\"hello\"");
    }

    #[test]
    fn test_to_json_bool() {
        let udf = ToJsonUdf::new();
        let vals = Arc::new(arrow_array::BooleanArray::from(vec![true, false])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_1(vals)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "true");
        assert_eq!(str_arr.value(1), "false");
    }

    #[test]
    fn test_to_json_null() {
        let udf = ToJsonUdf::new();
        let vals = Arc::new(arrow_array::Int64Array::from(vec![None::<i64>])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_1(vals)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "null");
    }

    // ── from_json tests ──────────────────────────────────────

    #[test]
    fn test_from_json_object() {
        let udf = FromJsonUdf::new();
        let strs = Arc::new(StringArray::from(vec![r#"{"name":"Alice","age":30}"#])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_1(strs)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert!(!bin.is_null(0));
        let val = bin.value(0);
        assert_eq!(json_types::jsonb_type_name(val), Some("object"));
        let name = json_types::jsonb_get_field(val, "name").unwrap();
        assert_eq!(json_types::jsonb_to_text(name), Some("Alice".to_owned()));
    }

    #[test]
    fn test_from_json_number() {
        let udf = FromJsonUdf::new();
        let strs = Arc::new(StringArray::from(vec!["42"])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_1(strs)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert_eq!(json_types::jsonb_type_name(bin.value(0)), Some("number"));
    }

    #[test]
    fn test_from_json_invalid_returns_null() {
        let udf = FromJsonUdf::new();
        let strs = Arc::new(StringArray::from(vec!["not json {{{"])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_1(strs)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert!(bin.is_null(0));
    }

    #[test]
    fn test_from_json_null_input() {
        let udf = FromJsonUdf::new();
        let strs = Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef;
        let result = udf.invoke_with_args(make_args_1(strs)).unwrap();
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert!(bin.is_null(0));
    }

    // ── Registration tests ───────────────────────────────────

    #[test]
    fn test_registration() {
        use datafusion_expr::ScalarUDF;

        let udfs = [
            ScalarUDF::new_from_impl(ParseEpochUdf::new()),
            ScalarUDF::new_from_impl(ParseTimestampUdf::new()),
            ScalarUDF::new_from_impl(ToJsonUdf::new()),
            ScalarUDF::new_from_impl(FromJsonUdf::new()),
        ];
        let names: Vec<&str> = udfs.iter().map(|u| u.name()).collect();
        assert_eq!(
            names,
            &["parse_epoch", "parse_timestamp", "to_json", "from_json"]
        );
    }
}
