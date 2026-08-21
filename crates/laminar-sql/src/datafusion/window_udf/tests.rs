use super::*;
use arrow::datatypes::{IntervalDayTime, IntervalMonthDayNano, TimestampMicrosecondType};
use arrow_array::cast::AsArray;
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

/// Returns the microsecond value of a UDF scalar result, or panics
/// if the result isn't a `TimestampMicrosecond` scalar.
fn expect_ts_us(result: ColumnarValue) -> Option<i64> {
    match result {
        ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(v, _)) => v,
        other => panic!("Expected TimestampMicrosecond scalar, got: {other:?}"),
    }
}

/// Returns the per-row microsecond values of a UDF array result.
fn array_values_us(arr: &dyn Array) -> Vec<Option<i64>> {
    let r = arr.as_primitive::<TimestampMicrosecondType>();
    (0..r.len())
        .map(|i| if r.is_null(i) { None } else { Some(r.value(i)) })
        .collect()
}

fn make_args(args: Vec<ColumnarValue>, rows: usize) -> ScalarFunctionArgs {
    ScalarFunctionArgs {
        args,
        arg_fields: vec![],
        number_rows: rows,
        return_field: Arc::new(Field::new(
            "output",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )),
        config_options: Arc::new(ConfigOptions::default()),
    }
}

// ── Tumble ──────────────────────────────────────────────────────────

#[test]
fn test_tumble_basic() {
    // 5-minute interval, ts=7 minutes (420 000 ms) → bucket start =
    // 5 minutes = 300 000 ms = 300 000 000 µs.
    let result = TumbleWindowStart::new()
        .invoke_with_args(make_args(
            vec![ts_ms(Some(420_000)), interval_dt(0, 300_000)],
            1,
        ))
        .unwrap();
    assert_eq!(expect_ts_us(result), Some(300_000_000));
}

#[test]
fn test_tumble_exact_boundary() {
    let result = TumbleWindowStart::new()
        .invoke_with_args(make_args(
            vec![ts_ms(Some(300_000)), interval_dt(0, 300_000)],
            1,
        ))
        .unwrap();
    assert_eq!(expect_ts_us(result), Some(300_000_000));
}

#[test]
fn test_tumble_zero_timestamp() {
    let result = TumbleWindowStart::new()
        .invoke_with_args(make_args(vec![ts_ms(Some(0)), interval_dt(0, 300_000)], 1))
        .unwrap();
    assert_eq!(expect_ts_us(result), Some(0));
}

#[test]
fn test_tumble_null_handling() {
    let result = TumbleWindowStart::new()
        .invoke_with_args(make_args(vec![ts_ms(None), interval_dt(0, 300_000)], 1))
        .unwrap();
    assert_eq!(expect_ts_us(result), None);
}

#[test]
fn test_tumble_array_input() {
    let ts = ColumnarValue::Array(Arc::new(TimestampMillisecondArray::from(vec![
        Some(0),
        Some(150_000),
        Some(300_000),
        Some(420_000),
        None,
    ])));
    let result = TumbleWindowStart::new()
        .invoke_with_args(make_args(vec![ts, interval_dt(0, 300_000)], 5))
        .unwrap();
    match result {
        ColumnarValue::Array(arr) => assert_eq!(
            array_values_us(&arr),
            vec![Some(0), Some(0), Some(300_000_000), Some(300_000_000), None,]
        ),
        ColumnarValue::Scalar(_) => panic!("Expected array result"),
    }
}

/// Regression: TUMBLE over a `Timestamp(Nanosecond)` column must
/// take the `to_millis_array` path (any precision in, milliseconds
/// out) rather than failing on the array fast path.
#[test]
fn test_tumble_array_input_nanosecond() {
    use arrow_array::TimestampNanosecondArray;
    let ts = ColumnarValue::Array(Arc::new(TimestampNanosecondArray::from(vec![
        Some(0),
        Some(150_000_000_000),
        Some(300_000_000_000),
        Some(420_000_000_000),
        None,
    ])));
    let result = TumbleWindowStart::new()
        .invoke_with_args(make_args(vec![ts, interval_dt(0, 300_000)], 5))
        .unwrap();
    match result {
        ColumnarValue::Array(arr) => assert_eq!(
            array_values_us(&arr),
            vec![Some(0), Some(0), Some(300_000_000), Some(300_000_000), None,]
        ),
        ColumnarValue::Scalar(_) => panic!("Expected array result"),
    }
}

/// Regression: HOP over `Timestamp(Nanosecond)` — same shape as the
/// tumble nanosecond regression.
#[test]
fn test_hop_array_input_nanosecond() {
    use arrow_array::TimestampNanosecondArray;
    let ts = ColumnarValue::Array(Arc::new(TimestampNanosecondArray::from(vec![Some(
        420_000_000_000,
    )])));
    let result = HopWindowStart::new()
        .invoke_with_args(make_args(
            vec![ts, interval_dt(0, 300_000), interval_dt(0, 600_000)],
            1,
        ))
        .unwrap();
    match result {
        ColumnarValue::Array(arr) => assert_eq!(array_values_us(&arr), vec![Some(0)]),
        ColumnarValue::Scalar(_) => panic!("Expected array result"),
    }
}

#[test]
fn test_tumble_month_day_nano_interval() {
    // 1 hour as IntervalMonthDayNano (3 600 s in nanoseconds);
    // ts = 90 minutes (5 400 000 ms) → bucket start = 1 hour =
    // 3 600 000 ms = 3 600 000 000 µs.
    let interval = ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(
        IntervalMonthDayNano::new(0, 0, 3_600_000_000_000),
    )));
    let result = TumbleWindowStart::new()
        .invoke_with_args(make_args(vec![ts_ms(Some(5_400_000)), interval], 1))
        .unwrap();
    assert_eq!(expect_ts_us(result), Some(3_600_000_000));
}

#[test]
fn test_tumble_rejects_zero_interval() {
    let result = TumbleWindowStart::new()
        .invoke_with_args(make_args(vec![ts_ms(Some(1000)), interval_dt(0, 0)], 1));
    assert!(result.is_err());
}

#[test]
fn test_tumble_rejects_wrong_arg_count() {
    let result = TumbleWindowStart::new().invoke_with_args(make_args(vec![ts_ms(Some(1000))], 1));
    assert!(result.is_err());
}

// ── Tumble end ──────────────────────────────────────────────────────

#[test]
fn test_tumble_end_basic() {
    // 5-minute interval, ts=7 min → window [5, 10) min → end = 10
    // min = 600 000 ms = 600 000 000 µs.
    let result = TumbleWindowEnd::new()
        .invoke_with_args(make_args(
            vec![ts_ms(Some(420_000)), interval_dt(0, 300_000)],
            1,
        ))
        .unwrap();
    assert_eq!(expect_ts_us(result), Some(600_000_000));
}

#[test]
fn test_tumble_end_at_boundary() {
    let result = TumbleWindowEnd::new()
        .invoke_with_args(make_args(
            vec![ts_ms(Some(300_000)), interval_dt(0, 300_000)],
            1,
        ))
        .unwrap();
    assert_eq!(expect_ts_us(result), Some(600_000_000));
}

#[test]
fn test_tumble_end_null_propagates() {
    let result = TumbleWindowEnd::new()
        .invoke_with_args(make_args(vec![ts_ms(None), interval_dt(0, 300_000)], 1))
        .unwrap();
    assert_eq!(expect_ts_us(result), None);
}

#[test]
fn test_tumble_end_array_input() {
    let ts = ColumnarValue::Array(Arc::new(TimestampMillisecondArray::from(vec![
        Some(0),
        Some(150_000),
        Some(300_000),
        Some(420_000),
        None,
    ])));
    let result = TumbleWindowEnd::new()
        .invoke_with_args(make_args(vec![ts, interval_dt(0, 300_000)], 5))
        .unwrap();
    match result {
        ColumnarValue::Array(arr) => assert_eq!(
            array_values_us(&arr),
            vec![
                Some(300_000_000),
                Some(300_000_000),
                Some(600_000_000),
                Some(600_000_000),
                None,
            ]
        ),
        ColumnarValue::Scalar(_) => panic!("Expected array result"),
    }
}

#[test]
fn test_tumble_end_array_input_nanosecond() {
    use arrow_array::TimestampNanosecondArray;
    let ts = ColumnarValue::Array(Arc::new(TimestampNanosecondArray::from(vec![Some(
        420_000_000_000,
    )])));
    let result = TumbleWindowEnd::new()
        .invoke_with_args(make_args(vec![ts, interval_dt(0, 300_000)], 1))
        .unwrap();
    match result {
        ColumnarValue::Array(arr) => assert_eq!(array_values_us(&arr), vec![Some(600_000_000)]),
        ColumnarValue::Scalar(_) => panic!("Expected array result"),
    }
}

#[test]
fn test_tumble_end_rejects_zero_interval() {
    let result = TumbleWindowEnd::new()
        .invoke_with_args(make_args(vec![ts_ms(Some(1000)), interval_dt(0, 0)], 1));
    assert!(result.is_err());
}

#[test]
fn test_tumble_end_rejects_wrong_arg_count() {
    let result = TumbleWindowEnd::new().invoke_with_args(make_args(vec![ts_ms(Some(1000))], 1));
    assert!(result.is_err());
}

// ── Hop ─────────────────────────────────────────────────────────────

#[test]
fn test_hop_basic() {
    // slide=5 min, size=10 min, ts=7 min → earliest start = 0 (the
    // `[-2 min, 8 min)` window doesn't exist because earliest start
    // is non-negative; correct earliest start that contains 7 min
    // is 0 because `[0, 10)` is the first such window).
    let result = HopWindowStart::new()
        .invoke_with_args(make_args(
            vec![
                ts_ms(Some(420_000)),
                interval_dt(0, 300_000),
                interval_dt(0, 600_000),
            ],
            1,
        ))
        .unwrap();
    assert_eq!(expect_ts_us(result), Some(0));
}

#[test]
fn test_hop_at_boundary() {
    let result = HopWindowStart::new()
        .invoke_with_args(make_args(
            vec![
                ts_ms(Some(300_000)),
                interval_dt(0, 300_000),
                interval_dt(0, 600_000),
            ],
            1,
        ))
        .unwrap();
    assert_eq!(expect_ts_us(result), Some(0));
}

#[test]
fn test_hop_rejects_wrong_arg_count() {
    let result = HopWindowStart::new().invoke_with_args(make_args(
        vec![ts_ms(Some(1000)), interval_dt(0, 300_000)],
        1,
    ));
    assert!(result.is_err());
}

// ── Hop end ─────────────────────────────────────────────────────────

#[test]
fn test_hop_end_basic() {
    // slide=5 min, size=10 min, ts=7 min → earliest start=0,
    // end=10 min = 600 000 000 µs.
    let result = HopWindowEnd::new()
        .invoke_with_args(make_args(
            vec![
                ts_ms(Some(420_000)),
                interval_dt(0, 300_000),
                interval_dt(0, 600_000),
            ],
            1,
        ))
        .unwrap();
    assert_eq!(expect_ts_us(result), Some(600_000_000));
}

#[test]
fn test_hop_end_rejects_wrong_arg_count() {
    let result = HopWindowEnd::new().invoke_with_args(make_args(
        vec![ts_ms(Some(1000)), interval_dt(0, 300_000)],
        1,
    ));
    assert!(result.is_err());
}

// ── Session ─────────────────────────────────────────────────────────

#[test]
fn test_session_passthrough_scalar() {
    let result = SessionWindowStart::new()
        .invoke_with_args(make_args(
            vec![ts_ms(Some(42_000)), interval_dt(0, 60_000)],
            1,
        ))
        .unwrap();
    assert_eq!(expect_ts_us(result), Some(42_000_000));
}

#[test]
fn test_session_passthrough_null() {
    let result = SessionWindowStart::new()
        .invoke_with_args(make_args(vec![ts_ms(None), interval_dt(0, 60_000)], 1))
        .unwrap();
    assert_eq!(expect_ts_us(result), None);
}

// ── Cumulate ────────────────────────────────────────────────────────

#[test]
fn test_cumulate_basic() {
    // step=1 min, size=5 min, ts=30 s → epoch start = 0.
    let result = CumulateWindowStart::new()
        .invoke_with_args(make_args(
            vec![
                ts_ms(Some(30_000)),
                interval_dt(0, 60_000),
                interval_dt(0, 300_000),
            ],
            1,
        ))
        .unwrap();
    assert_eq!(expect_ts_us(result), Some(0));
}

#[test]
fn test_cumulate_second_epoch() {
    // ts=350 s → epoch start = 5 min = 300 000 000 µs.
    let result = CumulateWindowStart::new()
        .invoke_with_args(make_args(
            vec![
                ts_ms(Some(350_000)),
                interval_dt(0, 60_000),
                interval_dt(0, 300_000),
            ],
            1,
        ))
        .unwrap();
    assert_eq!(expect_ts_us(result), Some(300_000_000));
}

#[test]
fn test_cumulate_rejects_step_exceeds_size() {
    let result = CumulateWindowStart::new().invoke_with_args(make_args(
        vec![
            ts_ms(Some(1000)),
            interval_dt(0, 600_000),
            interval_dt(0, 300_000),
        ],
        1,
    ));
    assert!(result.is_err());
}

#[test]
fn test_cumulate_rejects_not_divisible() {
    let result = CumulateWindowStart::new().invoke_with_args(make_args(
        vec![
            ts_ms(Some(1000)),
            interval_dt(0, 70_000),
            interval_dt(0, 300_000),
        ],
        1,
    ));
    assert!(result.is_err());
}

#[test]
fn test_cumulate_rejects_wrong_arg_count() {
    let result = CumulateWindowStart::new().invoke_with_args(make_args(
        vec![ts_ms(Some(1000)), interval_dt(0, 60_000)],
        1,
    ));
    assert!(result.is_err());
}

// ── Cumulate end ────────────────────────────────────────────────────

#[test]
fn test_cumulate_end_basic() {
    // ts=30 s → epoch=[0, 5 min) → end = 5 min = 300 000 000 µs.
    let result = CumulateWindowEnd::new()
        .invoke_with_args(make_args(
            vec![
                ts_ms(Some(30_000)),
                interval_dt(0, 60_000),
                interval_dt(0, 300_000),
            ],
            1,
        ))
        .unwrap();
    assert_eq!(expect_ts_us(result), Some(300_000_000));
}

#[test]
fn test_cumulate_end_rejects_step_exceeds_size() {
    let result = CumulateWindowEnd::new().invoke_with_args(make_args(
        vec![
            ts_ms(Some(1000)),
            interval_dt(0, 600_000),
            interval_dt(0, 300_000),
        ],
        1,
    ));
    assert!(result.is_err());
}

// ── Registration / signature ────────────────────────────────────────

#[test]
fn test_udf_registration() {
    for (impl_name, expected) in [
        (
            ScalarUDF::new_from_impl(TumbleWindowStart::new())
                .name()
                .to_string(),
            "tumble",
        ),
        (
            ScalarUDF::new_from_impl(TumbleWindowEnd::new())
                .name()
                .to_string(),
            "tumble_end",
        ),
        (
            ScalarUDF::new_from_impl(HopWindowStart::new())
                .name()
                .to_string(),
            "hop",
        ),
        (
            ScalarUDF::new_from_impl(HopWindowEnd::new())
                .name()
                .to_string(),
            "hop_end",
        ),
        (
            ScalarUDF::new_from_impl(SessionWindowStart::new())
                .name()
                .to_string(),
            "session",
        ),
        (
            ScalarUDF::new_from_impl(CumulateWindowStart::new())
                .name()
                .to_string(),
            "cumulate",
        ),
        (
            ScalarUDF::new_from_impl(CumulateWindowEnd::new())
                .name()
                .to_string(),
            "cumulate_end",
        ),
    ] {
        assert_eq!(impl_name, expected);
    }
}

#[test]
fn test_udf_signatures_immutable() {
    for sig in [
        TumbleWindowStart::new().signature().clone(),
        TumbleWindowEnd::new().signature().clone(),
        HopWindowStart::new().signature().clone(),
        HopWindowEnd::new().signature().clone(),
        SessionWindowStart::new().signature().clone(),
        CumulateWindowStart::new().signature().clone(),
        CumulateWindowEnd::new().signature().clone(),
    ] {
        assert_eq!(sig.volatility, Volatility::Immutable);
    }
}

#[test]
fn test_return_types_microsecond() {
    let target = DataType::Timestamp(TimeUnit::Microsecond, None);
    assert_eq!(TumbleWindowStart::new().return_type(&[]).unwrap(), target);
    assert_eq!(TumbleWindowEnd::new().return_type(&[]).unwrap(), target);
    assert_eq!(HopWindowStart::new().return_type(&[]).unwrap(), target);
    assert_eq!(HopWindowEnd::new().return_type(&[]).unwrap(), target);
    assert_eq!(SessionWindowStart::new().return_type(&[]).unwrap(), target);
    assert_eq!(CumulateWindowStart::new().return_type(&[]).unwrap(), target);
    assert_eq!(CumulateWindowEnd::new().return_type(&[]).unwrap(), target);
}
