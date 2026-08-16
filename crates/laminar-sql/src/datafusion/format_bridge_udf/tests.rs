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
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
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
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
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
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
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
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
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
        let ColumnarValue::Array(arr) = result else {
            panic!("expected array")
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
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
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
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
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
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
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
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
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
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(str_arr.value(0), "42");
}

#[test]
fn test_to_json_string() {
    let udf = ToJsonUdf::new();
    let vals = Arc::new(StringArray::from(vec!["hello"])) as ArrayRef;
    let result = udf.invoke_with_args(make_args_1(vals)).unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(str_arr.value(0), "\"hello\"");
}

#[test]
fn test_to_json_bool() {
    let udf = ToJsonUdf::new();
    let vals = Arc::new(arrow_array::BooleanArray::from(vec![true, false])) as ArrayRef;
    let result = udf.invoke_with_args(make_args_1(vals)).unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
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
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
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
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
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
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    assert_eq!(json_types::jsonb_type_name(bin.value(0)), Some("number"));
}

#[test]
fn test_from_json_invalid_returns_null() {
    let udf = FromJsonUdf::new();
    let strs = Arc::new(StringArray::from(vec!["not json {{{"])) as ArrayRef;
    let result = udf.invoke_with_args(make_args_1(strs)).unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    assert!(bin.is_null(0));
}

#[test]
fn test_from_json_null_input() {
    let udf = FromJsonUdf::new();
    let strs = Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef;
    let result = udf.invoke_with_args(make_args_1(strs)).unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
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
    let names: Vec<&str> = udfs.iter().map(datafusion_expr::ScalarUDF::name).collect();
    assert_eq!(
        names,
        &["parse_epoch", "parse_timestamp", "to_json", "from_json"]
    );
}
