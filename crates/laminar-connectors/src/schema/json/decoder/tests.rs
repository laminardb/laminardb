use super::value::checked_epoch_to_unit;
use super::*;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_schema::{Field, Schema, TimeUnit};

fn make_schema(fields: Vec<(&str, DataType, bool)>) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .into_iter()
            .map(|(name, dt, nullable)| Field::new(name, dt, nullable))
            .collect::<Vec<_>>(),
    ))
}

fn json_record(json: &str) -> RawRecord {
    RawRecord::new(json.as_bytes().to_vec())
}

// ── Basic decode tests ────────────────────────────────────

#[test]
fn test_decode_empty_batch() {
    let schema = make_schema(vec![("id", DataType::Int64, false)]);
    let decoder = JsonDecoder::new(schema.clone());
    let batch = decoder.decode_batch(&[]).unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.schema(), schema);
}

#[test]
fn test_decode_single_record() {
    let schema = make_schema(vec![
        ("id", DataType::Int64, false),
        ("name", DataType::Utf8, true),
    ]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"id": 42, "name": "Alice"}"#)];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        42
    );
    assert_eq!(batch.column(1).as_string::<i32>().value(0), "Alice");
}

#[test]
fn test_decode_multiple_records() {
    let schema = make_schema(vec![
        ("x", DataType::Int64, false),
        ("y", DataType::Float64, false),
    ]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![
        json_record(r#"{"x": 1, "y": 1.5}"#),
        json_record(r#"{"x": 2, "y": 2.5}"#),
        json_record(r#"{"x": 3, "y": 3.5}"#),
    ];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 3);
    let x_col = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(x_col.value(0), 1);
    assert_eq!(x_col.value(1), 2);
    assert_eq!(x_col.value(2), 3);
}

#[test]
fn test_decode_all_types() {
    let schema = make_schema(vec![
        ("bool_col", DataType::Boolean, false),
        ("int_col", DataType::Int64, false),
        ("float_col", DataType::Float64, false),
        ("str_col", DataType::Utf8, false),
    ]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(
        r#"{"bool_col": true, "int_col": 42, "float_col": 3.14, "str_col": "hello"}"#,
    )];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert!(batch.column(0).as_boolean().value(0));
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        42
    );
    let f = batch
        .column(2)
        .as_primitive::<arrow_array::types::Float64Type>()
        .value(0);
    assert!((f - 3.14).abs() < f64::EPSILON);
    assert_eq!(batch.column(3).as_string::<i32>().value(0), "hello");
}

// ── Null handling ─────────────────────────────────────────

#[test]
fn test_decode_null_values() {
    let schema = make_schema(vec![
        ("a", DataType::Int64, true),
        ("b", DataType::Utf8, true),
    ]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"a": null, "b": null}"#)];
    let batch = decoder.decode_batch(&records).unwrap();

    assert!(batch.column(0).is_null(0));
    assert!(batch.column(1).is_null(0));
}

#[test]
fn test_decode_missing_field_becomes_null() {
    let schema = make_schema(vec![
        ("a", DataType::Int64, true),
        ("b", DataType::Utf8, true),
    ]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"a": 1}"#)]; // "b" missing
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        1
    );
    assert!(batch.column(1).is_null(0));
}

// ── Type mismatch strategies ──────────────────────────────

#[test]
fn test_mismatch_null_strategy() {
    let schema = make_schema(vec![("x", DataType::Int64, true)]);
    let config = JsonDecoderConfig {
        type_mismatch: TypeMismatchStrategy::Null,
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    let records = vec![json_record(r#"{"x": "not_a_number"}"#)];
    let batch = decoder.decode_batch(&records).unwrap();

    assert!(batch.column(0).is_null(0));
    assert_eq!(decoder.mismatch_count(), 1);
}

#[test]
fn test_mismatch_coerce_strategy() {
    let schema = make_schema(vec![("x", DataType::Int64, true)]);
    let config = JsonDecoderConfig {
        type_mismatch: TypeMismatchStrategy::Coerce,
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    let records = vec![json_record(r#"{"x": "123"}"#)];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        123
    );
}

#[test]
fn test_mismatch_reject_strategy() {
    let schema = make_schema(vec![("x", DataType::Int64, false)]);
    let config = JsonDecoderConfig {
        type_mismatch: TypeMismatchStrategy::Reject,
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    let records = vec![json_record(r#"{"x": "not_a_number"}"#)];
    let result = decoder.decode_batch(&records);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("type mismatch"));
}

// ── Unknown field strategies ──────────────────────────────

#[test]
fn test_unknown_fields_ignore() {
    let schema = make_schema(vec![("a", DataType::Int64, false)]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"a": 1, "unknown": "value"}"#)];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_unknown_fields_reject() {
    let schema = make_schema(vec![("a", DataType::Int64, false)]);
    let config = JsonDecoderConfig {
        unknown_fields: UnknownFieldStrategy::Reject,
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    let records = vec![json_record(r#"{"a": 1, "unknown": "value"}"#)];
    let result = decoder.decode_batch(&records);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("unknown field"));
}

#[test]
fn test_unknown_fields_collect_extra() {
    let schema = make_schema(vec![("a", DataType::Int64, false)]);
    let config = JsonDecoderConfig {
        unknown_fields: UnknownFieldStrategy::CollectExtra,
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    let records = vec![json_record(r#"{"a": 1, "extra1": "v1", "extra2": 42}"#)];
    let batch = decoder.decode_batch(&records).unwrap();

    // Schema should have an extra `_extra` column.
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(1).name(), "_extra");
    assert!(!batch.column(1).is_null(0));
}

// ── Timestamp parsing ─────────────────────────────────────

#[test]
fn test_decode_timestamp_iso8601() {
    let schema = make_schema(vec![(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        false,
    )]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"ts": "2025-01-15T10:30:00Z"}"#)];
    let batch = decoder.decode_batch(&records).unwrap();

    assert!(!batch.column(0).is_null(0));
}

#[test]
fn test_decode_timestamp_epoch_millis() {
    let schema = make_schema(vec![(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"ts": 1705312200000}"#)];
    let batch = decoder.decode_batch(&records).unwrap();

    let ts_col = batch
        .column(0)
        .as_primitive::<arrow_array::types::TimestampNanosecondType>();
    // 1705312200000 ms * 1_000_000 = nanos
    assert_eq!(ts_col.value(0), 1_705_312_200_000_000_000);
}

#[test]
fn test_decode_out_of_range_float_timestamp_is_rejected() {
    // A garbage float epoch-ms must not silently saturate to i64::MAX
    // and poison event-time/watermarks — it routes through the
    // configured type-mismatch path instead.
    let schema = make_schema(vec![(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"ts": 1e30}"#)];
    let result = decoder.decode_batch(&records);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("not exactly representable"));
}

#[test]
fn test_decode_timestamp_overflow_on_nanosecond_scaling_is_rejected() {
    // A ms value that fits i64 but overflows when scaled to nanoseconds
    // must error, not wrap into a bogus (watermark-poisoning) timestamp.
    let schema = make_schema(vec![(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"ts": 9999999999999999}"#)];
    let result = decoder.decode_batch(&records);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("out of i64 Nanosecond range"));
}

// ── Nested objects as LargeBinary ─────────────────────────

#[test]
fn test_decode_nested_object_as_json_string() {
    let schema = make_schema(vec![("data", DataType::LargeBinary, true)]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"data": {"nested": true}}"#)];
    let batch = decoder.decode_batch(&records).unwrap();

    assert!(!batch.column(0).is_null(0));
}

// ── Error cases ───────────────────────────────────────────

#[test]
fn test_decode_invalid_json() {
    let schema = make_schema(vec![("a", DataType::Int64, false)]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![RawRecord::new(b"not json".to_vec())];
    let result = decoder.decode_batch(&records);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("JSON parse error"));
}

#[test]
fn test_decode_non_object_json() {
    let schema = make_schema(vec![("a", DataType::Int64, false)]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record("[1, 2, 3]")];
    let result = decoder.decode_batch(&records);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("must be an object"));
}

// ── FormatDecoder trait ───────────────────────────────────

#[test]
fn test_format_name() {
    let schema = make_schema(vec![("a", DataType::Int64, false)]);
    let decoder = JsonDecoder::new(schema);
    assert_eq!(decoder.format_name(), "json");
}

#[test]
fn test_output_schema() {
    let schema = make_schema(vec![
        ("a", DataType::Int64, false),
        ("b", DataType::Utf8, true),
    ]);
    let decoder = JsonDecoder::new(schema.clone());
    assert_eq!(decoder.output_schema(), schema);
}

#[test]
fn test_decode_one() {
    let schema = make_schema(vec![("x", DataType::Int64, false)]);
    let decoder = JsonDecoder::new(schema);
    let record = json_record(r#"{"x": 99}"#);
    let batch = decoder.decode_one(&record).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        99
    );
}

// ── Int/Float numeric coercion ────────────────────────────

#[test]
fn test_decode_int_from_float_json() {
    // JSON number 42.0 is parsed as f64 by serde_json. With the default
    // Coerce strategy, it is coerced to Int64 = 42.
    let schema = make_schema(vec![("x", DataType::Int64, true)]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"x": 42.0}"#)];
    let batch = decoder.decode_batch(&records).unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        42
    );
}

#[test]
fn test_decode_float_from_int_json() {
    // JSON integer 42 should decode as Float64 = 42.0.
    let schema = make_schema(vec![("x", DataType::Float64, false)]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"x": 42}"#)];
    let batch = decoder.decode_batch(&records).unwrap();
    let val = batch
        .column(0)
        .as_primitive::<arrow_array::types::Float64Type>()
        .value(0);
    assert!((val - 42.0).abs() < f64::EPSILON);
}

// ── Coercion tests (string→numeric, int→float, etc.) ────

#[test]
fn test_decode_string_number_to_float64() {
    let schema = make_schema(vec![("price", DataType::Float64, false)]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"price": "187.52"}"#)];
    let batch = decoder.decode_batch(&records).unwrap();
    let val = batch
        .column(0)
        .as_primitive::<arrow_array::types::Float64Type>()
        .value(0);
    assert!((val - 187.52).abs() < f64::EPSILON);
}

#[test]
fn test_decode_string_to_int() {
    let schema = make_schema(vec![("qty", DataType::Int32, false)]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"qty": "100"}"#)];
    let batch = decoder.decode_batch(&records).unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>()
            .value(0),
        100
    );
}

#[test]
fn test_decode_epoch_millis_to_timestamp_millis() {
    let schema = make_schema(vec![(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"ts": 1705312200000}"#)];
    let batch = decoder.decode_batch(&records).unwrap();
    let ts_col = batch
        .column(0)
        .as_primitive::<arrow_array::types::TimestampMillisecondType>();
    assert_eq!(ts_col.value(0), 1_705_312_200_000);
}

#[test]
fn test_decode_int_to_float_promotion() {
    let schema = make_schema(vec![("val", DataType::Float64, false)]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"val": 100}"#)];
    let batch = decoder.decode_batch(&records).unwrap();
    let val = batch
        .column(0)
        .as_primitive::<arrow_array::types::Float64Type>()
        .value(0);
    assert!((val - 100.0).abs() < f64::EPSILON);
}

#[test]
fn test_decode_string_boolean() {
    let schema = make_schema(vec![("active", DataType::Boolean, false)]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"active": "true"}"#)];
    let batch = decoder.decode_batch(&records).unwrap();
    assert!(batch.column(0).as_boolean().value(0));
}

#[test]
fn test_coerce_fails_on_unconvertible() {
    // With default Coerce, a string that can't be parsed as Int64 should error.
    let schema = make_schema(vec![("x", DataType::Int64, true)]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"x": "not_a_number"}"#)];
    let result = decoder.decode_batch(&records);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("type coercion failed"));
}

#[test]
fn test_enforcement_str_parsing() {
    assert_eq!(
        TypeMismatchStrategy::from_enforcement_str("coerce"),
        Some(TypeMismatchStrategy::Coerce)
    );
    assert_eq!(
        TypeMismatchStrategy::from_enforcement_str("STRICT"),
        Some(TypeMismatchStrategy::Reject)
    );
    assert_eq!(
        TypeMismatchStrategy::from_enforcement_str("Permissive"),
        Some(TypeMismatchStrategy::Null)
    );
    assert_eq!(TypeMismatchStrategy::from_enforcement_str("unknown"), None);
}

// ── Small integer types ──────────────────────────────────

#[test]
fn test_decode_i8_and_u8() {
    let schema = make_schema(vec![
        ("signed", DataType::Int8, false),
        ("unsigned", DataType::UInt8, false),
    ]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![json_record(r#"{"signed": -5, "unsigned": 200}"#)];
    let batch = decoder.decode_batch(&records).unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int8Type>()
            .value(0),
        -5
    );
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<arrow_array::types::UInt8Type>()
            .value(0),
        200
    );
}

// ── json.path tests ─────────────────────────────────────────

#[test]
fn test_json_path_single() {
    let schema = make_schema(vec![("id", DataType::Int64, false)]);
    let config = JsonDecoderConfig {
        json_path: Some(vec!["data".into()]),
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    let records = vec![json_record(r#"{"data":{"id":1}}"#)];
    let batch = decoder.decode_batch(&records).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        1
    );
}

#[test]
fn test_json_path_multi() {
    let schema = make_schema(vec![("id", DataType::Int64, false)]);
    let config = JsonDecoderConfig {
        json_path: Some(vec!["a".into(), "b".into()]),
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    let records = vec![json_record(r#"{"a":{"b":{"id":1}}}"#)];
    let batch = decoder.decode_batch(&records).unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        1
    );
}

/// Records that don't carry the configured json.path are skipped rather
/// than poisoning a batch containing mixed control and data envelopes.
#[test]
fn test_json_path_missing_skips_record() {
    let schema = make_schema(vec![("id", DataType::Int64, false)]);
    let config = JsonDecoderConfig {
        json_path: Some(vec!["data".into()]),
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    let records = vec![
        json_record(r#"{"success":true,"op":"subscribe"}"#), // ack — no `data`
        json_record(r#"{"data":{"id":42}}"#),
    ];
    let batch = decoder.decode_batch(&records).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        42
    );
}

// ── json.column.* tests ─────────────────────────────────────

#[test]
fn test_json_column_custom_path() {
    let schema = make_schema(vec![
        ("p", DataType::Float64, false),
        ("stream_name", DataType::Utf8, true),
    ]);
    let mut col_paths = HashMap::new();
    col_paths.insert("stream_name".into(), vec!["stream".into()]);
    let config = JsonDecoderConfig {
        json_path: Some(vec!["data".into()]),
        json_column_paths: col_paths,
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    let records = vec![json_record(
        r#"{"stream":"btcusdt@trade","data":{"p":67523.0}}"#,
    )];
    let batch = decoder.decode_batch(&records).unwrap();
    assert_eq!(batch.num_rows(), 1);
    let price = batch
        .column(0)
        .as_primitive::<arrow_array::types::Float64Type>()
        .value(0);
    assert!((price - 67523.0).abs() < f64::EPSILON);
    assert_eq!(batch.column(1).as_string::<i32>().value(0), "btcusdt@trade");
}

#[test]
fn test_json_column_deep_path() {
    let schema = make_schema(vec![
        ("p", DataType::Float64, false),
        ("ts", DataType::Int64, true),
    ]);
    let mut col_paths = HashMap::new();
    col_paths.insert("ts".into(), vec!["meta".into(), "timestamp".into()]);
    let config = JsonDecoderConfig {
        json_path: Some(vec!["data".into()]),
        json_column_paths: col_paths,
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    let records = vec![json_record(
        r#"{"meta":{"timestamp":123456},"data":{"p":99.5}}"#,
    )];
    let batch = decoder.decode_batch(&records).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        123456
    );
}

#[test]
fn test_json_column_missing_returns_null() {
    let schema = make_schema(vec![
        ("id", DataType::Int64, false),
        ("missing_col", DataType::Utf8, true),
    ]);
    let mut col_paths = HashMap::new();
    col_paths.insert("missing_col".into(), vec!["nowhere".into(), "gone".into()]);
    let config = JsonDecoderConfig {
        json_column_paths: col_paths,
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    let records = vec![json_record(r#"{"id":42}"#)];
    let batch = decoder.decode_batch(&records).unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        42
    );
    assert!(batch.column(1).is_null(0));
}

#[test]
fn test_json_column_path_to_nested_string_array() {
    use arrow_array::Array;
    use arrow_schema::Field;
    let schema = make_schema(vec![(
        "tags",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    )]);
    let mut col_paths = HashMap::new();
    col_paths.insert("tags".into(), vec!["record".into(), "tags".into()]);
    let config = JsonDecoderConfig {
        json_column_paths: col_paths,
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    let records = vec![json_record(r#"{"record":{"tags":["en","es"]}}"#)];
    let batch = decoder.decode_batch(&records).unwrap();

    let list = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .expect("List column");
    let vals = list.value(0);
    let strs = vals.as_string::<i32>();
    assert_eq!(strs.len(), 2);
    assert_eq!(strs.value(0), "en");
    assert_eq!(strs.value(1), "es");
}

#[test]
fn test_list_column_non_array_honors_reject_strategy() {
    use arrow_schema::Field;
    let schema = make_schema(vec![(
        "tags",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    )]);
    let config = JsonDecoderConfig {
        type_mismatch: TypeMismatchStrategy::Reject,
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    // A scalar where the list column expects an array must be rejected, not
    // silently coerced to NULL.
    let records = vec![json_record(r#"{"tags": "en"}"#)];
    let result = decoder.decode_batch(&records);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("type mismatch"));
}

// ── json.explode tests ──────────────────────────────────────

#[test]
fn test_json_explode_arrays() {
    let schema = make_schema(vec![
        ("price", DataType::Utf8, true),
        ("qty", DataType::Utf8, true),
    ]);
    let config = JsonDecoderConfig {
        json_explode: Some(vec!["price".into(), "qty".into()]),
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    let records = vec![json_record(r#"[["67523","1.5"],["67522","0.8"]]"#)];
    let batch = decoder.decode_batch(&records).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.column(0).as_string::<i32>().value(0), "67523");
    assert_eq!(batch.column(0).as_string::<i32>().value(1), "67522");
    assert_eq!(batch.column(1).as_string::<i32>().value(0), "1.5");
    assert_eq!(batch.column(1).as_string::<i32>().value(1), "0.8");
}

#[test]
fn test_json_explode_objects() {
    let schema = make_schema(vec![
        ("id", DataType::Int64, true),
        ("name", DataType::Utf8, true),
    ]);
    let config = JsonDecoderConfig {
        json_explode: Some(vec!["id".into(), "name".into()]),
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    let records = vec![json_record(
        r#"[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]"#,
    )];
    let batch = decoder.decode_batch(&records).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        1
    );
    assert_eq!(batch.column(1).as_string::<i32>().value(1), "Bob");
}

// ── Combined json.path + json.explode ───────────────────────

#[test]
fn test_json_path_plus_explode() {
    let schema = make_schema(vec![
        ("price", DataType::Utf8, true),
        ("qty", DataType::Utf8, true),
    ]);
    let config = JsonDecoderConfig {
        json_path: Some(vec!["bids".into()]),
        json_explode: Some(vec!["price".into(), "qty".into()]),
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    let records = vec![json_record(r#"{"bids":[["67523","1.5"],["67522","0.8"]]}"#)];
    let batch = decoder.decode_batch(&records).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.column(0).as_string::<i32>().value(0), "67523");
    assert_eq!(batch.column(1).as_string::<i32>().value(1), "0.8");
}

// ── from_connector_config tests ─────────────────────────────

#[test]
fn test_from_connector_config() {
    let mut config = crate::config::ConnectorConfig::new("json");
    config.set("json.path", "data.trade");
    config.set("json.column.stream_name", "stream");
    config.set("json.column.ts", "meta.timestamp");
    config.set("json.explode", "price, qty");
    config.set("schema.enforcement", "strict");
    config.set("nested.as.jsonb", "true");

    let schema = make_schema(vec![
        ("stream_name", DataType::Utf8, true),
        ("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ("price", DataType::Utf8, true),
        ("qty", DataType::Utf8, true),
    ]);
    let cfg = JsonDecoderConfig::from_connector_config(&config, &schema).unwrap();
    assert_eq!(cfg.json_path, Some(vec!["data".into(), "trade".into()]));
    assert_eq!(
        cfg.json_column_paths.get("stream_name"),
        Some(&vec!["stream".into()])
    );
    assert_eq!(
        cfg.json_column_paths.get("ts"),
        Some(&vec!["meta".into(), "timestamp".into()])
    );
    assert_eq!(cfg.json_explode, Some(vec!["price".into(), "qty".into()]));
    assert_eq!(cfg.type_mismatch, TypeMismatchStrategy::Reject);
    assert!(cfg.nested_as_jsonb);
}

#[test]
fn connector_config_rejects_invalid_projection_options() {
    let schema = make_schema(vec![
        ("id", DataType::Int64, false),
        ("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]);
    for (key, value) in [
        ("json.path", "data..event"),
        ("json.column.missing", "payload.value"),
        ("json.column.id", "payload..value"),
        ("json.explode", "id,missing"),
        ("json.explode", "id,id"),
        ("schema.enforcement", "strcit"),
        ("nested.as.jsonb", "yes"),
    ] {
        let mut config = crate::config::ConnectorConfig::new("json");
        config.set(key, value);
        let error = JsonDecoderConfig::from_connector_config(&config, &schema)
            .unwrap_err()
            .to_string();
        assert!(error.contains(key), "{key}={value}: {error}");
    }
}

#[test]
fn test_default_config_unchanged() {
    let schema = make_schema(vec![
        ("a", DataType::Int64, false),
        ("b", DataType::Utf8, true),
    ]);
    let decoder = JsonDecoder::new(schema);
    let records = vec![
        json_record(r#"{"a": 1, "b": "hello"}"#),
        json_record(r#"{"a": 2, "b": "world"}"#),
    ];
    let batch = decoder.decode_batch(&records).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        1
    );
    assert_eq!(batch.column(1).as_string::<i32>().value(1), "world");
}

#[test]
fn test_unknown_fields_with_path() {
    let schema = make_schema(vec![("a", DataType::Int64, false)]);
    let config = JsonDecoderConfig {
        json_path: Some(vec!["data".into()]),
        unknown_fields: UnknownFieldStrategy::CollectExtra,
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    let records = vec![json_record(r#"{"data":{"a":1,"extra":"value"}}"#)];
    let batch = decoder.decode_batch(&records).unwrap();
    assert_eq!(batch.num_columns(), 2); // a + _extra
    assert_eq!(batch.schema().field(1).name(), "_extra");
    assert!(!batch.column(1).is_null(0));
}

// ── Per-column numeric epoch unit ─────────────────────────

#[test]
fn from_connector_config_parses_epoch_unit_without_polluting_paths() {
    use crate::config::ConnectorConfig;
    let mut props = std::collections::HashMap::new();
    props.insert("json.column.evt".to_string(), "time_us".to_string());
    props.insert(
        "json.column.evt.epoch_unit".to_string(),
        "micros".to_string(),
    );
    let cc = ConnectorConfig::with_properties("json", props);
    let schema = make_schema(vec![(
        "evt",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    )]);
    let cfg = JsonDecoderConfig::from_connector_config(&cc, &schema).unwrap();
    assert_eq!(
        cfg.numeric_timestamp_units.get("evt"),
        Some(&EpochUnit::Micros)
    );
    // The `.epoch_unit` key must NOT become a phantom path column.
    assert!(cfg.json_column_paths.contains_key("evt"));
    assert!(!cfg.json_column_paths.contains_key("evt.epoch_unit"));
}

#[test]
fn from_connector_config_rejects_noncanonical_epoch_units() {
    for value in ["epoch_micros", "minutes"] {
        let mut config = crate::config::ConnectorConfig::new("source");
        config.set("json.column.evt.epoch_unit", value);
        let schema = make_schema(vec![(
            "evt",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]);
        let error = JsonDecoderConfig::from_connector_config(&config, &schema)
            .unwrap_err()
            .to_string();
        assert!(error.contains("json.column.evt.epoch_unit"), "{error}");
        assert!(error.contains(value), "{error}");
    }
}

#[test]
fn from_connector_config_rejects_unknown_or_non_timestamp_epoch_columns() {
    let schema = make_schema(vec![
        (
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        ("id", DataType::Int64, false),
    ]);

    for (column, expected) in [
        ("missing", "not present in the source schema"),
        ("id", "must be a Timestamp"),
    ] {
        let mut config = crate::config::ConnectorConfig::new("source");
        config.set(format!("json.column.{column}.epoch_unit"), "micros");
        let error = JsonDecoderConfig::from_connector_config(&config, &schema)
            .unwrap_err()
            .to_string();
        assert!(error.contains(column), "{error}");
        assert!(error.contains(expected), "{error}");
    }
}

#[test]
fn integer_epoch_conversion_covers_every_source_and_arrow_unit() {
    let source_values = [
        (EpochUnit::Seconds, 2),
        (EpochUnit::Millis, 2_000),
        (EpochUnit::Micros, 2_000_000),
        (EpochUnit::Nanos, 2_000_000_000),
    ];
    let target_values = [
        (TimeUnit::Second, 2),
        (TimeUnit::Millisecond, 2_000),
        (TimeUnit::Microsecond, 2_000_000),
        (TimeUnit::Nanosecond, 2_000_000_000),
    ];

    for (from, value) in source_values {
        for (to, expected) in &target_values {
            assert_eq!(
                checked_epoch_to_unit(value, from, *to).unwrap(),
                *expected,
                "from={from:?}, to={to:?}"
            );
        }
    }
}

#[test]
fn integer_epoch_downscale_matches_arrow_for_negative_values() {
    assert_eq!(
        checked_epoch_to_unit(-1_500, EpochUnit::Nanos, TimeUnit::Microsecond).unwrap(),
        -1
    );
    assert_eq!(
        checked_epoch_to_unit(-1_500, EpochUnit::Micros, TimeUnit::Millisecond).unwrap(),
        -1
    );
    assert_eq!(
        checked_epoch_to_unit(-2, EpochUnit::Seconds, TimeUnit::Nanosecond).unwrap(),
        -2_000_000_000
    );
}

#[test]
fn integer_epoch_upscale_rejects_positive_and_negative_overflow() {
    assert!(checked_epoch_to_unit(i64::MAX, EpochUnit::Seconds, TimeUnit::Nanosecond).is_err());
    assert!(checked_epoch_to_unit(i64::MIN, EpochUnit::Millis, TimeUnit::Microsecond).is_err());
}

#[test]
fn fractional_numeric_epoch_is_rejected_instead_of_rounded() {
    let schema = make_schema(vec![(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )]);
    let decoder = JsonDecoder::new(schema);
    let error = decoder
        .decode_batch(&[json_record(r#"{"ts": 1.5}"#)])
        .unwrap_err()
        .to_string();
    assert!(error.contains("fractional timestamp"), "{error}");
}

#[test]
fn integral_float_epoch_requires_exact_integer_range() {
    let schema = make_schema(vec![(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )]);
    let decoder = JsonDecoder::new(schema);

    let batch = decoder
        .decode_batch(&[json_record(r#"{"ts": 1000.0}"#)])
        .unwrap();
    let timestamps = batch
        .column(0)
        .as_primitive::<arrow_array::types::TimestampMillisecondType>();
    assert_eq!(timestamps.value(0), 1_000);

    let error = decoder
        .decode_batch(&[json_record(r#"{"ts": 9.007199254740992e15}"#)])
        .unwrap_err()
        .to_string();
    assert!(error.contains("not exactly representable"), "{error}");
}

#[test]
fn numeric_micros_decodes_into_millis_timestamp_column() {
    // End-to-end: a numeric microsecond field (Jetstream time_us shape)
    // mapped to a Timestamp(ms) column lands at a wall-clock-plausible
    // millisecond value instead of being misread as epoch-millis.
    let schema = make_schema(vec![(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )]);
    let mut units = HashMap::new();
    units.insert("ts".to_string(), EpochUnit::Micros);
    let config = JsonDecoderConfig {
        numeric_timestamp_units: units,
        ..Default::default()
    };
    let decoder = JsonDecoder::with_config(schema, config);
    // 1_779_019_200_123_456 µs == 1_779_019_200_123 ms (~2026-05-17).
    let records = vec![json_record(r#"{"ts": 1779019200123456}"#)];
    let batch = decoder.decode_batch(&records).unwrap();
    let col = batch
        .column(0)
        .as_primitive::<arrow_array::types::TimestampMillisecondType>();
    assert_eq!(col.value(0), 1_779_019_200_123);

    // Without an override, numeric epochs use milliseconds.
    let schema2 = make_schema(vec![(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )]);
    let d2 = JsonDecoder::new(schema2);
    let b2 = d2.decode_batch(&records).unwrap();
    let c2 = b2
        .column(0)
        .as_primitive::<arrow_array::types::TimestampMillisecondType>();
    assert_eq!(c2.value(0), 1_779_019_200_123_456);
}
