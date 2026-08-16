use std::sync::Arc;

use super::*;
use crate::datafusion::json_types;
use arrow::datatypes::DataType;
use arrow_array::builder::{MapBuilder, StringBuilder as MapSB};
use arrow_array::{Array, ArrayRef, LargeBinaryArray, ListArray, StringArray};
use arrow_schema::Field;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::json;

fn enc(v: &serde_json::Value) -> Vec<u8> {
    json_types::encode_jsonb(v)
}

fn make_jsonb_array(vals: &[serde_json::Value]) -> LargeBinaryArray {
    let encoded: Vec<Vec<u8>> = vals.iter().map(enc).collect();
    let refs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
    LargeBinaryArray::from_iter_values(refs)
}

fn make_args_2(a: ArrayRef, b: ArrayRef) -> ScalarFunctionArgs {
    ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(a), ColumnarValue::Array(b)],
        arg_fields: vec![],
        number_rows: 0,
        return_field: Arc::new(Field::new("output", DataType::LargeBinary, true)),
        config_options: Arc::new(ConfigOptions::default()),
    }
}

fn make_args_1(a: ArrayRef) -> ScalarFunctionArgs {
    ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(a)],
        arg_fields: vec![],
        number_rows: 0,
        return_field: Arc::new(Field::new("output", DataType::LargeBinary, true)),
        config_options: Arc::new(ConfigOptions::default()),
    }
}

fn decode_jsonb_result(result: ColumnarValue, row: usize) -> serde_json::Value {
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    assert!(!bin.is_null(row), "unexpected null at row {row}");
    json_types::jsonb_to_value(bin.value(row)).expect("invalid jsonb")
}

fn decode_text_result(result: ColumnarValue, row: usize) -> String {
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
    str_arr.value(row).to_owned()
}

// ── jsonb_merge tests ─────────────────────────────────────

#[test]
fn test_json_ext_merge_objects() {
    let udf = JsonbMerge::new();
    let left = make_jsonb_array(&[json!({"a": 1, "b": 2})]);
    let right = make_jsonb_array(&[json!({"b": 99, "c": 3})]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(left), Arc::new(right)))
        .unwrap();
    assert_eq!(
        decode_jsonb_result(result, 0),
        json!({"a": 1, "b": 99, "c": 3})
    );
}

#[test]
fn test_json_ext_merge_non_object() {
    let udf = JsonbMerge::new();
    let left = make_jsonb_array(&[json!(42)]);
    let right = make_jsonb_array(&[json!("hello")]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(left), Arc::new(right)))
        .unwrap();
    assert_eq!(decode_jsonb_result(result, 0), json!("hello"));
}

// ── jsonb_deep_merge tests ────────────────────────────────

#[test]
fn test_json_ext_deep_merge() {
    let udf = JsonbDeepMerge::new();
    let left = make_jsonb_array(&[json!({"a": {"x": 1, "y": 2}, "b": 10})]);
    let right = make_jsonb_array(&[json!({"a": {"y": 99, "z": 3}, "c": 20})]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(left), Arc::new(right)))
        .unwrap();
    assert_eq!(
        decode_jsonb_result(result, 0),
        json!({"a": {"x": 1, "y": 99, "z": 3}, "b": 10, "c": 20})
    );
}

#[test]
fn test_json_ext_deep_merge_non_object_override() {
    let udf = JsonbDeepMerge::new();
    let left = make_jsonb_array(&[json!({"a": {"x": 1}})]);
    let right = make_jsonb_array(&[json!({"a": 42})]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(left), Arc::new(right)))
        .unwrap();
    assert_eq!(decode_jsonb_result(result, 0), json!({"a": 42}));
}

// ── jsonb_strip_nulls tests ───────────────────────────────

#[test]
fn test_json_ext_strip_nulls() {
    let udf = JsonbStripNulls::new();
    let input = make_jsonb_array(&[json!({"a": 1, "b": null, "c": {"d": null, "e": 2}})]);
    let result = udf.invoke_with_args(make_args_1(Arc::new(input))).unwrap();
    assert_eq!(
        decode_jsonb_result(result, 0),
        json!({"a": 1, "c": {"e": 2}})
    );
}

#[test]
fn test_json_ext_strip_nulls_array_preserved() {
    let udf = JsonbStripNulls::new();
    let input = make_jsonb_array(&[json!({"arr": [1, null, 3]})]);
    let result = udf.invoke_with_args(make_args_1(Arc::new(input))).unwrap();
    assert_eq!(decode_jsonb_result(result, 0), json!({"arr": [1, null, 3]}));
}

// ── jsonb_rename_keys tests ───────────────────────────────

#[test]
fn test_json_ext_rename_keys() {
    let udf = JsonbRenameKeys::new();
    let input = make_jsonb_array(&[json!({"old_name": 1, "keep": 2})]);

    // Build a MapArray with one row: {"old_name": "new_name"}
    let key_builder = MapSB::new();
    let val_builder = MapSB::new();
    let mut map_builder = MapBuilder::new(None, key_builder, val_builder);
    map_builder.keys().append_value("old_name");
    map_builder.values().append_value("new_name");
    map_builder.append(true).unwrap();
    let map_arr = map_builder.finish();

    let result = udf
        .invoke_with_args(make_args_2(Arc::new(input), Arc::new(map_arr)))
        .unwrap();
    assert_eq!(
        decode_jsonb_result(result, 0),
        json!({"keep": 2, "new_name": 1})
    );
}

// ── jsonb_pick tests ──────────────────────────────────────

#[test]
fn test_json_ext_pick() {
    let udf = JsonbPick::new();
    let input = make_jsonb_array(&[json!({"a": 1, "b": 2, "c": 3})]);
    let keys = make_string_list(&[&["a", "c"]]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(input), Arc::new(keys)))
        .unwrap();
    assert_eq!(decode_jsonb_result(result, 0), json!({"a": 1, "c": 3}));
}

// ── jsonb_except tests ────────────────────────────────────

#[test]
fn test_json_ext_except() {
    let udf = JsonbExcept::new();
    let input = make_jsonb_array(&[json!({"a": 1, "b": 2, "c": 3})]);
    let keys = make_string_list(&[&["b"]]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(input), Arc::new(keys)))
        .unwrap();
    assert_eq!(decode_jsonb_result(result, 0), json!({"a": 1, "c": 3}));
}

// ── jsonb_flatten tests ───────────────────────────────────

#[test]
fn test_json_ext_flatten() {
    let udf = JsonbFlatten::new();
    let input = make_jsonb_array(&[json!({"a": {"b": 1, "c": [2, 3]}})]);
    let sep = StringArray::from(vec!["."]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(input), Arc::new(sep)))
        .unwrap();
    assert_eq!(
        decode_jsonb_result(result, 0),
        json!({"a.b": 1, "a.c.0": 2, "a.c.1": 3})
    );
}

#[test]
fn test_json_ext_flatten_custom_sep() {
    let udf = JsonbFlatten::new();
    let input = make_jsonb_array(&[json!({"x": {"y": 42}})]);
    let sep = StringArray::from(vec!["/"]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(input), Arc::new(sep)))
        .unwrap();
    assert_eq!(decode_jsonb_result(result, 0), json!({"x/y": 42}));
}

// ── jsonb_unflatten tests ─────────────────────────────────

#[test]
fn test_json_ext_unflatten() {
    let udf = JsonbUnflatten::new();
    let input = make_jsonb_array(&[json!({"a.b": 1, "a.c": 2, "d": 3})]);
    let sep = StringArray::from(vec!["."]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(input), Arc::new(sep)))
        .unwrap();
    assert_eq!(
        decode_jsonb_result(result, 0),
        json!({"a": {"b": 1, "c": 2}, "d": 3})
    );
}

// ── json_to_columns tests ─────────────────────────────────

#[test]
fn test_json_ext_to_columns() {
    let udf = JsonToColumns::new();
    let input = make_jsonb_array(&[json!({"name": "Alice", "age": 30, "active": true})]);
    let spec = StringArray::from(vec!["name VARCHAR, age BIGINT"]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(input), Arc::new(spec)))
        .unwrap();
    assert_eq!(
        decode_jsonb_result(result, 0),
        json!({"age": 30, "name": "Alice"})
    );
}

// ── json_infer_schema tests ───────────────────────────────

#[test]
fn test_json_ext_infer_schema_object() {
    let udf = JsonInferSchema::new();
    let input = make_jsonb_array(&[json!({"name": "Alice", "age": 30, "active": true})]);
    let args = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(Arc::new(input))],
        arg_fields: vec![],
        number_rows: 0,
        return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result = udf.invoke_with_args(args).unwrap();
    let text = decode_text_result(result, 0);
    let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(parsed["name"], "VARCHAR");
    assert_eq!(parsed["age"], "BIGINT");
    assert_eq!(parsed["active"], "BOOLEAN");
}

#[test]
fn test_json_ext_infer_schema_nested() {
    let udf = JsonInferSchema::new();
    let input = make_jsonb_array(&[json!({"tags": [1, 2], "meta": {"x": 1.5}})]);
    let args = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(Arc::new(input))],
        arg_fields: vec![],
        number_rows: 0,
        return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result = udf.invoke_with_args(args).unwrap();
    let text = decode_text_result(result, 0);
    let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(parsed["tags"], "ARRAY<BIGINT>");
    assert_eq!(parsed["meta"], "STRUCT(x DOUBLE)");
}

#[test]
fn test_json_ext_infer_schema_scalar() {
    let udf = JsonInferSchema::new();
    let input = make_jsonb_array(&[json!(42)]);
    let args = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(Arc::new(input))],
        arg_fields: vec![],
        number_rows: 0,
        return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result = udf.invoke_with_args(args).unwrap();
    assert_eq!(decode_text_result(result, 0), "BIGINT");
}

// ── Registration test ─────────────────────────────────────

#[test]
fn test_json_ext_all_udfs_register() {
    use datafusion_expr::ScalarUDF;

    let names: Vec<String> = vec![
        ScalarUDF::new_from_impl(JsonbMerge::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(JsonbDeepMerge::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(JsonbStripNulls::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(JsonbRenameKeys::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(JsonbPick::new()).name().to_owned(),
        ScalarUDF::new_from_impl(JsonbExcept::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(JsonbFlatten::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(JsonbUnflatten::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(JsonToColumns::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(JsonInferSchema::new())
            .name()
            .to_owned(),
    ];
    for name in &names {
        assert!(!name.is_empty(), "UDF has empty name");
    }
    assert_eq!(names.len(), 10);
}

// ── Null-handling tests ───────────────────────────────────

#[test]
fn test_json_ext_merge_null_input() {
    let udf = JsonbMerge::new();
    let left = LargeBinaryArray::new_null(1);
    let right = make_jsonb_array(&[json!({"a": 1})]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(left), Arc::new(right)))
        .unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    assert!(bin.is_null(0));
}

// ── Helper: build ListArray<Utf8> ─────────────────────────

fn make_string_list(rows: &[&[&str]]) -> ListArray {
    use arrow_array::builder::{ListBuilder, StringBuilder};

    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in rows {
        for &s in *row {
            builder.values().append_value(s);
        }
        builder.append(true);
    }
    builder.finish()
}
