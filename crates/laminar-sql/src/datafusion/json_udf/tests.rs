use std::sync::Arc;

use super::*;
use arrow::datatypes::DataType;
use arrow_array::{Array, ArrayRef, BooleanArray, LargeBinaryArray, StringArray};
use arrow_schema::Field;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl};

fn enc(v: &serde_json::Value) -> Vec<u8> {
    json_types::encode_jsonb(v)
}

fn make_jsonb_array(vals: &[serde_json::Value]) -> LargeBinaryArray {
    let encoded: Vec<Vec<u8>> = vals.iter().map(enc).collect();
    let refs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
    LargeBinaryArray::from_iter_values(refs)
}

fn make_string_array(vals: &[&str]) -> StringArray {
    StringArray::from(vals.to_vec())
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
        return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
        config_options: Arc::new(ConfigOptions::default()),
    }
}

// ── jsonb_get tests ──────────────────────────────────────

#[test]
fn test_jsonb_get_object_field() {
    let udf = JsonbGet::new();
    let jsonb = make_jsonb_array(&[serde_json::json!({"name": "Alice", "age": 30})]);
    let keys = make_string_array(&["name"]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(keys)))
        .unwrap();

    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    assert!(!bin.is_null(0));
    let val = bin.value(0);
    assert_eq!(json_types::jsonb_to_text(val), Some("Alice".to_owned()));
}

#[test]
fn test_jsonb_get_missing_key() {
    let udf = JsonbGet::new();
    let jsonb = make_jsonb_array(&[serde_json::json!({"a": 1})]);
    let keys = make_string_array(&["missing"]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(keys)))
        .unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    assert!(bin.is_null(0));
}

// ── jsonb_get_idx tests ──────────────────────────────────

#[test]
fn test_jsonb_get_idx() {
    let udf = JsonbGetIdx::new();
    let jsonb = make_jsonb_array(&[serde_json::json!([10, 20, 30])]);
    let idxs = arrow_array::Int32Array::from(vec![1]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(idxs)))
        .unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    let val = bin.value(0);
    assert_eq!(json_types::jsonb_to_text(val), Some("20".to_owned()));
}

#[test]
fn test_jsonb_get_idx_out_of_bounds() {
    let udf = JsonbGetIdx::new();
    let jsonb = make_jsonb_array(&[serde_json::json!([1, 2, 3])]);
    let idxs = arrow_array::Int32Array::from(vec![10]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(idxs)))
        .unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    assert!(bin.is_null(0));
}

// ── jsonb_get_text tests ─────────────────────────────────

#[test]
fn test_jsonb_get_text_string() {
    let udf = JsonbGetText::new();
    let jsonb = make_jsonb_array(&[serde_json::json!({"name": "Alice"})]);
    let keys = make_string_array(&["name"]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(keys)))
        .unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(str_arr.value(0), "Alice");
}

#[test]
fn test_jsonb_get_text_number() {
    let udf = JsonbGetText::new();
    let jsonb = make_jsonb_array(&[serde_json::json!({"age": 30})]);
    let keys = make_string_array(&["age"]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(keys)))
        .unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(str_arr.value(0), "30");
}

// ── jsonb_get_text_idx tests ─────────────────────────────

#[test]
fn test_jsonb_get_text_idx() {
    let udf = JsonbGetTextIdx::new();
    let jsonb = make_jsonb_array(&[serde_json::json!([10, 20, 30])]);
    let idxs = arrow_array::Int32Array::from(vec![2]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(idxs)))
        .unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(str_arr.value(0), "30");
}

// ── jsonb_exists tests ───────────────────────────────────

#[test]
fn test_jsonb_exists_true() {
    let udf = JsonbExists::new();
    let jsonb = make_jsonb_array(&[serde_json::json!({"name": "Alice", "age": 30})]);
    let keys = make_string_array(&["name"]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(keys)))
        .unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(bool_arr.value(0));
}

#[test]
fn test_jsonb_exists_false() {
    let udf = JsonbExists::new();
    let jsonb = make_jsonb_array(&[serde_json::json!({"name": "Alice"})]);
    let keys = make_string_array(&["missing"]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(keys)))
        .unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(!bool_arr.value(0));
}

// ── jsonb_contains tests ─────────────────────────────────

#[test]
fn test_jsonb_contains_true() {
    let udf = JsonbContains::new();
    let left = make_jsonb_array(&[serde_json::json!({"a": 1, "b": 2, "c": 3})]);
    let right = make_jsonb_array(&[serde_json::json!({"a": 1, "c": 3})]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(left), Arc::new(right)))
        .unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(bool_arr.value(0));
}

#[test]
fn test_jsonb_contains_false() {
    let udf = JsonbContains::new();
    let left = make_jsonb_array(&[serde_json::json!({"a": 1})]);
    let right = make_jsonb_array(&[serde_json::json!({"a": 1, "b": 2})]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(left), Arc::new(right)))
        .unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(!bool_arr.value(0));
}

// ── jsonb_contained_by tests ─────────────────────────────

#[test]
fn test_jsonb_contained_by() {
    let udf = JsonbContainedBy::new();
    let left = make_jsonb_array(&[serde_json::json!({"a": 1})]);
    let right = make_jsonb_array(&[serde_json::json!({"a": 1, "b": 2})]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(left), Arc::new(right)))
        .unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(bool_arr.value(0));
}

// ── json_typeof tests ────────────────────────────────────

#[test]
fn test_json_typeof_all_types() {
    let udf = JsonTypeof::new();
    let jsonb = make_jsonb_array(&[
        serde_json::json!({"a": 1}),
        serde_json::json!([1, 2]),
        serde_json::json!("hello"),
        serde_json::json!(42),
        serde_json::json!(true),
        serde_json::json!(null),
    ]);
    let result = udf.invoke_with_args(make_args_1(Arc::new(jsonb))).unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(str_arr.value(0), "object");
    assert_eq!(str_arr.value(1), "array");
    assert_eq!(str_arr.value(2), "string");
    assert_eq!(str_arr.value(3), "number");
    assert_eq!(str_arr.value(4), "boolean");
    assert_eq!(str_arr.value(5), "null");
}

// ── json_build_object tests ──────────────────────────────

#[test]
fn test_json_build_object() {
    let udf = JsonBuildObject::new();
    let keys = Arc::new(make_string_array(&["name"])) as ArrayRef;
    let vals = Arc::new(make_string_array(&["Alice"])) as ArrayRef;
    let args = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(keys), ColumnarValue::Array(vals)],
        arg_fields: vec![],
        number_rows: 0,
        return_field: Arc::new(Field::new("output", DataType::LargeBinary, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result = udf.invoke_with_args(args).unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    let val = bin.value(0);
    // Verify we can read back the field
    let name = json_types::jsonb_get_field(val, "name").unwrap();
    assert_eq!(json_types::jsonb_to_text(name), Some("Alice".to_owned()));
}

#[test]
fn test_json_build_object_odd_args() {
    let udf = JsonBuildObject::new();
    let a = Arc::new(make_string_array(&["key"])) as ArrayRef;
    let args = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(a)],
        arg_fields: vec![],
        number_rows: 0,
        return_field: Arc::new(Field::new("output", DataType::LargeBinary, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    assert!(udf.invoke_with_args(args).is_err());
}

// ── json_build_array tests ───────────────────────────────

#[test]
fn test_json_build_array() {
    let udf = JsonBuildArray::new();
    let a = Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef;
    let b = Arc::new(make_string_array(&["two"])) as ArrayRef;
    let args = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(a), ColumnarValue::Array(b)],
        arg_fields: vec![],
        number_rows: 0,
        return_field: Arc::new(Field::new("output", DataType::LargeBinary, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result = udf.invoke_with_args(args).unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    let val = bin.value(0);
    assert_eq!(json_types::jsonb_type_name(val), Some("array"));
    let elem0 = json_types::jsonb_array_get(val, 0).unwrap();
    assert_eq!(json_types::jsonb_to_text(elem0), Some("1".to_owned()));
    let elem1 = json_types::jsonb_array_get(val, 1).unwrap();
    assert_eq!(json_types::jsonb_to_text(elem1), Some("two".to_owned()));
}

// ── to_jsonb tests ───────────────────────────────────────

#[test]
fn test_to_jsonb_int() {
    let udf = ToJsonb::new();
    let a = Arc::new(arrow_array::Int64Array::from(vec![42])) as ArrayRef;
    let result = udf.invoke_with_args(make_args_1(a)).unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    let val = bin.value(0);
    assert_eq!(json_types::jsonb_to_text(val), Some("42".to_owned()));
}

#[test]
fn test_to_jsonb_string() {
    let udf = ToJsonb::new();
    let a = Arc::new(make_string_array(&["hello"])) as ArrayRef;
    let result = udf.invoke_with_args(make_args_1(a)).unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    let val = bin.value(0);
    assert_eq!(json_types::jsonb_to_text(val), Some("hello".to_owned()));
}

// ── Registration tests ───────────────────────────────────

#[test]
fn test_all_udfs_register() {
    let names = [
        ScalarUDF::new_from_impl(JsonbGet::new()).name().to_owned(),
        ScalarUDF::new_from_impl(JsonbGetIdx::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(JsonbGetText::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(JsonbGetTextIdx::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(JsonbExists::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(JsonbContains::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(JsonbContainedBy::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(JsonTypeof::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(JsonBuildObject::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(JsonBuildArray::new())
            .name()
            .to_owned(),
        ScalarUDF::new_from_impl(ToJsonb::new()).name().to_owned(),
    ];
    for name in &names {
        assert!(!name.is_empty(), "UDF has empty name");
    }
    assert_eq!(names.len(), 11);
}

// ── Nested extraction tests ──────────────────────────────

#[test]
fn test_nested_extraction() {
    // payload -> 'user' -> 'address' ->> 'city'
    let data = serde_json::json!({
        "user": {"address": {"city": "London"}}
    });
    let jsonb_bytes = enc(&data);

    // First: get 'user'
    let user = json_types::jsonb_get_field(&jsonb_bytes, "user").unwrap();
    // Then: get 'address'
    let addr = json_types::jsonb_get_field(user, "address").unwrap();
    // Then: get_text 'city'
    let city = json_types::jsonb_to_text(json_types::jsonb_get_field(addr, "city").unwrap());
    assert_eq!(city, Some("London".to_owned()));
}

// ── Multiple rows tests ──────────────────────────────────

#[test]
fn test_jsonb_get_multiple_rows() {
    let udf = JsonbGet::new();
    let jsonb = make_jsonb_array(&[
        serde_json::json!({"name": "Alice"}),
        serde_json::json!({"name": "Bob"}),
        serde_json::json!({"age": 30}),
    ]);
    let keys = make_string_array(&["name", "name", "name"]);
    let result = udf
        .invoke_with_args(make_args_2(Arc::new(jsonb), Arc::new(keys)))
        .unwrap();
    let ColumnarValue::Array(arr) = result else {
        panic!("expected array")
    };
    let bin = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    assert!(!bin.is_null(0)); // Alice
    assert!(!bin.is_null(1)); // Bob
    assert!(bin.is_null(2)); // no "name" field
}
