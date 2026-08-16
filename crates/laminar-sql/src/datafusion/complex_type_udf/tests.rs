use super::*;
use crate::datafusion::create_session_context;
use arrow_array::builder::*;
use arrow_array::*;
use arrow_schema::{DataType, Field, Fields};
use datafusion_common::config::ConfigOptions;

// ── Tier 1: Verify DataFusion built-in array functions ──────

#[tokio::test]
async fn test_builtin_array_length() {
    let ctx = create_session_context();
    let df = ctx
        .sql("SELECT array_length(make_array(1, 2, 3))")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn test_builtin_array_sort() {
    let ctx = create_session_context();
    let df = ctx
        .sql("SELECT array_sort(make_array(3, 1, 2))")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn test_builtin_array_distinct() {
    let ctx = create_session_context();
    let df = ctx
        .sql("SELECT array_distinct(make_array(1, 2, 2, 3))")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

// ── Tier 2: struct_extract ──────────────────────────────────

#[test]
fn test_struct_extract() {
    let fields = Fields::from(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
    ]);
    let struct_arr = StructArray::try_new(
        fields,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
        ],
        None,
    )
    .unwrap();

    let udf = StructExtract::new();
    let result = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(struct_arr)),
                ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some("b".into()))),
            ],
            number_rows: 0,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap();

    if let ColumnarValue::Array(arr) = result {
        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "x");
        assert_eq!(str_arr.value(1), "y");
        assert_eq!(str_arr.value(2), "z");
    } else {
        panic!("expected Array");
    }
}

// ── Tier 2: struct_drop ─────────────────────────────────────

#[test]
fn test_struct_drop() {
    let fields = Fields::from(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
    ]);
    let struct_arr = StructArray::try_new(
        fields,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
        None,
    )
    .unwrap();

    let udf = StructDrop::new();
    let result = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(struct_arr)),
                ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some("b".into()))),
            ],
            number_rows: 0,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap();

    if let ColumnarValue::Array(arr) = result {
        let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(s.num_columns(), 1);
        assert_eq!(s.fields()[0].name(), "a");
    } else {
        panic!("expected Array");
    }
}

// ── Tier 2: struct_rename ───────────────────────────────────

#[test]
fn test_struct_rename() {
    let fields = Fields::from(vec![Field::new("old_name", DataType::Int64, false)]);
    let struct_arr =
        StructArray::try_new(fields, vec![Arc::new(Int64Array::from(vec![42]))], None).unwrap();

    let udf = StructRename::new();
    let result = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(struct_arr)),
                ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some(
                    "old_name".into(),
                ))),
                ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some(
                    "new_name".into(),
                ))),
            ],
            number_rows: 0,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap();

    if let ColumnarValue::Array(arr) = result {
        let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(s.fields()[0].name(), "new_name");
    } else {
        panic!("expected Array");
    }
}

// ── Tier 2: struct_merge ────────────────────────────────────

#[test]
fn test_struct_merge() {
    let s1 = StructArray::try_new(
        Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["old"])),
        ],
        None,
    )
    .unwrap();

    let s2 = StructArray::try_new(
        Fields::from(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, false),
        ]),
        vec![
            Arc::new(StringArray::from(vec!["new"])),
            Arc::new(Float64Array::from(vec![3.125])),
        ],
        None,
    )
    .unwrap();

    let udf = StructMerge::new();
    let result = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(s1)),
                ColumnarValue::Array(Arc::new(s2)),
            ],
            number_rows: 0,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("output", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap();

    if let ColumnarValue::Array(arr) = result {
        let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
        // "a" from s1, "b" from s2 (override), "c" from s2
        assert_eq!(s.num_columns(), 3);
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    } else {
        panic!("expected Array");
    }
}

// ── Tier 2: map_contains_key ────────────────────────────────

#[test]
fn test_map_contains_key() {
    // Build a MapArray with one row: {"x": 1, "y": 2}
    let key_builder = StringBuilder::new();
    let val_builder = Int64Builder::new();
    let mut builder = MapBuilder::new(None, key_builder, val_builder);

    builder.keys().append_value("x");
    builder.values().append_value(1);
    builder.keys().append_value("y");
    builder.values().append_value(2);
    builder.append(true).unwrap();

    let map_arr = builder.finish();

    let udf = MapContainsKey::new();

    // Check for "x" — should be true.
    let result = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(map_arr.clone())),
                ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some("x".into()))),
            ],
            number_rows: 0,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("output", DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap();

    if let ColumnarValue::Array(arr) = result {
        let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_arr.value(0));
    }

    // Check for "z" — should be false.
    let result2 = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(map_arr)),
                ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some("z".into()))),
            ],
            number_rows: 0,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("output", DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap();

    if let ColumnarValue::Array(arr) = result2 {
        let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!bool_arr.value(0));
    }
}

// ── Registration ────────────────────────────────────────────

#[test]
fn test_register_complex_type_functions() {
    use datafusion::execution::FunctionRegistry;

    let ctx = create_session_context();
    register_complex_type_functions(&ctx);
    assert!(ctx.udf("struct_extract").is_ok());
    assert!(ctx.udf("struct_set").is_ok());
    assert!(ctx.udf("struct_drop").is_ok());
    assert!(ctx.udf("struct_rename").is_ok());
    assert!(ctx.udf("struct_merge").is_ok());
    assert!(ctx.udf("map_keys").is_ok());
    assert!(ctx.udf("map_values").is_ok());
    assert!(ctx.udf("map_contains_key").is_ok());
    assert!(ctx.udf("map_from_arrays").is_ok());
}
