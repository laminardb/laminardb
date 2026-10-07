use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, TimeUnit};

use super::*;

#[test]
fn preserves_nested_types_metadata_and_order() {
    let child = Field::new("amount", DataType::Decimal128(27, 9), true)
        .with_metadata(HashMap::from([("PARQUET:field_id".into(), "42".into())]));
    let schema = Schema::new_with_metadata(
        vec![
            Field::new("nested", DataType::Struct(vec![child].into()), true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("Europe/London".into())),
                false,
            ),
            Field::new(
                "list",
                DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ],
        HashMap::from([("z".into(), "last".into()), ("a".into(), "first".into())]),
    );
    let binding = SchemaBinding::logical(
        "test",
        SchemaDirection::Source,
        SchemaOrigin::Metadata,
        schema,
    )
    .unwrap();
    assert_eq!(
        SchemaBinding::decode(&binding.canonical_bytes().unwrap()).unwrap(),
        binding
    );
    let mut reversed = binding.clone();
    reversed.logical = reversed.logical.with_metadata(HashMap::from([
        ("a".into(), "first".into()),
        ("z".into(), "last".into()),
    ]));
    assert_eq!(
        binding.fingerprint().unwrap(),
        reversed.fingerprint().unwrap()
    );
}

#[test]
fn mapping_uses_names_and_rejects_directional_nullability() {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
    ]);
    let mut binding = SchemaBinding::logical(
        "test",
        SchemaDirection::Sink,
        SchemaOrigin::Query,
        schema.clone(),
    )
    .unwrap();
    binding
        .bind_external(Schema::new(vec![
            schema.field(1).clone(),
            schema.field(0).clone(),
        ]))
        .unwrap();
    assert_eq!(binding.mapping[0].external, "a");
    assert!(binding
        .bind_external(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false)
        ]))
        .is_err());
    assert!(binding
        .bind_external(Schema::new(vec![schema.field(0).clone()]))
        .is_err());
}

#[test]
fn sources_allow_explicit_projection_without_appending_fields() {
    let mut binding = SchemaBinding::logical(
        "test",
        SchemaDirection::Source,
        SchemaOrigin::Explicit,
        Schema::new(vec![Field::new("b", DataType::Utf8, true)]),
    )
    .unwrap();
    binding
        .bind_external(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]))
        .unwrap();
    assert_eq!(binding.logical.fields().len(), 1);
    assert_eq!(binding.logical.field(0).name(), "b");
}

#[test]
fn native_semantics_participate_in_fingerprint() {
    let mut binding = SchemaBinding::logical(
        "kafka",
        SchemaDirection::Source,
        SchemaOrigin::Metadata,
        Schema::new(vec![Field::new("a", DataType::Int64, true)]),
    )
    .unwrap();
    binding.value = Some(NativeSchema {
        format: "avro".into(),
        identity: BTreeMap::from([("id".into(), "1".into())]),
        definition: serde_json::json!({"type":"record", "name":"A", "fields":[{"name":"a", "type":["null", "long"], "default":null}]}),
        references: Vec::new(),
    });
    let original = binding.fingerprint().unwrap();
    binding.value.as_mut().unwrap().definition["name"] = "B".into();
    assert_ne!(original, binding.fingerprint().unwrap());
    binding.version = 99;
    assert!(binding.canonical_bytes().is_err());
}

#[test]
fn empty_or_unestablished_schemas_are_not_success() {
    assert!(SchemaBinding::logical(
        "test",
        SchemaDirection::Source,
        SchemaOrigin::Sample,
        Schema::empty()
    )
    .is_err());
    assert!(SchemaBinding::logical(
        "test",
        SchemaDirection::Source,
        SchemaOrigin::Sample,
        Schema::new(vec![Field::new("a", DataType::Null, true)])
    )
    .is_err());
}

#[test]
fn sink_control_fields_are_explicit_and_excluded_from_business_mapping() {
    let logical = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("__weight", DataType::Int64, false),
    ]);
    let mut binding = SchemaBinding::logical(
        "delta-lake",
        SchemaDirection::Sink,
        SchemaOrigin::Query,
        logical,
    )
    .unwrap();
    binding.control_fields = vec!["__weight".into()];
    binding
        .bind_external(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
        .unwrap();
    assert_eq!(binding.mapping.len(), 1);
    assert_eq!(
        SchemaBinding::decode(&binding.canonical_bytes().unwrap()).unwrap(),
        binding
    );
    binding.control_fields.push("id".into());
    assert!(binding.canonical_bytes().is_err());
}

#[test]
fn deep_and_duplicate_nested_schemas_fail_before_encoding() {
    let mut data_type = DataType::Utf8;
    for _ in 0..34 {
        data_type = DataType::List(Arc::new(Field::new("item", data_type, true)));
    }
    assert!(SchemaBinding::logical(
        "files",
        SchemaDirection::Source,
        SchemaOrigin::Metadata,
        Schema::new(vec![Field::new("a", data_type, true)])
    )
    .is_err());
    let repeated = DataType::Struct(
        vec![
            Field::new("same", DataType::Utf8, true),
            Field::new("same", DataType::Int64, true),
        ]
        .into(),
    );
    assert!(SchemaBinding::logical(
        "files",
        SchemaDirection::Source,
        SchemaOrigin::Metadata,
        Schema::new(vec![Field::new("a", repeated, true)])
    )
    .is_err());
}

#[test]
fn native_union_order_defaults_and_resource_identity_are_not_arrow_equality() {
    let mut binding = SchemaBinding::logical(
        "kafka",
        SchemaDirection::Source,
        SchemaOrigin::Metadata,
        Schema::new(vec![Field::new("a", DataType::Int64, true)]),
    )
    .unwrap();
    binding.value = Some(NativeSchema {
        format: "avro".into(),
        identity: BTreeMap::from([("id".into(), "1".into())]),
        definition: serde_json::json!({"type":["null", "long"], "default": null}),
        references: Vec::new(),
    });
    let fingerprint = binding.fingerprint().unwrap();
    binding.value.as_mut().unwrap().definition["type"] = serde_json::json!(["long", "null"]);
    assert_ne!(fingerprint, binding.fingerprint().unwrap());
    binding.value.as_mut().unwrap().definition["type"] = serde_json::json!(["null", "long"]);
    binding
        .value
        .as_mut()
        .unwrap()
        .identity
        .insert("id".into(), "2".into());
    assert_ne!(fingerprint, binding.fingerprint().unwrap());
}

#[test]
fn oversized_and_deep_native_content_is_bounded() {
    let mut binding = SchemaBinding::logical(
        "kafka",
        SchemaDirection::Source,
        SchemaOrigin::Metadata,
        Schema::new(vec![Field::new("a", DataType::Int64, false)]),
    )
    .unwrap();
    let mut definition = serde_json::Value::Null;
    for _ in 0..66 {
        definition = serde_json::json!([definition]);
    }
    binding.value = Some(NativeSchema {
        format: "avro".into(),
        identity: BTreeMap::from([("id".into(), "1".into())]),
        definition,
        references: Vec::new(),
    });
    assert!(binding.canonical_bytes().is_err());
    binding.value.as_mut().unwrap().definition =
        serde_json::json!({"schema": "x".repeat(MAX_SCHEMA_BINDING_BYTES)});
    assert!(binding.canonical_bytes().is_err());
}

#[test]
fn malformed_arrow_layouts_fail_before_codec_allocation() {
    for data_type in [
        DataType::FixedSizeBinary(-1),
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, true)), -1),
        DataType::Decimal128(0, 0),
        DataType::Decimal128(39, 1),
        DataType::Decimal128(10, 11),
        DataType::Time32(TimeUnit::Nanosecond),
        DataType::Time64(TimeUnit::Second),
        DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Utf8)),
        DataType::Map(
            Arc::new(Field::new("entries", DataType::Int64, false)),
            false,
        ),
    ] {
        assert!(SchemaBinding::logical(
            "custom",
            SchemaDirection::Source,
            SchemaOrigin::Explicit,
            Schema::new(vec![Field::new("value", data_type, true)])
        )
        .is_err());
    }
}

#[test]
fn native_nested_metadata_preserves_values_without_renaming_logical_fields() {
    let logical = DataType::Struct(vec![Field::new("id", DataType::Int64, false)].into());
    let native = DataType::Struct(
        vec![Field::new("id", DataType::Int64, false)
            .with_metadata(HashMap::from([("PARQUET:field_id".into(), "17".into())]))]
        .into(),
    );
    assert!(same_logical_type(&logical, &native));
    let renamed = DataType::Struct(vec![Field::new("renamed", DataType::Int64, false)].into());
    assert!(!same_logical_type(&logical, &renamed));
    let reordered = DataType::Struct(vec![Field::new("id", DataType::Int32, false)].into());
    assert!(!same_logical_type(&logical, &reordered));
    let nullable = DataType::Struct(vec![Field::new("id", DataType::Int64, true)].into());
    assert!(!same_logical_type(&logical, &nullable));
}
