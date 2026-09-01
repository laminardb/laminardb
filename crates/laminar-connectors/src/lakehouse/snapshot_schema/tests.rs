use std::sync::Arc;

use arrow_array::{Int32Array, StringArray};
use arrow_schema::{DataType, Field, Schema};

use super::*;

#[test]
fn declared_non_null_primary_key_is_preserved_and_enforced() {
    let source_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("id", DataType::Int32, true),
    ]));
    let declared_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        source_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
            Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
        ],
    )
    .unwrap();

    let conformed = conform_snapshot_batch(&batch, &declared_schema).unwrap();
    assert_eq!(conformed.schema(), declared_schema);
    assert_eq!(conformed.schema().field(0).name(), "id");
    assert!(!conformed.schema().field(0).is_nullable());

    let null_key_batch = RecordBatch::try_new(
        source_schema,
        vec![
            Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
            Arc::new(Int32Array::from(vec![Some(1), None])),
        ],
    )
    .unwrap();
    assert!(conform_snapshot_batch(&null_key_batch, &declared_schema).is_err());
}

#[test]
fn incompatible_snapshot_type_is_rejected() {
    let source_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let declared_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        source_schema,
        vec![Arc::new(StringArray::from(vec!["not-an-integer"]))],
    )
    .unwrap();

    assert!(conform_snapshot_batch(&batch, &declared_schema).is_err());
}

#[test]
fn string_view_is_normalized_to_declared_utf8() {
    let source_schema = Arc::new(Schema::new(vec![Field::new(
        "name",
        DataType::Utf8View,
        false,
    )]));
    let declared_schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        source_schema,
        vec![Arc::new(arrow_array::StringViewArray::from(vec!["one"]))],
    )
    .unwrap();

    let conformed = conform_snapshot_batch(&batch, &declared_schema).unwrap();
    assert_eq!(conformed.schema(), declared_schema);
    assert_eq!(conformed.column(0).data_type(), &DataType::Utf8);
}
