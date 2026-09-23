use super::*;
use arrow::array::{ArrayRef, DictionaryArray, Int8Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Int8Type};

fn dictionary_input(retract: bool, nested: bool) -> RecordBatch {
    // Different dictionary indices must still address the same retained values.
    let (keys, values) = if retract {
        (
            vec![Some(0), Some(1), None],
            vec![Some("b"), Some("a"), None],
        )
    } else {
        (
            vec![Some(1), Some(0), None, Some(2), Some(1)],
            vec![Some("a"), Some("b"), None],
        )
    };
    let rows = keys.len();
    let mut values: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(keys),
            Arc::new(StringArray::from(values)),
        )
        .unwrap(),
    );
    if nested {
        values = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("inner", values.data_type().clone(), true)),
            values,
        )]));
    }
    RecordBatch::try_from_iter(vec![
        ("value", values),
        (
            WEIGHT_COLUMN,
            Arc::new(Int64Array::from(vec![if retract { -1 } else { 1 }; rows])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn assert_values(store: &MvStore, schema: &SchemaRef, nested: bool, expected: &[Option<&str>]) {
    let batch = store.to_record_batch("m").unwrap().unwrap();
    assert_eq!(&batch.schema(), schema);
    let mut values = batch.column(0);
    if nested {
        values = values
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(0);
    }
    let decoded = arrow::compute::cast(values, &DataType::Utf8).unwrap();
    let mut actual: Vec<_> = decoded
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .collect();
    actual.sort_unstable();
    assert_eq!(actual, expected);
}

fn dictionary_roundtrip(checkpoint: bool) {
    for nested in [false, true] {
        let input = dictionary_input(false, nested);
        let plain = input.project(&[0]).unwrap();
        let schema = plain.schema();
        let mut store = MvStore::new();
        store
            .create_mv("m", schema.clone(), MvStorageMode::Multiset)
            .unwrap();
        store.update_cycle("m", &[input]).unwrap();
        if checkpoint {
            let bytes = store.checkpoint_states().unwrap();
            let mut restored = MvStore::new();
            restored
                .create_mv("m", schema.clone(), MvStorageMode::Multiset)
                .unwrap();
            restored.restore_from_ipc("m", &bytes["mv:m"]).unwrap();
            store = restored;
        }
        assert_values(
            &store,
            &schema,
            nested,
            &[None, None, Some("a"), Some("b"), Some("b")],
        );
        store
            .update_cycle("m", &[dictionary_input(true, nested)])
            .unwrap();
        assert_values(&store, &schema, nested, &[None, Some("b")]);
    }
}

#[test]
fn multiset_dictionary_snapshot_preserves_schema_and_values() {
    dictionary_roundtrip(false);
}

#[test]
fn multiset_dictionary_checkpoint_preserves_schema_and_values() {
    dictionary_roundtrip(true);
}
