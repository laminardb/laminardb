use super::*;
use arrow_array::{Int64Array, StringArray};

#[test]
fn cell_to_datum_null_and_unsupported() {
    let arr = Int64Array::from(vec![None, Some(1)]);
    assert!(IcebergLookupSource::cell_to_datum("id", &arr, 0)
        .unwrap()
        .is_none());
    assert!(IcebergLookupSource::cell_to_datum("id", &arr, 1)
        .unwrap()
        .is_some());
    // Binary keys are not supported as Iceberg predicates.
    let bin = arrow_array::BinaryArray::from(vec![b"x".as_ref()]);
    assert!(IcebergLookupSource::cell_to_datum("k", &bin, 0).is_err());
}

#[test]
fn single_col_predicate_is_in_list() {
    let cols = vec!["id".to_string()];
    let arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
    let s = format!(
        "{}",
        IcebergLookupSource::build_key_predicate(&cols, &arrays, 3).unwrap()
    )
    .to_uppercase();
    assert!(s.contains("IN") && s.contains("id".to_uppercase().as_str()));
}

#[test]
fn composite_predicate_is_or_of_and() {
    let cols = vec!["a".to_string(), "b".to_string()];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(StringArray::from(vec!["x", "y"])),
    ];
    let s = format!(
        "{}",
        IcebergLookupSource::build_key_predicate(&cols, &arrays, 2).unwrap()
    )
    .to_uppercase();
    assert!(s.contains("AND") && s.contains("OR"));
}

#[test]
fn null_key_adds_is_null_term() {
    let cols = vec!["id".to_string()];
    let arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![Some(1), None]))];
    let s = format!(
        "{}",
        IcebergLookupSource::build_key_predicate(&cols, &arrays, 2).unwrap()
    )
    .to_uppercase();
    assert!(s.contains("NULL"));
}
