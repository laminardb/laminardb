use super::*;
use arrow_array::{Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};

fn lookup_config() -> IcebergLookupSourceConfig {
    let mut config = crate::config::ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://catalog.invalid");
    config.set("catalog.warehouse", "file:///warehouse");
    config.set("namespace", "test");
    config.set("table.name", "events");
    IcebergLookupSourceConfig {
        catalog: IcebergCatalogConfig::from_config(&config).unwrap(),
        storage: IcebergStorageConfig::from_config(&config).unwrap(),
        primary_key_columns: vec!["id".into()],
    }
}

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

#[test]
fn lookup_key_limits_are_fixed() {
    let too_many = vec![b"k".as_slice(); MAX_LOOKUP_KEYS + 1];
    assert!(validate_lookup_keys(&too_many).is_err());
    let too_large = vec![0_u8; MAX_LOOKUP_KEY_BYTES + 1];
    assert!(validate_lookup_keys(&[too_large.as_slice()]).is_err());
}

#[test]
fn invalid_lookup_config_fails_before_catalog_io() {
    let mut config = lookup_config();
    config.primary_key_columns.clear();
    assert!(validate_lookup_config(&config).is_err());

    let mut config = lookup_config();
    config.storage.request_timeout = std::time::Duration::ZERO;
    assert!(validate_lookup_config(&config).is_err());
}

#[tokio::test]
async fn lookup_result_rows_are_bounded_while_streaming() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2]))]).unwrap();
    let stream = Box::pin(futures_util::stream::iter(vec![Ok::<_, iceberg::Error>(
        batch,
    )]));
    let timeout = std::time::Duration::from_secs(1);
    let error = read_lookup_batches(stream, 1, tokio::time::Instant::now() + timeout, timeout)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("distinct-key rows"));
}

#[test]
fn aligned_rows_drop_internal_key_projection() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("id", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("one")])),
            Arc::new(Int64Array::from(vec![1_i64])),
        ],
    )
    .unwrap();
    let projected = project_aligned_rows(vec![Some(batch)], &["name".into()]).unwrap();
    let projected = projected[0].as_ref().unwrap();
    assert_eq!(projected.num_columns(), 1);
    assert_eq!(projected.schema().field(0).name(), "name");
}
