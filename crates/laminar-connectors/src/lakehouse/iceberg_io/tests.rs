use super::*;

#[test]
fn catalog_commit_failure_has_unknown_outcome() {
    let source =
        iceberg::Error::new(iceberg::ErrorKind::Unexpected, "response lost").with_retryable(true);
    let error = iceberg_commit_error(&source);
    assert!(error.is_outcome_unknown());
    assert!(error.is_transient());
    assert!(error.to_string().contains("may have applied"));
}

#[test]
fn catalog_commit_conflict_is_a_definite_retryable_rejection() {
    let source = iceberg::Error::new(
        iceberg::ErrorKind::CatalogCommitConflicts,
        "base metadata changed",
    );
    let error = iceberg_commit_error(&source);
    assert!(!error.is_outcome_unknown());
    assert!(error.is_transient());
}

#[test]
fn test_storage_factory_infers_s3_from_warehouse_url() {
    let f = storage_factory("s3://bucket/warehouse", None).unwrap();
    assert!(format!("{f:?}").contains("S3"));
}

#[test]
fn test_storage_factory_infers_s3a_from_warehouse_url() {
    let f = storage_factory("s3a://bucket/warehouse", None).unwrap();
    assert!(format!("{f:?}").contains("S3"));
}

#[test]
fn test_storage_factory_infers_fs_from_file_url() {
    let f = storage_factory("file:///tmp/warehouse", None).unwrap();
    assert!(format!("{f:?}").contains("Fs"));
}

#[test]
fn test_storage_factory_bare_path_requires_explicit_storage_type() {
    // Trimmed `/` and `./` inference: REST catalogs use logical names
    // and we don't want a silent default to local fs.
    let err = storage_factory("/tmp/warehouse", None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("LDB-5100"), "got: {err}");
}

#[test]
fn test_storage_factory_explicit_overrides_inference() {
    // Lakekeeper-style: warehouse is a name, storage backend is S3.
    let f = storage_factory("demo", Some("s3")).unwrap();
    assert!(format!("{f:?}").contains("S3"));
}

#[test]
fn test_storage_factory_unknown_warehouse_without_storage_type_errors() {
    let err = storage_factory("demo", None).unwrap_err().to_string();
    assert!(err.contains("LDB-5100"), "got: {err}");
}

#[test]
fn test_storage_factory_rejects_unknown_storage_type() {
    let err = storage_factory("demo", Some("hdfs"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("LDB-5101"), "got: {err}");
}

fn empty_fixture_table() -> Table {
    let schema = iceberg::spec::Schema::builder()
        .with_fields(vec![])
        .build()
        .unwrap();

    let creation = iceberg::TableCreation::builder()
        .name("test_table".to_string())
        .schema(schema)
        .location("s3://test/location".to_string())
        .build();

    let metadata = iceberg::spec::TableMetadataBuilder::from_table_creation(creation)
        .unwrap()
        .build()
        .unwrap();

    Table::builder()
        .metadata(metadata.metadata)
        .identifier(TableIdent::new(
            iceberg::NamespaceIdent::new("test".to_string()),
            "t".to_string(),
        ))
        .file_io(iceberg::io::FileIO::new_with_memory())
        .build()
        .unwrap()
}

#[test]
fn test_current_snapshot_id_empty_table() {
    assert!(current_snapshot_id(&empty_fixture_table()).is_none());
}
