use super::*;
use arrow_array::{Float64Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use object_store::ObjectStoreExt as _;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

fn test_publication_deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(5)
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]))
}

fn test_batch(n: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..n as i64).collect();
    let names: Vec<&str> = (0..n).map(|_| "test").collect();
    let values: Vec<f64> = (0..n).map(|i| i as f64 * 1.5).collect();

    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap()
}

#[test]
fn delta_version_conversion_checks_laminardb_boundaries() {
    assert_eq!(to_delta_version(0).unwrap(), 0);
    assert_eq!(to_delta_version(i64::MAX).unwrap(), i64::MAX as u64);
    assert!(matches!(
        to_delta_version(-1),
        Err(ConnectorError::ConfigurationError(_))
    ));

    assert_eq!(from_delta_version(0).unwrap(), 0);
    assert_eq!(from_delta_version(i64::MAX as u64).unwrap(), i64::MAX);
    assert!(matches!(
        from_delta_version(i64::MAX as u64 + 1),
        Err(ConnectorError::ReadError(_))
    ));
}

async fn create_cdf_table(table_path: &str, schema: &SchemaRef) -> DeltaTable {
    use deltalake::kernel::engine::arrow_conversion::TryIntoKernel as _;
    use deltalake::TableProperty;

    let delta_schema: deltalake::kernel::StructType = schema.as_ref().try_into_kernel().unwrap();
    let url = path_to_url(table_path).unwrap();
    DeltaTable::try_from_url_with_storage_options(url, HashMap::new())
        .await
        .unwrap()
        .create()
        .with_columns(delta_schema.fields().cloned())
        .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
        .await
        .unwrap()
}

fn cdf_batch(change_types: Vec<Option<&str>>) -> RecordBatch {
    let rows = change_types.len();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_change_type", DataType::Utf8, true),
            Field::new("_commit_version", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(0..rows as i64)),
            Arc::new(StringArray::from(change_types)),
            Arc::new(Int64Array::from_value(7, rows)),
        ],
    )
    .unwrap()
}

#[test]
fn cdf_changelog_preserves_every_row_class_as_signed_weight() {
    let output = map_cdf_to_changelog(&cdf_batch(vec![
        Some("insert"),
        Some("delete"),
        Some("update_preimage"),
        Some("update_postimage"),
    ]))
    .unwrap();

    assert_eq!(output.num_rows(), 4);
    assert!(output.schema().index_of("_change_type").is_err());
    assert!(output.schema().index_of("_commit_version").is_err());
    let weight = laminar_core::changelog::WEIGHT_COLUMN;
    let schema = output.schema();
    let field = schema.field_with_name(weight).unwrap();
    assert_eq!(field.data_type(), &DataType::Int64);
    assert!(!field.is_nullable());
    assert_eq!(
        output
            .column_by_name(weight)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, -1, -1, 1]
    );
}

#[test]
fn cdf_changelog_rejects_null_and_unknown_change_types() {
    for change_types in [vec![None], vec![Some("truncate")]] {
        assert!(map_cdf_to_changelog(&cdf_batch(change_types)).is_err());
    }

    let collision = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("__WEIGHT", DataType::Int64, false),
            Field::new("_change_type", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["insert"])),
        ],
    )
    .unwrap();
    assert!(map_cdf_to_changelog(&collision).is_err());
}

#[test]
fn cdf_contract_failures_are_terminal() {
    let unavailable =
        map_cdf_scan_build_error(deltalake::DeltaTableError::ChangeDataNotEnabled { version: 0 });
    assert!(!unavailable.is_transient());
    assert!(unavailable.to_string().contains("CDF is unavailable"));

    let batch = cdf_batch(vec![Some("insert"), Some("delete")]);
    let row_error = checked_cdf_commit_usage(0, 0, &batch, 1, usize::MAX).unwrap_err();
    assert!(!row_error.is_transient());
    assert!(row_error.to_string().contains("hard row limit"));

    let byte_error =
        checked_cdf_commit_usage(0, 0, &batch, usize::MAX, batch.get_array_memory_size() - 1)
            .unwrap_err();
    assert!(!byte_error.is_transient());
    assert!(byte_error.to_string().contains("hard byte limit"));
}

#[test]
fn delta_metadata_retryability_uses_typed_transport_errors() {
    use object_store::client::{HttpError, HttpErrorKind};

    let transient = classify_delta_metadata_error(
        "read cursor",
        &deltalake::DeltaTableError::ObjectStore {
            source: deltalake::ObjectStoreError::Generic {
                store: "test",
                source: Box::new(HttpError::new(
                    HttpErrorKind::Timeout,
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"),
                )),
            },
        },
    );
    assert!(matches!(transient, ConnectorError::ReadError(_)));
    assert!(transient.is_transient());

    let kernel_transient = classify_delta_metadata_error(
        "read cursor",
        &deltalake::DeltaTableError::KernelError(delta_kernel::error::Error::ObjectStore(
            object_store::Error::Generic {
                store: "test",
                source: Box::new(HttpError::new(
                    HttpErrorKind::Request,
                    std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset"),
                )),
            },
        )),
    );
    assert!(matches!(kernel_transient, ConnectorError::ReadError(_)));
    assert!(kernel_transient.is_transient());

    let kernel_unknown_error = deltalake::DeltaTableError::KernelError(
        delta_kernel::error::Error::ObjectStore(object_store::Error::Generic {
            store: "test",
            source: Box::new(HttpError::new(
                HttpErrorKind::Unknown,
                std::io::Error::other("opaque request failure"),
            )),
        }),
    );
    let kernel_unknown_transport =
        classify_delta_metadata_error("read cursor", &kernel_unknown_error);
    assert!(matches!(
        kernel_unknown_transport,
        ConnectorError::ReadError(_)
    ));
    assert!(kernel_unknown_transport.is_transient());
    assert!(
        !delta_error_has_retryable_transport(&kernel_unknown_error),
        "an unknown HTTP write outcome must retain conservative publication semantics"
    );

    let object_store_unknown = classify_delta_object_store_metadata_error(
        "HEAD staged object",
        &deltalake::ObjectStoreError::Generic {
            store: "test",
            source: Box::new(HttpError::new(
                HttpErrorKind::Unknown,
                std::io::Error::other("opaque request failure"),
            )),
        },
    );
    assert!(matches!(object_store_unknown, ConnectorError::ReadError(_)));
    assert!(object_store_unknown.is_transient());

    let permanent = classify_delta_metadata_error(
        "read cursor",
        &deltalake::DeltaTableError::ObjectStore {
            source: deltalake::ObjectStoreError::PermissionDenied {
                path: "table".into(),
                source: Box::new(HttpError::new(
                    HttpErrorKind::Timeout,
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"),
                )),
            },
        },
    );
    assert!(matches!(permanent, ConnectorError::TransactionError(_)));
    assert!(!permanent.is_transient());

    let untyped = classify_delta_metadata_error(
        "read cursor",
        &deltalake::DeltaTableError::ObjectStore {
            source: deltalake::ObjectStoreError::Generic {
                store: "test",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "untyped timeout",
                )),
            },
        },
    );
    assert!(matches!(untyped, ConnectorError::TransactionError(_)));
    assert!(!untyped.is_transient());
}

#[test]
fn delta_storage_errors_do_not_echo_signed_request_urls() {
    use object_store::client::{HttpError, HttpErrorKind};

    let signed = "https://account.blob.example/table?sig=do-not-disclose";
    let error = deltalake::DeltaTableError::ObjectStore {
        source: deltalake::ObjectStoreError::Generic {
            store: "Azure",
            source: Box::new(HttpError::new(
                HttpErrorKind::Timeout,
                std::io::Error::new(std::io::ErrorKind::TimedOut, signed),
            )),
        },
    };
    let metadata = classify_delta_metadata_error("read cursor", &error).to_string();
    assert!(!metadata.contains("do-not-disclose"), "{metadata}");
    assert!(!metadata.contains("account.blob.example"), "{metadata}");
}

#[tokio::test]
async fn delta_metadata_retryability_reaches_real_s3_transport_chain() {
    use object_store::aws::AmazonS3Builder;
    use object_store::RetryConfig;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (connection, _) = listener.accept().await.unwrap();
        drop(connection);
    });
    let store = AmazonS3Builder::new()
        .with_access_key_id("test")
        .with_secret_access_key("test")
        .with_region("test")
        .with_bucket_name("test")
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_retry(RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        })
        .build()
        .unwrap();
    let error = store
        .head(&object_store::path::Path::from("object"))
        .await
        .unwrap_err();
    server.await.unwrap();

    let error =
        deltalake::DeltaTableError::KernelError(delta_kernel::error::Error::ObjectStore(error));
    let classified = classify_delta_metadata_error("read cursor", &error);
    assert!(
        matches!(classified, ConnectorError::ReadError(_)),
        "S3 transport chain was classified as {classified}"
    );
}

#[test]
fn only_proven_optimistic_collisions_are_retryable_conflicts() {
    use deltalake::kernel::transaction::{CommitConflictError, TransactionError};

    let proven = vec![
        deltalake::DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(CommitConflictError::ConcurrentAppend),
        },
        deltalake::DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(CommitConflictError::ConcurrentDeleteRead),
        },
        deltalake::DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(CommitConflictError::ConcurrentDeleteDelete),
        },
        deltalake::DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(CommitConflictError::ConcurrentTransaction),
        },
    ];
    for error in proven {
        assert!(DeltaWriteAttemptError::Delta(error).is_definite_optimistic_conflict());
    }

    for error in [
        deltalake::DeltaTableError::VersionAlreadyExists(4),
        deltalake::DeltaTableError::Transaction {
            source: TransactionError::VersionAlreadyExists(4),
        },
        deltalake::DeltaTableError::Transaction {
            source: TransactionError::MaxCommitAttempts(3),
        },
    ] {
        assert!(
            !DeltaWriteAttemptError::Delta(error).is_definite_optimistic_conflict(),
            "HTTP retries can turn a published conditional write into an AlreadyExists result"
        );
    }

    let excluded = [
        CommitConflictError::MetadataChanged,
        CommitConflictError::ProtocolChanged("concurrent conflict".into()),
        CommitConflictError::UnsupportedWriterVersion(99),
        CommitConflictError::UnsupportedReaderVersion(99),
        CommitConflictError::CorruptedState {
            source: Box::new(std::io::Error::other("concurrent conflict")),
        },
        CommitConflictError::Predicate {
            source: Box::new(std::io::Error::other("concurrent conflict")),
        },
        CommitConflictError::NoMetadata,
    ];
    for conflict in excluded {
        let error = DeltaWriteAttemptError::Delta(deltalake::DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(conflict),
        });
        assert!(!error.is_definite_optimistic_conflict(), "{error}");
    }
}

#[test]
fn coordinated_typed_collisions_prove_nonpublication() {
    use deltalake::kernel::transaction::TransactionError;

    let exhausted = deltalake::DeltaTableError::Transaction {
        source: TransactionError::MaxCommitAttempts(15),
    };
    assert!(is_definite_coordinated_nonpublication(&exhausted));
    assert!(!is_definite_coordinated_nonpublication(
        &deltalake::DeltaTableError::Transaction {
            source: TransactionError::LogStoreError {
                msg: "unknown".into(),
                source: Box::new(std::io::Error::other("unknown")),
            },
        }
    ));
}

#[test]
fn coordinated_storage_budget_replaces_provider_aliases() {
    let options = HashMap::from([
        ("AWS_TIMEOUT".into(), "10m".into()),
        ("azure_connect_timeout".into(), "5m".into()),
        ("retry_timeout".into(), "1h".into()),
        ("max_retries".into(), "100".into()),
        ("backoff.max_backoff".into(), "2m".into()),
        ("aws_region".into(), "us-east-1".into()),
    ]);

    let bounded = bound_coordinated_storage_options(options);
    assert_eq!(bounded["timeout"], COORDINATED_REQUEST_TIMEOUT);
    assert_eq!(bounded["connect_timeout"], COORDINATED_CONNECT_TIMEOUT);
    assert_eq!(bounded["retry_timeout"], COORDINATED_RETRY_TIMEOUT);
    assert_eq!(bounded["max_retries"], COORDINATED_HTTP_MAX_RETRIES);
    assert_eq!(bounded["max_backoff"], COORDINATED_MAX_BACKOFF);
    assert_eq!(bounded["aws_region"], "us-east-1");
    assert!(!bounded.contains_key("AWS_TIMEOUT"));
    assert!(!bounded.contains_key("azure_connect_timeout"));
    assert!(!bounded.contains_key("backoff.max_backoff"));
}

#[test]
fn coordinated_provider_and_retention_scope_fail_closed() {
    assert!(is_certified_coordinated_log_store("DefaultLogStore"));
    assert!(!is_certified_coordinated_log_store("S3DynamoDbLogStore"));
    assert!(!is_certified_coordinated_log_store("LakeFSLogStore"));

    let no_environment = |_: &str| None;
    let custom = HashMap::from([("aws_endpoint_url".into(), "http://minio:9000".into())]);
    assert!(validate_coordinated_storage_preflight_with_env(
        "s3://bucket/table",
        &custom,
        &no_environment,
    )
    .is_err());
    for conditional_put in ["disabled", "dynamo:commits"] {
        let options = HashMap::from([("aws_conditional_put".into(), conditional_put.into())]);
        assert!(validate_coordinated_storage_preflight_with_env(
            "s3://bucket/table",
            &options,
            &no_environment,
        )
        .is_err());
    }
    validate_coordinated_storage_preflight_with_env(
        "s3://bucket/table",
        &HashMap::from([("aws_conditional_put".into(), "etag".into())]),
        &no_environment,
    )
    .unwrap();

    let s3_environment = HashMap::from([("AWS_ENDPOINT_URL", "http://minio:9000")]);
    let s3_environment = |key: &str| s3_environment.get(key).map(ToString::to_string);
    assert!(validate_coordinated_storage_preflight_with_env(
        "s3://bucket/table",
        &HashMap::new(),
        &s3_environment,
    )
    .is_err());
    validate_coordinated_storage_preflight_with_env(
        "file:///tmp/table",
        &HashMap::new(),
        &s3_environment,
    )
    .unwrap();
    assert!(validate_coordinated_storage_preflight_with_env(
        "unknown://bucket/table",
        &HashMap::new(),
        &no_environment,
    )
    .is_err());
    let conditional_environment = HashMap::from([("AWS_CONDITIONAL_PUT", "dynamo:commits")]);
    let conditional_environment =
        |key: &str| conditional_environment.get(key).map(ToString::to_string);
    assert!(validate_coordinated_storage_preflight_with_env(
        "s3://bucket/table",
        &HashMap::new(),
        &conditional_environment,
    )
    .is_err());

    let azure_environment = HashMap::from([("AZURE_STORAGE_USE_EMULATOR", "true")]);
    let azure_environment = |key: &str| azure_environment.get(key).map(ToString::to_string);
    assert!(validate_coordinated_storage_preflight_with_env(
        "abfss://container@account/table",
        &HashMap::new(),
        &azure_environment,
    )
    .is_err());
    for path in [
        "az://container/table",
        "abfs://container/table",
        "abfss://container/table",
        "wasb://container/table",
        "wasbs://container/table",
    ] {
        validate_coordinated_storage_preflight_with_env(path, &HashMap::new(), &no_environment)
            .unwrap();
    }

    for options in [
        HashMap::from([("google_base_url".into(), "http://gcs-emulator".into())]),
        HashMap::from([(
            "google_service_account_key".into(),
            r#"{"gcs_base_url":"http://gcs-emulator"}"#.into(),
        )]),
        HashMap::from([(
            "google_service_account_path".into(),
            "service-account.json".into(),
        )]),
    ] {
        assert!(validate_coordinated_storage_preflight_with_env(
            "gs://bucket/table",
            &options,
            &no_environment,
        )
        .is_err());
    }
    validate_coordinated_storage_preflight_with_env(
        "gs://bucket/table",
        &HashMap::from([(
            "google_service_account_key".into(),
            r#"{"client_email":"service@example.com"}"#.into(),
        )]),
        &no_environment,
    )
    .unwrap();

    validate_coordinated_retention(MIN_COORDINATED_DELETED_FILE_RETENTION).unwrap();
    assert!(validate_coordinated_retention(
        MIN_COORDINATED_DELETED_FILE_RETENTION
            .checked_sub(Duration::from_secs(1))
            .unwrap()
    )
    .is_err());
}

#[test]
fn coordinated_emulator_override_is_debug_soak_only() {
    let azure_options = HashMap::from([(
        "azure_storage_endpoint".into(),
        "http://127.0.0.1:10000/devstoreaccount1".into(),
    )]);
    let azure_environment =
        |key: &str| (key == "LAMINAR_SOAK_ALLOW_AZURE_EMULATOR").then(|| "1".to_string());
    let azure = validate_coordinated_storage_preflight_with_env(
        "az://container/table",
        &azure_options,
        &azure_environment,
    );

    let gcs_options = HashMap::from([
        ("google_base_url".into(), "http://127.0.0.1:4443".into()),
        (
            "google_service_account_key".into(),
            r#"{"disable_oauth":true}"#.into(),
        ),
    ]);
    let gcs_environment =
        |key: &str| (key == "LAMINAR_SOAK_ALLOW_GCS_EMULATOR").then(|| "true".to_string());
    let gcs = validate_coordinated_storage_preflight_with_env(
        "gs://bucket/table",
        &gcs_options,
        &gcs_environment,
    );

    if cfg!(debug_assertions) {
        azure.unwrap();
        gcs.unwrap();
    } else {
        assert!(azure.is_err());
        assert!(gcs.is_err());
    }
}

async fn staged_adds(table: &DeltaTable, batch: RecordBatch) -> Vec<deltalake::kernel::Add> {
    use deltalake::writer::{DeltaWriter, RecordBatchWriter};

    let mut writer = RecordBatchWriter::for_table(table).unwrap();
    writer.write(batch).await.unwrap();
    writer.flush().await.unwrap()
}

fn coordinated_cursor(checkpoint_id: u64) -> crate::connector::CoordinatedCommitCursor {
    crate::connector::CoordinatedCommitCursor {
        checkpoint_id,
        fencing_token: 1,
    }
}

async fn assert_coordinated_cursor_absent(table: &DeltaTable, external_key: &str) {
    assert_eq!(
        get_coordinated_cursor(table, external_key).await.unwrap(),
        None,
        "failed publication must not advance the Delta transaction cursor"
    );
}

#[tokio::test]
async fn test_open_creates_table() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    // Open with schema should create the table.
    let schema = test_schema();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    assert_eq!(table.version(), Some(0));

    // Verify _delta_log directory was created.
    let delta_log = temp_dir.path().join("_delta_log");
    assert!(delta_log.exists(), "_delta_log directory should exist");
}

#[tokio::test]
async fn test_open_existing_table() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    // Create the table.
    let schema = test_schema();
    let _ = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    // Reopen without schema - should work.
    let table = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();

    assert_eq!(table.version(), Some(0));
}

#[tokio::test]
async fn test_open_nonexistent_without_schema_defers() {
    let temp_dir = TempDir::new().unwrap();
    let nonexistent_table = temp_dir.path().join("nonexistent");
    std::fs::create_dir_all(&nonexistent_table).unwrap();
    let table_path = nonexistent_table.to_str().unwrap();

    // Open without schema returns an uninitialized table (deferred creation).
    let result = open_or_create_table(table_path, HashMap::new(), None).await;
    assert!(result.is_ok());
    let table = result.unwrap();
    assert!(table.version().is_none(), "table should be uninitialized");
}

#[tokio::test]
async fn test_write_batch_creates_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    // Create table.
    let schema = test_schema();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    // Write a batch.
    let batch = test_batch(100);
    let (table, version) = write_batches(
        table,
        vec![batch],
        SaveMode::Append,
        None,
        false,
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(version, 1);
    assert_eq!(table.version(), Some(1));

    // Verify Parquet files were created (in the table directory).
    let parquet_files: Vec<_> = std::fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .collect();

    assert!(
        !parquet_files.is_empty(),
        "should have created Parquet files"
    );
}

/// Regression: `Timestamp(Millisecond)` used to fail table creation with
/// "Invalid data type for Delta Lake"; the boundary must widen it to
/// microseconds with timezone metadata preserved.
#[tokio::test]
async fn test_millisecond_timestamp_columns_widen_to_microseconds() {
    use arrow_array::TimestampMillisecondArray;
    use arrow_schema::TimeUnit;
    use deltalake::kernel::engine::arrow_conversion::TryFromKernel as _;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "probe_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
            true,
        ),
    ]));

    let table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(TimestampMillisecondArray::from(vec![Some(1_500)])),
            Arc::new(TimestampMillisecondArray::from(vec![Some(2_500)]).with_timezone("UTC")),
        ],
    )
    .unwrap();

    let (table, version) = write_batches(
        table,
        vec![batch],
        SaveMode::Append,
        None,
        false,
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(version, 1);

    let kernel_schema = table.snapshot().unwrap().schema();
    let stored = arrow_schema::Schema::try_from_kernel(kernel_schema.as_ref()).unwrap();
    assert_eq!(
        stored.field_with_name("probe_time").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        stored.field_with_name("event_time").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
    );
}

#[tokio::test]
async fn test_multiple_appends_sequential() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let schema = test_schema();
    let mut table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    for version in 1..=3 {
        let batch = test_batch(10);
        let result = write_batches(
            table,
            vec![batch],
            SaveMode::Append,
            None,
            false,
            None,
            None,
        )
        .await
        .unwrap();
        table = result.0;
        assert_eq!(result.1, i64::from(version));
    }

    assert_eq!(table.version(), Some(3));
}

#[tokio::test]
async fn test_get_table_schema() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let expected_schema = test_schema();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&expected_schema))
        .await
        .unwrap();

    let actual_schema = get_table_schema(&table).unwrap();

    // Verify field count and names match.
    assert_eq!(actual_schema.fields().len(), expected_schema.fields().len());
    for (expected, actual) in expected_schema.fields().iter().zip(actual_schema.fields()) {
        assert_eq!(expected.name(), actual.name());
    }
}

#[tokio::test]
async fn test_write_empty_batches() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let schema = test_schema();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    // Write empty batch list - should be no-op.
    let (table, version) = write_batches(table, vec![], SaveMode::Append, None, false, None, None)
        .await
        .unwrap();

    // Version should still be 0 (no write happened).
    assert_eq!(version, 0);
    assert_eq!(table.version(), Some(0));
}

#[tokio::test]
async fn test_write_multiple_batches() {
    // Test writing multiple batches in a single transaction.
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let schema = test_schema();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    // Write multiple batches.
    let batch1 = test_batch(50);
    let batch2 = test_batch(50);
    let (table, version) = write_batches(
        table,
        vec![batch1, batch2],
        SaveMode::Append,
        None,
        false,
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(version, 1);
    assert_eq!(table.version(), Some(1));

    // Reopen and verify we can read the state.
    let reopened = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    assert_eq!(reopened.version(), Some(1));
}

#[test]
fn test_path_to_url_local() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_str().unwrap();

    let url = path_to_url(path).unwrap();
    assert_eq!(url.scheme(), "file");
}

#[test]
fn test_path_to_url_s3() {
    let url = path_to_url("s3://my-bucket/path/to/table").unwrap();
    assert_eq!(url.scheme(), "s3");
    assert_eq!(url.host_str(), Some("my-bucket"));
}

#[test]
fn test_path_to_url_azure() {
    let url = path_to_url("az://my-container/path/to/table").unwrap();
    assert_eq!(url.scheme(), "az");
}

#[test]
fn test_path_to_url_gcs() {
    let url = path_to_url("gs://my-bucket/path/to/table").unwrap();
    assert_eq!(url.scheme(), "gs");

    let alias = path_to_url("GCS://my-bucket/path/to/table").unwrap();
    assert_eq!(alias.scheme(), "gs");
}

#[test]
fn delta_azure_adapter_preserves_qualified_authority_as_options() {
    let adapted =
        adapt_delta_location("wasbs://container@account.blob.core.chinacloudapi.cn/path/to/table")
            .unwrap();
    assert_eq!(adapted.url, "az://container/path/to/table");
    let mut options = HashMap::new();
    apply_url_derived_options(&mut options, &adapted).unwrap();
    assert_eq!(options["azure_storage_account_name"], "account");
    assert_eq!(options["azure_container_name"], "container");
    assert_eq!(
        options["azure_endpoint"],
        "https://account.blob.core.chinacloudapi.cn"
    );
}

#[test]
fn delta_azure_adapter_rejects_conflicting_authority_options() {
    let adapted =
        adapt_delta_location("abfss://filesystem@account.dfs.private.example/path/to/table")
            .unwrap();
    let mut options = HashMap::from([(
        "azure_storage_account_name".to_string(),
        "different".to_string(),
    )]);
    let error = apply_url_derived_options(&mut options, &adapted).unwrap_err();
    assert!(error.to_string().contains("conflicts"));
    assert!(!error.to_string().contains("different"));
}

#[test]
fn delta_path_rejects_signed_queries_without_echoing_them() {
    let error = path_to_url("gs://bucket/table?X-Goog-Signature=secret-value").unwrap_err();
    assert!(!error.to_string().contains("secret-value"));
}

// ── End-to-end tests for new functionality ──

#[tokio::test]
async fn test_get_latest_version() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let schema = test_schema();
    let mut table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    // Initial version is 0.
    let v = get_latest_version(&mut table).await.unwrap();
    assert_eq!(v, 0);

    // Write a batch -> version 1.
    let batch = test_batch(10);
    let (returned_table, version) = write_batches(
        table,
        vec![batch],
        SaveMode::Append,
        None,
        false,
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(version, 1);
    table = returned_table;

    let v = get_latest_version(&mut table).await.unwrap();
    assert_eq!(v, 1);
}

#[tokio::test]
async fn test_read_batches_at_version() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let schema = test_schema();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    // Write 50 rows at version 1.
    let batch = test_batch(50);
    let (table, _) = write_batches(
        table,
        vec![batch],
        SaveMode::Append,
        None,
        false,
        None,
        None,
    )
    .await
    .unwrap();

    // Write 30 more rows at version 2.
    let batch = test_batch(30);
    let (_table, _) = write_batches(
        table,
        vec![batch],
        SaveMode::Append,
        None,
        false,
        None,
        None,
    )
    .await
    .unwrap();

    // Read version 1 — should get 50 rows.
    let mut read_table = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let (batches, _) = read_batches_at_version(&mut read_table, 1, 10000)
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 50);

    // Read version 2 — should get 80 rows (cumulative).
    let (batches, _) = read_batches_at_version(&mut read_table, 2, 10000)
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 80);
}

#[tokio::test]
async fn test_sink_source_roundtrip() {
    use super::super::delta::DeltaLakeSink;
    use super::super::delta_config::DeltaLakeSinkConfig;
    use super::super::delta_source::DeltaSource;
    use super::super::delta_source_config::DeltaSourceConfig;
    use crate::config::ConnectorConfig;
    use crate::connector::{
        DeliveryGuarantee, SinkConnector, SourceConnector, SourcePosition, SourceStart,
    };

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    create_cdf_table(table_path, &test_schema()).await;

    // Write data via sink.
    let sink_config = DeltaLakeSinkConfig::new(table_path);
    let mut sink = DeltaLakeSink::with_schema(sink_config, test_schema());
    let connector_config = ConnectorConfig::new("delta-lake");
    sink.open(&connector_config).await.unwrap();

    let batch = test_batch(25);
    sink.write_batch(&batch).await.unwrap();
    sink.flush().await.unwrap();
    sink.close().await.unwrap();

    // Read data via source.
    let mut source_config = DeltaSourceConfig::new(table_path);
    source_config.starting_version = Some(1);
    let mut source = DeltaSource::new(source_config, None);
    let source_connector_config = ConnectorConfig::new("delta-lake");
    source
        .start(
            SourceStart::new(
                source_connector_config,
                SourcePosition::Initial,
                DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    // Poll — should get version 1 data (25 rows).
    let result = source.poll_batch(10000).await.unwrap();
    assert!(result.is_some(), "should have received a batch");
    let total_rows: usize = {
        let mut rows = result.unwrap().records.num_rows();
        // Drain any remaining buffered batches.
        while let Ok(Some(batch)) = source.poll_batch(10000).await {
            rows += batch.records.num_rows();
        }
        rows
    };
    assert_eq!(total_rows, 25);

    source.close().await.unwrap();
}

#[tokio::test]
async fn test_source_checkpoint_resume_is_rejected_until_delta_replay_is_certified() {
    use super::super::delta_source::DeltaSource;
    use super::super::delta_source_config::DeltaSourceConfig;
    use crate::config::ConnectorConfig;
    use crate::connector::{DeliveryGuarantee, SourceConnector, SourcePosition, SourceStart};

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    // Create table and write 2 versions.
    let schema = test_schema();
    let table = create_cdf_table(table_path, &schema).await;

    let (table, _) = write_batches(
        table,
        vec![test_batch(10)],
        SaveMode::Append,
        None,
        false,
        None,
        None,
    )
    .await
    .unwrap();
    let (_table, _) = write_batches(
        table,
        vec![test_batch(20)],
        SaveMode::Append,
        None,
        false,
        None,
        None,
    )
    .await
    .unwrap();

    // Consume both data commits in order from the first data version.
    let mut source_config = DeltaSourceConfig::new(table_path);
    source_config.starting_version = Some(1);
    let mut source = DeltaSource::new(source_config.clone(), None);
    let connector_config = ConnectorConfig::new("delta-lake");
    source
        .start(
            SourceStart::new(
                connector_config.clone(),
                SourcePosition::Initial,
                DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    // Drain data commits 1 and 2.
    let _ = source.poll_batch(10000).await.unwrap();
    // Drain buffered.
    while let Ok(Some(_)) = source.poll_batch(10000).await {}

    // Checkpoint reflects the fully-consumed latest version.
    let cp = source.checkpoint();
    assert_eq!(cp.get_offset("delta_version"), Some("2"));
    source.close().await.unwrap();

    // Delta replay is not admitted until its snapshot/CDF cut is certified.
    let mut source2 = DeltaSource::new(source_config, None);
    let error = source2
        .start(
            SourceStart::new(
                connector_config,
                SourcePosition::Resume {
                    attempt: laminar_core::checkpoint::CheckpointAttempt::new(2, 2),
                    checkpoint: cp,
                },
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("uncertified Delta replay must fail closed");
    assert!(error.to_string().contains("ephemeral"));
}

#[tokio::test]
async fn coordinated_cursor_persists_checkpoint_and_fence_atomically() {
    use crate::connector::CoordinatedCommitCursor;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let cursor = CoordinatedCommitCursor {
        checkpoint_id: 101,
        fencing_token: 4,
    };

    assert!(get_coordinated_cursor(&table, "ldb-c3-test")
        .await
        .unwrap()
        .is_none());
    commit_adds_coordinated(
        &table,
        vec![],
        "ldb-c3-test",
        cursor,
        test_publication_deadline(),
    )
    .await
    .unwrap();

    let reopened = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    assert_eq!(
        get_coordinated_cursor(&reopened, "ldb-c3-test")
            .await
            .unwrap(),
        Some(cursor)
    );
}

#[tokio::test]
async fn coordinated_cursor_rejects_token_change_stale_fence_and_checkpoint_rollback() {
    use crate::connector::CoordinatedCommitCursor;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    commit_adds_coordinated(
        &table,
        vec![],
        "ldb-c3-test",
        CoordinatedCommitCursor {
            checkpoint_id: 101,
            fencing_token: 4,
        },
        test_publication_deadline(),
    )
    .await
    .unwrap();

    let table = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let changed_token = commit_adds_coordinated(
        &table,
        vec![],
        "ldb-c3-test",
        CoordinatedCommitCursor {
            checkpoint_id: 101,
            fencing_token: 5,
        },
        test_publication_deadline(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        changed_token.contains("cannot change"),
        "got: {changed_token}"
    );

    let stale = commit_adds_coordinated(
        &table,
        vec![],
        "ldb-c3-test",
        CoordinatedCommitCursor {
            checkpoint_id: 102,
            fencing_token: 3,
        },
        test_publication_deadline(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(stale.contains("stale"), "got: {stale}");

    commit_adds_coordinated(
        &table,
        vec![],
        "ldb-c3-test",
        CoordinatedCommitCursor {
            checkpoint_id: 102,
            fencing_token: 5,
        },
        test_publication_deadline(),
    )
    .await
    .unwrap();
    let table = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let rollback = commit_adds_coordinated(
        &table,
        vec![],
        "ldb-c3-test",
        CoordinatedCommitCursor {
            checkpoint_id: 101,
            fencing_token: 6,
        },
        test_publication_deadline(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(rollback.contains("roll back"), "got: {rollback}");
}

#[tokio::test]
async fn coordinated_race_serializes_cursor_and_permanently_fences_stale_writer() {
    use crate::connector::CoordinatedCommitCursor;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();

    let stale_table = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let newer_table = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let stale_adds = staged_adds(&stale_table, test_batch(1)).await;
    let newer_adds = staged_adds(&newer_table, test_batch(2)).await;
    let newer_retry_adds = newer_adds.clone();

    let (stale_result, newer_result) = tokio::join!(
        commit_adds_coordinated(
            &stale_table,
            stale_adds,
            "ldb-c3-race",
            CoordinatedCommitCursor {
                checkpoint_id: 101,
                fencing_token: 1,
            },
            test_publication_deadline(),
        ),
        commit_adds_coordinated(
            &newer_table,
            newer_adds,
            "ldb-c3-race",
            CoordinatedCommitCursor {
                checkpoint_id: 102,
                fencing_token: 2,
            },
            test_publication_deadline(),
        ),
    );
    assert!(
        stale_result.is_ok() ^ newer_result.is_ok(),
        "the shared transaction ids must admit exactly one stale-base winner: \
         stale={stale_result:?}, newer={newer_result:?}"
    );

    let mut table = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let cursor = get_coordinated_cursor(&table, "ldb-c3-race")
        .await
        .unwrap()
        .unwrap();
    if cursor.fencing_token == 1 {
        commit_adds_coordinated(
            &table,
            newer_retry_adds,
            "ldb-c3-race",
            CoordinatedCommitCursor {
                checkpoint_id: 102,
                fencing_token: 2,
            },
            test_publication_deadline(),
        )
        .await
        .unwrap();
        table = open_or_create_table(table_path, HashMap::new(), None)
            .await
            .unwrap();
    }
    assert_eq!(
        get_coordinated_cursor(&table, "ldb-c3-race").await.unwrap(),
        Some(CoordinatedCommitCursor {
            checkpoint_id: 102,
            fencing_token: 2,
        })
    );

    let latest = get_latest_version(&mut table).await.unwrap();
    let (batches, _) = read_batches_at_version(&mut table, latest, 100)
        .await
        .unwrap();
    let rows_before: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert!(
        rows_before == 2 || rows_before == 3,
        "valid serial histories contain newer data alone or stale then newer data; got {rows_before} rows"
    );

    let stale_after_fence = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let forbidden_adds = staged_adds(&stale_after_fence, test_batch(4)).await;
    let error = commit_adds_coordinated(
        &stale_after_fence,
        forbidden_adds,
        "ldb-c3-race",
        CoordinatedCommitCursor {
            checkpoint_id: 103,
            fencing_token: 1,
        },
        test_publication_deadline(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("stale"), "got: {error}");

    let mut final_table = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let latest = get_latest_version(&mut final_table).await.unwrap();
    let (batches, _) = read_batches_at_version(&mut final_table, latest, 100)
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        rows_before
    );
}

#[tokio::test]
async fn coordinated_batch_filters_overlap_only_after_refreshing_stale_handle() {
    use crate::connector::{
        CoordinatedCommitBatch, CoordinatedCommitNamespace, CoordinatedCommitPayload,
    };
    use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
    use laminar_core::checkpoint::CheckpointAttempt;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let writer = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let stale_committer = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let binding = coordinated_table_binding(&writer).unwrap();
    let first_descriptor =
        encode_commit_descriptor(&binding, &staged_adds(&writer, test_batch(1)).await).unwrap();
    let second_descriptor =
        encode_commit_descriptor(&binding, &staged_adds(&writer, test_batch(2)).await).unwrap();
    let first_attempt = CheckpointAttempt::canonical(101);
    let second_attempt = CheckpointAttempt::canonical(102);
    let namespace = CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        "018f0000-0000-7000-8000-000000000001",
        "delta_overlap_refresh",
    )
    .unwrap();

    let first = CoordinatedCommitBatch {
        namespace: namespace.clone(),
        expected_predecessor: crate::connector::CoordinatedCommitCursor {
            checkpoint_id: 0,
            fencing_token: 0,
        },
        fencing_token: 1,
        target: first_attempt,
        entries: vec![CoordinatedCommitPayload {
            attempt: first_attempt,
            participant_id: 1,
            payload: Some(first_descriptor.clone()),
        }],
    };
    assert_eq!(
        commit_batch_coordinated(&writer, &first, test_publication_deadline())
            .await
            .unwrap(),
        1
    );
    assert_eq!(stale_committer.version(), Some(0));

    // This batch was assembled against checkpoint zero and includes both
    // attempts. The helper must refresh the stale handle to checkpoint 101,
    // filter its descriptor, and publish only checkpoint 102's file.
    let overlap = CoordinatedCommitBatch {
        namespace: namespace.clone(),
        expected_predecessor: crate::connector::CoordinatedCommitCursor {
            checkpoint_id: 0,
            fencing_token: 0,
        },
        fencing_token: 2,
        target: second_attempt,
        entries: vec![
            CoordinatedCommitPayload {
                attempt: first_attempt,
                participant_id: 1,
                payload: Some(first_descriptor),
            },
            CoordinatedCommitPayload {
                attempt: second_attempt,
                participant_id: 1,
                payload: Some(second_descriptor),
            },
        ],
    };
    assert_eq!(
        commit_batch_coordinated(&stale_committer, &overlap, test_publication_deadline())
            .await
            .unwrap(),
        1
    );

    let mut reopened = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    assert_eq!(
        get_coordinated_cursor(&reopened, &namespace.external_key())
            .await
            .unwrap(),
        Some(crate::connector::CoordinatedCommitCursor {
            checkpoint_id: 102,
            fencing_token: 2,
        })
    );
    let latest = get_latest_version(&mut reopened).await.unwrap();
    let (batches, _) = read_batches_at_version(&mut reopened, latest, 100)
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        3,
        "checkpoint 101's descriptor must not be appended again"
    );
}

#[tokio::test]
async fn coordinated_descriptor_batch_rejects_mixed_table_bindings() {
    let first_dir = TempDir::new().unwrap();
    let second_dir = TempDir::new().unwrap();
    let first = open_or_create_table(
        first_dir.path().to_str().unwrap(),
        HashMap::new(),
        Some(&test_schema()),
    )
    .await
    .unwrap();
    let second = open_or_create_table(
        second_dir.path().to_str().unwrap(),
        HashMap::new(),
        Some(&test_schema()),
    )
    .await
    .unwrap();
    let first_descriptor = encode_commit_descriptor(
        &coordinated_table_binding(&first).unwrap(),
        &staged_adds(&first, test_batch(1)).await,
    )
    .unwrap();
    let second_descriptor = encode_commit_descriptor(
        &coordinated_table_binding(&second).unwrap(),
        &staged_adds(&second, test_batch(1)).await,
    )
    .unwrap();

    let error = decode_commit_descriptors(&[first_descriptor, second_descriptor])
        .unwrap_err()
        .to_string();
    assert!(error.contains("different table metadata"), "got: {error}");
}

#[tokio::test]
async fn coordinated_late_exact_commit_and_higher_batch_cannot_both_win() {
    use crate::connector::{
        CoordinatedCommitBatch, CoordinatedCommitCursor, CoordinatedCommitNamespace,
        CoordinatedCommitPayload,
    };
    use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
    use laminar_core::checkpoint::CheckpointAttempt;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let writer = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let pending_committer = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let higher_committer = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let binding = coordinated_table_binding(&writer).unwrap();
    let pending_descriptor =
        encode_commit_descriptor(&binding, &staged_adds(&writer, test_batch(1)).await).unwrap();
    let higher_descriptor =
        encode_commit_descriptor(&binding, &staged_adds(&writer, test_batch(2)).await).unwrap();
    let second = CheckpointAttempt::canonical(102);
    let third = CheckpointAttempt::canonical(103);
    let namespace = CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        "018f0000-0000-7000-8000-000000000001",
        "delta_late_retry_race",
    )
    .unwrap();
    let pending = CoordinatedCommitBatch {
        namespace: namespace.clone(),
        expected_predecessor: CoordinatedCommitCursor {
            checkpoint_id: 0,
            fencing_token: 0,
        },
        fencing_token: 1,
        target: second,
        entries: vec![CoordinatedCommitPayload {
            attempt: second,
            participant_id: 1,
            payload: Some(pending_descriptor.clone()),
        }],
    };
    let higher = CoordinatedCommitBatch {
        namespace: namespace.clone(),
        expected_predecessor: CoordinatedCommitCursor {
            checkpoint_id: 0,
            fencing_token: 0,
        },
        fencing_token: 2,
        target: third,
        entries: vec![
            CoordinatedCommitPayload {
                attempt: second,
                participant_id: 1,
                payload: Some(pending_descriptor),
            },
            CoordinatedCommitPayload {
                attempt: third,
                participant_id: 1,
                payload: Some(higher_descriptor),
            },
        ],
    };

    let (pending_result, higher_result) = tokio::join!(
        commit_batch_coordinated(&pending_committer, &pending, test_publication_deadline()),
        commit_batch_coordinated(&higher_committer, &higher, test_publication_deadline()),
    );
    assert!(
        pending_result.is_ok() ^ higher_result.is_ok(),
        "shared cursor transactions must admit one winner: pending={pending_result:?}, higher={higher_result:?}"
    );

    let mut current = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    if get_coordinated_cursor(&current, &namespace.external_key())
        .await
        .unwrap()
        == Some(CoordinatedCommitCursor {
            checkpoint_id: 102,
            fencing_token: 1,
        })
    {
        commit_batch_coordinated(&current, &higher, test_publication_deadline())
            .await
            .unwrap();
        current = open_or_create_table(table_path, HashMap::new(), None)
            .await
            .unwrap();
    }
    assert_eq!(
        get_coordinated_cursor(&current, &namespace.external_key())
            .await
            .unwrap(),
        Some(CoordinatedCommitCursor {
            checkpoint_id: 103,
            fencing_token: 2,
        })
    );
    let latest = get_latest_version(&mut current).await.unwrap();
    let (batches, _) = read_batches_at_version(&mut current, latest, 100)
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
}

#[tokio::test]
async fn coordinated_empty_batch_commits_cursor_without_object_io() {
    use crate::connector::{
        CoordinatedCommitBatch, CoordinatedCommitNamespace, CoordinatedCommitPayload,
    };
    use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
    use laminar_core::checkpoint::CheckpointAttempt;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let namespace = CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        "018f0000-0000-7000-8000-000000000001",
        "delta_empty",
    )
    .unwrap();
    let target = CheckpointAttempt::canonical(101);
    let batch = CoordinatedCommitBatch {
        namespace: namespace.clone(),
        expected_predecessor: crate::connector::CoordinatedCommitCursor {
            checkpoint_id: 0,
            fencing_token: 0,
        },
        fencing_token: 1,
        target,
        entries: vec![CoordinatedCommitPayload {
            attempt: target,
            participant_id: 1,
            payload: None,
        }],
    };

    assert_eq!(
        commit_batch_coordinated(&table, &batch, test_publication_deadline())
            .await
            .unwrap(),
        0
    );
    let reopened = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    assert_eq!(
        get_coordinated_cursor(&reopened, &namespace.external_key())
            .await
            .unwrap(),
        Some(crate::connector::CoordinatedCommitCursor {
            checkpoint_id: 101,
            fencing_token: 1,
        })
    );
}

#[tokio::test]
async fn coordinated_cursor_rejects_partial_pair_and_finite_retention() {
    use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
    use deltalake::kernel::Transaction;
    use deltalake::protocol::DeltaOperation;

    let corrupt_dir = TempDir::new().unwrap();
    let corrupt_path = corrupt_dir.path().to_str().unwrap();
    let table = open_or_create_table(corrupt_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let snapshot = table.snapshot().unwrap();
    CommitBuilder::from(
        CommitProperties::default()
            .with_application_transaction(Transaction::new("ldb-c3-test.checkpoint", 101)),
    )
    .with_actions(vec![])
    .build(
        Some(snapshot),
        table.log_store(),
        DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        },
    )
    .await
    .unwrap();
    let corrupt = open_or_create_table(corrupt_path, HashMap::new(), None)
        .await
        .unwrap();
    assert!(get_coordinated_cursor(&corrupt, "ldb-c3-test")
        .await
        .unwrap_err()
        .to_string()
        .contains("corrupt"));

    let retained_dir = TempDir::new().unwrap();
    let retained_path = retained_dir.path().to_str().unwrap();
    let table = open_or_create_table(retained_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let table = table
        .set_tbl_properties()
        .with_properties(HashMap::from([(
            SET_TRANSACTION_RETENTION.to_string(),
            "interval 30 days".to_string(),
        )]))
        .await
        .unwrap();
    let error = get_coordinated_cursor(&table, "ldb-c3-test")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains(SET_TRANSACTION_RETENTION), "got: {error}");
}

#[tokio::test]
async fn coordinated_cursor_rejects_versions_outside_delta_range() {
    use crate::connector::CoordinatedCommitCursor;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    assert!(commit_adds_coordinated(
        &table,
        vec![],
        "ldb-c3-test",
        CoordinatedCommitCursor {
            checkpoint_id: u64::MAX,
            fencing_token: 1,
        },
        test_publication_deadline(),
    )
    .await
    .is_err());
    assert!(commit_adds_coordinated(
        &table,
        vec![],
        "ldb-c3-test",
        CoordinatedCommitCursor {
            checkpoint_id: 1,
            fencing_token: u64::MAX,
        },
        test_publication_deadline(),
    )
    .await
    .is_err());
}

#[tokio::test]
async fn coordinated_publication_rejects_missing_object_without_advancing_cursor() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let adds = staged_adds(&table, test_batch(2)).await;
    let path = deltalake::Path::parse(&adds[0].path).unwrap();
    table.object_store().delete(&path).await.unwrap();

    let error = commit_adds_coordinated(
        &table,
        adds,
        "ldb-c3-missing",
        coordinated_cursor(1),
        test_publication_deadline(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("HEAD Delta coordinated object"),
        "got: {error}"
    );
    assert_coordinated_cursor_absent(&table, "ldb-c3-missing").await;
}

#[tokio::test]
async fn coordinated_publication_rejects_truncated_object_without_advancing_cursor() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let adds = staged_adds(&table, test_batch(2)).await;
    let path = deltalake::Path::parse(&adds[0].path).unwrap();
    table
        .object_store()
        .put(&path, bytes::Bytes::from_static(b"x").into())
        .await
        .unwrap();

    let error = commit_adds_coordinated(
        &table,
        adds,
        "ldb-c3-truncated",
        coordinated_cursor(1),
        test_publication_deadline(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("size mismatch"), "got: {error}");
    assert_coordinated_cursor_absent(&table, "ldb-c3-truncated").await;
}

#[tokio::test]
async fn coordinated_publication_rejects_duplicate_object_without_advancing_cursor() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let mut adds = staged_adds(&table, test_batch(2)).await;
    adds.push(adds[0].clone());

    let error = commit_adds_coordinated(
        &table,
        adds,
        "ldb-c3-duplicate",
        coordinated_cursor(1),
        test_publication_deadline(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("duplicate"), "got: {error}");
    assert_coordinated_cursor_absent(&table, "ldb-c3-duplicate").await;
}

#[tokio::test]
async fn coordinated_publication_rejects_invalid_descriptors_without_advancing_cursor() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let add = staged_adds(&table, test_batch(2)).await.remove(0);

    let mut traversal = add.clone();
    traversal.path = "../outside.parquet".to_string();
    let traversal_error = commit_adds_coordinated(
        &table,
        vec![traversal],
        "ldb-c3-invalid",
        coordinated_cursor(1),
        test_publication_deadline(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        traversal_error.contains("invalid Delta coordinated Add path"),
        "got: {traversal_error}"
    );

    let mut absolute = add.clone();
    absolute.path = "/outside.parquet".to_string();
    let absolute_error = commit_adds_coordinated(
        &table,
        vec![absolute],
        "ldb-c3-invalid",
        coordinated_cursor(1),
        test_publication_deadline(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(absolute_error.contains("relative object path"));

    let mut negative_size = add;
    negative_size.size = -1;
    let size_error = commit_adds_coordinated(
        &table,
        vec![negative_size],
        "ldb-c3-invalid",
        coordinated_cursor(1),
        test_publication_deadline(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(size_error.contains("negative size"));
    assert_coordinated_cursor_absent(&table, "ldb-c3-invalid").await;
}

#[tokio::test]
async fn coordinated_publication_rejects_metadata_change_after_staging() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let stale_table = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let adds = staged_adds(&stale_table, test_batch(2)).await;

    // Mutate live write metadata through another handle after the Add was staged.
    let current = stale_table
        .clone()
        .set_tbl_properties()
        .with_properties(HashMap::from([(
            "delta.appendOnly".to_string(),
            "true".to_string(),
        )]))
        .await
        .unwrap();
    let error = commit_adds_coordinated(
        &stale_table,
        adds,
        "ldb-c3-metadata-race",
        coordinated_cursor(1),
        test_publication_deadline(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("table binding changed"), "got: {error}");
    assert_coordinated_cursor_absent(&current, "ldb-c3-metadata-race").await;
}

#[tokio::test]
async fn coordinated_publication_rejects_schema_change_after_staging() {
    use deltalake::kernel::{PrimitiveType, StructField};

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let stale_table = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let adds = staged_adds(&stale_table, test_batch(2)).await;
    let current = stale_table
        .clone()
        .add_columns()
        .with_fields([StructField::nullable(
            "added_after_staging",
            PrimitiveType::String,
        )])
        .await
        .unwrap();

    let error = commit_adds_coordinated(
        &stale_table,
        adds,
        "ldb-c3-schema-race",
        coordinated_cursor(1),
        test_publication_deadline(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("table binding changed"), "got: {error}");
    assert_coordinated_cursor_absent(&current, "ldb-c3-schema-race").await;
}

#[tokio::test]
async fn coordinated_publication_rejects_protocol_change_after_staging() {
    use deltalake::kernel::TableFeatures;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let stale_table = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let adds = staged_adds(&stale_table, test_batch(2)).await;
    let current = stale_table
        .clone()
        .add_feature()
        .with_feature(TableFeatures::ChangeDataFeed)
        .with_allow_protocol_versions_increase(true)
        .await
        .unwrap();

    let error = commit_adds_coordinated(
        &stale_table,
        adds,
        "ldb-c3-protocol-race",
        coordinated_cursor(1),
        test_publication_deadline(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("table binding changed"), "got: {error}");
    assert_coordinated_cursor_absent(&current, "ldb-c3-protocol-race").await;
}

#[tokio::test]
async fn coordinated_publication_rejects_drop_and_recreate_after_staging() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let stale_table = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let old_binding = coordinated_table_binding(&stale_table).unwrap();
    let adds = staged_adds(&stale_table, test_batch(2)).await;

    std::fs::rename(
        temp_dir.path().join("_delta_log"),
        temp_dir.path().join("_replaced_delta_log"),
    )
    .unwrap();
    let current = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    assert_ne!(
        old_binding.table_id,
        coordinated_table_binding(&current).unwrap().table_id
    );

    let error = commit_adds_coordinated(
        &stale_table,
        adds,
        "ldb-c3-recreate-race",
        coordinated_cursor(1),
        test_publication_deadline(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("table binding changed"), "got: {error}");
    assert_coordinated_cursor_absent(&current, "ldb-c3-recreate-race").await;
}

#[tokio::test]
async fn coordinated_descriptor_limits_accept_max_and_reject_max_plus_one() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let template = staged_adds(&table, test_batch(1)).await.remove(0);
    let make_adds = |count: usize| {
        (0..count)
            .map(|index| {
                let mut add = template.clone();
                add.path = format!("limit-{index}.parquet");
                add
            })
            .collect::<Vec<_>>()
    };
    let deadline = || tokio::time::Instant::now() + Duration::from_secs(30);

    assert!(validate_coordinated_descriptors(
        &make_adds(MAX_COORDINATED_ADD_ACTIONS - 1),
        &[],
        deadline(),
    )
    .is_ok());
    assert!(validate_coordinated_descriptors(
        &make_adds(MAX_COORDINATED_ADD_ACTIONS),
        &[],
        deadline(),
    )
    .is_ok());
    assert!(validate_coordinated_descriptors(
        &make_adds(MAX_COORDINATED_ADD_ACTIONS + 1),
        &[],
        deadline(),
    )
    .is_err());

    let per_payload = crate::connector::MAX_COORDINATED_COMMIT_PAYLOAD_BYTES;
    assert!(validate_descriptor_batch_lengths(
        [per_payload, per_payload, per_payload, per_payload - 1],
        deadline(),
    )
    .is_ok());
    assert!(validate_descriptor_batch_lengths(
        [per_payload, per_payload, per_payload, per_payload],
        deadline(),
    )
    .is_ok());
    assert!(validate_descriptor_batch_lengths(
        [per_payload, per_payload, per_payload, per_payload, 1],
        deadline(),
    )
    .is_err());
}

#[tokio::test]
async fn coordinated_descriptor_field_limits_and_windows_paths_are_platform_independent() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let template = staged_adds(&table, test_batch(1)).await.remove(0);
    let deadline = || tokio::time::Instant::now() + Duration::from_secs(30);
    let validate = |adds: &[deltalake::kernel::Add], partitions: &[String]| {
        validate_coordinated_descriptors(adds, partitions, deadline())
    };

    for path in [
        "_DELTA_LOG/file.parquet",
        "_delta_log./file.parquet",
        "folder./file.parquet",
        "folder /file.parquet",
        "%2e%2e/file.parquet",
        "%252e%252e/file.parquet",
        "folder%2fescape.parquet",
        "%43%3a/file.parquet",
        "%5c%5cserver/file.parquet",
    ] {
        let mut add = template.clone();
        add.path = path.into();
        assert!(
            validate(&[add], &[]).is_err(),
            "unsafe path was accepted: {path}"
        );
    }

    let mut first = template.clone();
    first.path = "Folder/File.PARQUET".into();
    let mut duplicate = template.clone();
    duplicate.path = "folder/file.parquet".into();
    assert!(validate(&[first, duplicate], &[]).is_err());

    let mut first = template.clone();
    first.path = "folder/file.parquet".into();
    let mut encoded_duplicate = template.clone();
    encoded_duplicate.path = "folder/%66ile.parquet".into();
    assert!(validate(&[first, encoded_duplicate], &[]).is_err());

    let mut path_at_limit = template.clone();
    path_at_limit.path = format!("{}.parquet", "a".repeat(MAX_COORDINATED_PATH_BYTES - 8));
    assert_eq!(path_at_limit.path.len(), MAX_COORDINATED_PATH_BYTES);
    assert!(validate(&[path_at_limit.clone()], &[]).is_ok());
    path_at_limit.path.insert(0, 'a');
    assert!(validate(&[path_at_limit], &[]).is_err());

    let json_at_limit = |limit: usize| {
        let prefix = "{\"value\":\"";
        let suffix = "\"}";
        format!(
            "{prefix}{}{suffix}",
            "x".repeat(limit - prefix.len() - suffix.len())
        )
    };
    let mut stats = template.clone();
    stats.stats = Some(json_at_limit(MAX_COORDINATED_STATS_BYTES));
    assert!(validate(&[stats.clone()], &[]).is_ok());
    let insert_at = stats.stats.as_ref().unwrap().len() - 2;
    stats.stats.as_mut().unwrap().insert(insert_at, 'x');
    assert!(validate(&[stats], &[]).is_err());

    let partitions = vec!["p".to_string()];
    let mut partitioned = template.clone();
    partitioned.partition_values.insert(
        "p".into(),
        Some("x".repeat(MAX_COORDINATED_PARTITION_BYTES - 1)),
    );
    assert!(validate(&[partitioned.clone()], &partitions).is_ok());
    partitioned
        .partition_values
        .get_mut("p")
        .unwrap()
        .as_mut()
        .unwrap()
        .push('x');
    assert!(validate(&[partitioned], &partitions).is_err());

    let mut with_deletion_vector = template;
    with_deletion_vector.deletion_vector = Some(deltalake::kernel::DeletionVectorDescriptor {
        storage_type: deltalake::kernel::StorageType::Inline,
        path_or_inline_dv: "00000".into(),
        offset: None,
        size_in_bytes: 1,
        cardinality: 1,
    });
    assert!(validate(&[with_deletion_vector], &[]).is_err());
}

#[tokio::test]
async fn coordinated_publication_accepts_recent_object_and_advances_cursor() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();
    let adds = staged_adds(&table, test_batch(2)).await;
    let cursor = coordinated_cursor(1);

    commit_adds_coordinated(
        &table,
        adds,
        "ldb-c3-recent",
        cursor,
        test_publication_deadline(),
    )
    .await
    .unwrap();
    let reopened = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    assert_eq!(
        get_coordinated_cursor(&reopened, "ldb-c3-recent")
            .await
            .unwrap(),
        Some(cursor)
    );
}

#[tokio::test]
async fn test_auto_flush_writes_data() {
    use super::super::delta::DeltaLakeSink;
    use super::super::delta_config::DeltaLakeSinkConfig;
    use crate::config::ConnectorConfig;
    use crate::connector::SinkConnector;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    // Configure a small buffer to trigger auto-flush.
    let mut sink_config = DeltaLakeSinkConfig::new(table_path);
    sink_config.max_buffer_records = 10;
    let mut sink = DeltaLakeSink::with_schema(sink_config, test_schema());

    let connector_config = ConnectorConfig::new("delta-lake");
    sink.open(&connector_config).await.unwrap();

    // Write 25 rows — should trigger auto-flush after 10.
    let batch = test_batch(25);
    sink.write_batch(&batch).await.unwrap();

    // Durably flush anything below the automatic threshold.
    sink.flush().await.unwrap();
    sink.close().await.unwrap();

    // Verify all 25 rows are in the Delta table.
    let mut table = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let latest = get_latest_version(&mut table).await.unwrap();
    assert!(latest >= 1, "should have at least 1 version");

    let (batches, _) = read_batches_at_version(&mut table, latest, 10000)
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_rows, 25,
        "all 25 rows should be written, not dropped by auto-flush"
    );
}

#[tokio::test]
async fn test_schema_evolution_adds_column() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let schema_v1 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let table = open_or_create_table(table_path, HashMap::new(), Some(&schema_v1))
        .await
        .unwrap();

    // Write batch with 2 columns.
    let batch_v1 = RecordBatch::try_new(
        schema_v1.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();
    let (table, _) = write_batches(
        table,
        vec![batch_v1],
        SaveMode::Append,
        None,
        true, // schema_evolution enabled
        None,
        None,
    )
    .await
    .unwrap();

    // Write batch with 3 columns (extra "score" column).
    let schema_v2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));
    let batch_v2 = RecordBatch::try_new(
        schema_v2,
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["c"])),
            Arc::new(Float64Array::from(vec![99.5])),
        ],
    )
    .unwrap();
    let (table, _) = write_batches(
        table,
        vec![batch_v2],
        SaveMode::Append,
        None,
        true,
        None,
        None,
    )
    .await
    .unwrap();

    // Verify table schema now has all 3 columns.
    let final_schema = get_table_schema(&table).unwrap();
    assert_eq!(final_schema.fields().len(), 3);
    assert_eq!(final_schema.field(0).name(), "id");
    assert_eq!(final_schema.field(1).name(), "name");
    assert_eq!(final_schema.field(2).name(), "score");

    // Verify all rows are readable.
    let mut read_table = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let (batches, _) = read_batches_at_version(&mut read_table, 2, 10000)
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3);
}
