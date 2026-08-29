use super::*;
use crate::config::ConnectorConfig;

fn connector_config() -> ConnectorConfig {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("catalog.warehouse", "s3://test/wh");
    config.set("namespace", "test");
    config.set("table.name", "events");
    config
}

fn test_source_config() -> IcebergSourceConfig {
    IcebergSourceConfig::from_config(&connector_config()).unwrap()
}

#[test]
fn new_source_has_no_uncommitted_scan_state() {
    let source = IcebergSource::new(test_source_config(), None);
    assert!(source.schema.is_none());
    #[cfg(feature = "iceberg-core")]
    {
        assert!(source.cursor.is_none());
        assert!(source.pending.is_none());
        assert!(source.scan.is_none());
    }
    assert_eq!(
        source.checkpoint.get_metadata("connector_type"),
        Some("iceberg")
    );
}

#[tokio::test]
async fn snapshot_resume_fails_before_catalog_io() {
    let mut source = IcebergSource::new(test_source_config(), None);
    let error = source
        .start(
            SourceStart::new(
                connector_config(),
                SourcePosition::Resume {
                    attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(11),
                    checkpoint: SourceCheckpoint::new(),
                },
                crate::connector::DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("bounded snapshot mode must not accept append cursors");
    assert!(error.to_string().contains("read.mode=append"));
    assert_eq!(source.state, ConnectorState::Created);
}

#[tokio::test]
async fn changelog_fails_before_catalog_io() {
    let mut config = connector_config();
    config.set("read.mode", "changelog");
    let mut source = IcebergSource::new(IcebergSourceConfig::from_config(&config).unwrap(), None);
    let error = source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                crate::connector::DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("unsupported changelog must fail before opening the catalog");
    assert!(matches!(error, ConnectorError::FeatureUnsupported(_)));
    assert_eq!(source.state, ConnectorState::Created);
}

#[tokio::test]
async fn malformed_declared_schema_fails_before_catalog_io() {
    let mut config = connector_config();
    config.set("_arrow_schema", "not-arrow-ipc");
    let mut source = IcebergSource::new(test_source_config(), None);
    let error = source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                crate::connector::DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("malformed engine schema must fail before catalog access");
    assert!(error.to_string().contains("_arrow_schema"));
    assert_eq!(source.state, ConnectorState::Created);
}

#[test]
fn contracts_match_typed_read_modes() {
    let source = IcebergSource::new(test_source_config(), None);
    let snapshot = source.contract(&connector_config()).unwrap();
    assert_eq!(snapshot.consistency, SourceConsistency::Ephemeral);
    assert_eq!(snapshot.topology, SourceTopology::Singleton);

    let mut append_config = connector_config();
    append_config.set("read.mode", "append");
    let append = source.contract(&append_config).unwrap();
    assert_eq!(append.consistency, SourceConsistency::Replayable);
    assert_eq!(append.input_mode, SourceInputMode::AppendOnly);
}

#[cfg(feature = "iceberg-core")]
#[tokio::test]
async fn completed_scan_observes_files_and_storage_bytes() {
    use crate::lakehouse::iceberg::test_support::{append_rows, create_test_table};

    let fixture = create_test_table(false).await;
    let (table, _) = append_rows(&fixture, &fixture.table, 1, &[(1, Some("read"))]).await;
    let mut source = IcebergSource::new(test_source_config(), None);
    source.table = Some(table);
    source.start_initial_scan().unwrap();

    while source.poll_batch(1).await.unwrap().is_some() {}

    assert_eq!(source.metrics.read_files.get(), 1);
    assert!(source.metrics.read_storage_bytes.get() > 0);
}

#[cfg(feature = "iceberg-core")]
#[tokio::test]
async fn full_snapshot_scan_enforces_planned_file_limit() {
    use crate::lakehouse::iceberg::test_support::{append_rows, create_test_table};

    let fixture = create_test_table(false).await;
    let (first, _) = append_rows(&fixture, &fixture.table, 1, &[(1, None)]).await;
    let (second, _) = append_rows(&fixture, &first, 2, &[(2, None)]).await;
    let mut config = test_source_config();
    config.max_planned_files = 1;
    let mut source = IcebergSource::new(config, None);
    source.table = Some(second);
    source.start_initial_scan().unwrap();

    let error = loop {
        match source.poll_batch(8_192).await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("two-file snapshot bypassed read.max.planned.files"),
            Err(error) => break error,
        }
    };
    assert!(
        error.to_string().contains("SCAN-FILE-LIMIT"),
        "got: {error}"
    );
    source.close().await.unwrap();
}

#[cfg(feature = "iceberg-core")]
#[tokio::test]
async fn append_schema_binding_ignores_later_nullable_columns() {
    use std::time::Duration;

    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use iceberg::spec::{PrimitiveType, Type};
    use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};

    use crate::lakehouse::iceberg::test_support::{append_batch, append_rows, create_test_table};

    let fixture = create_test_table(false).await;
    let (root, _) = append_rows(&fixture, &fixture.table, 1, &[(1, Some("root"))]).await;
    let root_schema_id = root.metadata().current_schema_id();
    let mut config = test_source_config();
    config.read_mode = IcebergReadMode::Append;
    config.bootstrap = IcebergReadBootstrap::Initial;
    config.poll_interval = Duration::from_millis(1);
    let mut source = IcebergSource::new(config, None);
    source.catalog = Some(Arc::clone(&fixture.catalog));
    source.table = Some(root.clone());
    source.start_initial_scan().unwrap();

    let first = source.poll_batch(8_192).await.unwrap().unwrap();
    assert_eq!(first.records.num_columns(), 2);
    assert!(source.poll_batch(8_192).await.unwrap().is_none());

    let transaction = Transaction::new(&root);
    let transaction = transaction
        .update_schema()
        .add_column(AddColumn::optional(
            "later",
            Type::Primitive(PrimitiveType::String),
        ))
        .apply(transaction)
        .unwrap();
    let evolved = transaction.commit(fixture.catalog.as_ref()).await.unwrap();
    let evolved_schema =
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&evolved.current_schema_ref()).unwrap());
    let evolved_batch = RecordBatch::try_new(
        evolved_schema,
        vec![
            Arc::new(Int64Array::from(vec![2])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("next")])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("ignored")])) as ArrayRef,
        ],
    )
    .unwrap();
    let _ = append_batch(&fixture, &evolved, 2, evolved_batch).await;

    tokio::time::sleep(Duration::from_millis(2)).await;
    let appended = source.poll_batch(8_192).await.unwrap().unwrap();
    assert_eq!(appended.records.num_columns(), 2);
    assert_eq!(appended.records.schema(), first.records.schema());
    assert_eq!(
        appended
            .records
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
    let cursor = IcebergSourceCursorV1::from_checkpoint(&source.checkpoint()).unwrap();
    assert_eq!(cursor.read_schema_id, root_schema_id);
    assert_eq!(
        cursor
            .retained_schema(source.table.as_ref().unwrap())
            .unwrap()
            .schema_id(),
        root_schema_id
    );
    source.close().await.unwrap();
}
