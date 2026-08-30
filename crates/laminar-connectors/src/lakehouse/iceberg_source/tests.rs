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

#[cfg(feature = "iceberg-core")]
fn replay_cursor(snapshot_id: i64) -> IcebergSourceCursorV1 {
    IcebergSourceCursorV1 {
        version: 1,
        catalog_identity: "0".repeat(64),
        table_uuid: "018f0f9d-7b2f-7a61-b72d-f4be1c7f43e1".into(),
        table_identifier: "test.events".into(),
        table_ref: "main".into(),
        origin: IcebergSourceCursorOriginV1::Snapshot,
        snapshot_id,
        sequence_number: snapshot_id,
        read_schema_id: 0,
        metadata_location: "s3://test/wh/test/events/metadata/v1.json".into(),
    }
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
        assert!(!source.replay_unit_in_progress);
        assert!(!source.scan_failed);
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

#[cfg(feature = "iceberg-core")]
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

#[cfg(not(feature = "iceberg-core"))]
#[tokio::test]
async fn source_without_iceberg_core_fails_before_catalog_io() {
    let mut source = IcebergSource::new(test_source_config(), None);

    let error = source
        .start(
            SourceStart::new(
                connector_config(),
                SourcePosition::Initial,
                crate::connector::DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("a build without iceberg-core must fail before catalog access");

    assert!(matches!(error, ConnectorError::FeatureUnsupported(_)));
    assert_eq!(source.state, ConnectorState::Failed);
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
#[test]
fn split_snapshot_batches_block_barriers_until_the_completed_cursor() {
    use arrow_array::Int64Array;

    let mut source = IcebergSource::new(test_source_config(), None);
    source.install_cursor(replay_cursor(1)).unwrap();
    source.pending = Some(PendingBatch {
        batch: RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "id",
                arrow_schema::DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap(),
        completed_cursor: Some(replay_cursor(2)),
    });

    for expected_snapshot in [1, 1] {
        let mut batch = source.emit_pending(1).unwrap().unwrap();
        let crate::connector::SourceBatchCursor::Complete(checkpoint) =
            batch.take_cursor().unwrap()
        else {
            panic!("Iceberg batches require complete snapshot cursors");
        };
        assert_eq!(
            IcebergSourceCursorV1::from_checkpoint(&checkpoint)
                .unwrap()
                .snapshot_id,
            expected_snapshot
        );
        assert!(source.try_checkpoint().unwrap().is_none());
    }

    let mut final_batch = source.emit_pending(1).unwrap().unwrap();
    let crate::connector::SourceBatchCursor::Complete(checkpoint) =
        final_batch.take_cursor().unwrap()
    else {
        panic!("Iceberg batches require complete snapshot cursors");
    };
    assert_eq!(
        IcebergSourceCursorV1::from_checkpoint(&checkpoint)
            .unwrap()
            .snapshot_id,
        2
    );
    assert!(source.try_checkpoint().unwrap().is_some());
    assert_eq!(
        source.checkpoint_unavailable_policy(),
        SourceCheckpointUnavailablePolicy::PollToReplayBoundary
    );
}

#[cfg(feature = "iceberg-core")]
#[tokio::test]
async fn scan_failures_cannot_complete_or_retry_a_partial_replay_unit() {
    async fn failed_scan(source: &mut IcebergSource) -> ConnectorError {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        sender
            .send(Err(ConnectorError::ReadError("injected".into())))
            .await
            .unwrap();
        drop(sender);
        source.scan = Some(ScanTask {
            receiver,
            handle: tokio::spawn(async {}),
            started_at: Instant::now(),
        });
        match source.next_scan_output().await {
            Err(error) => error,
            Ok(_) => panic!("injected scan failure was not returned"),
        }
    }

    let mut before_output = IcebergSource::new(test_source_config(), None);
    let error = failed_scan(&mut before_output).await;
    assert!(error.is_transient());
    assert!(before_output.next_scan_output().await.unwrap().is_none());
    assert!(!before_output.bounded_snapshot_complete);

    let mut after_output = IcebergSource::new(test_source_config(), None);
    after_output.state = ConnectorState::Running;
    after_output.replay_unit_in_progress = true;
    let error = failed_scan(&mut after_output).await;
    assert!(error.to_string().contains("LDB-ICEBERG-PARTIAL-SCAN"));
    assert!(!error.is_transient());
    assert_eq!(after_output.state, ConnectorState::Failed);
    assert!(after_output.try_checkpoint().unwrap().is_none());

    let retry = after_output.poll_batch(1).await.unwrap_err();
    assert!(retry.to_string().contains("LDB-ICEBERG-PARTIAL-SCAN"));
    assert!(!retry.is_transient());
    assert!(after_output.scan.is_some());
    after_output.close().await.unwrap();

    let mut after_output_join_failure = IcebergSource::new(test_source_config(), None);
    after_output_join_failure.state = ConnectorState::Running;
    after_output_join_failure.replay_unit_in_progress = true;
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    drop(sender);
    let handle = tokio::spawn(std::future::pending());
    handle.abort();
    after_output_join_failure.scan = Some(ScanTask {
        receiver,
        handle,
        started_at: Instant::now(),
    });

    let error = match after_output_join_failure.next_scan_output().await {
        Err(error) => error,
        Ok(_) => panic!("cancelled scan task was not reported"),
    };
    assert!(error.to_string().contains("LDB-ICEBERG-PARTIAL-SCAN"));
    assert_eq!(after_output_join_failure.state, ConnectorState::Failed);
    assert!(after_output_join_failure
        .try_checkpoint()
        .unwrap()
        .is_none());
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
    assert!(!error.is_transient());
    source.close().await.unwrap();
}

#[cfg(feature = "iceberg-core")]
#[tokio::test]
async fn append_without_bootstrap_rejects_malformed_filter_before_cursor_install() {
    use crate::lakehouse::iceberg::test_support::create_test_table;

    let fixture = create_test_table(false).await;
    let mut config = test_source_config();
    config.read_mode = IcebergReadMode::Append;
    config.bootstrap = IcebergReadBootstrap::None;
    config.filter = Some("{".into());
    let mut source = IcebergSource::new(config, None);
    source.table = Some(fixture.table);

    let error = source.start_initial_scan().unwrap_err();
    assert!(error.to_string().contains("FILTER-SYNTAX"));
    assert!(source.cursor.is_none());
    assert!(source.scan.is_none());
}

#[cfg(feature = "iceberg-core")]
#[tokio::test]
async fn append_without_bootstrap_rejects_unbound_filter_before_cursor_install() {
    use iceberg::expr::Reference;

    use crate::lakehouse::iceberg::test_support::create_test_table;

    let fixture = create_test_table(false).await;
    let mut config = test_source_config();
    config.read_mode = IcebergReadMode::Append;
    config.bootstrap = IcebergReadBootstrap::None;
    config.filter = Some(serde_json::to_string(&Reference::new("missing").is_null()).unwrap());
    let mut source = IcebergSource::new(config, None);
    source.table = Some(fixture.table);

    let error = source.start_initial_scan().unwrap_err();
    assert!(error.to_string().contains("FILTER-BIND"));
    assert!(source.cursor.is_none());
    assert!(source.scan.is_none());
}

#[cfg(feature = "iceberg-core")]
#[tokio::test]
async fn empty_append_checkpoint_resumes_before_the_first_snapshot() {
    use arrow_array::Int64Array;

    use crate::lakehouse::iceberg::test_support::{append_rows, create_test_table};

    let fixture = create_test_table(false).await;
    let mut config = test_source_config();
    config.read_mode = IcebergReadMode::Append;
    config.bootstrap = IcebergReadBootstrap::None;
    let mut initial = IcebergSource::new(config.clone(), None);
    initial.table = Some(fixture.table.clone());
    initial.start_initial_scan().unwrap();

    let checkpoint = initial.try_checkpoint().unwrap().unwrap();
    let empty_cursor = IcebergSourceCursorV1::from_checkpoint(&checkpoint).unwrap();
    assert!(empty_cursor.is_empty_table());
    assert!(initial.scan.is_none());
    initial.close().await.unwrap();

    let (current, _) = append_rows(&fixture, &fixture.table, 1, &[(1, Some("first"))]).await;
    empty_cursor.validate_binding(&config, &current).unwrap();
    let retained_schema = empty_cursor.retained_schema(&current).unwrap();
    let mut resumed = IcebergSource::new(config, None);
    resumed.bind_read_schema(&retained_schema, None).unwrap();
    resumed.catalog = Some(Arc::clone(&fixture.catalog));
    resumed.table = Some(current);
    resumed.install_cursor(empty_cursor).unwrap();

    let batch = resumed.poll_batch(8_192).await.unwrap().unwrap();
    let ids = batch
        .records
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1]);
    assert!(
        !IcebergSourceCursorV1::from_checkpoint(&resumed.checkpoint())
            .unwrap()
            .is_empty_table()
    );
    resumed.close().await.unwrap();
}

#[cfg(feature = "iceberg-core")]
#[tokio::test]
async fn admitted_filter_is_applied_by_the_scan() {
    use arrow_array::Int64Array;
    use iceberg::expr::Reference;

    use crate::lakehouse::iceberg::test_support::{append_rows, create_test_table};

    let fixture = create_test_table(false).await;
    let (table, _) =
        append_rows(&fixture, &fixture.table, 1, &[(1, None), (2, Some("drop"))]).await;
    let mut config = test_source_config();
    config.filter =
        Some(serde_json::to_string(&(!Reference::new("category").is_not_null())).unwrap());
    let mut source = IcebergSource::new(config, None);
    source.table = Some(table);
    source.start_initial_scan().unwrap();

    let batch = source.poll_batch(8_192).await.unwrap().unwrap();
    let ids = batch
        .records
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1]);
    assert!(source.poll_batch(8_192).await.unwrap().is_none());
    source.close().await.unwrap();
}

#[cfg(feature = "iceberg-core")]
#[tokio::test]
async fn append_file_limit_counts_only_files_added_after_the_cursor() {
    use arrow_array::Int64Array;
    use iceberg::expr::Reference;

    use crate::lakehouse::iceberg::test_support::{append_rows, create_test_table};

    let fixture = create_test_table(false).await;
    let (first, _) = append_rows(&fixture, &fixture.table, 1, &[(1, Some("old"))]).await;
    let mut config = test_source_config();
    config.read_mode = IcebergReadMode::Append;
    config.bootstrap = IcebergReadBootstrap::None;
    config.max_planned_files = 1;
    config.filter = Some(serde_json::to_string(&Reference::new("category").is_null()).unwrap());
    let mut source = IcebergSource::new(config, None);
    source.catalog = Some(Arc::clone(&fixture.catalog));
    source.table = Some(first.clone());
    source.start_initial_scan().unwrap();

    let _ = append_rows(&fixture, &first, 2, &[(2, None), (3, Some("filtered"))]).await;
    let batch = source.poll_batch(8_192).await.unwrap().unwrap();
    let ids = batch
        .records
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[2]);
    assert!(source.poll_batch(8_192).await.unwrap().is_none());
    source.close().await.unwrap();
}

#[cfg(feature = "iceberg-core")]
#[tokio::test]
async fn append_direct_tasks_read_partitioned_files() {
    use arrow_array::StringArray;

    use crate::lakehouse::iceberg::test_support::{append_rows, create_test_table};

    let fixture = create_test_table(true).await;
    let (first, _) = append_rows(&fixture, &fixture.table, 1, &[(1, Some("old"))]).await;
    let mut config = test_source_config();
    config.read_mode = IcebergReadMode::Append;
    config.bootstrap = IcebergReadBootstrap::None;
    let mut source = IcebergSource::new(config, None);
    source.catalog = Some(Arc::clone(&fixture.catalog));
    source.table = Some(first.clone());
    source.start_initial_scan().unwrap();

    let _ = append_rows(&fixture, &first, 2, &[(2, Some("new"))]).await;
    let batch = source.poll_batch(8_192).await.unwrap().unwrap();
    let categories = batch
        .records
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(categories.value(0), "new");
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
