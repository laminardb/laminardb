#[cfg(feature = "delta-lake")]
use super::super::delta_config::DeltaCatalogType;
use super::*;
use arrow_array::{Float64Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]))
}

fn test_config() -> DeltaLakeSinkConfig {
    use std::sync::atomic::{AtomicU64, Ordering};
    // Unique per call so a leftover dir from a prior run can't make a later run
    // hit an existing path (the hardcoded "8f3a" suffix caused flakes).
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let uniq = format!(
        "{}_{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    #[cfg(unix)]
    let path = format!("/tmp/delta_test_nonexistent_{uniq}");
    #[cfg(windows)]
    let path = format!("C:\\delta_test_nonexistent_{uniq}");
    DeltaLakeSinkConfig::new(&path)
}

fn upsert_config() -> DeltaLakeSinkConfig {
    let mut cfg = test_config();
    cfg.write_mode = DeltaWriteMode::Upsert;
    cfg.merge_key_columns = vec!["id".to_string()];
    cfg
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

// ── Constructor tests ──

#[cfg(feature = "delta-lake")]
#[test]
fn typed_delta_failures_are_classified_without_message_heuristics() {
    use deltalake::kernel::transaction::{CommitConflictError, TransactionError};

    let conflict =
        classify_delta_attempt_error(super::super::delta_io::DeltaWriteAttemptError::Delta(
            deltalake::DeltaTableError::Transaction {
                source: TransactionError::CommitConflict(CommitConflictError::ConcurrentDeleteRead),
            },
        ));
    assert!(matches!(conflict, ConnectorError::WriteError(_)));
    assert!(conflict.is_transient());

    let false_positive =
        classify_delta_attempt_error(super::super::delta_io::DeltaWriteAttemptError::Delta(
            deltalake::DeltaTableError::InvalidData {
                message: "concurrent conflict while decoding user data".into(),
            },
        ));
    assert!(matches!(
        false_positive,
        ConnectorError::OutcomeUnknown {
            retryable: false,
            ..
        }
    ));

    let protocol_change =
        classify_delta_attempt_error(super::super::delta_io::DeltaWriteAttemptError::Delta(
            deltalake::DeltaTableError::Transaction {
                source: TransactionError::CommitConflict(CommitConflictError::ProtocolChanged(
                    "concurrent conflict".into(),
                )),
            },
        ));
    assert!(!protocol_change.is_transient());

    let local =
        classify_delta_attempt_error(super::super::delta_io::DeltaWriteAttemptError::Local(
            ConnectorError::ConfigurationError("invalid merge".into()),
        ));
    assert!(matches!(local, ConnectorError::ConfigurationError(_)));
}

#[cfg(feature = "delta-lake")]
#[test]
fn object_store_retryability_uses_typed_positive_allowlist() {
    use object_store::client::{HttpError, HttpErrorKind};

    let transient =
        classify_delta_attempt_error(super::super::delta_io::DeltaWriteAttemptError::Delta(
            deltalake::DeltaTableError::ObjectStore {
                source: deltalake::ObjectStoreError::Generic {
                    store: "test",
                    source: Box::new(HttpError::new(
                        HttpErrorKind::Timeout,
                        std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"),
                    )),
                },
            },
        ));
    assert!(matches!(
        transient,
        ConnectorError::OutcomeUnknown {
            retryable: true,
            ..
        }
    ));

    let permanent =
        classify_delta_attempt_error(super::super::delta_io::DeltaWriteAttemptError::Delta(
            deltalake::DeltaTableError::ObjectStore {
                source: deltalake::ObjectStoreError::PermissionDenied {
                    path: "table".into(),
                    source: Box::new(HttpError::new(
                        HttpErrorKind::Timeout,
                        std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"),
                    )),
                },
            },
        ));
    assert!(matches!(
        permanent,
        ConnectorError::OutcomeUnknown {
            retryable: false,
            ..
        }
    ));

    let unknown =
        classify_delta_attempt_error(super::super::delta_io::DeltaWriteAttemptError::Delta(
            deltalake::DeltaTableError::ObjectStore {
                source: deltalake::ObjectStoreError::Generic {
                    store: "test",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "untyped timeout",
                    )),
                },
            },
        ));
    assert!(matches!(
        unknown,
        ConnectorError::OutcomeUnknown {
            retryable: false,
            ..
        }
    ));
}

#[cfg(feature = "delta-lake")]
#[test]
fn tracked_delta_future_owns_guard_before_first_poll() {
    let sink = DeltaLakeSink::new(test_config(), None);
    let terminal = sink.terminal_task_tracker().unwrap();
    let guard = sink.task_owner.track().unwrap();
    let task = run_tracked_delta_task(guard, std::future::pending::<()>());

    drop(sink);
    assert!(!terminal.is_terminated());
    drop(task);
    assert!(terminal.is_terminated());
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn dropped_join_handle_does_not_publish_false_terminal_proof() {
    let sink = DeltaLakeSink::new(test_config(), None);
    let terminal = sink.terminal_task_tracker().unwrap();
    let guard = sink.task_owner.track().unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let join = tokio::spawn(run_tracked_delta_task(guard, async move {
        let _ = started_tx.send(());
        let _ = release_rx.await;
    }));
    started_rx.await.unwrap();
    drop(join);
    drop(sink);
    assert!(!terminal.is_terminated());

    let _ = release_tx.send(());
    terminal.wait_terminated().await;
    assert!(terminal.is_terminated());
}

#[test]
fn test_new_defaults() {
    let sink = DeltaLakeSink::new(test_config(), None);
    assert_eq!(sink.state(), ConnectorState::Created);
    assert_eq!(sink.current_epoch(), 0);
    assert_eq!(sink.buffered_rows(), 0);
    assert_eq!(sink.buffered_bytes(), 0);
    assert_eq!(sink.delta_version(), 0);
    assert!(sink.schema.is_none());
}

#[test]
fn test_with_schema() {
    let schema = test_schema();
    let sink = DeltaLakeSink::with_schema(test_config(), schema.clone());
    assert_eq!(sink.schema(), schema);
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn open_rejects_injected_weight_schema_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("weighted");
    std::fs::create_dir_all(&table_dir).unwrap();
    let path = table_dir.to_string_lossy().to_string();

    let legacy_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            laminar_core::changelog::WEIGHT_COLUMN,
            DataType::Int64,
            true,
        ),
    ]));
    super::super::delta_io::open_or_create_table(
        &path,
        std::collections::HashMap::new(),
        Some(&legacy_schema),
    )
    .await
    .unwrap();

    let pipeline_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            laminar_core::changelog::WEIGHT_COLUMN,
            DataType::Int64,
            false,
        ),
    ]);
    let mut connector_config = ConnectorConfig::new("delta-lake");
    connector_config.set("table.path", &path);
    connector_config.set(
        "_arrow_schema",
        crate::config::encode_arrow_schema_ipc(&pipeline_schema),
    );

    let mut sink = DeltaLakeSink::new(DeltaLakeSinkConfig::new(&path), None);
    let error = sink.open(&connector_config).await.unwrap_err();
    assert!(matches!(&error, ConnectorError::SchemaMismatch(_)));
    assert!(error.to_string().contains("non-null Int64"));
}

#[test]
fn test_schema_empty_when_none() {
    let sink = DeltaLakeSink::new(test_config(), None);
    let schema = sink.schema();
    assert_eq!(schema.fields().len(), 0);
}

#[cfg(feature = "delta-lake")]
#[test]
fn test_deferred_init_flag_default_false() {
    let sink = DeltaLakeSink::new(test_config(), None);
    assert!(!sink.needs_deferred_delta_init);
}

#[cfg(feature = "delta-lake")]
fn unity_config() -> DeltaLakeSinkConfig {
    let mut config = test_config();
    config.catalog_type = DeltaCatalogType::Unity {
        workspace_url: "https://test.azuredatabricks.net".to_string(),
        access_token: "dapi123".to_string(),
    };
    config.catalog_name = Some("main".to_string());
    config.catalog_schema = Some("default".to_string());
    config.catalog_storage_location = Some("abfss://c@acct.dfs.core.windows.net/t".to_string());
    config
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn test_open_defers_init_for_unity_no_schema() {
    use crate::config::ConnectorConfig;

    let config = unity_config();
    let mut sink = DeltaLakeSink::new(config, None);

    // open() with empty ConnectorConfig (simulates factory path)
    let connector_config = ConnectorConfig::new("delta-lake");
    // open() will re-parse but table.path is "/tmp/delta_test" (local),
    // so it won't actually reach UC REST. However from_config requires
    // table.path, so we use the sink's existing config by passing empty.
    // The sink skips re-parse when properties are empty.
    let result = sink.open(&connector_config).await;
    assert!(result.is_ok());

    // Should be in Initializing state with deferred flag set.
    assert!(sink.needs_deferred_delta_init);
    assert_eq!(sink.state(), ConnectorState::Initializing);
    assert!(sink.schema.is_none());
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn test_deferred_init_transitions_to_failed_on_error() {
    // When deferred init fails, the sink must transition to Failed
    // to prevent an unbounded retry storm.
    let mut sink = DeltaLakeSink::new(test_config(), None);
    sink.state = ConnectorState::Initializing;
    sink.needs_deferred_delta_init = true;
    sink.schema = Some(test_schema());

    // begin_epoch will try init_delta_table() which will fail
    // (no real Delta table at /tmp/delta_test). The sink should
    // transition to Failed.
    let result = sink.begin_epoch(1).await;
    assert!(result.is_err());
    assert_eq!(sink.state(), ConnectorState::Failed);
    // Flag may still be set, but Failed state prevents further usage.
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn test_write_batch_accepts_initializing_state() {
    // During deferred init, write_batch must accept Initializing state
    // so the first batch can provide the schema.
    let mut sink = DeltaLakeSink::new(test_config(), None);
    sink.state = ConnectorState::Initializing;
    sink.needs_deferred_delta_init = true;

    let batch = test_batch(5);
    // write_batch sets schema, then tries init_delta_table which fails.
    // Sink transitions to Failed.
    let result = sink.write_batch(&batch).await;
    assert!(result.is_err());
    assert_eq!(sink.state(), ConnectorState::Failed);
    // Schema was set before init was attempted.
    assert!(sink.schema.is_some());
}

#[cfg(feature = "delta-lake")]
#[test]
fn test_no_deferred_init_without_catalog_storage_location() {
    // Unity catalog without catalog.storage.location should NOT defer.
    let mut config = unity_config();
    config.catalog_storage_location = None;
    let sink = DeltaLakeSink::new(config, None);

    assert!(!sink.needs_deferred_delta_init);
}

#[cfg(feature = "delta-lake")]
#[test]
fn test_no_deferred_init_with_schema() {
    // Unity catalog with schema already set should NOT defer.
    let config = unity_config();
    let sink = DeltaLakeSink::with_schema(config, test_schema());

    assert!(!sink.needs_deferred_delta_init);
}

// ── Batch size estimation ──

#[test]
fn test_estimate_batch_size() {
    let batch = test_batch(100);
    let size = DeltaLakeSink::estimate_batch_size(&batch);
    assert!(size > 0);
}

#[test]
fn test_estimate_batch_size_empty() {
    let batch = RecordBatch::new_empty(test_schema());
    let size = DeltaLakeSink::estimate_batch_size(&batch);
    // Arrow arrays have baseline buffer allocation even with 0 rows,
    // so size may be small but not necessarily zero.
    assert!(size < 1024);
}

// ── Should flush tests ──

#[test]
fn test_should_flush_by_rows() {
    let mut config = test_config();
    config.max_buffer_records = 100;
    let mut sink = DeltaLakeSink::new(config, None);
    sink.buffered_rows = 99;
    assert!(!sink.should_flush());
    sink.buffered_rows = 100;
    assert!(sink.should_flush());
}

#[test]
fn test_should_flush_by_bytes() {
    let mut config = test_config();
    config.target_file_size = 1000;
    let mut sink = DeltaLakeSink::new(config, None);
    sink.buffered_bytes = 999;
    assert!(!sink.should_flush());
    sink.buffered_bytes = 1000;
    assert!(sink.should_flush());
}

#[test]
fn test_should_flush_empty() {
    let sink = DeltaLakeSink::new(test_config(), None);
    assert!(!sink.should_flush());
}

#[tokio::test]
async fn test_at_least_once_failed_flush_backpressure() {
    // A failed at-least-once flush retains its staged rows. The next write
    // must be rejected before it grows that in-memory retry backlog.
    let mut config = test_config();
    config.delivery_guarantee = DeliveryGuarantee::AtLeastOnce;
    config.max_buffer_records = 10;
    let mut sink = DeltaLakeSink::new(config, None);
    sink.state = ConnectorState::Running;
    sink.staged_rows = 50;

    let batch = test_batch(5);
    let err = sink
        .write_batch(&batch)
        .await
        .expect_err("retained at-least-once backlog must remain bounded");
    let msg = err.to_string();
    assert!(
        msg.contains("buffer full"),
        "expected backpressure error, got: {msg}"
    );
    assert_eq!(sink.buffered_rows(), 0);
}

// ── Batch buffering tests ──

#[tokio::test]
async fn test_write_batch_buffering() {
    let mut config = test_config();
    config.max_buffer_records = 100;
    let mut sink = DeltaLakeSink::new(config, None);
    sink.state = ConnectorState::Running;

    let batch = test_batch(10);
    let result = sink.write_batch(&batch).await.unwrap();

    // Should buffer, not flush (10 < 100)
    assert_eq!(result.records_written, 0);
    assert_eq!(sink.buffered_rows(), 10);
    assert!(sink.buffered_bytes() > 0);
}

#[tokio::test]
async fn test_write_batch_empty() {
    let mut sink = DeltaLakeSink::new(test_config(), None);
    sink.state = ConnectorState::Running;

    let batch = test_batch(0);
    let result = sink.write_batch(&batch).await.unwrap();
    assert_eq!(result.records_written, 0);
    assert_eq!(sink.buffered_rows(), 0);
}

#[tokio::test]
async fn test_write_batch_not_running() {
    let mut sink = DeltaLakeSink::new(test_config(), None);
    // state is Created, not Running

    let batch = test_batch(10);
    let result = sink.write_batch(&batch).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_write_batch_sets_schema() {
    let mut sink = DeltaLakeSink::new(test_config(), None);
    sink.state = ConnectorState::Running;
    assert!(sink.schema.is_none());

    let batch = test_batch(5);
    sink.write_batch(&batch).await.unwrap();
    assert!(sink.schema.is_some());
    assert_eq!(sink.schema.as_ref().unwrap().fields().len(), 3);
}

#[tokio::test]
async fn test_multiple_write_batches_accumulate() {
    let mut config = test_config();
    config.max_buffer_records = 100;
    let mut sink = DeltaLakeSink::new(config, None);
    sink.state = ConnectorState::Running;

    let batch = test_batch(10);
    sink.write_batch(&batch).await.unwrap();
    sink.write_batch(&batch).await.unwrap();
    sink.write_batch(&batch).await.unwrap();

    assert_eq!(sink.buffered_rows(), 30);
}

// ── Epoch lifecycle tests ──
// Note: Epoch lifecycle with real I/O is tested in delta_io.rs integration tests.

#[tokio::test]
async fn coordinated_artifact_intent_does_not_authorize_delta_file_deletion() {
    let mut config = test_config();
    config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
    let mut sink = DeltaLakeSink::new(config, None);

    assert_eq!(sink.checkpoint_artifact_intent(7).await.unwrap(), None);
}

#[tokio::test]
async fn test_rollback_clears_buffer() {
    let mut config = test_config();
    config.max_buffer_records = 1000;
    let mut sink = DeltaLakeSink::new(config, None);
    sink.state = ConnectorState::Running;

    let batch = test_batch(50);
    sink.write_batch(&batch).await.unwrap();
    assert_eq!(sink.buffered_rows(), 50);

    sink.rollback_epoch(0).await.unwrap();
    assert_eq!(sink.buffered_rows(), 0);
    assert_eq!(sink.buffered_bytes(), 0);
}

/// D001: Rollback after `pre_commit` must discard staged data.
/// `pre_commit` stages batches; rollback discards them without writing to Delta.
#[tokio::test]
async fn test_rollback_after_pre_commit_discards_staged() {
    let mut config = test_config();
    config.max_buffer_records = 1000;
    let mut sink = DeltaLakeSink::new(config, None);
    sink.state = ConnectorState::Running;

    sink.begin_epoch(1).await.unwrap();
    let batch = test_batch(50);
    sink.write_batch(&batch).await.unwrap();
    assert_eq!(sink.buffered_rows(), 50);

    // pre_commit stages the buffer
    sink.pre_commit(1).await.unwrap();
    assert_eq!(sink.buffered_rows(), 0);
    assert_eq!(sink.staged_rows, 50);
    assert!(!sink.staged_batches.is_empty());

    // rollback discards both buffer and staged data
    sink.rollback_epoch(1).await.unwrap();
    assert_eq!(sink.buffered_rows(), 0);
    assert_eq!(sink.staged_rows, 0);
    assert_eq!(sink.staged_bytes, 0);
    assert!(sink.staged_batches.is_empty());
    assert_eq!(sink.delta_version(), 0); // no Delta write occurred
}

/// Staged data remains available for rollback until preparation completes.
#[tokio::test]
async fn test_staged_data_preserved_until_commit_or_rollback() {
    let mut config = test_config();
    config.max_buffer_records = 1000;
    let mut sink = DeltaLakeSink::new(config, None);
    sink.state = ConnectorState::Running;

    sink.begin_epoch(1).await.unwrap();
    sink.write_batch(&test_batch(25)).await.unwrap();
    sink.write_batch(&test_batch(25)).await.unwrap();

    // pre_commit moves buffer → staged
    sink.pre_commit(1).await.unwrap();
    assert_eq!(sink.staged_rows, 50);
    assert_eq!(sink.staged_batches.len(), 2);
    assert_eq!(sink.buffered_rows(), 0);

    // A failed preparation is followed by rollback, which discards the cut.
    sink.rollback_epoch(1).await.unwrap();
    assert!(sink.staged_batches.is_empty());
    assert_eq!(sink.staged_rows, 0);
    assert_eq!(sink.staged_bytes, 0);
}

// ── Open and close tests ──
// Note: These tests use fake paths that don't exist.
// With the delta-lake feature, open() tries to actually access the path.
// See delta_io.rs for integration tests with real I/O.

#[tokio::test]
async fn test_close() {
    let mut sink = DeltaLakeSink::new(test_config(), None);
    sink.state = ConnectorState::Running;

    sink.close().await.unwrap();
    assert_eq!(sink.state(), ConnectorState::Closed);
}

// ── Contract tests ──

#[test]
fn test_contract_append_exactly_once_certifies_only_direct_s3() {
    for (path, topology, cluster_exact) in [
        ("s3://bucket/table", SinkTopology::MultiWriter, true),
        ("s3a://bucket/table", SinkTopology::MultiWriter, true),
        ("gs://bucket/table", SinkTopology::MultiWriter, false),
        ("az://container/table", SinkTopology::MultiWriter, false),
        ("abfs://container/table", SinkTopology::MultiWriter, false),
        ("abfss://container/table", SinkTopology::MultiWriter, false),
        ("wasb://container/table", SinkTopology::MultiWriter, false),
        ("wasbs://container/table", SinkTopology::MultiWriter, false),
        (
            "uc://catalog/schema/table",
            SinkTopology::MultiWriter,
            false,
        ),
        ("C:/delta/table", SinkTopology::Singleton, false),
    ] {
        let mut config = DeltaLakeSinkConfig::new(path);
        config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
        let sink = DeltaLakeSink::new(config, None);
        let contract = sink.contract(&ConnectorConfig::new("delta-lake")).unwrap();
        assert_eq!(contract.consistency, SinkConsistency::CheckpointCommittable);
        assert_eq!(contract.topology, topology, "path={path}");
        assert_eq!(contract.input_mode, SinkInputMode::FullChangelog);
        assert_eq!(
            contract.is_cluster_exact_delivery_certified(),
            cluster_exact,
            "path={path}"
        );
        assert_eq!(sink.suggested_write_timeout(), Duration::from_secs(30));
    }
}

#[cfg(feature = "delta-lake")]
#[test]
fn write_timeout_is_one_operation_budget_not_per_retry() {
    let mut config = test_config();
    config.write_timeout = Duration::from_secs(7);
    let sink = DeltaLakeSink::new(config, None);

    assert_eq!(sink.suggested_write_timeout(), Duration::from_secs(7));
}

#[test]
fn test_contract_upsert() {
    let sink = DeltaLakeSink::new(upsert_config(), None);
    let contract = sink.contract(&ConnectorConfig::new("delta-lake")).unwrap();
    assert_eq!(contract.consistency, SinkConsistency::DurableAtLeastOnce);
    assert_eq!(contract.input_mode, SinkInputMode::FullChangelog);
    assert!(!contract.is_cluster_exact_delivery_certified());
}

#[test]
fn test_contract_at_least_once() {
    let mut config = test_config();
    config.delivery_guarantee = DeliveryGuarantee::AtLeastOnce;
    let sink = DeltaLakeSink::new(config, None);
    let contract = sink.contract(&ConnectorConfig::new("delta-lake")).unwrap();
    assert_eq!(contract.consistency, SinkConsistency::DurableAtLeastOnce);
    assert_eq!(contract.input_mode, SinkInputMode::FullChangelog);
    assert!(!contract.is_cluster_exact_delivery_certified());
}

// ── Changelog splitting tests ──

fn changelog_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("_op", DataType::Utf8, false),
        Field::new("_ts_ms", DataType::Int64, false),
    ]))
}

fn changelog_batch() -> RecordBatch {
    RecordBatch::try_new(
        changelog_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            Arc::new(StringArray::from(vec!["I", "U", "D", "I", "D"])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )
    .unwrap()
}

#[test]
fn test_split_changelog_batch() {
    let batch = changelog_batch();
    let (inserts, deletes) = DeltaLakeSink::split_changelog_batch(&batch).unwrap();

    // Inserts: rows 0 (I), 1 (U), 3 (I) = 3 rows
    assert_eq!(inserts.num_rows(), 3);
    // Deletes: rows 2 (D), 4 (D) = 2 rows
    assert_eq!(deletes.num_rows(), 2);

    // Metadata columns should be stripped
    assert_eq!(inserts.num_columns(), 2); // id, name only
    assert_eq!(deletes.num_columns(), 2);

    // Verify insert values
    let insert_ids = inserts
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(insert_ids.value(0), 1);
    assert_eq!(insert_ids.value(1), 2);
    assert_eq!(insert_ids.value(2), 4);

    // Verify delete values
    let delete_ids = deletes
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(delete_ids.value(0), 3);
    assert_eq!(delete_ids.value(1), 5);
}

#[test]
fn test_split_changelog_all_inserts() {
    let schema = changelog_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(StringArray::from(vec!["I", "I"])),
            Arc::new(Int64Array::from(vec![100, 200])),
        ],
    )
    .unwrap();

    let (inserts, deletes) = DeltaLakeSink::split_changelog_batch(&batch).unwrap();
    assert_eq!(inserts.num_rows(), 2);
    assert_eq!(deletes.num_rows(), 0);
}

#[test]
fn test_split_changelog_all_deletes() {
    let schema = changelog_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(StringArray::from(vec!["D", "D"])),
            Arc::new(Int64Array::from(vec![100, 200])),
        ],
    )
    .unwrap();

    let (inserts, deletes) = DeltaLakeSink::split_changelog_batch(&batch).unwrap();
    assert_eq!(inserts.num_rows(), 0);
    assert_eq!(deletes.num_rows(), 2);
}

#[test]
fn test_split_changelog_missing_op_column() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();

    let result = DeltaLakeSink::split_changelog_batch(&batch);
    assert!(result.is_err());
}

#[test]
fn test_split_changelog_snapshot_read() {
    let schema = changelog_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(StringArray::from(vec!["r"])), // snapshot read
            Arc::new(Int64Array::from(vec![100])),
        ],
    )
    .unwrap();

    let (inserts, deletes) = DeltaLakeSink::split_changelog_batch(&batch).unwrap();
    assert_eq!(inserts.num_rows(), 1);
    assert_eq!(deletes.num_rows(), 0);
}

// ── Debug output test ──

#[test]
fn test_debug_output() {
    let sink = DeltaLakeSink::new(test_config(), None);
    let debug = format!("{sink:?}");
    assert!(debug.contains("DeltaLakeSink"));
    assert!(debug.contains("delta_test_nonexistent_"));
}

// ── End-to-end upsert collapse (aggregating-MV changelog → Delta table) ──

/// A Z-set changelog batch shaped like aggregating-MV output:
/// `[region: Utf8, total: Int64, __weight: Int64]`.
#[cfg(feature = "delta-lake")]
fn zset_changelog(rows: &[(&str, i64, i64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("total", DataType::Int64, false),
        Field::new(
            laminar_core::changelog::WEIGHT_COLUMN,
            DataType::Int64,
            false,
        ),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// Land one at-least-once write cut through the durable flush path.
#[cfg(feature = "delta-lake")]
async fn flush_batch(sink: &mut DeltaLakeSink, batch: &RecordBatch) {
    sink.write_batch(batch).await.unwrap();
    sink.flush().await.unwrap();
}

/// Read the table back and return its current `(region, total)` rows, sorted.
#[cfg(feature = "delta-lake")]
async fn read_regions(path: &str) -> Vec<(String, i64)> {
    let ctx = datafusion::prelude::SessionContext::new();
    crate::lakehouse::delta_table_provider::register_delta_table(
        &ctx,
        "t",
        path,
        std::collections::HashMap::new(),
    )
    .await
    .unwrap();
    let batches = ctx
        .sql("SELECT region, total FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut out = Vec::new();
    for b in &batches {
        // DataFusion may return strings as Utf8View; cast to the concrete
        // types we downcast to.
        let region_arr = arrow_cast::cast(
            b.column(b.schema().index_of("region").unwrap()),
            &DataType::Utf8,
        )
        .unwrap();
        let total_arr = arrow_cast::cast(
            b.column(b.schema().index_of("total").unwrap()),
            &DataType::Int64,
        )
        .unwrap();
        let regions = region_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let totals = total_arr.as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((regions.value(i).to_string(), totals.value(i)));
        }
    }
    out.sort();
    out
}

/// An aggregating MV emits a Z-set changelog; the upsert sink must collapse
/// it into the table's current per-key state — surviving value updates,
/// group disappearance, and multiple updates to one key within a single
/// epoch (the case that triggers a delta-rs cardinality violation without
/// collapse).
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn upsert_collapses_aggregating_mv_to_current_state() {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("agg");
    // delta-rs' local object store requires the table directory to exist.
    std::fs::create_dir_all(&table_dir).unwrap();
    let path = table_dir.to_string_lossy().to_string();

    let mut cfg = DeltaLakeSinkConfig::new(&path);
    cfg.write_mode = DeltaWriteMode::Upsert;
    cfg.merge_key_columns = vec!["region".to_string()];
    cfg.delivery_guarantee = DeliveryGuarantee::AtLeastOnce;

    // No explicit schema: the schema (and table) is derived from the first
    // batch, exactly like the production pipeline — exercising the
    // `target_schema` strip of `__weight`.
    let mut sink = DeltaLakeSink::new(cfg, None);
    sink.open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();

    flush_batch(
        &mut sink,
        &zset_changelog(&[("east", 10, 1), ("west", 5, 1)]),
    )
    .await;
    assert_eq!(
        read_regions(&path).await,
        vec![("east".into(), 10), ("west".into(), 5)]
    );

    // Second cut: update east (retract 10 + insert 30), drop west, add north.
    flush_batch(
        &mut sink,
        &zset_changelog(&[
            ("east", 10, -1),
            ("east", 30, 1),
            ("west", 5, -1),
            ("north", 7, 1),
        ]),
    )
    .await;
    assert_eq!(
        read_regions(&path).await,
        vec![("east".into(), 30), ("north".into(), 7)]
    );

    // Third cut: two consecutive updates to "east" in one batch. Without
    // collapse this is multiple source rows for one merge key → delta-rs
    // cardinality violation. With collapse it folds to the final value.
    flush_batch(
        &mut sink,
        &zset_changelog(&[
            ("east", 30, -1),
            ("east", 40, 1),
            ("east", 40, -1),
            ("east", 55, 1),
        ]),
    )
    .await;
    assert_eq!(
        read_regions(&path).await,
        vec![("east".into(), 55), ("north".into(), 7)]
    );

    // The target table never carried the Z-set weight column.
    assert!(sink.schema.as_ref().unwrap().index_of("__weight").is_err());

    // Collapse observability fired across the three durable flushes.
    let m = sink.sink_metrics();
    assert_eq!(m.collapse_rows_in.get(), 10);
    assert!(m.collapse_deletes_out.get() >= 1, "west was dropped");
    assert!(m.collapse_upserts_out.get() >= 4);

    sink.close().await.unwrap();
}

// ── Coordinated-commit (designated-committer) regressions ──

#[cfg(feature = "delta-lake")]
fn coordinated_config(path: &str) -> DeltaLakeSinkConfig {
    let mut cfg = DeltaLakeSinkConfig::new(path);
    cfg.write_mode = DeltaWriteMode::Append;
    cfg.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
    cfg
}

#[cfg(feature = "delta-lake")]
fn coordinated_commit_context() -> crate::connector::CoordinatedCommitContext {
    crate::connector::CoordinatedCommitContext::new(
        tokio::time::Instant::now() + Duration::from_secs(30),
    )
}

#[cfg(feature = "delta-lake")]
async fn coordinated_row_count(path: &str) -> usize {
    let ctx = datafusion::prelude::SessionContext::new();
    crate::lakehouse::delta_table_provider::register_delta_table(
        &ctx,
        "t",
        path,
        std::collections::HashMap::new(),
    )
    .await
    .unwrap();
    ctx.sql("SELECT id FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

/// Exactly-once Delta is admitted only for coordinated append.
#[test]
fn exactly_once_is_coordinated_append_only() {
    let mut cfg = test_config();
    cfg.write_mode = DeltaWriteMode::Append;
    cfg.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
    assert!(DeltaLakeSink::new(cfg, None).is_coordinated());

    let mut cfg = test_config();
    cfg.write_mode = DeltaWriteMode::Append;
    cfg.delivery_guarantee = DeliveryGuarantee::AtLeastOnce;
    assert!(!DeltaLakeSink::new(cfg, None).is_coordinated());

    let mut cfg = test_config();
    cfg.write_mode = DeltaWriteMode::Upsert;
    cfg.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
    let sink = DeltaLakeSink::new(cfg, None);
    assert!(sink.contract(&ConnectorConfig::new("delta-lake")).is_err());
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn coordinated_cursor_rejects_a_mutated_namespace_before_table_io() {
    use crate::connector::{CoordinatedCommitNamespace, CoordinatedCommitter};
    use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;

    let sink = DeltaLakeSink::new(coordinated_config("unused"), None);
    let mut namespace = CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        "018f0000-0000-7000-8000-000000000001",
        "events",
    )
    .unwrap();
    namespace.deployment_id = "not-a-uuid".into();

    let error = sink.committed_cursor(&namespace).await.unwrap_err();
    assert!(matches!(error, ConnectorError::ConfigurationError(_)));
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn coordinated_millisecond_timestamp_is_widened_before_materialization() {
    use arrow_array::TimestampMillisecondArray;
    use arrow_schema::TimeUnit;

    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("coord_millisecond_timestamp");
    std::fs::create_dir_all(&table_dir).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "probe_time",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(TimestampMillisecondArray::from(vec![Some(1_500)]))],
    )
    .unwrap();
    let mut sink =
        DeltaLakeSink::with_schema(coordinated_config(&table_dir.to_string_lossy()), schema);

    sink.open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();
    sink.begin_epoch(1).await.unwrap();
    sink.write_batch(&batch).await.unwrap();
    let descriptor = sink.pre_commit(1).await.unwrap();

    assert!(descriptor.is_some());
    sink.rollback_epoch(1).await.unwrap();
    sink.close().await.unwrap();
}

/// An exact epoch larger than the former 4x in-memory cap is staged across
/// uniquely named, log-invisible Parquet files and published by one checkpoint
/// without replay.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn coordinated_epoch_over_four_times_buffer_cap_commits_once() {
    use std::collections::HashSet;

    use crate::connector::{
        CoordinatedCommitBatch, CoordinatedCommitCursor, CoordinatedCommitNamespace,
        CoordinatedCommitPayload, CoordinatedCommitter,
    };
    use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
    use laminar_core::checkpoint::CheckpointAttempt;

    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("coord_large_epoch");
    std::fs::create_dir_all(&table_dir).unwrap();
    let path = table_dir.to_string_lossy().to_string();

    let mut config = coordinated_config(&path);
    config.max_buffer_records = 2;
    let mut sink = DeltaLakeSink::with_schema(config, test_schema());
    sink.open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();
    sink.begin_epoch(1).await.unwrap();

    // Twelve rows exceed the removed 4 x 2 row cap. Every three-row write
    // crosses the staging threshold and must succeed without replay.
    for _ in 0..4 {
        sink.write_batch(&test_batch(3)).await.unwrap();
    }
    assert!(sink.buffer.is_empty());
    assert!(sink.staged_batches.is_empty());
    assert_eq!(sink.coordinated_adds.len(), 4);
    assert_eq!(
        coordinated_row_count(&path).await,
        0,
        "staged files must remain invisible before their Delta Add commit"
    );

    let descriptor = sink
        .pre_commit(1)
        .await
        .unwrap()
        .expect("non-empty exact epoch returns one descriptor");
    assert!(descriptor.len() <= crate::connector::MAX_COORDINATED_COMMIT_PAYLOAD_BYTES);
    assert!(sink.coordinated_adds.is_empty());
    assert_eq!(sink.coordinated_descriptor_bytes, 0);

    let adds = super::super::delta_io::decode_commit_descriptors(std::slice::from_ref(&descriptor))
        .unwrap()
        .unwrap()
        .adds;
    assert_eq!(adds.len(), 4);
    assert_eq!(
        adds.iter()
            .map(|add| add.path.as_str())
            .collect::<HashSet<_>>()
            .len(),
        adds.len(),
        "each incremental stage must use an attempt-unique object path"
    );
    assert_eq!(coordinated_row_count(&path).await, 0);

    let namespace = CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        "018f0000-0000-7000-8000-000000000001",
        "delta_out",
    )
    .unwrap();
    let attempt = CheckpointAttempt::canonical(101);
    sink.commit_aggregated(
        CoordinatedCommitBatch {
            namespace: namespace.clone(),
            expected_predecessor: CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            },
            fencing_token: 1,
            target: attempt,
            entries: vec![CoordinatedCommitPayload {
                attempt,
                participant_id: 1,
                payload: Some(descriptor),
            }],
        },
        coordinated_commit_context(),
    )
    .await
    .unwrap();

    assert_eq!(coordinated_row_count(&path).await, 12);
    assert_eq!(
        sink.committed_cursor(&namespace).await.unwrap(),
        Some(CoordinatedCommitCursor {
            checkpoint_id: 101,
            fencing_token: 1,
        })
    );
    sink.close().await.unwrap();
}

/// The designated cursor stores the canonical engine checkpoint identity;
/// Delta's local staging epoch remains connector-local.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn coordinated_recovery_reads_namespaced_checkpoint_id() {
    use crate::connector::{
        CoordinatedCommitBatch, CoordinatedCommitCursor, CoordinatedCommitNamespace,
        CoordinatedCommitPayload, CoordinatedCommitter,
    };
    use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
    use laminar_core::checkpoint::CheckpointAttempt;

    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("coord_recover");
    std::fs::create_dir_all(&table_dir).unwrap();
    let path = table_dir.to_string_lossy().to_string();

    let namespace = CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        "018f0000-0000-7000-8000-000000000001",
        "delta_out",
    )
    .unwrap();

    // Writer stages epochs 1 and 2; the designated committer advances the
    // exact checkpoint-id cursor under `namespace`.
    let mut writer = DeltaLakeSink::with_schema(coordinated_config(&path), test_schema());
    writer
        .open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();

    for epoch in 1..=2u64 {
        writer.begin_epoch(epoch).await.unwrap();
        writer.write_batch(&test_batch(3)).await.unwrap();
        let descriptor = writer
            .pre_commit(epoch)
            .await
            .unwrap()
            .expect("coordinated pre_commit returns a descriptor");
        let attempt = CheckpointAttempt::canonical(100 + epoch);
        writer
            .commit_aggregated(
                CoordinatedCommitBatch {
                    namespace: namespace.clone(),
                    expected_predecessor: CoordinatedCommitCursor {
                        checkpoint_id: if epoch == 1 { 0 } else { 100 + epoch - 1 },
                        fencing_token: u64::from(epoch != 1),
                    },
                    fencing_token: 1,
                    target: attempt,
                    entries: vec![CoordinatedCommitPayload {
                        attempt,
                        participant_id: 1,
                        payload: Some(descriptor),
                    }],
                },
                coordinated_commit_context(),
            )
            .await
            .unwrap();
    }
    writer.close().await.unwrap();

    // A fresh connector reads the exact external cursor independently of its
    // local descriptor-preparation lifecycle.
    let mut recovered = DeltaLakeSink::with_schema(coordinated_config(&path), test_schema());
    recovered
        .open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();
    assert_eq!(
        recovered.committed_cursor(&namespace).await.unwrap(),
        Some(crate::connector::CoordinatedCommitCursor {
            checkpoint_id: 102,
            fencing_token: 1,
        })
    );
    recovered.close().await.unwrap();
}

/// A new designated leader may construct its batch before observing a late
/// commit from the old leader. The fresh external cursor read must filter the
/// already-published attempt while still publishing the new attempt and
/// advancing the cursor to the batch target.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn coordinated_failover_overlap_does_not_duplicate_committed_attempt() {
    use crate::connector::{
        CoordinatedCommitBatch, CoordinatedCommitCursor, CoordinatedCommitNamespace,
        CoordinatedCommitPayload, CoordinatedCommitter,
    };
    use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
    use laminar_core::checkpoint::CheckpointAttempt;

    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("coord_failover_overlap");
    std::fs::create_dir_all(&table_dir).unwrap();
    let path = table_dir.to_string_lossy().to_string();
    let namespace = CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        "018f0000-0000-7000-8000-000000000001",
        "delta_out",
    )
    .unwrap();
    let first_attempt = CheckpointAttempt::canonical(101);
    let second_attempt = CheckpointAttempt::canonical(102);

    let mut writer = DeltaLakeSink::with_schema(coordinated_config(&path), test_schema());
    writer
        .open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();

    writer.begin_epoch(1).await.unwrap();
    writer.write_batch(&test_batch(3)).await.unwrap();
    let first_descriptor = writer
        .pre_commit(1)
        .await
        .unwrap()
        .expect("first coordinated descriptor");
    writer
        .commit_aggregated(
            CoordinatedCommitBatch {
                namespace: namespace.clone(),
                expected_predecessor: CoordinatedCommitCursor {
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
            },
            coordinated_commit_context(),
        )
        .await
        .unwrap();
    assert_eq!(coordinated_row_count(&path).await, 3);
    assert_eq!(
        writer.committed_cursor(&namespace).await.unwrap(),
        Some(crate::connector::CoordinatedCommitCursor {
            checkpoint_id: 101,
            fencing_token: 1,
        })
    );

    writer.begin_epoch(2).await.unwrap();
    writer.write_batch(&test_batch(2)).await.unwrap();
    let second_descriptor = writer
        .pre_commit(2)
        .await
        .unwrap()
        .expect("second coordinated descriptor");

    // Simulate leadership overlap: the replacement leader assembled this
    // batch before learning that checkpoint 101 had just committed.
    writer
        .commit_aggregated(
            CoordinatedCommitBatch {
                namespace: namespace.clone(),
                expected_predecessor: CoordinatedCommitCursor {
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
            },
            coordinated_commit_context(),
        )
        .await
        .unwrap();

    assert_eq!(
        coordinated_row_count(&path).await,
        5,
        "the already-committed three-row descriptor must not be appended twice"
    );
    assert_eq!(
        writer.committed_cursor(&namespace).await.unwrap(),
        Some(crate::connector::CoordinatedCommitCursor {
            checkpoint_id: 102,
            fencing_token: 2,
        }),
        "the overlapping batch must still advance the external cursor"
    );
    writer.close().await.unwrap();
}

/// Finding B: the coordinated descriptor write must honor configured Parquet
/// properties (cached at open), not the writer's hard-coded Snappy default.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn coordinated_open_caches_configured_writer_properties() {
    use deltalake::parquet::basic::Compression;
    use deltalake::parquet::schema::types::ColumnPath;

    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("coord_props");
    std::fs::create_dir_all(&table_dir).unwrap();
    let path = table_dir.to_string_lossy().to_string();

    let mut cfg = coordinated_config(&path);
    cfg.parquet.compression = "gzip".to_string();
    cfg.parquet.compression_level = 6;

    let mut sink = DeltaLakeSink::with_schema(cfg, test_schema());
    sink.open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();

    let props = sink
        .cached_writer_properties
        .as_ref()
        .expect("open() caches writer properties");
    assert!(
        matches!(
            props.compression(&ColumnPath::from(Vec::<String>::new())),
            Compression::GZIP(_)
        ),
        "descriptor writer must apply configured (non-default) compression"
    );
    sink.close().await.unwrap();
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn coordinated_unresolved_publication_allows_only_the_exact_batch_retry() {
    use crate::connector::{
        CoordinatedCommitBatch, CoordinatedCommitCursor, CoordinatedCommitNamespace,
        CoordinatedCommitPayload, CoordinatedCommitter,
    };
    use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
    use laminar_core::checkpoint::CheckpointAttempt;

    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("coord_reconcile");
    std::fs::create_dir_all(&table_dir).unwrap();
    let path = table_dir.to_string_lossy().to_string();
    let mut sink = DeltaLakeSink::with_schema(coordinated_config(&path), test_schema());
    sink.open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();
    let namespace = CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        "018f0000-0000-7000-8000-000000000001",
        "delta_out",
    )
    .unwrap();
    let attempt = CheckpointAttempt::canonical(101);
    let committed = CoordinatedCommitCursor {
        checkpoint_id: 101,
        fencing_token: 1,
    };
    sink.commit_aggregated(
        CoordinatedCommitBatch {
            namespace: namespace.clone(),
            expected_predecessor: CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            },
            fencing_token: 1,
            target: attempt,
            entries: vec![CoordinatedCommitPayload {
                attempt,
                participant_id: 1,
                payload: None,
            }],
        },
        coordinated_commit_context(),
    )
    .await
    .unwrap();

    let second = CheckpointAttempt::canonical(102);
    let third = CheckpointAttempt::canonical(103);
    let second_batch = CoordinatedCommitBatch {
        namespace: namespace.clone(),
        expected_predecessor: committed,
        fencing_token: 1,
        target: second,
        entries: vec![CoordinatedCommitPayload {
            attempt: second,
            participant_id: 1,
            payload: None,
        }],
    };
    *sink.coordinated_unresolved_publication.lock() = Some(UnresolvedDeltaPublication {
        external_key: namespace.external_key(),
        target: CoordinatedCommitCursor {
            checkpoint_id: 102,
            fencing_token: 1,
        },
        exact_batch_fingerprint: second_batch.exact_fingerprint(),
    });

    assert_eq!(
        sink.committed_cursor(&namespace).await.unwrap(),
        Some(committed)
    );
    assert!(sink.coordinated_unresolved_publication.lock().is_some());
    assert!(sink.begin_epoch(2).await.is_err());

    let higher_batch = CoordinatedCommitBatch {
        namespace: namespace.clone(),
        expected_predecessor: committed,
        fencing_token: 1,
        target: third,
        entries: vec![
            CoordinatedCommitPayload {
                attempt: second,
                participant_id: 1,
                payload: None,
            },
            CoordinatedCommitPayload {
                attempt: third,
                participant_id: 1,
                payload: None,
            },
        ],
    };
    assert!(sink
        .commit_aggregated(higher_batch, coordinated_commit_context())
        .await
        .is_err());
    assert!(sink.coordinated_unresolved_publication.lock().is_some());

    sink.commit_aggregated(second_batch, coordinated_commit_context())
        .await
        .unwrap();
    assert!(sink.coordinated_unresolved_publication.lock().is_none());
    let second_cursor = CoordinatedCommitCursor {
        checkpoint_id: 102,
        fencing_token: 1,
    };
    assert_eq!(
        sink.committed_cursor(&namespace).await.unwrap(),
        Some(second_cursor)
    );

    sink.commit_aggregated(
        CoordinatedCommitBatch {
            namespace: namespace.clone(),
            expected_predecessor: second_cursor,
            fencing_token: 1,
            target: third,
            entries: vec![CoordinatedCommitPayload {
                attempt: third,
                participant_id: 1,
                payload: None,
            }],
        },
        coordinated_commit_context(),
    )
    .await
    .unwrap();
    assert_eq!(
        sink.committed_cursor(&namespace).await.unwrap(),
        Some(CoordinatedCommitCursor {
            checkpoint_id: 103,
            fencing_token: 1,
        })
    );
    sink.begin_epoch(4).await.unwrap();
    sink.close().await.unwrap();
}

#[cfg(feature = "delta-lake")]
#[tokio::test(start_paused = true)]
async fn coordinated_catalog_commit_timeout_fences_later_work_until_cursor_read() {
    use crate::connector::{
        CoordinatedCommitBatch, CoordinatedCommitCursor, CoordinatedCommitNamespace,
        CoordinatedCommitPayload, CoordinatedCommitter,
    };
    use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
    use laminar_core::checkpoint::CheckpointAttempt;

    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("coord_commit_timeout");
    std::fs::create_dir_all(&table_dir).unwrap();
    let path = table_dir.to_string_lossy().to_string();
    let mut sink = DeltaLakeSink::with_schema(coordinated_config(&path), test_schema());
    sink.open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();
    let namespace = CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        "018f0000-0000-7000-8000-000000000001",
        "delta_timeout",
    )
    .unwrap();
    let attempt = CheckpointAttempt::canonical(101);
    let batch = CoordinatedCommitBatch {
        namespace: namespace.clone(),
        expected_predecessor: CoordinatedCommitCursor {
            checkpoint_id: 0,
            fencing_token: 0,
        },
        fencing_token: 1,
        target: attempt,
        entries: vec![CoordinatedCommitPayload {
            attempt,
            participant_id: 1,
            payload: None,
        }],
    };
    let delayed_commit = super::super::delta_io::DelayedCoordinatedCatalogCommit {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let error = super::super::delta_io::DELAY_COORDINATED_CATALOG_COMMIT
        .scope(
            delayed_commit.clone(),
            sink.commit_aggregated(
                batch,
                crate::connector::CoordinatedCommitContext::new(deadline),
            ),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("coordinated publication exceeded"),
        "got: {error}"
    );
    delayed_commit.started.notified().await;
    assert!(sink.coordinated_unresolved_publication.lock().is_some());
    assert!(sink.begin_epoch(2).await.is_err());

    assert_eq!(sink.committed_cursor(&namespace).await.unwrap(), None);
    assert!(sink.coordinated_unresolved_publication.lock().is_some());
    assert!(sink.begin_epoch(2).await.is_err());

    delayed_commit.release.notify_one();
    let target = CoordinatedCommitCursor {
        checkpoint_id: 101,
        fencing_token: 1,
    };
    let mut observed = None;
    for _ in 0..100 {
        observed = sink.committed_cursor(&namespace).await.unwrap();
        if observed == Some(target) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(observed, Some(target));
    assert!(sink.coordinated_unresolved_publication.lock().is_none());
    sink.begin_epoch(2).await.unwrap();
    sink.close().await.unwrap();
}

/// Coordinated preparation times out without discarding the staged checkpoint cut.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn coordinated_pre_commit_timeout_preserves_staged_data_until_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("coord_prepare_timeout");
    std::fs::create_dir_all(&table_dir).unwrap();
    let config = coordinated_config(&table_dir.to_string_lossy());

    let mut sink = DeltaLakeSink::with_schema(config, test_schema());
    sink.open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();
    sink.config.write_timeout = Duration::from_millis(10);
    sink.stall_descriptor_write = true;
    sink.begin_epoch(7).await.unwrap();
    sink.write_batch(&test_batch(3)).await.unwrap();

    let error = sink
        .pre_commit(7)
        .await
        .expect_err("the injected stalled descriptor write must time out");
    assert!(
        error.to_string().contains("timed out"),
        "expected a timeout error, got: {error}"
    );

    assert_eq!(sink.buffered_rows, 0, "pre_commit must stage the buffer");
    assert!(sink.buffer.is_empty());
    assert_eq!(sink.staged_rows, 3, "timeout must preserve staged rows");
    assert_eq!(sink.staged_batches.len(), 1);
    assert!(sink.staged_bytes > 0);

    sink.rollback_epoch(7).await.unwrap();
    assert_eq!(sink.staged_rows, 0);
    assert_eq!(sink.staged_bytes, 0);
    assert!(sink.staged_batches.is_empty());
    sink.close().await.unwrap();
}
