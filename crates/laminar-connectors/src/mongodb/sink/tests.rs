use super::*;
use arrow_array::{Int64Array, StringArray};
use arrow_schema::{Field, Schema};
use futures_util::FutureExt as _;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn test_batch(n: usize) -> RecordBatch {
    #[allow(clippy::cast_possible_wrap)]
    let ids: Vec<i64> = (0..n as i64).collect();
    let names: Vec<String> = (0..n).map(|i| format!("user_{i}")).collect();

    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

fn test_config() -> MongoDbSinkConfig {
    MongoDbSinkConfig::new("mongodb://localhost:27017", "db", "coll")
}

#[test]
fn test_new_sink() {
    let config = MongoDbSinkConfig::new("mongodb://localhost:27017", "db", "coll");
    let sink = MongoDbSink::new(test_schema(), config, None);
    assert_eq!(sink.buffered_rows(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_wait_before_bulk_task_first_poll_keeps_operation_tracked() {
    let sink = MongoDbSink::new(test_schema(), test_config(), None);
    let terminal = sink.terminal_task_tracker().unwrap();
    let polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let task_polled = Arc::clone(&polled);
    let release = Arc::new(tokio::sync::Notify::new());
    let task_release = Arc::clone(&release);

    let wait = await_mongo_sink_operation(
        &sink.task_owner,
        tokio::time::Instant::now() + Duration::from_secs(60),
        async move {
            task_polled.store(true, std::sync::atomic::Ordering::Release);
            task_release.notified().await;
        },
    );
    assert!(
        wait.now_or_never().is_none(),
        "operation wait must still be pending"
    );
    assert!(
        !polled.load(std::sync::atomic::Ordering::Acquire),
        "the spawned bulk task must not have reached its first poll"
    );

    drop(sink);
    assert!(!terminal.is_terminated());
    tokio::task::yield_now().await;
    assert!(polled.load(std::sync::atomic::Ordering::Acquire));

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), terminal.wait_terminated())
        .await
        .expect("tracker must resolve only after the detached operation exits");
}

#[tokio::test]
async fn bulk_deadline_keeps_generation_non_terminal_until_operation_exit() {
    let sink = MongoDbSink::new(test_schema(), test_config(), None);
    let terminal = sink.terminal_task_tracker().unwrap();
    let release = Arc::new(tokio::sync::Notify::new());
    let task_release = Arc::clone(&release);

    let outcome =
        await_mongo_sink_operation(&sink.task_owner, tokio::time::Instant::now(), async move {
            task_release.notified().await;
        })
        .await
        .unwrap();
    assert!(matches!(outcome, MongoOperationOutcome::Deadline));

    drop(sink);
    assert!(!terminal.is_terminated());
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), terminal.wait_terminated())
        .await
        .expect("deadline must not publish terminal state before the operation exits");
}

#[test]
fn mongo_bulk_failure_classification_table() {
    use MongoBulkDisposition::{DefinitelyNotApplied, OutcomeUnknown};
    use MongoBulkFailureShape::{
        Bulk, Command, Deadline, Transport, Unknown, WriteConcern, WriteRejected,
    };

    let facts = |shape, no_writes, retryable_signal| MongoBulkFailureFacts {
        no_writes,
        retryable_signal,
        shape,
    };
    let bulk = |partial, write_errors, write_concern_errors| Bulk {
        partial,
        write_errors,
        write_concern_errors,
    };

    let cases = [
        (
            "no writes performed",
            facts(Transport, true, true),
            DefinitelyNotApplied { retryable: true },
        ),
        (
            "partial permanent rejection",
            facts(bulk(true, true, false), false, false),
            OutcomeUnknown { retryable: false },
        ),
        (
            "partial transport failure",
            // The nested retry wrote nothing, but an earlier wire batch still succeeded.
            facts(bulk(true, false, false), true, true),
            OutcomeUnknown { retryable: true },
        ),
        (
            "deadline",
            facts(Deadline, false, true),
            OutcomeUnknown { retryable: true },
        ),
        (
            "first ordered write rejected",
            facts(bulk(false, true, false), false, false),
            DefinitelyNotApplied { retryable: false },
        ),
        (
            "unknown terminal failure",
            facts(Unknown, false, false),
            OutcomeUnknown { retryable: false },
        ),
        (
            "retryable write concern failure",
            facts(WriteConcern, false, true),
            OutcomeUnknown { retryable: true },
        ),
        (
            "retryable server rejection",
            facts(Command, false, true),
            OutcomeUnknown { retryable: true },
        ),
        (
            "per-item write rejection",
            facts(WriteRejected, false, false),
            DefinitelyNotApplied { retryable: false },
        ),
    ];

    for (name, facts, expected) in cases {
        assert_eq!(classify_mongo_bulk_facts(facts), expected, "{name}");
    }
}

#[test]
fn later_chunk_failure_preserves_partial_application() {
    let completed = WriteResult::new(7, 256);
    let error = mongo_partial_batch_error(
        &completed,
        ConnectorError::ConfigurationError("later chunk rejected".into()),
    );
    assert!(error.is_outcome_unknown());
    assert!(!error.is_transient());
    assert!(error.to_string().contains("7 records and 256 bytes"));

    let before_output = mongo_partial_batch_error(
        &WriteResult::new(0, 0),
        ConnectorError::ConfigurationError("rejected before output".into()),
    );
    assert!(matches!(
        before_output,
        ConnectorError::ConfigurationError(_)
    ));
}

#[test]
fn mongo_transport_and_deadline_errors_require_retirement() {
    let driver_error: mongodb::error::Error =
        std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset").into();
    let transport = classify_mongo_bulk_failure(
        "MongoDB sink bulk_write",
        MongoBulkFailure::Driver(&driver_error),
    );
    assert!(transport.is_outcome_unknown());
    assert!(transport.is_transient());

    let deadline = classify_mongo_bulk_failure(
        "MongoDB sink bulk_write",
        MongoBulkFailure::Deadline(Duration::from_secs(1)),
    );
    assert!(deadline.is_outcome_unknown());
    assert!(deadline.is_transient());
    assert!(deadline.to_string().contains("timed out after 1s"));
}

#[test]
fn mongo_driver_partial_result_is_classified_as_applied() {
    let mut bulk = mongodb::error::BulkWriteError::default();
    bulk.partial_result = Some(mongodb::error::PartialBulkWriteResult::Summary(
        mongodb::results::SummaryBulkWriteResult::default(),
    ));
    let driver_error: mongodb::error::Error = mongodb::error::ErrorKind::BulkWrite(bulk).into();

    assert_eq!(
        classify_mongo_bulk_facts(mongo_bulk_failure_facts(&driver_error)),
        MongoBulkDisposition::OutcomeUnknown { retryable: false }
    );
}

#[test]
fn unsupported_arrow_types_fail_schema_validation() {
    for data_type in [DataType::Binary, DataType::UInt64] {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            data_type.clone(),
            false,
        )]));
        let error = MongoDbSink::validate_schema(&schema, &test_config()).unwrap_err();
        assert!(error.to_string().contains("unsupported Arrow type"));
    }
}

#[test]
fn engine_schema_replaces_programmatic_schema_and_validates_upsert_keys() {
    let engine_schema = Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("sequence", DataType::Int64, false),
    ]));
    let mut config = ConnectorConfig::new("mongodb-sink");
    config.set("connection.uri", "mongodb://localhost:27017");
    config.set("database", "db");
    config.set("collection", "out");
    config.set("write.mode", "upsert");
    config.set("write.mode.key_fields", "tenant");
    config.set(
        "_arrow_schema",
        crate::config::encode_arrow_schema_ipc(engine_schema.as_ref()),
    );

    let mut sink = MongoDbSink::new(test_schema(), test_config(), None);
    sink.apply_connector_config(&config).unwrap();
    assert_eq!(sink.schema, engine_schema);

    config.set("write.mode.key_fields", "missing");
    let error = sink.apply_connector_config(&config).unwrap_err();
    assert!(error.to_string().contains("missing"));
}

#[test]
fn test_sink_contract_insert() {
    let config = test_config();
    let sink = MongoDbSink::new(test_schema(), config, None);
    let contract = sink.contract(&ConnectorConfig::new("mongodb")).unwrap();
    assert_eq!(contract.consistency, SinkConsistency::DurableAtLeastOnce);
    assert_eq!(contract.topology, SinkTopology::MultiWriter);
    assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
    assert_eq!(sink.suggested_write_timeout(), Duration::from_secs(30));
}

#[test]
fn test_sink_contract_upsert() {
    let mut config = test_config();
    config.write_mode = WriteMode::Upsert {
        key_fields: vec!["id".to_string()],
    };
    let sink = MongoDbSink::new(test_schema(), config, None);
    let contract = sink.contract(&ConnectorConfig::new("mongodb")).unwrap();
    assert_eq!(contract.topology, SinkTopology::Singleton);
    assert_eq!(contract.input_mode, SinkInputMode::FullChangelog);
}

#[test]
fn test_sink_contract_cdc_replay() {
    let mut config = test_config();
    config.write_mode = WriteMode::CdcReplay;
    let sink = MongoDbSink::new(super::super::mongodb_cdc_envelope_schema(), config, None);
    let contract = sink.contract(&ConnectorConfig::new("mongodb")).unwrap();
    assert_eq!(contract.topology, SinkTopology::Singleton);
    assert_eq!(contract.input_mode, SinkInputMode::FullChangelog);
}

#[test]
fn batches_to_bson_are_direct_and_working_set_bounded() {
    let batches = vec![test_batch(3)];
    let retained = retained_batch_bytes(&batches[0]);
    let (docs, byte_estimate) = MongoDbSink::batches_to_bson_docs(
        &batches,
        retained,
        MAX_SINK_WORKING_SET_BYTES,
        MAX_STANDARD_DOCUMENT_BYTES,
    )
    .unwrap();
    assert_eq!(docs.len(), 3);
    assert!(byte_estimate > 0);
    assert_eq!(docs[0].get_i64("id").unwrap(), 0);
    assert_eq!(docs[0].get_str("name").unwrap(), "user_0");
}

#[test]
fn arrow_to_bson_maps_scalar_types() {
    use mongodb::bson::Bson;
    let ts = arrow_array::TimestampMillisecondArray::from(vec![Some(1_700_000_000_000), None]);
    assert!(
        matches!(arrow_value_to_bson(&ts, 0).unwrap(), Bson::DateTime(dt) if dt.timestamp_millis() == 1_700_000_000_000)
    );
    assert_eq!(arrow_value_to_bson(&ts, 1).unwrap(), Bson::Null);
    assert_eq!(
        arrow_value_to_bson(&Int64Array::from(vec![42]), 0).unwrap(),
        Bson::Int64(42)
    );
    assert_eq!(
        arrow_value_to_bson(&StringArray::from(vec!["x"]), 0).unwrap(),
        Bson::String("x".to_string())
    );
}

#[test]
fn timestamp_conversion_floors_pre_epoch_values_and_rejects_overflow() {
    let micros = arrow_array::TimestampMicrosecondArray::from(vec![-1]);
    assert_eq!(timestamp_millis(&micros, 0).unwrap(), -1);
    let nanos = arrow_array::TimestampNanosecondArray::from(vec![-1]);
    assert_eq!(timestamp_millis(&nanos, 0).unwrap(), -1);
    let seconds = arrow_array::TimestampSecondArray::from(vec![i64::MAX]);
    assert!(timestamp_millis(&seconds, 0).is_err());
}

#[test]
fn take_buffer_resets_all_accounting() {
    let config = MongoDbSinkConfig::default();
    let mut sink = MongoDbSink::new(test_schema(), config, None);

    let batch = test_batch(5);
    let retained = retained_batch_bytes(&batch);
    sink.buffer.push(batch);
    sink.buffered_rows = 5;
    sink.buffered_retained_bytes = retained;

    let (pending, pending_retained) = sink.take_buffer();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending_retained, retained);
    assert_eq!(sink.buffered_rows, 0);
    assert_eq!(sink.buffered_retained_bytes, 0);
    assert!(sink.buffer.is_empty());
}

#[test]
fn configured_flush_interval_is_runtime_timer_authority() {
    let mut config = test_config();
    config.flush_interval_ms = 37;
    let sink = MongoDbSink::new(test_schema(), config, None);
    assert_eq!(sink.flush_interval(), Duration::from_millis(37));
}

#[test]
fn runtime_write_budget_derives_driver_headroom_and_clamps_client_timeouts() {
    let mut connector = ConnectorConfig::new("mongodb-sink");
    connector.set("sink.write.timeout.ms", "500");
    let timeout = MongoDbSink::configured_write_timeout(&connector).unwrap();
    let mut sink = MongoDbSink::new(test_schema(), test_config(), None);
    sink.write_timeout = timeout;

    assert_eq!(sink.suggested_write_timeout(), Duration::from_millis(500));
    assert_eq!(sink.driver_timeout(), Duration::from_millis(400));
    assert_eq!(
        clamp_client_timeout(Some(Duration::from_secs(2)), sink.driver_timeout()),
        Duration::from_millis(400)
    );
    assert_eq!(
        clamp_client_timeout(Some(Duration::from_millis(50)), sink.driver_timeout()),
        Duration::from_millis(50)
    );
    assert_eq!(
        clamp_client_timeout(Some(Duration::ZERO), sink.driver_timeout()),
        Duration::from_millis(400)
    );

    connector.set("sink.write.timeout.ms", "99");
    assert!(MongoDbSink::configured_write_timeout(&connector).is_err());
}

#[test]
fn mongodb_tls_defaults_to_verified_and_rejects_insecure_mode() {
    use mongodb::options::{ClientOptions, Tls, TlsOptions};

    let mut defaults = ClientOptions::default();
    harden_mongodb_tls(&mut defaults).unwrap();
    assert!(matches!(defaults.tls, Some(Tls::Enabled(_))));

    let mut explicit_plaintext = ClientOptions::default();
    explicit_plaintext.tls = Some(Tls::Disabled);
    harden_mongodb_tls(&mut explicit_plaintext).unwrap();
    assert_eq!(explicit_plaintext.tls, Some(Tls::Disabled));

    let mut insecure = ClientOptions::default();
    insecure.tls = Some(Tls::Enabled(
        TlsOptions::builder()
            .allow_invalid_certificates(true)
            .build(),
    ));
    let error = harden_mongodb_tls(&mut insecure).unwrap_err();
    assert!(error.to_string().contains("tlsInsecure"));
}

#[test]
fn namespace_exists_detection_uses_server_error_code() {
    assert!(is_namespace_exists_code(48));
    assert!(!is_namespace_exists_code(47));
    assert!(!is_namespace_exists_code(49));
}

#[test]
fn retained_limit_counts_variable_width_memory_and_allows_exact_boundary() {
    let narrow = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .unwrap();
    let wide_value = "x".repeat(4096);
    let wide = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(StringArray::from(vec![wide_value.as_str()])),
        ],
    )
    .unwrap();
    let narrow_bytes = retained_batch_bytes(&narrow);
    let wide_bytes = retained_batch_bytes(&wide);
    assert!(wide_bytes > narrow_bytes);

    let exact_limit = narrow_bytes + wide_bytes;
    assert!(!requires_preflush(narrow_bytes, wide_bytes, exact_limit).unwrap());
    assert!(requires_preflush(narrow_bytes, wide_bytes, exact_limit - 1).unwrap());
    assert!(requires_preflush(usize::MAX, 1, usize::MAX).unwrap());
}

#[test]
fn converted_limit_allows_exact_boundary_and_rejects_crossing() {
    assert_eq!(checked_converted_total(5, 7, 12, "test").unwrap(), 12);
    let error = checked_converted_total(5, 8, 12, "test").unwrap_err();
    assert!(!error.is_transient());
}

#[test]
fn one_working_set_budget_covers_retained_models_and_staging() {
    let retained = 1_024;
    let encoded = 2_048;
    let models = 3;
    let staging = 512;
    let exact = retained
        + encoded * MATERIALIZED_BYTE_CHARGE
        + models * WRITE_MODEL_OVERHEAD_BYTES
        + staging;
    assert_eq!(
        working_set_charge(retained, encoded as u64, models, staging),
        Some(exact)
    );
    ensure_working_set(retained, encoded as u64, models, staging, exact, "test").unwrap();
    let error = ensure_working_set(retained, encoded as u64, models, staging, exact - 1, "test")
        .unwrap_err();
    assert!(error.to_string().contains("working set"));
    assert!(working_set_charge(usize::MAX, 1, 1, 1).is_none());
}

#[test]
fn bson_document_limit_uses_exact_encoded_size() {
    let document = mongodb::bson::doc! { "id": 1_i64, "name": "value" };
    let exact = mongodb::bson::to_vec(&document).unwrap().len();
    assert_eq!(
        encoded_document_size(&document, exact, "test document").unwrap(),
        exact
    );
    let error = encoded_document_size(&document, exact - 1, "test document").unwrap_err();
    assert!(!error.is_transient());
    assert!(error.to_string().contains("BSON document"));
}

#[test]
fn time_series_conversion_enforces_four_mib_document_limit() {
    let value = "x".repeat(MAX_TIMESERIES_DOCUMENT_BYTES);
    let batch = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec![value])),
        ],
    )
    .unwrap();
    let retained = retained_batch_bytes(&batch);
    let error = MongoDbSink::batches_to_bson_docs(
        &[batch],
        retained,
        usize::MAX,
        MAX_TIMESERIES_DOCUMENT_BYTES,
    )
    .unwrap_err();
    assert!(error.to_string().contains("4194304"));
}

#[tokio::test]
async fn oversized_batch_rejection_preserves_existing_buffer() {
    let mut sink = MongoDbSink::new(test_schema(), test_config(), None);
    sink.state = ConnectorState::Running;
    let existing = test_batch(1);
    sink.write_batch_with_retained_limit(&existing, usize::MAX)
        .await
        .unwrap();
    let rows_before = sink.buffered_rows;
    let bytes_before = sink.buffered_retained_bytes;
    let batches_before = sink.buffer.len();

    let incoming = test_batch(2);
    let incoming_bytes = retained_batch_bytes(&incoming);
    let error = sink
        .write_batch_with_retained_limit(&incoming, incoming_bytes - 1)
        .await
        .expect_err("oversized batch must fail before admission");

    assert!(!error.is_transient());
    assert_eq!(sink.buffered_rows, rows_before);
    assert_eq!(sink.buffered_retained_bytes, bytes_before);
    assert_eq!(sink.buffer.len(), batches_before);
}

#[tokio::test]
async fn automatic_flush_error_clears_buffer_and_accounting() {
    let mut sink = MongoDbSink::new(test_schema(), test_config(), None);
    sink.state = ConnectorState::Running;
    let error = sink
        .write_batch_with_retained_limit(&test_batch(MAX_DOCUMENTS_PER_FLUSH), usize::MAX)
        .await
        .expect_err("missing collection must fail the automatic flush");

    assert!(matches!(error, ConnectorError::Internal(_)));
    assert!(sink.buffer.is_empty());
    assert_eq!(sink.buffered_rows, 0);
    assert_eq!(sink.buffered_retained_bytes, 0);
}

#[tokio::test]
async fn lifecycle_and_schema_are_checked_before_write() {
    let mut sink = MongoDbSink::new(test_schema(), test_config(), None);
    let state_error = sink.write_batch(&test_batch(1)).await.unwrap_err();
    assert!(matches!(state_error, ConnectorError::InvalidState { .. }));

    sink.state = ConnectorState::Running;
    let other_schema = Arc::new(Schema::new(vec![Field::new(
        "other",
        DataType::Int64,
        false,
    )]));
    let other_batch =
        RecordBatch::try_new(other_schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let schema_error = sink.write_batch(&other_batch).await.unwrap_err();
    assert!(matches!(schema_error, ConnectorError::SchemaMismatch(_)));
}

#[tokio::test]
async fn close_releases_resources_but_returns_pending_flush_error() {
    let mut sink = MongoDbSink::new(test_schema(), test_config(), None);
    sink.state = ConnectorState::Running;
    sink.write_batch_with_retained_limit(&test_batch(1), usize::MAX)
        .await
        .unwrap();

    let error = sink.close().await.expect_err("pending flush must fail");
    assert!(matches!(error, ConnectorError::Internal(_)));
    assert_eq!(sink.state, ConnectorState::Closed);
    assert!(sink.buffer.is_empty());
    assert_eq!(sink.buffered_rows, 0);
    assert_eq!(sink.buffered_retained_bytes, 0);
    assert!(sink.collection.is_none());
    assert!(sink.client.is_none());
}

#[test]
fn cdc_insert_is_prepared_as_document_keyed_idempotent_upsert() {
    let rows = vec![serde_json::json!({
        "_namespace": "db.coll",
        "_op": "I",
        "_document_key": r#"{"_id":"a"}"#,
        "_full_document": r#"{"_id":"a","value":1}"#,
        "_update_desc": null
    })];
    let (writes, bytes) =
        MongoDbSink::prepare_cdc_writes(&rows, MAX_SINK_WORKING_SET_BYTES, "db.coll").unwrap();
    assert!(bytes > 0);
    match &writes[0] {
        CdcWrite::Insert {
            filter,
            replacement,
        } => {
            assert_eq!(
                filter.get_document("_id").unwrap().get_str("$eq").unwrap(),
                "a"
            );
            assert_eq!(replacement.get_i32("value").unwrap(), 1);
        }
        _ => panic!("insert must be a keyed upsert plan"),
    }
}

#[test]
fn cdc_replay_requires_id_but_accepts_complete_sharded_and_document_keys() {
    let missing_id = vec![serde_json::json!({
        "_namespace": "db.coll",
        "_op": "D",
        "_document_key": r#"{"tenant":"a"}"#,
        "_full_document": null,
        "_update_desc": null
    })];
    let error = MongoDbSink::prepare_cdc_writes(&missing_id, MAX_SINK_WORKING_SET_BYTES, "db.coll")
        .unwrap_err();
    assert!(error.to_string().contains("must contain '_id'"));

    for key in [
        r#"{"_id":"a","tenant":"t"}"#,
        r#"{"_id":{"tenant":"t","sequence":1}}"#,
    ] {
        let rows = vec![serde_json::json!({
            "_namespace": "db.coll",
            "_op": "D",
            "_document_key": key,
            "_full_document": null,
            "_update_desc": null
        })];
        MongoDbSink::prepare_cdc_writes(&rows, MAX_SINK_WORKING_SET_BYTES, "db.coll").unwrap();
    }
}

#[test]
fn cdc_replay_rejects_cross_namespace_rows() {
    let rows = vec![serde_json::json!({
        "_namespace": "source.events",
        "_op": "D",
        "_document_key": r#"{"_id":"a"}"#,
        "_full_document": null,
        "_update_desc": null
    })];
    let error = MongoDbSink::prepare_cdc_writes(&rows, MAX_SINK_WORKING_SET_BYTES, "target.events")
        .unwrap_err();
    assert!(error.to_string().contains("fixed target"));
}

#[test]
fn cdc_replay_rejects_replacement_key_drift() {
    let rows = vec![serde_json::json!({
        "_namespace": "db.coll",
        "_op": "I",
        "_document_key": r#"{"_id":"a","tenant":"source"}"#,
        "_full_document": r#"{"_id":"a","tenant":"other","value":1}"#,
        "_update_desc": null
    })];
    let error =
        MongoDbSink::prepare_cdc_writes(&rows, MAX_SINK_WORKING_SET_BYTES, "db.coll").unwrap_err();
    assert!(error.to_string().contains("tenant"), "{error}");
}

#[test]
fn cdc_bulk_models_preserve_mixed_operation_order() {
    use mongodb::options::WriteModel;

    let writes = vec![
        CdcWrite::Insert {
            filter: mongodb::bson::doc! { "_id": "a" },
            replacement: mongodb::bson::doc! { "_id": "a", "v": 1 },
        },
        CdcWrite::Update {
            filter: mongodb::bson::doc! { "_id": "a" },
            update: mongodb::bson::doc! { "$set": { "v": 2 } },
        },
        CdcWrite::Noop,
        CdcWrite::Delete {
            filter: mongodb::bson::doc! { "_id": "a" },
        },
        CdcWrite::Replace {
            filter: mongodb::bson::doc! { "_id": "b" },
            replacement: mongodb::bson::doc! { "_id": "b", "v": 3 },
        },
    ];
    let (models, counts) = cdc_bulk_models(&mongodb::Namespace::new("db", "out"), writes);

    assert_eq!(models.len(), 4);
    assert!(matches!(&models[0], WriteModel::ReplaceOne(model) if model.upsert == Some(true)));
    assert!(matches!(&models[1], WriteModel::UpdateOne(_)));
    assert!(matches!(&models[2], WriteModel::DeleteOne(_)));
    assert!(matches!(&models[3], WriteModel::ReplaceOne(model) if model.upsert == Some(true)));
    assert_eq!(counts.inserts, 1);
    assert_eq!(counts.upserts, 2);
    assert_eq!(counts.deletes, 1);
}

#[test]
fn cdc_update_accepts_source_shape_and_preserves_array_truncation() {
    let update_description = serde_json::json!({
        "updated_fields": {"name": "new"},
        "removed_fields": ["obsolete"],
        "truncated_arrays": [{"field": "items", "new_size": 2}]
    });
    let rows = vec![serde_json::json!({
        "_namespace": "db.coll",
        "_op": "U",
        "_document_key": r#"{"_id":"a"}"#,
        "_full_document": null,
        "_update_desc": update_description.to_string()
    })];
    let (writes, _) =
        MongoDbSink::prepare_cdc_writes(&rows, MAX_SINK_WORKING_SET_BYTES, "db.coll").unwrap();
    match &writes[0] {
        CdcWrite::Update { update, .. } => {
            assert!(update.contains_key("$set"));
            assert!(update.contains_key("$unset"));
            assert!(update.contains_key("$push"));
        }
        _ => panic!("update event must produce an update plan"),
    }
}

#[test]
fn cdc_unknown_operation_fails_closed() {
    for operation in ["FUTURE_OP", "DROP", "RENAME", "INVALIDATE", "DROP_DATABASE"] {
        let rows = vec![serde_json::json!({
            "_namespace": "db.coll",
            "_op": operation
        })];
        let error = MongoDbSink::prepare_cdc_writes(&rows, MAX_SINK_WORKING_SET_BYTES, "db.coll")
            .unwrap_err();
        assert!(!error.is_transient());
        assert!(error.to_string().contains(operation), "{error}");
    }
}

#[test]
fn cdc_ambiguous_update_paths_fail_closed() {
    let update_description = serde_json::json!({
        "updated_fields": {"a.b": 1},
        "removed_fields": [],
        "truncated_arrays": [],
        "disambiguated_paths": {"a.b": ["a.b"]}
    });
    let rows = vec![serde_json::json!({
        "_namespace": "db.coll",
        "_op": "U",
        "_document_key": r#"{"_id":"a"}"#,
        "_full_document": null,
        "_update_desc": update_description.to_string()
    })];

    let error =
        MongoDbSink::prepare_cdc_writes(&rows, MAX_SINK_WORKING_SET_BYTES, "db.coll").unwrap_err();
    assert!(!error.is_transient());
    assert!(error.to_string().contains("ambiguous field paths"));
}

#[test]
fn cdc_full_document_update_uses_idempotent_replacement() {
    let update_description = serde_json::json!({
        "updated_fields": {"a.b": 1},
        "removed_fields": [],
        "truncated_arrays": [],
        "disambiguated_paths": {"a.b": ["a.b"]}
    });
    let rows = vec![serde_json::json!({
        "_namespace": "db.coll",
        "_op": "U",
        "_document_key": r#"{"_id":"a"}"#,
        "_full_document": r#"{"_id":"a","a.b":1}"#,
        "_update_desc": update_description.to_string()
    })];

    let (writes, _) =
        MongoDbSink::prepare_cdc_writes(&rows, MAX_SINK_WORKING_SET_BYTES, "db.coll").unwrap();
    assert!(matches!(&writes[0], CdcWrite::Replace { .. }));
}
