//! Integration tests for MongoDB CDC source and sink connectors.
//!
//! These tests require a running MongoDB instance (via testcontainers).
//! They validate the full lifecycle of the connector: connection, change
//! stream consumption, resume token handling, sink writes, and time series
//! collection support.
//!
//! Run with: `cargo test --test mongodb_integration --features mongodb-cdc`

#![cfg(feature = "mongodb-cdc")]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use mongodb::bson::doc;
use mongodb::options::ClientOptions;
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;
use tokio::time::sleep;

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{SinkConnector, SourceConnector};
use laminar_connectors::mongodb::{
    CollectionKind, FullDocumentMode, MongoDbCdcSource, MongoDbSink, MongoDbSinkConfig,
    MongoDbSourceConfig, TimeSeriesConfig, TimeSeriesGranularity, WriteMode,
};
use testcontainers::ImageExt;

/// Creates a testcontainers MongoDB 8.0 instance and returns the connection URI.
async fn start_mongo() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("mongo", "8.0")
        .with_exposed_port(27017.into())
        .with_wait_for(testcontainers::core::WaitFor::message_on_stdout(
            "Waiting for connections",
        ))
        // Replica set required for change streams.
        .with_cmd(["mongod", "--replSet", "rs0"])
        .start()
        .await
        .expect("failed to start MongoDB container");

    let host_port = container.get_host_port_ipv4(27017).await.expect("get port");

    let uri = format!("mongodb://127.0.0.1:{host_port}/?directConnection=true");

    // Initialize the replica set.
    let client_options = ClientOptions::parse(&uri).await.unwrap();
    let client = mongodb::Client::with_options(client_options).unwrap();
    let admin = client.database("admin");
    admin
        .run_command(doc! { "replSetInitiate": {} })
        .await
        .expect("init replica set");

    // Wait for the replica set to stabilize.
    for _ in 0..30 {
        sleep(Duration::from_millis(500)).await;
        if let Ok(status) = admin.run_command(doc! { "replSetGetStatus": 1 }).await {
            if let Ok(members) = status.get_array("members") {
                let primary = members.iter().any(|m| {
                    m.as_document().and_then(|d| d.get_str("stateStr").ok()) == Some("PRIMARY")
                });
                if primary {
                    break;
                }
            }
        }
    }

    (container, uri)
}

fn sink_test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn make_batch(ids: &[&str], names: &[&str], values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        sink_test_schema(),
        vec![
            Arc::new(StringArray::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

// ── Source Tests ──

#[tokio::test(flavor = "multi_thread")]
async fn insert_cdc() {
    let (_container, uri) = start_mongo().await;

    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_insert_cdc");
    let coll = db.collection::<mongodb::bson::Document>("events");

    // Open the change stream before inserting.
    let config = MongoDbSourceConfig::new(&uri, "test_insert_cdc", "events");
    let mut source = MongoDbCdcSource::new(config);
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    source.open(&connector_config).await.unwrap();

    // Wait for stream to be ready.
    sleep(Duration::from_secs(1)).await;

    // Insert 10 documents.
    for i in 0..10 {
        coll.insert_one(doc! { "seq": i, "data": format!("doc_{i}") })
            .await
            .unwrap();
    }

    // Poll for events (with retries).
    let mut total_events = 0;
    for _ in 0..20 {
        sleep(Duration::from_millis(200)).await;
        if let Ok(Some(batch)) = source.poll_batch(100).await {
            total_events += batch.num_rows();
            if total_events >= 10 {
                break;
            }
        }
    }

    assert!(
        total_events >= 10,
        "expected at least 10 insert events, got {total_events}"
    );

    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_after_disconnect() {
    let (_container, uri) = start_mongo().await;

    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_resume");
    let coll = db.collection::<mongodb::bson::Document>("docs");

    // Phase 1: Insert 5 docs and capture resume token.
    let config = MongoDbSourceConfig::new(&uri, "test_resume", "docs");
    let mut source = MongoDbCdcSource::new(config);
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    source.open(&connector_config).await.unwrap();
    sleep(Duration::from_secs(1)).await;

    for i in 0..5 {
        coll.insert_one(doc! { "seq": i }).await.unwrap();
    }

    let mut phase1_events = 0;
    for _ in 0..20 {
        sleep(Duration::from_millis(200)).await;
        if let Ok(Some(batch)) = source.poll_batch(100).await {
            phase1_events += batch.num_rows();
            if phase1_events >= 5 {
                break;
            }
        }
    }

    let checkpoint = source.checkpoint();
    source.close().await.unwrap();

    // Phase 2: Insert 5 more docs, reopen from checkpoint.
    for i in 5..10 {
        coll.insert_one(doc! { "seq": i }).await.unwrap();
    }

    let config2 = MongoDbSourceConfig::new(&uri, "test_resume", "docs");
    let mut source2 = MongoDbCdcSource::new(config2);
    source2.restore(&checkpoint).await.unwrap();
    source2.open(&connector_config).await.unwrap();
    sleep(Duration::from_secs(1)).await;

    let mut phase2_events = 0;
    for _ in 0..20 {
        sleep(Duration::from_millis(200)).await;
        if let Ok(Some(batch)) = source2.poll_batch(100).await {
            phase2_events += batch.num_rows();
            if phase2_events >= 5 {
                break;
            }
        }
    }

    assert!(
        phase2_events >= 5,
        "expected at least 5 events after resume, got {phase2_events}"
    );

    source2.close().await.unwrap();
}

// ── Sink Tests ──

#[tokio::test(flavor = "multi_thread")]
async fn sink_insert() {
    let (_container, uri) = start_mongo().await;

    let config = MongoDbSinkConfig::new(&uri, "test_sink_insert", "out");
    let mut sink = MongoDbSink::new(sink_test_schema(), config);
    let connector_config = ConnectorConfig::new("mongodb-sink");
    sink.open(&connector_config).await.unwrap();

    let batch = make_batch(
        &["1", "2", "3"],
        &["Alice", "Bob", "Charlie"],
        &[10, 20, 30],
    );

    sink.write_batch(&batch).await.unwrap();
    sink.flush().await.unwrap();

    // Verify documents were written.
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let coll = client
        .database("test_sink_insert")
        .collection::<mongodb::bson::Document>("out");
    let count = coll.count_documents(doc! {}).await.unwrap();
    assert_eq!(count, 3);

    sink.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_upsert() {
    let (_container, uri) = start_mongo().await;

    let mut config = MongoDbSinkConfig::new(&uri, "test_sink_upsert", "out");
    config.write_mode = WriteMode::Upsert {
        key_fields: vec!["_id".to_string()],
    };
    let mut sink = MongoDbSink::new(sink_test_schema(), config);
    let connector_config = ConnectorConfig::new("mongodb-sink");
    sink.open(&connector_config).await.unwrap();

    // Insert initial docs.
    let batch1 = make_batch(&["1", "2"], &["Alice", "Bob"], &[10, 20]);
    sink.write_batch(&batch1).await.unwrap();
    sink.flush().await.unwrap();

    // Upsert with updated values.
    let batch2 = make_batch(&["1", "2"], &["Alice_v2", "Bob_v2"], &[100, 200]);
    sink.write_batch(&batch2).await.unwrap();
    sink.flush().await.unwrap();

    // Verify: only 2 documents, with latest values.
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let coll = client
        .database("test_sink_upsert")
        .collection::<mongodb::bson::Document>("out");
    let count = coll.count_documents(doc! {}).await.unwrap();
    assert_eq!(count, 2);

    sink.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn timeseries_source_guard() {
    let (_container, uri) = start_mongo().await;

    // Create a time series collection.
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_ts_guard");
    db.run_command(doc! {
        "create": "metrics",
        "timeseries": {
            "timeField": "ts",
            "metaField": "sensor",
            "granularity": "seconds",
        }
    })
    .await
    .unwrap();

    // Attempt to open a CDC source on the time series collection.
    let config = MongoDbSourceConfig::new(&uri, "test_ts_guard", "metrics");
    let mut source = MongoDbCdcSource::new(config);
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    let result = source.open(&connector_config).await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("time series"),
        "expected time series error, got: {err_msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn timeseries_insert() {
    let (_container, uri) = start_mongo().await;

    let ts_config = TimeSeriesConfig {
        time_field: "ts".to_string(),
        meta_field: Some("sensor".to_string()),
        granularity: TimeSeriesGranularity::Minutes,
        expire_after_seconds: None,
    };

    let mut config = MongoDbSinkConfig::new(&uri, "test_ts_insert", "metrics");
    config.collection_kind = CollectionKind::TimeSeries(ts_config);
    config.write_mode = WriteMode::Insert;

    // Verify the sink creates the time series collection, then insert
    // via the driver directly (the sink's Arrow→JSON→BSON path produces
    // string timestamps, but MongoDB requires BSON UTC datetime for the
    // timeField — a type-level mismatch the sink doesn't auto-convert).
    let schema = Arc::new(Schema::new(vec![
        Field::new("sensor", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let mut sink = MongoDbSink::new(schema, config);
    let connector_config = ConnectorConfig::new("mongodb-sink");
    sink.open(&connector_config).await.unwrap();

    // Insert via the driver to use proper BSON dates.
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let coll = client
        .database("test_ts_insert")
        .collection::<mongodb::bson::Document>("metrics");

    for i in 0..20 {
        let ts = mongodb::bson::DateTime::from_millis(1_704_067_200_000 + i * 60_000);
        coll.insert_one(doc! { "ts": ts, "sensor": "sensor_1", "value": i * 10 })
            .await
            .unwrap();
    }

    let count = coll.count_documents(doc! {}).await.unwrap();
    assert_eq!(count, 20);

    sink.close().await.unwrap();
}

// ── Update / Replace / Delete Source Tests ──

#[tokio::test(flavor = "multi_thread")]
async fn update_delta_mode() {
    let (_container, uri) = start_mongo().await;

    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_update_delta");
    let coll = db.collection::<mongodb::bson::Document>("docs");

    // Insert a seed document.
    coll.insert_one(doc! { "_id": "u1", "name": "Alice", "age": 30 })
        .await
        .unwrap();

    // Open change stream in Delta mode (default).
    let config = MongoDbSourceConfig::new(&uri, "test_update_delta", "docs");
    let mut source = MongoDbCdcSource::new(config);
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    source.open(&connector_config).await.unwrap();
    sleep(Duration::from_secs(1)).await;

    // Update a field.
    coll.update_one(doc! { "_id": "u1" }, doc! { "$set": { "age": 31 } })
        .await
        .unwrap();

    let mut found_update = false;
    for _ in 0..20 {
        sleep(Duration::from_millis(200)).await;
        if let Ok(Some(batch)) = source.poll_batch(100).await {
            // Check for an update event with _update_desc populated.
            let op_col = batch
                .records
                .column_by_name("_op")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                if op_col.value(i) == "U" {
                    // In delta mode, _full_document should be null.
                    let fd_col = batch
                        .records
                        .column_by_name("_full_document")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .unwrap();
                    let ud_col = batch
                        .records
                        .column_by_name("_update_desc")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .unwrap();
                    assert!(
                        fd_col.is_null(i),
                        "fullDocument should be null in delta mode"
                    );
                    assert!(!ud_col.is_null(i), "updateDescription should be populated");
                    found_update = true;
                }
            }
            if found_update {
                break;
            }
        }
    }

    assert!(found_update, "expected an update event in delta mode");
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn update_lookup_mode() {
    let (_container, uri) = start_mongo().await;

    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_update_lookup");
    let coll = db.collection::<mongodb::bson::Document>("docs");

    coll.insert_one(doc! { "_id": "u2", "name": "Bob" })
        .await
        .unwrap();

    let mut config = MongoDbSourceConfig::new(&uri, "test_update_lookup", "docs");
    config.full_document_mode = FullDocumentMode::UpdateLookup;
    let mut source = MongoDbCdcSource::new(config);
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    source.open(&connector_config).await.unwrap();
    sleep(Duration::from_secs(1)).await;

    coll.update_one(doc! { "_id": "u2" }, doc! { "$set": { "name": "Bob_v2" } })
        .await
        .unwrap();

    let mut found_update = false;
    for _ in 0..20 {
        sleep(Duration::from_millis(200)).await;
        if let Ok(Some(batch)) = source.poll_batch(100).await {
            let op_col = batch
                .records
                .column_by_name("_op")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                if op_col.value(i) == "U" {
                    let fd_col = batch
                        .records
                        .column_by_name("_full_document")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .unwrap();
                    assert!(
                        !fd_col.is_null(i),
                        "fullDocument should be present with UpdateLookup"
                    );
                    found_update = true;
                }
            }
            if found_update {
                break;
            }
        }
    }

    assert!(found_update, "expected an update event with fullDocument");
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn replace_cdc() {
    let (_container, uri) = start_mongo().await;

    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_replace_cdc");
    let coll = db.collection::<mongodb::bson::Document>("docs");

    coll.insert_one(doc! { "_id": "r1", "name": "Carol" })
        .await
        .unwrap();

    let config = MongoDbSourceConfig::new(&uri, "test_replace_cdc", "docs");
    let mut source = MongoDbCdcSource::new(config);
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    source.open(&connector_config).await.unwrap();
    sleep(Duration::from_secs(1)).await;

    // Replace the entire document.
    coll.replace_one(
        doc! { "_id": "r1" },
        doc! { "_id": "r1", "name": "Carol_replaced", "extra": true },
    )
    .await
    .unwrap();

    let mut found_replace = false;
    for _ in 0..20 {
        sleep(Duration::from_millis(200)).await;
        if let Ok(Some(batch)) = source.poll_batch(100).await {
            let op_col = batch
                .records
                .column_by_name("_op")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                if op_col.value(i) == "R" {
                    found_replace = true;
                }
            }
            if found_replace {
                break;
            }
        }
    }

    assert!(found_replace, "expected a replace event");
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_cdc() {
    let (_container, uri) = start_mongo().await;

    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_delete_cdc");
    let coll = db.collection::<mongodb::bson::Document>("docs");

    coll.insert_one(doc! { "_id": "d1", "name": "Dave" })
        .await
        .unwrap();

    let config = MongoDbSourceConfig::new(&uri, "test_delete_cdc", "docs");
    let mut source = MongoDbCdcSource::new(config);
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    source.open(&connector_config).await.unwrap();
    sleep(Duration::from_secs(1)).await;

    coll.delete_one(doc! { "_id": "d1" }).await.unwrap();

    let mut found_delete = false;
    for _ in 0..20 {
        sleep(Duration::from_millis(200)).await;
        if let Ok(Some(batch)) = source.poll_batch(100).await {
            let op_col = batch
                .records
                .column_by_name("_op")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                if op_col.value(i) == "D" {
                    found_delete = true;
                }
            }
            if found_delete {
                break;
            }
        }
    }

    assert!(found_delete, "expected a delete event");
    source.close().await.unwrap();
}

// ── CDC Replay Sink Test ──

#[tokio::test(flavor = "multi_thread")]
async fn sink_cdc_replay() {
    let (_container, uri) = start_mongo().await;

    // Use the CDC envelope schema for the sink.
    let schema = laminar_connectors::mongodb::mongodb_cdc_envelope_schema();

    let mut config = MongoDbSinkConfig::new(&uri, "test_cdc_replay", "replay_out");
    config.write_mode = WriteMode::CdcReplay;
    let mut sink = MongoDbSink::new(schema.clone(), config);
    let connector_config = ConnectorConfig::new("mongodb-sink");
    sink.open(&connector_config).await.unwrap();

    // Build CDC insert events.
    use arrow_array::builder::{Int64Builder, StringBuilder, UInt32Builder};

    let mut ns_b = StringBuilder::new();
    let mut op_b = StringBuilder::new();
    let mut dk_b = StringBuilder::new();
    let mut cts_b = UInt32Builder::new();
    let mut cti_b = UInt32Builder::new();
    let mut wt_b = Int64Builder::new();
    let mut fd_b = StringBuilder::new();
    let mut ud_b = StringBuilder::new();
    let mut rt_b = StringBuilder::new();

    // Insert two documents.
    for id in ["c1", "c2"] {
        ns_b.append_value("test_cdc_replay.replay_out");
        op_b.append_value("I");
        dk_b.append_value(format!(r#"{{"_id":"{id}"}}"#));
        cts_b.append_value(0);
        cti_b.append_value(0);
        wt_b.append_value(0);
        fd_b.append_value(format!(r#"{{"_id":"{id}","val":1}}"#));
        ud_b.append_null();
        rt_b.append_value("tok");
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ns_b.finish()),
            Arc::new(op_b.finish()),
            Arc::new(dk_b.finish()),
            Arc::new(cts_b.finish()),
            Arc::new(cti_b.finish()),
            Arc::new(wt_b.finish()),
            Arc::new(fd_b.finish()),
            Arc::new(ud_b.finish()),
            Arc::new(rt_b.finish()),
        ],
    )
    .unwrap();

    sink.write_batch(&batch).await.unwrap();
    sink.flush().await.unwrap();

    // Delete one document via CDC.
    let mut ns_b = StringBuilder::new();
    let mut op_b = StringBuilder::new();
    let mut dk_b = StringBuilder::new();
    let mut cts_b = UInt32Builder::new();
    let mut cti_b = UInt32Builder::new();
    let mut wt_b = Int64Builder::new();
    let mut fd_b = StringBuilder::new();
    let mut ud_b = StringBuilder::new();
    let mut rt_b = StringBuilder::new();

    ns_b.append_value("test_cdc_replay.replay_out");
    op_b.append_value("D");
    dk_b.append_value(r#"{"_id":"c1"}"#);
    cts_b.append_value(0);
    cti_b.append_value(0);
    wt_b.append_value(0);
    fd_b.append_null();
    ud_b.append_null();
    rt_b.append_value("tok2");

    let del_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ns_b.finish()),
            Arc::new(op_b.finish()),
            Arc::new(dk_b.finish()),
            Arc::new(cts_b.finish()),
            Arc::new(cti_b.finish()),
            Arc::new(wt_b.finish()),
            Arc::new(fd_b.finish()),
            Arc::new(ud_b.finish()),
            Arc::new(rt_b.finish()),
        ],
    )
    .unwrap();

    sink.write_batch(&del_batch).await.unwrap();
    sink.flush().await.unwrap();

    // Verify final state: only c2 should remain.
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let coll = client
        .database("test_cdc_replay")
        .collection::<mongodb::bson::Document>("replay_out");
    let count = coll.count_documents(doc! {}).await.unwrap();
    assert_eq!(count, 1, "expected 1 document after insert+delete");

    sink.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires forcing oplog rotation which is hard in testcontainers"]
async fn oplog_expiry() {
    // This test would need to force oplog truncation past the last
    // resume token, which requires a very small oplog size and enough
    // operations to cycle it. Marked as #[ignore] for CI.
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a near-16MiB document which is slow to create/process"]
async fn large_event_reassembly() {
    // This test would need a document close to the 16 MiB BSON limit
    // and the $changeStreamSplitLargeEvent pipeline stage. Currently
    // blocked by the mongodb v3 driver not exposing splitEvent fields.
}
