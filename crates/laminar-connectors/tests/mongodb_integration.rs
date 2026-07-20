//! Integration tests for MongoDB CDC source and sink connectors.
//!
//! These tests require a running MongoDB instance (via testcontainers).
//! They validate the full lifecycle of the connector: connection, change
//! stream consumption, resume token handling, sink writes, and time series
//! collection support.
//!
//! Run with: `cargo test --test mongodb_integration --features mongodb-cdc`

#![cfg(feature = "mongodb-cdc")]
#![cfg(not(target_os = "windows"))]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures_util::TryStreamExt;
use mongodb::bson::doc;
use mongodb::options::{ClientOptions, IndexOptions};
use mongodb::IndexModel;
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    DeliveryGuarantee, SinkConnector, SourceConnector, SourcePosition, SourceStart,
};
use laminar_connectors::mongodb::lookup::{MongoLookupSource, MongoLookupSourceConfig};
use laminar_connectors::mongodb::{
    CollectionKind, FullDocumentMode, MongoDbCdcSource, MongoDbSink, MongoDbSinkConfig,
    MongoDbSourceConfig, TimeSeriesConfig, TimeSeriesGranularity, WriteMode,
};
use testcontainers::ImageExt;

fn initial_source_start(config: &ConnectorConfig) -> SourceStart {
    SourceStart::new(
        config.clone(),
        SourcePosition::Initial,
        DeliveryGuarantee::BestEffort,
    )
    .unwrap()
}

async fn collection_uuid(db: &mongodb::Database, collection: &str) -> String {
    let mut collections = db
        .list_collections()
        .filter(doc! { "name": collection })
        .await
        .unwrap();
    let specification = collections.try_next().await.unwrap().unwrap();
    let binary = specification.info.uuid.unwrap();
    Uuid::from_slice(&binary.bytes)
        .unwrap()
        .hyphenated()
        .to_string()
}

fn failpoint_count(response: &mongodb::bson::Document) -> i64 {
    response
        .get_i64("count")
        .or_else(|_| response.get_i32("count").map(i64::from))
        .expect("configureFailPoint response count")
}

async fn block_next_aggregate(admin: &mongodb::Database, block_time_ms: i32) -> i64 {
    let response = admin
        .run_command(doc! {
            "configureFailPoint": "failCommand",
            "mode": { "times": 1 },
            "data": {
                "failCommands": ["aggregate"],
                "blockConnection": true,
                "blockTimeMS": block_time_ms,
            },
        })
        .await
        .expect("enable aggregate failpoint");
    failpoint_count(&response)
}

async fn wait_for_failpoint(admin: &mongodb::Database, previous_count: i64) {
    admin
        .run_command(doc! {
            "waitForFailPoint": "failCommand",
            "timesEntered": previous_count + 1,
            "maxTimeMS": 30_000_i32,
        })
        .await
        .expect("aggregate did not enter failpoint");
}

async fn disable_fail_command(admin: &mongodb::Database) -> i64 {
    let response = admin
        .run_command(doc! {
            "configureFailPoint": "failCommand",
            "mode": "off",
        })
        .await
        .expect("disable aggregate failpoint");
    failpoint_count(&response)
}

/// Creates a testcontainers MongoDB 8.0 instance and returns the connection URI.
async fn start_mongo() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("mongo", "8.0")
        .with_exposed_port(27017.into())
        .with_wait_for(testcontainers::core::WaitFor::message_on_stdout(
            "Waiting for connections",
        ))
        // Replica set required for change streams.
        .with_cmd([
            "mongod",
            "--replSet",
            "rs0",
            "--setParameter",
            "enableTestCommands=1",
        ])
        .start()
        .await
        .expect("failed to start MongoDB container");

    let host_port = container.get_host_port_ipv4(27017).await.expect("get port");

    let uri = format!("mongodb://127.0.0.1:{host_port}/?directConnection=true&tls=false");

    // Initialize the replica set.
    let client_options = ClientOptions::parse(&uri).await.unwrap();
    let client = mongodb::Client::with_options(client_options).unwrap();
    let admin = client.database("admin");
    admin
        .run_command(doc! { "replSetInitiate": {} })
        .await
        .expect("init replica set");

    // Wait until the node actually accepts writes. With directConnection=true
    // the driver sends writes straight to this node rather than doing its own
    // primary selection, so `stateStr: PRIMARY` from replSetGetStatus is not
    // enough — a freshly elected primary briefly rejects writes with
    // NotWritablePrimary until catch-up completes. `hello.isWritablePrimary`
    // is the authoritative "ready for writes" signal.
    let mut writable = false;
    for _ in 0..40 {
        if let Ok(hello) = admin.run_command(doc! { "hello": 1 }).await {
            if hello.get_bool("isWritablePrimary").unwrap_or(false) {
                writable = true;
                break;
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
    assert!(writable, "replica set did not become writable in time");

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

async fn collect_insert_sequences_until(source: &mut MongoDbCdcSource, target: i64) -> Vec<i64> {
    let mut sequences = Vec::new();
    loop {
        if let Some(batch) = source.poll_batch(100).await.unwrap() {
            let operations = batch
                .records
                .column_by_name("_op")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let documents = batch
                .records
                .column_by_name("_full_document")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                if operations.value(row) == "I" && !documents.is_null(row) {
                    let document: serde_json::Value =
                        serde_json::from_str(documents.value(row)).unwrap();
                    if let Some(sequence) = document.get("seq").and_then(|value| value.as_i64()) {
                        sequences.push(sequence);
                    }
                }
            }
            if sequences.contains(&target) {
                return sequences;
            }
        }
        sleep(Duration::from_millis(20)).await;
    }
}

fn lookup_config(uri: &str, database: &str, primary_key: &str) -> MongoLookupSourceConfig {
    MongoLookupSourceConfig {
        connection_uri: uri.into(),
        database: database.into(),
        collection: "items".into(),
        primary_key_columns: vec![primary_key.into()],
        schema: Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("lookup_id", DataType::Int64, false),
            Field::new("non_unique", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ])),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn lookup_open_requires_a_usable_unique_key_index() {
    let (_container, uri) = start_mongo().await;
    let database = "test_lookup_index_admission";
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let collection = client
        .database(database)
        .collection::<mongodb::bson::Document>("items");
    collection
        .insert_one(doc! {
            "lookup_id": 1_i64,
            "non_unique": 1_i64,
            "value": "one"
        })
        .await
        .unwrap();

    let missing_error = MongoLookupSource::open(lookup_config(&uri, database, "lookup_id"))
        .await
        .err()
        .expect("an unindexed lookup key must be rejected");
    assert!(missing_error.to_string().contains("unique single-field"));

    collection
        .create_index(IndexModel::builder().keys(doc! { "non_unique": 1 }).build())
        .await
        .unwrap();
    let non_unique_error = MongoLookupSource::open(lookup_config(&uri, database, "non_unique"))
        .await
        .err()
        .expect("a non-unique lookup index must be rejected");
    assert!(non_unique_error.to_string().contains("unique single-field"));

    collection
        .create_index(
            IndexModel::builder()
                .keys(doc! { "lookup_id": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
        )
        .await
        .unwrap();
    MongoLookupSource::open(lookup_config(&uri, database, "lookup_id"))
        .await
        .expect("a unique single-field lookup index must be admitted");

    MongoLookupSource::open(lookup_config(&uri, database, "_id"))
        .await
        .expect("MongoDB's implicit _id index must be admitted");
}

// ── Source Tests ──

#[tokio::test(flavor = "multi_thread")]
async fn insert_cdc() {
    let (_container, uri) = start_mongo().await;

    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_insert_cdc");
    db.create_collection("events").await.unwrap();
    let coll = db.collection::<mongodb::bson::Document>("events");

    // Open the change stream before inserting.
    let config = MongoDbSourceConfig::new(&uri, "test_insert_cdc", "events");
    let mut source = MongoDbCdcSource::new(config, None);
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    source
        .start(initial_source_start(&connector_config))
        .await
        .unwrap();

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
        if let Some(batch) = source.poll_batch(100).await.unwrap() {
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
async fn fresh_start_does_not_replay_the_final_preexisting_write() {
    let (_container, uri) = start_mongo().await;
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_fresh_cut");
    db.create_collection("events").await.unwrap();
    let coll = db.collection::<mongodb::bson::Document>("events");
    coll.insert_one(doc! { "seq": 1_i32, "phase": "before" })
        .await
        .unwrap();

    let config = MongoDbSourceConfig::new(&uri, "test_fresh_cut", "events");
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    let mut source = MongoDbCdcSource::new(config, None);
    source
        .start(initial_source_start(&connector_config))
        .await
        .unwrap();
    assert!(source.checkpoint().get_offset("resume_token").is_some());

    coll.insert_one(doc! { "seq": 2_i32, "phase": "after" })
        .await
        .unwrap();

    let sequences = timeout(
        Duration::from_secs(30),
        collect_insert_sequences_until(&mut source, 2),
    )
    .await
    .expect("post-admission insert was not emitted");

    assert_eq!(sequences, vec![2]);
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_cut_is_stable_when_a_write_races_stream_open() {
    let (_container, uri) = start_mongo().await;
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_bootstrap_write_race");
    let admin = client.database("admin");
    db.create_collection("events").await.unwrap();
    let coll = db.collection::<mongodb::bson::Document>("events");

    let previous_count = block_next_aggregate(&admin, 3_000).await;
    let config = MongoDbSourceConfig::new(&uri, "test_bootstrap_write_race", "events");
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    let start_task = tokio::spawn(async move {
        let mut source = MongoDbCdcSource::new(config, None);
        let result = source.start(initial_source_start(&connector_config)).await;
        (source, result)
    });

    wait_for_failpoint(&admin, previous_count).await;
    coll.insert_one(doc! { "seq": 1_i32, "phase": "before_bootstrap_cut" })
        .await
        .unwrap();

    let (mut source, start_result) = timeout(Duration::from_secs(30), start_task)
        .await
        .expect("source admission timed out")
        .unwrap();
    start_result.unwrap();
    assert!(source.checkpoint().get_offset("resume_token").is_some());

    coll.insert_one(doc! { "seq": 2_i32, "phase": "after_bootstrap_cut" })
        .await
        .unwrap();
    let sequences = timeout(
        Duration::from_secs(30),
        collect_insert_sequences_until(&mut source, 2),
    )
    .await
    .expect("post-bootstrap insert was not emitted");
    assert_eq!(sequences, vec![2]);
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn admission_rejects_drop_recreate_while_bootstrap_aggregate_is_blocked() {
    let (_container, uri) = start_mongo().await;
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_bootstrap_uuid_race");
    let admin = client.database("admin");
    db.create_collection("events").await.unwrap();
    let original_uuid = collection_uuid(&db, "events").await;
    let coll = db.collection::<mongodb::bson::Document>("events");

    let previous_count = block_next_aggregate(&admin, 5_000).await;
    let config = MongoDbSourceConfig::new(&uri, "test_bootstrap_uuid_race", "events");
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    let start_task = tokio::spawn(async move {
        let mut source = MongoDbCdcSource::new(config, None);
        let result = source.start(initial_source_start(&connector_config)).await;
        (source, result)
    });

    wait_for_failpoint(&admin, previous_count).await;
    coll.drop().await.unwrap();
    db.create_collection("events").await.unwrap();
    assert_ne!(original_uuid, collection_uuid(&db, "events").await);

    let (_source, result) = timeout(Duration::from_secs(30), start_task)
        .await
        .expect("source admission timed out")
        .unwrap();
    let error = result.unwrap_err();
    assert!(
        error.to_string().contains("collection identity changed"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_resume_covers_empty_anchor_and_exact_emitted_token() {
    let (_container, uri) = start_mongo().await;

    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_resume");
    db.create_collection("docs").await.unwrap();
    let coll = db.collection::<mongodb::bson::Document>("docs");
    let expected_collection_uuid = collection_uuid(&db, "docs").await;

    // A checkpoint taken before the first event uses the stream's exact opening PBRT.
    let config = MongoDbSourceConfig::new(&uri, "test_resume", "docs");
    let mut source = MongoDbCdcSource::new(config, None);
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    source
        .start(initial_source_start(&connector_config))
        .await
        .unwrap();
    let empty_checkpoint = source.checkpoint();
    assert!(empty_checkpoint.get_offset("resume_token").is_some());
    assert_eq!(empty_checkpoint.offsets().len(), 1);
    assert_eq!(empty_checkpoint.get_metadata("version"), Some("4"));
    assert!(empty_checkpoint
        .get_metadata("deployment_identity")
        .is_some_and(|identity| identity.starts_with("replica-set:")));
    assert_eq!(
        empty_checkpoint.get_metadata("collection_uuid"),
        Some(expected_collection_uuid.as_str())
    );
    source.close().await.unwrap();

    // Writes after the empty checkpoint and before recovery must still be visible.
    for i in 0..5 {
        coll.insert_one(doc! { "seq": i }).await.unwrap();
    }

    let config = MongoDbSourceConfig::new(&uri, "test_resume", "docs");
    let mut source = MongoDbCdcSource::new(config, None);
    source
        .start(
            SourceStart::new(
                connector_config.clone(),
                SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::new(1, 1),
                    checkpoint: empty_checkpoint,
                },
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let mut phase1_events = 0;
    for _ in 0..20 {
        sleep(Duration::from_millis(200)).await;
        if let Some(batch) = source.poll_batch(100).await.unwrap() {
            phase1_events += batch.num_rows();
            if phase1_events >= 5 {
                break;
            }
        }
    }
    assert_eq!(phase1_events, 5);

    let checkpoint = source.checkpoint();
    assert!(checkpoint.get_offset("resume_token").is_some());
    assert_eq!(checkpoint.offsets().len(), 1);
    source.close().await.unwrap();

    // The emitted token resumes strictly after the fifth event.
    for i in 5..8 {
        coll.insert_one(doc! { "seq": i }).await.unwrap();
    }

    let config2 = MongoDbSourceConfig::new(&uri, "test_resume", "docs");
    let mut source2 = MongoDbCdcSource::new(config2, None);
    source2
        .start(
            SourceStart::new(
                connector_config,
                SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::canonical(2),
                    checkpoint,
                },
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let mut phase2_events = 0;
    for _ in 0..20 {
        sleep(Duration::from_millis(200)).await;
        if let Some(batch) = source2.poll_batch(100).await.unwrap() {
            phase2_events += batch.num_rows();
            if phase2_events >= 3 {
                break;
            }
        }
    }
    assert_eq!(phase2_events, 3);
    source2.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_resume_rejects_a_tampered_deployment_identity() {
    let (_container, uri) = start_mongo().await;
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_deployment_identity");
    db.create_collection("docs").await.unwrap();

    let connector_config = ConnectorConfig::new("mongodb-cdc");
    let config = MongoDbSourceConfig::new(&uri, "test_deployment_identity", "docs");
    let mut source = MongoDbCdcSource::new(config, None);
    source
        .start(initial_source_start(&connector_config))
        .await
        .unwrap();
    let mut checkpoint = source.checkpoint();
    source.close().await.unwrap();

    let live_identity = checkpoint
        .get_metadata("deployment_identity")
        .expect("fresh checkpoint deployment identity");
    let (kind, object_id) = live_identity
        .split_once(':')
        .expect("typed deployment identity");
    let replacement_prefix = if object_id.starts_with('0') { '1' } else { '0' };
    let tampered_identity = format!("{kind}:{replacement_prefix}{}", &object_id[1..]);
    checkpoint.set_metadata("deployment_identity", tampered_identity);

    let config = MongoDbSourceConfig::new(&uri, "test_deployment_identity", "docs");
    let mut resumed = MongoDbCdcSource::new(config, None);
    let error = resumed
        .start(
            SourceStart::new(
                connector_config,
                SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::canonical(2),
                    checkpoint,
                },
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(!error.is_transient());
    assert!(
        error.to_string().contains("deployment identity changed"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invalidate_checkpoint_rejects_recreated_collection() {
    let (_container, uri) = start_mongo().await;
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_invalidate_resume");
    let coll = db.collection::<mongodb::bson::Document>("docs");
    coll.insert_one(doc! { "seed": true }).await.unwrap();

    let config = MongoDbSourceConfig::new(&uri, "test_invalidate_resume", "docs");
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    let mut source = MongoDbCdcSource::new(config, None);
    source
        .start(initial_source_start(&connector_config))
        .await
        .unwrap();

    coll.drop().await.unwrap();
    let invalidate_checkpoint = timeout(Duration::from_secs(30), async {
        loop {
            if let Some(batch) = source.poll_batch(100).await.unwrap() {
                let operations = batch
                    .records
                    .column_by_name("_op")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                if operations.iter().flatten().any(|op| op == "INVALIDATE") {
                    break source.checkpoint();
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("invalidate event was not emitted");
    assert!(invalidate_checkpoint
        .get_offset("start_after_token")
        .is_some());
    assert!(invalidate_checkpoint.get_offset("resume_token").is_none());

    let reconnect_error = timeout(Duration::from_secs(30), async {
        loop {
            match source.poll_batch(100).await {
                Ok(_) => sleep(Duration::from_millis(20)).await,
                Err(error) => break error,
            }
        }
    })
    .await
    .expect("reader did not reject the dropped collection on reconnect");
    assert!(
        reconnect_error.to_string().contains("does not exist")
            || reconnect_error
                .to_string()
                .contains("collection identity changed"),
        "{reconnect_error}"
    );
    source.close().await.unwrap();

    coll.insert_one(doc! { "after_recreate": 1_i32 })
        .await
        .unwrap();
    let config = MongoDbSourceConfig::new(&uri, "test_invalidate_resume", "docs");
    let mut resumed = MongoDbCdcSource::new(config, None);
    let error = resumed
        .start(
            SourceStart::new(
                connector_config,
                SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::canonical(3),
                    checkpoint: invalidate_checkpoint,
                },
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("collection identity changed"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_token_can_cut_between_events_from_one_mongodb_transaction() {
    let (_container, uri) = start_mongo().await;
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_transaction_resume");
    db.create_collection("docs").await.unwrap();
    let coll = db.collection::<mongodb::bson::Document>("docs");

    let connector_config = ConnectorConfig::new("mongodb-cdc");
    let config = MongoDbSourceConfig::new(&uri, "test_transaction_resume", "docs");
    let mut source = MongoDbCdcSource::new(config, None);
    source
        .start(initial_source_start(&connector_config))
        .await
        .unwrap();

    let mut session = client.start_session().await.unwrap();
    session.start_transaction().await.unwrap();
    coll.insert_one(doc! { "seq": 1_i32 })
        .session(&mut session)
        .await
        .unwrap();
    coll.insert_one(doc! { "seq": 2_i32 })
        .session(&mut session)
        .await
        .unwrap();
    session.commit_transaction().await.unwrap();

    let first_checkpoint = timeout(Duration::from_secs(30), async {
        loop {
            if let Some(batch) = source.poll_batch(1).await.unwrap() {
                let document = batch
                    .records
                    .column_by_name("_full_document")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0);
                assert!(document.contains(r#""seq":1"#), "{document}");
                break source.checkpoint();
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("first transaction event was not emitted");
    assert!(first_checkpoint.get_offset("resume_token").is_some());
    source.close().await.unwrap();

    let config = MongoDbSourceConfig::new(&uri, "test_transaction_resume", "docs");
    let mut resumed = MongoDbCdcSource::new(config, None);
    resumed
        .start(
            SourceStart::new(
                connector_config,
                SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::canonical(4),
                    checkpoint: first_checkpoint,
                },
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    timeout(Duration::from_secs(30), async {
        loop {
            if let Some(batch) = resumed.poll_batch(1).await.unwrap() {
                let document = batch
                    .records
                    .column_by_name("_full_document")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0);
                assert!(document.contains(r#""seq":2"#), "{document}");
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("resumeAfter missed the remaining transaction event");
    resumed.close().await.unwrap();
}

// ── Sink Tests ──

#[tokio::test(flavor = "multi_thread")]
async fn sink_insert() {
    let (_container, uri) = start_mongo().await;

    let config = MongoDbSinkConfig::new(&uri, "test_sink_insert", "out");
    let mut sink = MongoDbSink::new(sink_test_schema(), config, None);
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
    let mut sink = MongoDbSink::new(sink_test_schema(), config, None);
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

/// A Z-set (`__weight`) changelog from an incremental MV is collapsed per key before the upsert:
/// retract+insert nets to a `replace_one`, and a key with only net-negative weight becomes a
/// `delete_one` (tombstone).
#[tokio::test(flavor = "multi_thread")]
async fn sink_upsert_collapses_zset_changelog() {
    fn changelog(ids: &[&str], names: &[&str], values: &[i64], weights: &[i64]) -> RecordBatch {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_id", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("value", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("__weight", arrow_schema::DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
                Arc::new(Int64Array::from(weights.to_vec())),
            ],
        )
        .unwrap()
    }

    let (_container, uri) = start_mongo().await;
    let mut config = MongoDbSinkConfig::new(&uri, "test_sink_zset", "out");
    config.write_mode = WriteMode::Upsert {
        key_fields: vec!["_id".to_string()],
    };
    // Build with the changelog schema so write_batch accepts `__weight`; the collapse strips it.
    let schema = changelog(&["x"], &["x"], &[0], &[1]).schema();
    let mut sink = MongoDbSink::new(schema, config, None);
    sink.open(&ConnectorConfig::new("mongodb-sink"))
        .await
        .unwrap();

    // Epoch 1: three groups appear.
    sink.write_batch(&changelog(
        &["1", "2", "3"],
        &["Alice", "Bob", "Carol"],
        &[10, 20, 30],
        &[1, 1, 1],
    ))
    .await
    .unwrap();
    sink.flush().await.unwrap();

    // Epoch 2: _id=1 updates (retract 10, insert 15); _id=3 is removed (retract only).
    sink.write_batch(&changelog(
        &["1", "1", "3"],
        &["Alice", "Alice", "Carol"],
        &[10, 15, 30],
        &[-1, 1, -1],
    ))
    .await
    .unwrap();
    sink.flush().await.unwrap();

    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let coll = client
        .database("test_sink_zset")
        .collection::<mongodb::bson::Document>("out");
    let count = coll.count_documents(doc! {}).await.unwrap();
    assert_eq!(
        count, 2,
        "_id=3 deleted, _id=1 updated in place, _id=2 kept"
    );
    let d1 = coll
        .find_one(doc! {"_id": "1"})
        .await
        .unwrap()
        .expect("_id=1 present");
    assert_eq!(
        d1.get_i64("value").unwrap(),
        15,
        "_id=1 collapsed to its latest value 15"
    );
    assert!(
        coll.find_one(doc! {"_id": "3"}).await.unwrap().is_none(),
        "_id=3 tombstoned"
    );

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
    let mut source = MongoDbCdcSource::new(config, None);
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    let result = source.start(initial_source_start(&connector_config)).await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("time series"),
        "expected time series error, got: {err_msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn standard_sink_rejects_existing_timeseries_collection() {
    let (_container, uri) = start_mongo().await;
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_standard_ts_guard");
    db.run_command(doc! {
        "create": "metrics",
        "timeseries": { "timeField": "ts" }
    })
    .await
    .unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
        false,
    )]));
    let config = MongoDbSinkConfig::new(&uri, "test_standard_ts_guard", "metrics");
    let mut sink = MongoDbSink::new(schema, config, None);
    let error = sink
        .open(&ConnectorConfig::new("mongodb-sink"))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Timeseries"), "{error}");
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

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("sensor", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let mut sink = MongoDbSink::new(Arc::clone(&schema), config, None);
    let connector_config = ConnectorConfig::new("mongodb-sink");
    sink.open(&connector_config).await.unwrap();

    let timestamps: Vec<i64> = (0..20).map(|i| 1_704_067_200_000 + i * 60_000).collect();
    let sensors = vec!["sensor_1"; 20];
    let values: Vec<i64> = (0..20).map(|i| i * 10).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(timestamps)),
            Arc::new(StringArray::from(sensors)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap();
    sink.write_batch(&batch).await.unwrap();
    sink.flush().await.unwrap();

    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let coll = client
        .database("test_ts_insert")
        .collection::<mongodb::bson::Document>("metrics");

    let count = coll.count_documents(doc! {}).await.unwrap();
    assert_eq!(count, 20);
    let first = coll
        .find_one(doc! { "value": 0_i64 })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        first.get_datetime("ts").unwrap().timestamp_millis(),
        1_704_067_200_000
    );

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
    let mut source = MongoDbCdcSource::new(config, None);
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    source
        .start(initial_source_start(&connector_config))
        .await
        .unwrap();
    // Update a field.
    coll.update_one(doc! { "_id": "u1" }, doc! { "$set": { "age": 31 } })
        .await
        .unwrap();

    let mut found_update = false;
    for _ in 0..20 {
        sleep(Duration::from_millis(200)).await;
        if let Some(batch) = source.poll_batch(100).await.unwrap() {
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
async fn required_post_image_mode_checks_admission_and_emits_post_image() {
    let (_container, uri) = start_mongo().await;

    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("test_required_post_image");
    let admin = client.database("admin");
    let coll = db.collection::<mongodb::bson::Document>("docs");

    coll.insert_one(doc! { "_id": "u2", "name": "Bob" })
        .await
        .unwrap();

    let mut config = MongoDbSourceConfig::new(&uri, "test_required_post_image", "docs");
    config.full_document_mode = FullDocumentMode::RequirePostImage;
    let mut source = MongoDbCdcSource::new(config, None);
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    let failpoint_before = block_next_aggregate(&admin, 10_000).await;
    let error = source
        .start(initial_source_start(&connector_config))
        .await
        .unwrap_err();
    assert!(!error.is_transient());
    assert!(
        error.to_string().contains("changeStreamPreAndPostImages"),
        "{error}"
    );
    assert_eq!(
        disable_fail_command(&admin).await,
        failpoint_before,
        "required post-image admission must reject before aggregate"
    );

    db.run_command(doc! {
        "collMod": "docs",
        "changeStreamPreAndPostImages": { "enabled": true },
    })
    .await
    .unwrap();
    source
        .start(initial_source_start(&connector_config))
        .await
        .unwrap();
    coll.update_one(doc! { "_id": "u2" }, doc! { "$set": { "name": "Bob_v2" } })
        .await
        .unwrap();

    let mut found_update = false;
    for _ in 0..20 {
        sleep(Duration::from_millis(200)).await;
        if let Some(batch) = source.poll_batch(100).await.unwrap() {
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
                        "fullDocument should be present in required post-image mode"
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
    let mut source = MongoDbCdcSource::new(config, None);
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    source
        .start(initial_source_start(&connector_config))
        .await
        .unwrap();
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
        if let Some(batch) = source.poll_batch(100).await.unwrap() {
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
    let mut source = MongoDbCdcSource::new(config, None);
    let connector_config = ConnectorConfig::new("mongodb-cdc");
    source
        .start(initial_source_start(&connector_config))
        .await
        .unwrap();
    coll.delete_one(doc! { "_id": "d1" }).await.unwrap();

    let mut found_delete = false;
    for _ in 0..20 {
        sleep(Duration::from_millis(200)).await;
        if let Some(batch) = source.poll_batch(100).await.unwrap() {
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
    let mut sink = MongoDbSink::new(schema.clone(), config, None);
    let connector_config = ConnectorConfig::new("mongodb-sink");
    sink.open(&connector_config).await.unwrap();

    // Build CDC insert events.
    use arrow_array::builder::{StringBuilder, TimestampMillisecondBuilder, UInt32Builder};

    let mut ns_b = StringBuilder::new();
    let mut op_b = StringBuilder::new();
    let mut dk_b = StringBuilder::new();
    let mut cts_b = UInt32Builder::new();
    let mut cti_b = UInt32Builder::new();
    let mut wt_b = TimestampMillisecondBuilder::new();
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
    let mut wt_b = TimestampMillisecondBuilder::new();
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
