use arrow_array::{Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use mongodb::bson::{doc, oid::ObjectId, Document};

use super::admission::{await_mongo_reader_ready, source_client_options};
use super::buffering::{buffered_retained_bytes, ChangeRecord, SnapshotRecord};
use super::checkpoint::{
    canonical_resume_token, parse_deployment_identity, set_emitted_offsets, RESUME_TOKEN_OFFSET,
    SEQUENCE_OFFSET, SNAPSHOT_AFTER_KEY_OFFSET, SNAPSHOT_AT_OFFSET, START_AFTER_TOKEN_OFFSET,
    START_AT_OFFSET,
};
use super::decoding::{event_operation, event_token};
use super::reader::{
    acquire_mongo_byte_permit, bootstrap_change_stream_options, change_stream_options,
    send_event_or_shutdown, verify_mongodb_collection, verify_mongodb_collection_uuid,
    verify_mongodb_deployment_identity,
};
use super::*;
use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{
    source_mutations, SourceConnector, SourceConsistency, SourceInputMode, SourcePosition,
    SourceStart,
};
use crate::mongodb::change_event::canonical_extjson;
use crate::mongodb::config::{FullDocumentMode, SnapshotMode};

const TEST_COLLECTION_UUID: &str = "123e4567-e89b-12d3-a456-426614174000";
const TEST_DEPLOYMENT_OBJECT_ID: &str = "0123456789abcdef01234567";
const TEST_DEPLOYMENT_IDENTITY: &str = "replica-set:0123456789abcdef01234567";

struct TaskDropSignal(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for TaskDropSignal {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

fn test_collection_uuid() -> Uuid {
    Uuid::parse_str(TEST_COLLECTION_UUID).unwrap()
}

fn test_deployment_identity() -> MongoDeploymentIdentity {
    MongoDeploymentIdentity::ReplicaSet(TEST_DEPLOYMENT_OBJECT_ID.into())
}

fn anchor() -> EmittedPosition {
    EmittedPosition {
        position: MongoCheckpointPosition::Stream(StreamPosition::ResumeAfter(
            r#"{"_data":"anchor"}"#.into(),
        )),
        next_sequence: 0,
    }
}

fn admitted_source(config: MongoDbSourceConfig) -> MongoDbCdcSource {
    let mut source = MongoDbCdcSource::new(config, None);
    source.collection_uuid = Some(test_collection_uuid());
    source.deployment_identity = Some(test_deployment_identity());
    source.emitted = Some(anchor());
    source
}

fn history_source() -> MongoDbCdcSource {
    admitted_source(MongoDbSourceConfig::new(
        "mongodb://localhost:27017",
        "testdb",
        "users",
    ))
}

fn document_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int64, true),
    ]))
}

fn document_config() -> MongoDbSourceConfig {
    let mut config = MongoDbSourceConfig::new("mongodb://localhost:27017", "testdb", "users");
    config.output_mode = SourceOutputMode::Document;
    config.full_document_mode = FullDocumentMode::RequirePostImage;
    config
}

fn document_source() -> MongoDbCdcSource {
    let mut source = admitted_source(document_config());
    let projection =
        DocumentProjection::try_new(&document_schema(), &["_id".into()], &[], None).unwrap();
    source.schema = Arc::clone(projection.schema());
    source.projection = Some(projection);
    source
}

fn valid_connector_config() -> ConnectorConfig {
    let mut config = ConnectorConfig::new("mongodb-cdc");
    config.set("connection.uri", "mongodb://localhost:27017");
    config.set("database", "testdb");
    config.set("collection", "users");
    config
}

fn change(token: &str, operation: &str, body: Document) -> BufferedMongoPayload {
    let mut event = doc! {
        "_id": { "_data": token },
        "operationType": operation,
        "ns": { "db": "testdb", "coll": "users" },
        "clusterTime": mongodb::bson::Timestamp { time: 1_700_000_000, increment: 7 },
        "wallTime": mongodb::bson::DateTime::from_millis(1_700_000_000_123),
    };
    event.extend(body);
    let raw = RawDocumentBuf::from_document(&event).unwrap();
    BufferedMongoPayload::Change(ChangeRecord {
        token: canonical_resume_token(&event_token(&raw).unwrap()).unwrap(),
        operation: event_operation(&raw).unwrap(),
        raw,
    })
}

fn put(token: &str, operation: &str, id: &str, name: &str) -> BufferedMongoPayload {
    change(
        token,
        operation,
        doc! {
            "documentKey": { "_id": id },
            "fullDocument": { "_id": id, "name": name, "age": 41_i64 },
        },
    )
}

fn delete(token: &str, id: &str) -> BufferedMongoPayload {
    change(token, "delete", doc! { "documentKey": { "_id": id } })
}

fn snapshot_row(document: &Document) -> BufferedMongoPayload {
    let raw = RawDocumentBuf::from_document(document).unwrap();
    let key = canonical_extjson(raw.get("_id").unwrap().unwrap()).unwrap();
    BufferedMongoPayload::Snapshot(SnapshotRecord { raw, key })
}

fn enqueue(source: &mut MongoDbCdcSource, payload: BufferedMongoPayload) {
    let bytes = u32::try_from(buffered_retained_bytes(&payload).unwrap()).unwrap();
    let permit = Arc::clone(&source.byte_budget)
        .try_acquire_many_owned(bytes)
        .expect("test item exceeds the byte budget");
    source
        .event_buffer
        .push_back(BufferedMongoEvent::new(payload, permit));
}

fn strings<'a>(batch: &'a arrow_array::RecordBatch, column: &str) -> &'a StringArray {
    batch
        .column(batch.schema().index_of(column).unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
}

fn recovery_checkpoint(config: &MongoDbSourceConfig, offsets: &[(&str, &str)]) -> SourceCheckpoint {
    let mut checkpoint = SourceCheckpoint::new();
    for (key, value) in offsets {
        checkpoint.set_offset(*key, *value);
    }
    checkpoint.set_metadata("connector", MONGODB_CHECKPOINT_CONNECTOR);
    checkpoint.set_metadata("version", MONGODB_CHECKPOINT_VERSION);
    checkpoint.set_metadata("database", &config.database);
    checkpoint.set_metadata("collection", &config.collection);
    checkpoint.set_metadata(COLLECTION_UUID_METADATA, TEST_COLLECTION_UUID);
    checkpoint.set_metadata(DEPLOYMENT_IDENTITY_METADATA, TEST_DEPLOYMENT_IDENTITY);
    checkpoint.set_metadata(STREAM_IDENTITY_METADATA, mongodb_stream_identity(config));
    checkpoint
}

// ── History records ──

#[test]
fn history_schema_uses_no_engine_changelog_names() {
    let schema = mongodb_history_schema();
    assert_eq!(schema.field(0).name(), "event_id");
    for field in schema.fields() {
        assert!(
            !["_op", "__op", "_ts_ms", "__weight"].contains(&field.name().as_str()),
            "{} would be interpreted as an engine mutation",
            field.name()
        );
        assert!(!field.name().starts_with('_'), "{}", field.name());
    }
}

#[test]
fn history_keeps_every_change_in_order_with_stable_identity() {
    let mut source = history_source();
    enqueue(&mut source, put("t1", "insert", "a", "first"));
    enqueue(
        &mut source,
        change(
            "t2",
            "update",
            doc! {
                "documentKey": { "_id": "a" },
                "updateDescription": {
                    "updatedFields": { "name": "second" },
                    "removedFields": [],
                    "truncatedArrays": [],
                },
            },
        ),
    );
    enqueue(&mut source, put("t3", "update", "a", "third"));
    enqueue(&mut source, put("t4", "replace", "a", "fourth"));
    enqueue(&mut source, delete("t5", "a"));

    let batch = source.drain_to_batch(10).unwrap().unwrap().records;
    assert_eq!(
        batch.num_rows(),
        5,
        "repeated changes to one key are not collapsed"
    );
    let operations = strings(&batch, "operation");
    assert_eq!(
        (0..5).map(|row| operations.value(row)).collect::<Vec<_>>(),
        ["insert", "update", "update", "replace", "delete"]
    );
    let ids = strings(&batch, "event_id");
    let unique: std::collections::BTreeSet<_> = (0..5).map(|row| ids.value(row)).collect();
    assert_eq!(unique.len(), 5);
    assert!(strings(&batch, "full_document").is_null(1));
    assert!(strings(&batch, "full_document").is_null(4));
    assert!(!strings(&batch, "update_description").is_null(1));
    assert_eq!(
        strings(&batch, "resume_token").value(4),
        r#"{"_data":"t5"}"#
    );

    // The identity is a function of the opaque token and the bound incarnation only.
    let mut replay = history_source();
    enqueue(&mut replay, put("t1", "insert", "a", "first"));
    let replayed = replay.drain_to_batch(1).unwrap().unwrap().records;
    assert_eq!(strings(&replayed, "event_id").value(0), ids.value(0));
    assert_eq!(
        source.checkpoint().get_offset(RESUME_TOKEN_OFFSET),
        Some(r#"{"_data":"t5"}"#)
    );
    assert_eq!(source.checkpoint().get_offset(SEQUENCE_OFFSET), Some("5"));
}

#[test]
fn history_preserves_exact_key_and_document_types() {
    let mut source = history_source();
    let object_id = ObjectId::parse_str("65a1b2c3d4e5f60718293a4b").unwrap();
    enqueue(
        &mut source,
        change(
            "t1",
            "insert",
            doc! {
                "documentKey": { "_id": object_id },
                "fullDocument": { "_id": object_id, "n": 5_i64, "price": "1.10".parse::<mongodb::bson::Decimal128>().unwrap() },
                "txnNumber": 3_i64,
                "lsid": { "id": mongodb::bson::Binary { subtype: mongodb::bson::spec::BinarySubtype::Uuid, bytes: vec![1; 16] } },
            },
        ),
    );
    enqueue(
        &mut source,
        put("t2", "insert", "65a1b2c3d4e5f60718293a4b", "string"),
    );
    let batch = source.drain_to_batch(10).unwrap().unwrap().records;
    let keys = strings(&batch, "document_key");
    assert_eq!(
        keys.value(0),
        r#"{"_id":{"$oid":"65a1b2c3d4e5f60718293a4b"}}"#
    );
    assert_eq!(keys.value(1), r#"{"_id":"65a1b2c3d4e5f60718293a4b"}"#);
    let document: serde_json::Value =
        serde_json::from_str(strings(&batch, "full_document").value(0)).unwrap();
    assert_eq!(document["n"], serde_json::json!({"$numberLong": "5"}));
    assert_eq!(
        document["price"],
        serde_json::json!({"$numberDecimal": "1.10"})
    );
    let txn = batch
        .column(batch.schema().index_of("txn_number").unwrap())
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(txn.value(0), 3);
    assert!(txn.is_null(1));
    assert!(!strings(&batch, "lsid").is_null(0));
}

#[test]
fn history_retains_lifecycle_details_and_fences_the_collection_uuid() {
    let mut source = history_source();
    enqueue(
        &mut source,
        change(
            "t1",
            "rename",
            doc! {
                "to": { "db": "testdb", "coll": "users_v2" },
                "collectionUUID": mongodb::bson::Binary {
                    subtype: mongodb::bson::spec::BinarySubtype::Uuid,
                    bytes: test_collection_uuid().as_bytes().to_vec(),
                },
            },
        ),
    );
    let batch = source.drain_to_batch(1).unwrap().unwrap().records;
    assert_eq!(strings(&batch, "operation").value(0), "rename");
    let details: serde_json::Value =
        serde_json::from_str(strings(&batch, "event_details").value(0)).unwrap();
    assert_eq!(details["to"]["coll"], "users_v2");

    let mut source = history_source();
    enqueue(
        &mut source,
        change(
            "t2",
            "insert",
            doc! {
                "documentKey": { "_id": 1 },
                "fullDocument": { "_id": 1 },
                "collectionUUID": mongodb::bson::Binary {
                    subtype: mongodb::bson::spec::BinarySubtype::Uuid,
                    bytes: vec![9; 16],
                },
            },
        ),
    );
    let error = source.drain_to_batch(1).unwrap_err();
    assert!(
        error.to_string().contains("different collection UUID"),
        "{error}"
    );
    assert_eq!(
        source.event_buffer.len(),
        1,
        "a rejected batch keeps the queue intact"
    );
}

#[test]
fn idle_high_watermarks_never_overtake_queued_records() {
    let mut source = history_source();
    enqueue(&mut source, put("event", "insert", "a", "x"));
    enqueue(
        &mut source,
        BufferedMongoPayload::HighWatermark {
            token: r#"{"_data":"post_batch"}"#.into(),
            requires_start_after: false,
        },
    );
    enqueue(&mut source, put("later", "insert", "b", "y"));

    assert_eq!(source.drain_to_batch(2).unwrap().unwrap().num_rows(), 1);
    assert_eq!(
        source.checkpoint().get_offset(RESUME_TOKEN_OFFSET),
        Some(r#"{"_data":"post_batch"}"#)
    );
    assert_eq!(source.event_buffer.len(), 1);
    assert_eq!(source.drain_to_batch(1).unwrap().unwrap().num_rows(), 1);
    enqueue(
        &mut source,
        BufferedMongoPayload::HighWatermark {
            token: r#"{"_data":"idle"}"#.into(),
            requires_start_after: false,
        },
    );
    assert!(source.drain_to_batch(1).unwrap().is_none());
    assert_eq!(
        source.checkpoint().get_offset(RESUME_TOKEN_OFFSET),
        Some(r#"{"_data":"idle"}"#)
    );
    assert_eq!(
        source.checkpoint().get_offset(SEQUENCE_OFFSET),
        Some("2"),
        "progress markers do not consume row sequence numbers"
    );
}

#[test]
fn invalidation_ends_the_batch_and_checkpoints_as_start_after() {
    let mut source = history_source();
    enqueue(&mut source, change("inv", "invalidate", Document::new()));
    enqueue(&mut source, put("after", "insert", "a", "x"));

    assert_eq!(source.drain_to_batch(10).unwrap().unwrap().num_rows(), 1);
    let checkpoint = source.checkpoint();
    assert_eq!(
        checkpoint.get_offset(START_AFTER_TOKEN_OFFSET),
        Some(r#"{"_data":"inv"}"#)
    );
    assert!(checkpoint.get_offset(RESUME_TOKEN_OFFSET).is_none());
    assert_eq!(source.event_buffer.len(), 1);
}

#[test]
fn draining_releases_byte_ownership() {
    let mut source = history_source();
    enqueue(&mut source, put("t1", "insert", "a", "x"));
    assert!(source.byte_budget.available_permits() < source.config.max_buffered_bytes);
    source.drain_to_batch(1).unwrap().unwrap();
    assert_eq!(
        source.byte_budget.available_permits(),
        source.config.max_buffered_bytes
    );
}

// ── Document replication ──

#[test]
fn document_mode_emits_puts_and_key_only_tombstones_with_positions() {
    let mut source = document_source();
    enqueue(&mut source, put("t1", "insert", "a", "first"));
    enqueue(&mut source, put("t2", "update", "a", "second"));
    enqueue(&mut source, delete("t3", "a"));
    enqueue(&mut source, put("t4", "insert", "a", "reborn"));

    let batch = source.drain_to_batch(10).unwrap().unwrap();
    let records = batch.records.clone();
    assert_eq!(records.num_rows(), 4);
    assert_eq!(
        batch.mutations().unwrap(),
        &[
            SourceMutation::Put,
            SourceMutation::Put,
            SourceMutation::Tombstone,
            SourceMutation::Put
        ]
    );
    let names = strings(&records, "name");
    assert_eq!(names.value(1), "second");
    assert!(names.is_null(2), "tombstones carry only the key");
    assert!(records.column(2).is_null(2));
    assert_eq!(strings(&records, "_id").value(2), "a");
    let positions = batch.row_positions().unwrap();
    let orders: Vec<u64> = (0..4)
        .map(|row| u64::from_be_bytes(positions.order_key().value(row).try_into().unwrap()))
        .collect();
    assert_eq!(orders, [0, 1, 2, 3]);
    assert_eq!(
        positions.partition().value(0),
        test_collection_uuid().as_bytes()
    );

    let encoded = batch
        .into_records_with_metadata(
            crate::connector::SourceRowPositionCapability::OrderedDeterministic,
            &crate::connector::schema_with_source_row_positions(&document_schema()).unwrap(),
            &crate::connector::schema_with_source_mutations_and_row_positions(&document_schema())
                .unwrap(),
        )
        .unwrap();
    assert_eq!(source_mutations(&encoded).unwrap().unwrap().len(), 4);
}

#[test]
fn document_sequences_do_not_depend_on_poll_size_or_progress_markers() {
    let events = || {
        vec![
            put("t1", "insert", "a", "1"),
            BufferedMongoPayload::HighWatermark {
                token: r#"{"_data":"hw"}"#.into(),
                requires_start_after: false,
            },
            put("t2", "update", "a", "2"),
            change(
                "t3",
                "createIndexes",
                doc! { "operationDescription": { "indexes": [] } },
            ),
            delete("t4", "a"),
        ]
    };
    let collect = |poll: usize| {
        let mut source = document_source();
        for event in events() {
            enqueue(&mut source, event);
        }
        let mut orders = Vec::new();
        while !source.event_buffer.is_empty() {
            if let Some(batch) = source.drain_to_batch(poll).unwrap() {
                let positions = batch.row_positions().unwrap();
                for row in 0..batch.num_rows() {
                    orders.push(u64::from_be_bytes(
                        positions.order_key().value(row).try_into().unwrap(),
                    ));
                }
            }
        }
        (orders, source.checkpoint())
    };
    let (one, one_checkpoint) = collect(1);
    let (all, all_checkpoint) = collect(100);
    assert_eq!(one, [0, 1, 2]);
    assert_eq!(one, all);
    assert_eq!(one_checkpoint, all_checkpoint);
}

#[test]
fn document_mode_never_treats_a_missing_image_as_a_delete() {
    let mut source = document_source();
    enqueue(
        &mut source,
        change("t1", "update", doc! { "documentKey": { "_id": "a" } }),
    );
    let error = source.drain_to_batch(1).unwrap_err();
    assert!(error.to_string().contains("missing image"), "{error}");
    assert!(!error.is_transient());
    assert_eq!(source.checkpoint().get_offset(SEQUENCE_OFFSET), Some("0"));
}

#[test]
fn document_mode_stops_before_destructive_lifecycle_events() {
    for operation in [
        "drop",
        "rename",
        "dropDatabase",
        "invalidate",
        "futureEvent",
    ] {
        let mut source = document_source();
        enqueue(&mut source, change("t1", operation, Document::new()));
        let error = source.drain_to_batch(1).unwrap_err();
        assert!(
            error.to_string().contains("not changed"),
            "{operation}: {error}"
        );
        assert_eq!(
            source.checkpoint().get_offset(RESUME_TOKEN_OFFSET),
            Some(r#"{"_data":"anchor"}"#),
            "the checkpoint stays before {operation}"
        );
    }
}

#[test]
fn document_mode_rejects_key_shape_and_in_place_key_changes() {
    let mut source = document_source();
    enqueue(
        &mut source,
        change(
            "t1",
            "insert",
            doc! {
                "documentKey": { "region": "eu", "_id": "a" },
                "fullDocument": { "_id": "a", "region": "eu" },
            },
        ),
    );
    let error = source.drain_to_batch(1).unwrap_err();
    assert!(error.to_string().contains("do not match"), "{error}");

    let mut source = document_source();
    enqueue(
        &mut source,
        change(
            "t1",
            "replace",
            doc! {
                "documentKey": { "_id": "a" },
                "fullDocument": { "_id": "b" },
            },
        ),
    );
    let error = source.drain_to_batch(1).unwrap_err();
    assert!(error.to_string().contains("in-place key change"), "{error}");
}

// ── Snapshot progress ──

#[test]
fn snapshot_rows_advance_the_durable_cut_then_hand_off_to_the_stream() {
    let at = mongodb::bson::Timestamp {
        time: 1_700_000_000,
        increment: 4,
    };
    let mut source = document_source();
    source.emitted = Some(EmittedPosition {
        position: MongoCheckpointPosition::Snapshot(SnapshotCut {
            at,
            after_key: None,
        }),
        next_sequence: 0,
    });
    enqueue(&mut source, snapshot_row(&doc! { "_id": "a", "name": "x" }));
    enqueue(&mut source, snapshot_row(&doc! { "_id": "b", "name": "y" }));

    let batch = source.drain_to_batch(1).unwrap().unwrap();
    assert!(batch.mutations().is_none(), "snapshot rows are puts");
    let checkpoint = source.checkpoint();
    assert_eq!(
        checkpoint.get_offset(SNAPSHOT_AT_OFFSET),
        Some("1700000000.4")
    );
    assert_eq!(
        checkpoint.get_offset(SNAPSHOT_AFTER_KEY_OFFSET),
        Some(r#""a""#)
    );
    let restored = parse_mongodb_checkpoint(&checkpoint, &source.config);
    assert!(
        restored.is_err(),
        "snapshot progress requires snapshot.mode=initial in the configuration"
    );
    source.config.snapshot_mode = SnapshotMode::Initial;
    let restored = parse_mongodb_checkpoint(&source.checkpoint(), &source.config).unwrap();
    assert_eq!(restored.emitted.next_sequence, 1);

    enqueue(&mut source, BufferedMongoPayload::SnapshotComplete);
    enqueue(&mut source, put("t1", "update", "a", "changed"));
    let batch = source.drain_to_batch(10).unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);
    let positions = batch.row_positions().unwrap();
    assert_eq!(positions.order_key().value(0), 1_u64.to_be_bytes());
    assert_eq!(positions.order_key().value(1), 2_u64.to_be_bytes());
    assert_eq!(
        source.checkpoint().get_offset(RESUME_TOKEN_OFFSET),
        Some(r#"{"_data":"t1"}"#)
    );

    let mut complete_only = document_source();
    complete_only.config.snapshot_mode = SnapshotMode::Initial;
    complete_only.emitted = Some(EmittedPosition {
        position: MongoCheckpointPosition::Snapshot(SnapshotCut {
            at,
            after_key: Some(r#""b""#.into()),
        }),
        next_sequence: 2,
    });
    enqueue(&mut complete_only, BufferedMongoPayload::SnapshotComplete);
    assert!(complete_only.drain_to_batch(1).unwrap().is_none());
    assert_eq!(
        complete_only.checkpoint().get_offset(START_AT_OFFSET),
        Some("1700000000.4"),
        "after the scan the stream resumes inclusively at the snapshot time"
    );
}

#[test]
fn history_snapshot_rows_are_provenanced_copies() {
    let at = mongodb::bson::Timestamp {
        time: 1_700_000_000,
        increment: 4,
    };
    let mut source = history_source();
    source.emitted = Some(EmittedPosition {
        position: MongoCheckpointPosition::Snapshot(SnapshotCut {
            at,
            after_key: None,
        }),
        next_sequence: 0,
    });
    enqueue(&mut source, snapshot_row(&doc! { "_id": 7_i64, "v": 1 }));
    let batch = source.drain_to_batch(1).unwrap().unwrap().records;
    assert_eq!(strings(&batch, "operation").value(0), SNAPSHOT_OPERATION);
    assert_eq!(strings(&batch, "snapshot_id").value(0), "1700000000.4");
    assert!(strings(&batch, "resume_token").is_null(0));
    assert_eq!(
        strings(&batch, "document_key").value(0),
        r#"{"_id":{"$numberLong":"7"}}"#
    );
}

#[tokio::test]
async fn committed_snapshot_cut_opens_the_copy_gate() {
    let at = mongodb::bson::Timestamp {
        time: 10,
        increment: 2,
    };
    let mut config = document_config();
    config.snapshot_mode = SnapshotMode::Initial;
    let mut source = admitted_source(config.clone());
    source.emitted = Some(EmittedPosition {
        position: MongoCheckpointPosition::Snapshot(SnapshotCut {
            at,
            after_key: None,
        }),
        next_sequence: 0,
    });
    let (tx, rx) = tokio::sync::watch::channel(false);
    source.snapshot_committed = Some((at, tx));

    source
        .notify_epoch_committed(1, &SourceCheckpoint::new())
        .await
        .unwrap();
    assert!(
        !*rx.borrow(),
        "an empty timer commit does not open the gate"
    );

    let committed = source.checkpoint();
    source.notify_epoch_committed(2, &committed).await.unwrap();
    assert!(*rx.borrow());
    assert!(source.snapshot_committed.is_none());
}

// ── Checkpoints ──

#[test]
fn checkpoint_requires_admitted_identity_and_position() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let mut source = MongoDbCdcSource::new(config, None);
    assert!(source.checkpoint().is_empty());
    source.collection_uuid = Some(test_collection_uuid());
    source.deployment_identity = Some(test_deployment_identity());
    assert!(source.checkpoint().is_empty());
    source.emitted = Some(anchor());
    let checkpoint = source.checkpoint();
    assert_eq!(checkpoint.get_metadata("version"), Some("5"));
    assert_eq!(
        checkpoint.get_metadata(DEPLOYMENT_IDENTITY_METADATA),
        Some(TEST_DEPLOYMENT_IDENTITY)
    );
    let parsed = parse_mongodb_checkpoint(&checkpoint, &source.config).unwrap();
    assert_eq!(parsed.emitted, anchor());
    assert_eq!(parsed.collection_uuid, test_collection_uuid());
}

#[test]
fn every_emitted_position_round_trips() {
    let mut config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    config.snapshot_mode = SnapshotMode::Initial;
    let at = mongodb::bson::Timestamp {
        time: 99,
        increment: 0,
    };
    for position in [
        MongoCheckpointPosition::Stream(StreamPosition::ResumeAfter(r#"{"_data":"r"}"#.into())),
        MongoCheckpointPosition::Stream(StreamPosition::StartAfter(r#"{"_data":"s"}"#.into())),
        MongoCheckpointPosition::Stream(StreamPosition::StartAt(at)),
        MongoCheckpointPosition::Snapshot(SnapshotCut {
            at,
            after_key: None,
        }),
        MongoCheckpointPosition::Snapshot(SnapshotCut {
            at,
            after_key: Some(r#"{"$oid":"65a1b2c3d4e5f60718293a4b"}"#.into()),
        }),
    ] {
        let emitted = EmittedPosition {
            position,
            next_sequence: 42,
        };
        let mut checkpoint = recovery_checkpoint(&config, &[]);
        set_emitted_offsets(&mut checkpoint, &emitted);
        assert_eq!(
            parse_mongodb_checkpoint(&checkpoint, &config)
                .unwrap()
                .emitted,
            emitted
        );
    }
}

#[test]
fn checkpoint_parser_rejects_ambiguous_noncanonical_and_unknown_positions() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let token = r#"{"_data":"token"}"#;
    for (offsets, needle) in [
        (
            vec![
                (RESUME_TOKEN_OFFSET, token),
                (START_AFTER_TOKEN_OFFSET, token),
                (SEQUENCE_OFFSET, "1"),
            ],
            "exactly one",
        ),
        (
            vec![
                (RESUME_TOKEN_OFFSET, r#"{"_data": "token"}"#),
                (SEQUENCE_OFFSET, "1"),
            ],
            "canonical",
        ),
        (vec![(RESUME_TOKEN_OFFSET, token)], "sequence"),
        (
            vec![(RESUME_TOKEN_OFFSET, token), (SEQUENCE_OFFSET, "01")],
            "sequence",
        ),
        (
            vec![(START_AT_OFFSET, "10:2"), (SEQUENCE_OFFSET, "1")],
            "cluster time",
        ),
        (
            vec![
                (RESUME_TOKEN_OFFSET, token),
                (SEQUENCE_OFFSET, "1"),
                ("unknown", "x"),
            ],
            "unknown position",
        ),
        (
            vec![("unknown_position", "x"), (SEQUENCE_OFFSET, "1")],
            "exactly one",
        ),
    ] {
        let checkpoint = recovery_checkpoint(&config, &offsets);
        let error = parse_mongodb_checkpoint(&checkpoint, &config).unwrap_err();
        assert!(error.to_string().contains(needle), "{offsets:?}: {error}");
    }
    let oversized = "x".repeat(MAX_RESUME_TOKEN_BYTES + 1);
    let checkpoint = recovery_checkpoint(
        &config,
        &[(RESUME_TOKEN_OFFSET, &oversized), (SEQUENCE_OFFSET, "1")],
    );
    assert!(parse_mongodb_checkpoint(&checkpoint, &config).is_err());
}

#[test]
fn checkpoint_binds_stream_shape_but_not_endpoint_or_buffering() {
    let mut config = MongoDbSourceConfig::new("mongodb://one:27017", "db", "coll");
    config.pipeline = vec![serde_json::json!({
        "$match": { "operationType": "insert", "ns.db": "db" }
    })];
    let checkpoint = admitted_source(config.clone()).checkpoint();

    let mut pipeline = config.clone();
    pipeline.pipeline = vec![serde_json::json!({ "$match": { "operationType": "update" } })];
    let mut images = config.clone();
    images.full_document_mode = FullDocumentMode::RequirePostImage;
    let mut snapshot = config.clone();
    snapshot.snapshot_mode = SnapshotMode::Initial;
    snapshot.pipeline.clear();
    let mut document = document_config();
    document.database = config.database.clone();
    document.collection = config.collection.clone();
    for changed in [pipeline, images, snapshot, document] {
        assert!(parse_mongodb_checkpoint(&checkpoint, &changed)
            .unwrap_err()
            .to_string()
            .contains("identity"));
    }

    let mut transport = config.clone();
    transport.connection_uri = "mongodb://two:27017".into();
    transport.max_buffered_bytes = 32 * 1024 * 1024;
    assert!(parse_mongodb_checkpoint(&checkpoint, &transport).is_ok());
}

#[test]
fn checkpoint_parser_rejects_legacy_and_noncanonical_identity() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let offsets = [
        (RESUME_TOKEN_OFFSET, r#"{"_data":"a"}"#),
        (SEQUENCE_OFFSET, "0"),
    ];

    let mut legacy = recovery_checkpoint(&config, &offsets);
    legacy.set_metadata("version", "4");
    assert!(parse_mongodb_checkpoint(&legacy, &config)
        .unwrap_err()
        .to_string()
        .contains("identity or format"));

    let mut uppercase = recovery_checkpoint(&config, &offsets);
    uppercase.set_metadata(
        COLLECTION_UUID_METADATA,
        TEST_COLLECTION_UUID.to_uppercase(),
    );
    assert!(parse_mongodb_checkpoint(&uppercase, &config)
        .unwrap_err()
        .to_string()
        .contains("canonical"));

    let complete = recovery_checkpoint(&config, &offsets);
    let mut missing = SourceCheckpoint::new();
    for (key, value) in complete.offsets() {
        missing.set_offset(key.clone(), value.clone());
    }
    for (key, value) in complete.metadata() {
        if key != DEPLOYMENT_IDENTITY_METADATA {
            missing.set_metadata(key.clone(), value.clone());
        }
    }
    assert!(parse_mongodb_checkpoint(&missing, &config)
        .unwrap_err()
        .to_string()
        .contains("deployment identity"));
}

#[test]
fn deployment_identity_parser_requires_a_canonical_typed_object_id() {
    assert_eq!(
        parse_deployment_identity(TEST_DEPLOYMENT_IDENTITY).unwrap(),
        test_deployment_identity()
    );
    for invalid in [
        "0123456789abcdef01234567",
        "standalone:0123456789abcdef01234567",
        "replica-set:0123456789ABCDEF01234567",
        "replica-set:not-an-object-id",
        "replica-set:0123456789abcdef01234567:extra",
    ] {
        assert!(parse_deployment_identity(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn identity_verification_rejects_deployment_and_collection_drift() {
    let expected = test_deployment_identity();
    assert!(verify_mongodb_deployment_identity(&expected, &expected).is_ok());
    let observed = MongoDeploymentIdentity::ReplicaSet("89abcdef0123456701234567".into());
    let error = verify_mongodb_deployment_identity(&expected, &observed).unwrap_err();
    assert!(error.to_string().contains("deployment identity changed"));
    assert!(!error.is_transient());

    let uuid = test_collection_uuid();
    let other = Uuid::parse_str("123e4567-e89b-12d3-a456-426614174001").unwrap();
    let error = verify_mongodb_collection_uuid(uuid, other, "db", "coll").unwrap_err();
    assert!(error.to_string().contains("collection identity changed"));

    let mut config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    config.full_document_mode = FullDocumentMode::RequirePostImage;
    let error = verify_mongodb_collection(
        &config,
        uuid,
        &MongoCollectionObservation {
            collection_uuid: uuid,
            post_images_enabled: false,
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("changeStreamPreAndPostImages"));
}

#[test]
fn identity_probes_fail_fast_for_permanent_server_rejections() {
    for (code, name) in [
        (13, "Unauthorized"),
        (59, "CommandNotFound"),
        (115, "CommandNotSupported"),
        (323, "APIStrictError"),
        (8000, "AtlasError"),
    ] {
        assert!(admission::mongodb_identity_command_is_permanent(code, name));
    }
    assert!(!admission::mongodb_identity_command_is_permanent(
        91,
        "ShutdownInProgress"
    ));
}

// ── Contracts and configuration ──

#[test]
fn contracts_follow_the_output_and_snapshot_modes() {
    let source = MongoDbCdcSource::new(MongoDbSourceConfig::default(), None);
    let history = source.contract(&valid_connector_config()).unwrap();
    assert_eq!(history.input_mode, SourceInputMode::AppendOnly);
    assert_eq!(history.consistency, SourceConsistency::Replayable);
    assert!(!history.is_exact_delivery_certified());

    let mut document = valid_connector_config();
    document.set("output.mode", "document");
    document.set("full.document.mode", "required");
    let contract = source.contract(&document).unwrap();
    assert_eq!(contract.input_mode, SourceInputMode::KeyedUpsert);
    assert_eq!(
        contract.row_positions,
        crate::connector::SourceRowPositionCapability::OrderedDeterministic
    );

    document.set("snapshot.mode", "initial");
    assert_eq!(
        source.contract(&document).unwrap().consistency,
        SourceConsistency::CommitCoupled
    );

    let mut delta_document = valid_connector_config();
    delta_document.set("output.mode", "document");
    assert!(source
        .contract(&delta_document)
        .unwrap_err()
        .to_string()
        .contains("full.document.mode=required"));

    let mut removed = valid_connector_config();
    removed.set("max.poll.records", "10");
    assert!(source
        .contract(&removed)
        .unwrap_err()
        .to_string()
        .contains("max.poll.records"));
}

#[test]
fn recovery_identity_ignores_endpoint_and_memory_but_fences_shape() {
    let base = valid_connector_config();
    let source = MongoDbCdcSource::new(MongoDbSourceConfig::from_config(&base).unwrap(), None);
    let mut tuned = base.clone();
    tuned.set("connection.uri", "mongodb://db-b.internal:27017");
    tuned.set("max.buffered.bytes", "134217728");
    let stored = source.recovery_identity_options(&base).unwrap();
    assert_eq!(stored, source.recovery_identity_options(&tuned).unwrap());
    assert_eq!(
        stored,
        source
            .recovery_identity_options(&ConnectorConfig::new("mongodb-cdc"))
            .unwrap()
    );
    for (key, value) in [
        ("collection", "other"),
        ("full.document.mode", "required"),
        ("snapshot.mode", "initial"),
    ] {
        let mut changed = base.clone();
        changed.set(key, value);
        assert_ne!(
            stored,
            source.recovery_identity_options(&changed).unwrap(),
            "{key}"
        );
    }
}

#[tokio::test]
async fn every_source_client_creation_uses_the_verified_tls_policy() {
    use mongodb::options::Tls;

    let defaults = source_client_options("mongodb://localhost:27017")
        .await
        .unwrap();
    assert!(matches!(defaults.tls, Some(Tls::Enabled(_))));
    let explicit_plaintext = source_client_options("mongodb://localhost:27017/?tls=false")
        .await
        .unwrap();
    assert_eq!(explicit_plaintext.tls, Some(Tls::Disabled));
    let error = source_client_options(
        "mongodb://localhost:27017/?tls=true&tlsAllowInvalidCertificates=true",
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("tlsInsecure"), "{error}");

    let capped = source_client_options(
        "mongodb://localhost:27017/?connectTimeoutMS=600000&serverSelectionTimeoutMS=600000",
    )
    .await
    .unwrap();
    assert_eq!(capped.connect_timeout, Some(READER_STARTUP_TIMEOUT));
    assert_eq!(
        capped.server_selection_timeout,
        Some(READER_STARTUP_TIMEOUT)
    );
}

#[test]
fn cursor_options_execute_supported_source_configuration() {
    let mut config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "events");
    config.full_document_mode = FullDocumentMode::RequirePostImage;
    config.max_buffered_bytes = 64 * 64 * 1024;

    let initial = change_stream_options(&config, None);
    assert!(matches!(
        initial.full_document,
        Some(mongodb::options::FullDocumentType::Required)
    ));
    assert_eq!(initial.batch_size, Some(64));
    assert_eq!(initial.show_expanded_events, Some(true));
    assert_eq!(
        bootstrap_change_stream_options(&config, None).batch_size,
        Some(0)
    );

    let token: mongodb::change_stream::event::ResumeToken =
        serde_json::from_str(r#"{"_data":"token"}"#).unwrap();
    let resumed = change_stream_options(
        &config,
        Some(&MongoResumePosition::ResumeAfter(token.clone())),
    );
    assert!(resumed.resume_after.is_some() && resumed.start_after.is_none());
    let restarted = change_stream_options(&config, Some(&MongoResumePosition::StartAfter(token)));
    assert!(restarted.start_after.is_some() && restarted.resume_after.is_none());
    let at = mongodb::bson::Timestamp {
        time: 5,
        increment: 1,
    };
    let snapshot = change_stream_options(&config, Some(&MongoResumePosition::StartAt(at)));
    assert_eq!(snapshot.start_at_operation_time, Some(at));
}

// ── Lifecycle, cancellation, and ownership ──

#[tokio::test]
async fn invalid_resume_checkpoint_fails_before_network_io() {
    let mut source = MongoDbCdcSource::new(
        MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll"),
        None,
    );
    let error = source
        .start(
            SourceStart::new(
                ConnectorConfig::new("mongodb-cdc"),
                SourcePosition::Resume {
                    attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(11),
                    checkpoint: SourceCheckpoint::new(),
                },
                crate::connector::DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .expect_err("an unbound empty checkpoint must be rejected");
    assert!(error.to_string().contains("checkpoint identity"));
    assert_eq!(source.state, ConnectorState::Created);
}

#[tokio::test]
async fn document_mode_start_requires_a_declared_keyed_projection() {
    let mut source = MongoDbCdcSource::new(MongoDbSourceConfig::default(), None);
    let mut config = valid_connector_config();
    config.set("output.mode", "document");
    config.set("full.document.mode", "required");
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
        .unwrap_err();
    assert!(error.to_string().contains("declared columns"), "{error}");
}

#[tokio::test]
async fn repeated_start_is_rejected_before_network_io() {
    let mut source = MongoDbCdcSource::new(
        MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll"),
        None,
    );
    source.state = ConnectorState::Running;
    let error = source
        .start(
            SourceStart::new(
                ConnectorConfig::new("mongodb-cdc"),
                SourcePosition::Initial,
                crate::connector::DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, ConnectorError::InvalidState { .. }));
}

#[tokio::test]
async fn close_interrupts_a_reader_blocked_on_a_full_queue() {
    let mut source = MongoDbCdcSource::new(
        MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll"),
        None,
    );
    let (tx, rx) = crossfire::mpsc::bounded_async::<BufferedMongoEvent>(1);
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let item = |shutdown_rx: &mut tokio::sync::watch::Receiver<bool>| {
        let payload = put("t", "insert", "a", "x");
        let bytes = buffered_retained_bytes(&payload).unwrap();
        let budget = Arc::clone(&source.byte_budget);
        let max = source.config.max_buffered_bytes;
        let mut shutdown_rx = shutdown_rx.clone();
        async move {
            let permit = acquire_mongo_byte_permit(bytes, &budget, max, &mut shutdown_rx)
                .await
                .unwrap()
                .unwrap();
            BufferedMongoEvent::new(payload, permit)
        }
    };
    let first = item(&mut shutdown_rx).await;
    tx.send(first).await.unwrap();
    let second = item(&mut shutdown_rx).await;
    let blocked_tx = tx.clone();
    let handle = tokio::spawn(async move {
        assert!(!send_event_or_shutdown(&blocked_tx, second, &mut shutdown_rx).await);
    });
    drop(tx);

    source.event_rx = Some(rx);
    source.reader_shutdown = Some(shutdown_tx);
    source.reader_handle = Some(handle);

    tokio::time::timeout(std::time::Duration::from_millis(250), source.close())
        .await
        .expect("close must not wait for queue capacity")
        .unwrap();
    assert!(source.event_rx.is_none());
    assert_eq!(
        source.byte_budget.available_permits(),
        source.config.max_buffered_bytes
    );
}

#[tokio::test]
async fn cancelling_close_preserves_the_tracked_reader_for_retry() {
    let mut source = MongoDbCdcSource::new(
        MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll"),
        None,
    );
    let terminal = source.terminal_task_tracker().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let release = Arc::new(Notify::new());
    let task_release = Arc::clone(&release);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let reader_guard = source.task_owner.track().unwrap();
    source.reader_shutdown = Some(shutdown_tx);
    source.reader_handle = Some(tokio::spawn(async move {
        let _reader_guard = reader_guard;
        let _ = started_tx.send(());
        task_release.notified().await;
    }));
    started_rx.await.expect("reader task did not start");

    let mut close = Box::pin(source.close());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut close)
            .await
            .is_err()
    );
    drop(close);
    assert!(source.reader_handle.is_some());
    assert!(*shutdown_rx.borrow());

    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(1), source.close())
        .await
        .expect("retry close must join the retained reader")
        .unwrap();
    drop(source);
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        terminal.wait_terminated(),
    )
    .await
    .expect("tracker must resolve after retry close joins the reader");
}

#[tokio::test(start_paused = true)]
async fn close_deadline_leaves_a_tracked_reaper_until_reader_exit() {
    let mut source = MongoDbCdcSource::new(
        MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll"),
        None,
    );
    let terminal = source.terminal_task_tracker().unwrap();
    let release = Arc::new(Notify::new());
    let task_release = Arc::clone(&release);
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let reader_guard = source.task_owner.track().unwrap();
    source.reader_handle = Some(tokio::spawn(async move {
        let _reader_guard = reader_guard;
        let _drop_signal = TaskDropSignal(Some(dropped_tx));
        task_release.notified().await;
    }));

    source.close().await.unwrap();
    drop(source);
    assert!(!terminal.is_terminated());
    release.notify_one();
    dropped_rx.await.unwrap();
    terminal.wait_terminated().await;
}

#[tokio::test]
async fn drop_signals_and_tracks_the_owned_reader() {
    let mut source = MongoDbCdcSource::new(
        MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll"),
        None,
    );
    let terminal = source.terminal_task_tracker().unwrap();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let reader_guard = source.task_owner.track().unwrap();
    source.reader_shutdown = Some(shutdown_tx);
    source.reader_handle = Some(tokio::spawn(async move {
        let _reader_guard = reader_guard;
        let _drop_signal = TaskDropSignal(Some(dropped_tx));
        let _ = shutdown_rx.changed().await;
    }));
    tokio::task::yield_now().await;

    drop(source);
    tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
        .await
        .expect("drop must stop the reader")
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        terminal.wait_terminated(),
    )
    .await
    .expect("MongoDB source tracker outlived its completed reader");
}

#[test]
fn tracker_covers_a_reader_destroyed_before_first_poll_on_another_runtime() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut source = MongoDbCdcSource::new(
        MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll"),
        None,
    );
    let terminal = source.terminal_task_tracker().unwrap();
    let reader_guard = source.task_owner.track().unwrap();
    let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let drop_signal = TaskDropSignal(Some(dropped_tx));
    source.reader_shutdown = Some(shutdown_tx);
    source.reader_handle = Some(runtime.spawn(async move {
        let _reader_guard = reader_guard;
        let _drop_signal = drop_signal;
        std::future::pending::<()>().await;
    }));

    drop(source);
    assert!(!terminal.is_terminated());
    drop(runtime);

    let observer = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    observer.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
            .await
            .expect("runtime destruction must drop the unpolled reader promptly")
            .expect("unpolled reader drop signal was lost");
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            terminal.wait_terminated(),
        )
        .await
        .expect("tracker must resolve across runtimes");
    });
}

#[tokio::test]
async fn byte_budget_wait_is_cancelled_by_shutdown() {
    let source = MongoDbCdcSource::new(
        MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll"),
        None,
    );
    let held = Arc::clone(&source.byte_budget)
        .acquire_many_owned(u32::try_from(source.config.max_buffered_bytes).unwrap())
        .await
        .unwrap();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let acquire = acquire_mongo_byte_permit(
        1024,
        &source.byte_budget,
        source.config.max_buffered_bytes,
        &mut shutdown_rx,
    );
    tokio::pin!(acquire);
    tokio::select! {
        _ = &mut acquire => panic!("byte-budget wait completed unexpectedly"),
        () = tokio::task::yield_now() => {}
    }
    shutdown_tx.send(true).unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut acquire)
            .await
            .expect("shutdown must cancel byte-budget wait")
            .unwrap()
            .is_none()
    );
    drop(held);
}

#[tokio::test]
async fn oversize_items_fail_instead_of_waiting_forever() {
    let source = MongoDbCdcSource::new(
        MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll"),
        None,
    );
    let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let error = acquire_mongo_byte_permit(
        source.config.max_buffered_bytes + 1,
        &source.byte_budget,
        source.config.max_buffered_bytes,
        &mut shutdown_rx,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("max.buffered.bytes"), "{error}");
    assert!(!error.is_transient());
}

#[tokio::test]
async fn terminal_reader_error_preserves_classification_outside_the_event_queue() {
    let mut source = MongoDbCdcSource::new(
        MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll"),
        None,
    );
    let (error_tx, error_rx) = tokio::sync::watch::channel(None);
    source.reader_error = Some(error_rx);
    error_tx.send_replace(Some(MongoReaderFailure::Configuration(
        "reader failed".to_string(),
    )));
    let error = source.poll_batch(1).await.unwrap_err();
    assert!(error.to_string().contains("reader failed"));
    assert!(!error.is_transient());
}

#[tokio::test(start_paused = true)]
async fn reader_admission_timeout_signals_and_joins_the_candidate() {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
    let mut handle = tokio::spawn(async move {
        let _ready_tx = ready_tx;
        shutdown_rx.changed().await.unwrap();
        let _ = stopped_tx.send(());
    });
    let error = await_mongo_reader_ready(ready_rx, &shutdown_tx, &mut handle)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("startup deadline"), "{error}");
    stopped_rx.await.unwrap();
    assert!(handle.is_finished());
}

#[tokio::test]
async fn cancelling_admission_signals_its_candidate() {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let _drop_signal = TaskDropSignal(Some(dropped_tx));
        let _ = shutdown_rx.changed().await;
    });
    tokio::task::yield_now().await;
    drop(MongoReaderAdmissionGuard::new(shutdown_tx.clone()));
    dropped_rx.await.unwrap();
    assert!(*shutdown_tx.borrow());
    handle.await.unwrap();
}

#[tokio::test]
async fn failed_reader_admission_preserves_state_and_allows_same_instance_retry() {
    let mut original = MongoDbSourceConfig::new("mongodb://localhost:27017", "original", "events");
    original.max_buffered_bytes = 32 * 1024 * 1024;
    let mut source = admitted_source(original);
    let original_config = serde_json::to_value(&source.config).unwrap();
    let original_checkpoint = source.checkpoint();
    let original_budget = Arc::clone(&source.byte_budget);

    let mut candidate = valid_connector_config();
    candidate.set("connection.uri", "http://localhost:27017");
    candidate.set("collection", "changes");
    let candidate_config = MongoDbSourceConfig::from_config(&candidate).unwrap();
    let checkpoint = recovery_checkpoint(
        &candidate_config,
        &[
            (RESUME_TOKEN_OFFSET, r#"{"_data":"candidate"}"#),
            (SEQUENCE_OFFSET, "0"),
        ],
    );
    for position in [
        SourcePosition::Resume {
            attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(11),
            checkpoint,
        },
        SourcePosition::Initial,
    ] {
        let error = source
            .start(
                SourceStart::new(
                    candidate.clone(),
                    position,
                    crate::connector::DeliveryGuarantee::BestEffort,
                )
                .unwrap(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("parse URI"), "{error}");
        assert!(!error.is_transient());
        assert_eq!(source.state, ConnectorState::Created);
        assert_eq!(
            serde_json::to_value(&source.config).unwrap(),
            original_config
        );
        assert_eq!(source.checkpoint(), original_checkpoint);
        assert!(Arc::ptr_eq(&source.byte_budget, &original_budget));
        assert!(source.reader_handle.is_none());
    }
}

#[test]
fn canonical_extended_json_preserves_document_field_order() {
    let raw = RawDocumentBuf::from_document(&doc! { "z": 1, "a": 2, "m": 3 }).unwrap();
    assert_eq!(
        crate::mongodb::change_event::canonical_document_extjson(&raw).unwrap(),
        r#"{"z":{"$numberInt":"1"},"a":{"$numberInt":"2"},"m":{"$numberInt":"3"}}"#,
        "document keys compare by field order; canonical text must not sort them"
    );
}
