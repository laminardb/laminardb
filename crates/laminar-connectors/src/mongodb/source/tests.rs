use super::super::change_event::Namespace;
use super::*;
use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{SourceConnector, SourcePosition, SourceStart};

const TEST_COLLECTION_UUID: &str = "123e4567-e89b-12d3-a456-426614174000";
const TEST_DEPLOYMENT_OBJECT_ID: &str = "0123456789abcdef01234567";
const TEST_DEPLOYMENT_IDENTITY: &str = "replica-set:0123456789abcdef01234567";

#[cfg(feature = "mongodb-cdc")]
struct TaskDropSignal(Option<tokio::sync::oneshot::Sender<()>>);

#[cfg(feature = "mongodb-cdc")]
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

fn admitted_source(config: MongoDbSourceConfig) -> MongoDbCdcSource {
    let mut source = MongoDbCdcSource::new(config, None);
    source.collection_uuid = Some(test_collection_uuid());
    source.deployment_identity = Some(test_deployment_identity());
    source.checkpoint_resume_token = Some(r#"{"_data":"anchor"}"#.into());
    source
}

fn parsed_checkpoint(position: MongoCheckpointPosition) -> ParsedMongoCheckpoint {
    ParsedMongoCheckpoint {
        position,
        collection_uuid: test_collection_uuid(),
        deployment_identity: test_deployment_identity(),
    }
}

fn valid_connector_config() -> ConnectorConfig {
    let mut config = ConnectorConfig::new("mongodb-cdc");
    config.set("connection.uri", "mongodb://localhost:27017");
    config.set("database", "testdb");
    config.set("collection", "users");
    config
}

fn recovery_identity_config() -> ConnectorConfig {
    let mut config = ConnectorConfig::new("mongodb-cdc");
    config.set("connection.uri", "mongodb://db-a.internal:27017");
    config.set("database", "orders");
    config.set("collection", "events");
    config
}

fn sample_event(op: OperationType) -> MongoDbChangeEvent {
    MongoDbChangeEvent {
        operation_type: op,
        namespace: Namespace {
            db: "testdb".to_string(),
            coll: "users".to_string(),
        },
        document_key: r#"{"_id": "1"}"#.to_string(),
        full_document: Some(r#"{"_id": "1", "name": "Alice"}"#.to_string()),
        update_description: None,
        cluster_time_secs: 1_700_000_000,
        cluster_time_inc: 1,
        resume_token: r#"{"_data":"token1"}"#.to_string(),
        wall_time_ms: 1_700_000_000_000,
    }
}

fn recovery_checkpoint(
    config: &MongoDbSourceConfig,
    key: &str,
    value: impl Into<String>,
) -> SourceCheckpoint {
    let mut checkpoint = SourceCheckpoint::new();
    checkpoint.set_offset(key, value);
    checkpoint.set_metadata("connector", MONGODB_CHECKPOINT_CONNECTOR);
    checkpoint.set_metadata("version", MONGODB_CHECKPOINT_VERSION);
    checkpoint.set_metadata("database", &config.database);
    checkpoint.set_metadata("collection", &config.collection);
    checkpoint.set_metadata(COLLECTION_UUID_METADATA, TEST_COLLECTION_UUID);
    checkpoint.set_metadata(DEPLOYMENT_IDENTITY_METADATA, TEST_DEPLOYMENT_IDENTITY);
    checkpoint.set_metadata(STREAM_IDENTITY_METADATA, mongodb_stream_identity(config));
    checkpoint
}

#[test]
fn test_schema() {
    let schema = mongodb_cdc_envelope_schema();
    assert_eq!(schema.fields().len(), 9);
    assert_eq!(schema.field(0).name(), "_namespace");
    assert_eq!(schema.field(6).name(), "_full_document");
    assert!(schema.field(6).is_nullable());
}

#[test]
fn test_new_source() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let source = MongoDbCdcSource::new(config, None);
    assert_eq!(source.buffered_events(), 0);
    assert_eq!(
        source.cancellation_policy(),
        crate::connector::ConnectorCancellationPolicy::RetireConnector
    );
}

#[test]
fn recovery_identity_ignores_endpoint_and_memory_tuning() {
    let left = recovery_identity_config();
    let source = MongoDbCdcSource::new(MongoDbSourceConfig::from_config(&left).unwrap(), None);
    let mut right = recovery_identity_config();
    right.set("connection.uri", "mongodb://db-b.internal:27017");
    right.set("max.buffered.bytes", "134217728");

    let stored = source.recovery_identity_options(&left).unwrap();
    assert_eq!(
        stored,
        source.recovery_identity_options(&right).unwrap(),
        "endpoint and memory tuning must not fence durable recovery"
    );
    assert_eq!(
        stored,
        source
            .recovery_identity_options(&ConnectorConfig::new("mongodb-cdc"))
            .unwrap(),
        "an empty runtime config must use the validated provider config"
    );
}

#[test]
fn recovery_identity_fences_collection_and_delivery_shape() {
    let left = recovery_identity_config();
    let source = MongoDbCdcSource::new(MongoDbSourceConfig::from_config(&left).unwrap(), None);
    let mut collection = recovery_identity_config();
    collection.set("collection", "other_events");
    assert_ne!(
        source.recovery_identity_options(&left).unwrap(),
        source.recovery_identity_options(&collection).unwrap(),
        "a different collection must fence recovery"
    );

    let mut full_document = recovery_identity_config();
    full_document.set("full.document.mode", "required");
    assert_ne!(
        source.recovery_identity_options(&left).unwrap(),
        source.recovery_identity_options(&full_document).unwrap(),
        "a different delivery shape must fence recovery"
    );
}

#[cfg(feature = "mongodb-cdc")]
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
}

#[cfg(feature = "mongodb-cdc")]
#[tokio::test]
async fn source_client_timeouts_cannot_exceed_the_startup_deadline() {
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

    let smaller = source_client_options(
        "mongodb://localhost:27017/?connectTimeoutMS=250&serverSelectionTimeoutMS=500",
    )
    .await
    .unwrap();
    assert_eq!(
        smaller.connect_timeout,
        Some(std::time::Duration::from_millis(250))
    );
    assert_eq!(
        smaller.server_selection_timeout,
        Some(std::time::Duration::from_millis(500))
    );
}

#[test]
fn test_enqueue_event() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let mut source = MongoDbCdcSource::new(config, None);

    source
        .enqueue_event(sample_event(OperationType::Insert))
        .unwrap();
    assert_eq!(source.buffered_events(), 1);
}

#[test]
fn test_enqueue_invalidate() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let mut source = MongoDbCdcSource::new(config, None);

    let mut event = sample_event(OperationType::Invalidate);
    event.full_document = None;
    source.enqueue_event(event).unwrap();
    assert_eq!(
        source.event_buffer[0].event().unwrap().operation_type,
        OperationType::Invalidate
    );
}

#[test]
fn drain_releases_buffered_byte_ownership() {
    let event = sample_event(OperationType::Insert);
    let retained_bytes = mongo_event_retained_bytes(&event).unwrap();
    let mut config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    config.max_buffered_bytes = retained_bytes;
    let mut source = MongoDbCdcSource::new(config, None);

    source.enqueue_event(event).unwrap();
    assert_eq!(source.byte_budget.available_permits(), 0);

    source.drain_to_batch(1).unwrap().unwrap();
    assert_eq!(
        source.byte_budget.available_permits(),
        source.config.max_buffered_bytes
    );
}

#[test]
fn enqueue_rejects_one_oversize_event_without_mutating_the_buffer() {
    let event = sample_event(OperationType::Insert);
    let retained_bytes = mongo_event_retained_bytes(&event).unwrap();
    let mut config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    config.max_buffered_bytes = retained_bytes - 1;
    let mut source = MongoDbCdcSource::new(config, None);

    let error = source.enqueue_event(event).unwrap_err();

    assert!(error.to_string().contains("hard byte bound"));
    assert!(source.event_buffer.is_empty());
    assert_eq!(
        source.byte_budget.available_permits(),
        source.config.max_buffered_bytes
    );
}

#[test]
fn enqueue_enforces_aggregate_byte_bound() {
    let event = sample_event(OperationType::Insert);
    let retained_bytes = mongo_event_retained_bytes(&event).unwrap();
    let mut config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    config.max_buffered_bytes = retained_bytes * 2 - 1;
    let mut source = MongoDbCdcSource::new(config, None);
    source.enqueue_event(event.clone()).unwrap();

    let available_before = source.byte_budget.available_permits();
    let error = source.enqueue_event(event).unwrap_err();

    assert!(error.to_string().contains("buffered bytes"));
    assert_eq!(source.event_buffer.len(), 1);
    assert_eq!(source.byte_budget.available_permits(), available_before);
}

#[test]
fn test_events_to_record_batch() {
    let schema = mongodb_cdc_envelope_schema();
    let events = vec![
        sample_event(OperationType::Insert),
        sample_event(OperationType::Delete),
    ];

    let batch = events_to_record_batch(&events, &schema).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 9);
}

#[test]
fn test_events_to_record_batch_empty() {
    let schema = mongodb_cdc_envelope_schema();
    let batch = events_to_record_batch(&[], &schema).unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 9);
}

#[test]
fn test_drain_to_batch() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let mut source = MongoDbCdcSource::new(config, None);

    // Empty buffer returns None.
    assert!(source.drain_to_batch(10).unwrap().is_none());

    // Add events and drain.
    for _ in 0..5 {
        source
            .enqueue_event(sample_event(OperationType::Insert))
            .unwrap();
    }
    assert!(source.drain_to_batch(0).unwrap().is_none());
    assert_eq!(source.buffered_events(), 5);
    let batch = source.drain_to_batch(3).unwrap().unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(source.buffered_events(), 2);

    // Drain remaining.
    let batch = source.drain_to_batch(10).unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(source.buffered_events(), 0);
}

#[test]
fn failed_batch_construction_preserves_queue_order() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let mut source = admitted_source(config);
    let mut prior = sample_event(OperationType::Insert);
    prior.resume_token = r#"{"_data":"prior"}"#.to_string();
    source.enqueue_event(prior).unwrap();
    source.drain_to_batch(1).unwrap().unwrap();
    let mut first = sample_event(OperationType::Insert);
    first.resume_token = r#"{"_data":"first"}"#.to_string();
    let mut second = sample_event(OperationType::Update);
    second.resume_token = r#"{"_data":"second"}"#.to_string();
    source.enqueue_event(first).unwrap();
    source.enqueue_event(second).unwrap();
    let available_before = source.byte_budget.available_permits();
    source.schema = Arc::new(arrow_schema::Schema::empty());

    source.drain_to_batch(2).unwrap_err();

    assert_eq!(
        source.checkpoint().get_offset(RESUME_TOKEN_OFFSET),
        Some(r#"{"_data":"prior"}"#),
        "a failed Arrow batch must not advance the durable emitted cursor"
    );
    assert_eq!(source.buffered_events(), 2);
    assert_eq!(
        source.event_buffer[0].event().unwrap().resume_token,
        r#"{"_data":"first"}"#
    );
    assert_eq!(
        source.event_buffer[1].event().unwrap().resume_token,
        r#"{"_data":"second"}"#
    );
    assert_eq!(source.byte_budget.available_permits(), available_before);

    source.schema = mongodb_cdc_envelope_schema();
    source.drain_to_batch(2).unwrap().unwrap();
    assert_eq!(
        source.byte_budget.available_permits(),
        source.config.max_buffered_bytes
    );
}

#[test]
fn checkpoint_tracks_only_the_last_successfully_emitted_token() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "testdb", "users");
    let mut source = admitted_source(config);
    let mut first = sample_event(OperationType::Insert);
    first.resume_token = r#"{"_data":"first"}"#.into();
    let mut second = sample_event(OperationType::Insert);
    second.resume_token = r#"{"_data":"second"}"#.into();
    source.enqueue_event(first).unwrap();
    source.enqueue_event(second).unwrap();

    source.drain_to_batch(1).unwrap().unwrap();
    let cp = source.checkpoint();
    assert_eq!(
        cp.get_offset(RESUME_TOKEN_OFFSET),
        Some(r#"{"_data":"first"}"#)
    );
    assert_eq!(cp.offsets().len(), 1);

    source.drain_to_batch(1).unwrap().unwrap();
    let cp = source.checkpoint();
    assert_eq!(
        cp.get_offset(RESUME_TOKEN_OFFSET),
        Some(r#"{"_data":"second"}"#)
    );
    assert_eq!(cp.get_metadata("connector"), Some("mongodb-cdc"));
}

#[test]
fn ordered_post_batch_high_watermark_advances_without_skipping_later_events() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "testdb", "users");
    let mut source = admitted_source(config);
    let mut first = sample_event(OperationType::Insert);
    first.resume_token = r#"{"_data":"event"}"#.into();
    let mut later = sample_event(OperationType::Update);
    later.resume_token = r#"{"_data":"later"}"#.into();
    source.enqueue_event(first).unwrap();
    source
        .enqueue_high_watermark(r#"{"_data":"post_batch"}"#)
        .unwrap();
    source.enqueue_event(later).unwrap();

    let first_batch = source.drain_to_batch(2).unwrap().unwrap();
    assert_eq!(
        first_batch.num_rows(),
        1,
        "high watermarks are not data rows"
    );
    assert_eq!(
        source.checkpoint().get_offset(RESUME_TOKEN_OFFSET),
        Some(r#"{"_data":"post_batch"}"#)
    );
    assert_eq!(source.buffered_events(), 1);

    let later_batch = source.drain_to_batch(1).unwrap().unwrap();
    assert_eq!(later_batch.num_rows(), 1);
    assert_eq!(
        source.checkpoint().get_offset(RESUME_TOKEN_OFFSET),
        Some(r#"{"_data":"later"}"#)
    );

    source
        .enqueue_high_watermark(r#"{"_data":"idle"}"#)
        .unwrap();
    assert!(source.drain_to_batch(1).unwrap().is_none());
    assert_eq!(
        source.checkpoint().get_offset(RESUME_TOKEN_OFFSET),
        Some(r#"{"_data":"idle"}"#)
    );
}

#[test]
fn checkpoint_parser_restores_exact_resume_token() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "testdb", "users");
    let mut source = admitted_source(config.clone());

    assert_eq!(
        parse_mongodb_checkpoint(&source.checkpoint(), &config).unwrap(),
        parsed_checkpoint(MongoCheckpointPosition::ResumeAfter(
            r#"{"_data":"anchor"}"#.into()
        ))
    );

    source.checkpoint_resume_token = Some(r#"{"_data":"resume"}"#.into());
    assert_eq!(
        parse_mongodb_checkpoint(&source.checkpoint(), &config).unwrap(),
        parsed_checkpoint(MongoCheckpointPosition::ResumeAfter(
            r#"{"_data":"resume"}"#.into()
        ))
    );
}

#[test]
fn checkpoint_requires_admitted_deployment_collection_and_token() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let mut source = MongoDbCdcSource::new(config, None);

    assert!(source.checkpoint().is_empty());

    source.collection_uuid = Some(test_collection_uuid());
    assert!(source.checkpoint().is_empty());
    source.deployment_identity = Some(test_deployment_identity());
    assert!(source.checkpoint().is_empty());
    source.checkpoint_resume_token = Some(r#"{"_data":"anchor"}"#.into());
    let checkpoint = source.checkpoint();
    assert_eq!(checkpoint.get_metadata("version"), Some("4"));
    assert_eq!(
        checkpoint.get_metadata(COLLECTION_UUID_METADATA),
        Some(TEST_COLLECTION_UUID)
    );
    assert_eq!(
        checkpoint.get_metadata(DEPLOYMENT_IDENTITY_METADATA),
        Some(TEST_DEPLOYMENT_IDENTITY)
    );
}

#[test]
fn checkpoint_binds_the_exact_change_stream_pipeline_and_options() {
    let mut config = MongoDbSourceConfig::new("mongodb://one:27017", "db", "coll");
    config.pipeline = vec![serde_json::json!({
        "$match": { "operationType": "insert", "ns.db": "db" }
    })];
    let checkpoint = admitted_source(config.clone()).checkpoint();

    let mut different_pipeline = config.clone();
    different_pipeline.pipeline =
        vec![serde_json::json!({ "$match": { "operationType": "update" } })];
    assert!(parse_mongodb_checkpoint(&checkpoint, &different_pipeline)
        .unwrap_err()
        .to_string()
        .contains("identity"));

    let mut different_document_mode = config.clone();
    different_document_mode.full_document_mode =
        super::super::config::FullDocumentMode::RequirePostImage;
    assert!(
        parse_mongodb_checkpoint(&checkpoint, &different_document_mode)
            .unwrap_err()
            .to_string()
            .contains("identity")
    );

    let mut reordered_pipeline = config.clone();
    reordered_pipeline.pipeline =
        vec![
            serde_json::from_str(r#"{"$match":{"ns.db":"db","operationType":"insert"}}"#).unwrap(),
        ];
    assert!(parse_mongodb_checkpoint(&checkpoint, &reordered_pipeline).is_ok());

    let mut transport_and_buffer_change = config.clone();
    transport_and_buffer_change.connection_uri = "mongodb://two:27017".into();
    transport_and_buffer_change.max_buffered_bytes = 32 * 1024 * 1024;
    assert_eq!(
        parse_mongodb_checkpoint(&checkpoint, &transport_and_buffer_change).unwrap(),
        parsed_checkpoint(MongoCheckpointPosition::ResumeAfter(
            r#"{"_data":"anchor"}"#.into()
        )),
        "failover endpoints and local buffering do not change stream semantics"
    );
}

#[test]
fn checkpoint_parser_rejects_ambiguous_noncanonical_and_oversize_positions() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let source = admitted_source(config.clone());

    let mut ambiguous = source.checkpoint();
    ambiguous.set_offset(START_AFTER_TOKEN_OFFSET, r#"{"_data":"token"}"#);
    assert!(parse_mongodb_checkpoint(&ambiguous, &config)
        .unwrap_err()
        .to_string()
        .contains("exactly one"));

    let noncanonical = recovery_checkpoint(&config, RESUME_TOKEN_OFFSET, r#"{"_data": "token"}"#);
    assert!(parse_mongodb_checkpoint(&noncanonical, &config)
        .unwrap_err()
        .to_string()
        .contains("canonical"));

    let oversized = recovery_checkpoint(
        &config,
        RESUME_TOKEN_OFFSET,
        "x".repeat(MAX_RESUME_TOKEN_BYTES + 1),
    );
    assert!(parse_mongodb_checkpoint(&oversized, &config)
        .unwrap_err()
        .to_string()
        .contains("size"));

    let invalid_json = recovery_checkpoint(&config, RESUME_TOKEN_OFFSET, "not-json");
    assert!(parse_mongodb_checkpoint(&invalid_json, &config)
        .unwrap_err()
        .to_string()
        .contains("valid JSON"));

    let bad_anchor = recovery_checkpoint(&config, "unknown_position", "10:2");
    assert!(parse_mongodb_checkpoint(&bad_anchor, &config)
        .unwrap_err()
        .to_string()
        .contains("unknown position"));
}

#[test]
fn checkpoint_parser_rejects_legacy_missing_and_noncanonical_collection_uuid() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");

    let mut legacy = recovery_checkpoint(&config, RESUME_TOKEN_OFFSET, r#"{"_data":"anchor"}"#);
    legacy.set_metadata("version", "3");
    assert!(parse_mongodb_checkpoint(&legacy, &config)
        .unwrap_err()
        .to_string()
        .contains("identity or format"));

    let mut missing = SourceCheckpoint::new();
    missing.set_offset(RESUME_TOKEN_OFFSET, r#"{"_data":"anchor"}"#);
    missing.set_metadata("connector", MONGODB_CHECKPOINT_CONNECTOR);
    missing.set_metadata("version", MONGODB_CHECKPOINT_VERSION);
    missing.set_metadata("database", &config.database);
    missing.set_metadata("collection", &config.collection);
    missing.set_metadata(COLLECTION_UUID_METADATA, TEST_COLLECTION_UUID);
    missing.set_metadata(STREAM_IDENTITY_METADATA, mongodb_stream_identity(&config));
    assert!(parse_mongodb_checkpoint(&missing, &config)
        .unwrap_err()
        .to_string()
        .contains("missing its deployment identity"));

    let mut uppercase = recovery_checkpoint(&config, RESUME_TOKEN_OFFSET, r#"{"_data":"anchor"}"#);
    uppercase.set_metadata(
        COLLECTION_UUID_METADATA,
        TEST_COLLECTION_UUID.to_uppercase(),
    );
    assert!(parse_mongodb_checkpoint(&uppercase, &config)
        .unwrap_err()
        .to_string()
        .contains("canonical"));

    let mut malformed = recovery_checkpoint(&config, RESUME_TOKEN_OFFSET, r#"{"_data":"anchor"}"#);
    malformed.set_metadata(COLLECTION_UUID_METADATA, "not-a-uuid");
    assert!(parse_mongodb_checkpoint(&malformed, &config)
        .unwrap_err()
        .to_string()
        .contains("invalid MongoDB CDC collection UUID"));
}

#[test]
fn deployment_identity_parser_requires_a_canonical_typed_object_id() {
    assert_eq!(
        parse_deployment_identity(TEST_DEPLOYMENT_IDENTITY).unwrap(),
        test_deployment_identity()
    );
    assert_eq!(
        parse_deployment_identity("sharded-cluster:89abcdef0123456701234567").unwrap(),
        MongoDeploymentIdentity::ShardedCluster("89abcdef0123456701234567".into())
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

#[cfg(feature = "mongodb-cdc")]
#[test]
fn deployment_identity_verification_rejects_checkpoint_drift() {
    let expected = test_deployment_identity();
    assert!(verify_mongodb_deployment_identity(&expected, &expected).is_ok());

    let observed = MongoDeploymentIdentity::ReplicaSet("89abcdef0123456701234567".into());
    let error = verify_mongodb_deployment_identity(&expected, &observed).unwrap_err();
    assert!(error.to_string().contains("deployment identity changed"));
    assert!(!error.is_transient());
}

#[cfg(feature = "mongodb-cdc")]
#[test]
fn immutable_identity_probes_fail_fast_for_permanent_server_rejections() {
    for (code, name) in [
        (13, "Unauthorized"),
        (59, "CommandNotFound"),
        (115, "CommandNotSupported"),
        (323, "APIStrictError"),
        (8000, "AtlasError"),
    ] {
        assert!(mongodb_identity_command_is_permanent(code, name));
    }
    assert!(!mongodb_identity_command_is_permanent(
        91,
        "ShutdownInProgress"
    ));
}

#[test]
fn invalidation_is_emitted_and_checkpointed_as_start_after() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let mut source = admitted_source(config);
    source
        .enqueue_event(sample_event(OperationType::Invalidate))
        .unwrap();

    let batch = source.drain_to_batch(1).unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let checkpoint = source.checkpoint();
    assert_eq!(
        checkpoint.get_offset(START_AFTER_TOKEN_OFFSET),
        Some(r#"{"_data":"token1"}"#)
    );
    assert!(checkpoint.get_offset(RESUME_TOKEN_OFFSET).is_none());
    assert_eq!(
        parse_mongodb_checkpoint(&checkpoint, &source.config).unwrap(),
        parsed_checkpoint(MongoCheckpointPosition::StartAfter(
            r#"{"_data":"token1"}"#.into()
        ))
    );
}

#[cfg(feature = "mongodb-cdc")]
#[tokio::test]
async fn invalid_resume_checkpoint_fails_before_network_io() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let mut source = MongoDbCdcSource::new(config, None);
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

#[cfg(feature = "mongodb-cdc")]
#[tokio::test]
async fn repeated_start_is_rejected_before_network_io() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let mut source = MongoDbCdcSource::new(config, None);
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

#[cfg(feature = "mongodb-cdc")]
#[test]
fn contract_fails_closed_for_raw_json_envelope() {
    let source = MongoDbCdcSource::new(
        MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll"),
        None,
    );
    let error = source
        .contract(&ConnectorConfig::new("mongodb-cdc"))
        .unwrap_err();
    assert!(error.to_string().contains("raw JSON change envelope"));

    let mut config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    config.full_document_mode = super::super::config::FullDocumentMode::RequirePostImage;
    let source = MongoDbCdcSource::new(config, None);
    let error = source
        .contract(&ConnectorConfig::new("mongodb-cdc"))
        .unwrap_err();
    assert!(error.to_string().contains("raw JSON change envelope"));
}

#[cfg(feature = "mongodb-cdc")]
#[test]
fn contract_validates_request_configuration() {
    let source = MongoDbCdcSource::new(MongoDbSourceConfig::default(), None);
    let mut config = valid_connector_config();
    config.set("full.document.mode", "required");
    let error = source.contract(&config).unwrap_err();
    assert!(error.to_string().contains("raw JSON change envelope"));

    let mut removed = config;
    removed.set("max.poll.records", "10");
    let error = source.contract(&removed).unwrap_err();
    assert!(error.to_string().contains("max.poll.records"));
}

#[cfg(feature = "mongodb-cdc")]
#[test]
fn cursor_options_execute_supported_source_configuration() {
    let mut config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "events");
    config.full_document_mode = super::super::config::FullDocumentMode::RequirePostImage;
    config.max_buffered_bytes = 64 * 64 * 1024;

    let initial = change_stream_options(&config, None);
    assert!(matches!(
        initial.full_document,
        Some(mongodb::options::FullDocumentType::Required)
    ));
    assert_eq!(
        initial.max_await_time,
        Some(std::time::Duration::from_secs(1))
    );
    assert_eq!(initial.batch_size, Some(64));
    assert_eq!(initial.show_expanded_events, Some(true));

    let bootstrap = bootstrap_change_stream_options(&config);
    assert_eq!(bootstrap.batch_size, Some(0));
    assert!(matches!(
        bootstrap.full_document,
        Some(mongodb::options::FullDocumentType::Required)
    ));
    assert_eq!(bootstrap.show_expanded_events, initial.show_expanded_events);

    let token: mongodb::change_stream::event::ResumeToken =
        serde_json::from_str(r#"{"_data":"token"}"#).unwrap();
    let resumed = change_stream_options(
        &config,
        Some(&MongoResumePosition::ResumeAfter(token.clone())),
    );
    assert!(resumed.resume_after.is_some());
    assert!(resumed.start_after.is_none());

    let restarted = change_stream_options(&config, Some(&MongoResumePosition::StartAfter(token)));
    assert!(restarted.resume_after.is_none());
    assert!(restarted.start_after.is_some());
}

#[cfg(feature = "mongodb-cdc")]
#[test]
fn collection_uuid_verification_rejects_namespace_reuse() {
    let expected = test_collection_uuid();
    assert!(verify_mongodb_collection_uuid(expected, expected, "db", "coll").is_ok());

    let observed = Uuid::parse_str("123e4567-e89b-12d3-a456-426614174001").unwrap();
    let error = verify_mongodb_collection_uuid(expected, observed, "db", "coll").unwrap_err();
    assert!(error.to_string().contains("collection identity changed"));
    assert!(!error.is_transient());
}

#[cfg(feature = "mongodb-cdc")]
#[test]
fn required_post_images_are_an_admission_requirement() {
    let expected = test_collection_uuid();
    let mut config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    config.full_document_mode = super::super::config::FullDocumentMode::RequirePostImage;

    let error = verify_mongodb_collection(
        &config,
        expected,
        &MongoCollectionObservation {
            collection_uuid: expected,
            post_images_enabled: false,
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("changeStreamPreAndPostImages"));

    verify_mongodb_collection(
        &config,
        expected,
        &MongoCollectionObservation {
            collection_uuid: expected,
            post_images_enabled: true,
        },
    )
    .unwrap();
}

#[cfg(feature = "mongodb-cdc")]
#[test]
fn disambiguated_update_paths_survive_the_source_envelope() {
    use mongodb::bson::{doc, Timestamp};

    let driver_event = mongodb::bson::from_document::<
        mongodb::change_stream::event::ChangeStreamEvent<mongodb::bson::Document>,
    >(doc! {
        "_id": { "_data": "token" },
        "operationType": "update",
        "ns": { "db": "testdb", "coll": "users" },
        "documentKey": { "_id": 1 },
        "updateDescription": {
            "updatedFields": { "a.b": 7 },
            "removedFields": [],
            "truncatedArrays": [],
            "disambiguatedPaths": { "a.b": ["a.b"] },
        },
        "clusterTime": Timestamp { time: 10, increment: 2 },
    })
    .unwrap();

    let event = parse_change_stream_event(&driver_event).unwrap();
    let paths = &event
        .update_description
        .as_ref()
        .unwrap()
        .disambiguated_paths;
    assert_eq!(paths["a.b"], serde_json::json!(["a.b"]));

    let batch = events_to_record_batch(&[event], &mongodb_cdc_envelope_schema()).unwrap();
    let descriptions = batch
        .column(batch.schema().index_of("_update_desc").unwrap())
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let description: serde_json::Value = serde_json::from_str(descriptions.value(0)).unwrap();
    assert_eq!(
        description["disambiguated_paths"]["a.b"],
        serde_json::json!(["a.b"])
    );
}

#[cfg(feature = "mongodb-cdc")]
#[test]
fn expanded_operation_preserves_exact_driver_value() {
    use mongodb::bson::{doc, Timestamp};

    let wire_event = mongodb::bson::to_vec(&doc! {
        "_id": { "_data": "token" },
        "operationType": "createIndexes",
        "ns": { "db": "testdb", "coll": "users" },
        "clusterTime": Timestamp { time: 10, increment: 2 },
    })
    .unwrap();
    let driver_event = mongodb::bson::from_slice::<
        mongodb::change_stream::event::ChangeStreamEvent<mongodb::bson::Document>,
    >(&wire_event)
    .unwrap();

    let event = parse_change_stream_event(&driver_event).unwrap();
    assert_eq!(
        event.operation_type,
        OperationType::Other("createIndexes".into())
    );
}

#[cfg(feature = "mongodb-cdc")]
#[tokio::test]
async fn close_interrupts_a_reader_blocked_on_a_full_queue() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let mut source = MongoDbCdcSource::new(config, None);
    let (tx, rx) = crossfire::mpsc::bounded_async::<BufferedMongoEvent>(1);
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let first = acquire_mongo_event_ownership(
        sample_event(OperationType::Insert),
        &source.byte_budget,
        source.config.max_buffered_bytes,
        &mut shutdown_rx,
    )
    .await
    .unwrap()
    .unwrap();
    tx.send(first).await.unwrap();
    let second = acquire_mongo_event_ownership(
        sample_event(OperationType::Insert),
        &source.byte_budget,
        source.config.max_buffered_bytes,
        &mut shutdown_rx,
    )
    .await
    .unwrap()
    .unwrap();
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
    assert!(source.event_buffer.is_empty());
    assert_eq!(
        source.byte_budget.available_permits(),
        source.config.max_buffered_bytes
    );
}

#[cfg(feature = "mongodb-cdc")]
#[tokio::test]
async fn cancelling_close_preserves_the_tracked_reader_for_retry() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let mut source = MongoDbCdcSource::new(config, None);
    let terminal = source.terminal_task_tracker().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let release = Arc::new(Notify::new());
    let task_release = Arc::clone(&release);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let reader_guard = source
        .task_owner
        .track()
        .expect("live test source must admit its reader");
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
            .is_err(),
        "reader should keep the first close pending"
    );
    drop(close);
    assert!(source.reader_handle.is_some());
    assert!(source.reader_shutdown.is_some());
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

#[cfg(feature = "mongodb-cdc")]
#[tokio::test(start_paused = true)]
async fn close_deadline_leaves_a_tracked_reaper_until_reader_exit() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let mut source = MongoDbCdcSource::new(config, None);
    let terminal = source.terminal_task_tracker().unwrap();
    let release = Arc::new(Notify::new());
    let task_release = Arc::clone(&release);
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let reader_guard = source
        .task_owner
        .track()
        .expect("live test source must admit its reader");
    source.reader_handle = Some(tokio::spawn(async move {
        let _reader_guard = reader_guard;
        let _drop_signal = TaskDropSignal(Some(dropped_tx));
        task_release.notified().await;
    }));

    source.close().await.unwrap();
    drop(source);
    assert!(!terminal.is_terminated());
    release.notify_one();
    dropped_rx
        .await
        .expect("tracked reader must exit after its release");
    terminal.wait_terminated().await;
}

#[cfg(feature = "mongodb-cdc")]
#[tokio::test]
async fn drop_signals_and_tracks_the_owned_reader() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let mut source = MongoDbCdcSource::new(config, None);
    let terminal = source.terminal_task_tracker().unwrap();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let reader_guard = source
        .task_owner
        .track()
        .expect("live test source must admit its reader");
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

#[cfg(feature = "mongodb-cdc")]
#[test]
fn tracker_covers_a_reader_destroyed_before_first_poll_on_another_runtime() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let mut source = MongoDbCdcSource::new(config, None);
    let terminal = source.terminal_task_tracker().unwrap();
    let reader_guard = source
        .task_owner
        .track()
        .expect("live test source must admit its reader");
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
        .expect("tracker must resolve across runtimes after actual task destruction");
    });
}

#[cfg(feature = "mongodb-cdc")]
#[tokio::test]
async fn byte_budget_wait_is_cancelled_by_shutdown() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let source = MongoDbCdcSource::new(config, None);
    let held = Arc::clone(&source.byte_budget)
        .acquire_many_owned(u32::try_from(source.config.max_buffered_bytes).unwrap())
        .await
        .unwrap();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let acquire = acquire_mongo_event_ownership(
        sample_event(OperationType::Insert),
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

#[cfg(feature = "mongodb-cdc")]
#[tokio::test]
async fn terminal_reader_error_preserves_classification_outside_the_event_queue() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let mut source = MongoDbCdcSource::new(config, None);
    let (error_tx, error_rx) = tokio::sync::watch::channel(None);
    source.reader_error = Some(error_rx);
    error_tx.send_replace(Some(MongoReaderFailure::Configuration(
        "reader failed".to_string(),
    )));

    let error = source.poll_batch(1).await.unwrap_err();
    assert!(error.to_string().contains("reader failed"));
    assert!(!error.is_transient());
}

#[cfg(feature = "mongodb-cdc")]
#[tokio::test(start_paused = true)]
async fn reader_admission_timeout_signals_and_joins_the_candidate() {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
    let mut handle = tokio::spawn(async move {
        let _ready_tx = ready_tx;
        shutdown_rx.changed().await.unwrap();
        assert!(*shutdown_rx.borrow());
        let _ = stopped_tx.send(());
    });

    let error = await_mongo_reader_ready(ready_rx, &shutdown_tx, &mut handle)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("startup deadline"), "{error}");
    stopped_rx.await.unwrap();
    assert!(handle.is_finished());
}

#[cfg(feature = "mongodb-cdc")]
#[tokio::test(start_paused = true)]
async fn reader_admission_timeout_does_not_claim_a_stuck_candidate_finished() {
    let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let mut handle = tokio::spawn(async move {
        let _ready_tx = ready_tx;
        let _drop_signal = TaskDropSignal(Some(dropped_tx));
        std::future::pending::<()>().await;
    });

    let error = await_mongo_reader_ready(ready_rx, &shutdown_tx, &mut handle)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("startup deadline"), "{error}");
    assert!(!handle.is_finished());
    handle.abort();
    let _ = handle.await;
    dropped_rx
        .await
        .expect("test cleanup must destroy the candidate reader future");
}

#[cfg(feature = "mongodb-cdc")]
#[tokio::test]
async fn cancelling_admission_signals_its_candidate() {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let _drop_signal = TaskDropSignal(Some(dropped_tx));
        let _ = shutdown_rx.changed().await;
    });
    tokio::task::yield_now().await;

    let guard = MongoReaderAdmissionGuard::new(shutdown_tx.clone());
    drop(guard);

    dropped_rx
        .await
        .expect("admission guard must stop the candidate reader");
    assert!(*shutdown_tx.borrow());
    handle.await.unwrap();
}

#[cfg(feature = "mongodb-cdc")]
#[tokio::test]
async fn failed_reader_admission_preserves_state_and_allows_same_instance_retry() {
    let mut original_config =
        MongoDbSourceConfig::new("mongodb://localhost:27017", "original", "events");
    original_config.max_buffered_bytes = 32 * 1024 * 1024;
    let mut source = admitted_source(original_config);
    let original_config = serde_json::to_value(&source.config).unwrap();
    let original_checkpoint = source.checkpoint();
    let original_byte_budget = Arc::clone(&source.byte_budget);

    let mut candidate = valid_connector_config();
    candidate.set("connection.uri", "http://localhost:27017");
    candidate.set("database", "candidate");
    candidate.set("collection", "changes");
    candidate.set("max.buffered.bytes", "16777216");
    let candidate_config = MongoDbSourceConfig::from_config(&candidate).unwrap();
    let checkpoint = recovery_checkpoint(
        &candidate_config,
        RESUME_TOKEN_OFFSET,
        r#"{"_data":"candidate"}"#,
    );

    let first_error = source
        .start(
            SourceStart::new(
                candidate.clone(),
                SourcePosition::Resume {
                    attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(11),
                    checkpoint,
                },
                crate::connector::DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(
        first_error.to_string().contains("parse URI"),
        "{first_error}"
    );
    assert!(!first_error.is_transient());
    assert_eq!(source.state, ConnectorState::Created);
    assert_eq!(
        serde_json::to_value(&source.config).unwrap(),
        original_config
    );
    assert_eq!(source.checkpoint(), original_checkpoint);
    assert!(Arc::ptr_eq(&source.byte_budget, &original_byte_budget));
    assert!(source.reader_handle.is_none());
    assert!(source.event_rx.is_none());
    assert!(source.reader_shutdown.is_none());
    assert!(source.reader_error.is_none());

    let retry_error = source
        .start(
            SourceStart::new(
                candidate,
                SourcePosition::Initial,
                crate::connector::DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(
        retry_error.to_string().contains("parse URI"),
        "{retry_error}"
    );
    assert!(!retry_error.is_transient());
    assert_eq!(source.state, ConnectorState::Created);
    assert_eq!(
        serde_json::to_value(&source.config).unwrap(),
        original_config
    );
    assert_eq!(source.checkpoint(), original_checkpoint);
}

#[test]
fn drain_preserves_resume_token_in_output_envelope() {
    let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
    let mut source = MongoDbCdcSource::new(config, None);

    let mut event = sample_event(OperationType::Insert);
    event.resume_token = r#"{"_data":"final_token"}"#.to_string();
    source.enqueue_event(event).unwrap();

    let batch = source.drain_to_batch(10).unwrap().unwrap().records;
    let index = batch.schema().index_of("_resume_token").unwrap();
    let tokens = batch
        .column(index)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(tokens.value(0), r#"{"_data":"final_token"}"#);
}
