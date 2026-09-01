use super::debezium::decode_debezium_mutations;
use super::drain::{
    claim_kafka_drain_execution, kafka_assignment_fence_matches,
    validate_kafka_partition_error_list, KAFKA_DRAIN_EXECUTION_CANCELLED,
    KAFKA_DRAIN_EXECUTION_PENDING,
};
use super::*;
use crate::connector::SourceBatchCursor;
use crate::kafka::offsets::{KAFKA_CHECKPOINT_VERSION, KAFKA_CHECKPOINT_VERSION_KEY};
use arrow_schema::{DataType, Field, Schema};
use rdkafka::mocking::MockCluster;
use std::time::Duration;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

fn test_config() -> KafkaSourceConfig {
    let mut cfg = KafkaSourceConfig::default();
    cfg.bootstrap_servers = "localhost:9092".into();
    cfg.group_id = "test-group".into();
    cfg.subscription = TopicSubscription::Topics(vec!["events".into()]);
    cfg
}

fn assignment_publication(
    source_name: &str,
    version: u64,
    owned_partitions: Arc<KafkaPartitionSet>,
    baselines: KafkaRotationBaselines,
) -> KafkaAssignmentPublication {
    let input_channels = kafka_input_channels(source_name, &owned_partitions).unwrap();
    KafkaAssignmentPublication::new(version, owned_partitions, input_channels, baselines)
}

fn install_manual_partitions(source: &mut KafkaSource, partitions: KafkaPartitionSet) {
    source.manual_input_channels =
        kafka_input_channels(source.source_name.as_ref(), &partitions).unwrap();
    source.manual_topic_partitions = partitions;
}

fn drain_request() -> SourceDrainRequest {
    SourceDrainRequest::new(laminar_core::checkpoint::AssignmentDrainId {
        predecessor_version: 7,
        target_version: 8,
        digest: [9; 32],
    })
    .unwrap()
}

fn payload(offset: i64) -> KafkaPayload {
    KafkaPayload {
        data: vec![u8::try_from(offset).unwrap_or_default()],
        topic: Arc::from("events"),
        partition: 1,
        partition_vnode: None,
        offset,
        timestamp_ms: None,
        headers_json: None,
    }
}

struct FirstRecordOnlyDeserializer;

impl RecordDeserializer for FirstRecordOnlyDeserializer {
    fn deserialize(
        &self,
        data: &[u8],
        schema: &SchemaRef,
    ) -> Result<arrow_array::RecordBatch, crate::error::SerdeError> {
        serde::json::JsonDeserializer::new().deserialize(data, schema)
    }

    fn deserialize_batch(
        &self,
        records: &[&[u8]],
        schema: &SchemaRef,
    ) -> Result<arrow_array::RecordBatch, crate::error::SerdeError> {
        records.first().map_or_else(
            || Ok(arrow_array::RecordBatch::new_empty(Arc::clone(schema))),
            |record| self.deserialize(record, schema),
        )
    }

    fn format(&self) -> Format {
        Format::Json
    }
}

struct FlipAssignmentDuringDecode {
    registry: Arc<laminar_core::state::VnodeRegistry>,
    publication: Arc<Mutex<Arc<KafkaAssignmentPublication>>>,
    next_owner: laminar_core::state::NodeId,
    next_publication: Arc<KafkaAssignmentPublication>,
}

impl RecordDeserializer for FlipAssignmentDuringDecode {
    fn deserialize(
        &self,
        data: &[u8],
        schema: &SchemaRef,
    ) -> Result<arrow_array::RecordBatch, crate::error::SerdeError> {
        serde::json::JsonDeserializer::new().deserialize(data, schema)
    }

    fn deserialize_batch(
        &self,
        records: &[&[u8]],
        schema: &SchemaRef,
    ) -> Result<arrow_array::RecordBatch, crate::error::SerdeError> {
        self.registry.set_assignment_and_version(
            [self.next_owner].into(),
            self.next_publication.assignment_version,
        );
        *lock_or_recover(&self.publication) = Arc::clone(&self.next_publication);
        serde::json::JsonDeserializer::new().deserialize_batch(records, schema)
    }

    fn format(&self) -> Format {
        Format::Json
    }
}

#[tokio::test]
async fn guaranteed_delivery_rejects_any_decode_failure_without_advancing_cursor() {
    let mut config = test_config();
    config.max_deser_error_rate = 1.0;
    let mut source = KafkaSource::new(test_schema(), config, None);
    source.source_name = Arc::from("events-source");
    source.state = ConnectorState::Running;
    source.delivery = DeliveryGuarantee::AtLeastOnce;
    source.offsets.update_force("events", 1, 9);
    lock_or_recover(&source.offset_snapshot).update_force("events", 1, 9);
    install_manual_partitions(
        &mut source,
        KafkaPartitionSet::from([("events".to_string(), 1)]),
    );
    let checkpoint_before = source.try_checkpoint().unwrap().unwrap();

    let (tx, rx) = crossfire::mpsc::bounded_async::<KafkaReaderItem>(3);
    for (offset, data) in [
        (10, br#"{"id":10,"value":"good"}"#.as_slice()),
        (11, b"not-json".as_slice()),
        (12, br#"{"id":12,"value":"good"}"#.as_slice()),
    ] {
        tx.send(KafkaReaderItem::Payload(KafkaPayload {
            data: data.to_vec(),
            topic: Arc::from("events"),
            partition: 1,
            partition_vnode: None,
            offset,
            timestamp_ms: None,
            headers_json: None,
        }))
        .await
        .unwrap();
    }
    source.channel_len.store(3, Ordering::Release);
    source.msg_rx = Some(rx);

    let error = source
        .poll_batch(3)
        .await
        .expect_err("guaranteed delivery must not skip a poison pill");
    assert!(matches!(error, ConnectorError::Serde(_)));
    assert_eq!(source.state, ConnectorState::Failed);
    assert_eq!(source.offsets.get("events", 1), Some(9));
    assert_eq!(
        lock_or_recover(&source.offset_snapshot).get("events", 1),
        Some(9)
    );
    assert_eq!(source.try_checkpoint().unwrap().unwrap(), checkpoint_before);
}

#[tokio::test]
async fn guaranteed_post_drain_registry_failure_requires_a_fresh_generation() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let registry_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/42"))
        .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
        .mount(&registry_server)
        .await;

    let mut config = test_config();
    config.format = Format::Avro;
    config.schema_registry_url = Some(registry_server.uri());
    let registry = SchemaRegistryClient::new(registry_server.uri(), None).unwrap();
    let mut source = KafkaSource::with_schema_registry(test_schema(), config, registry);
    source.source_name = Arc::from("events-source");
    source.state = ConnectorState::Running;
    source.delivery = DeliveryGuarantee::AtLeastOnce;
    source.offsets.update_force("events", 1, 9);
    lock_or_recover(&source.offset_snapshot).update_force("events", 1, 9);
    install_manual_partitions(
        &mut source,
        KafkaPartitionSet::from([("events".to_string(), 1)]),
    );
    let checkpoint_before = source.try_checkpoint().unwrap().unwrap();
    let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    source.reader_shutdown = Some(shutdown);

    let (tx, rx) = crossfire::mpsc::bounded_async::<KafkaReaderItem>(1);
    tx.send(KafkaReaderItem::Payload(KafkaPayload {
        data: vec![0, 0, 0, 0, 42],
        topic: Arc::from("events"),
        partition: 1,
        partition_vnode: None,
        offset: 10,
        timestamp_ms: None,
        headers_json: None,
    }))
    .await
    .unwrap();
    source.channel_len.store(1, Ordering::Release);
    source.msg_rx = Some(rx);

    let error = source
        .poll_batch(1)
        .await
        .expect_err("a post-drain registry failure must retire guaranteed delivery");
    assert!(
        matches!(error, ConnectorError::Internal(message) if message.contains("durable cursor"))
    );
    assert_eq!(source.state, ConnectorState::Failed);
    assert!(*shutdown_rx.borrow());
    assert_eq!(source.offsets.get("events", 1), Some(9));
    assert_eq!(source.try_checkpoint().unwrap().unwrap(), checkpoint_before);
}

#[tokio::test]
async fn best_effort_retains_explicit_poison_pill_threshold() {
    let mut config = test_config();
    config.max_deser_error_rate = 1.0;
    let mut source = KafkaSource::new(test_schema(), config, None);
    source.source_name = Arc::from("events-source");
    source.state = ConnectorState::Running;
    source.delivery = DeliveryGuarantee::BestEffort;
    source.offsets.update_force("events", 1, 9);
    lock_or_recover(&source.offset_snapshot).update_force("events", 1, 9);

    let (tx, rx) = crossfire::mpsc::bounded_async::<KafkaReaderItem>(3);
    for (offset, data) in [
        (10, br#"{"id":10,"value":"good"}"#.as_slice()),
        (11, b"not-json".as_slice()),
        (12, br#"{"id":12,"value":"good"}"#.as_slice()),
    ] {
        tx.send(KafkaReaderItem::Payload(KafkaPayload {
            data: data.to_vec(),
            topic: Arc::from("events"),
            partition: 1,
            partition_vnode: None,
            offset,
            timestamp_ms: None,
            headers_json: None,
        }))
        .await
        .unwrap();
    }
    source.channel_len.store(3, Ordering::Release);
    source.msg_rx = Some(rx);

    let batch = source.poll_batch(3).await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(source.state, ConnectorState::Running);
    assert_eq!(source.offsets.get("events", 1), Some(12));
    assert_eq!(
        lock_or_recover(&source.offset_snapshot).get("events", 1),
        Some(12)
    );
}

#[tokio::test]
async fn decoded_row_count_mismatch_preserves_cursor_and_rotation_baseline() {
    let node = laminar_core::state::NodeId(1);
    let registry = Arc::new(laminar_core::state::VnodeRegistry::single_owner(1, node));
    let version = registry.assignment_version();
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source.source_name = Arc::from("events-source");
    source.state = ConnectorState::Running;
    source.delivery = DeliveryGuarantee::AtLeastOnce;
    source.deserializer = Box::new(FirstRecordOnlyDeserializer);
    source.vnode_assignment = Some((Arc::clone(&registry), node));
    source
        .reconciled_assignment_version
        .store(version, Ordering::Release);
    source.applied_rotation_baseline_version = Some(version);
    source.offsets.update_force("events", 1, 9);
    lock_or_recover(&source.offset_snapshot).update_force("events", 1, 9);

    let baselines = KafkaRotationBaselines::from([(
        Arc::from("events"),
        std::collections::HashMap::from([(1, 10)]),
    )]);
    *lock_or_recover(&source.assignment_publication) = Arc::new(assignment_publication(
        source.source_name.as_ref(),
        version,
        Arc::new(KafkaPartitionSet::from([("events".to_string(), 1)])),
        baselines,
    ));
    source
        .rotation_partition_baseline_count
        .store(1, Ordering::Release);
    let checkpoint_before = source.try_checkpoint().unwrap().unwrap();

    let (tx, rx) = crossfire::mpsc::bounded_async::<KafkaReaderItem>(2);
    for offset in [10, 11] {
        tx.send(KafkaReaderItem::Payload(KafkaPayload {
            data: format!(r#"{{"id":{offset},"value":"good"}}"#).into_bytes(),
            topic: Arc::from("events"),
            partition: 1,
            partition_vnode: Some(0),
            offset,
            timestamp_ms: None,
            headers_json: None,
        }))
        .await
        .unwrap();
    }
    source.channel_len.store(2, Ordering::Release);
    source.msg_rx = Some(rx);

    let error = source
        .poll_batch(2)
        .await
        .expect_err("a partial successful decode must reject cursor publication");
    assert!(matches!(
        error,
        ConnectorError::Serde(SerdeError::RecordCountMismatch {
            expected: 2,
            got: 1
        })
    ));
    assert_eq!(source.state, ConnectorState::Failed);
    assert_eq!(source.offsets.get("events", 1), Some(9));
    assert_eq!(
        lock_or_recover(&source.offset_snapshot).get("events", 1),
        Some(9)
    );
    let publication = lock_or_recover(&source.assignment_publication);
    assert_eq!(
        rotation_partition_baseline(&publication.baselines, "events", 1),
        Some(10)
    );
    assert_eq!(
        source
            .rotation_partition_baseline_count
            .load(Ordering::Acquire),
        1
    );
    drop(publication);
    assert_eq!(source.try_checkpoint().unwrap().unwrap(), checkpoint_before);
}

#[tokio::test]
async fn decoded_batch_retains_the_assignment_cut_pinned_during_drain() {
    let old_owner = laminar_core::state::NodeId(1);
    let next_owner = laminar_core::state::NodeId(2);
    let registry = Arc::new(laminar_core::state::VnodeRegistry::new_unassigned(1));
    registry.set_assignment_and_version([old_owner].into(), 1);

    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source.source_name = Arc::from("events-source");
    source.state = ConnectorState::Running;
    source.delivery = DeliveryGuarantee::AtLeastOnce;
    source.vnode_assignment = Some((Arc::clone(&registry), old_owner));
    source
        .reconciled_assignment_version
        .store(1, Ordering::Release);
    source.applied_rotation_baseline_version = Some(1);

    let previous_partitions = Arc::new(KafkaPartitionSet::from([("events".to_string(), 0)]));
    let old_channels =
        kafka_input_channels(source.source_name.as_ref(), &previous_partitions).unwrap();
    let baselines = KafkaRotationBaselines::from([(
        Arc::from("events"),
        std::collections::HashMap::from([(0, 10)]),
    )]);
    *lock_or_recover(&source.assignment_publication) = Arc::new(KafkaAssignmentPublication::new(
        1,
        Arc::clone(&previous_partitions),
        Arc::clone(&old_channels),
        baselines,
    ));
    source
        .rotation_partition_baseline_count
        .store(1, Ordering::Release);

    let next_publication = Arc::new(assignment_publication(
        source.source_name.as_ref(),
        2,
        Arc::new(KafkaPartitionSet::new()),
        KafkaRotationBaselines::new(),
    ));
    source.deserializer = Box::new(FlipAssignmentDuringDecode {
        registry: Arc::clone(&registry),
        publication: Arc::clone(&source.assignment_publication),
        next_owner,
        next_publication,
    });

    let (tx, rx) = crossfire::mpsc::bounded_async::<KafkaReaderItem>(1);
    tx.send(KafkaReaderItem::Payload(KafkaPayload {
        data: br#"{"id":10,"value":"accepted"}"#.to_vec(),
        topic: Arc::from("events"),
        partition: 0,
        partition_vnode: Some(0),
        offset: 10,
        timestamp_ms: None,
        headers_json: None,
    }))
    .await
    .unwrap();
    source.channel_len.store(1, Ordering::Release);
    source.msg_rx = Some(rx);

    let mut batch = source.poll_batch(1).await.unwrap().unwrap();
    let SourceBatchCursor::Complete(checkpoint) = batch
        .take_cursor()
        .expect("vnode batch carries its drained assignment cut")
    else {
        panic!("vnode batch carried an incremental cursor for a new assignment");
    };

    assert_eq!(registry.assignment_version(), 2);
    assert_eq!(checkpoint.assignment_version(), NonZeroU64::new(1));
    assert_eq!(checkpoint.input_channels(), Some(old_channels.as_ref()));
    assert_eq!(checkpoint.get_offset("events:0"), Some("10"));
    assert!(decode_partition_baselines(&checkpoint).unwrap().is_empty());
    assert!(source.try_checkpoint().unwrap().is_none());
}

#[tokio::test]
async fn assignment_batch_cursor_is_complete_once_then_incremental_until_rotation() {
    let node = laminar_core::state::NodeId(1);
    let registry = Arc::new(laminar_core::state::VnodeRegistry::new_unassigned(2));
    registry.set_assignment_and_version([node, node].into(), 1);
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source.source_name = Arc::from("events-source");
    source.state = ConnectorState::Running;
    source.delivery = DeliveryGuarantee::ExactlyOnce;
    source.vnode_assignment = Some((Arc::clone(&registry), node));
    source.manual_partition_baselines =
        KafkaPartitionBaselines::from([(("events".to_string(), 0), 10)]);
    source
        .reconciled_assignment_version
        .store(1, Ordering::Release);
    source.applied_rotation_baseline_version = Some(1);
    let owned = Arc::new(KafkaPartitionSet::from([
        ("events".to_string(), 0),
        ("events".to_string(), 1),
    ]));
    let publication = Arc::new(assignment_publication(
        source.source_name.as_ref(),
        1,
        Arc::clone(&owned),
        KafkaRotationBaselines::from([(
            Arc::from("events"),
            std::collections::HashMap::from([(0, 10), (1, 20)]),
        )]),
    ));
    let channels = Arc::clone(&publication.input_channels);
    *lock_or_recover(&source.assignment_publication) = publication;
    source
        .rotation_partition_baseline_count
        .store(2, Ordering::Release);

    let (tx, rx) = crossfire::mpsc::bounded_async::<KafkaReaderItem>(3);
    for (partition, partition_vnode, offset) in [(0, 0, 10), (1, 1, 20), (0, 0, 12)] {
        tx.send(KafkaReaderItem::Payload(KafkaPayload {
            data: format!(r#"{{"id":{offset},"value":"accepted"}}"#).into_bytes(),
            topic: Arc::from("events"),
            partition,
            partition_vnode: Some(partition_vnode),
            offset,
            timestamp_ms: None,
            headers_json: None,
        }))
        .await
        .unwrap();
    }
    source.channel_len.store(3, Ordering::Release);
    source.msg_rx = Some(rx);

    let mut first = source.poll_batch(1).await.unwrap().unwrap();
    let SourceBatchCursor::Complete(first) = first
        .take_cursor()
        .expect("the first batch establishes the complete assignment cursor")
    else {
        panic!("the first assignment batch carried an incremental cursor");
    };
    assert_eq!(first.assignment_version(), NonZeroU64::new(1));
    assert_eq!(first.input_channels(), Some(channels.as_ref()));
    assert_eq!(first.get_offset("events:0"), Some("10"));
    assert_eq!(
        decode_partition_baselines(&first).unwrap(),
        KafkaPartitionBaselines::from([
            (("events".to_string(), 0), 10),
            (("events".to_string(), 1), 20),
        ])
    );

    let mut second = source.poll_batch(1).await.unwrap().unwrap();
    let SourceBatchCursor::Incremental(second) = second
        .take_cursor()
        .expect("a later batch carries only touched offsets")
    else {
        panic!("a later assignment batch repeated its complete cursor");
    };
    assert_eq!(second.assignment_version().get(), 1);
    assert!(Arc::ptr_eq(second.input_channels_arc(), &channels));
    assert_eq!(second.changes().len(), 2);
    assert_eq!(
        second.changes().get("events:1").and_then(Option::as_deref),
        Some("20")
    );
    assert_eq!(
        second.changes().get("@laminar.kafka.next.v1:events:1"),
        Some(&None)
    );
    let mut merged = first.clone();
    merged.apply_delta(second).unwrap();
    let barrier = source.try_checkpoint().unwrap().unwrap();
    assert_eq!(merged, barrier);
    assert_eq!(barrier.get_offset("events:1"), Some("20"));
    assert_eq!(barrier.input_channels(), Some(channels.as_ref()));
    assert_eq!(
        decode_partition_baselines(&barrier).unwrap(),
        source.manual_partition_baselines
    );

    registry.set_assignment_and_version([node, node].into(), 2);
    source
        .reconciled_assignment_version
        .store(2, Ordering::Release);
    source.applied_rotation_baseline_version = Some(2);
    *lock_or_recover(&source.assignment_publication) = Arc::new(assignment_publication(
        source.source_name.as_ref(),
        2,
        owned,
        KafkaRotationBaselines::from([(
            Arc::from("events"),
            std::collections::HashMap::from([(0, 12), (1, 30)]),
        )]),
    ));
    source
        .rotation_partition_baseline_count
        .store(2, Ordering::Release);

    let mut rotated = source.poll_batch(1).await.unwrap().unwrap();
    let SourceBatchCursor::Complete(rotated) = rotated
        .take_cursor()
        .expect("assignment rotation publishes a new complete cursor")
    else {
        panic!("assignment rotation emitted an incremental cursor");
    };
    assert_eq!(rotated.assignment_version(), NonZeroU64::new(2));
    assert_eq!(rotated.get_offset("events:0"), Some("12"));
    assert_eq!(
        decode_partition_baselines(&rotated).unwrap(),
        KafkaPartitionBaselines::from([
            (("events".to_string(), 0), 10),
            (("events".to_string(), 1), 30),
        ])
    );
}

#[test]
fn drain_partition_set_covers_the_complete_live_assignment() {
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition("events", 7);
    assignment.add_partition("audit", 2);
    assignment.add_partition("events", 1);
    let inputs = kafka_drain_partitions(&assignment).unwrap();
    assert_eq!(
        inputs
            .iter()
            .map(|input| (input.topic.as_ref(), input.partition))
            .collect::<Vec<_>>(),
        [("audit", 2), ("events", 1), ("events", 7)]
    );
}

#[test]
fn drain_partition_set_rejects_empty_and_duplicate_inputs() {
    let mut empty_topic = TopicPartitionList::new();
    empty_topic.add_partition("", 1);
    assert!(matches!(
        kafka_drain_partitions(&empty_topic),
        Err(ConnectorError::ConfigurationError(message))
            if message.contains("empty topic")
    ));

    let mut duplicate = TopicPartitionList::new();
    duplicate.add_partition("events", 1);
    duplicate.add_partition("events", 1);
    assert!(matches!(
        kafka_drain_partitions(&duplicate),
        Err(ConnectorError::ConfigurationError(message))
            if message.contains("duplicate input 'events-1'")
    ));
}

#[test]
fn drain_resolution_waits_for_the_exact_reconciled_target() {
    assert_eq!(kafka_drain_target_ready(8, 7, 7), Ok(false));
    assert_eq!(kafka_drain_target_ready(8, 8, 7), Ok(false));
    assert_eq!(kafka_drain_target_ready(8, 8, 8), Ok(true));
    assert!(kafka_drain_target_ready(8, 9, 9).is_err());
}

#[test]
fn cancelled_drain_resolution_cannot_later_claim_provider_execution() {
    let execution = AtomicU8::new(KAFKA_DRAIN_EXECUTION_PENDING);
    execution
        .compare_exchange(
            KAFKA_DRAIN_EXECUTION_PENDING,
            KAFKA_DRAIN_EXECUTION_CANCELLED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .unwrap();

    let error = claim_kafka_drain_execution(
        &execution,
        tokio::time::Instant::now() + std::time::Duration::from_secs(1),
    )
    .expect_err("cancelled resolution must not seek or resume");
    assert!(error.contains("cancelled before execution"));
    assert_eq!(
        execution.load(Ordering::Acquire),
        KAFKA_DRAIN_EXECUTION_CANCELLED
    );
}

#[tokio::test]
async fn captured_cut_outlives_prepare_deadline_and_uses_resolution_deadline() {
    let request = drain_request();
    let prepare_deadline = tokio::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(1))
        .unwrap();
    let inputs: Arc<[KafkaDrainPartition]> = Arc::from([KafkaDrainPartition {
        topic: Arc::from("events"),
        partition: 0,
    }]);
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source.source_drain = Some(KafkaSourceDrain {
        request: request.clone(),
        prepare_deadline,
        boundary: Some(KafkaDrainBoundary {
            round: request.round,
            inputs,
        }),
        cut: Some(Arc::from([KafkaDrainPosition {
            topic: Arc::from("events"),
            partition: 0,
            next_offset: 42,
        }])),
        pending_resolution: None,
    });

    // A retry's fresh wait budget cannot replace the already-completed prepare phase.
    source
        .begin_drain(
            &request,
            tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        )
        .unwrap();
    assert_eq!(
        source.source_drain.as_ref().unwrap().prepare_deadline,
        prepare_deadline
    );

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    source.reader_drain_tx = Some(tx);
    let resolution = SourceDrainResolution {
        round: request.round,
        outcome: SourceDrainOutcome::Commit,
    };
    let resolution_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    let finish = source.finish_drain(resolution, resolution_deadline);
    let respond = async {
        let KafkaReaderDrainCommand::Resolve {
            resolution: actual,
            deadline,
            execution,
            reply,
            ..
        } = rx.recv().await.unwrap()
        else {
            panic!("expected Kafka drain resolution");
        };
        assert_eq!(actual, resolution);
        assert_eq!(deadline, resolution_deadline);
        claim_kafka_drain_execution(&execution, deadline).unwrap();
        reply.send(Ok(())).unwrap();
    };
    let (result, ()) = tokio::join!(finish, respond);
    result.unwrap();
    assert!(source.source_drain.is_none());
}

#[tokio::test]
async fn drain_boundary_cannot_overtake_a_full_payload_fifo() {
    let (tx, mut rx) = crossfire::mpsc::bounded_async::<KafkaReaderItem>(1);
    tx.send(KafkaReaderItem::Payload(payload(10)))
        .await
        .unwrap();
    let inputs: Arc<[KafkaDrainPartition]> = Arc::from([KafkaDrainPartition {
        topic: Arc::from("events"),
        partition: 1,
    }]);
    let boundary = KafkaDrainBoundary {
        round: drain_request().round,
        inputs,
    };
    let marker =
        tokio::spawn(async move { tx.send(KafkaReaderItem::DrainBoundary(boundary)).await });
    tokio::task::yield_now().await;
    assert!(
        !marker.is_finished(),
        "full payload slot must hold the marker back"
    );

    assert!(matches!(
        rx.recv().await.unwrap(),
        KafkaReaderItem::Payload(KafkaPayload { offset: 10, .. })
    ));
    marker.await.unwrap().unwrap();
    assert!(matches!(
        rx.recv().await.unwrap(),
        KafkaReaderItem::DrainBoundary(_)
    ));
}

#[test]
fn any_partition_error_rejects_pause_completion() {
    assert!(validate_kafka_partition_error_list("drain pause", &[]).is_ok());
    let error = validate_kafka_partition_error_list(
        "drain pause",
        &["events-3: Local: Erroneous state".into()],
    )
    .unwrap_err();
    assert!(error.contains("events-3"));
}

#[test]
fn test_new_defaults() {
    let source = KafkaSource::new(test_schema(), test_config(), None);
    assert_eq!(source.state(), ConnectorState::Created);
    assert!(source.consumer.is_none());
    assert_eq!(source.offsets.partition_count(), 0);
}

#[tokio::test]
async fn retired_generation_reaps_uncooperative_blocking_worker() {
    let (task_owner, task_tracker) = ConnectorTaskOwner::new();
    let tasks = KafkaBlockingTasks::new(task_owner.track().unwrap());
    let worker_tasks = tasks.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();

    let caller = tokio::spawn(async move {
        worker_tasks
            .run(move || {
                let _ = started_tx.send(());
                release_rx.recv().expect("release blocking worker");
                7usize
            })
            .await
    });
    started_rx.await.expect("blocking worker did not start");
    caller.abort();
    let _ = caller.await;

    assert_eq!(tasks.tracked_count().await, 1);
    assert!(
        !tasks
            .join_until(tokio::time::Instant::now() + Duration::from_millis(10))
            .await,
        "a started spawn_blocking worker cannot be aborted"
    );
    tasks.ensure_reaper();
    release_tx.send(()).unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        while tasks.tracked_count().await != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("generation reaper did not join the blocking worker");

    let result = tasks.run(|| 9usize).await;
    assert_eq!(result, Err(KafkaBlockingTaskError::Retired));
    drop(tasks);
    drop(task_owner);
    tokio::time::timeout(Duration::from_secs(1), task_tracker.wait_terminated())
        .await
        .expect("terminal tracker retained a completed blocking generation");
}

#[test]
fn source_contract_is_replayable_and_splittable() {
    let source = KafkaSource::new(test_schema(), test_config(), None);
    let contract = source
        .contract(&ConnectorConfig::new("kafka"))
        .expect("static Kafka contract");
    assert_eq!(contract.consistency, SourceConsistency::Replayable);
    assert_eq!(contract.topology, SourceTopology::Splittable);
    assert_eq!(contract.input_mode, SourceInputMode::AppendOnly);
    assert_eq!(
        contract.row_positions,
        SourceRowPositionCapability::OrderedDeterministic
    );
    assert!(contract.is_exact_delivery_certified());
}

#[test]
fn kafka_positions_use_topic_partition_and_ordered_offset() {
    let staged = vec![
        (Arc::<str>::from("events"), 3, 7),
        (Arc::<str>::from("events"), 3, 8),
    ];
    let positions = kafka_row_positions("prices", &staged, None).unwrap();
    let encoded_partition = positions.partition().value(0);

    assert_eq!(&encoded_partition[4..10], b"prices");
    assert_eq!(&encoded_partition[14..20], b"events");
    assert_eq!(&encoded_partition[20..], &3_i32.to_be_bytes());
    assert!(positions.order_key().value(0) < positions.order_key().value(1));
    assert_eq!(positions.sub_offset().values(), &[0, 0]);
}

#[test]
fn debezium_operations_are_validated_and_normalized() {
    let operations = arrow_array::StringArray::from(vec!["c", "u", "r", "d"]);
    assert_eq!(
        decode_debezium_mutations(&operations, 4)
            .unwrap()
            .as_deref(),
        Some(
            &[
                SourceMutation::Put,
                SourceMutation::Put,
                SourceMutation::Put,
                SourceMutation::Tombstone,
            ][..]
        )
    );

    let put_only = arrow_array::StringArray::from(vec!["c", "u", "r"]);
    assert!(decode_debezium_mutations(&put_only, 3).unwrap().is_none());
    assert!(decode_debezium_mutations(&put_only, 2).is_err());
    assert!(
        decode_debezium_mutations(&arrow_array::StringArray::from(vec![Some("c"), None]), 2)
            .is_err()
    );
    assert!(decode_debezium_mutations(&arrow_array::StringArray::from(vec!["x"]), 1).is_err());
}

#[tokio::test]
async fn debezium_poll_returns_declared_schema_with_positions_and_mutations() {
    let mut config = test_config();
    config.format = Format::Debezium;
    config.include_metadata = true;
    config.include_headers = true;
    let mut source = KafkaSource::new(test_schema(), config, None);
    source.state = ConnectorState::Running;
    source.source_name = Arc::from("inventory");

    let (tx, rx) = crossfire::mpsc::bounded_async::<KafkaReaderItem>(2);
    for (offset, data) in [
        (
            10,
            br#"{"before":null,"after":{"id":1,"value":"new"},"op":"c","ts_ms":1}"#.as_slice(),
        ),
        (
            11,
            br#"{"before":{"id":1,"value":"new"},"after":null,"op":"d","ts_ms":2}"#.as_slice(),
        ),
    ] {
        tx.send(KafkaReaderItem::Payload(KafkaPayload {
            data: data.to_vec(),
            topic: Arc::from("events"),
            partition: 1,
            partition_vnode: None,
            offset,
            timestamp_ms: Some(offset),
            headers_json: Some(format!(r#"{{"offset":{offset}}}"#)),
        }))
        .await
        .unwrap();
    }
    source.channel_len.store(2, Ordering::Release);
    source.msg_rx = Some(rx);

    let batch = source.poll_batch(2).await.unwrap().unwrap();
    assert_eq!(
        batch.mutations(),
        Some(&[SourceMutation::Put, SourceMutation::Tombstone][..])
    );
    assert_eq!(batch.records.schema(), source.schema());
    assert!(batch.records.column_by_name("__op").is_none());
    assert!(batch.records.column_by_name("__ts_ms").is_none());
    for name in ["_partition", "_offset", "_timestamp", "_headers"] {
        assert!(
            batch.records.column_by_name(name).is_some(),
            "missing {name}"
        );
    }
    let positions = batch.row_positions().unwrap();
    assert_eq!(positions.partition().len(), 2);
    assert!(positions.order_key().value(0) < positions.order_key().value(1));
}

#[test]
fn debezium_contract_is_keyed_upsert_from_request_config() {
    let source = KafkaSource::new(test_schema(), test_config(), None);
    let mut config = ConnectorConfig::new("kafka");
    config.set("bootstrap.servers", "localhost:9092");
    config.set("group.id", "contract-test");
    config.set("topic", "events");
    config.set("format", "debezium");

    let contract = source.contract(&config).unwrap();
    assert_eq!(contract.input_mode, SourceInputMode::KeyedUpsert);
}

#[tokio::test]
async fn progress_commit_enqueues_before_first_poll() {
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source
        .start(
            SourceStart::new(
                ConnectorConfig::new("kafka"),
                SourcePosition::Initial,
                DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .expect("consumer creation and subscription are non-blocking");

    assert!(source.consumer.is_some());
    assert!(source.reader_handle.is_none());
    assert!(source.msg_rx.is_none());
    source
        .notify_epoch_committed(1, &durable_kafka_checkpoint())
        .await
        .expect("advisory commit enqueue must not depend on source polling");

    source.channel_len.store(7, Ordering::Release);
    source.close().await.unwrap();
    assert!(source.consumer.is_none());
    assert_eq!(source.channel_len.load(Ordering::Acquire), 0);

    let error = source
        .start(
            SourceStart::new(
                ConnectorConfig::new("kafka"),
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("a closed connector instance is not restartable");
    assert!(matches!(error, ConnectorError::InvalidState { .. }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborting_a_blocked_background_task_respects_the_close_deadline() {
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let mut handle = Some(tokio::spawn(async move {
        let _ = started_tx.send(());
        std::thread::sleep(std::time::Duration::from_millis(250));
    }));
    started_rx.await.unwrap();

    let started = tokio::time::Instant::now();
    join_background_task(
        &mut handle,
        started + std::time::Duration::from_millis(10),
        "blocked-test-task",
    )
    .await;
    assert!(
        started.elapsed() < std::time::Duration::from_millis(100),
        "aborting a task in synchronous library code must not extend close()"
    );
}

#[tokio::test]
async fn dropping_source_signals_and_destroys_retained_reader() {
    struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    let terminal = source.terminal_task_tracker().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let reader = tokio::spawn(async move {
        let _drop_signal = DropSignal(Some(dropped_tx));
        let _ = started_tx.send(());
        std::future::pending::<()>().await;
    });
    started_rx.await.expect("test reader did not start");

    source.reader_shutdown = Some(shutdown_tx);
    source.reader_handle = Some(reader);
    drop(source);

    assert!(*shutdown_rx.borrow());
    tokio::time::timeout(Duration::from_secs(1), dropped_rx)
        .await
        .expect("retained Kafka reader was not aborted on source drop")
        .expect("reader drop signal was lost");
    tokio::time::timeout(Duration::from_secs(1), terminal.wait_terminated())
        .await
        .expect("Kafka source tracker outlived its reaped reader");
}

#[tokio::test]
async fn guaranteed_dynamic_broker_ownership_fails_before_activation() {
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    let mut request_config = ConnectorConfig::new("kafka");
    request_config.set("bootstrap.servers", "unreachable.invalid:9092");
    request_config.set("group.id", "replacement-group");
    request_config.set("topic.pattern", "replacement-.*");
    request_config.set("laminar.source.name", "replacement-source");

    let mut checkpoint = SourceCheckpoint::with_offsets(std::collections::HashMap::from([(
        "replacement-topic:0".to_string(),
        "42".to_string(),
    )]));
    checkpoint.set_metadata("connector", "kafka");
    checkpoint.set_metadata(KAFKA_CHECKPOINT_VERSION_KEY, KAFKA_CHECKPOINT_VERSION);
    let error = source
        .start(
            SourceStart::new(
                request_config,
                SourcePosition::Resume {
                    attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(23),
                    checkpoint,
                },
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("dynamic broker-managed ownership must fail closed");

    assert!(
        matches!(
        &error,
        ConnectorError::ConfigurationError(message)
            if message.contains("topic patterns") && message.contains("engine-owned")
        ),
        "unexpected admission error: {error:?}"
    );
    assert_eq!(source.state(), ConnectorState::Created);
    assert!(source.consumer.is_none());
    assert!(source.msg_rx.is_none());
    assert!(source.reader_handle.is_none());
    assert_eq!(source.config.group_id, "test-group");
    assert_eq!(source.offsets.partition_count(), 0);
    assert!(source.source_name.is_empty());
    assert!(!source
        .deterministic_unrecorded_position
        .load(Ordering::Acquire));
}

#[tokio::test]
async fn vnode_assignment_rejects_dynamic_topic_patterns_before_activation() {
    let mut config = test_config();
    config.subscription = TopicSubscription::Pattern("events-.*".into());
    let mut source = KafkaSource::new(test_schema(), config, None);
    source
        .set_vnode_assignment(
            "events_source",
            Arc::new(laminar_core::state::VnodeRegistry::new(4)),
            laminar_core::state::NodeId(1),
        )
        .unwrap();
    let error = source
        .start(
            SourceStart::new(
                ConnectorConfig::new("kafka"),
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("dynamic topic inventory cannot be fenced by vnode assignment");
    assert!(matches!(
        error,
        ConnectorError::ConfigurationError(message)
            if message.contains("topic patterns") && message.contains("engine-owned")
    ));
    assert_eq!(source.state(), ConnectorState::Created);
    assert!(source.consumer.is_none());
}

#[test]
fn vnode_assignment_rejects_invalid_identity_or_owner() {
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    let error = source
        .set_vnode_assignment(
            "",
            Arc::new(laminar_core::state::VnodeRegistry::new(4)),
            laminar_core::state::NodeId(1),
        )
        .expect_err("vnode ownership requires a canonical source identity");
    assert!(matches!(
        error,
        ConnectorError::ConfigurationError(message)
            if message.contains("canonical source identity")
    ));

    let error = source
        .set_vnode_assignment(
            "events_source",
            Arc::new(laminar_core::state::VnodeRegistry::new(4)),
            laminar_core::state::NodeId::UNASSIGNED,
        )
        .expect_err("the reserved unassigned node must never own Kafka inputs");

    assert!(matches!(
        error,
        ConnectorError::ConfigurationError(message)
            if message.contains("nonzero node identity")
    ));
    assert!(source.vnode_assignment.is_none());
}

#[tokio::test]
async fn vnode_assignment_rejects_a_mismatched_catalog_identity_before_io() {
    let node = laminar_core::state::NodeId(1);
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source
        .set_vnode_assignment(
            "events_source",
            Arc::new(laminar_core::state::VnodeRegistry::single_owner(4, node)),
            node,
        )
        .unwrap();
    let mut request_config = ConnectorConfig::new("kafka");
    request_config.set("bootstrap.servers", "unreachable.invalid:9092");
    request_config.set("group.id", "test-group");
    request_config.set("topic", "events");
    request_config.set("startup.mode", "earliest");
    request_config.set("laminar.source.name", "other_source");

    let error = source
        .start(
            SourceStart::new(
                request_config,
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("catalog identity mismatch must fail before Kafka construction");
    assert!(matches!(
        error,
        ConnectorError::ConfigurationError(message)
            if message.contains("events_source") && message.contains("other_source")
    ));
    assert_eq!(source.state(), ConnectorState::Created);
    assert!(source.consumer.is_none());
    assert!(source.reader_handle.is_none());
}

#[tokio::test]
async fn terminal_reader_fault_preempts_assignment_wait_and_control_plane() {
    let node = laminar_core::state::NodeId(1);
    let registry = Arc::new(laminar_core::state::VnodeRegistry::single_owner(1, node));
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source.state = ConnectorState::Running;
    source.source_name = Arc::from("events_source");
    source.vnode_assignment = Some((Arc::clone(&registry), node));
    source
        .reconciled_assignment_version
        .store(registry.assignment_version(), Ordering::Release);

    // Make the control-plane publication newer than the reader fence. Before
    // the terminal-fault latch, poll_batch returned Ok(None) forever here.
    registry.set_assignment(registry.snapshot());
    assert_ne!(
        source.reconciled_assignment_version.load(Ordering::Acquire),
        registry.assignment_version()
    );
    let data_ready = Arc::clone(&source.data_ready);
    let wake = data_ready.notified();
    publish_reader_fault(
        &source.reader_fault,
        &source.data_ready,
        "injected terminal reader fault",
    );
    wake.await;
    let drain_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);

    for error in [
        source
            .poll_batch(1)
            .await
            .expect_err("poll must fail closed"),
        source
            .checkpoint_ready()
            .expect_err("checkpoint readiness must fail closed"),
        source
            .try_checkpoint()
            .expect_err("checkpoint capture must fail closed"),
        source
            .begin_drain(&drain_request(), drain_deadline)
            .expect_err("drain start must fail closed"),
        source
            .poll_drain_ready(drain_request().round)
            .expect_err("drain cut must fail closed"),
        source
            .finish_drain(
                SourceDrainResolution {
                    round: drain_request().round,
                    outcome: SourceDrainOutcome::Abort,
                },
                drain_deadline,
            )
            .await
            .expect_err("drain resolution must fail closed"),
    ] {
        assert!(
            error.to_string().contains("injected terminal reader fault"),
            "unexpected terminal-fault error: {error}"
        );
    }
}

#[test]
fn drain_rejects_an_unreconciled_predecessor_before_starting_the_reader() {
    let node = laminar_core::state::NodeId(1);
    let registry = Arc::new(laminar_core::state::VnodeRegistry::single_owner(4, node));
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source.source_name = Arc::from("events_source");
    source.vnode_assignment = Some((Arc::clone(&registry), node));
    let request = SourceDrainRequest::new(laminar_core::checkpoint::AssignmentDrainId {
        predecessor_version: registry.assignment_version(),
        target_version: registry.assignment_version() + 1,
        digest: [9; 32],
    })
    .unwrap();

    let error = source
        .begin_drain(
            &request,
            tokio::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .expect_err("unreconciled predecessor must fail closed");
    assert!(matches!(
        error,
        ConnectorError::InvalidState { expected, actual }
            if expected.contains("reconciled Kafka predecessor") && actual == "0"
    ));
    assert!(source.reader_handle.is_none());
    assert!(source.reader_drain_tx.is_none());
    assert!(source.source_drain.is_none());
}

#[tokio::test]
async fn guaranteed_delivery_rejects_latest_before_activation() {
    let mut config = test_config();
    config.startup_mode = StartupMode::Latest;
    let mut source = KafkaSource::new(test_schema(), config, None);

    let error = source
        .start(
            SourceStart::new(
                ConnectorConfig::new("kafka"),
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("a moving latest cut can skip records after recovery");
    assert!(matches!(
        error,
        ConnectorError::ConfigurationError(message)
            if message.contains("stable unrecorded-partition start")
    ));
    assert_eq!(source.state(), ConnectorState::Created);
    assert!(source.consumer.is_none());
}

#[tokio::test]
async fn guaranteed_delivery_rejects_broker_group_offsets_before_activation() {
    let mut source = KafkaSource::new(test_schema(), test_config(), None);

    let error = source
        .start(
            SourceStart::new(
                ConnectorConfig::new("kafka"),
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("a mutable broker group cursor cannot define a guaranteed initial cut");
    assert!(matches!(
        error,
        ConnectorError::ConfigurationError(message)
            if message.contains("cannot use broker group offsets")
    ));
    assert_eq!(source.state(), ConnectorState::Created);
    assert!(source.consumer.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guaranteed_local_start_modes_install_manual_assignment_without_subscription() {
    let cluster = MockCluster::new(1).expect("create in-process Kafka mock cluster");
    cluster
        .create_topic("events", 2, 1)
        .expect("create explicit topic inventory");

    let starts = [
        StartupMode::Earliest,
        StartupMode::SpecificOffsets(std::collections::HashMap::from([(0, 0), (1, 0)])),
        StartupMode::Timestamp(1_700_000_000_000),
    ];
    for (start_index, startup_mode) in starts.into_iter().enumerate() {
        let mut config = test_config();
        config.bootstrap_servers = cluster.bootstrap_servers();
        config.group_id = format!("manual-assignment-{start_index}");
        config.startup_mode = startup_mode.clone();
        let mut source = KafkaSource::new(test_schema(), config, None);
        let mut request_config = ConnectorConfig::new("kafka");
        request_config.set("bootstrap.servers", cluster.bootstrap_servers());
        request_config.set("group.id", format!("manual-assignment-{start_index}"));
        request_config.set("topic", "events");
        request_config.set("laminar.source.name", "events-source");
        match &startup_mode {
            StartupMode::Earliest => request_config.set("startup.mode", "earliest"),
            StartupMode::SpecificOffsets(_) => {
                request_config.set("startup.specific.offsets", "0:0,1:0");
            }
            StartupMode::Timestamp(timestamp) => {
                request_config.set("startup.timestamp.ms", timestamp.to_string());
            }
            _ => unreachable!("test covers engine-owned start modes only"),
        }

        source
            .start(
                SourceStart::new(
                    request_config.clone(),
                    SourcePosition::Initial,
                    DeliveryGuarantee::AtLeastOnce,
                )
                .unwrap(),
            )
            .await
            .expect("supported guaranteed start must activate against explicit inventory");

        let consumer = source.consumer.as_ref().expect("active consumer");
        assert_eq!(
            consumer.subscription().expect("read subscription").count(),
            0,
            "guaranteed mode must not join broker-managed subscription"
        );
        let assignment = consumer.assignment().expect("read manual assignment");
        let mut partitions: Vec<_> = assignment
            .elements()
            .iter()
            .map(|element| (element.topic().to_string(), element.partition()))
            .collect();
        partitions.sort_unstable();
        assert_eq!(
            partitions,
            vec![("events".to_string(), 0), ("events".to_string(), 1)]
        );

        if matches!(startup_mode, StartupMode::Timestamp(_)) {
            let checkpoint = source.capture_non_vnode_checkpoint().unwrap();
            assert_eq!(
                decode_partition_baselines(&checkpoint).unwrap(),
                KafkaPartitionBaselines::from([
                    (("events".to_string(), 0), 0),
                    (("events".to_string(), 1), 0),
                ])
            );

            let mut incomplete = checkpoint;
            let first_channel = incomplete.input_channels().unwrap()[0].clone();
            incomplete.set_input_channels(vec![first_channel]).unwrap();
            let mut resumed = KafkaSource::new(test_schema(), test_config(), None);
            let error = resumed
                .start(
                    SourceStart::new(
                        request_config,
                        SourcePosition::Resume {
                            attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(1),
                            checkpoint: incomplete,
                        },
                        DeliveryGuarantee::AtLeastOnce,
                    )
                    .unwrap(),
                )
                .await
                .expect_err("timestamp resume must reject a changed partition roster");
            assert!(error
                .to_string()
                .contains("input-channel inventory changed"));
            resumed.fail_startup();
        }

        source.close().await.expect("close mock consumer");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vnode_start_publishes_the_exact_active_kafka_assignment() {
    let cluster = MockCluster::new(1).expect("create in-process Kafka mock cluster");
    cluster
        .create_topic("events", 4, 1)
        .expect("create explicit topic inventory");

    let node1 = laminar_core::state::NodeId(1);
    let node2 = laminar_core::state::NodeId(2);
    let registry = Arc::new(laminar_core::state::VnodeRegistry::new(8));
    registry.set_assignment([node1, node2, node1, node2, node2, node1, node2, node1].into());
    let mut config = test_config();
    config.bootstrap_servers = cluster.bootstrap_servers();
    config.group_id = "vnode-start".into();
    config.startup_mode = StartupMode::Earliest;
    let mut source = KafkaSource::new(test_schema(), config, None);
    source
        .set_vnode_assignment("events_source", Arc::clone(&registry), node1)
        .unwrap();

    source
        .start(
            SourceStart::new(
                ConnectorConfig::new("kafka"),
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect("vnode assignment must activate");

    let active = kafka_partition_set(
        &source
            .consumer
            .as_ref()
            .expect("active consumer")
            .assignment()
            .expect("read active assignment"),
    )
    .unwrap();
    let expected: KafkaPartitionSet = crate::kafka::vnode_routing::owned_partitions_in_assignment(
        "events_source",
        "events",
        4,
        &registry.snapshot(),
        node1,
    )
    .unwrap()
    .into_iter()
    .map(|partition| ("events".to_string(), partition))
    .collect();
    assert_eq!(active, expected);

    let publication = Arc::clone(&lock_or_recover(&source.assignment_publication));
    assert_eq!(
        publication.assignment_version,
        registry.assignment_version()
    );
    assert_eq!(publication.owned_partitions.as_ref(), &expected);
    assert!(source.checkpoint_ready().unwrap());
    assert_eq!(
        source
            .try_checkpoint()
            .unwrap()
            .unwrap()
            .assignment_version(),
        NonZeroU64::new(registry.assignment_version())
    );

    source.close().await.expect("close mock consumer");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vnode_bootstrap_starts_empty_then_reconciles_durable_assignment() {
    let cluster = MockCluster::new(1).expect("create in-process Kafka mock cluster");
    cluster
        .create_topic("events", 4, 1)
        .expect("create explicit topic inventory");

    let node1 = laminar_core::state::NodeId(1);
    let node2 = laminar_core::state::NodeId(2);
    let registry = Arc::new(laminar_core::state::VnodeRegistry::new_unassigned(8));
    let mut config = test_config();
    config.bootstrap_servers = cluster.bootstrap_servers();
    config.group_id = "vnode-bootstrap".into();
    config.startup_mode = StartupMode::Earliest;
    let mut source = KafkaSource::new(test_schema(), config, None);
    source
        .set_vnode_assignment("events_source", Arc::clone(&registry), node1)
        .unwrap();

    source
        .start(
            SourceStart::new(
                ConnectorConfig::new("kafka"),
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect("canonical unassigned bootstrap must start fenced");

    let active = source
        .consumer
        .as_ref()
        .expect("active consumer")
        .assignment()
        .expect("read bootstrap assignment");
    assert_eq!(active.count(), 0);
    assert!(!source.checkpoint_ready().unwrap());
    assert!(source.try_checkpoint().unwrap().is_none());
    let publication = Arc::clone(&lock_or_recover(&source.assignment_publication));
    assert_eq!(publication.assignment_version, 0);
    assert!(publication.owned_partitions.is_empty());

    registry.set_assignment_and_version(
        [node1, node2, node1, node2, node2, node1, node2, node1].into(),
        1,
    );
    source.drive_control_plane();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !source.checkpoint_ready().unwrap() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "Kafka bootstrap assignment did not reconcile"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let expected: KafkaPartitionSet = crate::kafka::vnode_routing::owned_partitions_in_assignment(
        "events_source",
        "events",
        4,
        &registry.snapshot(),
        node1,
    )
    .unwrap()
    .into_iter()
    .map(|partition| ("events".to_string(), partition))
    .collect();
    let active = kafka_partition_set(
        &source
            .consumer
            .as_ref()
            .expect("active consumer")
            .assignment()
            .expect("read adopted assignment"),
    )
    .unwrap();
    assert_eq!(active, expected);
    assert_eq!(
        source
            .try_checkpoint()
            .unwrap()
            .expect("adopted assignment checkpoint")
            .assignment_version(),
        NonZeroU64::new(1)
    );

    source.close().await.expect("close mock consumer");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vnode_rotation_reconciles_only_the_verified_target_assignment() {
    let cluster = MockCluster::new(1).expect("create in-process Kafka mock cluster");
    cluster
        .create_topic("events", 4, 1)
        .expect("create explicit topic inventory");

    let node1 = laminar_core::state::NodeId(1);
    let node2 = laminar_core::state::NodeId(2);
    let registry = Arc::new(laminar_core::state::VnodeRegistry::single_owner(8, node1));
    let mut config = test_config();
    config.bootstrap_servers = cluster.bootstrap_servers();
    config.group_id = "vnode-rotation".into();
    config.startup_mode = StartupMode::Earliest;
    let mut source = KafkaSource::new(test_schema(), config, None);
    source
        .set_vnode_assignment("events_source", Arc::clone(&registry), node1)
        .unwrap();
    source
        .start(
            SourceStart::new(
                ConnectorConfig::new("kafka"),
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect("initial vnode assignment must activate");
    registry.set_assignment_and_version(
        [node1, node2, node1, node2, node2, node1, node2, node1].into(),
        2,
    );
    assert!(!source.checkpoint_ready().unwrap());
    source.drive_control_plane();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while !source.checkpoint_ready().unwrap() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "Kafka vnode rotation did not reconcile"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let expected: KafkaPartitionSet = crate::kafka::vnode_routing::owned_partitions_in_assignment(
        "events_source",
        "events",
        4,
        &registry.snapshot(),
        node1,
    )
    .unwrap()
    .into_iter()
    .map(|partition| ("events".to_string(), partition))
    .collect();
    let active = kafka_partition_set(
        &source
            .consumer
            .as_ref()
            .expect("active consumer")
            .assignment()
            .expect("read rotated assignment"),
    )
    .unwrap();
    assert_eq!(active, expected);
    let publication = Arc::clone(&lock_or_recover(&source.assignment_publication));
    assert_eq!(publication.assignment_version, 2);
    assert_eq!(publication.owned_partitions.as_ref(), &expected);
    assert_eq!(
        source
            .try_checkpoint()
            .unwrap()
            .unwrap()
            .assignment_version(),
        NonZeroU64::new(2)
    );

    source.close().await.expect("close mock consumer");
}

#[test]
fn acquired_position_uses_checkpoint_offset_then_baseline() {
    let map =
        |off: &str| std::collections::HashMap::from([("events:0".to_string(), off.to_string())]);
    let offsets = OffsetTracker::try_from_offset_map(&map("100")).unwrap();
    let empty = OffsetTracker::new();
    let baseline = KafkaPartitionBaselines::from([(("events".to_string(), 0), 101)]);

    assert_eq!(
        acquired_numeric_position(&offsets, &baseline, "events", 0).unwrap(),
        Some(101)
    );
    assert_eq!(
        acquired_numeric_position(&empty, &baseline, "events", 0).unwrap(),
        Some(101)
    );
    assert_eq!(
        acquired_numeric_position(&empty, &KafkaPartitionBaselines::new(), "events", 0).unwrap(),
        None
    );
}

#[test]
fn vnode_payload_filter_rejects_revoked_and_pre_cut_records() {
    let node1 = laminar_core::state::NodeId(1);
    let node2 = laminar_core::state::NodeId(2);
    let assignment = [node1, node2];
    let owned_partition = (0..100)
        .find(|partition| {
            crate::kafka::vnode_routing::partition_vnode("events_source", "events", *partition, 2)
                .unwrap()
                == 0
        })
        .unwrap();
    let revoked_partition = (0..100)
        .find(|partition| {
            crate::kafka::vnode_routing::partition_vnode("events_source", "events", *partition, 2)
                .unwrap()
                == 1
        })
        .unwrap();

    assert!(vnode_payload_is_current(
        Some((&assignment, node1)),
        Some(
            crate::kafka::vnode_routing::partition_vnode(
                "events_source",
                "events",
                owned_partition,
                2,
            )
            .unwrap(),
        ),
        Some(10),
        10,
    )
    .unwrap());
    assert!(!vnode_payload_is_current(
        Some((&assignment, node1)),
        Some(
            crate::kafka::vnode_routing::partition_vnode(
                "events_source",
                "events",
                owned_partition,
                2,
            )
            .unwrap(),
        ),
        Some(10),
        9,
    )
    .unwrap());
    let error = vnode_payload_is_current(Some((&assignment, node1)), Some(2), None, 10)
        .expect_err("a stale route table must fault instead of dropping a consumed payload");
    assert!(error.to_string().contains("outside owner map cardinality"));
    assert!(!vnode_payload_is_current(
        Some((&assignment, node1)),
        Some(
            crate::kafka::vnode_routing::partition_vnode(
                "events_source",
                "events",
                revoked_partition,
                2,
            )
            .unwrap(),
        ),
        None,
        10,
    )
    .unwrap());
}

#[test]
fn cached_payload_route_rejects_partition_inventory_drift() {
    assert_eq!(cached_partition_vnode(None, 9).unwrap(), None);
    assert_eq!(cached_partition_vnode(Some(&[7, 11]), 1).unwrap(), Some(11));
    assert!(cached_partition_vnode(Some(&[7, 11]), -1)
        .unwrap_err()
        .to_string()
        .contains("negative partition"));
    assert!(cached_partition_vnode(Some(&[7, 11]), 2)
        .unwrap_err()
        .to_string()
        .contains("outside the activated topic inventory"));
}

#[test]
fn owner_generation_detects_self_other_self_before_reader_turn() {
    let self_id = laminar_core::state::NodeId(1);
    let other = laminar_core::state::NodeId(2);
    let registry = laminar_core::state::VnodeRegistry::new_unassigned(1);
    registry.set_assignment_and_version(vec![self_id].into(), 1);
    registry.set_assignment_and_version(vec![other].into(), 2);
    registry.set_assignment_and_version(vec![self_id].into(), 3);

    let published = registry.versioned_snapshot();
    let routes = kafka_partition_routes("events_source", 1, &[(Arc::from("events"), 1)]).unwrap();
    let (owned, reacquired) = kafka_owned_partition_sets(&routes, &published, self_id, 1).unwrap();
    assert_eq!(owned, KafkaPartitionSet::from([("events".to_string(), 0)]));
    assert_eq!(reacquired, owned);
    let (_, reacquired) = kafka_owned_partition_sets(&routes, &published, self_id, 3).unwrap();
    assert!(reacquired.is_empty());
}

#[test]
fn vnode_reconciliation_rejects_noncanonical_unassigned_maps() {
    let self_id = laminar_core::state::NodeId(1);
    let registry = laminar_core::state::VnodeRegistry::new_unassigned(2);
    assert!(kafka_bootstrap_is_unassigned(&registry.versioned_snapshot(), self_id).unwrap());

    registry
        .set_assignment_and_version([self_id, laminar_core::state::NodeId::UNASSIGNED].into(), 1);
    let published = registry.versioned_snapshot();
    let error = kafka_bootstrap_is_unassigned(&published, self_id).unwrap_err();
    assert!(error.to_string().contains("unassigned owner at vnode 1"));
    let routes = kafka_partition_routes("events_source", 2, &[(Arc::from("events"), 2)]).unwrap();
    let error = kafka_owned_partition_sets(&routes, &published, self_id, 0).unwrap_err();
    assert!(error.to_string().contains("unassigned owner at vnode 1"));
}

#[test]
fn cached_routes_define_exact_multi_topic_assignment() {
    let node1 = laminar_core::state::NodeId(1);
    let node2 = laminar_core::state::NodeId(2);
    let registry = laminar_core::state::VnodeRegistry::new(8);
    registry.set_assignment([node1, node2, node2, node1, node1, node2, node1, node2].into());
    let topics = [(Arc::from("events"), 7), (Arc::from("orders"), 5)];
    let routes = kafka_partition_routes("source", 8, &topics).unwrap();
    let published = registry.versioned_snapshot();
    let (owned, reacquired) = kafka_owned_partition_sets(&routes, &published, node1, 0).unwrap();
    assert!(reacquired.is_empty());

    let expected: KafkaPartitionSet = topics
        .iter()
        .flat_map(|(topic, count)| {
            crate::kafka::vnode_routing::owned_partitions_in_assignment(
                "source",
                topic,
                *count,
                published.owners(),
                node1,
            )
            .unwrap()
            .into_iter()
            .map(|partition| (topic.to_string(), partition))
        })
        .collect();
    assert_eq!(owned, expected);
}

#[test]
fn assignment_fence_rejects_zero_and_rotated_versions() {
    let node = laminar_core::state::NodeId(1);
    let registry = laminar_core::state::VnodeRegistry::new_unassigned(1);
    let reconciled = AtomicU64::new(0);
    assert!(!kafka_assignment_fence_matches(&registry, &reconciled, 0));

    registry.set_assignment_and_version([node].into(), 1);
    reconciled.store(1, Ordering::Release);
    assert!(kafka_assignment_fence_matches(&registry, &reconciled, 1));
    let publication = Mutex::new(Arc::new(assignment_publication(
        "source",
        1,
        Arc::new(KafkaPartitionSet::new()),
        KafkaRotationBaselines::new(),
    )));
    assert_eq!(
        try_capture_at_assignment_fence(&registry, &reconciled, &publication, |_| Ok(7)).unwrap(),
        Some(7)
    );

    let raced = try_capture_at_assignment_fence(&registry, &reconciled, &publication, |_| {
        registry.set_assignment_and_version([node].into(), 2);
        Ok(7)
    })
    .unwrap();
    assert_eq!(raced, None);

    assert!(!kafka_assignment_fence_matches(&registry, &reconciled, 1));
    assert!(!kafka_assignment_fence_matches(&registry, &reconciled, 2));
    reconciled.store(2, Ordering::Release);
    assert!(kafka_assignment_fence_matches(&registry, &reconciled, 2));
}

#[test]
fn assignment_mismatch_diagnostics_are_stable() {
    let expected = KafkaPartitionSet::from([
        ("b".to_string(), 1),
        ("a".to_string(), 2),
        ("a".to_string(), 1),
    ]);
    assert!(validate_kafka_assignment(&expected, &expected).is_ok());

    let actual = KafkaPartitionSet::from([("b".to_string(), 1), ("c".to_string(), 3)]);
    let error = validate_kafka_assignment(&expected, &actual).unwrap_err();
    assert!(error.contains("first missing: a-1"));
    assert!(error.contains("first unexpected: c-3"));

    let missing_only = KafkaPartitionSet::from([("a".to_string(), 2), ("b".to_string(), 1)]);
    let error = validate_kafka_assignment(&expected, &missing_only).unwrap_err();
    assert!(error.contains("first missing: a-1"));
    assert!(error.contains("first unexpected: none"));

    let mut unexpected_only = expected.clone();
    unexpected_only.insert(("c".to_string(), 3));
    let error = validate_kafka_assignment(&expected, &unexpected_only).unwrap_err();
    assert!(error.contains("first missing: none"));
    assert!(error.contains("first unexpected: c-3"));
}

#[test]
fn assignment_list_rejects_noncanonical_partitions() {
    let mut negative = TopicPartitionList::new();
    negative.add_partition("events", -1);
    assert!(kafka_partition_set(&negative)
        .unwrap_err()
        .contains("negative partition"));

    let mut duplicate = TopicPartitionList::new();
    duplicate.add_partition("events", 0);
    duplicate.add_partition("events", 0);
    assert!(kafka_partition_set(&duplicate)
        .unwrap_err()
        .contains("duplicate partition"));
}

#[test]
fn test_schema_returned() {
    let schema = test_schema();
    let source = KafkaSource::new(schema.clone(), test_config(), None);
    assert_eq!(source.schema(), schema);
}

#[test]
fn test_checkpoint_empty() {
    let source = KafkaSource::new(test_schema(), test_config(), None);
    let cp = source.checkpoint();
    assert!(cp.is_empty());
}

#[test]
fn test_checkpoint_with_offsets() {
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source.source_name = Arc::from("events-source");
    source.offsets.update("events", 0, 100);
    source.offsets.update("events", 1, 200);

    // Simulate rebalance assign so partitions are in the assigned set.
    {
        let mut state = source.rebalance_state.lock().unwrap();
        state.on_assign(&[("events".into(), 0), ("events".into(), 1)]);
    }

    let cp = source.checkpoint();
    assert_eq!(cp.get_offset("events:0"), Some("100"));
    assert_eq!(cp.get_offset("events:1"), Some("200"));
}

#[test]
fn test_checkpoint_vnode_assigned_uses_owned_partitions() {
    // Vnode mode uses manual assign(), which fires no rebalance callbacks,
    // so rebalance_state stays empty. checkpoint() must filter by owned
    // partitions instead — otherwise a cluster Kafka source records zero
    // offsets and replays from the start on recovery.
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source.offsets.update("events", 0, 100);
    source.offsets.update("events", 1, 200);
    source.offsets.update("events", 2, 300);
    source.offsets.update("events", 3, 400);

    // Four vnodes split across two valid cluster node identities.
    let registry = Arc::new(laminar_core::state::VnodeRegistry::new(4));
    let node1 = laminar_core::state::NodeId(1);
    let node2 = laminar_core::state::NodeId(2);
    registry.set_assignment(vec![node1, node2, node1, node2].into());

    source.source_name = Arc::from("events_source");
    source.vnode_assignment = Some((Arc::clone(&registry), node1));
    source.manual_partition_baselines = KafkaPartitionBaselines::from([
        (("events".to_string(), 0), 10),
        (("events".to_string(), 1), 20),
        (("events".to_string(), 2), 30),
        (("events".to_string(), 3), 40),
    ]);
    let version = registry.assignment_version();
    source
        .reconciled_assignment_version
        .store(version, Ordering::Release);
    let owned: KafkaPartitionSet = crate::kafka::vnode_routing::owned_partitions_in_assignment(
        "events_source",
        "events",
        4,
        &registry.snapshot(),
        node1,
    )
    .unwrap()
    .into_iter()
    .map(|partition| ("events".to_string(), partition))
    .collect();
    *lock_or_recover(&source.assignment_publication) = Arc::new(assignment_publication(
        source.source_name.as_ref(),
        version,
        Arc::new(owned.clone()),
        KafkaRotationBaselines::new(),
    ));

    // rebalance_state is empty (no callbacks under manual assign): the old
    // code returned an empty checkpoint here.
    let cp = source.checkpoint();
    for partition in 0..4 {
        let expected =
            owned
                .contains(&("events".to_string(), partition))
                .then(|| match partition {
                    0 => "100",
                    1 => "200",
                    2 => "300",
                    3 => "400",
                    _ => unreachable!(),
                });
        assert_eq!(cp.get_offset(&format!("events:{partition}")), expected);
    }
    assert_eq!(
        decode_partition_baselines(&cp).unwrap(),
        owned
            .into_iter()
            .map(|(topic, partition)| {
                let next = i64::from(partition + 1) * 10;
                ((topic, partition), next)
            })
            .collect::<KafkaPartitionBaselines>()
    );
}

#[test]
fn vnode_checkpoint_keeps_rotation_baseline_authoritative_until_accept() {
    let node1 = laminar_core::state::NodeId(1);
    let registry = Arc::new(laminar_core::state::VnodeRegistry::single_owner(1, node1));
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source.source_name = Arc::from("events_source");
    source.vnode_assignment = Some((Arc::clone(&registry), node1));
    source.offsets.update_force("events", 0, 250); // stale prior stint
    source.manual_partition_baselines =
        KafkaPartitionBaselines::from([(("events".to_string(), 0), 5)]);
    let rotation = KafkaRotationBaselines::from([(
        Arc::from("events"),
        std::collections::HashMap::from([(0, 101)]),
    )]);
    *lock_or_recover(&source.assignment_publication) = Arc::new(assignment_publication(
        source.source_name.as_ref(),
        1,
        Arc::new(KafkaPartitionSet::from([("events".to_string(), 0)])),
        rotation,
    ));
    source
        .rotation_partition_baseline_count
        .store(1, Ordering::Release);
    source
        .reconciled_assignment_version
        .store(registry.assignment_version(), Ordering::Release);

    let checkpoint = source.checkpoint();
    assert_eq!(checkpoint.get_offset("events:0"), None);
    assert_eq!(
        decode_partition_baselines(&checkpoint).unwrap(),
        KafkaPartitionBaselines::from([(("events".to_string(), 0), 101)])
    );

    let pinned = Arc::clone(&lock_or_recover(&source.assignment_publication));
    {
        let mut published = lock_or_recover(&source.assignment_publication);
        let publication = Arc::make_mut(&mut published);
        retire_accepted_rotation_baselines(
            &mut publication.baselines,
            &[(Arc::from("events"), 0, 100)],
        );
        assert_eq!(
            rotation_partition_baseline(&publication.baselines, "events", 0),
            Some(101)
        );
        retire_accepted_rotation_baselines(
            &mut publication.baselines,
            &[(Arc::from("events"), 0, 101)],
        );
        assert!(publication.baselines.is_empty());
    }
    let current = Arc::clone(&lock_or_recover(&source.assignment_publication));
    assert!(Arc::ptr_eq(
        &pinned.owned_partitions,
        &current.owned_partitions
    ));
    assert_eq!(
        rotation_partition_baseline(&pinned.baselines, "events", 0),
        Some(101)
    );
}

#[test]
fn assignment_flip_fences_checkpoint_until_exact_version_reconciles() {
    let node1 = laminar_core::state::NodeId(1);
    let registry = Arc::new(laminar_core::state::VnodeRegistry::single_owner(1, node1));
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source.source_name = Arc::from("events-source");
    source.vnode_assignment = Some((Arc::clone(&registry), node1));
    source.offsets.update("events", 0, 100);
    source
        .reconciled_assignment_version
        .store(registry.assignment_version(), Ordering::Release);
    let owned = Arc::new(KafkaPartitionSet::from([("events".to_string(), 0)]));
    *lock_or_recover(&source.assignment_publication) = Arc::new(assignment_publication(
        source.source_name.as_ref(),
        registry.assignment_version(),
        Arc::clone(&owned),
        KafkaRotationBaselines::new(),
    ));
    assert!(source.checkpoint_ready().unwrap());
    let checkpoint = source.try_checkpoint().unwrap().unwrap();
    assert_eq!(checkpoint.get_offset("events:0"), Some("100"));
    let positions =
        kafka_row_positions("events-source", &[(Arc::from("events"), 0, 100)], None).unwrap();
    assert_eq!(
        checkpoint.input_channels(),
        Some([positions.partition().value(0).to_vec()].as_slice())
    );

    registry.set_assignment_and_version([laminar_core::state::NodeId(2)].into(), 2);

    assert!(
        !source.checkpoint_ready().unwrap(),
        "the runtime must not poll or consume a barrier against unreconciled ownership"
    );
    assert!(source.try_checkpoint().unwrap().is_none());
    source
        .reconciled_assignment_version
        .store(2, Ordering::Release);
    assert!(source.checkpoint_ready().unwrap());
    assert!(
        source.try_checkpoint().unwrap().is_none(),
        "cursor publication must match even after the consumer version advances"
    );
    *lock_or_recover(&source.assignment_publication) = Arc::new(assignment_publication(
        source.source_name.as_ref(),
        2,
        Arc::new(KafkaPartitionSet::new()),
        KafkaRotationBaselines::new(),
    ));
    let checkpoint = source.try_checkpoint().unwrap().unwrap();
    assert_eq!(checkpoint.assignment_version(), NonZeroU64::new(2));
    assert_eq!(checkpoint.get_offset("events:0"), None);
    assert_eq!(checkpoint.input_channels(), Some([].as_slice()));
}

#[test]
fn boot_unassigned_vnode_source_is_not_checkpointable() {
    let registry = Arc::new(laminar_core::state::VnodeRegistry::new_unassigned(1));
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source.vnode_assignment = Some((registry, laminar_core::state::NodeId(1)));

    assert!(!source.checkpoint_ready().unwrap());
    assert!(source.try_checkpoint().unwrap().is_none());
}

#[test]
fn manual_checkpoint_captures_input_channels_before_first_record() {
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source.source_name = Arc::from("events-source");
    install_manual_partitions(
        &mut source,
        KafkaPartitionSet::from([("events".to_string(), 0), ("events".to_string(), 1)]),
    );
    source.manual_partition_baselines = KafkaPartitionBaselines::from([
        (("events".to_string(), 0), 5),
        (("events".to_string(), 1), 9),
    ]);

    let checkpoint = source.checkpoint();
    assert_eq!(checkpoint.get_offset("events:0"), None);
    assert_eq!(checkpoint.get_offset("events:1"), None);
    validate_resume_input_channels(
        source.source_name.as_ref(),
        checkpoint.input_channels(),
        &source.manual_topic_partitions,
    )
    .unwrap();
    let positions = kafka_row_positions(
        "events-source",
        &[(Arc::from("events"), 0, 0), (Arc::from("events"), 1, 0)],
        None,
    )
    .unwrap();
    assert_eq!(
        checkpoint.input_channels(),
        Some(
            [
                positions.partition().value(0).to_vec(),
                positions.partition().value(1).to_vec(),
            ]
            .as_slice()
        )
    );
    assert_eq!(
        decode_partition_baselines(&checkpoint).unwrap(),
        source.manual_partition_baselines
    );

    let mut expanded = source.manual_topic_partitions.clone();
    expanded.insert(("events".to_string(), 2));
    let error = validate_resume_input_channels(
        source.source_name.as_ref(),
        checkpoint.input_channels(),
        &expanded,
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("input-channel inventory changed"));
}

#[test]
fn group_checkpoint_uses_the_row_position_channel_identity() {
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source.source_name = Arc::from("events-source");
    lock_or_recover(&source.rebalance_state)
        .on_assign(&[("events".to_string(), 0), ("events".to_string(), 1)]);

    let checkpoint = source.capture_non_vnode_checkpoint().unwrap();
    let positions = kafka_row_positions(
        "events-source",
        &[(Arc::from("events"), 0, 0), (Arc::from("events"), 1, 0)],
        None,
    )
    .unwrap();
    assert_eq!(
        checkpoint.input_channels(),
        Some(
            [
                positions.partition().value(0).to_vec(),
                positions.partition().value(1).to_vec(),
            ]
            .as_slice()
        )
    );
}

#[test]
fn build_vnode_assignment_uses_checkpoint_offset_then_baseline() {
    let node1 = laminar_core::state::NodeId(1);
    let registry = laminar_core::state::VnodeRegistry::single_owner(4, node1);
    let topic_meta = vec![(Arc::from("events"), 4)];

    let mut offsets = OffsetTracker::new();
    offsets.update_force("events", 0, 100);
    offsets.update_force("events", 2, 200);
    let baselines = KafkaPartitionBaselines::from([
        (("events".to_string(), 1), 51),
        (("events".to_string(), 2), 999),
    ]);
    let tpl = build_vnode_assignment_tpl(
        "events_source",
        &registry.snapshot(),
        node1,
        &topic_meta,
        &offsets,
        &baselines,
        rdkafka::Offset::Beginning,
    )
    .unwrap();

    let offset_of = |p: i32| tpl.find_partition("events", p).map(|e| e.offset());
    assert_eq!(offset_of(0), Some(rdkafka::Offset::Offset(101)));
    assert_eq!(offset_of(1), Some(rdkafka::Offset::Offset(51)));
    assert_eq!(offset_of(2), Some(rdkafka::Offset::Offset(201)));
    assert_eq!(offset_of(3), Some(rdkafka::Offset::Beginning));
}

/// Broker group offsets survive engine rewinds, so a guaranteed initial
/// position must never resolve to `Stored`.
#[test]
fn deterministic_initial_never_uses_stored_offsets() {
    assert_eq!(
        deterministic_initial_offset(&StartupMode::GroupOffsets, OffsetReset::Earliest),
        Some(rdkafka::Offset::Beginning)
    );
    assert_eq!(
        deterministic_initial_offset(&StartupMode::GroupOffsets, OffsetReset::Latest),
        Some(rdkafka::Offset::End)
    );
    assert_eq!(
        deterministic_initial_offset(&StartupMode::Latest, OffsetReset::Earliest),
        Some(rdkafka::Offset::End)
    );
    assert_eq!(
        deterministic_initial_offset(&StartupMode::Earliest, OffsetReset::Latest),
        Some(rdkafka::Offset::Beginning)
    );
    assert_eq!(
        deterministic_initial_offset(&StartupMode::GroupOffsets, OffsetReset::None),
        None
    );
}

#[test]
fn empty_resume_checkpoint_is_valid() {
    let mut checkpoint = SourceCheckpoint::new();
    checkpoint.set_metadata("connector", "kafka");
    checkpoint.set_metadata(KAFKA_CHECKPOINT_VERSION_KEY, KAFKA_CHECKPOINT_VERSION);
    let offsets =
        OffsetTracker::try_from_checkpoint(&checkpoint).expect("empty resume is a valid cut");
    assert_eq!(offsets.partition_count(), 0);
}

#[test]
fn resume_checkpoint_decode_is_strict() {
    for (key, value) in [
        ("missing_separator", "1"),
        (":0", "1"),
        ("events:x", "1"),
        ("events:-1", "1"),
        ("events:00", "1"),
        ("events:0", "not-an-offset"),
        ("events:0", "-1"),
        ("events:0", "9223372036854775807"),
        ("invalid:topic:0", "1"),
    ] {
        let mut checkpoint = SourceCheckpoint::with_offsets(std::collections::HashMap::from([(
            key.to_string(),
            value.to_string(),
        )]));
        checkpoint.set_metadata("connector", "kafka");
        checkpoint.set_metadata(KAFKA_CHECKPOINT_VERSION_KEY, KAFKA_CHECKPOINT_VERSION);
        assert!(
            OffsetTracker::try_from_checkpoint(&checkpoint).is_err(),
            "malformed checkpoint entry {key}={value} must fail closed"
        );
    }
}

#[test]
fn guaranteed_assignment_positions_unrecorded_partitions_explicitly() {
    let mut offsets = OffsetTracker::new();
    offsets.update_force("events", 0, 100);
    let assigned = vec![("events".to_string(), 0), ("events".to_string(), 1)];
    let baselines = KafkaPartitionBaselines::from([(("events".to_string(), 1), 17)]);
    let tpl = assignment_seek_tpl(&offsets, &assigned, Some(&baselines), None, true)
        .expect("valid seek list");

    assert_eq!(
        tpl.find_partition("events", 0).map(|e| e.offset()),
        Some(rdkafka::Offset::Offset(101))
    );
    assert_eq!(
        tpl.find_partition("events", 1).map(|e| e.offset()),
        Some(rdkafka::Offset::Offset(17))
    );
}

#[test]
fn guaranteed_resume_rejects_positions_expired_by_retention() {
    let inventory = KafkaPartitionSet::from([("events".to_string(), 0)]);
    let baselines = KafkaPartitionBaselines::from([(("events".to_string(), 0), 5)]);
    let low_watermarks = KafkaPartitionBaselines::from([(("events".to_string(), 0), 6)]);

    let error = validate_positions_not_expired(
        &OffsetTracker::new(),
        &baselines,
        &low_watermarks,
        &inventory,
    )
    .unwrap_err();
    assert!(error.to_string().contains("retention advanced"));

    let mut consumed = OffsetTracker::new();
    consumed.update_force("events", 0, 10);
    let low_watermarks = KafkaPartitionBaselines::from([(("events".to_string(), 0), 12)]);
    let error = validate_positions_not_expired(&consumed, &baselines, &low_watermarks, &inventory)
        .unwrap_err();
    assert!(error.to_string().contains("position 11"));
}

#[test]
fn reader_error_classification_uses_a_positive_transient_allowlist() {
    use rdkafka::types::RDKafkaErrorCode;

    for error in [
        KafkaError::PartitionEOF(0),
        KafkaError::NoMessageReceived,
        KafkaError::MessageConsumption(RDKafkaErrorCode::BrokerTransportFailure),
        KafkaError::MessageConsumption(RDKafkaErrorCode::CoordinatorNotAvailable),
        KafkaError::MessageConsumption(RDKafkaErrorCode::RebalanceInProgress),
        KafkaError::Global(RDKafkaErrorCode::AllBrokersDown),
    ] {
        assert!(kafka_reader_error_is_transient(&error), "{error:?}");
    }

    for code in [
        RDKafkaErrorCode::Authentication,
        RDKafkaErrorCode::SaslAuthenticationFailed,
        RDKafkaErrorCode::TopicAuthorizationFailed,
        RDKafkaErrorCode::GroupAuthorizationFailed,
        RDKafkaErrorCode::ClusterAuthorizationFailed,
        RDKafkaErrorCode::InvalidTopic,
        RDKafkaErrorCode::InvalidGroupId,
        RDKafkaErrorCode::InvalidConfig,
        RDKafkaErrorCode::UnsupportedSASLMechanism,
        RDKafkaErrorCode::OffsetOutOfRange,
        RDKafkaErrorCode::AutoOffsetReset,
        RDKafkaErrorCode::LogTruncation,
        RDKafkaErrorCode::Unknown,
    ] {
        let error = KafkaError::MessageConsumption(code);
        assert!(!kafka_reader_error_is_transient(&error), "{error:?}");
    }

    assert!(!kafka_reader_error_is_transient(
        &KafkaError::MessageConsumptionFatal(RDKafkaErrorCode::BrokerTransportFailure)
    ));
    assert!(!kafka_reader_error_is_transient(
        &KafkaError::ClientCreation("invalid local configuration".into())
    ));
    assert!(!kafka_reader_error_is_transient(&KafkaError::Subscription(
        "invalid topic subscription".into()
    )));
}

#[test]
fn local_consumer_creation_failure_is_terminal_configuration() {
    let error = consumer_creation_error(&KafkaError::ClientCreation(
        "invalid local configuration".into(),
    ));
    assert!(matches!(error, ConnectorError::ConfigurationError(_)));
    assert!(!error.is_transient());
}

#[test]
fn test_deserializer_selection_json() {
    let source = KafkaSource::new(test_schema(), test_config(), None);
    assert_eq!(source.deserializer.format(), Format::Json);
}

#[test]
fn test_deserializer_selection_csv() {
    let mut cfg = test_config();
    cfg.format = Format::Csv;
    let source = KafkaSource::new(test_schema(), cfg, None);
    assert_eq!(source.deserializer.format(), Format::Csv);
}

#[test]
fn test_with_schema_registry() {
    let sr = SchemaRegistryClient::new("http://localhost:8081", None).unwrap();
    let mut cfg = test_config();
    cfg.format = Format::Avro;
    cfg.schema_registry_url = Some("http://localhost:8081".into());

    let source = KafkaSource::with_schema_registry(test_schema(), cfg, sr);
    assert!(source.schema_registry.is_some());
    assert_eq!(source.deserializer.format(), Format::Avro);
}

#[tokio::test]
async fn test_start_preserves_injected_schema_registry() {
    let sr = SchemaRegistryClient::new("http://localhost:8081", None).unwrap();
    let mut cfg = test_config();
    cfg.format = Format::Avro;
    cfg.schema_registry_url = Some("http://localhost:8081".into());
    let mut source = KafkaSource::with_schema_registry(test_schema(), cfg, sr);

    // start() with empty config should preserve injected SR.
    let empty_config = crate::config::ConnectorConfig::new("kafka");
    // start() will fail to connect (no broker), but the deserializer
    // re-selection happens before the connection attempt.
    let _ = source
        .start(
            SourceStart::new(
                empty_config,
                SourcePosition::Initial,
                DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await;
    assert!(source.schema_registry.is_some());
    assert_eq!(source.deserializer.format(), Format::Avro);
}

#[test]
fn test_debug_output() {
    let source = KafkaSource::new(test_schema(), test_config(), None);
    let debug = format!("{source:?}");
    assert!(debug.contains("KafkaSource"));
    assert!(debug.contains("events"));
}

#[test]
fn test_checkpoint_filters_revoked_partitions() {
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source.source_name = Arc::from("events-source");
    source.offsets.update("events", 0, 100);
    source.offsets.update("events", 1, 200);
    source.offsets.update("events", 2, 300);

    // Simulate rebalance: only partitions 0 and 2 are assigned.
    {
        let mut state = source.rebalance_state.lock().unwrap();
        state.on_assign(&[("events".into(), 0), ("events".into(), 2)]);
    }

    let cp = source.checkpoint();
    assert_eq!(cp.get_offset("events:0"), Some("100"));
    assert_eq!(cp.get_offset("events:1"), None); // revoked — filtered out
    assert_eq!(cp.get_offset("events:2"), Some("300"));
}

#[test]
fn test_checkpoint_empty_before_first_rebalance() {
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source.offsets.update("events", 0, 100);
    source.offsets.update("events", 1, 200);

    // No rebalance has occurred — assigned_partitions is empty.
    // No assigned partitions means no offsets should be checkpointed.
    let cp = source.checkpoint();
    assert!(cp.is_empty());
}

// discover_schema tests. Control-flow cases (when to skip, when to
// fail) use plain config inputs; the happy path uses a wiremock HTTP
// server mocking Confluent Schema Registry's REST API.

fn empty_schema() -> SchemaRef {
    Arc::new(Schema::empty())
}

fn props(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[tokio::test]
async fn discover_schema_skips_non_avro_format() {
    let mut source = KafkaSource::new(empty_schema(), KafkaSourceConfig::default(), None);
    source
        .discover_schema(&props(&[
            ("bootstrap.servers", "localhost:9092"),
            ("group.id", "g"),
            ("topic", "t"),
            ("format", "json"),
            ("schema.registry.url", "http://localhost:8081"),
        ]))
        .await
        .expect("non-avro format is a legitimate skip");
    assert_eq!(source.schema().fields().len(), 0);
}

#[tokio::test]
async fn discover_schema_errors_on_avro_without_sr_url() {
    let mut source = KafkaSource::new(empty_schema(), KafkaSourceConfig::default(), None);
    let err = source
        .discover_schema(&props(&[
            ("bootstrap.servers", "localhost:9092"),
            ("group.id", "g"),
            ("topic", "t"),
            ("format", "avro"),
        ]))
        .await
        .expect_err("avro without schema.registry.url must surface a configuration error");
    let msg = err.to_string();
    assert!(
        msg.contains("schema.registry.url"),
        "error must name the missing key, got: {msg}"
    );
    assert_eq!(source.schema().fields().len(), 0);
}

#[tokio::test]
async fn discover_schema_errors_on_topic_pattern() {
    let mut source = KafkaSource::new(empty_schema(), KafkaSourceConfig::default(), None);
    let err = source
        .discover_schema(&props(&[
            ("bootstrap.servers", "localhost:9092"),
            ("group.id", "g"),
            ("topic.pattern", "events-.*"),
            ("format", "avro"),
            ("schema.registry.url", "http://localhost:8081"),
        ]))
        .await
        .expect_err("topic.pattern + avro must surface a configuration error");
    let msg = err.to_string();
    assert!(
        msg.contains("topic.pattern"),
        "error must name the offending key, got: {msg}"
    );
    assert_eq!(source.schema().fields().len(), 0);
}

#[tokio::test]
async fn discover_schema_errors_on_sr_unreachable() {
    let mut source = KafkaSource::new(empty_schema(), KafkaSourceConfig::default(), None);
    let start = std::time::Instant::now();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        source.discover_schema(&props(&[
            ("bootstrap.servers", "localhost:9092"),
            ("group.id", "g"),
            ("topic", "t"),
            ("format", "avro"),
            ("schema.registry.url", "http://192.0.2.1:65535"),
        ])),
    )
    .await
    .expect("discover_schema must honor its own 10s timeout");
    assert!(
        start.elapsed() < std::time::Duration::from_secs(15),
        "discover_schema should have returned well before the outer 20s budget"
    );
    let err = result.expect_err("unreachable SR must surface as Err");
    assert!(
        matches!(
            err,
            ConnectorError::ConnectionFailed(_) | ConnectorError::Timeout(_)
        ),
        "expected ConnectionFailed or Timeout, got: {err:?}"
    );
    assert_eq!(source.schema().fields().len(), 0);
}

#[tokio::test]
async fn discover_schema_preserves_terminal_registry_classification() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let sr = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/subjects/orders-value/versions/latest"))
        .respond_with(ResponseTemplate::new(401).set_body_string("invalid credentials"))
        .mount(&sr)
        .await;

    let mut source = KafkaSource::new(empty_schema(), KafkaSourceConfig::default(), None);
    let error = source
        .discover_schema(&props(&[
            ("bootstrap.servers", "localhost:9092"),
            ("group.id", "g"),
            ("topic", "orders"),
            ("format", "avro"),
            ("schema.registry.url", &sr.uri()),
        ]))
        .await
        .unwrap_err();

    assert!(matches!(error, ConnectorError::ConfigurationError(_)));
    assert!(!error.is_transient());
    assert!(error.to_string().contains("orders-value"));
}

#[tokio::test]
async fn source_start_fails_closed_on_terminal_registry_prefetch_error() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let sr = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/subjects/orders-value/versions/latest"))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&sr)
        .await;

    let mut config = test_config();
    config.format = Format::Avro;
    config.subscription = TopicSubscription::Topics(vec!["orders".into()]);
    config.schema_registry_url = Some(sr.uri());
    let registry = SchemaRegistryClient::new(sr.uri(), None).unwrap();
    let mut source = KafkaSource::with_schema_registry(test_schema(), config, registry);

    let error = source
        .start(
            SourceStart::new(
                ConnectorConfig::new("kafka"),
                SourcePosition::Initial,
                DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, ConnectorError::ConfigurationError(_)));
    assert!(!error.is_transient());
    assert!(error.to_string().contains("orders-value"));
    assert_eq!(source.state(), ConnectorState::Failed);
    assert!(source.consumer.is_none());
    assert!(source.blocking_tasks.retired.load(Ordering::Acquire));
}

#[tokio::test]
async fn discover_schema_propagates_broker_commit_interval_rejection() {
    let mut source = KafkaSource::new(empty_schema(), KafkaSourceConfig::default(), None);
    let err = source
        .discover_schema(&props(&[
            ("bootstrap.servers", "localhost:9092"),
            ("group.id", "g"),
            ("topic", "t"),
            ("format", "avro"),
            ("schema.registry.url", "http://localhost:8081"),
            ("broker.commit.interval.ms", "5000"),
        ]))
        .await
        .expect_err("deprecated config key must produce a propagated error");
    let msg = err.to_string();
    assert!(
        msg.contains("broker.commit.interval.ms"),
        "error must name the offending key, got: {msg}"
    );
}

/// Happy path: wiremock SR returns a record-with-map Avro schema
/// (the original "No Field name data" bug shape); `discover_schema`
/// converts it correctly and preserves the Map type.
#[tokio::test]
async fn discover_schema_happy_path_with_wiremock_sr() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let avro_schema = serde_json::json!({
        "type": "record",
        "name": "event",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "data", "type": {"type": "map", "values": "string"}}
        ]
    })
    .to_string();

    let sr = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/subjects/ion_tw-value/versions/latest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 42,
            "version": 1,
            "subject": "ion_tw-value",
            "schema": avro_schema,
            "schemaType": "AVRO",
        })))
        .mount(&sr)
        .await;

    let mut source = KafkaSource::new(empty_schema(), KafkaSourceConfig::default(), None);
    source
        .discover_schema(&props(&[
            ("bootstrap.servers", "localhost:9092"),
            ("group.id", "g"),
            ("topic", "ion_tw"),
            ("format", "avro"),
            ("schema.registry.url", &sr.uri()),
        ]))
        .await
        .expect("happy-path discovery must succeed");

    let schema = source.schema();
    assert_eq!(schema.fields().len(), 2, "expected [id, data]");
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "data");
    assert!(
        matches!(
            schema.field(1).data_type(),
            arrow_schema::DataType::Map(_, _)
        ),
        "'data' field must survive as a Map type (got {:?})",
        schema.field(1).data_type()
    );
}

/// Record-name subject strategy resolves to `{record_name}-value`
/// rather than the default `{topic}-value`.
#[tokio::test]
async fn discover_schema_happy_path_record_name_strategy() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let avro_schema = serde_json::json!({
        "type": "record",
        "name": "com.acme.Order",
        "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "amount", "type": "double"}
        ]
    })
    .to_string();

    let sr = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/subjects/com.acme.Order-value/versions/latest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 7,
            "version": 1,
            "subject": "com.acme.Order-value",
            "schema": avro_schema,
            "schemaType": "AVRO",
        })))
        .mount(&sr)
        .await;

    let mut source = KafkaSource::new(empty_schema(), KafkaSourceConfig::default(), None);
    source
        .discover_schema(&props(&[
            ("bootstrap.servers", "localhost:9092"),
            ("group.id", "g"),
            ("topic", "orders"),
            ("format", "avro"),
            ("schema.registry.url", &sr.uri()),
            ("schema.registry.subject.name.strategy", "record-name"),
            ("schema.registry.record.name", "com.acme.Order"),
        ]))
        .await
        .expect("happy-path discovery must succeed");

    let schema = source.schema();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "order_id");
    assert_eq!(schema.field(1).name(), "amount");
}

/// Drift detection: catalog has a stale 2-field schema, live SR
/// has evolved to 3 fields. Catalog stays pinned; only
/// `last_avro_schema` tracks the live SR shape.
#[tokio::test]
async fn start_logs_drift_when_sr_evolved_since_ddl() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let evolved_schema = serde_json::json!({
        "type": "record",
        "name": "event",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "data", "type": {"type": "map", "values": "string"}},
            {"name": "version", "type": "int"}
        ]
    })
    .to_string();

    let sr = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/subjects/ion_tw-value/versions/latest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 99,
            "version": 2,
            "subject": "ion_tw-value",
            "schema": evolved_schema,
            "schemaType": "AVRO",
        })))
        .mount(&sr)
        .await;

    // Catalog schema baked at CREATE SOURCE time — only two fields,
    // predates the `version` field that was just added in SR.
    let stale_catalog = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "data",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(arrow_schema::Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        ),
    ]));

    let mut cfg = KafkaSourceConfig::default();
    cfg.bootstrap_servers = "localhost:9092".into();
    cfg.group_id = "g".into();
    cfg.subscription = TopicSubscription::Topics(vec!["ion_tw".into()]);
    cfg.format = Format::Avro;
    cfg.schema_registry_url = Some(sr.uri());
    let sr_client = SchemaRegistryClient::new(sr.uri(), None).unwrap();
    let mut source = KafkaSource::with_schema_registry(stale_catalog, cfg, sr_client);

    let empty_cfg = crate::config::ConnectorConfig::new("kafka");
    let _ = source
        .start(
            SourceStart::new(
                empty_cfg,
                SourcePosition::Initial,
                DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await; // broker unreachable — later errors irrelevant

    assert_eq!(
        source.schema().fields().len(),
        2,
        "catalog schema must stay pinned even after SR drift"
    );
    assert_eq!(
        source.last_avro_schema.as_ref().map(|s| s.fields().len()),
        Some(3),
        "last_avro_schema should reflect the evolved SR shape"
    );
}

// The hook builds a TPL from a durable SourceCheckpoint and uses
// offset+1 (next-to-fetch) per Kafka convention. We exercise the
// translation directly via OffsetTracker, since the hook delegates
// to it.
#[test]
fn test_checkpoint_to_tpl_uses_next_offset() {
    let mut offsets = std::collections::HashMap::new();
    offsets.insert("events:0".to_string(), "100".to_string());
    offsets.insert("events:1".to_string(), "200".to_string());
    let mut cp = SourceCheckpoint::with_offsets(offsets);
    cp.set_metadata("connector", "kafka");
    cp.set_metadata(KAFKA_CHECKPOINT_VERSION_KEY, KAFKA_CHECKPOINT_VERSION);
    let tpl = OffsetTracker::try_from_checkpoint(&cp)
        .unwrap()
        .to_topic_partition_list();
    assert_eq!(tpl.count(), 2);
    for elem in tpl.elements() {
        let expected = match elem.partition() {
            0 => rdkafka::Offset::Offset(101),
            1 => rdkafka::Offset::Offset(201),
            p => panic!("unexpected partition {p}"),
        };
        assert_eq!(elem.offset(), expected);
    }
}

#[tokio::test]
async fn test_notify_epoch_committed_empty_cp_is_noop() {
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    // No active consumer — the empty-checkpoint opt-out path must not fail.
    source
        .notify_epoch_committed(1, &SourceCheckpoint::new())
        .await
        .unwrap();
}

fn durable_kafka_checkpoint() -> SourceCheckpoint {
    let mut checkpoint = SourceCheckpoint::new();
    checkpoint.set_offset("events:0", "100");
    checkpoint.set_metadata("connector", "kafka");
    checkpoint.set_metadata(KAFKA_CHECKPOINT_VERSION_KEY, KAFKA_CHECKPOINT_VERSION);
    checkpoint
}

#[tokio::test]
async fn notify_epoch_committed_treats_missing_consumer_as_advisory_failure() {
    let mut source = KafkaSource::new(test_schema(), test_config(), None);
    source
        .notify_epoch_committed(7, &durable_kafka_checkpoint())
        .await
        .unwrap();
    assert_eq!(source.metrics.commit_failures.get(), 1);
}
