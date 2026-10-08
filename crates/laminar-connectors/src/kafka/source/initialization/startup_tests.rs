//! Native in-process broker startup and the separate real-broker fixture.

use super::super::lock_or_recover;
use super::*;
use crate::connector::{DeliveryGuarantee, SourceConnector, SourcePosition, SourceStart};
use arrow_array::{Array, Int64Array};
use arrow_schema::{DataType, Field, Schema};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use std::sync::Arc;
use std::time::Duration;

fn request(brokers: &str, topic: &str, group: &str) -> ConnectorConfig {
    let mut request = ConnectorConfig::new("kafka");
    for (key, value) in [
        ("bootstrap.servers", brokers),
        ("topic", topic),
        ("group.id", group),
        ("startup.mode", "latest"),
        ("laminar.source.name", "added_source"),
    ] {
        request.set(key, value);
    }
    request
}

fn source() -> KafkaSource {
    KafkaSource::new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        KafkaSourceConfig::default(),
        None,
    )
}

fn producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .unwrap()
}

async fn append(producer: &FutureProducer, topic: &str, partition: i32, id: i64) {
    producer
        .send(
            FutureRecord::to(topic)
                .partition(partition)
                .key("key")
                .payload(&format!("{{\"id\":{id}}}")),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
}

async fn poll_ids(source: &mut KafkaSource, count: usize) -> Vec<i64> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut ids = Vec::new();
        while ids.len() < count {
            if let Some(batch) = source.poll_batch(16).await.unwrap() {
                let values = batch
                    .records
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(values.null_count(), 0);
                ids.extend(values.values().iter().copied());
            } else {
                tokio::task::yield_now().await;
            }
        }
        ids.sort_unstable();
        ids
    })
    .await
    .expect("sealed startup must consume the retained post-boundary input")
}

fn assert_unacknowledged(brokers: &str, topic: &str, group: &str) {
    let observer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .create()
        .unwrap();
    let mut requested = TopicPartitionList::new();
    for partition in 0..3 {
        requested.add_partition(topic, partition);
    }
    let committed = observer
        .committed_offsets(requested, Duration::from_secs(5))
        .unwrap();
    assert!(committed
        .elements()
        .iter()
        .all(|entry| entry.offset() == Offset::Invalid));
}

async fn startup_retry_oracle(brokers: &str, topic: &str, group: &str) {
    let producer = producer(brokers);
    for (partition, count) in [(0, 2), (1, 3)] {
        for id in 0..count {
            append(&producer, topic, partition, id).await;
        }
    }
    let config = request(brokers, topic, group);
    let sealed = source().resolve_initial_position(&config).await.unwrap();
    let expected = KafkaPartitionBaselines::from([
        ((topic.to_string(), 0), 2),
        ((topic.to_string(), 1), 3),
        ((topic.to_string(), 2), 0),
    ]);
    assert_eq!(
        super::super::checkpoint::decode_partition_baselines(&sealed).unwrap(),
        expected
    );
    assert_eq!(
        OffsetTracker::try_from_checkpoint(&sealed)
            .unwrap()
            .partition_count(),
        0
    );
    for (partition, id) in [(0, 100), (1, 101), (2, 102)] {
        append(&producer, topic, partition, id).await;
    }
    // Each replacement installs the same sealed boundary after the log's high watermarks moved.
    // There is no fabricated engine attempt or external acknowledgement, including on close.
    let mut processed_cursor = None;
    for _ in 0..2 {
        let mut source = source();
        source
            .start(
                SourceStart::new(
                    config.clone(),
                    SourcePosition::Initialized {
                        checkpoint: sealed.clone(),
                    },
                    DeliveryGuarantee::AtLeastOnce,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let consumer = source.consumer.as_ref().unwrap();
        assert_eq!(consumer.subscription().unwrap().count(), 0);
        let assignment = consumer.assignment().unwrap();
        for partition in 0..3 {
            assert_eq!(
                assignment
                    .find_partition(topic, partition)
                    .unwrap()
                    .offset(),
                Offset::Offset(expected[&(topic.to_string(), partition)])
            );
        }
        assert!(source.reader_handle.is_none());
        assert_eq!(source.offsets.partition_count(), 0);
        assert_eq!(source.manual_partition_baselines, expected);
        assert_eq!(poll_ids(&mut source, 3).await, vec![100, 101, 102]);
        processed_cursor = source.try_checkpoint().unwrap();
        source.close().await.unwrap();
        assert_unacknowledged(brokers, topic, group);
    }
    append(&producer, topic, 0, 103).await;
    resume_cursor_oracle(config, processed_cursor.unwrap()).await;
    assert_unacknowledged(brokers, topic, group);
    println!("sealed Kafka startup: next=[2,3,0], retries=2, post_boundary_ids=[100,101,102], subscribed=false, acknowledged=false");
}

async fn resume_cursor_oracle(config: ConnectorConfig, checkpoint: SourceCheckpoint) {
    let mut source = source();
    // Codec round trip through the existing resume request. This fixture supplies the attempt;
    // it does not claim that the cursor was durably committed by an engine checkpoint.
    source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Resume {
                    attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(7),
                    checkpoint,
                },
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(poll_ids(&mut source, 1).await, vec![103]);
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topology_start_kafka_native_retries_preserve_latest_vector_and_no_ack() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("sealed-events", 3, 1).unwrap();
    startup_retry_oracle(
        &cluster.bootstrap_servers(),
        "sealed-events",
        "sealed-group",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topology_start_kafka_current_vnode_owners_filter_one_global_sealed_vector() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("sealed-events", 3, 1).unwrap();
    let config = request(
        &cluster.bootstrap_servers(),
        "sealed-events",
        "sealed-vnodes",
    );
    let sealed = source().resolve_initial_position(&config).await.unwrap();
    let registry = Arc::new(laminar_core::state::VnodeRegistry::new(8));
    let nodes = [
        laminar_core::state::NodeId(1),
        laminar_core::state::NodeId(2),
    ];
    registry.set_assignment(
        [
            nodes[0], nodes[1], nodes[0], nodes[1], nodes[0], nodes[1], nodes[0], nodes[1],
        ]
        .into(),
    );
    let mut seen = KafkaPartitionSet::new();
    for node in nodes {
        let mut source = source();
        source
            .set_vnode_assignment("added_source", Arc::clone(&registry), node)
            .unwrap();
        source
            .start(
                SourceStart::new(
                    config.clone(),
                    SourcePosition::Initialized {
                        checkpoint: sealed.clone(),
                    },
                    DeliveryGuarantee::AtLeastOnce,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let publication = lock_or_recover(&source.assignment_publication).clone();
        assert_eq!(
            publication.assignment_version,
            registry.assignment_version()
        );
        let checkpoint = source.try_checkpoint().unwrap().unwrap();
        assert_eq!(
            checkpoint.assignment_version().unwrap().get(),
            registry.assignment_version()
        );
        assert_eq!(
            checkpoint.input_channels().unwrap(),
            publication.input_channels.as_ref()
        );
        assert_eq!(
            OffsetTracker::try_from_checkpoint(&checkpoint)
                .unwrap()
                .partition_count(),
            0
        );
        for partition in publication.owned_partitions.iter() {
            assert!(
                seen.insert(partition.clone()),
                "two active readers owned one partition"
            );
            assert_eq!(
                source
                    .consumer
                    .as_ref()
                    .unwrap()
                    .assignment()
                    .unwrap()
                    .find_partition(&partition.0, partition.1)
                    .unwrap()
                    .offset(),
                Offset::Offset(0)
            );
        }
        source.close().await.unwrap();
    }
    assert_eq!(seen.len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topology_start_kafka_rejects_changed_inventory_and_future_offsets_without_reader() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("sealed-events", 3, 1).unwrap();
    let config = request(
        &cluster.bootstrap_servers(),
        "sealed-events",
        "sealed-corrupt",
    );
    let sealed = source().resolve_initial_position(&config).await.unwrap();
    // A syntactically valid complete vector beyond the log must reject, never reset to latest.
    let mut future = sealed.clone();
    future.set_offset(
        super::super::checkpoint::partition_baseline_key("sealed-events", 0),
        "91",
    );
    let inventory =
        KafkaPartitionSet::from([("sealed-events".into(), 0), ("sealed-events".into(), 1)]);
    let mut changed = OffsetTracker::new()
        .to_checkpoint_for_partitions(inventory.iter().map(|(t, p)| (t.as_str(), *p)));
    attach_partition_baselines(
        &mut changed,
        &KafkaPartitionBaselines::from([
            (("sealed-events".into(), 0), 0),
            (("sealed-events".into(), 1), 0),
        ]),
        &inventory,
    );
    changed
        .set_input_channels(kafka_input_channels("added_source", &inventory).unwrap())
        .unwrap();
    for checkpoint in [future, changed] {
        let mut source = source();
        assert!(source
            .start(
                SourceStart::new(
                    config.clone(),
                    SourcePosition::Initialized { checkpoint },
                    DeliveryGuarantee::AtLeastOnce
                )
                .unwrap()
            )
            .await
            .is_err());
        assert_eq!(source.state(), ConnectorState::Created);
        assert!(source.consumer.is_none());
        assert!(source.reader_handle.is_none());
        assert!(source.blocking_tasks.is_idle().await);
    }
}

#[tokio::test]
async fn topology_start_kafka_rejects_processed_or_wrong_source_cursors_before_native_work() {
    let mut processed = super::tests::sealed_position();
    processed.set_offset("events:0", "90");
    let config = request("127.0.0.1:1", "events", "sealed-invalid");
    let mut wrong_source = config.clone();
    wrong_source.set("laminar.source.name", "other_source");
    for (config, checkpoint) in [
        (config, processed),
        (wrong_source, super::tests::sealed_position()),
    ] {
        let mut source = source();
        assert!(source
            .start(
                SourceStart::new(
                    config,
                    SourcePosition::Initialized { checkpoint },
                    DeliveryGuarantee::AtLeastOnce
                )
                .unwrap()
            )
            .await
            .is_err());
        assert_eq!(source.state(), ConnectorState::Created);
        assert!(source.consumer.is_none());
        assert!(source.reader_handle.is_none());
        assert!(source.blocking_tasks.is_idle().await);
    }
}

#[tokio::test(start_paused = true)]
async fn topology_start_kafka_shared_validation_deadline_leaves_no_active_reader() {
    let _occupied = INITIALIZATION_SLOT.acquire().await.unwrap();
    let mut source = source();
    let sealed = super::tests::sealed_position();
    assert!(matches!(
        source
            .start(
                SourceStart::new(
                    request("127.0.0.1:1", "events", "sealed-deadline"),
                    SourcePosition::Initialized { checkpoint: sealed },
                    DeliveryGuarantee::AtLeastOnce,
                )
                .unwrap()
            )
            .await,
        Err(ConnectorError::Timeout(10_000))
    ));
    assert_eq!(source.state(), ConnectorState::Created);
    assert!(source.consumer.is_none());
    assert!(source.reader_handle.is_none());
    assert!(source.blocking_tasks.is_idle().await);
}

#[tokio::test]
async fn topology_start_kafka_cancelled_validation_keeps_exact_request_reusable() {
    let occupied = INITIALIZATION_SLOT.acquire().await.unwrap();
    let mut source = source();
    let start = SourceStart::new(
        request("127.0.0.1:1", "events", "sealed-cancel"),
        SourcePosition::Initialized {
            checkpoint: super::tests::sealed_position(),
        },
        DeliveryGuarantee::AtLeastOnce,
    )
    .unwrap();
    {
        let mut work = Box::pin(source.start(start.clone()));
        std::future::poll_fn(|context| {
            assert!(std::future::Future::poll(work.as_mut(), context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }
    drop(occupied);
    assert_eq!(source.state(), ConnectorState::Created);
    assert!(source.consumer.is_none());
    assert!(source.reader_handle.is_none());
    assert!(source.blocking_tasks.is_idle().await);
    assert!(
        matches!(start.into_parts().1, SourcePosition::Initialized { checkpoint }
        if super::super::checkpoint::decode_partition_baselines(&checkpoint).unwrap()[&("events".into(),0)] == 91)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Kafka/Redpanda at LAMINAR_KAFKA_TEST_BROKERS (default 127.0.0.1:19092)"]
async fn topology_start_kafka_real_broker_sealed_latest_retries_without_history_or_ack() {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    let brokers =
        std::env::var("LAMINAR_KAFKA_TEST_BROKERS").unwrap_or_else(|_| "127.0.0.1:19092".into());
    let topic = format!("topology-start-{}", uuid::Uuid::new_v4());
    let group = format!("{topic}-group");
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();
    let options = AdminOptions::new().request_timeout(Some(Duration::from_secs(10)));
    assert!(admin
        .create_topics(
            [&NewTopic::new(&topic, 3, TopicReplication::Fixed(1))],
            &options
        )
        .await
        .unwrap()
        .into_iter()
        .all(|result| result.is_ok()));
    startup_retry_oracle(&brokers, &topic, &group).await;
    assert!(admin
        .delete_topics(&[&topic], &options)
        .await
        .unwrap()
        .into_iter()
        .all(|result| result.is_ok()));
}
