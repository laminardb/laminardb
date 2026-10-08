use super::*;
use crate::connector::SourceConnector;

fn config(mode: StartupMode) -> KafkaSourceConfig {
    KafkaSourceConfig {
        bootstrap_servers: "127.0.0.1:1".into(),
        group_id: "initialization-test".into(),
        subscription: TopicSubscription::Topics(vec!["events".into()]),
        startup_mode: mode,
        ..KafkaSourceConfig::default()
    }
}

pub(super) fn sealed_position() -> SourceCheckpoint {
    let inventory = KafkaPartitionSet::from([("events".into(), 0), ("events".into(), 1)]);
    let mut checkpoint = OffsetTracker::new().to_checkpoint_for_partitions(
        inventory
            .iter()
            .map(|(topic, partition)| (topic.as_str(), *partition)),
    );
    attach_partition_baselines(
        &mut checkpoint,
        &KafkaPartitionBaselines::from([(("events".into(), 0), 91), (("events".into(), 1), 0)]),
        &inventory,
    );
    checkpoint
        .set_input_channels(kafka_input_channels("added_source", &inventory).unwrap())
        .unwrap();
    checkpoint
}

#[test]
fn topology_restore_kafka_preserves_sealed_offsets_as_the_log_advances() {
    let checkpoint = sealed_position();
    let baselines =
        validate_sealed_position(&checkpoint, "added_source", &["events".into()]).unwrap();
    assert_eq!(baselines[&("events".into(), 0)], 91);
    assert_eq!(baselines[&("events".into(), 1)], 0);
    for (next, low, high) in [(91, 7, 123), (0, 0, 0), (91, 91, 123)] {
        validate_sealed_next_offset(next, low, high).unwrap();
    }
    for (next, low, high) in [(91, 92, 123), (91, 0, 90), (0, -1, 0), (0, 1, 0)] {
        assert!(validate_sealed_next_offset(next, low, high).is_err());
    }
}

#[tokio::test]
async fn topology_restore_kafka_rejects_malformed_or_owned_cursors_before_native_work() {
    let mut checkpoints = vec![sealed_position(); 5];
    checkpoints[0].bind_assignment_version(std::num::NonZeroU64::MIN);
    checkpoints[1].set_offset("events:0", "90");
    checkpoints[2].set_metadata("checkpoint.version", "1");
    checkpoints[3].set_input_channels(vec![vec![1]]).unwrap();
    checkpoints[4].set_offset(
        super::super::checkpoint::partition_baseline_key("events", 0),
        "-1",
    );
    let mut source = KafkaSource::new(
        std::sync::Arc::new(arrow_schema::Schema::empty()),
        config(StartupMode::Latest),
        None,
    );
    let mut request = ConnectorConfig::new("kafka");
    request.set("laminar.source.name", "added_source");
    request.set("bootstrap.servers", "127.0.0.1:1");
    request.set("group.id", "initialization-test");
    request.set("topic", "events");
    request.set("startup.mode", "latest");
    for checkpoint in checkpoints {
        assert!(validate_sealed_position(&checkpoint, "added_source", &["events".into()]).is_err());
        assert!(source
            .validate_initial_position(&request, &checkpoint)
            .await
            .is_err());
        assert_eq!(source.state(), ConnectorState::Created);
        assert!(source.consumer.is_none());
        assert!(source.reader_handle.is_none());
        assert!(source.blocking_tasks.is_idle().await);
    }
}

#[test]
fn topology_initialization_offsets_preserve_empty_and_never_read_partitions() {
    assert_eq!(initial_next_offset(&StartupMode::Latest, 0, 0).unwrap(), 0);
    assert_eq!(
        initial_next_offset(&StartupMode::Earliest, 7, 91).unwrap(),
        7
    );
    assert_eq!(
        initial_next_offset(&StartupMode::Latest, 7, 91).unwrap(),
        91
    );
    for (low, high) in [(-1, 3), (4, 3), (0, i64::MAX), (i64::MAX, i64::MAX)] {
        assert!(initial_next_offset(&StartupMode::Latest, low, high).is_err());
    }
    assert!(initial_next_offset(&StartupMode::GroupOffsets, 0, 3).is_err());
}

#[tokio::test]
async fn topology_initialization_rejects_uncertified_configurations_before_native_io() {
    for mode in [
        StartupMode::GroupOffsets,
        StartupMode::Timestamp(1),
        StartupMode::SpecificOffsets(std::collections::HashMap::from([(0, 1)])),
    ] {
        let mut source = KafkaSource::new(
            std::sync::Arc::new(arrow_schema::Schema::empty()),
            config(mode),
            None,
        );
        assert!(source
            .resolve_initial_position(&ConnectorConfig::new("kafka"))
            .await
            .is_err());
        assert_eq!(source.state(), ConnectorState::Created);
        assert!(source.consumer.is_none());
        assert!(source.reader_handle.is_none());
        assert!(source.blocking_tasks.is_idle().await);
    }
    let mut patterns = config(StartupMode::Latest);
    patterns.subscription = TopicSubscription::Pattern("events.*".into());
    assert!(validate_initialization_config(&patterns).is_err());
    patterns.subscription = TopicSubscription::Topics(vec!["events".into(), "events".into()]);
    assert!(validate_initialization_config(&patterns).is_err());
    patterns.subscription =
        TopicSubscription::Topics((0..65).map(|i| format!("topic-{i}")).collect());
    assert!(validate_initialization_config(&patterns).is_err());
    let mut source = KafkaSource::new(
        std::sync::Arc::new(arrow_schema::Schema::empty()),
        config(StartupMode::Latest),
        None,
    );
    assert!(source
        .resolve_initial_position(&ConnectorConfig::new("kafka"))
        .await
        .is_err());
    assert!(source.blocking_tasks.is_idle().await);
    assert!(source.consumer.is_none());
}

#[tokio::test]
#[ignore = "requires Kafka/Redpanda at LAMINAR_KAFKA_TEST_BROKERS (default 127.0.0.1:19092)"]
async fn topology_initialization_real_broker_reads_complete_cursors_without_consumption_or_ack() {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::{ClientConfig, Offset, TopicPartitionList};
    use std::time::{Duration, Instant};

    let brokers =
        std::env::var("LAMINAR_KAFKA_TEST_BROKERS").unwrap_or_else(|_| "127.0.0.1:19092".into());
    let topic = format!("topology-initialization-{}", uuid::Uuid::new_v4());
    let group = format!("{topic}-group");
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();
    let results = admin
        .create_topics(
            [&NewTopic::new(&topic, 3, TopicReplication::Fixed(1))],
            &AdminOptions::new().request_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .unwrap();
    assert!(results.into_iter().all(|r| r.is_ok()));
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .unwrap();
    for (partition, count) in [(0, 2), (1, 3)] {
        for id in 0..count {
            producer
                .send(
                    FutureRecord::to(&topic)
                        .partition(partition)
                        .key("key")
                        .payload(&format!("{id}")),
                    Duration::from_secs(5),
                )
                .await
                .unwrap();
        }
    }
    let mut request = ConnectorConfig::new("kafka");
    for (key, value) in [
        ("bootstrap.servers", brokers.as_str()),
        ("group.id", group.as_str()),
        ("topic", topic.as_str()),
        ("startup.mode", "latest"),
        ("laminar.source.name", "added_source"),
    ] {
        request.set(key, value);
    }
    let mut source = KafkaSource::new(
        std::sync::Arc::new(arrow_schema::Schema::empty()),
        config(StartupMode::Latest),
        None,
    );
    let started = Instant::now();
    let latest = source.resolve_initial_position(&request).await.unwrap();
    let elapsed = started.elapsed();
    let latest_baselines = super::super::checkpoint::decode_partition_baselines(&latest).unwrap();
    assert_eq!(
        latest_baselines,
        KafkaPartitionBaselines::from([
            ((topic.clone(), 0), 2),
            ((topic.clone(), 1), 3),
            ((topic.clone(), 2), 0),
        ])
    );
    assert_eq!(latest.input_channels().unwrap().len(), 3);
    assert_eq!(latest.assignment_version(), None);
    assert_eq!(
        OffsetTracker::try_from_checkpoint(&latest)
            .unwrap()
            .partition_count(),
        0
    );
    assert_eq!(source.state(), ConnectorState::Created);
    assert!(source.consumer.is_none());
    assert!(source.reader_handle.is_none());
    source
        .validate_initial_position(&request, &latest)
        .await
        .unwrap();
    // The log advances after sealing. Validation preserves the old vector instead of resolving
    // latest again, and never starts a reader or acknowledges the skipped prefix.
    producer
        .send(
            FutureRecord::to(&topic)
                .partition(0)
                .payload("later")
                .key("later"),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    source
        .validate_initial_position(&request, &latest)
        .await
        .unwrap();
    assert_eq!(
        super::super::checkpoint::decode_partition_baselines(&latest).unwrap(),
        latest_baselines
    );
    request.set("startup.mode", "earliest");
    let earliest = source.resolve_initial_position(&request).await.unwrap();
    assert!(
        super::super::checkpoint::decode_partition_baselines(&earliest)
            .unwrap()
            .values()
            .all(|p| *p == 0)
    );
    let observer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group)
        .create()
        .unwrap();
    let mut requested = TopicPartitionList::new();
    for partition in 0..3 {
        requested.add_partition(&topic, partition);
    }
    let committed = observer
        .committed_offsets(requested, Duration::from_secs(5))
        .unwrap();
    assert!(committed
        .elements()
        .iter()
        .all(|e| e.offset() == Offset::Invalid));
    println!("topology source cursor: latest_lookup_ms={:.3}, channels=3, next=[2,3,0], validated_after_append=true, reader_started=false, acknowledged=false", elapsed.as_secs_f64() * 1000.0);
    let results = admin
        .delete_topics(&[topic.as_str()], &AdminOptions::new())
        .await
        .unwrap();
    assert!(results.into_iter().all(|r| r.is_ok()));
}

#[tokio::test(start_paused = true)]
async fn topology_initialization_client_slot_wait_has_a_bounded_deadline_without_starting_work() {
    let _occupied = INITIALIZATION_SLOT.acquire().await.unwrap();
    let mut source = KafkaSource::new(
        std::sync::Arc::new(arrow_schema::Schema::empty()),
        config(StartupMode::Latest),
        None,
    );
    let mut request = ConnectorConfig::new("kafka");
    for (key, value) in [
        ("bootstrap.servers", "127.0.0.1:1"),
        ("group.id", "initialization-test"),
        ("topic", "events"),
        ("startup.mode", "latest"),
        ("laminar.source.name", "added_source"),
    ] {
        request.set(key, value);
    }
    assert!(matches!(
        source.resolve_initial_position(&request).await,
        Err(ConnectorError::Timeout(10_000))
    ));
    assert!(source.blocking_tasks.is_idle().await);
    assert!(source.consumer.is_none());
    assert!(source.reader_handle.is_none());
    assert_eq!(source.state(), ConnectorState::Created);
}
