use super::super::{
    ConnectorConfig, DeliveryGuarantee, KafkaSourceConfig, SourceConnector, SourcePosition,
    SourceStart, TopicPartitionList,
};
use super::*;
use arrow_schema::{DataType, Field, Schema};
use prometheus::{Encoder, TextEncoder};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
use rdkafka::{ClientConfig, Message};

fn scrape(registry: &Registry) -> String {
    let mut output = Vec::new();
    TextEncoder::new()
        .encode(&registry.gather(), &mut output)
        .unwrap();
    String::from_utf8(output).unwrap()
}

fn partitions(ids: &[i32]) -> KafkaPartitionSet {
    ids.iter().map(|id| ("events".to_owned(), *id)).collect()
}

async fn mock_source(brokers: &str) -> (KafkaSource, FutureProducer) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut source = KafkaSource::new(schema, KafkaSourceConfig::default(), None);
    let mut config = ConnectorConfig::new("kafka");
    config.set("bootstrap.servers", brokers);
    config.set("group.id", "progress-tests");
    config.set("topic", "events");
    config.set("startup.mode", "earliest");
    config.set("laminar.source.name", "input");
    source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let producer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "3000")
        .create::<FutureProducer>()
        .unwrap();
    for partition in [0, 1] {
        produce(&producer, partition, 1).await;
    }
    let consumer = source.consumer.as_ref().unwrap();
    let mut received_partitions = HashSet::new();
    for _ in 0..2 {
        let message = tokio::time::timeout(Duration::from_secs(5), consumer.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.offset(), 0);
        received_partitions.insert(message.partition());
    }
    assert_eq!(received_partitions, HashSet::from([0, 1]));
    (source, producer)
}

async fn produce(producer: &FutureProducer, partition: i32, count: usize) {
    for _ in 0..count {
        producer
            .send(
                FutureRecord::to("events")
                    .partition(partition)
                    .key("key")
                    .payload(r#"{"id":1}"#),
                Duration::from_secs(3),
            )
            .await
            .unwrap();
    }
}

fn sampler(source: &KafkaSource, progress: &KafkaProgress) -> ProgressSampler {
    ProgressSampler {
        consumer: Arc::clone(source.consumer.as_ref().unwrap()),
        blocking_tasks: source.blocking_tasks.clone(),
        metrics: progress.metrics.clone(),
        revoke_generation: Arc::clone(&source.revoke_generation),
        assign_generation: Arc::clone(&source.assign_generation),
        assignment_version: Arc::clone(&source.reconciled_assignment_version),
        published: HashSet::new(),
    }
}

async fn wait_for_native_lookup(tasks: &KafkaBlockingTasks) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while tasks.is_idle().await {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("sampler must start a tracked native lookup");
}

fn run_sampler(
    source: &KafkaSource,
    progress: &mut KafkaProgress,
    sampler: ProgressSampler,
    period: Duration,
) {
    let guard = source.task_owner.track().unwrap();
    let (shutdown, receiver) = tokio::sync::oneshot::channel();
    progress.shutdown = Some(shutdown);
    progress.task = Some(tokio::spawn(async move {
        let _guard = guard;
        sampler.run(receiver, period).await;
    }));
}

#[test]
fn numbered_sources_share_registry_without_collector_identity_collisions() {
    let registry = Registry::new();
    let mut sources = Vec::new();
    for index in 0..4 {
        let progress = KafkaProgress::register(&registry, &format!("input_{index}")).unwrap();
        progress.metrics.last_batch.set(f64::from(index));
        sources.push(progress);
    }
    let rendered = scrape(&registry);
    for index in 0..4 {
        assert!(rendered.contains(&format!(
            "kafka_source_last_batch_timestamp_seconds{{source=\"input_{index}\"}} {index}"
        )));
    }
    drop(sources.remove(1));
    assert!(!scrape(&registry).contains("source=\"input_1\""));
    let replacement = KafkaProgress::register(&registry, "input_1").unwrap();
    replacement.metrics.last_batch.set(10.0);
    assert!(scrape(&registry)
        .contains("kafka_source_last_batch_timestamp_seconds{source=\"input_1\"} 10"));
    drop(replacement);
    drop(sources);
    assert!(registry.gather().is_empty());
}

#[test]
fn registration_conflicts_roll_back_only_this_attempt() {
    for conflict_index in 0..4 {
        let registry = Registry::new();
        let existing = ProgressMetrics::new("input");
        existing
            .reader_lag
            .with_label_values(&["events", "0"])
            .set(3);
        existing
            .available
            .with_label_values(&["events", "0"])
            .set(1);
        existing
            .sampled_at
            .with_label_values(&["events", "0"])
            .set(10.0);
        existing.last_batch.set(20.0);
        registry
            .register(
                existing
                    .collectors()
                    .into_iter()
                    .nth(conflict_index)
                    .unwrap(),
            )
            .unwrap();
        let unaffected = KafkaProgress::register(&registry, "unaffected").unwrap();
        let before = scrape(&registry);

        assert!(KafkaProgress::register(&registry, "input").is_err());
        assert_eq!(scrape(&registry), before);
        registry
            .unregister(
                existing
                    .collectors()
                    .into_iter()
                    .nth(conflict_index)
                    .unwrap(),
            )
            .unwrap();
        let replacement = KafkaProgress::register(&registry, "input").unwrap();
        drop(replacement);
        assert!(scrape(&registry).contains("source=\"unaffected\""));
        assert!(!scrape(&registry).contains("source=\"input\""));
        drop(unaffected);
        assert!(registry.gather().is_empty());
    }
}

#[test]
fn named_sources_share_registry_and_restart_without_stale_collectors() {
    let registry = Registry::new();
    let first = KafkaProgress::register(&registry, "first").unwrap();
    let second = KafkaProgress::register(&registry, "second").unwrap();
    first.metrics.last_batch.set(10.0);
    second.metrics.last_batch.set(20.0);
    let rendered = scrape(&registry);
    assert!(rendered.contains("kafka_source_last_batch_timestamp_seconds{source=\"first\"} 10"));
    assert!(rendered.contains("kafka_source_last_batch_timestamp_seconds{source=\"second\"} 20"));
    assert!(KafkaProgress::register(&registry, "first").is_err());

    let old_worker = first.metrics.clone();
    drop(first);
    let replacement = KafkaProgress::register(&registry, "first").unwrap();
    replacement.metrics.last_batch.set(30.0);
    old_worker.last_batch.set(999.0);
    drop(old_worker);
    let rendered = scrape(&registry);
    assert!(rendered.contains("kafka_source_last_batch_timestamp_seconds{source=\"first\"} 30"));
    assert!(!rendered.contains("999"));
    drop(replacement);
    assert!(!scrape(&registry).contains("source=\"first\""));
    assert!(scrape(&registry).contains("source=\"second\""));
}

#[test]
fn unknown_failed_and_racing_samples_are_not_zero_lag() {
    let registry = Registry::new();
    let progress = KafkaProgress::register(&registry, "input").unwrap();
    let assigned = partitions(&[0, 1, 2]);
    let highs = KafkaPartitionBaselines::from([
        (("events".to_owned(), 0), 10),
        (("events".to_owned(), 1), 10),
        (("events".to_owned(), 2), 10),
    ]);
    let positions = KafkaPartitionBaselines::from([
        (("events".to_owned(), 0), 10),
        (("events".to_owned(), 2), 11),
    ]);
    progress
        .metrics
        .publish(&assigned, Some(&highs), &positions, 100.0);
    assert_eq!(
        progress
            .metrics
            .available
            .with_label_values(&["events", "0"])
            .get(),
        1
    );
    assert_eq!(
        progress
            .metrics
            .reader_lag
            .with_label_values(&["events", "0"])
            .get(),
        0
    );
    assert_eq!(
        progress
            .metrics
            .available
            .with_label_values(&["events", "1"])
            .get(),
        0
    );
    assert_eq!(
        progress
            .metrics
            .available
            .with_label_values(&["events", "2"])
            .get(),
        0
    );
    let lag_family = registry
        .gather()
        .into_iter()
        .find(|family| family.name() == "kafka_source_reader_lag_offsets")
        .unwrap();
    assert_eq!(lag_family.get_metric().len(), 1);

    progress.metrics.publish(&assigned, None, &positions, 200.0);
    assert_eq!(
        progress
            .metrics
            .available
            .with_label_values(&["events", "0"])
            .get(),
        0
    );
    assert_eq!(
        progress
            .metrics
            .sampled_at
            .with_label_values(&["events", "0"])
            .get(),
        100.0
    );
    assert!(!registry
        .gather()
        .iter()
        .any(|family| family.name() == "kafka_source_reader_lag_offsets"));
}

#[test]
fn revoked_partition_series_are_removed_and_freshness_requires_nonempty_batch() {
    let registry = Registry::new();
    let progress = KafkaProgress::register(&registry, "input").unwrap();
    let mut published = partitions(&[0, 1]);
    for partition in ["0", "1"] {
        progress
            .metrics
            .reader_lag
            .with_label_values(&["events", partition])
            .set(3);
        progress
            .metrics
            .available
            .with_label_values(&["events", partition])
            .set(1);
        progress
            .metrics
            .sampled_at
            .with_label_values(&["events", partition])
            .set(100.0);
    }
    progress
        .metrics
        .reconcile(&mut published, &partitions(&[1]));
    assert!(!scrape(&registry).contains("partition=\"0\""));
    assert!(scrape(&registry).contains("partition=\"1\""));
    assert_eq!(published, partitions(&[1]));
    progress.record_batch(0);
    assert_eq!(progress.metrics.last_batch.get(), 0.0);
    progress.record_batch(1);
    let timestamp = progress.metrics.last_batch.get();
    assert!(timestamp > 0.0);
    progress.record_batch(0);
    assert_eq!(progress.metrics.last_batch.get(), timestamp);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paused_reader_reports_new_broker_backlog_without_polling() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("events", 2, 1).unwrap();
    let (mut source, producer) = mock_source(&cluster.bootstrap_servers()).await;
    let registry = Registry::new();
    let mut progress = KafkaProgress::register(&registry, "input").unwrap();
    let mut sampler = sampler(&source, &progress);
    let consumer = source.consumer.as_ref().unwrap();
    consumer.pause(&consumer.assignment().unwrap()).unwrap();
    sampler.sample().await;
    let labels = &["events", "0"];
    assert_eq!(
        progress.metrics.available.with_label_values(labels).get(),
        1
    );
    assert_eq!(
        progress.metrics.reader_lag.with_label_values(labels).get(),
        0
    );
    let previous_sample = progress.metrics.sampled_at.with_label_values(labels).get();

    run_sampler(&source, &mut progress, sampler, SAMPLE_INTERVAL);
    produce(&producer, 0, 3).await;
    tokio::time::timeout(SAMPLE_INTERVAL * 2, async {
        while progress.metrics.reader_lag.with_label_values(labels).get() != 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background sampling must observe writes while the reader stays paused");
    assert!(progress.metrics.sampled_at.with_label_values(labels).get() > previous_sample);
    assert_eq!(
        consumer
            .position()
            .unwrap()
            .find_partition("events", 0)
            .unwrap()
            .offset(),
        Offset::Offset(1)
    );
    assert_eq!(progress.metrics.last_batch.get(), 0.0);
    progress
        .close(tokio::time::Instant::now() + Duration::from_millis(500))
        .await;
    assert!(progress.task.is_none());
    drop(progress);
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_flight_samples_are_rejected_at_each_assignment_fence() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("events", 2, 1).unwrap();
    let (mut source, _producer) = mock_source(&cluster.bootstrap_servers()).await;
    let registry = Registry::new();
    let progress = KafkaProgress::register(&registry, "input").unwrap();
    let mut sampler = sampler(&source, &progress);
    let generations = [
        Arc::clone(&source.revoke_generation),
        Arc::clone(&source.assign_generation),
        Arc::clone(&source.reconciled_assignment_version),
    ];
    for generation in generations {
        sampler.sample().await;
        assert_eq!(
            progress
                .metrics
                .available
                .with_label_values(&["events", "0"])
                .get(),
            1
        );
        let sampled_at = progress
            .metrics
            .sampled_at
            .with_label_values(&["events", "0"])
            .get();
        cluster
            .broker_round_trip_time(1, Duration::from_millis(100))
            .unwrap();
        let task = tokio::spawn(async move {
            sampler.sample().await;
            sampler
        });
        wait_for_native_lookup(&source.blocking_tasks).await;
        generation.fetch_add(1, Ordering::Release);
        sampler = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            progress
                .metrics
                .available
                .with_label_values(&["events", "0"])
                .get(),
            0
        );
        assert_eq!(
            progress
                .metrics
                .sampled_at
                .with_label_values(&["events", "0"])
                .get(),
            sampled_at
        );
        assert!(!registry
            .gather()
            .iter()
            .any(|family| family.name() == "kafka_source_reader_lag_offsets"));
        cluster.broker_round_trip_time(1, Duration::ZERO).unwrap();
    }
    drop(sampler);
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_assignment_and_revoke_remove_obsolete_partition_series() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("events", 2, 1).unwrap();
    let (mut source, _producer) = mock_source(&cluster.bootstrap_servers()).await;
    let registry = Registry::new();
    let progress = KafkaProgress::register(&registry, "input").unwrap();
    let mut sampler = sampler(&source, &progress);
    sampler.sample().await;
    assert!(scrape(&registry).contains("partition=\"0\""));

    let consumer = source.consumer.as_ref().unwrap();
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset("events", 1, Offset::Offset(1))
        .unwrap();
    consumer.assign(&assignment).unwrap();
    source
        .reconciled_assignment_version
        .fetch_add(1, Ordering::Release);
    sampler.sample().await;
    assert!(!scrape(&registry).contains("partition=\"0\""));
    assert!(scrape(&registry).contains("partition=\"1\""));

    consumer.unassign().unwrap();
    source.revoke_generation.fetch_add(1, Ordering::Release);
    sampler.sample().await;
    assert!(!scrape(&registry).contains("partition="));
    drop(sampler);
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_lookup_is_unavailable_and_in_flight_shutdown_is_bounded() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("events", 2, 1).unwrap();
    let (mut source, _producer) = mock_source(&cluster.bootstrap_servers()).await;
    let registry = Registry::new();
    let mut progress = KafkaProgress::register(&registry, "input").unwrap();
    let mut sampler = sampler(&source, &progress);
    sampler.sample().await;
    let labels = &["events", "0"];
    let sampled_at = progress.metrics.sampled_at.with_label_values(labels).get();
    cluster.request_errors(
        RDKafkaApiKey::ListOffsets,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED; 16],
    );
    tokio::time::timeout(Duration::from_secs(3), sampler.sample())
        .await
        .unwrap();
    assert_eq!(
        progress.metrics.available.with_label_values(labels).get(),
        0
    );
    assert_eq!(
        progress.metrics.sampled_at.with_label_values(labels).get(),
        sampled_at
    );
    assert!(!registry
        .gather()
        .iter()
        .any(|family| family.name() == "kafka_source_reader_lag_offsets"));

    cluster.clear_request_errors(RDKafkaApiKey::ListOffsets);
    cluster
        .broker_round_trip_time(1, Duration::from_secs(2))
        .unwrap();
    run_sampler(&source, &mut progress, sampler, Duration::from_millis(20));
    wait_for_native_lookup(&source.blocking_tasks).await;
    tokio::time::timeout(
        Duration::from_secs(1),
        progress.close(tokio::time::Instant::now() + Duration::from_millis(100)),
    )
    .await
    .expect("sampler close must not wait for native network timeout");
    assert!(progress.task.is_none());
    drop(progress);
    assert!(!scrape(&registry).contains("kafka_source_"));
    let terminal = source.terminal_task_tracker().unwrap();
    tokio::time::timeout(Duration::from_secs(2), source.close())
        .await
        .unwrap()
        .unwrap();
    drop(source);
    tokio::time::timeout(Duration::from_secs(12), terminal.wait_terminated())
        .await
        .expect("retired native work must eventually release its generation");
}

#[tokio::test]
async fn failed_consumer_creation_releases_progress_registration() {
    let registry = Registry::new();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut source = KafkaSource::new(schema, KafkaSourceConfig::default(), Some(&registry));
    let mut config = ConnectorConfig::new("kafka");
    config.set("bootstrap.servers", "localhost:9092");
    config.set("group.id", "progress-failed-startup");
    config.set("topic", "events");
    config.set("laminar.source.name", "failed-input");
    config.set("kafka.laminar.invalid.setting", "true");
    let error = source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("laminar.invalid.setting"),
        "{error}"
    );
    assert_eq!(source.state, super::super::ConnectorState::Failed);
    assert!(source.progress.is_none());
    assert!(source.consumer.is_none());
    let replacement = KafkaProgress::register(&registry, "failed-input").unwrap();
    drop(replacement);
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn named_source_start_and_close_own_progress_registration_and_task() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("events", 1, 1).unwrap();
    let registry = Registry::new();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut source = KafkaSource::new(schema, KafkaSourceConfig::default(), Some(&registry));
    let mut config = ConnectorConfig::new("kafka");
    config.set("bootstrap.servers", cluster.bootstrap_servers());
    config.set("group.id", "progress-startup");
    config.set("topic", "events");
    config.set("startup.mode", "earliest");
    config.set("laminar.source.name", "named-input");
    source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let progress = source
        .progress
        .as_ref()
        .expect("named source registers progress");
    assert!(!progress
        .task
        .as_ref()
        .expect("startup launches sampler")
        .is_finished());
    tokio::time::timeout(Duration::from_secs(2), async {
        while !scrape(&registry).contains("kafka_source_lag_sample_available{partition=\"0\",source=\"named-input\",topic=\"events\"}") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("startup sampler must publish assigned partition availability");

    source.close().await.unwrap();
    assert!(source.progress.is_none());
    assert!(!scrape(&registry).contains("source=\"named-input\""));
}
