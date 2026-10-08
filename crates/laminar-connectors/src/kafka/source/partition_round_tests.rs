use super::*;

use arrow_array::Int64Array;
use laminar_core::checkpoint::{AssignmentDrainId, CheckpointAttempt};
use laminar_core::state::{NodeId, VnodeRegistry};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

type PositionedRow = (i64, i64, Vec<u8>, Vec<u8>, u32);

fn config(brokers: &str) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("kafka");
    for (key, value) in [
        ("bootstrap.servers", brokers),
        ("group.id", "round-test"),
        ("topic", "events"),
        ("startup.mode", "earliest"),
        ("replay.order", "partition_rounds"),
        ("max.poll.records", "2"),
        ("reader.channel.capacity", "128"),
        ("laminar.source.name", "rounds"),
        ("queued.max.messages.kbytes", "1"),
    ] {
        config.set(key, value);
    }
    config
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}

async fn open(config: &ConnectorConfig, position: SourcePosition) -> KafkaSource {
    let parsed = KafkaSourceConfig::from_config(config).unwrap();
    let mut source = KafkaSource::new(schema(), parsed, None);
    source
        .start(SourceStart::new(config.clone(), position, DeliveryGuarantee::AtLeastOnce).unwrap())
        .await
        .unwrap();
    source
}

fn producer(
    cluster: &MockCluster<'_, rdkafka::producer::DefaultProducerContext>,
) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .create()
        .unwrap()
}

async fn send(producer: &FutureProducer, partition: i32, row: Option<(i64, i64)>) {
    let payload = row.map(|(id, ts)| format!("{{\"id\":{id},\"ts\":{ts}}}"));
    let record = FutureRecord::<str, str>::to("events")
        .partition(partition)
        .key("a");
    let record = match payload.as_deref() {
        Some(payload) => record.payload(payload),
        None => record,
    };
    producer.send(record, Duration::from_secs(5)).await.unwrap();
}

async fn next_round(source: &mut KafkaSource, poll_limit: usize) -> SourceBatch {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(batch) = source.poll_batch(poll_limit).await.unwrap() {
                return batch;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("complete Kafka round did not arrive")
}

async fn partial_round(source: &mut KafkaSource) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while source.poll_payloads.is_empty() {
            assert!(source.poll_batch(2).await.unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Kafka did not consume the first member of the incomplete round");
    assert_eq!(source.poll_payloads.len(), 1);
}

fn rows(batch: &SourceBatch) -> Vec<PositionedRow> {
    let ids = batch
        .records
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let timestamps = batch
        .records
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let positions = batch.row_positions().unwrap();
    (0..batch.records.num_rows())
        .map(|row| {
            (
                ids.value(row),
                timestamps.value(row),
                positions.partition().value(row).to_vec(),
                positions.order_key().value(row).to_vec(),
                positions.sub_offset().value(row),
            )
        })
        .collect()
}

#[test]
fn round_contract_requires_explicit_append_profile_and_binds_inventory() {
    let mut cfg = config("127.0.0.1:1");
    let source = KafkaSource::new(
        schema(),
        KafkaSourceConfig::from_config(&cfg).unwrap(),
        None,
    );
    assert_eq!(
        source.contract(&cfg).unwrap().replay_order,
        SourceReplayOrder::SingleChannelFixedBatches
    );
    for (key, invalid) in [
        ("format", "debezium"),
        ("topic.pattern", "events.*"),
        ("startup.mode", "group-offsets"),
        ("fetch.max.bytes", "0"),
        ("replay.order", "arrival_journal"),
    ] {
        let mut invalid_config = cfg.clone();
        invalid_config.set(key, invalid);
        assert!(
            KafkaSourceConfig::from_config(&invalid_config).is_err(),
            "{key}"
        );
    }
    cfg.set("replay.order", "unspecified");
    assert_eq!(
        source.contract(&cfg).unwrap().replay_order,
        SourceReplayOrder::Unspecified
    );
    let inventory = KafkaPartitionSet::from([("events".into(), 0), ("events".into(), 1)]);
    let channel =
        kafka_input_channels("rounds", &inventory, KafkaReplayOrder::PartitionRounds).unwrap();
    assert_eq!(channel.len(), 1);
    let mut expanded = inventory.clone();
    expanded.insert(("events".into(), 2));
    assert!(validate_resume_input_channels(
        "rounds",
        Some(&channel),
        &expanded,
        KafkaReplayOrder::PartitionRounds
    )
    .is_err());
    assert!(validate_resume_input_channels(
        "rounds",
        Some(&channel),
        &inventory,
        KafkaReplayOrder::Unspecified
    )
    .is_err());
}

#[tokio::test]
async fn rounds_reject_best_effort_before_creating_a_consumer() {
    let cfg = config("127.0.0.1:1");
    let mut source = KafkaSource::new(
        schema(),
        KafkaSourceConfig::from_config(&cfg).unwrap(),
        None,
    );
    let error = source
        .start(
            SourceStart::new(cfg, SourcePosition::Initial, DeliveryGuarantee::BestEffort).unwrap(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("guaranteed delivery"));
    assert!(source.consumer.is_none());
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rounds_hold_idle_cuts_and_replay_fixed_batches_after_partial_consumption() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("events", 2, 1).unwrap();
    let producer = producer(&cluster);
    let cfg = config(&cluster.bootstrap_servers());
    let mut source = open(&cfg, SourcePosition::Initial).await;
    // More future data than the native prefetch byte bound must not starve the idle partition.
    send(&producer, 0, None).await;
    for index in 0..100 {
        send(&producer, 0, Some((index, 100 + index))).await;
    }
    partial_round(&mut source).await;
    for _ in 0..20 {
        assert!(source.poll_batch(1000).await.unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let initial = source.checkpoint();
    assert_eq!(initial.get_offset("events:0"), None);
    assert_eq!(initial.get_offset("events:1"), None);
    send(&producer, 1, Some((1000, 90))).await;
    let first = next_round(&mut source, 1000).await;
    let first_rows = rows(&first);
    assert_eq!(
        first_rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        [0, 1000]
    );
    assert_eq!(first_rows[0].2, first_rows[1].2);
    assert_eq!(first_rows[0].3, first_rows[1].3);
    assert_eq!((first_rows[0].4, first_rows[1].4), (0, 1));
    let committed = source.checkpoint();
    assert_eq!(committed.get_offset("events:0"), Some("1"));
    assert_eq!(committed.get_offset("events:1"), Some("0"));
    // Consume the first member of the next round without publishing its cursor.
    partial_round(&mut source).await;
    assert_eq!(source.checkpoint().get_offset("events:0"), Some("1"));
    send(&producer, 1, Some((1001, 200))).await;
    let uninterrupted = rows(&next_round(&mut source, 1000).await);
    source.close().await.unwrap();
    let mut resumed = open(
        &cfg,
        SourcePosition::Resume {
            attempt: CheckpointAttempt::new(1, 1),
            checkpoint: committed,
        },
    )
    .await;
    let replayed = rows(&next_round(&mut resumed, 2).await);
    assert_eq!(replayed, uninterrupted);
    assert_eq!(
        replayed.iter().map(|row| row.0).collect::<Vec<_>>(),
        [1, 1001]
    );
    assert!(first_rows[1].3 < replayed[0].3);
    resumed.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_round_drain_rewinds_partial_input_on_the_retained_owner() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("events", 2, 1).unwrap();
    let producer = producer(&cluster);
    let cfg = config(&cluster.bootstrap_servers());
    let registry = Arc::new(VnodeRegistry::single_owner(4, NodeId(1)));
    let mut source = KafkaSource::new(
        schema(),
        KafkaSourceConfig::from_config(&cfg).unwrap(),
        None,
    );
    source
        .set_vnode_assignment("rounds", Arc::clone(&registry), NodeId(1))
        .unwrap();
    source
        .start(
            SourceStart::new(
                cfg.clone(),
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let routes = kafka_partition_routes(
        "rounds",
        4,
        &[(Arc::from("events"), 2)],
        KafkaReplayOrder::PartitionRounds,
    )
    .unwrap();
    assert_eq!(routes["events"].as_ref(), [0, 0]);
    send(&producer, 0, Some((10, 100))).await;
    partial_round(&mut source).await;
    let round = AssignmentDrainId {
        predecessor_version: registry.assignment_version(),
        target_version: registry.assignment_version() + 1,
        digest: [7; 32],
    };
    source
        .begin_drain(
            &SourceDrainRequest::new(round).unwrap(),
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !source.poll_drain_ready(round).unwrap() {
            assert!(source.poll_batch(2).await.unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(source.checkpoint().get_offset("events:0"), None);
    registry.set_assignment([NodeId(1), NodeId(2), NodeId(2), NodeId(2)].into());
    source
        .finish_drain(
            SourceDrainResolution {
                round,
                outcome: SourceDrainOutcome::Commit,
            },
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    send(&producer, 1, Some((11, 120))).await;
    assert_eq!(
        rows(&next_round(&mut source, 2).await)
            .iter()
            .map(|row| row.0)
            .collect::<Vec<_>>(),
        [10, 11]
    );
    source.close().await.unwrap();
}
