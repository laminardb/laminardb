//! A partitioned Kafka source must stop its complete live assignment at a cluster
//! rotation cut, reconcile the target, and resume from that exact cut.
//!
//! Needs a real broker. It skips when the default local address is absent, but a
//! configured `LAMINAR_KAFKA_BROKERS` must be reachable. Run with:
//!   LAMINAR_KAFKA_BROKERS=127.0.0.1:19092 \
//!     cargo test -p laminar-connectors --features kafka,testing --test kafka_global_drain -- --nocapture

#![cfg(all(feature = "kafka", feature = "testing"))]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int32Array, Int64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::time::{sleep, Instant as TokioInstant};

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    DeliveryGuarantee, SourceConnector, SourceDrainOutcome, SourceDrainRequest,
    SourceDrainResolution, SourcePosition, SourceStart,
};
use laminar_connectors::kafka::testing::partition_vnodes;
use laminar_connectors::kafka::{KafkaSource, KafkaSourceConfig, StartupMode, TopicSubscription};
use laminar_core::checkpoint::AssignmentDrainId;
use laminar_core::state::{CheckpointAttempt, NodeId, VnodeRegistry};

const DEFAULT_BROKERS: &str = "127.0.0.1:19092";

fn broker() -> Result<Option<String>, String> {
    let configured = std::env::var("LAMINAR_KAFKA_BROKERS").ok();
    let brokers = configured.clone().unwrap_or_else(|| DEFAULT_BROKERS.into());
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set(
            "group.id",
            format!("global-drain-probe-{}", uuid::Uuid::new_v4()),
        )
        .create()
        .map_err(|error| format!("invalid Kafka broker configuration '{brokers}': {error}"))?;
    match consumer.fetch_metadata(None, Duration::from_secs(1)) {
        Ok(metadata) if !metadata.brokers().is_empty() => Ok(Some(brokers)),
        Ok(_) if configured.is_some() => Err(format!(
            "configured Kafka broker '{brokers}' returned no broker metadata"
        )),
        Ok(_) => Ok(None),
        Err(error) if configured.is_some() => Err(format!(
            "configured Kafka broker '{brokers}' is unreachable: {error}"
        )),
        Err(_) => Ok(None),
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]))
}

async fn create_topic(brokers: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let new = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    let results = admin
        .create_topics([&new], &AdminOptions::new())
        .await
        .expect("create_topics request");
    assert_eq!(results.len(), 1, "create_topics result cardinality");
    for result in results {
        result.expect("create topic");
    }
}

async fn delete_topic(brokers: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let results = admin
        .delete_topics(&[topic], &AdminOptions::new())
        .await
        .expect("delete_topics request");
    assert_eq!(results.len(), 1, "delete_topics result cardinality");
    for result in results {
        result.expect("delete topic");
    }
}

/// Produce an exact number of records to every partition.
async fn produce_each_partition(
    brokers: &str,
    topic: &str,
    partitions: i32,
    start: i64,
    count_per_partition: i64,
) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("producer");
    for partition in 0..partitions {
        for index in 0..count_per_partition {
            let seq = start + i64::from(partition) * count_per_partition + index;
            let payload = format!(r#"{{"seq":{seq}}}"#);
            producer
                .send(
                    FutureRecord::<(), _>::to(topic)
                        .partition(partition)
                        .payload(&payload),
                    Duration::from_secs(10),
                )
                .await
                .expect("send");
        }
    }
}

/// Last-consumed offset for one partition, from the source checkpoint (`-1` = none yet).
fn part_offset(source: &KafkaSource, topic: &str, partition: i32) -> i64 {
    source
        .checkpoint()
        .get_offset(&format!("{topic}:{partition}"))
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(-1)
}

/// Poll for `dur`, draining whatever the reader has buffered.
async fn poll_for(source: &mut KafkaSource, dur: Duration) {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        source.poll_batch(1000).await.expect("poll_batch");
        sleep(Duration::from_millis(50)).await;
    }
}

async fn take_global_cut(
    source: &mut KafkaSource,
    registry: &VnodeRegistry,
    topic: &str,
    partitions: i32,
    digest: u8,
) -> (AssignmentDrainId, Vec<(i32, i64)>, TokioInstant) {
    let predecessor_version = registry.assignment_version();
    let round = AssignmentDrainId {
        predecessor_version,
        target_version: predecessor_version + 1,
        digest: [digest; 32],
    };
    let request = SourceDrainRequest::new(round).unwrap();
    let prepare_deadline = TokioInstant::now() + Duration::from_secs(10);
    source.begin_drain(&request, prepare_deadline).unwrap();
    let boundary_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        source.poll_batch(1_000).await.unwrap();
        if source.poll_drain_ready(round).unwrap() {
            break;
        }
        assert!(
            Instant::now() < boundary_deadline,
            "Kafka drain boundary timed out"
        );
        sleep(Duration::from_millis(20)).await;
    }
    let cut = (0..partitions)
        .map(|partition| (partition, part_offset(source, topic, partition)))
        .collect();
    (round, cut, prepare_deadline)
}

async fn assert_cut_held(source: &mut KafkaSource, topic: &str, cut: &[(i32, i64)]) {
    poll_for(source, Duration::from_secs(6)).await;
    for (partition, cut_offset) in cut {
        assert_eq!(
            part_offset(source, topic, *partition),
            *cut_offset,
            "partition {partition} advanced past the global cut"
        );
    }
}

async fn first_offsets(source: &mut KafkaSource, expected_partitions: &[i32]) -> Vec<(i32, i64)> {
    let mut first: Vec<_> = expected_partitions
        .iter()
        .map(|partition| (*partition, None::<i64>))
        .collect();
    let deadline = Instant::now() + Duration::from_secs(6);
    while first.iter().any(|(_, offset)| offset.is_none()) {
        if let Some(batch) = source.poll_batch(1_000).await.unwrap() {
            let schema = batch.records.schema();
            let partition_index = schema.index_of("_partition").unwrap();
            let offset_index = schema.index_of("_offset").unwrap();
            let batch_partitions = batch.records.column(partition_index);
            let batch_partitions = batch_partitions
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let batch_offsets = batch.records.column(offset_index);
            let batch_offsets = batch_offsets.as_any().downcast_ref::<Int64Array>().unwrap();
            for row in 0..batch.records.num_rows() {
                let partition = batch_partitions.value(row);
                let offset = batch_offsets.value(row);
                let (_, first_offset) = first
                    .iter_mut()
                    .find(|(expected, _)| *expected == partition)
                    .unwrap_or_else(|| panic!("source emitted unowned partition {partition}"));
                first_offset.get_or_insert(offset);
            }
        }
        assert!(
            Instant::now() < deadline,
            "Kafka source did not resume every expected input: {first:?}"
        );
        sleep(Duration::from_millis(20)).await;
    }
    first
        .into_iter()
        .map(|(partition, offset)| (partition, offset.unwrap()))
        .collect()
}

async fn assert_partitions_not_emitted(
    source: &mut KafkaSource,
    forbidden_partitions: &[i32],
    dur: Duration,
) {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        if let Some(batch) = source.poll_batch(1_000).await.unwrap() {
            let partition_index = batch.records.schema().index_of("_partition").unwrap();
            let partitions = batch.records.column(partition_index);
            let partitions = partitions.as_any().downcast_ref::<Int32Array>().unwrap();
            for row in 0..batch.records.num_rows() {
                let partition = partitions.value(row);
                assert!(
                    !forbidden_partitions.contains(&partition),
                    "predecessor emitted moved partition {partition} after the ownership cut"
                );
            }
        }
        sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_holds_global_cuts_across_abort_and_commit() {
    let Some(brokers) = broker().unwrap_or_else(|error| panic!("{error}")) else {
        eprintln!("kafka_global_drain: no reachable broker — skipping");
        return;
    };

    const PARTS: i32 = 4;
    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let topic = format!("global-drain-{run_id}");
    create_topic(&brokers, &topic, PARTS).await;
    produce_each_partition(&brokers, &topic, PARTS, 0, 100).await;

    // One node owns the complete source inventory initially.
    let registry = Arc::new(VnodeRegistry::single_owner(PARTS as u32, NodeId(1)));
    let (source_identity, routes) = (0..1_024)
        .find_map(|salt| {
            let identity = format!("global_drain_source_{salt}");
            let routes = partition_vnodes(&identity, &topic, PARTS, PARTS as u32).unwrap();
            (routes.iter().any(|route| *route != routes[0])).then_some((identity, routes))
        })
        .expect("test input must cover retained and removed target inputs");
    let cfg = KafkaSourceConfig {
        bootstrap_servers: brokers.clone(),
        group_id: format!("global-drain-grp-{run_id}"),
        subscription: TopicSubscription::Topics(vec![topic.clone()]),
        startup_mode: StartupMode::Earliest,
        include_metadata: true,
        ..KafkaSourceConfig::default()
    };
    let mut source = KafkaSource::new(schema(), cfg.clone(), None);
    source
        .set_vnode_assignment(&source_identity, Arc::clone(&registry), NodeId(1))
        .unwrap();
    source
        .start(SourceStart {
            config: ConnectorConfig::new("kafka"),
            position: SourcePosition::Initial,
            delivery: DeliveryGuarantee::AtLeastOnce,
        })
        .await
        .unwrap();

    // Consume the initial batch across all partitions.
    poll_for(&mut source, Duration::from_secs(4)).await;
    assert!(
        (0..PARTS).all(|p| part_offset(&source, &topic, p) >= 0),
        "every partition should have advanced before draining: {:?}",
        (0..PARTS)
            .map(|p| part_offset(&source, &topic, p))
            .collect::<Vec<_>>(),
    );

    let (abort_round, abort_cut, abort_prepare_deadline) =
        take_global_cut(&mut source, &registry, &topic, PARTS, 9).await;

    // No provider input may cross the global cut while target ownership is pending.
    produce_each_partition(&brokers, &topic, PARTS, 400, 150).await;
    assert_cut_held(&mut source, &topic, &abort_cut).await;
    tokio::time::sleep_until(abort_prepare_deadline + Duration::from_millis(20)).await;
    assert!(
        TokioInstant::now() >= abort_prepare_deadline,
        "test must resolve after the completed prepare phase expired"
    );

    // Abort installs the predecessor owner map at the target version. Every input must seek to
    // the first offset after its saved cut; neither a stale fetch queue nor broker group state may
    // choose the restart position.
    registry.set_assignment(registry.snapshot());
    assert_eq!(registry.assignment_version(), abort_round.target_version);
    let abort_resolution_deadline = TokioInstant::now() + Duration::from_secs(15);
    source
        .finish_drain(
            SourceDrainResolution {
                round: abort_round,
                outcome: SourceDrainOutcome::Abort,
            },
            abort_resolution_deadline,
        )
        .await
        .unwrap();
    let all_partitions: Vec<_> = (0..PARTS).collect();
    let resumed = first_offsets(&mut source, &all_partitions).await;
    for (partition, cut_offset) in &abort_cut {
        assert_eq!(
            resumed
                .iter()
                .find_map(|(resumed_partition, offset)| {
                    (*resumed_partition == *partition).then_some(*offset)
                })
                .unwrap(),
            cut_offset + 1,
            "partition {partition} did not resume at its saved cut"
        );
    }

    let (commit_round, commit_cut, _commit_prepare_deadline) =
        take_global_cut(&mut source, &registry, &topic, PARTS, 10).await;
    let successor_checkpoint = source.checkpoint();
    for (partition, cut_offset) in &commit_cut {
        assert_eq!(
            successor_checkpoint
                .get_offset(&format!("{topic}:{partition}"))
                .and_then(|offset| offset.parse::<i64>().ok()),
            Some(*cut_offset),
            "successor checkpoint does not match the committed drain cut"
        );
    }
    produce_each_partition(&brokers, &topic, PARTS, 1_000, 150).await;
    assert_cut_held(&mut source, &topic, &commit_cut).await;

    let moved_vnode = routes[0];
    let removed_partitions: Vec<i32> = (0..PARTS)
        .filter(|partition| routes[*partition as usize] == moved_vnode)
        .collect();
    let retained_partitions: Vec<i32> = (0..PARTS)
        .filter(|partition| routes[*partition as usize] != moved_vnode)
        .collect();
    assert!(!removed_partitions.is_empty());
    assert!(!retained_partitions.is_empty());
    let mut target_owners = registry.snapshot().to_vec();
    target_owners[moved_vnode as usize] = NodeId(2);
    registry.set_assignment(target_owners.into());
    assert_eq!(registry.assignment_version(), commit_round.target_version);
    let commit_resolution_deadline = TokioInstant::now() + Duration::from_secs(15);
    source
        .finish_drain(
            SourceDrainResolution {
                round: commit_round,
                outcome: SourceDrainOutcome::Commit,
            },
            commit_resolution_deadline,
        )
        .await
        .unwrap();

    let mut successor = KafkaSource::new(schema(), cfg, None);
    successor
        .set_vnode_assignment(&source_identity, Arc::clone(&registry), NodeId(2))
        .unwrap();
    successor
        .start(SourceStart {
            config: ConnectorConfig::new("kafka"),
            position: SourcePosition::Resume {
                attempt: CheckpointAttempt::new(1, 1),
                checkpoint: successor_checkpoint,
            },
            delivery: DeliveryGuarantee::AtLeastOnce,
        })
        .await
        .unwrap();

    let (successor_first, ()) = tokio::join!(
        first_offsets(&mut successor, &removed_partitions),
        assert_partitions_not_emitted(&mut source, &removed_partitions, Duration::from_secs(6))
    );
    for (partition, cut_offset) in &commit_cut {
        if removed_partitions.contains(partition) {
            assert_eq!(
                successor_first
                    .iter()
                    .find_map(|(resumed_partition, offset)| {
                        (resumed_partition == partition).then_some(*offset)
                    })
                    .unwrap(),
                cut_offset + 1,
                "successor partition {partition} did not resume at the committed cut"
            );
        }
    }

    for (partition, cut_offset) in commit_cut {
        let after = part_offset(&source, &topic, partition);
        if retained_partitions.contains(&partition) {
            assert!(
                after > cut_offset,
                "retained target partition {partition} did not resume"
            );
        } else {
            assert!(removed_partitions.contains(&partition));
            assert!(
                after <= cut_offset,
                "removed target partition {partition} advanced after ownership changed"
            );
        }
    }

    successor.close().await.unwrap();
    source.close().await.unwrap();
    delete_topic(&brokers, &topic).await;
}
