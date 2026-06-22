//! B2 source-pause: a vnode-partitioned Kafka source must stop consuming the
//! partitions of vnodes marked draining (so a pre-rotation checkpoint is a clean
//! cut), and resume them when the drain clears.
//!
//! Needs a real broker. Skips (does not fail) when `LAMINAR_KAFKA_BROKERS`
//! (default `127.0.0.1:19092`) is unreachable, so it's a no-op in environments
//! without one. Run against a broker with:
//!   LAMINAR_KAFKA_BROKERS=127.0.0.1:19192 \
//!     cargo test -p laminar-connectors --features kafka --test kafka_vnode_drain -- --nocapture

#![cfg(feature = "kafka")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::time::sleep;

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::SourceConnector;
use laminar_connectors::kafka::{KafkaSource, KafkaSourceConfig, StartupMode, TopicSubscription};
use laminar_core::state::{NodeId, VnodeRegistry};

const DEFAULT_BROKERS: &str = "127.0.0.1:19092";

/// Brokers if the first one is reachable, else `None` (skip the test).
fn broker() -> Option<String> {
    let brokers = std::env::var("LAMINAR_KAFKA_BROKERS").unwrap_or_else(|_| DEFAULT_BROKERS.into());
    let addr: std::net::SocketAddr = brokers.split(',').next()?.trim().parse().ok()?;
    std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(1)).ok()?;
    Some(brokers)
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
    admin
        .create_topics([&new], &AdminOptions::new())
        .await
        .expect("create_topics");
}

/// Produce `seq in [start, start+count)` as `{"seq": n}`, keyed by `seq` so the
/// default partitioner spreads them across partitions.
async fn produce(brokers: &str, topic: &str, start: i64, count: i64) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("producer");
    for n in start..start + count {
        let payload = format!(r#"{{"seq":{n}}}"#);
        let key = n.to_string();
        producer
            .send(
                FutureRecord::to(topic).payload(&payload).key(&key),
                Duration::from_secs(10),
            )
            .await
            .expect("send");
    }
}

/// Last-consumed offset for one partition, from the source checkpoint (`-1` = none yet).
fn part_offset(source: &KafkaSource, topic: &str, partition: i32) -> i64 {
    source
        .checkpoint()
        .get_offset(&format!("{topic}-{partition}"))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_pauses_draining_vnode_partitions() {
    let Some(brokers) = broker() else {
        eprintln!("kafka_vnode_drain: no reachable broker — skipping");
        return;
    };

    const PARTS: i32 = 4;
    let topic = format!("vnode-drain-{}", std::process::id());
    create_topic(&brokers, &topic, PARTS).await;
    produce(&brokers, &topic, 0, 400).await;

    // One node owns every vnode; with vnode_count == partition count, partition p
    // binds to vnode p, so the source manually assigns all four partitions.
    let registry = Arc::new(VnodeRegistry::single_owner(PARTS as u32, NodeId(0)));
    let cfg = KafkaSourceConfig {
        bootstrap_servers: brokers.clone(),
        group_id: format!("vnode-drain-grp-{}", std::process::id()),
        subscription: TopicSubscription::Topics(vec![topic.clone()]),
        startup_mode: StartupMode::Earliest,
        ..KafkaSourceConfig::default()
    };
    let mut source = KafkaSource::new(schema(), cfg, None);
    source.set_vnode_assignment(Arc::clone(&registry), NodeId(0));
    source.open(&ConnectorConfig::new("kafka")).await.unwrap();

    // Consume the initial batch across all partitions.
    poll_for(&mut source, Duration::from_secs(4)).await;
    assert!(
        (0..PARTS).all(|p| part_offset(&source, &topic, p) >= 0),
        "every partition should have advanced before draining: {:?}",
        (0..PARTS)
            .map(|p| part_offset(&source, &topic, p))
            .collect::<Vec<_>>(),
    );

    // Drain vnode 1 → the source pauses partition 1. Let the pause take effect and
    // any already-buffered partition-1 records flush, then snapshot the cut.
    registry.mark_draining(&[1]);
    poll_for(&mut source, Duration::from_secs(3)).await;
    let cut_p1 = part_offset(&source, &topic, 1);
    let pre_p0 = part_offset(&source, &topic, 0);

    // Produce more to every partition; only the non-draining ones should advance.
    produce(&brokers, &topic, 400, 600).await;
    poll_for(&mut source, Duration::from_secs(6)).await;

    assert_eq!(
        part_offset(&source, &topic, 1),
        cut_p1,
        "draining partition 1 must not advance past the cut",
    );
    assert!(
        part_offset(&source, &topic, 0) > pre_p0,
        "a non-draining partition must keep consuming (p0 {pre_p0} -> {})",
        part_offset(&source, &topic, 0),
    );

    // Clear the drain → partition 1 resumes.
    registry.clear_draining();
    poll_for(&mut source, Duration::from_secs(6)).await;
    assert!(
        part_offset(&source, &topic, 1) > cut_p1,
        "partition 1 must resume after the drain clears ({cut_p1} -> {})",
        part_offset(&source, &topic, 1),
    );

    source.close().await.unwrap();
}
