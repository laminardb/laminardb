//! Recent reads through Kafka: a read waits for the first committed checkpoint that holds every
//! record its views' sources had upstream when it arrived, so a client reads its own writes,
//! past transaction markers and without waiting for an open transaction. A waiting read brings
//! the next periodic checkpoint forward when the configuration allows it.
//!
//! It skips when the default local address is absent, but a configured `LAMINAR_KAFKA_BROKERS`
//! must be reachable. Run with:
//!   LAMINAR_KAFKA_BROKERS=127.0.0.1:19092 \
//!     cargo test -p laminar-db --features kafka --test recent_reads

#![cfg(feature = "kafka")]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array};
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::{
    CommittedQuery, CommittedRead, CommittedReadError, DbError, EngineMetrics, LaminarConfig,
    LaminarDB,
};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

const DEFAULT_BROKERS: &str = "127.0.0.1:19092";
const PARTITIONS: i32 = 3;
const TOTAL: &str = "SELECT SUM(n) FROM totals";

fn broker() -> Option<String> {
    let configured = std::env::var("LAMINAR_KAFKA_BROKERS").ok();
    let brokers = configured.clone().unwrap_or_else(|| DEFAULT_BROKERS.into());
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("probe consumer");
    match consumer.fetch_metadata(None, Duration::from_secs(10)) {
        Ok(metadata) if !metadata.brokers().is_empty() => Some(brokers),
        _ if configured.is_some() => panic!("configured Kafka broker '{brokers}' is unreachable"),
        _ => None,
    }
}

async fn create_topic(brokers: &str) -> String {
    let topic = format!("recent-reads-{}", uuid::Uuid::new_v4());
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin");
    let new = NewTopic::new(&topic, PARTITIONS, TopicReplication::Fixed(1));
    for result in admin
        .create_topics([&new], &AdminOptions::new())
        .await
        .expect("create")
    {
        result.expect("create topic");
    }
    topic
}

fn producer(brokers: &str, transactional: bool) -> BaseProducer {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers);
    if transactional {
        config.set(
            "transactional.id",
            format!("recent-reads-{}", uuid::Uuid::new_v4()),
        );
    }
    let producer: BaseProducer = config.create().expect("producer");
    if transactional {
        producer
            .init_transactions(Duration::from_secs(10))
            .expect("init transactions");
    }
    producer
}

/// Write rows `ids`, keyed by id so they spread over every partition.
fn produce(producer: &BaseProducer, topic: &str, ids: std::ops::Range<i64>) {
    for id in ids {
        let key = id.to_string();
        let value = format!(r#"{{"id":{id},"k":{}}}"#, id % 4);
        producer
            .send(BaseRecord::to(topic).key(&key).payload(&value))
            .map_err(|(error, _)| error)
            .expect("enqueue");
    }
    producer.flush(Duration::from_secs(10)).expect("flush");
}

fn manual() -> StreamCheckpointConfig {
    StreamCheckpointConfig {
        interval_ms: None,
        ..Default::default()
    }
}

async fn open(
    brokers: &str,
    topic: &str,
    storage: &std::path::Path,
    checkpoint: StreamCheckpointConfig,
) -> Arc<LaminarDB> {
    let db = LaminarDB::open_with_config(LaminarConfig {
        storage_dir: Some(storage.to_path_buf()),
        checkpoint: Some(checkpoint),
        ..LaminarConfig::default()
    })
    .expect("open");
    for statement in [
        format!(
            "CREATE SOURCE input (id BIGINT NOT NULL, k BIGINT NOT NULL) FROM KAFKA (\
             'bootstrap.servers' = '{brokers}', 'topic' = '{topic}', 'group.id' = 'recent', \
             'startup.mode' = 'earliest') FORMAT JSON"
        ),
        "CREATE SOURCE pushed (k BIGINT)".into(),
        "CREATE STREAM keyed AS SELECT id, k FROM input".into(),
        "CREATE MATERIALIZED VIEW totals AS SELECT k, COUNT(*) AS n FROM keyed GROUP BY k".into(),
        "CREATE MATERIALIZED VIEW pushed_counts AS SELECT k, COUNT(*) AS n FROM pushed GROUP BY k"
            .into(),
    ] {
        db.execute(&statement)
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    db.start().await.expect("start");
    db
}

async fn checkpoint(db: &LaminarDB) -> u64 {
    let result = db.checkpoint().await.expect("checkpoint");
    assert!(result.success, "checkpoint failed: {:?}", result.error);
    result.epoch
}

/// Read `sql` at the recent level, checkpointing every 200 ms until it returns.
async fn recent(db: &LaminarDB, sql: &str, at_least: u64) -> Result<CommittedQuery, DbError> {
    let read = db.query_committed(sql, CommittedRead::Recent { at_least });
    tokio::pin!(read);
    loop {
        tokio::select! {
            result = &mut read => return result,
            () = tokio::time::sleep(Duration::from_millis(200)) => {
                checkpoint(db).await;
            }
        }
    }
}

fn total(query: &CommittedQuery) -> i64 {
    let column = query.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(column.len(), 1);
    column.value(0)
}

#[tokio::test]
async fn a_recent_read_holds_every_record_written_before_it() {
    let Some(brokers) = broker() else {
        eprintln!("skipping: no Kafka broker at {DEFAULT_BROKERS}");
        return;
    };
    let topic = create_topic(&brokers).await;
    let dir = tempfile::tempdir().unwrap();
    let db = open(&brokers, &topic, dir.path(), manual()).await;
    let metrics = Arc::new(EngineMetrics::new(&prometheus::Registry::new()));
    db.set_engine_metrics(Arc::clone(&metrics));
    let plain = producer(&brokers, false);

    produce(&plain, &topic, 0..30);
    let first = recent(&db, TOTAL, 0).await.unwrap();
    assert_eq!(total(&first), 30);

    produce(&plain, &topic, 30..50);
    let read = db.query_committed(TOTAL, CommittedRead::Recent { at_least: 0 });
    tokio::pin!(read);
    assert!(
        tokio::time::timeout(Duration::from_millis(500), &mut read)
            .await
            .is_err(),
        "epoch {} does not hold the new records",
        first.epoch
    );
    let mut second = None;
    while second.is_none() {
        tokio::select! {
            result = &mut read => second = Some(result.unwrap()),
            () = tokio::time::sleep(Duration::from_millis(200)) => {
                checkpoint(&db).await;
            }
        }
    }
    let second = second.unwrap();
    assert!(second.epoch > first.epoch);
    assert_eq!(total(&second), 50);

    // A floor above the covering cut is waited for too.
    let floored = recent(&db, TOTAL, second.epoch + 3).await.unwrap();
    assert!(floored.epoch >= second.epoch + 3);
    assert_eq!(total(&floored), 50);

    let reads = |level| metrics.views.reads_total.with_label_values(&[level]).get();
    assert_eq!(reads("recent"), 3);
    assert_eq!(metrics.views.recent_read_wait_seconds.get_sample_count(), 3);
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_recent_read_passes_transaction_markers_and_skips_open_transactions() {
    let Some(brokers) = broker() else {
        eprintln!("skipping: no Kafka broker at {DEFAULT_BROKERS}");
        return;
    };
    let topic = create_topic(&brokers).await;
    let dir = tempfile::tempdir().unwrap();
    let db = open(&brokers, &topic, dir.path(), manual()).await;

    let committed = producer(&brokers, true);
    committed.begin_transaction().unwrap();
    produce(&committed, &topic, 0..10);
    committed
        .commit_transaction(Duration::from_secs(10))
        .unwrap();
    // Each partition now ends in a commit marker the source never delivers.
    assert_eq!(total(&recent(&db, TOTAL, 0).await.unwrap()), 10);

    let open_transaction = producer(&brokers, true);
    open_transaction.begin_transaction().unwrap();
    produce(&open_transaction, &topic, 10..15);
    assert_eq!(total(&recent(&db, TOTAL, 0).await.unwrap()), 10);
    open_transaction
        .commit_transaction(Duration::from_secs(10))
        .unwrap();
    assert_eq!(total(&recent(&db, TOTAL, 0).await.unwrap()), 15);
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_recent_read_waits_only_for_the_sources_behind_its_views() {
    let Some(brokers) = broker() else {
        eprintln!("skipping: no Kafka broker at {DEFAULT_BROKERS}");
        return;
    };
    let topic = create_topic(&brokers).await;
    let dir = tempfile::tempdir().unwrap();
    let db = open(&brokers, &topic, dir.path(), manual()).await;
    produce(&producer(&brokers, false), &topic, 0..5);

    // `pushed` has no upstream to ask, so it is not part of a read of `totals`.
    assert_eq!(total(&recent(&db, TOTAL, 0).await.unwrap()), 5);
    let error = recent(&db, "SELECT * FROM pushed_counts", 0)
        .await
        .unwrap_err();
    assert!(
        matches!(
            &error,
            DbError::CommittedRead(CommittedReadError::RecencyUnsupported { input })
                if input == "pushed"
        ),
        "{error}"
    );
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_waiting_recent_read_brings_the_next_periodic_checkpoint_forward() {
    let Some(brokers) = broker() else {
        eprintln!("skipping: no Kafka broker at {DEFAULT_BROKERS}");
        return;
    };
    let topic = create_topic(&brokers).await;
    let dir = tempfile::tempdir().unwrap();
    // No periodic checkpoint is due within the read deadline; only the reads' requests are.
    let checkpoint = StreamCheckpointConfig {
        interval_ms: Some(600_000),
        recent_read_interval_ms: Some(100),
        ..Default::default()
    };
    let db = open(&brokers, &topic, dir.path(), checkpoint).await;
    let plain = producer(&brokers, false);

    let mut epoch = 0;
    for written in [10, 20, 30] {
        produce(&plain, &topic, written - 10..written);
        let read = db
            .query_committed(TOTAL, CommittedRead::Recent { at_least: 0 })
            .await
            .unwrap();
        assert_eq!(total(&read), written);
        assert!(read.epoch > epoch);
        epoch = read.epoch;
    }
    db.shutdown().await.unwrap();
}
