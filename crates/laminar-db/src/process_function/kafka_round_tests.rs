use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, StringArray, TimestampMicrosecondArray};
use laminar_connectors::connector::DeliveryGuarantee;
use laminar_core::streaming::StreamCheckpointConfig;
use parking_lot::Mutex;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{FutureProducer, FutureRecord};

use super::tests::{descriptor, AccountActivity};
use super::{NativeProcessFunction, ProcessActivation, ProcessActivationResult, ProcessCallback};
use crate::subscription::{PortalFrame, SubscribeStart, SubscriptionPortal};
use crate::{DbError, LaminarDB};

type Callback = (u64, String, i64, bool);
type Output = (String, String, i64, i64);

struct RecordedActivity(Arc<Mutex<Vec<Callback>>>);

impl NativeProcessFunction for RecordedActivity {
    fn invoke(
        &self,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, DbError> {
        self.0.lock().extend(activations.iter().map(|activation| {
            (
                activation.id,
                activation.key_text.clone(),
                activation.event_time_us,
                matches!(activation.callback, ProcessCallback::Timer { .. }),
            )
        }));
        AccountActivity.invoke(activations)
    }
}

async fn database(
    storage: &std::path::Path,
    brokers: &str,
    callbacks: &Arc<Mutex<Vec<Callback>>>,
    buffer: usize,
) -> Arc<LaminarDB> {
    let db = LaminarDB::builder()
        .storage_dir(storage)
        .buffer_size(buffer)
        .source_idle_timeout(Duration::from_millis(1))
        .pipeline_batch_window(Duration::from_millis(50))
        .checkpoint(StreamCheckpointConfig {
            interval_ms: None,
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .build()
        .await
        .unwrap();
    db.execute(&format!(
        "CREATE SOURCE events (account VARCHAR NOT NULL, amount BIGINT NOT NULL, \
         ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND) \
         FROM KAFKA ('bootstrap.servers' = '{brokers}', 'group.id' = 'activity-rounds', \
         'topic' = 'events', 'startup.mode' = 'earliest', 'replay.order' = 'partition_rounds') FORMAT JSON"
    )).await.unwrap();
    db.register_native_process_function(
        "activity",
        "events",
        descriptor(),
        Arc::new(RecordedActivity(Arc::clone(callbacks))),
    )
    .await
    .unwrap();
    db
}

async fn output(portal: &mut SubscriptionPortal, db: &LaminarDB, count: usize) -> Vec<Output> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut rows = Vec::new();
        while rows.len() < count {
            let frame = portal.next_frame().await.unwrap_or_else(|| {
                panic!("Kafka process subscription closed: {:?}", db.last_fault())
            });
            let batch = match frame {
                PortalFrame::Batch { batch, .. } => batch,
                PortalFrame::Error { message } => panic!("Kafka process subscription: {message}"),
                PortalFrame::Lagged(skipped) => {
                    panic!("Kafka process subscription lagged by {skipped}")
                }
                _ => continue,
            };
            let accounts = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let kinds = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let totals = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let timestamps = batch
                .column(4)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            rows.extend((0..batch.num_rows()).map(|row| {
                (
                    accounts.value(row).into(),
                    kinds.value(row).into(),
                    totals.value(row),
                    timestamps.value(row),
                )
            }));
        }
        assert_eq!(rows.len(), count);
        rows
    })
    .await
    .expect("Kafka process outputs did not reach the expected watermark cut")
}

async fn send(producer: &FutureProducer, partition: i32, account: &str, amount: i64, ts: i64) {
    // Numeric JSON timestamps default to milliseconds; fixture expectations use microseconds.
    assert_eq!(ts % 1000, 0);
    let payload = format!(
        "{{\"account\":\"{account}\",\"amount\":{amount},\"ts\":{}}}",
        ts / 1000
    );
    producer
        .send(
            FutureRecord::to("events")
                .partition(partition)
                .key(account)
                .payload(&payload),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
}

async fn qualify(restart: bool) -> (Vec<Output>, Vec<Callback>) {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("events", 2, 1).unwrap();
    let brokers = cluster.bootstrap_servers();
    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();
    let storage = tempfile::tempdir().unwrap();
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    let mut db = database(storage.path(), &brokers, &callbacks, 1024).await;
    let mut portal = db
        .open_subscription("activity", None, SubscribeStart::Tail)
        .await
        .unwrap();
    db.start().await.unwrap();
    send(&producer, 0, "a", 60, 100_000).await;
    send(&producer, 1, "b", 10, 100_000).await;
    assert_eq!(output(&mut portal, &db, 2).await.len(), 2);
    assert!(db.checkpoint().await.unwrap().success);
    callbacks.lock().clear();
    // The next round's head remains unaccepted while the second partition idles.
    send(&producer, 0, "a", 50, 108_000).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    if restart {
        db.shutdown().await.unwrap();
        drop(portal);
        drop(db);
        db = database(storage.path(), &brokers, &callbacks, 2).await;
        portal = db
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        db.start().await.unwrap();
    }
    db.source_untyped("events").unwrap().watermark(9_000_000);
    send(&producer, 1, "c", 7, 112_000).await;
    // Reverse producer arrival order for complete future rounds. Native partition order wins.
    send(&producer, 1, "e", 4, 120_000).await;
    send(&producer, 0, "d", 3, 120_000).await;
    send(&producer, 1, "d", 6, 130_000).await;
    send(&producer, 0, "c", 5, 121_000).await;
    let mut rows = output(&mut portal, &db, 9).await;
    db.shutdown().await.unwrap();
    rows.sort();
    let mut callbacks = callbacks.lock().clone();
    callbacks.sort();
    (rows, callbacks)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_partition_rounds_replay_matching_process_ids_and_watermark_cuts() {
    let (expected, expected_callbacks) = qualify(false).await;
    let (actual, actual_callbacks) = qualify(true).await;
    assert_eq!(actual, expected);
    assert_eq!(actual_callbacks, expected_callbacks);
    assert!(
        actual.contains(&("a".into(), "inactive".into(), 110, 118_000)),
        "unexpected process outputs: {actual:?}"
    );
    assert!(!actual
        .iter()
        .any(|row| row.0 == "a" && row.1 == "inactive" && row.3 == 110_000));
    assert_eq!(actual_callbacks.len(), 9);
    assert!(actual_callbacks
        .windows(2)
        .all(|pair| pair[0].0 != pair[1].0));
}
