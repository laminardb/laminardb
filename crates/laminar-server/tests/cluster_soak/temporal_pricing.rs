//! The workload of the soaks that price Kafka trades through a temporal join whose right side is
//! a placed rates source: key `k`'s only rate is [`RATE_BASE`]` + k`, trades `0..` are stamped
//! with the wall time, and a filter keeps only the trades priced with their key's rate, so a wrong
//! or missing join shows as a lost id.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::kill_rejoin::SoakSettings;
use super::multi_writer_sinks::{block_on, wait_for_delivery, Oracle};
use super::{
    matrix_event_time_ms, wait_for, wait_for_converged_durable_checkpoint, DurableCheckpointStatus,
    Node, SOAK_PRODUCER_MAX_IN_FLIGHT,
};

pub(super) const KEYS: i64 = 64;
/// Key `k`'s only rate is `RATE_BASE + k`.
pub(super) const RATE_BASE: i64 = 1_000;
/// The heartbeat row's key, which no trade uses.
pub(super) const HEARTBEAT_KEY: i64 = -1;
pub(super) const HEARTBEAT_EVERY: Duration = Duration::from_millis(250);
/// Longer than the clock difference between the soak host and the rates' server, so every trade
/// is stamped after its key's rate changed.
pub(super) const RATES_SETTLE: Duration = Duration::from_secs(2);
pub(super) const TRADE_PARTITIONS: i32 = 4;

/// The trades source, the `priced` stream that joins each trade to the rate its key had at the
/// trade's time, and the `correctly_priced` stream of the ids priced with their key's rate;
/// `rate_key` is the rates source's key column.
pub(super) fn trades_and_pricing(brokers: &str, trades: &str, rate_key: &str) -> String {
    format!(
        r#"
[[source]]
name = "trades"
connector = "kafka"
format = "json"
[source.properties]
"bootstrap.servers" = "{brokers}"
topic = "{trades}"
"group.id" = "{trades}"
"startup.mode" = "earliest"
"json.column.at.epoch_unit" = "millis"
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "key"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "at"
type = "TIMESTAMP"
nullable = false
[source.watermark]
column = "at"
max_out_of_orderness = "1s"

[[pipeline]]
name = "priced"
sql = """
SELECT t.id AS id, t.key AS key, r.rate AS rate
FROM trades t
LEFT JOIN rates FOR SYSTEM_TIME AS OF t.at AS r
  ON t.key = r.{rate_key}
"""

# A trade priced with no rate or another key's is dropped here, so it shows as a lost id.
[[pipeline]]
name = "correctly_priced"
sql = "SELECT id FROM priced WHERE rate = key + {RATE_BASE}"
"#
    )
}

/// Wait until the source owner has read a heartbeat, which grows its `received` metric, then
/// for two checkpoints so the committed cut holds the source's position. Returns the last of
/// them.
pub(super) fn warm_up(
    nodes: &mut [Node],
    heartbeat: &mut Worker,
    settings: &SoakSettings,
    latest: DurableCheckpointStatus,
    received: &str,
) -> DurableCheckpointStatus {
    wait_for(
        "the source owner to read a heartbeat",
        settings.recovery_ceiling,
        || {
            heartbeat.assert_running();
            nodes
                .iter()
                .any(|node| node.metric(received).is_some_and(|events| events > 0.0))
        },
    );
    let deadline = Instant::now() + settings.recovery_ceiling;
    let first = wait_for_converged_durable_checkpoint(nodes, deadline, "warm-up", Some(latest));
    wait_for_converged_durable_checkpoint(nodes, deadline, "warm-up", Some(first))
}

/// The output once every produced trade reached it.
pub(super) struct PricedTrades {
    pub(super) produced: u64,
    pub(super) duplicates: u64,
    /// Every node that opened the rates source.
    pub(super) owners: BTreeSet<usize>,
}

/// Stop the trader and wait until every produced trade reached the output; the heartbeat keeps
/// moving the rates' watermark past the last trades until they are priced. No other id may reach
/// the output, and more than one node must have opened the rates source.
pub(super) fn priced_trades(
    nodes: &[Node],
    trader: Worker,
    heartbeat: Worker,
    oracle: Oracle,
    settings: &SoakSettings,
) -> PricedTrades {
    let owners = placed_owners(nodes, "rates");
    let produced = trader.stop();
    let observed = wait_for_delivery(oracle, produced, settings.recovery_ceiling);
    heartbeat.stop();
    let unexpected: Vec<u64> = observed.range(produced..).map(|(id, _)| *id).collect();
    assert!(
        unexpected.is_empty(),
        "the output holds {} ids that were never produced, first {:?}",
        unexpected.len(),
        &unexpected[..unexpected.len().min(8)]
    );
    assert!(
        owners.len() > 1,
        "only node {owners:?} ever opened the rates source: it never moved"
    );
    PricedTrades {
        produced,
        duplicates: observed.values().map(|count| u64::from(count - 1)).sum(),
        owners,
    }
}

/// Every node whose log shows it opened the placed `source`. Logs are appended across restarts,
/// and a survivor may hold the source only for the seconds before the dead owner rejoins, too
/// briefly to count events on.
fn placed_owners(nodes: &[Node], source: &str) -> BTreeSet<usize> {
    let opened = format!("placed singleton source opened on this node source={source}");
    nodes
        .iter()
        .filter(|node| {
            std::fs::read_to_string(&node.log_path).is_ok_and(|log| log.contains(&opened))
        })
        .map(|node| node.id)
        .collect()
}

/// A background thread that runs `work` until stopped and returns its count.
pub(super) struct Worker {
    name: &'static str,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<u64>>,
}

impl Worker {
    pub(super) fn spawn<F, Fut>(name: &'static str, work: F) -> Self
    where
        F: FnOnce(Arc<AtomicBool>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = u64>,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || block_on(work(stop)))
        };
        Self {
            name,
            stop,
            handle: Some(handle),
        }
    }

    pub(super) fn assert_running(&mut self) {
        if !self.handle.as_ref().is_some_and(JoinHandle::is_finished) {
            return;
        }
        match self.handle.take().expect("worker handle").join() {
            Ok(_) => panic!("the {} stopped before the soak completed", self.name),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    pub(super) fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Release);
        self.handle
            .take()
            .unwrap_or_else(|| panic!("the {} was already stopped", self.name))
            .join()
            .unwrap_or_else(|_| panic!("the {} thread failed", self.name))
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Produces trades `0..` at `rps`, each on key `id % KEYS` and stamped with the wall time. Every
/// send must be acknowledged before the count is returned, so the acknowledged ids are the prefix
/// `0..produced`.
pub(super) fn spawn_trader(brokers: String, topic: String, rps: u64) -> Worker {
    Worker::spawn("trade producer", move |stop| async move {
        produce(&brokers, &topic, rps, &stop).await
    })
}

async fn produce(brokers: &str, topic: &str, rps: u64, stop: &AtomicBool) -> u64 {
    use futures::stream::{FuturesUnordered, StreamExt as _};
    use rdkafka::producer::{FutureProducer, FutureRecord};

    let acknowledge =
        |delivery: Option<Result<_, futures::channel::oneshot::Canceled>>| match delivery {
            Some(Ok(Ok(_))) => {}
            Some(Ok(Err((error, _)))) => panic!("Kafka never acknowledged a trade: {error}"),
            Some(Err(_)) | None => panic!("a trade's delivery was cancelled"),
        };
    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .set("enable.idempotence", "true")
        .create()
        .expect("trade producer");
    let start = tokio::time::Instant::now();
    let mut deliveries = FuturesUnordered::new();
    let mut produced = 0_u64;
    while !stop.load(Ordering::Acquire) {
        let due = start + Duration::from_secs_f64(produced as f64 / rps as f64);
        // Settle deliveries while the next trade is not yet due or too many are in flight.
        while deliveries.len() >= SOAK_PRODUCER_MAX_IN_FLIGHT || tokio::time::Instant::now() < due {
            tokio::select! {
                delivery = deliveries.next(), if !deliveries.is_empty() => acknowledge(delivery),
                () = tokio::time::sleep_until(due),
                    if deliveries.len() < SOAK_PRODUCER_MAX_IN_FLIGHT => {}
            }
        }
        let id = i64::try_from(produced).expect("trade ids fit i64");
        let payload = format!(
            r#"{{"id":{id},"key":{},"at":{}}}"#,
            id % KEYS,
            matrix_event_time_ms()
        );
        let key = id.to_string();
        let delivery = producer
            .send_result(FutureRecord::to(topic).payload(&payload).key(&key))
            .unwrap_or_else(|(error, _)| panic!("could not queue trade {id}: {error}"));
        deliveries.push(delivery);
        produced += 1;
    }
    while !deliveries.is_empty() {
        acknowledge(deliveries.next().await);
    }
    produced
}
