//! External Kafka ledger and real process loss at a saturated graph port.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{
    capture_stopped_writer_cut, consume_through_cut, create_topic, kafka_test_brokers, unique,
    validated_id_counts, wait_for_required_ids, ClientConfig,
};

const DEADLINE: Duration = Duration::from_secs(45);
const WORKER: &str = "kafka_saturation::worker::run";

mod worker;

#[derive(Clone, Serialize, Deserialize)]
struct Case {
    directory: PathBuf,
    brokers: String,
    input: String,
    output: String,
    fail: bool,
    byte_capacity: Option<usize>,
}

fn record(path: impl AsRef<Path>, value: serde_json::Value) {
    fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
}

struct Worker(Child);

impl Worker {
    fn start(case: &Case, phase: &str) -> Self {
        let log = fs::File::create(case.directory.join(format!("{phase}.log"))).unwrap();
        Self(
            Command::new(std::env::current_exe().unwrap())
                .args([WORKER, "--ignored", "--exact", "--nocapture"])
                .env(
                    "LAMINAR_SATURATION_CASE",
                    serde_json::to_string(case).unwrap(),
                )
                .env("LAMINAR_SATURATION_PHASE", phase)
                .stdout(Stdio::from(log.try_clone().unwrap()))
                .stderr(Stdio::from(log))
                .spawn()
                .unwrap(),
        )
    }

    async fn wait_for(&mut self, path: &Path) {
        tokio::time::timeout(DEADLINE, async {
            while !path.exists() {
                assert!(
                    self.0.try_wait().unwrap().is_none(),
                    "worker exited: {path:?}"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("worker deadline: {path:?}"));
    }

    fn kill(&mut self) {
        self.0.kill().unwrap();
        assert!(!self.0.wait().unwrap().success());
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        // This owner reaps only its child, including on assertion failure.
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn produce(case: &Case, ids: std::ops::Range<i64>) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &case.brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .unwrap();
    let mut ledger = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(case.directory.join("acknowledged.jsonl"))
        .unwrap();
    for id in ids {
        let payload = json!({"id":id, "value":id * 10}).to_string();
        let delivery = producer
            .send(
                FutureRecord::to(&case.input)
                    .payload(&payload)
                    .key(&id.to_string()),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        writeln!(
            ledger,
            "{}",
            json!({"id":id, "payload":payload,
            "partition":delivery.partition, "offset":delivery.offset})
        )
        .unwrap();
        ledger.flush().unwrap();
    }
}

async fn audit(case: &Case, topic: &str, end: i64, label: &str) -> usize {
    let brokers = &case.brokers;
    let cut = capture_stopped_writer_cut(brokers, topic).await;
    let rows =
        consume_through_cut(brokers, topic, &unique("saturation_audit"), &cut, DEADLINE).await;
    let counts = validated_id_counts(&rows, 0..end);
    record(
        case.directory.join(format!("{label}.json")),
        json!({
            "topic":topic, "cut":cut, "rows":rows,
            "verified_ids":end, "duplicates":counts.values().sum::<usize>() - counts.len()
        }),
    );
    counts.values().sum::<usize>() - counts.len()
}

fn committed(case: &Case) -> Vec<u8> {
    let deployments: Vec<_> = fs::read_dir(case.directory.join("checkpoints/checkpoint-decisions"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(deployments.len(), 1);
    fs::read(deployments[0].join("head")).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Redpanda and a new LAMINAR_S12_DIAGNOSTICS directory"]
async fn durable_saturation_checkpoint_restart_ledger() {
    let brokers = kafka_test_brokers().expect("qualification requires real Kafka");
    let evidence = PathBuf::from(std::env::var("LAMINAR_S12_DIAGNOSTICS").unwrap());
    for (fail, bytes) in [(false, false), (true, false), (false, true), (true, true)] {
        let policy = if fail { "Fail" } else { "Backpressure" };
        let capacity = if bytes { "bytes" } else { "count" };
        let mut case = Case {
            directory: evidence.join(format!("saturation-{capacity}-{policy}")),
            brokers: brokers.to_owned(),
            input: unique("saturation_input"),
            output: unique("saturation_output"),
            fail,
            byte_capacity: None,
        };
        fs::create_dir(&case.directory).unwrap();
        create_topic(brokers, &case.input, 1).await;
        create_topic(brokers, &case.output, 1).await;
        let prefix = seed_case(&mut case, bytes).await;
        record(
            case.directory.join("case.json"),
            json!({
                "case":case, "mode":"embedded", "delivery":"at_least_once",
                "composition":"kafka_to_kafka", "capacity":capacity,
                "source_max_poll_records":1, "s12_qualified":false
            }),
        );
        pressure_case(&case, &prefix).await;
        let end = if fail { 6 } else { 8 };
        recover_case(&case, end).await;
        assert_eq!(audit(&case, &case.input, end + 2, "source-ledger").await, 0);
        let duplicates = audit(&case, &case.output, end + 2, "final-output").await;
        record(
            case.directory.join("report.json"),
            json!({
                "status":"passed", "policy":policy, "verified_ids":end + 2,
                "duplicates":duplicates, "s12_qualified":false, "capacity":capacity,
                "limits":"deterministic test UDF gate; no production SLO or cluster claim"
            }),
        );
        eprintln!(
            "{capacity}/{policy}: {} IDs verified, {duplicates} ALO duplicates",
            end + 2
        );
    }
}

async fn seed_case(case: &mut Case, bytes: bool) -> Vec<u8> {
    produce(case, 0..2).await;
    let mut seed = Worker::start(case, "seed");
    seed.wait_for(&case.directory.join("seed.done")).await;
    seed.kill();
    if bytes {
        let batches: Vec<BatchObservation> =
            serde_json::from_slice(&fs::read(case.directory.join("seed-batches.json")).unwrap())
                .unwrap();
        assert_eq!(batches.len(), 2);
        case.byte_capacity = Some(batches.iter().map(|batch| batch.retained_bytes).sum());
    }
    audit(case, &case.output, 2, "seed-output").await;
    let prefix = committed(case);
    fs::write(case.directory.join("seed-committed.json"), &prefix).unwrap();
    prefix
}

async fn pressure_case(case: &Case, prefix: &[u8]) {
    produce(case, 2..4).await;
    let mut pressure = Worker::start(case, "pressure");
    pressure
        .wait_for(&case.directory.join("pressure.checkpoint"))
        .await;
    assert_eq!(committed(case), prefix, "pressure must not advance the cut");
    // These broker-acknowledged successors arrive while replay owns the original cursors.
    produce(case, 4..6).await;
    if case.fail {
        pressure.kill();
        audit(case, &case.output, 2, "fault-output").await;
        return;
    }
    fs::write(case.directory.join("pressure.release"), []).unwrap();
    pressure
        .wait_for(&case.directory.join("pressure.done"))
        .await;
    pressure.kill();
    audit(case, &case.output, 6, "drained-output").await;
    let drained = committed(case);
    assert_ne!(drained, prefix);
    fs::write(case.directory.join("drained-committed.json"), &drained).unwrap();
    produce(case, 6..8).await;
    let mut crash = Worker::start(case, "crash");
    crash
        .wait_for(&case.directory.join("crash.checkpoint"))
        .await;
    assert_eq!(committed(case), drained);
    crash.kill();
    audit(case, &case.output, 6, "crash-output").await;
}

async fn recover_case(case: &Case, end: i64) {
    let brokers = &case.brokers;
    let mut recovery = Worker::start(case, "recovery");
    recovery
        .wait_for(&case.directory.join("recovery.ready"))
        .await;
    wait_for_required_ids(brokers, &case.output, &unique("recovery"), 0..end, DEADLINE).await;
    produce(case, end..end + 2).await;
    wait_for_required_ids(
        brokers,
        &case.output,
        &unique("canary"),
        0..end + 2,
        DEADLINE,
    )
    .await;
    fs::write(case.directory.join("recovery.finish"), []).unwrap();
    recovery
        .wait_for(&case.directory.join("recovery.done"))
        .await;
    recovery.kill();
}

#[derive(Clone, Serialize, Deserialize)]
struct BatchObservation {
    rows: usize,
    retained_bytes: usize,
}
