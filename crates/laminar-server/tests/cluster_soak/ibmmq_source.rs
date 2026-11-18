//! Three-node at-least-once soak for an IBM MQ source, which one node at a time consumes under
//! syncpoint. A publisher puts ids `0..` on a run-scoped local queue, committing each put. The node
//! that owns the source's home vnode gets them into one unit of work and commits it when the
//! checkpoint holding it is durable; a stream forwards the rows to an IBM MQ sink, and an
//! independent oracle drains the output queue. The observed leader and each follower are killed
//! inside a checkpoint and rejoin (`kill_rejoin`): when the consuming node dies the queue manager
//! backs out its unit of work and a survivor takes the source from the committed cut, and when the
//! source moves at a rejoin the donor commits the handoff unit before it closes.
//!
//! Every published id must reach the output and no other id may; duplicates are reported. More
//! than one node must have consumed the queue, and every node must keep committing checkpoints.
//!
//! IBM MQ's live suites are a manual lane (the queue manager image is amd64-only and licensed for
//! development), so no workflow runs this leg:
//!
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,ibmmq --test cluster_soak \
//!   ibmmq_source::three_node_ibmmq_source_kill9_soak -- --ignored --exact --nocapture
//! ```
//!
//! It reads the shared `LAMINAR_SOAK_CHECKPOINT_URL`, `LAMINAR_SOAK_S3_*`,
//! `LAMINAR_SOAK_KEY_GROUPS`, `LAMINAR_SOAK_INTERVAL_MS`, `LAMINAR_SOAK_MAX_RECOVERY_MS` and
//! `LAMINAR_SOAK_DISCOVERY` knobs, plus `LAMINAR_SOAK_KILLS` (default 4), `LAMINAR_SOAK_SECONDS`
//! (steady phase after the kills, default 30), `LAMINAR_SOAK_RPS` (default 400) and the IBM MQ
//! settings of the IBM MQ sink soak.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::SinkConnector as _;
use laminar_connectors::ibmmq::IbmMqSink;

use super::kill_rejoin::{
    boot, cluster_nodes, kill_round, record_consumers, steady_phase, SoakSettings,
    CHECKPOINTS_COMPLETED,
};
use super::multi_writer_sinks::ibmmq::IbmMq;
use super::multi_writer_sinks::{block_on, wait_for_delivery, Destination as _};
use super::{
    scoped_soak_storage_url, soak_run_id, wait_for_converged_durable_checkpoint, JoinDelivery,
    ResolvedExecutable, NODES,
};

const PUBLISH_TICK: Duration = Duration::from_millis(25);
/// Most ids one put carries, so a backlog after a failure drains in bounded units.
const PUBLISH_BATCH: u64 = 500;
const PUBLISH_ATTEMPTS: u32 = 40;

#[test]
#[ignore = "spawns 3 real laminardb processes with S3 and an IBM MQ queue manager"]
fn three_node_ibmmq_source_kill9_soak() {
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let settings = SoakSettings::from_environment(JoinDelivery::AtLeastOnce);
    let ibmmq = IbmMq::from_environment();
    let run_id = soak_run_id();
    let checkpoint_url =
        scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "checkpoints");
    let (input, output) = (IbmMq::source_queue(&run_id), ibmmq.name(&run_id));
    ibmmq.create(&input);
    ibmmq.create(&output);
    let mut oracle = ibmmq.spawn_oracle(&output);
    let mut publisher = Publisher::spawn(ibmmq.client_config(&input, "json"), settings.rps);

    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir: PathBuf = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive soak log directory");
    eprintln!("soak: IBM MQ source node logs in {}", log_dir.display());
    let mut nodes = cluster_nodes(
        &executable,
        dir.path(),
        &log_dir,
        &settings,
        &checkpoint_url,
        "",
        &workload(&ibmmq, &input, &output),
    );
    let mut live = || {
        publisher.assert_running();
        oracle.assert_running();
    };

    let mut consumers = BTreeSet::new();
    let mut latest = boot(&mut nodes, &mut live, &settings, CHECKPOINTS_COMPLETED);
    record_consumers(&nodes, &mut consumers);
    let mut killed_followers = BTreeSet::new();
    for round in 0..settings.kills {
        (_, latest) = kill_round(
            &mut nodes,
            &mut live,
            round,
            &mut killed_followers,
            &settings,
            CHECKPOINTS_COMPLETED,
            latest,
        );
        record_consumers(&nodes, &mut consumers);
    }
    steady_phase(
        &mut nodes,
        &mut live,
        settings.steady,
        CHECKPOINTS_COMPLETED,
    );
    wait_for_converged_durable_checkpoint(
        &mut nodes,
        Instant::now() + settings.recovery_ceiling,
        "steady durable progress",
        Some(latest),
    );
    record_consumers(&nodes, &mut consumers);

    let published = publisher.stop();
    let observed = wait_for_delivery(oracle, published, settings.recovery_ceiling);
    let unexpected: Vec<u64> = observed.range(published..).map(|(id, _)| *id).collect();
    assert!(
        unexpected.is_empty(),
        "the output holds {} ids that were never published, first {:?}",
        unexpected.len(),
        &unexpected[..unexpected.len().min(8)]
    );
    assert!(
        consumers.len() > 1,
        "only node {consumers:?} ever consumed the queue: the source never moved"
    );
    let duplicates: u64 = observed.values().map(|count| u64::from(count - 1)).sum();
    eprintln!(
        "soak: IBM MQ source across {NODES} nodes: published={published} lost=0 \
         duplicates={duplicates} consumers={consumers:?} kills={} (followers \
         {killed_followers:?})",
        settings.kills
    );
    drop(nodes);
    ibmmq.delete(&input);
    ibmmq.delete(&output);
}

fn workload(ibmmq: &IbmMq, input: &str, output: &str) -> String {
    format!(
        r#"
[[source]]
name = "queue_input"
connector = "ibmmq"
format = "json"
[source.properties]
{source}
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "payload"
type = "VARCHAR"
nullable = false

# A cluster sink may not read a source directly.
[[pipeline]]
name = "queue_forwarded"
sql = "SELECT id, payload FROM queue_input"

[[sink]]
name = "queue_output"
pipeline = "queue_forwarded"
connector = "ibmmq"
format = "json"
[sink.properties]
{sink}"#,
        source = ibmmq.queue_properties(input),
        sink = ibmmq.sink_properties(output),
    )
}

fn ids_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

/// Paced publisher of ids `0..`. A put is retried until it commits, so the committed ids are the
/// prefix `0..published` and no later id reaches the queue; a retry may put an id twice.
struct Publisher {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<u64>>,
}

impl Publisher {
    fn spawn(config: ConnectorConfig, rps: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || block_on(publish(&config, rps, &stop)))
        };
        Self {
            stop,
            handle: Some(handle),
        }
    }

    fn assert_running(&mut self) {
        if !self.handle.as_ref().is_some_and(JoinHandle::is_finished) {
            return;
        }
        match self.handle.take().expect("publisher handle").join() {
            Ok(_) => panic!("IBM MQ publisher stopped before the soak completed"),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Release);
        self.handle
            .take()
            .expect("IBM MQ publisher was already stopped")
            .join()
            .expect("IBM MQ publisher thread failed")
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn ids(range: std::ops::Range<u64>) -> RecordBatch {
    let ids: Vec<i64> = range
        .clone()
        .map(|id| i64::try_from(id).expect("id fits in i64"))
        .collect();
    let payloads: Vec<String> = range.map(|id| format!("p{id}")).collect();
    RecordBatch::try_new(
        ids_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(payloads)),
        ],
    )
    .expect("id batch")
}

/// Put the ids due at `rps`; the sink commits each put before it returns.
async fn publish(config: &ConnectorConfig, rps: u64, stop: &AtomicBool) -> u64 {
    let start = tokio::time::Instant::now();
    let mut sink: Option<IbmMqSink> = None;
    let mut published = 0_u64;
    let mut failures = 0_u32;
    // A failed put is retried even after the stop, so the committed ids stay a prefix.
    while failures > 0 || !stop.load(Ordering::Acquire) {
        tokio::time::sleep(PUBLISH_TICK).await;
        let due = (start.elapsed().as_secs_f64() * rps as f64) as u64;
        if due <= published {
            continue;
        }
        let range = published..due.min(published + PUBLISH_BATCH);
        let outcome = async {
            if sink.is_none() {
                let mut opened = IbmMqSink::new(ids_schema(), None);
                opened.open(config).await?;
                sink = Some(opened);
            }
            let open = sink.as_mut().expect("the sink was just opened");
            open.write_batch(&ids(range.clone())).await
        }
        .await;
        match outcome {
            Ok(_) => {
                published = range.end;
                failures = 0;
            }
            Err(error) => {
                if let Some(mut failed) = sink.take() {
                    let _ = failed.close().await;
                }
                failures += 1;
                assert!(
                    failures < PUBLISH_ATTEMPTS,
                    "IBM MQ never committed ids {range:?}: {error}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    if let Some(mut open) = sink {
        let _ = open.close().await;
    }
    published
}
