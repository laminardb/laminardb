//! Three-node at-least-once soak for a RabbitMQ source, which one node at a time consumes. A
//! confirmed publisher sends ids `0..` to a durable queue. The node that owns the source's home
//! vnode consumes it and acknowledges what each committed checkpoint holds; a stream forwards the
//! rows to a RabbitMQ sink, and an independent oracle drains the output queue. The observed leader
//! and each follower are killed inside a checkpoint and rejoin (`kill_rejoin`): when the consuming
//! node dies the broker returns its unacknowledged deliveries to the queue and a survivor takes the
//! source from the committed cut, and when the source moves at a rejoin the donor acknowledges the
//! handoff cut before it closes. Two nodes consuming at once would claim different deliveries in
//! one cut, which fails the checkpoint.
//!
//! Every confirmed id must reach the output and no other id may; duplicates are reported. More
//! than one node must have consumed the queue, and every node must keep committing checkpoints.
//!
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,rabbitmq --test cluster_soak \
//!   rabbitmq_source::three_node_rabbitmq_source_kill9_soak -- --ignored --exact --nocapture
//! ```
//!
//! It reads the shared `LAMINAR_SOAK_CHECKPOINT_URL`, `LAMINAR_SOAK_S3_*`,
//! `LAMINAR_SOAK_KEY_GROUPS`, `LAMINAR_SOAK_INTERVAL_MS`, `LAMINAR_SOAK_MAX_RECOVERY_MS` and
//! `LAMINAR_SOAK_DISCOVERY` knobs, plus `LAMINAR_SOAK_KILLS` (default 4), `LAMINAR_SOAK_SECONDS`
//! (steady phase after the kills, default 30), `LAMINAR_SOAK_RPS` (default 400) and the RabbitMQ
//! settings of the RabbitMQ sink soak.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use lapin::options::{BasicPublishOptions, ConfirmSelectOptions};
use lapin::uri::AMQPUri;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};

use super::kill_rejoin::{
    boot, cluster_nodes, kill_round, record_consumers, steady_phase, SoakSettings,
    CHECKPOINTS_COMPLETED,
};
use super::multi_writer_sinks::rabbitmq::RabbitMq;
use super::multi_writer_sinks::{block_on, wait_for_delivery, Destination as _};
use super::{
    scoped_soak_storage_url, soak_run_id, wait_for_converged_durable_checkpoint, JoinDelivery,
    ResolvedExecutable, NODES,
};

const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);
const PUBLISH_ATTEMPTS: u32 = 40;

#[test]
#[ignore = "spawns 3 real laminardb processes with S3 and RabbitMQ"]
fn three_node_rabbitmq_source_kill9_soak() {
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let settings = SoakSettings::from_environment(JoinDelivery::AtLeastOnce);
    let rabbitmq = RabbitMq::from_environment();
    let run_id = soak_run_id();
    let checkpoint_url =
        scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "checkpoints");
    let (input, output) = (format!("soak.source.{run_id}"), rabbitmq.name(&run_id));
    rabbitmq.create(&input);
    rabbitmq.create(&output);
    let mut oracle = rabbitmq.spawn_oracle(&output);
    let mut publisher = Publisher::spawn(rabbitmq.uri(), input.clone(), settings.rps);

    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir: PathBuf = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive soak log directory");
    eprintln!("soak: RabbitMQ source node logs in {}", log_dir.display());
    let mut nodes = cluster_nodes(
        &executable,
        dir.path(),
        &log_dir,
        &settings,
        &checkpoint_url,
        "",
        &workload(&rabbitmq, &input, &output),
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
    let observed = wait_for_delivery(oracle, 0..published, settings.recovery_ceiling);
    let unexpected: Vec<u64> = observed
        .keys()
        .copied()
        .filter(|id| *id >= published)
        .collect();
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
        "soak: RabbitMQ source across {NODES} nodes: published={published} lost=0 \
         duplicates={duplicates} consumers={consumers:?} kills={} (followers \
         {killed_followers:?})",
        settings.kills
    );
    drop(nodes);
    rabbitmq.delete(&input);
    rabbitmq.delete(&output);
}

fn workload(rabbitmq: &RabbitMq, input: &str, output: &str) -> String {
    format!(
        r#"
[[source]]
name = "queue_input"
connector = "rabbitmq"
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
connector = "rabbitmq"
format = "json"
[sink.properties]
{sink}"#,
        source = rabbitmq.source_properties(input),
        sink = rabbitmq.sink_properties(output),
    )
}

/// Paced publisher of ids `0..`. An id is retried until the broker confirms it, so the confirmed
/// ids are the prefix `0..published` and no later id reaches the queue; a retry may enqueue an id
/// twice.
struct Publisher {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<u64>>,
}

impl Publisher {
    fn spawn(uri: AMQPUri, queue: String, rps: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || block_on(publish(&uri, &queue, rps, &stop)))
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
            Ok(_) => panic!("RabbitMQ publisher stopped before the soak completed"),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Release);
        self.handle
            .take()
            .expect("RabbitMQ publisher was already stopped")
            .join()
            .expect("RabbitMQ publisher thread failed")
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

/// A channel in confirm mode; dropping the session closes its connection.
struct Session {
    _connection: Connection,
    channel: Channel,
}

async fn open_session(uri: &AMQPUri) -> Result<Session, String> {
    let open = async {
        let connection =
            Connection::connect_uri(uri.clone(), ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await?;
        Ok::<_, lapin::Error>(Session {
            _connection: connection,
            channel,
        })
    };
    tokio::time::timeout(PUBLISH_TIMEOUT, open)
        .await
        .map_err(|_| "connect timed out".to_owned())?
        .map_err(|error| error.to_string())
}

/// Publish one persistent message and wait for the broker to confirm it.
async fn confirm(session: &Session, queue: &str, payload: &[u8]) -> Result<(), String> {
    let publish = async {
        session
            .channel
            .basic_publish(
                "".into(),
                queue.into(),
                BasicPublishOptions::default(),
                payload,
                BasicProperties::default().with_delivery_mode(2),
            )
            .await?
            .await
    };
    let confirmation = tokio::time::timeout(PUBLISH_TIMEOUT, publish)
        .await
        .map_err(|_| "confirm timed out".to_owned())?
        .map_err(|error| error.to_string())?;
    if confirmation.is_ack() {
        Ok(())
    } else {
        Err(format!("the broker refused the message: {confirmation:?}"))
    }
}

async fn publish(uri: &AMQPUri, queue: &str, rps: u64, stop: &AtomicBool) -> u64 {
    let start = tokio::time::Instant::now();
    let mut session = None;
    let mut published = 0_u64;
    let mut failures = 0_u32;
    // A failed id is retried even after the stop, so the confirmed ids stay a prefix.
    while failures > 0 || !stop.load(Ordering::Acquire) {
        tokio::time::sleep_until(start + Duration::from_secs_f64(published as f64 / rps as f64))
            .await;
        let payload = format!(r#"{{"id":{published},"payload":"p{published}"}}"#);
        let open = match session.take() {
            Some(open) => Ok(open),
            None => open_session(uri).await,
        };
        let outcome = match open {
            Ok(open) => {
                let outcome = confirm(&open, queue, payload.as_bytes()).await;
                if outcome.is_ok() {
                    session = Some(open);
                }
                outcome
            }
            Err(error) => Err(error),
        };
        match outcome {
            Ok(()) => {
                published += 1;
                failures = 0;
            }
            Err(error) => {
                failures += 1;
                assert!(
                    failures < PUBLISH_ATTEMPTS,
                    "RabbitMQ never confirmed id {published}: {error}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    published
}
