//! Standalone-server RabbitMQ failure tests: a steady confirmed publisher, injected faults, and an
//! independent oracle over every published id.
//!
//! - `rabbitmq_at_least_once_survives_repeated_hard_kills`: `SIGKILL`s of the `laminardb` process
//!   (needs `docker compose -f tests/docker/compose.yml up -d --wait rabbitmq`).
//! - `rabbitmq_at_least_once_survives_broker_restarts`: `docker restart` of that broker.
//! - `rabbitmq_quorum_survives_broker_node_loss`: `docker kill` of the cluster node the pipeline is
//!   connected to (needs `tests/docker/rabbitmq-cluster-compose.yml`); the restarted pipeline must
//!   reach the next `url` endpoint.
//!
//! Run with `cargo test --release -p laminar-server --no-default-features --features rabbitmq \
//!  --test rabbitmq_kill -- --ignored --nocapture --test-threads=1`. `LAMINAR_RABBITMQ_KILL_SECONDS`
//! (default 60), `LAMINAR_RABBITMQ_KILL_EVERY_MS` (default 7000), `LAMINAR_RABBITMQ_KILL_RATE`
//! (messages/s, default 500), and `LAMINAR_RABBITMQ_KILL_QUEUE_TYPE` (`classic` or `quorum`) size
//! the runs. No published message may be missing from the output; duplicates are permitted and
//! reported.

#![cfg(feature = "rabbitmq")]

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ConfirmSelectOptions,
    QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use tokio::sync::Mutex;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// What the scenario breaks while the pipeline runs.
#[derive(Clone, Copy)]
enum Fault {
    /// `SIGKILL` the `laminardb` process every interval and start it again.
    KillServer,
    /// `docker restart` the named broker container every interval.
    RestartBroker(&'static str),
    /// `docker kill` the named cluster node once, then start it again after the run.
    KillNode(&'static str),
}

struct Scenario {
    queue_type: String,
    /// Endpoints the pipeline uses, tried in order at each start.
    pipeline_urls: &'static str,
    /// Endpoint the test publisher and oracle use; it is never faulted.
    admin_url: &'static str,
    fault: Fault,
}

fn server_config(root: &Path, scenario: &Scenario, input: &str, output: &str) -> String {
    let checkpoint = root
        .join("checkpoints")
        .to_string_lossy()
        .replace('\\', "/");
    let checkpoint_url = if checkpoint.starts_with('/') {
        format!("file://{checkpoint}")
    } else {
        format!("file:///{checkpoint}")
    };
    let options = format!(
        "'url' = '{}', 'username' = 'guest', 'password' = '$${{KILL_RABBITMQ_PASSWORD}}', \
         'connect.timeout.ms' = '3000'",
        scenario.pipeline_urls
    );
    let sql = format!(
        "CREATE SOURCE events (id BIGINT, payload VARCHAR) FROM RABBITMQ ({options}, \
         'queue' = '{input}', 'prefetch.count' = '5000') FORMAT JSON;
         CREATE STREAM forwarded AS SELECT id, payload FROM events;
         CREATE SINK out FROM forwarded INTO RABBITMQ ({options}, 'routing.key' = '{output}') \
         FORMAT JSON;"
    );
    // A broker outage outlasts the default restart budget (5 in 60 s), so widen it.
    format!(
        "sql = {sql}\n[server]\nbind = {bind}\ndelivery = \"at_least_once\"\n\
         [checkpoint]\nurl = {checkpoint_url}\ninterval = \"300ms\"\n\
         [supervision]\nmax_restarts = 100\nwindow_secs = 60\ninitial_backoff_ms = 200\n\
         max_backoff_secs = 2\n",
        sql = serde_json::to_string(&sql).unwrap(),
        bind = serde_json::to_string(&format!("127.0.0.1:{}", free_port())).unwrap(),
        checkpoint_url = serde_json::to_string(&checkpoint_url).unwrap(),
    )
}

fn spawn_server(config: &Path, log: &Path) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .unwrap();
    Command::new(env!("CARGO_BIN_EXE_laminardb"))
        .arg("--config")
        .arg(config)
        .args(["--log-level", "warn"])
        .env("KILL_RABBITMQ_PASSWORD", "guest")
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("start laminardb")
}

fn docker(args: &[&str]) {
    let output = Command::new("docker").args(args).output().expect("docker");
    assert!(
        output.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Connect, retrying while a broker restarts.
async fn channel(url: &str, deadline: Instant) -> Channel {
    loop {
        if let Ok(Ok(connection)) = tokio::time::timeout(
            Duration::from_secs(3),
            Connection::connect(url, ConnectionProperties::default()),
        )
        .await
        {
            if let Ok(channel) = connection.create_channel().await {
                std::mem::forget(connection);
                return channel;
            }
        }
        assert!(Instant::now() < deadline, "{url} unreachable");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn declare(channel: &Channel, queue: &str, kind: &str) {
    let mut arguments = FieldTable::default();
    arguments.insert("x-queue-type".into(), AMQPValue::LongString(kind.into()));
    channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            arguments,
        )
        .await
        .expect("declare");
}

async fn ready(channel: &Channel, queue: &str) -> u32 {
    channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap()
        .message_count()
}

/// Publish ids `0..` at `rate` per second until `run` ends; a broker-confirmed id counts as
/// published. A failed publish is retried on a new connection (input duplicates are allowed).
async fn publish(url: &'static str, queue: String, run: Duration, rate: u64) -> i64 {
    let started = Instant::now();
    let deadline = started + run + Duration::from_secs(60);
    let mut channel = channel(url, deadline).await;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let mut published = 0_i64;
    while started.elapsed() < run {
        let due = i64::try_from(started.elapsed().as_millis() as u64 * rate / 1_000).unwrap();
        while published < due {
            let payload = format!(r#"{{"id":{published},"payload":"p{published}"}}"#);
            let confirmed = async {
                channel
                    .basic_publish(
                        "".into(),
                        queue.as_str().into(),
                        BasicPublishOptions::default(),
                        payload.as_bytes(),
                        BasicProperties::default().with_delivery_mode(2),
                    )
                    .await?
                    .await
            };
            match tokio::time::timeout(Duration::from_secs(5), confirmed).await {
                Ok(Ok(confirmation)) if confirmation.is_ack() => published += 1,
                _ => {
                    channel = self::channel(url, deadline).await;
                    channel
                        .confirm_select(ConfirmSelectOptions::default())
                        .await
                        .unwrap();
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    published
}

/// Record every output id, acknowledging after recording and reconnecting after a broker fault.
async fn observe(url: &'static str, queue: String, observed: Arc<Mutex<BTreeMap<i64, u32>>>) {
    loop {
        let channel = channel(url, Instant::now() + Duration::from_secs(600)).await;
        let Ok(mut consumer) = channel
            .basic_consume(
                queue.as_str().into(),
                "".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
        else {
            continue;
        };
        while let Some(Ok(delivery)) = consumer.next().await {
            let value: serde_json::Value = serde_json::from_slice(&delivery.data).unwrap();
            *observed
                .lock()
                .await
                .entry(value["id"].as_i64().unwrap())
                .or_default() += 1;
            if delivery.ack(BasicAckOptions::default()).await.is_err() {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn run(scenario: Scenario) {
    let duration = Duration::from_secs(env_u64("LAMINAR_RABBITMQ_KILL_SECONDS", 60));
    let every = Duration::from_millis(env_u64("LAMINAR_RABBITMQ_KILL_EVERY_MS", 7_000));
    let rate = env_u64("LAMINAR_RABBITMQ_KILL_RATE", 500).max(1);
    let tag = format!("{}.{}", std::process::id(), free_port());
    let (input, output) = (
        format!("laminar.kill.in.{tag}"),
        format!("laminar.kill.out.{tag}"),
    );
    let admin = channel(scenario.admin_url, Instant::now() + Duration::from_secs(30)).await;
    declare(&admin, &input, &scenario.queue_type).await;
    declare(&admin, &output, &scenario.queue_type).await;

    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("server.toml");
    let log = root.path().join("server.log");
    std::fs::write(
        &config,
        server_config(root.path(), &scenario, &input, &output),
    )
    .unwrap();
    let observed: Arc<Mutex<BTreeMap<i64, u32>>> = Arc::default();
    let reader = tokio::spawn(observe(
        scenario.admin_url,
        output.clone(),
        Arc::clone(&observed),
    ));
    let publisher = tokio::spawn(publish(scenario.admin_url, input.clone(), duration, rate));

    let mut server = spawn_server(&config, &log);
    let mut faults = 0;
    let started = Instant::now();
    while started.elapsed() < duration {
        tokio::time::sleep(every).await;
        match scenario.fault {
            Fault::KillServer => {
                server.kill().expect("SIGKILL laminardb");
                server.wait().unwrap();
                server = spawn_server(&config, &log);
            }
            Fault::RestartBroker(container) => docker(&["restart", container]),
            Fault::KillNode(container) if faults == 0 => docker(&["kill", container]),
            Fault::KillNode(_) => continue,
        }
        faults += 1;
    }
    let published = publisher.await.unwrap();

    let catch_up = Instant::now();
    loop {
        let missing: Vec<i64> = {
            let observed = observed.lock().await;
            (0..published)
                .filter(|id| !observed.contains_key(id))
                .take(5)
                .collect()
        };
        if missing.is_empty() {
            break;
        }
        assert!(
            catch_up.elapsed() < Duration::from_secs(180),
            "published ids such as {missing:?} (of {published}) never reached the output after \
             {faults} faults; see {}",
            log.display()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let catch_up = catch_up.elapsed();
    let drained = Instant::now();
    let admin = channel(scenario.admin_url, Instant::now() + Duration::from_secs(30)).await;
    while ready(&admin, &input).await > 0 && drained.elapsed() < Duration::from_secs(30) {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    server.kill().unwrap();
    server.wait().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut requeued = ready(&admin, &input).await;
    while requeued > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        requeued = ready(&admin, &input).await;
    }
    reader.abort();
    if let Fault::KillNode(container) = scenario.fault {
        docker(&["start", container]);
    }
    let observed = observed.lock().await;
    let duplicates: u32 = observed.values().map(|count| count - 1).sum();
    let unexpected = observed.keys().filter(|id| **id >= published).count();
    println!(
        "rabbitmq fault test: queue_type={} published={published} faults={faults} \
         duplicates={duplicates} catch_up_ms={} uncommitted_after_final_kill={requeued}",
        scenario.queue_type,
        catch_up.as_millis()
    );
    assert_eq!(
        unexpected, 0,
        "output contains ids that were never published"
    );
    assert_eq!(
        requeued, 0,
        "inputs stayed unacknowledged past several checkpoints"
    );
    for queue in [&input, &output] {
        admin
            .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
            .await
            .unwrap();
    }
}

fn queue_type() -> String {
    std::env::var("LAMINAR_RABBITMQ_KILL_QUEUE_TYPE").unwrap_or_else(|_| "classic".into())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone-server kill test; needs the compose rabbitmq service"]
async fn rabbitmq_at_least_once_survives_repeated_hard_kills() {
    run(Scenario {
        queue_type: queue_type(),
        pipeline_urls: "amqp://127.0.0.1:5672/%2f",
        admin_url: "amqp://guest:guest@127.0.0.1:5672/%2f",
        fault: Fault::KillServer,
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "restarts the compose rabbitmq broker"]
async fn rabbitmq_at_least_once_survives_broker_restarts() {
    run(Scenario {
        queue_type: queue_type(),
        pipeline_urls: "amqp://127.0.0.1:5672/%2f",
        admin_url: "amqp://guest:guest@127.0.0.1:5672/%2f",
        fault: Fault::RestartBroker("laminardb-rabbitmq"),
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "kills a node of tests/docker/rabbitmq-cluster-compose.yml"]
async fn rabbitmq_quorum_survives_broker_node_loss() {
    run(Scenario {
        queue_type: "quorum".into(),
        pipeline_urls:
            "amqp://127.0.0.1:5681/%2f,amqp://127.0.0.1:5682/%2f,amqp://127.0.0.1:5683/%2f",
        admin_url: "amqp://guest:guest@127.0.0.1:5683/%2f",
        fault: Fault::KillNode("laminardb-rmq1"),
    })
    .await;
}
