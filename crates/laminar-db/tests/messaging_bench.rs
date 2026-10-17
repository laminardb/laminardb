//! Throughput and latency harness for the RabbitMQ and IBM MQ connectors (ignored; release only).
//!
//! An open-loop publisher offers `rate` messages per second and stamps each message with its
//! *scheduled* send time on one monotonic clock shared with the consumer, so latency includes
//! queueing behind a slow pipeline instead of hiding it (no coordinated omission). The pipeline
//! is `source -> stream -> sink` through embedded SQL with periodic checkpoints; the consumer reads
//! the sink's queue. Peak RSS and CPU time come from `ps` and cover the whole test process:
//! pipeline, publisher and consumer.
//!
//! ```text
//! cargo test --release -p laminar-db --no-default-features --features rabbitmq,ibmmq \
//!   --test messaging_bench -- --ignored --nocapture --test-threads=1
//! ```
//! `LAMINAR_BENCH_SECONDS` (default 15), `LAMINAR_BENCH_RABBITMQ_RATE` (default 5000),
//! `LAMINAR_BENCH_IBMMQ_RATE` (default 1000). The IBM MQ runs need the same environment as
//! `ibmmq_e2e`.

#![cfg(any(feature = "rabbitmq", feature = "ibmmq"))]
#![allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use laminar_db::{DeliveryGuarantee, LaminarDB};
use tokio::sync::Mutex;

fn base() -> Instant {
    static BASE: OnceLock<Instant> = OnceLock::new();
    *BASE.get_or_init(Instant::now)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[derive(Clone, Copy, Debug)]
enum Codec {
    Json,
    Raw,
}

/// One message: id, scheduled offset in nanoseconds, and padding to `size` bytes.
fn encode(codec: Codec, id: u64, scheduled_ns: u64, size: usize) -> Vec<u8> {
    match codec {
        Codec::Json => {
            let head = format!(r#"{{"id":{id},"t":{scheduled_ns},"pad":""#);
            let pad = size.saturating_sub(head.len() + 2);
            let mut body = head.into_bytes();
            body.extend(std::iter::repeat_n(b'x', pad));
            body.extend_from_slice(b"\"}");
            body
        }
        Codec::Raw => {
            let mut body = Vec::with_capacity(size.max(16));
            body.extend_from_slice(&id.to_le_bytes());
            body.extend_from_slice(&scheduled_ns.to_le_bytes());
            body.resize(size.max(16), 0xa5);
            body
        }
    }
}

fn decode(codec: Codec, body: &[u8]) -> (u64, u64) {
    match codec {
        Codec::Json => {
            let value: serde_json::Value = serde_json::from_slice(body).expect("json");
            (value["id"].as_u64().unwrap(), value["t"].as_u64().unwrap())
        }
        Codec::Raw => (
            u64::from_le_bytes(body[0..8].try_into().unwrap()),
            u64::from_le_bytes(body[8..16].try_into().unwrap()),
        ),
    }
}

#[derive(Default)]
struct Observed {
    latencies_us: Vec<u64>,
    ids: std::collections::BTreeSet<u64>,
    first: Option<Instant>,
    last: Option<Instant>,
}

impl Observed {
    fn record(&mut self, id: u64, scheduled_ns: u64) {
        let now = Instant::now();
        let elapsed = now.duration_since(base()).as_nanos() as u64;
        self.latencies_us
            .push(elapsed.saturating_sub(scheduled_ns) / 1_000);
        self.ids.insert(id);
        self.first.get_or_insert(now);
        self.last = Some(now);
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((sorted.len() as f64 - 1.0) * p).round() as usize]
}

/// Peak resident set (MiB) and CPU time (s) of this process, sampled with `ps`.
fn process_usage() -> (u64, f64) {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=,time=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps");
    let text = String::from_utf8_lossy(&output.stdout);
    let mut parts = text.split_whitespace();
    let rss = parts
        .next()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
        / 1024;
    let cpu = parts.next().map_or(0.0, |time| {
        time.split(':')
            .rev()
            .zip([1.0, 60.0, 3600.0])
            .map(|(part, scale)| part.parse::<f64>().unwrap_or(0.0) * scale)
            .sum()
    });
    (rss, cpu)
}

struct Report {
    /// A pipeline fault observed at shutdown, reported rather than aborting the matrix.
    fault: Option<String>,
    broker: &'static str,
    codec: Codec,
    size: usize,
    interval_ms: u64,
    offered: u64,
    sent: u64,
    observed: Observed,
    peak_rss: u64,
    cpu: f64,
}

impl Report {
    fn print(mut self) {
        self.observed.latencies_us.sort_unstable();
        let lat = &self.observed.latencies_us;
        let span = match (self.observed.first, self.observed.last) {
            (Some(first), Some(last)) => last.duration_since(first).as_secs_f64().max(0.001),
            _ => 0.001,
        };
        let received = self.observed.ids.len() as f64;
        println!(
            "| {} | {:?} | {} | {} ms | {} | {} | {:.0} | {:.0} | {:.1} | {:.1} | {:.1} | {:.1} | {} | {:.1} |",
            self.broker,
            self.codec,
            self.size,
            self.interval_ms,
            self.offered,
            self.sent,
            received / span,
            received / span,
            percentile(lat, 0.50) as f64 / 1000.0,
            percentile(lat, 0.99) as f64 / 1000.0,
            percentile(lat, 0.999) as f64 / 1000.0,
            lat.last().copied().unwrap_or(0) as f64 / 1000.0,
            self.peak_rss,
            self.cpu,
        );
        if let Some(fault) = &self.fault {
            // An embedded pipeline is unsupervised: after a fault the rest stays on the queue.
            println!(
                "|  | faulted after {} of {} messages: {fault} |",
                self.observed.ids.len(),
                self.sent
            );
            return;
        }
        assert_eq!(
            self.observed.ids.len() as u64,
            self.sent,
            "every sent message must arrive"
        );
    }
}

async fn open(storage: &std::path::Path, interval_ms: u64, password: &str) -> Arc<LaminarDB> {
    LaminarDB::builder()
        .storage_dir(storage)
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: Some(interval_ms),
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .config_var("BENCH_PASSWORD", password)
        .build()
        .await
        .expect("open database")
}

/// Shut the pipeline down and return its fault, if it faulted.
async fn stop(db: &LaminarDB) -> Option<String> {
    let fault = (db.pipeline_state() == "Faulted")
        .then(|| db.last_fault().unwrap_or_else(|| "Faulted".into()));
    let shutdown = db.shutdown().await;
    fault.or(shutdown.err().map(|error| error.to_string()))
}

fn ddl(codec: Codec, from: &str, into: &str) -> [String; 3] {
    let (columns, format, projection) = match codec {
        Codec::Json => ("(id BIGINT, t BIGINT, pad VARCHAR)", "JSON", "id, t, pad"),
        Codec::Raw => ("(value BYTEA)", "RAW", "value"),
    };
    [
        format!("CREATE SOURCE bench_in {columns} FROM {from} FORMAT {format}"),
        format!("CREATE STREAM bench_mid AS SELECT {projection} FROM bench_in"),
        format!("CREATE SINK bench_out FROM bench_mid INTO {into} FORMAT {format}"),
    ]
}

/// Sample peak RSS while `work` runs.
async fn sampled<T>(work: impl std::future::Future<Output = T>) -> (T, u64, f64) {
    let peak = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let sampler = {
        let peak = Arc::clone(&peak);
        tokio::spawn(async move {
            loop {
                let (rss, _) = process_usage();
                peak.fetch_max(rss, std::sync::atomic::Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
    };
    let (_, cpu_before) = process_usage();
    let result = work.await;
    let (_, cpu_after) = process_usage();
    sampler.abort();
    (
        result,
        peak.load(std::sync::atomic::Ordering::Relaxed),
        cpu_after - cpu_before,
    )
}

/// The full matrix; `LAMINAR_BENCH_CELL=json,16384,2000` runs one cell and
/// `LAMINAR_BENCH_QUICK=1` the JSON 1 KiB 300 ms cell. `LAMINAR_BENCH_LOG=1` prints warnings.
fn matrix() -> Vec<(Codec, usize, u64)> {
    if std::env::var("LAMINAR_BENCH_LOG").is_ok() {
        let _ = tracing_subscriber::fmt().with_env_filter("warn").try_init();
    }
    if let Ok(cell) = std::env::var("LAMINAR_BENCH_CELL") {
        let parts: Vec<&str> = cell.split(',').collect();
        let codec = if parts[0] == "raw" {
            Codec::Raw
        } else {
            Codec::Json
        };
        return vec![(codec, parts[1].parse().unwrap(), parts[2].parse().unwrap())];
    }
    if std::env::var("LAMINAR_BENCH_QUICK").is_ok() {
        return vec![(Codec::Json, 1024, 300)];
    }
    let mut cells = Vec::new();
    for codec in [Codec::Json, Codec::Raw] {
        for size in [128, 1024, 16 * 1024] {
            for interval_ms in [300, 2_000] {
                cells.push((codec, size, interval_ms));
            }
        }
    }
    cells
}

const HEADER: &str = "| broker | codec | bytes | checkpoint | offered/s | sent | msgs/s | rows/s | p50 ms | p99 ms | p99.9 ms | max ms | peak RSS MiB | CPU s |\n|---|---|---|---|---|---|---|---|---|---|---|---|---|---|";

#[cfg(feature = "rabbitmq")]
mod rabbitmq {
    use super::*;
    use futures_util::StreamExt;
    use lapin::options::{
        BasicConsumeOptions, BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions,
        QueueDeleteOptions,
    };
    use lapin::types::FieldTable;
    use lapin::{BasicProperties, Connection, ConnectionProperties};

    const ADMIN: &str = "amqp://guest:guest@127.0.0.1:5672/%2f";
    const OPTIONS: &str =
        "'url' = 'amqp://127.0.0.1:5672/%2f', 'username' = 'guest', 'password' = '${BENCH_PASSWORD}'";

    async fn channel() -> (Connection, lapin::Channel) {
        let connection = Connection::connect(ADMIN, ConnectionProperties::default())
            .await
            .unwrap();
        let channel = connection.create_channel().await.unwrap();
        (connection, channel)
    }

    async fn run(codec: Codec, size: usize, interval_ms: u64, rate: u64, seconds: u64) -> Report {
        let (admin_connection, admin) = channel().await;
        let tag = format!("{}.{size}.{interval_ms}.{codec:?}", std::process::id());
        let (input, output) = (format!("bench.in.{tag}"), format!("bench.out.{tag}"));
        for queue in [&input, &output] {
            admin
                .queue_declare(
                    queue.as_str().into(),
                    QueueDeclareOptions {
                        durable: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .unwrap();
        }
        let storage = tempfile::tempdir().unwrap();
        let db = open(storage.path(), interval_ms, "guest").await;
        let [source, stream, sink] = ddl(
            codec,
            &format!("RABBITMQ ({OPTIONS}, 'queue' = '{input}', 'prefetch.count' = '20000')"),
            &format!("RABBITMQ ({OPTIONS}, 'routing.key' = '{output}')"),
        );
        for statement in [source, stream, sink] {
            db.execute(&statement).await.unwrap();
        }
        db.start().await.unwrap();

        let observed = Arc::new(Mutex::new(Observed::default()));
        let consumer = {
            let observed = Arc::clone(&observed);
            let output = output.clone();
            tokio::spawn(async move {
                let (_connection, channel) = channel().await;
                let mut deliveries = channel
                    .basic_consume(
                        output.as_str().into(),
                        "".into(),
                        BasicConsumeOptions {
                            no_ack: true,
                            ..Default::default()
                        },
                        FieldTable::default(),
                    )
                    .await
                    .unwrap();
                while let Some(Ok(delivery)) = deliveries.next().await {
                    let (id, scheduled) = decode(codec, &delivery.data);
                    observed.lock().await.record(id, scheduled);
                }
            })
        };
        let ((sent, _), peak_rss, cpu) = sampled(async {
            let (publisher_connection, publisher) = channel().await;
            publisher
                .confirm_select(ConfirmSelectOptions::default())
                .await
                .unwrap();
            let start = Instant::now();
            let mut sent = 0_u64;
            let mut confirms = Vec::new();
            while start.elapsed() < Duration::from_secs(seconds) {
                let due = (start.elapsed().as_secs_f64() * rate as f64) as u64;
                while sent < due {
                    let scheduled = start + Duration::from_secs_f64(sent as f64 / rate as f64);
                    let scheduled_ns = scheduled.duration_since(base()).as_nanos() as u64;
                    let body = encode(codec, sent, scheduled_ns, size);
                    confirms.push(
                        publisher
                            .basic_publish(
                                "".into(),
                                input.as_str().into(),
                                BasicPublishOptions::default(),
                                &body,
                                BasicProperties::default().with_delivery_mode(2),
                            )
                            .await
                            .unwrap(),
                    );
                    sent += 1;
                }
                if confirms.len() > 2_000 {
                    for confirm in confirms.drain(..) {
                        assert!(confirm.await.unwrap().is_ack());
                    }
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            for confirm in confirms.drain(..) {
                assert!(confirm.await.unwrap().is_ack());
            }
            let drain = Instant::now();
            while (observed.lock().await.ids.len() as u64) < sent
                && drain.elapsed() < Duration::from_secs(60)
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            publisher_connection
                .close(200, "done".into())
                .await
                .unwrap();
            (sent, ())
        })
        .await;
        consumer.abort();
        let fault = stop(&db).await;
        for queue in [&input, &output] {
            admin
                .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
                .await
                .unwrap();
        }
        admin_connection.close(200, "done".into()).await.unwrap();
        let observed = std::mem::take(&mut *observed.lock().await);
        Report {
            fault,
            broker: "RabbitMQ 4.3.6 classic durable",
            codec,
            size,
            interval_ms,
            offered: rate,
            sent,
            observed,
            peak_rss,
            cpu,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "benchmark; needs the compose rabbitmq service"]
    async fn rabbitmq_end_to_end_latency_and_throughput() {
        let seconds = env_u64("LAMINAR_BENCH_SECONDS", 15);
        let rate = env_u64("LAMINAR_BENCH_RABBITMQ_RATE", 5_000);
        println!("{HEADER}");
        for (codec, size, interval_ms) in matrix() {
            run(codec, size, interval_ms, rate, seconds).await.print();
        }
    }
}

#[cfg(feature = "ibmmq")]
mod ibmmq {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::{BinaryArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use laminar_connectors::config::ConnectorConfig;
    use laminar_connectors::connector::{
        SinkConnector, SourceConnector, SourcePosition, SourceStart,
    };
    use laminar_connectors::ibmmq::{IbmMqSink, IbmMqSource};

    /// The compose `ibmmq` service's development application password.
    const PASSWORD: &str = "laminar-test-app";

    fn library() -> String {
        std::env::var("LAMINAR_TEST_IBMMQ_LIBRARY").expect("LAMINAR_TEST_IBMMQ_LIBRARY")
    }

    fn options() -> String {
        format!(
            "'client.library' = '{}', 'queue.manager' = 'QM1', 'channel' = 'DEV.APP.SVRCONN', \
             'connection.name' = '127.0.0.1(1414)', 'username' = 'app', \
             'password' = '${{BENCH_PASSWORD}}'",
            library()
        )
    }

    fn connector(queue: &str) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("ibmmq");
        let library = library();
        for (key, value) in [
            ("client.library", library.as_str()),
            ("queue.manager", "QM1"),
            ("channel", "DEV.APP.SVRCONN"),
            ("connection.name", "127.0.0.1(1414)"),
            ("username", "app"),
            ("password", PASSWORD),
            ("queue", queue),
            ("format", "raw"),
        ] {
            config.set(key, value);
        }
        config
    }

    fn mqsc(commands: &str) {
        let container =
            std::env::var("LAMINAR_TEST_IBMMQ_CONTAINER").expect("LAMINAR_TEST_IBMMQ_CONTAINER");
        let mut child = std::process::Command::new("docker")
            .args(["exec", "-i", &container, "runmqsc", "QM1"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        use std::io::Write;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(commands.as_bytes())
            .unwrap();
        child.wait().unwrap();
    }

    async fn run(codec: Codec, size: usize, interval_ms: u64, rate: u64, seconds: u64) -> Report {
        let tag = format!("{}.{}.{}", std::process::id() % 10_000, size, interval_ms);
        let (input, output) = (
            format!("DEV.BENCH.IN.{tag}"),
            format!("DEV.BENCH.OUT.{tag}"),
        );
        mqsc(&format!(
            "DEFINE QLOCAL('{input}') MAXDEPTH(999999999) MAXMSGL(104857600) REPLACE\n\
             DEFINE QLOCAL('{output}') MAXDEPTH(999999999) MAXMSGL(104857600) REPLACE\n"
        ));
        let storage = tempfile::tempdir().unwrap();
        let db = open(storage.path(), interval_ms, PASSWORD).await;
        let options = options();
        let [source, stream, sink] = ddl(
            codec,
            &format!("IBMMQ ({options}, 'queue' = '{input}', 'wait.interval.ms' = '20', 'max.uow.messages' = '50000')"),
            &format!("IBMMQ ({options}, 'queue' = '{output}')"),
        );
        for statement in [source, stream, sink] {
            db.execute(&statement).await.unwrap();
        }
        db.start().await.unwrap();

        let observed = Arc::new(Mutex::new(Observed::default()));
        let consumer = {
            let observed = Arc::clone(&observed);
            let mut config = connector(&output);
            config.set("wait.interval.ms", "20");
            tokio::spawn(async move {
                // Small polls keep each oracle unit of work short; a failed generation (for
                // example one the queue manager rolled back for log space) starts again.
                loop {
                    let schema = Arc::new(Schema::new(vec![Field::new(
                        "value",
                        DataType::Binary,
                        false,
                    )]));
                    let mut source = IbmMqSource::new(schema, None);
                    let request = SourceStart::new(
                        config.clone(),
                        SourcePosition::Initial,
                        DeliveryGuarantee::AtLeastOnce,
                    )
                    .unwrap();
                    if source.start(request).await.is_err() {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                    loop {
                        match source.poll_batch(200).await {
                            Ok(Some(batch)) => {
                                let mut observed = observed.lock().await;
                                for body in
                                    batch.records.column(0).as_binary::<i32>().iter().flatten()
                                {
                                    let (id, scheduled) = decode(codec, body);
                                    observed.record(id, scheduled);
                                }
                                drop(observed);
                                let cut = source.checkpoint();
                                if source.notify_epoch_committed(1, &cut).await.is_err() {
                                    break;
                                }
                            }
                            Ok(None) => {}
                            Err(_) => break,
                        }
                    }
                    let _ = source.close().await;
                }
            })
        };
        let ((sent, _), peak_rss, cpu) = sampled(async {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Binary,
                false,
            )]));
            let mut publisher = IbmMqSink::new(schema.clone(), None);
            let mut config = connector(&input);
            config.set("max.uow.messages", "5000");
            publisher.open(&config).await.unwrap();
            let start = Instant::now();
            let mut sent = 0_u64;
            while start.elapsed() < Duration::from_secs(seconds) {
                let due = (start.elapsed().as_secs_f64() * rate as f64) as u64;
                if sent < due {
                    let bodies: Vec<Vec<u8>> = (sent..due)
                        .map(|id| {
                            let scheduled =
                                start + Duration::from_secs_f64(id as f64 / rate as f64);
                            encode(
                                codec,
                                id,
                                scheduled.duration_since(base()).as_nanos() as u64,
                                size,
                            )
                        })
                        .collect();
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(BinaryArray::from(
                            bodies.iter().map(Vec::as_slice).collect::<Vec<_>>(),
                        ))],
                    )
                    .unwrap();
                    publisher.write_batch(&batch).await.unwrap();
                    sent = due;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            publisher.close().await.unwrap();
            let drain = Instant::now();
            while (observed.lock().await.ids.len() as u64) < sent
                && drain.elapsed() < Duration::from_secs(120)
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            (sent, ())
        })
        .await;
        consumer.abort();
        let fault = stop(&db).await;
        mqsc(&format!(
            "DELETE QLOCAL('{input}') PURGE\nDELETE QLOCAL('{output}') PURGE\n"
        ));
        let observed = std::mem::take(&mut *observed.lock().await);
        Report {
            fault,
            broker: "IBM MQ 9.4.0.26 (Rosetta) persistent",
            codec,
            size,
            interval_ms,
            offered: rate,
            sent,
            observed,
            peak_rss,
            cpu,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "benchmark; needs the compose ibmmq service and an IBM MQ client"]
    async fn ibmmq_end_to_end_latency_and_throughput() {
        let seconds = env_u64("LAMINAR_BENCH_SECONDS", 15);
        let rate = env_u64("LAMINAR_BENCH_IBMMQ_RATE", 1_000);
        println!("{HEADER}");
        for (codec, size, interval_ms) in matrix() {
            run(codec, size, interval_ms, rate, seconds).await.print();
        }
    }
}
