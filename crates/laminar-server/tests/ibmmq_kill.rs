//! Standalone-server IBM MQ hard-kill test: a steady publisher, repeated `SIGKILL`s of the
//! `laminardb` process, and an independent oracle over every published id.
//!
//! Needs the `tests/docker/compose.yml` service `ibmmq` and an IBM MQ client library. Run with
//! `LAMINAR_TEST_IBMMQ_LIBRARY=/opt/mqm/lib64/libmqm_r.so LAMINAR_TEST_IBMMQ_CONTAINER=laminardb-ibmmq
//!  cargo test --release -p laminar-server --no-default-features --features ibmmq \
//!  --test ibmmq_kill -- --ignored --nocapture`. `LAMINAR_IBMMQ_KILL_SECONDS` (default 60),
//! `LAMINAR_IBMMQ_KILL_EVERY_MS` (default 7000), and `LAMINAR_IBMMQ_KILL_RATE` (messages/s,
//! default 200) size the run.
//!
//! The asserted failure model is process loss with a live queue manager: the killed client's
//! connection breaks, the queue manager backs out its unit of work, and no published message may
//! be missing from the output; duplicates are permitted and reported.

#![cfg(feature = "ibmmq")]

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    DeliveryGuarantee, SinkConnector, SourceConnector, SourcePosition, SourceStart,
};
use laminar_connectors::ibmmq::{IbmMqSink, IbmMqSource};
use tokio::sync::Mutex;

const PASSWORD: &str = "laminar-test-app";

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn mqsc(container: &str, commands: &str) -> String {
    let mut child = Command::new("docker")
        .args(["exec", "-i", container, "runmqsc", "QM1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("runmqsc");
    {
        use std::io::Write;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(commands.as_bytes())
            .unwrap();
    }
    String::from_utf8_lossy(&child.wait_with_output().unwrap().stdout).into_owned()
}

fn depth(container: &str, queue: &str) -> u32 {
    mqsc(container, &format!("DISPLAY QLOCAL('{queue}') CURDEPTH\n"))
        .split("CURDEPTH(")
        .nth(1)
        .and_then(|rest| rest.split(')').next())
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn connector(library: &str, queue: &str) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("ibmmq");
    for (key, value) in [
        ("client.library", library),
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

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn server_config(root: &Path, library: &str, input: &str, output: &str) -> String {
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
        "'client.library' = '{library}', 'queue.manager' = 'QM1', 'channel' = 'DEV.APP.SVRCONN', \
         'connection.name' = '127.0.0.1(1414)', 'username' = 'app', \
         'password' = '$${{KILL_IBMMQ_PASSWORD}}'"
    );
    let sql = format!(
        "CREATE SOURCE events (id BIGINT, payload VARCHAR) FROM IBMMQ ({options}, \
         'queue' = '{input}', 'wait.interval.ms' = '50', 'max.uow.messages' = '20000') FORMAT JSON;
         CREATE STREAM forwarded AS SELECT id, payload FROM events;
         CREATE SINK out FROM forwarded INTO IBMMQ ({options}, 'queue' = '{output}') FORMAT JSON;"
    );
    format!(
        "sql = {sql}\n[server]\nbind = {bind}\ndelivery = \"at_least_once\"\n\
         [checkpoint]\nurl = {checkpoint_url}\ninterval = \"300ms\"\n",
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
        .env("KILL_IBMMQ_PASSWORD", PASSWORD)
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("start laminardb")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone-server kill test; needs the compose ibmmq service and an IBM MQ client"]
async fn ibmmq_at_least_once_survives_repeated_hard_kills() {
    let library = std::env::var("LAMINAR_TEST_IBMMQ_LIBRARY").expect("LAMINAR_TEST_IBMMQ_LIBRARY");
    let container =
        std::env::var("LAMINAR_TEST_IBMMQ_CONTAINER").expect("LAMINAR_TEST_IBMMQ_CONTAINER");
    let run = Duration::from_secs(env_u64("LAMINAR_IBMMQ_KILL_SECONDS", 60));
    let kill_every = Duration::from_millis(env_u64("LAMINAR_IBMMQ_KILL_EVERY_MS", 7_000));
    let rate = env_u64("LAMINAR_IBMMQ_KILL_RATE", 200).max(1);
    let tag = std::process::id() % 100_000;
    let (input, output) = (
        format!("DEV.LAMINAR.KILL.IN.{tag}"),
        format!("DEV.LAMINAR.KILL.OUT.{tag}"),
    );
    for queue in [&input, &output] {
        mqsc(
            &container,
            &format!(
                "DEFINE QLOCAL('{queue}') MAXDEPTH(100000) REPLACE\nCLEAR QLOCAL('{queue}')\n"
            ),
        );
    }
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("server.toml");
    let log = root.path().join("server.log");
    std::fs::write(
        &config,
        server_config(root.path(), &library, &input, &output),
    )
    .unwrap();

    let observed: Arc<Mutex<BTreeMap<i64, u32>>> = Arc::default();
    let reader = {
        let observed = Arc::clone(&observed);
        let mut config = connector(&library, &output);
        config.set("wait.interval.ms", "50");
        tokio::spawn(async move {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )]));
            let mut source = IbmMqSource::new(schema, None);
            source
                .start(
                    SourceStart::new(
                        config,
                        SourcePosition::Initial,
                        DeliveryGuarantee::AtLeastOnce,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            loop {
                match source.poll_batch(1_000).await.unwrap() {
                    Some(batch) => {
                        let mut observed = observed.lock().await;
                        for text in batch.records.column(0).as_string::<i32>().iter().flatten() {
                            let value: serde_json::Value = serde_json::from_str(text).unwrap();
                            *observed.entry(value["id"].as_i64().unwrap()).or_default() += 1;
                        }
                        drop(observed);
                        let cut = source.checkpoint();
                        source.notify_epoch_committed(1, &cut).await.unwrap();
                    }
                    None => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            }
        })
    };
    let publisher = {
        let config = connector(&library, &input);
        tokio::spawn(async move {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )]));
            let mut sink = IbmMqSink::new(schema.clone(), None);
            sink.open(&config).await.unwrap();
            let started = Instant::now();
            let mut published = 0_i64;
            while started.elapsed() < run {
                let due =
                    i64::try_from(started.elapsed().as_millis() as u64 * rate / 1_000).unwrap();
                if published < due {
                    let ids: Vec<String> = (published..due)
                        .map(|id| format!(r#"{{"id":{id},"payload":"p{id}"}}"#))
                        .collect();
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(StringArray::from(ids))],
                    )
                    .unwrap();
                    sink.write_batch(&batch).await.unwrap();
                    published = due;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            sink.close().await.unwrap();
            published
        })
    };

    let mut server = spawn_server(&config, &log);
    let mut kills = 0;
    let started = Instant::now();
    while started.elapsed() < run {
        tokio::time::sleep(kill_every).await;
        server.kill().expect("SIGKILL laminardb");
        server.wait().unwrap();
        kills += 1;
        server = spawn_server(&config, &log);
    }
    let published = publisher.await.unwrap();
    let catch_up = Instant::now();
    loop {
        let missing = {
            let observed = observed.lock().await;
            (0..published)
                .filter(|id| !observed.contains_key(id))
                .count()
        };
        if missing == 0 {
            break;
        }
        assert!(
            catch_up.elapsed() < Duration::from_secs(180),
            "{missing} of {published} published ids never reached the output after {kills} kills; see {}",
            log.display()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let catch_up = catch_up.elapsed();
    let drained = Instant::now();
    while depth(&container, &input) > 0 && drained.elapsed() < Duration::from_secs(30) {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    server.kill().unwrap();
    server.wait().unwrap();
    // The queue manager backs out the killed client's unit of work once it sees the broken
    // connection; poll until it settles.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut requeued = depth(&container, &input);
    while requeued > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        requeued = depth(&container, &input);
    }
    reader.abort();
    let observed = observed.lock().await;
    let duplicates: u32 = observed.values().map(|count| count - 1).sum();
    let unexpected = observed.keys().filter(|id| **id >= published).count();
    println!(
        "ibmmq kill test: published={published} kills={kills} duplicates={duplicates} \
         catch_up_ms={} uncommitted_after_final_kill={requeued}",
        catch_up.as_millis()
    );
    assert_eq!(
        unexpected, 0,
        "output contains ids that were never published"
    );
    assert_eq!(
        requeued, 0,
        "inputs remained uncommitted although the final generation ran past several checkpoints"
    );
    for queue in [&input, &output] {
        mqsc(&container, &format!("DELETE QLOCAL('{queue}') PURGE\n"));
    }
}
