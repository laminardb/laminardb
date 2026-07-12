//! Three-node real-binary checkpoint soak with `kill -9` fault injection.
//!
//! Spawns three `laminardb` processes in cluster mode (real gRPC control plane) against a shared
//! checkpoint store, runs tight-cadence checkpoints, and repeatedly hard-kills the leader and a
//! follower mid-epoch. After every fault it asserts the survivors keep committing, epochs never
//! regress (abandonment leaves gaps, never reuse), and the restarted node rejoins and resumes.
//!
//! Ignored by default — spawns processes and runs for minutes:
//!
//! ```text
//! cargo test -p laminar-server --no-default-features --features cluster,aws,kafka \
//!   --test cluster_soak three_node_kill9_soak -- --ignored --nocapture
//! ```
//!
//! Environment knobs:
//! - `LAMINAR_SOAK_SECONDS`      total soak duration (default 90)
//! - `LAMINAR_SOAK_INTERVAL_MS`  checkpoint cadence (default 500; floor 100)
//! - `LAMINAR_SOAK_CHECKPOINT_URL`  required cluster-shared checkpoint prefix
//! - `LAMINAR_SOAK_STATE_URL`  required cluster-shared state prefix for vnode partials
//! - `LAMINAR_SOAK_S3_ENDPOINT` / `_ACCESS_KEY` / `_SECRET_KEY` / `_REGION`  forwarded into both
//!   storage maps
//! - `LAMINAR_SOAK_KAFKA_SOURCE_BROKERS`  required shared Kafka/Redpanda source broker
//! - `LAMINAR_SOAK_RPS`  source production rate
//! - `LAMINAR_SOAK_STATE_TIER`  enable the disk cold tier (build `--features state-tier`); adds a
//!   memory budget + `EMIT CHANGES` agg so state demotes, then asserts demote/promote counters
//!   moved. Knobs: `LAMINAR_SOAK_BUDGET_BYTES` (256 KiB), `_VNODES` (256), `_RPS` (400),
//!   `_GROUPS` (2000 — agg key-space), `_SPAN` (12 — consecutive rows per agg key)
//! - `LAMINAR_SOAK_CHANGELOG_AGG`  add an `EMIT CHANGES` agg exercising the changelog
//!   `last_emitted` delta path; pair with `LAMINAR_SOAK_DELTA_CHAIN_MAX` (else it captures FULL)
//! - `LAMINAR_SOAK_FAULT_INJECT_ROLE`  trigger one fatal cycle fault after steady state on the
//!   observed `leader` or a `follower`

use std::io::{Read, Write as _};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const NODES: usize = 3;
/// Per-node ports: http = BASE + i, gossip = BASE + 100 + i.
const BASE_PORT: u16 = 19310;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Node {
    id: usize,
    config_path: PathBuf,
    log_path: PathBuf,
    child: Option<Child>,
    http_port: u16,
    /// Per-process one-shot fault trigger used only by the coordinated-recovery soak.
    fault_trigger_path: Option<PathBuf>,
    /// Debug checkpoint handshake used to prove a hard kill landed inside an active phase.
    checkpoint_gate_path: Option<PathBuf>,
}

impl Node {
    fn spawn(&mut self) {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .expect("node log file");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_laminardb"));
        cmd.arg("--config")
            .arg(&self.config_path)
            .env(
                "RUST_LOG",
                "laminardb=info,laminar_server=info,laminar_db=info,laminar_core=info",
            )
            .stdout(Stdio::from(log.try_clone().expect("clone log handle")))
            .stderr(Stdio::from(log));
        match &self.fault_trigger_path {
            Some(path) => {
                cmd.env("LAMINAR_FAULT_INJECT_TRIGGER_FILE", path);
            }
            None => {
                cmd.env_remove("LAMINAR_FAULT_INJECT_TRIGGER_FILE");
            }
        }
        match &self.checkpoint_gate_path {
            Some(path) => {
                cmd.env("LAMINAR_CHECKPOINT_KILL_GATE_FILE", path);
            }
            None => {
                cmd.env_remove("LAMINAR_CHECKPOINT_KILL_GATE_FILE");
            }
        }
        self.child = Some(cmd.spawn().expect("spawn laminardb"));
    }

    /// `kill -9` equivalent: no shutdown hooks, no final checkpoint.
    fn kill9(&mut self) {
        if let Some(mut c) = self.child.take() {
            c.kill().ok();
            c.wait().ok();
        }
    }

    fn http_get(&self, path: &str) -> Option<String> {
        let mut stream = TcpStream::connect(("127.0.0.1", self.http_port)).ok()?;
        stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).ok()?;
        let mut response = String::new();
        stream.read_to_string(&mut response).ok()?;
        let (headers, body) = response.split_once("\r\n\r\n")?;
        if !headers.lines().next()?.contains(" 200 ") {
            return None;
        }
        Some(body.to_owned())
    }

    /// Scrape one gauge/counter from `/metrics`; `None` while the node is down or booting.
    fn metric(&self, name: &str) -> Option<f64> {
        let body = self.http_get("/metrics")?;
        let mut found = false;
        let sum = body
            .lines()
            .filter(|line| {
                line.strip_prefix(name)
                    .is_some_and(|rest| rest.starts_with(' ') || rest.starts_with('{'))
            })
            .filter_map(|line| line.split_whitespace().last()?.parse::<f64>().ok())
            .inspect(|_| found = true)
            .sum();
        found.then_some(sum)
    }

    fn is_leader(&self) -> Option<bool> {
        let body = self.http_get("/api/v1/cluster/leader")?;
        serde_json::from_str::<serde_json::Value>(&body)
            .ok()?
            .get("is_leader")?
            .as_bool()
    }

    fn peer_names(&self) -> Option<Vec<String>> {
        serde_json::from_str::<Vec<serde_json::Value>>(&self.http_get("/api/v1/cluster/nodes")?)
            .ok()?
            .into_iter()
            .map(|node| {
                if node.get("state")?.as_str()? != "Active" {
                    return None;
                }
                node.get("name")?.as_str().map(str::to_owned)
            })
            .collect()
    }

    fn trigger_fault(&self, role: &str) {
        let path = self
            .fault_trigger_path
            .as_ref()
            .expect("fault trigger was not configured for this node");
        std::fs::write(path, role).expect("create fault trigger");
    }

    fn arm_checkpoint_kill(&self, role: &str) {
        let path = self
            .checkpoint_gate_path
            .as_ref()
            .expect("checkpoint kill gate was not configured for this node");
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("ready"));
        std::fs::write(path, role).expect("arm checkpoint kill gate");
    }

    fn checkpoint_kill_ready(&self, role: &str) -> bool {
        self.checkpoint_gate_path.as_ref().is_some_and(|path| {
            std::fs::read_to_string(path.with_extension("ready"))
                .is_ok_and(|ready| ready.trim() == role)
        })
    }

    fn disarm_checkpoint_kill(&self) {
        if let Some(path) = &self.checkpoint_gate_path {
            let _ = std::fs::remove_file(path);
            let _ = std::fs::remove_file(path.with_extension("ready"));
        }
    }

    fn epoch(&self) -> Option<f64> {
        self.metric("laminardb_checkpoint_epoch")
    }

    /// Committed checkpoints — the real progress signal (`checkpoint_epoch` also advances on aborts).
    fn commits(&self) -> Option<f64> {
        self.metric("laminardb_checkpoints_completed_total")
    }

    fn dump_log_tail(&self) {
        eprintln!("--- node{} log tail:", self.id);
        if let Ok(log) = std::fs::read_to_string(&self.log_path) {
            for line in log.lines().rev().take(40).collect::<Vec<_>>().iter().rev() {
                eprintln!("  {line}");
            }
        }
    }

    fn assert_running(&mut self) {
        let Some(child) = self.child.as_mut() else {
            panic!("node{} has no child process", self.id);
        };
        match child.try_wait() {
            Ok(None) => {}
            Ok(Some(status)) => {
                self.dump_log_tail();
                panic!("node{} exited before becoming ready: {status}", self.id);
            }
            Err(error) => {
                self.dump_log_tail();
                panic!("failed to inspect node{} process: {error}", self.id);
            }
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.kill9();
    }
}

struct ProducerGuard {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl ProducerGuard {
    fn spawn(brokers: String, topic: String, rps: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let producer_stop = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            produce_seq(&brokers, &topic, rps, &producer_stop);
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    fn assert_running(&mut self) {
        if !self
            .handle
            .as_ref()
            .is_some_and(std::thread::JoinHandle::is_finished)
        {
            return;
        }
        let result = self.handle.take().expect("producer handle").join();
        match result {
            Ok(()) => panic!("Kafka producer stopped before the soak completed"),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            handle.join().expect("Kafka producer thread failed");
        }
    }
}

impl Drop for ProducerGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

struct KafkaCommitOracle {
    consumer: rdkafka::consumer::BaseConsumer,
    topic: String,
    partitions: i32,
}

impl KafkaCommitOracle {
    fn new(brokers: &str, group: &str, topic: &str, partitions: i32) -> Self {
        use rdkafka::consumer::Consumer;

        let consumer: rdkafka::consumer::BaseConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .create()
            .expect("Kafka commit oracle consumer");
        let metadata = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .expect("fetch soak topic metadata");
        let actual = metadata
            .topics()
            .iter()
            .find(|candidate| candidate.name() == topic)
            .map(|candidate| candidate.partitions().len())
            .expect("created soak topic missing from Kafka metadata");
        assert_eq!(
            actual, partitions as usize,
            "soak topic has {actual} partitions, expected {partitions}"
        );
        Self {
            consumer,
            topic: topic.to_owned(),
            partitions,
        }
    }

    fn committed_offsets(&self) -> Option<Vec<i64>> {
        use rdkafka::consumer::Consumer;
        use rdkafka::{Offset, TopicPartitionList};

        let mut requested = TopicPartitionList::with_capacity(self.partitions as usize);
        for partition in 0..self.partitions {
            requested.add_partition(&self.topic, partition);
        }
        let committed = self
            .consumer
            .committed_offsets(requested, Duration::from_secs(5))
            .ok()?;
        let mut offsets = vec![None; self.partitions as usize];
        for partition in committed.elements() {
            let index = usize::try_from(partition.partition()).ok()?;
            if index >= offsets.len() || partition.topic() != self.topic {
                return None;
            }
            if let Offset::Offset(offset) = partition.offset() {
                if offset >= 0 {
                    offsets[index] = Some(offset);
                }
            }
        }
        Some(
            offsets
                .into_iter()
                .map(|offset| offset.unwrap_or(-1))
                .collect(),
        )
    }

    fn committed_offset_sum(&self) -> Option<i64> {
        self.committed_offsets()
            .map(|offsets| offsets.into_iter().sum())
    }
}

fn write_config(
    dir: &Path,
    id: usize,
    interval_ms: u64,
    checkpoint_url: &str,
    brokers: &str,
    input_topic: &str,
    consumer_group: &str,
) -> PathBuf {
    let depth = env_u64("LAMINAR_SOAK_DEPTH", 4);
    // Vnode partials go through [state], not [checkpoint]: without a SHARED state store the leader
    // durability gate (which lists the full registry) can never seal an epoch.
    let state_url = std::env::var("LAMINAR_SOAK_STATE_URL")
        .expect("three_node_kill9_soak requires cluster-shared LAMINAR_SOAK_STATE_URL storage");
    let http = BASE_PORT + id as u16;
    let gossip = BASE_PORT + 100 + id as u16;
    let seeds: Vec<String> = (0..NODES)
        .map(|i| format!("\"127.0.0.1:{}\"", BASE_PORT + 100 + i as u16))
        .collect();
    let data_dir = dir.join(format!("node{id}-data"));
    std::fs::create_dir_all(&data_dir).unwrap();

    // Cold tier: a tiny budget forces demotion, a larger vnode ring keeps vnodes clean (only
    // clean vnodes are demotable).
    let tier = std::env::var("LAMINAR_SOAK_STATE_TIER").is_ok();
    let vnodes = env_u64("LAMINAR_SOAK_VNODES", if tier { 256 } else { 64 });
    let mut server_extra = String::new();
    if tier {
        let budget = env_u64("LAMINAR_SOAK_BUDGET_BYTES", 256 * 1024);
        let tier_dir = data_dir
            .join("tier")
            .display()
            .to_string()
            .replace('\\', "/");
        server_extra =
            format!("state_tier_dir = \"{tier_dir}\"\nstate_memory_budget_bytes = {budget}\n");
        // Shed idle GROUPS, not whole vnodes; needs delta on (pair with LAMINAR_SOAK_DELTA_CHAIN_MAX).
        if std::env::var("LAMINAR_SOAK_STATE_TIER_GROUP").is_ok() {
            server_extra.push_str("state_tier_group_demotion = true\n");
        }
    }

    let mut storage = String::new();
    for (env, key) in [
        ("LAMINAR_SOAK_S3_ENDPOINT", "endpoint"),
        ("LAMINAR_SOAK_S3_ACCESS_KEY", "aws_access_key_id"),
        ("LAMINAR_SOAK_S3_SECRET_KEY", "aws_secret_access_key"),
        ("LAMINAR_SOAK_S3_REGION", "region"),
    ] {
        if let Ok(v) = std::env::var(env) {
            storage.push_str(&format!("{key} = \"{v}\"\n"));
        }
    }
    if storage.contains("endpoint") {
        storage.push_str("allow_http = \"true\"\n");
    }

    // Discovery: gossip (phi-accrual failure detection) by default;
    // `LAMINAR_SOAK_DISCOVERY=static` for the seed-list heartbeat path.
    let discovery = std::env::var("LAMINAR_SOAK_DISCOVERY").unwrap_or_else(|_| "gossip".into());
    assert!(
        matches!(discovery.as_str(), "gossip" | "static"),
        "LAMINAR_SOAK_DISCOVERY must be 'gossip' or 'static', got {discovery:?}"
    );

    // `LAMINAR_SOAK_DELTA_CHAIN_MAX=N` enables delta checkpoints plus a non-changelog agg
    // so kill -9 exercises the delta write + chain-recovery path.
    let delta_chain_max = std::env::var("LAMINAR_SOAK_DELTA_CHAIN_MAX").ok().map(|v| {
        v.parse::<u32>()
            .expect("LAMINAR_SOAK_DELTA_CHAIN_MAX must be a u32")
    });
    // Enabling delta also makes the chain the primary aggregate checkpoint.
    let delta_line = delta_chain_max.map_or(String::new(), |n| format!("delta_chain_max = {n}"));

    let mut toml = format!(
        r#"
node_id = "n{id}"

[server]
mode = "cluster"
bind = "127.0.0.1:{http}"
{server_extra}
[discovery]
strategy = "{discovery}"
seeds = [{seeds}]
gossip_port = {gossip}
advertise_host = "127.0.0.1"

[state]
backend = "object_store"
url = "{state_url}"
instance_id = "n{id}"
vnode_capacity = {vnodes}

[state.storage]
{storage}
[checkpoint]
url = "{url}"
interval = "{interval_ms}ms"
max_retained = 5
max_in_flight_epochs = {depth}
{delta_line}

[checkpoint.storage]
{storage}

# Workload: a shared Kafka consumer group gives cluster-admissible,
# replayable split ownership across the three processes.
[[source]]
name = "kin"
connector = "kafka"
format = "json"
[source.properties]
"bootstrap.servers" = "{brokers}"
topic = "{input_topic}"
"group.id" = "{consumer_group}"
"startup.mode" = "earliest"
[[source.schema]]
name = "seq"
type = "BIGINT"
nullable = false

[[pipeline]]
name = "soak_stream"
sql = "SELECT seq FROM kin"
"#,
        seeds = seeds.join(", "),
        url = checkpoint_url,
    );

    // Non-changelog agg over a slow-cycling key space: per-vnode state accrues as delta partials,
    // so kill -9 + rebalance exercises delta write/chain-recovery. `LAMINAR_SOAK_AGG=1` adds it
    // without delta, to isolate the shuffle path from the delta path.
    if delta_chain_max.is_some() || std::env::var("LAMINAR_SOAK_AGG").is_ok() {
        let groups = env_u64("LAMINAR_SOAK_GROUPS", 2000);
        let span = env_u64("LAMINAR_SOAK_SPAN", 12);
        toml.push_str(&format!(
            r#"
[[pipeline]]
name = "soak_delta_agg"
        sql = "SELECT (seq / {span}) % {groups} AS k, COUNT(*) AS n, MAX(seq) AS hi FROM kin GROUP BY (seq / {span}) % {groups}"
"#,
        ));
    }

    // Changelog agg (EMIT CHANGES) under delta capture — exercises the `last_emitted` delta path
    // (changelog aggs used to force-FULL every epoch). Pair with `LAMINAR_SOAK_DELTA_CHAIN_MAX`.
    if std::env::var("LAMINAR_SOAK_CHANGELOG_AGG").is_ok() {
        let groups = env_u64("LAMINAR_SOAK_GROUPS", 2000);
        let span = env_u64("LAMINAR_SOAK_SPAN", 12);
        toml.push_str(&format!(
            r#"
[[pipeline]]
name = "soak_changelog_agg"
        sql = "SELECT (seq / {span}) % {groups} AS k, COUNT(*) AS n, MAX(seq) AS hi FROM kin GROUP BY (seq / {span}) % {groups} EMIT CHANGES"
"#,
        ));
    }

    // Demotable per-vnode state for the cold tier: an EMIT CHANGES agg over a slow-cycling key
    // space. Only changelog aggs demote, and only CLEAN vnodes (untouched since last capture) — so
    // keys must idle. `(seq / SPAN) % GROUPS` bursts SPAN rows per key then moves on, leaving each
    // vnode idle a full cycle (demotable) before returning (promotable); plain `seq % GROUPS`
    // scatters keys every cycle so no vnode idles and demotion just thrashes.
    if tier {
        let groups = env_u64("LAMINAR_SOAK_GROUPS", 2000);
        let span = env_u64("LAMINAR_SOAK_SPAN", 12);
        toml.push_str(&format!(
            r#"
[[pipeline]]
name = "soak_agg"
        sql = "SELECT (seq / {span}) % {groups} AS k, COUNT(*) AS n FROM kin GROUP BY (seq / {span}) % {groups} EMIT CHANGES"
"#,
        ));
    }

    let path = dir.join(format!("node{id}.toml"));
    std::fs::write(&path, toml).unwrap();
    path
}

/// Wait until `pred` holds, polling, or panic with `what` at deadline.
fn wait_for(what: &str, deadline: Duration, mut pred: impl FnMut() -> bool) {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if pred() {
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!("soak: timed out after {deadline:?} waiting for: {what}");
}

/// Observe the embedded aggregate's live changelog. With `key = None`, returns a key whose count
/// reached `minimum`; with a key, returns that key's first post-connect batch maximum. The latter
/// is a data oracle across process death: after a post-observation checkpoint, restored state must
/// continue above the pre-kill count rather than restart the aggregate from zero.
fn observe_aggregate_count(
    node: &mut Node,
    key: Option<i64>,
    minimum: i64,
    deadline: Duration,
) -> (i64, i64) {
    use std::io::ErrorKind;
    use tungstenite::stream::MaybeTlsStream;
    use tungstenite::{Error, Message};

    let start = Instant::now();
    let url = format!("ws://127.0.0.1:{}/ws/soak_agg", node.http_port);
    let (mut socket, _) = loop {
        node.assert_running();
        match tungstenite::connect(url.as_str()) {
            Ok(connected) => break connected,
            Err(_) if start.elapsed() < deadline => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => panic!("failed to connect aggregate data oracle: {error}"),
        }
    };
    if let MaybeTlsStream::Plain(stream) = socket.get_mut() {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set aggregate WebSocket read timeout");
    }

    while start.elapsed() < deadline {
        node.assert_running();
        match socket.read() {
            Ok(Message::Text(text)) => {
                let envelope: serde_json::Value =
                    serde_json::from_str(text.as_str()).expect("aggregate WebSocket JSON");
                let Some(rows) = envelope.get("data").and_then(serde_json::Value::as_array) else {
                    continue;
                };
                let mut matches = rows
                    .iter()
                    .filter_map(|row| Some((row.get("k")?.as_i64()?, row.get("n")?.as_i64()?)));
                if let Some(wanted) = key {
                    if let Some(count) = matches
                        .filter_map(|(observed, count)| (observed == wanted).then_some(count))
                        .max()
                    {
                        return (wanted, count);
                    }
                } else if let Some((observed, count)) = matches.find(|(_, count)| *count >= minimum)
                {
                    return (observed, count);
                }
            }
            Ok(Message::Ping(payload)) => socket
                .send(Message::Pong(payload))
                .expect("aggregate WebSocket pong"),
            Ok(Message::Close(frame)) => {
                panic!("aggregate data oracle closed unexpectedly: {frame:?}")
            }
            Ok(_) => {}
            Err(Error::Io(error))
                if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(error) => panic!("aggregate data oracle failed: {error}"),
        }
    }
    panic!("timed out waiting for aggregate data-oracle observation (key={key:?})");
}

/// Highest epoch visible on any live node.
fn cluster_epoch(nodes: &[Node]) -> f64 {
    let mut epochs = nodes
        .iter()
        .filter(|node| node.child.is_some())
        .map(|node| node.epoch());
    let first = epochs
        .next()
        .flatten()
        .expect("expected-live node did not expose checkpoint_epoch");
    epochs
        .try_fold(first, |highest, epoch| {
            epoch.map(|epoch| highest.max(epoch))
        })
        .expect("expected-live node did not expose checkpoint_epoch")
}

fn try_cluster_metric(nodes: &[Node], name: &str) -> Option<f64> {
    let mut total = 0.0;
    let mut live = 0usize;
    for node in nodes.iter().filter(|node| node.child.is_some()) {
        total += node.metric(name)?;
        live += 1;
    }
    (live > 0).then_some(total)
}

fn cluster_metric(nodes: &[Node], name: &str) -> f64 {
    try_cluster_metric(nodes, name)
        .unwrap_or_else(|| panic!("expected-live node did not expose metric {name:?}"))
}

fn cluster_commits(nodes: &[Node]) -> f64 {
    cluster_metric(nodes, "laminardb_checkpoints_completed_total")
}

fn assert_running_nodes(nodes: &mut [Node]) {
    let mut live = 0;
    for node in nodes.iter_mut().filter(|node| node.child.is_some()) {
        node.assert_running();
        live += 1;
    }
    assert!(live > 0, "soak has no expected-live node processes");
}

fn observed_leader(nodes: &[Node]) -> Option<usize> {
    let roles: Option<Vec<bool>> = nodes.iter().map(Node::is_leader).collect();
    let leaders: Vec<usize> = roles?
        .into_iter()
        .enumerate()
        .filter_map(|(id, is_leader)| is_leader.then_some(id))
        .collect();
    (leaders.len() == 1).then_some(leaders[0])
}

fn has_full_membership(nodes: &[Node]) -> bool {
    nodes.iter().all(|node| {
        let Some(peers) = node.peer_names() else {
            return false;
        };
        let peers: std::collections::BTreeSet<String> = peers
            .into_iter()
            .filter(|name| name != &format!("n{}", node.id))
            .collect();
        let expected: std::collections::BTreeSet<String> = (0..NODES)
            .filter(|id| *id != node.id)
            .map(|id| format!("n{id}"))
            .collect();
        peers == expected
    })
}

fn wait_for_stable_leader(nodes: &mut [Node], producer: &mut ProducerGuard) -> usize {
    let mut candidate = None;
    let mut stable_samples = 0u8;
    let mut chosen = None;
    wait_for(
        "one stable observed cluster leader",
        Duration::from_secs(30),
        || {
            assert_running_nodes(nodes);
            producer.assert_running();
            match observed_leader(nodes) {
                Some(id) if candidate == Some(id) => stable_samples += 1,
                Some(id) => {
                    candidate = Some(id);
                    stable_samples = 1;
                }
                None => {
                    candidate = None;
                    stable_samples = 0;
                }
            }
            if stable_samples >= 3 {
                chosen = candidate;
                true
            } else {
                false
            }
        },
    );
    chosen.expect("stable leader wait completed without a leader")
}

/// Assert two committed checkpoints over advancing source data. With Kafka, also require a new
/// broker offset commit so an empty-checkpoint loop cannot satisfy the soak.
fn assert_progress(
    nodes: &mut [Node],
    mut producer: Option<&mut ProducerGuard>,
    commit_oracle: Option<&KafkaCommitOracle>,
    window: Duration,
    label: &str,
) -> f64 {
    assert_running_nodes(nodes);
    if let Some(producer) = producer.as_deref_mut() {
        producer.assert_running();
    }
    let ingested_target = cluster_metric(nodes, "laminardb_events_ingested_total") + 1.0;
    let emitted_target = cluster_metric(nodes, "laminardb_events_emitted_total") + 1.0;
    wait_for(
        &format!("{label}: source ingestion and graph output to advance"),
        window,
        || {
            assert_running_nodes(nodes);
            if let Some(producer) = producer.as_deref_mut() {
                producer.assert_running();
            }
            try_cluster_metric(nodes, "laminardb_events_ingested_total")
                .is_some_and(|ingested| ingested >= ingested_target)
                && try_cluster_metric(nodes, "laminardb_events_emitted_total")
                    .is_some_and(|emitted| emitted >= emitted_target)
        },
    );

    // Take the durability baselines only after graph output advanced, so checkpoints that happened
    // before this phase cannot satisfy the source-offset proof.
    let checkpoint_target = cluster_commits(nodes) + 2.0;
    let mut kafka_offset_baseline = None;
    if let Some(oracle) = commit_oracle {
        wait_for(
            &format!("{label}: complete Kafka committed-offset snapshot"),
            window,
            || {
                assert_running_nodes(nodes);
                if let Some(producer) = producer.as_deref_mut() {
                    producer.assert_running();
                }
                kafka_offset_baseline = oracle.committed_offsets();
                kafka_offset_baseline.is_some()
            },
        );
    }
    let kafka_offset_targets = kafka_offset_baseline
        .as_ref()
        .map(|offsets| offsets.iter().map(|offset| offset + 1).collect::<Vec<_>>());
    wait_for(
        &format!("{label}: checkpoints and durable source offsets to advance"),
        window,
        || {
            assert_running_nodes(nodes);
            if let Some(producer) = producer.as_deref_mut() {
                producer.assert_running();
            }
            try_cluster_metric(nodes, "laminardb_checkpoints_completed_total")
                .is_some_and(|commits| commits >= checkpoint_target)
                && kafka_offset_targets.as_ref().is_none_or(|targets| {
                    commit_oracle
                        .and_then(KafkaCommitOracle::committed_offsets)
                        .is_some_and(|current| {
                            let baseline = kafka_offset_baseline.as_ref().expect("offset baseline");
                            assert_eq!(
                                current.len(),
                                baseline.len(),
                                "Kafka committed-offset partition count changed"
                            );
                            for (partition, (current, baseline)) in
                                current.iter().zip(baseline).enumerate()
                            {
                                assert!(
                                    current >= baseline,
                                    "Kafka partition {partition} committed offset regressed: \
                                     {current} < {baseline}"
                                );
                            }
                            current
                                .iter()
                                .zip(targets)
                                .all(|(current, target)| current >= target)
                        })
                })
        },
    );
    cluster_epoch(nodes)
}

/// EMBEDDED single-node config: tiny budget + slow-cycling `EMIT CHANGES` agg drive group
/// demote→promote; cold groups survive kill -9 via the cold-only partials.
fn write_embedded_config(dir: &Path, id: usize, interval_ms: u64) -> PathBuf {
    let data_dir = dir.join(format!("node{id}-data"));
    std::fs::create_dir_all(&data_dir).unwrap();
    let state_shared = dir.join("state");
    std::fs::create_dir_all(&state_shared).unwrap();
    let ckpt_shared = dir.join("checkpoints");
    std::fs::create_dir_all(&ckpt_shared).unwrap();
    let fwd = |p: &Path| p.display().to_string().replace('\\', "/");

    let http = BASE_PORT + id as u16;
    let budget = env_u64("LAMINAR_SOAK_BUDGET_BYTES", 8192);
    let vnodes = env_u64("LAMINAR_SOAK_VNODES", 64);
    let rps = env_u64("LAMINAR_SOAK_RPS", 400);
    let groups = env_u64("LAMINAR_SOAK_GROUPS", 600);
    let span = env_u64("LAMINAR_SOAK_SPAN", 12);
    let tier_dir = fwd(&data_dir.join("tier"));

    // No delta_chain_max: single-node group demotion uses delta DIRTY-tracking, not the primary
    // chain, so the whole-node manifest stays authoritative and demoted groups fold into cold-only
    // partials the additive rehydrate merges back.
    // `backend = "local"` builds an ObjectStoreBackend over local FS AND supplies the
    // local_storage_dir the Embedded profile requires; object_store/file:// has neither.
    let toml = format!(
        r#"
node_id = "n{id}"

[server]
mode = "embedded"
bind = "127.0.0.1:{http}"
state_tier_dir = "{tier_dir}"
state_memory_budget_bytes = {budget}
state_tier_group_demotion = true

[state]
backend = "local"
path = "{state_path}"
instance_id = "n{id}"
vnode_capacity = {vnodes}

[checkpoint]
url = "file:///{ckpt_url}"
interval = "{interval_ms}ms"
max_retained = 5

[[source]]
name = "gen"
connector = "generator"
properties = {{ "rows.per.second" = "{rps}", "batch.max.size" = "256" }}

[[pipeline]]
name = "soak_agg"
sql = "SELECT (seq / {span}) % {groups} AS k, COUNT(*) AS n FROM gen GROUP BY (seq / {span}) % {groups} EMIT CHANGES"
"#,
        state_path = fwd(&state_shared),
        ckpt_url = fwd(&ckpt_shared),
    );
    let path = dir.join(format!("node{id}-embedded.toml"));
    std::fs::write(&path, toml).unwrap();
    path
}

/// Single-node EMBEDDED group-demotion recovery under kill -9: demote idle groups, hard-kill, then
/// recover from the cold-only partials — what the single-node cluster soak can't (gate stalls).
#[test]
#[ignore = "spawns a real laminardb process; run with --ignored --features state-tier"]
fn embedded_kill9_group_demotion_soak() {
    let soak_secs = env_u64("LAMINAR_SOAK_SECONDS", 75);
    let interval_ms = env_u64("LAMINAR_SOAK_INTERVAL_MS", 300).max(100);
    let max_kills = env_u64("LAMINAR_SOAK_KILLS", 4);
    let oracle_min_count = i64::try_from(env_u64("LAMINAR_SOAK_SPAN", 12).saturating_mul(2))
        .expect("LAMINAR_SOAK_SPAN is too large for the aggregate data oracle");

    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir =
        Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-embed-{}", std::process::id()));
    std::fs::create_dir_all(&log_dir).unwrap();
    eprintln!("soak: embedded node logs in {}", log_dir.display());

    let mut node = Node {
        id: 0,
        config_path: write_embedded_config(dir.path(), 0, interval_ms),
        log_path: log_dir.join("node0.log"),
        child: None,
        http_port: BASE_PORT,
        fault_trigger_path: None,
        checkpoint_gate_path: None,
    };

    node.spawn();
    let boot = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        wait_for(
            "embedded node to commit a checkpoint",
            Duration::from_secs(30),
            || {
                node.assert_running();
                node.commits().unwrap_or(0.0) >= 1.0
            },
        );
    }));
    if let Err(payload) = boot {
        node.dump_log_tail();
        std::panic::resume_unwind(payload);
    }
    eprintln!("soak: embedded node up");

    // Wait for demotion AND a following checkpoint (captures cold-only partials) so the first kill
    // actually exercises cold-group recovery.
    wait_for("group demotion to fire", Duration::from_secs(60), || {
        node.assert_running();
        node.metric("laminardb_state_tier_demote_total")
            .unwrap_or(0.0)
            > 0.0
    });
    let after_demote = node.commits().unwrap_or(0.0);
    wait_for(
        "a checkpoint after demotion (captures cold-only partials)",
        Duration::from_secs(30),
        || {
            node.assert_running();
            node.commits().unwrap_or(0.0) >= after_demote + 1.0
        },
    );
    eprintln!(
        "soak: demotion fired (demotes={}), cold groups captured",
        node.metric("laminardb_state_tier_demote_total")
            .unwrap_or(0.0)
    );

    let deadline = Instant::now() + Duration::from_secs(soak_secs);
    let mut kills = 0u64;
    let mut round = 0u64;
    let mut max_resident = 0.0f64;
    while kills < max_kills {
        round += 1;
        let (oracle_key, baseline_count) =
            observe_aggregate_count(&mut node, None, oracle_min_count, Duration::from_secs(60));
        let commits_after_observation = node
            .commits()
            .expect("embedded node stopped exposing checkpoint commits");
        wait_for(
            "two checkpoints after the aggregate data-oracle observation",
            Duration::from_secs(30),
            || {
                node.assert_running();
                node.commits()
                    .is_some_and(|commits| commits >= commits_after_observation + 2.0)
            },
        );
        eprintln!("soak round {round}: kill -9 embedded node");
        node.kill9();
        kills += 1;
        node.spawn();
        let (_, recovered_count) =
            observe_aggregate_count(&mut node, Some(oracle_key), 0, Duration::from_secs(60));
        assert!(
            recovered_count > baseline_count,
            "embedded aggregate key {oracle_key} restarted at {recovered_count} after kill; \
             the checkpointed pre-kill count was {baseline_count}"
        );
        eprintln!(
            "soak round {round}: aggregate key {oracle_key} continued from {baseline_count} \
             to {recovered_count} after restart"
        );
        // Must recover from checkpoint (cold groups from cold-only partials) and resume —
        // the single-node cluster path stalls here; embedded must not.
        assert_progress(
            std::slice::from_mut(&mut node),
            None,
            None,
            Duration::from_secs(60),
            "progress after kill",
        );
        eprintln!(
            "soak round {round}: demotes={} fetches={} resident_bytes={} state_bytes={}",
            node.metric("laminardb_state_tier_demote_total")
                .unwrap_or(0.0),
            node.metric("laminardb_state_tier_fetch_total")
                .unwrap_or(0.0),
            node.metric("laminardb_state_tier_bytes").unwrap_or(0.0),
            node.metric("laminardb_state_bytes").unwrap_or(0.0),
        );
        max_resident = max_resident.max(node.metric("laminardb_state_bytes").unwrap_or(0.0));
    }
    assert_eq!(
        kills, max_kills,
        "embedded soak did not complete every requested kill"
    );

    while Instant::now() < deadline {
        round += 1;
        assert_progress(
            std::slice::from_mut(&mut node),
            None,
            None,
            Duration::from_secs(60),
            "steady progress",
        );
        max_resident = max_resident.max(node.metric("laminardb_state_bytes").unwrap_or(0.0));
        std::thread::sleep(Duration::from_secs(2));
    }

    let demotes = node
        .metric("laminardb_state_tier_demote_total")
        .unwrap_or(0.0);
    let fetches = node
        .metric("laminardb_state_tier_fetch_total")
        .unwrap_or(0.0);
    eprintln!(
        "soak: completed {round} rounds ({kills} kills); demotes={demotes} fetches={fetches}"
    );
    // demote→promote survived the kill rounds: a row hitting a cold group after restart promotes it
    // back, which only works if recovery rebuilt it from the cold-only partial (not silently from zero).
    assert!(
        demotes > 0.0,
        "embedded: group demotion never fired: demotes={demotes}"
    );
    assert!(
        fetches > 0.0,
        "embedded: demoted groups never promoted across kills — recovery may have lost them: fetches={fetches}"
    );
    // Bounded-RAM gate (opt-in): demotion must keep resident agg state below the full key-space.
    // LAMINAR_SOAK_MAX_RESIDENT_BYTES asserts a ceiling (meaningful only when GROUPS >> budget).
    eprintln!("soak: max resident agg state across rounds: {max_resident} B");
    if let Ok(v) = std::env::var("LAMINAR_SOAK_MAX_RESIDENT_BYTES") {
        let ceiling: f64 = v
            .parse()
            .expect("LAMINAR_SOAK_MAX_RESIDENT_BYTES must be a number");
        assert!(
            max_resident <= ceiling,
            "embedded: resident agg state {max_resident} B exceeded the {ceiling} B ceiling — \
             group demotion is not bounding RAM"
        );
    }
    node.kill9();
}

#[test]
#[ignore = "spawns 3 real laminardb processes; run with --ignored"]
fn three_node_kill9_soak() {
    let soak_secs = env_u64("LAMINAR_SOAK_SECONDS", 90);
    let interval_ms = env_u64("LAMINAR_SOAK_INTERVAL_MS", 500).max(100);

    let dir = tempfile::tempdir().expect("tempdir");
    let url = std::env::var("LAMINAR_SOAK_CHECKPOINT_URL").expect(
        "three_node_kill9_soak requires cluster-shared LAMINAR_SOAK_CHECKPOINT_URL storage",
    );
    let brokers = std::env::var("LAMINAR_SOAK_KAFKA_SOURCE_BROKERS")
        .expect("three_node_kill9_soak requires LAMINAR_SOAK_KAFKA_SOURCE_BROKERS");
    let input_topic = format!("soak-cluster-in-{}", std::process::id());
    let consumer_group = format!("soak-cluster-{}", std::process::id());
    kafka_create_topic(&brokers, &input_topic, NODES as i32);
    let commit_oracle =
        KafkaCommitOracle::new(&brokers, &consumer_group, &input_topic, NODES as i32);
    let source_rps = env_u64("LAMINAR_SOAK_RPS", 400).max(1);
    let mut producer = ProducerGuard::spawn(brokers.clone(), input_topic.clone(), source_rps);

    // Node logs under target/ (not the tempdir) so they survive a failed run for post-mortem.
    let log_dir =
        Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{}", std::process::id()));
    std::fs::create_dir_all(&log_dir).unwrap();
    eprintln!("soak: node logs in {}", log_dir.display());

    let fault_role = std::env::var("LAMINAR_SOAK_FAULT_INJECT_ROLE").ok();
    if let Some(role) = fault_role.as_deref() {
        assert!(
            matches!(role, "leader" | "follower"),
            "LAMINAR_SOAK_FAULT_INJECT_ROLE must be 'leader' or 'follower', got {role:?}"
        );
    }
    let max_kills = env_u64("LAMINAR_SOAK_KILLS", 4);
    assert!(
        fault_role.is_none() || max_kills == 0,
        "coordinated-recovery fault injection and process kill rounds are separate soak modes"
    );

    let mut nodes: Vec<Node> = (0..NODES)
        .map(|id| Node {
            id,
            config_path: write_config(
                dir.path(),
                id,
                interval_ms,
                &url,
                &brokers,
                &input_topic,
                &consumer_group,
            ),
            log_path: log_dir.join(format!("node{id}.log")),
            child: None,
            http_port: BASE_PORT + id as u16,
            fault_trigger_path: fault_role
                .as_ref()
                .map(|_| dir.path().join(format!("fault-node-{id}.trigger"))),
            checkpoint_gate_path: (max_kills > 0)
                .then(|| dir.path().join(format!("checkpoint-node-{id}.arm"))),
        })
        .collect();
    for n in &mut nodes {
        n.spawn();
        // Stagger process startup to reduce formation churn; role is observed from the API below.
        std::thread::sleep(Duration::from_millis(500));
    }

    // On boot failure dump the node log tails so the cause is visible in test output.
    let boot = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        wait_for(
            "all nodes serving /metrics",
            Duration::from_secs(60),
            || {
                producer.assert_running();
                nodes.iter_mut().all(|n| {
                    n.assert_running();
                    n.epoch().is_some()
                })
            },
        );
    }));
    if boot.is_err() {
        for n in &nodes {
            n.dump_log_tail();
        }
        panic!("soak: cluster failed to boot — node log tails above");
    }
    wait_for(
        "every node to observe full cluster membership",
        Duration::from_secs(60),
        || {
            assert_running_nodes(&mut nodes);
            producer.assert_running();
            has_full_membership(&nodes)
        },
    );
    // A pre-join epoch can burn a full 30s gate timeout before convergence, so allow for it.
    let mut latest_epoch = assert_progress(
        &mut nodes,
        Some(&mut producer),
        Some(&commit_oracle),
        Duration::from_secs(90),
        "startup",
    );
    eprintln!(
        "soak: cluster up, epoch {latest_epoch}, ingested={}, Kafka committed offset sum={}",
        cluster_metric(&nodes, "laminardb_events_ingested_total"),
        commit_oracle.committed_offset_sum().unwrap_or(0)
    );

    if let Some(role) = fault_role.as_deref() {
        let leader = wait_for_stable_leader(&mut nodes, &mut producer);
        let victim = match role {
            "leader" => leader,
            "follower" => (0..NODES)
                .find(|id| *id != leader)
                .expect("three-node cluster has no follower"),
            _ => unreachable!(),
        };
        let fault_baseline = nodes[victim]
            .metric("laminardb_pipeline_faults_total")
            .expect("selected node did not expose pipeline_faults_total");
        let recovery_baselines: Vec<f64> = nodes
            .iter()
            .map(|node| {
                node.metric("laminardb_coordinated_recoveries_total")
                    .expect("node did not expose coordinated_recoveries_total")
            })
            .collect();
        let failure_baselines: Vec<f64> = nodes
            .iter()
            .map(|node| {
                node.metric("laminardb_coordinated_recovery_failures_total")
                    .expect("node did not expose coordinated_recovery_failures_total")
            })
            .collect();
        eprintln!("soak: trigger fatal cycle fault on observed {role} node {victim}");
        nodes[victim].trigger_fault(role);
        wait_for(
            "selected node to report the injected pipeline fault",
            Duration::from_secs(30),
            || {
                assert_running_nodes(&mut nodes);
                producer.assert_running();
                nodes[victim]
                    .metric("laminardb_pipeline_faults_total")
                    .is_some_and(|faults| faults > fault_baseline)
            },
        );
        wait_for(
            "every node to apply the coordinated recovery round",
            Duration::from_secs(90),
            || {
                assert_running_nodes(&mut nodes);
                producer.assert_running();
                nodes
                    .iter()
                    .zip(&recovery_baselines)
                    .all(|(node, baseline)| {
                        node.metric("laminardb_coordinated_recoveries_total")
                            .is_some_and(|recoveries| recoveries > *baseline)
                    })
            },
        );
        for (node, baseline) in nodes.iter().zip(&failure_baselines) {
            let failures = node
                .metric("laminardb_coordinated_recovery_failures_total")
                .expect("node stopped exposing coordinated_recovery_failures_total");
            assert_eq!(
                failures, *baseline,
                "node{} recorded a coordinated recovery failure",
                node.id
            );
        }
        latest_epoch = assert_progress(
            &mut nodes,
            Some(&mut producer),
            Some(&commit_oracle),
            Duration::from_secs(90),
            "progress after coordinated recovery",
        );
        eprintln!("soak: coordinated {role} recovery complete, epoch {latest_epoch}");
    }

    let deadline = Instant::now() + Duration::from_secs(soak_secs);
    let mut round = 0u32;
    let mut kills = 0u64;
    let mut leader_kills = 0u64;
    let mut follower_kills = 0u64;
    let log_tier = |nodes: &[Node], round: u32| {
        if std::env::var("LAMINAR_SOAK_STATE_TIER").is_ok() {
            eprintln!(
                "soak round {round} tier: demotes={} fetches={} resident_bytes={} \
                 slices={} in_memory_state={}",
                cluster_metric(nodes, "laminardb_state_tier_demote_total"),
                cluster_metric(nodes, "laminardb_state_tier_fetch_total"),
                cluster_metric(nodes, "laminardb_state_tier_bytes"),
                cluster_metric(nodes, "laminardb_state_tier_slices"),
                cluster_metric(nodes, "laminardb_state_bytes"),
            );
        }
    };

    while kills < max_kills {
        round += 1;
        let leader = wait_for_stable_leader(&mut nodes, &mut producer);
        let (victim, victim_role) = if kills.is_multiple_of(NODES as u64) {
            leader_kills += 1;
            (leader, "leader")
        } else {
            let followers: Vec<usize> = (0..NODES).filter(|id| *id != leader).collect();
            let victim = followers[((kills - 1) as usize) % followers.len()];
            follower_kills += 1;
            (victim, "follower")
        };
        nodes[victim].assert_running();
        nodes[victim].arm_checkpoint_kill(victim_role);
        wait_for(
            "selected node to enter its armed checkpoint phase",
            Duration::from_secs(45),
            || {
                assert_running_nodes(&mut nodes);
                producer.assert_running();
                nodes[victim].checkpoint_kill_ready(victim_role)
            },
        );
        eprintln!(
            "soak round {round}: kill -9 observed {victim_role} node {victim} inside checkpoint"
        );
        nodes[victim].kill9();
        nodes[victim].disarm_checkpoint_kill();
        kills += 1;
        let post_kill_epoch = assert_progress(
            &mut nodes,
            Some(&mut producer),
            Some(&commit_oracle),
            Duration::from_secs(90),
            "progress after kill",
        );
        eprintln!("soak round {round}: survivors advanced to epoch {post_kill_epoch}");

        // Restart it; every process must be live again and the workload/checkpoint offsets advance.
        nodes[victim].spawn();
        wait_for(
            "killed node serving /metrics again",
            Duration::from_secs(60),
            || {
                assert_running_nodes(&mut nodes);
                producer.assert_running();
                nodes[victim].epoch().is_some()
            },
        );
        let victim_ingested = nodes[victim]
            .metric("laminardb_events_ingested_total")
            .expect("restarted node did not expose events_ingested_total");
        wait_for(
            "restarted node to rejoin membership and ingest its assigned workload",
            Duration::from_secs(60),
            || {
                assert_running_nodes(&mut nodes);
                producer.assert_running();
                has_full_membership(&nodes)
                    && nodes[victim]
                        .metric("laminardb_events_ingested_total")
                        .is_some_and(|ingested| ingested > victim_ingested)
            },
        );
        latest_epoch = assert_progress(
            &mut nodes,
            Some(&mut producer),
            Some(&commit_oracle),
            Duration::from_secs(90),
            "progress after rejoin",
        );
        log_tier(&nodes, round);
    }
    assert_eq!(
        kills, max_kills,
        "cluster soak did not complete every requested kill"
    );
    if max_kills >= NODES as u64 {
        assert!(
            leader_kills > 0,
            "cluster soak did not kill an observed leader"
        );
        assert!(
            follower_kills >= 2,
            "cluster soak did not complete both requested follower-role kills"
        );
    }

    while Instant::now() < deadline {
        round += 1;
        latest_epoch = assert_progress(
            &mut nodes,
            Some(&mut producer),
            Some(&commit_oracle),
            Duration::from_secs(90),
            "steady progress",
        );
        log_tier(&nodes, round);
        std::thread::sleep(Duration::from_secs(5));
    }

    assert_running_nodes(&mut nodes);
    producer.assert_running();
    eprintln!(
        "soak: completed {round} rounds ({kills} kills: {leader_kills} leader, \
         {follower_kills} follower), final epoch {latest_epoch}"
    );

    // Durability-gate poll wait vs whole checkpoint (leader-only metric; sum picks it up).
    // avg = histogram sum/count.
    {
        let m = |n: &str| -> f64 { nodes.iter().filter_map(|x| x.metric(n)).sum() };
        let gw_sum = m("laminardb_checkpoint_restorable_gate_wait_seconds_sum");
        let gw_cnt = m("laminardb_checkpoint_restorable_gate_wait_seconds_count");
        let cd_sum = m("laminardb_checkpoint_duration_seconds_sum");
        let cd_cnt = m("laminardb_checkpoint_duration_seconds_count");
        if gw_cnt > 0.0 {
            eprintln!(
                "soak: PROFILE gate-wait avg={:.0}ms over {} obs; checkpoint_duration avg={:.0}ms over {} obs",
                gw_sum / gw_cnt * 1000.0,
                gw_cnt as u64,
                cd_sum / cd_cnt.max(1.0) * 1000.0,
                cd_cnt as u64,
            );
        }
    }

    // Tier validation: scrape while every node is still live.
    // Demotions prove the budget→demote trigger fired on clean vnodes; fetches prove a row hit a
    // cold vnode and promotion read it back. Both must survive the kills (restart rehydrates
    // demoted vnodes from durable partials, not the wiped tier).
    if std::env::var("LAMINAR_SOAK_STATE_TIER").is_ok() {
        let sum = |name: &str| -> f64 { nodes.iter().filter_map(|n| n.metric(name)).sum() };
        let demotes = sum("laminardb_state_tier_demote_total");
        let fetches = sum("laminardb_state_tier_fetch_total");
        let resident = sum("laminardb_state_tier_bytes");
        let slices = sum("laminardb_state_tier_slices");
        let state = sum("laminardb_state_bytes");
        eprintln!(
            "soak: tier demotes={demotes} fetches={fetches} resident_bytes={resident} \
             slices={slices} in_memory_state_bytes={state}"
        );
        // `demotes` counts *effective* demotions (slices that actually left memory); with
        // fetches >0 this proves the demote→promote cycle ran end-to-end.
        assert!(
            demotes > 0.0,
            "tier enabled but no demotions — set LAMINAR_SOAK_BUDGET_BYTES below \
             the per-node state (and LAMINAR_SOAK_KILLS=0 to avoid rebalance churn \
             holding state dirty)",
        );
        assert!(
            fetches > 0.0,
            "tier demoted but never promoted — lower LAMINAR_SOAK_GROUPS so demoted keys \
             are revisited (a row must hit a cold vnode to trigger a fetch)",
        );
    }

    producer.stop();
}

/// Create `topic` with `partitions` partitions (blocking; the admin API is async).
fn kafka_create_topic(brokers: &str, topic: &str, partitions: i32) {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .expect("admin client");
        let new = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
        let results = admin
            .create_topics([&new], &AdminOptions::new())
            .await
            .expect("create_topics");
        for result in results {
            if let Err((failed_topic, error)) = result {
                panic!("failed to create Kafka topic {failed_topic:?}: {error}");
            }
        }
    });
}

/// Produce an unbounded `{"seq": n}` stream, paced near `rps`, until the guard requests stop.
/// Every delivery is awaited so broker-side rejection or timeout fails the soak.
fn produce_seq(brokers: &str, topic: &str, rps: u64, stop: &AtomicBool) {
    use rdkafka::producer::{FutureProducer, FutureRecord};

    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .set("enable.idempotence", "true")
        .create()
        .expect("producer");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("producer runtime");
    runtime.block_on(async {
        let start = tokio::time::Instant::now();
        let mut n = 0i64;
        while !stop.load(Ordering::Acquire) {
            let payload = format!(r#"{{"seq":{n}}}"#);
            let key = n.to_string();
            producer
                .send(
                    FutureRecord::to(topic).payload(&payload).key(&key),
                    Duration::from_secs(10),
                )
                .await
                .unwrap_or_else(|(error, _)| panic!("Kafka delivery failed: {error}"));
            n = n.checked_add(1).expect("soak sequence overflow");
            let target = start + Duration::from_secs_f64(n as f64 / rps as f64);
            tokio::time::sleep_until(target).await;
        }
    });
}
