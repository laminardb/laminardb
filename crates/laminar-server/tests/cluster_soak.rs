//! Three-node real-binary checkpoint soak with `kill -9` fault injection.
//!
//! Spawns three `laminardb` processes in cluster mode (real gRPC control
//! plane — the path the in-process `cluster_integration` suites do NOT
//! cover) against a shared checkpoint store, runs tight-cadence
//! checkpoints, and repeatedly hard-kills the current leader and a
//! follower mid-epoch, verifying after every fault that:
//!
//! - the survivors keep committing (cluster-wide `checkpoint_epoch`
//!   advances within a bounded window),
//! - epochs never regress on any node (abandonment leaves gaps, never
//!   reuse),
//! - the restarted node rejoins and resumes committing.
//!
//! KNOWN GAP: the post-kill progress assertion currently wedges (durability gate +
//! shuffle alignment wait on the dead node's vnodes) — see
//! `docs/plans/cluster-leader-kill-failover-hardening.md`. Until that lands, expect
//! a `progress after kill` timeout on the first kill.
//!
//! Ignored by default — it spawns processes and runs for minutes:
//!
//! ```text
//! cargo test -p laminar-server --test cluster_soak -- --ignored --nocapture
//! ```
//!
//! Environment knobs:
//! - `LAMINAR_SOAK_SECONDS`      total soak duration (default 90)
//! - `LAMINAR_SOAK_INTERVAL_MS`  checkpoint cadence (default 500; floor 100)
//! - `LAMINAR_SOAK_CHECKPOINT_URL`  e.g. `s3://bucket/soak` for MinIO/S3
//!   (default: shared `file://` dir).
//! - `LAMINAR_SOAK_STATE_URL`  the `[state]` backend (vnode partials +
//!   durability gate); also takes `s3://` (default: shared `file://`).
//! - `LAMINAR_SOAK_S3_ENDPOINT` / `_ACCESS_KEY` / `_SECRET_KEY` /
//!   `_REGION`  forwarded into both storage maps.
//! - `LAMINAR_SOAK_KAFKA_BROKERS`  e.g. `127.0.0.1:19092` (the compose
//!   Redpanda). Adds a per-node exactly-once Kafka sink to the
//!   workload, and after the fault rounds diffs each topic
//!   (read_committed) against the generator's deterministic output:
//!   every seq must appear exactly once, no gaps, no duplicates —
//!   the exactly-once proof under kill -9.
//! - `LAMINAR_SOAK_STATE_TIER`  any value: enable the disk cold tier
//!   (build with `--features state-tier`). Adds a small memory budget
//!   and an `EMIT CHANGES` aggregation so state is demoted under load,
//!   then asserts demotion AND promotion counters moved across the
//!   kill -9 rounds. Knobs (tier mode only): `LAMINAR_SOAK_BUDGET_BYTES`
//!   (default 256 KiB), `LAMINAR_SOAK_VNODES` (256), `LAMINAR_SOAK_RPS`
//!   (400), `LAMINAR_SOAK_GROUPS` (2000 — the agg key-space size),
//!   `LAMINAR_SOAK_SPAN` (12 — consecutive rows per agg key).

use std::io::{Read, Write as _};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
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
}

impl Node {
    fn spawn(&mut self) {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .expect("node log file");
        let child = Command::new(env!("CARGO_BIN_EXE_laminardb"))
            .arg("--config")
            .arg(&self.config_path)
            .env(
                "RUST_LOG",
                "laminardb=info,laminar_server=info,laminar_db=info,laminar_core=info",
            )
            .stdout(Stdio::from(log.try_clone().expect("clone log handle")))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn laminardb");
        self.child = Some(child);
    }

    /// `kill -9` equivalent: no shutdown hooks, no final checkpoint.
    fn kill9(&mut self) {
        if let Some(mut c) = self.child.take() {
            c.kill().ok();
            c.wait().ok();
        }
    }

    /// Scrape one gauge/counter from `/metrics`. `None` while the node
    /// is down or still booting.
    fn metric(&self, name: &str) -> Option<f64> {
        let mut stream = TcpStream::connect(("127.0.0.1", self.http_port)).ok()?;
        stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .ok()?;
        let mut body = String::new();
        stream.read_to_string(&mut body).ok()?;
        body.lines()
            .find(|l| l.starts_with(name) && !l.starts_with('#'))
            .and_then(|l| l.split_whitespace().last())
            .and_then(|v| v.parse().ok())
    }

    fn epoch(&self) -> Option<f64> {
        self.metric("laminardb_checkpoint_epoch")
    }

    /// Committed checkpoints — the REAL progress signal.
    /// `checkpoint_epoch` advances on aborted epochs too (abandonment
    /// churns ids), so asserting on it only proves the control loop is
    /// alive, not that the cluster can actually complete a checkpoint.
    fn commits(&self) -> Option<f64> {
        self.metric("laminardb_checkpoints_completed_total")
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.kill9();
    }
}

/// Per-node sink topic. Unique per test process so reruns against a
/// long-lived broker never diff a previous run's records.
fn eo_topic(id: usize) -> String {
    format!("soak-eo-n{id}-{}", std::process::id())
}

fn write_config(dir: &Path, id: usize, interval_ms: u64, checkpoint_url: &str) -> PathBuf {
    let depth = env_u64("LAMINAR_SOAK_DEPTH", 4);
    // Vnode partials go through the [state] backend, NOT [checkpoint] -
    // without a SHARED state store each node writes partials to its own
    // local default and the leader durability gate (which lists the
    // full registry) can never seal an epoch. The first soak runs
    // missed this and masked it by asserting on epoch churn.
    let state_url = std::env::var("LAMINAR_SOAK_STATE_URL").unwrap_or_else(|_| {
        let shared = dir.join("state");
        std::fs::create_dir_all(&shared).unwrap();
        let fwd = shared.display().to_string().replace(char::from(92), "/");
        format!("file:///{fwd}")
    });
    let http = BASE_PORT + id as u16;
    let gossip = BASE_PORT + 100 + id as u16;
    let seeds: Vec<String> = (0..NODES)
        .map(|i| format!("\"127.0.0.1:{}\"", BASE_PORT + 100 + i as u16))
        .collect();
    let data_dir = dir.join(format!("node{id}-data"));
    std::fs::create_dir_all(&data_dir).unwrap();

    // Optional disk cold tier (Phase 5 soak). A tiny budget forces
    // demotion under load, a larger vnode ring keeps most vnodes clean
    // each capture interval (only clean vnodes are demotable), and the
    // `soak_agg` pipeline below gives the tier demotable per-vnode state
    // (the pass-through `soak_stream` has none). Bounded key space (mod
    // GROUPS) means demoted keys are revisited, driving promotion too.
    // Gated on the env var so default runs are byte-identical.
    let tier = std::env::var("LAMINAR_SOAK_STATE_TIER").is_ok();
    let rps = env_u64("LAMINAR_SOAK_RPS", if tier { 400 } else { 200 });
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

    // Discovery strategy: gossip (chitchat phi-accrual failure detection) by
    // default; `LAMINAR_SOAK_DISCOVERY=static` for the seed-list heartbeat path.
    let discovery = std::env::var("LAMINAR_SOAK_DISCOVERY").unwrap_or_else(|_| "gossip".into());

    let mut toml = format!(
        r#"
node_id = "n{id}"
storage_dir = "{data}"

[server]
mode = "cluster"
bind = "127.0.0.1:{http}"
{server_extra}
[discovery]
strategy = "{discovery}"
seeds = [{seeds}]
gossip_port = {gossip}
advertise_host = "127.0.0.1"

[coordination]
strategy = "raft"

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

[checkpoint.storage]
{storage}

# Workload: deterministic generator source + a pass-through stream so
# the pipeline (and checkpointing) actually runs.
[[source]]
name = "gen"
connector = "generator"
properties = {{ "rows.per.second" = "{rps}", "batch.max.size" = "256" }}

[[pipeline]]
name = "soak_stream"
sql = "SELECT seq, ts_ms, value FROM gen"
"#,
        data = data_dir.display().to_string().replace('\\', "/"),
        seeds = seeds.join(", "),
        url = checkpoint_url,
    );

    // Demotable per-vnode aggregate state for the cold tier: an EMIT
    // CHANGES agg over a SLOW-CYCLING bounded key space. Only changelog
    // aggs are demotable, and demotion only sheds vnodes that are CLEAN
    // (untouched since the last capture) — so the key must idle long
    // enough for whole vnodes to fall quiet. `(seq / SPAN) % GROUPS`
    // writes each key in a burst of SPAN consecutive rows, then moves
    // on; a key (and its vnode) is then idle for a full GROUPS*SPAN-row
    // cycle (→ demotable) before the cycle returns to it (→ promotable).
    // A plain `seq % GROUPS` instead scatters every key across the ring
    // every cycle, so no vnode is ever idle and demotion just thrashes.
    if tier {
        let groups = env_u64("LAMINAR_SOAK_GROUPS", 2000);
        let span = env_u64("LAMINAR_SOAK_SPAN", 12);
        toml.push_str(&format!(
            r#"
[[pipeline]]
name = "soak_agg"
sql = "SELECT (seq / {span}) % {groups} AS k, COUNT(*) AS n FROM gen GROUP BY (seq / {span}) % {groups} EMIT CHANGES"
"#,
        ));
    }

    // Optional exactly-once Kafka sink: each node writes its own topic
    // (its generator is an independent seq stream), so each topic must
    // be a dense 0..=max with no duplicates under read_committed.
    if let Ok(brokers) = std::env::var("LAMINAR_SOAK_KAFKA_BROKERS") {
        toml.push_str(&format!(
            r#"
[[sink]]
name = "soak_sink"
pipeline = "soak_stream"
connector = "kafka"
delivery = "exactly_once"

[sink.properties]
"bootstrap.servers" = "{brokers}"
topic = "{topic}"
format = "json"
"delivery.guarantee" = "exactly-once"
"#,
            topic = eo_topic(id),
        ));
    }

    let path = dir.join(format!("node{id}.toml"));
    std::fs::write(&path, toml).unwrap();
    path
}

/// Diff each node's sink topic against the generator's deterministic
/// output. The generator emits seq 0,1,2,… and the pipeline is a
/// pass-through, so under exactly-once the topic (read_committed) must
/// be DENSE: every seq from 0 to the max committed exactly once.
/// A gap = lost rows (offsets advanced past uncommitted output); a
/// duplicate = replayed rows leaked outside a transaction. Records
/// from transactions still open when the writer was killed are
/// invisible under read_committed — that's the abort path working.
fn verify_exactly_once_output(brokers: &str) {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

    for id in 0..NODES {
        let topic = eo_topic(id);
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", format!("soak-eo-diff-{}", std::process::id()))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("isolation.level", "read_committed")
            .create()
            .expect("diff consumer");
        // Assign every partition from broker metadata — hard-coding
        // partition 0 silently misses data if the broker auto-creates
        // the topic with more than one partition.
        let metadata = consumer
            .fetch_metadata(Some(&topic), Duration::from_secs(10))
            .expect("topic metadata");
        let partitions: Vec<i32> = metadata
            .topics()
            .first()
            .map(|t| {
                t.partitions()
                    .iter()
                    .map(rdkafka::metadata::MetadataPartition::id)
                    .collect()
            })
            .unwrap_or_default();
        assert!(!partitions.is_empty(), "{topic}: no partitions in metadata");
        let mut tpl = TopicPartitionList::new();
        for p in partitions {
            tpl.add_partition_offset(&topic, p, Offset::Beginning)
                .unwrap();
        }
        consumer.assign(&tpl).expect("assign");

        // Read until the topic goes idle (the LSO stops a
        // read_committed consumer ahead of any still-open transaction).
        let mut seqs: Vec<i64> = Vec::new();
        let mut idle = 0u32;
        while idle < 5 {
            match consumer.poll(Duration::from_secs(2)) {
                Some(Ok(msg)) => {
                    idle = 0;
                    let v: serde_json::Value =
                        serde_json::from_slice(msg.payload().unwrap_or_default())
                            .unwrap_or_else(|e| panic!("{topic}: undecodable sink record: {e}"));
                    seqs.push(
                        v["seq"]
                            .as_i64()
                            .unwrap_or_else(|| panic!("{topic}: record without seq: {v}")),
                    );
                }
                Some(Err(e)) => panic!("{topic}: consume error: {e}"),
                None => idle += 1,
            }
        }

        assert!(
            !seqs.is_empty(),
            "{topic}: no committed records — the sink never committed a transaction",
        );
        let count = seqs.len();
        let max = *seqs.iter().max().unwrap();
        seqs.sort_unstable();
        seqs.dedup();
        let duplicates = count - seqs.len();
        let mut gaps: Vec<(i64, i64)> = Vec::new(); // (expected, found)
        let mut expected = 0i64;
        for &s in &seqs {
            if s != expected {
                gaps.push((expected, s));
                expected = s;
            }
            expected += 1;
        }
        assert!(
            duplicates == 0 && gaps.is_empty(),
            "{topic}: exactly-once VIOLATED — {count} records, max seq {max}, \
             {duplicates} duplicate(s), gap(s) at {gaps:?}",
        );
        assert!(
            max >= 200,
            "{topic}: only {count} committed records (max seq {max}) — too little \
             output survived the fault rounds for the diff to be meaningful",
        );
        eprintln!("soak: {topic}: exactly-once OK — {count} records, dense 0..={max}");
    }
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

/// Highest epoch visible on any live node.
fn cluster_epoch(nodes: &[Node]) -> f64 {
    nodes.iter().filter_map(Node::epoch).fold(0.0, f64::max)
}

/// Total commits across live nodes (per-node counters; a killed node's
/// contribution drops out, so progress is always asserted relative to
/// a fresh reading, never an absolute floor).
fn cluster_commits(nodes: &[Node]) -> f64 {
    nodes.iter().filter_map(Node::commits).sum()
}

/// Assert the cluster COMMITS two more checkpoints within `window`
/// (sink 2PC + recovery point both key off commits — epoch numbers
/// also advance on aborts, so they prove nothing). Returns the new
/// epoch floor for logging. Leadership is a NodeId-hash order, not
/// spawn order, so rounds kill nodes round-robin — over the soak both
/// leader and followers get hit.
fn assert_progress(nodes: &[Node], floor: f64, window: Duration, label: &str) -> f64 {
    let target = cluster_commits(nodes) + 2.0;
    wait_for(
        &format!("{label}: cluster commits to reach {target}"),
        window,
        || cluster_commits(nodes) >= target,
    );
    let new_epoch = cluster_epoch(nodes);
    // Abandonment leaves gaps, never reuse — epochs must be monotonic.
    assert!(
        new_epoch >= floor,
        "{label}: cluster epoch regressed: {new_epoch} < previous floor {floor}",
    );
    new_epoch
}

#[test]
#[ignore = "spawns 3 real laminardb processes; run with --ignored"]
fn three_node_kill9_soak() {
    let soak_secs = env_u64("LAMINAR_SOAK_SECONDS", 90);
    let interval_ms = env_u64("LAMINAR_SOAK_INTERVAL_MS", 500).max(100);

    let dir = tempfile::tempdir().expect("tempdir");
    let shared = dir.path().join("checkpoints");
    std::fs::create_dir_all(&shared).unwrap();
    let default_url = format!(
        "file:///{}",
        shared.display().to_string().replace('\\', "/")
    );
    let url = std::env::var("LAMINAR_SOAK_CHECKPOINT_URL").unwrap_or(default_url);

    // Node logs go under target/ (not the tempdir) so they survive a
    // failed run for post-mortem; the path is printed up front.
    let log_dir =
        Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{}", std::process::id()));
    std::fs::create_dir_all(&log_dir).unwrap();
    eprintln!("soak: node logs in {}", log_dir.display());

    let mut nodes: Vec<Node> = (0..NODES)
        .map(|id| Node {
            id,
            config_path: write_config(dir.path(), id, interval_ms, &url),
            log_path: log_dir.join(format!("node{id}.log")),
            child: None,
            http_port: BASE_PORT + id as u16,
        })
        .collect();
    for n in &mut nodes {
        n.spawn();
        // Stagger so node 0 (lowest id) is the stable initial leader.
        std::thread::sleep(Duration::from_millis(500));
    }

    // Cluster forms and commits its first epochs; on boot failure dump
    // the node logs so the cause is visible in test output.
    let boot = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        wait_for(
            "all nodes serving /metrics",
            Duration::from_secs(60),
            || nodes.iter().all(|n| n.epoch().is_some()),
        );
    }));
    if boot.is_err() {
        for n in &nodes {
            eprintln!("--- node{} log tail:", n.id);
            if let Ok(log) = std::fs::read_to_string(&n.log_path) {
                for line in log.lines().rev().take(20).collect::<Vec<_>>().iter().rev() {
                    eprintln!("  {line}");
                }
            }
        }
        panic!("soak: cluster failed to boot — node log tails above");
    }
    // First epochs: a pre-join epoch can burn one full 30s gate
    // timeout before the cluster converges, so allow for it.
    let mut floor = assert_progress(&nodes, 0.0, Duration::from_secs(90), "startup");
    eprintln!("soak: cluster up, epoch {floor}");

    // Cap on kill rounds. `LAMINAR_SOAK_KILLS=0` runs a steady, no-fault
    // soak — the right vehicle to observe *effective* demotion/promotion,
    // since rebalance rehydration (every kill) sets `dirty_all` and refuses
    // demotion until the next capture. Default: kill for the whole window.
    let max_kills = env_u64("LAMINAR_SOAK_KILLS", u64::MAX);
    let deadline = Instant::now() + Duration::from_secs(soak_secs);
    let mut round = 0u32;
    let mut kills = 0u64;
    while Instant::now() < deadline {
        round += 1;
        if kills < max_kills {
            // kill -9 a node mid-epoch (no drain, no final checkpoint —
            // the cadence guarantees an epoch is in flight). Round-robin:
            // over the soak this hits the leader and every follower.
            let victim = (kills as usize) % NODES;
            kills += 1;
            eprintln!("soak round {round}: kill -9 node {victim}");
            nodes[victim].kill9();
            floor = assert_progress(
                &nodes,
                floor,
                Duration::from_secs(90),
                "progress after kill",
            );

            // Restart it; it must rejoin and the cluster keeps committing.
            nodes[victim].spawn();
            wait_for(
                "killed node serving /metrics again",
                Duration::from_secs(60),
                || nodes[victim].epoch().is_some(),
            );
            floor = assert_progress(
                &nodes,
                floor,
                Duration::from_secs(90),
                "progress after rejoin",
            );
        } else {
            // No-fault steady state: just confirm the cluster keeps
            // committing, then pace the loop so demotion has clean windows.
            floor = assert_progress(&nodes, floor, Duration::from_secs(90), "steady progress");
            std::thread::sleep(Duration::from_secs(5));
        }

        // Per-round tier snapshot: demotion/promotion evolve across the
        // run, and the final scrape can land just after a rebalance wiped
        // a node's tier — logging each round captures the peak.
        if std::env::var("LAMINAR_SOAK_STATE_TIER").is_ok() {
            let s = |m: &str| -> f64 { nodes.iter().filter_map(|n| n.metric(m)).sum() };
            eprintln!(
                "soak round {round} tier: demotes={} fetches={} resident_bytes={} \
                 slices={} in_memory_state={}",
                s("laminardb_state_tier_demote_total"),
                s("laminardb_state_tier_fetch_total"),
                s("laminardb_state_tier_bytes"),
                s("laminardb_state_tier_slices"),
                s("laminardb_state_bytes"),
            );
        }
    }

    eprintln!("soak: completed {round} rounds ({kills} kills), final epoch {floor}");

    // Tier validation: scrape while every node is still live (the Kafka
    // diff below kills them all). Demotions prove the budget→demote
    // trigger fired on clean vnodes after a committed capture; fetches
    // prove a row hit a cold vnode and promotion read it back. Both must
    // survive the kill -9 rounds (restart rehydrates demoted vnodes from
    // durable partials, not from the wiped tier).
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
        // `demotes` (state_tier_demote_total) counts *effective* demotions —
        // slices that actually left memory — and resident_bytes/slices show
        // what the cold tier still holds. Both >0 with fetches >0 proves the
        // demote→promote cycle ran end-to-end.
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

    if let Ok(brokers) = std::env::var("LAMINAR_SOAK_KAFKA_BROKERS") {
        // Let in-flight epochs commit, then stop every writer so the
        // diff reads a stable topic (transactions open at the kill are
        // aborted and invisible under read_committed).
        std::thread::sleep(Duration::from_secs(5));
        for n in &mut nodes {
            n.kill9();
        }
        verify_exactly_once_output(&brokers);
    }
}
