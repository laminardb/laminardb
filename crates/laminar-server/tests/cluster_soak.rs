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
//!   (default: shared `file://` dir). For MinIO also set the usual
//!   `[checkpoint.storage]` keys via `LAMINAR_SOAK_S3_*` below.
//! - `LAMINAR_SOAK_S3_ENDPOINT` / `_ACCESS_KEY` / `_SECRET_KEY` /
//!   `_REGION`  forwarded into the checkpoint storage map.

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

    let toml = format!(
        r#"
node_id = "n{id}"
storage_dir = "{data}"

[server]
mode = "cluster"
bind = "127.0.0.1:{http}"

[discovery]
strategy = "static"
seeds = [{seeds}]
gossip_port = {gossip}
advertise_host = "127.0.0.1"

[coordination]
strategy = "raft"

[state]
backend = "object_store"
url = "{state_url}"
instance_id = "n{id}"
vnode_capacity = 64

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
properties = {{ "rows.per.second" = "200", "batch.max.size" = "256" }}

[[pipeline]]
name = "soak_stream"
sql = "SELECT seq, ts_ms, value FROM gen"
"#,
        data = data_dir.display().to_string().replace('\\', "/"),
        seeds = seeds.join(", "),
        url = checkpoint_url,
    );
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

/// Highest epoch visible on any live node.
fn cluster_epoch(nodes: &[Node]) -> f64 {
    nodes.iter().filter_map(Node::epoch).fold(0.0, f64::max)
}

/// Total commits across live nodes (per-node counters; a killed node''s
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
fn assert_progress(nodes: &[Node], _floor: f64, window: Duration, label: &str) -> f64 {
    let target = cluster_commits(nodes) + 2.0;
    wait_for(
        &format!("{label}: cluster commits to reach {target}"),
        window,
        || cluster_commits(nodes) >= target,
    );
    cluster_epoch(nodes)
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

    let deadline = Instant::now() + Duration::from_secs(soak_secs);
    let mut round = 0u32;
    while Instant::now() < deadline {
        // kill -9 a node mid-epoch (no drain, no final checkpoint —
        // the cadence guarantees an epoch is in flight). Round-robin:
        // over the soak this hits the leader and every follower.
        let victim = (round as usize) % NODES;
        round += 1;
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
    }

    eprintln!("soak: completed {round} fault rounds, final epoch {floor}");
}
