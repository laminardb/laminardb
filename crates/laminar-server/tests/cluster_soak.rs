//! Real-binary checkpoint soaks with `kill -9` fault injection.
//!
//! Spawns three `laminardb` processes in cluster mode (real gRPC control plane) against a shared
//! checkpoint store, runs tight-cadence checkpoints, and repeatedly hard-kills the leader and a
//! follower mid-epoch. After every fault it asserts the survivors keep committing, observed
//! committed epochs strictly advance, and the restarted node rejoins and resumes. The cluster
//! workload is explicitly at-least-once; duplicate sink records are diagnostic, not a cluster
//! exactly-once certification. The single-node leg validates exact recovery of a finite source cut
//! and materialized state; it makes no sink exactly-once claim.
//!
//! Ignored by default — spawns processes and runs for minutes:
//!
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features --features cluster,aws,kafka \
//!   --test cluster_soak three_node_kill9_soak -- --ignored --nocapture
//! cargo test --profile soak -p laminar-server --no-default-features --features cluster \
//!   --test cluster_soak local_exact_source_state_kill9_soak -- --ignored --nocapture
//! ```
//!
//! Environment knobs:
//! - `LAMINAR_SOAK_SECONDS`      steady-soak duration after fault rounds (default 90)
//! - `LAMINAR_SOAK_INTERVAL_MS`  checkpoint cadence (default 500; minimum 100)
//! - `LAMINAR_SOAK_KILLS`  total fault rounds (local exact requires at least two)
//! - `LAMINAR_SOAK_CHECKPOINT_URL`  required cluster-shared checkpoint prefix
//! - `LAMINAR_SOAK_STATE_URL`  required cluster-shared state prefix for vnode partials
//! - `LAMINAR_SOAK_S3_ENDPOINT` / `_ACCESS_KEY` / `_SECRET_KEY` / `_REGION`  forwarded into both
//!   storage maps
//! - `LAMINAR_SOAK_KAFKA_SOURCE_BROKERS`  required shared Kafka/Redpanda source broker
//! - `LAMINAR_SOAK_KAFKA_PARTITIONS`  source topic partition count (default 96)
//! - `LAMINAR_SOAK_RPS`  source production rate
//! - `LAMINAR_SOAK_KEY_GROUPS`  stable cluster key-group count (default 64)
//! - `LAMINAR_SOAK_FAULT_INJECT_ROLE`  trigger one fatal cycle fault after steady state on the
//!   observed `leader` or a `follower`
//! - `LAMINAR_SOAK_MAX_RECOVERY_MS`  maximum time for each failover or restarted-node recovery
//!   phase (default and upper bound 90s, matching the liveness window)

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write as _};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const NODES: usize = 3;
/// Per-node ports: http = BASE + i, gossip = BASE + 100 + i.
const BASE_PORT: u16 = 19310;
const DEFAULT_CLUSTER_KEY_GROUPS: u32 = 64;
const DEFAULT_KAFKA_PARTITIONS: u64 = 96;
const OUTPUT_TOPIC_PARTITIONS: i32 = 1;
const RECOVERY_LIVENESS_WINDOW: Duration = Duration::from_secs(90);
const DEFAULT_MAX_RECOVERY_MS: u64 = 90_000;
const LOCAL_EXACT_PREFIX_CYCLES: u64 = 4;
const HARD_KILL_TIMEOUT: Duration = Duration::from_secs(10);
const OUTPUT_BOUNDARY_STABILITY: Duration = Duration::from_secs(3);

fn env_u64(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|error| panic!("{name}={value:?} is not a valid u64: {error}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(value)) => {
            panic!("{name} is not valid Unicode: {value:?}")
        }
    }
}

fn cluster_key_group_count() -> u32 {
    let key_groups = env_u64(
        "LAMINAR_SOAK_KEY_GROUPS",
        u64::from(DEFAULT_CLUSTER_KEY_GROUPS),
    );
    let key_groups = u32::try_from(key_groups).expect("LAMINAR_SOAK_KEY_GROUPS must fit in a u32");
    assert!(
        (1..=65_535).contains(&key_groups),
        "LAMINAR_SOAK_KEY_GROUPS must be in 1..=65535"
    );
    key_groups
}

fn cluster_kafka_partition_count() -> i32 {
    let partitions = env_u64("LAMINAR_SOAK_KAFKA_PARTITIONS", DEFAULT_KAFKA_PARTITIONS);
    let partitions = i32::try_from(partitions)
        .expect("LAMINAR_SOAK_KAFKA_PARTITIONS must fit in Kafka's signed partition count");
    assert!(
        partitions > 0,
        "LAMINAR_SOAK_KAFKA_PARTITIONS must be greater than zero"
    );
    partitions
}

fn recovery_ceiling() -> Duration {
    let ceiling_ms = env_u64("LAMINAR_SOAK_MAX_RECOVERY_MS", DEFAULT_MAX_RECOVERY_MS);
    let liveness_ms = u64::try_from(RECOVERY_LIVENESS_WINDOW.as_millis()).unwrap_or(u64::MAX);
    assert!(
        ceiling_ms > 0 && ceiling_ms <= liveness_ms,
        "LAMINAR_SOAK_MAX_RECOVERY_MS must be in 1..={liveness_ms} (the recovery liveness window)"
    );
    Duration::from_millis(ceiling_ms)
}

fn local_exact_prefix_rows(groups: u64, span: u64) -> u64 {
    groups
        .checked_mul(span)
        .and_then(|cycle| cycle.checked_mul(LOCAL_EXACT_PREFIX_CYCLES))
        .expect("local exact source prefix must fit in a u64")
}

fn validate_checkpoint_liveness(interval_ms: u64, recovery: Duration) {
    assert!(
        u128::from(interval_ms).saturating_mul(4) <= recovery.as_millis(),
        "checkpoint interval {interval_ms}ms leaves insufficient time for two commits within the {recovery:?} recovery ceiling"
    );
}

fn steady_progress_budget(interval_ms: u64) -> Duration {
    Duration::from_millis(interval_ms.saturating_mul(4).saturating_add(1000))
}

fn validate_local_source_liveness(
    rows: u64,
    rows_per_second: u64,
    interval_ms: u64,
    recovery: Duration,
) {
    let generation_ms = u128::from(rows)
        .saturating_mul(1000)
        .div_ceil(u128::from(rows_per_second));
    assert!(
        generation_ms >= u128::from(interval_ms).saturating_mul(4),
        "local exact prefix would finish in {generation_ms}ms; it must run for at least four {interval_ms}ms checkpoint intervals to exercise a moving-source fault"
    );
    assert!(
        generation_ms.saturating_mul(2) <= recovery.as_millis(),
        "local exact prefix needs {generation_ms}ms to generate; it must fit within half of the {recovery:?} recovery ceiling"
    );
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DurableCheckpointStatus {
    checkpoint_id: u64,
    epoch: u64,
}

fn checkpoint_epoch_advanced(previous: u64, current: u64) -> bool {
    current > previous
}

fn assert_checkpoint_epoch_advanced(previous: u64, current: u64, label: &str) {
    assert!(
        checkpoint_epoch_advanced(previous, current),
        "soak: {label} reused or regressed checkpoint epoch: previous={previous}, current={current}"
    );
}

fn log_line_reports_recovery(line: &str, checkpoint: DurableCheckpointStatus) -> bool {
    let has_field = |name: &str, value: u64| {
        [format!("{name}={value}"), format!("{name}: {value}")]
            .iter()
            .any(|needle| {
                line.match_indices(needle).any(|(offset, _)| {
                    line.as_bytes()
                        .get(offset + needle.len())
                        .is_none_or(|next| !next.is_ascii_digit())
                })
            })
    };
    line.contains("Recovered from unified checkpoint")
        && has_field("checkpoint_id", checkpoint.checkpoint_id)
        && has_field("epoch", checkpoint.epoch)
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
        let child = self
            .child
            .as_mut()
            .unwrap_or_else(|| panic!("node{} has no process to hard-kill", self.id));
        let pid = child.id();
        child.kill().unwrap_or_else(|error| {
            panic!("failed to hard-kill node{} pid {pid}: {error}", self.id)
        });
        let deadline = Instant::now() + HARD_KILL_TIMEOUT;
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Ok(None) => panic!(
                    "node{} pid {pid} did not exit within {HARD_KILL_TIMEOUT:?} after hard kill",
                    self.id
                ),
                Err(error) => panic!(
                    "failed to reap hard-killed node{} pid {pid}: {error}",
                    self.id
                ),
            }
        };
        self.child = None;

        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt as _;
            assert_eq!(
                status.signal(),
                Some(9),
                "node{} pid {pid} was not terminated by SIGKILL: {status}",
                self.id
            );
        }
        #[cfg(windows)]
        assert!(
            !status.success(),
            "node{} pid {pid} reported successful exit after TerminateProcess",
            self.id
        );
        #[cfg(not(any(unix, windows)))]
        assert!(
            !status.success(),
            "node{} pid {pid} reported successful exit after hard termination",
            self.id
        );
    }

    fn terminate_best_effort(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let _ = child.kill();
        let deadline = Instant::now() + HARD_KILL_TIMEOUT;
        while Instant::now() < deadline {
            match child.try_wait() {
                Ok(Some(_)) | Err(_) => break,
                Ok(None) => std::thread::sleep(Duration::from_millis(10)),
            }
        }
        if !matches!(child.try_wait(), Ok(Some(_))) {
            let _ = std::thread::Builder::new()
                .name(format!("soak-node-{}-reaper", self.id))
                .spawn(move || {
                    let _ = child.kill();
                    let _ = child.wait();
                });
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

    fn epoch(&self) -> Option<u64> {
        let epoch = self.metric("laminardb_checkpoint_epoch")?;
        // Prometheus samples are f64; above 2^53 an integer epoch is no longer exact.
        if !(0.0..=9_007_199_254_740_992.0).contains(&epoch) || epoch.fract() != 0.0 {
            return None;
        }
        Some(epoch as u64)
    }

    /// Committed checkpoints — the real progress signal (`checkpoint_epoch` also advances on aborts).
    fn commits(&self) -> Option<f64> {
        self.metric("laminardb_checkpoints_completed_total")
    }

    fn durable_checkpoint_status(&self) -> Option<DurableCheckpointStatus> {
        let body = self.http_get("/api/v1/cluster/checkpoints")?;
        let response = serde_json::from_str::<serde_json::Value>(&body).ok()?;
        let row = response.as_array()?.first()?;
        let checkpoint = DurableCheckpointStatus {
            checkpoint_id: json_u64(row.get("checkpoint_id")?)?,
            epoch: json_u64(row.get("epoch")?)?,
        };
        (checkpoint.checkpoint_id > 0 && checkpoint.epoch > 0).then_some(checkpoint)
    }

    fn log_len(&self) -> u64 {
        std::fs::metadata(&self.log_path).map_or(0, |metadata| metadata.len())
    }

    fn logged_recovery_since(
        &self,
        start_offset: u64,
        checkpoint: DurableCheckpointStatus,
    ) -> bool {
        let Ok(log) = std::fs::read(&self.log_path) else {
            return false;
        };
        let Ok(start_offset) = usize::try_from(start_offset) else {
            return false;
        };
        let Some(tail) = log.get(start_offset..) else {
            return false;
        };
        String::from_utf8_lossy(tail)
            .lines()
            .any(|line| log_line_reports_recovery(line, checkpoint))
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
        self.terminate_best_effort();
    }
}

struct ProducerGuard {
    stop: Arc<AtomicBool>,
    produced: Arc<AtomicU64>,
    handle: Option<JoinHandle<Vec<i64>>>,
}

struct ProducedPrefix {
    count: u64,
    end_offsets: Vec<i64>,
}

impl ProducerGuard {
    fn spawn(brokers: String, topic: String, partitions: i32, rps: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let produced = Arc::new(AtomicU64::new(0));
        let producer_stop = Arc::clone(&stop);
        let producer_count = Arc::clone(&produced);
        let handle = std::thread::spawn(move || {
            produce_seq(
                &brokers,
                &topic,
                partitions,
                rps,
                &producer_stop,
                &producer_count,
            )
        });
        Self {
            stop,
            produced,
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
            Ok(_) => panic!("Kafka producer stopped before the soak completed"),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    fn stop(&mut self) -> ProducedPrefix {
        self.stop.store(true, Ordering::Release);
        let end_offsets = self
            .handle
            .take()
            .expect("Kafka producer was already stopped")
            .join()
            .expect("Kafka producer thread failed");
        ProducedPrefix {
            count: self.produced.load(Ordering::Acquire),
            end_offsets,
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

    fn covers(&self, boundary: &[i64]) -> bool {
        self.committed_offsets().is_some_and(|offsets| {
            offsets.len() == boundary.len()
                && offsets
                    .iter()
                    .zip(boundary)
                    .all(|(committed, boundary)| committed >= boundary)
        })
    }
}

fn kafka_high_watermarks(
    consumer: &rdkafka::consumer::BaseConsumer,
    topic: &str,
    partitions: i32,
) -> Option<Vec<i64>> {
    use rdkafka::consumer::Consumer as _;

    (0..partitions)
        .map(|partition| {
            consumer
                .fetch_watermarks(topic, partition, Duration::from_secs(2))
                .ok()
                .map(|(_, high)| high)
        })
        .collect()
}

fn record_consumed_offset(consumed: &mut [i64], partition: i32, offset: i64) {
    let partition = usize::try_from(partition).expect("Kafka returned a negative partition");
    let next = offset.saturating_add(1);
    let consumed = consumed
        .get_mut(partition)
        .expect("Kafka returned an out-of-range partition");
    *consumed = (*consumed).max(next);
}

struct KafkaOutputOracle {
    consumer: rdkafka::consumer::BaseConsumer,
    topic: String,
    partitions: i32,
    consumed_offsets: Vec<i64>,
    seen: BTreeSet<u64>,
    duplicates: u64,
}

impl KafkaOutputOracle {
    fn new(brokers: &str, topic: &str, partitions: i32) -> Self {
        use rdkafka::consumer::Consumer;
        use rdkafka::{Offset, TopicPartitionList};

        let consumer: rdkafka::consumer::BaseConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set(
                "group.id",
                format!("laminardb-soak-output-oracle-{}", std::process::id()),
            )
            .set("enable.auto.commit", "false")
            .create()
            .expect("Kafka output oracle consumer");
        let partition_capacity =
            usize::try_from(partitions).expect("output topic partition count fits usize");
        let mut assignment = TopicPartitionList::with_capacity(partition_capacity);
        for partition in 0..partitions {
            assignment
                .add_partition_offset(topic, partition, Offset::Beginning)
                .expect("build output oracle assignment");
        }
        consumer
            .assign(&assignment)
            .expect("assign output oracle from beginning");
        Self {
            consumer,
            topic: topic.to_owned(),
            partitions,
            consumed_offsets: vec![0; partition_capacity],
            seen: BTreeSet::new(),
            duplicates: 0,
        }
    }

    fn high_watermarks(&self) -> Option<Vec<i64>> {
        kafka_high_watermarks(&self.consumer, &self.topic, self.partitions)
    }

    fn consumed_through(&self, boundary: &[i64]) -> bool {
        self.consumed_offsets.len() == boundary.len()
            && self
                .consumed_offsets
                .iter()
                .zip(boundary)
                .all(|(consumed, boundary)| consumed >= boundary)
    }

    fn drain(&mut self, produced_count: u64, boundary: &[i64]) -> usize {
        use rdkafka::message::Message;

        let mut drained = 0usize;
        while let Some(result) = self.consumer.poll(Duration::ZERO) {
            let message =
                result.unwrap_or_else(|error| panic!("Kafka output read failed: {error}"));
            assert_eq!(
                message.topic(),
                self.topic.as_str(),
                "output oracle read wrong topic"
            );
            let partition = usize::try_from(message.partition())
                .expect("Kafka output returned a negative partition");
            let frozen_end = *boundary
                .get(partition)
                .expect("Kafka output returned an out-of-range partition");
            assert!(
                message.offset() < frozen_end,
                "Kafka output appended after frozen boundary: partition={}, offset={}, boundary={frozen_end}",
                message.partition(),
                message.offset()
            );
            record_consumed_offset(
                &mut self.consumed_offsets,
                message.partition(),
                message.offset(),
            );
            let payload = message
                .payload()
                .expect("Kafka output record unexpectedly had a null payload");
            let value: serde_json::Value = serde_json::from_slice(payload)
                .unwrap_or_else(|error| panic!("invalid Kafka output JSON: {error}"));
            let seq = value
                .get("seq")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_else(|| panic!("Kafka output record has no non-negative integer seq"));
            assert!(
                seq < produced_count,
                "Kafka output contains seq {seq} outside produced range 0..{produced_count}"
            );
            if !self.seen.insert(seq) {
                self.duplicates += 1;
            }
            drained += 1;
        }
        drained
    }

    fn is_complete(&self, produced_count: u64) -> bool {
        let expected = usize::try_from(produced_count).expect("produced record count fits usize");
        self.seen.len() == expected
    }

    fn missing(&self, produced_count: u64) -> Vec<u64> {
        (0..produced_count)
            .filter(|seq| !self.seen.contains(seq))
            .take(16)
            .collect()
    }
}

fn assert_final_outputs(
    nodes: &mut [Node],
    output: &mut KafkaOutputOracle,
    produced_count: u64,
    output_boundary: &[i64],
    window: Duration,
) {
    assert!(produced_count > 0, "soak producer emitted no input records");
    let start = Instant::now();
    let mut quiet_since = None;
    while start.elapsed() < window {
        assert_running_nodes(nodes);
        let boundaries_stable = match output.high_watermarks() {
            Some(current_output) => {
                assert_eq!(
                    current_output, output_boundary,
                    "Kafka output high watermark changed after the durable input cut"
                );
                true
            }
            None => false,
        };
        let drained = output.drain(produced_count, output_boundary);
        if boundaries_stable
            && output.consumed_through(output_boundary)
            && output.is_complete(produced_count)
        {
            if drained == 0 {
                let quiet_since = quiet_since.get_or_insert_with(Instant::now);
                if quiet_since.elapsed() >= OUTPUT_BOUNDARY_STABILITY {
                    assert_eq!(
                        output.high_watermarks().as_deref(),
                        Some(output_boundary),
                        "Kafka output boundary changed during final drain"
                    );
                    eprintln!(
                        "soak: output oracle consumed the frozen broker boundary and observed all {produced_count} IDs with {} at-least-once duplicates",
                        output.duplicates
                    );
                    return;
                }
            } else {
                quiet_since = None;
            }
        } else {
            quiet_since = None;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = output.drain(produced_count, output_boundary);
    assert!(
        output.consumed_through(output_boundary),
        "soak: output oracle did not consume through frozen boundary {output_boundary:?}; consumed {:?}",
        output.consumed_offsets
    );
    if !output.is_complete(produced_count) {
        let missing = output.missing(produced_count);
        panic!(
            "soak: output oracle saw {}/{} IDs ({} duplicates); first missing IDs: {missing:?}",
            output.seen.len(),
            produced_count,
            output.duplicates
        );
    }
    panic!(
        "soak: frozen Kafka output boundaries did not remain drained and stable for {OUTPUT_BOUNDARY_STABILITY:?}"
    );
}

fn json_u64(value: &serde_json::Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|value| value.try_into().ok()))
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn json_u64_field(value: &serde_json::Value, field: &str) -> u64 {
    value
        .get(field)
        .and_then(json_u64)
        .unwrap_or_else(|| panic!("aggregate output has no non-negative integer {field}: {value}"))
}

fn expected_aggregate_count(produced_count: u64, key: u64, groups: u64, span: u64) -> u64 {
    assert!(key < groups, "aggregate key {key} is out of range");
    let cycle = groups.checked_mul(span).expect("aggregate cycle overflow");
    let complete = produced_count / cycle;
    let remainder = produced_count % cycle;
    let key_start = key
        .checked_mul(span)
        .expect("aggregate key offset overflow");
    complete
        .checked_mul(span)
        .and_then(|count| count.checked_add(remainder.saturating_sub(key_start).min(span)))
        .expect("aggregate count overflow")
}

fn aggregate_high_seq(key: u64, count: u64, groups: u64, span: u64) -> u64 {
    assert!(count > 0, "aggregate count must be positive");
    let cycle = groups.checked_mul(span).expect("aggregate cycle overflow");
    let index = count - 1;
    (index / span)
        .checked_mul(cycle)
        .and_then(|seq| seq.checked_add(key.checked_mul(span)?))
        .and_then(|seq| seq.checked_add(index % span))
        .expect("aggregate sequence overflow")
}

fn write_config(
    dir: &Path,
    id: usize,
    interval_ms: u64,
    key_groups: u32,
    checkpoint_url: &str,
    brokers: &str,
    input_topic: &str,
    output_topic: &str,
    consumer_group: &str,
) -> PathBuf {
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

    let toml = format!(
        r#"
node_id = "n{id}"

[server]
mode = "cluster"
bind = "127.0.0.1:{http}"
delivery = "at_least_once"
key_groups = {key_groups}
[discovery]
strategy = "{discovery}"
seeds = [{seeds}]
gossip_port = {gossip}
advertise_host = "127.0.0.1"

[state]
backend = "object_store"
url = "{state_url}"

[state.storage]
{storage}
[checkpoint]
url = "{url}"
interval = "{interval_ms}ms"
max_retained = 5

[checkpoint.storage]
{storage}

# Guaranteed delivery uses engine-owned vnode assignment. The group ID below is only the
# advisory broker-offset namespace; LaminarDB checkpoints remain the recovery authority.
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

[[sink]]
name = "soak_output"
pipeline = "soak_stream"
connector = "kafka"
[sink.properties]
"bootstrap.servers" = "{brokers}"
topic = "{output_topic}"
format = "json"
acks = "all"
"key.column" = "seq"
"#,
        seeds = seeds.join(", "),
        url = checkpoint_url,
    );

    let path = dir.join(format!("node{id}.toml"));
    std::fs::write(&path, toml).unwrap();
    path
}

fn write_local_exact_config(
    dir: &Path,
    interval_ms: u64,
    groups: u64,
    span: u64,
    max_rows: u64,
    rows_per_second: u64,
) -> PathBuf {
    let state_dir = dir.join("state");
    let checkpoint_dir = dir.join("checkpoints");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::create_dir_all(&checkpoint_dir).unwrap();
    let portable = |path: &Path| path.display().to_string().replace('\\', "/");
    let checkpoint_path = portable(&checkpoint_dir);
    let checkpoint_url = if checkpoint_path.starts_with('/') {
        format!("file://{checkpoint_path}")
    } else {
        format!("file:///{checkpoint_path}")
    };

    let config = format!(
        r#"
node_id = "local-exact"
sql = """
CREATE MATERIALIZED VIEW local_exact_agg AS
SELECT (seq / {span}) % {groups} AS k, COUNT(*) AS n, MAX(seq) AS hi
FROM gen
GROUP BY (seq / {span}) % {groups};
"""

[server]
mode = "single"
bind = "127.0.0.1:{BASE_PORT}"
delivery = "exactly_once"

[state]
backend = "local"
path = "{state_path}"

[checkpoint]
url = "{checkpoint_url}"
interval = "{interval_ms}ms"
timeout = "30s"
max_retained = 5

[[source]]
name = "gen"
connector = "generator"
properties = {{ "rows.per.second" = "{rows_per_second}", "batch.max.size" = "256", "max.rows" = "{max_rows}" }}
"#,
        state_path = portable(&state_dir),
    );
    let path = dir.join("local-exact.toml");
    std::fs::write(&path, config).unwrap();
    path
}

/// Wait until `pred` holds, polling, or panic with `what` at deadline.
fn wait_for(what: &str, deadline: Duration, mut pred: impl FnMut() -> bool) {
    let expires_at = Instant::now() + deadline;
    while remaining_at(expires_at, Instant::now()).is_some() {
        if pred() {
            return;
        }
        if let Some(remaining) = remaining_at(expires_at, Instant::now()) {
            std::thread::sleep(remaining.min(Duration::from_millis(250)));
        }
    }
    panic!("soak: timed out after {deadline:?} waiting for: {what}");
}

/// Require the complete materialized aggregate for a finite deterministic source prefix. A Tail
/// subscription is seeded from the recovered snapshot, so this validates restored state even when
/// the source is already exhausted and cannot hide a rollback by advancing beyond a baseline.
fn assert_local_exact_prefix(
    node: &mut Node,
    produced_count: u64,
    groups: u64,
    span: u64,
    deadline: Duration,
) {
    use std::io::ErrorKind;
    use tungstenite::stream::MaybeTlsStream;
    use tungstenite::{Error, Message};

    let start = Instant::now();
    let url = format!("ws://127.0.0.1:{}/ws/local_exact_agg", node.http_port);
    let (mut socket, _) = loop {
        node.assert_running();
        match tungstenite::connect(url.as_str()) {
            Ok(connected) => break connected,
            Err(_) if start.elapsed() < deadline => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => panic!("failed to connect local aggregate oracle: {error}"),
        }
    };
    if let MaybeTlsStream::Plain(stream) = socket.get_mut() {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set local aggregate WebSocket read timeout");
    }

    let mut latest = BTreeMap::new();
    while start.elapsed() < deadline {
        node.assert_running();
        match socket.read() {
            Ok(Message::Text(text)) => {
                let envelope: serde_json::Value =
                    serde_json::from_str(text.as_str()).expect("local aggregate WebSocket JSON");
                let Some(rows) = envelope.get("data").and_then(serde_json::Value::as_array) else {
                    continue;
                };
                for row in rows {
                    let key = json_u64_field(row, "k");
                    let count = json_u64_field(row, "n");
                    let high_seq = json_u64_field(row, "hi");
                    assert!(key < groups, "local aggregate key {key} is out of range");
                    assert!(count > 0, "local aggregate key {key} emitted zero rows");
                    assert_eq!(
                        high_seq,
                        aggregate_high_seq(key, count, groups, span),
                        "local exact aggregate diverged for key {key} at generator seq {high_seq}"
                    );
                    let final_count = expected_aggregate_count(produced_count, key, groups, span);
                    assert!(
                        count <= final_count,
                        "local exact aggregate key {key} inflated to {count}; frozen prefix requires {final_count}"
                    );
                    latest.insert(key, (count, high_seq));
                }
                if (0..groups).all(|key| {
                    let count = expected_aggregate_count(produced_count, key, groups, span);
                    latest.get(&key) == Some(&(count, aggregate_high_seq(key, count, groups, span)))
                }) {
                    return;
                }
            }
            Ok(Message::Ping(payload)) => socket
                .send(Message::Pong(payload))
                .expect("local aggregate WebSocket pong"),
            Ok(Message::Close(frame)) => {
                panic!("local aggregate oracle closed unexpectedly: {frame:?}")
            }
            Ok(_) => {}
            Err(Error::Io(error))
                if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(error) => panic!("local aggregate oracle failed: {error}"),
        }
    }
    let mismatches: Vec<_> = (0..groups)
        .filter_map(|key| {
            let expected = expected_aggregate_count(produced_count, key, groups, span);
            let observed = latest.get(&key).copied();
            (observed.map(|(count, _)| count) != Some(expected))
                .then_some((key, observed, expected))
        })
        .take(16)
        .collect();
    panic!("timed out waiting for exact local frozen prefix; mismatches: {mismatches:?}");
}

fn remaining_at(deadline: Instant, now: Instant) -> Option<Duration> {
    deadline
        .checked_duration_since(now)
        .filter(|remaining| !remaining.is_zero())
}

fn remaining_progress_window(deadline: Instant, label: &str) -> Duration {
    remaining_at(deadline, Instant::now())
        .unwrap_or_else(|| panic!("soak: {label} exhausted its shared progress window"))
}

fn select_follower_victim(
    leader: usize,
    previously_killed: &BTreeSet<usize>,
    rotation: usize,
) -> usize {
    let followers: Vec<usize> = (0..NODES).filter(|id| *id != leader).collect();
    assert!(!followers.is_empty(), "cluster has no follower to kill");
    followers
        .iter()
        .copied()
        .find(|id| !previously_killed.contains(id))
        .unwrap_or_else(|| followers[rotation % followers.len()])
}

fn assert_recovery_within(started: Instant, ceiling: Duration, label: &str) -> Duration {
    let elapsed = started.elapsed();
    assert!(
        elapsed <= ceiling,
        "soak: {label} took {elapsed:?}, exceeding recovery ceiling {ceiling:?}"
    );
    elapsed
}

fn local_checkpoint_epoch(nodes: &[Node]) -> u64 {
    let epochs: Vec<u64> = nodes
        .iter()
        .filter(|node| node.child.is_some())
        .map(|node| {
            node.epoch()
                .expect("expected-live node did not expose checkpoint_epoch")
        })
        .collect();
    assert_eq!(
        epochs.len(),
        1,
        "local checkpoint progress requires exactly one live node"
    );
    epochs[0]
}

fn converged_durable_checkpoint(nodes: &[Node]) -> Option<DurableCheckpointStatus> {
    let mut statuses = nodes
        .iter()
        .filter(|node| node.child.is_some())
        .map(Node::durable_checkpoint_status);
    let first = statuses.next().flatten()?;
    statuses
        .all(|status| status == Some(first))
        .then_some(first)
}

fn wait_for_converged_durable_checkpoint(
    nodes: &mut [Node],
    deadline: Instant,
    label: &str,
    previous: Option<DurableCheckpointStatus>,
) -> DurableCheckpointStatus {
    let mut converged = None;
    wait_for(
        &format!("{label}: every live node to converge on one durable checkpoint"),
        remaining_progress_window(deadline, label),
        || {
            assert_running_nodes(nodes);
            let Some(candidate) = converged_durable_checkpoint(nodes) else {
                return false;
            };
            if previous.is_some_and(|previous| {
                candidate.checkpoint_id <= previous.checkpoint_id
                    || candidate.epoch <= previous.epoch
            }) {
                return false;
            }
            converged = Some(candidate);
            true
        },
    );
    converged.expect("durable checkpoint convergence completed without a checkpoint")
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

fn assert_every_node_ingests(nodes: &mut [Node], producer: &mut ProducerGuard, window: Duration) {
    assert_running_nodes(nodes);
    producer.assert_running();
    let baselines: Vec<f64> = nodes
        .iter()
        .map(|node| {
            node.metric("laminardb_events_ingested_total")
                .expect("node did not expose events_ingested_total")
        })
        .collect();
    wait_for("every node to ingest assigned Kafka work", window, || {
        assert_running_nodes(nodes);
        producer.assert_running();
        nodes.iter().zip(&baselines).all(|(node, baseline)| {
            node.metric("laminardb_events_ingested_total")
                .is_some_and(|ingested| ingested > *baseline)
        })
    });
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

/// Assert two committed checkpoints and strict epoch advancement without requiring source input.
fn assert_checkpoint_progress(
    nodes: &mut [Node],
    window: Duration,
    label: &str,
    previous_epoch: u64,
) -> u64 {
    assert_running_nodes(nodes);
    let checkpoint_target = cluster_commits(nodes) + 2.0;
    wait_for(label, window, || {
        assert_running_nodes(nodes);
        try_cluster_metric(nodes, "laminardb_checkpoints_completed_total")
            .is_some_and(|commits| commits >= checkpoint_target)
    });
    let current_epoch = local_checkpoint_epoch(nodes);
    assert_checkpoint_epoch_advanced(previous_epoch, current_epoch, label);
    current_epoch
}

/// Assert two committed checkpoints over advancing source data. With Kafka, also require a new
/// broker offset commit so an empty-checkpoint loop cannot satisfy the soak.
fn assert_progress(
    nodes: &mut [Node],
    mut producer: Option<&mut ProducerGuard>,
    commit_oracle: Option<&KafkaCommitOracle>,
    window: Duration,
    label: &str,
    previous_checkpoint: Option<DurableCheckpointStatus>,
) -> DurableCheckpointStatus {
    let deadline = Instant::now() + window;
    assert_running_nodes(nodes);
    if let Some(producer) = producer.as_deref_mut() {
        producer.assert_running();
    }
    let ingested_target = cluster_metric(nodes, "laminardb_events_ingested_total") + 1.0;
    let emitted_target = cluster_metric(nodes, "laminardb_events_emitted_total") + 1.0;
    wait_for(
        &format!("{label}: source ingestion and graph output to advance"),
        remaining_progress_window(deadline, label),
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
            remaining_progress_window(deadline, label),
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
        remaining_progress_window(deadline, label),
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
    wait_for_converged_durable_checkpoint(nodes, deadline, label, previous_checkpoint)
}

fn assert_final_input_cut(
    nodes: &mut [Node],
    commit_oracle: &KafkaCommitOracle,
    input_boundary: &[i64],
    window: Duration,
    previous_checkpoint: DurableCheckpointStatus,
) -> DurableCheckpointStatus {
    let deadline = Instant::now() + window;
    assert_running_nodes(nodes);
    let checkpoint_target = cluster_commits(nodes) + 2.0;
    wait_for(
        "frozen input offsets and two later checkpoints to commit",
        remaining_progress_window(deadline, "final input cut"),
        || {
            assert_running_nodes(nodes);
            try_cluster_metric(nodes, "laminardb_checkpoints_completed_total")
                .is_some_and(|commits| commits >= checkpoint_target)
                && commit_oracle.covers(input_boundary)
        },
    );
    wait_for_converged_durable_checkpoint(
        nodes,
        deadline,
        "final input cut",
        Some(previous_checkpoint),
    )
}

#[test]
#[ignore = "spawns a real laminardb process; run with --ignored"]
fn local_exact_source_state_kill9_soak() {
    let steady_seconds = env_u64("LAMINAR_SOAK_SECONDS", 60);
    let interval_ms = env_u64("LAMINAR_SOAK_INTERVAL_MS", 300);
    assert!(
        interval_ms >= 100,
        "LAMINAR_SOAK_INTERVAL_MS must be at least 100"
    );
    let max_kills = env_u64("LAMINAR_SOAK_KILLS", 4);
    assert!(
        max_kills >= 2,
        "LAMINAR_SOAK_KILLS must be at least 2 (one moving-source and one exhausted-cut fault)"
    );
    let recovery_ceiling = recovery_ceiling();
    validate_checkpoint_liveness(interval_ms, recovery_ceiling);
    let groups = env_u64("LAMINAR_SOAK_GROUPS", 64);
    let span = env_u64("LAMINAR_SOAK_SPAN", 16);
    let rows_per_second = env_u64("LAMINAR_SOAK_RPS", 400);
    assert!(groups > 0, "LAMINAR_SOAK_GROUPS must be greater than zero");
    assert!(span > 0, "LAMINAR_SOAK_SPAN must be greater than zero");
    assert!(
        rows_per_second > 0,
        "LAMINAR_SOAK_RPS must be greater than zero"
    );
    let prefix_rows = local_exact_prefix_rows(groups, span);
    validate_local_source_liveness(prefix_rows, rows_per_second, interval_ms, recovery_ceiling);

    let dir = tempfile::tempdir().expect("local exact soak tempdir");
    let log_dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("soak-local-exact-{}", std::process::id()));
    std::fs::create_dir_all(&log_dir).unwrap();
    eprintln!("soak: local exact node logs in {}", log_dir.display());

    let mut node = Node {
        id: 0,
        config_path: write_local_exact_config(
            dir.path(),
            interval_ms,
            groups,
            span,
            prefix_rows,
            rows_per_second,
        ),
        log_path: log_dir.join("node0.log"),
        child: None,
        http_port: BASE_PORT,
        fault_trigger_path: None,
        // The runtime remains single-node. Building this ignored test with the `cluster` feature
        // makes the existing debug-only checkpoint gate available without adding a production
        // configuration dimension.
        checkpoint_gate_path: Some(dir.path().join("checkpoint-local-exact.arm")),
    };
    node.spawn();
    let mut initial_checkpoint = None;
    wait_for(
        "local exact node to commit its first checkpoint",
        recovery_ceiling,
        || {
            node.assert_running();
            if node.commits().is_some_and(|commits| commits >= 1.0) {
                initial_checkpoint = node.durable_checkpoint_status();
            }
            initial_checkpoint.is_some()
        },
    );
    let initial_checkpoint =
        initial_checkpoint.expect("first committed local checkpoint has no durable status");

    // First fault while the finite source is still moving. Recovery must select the exact durable
    // predecessor, ingest a non-empty suffix, and finish at the deterministic aggregate prefix.
    node.arm_checkpoint_kill("leader");
    wait_for(
        "local exact node to enter a moving-source checkpoint",
        recovery_ceiling.min(Duration::from_secs(45)),
        || {
            node.assert_running();
            node.checkpoint_kill_ready("leader")
        },
    );
    let moving_checkpoint = node
        .durable_checkpoint_status()
        .expect("moving local checkpoint has no preceding durable recovery cut");
    assert!(
        moving_checkpoint.checkpoint_id >= initial_checkpoint.checkpoint_id
            && moving_checkpoint.epoch >= initial_checkpoint.epoch,
        "moving-source durable cut regressed from {initial_checkpoint:?} to {moving_checkpoint:?}"
    );
    let moving_ingested = node
        .metric("laminardb_events_ingested_total")
        .expect("moving local node did not expose events_ingested_total");
    assert!(
        moving_ingested > 0.0 && moving_ingested < prefix_rows as f64,
        "moving-source fault did not land inside the finite prefix: ingested={moving_ingested}, prefix={prefix_rows}"
    );
    let moving_recovery_started = Instant::now();
    let moving_recovery_deadline = moving_recovery_started + recovery_ceiling;
    node.kill9();
    node.disarm_checkpoint_kill();
    let moving_recovery_log_offset = node.log_len();
    node.spawn();
    wait_for(
        "local node to recover the exact moving-source predecessor",
        remaining_progress_window(moving_recovery_deadline, "moving local exact recovery"),
        || {
            node.assert_running();
            node.logged_recovery_since(moving_recovery_log_offset, moving_checkpoint)
        },
    );
    wait_for(
        "recovered finite source to ingest a non-empty suffix",
        remaining_progress_window(moving_recovery_deadline, "moving local exact recovery"),
        || {
            node.assert_running();
            node.metric("laminardb_events_ingested_total")
                .is_some_and(|ingested| ingested > 0.0)
        },
    );
    assert_local_exact_prefix(
        &mut node,
        prefix_rows,
        groups,
        span,
        remaining_progress_window(moving_recovery_deadline, "moving local exact recovery"),
    );
    // Take the completion baseline only after the exact finite prefix is visible. Two later
    // completions make that prefix part of the durable cut used by the armed kill below.
    let mut latest_epoch = assert_checkpoint_progress(
        std::slice::from_mut(&mut node),
        remaining_progress_window(moving_recovery_deadline, "moving local exact recovery"),
        "durably checkpoint the finite local source prefix",
        moving_checkpoint.epoch,
    );
    let mut latest_checkpoint_id = moving_checkpoint.checkpoint_id;
    let moving_recovery_elapsed = assert_recovery_within(
        moving_recovery_started,
        recovery_ceiling,
        "moving local exact restart to final aggregate and checkpoint progress",
    );
    eprintln!(
        "soak round 1: moving local source recovered checkpoint {} epoch {}, ingested a suffix, and reached the exact prefix in {moving_recovery_elapsed:?}",
        moving_checkpoint.checkpoint_id, moving_checkpoint.epoch
    );

    for round in 2..=max_kills {
        node.arm_checkpoint_kill("leader");
        wait_for(
            "local exact node to enter its armed checkpoint phase",
            recovery_ceiling.min(Duration::from_secs(45)),
            || {
                node.assert_running();
                node.checkpoint_kill_ready("leader")
            },
        );
        let durable_checkpoint = node
            .durable_checkpoint_status()
            .expect("armed local checkpoint has no preceding durable recovery cut");
        // `.ready` is published while the new attempt is held in Snapshotting, so the latest
        // status is necessarily the preceding committed recovery cut, not the armed attempt.
        assert!(
            durable_checkpoint.epoch >= latest_epoch,
            "committed epoch regressed before the armed local checkpoint: previous={latest_epoch}, current={}",
            durable_checkpoint.epoch
        );
        assert!(
            durable_checkpoint.checkpoint_id > latest_checkpoint_id,
            "local checkpoint id reused or regressed: previous={latest_checkpoint_id}, current={}",
            durable_checkpoint.checkpoint_id
        );
        latest_checkpoint_id = durable_checkpoint.checkpoint_id;

        eprintln!(
            "soak round {round}: kill -9 local exact source/state node inside checkpoint after \
             durable epoch {} checkpoint {} covered the frozen {prefix_rows}-row prefix",
            durable_checkpoint.epoch, durable_checkpoint.checkpoint_id,
        );
        let recovery_started = Instant::now();
        let recovery_deadline = recovery_started + recovery_ceiling;
        node.kill9();
        node.disarm_checkpoint_kill();
        let recovery_log_offset = node.log_len();
        node.spawn();
        wait_for(
            "local node to report recovery from the exact durable checkpoint",
            remaining_progress_window(recovery_deadline, "local exact source/state recovery"),
            || {
                node.assert_running();
                node.logged_recovery_since(recovery_log_offset, durable_checkpoint)
            },
        );
        let ingested_after_recovery = node
            .metric("laminardb_events_ingested_total")
            .expect("restarted node did not expose events_ingested_total");
        assert_eq!(
            ingested_after_recovery, 0.0,
            "finite generator replayed input after recovering its exhausted durable source cut"
        );
        assert_local_exact_prefix(
            &mut node,
            prefix_rows,
            groups,
            span,
            remaining_progress_window(recovery_deadline, "local exact recovery"),
        );
        assert_eq!(
            node.metric("laminardb_events_ingested_total")
                .expect("restarted node stopped exposing events_ingested_total"),
            0.0,
            "finite generator replayed input while validating recovered materialized state"
        );
        latest_epoch = assert_checkpoint_progress(
            std::slice::from_mut(&mut node),
            remaining_progress_window(recovery_deadline, "local exact recovery"),
            "durable progress after local exact restart",
            durable_checkpoint.epoch,
        );
        let recovery_elapsed = assert_recovery_within(
            recovery_started,
            recovery_ceiling,
            "local exact restart to aggregate and checkpoint progress",
        );
        eprintln!(
            "soak round {round}: exact frozen source/state prefix recovered in {recovery_elapsed:?}"
        );
    }

    let steady_deadline = Instant::now() + Duration::from_secs(steady_seconds);
    while let Some(remaining) = remaining_at(steady_deadline, Instant::now()) {
        if remaining < steady_progress_budget(interval_ms) {
            std::thread::sleep(remaining);
            break;
        }
        latest_epoch = assert_checkpoint_progress(
            std::slice::from_mut(&mut node),
            remaining.min(recovery_ceiling),
            "local exact steady progress",
            latest_epoch,
        );
    }
    node.kill9();
}

#[test]
#[ignore = "spawns 3 real laminardb processes; run with --ignored"]
fn three_node_kill9_soak() {
    let soak_secs = env_u64("LAMINAR_SOAK_SECONDS", 90);
    let interval_ms = env_u64("LAMINAR_SOAK_INTERVAL_MS", 500);
    assert!(
        interval_ms >= 100,
        "LAMINAR_SOAK_INTERVAL_MS must be at least 100"
    );
    let recovery_ceiling = recovery_ceiling();
    validate_checkpoint_liveness(interval_ms, recovery_ceiling);
    let key_group_count = cluster_key_group_count();
    let kafka_partitions = cluster_kafka_partition_count();

    let dir = tempfile::tempdir().expect("tempdir");
    let url = std::env::var("LAMINAR_SOAK_CHECKPOINT_URL").expect(
        "three_node_kill9_soak requires cluster-shared LAMINAR_SOAK_CHECKPOINT_URL storage",
    );
    let brokers = std::env::var("LAMINAR_SOAK_KAFKA_SOURCE_BROKERS")
        .expect("three_node_kill9_soak requires LAMINAR_SOAK_KAFKA_SOURCE_BROKERS");
    let input_topic = format!("soak-cluster-in-{}", std::process::id());
    let output_topic = format!("soak-cluster-out-{}", std::process::id());
    let consumer_group = format!("soak-cluster-{}", std::process::id());
    // Kafka partitioning is independent from engine key-group cardinality. The provider hashes
    // each source/topic/partition identity onto the current key-group topology; the producer below
    // assigns records round-robin so every external partition receives deterministic traffic.
    kafka_create_topic(&brokers, &input_topic, kafka_partitions);
    kafka_create_topic(&brokers, &output_topic, OUTPUT_TOPIC_PARTITIONS);
    let commit_oracle =
        KafkaCommitOracle::new(&brokers, &consumer_group, &input_topic, kafka_partitions);
    let mut output_oracle =
        KafkaOutputOracle::new(&brokers, &output_topic, OUTPUT_TOPIC_PARTITIONS);
    let source_rps = env_u64("LAMINAR_SOAK_RPS", 400);
    assert!(source_rps > 0, "LAMINAR_SOAK_RPS must be greater than zero");
    let mut producer = ProducerGuard::spawn(
        brokers.clone(),
        input_topic.clone(),
        kafka_partitions,
        source_rps,
    );

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
                key_group_count,
                &url,
                &brokers,
                &input_topic,
                &output_topic,
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
    let mut latest_checkpoint = assert_progress(
        &mut nodes,
        Some(&mut producer),
        Some(&commit_oracle),
        Duration::from_secs(90),
        "startup",
        None,
    );
    eprintln!(
        "soak: cluster up, checkpoint {} epoch {}, ingested={}, Kafka committed offset sum={}",
        latest_checkpoint.checkpoint_id,
        latest_checkpoint.epoch,
        cluster_metric(&nodes, "laminardb_events_ingested_total"),
        commit_oracle.committed_offset_sum().unwrap_or(0)
    );
    assert_every_node_ingests(&mut nodes, &mut producer, Duration::from_secs(60));

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
        let recovery_started = Instant::now();
        let recovery_deadline = recovery_started + recovery_ceiling;
        nodes[victim].trigger_fault(role);
        wait_for(
            "selected node to report the injected pipeline fault",
            remaining_progress_window(recovery_deadline, "coordinated recovery")
                .min(Duration::from_secs(30)),
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
            remaining_progress_window(recovery_deadline, "coordinated recovery"),
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
        latest_checkpoint = assert_progress(
            &mut nodes,
            Some(&mut producer),
            Some(&commit_oracle),
            remaining_progress_window(recovery_deadline, "coordinated recovery"),
            "progress after coordinated recovery",
            Some(latest_checkpoint),
        );
        let recovery_elapsed = assert_recovery_within(
            recovery_started,
            recovery_ceiling,
            "coordinated recovery to resumed output",
        );
        eprintln!(
            "soak: coordinated {role} recovery complete in {recovery_elapsed:?}, checkpoint {} epoch {}",
            latest_checkpoint.checkpoint_id, latest_checkpoint.epoch
        );
    }

    let mut round = 0u32;
    let mut kills = 0u64;
    let mut leader_kills = 0u64;
    let mut follower_kills = 0u64;
    let mut killed_follower_nodes = BTreeSet::new();
    while kills < max_kills {
        round += 1;
        let leader = wait_for_stable_leader(&mut nodes, &mut producer);
        let (victim, victim_role) = if kills.is_multiple_of(NODES as u64) {
            leader_kills += 1;
            (leader, "leader")
        } else {
            let victim = select_follower_victim(
                leader,
                &killed_follower_nodes,
                usize::try_from(follower_kills).unwrap_or(usize::MAX),
            );
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
        let failover_started = Instant::now();
        let failover_deadline = failover_started + recovery_ceiling;
        nodes[victim].kill9();
        nodes[victim].disarm_checkpoint_kill();
        if victim_role == "follower" {
            killed_follower_nodes.insert(victim);
        }
        kills += 1;
        latest_checkpoint = assert_progress(
            &mut nodes,
            Some(&mut producer),
            Some(&commit_oracle),
            remaining_progress_window(failover_deadline, "kill-9 failover"),
            "progress after kill",
            Some(latest_checkpoint),
        );
        let failover_elapsed = assert_recovery_within(
            failover_started,
            recovery_ceiling,
            "kill-9 to survivor progress",
        );
        eprintln!(
            "soak round {round}: survivors advanced to checkpoint {} epoch {} in {failover_elapsed:?}",
            latest_checkpoint.checkpoint_id, latest_checkpoint.epoch
        );

        // Restart it under a separate SLO: serving metrics, rejoining membership, ingesting owned
        // work, and participating in durable progress all share one recovery deadline.
        let rejoin_started = Instant::now();
        let rejoin_deadline = rejoin_started + recovery_ceiling;
        nodes[victim].spawn();
        wait_for(
            "killed node serving /metrics again",
            remaining_progress_window(rejoin_deadline, "restarted-node recovery"),
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
            remaining_progress_window(rejoin_deadline, "restarted-node recovery"),
            || {
                assert_running_nodes(&mut nodes);
                producer.assert_running();
                has_full_membership(&nodes)
                    && nodes[victim]
                        .metric("laminardb_events_ingested_total")
                        .is_some_and(|ingested| ingested > victim_ingested)
            },
        );
        latest_checkpoint = assert_progress(
            &mut nodes,
            Some(&mut producer),
            Some(&commit_oracle),
            remaining_progress_window(rejoin_deadline, "restarted-node recovery"),
            "progress after rejoin",
            Some(latest_checkpoint),
        );
        let rejoin_elapsed = assert_recovery_within(
            rejoin_started,
            recovery_ceiling,
            "restarted node to durable owned-work progress",
        );
        eprintln!(
            "soak round {round}: node {victim} rejoined and resumed durable work in {rejoin_elapsed:?}"
        );
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
            killed_follower_nodes.len() >= NODES.saturating_sub(1),
            "cluster soak did not kill {} distinct follower-role nodes: {killed_follower_nodes:?}",
            NODES.saturating_sub(1)
        );
    }

    let steady_deadline = Instant::now() + Duration::from_secs(soak_secs);
    while let Some(remaining) = remaining_at(steady_deadline, Instant::now()) {
        if remaining < steady_progress_budget(interval_ms) {
            std::thread::sleep(remaining);
            break;
        }
        round += 1;
        latest_checkpoint = assert_progress(
            &mut nodes,
            Some(&mut producer),
            Some(&commit_oracle),
            remaining.min(RECOVERY_LIVENESS_WINDOW),
            "steady progress",
            Some(latest_checkpoint),
        );
        if let Some(remaining) = remaining_at(steady_deadline, Instant::now()) {
            std::thread::sleep(remaining.min(Duration::from_secs(5)));
        }
    }

    assert_running_nodes(&mut nodes);
    producer.assert_running();
    eprintln!(
        "soak: completed {round} rounds ({kills} kills: {leader_kills} leader, \
         {follower_kills} follower), final checkpoint {} epoch {}",
        latest_checkpoint.checkpoint_id, latest_checkpoint.epoch
    );

    // Freeze the exact broker-acknowledged input offsets. Require that source commits cover them
    // and two later checkpoints complete before freezing the sink broker boundaries.
    let produced_prefix = producer.stop();
    let produced_count = produced_prefix.count;
    assert!(produced_count > 0, "soak producer emitted no input records");
    assert!(
        produced_count >= u64::try_from(kafka_partitions).expect("Kafka partition count fits u64"),
        "soak produced {produced_count} rows for {kafka_partitions} input partitions; every vnode must receive work"
    );
    assert_eq!(
        produced_prefix.end_offsets.len(),
        usize::try_from(kafka_partitions).expect("Kafka partition count fits usize"),
        "producer did not report every input partition boundary"
    );
    assert!(
        produced_prefix.end_offsets.iter().all(|offset| *offset > 0),
        "producer did not write every input partition: {:?}",
        produced_prefix.end_offsets
    );
    latest_checkpoint = assert_final_input_cut(
        &mut nodes,
        &commit_oracle,
        &produced_prefix.end_offsets,
        recovery_ceiling,
        latest_checkpoint,
    );
    eprintln!(
        "soak: frozen input prefix is durable through checkpoint {} epoch {}",
        latest_checkpoint.checkpoint_id, latest_checkpoint.epoch
    );

    let boundary_deadline = Instant::now() + recovery_ceiling;
    let mut output_boundary = None;
    wait_for(
        "Kafka output high-watermark snapshots after the durable input cut",
        remaining_progress_window(boundary_deadline, "output boundary snapshot"),
        || {
            assert_running_nodes(&mut nodes);
            output_boundary = output_oracle.high_watermarks();
            output_boundary.is_some()
        },
    );
    let output_boundary = output_boundary.expect("output boundary wait completed without a value");
    assert_final_outputs(
        &mut nodes,
        &mut output_oracle,
        produced_count,
        &output_boundary,
        remaining_progress_window(boundary_deadline, "frozen output validation"),
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
/// Explicit round-robin assignment guarantees that every source partition and vnode receives
/// records; Kafka keys remain unique diagnostics and do not determine placement.
fn produce_seq(
    brokers: &str,
    topic: &str,
    partitions: i32,
    rps: u64,
    stop: &AtomicBool,
    produced: &AtomicU64,
) -> Vec<i64> {
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
        let mut n = 0u64;
        let partition_count = u64::try_from(partitions).expect("positive partition count");
        assert!(partition_count > 0, "partition count must be positive");
        let mut end_offsets =
            vec![0; usize::try_from(partition_count).expect("partition count fits usize")];
        while !stop.load(Ordering::Acquire) {
            let payload = format!(r#"{{"seq":{n}}}"#);
            let key = n.to_string();
            let partition =
                i32::try_from(n % partition_count).expect("round-robin partition fits i32");
            let delivery = producer
                .send(
                    FutureRecord::to(topic)
                        .payload(&payload)
                        .key(&key)
                        .partition(partition),
                    Duration::from_secs(10),
                )
                .await
                .unwrap_or_else(|(error, _)| panic!("Kafka delivery failed: {error}"));
            let partition = usize::try_from(delivery.partition)
                .expect("Kafka returned a negative delivery partition");
            let boundary = end_offsets
                .get_mut(partition)
                .expect("Kafka returned an out-of-range delivery partition");
            *boundary = (*boundary).max(delivery.offset.saturating_add(1));
            n = n.checked_add(1).expect("soak sequence overflow");
            produced.store(n, Ordering::Release);
            let target = start + Duration::from_secs_f64(n as f64 / rps as f64);
            tokio::time::sleep_until(target).await;
        }
        end_offsets
    })
}

#[test]
fn checkpoint_epoch_progress_requires_strict_advance() {
    assert!(checkpoint_epoch_advanced(7, 8));
    assert!(!checkpoint_epoch_advanced(7, 7));
    assert!(!checkpoint_epoch_advanced(7, 6));
}

#[test]
fn recovery_log_match_binds_checkpoint_and_epoch() {
    let expected = DurableCheckpointStatus {
        checkpoint_id: 41,
        epoch: 43,
    };
    assert!(log_line_reports_recovery(
        "Recovered from unified checkpoint checkpoint_id=41 epoch=43",
        expected
    ));
    assert!(log_line_reports_recovery(
        "Recovered from unified checkpoint checkpoint_id: 41 epoch: 43",
        expected
    ));
    assert!(!log_line_reports_recovery(
        "Recovered from unified checkpoint checkpoint_id=40 epoch=43",
        expected
    ));
    assert!(!log_line_reports_recovery(
        "Recovered from unified checkpoint checkpoint_id=41 epoch=42",
        expected
    ));
    assert!(!log_line_reports_recovery(
        "Recovered from unified checkpoint checkpoint_id=410 epoch=43",
        expected
    ));
}

#[test]
fn follower_selection_covers_distinct_nodes_before_rotating() {
    let mut killed = BTreeSet::new();
    let first = select_follower_victim(0, &killed, 0);
    killed.insert(first);
    let second = select_follower_victim(0, &killed, 1);
    assert_ne!(first, second);
    killed.insert(second);
    assert_eq!(killed.len(), NODES - 1);

    let rotated = select_follower_victim(0, &killed, 0);
    assert!(killed.contains(&rotated));
}

#[test]
fn follower_selection_preserves_distinct_coverage_across_leader_change() {
    let killed = BTreeSet::from([1]);
    assert_eq!(select_follower_victim(2, &killed, 0), 0);
}

#[test]
fn aggregate_prefix_formula_matches_sequential_source() {
    let groups = 3;
    let span = 2;
    for produced_count in 0..20 {
        for key in 0..groups {
            let rows: Vec<_> = (0..produced_count)
                .filter(|seq| (seq / span) % groups == key)
                .collect();
            assert_eq!(
                expected_aggregate_count(produced_count, key, groups, span),
                u64::try_from(rows.len()).expect("test row count fits u64")
            );
            for (index, seq) in rows.into_iter().enumerate() {
                assert_eq!(aggregate_high_seq(key, index as u64 + 1, groups, span), seq);
            }
        }
    }
}
