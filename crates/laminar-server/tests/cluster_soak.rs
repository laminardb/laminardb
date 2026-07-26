// The real-binary soak fixture keeps its process-control scaffolding explicit.
#![allow(
    clippy::assertions_on_constants,
    clippy::disallowed_types,
    clippy::too_many_arguments
)] // Fault-soak setup keeps explicit process inputs and platform-gated assertions.

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
#[cfg(feature = "kafka")]
use std::io::{Seek as _, SeekFrom};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
#[cfg(feature = "kafka")]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(feature = "kafka")]
use std::sync::Arc;
#[cfg(feature = "kafka")]
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[cfg(feature = "kafka")]
use laminar_core::cluster::control::{
    AssignmentSnapshot, CheckpointAssignmentAdoption, CheckpointParticipant,
    LocalProcessAuthorityEvidence, LocalProcessAuthorityIdentity,
};
#[cfg(feature = "kafka")]
use laminar_core::state::{CheckpointAttempt, CheckpointAttemptRelation};

const NODES: usize = 3;
/// Per-node ports: http = BASE + i, gossip = BASE + 100 + i.
const BASE_PORT: u16 = 19310;
const SOAK_CONSOLE_TOKEN: &str = "laminardb-cluster-soak";
#[cfg(feature = "kafka")]
const SOAK_HTTP_HEADER_MAX_BYTES: usize = 16 * 1_024;
#[cfg(feature = "kafka")]
const LOCAL_AUTHORITY_EVIDENCE_MAX_BYTES: usize = 4 * 1_024;
#[cfg(feature = "kafka")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_MAX_BYTES: usize = 64 * 1_024;
#[cfg(feature = "kafka")]
const ASSIGNMENT_SNAPSHOT_MAX_BYTES: usize = 4 * 1_024 * 1_024;
#[cfg(feature = "kafka")]
const LOCAL_AUTHORITY_EVIDENCE_SCHEMA: &str = "laminardb-local-authority-evidence/v1";
#[cfg(feature = "kafka")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_SCHEMA: &str =
    "laminardb-local-checkpoint-barrier-timings/v1";
#[cfg(feature = "kafka")]
const DEFAULT_CLUSTER_KEY_GROUPS: u32 = 64;
#[cfg(feature = "kafka")]
const DEFAULT_KAFKA_PARTITIONS: u64 = 96;
#[cfg(feature = "kafka")]
const OUTPUT_TOPIC_PARTITIONS: i32 = 1;
#[cfg(feature = "kafka")]
const SOAK_PRODUCER_MAX_IN_FLIGHT: usize = 4_096;
#[cfg(feature = "kafka")]
const ACTIVE_LOAD_SAMPLE_WINDOW: Duration = Duration::from_secs(15);
#[cfg(feature = "kafka")]
const ACTIVE_LOAD_MINIMUM_RATIO: f64 = 0.9;
#[cfg(feature = "kafka")]
const CHECKPOINT_PIPELINE_STALL_SLO_SECONDS: f64 = 1.024;
#[cfg(feature = "kafka")]
const CHECKPOINT_PIPELINE_STALL_SLO_NS: u64 = 1_024_000_000;
#[cfg(feature = "kafka")]
const MIN_CHECKPOINT_PIPELINE_STALL_OBSERVATIONS: u64 = 100;
#[cfg(feature = "kafka")]
const RECOVERY_RELEASE_LOG: &str = "coordinated recovery: releasing source gate";
#[cfg(feature = "kafka")]
const CHECKPOINT_ATTEMPT_RESERVED_LOG: &str = "checkpoint attempt reserved";
#[cfg(feature = "kafka")]
const CHECKPOINT_ATTEMPT_FAILED_LOG: &str = "checkpoint attempt failed";
#[cfg(feature = "kafka")]
const CHECKPOINT_ADMISSION_FAILED_LOG: &str = "checkpoint admission failed";
#[cfg(feature = "kafka")]
const CHECKPOINT_CONTINUATION_FAILED_LOG: &str = "checkpoint continuation failed";
#[cfg(feature = "kafka")]
const CHECKPOINT_FAILURE_METRIC_LOG: &str = "checkpoint failure metric recorded";
#[cfg(feature = "kafka")]
const RECOVERY_PREPARE_LOG: &str = "leader announced recovery prepare";
const RECOVERY_LIVENESS_WINDOW: Duration = Duration::from_secs(90);
const DEFAULT_MAX_RECOVERY_MS: u64 = 90_000;
const LOCAL_EXACT_PREFIX_CYCLES: u64 = 4;
const HARD_KILL_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(feature = "kafka")]
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

fn soak_run_id() -> String {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before the Unix epoch")
        .as_nanos();
    format!("{}-{nonce}", std::process::id())
}

#[cfg(feature = "kafka")]
fn cluster_key_group_count() -> u32 {
    let key_groups = env_u64(
        "LAMINAR_SOAK_KEY_GROUPS",
        u64::from(DEFAULT_CLUSTER_KEY_GROUPS),
    );
    let key_groups = u32::try_from(key_groups).expect("LAMINAR_SOAK_KEY_GROUPS must fit in a u32");
    let minimum = u32::try_from(NODES).expect("soak node count must fit in a u32");
    assert!(
        (minimum..=65_535).contains(&key_groups),
        "LAMINAR_SOAK_KEY_GROUPS must be in {minimum}..=65535 so every soak node can own a vnode"
    );
    key_groups
}

#[cfg(feature = "kafka")]
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
    #[cfg(feature = "kafka")]
    process_generation: u64,
    http_port: u16,
    /// Per-process one-shot fault trigger used only by the coordinated-recovery soak.
    fault_trigger_path: Option<PathBuf>,
    /// Debug checkpoint handshake used to prove a hard kill landed inside an active phase.
    checkpoint_gate_path: Option<PathBuf>,
}

#[cfg(feature = "kafka")]
#[derive(Debug)]
struct BoundedHttpResponse {
    status: u16,
    body: Vec<u8>,
}

#[cfg(feature = "kafka")]
#[derive(Debug)]
enum BoundedHttpError {
    Unavailable(String),
    Invalid(String),
}

#[cfg(feature = "kafka")]
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalAuthorityEvidenceEnvelope {
    schema_version: String,
    evidence: LocalProcessAuthorityEvidence,
}

#[cfg(feature = "kafka")]
#[derive(Debug)]
enum LocalAuthorityObservation {
    Pending(String),
    Available(LocalProcessAuthorityEvidence),
    Contradiction(String),
}

#[cfg(feature = "kafka")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum CheckpointBarrierTimingRole {
    Leader,
    Follower,
}

#[cfg(feature = "kafka")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointBarrierTimingRecord {
    sequence: u64,
    process: LocalProcessAuthorityIdentity,
    attempt: CheckpointAttempt,
    role: CheckpointBarrierTimingRole,
    assignment_version: u64,
    assignment_digest: [u8; 32],
    pipeline_stall_ns: u64,
    local_barrier_ns: u64,
    aligned_resume_ns: Option<u64>,
    durable_tail_handoff: bool,
    deadline_exhausted: bool,
}

#[cfg(feature = "kafka")]
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointBarrierTimingPage {
    capacity: usize,
    oldest_retained_sequence: Option<u64>,
    next_sequence: u64,
    overwritten_record_count: u64,
    recording_loss_count: u64,
    metadata_exhausted: bool,
    has_more: bool,
    records: Vec<CheckpointBarrierTimingRecord>,
}

#[cfg(feature = "kafka")]
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointBarrierTimingEnvelope {
    schema_version: String,
    process_identity: LocalProcessAuthorityIdentity,
    after_sequence: u64,
    page: CheckpointBarrierTimingPage,
}

#[cfg(feature = "kafka")]
#[derive(Debug)]
enum CheckpointBarrierTimingObservation {
    Pending(String),
    Available(CheckpointBarrierTimingEnvelope),
    Contradiction(String),
}

#[cfg(feature = "kafka")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CheckpointBarrierTimingMetadata {
    capacity: usize,
    oldest_retained_sequence: Option<u64>,
    next_sequence: u64,
    overwritten_record_count: u64,
    recording_loss_count: u64,
    metadata_exhausted: bool,
}

#[cfg(feature = "kafka")]
impl From<&CheckpointBarrierTimingPage> for CheckpointBarrierTimingMetadata {
    fn from(page: &CheckpointBarrierTimingPage) -> Self {
        Self {
            capacity: page.capacity,
            oldest_retained_sequence: page.oldest_retained_sequence,
            next_sequence: page.next_sequence,
            overwritten_record_count: page.overwritten_record_count,
            recording_loss_count: page.recording_loss_count,
            metadata_exhausted: page.metadata_exhausted,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DurableCheckpointStatus {
    checkpoint_id: u64,
    epoch: u64,
}

impl DurableCheckpointStatus {
    const fn is_canonical(self) -> bool {
        self.checkpoint_id != 0 && self.epoch != 0 && self.epoch == self.checkpoint_id
    }

    #[cfg(feature = "kafka")]
    fn require_canonical(self, context: &str) -> Result<Self, String> {
        if !self.is_canonical() {
            return Err(format!(
                "{context} carried a non-canonical identity: checkpoint_id={}, epoch={}; both fields must contain the same nonzero checkpoint ID",
                self.checkpoint_id, self.epoch
            ));
        }
        Ok(self)
    }

    #[cfg(feature = "kafka")]
    fn try_attempt(self, context: &str) -> Result<CheckpointAttempt, String> {
        self.require_canonical(context)?;
        Ok(CheckpointAttempt::canonical(self.checkpoint_id))
    }
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

fn log_line_has_u64_field(line: &str, name: &str, value: u64) -> bool {
    [format!("{name}={value}"), format!("{name}: {value}")]
        .iter()
        .any(|needle| {
            line.match_indices(needle).any(|(offset, _)| {
                let bytes = line.as_bytes();
                let starts_at_field_boundary = offset == 0
                    || bytes.get(offset - 1).is_some_and(|previous| {
                        !previous.is_ascii_alphanumeric() && *previous != b'_'
                    });
                starts_at_field_boundary
                    && bytes
                        .get(offset + needle.len())
                        .is_none_or(|next| !next.is_ascii_digit())
            })
        })
}

#[cfg(feature = "kafka")]
fn log_line_u64_field(line: &str, name: &str) -> Option<u64> {
    [format!("{name}="), format!("{name}: ")]
        .iter()
        .find_map(|needle| {
            line.match_indices(needle).find_map(|(offset, _)| {
                let bytes = line.as_bytes();
                let starts_at_field_boundary = offset == 0
                    || bytes.get(offset - 1).is_some_and(|previous| {
                        !previous.is_ascii_alphanumeric() && *previous != b'_'
                    });
                if !starts_at_field_boundary {
                    return None;
                }
                let digits = line[offset + needle.len()..]
                    .bytes()
                    .take_while(u8::is_ascii_digit)
                    .count();
                (digits > 0)
                    .then(|| line[offset + needle.len()..offset + needle.len() + digits].parse())?
                    .ok()
            })
        })
}

fn log_line_reports_recovery(line: &str, checkpoint: DurableCheckpointStatus) -> bool {
    checkpoint.is_canonical()
        && line.contains("Recovered from unified checkpoint")
        && log_line_has_u64_field(line, "checkpoint_id", checkpoint.checkpoint_id)
        && log_line_has_u64_field(line, "epoch", checkpoint.epoch)
}

#[cfg(feature = "kafka")]
fn log_line_reports_checkpoint_completion(line: &str, checkpoint: DurableCheckpointStatus) -> bool {
    checkpoint.is_canonical()
        && line.contains("checkpoint completed")
        && log_line_has_u64_field(line, "checkpoint_id", checkpoint.checkpoint_id)
        && log_line_has_u64_field(line, "epoch", checkpoint.epoch)
}

#[cfg(feature = "kafka")]
fn checkpoint_reservation_from_log_line(
    line: &str,
) -> Result<Option<DurableCheckpointStatus>, String> {
    if !line.contains(CHECKPOINT_ATTEMPT_RESERVED_LOG) {
        return Ok(None);
    }
    let checkpoint_id = log_line_u64_field(line, "checkpoint_id")
        .ok_or_else(|| "checkpoint reservation log omitted checkpoint_id".to_string())?;
    let epoch = log_line_u64_field(line, "epoch")
        .ok_or_else(|| "checkpoint reservation log omitted epoch".to_string())?;
    let checkpoint = DurableCheckpointStatus {
        checkpoint_id,
        epoch,
    };
    Ok(Some(
        checkpoint.require_canonical("checkpoint reservation log")?,
    ))
}

#[cfg(feature = "kafka")]
fn checkpoint_failure_metric_from_log_line(
    line: &str,
) -> Result<Option<DurableCheckpointStatus>, String> {
    if !line.contains(CHECKPOINT_FAILURE_METRIC_LOG) {
        return Ok(None);
    }
    let checkpoint_id = log_line_u64_field(line, "checkpoint_id")
        .ok_or_else(|| "checkpoint failure metric log omitted checkpoint_id".to_string())?;
    let epoch = log_line_u64_field(line, "epoch")
        .ok_or_else(|| "checkpoint failure metric log omitted epoch".to_string())?;
    let checkpoint = DurableCheckpointStatus {
        checkpoint_id,
        epoch,
    };
    Ok(Some(
        checkpoint.require_canonical("checkpoint failure metric log")?,
    ))
}

#[cfg(feature = "kafka")]
fn validate_post_release_checkpoint_lifecycle(
    logs: &[String],
    resumed_checkpoint: DurableCheckpointStatus,
) -> Result<DurableCheckpointStatus, String> {
    let resumed_attempt = resumed_checkpoint.try_attempt("resumed checkpoint")?;
    let mut post_release = Vec::with_capacity(logs.len());
    for (node_id, log) in logs.iter().enumerate() {
        let releases = log.matches(RECOVERY_RELEASE_LOG).count();
        if releases != 1 {
            return Err(format!(
                "node{node_id} consumed {releases} recovery Releases instead of exactly one"
            ));
        }
        let release_offset = log
            .find(RECOVERY_RELEASE_LOG)
            .expect("one Release was counted above");
        let after_release = &log[release_offset + RECOVERY_RELEASE_LOG.len()..];
        if let Some(failure) = after_release.lines().find(|line| {
            line.contains(CHECKPOINT_ATTEMPT_FAILED_LOG)
                || line.contains(CHECKPOINT_ADMISSION_FAILED_LOG)
                || line.contains(CHECKPOINT_CONTINUATION_FAILED_LOG)
        }) {
            return Err(format!(
                "node{node_id} reported a checkpoint lifecycle failure after recovery Release: {failure}"
            ));
        }
        post_release.push(after_release);
    }

    let mut reservations = Vec::new();
    for (node_id, log) in post_release.iter().enumerate() {
        for (line_index, line) in log.lines().enumerate() {
            if let Some(attempt) = checkpoint_reservation_from_log_line(line)? {
                reservations.push((attempt, node_id, line_index));
            }
        }
    }
    let Some(mut first_reservation) = reservations.first().copied() else {
        return Err("no checkpoint attempt was reserved after recovery Release".into());
    };
    for (index, left) in reservations.iter().enumerate() {
        let left_attempt = left.0.try_attempt("checkpoint reservation log")?;
        for right in &reservations[index + 1..] {
            let right_attempt = right.0.try_attempt("checkpoint reservation log")?;
            match left_attempt.relation_to(right_attempt) {
                CheckpointAttemptRelation::Older | CheckpointAttemptRelation::Newer => {}
                CheckpointAttemptRelation::Exact => {
                    return Err(format!(
                        "duplicate checkpoint reservation after recovery Release: checkpoint {} epoch {}",
                        left.0.checkpoint_id, left.0.epoch
                    ));
                }
                CheckpointAttemptRelation::Conflict => {
                    return Err(format!(
                        "conflicting checkpoint reservations after recovery Release: checkpoint {} epoch {} versus checkpoint {} epoch {}",
                        left.0.checkpoint_id,
                        left.0.epoch,
                        right.0.checkpoint_id,
                        right.0.epoch
                    ));
                }
            }
        }
        if left_attempt.relation_to(
            first_reservation
                .0
                .try_attempt("checkpoint reservation log")?,
        ) == CheckpointAttemptRelation::Older
        {
            first_reservation = *left;
        }
    }
    let first_attempt = first_reservation.0;
    match first_attempt
        .try_attempt("checkpoint reservation log")?
        .relation_to(resumed_attempt)
    {
        CheckpointAttemptRelation::Exact | CheckpointAttemptRelation::Older => {}
        CheckpointAttemptRelation::Newer | CheckpointAttemptRelation::Conflict => {
            return Err(format!(
                "resumed checkpoint {} epoch {} does not follow first post-Release checkpoint {} epoch {}",
                resumed_checkpoint.checkpoint_id,
                resumed_checkpoint.epoch,
                first_attempt.checkpoint_id,
                first_attempt.epoch
            ));
        }
    }
    let completion_follows = |reservation: (DurableCheckpointStatus, usize, usize)| {
        post_release[reservation.1]
            .lines()
            .skip(reservation.2 + 1)
            .any(|line| log_line_reports_checkpoint_completion(line, reservation.0))
    };
    if !completion_follows(first_reservation) {
        return Err(format!(
            "first checkpoint reserved after recovery Release did not complete after its reservation: checkpoint {} epoch {}",
            first_attempt.checkpoint_id, first_attempt.epoch
        ));
    }
    let Some(resumed_reservation) = reservations
        .iter()
        .copied()
        .find(|reservation| reservation.0 == resumed_checkpoint)
    else {
        return Err(format!(
            "exact resumed checkpoint {} epoch {} was not reserved after recovery Release",
            resumed_checkpoint.checkpoint_id, resumed_checkpoint.epoch
        ));
    };
    if !completion_follows(resumed_reservation) {
        return Err(format!(
            "exact resumed checkpoint {} epoch {} did not complete after its reservation",
            resumed_checkpoint.checkpoint_id, resumed_checkpoint.epoch
        ));
    }
    Ok(first_attempt)
}

fn remove_marker(path: &Path) {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("failed to remove soak marker '{}': {error}", path.display()),
    }
}

#[cfg(feature = "kafka")]
fn prometheus_histogram_bucket_value(body: &str, metric: &str, upper_bound: f64) -> Option<f64> {
    let mut found = false;
    let sum = body
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix(metric)?;
            let (labels, _) = rest.strip_prefix('{')?.split_once('}')?;
            let encoded_bound = labels.split(',').find_map(|label| {
                label
                    .strip_prefix("le=\"")?
                    .strip_suffix('"')?
                    .parse::<f64>()
                    .ok()
            })?;
            let tolerance = f64::EPSILON * upper_bound.abs().max(1.0) * 4.0;
            ((encoded_bound - upper_bound).abs() <= tolerance)
                .then(|| line.split_whitespace().last()?.parse::<f64>().ok())?
        })
        .inspect(|_| found = true)
        .sum();
    found.then_some(sum)
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
            .env("NO_COLOR", "1")
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
        let child = cmd.spawn().expect("spawn laminardb");
        #[cfg(feature = "kafka")]
        {
            self.process_generation = self
                .process_generation
                .checked_add(1)
                .expect("soak process generation overflow");
        }
        self.child = Some(child);
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

    fn http_request(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
        timeout: Duration,
    ) -> Option<String> {
        let mut stream = TcpStream::connect(("127.0.0.1", self.http_port)).ok()?;
        stream.set_read_timeout(Some(timeout)).ok()?;
        let body = body.unwrap_or_default();
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {SOAK_CONSOLE_TOKEN}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).ok()?;
        let mut response = String::new();
        stream.read_to_string(&mut response).ok()?;
        let (headers, body) = response.split_once("\r\n\r\n")?;
        if !headers.lines().next()?.contains(" 200 ") {
            return None;
        }
        Some(body.to_owned())
    }

    fn http_get(&self, path: &str) -> Option<String> {
        self.http_request("GET", path, None, Duration::from_secs(2))
    }

    #[cfg(feature = "kafka")]
    fn bounded_http_get(
        &self,
        path: &str,
        body_cap: usize,
        deadline: Instant,
    ) -> Result<BoundedHttpResponse, BoundedHttpError> {
        let remaining = || {
            remaining_at(deadline, Instant::now())
                .map(|duration| duration.min(Duration::from_secs(6)))
                .ok_or_else(|| {
                    BoundedHttpError::Unavailable(format!(
                        "node{} HTTP evidence deadline was exhausted",
                        self.id
                    ))
                })
        };
        let address = std::net::SocketAddr::from(([127, 0, 0, 1], self.http_port));
        let mut stream = TcpStream::connect_timeout(&address, remaining()?).map_err(|error| {
            BoundedHttpError::Unavailable(format!("node{} HTTP connect failed: {error}", self.id))
        })?;
        stream
            .set_write_timeout(Some(remaining()?))
            .map_err(|error| {
                BoundedHttpError::Unavailable(format!(
                    "node{} HTTP write-timeout setup failed: {error}",
                    self.id
                ))
            })?;
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {SOAK_CONSOLE_TOKEN}\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).map_err(|error| {
            BoundedHttpError::Unavailable(format!("node{} HTTP request failed: {error}", self.id))
        })?;
        let response_cap = SOAK_HTTP_HEADER_MAX_BYTES
            .checked_add(body_cap)
            .and_then(|cap| cap.checked_add(1))
            .ok_or_else(|| {
                BoundedHttpError::Invalid("bounded HTTP response cap overflow".to_string())
            })?;
        let mut response = Vec::with_capacity(response_cap.min(64 * 1_024));
        let mut chunk = [0_u8; 8 * 1_024];
        loop {
            stream
                .set_read_timeout(Some(remaining()?))
                .map_err(|error| {
                    BoundedHttpError::Unavailable(format!(
                        "node{} HTTP read-timeout setup failed: {error}",
                        self.id
                    ))
                })?;
            let available = response_cap - response.len();
            let read_cap = available.min(chunk.len());
            let read = stream.read(&mut chunk[..read_cap]).map_err(|error| {
                BoundedHttpError::Unavailable(format!(
                    "node{} HTTP response read failed: {error}",
                    self.id
                ))
            })?;
            if read == 0 {
                break;
            }
            response.extend_from_slice(&chunk[..read]);
            if response.len() == response_cap {
                return Err(BoundedHttpError::Invalid(format!(
                    "node{} HTTP response exceeded the {body_cap}-byte body budget",
                    self.id
                )));
            }
        }
        let _remaining = remaining()?;
        let header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|offset| offset + 4)
            .ok_or_else(|| {
                BoundedHttpError::Invalid(format!(
                    "node{} HTTP response lacked a header terminator",
                    self.id
                ))
            })?;
        if header_end > SOAK_HTTP_HEADER_MAX_BYTES {
            return Err(BoundedHttpError::Invalid(format!(
                "node{} HTTP headers exceeded {SOAK_HTTP_HEADER_MAX_BYTES} bytes",
                self.id
            )));
        }
        let headers = std::str::from_utf8(&response[..header_end]).map_err(|error| {
            BoundedHttpError::Invalid(format!(
                "node{} HTTP headers were not UTF-8: {error}",
                self.id
            ))
        })?;
        let status = headers
            .lines()
            .next()
            .and_then(|line| line.split_ascii_whitespace().nth(1))
            .ok_or_else(|| {
                BoundedHttpError::Invalid(format!("node{} HTTP status line was malformed", self.id))
            })?
            .parse::<u16>()
            .map_err(|error| {
                BoundedHttpError::Invalid(format!(
                    "node{} HTTP status was malformed: {error}",
                    self.id
                ))
            })?;
        let body = response.split_off(header_end);
        if body.len() > body_cap {
            return Err(BoundedHttpError::Invalid(format!(
                "node{} HTTP body was {} bytes; maximum is {body_cap}",
                self.id,
                body.len()
            )));
        }
        Ok(BoundedHttpResponse { status, body })
    }

    #[cfg(feature = "kafka")]
    fn local_authority_observation(&self, deadline: Instant) -> LocalAuthorityObservation {
        let response = match self.bounded_http_get(
            "/api/v1/cluster/local-evidence",
            LOCAL_AUTHORITY_EVIDENCE_MAX_BYTES,
            deadline,
        ) {
            Ok(response) => response,
            Err(BoundedHttpError::Unavailable(error)) => {
                return LocalAuthorityObservation::Pending(error);
            }
            Err(BoundedHttpError::Invalid(error)) => {
                return LocalAuthorityObservation::Contradiction(error);
            }
        };
        if response.status == 503 {
            return LocalAuthorityObservation::Pending(format!(
                "node{} local evidence is temporarily unavailable",
                self.id
            ));
        }
        if response.status != 200 {
            return LocalAuthorityObservation::Contradiction(format!(
                "node{} local evidence returned HTTP {}: {}",
                self.id,
                response.status,
                String::from_utf8_lossy(&response.body)
            ));
        }
        let envelope: LocalAuthorityEvidenceEnvelope = match serde_json::from_slice(&response.body)
        {
            Ok(envelope) => envelope,
            Err(error) => {
                return LocalAuthorityObservation::Contradiction(format!(
                    "node{} local evidence JSON was invalid: {error}",
                    self.id
                ));
            }
        };
        if envelope.schema_version != LOCAL_AUTHORITY_EVIDENCE_SCHEMA {
            return LocalAuthorityObservation::Contradiction(format!(
                "node{} local evidence schema was {:?}; expected {LOCAL_AUTHORITY_EVIDENCE_SCHEMA:?}",
                self.id, envelope.schema_version
            ));
        }
        let evidence = envelope.evidence;
        if evidence.participant.node_id == 0
            || evidence.participant.boot_incarnation.is_nil()
            || evidence.process_term == 0
        {
            return LocalAuthorityObservation::Contradiction(format!(
                "node{} local evidence carried a non-canonical process identity",
                self.id
            ));
        }
        let adoption = &evidence.adopted_assignment;
        if !adoption.is_canonical() || adoption.participant != evidence.participant {
            return LocalAuthorityObservation::Contradiction(format!(
                "node{} local adoption did not bind its top-level process identity",
                self.id
            ));
        }
        if remaining_at(deadline, Instant::now()).is_none() {
            return LocalAuthorityObservation::Pending(format!(
                "node{} local evidence deadline was exhausted",
                self.id
            ));
        }
        LocalAuthorityObservation::Available(evidence)
    }

    #[cfg(feature = "kafka")]
    fn checkpoint_barrier_timing_observation(
        &self,
        expected_process: Option<LocalProcessAuthorityIdentity>,
        after_sequence: u64,
        deadline: Instant,
    ) -> CheckpointBarrierTimingObservation {
        let mut path = format!(
            "/api/v1/cluster/local-checkpoint-barrier-timings?after_sequence={after_sequence}"
        );
        if let Some(process) = expected_process {
            use std::fmt::Write as _;
            write!(
                path,
                "&expected_node_id={}&expected_boot_incarnation={}&expected_process_term={}",
                process.participant.node_id,
                process.participant.boot_incarnation,
                process.process_term,
            )
            .expect("writing a timing query into String cannot fail");
        }
        let response = match self.bounded_http_get(
            &path,
            LOCAL_CHECKPOINT_BARRIER_TIMINGS_MAX_BYTES,
            deadline,
        ) {
            Ok(response) => response,
            Err(BoundedHttpError::Unavailable(error)) => {
                return CheckpointBarrierTimingObservation::Pending(error);
            }
            Err(BoundedHttpError::Invalid(error)) => {
                return CheckpointBarrierTimingObservation::Contradiction(error);
            }
        };
        if response.status == 503 {
            return CheckpointBarrierTimingObservation::Pending(format!(
                "node{} local checkpoint barrier timings are temporarily unavailable",
                self.id
            ));
        }
        if response.status != 200 {
            return CheckpointBarrierTimingObservation::Contradiction(format!(
                "node{} local checkpoint barrier timings returned HTTP {}: {}",
                self.id,
                response.status,
                String::from_utf8_lossy(&response.body)
            ));
        }
        let envelope: CheckpointBarrierTimingEnvelope = match serde_json::from_slice(&response.body)
        {
            Ok(envelope) => envelope,
            Err(error) => {
                return CheckpointBarrierTimingObservation::Contradiction(format!(
                    "node{} local checkpoint barrier timing JSON was invalid: {error}",
                    self.id
                ));
            }
        };
        if envelope.schema_version != LOCAL_CHECKPOINT_BARRIER_TIMINGS_SCHEMA {
            return CheckpointBarrierTimingObservation::Contradiction(format!(
                "node{} local checkpoint barrier timing schema was {:?}; expected {LOCAL_CHECKPOINT_BARRIER_TIMINGS_SCHEMA:?}",
                self.id, envelope.schema_version
            ));
        }
        if envelope.after_sequence != after_sequence {
            return CheckpointBarrierTimingObservation::Contradiction(format!(
                "node{} local checkpoint barrier timing echoed cursor {}, requested {after_sequence}",
                self.id, envelope.after_sequence
            ));
        }
        if !envelope.process_identity.is_canonical() {
            return CheckpointBarrierTimingObservation::Contradiction(format!(
                "node{} local checkpoint barrier timing identity was non-canonical",
                self.id
            ));
        }
        if expected_process.is_some_and(|expected| expected != envelope.process_identity) {
            return CheckpointBarrierTimingObservation::Contradiction(format!(
                "node{} local checkpoint barrier timing response changed process identity",
                self.id
            ));
        }
        if remaining_at(deadline, Instant::now()).is_none() {
            return CheckpointBarrierTimingObservation::Pending(format!(
                "node{} local checkpoint barrier timing deadline was exhausted",
                self.id
            ));
        }
        CheckpointBarrierTimingObservation::Available(envelope)
    }

    #[cfg(feature = "kafka")]
    fn durable_assignment_observation(
        &self,
        deadline: Instant,
    ) -> Result<Option<AssignmentSnapshot>, String> {
        let response = match self.bounded_http_get(
            "/api/v1/cluster/vnodes",
            ASSIGNMENT_SNAPSHOT_MAX_BYTES,
            deadline,
        ) {
            Ok(response) => response,
            Err(BoundedHttpError::Unavailable(_)) => return Ok(None),
            Err(BoundedHttpError::Invalid(error)) => return Err(error),
        };
        if response.status == 503 {
            return Ok(None);
        }
        if response.status != 200 {
            return Err(format!(
                "node{} durable assignment returned HTTP {}: {}",
                self.id,
                response.status,
                String::from_utf8_lossy(&response.body)
            ));
        }
        let snapshot: AssignmentSnapshot =
            serde_json::from_slice(&response.body).map_err(|error| {
                format!(
                    "node{} durable assignment JSON was invalid: {error}",
                    self.id
                )
            })?;
        snapshot.assignment_fence().map_err(|error| {
            format!(
                "node{} durable assignment was non-canonical: {error}",
                self.id
            )
        })?;
        if remaining_at(deadline, Instant::now()).is_none() {
            return Ok(None);
        }
        Ok(Some(snapshot))
    }

    #[cfg(feature = "kafka")]
    fn is_ready(&self) -> bool {
        self.http_get("/ready").is_some()
    }

    fn sql(&self, statement: &str) -> Option<serde_json::Value> {
        let request = serde_json::to_string(&serde_json::json!({ "sql": statement })).ok()?;
        serde_json::from_str(&self.http_request(
            "POST",
            "/api/v1/sql",
            Some(&request),
            Duration::from_secs(6),
        )?)
        .ok()
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

    #[cfg(feature = "kafka")]
    fn checkpoint_latency_metrics(&self) -> Option<CheckpointLatencySnapshot> {
        let body = self.http_get("/metrics")?;
        let value = |name: &str| {
            let values = body
                .lines()
                .filter(|line| {
                    line.strip_prefix(name)
                        .is_some_and(|rest| rest.starts_with(' ') || rest.starts_with('{'))
                })
                .map(|line| line.split_whitespace().last()?.parse::<f64>().ok())
                .collect::<Option<Vec<_>>>()?;
            (!values.is_empty()).then(|| values.into_iter().sum())
        };
        Some(CheckpointLatencySnapshot {
            gate_wait_seconds: value("laminardb_checkpoint_restorable_gate_wait_seconds_sum")?,
            gate_wait_observations: value(
                "laminardb_checkpoint_restorable_gate_wait_seconds_count",
            )?,
            checkpoint_seconds: value("laminardb_checkpoint_duration_seconds_sum")?,
            checkpoint_observations: value("laminardb_checkpoint_duration_seconds_count")?,
            pipeline_stall_observations: value(
                "laminardb_checkpoint_pipeline_stall_duration_seconds_count",
            )?,
            pipeline_stall_within_slo: prometheus_histogram_bucket_value(
                &body,
                "laminardb_checkpoint_pipeline_stall_duration_seconds_bucket",
                CHECKPOINT_PIPELINE_STALL_SLO_SECONDS,
            )?,
            barrier_local_seconds: value(
                "laminardb_checkpoint_barrier_local_duration_seconds_sum",
            )?,
            barrier_local_observations: value(
                "laminardb_checkpoint_barrier_local_duration_seconds_count",
            )?,
            barrier_local_within_slo: prometheus_histogram_bucket_value(
                &body,
                "laminardb_checkpoint_barrier_local_duration_seconds_bucket",
                CHECKPOINT_PIPELINE_STALL_SLO_SECONDS,
            )?,
            aligned_resume_seconds: value("laminardb_checkpoint_aligned_resume_wait_seconds_sum")?,
            aligned_resume_observations: value(
                "laminardb_checkpoint_aligned_resume_wait_seconds_count",
            )?,
            aligned_resume_within_slo: prometheus_histogram_bucket_value(
                &body,
                "laminardb_checkpoint_aligned_resume_wait_seconds_bucket",
                CHECKPOINT_PIPELINE_STALL_SLO_SECONDS,
            )?,
        })
    }

    #[cfg(feature = "kafka")]
    fn is_leader(&self) -> Option<bool> {
        let body = self.http_get("/api/v1/cluster/leader")?;
        serde_json::from_str::<serde_json::Value>(&body)
            .ok()?
            .get("is_leader")?
            .as_bool()
    }

    #[cfg(feature = "kafka")]
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

    #[cfg(feature = "kafka")]
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
        remove_marker(path);
        remove_marker(&path.with_extension("ready"));
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
            remove_marker(path);
            remove_marker(&path.with_extension("ready"));
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
        checkpoint.is_canonical().then_some(checkpoint)
    }

    fn log_len(&self) -> u64 {
        std::fs::metadata(&self.log_path).map_or(0, |metadata| metadata.len())
    }

    #[cfg(feature = "kafka")]
    fn log_since(&self, start_offset: u64) -> String {
        let mut log = std::fs::File::open(&self.log_path)
            .unwrap_or_else(|error| panic!("read node{} soak log: {error}", self.id));
        let log_len = log
            .metadata()
            .unwrap_or_else(|error| panic!("inspect node{} soak log: {error}", self.id))
            .len();
        assert!(
            start_offset <= log_len,
            "node{} log shrank during the soak: {log_len} < {start_offset}",
            self.id
        );
        log.seek(SeekFrom::Start(start_offset))
            .unwrap_or_else(|error| panic!("seek node{} soak log: {error}", self.id));
        let mut tail = Vec::new();
        log.read_to_end(&mut tail)
            .unwrap_or_else(|error| panic!("read node{} soak log tail: {error}", self.id));
        String::from_utf8_lossy(&tail).into_owned()
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

#[cfg(feature = "kafka")]
struct ProducerGuard {
    stop: Arc<AtomicBool>,
    enqueued: Arc<AtomicU64>,
    handle: Option<JoinHandle<ProducedPrefix>>,
}

#[cfg(feature = "kafka")]
struct ProducedPrefix {
    count: u64,
    end_offsets: Vec<i64>,
    elapsed: Duration,
}

#[cfg(feature = "kafka")]
struct ExplicitFaultEvidence {
    victim: usize,
    recovery_leader: usize,
    log_offsets: Vec<u64>,
    pipeline_fault_baselines: Vec<f64>,
    recovery_baselines: Vec<f64>,
    recovery_failure_baselines: Vec<f64>,
    checkpoint_failure_baselines: Vec<f64>,
    checkpoint_failure_totals: Vec<f64>,
    interrupted_checkpoint: Option<DurableCheckpointStatus>,
    resumed_checkpoint: DurableCheckpointStatus,
}

#[cfg(feature = "kafka")]
struct CheckpointFailureSnapshot {
    totals: Vec<f64>,
    log_offsets: Vec<u64>,
    log_prefixes: Vec<Vec<u8>>,
}

#[cfg(feature = "kafka")]
fn capture_checkpoint_failure_snapshot(
    nodes: &[Node],
    timeout: Duration,
) -> CheckpointFailureSnapshot {
    let deadline = Instant::now() + timeout;
    loop {
        let before = nodes
            .iter()
            .map(|node| node.metric("laminardb_checkpoints_failed_total"))
            .collect::<Option<Vec<_>>>();
        let log_offsets = nodes.iter().map(Node::log_len).collect::<Vec<_>>();
        let log_prefixes = nodes
            .iter()
            .zip(&log_offsets)
            .map(|(node, end)| {
                let mut bytes = std::fs::read(&node.log_path).unwrap_or_else(|error| {
                    panic!("read node{} checkpoint evidence log: {error}", node.id)
                });
                let end = usize::try_from(*end)
                    .unwrap_or_else(|_| panic!("node{} log offset exceeds usize", node.id));
                assert!(
                    end <= bytes.len(),
                    "node{} checkpoint evidence log shrank from {end} to {} bytes",
                    node.id,
                    bytes.len()
                );
                bytes.truncate(end);
                bytes
            })
            .collect::<Vec<_>>();
        let after = nodes
            .iter()
            .map(|node| node.metric("laminardb_checkpoints_failed_total"))
            .collect::<Option<Vec<_>>>();

        if let (Some(before), Some(after)) = (before, after) {
            let logs_match_metrics = log_prefixes.iter().zip(&after).all(|(log, total)| {
                String::from_utf8_lossy(log)
                    .matches(CHECKPOINT_FAILURE_METRIC_LOG)
                    .count() as f64
                    == *total
            });
            if before == after && logs_match_metrics {
                return CheckpointFailureSnapshot {
                    totals: after,
                    log_offsets,
                    log_prefixes,
                };
            }
        }

        assert!(
            Instant::now() < deadline,
            "checkpoint failure metrics and exact log markers did not reach a coherent snapshot"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(feature = "kafka")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ProcessGeneration {
    node_id: usize,
    generation: u64,
}

#[cfg(feature = "kafka")]
#[derive(Default)]
struct CheckpointBarrierTimingGeneration {
    process: Option<LocalProcessAuthorityIdentity>,
    authority_bound: bool,
    cursor: u64,
    last_attempt: Option<CheckpointAttempt>,
    last_assignment_version: Option<u64>,
    assignment_digests: BTreeMap<u64, [u8; 32]>,
    records: Vec<CheckpointBarrierTimingRecord>,
    metadata: Option<CheckpointBarrierTimingMetadata>,
    finalized: bool,
}

#[cfg(feature = "kafka")]
impl CheckpointBarrierTimingGeneration {
    fn bind_authority(
        &mut self,
        generation: ProcessGeneration,
        expected: LocalProcessAuthorityIdentity,
    ) -> Result<(), String> {
        if !expected.is_canonical() {
            return Err(format!(
                "node{} process generation {} received non-canonical authority evidence",
                generation.node_id, generation.generation
            ));
        }
        if self.process.is_some_and(|observed| observed != expected) {
            return Err(format!(
                "node{} process generation {} timing identity does not match converged local authority",
                generation.node_id, generation.generation
            ));
        }
        self.process.get_or_insert(expected);
        self.authority_bound = true;
        Ok(())
    }

    fn apply_page(
        &mut self,
        generation: ProcessGeneration,
        envelope: CheckpointBarrierTimingEnvelope,
    ) -> Result<bool, String> {
        let process = envelope.process_identity;
        if !process.is_canonical() {
            return Err(format!(
                "node{} process generation {} returned a non-canonical timing identity",
                generation.node_id, generation.generation
            ));
        }
        if self.process.is_some_and(|expected| expected != process) {
            return Err(format!(
                "node{} process generation {} changed exact process identity",
                generation.node_id, generation.generation
            ));
        }
        if envelope.after_sequence != self.cursor {
            return Err(format!(
                "node{} process generation {} timing page echoed cursor {}, expected {}",
                generation.node_id, generation.generation, envelope.after_sequence, self.cursor
            ));
        }
        let page = envelope.page;
        if page.capacity != laminar_db::checkpoint_timing::CHECKPOINT_BARRIER_TIMING_CAPACITY {
            return Err(format!(
                "node{} process generation {} timing capacity was {}, expected {}",
                generation.node_id,
                generation.generation,
                page.capacity,
                laminar_db::checkpoint_timing::CHECKPOINT_BARRIER_TIMING_CAPACITY
            ));
        }
        if page.records.len()
            > laminar_db::checkpoint_timing::MAX_CHECKPOINT_BARRIER_TIMING_PAGE_RECORDS
        {
            return Err(format!(
                "node{} process generation {} returned {} timing records in one page",
                generation.node_id,
                generation.generation,
                page.records.len()
            ));
        }
        if page.has_more
            && page.records.len()
                != laminar_db::checkpoint_timing::MAX_CHECKPOINT_BARRIER_TIMING_PAGE_RECORDS
        {
            return Err(format!(
                "node{} process generation {} marked a short timing page as incomplete",
                generation.node_id, generation.generation
            ));
        }
        if page.recording_loss_count != 0 || page.metadata_exhausted {
            return Err(format!(
                "node{} process generation {} timing evidence is incomplete: lost={}, metadata_exhausted={}",
                generation.node_id,
                generation.generation,
                page.recording_loss_count,
                page.metadata_exhausted
            ));
        }
        if page.next_sequence == 0 {
            return Err(format!(
                "node{} process generation {} timing next_sequence was zero",
                generation.node_id, generation.generation
            ));
        }
        let accepted = page.next_sequence - 1;
        let capacity = u64::try_from(page.capacity)
            .map_err(|_| "checkpoint timing capacity exceeds u64".to_string())?;
        let expected_overwrites = accepted.saturating_sub(capacity);
        if page.overwritten_record_count != expected_overwrites {
            return Err(format!(
                "node{} process generation {} timing overwrite count was {}, expected {expected_overwrites} for {accepted} accepted records and capacity {capacity}",
                generation.node_id, generation.generation, page.overwritten_record_count
            ));
        }
        let expected_oldest = (accepted != 0).then_some(expected_overwrites + 1);
        if page.oldest_retained_sequence != expected_oldest {
            return Err(format!(
                "node{} process generation {} oldest retained timing sequence was {:?}, expected {expected_oldest:?}",
                generation.node_id, generation.generation, page.oldest_retained_sequence
            ));
        }
        if page
            .oldest_retained_sequence
            .is_some_and(|oldest| self.cursor.saturating_add(1) < oldest)
        {
            return Err(format!(
                "node{} process generation {} lost unread timing records after cursor {} before retained sequence {}",
                generation.node_id,
                generation.generation,
                self.cursor,
                page.oldest_retained_sequence.unwrap_or(1)
            ));
        }
        let metadata = CheckpointBarrierTimingMetadata::from(&page);
        if let Some(previous) = self.metadata {
            if metadata.capacity != previous.capacity
                || metadata.next_sequence < previous.next_sequence
                || metadata.overwritten_record_count < previous.overwritten_record_count
                || metadata.recording_loss_count < previous.recording_loss_count
                || (previous.metadata_exhausted && !metadata.metadata_exhausted)
            {
                return Err(format!(
                    "node{} process generation {} timing metadata regressed from {previous:?} to {metadata:?}",
                    generation.node_id, generation.generation
                ));
            }
        }

        for record in &page.records {
            let expected_sequence = self.cursor.checked_add(1).ok_or_else(|| {
                format!(
                    "node{} process generation {} timing cursor exhausted",
                    generation.node_id, generation.generation
                )
            })?;
            if record.sequence != expected_sequence {
                return Err(format!(
                    "node{} process generation {} timing sequence jumped from {} to {}",
                    generation.node_id, generation.generation, self.cursor, record.sequence
                ));
            }
            if record.process != process {
                return Err(format!(
                    "node{} process generation {} timing record {} belongs to another process",
                    generation.node_id, generation.generation, record.sequence
                ));
            }
            if let Some(previous) = self.last_attempt {
                match record.attempt.relation_to(previous) {
                    CheckpointAttemptRelation::Newer => {}
                    CheckpointAttemptRelation::Exact => {
                        return Err(format!(
                            "node{} process generation {} recorded checkpoint attempt epoch={} id={} more than once",
                            generation.node_id,
                            generation.generation,
                            record.attempt.epoch,
                            record.attempt.checkpoint_id
                        ));
                    }
                    CheckpointAttemptRelation::Older => {
                        return Err(format!(
                            "node{} process generation {} checkpoint attempt regressed from {previous:?} to {:?}",
                            generation.node_id, generation.generation, record.attempt
                        ));
                    }
                    CheckpointAttemptRelation::Conflict => {
                        return Err(format!(
                            "node{} process generation {} checkpoint attempt conflicted with {previous:?}: {:?}",
                            generation.node_id, generation.generation, record.attempt
                        ));
                    }
                }
            }
            if !record.attempt.is_canonical()
                || record.assignment_version == 0
                || record.assignment_digest == [0; 32]
            {
                return Err(format!(
                    "node{} process generation {} timing record {} has non-canonical attempt or assignment metadata",
                    generation.node_id, generation.generation, record.sequence
                ));
            }
            if self
                .last_assignment_version
                .is_some_and(|version| record.assignment_version < version)
            {
                return Err(format!(
                    "node{} process generation {} timing assignment version regressed to {} at sequence {}",
                    generation.node_id,
                    generation.generation,
                    record.assignment_version,
                    record.sequence
                ));
            }
            if self
                .assignment_digests
                .get(&record.assignment_version)
                .is_some_and(|digest| *digest != record.assignment_digest)
            {
                return Err(format!(
                    "node{} process generation {} timing assignment version {} mapped to conflicting digests",
                    generation.node_id, generation.generation, record.assignment_version
                ));
            }
            let combined = record
                .local_barrier_ns
                .checked_add(record.aligned_resume_ns.unwrap_or(0));
            if combined.is_none_or(|duration| duration > record.pipeline_stall_ns)
                || (record.aligned_resume_ns.is_some() && !record.durable_tail_handoff)
            {
                return Err(format!(
                    "node{} process generation {} timing record {} has impossible stage durations or handoff state",
                    generation.node_id, generation.generation, record.sequence
                ));
            }
            self.assignment_digests
                .entry(record.assignment_version)
                .or_insert(record.assignment_digest);
            self.last_assignment_version = Some(record.assignment_version);
            self.last_attempt = Some(record.attempt);
            self.cursor = record.sequence;
        }
        if !page.has_more && self.cursor != page.next_sequence - 1 {
            return Err(format!(
                "node{} process generation {} timing tail cursor {} contradicts next sequence {}",
                generation.node_id, generation.generation, self.cursor, page.next_sequence
            ));
        }
        self.process.get_or_insert(process);
        self.records.extend(page.records);
        self.metadata = Some(metadata);
        Ok(page.has_more)
    }

    fn validate_against_metrics(
        &self,
        generation: ProcessGeneration,
        metrics: CheckpointLatencySnapshot,
    ) -> Result<(), String> {
        let process = self.process.ok_or_else(|| {
            format!(
                "node{} process generation {} has no timing process identity",
                generation.node_id, generation.generation
            )
        })?;
        let metadata = self.metadata.ok_or_else(|| {
            format!(
                "node{} process generation {} has no timing metadata",
                generation.node_id, generation.generation
            )
        })?;
        if !self.authority_bound {
            return Err(format!(
                "node{} process generation {} timing identity was never bound to converged local authority",
                generation.node_id, generation.generation
            ));
        }
        if metadata.capacity != laminar_db::checkpoint_timing::CHECKPOINT_BARRIER_TIMING_CAPACITY
            || metadata.recording_loss_count != 0
            || metadata.metadata_exhausted
        {
            return Err(format!(
                "node{} process generation {} final timing metadata is incomplete: {metadata:?}",
                generation.node_id, generation.generation
            ));
        }
        let record_count = u64::try_from(self.records.len())
            .map_err(|_| "checkpoint timing record count exceeds u64".to_string())?;
        if self.cursor != record_count {
            return Err(format!(
                "node{} process generation {} collected {record_count} records through cursor {}",
                generation.node_id, generation.generation, self.cursor
            ));
        }
        let capacity = u64::try_from(metadata.capacity)
            .map_err(|_| "checkpoint timing capacity exceeds u64".to_string())?;
        let expected_overwrites = record_count.saturating_sub(capacity);
        let expected_oldest = (record_count != 0).then_some(expected_overwrites + 1);
        if metadata.oldest_retained_sequence != expected_oldest {
            return Err(format!(
                "node{} process generation {} final oldest timing sequence was {:?}, expected {expected_oldest:?}",
                generation.node_id,
                generation.generation,
                metadata.oldest_retained_sequence
            ));
        }
        if metadata.overwritten_record_count != expected_overwrites {
            return Err(format!(
                "node{} process generation {} final overwrite count was {}, expected {expected_overwrites}",
                generation.node_id, generation.generation, metadata.overwritten_record_count
            ));
        }
        if metadata.next_sequence != record_count.saturating_add(1) {
            return Err(format!(
                "node{} process generation {} retained {} exact records but next_sequence is {}",
                generation.node_id, generation.generation, record_count, metadata.next_sequence
            ));
        }
        let aligned_count = u64::try_from(
            self.records
                .iter()
                .filter(|record| record.aligned_resume_ns.is_some())
                .count(),
        )
        .map_err(|_| "checkpoint aligned timing count exceeds u64".to_string())?;
        let stall_within = u64::try_from(
            self.records
                .iter()
                .filter(|record| record.pipeline_stall_ns <= CHECKPOINT_PIPELINE_STALL_SLO_NS)
                .count(),
        )
        .map_err(|_| "checkpoint stall bucket count exceeds u64".to_string())?;
        let local_within = u64::try_from(
            self.records
                .iter()
                .filter(|record| record.local_barrier_ns <= CHECKPOINT_PIPELINE_STALL_SLO_NS)
                .count(),
        )
        .map_err(|_| "checkpoint local bucket count exceeds u64".to_string())?;
        let aligned_within = u64::try_from(
            self.records
                .iter()
                .filter_map(|record| record.aligned_resume_ns)
                .filter(|duration| *duration <= CHECKPOINT_PIPELINE_STALL_SLO_NS)
                .count(),
        )
        .map_err(|_| "checkpoint aligned bucket count exceeds u64".to_string())?;
        for (name, prometheus, exact) in [
            (
                "pipeline-stall count",
                metrics.pipeline_stall_observations,
                record_count,
            ),
            (
                "local-barrier count",
                metrics.barrier_local_observations,
                record_count,
            ),
            (
                "aligned-resume count",
                metrics.aligned_resume_observations,
                aligned_count,
            ),
            (
                "pipeline-stall SLO bucket",
                metrics.pipeline_stall_within_slo,
                stall_within,
            ),
            (
                "local-barrier SLO bucket",
                metrics.barrier_local_within_slo,
                local_within,
            ),
            (
                "aligned-resume SLO bucket",
                metrics.aligned_resume_within_slo,
                aligned_within,
            ),
        ] {
            let prometheus = exact_prometheus_count(prometheus, name)?;
            if prometheus != exact {
                let delta = i128::from(prometheus) - i128::from(exact);
                return Err(format!(
                    "node{} process generation {} ({:?}) {name} mismatch: Prometheus={prometheus}, exact={exact}, delta={delta}; {}",
                    generation.node_id,
                    generation.generation,
                    process,
                    self.violation_summary(8)
                ));
            }
        }
        Ok(())
    }

    fn violation_summary(&self, limit: usize) -> String {
        let mut violations = Vec::new();
        let mut total = 0usize;
        for record in &self.records {
            for (stage, duration) in [
                ("pipeline_stall", Some(record.pipeline_stall_ns)),
                ("local_barrier", Some(record.local_barrier_ns)),
                ("aligned_resume", record.aligned_resume_ns),
            ] {
                if let Some(duration) =
                    duration.filter(|duration| *duration > CHECKPOINT_PIPELINE_STALL_SLO_NS)
                {
                    total += 1;
                    if violations.len() == limit {
                        continue;
                    }
                    violations.push(format!(
                        "stage={stage} sequence={} attempt={}/{} role={:?} assignment_version={} handoff={} process={:?} duration_ns={duration}",
                        record.sequence,
                        record.attempt.epoch,
                        record.attempt.checkpoint_id,
                        record.role,
                        record.assignment_version,
                        record.durable_tail_handoff,
                        record.process,
                    ));
                }
            }
        }
        if violations.is_empty() {
            "no exact observations exceeded the SLO".into()
        } else {
            let omitted = total.saturating_sub(violations.len());
            if omitted == 0 {
                violations.join("; ")
            } else {
                format!(
                    "{}; ... {omitted} additional violations",
                    violations.join("; ")
                )
            }
        }
    }
}

#[cfg(feature = "kafka")]
fn exact_prometheus_count(value: f64, name: &str) -> Result<u64, String> {
    if !value.is_finite() || value < 0.0 || value.fract() != 0.0 || value > 9_007_199_254_740_992.0
    {
        return Err(format!(
            "Prometheus {name} must be an exact non-negative integer, got {value}"
        ));
    }
    Ok(value as u64)
}

#[cfg(feature = "kafka")]
#[derive(Default)]
struct CheckpointBarrierTimingEvidence {
    generations: BTreeMap<ProcessGeneration, CheckpointBarrierTimingGeneration>,
}

#[cfg(feature = "kafka")]
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct CheckpointLatencySnapshot {
    gate_wait_seconds: f64,
    gate_wait_observations: f64,
    checkpoint_seconds: f64,
    checkpoint_observations: f64,
    pipeline_stall_observations: f64,
    pipeline_stall_within_slo: f64,
    barrier_local_seconds: f64,
    barrier_local_observations: f64,
    barrier_local_within_slo: f64,
    aligned_resume_seconds: f64,
    aligned_resume_observations: f64,
    aligned_resume_within_slo: f64,
}

#[cfg(feature = "kafka")]
impl CheckpointLatencySnapshot {
    fn validate(self) -> Result<Self, String> {
        for (name, value) in [
            ("restorable-gate wait sum", self.gate_wait_seconds),
            ("restorable-gate wait count", self.gate_wait_observations),
            ("checkpoint duration sum", self.checkpoint_seconds),
            ("checkpoint duration count", self.checkpoint_observations),
            (
                "checkpoint pipeline-stall count",
                self.pipeline_stall_observations,
            ),
            (
                "checkpoint pipeline-stall SLO bucket",
                self.pipeline_stall_within_slo,
            ),
            ("checkpoint local-barrier sum", self.barrier_local_seconds),
            (
                "checkpoint local-barrier count",
                self.barrier_local_observations,
            ),
            (
                "checkpoint local-barrier SLO bucket",
                self.barrier_local_within_slo,
            ),
            ("checkpoint aligned-resume sum", self.aligned_resume_seconds),
            (
                "checkpoint aligned-resume count",
                self.aligned_resume_observations,
            ),
            (
                "checkpoint aligned-resume SLO bucket",
                self.aligned_resume_within_slo,
            ),
        ] {
            if !value.is_finite() || value < 0.0 {
                return Err(format!(
                    "{name} must be finite and non-negative, got {value}"
                ));
            }
        }
        if self.pipeline_stall_within_slo > self.pipeline_stall_observations {
            return Err(format!(
                "checkpoint pipeline-stall SLO bucket {} exceeds histogram count {}",
                self.pipeline_stall_within_slo, self.pipeline_stall_observations
            ));
        }
        if self.barrier_local_within_slo > self.barrier_local_observations {
            return Err(format!(
                "checkpoint local-barrier SLO bucket {} exceeds histogram count {}",
                self.barrier_local_within_slo, self.barrier_local_observations
            ));
        }
        if self.aligned_resume_within_slo > self.aligned_resume_observations {
            return Err(format!(
                "checkpoint aligned-resume SLO bucket {} exceeds histogram count {}",
                self.aligned_resume_within_slo, self.aligned_resume_observations
            ));
        }
        Ok(self)
    }

    fn merge(&mut self, other: Self) {
        self.gate_wait_seconds += other.gate_wait_seconds;
        self.gate_wait_observations += other.gate_wait_observations;
        self.checkpoint_seconds += other.checkpoint_seconds;
        self.checkpoint_observations += other.checkpoint_observations;
        self.pipeline_stall_observations += other.pipeline_stall_observations;
        self.pipeline_stall_within_slo += other.pipeline_stall_within_slo;
        self.barrier_local_seconds += other.barrier_local_seconds;
        self.barrier_local_observations += other.barrier_local_observations;
        self.barrier_local_within_slo += other.barrier_local_within_slo;
        self.aligned_resume_seconds += other.aligned_resume_seconds;
        self.aligned_resume_observations += other.aligned_resume_observations;
        self.aligned_resume_within_slo += other.aligned_resume_within_slo;
    }

    fn pipeline_stall_within_slo_percent(self) -> Option<f64> {
        (self.pipeline_stall_observations > 0.0)
            .then(|| self.pipeline_stall_within_slo / self.pipeline_stall_observations * 100.0)
    }

    fn validate_pipeline_stall_slo(self, label: &str) -> Result<(), String> {
        let within_slo_percent = self
            .pipeline_stall_within_slo_percent()
            .ok_or_else(|| format!("{label} captured no checkpoint pipeline-stall observations"))?;
        if within_slo_percent < 99.0 {
            return Err(format!(
                "{label} checkpoint pipeline-stall SLO requires 99.00% of observations at or below {:.0}ms; only {within_slo_percent:.2}% of {} observations complied",
                CHECKPOINT_PIPELINE_STALL_SLO_SECONDS * 1_000.0,
                self.pipeline_stall_observations as u64,
            ));
        }
        Ok(())
    }

    fn pipeline_stall_profile(self) -> String {
        let Some(within_slo_percent) = self.pipeline_stall_within_slo_percent() else {
            return "no observations".into();
        };
        format!(
            "<= {:.0}ms={within_slo_percent:.2}% of {} obs",
            CHECKPOINT_PIPELINE_STALL_SLO_SECONDS * 1_000.0,
            self.pipeline_stall_observations as u64,
        )
    }

    fn phase_profile(sum_seconds: f64, observations: f64, within_slo: f64) -> String {
        if observations == 0.0 {
            return "no observations".into();
        }
        format!(
            "avg={:.0}ms, <= {:.0}ms={:.2}% of {} obs",
            sum_seconds / observations * 1_000.0,
            CHECKPOINT_PIPELINE_STALL_SLO_SECONDS * 1_000.0,
            within_slo / observations * 100.0,
            observations as u64,
        )
    }
}

#[cfg(feature = "kafka")]
#[derive(Default)]
struct CheckpointLatencyEvidence {
    generations: BTreeMap<ProcessGeneration, CheckpointLatencySnapshot>,
}

#[cfg(feature = "kafka")]
impl CheckpointLatencyEvidence {
    fn record_generation(
        &mut self,
        generation: ProcessGeneration,
        snapshot: CheckpointLatencySnapshot,
    ) -> Result<(), String> {
        let snapshot = snapshot.validate()?;
        match self.generations.entry(generation) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(snapshot);
                Ok(())
            }
            std::collections::btree_map::Entry::Occupied(_) => Err(format!(
                "checkpoint latency for node{} process generation {} was captured more than once",
                generation.node_id, generation.generation
            )),
        }
    }

    fn aggregate(&self) -> Result<CheckpointLatencySnapshot, String> {
        let mut aggregate = CheckpointLatencySnapshot::default();
        for snapshot in self.generations.values() {
            aggregate.merge(*snapshot);
        }
        aggregate.validate()
    }

    fn aggregate_by_node(&self) -> Result<BTreeMap<usize, CheckpointLatencySnapshot>, String> {
        let mut nodes = BTreeMap::<usize, CheckpointLatencySnapshot>::new();
        for (generation, snapshot) in &self.generations {
            if snapshot.pipeline_stall_observations <= 0.0 {
                return Err(format!(
                    "node{} process generation {} captured no checkpoint pipeline-stall observations",
                    generation.node_id, generation.generation
                ));
            }
            nodes
                .entry(generation.node_id)
                .or_default()
                .merge(*snapshot);
        }
        for snapshot in nodes.values_mut() {
            snapshot.validate()?;
        }
        Ok(nodes)
    }

    fn validate_slos(&self) -> Result<CheckpointLatencySnapshot, String> {
        let aggregate = self.aggregate()?;
        if aggregate.checkpoint_observations <= 0.0 {
            return Err("aggregate captured no checkpoint latency observations".into());
        }
        for (node_id, snapshot) in self.aggregate_by_node()? {
            if snapshot.pipeline_stall_observations
                < MIN_CHECKPOINT_PIPELINE_STALL_OBSERVATIONS as f64
            {
                return Err(format!(
                    "node{node_id} captured only {} checkpoint pipeline-stall observations across process generations; at least {MIN_CHECKPOINT_PIPELINE_STALL_OBSERVATIONS} are required for the observed 99% SLO",
                    snapshot.pipeline_stall_observations as u64,
                ));
            }
            snapshot.validate_pipeline_stall_slo(&format!(
                "node{node_id} across process generations"
            ))?;
        }
        aggregate.validate_pipeline_stall_slo("aggregate")?;
        Ok(aggregate)
    }

    fn report(&self) {
        let aggregate = self.aggregate().unwrap_or_else(|error| panic!("{error}"));
        for (generation, snapshot) in &self.generations {
            eprintln!(
                "soak: PROFILE node{} process generation {}: total stall {}; local barrier {}; aligned resume {}",
                generation.node_id,
                generation.generation,
                snapshot.pipeline_stall_profile(),
                CheckpointLatencySnapshot::phase_profile(
                    snapshot.barrier_local_seconds,
                    snapshot.barrier_local_observations,
                    snapshot.barrier_local_within_slo,
                ),
                CheckpointLatencySnapshot::phase_profile(
                    snapshot.aligned_resume_seconds,
                    snapshot.aligned_resume_observations,
                    snapshot.aligned_resume_within_slo,
                ),
            );
        }
        let within_slo_percent = aggregate.pipeline_stall_within_slo_percent().unwrap_or(0.0);
        let checkpoint_average_ms =
            aggregate.checkpoint_seconds / aggregate.checkpoint_observations * 1_000.0;
        if aggregate.gate_wait_observations > 0.0 {
            eprintln!(
                "soak: PROFILE pipeline-stall <= {:.0}ms for {within_slo_percent:.2}% of {} obs; gate-wait avg={:.0}ms over {} obs; checkpoint_duration avg={checkpoint_average_ms:.0}ms over {} obs (finalized pre-restart generations plus observed cuts of live generations)",
                CHECKPOINT_PIPELINE_STALL_SLO_SECONDS * 1_000.0,
                aggregate.pipeline_stall_observations as u64,
                aggregate.gate_wait_seconds / aggregate.gate_wait_observations * 1_000.0,
                aggregate.gate_wait_observations as u64,
                aggregate.checkpoint_observations as u64,
            );
        } else {
            eprintln!(
                "soak: PROFILE pipeline-stall <= {:.0}ms for {within_slo_percent:.2}% of {} obs; checkpoint_duration avg={checkpoint_average_ms:.0}ms over {} obs (finalized pre-restart generations plus observed cuts of live generations); no restorable-gate waits were observed",
                CHECKPOINT_PIPELINE_STALL_SLO_SECONDS * 1_000.0,
                aggregate.pipeline_stall_observations as u64,
                aggregate.checkpoint_observations as u64,
            );
        }
        self.validate_slos()
            .unwrap_or_else(|error| panic!("{error}"));
    }
}

#[cfg(feature = "kafka")]
impl CheckpointBarrierTimingEvidence {
    fn generation(node: &Node) -> ProcessGeneration {
        ProcessGeneration {
            node_id: node.id,
            generation: node.process_generation,
        }
    }

    fn capture_node(
        &mut self,
        node: &Node,
        expected_process: Option<LocalProcessAuthorityIdentity>,
        deadline: Instant,
    ) -> Result<(), String> {
        let generation = Self::generation(node);
        loop {
            let state = self.generations.entry(generation).or_default();
            if state.finalized {
                return Err(format!(
                    "node{} process generation {} timing evidence was already finalized",
                    generation.node_id, generation.generation
                ));
            }
            if let Some(expected) = expected_process {
                state.bind_authority(generation, expected)?;
            }
            let request_process = state.process;
            let after_sequence = state.cursor;
            match node.checkpoint_barrier_timing_observation(
                request_process,
                after_sequence,
                deadline,
            ) {
                CheckpointBarrierTimingObservation::Pending(error) => {
                    if remaining_at(deadline, Instant::now()).is_none() {
                        return Err(format!(
                            "node{} process generation {} timing evidence remained unavailable: {error}",
                            generation.node_id, generation.generation
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                CheckpointBarrierTimingObservation::Contradiction(error) => return Err(error),
                CheckpointBarrierTimingObservation::Available(envelope) => {
                    let has_more = self
                        .generations
                        .get_mut(&generation)
                        .expect("timing generation was inserted before its HTTP read")
                        .apply_page(generation, envelope)?;
                    if !has_more {
                        return Ok(());
                    }
                }
            }
        }
    }

    fn capture_nodes_unbound(&mut self, nodes: &[Node], deadline: Instant, label: &str) {
        for node in nodes {
            self.capture_node(node, None, deadline)
                .unwrap_or_else(|error| {
                    panic!(
                        "{label}: failed to capture node{} exact checkpoint timings: {error}",
                        node.id
                    )
                });
        }
    }

    fn capture_nodes_bound(
        &mut self,
        nodes: &[Node],
        authority_by_node: &BTreeMap<usize, LocalProcessAuthorityEvidence>,
        deadline: Instant,
        label: &str,
    ) {
        for node in nodes {
            let evidence = authority_by_node.get(&node.id).unwrap_or_else(|| {
                panic!(
                    "{label}: converged local authority omitted live node{}",
                    node.id,
                )
            });
            let expected_process = local_process_identity(evidence);
            self.capture_node(node, Some(expected_process), deadline)
                .unwrap_or_else(|error| {
                    panic!(
                        "{label}: failed to capture node{} exact checkpoint timings: {error}",
                        node.id
                    )
                });
        }
    }

    fn finalize_node(
        &mut self,
        node: &Node,
        expected_process: LocalProcessAuthorityIdentity,
        latency: &mut CheckpointLatencyEvidence,
        deadline: Instant,
    ) -> Result<(), String> {
        let generation = Self::generation(node);
        if self
            .generations
            .get(&generation)
            .is_some_and(|state| state.finalized)
        {
            return Err(format!(
                "node{} process generation {} timing evidence was finalized twice",
                generation.node_id, generation.generation
            ));
        }
        loop {
            self.capture_node(node, Some(expected_process), deadline)?;
            let (cursor_before, metadata_before) = self
                .generations
                .get(&generation)
                .and_then(|state| state.metadata.map(|metadata| (state.cursor, metadata)))
                .ok_or_else(|| "timing capture produced no metadata".to_string())?;
            let Some(metrics_before) = node.checkpoint_latency_metrics() else {
                if remaining_at(deadline, Instant::now()).is_none() {
                    return Err(format!(
                        "node{} process generation {} did not expose checkpoint latency metrics",
                        generation.node_id, generation.generation
                    ));
                }
                std::thread::sleep(Duration::from_millis(10));
                continue;
            };
            self.capture_node(node, Some(expected_process), deadline)?;
            let (cursor_after, metadata_after) = self
                .generations
                .get(&generation)
                .and_then(|state| state.metadata.map(|metadata| (state.cursor, metadata)))
                .ok_or_else(|| "timing confirmation produced no metadata".to_string())?;
            let Some(metrics_after) = node.checkpoint_latency_metrics() else {
                if remaining_at(deadline, Instant::now()).is_none() {
                    return Err(format!(
                        "node{} process generation {} lost checkpoint latency metrics during finalization",
                        generation.node_id, generation.generation
                    ));
                }
                std::thread::sleep(Duration::from_millis(10));
                continue;
            };
            self.capture_node(node, Some(expected_process), deadline)?;
            let (cursor_confirmed, metadata_confirmed) = self
                .generations
                .get(&generation)
                .and_then(|state| state.metadata.map(|metadata| (state.cursor, metadata)))
                .ok_or_else(|| "final timing confirmation produced no metadata".to_string())?;
            let incoherence = if cursor_before == cursor_after
                && cursor_after == cursor_confirmed
                && metadata_before == metadata_after
                && metadata_after == metadata_confirmed
                && metrics_before == metrics_after
            {
                match self
                    .generations
                    .get(&generation)
                    .expect("stable timing generation must exist")
                    .validate_against_metrics(generation, metrics_after)
                {
                    Ok(()) => {
                        latency.record_generation(generation, metrics_after)?;
                        self.generations
                            .get_mut(&generation)
                            .expect("validated timing generation must exist")
                            .finalized = true;
                        return Ok(());
                    }
                    Err(error) => error,
                }
            } else {
                format!(
                    "unstable cut: cursors={cursor_before}/{cursor_after}/{cursor_confirmed}, metadata_equal={}, metrics_equal={}",
                    metadata_before == metadata_after && metadata_after == metadata_confirmed,
                    metrics_before == metrics_after
                )
            };
            if remaining_at(deadline, Instant::now()).is_none() {
                return Err(format!(
                    "node{} process generation {} never reached a coherent observed timing/metrics cut: {}",
                    generation.node_id,
                    generation.generation,
                    incoherence
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn validate_observed_cuts(
        &self,
        latency: &CheckpointLatencyEvidence,
        nodes: &[Node],
    ) -> Result<(), String> {
        for (generation, timing) in &self.generations {
            if !timing.finalized {
                return Err(format!(
                    "node{} process generation {} exact timing evidence was never finalized",
                    generation.node_id, generation.generation
                ));
            }
        }
        let timing_generations = self.generations.keys().copied().collect::<BTreeSet<_>>();
        let metric_generations = latency.generations.keys().copied().collect::<BTreeSet<_>>();
        if timing_generations != metric_generations {
            return Err(format!(
                "exact timing generations {timing_generations:?} differ from Prometheus generations {metric_generations:?}"
            ));
        }
        let mut expected_generations = BTreeSet::new();
        for node in nodes {
            if node.process_generation == 0 {
                return Err(format!("node{} never started a soak process", node.id));
            }
            expected_generations.extend((1..=node.process_generation).map(|generation| {
                ProcessGeneration {
                    node_id: node.id,
                    generation,
                }
            }));
        }
        if timing_generations != expected_generations {
            return Err(format!(
                "exact timing generations {timing_generations:?} differ from every spawned process generation {expected_generations:?}"
            ));
        }
        Ok(())
    }

    fn report(&self) {
        fn max_record(
            records: &[CheckpointBarrierTimingRecord],
            duration: impl Fn(&CheckpointBarrierTimingRecord) -> Option<u64>,
        ) -> Option<(&CheckpointBarrierTimingRecord, u64)> {
            records
                .iter()
                .filter_map(|record| Some((record, duration(record)?)))
                .max_by_key(|(_, duration)| *duration)
        }

        for (generation, timing) in &self.generations {
            let process = timing
                .process
                .expect("reported exact timing generation has a process identity");
            let leader_count = timing
                .records
                .iter()
                .filter(|record| record.role == CheckpointBarrierTimingRole::Leader)
                .count();
            let follower_count = timing.records.len().saturating_sub(leader_count);
            let no_handoff_count = timing
                .records
                .iter()
                .filter(|record| !record.durable_tail_handoff)
                .count();
            let deadline_exhausted_count = timing
                .records
                .iter()
                .filter(|record| record.deadline_exhausted)
                .count();
            eprintln!(
                "soak: EXACT node{} process generation {} {:?}: {} records (leader={leader_count}, follower={follower_count}, no_handoff={no_handoff_count}, deadline_exhausted={deadline_exhausted_count})",
                generation.node_id,
                generation.generation,
                process,
                timing.records.len()
            );
            for (stage, maximum) in [
                (
                    "pipeline_stall",
                    max_record(&timing.records, |record| Some(record.pipeline_stall_ns)),
                ),
                (
                    "local_barrier",
                    max_record(&timing.records, |record| Some(record.local_barrier_ns)),
                ),
                (
                    "aligned_resume",
                    max_record(&timing.records, |record| record.aligned_resume_ns),
                ),
            ] {
                if let Some((record, duration_ns)) = maximum {
                    eprintln!(
                        "soak: EXACT MAX node{} generation {} stage={stage} sequence={} attempt={}/{} role={:?} assignment_version={} assignment_digest={:?} handoff={} process={:?} duration_ns={duration_ns} deadline_exhausted={}",
                        generation.node_id,
                        generation.generation,
                        record.sequence,
                        record.attempt.epoch,
                        record.attempt.checkpoint_id,
                        record.role,
                        record.assignment_version,
                        record.assignment_digest,
                        record.durable_tail_handoff,
                        record.process,
                        record.deadline_exhausted,
                    );
                }
            }
            for record in timing
                .records
                .iter()
                .filter(|record| record.deadline_exhausted)
            {
                eprintln!(
                    "soak: EXACT DEADLINE node{} generation {} sequence={} attempt={}/{} role={:?} assignment_version={} assignment_digest={:?} handoff={} process={:?}",
                    generation.node_id,
                    generation.generation,
                    record.sequence,
                    record.attempt.epoch,
                    record.attempt.checkpoint_id,
                    record.role,
                    record.assignment_version,
                    record.assignment_digest,
                    record.durable_tail_handoff,
                    record.process,
                );
            }
            for record in &timing.records {
                for (stage, duration) in [
                    ("pipeline_stall", Some(record.pipeline_stall_ns)),
                    ("local_barrier", Some(record.local_barrier_ns)),
                    ("aligned_resume", record.aligned_resume_ns),
                ] {
                    if let Some(duration_ns) =
                        duration.filter(|duration| *duration > CHECKPOINT_PIPELINE_STALL_SLO_NS)
                    {
                        eprintln!(
                            "soak: EXACT SLO VIOLATION node{} generation {} stage={stage} sequence={} attempt={}/{} role={:?} assignment_version={} assignment_digest={:?} handoff={} process={:?} duration_ns={duration_ns} deadline_exhausted={}",
                            generation.node_id,
                            generation.generation,
                            record.sequence,
                            record.attempt.epoch,
                            record.attempt.checkpoint_id,
                            record.role,
                            record.assignment_version,
                            record.assignment_digest,
                            record.durable_tail_handoff,
                            record.process,
                            record.deadline_exhausted,
                        );
                    }
                }
            }
        }
    }
}

#[cfg(feature = "kafka")]
impl ProducerGuard {
    fn spawn(brokers: String, topic: String, partitions: i32, rps: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let enqueued = Arc::new(AtomicU64::new(0));
        let producer_stop = Arc::clone(&stop);
        let producer_enqueued = Arc::clone(&enqueued);
        let handle = std::thread::spawn(move || {
            produce_seq(
                &brokers,
                &topic,
                partitions,
                rps,
                &producer_stop,
                &producer_enqueued,
            )
        });
        Self {
            stop,
            enqueued,
            handle: Some(handle),
        }
    }

    fn enqueued(&self) -> u64 {
        self.enqueued.load(Ordering::Acquire)
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
        self.handle
            .take()
            .expect("Kafka producer was already stopped")
            .join()
            .expect("Kafka producer thread failed")
    }
}

#[cfg(feature = "kafka")]
impl Drop for ProducerGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(feature = "kafka")]
struct KafkaCommitOracle {
    consumer: rdkafka::consumer::BaseConsumer,
    topic: String,
    partitions: i32,
}

#[cfg(feature = "kafka")]
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

#[cfg(feature = "kafka")]
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

#[cfg(feature = "kafka")]
fn monotonic_offset_delta(label: &str, start: &[i64], end: &[i64]) -> u64 {
    assert_eq!(
        start.len(),
        end.len(),
        "{label} partition count changed during active-load sampling"
    );
    start
        .iter()
        .zip(end)
        .enumerate()
        .try_fold(0_u64, |total, (partition, (start, end))| {
            assert!(
                *start >= 0 && end >= start,
                "{label} partition {partition} regressed or was uninitialized: {start}->{end}"
            );
            let delta = u64::try_from(end - start).expect("non-negative Kafka offset delta");
            total
                .checked_add(delta)
                .ok_or("Kafka offset delta sum overflow")
        })
        .unwrap_or_else(|error| panic!("{label}: {error}"))
}

#[cfg(feature = "kafka")]
fn initialized_offset_sum(label: &str, offsets: &[i64]) -> u64 {
    offsets
        .iter()
        .enumerate()
        .try_fold(0_u64, |total, (partition, offset)| {
            assert!(
                *offset >= 0,
                "{label} partition {partition} is uninitialized: {offset}"
            );
            let offset = u64::try_from(*offset).expect("non-negative Kafka offset");
            total.checked_add(offset).ok_or("Kafka offset sum overflow")
        })
        .unwrap_or_else(|error| panic!("{label}: {error}"))
}

#[cfg(feature = "kafka")]
fn all_partition_offsets_advanced(start: &[i64], end: &[i64]) -> Result<bool, String> {
    if start.is_empty() {
        return Err("committed-offset frontier has no partitions".to_string());
    }
    if start.len() != end.len() {
        return Err(format!(
            "partition count changed from {} to {}",
            start.len(),
            end.len()
        ));
    }
    let mut all_advanced = true;
    for (partition, (start, end)) in start.iter().zip(end).enumerate() {
        if *start < 0 || end < start {
            return Err(format!(
                "partition {partition} regressed or was uninitialized: {start}->{end}"
            ));
        }
        all_advanced &= end > start;
    }
    Ok(all_advanced)
}

#[cfg(feature = "kafka")]
fn timed_snapshot<T>(snapshot: impl FnOnce() -> T) -> (Instant, Instant, T) {
    let started = Instant::now();
    let value = snapshot();
    (started, Instant::now(), value)
}

#[cfg(feature = "kafka")]
fn wait_for_committed_offset_advance(
    nodes: &mut [Node],
    producer: &mut ProducerGuard,
    input: &KafkaCommitOracle,
    baseline: &[i64],
    window: Duration,
    label: &str,
) -> (Instant, Vec<i64>) {
    let mut observed = None;
    wait_for(
        &format!("{label}: every Kafka source partition to advance its committed offset"),
        window,
        || {
            assert_running_nodes(nodes);
            producer.assert_running();
            let Some(current) = input.committed_offsets() else {
                return false;
            };
            match all_partition_offsets_advanced(baseline, &current) {
                Ok(true) => {
                    observed = Some((Instant::now(), current));
                    true
                }
                Ok(false) => false,
                Err(error) => panic!("{label}: invalid Kafka committed-offset frontier: {error}"),
            }
        },
    );
    observed.expect("committed-offset wait completed without an offset frontier")
}

#[cfg(feature = "kafka")]
fn assert_active_load_throughput(
    nodes: &mut [Node],
    producer: &mut ProducerGuard,
    input: &KafkaCommitOracle,
    output: &KafkaOutputOracle,
    target_rps: u64,
    recovery_ceiling: Duration,
) {
    assert_running_nodes(nodes);
    producer.assert_running();
    let committed_seed = input
        .committed_offsets()
        .expect("active-load initial input committed-offset snapshot");
    initialized_offset_sum(
        "active-load initial input committed offsets",
        &committed_seed,
    );
    // Kafka's committed frontier advances only after the asynchronous checkpoint tail completes.
    // Anchor both ends on observable terminal cuts so a wall-clock endpoint cannot omit an entire
    // in-flight checkpoint from an otherwise healthy throughput window.
    let (durable_start_at, committed_start_offsets) = wait_for_committed_offset_advance(
        nodes,
        producer,
        input,
        &committed_seed,
        recovery_ceiling,
        "active-load durable baseline",
    );
    let committed_start = initialized_offset_sum(
        "active-load input committed offsets",
        &committed_start_offsets,
    );
    let (offered_start_at, _, offered_start) = timed_snapshot(|| producer.enqueued());
    let (emitted_start_at, _, output_start) = timed_snapshot(|| {
        output
            .high_watermarks()
            .expect("active-load output high-watermark snapshot")
    });
    let sample_started = Instant::now();
    while sample_started.elapsed() < ACTIVE_LOAD_SAMPLE_WINDOW {
        assert_running_nodes(nodes);
        producer.assert_running();
        std::thread::sleep(Duration::from_millis(100));
    }
    let (_, offered_end_at, offered_end) = timed_snapshot(|| producer.enqueued());
    let (_, emitted_end_at, output_end) = timed_snapshot(|| {
        output
            .high_watermarks()
            .expect("active-load final output high-watermark snapshot")
    });
    let committed_at_deadline = input
        .committed_offsets()
        .expect("active-load deadline input committed-offset snapshot");
    all_partition_offsets_advanced(&committed_start_offsets, &committed_at_deadline)
        .unwrap_or_else(|error| panic!("active-load durable deadline: {error}"));
    let (durable_end_at, committed_end_offsets) = wait_for_committed_offset_advance(
        nodes,
        producer,
        input,
        &committed_at_deadline,
        recovery_ceiling,
        "active-load durable endpoint",
    );
    let committed_end = initialized_offset_sum(
        "active-load final input committed offsets",
        &committed_end_offsets,
    );
    let offered = offered_end
        .checked_sub(offered_start)
        .expect("producer enqueue count regressed");
    let durable = committed_end
        .checked_sub(committed_start)
        .expect("committed input offset sum regressed");
    let emitted = monotonic_offset_delta("sink output", &output_start, &output_end);
    let offered_elapsed = offered_end_at
        .duration_since(offered_start_at)
        .as_secs_f64();
    let durable_elapsed = durable_end_at
        .duration_since(durable_start_at)
        .as_secs_f64();
    let emitted_elapsed = emitted_end_at
        .duration_since(emitted_start_at)
        .as_secs_f64();
    let offered_rps = offered as f64 / offered_elapsed;
    let durable_rps = durable as f64 / durable_elapsed;
    let emitted_rps = emitted as f64 / emitted_elapsed;
    let minimum_rps = target_rps as f64 * ACTIVE_LOAD_MINIMUM_RATIO;
    eprintln!(
        "soak: ACTIVE LOAD producer_accepted={offered_rps:.1} rps/{offered} records/{offered_elapsed:.1}s, durable_input={durable_rps:.1} rps/{durable} records/{durable_elapsed:.1}s, sink_output={emitted_rps:.1} rps/{emitted} records/{emitted_elapsed:.1}s"
    );
    assert!(
        offered_rps >= minimum_rps,
        "active-load producer accepted only {offered_rps:.1} rps against target {target_rps} rps"
    );
    assert!(
        durable_rps >= minimum_rps,
        "LaminarDB durably advanced source offsets at only {durable_rps:.1} rps against target {target_rps} rps"
    );
    assert!(
        emitted_rps >= minimum_rps,
        "LaminarDB sink output advanced at only {emitted_rps:.1} rps against target {target_rps} rps"
    );
}

#[cfg(feature = "kafka")]
fn record_consumed_offset(consumed: &mut [i64], partition: i32, offset: i64) {
    let partition = usize::try_from(partition).expect("Kafka returned a negative partition");
    let next = offset.saturating_add(1);
    let consumed = consumed
        .get_mut(partition)
        .expect("Kafka returned an out-of-range partition");
    *consumed = (*consumed).max(next);
}

#[cfg(feature = "kafka")]
struct KafkaOutputOracle {
    consumer: rdkafka::consumer::BaseConsumer,
    topic: String,
    partitions: i32,
    consumed_offsets: Vec<i64>,
    seen: BTreeSet<u64>,
    duplicates: u64,
}

#[cfg(feature = "kafka")]
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

#[cfg(feature = "kafka")]
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

#[cfg(feature = "kafka")]
fn assert_no_unsolicited_cold_start_recovery(nodes: &[Node]) {
    for node in nodes {
        assert_eq!(
            node.metric("laminardb_coordinated_recoveries_total")
                .expect("node did not expose coordinated recovery count"),
            0.0,
            "node{} performed an unsolicited cold-start recovery",
            node.id
        );
        assert_eq!(
            node.metric("laminardb_coordinated_recovery_failures_total")
                .expect("node did not expose coordinated recovery failure count"),
            0.0,
            "node{} failed an unsolicited cold-start recovery",
            node.id
        );
        let log = node.log_since(0);
        assert!(
            !log.contains(RECOVERY_PREPARE_LOG) && !log.contains(RECOVERY_RELEASE_LOG),
            "node{} log contains unsolicited cold-start recovery activity",
            node.id
        );
    }
}

#[cfg(feature = "kafka")]
fn validate_explicit_pipeline_fault_totals(
    baselines: &[f64],
    totals: &[f64],
    victim: usize,
) -> Result<(), String> {
    if baselines.len() != totals.len() {
        return Err(format!(
            "pipeline fault metric cardinality changed from {} to {}",
            baselines.len(),
            totals.len()
        ));
    }
    if victim >= totals.len() {
        return Err(format!(
            "explicit fault victim node{victim} is outside {} metric samples",
            totals.len()
        ));
    }
    for (node_id, (baseline, total)) in baselines.iter().zip(totals).enumerate() {
        let expected = baseline + if node_id == victim { 1.0 } else { 0.0 };
        if *total != expected {
            return Err(format!(
                "node{node_id} pipeline fault total is {total}, expected {expected} after explicit fault on node{victim}"
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "kafka")]
fn validate_recovery_checkpoint_failure_totals(
    baselines: &[f64],
    totals: &[f64],
    leader: usize,
) -> Result<(), String> {
    if baselines.len() != totals.len() {
        return Err(format!(
            "checkpoint failure metric cardinality changed from {} to {}",
            baselines.len(),
            totals.len()
        ));
    }
    if leader >= totals.len() {
        return Err(format!(
            "recovery leader node{leader} is outside {} metric samples",
            totals.len()
        ));
    }
    for (node_id, (baseline, total)) in baselines.iter().zip(totals).enumerate() {
        let delta = total - baseline;
        let valid = if node_id == leader {
            delta == 0.0 || delta == 1.0
        } else {
            delta == 0.0
        };
        if !valid {
            return Err(format!(
                "node{node_id} checkpoint failure total changed by {delta}; only recovery leader node{leader} may record one durable aborted attempt"
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "kafka")]
fn validate_recovery_checkpoint_failure_evidence(
    baselines: &[f64],
    totals: &[f64],
    leader: usize,
    fault_logs: &[String],
    leader_log: &str,
    resumed_checkpoint: DurableCheckpointStatus,
) -> Result<Option<DurableCheckpointStatus>, String> {
    let resumed_attempt = resumed_checkpoint.try_attempt("resumed checkpoint")?;
    validate_recovery_checkpoint_failure_totals(baselines, totals, leader)?;
    if fault_logs.len() != totals.len() {
        return Err(format!(
            "checkpoint failure log cardinality {} does not match {} metric samples",
            fault_logs.len(),
            totals.len()
        ));
    }

    let mut leader_failure = None;
    for (node_id, log) in fault_logs.iter().enumerate() {
        let mut failures = Vec::new();
        for line in log.lines() {
            if let Some(failure) = checkpoint_failure_metric_from_log_line(line)? {
                failures.push(failure);
            }
        }
        let delta = totals[node_id] - baselines[node_id];
        let expected = if delta == 1.0 { 1 } else { 0 };
        if failures.len() != expected {
            return Err(format!(
                "node{node_id} checkpoint failure metric changed by {delta} but its fault log contains {} exact metric records",
                failures.len()
            ));
        }
        if node_id == leader {
            leader_failure = failures.first().copied();
        }
    }
    let Some(failed) = leader_failure else {
        return Ok(None);
    };
    let prepare_count = fault_logs
        .iter()
        .map(|log| log.matches(RECOVERY_PREPARE_LOG).count())
        .sum::<usize>();
    if prepare_count != 1 {
        return Err(format!(
            "checkpoint failure metric was recorded with {prepare_count} recovery Prepare records"
        ));
    }
    let leader_fault_lines = fault_logs[leader].lines().collect::<Vec<_>>();
    let prepare = leader_fault_lines
        .iter()
        .position(|line| line.contains(RECOVERY_PREPARE_LOG))
        .ok_or_else(|| {
            "checkpoint failure was not preceded by the recovery leader's Prepare".to_string()
        })?;
    let mut fault_failure = None;
    for (index, line) in leader_fault_lines.iter().enumerate() {
        if checkpoint_failure_metric_from_log_line(line)? == Some(failed) {
            fault_failure = Some(index);
            break;
        }
    }
    let fault_failure = fault_failure.ok_or_else(|| {
        format!(
            "checkpoint {} epoch {} failure is absent from the recovery leader fault log",
            failed.checkpoint_id, failed.epoch
        )
    })?;
    if prepare >= fault_failure {
        return Err(format!(
            "checkpoint {} epoch {} failure was not caused by the injected recovery Prepare",
            failed.checkpoint_id, failed.epoch
        ));
    }
    if !leader_fault_lines[fault_failure + 1..]
        .iter()
        .any(|line| line.contains(RECOVERY_RELEASE_LOG))
    {
        return Err(format!(
            "checkpoint {} epoch {} failure was not followed by recovery Release",
            failed.checkpoint_id, failed.epoch
        ));
    }

    let lines = leader_log.lines().collect::<Vec<_>>();
    let mut reservations = Vec::new();
    let mut failures = Vec::new();
    let mut completions = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if checkpoint_reservation_from_log_line(line)? == Some(failed) {
            reservations.push(index);
        }
        if checkpoint_failure_metric_from_log_line(line)? == Some(failed) {
            failures.push(index);
        }
        if log_line_reports_checkpoint_completion(line, failed) {
            completions.push(index);
        }
    }
    if reservations.len() != 1 {
        return Err(format!(
            "checkpoint failure metric for checkpoint {} epoch {} has {} matching leader reservations",
            failed.checkpoint_id,
            failed.epoch,
            reservations.len()
        ));
    }
    if failures.len() != 1 {
        return Err(format!(
            "checkpoint failure metric for checkpoint {} epoch {} appears {} times in the leader log",
            failed.checkpoint_id,
            failed.epoch,
            failures.len()
        ));
    }
    let reservation = reservations[0];
    let failure = failures[0];
    if reservation >= failure {
        return Err(format!(
            "checkpoint {} epoch {} failed before its exact reservation",
            failed.checkpoint_id, failed.epoch
        ));
    }
    if !completions.is_empty() {
        return Err(format!(
            "checkpoint {} epoch {} both completed and recorded a failure metric",
            failed.checkpoint_id, failed.epoch
        ));
    }
    let failed_attempt = failed.try_attempt("checkpoint failure metric log")?;
    if failed_attempt.relation_to(resumed_attempt) != CheckpointAttemptRelation::Older {
        return Err(format!(
            "resumed checkpoint {} epoch {} is not strictly newer than interrupted checkpoint {} epoch {}",
            resumed_checkpoint.checkpoint_id,
            resumed_checkpoint.epoch,
            failed.checkpoint_id,
            failed.epoch
        ));
    }
    Ok(Some(failed))
}

#[cfg(feature = "kafka")]
fn assert_explicit_fault_recovery_evidence(nodes: &[Node], evidence: &ExplicitFaultEvidence) {
    assert_eq!(nodes.len(), evidence.log_offsets.len());
    assert_eq!(nodes.len(), evidence.pipeline_fault_baselines.len());
    assert_eq!(nodes.len(), evidence.recovery_baselines.len());
    assert_eq!(nodes.len(), evidence.recovery_failure_baselines.len());
    assert_eq!(nodes.len(), evidence.checkpoint_failure_baselines.len());
    assert_eq!(nodes.len(), evidence.checkpoint_failure_totals.len());
    let logs = nodes
        .iter()
        .zip(&evidence.log_offsets)
        .map(|(node, offset)| node.log_since(*offset))
        .collect::<Vec<_>>();
    let prepare_count: usize = logs
        .iter()
        .map(|log| log.matches(RECOVERY_PREPARE_LOG).count())
        .sum();
    assert_eq!(
        prepare_count, 1,
        "explicit fault created {prepare_count} recovery Prepare generations instead of exactly one"
    );
    let leader_log = std::fs::read_to_string(&nodes[evidence.recovery_leader].log_path)
        .expect("read recovery leader log for checkpoint failure evidence");
    let interrupted = validate_recovery_checkpoint_failure_evidence(
        &evidence.checkpoint_failure_baselines,
        &evidence.checkpoint_failure_totals,
        evidence.recovery_leader,
        &logs,
        &leader_log,
        evidence.resumed_checkpoint,
    )
    .unwrap_or_else(|error| panic!("explicit recovery checkpoint failure invalid: {error}"));
    assert_eq!(interrupted, evidence.interrupted_checkpoint);
    validate_post_release_checkpoint_lifecycle(&logs, evidence.resumed_checkpoint)
        .unwrap_or_else(|error| panic!("explicit recovery checkpoint lifecycle invalid: {error}"));

    let pipeline_fault_totals = nodes
        .iter()
        .map(|node| {
            node.metric("laminardb_pipeline_faults_total")
                .expect("node stopped exposing pipeline fault count")
        })
        .collect::<Vec<_>>();
    validate_explicit_pipeline_fault_totals(
        &evidence.pipeline_fault_baselines,
        &pipeline_fault_totals,
        evidence.victim,
    )
    .unwrap_or_else(|error| panic!("explicit fault amplification detected: {error}"));

    for (index, node) in nodes.iter().enumerate() {
        let recovery_baseline = evidence.recovery_baselines[index];
        let recoveries = node
            .metric("laminardb_coordinated_recoveries_total")
            .expect("node stopped exposing coordinated recovery count");
        assert_eq!(
            recoveries,
            recovery_baseline + 1.0,
            "node{} applied {} recovery generations for one explicit fault",
            node.id,
            recoveries - recovery_baseline
        );
        let failures = node
            .metric("laminardb_coordinated_recovery_failures_total")
            .expect("node stopped exposing coordinated recovery failure count");
        assert_eq!(
            failures, evidence.recovery_failure_baselines[index],
            "node{} recorded a coordinated recovery failure",
            node.id
        );
        let checkpoint_failures = node
            .metric("laminardb_checkpoints_failed_total")
            .expect("node stopped exposing checkpoint failure count");
        assert_eq!(
            checkpoint_failures, evidence.checkpoint_failure_totals[index],
            "node{} checkpoint failure count changed after explicit recovery",
            node.id
        );
    }
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

#[cfg(feature = "kafka")]
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
console_token = "{SOAK_CONSOLE_TOKEN}"
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

fn local_exact_checkpoint_source_sequence(
    checkpoint_dir: &Path,
    checkpoint: DurableCheckpointStatus,
) -> Result<u64, String> {
    let path = checkpoint_dir.join(format!(
        "checkpoints/checkpoint_{:06}/manifest.json",
        checkpoint.checkpoint_id
    ));
    let bytes = std::fs::read(&path)
        .map_err(|error| format!("read checkpoint manifest '{}': {error}", path.display()))?;
    let manifest: laminar_core::checkpoint::CheckpointManifest = serde_json::from_slice(&bytes)
        .map_err(|error| format!("decode checkpoint manifest '{}': {error}", path.display()))?;
    if manifest.checkpoint_id != checkpoint.checkpoint_id || manifest.epoch != checkpoint.epoch {
        return Err(format!(
            "checkpoint status {checkpoint:?} resolved to manifest identity checkpoint={} epoch={}",
            manifest.checkpoint_id, manifest.epoch
        ));
    }
    if manifest.durable_phase
        != laminar_core::checkpoint::checkpoint_manifest::DurableCheckpointPhase::Finalized
    {
        return Err(format!(
            "checkpoint {checkpoint:?} source cursor was read from a non-finalized manifest"
        ));
    }
    let sequence = manifest
        .source_offsets
        .get("gen")
        .and_then(|checkpoint| checkpoint.offsets.get("seq"))
        .ok_or_else(|| format!("checkpoint {checkpoint:?} has no generator sequence offset"))?;
    sequence.parse::<u64>().map_err(|error| {
        format!("checkpoint {checkpoint:?} has invalid generator sequence {sequence:?}: {error}")
    })
}

fn assert_no_local_checkpoint_consistency_fault(node: &Node) {
    let log = std::fs::read_to_string(&node.log_path)
        .unwrap_or_else(|error| panic!("read local exact soak log: {error}"));
    for marker in [
        "[LDB-6024]",
        "[LDB-6026]",
        "auto-restarting faulted pipeline",
    ] {
        assert!(
            !log.contains(marker),
            "local exact soak observed an internal checkpoint consistency fault: {marker}"
        );
    }
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

#[cfg(feature = "kafka")]
#[derive(Debug)]
struct LocalAssignmentConvergence {
    snapshot: AssignmentSnapshot,
    evidence_by_node: BTreeMap<usize, LocalProcessAuthorityEvidence>,
}

#[cfg(feature = "kafka")]
fn local_process_identity(
    evidence: &LocalProcessAuthorityEvidence,
) -> LocalProcessAuthorityIdentity {
    LocalProcessAuthorityIdentity {
        participant: evidence.participant,
        process_term: evidence.process_term,
    }
}

#[cfg(feature = "kafka")]
fn classify_local_assignment_cut(
    before: &AssignmentSnapshot,
    evidence_by_node: BTreeMap<usize, LocalProcessAuthorityEvidence>,
    after: &AssignmentSnapshot,
) -> Result<Option<LocalAssignmentConvergence>, String> {
    if before != after || before.draining {
        return Ok(None);
    }
    let fence = before
        .assignment_fence()
        .map_err(|error| format!("durable assignment is non-canonical: {error}"))?;
    let mut evidence_participants = BTreeMap::new();
    for (node, evidence) in &evidence_by_node {
        let adoption = &evidence.adopted_assignment;
        if adoption.assignment_version < fence.assignment_version {
            return Ok(None);
        }
        if adoption.assignment_version > fence.assignment_version {
            return Err(format!(
                "node{node} local assignment {} is ahead of stable durable head {}",
                adoption.assignment_version, fence.assignment_version
            ));
        }
        if !adoption.matches_fence(&fence) {
            return Err(format!(
                "node{node} local adoption conflicts with durable assignment {}",
                fence.assignment_version
            ));
        }
        if evidence_participants
            .insert(evidence.participant.node_id, evidence.participant)
            .is_some()
        {
            return Err(format!(
                "multiple live processes reported stable node {}",
                evidence.participant.node_id
            ));
        }
    }
    let durable_participants: BTreeMap<_, _> = fence
        .participants
        .iter()
        .map(|participant| (participant.node_id, *participant))
        .collect();
    if evidence_participants != durable_participants {
        return Ok(None);
    }
    Ok(Some(LocalAssignmentConvergence {
        snapshot: before.clone(),
        evidence_by_node,
    }))
}

#[cfg(feature = "kafka")]
fn sleep_until_local_evidence_poll(deadline: Instant) {
    if let Some(remaining) = remaining_at(deadline, Instant::now()) {
        std::thread::sleep(remaining.min(Duration::from_millis(500)));
    }
}

#[cfg(feature = "kafka")]
fn wait_for_local_assignment_convergence(
    nodes: &mut [Node],
    live_nodes: &BTreeSet<usize>,
    deadline: Instant,
    context: &str,
) -> LocalAssignmentConvergence {
    assert!(
        !live_nodes.is_empty(),
        "local evidence requires a live node"
    );
    let probe = *live_nodes.first().expect("live node set is nonempty");
    let mut last_pending = "no sample completed".to_string();
    while remaining_at(deadline, Instant::now()).is_some() {
        for node in live_nodes {
            nodes[*node].assert_running();
        }
        let before = match nodes[probe].durable_assignment_observation(deadline) {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                last_pending = format!("node{probe} durable assignment was unavailable");
                sleep_until_local_evidence_poll(deadline);
                continue;
            }
            Err(error) => panic!("{context}: invalid durable-before sample: {error}"),
        };
        if before.draining {
            last_pending = format!("durable assignment {} was still draining", before.version);
            sleep_until_local_evidence_poll(deadline);
            continue;
        }

        let mut evidence_by_node = BTreeMap::new();
        let mut pending = None;
        for node in live_nodes {
            match nodes[*node].local_authority_observation(deadline) {
                LocalAuthorityObservation::Available(evidence) => {
                    evidence_by_node.insert(*node, evidence);
                }
                LocalAuthorityObservation::Pending(reason) => {
                    pending = Some(reason);
                    break;
                }
                LocalAuthorityObservation::Contradiction(error) => {
                    panic!("{context}: contradictory node{node} local evidence: {error}");
                }
            }
        }
        if let Some(reason) = pending {
            last_pending = reason;
            sleep_until_local_evidence_poll(deadline);
            continue;
        }

        let after = match nodes[probe].durable_assignment_observation(deadline) {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                last_pending = format!("node{probe} durable-after sample was unavailable");
                sleep_until_local_evidence_poll(deadline);
                continue;
            }
            Err(error) => panic!("{context}: invalid durable-after sample: {error}"),
        };
        match classify_local_assignment_cut(&before, evidence_by_node, &after) {
            Ok(Some(convergence)) if remaining_at(deadline, Instant::now()).is_some() => {
                return convergence;
            }
            Ok(Some(_)) => {
                last_pending = "exact convergence was first observed after the deadline".into();
                break;
            }
            Ok(None) => {
                last_pending = format!(
                    "durable/local assignment cut had not converged (before={}, after={})",
                    before.version, after.version
                );
            }
            Err(error) => panic!("{context}: contradictory assignment evidence: {error}"),
        }
        sleep_until_local_evidence_poll(deadline);
    }
    panic!(
        "soak: {context} did not reach exact local assignment convergence before its existing deadline: {last_pending}"
    );
}

fn validate_local_exact_snapshot(
    rows: &[serde_json::Value],
    produced_count: u64,
    groups: u64,
    span: u64,
) -> BTreeMap<u64, (u64, u64)> {
    let mut observed = BTreeMap::new();
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
        assert!(
            observed.insert(key, (count, high_seq)).is_none(),
            "local exact aggregate snapshot contains duplicate key {key}"
        );
    }
    observed
}

/// Require the complete materialized aggregate for a finite deterministic source prefix. A
/// snapshot query reads recovered MV state even after the finite source is exhausted, so rollback
/// cannot be hidden by waiting for a future subscription update.
fn assert_local_exact_prefix(
    node: &mut Node,
    produced_count: u64,
    groups: u64,
    span: u64,
    deadline: Duration,
) {
    let start = Instant::now();
    let mut latest = BTreeMap::new();
    while start.elapsed() < deadline {
        node.assert_running();
        if let Some(response) = node.sql("SELECT k, n, hi FROM local_exact_agg") {
            assert_eq!(
                response
                    .get("result_type")
                    .and_then(serde_json::Value::as_str),
                Some("query"),
                "local aggregate oracle returned a non-query response: {response}"
            );
            if let Some(rows) = response.get("data").and_then(serde_json::Value::as_array) {
                latest = validate_local_exact_snapshot(rows, produced_count, groups, span);
                if (0..groups).all(|key| {
                    let count = expected_aggregate_count(produced_count, key, groups, span);
                    latest.get(&key) == Some(&(count, aggregate_high_seq(key, count, groups, span)))
                }) {
                    return;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
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

#[cfg(feature = "kafka")]
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

#[cfg(feature = "kafka")]
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

#[cfg(feature = "kafka")]
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

#[cfg(feature = "kafka")]
fn observed_leader(nodes: &[Node]) -> Option<usize> {
    let roles: Option<Vec<bool>> = nodes.iter().map(Node::is_leader).collect();
    let leaders: Vec<usize> = roles?
        .into_iter()
        .enumerate()
        .filter_map(|(id, is_leader)| is_leader.then_some(id))
        .collect();
    (leaders.len() == 1).then_some(leaders[0])
}

#[cfg(feature = "kafka")]
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

#[cfg(feature = "kafka")]
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
#[cfg(feature = "kafka")]
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

#[cfg(feature = "kafka")]
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
    assert!(
        cfg!(debug_assertions),
        "local exact-state kill injection requires `cargo test --profile soak`; the release \
         profile excludes the test-only checkpoint gate"
    );
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
    let checkpoint_dir = dir.path().join("checkpoints");
    let log_dir =
        Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-local-exact-{}", soak_run_id()));
    std::fs::create_dir(&log_dir).expect("create exclusive local exact soak log directory");
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
        #[cfg(feature = "kafka")]
        process_generation: 0,
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
    let moving_checkpoint_sequence =
        local_exact_checkpoint_source_sequence(&checkpoint_dir, moving_checkpoint)
            .unwrap_or_else(|error| panic!("moving-source recovery cut is invalid: {error}"));
    assert!(
        moving_checkpoint.checkpoint_id >= initial_checkpoint.checkpoint_id
            && moving_checkpoint.epoch >= initial_checkpoint.epoch,
        "moving-source durable cut regressed from {initial_checkpoint:?} to {moving_checkpoint:?}"
    );
    assert!(
        moving_checkpoint_sequence < prefix_rows,
        "moving-source fault selected an already exhausted durable cut: sequence={moving_checkpoint_sequence}, prefix={prefix_rows}"
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
        assert_no_local_checkpoint_consistency_fault(&node);
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
        let durable_source_sequence =
            local_exact_checkpoint_source_sequence(&checkpoint_dir, durable_checkpoint)
                .unwrap_or_else(|error| panic!("frozen-prefix recovery cut is invalid: {error}"));
        assert_eq!(
            durable_source_sequence, prefix_rows,
            "checkpoint {durable_checkpoint:?} does not contain the exhausted finite source cut"
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
    while remaining_at(steady_deadline, Instant::now()).is_some() {
        latest_epoch = assert_checkpoint_progress(
            std::slice::from_mut(&mut node),
            recovery_ceiling,
            "local exact steady progress",
            latest_epoch,
        );
    }
    assert_no_local_checkpoint_consistency_fault(&node);
    node.kill9();
}

#[test]
#[ignore = "spawns 3 real laminardb processes; run with --ignored"]
#[cfg(feature = "kafka")]
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
    assert!(
        cfg!(debug_assertions) || (fault_role.is_none() && max_kills == 0),
        "cluster checkpoint fault injection requires `cargo test --profile soak`; the release \
         profile excludes the test-only injector and checkpoint gate"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let url = std::env::var("LAMINAR_SOAK_CHECKPOINT_URL").expect(
        "three_node_kill9_soak requires cluster-shared LAMINAR_SOAK_CHECKPOINT_URL storage",
    );
    let brokers = std::env::var("LAMINAR_SOAK_KAFKA_SOURCE_BROKERS")
        .expect("three_node_kill9_soak requires LAMINAR_SOAK_KAFKA_SOURCE_BROKERS");
    let run_id = soak_run_id();
    let input_topic = format!("soak-cluster-in-{run_id}");
    let output_topic = format!("soak-cluster-out-{run_id}");
    let consumer_group = format!("soak-cluster-{run_id}");
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
    let log_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive cluster soak log directory");
    eprintln!("soak: node logs in {}", log_dir.display());

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
            process_generation: 0,
            http_port: BASE_PORT + id as u16,
            fault_trigger_path: fault_role
                .as_ref()
                .map(|_| dir.path().join(format!("fault-node-{id}.trigger"))),
            checkpoint_gate_path: (max_kills > 0)
                .then(|| dir.path().join(format!("checkpoint-node-{id}.arm"))),
        })
        .collect();
    let mut latency_evidence = CheckpointLatencyEvidence::default();
    let mut exact_timing_evidence = CheckpointBarrierTimingEvidence::default();
    for n in &mut nodes {
        n.spawn();
        // Stagger process startup to reduce formation churn; role is observed from the API below.
        std::thread::sleep(Duration::from_millis(500));
    }

    // On boot failure dump the node log tails so the cause is visible in test output.
    let boot = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        wait_for(
            "all nodes to complete startup authority and become ready",
            Duration::from_secs(60),
            || {
                producer.assert_running();
                nodes.iter_mut().all(|n| {
                    n.assert_running();
                    n.epoch().is_some() && n.is_ready()
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
    exact_timing_evidence.capture_nodes_unbound(
        &nodes,
        Instant::now() + Duration::from_secs(10),
        "initial readiness",
    );
    wait_for(
        "every node to observe full cluster membership",
        Duration::from_secs(60),
        || {
            assert_running_nodes(&mut nodes);
            producer.assert_running();
            has_full_membership(&nodes)
        },
    );
    exact_timing_evidence.capture_nodes_unbound(
        &nodes,
        Instant::now() + Duration::from_secs(10),
        "initial full membership",
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
    exact_timing_evidence.capture_nodes_unbound(
        &nodes,
        Instant::now() + Duration::from_secs(10),
        "startup progress",
    );
    assert_every_node_ingests(&mut nodes, &mut producer, Duration::from_secs(60));
    let all_live_nodes: BTreeSet<usize> = (0..NODES).collect();
    let mut local_convergence = wait_for_local_assignment_convergence(
        &mut nodes,
        &all_live_nodes,
        Instant::now() + Duration::from_secs(60),
        "stable startup local assignment",
    );
    exact_timing_evidence.capture_nodes_bound(
        &nodes,
        &local_convergence.evidence_by_node,
        Instant::now() + Duration::from_secs(10),
        "stable startup",
    );

    let mut explicit_fault_evidence = None;
    if let Some(role) = fault_role.as_deref() {
        assert_no_unsolicited_cold_start_recovery(&nodes);
        let leader = wait_for_stable_leader(&mut nodes, &mut producer);
        let victim = match role {
            "leader" => leader,
            "follower" => (0..NODES)
                .find(|id| *id != leader)
                .expect("three-node cluster has no follower"),
            _ => unreachable!(),
        };
        let pipeline_fault_baselines: Vec<f64> = nodes
            .iter()
            .map(|node| {
                node.metric("laminardb_pipeline_faults_total")
                    .expect("node did not expose pipeline_faults_total")
            })
            .collect();
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
        let CheckpointFailureSnapshot {
            totals: checkpoint_failure_baselines,
            log_offsets: fault_log_offsets,
            ..
        } = capture_checkpoint_failure_snapshot(&nodes, Duration::from_secs(5));
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
                    .is_some_and(|faults| faults > pipeline_fault_baselines[victim])
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
        latest_checkpoint = assert_progress(
            &mut nodes,
            Some(&mut producer),
            Some(&commit_oracle),
            remaining_progress_window(recovery_deadline, "coordinated recovery"),
            "progress after coordinated recovery",
            Some(latest_checkpoint),
        );
        local_convergence = wait_for_local_assignment_convergence(
            &mut nodes,
            &all_live_nodes,
            recovery_deadline,
            "post-recovery local assignment",
        );
        exact_timing_evidence.capture_nodes_bound(
            &nodes,
            &local_convergence.evidence_by_node,
            Instant::now() + Duration::from_secs(10),
            "post-recovery",
        );
        let failure_snapshot = capture_checkpoint_failure_snapshot(&nodes, Duration::from_secs(5));
        let checkpoint_failure_totals = failure_snapshot.totals.clone();
        let fault_logs = failure_snapshot
            .log_prefixes
            .iter()
            .zip(&fault_log_offsets)
            .enumerate()
            .map(|(node_id, (log, offset))| {
                let offset = usize::try_from(*offset)
                    .unwrap_or_else(|_| panic!("node{node_id} log offset exceeds usize"));
                assert!(
                    offset <= log.len(),
                    "node{node_id} checkpoint evidence log shrank below its fault boundary"
                );
                String::from_utf8_lossy(&log[offset..]).into_owned()
            })
            .collect::<Vec<_>>();
        let leader_log =
            String::from_utf8_lossy(&failure_snapshot.log_prefixes[leader]).into_owned();
        let interrupted_checkpoint = validate_recovery_checkpoint_failure_evidence(
            &checkpoint_failure_baselines,
            &checkpoint_failure_totals,
            leader,
            &fault_logs,
            &leader_log,
            latest_checkpoint,
        )
        .unwrap_or_else(|error| panic!("invalid explicit recovery checkpoint failures: {error}"));
        let evidence = ExplicitFaultEvidence {
            victim,
            recovery_leader: leader,
            log_offsets: fault_log_offsets,
            pipeline_fault_baselines,
            recovery_baselines,
            recovery_failure_baselines: failure_baselines,
            checkpoint_failure_baselines,
            checkpoint_failure_totals,
            interrupted_checkpoint,
            resumed_checkpoint: latest_checkpoint,
        };
        assert_explicit_fault_recovery_evidence(&nodes, &evidence);
        explicit_fault_evidence = Some(evidence);
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
        let previous_assignment_version = local_convergence.snapshot.version;
        let previous_victim_evidence = local_convergence
            .evidence_by_node
            .get(&victim)
            .unwrap_or_else(|| panic!("node{victim} was absent from the converged local cut"))
            .clone();
        let pre_arm_latency = nodes[victim]
            .checkpoint_latency_metrics()
            .expect("kill victim did not expose checkpoint latency metrics");
        assert!(
            pre_arm_latency.pipeline_stall_observations > 0.0,
            "node{victim} process generation {} had no completed checkpoint stall observation before arming kill-{round}",
            nodes[victim].process_generation
        );
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
        exact_timing_evidence
            .finalize_node(
                &nodes[victim],
                local_process_identity(&previous_victim_evidence),
                &mut latency_evidence,
                Instant::now() + Duration::from_secs(10),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "kill-{round} could not finalize node{victim} checkpoint timing evidence: {error}"
                )
            });
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
        let survivor_nodes: BTreeSet<usize> = (0..NODES).filter(|node| *node != victim).collect();
        let survivor_convergence = wait_for_local_assignment_convergence(
            &mut nodes,
            &survivor_nodes,
            failover_deadline,
            "post-kill survivor local assignment",
        );
        for survivor in &survivor_nodes {
            let expected_process = survivor_convergence
                .evidence_by_node
                .get(survivor)
                .map(local_process_identity)
                .unwrap_or_else(|| {
                    panic!("kill-{round} converged authority omitted survivor node{survivor}")
                });
            exact_timing_evidence
                .capture_node(
                    &nodes[*survivor],
                    Some(expected_process),
                    Instant::now() + Duration::from_secs(10),
                )
                .unwrap_or_else(|error| {
                    panic!(
                        "kill-{round} survivor node{survivor} exact timing capture failed: {error}"
                    )
                });
        }
        assert!(
            survivor_convergence.snapshot.version > previous_assignment_version,
            "kill-{round} survivor assignment did not advance beyond {previous_assignment_version}"
        );
        let old_stable_node = previous_victim_evidence.participant.node_id;
        let survivor_fence = survivor_convergence
            .snapshot
            .assignment_fence()
            .expect("converged survivor assignment is canonical");
        assert!(
            !survivor_fence.contains(old_stable_node)
                && survivor_convergence
                    .snapshot
                    .vnodes
                    .values()
                    .all(|owner| owner.0 != old_stable_node),
            "kill-{round} survivor assignment retained old victim process authority"
        );
        let failover_elapsed = assert_recovery_within(
            failover_started,
            recovery_ceiling,
            "kill-9 to survivor progress and exact local assignment convergence",
        );
        eprintln!(
            "soak round {round}: survivors advanced to checkpoint {} epoch {} with exact local assignment evidence in {failover_elapsed:?}",
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
        let rejoined_convergence = wait_for_local_assignment_convergence(
            &mut nodes,
            &all_live_nodes,
            rejoin_deadline,
            "rejoined local assignment",
        );
        exact_timing_evidence.capture_nodes_bound(
            &nodes,
            &rejoined_convergence.evidence_by_node,
            Instant::now() + Duration::from_secs(10),
            &format!("kill-{round} rejoin"),
        );
        assert!(
            rejoined_convergence.snapshot.version > survivor_convergence.snapshot.version,
            "kill-{round} rejoin assignment did not advance beyond survivor version {}",
            survivor_convergence.snapshot.version
        );
        let current_victim_evidence = rejoined_convergence
            .evidence_by_node
            .get(&victim)
            .unwrap_or_else(|| panic!("rejoined node{victim} was absent from local evidence"));
        assert_eq!(
            current_victim_evidence.participant.node_id, old_stable_node,
            "rejoined node{victim} changed stable node identity"
        );
        assert_ne!(
            current_victim_evidence.participant.boot_incarnation,
            previous_victim_evidence.participant.boot_incarnation,
            "rejoined node{victim} reused its killed boot incarnation"
        );
        assert!(
            current_victim_evidence.process_term > previous_victim_evidence.process_term,
            "rejoined node{victim} process term {} did not exceed killed term {}",
            current_victim_evidence.process_term,
            previous_victim_evidence.process_term
        );
        let rejoined_fence = rejoined_convergence
            .snapshot
            .assignment_fence()
            .expect("converged rejoin assignment is canonical");
        assert_eq!(
            rejoined_fence.participant_incarnation(old_stable_node),
            Some(current_victim_evidence.participant.boot_incarnation),
            "rejoin assignment did not bind the new victim boot"
        );
        assert!(
            rejoined_convergence
                .snapshot
                .vnodes
                .values()
                .any(|owner| owner.0 == old_stable_node),
            "rejoined node{victim} did not own any vnode"
        );
        let rejoin_elapsed = assert_recovery_within(
            rejoin_started,
            recovery_ceiling,
            "restarted node to durable owned-work progress and exact local assignment convergence",
        );
        eprintln!(
            "soak round {round}: node {victim} rejoined and resumed durable work with exact local assignment evidence in {rejoin_elapsed:?}"
        );
        local_convergence = rejoined_convergence;
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
    while remaining_at(steady_deadline, Instant::now()).is_some() {
        round += 1;
        latest_checkpoint = assert_progress(
            &mut nodes,
            Some(&mut producer),
            Some(&commit_oracle),
            // The soak deadline decides whether to start another proof round. Once started, the
            // round gets the full configured liveness SLO instead of an artificial truncated
            // timeout at the end of the requested observation period.
            recovery_ceiling,
            "steady progress",
            Some(latest_checkpoint),
        );
        exact_timing_evidence.capture_nodes_bound(
            &nodes,
            &local_convergence.evidence_by_node,
            Instant::now() + Duration::from_secs(10),
            &format!("steady round {round}"),
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

    assert_active_load_throughput(
        &mut nodes,
        &mut producer,
        &commit_oracle,
        &output_oracle,
        source_rps,
        recovery_ceiling,
    );
    exact_timing_evidence.capture_nodes_bound(
        &nodes,
        &local_convergence.evidence_by_node,
        Instant::now() + Duration::from_secs(10),
        "active-load boundary",
    );
    if let Some(evidence) = &explicit_fault_evidence {
        assert_explicit_fault_recovery_evidence(&nodes, evidence);
    }

    // Freeze the exact broker-acknowledged input offsets. Require that source commits cover them
    // and two later checkpoints complete before freezing the sink broker boundaries.
    let produced_prefix = producer.stop();
    let produced_count = produced_prefix.count;
    assert!(produced_count > 0, "soak producer emitted no input records");
    let achieved_rps = produced_count as f64 / produced_prefix.elapsed.as_secs_f64();
    eprintln!(
        "soak: producer acknowledged {produced_count} records in {:.1}s ({achieved_rps:.1} rps; target {source_rps} rps)",
        produced_prefix.elapsed.as_secs_f64()
    );
    assert!(
        achieved_rps >= source_rps as f64 * 0.9,
        "soak producer achieved only {achieved_rps:.1} rps against target {source_rps} rps"
    );
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
    exact_timing_evidence.capture_nodes_bound(
        &nodes,
        &local_convergence.evidence_by_node,
        Instant::now() + Duration::from_secs(10),
        "final durable input cut",
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
    exact_timing_evidence.capture_nodes_bound(
        &nodes,
        &local_convergence.evidence_by_node,
        Instant::now() + Duration::from_secs(10),
        "final output validation",
    );

    for node in &nodes {
        let expected_process = local_convergence
            .evidence_by_node
            .get(&node.id)
            .map(local_process_identity)
            .unwrap_or_else(|| panic!("final converged authority omitted node{}", node.id));
        exact_timing_evidence
            .finalize_node(
                node,
                expected_process,
                &mut latency_evidence,
                Instant::now() + Duration::from_secs(10),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "final node{} checkpoint timing evidence did not stabilize: {error}",
                    node.id
                )
            });
    }
    exact_timing_evidence
        .validate_observed_cuts(&latency_evidence, &nodes)
        .unwrap_or_else(|error| {
            panic!("incomplete exact checkpoint observed-cut evidence: {error}")
        });
    exact_timing_evidence.report();
    latency_evidence.report();
}

/// Create `topic` with `partitions` partitions (blocking; the admin API is async).
#[cfg(feature = "kafka")]
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
/// Deliveries are bounded and awaited concurrently so broker-side rejection or timeout fails the
/// soak without serializing the configured input rate on round-trip latency.
/// Explicit round-robin assignment guarantees that every source partition and vnode receives
/// records; Kafka keys remain unique diagnostics and do not determine placement.
#[cfg(feature = "kafka")]
fn produce_seq(
    brokers: &str,
    topic: &str,
    partitions: i32,
    rps: u64,
    stop: &AtomicBool,
    enqueued_count: &AtomicU64,
) -> ProducedPrefix {
    use futures::stream::{FuturesUnordered, StreamExt as _};
    use rdkafka::producer::{FutureProducer, FutureRecord};

    fn record_delivery(
        result: Result<
            rdkafka::producer::future_producer::OwnedDeliveryResult,
            futures::channel::oneshot::Canceled,
        >,
        end_offsets: &mut [i64],
        acknowledged: &mut u64,
    ) {
        let delivery = result
            .expect("Kafka delivery future was cancelled before the producer stopped")
            .unwrap_or_else(|(error, _)| panic!("Kafka delivery failed: {error}"));
        let partition = usize::try_from(delivery.partition)
            .expect("Kafka returned a negative delivery partition");
        let boundary = end_offsets
            .get_mut(partition)
            .expect("Kafka returned an out-of-range delivery partition");
        *boundary = (*boundary).max(delivery.offset.saturating_add(1));
        *acknowledged = acknowledged
            .checked_add(1)
            .expect("soak acknowledgement count overflow");
    }

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
        let mut deliveries = FuturesUnordered::new();
        let mut acknowledged = 0u64;
        while !stop.load(Ordering::Acquire) {
            if deliveries.len() >= SOAK_PRODUCER_MAX_IN_FLIGHT {
                let delivery = deliveries
                    .next()
                    .await
                    .expect("bounded producer has an in-flight delivery");
                record_delivery(delivery, &mut end_offsets, &mut acknowledged);
                continue;
            }

            let target = start + Duration::from_secs_f64(n as f64 / rps as f64);
            if target > tokio::time::Instant::now() {
                if deliveries.is_empty() {
                    tokio::time::sleep_until(target).await;
                } else {
                    tokio::select! {
                        delivery = deliveries.next() => {
                            record_delivery(
                                delivery.expect("producer has an in-flight delivery"),
                                &mut end_offsets,
                                &mut acknowledged,
                            );
                            continue;
                        }
                        () = tokio::time::sleep_until(target) => {}
                    }
                }
            }
            if stop.load(Ordering::Acquire) {
                break;
            }

            let payload = format!(r#"{{"seq":{n}}}"#);
            let key = n.to_string();
            let partition =
                i32::try_from(n % partition_count).expect("round-robin partition fits i32");
            let delivery = producer
                .send_result(
                    FutureRecord::to(topic)
                        .payload(&payload)
                        .key(&key)
                        .partition(partition),
                )
                .unwrap_or_else(|(error, _)| panic!("Kafka enqueue failed: {error}"));
            deliveries.push(delivery);
            n = n.checked_add(1).expect("soak sequence overflow");
            enqueued_count.store(n, Ordering::Release);
        }
        while let Some(delivery) = deliveries.next().await {
            record_delivery(delivery, &mut end_offsets, &mut acknowledged);
        }
        assert_eq!(
            acknowledged, n,
            "producer stopped before every enqueued record was acknowledged"
        );
        ProducedPrefix {
            count: acknowledged,
            end_offsets,
            elapsed: start.elapsed(),
        }
    })
}

#[test]
fn checkpoint_epoch_progress_requires_strict_advance() {
    assert!(checkpoint_epoch_advanced(7, 8));
    assert!(!checkpoint_epoch_advanced(7, 7));
    assert!(!checkpoint_epoch_advanced(7, 6));
}

#[cfg(feature = "kafka")]
#[test]
fn explicit_fault_oracle_requires_one_fault_on_only_the_victim() {
    let baselines = [3.0, 7.0, 2.0];
    assert!(validate_explicit_pipeline_fault_totals(&baselines, &[3.0, 8.0, 2.0], 1).is_ok());
    assert!(
        validate_explicit_pipeline_fault_totals(&baselines, &[4.0, 8.0, 2.0], 1)
            .unwrap_err()
            .contains("node0")
    );
    assert!(
        validate_explicit_pipeline_fault_totals(&baselines, &[3.0, 9.0, 2.0], 1)
            .unwrap_err()
            .contains("node1")
    );
}

#[cfg(feature = "kafka")]
#[test]
fn recovery_checkpoint_failure_oracle_allows_only_one_leader_abort() {
    let baselines = [3.0, 7.0, 2.0];
    assert!(validate_recovery_checkpoint_failure_totals(&baselines, &baselines, 0).is_ok());
    assert!(validate_recovery_checkpoint_failure_totals(&baselines, &[4.0, 7.0, 2.0], 0).is_ok());
    assert!(
        validate_recovery_checkpoint_failure_totals(&baselines, &[5.0, 7.0, 2.0], 0)
            .unwrap_err()
            .contains("node0")
    );
    assert!(
        validate_recovery_checkpoint_failure_totals(&baselines, &[4.0, 8.0, 2.0], 0)
            .unwrap_err()
            .contains("node1")
    );
}

#[cfg(feature = "kafka")]
#[test]
fn recovery_checkpoint_failure_evidence_binds_the_interrupted_attempt() {
    let baselines = [3.0, 7.0, 2.0];
    let totals = [4.0, 7.0, 2.0];
    let failed = DurableCheckpointStatus {
        checkpoint_id: 4,
        epoch: 4,
    };
    let resumed = DurableCheckpointStatus {
        checkpoint_id: 5,
        epoch: 5,
    };
    let metric = format!("checkpoint_id=4 epoch=4 {CHECKPOINT_FAILURE_METRIC_LOG}");
    let leader_log = format!(
        "checkpoint_id=4 epoch=4 {CHECKPOINT_ATTEMPT_RESERVED_LOG}\n{RECOVERY_PREPARE_LOG}\n{metric}\n{RECOVERY_RELEASE_LOG}\ncheckpoint_id=5 epoch=5 checkpoint completed"
    );
    let fault_logs = vec![
        format!("{RECOVERY_PREPARE_LOG}\n{metric}\n{RECOVERY_RELEASE_LOG}"),
        RECOVERY_RELEASE_LOG.into(),
        RECOVERY_RELEASE_LOG.into(),
    ];

    assert_eq!(
        validate_recovery_checkpoint_failure_evidence(
            &baselines,
            &totals,
            0,
            &fault_logs,
            &leader_log,
            resumed,
        )
        .unwrap(),
        Some(failed)
    );

    let pre_prepare_failure_logs = vec![
        format!("{metric}\n{RECOVERY_PREPARE_LOG}\n{RECOVERY_RELEASE_LOG}"),
        RECOVERY_RELEASE_LOG.into(),
        RECOVERY_RELEASE_LOG.into(),
    ];
    let error = validate_recovery_checkpoint_failure_evidence(
        &baselines,
        &totals,
        0,
        &pre_prepare_failure_logs,
        &leader_log,
        resumed,
    )
    .unwrap_err();
    assert!(
        error.contains("not caused by the injected recovery"),
        "{error}"
    );

    let no_failure_logs = vec![
        format!("{RECOVERY_PREPARE_LOG}\n{RECOVERY_RELEASE_LOG}"),
        RECOVERY_RELEASE_LOG.into(),
        RECOVERY_RELEASE_LOG.into(),
    ];
    assert_eq!(
        validate_recovery_checkpoint_failure_evidence(
            &baselines,
            &baselines,
            0,
            &no_failure_logs,
            RECOVERY_RELEASE_LOG,
            resumed,
        )
        .unwrap(),
        None
    );

    let error = validate_recovery_checkpoint_failure_evidence(
        &baselines,
        &baselines,
        0,
        &fault_logs,
        &leader_log,
        resumed,
    )
    .unwrap_err();
    assert!(error.contains("exact metric records"), "{error}");

    let duplicate_logs = vec![
        format!("{RECOVERY_PREPARE_LOG}\n{metric}\n{metric}\n{RECOVERY_RELEASE_LOG}"),
        RECOVERY_RELEASE_LOG.into(),
        RECOVERY_RELEASE_LOG.into(),
    ];
    let error = validate_recovery_checkpoint_failure_evidence(
        &baselines,
        &totals,
        0,
        &duplicate_logs,
        &leader_log,
        resumed,
    )
    .unwrap_err();
    assert!(error.contains("exact metric records"), "{error}");

    let completed_log = format!(
        "checkpoint_id=4 epoch=4 {CHECKPOINT_ATTEMPT_RESERVED_LOG}\ncheckpoint_id=4 epoch=4 checkpoint completed\n{RECOVERY_PREPARE_LOG}\n{metric}\n{RECOVERY_RELEASE_LOG}"
    );
    let error = validate_recovery_checkpoint_failure_evidence(
        &baselines,
        &totals,
        0,
        &fault_logs,
        &completed_log,
        resumed,
    )
    .unwrap_err();
    assert!(error.contains("both completed"), "{error}");

    let error = validate_recovery_checkpoint_failure_evidence(
        &baselines,
        &totals,
        0,
        &fault_logs,
        &leader_log,
        failed,
    )
    .unwrap_err();
    assert!(error.contains("not strictly newer"), "{error}");
}

#[cfg(feature = "kafka")]
fn local_assignment_cut_fixture() -> (
    AssignmentSnapshot,
    BTreeMap<usize, LocalProcessAuthorityEvidence>,
) {
    let participants = vec![
        CheckpointParticipant {
            node_id: 11,
            boot_incarnation: uuid::Uuid::from_u128(11),
        },
        CheckpointParticipant {
            node_id: 22,
            boot_incarnation: uuid::Uuid::from_u128(22),
        },
    ];
    let snapshot = AssignmentSnapshot {
        version: 7,
        partitioning_abi_version: laminar_core::state::PARTITIONING_ABI_VERSION,
        vnodes: [
            (0, laminar_core::cluster::discovery::NodeId(11)),
            (1, laminar_core::cluster::discovery::NodeId(22)),
        ]
        .into_iter()
        .collect(),
        participants: participants.clone(),
        updated_at_ms: 1,
        draining: false,
        drain_transition: None,
    };
    let fence = snapshot.assignment_fence().unwrap();
    let evidence = participants
        .into_iter()
        .enumerate()
        .map(|(node, participant)| {
            (
                node,
                LocalProcessAuthorityEvidence {
                    participant,
                    process_term: u64::try_from(node).unwrap() + 1,
                    adopted_assignment: CheckpointAssignmentAdoption {
                        participant,
                        assignment_version: fence.assignment_version,
                        partitioning_abi_version: fence.partitioning_abi_version,
                        vnode_count: fence.vnode_count,
                        assignment_digest: fence.assignment_digest,
                    },
                },
            )
        })
        .collect();
    (snapshot, evidence)
}

#[cfg(feature = "kafka")]
#[test]
fn local_assignment_cut_classifier_separates_pending_from_contradiction() {
    let (snapshot, mut evidence) = local_assignment_cut_fixture();
    let converged = classify_local_assignment_cut(&snapshot, evidence.clone(), &snapshot)
        .unwrap()
        .unwrap();
    assert_eq!(converged.snapshot.version, 7);

    let mut changed_after = snapshot.clone();
    changed_after.updated_at_ms += 1;
    assert!(
        classify_local_assignment_cut(&snapshot, evidence.clone(), &changed_after)
            .unwrap()
            .is_none()
    );

    let mut draining = snapshot.clone();
    draining.draining = true;
    assert!(
        classify_local_assignment_cut(&draining, evidence.clone(), &draining)
            .unwrap()
            .is_none()
    );

    let mut trailing = evidence.clone();
    trailing
        .get_mut(&0)
        .unwrap()
        .adopted_assignment
        .assignment_version -= 1;
    assert!(
        classify_local_assignment_cut(&snapshot, trailing, &snapshot)
            .unwrap()
            .is_none()
    );

    let mut missing = evidence.clone();
    missing.remove(&0);
    assert!(classify_local_assignment_cut(&snapshot, missing, &snapshot)
        .unwrap()
        .is_none());

    let mut ahead = evidence.clone();
    ahead
        .get_mut(&0)
        .unwrap()
        .adopted_assignment
        .assignment_version += 1;
    let error = classify_local_assignment_cut(&snapshot, ahead, &snapshot).unwrap_err();
    assert!(error.contains("is ahead of stable durable head"), "{error}");

    let mut conflicting = evidence.clone();
    conflicting
        .get_mut(&0)
        .unwrap()
        .adopted_assignment
        .assignment_digest[0] ^= 0xff;
    let error = classify_local_assignment_cut(&snapshot, conflicting, &snapshot).unwrap_err();
    assert!(
        error.contains("conflicts with durable assignment"),
        "{error}"
    );

    let participant = evidence.get(&0).unwrap().participant;
    evidence.get_mut(&1).unwrap().participant = participant;
    evidence.get_mut(&1).unwrap().adopted_assignment.participant = participant;
    let error = classify_local_assignment_cut(&snapshot, evidence, &snapshot).unwrap_err();
    assert!(
        error.contains("multiple live processes reported stable node"),
        "{error}"
    );
}

#[test]
fn recovery_log_match_binds_checkpoint_and_epoch() {
    let expected = DurableCheckpointStatus {
        checkpoint_id: 41,
        epoch: 41,
    };
    assert!(log_line_reports_recovery(
        "Recovered from unified checkpoint checkpoint_id=41 epoch=41",
        expected
    ));
    assert!(log_line_reports_recovery(
        "Recovered from unified checkpoint checkpoint_id: 41 epoch: 41",
        expected
    ));
    assert!(!log_line_reports_recovery(
        "Recovered from unified checkpoint checkpoint_id=40 epoch=40",
        expected
    ));
    assert!(!log_line_reports_recovery(
        "Recovered from unified checkpoint checkpoint_id=42 epoch=42",
        expected
    ));
    assert!(!log_line_reports_recovery(
        "Recovered from unified checkpoint checkpoint_id=410 epoch=410",
        expected
    ));
    assert!(!log_line_reports_recovery(
        "Recovered from unified checkpoint previous_checkpoint_id=41 epoch=41",
        expected
    ));
    assert!(!log_line_reports_recovery(
        "Recovered from unified checkpoint checkpoint_id=41 epoch=43",
        expected
    ));
    assert!(!log_line_reports_recovery(
        "Recovered from unified checkpoint checkpoint_id=41 epoch=41",
        DurableCheckpointStatus {
            checkpoint_id: 41,
            epoch: 43,
        }
    ));
}

#[cfg(feature = "kafka")]
#[test]
fn checkpoint_completion_log_match_binds_checkpoint_and_epoch() {
    let expected = DurableCheckpointStatus {
        checkpoint_id: 41,
        epoch: 41,
    };
    assert!(log_line_reports_checkpoint_completion(
        "checkpoint completed checkpoint_id=41 epoch=41",
        expected
    ));
    assert!(!log_line_reports_checkpoint_completion(
        "checkpoint completed checkpoint_id=40 epoch=40",
        expected
    ));
    assert!(!log_line_reports_checkpoint_completion(
        "checkpoint completed checkpoint_id=42 epoch=42",
        expected
    ));
    assert!(!log_line_reports_checkpoint_completion(
        "checkpoint completed checkpoint_id=41 epoch=43",
        expected
    ));
    assert!(!log_line_reports_checkpoint_completion(
        "checkpoint failed checkpoint_id=41 epoch=41",
        expected
    ));
}

#[cfg(feature = "kafka")]
#[test]
fn checkpoint_log_parsers_reject_zero_and_split_identities() {
    for (checkpoint_id, epoch) in [(0, 0), (41, 43)] {
        let reservation = format!(
            "checkpoint_id={checkpoint_id} epoch={epoch} {CHECKPOINT_ATTEMPT_RESERVED_LOG}"
        );
        let error = checkpoint_reservation_from_log_line(&reservation).unwrap_err();
        assert!(error.contains("non-canonical identity"), "{error}");

        let failure =
            format!("checkpoint_id={checkpoint_id} epoch={epoch} {CHECKPOINT_FAILURE_METRIC_LOG}");
        let error = checkpoint_failure_metric_from_log_line(&failure).unwrap_err();
        assert!(error.contains("non-canonical identity"), "{error}");
    }
}

#[cfg(feature = "kafka")]
#[test]
fn post_release_lifecycle_requires_first_reserved_attempt_to_complete() {
    let logs = vec![
        format!(
            "{RECOVERY_RELEASE_LOG}\ncheckpoint_id=41 epoch=41 {CHECKPOINT_ATTEMPT_RESERVED_LOG}\ncheckpoint_id=41 epoch=41 checkpoint completed\ncheckpoint_id=42 epoch=42 {CHECKPOINT_ATTEMPT_RESERVED_LOG}\ncheckpoint_id=42 epoch=42 checkpoint completed"
        ),
        RECOVERY_RELEASE_LOG.to_string(),
        RECOVERY_RELEASE_LOG.to_string(),
    ];
    assert_eq!(
        validate_post_release_checkpoint_lifecycle(
            &logs,
            DurableCheckpointStatus {
                checkpoint_id: 42,
                epoch: 42,
            },
        )
        .unwrap(),
        DurableCheckpointStatus {
            checkpoint_id: 41,
            epoch: 41,
        }
    );

    let skipped = vec![
        format!(
            "{RECOVERY_RELEASE_LOG}\ncheckpoint_id=41 epoch=41 {CHECKPOINT_ATTEMPT_RESERVED_LOG}\nunrecognized failure text\ncheckpoint_id=42 epoch=42 {CHECKPOINT_ATTEMPT_RESERVED_LOG}\ncheckpoint_id=42 epoch=42 checkpoint completed"
        ),
        RECOVERY_RELEASE_LOG.to_string(),
        RECOVERY_RELEASE_LOG.to_string(),
    ];
    let error = validate_post_release_checkpoint_lifecycle(
        &skipped,
        DurableCheckpointStatus {
            checkpoint_id: 42,
            epoch: 42,
        },
    )
    .unwrap_err();
    assert!(error.contains("checkpoint 41 epoch 41"), "{error}");
}

#[cfg(feature = "kafka")]
#[test]
fn post_release_lifecycle_rejects_missing_or_pre_release_evidence() {
    let resumed = DurableCheckpointStatus {
        checkpoint_id: 41,
        epoch: 41,
    };
    let no_reservation = vec![
        format!("{RECOVERY_RELEASE_LOG}\ncheckpoint_id=41 epoch=41 checkpoint completed"),
        RECOVERY_RELEASE_LOG.to_string(),
        RECOVERY_RELEASE_LOG.to_string(),
    ];
    assert!(
        validate_post_release_checkpoint_lifecycle(&no_reservation, resumed)
            .unwrap_err()
            .contains("no checkpoint attempt was reserved")
    );

    let completion_before_release = vec![
        format!(
            "checkpoint_id=41 epoch=41 checkpoint completed\n{RECOVERY_RELEASE_LOG}\ncheckpoint_id=41 epoch=41 {CHECKPOINT_ATTEMPT_RESERVED_LOG}"
        ),
        RECOVERY_RELEASE_LOG.to_string(),
        RECOVERY_RELEASE_LOG.to_string(),
    ];
    assert!(
        validate_post_release_checkpoint_lifecycle(&completion_before_release, resumed)
            .unwrap_err()
            .contains("did not complete")
    );

    let completion_before_reservation = vec![
        format!(
            "{RECOVERY_RELEASE_LOG}\ncheckpoint_id=41 epoch=41 checkpoint completed\ncheckpoint_id=41 epoch=41 {CHECKPOINT_ATTEMPT_RESERVED_LOG}"
        ),
        RECOVERY_RELEASE_LOG.to_string(),
        RECOVERY_RELEASE_LOG.to_string(),
    ];
    assert!(
        validate_post_release_checkpoint_lifecycle(&completion_before_reservation, resumed)
            .unwrap_err()
            .contains("did not complete after its reservation")
    );

    let completion_on_another_node = vec![
        format!(
            "{RECOVERY_RELEASE_LOG}\ncheckpoint_id=41 epoch=41 {CHECKPOINT_ATTEMPT_RESERVED_LOG}"
        ),
        format!("{RECOVERY_RELEASE_LOG}\ncheckpoint_id=41 epoch=41 checkpoint completed"),
        RECOVERY_RELEASE_LOG.to_string(),
    ];
    assert!(
        validate_post_release_checkpoint_lifecycle(&completion_on_another_node, resumed)
            .unwrap_err()
            .contains("did not complete after its reservation")
    );

    let resumed_without_reservation = vec![
        format!(
            "{RECOVERY_RELEASE_LOG}\ncheckpoint_id=41 epoch=41 {CHECKPOINT_ATTEMPT_RESERVED_LOG}\ncheckpoint_id=41 epoch=41 checkpoint completed\ncheckpoint_id=42 epoch=42 checkpoint completed"
        ),
        RECOVERY_RELEASE_LOG.to_string(),
        RECOVERY_RELEASE_LOG.to_string(),
    ];
    assert!(validate_post_release_checkpoint_lifecycle(
        &resumed_without_reservation,
        DurableCheckpointStatus {
            checkpoint_id: 42,
            epoch: 42,
        },
    )
    .unwrap_err()
    .contains("was not reserved"));

    let admission_failure = vec![
        format!("{RECOVERY_RELEASE_LOG}\n{CHECKPOINT_ADMISSION_FAILED_LOG}"),
        RECOVERY_RELEASE_LOG.to_string(),
        RECOVERY_RELEASE_LOG.to_string(),
    ];
    assert!(
        validate_post_release_checkpoint_lifecycle(&admission_failure, resumed)
            .unwrap_err()
            .contains("lifecycle failure")
    );

    let continuation_failure = vec![
        format!("{RECOVERY_RELEASE_LOG}\n{CHECKPOINT_CONTINUATION_FAILED_LOG}"),
        RECOVERY_RELEASE_LOG.to_string(),
        RECOVERY_RELEASE_LOG.to_string(),
    ];
    assert!(
        validate_post_release_checkpoint_lifecycle(&continuation_failure, resumed)
            .unwrap_err()
            .contains("lifecycle failure")
    );

    let split_identity = vec![
        format!(
            "{RECOVERY_RELEASE_LOG}\ncheckpoint_id=42 epoch=42 {CHECKPOINT_ATTEMPT_RESERVED_LOG}\ncheckpoint_id=42 epoch=42 checkpoint completed"
        ),
        format!(
            "{RECOVERY_RELEASE_LOG}\ncheckpoint_id=41 epoch=43 {CHECKPOINT_ATTEMPT_RESERVED_LOG}\ncheckpoint_id=41 epoch=43 checkpoint completed"
        ),
        RECOVERY_RELEASE_LOG.to_string(),
    ];
    assert!(validate_post_release_checkpoint_lifecycle(
        &split_identity,
        DurableCheckpointStatus {
            checkpoint_id: 42,
            epoch: 42,
        },
    )
    .unwrap_err()
    .contains("non-canonical identity"));

    let duplicate = vec![
        format!(
            "{RECOVERY_RELEASE_LOG}\ncheckpoint_id=41 epoch=41 {CHECKPOINT_ATTEMPT_RESERVED_LOG}\ncheckpoint_id=41 epoch=41 {CHECKPOINT_ATTEMPT_RESERVED_LOG}\ncheckpoint_id=41 epoch=41 checkpoint completed"
        ),
        RECOVERY_RELEASE_LOG.to_string(),
        RECOVERY_RELEASE_LOG.to_string(),
    ];
    assert!(
        validate_post_release_checkpoint_lifecycle(&duplicate, resumed)
            .unwrap_err()
            .contains("duplicate checkpoint reservation")
    );
}

#[cfg(feature = "kafka")]
#[test]
fn committed_offset_frontier_requires_every_partition_to_advance() {
    assert!(all_partition_offsets_advanced(&[10, 20, 30], &[11, 21, 31]).unwrap());
    assert!(!all_partition_offsets_advanced(&[10, 20, 30], &[11, 20, 31]).unwrap());
}

#[cfg(feature = "kafka")]
#[test]
fn committed_offset_frontier_rejects_malformed_samples() {
    let empty = all_partition_offsets_advanced(&[], &[]).unwrap_err();
    assert!(empty.contains("no partitions"), "{empty}");

    let changed = all_partition_offsets_advanced(&[10, 20], &[11]).unwrap_err();
    assert!(changed.contains("partition count changed"), "{changed}");

    let regressed = all_partition_offsets_advanced(&[10, 20], &[11, 19]).unwrap_err();
    assert!(regressed.contains("partition 1 regressed"), "{regressed}");

    let uninitialized = all_partition_offsets_advanced(&[-1, 20], &[0, 21]).unwrap_err();
    assert!(uninitialized.contains("uninitialized"), "{uninitialized}");
}

#[cfg(feature = "kafka")]
#[test]
fn prometheus_bucket_parser_accepts_registry_labels() {
    let body = concat!(
        "laminardb_checkpoint_pipeline_stall_duration_seconds_bucket{instance=\"node0\",pipeline=\"soak\",le=\"0.512\"} 38\n",
        "laminardb_checkpoint_pipeline_stall_duration_seconds_bucket{instance=\"node0\",pipeline=\"soak\",le=\"1.024\"} 41\n",
        "laminardb_checkpoint_pipeline_stall_duration_seconds_bucket{instance=\"node0\",pipeline=\"soak\",le=\"+Inf\"} 43\n",
    );
    assert_eq!(
        prometheus_histogram_bucket_value(
            body,
            "laminardb_checkpoint_pipeline_stall_duration_seconds_bucket",
            CHECKPOINT_PIPELINE_STALL_SLO_SECONDS,
        ),
        Some(41.0)
    );
}

#[cfg(feature = "kafka")]
fn timing_test_process(process_term: u64) -> LocalProcessAuthorityIdentity {
    LocalProcessAuthorityIdentity {
        participant: CheckpointParticipant {
            node_id: 71,
            boot_incarnation: uuid::Uuid::from_u128(7100 + u128::from(process_term)),
        },
        process_term,
    }
}

#[cfg(feature = "kafka")]
fn timing_test_record(sequence: u64) -> CheckpointBarrierTimingRecord {
    let aligned_resume_ns = sequence.is_multiple_of(2).then_some(5);
    CheckpointBarrierTimingRecord {
        sequence,
        process: timing_test_process(1),
        attempt: CheckpointAttempt::canonical(sequence),
        role: if sequence.is_multiple_of(3) {
            CheckpointBarrierTimingRole::Leader
        } else {
            CheckpointBarrierTimingRole::Follower
        },
        assignment_version: 4,
        assignment_digest: [44; 32],
        pipeline_stall_ns: 20,
        local_barrier_ns: 10,
        aligned_resume_ns,
        durable_tail_handoff: true,
        deadline_exhausted: false,
    }
}

#[cfg(feature = "kafka")]
fn timing_test_envelope(
    process: LocalProcessAuthorityIdentity,
    after_sequence: u64,
    records: Vec<CheckpointBarrierTimingRecord>,
    next_sequence: u64,
    has_more: bool,
) -> CheckpointBarrierTimingEnvelope {
    let capacity = laminar_db::checkpoint_timing::CHECKPOINT_BARRIER_TIMING_CAPACITY;
    let accepted = next_sequence.checked_sub(1).expect("test next sequence");
    let overwritten_record_count =
        accepted.saturating_sub(u64::try_from(capacity).expect("test capacity fits u64"));
    CheckpointBarrierTimingEnvelope {
        schema_version: LOCAL_CHECKPOINT_BARRIER_TIMINGS_SCHEMA.into(),
        process_identity: process,
        after_sequence,
        page: CheckpointBarrierTimingPage {
            capacity,
            oldest_retained_sequence: (accepted != 0).then_some(overwritten_record_count + 1),
            next_sequence,
            overwritten_record_count,
            recording_loss_count: 0,
            metadata_exhausted: false,
            has_more,
            records,
        },
    }
}

#[cfg(feature = "kafka")]
fn timing_test_generation(record_count: u64) -> CheckpointBarrierTimingGeneration {
    let generation = ProcessGeneration {
        node_id: 0,
        generation: 1,
    };
    let process = timing_test_process(1);
    let mut timing = CheckpointBarrierTimingGeneration::default();
    timing.bind_authority(generation, process).unwrap();
    let page_size =
        u64::try_from(laminar_db::checkpoint_timing::MAX_CHECKPOINT_BARRIER_TIMING_PAGE_RECORDS)
            .expect("test page size fits u64");
    if record_count == 0 {
        timing
            .apply_page(
                generation,
                timing_test_envelope(process, 0, Vec::new(), 1, false),
            )
            .unwrap();
        return timing;
    }
    while timing.cursor < record_count {
        let end = timing.cursor.saturating_add(page_size).min(record_count);
        let records = (timing.cursor + 1..=end)
            .map(timing_test_record)
            .collect::<Vec<_>>();
        let has_more = end < record_count;
        let envelope =
            timing_test_envelope(process, timing.cursor, records, record_count + 1, has_more);
        assert_eq!(timing.apply_page(generation, envelope).unwrap(), has_more);
    }
    timing
}

#[cfg(feature = "kafka")]
fn timing_test_metrics(timing: &CheckpointBarrierTimingGeneration) -> CheckpointLatencySnapshot {
    let count = timing.records.len() as f64;
    let aligned = timing
        .records
        .iter()
        .filter(|record| record.aligned_resume_ns.is_some())
        .count() as f64;
    CheckpointLatencySnapshot {
        checkpoint_seconds: count * 0.1,
        checkpoint_observations: count,
        pipeline_stall_observations: count,
        pipeline_stall_within_slo: timing
            .records
            .iter()
            .filter(|record| record.pipeline_stall_ns <= CHECKPOINT_PIPELINE_STALL_SLO_NS)
            .count() as f64,
        barrier_local_seconds: count * 0.01,
        barrier_local_observations: count,
        barrier_local_within_slo: timing
            .records
            .iter()
            .filter(|record| record.local_barrier_ns <= CHECKPOINT_PIPELINE_STALL_SLO_NS)
            .count() as f64,
        aligned_resume_seconds: aligned * 0.005,
        aligned_resume_observations: aligned,
        aligned_resume_within_slo: timing
            .records
            .iter()
            .filter_map(|record| record.aligned_resume_ns)
            .filter(|duration| *duration <= CHECKPOINT_PIPELINE_STALL_SLO_NS)
            .count() as f64,
        ..CheckpointLatencySnapshot::default()
    }
}

#[cfg(feature = "kafka")]
#[test]
fn exact_timing_collector_accepts_pagination_and_exact_metric_boundary() {
    let generation = ProcessGeneration {
        node_id: 0,
        generation: 1,
    };
    let mut timing = timing_test_generation(65);
    assert_eq!(timing.cursor, 65);
    assert_eq!(timing.records.len(), 65);
    assert_eq!(timing.metadata.unwrap().next_sequence, 66);

    timing.records[64].pipeline_stall_ns = CHECKPOINT_PIPELINE_STALL_SLO_NS;
    timing.records[64].local_barrier_ns = CHECKPOINT_PIPELINE_STALL_SLO_NS;
    timing.records[64].aligned_resume_ns = None;
    let metrics = timing_test_metrics(&timing);
    timing
        .validate_against_metrics(generation, metrics)
        .unwrap();
}

#[cfg(feature = "kafka")]
#[test]
fn exact_timing_collector_allows_exported_eviction_but_rejects_unread_overwrite() {
    let generation = ProcessGeneration {
        node_id: 0,
        generation: 1,
    };
    let process = timing_test_process(1);
    let capacity =
        u64::try_from(laminar_db::checkpoint_timing::CHECKPOINT_BARRIER_TIMING_CAPACITY).unwrap();
    let mut exported = timing_test_generation(capacity);
    let wrapped = timing_test_envelope(
        process,
        capacity,
        vec![timing_test_record(capacity + 1)],
        capacity + 2,
        false,
    );
    exported.apply_page(generation, wrapped).unwrap();
    assert_eq!(exported.cursor, capacity + 1);
    assert_eq!(exported.metadata.unwrap().overwritten_record_count, 1);

    let mut unread = CheckpointBarrierTimingGeneration::default();
    let unread_page = timing_test_envelope(
        process,
        0,
        (2..=65).map(timing_test_record).collect(),
        capacity + 2,
        true,
    );
    let error = unread.apply_page(generation, unread_page).unwrap_err();
    assert!(error.contains("lost unread timing records"), "{error}");
}

#[cfg(feature = "kafka")]
#[test]
fn exact_timing_collector_rejects_identity_gap_and_attempt_order_failures() {
    let generation = ProcessGeneration {
        node_id: 0,
        generation: 1,
    };
    let process = timing_test_process(1);

    let mut identity = timing_test_generation(1);
    let mut changed = timing_test_envelope(process, 1, Vec::new(), 2, false);
    changed.process_identity = timing_test_process(2);
    assert!(identity
        .apply_page(generation, changed)
        .unwrap_err()
        .contains("changed exact process identity"));

    let mut gap = timing_test_generation(1);
    let gap_page = timing_test_envelope(process, 1, vec![timing_test_record(3)], 4, false);
    assert!(gap
        .apply_page(generation, gap_page)
        .unwrap_err()
        .contains("sequence jumped"));

    let mut duplicate = timing_test_generation(1);
    let mut duplicate_record = timing_test_record(2);
    duplicate_record.attempt = CheckpointAttempt::canonical(1);
    let duplicate_page = timing_test_envelope(process, 1, vec![duplicate_record], 3, false);
    assert!(duplicate
        .apply_page(generation, duplicate_page)
        .unwrap_err()
        .contains("more than once"));

    let mut regression = timing_test_generation(2);
    let mut regressed_record = timing_test_record(3);
    regressed_record.attempt = CheckpointAttempt::canonical(1);
    let regression_page = timing_test_envelope(process, 2, vec![regressed_record], 4, false);
    assert!(regression
        .apply_page(generation, regression_page)
        .unwrap_err()
        .contains("attempt regressed"));

    let mut conflict = timing_test_generation(2);
    let mut conflicting_record = timing_test_record(3);
    conflicting_record.attempt = CheckpointAttempt::new(3, 1);
    let conflict_page = timing_test_envelope(process, 2, vec![conflicting_record], 4, false);
    assert!(conflict
        .apply_page(generation, conflict_page)
        .unwrap_err()
        .contains("attempt conflicted"));
}

#[cfg(feature = "kafka")]
#[test]
fn exact_timing_collector_rejects_loss_metadata_and_impossible_records() {
    let generation = ProcessGeneration {
        node_id: 0,
        generation: 1,
    };
    let process = timing_test_process(1);

    let mut lost_page = timing_test_envelope(process, 0, Vec::new(), 1, false);
    lost_page.page.recording_loss_count = 1;
    assert!(CheckpointBarrierTimingGeneration::default()
        .apply_page(generation, lost_page)
        .unwrap_err()
        .contains("lost=1"));

    let mut exhausted_page = timing_test_envelope(process, 0, Vec::new(), 1, false);
    exhausted_page.page.metadata_exhausted = true;
    assert!(CheckpointBarrierTimingGeneration::default()
        .apply_page(generation, exhausted_page)
        .unwrap_err()
        .contains("metadata_exhausted=true"));

    let mut bad_overwrite = timing_test_envelope(process, 0, Vec::new(), 1, false);
    bad_overwrite.page.overwritten_record_count = 1;
    assert!(CheckpointBarrierTimingGeneration::default()
        .apply_page(generation, bad_overwrite)
        .unwrap_err()
        .contains("overwrite count"));

    let mut impossible = timing_test_record(1);
    impossible.local_barrier_ns = impossible.pipeline_stall_ns + 1;
    let impossible_page = timing_test_envelope(process, 0, vec![impossible], 2, false);
    assert!(CheckpointBarrierTimingGeneration::default()
        .apply_page(generation, impossible_page)
        .unwrap_err()
        .contains("impossible stage durations"));

    let mut conflicting_digest = timing_test_generation(1);
    let mut second = timing_test_record(2);
    second.assignment_digest[0] ^= 0xff;
    let digest_page = timing_test_envelope(process, 1, vec![second], 3, false);
    assert!(conflicting_digest
        .apply_page(generation, digest_page)
        .unwrap_err()
        .contains("conflicting digests"));
}

#[cfg(feature = "kafka")]
#[test]
fn exact_timing_reconciliation_rejects_each_count_and_bucket_disagreement() {
    let generation = ProcessGeneration {
        node_id: 0,
        generation: 1,
    };
    let timing = timing_test_generation(2);
    let expected = timing_test_metrics(&timing);
    timing
        .validate_against_metrics(generation, expected)
        .unwrap();

    macro_rules! assert_mismatch {
        ($field:ident, $name:literal) => {{
            let mut mismatched = expected;
            mismatched.$field -= 1.0;
            let error = timing
                .validate_against_metrics(generation, mismatched)
                .unwrap_err();
            assert!(error.contains($name), "{error}");
            assert!(error.contains("delta=-1"), "{error}");
        }};
    }
    assert_mismatch!(pipeline_stall_observations, "pipeline-stall count");
    assert_mismatch!(barrier_local_observations, "local-barrier count");
    assert_mismatch!(aligned_resume_observations, "aligned-resume count");
    assert_mismatch!(pipeline_stall_within_slo, "pipeline-stall SLO bucket");
    assert_mismatch!(barrier_local_within_slo, "local-barrier SLO bucket");
    assert_mismatch!(aligned_resume_within_slo, "aligned-resume SLO bucket");
}

#[cfg(feature = "kafka")]
#[test]
fn exact_timing_reconciliation_requires_authority_binding_and_integer_metrics() {
    let generation = ProcessGeneration {
        node_id: 0,
        generation: 1,
    };
    let process = timing_test_process(1);
    let mut unbound = CheckpointBarrierTimingGeneration::default();
    unbound
        .apply_page(
            generation,
            timing_test_envelope(process, 0, Vec::new(), 1, false),
        )
        .unwrap();
    assert!(unbound
        .validate_against_metrics(generation, CheckpointLatencySnapshot::default())
        .unwrap_err()
        .contains("never bound"));
    assert!(unbound
        .bind_authority(generation, timing_test_process(2))
        .unwrap_err()
        .contains("does not match"));
    unbound.bind_authority(generation, process).unwrap();
    unbound
        .validate_against_metrics(generation, CheckpointLatencySnapshot::default())
        .unwrap();

    for invalid in [-1.0, 0.5, f64::NAN, 9_007_199_254_740_994.0, f64::INFINITY] {
        assert!(exact_prometheus_count(invalid, "test count").is_err());
    }
    assert_eq!(
        exact_prometheus_count(9_007_199_254_740_992.0, "test count").unwrap(),
        9_007_199_254_740_992
    );
}

#[cfg(feature = "kafka")]
#[test]
fn exact_timing_violation_diagnostic_is_exact_and_bounded() {
    let mut timing = timing_test_generation(2);
    for record in &mut timing.records {
        record.pipeline_stall_ns = CHECKPOINT_PIPELINE_STALL_SLO_NS + record.sequence;
        record.local_barrier_ns = CHECKPOINT_PIPELINE_STALL_SLO_NS + record.sequence;
        record.aligned_resume_ns = None;
        record.deadline_exhausted = true;
    }
    let diagnostic = timing.violation_summary(1);
    assert!(diagnostic.contains("sequence=1"), "{diagnostic}");
    assert!(diagnostic.contains("attempt=1/1"), "{diagnostic}");
    assert!(diagnostic.contains("assignment_version=4"), "{diagnostic}");
    assert!(
        diagnostic.contains("3 additional violations"),
        "{diagnostic}"
    );
}

#[cfg(feature = "kafka")]
#[test]
fn exact_timing_completeness_covers_every_spawned_generation() {
    let nodes = [
        Node {
            id: 0,
            config_path: PathBuf::new(),
            log_path: PathBuf::new(),
            child: None,
            process_generation: 2,
            http_port: 0,
            fault_trigger_path: None,
            checkpoint_gate_path: None,
        },
        Node {
            id: 1,
            config_path: PathBuf::new(),
            log_path: PathBuf::new(),
            child: None,
            process_generation: 1,
            http_port: 0,
            fault_trigger_path: None,
            checkpoint_gate_path: None,
        },
    ];
    let expected = [
        ProcessGeneration {
            node_id: 0,
            generation: 1,
        },
        ProcessGeneration {
            node_id: 0,
            generation: 2,
        },
        ProcessGeneration {
            node_id: 1,
            generation: 1,
        },
    ];
    let mut timing = CheckpointBarrierTimingEvidence::default();
    let mut latency = CheckpointLatencyEvidence::default();
    for generation in expected.iter().copied() {
        timing.generations.insert(
            generation,
            CheckpointBarrierTimingGeneration {
                finalized: true,
                ..CheckpointBarrierTimingGeneration::default()
            },
        );
        latency
            .generations
            .insert(generation, CheckpointLatencySnapshot::default());
    }
    timing.validate_observed_cuts(&latency, &nodes).unwrap();
    timing.generations.remove(&expected[0]);
    latency.generations.remove(&expected[0]);
    let error = timing.validate_observed_cuts(&latency, &nodes).unwrap_err();
    assert!(
        error.contains("every spawned process generation"),
        "{error}"
    );
}

#[cfg(feature = "kafka")]
fn test_checkpoint_latency_snapshot(
    checkpoint_observations: f64,
    pipeline_stall_observations: f64,
    pipeline_stall_within_slo: f64,
) -> CheckpointLatencySnapshot {
    CheckpointLatencySnapshot {
        gate_wait_seconds: 0.25,
        gate_wait_observations: 1.0,
        checkpoint_seconds: checkpoint_observations * 0.1,
        checkpoint_observations,
        pipeline_stall_observations,
        pipeline_stall_within_slo,
        barrier_local_seconds: pipeline_stall_observations * 0.025,
        barrier_local_observations: pipeline_stall_observations,
        barrier_local_within_slo: pipeline_stall_observations,
        aligned_resume_seconds: pipeline_stall_observations * 0.05,
        aligned_resume_observations: pipeline_stall_observations,
        aligned_resume_within_slo: pipeline_stall_observations,
    }
}

#[cfg(feature = "kafka")]
#[test]
fn checkpoint_latency_snapshot_rejects_malformed_histograms() {
    let mut non_finite = test_checkpoint_latency_snapshot(1.0, 1.0, 1.0);
    non_finite.pipeline_stall_observations = f64::NAN;
    assert!(non_finite.validate().unwrap_err().contains("finite"));

    let mut negative = test_checkpoint_latency_snapshot(1.0, 1.0, 1.0);
    negative.pipeline_stall_within_slo = -1.0;
    assert!(negative.validate().unwrap_err().contains("non-negative"));

    let impossible_bucket = test_checkpoint_latency_snapshot(1.0, 10.0, 11.0);
    assert!(impossible_bucket
        .validate()
        .unwrap_err()
        .contains("exceeds histogram count"));

    let mut impossible_local_bucket = test_checkpoint_latency_snapshot(1.0, 10.0, 10.0);
    impossible_local_bucket.barrier_local_within_slo = 11.0;
    assert!(impossible_local_bucket
        .validate()
        .unwrap_err()
        .contains("local-barrier SLO bucket"));

    let mut impossible_resume_bucket = test_checkpoint_latency_snapshot(1.0, 10.0, 10.0);
    impossible_resume_bucket.aligned_resume_within_slo = 11.0;
    assert!(impossible_resume_bucket
        .validate()
        .unwrap_err()
        .contains("aligned-resume SLO bucket"));
}

#[cfg(feature = "kafka")]
#[test]
fn checkpoint_latency_node_gate_prevents_cluster_dilution() {
    let mut evidence = CheckpointLatencyEvidence::default();
    evidence
        .record_generation(
            ProcessGeneration {
                node_id: 0,
                generation: 1,
            },
            test_checkpoint_latency_snapshot(60.0, 60.0, 59.0),
        )
        .unwrap();
    evidence
        .record_generation(
            ProcessGeneration {
                node_id: 0,
                generation: 2,
            },
            test_checkpoint_latency_snapshot(60.0, 60.0, 59.0),
        )
        .unwrap();
    evidence
        .record_generation(
            ProcessGeneration {
                node_id: 1,
                generation: 1,
            },
            test_checkpoint_latency_snapshot(200.0, 200.0, 200.0),
        )
        .unwrap();

    let aggregate = evidence.aggregate().unwrap();
    assert!(aggregate.pipeline_stall_within_slo_percent().unwrap() > 99.0);
    let error = evidence.validate_slos().unwrap_err();
    assert!(error.contains("node0 across process generations"));
    assert!(error.contains("98.33%"));

    let mut missing_generation = CheckpointLatencyEvidence::default();
    missing_generation
        .record_generation(
            ProcessGeneration {
                node_id: 0,
                generation: 1,
            },
            test_checkpoint_latency_snapshot(100.0, 100.0, 100.0),
        )
        .unwrap();
    missing_generation
        .record_generation(
            ProcessGeneration {
                node_id: 1,
                generation: 1,
            },
            test_checkpoint_latency_snapshot(0.0, 0.0, 0.0),
        )
        .unwrap();
    let error = missing_generation.validate_slos().unwrap_err();
    assert!(error.contains("node1 process generation 1"), "{error}");
    assert!(
        error.contains("no checkpoint pipeline-stall observations"),
        "{error}"
    );

    let mut healthy_follower = CheckpointLatencyEvidence::default();
    healthy_follower
        .record_generation(
            ProcessGeneration {
                node_id: 0,
                generation: 1,
            },
            test_checkpoint_latency_snapshot(100.0, 100.0, 100.0),
        )
        .unwrap();
    healthy_follower
        .record_generation(
            ProcessGeneration {
                node_id: 1,
                generation: 1,
            },
            test_checkpoint_latency_snapshot(0.0, 100.0, 100.0),
        )
        .unwrap();
    assert!(healthy_follower.validate_slos().is_ok());
}

#[cfg(feature = "kafka")]
#[test]
fn checkpoint_latency_slo_window_survives_process_restart() {
    let mut evidence = CheckpointLatencyEvidence::default();
    evidence
        .record_generation(
            ProcessGeneration {
                node_id: 0,
                generation: 1,
            },
            test_checkpoint_latency_snapshot(61.0, 61.0, 60.0),
        )
        .unwrap();
    evidence
        .record_generation(
            ProcessGeneration {
                node_id: 0,
                generation: 2,
            },
            test_checkpoint_latency_snapshot(61.0, 61.0, 61.0),
        )
        .unwrap();

    let nodes = evidence.aggregate_by_node().unwrap();
    let node = nodes.get(&0).unwrap();
    assert_eq!(node.pipeline_stall_observations, 122.0);
    assert_eq!(node.pipeline_stall_within_slo, 121.0);
    assert!(evidence.validate_slos().is_ok());
}

#[cfg(feature = "kafka")]
#[test]
fn checkpoint_latency_aggregation_preserves_restart_generations_once() {
    let mut evidence = CheckpointLatencyEvidence::default();
    let first = ProcessGeneration {
        node_id: 0,
        generation: 1,
    };
    evidence
        .record_generation(first, test_checkpoint_latency_snapshot(10.0, 10.0, 10.0))
        .unwrap();
    evidence
        .record_generation(
            ProcessGeneration {
                node_id: 0,
                generation: 2,
            },
            test_checkpoint_latency_snapshot(20.0, 20.0, 20.0),
        )
        .unwrap();

    let duplicate = evidence
        .record_generation(first, test_checkpoint_latency_snapshot(99.0, 99.0, 99.0))
        .unwrap_err();
    assert!(duplicate.contains("captured more than once"));
    let aggregate = evidence.aggregate().unwrap();
    assert_eq!(aggregate.checkpoint_observations, 30.0);
    assert_eq!(aggregate.pipeline_stall_observations, 30.0);
    assert_eq!(aggregate.pipeline_stall_within_slo, 30.0);
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

#[test]
fn local_exact_source_cursor_oracle_reads_filesystem_checkpoint_layout() {
    let directory = tempfile::tempdir().unwrap();
    let checkpoint = DurableCheckpointStatus {
        checkpoint_id: 7,
        epoch: 7,
    };
    let mut manifest = laminar_core::checkpoint::CheckpointManifest::new(7, 7);
    manifest.durable_phase =
        laminar_core::checkpoint::checkpoint_manifest::DurableCheckpointPhase::Finalized;
    manifest.source_offsets.insert(
        "gen".into(),
        laminar_core::checkpoint::ConnectorCheckpoint::with_offsets(
            std::collections::HashMap::from([("seq".into(), "4096".into())]),
        ),
    );
    let path = directory
        .path()
        .join("checkpoints/checkpoint_000007/manifest.json");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();

    assert_eq!(
        local_exact_checkpoint_source_sequence(directory.path(), checkpoint).unwrap(),
        4096
    );
}

#[test]
fn exact_prefix_snapshot_accepts_complete_unordered_state() {
    let rows = serde_json::json!([
        { "k": 2, "n": 4, "hi": 11 },
        { "k": 0, "n": 4, "hi": 7 },
        { "k": 1, "n": 4, "hi": 9 }
    ]);
    let observed = validate_local_exact_snapshot(rows.as_array().expect("snapshot rows"), 12, 3, 2);
    assert_eq!(
        observed,
        BTreeMap::from([(0, (4, 7)), (1, (4, 9)), (2, (4, 11))])
    );
}
