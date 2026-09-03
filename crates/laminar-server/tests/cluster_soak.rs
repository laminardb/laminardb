// The real-binary soak fixture keeps its process-control scaffolding explicit.
#![allow(
    clippy::assertions_on_constants,
    clippy::disallowed_types,
    clippy::too_many_arguments
)] // Fault-soak setup keeps explicit process inputs and platform-gated assertions.

//! Real-binary checkpoint soaks with `kill -9` fault injection.
//!
//! Spawns three `laminardb` processes in cluster mode (real gRPC control plane) against a shared
//! checkpoint store, runs bounded interval and temporal ASOF joins under skew, and repeatedly
//! hard-kills the leader and a follower mid-epoch. A small filtered probe crosses all eight bounded
//! join kinds with ordered composite keys and a named join-to-keyed-aggregate stage in the same
//! checkpoint graph. After every fault it asserts the survivors keep committing, observed committed
//! epochs strictly advance, and the restarted node rejoins and resumes. The Kafka sink legs are
//! explicitly at-least-once: their oracles reject impossible output and loss while counting
//! duplicates. The exact legs use checkpoint-replayable Kafka inputs and coordinated Delta append;
//! an independent snapshot oracle requires the main frozen output exactly once. All four join legs
//! phase the matrix across faults and require connector-visible keyed aggregate results that combine
//! pre-fault retained state with post-fault input. That phased source also closes direct TUMBLE,
//! HOP, and SESSION state after recovery. The local exact leg separately validates finite-source
//! and retained-state recovery.
//!
//! Ignored by default — spawns processes and runs for minutes:
//!
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features --features cluster,aws,kafka \
//!   --test cluster_soak three_node_alo_join_kill9_soak -- --ignored --nocapture
//! cargo test --profile soak -p laminar-server --no-default-features --features cluster,kafka \
//!   --test cluster_soak single_node_alo_join_kill9_soak -- --ignored --nocapture
//! cargo test --profile soak -p laminar-server --no-default-features --features cluster,aws,kafka,delta-lake-s3 \
//!   --test cluster_soak three_node_eo_join_kill9_soak -- --ignored --nocapture
//! cargo test --profile soak -p laminar-server --no-default-features --features cluster,kafka,delta-lake-s3 \
//!   --test cluster_soak single_node_eo_join_kill9_soak -- --ignored --nocapture
//! cargo test --profile soak -p laminar-server --no-default-features --features cluster \
//!   --test cluster_soak local_exact_source_state_kill9_soak -- --ignored --nocapture
//! ```
//!
//! Environment knobs:
//! - `LAMINAR_SOAK_SECONDS`      steady-soak duration after fault rounds (default 90)
//! - `LAMINAR_SOAK_INTERVAL_MS`  checkpoint cadence (default 500; minimum 100)
//! - `LAMINAR_SOAK_CHECKPOINT_SLO_MODE`  `certify` (default) enforces the checkpoint latency
//!   sample-size and percentile SLOs; `observe` retains exact timing evidence and diagnostics for
//!   functional smoke runs without claiming performance certification
//! - `LAMINAR_SOAK_EO_VISIBILITY_SLO_MODE`  `certify` (default) enforces frozen-input-to-Delta
//!   visibility; `observe` records the immutable boundary but still requires bounded exact output
//! - `LAMINAR_SOAK_KILLS`  total fault rounds (local exact requires at least two)
//! - `LAMINAR_SOAK_CHECKPOINT_URL`  required cluster-shared checkpoint prefix
//! - `LAMINAR_SOAK_S3_ENDPOINT` / `_ACCESS_KEY` / `_SECRET_KEY` / `_REGION`  checkpoint storage
//! - `LAMINAR_SOAK_DELTA_BUCKET`  existing bucket for unique EO output tables
//! - `LAMINAR_SOAK_ALLOW_S3_EMULATOR=1`  debug/soak-only MinIO protocol validation; this does not
//!   certify an emulator or custom endpoint for production; EO emulator runs use a bounded 60s
//!   checkpoint-operation timeout while the real-store soak profile retains 30s
//! - `LAMINAR_SOAK_ALO_VISIBILITY_MS`  maximum Kafka output visibility latency (default 10000)
//! - `LAMINAR_SOAK_EO_VISIBILITY_MS`  maximum frozen-input-to-Delta visibility latency (default 10000)
//! - `LAMINAR_SOAK_KAFKA_SOURCE_BROKERS`  required shared Kafka/Redpanda source broker
//! - `LAMINAR_SOAK_KAFKA_PARTITIONS`  source topic partition count (default 96)
//! - `LAMINAR_SOAK_RPS`  source production rate (1..=1000 for exact temporal timestamps)
//! - `LAMINAR_SOAK_JOIN_INTERVAL_MS`  retained join horizon (default 100)
//! - `LAMINAR_SOAK_JOIN_KEYS` / `LAMINAR_SOAK_ZIPF_MILLI`  key count and Zipf exponent × 1000
//! - `LAMINAR_SOAK_MIN_LIVE_STATE_BYTES`  optional retained-state high-water gate
//! - `LAMINAR_SOAK_HOT_P50_MS` / `_P95_MS` / `_P99_MS`  hot-cycle latency gates
//! - `LAMINAR_SOAK_HOT_SLO_MODE`  `certify` (default) enforces hot-cycle latency limits;
//!   `observe` records the same evidence without treating shared-runner timing as certification
//! - `LAMINAR_SOAK_HOT_MIN_CYCLES`  minimum latency sample count
//! - `LAMINAR_SOAK_KEY_GROUPS`  stable cluster key-group count (default 64)
//! - `LAMINAR_SOAK_FAULT_INJECT_ROLE`  trigger one fatal cycle fault after steady state on the
//!   observed `leader` or a `follower`
//! - `LAMINAR_SOAK_MAX_RECOVERY_MS`  maximum time for each failover or restarted-node recovery
//!   phase (default and upper bound 90s, matching the liveness window)
//! - `LAMINAR_SOAK_LAMINARDB_EXE` / `LAMINAR_SOAK_LAMINARDB_SHA256`  optional all-or-nothing
//!   absolute prebuilt server path and exact lowercase SHA-256; path/digest validation precedes
//!   dependency creation, while executable-format/permission errors remain OS spawn failures

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::io::{Read, Write as _};
#[cfg(feature = "kafka")]
use std::io::{Seek as _, SeekFrom};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
#[cfg(feature = "kafka")]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(feature = "kafka")]
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
use arrow_array::{Array as _, Int64Array, TimestampMillisecondArray};
#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
use arrow_schema::{DataType, TimeUnit};
use sha2::{Digest as _, Sha256};

#[cfg(feature = "kafka")]
use laminar_core::checkpoint::{CheckpointAttempt, CheckpointAttemptRelation};
#[cfg(feature = "kafka")]
use laminar_core::cluster::control::{
    AssignmentSnapshot, CheckpointAssignmentAdoption, CheckpointAssignmentFence,
    CheckpointParticipant, LocalProcessAuthorityEvidence, LocalProcessAuthorityIdentity,
};

const NODES: usize = 3;
/// Per-node ports: http = BASE + i, gossip = BASE + 100 + i.
const BASE_PORT: u16 = 19310;
const SOAK_CONSOLE_TOKEN: &str = "laminardb-cluster-soak";
#[cfg(feature = "kafka")]
const SOAK_HTTP_HEADER_MAX_BYTES: usize = 16 * 1_024;
#[cfg(feature = "kafka")]
const READINESS_DIAGNOSTIC_MAX_BYTES: usize = 4 * 1_024;
#[cfg(feature = "kafka")]
const LOCAL_AUTHORITY_EVIDENCE_MAX_BYTES: usize = 4 * 1_024;
#[cfg(feature = "kafka")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_MAX_BYTES: usize = 64 * 1_024;
#[cfg(feature = "kafka")]
const CHECKPOINT_BARRIER_TIMING_DIAGNOSTIC_WITNESSES: usize = 8;
#[cfg(feature = "kafka")]
const CHECKPOINT_BARRIER_TIMING_MAX_PROCESS_GENERATIONS: usize = 1_024;
#[cfg(feature = "kafka")]
const CHECKPOINT_BARRIER_TIMING_MAX_ASSIGNMENTS_PER_PROCESS: usize = 1_024;
#[cfg(feature = "kafka")]
const ASSIGNMENT_SNAPSHOT_MAX_BYTES: usize = 4 * 1_024 * 1_024;
#[cfg(feature = "kafka")]
const LOCAL_AUTHORITY_EVIDENCE_SCHEMA: &str = "laminardb-local-authority-evidence/v1";
#[cfg(feature = "kafka")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_SCHEMA: &str =
    "laminardb-local-checkpoint-barrier-timings/v1";
#[cfg(feature = "kafka")]
const CHECKPOINT_BARRIER_TIMING_ARTIFACT_SCHEMA: &str =
    "laminardb-soak-checkpoint-barrier-timing/v1";
#[cfg(feature = "kafka")]
const DEFAULT_CLUSTER_KEY_GROUPS: u32 = 64;
#[cfg(feature = "kafka")]
const DEFAULT_KAFKA_PARTITIONS: u64 = 96;
#[cfg(feature = "kafka")]
const OUTPUT_TOPIC_PARTITIONS: i32 = 1;
#[cfg(feature = "kafka")]
// WHY: Hosted runners can pause one of the 96 continuously active Kafka partitions for longer
// than five seconds. Keep that partition in the watermark minimum while quiet canaries still
// become idle well within the 90-second recovery window.
const SOAK_SOURCE_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(feature = "kafka")]
// This latency-certification profile deliberately favors prompt delivery over producer-side
// request aggregation. The Kafka connector's production default remains 5 ms.
const SOAK_KAFKA_SINK_LINGER_MS: u64 = 0;
#[cfg(feature = "kafka")]
const SOAK_PRODUCER_MAX_IN_FLIGHT: usize = 4_096;
#[cfg(feature = "kafka")]
const MAX_TEMPORAL_LOAD_RPS: u64 = 1_000;
#[cfg(feature = "kafka")]
// WHY: A checkpoint fence can delay a bounded publication burst at either sample boundary. A
// 30-second window prevents an ordinary one-second fence from consuming most of the 10% capacity
// allowance without masking sustained under-capacity.
const ACTIVE_LOAD_SAMPLE_WINDOW: Duration = Duration::from_secs(30);
#[cfg(feature = "kafka")]
const ACTIVE_LOAD_MINIMUM_RATIO: f64 = 0.9;
#[cfg(feature = "kafka")]
const CHECKPOINT_PIPELINE_STALL_SLO_SECONDS: f64 = 1.024;
#[cfg(feature = "kafka")]
const CHECKPOINT_PIPELINE_STALL_SLO_NS: u64 = 1_024_000_000;
#[cfg(feature = "kafka")]
const MIN_CHECKPOINT_PIPELINE_STALL_OBSERVATIONS: u64 = 100;
#[cfg(feature = "kafka")]
const SOAK_CHECKPOINT_SLO_MODE_ENV: &str = "LAMINAR_SOAK_CHECKPOINT_SLO_MODE";
#[cfg(feature = "kafka")]
const SOAK_HOT_SLO_MODE_ENV: &str = "LAMINAR_SOAK_HOT_SLO_MODE";
#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
const SOAK_EO_VISIBILITY_SLO_MODE_ENV: &str = "LAMINAR_SOAK_EO_VISIBILITY_SLO_MODE";
#[cfg(feature = "kafka")]
const CHECKPOINT_STATE_CAPTURE_SLO_SECONDS: f64 = 0.064;
#[cfg(feature = "kafka")]
const MIN_CHECKPOINT_STATE_CAPTURE_OBSERVATIONS: u64 = 100;
#[cfg(feature = "kafka")]
const CHECKPOINT_OBSERVATION_COLLECTION_CAP: Duration = Duration::from_secs(20 * 60);
const SINGLE_CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(feature = "kafka")]
const CLUSTER_CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(30);
// WHY: the shared-runner MinIO profile drives 23 Delta sinks plus vnode output segments through
// one emulator. Give that non-certifying profile one bounded slow-storage allowance without
// changing the production-store timeout or the recovery liveness ceiling.
#[cfg(feature = "kafka")]
const CLUSTER_EMULATOR_EO_CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(60);
#[cfg(feature = "kafka")]
const MIN_CONTINUOUS_AGGREGATE_STATE_BYTES: u64 = 64 * 1_024;
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
#[cfg(feature = "kafka")]
const RECOVERY_SUPERSEDED_LOG: &str = "recovery round was superseded:";
#[cfg(feature = "kafka")]
const RECOVERY_PREPARE_HANDOFF_LOG: &str =
    "retrying stopped recovery with a direct Prepare-to-Prepare handoff";
#[cfg(feature = "kafka")]
const RECOVERY_RETRY_HOLD_LOG: &str = "holding intake shut and requesting a fresh recovery round";
#[cfg(feature = "kafka")]
const RECOVERY_DIAGNOSTIC_LOG_TAIL_MAX_BYTES: u64 = 4 * 1024 * 1024;
#[cfg(feature = "kafka")]
const RECOVERY_DIAGNOSTIC_MARKERS: [(&str, &str); 15] = [
    (
        "leader_local_deadline_expired",
        "leader lease local deadline expired",
    ),
    (
        "leader_operation_deadline_exceeded",
        "leader lease operation exceeded its local deadline",
    ),
    (
        "leader_response_after_deadline",
        "leader lease response arrived after its local deadline",
    ),
    (
        "leader_publication_after_deadline",
        "leader lease publication crossed its local deadline",
    ),
    ("leader_operation_failed", "leader lease operation failed"),
    ("leader_renewal_fenced", "leader lease renewal was fenced"),
    (
        "missing_live_leader_proof",
        "coordinated recovery has no live durable leader proof",
    ),
    (
        "fault_inventory_read_failed",
        "could not read cluster recovery fault reports",
    ),
    (
        "local_fault_read_failed",
        "could not read the local recovery fault report",
    ),
    (
        "decision_store_unreadable",
        "coordinated recovery: decision store unreadable",
    ),
    (
        "assignment_unavailable",
        "coordinated recovery: no current owner-complete assignment certificate",
    ),
    (
        "assignment_audit_failed",
        "coordinated recovery: owner-complete assignment audit failed",
    ),
    ("recovery_prepare", RECOVERY_PREPARE_LOG),
    ("recovery_start", "leader announced recovery start"),
    ("recovery_release", RECOVERY_RELEASE_LOG),
];
const RECOVERY_LIVENESS_WINDOW: Duration = Duration::from_secs(90);
const DEFAULT_MAX_RECOVERY_MS: u64 = 90_000;
const LOCAL_EXACT_PREFIX_CYCLES: u64 = 4;
const HARD_KILL_TIMEOUT: Duration = Duration::from_secs(10);
const SOAK_LAMINARDB_EXE_ENV: &str = "LAMINAR_SOAK_LAMINARDB_EXE";
const SOAK_LAMINARDB_SHA256_ENV: &str = "LAMINAR_SOAK_LAMINARDB_SHA256";
#[cfg(feature = "kafka")]
const OUTPUT_BOUNDARY_STABILITY: Duration = Duration::from_secs(3);
#[cfg(feature = "kafka")]
const MATRIX_INPUT_PARTITIONS: i32 = 1;
#[cfg(feature = "kafka")]
const MUTABLE_INTERVAL_INPUT_PARTITIONS: i32 = 1;
#[cfg(feature = "kafka")]
const RECOVERY_CANARY_EVENT_LEAD_MS: u64 = 45 * 60 * 1_000;
#[cfg(feature = "kafka")]
const SOAK_EVENT_MAX_FUTURE_SKEW_MS: u64 = 60 * 60 * 1_000;
#[cfg(feature = "kafka")]
const DEFAULT_JOIN_INTERVAL_MS: u64 = 100;
#[cfg(feature = "kafka")]
const DEFAULT_JOIN_KEYS: u64 = 4_096;
#[cfg(feature = "kafka")]
const DEFAULT_ZIPF_MILLI: u64 = 1_200;
#[cfg(feature = "kafka")]
const DEFAULT_HOT_PATH_P50_MS: u64 = 5;
#[cfg(feature = "kafka")]
const DEFAULT_HOT_PATH_P95_MS: u64 = 10;
#[cfg(feature = "kafka")]
const DEFAULT_HOT_PATH_P99_MS: u64 = 50;
#[cfg(feature = "kafka")]
const HOT_PATH_TOP_OPERATOR_PROFILES: usize = 8;
#[cfg(feature = "kafka")]
const DEFAULT_HOT_PATH_MIN_CYCLES: u64 = 100;
#[cfg(feature = "kafka")]
const MAX_EXPECTED_JOIN_PAIRS: usize = 10_000_000;
#[cfg(feature = "kafka")]
const SINGLE_JOIN_PORT: u16 = 19_410;
#[cfg(feature = "kafka")]
const TEMPORAL_RECOVERY_LEFT_ID: u64 = 8_000_000_000_000_001;
#[cfg(feature = "kafka")]
const TEMPORAL_CREATE_RIGHT_ID: u64 = 8_000_000_000_000_002;
#[cfg(feature = "kafka")]
const TEMPORAL_UPDATE_RIGHT_ID: u64 = 8_000_000_000_000_003;
#[cfg(feature = "kafka")]
const TEMPORAL_REVIVAL_RIGHT_ID: u64 = 8_000_000_000_000_004;
#[cfg(feature = "kafka")]
const TEMPORAL_REVIVAL_LEFT_ID: u64 = 8_000_000_000_000_005;
#[cfg(feature = "kafka")]
const TEMPORAL_SENTINEL_ID_BASE: u64 = 8_000_000_000_001_000;
#[cfg(feature = "kafka")]
const TEMPORAL_WATERMARK_ADVANCE_MS: u64 = 5_001;
#[cfg(feature = "kafka")]
const TEMPORAL_PROBE_OFFSETS_MS: [i64; 5] = [-4_000, -3_000, -2_000, -1_000, 1_000];
#[cfg(feature = "kafka")]
const MUTABLE_INTERVAL_LEFT_ID: u64 = 8_000_000_000_010_001;
#[cfg(feature = "kafka")]
const MUTABLE_INTERVAL_RIGHT_ID: u64 = 8_000_000_000_010_002;
#[cfg(feature = "kafka")]
const MUTABLE_INTERVAL_JOIN_KEY: i64 = 8_000_000_000_010_003;
#[cfg(feature = "kafka")]
const MUTABLE_INTERVAL_INITIAL_VALUE: i64 = 10;
#[cfg(feature = "kafka")]
const MUTABLE_INTERVAL_UPDATED_VALUE: i64 = 20;
#[cfg(feature = "kafka")]
const MUTABLE_INTERVAL_REVIVED_VALUE: i64 = 30;
#[cfg(feature = "kafka")]
const DEFAULT_ALO_VISIBILITY_MS: u64 = 10_000;
#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
const DEFAULT_EO_VISIBILITY_MS: u64 = 10_000;

#[cfg(feature = "kafka")]
#[derive(Clone, Copy)]
struct BoundedJoinSoakCase {
    name: &'static str,
    keyword: &'static str,
    projection: &'static str,
    filter: &'static str,
}

#[cfg(feature = "kafka")]
const BOUNDED_JOIN_SOAK_CASES: [BoundedJoinSoakCase; 8] = [
    BoundedJoinSoakCase {
        name: "inner",
        keyword: "JOIN",
        projection: "l.id AS left_id, r.id AS right_id",
        filter: "l.join_key < 0 OR r.join_key_2 < 0",
    },
    BoundedJoinSoakCase {
        name: "left",
        keyword: "LEFT JOIN",
        projection: "l.id AS left_id, r.id AS right_id",
        filter: "l.join_key < 0 OR r.join_key_2 < 0",
    },
    BoundedJoinSoakCase {
        name: "right",
        keyword: "RIGHT JOIN",
        projection: "l.id AS left_id, r.id AS right_id",
        filter: "l.join_key < 0 OR r.join_key_2 < 0",
    },
    BoundedJoinSoakCase {
        name: "full",
        keyword: "FULL JOIN",
        projection: "l.id AS left_id, r.id AS right_id",
        filter: "l.join_key < 0 OR r.join_key_2 < 0",
    },
    BoundedJoinSoakCase {
        name: "left_semi",
        keyword: "LEFT SEMI JOIN",
        projection: "l.id AS left_id, CAST(NULL AS BIGINT) AS right_id",
        filter: "l.join_key < 0",
    },
    BoundedJoinSoakCase {
        name: "left_anti",
        keyword: "LEFT ANTI JOIN",
        projection: "l.id AS left_id, CAST(NULL AS BIGINT) AS right_id",
        filter: "l.join_key < 0",
    },
    BoundedJoinSoakCase {
        name: "right_semi",
        keyword: "RIGHT SEMI JOIN",
        projection: "CAST(NULL AS BIGINT) AS left_id, r.id AS right_id",
        filter: "r.join_key_2 < 0",
    },
    BoundedJoinSoakCase {
        name: "right_anti",
        keyword: "RIGHT ANTI JOIN",
        projection: "CAST(NULL AS BIGINT) AS left_id, r.id AS right_id",
        filter: "r.join_key_2 < 0",
    },
];

#[cfg(feature = "kafka")]
const MATRIX_OUTPUT_PIPELINES: [&str; 8] = [
    "soak_matrix_inner",
    "soak_matrix_left",
    "soak_matrix_right",
    "soak_matrix_full",
    "soak_matrix_left_semi",
    "soak_matrix_left_anti",
    "soak_matrix_right_semi",
    "soak_matrix_right_anti",
];

#[cfg(feature = "kafka")]
#[derive(Clone, Copy)]
struct CoreWindowCanary {
    kind: &'static str,
    pipeline: &'static str,
    expression: &'static str,
    emit: &'static str,
}

#[cfg(feature = "kafka")]
const CORE_WINDOW_CANARIES: [CoreWindowCanary; 3] = [
    CoreWindowCanary {
        kind: "tumble",
        pipeline: "soak_window_tumble",
        expression: "TUMBLE(event_time, INTERVAL '1' SECOND)",
        emit: "EMIT ON WINDOW CLOSE",
    },
    CoreWindowCanary {
        kind: "hop",
        pipeline: "soak_window_hop",
        expression: "HOP(event_time, INTERVAL '500' MILLISECOND, INTERVAL '1' SECOND)",
        emit: "EMIT FINAL",
    },
    CoreWindowCanary {
        kind: "session",
        pipeline: "soak_window_session",
        expression: "SESSION(event_time, INTERVAL '1' SECOND)",
        emit: "EMIT ON WINDOW CLOSE",
    },
];

#[cfg(feature = "kafka")]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MatrixOutput {
    join_case: String,
    left_id: Option<i64>,
    right_id: Option<i64>,
}

#[cfg(feature = "kafka")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MatrixAggregateOutput {
    row_count: u64,
    left_count: u64,
    right_count: u64,
    left_sum: Option<i64>,
    right_sum: Option<i64>,
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
type DeltaAggregateRows = Vec<(String, MatrixAggregateOutput, i64)>;

#[cfg(feature = "kafka")]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CoreWindowOutput {
    kind: String,
    window_start_ms: i64,
    row_count: u64,
    id_sum: i64,
}

#[cfg(feature = "kafka")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MutableIntervalAggregateOutput {
    match_count: u64,
    value_sum: i64,
}

#[cfg(feature = "kafka")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MutableIntervalSinkState {
    Live(MutableIntervalAggregateOutput),
    Tombstone,
}

#[cfg(feature = "kafka")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TemporalOutputKind {
    LeftAsof,
    InnerProbe,
}

#[cfg(feature = "kafka")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum TemporalOutput {
    LeftAsof {
        left_id: u64,
        right_id: Option<u64>,
    },
    InnerProbe {
        left_id: u64,
        right_id: u64,
        offset_ms: i64,
        probe_time_ms: i64,
    },
}

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

#[cfg(feature = "kafka")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SloMode {
    Certify,
    Observe,
}

#[cfg(feature = "kafka")]
impl SloMode {
    fn parse(environment: &str, value: Option<&str>) -> Result<Self, String> {
        match value {
            None | Some("certify") => Ok(Self::Certify),
            Some("observe") => Ok(Self::Observe),
            Some(value) => Err(format!(
                "{environment} must be 'certify' or 'observe', got {value:?}"
            )),
        }
    }

    fn from_environment(environment: &str) -> Result<Self, String> {
        match std::env::var(environment) {
            Ok(value) => Self::parse(environment, Some(&value)),
            Err(std::env::VarError::NotPresent) => Self::parse(environment, None),
            Err(std::env::VarError::NotUnicode(value)) => {
                Err(format!("{environment} is not valid Unicode: {value:?}"))
            }
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Certify => "certify",
            Self::Observe => "observe",
        }
    }

    const fn observation_floor(self, certification_floor: u64) -> u64 {
        match self {
            Self::Certify => certification_floor,
            Self::Observe => 1,
        }
    }
}

#[cfg(feature = "kafka")]
fn resolve_visibility_slo_mode(delivery: JoinDelivery) -> SloMode {
    match delivery {
        JoinDelivery::AtLeastOnce => SloMode::Certify,
        JoinDelivery::ExactlyOnce => {
            #[cfg(feature = "delta-lake-s3")]
            {
                SloMode::from_environment(SOAK_EO_VISIBILITY_SLO_MODE_ENV)
                    .unwrap_or_else(|error| panic!("invalid EO visibility SLO mode: {error}"))
            }
            #[cfg(not(feature = "delta-lake-s3"))]
            {
                SloMode::Certify
            }
        }
    }
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn enforce_or_report_visibility_slo(
    mode: SloMode,
    label: &str,
    visibility: Duration,
    visibility_slo: Duration,
) {
    if visibility <= visibility_slo {
        return;
    }
    match mode {
        SloMode::Certify => {
            panic!("{label} visibility {visibility:?} exceeded {visibility_slo:?}")
        }
        SloMode::Observe => eprintln!(
            "soak: PROFILE {label} visibility {visibility:?} exceeded the non-certifying {visibility_slo:?} SLO boundary"
        ),
    }
}

#[cfg(feature = "kafka")]
fn required_live_checkpoint_observations(
    mode: SloMode,
    certification_floor: u64,
    finalized: u64,
) -> u64 {
    mode.observation_floor(certification_floor)
        .saturating_sub(finalized)
        .max(1)
}

#[cfg(feature = "kafka")]
fn join_interval_ms() -> u64 {
    let interval_ms = env_u64("LAMINAR_SOAK_JOIN_INTERVAL_MS", DEFAULT_JOIN_INTERVAL_MS);
    assert!(
        interval_ms > 0,
        "LAMINAR_SOAK_JOIN_INTERVAL_MS must be greater than zero"
    );
    interval_ms
}

fn soak_run_id() -> String {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before the Unix epoch")
        .as_nanos();
    format!("{}-{nonce}", std::process::id())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExecutableOrigin {
    CargoBuilt,
    Override,
}

impl ExecutableOrigin {
    const fn label(self) -> &'static str {
        match self {
            Self::CargoBuilt => "cargo-built",
            Self::Override => "prebuilt-override",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResolvedExecutable {
    path: PathBuf,
    sha256: [u8; 32],
    byte_len: u64,
    origin: ExecutableOrigin,
}

impl ResolvedExecutable {
    fn from_environment() -> Result<Self, String> {
        resolve_laminardb_executable(
            std::env::var_os(SOAK_LAMINARDB_EXE_ENV),
            std::env::var_os(SOAK_LAMINARDB_SHA256_ENV),
            Path::new(env!("CARGO_BIN_EXE_laminardb")),
        )
    }

    fn describe(&self) {
        eprintln!(
            "soak: laminardb executable origin={} path={} sha256={} bytes={}",
            self.origin.label(),
            self.path.display(),
            encode_sha256(self.sha256),
            self.byte_len
        );
    }

    fn verify_unchanged(&self) -> Result<(), String> {
        let current = resolve_executable(&self.path, Some(self.sha256), self.origin)?;
        if current.byte_len != self.byte_len {
            return Err(format!(
                "executable length changed for {}: expected {}, got {}",
                self.path.display(),
                self.byte_len,
                current.byte_len
            ));
        }
        Ok(())
    }
}

fn resolve_laminardb_executable(
    configured_path: Option<OsString>,
    configured_sha256: Option<OsString>,
    cargo_fallback: &Path,
) -> Result<ResolvedExecutable, String> {
    match (configured_path, configured_sha256) {
        (None, None) => resolve_executable(cargo_fallback, None, ExecutableOrigin::CargoBuilt),
        (Some(path), Some(sha256)) => {
            if path.is_empty() {
                return Err(format!("{SOAK_LAMINARDB_EXE_ENV} must not be empty"));
            }
            let path = PathBuf::from(path);
            if !path.is_absolute() {
                return Err(format!(
                    "{SOAK_LAMINARDB_EXE_ENV} must be an absolute path, got {}",
                    path.display()
                ));
            }
            let expected_sha256 = parse_sha256(&sha256)?;
            resolve_executable(&path, Some(expected_sha256), ExecutableOrigin::Override)
        }
        (Some(_), None) => Err(format!(
            "{SOAK_LAMINARDB_EXE_ENV} requires {SOAK_LAMINARDB_SHA256_ENV}"
        )),
        (None, Some(_)) => Err(format!(
            "{SOAK_LAMINARDB_SHA256_ENV} requires {SOAK_LAMINARDB_EXE_ENV}"
        )),
    }
}

fn resolve_executable(
    path: &Path,
    expected_sha256: Option<[u8; 32]>,
    origin: ExecutableOrigin,
) -> Result<ResolvedExecutable, String> {
    let canonical_path = path
        .canonicalize()
        .map_err(|error| format!("cannot canonicalize executable {}: {error}", path.display()))?;
    let path_metadata = canonical_path.metadata().map_err(|error| {
        format!(
            "cannot inspect executable path {}: {error}",
            canonical_path.display()
        )
    })?;
    if !path_metadata.is_file() {
        return Err(format!(
            "executable path is not a regular file: {}",
            canonical_path.display()
        ));
    }
    let mut file = std::fs::File::open(&canonical_path).map_err(|error| {
        format!(
            "cannot open executable {}: {error}",
            canonical_path.display()
        )
    })?;
    let metadata = file.metadata().map_err(|error| {
        format!(
            "cannot inspect executable {}: {error}",
            canonical_path.display()
        )
    })?;
    if !metadata.is_file() {
        return Err(format!(
            "executable path is not a regular file: {}",
            canonical_path.display()
        ));
    }
    let actual_sha256 = sha256_reader(&mut file).map_err(|error| {
        format!(
            "cannot hash executable {}: {error}",
            canonical_path.display()
        )
    })?;
    if let Some(expected_sha256) = expected_sha256 {
        if actual_sha256 != expected_sha256 {
            return Err(format!(
                "executable SHA-256 mismatch for {}: expected {}, got {}",
                canonical_path.display(),
                encode_sha256(expected_sha256),
                encode_sha256(actual_sha256)
            ));
        }
    }
    Ok(ResolvedExecutable {
        path: canonical_path,
        sha256: actual_sha256,
        byte_len: metadata.len(),
        origin,
    })
}

fn sha256_reader(reader: &mut impl Read) -> std::io::Result<[u8; 32]> {
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1_024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

fn parse_sha256(value: &OsStr) -> Result<[u8; 32], String> {
    let value = value.to_str().ok_or_else(|| {
        format!("{SOAK_LAMINARDB_SHA256_ENV} must be valid Unicode lowercase hex")
    })?;
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "{SOAK_LAMINARDB_SHA256_ENV} must contain exactly 64 lowercase hexadecimal characters"
        ));
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        decoded[index] = (lower_hex_nibble(pair[0]) << 4) | lower_hex_nibble(pair[1]);
    }
    Ok(decoded)
}

const fn lower_hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!(),
    }
}

fn encode_sha256(value: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in value {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
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

fn validate_checkpoint_liveness(
    interval_ms: u64,
    checkpoint_timeout: Duration,
    recovery: Duration,
) {
    assert!(
        checkpoint_timeout < recovery,
        "checkpoint timeout {checkpoint_timeout:?} leaves no bounded recovery interval within {recovery:?}"
    );
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
    executable: Arc<ResolvedExecutable>,
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

struct VerifiedExecutable(Arc<ResolvedExecutable>);

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
#[derive(Debug, PartialEq, Eq)]
enum ReadinessDiagnostic {
    Ready,
    Starting,
    ServingFenced,
    ProcessLeaseExpired,
    Recovering,
    PipelineNotRunning,
    TemporarilyUnavailable,
    UnexpectedStatus(u16),
    TransportUnavailable,
    InvalidResponse,
}

#[cfg(feature = "kafka")]
fn classify_readiness_response(status: u16, body: &[u8]) -> ReadinessDiagnostic {
    if status == 200 {
        return ReadinessDiagnostic::Ready;
    }
    if status != 503 {
        return ReadinessDiagnostic::UnexpectedStatus(status);
    }
    let body = String::from_utf8_lossy(body);
    for (message, diagnostic) in [
        (
            "server startup is not complete",
            ReadinessDiagnostic::Starting,
        ),
        (
            "server serving authority is fenced",
            ReadinessDiagnostic::ServingFenced,
        ),
        (
            "server process lease is no longer live",
            ReadinessDiagnostic::ProcessLeaseExpired,
        ),
        (
            "server is completing coordinated recovery",
            ReadinessDiagnostic::Recovering,
        ),
    ] {
        if body.contains(message) {
            return diagnostic;
        }
    }
    if body.contains("pipeline is ") && body.contains(", not Running") {
        return ReadinessDiagnostic::PipelineNotRunning;
    }
    ReadinessDiagnostic::TemporarilyUnavailable
}

#[cfg(feature = "kafka")]
fn is_retryable_diagnostic_status(status: u16) -> bool {
    // 429 is the bounded single-flight/rate limit, 503 is unavailable evidence, and 504 is the
    // diagnostic handler deadline. None contradicts the evidence being sampled.
    matches!(status, 429 | 503 | 504)
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckpointBarrierTimingRole {
    Leader,
    Follower,
}

#[cfg(feature = "kafka")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
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
#[derive(Debug, Clone, Copy)]
struct CheckpointBarrierTimingAuthority {
    process: LocalProcessAuthorityIdentity,
    assignment_version: u64,
    assignment_certificate_digest: [u8; 32],
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
        && (line.contains("Recovered from unified checkpoint")
            || line.contains("recovered committed checkpoint"))
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
                        left.0.checkpoint_id, left.0.epoch, right.0.checkpoint_id, right.0.epoch
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

#[cfg(feature = "kafka")]
#[derive(Clone, Copy, Debug)]
struct HotPathLatencySnapshot {
    observations: u64,
    sum_seconds: Option<f64>,
    p50_upper_seconds: f64,
    p95_upper_seconds: f64,
    p99_upper_seconds: f64,
}

#[cfg(feature = "kafka")]
impl HotPathLatencySnapshot {
    fn profile(self) -> String {
        let total_and_average = self.sum_seconds.map_or_else(
            || "total/avg=unavailable".into(),
            |sum_seconds| {
                format!(
                    "total={sum_seconds:.3}s avg={:.3}ms",
                    sum_seconds / self.observations as f64 * 1_000.0,
                )
            },
        );
        format!(
            "{total_and_average} p50<={:.3}ms p95<={:.3}ms p99<={:.3}ms over {} obs",
            self.p50_upper_seconds * 1_000.0,
            self.p95_upper_seconds * 1_000.0,
            self.p99_upper_seconds * 1_000.0,
            self.observations,
        )
    }
}

#[cfg(feature = "kafka")]
fn prometheus_metric_labels<'a>(line: &'a str, metric: &str) -> Option<&'a str> {
    let rest = line.strip_prefix(metric)?;
    if rest.starts_with(' ') || rest.starts_with('\t') {
        return Some("");
    }
    let rest = rest.strip_prefix('{')?;
    rest.split_once('}').map(|(labels, _)| labels)
}

#[cfg(feature = "kafka")]
fn prometheus_label_value(labels: &str, wanted: &str) -> Option<String> {
    let mut remaining = labels.trim();
    while !remaining.is_empty() {
        let equals = remaining.find('=')?;
        let name = remaining[..equals].trim();
        let encoded = remaining[equals + 1..].strip_prefix('"')?;
        let mut value = String::new();
        let mut escaped = false;
        let mut closing_quote = None;
        for (offset, ch) in encoded.char_indices() {
            if escaped {
                value.push(match ch {
                    'n' => '\n',
                    '\\' => '\\',
                    '"' => '"',
                    other => other,
                });
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                closing_quote = Some(offset);
                break;
            } else {
                value.push(ch);
            }
        }
        let closing_quote = closing_quote?;
        if name == wanted {
            return Some(value);
        }
        remaining = encoded[closing_quote + 1..].trim_start();
        if remaining.is_empty() {
            break;
        }
        remaining = remaining.strip_prefix(',')?.trim_start();
    }
    None
}

#[cfg(feature = "kafka")]
fn prometheus_metric_has_labels(
    line: &str,
    metric: &str,
    required_labels: &[(&str, &str)],
) -> bool {
    let Some(labels) = prometheus_metric_labels(line, metric) else {
        return false;
    };
    required_labels
        .iter()
        .all(|(name, value)| prometheus_label_value(labels, name).as_deref() == Some(*value))
}

#[cfg(feature = "kafka")]
fn prometheus_histogram_latency_for_labels(
    body: &str,
    metric: &str,
    required_labels: &[(&str, &str)],
) -> Result<HotPathLatencySnapshot, String> {
    let count_metric = metric
        .strip_suffix("_bucket")
        .ok_or_else(|| format!("histogram metric {metric:?} does not end in _bucket"))?;
    let count_metric = format!("{count_metric}_count");
    let matching_count_series = body
        .lines()
        .filter(|line| prometheus_metric_has_labels(line, &count_metric, required_labels))
        .count();
    let observations = body
        .lines()
        .filter(|line| prometheus_metric_has_labels(line, &count_metric, required_labels))
        .map(|line| {
            let value = line
                .split_whitespace()
                .last()
                .ok_or_else(|| format!("metric {count_metric} has no value"))?
                .parse::<f64>()
                .map_err(|error| format!("metric {count_metric} has an invalid value: {error}"))?;
            exact_prometheus_count(value, &count_metric)
        })
        .try_fold(0_u64, |sum, value| {
            sum.checked_add(value?)
                .ok_or_else(|| format!("metric {count_metric} observation count overflowed u64"))
        })?;
    if observations == 0 {
        return Err(format!("histogram {metric} has no observations"));
    }

    let sum_metric = metric
        .strip_suffix("_bucket")
        .expect("histogram suffix was checked above");
    let sum_metric = format!("{sum_metric}_sum");
    let sums = body
        .lines()
        .filter(|line| prometheus_metric_has_labels(line, &sum_metric, required_labels))
        .map(|line| line.split_whitespace().last()?.parse::<f64>().ok())
        .collect::<Option<Vec<_>>>();
    // `_sum` is diagnostic only. Keep the established count/bucket certification semantics if a
    // registry proxy omits or mangles sums, while still reporting totals and averages normally.
    let sum_seconds = sums.and_then(|sums| {
        (sums.len() == matching_count_series && matching_count_series != 0)
            .then(|| sums.into_iter().sum::<f64>())
            .filter(|sum| sum.is_finite() && *sum >= 0.0)
    });

    let mut buckets = Vec::<(f64, u64)>::new();
    let mut infinite_count = 0_u64;
    for line in body
        .lines()
        .filter(|line| prometheus_metric_has_labels(line, metric, required_labels))
    {
        let rest = line
            .strip_prefix(metric)
            .expect("metric prefix was filtered above");
        let (labels, _) = rest
            .strip_prefix('{')
            .and_then(|rest| rest.split_once('}'))
            .ok_or_else(|| format!("histogram bucket has malformed labels: {line}"))?;
        let bound = prometheus_label_value(labels, "le")
            .ok_or_else(|| format!("histogram bucket omitted le: {line}"))?;
        let value = line
            .split_whitespace()
            .last()
            .ok_or_else(|| format!("histogram bucket has no value: {line}"))?
            .parse::<f64>()
            .map_err(|error| format!("histogram bucket has an invalid value: {error}"))?;
        let value = exact_prometheus_count(value, metric)?;
        if bound == "+Inf" {
            infinite_count = infinite_count
                .checked_add(value)
                .ok_or_else(|| format!("histogram {metric} +Inf bucket count overflowed u64"))?;
        } else {
            let bound = bound
                .parse::<f64>()
                .map_err(|error| format!("histogram bucket has invalid le={bound:?}: {error}"))?;
            if !bound.is_finite() {
                return Err(format!(
                    "histogram bucket has non-finite le={bound:?}: {line}"
                ));
            }
            buckets.push((bound, value));
        }
    }
    if infinite_count != observations {
        return Err(format!(
            "histogram {metric} +Inf bucket {infinite_count:?} disagrees with count {observations}"
        ));
    }
    buckets.sort_by(|left, right| left.0.total_cmp(&right.0));
    let mut aggregated = Vec::<(f64, u64)>::with_capacity(buckets.len());
    for (bound, value) in buckets {
        let duplicate = aggregated
            .last()
            .is_some_and(|(previous_bound, _)| *previous_bound == bound);
        if duplicate {
            let (_, previous_value) = aggregated
                .last_mut()
                .expect("duplicate histogram bucket has a predecessor");
            *previous_value = previous_value.checked_add(value).ok_or_else(|| {
                format!("histogram {metric} le={bound} bucket count overflowed u64")
            })?;
        } else {
            aggregated.push((bound, value));
        }
    }
    let quantile = |numerator: u64| {
        let rank = observations
            .checked_mul(numerator)
            .expect("histogram quantile rank overflow")
            .div_ceil(100);
        aggregated
            .iter()
            .find_map(|(bound, cumulative)| (*cumulative >= rank).then_some(*bound))
            .ok_or_else(|| {
                format!("histogram {metric} p{numerator} lies above its largest finite bucket")
            })
    };
    Ok(HotPathLatencySnapshot {
        observations,
        sum_seconds,
        p50_upper_seconds: quantile(50)?,
        p95_upper_seconds: quantile(95)?,
        p99_upper_seconds: quantile(99)?,
    })
}

#[cfg(feature = "kafka")]
fn prometheus_histogram_latency(
    body: &str,
    metric: &str,
) -> Result<HotPathLatencySnapshot, String> {
    prometheus_histogram_latency_for_labels(body, metric, &[])
}

#[cfg(feature = "kafka")]
#[derive(Debug)]
struct NamedHotPathLatency {
    name: String,
    latency: Result<HotPathLatencySnapshot, String>,
}

#[cfg(feature = "kafka")]
#[derive(Debug)]
struct HotPathLatencyEvidence {
    cycle: HotPathLatencySnapshot,
    phases: Vec<NamedHotPathLatency>,
    operators: Vec<NamedHotPathLatency>,
    operator_errors: Vec<String>,
}

#[cfg(feature = "kafka")]
fn prometheus_histogram_label_pairs(
    body: &str,
    metric: &str,
    first: &str,
    second: &str,
) -> BTreeSet<(String, String)> {
    let count_metric = format!(
        "{}_count",
        metric
            .strip_suffix("_bucket")
            .expect("histogram metric ends in _bucket")
    );
    body.lines()
        .filter_map(|line| {
            let labels = prometheus_metric_labels(line, &count_metric)?;
            Some((
                prometheus_label_value(labels, first)?,
                prometheus_label_value(labels, second)?,
            ))
        })
        .collect()
}

#[cfg(feature = "kafka")]
impl HotPathLatencyEvidence {
    fn from_metrics(body: &str) -> Result<Self, String> {
        const OPERATOR_METRIC: &str = "laminardb_operator_process_duration_seconds_bucket";
        let cycle = prometheus_histogram_latency(body, "laminardb_cycle_duration_seconds_bucket")?;
        let phases = [
            ("execute", "laminardb_cycle_execute_duration_seconds_bucket"),
            (
                "output_store",
                "laminardb_cycle_output_store_duration_seconds_bucket",
            ),
            (
                "sink_enqueue",
                "laminardb_cycle_sink_enqueue_duration_seconds_bucket",
            ),
        ]
        .into_iter()
        .map(|(name, metric)| NamedHotPathLatency {
            name: name.into(),
            latency: prometheus_histogram_latency(body, metric),
        })
        .collect();

        let mut operators = Vec::new();
        let mut operator_errors = Vec::new();
        for (operator, mode) in
            prometheus_histogram_label_pairs(body, OPERATOR_METRIC, "operator", "mode")
        {
            if mode != "normal" {
                continue;
            }
            let required_labels = [("operator", operator.as_str()), ("mode", mode.as_str())];
            match prometheus_histogram_latency_for_labels(body, OPERATOR_METRIC, &required_labels) {
                Ok(latency) => operators.push(NamedHotPathLatency {
                    name: format!("{operator} mode={mode}"),
                    latency: Ok(latency),
                }),
                Err(error) => operator_errors.push(format!("{operator} mode={mode}: {error}")),
            }
        }
        operators.sort_by(|left, right| {
            let left_total = left
                .latency
                .as_ref()
                .ok()
                .and_then(|latency| latency.sum_seconds)
                .unwrap_or_default();
            let right_total = right
                .latency
                .as_ref()
                .ok()
                .and_then(|latency| latency.sum_seconds)
                .unwrap_or_default();
            right_total
                .total_cmp(&left_total)
                .then_with(|| left.name.cmp(&right.name))
        });
        operators.truncate(HOT_PATH_TOP_OPERATOR_PROFILES);
        if operators.is_empty() && operator_errors.is_empty() {
            operator_errors.push("no normal-mode operator histogram series".into());
        }

        Ok(Self {
            cycle,
            phases,
            operators,
            operator_errors,
        })
    }

    fn report(&self, node_id: usize, label: &str) {
        let p50_ms = self.cycle.p50_upper_seconds * 1_000.0;
        let p95_ms = self.cycle.p95_upper_seconds * 1_000.0;
        let p99_ms = self.cycle.p99_upper_seconds * 1_000.0;
        eprintln!(
            "soak: HOT PATH {label} node{node_id} p50<={p50_ms:.3}ms p95<={p95_ms:.3}ms p99<={p99_ms:.3}ms over {} cycles",
            self.cycle.observations
        );
        for phase in &self.phases {
            match &phase.latency {
                Ok(latency) => eprintln!(
                    "soak: HOT PHASE {label} node{node_id} {} {}",
                    phase.name,
                    latency.profile(),
                ),
                Err(error) => eprintln!(
                    "soak: HOT PHASE {label} node{node_id} {} unavailable: {error}",
                    phase.name,
                ),
            }
        }
        for operator in &self.operators {
            let latency = operator
                .latency
                .as_ref()
                .expect("only successful operator profiles are retained");
            eprintln!(
                "soak: HOT OPERATOR {label} node{node_id} {} {}",
                operator.name,
                latency.profile(),
            );
        }
        for error in &self.operator_errors {
            eprintln!("soak: HOT OPERATOR {label} node{node_id} unavailable: {error}");
        }
    }

    fn failure_diagnostic(&self) -> String {
        let phases = self
            .phases
            .iter()
            .map(|phase| match &phase.latency {
                Ok(latency) => format!("{} {}", phase.name, latency.profile()),
                Err(error) => format!("{} unavailable ({error})", phase.name),
            })
            .collect::<Vec<_>>()
            .join("; ");
        let mut operators = self
            .operators
            .iter()
            .map(|operator| {
                format!(
                    "{} {}",
                    operator.name,
                    operator
                        .latency
                        .as_ref()
                        .expect("only successful operator profiles are retained")
                        .profile()
                )
            })
            .collect::<Vec<_>>();
        operators.extend(
            self.operator_errors
                .iter()
                .map(|error| format!("unavailable ({error})")),
        );
        format!(
            "cycle {}; phases [{phases}]; top normal-mode operators [{}]",
            self.cycle.profile(),
            operators.join("; "),
        )
    }
}

impl Node {
    /// Hash before the caller starts a recovery timer; consuming the permit in `spawn` makes every
    /// process generation reuse the resolved executable identity without charging hash I/O to RTO.
    fn verify_executable_for_spawn(&self) -> VerifiedExecutable {
        self.executable.verify_unchanged().unwrap_or_else(|error| {
            panic!(
                "refusing to spawn node{} from a changed soak executable: {error}",
                self.id
            )
        });
        VerifiedExecutable(Arc::clone(&self.executable))
    }

    fn spawn(&mut self, verified: VerifiedExecutable) {
        assert!(
            Arc::ptr_eq(&self.executable, &verified.0),
            "node{} spawn permit belongs to a different executable",
            self.id
        );
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .expect("node log file");
        let mut cmd = Command::new(&verified.0.path);
        cmd.arg("--config")
            .arg(&self.config_path)
            .env(
                "RUST_LOG",
                "laminardb=info,laminar_server=info,laminar_db=info,laminar_core=info",
            )
            .env("NO_COLOR", "1")
            .env_remove(SOAK_LAMINARDB_EXE_ENV)
            .env_remove(SOAK_LAMINARDB_SHA256_ENV)
            .stdout(Stdio::from(log.try_clone().expect("clone log handle")))
            .stderr(Stdio::from(log));
        if std::env::var("LAMINAR_SOAK_ALLOW_S3_EMULATOR").as_deref() == Ok("1") {
            cmd.env("LAMINAR_SOAK_ALLOW_S3_EMULATOR", "1");
        } else {
            cmd.env_remove("LAMINAR_SOAK_ALLOW_S3_EMULATOR");
        }
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
        let child = cmd.spawn().unwrap_or_else(|error| {
            panic!(
                "spawn laminardb from {} (sha256={}): {error}",
                verified.0.path.display(),
                encode_sha256(verified.0.sha256)
            )
        });
        let pid = child.id();
        self.child = Some(child);
        #[cfg(feature = "kafka")]
        {
            self.process_generation = self
                .process_generation
                .checked_add(1)
                .expect("soak process generation overflow");
            eprintln!(
                "soak: node{} generation={} pid={pid} executable_sha256={}",
                self.id,
                self.process_generation,
                encode_sha256(verified.0.sha256)
            );
        }
        #[cfg(not(feature = "kafka"))]
        eprintln!(
            "soak: node{} pid={pid} executable_sha256={}",
            self.id,
            encode_sha256(verified.0.sha256)
        );
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
    fn readiness_diagnostic(&self) -> ReadinessDiagnostic {
        let deadline = Instant::now() + Duration::from_secs(2);
        match self.bounded_http_get("/ready", READINESS_DIAGNOSTIC_MAX_BYTES, deadline) {
            Ok(response) => classify_readiness_response(response.status, &response.body),
            Err(BoundedHttpError::Unavailable(_)) => ReadinessDiagnostic::TransportUnavailable,
            Err(BoundedHttpError::Invalid(_)) => ReadinessDiagnostic::InvalidResponse,
        }
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
        if is_retryable_diagnostic_status(response.status) {
            return LocalAuthorityObservation::Pending(format!(
                "node{} local evidence is temporarily unavailable (HTTP {})",
                self.id, response.status
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
        if is_retryable_diagnostic_status(response.status) {
            return CheckpointBarrierTimingObservation::Pending(format!(
                "node{} local checkpoint barrier timings are temporarily unavailable (HTTP {})",
                self.id, response.status
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
    fn metric_with_labels(&self, name: &str, labels: &[&str]) -> Option<f64> {
        let body = self.http_get("/metrics")?;
        let mut found = false;
        let sum = body
            .lines()
            .filter(|line| {
                line.strip_prefix(name).is_some_and(|rest| {
                    rest.starts_with('{') && labels.iter().all(|label| rest.contains(label))
                })
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
            checkpoint_seconds: value("laminardb_checkpoint_duration_seconds_sum")?,
            checkpoint_observations: value("laminardb_checkpoint_duration_seconds_count")?,
            state_capture_seconds: value(
                "laminardb_checkpoint_state_capture_duration_seconds_sum",
            )?,
            state_capture_observations: value(
                "laminardb_checkpoint_state_capture_duration_seconds_count",
            )?,
            state_capture_within_slo: prometheus_histogram_bucket_value(
                &body,
                "laminardb_checkpoint_state_capture_duration_seconds_bucket",
                CHECKPOINT_STATE_CAPTURE_SLO_SECONDS,
            )?,
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
    fn hot_path_latency_evidence(&self) -> Result<HotPathLatencyEvidence, String> {
        let body = self
            .http_get("/metrics")
            .ok_or_else(|| format!("node{} did not serve /metrics", self.id))?;
        HotPathLatencyEvidence::from_metrics(&body)
            .map_err(|error| format!("node{} hot-path histogram: {error}", self.id))
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

    fn checkpoint_kill_predecessor(&self, role: &str) -> Option<DurableCheckpointStatus> {
        let path = self.checkpoint_gate_path.as_ref()?;
        let ready = std::fs::read_to_string(path.with_extension("ready")).ok()?;
        let mut fields = ready.split_ascii_whitespace();
        if fields.next()? != role {
            return None;
        }
        let armed = DurableCheckpointStatus {
            checkpoint_id: fields.next()?.parse().ok()?,
            epoch: fields.next()?.parse().ok()?,
        };
        let predecessor = DurableCheckpointStatus {
            checkpoint_id: fields.next()?.parse().ok()?,
            epoch: fields.next()?.parse().ok()?,
        };
        if fields.next().is_some()
            || !armed.is_canonical()
            || !predecessor.is_canonical()
            || predecessor.checkpoint_id >= armed.checkpoint_id
        {
            return None;
        }
        Some(predecessor)
    }

    fn checkpoint_kill_ready(&self, role: &str) -> bool {
        self.checkpoint_kill_predecessor(role).is_some()
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
    fn recovery_log_marker_counts(&self) -> Option<BTreeMap<&'static str, usize>> {
        let mut log = std::fs::File::open(&self.log_path).ok()?;
        let log_len = log.metadata().ok()?.len();
        let start = log_len.saturating_sub(RECOVERY_DIAGNOSTIC_LOG_TAIL_MAX_BYTES);
        log.seek(SeekFrom::Start(start)).ok()?;
        let mut tail = Vec::new();
        log.take(RECOVERY_DIAGNOSTIC_LOG_TAIL_MAX_BYTES)
            .read_to_end(&mut tail)
            .ok()?;
        Some(count_recovery_diagnostic_markers(&String::from_utf8_lossy(
            &tail,
        )))
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct JoinInput {
    id: u64,
    key: u64,
    event_time_ms: u64,
}

#[cfg(feature = "kafka")]
#[derive(Clone, Copy)]
struct TemporalSoakRecord {
    partition: i32,
    id: u64,
    key: i64,
    temporal_time_ms: u64,
}

#[cfg(feature = "kafka")]
struct TemporalDebeziumMutation {
    before: Option<TemporalSoakRecord>,
    after: Option<TemporalSoakRecord>,
    operation: &'static str,
}

#[cfg(feature = "kafka")]
#[derive(Clone, Copy)]
struct MutableIntervalRightRow {
    id: u64,
    join_key: i64,
    event_time_ms: u64,
    value: i64,
}

#[cfg(feature = "kafka")]
struct MutableIntervalMutation {
    before: Option<MutableIntervalRightRow>,
    after: Option<MutableIntervalRightRow>,
    operation: &'static str,
}

#[cfg(feature = "kafka")]
struct MutableIntervalRecoverySeed {
    right: MutableIntervalRightRow,
    input_boundary: Vec<i64>,
}

#[cfg(feature = "kafka")]
struct TemporalRecoverySeed {
    key: i64,
    base_time_ms: u64,
    input_boundary: Vec<i64>,
}

#[cfg(feature = "kafka")]
struct TemporalPostFaultInput {
    revival_input_boundary: Vec<i64>,
    expected_asof: BTreeSet<TemporalOutput>,
    expected_probe: BTreeSet<TemporalOutput>,
}

#[cfg(feature = "kafka")]
struct ProducedPrefix {
    count: u64,
    end_offsets: Vec<i64>,
    expected_pairs: BTreeSet<(u64, u64)>,
    expected_temporal_pairs: BTreeSet<(u64, u64)>,
    elapsed: Duration,
    broker_acked_at: Instant,
}

#[cfg(feature = "kafka")]
struct ZipfSampler {
    cumulative: Vec<f64>,
}

#[cfg(feature = "kafka")]
impl ZipfSampler {
    fn new(key_count: u64, exponent_milli: u64) -> Self {
        assert!(
            key_count > 0,
            "LAMINAR_SOAK_JOIN_KEYS must be greater than zero"
        );
        assert!(
            exponent_milli <= 3_000,
            "LAMINAR_SOAK_ZIPF_MILLI must be in 0..=3000"
        );
        let key_count = usize::try_from(key_count)
            .expect("LAMINAR_SOAK_JOIN_KEYS must fit in the host address space");
        let exponent = exponent_milli as f64 / 1_000.0;
        let mut cumulative = Vec::with_capacity(key_count);
        let mut total = 0.0;
        for rank in 1..=key_count {
            total += (rank as f64).powf(-exponent);
            cumulative.push(total);
        }
        for edge in &mut cumulative {
            *edge /= total;
        }
        Self { cumulative }
    }

    fn sample(&self, id: u64) -> u64 {
        let random = splitmix64(id ^ 0x6a09_e667_f3bc_c909);
        let unit = (random >> 11) as f64 / ((1_u64 << 53) as f64);
        let index = self
            .cumulative
            .partition_point(|edge| *edge <= unit)
            .min(self.cumulative.len() - 1);
        u64::try_from(index).expect("Zipf key index fits u64")
    }
}

#[cfg(feature = "kafka")]
const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(feature = "kafka")]
fn expected_join_pairs(inputs: &[JoinInput]) -> BTreeSet<(u64, u64)> {
    expected_join_pairs_for_interval(inputs, join_interval_ms())
}

#[cfg(feature = "kafka")]
fn expected_temporal_pairs(produced_count: u64) -> BTreeSet<(u64, u64)> {
    (0..produced_count).map(|id| (id, id)).collect()
}

/// Advance the continuous temporal-load clock at least as quickly as wall-clock pacing while
/// preserving a distinct millisecond timestamp for every generated row. Without the paced term,
/// the default 400-row/s profile advances event time by only 400ms/s, so a five-minute temporal
/// retention window accumulates 12.5 minutes of input before its first history eviction.
#[cfg(feature = "kafka")]
fn temporal_load_offset_ms(sequence: u64, rps: u64) -> u64 {
    assert!(
        (1..=MAX_TEMPORAL_LOAD_RPS).contains(&rps),
        "temporal-load RPS must be between 1 and {MAX_TEMPORAL_LOAD_RPS}"
    );
    sequence
        .checked_mul(1_000)
        .expect("temporal-load timestamp numerator overflow")
        / rps
}

#[cfg(feature = "kafka")]
fn expected_join_pairs_for_interval(
    inputs: &[JoinInput],
    join_interval_ms: u64,
) -> BTreeSet<(u64, u64)> {
    let mut by_key = BTreeMap::<u64, Vec<JoinInput>>::new();
    for input in inputs {
        by_key.entry(input.key).or_default().push(*input);
    }

    let mut expected = BTreeSet::new();
    for records in by_key.values() {
        for left in records {
            let start = left.event_time_ms;
            let end = left
                .event_time_ms
                .checked_add(join_interval_ms)
                .expect("join interval timestamp overflow");
            let first = records.partition_point(|right| right.event_time_ms < start);
            let after_last = records.partition_point(|right| right.event_time_ms <= end);
            for right in &records[first..after_last] {
                expected.insert((left.id, right.id));
                assert!(
                    expected.len() <= MAX_EXPECTED_JOIN_PAIRS,
                    "join oracle exceeded {MAX_EXPECTED_JOIN_PAIRS} expected pairs; reduce LAMINAR_SOAK_RPS, LAMINAR_SOAK_SECONDS, LAMINAR_SOAK_ZIPF_MILLI, or the fault rounds"
                );
            }
        }
    }
    expected
}

#[cfg(feature = "kafka")]
fn expected_matrix_outputs() -> BTreeSet<MatrixOutput> {
    [
        ("inner", Some(-101), Some(-201)),
        ("inner", Some(-103), Some(-203)),
        ("inner", Some(-104), Some(-204)),
        ("inner", Some(-105), Some(-205)),
        ("left", Some(-101), Some(-201)),
        ("left", Some(-103), Some(-203)),
        ("left", Some(-104), Some(-204)),
        ("left", Some(-105), Some(-205)),
        ("left", Some(-102), None),
        ("right", Some(-101), Some(-201)),
        ("right", Some(-103), Some(-203)),
        ("right", Some(-104), Some(-204)),
        ("right", Some(-105), Some(-205)),
        ("right", None, Some(-202)),
        ("full", Some(-101), Some(-201)),
        ("full", Some(-103), Some(-203)),
        ("full", Some(-104), Some(-204)),
        ("full", Some(-105), Some(-205)),
        ("full", Some(-102), None),
        ("full", None, Some(-202)),
        ("left_semi", Some(-101), None),
        ("left_semi", Some(-103), None),
        ("left_semi", Some(-104), None),
        ("left_semi", Some(-105), None),
        ("left_anti", Some(-102), None),
        ("right_semi", None, Some(-201)),
        ("right_semi", None, Some(-203)),
        ("right_semi", None, Some(-204)),
        ("right_semi", None, Some(-205)),
        ("right_anti", None, Some(-202)),
    ]
    .into_iter()
    .map(|(join_case, left_id, right_id)| MatrixOutput {
        join_case: join_case.to_owned(),
        left_id,
        right_id,
    })
    .collect()
}

#[cfg(feature = "kafka")]
fn expected_matrix_aggregates() -> BTreeMap<String, MatrixAggregateOutput> {
    let mut expected = BTreeMap::new();
    for row in expected_matrix_outputs() {
        let aggregate = expected
            .entry(row.join_case)
            .or_insert(MatrixAggregateOutput {
                row_count: 0,
                left_count: 0,
                right_count: 0,
                left_sum: None,
                right_sum: None,
            });
        aggregate.row_count += 1;
        if let Some(left_id) = row.left_id {
            aggregate.left_count += 1;
            aggregate.left_sum = Some(aggregate.left_sum.unwrap_or(0) + left_id);
        }
        if let Some(right_id) = row.right_id {
            aggregate.right_count += 1;
            aggregate.right_sum = Some(aggregate.right_sum.unwrap_or(0) + right_id);
        }
    }
    expected
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
    converged_assignment_digests: BTreeMap<u64, [u8; 32]>,
    record_count: u64,
    aligned_resume_count: u64,
    pipeline_stall_within_slo: u64,
    barrier_local_within_slo: u64,
    aligned_resume_within_slo: u64,
    leader_count: u64,
    follower_count: u64,
    no_handoff_count: u64,
    deadline_exhausted_count: u64,
    pipeline_stall_max: Option<CheckpointBarrierTimingRecord>,
    barrier_local_max: Option<CheckpointBarrierTimingRecord>,
    aligned_resume_max: Option<CheckpointBarrierTimingRecord>,
    violation_count: u64,
    violation_witnesses: Vec<CheckpointBarrierTimingViolation>,
    artifact: Option<CheckpointBarrierTimingArtifact>,
    metadata: Option<CheckpointBarrierTimingMetadata>,
    finalized: bool,
}

#[cfg(feature = "kafka")]
#[derive(Debug, Clone, Copy)]
struct CheckpointBarrierTimingViolation {
    stage: &'static str,
    record: CheckpointBarrierTimingRecord,
    duration_ns: u64,
}

#[cfg(feature = "kafka")]
struct CheckpointBarrierTimingArtifact {
    path: PathBuf,
    writer: Option<std::io::BufWriter<std::fs::File>>,
}

#[cfg(feature = "kafka")]
#[derive(serde::Serialize)]
struct CheckpointBarrierTimingArtifactLine<'a> {
    schema_version: &'static str,
    node_ordinal: usize,
    process_generation: u64,
    record: &'a CheckpointBarrierTimingRecord,
}

#[cfg(feature = "kafka")]
impl CheckpointBarrierTimingGeneration {
    fn with_artifact(directory: &Path, generation: ProcessGeneration) -> Result<Self, String> {
        let path = directory.join(format!(
            "checkpoint-timing-node{}-generation{}.jsonl.log",
            generation.node_id, generation.generation
        ));
        let file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|error| {
                format!(
                    "could not create checkpoint timing artifact {}: {error}",
                    path.display()
                )
            })?;
        Ok(Self {
            artifact: Some(CheckpointBarrierTimingArtifact {
                path,
                writer: Some(std::io::BufWriter::new(file)),
            }),
            ..Self::default()
        })
    }

    fn bind_authority(
        &mut self,
        generation: ProcessGeneration,
        authority: CheckpointBarrierTimingAuthority,
    ) -> Result<(), String> {
        let expected = authority.process;
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
        if authority.assignment_version == 0 || authority.assignment_certificate_digest == [0; 32] {
            return Err(format!(
                "node{} process generation {} received non-canonical converged assignment-certificate evidence",
                generation.node_id, generation.generation
            ));
        }
        if self
            .converged_assignment_digests
            .get(&authority.assignment_version)
            .is_some_and(|digest| *digest != authority.assignment_certificate_digest)
        {
            return Err(format!(
                "node{} process generation {} converged assignment version {} mapped to conflicting digests",
                generation.node_id, generation.generation, authority.assignment_version
            ));
        }
        if !self
            .converged_assignment_digests
            .contains_key(&authority.assignment_version)
            && self.converged_assignment_digests.len()
                == CHECKPOINT_BARRIER_TIMING_MAX_ASSIGNMENTS_PER_PROCESS
        {
            return Err(format!(
                "node{} process generation {} exceeded the bounded converged-assignment evidence budget",
                generation.node_id, generation.generation
            ));
        }
        if self
            .assignment_digests
            .get(&authority.assignment_version)
            .is_some_and(|digest| *digest != authority.assignment_certificate_digest)
        {
            return Err(format!(
                "node{} process generation {} timing digest conflicts with converged assignment version {}",
                generation.node_id, generation.generation, authority.assignment_version
            ));
        }
        self.process.get_or_insert(expected);
        self.converged_assignment_digests
            .entry(authority.assignment_version)
            .or_insert(authority.assignment_certificate_digest);
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
            if !self
                .assignment_digests
                .contains_key(&record.assignment_version)
                && self.assignment_digests.len()
                    == CHECKPOINT_BARRIER_TIMING_MAX_ASSIGNMENTS_PER_PROCESS
            {
                return Err(format!(
                    "node{} process generation {} exceeded the bounded observed-assignment evidence budget",
                    generation.node_id, generation.generation
                ));
            }
            if self
                .converged_assignment_digests
                .get(&record.assignment_version)
                .is_some_and(|digest| *digest != record.assignment_digest)
            {
                return Err(format!(
                    "node{} process generation {} timing record {} conflicts with converged assignment version {}",
                    generation.node_id,
                    generation.generation,
                    record.sequence,
                    record.assignment_version
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
            self.record_observation(generation, *record)?;
        }
        if !page.has_more && self.cursor != page.next_sequence - 1 {
            return Err(format!(
                "node{} process generation {} timing tail cursor {} contradicts next sequence {}",
                generation.node_id, generation.generation, self.cursor, page.next_sequence
            ));
        }
        self.process.get_or_insert(process);
        self.metadata = Some(metadata);
        Ok(page.has_more)
    }

    fn record_observation(
        &mut self,
        generation: ProcessGeneration,
        record: CheckpointBarrierTimingRecord,
    ) -> Result<(), String> {
        let increment = |value: u64, name: &str| {
            value.checked_add(1).ok_or_else(|| {
                format!(
                    "node{} process generation {} {name} exhausted",
                    generation.node_id, generation.generation
                )
            })
        };
        let aligned_resume_count = if record.aligned_resume_ns.is_some() {
            increment(self.aligned_resume_count, "aligned-resume count")?
        } else {
            self.aligned_resume_count
        };
        let pipeline_stall_within_slo =
            if record.pipeline_stall_ns <= CHECKPOINT_PIPELINE_STALL_SLO_NS {
                increment(self.pipeline_stall_within_slo, "pipeline-stall SLO count")?
            } else {
                self.pipeline_stall_within_slo
            };
        let barrier_local_within_slo =
            if record.local_barrier_ns <= CHECKPOINT_PIPELINE_STALL_SLO_NS {
                increment(self.barrier_local_within_slo, "local-barrier SLO count")?
            } else {
                self.barrier_local_within_slo
            };
        let aligned_resume_within_slo = if record
            .aligned_resume_ns
            .is_some_and(|duration| duration <= CHECKPOINT_PIPELINE_STALL_SLO_NS)
        {
            increment(self.aligned_resume_within_slo, "aligned-resume SLO count")?
        } else {
            self.aligned_resume_within_slo
        };
        let (leader_count, follower_count) = match record.role {
            CheckpointBarrierTimingRole::Leader => (
                increment(self.leader_count, "leader count")?,
                self.follower_count,
            ),
            CheckpointBarrierTimingRole::Follower => (
                self.leader_count,
                increment(self.follower_count, "follower count")?,
            ),
        };
        let no_handoff_count = if record.durable_tail_handoff {
            self.no_handoff_count
        } else {
            increment(self.no_handoff_count, "no-handoff count")?
        };
        let deadline_exhausted_count = if record.deadline_exhausted {
            increment(self.deadline_exhausted_count, "deadline-exhausted count")?
        } else {
            self.deadline_exhausted_count
        };
        let violations = [
            ("pipeline_stall", Some(record.pipeline_stall_ns)),
            ("local_barrier", Some(record.local_barrier_ns)),
            ("aligned_resume", record.aligned_resume_ns),
        ];
        let new_violations = u64::try_from(
            violations
                .iter()
                .filter(|(_, duration)| {
                    duration.is_some_and(|duration| duration > CHECKPOINT_PIPELINE_STALL_SLO_NS)
                })
                .count(),
        )
        .map_err(|_| "checkpoint timing violation count exceeds u64".to_string())?;
        let violation_count = self
            .violation_count
            .checked_add(new_violations)
            .ok_or_else(|| "checkpoint timing violation count exhausted".to_string())?;

        if let Some(artifact) = self.artifact.as_mut() {
            let writer = artifact
                .writer
                .as_mut()
                .ok_or_else(|| "checkpoint timing artifact was already closed".to_string())?;
            let artifact_line = CheckpointBarrierTimingArtifactLine {
                schema_version: CHECKPOINT_BARRIER_TIMING_ARTIFACT_SCHEMA,
                node_ordinal: generation.node_id,
                process_generation: generation.generation,
                record: &record,
            };
            serde_json::to_writer(&mut *writer, &artifact_line).map_err(|error| {
                format!(
                    "could not write checkpoint timing artifact {}: {error}",
                    artifact.path.display()
                )
            })?;
            writer.write_all(b"\n").map_err(|error| {
                format!(
                    "could not delimit checkpoint timing artifact {}: {error}",
                    artifact.path.display()
                )
            })?;
        }

        self.record_count = record.sequence;
        self.aligned_resume_count = aligned_resume_count;
        self.pipeline_stall_within_slo = pipeline_stall_within_slo;
        self.barrier_local_within_slo = barrier_local_within_slo;
        self.aligned_resume_within_slo = aligned_resume_within_slo;
        self.leader_count = leader_count;
        self.follower_count = follower_count;
        self.no_handoff_count = no_handoff_count;
        self.deadline_exhausted_count = deadline_exhausted_count;
        self.violation_count = violation_count;
        if self
            .pipeline_stall_max
            .is_none_or(|maximum| record.pipeline_stall_ns > maximum.pipeline_stall_ns)
        {
            self.pipeline_stall_max = Some(record);
        }
        if self
            .barrier_local_max
            .is_none_or(|maximum| record.local_barrier_ns > maximum.local_barrier_ns)
        {
            self.barrier_local_max = Some(record);
        }
        if record.aligned_resume_ns.is_some_and(|duration| {
            self.aligned_resume_max
                .and_then(|maximum| maximum.aligned_resume_ns)
                .is_none_or(|maximum| duration > maximum)
        }) {
            self.aligned_resume_max = Some(record);
        }
        for (stage, duration) in violations {
            if let Some(duration_ns) =
                duration.filter(|duration| *duration > CHECKPOINT_PIPELINE_STALL_SLO_NS)
            {
                if self.violation_witnesses.len() < CHECKPOINT_BARRIER_TIMING_DIAGNOSTIC_WITNESSES {
                    self.violation_witnesses
                        .push(CheckpointBarrierTimingViolation {
                            stage,
                            record,
                            duration_ns,
                        });
                }
            }
        }
        Ok(())
    }

    fn flush_artifact(&mut self) -> Result<(), String> {
        if let Some(artifact) = self.artifact.as_mut() {
            if let Some(mut writer) = artifact.writer.take() {
                writer.flush().map_err(|error| {
                    format!(
                        "could not flush checkpoint timing artifact {}: {error}",
                        artifact.path.display()
                    )
                })?;
            }
        }
        Ok(())
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
        for (version, expected_digest) in &self.converged_assignment_digests {
            match self.assignment_digests.get(version) {
                Some(observed_digest) if observed_digest == expected_digest => {}
                Some(observed_digest) => {
                    return Err(format!(
                        "node{} process generation {} timing digest {:?} conflicts with converged digest {:?} for assignment version {version}",
                        generation.node_id, generation.generation, observed_digest, expected_digest
                    ));
                }
                None => {
                    return Err(format!(
                        "node{} process generation {} has no exact timing record under converged assignment version {version}",
                        generation.node_id, generation.generation
                    ));
                }
            }
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
        let record_count = self.record_count;
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
                generation.node_id, generation.generation, metadata.oldest_retained_sequence
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
                self.aligned_resume_count,
            ),
            (
                "pipeline-stall SLO bucket",
                metrics.pipeline_stall_within_slo,
                self.pipeline_stall_within_slo,
            ),
            (
                "local-barrier SLO bucket",
                metrics.barrier_local_within_slo,
                self.barrier_local_within_slo,
            ),
            (
                "aligned-resume SLO bucket",
                metrics.aligned_resume_within_slo,
                self.aligned_resume_within_slo,
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
        let violations = self
            .violation_witnesses
            .iter()
            .take(limit)
            .map(|violation| {
                let record = violation.record;
                format!(
                    "stage={} sequence={} attempt={}/{} role={:?} assignment_version={} handoff={} process={:?} duration_ns={}",
                    violation.stage,
                    record.sequence,
                    record.attempt.epoch,
                    record.attempt.checkpoint_id,
                    record.role,
                    record.assignment_version,
                    record.durable_tail_handoff,
                    record.process,
                    violation.duration_ns,
                )
            })
            .collect::<Vec<_>>();
        if violations.is_empty() {
            "no exact observations exceeded the SLO".into()
        } else {
            let omitted = self
                .violation_count
                .saturating_sub(u64::try_from(violations.len()).unwrap_or(u64::MAX));
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
    artifact_directory: Option<PathBuf>,
}

#[cfg(feature = "kafka")]
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct CheckpointLatencySnapshot {
    checkpoint_seconds: f64,
    checkpoint_observations: f64,
    state_capture_seconds: f64,
    state_capture_observations: f64,
    state_capture_within_slo: f64,
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
            ("checkpoint duration sum", self.checkpoint_seconds),
            ("checkpoint duration count", self.checkpoint_observations),
            ("checkpoint state-capture sum", self.state_capture_seconds),
            (
                "checkpoint state-capture count",
                self.state_capture_observations,
            ),
            (
                "checkpoint state-capture SLO bucket",
                self.state_capture_within_slo,
            ),
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
        exact_prometheus_count(
            self.state_capture_observations,
            "checkpoint state-capture count",
        )?;
        exact_prometheus_count(
            self.state_capture_within_slo,
            "checkpoint state-capture SLO bucket",
        )?;
        if self.state_capture_within_slo > self.state_capture_observations {
            return Err(format!(
                "checkpoint state-capture SLO bucket {} exceeds histogram count {}",
                self.state_capture_within_slo, self.state_capture_observations
            ));
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
        self.checkpoint_seconds += other.checkpoint_seconds;
        self.checkpoint_observations += other.checkpoint_observations;
        self.state_capture_seconds += other.state_capture_seconds;
        self.state_capture_observations += other.state_capture_observations;
        self.state_capture_within_slo += other.state_capture_within_slo;
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

    fn state_capture_within_slo_percent(self) -> Option<f64> {
        (self.state_capture_observations > 0.0)
            .then(|| self.state_capture_within_slo / self.state_capture_observations * 100.0)
    }

    fn validate_state_capture_slo(self, label: &str) -> Result<(), String> {
        let within_slo_percent = self
            .state_capture_within_slo_percent()
            .ok_or_else(|| format!("{label} captured no checkpoint state-capture observations"))?;
        if within_slo_percent < 99.0 {
            return Err(format!(
                "{label} checkpoint state-capture SLO requires 99.00% of observations at or below {:.0}ms; only {within_slo_percent:.2}% of {} observations complied",
                CHECKPOINT_STATE_CAPTURE_SLO_SECONDS * 1_000.0,
                self.state_capture_observations as u64,
            ));
        }
        Ok(())
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

    fn state_capture_profile(self) -> String {
        let Some(within_slo_percent) = self.state_capture_within_slo_percent() else {
            return "no observations".into();
        };
        format!(
            "avg={:.0}ms, <= {:.0}ms={within_slo_percent:.2}% of {} obs",
            self.state_capture_seconds / self.state_capture_observations * 1_000.0,
            CHECKPOINT_STATE_CAPTURE_SLO_SECONDS * 1_000.0,
            self.state_capture_observations as u64,
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CheckpointObservationBudget {
    collection: Duration,
    total: Duration,
    max_missing: u64,
    max_cadence: Duration,
    capped: bool,
}

#[cfg(feature = "kafka")]
fn checkpoint_observation_budget(
    interval_ms: u64,
    checkpoint_timeout: Duration,
    recovery_ceiling: Duration,
    nodes: &[(u64, u64, CheckpointLatencySnapshot)],
) -> Result<CheckpointObservationBudget, String> {
    if nodes.is_empty() {
        return Err("checkpoint observation budget requires at least one node".into());
    }
    let checkpoint_timeout_ms = u64::try_from(checkpoint_timeout.as_millis())
        .map_err(|_| "checkpoint timeout does not fit u64 milliseconds".to_string())?;
    if checkpoint_timeout_ms == 0 {
        return Err("checkpoint timeout must be greater than zero".into());
    }
    let collection_cap_ms = u64::try_from(CHECKPOINT_OBSERVATION_COLLECTION_CAP.as_millis())
        .expect("checkpoint observation collection cap fits u64 milliseconds");
    let stall_slo_ms = CHECKPOINT_PIPELINE_STALL_SLO_NS / 1_000_000;
    let mut max_missing = 0_u64;
    let mut max_cadence_ms = 0_u64;

    for (required_stalls, required_captures, snapshot) in nodes {
        let snapshot = snapshot.validate()?;
        let checkpoint_observations = exact_prometheus_count(
            snapshot.checkpoint_observations,
            "live checkpoint-duration observations",
        )?;
        if checkpoint_observations == 0 {
            return Err("live process exposed no checkpoint-duration observations".into());
        }
        let live_stalls = exact_prometheus_count(
            snapshot.pipeline_stall_observations,
            "live checkpoint pipeline-stall observations",
        )?;
        let live_captures = exact_prometheus_count(
            snapshot.state_capture_observations,
            "live checkpoint state-capture observations",
        )?;
        let missing = required_stalls
            .saturating_sub(live_stalls)
            .max(required_captures.saturating_sub(live_captures));
        let doubled_average_ms =
            (snapshot.checkpoint_seconds / checkpoint_observations as f64 * 2_000.0).ceil();
        if !doubled_average_ms.is_finite()
            || doubled_average_ms < 0.0
            || doubled_average_ms >= u64::MAX as f64
        {
            return Err(format!(
                "doubled live checkpoint-duration average does not fit u64 milliseconds: {doubled_average_ms}"
            ));
        }
        let durable_tail_ms = (doubled_average_ms as u64)
            .max(stall_slo_ms)
            .min(checkpoint_timeout_ms);
        let cadence_ms = interval_ms.saturating_add(durable_tail_ms);
        max_missing = max_missing.max(missing);
        max_cadence_ms = max_cadence_ms.max(cadence_ms);
    }

    // Cluster observations advance on one coordinated checkpoint cadence. The node with the most
    // missing observations can therefore be gated by the slowest observed node even when those
    // maxima come from different nodes after staggered restarts.
    let max_collection_ms = max_missing.saturating_mul(max_cadence_ms);
    let capped = max_collection_ms > collection_cap_ms;
    let collection_ms = max_collection_ms.min(collection_cap_ms);
    let recovery_ms = u64::try_from(recovery_ceiling.as_millis())
        .map_err(|_| "recovery ceiling does not fit u64 milliseconds".to_string())?;
    Ok(CheckpointObservationBudget {
        collection: Duration::from_millis(collection_ms),
        total: Duration::from_millis(collection_ms.saturating_add(recovery_ms)),
        max_missing,
        max_cadence: Duration::from_millis(max_cadence_ms),
        capped,
    })
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

    /// Single-node runtime does not expose the cluster-only exact barrier ledger. Capture a
    /// coherent Prometheus cut for the process generation while the checkpoint kill gate holds
    /// the in-flight barrier, preserving the same once-per-generation accounting used by cluster
    /// exact timing evidence.
    fn finalize_stable_generation<M>(
        &mut self,
        generation: ProcessGeneration,
        deadline: Instant,
        mut read_metrics: M,
    ) -> Result<(), String>
    where
        M: FnMut() -> Option<CheckpointLatencySnapshot>,
    {
        if self.generations.contains_key(&generation) {
            return Err(format!(
                "checkpoint latency for node{} process generation {} was captured more than once",
                generation.node_id, generation.generation
            ));
        }
        let mut previous = None;
        loop {
            if let Some(snapshot) = read_metrics() {
                let snapshot = snapshot.validate()?;
                if previous == Some(snapshot) {
                    return self.record_generation(generation, snapshot);
                }
                previous = Some(snapshot);
            }
            if remaining_at(deadline, Instant::now()).is_none() {
                return Err(format!(
                    "node{} process generation {} never exposed a stable checkpoint latency cut",
                    generation.node_id, generation.generation
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn finalize_single_node(&mut self, node: &Node, deadline: Instant) -> Result<(), String> {
        self.finalize_stable_generation(
            ProcessGeneration {
                node_id: node.id,
                generation: node.process_generation,
            },
            deadline,
            || node.checkpoint_latency_metrics(),
        )
    }

    fn validate_observed_generations(&self, nodes: &[Node]) -> Result<(), String> {
        let observed = self.generations.keys().copied().collect::<BTreeSet<_>>();
        let mut expected = BTreeSet::new();
        for node in nodes {
            if node.process_generation == 0 {
                return Err(format!("node{} never started a soak process", node.id));
            }
            expected.extend(
                (1..=node.process_generation).map(|generation| ProcessGeneration {
                    node_id: node.id,
                    generation,
                }),
            );
        }
        if observed != expected {
            return Err(format!(
                "checkpoint latency generations {observed:?} differ from every spawned process generation {expected:?}"
            ));
        }
        Ok(())
    }

    fn aggregate_by_node(&self) -> Result<BTreeMap<usize, CheckpointLatencySnapshot>, String> {
        let mut nodes = BTreeMap::<usize, CheckpointLatencySnapshot>::new();
        for (generation, snapshot) in &self.generations {
            if snapshot.checkpoint_observations <= 0.0 {
                return Err(format!(
                    "node{} process generation {} captured no checkpoint-duration observations",
                    generation.node_id, generation.generation
                ));
            }
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

    fn validated_observations(
        &self,
        require_state_capture: bool,
    ) -> Result<
        (
            CheckpointLatencySnapshot,
            BTreeMap<usize, CheckpointLatencySnapshot>,
        ),
        String,
    > {
        let aggregate = self.aggregate()?;
        if aggregate.checkpoint_observations <= 0.0 {
            return Err("aggregate captured no checkpoint latency observations".into());
        }
        let nodes = self.aggregate_by_node()?;
        if require_state_capture {
            for (generation, snapshot) in &self.generations {
                if snapshot.state_capture_observations <= 0.0 {
                    return Err(format!(
                        "node{} process generation {} captured no checkpoint state-capture observations",
                        generation.node_id, generation.generation
                    ));
                }
            }
        }
        Ok((aggregate, nodes))
    }

    fn validate_slos(
        &self,
        certify_state_capture: bool,
    ) -> Result<CheckpointLatencySnapshot, String> {
        let (aggregate, nodes) = self.validated_observations(certify_state_capture)?;
        for (node_id, snapshot) in nodes {
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
            if snapshot.state_capture_observations
                < MIN_CHECKPOINT_STATE_CAPTURE_OBSERVATIONS as f64
            {
                return Err(format!(
                    "node{node_id} captured only {} checkpoint state-capture observations across process generations; at least {MIN_CHECKPOINT_STATE_CAPTURE_OBSERVATIONS} are required for the observed 99% SLO",
                    snapshot.state_capture_observations as u64,
                ));
            }
            snapshot
                .validate_state_capture_slo(&format!("node{node_id} across process generations"))?;
        }
        aggregate.validate_pipeline_stall_slo("aggregate")?;
        aggregate.validate_state_capture_slo("aggregate")?;
        Ok(aggregate)
    }

    fn validate_for_mode(
        &self,
        slo_mode: SloMode,
        certify_state_capture: bool,
    ) -> Result<CheckpointLatencySnapshot, String> {
        match slo_mode {
            SloMode::Certify => self.validate_slos(certify_state_capture),
            // Observation mode is deliberately not an evidence opt-out. Require every process
            // generation to contribute valid, nonzero checkpoint, stall, and capture samples, but
            // leave sample-size and percentile certification to an explicit certifying run.
            SloMode::Observe => self
                .validated_observations(true)
                .map(|(aggregate, _)| aggregate),
        }
    }

    fn report(&self, minimum_live_state_bytes: u64, slo_mode: SloMode) {
        let aggregate = self.aggregate().unwrap_or_else(|error| panic!("{error}"));
        for (generation, snapshot) in &self.generations {
            eprintln!(
                "soak: PROFILE node{} process generation {}: state capture {}; total stall {}; local barrier {}; aligned resume {}",
                generation.node_id,
                generation.generation,
                snapshot.state_capture_profile(),
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
        eprintln!(
            "soak: PROFILE checkpoint_slo_mode={}; state-capture {}; pipeline-stall <= {:.0}ms for {within_slo_percent:.2}% of {} obs; checkpoint_duration avg={checkpoint_average_ms:.0}ms over {} obs (finalized pre-restart generations plus observed cuts of live generations)",
            slo_mode.label(),
            if minimum_live_state_bytes == 0 {
                "non-certifying because retained-state floor is disabled".into()
            } else {
                aggregate.state_capture_profile()
            },
            CHECKPOINT_PIPELINE_STALL_SLO_SECONDS * 1_000.0,
            aggregate.pipeline_stall_observations as u64,
            aggregate.checkpoint_observations as u64,
        );
        if slo_mode == SloMode::Observe {
            eprintln!(
                "soak: PROFILE checkpoint latency is observational and non-certifying; exact timing evidence remains enforced"
            );
        }
        self.validate_for_mode(slo_mode, minimum_live_state_bytes > 0)
            .unwrap_or_else(|error| panic!("{error}"));
    }
}

#[cfg(feature = "kafka")]
impl CheckpointBarrierTimingEvidence {
    fn with_artifact_directory(directory: PathBuf) -> Self {
        Self {
            artifact_directory: Some(directory),
            ..Self::default()
        }
    }

    fn generation(node: &Node) -> ProcessGeneration {
        ProcessGeneration {
            node_id: node.id,
            generation: node.process_generation,
        }
    }

    fn capture_node(
        &mut self,
        node: &Node,
        expected_authority: Option<CheckpointBarrierTimingAuthority>,
        deadline: Instant,
    ) -> Result<(), String> {
        let generation = Self::generation(node);
        if !self.generations.contains_key(&generation) {
            if self.generations.len() == CHECKPOINT_BARRIER_TIMING_MAX_PROCESS_GENERATIONS {
                return Err(format!(
                    "checkpoint timing evidence exceeded the bounded {}-generation budget",
                    CHECKPOINT_BARRIER_TIMING_MAX_PROCESS_GENERATIONS
                ));
            }
            let state = if let Some(directory) = &self.artifact_directory {
                CheckpointBarrierTimingGeneration::with_artifact(directory, generation)?
            } else {
                CheckpointBarrierTimingGeneration::default()
            };
            self.generations.insert(generation, state);
        }
        loop {
            let state = self
                .generations
                .get_mut(&generation)
                .expect("timing generation was inserted before collection");
            if state.finalized {
                return Err(format!(
                    "node{} process generation {} timing evidence was already finalized",
                    generation.node_id, generation.generation
                ));
            }
            if let Some(authority) = expected_authority {
                state.bind_authority(generation, authority)?;
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
        convergence: &LocalAssignmentConvergence,
        deadline: Instant,
        label: &str,
    ) {
        let fence = convergence
            .snapshot
            .assignment_fence()
            .unwrap_or_else(|error| panic!("{label}: converged assignment is invalid: {error}"));
        for node in nodes {
            let evidence = convergence
                .evidence_by_node
                .get(&node.id)
                .unwrap_or_else(|| {
                    panic!(
                        "{label}: converged local authority omitted live node{}",
                        node.id,
                    )
                });
            let authority = checkpoint_barrier_timing_authority(evidence, &fence)
                .unwrap_or_else(|error| panic!("{label}: node{}: {error}", node.id));
            self.capture_node(node, Some(authority), deadline)
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
        expected_authority: CheckpointBarrierTimingAuthority,
        latency: &mut CheckpointLatencyEvidence,
        deadline: Instant,
    ) -> Result<(), String> {
        let generation = Self::generation(node);
        self.finalize_generation_with(
            generation,
            latency,
            deadline,
            |timing| timing.capture_node(node, Some(expected_authority), deadline),
            || node.checkpoint_latency_metrics(),
        )
    }

    fn finalize_generation_with<C, M>(
        &mut self,
        generation: ProcessGeneration,
        latency: &mut CheckpointLatencyEvidence,
        deadline: Instant,
        mut capture: C,
        mut read_metrics: M,
    ) -> Result<(), String>
    where
        C: FnMut(&mut Self) -> Result<(), String>,
        M: FnMut() -> Option<CheckpointLatencySnapshot>,
    {
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
            capture(self)?;
            let (cursor_before, metadata_before) = self
                .generations
                .get(&generation)
                .and_then(|state| state.metadata.map(|metadata| (state.cursor, metadata)))
                .ok_or_else(|| "timing capture produced no metadata".to_string())?;
            let Some(metrics_before) = read_metrics() else {
                if remaining_at(deadline, Instant::now()).is_none() {
                    return Err(format!(
                        "node{} process generation {} did not expose checkpoint latency metrics",
                        generation.node_id, generation.generation
                    ));
                }
                std::thread::sleep(Duration::from_millis(10));
                continue;
            };
            capture(self)?;
            let (cursor_after, metadata_after) = self
                .generations
                .get(&generation)
                .and_then(|state| state.metadata.map(|metadata| (state.cursor, metadata)))
                .ok_or_else(|| "timing confirmation produced no metadata".to_string())?;
            let Some(metrics_after) = read_metrics() else {
                if remaining_at(deadline, Instant::now()).is_none() {
                    return Err(format!(
                        "node{} process generation {} lost checkpoint latency metrics during finalization",
                        generation.node_id, generation.generation
                    ));
                }
                std::thread::sleep(Duration::from_millis(10));
                continue;
            };
            capture(self)?;
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
                        self.generations
                            .get_mut(&generation)
                            .expect("validated timing generation must exist")
                            .flush_artifact()?;
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
                    generation.node_id, generation.generation, incoherence
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
        for (generation, timing) in &self.generations {
            let process = timing
                .process
                .expect("reported exact timing generation has a process identity");
            eprintln!(
                "soak: EXACT node{} process generation {} {:?}: {} records (leader={}, follower={}, no_handoff={}, deadline_exhausted={}, slo_violations={})",
                generation.node_id,
                generation.generation,
                process,
                timing.record_count,
                timing.leader_count,
                timing.follower_count,
                timing.no_handoff_count,
                timing.deadline_exhausted_count,
                timing.violation_count,
            );
            for (stage, maximum) in [
                (
                    "pipeline_stall",
                    timing
                        .pipeline_stall_max
                        .map(|record| (record, record.pipeline_stall_ns)),
                ),
                (
                    "local_barrier",
                    timing
                        .barrier_local_max
                        .map(|record| (record, record.local_barrier_ns)),
                ),
                (
                    "aligned_resume",
                    timing
                        .aligned_resume_max
                        .and_then(|record| Some((record, record.aligned_resume_ns?))),
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
            if timing.violation_count != 0 {
                eprintln!(
                    "soak: EXACT SLO VIOLATION SUMMARY node{} generation {}: {}",
                    generation.node_id,
                    generation.generation,
                    timing.violation_summary(CHECKPOINT_BARRIER_TIMING_DIAGNOSTIC_WITNESSES),
                );
            }
            if let Some(artifact) = &timing.artifact {
                eprintln!(
                    "soak: EXACT ARTIFACT node{} generation {}: {}",
                    generation.node_id,
                    generation.generation,
                    artifact.path.display()
                );
            }
        }
    }
}

#[cfg(feature = "kafka")]
impl ProducerGuard {
    fn spawn(
        brokers: String,
        left_topic: String,
        right_topic: String,
        partitions: i32,
        rps: u64,
        key_count: u64,
        zipf_milli: u64,
        timestamp_floor_ms: u64,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let enqueued = Arc::new(AtomicU64::new(0));
        let producer_stop = Arc::clone(&stop);
        let producer_enqueued = Arc::clone(&enqueued);
        let handle = std::thread::spawn(move || {
            produce_join_inputs(
                &brokers,
                &left_topic,
                &right_topic,
                partitions,
                rps,
                key_count,
                zipf_milli,
                timestamp_floor_ms,
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

    fn stop(&mut self) -> (ProducedPrefix, Instant) {
        self.stop.store(true, Ordering::Release);
        let prefix = self
            .handle
            .take()
            .expect("Kafka producer was already stopped")
            .join()
            .expect("Kafka producer thread failed");
        let frozen_at = prefix.broker_acked_at;
        (prefix, frozen_at)
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
const CLUSTER_SUBSCRIPTION_SOAK_STREAM: &str = "soak_subscription_aggregate";
#[cfg(feature = "kafka")]
const SUBSCRIPTION_PROGRESS_PER_GATEWAY: u64 = 2;
#[cfg(feature = "kafka")]
const MAX_SUBSCRIPTION_CHUNKS_PER_INTERVAL: usize = 65_536;

#[cfg(feature = "kafka")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SubscriptionLogicalFrame {
    partition: u16,
    sequence: u64,
}

#[cfg(feature = "kafka")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SubscriptionChunkId {
    frame: SubscriptionLogicalFrame,
    row_offset: usize,
}

#[cfg(feature = "kafka")]
#[derive(Clone, Default)]
struct ClusterSubscriptionObservation {
    stream_generation: Option<String>,
    expected_sequences: BTreeMap<u16, u64>,
    frame_next_offsets: BTreeMap<SubscriptionLogicalFrame, usize>,
    chunk_digests: BTreeMap<SubscriptionChunkId, [u8; 32]>,
    aggregate: BTreeMap<u64, (u64, u64)>,
    observed_partitions: BTreeSet<u16>,
    used_gateways: BTreeSet<usize>,
    pending_epoch: Option<u64>,
    last_progress_epoch: u64,
    progress_events: u64,
    reconnects: u64,
    last_connection_error: Option<String>,
}

#[cfg(feature = "kafka")]
impl ClusterSubscriptionObservation {
    fn observe_data(&mut self, frame: &serde_json::Value) -> Result<(), String> {
        let generation = subscription_string_field(frame, "stream_generation")?;
        self.observe_generation(generation)?;
        let partition = u16::try_from(subscription_u64_field(frame, "partition")?)
            .map_err(|_| "subscription partition exceeds u16".to_string())?;
        let sequence = subscription_u64_field(frame, "partition_sequence")?;
        let committed_epoch = subscription_u64_field(frame, "committed_epoch")?;
        if committed_epoch == 0 {
            return Err("subscription data carried epoch zero".to_string());
        }
        match self.pending_epoch {
            Some(expected) if expected != committed_epoch => {
                return Err(format!(
                    "subscription data crossed epochs without progress: {expected}->{committed_epoch}"
                ));
            }
            None => self.pending_epoch = Some(committed_epoch),
            Some(_) => {}
        }
        let row_offset = usize::try_from(subscription_u64_field(frame, "row_offset")?)
            .map_err(|_| "subscription row offset exceeds usize".to_string())?;
        let row_count = usize::try_from(subscription_u64_field(frame, "row_count")?)
            .map_err(|_| "subscription row count exceeds usize".to_string())?;
        let data = frame
            .get("data")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "subscription data frame omitted its row array".to_string())?;
        if data.is_empty() || data.len() != row_count {
            return Err("subscription data frame row count is inconsistent".to_string());
        }
        let logical = SubscriptionLogicalFrame {
            partition,
            sequence,
        };
        let chunk = SubscriptionChunkId {
            frame: logical,
            row_offset,
        };
        let digest: [u8; 32] = Sha256::digest(
            serde_json::to_vec(data)
                .map_err(|error| format!("encode subscription rows for digest: {error}"))?,
        )
        .into();
        if let Some(existing) = self.chunk_digests.get(&chunk) {
            if existing != &digest {
                return Err(format!(
                    "conflicting duplicate subscription frame partition={partition} sequence={sequence} row_offset={row_offset}"
                ));
            }
            return Ok(());
        }
        if self.chunk_digests.len() == MAX_SUBSCRIPTION_CHUNKS_PER_INTERVAL {
            return Err("subscription replay interval exceeded the bounded chunk oracle".into());
        }
        self.begin_or_continue_frame(logical, row_offset, row_count)?;
        self.chunk_digests.insert(chunk, digest);
        self.observed_partitions.insert(partition);
        for row in data {
            let key = subscription_u64_field(row, "join_key")?;
            let count = subscription_u64_field(row, "match_count")?;
            let max_right_id = subscription_u64_field(row, "max_right_id")?;
            self.aggregate.insert(key, (count, max_right_id));
        }
        Ok(())
    }

    fn begin_or_continue_frame(
        &mut self,
        frame: SubscriptionLogicalFrame,
        row_offset: usize,
        row_count: usize,
    ) -> Result<(), String> {
        if let Some(next_offset) = self.frame_next_offsets.get_mut(&frame) {
            if row_offset != *next_offset {
                return Err(format!(
                    "subscription frame chunk gap partition={} sequence={}: expected row offset {}, got {row_offset}",
                    frame.partition, frame.sequence, *next_offset
                ));
            }
            *next_offset = next_offset
                .checked_add(row_count)
                .ok_or_else(|| "subscription row offset overflow".to_string())?;
            return Ok(());
        }
        let expected = self
            .expected_sequences
            .entry(frame.partition)
            .or_insert(frame.sequence);
        if frame.sequence != *expected {
            return Err(format!(
                "subscription partition sequence gap for {}: expected {}, got {}",
                frame.partition, *expected, frame.sequence
            ));
        }
        if row_offset != 0 {
            return Err(format!(
                "subscription logical frame began at row offset {row_offset}"
            ));
        }
        *expected = expected
            .checked_add(1)
            .ok_or_else(|| "subscription partition sequence overflow".to_string())?;
        self.frame_next_offsets.insert(frame, row_count);
        Ok(())
    }

    fn observe_progress(
        &mut self,
        frame: &serde_json::Value,
        progress_path: &Path,
    ) -> Result<(), String> {
        let generation = subscription_string_field(frame, "stream_generation")?;
        self.observe_generation(generation)?;
        let epoch = subscription_u64_field(frame, "epoch")?;
        let checkpoint_id = subscription_u64_field(frame, "checkpoint_id")?;
        if epoch == 0 || checkpoint_id != epoch || epoch <= self.last_progress_epoch {
            return Err(format!(
                "subscription progress is not a strictly advancing canonical checkpoint: epoch={epoch}, checkpoint_id={checkpoint_id}, previous={}",
                self.last_progress_epoch
            ));
        }
        if self.pending_epoch.is_some_and(|pending| pending != epoch) {
            return Err(format!(
                "subscription progress epoch {epoch} differs from pending data epoch {:?}",
                self.pending_epoch
            ));
        }
        persist_subscription_progress(progress_path, epoch)?;
        self.last_progress_epoch = epoch;
        self.progress_events = self
            .progress_events
            .checked_add(1)
            .ok_or_else(|| "subscription progress count overflow".to_string())?;
        self.pending_epoch = None;
        self.frame_next_offsets.clear();
        self.chunk_digests.clear();
        Ok(())
    }

    fn observe_generation(&mut self, generation: &str) -> Result<(), String> {
        if generation.is_empty() || generation.len() > 128 {
            return Err("subscription stream generation is not canonical".to_string());
        }
        match self.stream_generation.as_deref() {
            Some(expected) if expected != generation => Err(format!(
                "subscription stream generation changed from {expected} to {generation}"
            )),
            None => {
                self.stream_generation = Some(generation.to_owned());
                Ok(())
            }
            Some(_) => Ok(()),
        }
    }
}

#[cfg(feature = "kafka")]
struct ClusterSubscriptionObserver {
    stop: Arc<AtomicBool>,
    attached: Arc<AtomicBool>,
    observation: Arc<parking_lot::Mutex<ClusterSubscriptionObservation>>,
    handle: Option<JoinHandle<Result<(), String>>>,
}

#[cfg(feature = "kafka")]
impl ClusterSubscriptionObserver {
    fn spawn(progress_path: PathBuf) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let attached = Arc::new(AtomicBool::new(false));
        let observation = Arc::new(parking_lot::Mutex::new(
            ClusterSubscriptionObservation::default(),
        ));
        let task_stop = Arc::clone(&stop);
        let task_attached = Arc::clone(&attached);
        let task_observation = Arc::clone(&observation);
        let handle = std::thread::Builder::new()
            .name("cluster-subscription-soak-reader".into())
            .spawn(move || {
                run_cluster_subscription_observer(
                    &progress_path,
                    &task_stop,
                    &task_attached,
                    &task_observation,
                )
            })
            .expect("spawn cluster subscription observer");
        Self {
            stop,
            attached,
            observation,
            handle: Some(handle),
        }
    }

    fn wait_until_attached(&mut self, window: Duration) {
        wait_for("cluster subscription gateway attachment", window, || {
            self.assert_running();
            self.attached.load(Ordering::Acquire)
        });
    }

    fn wait_until_progress(&mut self, epoch: u64, window: Duration) {
        wait_for("cluster subscription committed progress", window, || {
            self.assert_running();
            self.observation.lock().last_progress_epoch >= epoch
        });
    }

    fn assert_running(&mut self) {
        if !self
            .handle
            .as_ref()
            .is_some_and(std::thread::JoinHandle::is_finished)
        {
            return;
        }
        let result = self
            .handle
            .take()
            .expect("subscription observer handle")
            .join();
        match result {
            Ok(Ok(())) => panic!("cluster subscription observer stopped before the soak"),
            Ok(Err(error)) => panic!("cluster subscription observer failed: {error}"),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    fn stop(mut self) -> ClusterSubscriptionObservation {
        self.stop.store(true, Ordering::Release);
        let result = self
            .handle
            .take()
            .expect("subscription observer was already stopped")
            .join();
        match result {
            Ok(Ok(())) => self.observation.lock().clone(),
            Ok(Err(error)) => panic!("cluster subscription observer failed: {error}"),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
}

#[cfg(feature = "kafka")]
impl Drop for ClusterSubscriptionObserver {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(feature = "kafka")]
fn run_cluster_subscription_observer(
    progress_path: &Path,
    stop: &AtomicBool,
    attached: &AtomicBool,
    observation: &parking_lot::Mutex<ClusterSubscriptionObservation>,
) -> Result<(), String> {
    let mut gateway_cursor = 0usize;
    let mut connected_once = false;
    while !stop.load(Ordering::Acquire) {
        let gateway = gateway_cursor % NODES;
        gateway_cursor = gateway_cursor
            .checked_add(1)
            .ok_or_else(|| "subscription gateway cursor overflow".to_string())?;
        let resume_epoch = load_subscription_progress(progress_path)?;
        let Some(mut socket) = connect_cluster_subscription_gateway(gateway, resume_epoch)? else {
            std::thread::sleep(Duration::from_millis(100));
            continue;
        };
        {
            let mut state = observation.lock();
            state.used_gateways.insert(gateway);
            state.last_connection_error = None;
            if connected_once {
                state.reconnects = state
                    .reconnects
                    .checked_add(1)
                    .ok_or_else(|| "subscription reconnect count overflow".to_string())?;
            }
        }
        connected_once = true;
        attached.store(true, Ordering::Release);
        match consume_cluster_subscription_gateway(&mut socket, progress_path, stop, observation) {
            Ok(()) => {}
            Err(error) if subscription_connection_error_is_retryable(&error) => {
                observation.lock().last_connection_error = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(feature = "kafka")]
fn connect_cluster_subscription_gateway(
    gateway: usize,
    resume_epoch: Option<u64>,
) -> Result<Option<tungstenite::WebSocket<TcpStream>>, String> {
    let port = BASE_PORT
        .checked_add(u16::try_from(gateway).map_err(|_| "gateway index exceeds u16")?)
        .ok_or_else(|| "gateway port overflow".to_string())?;
    let address = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let stream = match TcpStream::connect_timeout(&address, Duration::from_secs(2)) {
        Ok(stream) => stream,
        Err(_) => return Ok(None),
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|error| format!("set subscription socket read timeout: {error}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|error| format!("set subscription socket write timeout: {error}"))?;
    stream
        .set_nodelay(true)
        .map_err(|error| format!("set subscription socket nodelay: {error}"))?;
    let replay = resume_epoch.map_or_else(String::new, |epoch| format!("&as_of_epoch={epoch}"));
    let request = format!(
        "ws://127.0.0.1:{port}/ws/{CLUSTER_SUBSCRIPTION_SOAK_STREAM}?token={SOAK_CONSOLE_TOKEN}{replay}"
    );
    match tungstenite::client(request, stream) {
        Ok((socket, response)) if response.status().as_u16() == 101 => Ok(Some(socket)),
        Ok((_, response)) => Err(format!(
            "retryable subscription handshake status {}",
            response.status()
        )),
        Err(tungstenite::HandshakeError::Interrupted(_)) => Ok(None),
        Err(tungstenite::HandshakeError::Failure(tungstenite::Error::Http(response)))
            if response.status().is_server_error() =>
        {
            Ok(None)
        }
        Err(tungstenite::HandshakeError::Failure(tungstenite::Error::Http(response))) => {
            Err(format!(
                "subscription handshake failed with HTTP {}",
                response.status()
            ))
        }
        Err(tungstenite::HandshakeError::Failure(error))
            if subscription_websocket_error_is_retryable(&error) =>
        {
            Ok(None)
        }
        Err(error) => Err(format!("subscription handshake failed: {error}")),
    }
}

#[cfg(feature = "kafka")]
fn consume_cluster_subscription_gateway(
    socket: &mut tungstenite::WebSocket<TcpStream>,
    progress_path: &Path,
    stop: &AtomicBool,
    observation: &parking_lot::Mutex<ClusterSubscriptionObservation>,
) -> Result<(), String> {
    let mut progress_on_connection = 0u64;
    while !stop.load(Ordering::Acquire) {
        match socket.read() {
            Ok(tungstenite::Message::Text(text)) => {
                let frame: serde_json::Value = serde_json::from_str(text.as_ref())
                    .map_err(|error| format!("invalid subscription JSON frame: {error}"))?;
                match frame.get("type").and_then(serde_json::Value::as_str) {
                    Some("data") => observation.lock().observe_data(&frame)?,
                    Some("progress") => {
                        observation.lock().observe_progress(&frame, progress_path)?;
                        progress_on_connection =
                            progress_on_connection.checked_add(1).ok_or_else(|| {
                                "subscription connection progress overflow".to_string()
                            })?;
                        if progress_on_connection == SUBSCRIPTION_PROGRESS_PER_GATEWAY {
                            return Ok(());
                        }
                    }
                    Some("error" | "gap") => {
                        return Err(format!("subscription gateway reported {frame}"));
                    }
                    Some(kind) => return Err(format!("unknown subscription frame type {kind:?}")),
                    None => return Err("subscription frame omitted its type".to_string()),
                }
            }
            Ok(tungstenite::Message::Ping(payload)) => {
                socket
                    .send(tungstenite::Message::Pong(payload))
                    .map_err(|error| format!("retryable subscription pong failed: {error}"))?;
            }
            Ok(tungstenite::Message::Pong(_)) => {}
            Ok(tungstenite::Message::Close(_)) => {
                return Err("retryable subscription gateway closed".to_string());
            }
            Ok(tungstenite::Message::Binary(_)) => {
                return Err("subscription gateway emitted a binary frame".to_string());
            }
            Ok(_) => {}
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) if subscription_websocket_error_is_retryable(&error) => {
                return Err("retryable subscription transport closed".to_string());
            }
            Err(error) => return Err(format!("subscription WebSocket failed: {error}")),
        }
    }
    Ok(())
}

#[cfg(feature = "kafka")]
fn subscription_connection_error_is_retryable(error: &str) -> bool {
    error.starts_with("retryable subscription")
}

#[cfg(feature = "kafka")]
fn subscription_websocket_error_is_retryable(error: &tungstenite::Error) -> bool {
    matches!(
        error,
        tungstenite::Error::Io(_)
            | tungstenite::Error::ConnectionClosed
            | tungstenite::Error::AlreadyClosed
            | tungstenite::Error::Protocol(
                tungstenite::error::ProtocolError::ResetWithoutClosingHandshake
            )
    )
}

#[cfg(feature = "kafka")]
fn subscription_string_field<'a>(
    value: &'a serde_json::Value,
    field: &str,
) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("subscription frame omitted string field {field:?}"))
}

#[cfg(feature = "kafka")]
fn subscription_u64_field(value: &serde_json::Value, field: &str) -> Result<u64, String> {
    value
        .get(field)
        .and_then(json_u64)
        .ok_or_else(|| format!("subscription frame omitted non-negative field {field:?}"))
}

#[cfg(feature = "kafka")]
fn persist_subscription_progress(path: &Path, epoch: u64) -> Result<(), String> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| format!("open subscription progress journal: {error}"))?;
    writeln!(file, "{epoch}")
        .map_err(|error| format!("append subscription progress journal: {error}"))?;
    file.sync_data()
        .map_err(|error| format!("sync subscription progress journal: {error}"))
}

#[cfg(feature = "kafka")]
fn load_subscription_progress(path: &Path) -> Result<Option<u64>, String> {
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("read subscription progress journal: {error}")),
    };
    if contents.len() > 1024 * 1024 {
        return Err("subscription progress journal exceeded 1 MiB".to_string());
    }
    contents
        .lines()
        .next_back()
        .map(|epoch| {
            epoch
                .parse::<u64>()
                .map_err(|error| format!("parse subscription progress epoch: {error}"))
        })
        .transpose()
}

#[cfg(feature = "kafka")]
fn expected_cluster_subscription_aggregate(
    produced: &ProducedPrefix,
    key_count: u64,
    zipf_milli: u64,
) -> BTreeMap<u64, (u64, u64)> {
    let sampler = ZipfSampler::new(key_count, zipf_milli);
    let mut expected = BTreeMap::<u64, (u64, u64)>::new();
    for id in 0..produced.count {
        let key = sampler.sample(id);
        let entry = expected.entry(key).or_insert((0, 0));
        entry.0 = entry
            .0
            .checked_add(1)
            .expect("subscription aggregate count overflow");
        entry.1 = entry.1.max(id);
    }
    expected
}

#[cfg(feature = "kafka")]
fn assert_cluster_subscription_observation(
    observation: &ClusterSubscriptionObservation,
    produced: &ProducedPrefix,
    key_count: u64,
    zipf_milli: u64,
) {
    let expected = expected_cluster_subscription_aggregate(produced, key_count, zipf_milli);
    assert_eq!(
        observation.aggregate, expected,
        "committed cluster subscription aggregate differs from the independent input oracle"
    );
    assert_eq!(
        observation.used_gateways,
        BTreeSet::from([0, 1, 2]),
        "subscription observer did not successfully attach through every gateway"
    );
    assert!(
        observation.reconnects >= 2,
        "subscription observer did not exercise cross-gateway replay"
    );
    assert!(
        observation.observed_partitions.len() >= NODES.saturating_mul(2),
        "subscription output did not span enough vnode partitions: {:?}",
        observation.observed_partitions
    );
    assert!(
        observation.last_connection_error.is_none(),
        "subscription observer ended with a connection failure: {:?}",
        observation.last_connection_error
    );
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
struct KafkaJoinCommitOracle {
    left: KafkaCommitOracle,
    right: KafkaCommitOracle,
}

#[cfg(feature = "kafka")]
impl KafkaJoinCommitOracle {
    fn new(
        brokers: &str,
        group: &str,
        left_topic: &str,
        right_topic: &str,
        partitions: i32,
    ) -> Self {
        Self {
            left: KafkaCommitOracle::new(brokers, &format!("{group}-left"), left_topic, partitions),
            right: KafkaCommitOracle::new(
                brokers,
                &format!("{group}-right"),
                right_topic,
                partitions,
            ),
        }
    }

    fn committed_offsets(&self) -> Option<Vec<i64>> {
        let mut offsets = self.left.committed_offsets()?;
        offsets.extend(self.right.committed_offsets()?);
        Some(offsets)
    }

    fn committed_offset_sum(&self) -> Option<i64> {
        self.committed_offsets()
            .map(|offsets| offsets.into_iter().sum())
    }

    fn covers(&self, boundary: &[i64]) -> bool {
        let partitions = usize::try_from(self.left.partitions).ok();
        partitions.is_some_and(|partitions| {
            boundary.len() == partitions.saturating_mul(2)
                && self.left.covers(&boundary[..partitions])
                && self.right.covers(&boundary[partitions..])
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
fn wait_for_offset_advance(
    nodes: &mut [Node],
    producer: &mut ProducerGuard,
    baseline: &[i64],
    window: Duration,
    label: &str,
    mut current_offsets: impl FnMut() -> Option<Vec<i64>>,
) -> (Instant, Vec<i64>) {
    let mut observed = None;
    wait_for(
        &format!("{label}: every Kafka partition offset to advance"),
        window,
        || {
            assert_running_nodes(nodes);
            producer.assert_running();
            let Some(current) = current_offsets() else {
                return false;
            };
            match all_partition_offsets_advanced(baseline, &current) {
                Ok(true) => {
                    observed = Some((Instant::now(), current));
                    true
                }
                Ok(false) => false,
                Err(error) => panic!("{label}: invalid Kafka offset frontier: {error}"),
            }
        },
    );
    observed.expect("offset wait completed without an observed frontier")
}

#[cfg(feature = "kafka")]
fn wait_for_minimum_offset_rate(
    nodes: &mut [Node],
    producer: &mut ProducerGuard,
    start_at: Instant,
    start_offsets: &[i64],
    deadline_offsets: &[i64],
    minimum_rps: f64,
    window: Duration,
    label: &str,
    mut current_offsets: impl FnMut() -> Option<Vec<i64>>,
) -> (Instant, Vec<i64>) {
    let mut observed = None;
    wait_for(
        &format!("{label}: post-window frontier to sustain at least {minimum_rps:.1} rps"),
        window,
        || {
            assert_running_nodes(nodes);
            producer.assert_running();
            let Some(current) = current_offsets() else {
                return false;
            };
            match all_partition_offsets_advanced(deadline_offsets, &current) {
                Ok(true) => {}
                Ok(false) => return false,
                Err(error) => panic!("{label}: invalid Kafka offset frontier: {error}"),
            }
            let observed_at = Instant::now();
            let emitted = monotonic_offset_delta(label, start_offsets, &current);
            let elapsed = observed_at.duration_since(start_at).as_secs_f64();
            if emitted as f64 / elapsed < minimum_rps {
                return false;
            }
            observed = Some((observed_at, current));
            true
        },
    );
    observed.expect("offset-rate wait completed without an observed frontier")
}

#[cfg(feature = "kafka")]
fn recovery_aware_durable_progress_window(
    interval_ms: u64,
    checkpoint_timeout: Duration,
    recovery_ceiling: Duration,
    required_commits: u32,
) -> Duration {
    let checkpoint_cycle = Duration::from_millis(interval_ms).saturating_add(checkpoint_timeout);
    recovery_ceiling
        .saturating_add(checkpoint_cycle.saturating_mul(required_commits.saturating_add(1)))
}

#[cfg(feature = "kafka")]
fn assert_active_load_throughput(
    nodes: &mut [Node],
    producer: &mut ProducerGuard,
    inputs: &[(&str, &KafkaJoinCommitOracle)],
    outputs: &[(&str, Option<&KafkaOutputOracle>)],
    target_rps: u64,
    interval_ms: u64,
    checkpoint_timeout: Duration,
    recovery_ceiling: Duration,
    delivery: JoinDelivery,
) {
    assert!(!inputs.is_empty(), "active-load sample has no input oracle");
    assert_running_nodes(nodes);
    producer.assert_running();
    // Kafka's committed frontier advances only after the asynchronous checkpoint tail completes.
    // A constrained object store can consume one tail before recovery starts, and the restored
    // generation then needs another cycle to publish its first durable cut.
    let durable_progress_window = recovery_aware_durable_progress_window(
        interval_ms,
        checkpoint_timeout,
        recovery_ceiling,
        1,
    );
    for &(label, input) in inputs {
        let seed = input
            .committed_offsets()
            .unwrap_or_else(|| panic!("active-load {label} initial committed-offset snapshot"));
        initialized_offset_sum(&format!("active-load {label} initial offsets"), &seed);
        wait_for_offset_advance(
            nodes,
            producer,
            &seed,
            durable_progress_window,
            &format!("active-load {label} durable baseline"),
            || input.committed_offsets(),
        );
    }
    // INVARIANT: ALO output can publish ahead of its source checkpoint. Delimit output samples
    // with output publication advances, not committed source cuts, so both endpoints represent
    // the same externally observable boundary.
    let mut output_starts = Vec::new();
    for &(label, output) in outputs {
        let Some(output) = output else {
            continue;
        };
        let seed = output
            .high_watermarks()
            .unwrap_or_else(|| panic!("active-load {label} initial output snapshot"));
        let (started_at, offsets) = wait_for_offset_advance(
            nodes,
            producer,
            &seed,
            durable_progress_window,
            &format!("active-load {label} output baseline"),
            || output.high_watermarks(),
        );
        output_starts.push((label, output, started_at, offsets));
    }
    let committed_starts = inputs
        .iter()
        .map(|&(label, input)| {
            let (_, observed_at, offsets) = timed_snapshot(|| {
                input
                    .committed_offsets()
                    .unwrap_or_else(|| panic!("active-load {label} start offsets"))
            });
            let sum =
                initialized_offset_sum(&format!("active-load {label} start offsets"), &offsets);
            (label, input, observed_at, offsets, sum)
        })
        .collect::<Vec<_>>();
    let (offered_start_at, _, offered_start) = timed_snapshot(|| producer.enqueued());
    let sample_started = Instant::now();
    while sample_started.elapsed() < ACTIVE_LOAD_SAMPLE_WINDOW {
        assert_running_nodes(nodes);
        producer.assert_running();
        std::thread::sleep(Duration::from_millis(100));
    }
    let (_, offered_end_at, offered_end) = timed_snapshot(|| producer.enqueued());
    let committed_at_deadline = committed_starts
        .iter()
        .map(|(label, input, _, _, _)| {
            input
                .committed_offsets()
                .unwrap_or_else(|| panic!("active-load {label} deadline offsets"))
        })
        .collect::<Vec<_>>();
    let output_at_deadline = output_starts
        .iter()
        .map(|(label, output, _, _)| {
            output
                .high_watermarks()
                .unwrap_or_else(|| panic!("active-load {label} output deadline"))
        })
        .collect::<Vec<_>>();
    let offered_pairs = offered_end
        .checked_sub(offered_start)
        .expect("producer enqueue count regressed");
    let offered_elapsed = offered_end_at
        .duration_since(offered_start_at)
        .as_secs_f64();
    let offered_pair_rps = offered_pairs as f64 / offered_elapsed;
    let minimum_pair_rps = target_rps as f64 * ACTIVE_LOAD_MINIMUM_RATIO;
    eprintln!(
        "soak: ACTIVE LOAD producer_accepted={offered_pair_rps:.1} logical_pairs/s/{offered_pairs} pairs/{offered_elapsed:.1}s"
    );
    assert!(
        offered_pair_rps >= minimum_pair_rps,
        "active-load producer accepted only {offered_pair_rps:.1} logical pairs/s against target {target_rps}"
    );
    for ((label, input, started_at, start_offsets, start), deadline_offsets) in
        committed_starts.into_iter().zip(committed_at_deadline)
    {
        all_partition_offsets_advanced(&start_offsets, &deadline_offsets)
            .unwrap_or_else(|error| panic!("active-load {label} durable deadline: {error}"));
        let minimum_row_rps = minimum_pair_rps * 2.0;
        let (ended_at, end_offsets) = wait_for_minimum_offset_rate(
            nodes,
            producer,
            started_at,
            &start_offsets,
            &deadline_offsets,
            minimum_row_rps,
            durable_progress_window,
            &format!("active-load {label} durable endpoint"),
            || input.committed_offsets(),
        );
        let end =
            initialized_offset_sum(&format!("active-load {label} final offsets"), &end_offsets);
        let rows = end
            .checked_sub(start)
            .expect("committed input offsets regressed");
        let elapsed = ended_at.duration_since(started_at).as_secs_f64();
        let pair_rps = rows as f64 / 2.0 / elapsed;
        eprintln!(
            "soak: ACTIVE LOAD {label}_durable={pair_rps:.1} logical_pair_equivalents/s/{rows} rows/{elapsed:.1}s"
        );
        if delivery == JoinDelivery::AtLeastOnce {
            assert!(
                pair_rps >= minimum_pair_rps,
                "{label} durably advanced at only {pair_rps:.1} logical-pair equivalents/s against target {target_rps}"
            );
        }
    }
    for ((label, output, emitted_start_at, output_start), deadline_offsets) in
        output_starts.into_iter().zip(output_at_deadline)
    {
        let (emitted_end_at, output_end) = wait_for_minimum_offset_rate(
            nodes,
            producer,
            emitted_start_at,
            &output_start,
            &deadline_offsets,
            minimum_pair_rps,
            durable_progress_window,
            &format!("active-load {label} output endpoint"),
            || output.high_watermarks(),
        );
        let emitted = monotonic_offset_delta(label, &output_start, &output_end);
        let emitted_elapsed = emitted_end_at
            .duration_since(emitted_start_at)
            .as_secs_f64();
        let emitted_rps = emitted as f64 / emitted_elapsed;
        eprintln!(
            "soak: ACTIVE LOAD {label}_output={emitted_rps:.1} rps/{emitted} records/{emitted_elapsed:.1}s"
        );
        if delivery == JoinDelivery::AtLeastOnce {
            assert!(
                emitted_rps >= minimum_pair_rps,
                "{label} output advanced at only {emitted_rps:.1} rps against target {target_rps} rps"
            );
        }
    }
    if delivery == JoinDelivery::ExactlyOnce {
        eprintln!(
            "soak: ACTIVE LOAD durable/output rates are observational for MinIO EO protocol validation"
        );
    }
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
fn expected_core_window_outputs(event_time_ms: u64) -> BTreeMap<CoreWindowOutput, u64> {
    let event_time_ms = i64::try_from(event_time_ms).expect("window event time fits i64");
    let tumble_start = event_time_ms.div_euclid(1_000) * 1_000;
    let latest_hop_start = event_time_ms.div_euclid(500) * 500;
    [
        CoreWindowOutput {
            kind: "tumble".to_owned(),
            window_start_ms: tumble_start,
            row_count: 4,
            id_sum: -412,
        },
        CoreWindowOutput {
            kind: "hop".to_owned(),
            window_start_ms: latest_hop_start - 500,
            row_count: 4,
            id_sum: -412,
        },
        CoreWindowOutput {
            kind: "hop".to_owned(),
            window_start_ms: latest_hop_start,
            row_count: 4,
            id_sum: -412,
        },
        CoreWindowOutput {
            kind: "session".to_owned(),
            window_start_ms: event_time_ms,
            row_count: 4,
            id_sum: -412,
        },
    ]
    .into_iter()
    .fold(BTreeMap::new(), |mut expected, row| {
        *expected.entry(row).or_default() += 1;
        expected
    })
}

#[cfg(feature = "kafka")]
fn json_timestamp_ms(value: &serde_json::Value, field: &str, context: &str) -> Result<i64, String> {
    let value = value
        .get(field)
        .ok_or_else(|| format!("{context} has no {field}: {value}"))?;
    if let Some(timestamp) = value.as_i64() {
        return Ok(timestamp);
    }
    let timestamp = value
        .as_str()
        .ok_or_else(|| format!("{context} {field} is not a timestamp string: {value}"))?;
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .map(|parsed| parsed.timestamp_millis())
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%dT%H:%M:%S%.f")
                .map(|parsed| parsed.and_utc().timestamp_millis())
        })
        .map_err(|error| format!("{context} {field} {timestamp:?} is invalid: {error}"))
}

#[cfg(feature = "kafka")]
fn decode_core_window_output(value: &serde_json::Value) -> Result<CoreWindowOutput, String> {
    let kind = value
        .get("window_kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("window output has no string window_kind: {value}"))?;
    let row_count = value
        .get("row_count")
        .and_then(json_u64)
        .ok_or_else(|| format!("window output has no non-negative row_count: {value}"))?;
    let id_sum = value
        .get("id_sum")
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        })
        .ok_or_else(|| format!("window output has no signed id_sum: {value}"))?;
    Ok(CoreWindowOutput {
        kind: kind.to_owned(),
        window_start_ms: json_timestamp_ms(value, "window_start", "window output")?,
        row_count,
        id_sum,
    })
}

#[cfg(feature = "kafka")]
struct KafkaCoreWindowOracle {
    consumer: rdkafka::consumer::BaseConsumer,
    topic: String,
    consumed_offsets: Vec<i64>,
    observed: BTreeMap<CoreWindowOutput, u64>,
}

#[cfg(feature = "kafka")]
impl KafkaCoreWindowOracle {
    fn new(brokers: &str, topic: &str) -> Self {
        use rdkafka::consumer::Consumer;
        use rdkafka::{Offset, TopicPartitionList};

        let consumer: rdkafka::consumer::BaseConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set(
                "group.id",
                format!("laminardb-soak-window-oracle-{}", std::process::id()),
            )
            .set("enable.auto.commit", "false")
            .create()
            .expect("Kafka CoreWindow oracle consumer");
        let mut assignment = TopicPartitionList::with_capacity(
            usize::try_from(OUTPUT_TOPIC_PARTITIONS).expect("window partition count fits usize"),
        );
        for partition in 0..OUTPUT_TOPIC_PARTITIONS {
            assignment
                .add_partition_offset(topic, partition, Offset::Beginning)
                .expect("build CoreWindow oracle assignment");
        }
        consumer
            .assign(&assignment)
            .expect("assign CoreWindow oracle from beginning");
        Self {
            consumer,
            topic: topic.to_owned(),
            consumed_offsets: vec![
                0;
                usize::try_from(OUTPUT_TOPIC_PARTITIONS)
                    .expect("window partition count fits usize")
            ],
            observed: BTreeMap::new(),
        }
    }

    fn high_watermarks(&self) -> Option<Vec<i64>> {
        kafka_high_watermarks(&self.consumer, &self.topic, OUTPUT_TOPIC_PARTITIONS)
    }

    fn consumed_through(&self, boundary: &[i64]) -> bool {
        self.consumed_offsets.len() == boundary.len()
            && self
                .consumed_offsets
                .iter()
                .zip(boundary)
                .all(|(consumed, boundary)| consumed >= boundary)
    }

    fn drain(&mut self) -> Result<usize, String> {
        use rdkafka::message::Message;

        let mut drained = 0;
        while let Some(result) = self.consumer.poll(Duration::ZERO) {
            let message = result.map_err(|error| format!("Kafka window read failed: {error}"))?;
            record_consumed_offset(
                &mut self.consumed_offsets,
                message.partition(),
                message.offset(),
            );
            let payload = message
                .payload()
                .ok_or_else(|| "Kafka window output had a null payload".to_owned())?;
            let value: serde_json::Value = serde_json::from_slice(payload)
                .map_err(|error| format!("invalid Kafka window JSON: {error}"))?;
            let output = decode_core_window_output(&value)?;
            *self.observed.entry(output).or_default() += 1;
            drained += 1;
        }
        Ok(drained)
    }
}

#[cfg(feature = "kafka")]
fn validate_core_window_outputs(
    observed: &BTreeMap<CoreWindowOutput, u64>,
    expected: &BTreeMap<CoreWindowOutput, u64>,
    exactly_once: bool,
) -> Result<u64, String> {
    for (row, count) in observed {
        let Some(expected_count) = expected.get(row) else {
            return Err(format!("unexpected CoreWindow output {row:?}"));
        };
        if exactly_once && count != expected_count {
            return Err(format!(
                "CoreWindow output {row:?} occurred {count} times; expected {expected_count}"
            ));
        }
    }
    for (row, expected_count) in expected {
        let observed_count = observed.get(row).copied().unwrap_or(0);
        if observed_count < *expected_count {
            return Err(format!(
                "CoreWindow output {row:?} occurred {observed_count} times; expected at least {expected_count}"
            ));
        }
    }
    Ok(observed.values().copied().sum())
}

#[cfg(feature = "kafka")]
fn assert_kafka_core_window_outputs(
    nodes: &mut [Node],
    output: &mut KafkaCoreWindowOracle,
    expected: &BTreeMap<CoreWindowOutput, u64>,
    frozen_input_at: Instant,
    window: Duration,
    label: &str,
) {
    let deadline = Instant::now() + window;
    let visibility_slo = Duration::from_millis(env_u64(
        "LAMINAR_SOAK_ALO_VISIBILITY_MS",
        DEFAULT_ALO_VISIBILITY_MS,
    ));
    let mut last_boundary = None;
    let mut quiet_since = None;
    let mut visibility_checked = false;
    loop {
        assert_running_nodes(nodes);
        let drained = output
            .drain()
            .unwrap_or_else(|error| panic!("{label}: {error}"));
        if let Some((row, count)) = output
            .observed
            .iter()
            .find(|(row, _)| !expected.contains_key(*row))
        {
            panic!("{label}: unexpected CoreWindow output {row:?} occurred {count} times");
        }
        let complete = validate_core_window_outputs(&output.observed, expected, false).is_ok();
        if complete && !visibility_checked {
            let visibility = frozen_input_at.elapsed();
            assert!(
                visibility <= visibility_slo,
                "{label}: CoreWindow output visibility {visibility:?} exceeded {visibility_slo:?}"
            );
            eprintln!(
                "soak: PROFILE {label} CoreWindow visibility_ms={:.3}",
                visibility.as_secs_f64() * 1_000.0
            );
            visibility_checked = true;
        }
        let boundary = output.high_watermarks();
        let consumed_boundary = boundary
            .as_ref()
            .is_some_and(|boundary| output.consumed_through(boundary));
        if complete
            && consumed_boundary
            && drained == 0
            && boundary.is_some()
            && boundary == last_boundary
        {
            let quiet_since = quiet_since.get_or_insert_with(Instant::now);
            if quiet_since.elapsed() >= OUTPUT_BOUNDARY_STABILITY {
                let records = validate_core_window_outputs(&output.observed, expected, false)
                    .expect("CoreWindow output was validated above");
                eprintln!(
                    "soak: {label} validated {} window rows across {records} ALO records",
                    expected.len()
                );
                return;
            }
        } else {
            quiet_since = None;
        }
        last_boundary = boundary;
        assert!(
            Instant::now() < deadline,
            "{label}: CoreWindow output incomplete: {}",
            match validate_core_window_outputs(&output.observed, expected, false) {
                Ok(records) => format!(
                    "observed {records} valid records but the output boundary did not stabilize"
                ),
                Err(error) => error,
            }
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(feature = "kafka")]
fn expected_mutable_interval_transitions() -> [MutableIntervalSinkState; 4] {
    [
        MutableIntervalSinkState::Live(MutableIntervalAggregateOutput {
            match_count: 1,
            value_sum: MUTABLE_INTERVAL_INITIAL_VALUE,
        }),
        MutableIntervalSinkState::Live(MutableIntervalAggregateOutput {
            match_count: 1,
            value_sum: MUTABLE_INTERVAL_UPDATED_VALUE,
        }),
        MutableIntervalSinkState::Tombstone,
        MutableIntervalSinkState::Live(MutableIntervalAggregateOutput {
            match_count: 1,
            value_sum: MUTABLE_INTERVAL_REVIVED_VALUE,
        }),
    ]
}

#[cfg(feature = "kafka")]
fn validate_mutable_interval_transitions(
    observed: &[MutableIntervalSinkState],
    expected: &[MutableIntervalSinkState],
) -> Result<bool, String> {
    for (index, observed) in observed.iter().enumerate() {
        let Some(expected) = expected.get(index) else {
            return Err(format!(
                "mutable interval sink emitted extra state transition {observed:?} at index {index}"
            ));
        };
        if observed != expected {
            return Err(format!(
                "mutable interval sink transition {index} is {observed:?}, expected {expected:?}"
            ));
        }
    }
    Ok(observed.len() == expected.len())
}

#[cfg(feature = "kafka")]
struct KafkaMutableIntervalOracle {
    consumer: rdkafka::consumer::BaseConsumer,
    topic: String,
    consumed_offsets: Vec<i64>,
    transitions: Vec<MutableIntervalSinkState>,
    records: u64,
}

#[cfg(feature = "kafka")]
impl KafkaMutableIntervalOracle {
    fn new(brokers: &str, topic: &str) -> Self {
        use rdkafka::consumer::Consumer;
        use rdkafka::{Offset, TopicPartitionList};

        let consumer: rdkafka::consumer::BaseConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set(
                "group.id",
                format!("laminardb-soak-mutable-interval-{}", std::process::id()),
            )
            .set("enable.auto.commit", "false")
            .create()
            .expect("Kafka mutable interval oracle consumer");
        let mut assignment = TopicPartitionList::with_capacity(1);
        assignment
            .add_partition_offset(topic, 0, Offset::Beginning)
            .expect("build mutable interval oracle assignment");
        consumer
            .assign(&assignment)
            .expect("assign mutable interval oracle from beginning");
        Self {
            consumer,
            topic: topic.to_owned(),
            consumed_offsets: vec![0],
            transitions: Vec::new(),
            records: 0,
        }
    }

    fn high_watermarks(&self) -> Option<Vec<i64>> {
        kafka_high_watermarks(&self.consumer, &self.topic, OUTPUT_TOPIC_PARTITIONS)
    }

    fn consumed_through(&self, boundary: &[i64]) -> bool {
        self.consumed_offsets.len() == boundary.len()
            && self
                .consumed_offsets
                .iter()
                .zip(boundary)
                .all(|(consumed, boundary)| consumed >= boundary)
    }

    fn drain(&mut self) -> Result<usize, String> {
        use rdkafka::message::Message;

        let mut drained = 0;
        while let Some(result) = self.consumer.poll(Duration::ZERO) {
            let message =
                result.map_err(|error| format!("Kafka mutable interval read failed: {error}"))?;
            if message.topic() != self.topic {
                return Err(format!(
                    "mutable interval oracle read topic {:?}, expected {:?}",
                    message.topic(),
                    self.topic
                ));
            }
            record_consumed_offset(
                &mut self.consumed_offsets,
                message.partition(),
                message.offset(),
            );
            let key = message
                .key()
                .ok_or_else(|| "mutable interval upsert output had a null key".to_owned())?;
            let key = std::str::from_utf8(key)
                .map_err(|error| format!("mutable interval output key is not UTF-8: {error}"))?;
            let key = key.parse::<u64>().map_err(|error| {
                format!("mutable interval output key {key:?} is invalid: {error}")
            })?;
            if key != MUTABLE_INTERVAL_LEFT_ID {
                return Err(format!(
                    "mutable interval output used unexpected left key {key}"
                ));
            }
            let state = match message.payload() {
                None => MutableIntervalSinkState::Tombstone,
                Some(payload) => {
                    let value: serde_json::Value = serde_json::from_slice(payload)
                        .map_err(|error| format!("invalid mutable interval Kafka JSON: {error}"))?;
                    if value.get("__weight").is_some() || value.get("_op").is_some() {
                        return Err(format!(
                            "mutable interval upsert sink leaked changelog metadata: {value}"
                        ));
                    }
                    let left_id = value
                        .get("left_id")
                        .and_then(serde_json::Value::as_u64)
                        .ok_or_else(|| {
                            format!("mutable interval output has no unsigned left_id: {value}")
                        })?;
                    if left_id != MUTABLE_INTERVAL_LEFT_ID {
                        return Err(format!(
                            "mutable interval payload left_id {left_id} differs from Kafka key {key}"
                        ));
                    }
                    let match_count = value
                        .get("match_count")
                        .and_then(serde_json::Value::as_u64)
                        .ok_or_else(|| {
                            format!("mutable interval output has no unsigned match_count: {value}")
                        })?;
                    let value_sum = value
                        .get("value_sum")
                        .and_then(serde_json::Value::as_i64)
                        .ok_or_else(|| {
                            format!("mutable interval output has no signed value_sum: {value}")
                        })?;
                    MutableIntervalSinkState::Live(MutableIntervalAggregateOutput {
                        match_count,
                        value_sum,
                    })
                }
            };
            if self.transitions.last().copied() != Some(state) {
                self.transitions.push(state);
            }
            self.records = self
                .records
                .checked_add(1)
                .ok_or_else(|| "mutable interval output record count overflow".to_owned())?;
            drained += 1;
        }
        Ok(drained)
    }
}

#[cfg(feature = "kafka")]
fn assert_mutable_interval_output_phase(
    nodes: &mut [Node],
    output: &mut KafkaMutableIntervalOracle,
    expected: &[MutableIntervalSinkState],
    frozen_input_at: Option<Instant>,
    window: Duration,
    label: &str,
) {
    let deadline = Instant::now() + window;
    loop {
        assert_running_nodes(nodes);
        output
            .drain()
            .unwrap_or_else(|error| panic!("{label}: {error}"));
        let complete = validate_mutable_interval_transitions(&output.transitions, expected)
            .unwrap_or_else(|error| panic!("{label}: {error}"));
        if complete {
            if let Some(frozen_input_at) = frozen_input_at {
                let visibility = frozen_input_at.elapsed();
                let visibility_slo = Duration::from_millis(env_u64(
                    "LAMINAR_SOAK_ALO_VISIBILITY_MS",
                    DEFAULT_ALO_VISIBILITY_MS,
                ));
                assert!(
                    visibility <= visibility_slo,
                    "{label}: mutable output visibility {visibility:?} exceeded {visibility_slo:?}"
                );
                eprintln!(
                    "soak: PROFILE {label} visibility_ms={:.3}",
                    visibility.as_secs_f64() * 1_000.0,
                );
            }
            eprintln!(
                "soak: {label} observed transition {} across {} ALO records",
                expected.len(),
                output.records,
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{label}: mutable interval output remained at {:?}, expected {:?}",
            output.transitions,
            expected,
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(feature = "kafka")]
fn mutable_interval_logical_output_stable(
    observed: &[MutableIntervalSinkState],
    expected: &[MutableIntervalSinkState],
    physical_boundary_quiet: bool,
    stable_since: &mut Option<Instant>,
    now: Instant,
    stability: Duration,
) -> Result<bool, String> {
    let complete = validate_mutable_interval_transitions(observed, expected)?;
    if !complete || !physical_boundary_quiet {
        *stable_since = None;
        return Ok(false);
    }
    let stable_since = stable_since.get_or_insert(now);
    Ok(now.saturating_duration_since(*stable_since) >= stability)
}

#[cfg(feature = "kafka")]
fn assert_mutable_interval_output_stable(
    nodes: &mut [Node],
    output: &mut KafkaMutableIntervalOracle,
    window: Duration,
    label: &str,
) {
    let expected = expected_mutable_interval_transitions();
    let deadline = Instant::now() + window;
    let mut last_boundary = None;
    let mut stable_since = None;
    loop {
        assert_running_nodes(nodes);
        let drained = output
            .drain()
            .unwrap_or_else(|error| panic!("{label}: {error}"));
        let boundary = output.high_watermarks();
        let caught_up = boundary
            .as_ref()
            .is_some_and(|boundary| output.consumed_through(boundary));
        let physical_boundary_quiet =
            caught_up && drained == 0 && boundary.is_some() && boundary == last_boundary;
        if mutable_interval_logical_output_stable(
            &output.transitions,
            &expected,
            physical_boundary_quiet,
            &mut stable_since,
            Instant::now(),
            OUTPUT_BOUNDARY_STABILITY,
        )
        .unwrap_or_else(|error| panic!("{label}: {error}"))
        {
            eprintln!(
                "soak: {label} validated weighted update, retraction tombstone, and revival across {} ALO records",
                output.records
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{label}: mutable interval output did not stabilize: transitions={:?}, boundary={boundary:?}, consumed={:?}",
            output.transitions,
            output.consumed_offsets,
        );
        last_boundary = boundary;
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(feature = "kafka")]
struct KafkaOutputOracle {
    consumer: rdkafka::consumer::BaseConsumer,
    topic: String,
    partitions: i32,
    consumed_offsets: Vec<i64>,
    seen: BTreeSet<(u64, u64)>,
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

    fn drain(&mut self, expected: &BTreeSet<(u64, u64)>, boundary: &mut [i64]) -> usize {
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
            let boundary_end = boundary
                .get_mut(partition)
                .expect("Kafka output returned an out-of-range partition");
            *boundary_end = (*boundary_end).max(message.offset().saturating_add(1));
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
            let left_id = value
                .get("left_id")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_else(|| {
                    panic!("Kafka output record has no non-negative integer left_id")
                });
            let right_id = value
                .get("right_id")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_else(|| {
                    panic!("Kafka output record has no non-negative integer right_id")
                });
            let pair = (left_id, right_id);
            assert!(
                expected.contains(&pair),
                "Kafka output contains impossible join pair {pair:?}"
            );
            if !self.seen.insert(pair) {
                self.duplicates += 1;
            }
            drained += 1;
        }
        drained
    }

    fn is_complete(&self, expected: &BTreeSet<(u64, u64)>) -> bool {
        self.seen.len() == expected.len()
    }

    fn missing(&self, expected: &BTreeSet<(u64, u64)>) -> Vec<(u64, u64)> {
        expected
            .iter()
            .copied()
            .filter(|pair| !self.seen.contains(pair))
            .take(16)
            .collect()
    }
}

#[cfg(feature = "kafka")]
fn parse_temporal_output(
    kind: TemporalOutputKind,
    value: &serde_json::Value,
) -> Result<TemporalOutput, String> {
    let unsigned = |name: &str| {
        value
            .get(name)
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| format!("temporal output has no non-negative integer {name}: {value}"))
    };
    let left_id = unsigned("left_id")?;
    match kind {
        TemporalOutputKind::LeftAsof => {
            let right_id = match value.get("right_id") {
                Some(field) if field.is_null() => None,
                Some(_) => Some(unsigned("right_id")?),
                None => return Err(format!("temporal ASOF output has no right_id: {value}")),
            };
            Ok(TemporalOutput::LeftAsof { left_id, right_id })
        }
        TemporalOutputKind::InnerProbe => {
            let offset_ms = value
                .get("offset_ms")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| {
                    format!("temporal probe output has no integer offset_ms: {value}")
                })?;
            Ok(TemporalOutput::InnerProbe {
                left_id,
                right_id: unsigned("right_id")?,
                offset_ms,
                probe_time_ms: json_timestamp_ms(value, "probe_time", "temporal probe output")?,
            })
        }
    }
}

#[cfg(feature = "kafka")]
struct KafkaTemporalOracle {
    consumer: rdkafka::consumer::BaseConsumer,
    topic: String,
    kind: TemporalOutputKind,
    consumed_offsets: Vec<i64>,
    seen: BTreeSet<TemporalOutput>,
    duplicates: u64,
}

#[cfg(feature = "kafka")]
impl KafkaTemporalOracle {
    fn new(brokers: &str, topic: &str, kind: TemporalOutputKind) -> Self {
        use rdkafka::consumer::Consumer;
        use rdkafka::{Offset, TopicPartitionList};

        let consumer: rdkafka::consumer::BaseConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set(
                "group.id",
                format!(
                    "laminardb-soak-temporal-oracle-{}-{kind:?}",
                    std::process::id()
                ),
            )
            .set("enable.auto.commit", "false")
            .create()
            .expect("Kafka temporal output oracle consumer");
        let mut assignment = TopicPartitionList::with_capacity(
            usize::try_from(OUTPUT_TOPIC_PARTITIONS).expect("temporal partition count fits usize"),
        );
        for partition in 0..OUTPUT_TOPIC_PARTITIONS {
            assignment
                .add_partition_offset(topic, partition, Offset::Beginning)
                .expect("build temporal output oracle assignment");
        }
        consumer
            .assign(&assignment)
            .expect("assign temporal output oracle from beginning");
        Self {
            consumer,
            topic: topic.to_owned(),
            kind,
            consumed_offsets: vec![
                0;
                usize::try_from(OUTPUT_TOPIC_PARTITIONS)
                    .expect("temporal partition count fits usize")
            ],
            seen: BTreeSet::new(),
            duplicates: 0,
        }
    }

    fn high_watermarks(&self) -> Option<Vec<i64>> {
        kafka_high_watermarks(&self.consumer, &self.topic, OUTPUT_TOPIC_PARTITIONS)
    }

    fn drain(&mut self, expected: &BTreeSet<TemporalOutput>) -> usize {
        use rdkafka::message::Message;

        let mut drained = 0;
        while let Some(result) = self.consumer.poll(Duration::ZERO) {
            let message =
                result.unwrap_or_else(|error| panic!("Kafka temporal output read failed: {error}"));
            assert_eq!(
                message.topic(),
                self.topic,
                "temporal oracle read wrong topic"
            );
            record_consumed_offset(
                &mut self.consumed_offsets,
                message.partition(),
                message.offset(),
            );
            let payload = message
                .payload()
                .expect("Kafka temporal output unexpectedly had a null payload");
            let value: serde_json::Value = serde_json::from_slice(payload)
                .unwrap_or_else(|error| panic!("invalid Kafka temporal output JSON: {error}"));
            let output = parse_temporal_output(self.kind, &value)
                .unwrap_or_else(|error| panic!("invalid Kafka temporal output: {error}"));
            assert!(
                expected.contains(&output),
                "Kafka temporal output contains impossible row {output:?}"
            );
            if !self.seen.insert(output) {
                self.duplicates = self
                    .duplicates
                    .checked_add(1)
                    .expect("Kafka temporal duplicate count overflow");
            }
            drained += 1;
        }
        drained
    }
}

#[cfg(feature = "kafka")]
struct KafkaTemporalExactOutput<'a> {
    oracle: &'a mut KafkaTemporalOracle,
    expected: &'a BTreeSet<TemporalOutput>,
    frozen_input_at: Instant,
    label: &'a str,
}

#[cfg(feature = "kafka")]
fn assert_kafka_temporal_outputs(
    nodes: &mut [Node],
    outputs: &mut [KafkaTemporalExactOutput<'_>],
    window: Duration,
) {
    let deadline = Instant::now() + window;
    let visibility_slo = Duration::from_millis(env_u64(
        "LAMINAR_SOAK_ALO_VISIBILITY_MS",
        DEFAULT_ALO_VISIBILITY_MS,
    ));
    let mut boundaries = outputs
        .iter()
        .map(|_| {
            vec![
                0;
                usize::try_from(OUTPUT_TOPIC_PARTITIONS)
                    .expect("temporal partition count fits usize")
            ]
        })
        .collect::<Vec<_>>();
    let mut quiet_since = vec![None; outputs.len()];
    let mut visible = vec![false; outputs.len()];
    loop {
        assert_running_nodes(nodes);
        let mut complete = true;
        for (index, output) in outputs.iter_mut().enumerate() {
            let Some(current) = output.oracle.high_watermarks() else {
                complete = false;
                continue;
            };
            if current != boundaries[index] {
                boundaries[index] = current;
                quiet_since[index] = None;
            }
            let drained = output.oracle.drain(output.expected);
            let stable = output
                .oracle
                .high_watermarks()
                .is_some_and(|current| current == boundaries[index]);
            let consumed = output
                .oracle
                .consumed_offsets
                .iter()
                .zip(&boundaries[index])
                .all(|(consumed, boundary)| consumed >= boundary);
            let exact = output.oracle.seen == *output.expected;
            if stable && consumed && exact {
                if !visible[index] {
                    let visibility =
                        Instant::now().saturating_duration_since(output.frozen_input_at);
                    assert!(
                        visibility <= visibility_slo,
                        "{} frozen input cut took {visibility:?} to become visible in Kafka; SLO is {visibility_slo:?}",
                        output.label
                    );
                    eprintln!(
                        "soak: PROFILE {} exact Kafka visibility_ms={:.3} rows={}",
                        output.label,
                        visibility.as_secs_f64() * 1_000.0,
                        output.oracle.seen.len()
                    );
                    visible[index] = true;
                }
                if drained == 0 {
                    let quiet = quiet_since[index].get_or_insert_with(Instant::now);
                    complete &= quiet.elapsed() >= OUTPUT_BOUNDARY_STABILITY;
                } else {
                    quiet_since[index] = None;
                    complete = false;
                }
            } else {
                quiet_since[index] = None;
                complete = false;
            }
        }
        if complete {
            for output in outputs.iter() {
                eprintln!(
                    "soak: {} observed {} exact rows with {} ALO duplicates",
                    output.label,
                    output.oracle.seen.len(),
                    output.oracle.duplicates
                );
            }
            return;
        }
        assert!(
            Instant::now() < deadline,
            "temporal Kafka outputs did not reach exact stable results: {:?}",
            outputs
                .iter()
                .map(|output| {
                    let missing = output
                        .expected
                        .difference(&output.oracle.seen)
                        .take(8)
                        .copied()
                        .collect::<Vec<_>>();
                    (output.label, missing)
                })
                .collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(feature = "kafka")]
fn assert_final_outputs(
    nodes: &mut [Node],
    output: &mut KafkaOutputOracle,
    produced_count: u64,
    expected: &BTreeSet<(u64, u64)>,
    mut output_boundary: Vec<i64>,
    window: Duration,
) {
    assert!(produced_count > 0, "soak producer emitted no input records");
    assert!(!expected.is_empty(), "join oracle expected no output pairs");
    let start = Instant::now();
    let mut quiet_since = None;
    while start.elapsed() < window {
        assert_running_nodes(nodes);
        let Some(current_output) = output.high_watermarks() else {
            std::thread::sleep(Duration::from_millis(100));
            continue;
        };
        if current_output != output_boundary {
            output_boundary = current_output;
            quiet_since = None;
        }
        let drained = output.drain(expected, &mut output_boundary);
        let boundaries_stable = output
            .high_watermarks()
            .is_some_and(|current| current == output_boundary);
        if boundaries_stable
            && output.consumed_through(&output_boundary)
            && output.is_complete(expected)
        {
            if drained == 0 {
                let quiet_since = quiet_since.get_or_insert_with(Instant::now);
                if quiet_since.elapsed() >= OUTPUT_BOUNDARY_STABILITY {
                    assert_eq!(
                        output.high_watermarks().as_deref(),
                        Some(output_boundary.as_slice()),
                        "Kafka output boundary changed during final drain"
                    );
                    eprintln!(
                        "soak: output oracle consumed the frozen broker boundary and observed all {} pairs from {produced_count} logical input IDs with {} at-least-once duplicates",
                        expected.len(),
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
    let _ = output.drain(expected, &mut output_boundary);
    assert!(
        output.consumed_through(&output_boundary),
        "soak: output oracle did not consume through frozen boundary {output_boundary:?}; consumed {:?}",
        output.consumed_offsets
    );
    if !output.is_complete(expected) {
        let missing = output.missing(expected);
        panic!(
            "soak: output oracle saw {}/{} pairs ({} duplicates); first missing pairs: {missing:?}",
            output.seen.len(),
            expected.len(),
            output.duplicates
        );
    }
    panic!(
        "soak: frozen Kafka output boundaries did not remain drained and stable for {OUTPUT_BOUNDARY_STABILITY:?}"
    );
}

#[cfg(feature = "kafka")]
fn assert_frozen_kafka_outputs(
    nodes: &mut [Node],
    output: &mut KafkaOutputOracle,
    produced_count: u64,
    expected: &BTreeSet<(u64, u64)>,
    window: Duration,
    label: &str,
) {
    let deadline = Instant::now() + window;
    let mut boundary = None;
    wait_for(
        &format!("{label} Kafka output high-watermark snapshot"),
        remaining_progress_window(deadline, label),
        || {
            assert_running_nodes(nodes);
            boundary = output.high_watermarks();
            boundary.is_some()
        },
    );
    assert_final_outputs(
        nodes,
        output,
        produced_count,
        expected,
        boundary.expect("Kafka output boundary wait completed without a value"),
        remaining_progress_window(deadline, label),
    );
}

#[cfg(feature = "kafka")]
struct KafkaMatrixOracle {
    consumer: rdkafka::consumer::BaseConsumer,
    topic: String,
    consumed_offsets: Vec<i64>,
    seen: BTreeSet<MatrixOutput>,
    duplicates: u64,
}

#[cfg(feature = "kafka")]
impl KafkaMatrixOracle {
    fn new(brokers: &str, topic: &str) -> Self {
        use rdkafka::consumer::Consumer;
        use rdkafka::{Offset, TopicPartitionList};

        let consumer: rdkafka::consumer::BaseConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set(
                "group.id",
                format!("laminardb-soak-matrix-oracle-{}", std::process::id()),
            )
            .set("enable.auto.commit", "false")
            .create()
            .expect("Kafka matrix oracle consumer");
        let mut assignment = TopicPartitionList::with_capacity(
            usize::try_from(OUTPUT_TOPIC_PARTITIONS).expect("matrix partition count fits usize"),
        );
        for partition in 0..OUTPUT_TOPIC_PARTITIONS {
            assignment
                .add_partition_offset(topic, partition, Offset::Beginning)
                .expect("build matrix oracle assignment");
        }
        consumer
            .assign(&assignment)
            .expect("assign matrix oracle from beginning");
        Self {
            consumer,
            topic: topic.to_owned(),
            consumed_offsets: vec![
                0;
                usize::try_from(OUTPUT_TOPIC_PARTITIONS)
                    .expect("matrix partition count fits usize")
            ],
            seen: BTreeSet::new(),
            duplicates: 0,
        }
    }

    fn high_watermarks(&self) -> Option<Vec<i64>> {
        kafka_high_watermarks(&self.consumer, &self.topic, OUTPUT_TOPIC_PARTITIONS)
    }

    fn drain(&mut self, expected: &BTreeSet<MatrixOutput>, boundary: &[i64]) -> usize {
        use rdkafka::message::Message;

        let mut drained = 0;
        while let Some(result) = self.consumer.poll(Duration::ZERO) {
            let message =
                result.unwrap_or_else(|error| panic!("Kafka matrix read failed: {error}"));
            let partition = usize::try_from(message.partition())
                .expect("Kafka matrix output returned a negative partition");
            let frozen_end = *boundary
                .get(partition)
                .expect("Kafka matrix output returned an out-of-range partition");
            assert!(
                message.offset() < frozen_end,
                "Kafka matrix output appended after frozen boundary: partition={}, offset={}, boundary={frozen_end}",
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
                .expect("Kafka matrix output unexpectedly had a null payload");
            let value: serde_json::Value = serde_json::from_slice(payload)
                .unwrap_or_else(|error| panic!("invalid Kafka matrix JSON: {error}"));
            let join_case = value
                .get("join_case")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| panic!("Kafka matrix output has no string join_case: {value}"));
            let nullable_id = |name| {
                let field = value
                    .get(name)
                    .unwrap_or_else(|| panic!("Kafka matrix output has no {name}: {value}"));
                if field.is_null() {
                    None
                } else {
                    Some(field.as_i64().unwrap_or_else(|| {
                        panic!("Kafka matrix output {name} is not a signed integer: {value}")
                    }))
                }
            };
            let row = MatrixOutput {
                join_case: join_case.to_owned(),
                left_id: nullable_id("left_id"),
                right_id: nullable_id("right_id"),
            };
            assert!(
                expected.contains(&row),
                "Kafka matrix output contains impossible row {row:?}"
            );
            if !self.seen.insert(row) {
                self.duplicates = self
                    .duplicates
                    .checked_add(1)
                    .expect("Kafka matrix duplicate count overflow");
            }
            drained += 1;
        }
        drained
    }
}

#[cfg(feature = "kafka")]
fn assert_kafka_matrix_outputs(
    nodes: &mut [Node],
    output: &mut KafkaMatrixOracle,
    window: Duration,
    label: &str,
) {
    let expected = expected_matrix_outputs();
    let deadline = Instant::now() + window;
    let mut boundary = None;
    wait_for(
        &format!("{label}: Kafka matrix output boundary"),
        remaining_progress_window(deadline, label),
        || {
            assert_running_nodes(nodes);
            boundary = output.high_watermarks();
            boundary.is_some()
        },
    );
    let boundary = boundary.expect("matrix boundary wait completed without a value");
    let mut quiet_since = None;
    while remaining_at(deadline, Instant::now()).is_some() {
        assert_running_nodes(nodes);
        assert_eq!(
            output.high_watermarks().as_deref(),
            Some(boundary.as_slice()),
            "{label}: matrix output boundary changed after the durable input cut"
        );
        let drained = output.drain(&expected, &boundary);
        let consumed = output
            .consumed_offsets
            .iter()
            .zip(&boundary)
            .all(|(consumed, boundary)| consumed >= boundary);
        if consumed && output.seen == expected {
            if drained == 0 {
                let quiet_since = quiet_since.get_or_insert_with(Instant::now);
                if quiet_since.elapsed() >= OUTPUT_BOUNDARY_STABILITY {
                    eprintln!(
                        "soak: {label} observed all {} bounded-join matrix rows with {} ALO duplicates",
                        expected.len(),
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
    let missing = expected
        .difference(&output.seen)
        .take(16)
        .cloned()
        .collect::<Vec<_>>();
    panic!(
        "{label}: Kafka matrix saw {}/{} rows ({} duplicates); first missing {missing:?}",
        output.seen.len(),
        expected.len(),
        output.duplicates
    );
}

#[cfg(feature = "kafka")]
struct KafkaMatrixAggregateOracle {
    consumer: rdkafka::consumer::BaseConsumer,
    topic: String,
    consumed_offsets: Vec<i64>,
    latest: BTreeMap<String, MatrixAggregateOutput>,
    records: u64,
}

#[cfg(feature = "kafka")]
impl KafkaMatrixAggregateOracle {
    fn new(brokers: &str, topic: &str) -> Self {
        use rdkafka::consumer::Consumer;
        use rdkafka::{Offset, TopicPartitionList};

        let consumer: rdkafka::consumer::BaseConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set(
                "group.id",
                format!("laminardb-soak-matrix-aggregate-{}", std::process::id()),
            )
            .set("enable.auto.commit", "false")
            .create()
            .expect("Kafka matrix aggregate oracle consumer");
        let mut assignment = TopicPartitionList::with_capacity(
            usize::try_from(OUTPUT_TOPIC_PARTITIONS).expect("aggregate partition count fits usize"),
        );
        for partition in 0..OUTPUT_TOPIC_PARTITIONS {
            assignment
                .add_partition_offset(topic, partition, Offset::Beginning)
                .expect("build matrix aggregate oracle assignment");
        }
        consumer
            .assign(&assignment)
            .expect("assign matrix aggregate oracle from beginning");
        Self {
            consumer,
            topic: topic.to_owned(),
            consumed_offsets: vec![
                0;
                usize::try_from(OUTPUT_TOPIC_PARTITIONS)
                    .expect("aggregate partition count fits usize")
            ],
            latest: BTreeMap::new(),
            records: 0,
        }
    }

    fn high_watermarks(&self) -> Option<Vec<i64>> {
        kafka_high_watermarks(&self.consumer, &self.topic, OUTPUT_TOPIC_PARTITIONS)
    }

    fn consumed_through(&self, boundary: &[i64]) -> bool {
        self.consumed_offsets.len() == boundary.len()
            && self
                .consumed_offsets
                .iter()
                .zip(boundary)
                .all(|(consumed, boundary)| consumed >= boundary)
    }

    fn drain(&mut self) -> Result<usize, String> {
        use rdkafka::message::Message;

        let mut drained = 0;
        while let Some(result) = self.consumer.poll(Duration::ZERO) {
            let message =
                result.map_err(|error| format!("Kafka aggregate read failed: {error}"))?;
            if message.topic() != self.topic {
                return Err(format!(
                    "matrix aggregate oracle read topic {:?}, expected {:?}",
                    message.topic(),
                    self.topic
                ));
            }
            record_consumed_offset(
                &mut self.consumed_offsets,
                message.partition(),
                message.offset(),
            );
            let key = message
                .key()
                .ok_or_else(|| "Kafka aggregate upsert output had a null key".to_owned())?;
            let key = std::str::from_utf8(key)
                .map_err(|error| format!("matrix aggregate key is not UTF-8: {error}"))?;
            let payload = message
                .payload()
                .ok_or_else(|| "Kafka aggregate unexpectedly deleted a live group".to_owned())?;
            let value: serde_json::Value = serde_json::from_slice(payload)
                .map_err(|error| format!("invalid Kafka aggregate JSON: {error}"))?;
            if value.get("__weight").is_some() || value.get("_op").is_some() {
                return Err(format!(
                    "matrix aggregate upsert sink leaked changelog metadata: {value}"
                ));
            }
            let (join_case, aggregate) = decode_matrix_aggregate(&value)?;
            if key != join_case {
                return Err(format!(
                    "matrix aggregate Kafka key {key:?} differs from payload case {join_case:?}"
                ));
            }
            self.latest.insert(join_case, aggregate);
            self.records = self
                .records
                .checked_add(1)
                .ok_or_else(|| "matrix aggregate record count overflow".to_owned())?;
            drained += 1;
        }
        Ok(drained)
    }
}

#[cfg(feature = "kafka")]
fn assert_matrix_aggregate_final_state_pending(
    output: &mut KafkaMatrixAggregateOracle,
    label: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_boundary = None;
    let mut quiet_since = None;
    loop {
        let drained = output
            .drain()
            .unwrap_or_else(|error| panic!("{label}: {error}"));
        let boundary = output.high_watermarks();
        let caught_up = boundary
            .as_ref()
            .is_some_and(|boundary| output.consumed_through(boundary));
        if caught_up && drained == 0 && boundary.is_some() && boundary == last_boundary {
            let quiet_since = quiet_since.get_or_insert_with(Instant::now);
            if quiet_since.elapsed() >= OUTPUT_BOUNDARY_STABILITY {
                break;
            }
        } else {
            quiet_since = None;
        }
        last_boundary = boundary;
        assert!(
            Instant::now() < deadline,
            "{label}: aggregate output did not reach a stable pre-fault Kafka boundary; boundary={last_boundary:?}, consumed={:?}",
            output.consumed_offsets,
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let expected = expected_matrix_aggregates();
    if let Some(join_case) = output
        .latest
        .keys()
        .find(|join_case| !expected.contains_key(*join_case))
    {
        panic!("{label}: unknown bounded-join aggregate case {join_case:?}");
    }
    let prematurely_final = output
        .latest
        .iter()
        .filter(|(join_case, state)| expected.get(*join_case) == Some(*state))
        .collect::<Vec<_>>();
    assert!(
        prematurely_final.is_empty(),
        "{label}: aggregate cases reached their post-fault final state before completion: {prematurely_final:?}"
    );
}

#[cfg(feature = "kafka")]
fn assert_kafka_matrix_aggregates(
    nodes: &mut [Node],
    output: &mut KafkaMatrixAggregateOracle,
    window: Duration,
    label: &str,
) {
    let deadline = Instant::now() + window;
    let expected = expected_matrix_aggregates();
    let mut last_boundary = None;
    let mut quiet_since = None;
    loop {
        assert_running_nodes(nodes);
        let drained = output
            .drain()
            .unwrap_or_else(|error| panic!("{label}: {error}"));
        let observation = validate_matrix_aggregate_latest(&output.latest, &expected);
        let boundary = output.high_watermarks();
        let caught_up = boundary
            .as_ref()
            .is_some_and(|boundary| output.consumed_through(boundary));
        if observation.is_ok()
            && caught_up
            && drained == 0
            && boundary.is_some()
            && boundary == last_boundary
        {
            let quiet_since = quiet_since.get_or_insert_with(Instant::now);
            if quiet_since.elapsed() >= OUTPUT_BOUNDARY_STABILITY {
                eprintln!(
                    "soak: {label} validated all {} differential join-to-aggregate pipelines across {} Kafka upserts",
                    BOUNDED_JOIN_SOAK_CASES.len(),
                    output.records,
                );
                return;
            }
        } else {
            quiet_since = None;
        }
        last_boundary = boundary;
        let observation = observation.err().unwrap_or_else(|| {
            "final states are present but the Kafka boundary is not yet stable".to_owned()
        });
        assert!(
            Instant::now() < deadline,
            "{label}: aggregate output did not stabilize at the exact final states: {}; boundary={last_boundary:?}, consumed={:?}",
            observation,
            output.consumed_offsets,
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
#[derive(Debug)]
struct DeltaJoinSnapshot {
    version: u64,
    observed_at: Instant,
    rows: usize,
    pairs: BTreeSet<(u64, u64)>,
    duplicate_rows: usize,
    first_duplicate: Option<(u64, u64)>,
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
struct OpenDeltaJoinSnapshot {
    table: deltalake::DeltaTable,
    version: u64,
    observed_at: Instant,
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
#[derive(Clone, Debug)]
struct DeltaMatrixSnapshot {
    version: u64,
    rows: usize,
    outputs: BTreeSet<MatrixOutput>,
    duplicate_rows: usize,
    first_duplicate: Option<MatrixOutput>,
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
#[derive(Clone, Debug)]
struct DeltaTemporalSnapshot {
    version: u64,
    rows: usize,
    outputs: BTreeSet<TemporalOutput>,
    duplicate_rows: usize,
    first_duplicate: Option<TemporalOutput>,
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
#[derive(Clone, Debug)]
struct DeltaCoreWindowSnapshot {
    version: u64,
    outputs: BTreeMap<CoreWindowOutput, u64>,
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
struct DeltaOutputOracle {
    table_uri: String,
    storage_options: HashMap<String, String>,
    join_table: tokio::sync::Mutex<Option<deltalake::DeltaTable>>,
    runtime: tokio::runtime::Runtime,
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
impl DeltaOutputOracle {
    fn new(table_uri: String, storage: &DeltaSoakStorage) -> Self {
        Self {
            table_uri,
            storage_options: storage.options(),
            join_table: tokio::sync::Mutex::new(None),
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Delta output oracle runtime"),
        }
    }

    async fn refresh_join_table(&self) -> Result<deltalake::DeltaTable, String> {
        // INVARIANT: Metadata refreshes for one oracle are serialized so every returned clone
        // retains one immutable version while later observations reuse the object-store client.
        let mut cached = self.join_table.lock().await;
        if let Some(table) = cached.as_mut() {
            let latest = table
                .get_latest_version()
                .await
                .map_err(|error| format!("read latest Delta output version: {error}"))?;
            if table.version() != Some(latest) {
                table
                    .load_version(latest)
                    .await
                    .map_err(|error| format!("load Delta output version {latest}: {error}"))?;
            }
            return Ok(table.clone());
        }

        let uri = deltalake::ensure_table_uri(&self.table_uri)
            .map_err(|error| format!("invalid Delta table URI: {error}"))?;
        let table = deltalake::open_table_with_storage_options(uri, self.storage_options.clone())
            .await
            .map_err(|error| format!("open Delta output: {error}"))?;
        *cached = Some(table.clone());
        Ok(table)
    }

    fn open_join_snapshot(&self) -> Result<OpenDeltaJoinSnapshot, String> {
        self.runtime.block_on(async {
            let table = self.refresh_join_table().await?;
            let version = table
                .version()
                .ok_or_else(|| "Delta output has no committed table version".to_owned())?;
            Ok(OpenDeltaJoinSnapshot {
                table,
                version,
                observed_at: Instant::now(),
            })
        })
    }

    fn snapshot(&self) -> Result<DeltaJoinSnapshot, String> {
        let opened = self.open_join_snapshot()?;
        self.scan_join_snapshot(&opened)
    }

    fn scan_join_snapshot(
        &self,
        opened: &OpenDeltaJoinSnapshot,
    ) -> Result<DeltaJoinSnapshot, String> {
        self.runtime.block_on(async {
            let context = deltalake::datafusion::prelude::SessionContext::new();
            opened
                .table
                .update_datafusion_session(&context.state())
                .map_err(|error| format!("register Delta object store: {error}"))?;
            let provider = opened
                .table
                .table_provider()
                .build()
                .await
                .map_err(|error| format!("build Delta table provider: {error}"))?;
            context
                .register_table("soak_delta_output", Arc::new(provider))
                .map_err(|error| format!("register Delta output table: {error}"))?;
            let batches = context
                .sql("SELECT left_id, right_id FROM soak_delta_output")
                .await
                .map_err(|error| format!("plan Delta output scan: {error}"))?
                .collect()
                .await
                .map_err(|error| format!("scan Delta output: {error}"))?;

            let mut rows = 0usize;
            let mut pairs = BTreeSet::new();
            let mut duplicate_rows = 0usize;
            let mut first_duplicate = None;
            for batch in batches {
                let left_index = batch
                    .schema()
                    .index_of("left_id")
                    .map_err(|error| format!("Delta output left_id column: {error}"))?;
                let right_index = batch
                    .schema()
                    .index_of("right_id")
                    .map_err(|error| format!("Delta output right_id column: {error}"))?;
                let left = arrow_cast::cast(batch.column(left_index), &DataType::Int64)
                    .map_err(|error| format!("cast Delta left_id: {error}"))?;
                let right = arrow_cast::cast(batch.column(right_index), &DataType::Int64)
                    .map_err(|error| format!("cast Delta right_id: {error}"))?;
                let left = left
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "Delta left_id did not cast to Int64".to_owned())?;
                let right = right
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "Delta right_id did not cast to Int64".to_owned())?;
                for row in 0..batch.num_rows() {
                    if left.is_null(row) || right.is_null(row) {
                        return Err("Delta output contains a null join ID".to_owned());
                    }
                    let pair = (
                        u64::try_from(left.value(row)).map_err(|_| {
                            format!("Delta output has negative left_id at row {rows}")
                        })?,
                        u64::try_from(right.value(row)).map_err(|_| {
                            format!("Delta output has negative right_id at row {rows}")
                        })?,
                    );
                    rows = rows
                        .checked_add(1)
                        .ok_or_else(|| "Delta output row count overflow".to_owned())?;
                    if !pairs.insert(pair) {
                        duplicate_rows = duplicate_rows
                            .checked_add(1)
                            .ok_or_else(|| "Delta duplicate count overflow".to_owned())?;
                        first_duplicate.get_or_insert(pair);
                    }
                }
            }
            Ok(DeltaJoinSnapshot {
                version: opened.version,
                observed_at: opened.observed_at,
                rows,
                pairs,
                duplicate_rows,
                first_duplicate,
            })
        })
    }

    fn temporal_snapshot(&self, kind: TemporalOutputKind) -> Result<DeltaTemporalSnapshot, String> {
        self.runtime.block_on(async {
            let uri = deltalake::ensure_table_uri(&self.table_uri)
                .map_err(|error| format!("invalid Delta temporal table URI: {error}"))?;
            let table =
                deltalake::open_table_with_storage_options(uri, self.storage_options.clone())
                    .await
                    .map_err(|error| format!("open Delta temporal output: {error}"))?;
            let version = table
                .version()
                .ok_or_else(|| "Delta temporal output has no committed table version".to_owned())?;
            let context = deltalake::datafusion::prelude::SessionContext::new();
            table
                .update_datafusion_session(&context.state())
                .map_err(|error| format!("register Delta temporal object store: {error}"))?;
            let provider = table
                .table_provider()
                .build()
                .await
                .map_err(|error| format!("build Delta temporal table provider: {error}"))?;
            context
                .register_table("soak_delta_temporal", Arc::new(provider))
                .map_err(|error| format!("register Delta temporal table: {error}"))?;
            let projection = match kind {
                TemporalOutputKind::LeftAsof => "left_id, right_id",
                TemporalOutputKind::InnerProbe => "left_id, right_id, offset_ms, probe_time",
            };
            let batches = context
                .sql(&format!("SELECT {projection} FROM soak_delta_temporal"))
                .await
                .map_err(|error| format!("plan Delta temporal scan: {error}"))?
                .collect()
                .await
                .map_err(|error| format!("scan Delta temporal output: {error}"))?;

            let mut rows = 0usize;
            let mut outputs = BTreeSet::new();
            let mut duplicate_rows = 0usize;
            let mut first_duplicate = None;
            for batch in batches {
                let cast_i64 = |name: &str| -> Result<arrow_array::ArrayRef, String> {
                    let index = batch
                        .schema()
                        .index_of(name)
                        .map_err(|error| format!("Delta temporal {name} column: {error}"))?;
                    arrow_cast::cast(batch.column(index), &DataType::Int64)
                        .map_err(|error| format!("cast Delta temporal {name}: {error}"))
                };
                let left = cast_i64("left_id")?;
                let right = cast_i64("right_id")?;
                let left = left
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "Delta temporal left_id did not cast to Int64".to_owned())?;
                let right = right
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "Delta temporal right_id did not cast to Int64".to_owned())?;
                let probe_columns = if kind == TemporalOutputKind::InnerProbe {
                    let offset = cast_i64("offset_ms")?;
                    let offset = offset
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| "Delta temporal offset_ms did not cast to Int64".to_owned())?
                        .clone();
                    let probe_index = batch
                        .schema()
                        .index_of("probe_time")
                        .map_err(|error| format!("Delta temporal probe_time column: {error}"))?;
                    let probe = arrow_cast::cast(
                        batch.column(probe_index),
                        &DataType::Timestamp(TimeUnit::Millisecond, None),
                    )
                    .map_err(|error| format!("cast Delta temporal probe_time: {error}"))?;
                    let probe = probe
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| {
                            "Delta temporal probe_time did not cast to Timestamp(ms)".to_owned()
                        })?
                        .clone();
                    Some((offset, probe))
                } else {
                    None
                };
                for row in 0..batch.num_rows() {
                    if left.is_null(row) {
                        return Err("Delta temporal output contains a null left_id".to_owned());
                    }
                    let left_id = u64::try_from(left.value(row)).map_err(|_| {
                        format!("Delta temporal output has negative left_id at row {rows}")
                    })?;
                    let output = match kind {
                        TemporalOutputKind::LeftAsof => TemporalOutput::LeftAsof {
                            left_id,
                            right_id: if right.is_null(row) {
                                None
                            } else {
                                Some(u64::try_from(right.value(row)).map_err(|_| {
                                    format!(
                                        "Delta temporal output has negative right_id at row {rows}"
                                    )
                                })?)
                            },
                        },
                        TemporalOutputKind::InnerProbe => {
                            let (offset, probe) = probe_columns
                                .as_ref()
                                .expect("probe columns are present for probe output");
                            if right.is_null(row) || offset.is_null(row) || probe.is_null(row) {
                                return Err(
                                    "Delta inner temporal probe contains a null output".to_owned()
                                );
                            }
                            TemporalOutput::InnerProbe {
                                left_id,
                                right_id: u64::try_from(right.value(row)).map_err(|_| {
                                    format!(
                                        "Delta temporal output has negative right_id at row {rows}"
                                    )
                                })?,
                                offset_ms: offset.value(row),
                                probe_time_ms: probe.value(row),
                            }
                        }
                    };
                    rows = rows
                        .checked_add(1)
                        .ok_or_else(|| "Delta temporal output row count overflow".to_owned())?;
                    if !outputs.insert(output) {
                        duplicate_rows = duplicate_rows
                            .checked_add(1)
                            .ok_or_else(|| "Delta temporal duplicate count overflow".to_owned())?;
                        first_duplicate.get_or_insert(output);
                    }
                }
            }
            Ok(DeltaTemporalSnapshot {
                version,
                rows,
                outputs,
                duplicate_rows,
                first_duplicate,
            })
        })
    }

    fn matrix_snapshot(&self) -> Result<DeltaMatrixSnapshot, String> {
        self.runtime.block_on(async {
            let uri = deltalake::ensure_table_uri(&self.table_uri)
                .map_err(|error| format!("invalid Delta matrix table URI: {error}"))?;
            let table =
                deltalake::open_table_with_storage_options(uri, self.storage_options.clone())
                    .await
                    .map_err(|error| format!("open Delta matrix output: {error}"))?;
            let version = table
                .version()
                .ok_or_else(|| "Delta matrix output has no committed table version".to_owned())?;
            let context = deltalake::datafusion::prelude::SessionContext::new();
            table
                .update_datafusion_session(&context.state())
                .map_err(|error| format!("register Delta matrix object store: {error}"))?;
            let provider = table
                .table_provider()
                .build()
                .await
                .map_err(|error| format!("build Delta matrix table provider: {error}"))?;
            context
                .register_table("soak_delta_matrix", Arc::new(provider))
                .map_err(|error| format!("register Delta matrix table: {error}"))?;
            let batches = context
                .sql("SELECT join_case, left_id, right_id FROM soak_delta_matrix")
                .await
                .map_err(|error| format!("plan Delta matrix scan: {error}"))?
                .collect()
                .await
                .map_err(|error| format!("scan Delta matrix output: {error}"))?;

            let mut rows = 0usize;
            let mut outputs = BTreeSet::new();
            let mut duplicate_rows = 0usize;
            let mut first_duplicate = None;
            for batch in batches {
                let join_case_index = batch
                    .schema()
                    .index_of("join_case")
                    .map_err(|error| format!("Delta matrix join_case column: {error}"))?;
                let left_index = batch
                    .schema()
                    .index_of("left_id")
                    .map_err(|error| format!("Delta matrix left_id column: {error}"))?;
                let right_index = batch
                    .schema()
                    .index_of("right_id")
                    .map_err(|error| format!("Delta matrix right_id column: {error}"))?;
                let join_case = arrow_cast::cast(batch.column(join_case_index), &DataType::Utf8)
                    .map_err(|error| format!("cast Delta matrix join_case: {error}"))?;
                let left = arrow_cast::cast(batch.column(left_index), &DataType::Int64)
                    .map_err(|error| format!("cast Delta matrix left_id: {error}"))?;
                let right = arrow_cast::cast(batch.column(right_index), &DataType::Int64)
                    .map_err(|error| format!("cast Delta matrix right_id: {error}"))?;
                let join_case = join_case
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .ok_or_else(|| "Delta matrix join_case did not cast to Utf8".to_owned())?;
                let left = left
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "Delta matrix left_id did not cast to Int64".to_owned())?;
                let right = right
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "Delta matrix right_id did not cast to Int64".to_owned())?;
                for row in 0..batch.num_rows() {
                    if join_case.is_null(row) {
                        return Err("Delta matrix output contains a null join_case".to_owned());
                    }
                    let output = MatrixOutput {
                        join_case: join_case.value(row).to_owned(),
                        left_id: (!left.is_null(row)).then(|| left.value(row)),
                        right_id: (!right.is_null(row)).then(|| right.value(row)),
                    };
                    rows = rows
                        .checked_add(1)
                        .ok_or_else(|| "Delta matrix row count overflow".to_owned())?;
                    if !outputs.insert(output.clone()) {
                        duplicate_rows = duplicate_rows
                            .checked_add(1)
                            .ok_or_else(|| "Delta matrix duplicate count overflow".to_owned())?;
                        first_duplicate.get_or_insert(output);
                    }
                }
            }
            Ok(DeltaMatrixSnapshot {
                version,
                rows,
                outputs,
                duplicate_rows,
                first_duplicate,
            })
        })
    }

    fn aggregate_rows(&self) -> Result<(u64, DeltaAggregateRows), String> {
        self.runtime.block_on(async {
            let uri = deltalake::ensure_table_uri(&self.table_uri)
                .map_err(|error| format!("invalid Delta aggregate table URI: {error}"))?;
            let table =
                deltalake::open_table_with_storage_options(uri, self.storage_options.clone())
                    .await
                    .map_err(|error| format!("open Delta aggregate output: {error}"))?;
            let version = table
                .version()
                .ok_or_else(|| "Delta aggregate output has no committed version".to_owned())?;
            let context = deltalake::datafusion::prelude::SessionContext::new();
            table
                .update_datafusion_session(&context.state())
                .map_err(|error| format!("register Delta aggregate object store: {error}"))?;
            let provider = table
                .table_provider()
                .build()
                .await
                .map_err(|error| format!("build Delta aggregate table provider: {error}"))?;
            context
                .register_table("soak_delta_aggregate", Arc::new(provider))
                .map_err(|error| format!("register Delta aggregate table: {error}"))?;
            let batches = context
                .sql(
                    "SELECT join_case, row_count, left_count, right_count, left_sum, right_sum, \
                     \"__weight\" \
                     FROM soak_delta_aggregate",
                )
                .await
                .map_err(|error| format!("plan Delta aggregate scan: {error}"))?
                .collect()
                .await
                .map_err(|error| format!("scan Delta aggregate output: {error}"))?;

            let mut rows = Vec::new();
            for batch in batches {
                let cast_i64 = |name: &str| -> Result<arrow_array::ArrayRef, String> {
                    let index = batch
                        .schema()
                        .index_of(name)
                        .map_err(|error| format!("Delta aggregate {name} column: {error}"))?;
                    arrow_cast::cast(batch.column(index), &DataType::Int64)
                        .map_err(|error| format!("cast Delta aggregate {name}: {error}"))
                };
                let join_case_index = batch
                    .schema()
                    .index_of("join_case")
                    .map_err(|error| format!("Delta aggregate join_case column: {error}"))?;
                let join_case = arrow_cast::cast(batch.column(join_case_index), &DataType::Utf8)
                    .map_err(|error| format!("cast Delta aggregate join_case: {error}"))?;
                let join_case = join_case
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .ok_or_else(|| "Delta aggregate join_case did not cast to Utf8".to_owned())?;
                let row_count = cast_i64("row_count")?;
                let left_count = cast_i64("left_count")?;
                let right_count = cast_i64("right_count")?;
                let left_sum = cast_i64("left_sum")?;
                let right_sum = cast_i64("right_sum")?;
                let weight = cast_i64("__weight")?;
                let row_count = row_count
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "Delta aggregate row_count did not cast to Int64".to_owned())?;
                let left_count = left_count
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "Delta aggregate left_count did not cast to Int64".to_owned())?;
                let right_count = right_count
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        "Delta aggregate right_count did not cast to Int64".to_owned()
                    })?;
                let left_sum = left_sum
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "Delta aggregate left_sum did not cast to Int64".to_owned())?;
                let right_sum = right_sum
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "Delta aggregate right_sum did not cast to Int64".to_owned())?;
                let weight = weight
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "Delta aggregate __weight did not cast to Int64".to_owned())?;
                for row in 0..batch.num_rows() {
                    if join_case.is_null(row)
                        || row_count.is_null(row)
                        || left_count.is_null(row)
                        || right_count.is_null(row)
                        || weight.is_null(row)
                    {
                        return Err(
                            "Delta aggregate contains a null key, count, or weight".to_owned()
                        );
                    }
                    let non_negative = |value: i64, field: &str| {
                        u64::try_from(value).map_err(|_| {
                            format!("Delta aggregate contains a negative {field}: {value}")
                        })
                    };
                    rows.push((
                        join_case.value(row).to_owned(),
                        MatrixAggregateOutput {
                            row_count: non_negative(row_count.value(row), "row_count")?,
                            left_count: non_negative(left_count.value(row), "left_count")?,
                            right_count: non_negative(right_count.value(row), "right_count")?,
                            left_sum: (!left_sum.is_null(row)).then(|| left_sum.value(row)),
                            right_sum: (!right_sum.is_null(row)).then(|| right_sum.value(row)),
                        },
                        weight.value(row),
                    ));
                }
            }
            Ok((version, rows))
        })
    }

    fn core_window_snapshot(&self) -> Result<DeltaCoreWindowSnapshot, String> {
        self.runtime.block_on(async {
            let uri = deltalake::ensure_table_uri(&self.table_uri)
                .map_err(|error| format!("invalid Delta CoreWindow table URI: {error}"))?;
            let table =
                deltalake::open_table_with_storage_options(uri, self.storage_options.clone())
                    .await
                    .map_err(|error| format!("open Delta CoreWindow output: {error}"))?;
            let version = table
                .version()
                .ok_or_else(|| "Delta CoreWindow output has no committed version".to_owned())?;
            let context = deltalake::datafusion::prelude::SessionContext::new();
            table
                .update_datafusion_session(&context.state())
                .map_err(|error| format!("register Delta CoreWindow object store: {error}"))?;
            let provider = table
                .table_provider()
                .build()
                .await
                .map_err(|error| format!("build Delta CoreWindow table provider: {error}"))?;
            context
                .register_table("soak_delta_window", Arc::new(provider))
                .map_err(|error| format!("register Delta CoreWindow table: {error}"))?;
            let batches = context
                .sql(
                    "SELECT window_kind, window_start, row_count, id_sum \
                     FROM soak_delta_window",
                )
                .await
                .map_err(|error| format!("plan Delta CoreWindow scan: {error}"))?
                .collect()
                .await
                .map_err(|error| format!("scan Delta CoreWindow output: {error}"))?;

            let mut outputs = BTreeMap::new();
            for batch in batches {
                let column = |name: &str| {
                    batch
                        .schema()
                        .index_of(name)
                        .map(|index| batch.column(index))
                        .map_err(|error| format!("Delta CoreWindow {name} column: {error}"))
                };
                let kind = arrow_cast::cast(column("window_kind")?, &DataType::Utf8)
                    .map_err(|error| format!("cast Delta CoreWindow window_kind: {error}"))?;
                let start = arrow_cast::cast(
                    column("window_start")?,
                    &DataType::Timestamp(TimeUnit::Millisecond, None),
                )
                .map_err(|error| format!("cast Delta CoreWindow window_start: {error}"))?;
                let count = arrow_cast::cast(column("row_count")?, &DataType::Int64)
                    .map_err(|error| format!("cast Delta CoreWindow row_count: {error}"))?;
                let sum = arrow_cast::cast(column("id_sum")?, &DataType::Int64)
                    .map_err(|error| format!("cast Delta CoreWindow id_sum: {error}"))?;
                let kind = kind
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .ok_or_else(|| "Delta CoreWindow kind did not cast to Utf8".to_owned())?;
                let start = start
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        "Delta CoreWindow start did not cast to Timestamp(ms)".to_owned()
                    })?;
                let count = count
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "Delta CoreWindow count did not cast to Int64".to_owned())?;
                let sum = sum
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "Delta CoreWindow sum did not cast to Int64".to_owned())?;
                for row in 0..batch.num_rows() {
                    if kind.is_null(row)
                        || start.is_null(row)
                        || count.is_null(row)
                        || sum.is_null(row)
                    {
                        return Err("Delta CoreWindow output contains a null field".to_owned());
                    }
                    let output = CoreWindowOutput {
                        kind: kind.value(row).to_owned(),
                        window_start_ms: start.value(row),
                        row_count: u64::try_from(count.value(row)).map_err(|_| {
                            "Delta CoreWindow output has a negative row_count".to_owned()
                        })?,
                        id_sum: sum.value(row),
                    };
                    *outputs.entry(output).or_default() += 1;
                }
            }
            Ok(DeltaCoreWindowSnapshot { version, outputs })
        })
    }
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn retry_delta_snapshot_until<T, F>(
    nodes: &mut [Node],
    deadline: Instant,
    label: &str,
    mut snapshot: F,
) -> T
where
    F: FnMut() -> Result<T, String>,
{
    let mut last_error = None;
    loop {
        assert_running_nodes(nodes);
        if remaining_at(deadline, Instant::now()).is_none() {
            let detail = last_error
                .as_deref()
                .unwrap_or("deadline elapsed before the first observation");
            panic!("{label} Delta snapshot read failed through its deadline: {detail}");
        }
        match snapshot() {
            Ok(snapshot) => return snapshot,
            Err(error) => {
                last_error = Some(error);
                let Some(remaining) = remaining_at(deadline, Instant::now()) else {
                    continue;
                };
                std::thread::sleep(remaining.min(Duration::from_millis(100)));
            }
        }
    }
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn assert_delta_core_window_outputs(
    nodes: &mut [Node],
    outputs: &BTreeMap<String, DeltaOutputOracle>,
    expected: &BTreeMap<CoreWindowOutput, u64>,
    frozen_input_at: Instant,
    slo_mode: SloMode,
    window: Duration,
    label: &str,
) {
    let deadline = Instant::now() + window;
    let visibility_slo = Duration::from_millis(env_u64(
        "LAMINAR_SOAK_EO_VISIBILITY_MS",
        DEFAULT_EO_VISIBILITY_MS,
    ));
    let completed = loop {
        assert_running_nodes(nodes);
        let mut snapshots = BTreeMap::new();
        let mut observed = BTreeMap::new();
        let mut pending = None;
        for (kind, output) in outputs {
            match output.core_window_snapshot() {
                Ok(snapshot) => {
                    for (row, count) in &snapshot.outputs {
                        assert_eq!(
                            row.kind, *kind,
                            "{label}: {kind} table contains a {} row",
                            row.kind
                        );
                        let Some(expected_count) = expected.get(row) else {
                            panic!("{label}: unexpected Delta CoreWindow output {row:?}");
                        };
                        assert!(
                            count <= expected_count,
                            "{label}: Delta CoreWindow output {row:?} occurred {count} times; expected {expected_count}"
                        );
                        *observed.entry(row.clone()).or_default() += count;
                    }
                    snapshots.insert(kind.clone(), snapshot);
                }
                Err(error) => {
                    pending = Some(format!("{kind}: {error}"));
                    break;
                }
            }
        }
        if pending.is_none()
            && snapshots.len() == outputs.len()
            && validate_core_window_outputs(&observed, expected, true).is_ok()
        {
            let visibility = frozen_input_at.elapsed();
            enforce_or_report_visibility_slo(
                slo_mode,
                &format!("{label}: CoreWindow output"),
                visibility,
                visibility_slo,
            );
            eprintln!(
                "soak: PROFILE {label} CoreWindow visibility_ms={:.3}",
                visibility.as_secs_f64() * 1_000.0
            );
            break snapshots;
        }
        assert!(
            Instant::now() < deadline,
            "{label}: Delta CoreWindow output incomplete: {}",
            pending.unwrap_or_else(|| {
                validate_core_window_outputs(&observed, expected, true)
                    .expect_err("incomplete CoreWindow output must explain its gap")
            })
        );
        std::thread::sleep(Duration::from_millis(100));
    };

    let quiet_deadline = Instant::now() + OUTPUT_BOUNDARY_STABILITY;
    while Instant::now() < quiet_deadline {
        assert_running_nodes(nodes);
        std::thread::sleep(Duration::from_millis(100));
    }
    let reread_deadline = Instant::now() + window;
    for (kind, output) in outputs {
        let previous = completed
            .get(kind)
            .expect("every Delta CoreWindow canary completed");
        let stable =
            retry_delta_snapshot_until(nodes, reread_deadline, &format!("{label}: {kind}"), || {
                output.core_window_snapshot()
            });
        assert!(
            stable.version >= previous.version,
            "{label}: {kind} Delta version regressed from {} to {}",
            previous.version,
            stable.version
        );
        assert_eq!(
            stable.outputs, previous.outputs,
            "{label}: {kind} Delta output changed during the quiet re-read"
        );
    }
    eprintln!(
        "soak: {label} validated {} exact CoreWindow rows and a stable EO reread",
        expected.len()
    );
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn assert_delta_matrix_outputs(
    nodes: &mut [Node],
    outputs: &BTreeMap<String, DeltaOutputOracle>,
    window: Duration,
    label: &str,
) {
    let all_expected = expected_matrix_outputs();
    let expected = BOUNDED_JOIN_SOAK_CASES
        .iter()
        .map(|case| {
            (
                case.name.to_owned(),
                all_expected
                    .iter()
                    .filter(|row| row.join_case == case.name)
                    .cloned()
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        outputs.keys().collect::<BTreeSet<_>>(),
        expected.keys().collect::<BTreeSet<_>>(),
        "{label}: Delta raw matrix table roster differs from the bounded-join cases"
    );

    let deadline = Instant::now() + window;
    let completed = loop {
        assert_running_nodes(nodes);
        let mut completed = BTreeMap::new();
        let mut pending = None;
        for (join_case, output) in outputs {
            let expected_rows = expected
                .get(join_case)
                .expect("Delta raw matrix case was validated above");
            match output.matrix_snapshot() {
                Ok(snapshot) => {
                    assert_eq!(
                        snapshot.duplicate_rows, 0,
                        "{label}: {join_case} Delta snapshot version {} contains {} duplicate rows; first duplicate {:?}",
                        snapshot.version, snapshot.duplicate_rows, snapshot.first_duplicate
                    );
                    if let Some(row) = snapshot
                        .outputs
                        .iter()
                        .find(|row| !expected_rows.contains(*row))
                    {
                        panic!(
                            "{label}: {join_case} Delta snapshot version {} contains impossible row {row:?}",
                            snapshot.version
                        );
                    }
                    if snapshot.rows == expected_rows.len() && snapshot.outputs == *expected_rows {
                        completed.insert(join_case.clone(), snapshot);
                    } else {
                        let missing = expected_rows
                            .difference(&snapshot.outputs)
                            .take(16)
                            .cloned()
                            .collect::<Vec<_>>();
                        pending = Some(format!(
                            "{join_case} version {} exposed {}/{} exact rows; first missing {missing:?}",
                            snapshot.version,
                            snapshot.outputs.len(),
                            expected_rows.len()
                        ));
                        break;
                    }
                }
                Err(error) => {
                    pending = Some(format!("{join_case}: {error}"));
                    break;
                }
            }
        }
        if completed.len() == expected.len() {
            break completed;
        }
        assert!(
            Instant::now() < deadline,
            "{label}: Delta raw matrix did not expose every exact join row: {}",
            pending.unwrap_or_else(|| "incomplete table roster".to_owned())
        );
        std::thread::sleep(Duration::from_millis(100));
    };

    let quiet_deadline = Instant::now() + OUTPUT_BOUNDARY_STABILITY;
    while Instant::now() < quiet_deadline {
        assert_running_nodes(nodes);
        std::thread::sleep(Duration::from_millis(100));
    }
    let reread_deadline = Instant::now() + window;
    for (join_case, output) in outputs {
        let previous = completed
            .get(join_case)
            .expect("every Delta raw matrix case completed");
        let stable = retry_delta_snapshot_until(
            nodes,
            reread_deadline,
            &format!("{label}: {join_case}"),
            || output.matrix_snapshot(),
        );
        assert!(
            stable.version >= previous.version,
            "{label}: {join_case} Delta version regressed from {} to {}",
            previous.version,
            stable.version
        );
        assert_eq!(
            stable.duplicate_rows, 0,
            "{label}: {join_case} Delta quiet re-read contains duplicate rows"
        );
        assert_eq!(
            stable.rows, previous.rows,
            "{label}: {join_case} Delta row count changed during the quiet re-read"
        );
        assert_eq!(
            stable.outputs, previous.outputs,
            "{label}: {join_case} Delta row set changed during the quiet re-read"
        );
    }
    eprintln!(
        "soak: {label} validated exact raw output for all {} join kinds and remained stable for {OUTPUT_BOUNDARY_STABILITY:?}",
        expected.len()
    );
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
#[derive(Debug)]
struct DeltaMatrixAggregateSnapshot {
    version: u64,
    physical_rows: usize,
    net: BTreeMap<MatrixAggregateOutput, i64>,
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn delta_matrix_aggregate_snapshot(
    output: &DeltaOutputOracle,
    expected_case: &str,
) -> Result<DeltaMatrixAggregateSnapshot, String> {
    let (version, rows) = output.aggregate_rows()?;
    let physical_rows = rows.len();
    let mut deltas = Vec::with_capacity(physical_rows);
    for (join_case, aggregate, weight) in rows {
        if join_case != expected_case {
            return Err(format!(
                "Delta table for {expected_case} contains aggregate case {join_case}"
            ));
        }
        deltas.push((aggregate, weight));
    }
    let net = net_matrix_aggregate_deltas(deltas, expected_case)?;
    Ok(DeltaMatrixAggregateSnapshot {
        version,
        physical_rows,
        net,
    })
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn assert_delta_matrix_aggregates(
    nodes: &mut [Node],
    outputs: &BTreeMap<String, DeltaOutputOracle>,
    window: Duration,
    label: &str,
) {
    let deadline = Instant::now() + window;
    let expected = expected_matrix_aggregates();
    let completed = loop {
        assert_running_nodes(nodes);
        let mut scan_error = None;
        let mut completed = BTreeMap::new();
        for (expected_case, output) in outputs {
            match delta_matrix_aggregate_snapshot(output, expected_case) {
                Ok(snapshot) => {
                    let expected_state = expected
                        .get(expected_case)
                        .expect("every bounded join has a final aggregate");
                    if snapshot.net.len() != 1 || snapshot.net.get(expected_state) != Some(&1) {
                        scan_error = Some(format!(
                            "{expected_case}: differential aggregate net is {:?}, expected {{{expected_state:?}: 1}}",
                            snapshot.net,
                        ));
                        break;
                    }
                    completed.insert(expected_case.clone(), snapshot);
                }
                Err(error) => {
                    scan_error = Some(format!("{expected_case}: {error}"));
                    break;
                }
            }
        }
        let observation = match scan_error {
            Some(error) => error,
            None if outputs.len() == expected.len() && completed.len() == expected.len() => {
                break completed;
            }
            None => format!(
                "validated {}/{} expected aggregate tables",
                completed.len(),
                expected.len()
            ),
        };
        assert!(
            Instant::now() < deadline,
            "{label}: Delta aggregate outputs did not net to every exact final state: {observation}"
        );
        std::thread::sleep(Duration::from_millis(100));
    };

    let quiet_deadline = Instant::now() + OUTPUT_BOUNDARY_STABILITY;
    while Instant::now() < quiet_deadline {
        assert_running_nodes(nodes);
        std::thread::sleep(Duration::from_millis(100));
    }
    let reread_deadline = Instant::now() + window;
    for (expected_case, output) in outputs {
        let previous = completed
            .get(expected_case)
            .expect("every Delta aggregate case completed");
        let stable = retry_delta_snapshot_until(
            nodes,
            reread_deadline,
            &format!("{label}: {expected_case}"),
            || delta_matrix_aggregate_snapshot(output, expected_case),
        );
        assert!(
            stable.version >= previous.version,
            "{label}: {expected_case} Delta version regressed from {} to {}",
            previous.version,
            stable.version
        );
        assert_eq!(
            stable.physical_rows, previous.physical_rows,
            "{label}: {expected_case} Delta aggregate row count changed during the quiet re-read"
        );
        assert_eq!(
            stable.net, previous.net,
            "{label}: {expected_case} Delta aggregate net changed during the quiet re-read"
        );
    }
    eprintln!(
        "soak: {label} validated all {} differential join-to-aggregate Delta nets and remained stable for {OUTPUT_BOUNDARY_STABILITY:?}",
        completed.len(),
    );
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
struct DeltaExactOutput<'a> {
    oracle: &'a DeltaOutputOracle,
    expected: &'a BTreeSet<(u64, u64)>,
    frozen_input_at: Instant,
    label: &'a str,
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
struct DeltaVisibilityBoundaries {
    opened: Vec<Option<OpenDeltaJoinSnapshot>>,
    visibility_slo: Duration,
    slo_mode: SloMode,
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn validate_delta_exact_snapshot(
    output: &DeltaExactOutput<'_>,
    snapshot: DeltaJoinSnapshot,
) -> Result<DeltaJoinSnapshot, String> {
    assert_eq!(
        snapshot.duplicate_rows, 0,
        "{} Delta snapshot version {} contains {} duplicate rows; first duplicate {:?}",
        output.label, snapshot.version, snapshot.duplicate_rows, snapshot.first_duplicate
    );
    if let Some(pair) = snapshot
        .pairs
        .iter()
        .find(|pair| !output.expected.contains(pair))
    {
        panic!(
            "{} Delta snapshot version {} contains impossible join pair {pair:?}",
            output.label, snapshot.version
        );
    }
    if snapshot.rows == output.expected.len() && snapshot.pairs.len() == output.expected.len() {
        return Ok(snapshot);
    }
    let missing = output
        .expected
        .iter()
        .filter(|pair| !snapshot.pairs.contains(pair))
        .take(16)
        .copied()
        .collect::<Vec<_>>();
    Err(format!(
        "version {} exposed {}/{} exact pairs; first missing {missing:?}",
        snapshot.version,
        snapshot.pairs.len(),
        output.expected.len()
    ))
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn open_delta_visibility_boundaries(
    nodes: &mut [Node],
    outputs: &[DeltaExactOutput<'_>],
    visibility_slo: Duration,
    slo_mode: SloMode,
) -> Vec<Option<OpenDeltaJoinSnapshot>> {
    let deadlines = outputs
        .iter()
        .map(|output| output.frozen_input_at + visibility_slo)
        .collect::<Vec<_>>();
    let mut latest = (0..outputs.len()).map(|_| None).collect::<Vec<_>>();
    let mut observations = vec![String::new(); outputs.len()];
    loop {
        assert_running_nodes(nodes);
        let now = Instant::now();
        let active = deadlines
            .iter()
            .enumerate()
            .filter_map(|(index, deadline)| (now < *deadline).then_some(index))
            .collect::<Vec<_>>();
        if active.is_empty() {
            break;
        }
        let opened = std::thread::scope(|scope| {
            let workers = active
                .iter()
                .map(|index| {
                    let output = &outputs[*index];
                    (*index, scope.spawn(|| output.oracle.open_join_snapshot()))
                })
                .collect::<Vec<_>>();
            workers
                .into_iter()
                .map(|(index, worker)| {
                    let result = worker
                        .join()
                        .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
                    (index, result)
                })
                .collect::<Vec<_>>()
        });
        for (index, opened) in opened {
            match opened {
                Ok(opened) if opened.observed_at <= deadlines[index] => {
                    latest[index] = Some(opened);
                }
                Ok(opened) => {
                    let overrun = opened
                        .observed_at
                        .saturating_duration_since(deadlines[index]);
                    observations[index] = format!(
                        "metadata observation completed {overrun:?} after the SLO boundary"
                    );
                }
                Err(error) => observations[index] = error,
            }
        }
        let Some(remaining) = deadlines
            .iter()
            .filter_map(|deadline| remaining_at(*deadline, Instant::now()))
            .min()
        else {
            continue;
        };
        let poll_interval = if remaining <= Duration::from_secs(2) {
            Duration::from_millis(100)
        } else {
            Duration::from_secs(1)
        };
        std::thread::sleep(remaining.min(poll_interval));
    }

    latest
        .into_iter()
        .enumerate()
        .map(|(index, opened)| {
            if opened.is_some() {
                return opened;
            }
            let observation = if observations[index].is_empty() {
                "no metadata observation completed before the boundary"
            } else {
                &observations[index]
            };
            match slo_mode {
                SloMode::Certify => panic!(
                    "{} Delta metadata had no observable version at or before its {visibility_slo:?} visibility boundary: {observation}",
                    outputs[index].label
                ),
                SloMode::Observe => eprintln!(
                    "soak: PROFILE {} Delta metadata missed the non-certifying {visibility_slo:?} visibility boundary: {observation}",
                    outputs[index].label
                ),
            }
            None
        })
        .collect()
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn scan_delta_visibility_boundaries(
    nodes: &mut [Node],
    outputs: &[DeltaExactOutput<'_>],
    opened: &[Option<OpenDeltaJoinSnapshot>],
    window: Duration,
    visibility_slo: Duration,
    slo_mode: SloMode,
) -> Vec<Option<DeltaJoinSnapshot>> {
    let mut completed = Vec::with_capacity(outputs.len());
    for (output, opened) in outputs.iter().zip(opened) {
        let Some(opened) = opened else {
            completed.push(None);
            continue;
        };
        let scan_deadline = Instant::now() + window;
        let snapshot = retry_delta_snapshot_until(nodes, scan_deadline, output.label, || {
            output.oracle.scan_join_snapshot(opened)
        });
        let snapshot = match validate_delta_exact_snapshot(output, snapshot) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                match slo_mode {
                    SloMode::Certify => panic!(
                        "{} was not exact at its pre-SLO Delta boundary: {error}",
                        output.label
                    ),
                    SloMode::Observe => eprintln!(
                        "soak: PROFILE {} missed the non-certifying {visibility_slo:?} exact Delta boundary: {error}",
                        output.label
                    ),
                }
                completed.push(None);
                continue;
            }
        };
        let visibility = snapshot
            .observed_at
            .saturating_duration_since(output.frozen_input_at);
        assert!(
            visibility <= visibility_slo,
            "{} exact Delta boundary was observed after {visibility:?}; SLO is {visibility_slo:?}",
            output.label
        );
        eprintln!(
            "soak: PROFILE {} exact Delta visibility_upper_bound_ms={:.3} rows={} table_version={}",
            output.label,
            visibility.as_secs_f64() * 1_000.0,
            snapshot.rows,
            snapshot.version
        );
        completed.push(Some(snapshot));
    }
    completed
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn capture_delta_visibility_boundaries(
    nodes: &mut [Node],
    outputs: &[DeltaExactOutput<'_>],
    slo_mode: SloMode,
) -> DeltaVisibilityBoundaries {
    assert!(
        !outputs.is_empty(),
        "Delta exact-output check has no outputs"
    );
    for output in outputs {
        assert!(
            !output.expected.is_empty(),
            "{} join oracle expected no output pairs",
            output.label
        );
    }
    let visibility_slo = Duration::from_millis(env_u64(
        "LAMINAR_SOAK_EO_VISIBILITY_MS",
        DEFAULT_EO_VISIBILITY_MS,
    ));
    // WHY: Full-table scans are verifier work. Timestamp the immutable publication boundary first
    // so scan order and transient read retries cannot consume or move the delivery SLO.
    let opened = open_delta_visibility_boundaries(nodes, outputs, visibility_slo, slo_mode);
    DeltaVisibilityBoundaries {
        opened,
        visibility_slo,
        slo_mode,
    }
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn wait_delta_exact_output_after_boundary(
    nodes: &mut [Node],
    output: &DeltaExactOutput<'_>,
    boundary_version: Option<u64>,
    window: Duration,
) -> DeltaJoinSnapshot {
    let deadline = Instant::now() + window;
    let mut latest_version = boundary_version;
    loop {
        assert_running_nodes(nodes);
        let observation = match output.oracle.open_join_snapshot() {
            Ok(opened) if latest_version.is_some_and(|version| opened.version <= version) => {
                format!(
                    "latest version {} has not advanced past the incomplete boundary",
                    opened.version
                )
            }
            Ok(opened) => {
                let snapshot = retry_delta_snapshot_until(nodes, deadline, output.label, || {
                    output.oracle.scan_join_snapshot(&opened)
                });
                latest_version = Some(snapshot.version);
                match validate_delta_exact_snapshot(output, snapshot) {
                    Ok(snapshot) => return snapshot,
                    Err(error) => error,
                }
            }
            Err(error) => error,
        };
        assert!(
            Instant::now() < deadline,
            "{} did not become exact after its observed Delta boundary: {observation}",
            output.label
        );
        let Some(remaining) = remaining_at(deadline, Instant::now()) else {
            continue;
        };
        std::thread::sleep(remaining.min(Duration::from_secs(1)));
    }
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn wait_delta_exact_outputs(
    nodes: &mut [Node],
    outputs: &[DeltaExactOutput<'_>],
    boundaries: &DeltaVisibilityBoundaries,
    window: Duration,
) -> Vec<DeltaJoinSnapshot> {
    assert_eq!(
        outputs.len(),
        boundaries.opened.len(),
        "Delta visibility boundaries do not match the checked outputs"
    );
    let at_boundary = scan_delta_visibility_boundaries(
        nodes,
        outputs,
        &boundaries.opened,
        window,
        boundaries.visibility_slo,
        boundaries.slo_mode,
    );
    outputs
        .iter()
        .zip(&boundaries.opened)
        .zip(at_boundary)
        .map(|((output, opened), snapshot)| {
            snapshot.unwrap_or_else(|| {
                let snapshot = wait_delta_exact_output_after_boundary(
                    nodes,
                    output,
                    opened.as_ref().map(|opened| opened.version),
                    window,
                );
                let visibility = snapshot
                    .observed_at
                    .saturating_duration_since(output.frozen_input_at);
                eprintln!(
                    "soak: PROFILE {} exact Delta protocol visibility_ms={:.3} rows={} table_version={} after non-certifying boundary",
                    output.label,
                    visibility.as_secs_f64() * 1_000.0,
                    snapshot.rows,
                    snapshot.version
                );
                snapshot
            })
        })
        .collect()
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn assert_delta_exact_outputs_stable(
    nodes: &mut [Node],
    outputs: &[DeltaExactOutput<'_>],
    completed: &[DeltaJoinSnapshot],
    window: Duration,
) {
    assert!(!outputs.is_empty(), "Delta stability check has no outputs");
    assert_eq!(
        outputs.len(),
        completed.len(),
        "Delta stability snapshots do not match the checked outputs"
    );
    let quiet_deadline = Instant::now() + OUTPUT_BOUNDARY_STABILITY;
    while Instant::now() < quiet_deadline {
        assert_running_nodes(nodes);
        std::thread::sleep(Duration::from_millis(100));
    }
    for (output, completed) in outputs.iter().zip(completed) {
        let reread_deadline = Instant::now() + window;
        let stable = retry_delta_snapshot_until(nodes, reread_deadline, output.label, || {
            output.oracle.snapshot()
        });
        assert!(
            stable.version >= completed.version,
            "{} Delta version regressed from {} to {}",
            output.label,
            completed.version,
            stable.version
        );
        assert_eq!(
            stable.rows, completed.rows,
            "{} Delta row count changed during the quiet re-read",
            output.label
        );
        assert_eq!(
            stable.duplicate_rows, 0,
            "{} Delta quiet re-read contains duplicate rows",
            output.label
        );
        assert_eq!(
            stable.pairs, completed.pairs,
            "{} Delta pair multiset changed during the quiet re-read",
            output.label
        );
        eprintln!(
            "soak: {} Delta snapshot remained exact and stable for {OUTPUT_BOUNDARY_STABILITY:?}",
            output.label
        );
    }
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
struct DeltaTemporalExactOutput<'a> {
    oracle: &'a DeltaOutputOracle,
    kind: TemporalOutputKind,
    expected: &'a BTreeSet<TemporalOutput>,
    frozen_input_at: Instant,
    label: &'a str,
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn assert_delta_temporal_outputs(
    nodes: &mut [Node],
    outputs: &[DeltaTemporalExactOutput<'_>],
    slo_mode: SloMode,
    window: Duration,
) {
    let deadline = Instant::now() + window;
    let visibility_slo = Duration::from_millis(env_u64(
        "LAMINAR_SOAK_EO_VISIBILITY_MS",
        DEFAULT_EO_VISIBILITY_MS,
    ));
    let mut completed = vec![None; outputs.len()];
    let mut observations = vec![String::new(); outputs.len()];
    while completed.iter().any(Option::is_none) {
        assert_running_nodes(nodes);
        for (index, output) in outputs.iter().enumerate() {
            if completed[index].is_some() {
                continue;
            }
            match output.oracle.temporal_snapshot(output.kind) {
                Ok(snapshot) => {
                    assert_eq!(
                        snapshot.duplicate_rows,
                        0,
                        "{} Delta snapshot version {} contains {} duplicate rows; first duplicate {:?}",
                        output.label,
                        snapshot.version,
                        snapshot.duplicate_rows,
                        snapshot.first_duplicate
                    );
                    if let Some(impossible) = snapshot
                        .outputs
                        .iter()
                        .find(|row| !output.expected.contains(row))
                    {
                        panic!(
                            "{} Delta snapshot version {} contains impossible row {impossible:?}",
                            output.label, snapshot.version
                        );
                    }
                    if snapshot.rows == output.expected.len()
                        && snapshot.outputs == *output.expected
                    {
                        let visibility =
                            Instant::now().saturating_duration_since(output.frozen_input_at);
                        enforce_or_report_visibility_slo(
                            slo_mode,
                            &format!("{} exact Delta output", output.label),
                            visibility,
                            visibility_slo,
                        );
                        eprintln!(
                            "soak: PROFILE {} exact Delta visibility_ms={:.3} rows={} table_version={}",
                            output.label,
                            visibility.as_secs_f64() * 1_000.0,
                            snapshot.rows,
                            snapshot.version
                        );
                        completed[index] = Some(snapshot);
                    } else {
                        let missing = output
                            .expected
                            .difference(&snapshot.outputs)
                            .take(8)
                            .copied()
                            .collect::<Vec<_>>();
                        observations[index] = format!(
                            "version {} exposed {}/{} exact rows; first missing {missing:?}",
                            snapshot.version,
                            snapshot.outputs.len(),
                            output.expected.len()
                        );
                    }
                }
                Err(error) => observations[index] = error,
            }
        }
        if completed.iter().any(Option::is_none) {
            assert!(
                Instant::now() < deadline,
                "Delta temporal outputs did not expose every exact row: {:?}",
                outputs
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| completed[*index].is_none())
                    .map(|(index, output)| (output.label, observations[index].as_str()))
                    .collect::<Vec<_>>()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    let quiet_deadline = Instant::now() + OUTPUT_BOUNDARY_STABILITY;
    while Instant::now() < quiet_deadline {
        assert_running_nodes(nodes);
        std::thread::sleep(Duration::from_millis(100));
    }
    let reread_deadline = Instant::now() + window;
    for (output, completed) in outputs.iter().zip(completed) {
        let previous = completed.expect("every Delta temporal output completed");
        let stable = retry_delta_snapshot_until(nodes, reread_deadline, output.label, || {
            output.oracle.temporal_snapshot(output.kind)
        });
        assert!(
            stable.version >= previous.version,
            "{} Delta version regressed from {} to {}",
            output.label,
            previous.version,
            stable.version
        );
        assert_eq!(
            stable.duplicate_rows, 0,
            "{} Delta quiet re-read contains duplicate rows",
            output.label
        );
        assert_eq!(
            stable.rows, previous.rows,
            "{} Delta row count changed during the quiet re-read",
            output.label
        );
        assert_eq!(
            stable.outputs, previous.outputs,
            "{} Delta row set changed during the quiet re-read",
            output.label
        );
    }
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
        if node_id == victim {
            if *total != baseline + 1.0 {
                return Err(format!(
                    "node{node_id} pipeline fault total is {total}, expected {} after explicit fault on node{victim}",
                    baseline + 1.0
                ));
            }
        } else {
            // A non-victim may fault exactly once: its in-flight shuffle send to the faulted
            // victim may have been partially admitted, so it fail-closed joins the same
            // recovery round. More than that single cascade participation is amplification.
            if *total > baseline + 1.0 {
                return Err(format!(
                    "node{node_id} pipeline fault total is {total}, amplifying the explicit fault on node{victim}"
                ));
            }
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecoveryPrepareSequence {
    final_prepare_line: usize,
    abandoned_rounds: u32,
}

#[cfg(feature = "kafka")]
fn validate_recovery_prepare_sequence(
    fault_logs: &[String],
    leader: usize,
) -> Result<RecoveryPrepareSequence, String> {
    let leader_log = fault_logs
        .get(leader)
        .ok_or_else(|| format!("recovery leader node{leader} has no fault log"))?;
    let leader_lines = leader_log.lines().collect::<Vec<_>>();
    let prepares = leader_lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| line.contains(RECOVERY_PREPARE_LOG).then_some(index))
        .collect::<Vec<_>>();
    let total_prepares = fault_logs
        .iter()
        .map(|log| log.matches(RECOVERY_PREPARE_LOG).count())
        .sum::<usize>();
    if total_prepares != prepares.len() {
        return Err("recovery Prepare was emitted by a non-leader process".into());
    }
    match prepares.as_slice() {
        [prepare] => Ok(RecoveryPrepareSequence {
            final_prepare_line: *prepare,
            abandoned_rounds: 0,
        }),
        [first, second] => {
            let between = &leader_lines[first + 1..*second];
            let superseded = between
                .iter()
                .position(|line| line.contains(RECOVERY_SUPERSEDED_LOG))
                .ok_or_else(|| {
                    "two recovery Prepare records have no intervening superseded-round fence"
                        .to_string()
                })?;
            if !between[superseded + 1..]
                .iter()
                .any(|line| line.contains(RECOVERY_PREPARE_HANDOFF_LOG))
            {
                return Err(
                    "two recovery Prepare records have no ordered direct handoff fence".into(),
                );
            }
            let retry_holds = between
                .iter()
                .filter(|line| line.contains(RECOVERY_RETRY_HOLD_LOG))
                .count();
            if retry_holds != 1 {
                return Err(format!(
                    "fenced recovery replacement recorded {retry_holds} retry holds; expected exactly one"
                ));
            }
            Ok(RecoveryPrepareSequence {
                final_prepare_line: *second,
                abandoned_rounds: 1,
            })
        }
        _ => Err(format!(
            "explicit fault created {total_prepares} recovery Prepare generations; expected one, or one fenced direct replacement"
        )),
    }
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
    let prepare = validate_recovery_prepare_sequence(fault_logs, leader)?.final_prepare_line;
    let Some(failed) = leader_failure else {
        return Ok(None);
    };
    let leader_fault_lines = fault_logs[leader].lines().collect::<Vec<_>>();
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
    // A checkpoint already in flight at the fault boundary can observe the victim's closed
    // shuffle scope before the recovery leader durably announces Prepare. Both orders are valid;
    // the captured log boundary and the single recovery generation provide the causal fence.
    if !leader_fault_lines[prepare + 1..]
        .iter()
        .any(|line| line.contains(RECOVERY_RELEASE_LOG))
    {
        return Err("recovery Prepare was not followed by recovery Release".into());
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
    let prepare_sequence = validate_recovery_prepare_sequence(&logs, evidence.recovery_leader)
        .unwrap_or_else(|error| panic!("explicit recovery Prepare sequence invalid: {error}"));
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
        let expected_failures = evidence.recovery_failure_baselines[index]
            + if index == evidence.recovery_leader {
                f64::from(prepare_sequence.abandoned_rounds)
            } else {
                0.0
            };
        assert_eq!(
            failures, expected_failures,
            "node{} coordinated recovery failure count did not match the certified Prepare sequence",
            node.id,
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

#[cfg(feature = "kafka")]
fn json_i64_field(value: &serde_json::Value, field: &str) -> Result<i64, String> {
    value
        .get(field)
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        })
        .ok_or_else(|| format!("aggregate output has no signed integer {field}: {value}"))
}

#[cfg(feature = "kafka")]
fn json_nullable_i64_field(value: &serde_json::Value, field: &str) -> Result<Option<i64>, String> {
    let value = value
        .get(field)
        .ok_or_else(|| format!("aggregate output has no {field}: {value}"))?;
    if value.is_null() {
        Ok(None)
    } else {
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
            .map(Some)
            .ok_or_else(|| format!("aggregate output {field} is not a signed integer: {value}"))
    }
}

#[cfg(feature = "kafka")]
fn decode_matrix_aggregate(
    value: &serde_json::Value,
) -> Result<(String, MatrixAggregateOutput), String> {
    let join_case = value
        .get("join_case")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("aggregate output has no string join_case: {value}"))?;
    let non_negative = |field| {
        u64::try_from(json_i64_field(value, field)?)
            .map_err(|_| format!("aggregate output has a negative {field}: {value}"))
    };
    Ok((
        join_case.to_owned(),
        MatrixAggregateOutput {
            row_count: non_negative("row_count")?,
            left_count: non_negative("left_count")?,
            right_count: non_negative("right_count")?,
            left_sum: json_nullable_i64_field(value, "left_sum")?,
            right_sum: json_nullable_i64_field(value, "right_sum")?,
        },
    ))
}

#[cfg(feature = "kafka")]
fn validate_matrix_aggregate_latest(
    observed: &BTreeMap<String, MatrixAggregateOutput>,
    expected: &BTreeMap<String, MatrixAggregateOutput>,
) -> Result<(), String> {
    for join_case in observed.keys() {
        if !expected.contains_key(join_case) {
            return Err(format!("unknown bounded-join aggregate case {join_case:?}"));
        }
    }
    for (join_case, expected_state) in expected {
        let observed_state = observed
            .get(join_case)
            .ok_or_else(|| format!("no aggregate output for {join_case}"))?;
        if observed_state != expected_state {
            return Err(format!(
                "{join_case} latest state is {observed_state:?}, expected {expected_state:?}"
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "kafka")]
fn net_matrix_aggregate_deltas(
    deltas: impl IntoIterator<Item = (MatrixAggregateOutput, i64)>,
    label: &str,
) -> Result<BTreeMap<MatrixAggregateOutput, i64>, String> {
    let mut net = BTreeMap::<MatrixAggregateOutput, i64>::new();
    for (aggregate, weight) in deltas {
        if weight == 0 {
            return Err(format!(
                "{label}: differential aggregate contains a physical zero weight"
            ));
        }
        let current = net.entry(aggregate).or_default();
        *current = current
            .checked_add(weight)
            .ok_or_else(|| format!("{label}: aggregate weight overflow"))?;
    }
    net.retain(|_, weight| *weight != 0);
    Ok(net)
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
#[cfg_attr(not(feature = "delta-lake-s3"), allow(dead_code))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinDelivery {
    AtLeastOnce,
    ExactlyOnce,
}

#[cfg(feature = "kafka")]
impl JoinDelivery {
    const fn server_value(self) -> &'static str {
        match self {
            Self::AtLeastOnce => "at_least_once",
            Self::ExactlyOnce => "exactly_once",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::AtLeastOnce => "ALO",
            Self::ExactlyOnce => "EO",
        }
    }
}

#[cfg(feature = "kafka")]
fn cluster_checkpoint_timeout(delivery: JoinDelivery, s3_emulator: bool) -> Duration {
    if delivery == JoinDelivery::ExactlyOnce && s3_emulator {
        CLUSTER_EMULATOR_EO_CHECKPOINT_TIMEOUT
    } else {
        CLUSTER_CHECKPOINT_TIMEOUT
    }
}

#[cfg(feature = "kafka")]
fn s3_emulator_enabled() -> bool {
    std::env::var("LAMINAR_SOAK_ALLOW_S3_EMULATOR").as_deref() == Ok("1")
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
struct DeltaSoakStorage {
    endpoint: String,
    access_key: String,
    secret_key: String,
    region: String,
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
impl DeltaSoakStorage {
    fn from_environment() -> Self {
        assert!(
            cfg!(debug_assertions),
            "EO MinIO soaks require cargo test --profile soak; release builds keep custom S3 endpoints fail-closed"
        );
        assert_eq!(
            std::env::var("LAMINAR_SOAK_ALLOW_S3_EMULATOR").as_deref(),
            Ok("1"),
            "EO MinIO soaks require LAMINAR_SOAK_ALLOW_S3_EMULATOR=1; this debug-only gate validates the protocol under faults, not production S3 semantics"
        );
        eprintln!(
            "soak: MinIO EO mode validates recovery/publication protocol only; it is not cloud-provider certification"
        );
        let required =
            |name| std::env::var(name).unwrap_or_else(|_| panic!("EO Delta soak requires {name}"));
        Self {
            endpoint: required("LAMINAR_SOAK_S3_ENDPOINT"),
            access_key: required("LAMINAR_SOAK_S3_ACCESS_KEY"),
            secret_key: required("LAMINAR_SOAK_S3_SECRET_KEY"),
            region: required("LAMINAR_SOAK_S3_REGION"),
        }
    }

    fn options(&self) -> HashMap<String, String> {
        [
            ("aws_endpoint", self.endpoint.as_str()),
            ("aws_access_key_id", self.access_key.as_str()),
            ("aws_secret_access_key", self.secret_key.as_str()),
            ("aws_region", self.region.as_str()),
            ("aws_allow_http", "true"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
    }
}

#[cfg(feature = "kafka")]
struct JoinOutputOracles {
    kafka_bounded: Option<KafkaOutputOracle>,
    kafka_temporal_load: Option<KafkaOutputOracle>,
    kafka_temporal: Option<KafkaTemporalOracle>,
    kafka_temporal_probe: Option<KafkaTemporalOracle>,
    #[cfg(feature = "delta-lake-s3")]
    delta_bounded: Option<DeltaOutputOracle>,
    #[cfg(feature = "delta-lake-s3")]
    delta_temporal_load: Option<DeltaOutputOracle>,
    #[cfg(feature = "delta-lake-s3")]
    delta_temporal: Option<DeltaOutputOracle>,
    #[cfg(feature = "delta-lake-s3")]
    delta_temporal_probe: Option<DeltaOutputOracle>,
}

#[cfg(feature = "kafka")]
impl JoinOutputOracles {
    fn kafka(
        brokers: &str,
        bounded_topic: &str,
        temporal_load_topic: &str,
        temporal_topic: &str,
        temporal_probe_topic: &str,
    ) -> (Self, String) {
        for topic in [
            bounded_topic,
            temporal_load_topic,
            temporal_topic,
            temporal_probe_topic,
        ] {
            kafka_create_topic(brokers, topic, OUTPUT_TOPIC_PARTITIONS);
        }
        let sinks = format!(
            "{}{}{}",
            kafka_pair_sink_config("soak_output", "soak_join", brokers, bounded_topic),
            kafka_pair_sink_config(
                "soak_temporal_load_output",
                "soak_temporal_load",
                brokers,
                temporal_load_topic,
            ),
            kafka_temporal_sink_config(brokers, temporal_topic, temporal_probe_topic),
        );
        (
            Self {
                kafka_bounded: Some(KafkaOutputOracle::new(
                    brokers,
                    bounded_topic,
                    OUTPUT_TOPIC_PARTITIONS,
                )),
                kafka_temporal_load: Some(KafkaOutputOracle::new(
                    brokers,
                    temporal_load_topic,
                    OUTPUT_TOPIC_PARTITIONS,
                )),
                kafka_temporal: Some(KafkaTemporalOracle::new(
                    brokers,
                    temporal_topic,
                    TemporalOutputKind::LeftAsof,
                )),
                kafka_temporal_probe: Some(KafkaTemporalOracle::new(
                    brokers,
                    temporal_probe_topic,
                    TemporalOutputKind::InnerProbe,
                )),
                #[cfg(feature = "delta-lake-s3")]
                delta_bounded: None,
                #[cfg(feature = "delta-lake-s3")]
                delta_temporal_load: None,
                #[cfg(feature = "delta-lake-s3")]
                delta_temporal: None,
                #[cfg(feature = "delta-lake-s3")]
                delta_temporal_probe: None,
            },
            sinks,
        )
    }

    #[cfg(feature = "delta-lake-s3")]
    fn delta(run_id: &str, topology: &str, storage: &DeltaSoakStorage) -> (Self, String) {
        let bounded_uri = delta_soak_table_uri(run_id, topology);
        let temporal_load_uri = delta_soak_table_uri(run_id, &format!("{topology}-temporal-load"));
        let temporal_uri = delta_soak_table_uri(run_id, &format!("{topology}-temporal"));
        let temporal_probe_uri =
            delta_soak_table_uri(run_id, &format!("{topology}-temporal-probe"));
        let sinks = format!(
            "{}{}{}",
            delta_join_sink_config(&bounded_uri, storage),
            delta_append_sink_config(
                "soak_temporal_load_output",
                "soak_temporal_load",
                &temporal_load_uri,
                storage,
            ),
            delta_temporal_sink_config(&temporal_uri, &temporal_probe_uri, storage),
        );
        (
            Self {
                kafka_bounded: None,
                kafka_temporal_load: None,
                kafka_temporal: None,
                kafka_temporal_probe: None,
                delta_bounded: Some(DeltaOutputOracle::new(bounded_uri, storage)),
                delta_temporal_load: Some(DeltaOutputOracle::new(temporal_load_uri, storage)),
                delta_temporal: Some(DeltaOutputOracle::new(temporal_uri, storage)),
                delta_temporal_probe: Some(DeltaOutputOracle::new(temporal_probe_uri, storage)),
            },
            sinks,
        )
    }
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn delta_join_exact_outputs<'a>(
    oracles: &'a JoinOutputOracles,
    produced: &'a ProducedPrefix,
    frozen_input_at: Instant,
    bounded_label: &'a str,
    temporal_label: &'a str,
) -> [DeltaExactOutput<'a>; 2] {
    [
        DeltaExactOutput {
            oracle: oracles
                .delta_bounded
                .as_ref()
                .expect("EO bounded Delta oracle"),
            expected: &produced.expected_pairs,
            frozen_input_at,
            label: bounded_label,
        },
        DeltaExactOutput {
            oracle: oracles
                .delta_temporal_load
                .as_ref()
                .expect("EO temporal-load Delta oracle"),
            expected: &produced.expected_temporal_pairs,
            frozen_input_at,
            label: temporal_label,
        },
    ]
}

#[cfg(feature = "kafka")]
fn assert_temporal_canaries(
    nodes: &mut [Node],
    oracles: &mut JoinOutputOracles,
    delivery: JoinDelivery,
    input: &TemporalPostFaultInput,
    frontier_released_at: Instant,
    slo_mode: SloMode,
    window: Duration,
    topology: &str,
) {
    #[cfg(not(feature = "delta-lake-s3"))]
    let _ = slo_mode;
    let asof_label = format!(
        "{topology} {} nullable temporal ASOF canary",
        delivery.label()
    );
    let probe_label = format!(
        "{topology} {} inner temporal probe canary",
        delivery.label()
    );
    match delivery {
        JoinDelivery::AtLeastOnce => {
            let mut outputs = [
                KafkaTemporalExactOutput {
                    oracle: oracles
                        .kafka_temporal
                        .as_mut()
                        .expect("ALO temporal ASOF Kafka oracle"),
                    expected: &input.expected_asof,
                    frozen_input_at: frontier_released_at,
                    label: &asof_label,
                },
                KafkaTemporalExactOutput {
                    oracle: oracles
                        .kafka_temporal_probe
                        .as_mut()
                        .expect("ALO temporal probe Kafka oracle"),
                    expected: &input.expected_probe,
                    frozen_input_at: frontier_released_at,
                    label: &probe_label,
                },
            ];
            assert_kafka_temporal_outputs(nodes, &mut outputs, window);
        }
        JoinDelivery::ExactlyOnce => {
            #[cfg(feature = "delta-lake-s3")]
            assert_delta_temporal_outputs(
                nodes,
                &[
                    DeltaTemporalExactOutput {
                        oracle: oracles
                            .delta_temporal
                            .as_ref()
                            .expect("EO temporal ASOF Delta oracle"),
                        kind: TemporalOutputKind::LeftAsof,
                        expected: &input.expected_asof,
                        frozen_input_at: frontier_released_at,
                        label: &asof_label,
                    },
                    DeltaTemporalExactOutput {
                        oracle: oracles
                            .delta_temporal_probe
                            .as_ref()
                            .expect("EO temporal probe Delta oracle"),
                        kind: TemporalOutputKind::InnerProbe,
                        expected: &input.expected_probe,
                        frozen_input_at: frontier_released_at,
                        label: &probe_label,
                    },
                ],
                slo_mode,
                window,
            );
            #[cfg(not(feature = "delta-lake-s3"))]
            unreachable!("EO runner is unavailable without delta-lake-s3");
        }
    }
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn delta_soak_table_uri(run_id: &str, topology: &str) -> String {
    let bucket =
        std::env::var("LAMINAR_SOAK_DELTA_BUCKET").unwrap_or_else(|_| "laminardb-soak".to_owned());
    let bucket = bucket.trim_matches('/');
    assert!(!bucket.is_empty(), "LAMINAR_SOAK_DELTA_BUCKET is empty");
    let table_uri = format!("s3://{bucket}/join-eo-{topology}-{run_id}");
    eprintln!("soak: EO Delta output table {table_uri}");
    table_uri
}

#[cfg(feature = "kafka")]
fn kafka_pair_sink_config(name: &str, pipeline: &str, brokers: &str, output_topic: &str) -> String {
    format!(
        r#"
[[sink]]
name = "{name}"
pipeline = "{pipeline}"
connector = "kafka"
format = "json"
[sink.properties]
"bootstrap.servers" = "{brokers}"
topic = "{output_topic}"
"key.column" = "left_id"
"linger.ms" = "{SOAK_KAFKA_SINK_LINGER_MS}"
"#,
    )
}

#[cfg(feature = "kafka")]
fn kafka_temporal_sink_config(
    brokers: &str,
    asof_output_topic: &str,
    probe_output_topic: &str,
) -> String {
    format!(
        "{}{}",
        kafka_pair_sink_config(
            "soak_temporal_output",
            "soak_temporal",
            brokers,
            asof_output_topic,
        ),
        kafka_pair_sink_config(
            "soak_temporal_probe_output",
            "soak_temporal_probe",
            brokers,
            probe_output_topic,
        ),
    )
}

#[cfg(feature = "kafka")]
fn kafka_matrix_sink_config(brokers: &str, output_topic: &str) -> String {
    MATRIX_OUTPUT_PIPELINES
        .iter()
        .map(|pipeline| {
            format!(
                r#"
[[sink]]
name = "{pipeline}_output"
pipeline = "{pipeline}"
connector = "kafka"
format = "json"
[sink.properties]
"bootstrap.servers" = "{brokers}"
topic = "{output_topic}"
"key.column" = "join_case"
"linger.ms" = "{SOAK_KAFKA_SINK_LINGER_MS}"
"#,
            )
        })
        .collect()
}

#[cfg(feature = "kafka")]
fn kafka_matrix_aggregate_sink_config(brokers: &str, output_topic: &str) -> String {
    BOUNDED_JOIN_SOAK_CASES
        .iter()
        .map(|case| {
            let pipeline = format!("soak_matrix_{}_aggregate", case.name);
            format!(
                r#"
[[sink]]
name = "{pipeline}_output"
pipeline = "{pipeline}"
connector = "kafka"
format = "json"
[sink.properties]
"bootstrap.servers" = "{brokers}"
topic = "{output_topic}"
"key.column" = "join_case"
envelope = "upsert"
"linger.ms" = "{SOAK_KAFKA_SINK_LINGER_MS}"
"#,
            )
        })
        .collect()
}

#[cfg(feature = "kafka")]
fn kafka_core_window_sink_config(brokers: &str, output_topic: &str) -> String {
    CORE_WINDOW_CANARIES
        .iter()
        .map(|canary| {
            format!(
                r#"
[[sink]]
name = "{pipeline}_output"
pipeline = "{pipeline}"
connector = "kafka"
format = "json"
[sink.properties]
"bootstrap.servers" = "{brokers}"
topic = "{output_topic}"
"key.column" = "window_kind"
"linger.ms" = "{SOAK_KAFKA_SINK_LINGER_MS}"
"#,
                pipeline = canary.pipeline,
            )
        })
        .collect()
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn delta_append_sink_config(
    name: &str,
    pipeline: &str,
    table_uri: &str,
    storage: &DeltaSoakStorage,
) -> String {
    // Cluster catalog DDL rejects literal secrets; `$${...}` survives server TOML substitution
    // as a `${...}` env reference the catalog accepts and the connector resolves from the env.
    format!(
        r#"
[[sink]]
name = "{name}"
pipeline = "{pipeline}"
connector = "delta-lake"
[sink.properties]
"table.path" = "{table_uri}"
"write.mode" = "append"
"storage.aws_endpoint" = "{endpoint}"
"storage.aws_access_key_id" = "{access_key}"
"storage.aws_secret_access_key" = "$${{LAMINAR_SOAK_S3_SECRET_KEY}}"
"storage.aws_region" = "{region}"
"storage.aws_allow_http" = "true"
"#,
        endpoint = storage.endpoint,
        access_key = storage.access_key,
        region = storage.region,
    )
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn delta_join_sink_config(table_uri: &str, storage: &DeltaSoakStorage) -> String {
    delta_append_sink_config("soak_output", "soak_join", table_uri, storage)
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn delta_temporal_sink_config(
    asof_table_uri: &str,
    probe_table_uri: &str,
    storage: &DeltaSoakStorage,
) -> String {
    format!(
        "{}{}",
        delta_append_sink_config(
            "soak_temporal_output",
            "soak_temporal",
            asof_table_uri,
            storage,
        ),
        delta_append_sink_config(
            "soak_temporal_probe_output",
            "soak_temporal_probe",
            probe_table_uri,
            storage,
        )
    )
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn delta_matrix_output_table_uris(run_id: &str, topology: &str) -> BTreeMap<String, String> {
    BOUNDED_JOIN_SOAK_CASES
        .iter()
        .map(|case| {
            let table_topology = format!("{topology}-matrix-{}", case.name);
            (
                case.name.to_owned(),
                delta_soak_table_uri(run_id, &table_topology),
            )
        })
        .collect()
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn delta_matrix_output_sink_config(
    table_uris: &BTreeMap<String, String>,
    storage: &DeltaSoakStorage,
) -> String {
    table_uris
        .iter()
        .map(|(join_case, table_uri)| {
            let pipeline = format!("soak_matrix_{join_case}");
            delta_append_sink_config(&format!("{pipeline}_output"), &pipeline, table_uri, storage)
        })
        .collect()
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn delta_matrix_aggregate_table_uris(run_id: &str, topology: &str) -> BTreeMap<String, String> {
    BOUNDED_JOIN_SOAK_CASES
        .iter()
        .map(|case| {
            let table_topology = format!("{topology}-matrix-{}-aggregate", case.name);
            (
                case.name.to_owned(),
                delta_soak_table_uri(run_id, &table_topology),
            )
        })
        .collect()
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn delta_matrix_aggregate_sink_config(
    table_uris: &BTreeMap<String, String>,
    storage: &DeltaSoakStorage,
) -> String {
    table_uris
        .iter()
        .map(|(join_case, table_uri)| {
            let pipeline = format!("soak_matrix_{join_case}_aggregate");
            delta_append_sink_config(&format!("{pipeline}_output"), &pipeline, table_uri, storage)
        })
        .collect()
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn delta_core_window_table_uris(run_id: &str, topology: &str) -> BTreeMap<String, String> {
    CORE_WINDOW_CANARIES
        .iter()
        .map(|canary| {
            (
                canary.kind.to_owned(),
                delta_soak_table_uri(run_id, &format!("{topology}-window-{}", canary.kind)),
            )
        })
        .collect()
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn delta_core_window_sink_config(
    table_uris: &BTreeMap<String, String>,
    storage: &DeltaSoakStorage,
) -> String {
    CORE_WINDOW_CANARIES
        .iter()
        .map(|canary| {
            delta_append_sink_config(
                &format!("{}_output", canary.pipeline),
                canary.pipeline,
                table_uris
                    .get(canary.kind)
                    .expect("every CoreWindow canary has a Delta URI"),
                storage,
            )
        })
        .collect()
}

#[cfg(feature = "kafka")]
fn core_window_canary_pipeline_config() -> String {
    CORE_WINDOW_CANARIES
        .iter()
        .map(|canary| {
            format!(
                r#"
[[pipeline]]
name = "{pipeline}"
sql = """
SELECT '{kind}' AS window_kind, {expression} AS window_start,
       COUNT(*) AS row_count, SUM(id) AS id_sum
FROM soak_matrix_lhs
WHERE id IN (-101, -102, -104, -105)
GROUP BY {expression}
{emit}
"""
"#,
                kind = canary.kind,
                pipeline = canary.pipeline,
                expression = canary.expression,
                emit = canary.emit,
            )
        })
        .collect()
}

#[cfg(feature = "kafka")]
fn bounded_join_matrix_workload_config(
    brokers: &str,
    left_topic: &str,
    right_topic: &str,
    consumer_group: &str,
    sinks: &str,
) -> String {
    let join_interval_ms = join_interval_ms();
    let mut config = format!(
        r#"
[[source]]
name = "soak_matrix_lhs"
connector = "kafka"
format = "json"
[source.properties]
"bootstrap.servers" = "{brokers}"
topic = "{left_topic}"
"group.id" = "{consumer_group}-left"
"startup.mode" = "earliest"
"json.column.event_time.epoch_unit" = "millis"
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "join_key"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "join_key_2"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "event_time"
type = "TIMESTAMP"
nullable = false
[source.watermark]
column = "event_time"
max_out_of_orderness = "2s"

[[source]]
name = "soak_matrix_rhs"
connector = "kafka"
format = "json"
[source.properties]
"bootstrap.servers" = "{brokers}"
topic = "{right_topic}"
"group.id" = "{consumer_group}-right"
"startup.mode" = "earliest"
"json.column.event_time.epoch_unit" = "millis"
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "join_key"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "join_key_2"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "event_time"
type = "TIMESTAMP"
nullable = false
[source.watermark]
column = "event_time"
max_out_of_orderness = "2s"
"#,
    );
    for case in BOUNDED_JOIN_SOAK_CASES {
        config.push_str(&format!(
            r#"
[[pipeline]]
name = "soak_matrix_{name}"
sql = """
SELECT '{name}' AS join_case, {projection}
FROM soak_matrix_lhs l
{keyword} soak_matrix_rhs r
  ON l.join_key = r.join_key_2
 AND l.join_key_2 = r.join_key
 AND r.event_time BETWEEN l.event_time
                          AND l.event_time + INTERVAL '{join_interval_ms}' MILLISECOND
WHERE {filter}
"""
"#,
            name = case.name,
            projection = case.projection,
            keyword = case.keyword,
            filter = case.filter,
        ));
    }
    config.push_str(&bounded_join_matrix_aggregate_pipeline_config());
    config.push_str(&core_window_canary_pipeline_config());
    config.push_str(sinks);
    config
}

#[cfg(feature = "kafka")]
fn bounded_join_matrix_aggregate_pipeline_config() -> String {
    BOUNDED_JOIN_SOAK_CASES
        .iter()
        .map(|case| {
            format!(
                r#"
[[pipeline]]
name = "soak_matrix_{name}_aggregate"
sql = """
SELECT join_case, COUNT(*) AS row_count,
       COUNT(left_id) AS left_count, COUNT(right_id) AS right_count,
       SUM(left_id) AS left_sum, SUM(right_id) AS right_sum
FROM soak_matrix_{name}
GROUP BY join_case
EMIT CHANGES
"""
"#,
                name = case.name,
            )
        })
        .collect()
}

#[cfg(feature = "kafka")]
fn temporal_join_source_config(
    brokers: &str,
    canary_left_topic: &str,
    canary_right_topic: &str,
    load_left_topic: &str,
    load_right_topic: &str,
    consumer_group: &str,
) -> String {
    [
        (
            "temporal_left",
            canary_left_topic,
            "temporal-left",
            "json",
            false,
        ),
        (
            "temporal_right",
            canary_right_topic,
            "temporal-right",
            "debezium",
            true,
        ),
        (
            "temporal_load_left",
            load_left_topic,
            "temporal-load-left",
            "json",
            false,
        ),
        (
            "temporal_load_right",
            load_right_topic,
            "temporal-load-right",
            "json",
            true,
        ),
    ]
    .into_iter()
    .map(|(name, topic, group_suffix, format, primary_key)| {
        let temporal_load = name.starts_with("temporal_load_");
        let primary_key = match (primary_key, temporal_load) {
            (true, true) => "primary_key = [\"join_key\", \"source_partition\"]\n",
            (true, false) => "primary_key = [\"join_key\"]\n",
            (false, _) => "",
        };
        let source_partition = if temporal_load {
            r#"[[source.schema]]
name = "source_partition"
type = "BIGINT"
nullable = false
"#
        } else {
            ""
        };
        format!(
            r#"
[[source]]
name = "{name}"
connector = "kafka"
format = "{format}"
{primary_key}[source.properties]
"bootstrap.servers" = "{brokers}"
topic = "{topic}"
"group.id" = "{consumer_group}-{group_suffix}"
"startup.mode" = "earliest"
"json.column.temporal_time.epoch_unit" = "millis"
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "join_key"
type = "BIGINT"
nullable = false
{source_partition}[[source.schema]]
name = "temporal_time"
type = "TIMESTAMP"
nullable = false
[source.watermark]
column = "temporal_time"
max_out_of_orderness = "2s"
"#,
        )
    })
    .collect()
}

#[cfg(feature = "kafka")]
fn mutable_bounded_interval_workload_config(
    brokers: &str,
    left_topic: &str,
    right_topic: &str,
    consumer_group: &str,
    output_topic: &str,
) -> String {
    let join_interval_ms = join_interval_ms();
    format!(
        r#"
[[source]]
name = "soak_mutable_interval_left"
connector = "kafka"
format = "json"
[source.properties]
"bootstrap.servers" = "{brokers}"
topic = "{left_topic}"
"group.id" = "{consumer_group}-left"
"startup.mode" = "earliest"
"json.column.event_time.epoch_unit" = "millis"
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "join_key"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "event_time"
type = "TIMESTAMP"
nullable = false
[source.watermark]
column = "event_time"
max_out_of_orderness = "2s"

[[source]]
name = "soak_mutable_interval_right"
connector = "kafka"
format = "debezium"
primary_key = ["id", "join_key", "event_time"]
[source.properties]
"bootstrap.servers" = "{brokers}"
topic = "{right_topic}"
"group.id" = "{consumer_group}-right"
"startup.mode" = "earliest"
"json.column.event_time.epoch_unit" = "millis"
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "join_key"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "event_time"
type = "TIMESTAMP"
nullable = false
[[source.schema]]
name = "right_value"
type = "BIGINT"
nullable = false
[source.watermark]
column = "event_time"
max_out_of_orderness = "2s"

[[pipeline]]
name = "soak_mutable_interval"
sql = """
SELECT l.id AS left_id, r.id AS right_id, r.right_value AS right_value
FROM soak_mutable_interval_left l
JOIN soak_mutable_interval_right r
  ON l.join_key = r.join_key
 AND r.event_time BETWEEN l.event_time
                          AND l.event_time + INTERVAL '{join_interval_ms}' MILLISECOND
"""

[[pipeline]]
name = "soak_mutable_interval_aggregate"
sql = """
SELECT left_id, COUNT(*) AS match_count, SUM(right_value) AS value_sum
FROM soak_mutable_interval
GROUP BY left_id
EMIT CHANGES
"""

[[sink]]
name = "soak_mutable_interval_output"
pipeline = "soak_mutable_interval_aggregate"
connector = "kafka"
format = "json"
[sink.properties]
"bootstrap.servers" = "{brokers}"
topic = "{output_topic}"
"key.column" = "left_id"
envelope = "upsert"
"linger.ms" = "{SOAK_KAFKA_SINK_LINGER_MS}"
"#,
    )
}

#[cfg(feature = "kafka")]
fn kafka_join_workload_config(
    brokers: &str,
    left_topic: &str,
    right_topic: &str,
    temporal_left_topic: &str,
    temporal_right_topic: &str,
    consumer_group: &str,
    matrix_left_topic: &str,
    matrix_right_topic: &str,
    matrix_consumer_group: &str,
    sink: &str,
    matrix_sinks: &str,
    extra_workload: &str,
) -> String {
    let join_interval_ms = join_interval_ms();
    let temporal_sources = temporal_join_source_config(
        brokers,
        temporal_left_topic,
        temporal_right_topic,
        left_topic,
        right_topic,
        consumer_group,
    );
    let matrix = bounded_join_matrix_workload_config(
        brokers,
        matrix_left_topic,
        matrix_right_topic,
        matrix_consumer_group,
        matrix_sinks,
    );
    format!(
        r#"
[[source]]
name = "join_left"
connector = "kafka"
format = "json"
[source.properties]
"bootstrap.servers" = "{brokers}"
topic = "{left_topic}"
"group.id" = "{consumer_group}-left"
"startup.mode" = "earliest"
"json.column.event_time.epoch_unit" = "millis"
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "join_key"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "event_time"
type = "TIMESTAMP"
nullable = false
[source.watermark]
column = "event_time"
max_out_of_orderness = "2s"

[[source]]
name = "join_right"
connector = "kafka"
format = "json"
[source.properties]
"bootstrap.servers" = "{brokers}"
topic = "{right_topic}"
"group.id" = "{consumer_group}-right"
"startup.mode" = "earliest"
"json.column.event_time.epoch_unit" = "millis"
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "join_key"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "event_time"
type = "TIMESTAMP"
nullable = false
[source.watermark]
column = "event_time"
max_out_of_orderness = "2s"

{temporal_sources}

[[pipeline]]
name = "soak_join"
# This canonical BETWEEN clause admits right-side rows from the left timestamp
# through the inclusive upper interval bound.
sql = """
SELECT l.id AS left_id, r.id AS right_id, l.join_key AS join_key
FROM join_left l
JOIN join_right r
  ON l.join_key = r.join_key
AND r.event_time BETWEEN l.event_time
                      AND l.event_time + INTERVAL '{join_interval_ms}' MILLISECOND
WHERE l.join_key >= 0 AND r.join_key >= 0
"""

[[pipeline]]
name = "soak_temporal"
sql = """
SELECT l.id AS left_id, r.id AS right_id
FROM temporal_left l
LEFT JOIN temporal_right
FOR SYSTEM_TIME AS OF l.temporal_time AS r
  ON l.join_key = r.join_key
WHERE l.join_key >= 0
"""

[[pipeline]]
name = "soak_temporal_probe"
sql = """
SELECT l.id AS left_id, r.id AS right_id,
       probe.offset_ms AS offset_ms, probe.probe_time AS probe_time
FROM temporal_left l
TEMPORAL PROBE JOIN temporal_right r
  ON (join_key)
  TIMESTAMPS (temporal_time, temporal_time)
  LIST (-4s, -3s, -2s, -1s, 1s) AS probe
WHERE l.join_key >= 0 AND l.id = {TEMPORAL_RECOVERY_LEFT_ID}
"""

[[pipeline]]
name = "soak_temporal_load"
sql = """
SELECT l.id AS left_id, r.id AS right_id
FROM temporal_load_left l
LEFT JOIN temporal_load_right
FOR SYSTEM_TIME AS OF l.temporal_time AS r
  ON l.join_key = r.join_key
 AND l.source_partition = r.source_partition
WHERE l.join_key >= 0
"""

[[pipeline]]
name = "soak_join_aggregate"
sql = """
SELECT join_key, COUNT(*) AS match_count, MAX(right_id) AS max_right_id
FROM soak_join
GROUP BY join_key
"""

{sink}

{extra_workload}

{matrix}
"#,
    )
}

#[cfg(feature = "kafka")]
fn cluster_subscription_workload_config() -> &'static str {
    // WHY: keep this gate proportional to input volume so it certifies the subscription data
    // plane independently from the high-amplification interval-join soak running beside it.
    r#"
[[pipeline]]
name = "soak_subscription_aggregate"
sql = """
SELECT join_key, COUNT(*) AS match_count, MAX(id) AS max_right_id
FROM join_left
WHERE join_key >= 0
GROUP BY join_key
WITH ('retain_history' = '256mb')
"""
"#
}

#[cfg(feature = "kafka")]
fn write_config(
    dir: &Path,
    id: usize,
    interval_ms: u64,
    key_groups: u32,
    checkpoint_url: &str,
    brokers: &str,
    left_topic: &str,
    right_topic: &str,
    temporal_left_topic: &str,
    temporal_right_topic: &str,
    consumer_group: &str,
    matrix_left_topic: &str,
    matrix_right_topic: &str,
    matrix_consumer_group: &str,
    delivery: JoinDelivery,
    sink: &str,
    matrix_sinks: &str,
    extra_workload: &str,
) -> PathBuf {
    let http = BASE_PORT + id as u16;
    let gossip = BASE_PORT + 100 + id as u16;
    let seeds: Vec<String> = (0..NODES)
        .map(|i| format!("\"127.0.0.1:{}\"", BASE_PORT + 100 + i as u16))
        .collect();
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
    let checkpoint_timeout_secs =
        cluster_checkpoint_timeout(delivery, s3_emulator_enabled()).as_secs();

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
delivery = "{delivery}"
key_groups = {key_groups}
console_token = "{SOAK_CONSOLE_TOKEN}"
temporal_join_idle_history_retention = "5m"
source_idle_timeout = "{source_idle_timeout_secs}s"
event_time_max_future_skew = "{event_time_max_future_skew_ms}ms"
[discovery]
strategy = "{discovery}"
seeds = [{seeds}]
gossip_port = {gossip}
advertise_host = "127.0.0.1"

[checkpoint]
url = "{url}"
interval = "{interval_ms}ms"
timeout = "{checkpoint_timeout_secs}s"

[checkpoint.storage]
{storage}

# Guaranteed delivery uses engine-owned vnode assignment. The group ID below is only the
# advisory broker-offset namespace; LaminarDB checkpoints remain the recovery authority.
{workload}
"#,
        seeds = seeds.join(", "),
        url = checkpoint_url,
        delivery = delivery.server_value(),
        source_idle_timeout_secs = SOAK_SOURCE_IDLE_TIMEOUT.as_secs(),
        event_time_max_future_skew_ms = SOAK_EVENT_MAX_FUTURE_SKEW_MS,
        workload = kafka_join_workload_config(
            brokers,
            left_topic,
            right_topic,
            temporal_left_topic,
            temporal_right_topic,
            consumer_group,
            matrix_left_topic,
            matrix_right_topic,
            matrix_consumer_group,
            sink,
            matrix_sinks,
            extra_workload,
        ),
    );

    let path = dir.join(format!("node{id}.toml"));
    std::fs::write(&path, toml).unwrap();
    path
}

#[cfg(feature = "kafka")]
fn scoped_soak_storage_url(environment: &str, run_id: &str, namespace: &str) -> String {
    let base = std::env::var(environment)
        .unwrap_or_else(|_| panic!("cluster join soak requires cluster-shared {environment}"));
    assert!(!base.trim().is_empty(), "{environment} must not be empty");
    let separator = if base.ends_with('/') { "" } else { "/" };
    format!("{base}{separator}{run_id}/{namespace}")
}

#[cfg(feature = "kafka")]
fn write_single_join_config(
    dir: &Path,
    interval_ms: u64,
    brokers: &str,
    left_topic: &str,
    right_topic: &str,
    temporal_left_topic: &str,
    temporal_right_topic: &str,
    consumer_group: &str,
    matrix_left_topic: &str,
    matrix_right_topic: &str,
    matrix_consumer_group: &str,
    delivery: JoinDelivery,
    sink: &str,
    matrix_sinks: &str,
    extra_workload: &str,
) -> PathBuf {
    let checkpoint_dir = dir.join("join-checkpoints");
    std::fs::create_dir_all(&checkpoint_dir).unwrap();
    let checkpoint_path = checkpoint_dir.display().to_string().replace('\\', "/");
    let checkpoint_url = if checkpoint_path.starts_with('/') {
        format!("file://{checkpoint_path}")
    } else {
        format!("file:///{checkpoint_path}")
    };
    let config = format!(
        r#"
node_id = "single-join"

[server]
mode = "single"
bind = "127.0.0.1:{SINGLE_JOIN_PORT}"
delivery = "{delivery}"
console_token = "{SOAK_CONSOLE_TOKEN}"
temporal_join_idle_history_retention = "5m"
source_idle_timeout = "{source_idle_timeout_secs}s"
event_time_max_future_skew = "{event_time_max_future_skew_ms}ms"

[checkpoint]
url = "{checkpoint_url}"
interval = "{interval_ms}ms"
timeout = "30s"

{workload}
"#,
        delivery = delivery.server_value(),
        source_idle_timeout_secs = SOAK_SOURCE_IDLE_TIMEOUT.as_secs(),
        event_time_max_future_skew_ms = SOAK_EVENT_MAX_FUTURE_SKEW_MS,
        workload = kafka_join_workload_config(
            brokers,
            left_topic,
            right_topic,
            temporal_left_topic,
            temporal_right_topic,
            consumer_group,
            matrix_left_topic,
            matrix_right_topic,
            matrix_consumer_group,
            sink,
            matrix_sinks,
            extra_workload,
        ),
    );
    let path = dir.join("single-join.toml");
    std::fs::write(&path, config).unwrap();
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
    let checkpoint_dir = dir.join("checkpoints");
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

[checkpoint]
url = "{checkpoint_url}"
interval = "{interval_ms}ms"
timeout = "30s"

[[source]]
name = "gen"
connector = "generator"
properties = {{ "rows.per.second" = "{rows_per_second}", "batch.max.size" = "256", "max.rows" = "{max_rows}" }}
"#,
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
        "nodes/1/checkpoints/{:020}/manifest.json",
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
fn wait_for(what: &str, deadline: Duration, pred: impl FnMut() -> bool) {
    if wait_until(deadline, pred) {
        return;
    }
    panic!("soak: timed out after {deadline:?} waiting for: {what}");
}

fn wait_until(deadline: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let expires_at = Instant::now() + deadline;
    while remaining_at(expires_at, Instant::now()).is_some() {
        if pred() {
            return true;
        }
        if let Some(remaining) = remaining_at(expires_at, Instant::now()) {
            std::thread::sleep(remaining.min(Duration::from_millis(250)));
        }
    }
    false
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
fn checkpoint_barrier_timing_authority(
    evidence: &LocalProcessAuthorityEvidence,
    fence: &CheckpointAssignmentFence,
) -> Result<CheckpointBarrierTimingAuthority, String> {
    let process = local_process_identity(evidence);
    if !process.is_canonical()
        || !fence.is_canonical()
        || evidence.adopted_assignment.participant != evidence.participant
        || !evidence.adopted_assignment.matches_fence(fence)
        || fence.participant_incarnation(process.participant.node_id)
            != Some(process.participant.boot_incarnation)
    {
        return Err("local authority does not match the converged assignment fence".into());
    }
    Ok(CheckpointBarrierTimingAuthority {
        process,
        assignment_version: fence.assignment_version,
        assignment_certificate_digest: fence.digest(),
    })
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
    let diagnostics = durable_progress_diagnostics(nodes, None);
    panic!(
        "soak: {context} did not reach exact local assignment convergence before its existing \
         deadline: {last_pending}; observation=({diagnostics})"
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

#[cfg(feature = "kafka")]
fn observe_live_join_state(
    nodes: &[Node],
    join_high_water: &mut Option<f64>,
    temporal_high_water: &mut Option<f64>,
) {
    for (operator, high_water) in [
        ("soak_join", join_high_water),
        ("soak_temporal_load", temporal_high_water),
    ] {
        let current = live_operator_state_bytes(nodes, operator);
        *high_water = Some(high_water.unwrap_or(0.0).max(current));
    }
}

#[cfg(feature = "kafka")]
fn observe_live_core_window_state(
    nodes: &[Node],
    high_water: &mut [Option<f64>; CORE_WINDOW_CANARIES.len()],
) {
    for (canary, observed) in CORE_WINDOW_CANARIES.iter().zip(high_water) {
        let current = live_operator_state_bytes(nodes, canary.pipeline);
        *observed = Some(observed.map_or(current, |previous| previous.max(current)));
    }
}

#[cfg(feature = "kafka")]
fn assert_core_window_state_bounds(
    observed: &[Option<f64>; CORE_WINDOW_CANARIES.len()],
    nodes: usize,
    label: &str,
) {
    for (canary, observed) in CORE_WINDOW_CANARIES.iter().zip(observed) {
        assert_live_join_state_bytes(
            *observed,
            1,
            nodes,
            &format!("{label} {} CoreWindow", canary.kind),
        );
    }
}

#[cfg(feature = "kafka")]
fn live_operator_state_bytes(nodes: &[Node], operator: &str) -> f64 {
    let operator_label = format!("operator=\"{operator}\"");
    let mut current = 0.0;
    let mut live = 0usize;
    for node in nodes.iter().filter(|node| node.child.is_some()) {
        current += node
            .metric_with_labels(
                "laminardb_managed_state_accounted_bytes",
                &[&operator_label, "phase=\"live\""],
            )
            .unwrap_or_else(|| {
                panic!(
                    "node{} did not expose live managed-state accounting for {operator}",
                    node.id
                )
            });
        live += 1;
    }
    assert!(live > 0, "soak has no live node for state accounting");
    current
}

#[cfg(feature = "kafka")]
fn continuous_aggregate_minimum_bytes(join_keys: u64) -> u64 {
    let profiled_keys = join_keys.min(DEFAULT_JOIN_KEYS);
    MIN_CONTINUOUS_AGGREGATE_STATE_BYTES
        .saturating_mul(profiled_keys)
        .div_ceil(DEFAULT_JOIN_KEYS)
}

#[cfg(feature = "kafka")]
fn validate_retained_state_profile(soak_secs: u64, interval_ms: u64, minimum_bytes: u64) {
    if minimum_bytes == 0 {
        return;
    }
    let soak_ms = soak_secs
        .checked_mul(1_000)
        .expect("LAMINAR_SOAK_SECONDS is too large");
    assert!(
        soak_ms > interval_ms,
        "retained-state profile must run longer than LAMINAR_SOAK_JOIN_INTERVAL_MS"
    );
}

#[cfg(feature = "kafka")]
fn assert_live_join_state_bytes(
    observed: Option<f64>,
    minimum_bytes: u64,
    node_count: usize,
    label: &str,
) {
    let observed = observed.unwrap_or_else(|| panic!("{label}: no live-state sample"));
    let maximum_bytes = laminar_db::DEFAULT_MAX_MANAGED_STATE_BYTES
        .checked_mul(node_count)
        .expect("managed-state accounting bound overflow");
    eprintln!(
        "soak: PROFILE {label} accounted-state observation={observed:.0} bytes, aggregate operator ceiling={maximum_bytes} bytes"
    );
    assert!(
        observed >= minimum_bytes as f64,
        "{label}: live managed-state observation {observed:.0} bytes is below required {minimum_bytes} bytes"
    );
    assert!(
        observed <= maximum_bytes as f64,
        "{label}: accounted-state observation {observed:.0} bytes exceeds the {maximum_bytes}-byte aggregate operator ceiling"
    );
}

#[cfg(feature = "kafka")]
fn enforce_or_report_hot_path_slo(
    mode: SloMode,
    label: &str,
    node_id: usize,
    quantile: &str,
    observed_ms: f64,
    limit_ms: u64,
    diagnostic: &str,
) {
    if observed_ms <= limit_ms as f64 {
        return;
    }
    match mode {
        SloMode::Certify => panic!(
            "{label}: node{node_id} hot-path {quantile} bucket upper bound {observed_ms:.3}ms exceeds {limit_ms}ms; {diagnostic}"
        ),
        SloMode::Observe => eprintln!(
            "soak: PROFILE {label} node{node_id} hot-path {quantile} bucket upper bound {observed_ms:.3}ms exceeded the non-certifying {limit_ms}ms SLO boundary; {diagnostic}"
        ),
    }
}

#[cfg(feature = "kafka")]
fn assert_hot_path_latency(nodes: &[Node], label: &str, slo_mode: SloMode) {
    let minimum = env_u64("LAMINAR_SOAK_HOT_MIN_CYCLES", DEFAULT_HOT_PATH_MIN_CYCLES);
    let p50_limit_ms = env_u64("LAMINAR_SOAK_HOT_P50_MS", DEFAULT_HOT_PATH_P50_MS);
    let p95_limit_ms = env_u64("LAMINAR_SOAK_HOT_P95_MS", DEFAULT_HOT_PATH_P95_MS);
    let p99_limit_ms = env_u64("LAMINAR_SOAK_HOT_P99_MS", DEFAULT_HOT_PATH_P99_MS);
    assert!(minimum > 0, "LAMINAR_SOAK_HOT_MIN_CYCLES must be positive");
    assert!(
        p50_limit_ms > 0 && p50_limit_ms <= p95_limit_ms && p95_limit_ms <= p99_limit_ms,
        "hot-path latency limits must be positive and ordered p50 <= p95 <= p99"
    );
    let evidence = nodes
        .iter()
        .filter(|node| node.child.is_some())
        .map(|node| (node.id, node.hot_path_latency_evidence()))
        .collect::<Vec<_>>();
    assert!(
        !evidence.is_empty(),
        "{label}: no live node exposed hot-path evidence"
    );
    for (node_id, evidence) in &evidence {
        if let Ok(evidence) = evidence {
            evidence.report(*node_id, label);
        }
    }
    for (_, evidence) in &evidence {
        if let Err(error) = evidence {
            panic!("{label}: {error}");
        }
    }
    for (node_id, evidence) in evidence {
        let evidence = evidence.expect("metric collection errors were rejected above");
        let latency = evidence.cycle;
        let diagnostic = evidence.failure_diagnostic();
        assert!(
            latency.observations >= minimum,
            "{label}: node{node_id} captured only {} hot-path cycles; at least {minimum} are required; {diagnostic}",
            latency.observations,
        );
        let p50_ms = latency.p50_upper_seconds * 1_000.0;
        let p95_ms = latency.p95_upper_seconds * 1_000.0;
        let p99_ms = latency.p99_upper_seconds * 1_000.0;
        for (quantile, observed_ms, limit_ms) in [
            ("p50", p50_ms, p50_limit_ms),
            ("p95", p95_ms, p95_limit_ms),
            ("p99", p99_ms, p99_limit_ms),
        ] {
            enforce_or_report_hot_path_slo(
                slo_mode,
                label,
                node_id,
                quantile,
                observed_ms,
                limit_ms,
                &diagnostic,
            );
        }
    }
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

#[cfg(feature = "kafka")]
fn count_recovery_diagnostic_markers(log: &str) -> BTreeMap<&'static str, usize> {
    RECOVERY_DIAGNOSTIC_MARKERS
        .iter()
        .filter_map(|(name, marker)| {
            let count = log.matches(marker).count();
            (count > 0).then_some((*name, count))
        })
        .collect()
}

/// Assert two committed checkpoints over advancing source data. With Kafka, also require a new
/// broker offset commit so an empty-checkpoint loop cannot satisfy the soak.
#[cfg(feature = "kafka")]
fn durable_progress_diagnostics(
    nodes: &[Node],
    commit_oracle: Option<&KafkaJoinCommitOracle>,
) -> String {
    let live_nodes: Vec<&Node> = nodes.iter().filter(|node| node.child.is_some()).collect();
    let live_node_ids: Vec<usize> = live_nodes.iter().map(|node| node.id).collect();
    let leader_by_node: Vec<_> = live_nodes
        .iter()
        .map(|node| (node.id, node.is_leader()))
        .collect();
    let readiness_by_node: Vec<_> = live_nodes
        .iter()
        .map(|node| (node.id, node.readiness_diagnostic()))
        .collect();
    let recovery_log_markers_by_node: Vec<_> = live_nodes
        .iter()
        .map(|node| (node.id, node.recovery_log_marker_counts()))
        .collect();
    let checkpoint_size_bytes_by_node: Vec<_> = live_nodes
        .iter()
        .map(|node| (node.id, node.metric("laminardb_checkpoint_size_bytes")))
        .collect();
    let durable_checkpoint_by_node: Vec<_> = live_nodes
        .iter()
        .map(|node| (node.id, node.durable_checkpoint_status()))
        .collect();
    let completed = try_cluster_metric(nodes, "laminardb_checkpoints_completed_total");
    let failed = try_cluster_metric(nodes, "laminardb_checkpoints_failed_total");
    let recovery_metrics = [
        (
            "pipeline_faults",
            try_cluster_metric(nodes, "laminardb_pipeline_faults_total"),
        ),
        (
            "pipeline_restarts",
            try_cluster_metric(nodes, "laminardb_pipeline_restarts_total"),
        ),
        (
            "coordinated_recoveries",
            try_cluster_metric(nodes, "laminardb_coordinated_recoveries_total"),
        ),
        (
            "coordinated_recovery_failures",
            try_cluster_metric(nodes, "laminardb_coordinated_recovery_failures_total"),
        ),
        (
            "shuffle_delivery_loss_incidents",
            try_cluster_metric(nodes, "laminardb_shuffle_delivery_loss_incidents_total"),
        ),
        (
            "pipeline_cycle_errors",
            try_cluster_metric(nodes, "laminardb_pipeline_cycle_errors_total"),
        ),
    ];
    let offsets = commit_oracle.and_then(KafkaJoinCommitOracle::committed_offsets);
    format!(
        "live_node_ids={live_node_ids:?}, leader_by_node={leader_by_node:?}, \
         readiness_by_node={readiness_by_node:?}, \
         completed_checkpoints={completed:?}, failed_checkpoints={failed:?}, \
         recovery_metrics={recovery_metrics:?}, \
         recovery_log_markers_by_node={recovery_log_markers_by_node:?}, \
         checkpoint_size_bytes_by_node={checkpoint_size_bytes_by_node:?}, \
         durable_checkpoint_by_node={durable_checkpoint_by_node:?}, \
         committed_source_offsets={offsets:?}"
    )
}

#[cfg(feature = "kafka")]
fn assert_progress(
    nodes: &mut [Node],
    mut producer: Option<&mut ProducerGuard>,
    commit_oracle: Option<&KafkaJoinCommitOracle>,
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
    let advance_window = remaining_progress_window(deadline, label);
    let graph_advanced = wait_until(advance_window, || {
        assert_running_nodes(nodes);
        if let Some(producer) = producer.as_deref_mut() {
            producer.assert_running();
        }
        try_cluster_metric(nodes, "laminardb_events_ingested_total")
            .is_some_and(|ingested| ingested >= ingested_target)
            && try_cluster_metric(nodes, "laminardb_events_emitted_total")
                .is_some_and(|emitted| emitted >= emitted_target)
    });
    if !graph_advanced {
        let observed_ingested = try_cluster_metric(nodes, "laminardb_events_ingested_total");
        let observed_emitted = try_cluster_metric(nodes, "laminardb_events_emitted_total");
        let diagnostics = durable_progress_diagnostics(nodes, commit_oracle);
        panic!(
            "soak: timed out after {advance_window:?} waiting for: {label}: source ingestion and \
             graph output to advance; ingested_target={ingested_target}, \
             observed_ingested={observed_ingested:?}, emitted_target={emitted_target}, \
             observed_emitted={observed_emitted:?}, observation=({diagnostics})"
        );
    }

    // Take the durability baselines only after graph output advanced, so checkpoints that happened
    // before this phase cannot satisfy the source-offset proof.
    let checkpoint_target = cluster_commits(nodes) + 2.0;
    let checkpoint_failure_baseline =
        try_cluster_metric(nodes, "laminardb_checkpoints_failed_total");
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
    let durability_window = remaining_progress_window(deadline, label);
    let durability_advanced = wait_until(durability_window, || {
        assert_running_nodes(nodes);
        if let Some(producer) = producer.as_deref_mut() {
            producer.assert_running();
        }
        try_cluster_metric(nodes, "laminardb_checkpoints_completed_total")
            .is_some_and(|commits| commits >= checkpoint_target)
            && kafka_offset_targets.as_ref().is_none_or(|targets| {
                commit_oracle
                    .and_then(KafkaJoinCommitOracle::committed_offsets)
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
    });
    if !durability_advanced {
        let diagnostics = durable_progress_diagnostics(nodes, commit_oracle);
        panic!(
            "soak: timed out after {durability_window:?} waiting for: {label}: checkpoints and \
             durable source offsets to advance; checkpoint_target={checkpoint_target}, \
             checkpoint_failure_baseline={checkpoint_failure_baseline:?}, \
             kafka_offset_baseline={kafka_offset_baseline:?}, \
             kafka_offset_targets={kafka_offset_targets:?}, observation=({diagnostics})"
        );
    }
    wait_for_converged_durable_checkpoint(nodes, deadline, label, previous_checkpoint)
}

#[cfg(feature = "kafka")]
fn assert_final_input_cut(
    nodes: &mut [Node],
    commit_oracle: &KafkaJoinCommitOracle,
    input_boundary: &[i64],
    window: Duration,
    previous_checkpoint: DurableCheckpointStatus,
) -> DurableCheckpointStatus {
    assert_final_input_cuts(
        nodes,
        &[commit_oracle],
        input_boundary,
        window,
        previous_checkpoint,
    )
}

#[cfg(feature = "kafka")]
fn assert_final_input_cuts(
    nodes: &mut [Node],
    commit_oracles: &[&KafkaJoinCommitOracle],
    input_boundary: &[i64],
    window: Duration,
    previous_checkpoint: DurableCheckpointStatus,
) -> DurableCheckpointStatus {
    assert!(
        !commit_oracles.is_empty(),
        "final input cut requires at least one source commit oracle"
    );
    let deadline = Instant::now() + window;
    assert_running_nodes(nodes);
    let checkpoint_targets = nodes
        .iter()
        .filter(|node| node.child.is_some())
        .map(|node| {
            (
                node.id,
                node.commits()
                    .unwrap_or_else(|| panic!("node{} did not expose checkpoint commits", node.id))
                    + 2.0,
            )
        })
        .collect::<Vec<_>>();
    wait_for(
        "frozen input offsets and two later checkpoints to commit",
        remaining_progress_window(deadline, "final input cut"),
        || {
            assert_running_nodes(nodes);
            checkpoint_targets.iter().all(|(node_id, target)| {
                nodes
                    .iter()
                    .find(|node| node.id == *node_id && node.child.is_some())
                    .and_then(Node::commits)
                    .is_some_and(|commits| commits >= *target)
            }) && commit_oracles
                .iter()
                .all(|oracle| oracle.covers(input_boundary))
        },
    );
    wait_for_converged_durable_checkpoint(
        nodes,
        deadline,
        "final input cut",
        Some(previous_checkpoint),
    )
}

#[cfg(feature = "kafka")]
fn assert_temporal_idle_checkpoint(
    nodes: &mut [Node],
    commit_oracle: &KafkaJoinCommitOracle,
    input_boundary: &[i64],
    window: Duration,
    previous_checkpoint: DurableCheckpointStatus,
    label: &str,
) -> DurableCheckpointStatus {
    wait_for(
        &format!("{label}: temporal sources to report idle"),
        window,
        || {
            assert_running_nodes(nodes);
            ["temporal_left", "temporal_right"]
                .into_iter()
                .all(|source| {
                    let source_label = format!("source=\"{source}\"");
                    nodes
                        .iter()
                        .filter(|node| node.child.is_some())
                        .all(|node| {
                            node.metric_with_labels("laminardb_source_idle", &[&source_label])
                                .is_some_and(|idle| idle == 1.0)
                        })
                })
        },
    );
    let checkpoint = assert_final_input_cut(
        nodes,
        commit_oracle,
        input_boundary,
        window,
        previous_checkpoint,
    );
    eprintln!(
        "soak: {label} observed idle temporal inputs and committed checkpoint {} epoch {}",
        checkpoint.checkpoint_id, checkpoint.epoch
    );
    checkpoint
}

#[test]
#[ignore = "spawns a real laminardb process; run with --ignored"]
fn local_exact_source_state_kill9_soak() {
    assert!(
        cfg!(debug_assertions),
        "local exact-state kill injection requires `cargo test --profile soak`; the release \
         profile excludes the test-only checkpoint gate"
    );
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
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
    validate_checkpoint_liveness(interval_ms, SINGLE_CHECKPOINT_TIMEOUT, recovery_ceiling);
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
        executable,
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
    let initial_spawn = node.verify_executable_for_spawn();
    node.spawn(initial_spawn);
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
        .checkpoint_kill_predecessor("leader")
        .expect("moving local checkpoint gate has no exact durable-cut evidence");
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
    let moving_restart = node.verify_executable_for_spawn();
    let moving_recovery_started = Instant::now();
    let moving_recovery_deadline = moving_recovery_started + recovery_ceiling;
    node.kill9();
    node.disarm_checkpoint_kill();
    let moving_recovery_log_offset = node.log_len();
    node.spawn(moving_restart);
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
            .checkpoint_kill_predecessor("leader")
            .expect("armed local checkpoint gate has no exact durable-cut evidence");
        // `.ready` carries the preceding committed recovery cut while the new attempt is held
        // before its Commit decision.
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
        let restart = node.verify_executable_for_spawn();
        let recovery_started = Instant::now();
        let recovery_deadline = recovery_started + recovery_ceiling;
        node.kill9();
        node.disarm_checkpoint_kill();
        let recovery_log_offset = node.log_len();
        node.spawn(restart);
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
#[ignore = "spawns a real laminardb process and Kafka workload; run with --ignored"]
#[cfg(feature = "kafka")]
fn single_node_alo_join_kill9_soak() {
    run_single_node_join_kill9_soak(JoinDelivery::AtLeastOnce);
}

#[test]
#[ignore = "spawns a real laminardb process with Kafka and Delta S3; run with --ignored"]
#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn single_node_eo_join_kill9_soak() {
    run_single_node_join_kill9_soak(JoinDelivery::ExactlyOnce);
}

#[cfg(feature = "kafka")]
fn run_single_node_join_kill9_soak(delivery: JoinDelivery) {
    let delivery_label = delivery.label();
    assert!(
        cfg!(debug_assertions),
        "single-node checkpoint kill injection requires `cargo test --profile soak`"
    );
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let soak_secs = env_u64("LAMINAR_SOAK_SECONDS", 90);
    let interval_ms = env_u64("LAMINAR_SOAK_INTERVAL_MS", 500);
    let retained_interval_ms = join_interval_ms();
    let minimum_live_state_bytes = env_u64("LAMINAR_SOAK_MIN_LIVE_STATE_BYTES", 0);
    let checkpoint_slo_mode = SloMode::from_environment(SOAK_CHECKPOINT_SLO_MODE_ENV)
        .unwrap_or_else(|error| panic!("invalid checkpoint SLO mode: {error}"));
    let hot_path_slo_mode = SloMode::from_environment(SOAK_HOT_SLO_MODE_ENV)
        .unwrap_or_else(|error| panic!("invalid hot-path SLO mode: {error}"));
    let visibility_slo_mode = resolve_visibility_slo_mode(delivery);
    let kills = env_u64("LAMINAR_SOAK_SINGLE_KILLS", 1);
    if delivery == JoinDelivery::AtLeastOnce {
        assert!(
            kills > 0,
            "single-node ALO soak requires at least one hard restart to validate mutable interval recovery"
        );
    }
    assert!(
        interval_ms >= 100,
        "LAMINAR_SOAK_INTERVAL_MS must be at least 100"
    );
    validate_retained_state_profile(soak_secs, retained_interval_ms, minimum_live_state_bytes);
    let recovery_ceiling = recovery_ceiling();
    validate_checkpoint_liveness(interval_ms, SINGLE_CHECKPOINT_TIMEOUT, recovery_ceiling);
    let durable_cut_window = recovery_aware_durable_progress_window(
        interval_ms,
        SINGLE_CHECKPOINT_TIMEOUT,
        recovery_ceiling,
        2,
    );
    if delivery == JoinDelivery::AtLeastOnce {
        validate_mutable_interval_recovery_horizon(kills, recovery_ceiling);
    }
    validate_matrix_recovery_horizon(kills, recovery_ceiling, soak_secs, retained_interval_ms);
    let kafka_partitions = cluster_kafka_partition_count();
    let brokers = std::env::var("LAMINAR_SOAK_KAFKA_SOURCE_BROKERS")
        .expect("single-node join soak requires LAMINAR_SOAK_KAFKA_SOURCE_BROKERS");
    let run_id = soak_run_id();
    let left_topic = format!("soak-single-left-{run_id}");
    let right_topic = format!("soak-single-right-{run_id}");
    let temporal_left_topic = format!("soak-single-temporal-left-{run_id}");
    let temporal_right_topic = format!("soak-single-temporal-right-{run_id}");
    let matrix_left_topic = format!("soak-single-matrix-left-{run_id}");
    let matrix_right_topic = format!("soak-single-matrix-right-{run_id}");
    let output_topic = format!("soak-single-out-{run_id}");
    let temporal_load_output_topic = format!("soak-single-temporal-load-{run_id}");
    let temporal_output_topic = format!("soak-single-temporal-{run_id}");
    let temporal_probe_output_topic = format!("soak-single-temporal-probe-{run_id}");
    let matrix_output_topic = format!("soak-single-matrix-{run_id}");
    let matrix_aggregate_topic = format!("soak-single-matrix-aggregate-{run_id}");
    let window_output_topic = format!("soak-single-window-{run_id}");
    let mutable_left_topic = format!("soak-single-mutable-left-{run_id}");
    let mutable_right_topic = format!("soak-single-mutable-right-{run_id}");
    let mutable_output_topic = format!("soak-single-mutable-out-{run_id}");
    let consumer_group = format!("soak-single-{run_id}");
    let matrix_consumer_group = format!("soak-single-matrix-{run_id}");
    let mutable_consumer_group = format!("soak-single-mutable-{run_id}");
    kafka_create_topic(&brokers, &left_topic, kafka_partitions);
    kafka_create_topic(&brokers, &right_topic, kafka_partitions);
    kafka_create_topic(&brokers, &temporal_left_topic, kafka_partitions);
    kafka_create_topic(&brokers, &temporal_right_topic, kafka_partitions);
    kafka_create_topic(&brokers, &matrix_left_topic, MATRIX_INPUT_PARTITIONS);
    kafka_create_topic(&brokers, &matrix_right_topic, MATRIX_INPUT_PARTITIONS);
    let (matrix_input_boundary, matrix_pre_fault_event_time) =
        produce_matrix_pre_fault_inputs(&brokers, &matrix_left_topic, &matrix_right_topic);
    let mut mutable_output_oracle = None;
    let mut mutable_recovery_seed = None;
    let mutable_workload = if delivery == JoinDelivery::AtLeastOnce {
        kafka_create_topic(
            &brokers,
            &mutable_left_topic,
            MUTABLE_INTERVAL_INPUT_PARTITIONS,
        );
        kafka_create_topic(
            &brokers,
            &mutable_right_topic,
            MUTABLE_INTERVAL_INPUT_PARTITIONS,
        );
        kafka_create_compacted_topic(&brokers, &mutable_output_topic, OUTPUT_TOPIC_PARTITIONS);
        let (seed, _) = produce_mutable_interval_pre_fault_input(
            &brokers,
            &mutable_left_topic,
            &mutable_right_topic,
        );
        mutable_recovery_seed = Some(seed);
        mutable_output_oracle = Some(KafkaMutableIntervalOracle::new(
            &brokers,
            &mutable_output_topic,
        ));
        mutable_bounded_interval_workload_config(
            &brokers,
            &mutable_left_topic,
            &mutable_right_topic,
            &mutable_consumer_group,
            &mutable_output_topic,
        )
    } else {
        String::new()
    };
    let mut output_oracles;
    #[cfg(feature = "delta-lake-s3")]
    let mut matrix_output_oracle = None;
    #[cfg(not(feature = "delta-lake-s3"))]
    let mut matrix_output_oracle;
    #[cfg(feature = "delta-lake-s3")]
    let mut matrix_aggregate_oracle = None;
    #[cfg(not(feature = "delta-lake-s3"))]
    let mut matrix_aggregate_oracle;
    #[cfg(feature = "delta-lake-s3")]
    let mut delta_matrix_output_oracles = None;
    #[cfg(feature = "delta-lake-s3")]
    let mut delta_matrix_aggregate_oracles = None;
    #[cfg(feature = "delta-lake-s3")]
    let mut window_output_oracle = None;
    #[cfg(not(feature = "delta-lake-s3"))]
    let mut window_output_oracle;
    #[cfg(feature = "delta-lake-s3")]
    let mut delta_window_output_oracles = None;
    let (sink_config, matrix_sink_config) = match delivery {
        JoinDelivery::AtLeastOnce => {
            let (configured_oracles, sink_config) = JoinOutputOracles::kafka(
                &brokers,
                &output_topic,
                &temporal_load_output_topic,
                &temporal_output_topic,
                &temporal_probe_output_topic,
            );
            output_oracles = configured_oracles;
            kafka_create_topic(&brokers, &matrix_output_topic, OUTPUT_TOPIC_PARTITIONS);
            kafka_create_compacted_topic(
                &brokers,
                &matrix_aggregate_topic,
                OUTPUT_TOPIC_PARTITIONS,
            );
            kafka_create_topic(&brokers, &window_output_topic, OUTPUT_TOPIC_PARTITIONS);
            matrix_output_oracle = Some(KafkaMatrixOracle::new(&brokers, &matrix_output_topic));
            matrix_aggregate_oracle = Some(KafkaMatrixAggregateOracle::new(
                &brokers,
                &matrix_aggregate_topic,
            ));
            window_output_oracle = Some(KafkaCoreWindowOracle::new(&brokers, &window_output_topic));
            (
                sink_config,
                format!(
                    "{}{}{}",
                    kafka_matrix_sink_config(&brokers, &matrix_output_topic),
                    kafka_matrix_aggregate_sink_config(&brokers, &matrix_aggregate_topic),
                    kafka_core_window_sink_config(&brokers, &window_output_topic),
                ),
            )
        }
        JoinDelivery::ExactlyOnce => {
            #[cfg(feature = "delta-lake-s3")]
            {
                let storage = DeltaSoakStorage::from_environment();
                let (configured_oracles, sink_config) =
                    JoinOutputOracles::delta(&run_id, "single", &storage);
                output_oracles = configured_oracles;
                let matrix_uris = delta_matrix_output_table_uris(&run_id, "single");
                let matrix_sinks = delta_matrix_output_sink_config(&matrix_uris, &storage);
                delta_matrix_output_oracles = Some(
                    matrix_uris
                        .into_iter()
                        .map(|(join_case, uri)| (join_case, DeltaOutputOracle::new(uri, &storage)))
                        .collect::<BTreeMap<_, _>>(),
                );
                let aggregate_uris = delta_matrix_aggregate_table_uris(&run_id, "single");
                let aggregate_sinks = delta_matrix_aggregate_sink_config(&aggregate_uris, &storage);
                delta_matrix_aggregate_oracles = Some(
                    aggregate_uris
                        .into_iter()
                        .map(|(join_case, uri)| (join_case, DeltaOutputOracle::new(uri, &storage)))
                        .collect::<BTreeMap<_, _>>(),
                );
                let window_uris = delta_core_window_table_uris(&run_id, "single");
                let window_sinks = delta_core_window_sink_config(&window_uris, &storage);
                delta_window_output_oracles = Some(
                    window_uris
                        .into_iter()
                        .map(|(kind, uri)| (kind, DeltaOutputOracle::new(uri, &storage)))
                        .collect::<BTreeMap<_, _>>(),
                );
                (
                    sink_config,
                    format!("{matrix_sinks}{aggregate_sinks}{window_sinks}"),
                )
            }
            #[cfg(not(feature = "delta-lake-s3"))]
            panic!("EO join soak requires the delta-lake-s3 feature");
        }
    };
    let commit_oracle = KafkaJoinCommitOracle::new(
        &brokers,
        &consumer_group,
        &left_topic,
        &right_topic,
        kafka_partitions,
    );
    let temporal_commit_oracle = KafkaJoinCommitOracle::new(
        &brokers,
        &format!("{consumer_group}-temporal"),
        &temporal_left_topic,
        &temporal_right_topic,
        kafka_partitions,
    );
    let temporal_load_commit_oracle = KafkaJoinCommitOracle::new(
        &brokers,
        &format!("{consumer_group}-temporal-load"),
        &left_topic,
        &right_topic,
        kafka_partitions,
    );
    let matrix_commit_oracle = KafkaJoinCommitOracle::new(
        &brokers,
        &matrix_consumer_group,
        &matrix_left_topic,
        &matrix_right_topic,
        MATRIX_INPUT_PARTITIONS,
    );
    let mutable_commit_oracle = (delivery == JoinDelivery::AtLeastOnce).then(|| {
        KafkaJoinCommitOracle::new(
            &brokers,
            &mutable_consumer_group,
            &mutable_left_topic,
            &mutable_right_topic,
            MUTABLE_INTERVAL_INPUT_PARTITIONS,
        )
    });
    let source_rps = env_u64("LAMINAR_SOAK_RPS", 400);
    assert!(source_rps > 0, "LAMINAR_SOAK_RPS must be greater than zero");
    assert!(
        source_rps <= MAX_TEMPORAL_LOAD_RPS,
        "LAMINAR_SOAK_RPS must not exceed {MAX_TEMPORAL_LOAD_RPS}; the exact temporal AS-OF oracle requires distinct millisecond timestamps"
    );
    let join_keys = env_u64("LAMINAR_SOAK_JOIN_KEYS", DEFAULT_JOIN_KEYS);
    let zipf_milli = env_u64("LAMINAR_SOAK_ZIPF_MILLI", DEFAULT_ZIPF_MILLI);
    eprintln!(
        "soak: PROFILE mode=single delivery={} seconds={soak_secs} checkpoint_ms={interval_ms} checkpoint_slo_mode={} hot_path_slo_mode={} join_interval_ms={retained_interval_ms} rps={source_rps} keys={join_keys} zipf_milli={zipf_milli} kills={kills} min_live_state_bytes={minimum_live_state_bytes} kafka_sink_linger_ms={SOAK_KAFKA_SINK_LINGER_MS}",
        delivery.label(),
        checkpoint_slo_mode.label(),
        hot_path_slo_mode.label(),
    );
    let temporal_recovery_seed = produce_temporal_pre_fault_input(
        &brokers,
        &temporal_left_topic,
        &temporal_right_topic,
        kafka_partitions,
        join_keys,
    );
    let mut producer = ProducerGuard::spawn(
        brokers.clone(),
        left_topic.clone(),
        right_topic.clone(),
        kafka_partitions,
        source_rps,
        join_keys,
        zipf_milli,
        matrix_event_time_ms(),
    );

    let dir = tempfile::tempdir().expect("single-node join tempdir");
    let log_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive single-node soak log directory");
    eprintln!(
        "soak: single {delivery_label} node log in {}",
        log_dir.display()
    );
    let mut nodes = vec![Node {
        id: 0,
        executable: Arc::clone(&executable),
        config_path: write_single_join_config(
            dir.path(),
            interval_ms,
            &brokers,
            &left_topic,
            &right_topic,
            &temporal_left_topic,
            &temporal_right_topic,
            &consumer_group,
            &matrix_left_topic,
            &matrix_right_topic,
            &matrix_consumer_group,
            delivery,
            &sink_config,
            &matrix_sink_config,
            &mutable_workload,
        ),
        log_path: log_dir.join("node0.log"),
        child: None,
        process_generation: 0,
        http_port: SINGLE_JOIN_PORT,
        fault_trigger_path: None,
        checkpoint_gate_path: Some(dir.path().join("checkpoint-single-join.arm")),
    }];
    let mut latency_evidence = CheckpointLatencyEvidence::default();
    let mut live_state_high_water = None;
    let mut temporal_state_high_water = None;
    let mut window_state_high_water = [None; CORE_WINDOW_CANARIES.len()];
    let initial_spawn = nodes[0].verify_executable_for_spawn();
    nodes[0].spawn(initial_spawn);
    wait_for(
        &format!("single {delivery_label} join node readiness"),
        Duration::from_secs(60),
        || {
            producer.assert_running();
            nodes[0].assert_running();
            nodes[0].is_ready() && nodes[0].epoch().is_some()
        },
    );
    let mut latest_checkpoint = assert_progress(
        &mut nodes,
        Some(&mut producer),
        Some(&commit_oracle),
        recovery_ceiling,
        &format!("single {delivery_label} initial bounded-join progress"),
        None,
    );
    observe_live_join_state(
        &nodes,
        &mut live_state_high_water,
        &mut temporal_state_high_water,
    );
    latest_checkpoint = assert_temporal_idle_checkpoint(
        &mut nodes,
        &temporal_commit_oracle,
        &temporal_recovery_seed.input_boundary,
        durable_cut_window,
        latest_checkpoint,
        &format!("single {delivery_label} pre-fault temporal history"),
    );
    eprintln!(
        "soak: single {delivery_label} temporal right history is durable before fault injection"
    );
    latest_checkpoint = assert_final_input_cut(
        &mut nodes,
        &matrix_commit_oracle,
        &matrix_input_boundary,
        durable_cut_window,
        latest_checkpoint,
    );
    if let (Some(commit_oracle), Some(seed), Some(output)) = (
        mutable_commit_oracle.as_ref(),
        mutable_recovery_seed.as_ref(),
        mutable_output_oracle.as_mut(),
    ) {
        latest_checkpoint = assert_final_input_cut(
            &mut nodes,
            commit_oracle,
            &seed.input_boundary,
            durable_cut_window,
            latest_checkpoint,
        );
        let expected = expected_mutable_interval_transitions();
        assert_mutable_interval_output_phase(
            &mut nodes,
            output,
            &expected[..1],
            None,
            recovery_ceiling,
            "single-node ALO mutable interval pre-fault insertion",
        );
        eprintln!(
            "soak: single ALO mutable interval input and aggregate state are durable before hard restart"
        );
    }
    observe_live_core_window_state(&nodes, &mut window_state_high_water);
    if let Some(output) = matrix_aggregate_oracle.as_mut() {
        assert_matrix_aggregate_final_state_pending(
            output,
            &format!("single-node {delivery_label} pre-fault aggregate matrix"),
        );
    }

    for round in 0..kills {
        producer.assert_running();
        nodes[0].arm_checkpoint_kill("leader");
        wait_for(
            &format!("single {delivery_label} node to enter checkpoint before kill-{round}"),
            Duration::from_secs(45),
            || {
                producer.assert_running();
                nodes[0].assert_running();
                nodes[0].checkpoint_kill_ready("leader")
            },
        );
        let preceding_checkpoint = nodes[0]
            .checkpoint_kill_predecessor("leader")
            .expect("single-node checkpoint gate has no exact durable-cut evidence");
        assert!(
            preceding_checkpoint.checkpoint_id >= latest_checkpoint.checkpoint_id
                && preceding_checkpoint.epoch >= latest_checkpoint.epoch,
            "single {delivery_label} durable checkpoint regressed before kill-{round}: previous={latest_checkpoint:?}, preceding={preceding_checkpoint:?}"
        );
        eprintln!(
            "soak round {}: kill -9 single {delivery_label} node inside checkpoint after durable checkpoint {} epoch {}",
            round + 1,
            preceding_checkpoint.checkpoint_id,
            preceding_checkpoint.epoch,
        );
        latency_evidence
            .finalize_single_node(&nodes[0], Instant::now() + Duration::from_secs(10))
            .unwrap_or_else(|error| {
                panic!(
                    "single {delivery_label} kill-{round} could not finalize checkpoint timing evidence: {error}"
                )
            });
        let restart = nodes[0].verify_executable_for_spawn();
        let recovery_started = Instant::now();
        nodes[0].kill9();
        nodes[0].disarm_checkpoint_kill();
        nodes[0].spawn(restart);
        wait_for(
            &format!("single {delivery_label} node readiness after kill-{round}"),
            recovery_ceiling,
            || {
                producer.assert_running();
                nodes[0].assert_running();
                nodes[0].is_ready() && nodes[0].epoch().is_some()
            },
        );
        latest_checkpoint = assert_progress(
            &mut nodes,
            Some(&mut producer),
            Some(&commit_oracle),
            recovery_ceiling,
            &format!("single {delivery_label} progress after kill-{round}"),
            Some(preceding_checkpoint),
        );
        assert_recovery_within(
            recovery_started,
            recovery_ceiling,
            &format!("single {delivery_label} kill-to-durable-join progress"),
        );
        observe_live_join_state(
            &nodes,
            &mut live_state_high_water,
            &mut temporal_state_high_water,
        );
    }

    if let (Some(commit_oracle), Some(seed), Some(output)) = (
        mutable_commit_oracle.as_ref(),
        mutable_recovery_seed.as_mut(),
        mutable_output_oracle.as_mut(),
    ) {
        let expected = expected_mutable_interval_transitions();
        let updated = MutableIntervalRightRow {
            value: MUTABLE_INTERVAL_UPDATED_VALUE,
            ..seed.right
        };
        let (right_boundary, frozen_input_at) = produce_mutable_interval_right_mutations(
            &brokers,
            &mutable_right_topic,
            &[MutableIntervalMutation {
                before: Some(seed.right),
                after: Some(updated),
                operation: "u",
            }],
        );
        merge_mutable_interval_right_boundary(&mut seed.input_boundary, &right_boundary);
        assert_mutable_interval_output_phase(
            &mut nodes,
            output,
            &expected[..2],
            Some(frozen_input_at),
            recovery_ceiling,
            "single-node ALO restored mutable interval update",
        );

        let (right_boundary, frozen_input_at) = produce_mutable_interval_right_mutations(
            &brokers,
            &mutable_right_topic,
            &[MutableIntervalMutation {
                before: Some(updated),
                after: None,
                operation: "d",
            }],
        );
        merge_mutable_interval_right_boundary(&mut seed.input_boundary, &right_boundary);
        assert_mutable_interval_output_phase(
            &mut nodes,
            output,
            &expected[..3],
            Some(frozen_input_at),
            recovery_ceiling,
            "single-node ALO mutable interval delete retraction",
        );

        let revived = MutableIntervalRightRow {
            value: MUTABLE_INTERVAL_REVIVED_VALUE,
            ..updated
        };
        let (right_boundary, frozen_input_at) = produce_mutable_interval_right_mutations(
            &brokers,
            &mutable_right_topic,
            &[MutableIntervalMutation {
                before: None,
                after: Some(revived),
                operation: "c",
            }],
        );
        merge_mutable_interval_right_boundary(&mut seed.input_boundary, &right_boundary);
        assert_mutable_interval_output_phase(
            &mut nodes,
            output,
            &expected,
            Some(frozen_input_at),
            recovery_ceiling,
            "single-node ALO mutable interval revival",
        );
        seed.right = revived;
        latest_checkpoint = assert_final_input_cut(
            &mut nodes,
            commit_oracle,
            &seed.input_boundary,
            durable_cut_window,
            latest_checkpoint,
        );
        assert_mutable_interval_output_stable(
            &mut nodes,
            output,
            recovery_ceiling,
            "single-node ALO mutable interval weighted output",
        );
        eprintln!(
            "soak: single ALO mutable bounded interval recovered its pre-fault row, netted update 10->20 without double-counting, emitted a delete tombstone, revived at 30, and committed checkpoint {}",
            latest_checkpoint.checkpoint_id
        );
    }

    let steady_deadline = Instant::now() + Duration::from_secs(soak_secs);
    let mut round = 0u64;
    while remaining_at(steady_deadline, Instant::now()).is_some() {
        round = round
            .checked_add(1)
            .expect("single-node soak round overflow");
        latest_checkpoint = assert_progress(
            &mut nodes,
            Some(&mut producer),
            Some(&commit_oracle),
            recovery_ceiling,
            &format!("single {delivery_label} steady bounded-join progress"),
            Some(latest_checkpoint),
        );
        observe_live_join_state(
            &nodes,
            &mut live_state_high_water,
            &mut temporal_state_high_water,
        );
        if let Some(remaining) = remaining_at(steady_deadline, Instant::now()) {
            std::thread::sleep(remaining.min(Duration::from_secs(2)));
        }
    }
    eprintln!(
        "soak: single {delivery_label} completed {round} steady rounds and {kills} hard restarts"
    );

    let (matrix_final_boundary, window_frozen_input_at) = produce_matrix_post_fault_inputs(
        &brokers,
        &matrix_left_topic,
        &matrix_right_topic,
        matrix_pre_fault_event_time,
    );
    let expected_windows = expected_core_window_outputs(matrix_pre_fault_event_time);
    match delivery {
        JoinDelivery::AtLeastOnce => assert_kafka_core_window_outputs(
            &mut nodes,
            window_output_oracle
                .as_mut()
                .expect("single ALO Kafka CoreWindow oracle"),
            &expected_windows,
            window_frozen_input_at,
            recovery_ceiling,
            "single-node ALO CoreWindow canaries",
        ),
        JoinDelivery::ExactlyOnce => {
            #[cfg(feature = "delta-lake-s3")]
            assert_delta_core_window_outputs(
                &mut nodes,
                delta_window_output_oracles
                    .as_ref()
                    .expect("single EO Delta CoreWindow oracles"),
                &expected_windows,
                window_frozen_input_at,
                visibility_slo_mode,
                recovery_ceiling,
                "single-node EO CoreWindow canaries",
            );
            #[cfg(not(feature = "delta-lake-s3"))]
            unreachable!("EO runner is unavailable without delta-lake-s3");
        }
    }
    latest_checkpoint = assert_final_input_cut(
        &mut nodes,
        &matrix_commit_oracle,
        &matrix_final_boundary,
        durable_cut_window,
        latest_checkpoint,
    );
    eprintln!(
        "soak: single {delivery_label} post-fault join matrix is durable through checkpoint {}",
        latest_checkpoint.checkpoint_id
    );

    assert_active_load_throughput(
        &mut nodes,
        &mut producer,
        &[
            ("bounded", &commit_oracle),
            ("temporal_load", &temporal_load_commit_oracle),
        ],
        &[
            ("bounded", output_oracles.kafka_bounded.as_ref()),
            ("temporal_load", output_oracles.kafka_temporal_load.as_ref()),
        ],
        source_rps,
        interval_ms,
        SINGLE_CHECKPOINT_TIMEOUT,
        recovery_ceiling,
        delivery,
    );
    observe_live_join_state(
        &nodes,
        &mut live_state_high_water,
        &mut temporal_state_high_water,
    );
    let aggregate_state_sample = Some(live_operator_state_bytes(&nodes, "soak_join_aggregate"));
    assert_live_join_state_bytes(
        aggregate_state_sample,
        continuous_aggregate_minimum_bytes(join_keys),
        nodes.len(),
        &format!("single {delivery_label} continuous join aggregate"),
    );

    let finalized_latency = latency_evidence
        .aggregate()
        .unwrap_or_else(|error| panic!("single {delivery_label} checkpoint latency: {error}"));
    let finalized_stalls = exact_prometheus_count(
        finalized_latency.pipeline_stall_observations,
        "finalized checkpoint pipeline-stall observations",
    )
    .unwrap_or_else(|error| panic!("single {delivery_label} checkpoint latency: {error}"));
    let finalized_captures = exact_prometheus_count(
        finalized_latency.state_capture_observations,
        "finalized checkpoint state-capture observations",
    )
    .unwrap_or_else(|error| panic!("single {delivery_label} checkpoint latency: {error}"));
    let required_live_stalls = required_live_checkpoint_observations(
        checkpoint_slo_mode,
        MIN_CHECKPOINT_PIPELINE_STALL_OBSERVATIONS,
        finalized_stalls,
    );
    let required_live_captures = required_live_checkpoint_observations(
        checkpoint_slo_mode,
        MIN_CHECKPOINT_STATE_CAPTURE_OBSERVATIONS,
        finalized_captures,
    );
    let live_latency = nodes[0]
        .checkpoint_latency_metrics()
        .unwrap_or_else(|| panic!("single {delivery_label} node exposed no checkpoint metrics"));
    let observation_budget = checkpoint_observation_budget(
        interval_ms,
        SINGLE_CHECKPOINT_TIMEOUT,
        recovery_ceiling,
        &[(required_live_stalls, required_live_captures, live_latency)],
    )
    .unwrap_or_else(|error| {
        panic!("single {delivery_label} checkpoint observation budget: {error}")
    });
    eprintln!(
        "soak: single {delivery_label} checkpoint observation budget total={:?} collection={:?} max_missing={} max_cadence={:?} capped={}",
        observation_budget.total,
        observation_budget.collection,
        observation_budget.max_missing,
        observation_budget.max_cadence,
        observation_budget.capped,
    );
    wait_for(
        &format!("single {delivery_label} checkpoint latency observations under active load"),
        observation_budget.total,
        || {
            producer.assert_running();
            nodes[0].assert_running();
            nodes[0]
                .checkpoint_latency_metrics()
                .is_some_and(|metrics| {
                    metrics.pipeline_stall_observations >= required_live_stalls as f64
                        && metrics.state_capture_observations >= required_live_captures as f64
                })
        },
    );
    nodes[0].arm_checkpoint_kill("leader");
    wait_for(
        &format!("single {delivery_label} final checkpoint timing cut"),
        Duration::from_secs(45),
        || {
            producer.assert_running();
            nodes[0].assert_running();
            nodes[0].checkpoint_kill_ready("leader")
        },
    );
    latency_evidence
        .finalize_single_node(&nodes[0], Instant::now() + Duration::from_secs(10))
        .unwrap_or_else(|error| {
            panic!(
                "single {delivery_label} final checkpoint timing evidence did not stabilize: {error}"
            )
        });
    nodes[0].disarm_checkpoint_kill();
    latency_evidence
        .validate_observed_generations(&nodes)
        .unwrap_or_else(|error| {
            panic!("single {delivery_label} checkpoint latency evidence: {error}")
        });
    latency_evidence.report(minimum_live_state_bytes, checkpoint_slo_mode);

    // First make the future-dated temporal mutations durable while the continuous sources still
    // pin the global admission floor near wall clock. Only then publish the per-partition closing
    // sentinels: an idle partition may otherwise be excluded from the frontier minimum while its
    // revival is still waiting in Kafka, allowing another partition's sentinel to overtake it.
    let temporal_post = produce_temporal_post_fault_revivals(
        &brokers,
        &temporal_left_topic,
        &temporal_right_topic,
        kafka_partitions,
        &temporal_recovery_seed,
    );
    let mut temporal_input_boundary = temporal_recovery_seed.input_boundary.clone();
    merge_offset_boundary(
        &mut temporal_input_boundary,
        &temporal_post.revival_input_boundary,
    );
    latest_checkpoint = assert_final_input_cut(
        &mut nodes,
        &temporal_commit_oracle,
        &temporal_input_boundary,
        durable_cut_window,
        latest_checkpoint,
    );
    let temporal_closing_boundary = produce_temporal_post_fault_sentinels(
        &brokers,
        &temporal_left_topic,
        &temporal_right_topic,
        kafka_partitions,
        &temporal_recovery_seed,
    );
    merge_offset_boundary(&mut temporal_input_boundary, &temporal_closing_boundary);
    latest_checkpoint = assert_final_input_cut(
        &mut nodes,
        &temporal_commit_oracle,
        &temporal_input_boundary,
        durable_cut_window,
        latest_checkpoint,
    );
    let (produced_prefix, _bounded_frozen_input_at) = producer.stop();
    let temporal_frontier_released_at = Instant::now();
    assert_temporal_canaries(
        &mut nodes,
        &mut output_oracles,
        delivery,
        &temporal_post,
        temporal_frontier_released_at,
        visibility_slo_mode,
        recovery_ceiling,
        "single-node",
    );
    #[cfg(feature = "delta-lake-s3")]
    let delta_visibility_boundaries = (delivery == JoinDelivery::ExactlyOnce).then(|| {
        let outputs = delta_join_exact_outputs(
            &output_oracles,
            &produced_prefix,
            _bounded_frozen_input_at,
            "single-node EO bounded join",
            "single-node EO temporal ASOF load",
        );
        capture_delta_visibility_boundaries(&mut nodes, &outputs, visibility_slo_mode)
    });
    let produced_count = produced_prefix.count;
    assert!(
        produced_count > 0,
        "single {delivery_label} producer emitted no input IDs"
    );
    let achieved_rps = produced_count as f64 / produced_prefix.elapsed.as_secs_f64();
    assert!(
        achieved_rps >= source_rps as f64 * ACTIVE_LOAD_MINIMUM_RATIO,
        "single {delivery_label} producer achieved only {achieved_rps:.1} logical pairs/s against target {source_rps}"
    );
    let expected_boundaries = usize::try_from(kafka_partitions)
        .expect("Kafka partition count fits usize")
        .saturating_mul(2);
    assert_eq!(
        produced_prefix.end_offsets.len(),
        expected_boundaries,
        "single {delivery_label} producer omitted a join-input partition boundary"
    );
    assert!(
        produced_prefix.end_offsets.iter().all(|offset| *offset > 0),
        "single {delivery_label} producer did not write every join-input partition: {:?}",
        produced_prefix.end_offsets
    );
    latest_checkpoint = assert_final_input_cuts(
        &mut nodes,
        &[&commit_oracle, &temporal_load_commit_oracle],
        &produced_prefix.end_offsets,
        durable_cut_window,
        latest_checkpoint,
    );
    eprintln!(
        "soak: single {delivery_label} frozen bounded and temporal input prefix is durable through checkpoint {}",
        latest_checkpoint.checkpoint_id
    );
    #[cfg(feature = "delta-lake-s3")]
    let completed_delta_outputs = (delivery == JoinDelivery::ExactlyOnce).then(|| {
        let outputs = delta_join_exact_outputs(
            &output_oracles,
            &produced_prefix,
            _bounded_frozen_input_at,
            "single-node EO bounded join",
            "single-node EO temporal ASOF load",
        );
        wait_delta_exact_outputs(
            &mut nodes,
            &outputs,
            delta_visibility_boundaries
                .as_ref()
                .expect("single EO visibility boundaries captured before final cuts"),
            recovery_ceiling,
        )
    });
    match delivery {
        JoinDelivery::AtLeastOnce => {
            assert_frozen_kafka_outputs(
                &mut nodes,
                output_oracles
                    .kafka_bounded
                    .as_mut()
                    .expect("single ALO Kafka output oracle"),
                produced_count,
                &produced_prefix.expected_pairs,
                recovery_ceiling,
                "single-node ALO bounded join",
            );
            assert_frozen_kafka_outputs(
                &mut nodes,
                output_oracles
                    .kafka_temporal_load
                    .as_mut()
                    .expect("single ALO temporal-load Kafka output oracle"),
                produced_count,
                &produced_prefix.expected_temporal_pairs,
                recovery_ceiling,
                "single-node ALO temporal ASOF load",
            );
            assert_kafka_matrix_outputs(
                &mut nodes,
                matrix_output_oracle
                    .as_mut()
                    .expect("single ALO Kafka matrix oracle"),
                recovery_ceiling,
                "single-node ALO bounded-join matrix",
            );
            assert_kafka_matrix_aggregates(
                &mut nodes,
                matrix_aggregate_oracle
                    .as_mut()
                    .expect("single ALO Kafka matrix aggregate oracle"),
                recovery_ceiling,
                "single-node ALO bounded-join aggregate matrix",
            );
        }
        JoinDelivery::ExactlyOnce => {
            #[cfg(feature = "delta-lake-s3")]
            {
                let exact_outputs = delta_join_exact_outputs(
                    &output_oracles,
                    &produced_prefix,
                    _bounded_frozen_input_at,
                    "single-node EO bounded join",
                    "single-node EO temporal ASOF load",
                );
                assert_delta_exact_outputs_stable(
                    &mut nodes,
                    &exact_outputs,
                    completed_delta_outputs
                        .as_deref()
                        .expect("single EO outputs completed before final cuts"),
                    recovery_ceiling,
                );
                assert_delta_matrix_outputs(
                    &mut nodes,
                    delta_matrix_output_oracles
                        .as_ref()
                        .expect("single EO Delta raw matrix oracles"),
                    recovery_ceiling,
                    "single-node EO bounded-join raw matrix",
                );
                assert_delta_matrix_aggregates(
                    &mut nodes,
                    delta_matrix_aggregate_oracles
                        .as_ref()
                        .expect("single EO Delta matrix aggregate oracles"),
                    recovery_ceiling,
                    "single-node EO bounded-join aggregate matrix",
                );
            }
            #[cfg(not(feature = "delta-lake-s3"))]
            unreachable!("EO runner is unavailable without delta-lake-s3");
        }
    }

    assert_hot_path_latency(
        &nodes,
        &format!("single-node {delivery_label} stateful workload"),
        hot_path_slo_mode,
    );
    assert_live_join_state_bytes(
        live_state_high_water,
        minimum_live_state_bytes,
        nodes.len(),
        &format!("single-node {delivery_label} bounded join"),
    );
    assert_live_join_state_bytes(
        temporal_state_high_water,
        minimum_live_state_bytes.max(1),
        nodes.len(),
        &format!("single-node {delivery_label} temporal ASOF load"),
    );
    assert_core_window_state_bounds(
        &window_state_high_water,
        nodes.len(),
        &format!("single-node {delivery_label}"),
    );
}

#[test]
#[ignore = "spawns 3 real laminardb processes; run with --ignored"]
#[cfg(feature = "kafka")]
fn three_node_alo_join_kill9_soak() {
    run_three_node_join_kill9_soak(JoinDelivery::AtLeastOnce, false);
}

#[test]
#[ignore = "spawns 3 real laminardb processes with a durable WebSocket subscription"]
#[cfg(feature = "kafka")]
fn three_node_alo_cluster_subscription_kill9_soak() {
    run_three_node_join_kill9_soak(JoinDelivery::AtLeastOnce, true);
}

#[test]
#[ignore = "spawns 3 real laminardb processes with Kafka and Delta S3; run with --ignored"]
#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
fn three_node_eo_join_kill9_soak() {
    run_three_node_join_kill9_soak(JoinDelivery::ExactlyOnce, false);
}

#[cfg(feature = "kafka")]
fn run_three_node_join_kill9_soak(delivery: JoinDelivery, subscription_soak: bool) {
    let delivery_label = delivery.label();
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let soak_secs = env_u64("LAMINAR_SOAK_SECONDS", 90);
    let interval_ms = env_u64("LAMINAR_SOAK_INTERVAL_MS", 500);
    let retained_interval_ms = join_interval_ms();
    let minimum_live_state_bytes = env_u64("LAMINAR_SOAK_MIN_LIVE_STATE_BYTES", 0);
    let checkpoint_slo_mode = SloMode::from_environment(SOAK_CHECKPOINT_SLO_MODE_ENV)
        .unwrap_or_else(|error| panic!("invalid checkpoint SLO mode: {error}"));
    let hot_path_slo_mode = SloMode::from_environment(SOAK_HOT_SLO_MODE_ENV)
        .unwrap_or_else(|error| panic!("invalid hot-path SLO mode: {error}"));
    let visibility_slo_mode = resolve_visibility_slo_mode(delivery);
    assert!(
        interval_ms >= 100,
        "LAMINAR_SOAK_INTERVAL_MS must be at least 100"
    );
    validate_retained_state_profile(soak_secs, retained_interval_ms, minimum_live_state_bytes);
    let recovery_ceiling = recovery_ceiling();
    let checkpoint_timeout = cluster_checkpoint_timeout(delivery, s3_emulator_enabled());
    validate_checkpoint_liveness(interval_ms, checkpoint_timeout, recovery_ceiling);
    let durable_output_window = recovery_aware_durable_progress_window(
        interval_ms,
        checkpoint_timeout,
        recovery_ceiling,
        1,
    );
    let durable_cut_window = recovery_aware_durable_progress_window(
        interval_ms,
        checkpoint_timeout,
        recovery_ceiling,
        2,
    );
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
    validate_matrix_recovery_horizon(max_kills, recovery_ceiling, soak_secs, retained_interval_ms);
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
    let brokers = std::env::var("LAMINAR_SOAK_KAFKA_SOURCE_BROKERS")
        .expect("cluster join soak requires LAMINAR_SOAK_KAFKA_SOURCE_BROKERS");
    let run_id = soak_run_id();
    let checkpoint_url =
        scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "checkpoints");
    let left_topic = format!("soak-cluster-left-{run_id}");
    let right_topic = format!("soak-cluster-right-{run_id}");
    let temporal_left_topic = format!("soak-cluster-temporal-left-{run_id}");
    let temporal_right_topic = format!("soak-cluster-temporal-right-{run_id}");
    let matrix_left_topic = format!("soak-cluster-matrix-left-{run_id}");
    let matrix_right_topic = format!("soak-cluster-matrix-right-{run_id}");
    let output_topic = format!("soak-cluster-out-{run_id}");
    let temporal_load_output_topic = format!("soak-cluster-temporal-load-{run_id}");
    let temporal_output_topic = format!("soak-cluster-temporal-{run_id}");
    let temporal_probe_output_topic = format!("soak-cluster-temporal-probe-{run_id}");
    let matrix_output_topic = format!("soak-cluster-matrix-{run_id}");
    let window_output_topic = format!("soak-cluster-window-{run_id}");
    let consumer_group = format!("soak-cluster-{run_id}");
    let matrix_consumer_group = format!("soak-cluster-matrix-{run_id}");
    // Kafka partitioning is independent from engine key-group cardinality. The provider hashes
    // each source/topic/partition identity onto the current key-group topology; the producer below
    // assigns records round-robin so every external partition receives deterministic traffic.
    kafka_create_topic(&brokers, &left_topic, kafka_partitions);
    kafka_create_topic(&brokers, &right_topic, kafka_partitions);
    kafka_create_topic(&brokers, &temporal_left_topic, kafka_partitions);
    kafka_create_topic(&brokers, &temporal_right_topic, kafka_partitions);
    kafka_create_topic(&brokers, &matrix_left_topic, MATRIX_INPUT_PARTITIONS);
    kafka_create_topic(&brokers, &matrix_right_topic, MATRIX_INPUT_PARTITIONS);
    let (matrix_input_boundary, matrix_pre_fault_event_time) =
        produce_matrix_pre_fault_inputs(&brokers, &matrix_left_topic, &matrix_right_topic);
    let mut output_oracles;
    #[cfg(feature = "delta-lake-s3")]
    let mut matrix_output_oracle = None;
    #[cfg(not(feature = "delta-lake-s3"))]
    let mut matrix_output_oracle;
    #[cfg(feature = "delta-lake-s3")]
    let mut delta_matrix_output_oracles = None;
    #[cfg(feature = "delta-lake-s3")]
    let mut delta_matrix_aggregate_oracles = None;
    #[cfg(feature = "delta-lake-s3")]
    let mut window_output_oracle = None;
    #[cfg(not(feature = "delta-lake-s3"))]
    let mut window_output_oracle;
    #[cfg(feature = "delta-lake-s3")]
    let mut delta_window_output_oracles = None;
    let (sink_config, matrix_sink_config) = match delivery {
        JoinDelivery::AtLeastOnce => {
            let (configured_oracles, sink_config) = JoinOutputOracles::kafka(
                &brokers,
                &output_topic,
                &temporal_load_output_topic,
                &temporal_output_topic,
                &temporal_probe_output_topic,
            );
            output_oracles = configured_oracles;
            kafka_create_topic(&brokers, &matrix_output_topic, OUTPUT_TOPIC_PARTITIONS);
            kafka_create_topic(&brokers, &window_output_topic, OUTPUT_TOPIC_PARTITIONS);
            matrix_output_oracle = Some(KafkaMatrixOracle::new(&brokers, &matrix_output_topic));
            window_output_oracle = Some(KafkaCoreWindowOracle::new(&brokers, &window_output_topic));
            // Kafka's changelog-capable upsert envelope is deliberately singleton, so the
            // three-node ALO runner cannot attach it to the differential aggregate canaries.
            // Single-node ALO validates those keyed outputs, while cluster EO validates the
            // same aggregate states and weights through coordinated Delta append.
            (
                sink_config,
                format!(
                    "{}{}",
                    kafka_matrix_sink_config(&brokers, &matrix_output_topic),
                    kafka_core_window_sink_config(&brokers, &window_output_topic),
                ),
            )
        }
        JoinDelivery::ExactlyOnce => {
            #[cfg(feature = "delta-lake-s3")]
            {
                let storage = DeltaSoakStorage::from_environment();
                let (configured_oracles, sink_config) =
                    JoinOutputOracles::delta(&run_id, "cluster", &storage);
                output_oracles = configured_oracles;
                let matrix_uris = delta_matrix_output_table_uris(&run_id, "cluster");
                let matrix_sinks = delta_matrix_output_sink_config(&matrix_uris, &storage);
                delta_matrix_output_oracles = Some(
                    matrix_uris
                        .into_iter()
                        .map(|(join_case, uri)| (join_case, DeltaOutputOracle::new(uri, &storage)))
                        .collect::<BTreeMap<_, _>>(),
                );
                let aggregate_uris = delta_matrix_aggregate_table_uris(&run_id, "cluster");
                let aggregate_sinks = delta_matrix_aggregate_sink_config(&aggregate_uris, &storage);
                delta_matrix_aggregate_oracles = Some(
                    aggregate_uris
                        .into_iter()
                        .map(|(join_case, uri)| (join_case, DeltaOutputOracle::new(uri, &storage)))
                        .collect::<BTreeMap<_, _>>(),
                );
                let window_uris = delta_core_window_table_uris(&run_id, "cluster");
                let window_sinks = delta_core_window_sink_config(&window_uris, &storage);
                delta_window_output_oracles = Some(
                    window_uris
                        .into_iter()
                        .map(|(kind, uri)| (kind, DeltaOutputOracle::new(uri, &storage)))
                        .collect::<BTreeMap<_, _>>(),
                );
                (
                    sink_config,
                    format!("{matrix_sinks}{aggregate_sinks}{window_sinks}"),
                )
            }
            #[cfg(not(feature = "delta-lake-s3"))]
            panic!("EO join soak requires the delta-lake-s3 feature");
        }
    };
    let commit_oracle = KafkaJoinCommitOracle::new(
        &brokers,
        &consumer_group,
        &left_topic,
        &right_topic,
        kafka_partitions,
    );
    let temporal_commit_oracle = KafkaJoinCommitOracle::new(
        &brokers,
        &format!("{consumer_group}-temporal"),
        &temporal_left_topic,
        &temporal_right_topic,
        kafka_partitions,
    );
    let temporal_load_commit_oracle = KafkaJoinCommitOracle::new(
        &brokers,
        &format!("{consumer_group}-temporal-load"),
        &left_topic,
        &right_topic,
        kafka_partitions,
    );
    let matrix_commit_oracle = KafkaJoinCommitOracle::new(
        &brokers,
        &matrix_consumer_group,
        &matrix_left_topic,
        &matrix_right_topic,
        MATRIX_INPUT_PARTITIONS,
    );
    let source_rps = env_u64("LAMINAR_SOAK_RPS", 400);
    assert!(source_rps > 0, "LAMINAR_SOAK_RPS must be greater than zero");
    assert!(
        source_rps <= MAX_TEMPORAL_LOAD_RPS,
        "LAMINAR_SOAK_RPS must not exceed {MAX_TEMPORAL_LOAD_RPS}; the exact temporal AS-OF oracle requires distinct millisecond timestamps"
    );
    let join_keys = env_u64("LAMINAR_SOAK_JOIN_KEYS", DEFAULT_JOIN_KEYS);
    let zipf_milli = env_u64("LAMINAR_SOAK_ZIPF_MILLI", DEFAULT_ZIPF_MILLI);
    eprintln!(
        "soak: PROFILE mode=cluster delivery={} seconds={soak_secs} checkpoint_ms={interval_ms} checkpoint_slo_mode={} hot_path_slo_mode={} join_interval_ms={retained_interval_ms} rps={source_rps} keys={join_keys} zipf_milli={zipf_milli} kills={max_kills} min_live_state_bytes={minimum_live_state_bytes} kafka_sink_linger_ms={SOAK_KAFKA_SINK_LINGER_MS}",
        delivery.label(),
        checkpoint_slo_mode.label(),
        hot_path_slo_mode.label(),
    );
    let temporal_recovery_seed = produce_temporal_pre_fault_input(
        &brokers,
        &temporal_left_topic,
        &temporal_right_topic,
        kafka_partitions,
        join_keys,
    );
    let mut producer = ProducerGuard::spawn(
        brokers.clone(),
        left_topic.clone(),
        right_topic.clone(),
        kafka_partitions,
        source_rps,
        join_keys,
        zipf_milli,
        matrix_event_time_ms(),
    );

    // Node logs under target/ (not the tempdir) so they survive a failed run for post-mortem.
    let log_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive cluster soak log directory");
    eprintln!("soak: node logs in {}", log_dir.display());
    let subscription_workload = subscription_soak
        .then(cluster_subscription_workload_config)
        .unwrap_or_default();

    let mut nodes: Vec<Node> = (0..NODES)
        .map(|id| Node {
            id,
            executable: Arc::clone(&executable),
            config_path: write_config(
                dir.path(),
                id,
                interval_ms,
                key_group_count,
                &checkpoint_url,
                &brokers,
                &left_topic,
                &right_topic,
                &temporal_left_topic,
                &temporal_right_topic,
                &consumer_group,
                &matrix_left_topic,
                &matrix_right_topic,
                &matrix_consumer_group,
                delivery,
                &sink_config,
                &matrix_sink_config,
                subscription_workload,
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
    let mut exact_timing_evidence =
        CheckpointBarrierTimingEvidence::with_artifact_directory(log_dir.clone());
    let mut live_state_high_water = None;
    let mut temporal_state_high_water = None;
    let mut window_state_high_water = [None; CORE_WINDOW_CANARIES.len()];
    for n in &mut nodes {
        let initial_spawn = n.verify_executable_for_spawn();
        n.spawn(initial_spawn);
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
    observe_live_join_state(
        &nodes,
        &mut live_state_high_water,
        &mut temporal_state_high_water,
    );
    let all_live_nodes: BTreeSet<usize> = (0..NODES).collect();
    let mut local_convergence = wait_for_local_assignment_convergence(
        &mut nodes,
        &all_live_nodes,
        Instant::now() + Duration::from_secs(60),
        "stable startup local assignment",
    );
    exact_timing_evidence.capture_nodes_bound(
        &nodes,
        &local_convergence,
        Instant::now() + Duration::from_secs(10),
        "stable startup",
    );
    let mut subscription_observer = subscription_soak.then(|| {
        let mut observer = ClusterSubscriptionObserver::spawn(
            dir.path().join("cluster-subscription-progress.log"),
        );
        observer.wait_until_attached(Duration::from_secs(30));
        observer
    });
    latest_checkpoint = assert_temporal_idle_checkpoint(
        &mut nodes,
        &temporal_commit_oracle,
        &temporal_recovery_seed.input_boundary,
        durable_cut_window,
        latest_checkpoint,
        &format!("three-node {delivery_label} pre-fault temporal history"),
    );
    eprintln!(
        "soak: three-node {delivery_label} temporal right history is durable before fault injection"
    );
    latest_checkpoint = assert_final_input_cut(
        &mut nodes,
        &matrix_commit_oracle,
        &matrix_input_boundary,
        durable_cut_window,
        latest_checkpoint,
    );
    observe_live_core_window_state(&nodes, &mut window_state_high_water);
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
            &local_convergence,
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
        let previous_assignment_fence = local_convergence
            .snapshot
            .assignment_fence()
            .expect("pre-kill converged assignment is canonical");
        let previous_victim_timing_authority = checkpoint_barrier_timing_authority(
            &previous_victim_evidence,
            &previous_assignment_fence,
        )
        .unwrap_or_else(|error| panic!("kill-{round} victim timing authority: {error}"));
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
                previous_victim_timing_authority,
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
        eprintln!("soak round {round}: kill -9 delivered to {victim_role} node {victim}");
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
        let survivor_fence = survivor_convergence
            .snapshot
            .assignment_fence()
            .expect("converged survivor assignment is canonical");
        for survivor in &survivor_nodes {
            let evidence = survivor_convergence
                .evidence_by_node
                .get(survivor)
                .unwrap_or_else(|| {
                    panic!("kill-{round} converged authority omitted survivor node{survivor}")
                });
            let expected_authority = checkpoint_barrier_timing_authority(evidence, &survivor_fence)
                .unwrap_or_else(|error| {
                    panic!("kill-{round} survivor node{survivor} timing authority: {error}")
                });
            exact_timing_evidence
                .capture_node(
                    &nodes[*survivor],
                    Some(expected_authority),
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
        let restart = nodes[victim].verify_executable_for_spawn();
        let rejoin_started = Instant::now();
        let rejoin_deadline = rejoin_started + recovery_ceiling;
        nodes[victim].spawn(restart);
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
            &rejoined_convergence,
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
        observe_live_join_state(
            &nodes,
            &mut live_state_high_water,
            &mut temporal_state_high_water,
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
            &local_convergence,
            Instant::now() + Duration::from_secs(10),
            &format!("steady round {round}"),
        );
        observe_live_join_state(
            &nodes,
            &mut live_state_high_water,
            &mut temporal_state_high_water,
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

    let (matrix_final_boundary, _window_frozen_input_at) = produce_matrix_post_fault_inputs(
        &brokers,
        &matrix_left_topic,
        &matrix_right_topic,
        matrix_pre_fault_event_time,
    );
    let expected_windows = expected_core_window_outputs(matrix_pre_fault_event_time);
    latest_checkpoint = assert_final_input_cut(
        &mut nodes,
        &matrix_commit_oracle,
        &matrix_final_boundary,
        durable_cut_window,
        latest_checkpoint,
    );
    eprintln!(
        "soak: three-node {delivery_label} post-fault join matrix is durable through checkpoint {}",
        latest_checkpoint.checkpoint_id
    );

    assert_active_load_throughput(
        &mut nodes,
        &mut producer,
        &[
            ("bounded", &commit_oracle),
            ("temporal_load", &temporal_load_commit_oracle),
        ],
        &[
            ("bounded", output_oracles.kafka_bounded.as_ref()),
            ("temporal_load", output_oracles.kafka_temporal_load.as_ref()),
        ],
        source_rps,
        interval_ms,
        checkpoint_timeout,
        recovery_ceiling,
        delivery,
    );
    observe_live_join_state(
        &nodes,
        &mut live_state_high_water,
        &mut temporal_state_high_water,
    );
    let aggregate_state_sample = Some(live_operator_state_bytes(&nodes, "soak_join_aggregate"));
    assert_live_join_state_bytes(
        aggregate_state_sample,
        continuous_aggregate_minimum_bytes(join_keys),
        nodes.len(),
        &format!("three-node {delivery_label} continuous join aggregate"),
    );
    exact_timing_evidence.capture_nodes_bound(
        &nodes,
        &local_convergence,
        Instant::now() + Duration::from_secs(10),
        "active-load boundary",
    );
    if let Some(evidence) = &explicit_fault_evidence {
        assert_explicit_fault_recovery_evidence(&nodes, evidence);
    }

    let finalized_latency_by_node = latency_evidence
        .aggregate_by_node()
        .unwrap_or_else(|error| panic!("three-node {delivery_label} checkpoint latency: {error}"));
    let required_live_observations = nodes
        .iter()
        .map(|node| {
            let finalized_stalls = finalized_latency_by_node
                .get(&node.id)
                .map(|snapshot| {
                    exact_prometheus_count(
                        snapshot.pipeline_stall_observations,
                        "finalized checkpoint pipeline-stall observations",
                    )
                    .unwrap_or_else(|error| {
                        panic!("three-node {delivery_label} checkpoint latency: {error}")
                    })
                })
                .unwrap_or(0);
            let finalized_captures = finalized_latency_by_node
                .get(&node.id)
                .map(|snapshot| {
                    exact_prometheus_count(
                        snapshot.state_capture_observations,
                        "finalized checkpoint state-capture observations",
                    )
                    .unwrap_or_else(|error| {
                        panic!("three-node {delivery_label} checkpoint latency: {error}")
                    })
                })
                .unwrap_or(0);
            let stalls = required_live_checkpoint_observations(
                checkpoint_slo_mode,
                MIN_CHECKPOINT_PIPELINE_STALL_OBSERVATIONS,
                finalized_stalls,
            );
            let captures = required_live_checkpoint_observations(
                checkpoint_slo_mode,
                MIN_CHECKPOINT_STATE_CAPTURE_OBSERVATIONS,
                finalized_captures,
            );
            (stalls, captures)
        })
        .collect::<Vec<_>>();
    let live_observation_budget = nodes
        .iter()
        .zip(&required_live_observations)
        .map(|(node, (required_stalls, required_captures))| {
            let metrics = node.checkpoint_latency_metrics().unwrap_or_else(|| {
                panic!(
                    "three-node {delivery_label} node{} exposed no checkpoint metrics",
                    node.id
                )
            });
            (*required_stalls, *required_captures, metrics)
        })
        .collect::<Vec<_>>();
    let observation_budget = checkpoint_observation_budget(
        interval_ms,
        CLUSTER_CHECKPOINT_TIMEOUT,
        recovery_ceiling,
        &live_observation_budget,
    )
    .unwrap_or_else(|error| {
        panic!("three-node {delivery_label} checkpoint observation budget: {error}")
    });
    eprintln!(
        "soak: three-node {delivery_label} checkpoint observation budget total={:?} collection={:?} max_missing={} max_cadence={:?} capped={}",
        observation_budget.total,
        observation_budget.collection,
        observation_budget.max_missing,
        observation_budget.max_cadence,
        observation_budget.capped,
    );
    wait_for(
        &format!(
            "three-node {delivery_label} checkpoint latency observations per node under active load"
        ),
        observation_budget.total,
        || {
            assert_running_nodes(&mut nodes);
            producer.assert_running();
            nodes.iter().zip(&required_live_observations).all(
                |(node, (required_stalls, required_captures))| {
                    node.checkpoint_latency_metrics().is_some_and(|metrics| {
                        metrics.pipeline_stall_observations >= *required_stalls as f64
                            && metrics.state_capture_observations >= *required_captures as f64
                    })
                },
            )
        },
    );
    let final_fence = local_convergence
        .snapshot
        .assignment_fence()
        .expect("final converged assignment is canonical");
    for node in &nodes {
        producer.assert_running();
        let evidence = local_convergence
            .evidence_by_node
            .get(&node.id)
            .unwrap_or_else(|| panic!("final converged authority omitted node{}", node.id));
        let expected_authority = checkpoint_barrier_timing_authority(evidence, &final_fence)
            .unwrap_or_else(|error| panic!("final node{} timing authority: {error}", node.id));
        exact_timing_evidence
            .finalize_node(
                node,
                expected_authority,
                &mut latency_evidence,
                Instant::now() + Duration::from_secs(10),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "final node{} checkpoint timing evidence did not stabilize under active load: {error}",
                    node.id
                )
            });
    }
    producer.assert_running();
    exact_timing_evidence
        .validate_observed_cuts(&latency_evidence, &nodes)
        .unwrap_or_else(|error| {
            panic!("incomplete exact checkpoint observed-cut evidence: {error}")
        });
    exact_timing_evidence.report();
    latency_evidence.report(minimum_live_state_bytes, checkpoint_slo_mode);

    // Freeze the exact broker-acknowledged input offsets and require source commits to cover them.
    observe_live_join_state(
        &nodes,
        &mut live_state_high_water,
        &mut temporal_state_high_water,
    );
    // Make the temporal revival/probe rows durable while continuous inputs still hold the cluster
    // minimum frontier below every future-dated recovery canary. Publish the closing sentinels only
    // after that exact source cut: time-based idleness deliberately excludes a quiet partition from
    // the minimum and therefore cannot itself order a queued revival against another partition.
    let temporal_post = produce_temporal_post_fault_revivals(
        &brokers,
        &temporal_left_topic,
        &temporal_right_topic,
        kafka_partitions,
        &temporal_recovery_seed,
    );
    let mut temporal_input_boundary = temporal_recovery_seed.input_boundary.clone();
    merge_offset_boundary(
        &mut temporal_input_boundary,
        &temporal_post.revival_input_boundary,
    );
    latest_checkpoint = assert_final_input_cut(
        &mut nodes,
        &temporal_commit_oracle,
        &temporal_input_boundary,
        durable_cut_window,
        latest_checkpoint,
    );
    let temporal_closing_boundary = produce_temporal_post_fault_sentinels(
        &brokers,
        &temporal_left_topic,
        &temporal_right_topic,
        kafka_partitions,
        &temporal_recovery_seed,
    );
    merge_offset_boundary(&mut temporal_input_boundary, &temporal_closing_boundary);
    latest_checkpoint = assert_final_input_cut(
        &mut nodes,
        &temporal_commit_oracle,
        &temporal_input_boundary,
        durable_cut_window,
        latest_checkpoint,
    );
    let (produced_prefix, _bounded_frozen_input_at) = producer.stop();
    let temporal_frontier_released_at = Instant::now();
    assert_temporal_canaries(
        &mut nodes,
        &mut output_oracles,
        delivery,
        &temporal_post,
        temporal_frontier_released_at,
        visibility_slo_mode,
        durable_output_window,
        "three-node",
    );
    // Cluster operators use the minimum active-source frontier. The matrix canary is deliberately
    // future-dated so its pre-fault state survives long fault schedules; release the continuous
    // near-wall-clock sources before requiring its sentinel to close the windows. Matrix source
    // offsets were checkpointed above, so this changes only when output visibility is measured.
    let window_frontier_released_at = Instant::now();
    match delivery {
        JoinDelivery::AtLeastOnce => assert_kafka_core_window_outputs(
            &mut nodes,
            window_output_oracle
                .as_mut()
                .expect("cluster ALO Kafka CoreWindow oracle"),
            &expected_windows,
            window_frontier_released_at,
            recovery_ceiling,
            "three-node ALO CoreWindow canaries",
        ),
        JoinDelivery::ExactlyOnce => {
            #[cfg(feature = "delta-lake-s3")]
            assert_delta_core_window_outputs(
                &mut nodes,
                delta_window_output_oracles
                    .as_ref()
                    .expect("cluster EO Delta CoreWindow oracles"),
                &expected_windows,
                window_frontier_released_at,
                visibility_slo_mode,
                durable_output_window,
                "three-node EO CoreWindow canaries",
            );
            #[cfg(not(feature = "delta-lake-s3"))]
            unreachable!("EO runner is unavailable without delta-lake-s3");
        }
    }
    #[cfg(feature = "delta-lake-s3")]
    let delta_visibility_boundaries = (delivery == JoinDelivery::ExactlyOnce).then(|| {
        let outputs = delta_join_exact_outputs(
            &output_oracles,
            &produced_prefix,
            _bounded_frozen_input_at,
            "three-node EO bounded join",
            "three-node EO temporal ASOF load",
        );
        capture_delta_visibility_boundaries(&mut nodes, &outputs, visibility_slo_mode)
    });
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
        usize::try_from(kafka_partitions)
            .expect("Kafka partition count fits usize")
            .saturating_mul(2),
        "producer did not report every partition boundary on both join inputs"
    );
    assert!(
        produced_prefix.end_offsets.iter().all(|offset| *offset > 0),
        "producer did not write every input partition: {:?}",
        produced_prefix.end_offsets
    );
    latest_checkpoint = assert_final_input_cuts(
        &mut nodes,
        &[&commit_oracle, &temporal_load_commit_oracle],
        &produced_prefix.end_offsets,
        durable_cut_window,
        latest_checkpoint,
    );
    eprintln!(
        "soak: frozen bounded and temporal input prefix is durable through checkpoint {} epoch {}",
        latest_checkpoint.checkpoint_id, latest_checkpoint.epoch
    );
    if let Some(mut observer) = subscription_observer.take() {
        observer.wait_until_progress(latest_checkpoint.epoch, recovery_ceiling);
        let observation = observer.stop();
        assert_cluster_subscription_observation(
            &observation,
            &produced_prefix,
            join_keys,
            zipf_milli,
        );
        eprintln!(
            "soak: committed subscription replay verified through epoch {} across gateways {:?} and {} vnode partitions",
            observation.last_progress_epoch,
            observation.used_gateways,
            observation.observed_partitions.len(),
        );
    }
    #[cfg(feature = "delta-lake-s3")]
    let completed_delta_outputs = (delivery == JoinDelivery::ExactlyOnce).then(|| {
        let outputs = delta_join_exact_outputs(
            &output_oracles,
            &produced_prefix,
            _bounded_frozen_input_at,
            "three-node EO bounded join",
            "three-node EO temporal ASOF load",
        );
        wait_delta_exact_outputs(
            &mut nodes,
            &outputs,
            delta_visibility_boundaries
                .as_ref()
                .expect("cluster EO visibility boundaries captured before final cuts"),
            durable_output_window,
        )
    });
    match delivery {
        JoinDelivery::AtLeastOnce => {
            assert_frozen_kafka_outputs(
                &mut nodes,
                output_oracles
                    .kafka_bounded
                    .as_mut()
                    .expect("cluster ALO Kafka output oracle"),
                produced_count,
                &produced_prefix.expected_pairs,
                recovery_ceiling,
                "three-node ALO bounded join",
            );
            assert_frozen_kafka_outputs(
                &mut nodes,
                output_oracles
                    .kafka_temporal_load
                    .as_mut()
                    .expect("cluster ALO temporal-load Kafka output oracle"),
                produced_count,
                &produced_prefix.expected_temporal_pairs,
                recovery_ceiling,
                "three-node ALO temporal ASOF load",
            );
            assert_kafka_matrix_outputs(
                &mut nodes,
                matrix_output_oracle
                    .as_mut()
                    .expect("cluster ALO Kafka matrix oracle"),
                recovery_ceiling,
                "three-node ALO bounded-join matrix",
            );
        }
        JoinDelivery::ExactlyOnce => {
            #[cfg(feature = "delta-lake-s3")]
            {
                let exact_outputs = delta_join_exact_outputs(
                    &output_oracles,
                    &produced_prefix,
                    _bounded_frozen_input_at,
                    "three-node EO bounded join",
                    "three-node EO temporal ASOF load",
                );
                assert_delta_exact_outputs_stable(
                    &mut nodes,
                    &exact_outputs,
                    completed_delta_outputs
                        .as_deref()
                        .expect("cluster EO outputs completed before final cuts"),
                    recovery_ceiling,
                );
                assert_delta_matrix_outputs(
                    &mut nodes,
                    delta_matrix_output_oracles
                        .as_ref()
                        .expect("cluster EO Delta raw matrix oracles"),
                    recovery_ceiling,
                    "three-node EO bounded-join raw matrix",
                );
                assert_delta_matrix_aggregates(
                    &mut nodes,
                    delta_matrix_aggregate_oracles
                        .as_ref()
                        .expect("cluster EO Delta matrix aggregate oracles"),
                    recovery_ceiling,
                    "three-node EO bounded-join aggregate matrix",
                );
            }
            #[cfg(not(feature = "delta-lake-s3"))]
            unreachable!("EO runner is unavailable without delta-lake-s3");
        }
    }
    assert_hot_path_latency(
        &nodes,
        &format!("three-node {delivery_label} stateful workload"),
        hot_path_slo_mode,
    );
    assert_live_join_state_bytes(
        live_state_high_water,
        minimum_live_state_bytes,
        nodes.len(),
        &format!("three-node {delivery_label} bounded join"),
    );
    assert_live_join_state_bytes(
        temporal_state_high_water,
        minimum_live_state_bytes.max(1),
        nodes.len(),
        &format!("three-node {delivery_label} temporal ASOF load"),
    );
    assert_core_window_state_bounds(
        &window_state_high_water,
        nodes.len(),
        &format!("three-node {delivery_label}"),
    );
}

/// Create `topic` with `partitions` partitions (blocking; the admin API is async).
#[cfg(feature = "kafka")]
fn kafka_create_topic(brokers: &str, topic: &str, partitions: i32) {
    kafka_create_topic_with_config(brokers, topic, partitions, &[]);
}

/// Create a log-compacted topic for a keyed upsert sink while retaining every transition for the
/// duration of the finite soak oracle. The recovery horizon is capped below 45 minutes, so a
/// one-hour compaction lag keeps the transition history observable without violating the sink's
/// production compacted-topic contract.
#[cfg(feature = "kafka")]
fn kafka_create_compacted_topic(brokers: &str, topic: &str, partitions: i32) {
    kafka_create_topic_with_config(
        brokers,
        topic,
        partitions,
        &[
            ("cleanup.policy", "compact"),
            ("min.compaction.lag.ms", "3600000"),
        ],
    );
}

#[cfg(feature = "kafka")]
fn kafka_create_topic_with_config<'a>(
    brokers: &str,
    topic: &'a str,
    partitions: i32,
    config: &'a [(&'a str, &'a str)],
) {
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
        let mut new = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
        for &(key, value) in config {
            new = new.set(key, value);
        }
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

/// Publish one phase of the finite composite-key matrix and return the resulting topic boundaries.
#[cfg(feature = "kafka")]
fn produce_matrix_phase(
    brokers: &str,
    left_topic: &str,
    right_topic: &str,
    left: &[(i64, i64, i64, u64)],
    right: &[(i64, i64, i64, u64)],
) -> Vec<i64> {
    use rdkafka::producer::{FutureProducer, FutureRecord};

    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .set("enable.idempotence", "true")
        .create()
        .expect("matrix producer");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("matrix producer runtime");
    runtime.block_on(async {
        let boundary_len = usize::try_from(MATRIX_INPUT_PARTITIONS)
            .expect("matrix input partition count fits usize");
        let mut left_boundaries = vec![0; boundary_len];
        let mut right_boundaries = vec![0; boundary_len];
        for (topic, records, boundaries) in [
            (left_topic, left, &mut left_boundaries),
            (right_topic, right, &mut right_boundaries),
        ] {
            for (index, (id, join_key, join_key_2, event_time)) in records.iter().enumerate() {
                let payload = format!(
                    r#"{{"id":{id},"join_key":{join_key},"join_key_2":{join_key_2},"event_time":{event_time}}}"#
                );
                let key = format!("{join_key}:{join_key_2}");
                let partition = i32::try_from(index % boundary_len)
                    .expect("matrix probe partition fits i32");
                let delivery = producer
                    .send_result(
                        FutureRecord::to(topic)
                            .payload(&payload)
                            .key(&key)
                            .partition(partition),
                    )
                    .unwrap_or_else(|(error, _)| panic!("matrix Kafka enqueue failed: {error}"))
                    .await
                    .expect("matrix Kafka delivery future was cancelled")
                    .unwrap_or_else(|(error, _)| panic!("matrix Kafka delivery failed: {error}"));
                let delivered_partition = usize::try_from(delivery.partition)
                    .expect("Kafka returned a negative matrix partition");
                boundaries[delivered_partition] = boundaries[delivered_partition]
                    .max(delivery.offset.saturating_add(1));
            }
        }
        assert!(
            left_boundaries.iter().all(|offset| *offset > 0)
                && right_boundaries.iter().all(|offset| *offset > 0),
            "matrix producer did not cover every input partition"
        );
        left_boundaries.extend(right_boundaries);
        left_boundaries
    })
}

#[cfg(feature = "kafka")]
fn matrix_event_time_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is before the Unix epoch")
            .as_millis(),
    )
    .expect("wall-clock milliseconds fit u64")
}

#[cfg(feature = "kafka")]
fn recovery_canary_event_time_ms(now_ms: u64) -> u64 {
    // Recovery canaries intentionally complete or mutate retained state after hard restarts. Keep
    // their event time ahead of the closed cut while unrelated live inputs advance the global
    // watermark; every before/after or pre/post image retains this exact timestamp.
    now_ms.saturating_add(RECOVERY_CANARY_EVENT_LEAD_MS)
}

#[cfg(feature = "kafka")]
fn matrix_post_event_time_ms(now_ms: u64, pre_fault_event_time_ms: u64) -> u64 {
    now_ms.max(pre_fault_event_time_ms)
}

#[cfg(feature = "kafka")]
fn validate_mutable_interval_recovery_horizon(kills: u64, recovery_ceiling: Duration) {
    // Before the final revival, the scenario has eight recovery-bounded observation phases plus
    // two per hard restart. Each restart also has a 45s checkpoint gate and 10s kill/finalization
    // allowance. Keep one extra minute for topic/config/process startup outside those waits.
    let recovery_phases = 8_u64.saturating_add(kills.saturating_mul(2));
    let recovery_phases =
        u32::try_from(recovery_phases).expect("mutable interval recovery phase count fits u32");
    let required = recovery_ceiling
        .saturating_mul(recovery_phases)
        .saturating_add(Duration::from_secs(kills.saturating_mul(55)))
        .saturating_add(Duration::from_secs(60));
    let available = Duration::from_millis(RECOVERY_CANARY_EVENT_LEAD_MS);
    assert!(
        required <= available,
        "mutable interval recovery canary needs {required:?} before its final mutation, but its ordered event-time horizon is only {available:?}; reduce LAMINAR_SOAK_SINGLE_KILLS/LAMINAR_SOAK_MAX_RECOVERY_MS or extend the configured canary horizon"
    );
}

#[cfg(feature = "kafka")]
fn validate_matrix_recovery_horizon(
    kills: u64,
    recovery_ceiling: Duration,
    soak_secs: u64,
    join_interval_ms: u64,
) {
    // Matrix/CoreWindow state must remain open through startup, every fault/rejoin proof, the
    // mutable canary phases, and the requested steady interval. Cluster kill rounds can consume
    // three recovery-bounded waits each; the extra fixed allowance covers checkpoint gates and
    // process/config startup. Fail before intake rather than eventually submitting a late row.
    let recovery_phases = 8_u64.saturating_add(kills.saturating_mul(3));
    let recovery_phases =
        u32::try_from(recovery_phases).expect("matrix recovery phase count fits u32");
    let required = recovery_ceiling
        .saturating_mul(recovery_phases)
        .saturating_add(Duration::from_secs(kills.saturating_mul(55)))
        .saturating_add(Duration::from_secs(soak_secs))
        .saturating_add(Duration::from_secs(60));
    let available = Duration::from_millis(RECOVERY_CANARY_EVENT_LEAD_MS);
    assert!(
        required <= available,
        "matrix/CoreWindow recovery canary needs {required:?} before its post-fault completion, but its held-open event-time horizon is only {available:?}; reduce the configured kill count, recovery ceiling, or soak duration"
    );
    let closing_sentinel_lead = RECOVERY_CANARY_EVENT_LEAD_MS
        .checked_add(join_interval_ms)
        .and_then(|lead| lead.checked_add(3_001))
        .expect("matrix recovery sentinel lead overflow");
    assert!(
        closing_sentinel_lead <= SOAK_EVENT_MAX_FUTURE_SKEW_MS,
        "matrix closing sentinel needs {closing_sentinel_lead}ms of future skew, but the soak config admits only {SOAK_EVENT_MAX_FUTURE_SKEW_MS}ms; reduce LAMINAR_SOAK_JOIN_INTERVAL_MS or extend the configured future-skew guard"
    );
    let temporal_closing_sentinel_lead = RECOVERY_CANARY_EVENT_LEAD_MS
        .checked_add(5_000)
        .and_then(|lead| lead.checked_add(TEMPORAL_WATERMARK_ADVANCE_MS))
        .expect("temporal recovery sentinel lead overflow");
    assert!(
        temporal_closing_sentinel_lead <= SOAK_EVENT_MAX_FUTURE_SKEW_MS,
        "temporal closing sentinel needs {temporal_closing_sentinel_lead}ms of future skew, but the soak config admits only {SOAK_EVENT_MAX_FUTURE_SKEW_MS}ms"
    );
}

/// Seed one complete match plus directional match halves and unmatched rows that must survive every
/// fault. Without a later row neither source watermark can close the pending outer/anti results.
#[cfg(feature = "kafka")]
fn produce_matrix_pre_fault_inputs(
    brokers: &str,
    left_topic: &str,
    right_topic: &str,
) -> (Vec<i64>, u64) {
    let event_time = recovery_canary_event_time_ms(matrix_event_time_ms());
    let left = [
        (-101_i64, -11_i64, -21_i64, event_time),
        (-102, -12, -22, event_time),
        // The matching right row arrives only after recovery.
        (-104, -14, -25, event_time),
    ];
    let right = [
        (-201_i64, -21_i64, -11_i64, event_time),
        // Agrees with left -102 on the first ordered predicate only.
        (-202, -23, -12, event_time),
        // The matching left row arrives only after recovery.
        (-205, -26, -15, event_time),
    ];
    (
        produce_matrix_phase(brokers, left_topic, right_topic, &left, &right),
        event_time,
    )
}

/// After all fault rounds, complete both retained matches, add a fresh pair, then advance both
/// watermarks with a positive-key sentinel that every join projection removes.
#[cfg(feature = "kafka")]
fn produce_matrix_post_fault_inputs(
    brokers: &str,
    left_topic: &str,
    right_topic: &str,
    pre_fault_event_time: u64,
) -> (Vec<i64>, Instant) {
    let event_time = matrix_post_event_time_ms(matrix_event_time_ms(), pre_fault_event_time);
    let sentinel_time = event_time
        .max(pre_fault_event_time)
        .checked_add(join_interval_ms())
        .and_then(|time| time.checked_add(3_001))
        .expect("matrix sentinel timestamp overflow");
    let left = [
        (-105_i64, -15_i64, -26_i64, pre_fault_event_time),
        (-103_i64, -13_i64, -24_i64, event_time),
        (-901, 1, 2, sentinel_time),
    ];
    let right = [
        (-204_i64, -25_i64, -14_i64, pre_fault_event_time),
        (-203_i64, -24_i64, -13_i64, event_time),
        (-902, 2, 1, sentinel_time),
    ];
    let boundary = produce_matrix_phase(brokers, left_topic, right_topic, &left, &right);
    (boundary, Instant::now())
}

#[cfg(feature = "kafka")]
fn produce_temporal_payloads(
    brokers: &str,
    topic: &str,
    partitions: i32,
    records: &[(i32, String, String)],
) -> (Vec<i64>, Instant) {
    use rdkafka::producer::{FutureProducer, FutureRecord};

    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .set("enable.idempotence", "true")
        .create()
        .expect("temporal probe producer");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("temporal probe producer runtime");
    runtime.block_on(async {
        let mut boundaries =
            vec![0; usize::try_from(partitions).expect("Kafka partition count fits usize")];
        for (record_partition, key, payload) in records {
            let delivery = producer
                .send_result(
                    FutureRecord::to(topic)
                        .payload(payload)
                        .key(key)
                        .partition(*record_partition),
                )
                .unwrap_or_else(|(error, _)| panic!("temporal Kafka enqueue failed: {error}"))
                .await
                .expect("temporal Kafka delivery future was cancelled")
                .unwrap_or_else(|(error, _)| panic!("temporal Kafka delivery failed: {error}"));
            let partition = usize::try_from(delivery.partition)
                .expect("Kafka returned a negative temporal partition");
            let boundary = boundaries
                .get_mut(partition)
                .expect("Kafka returned an out-of-range temporal partition");
            *boundary = (*boundary).max(delivery.offset.saturating_add(1));
        }
        (boundaries, Instant::now())
    })
}

#[cfg(feature = "kafka")]
fn temporal_row_value(record: TemporalSoakRecord) -> serde_json::Value {
    serde_json::json!({
        "id": record.id,
        "join_key": record.key,
        "temporal_time": record.temporal_time_ms,
    })
}

#[cfg(feature = "kafka")]
fn produce_temporal_left_records(
    brokers: &str,
    topic: &str,
    partitions: i32,
    records: &[TemporalSoakRecord],
) -> (Vec<i64>, Instant) {
    let payloads = records
        .iter()
        .map(|record| {
            (
                record.partition,
                record.key.to_string(),
                temporal_row_value(*record).to_string(),
            )
        })
        .collect::<Vec<_>>();
    produce_temporal_payloads(brokers, topic, partitions, &payloads)
}

#[cfg(feature = "kafka")]
fn produce_temporal_right_mutations(
    brokers: &str,
    topic: &str,
    partitions: i32,
    mutations: &[TemporalDebeziumMutation],
) -> (Vec<i64>, Instant) {
    let payloads = mutations
        .iter()
        .map(|mutation| {
            let record = mutation
                .after
                .or(mutation.before)
                .expect("Debezium mutation must contain a row");
            let payload = serde_json::json!({
                "before": mutation.before.map(temporal_row_value),
                "after": mutation.after.map(temporal_row_value),
                "op": mutation.operation,
                "ts_ms": record.temporal_time_ms,
            });
            (
                record.partition,
                record.key.to_string(),
                payload.to_string(),
            )
        })
        .collect::<Vec<_>>();
    produce_temporal_payloads(brokers, topic, partitions, &payloads)
}

#[cfg(feature = "kafka")]
fn mutable_interval_right_value(record: MutableIntervalRightRow) -> serde_json::Value {
    serde_json::json!({
        "id": record.id,
        "join_key": record.join_key,
        "event_time": record.event_time_ms,
        "right_value": record.value,
    })
}

#[cfg(feature = "kafka")]
fn produce_mutable_interval_right_mutations(
    brokers: &str,
    topic: &str,
    mutations: &[MutableIntervalMutation],
) -> (Vec<i64>, Instant) {
    let payloads = mutations
        .iter()
        .map(|mutation| {
            let record = mutation
                .after
                .or(mutation.before)
                .expect("mutable interval mutation must contain a row");
            let payload = serde_json::json!({
                "before": mutation.before.map(mutable_interval_right_value),
                "after": mutation.after.map(mutable_interval_right_value),
                "op": mutation.operation,
                "ts_ms": record.event_time_ms,
            });
            (
                0,
                format!("{}:{}:{}", record.id, record.join_key, record.event_time_ms),
                payload.to_string(),
            )
        })
        .collect::<Vec<_>>();
    produce_temporal_payloads(brokers, topic, MUTABLE_INTERVAL_INPUT_PARTITIONS, &payloads)
}

#[cfg(feature = "kafka")]
fn produce_mutable_interval_pre_fault_input(
    brokers: &str,
    left_topic: &str,
    right_topic: &str,
) -> (MutableIntervalRecoverySeed, Instant) {
    let event_time_ms = recovery_canary_event_time_ms(matrix_event_time_ms());
    let left_payload = serde_json::json!({
        "id": MUTABLE_INTERVAL_LEFT_ID,
        "join_key": MUTABLE_INTERVAL_JOIN_KEY,
        "event_time": event_time_ms,
    });
    let (left_boundary, _) = produce_temporal_payloads(
        brokers,
        left_topic,
        MUTABLE_INTERVAL_INPUT_PARTITIONS,
        &[(
            0,
            MUTABLE_INTERVAL_LEFT_ID.to_string(),
            left_payload.to_string(),
        )],
    );
    let right = MutableIntervalRightRow {
        id: MUTABLE_INTERVAL_RIGHT_ID,
        join_key: MUTABLE_INTERVAL_JOIN_KEY,
        event_time_ms,
        value: MUTABLE_INTERVAL_INITIAL_VALUE,
    };
    let (right_boundary, frozen_input_at) = produce_mutable_interval_right_mutations(
        brokers,
        right_topic,
        &[MutableIntervalMutation {
            before: None,
            after: Some(right),
            operation: "c",
        }],
    );
    let mut input_boundary = left_boundary;
    input_boundary.extend(right_boundary);
    assert_eq!(
        input_boundary.len(),
        usize::try_from(MUTABLE_INTERVAL_INPUT_PARTITIONS)
            .expect("mutable interval partition count fits usize")
            .saturating_mul(2),
        "mutable interval pre-fault boundary omitted an input side"
    );
    assert!(
        input_boundary.iter().all(|offset| *offset > 0),
        "mutable interval pre-fault input did not cover both topics"
    );
    (
        MutableIntervalRecoverySeed {
            right,
            input_boundary,
        },
        frozen_input_at,
    )
}

#[cfg(feature = "kafka")]
fn merge_mutable_interval_right_boundary(input_boundary: &mut [i64], right_boundary: &[i64]) {
    let partitions = usize::try_from(MUTABLE_INTERVAL_INPUT_PARTITIONS)
        .expect("mutable interval partition count fits usize");
    assert_eq!(
        input_boundary.len(),
        partitions.saturating_mul(2),
        "mutable interval combined boundary changed cardinality"
    );
    merge_offset_boundary(&mut input_boundary[partitions..], right_boundary);
}

#[cfg(feature = "kafka")]
fn produce_temporal_pre_fault_input(
    brokers: &str,
    left_topic: &str,
    right_topic: &str,
    partitions: i32,
    join_keys: u64,
) -> TemporalRecoverySeed {
    let key = i64::try_from(join_keys).expect("LAMINAR_SOAK_JOIN_KEYS must fit BIGINT");
    // This history is mutated and probed only after the fault rounds and steady-load latency
    // certification. Keep it ahead of the global admission watermark while unrelated live sources
    // advance, but do not floor those live sources to this timestamp: their lower frontier keeps
    // every recovery canary open until the producer is stopped.
    let base_time_ms = recovery_canary_event_time_ms(matrix_event_time_ms());
    let at = |offset| {
        base_time_ms
            .checked_add(offset)
            .expect("temporal certification timestamp overflow")
    };
    let created = TemporalSoakRecord {
        partition: 0,
        id: TEMPORAL_CREATE_RIGHT_ID,
        key,
        temporal_time_ms: at(2_000),
    };
    let updated = TemporalSoakRecord {
        id: TEMPORAL_UPDATE_RIGHT_ID,
        temporal_time_ms: at(3_000),
        ..created
    };
    let tombstoned = TemporalSoakRecord {
        temporal_time_ms: at(4_000),
        ..updated
    };
    let left_fillers = (0..partitions)
        .map(|partition| TemporalSoakRecord {
            partition,
            id: TEMPORAL_SENTINEL_ID_BASE
                .checked_add(u64::try_from(partition).expect("non-negative Kafka partition"))
                .expect("temporal left filler id overflow"),
            key: -i64::from(partition) - 1,
            temporal_time_ms: at(4_000),
        })
        .collect::<Vec<_>>();
    let mut right_mutations = vec![
        TemporalDebeziumMutation {
            before: None,
            after: Some(created),
            operation: "c",
        },
        TemporalDebeziumMutation {
            before: Some(created),
            after: Some(updated),
            operation: "u",
        },
        TemporalDebeziumMutation {
            before: Some(tombstoned),
            after: None,
            operation: "d",
        },
    ];
    right_mutations.extend((0..partitions).map(|partition| {
        TemporalDebeziumMutation {
            before: None,
            after: Some(TemporalSoakRecord {
                partition,
                id: TEMPORAL_SENTINEL_ID_BASE
                    .checked_add(1_000)
                    .and_then(|id| {
                        id.checked_add(
                            u64::try_from(partition).expect("non-negative Kafka partition"),
                        )
                    })
                    .expect("temporal right filler id overflow"),
                key: -i64::from(partition) - 1,
                temporal_time_ms: at(4_000),
            }),
            operation: "c",
        }
    }));
    let (left_boundary, _) =
        produce_temporal_left_records(brokers, left_topic, partitions, &left_fillers);
    let (right_boundary, _) =
        produce_temporal_right_mutations(brokers, right_topic, partitions, &right_mutations);
    assert!(
        left_boundary.iter().all(|offset| *offset > 0),
        "temporal pre-fault fillers did not cover every left partition"
    );
    assert!(
        right_boundary.iter().all(|offset| *offset > 0),
        "temporal pre-fault seed did not cover every right partition"
    );
    let mut input_boundary = left_boundary;
    input_boundary.extend(right_boundary);
    TemporalRecoverySeed {
        key,
        base_time_ms,
        input_boundary,
    }
}

#[cfg(feature = "kafka")]
fn produce_temporal_post_fault_revivals(
    brokers: &str,
    left_topic: &str,
    right_topic: &str,
    partitions: i32,
    seed: &TemporalRecoverySeed,
) -> TemporalPostFaultInput {
    let probe_time_ms = seed
        .base_time_ms
        .checked_add(5_000)
        .expect("temporal recovery probe time overflow");
    let revival_time_ms = seed
        .base_time_ms
        .checked_add(6_000)
        .expect("temporal revival time overflow");
    let post_revival_time_ms = seed
        .base_time_ms
        .checked_add(7_000)
        .expect("post-revival ASOF time overflow");
    let revival = TemporalSoakRecord {
        partition: 0,
        id: TEMPORAL_REVIVAL_RIGHT_ID,
        key: seed.key,
        temporal_time_ms: revival_time_ms,
    };
    let (right_revival_boundary, _) = produce_temporal_right_mutations(
        brokers,
        right_topic,
        partitions,
        &[TemporalDebeziumMutation {
            before: None,
            after: Some(revival),
            operation: "c",
        }],
    );
    let left = [
        TemporalSoakRecord {
            partition: 0,
            id: TEMPORAL_RECOVERY_LEFT_ID,
            key: seed.key,
            temporal_time_ms: probe_time_ms,
        },
        TemporalSoakRecord {
            partition: 0,
            id: TEMPORAL_REVIVAL_LEFT_ID,
            key: seed.key,
            temporal_time_ms: post_revival_time_ms,
        },
    ];
    let (left_boundary, _) = produce_temporal_left_records(brokers, left_topic, partitions, &left);
    let mut revival_input_boundary = left_boundary;
    revival_input_boundary.extend(right_revival_boundary);

    let probe_time_ms = i64::try_from(probe_time_ms).expect("probe timestamp fits i64");
    let expected_asof = BTreeSet::from([
        TemporalOutput::LeftAsof {
            left_id: TEMPORAL_RECOVERY_LEFT_ID,
            right_id: None,
        },
        TemporalOutput::LeftAsof {
            left_id: TEMPORAL_REVIVAL_LEFT_ID,
            right_id: Some(TEMPORAL_REVIVAL_RIGHT_ID),
        },
    ]);
    let expected_probe = BTreeSet::from([
        TemporalOutput::InnerProbe {
            left_id: TEMPORAL_RECOVERY_LEFT_ID,
            right_id: TEMPORAL_CREATE_RIGHT_ID,
            offset_ms: TEMPORAL_PROBE_OFFSETS_MS[1],
            probe_time_ms: probe_time_ms + TEMPORAL_PROBE_OFFSETS_MS[1],
        },
        TemporalOutput::InnerProbe {
            left_id: TEMPORAL_RECOVERY_LEFT_ID,
            right_id: TEMPORAL_UPDATE_RIGHT_ID,
            offset_ms: TEMPORAL_PROBE_OFFSETS_MS[2],
            probe_time_ms: probe_time_ms + TEMPORAL_PROBE_OFFSETS_MS[2],
        },
        TemporalOutput::InnerProbe {
            left_id: TEMPORAL_RECOVERY_LEFT_ID,
            right_id: TEMPORAL_REVIVAL_RIGHT_ID,
            offset_ms: TEMPORAL_PROBE_OFFSETS_MS[4],
            probe_time_ms: probe_time_ms + TEMPORAL_PROBE_OFFSETS_MS[4],
        },
    ]);
    TemporalPostFaultInput {
        revival_input_boundary,
        expected_asof,
        expected_probe,
    }
}

#[cfg(feature = "kafka")]
fn produce_temporal_post_fault_sentinels(
    brokers: &str,
    left_topic: &str,
    right_topic: &str,
    partitions: i32,
    seed: &TemporalRecoverySeed,
) -> Vec<i64> {
    let frontier_time_ms = seed
        .base_time_ms
        .checked_add(5_000)
        .and_then(|probe| probe.checked_add(TEMPORAL_WATERMARK_ADVANCE_MS))
        .expect("temporal watermark sentinel time overflow");
    let left = (0..partitions)
        .map(|partition| TemporalSoakRecord {
            partition,
            id: TEMPORAL_SENTINEL_ID_BASE
                .checked_add(2_000)
                .and_then(|id| {
                    id.checked_add(u64::try_from(partition).expect("non-negative Kafka partition"))
                })
                .expect("temporal left sentinel id overflow"),
            key: -i64::from(partitions) - i64::from(partition) - 1,
            temporal_time_ms: frontier_time_ms,
        })
        .collect::<Vec<_>>();
    let right = (0..partitions)
        .map(|partition| TemporalDebeziumMutation {
            before: None,
            after: Some(TemporalSoakRecord {
                partition,
                id: TEMPORAL_SENTINEL_ID_BASE
                    .checked_add(3_000)
                    .and_then(|id| {
                        id.checked_add(
                            u64::try_from(partition).expect("non-negative Kafka partition"),
                        )
                    })
                    .expect("temporal right sentinel id overflow"),
                key: -i64::from(partitions) - i64::from(partition) - 1,
                temporal_time_ms: frontier_time_ms,
            }),
            operation: "c",
        })
        .collect::<Vec<_>>();
    let (left_boundary, _) = produce_temporal_left_records(brokers, left_topic, partitions, &left);
    let (right_boundary, _) =
        produce_temporal_right_mutations(brokers, right_topic, partitions, &right);
    let mut input_boundary = left_boundary;
    input_boundary.extend(right_boundary);
    input_boundary
}

#[cfg(feature = "kafka")]
fn merge_offset_boundary(boundary: &mut [i64], update: &[i64]) {
    assert_eq!(
        boundary.len(),
        update.len(),
        "Kafka input boundary partition count changed"
    );
    for (boundary, update) in boundary.iter_mut().zip(update) {
        *boundary = (*boundary).max(*update);
    }
}

/// Produce one deterministically identified row on each join side, paced near `rps`, until stopped.
/// Explicit round-robin Kafka partitions cover every external partition; LaminarDB independently
/// shuffles the Zipf-distributed logical join key to its canonical vnode.
#[cfg(feature = "kafka")]
fn produce_join_inputs(
    brokers: &str,
    left_topic: &str,
    right_topic: &str,
    partitions: i32,
    rps: u64,
    key_count: u64,
    zipf_milli: u64,
    timestamp_floor_ms: u64,
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
        let event_time_base = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock is before the Unix epoch")
                .as_millis(),
        )
        .expect("wall-clock milliseconds fit u64")
        .max(timestamp_floor_ms);
        let sampler = ZipfSampler::new(key_count, zipf_milli);
        let mut n = 0u64;
        let partition_count = u64::try_from(partitions).expect("positive partition count");
        assert!(partition_count > 0, "partition count must be positive");
        let mut left_end_offsets =
            vec![0; usize::try_from(partition_count).expect("partition count fits usize")];
        let mut right_end_offsets = left_end_offsets.clone();
        let mut left_deliveries = FuturesUnordered::new();
        let mut right_deliveries = FuturesUnordered::new();
        let mut left_acknowledged = 0u64;
        let mut right_acknowledged = 0u64;
        let mut inputs = Vec::new();

        while !stop.load(Ordering::Acquire) {
            if left_deliveries.len() + right_deliveries.len() >= SOAK_PRODUCER_MAX_IN_FLIGHT {
                tokio::select! {
                    delivery = left_deliveries.next(), if !left_deliveries.is_empty() => {
                        record_delivery(
                            delivery.expect("left producer has an in-flight delivery"),
                            &mut left_end_offsets,
                            &mut left_acknowledged,
                        );
                    }
                    delivery = right_deliveries.next(), if !right_deliveries.is_empty() => {
                        record_delivery(
                            delivery.expect("right producer has an in-flight delivery"),
                            &mut right_end_offsets,
                            &mut right_acknowledged,
                        );
                    }
                }
                continue;
            }

            let target = start + Duration::from_secs_f64(n as f64 / rps as f64);
            while target > tokio::time::Instant::now() {
                tokio::select! {
                    delivery = left_deliveries.next(), if !left_deliveries.is_empty() => {
                        record_delivery(
                            delivery.expect("left producer has an in-flight delivery"),
                            &mut left_end_offsets,
                            &mut left_acknowledged,
                        );
                    }
                    delivery = right_deliveries.next(), if !right_deliveries.is_empty() => {
                        record_delivery(
                            delivery.expect("right producer has an in-flight delivery"),
                            &mut right_end_offsets,
                            &mut right_acknowledged,
                        );
                    }
                    () = tokio::time::sleep_until(target) => {}
                }
            }
            if stop.load(Ordering::Acquire) {
                break;
            }

            let join_key = sampler.sample(n);
            let load_offset_ms = temporal_load_offset_ms(n, rps);
            let event_time_ms = event_time_base
                .checked_add(load_offset_ms)
                .expect("event-time timestamp overflow");
            let temporal_time_ms = event_time_base
                .checked_add(load_offset_ms)
                .expect("temporal timestamp overflow");
            let partition =
                i32::try_from(n % partition_count).expect("round-robin partition fits i32");
            let payload = format!(
                r#"{{"id":{n},"join_key":{join_key},"source_partition":{partition},"event_time":{event_time_ms},"temporal_time":{temporal_time_ms}}}"#
            );
            let key = join_key.to_string();
            let left_delivery = producer
                .send_result(
                    FutureRecord::to(left_topic)
                        .payload(&payload)
                        .key(&key)
                        .partition(partition),
                )
                .unwrap_or_else(|(error, _)| panic!("left Kafka enqueue failed: {error}"));
            let right_delivery = producer
                .send_result(
                    FutureRecord::to(right_topic)
                        .payload(&payload)
                        .key(&key)
                        .partition(partition),
                )
                .unwrap_or_else(|(error, _)| panic!("right Kafka enqueue failed: {error}"));
            left_deliveries.push(left_delivery);
            right_deliveries.push(right_delivery);
            inputs.push(JoinInput {
                id: n,
                key: join_key,
                event_time_ms,
            });
            n = n.checked_add(1).expect("soak sequence overflow");
            enqueued_count.store(n, Ordering::Release);
        }
        while let Some(delivery) = left_deliveries.next().await {
            record_delivery(delivery, &mut left_end_offsets, &mut left_acknowledged);
        }
        while let Some(delivery) = right_deliveries.next().await {
            record_delivery(delivery, &mut right_end_offsets, &mut right_acknowledged);
        }
        assert_eq!(
            left_acknowledged, n,
            "left producer stopped before every enqueued record was acknowledged"
        );
        assert_eq!(
            right_acknowledged, n,
            "right producer stopped before every enqueued record was acknowledged"
        );
        assert!(n > 0, "soak producer emitted no input records");
        let elapsed = start.elapsed();
        let last_load_offset_ms = temporal_load_offset_ms(n - 1, rps);
        let event_time_ms = event_time_base
            .checked_add(last_load_offset_ms)
            .and_then(|time| time.checked_add(join_interval_ms()))
            .and_then(|time| time.checked_add(3_001))
            .expect("bounded-load watermark sentinel time overflow");
        let temporal_time_ms = event_time_base
            .checked_add(last_load_offset_ms)
            .and_then(|time| time.checked_add(TEMPORAL_WATERMARK_ADVANCE_MS))
            .expect("temporal-load watermark sentinel time overflow");
        let mut sentinel_acknowledged = 0;
        for (topic, end_offsets, id_offset) in [
            (left_topic, &mut left_end_offsets, 4_000),
            (right_topic, &mut right_end_offsets, 5_000),
        ] {
            for partition in 0..partitions {
                let id = TEMPORAL_SENTINEL_ID_BASE
                    .checked_add(id_offset)
                    .and_then(|id| {
                        id.checked_add(
                            u64::try_from(partition).expect("non-negative Kafka partition"),
                        )
                    })
                    .expect("temporal-load sentinel id overflow");
                let join_key = -i64::from(partitions) - i64::from(partition) - 1;
                let key = join_key.to_string();
                let payload = format!(
                    r#"{{"id":{id},"join_key":{join_key},"source_partition":{partition},"event_time":{event_time_ms},"temporal_time":{temporal_time_ms}}}"#
                );
                let delivery = producer
                    .send_result(
                        FutureRecord::to(topic)
                            .payload(&payload)
                            .key(&key)
                            .partition(partition),
                    )
                    .unwrap_or_else(|(error, _)| {
                        panic!("temporal-load sentinel enqueue failed: {error}")
                    })
                    .await;
                record_delivery(delivery, end_offsets, &mut sentinel_acknowledged);
            }
        }
        assert_eq!(sentinel_acknowledged, partition_count.saturating_mul(2));
        let broker_acked_at = Instant::now();
        left_end_offsets.extend(right_end_offsets);
        ProducedPrefix {
            count: n,
            end_offsets: left_end_offsets,
            expected_pairs: expected_join_pairs(&inputs),
            expected_temporal_pairs: expected_temporal_pairs(n),
            elapsed,
            broker_acked_at,
        }
    })
}

#[cfg(feature = "kafka")]
#[test]
fn bounded_diagnostic_statuses_retry_only_transient_responses() {
    for status in [429, 503, 504] {
        assert!(
            is_retryable_diagnostic_status(status),
            "HTTP {status} must remain pending until the outer evidence deadline"
        );
    }
    for status in [200, 400, 401, 403, 404, 500] {
        assert!(
            !is_retryable_diagnostic_status(status),
            "HTTP {status} must not hide invalid or contradictory evidence"
        );
    }
}

#[cfg(feature = "kafka")]
#[test]
fn readiness_diagnostics_expose_only_fixed_state_categories() {
    for (status, body, expected) in [
        (200, "ready", ReadinessDiagnostic::Ready),
        (
            503,
            "server startup is not complete",
            ReadinessDiagnostic::Starting,
        ),
        (
            503,
            "server serving authority is fenced",
            ReadinessDiagnostic::ServingFenced,
        ),
        (
            503,
            "server process lease is no longer live",
            ReadinessDiagnostic::ProcessLeaseExpired,
        ),
        (
            503,
            "server is completing coordinated recovery",
            ReadinessDiagnostic::Recovering,
        ),
        (
            503,
            "pipeline is Faulted, not Running",
            ReadinessDiagnostic::PipelineNotRunning,
        ),
        (
            503,
            "unrecognized response containing sig=secret",
            ReadinessDiagnostic::TemporarilyUnavailable,
        ),
        (
            401,
            "sig=secret",
            ReadinessDiagnostic::UnexpectedStatus(401),
        ),
    ] {
        assert_eq!(
            classify_readiness_response(status, body.as_bytes()),
            expected
        );
    }
}

#[cfg(feature = "kafka")]
#[test]
fn recovery_log_diagnostics_count_markers_without_copying_log_values() {
    let log = "leader lease operation failed: signed_url=?sig=secret\n\
               leader lease operation failed: account=secret\n\
               leader announced recovery prepare\n\
               coordinated recovery has no live durable leader proof\n";
    let counts = count_recovery_diagnostic_markers(log);
    assert_eq!(counts.get("leader_operation_failed"), Some(&2));
    assert_eq!(counts.get("recovery_prepare"), Some(&1));
    assert_eq!(counts.get("missing_live_leader_proof"), Some(&1));
    let diagnostic = format!("{counts:?}");
    assert!(!diagnostic.contains("secret"));
    assert!(!diagnostic.contains("sig="));
    assert!(!diagnostic.contains("account="));
}

#[cfg(feature = "kafka")]
#[test]
fn temporal_load_clock_tracks_pacing_without_losing_strict_order() {
    // At the canonical 400 rows/s, 120,000 rows are five minutes of wall-clock input. The old
    // sequence-only clock represented those rows as only two minutes of temporal history.
    assert_eq!(temporal_load_offset_ms(0, 400), 0);
    assert_eq!(temporal_load_offset_ms(1, 400), 2);
    assert_eq!(temporal_load_offset_ms(2, 400), 5);
    assert_eq!(temporal_load_offset_ms(400, 400), 1_000);
    assert_eq!(temporal_load_offset_ms(120_000, 400), 300_000);
    for rps in 1..=MAX_TEMPORAL_LOAD_RPS {
        let timestamps = (0..=2_000)
            .map(|sequence| temporal_load_offset_ms(sequence, rps))
            .collect::<Vec<_>>();
        assert!(timestamps.windows(2).all(|pair| pair[0] < pair[1]));
    }

    let unsupported = std::panic::catch_unwind(|| temporal_load_offset_ms(1, 1_001));
    assert!(unsupported.is_err());
}

#[cfg(feature = "kafka")]
#[test]
fn temporal_load_primary_key_is_stable_within_each_kafka_partition() {
    let sources = temporal_join_source_config(
        "broker",
        "canary-left",
        "canary-right",
        "load-left",
        "load-right",
        "group",
    );
    assert!(sources.contains(
        "name = \"temporal_load_right\"\nconnector = \"kafka\"\nformat = \"json\"\nprimary_key = [\"join_key\", \"source_partition\"]"
    ));
    assert_eq!(sources.matches("name = \"source_partition\"").count(), 2);

    let workload = kafka_join_workload_config(
        "broker",
        "left",
        "right",
        "temporal-left",
        "temporal-right",
        "group",
        "matrix-left",
        "matrix-right",
        "matrix-group",
        "",
        "",
        "",
    );
    assert!(workload
        .contains("ON l.join_key = r.join_key\n AND l.source_partition = r.source_partition"));
}

#[test]
fn executable_resolution_binds_and_revalidates_the_cargo_fallback() {
    let directory = tempfile::tempdir().unwrap();
    let fallback = directory.path().join("cargo laminardb fixture");
    std::fs::write(&fallback, b"cargo-built-server").unwrap();

    let resolved = resolve_laminardb_executable(None, None, &fallback).unwrap();
    assert_eq!(resolved.path, fallback.canonicalize().unwrap());
    assert_eq!(resolved.origin, ExecutableOrigin::CargoBuilt);
    assert_eq!(resolved.byte_len, 18);
    assert_eq!(
        encode_sha256(resolved.sha256),
        "d5f410d6aca92063263450e616c8afc1ceb27f116414112a5bd5666e93bbc22a"
    );

    std::fs::write(&fallback, b"substituted-server").unwrap();
    let error = resolved.verify_unchanged().unwrap_err();
    assert!(error.contains("SHA-256 mismatch"), "{error}");
}

#[test]
fn executable_resolution_accepts_an_exact_absolute_override() {
    let directory = tempfile::tempdir().unwrap();
    let executable = directory.path().join("prebuilt laminardb with spaces");
    std::fs::write(&executable, b"prebuilt-server").unwrap();
    let expected = "f400c41753421ae14bceac6e85510f006306066e3d09fd75a7c144daa58ef321";

    let resolved = resolve_laminardb_executable(
        Some(executable.clone().into_os_string()),
        Some(OsString::from(expected)),
        Path::new("unused-fallback"),
    )
    .unwrap();
    assert_eq!(resolved.path, executable.canonicalize().unwrap());
    assert_eq!(resolved.origin, ExecutableOrigin::Override);
    assert_eq!(encode_sha256(resolved.sha256), expected);
}

#[test]
fn executable_resolution_rejects_partial_or_noncanonical_digest_configuration() {
    let directory = tempfile::tempdir().unwrap();
    let executable = directory.path().join("laminardb");
    std::fs::write(&executable, b"prebuilt-server").unwrap();
    let path = executable.into_os_string();
    let valid = OsString::from("f400c41753421ae14bceac6e85510f006306066e3d09fd75a7c144daa58ef321");

    assert!(resolve_laminardb_executable(Some(path.clone()), None, Path::new("unused")).is_err());
    assert!(resolve_laminardb_executable(None, Some(valid.clone()), Path::new("unused")).is_err());
    assert!(resolve_laminardb_executable(
        Some(OsString::new()),
        Some(valid.clone()),
        Path::new("unused")
    )
    .is_err());
    for malformed in [
        "",
        "1ef4",
        "F400C41753421AE14BCEAC6E85510F006306066E3D09FD75A7C144DAA58EF321",
        "f400c41753421ae14bceac6e85510f006306066e3d09fd75a7c144daa58ef321 ",
        "z400c41753421ae14bceac6e85510f006306066e3d09fd75a7c144daa58ef321",
    ] {
        assert!(resolve_laminardb_executable(
            Some(path.clone()),
            Some(OsString::from(malformed)),
            Path::new("unused")
        )
        .is_err());
    }
}

#[test]
fn executable_resolution_rejects_unusable_paths_and_digest_mismatch() {
    let directory = tempfile::tempdir().unwrap();
    let executable = directory.path().join("laminardb");
    std::fs::write(&executable, b"prebuilt-server").unwrap();
    let expected =
        OsString::from("f400c41753421ae14bceac6e85510f006306066e3d09fd75a7c144daa58ef321");

    assert!(resolve_laminardb_executable(
        Some(OsString::from("relative-laminardb")),
        Some(expected.clone()),
        Path::new("unused")
    )
    .is_err());
    assert!(resolve_laminardb_executable(
        Some(directory.path().join("missing").into_os_string()),
        Some(expected.clone()),
        Path::new("unused")
    )
    .is_err());
    assert!(resolve_laminardb_executable(
        Some(directory.path().as_os_str().to_os_string()),
        Some(expected.clone()),
        Path::new("unused")
    )
    .is_err());

    std::fs::write(&executable, b"substituted-server").unwrap();
    let error = resolve_laminardb_executable(
        Some(executable.into_os_string()),
        Some(expected),
        Path::new("unused"),
    )
    .unwrap_err();
    assert!(error.contains("SHA-256 mismatch"), "{error}");
}

#[test]
fn checkpoint_epoch_progress_requires_strict_advance() {
    assert!(checkpoint_epoch_advanced(7, 8));
    assert!(!checkpoint_epoch_advanced(7, 7));
    assert!(!checkpoint_epoch_advanced(7, 6));
}

#[cfg(feature = "kafka")]
#[test]
fn explicit_fault_oracle_bounds_cascade_participation_to_one() {
    let baselines = [3.0, 7.0, 2.0];
    assert!(validate_explicit_pipeline_fault_totals(&baselines, &[3.0, 8.0, 2.0], 1).is_ok());
    assert!(validate_explicit_pipeline_fault_totals(&baselines, &[4.0, 8.0, 2.0], 1).is_ok());
    assert!(
        validate_explicit_pipeline_fault_totals(&baselines, &[5.0, 8.0, 2.0], 1)
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
fn recovery_prepare_oracle_allows_only_fenced_direct_replacement() {
    let one = vec![RECOVERY_PREPARE_LOG.to_string(), String::new()];
    assert_eq!(
        validate_recovery_prepare_sequence(&one, 0).unwrap(),
        RecoveryPrepareSequence {
            final_prepare_line: 0,
            abandoned_rounds: 0,
        }
    );

    let replacement = vec![
        format!(
            "{RECOVERY_PREPARE_LOG}\n{RECOVERY_SUPERSEDED_LOG} fault set changed\n\
             {RECOVERY_RETRY_HOLD_LOG}\n{RECOVERY_PREPARE_HANDOFF_LOG}\n{RECOVERY_PREPARE_LOG}"
        ),
        String::new(),
    ];
    assert_eq!(
        validate_recovery_prepare_sequence(&replacement, 0).unwrap(),
        RecoveryPrepareSequence {
            final_prepare_line: 4,
            abandoned_rounds: 1,
        }
    );

    let unfenced = vec![
        format!("{RECOVERY_PREPARE_LOG}\n{RECOVERY_PREPARE_LOG}"),
        String::new(),
    ];
    assert!(validate_recovery_prepare_sequence(&unfenced, 0)
        .unwrap_err()
        .contains("superseded-round fence"));

    let missing_retry_hold = vec![
        format!(
            "{RECOVERY_PREPARE_LOG}\n{RECOVERY_SUPERSEDED_LOG} fault set changed\n\
             {RECOVERY_PREPARE_HANDOFF_LOG}\n{RECOVERY_PREPARE_LOG}"
        ),
        String::new(),
    ];
    assert!(validate_recovery_prepare_sequence(&missing_retry_hold, 0)
        .unwrap_err()
        .contains("retry holds"));

    let wrong_process = vec![
        RECOVERY_PREPARE_LOG.to_string(),
        RECOVERY_PREPARE_LOG.to_string(),
    ];
    assert!(validate_recovery_prepare_sequence(&wrong_process, 0)
        .unwrap_err()
        .contains("non-leader"));
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

    let pre_prepare_leader_log = format!(
        "checkpoint_id=4 epoch=4 {CHECKPOINT_ATTEMPT_RESERVED_LOG}\n{metric}\n{RECOVERY_PREPARE_LOG}\n{RECOVERY_RELEASE_LOG}\ncheckpoint_id=5 epoch=5 checkpoint completed"
    );
    let pre_prepare_failure_logs = vec![
        format!("{metric}\n{RECOVERY_PREPARE_LOG}\n{RECOVERY_RELEASE_LOG}"),
        RECOVERY_RELEASE_LOG.into(),
        RECOVERY_RELEASE_LOG.into(),
    ];
    assert_eq!(
        validate_recovery_checkpoint_failure_evidence(
            &baselines,
            &totals,
            0,
            &pre_prepare_failure_logs,
            &pre_prepare_leader_log,
            resumed,
        )
        .unwrap(),
        Some(failed)
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
                        vnode_state_ready: true,
                    },
                },
            )
        })
        .collect();
    (snapshot, evidence)
}

#[cfg(feature = "kafka")]
#[test]
fn checkpoint_timing_authority_uses_certificate_digest_domain() {
    let (snapshot, evidence) = local_assignment_cut_fixture();
    let fence = snapshot.assignment_fence().unwrap();
    let local = evidence.get(&0).unwrap();
    let authority = checkpoint_barrier_timing_authority(local, &fence).unwrap();

    assert_eq!(authority.process, local_process_identity(local));
    assert_eq!(authority.assignment_version, fence.assignment_version);
    assert_eq!(authority.assignment_certificate_digest, fence.digest());
    assert_ne!(
        authority.assignment_certificate_digest, local.adopted_assignment.assignment_digest,
        "certificate and owner-map digests are intentionally separate domains"
    );
}

#[cfg(feature = "kafka")]
#[test]
fn checkpoint_timing_authority_rejects_cross_domain_and_identity_substitution() {
    let (snapshot, evidence) = local_assignment_cut_fixture();
    let fence = snapshot.assignment_fence().unwrap();
    let local = evidence.get(&0).unwrap();

    let mut substituted_reporter = local.clone();
    substituted_reporter.adopted_assignment.participant = evidence.get(&1).unwrap().participant;
    assert!(checkpoint_barrier_timing_authority(&substituted_reporter, &fence).is_err());

    let mut substituted_owner_map = local.clone();
    substituted_owner_map.adopted_assignment.assignment_digest[0] ^= 0xff;
    assert!(checkpoint_barrier_timing_authority(&substituted_owner_map, &fence).is_err());

    let mut wrong_boot = local.clone();
    wrong_boot.participant.boot_incarnation = uuid::Uuid::from_u128(999);
    wrong_boot.adopted_assignment.participant = wrong_boot.participant;
    assert!(checkpoint_barrier_timing_authority(&wrong_boot, &fence).is_err());

    let mut noncanonical_process = local.clone();
    noncanonical_process.process_term = 0;
    assert!(checkpoint_barrier_timing_authority(&noncanonical_process, &fence).is_err());

    let mut noncanonical_fence = fence.clone();
    noncanonical_fence.assignment_version = 0;
    assert!(checkpoint_barrier_timing_authority(local, &noncanonical_fence).is_err());
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
        "recovered committed checkpoint checkpoint_id=41 epoch=41",
        expected
    ));
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
        "recovered committed checkpoint checkpoint_id=41 epoch=43",
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
        "laminardb_checkpoint_state_capture_duration_seconds_bucket{instance=\"node0\",pipeline=\"soak\",le=\"0.064\"} 39\n",
    );
    assert_eq!(
        prometheus_histogram_bucket_value(
            body,
            "laminardb_checkpoint_pipeline_stall_duration_seconds_bucket",
            CHECKPOINT_PIPELINE_STALL_SLO_SECONDS,
        ),
        Some(41.0)
    );
    assert_eq!(
        prometheus_histogram_bucket_value(
            body,
            "laminardb_checkpoint_state_capture_duration_seconds_bucket",
            CHECKPOINT_STATE_CAPTURE_SLO_SECONDS,
        ),
        Some(39.0)
    );
}

#[cfg(feature = "kafka")]
#[test]
fn hot_path_evidence_profiles_phases_and_top_normal_operators() {
    fn push_histogram(
        body: &mut String,
        metric: &str,
        labels: &str,
        count: u64,
        sum: f64,
        buckets: &[(&str, u64)],
    ) {
        body.push_str(&format!("{metric}_count{{{labels}}} {count}\n"));
        body.push_str(&format!("{metric}_sum{{{labels}}} {sum}\n"));
        for (bound, cumulative) in buckets {
            body.push_str(&format!(
                "{metric}_bucket{{{labels},le=\"{bound}\"}} {cumulative}\n"
            ));
        }
    }

    assert_eq!(
        prometheus_label_value(
            r#"instance="node0",operator="join,\"quoted\"\\tail",mode="normal""#,
            "operator",
        )
        .as_deref(),
        Some("join,\"quoted\"\\tail"),
    );

    let cycle_buckets = [("0.005", 90), ("0.01", 96), ("0.05", 100), ("+Inf", 100)];
    let operator_buckets = [
        ("0.0001", 40),
        ("0.0004", 80),
        ("0.0016", 100),
        ("+Inf", 100),
    ];
    let mut body = String::new();
    push_histogram(
        &mut body,
        "laminardb_cycle_duration_seconds",
        "instance=\"node0\"",
        100,
        0.8,
        &cycle_buckets,
    );
    for (phase, sum) in [
        ("execute", 0.5),
        ("output_store", 0.1),
        ("sink_enqueue", 0.2),
    ] {
        push_histogram(
            &mut body,
            &format!("laminardb_cycle_{phase}_duration_seconds"),
            "instance=\"node0\"",
            100,
            sum,
            &cycle_buckets,
        );
    }
    push_histogram(
        &mut body,
        "laminardb_operator_process_duration_seconds",
        "instance=\"node0\",operator=\"fast\",mode=\"normal\"",
        100,
        0.1,
        &operator_buckets,
    );
    push_histogram(
        &mut body,
        "laminardb_operator_process_duration_seconds",
        "instance=\"node0\",operator=\"slow\",mode=\"normal\"",
        100,
        0.4,
        &operator_buckets,
    );
    push_histogram(
        &mut body,
        "laminardb_operator_process_duration_seconds",
        "instance=\"node0\",operator=\"drain\",mode=\"checkpoint_drain\"",
        100,
        9.0,
        &operator_buckets,
    );

    let evidence = HotPathLatencyEvidence::from_metrics(&body).unwrap();
    assert_eq!(evidence.cycle.observations, 100);
    assert_eq!(evidence.phases.len(), 3);
    assert_eq!(evidence.operators.len(), 2);
    assert_eq!(evidence.operators[0].name, "slow mode=normal");
    assert_eq!(evidence.operators[1].name, "fast mode=normal");
    let diagnostic = evidence.failure_diagnostic();
    assert!(diagnostic.contains("execute total=0.500s"), "{diagnostic}");
    assert!(
        diagnostic.contains("slow mode=normal total=0.400s"),
        "{diagnostic}"
    );
    assert!(!diagnostic.contains("drain"), "{diagnostic}");
}

#[cfg(feature = "kafka")]
#[test]
fn prometheus_histogram_latency_aggregates_duplicate_label_series_by_bucket() {
    let body = concat!(
        "laminardb_cycle_duration_seconds_count{instance=\"node0\",pipeline=\"a\"} 60\n",
        "laminardb_cycle_duration_seconds_sum{instance=\"node0\",pipeline=\"a\"} 0.6\n",
        "laminardb_cycle_duration_seconds_bucket{instance=\"node0\",pipeline=\"a\",le=\"0.005\"} 60\n",
        "laminardb_cycle_duration_seconds_bucket{instance=\"node0\",pipeline=\"a\",le=\"0.01\"} 60\n",
        "laminardb_cycle_duration_seconds_bucket{instance=\"node0\",pipeline=\"a\",le=\"0.05\"} 60\n",
        "laminardb_cycle_duration_seconds_bucket{instance=\"node0\",pipeline=\"a\",le=\"+Inf\"} 60\n",
        "laminardb_cycle_duration_seconds_count{instance=\"node1\",pipeline=\"b\"} 40\n",
        "laminardb_cycle_duration_seconds_sum{instance=\"node1\",pipeline=\"b\"} 0.4\n",
        "laminardb_cycle_duration_seconds_bucket{instance=\"node1\",pipeline=\"b\",le=\"0.005\"} 0\n",
        "laminardb_cycle_duration_seconds_bucket{instance=\"node1\",pipeline=\"b\",le=\"0.01\"} 35\n",
        "laminardb_cycle_duration_seconds_bucket{instance=\"node1\",pipeline=\"b\",le=\"0.05\"} 40\n",
        "laminardb_cycle_duration_seconds_bucket{instance=\"node1\",pipeline=\"b\",le=\"+Inf\"} 40\n",
    );

    let latency =
        prometheus_histogram_latency(body, "laminardb_cycle_duration_seconds_bucket").unwrap();
    assert_eq!(latency.observations, 100);
    assert_eq!(latency.sum_seconds, Some(1.0));
    assert_eq!(latency.p50_upper_seconds, 0.005);
    assert_eq!(latency.p95_upper_seconds, 0.01);
    assert_eq!(latency.p99_upper_seconds, 0.05);
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
fn timing_test_authority(
    process_term: u64,
    assignment_version: u64,
    assignment_digest: [u8; 32],
) -> CheckpointBarrierTimingAuthority {
    CheckpointBarrierTimingAuthority {
        process: timing_test_process(process_term),
        assignment_version,
        assignment_certificate_digest: assignment_digest,
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
    let authority = timing_test_authority(1, 4, [44; 32]);
    timing.bind_authority(generation, authority).unwrap();
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
    let count = timing.record_count as f64;
    let aligned = timing.aligned_resume_count as f64;
    CheckpointLatencySnapshot {
        checkpoint_seconds: count * 0.1,
        checkpoint_observations: count,
        state_capture_seconds: count * 0.01,
        state_capture_observations: count,
        state_capture_within_slo: count,
        pipeline_stall_observations: count,
        pipeline_stall_within_slo: timing.pipeline_stall_within_slo as f64,
        barrier_local_seconds: count * 0.01,
        barrier_local_observations: count,
        barrier_local_within_slo: timing.barrier_local_within_slo as f64,
        aligned_resume_seconds: aligned * 0.005,
        aligned_resume_observations: aligned,
        aligned_resume_within_slo: timing.aligned_resume_within_slo as f64,
    }
}

#[cfg(feature = "kafka")]
#[test]
fn exact_timing_collector_accepts_pagination_and_exact_metric_boundary() {
    let generation = ProcessGeneration {
        node_id: 0,
        generation: 1,
    };
    let timing = timing_test_generation(65);
    assert_eq!(timing.cursor, 65);
    assert_eq!(timing.record_count, 65);
    assert_eq!(timing.metadata.unwrap().next_sequence, 66);
    timing
        .validate_against_metrics(generation, timing_test_metrics(&timing))
        .unwrap();

    let process = timing_test_process(1);
    let authority = timing_test_authority(1, 4, [44; 32]);
    let mut boundary = CheckpointBarrierTimingGeneration::default();
    boundary.bind_authority(generation, authority).unwrap();
    let mut record = timing_test_record(1);
    record.pipeline_stall_ns = CHECKPOINT_PIPELINE_STALL_SLO_NS;
    record.local_barrier_ns = CHECKPOINT_PIPELINE_STALL_SLO_NS;
    record.aligned_resume_ns = None;
    boundary
        .apply_page(
            generation,
            timing_test_envelope(process, 0, vec![record], 2, false),
        )
        .unwrap();
    boundary
        .validate_against_metrics(generation, timing_test_metrics(&boundary))
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
fn exact_timing_finalization_retries_transient_incoherent_cut() {
    let generation = ProcessGeneration {
        node_id: 0,
        generation: 1,
    };
    let process = timing_test_process(1);
    let initial = timing_test_generation(1);
    let one_record_metrics_before = timing_test_metrics(&initial);
    let mut one_record_metrics_after = one_record_metrics_before;
    one_record_metrics_after.checkpoint_seconds += 0.001;
    let two_record_metrics = timing_test_metrics(&timing_test_generation(2));
    let mut timing = CheckpointBarrierTimingEvidence::default();
    timing.generations.insert(generation, initial);
    let mut latency = CheckpointLatencyEvidence::default();
    let mut capture_calls = 0_u8;
    let mut metric_calls = 0_u8;

    timing
        .finalize_generation_with(
            generation,
            &mut latency,
            Instant::now() + Duration::from_secs(1),
            |evidence| {
                capture_calls += 1;
                let state = evidence
                    .generations
                    .get_mut(&generation)
                    .expect("scripted timing generation exists");
                if matches!(capture_calls, 1 | 4 | 7) {
                    assert!(
                        !state.finalized,
                        "an incoherent cut must not commit transient evidence"
                    );
                }
                let after_sequence = state.cursor;
                let records = if capture_calls == 6 {
                    vec![timing_test_record(2)]
                } else {
                    Vec::new()
                };
                let next_sequence = if records.is_empty() {
                    after_sequence + 1
                } else {
                    3
                };
                let has_more = state.apply_page(
                    generation,
                    timing_test_envelope(process, after_sequence, records, next_sequence, false),
                )?;
                if has_more {
                    return Err("scripted coherent-cut page unexpectedly continued".into());
                }
                Ok(())
            },
            || {
                metric_calls += 1;
                Some(match metric_calls {
                    1 => one_record_metrics_before,
                    2 => one_record_metrics_after,
                    3..=6 => two_record_metrics,
                    _ => panic!("coherent-cut script read too many metric snapshots"),
                })
            },
        )
        .unwrap();

    assert_eq!(capture_calls, 9, "both incoherent cuts must retry");
    assert_eq!(metric_calls, 6, "both incoherent cuts must retry");
    let finalized = timing.generations.get(&generation).unwrap();
    assert!(finalized.finalized);
    assert_eq!(finalized.cursor, 2);
    assert_eq!(finalized.record_count, 2);
    assert_eq!(
        latency.generations.get(&generation),
        Some(&two_record_metrics)
    );
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
            timing_test_envelope(process, 0, vec![timing_test_record(1)], 2, false),
        )
        .unwrap();
    assert!(unbound
        .validate_against_metrics(generation, timing_test_metrics(&unbound))
        .unwrap_err()
        .contains("never bound"));
    let wrong_process = timing_test_authority(2, 4, [44; 32]);
    assert!(unbound
        .bind_authority(generation, wrong_process)
        .unwrap_err()
        .contains("does not match"));
    let authority = timing_test_authority(1, 4, [44; 32]);
    unbound.bind_authority(generation, authority).unwrap();
    unbound
        .validate_against_metrics(generation, timing_test_metrics(&unbound))
        .unwrap();

    let mut conflicting_assignment = CheckpointBarrierTimingGeneration::default();
    conflicting_assignment
        .apply_page(
            generation,
            timing_test_envelope(process, 0, vec![timing_test_record(1)], 2, false),
        )
        .unwrap();
    let conflicting_authority = timing_test_authority(1, 4, [45; 32]);
    assert!(conflicting_assignment
        .bind_authority(generation, conflicting_authority)
        .unwrap_err()
        .contains("timing digest conflicts"));
    let mut missing_assignment = timing_test_generation(1);
    let later_authority = timing_test_authority(1, 5, [55; 32]);
    missing_assignment
        .bind_authority(generation, later_authority)
        .unwrap();
    assert!(missing_assignment
        .validate_against_metrics(generation, timing_test_metrics(&missing_assignment))
        .unwrap_err()
        .contains("no exact timing record under converged assignment version 5"));

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
    let generation = ProcessGeneration {
        node_id: 0,
        generation: 1,
    };
    let process = timing_test_process(1);
    let authority = timing_test_authority(1, 4, [44; 32]);
    let mut timing = CheckpointBarrierTimingGeneration::default();
    timing.bind_authority(generation, authority).unwrap();
    let mut records = (1..=2).map(timing_test_record).collect::<Vec<_>>();
    for record in &mut records {
        record.pipeline_stall_ns = CHECKPOINT_PIPELINE_STALL_SLO_NS + record.sequence;
        record.local_barrier_ns = CHECKPOINT_PIPELINE_STALL_SLO_NS + record.sequence;
        record.aligned_resume_ns = None;
        record.deadline_exhausted = true;
    }
    timing
        .apply_page(
            generation,
            timing_test_envelope(process, 0, records, 3, false),
        )
        .unwrap();
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
fn exact_timing_collector_streams_complete_artifact_with_bounded_memory() {
    let directory = tempfile::tempdir().unwrap();
    let generation = ProcessGeneration {
        node_id: 0,
        generation: 1,
    };
    let process = timing_test_process(1);
    let authority = timing_test_authority(1, 4, [44; 32]);
    let mut timing =
        CheckpointBarrierTimingGeneration::with_artifact(directory.path(), generation).unwrap();
    timing.bind_authority(generation, authority).unwrap();
    let mut records = (1..=65).map(timing_test_record).collect::<Vec<_>>();
    for record in records.iter_mut().take(9) {
        record.pipeline_stall_ns = CHECKPOINT_PIPELINE_STALL_SLO_NS + record.sequence;
    }
    let tail = records.split_off(64);
    timing
        .apply_page(
            generation,
            timing_test_envelope(process, 0, records, 66, true),
        )
        .unwrap();
    timing
        .apply_page(
            generation,
            timing_test_envelope(process, 64, tail, 66, false),
        )
        .unwrap();
    assert_eq!(timing.record_count, 65);
    assert_eq!(timing.violation_count, 9);
    assert_eq!(
        timing.violation_witnesses.len(),
        CHECKPOINT_BARRIER_TIMING_DIAGNOSTIC_WITNESSES
    );
    let artifact_path = timing.artifact.as_ref().unwrap().path.clone();
    timing.flush_artifact().unwrap();
    assert!(timing.artifact.as_ref().unwrap().writer.is_none());
    let artifact = std::fs::read_to_string(artifact_path).unwrap();
    let lines = artifact.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 65);
    let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    let last: serde_json::Value = serde_json::from_str(lines[64]).unwrap();
    assert_eq!(
        first["schema_version"],
        CHECKPOINT_BARRIER_TIMING_ARTIFACT_SCHEMA
    );
    assert_eq!(first["node_ordinal"], 0);
    assert_eq!(first["process_generation"], 1);
    assert_eq!(first["record"]["sequence"], 1);
    assert_eq!(last["record"]["sequence"], 65);
}

#[cfg(feature = "kafka")]
#[test]
fn exact_timing_completeness_covers_every_spawned_generation() {
    let nodes = [
        Node {
            id: 0,
            executable: Arc::new(ResolvedExecutable {
                path: PathBuf::new(),
                sha256: [0; 32],
                byte_len: 0,
                origin: ExecutableOrigin::CargoBuilt,
            }),
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
            executable: Arc::new(ResolvedExecutable {
                path: PathBuf::new(),
                sha256: [0; 32],
                byte_len: 0,
                origin: ExecutableOrigin::CargoBuilt,
            }),
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
    latency.validate_observed_generations(&nodes).unwrap();
    timing.generations.remove(&expected[0]);
    latency.generations.remove(&expected[0]);
    let error = timing.validate_observed_cuts(&latency, &nodes).unwrap_err();
    assert!(
        error.contains("every spawned process generation"),
        "{error}"
    );
    let error = latency.validate_observed_generations(&nodes).unwrap_err();
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
        checkpoint_seconds: checkpoint_observations * 0.1,
        checkpoint_observations,
        state_capture_seconds: pipeline_stall_observations * 0.01,
        state_capture_observations: pipeline_stall_observations,
        state_capture_within_slo: pipeline_stall_observations,
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
fn slo_mode_is_explicit_and_fail_closed() {
    for environment in [
        SOAK_CHECKPOINT_SLO_MODE_ENV,
        SOAK_HOT_SLO_MODE_ENV,
        #[cfg(feature = "delta-lake-s3")]
        SOAK_EO_VISIBILITY_SLO_MODE_ENV,
    ] {
        assert_eq!(SloMode::parse(environment, None).unwrap(), SloMode::Certify);
        assert_eq!(
            SloMode::parse(environment, Some("certify")).unwrap(),
            SloMode::Certify
        );
        assert_eq!(
            SloMode::parse(environment, Some("observe")).unwrap(),
            SloMode::Observe
        );
        for invalid in ["", "Observe", "off", "0"] {
            let error = SloMode::parse(environment, Some(invalid)).unwrap_err();
            assert!(error.contains(environment), "{error}");
            assert!(error.contains("'certify' or 'observe'"), "{error}");
        }
    }
}

#[cfg(feature = "kafka")]
#[test]
fn observed_hot_path_slo_miss_remains_non_certifying() {
    enforce_or_report_hot_path_slo(
        SloMode::Observe,
        "test workload",
        0,
        "p99",
        51.0,
        50,
        "diagnostic",
    );
}

#[cfg(feature = "kafka")]
#[test]
#[should_panic(expected = "hot-path p99 bucket upper bound 51.000ms exceeds 50ms")]
fn certified_hot_path_slo_miss_fails_closed() {
    enforce_or_report_hot_path_slo(
        SloMode::Certify,
        "test workload",
        0,
        "p99",
        51.0,
        50,
        "diagnostic",
    );
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
#[test]
fn observed_visibility_slo_miss_remains_non_certifying() {
    enforce_or_report_visibility_slo(
        SloMode::Observe,
        "test Delta output",
        Duration::from_millis(11),
        Duration::from_millis(10),
    );
}

#[cfg(all(feature = "kafka", feature = "delta-lake-s3"))]
#[test]
#[should_panic(expected = "test Delta output visibility 11ms exceeded 10ms")]
fn certified_visibility_slo_miss_fails_closed() {
    enforce_or_report_visibility_slo(
        SloMode::Certify,
        "test Delta output",
        Duration::from_millis(11),
        Duration::from_millis(10),
    );
}

#[cfg(feature = "kafka")]
#[test]
fn checkpoint_slo_observation_targets_preserve_certification_floor() {
    assert_eq!(
        required_live_checkpoint_observations(
            SloMode::Certify,
            MIN_CHECKPOINT_PIPELINE_STALL_OBSERVATIONS,
            0,
        ),
        MIN_CHECKPOINT_PIPELINE_STALL_OBSERVATIONS
    );
    assert_eq!(
        required_live_checkpoint_observations(
            SloMode::Certify,
            MIN_CHECKPOINT_PIPELINE_STALL_OBSERVATIONS,
            47,
        ),
        53
    );
    assert_eq!(
        required_live_checkpoint_observations(
            SloMode::Certify,
            MIN_CHECKPOINT_PIPELINE_STALL_OBSERVATIONS,
            MIN_CHECKPOINT_PIPELINE_STALL_OBSERVATIONS,
        ),
        1
    );
    for finalized in [0, 1, MIN_CHECKPOINT_PIPELINE_STALL_OBSERVATIONS, u64::MAX] {
        assert_eq!(
            required_live_checkpoint_observations(
                SloMode::Observe,
                MIN_CHECKPOINT_PIPELINE_STALL_OBSERVATIONS,
                finalized,
            ),
            1
        );
    }
}

#[cfg(feature = "kafka")]
#[test]
fn checkpoint_slo_observe_mode_requires_evidence_without_certifying_performance() {
    let generation = ProcessGeneration {
        node_id: 0,
        generation: 1,
    };
    let mut slow = CheckpointLatencyEvidence::default();
    slow.record_generation(
        generation,
        test_checkpoint_latency_snapshot(100.0, 100.0, 88.0),
    )
    .unwrap();
    assert!(slow.validate_for_mode(SloMode::Observe, true).is_ok());
    let error = slow.validate_for_mode(SloMode::Certify, true).unwrap_err();
    assert!(error.contains("88.00%"), "{error}");

    let mut slow_capture = CheckpointLatencyEvidence::default();
    let mut snapshot = test_checkpoint_latency_snapshot(100.0, 100.0, 100.0);
    snapshot.state_capture_within_slo = 88.0;
    slow_capture
        .record_generation(generation, snapshot)
        .unwrap();
    assert!(slow_capture
        .validate_for_mode(SloMode::Observe, true)
        .is_ok());
    let error = slow_capture
        .validate_for_mode(SloMode::Certify, true)
        .unwrap_err();
    assert!(error.contains("state-capture SLO"), "{error}");
    assert!(error.contains("88.00%"), "{error}");

    let mut missing_stall = CheckpointLatencyEvidence::default();
    missing_stall
        .record_generation(generation, test_checkpoint_latency_snapshot(1.0, 0.0, 0.0))
        .unwrap();
    let error = missing_stall
        .validate_for_mode(SloMode::Observe, true)
        .unwrap_err();
    assert!(
        error.contains("captured no checkpoint pipeline-stall observations"),
        "{error}"
    );

    let mut missing_capture = CheckpointLatencyEvidence::default();
    let mut snapshot = test_checkpoint_latency_snapshot(1.0, 1.0, 1.0);
    snapshot.state_capture_seconds = 0.0;
    snapshot.state_capture_observations = 0.0;
    snapshot.state_capture_within_slo = 0.0;
    missing_capture
        .record_generation(generation, snapshot)
        .unwrap();
    let error = missing_capture
        .validate_for_mode(SloMode::Observe, false)
        .unwrap_err();
    assert!(
        error.contains("captured no checkpoint state-capture observations"),
        "{error}"
    );
}

#[cfg(feature = "kafka")]
#[test]
fn checkpoint_observation_budget_tracks_durable_cadence_and_caps() {
    let fast = test_checkpoint_latency_snapshot(90.0, 90.0, 90.0);
    let fast_budget = checkpoint_observation_budget(
        250,
        SINGLE_CHECKPOINT_TIMEOUT,
        Duration::from_secs(90),
        &[(100, 100, fast)],
    )
    .unwrap();
    assert_eq!(fast_budget.max_missing, 10);
    assert_eq!(fast_budget.max_cadence, Duration::from_millis(1_274));
    assert_eq!(fast_budget.collection, Duration::from_millis(12_740));
    assert_eq!(fast_budget.total, Duration::from_millis(102_740));
    assert!(!fast_budget.capped);

    let mut slow = test_checkpoint_latency_snapshot(34.0, 34.0, 34.0);
    slow.checkpoint_seconds = 34.0 * 5.64;
    let slow_budget = checkpoint_observation_budget(
        250,
        SINGLE_CHECKPOINT_TIMEOUT,
        Duration::from_secs(90),
        &[(100, 100, slow)],
    )
    .unwrap();
    assert_eq!(slow_budget.max_missing, 66);
    assert_eq!(slow_budget.max_cadence, Duration::from_millis(11_530));
    assert_eq!(slow_budget.collection, Duration::from_millis(760_980));
    assert_eq!(slow_budget.total, Duration::from_millis(850_980));
    assert!(!slow_budget.capped);

    let cluster_budget = checkpoint_observation_budget(
        250,
        CLUSTER_CHECKPOINT_TIMEOUT,
        Duration::from_secs(90),
        &[(100, 100, fast), (100, 100, slow)],
    )
    .unwrap();
    assert_eq!(cluster_budget.collection, slow_budget.collection);

    let mut slow_almost_complete = slow;
    slow_almost_complete.pipeline_stall_observations = 99.0;
    slow_almost_complete.state_capture_observations = 99.0;
    let crossing_budget = checkpoint_observation_budget(
        250,
        CLUSTER_CHECKPOINT_TIMEOUT,
        Duration::from_secs(90),
        &[(100, 100, fast), (100, 100, slow_almost_complete)],
    )
    .unwrap();
    assert_eq!(crossing_budget.max_missing, 10);
    assert_eq!(crossing_budget.max_cadence, Duration::from_millis(11_530));
    assert_eq!(crossing_budget.collection, Duration::from_millis(115_300));

    let mut saturated = test_checkpoint_latency_snapshot(1.0, 1.0, 1.0);
    saturated.checkpoint_seconds = 60.0;
    let saturated_budget = checkpoint_observation_budget(
        250,
        SINGLE_CHECKPOINT_TIMEOUT,
        Duration::from_secs(90),
        &[(100, 100, saturated)],
    )
    .unwrap();
    assert_eq!(saturated_budget.max_cadence, Duration::from_millis(30_250));
    assert_eq!(
        saturated_budget.collection,
        CHECKPOINT_OBSERVATION_COLLECTION_CAP
    );
    assert_eq!(
        saturated_budget.total,
        CHECKPOINT_OBSERVATION_COLLECTION_CAP + Duration::from_secs(90)
    );
    assert!(saturated_budget.capped);

    let complete = test_checkpoint_latency_snapshot(100.0, 100.0, 100.0);
    let complete_budget = checkpoint_observation_budget(
        250,
        SINGLE_CHECKPOINT_TIMEOUT,
        Duration::from_secs(90),
        &[(100, 100, complete)],
    )
    .unwrap();
    assert_eq!(complete_budget.collection, Duration::ZERO);
    assert_eq!(complete_budget.total, Duration::from_secs(90));
}

#[cfg(feature = "kafka")]
#[test]
fn checkpoint_observation_budget_rejects_invalid_metrics() {
    assert!(checkpoint_observation_budget(
        250,
        SINGLE_CHECKPOINT_TIMEOUT,
        Duration::from_secs(90),
        &[]
    )
    .unwrap_err()
    .contains("at least one node"));
    assert!(checkpoint_observation_budget(
        250,
        Duration::ZERO,
        Duration::from_secs(90),
        &[(1, 1, test_checkpoint_latency_snapshot(1.0, 1.0, 1.0))]
    )
    .unwrap_err()
    .contains("greater than zero"));
    assert!(checkpoint_observation_budget(
        250,
        SINGLE_CHECKPOINT_TIMEOUT,
        Duration::from_secs(90),
        &[(1, 1, CheckpointLatencySnapshot::default())]
    )
    .unwrap_err()
    .contains("no checkpoint-duration observations"));
    assert!(checkpoint_observation_budget(
        250,
        SINGLE_CHECKPOINT_TIMEOUT,
        Duration::from_secs(90),
        &[(1, 1, test_checkpoint_latency_snapshot(1.0, 1.5, 1.0))]
    )
    .unwrap_err()
    .contains("exact non-negative integer"));
    assert!(checkpoint_observation_budget(
        250,
        SINGLE_CHECKPOINT_TIMEOUT,
        Duration::MAX,
        &[(1, 1, test_checkpoint_latency_snapshot(1.0, 1.0, 1.0))]
    )
    .unwrap_err()
    .contains("recovery ceiling does not fit"));
}

#[cfg(feature = "kafka")]
#[test]
fn durable_progress_window_covers_failed_and_restored_checkpoint_cycles() {
    assert_eq!(
        recovery_aware_durable_progress_window(
            10_000,
            CLUSTER_CHECKPOINT_TIMEOUT,
            Duration::from_secs(90),
            1,
        ),
        Duration::from_secs(170)
    );
    assert_eq!(
        recovery_aware_durable_progress_window(
            10_000,
            CLUSTER_CHECKPOINT_TIMEOUT,
            Duration::from_secs(90),
            2,
        ),
        Duration::from_secs(210)
    );
    assert_eq!(
        recovery_aware_durable_progress_window(
            10_000,
            CLUSTER_EMULATOR_EO_CHECKPOINT_TIMEOUT,
            Duration::from_secs(90),
            1,
        ),
        Duration::from_secs(230)
    );
    assert_eq!(
        recovery_aware_durable_progress_window(
            10_000,
            CLUSTER_EMULATOR_EO_CHECKPOINT_TIMEOUT,
            Duration::from_secs(90),
            2,
        ),
        Duration::from_secs(300)
    );
    assert_eq!(
        recovery_aware_durable_progress_window(u64::MAX, Duration::MAX, Duration::MAX, u32::MAX),
        Duration::MAX
    );
}

#[cfg(feature = "kafka")]
#[test]
fn single_node_latency_finalization_requires_a_stable_once_only_cut() {
    let generation = ProcessGeneration {
        node_id: 0,
        generation: 1,
    };
    let first = test_checkpoint_latency_snapshot(1.0, 1.0, 1.0);
    let stable = test_checkpoint_latency_snapshot(2.0, 2.0, 2.0);
    let mut samples =
        std::collections::VecDeque::from([None, Some(first), Some(stable), Some(stable)]);
    let mut evidence = CheckpointLatencyEvidence::default();
    evidence
        .finalize_stable_generation(generation, Instant::now() + Duration::from_secs(1), || {
            samples.pop_front().flatten()
        })
        .unwrap();
    assert_eq!(evidence.generations.get(&generation), Some(&stable));

    let error = evidence
        .finalize_stable_generation(generation, Instant::now() + Duration::from_secs(1), || {
            Some(stable)
        })
        .unwrap_err();
    assert!(error.contains("captured more than once"), "{error}");
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

    let mut fractional_capture_count = test_checkpoint_latency_snapshot(1.0, 10.0, 10.0);
    fractional_capture_count.state_capture_observations = 9.5;
    assert!(fractional_capture_count
        .validate()
        .unwrap_err()
        .contains("exact non-negative integer"));

    let mut impossible_capture_bucket = test_checkpoint_latency_snapshot(1.0, 10.0, 10.0);
    impossible_capture_bucket.state_capture_within_slo = 11.0;
    assert!(impossible_capture_bucket
        .validate()
        .unwrap_err()
        .contains("state-capture SLO bucket"));

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
    let error = evidence.validate_slos(true).unwrap_err();
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
            test_checkpoint_latency_snapshot(1.0, 0.0, 0.0),
        )
        .unwrap();
    let error = missing_generation.validate_slos(true).unwrap_err();
    assert!(error.contains("node1 process generation 1"), "{error}");
    assert!(
        error.contains("no checkpoint pipeline-stall observations"),
        "{error}"
    );

    let mut missing_follower_duration = CheckpointLatencyEvidence::default();
    missing_follower_duration
        .record_generation(
            ProcessGeneration {
                node_id: 0,
                generation: 1,
            },
            test_checkpoint_latency_snapshot(100.0, 100.0, 100.0),
        )
        .unwrap();
    missing_follower_duration
        .record_generation(
            ProcessGeneration {
                node_id: 1,
                generation: 1,
            },
            test_checkpoint_latency_snapshot(0.0, 100.0, 100.0),
        )
        .unwrap();
    let error = missing_follower_duration.validate_slos(true).unwrap_err();
    assert!(
        error.contains("node1 process generation 1 captured no checkpoint-duration observations"),
        "{error}"
    );

    let mut healthy_follower = CheckpointLatencyEvidence::default();
    for node_id in 0..=1 {
        healthy_follower
            .record_generation(
                ProcessGeneration {
                    node_id,
                    generation: 1,
                },
                test_checkpoint_latency_snapshot(100.0, 100.0, 100.0),
            )
            .unwrap();
    }
    assert!(healthy_follower.validate_slos(true).is_ok());
}

#[cfg(feature = "kafka")]
#[test]
fn checkpoint_state_capture_gate_survives_restarts_without_cluster_dilution() {
    let mut evidence = CheckpointLatencyEvidence::default();
    for generation in 1..=2 {
        let mut snapshot = test_checkpoint_latency_snapshot(60.0, 60.0, 60.0);
        snapshot.state_capture_within_slo = 59.0;
        evidence
            .record_generation(
                ProcessGeneration {
                    node_id: 0,
                    generation,
                },
                snapshot,
            )
            .unwrap();
    }
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
    assert!(aggregate.state_capture_within_slo_percent().unwrap() > 99.0);
    let error = evidence.validate_slos(true).unwrap_err();
    assert!(
        error.contains("node0 across process generations"),
        "{error}"
    );
    assert!(error.contains("state-capture SLO"), "{error}");
    assert!(error.contains("98.33%"), "{error}");

    let mut missing_generation = CheckpointLatencyEvidence::default();
    let mut empty_capture = test_checkpoint_latency_snapshot(1.0, 100.0, 100.0);
    empty_capture.state_capture_seconds = 0.0;
    empty_capture.state_capture_observations = 0.0;
    empty_capture.state_capture_within_slo = 0.0;
    missing_generation
        .record_generation(
            ProcessGeneration {
                node_id: 0,
                generation: 1,
            },
            empty_capture,
        )
        .unwrap();
    missing_generation
        .record_generation(
            ProcessGeneration {
                node_id: 0,
                generation: 2,
            },
            test_checkpoint_latency_snapshot(100.0, 100.0, 100.0),
        )
        .unwrap();
    let error = missing_generation.validate_slos(true).unwrap_err();
    assert!(error.contains("node0 process generation 1"), "{error}");
    assert!(error.contains("no checkpoint state-capture"), "{error}");
    assert!(missing_generation.validate_slos(false).is_ok());

    let mut insufficient = CheckpointLatencyEvidence::default();
    let mut snapshot = test_checkpoint_latency_snapshot(100.0, 100.0, 100.0);
    snapshot.state_capture_seconds = 0.99;
    snapshot.state_capture_observations = 99.0;
    snapshot.state_capture_within_slo = 99.0;
    insufficient
        .record_generation(
            ProcessGeneration {
                node_id: 0,
                generation: 1,
            },
            snapshot,
        )
        .unwrap();
    let error = insufficient.validate_slos(false).unwrap_err();
    assert!(
        error.contains("captured only 99 checkpoint state-capture observations"),
        "{error}"
    );
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
    assert_eq!(node.state_capture_observations, 122.0);
    assert_eq!(node.state_capture_within_slo, 122.0);
    assert!(evidence.validate_slos(true).is_ok());
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
    assert_eq!(aggregate.state_capture_observations, 30.0);
    assert_eq!(aggregate.state_capture_within_slo, 30.0);
    assert_eq!(aggregate.pipeline_stall_observations, 30.0);
    assert_eq!(aggregate.pipeline_stall_within_slo, 30.0);
}

#[test]
#[cfg(feature = "kafka")]
fn bounded_join_oracle_matches_one_sided_sql_contract() {
    let inputs = [
        JoinInput {
            id: 0,
            key: 7,
            event_time_ms: 1_000,
        },
        JoinInput {
            id: 1,
            key: 7,
            event_time_ms: 1_050,
        },
        JoinInput {
            id: 2,
            key: 7,
            event_time_ms: 1_151,
        },
        JoinInput {
            id: 3,
            key: 8,
            event_time_ms: 1_050,
        },
    ];

    assert_eq!(
        expected_join_pairs_for_interval(&inputs, DEFAULT_JOIN_INTERVAL_MS),
        BTreeSet::from([(0, 0), (0, 1), (1, 1), (2, 2), (3, 3)])
    );
}

#[cfg(feature = "kafka")]
#[test]
fn cluster_soak_config_bounds_checkpoint_timeout_within_liveness_window() {
    assert_eq!(CLUSTER_CHECKPOINT_TIMEOUT, Duration::from_secs(30));
    assert_eq!(
        cluster_checkpoint_timeout(JoinDelivery::AtLeastOnce, true),
        CLUSTER_CHECKPOINT_TIMEOUT
    );
    assert_eq!(
        cluster_checkpoint_timeout(JoinDelivery::ExactlyOnce, false),
        CLUSTER_CHECKPOINT_TIMEOUT
    );
    assert_eq!(
        cluster_checkpoint_timeout(JoinDelivery::ExactlyOnce, true),
        CLUSTER_EMULATOR_EO_CHECKPOINT_TIMEOUT
    );
    assert!(CLUSTER_CHECKPOINT_TIMEOUT < RECOVERY_LIVENESS_WINDOW);
    assert!(CLUSTER_EMULATOR_EO_CHECKPOINT_TIMEOUT < RECOVERY_LIVENESS_WINDOW);

    let directory = tempfile::tempdir().unwrap();
    let path = write_config(
        directory.path(),
        0,
        250,
        DEFAULT_CLUSTER_KEY_GROUPS,
        "s3://soak/checkpoints",
        "broker:9092",
        "left",
        "right",
        "temporal-left",
        "temporal-right",
        "soak-group",
        "matrix-left",
        "matrix-right",
        "matrix-group",
        JoinDelivery::AtLeastOnce,
        "",
        "",
        "",
    );
    let config = std::fs::read_to_string(path).unwrap();
    let (_, checkpoint_and_later) = config.split_once("[checkpoint]").unwrap();
    let (checkpoint, _) = checkpoint_and_later
        .split_once("[checkpoint.storage]")
        .unwrap();
    assert!(checkpoint.contains("timeout = \"30s\""), "{checkpoint}");
}

#[cfg(feature = "kafka")]
fn subscription_data_fixture(
    partition: u16,
    sequence: u64,
    epoch: u64,
    key: u64,
    count: u64,
    max_right_id: u64,
) -> serde_json::Value {
    serde_json::json!({
        "type": "data",
        "stream_generation": "0101010101010101010101010101010101010101010101010101010101010101",
        "partition": partition.to_string(),
        "partition_sequence": sequence.to_string(),
        "committed_epoch": epoch.to_string(),
        "row_offset": "0",
        "row_count": "1",
        "data": [{
            "join_key": key,
            "match_count": count,
            "max_right_id": max_right_id,
        }],
    })
}

#[cfg(feature = "kafka")]
#[test]
fn cluster_subscription_oracle_checks_duplicates_gaps_and_progress() {
    let directory = tempfile::tempdir().unwrap();
    let journal = directory.path().join("progress.log");
    let mut observation = ClusterSubscriptionObservation::default();
    let first = subscription_data_fixture(3, 40, 7, 11, 2, 9);
    observation.observe_data(&first).unwrap();
    observation
        .observe_data(&subscription_data_fixture(1, 12, 7, 22, 4, 17))
        .unwrap();
    observation
        .observe_data(&subscription_data_fixture(3, 41, 7, 11, 3, 19))
        .unwrap();
    observation
        .observe_data(&first)
        .expect("an exact duplicate chunk is idempotent");
    let conflict = subscription_data_fixture(3, 40, 7, 11, 99, 9);
    assert!(observation.observe_data(&conflict).is_err());

    observation
        .observe_progress(
            &serde_json::json!({
                "type": "progress",
                "stream_generation": "0101010101010101010101010101010101010101010101010101010101010101",
                "epoch": "7",
                "checkpoint_id": "7",
            }),
            &journal,
        )
        .unwrap();
    assert_eq!(load_subscription_progress(&journal).unwrap(), Some(7));
    assert!(observation
        .observe_data(&subscription_data_fixture(3, 43, 8, 11, 4, 29))
        .is_err());
}

#[cfg(feature = "kafka")]
#[test]
fn cluster_subscription_input_oracle_is_independent_and_bounded_by_key_count() {
    let produced = ProducedPrefix {
        count: 3,
        end_offsets: vec![3, 3],
        expected_pairs: BTreeSet::from([(0, 0), (0, 1), (1, 1), (2, 2)]),
        expected_temporal_pairs: BTreeSet::new(),
        elapsed: Duration::from_secs(1),
        broker_acked_at: Instant::now(),
    };
    let expected = expected_cluster_subscription_aggregate(&produced, 4, 0);
    assert!(!expected.is_empty());
    assert!(expected.len() <= 4);
    assert_eq!(expected.values().map(|(count, _)| count).sum::<u64>(), 3);
    let workload = cluster_subscription_workload_config();
    assert!(workload.contains("name = \"soak_subscription_aggregate\""));
    assert!(workload.contains("FROM join_left"));
    assert!(workload.contains("WHERE join_key >= 0"));
    assert!(workload.contains("WITH ('retain_history' = '256mb')"));
}

#[cfg(feature = "kafka")]
#[test]
fn cluster_subscription_observer_retries_gateway_reset_without_close_handshake() {
    let reset = tungstenite::Error::Protocol(
        tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
    );
    assert!(subscription_websocket_error_is_retryable(&reset));
}

#[cfg(feature = "kafka")]
#[test]
fn mutable_interval_workload_binds_key_time_and_changelog_sink() {
    let config = mutable_bounded_interval_workload_config(
        "broker:9092",
        "mutable-left",
        "mutable-right",
        "mutable-group",
        "mutable-output",
    );
    assert!(config.contains("format = \"debezium\""));
    assert!(config.contains("primary_key = [\"id\", \"join_key\", \"event_time\"]"));
    assert!(config.contains("l.join_key = r.join_key"));
    assert!(config.contains("r.event_time BETWEEN l.event_time"));
    assert!(config.contains("FROM soak_mutable_interval\nGROUP BY left_id"));
    assert!(config.contains("GROUP BY left_id\nEMIT CHANGES"));
    assert!(config.contains("envelope = \"upsert\""));
    assert!(config.contains("\"key.column\" = \"left_id\""));
}

#[cfg(feature = "kafka")]
#[test]
fn kafka_output_soak_profile_disables_linger_for_every_keyed_sink() {
    let config = format!(
        "{}{}{}{}{}",
        kafka_pair_sink_config("pair-output", "pair-pipeline", "broker:9092", "pair-topic"),
        kafka_matrix_sink_config("broker:9092", "matrix-topic"),
        kafka_matrix_aggregate_sink_config("broker:9092", "matrix-aggregate-topic"),
        kafka_core_window_sink_config("broker:9092", "window-topic"),
        mutable_bounded_interval_workload_config(
            "broker:9092",
            "mutable-left",
            "mutable-right",
            "mutable-group",
            "mutable-output",
        ),
    );
    let parsed = toml::from_str::<toml::Value>(&config).expect("generated soak config is TOML");
    let sinks = parsed
        .get("sink")
        .and_then(toml::Value::as_array)
        .expect("generated config has sink tables");
    assert_eq!(
        sinks.len(),
        1 + MATRIX_OUTPUT_PIPELINES.len()
            + BOUNDED_JOIN_SOAK_CASES.len()
            + CORE_WINDOW_CANARIES.len()
            + 1,
        "the representative config must cover every Kafka sink generator family"
    );
    for sink in sinks {
        assert_eq!(
            sink.get("connector").and_then(toml::Value::as_str),
            Some("kafka")
        );
        let properties = sink
            .get("properties")
            .and_then(toml::Value::as_table)
            .expect("Kafka soak sink has properties");
        assert!(
            properties
                .get("key.column")
                .and_then(toml::Value::as_str)
                .is_some_and(|key| !key.trim().is_empty()),
            "every low-latency soak sink must remain keyed"
        );
        assert_eq!(
            properties.get("linger.ms").and_then(toml::Value::as_str),
            Some("0"),
            "every Kafka output in the latency profile must explicitly disable linger"
        );
    }
}

#[cfg(feature = "kafka")]
#[test]
fn matrix_aggregate_canaries_emit_only_differential_final_state() {
    let pipelines = bounded_join_matrix_aggregate_pipeline_config();
    assert!(
        !pipelines.contains("HAVING"),
        "managed changelog aggregates must not depend on unsupported HAVING retractions"
    );
    assert_eq!(
        pipelines.matches("EMIT CHANGES").count(),
        BOUNDED_JOIN_SOAK_CASES.len(),
        "every matrix aggregate must emit only input-driven differential updates"
    );

    let sinks = kafka_matrix_aggregate_sink_config("broker:9092", "matrix-output");
    assert_eq!(
        sinks.matches("envelope = \"upsert\"").count(),
        BOUNDED_JOIN_SOAK_CASES.len(),
        "every weighted matrix aggregate must use a changelog-capable keyed sink"
    );
    assert_eq!(
        sinks.matches("\"key.column\" = \"join_case\"").count(),
        BOUNDED_JOIN_SOAK_CASES.len()
    );

    let expected = expected_matrix_aggregates();
    assert_eq!(
        validate_matrix_aggregate_latest(&expected, &expected),
        Ok(())
    );
    let mut incomplete = expected.clone();
    incomplete.remove(BOUNDED_JOIN_SOAK_CASES[0].name);
    assert!(validate_matrix_aggregate_latest(&incomplete, &expected).is_err());

    let old = MatrixAggregateOutput {
        row_count: 1,
        left_count: 1,
        right_count: 1,
        left_sum: Some(10),
        right_sum: Some(20),
    };
    let final_state = MatrixAggregateOutput {
        row_count: 2,
        ..old
    };
    let expected_net = BTreeMap::from([(final_state, 1)]);
    assert_eq!(
        net_matrix_aggregate_deltas([(old, 1), (old, -1), (final_state, 1)], "ordered",),
        Ok(expected_net.clone())
    );
    assert_eq!(
        net_matrix_aggregate_deltas([(final_state, 1), (old, -1), (old, 1)], "reordered",),
        Ok(expected_net)
    );
    assert_eq!(
        net_matrix_aggregate_deltas([(old, 1), (final_state, 1)], "missing retraction")
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        net_matrix_aggregate_deltas([(final_state, 1), (final_state, 1)], "duplicate")
            .unwrap()
            .get(&final_state),
        Some(&2)
    );
    assert!(net_matrix_aggregate_deltas([(final_state, 0)], "zero").is_err());
}

#[cfg(feature = "kafka")]
#[test]
fn recovery_canary_event_time_holds_ordered_and_window_cuts_open() {
    assert!(
        RECOVERY_CANARY_EVENT_LEAD_MS < SOAK_EVENT_MAX_FUTURE_SKEW_MS,
        "the recovery event must remain inside the server's configured future-skew guard"
    );
    assert_eq!(
        recovery_canary_event_time_ms(1_000),
        1_000 + RECOVERY_CANARY_EVENT_LEAD_MS
    );
    assert_eq!(recovery_canary_event_time_ms(u64::MAX), u64::MAX);
    assert_eq!(matrix_post_event_time_ms(1_000, 2_000), 2_000);
    assert_eq!(matrix_post_event_time_ms(3_000, 2_000), 3_000);
    validate_mutable_interval_recovery_horizon(4, Duration::from_secs(90));
    validate_matrix_recovery_horizon(4, Duration::from_secs(90), 90, DEFAULT_JOIN_INTERVAL_MS);
    assert!(
        std::panic::catch_unwind(|| {
            validate_mutable_interval_recovery_horizon(9, Duration::from_secs(90));
        })
        .is_err(),
        "a configured fault schedule beyond the held-open event horizon must fail before intake"
    );
    assert!(
        std::panic::catch_unwind(|| {
            validate_matrix_recovery_horizon(
                6,
                Duration::from_secs(90),
                90,
                DEFAULT_JOIN_INTERVAL_MS,
            );
        })
        .is_err(),
        "a matrix fault schedule beyond the held-open event horizon must fail before intake"
    );

    let expected = expected_core_window_outputs(12_345);
    for hop_start in [11_500, 12_000] {
        assert!(expected.contains_key(&CoreWindowOutput {
            kind: "hop".to_owned(),
            window_start_ms: hop_start,
            row_count: 4,
            id_sum: -412,
        }));
    }
}

#[cfg(feature = "kafka")]
#[test]
fn mutable_interval_transition_oracle_rejects_positive_only_update() {
    let expected = expected_mutable_interval_transitions();
    assert_eq!(
        validate_mutable_interval_transitions(&expected, &expected),
        Ok(true)
    );
    assert_eq!(
        validate_mutable_interval_transitions(&expected[..2], &expected),
        Ok(false)
    );

    let positive_only = [
        expected[0],
        MutableIntervalSinkState::Live(MutableIntervalAggregateOutput {
            match_count: 2,
            value_sum: MUTABLE_INTERVAL_INITIAL_VALUE + MUTABLE_INTERVAL_UPDATED_VALUE,
        }),
    ];
    let error = validate_mutable_interval_transitions(&positive_only, &expected).unwrap_err();
    assert!(error.contains("transition 1"), "{error}");
}

#[cfg(feature = "kafka")]
#[test]
fn mutable_interval_stability_requires_a_quiet_broker_boundary() {
    let expected = expected_mutable_interval_transitions();
    let start = Instant::now();
    let stability = Duration::from_secs(3);
    let mut stable_since = None;

    assert!(!mutable_interval_logical_output_stable(
        &expected,
        &expected,
        false,
        &mut stable_since,
        start,
        stability,
    )
    .unwrap());
    assert_eq!(
        stable_since, None,
        "physical consumer lag must reset output-boundary stability"
    );
    assert!(!mutable_interval_logical_output_stable(
        &expected,
        &expected,
        true,
        &mut stable_since,
        start + Duration::from_secs(1),
        stability,
    )
    .unwrap());
    assert_eq!(stable_since, Some(start + Duration::from_secs(1)));
    // Even an adjacent-identical ALO upsert leaves physical evidence. A growing broker boundary
    // must restart the quiet interval despite leaving the deduplicated transition slice unchanged.
    assert!(!mutable_interval_logical_output_stable(
        &expected,
        &expected,
        false,
        &mut stable_since,
        start + Duration::from_secs(2),
        stability,
    )
    .unwrap());
    assert_eq!(stable_since, None);
    assert!(!mutable_interval_logical_output_stable(
        &expected,
        &expected,
        true,
        &mut stable_since,
        start + Duration::from_secs(2),
        stability,
    )
    .unwrap());
    assert!(mutable_interval_logical_output_stable(
        &expected,
        &expected,
        true,
        &mut stable_since,
        start + Duration::from_secs(2) + stability,
        stability,
    )
    .unwrap());

    let mut changed = expected.to_vec();
    changed.push(MutableIntervalSinkState::Tombstone);
    let error = mutable_interval_logical_output_stable(
        &changed,
        &expected,
        true,
        &mut stable_since,
        start + Duration::from_secs(2) + stability,
        stability,
    )
    .unwrap_err();
    assert!(error.contains("extra state transition"), "{error}");
}

#[cfg(feature = "kafka")]
#[test]
fn continuous_aggregate_minimum_scales_with_key_profile() {
    assert_eq!(
        continuous_aggregate_minimum_bytes(DEFAULT_JOIN_KEYS),
        MIN_CONTINUOUS_AGGREGATE_STATE_BYTES
    );
    assert_eq!(
        continuous_aggregate_minimum_bytes(DEFAULT_JOIN_KEYS / 4),
        MIN_CONTINUOUS_AGGREGATE_STATE_BYTES / 4
    );
    assert_eq!(continuous_aggregate_minimum_bytes(1), 16);
    assert_eq!(
        continuous_aggregate_minimum_bytes(DEFAULT_JOIN_KEYS * 2),
        MIN_CONTINUOUS_AGGREGATE_STATE_BYTES
    );
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
fn local_exact_source_cursor_oracle_reads_v8_node_manifest() {
    let directory = tempfile::tempdir().unwrap();
    let checkpoint = DurableCheckpointStatus {
        checkpoint_id: 7,
        epoch: 7,
    };
    let mut manifest = laminar_core::checkpoint::CheckpointManifest::new(7, 7);
    manifest.source_offsets.insert(
        "gen".into(),
        laminar_core::checkpoint::ConnectorCheckpoint::with_offsets(
            std::collections::HashMap::from([("seq".into(), "4096".into())]),
        ),
    );
    let path = directory
        .path()
        .join("nodes/1/checkpoints/00000000000000000007/manifest.json");
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
