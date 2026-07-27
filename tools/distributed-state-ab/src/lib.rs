#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::fmt;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

pub mod observer_protocol;
pub mod paced_observer;

pub const NOTICE: &str = "NOT A/B OR CERTIFICATION EVIDENCE";
pub const MANIFEST_SCHEMA: &str = "laminardb-instrumentation-ab-nonfeedback-manifest/v1";
pub const BASE_PLAN_SCHEMA: &str = "laminardb-instrumentation-ab-base-plan/v1";
pub const DRIVER_TRACE_SCHEMA: &str = "laminardb-instrumentation-ab-driver-trace/v1";
pub const OBSERVER_RESULT_SCHEMA: &str = "laminardb-instrumentation-ab-observer-dry-run/v1";
pub const DRY_RUN_RECORD_SCHEMA: &str = "laminardb-instrumentation-ab-nonfeedback-result/v1";
pub const FAKE_PROTOCOL_RUN_RECORD_SCHEMA: &str =
    "laminardb-instrumentation-ab-fake-protocol-run/v1";
pub const START_SIGNAL_BYTES: &[u8] = b"LAMINARDB_AB_DRY_RUN_START_V1\n";

const PURPOSE: &str = "engineering_instrumentation_ab_nonfeedback_dry_run";
const MAX_MANIFEST_BYTES: usize = 64 * 1_024;
const WINDOW_END_NS: u64 = 290_000_000_000;
const INPUT_TARGET_END_NS: u64 = 200_000_000_000;
const CHECKPOINT_INTERVAL_NS: u64 = 1_500_000_000;
const POST_RECOVERY_BASE_NS: u64 = 255_000_000_000;
const OBSERVER_INTERVAL_NS: u64 = 5_000_000_000;
const EXPECTED_DRIVER_ACTIONS: u32 = 104;
const EXPECTED_OBSERVER_SLOTS: u32 = 58;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractError(String);

impl ContractError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ContractError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Arm {
    PollingControl,
    PollingTreatment,
}

impl Arm {
    pub const fn observer_mode(self) -> ObserverMode {
        match self {
            Self::PollingControl => ObserverMode::Suppress,
            Self::PollingTreatment => ObserverMode::Poll,
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::PollingControl => "C",
            Self::PollingTreatment => "D",
        }
    }
}

impl FromStr for Arm {
    type Err = ContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "C" | "polling_control" => Ok(Self::PollingControl),
            "D" | "polling_treatment" => Ok(Self::PollingTreatment),
            _ => Err(ContractError::new(
                "arm must be C/polling_control or D/polling_treatment",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserverMode {
    Suppress,
    Poll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObserverBehavior {
    Success,
    Exit,
    Hang,
    Malformed,
}

impl ObserverBehavior {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Exit => "exit",
            Self::Hang => "hang",
            Self::Malformed => "malformed",
        }
    }
}

impl FromStr for ObserverBehavior {
    type Err = ContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "success" => Ok(Self::Success),
            "exit" => Ok(Self::Exit),
            "hang" => Ok(Self::Hang),
            "malformed" => Ok(Self::Malformed),
            _ => Err(ContractError::new(
                "observer behavior must be success, exit, hang, or malformed",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactIdentityV1 {
    pub path: PathBuf,
    pub byte_length: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactSetV1 {
    pub driver: ArtifactIdentityV1,
    pub observer: ArtifactIdentityV1,
    pub server: ArtifactIdentityV1,
    pub trace_manifest: ArtifactIdentityV1,
    pub redacted_config: ArtifactIdentityV1,
    pub dependency_manifest: ArtifactIdentityV1,
    pub virtual_control_script: ArtifactIdentityV1,
    pub protocol_spec: ArtifactIdentityV1,
}

impl ArtifactSetV1 {
    fn entries(&self) -> [(&'static str, &ArtifactIdentityV1); 8] {
        [
            ("driver", &self.driver),
            ("observer", &self.observer),
            ("server", &self.server),
            ("trace_manifest", &self.trace_manifest),
            ("redacted_config", &self.redacted_config),
            ("dependency_manifest", &self.dependency_manifest),
            ("virtual_control_script", &self.virtual_control_script),
            ("protocol_spec", &self.protocol_spec),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadV1 {
    pub record_count: u64,
    pub records_per_second: u32,
    pub input_target_end_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverPolicyV1 {
    pub poll_interval_ns: u64,
    pub local_evidence_max_bytes: u32,
    pub exact_timing_max_bytes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsV1 {
    pub max_driver_actions: u32,
    pub max_observer_slots: u32,
    pub observer_stdout_max_bytes: u32,
    pub observer_stderr_max_bytes: u32,
    pub observer_completion_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PairV1 {
    pub control: ObserverMode,
    pub treatment: ObserverMode,
    pub shared_server: bool,
    pub shared_config: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthenticationV1 {
    SyntheticNone,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestV1 {
    pub schema_version: String,
    pub notice: String,
    pub execution_eligible: bool,
    pub purpose: String,
    pub attempt_id: String,
    pub artifacts: ArtifactSetV1,
    pub node_ordinals: [u8; 3],
    pub workload: WorkloadV1,
    pub observer_policy: ObserverPolicyV1,
    pub limits: LimitsV1,
    pub pair: PairV1,
    pub authentication: AuthenticationV1,
}

impl ManifestV1 {
    fn semantic_errors(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if self.schema_version != MANIFEST_SCHEMA {
            errors.push(format!("schema_version must be {MANIFEST_SCHEMA:?}"));
        }
        if self.notice != NOTICE {
            errors.push(format!("notice must be {NOTICE:?}"));
        }
        if self.execution_eligible {
            errors.push("execution_eligible must remain false".to_owned());
        }
        if self.purpose != PURPOSE {
            errors.push(format!("purpose must be {PURPOSE:?}"));
        }
        if self.attempt_id.is_empty()
            || self.attempt_id.len() > 64
            || !self
                .attempt_id
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            errors.push(
                "attempt_id must contain 1..=64 lowercase ASCII letters, digits, or hyphens"
                    .to_owned(),
            );
        }
        if self.node_ordinals != [0, 1, 2] {
            errors.push("node_ordinals must be exactly [0, 1, 2]".to_owned());
        }
        if self.workload.record_count != 80_000
            || self.workload.records_per_second != 400
            || self.workload.input_target_end_ns != INPUT_TARGET_END_NS
        {
            errors.push(
                "workload must be 80,000 records at 400 records/s before 200,000,000,000ns"
                    .to_owned(),
            );
        }
        let policy = &self.observer_policy;
        if policy.poll_interval_ns != OBSERVER_INTERVAL_NS {
            errors.push("observer poll_interval_ns must be 5,000,000,000".to_owned());
        }
        if policy.local_evidence_max_bytes == 0 || policy.local_evidence_max_bytes > 4 * 1_024 {
            errors.push("local_evidence_max_bytes must be in 1..=4096".to_owned());
        }
        if policy.exact_timing_max_bytes == 0 || policy.exact_timing_max_bytes > 64 * 1_024 {
            errors.push("exact_timing_max_bytes must be in 1..=65536".to_owned());
        }
        if self.limits.max_driver_actions != EXPECTED_DRIVER_ACTIONS {
            errors.push("max_driver_actions must be 104".to_owned());
        }
        if self.limits.max_observer_slots != EXPECTED_OBSERVER_SLOTS {
            errors.push("max_observer_slots must be 58".to_owned());
        }
        if !(256 * 1_024..=1024 * 1_024).contains(&self.limits.observer_stdout_max_bytes) {
            errors.push("observer_stdout_max_bytes must be in 262144..=1048576".to_owned());
        }
        if self.limits.observer_stderr_max_bytes == 0
            || self.limits.observer_stderr_max_bytes > 64 * 1_024
        {
            errors.push("observer_stderr_max_bytes must be in 1..=65536".to_owned());
        }
        if !(100..=60_000).contains(&self.limits.observer_completion_timeout_ms) {
            errors.push("observer_completion_timeout_ms must be in 100..=60000".to_owned());
        }
        if self.pair.control != ObserverMode::Suppress
            || self.pair.treatment != ObserverMode::Poll
            || !self.pair.shared_server
            || !self.pair.shared_config
        {
            errors.push(
                "pair must bind control=suppress, treatment=poll, shared_server=true, and shared_config=true"
                    .to_owned(),
            );
        }
        for (name, artifact) in self.artifacts.entries() {
            if !artifact.path.is_absolute() {
                errors.push(format!("artifacts.{name}.path must be absolute"));
            }
            if artifact.byte_length == 0 {
                errors.push(format!("artifacts.{name}.byte_length must be positive"));
            }
            if !is_lower_sha256(&artifact.sha256) {
                errors.push(format!(
                    "artifacts.{name}.sha256 must be 64 lowercase hexadecimal characters"
                ));
            }
        }
        errors
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedArtifactV1 {
    pub path: PathBuf,
    pub byte_length: u64,
    pub sha256: String,
}

#[derive(Debug, Clone)]
pub struct ValidatedManifestV1 {
    manifest: ManifestV1,
    raw_sha256: String,
    canonical_sha256: String,
    artifacts: ResolvedArtifactSetV1,
}

#[derive(Debug, Clone)]
pub struct ResolvedArtifactSetV1 {
    pub driver: ResolvedArtifactV1,
    pub observer: ResolvedArtifactV1,
    pub server: ResolvedArtifactV1,
    pub trace_manifest: ResolvedArtifactV1,
    pub redacted_config: ResolvedArtifactV1,
    pub dependency_manifest: ResolvedArtifactV1,
    pub virtual_control_script: ResolvedArtifactV1,
    pub protocol_spec: ResolvedArtifactV1,
}

impl ValidatedManifestV1 {
    pub fn manifest(&self) -> &ManifestV1 {
        &self.manifest
    }

    pub fn raw_sha256(&self) -> &str {
        &self.raw_sha256
    }

    pub fn canonical_sha256(&self) -> &str {
        &self.canonical_sha256
    }

    pub fn artifacts(&self) -> &ResolvedArtifactSetV1 {
        &self.artifacts
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactDigestV1 {
    pub byte_length: u64,
    pub sha256: String,
}

impl From<&ResolvedArtifactV1> for ArtifactDigestV1 {
    fn from(artifact: &ResolvedArtifactV1) -> Self {
        Self {
            byte_length: artifact.byte_length,
            sha256: artifact.sha256.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactDigestsV1 {
    pub driver: ArtifactDigestV1,
    pub observer: ArtifactDigestV1,
    pub server: ArtifactDigestV1,
    pub trace_manifest: ArtifactDigestV1,
    pub redacted_config: ArtifactDigestV1,
    pub dependency_manifest: ArtifactDigestV1,
    pub virtual_control_script: ArtifactDigestV1,
    pub protocol_spec: ArtifactDigestV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultContractV1 {
    pub target_rule: String,
    pub gate_timeout_ns: u64,
    pub recovery_timeout_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DriverActionV1 {
    StartWindow {
        at_ns: u64,
    },
    Checkpoint {
        ordinal: u32,
        at_ns: u64,
        fault: Option<FaultContractV1>,
    },
    InputTargetEnd {
        at_ns: u64,
    },
    EndWindow {
        at_ns: u64,
    },
}

impl DriverActionV1 {
    pub const fn at_ns(&self) -> u64 {
        match self {
            Self::StartWindow { at_ns }
            | Self::Checkpoint { at_ns, .. }
            | Self::InputTargetEnd { at_ns }
            | Self::EndWindow { at_ns } => *at_ns,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverScheduleV1 {
    pub actions: Vec<DriverActionV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverSlotV1 {
    pub ordinal: u32,
    pub at_ns: u64,
    pub boundary: Option<LifecycleBoundaryV1>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleBoundaryV1 {
    WindowStart,
    FaultCheckpoint,
    InputTargetEnd,
    PostRecoverySamplingAnchor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverScheduleV1 {
    pub slots: Vec<ObserverSlotV1>,
    pub node_ordinals: [u8; 3],
    pub route_order: [DiagnosticRouteV1; 2],
    pub policy: ObserverPolicyV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticRouteV1 {
    LocalEvidence,
    ExactTiming,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BasePlanV1 {
    pub schema_version: String,
    pub notice: String,
    pub execution_eligible: bool,
    /// Binds every manifest field, including supervisor limits and the C/D mapping.
    pub source_manifest_sha256: String,
    pub artifact_digests: ArtifactDigestsV1,
    pub node_ordinals: [u8; 3],
    pub workload: WorkloadV1,
    pub driver_schedule: DriverScheduleV1,
    pub observer_schedule: ObserverScheduleV1,
}

#[derive(Debug, Clone)]
pub struct SealedPlanV1 {
    plan: BasePlanV1,
    canonical_bytes: Vec<u8>,
    sha256: String,
    driver_schedule_sha256: String,
    observer_schedule_sha256: String,
}

impl SealedPlanV1 {
    pub fn plan(&self) -> &BasePlanV1 {
        &self.plan
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub fn driver_schedule_sha256(&self) -> &str {
        &self.driver_schedule_sha256
    }

    pub fn observer_schedule_sha256(&self) -> &str {
        &self.observer_schedule_sha256
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverTraceV1 {
    pub schema_version: String,
    pub notice: String,
    pub execution_eligible: bool,
    pub actions: Vec<DriverActionV1>,
}

#[derive(Debug)]
pub struct CompletedTraceV1 {
    canonical_bytes: Vec<u8>,
    sha256: String,
    action_count: u32,
    end_ns: u64,
}

impl CompletedTraceV1 {
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub fn action_count(&self) -> u32 {
        self.action_count
    }

    pub fn end_ns(&self) -> u64 {
        self.end_ns
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedProbeV1 {
    pub slot_ordinal: u32,
    pub at_ns: u64,
    pub boundary: Option<LifecycleBoundaryV1>,
    pub node_ordinal: u8,
    pub route: DiagnosticRouteV1,
    pub max_response_bytes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverResultV1 {
    pub schema_version: String,
    pub notice: String,
    pub execution_eligible: bool,
    pub attempt_id: String,
    pub arm: Arm,
    pub mode: ObserverMode,
    pub manifest_sha256: String,
    pub base_plan_sha256: String,
    pub observer_schedule_sha256: String,
    pub scheduled_slots: u32,
    pub suppressed_probes: u32,
    /// Static declarations only. This v1 observer has no network or response parser.
    pub planned_probes: Vec<PlannedProbeV1>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserverDispositionV1 {
    Valid,
    ExitNonzero,
    HungKilled,
    Malformed,
    OutputOversized,
    SpawnFailed,
    StartSignalFailed,
    TerminationFailed,
    CaptureIncomplete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DryRunRecordV1 {
    pub schema_version: String,
    pub notice: String,
    pub execution_eligible: bool,
    pub attempt_id: String,
    pub arm: Arm,
    pub injected_observer_behavior: String,
    pub raw_manifest_sha256: String,
    pub canonical_manifest_sha256: String,
    pub base_plan_sha256: String,
    pub driver_schedule_sha256: String,
    pub observer_schedule_sha256: String,
    pub driver_trace_sha256: String,
    pub driver_sha256: String,
    pub observer_sha256: String,
    pub artifact_digests: ArtifactDigestsV1,
    pub observer_pid: Option<u32>,
    pub action_count: u32,
    pub scheduled_end_ns: u64,
    pub trace_matches_plan: bool,
    pub observer_outcome_consumed_only_after_end: bool,
    pub observer_disposition: ObserverDispositionV1,
    pub observer_stdout_retained_bytes: u32,
    pub observer_stdout_retained_sha256: String,
    pub observer_stderr_retained_bytes: u32,
    pub observer_stderr_retained_sha256: String,
    pub supervisor_events: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FakeProtocolObserverDispositionV1 {
    Complete,
    Incomplete,
    ProvisioningRejected,
    SpawnFailed,
    BootstrapDeliveryFailed,
    ExitNonzero,
    InvalidResult,
    CompletionDeadlineExceeded,
    StatusInspectionFailed,
    OutputOversized,
    CaptureIncomplete,
    TerminationFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FakeProtocolRunRecordV1 {
    pub schema_version: String,
    pub notice: String,
    pub execution_eligible: bool,
    pub attempt_id: String,
    pub arm: Arm,
    pub injected_observer_behavior: String,
    pub raw_manifest_sha256: String,
    pub canonical_manifest_sha256: String,
    pub base_plan_sha256: String,
    pub driver_schedule_sha256: String,
    pub observer_schedule_sha256: String,
    pub sanitized_observer_plan_sha256: Option<String>,
    pub observer_invocation_id: Option<uuid::Uuid>,
    pub driver_trace_sha256: String,
    pub driver_sha256: String,
    pub observer_sha256: String,
    pub artifact_digests: ArtifactDigestsV1,
    pub observer_pid: Option<u32>,
    pub action_count: u32,
    pub scheduled_end_ns: u64,
    pub trace_matches_plan: bool,
    pub observer_outcome_consumed_only_after_end: bool,
    pub observer_disposition: FakeProtocolObserverDispositionV1,
    pub observer_protocol_result: Option<observer_protocol::ObserverProtocolResultV3>,
    pub observer_stdout_retained_bytes: u32,
    pub observer_stdout_retained_sha256: String,
    pub observer_stderr_retained_bytes: u32,
    pub observer_stderr_retained_sha256: String,
    pub raw_observer_output_persisted: bool,
    pub supervisor_events: Vec<String>,
}

pub fn validate_manifest_path(path: &Path) -> Result<ValidatedManifestV1, ContractError> {
    let file = std::fs::File::open(path).map_err(|error| {
        ContractError::new(format!("open manifest {}: {error}", path.display()))
    })?;
    let metadata = file.metadata().map_err(|error| {
        ContractError::new(format!("inspect manifest {}: {error}", path.display()))
    })?;
    if !metadata.is_file() {
        return Err(ContractError::new(format!(
            "manifest is not a regular file: {}",
            path.display()
        )));
    }
    if metadata.len() > MAX_MANIFEST_BYTES as u64 {
        return Err(ContractError::new(format!(
            "manifest is {} bytes; maximum is {MAX_MANIFEST_BYTES}",
            metadata.len()
        )));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_MANIFEST_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            ContractError::new(format!("read manifest {}: {error}", path.display()))
        })?;
    validate_manifest_bytes(&bytes)
}

pub fn validate_manifest_bytes(bytes: &[u8]) -> Result<ValidatedManifestV1, ContractError> {
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(ContractError::new(format!(
            "manifest is {} bytes; maximum is {MAX_MANIFEST_BYTES}",
            bytes.len()
        )));
    }
    let manifest: ManifestV1 = serde_json::from_slice(bytes)
        .map_err(|error| ContractError::new(format!("decode manifest: {error}")))?;
    let errors = manifest.semantic_errors();
    if !errors.is_empty() {
        return Err(ContractError::new(errors.join("; ")));
    }
    let artifacts = verify_artifacts(&manifest.artifacts)?;
    let canonical = serde_json::to_vec(&manifest)
        .map_err(|error| ContractError::new(format!("encode canonical manifest: {error}")))?;
    Ok(ValidatedManifestV1 {
        manifest,
        raw_sha256: sha256_bytes(bytes),
        canonical_sha256: domain_hash(b"laminardb-instrumentation-ab-manifest/v1\0", &canonical),
        artifacts,
    })
}

pub fn verify_current_executable(expected: &ResolvedArtifactV1) -> Result<PathBuf, ContractError> {
    let current = std::env::current_exe()
        .map_err(|error| ContractError::new(format!("resolve current executable: {error}")))?
        .canonicalize()
        .map_err(|error| ContractError::new(format!("canonicalize current executable: {error}")))?;
    if current != expected.path {
        return Err(ContractError::new(format!(
            "current executable {} does not match manifest artifact {}",
            current.display(),
            expected.path.display()
        )));
    }
    Ok(current)
}

pub fn verify_resolved_artifact(
    name: &str,
    expected: &ResolvedArtifactV1,
) -> Result<PathBuf, ContractError> {
    let declared = ArtifactIdentityV1 {
        path: expected.path.clone(),
        byte_length: expected.byte_length,
        sha256: expected.sha256.clone(),
    };
    let observed = verify_artifact(name, &declared)?;
    if &observed != expected {
        return Err(ContractError::new(format!(
            "{name} resolved identity changed before use"
        )));
    }
    Ok(observed.path)
}

pub fn seal_base_plan(manifest: &ValidatedManifestV1) -> Result<SealedPlanV1, ContractError> {
    let artifacts = manifest.artifacts();
    let driver_schedule = build_driver_schedule()?;
    let observer_schedule = build_observer_schedule(manifest.manifest())?;
    let plan = BasePlanV1 {
        schema_version: BASE_PLAN_SCHEMA.to_owned(),
        notice: NOTICE.to_owned(),
        execution_eligible: false,
        source_manifest_sha256: manifest.raw_sha256().to_owned(),
        artifact_digests: ArtifactDigestsV1 {
            driver: (&artifacts.driver).into(),
            observer: (&artifacts.observer).into(),
            server: (&artifacts.server).into(),
            trace_manifest: (&artifacts.trace_manifest).into(),
            redacted_config: (&artifacts.redacted_config).into(),
            dependency_manifest: (&artifacts.dependency_manifest).into(),
            virtual_control_script: (&artifacts.virtual_control_script).into(),
            protocol_spec: (&artifacts.protocol_spec).into(),
        },
        node_ordinals: manifest.manifest().node_ordinals,
        workload: manifest.manifest().workload.clone(),
        driver_schedule,
        observer_schedule,
    };
    let canonical_bytes = serde_json::to_vec(&plan)
        .map_err(|error| ContractError::new(format!("encode base plan: {error}")))?;
    let driver_schedule_bytes = serde_json::to_vec(&plan.driver_schedule)
        .map_err(|error| ContractError::new(format!("encode driver schedule: {error}")))?;
    let observer_schedule_bytes = serde_json::to_vec(&plan.observer_schedule)
        .map_err(|error| ContractError::new(format!("encode observer schedule: {error}")))?;
    let sealed = SealedPlanV1 {
        plan,
        sha256: domain_hash(
            b"laminardb-instrumentation-ab-base-plan/v1\0",
            &canonical_bytes,
        ),
        driver_schedule_sha256: domain_hash(
            b"laminardb-instrumentation-ab-driver-schedule/v1\0",
            &driver_schedule_bytes,
        ),
        observer_schedule_sha256: domain_hash(
            b"laminardb-instrumentation-ab-observer-schedule/v1\0",
            &observer_schedule_bytes,
        ),
        canonical_bytes,
    };
    for arm in [Arm::PollingControl, Arm::PollingTreatment] {
        let result = build_observer_result(manifest, &sealed, arm)?;
        let encoded = serde_json::to_vec(&result)
            .map_err(|error| ContractError::new(format!("encode observer result: {error}")))?;
        if encoded.len() > manifest.manifest().limits.observer_stdout_max_bytes as usize {
            return Err(ContractError::new(format!(
                "official {} observer result is {} bytes; stdout cap is {}",
                arm.label(),
                encoded.len(),
                manifest.manifest().limits.observer_stdout_max_bytes
            )));
        }
    }
    Ok(sealed)
}

pub fn materialize_driver_trace(plan: &SealedPlanV1) -> Result<CompletedTraceV1, ContractError> {
    validate_driver_actions(&plan.plan.driver_schedule.actions)?;
    let trace = DriverTraceV1 {
        schema_version: DRIVER_TRACE_SCHEMA.to_owned(),
        notice: NOTICE.to_owned(),
        execution_eligible: false,
        actions: plan.plan.driver_schedule.actions.clone(),
    };
    let canonical_bytes = serde_json::to_vec(&trace)
        .map_err(|error| ContractError::new(format!("encode driver trace: {error}")))?;
    let action_count = u32::try_from(trace.actions.len())
        .map_err(|_| ContractError::new("driver action count does not fit u32"))?;
    let end_ns = trace
        .actions
        .last()
        .map(DriverActionV1::at_ns)
        .ok_or_else(|| ContractError::new("driver trace is empty"))?;
    Ok(CompletedTraceV1 {
        sha256: domain_hash(
            b"laminardb-instrumentation-ab-driver-trace/v1\0",
            &canonical_bytes,
        ),
        canonical_bytes,
        action_count,
        end_ns,
    })
}

pub fn validate_completed_trace(
    completed: &CompletedTraceV1,
    plan: &SealedPlanV1,
) -> Result<(), ContractError> {
    let expected = DriverTraceV1 {
        schema_version: DRIVER_TRACE_SCHEMA.to_owned(),
        notice: NOTICE.to_owned(),
        execution_eligible: false,
        actions: plan.plan.driver_schedule.actions.clone(),
    };
    let observed: DriverTraceV1 = serde_json::from_slice(&completed.canonical_bytes)
        .map_err(|error| ContractError::new(format!("decode completed driver trace: {error}")))?;
    if observed != expected {
        return Err(ContractError::new(
            "completed driver trace does not exactly match the sealed schedule",
        ));
    }
    let canonical = serde_json::to_vec(&observed)
        .map_err(|error| ContractError::new(format!("re-encode driver trace: {error}")))?;
    if canonical != completed.canonical_bytes {
        return Err(ContractError::new(
            "completed driver trace bytes are not canonical",
        ));
    }
    let action_count = u32::try_from(observed.actions.len())
        .map_err(|_| ContractError::new("driver action count does not fit u32"))?;
    let end_ns = observed
        .actions
        .last()
        .map(DriverActionV1::at_ns)
        .ok_or_else(|| ContractError::new("completed driver trace is empty"))?;
    let sha256 = domain_hash(
        b"laminardb-instrumentation-ab-driver-trace/v1\0",
        &canonical,
    );
    if action_count != completed.action_count
        || end_ns != completed.end_ns
        || sha256 != completed.sha256
    {
        return Err(ContractError::new(
            "completed driver trace metadata does not match its canonical bytes",
        ));
    }
    Ok(())
}

pub fn build_observer_result(
    manifest: &ValidatedManifestV1,
    plan: &SealedPlanV1,
    arm: Arm,
) -> Result<ObserverResultV1, ContractError> {
    let mut planned_probes = Vec::new();
    let total_probes = plan
        .plan
        .observer_schedule
        .slots
        .len()
        .checked_mul(plan.plan.observer_schedule.node_ordinals.len())
        .and_then(|count| count.checked_mul(plan.plan.observer_schedule.route_order.len()))
        .ok_or_else(|| ContractError::new("observer probe count overflow"))?;
    if arm.observer_mode() == ObserverMode::Poll {
        planned_probes.reserve(total_probes);
        let policy = &plan.plan.observer_schedule.policy;
        for slot in &plan.plan.observer_schedule.slots {
            for node_ordinal in plan.plan.observer_schedule.node_ordinals {
                for route in plan.plan.observer_schedule.route_order {
                    let max_response_bytes = match route {
                        DiagnosticRouteV1::LocalEvidence => policy.local_evidence_max_bytes,
                        DiagnosticRouteV1::ExactTiming => policy.exact_timing_max_bytes,
                    };
                    planned_probes.push(PlannedProbeV1 {
                        slot_ordinal: slot.ordinal,
                        at_ns: slot.at_ns,
                        boundary: slot.boundary,
                        node_ordinal,
                        route,
                        max_response_bytes,
                    });
                }
            }
        }
    }
    let scheduled_slots = u32::try_from(plan.plan.observer_schedule.slots.len())
        .map_err(|_| ContractError::new("observer slot count does not fit u32"))?;
    let suppressed_probes = if arm.observer_mode() == ObserverMode::Suppress {
        u32::try_from(total_probes)
            .map_err(|_| ContractError::new("observer probe count does not fit u32"))?
    } else {
        0
    };
    Ok(ObserverResultV1 {
        schema_version: OBSERVER_RESULT_SCHEMA.to_owned(),
        notice: NOTICE.to_owned(),
        execution_eligible: false,
        attempt_id: manifest.manifest().attempt_id.clone(),
        arm,
        mode: arm.observer_mode(),
        manifest_sha256: manifest.raw_sha256().to_owned(),
        base_plan_sha256: plan.sha256().to_owned(),
        observer_schedule_sha256: plan.observer_schedule_sha256().to_owned(),
        scheduled_slots,
        suppressed_probes,
        planned_probes,
    })
}

pub fn validate_observer_result(
    bytes: &[u8],
    manifest: &ValidatedManifestV1,
    plan: &SealedPlanV1,
    arm: Arm,
) -> Result<ObserverResultV1, ContractError> {
    let observed: ObserverResultV1 = serde_json::from_slice(bytes)
        .map_err(|error| ContractError::new(format!("decode observer result: {error}")))?;
    let expected = build_observer_result(manifest, plan, arm)?;
    if observed != expected {
        return Err(ContractError::new(
            "observer result does not match the sealed manifest, arm, and schedule",
        ));
    }
    Ok(observed)
}

fn verify_artifacts(artifacts: &ArtifactSetV1) -> Result<ResolvedArtifactSetV1, ContractError> {
    let mut resolved = Vec::with_capacity(artifacts.entries().len());
    let mut paths = BTreeSet::new();
    for (name, artifact) in artifacts.entries() {
        let value = verify_artifact(name, artifact)?;
        if !paths.insert(value.path.clone()) {
            return Err(ContractError::new(format!(
                "artifact path is reused: {}",
                value.path.display()
            )));
        }
        resolved.push(value);
    }
    if resolved[0].sha256 == resolved[1].sha256 {
        return Err(ContractError::new(
            "driver and observer must be separate executable byte identities",
        ));
    }
    let mut values = resolved.into_iter();
    Ok(ResolvedArtifactSetV1 {
        driver: values.next().expect("fixed driver artifact"),
        observer: values.next().expect("fixed observer artifact"),
        server: values.next().expect("fixed server artifact"),
        trace_manifest: values.next().expect("fixed trace artifact"),
        redacted_config: values.next().expect("fixed config artifact"),
        dependency_manifest: values.next().expect("fixed dependency artifact"),
        virtual_control_script: values.next().expect("fixed virtual script artifact"),
        protocol_spec: values.next().expect("fixed protocol artifact"),
    })
}

fn verify_artifact(
    name: &str,
    artifact: &ArtifactIdentityV1,
) -> Result<ResolvedArtifactV1, ContractError> {
    let path = artifact.path.canonicalize().map_err(|error| {
        ContractError::new(format!(
            "canonicalize artifacts.{name} {}: {error}",
            artifact.path.display()
        ))
    })?;
    let path_metadata = path.metadata().map_err(|error| {
        ContractError::new(format!(
            "inspect artifacts.{name} {}: {error}",
            path.display()
        ))
    })?;
    if !path_metadata.is_file() {
        return Err(ContractError::new(format!(
            "artifacts.{name} is not a regular file: {}",
            path.display()
        )));
    }
    let mut file = std::fs::File::open(&path).map_err(|error| {
        ContractError::new(format!("open artifacts.{name} {}: {error}", path.display()))
    })?;
    let metadata = file.metadata().map_err(|error| {
        ContractError::new(format!(
            "inspect open artifacts.{name} {}: {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(ContractError::new(format!(
            "opened artifacts.{name} is not a regular file: {}",
            path.display()
        )));
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1_024];
    loop {
        let count = file.read(&mut buffer).map_err(|error| {
            ContractError::new(format!("hash artifacts.{name} {}: {error}", path.display()))
        })?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    let actual_sha256 = encode_sha256(hasher.finalize().as_slice());
    if metadata.len() != artifact.byte_length || actual_sha256 != artifact.sha256 {
        return Err(ContractError::new(format!(
            "artifacts.{name} identity mismatch: expected bytes={} sha256={}, got bytes={} sha256={actual_sha256}",
            artifact.byte_length,
            artifact.sha256,
            metadata.len()
        )));
    }
    Ok(ResolvedArtifactV1 {
        path,
        byte_length: metadata.len(),
        sha256: actual_sha256,
    })
}

fn build_driver_schedule() -> Result<DriverScheduleV1, ContractError> {
    let mut actions = Vec::with_capacity(EXPECTED_DRIVER_ACTIONS as usize);
    actions.push(DriverActionV1::StartWindow { at_ns: 0 });
    for ordinal in 1..=80_u32 {
        let at_ns = u64::from(ordinal)
            .checked_mul(CHECKPOINT_INTERVAL_NS)
            .ok_or_else(|| ContractError::new("pre-fault checkpoint time overflow"))?;
        let fault = (ordinal == 80).then(|| FaultContractV1 {
            target_rule: "observed_leader_before_checkpoint_80".to_owned(),
            gate_timeout_ns: 45_000_000_000,
            recovery_timeout_ns: 90_000_000_000,
        });
        actions.push(DriverActionV1::Checkpoint {
            ordinal,
            at_ns,
            fault,
        });
    }
    actions.push(DriverActionV1::InputTargetEnd {
        at_ns: INPUT_TARGET_END_NS,
    });
    for ordinal in 81..=101_u32 {
        let relative = u64::from(ordinal - 80)
            .checked_mul(CHECKPOINT_INTERVAL_NS)
            .ok_or_else(|| ContractError::new("post-recovery checkpoint time overflow"))?;
        let at_ns = POST_RECOVERY_BASE_NS
            .checked_add(relative)
            .ok_or_else(|| ContractError::new("post-recovery checkpoint time overflow"))?;
        actions.push(DriverActionV1::Checkpoint {
            ordinal,
            at_ns,
            fault: None,
        });
    }
    actions.push(DriverActionV1::EndWindow {
        at_ns: WINDOW_END_NS,
    });
    validate_driver_actions(&actions)?;
    Ok(DriverScheduleV1 { actions })
}

fn validate_driver_actions(actions: &[DriverActionV1]) -> Result<(), ContractError> {
    if actions.len() != EXPECTED_DRIVER_ACTIONS as usize {
        return Err(ContractError::new(format!(
            "driver plan has {} actions; expected {EXPECTED_DRIVER_ACTIONS}",
            actions.len()
        )));
    }
    if !matches!(
        actions.first(),
        Some(DriverActionV1::StartWindow { at_ns: 0 })
    ) {
        return Err(ContractError::new("driver plan must start at zero"));
    }
    if !matches!(
        actions.last(),
        Some(DriverActionV1::EndWindow {
            at_ns: WINDOW_END_NS
        })
    ) {
        return Err(ContractError::new(
            "driver plan must end at 290,000,000,000ns",
        ));
    }
    let mut previous = 0;
    let mut checkpoints = 0_u32;
    let mut fault_count = 0_u32;
    for (index, action) in actions.iter().enumerate() {
        let at_ns = action.at_ns();
        if index > 0 && at_ns <= previous {
            return Err(ContractError::new(format!(
                "driver action {index} is not strictly later than its predecessor"
            )));
        }
        if at_ns > WINDOW_END_NS {
            return Err(ContractError::new(format!(
                "driver action {index} occurs after the scheduled end"
            )));
        }
        if let DriverActionV1::Checkpoint { ordinal, fault, .. } = action {
            checkpoints = checkpoints
                .checked_add(1)
                .ok_or_else(|| ContractError::new("checkpoint count overflow"))?;
            if *ordinal != checkpoints {
                return Err(ContractError::new(format!(
                    "checkpoint ordinal {ordinal} is not the expected {checkpoints}"
                )));
            }
            if fault.is_some() {
                fault_count += 1;
                if *ordinal != 80 {
                    return Err(ContractError::new(
                        "only checkpoint ordinal 80 may carry the fault contract",
                    ));
                }
            }
        }
        previous = at_ns;
    }
    if checkpoints != 101 || fault_count != 1 {
        return Err(ContractError::new(format!(
            "driver plan must contain 101 checkpoints and one fault, got {checkpoints} and {fault_count}"
        )));
    }
    Ok(())
}

fn build_observer_schedule(manifest: &ManifestV1) -> Result<ObserverScheduleV1, ContractError> {
    let interval = manifest.observer_policy.poll_interval_ns;
    if !WINDOW_END_NS.is_multiple_of(interval) {
        return Err(ContractError::new(
            "observer interval must divide the scheduled window exactly",
        ));
    }
    // The final probe is one full interval before EndWindow, never on the end boundary.
    let count = WINDOW_END_NS / interval;
    if count != u64::from(EXPECTED_OBSERVER_SLOTS) {
        return Err(ContractError::new(format!(
            "observer schedule has {count} slots; expected {EXPECTED_OBSERVER_SLOTS}"
        )));
    }
    let mut slots = Vec::with_capacity(EXPECTED_OBSERVER_SLOTS as usize);
    for ordinal in 0..EXPECTED_OBSERVER_SLOTS {
        let at_ns = u64::from(ordinal)
            .checked_mul(interval)
            .ok_or_else(|| ContractError::new("observer slot time overflow"))?;
        let boundary = match at_ns {
            0 => Some(LifecycleBoundaryV1::WindowStart),
            120_000_000_000 => Some(LifecycleBoundaryV1::FaultCheckpoint),
            INPUT_TARGET_END_NS => Some(LifecycleBoundaryV1::InputTargetEnd),
            POST_RECOVERY_BASE_NS => Some(LifecycleBoundaryV1::PostRecoverySamplingAnchor),
            _ => None,
        };
        slots.push(ObserverSlotV1 {
            ordinal,
            at_ns,
            boundary,
        });
    }
    Ok(ObserverScheduleV1 {
        slots,
        node_ordinals: manifest.node_ordinals,
        route_order: [
            DiagnosticRouteV1::LocalEvidence,
            DiagnosticRouteV1::ExactTiming,
        ],
        policy: manifest.observer_policy.clone(),
    })
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn sha256_bytes(bytes: &[u8]) -> String {
    encode_sha256(Sha256::digest(bytes).as_slice())
}

fn domain_hash(domain: &[u8], bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(bytes);
    encode_sha256(hasher.finalize().as_slice())
}

fn encode_sha256(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_schedules_have_expected_boundaries() {
        let driver = build_driver_schedule().unwrap();
        assert_eq!(driver.actions.len(), EXPECTED_DRIVER_ACTIONS as usize);
        assert!(matches!(
            &driver.actions[80],
            DriverActionV1::Checkpoint {
                ordinal: 80,
                at_ns: 120_000_000_000,
                fault: Some(_)
            }
        ));
        assert!(matches!(
            &driver.actions[81],
            DriverActionV1::InputTargetEnd {
                at_ns: INPUT_TARGET_END_NS
            }
        ));
        assert!(matches!(
            &driver.actions[82],
            DriverActionV1::Checkpoint {
                ordinal: 81,
                at_ns: 256_500_000_000,
                fault: None
            }
        ));
        assert!(matches!(
            driver.actions.last(),
            Some(DriverActionV1::EndWindow {
                at_ns: WINDOW_END_NS
            })
        ));
    }

    #[test]
    fn arm_parser_accepts_only_the_frozen_pair() {
        assert_eq!(Arm::from_str("C").unwrap(), Arm::PollingControl);
        assert_eq!(Arm::from_str("D").unwrap(), Arm::PollingTreatment);
        assert!(Arm::from_str("A").is_err());
    }
}
