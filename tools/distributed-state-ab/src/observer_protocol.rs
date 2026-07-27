//! Bounded observer protocol exercised only against loopback fake servers in Cycle 65.
//!
//! This module is deliberately separate from LaminarDB's runtime and from the dry-run driver.
//! It proves request construction, authority delivery, response validation, and cursor behavior;
//! it does not authorize a live cluster request or produce A/B evidence.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zeroize::{Zeroize as _, Zeroizing};

use crate::{
    domain_hash, is_lower_sha256, Arm, DiagnosticRouteV1, LifecycleBoundaryV1, ObserverScheduleV1,
    ObserverSlotV1, SealedPlanV1, NOTICE,
};

pub const SANITIZED_PLAN_SCHEMA: &str = "laminardb-observer-sanitized-plan/v2";
pub const PROTOCOL_RESULT_SCHEMA: &str = "laminardb-observer-loopback-fake-protocol/v3";
pub const SUPERVISOR_BOOTSTRAP_PREFIX: &[u8] = b"LAMINARDB_AB_OBSERVER_BOOTSTRAP_V4\n";
pub const SUPERVISOR_CANCEL_BYTES: &[u8] = b"LAMINARDB_AB_OBSERVER_CANCEL_V1\n";
pub const DRIVER_FAKE_INPUT_SCHEMA: &str = "laminardb-observer-driver-fake-input/v1";
pub const DRIVER_FAKE_INPUT_PREFIX: &[u8] = b"LAMINARDB_AB_DRIVER_FAKE_INPUT_V1\n";
pub const PROTOCOL_RESULT_MAX_BYTES: usize = 64 * 1_024;

const MAX_SANITIZED_PLAN_BYTES: usize = 64 * 1_024;
const MAX_DRIVER_ENDPOINT_BYTES: usize = 512;
const DIAGNOSTIC_SECRET_ENCODED_BYTES: usize = 43;
const DIAGNOSTIC_SECRET_DECODED_BYTES: usize = 32;
const LOCAL_EVIDENCE_PATH: &str = "/api/v1/cluster/local-evidence";
const TIMING_PATH: &str = "/api/v1/cluster/local-checkpoint-barrier-timings";
const LOCAL_EVIDENCE_SCHEMA: &str = "laminardb-local-authority-evidence/v1";
const TIMING_SCHEMA: &str = "laminardb-local-checkpoint-barrier-timings/v1";
const PARTITIONING_ABI_VERSION: u16 = 1;
const TIMING_LEDGER_CAPACITY: usize = 1_024;
const MAX_TIMING_PAGE_RECORDS: usize = 64;
const MAX_ASSIGNMENT_IDENTITIES_PER_RUN: usize = 1_024;
const MAX_PROCESS_GENERATIONS_PER_NODE_RUN: usize = EXPECTED_OBSERVER_SLOTS * 2;

const CONNECT_TIMEOUT: Duration = Duration::from_millis(250);
const WRITE_TIMEOUT: Duration = Duration::from_millis(250);
const READ_IDLE_TIMEOUT: Duration = Duration::from_millis(2_250);
const READ_POLL_INTERVAL: Duration = Duration::from_millis(50);
const REQUEST_TOTAL_TIMEOUT: Duration = Duration::from_millis(2_500);
const NODE_SLOT_TOTAL_TIMEOUT: Duration = Duration::from_millis(4_500);
const RETRY_DELAY: Duration = Duration::from_millis(100);
const FAKE_RUN_TOTAL_TIMEOUT: Duration = Duration::from_secs(60);
const HTTP_HEADER_MAX_BYTES: usize = 4 * 1_024;
const MAX_REQUEST_ATTEMPTS: u8 = 2;
const MAX_NODE_SLOT_ATTEMPTS: u8 = 8;
const MAX_TIMING_PAGES_PER_SLOT: u8 = 6;
const MAX_TIMING_BYTES_PER_SLOT: usize = 384 * 1_024;
const MAX_TIMING_RECORDS_PER_SLOT: usize = 384;
const MAX_RETAINED_EVENTS: usize = 32;

const EXPECTED_OBSERVER_SLOTS: usize = 58;
const OBSERVER_INTERVAL_NS: u64 = 5_000_000_000;
const INPUT_TARGET_END_NS: u64 = 200_000_000_000;
const POST_RECOVERY_BASE_NS: u64 = 255_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverEndpointV2 {
    pub node_ordinal: u8,
    pub address: SocketAddr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DriverFakeEndpointInputV1 {
    schema_version: String,
    notice: String,
    execution_eligible: bool,
    endpoints: [ObserverEndpointV2; 3],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverProtocolPolicyV2 {
    pub connect_timeout_ms: u64,
    pub write_timeout_ms: u64,
    pub read_idle_timeout_ms: u64,
    pub request_total_timeout_ms: u64,
    pub node_slot_total_timeout_ms: u64,
    pub retry_delay_ms: u64,
    pub cancellation_poll_ms: u64,
    pub fake_run_total_timeout_ms: u64,
    pub max_request_attempts: u8,
    pub max_node_slot_attempts: u8,
    pub max_timing_pages_per_slot: u8,
    pub max_timing_bytes_per_slot: u32,
    pub max_timing_records_per_slot: u16,
    pub http_header_max_bytes: u32,
    pub max_retained_events: u8,
}

impl ObserverProtocolPolicyV2 {
    pub const FROZEN: Self = Self {
        connect_timeout_ms: 250,
        write_timeout_ms: 250,
        read_idle_timeout_ms: 2_250,
        request_total_timeout_ms: 2_500,
        node_slot_total_timeout_ms: 4_500,
        retry_delay_ms: 100,
        cancellation_poll_ms: 50,
        fake_run_total_timeout_ms: 60_000,
        max_request_attempts: MAX_REQUEST_ATTEMPTS,
        max_node_slot_attempts: MAX_NODE_SLOT_ATTEMPTS,
        max_timing_pages_per_slot: MAX_TIMING_PAGES_PER_SLOT,
        max_timing_bytes_per_slot: MAX_TIMING_BYTES_PER_SLOT as u32,
        max_timing_records_per_slot: MAX_TIMING_RECORDS_PER_SLOT as u16,
        http_header_max_bytes: HTTP_HEADER_MAX_BYTES as u32,
        max_retained_events: MAX_RETAINED_EVENTS as u8,
    };
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SanitizedObserverPlanV2 {
    pub schema_version: String,
    pub notice: String,
    pub execution_eligible: bool,
    pub base_plan_sha256: String,
    pub observer_schedule_sha256: String,
    pub observer_schedule: ObserverScheduleV1,
    pub endpoints: [ObserverEndpointV2; 3],
    pub protocol: ObserverProtocolPolicyV2,
}

#[derive(Debug, Clone)]
pub struct ValidatedSanitizedObserverPlanV2 {
    plan: SanitizedObserverPlanV2,
    canonical_sha256: String,
}

impl ValidatedSanitizedObserverPlanV2 {
    pub fn plan(&self) -> &SanitizedObserverPlanV2 {
        &self.plan
    }

    pub fn canonical_sha256(&self) -> &str {
        &self.canonical_sha256
    }
}

pub fn build_sanitized_observer_plan(
    base_plan: &SealedPlanV1,
    addresses: [SocketAddr; 3],
) -> Result<SanitizedObserverPlanV2, ObserverProtocolError> {
    let schedule = &base_plan.plan().observer_schedule;
    let endpoints = std::array::from_fn(|index| ObserverEndpointV2 {
        node_ordinal: schedule.node_ordinals[index],
        address: addresses[index],
    });
    let plan = SanitizedObserverPlanV2 {
        schema_version: SANITIZED_PLAN_SCHEMA.to_owned(),
        notice: NOTICE.to_owned(),
        execution_eligible: false,
        base_plan_sha256: base_plan.sha256().to_owned(),
        observer_schedule_sha256: base_plan.observer_schedule_sha256().to_owned(),
        observer_schedule: schedule.clone(),
        endpoints,
        protocol: ObserverProtocolPolicyV2::FROZEN,
    };
    validate_sanitized_plan(plan)
}

pub fn validate_sanitized_plan_bytes(
    bytes: &[u8],
) -> Result<ValidatedSanitizedObserverPlanV2, ObserverProtocolError> {
    if bytes.is_empty() || bytes.len() > MAX_SANITIZED_PLAN_BYTES {
        return Err(ObserverProtocolError::new(ProtocolFailureKind::InvalidPlan));
    }
    let plan: SanitizedObserverPlanV2 = serde_json::from_slice(bytes)
        .map_err(|_| ObserverProtocolError::new(ProtocolFailureKind::InvalidPlan))?;
    let canonical = serde_json::to_vec(&plan)
        .map_err(|_| ObserverProtocolError::new(ProtocolFailureKind::InvalidPlan))?;
    if canonical != bytes {
        return Err(ObserverProtocolError::new(ProtocolFailureKind::InvalidPlan));
    }
    validate_sanitized_plan(plan).map(|plan| ValidatedSanitizedObserverPlanV2 {
        plan,
        canonical_sha256: domain_hash(b"laminardb-observer-sanitized-plan/v2\0", &canonical),
    })
}

fn validate_sanitized_plan(
    plan: SanitizedObserverPlanV2,
) -> Result<SanitizedObserverPlanV2, ObserverProtocolError> {
    let invalid = || ObserverProtocolError::new(ProtocolFailureKind::InvalidPlan);
    if plan.schema_version != SANITIZED_PLAN_SCHEMA
        || plan.notice != NOTICE
        || plan.execution_eligible
        || !is_lower_sha256(&plan.base_plan_sha256)
        || !is_lower_sha256(&plan.observer_schedule_sha256)
        || plan.protocol != ObserverProtocolPolicyV2::FROZEN
    {
        return Err(invalid());
    }
    validate_observer_schedule(&plan.observer_schedule)?;
    let schedule_bytes = serde_json::to_vec(&plan.observer_schedule).map_err(|_| invalid())?;
    if domain_hash(
        b"laminardb-instrumentation-ab-observer-schedule/v1\0",
        &schedule_bytes,
    ) != plan.observer_schedule_sha256
    {
        return Err(invalid());
    }
    let mut addresses = std::collections::BTreeSet::new();
    for (index, endpoint) in plan.endpoints.iter().enumerate() {
        if endpoint.node_ordinal != plan.observer_schedule.node_ordinals[index]
            || endpoint.address.port() == 0
            || !endpoint.address.ip().is_loopback()
            || !addresses.insert(endpoint.address)
        {
            return Err(invalid());
        }
    }
    Ok(plan)
}

fn validate_observer_schedule(schedule: &ObserverScheduleV1) -> Result<(), ObserverProtocolError> {
    let invalid = || ObserverProtocolError::new(ProtocolFailureKind::InvalidPlan);
    if schedule.slots.len() != EXPECTED_OBSERVER_SLOTS
        || schedule.node_ordinals != [0, 1, 2]
        || schedule.route_order
            != [
                DiagnosticRouteV1::LocalEvidence,
                DiagnosticRouteV1::ExactTiming,
            ]
        || schedule.policy.poll_interval_ns != OBSERVER_INTERVAL_NS
        || schedule.policy.local_evidence_max_bytes != 4 * 1_024
        || schedule.policy.exact_timing_max_bytes != 64 * 1_024
    {
        return Err(invalid());
    }
    for (index, slot) in schedule.slots.iter().enumerate() {
        let ordinal = u32::try_from(index).map_err(|_| invalid())?;
        let at_ns = u64::from(ordinal)
            .checked_mul(OBSERVER_INTERVAL_NS)
            .ok_or_else(invalid)?;
        let boundary = match at_ns {
            0 => Some(LifecycleBoundaryV1::WindowStart),
            120_000_000_000 => Some(LifecycleBoundaryV1::FaultCheckpoint),
            INPUT_TARGET_END_NS => Some(LifecycleBoundaryV1::InputTargetEnd),
            POST_RECOVERY_BASE_NS => Some(LifecycleBoundaryV1::PostRecoverySamplingAnchor),
            _ => None,
        };
        if slot
            != &(ObserverSlotV1 {
                ordinal,
                at_ns,
                boundary,
            })
        {
            return Err(invalid());
        }
    }
    Ok(())
}

pub struct DiagnosticReadSecret {
    encoded: [u8; DIAGNOSTIC_SECRET_ENCODED_BYTES],
}

impl DiagnosticReadSecret {
    pub fn from_provisioned_bytes(bytes: &[u8]) -> Result<Self, SecretSourceError> {
        if bytes.len() != DIAGNOSTIC_SECRET_ENCODED_BYTES {
            return Err(SecretSourceError::Invalid);
        }
        let mut decoded = [0_u8; DIAGNOSTIC_SECRET_DECODED_BYTES];
        let decoded_len = URL_SAFE_NO_PAD.decode_slice(bytes, &mut decoded);
        let mut canonical = if decoded_len.is_ok() {
            URL_SAFE_NO_PAD.encode(decoded)
        } else {
            String::new()
        };
        let valid =
            decoded_len == Ok(DIAGNOSTIC_SECRET_DECODED_BYTES) && canonical.as_bytes() == bytes;
        decoded.zeroize();
        canonical.zeroize();
        if !valid {
            return Err(SecretSourceError::Invalid);
        }
        let mut encoded = [0_u8; DIAGNOSTIC_SECRET_ENCODED_BYTES];
        encoded.copy_from_slice(bytes);
        Ok(Self { encoded })
    }

    fn encoded(&self) -> &[u8] {
        &self.encoded
    }
}

impl fmt::Debug for DiagnosticReadSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DiagnosticReadSecret([REDACTED])")
    }
}

impl Drop for DiagnosticReadSecret {
    fn drop(&mut self) {
        self.encoded.zeroize();
    }
}

pub struct ProvisionedDriverFakeInput {
    pub addresses: [SocketAddr; 3],
    pub secret: DiagnosticReadSecret,
}

pub struct DriverFakeInputSource<R> {
    reader: Option<R>,
}

impl<R> DriverFakeInputSource<R> {
    pub const fn new(reader: R) -> Self {
        Self {
            reader: Some(reader),
        }
    }
}

impl<R: Read> DriverFakeInputSource<R> {
    pub fn take(&mut self) -> Result<ProvisionedDriverFakeInput, ObserverProtocolError> {
        let mut reader = self.reader.take().ok_or_else(|| {
            ObserverProtocolError::new(ProtocolFailureKind::SecretAlreadyConsumed)
        })?;
        let invalid = || ObserverProtocolError::new(ProtocolFailureKind::InvalidBootstrap);
        let mut prefix = vec![0_u8; DRIVER_FAKE_INPUT_PREFIX.len()];
        if reader.read_exact(&mut prefix).is_err() || prefix != DRIVER_FAKE_INPUT_PREFIX {
            prefix.zeroize();
            return Err(invalid());
        }
        prefix.zeroize();

        let mut length_bytes = [0_u8; 4];
        if reader.read_exact(&mut length_bytes).is_err() {
            return Err(invalid());
        }
        let endpoint_len =
            usize::try_from(u32::from_be_bytes(length_bytes)).map_err(|_| invalid())?;
        if endpoint_len == 0 || endpoint_len > MAX_DRIVER_ENDPOINT_BYTES {
            return Err(invalid());
        }
        let mut endpoint_bytes = vec![0_u8; endpoint_len];
        if reader.read_exact(&mut endpoint_bytes).is_err() {
            endpoint_bytes.zeroize();
            return Err(invalid());
        }
        let mut endpoints: DriverFakeEndpointInputV1 = match serde_json::from_slice(&endpoint_bytes)
        {
            Ok(endpoints) => endpoints,
            Err(_) => {
                endpoint_bytes.zeroize();
                return Err(invalid());
            }
        };
        let mut canonical = match serde_json::to_vec(&endpoints) {
            Ok(canonical) => canonical,
            Err(_) => {
                endpoint_bytes.zeroize();
                return Err(invalid());
            }
        };
        let canonical_input = canonical == endpoint_bytes;
        canonical.zeroize();
        endpoint_bytes.zeroize();
        let valid_endpoints = valid_driver_endpoint_input(&endpoints);
        let addresses = endpoints.endpoints.map(|endpoint| endpoint.address);
        endpoints.schema_version.zeroize();
        endpoints.notice.zeroize();
        if !canonical_input || !valid_endpoints {
            return Err(invalid());
        }

        let mut secret_bytes = [0_u8; DIAGNOSTIC_SECRET_ENCODED_BYTES];
        if reader.read_exact(&mut secret_bytes).is_err() {
            secret_bytes.zeroize();
            return Err(invalid());
        }
        let secret = DiagnosticReadSecret::from_provisioned_bytes(&secret_bytes)
            .map_err(|_| ObserverProtocolError::new(ProtocolFailureKind::InvalidSecret));
        secret_bytes.zeroize();
        let secret = secret?;
        let mut terminator = [0_u8; 1];
        if reader.read_exact(&mut terminator).is_err() || terminator != [b'\n'] {
            return Err(invalid());
        }
        Ok(ProvisionedDriverFakeInput { addresses, secret })
    }
}

pub fn write_driver_fake_input<W: Write>(
    mut writer: W,
    addresses: [SocketAddr; 3],
    secret: &DiagnosticReadSecret,
) -> Result<(), ObserverProtocolError> {
    let endpoints = std::array::from_fn(|index| ObserverEndpointV2 {
        node_ordinal: index as u8,
        address: addresses[index],
    });
    let input = DriverFakeEndpointInputV1 {
        schema_version: DRIVER_FAKE_INPUT_SCHEMA.to_owned(),
        notice: NOTICE.to_owned(),
        execution_eligible: false,
        endpoints,
    };
    if !valid_driver_endpoint_input(&input) {
        return Err(ObserverProtocolError::new(ProtocolFailureKind::InvalidPlan));
    }
    let bytes = serde_json::to_vec(&input)
        .map_err(|_| ObserverProtocolError::new(ProtocolFailureKind::InvalidPlan))?;
    let length = u32::try_from(bytes.len())
        .map_err(|_| ObserverProtocolError::new(ProtocolFailureKind::InvalidPlan))?;
    writer
        .write_all(DRIVER_FAKE_INPUT_PREFIX)
        .and_then(|()| writer.write_all(&length.to_be_bytes()))
        .and_then(|()| writer.write_all(&bytes))
        .and_then(|()| writer.write_all(secret.encoded()))
        .and_then(|()| writer.write_all(b"\n"))
        .and_then(|()| writer.flush())
        .map_err(|_| ObserverProtocolError::new(ProtocolFailureKind::SecretUnavailable))
}

fn valid_driver_endpoint_input(input: &DriverFakeEndpointInputV1) -> bool {
    if input.schema_version != DRIVER_FAKE_INPUT_SCHEMA
        || input.notice != NOTICE
        || input.execution_eligible
    {
        return false;
    }
    let mut addresses = BTreeSet::new();
    input.endpoints.iter().enumerate().all(|(index, endpoint)| {
        endpoint.node_ordinal == index as u8
            && endpoint.address.port() != 0
            && endpoint.address.ip().is_loopback()
            && addresses.insert(endpoint.address)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretSourceError {
    Unavailable,
    Invalid,
    AlreadyConsumed,
}

impl fmt::Display for SecretSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "diagnostic secret source is unavailable",
            Self::Invalid => "diagnostic secret source is invalid",
            Self::AlreadyConsumed => "diagnostic secret source was already consumed",
        })
    }
}

impl std::error::Error for SecretSourceError {}

pub struct SupervisorBootstrapSource<R> {
    reader: Option<R>,
}

impl<R> SupervisorBootstrapSource<R> {
    pub const fn new(reader: R) -> Self {
        Self {
            reader: Some(reader),
        }
    }
}

pub struct ProvisionedObserverInput {
    pub plan: ValidatedSanitizedObserverPlanV2,
    pub invocation_id: Uuid,
    pub secret: DiagnosticReadSecret,
}

impl<R: Read> SupervisorBootstrapSource<R> {
    pub fn take(&mut self) -> Result<ProvisionedObserverInput, ObserverProtocolError> {
        let mut reader = self.reader.take().ok_or_else(|| {
            ObserverProtocolError::new(ProtocolFailureKind::SecretAlreadyConsumed)
        })?;
        let invalid = || ObserverProtocolError::new(ProtocolFailureKind::InvalidBootstrap);
        let mut prefix = vec![0_u8; SUPERVISOR_BOOTSTRAP_PREFIX.len()];
        if reader.read_exact(&mut prefix).is_err() || prefix != SUPERVISOR_BOOTSTRAP_PREFIX {
            prefix.zeroize();
            return Err(invalid());
        }
        prefix.zeroize();

        let mut length_bytes = [0_u8; 4];
        if reader.read_exact(&mut length_bytes).is_err() {
            return Err(invalid());
        }
        let plan_len = usize::try_from(u32::from_be_bytes(length_bytes)).map_err(|_| invalid())?;
        if plan_len == 0 || plan_len > MAX_SANITIZED_PLAN_BYTES {
            return Err(invalid());
        }
        let mut plan_bytes = vec![0_u8; plan_len];
        if reader.read_exact(&mut plan_bytes).is_err() {
            plan_bytes.zeroize();
            return Err(invalid());
        }
        let plan_result = validate_sanitized_plan_bytes(&plan_bytes);
        plan_bytes.zeroize();
        let plan = plan_result?;

        let mut invocation_bytes = [0_u8; 16];
        if reader.read_exact(&mut invocation_bytes).is_err() {
            invocation_bytes.zeroize();
            return Err(invalid());
        }
        let invocation_id = Uuid::from_bytes(invocation_bytes);
        invocation_bytes.zeroize();
        if !is_random_invocation_id(invocation_id) {
            return Err(invalid());
        }

        let mut secret_bytes = [0_u8; DIAGNOSTIC_SECRET_ENCODED_BYTES];
        if reader.read_exact(&mut secret_bytes).is_err() {
            secret_bytes.zeroize();
            return Err(invalid());
        }
        let secret_result = DiagnosticReadSecret::from_provisioned_bytes(&secret_bytes);
        secret_bytes.zeroize();
        let secret = secret_result.map_err(|error| match error {
            SecretSourceError::Unavailable => {
                ObserverProtocolError::new(ProtocolFailureKind::SecretUnavailable)
            }
            SecretSourceError::Invalid => {
                ObserverProtocolError::new(ProtocolFailureKind::InvalidSecret)
            }
            SecretSourceError::AlreadyConsumed => {
                ObserverProtocolError::new(ProtocolFailureKind::SecretAlreadyConsumed)
            }
        })?;
        let mut terminator = [0_u8; 1];
        if reader.read_exact(&mut terminator).is_err() || terminator != [b'\n'] {
            return Err(invalid());
        }
        Ok(ProvisionedObserverInput {
            plan,
            invocation_id,
            secret,
        })
    }
}

pub fn write_supervisor_bootstrap<W: Write>(
    mut writer: W,
    plan: &SanitizedObserverPlanV2,
    invocation_id: Uuid,
    secret: &DiagnosticReadSecret,
) -> Result<(), ObserverProtocolError> {
    let plan = validate_sanitized_plan(plan.clone())?;
    if !is_random_invocation_id(invocation_id) {
        return Err(ObserverProtocolError::new(
            ProtocolFailureKind::InvalidBootstrap,
        ));
    }
    let bytes = serde_json::to_vec(&plan)
        .map_err(|_| ObserverProtocolError::new(ProtocolFailureKind::InvalidPlan))?;
    let length = u32::try_from(bytes.len())
        .map_err(|_| ObserverProtocolError::new(ProtocolFailureKind::InvalidPlan))?;
    writer
        .write_all(SUPERVISOR_BOOTSTRAP_PREFIX)
        .and_then(|()| writer.write_all(&length.to_be_bytes()))
        .and_then(|()| writer.write_all(&bytes))
        .and_then(|()| writer.write_all(invocation_id.as_bytes()))
        .and_then(|()| writer.write_all(secret.encoded()))
        .and_then(|()| writer.write_all(b"\n"))
        .and_then(|()| writer.flush())
        .map_err(|_| ObserverProtocolError::new(ProtocolFailureKind::SecretUnavailable))
}

fn is_random_invocation_id(invocation_id: Uuid) -> bool {
    invocation_id.get_version_num() == 4 && invocation_id.get_variant() == uuid::Variant::RFC4122
}

#[derive(Clone, Default)]
pub struct ProtocolCancellation {
    cancelled: Arc<AtomicBool>,
}

impl ProtocolCancellation {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

impl fmt::Debug for ProtocolCancellation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProtocolCancellation")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolEventKindV1 {
    TransportUnavailable,
    ServerBusy,
    ProcessChanged,
    NodeSlotBudgetExhausted,
    TimingPageBudgetExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverProtocolEventV1 {
    pub slot_ordinal: u32,
    pub node_ordinal: u8,
    pub route: DiagnosticRouteV1,
    pub kind: ProtocolEventKindV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverProtocolResultV3 {
    pub schema_version: String,
    pub notice: String,
    pub execution_eligible: bool,
    pub arm: Arm,
    pub invocation_id: Uuid,
    pub sanitized_plan_sha256: String,
    pub base_plan_sha256: String,
    pub observer_schedule_sha256: String,
    pub scheduled_slots: u32,
    pub suppressed_probes: u32,
    pub connection_attempts: u32,
    pub parsed_responses: u32,
    pub retries: u32,
    pub transient_failures: u32,
    pub process_transitions: u32,
    pub timing_records: u64,
    pub page_budget_deferrals: u32,
    pub unresolved_timing_nodes: u8,
    pub retained_events_dropped: u32,
    pub disposition: ProtocolDispositionV2,
    pub retained_events: Vec<ObserverProtocolEventV1>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolDispositionV2 {
    Complete,
    Incomplete,
}

#[derive(Debug, Clone)]
pub struct ObserverProtocolResultExpectationV3 {
    plan: ValidatedSanitizedObserverPlanV2,
    arm: Arm,
    invocation_id: Uuid,
}

impl ObserverProtocolResultExpectationV3 {
    pub fn sanitized_plan_sha256(&self) -> &str {
        self.plan.canonical_sha256()
    }

    pub fn invocation_id(&self) -> Uuid {
        self.invocation_id
    }
}

pub fn bind_observer_protocol_plan(
    plan: &SanitizedObserverPlanV2,
    base_plan: &SealedPlanV1,
    arm: Arm,
    invocation_id: Uuid,
) -> Result<ObserverProtocolResultExpectationV3, ObserverProtocolError> {
    if !is_random_invocation_id(invocation_id) {
        return Err(ObserverProtocolError::new(ProtocolFailureKind::InvalidPlan));
    }
    let bytes = serde_json::to_vec(plan)
        .map_err(|_| ObserverProtocolError::new(ProtocolFailureKind::InvalidPlan))?;
    let plan = validate_sanitized_plan_bytes(&bytes)?;
    validate_sanitized_plan_binding(&plan, base_plan)?;
    Ok(ObserverProtocolResultExpectationV3 {
        plan,
        arm,
        invocation_id,
    })
}

pub fn validate_sanitized_plan_binding(
    plan: &ValidatedSanitizedObserverPlanV2,
    base_plan: &SealedPlanV1,
) -> Result<(), ObserverProtocolError> {
    let addresses = plan.plan().endpoints.map(|endpoint| endpoint.address);
    let expected = build_sanitized_observer_plan(base_plan, addresses)?;
    if plan.plan() != &expected {
        return Err(ObserverProtocolError::new(ProtocolFailureKind::InvalidPlan));
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ObserverProtocolResultWire<'a> {
    #[serde(borrow)]
    schema_version: &'a str,
    #[serde(borrow)]
    notice: &'a str,
    execution_eligible: bool,
    arm: Arm,
    invocation_id: Uuid,
    #[serde(borrow)]
    sanitized_plan_sha256: &'a str,
    #[serde(borrow)]
    base_plan_sha256: &'a str,
    #[serde(borrow)]
    observer_schedule_sha256: &'a str,
    scheduled_slots: u32,
    suppressed_probes: u32,
    connection_attempts: u32,
    parsed_responses: u32,
    retries: u32,
    transient_failures: u32,
    process_transitions: u32,
    timing_records: u64,
    page_budget_deferrals: u32,
    unresolved_timing_nodes: u8,
    retained_events_dropped: u32,
    disposition: ProtocolDispositionV2,
    retained_events: Vec<ObserverProtocolEventV1>,
}

pub fn validate_observer_protocol_result(
    bytes: &[u8],
    expectation: &ObserverProtocolResultExpectationV3,
) -> Result<ObserverProtocolResultV3, ObserverProtocolError> {
    let invalid = || ObserverProtocolError::new(ProtocolFailureKind::InvalidResult);
    if bytes.is_empty() || bytes.len() > PROTOCOL_RESULT_MAX_BYTES {
        return Err(invalid());
    }
    let observed: ObserverProtocolResultWire<'_> =
        serde_json::from_slice(bytes).map_err(|_| invalid())?;
    let plan = expectation.plan.plan();
    let scheduled_slots =
        u32::try_from(plan.observer_schedule.slots.len()).map_err(|_| invalid())?;
    let node_slots = scheduled_slots.checked_mul(3).ok_or_else(invalid)?;
    let max_connection_attempts = node_slots
        .checked_mul(u32::from(MAX_NODE_SLOT_ATTEMPTS))
        .ok_or_else(invalid)?;
    let max_process_transitions = scheduled_slots
        .checked_mul(2)
        .and_then(|per_node| per_node.checked_sub(1))
        .and_then(|per_node| per_node.checked_mul(3))
        .ok_or_else(invalid)?;
    let max_timing_records = u64::from(node_slots)
        .checked_mul(MAX_TIMING_RECORDS_PER_SLOT as u64)
        .ok_or_else(invalid)?;
    let retained_total = u32::try_from(observed.retained_events.len())
        .map_err(|_| invalid())?
        .checked_add(observed.retained_events_dropped)
        .ok_or_else(invalid)?;
    let event_total = observed
        .transient_failures
        .checked_add(observed.process_transitions)
        .and_then(|count| count.checked_add(observed.page_budget_deferrals))
        .ok_or_else(invalid)?;
    let terminal_node_slot_events = observed
        .transient_failures
        .checked_add(observed.page_budget_deferrals)
        .ok_or_else(invalid)?;
    let disposition_consistent = match observed.disposition {
        ProtocolDispositionV2::Complete => {
            observed.transient_failures == 0 && observed.unresolved_timing_nodes == 0
        }
        ProtocolDispositionV2::Incomplete => {
            observed.transient_failures != 0 || observed.unresolved_timing_nodes != 0
        }
    };
    let retries_consistent = observed
        .retries
        .checked_mul(2)
        .is_some_and(|attempts| attempts <= observed.connection_attempts);
    let timing_records_consistent = u64::from(observed.parsed_responses)
        .checked_mul(MAX_TIMING_PAGE_RECORDS as u64)
        .is_some_and(|records| observed.timing_records <= records);
    let page_deferrals_consistent = observed
        .page_budget_deferrals
        .checked_mul(u32::from(MAX_TIMING_PAGES_PER_SLOT))
        .is_some_and(|pages| pages <= observed.parsed_responses)
        && u64::from(observed.page_budget_deferrals)
            .checked_mul(MAX_TIMING_RECORDS_PER_SLOT as u64)
            .is_some_and(|records| records <= observed.timing_records);
    let logical_probes = observed
        .connection_attempts
        .checked_sub(observed.retries)
        .ok_or_else(invalid)?;
    let logical_probes_have_outcomes = observed
        .parsed_responses
        .checked_add(observed.transient_failures)
        .is_some_and(|outcomes| logical_probes <= outcomes);
    let minimum_logical_probes = node_slots
        .checked_mul(2)
        .and_then(|baseline| baseline.checked_sub(observed.transient_failures))
        .and_then(|baseline| {
            observed
                .page_budget_deferrals
                .checked_mul(u32::from(MAX_TIMING_PAGES_PER_SLOT - 1))
                .and_then(|extra| baseline.checked_add(extra))
        })
        .ok_or_else(invalid)?;
    let minimum_parsed_responses = node_slots
        .checked_mul(2)
        .and_then(|baseline| {
            observed
                .transient_failures
                .checked_mul(2)
                .and_then(|missing| baseline.checked_sub(missing))
        })
        .and_then(|baseline| {
            observed
                .page_budget_deferrals
                .checked_mul(u32::from(MAX_TIMING_PAGES_PER_SLOT - 1))
                .and_then(|extra| baseline.checked_add(extra))
        })
        .ok_or_else(invalid)?;
    let minimum_parsed_for_timing = observed
        .timing_records
        .div_ceil(MAX_TIMING_PAGE_RECORDS as u64)
        .checked_add(
            observed
                .timing_records
                .div_ceil(MAX_TIMING_RECORDS_PER_SLOT as u64),
        )
        .ok_or_else(invalid)?;
    let minimum_logical_for_timing = u64::from(node_slots)
        .checked_add(
            observed
                .timing_records
                .div_ceil(MAX_TIMING_PAGE_RECORDS as u64),
        )
        .ok_or_else(invalid)?;
    let maximum_timing_after_transients = u64::from(node_slots)
        .checked_mul(MAX_TIMING_RECORDS_PER_SLOT as u64)
        .and_then(|maximum| {
            u64::from(observed.transient_failures)
                .checked_mul(MAX_TIMING_PAGE_RECORDS as u64)
                .and_then(|unavailable| maximum.checked_sub(unavailable))
        })
        .ok_or_else(invalid)?;
    let control_consistent = if expectation.arm == Arm::PollingControl {
        observed.suppressed_probes == scheduled_slots * 6
            && observed.connection_attempts == 0
            && observed.parsed_responses == 0
            && observed.retries == 0
            && observed.transient_failures == 0
            && observed.process_transitions == 0
            && observed.timing_records == 0
            && observed.page_budget_deferrals == 0
            && observed.unresolved_timing_nodes == 0
            && observed.retained_events_dropped == 0
            && observed.retained_events.is_empty()
            && observed.disposition == ProtocolDispositionV2::Complete
    } else {
        observed.suppressed_probes == 0
            && logical_probes >= minimum_logical_probes
            && logical_probes_have_outcomes
            && observed.parsed_responses >= minimum_parsed_responses
            && u64::from(observed.parsed_responses) >= minimum_parsed_for_timing
            && u64::from(logical_probes) >= minimum_logical_for_timing
            && observed.timing_records <= maximum_timing_after_transients
    };
    let retained_transient_events = observed
        .retained_events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                ProtocolEventKindV1::TransportUnavailable
                    | ProtocolEventKindV1::ServerBusy
                    | ProtocolEventKindV1::NodeSlotBudgetExhausted
            )
        })
        .count();
    let retained_process_events = observed
        .retained_events
        .iter()
        .filter(|event| event.kind == ProtocolEventKindV1::ProcessChanged)
        .count();
    let retained_page_events = observed
        .retained_events
        .iter()
        .filter(|event| event.kind == ProtocolEventKindV1::TimingPageBudgetExhausted)
        .count();
    let mut retained_node_slots = BTreeMap::<(u32, u8), (u8, bool)>::new();
    let mut retained_node_slot_shapes_valid = true;
    for event in &observed.retained_events {
        let state = retained_node_slots
            .entry((event.slot_ordinal, event.node_ordinal))
            .or_default();
        if event.kind == ProtocolEventKindV1::ProcessChanged {
            if state.1 || state.0 == 2 {
                retained_node_slot_shapes_valid = false;
            }
            state.0 = state.0.saturating_add(1);
        } else {
            if state.1
                || (event.kind == ProtocolEventKindV1::TimingPageBudgetExhausted && state.0 > 1)
            {
                retained_node_slot_shapes_valid = false;
            }
            state.1 = true;
        }
    }
    let events_valid = observed.retained_events.len() <= MAX_RETAINED_EVENTS
        && (observed.retained_events_dropped == 0
            || observed.retained_events.len() == MAX_RETAINED_EVENTS)
        && retained_transient_events <= observed.transient_failures as usize
        && retained_process_events <= observed.process_transitions as usize
        && retained_page_events <= observed.page_budget_deferrals as usize
        && retained_node_slot_shapes_valid
        && observed.retained_events.iter().all(|event| {
            (event.slot_ordinal as usize) < plan.observer_schedule.slots.len()
                && plan
                    .observer_schedule
                    .node_ordinals
                    .contains(&event.node_ordinal)
                && (!matches!(event.kind, ProtocolEventKindV1::ProcessChanged)
                    || event.route == DiagnosticRouteV1::LocalEvidence)
                && (!matches!(event.kind, ProtocolEventKindV1::TimingPageBudgetExhausted)
                    || event.route == DiagnosticRouteV1::ExactTiming)
        })
        && observed.retained_events.windows(2).all(|events| {
            (events[0].slot_ordinal, events[0].node_ordinal)
                <= (events[1].slot_ordinal, events[1].node_ordinal)
        });
    if observed.schema_version != PROTOCOL_RESULT_SCHEMA
        || observed.notice != NOTICE
        || observed.execution_eligible
        || observed.arm != expectation.arm
        || observed.invocation_id != expectation.invocation_id
        || observed.sanitized_plan_sha256 != expectation.plan.canonical_sha256()
        || observed.base_plan_sha256 != plan.base_plan_sha256
        || observed.observer_schedule_sha256 != plan.observer_schedule_sha256
        || observed.scheduled_slots != scheduled_slots
        || observed.connection_attempts > max_connection_attempts
        || observed.parsed_responses > observed.connection_attempts
        || !retries_consistent
        || observed.transient_failures > node_slots
        || observed.transient_failures > observed.connection_attempts
        || terminal_node_slot_events > node_slots
        || observed.process_transitions > max_process_transitions
        || observed
            .process_transitions
            .checked_add(observed.page_budget_deferrals)
            .is_none_or(|events| events > max_process_transitions)
        || observed.process_transitions > observed.parsed_responses
        || observed.timing_records > max_timing_records
        || !timing_records_consistent
        || observed.page_budget_deferrals > node_slots
        || !page_deferrals_consistent
        || observed.unresolved_timing_nodes > 3
        || u32::from(observed.unresolved_timing_nodes) > terminal_node_slot_events
        || retained_total != event_total
        || !disposition_consistent
        || !control_consistent
        || !events_valid
    {
        return Err(invalid());
    }
    let result = ObserverProtocolResultV3 {
        schema_version: PROTOCOL_RESULT_SCHEMA.to_owned(),
        notice: NOTICE.to_owned(),
        execution_eligible: false,
        arm: observed.arm,
        invocation_id: observed.invocation_id,
        sanitized_plan_sha256: expectation.plan.canonical_sha256().to_owned(),
        base_plan_sha256: plan.base_plan_sha256.clone(),
        observer_schedule_sha256: plan.observer_schedule_sha256.clone(),
        scheduled_slots: observed.scheduled_slots,
        suppressed_probes: observed.suppressed_probes,
        connection_attempts: observed.connection_attempts,
        parsed_responses: observed.parsed_responses,
        retries: observed.retries,
        transient_failures: observed.transient_failures,
        process_transitions: observed.process_transitions,
        timing_records: observed.timing_records,
        page_budget_deferrals: observed.page_budget_deferrals,
        unresolved_timing_nodes: observed.unresolved_timing_nodes,
        retained_events_dropped: observed.retained_events_dropped,
        disposition: observed.disposition,
        retained_events: observed.retained_events,
    };
    let canonical = serde_json::to_vec(&result).map_err(|_| invalid())?;
    if canonical != bytes {
        return Err(invalid());
    }
    Ok(result)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolFailureKind {
    InvalidPlan,
    InvalidBootstrap,
    SecretUnavailable,
    InvalidSecret,
    SecretAlreadyConsumed,
    Cancelled,
    TotalDeadlineExceeded,
    InvalidHttpResponse,
    ResponseTooLarge,
    RedirectRejected,
    AuthorizationRejected,
    UnexpectedStatus,
    InvalidLocalEvidence,
    InvalidTimingPage,
    ProcessIdentityConflict,
    CursorConflict,
    EvidenceEvicted,
    EvidenceLoss,
    InvalidResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObserverProtocolError {
    pub kind: ProtocolFailureKind,
    pub slot_ordinal: Option<u32>,
    pub node_ordinal: Option<u8>,
    pub route: Option<DiagnosticRouteV1>,
}

impl ObserverProtocolError {
    fn new(kind: ProtocolFailureKind) -> Self {
        Self {
            kind,
            slot_ordinal: None,
            node_ordinal: None,
            route: None,
        }
    }

    fn at(
        kind: ProtocolFailureKind,
        slot: &ObserverSlotV1,
        node_ordinal: u8,
        route: DiagnosticRouteV1,
    ) -> Self {
        Self {
            kind,
            slot_ordinal: Some(slot.ordinal),
            node_ordinal: Some(node_ordinal),
            route: Some(route),
        }
    }
}

impl fmt::Display for ObserverProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "observer protocol failed: {:?}", self.kind)?;
        if let Some(slot) = self.slot_ordinal {
            write!(formatter, " at slot {slot}")?;
        }
        if let Some(node) = self.node_ordinal {
            write!(formatter, " node {node}")?;
        }
        if let Some(route) = self.route {
            write!(formatter, " route {route:?}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ObserverProtocolError {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalEvidenceEnvelope {
    schema_version: String,
    evidence: LocalEvidenceWire,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalEvidenceWire {
    participant: ParticipantWire,
    process_term: u64,
    adopted_assignment: AssignmentAdoptionWire,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ParticipantWire {
    node_id: u64,
    boot_incarnation: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AssignmentAdoptionWire {
    participant: ParticipantWire,
    assignment_version: u64,
    partitioning_abi_version: u16,
    vnode_count: u32,
    assignment_digest: [u8; 32],
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimingEnvelopeWire {
    schema_version: String,
    process_identity: ProcessIdentityWire,
    after_sequence: u64,
    page: TimingPageWire,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessIdentityWire {
    participant: ParticipantWire,
    process_term: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimingPageWire {
    capacity: usize,
    oldest_retained_sequence: Option<u64>,
    next_sequence: u64,
    overwritten_record_count: u64,
    recording_loss_count: u64,
    metadata_exhausted: bool,
    has_more: bool,
    records: Vec<TimingRecordWire>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TimingRoleWire {
    Leader,
    Follower,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimingRecordWire {
    sequence: u64,
    process: ProcessIdentityWire,
    attempt: CheckpointAttemptWire,
    role: TimingRoleWire,
    assignment_version: u64,
    assignment_digest: [u8; 32],
    pipeline_stall_ns: u64,
    local_barrier_ns: u64,
    aligned_resume_ns: Option<u64>,
    durable_tail_handoff: bool,
    deadline_exhausted: bool,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointAttemptWire {
    epoch: u64,
    checkpoint_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcessIdentity {
    node_id: u64,
    boot_incarnation: Uuid,
    process_term: u64,
}

impl ProcessIdentity {
    fn from_wire(wire: &ProcessIdentityWire) -> Result<Self, ProtocolFailureKind> {
        Self::from_parts(&wire.participant, wire.process_term)
    }

    fn from_parts(
        participant: &ParticipantWire,
        process_term: u64,
    ) -> Result<Self, ProtocolFailureKind> {
        let boot_incarnation = Uuid::parse_str(&participant.boot_incarnation)
            .map_err(|_| ProtocolFailureKind::ProcessIdentityConflict)?;
        if participant.node_id == 0
            || boot_incarnation.is_nil()
            || boot_incarnation.to_string() != participant.boot_incarnation
            || process_term == 0
        {
            return Err(ProtocolFailureKind::ProcessIdentityConflict);
        }
        Ok(Self {
            node_id: participant.node_id,
            boot_incarnation,
            process_term,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuthorityState {
    process: ProcessIdentity,
    assignment_version: u64,
    vnode_count: u32,
    owner_map_digest: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimingMetadata {
    capacity: usize,
    oldest_retained_sequence: Option<u64>,
    next_sequence: u64,
    overwritten_record_count: u64,
    recording_loss_count: u64,
    metadata_exhausted: bool,
}

#[derive(Default)]
struct TimingState {
    cursor: u64,
    metadata: Option<TimingMetadata>,
    assignment_floor: Option<u64>,
    last_assignment_version: Option<u64>,
}

impl TimingState {
    fn reset(&mut self) {
        *self = Self::default();
    }

    fn has_unread_records(&self) -> bool {
        self.metadata
            .is_some_and(|metadata| self.cursor < metadata.next_sequence - 1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OwnerMapIdentity {
    vnode_count: u32,
    digest: [u8; 32],
}

#[derive(Default)]
struct RunAssignmentHistory {
    owner_map_identities: BTreeMap<u64, OwnerMapIdentity>,
    timing_certificate_digests: BTreeMap<u64, [u8; 32]>,
}

impl RunAssignmentHistory {
    fn observe_owner_map(
        &mut self,
        version: u64,
        identity: OwnerMapIdentity,
    ) -> Result<(), ProtocolFailureKind> {
        if self
            .owner_map_identities
            .get(&version)
            .is_some_and(|observed| observed != &identity)
        {
            return Err(ProtocolFailureKind::InvalidLocalEvidence);
        }
        if !self.owner_map_identities.contains_key(&version)
            && self.owner_map_identities.len() == MAX_ASSIGNMENT_IDENTITIES_PER_RUN
        {
            return Err(ProtocolFailureKind::EvidenceLoss);
        }
        self.owner_map_identities.entry(version).or_insert(identity);
        Ok(())
    }

    fn observe_timing_certificate(
        &mut self,
        version: u64,
        digest: [u8; 32],
    ) -> Result<(), ProtocolFailureKind> {
        if self
            .timing_certificate_digests
            .get(&version)
            .is_some_and(|observed| observed != &digest)
        {
            return Err(ProtocolFailureKind::InvalidTimingPage);
        }
        if !self.timing_certificate_digests.contains_key(&version)
            && self.timing_certificate_digests.len() == MAX_ASSIGNMENT_IDENTITIES_PER_RUN
        {
            return Err(ProtocolFailureKind::EvidenceLoss);
        }
        self.timing_certificate_digests
            .entry(version)
            .or_insert(digest);
        Ok(())
    }
}

#[derive(Default)]
struct NodeProtocolState {
    stable_node_id: Option<u64>,
    authority: Option<AuthorityState>,
    seen_boot_incarnations: BTreeSet<Uuid>,
    greatest_observed_assignment_version: Option<u64>,
    greatest_observed_checkpoint_id: Option<u64>,
    timing: TimingState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthorityTransition {
    Initial,
    Stable,
    Restarted,
}

impl NodeProtocolState {
    fn apply_local_evidence(
        &mut self,
        envelope: LocalEvidenceEnvelope,
        assignment_history: &mut RunAssignmentHistory,
    ) -> Result<AuthorityTransition, ProtocolFailureKind> {
        if envelope.schema_version != LOCAL_EVIDENCE_SCHEMA {
            return Err(ProtocolFailureKind::InvalidLocalEvidence);
        }
        let process = ProcessIdentity::from_parts(
            &envelope.evidence.participant,
            envelope.evidence.process_term,
        )?;
        let adoption = envelope.evidence.adopted_assignment;
        let adoption_process =
            ProcessIdentity::from_parts(&adoption.participant, envelope.evidence.process_term)?;
        if adoption_process != process
            || adoption.assignment_version == 0
            || adoption.partitioning_abi_version != PARTITIONING_ABI_VERSION
            || adoption.vnode_count == 0
            || adoption.vnode_count > u32::from(u16::MAX)
            || adoption.assignment_digest == [0; 32]
        {
            return Err(ProtocolFailureKind::InvalidLocalEvidence);
        }
        if self
            .stable_node_id
            .is_some_and(|node_id| node_id != process.node_id)
        {
            return Err(ProtocolFailureKind::ProcessIdentityConflict);
        }
        let next = AuthorityState {
            process,
            assignment_version: adoption.assignment_version,
            vnode_count: adoption.vnode_count,
            owner_map_digest: adoption.assignment_digest,
        };
        if self
            .greatest_observed_assignment_version
            .is_some_and(|version| next.assignment_version < version)
        {
            return Err(ProtocolFailureKind::InvalidLocalEvidence);
        }
        let transition = match self.authority.as_ref() {
            None => AuthorityTransition::Initial,
            Some(previous) if previous.process == process => {
                if next.assignment_version < previous.assignment_version
                    || (next.assignment_version == previous.assignment_version
                        && (next.vnode_count != previous.vnode_count
                            || next.owner_map_digest != previous.owner_map_digest))
                {
                    return Err(ProtocolFailureKind::InvalidLocalEvidence);
                }
                AuthorityTransition::Stable
            }
            Some(previous) => {
                if self
                    .seen_boot_incarnations
                    .contains(&process.boot_incarnation)
                    || process.process_term <= previous.process.process_term
                {
                    return Err(ProtocolFailureKind::ProcessIdentityConflict);
                }
                if self
                    .greatest_observed_assignment_version
                    .is_some_and(|version| next.assignment_version <= version)
                {
                    return Err(ProtocolFailureKind::InvalidLocalEvidence);
                }
                if self.timing.has_unread_records() {
                    return Err(ProtocolFailureKind::EvidenceLoss);
                }
                AuthorityTransition::Restarted
            }
        };
        let new_boot = !self
            .seen_boot_incarnations
            .contains(&process.boot_incarnation);
        if new_boot && self.seen_boot_incarnations.len() == MAX_PROCESS_GENERATIONS_PER_NODE_RUN {
            return Err(ProtocolFailureKind::EvidenceLoss);
        }
        assignment_history.observe_owner_map(
            next.assignment_version,
            OwnerMapIdentity {
                vnode_count: next.vnode_count,
                digest: next.owner_map_digest,
            },
        )?;
        if transition == AuthorityTransition::Restarted {
            let assignment_floor = self
                .greatest_observed_assignment_version
                .and_then(|version| version.checked_add(1))
                .ok_or(ProtocolFailureKind::EvidenceLoss)?;
            self.timing.reset();
            self.timing.assignment_floor = Some(assignment_floor);
        }
        self.seen_boot_incarnations.insert(process.boot_incarnation);
        self.greatest_observed_assignment_version = Some(
            self.greatest_observed_assignment_version
                .map_or(next.assignment_version, |version| {
                    version.max(next.assignment_version)
                }),
        );
        self.stable_node_id.get_or_insert(process.node_id);
        self.authority = Some(next);
        Ok(transition)
    }

    fn apply_timing_page(
        &mut self,
        envelope: TimingEnvelopeWire,
        assignment_history: &mut RunAssignmentHistory,
    ) -> Result<(bool, usize), ProtocolFailureKind> {
        if envelope.schema_version != TIMING_SCHEMA {
            return Err(ProtocolFailureKind::InvalidTimingPage);
        }
        let authority = self
            .authority
            .as_ref()
            .ok_or(ProtocolFailureKind::ProcessIdentityConflict)?;
        let process = ProcessIdentity::from_wire(&envelope.process_identity)?;
        if process != authority.process {
            return Err(ProtocolFailureKind::ProcessIdentityConflict);
        }
        if envelope.after_sequence != self.timing.cursor {
            return Err(ProtocolFailureKind::CursorConflict);
        }
        let page = envelope.page;
        if page.capacity != TIMING_LEDGER_CAPACITY
            || page.records.len() > MAX_TIMING_PAGE_RECORDS
            || (page.has_more && page.records.len() != MAX_TIMING_PAGE_RECORDS)
            || page.recording_loss_count != 0
            || page.metadata_exhausted
            || page.next_sequence == 0
        {
            return Err(ProtocolFailureKind::EvidenceLoss);
        }
        let accepted = page.next_sequence - 1;
        let capacity =
            u64::try_from(page.capacity).map_err(|_| ProtocolFailureKind::InvalidTimingPage)?;
        let expected_overwritten = accepted.saturating_sub(capacity);
        let expected_oldest = (accepted != 0).then_some(expected_overwritten + 1);
        if page.overwritten_record_count != expected_overwritten
            || page.oldest_retained_sequence != expected_oldest
        {
            return Err(ProtocolFailureKind::InvalidTimingPage);
        }
        if page
            .oldest_retained_sequence
            .is_some_and(|oldest| self.timing.cursor.saturating_add(1) < oldest)
        {
            return Err(ProtocolFailureKind::EvidenceEvicted);
        }
        let metadata = TimingMetadata {
            capacity: page.capacity,
            oldest_retained_sequence: page.oldest_retained_sequence,
            next_sequence: page.next_sequence,
            overwritten_record_count: page.overwritten_record_count,
            recording_loss_count: page.recording_loss_count,
            metadata_exhausted: page.metadata_exhausted,
        };
        if self.timing.metadata.is_some_and(|previous| {
            metadata.capacity != previous.capacity
                || metadata.next_sequence < previous.next_sequence
                || metadata.overwritten_record_count < previous.overwritten_record_count
                || metadata.recording_loss_count < previous.recording_loss_count
                || (previous.metadata_exhausted && !metadata.metadata_exhausted)
        }) {
            return Err(ProtocolFailureKind::InvalidTimingPage);
        }

        let record_count = page.records.len();
        for record in page.records {
            let expected_sequence = self
                .timing
                .cursor
                .checked_add(1)
                .ok_or(ProtocolFailureKind::InvalidTimingPage)?;
            let record_process = ProcessIdentity::from_wire(&record.process)?;
            if record.sequence != expected_sequence
                || record_process != process
                || record.attempt.epoch == 0
                || record.attempt.epoch != record.attempt.checkpoint_id
                || self
                    .greatest_observed_checkpoint_id
                    .is_some_and(|previous| record.attempt.checkpoint_id <= previous)
                || record.assignment_version == 0
                || record.assignment_digest == [0; 32]
                || self
                    .timing
                    .assignment_floor
                    .is_some_and(|floor| record.assignment_version < floor)
                || self
                    .timing
                    .last_assignment_version
                    .is_some_and(|previous| record.assignment_version < previous)
                || record.local_barrier_ns > record.pipeline_stall_ns
                || record
                    .aligned_resume_ns
                    .is_some_and(|duration| duration > record.pipeline_stall_ns)
                || record
                    .local_barrier_ns
                    .checked_add(record.aligned_resume_ns.unwrap_or(0))
                    .is_none_or(|duration| duration > record.pipeline_stall_ns)
                || (record.aligned_resume_ns.is_some() && !record.durable_tail_handoff)
            {
                return Err(ProtocolFailureKind::InvalidTimingPage);
            }
            let _role = record.role;
            let _deadline_exhausted = record.deadline_exhausted;
            assignment_history
                .observe_timing_certificate(record.assignment_version, record.assignment_digest)?;
            self.greatest_observed_assignment_version = Some(
                self.greatest_observed_assignment_version
                    .map_or(record.assignment_version, |version| {
                        version.max(record.assignment_version)
                    }),
            );
            self.timing.last_assignment_version = Some(record.assignment_version);
            self.greatest_observed_checkpoint_id = Some(record.attempt.checkpoint_id);
            self.timing.cursor = record.sequence;
        }
        if page.has_more
            && (record_count == 0 || self.timing.cursor >= page.next_sequence.saturating_sub(1))
        {
            return Err(ProtocolFailureKind::InvalidTimingPage);
        }
        if !page.has_more && self.timing.cursor != page.next_sequence - 1 {
            return Err(ProtocolFailureKind::InvalidTimingPage);
        }
        self.timing.metadata = Some(metadata);
        Ok((page.has_more, record_count))
    }
}

struct NodeSlotBudget {
    attempts_remaining: u8,
    deadline: Instant,
}

struct ObserverRunContext<'a> {
    secret: &'a DiagnosticReadSecret,
    cancellation: &'a ProtocolCancellation,
    deadline: Instant,
}

impl NodeSlotBudget {
    fn new(run_deadline: Instant) -> Self {
        Self {
            attempts_remaining: MAX_NODE_SLOT_ATTEMPTS,
            deadline: minimum_deadline(Instant::now() + NODE_SLOT_TOTAL_TIMEOUT, run_deadline),
        }
    }

    fn take_attempt(&mut self) -> bool {
        if self.attempts_remaining == 0 {
            false
        } else {
            self.attempts_remaining -= 1;
            true
        }
    }
}

enum ProbeOutcome {
    Response(BoundedHttpResponse),
    Unavailable,
}

struct BoundedHttpResponse {
    status: u16,
    body: Vec<u8>,
}

impl Drop for BoundedHttpResponse {
    fn drop(&mut self) {
        self.body.zeroize();
    }
}

enum HttpAttemptError {
    Retryable,
    Fatal(ProtocolFailureKind),
}

pub fn run_observer_protocol(
    input: ProvisionedObserverInput,
    arm: Arm,
    cancellation: &ProtocolCancellation,
) -> Result<ObserverProtocolResultV3, ObserverProtocolError> {
    run_observer_protocol_inner(input, arm, cancellation, usize::MAX)
}

fn run_observer_protocol_inner(
    input: ProvisionedObserverInput,
    arm: Arm,
    cancellation: &ProtocolCancellation,
    slot_limit: usize,
) -> Result<ObserverProtocolResultV3, ObserverProtocolError> {
    let ProvisionedObserverInput {
        plan,
        invocation_id,
        secret,
    } = input;
    let sanitized_plan_sha256 = plan.canonical_sha256().to_owned();
    let plan = plan.plan();
    let scheduled_slots = u32::try_from(plan.observer_schedule.slots.len())
        .map_err(|_| ObserverProtocolError::new(ProtocolFailureKind::InvalidPlan))?;
    let total_probes = scheduled_slots
        .checked_mul(3)
        .and_then(|count| count.checked_mul(2))
        .ok_or_else(|| ObserverProtocolError::new(ProtocolFailureKind::InvalidPlan))?;
    let mut result = ObserverProtocolResultV3 {
        schema_version: PROTOCOL_RESULT_SCHEMA.to_owned(),
        notice: NOTICE.to_owned(),
        execution_eligible: false,
        arm,
        invocation_id,
        sanitized_plan_sha256,
        base_plan_sha256: plan.base_plan_sha256.clone(),
        observer_schedule_sha256: plan.observer_schedule_sha256.clone(),
        scheduled_slots,
        suppressed_probes: 0,
        connection_attempts: 0,
        parsed_responses: 0,
        retries: 0,
        transient_failures: 0,
        process_transitions: 0,
        timing_records: 0,
        page_budget_deferrals: 0,
        unresolved_timing_nodes: 0,
        retained_events_dropped: 0,
        disposition: ProtocolDispositionV2::Complete,
        retained_events: Vec::new(),
    };
    if arm.observer_mode() == crate::ObserverMode::Suppress {
        result.suppressed_probes = total_probes;
        return Ok(result);
    }
    let run_deadline = Instant::now() + FAKE_RUN_TOTAL_TIMEOUT;
    let run_context = ObserverRunContext {
        secret: &secret,
        cancellation,
        deadline: run_deadline,
    };
    let mut states: [NodeProtocolState; 3] = std::array::from_fn(|_| NodeProtocolState::default());
    let mut assignment_history = RunAssignmentHistory::default();
    for slot in plan.observer_schedule.slots.iter().take(slot_limit) {
        check_active(cancellation, run_deadline)?;
        for (index, endpoint) in plan.endpoints.iter().enumerate() {
            observe_node_slot(
                slot,
                endpoint,
                &mut states[index],
                &mut assignment_history,
                &run_context,
                &mut result,
            )?;
            validate_distinct_node_ids(slot, endpoint.node_ordinal, &states)?;
        }
    }
    result.unresolved_timing_nodes = u8::try_from(
        states
            .iter()
            .filter(|state| state.timing.has_unread_records())
            .count(),
    )
    .map_err(|_| ObserverProtocolError::new(ProtocolFailureKind::EvidenceLoss))?;
    if result.transient_failures != 0 || result.unresolved_timing_nodes != 0 {
        result.disposition = ProtocolDispositionV2::Incomplete;
    }
    Ok(result)
}

fn validate_distinct_node_ids(
    slot: &ObserverSlotV1,
    current_ordinal: u8,
    states: &[NodeProtocolState; 3],
) -> Result<(), ObserverProtocolError> {
    for (left, state) in states.iter().enumerate() {
        let Some(left_node_id) = state.stable_node_id else {
            continue;
        };
        for right in states.iter().skip(left + 1) {
            if right.stable_node_id == Some(left_node_id) {
                return Err(ObserverProtocolError::at(
                    ProtocolFailureKind::ProcessIdentityConflict,
                    slot,
                    current_ordinal,
                    DiagnosticRouteV1::LocalEvidence,
                ));
            }
        }
    }
    Ok(())
}

fn observe_node_slot(
    slot: &ObserverSlotV1,
    endpoint: &ObserverEndpointV2,
    state: &mut NodeProtocolState,
    assignment_history: &mut RunAssignmentHistory,
    run_context: &ObserverRunContext<'_>,
    result: &mut ObserverProtocolResultV3,
) -> Result<(), ObserverProtocolError> {
    let mut budget = NodeSlotBudget::new(run_context.deadline);
    let Some(transition) = read_local_evidence(
        slot,
        endpoint,
        state,
        assignment_history,
        run_context,
        &mut budget,
        result,
    )?
    else {
        return Ok(());
    };
    record_authority_transition(slot, endpoint, transition, result);

    let mut pages = 0_u8;
    let mut timing_bytes = 0_usize;
    let mut timing_records = 0_usize;
    let mut conflict_recovered = false;
    loop {
        if pages == MAX_TIMING_PAGES_PER_SLOT {
            result.page_budget_deferrals += 1;
            retain_event(
                result,
                slot,
                endpoint.node_ordinal,
                DiagnosticRouteV1::ExactTiming,
                ProtocolEventKindV1::TimingPageBudgetExhausted,
            );
            break;
        }
        let authority = state
            .authority
            .as_ref()
            .ok_or_else(|| {
                ObserverProtocolError::at(
                    ProtocolFailureKind::ProcessIdentityConflict,
                    slot,
                    endpoint.node_ordinal,
                    DiagnosticRouteV1::ExactTiming,
                )
            })?
            .process;
        let path = timing_request_path(state.timing.cursor, authority);
        let response = probe_with_retry(
            slot,
            endpoint,
            DiagnosticRouteV1::ExactTiming,
            &path,
            64 * 1_024,
            run_context.secret,
            run_context.cancellation,
            &mut budget,
            result,
        )?;
        let ProbeOutcome::Response(response) = response else {
            break;
        };
        match response.status {
            200 => {
                timing_bytes = timing_bytes
                    .checked_add(response.body.len())
                    .ok_or_else(|| {
                        ObserverProtocolError::at(
                            ProtocolFailureKind::ResponseTooLarge,
                            slot,
                            endpoint.node_ordinal,
                            DiagnosticRouteV1::ExactTiming,
                        )
                    })?;
                if timing_bytes > MAX_TIMING_BYTES_PER_SLOT {
                    return Err(ObserverProtocolError::at(
                        ProtocolFailureKind::ResponseTooLarge,
                        slot,
                        endpoint.node_ordinal,
                        DiagnosticRouteV1::ExactTiming,
                    ));
                }
                let envelope: TimingEnvelopeWire =
                    serde_json::from_slice(&response.body).map_err(|_| {
                        ObserverProtocolError::at(
                            ProtocolFailureKind::InvalidTimingPage,
                            slot,
                            endpoint.node_ordinal,
                            DiagnosticRouteV1::ExactTiming,
                        )
                    })?;
                let (has_more, records) = state
                    .apply_timing_page(envelope, assignment_history)
                    .map_err(|kind| {
                        ObserverProtocolError::at(
                            kind,
                            slot,
                            endpoint.node_ordinal,
                            DiagnosticRouteV1::ExactTiming,
                        )
                    })?;
                timing_records = timing_records.checked_add(records).ok_or_else(|| {
                    ObserverProtocolError::at(
                        ProtocolFailureKind::EvidenceLoss,
                        slot,
                        endpoint.node_ordinal,
                        DiagnosticRouteV1::ExactTiming,
                    )
                })?;
                if timing_records > MAX_TIMING_RECORDS_PER_SLOT {
                    return Err(ObserverProtocolError::at(
                        ProtocolFailureKind::EvidenceLoss,
                        slot,
                        endpoint.node_ordinal,
                        DiagnosticRouteV1::ExactTiming,
                    ));
                }
                result.timing_records = result
                    .timing_records
                    .checked_add(u64::try_from(records).map_err(|_| {
                        ObserverProtocolError::new(ProtocolFailureKind::EvidenceLoss)
                    })?)
                    .ok_or_else(|| ObserverProtocolError::new(ProtocolFailureKind::EvidenceLoss))?;
                pages += 1;
                if !has_more {
                    break;
                }
            }
            409 if !conflict_recovered => {
                let before = state.authority.as_ref().map(|authority| authority.process);
                let Some(transition) = read_local_evidence(
                    slot,
                    endpoint,
                    state,
                    assignment_history,
                    run_context,
                    &mut budget,
                    result,
                )?
                else {
                    break;
                };
                let after = state.authority.as_ref().map(|authority| authority.process);
                if before == after || transition != AuthorityTransition::Restarted {
                    return Err(ObserverProtocolError::at(
                        ProtocolFailureKind::CursorConflict,
                        slot,
                        endpoint.node_ordinal,
                        DiagnosticRouteV1::ExactTiming,
                    ));
                }
                record_authority_transition(slot, endpoint, transition, result);
                conflict_recovered = true;
            }
            409 => {
                return Err(ObserverProtocolError::at(
                    ProtocolFailureKind::CursorConflict,
                    slot,
                    endpoint.node_ordinal,
                    DiagnosticRouteV1::ExactTiming,
                ));
            }
            410 => {
                return Err(ObserverProtocolError::at(
                    ProtocolFailureKind::EvidenceEvicted,
                    slot,
                    endpoint.node_ordinal,
                    DiagnosticRouteV1::ExactTiming,
                ));
            }
            429 | 503 | 504 => {
                record_transient(
                    result,
                    slot,
                    endpoint.node_ordinal,
                    DiagnosticRouteV1::ExactTiming,
                    ProtocolEventKindV1::ServerBusy,
                );
                break;
            }
            300..=399 => {
                return Err(ObserverProtocolError::at(
                    ProtocolFailureKind::RedirectRejected,
                    slot,
                    endpoint.node_ordinal,
                    DiagnosticRouteV1::ExactTiming,
                ));
            }
            401 | 403 => {
                return Err(ObserverProtocolError::at(
                    ProtocolFailureKind::AuthorizationRejected,
                    slot,
                    endpoint.node_ordinal,
                    DiagnosticRouteV1::ExactTiming,
                ));
            }
            _ => {
                return Err(ObserverProtocolError::at(
                    ProtocolFailureKind::UnexpectedStatus,
                    slot,
                    endpoint.node_ordinal,
                    DiagnosticRouteV1::ExactTiming,
                ));
            }
        }
    }
    Ok(())
}

fn read_local_evidence(
    slot: &ObserverSlotV1,
    endpoint: &ObserverEndpointV2,
    state: &mut NodeProtocolState,
    assignment_history: &mut RunAssignmentHistory,
    run_context: &ObserverRunContext<'_>,
    budget: &mut NodeSlotBudget,
    result: &mut ObserverProtocolResultV3,
) -> Result<Option<AuthorityTransition>, ObserverProtocolError> {
    let response = probe_with_retry(
        slot,
        endpoint,
        DiagnosticRouteV1::LocalEvidence,
        LOCAL_EVIDENCE_PATH,
        4 * 1_024,
        run_context.secret,
        run_context.cancellation,
        budget,
        result,
    )?;
    let ProbeOutcome::Response(response) = response else {
        return Ok(None);
    };
    match response.status {
        200 => {
            let envelope: LocalEvidenceEnvelope =
                serde_json::from_slice(&response.body).map_err(|_| {
                    ObserverProtocolError::at(
                        ProtocolFailureKind::InvalidLocalEvidence,
                        slot,
                        endpoint.node_ordinal,
                        DiagnosticRouteV1::LocalEvidence,
                    )
                })?;
            state
                .apply_local_evidence(envelope, assignment_history)
                .map(Some)
                .map_err(|kind| {
                    ObserverProtocolError::at(
                        kind,
                        slot,
                        endpoint.node_ordinal,
                        DiagnosticRouteV1::LocalEvidence,
                    )
                })
        }
        429 | 503 | 504 => {
            record_transient(
                result,
                slot,
                endpoint.node_ordinal,
                DiagnosticRouteV1::LocalEvidence,
                ProtocolEventKindV1::ServerBusy,
            );
            Ok(None)
        }
        300..=399 => Err(ObserverProtocolError::at(
            ProtocolFailureKind::RedirectRejected,
            slot,
            endpoint.node_ordinal,
            DiagnosticRouteV1::LocalEvidence,
        )),
        401 | 403 => Err(ObserverProtocolError::at(
            ProtocolFailureKind::AuthorizationRejected,
            slot,
            endpoint.node_ordinal,
            DiagnosticRouteV1::LocalEvidence,
        )),
        _ => Err(ObserverProtocolError::at(
            ProtocolFailureKind::UnexpectedStatus,
            slot,
            endpoint.node_ordinal,
            DiagnosticRouteV1::LocalEvidence,
        )),
    }
}

fn timing_request_path(cursor: u64, process: ProcessIdentity) -> String {
    format!(
        "{TIMING_PATH}?after_sequence={cursor}&expected_node_id={}&expected_boot_incarnation={}&expected_process_term={}",
        process.node_id, process.boot_incarnation, process.process_term
    )
}

fn record_authority_transition(
    slot: &ObserverSlotV1,
    endpoint: &ObserverEndpointV2,
    transition: AuthorityTransition,
    result: &mut ObserverProtocolResultV3,
) {
    if transition == AuthorityTransition::Restarted {
        result.process_transitions += 1;
        retain_event(
            result,
            slot,
            endpoint.node_ordinal,
            DiagnosticRouteV1::LocalEvidence,
            ProtocolEventKindV1::ProcessChanged,
        );
    }
}

fn retain_event(
    result: &mut ObserverProtocolResultV3,
    slot: &ObserverSlotV1,
    node_ordinal: u8,
    route: DiagnosticRouteV1,
    kind: ProtocolEventKindV1,
) {
    if result.retained_events.len() < MAX_RETAINED_EVENTS {
        result.retained_events.push(ObserverProtocolEventV1 {
            slot_ordinal: slot.ordinal,
            node_ordinal,
            route,
            kind,
        });
    } else {
        result.retained_events_dropped += 1;
    }
}

fn record_transient(
    result: &mut ObserverProtocolResultV3,
    slot: &ObserverSlotV1,
    node_ordinal: u8,
    route: DiagnosticRouteV1,
    kind: ProtocolEventKindV1,
) {
    result.transient_failures += 1;
    retain_event(result, slot, node_ordinal, route, kind);
}

#[allow(clippy::too_many_arguments)]
fn probe_with_retry(
    slot: &ObserverSlotV1,
    endpoint: &ObserverEndpointV2,
    route: DiagnosticRouteV1,
    path: &str,
    body_cap: usize,
    secret: &DiagnosticReadSecret,
    cancellation: &ProtocolCancellation,
    budget: &mut NodeSlotBudget,
    result: &mut ObserverProtocolResultV3,
) -> Result<ProbeOutcome, ObserverProtocolError> {
    let logical_deadline =
        minimum_deadline(Instant::now() + REQUEST_TOTAL_TIMEOUT, budget.deadline);
    let mut attempt = 0_u8;
    loop {
        check_active(cancellation, logical_deadline).map_err(|mut error| {
            error.slot_ordinal = Some(slot.ordinal);
            error.node_ordinal = Some(endpoint.node_ordinal);
            error.route = Some(route);
            error
        })?;
        if !budget.take_attempt() {
            record_transient(
                result,
                slot,
                endpoint.node_ordinal,
                route,
                ProtocolEventKindV1::NodeSlotBudgetExhausted,
            );
            return Ok(ProbeOutcome::Unavailable);
        }
        attempt += 1;
        result.connection_attempts += 1;
        let response = bounded_http_get(
            endpoint.address,
            path,
            body_cap,
            secret,
            cancellation,
            logical_deadline,
        );
        if response.is_ok() {
            result.parsed_responses += 1;
        }
        let retryable = match &response {
            Ok(response) => matches!(response.status, 429 | 503 | 504),
            Err(HttpAttemptError::Retryable) => true,
            Err(HttpAttemptError::Fatal(_)) => false,
        };
        if retryable && attempt < MAX_REQUEST_ATTEMPTS && budget.attempts_remaining > 0 {
            result.retries += 1;
            cancellable_delay(cancellation, logical_deadline).map_err(|mut error| {
                error.slot_ordinal = Some(slot.ordinal);
                error.node_ordinal = Some(endpoint.node_ordinal);
                error.route = Some(route);
                error
            })?;
            continue;
        }
        return match response {
            Ok(response) => Ok(ProbeOutcome::Response(response)),
            Err(HttpAttemptError::Retryable) => {
                record_transient(
                    result,
                    slot,
                    endpoint.node_ordinal,
                    route,
                    ProtocolEventKindV1::TransportUnavailable,
                );
                Ok(ProbeOutcome::Unavailable)
            }
            Err(HttpAttemptError::Fatal(kind)) => Err(ObserverProtocolError::at(
                kind,
                slot,
                endpoint.node_ordinal,
                route,
            )),
        };
    }
}

fn bounded_http_get(
    address: SocketAddr,
    path: &str,
    body_cap: usize,
    secret: &DiagnosticReadSecret,
    cancellation: &ProtocolCancellation,
    deadline: Instant,
) -> Result<BoundedHttpResponse, HttpAttemptError> {
    if !address.ip().is_loopback()
        || address.port() == 0
        || !path.starts_with('/')
        || path.bytes().any(|byte| byte <= b' ' || byte == 0x7f)
    {
        return Err(HttpAttemptError::Fatal(ProtocolFailureKind::InvalidPlan));
    }
    ensure_attempt_active(cancellation, deadline)?;
    let connect_timeout = remaining(deadline)
        .map(|value| value.min(CONNECT_TIMEOUT))
        .ok_or(HttpAttemptError::Fatal(
            ProtocolFailureKind::TotalDeadlineExceeded,
        ))?;
    let mut stream = TcpStream::connect_timeout(&address, connect_timeout)
        .map_err(|_| HttpAttemptError::Retryable)?;
    stream
        .set_nodelay(true)
        .map_err(|_| HttpAttemptError::Retryable)?;
    ensure_attempt_active(cancellation, deadline)?;
    let write_timeout = remaining(deadline)
        .map(|value| value.min(WRITE_TIMEOUT))
        .ok_or(HttpAttemptError::Fatal(
            ProtocolFailureKind::TotalDeadlineExceeded,
        ))?;
    stream
        .set_write_timeout(Some(write_timeout))
        .map_err(|_| HttpAttemptError::Retryable)?;
    let authority = address.to_string();
    let mut request = Vec::with_capacity(path.len() + authority.len() + 160);
    request.extend_from_slice(b"GET ");
    request.extend_from_slice(path.as_bytes());
    request.extend_from_slice(b" HTTP/1.1\r\nHost: ");
    request.extend_from_slice(authority.as_bytes());
    request.extend_from_slice(b"\r\nAccept: application/json\r\nAuthorization: Bearer ");
    request.extend_from_slice(secret.encoded());
    request.extend_from_slice(b"\r\nConnection: close\r\n\r\n");
    let write_result = stream.write_all(&request);
    request.zeroize();
    write_result.map_err(|_| HttpAttemptError::Retryable)?;
    ensure_attempt_active(cancellation, deadline)?;
    read_http_response(&mut stream, body_cap, cancellation, deadline)
}

fn read_http_response(
    stream: &mut TcpStream,
    body_cap: usize,
    cancellation: &ProtocolCancellation,
    deadline: Instant,
) -> Result<BoundedHttpResponse, HttpAttemptError> {
    let response_cap = HTTP_HEADER_MAX_BYTES
        .checked_add(body_cap)
        .and_then(|value| value.checked_add(1))
        .ok_or(HttpAttemptError::Fatal(
            ProtocolFailureKind::ResponseTooLarge,
        ))?;
    let mut response = Zeroizing::new(Vec::with_capacity(response_cap.min(64 * 1_024)));
    let mut header_end = None;
    let mut last_progress = Instant::now();
    while header_end.is_none() {
        if response.len() == response_cap {
            return Err(HttpAttemptError::Fatal(
                ProtocolFailureKind::ResponseTooLarge,
            ));
        }
        let read = read_some(
            stream,
            &mut response,
            response_cap,
            cancellation,
            deadline,
            &mut last_progress,
        )?;
        if read == 0 {
            return Err(HttpAttemptError::Fatal(
                ProtocolFailureKind::InvalidHttpResponse,
            ));
        }
        header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|offset| offset + 4);
        if header_end.is_none() && response.len() >= HTTP_HEADER_MAX_BYTES {
            return Err(HttpAttemptError::Fatal(
                ProtocolFailureKind::ResponseTooLarge,
            ));
        }
    }
    let header_end = header_end.expect("header terminator was found");
    if header_end > HTTP_HEADER_MAX_BYTES {
        return Err(HttpAttemptError::Fatal(
            ProtocolFailureKind::ResponseTooLarge,
        ));
    }
    let (status, content_length) = parse_http_headers(&response[..header_end], body_cap)?;
    let observed_body = response.len() - header_end;
    if observed_body > content_length {
        return Err(HttpAttemptError::Fatal(
            ProtocolFailureKind::InvalidHttpResponse,
        ));
    }
    while response.len() - header_end < content_length {
        let maximum = header_end
            .checked_add(content_length)
            .ok_or(HttpAttemptError::Fatal(
                ProtocolFailureKind::ResponseTooLarge,
            ))?;
        let read = read_some(
            stream,
            &mut response,
            maximum,
            cancellation,
            deadline,
            &mut last_progress,
        )?;
        if read == 0 {
            return Err(HttpAttemptError::Fatal(
                ProtocolFailureKind::InvalidHttpResponse,
            ));
        }
    }
    ensure_attempt_active(cancellation, deadline)?;
    Ok(BoundedHttpResponse {
        status,
        body: response.split_off(header_end),
    })
}

fn read_some(
    stream: &mut TcpStream,
    response: &mut Vec<u8>,
    maximum: usize,
    cancellation: &ProtocolCancellation,
    deadline: Instant,
    last_progress: &mut Instant,
) -> Result<usize, HttpAttemptError> {
    loop {
        ensure_attempt_active(cancellation, deadline)?;
        let now = Instant::now();
        if now.duration_since(*last_progress) >= READ_IDLE_TIMEOUT {
            return Err(HttpAttemptError::Retryable);
        }
        let idle_remaining = READ_IDLE_TIMEOUT - now.duration_since(*last_progress);
        let timeout = remaining(deadline)
            .map(|value| value.min(idle_remaining).min(READ_POLL_INTERVAL))
            .ok_or(HttpAttemptError::Fatal(
                ProtocolFailureKind::TotalDeadlineExceeded,
            ))?;
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|_| HttpAttemptError::Retryable)?;
        let available = maximum.saturating_sub(response.len());
        if available == 0 {
            return Err(HttpAttemptError::Fatal(
                ProtocolFailureKind::ResponseTooLarge,
            ));
        }
        let mut chunk = [0_u8; 8 * 1_024];
        let read_cap = available.min(chunk.len());
        match stream.read(&mut chunk[..read_cap]) {
            Ok(read) => {
                if read != 0 {
                    response.extend_from_slice(&chunk[..read]);
                    *last_progress = Instant::now();
                }
                return Ok(read);
            }
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::TimedOut | ErrorKind::WouldBlock | ErrorKind::Interrupted
                ) => {}
            Err(_) => return Err(HttpAttemptError::Retryable),
        }
    }
}

fn parse_http_headers(headers: &[u8], body_cap: usize) -> Result<(u16, usize), HttpAttemptError> {
    let headers = std::str::from_utf8(headers)
        .map_err(|_| HttpAttemptError::Fatal(ProtocolFailureKind::InvalidHttpResponse))?;
    let Some(headers) = headers.strip_suffix("\r\n\r\n") else {
        return Err(HttpAttemptError::Fatal(
            ProtocolFailureKind::InvalidHttpResponse,
        ));
    };
    let mut lines = headers.split("\r\n");
    let status_line = lines.next().ok_or(HttpAttemptError::Fatal(
        ProtocolFailureKind::InvalidHttpResponse,
    ))?;
    let mut status_parts = status_line.splitn(3, ' ');
    if status_parts.next() != Some("HTTP/1.1") {
        return Err(HttpAttemptError::Fatal(
            ProtocolFailureKind::InvalidHttpResponse,
        ));
    }
    let status_text = status_parts.next().ok_or(HttpAttemptError::Fatal(
        ProtocolFailureKind::InvalidHttpResponse,
    ))?;
    if status_text.len() != 3 || !status_text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(HttpAttemptError::Fatal(
            ProtocolFailureKind::InvalidHttpResponse,
        ));
    }
    let status = status_text
        .parse::<u16>()
        .map_err(|_| HttpAttemptError::Fatal(ProtocolFailureKind::InvalidHttpResponse))?;
    if !(100..=599).contains(&status) {
        return Err(HttpAttemptError::Fatal(
            ProtocolFailureKind::InvalidHttpResponse,
        ));
    }
    let mut content_length = None;
    let mut content_type_json = false;
    for line in lines {
        if line.is_empty() || line.starts_with([' ', '\t']) {
            return Err(HttpAttemptError::Fatal(
                ProtocolFailureKind::InvalidHttpResponse,
            ));
        }
        let (name, raw_value) = line.split_once(':').ok_or(HttpAttemptError::Fatal(
            ProtocolFailureKind::InvalidHttpResponse,
        ))?;
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(HttpAttemptError::Fatal(
                ProtocolFailureKind::InvalidHttpResponse,
            ));
        }
        let value = raw_value.trim_matches([' ', '\t']);
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err(HttpAttemptError::Fatal(
                ProtocolFailureKind::InvalidHttpResponse,
            ));
        }
        if name.eq_ignore_ascii_case("content-length") {
            if content_length.is_some()
                || value.is_empty()
                || !value.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(HttpAttemptError::Fatal(
                    ProtocolFailureKind::InvalidHttpResponse,
                ));
            }
            let length = value
                .parse::<usize>()
                .map_err(|_| HttpAttemptError::Fatal(ProtocolFailureKind::InvalidHttpResponse))?;
            if length > body_cap {
                return Err(HttpAttemptError::Fatal(
                    ProtocolFailureKind::ResponseTooLarge,
                ));
            }
            content_length = Some(length);
        }
        if name.eq_ignore_ascii_case("content-type") {
            content_type_json = value.eq_ignore_ascii_case("application/json");
        }
    }
    if status == 200 && !content_type_json {
        return Err(HttpAttemptError::Fatal(
            ProtocolFailureKind::InvalidHttpResponse,
        ));
    }
    Ok((
        status,
        content_length.ok_or(HttpAttemptError::Fatal(
            ProtocolFailureKind::InvalidHttpResponse,
        ))?,
    ))
}

fn cancellable_delay(
    cancellation: &ProtocolCancellation,
    deadline: Instant,
) -> Result<(), ObserverProtocolError> {
    let delay_deadline = minimum_deadline(Instant::now() + RETRY_DELAY, deadline);
    loop {
        check_active(cancellation, deadline)?;
        let now = Instant::now();
        if now >= delay_deadline {
            return Ok(());
        }
        std::thread::sleep((delay_deadline - now).min(Duration::from_millis(10)));
    }
}

fn ensure_attempt_active(
    cancellation: &ProtocolCancellation,
    deadline: Instant,
) -> Result<(), HttpAttemptError> {
    if cancellation.is_cancelled() {
        Err(HttpAttemptError::Fatal(ProtocolFailureKind::Cancelled))
    } else if Instant::now() >= deadline {
        Err(HttpAttemptError::Fatal(
            ProtocolFailureKind::TotalDeadlineExceeded,
        ))
    } else {
        Ok(())
    }
}

fn check_active(
    cancellation: &ProtocolCancellation,
    deadline: Instant,
) -> Result<(), ObserverProtocolError> {
    if cancellation.is_cancelled() {
        Err(ObserverProtocolError::new(ProtocolFailureKind::Cancelled))
    } else if Instant::now() >= deadline {
        Err(ObserverProtocolError::new(
            ProtocolFailureKind::TotalDeadlineExceeded,
        ))
    } else {
        Ok(())
    }
}

fn remaining(deadline: Instant) -> Option<Duration> {
    deadline.checked_duration_since(Instant::now())
}

fn minimum_deadline(left: Instant, right: Instant) -> Instant {
    if left <= right {
        left
    } else {
        right
    }
}

#[cfg(test)]
#[allow(
    clippy::disallowed_types,
    reason = "the loopback fake-server fixture uses a poisoned standard mutex only in tests"
)]
mod tests {
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;

    use serde_json::{json, Value};

    use super::*;
    use crate::ObserverPolicyV1;

    const TEST_SECRET: &str = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc";

    enum FakeAction {
        Json(u16, Vec<u8>, Vec<(&'static str, String)>),
        Raw(Vec<u8>),
        Stall(Duration),
    }

    struct FakeServer {
        address: SocketAddr,
        connections: Arc<AtomicUsize>,
        requests: Arc<Mutex<Vec<Vec<u8>>>>,
        stop: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    impl FakeServer {
        fn spawn(responder: impl Fn(usize, &[u8]) -> FakeAction + Send + Sync + 'static) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let connections = Arc::new(AtomicUsize::new(0));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let responder = Arc::new(responder);
            let thread_connections = connections.clone();
            let thread_requests = requests.clone();
            let thread_stop = stop.clone();
            let thread = std::thread::spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    let (mut stream, _) = match listener.accept() {
                        Ok(value) => value,
                        Err(_) => break,
                    };
                    if thread_stop.load(Ordering::Acquire) {
                        break;
                    }
                    stream.set_nodelay(true).unwrap();
                    let ordinal = thread_connections.fetch_add(1, Ordering::AcqRel);
                    stream
                        .set_read_timeout(Some(Duration::from_secs(1)))
                        .unwrap();
                    let mut request = Vec::new();
                    let mut chunk = [0_u8; 512];
                    while request.len() < 8 * 1_024 {
                        match stream.read(&mut chunk) {
                            Ok(0) | Err(_) => break,
                            Ok(read) => {
                                request.extend_from_slice(&chunk[..read]);
                                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                                    break;
                                }
                            }
                        }
                    }
                    if request.is_empty() {
                        continue;
                    }
                    thread_requests.lock().unwrap().push(request.clone());
                    match responder(ordinal, &request) {
                        FakeAction::Json(status, body, headers) => {
                            let reason = match status {
                                200 => "OK",
                                302 => "Found",
                                409 => "Conflict",
                                410 => "Gone",
                                429 => "Too Many Requests",
                                503 => "Service Unavailable",
                                504 => "Gateway Timeout",
                                _ => "Error",
                            };
                            let mut response = format!(
                                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
                                body.len()
                            )
                            .into_bytes();
                            for (name, value) in headers {
                                response.extend_from_slice(name.as_bytes());
                                response.extend_from_slice(b": ");
                                response.extend_from_slice(value.as_bytes());
                                response.extend_from_slice(b"\r\n");
                            }
                            response.extend_from_slice(b"Connection: close\r\n\r\n");
                            response.extend_from_slice(&body);
                            let _ = stream.write_all(&response);
                        }
                        FakeAction::Raw(response) => {
                            let _ = stream.write_all(&response);
                        }
                        FakeAction::Stall(duration) => std::thread::sleep(duration),
                    }
                }
            });
            Self {
                address,
                connections,
                requests,
                stop,
                thread: Some(thread),
            }
        }

        fn connection_count(&self) -> usize {
            self.connections.load(Ordering::Acquire)
        }

        fn requests(&self) -> Vec<Vec<u8>> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl Drop for FakeServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            let _ = TcpStream::connect(self.address);
            if let Some(thread) = self.thread.take() {
                thread.join().unwrap();
            }
        }
    }

    fn observer_schedule() -> ObserverScheduleV1 {
        let slots = (0..EXPECTED_OBSERVER_SLOTS)
            .map(|index| {
                let ordinal = index as u32;
                let at_ns = u64::from(ordinal) * OBSERVER_INTERVAL_NS;
                let boundary = match at_ns {
                    0 => Some(LifecycleBoundaryV1::WindowStart),
                    120_000_000_000 => Some(LifecycleBoundaryV1::FaultCheckpoint),
                    INPUT_TARGET_END_NS => Some(LifecycleBoundaryV1::InputTargetEnd),
                    POST_RECOVERY_BASE_NS => Some(LifecycleBoundaryV1::PostRecoverySamplingAnchor),
                    _ => None,
                };
                ObserverSlotV1 {
                    ordinal,
                    at_ns,
                    boundary,
                }
            })
            .collect();
        ObserverScheduleV1 {
            slots,
            node_ordinals: [0, 1, 2],
            route_order: [
                DiagnosticRouteV1::LocalEvidence,
                DiagnosticRouteV1::ExactTiming,
            ],
            policy: ObserverPolicyV1 {
                poll_interval_ns: OBSERVER_INTERVAL_NS,
                local_evidence_max_bytes: 4 * 1_024,
                exact_timing_max_bytes: 64 * 1_024,
            },
        }
    }

    fn validated_plan(addresses: [SocketAddr; 3]) -> ValidatedSanitizedObserverPlanV2 {
        let observer_schedule = observer_schedule();
        let schedule_bytes = serde_json::to_vec(&observer_schedule).unwrap();
        let observer_schedule_sha256 = domain_hash(
            b"laminardb-instrumentation-ab-observer-schedule/v1\0",
            &schedule_bytes,
        );
        let endpoints = std::array::from_fn(|index| ObserverEndpointV2 {
            node_ordinal: index as u8,
            address: addresses[index],
        });
        let plan = SanitizedObserverPlanV2 {
            schema_version: SANITIZED_PLAN_SCHEMA.to_owned(),
            notice: NOTICE.to_owned(),
            execution_eligible: false,
            base_plan_sha256: "a".repeat(64),
            observer_schedule_sha256,
            observer_schedule,
            endpoints,
            protocol: ObserverProtocolPolicyV2::FROZEN,
        };
        let bytes = serde_json::to_vec(&validate_sanitized_plan(plan).unwrap()).unwrap();
        validate_sanitized_plan_bytes(&bytes).unwrap()
    }

    fn secret() -> DiagnosticReadSecret {
        DiagnosticReadSecret::from_provisioned_bytes(TEST_SECRET.as_bytes()).unwrap()
    }

    fn invocation_id() -> Uuid {
        Uuid::parse_str("a6ee93c9-e014-41f8-b2c6-8966ea90a274").unwrap()
    }

    fn retained_prefix(
        route: DiagnosticRouteV1,
        kind: ProtocolEventKindV1,
    ) -> Vec<ObserverProtocolEventV1> {
        (0..MAX_RETAINED_EVENTS)
            .map(|index| ObserverProtocolEventV1 {
                slot_ordinal: (index / 3) as u32,
                node_ordinal: (index % 3) as u8,
                route,
                kind,
            })
            .collect()
    }

    fn input(addresses: [SocketAddr; 3]) -> ProvisionedObserverInput {
        ProvisionedObserverInput {
            plan: validated_plan(addresses),
            invocation_id: invocation_id(),
            secret: secret(),
        }
    }

    fn boot(node: u64, generation: u64) -> String {
        format!("00000000-0000-4000-8000-{node:06x}{generation:06x}")
    }

    fn local_evidence(node: u64, generation: u64) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "schema_version": LOCAL_EVIDENCE_SCHEMA,
            "evidence": {
                "participant": {
                    "node_id": node,
                    "boot_incarnation": boot(node, generation),
                },
                "process_term": generation,
                "adopted_assignment": {
                    "participant": {
                        "node_id": node,
                        "boot_incarnation": boot(node, generation),
                    },
                    "assignment_version": generation,
                    "partitioning_abi_version": PARTITIONING_ABI_VERSION,
                    "vnode_count": 256,
                    "assignment_digest": vec![generation as u8; 32],
                }
            }
        }))
        .unwrap()
    }

    fn timing_record(node: u64, generation: u64, sequence: u64) -> Value {
        let certificate_digest_byte = (generation as u8) ^ 0x80;
        json!({
            "sequence": sequence,
            "process": {
                "participant": {
                    "node_id": node,
                    "boot_incarnation": boot(node, generation),
                },
                "process_term": generation,
            },
            "attempt": {"epoch": sequence, "checkpoint_id": sequence},
            "role": if sequence.is_multiple_of(2) {"leader"} else {"follower"},
            "assignment_version": generation,
            "assignment_digest": vec![certificate_digest_byte; 32],
            "pipeline_stall_ns": 30,
            "local_barrier_ns": 10,
            "aligned_resume_ns": 5,
            "durable_tail_handoff": true,
            "deadline_exhausted": false,
        })
    }

    fn timing_page(
        node: u64,
        generation: u64,
        after: u64,
        records: Vec<Value>,
        next_sequence: u64,
        has_more: bool,
    ) -> Vec<u8> {
        let accepted = next_sequence - 1;
        let overwritten = accepted.saturating_sub(TIMING_LEDGER_CAPACITY as u64);
        serde_json::to_vec(&json!({
            "schema_version": TIMING_SCHEMA,
            "process_identity": {
                "participant": {
                    "node_id": node,
                    "boot_incarnation": boot(node, generation),
                },
                "process_term": generation,
            },
            "after_sequence": after,
            "page": {
                "capacity": TIMING_LEDGER_CAPACITY,
                "oldest_retained_sequence": if accepted == 0 {Value::Null} else {json!(overwritten + 1)},
                "next_sequence": next_sequence,
                "overwritten_record_count": overwritten,
                "recording_loss_count": 0,
                "metadata_exhausted": false,
                "has_more": has_more,
                "records": records,
            }
        }))
        .unwrap()
    }

    fn query_value(request: &[u8], name: &str) -> Option<u64> {
        let request = std::str::from_utf8(request).ok()?;
        let target = request.lines().next()?.split_ascii_whitespace().nth(1)?;
        let query = target.split_once('?')?.1;
        query.split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            (key == name).then(|| value.parse().ok()).flatten()
        })
    }

    fn success_server(node: u64) -> FakeServer {
        FakeServer::spawn(move |_ordinal, request| {
            let request = std::str::from_utf8(request).unwrap();
            if request.starts_with(&format!("GET {LOCAL_EVIDENCE_PATH} HTTP/1.1\r\n")) {
                FakeAction::Json(200, local_evidence(node, 1), Vec::new())
            } else {
                let after = query_value(request.as_bytes(), "after_sequence").unwrap();
                FakeAction::Json(
                    200,
                    timing_page(node, 1, after, Vec::new(), after + 1, false),
                    Vec::new(),
                )
            }
        })
    }

    fn run_with_servers(
        servers: &[FakeServer; 3],
        arm: Arm,
        cancellation: &ProtocolCancellation,
    ) -> Result<ObserverProtocolResultV3, ObserverProtocolError> {
        run_observer_protocol_inner(
            input([servers[0].address, servers[1].address, servers[2].address]),
            arm,
            cancellation,
            1,
        )
    }

    fn run_full_with_servers(
        servers: &[FakeServer; 3],
        arm: Arm,
        cancellation: &ProtocolCancellation,
    ) -> Result<ObserverProtocolResultV3, ObserverProtocolError> {
        run_observer_protocol(
            input([servers[0].address, servers[1].address, servers[2].address]),
            arm,
            cancellation,
        )
    }

    #[test]
    fn serialized_policy_matches_every_runtime_bound() {
        let policy = ObserverProtocolPolicyV2::FROZEN;
        assert_eq!(
            CONNECT_TIMEOUT,
            Duration::from_millis(policy.connect_timeout_ms)
        );
        assert_eq!(
            WRITE_TIMEOUT,
            Duration::from_millis(policy.write_timeout_ms)
        );
        assert_eq!(
            READ_IDLE_TIMEOUT,
            Duration::from_millis(policy.read_idle_timeout_ms)
        );
        assert_eq!(
            REQUEST_TOTAL_TIMEOUT,
            Duration::from_millis(policy.request_total_timeout_ms)
        );
        assert_eq!(
            NODE_SLOT_TOTAL_TIMEOUT,
            Duration::from_millis(policy.node_slot_total_timeout_ms)
        );
        assert_eq!(RETRY_DELAY, Duration::from_millis(policy.retry_delay_ms));
        assert_eq!(
            READ_POLL_INTERVAL,
            Duration::from_millis(policy.cancellation_poll_ms)
        );
        assert_eq!(
            FAKE_RUN_TOTAL_TIMEOUT,
            Duration::from_millis(policy.fake_run_total_timeout_ms)
        );
        assert_eq!(MAX_REQUEST_ATTEMPTS, policy.max_request_attempts);
        assert_eq!(MAX_NODE_SLOT_ATTEMPTS, policy.max_node_slot_attempts);
        assert_eq!(MAX_TIMING_PAGES_PER_SLOT, policy.max_timing_pages_per_slot);
        assert_eq!(
            MAX_TIMING_BYTES_PER_SLOT,
            policy.max_timing_bytes_per_slot as usize
        );
        assert_eq!(
            MAX_TIMING_RECORDS_PER_SLOT,
            policy.max_timing_records_per_slot as usize
        );
        assert_eq!(HTTP_HEADER_MAX_BYTES, policy.http_header_max_bytes as usize);
        assert_eq!(MAX_RETAINED_EVENTS, policy.max_retained_events as usize);
    }

    #[test]
    fn bootstrap_is_canonical_typed_and_control_opens_zero_connections() {
        let servers = [success_server(1), success_server(2), success_server(3)];
        let plan = validated_plan([servers[0].address, servers[1].address, servers[2].address]);
        let mut frame = Vec::new();
        write_supervisor_bootstrap(&mut frame, plan.plan(), invocation_id(), &secret()).unwrap();
        frame.extend_from_slice(SUPERVISOR_CANCEL_BYTES);
        let mut source = SupervisorBootstrapSource::new(frame.as_slice());
        let provisioned = source.take().unwrap();
        assert_eq!(
            format!("{:?}", provisioned.secret),
            "DiagnosticReadSecret([REDACTED])"
        );
        let second_take = match source.take() {
            Ok(_) => panic!("bootstrap source unexpectedly produced a second secret"),
            Err(error) => error,
        };
        assert_eq!(second_take.kind, ProtocolFailureKind::SecretAlreadyConsumed);
        let result = run_observer_protocol(
            provisioned,
            Arm::PollingControl,
            &ProtocolCancellation::default(),
        )
        .unwrap();
        assert_eq!(result.suppressed_probes, 348);
        assert_eq!(result.connection_attempts, 0);
        assert_eq!(result.parsed_responses, 0);
        assert_eq!(result.disposition, ProtocolDispositionV2::Complete);
        assert_eq!(result.sanitized_plan_sha256, plan.canonical_sha256());
        std::thread::sleep(Duration::from_millis(20));
        assert!(servers.iter().all(|server| server.connection_count() == 0));

        let mut invalid_plan = serde_json::to_value(plan.plan()).unwrap();
        invalid_plan["console_token"] = Value::String(TEST_SECRET.to_owned());
        let invalid_bytes = serde_json::to_vec(&invalid_plan).unwrap();
        assert_eq!(
            validate_sanitized_plan_bytes(&invalid_bytes)
                .unwrap_err()
                .kind,
            ProtocolFailureKind::InvalidPlan
        );
        let debug = format!(
            "{:?}",
            validate_sanitized_plan_bytes(&invalid_bytes).unwrap_err()
        );
        assert!(!debug.contains(TEST_SECRET));
    }

    #[test]
    fn treatment_uses_exact_origin_form_gets_and_bounded_empty_pages() {
        let servers = [success_server(1), success_server(2), success_server(3)];
        let result = run_full_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            result.connection_attempts,
            348,
            "result={result:?}, accepts={:?}",
            servers
                .iter()
                .map(FakeServer::connection_count)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            result.parsed_responses,
            348,
            "result={result:?}, accepts={:?}, requests={:?}",
            servers
                .iter()
                .map(FakeServer::connection_count)
                .collect::<Vec<_>>(),
            servers
                .iter()
                .map(|server| server.requests().len())
                .collect::<Vec<_>>()
        );
        assert_eq!(result.retries, 0);
        assert_eq!(result.timing_records, 0);
        assert_eq!(result.disposition, ProtocolDispositionV2::Complete);
        assert_eq!(result.unresolved_timing_nodes, 0);
        for (index, server) in servers.iter().enumerate() {
            assert_eq!(server.connection_count(), 116);
            let requests = server.requests();
            let local = format!(
                "GET {LOCAL_EVIDENCE_PATH} HTTP/1.1\r\nHost: {}\r\nAccept: application/json\r\nAuthorization: Bearer {TEST_SECRET}\r\nConnection: close\r\n\r\n",
                server.address
            );
            assert_eq!(requests[0], local.as_bytes());
            let node = index as u64 + 1;
            let timing = format!(
                "GET {TIMING_PATH}?after_sequence=0&expected_node_id={node}&expected_boot_incarnation={}&expected_process_term=1 HTTP/1.1\r\nHost: {}\r\nAccept: application/json\r\nAuthorization: Bearer {TEST_SECRET}\r\nConnection: close\r\n\r\n",
                boot(node, 1), server.address
            );
            assert_eq!(requests[1], timing.as_bytes());
            assert!(requests
                .iter()
                .all(|request| !request.starts_with(b"GET http")));
            assert!(requests
                .iter()
                .all(|request| !request.windows(6).any(|value| value == b"Cookie")));
        }
    }

    #[test]
    fn timing_pages_advance_a_process_bound_cursor_without_retaining_bodies() {
        let first_timing = Arc::new(AtomicBool::new(true));
        let marker = first_timing.clone();
        let node0 = FakeServer::spawn(move |_ordinal, request| {
            let text = std::str::from_utf8(request).unwrap();
            if text.starts_with(&format!("GET {LOCAL_EVIDENCE_PATH} HTTP/1.1\r\n")) {
                return FakeAction::Json(200, local_evidence(1, 1), Vec::new());
            }
            let after = query_value(request, "after_sequence").unwrap();
            if marker.swap(false, Ordering::AcqRel) {
                let records = (1..=64).map(|value| timing_record(1, 1, value)).collect();
                FakeAction::Json(200, timing_page(1, 1, 0, records, 66, true), Vec::new())
            } else if after == 64 {
                FakeAction::Json(
                    200,
                    timing_page(1, 1, 64, vec![timing_record(1, 1, 65)], 66, false),
                    Vec::new(),
                )
            } else {
                FakeAction::Json(
                    200,
                    timing_page(1, 1, after, Vec::new(), 66, false),
                    Vec::new(),
                )
            }
        });
        let servers = [node0, success_server(2), success_server(3)];
        let result = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap();
        assert_eq!(result.timing_records, 65);
        assert_eq!(result.connection_attempts, 7);
        let requests = servers[0].requests();
        let continuation = std::str::from_utf8(&requests[2]).unwrap();
        assert!(continuation.starts_with(&format!(
            "GET {TIMING_PATH}?after_sequence=64&expected_node_id=1&expected_boot_incarnation={}&expected_process_term=1 HTTP/1.1\r\n",
            boot(1, 1)
        )));
        assert!(result.retained_events.is_empty());
        assert_eq!(result.disposition, ProtocolDispositionV2::Complete);
    }

    #[test]
    fn retry_redirect_eviction_and_truncation_fail_closed() {
        let retry_counter = Arc::new(AtomicUsize::new(0));
        let counter = retry_counter.clone();
        let retry = FakeServer::spawn(move |_ordinal, request| {
            if counter.fetch_add(1, Ordering::AcqRel) == 0 {
                FakeAction::Json(503, b"{}".to_vec(), Vec::new())
            } else if std::str::from_utf8(request)
                .unwrap()
                .starts_with(&format!("GET {LOCAL_EVIDENCE_PATH} HTTP/1.1\r\n"))
            {
                FakeAction::Json(200, local_evidence(1, 1), Vec::new())
            } else {
                let after = query_value(request, "after_sequence").unwrap();
                FakeAction::Json(
                    200,
                    timing_page(1, 1, after, Vec::new(), after + 1, false),
                    Vec::new(),
                )
            }
        });
        let servers = [retry, success_server(2), success_server(3)];
        let result = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap();
        assert_eq!(result.retries, 1);
        assert_eq!(result.connection_attempts, 7);
        drop(servers);

        let trap = success_server(99);
        let trap_address = trap.address;
        let redirect = FakeServer::spawn(move |_ordinal, _request| {
            FakeAction::Json(
                302,
                Vec::new(),
                vec![("Location", format!("http://{trap_address}/stolen"))],
            )
        });
        let servers = [redirect, success_server(2), success_server(3)];
        let error = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind, ProtocolFailureKind::RedirectRejected);
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(trap.connection_count(), 0);
        drop(servers);

        let gone = FakeServer::spawn(move |_ordinal, request| {
            if std::str::from_utf8(request)
                .unwrap()
                .starts_with(&format!("GET {LOCAL_EVIDENCE_PATH} HTTP/1.1\r\n"))
            {
                FakeAction::Json(200, local_evidence(1, 1), Vec::new())
            } else {
                FakeAction::Json(410, b"{}".to_vec(), Vec::new())
            }
        });
        let servers = [gone, success_server(2), success_server(3)];
        let error = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind, ProtocolFailureKind::EvidenceEvicted);
        drop(servers);

        let truncated = FakeServer::spawn(move |_ordinal, _request| {
            FakeAction::Raw(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 20\r\n\r\n{}"
                    .to_vec(),
            )
        });
        let servers = [truncated, success_server(2), success_server(3)];
        let error = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind, ProtocolFailureKind::InvalidHttpResponse);
    }

    #[test]
    fn cancellation_interrupts_a_stalled_read_and_oversized_headers_are_rejected() {
        let stalled = FakeServer::spawn(move |_ordinal, _request| {
            FakeAction::Stall(Duration::from_millis(500))
        });
        let servers = [stalled, success_server(2), success_server(3)];
        let cancellation = ProtocolCancellation::default();
        let trigger = cancellation.clone();
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(75));
            trigger.cancel();
        });
        let started = Instant::now();
        let error = run_with_servers(&servers, Arm::PollingTreatment, &cancellation).unwrap_err();
        canceller.join().unwrap();
        assert_eq!(error.kind, ProtocolFailureKind::Cancelled);
        assert!(started.elapsed() < Duration::from_millis(300));
        drop(servers);

        let deadline_stall =
            FakeServer::spawn(move |_ordinal, _request| FakeAction::Stall(Duration::from_secs(3)));
        let servers = [deadline_stall, success_server(2), success_server(3)];
        let started = Instant::now();
        let error = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind, ProtocolFailureKind::TotalDeadlineExceeded);
        assert!(started.elapsed() < Duration::from_secs(3));
        drop(servers);

        let oversized = FakeServer::spawn(move |_ordinal, _request| {
            let mut response = b"HTTP/1.1 200 OK\r\nX-Fill: ".to_vec();
            response.extend(std::iter::repeat_n(b'a', HTTP_HEADER_MAX_BYTES));
            response.extend_from_slice(b"\r\n\r\n");
            FakeAction::Raw(response)
        });
        let servers = [oversized, success_server(2), success_server(3)];
        let error = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind, ProtocolFailureKind::ResponseTooLarge);
    }

    #[test]
    fn a_single_conflict_may_rebind_only_to_a_new_process() {
        let restarted = FakeServer::spawn(move |ordinal, _request| match ordinal {
            0 => FakeAction::Json(200, local_evidence(1, 1), Vec::new()),
            1 => FakeAction::Json(409, b"{}".to_vec(), Vec::new()),
            2 => FakeAction::Json(200, local_evidence(1, 2), Vec::new()),
            _ => FakeAction::Json(200, timing_page(1, 2, 0, Vec::new(), 1, false), Vec::new()),
        });
        let servers = [restarted, success_server(2), success_server(3)];
        let result = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap();
        assert_eq!(result.process_transitions, 1);
        assert_eq!(servers[0].connection_count(), 4);
        let requests = servers[0].requests();
        let rebound = std::str::from_utf8(requests.last().unwrap()).unwrap();
        assert!(rebound.starts_with(&format!(
            "GET {TIMING_PATH}?after_sequence=0&expected_node_id=1&expected_boot_incarnation={}&expected_process_term=2 HTTP/1.1\r\n",
            boot(1, 2)
        )));

        let unchanged = FakeServer::spawn(move |ordinal, _request| match ordinal {
            0 | 2 => FakeAction::Json(200, local_evidence(1, 1), Vec::new()),
            _ => FakeAction::Json(409, b"{}".to_vec(), Vec::new()),
        });
        let servers = [unchanged, success_server(2), success_server(3)];
        let error = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind, ProtocolFailureKind::CursorConflict);
        assert_eq!(servers[0].connection_count(), 3);
    }

    #[test]
    fn strict_json_retry_and_page_budgets_are_deterministic() {
        let malformed = FakeServer::spawn(move |_ordinal, _request| {
            let mut evidence: Value = serde_json::from_slice(&local_evidence(1, 1)).unwrap();
            evidence["unexpected"] = Value::Bool(true);
            FakeAction::Json(200, serde_json::to_vec(&evidence).unwrap(), Vec::new())
        });
        let servers = [malformed, success_server(2), success_server(3)];
        let error = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind, ProtocolFailureKind::InvalidLocalEvidence);
        drop(servers);

        let oversized = FakeServer::spawn(move |_ordinal, _request| {
            let body = vec![b'x'; 4 * 1_024 + 1];
            FakeAction::Json(200, body, Vec::new())
        });
        let servers = [oversized, success_server(2), success_server(3)];
        let error = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind, ProtocolFailureKind::ResponseTooLarge);
        drop(servers);

        let unavailable = FakeServer::spawn(move |_ordinal, _request| {
            FakeAction::Json(503, b"{}".to_vec(), Vec::new())
        });
        let servers = [unavailable, success_server(2), success_server(3)];
        let result = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap();
        assert_eq!(result.retries, 1);
        assert_eq!(result.transient_failures, 1);
        assert_eq!(servers[0].connection_count(), 2);
        assert_eq!(result.retained_events.len(), 1);
        assert_eq!(result.disposition, ProtocolDispositionV2::Incomplete);
        assert_eq!(result.unresolved_timing_nodes, 0);
        drop(servers);

        let short_page = FakeServer::spawn(move |_ordinal, request| {
            if std::str::from_utf8(request)
                .unwrap()
                .starts_with(&format!("GET {LOCAL_EVIDENCE_PATH} HTTP/1.1\r\n"))
            {
                FakeAction::Json(200, local_evidence(1, 1), Vec::new())
            } else {
                FakeAction::Json(
                    200,
                    timing_page(1, 1, 0, vec![timing_record(1, 1, 1)], 3, true),
                    Vec::new(),
                )
            }
        });
        let servers = [short_page, success_server(2), success_server(3)];
        let error = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind, ProtocolFailureKind::EvidenceLoss);
        drop(servers);

        let paging = FakeServer::spawn(move |_ordinal, request| {
            let text = std::str::from_utf8(request).unwrap();
            if text.starts_with(&format!("GET {LOCAL_EVIDENCE_PATH} HTTP/1.1\r\n")) {
                return FakeAction::Json(200, local_evidence(1, 1), Vec::new());
            }
            let after = query_value(request, "after_sequence").unwrap();
            let records = (after + 1..=after + 64)
                .map(|sequence| timing_record(1, 1, sequence))
                .collect();
            FakeAction::Json(
                200,
                timing_page(1, 1, after, records, after + 66, true),
                Vec::new(),
            )
        });
        let servers = [paging, success_server(2), success_server(3)];
        let result = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap();
        assert_eq!(result.timing_records, 384);
        assert_eq!(servers[0].connection_count(), 7);
        assert_eq!(result.page_budget_deferrals, 1);
        assert_eq!(result.unresolved_timing_nodes, 1);
        assert_eq!(result.disposition, ProtocolDispositionV2::Incomplete);
        assert!(result
            .retained_events
            .iter()
            .any(|event| { event.kind == ProtocolEventKindV1::TimingPageBudgetExhausted }));
    }

    #[test]
    fn timing_schema_cursor_echo_and_page_progress_fail_closed() {
        let unknown = FakeServer::spawn(move |_ordinal, request| {
            let text = std::str::from_utf8(request).unwrap();
            if text.starts_with(&format!("GET {LOCAL_EVIDENCE_PATH} HTTP/1.1\r\n")) {
                return FakeAction::Json(200, local_evidence(1, 1), Vec::new());
            }
            let mut page: Value =
                serde_json::from_slice(&timing_page(1, 1, 0, Vec::new(), 1, false)).unwrap();
            page["unexpected"] = Value::Bool(true);
            FakeAction::Json(200, serde_json::to_vec(&page).unwrap(), Vec::new())
        });
        let servers = [unknown, success_server(2), success_server(3)];
        let error = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind, ProtocolFailureKind::InvalidTimingPage);
        drop(servers);

        let cursor_jump = FakeServer::spawn(move |_ordinal, request| {
            let text = std::str::from_utf8(request).unwrap();
            if text.starts_with(&format!("GET {LOCAL_EVIDENCE_PATH} HTTP/1.1\r\n")) {
                FakeAction::Json(200, local_evidence(1, 1), Vec::new())
            } else {
                FakeAction::Json(200, timing_page(1, 1, 1, Vec::new(), 1, false), Vec::new())
            }
        });
        let servers = [cursor_jump, success_server(2), success_server(3)];
        let error = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind, ProtocolFailureKind::CursorConflict);
        drop(servers);

        let no_progress = FakeServer::spawn(move |_ordinal, request| {
            let text = std::str::from_utf8(request).unwrap();
            if text.starts_with(&format!("GET {LOCAL_EVIDENCE_PATH} HTTP/1.1\r\n")) {
                return FakeAction::Json(200, local_evidence(1, 1), Vec::new());
            }
            let records = (1..=64).map(|value| timing_record(1, 1, value)).collect();
            FakeAction::Json(200, timing_page(1, 1, 0, records, 65, true), Vec::new())
        });
        let servers = [no_progress, success_server(2), success_server(3)];
        let error = run_with_servers(
            &servers,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind, ProtocolFailureKind::InvalidTimingPage);
    }

    #[test]
    fn assignment_digest_domains_are_separate_and_self_consistent() {
        let mut history = RunAssignmentHistory::default();
        let mut state = NodeProtocolState::default();
        let local: LocalEvidenceEnvelope = serde_json::from_slice(&local_evidence(1, 1)).unwrap();
        state.apply_local_evidence(local, &mut history).unwrap();

        let timing: TimingEnvelopeWire = serde_json::from_slice(&timing_page(
            1,
            1,
            0,
            vec![timing_record(1, 1, 1)],
            2,
            false,
        ))
        .unwrap();
        assert_eq!(
            state.apply_timing_page(timing, &mut history).unwrap(),
            (false, 1)
        );
        assert_ne!(
            history.owner_map_identities.get(&1).unwrap().digest,
            *history.timing_certificate_digests.get(&1).unwrap()
        );

        let mut conflicting_local: Value = serde_json::from_slice(&local_evidence(1, 1)).unwrap();
        conflicting_local["evidence"]["adopted_assignment"]["assignment_digest"] =
            json!(vec![9; 32]);
        let conflicting_local = serde_json::from_value(conflicting_local).unwrap();
        assert_eq!(
            state
                .apply_local_evidence(conflicting_local, &mut history)
                .unwrap_err(),
            ProtocolFailureKind::InvalidLocalEvidence
        );

        let mut conflicting_record = timing_record(1, 1, 2);
        conflicting_record["assignment_digest"] = json!(vec![9; 32]);
        let conflicting_timing: TimingEnvelopeWire =
            serde_json::from_slice(&timing_page(1, 1, 1, vec![conflicting_record], 3, false))
                .unwrap();
        assert_eq!(
            state
                .apply_timing_page(conflicting_timing, &mut history)
                .unwrap_err(),
            ProtocolFailureKind::InvalidTimingPage
        );
    }

    #[test]
    fn assignment_versions_reject_regression_but_allow_historical_timing() {
        let mut history = RunAssignmentHistory::default();
        let mut state = NodeProtocolState::default();
        let local: LocalEvidenceEnvelope = serde_json::from_slice(&local_evidence(1, 1)).unwrap();
        state.apply_local_evidence(local, &mut history).unwrap();
        let mut record_v2 = timing_record(1, 1, 1);
        record_v2["assignment_version"] = json!(2);
        record_v2["assignment_digest"] = json!(vec![0x82; 32]);
        let timing_v2: TimingEnvelopeWire =
            serde_json::from_slice(&timing_page(1, 1, 0, vec![record_v2], 2, false)).unwrap();
        state.apply_timing_page(timing_v2, &mut history).unwrap();
        let regressed: LocalEvidenceEnvelope =
            serde_json::from_slice(&local_evidence(1, 1)).unwrap();
        assert_eq!(
            state
                .apply_local_evidence(regressed, &mut history)
                .unwrap_err(),
            ProtocolFailureKind::InvalidLocalEvidence
        );
        let mut caught_up: Value = serde_json::from_slice(&local_evidence(1, 1)).unwrap();
        caught_up["evidence"]["adopted_assignment"]["assignment_version"] = json!(2);
        caught_up["evidence"]["adopted_assignment"]["assignment_digest"] = json!(vec![2; 32]);
        assert_eq!(
            state
                .apply_local_evidence(serde_json::from_value(caught_up).unwrap(), &mut history)
                .unwrap(),
            AuthorityTransition::Stable
        );
        let equal_restart: LocalEvidenceEnvelope =
            serde_json::from_slice(&local_evidence(1, 2)).unwrap();
        assert_eq!(
            state
                .apply_local_evidence(equal_restart, &mut history)
                .unwrap_err(),
            ProtocolFailureKind::InvalidLocalEvidence
        );

        let mut history = RunAssignmentHistory::default();
        let mut state = NodeProtocolState::default();
        let local: LocalEvidenceEnvelope = serde_json::from_slice(&local_evidence(1, 1)).unwrap();
        state.apply_local_evidence(local, &mut history).unwrap();
        let mut record_v3 = timing_record(1, 1, 1);
        record_v3["assignment_version"] = json!(3);
        record_v3["assignment_digest"] = json!(vec![0x83; 32]);
        let timing_v3: TimingEnvelopeWire =
            serde_json::from_slice(&timing_page(1, 1, 0, vec![record_v3], 2, false)).unwrap();
        state.apply_timing_page(timing_v3, &mut history).unwrap();
        let restarted_v2: LocalEvidenceEnvelope =
            serde_json::from_slice(&local_evidence(1, 2)).unwrap();
        assert_eq!(
            state
                .apply_local_evidence(restarted_v2, &mut history)
                .unwrap_err(),
            ProtocolFailureKind::InvalidLocalEvidence
        );

        let mut history = RunAssignmentHistory::default();
        let mut state = NodeProtocolState::default();
        let mut current_v2: Value = serde_json::from_slice(&local_evidence(1, 1)).unwrap();
        current_v2["evidence"]["adopted_assignment"]["assignment_version"] = json!(2);
        current_v2["evidence"]["adopted_assignment"]["assignment_digest"] = json!(vec![2; 32]);
        state
            .apply_local_evidence(serde_json::from_value(current_v2).unwrap(), &mut history)
            .unwrap();
        let historical_v1: TimingEnvelopeWire = serde_json::from_slice(&timing_page(
            1,
            1,
            0,
            vec![timing_record(1, 1, 1)],
            2,
            false,
        ))
        .unwrap();
        assert_eq!(
            state
                .apply_timing_page(historical_v1, &mut history)
                .unwrap(),
            (false, 1)
        );
    }

    #[test]
    fn process_history_rejects_reused_boots_invalid_restarts_and_unread_rollover() {
        let mut history = RunAssignmentHistory::default();
        let mut state = NodeProtocolState::default();
        let first: LocalEvidenceEnvelope = serde_json::from_slice(&local_evidence(1, 1)).unwrap();
        state.apply_local_evidence(first, &mut history).unwrap();
        let second: LocalEvidenceEnvelope = serde_json::from_slice(&local_evidence(1, 2)).unwrap();
        state.apply_local_evidence(second, &mut history).unwrap();
        let reused_boot = boot(1, 1);
        let mut third: Value = serde_json::from_slice(&local_evidence(1, 3)).unwrap();
        third["evidence"]["participant"]["boot_incarnation"] = json!(reused_boot);
        third["evidence"]["adopted_assignment"]["participant"]["boot_incarnation"] =
            json!(reused_boot);
        assert_eq!(
            state
                .apply_local_evidence(serde_json::from_value(third).unwrap(), &mut history)
                .unwrap_err(),
            ProtocolFailureKind::ProcessIdentityConflict
        );

        let mut history = RunAssignmentHistory::default();
        let mut state = NodeProtocolState::default();
        let first: LocalEvidenceEnvelope = serde_json::from_slice(&local_evidence(1, 1)).unwrap();
        state.apply_local_evidence(first, &mut history).unwrap();
        let mut same_version: Value = serde_json::from_slice(&local_evidence(1, 2)).unwrap();
        same_version["evidence"]["adopted_assignment"]["assignment_version"] = json!(1);
        same_version["evidence"]["adopted_assignment"]["assignment_digest"] = json!(vec![1; 32]);
        assert_eq!(
            state
                .apply_local_evidence(serde_json::from_value(same_version).unwrap(), &mut history)
                .unwrap_err(),
            ProtocolFailureKind::InvalidLocalEvidence
        );

        let mut history = RunAssignmentHistory::default();
        let mut state = NodeProtocolState::default();
        let first: LocalEvidenceEnvelope = serde_json::from_slice(&local_evidence(1, 1)).unwrap();
        state.apply_local_evidence(first, &mut history).unwrap();
        let first_timing: TimingEnvelopeWire = serde_json::from_slice(&timing_page(
            1,
            1,
            0,
            vec![timing_record(1, 1, 1)],
            2,
            false,
        ))
        .unwrap();
        state.apply_timing_page(first_timing, &mut history).unwrap();
        let restarted: LocalEvidenceEnvelope =
            serde_json::from_slice(&local_evidence(1, 2)).unwrap();
        state.apply_local_evidence(restarted, &mut history).unwrap();
        let reused_checkpoint: TimingEnvelopeWire = serde_json::from_slice(&timing_page(
            1,
            2,
            0,
            vec![timing_record(1, 2, 1)],
            2,
            false,
        ))
        .unwrap();
        assert_eq!(
            state
                .apply_timing_page(reused_checkpoint, &mut history)
                .unwrap_err(),
            ProtocolFailureKind::InvalidTimingPage
        );

        let mut old_certificate = timing_record(1, 2, 2);
        old_certificate["sequence"] = json!(1);
        old_certificate["assignment_version"] = json!(1);
        old_certificate["assignment_digest"] = json!(vec![0x81; 32]);
        let old_certificate: TimingEnvelopeWire =
            serde_json::from_slice(&timing_page(1, 2, 0, vec![old_certificate], 2, false)).unwrap();
        assert_eq!(
            state
                .apply_timing_page(old_certificate, &mut history)
                .unwrap_err(),
            ProtocolFailureKind::InvalidTimingPage
        );

        let mut history = RunAssignmentHistory::default();
        let mut state = NodeProtocolState::default();
        let first: LocalEvidenceEnvelope = serde_json::from_slice(&local_evidence(1, 1)).unwrap();
        state.apply_local_evidence(first, &mut history).unwrap();
        let mut changed_width: Value = serde_json::from_slice(&local_evidence(1, 1)).unwrap();
        changed_width["evidence"]["adopted_assignment"]["vnode_count"] = json!(512);
        assert_eq!(
            state
                .apply_local_evidence(serde_json::from_value(changed_width).unwrap(), &mut history)
                .unwrap_err(),
            ProtocolFailureKind::InvalidLocalEvidence
        );

        let mut history = RunAssignmentHistory::default();
        let mut state = NodeProtocolState::default();
        let first: LocalEvidenceEnvelope = serde_json::from_slice(&local_evidence(1, 1)).unwrap();
        state.apply_local_evidence(first, &mut history).unwrap();
        let records = (1..=64)
            .map(|sequence| timing_record(1, 1, sequence))
            .collect();
        let unread: TimingEnvelopeWire =
            serde_json::from_slice(&timing_page(1, 1, 0, records, 66, true)).unwrap();
        state.apply_timing_page(unread, &mut history).unwrap();
        let restarted: LocalEvidenceEnvelope =
            serde_json::from_slice(&local_evidence(1, 2)).unwrap();
        assert_eq!(
            state
                .apply_local_evidence(restarted, &mut history)
                .unwrap_err(),
            ProtocolFailureKind::EvidenceLoss
        );
    }

    #[test]
    fn run_history_rejects_cross_node_conflicts_within_each_digest_domain() {
        let mut history = RunAssignmentHistory::default();
        let mut first = NodeProtocolState::default();
        let local: LocalEvidenceEnvelope = serde_json::from_slice(&local_evidence(1, 1)).unwrap();
        first.apply_local_evidence(local, &mut history).unwrap();
        let timing: TimingEnvelopeWire = serde_json::from_slice(&timing_page(
            1,
            1,
            0,
            vec![timing_record(1, 1, 1)],
            2,
            false,
        ))
        .unwrap();
        first.apply_timing_page(timing, &mut history).unwrap();

        let mut conflicting_owner: Value = serde_json::from_slice(&local_evidence(2, 1)).unwrap();
        conflicting_owner["evidence"]["adopted_assignment"]["assignment_digest"] =
            json!(vec![9; 32]);
        let mut second = NodeProtocolState::default();
        assert_eq!(
            second
                .apply_local_evidence(
                    serde_json::from_value(conflicting_owner).unwrap(),
                    &mut history,
                )
                .unwrap_err(),
            ProtocolFailureKind::InvalidLocalEvidence
        );

        let local: LocalEvidenceEnvelope = serde_json::from_slice(&local_evidence(2, 1)).unwrap();
        second.apply_local_evidence(local, &mut history).unwrap();
        let mut conflicting_record = timing_record(2, 1, 1);
        conflicting_record["assignment_digest"] = json!(vec![9; 32]);
        let timing: TimingEnvelopeWire =
            serde_json::from_slice(&timing_page(2, 1, 0, vec![conflicting_record], 2, false))
                .unwrap();
        assert_eq!(
            second.apply_timing_page(timing, &mut history).unwrap_err(),
            ProtocolFailureKind::InvalidTimingPage
        );
    }

    #[test]
    fn retained_event_truncation_is_counted_without_changing_control_behavior() {
        let servers = [success_server(1), success_server(2), success_server(3)];
        let mut result = run_with_servers(
            &servers,
            Arm::PollingControl,
            &ProtocolCancellation::default(),
        )
        .unwrap();
        let slot = &observer_schedule().slots[0];
        for _ in 0..MAX_RETAINED_EVENTS + 3 {
            retain_event(
                &mut result,
                slot,
                0,
                DiagnosticRouteV1::LocalEvidence,
                ProtocolEventKindV1::TransportUnavailable,
            );
        }
        assert_eq!(result.retained_events.len(), MAX_RETAINED_EVENTS);
        assert_eq!(result.retained_events_dropped, 3);
        assert_eq!(result.disposition, ProtocolDispositionV2::Complete);
        assert!(servers.iter().all(|server| server.connection_count() == 0));
    }

    #[test]
    fn bootstrap_and_endpoint_validation_have_no_secret_fallback() {
        assert_eq!(
            DiagnosticReadSecret::from_provisioned_bytes(b"console-token").unwrap_err(),
            SecretSourceError::Invalid
        );
        let servers = [success_server(1), success_server(2), success_server(3)];
        let plan = validated_plan([servers[0].address, servers[1].address, servers[2].address]);
        let mut frame = Vec::new();
        write_supervisor_bootstrap(&mut frame, plan.plan(), invocation_id(), &secret()).unwrap();
        let reject = |candidate: Vec<u8>| {
            let error = match SupervisorBootstrapSource::new(candidate.as_slice()).take() {
                Ok(_) => panic!("invalid bootstrap was accepted"),
                Err(error) => error,
            };
            assert!(!format!("{error:?}").contains(TEST_SECRET));
        };
        let plan_start = SUPERVISOR_BOOTSTRAP_PREFIX.len() + 4;
        let plan_len = u32::from_be_bytes(
            frame[SUPERVISOR_BOOTSTRAP_PREFIX.len()..plan_start]
                .try_into()
                .unwrap(),
        ) as usize;
        let invocation_start = plan_start + plan_len;
        let secret_start = invocation_start + 16;
        let mut invalid = frame.clone();
        invalid[0] ^= 1;
        reject(invalid);
        reject(SUPERVISOR_BOOTSTRAP_PREFIX[..3].to_vec());
        let mut invalid = SUPERVISOR_BOOTSTRAP_PREFIX.to_vec();
        invalid.extend_from_slice(&[0, 0]);
        reject(invalid);
        let mut invalid = frame.clone();
        invalid[SUPERVISOR_BOOTSTRAP_PREFIX.len()..plan_start]
            .copy_from_slice(&0_u32.to_be_bytes());
        reject(invalid);
        let mut invalid = frame.clone();
        invalid[SUPERVISOR_BOOTSTRAP_PREFIX.len()..plan_start]
            .copy_from_slice(&((MAX_SANITIZED_PLAN_BYTES + 1) as u32).to_be_bytes());
        reject(invalid);
        reject(frame[..plan_start + plan_len - 1].to_vec());
        let mut invalid = frame.clone();
        invalid[invocation_start..secret_start].fill(0);
        reject(invalid);
        let mut invalid = frame.clone();
        invalid[secret_start] = b'!';
        reject(invalid);
        reject(frame[..secret_start + 11].to_vec());
        let mut invalid = frame.clone();
        *invalid.last_mut().unwrap() = b'x';
        reject(invalid);
        reject(frame[..frame.len() - 1].to_vec());

        let raw_frame = |candidate: &SanitizedObserverPlanV2| {
            let bytes = serde_json::to_vec(candidate).unwrap();
            let mut raw = SUPERVISOR_BOOTSTRAP_PREFIX.to_vec();
            raw.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            raw.extend_from_slice(&bytes);
            raw.extend_from_slice(invocation_id().as_bytes());
            raw.extend_from_slice(TEST_SECRET.as_bytes());
            raw.push(b'\n');
            raw
        };
        let mut invalid_plans = Vec::new();
        let mut candidate = plan.plan().clone();
        candidate.schema_version = "wrong".to_owned();
        invalid_plans.push(candidate);
        let mut candidate = plan.plan().clone();
        candidate.notice = "wrong".to_owned();
        invalid_plans.push(candidate);
        let mut candidate = plan.plan().clone();
        candidate.execution_eligible = true;
        invalid_plans.push(candidate);
        let mut candidate = plan.plan().clone();
        candidate.endpoints[0].node_ordinal = 2;
        invalid_plans.push(candidate);
        let mut candidate = plan.plan().clone();
        candidate.endpoints[0].address.set_port(0);
        invalid_plans.push(candidate);
        let mut candidate = plan.plan().clone();
        candidate.endpoints[0].address = "192.0.2.1:4317".parse().unwrap();
        invalid_plans.push(candidate);
        let mut candidate = plan.plan().clone();
        candidate.endpoints[1].address = candidate.endpoints[0].address;
        invalid_plans.push(candidate);
        for candidate in invalid_plans {
            reject(raw_frame(&candidate));
        }

        let duplicate_nodes = [success_server(1), success_server(1), success_server(3)];
        let error = run_with_servers(
            &duplicate_nodes,
            Arm::PollingTreatment,
            &ProtocolCancellation::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind, ProtocolFailureKind::ProcessIdentityConflict);

        let cancellation = ProtocolCancellation::default();
        cancellation.cancel();
        let before = servers
            .iter()
            .map(FakeServer::connection_count)
            .collect::<Vec<_>>();
        let error = run_with_servers(&servers, Arm::PollingTreatment, &cancellation).unwrap_err();
        assert_eq!(error.kind, ProtocolFailureKind::Cancelled);
        assert_eq!(
            before,
            servers
                .iter()
                .map(FakeServer::connection_count)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn driver_fake_input_is_canonical_endpoint_only_and_single_use() {
        let addresses = [
            "127.0.0.1:31001".parse().unwrap(),
            "127.0.0.1:31002".parse().unwrap(),
            "[::1]:31003".parse().unwrap(),
        ];
        let mut bytes = Vec::new();
        write_driver_fake_input(&mut bytes, addresses, &secret()).unwrap();
        let payload_start = DRIVER_FAKE_INPUT_PREFIX.len() + 4;
        let payload_len = u32::from_be_bytes(
            bytes[DRIVER_FAKE_INPUT_PREFIX.len()..payload_start]
                .try_into()
                .unwrap(),
        ) as usize;
        assert!(!bytes[payload_start..payload_start + payload_len]
            .windows(TEST_SECRET.len())
            .any(|window| window == TEST_SECRET.as_bytes()));
        let mut source = DriverFakeInputSource::new(std::io::Cursor::new(bytes.clone()));
        let input = source.take().unwrap();
        assert_eq!(input.addresses, addresses);
        let error = match source.take() {
            Ok(_) => panic!("driver fake input was consumed twice"),
            Err(error) => error,
        };
        assert_eq!(error.kind, ProtocolFailureKind::SecretAlreadyConsumed);

        let reject = |candidate: Vec<u8>| {
            let error = match DriverFakeInputSource::new(std::io::Cursor::new(candidate)).take() {
                Ok(_) => panic!("invalid driver fake input was accepted"),
                Err(error) => error,
            };
            assert!(!format!("{error:?}").contains(TEST_SECRET));
        };
        let secret_start = payload_start + payload_len;
        let mut invalid = bytes.clone();
        invalid[0] ^= 1;
        reject(invalid);
        reject(DRIVER_FAKE_INPUT_PREFIX[..3].to_vec());
        let mut invalid = DRIVER_FAKE_INPUT_PREFIX.to_vec();
        invalid.extend_from_slice(&[0, 0]);
        reject(invalid);
        let mut invalid = bytes.clone();
        invalid[DRIVER_FAKE_INPUT_PREFIX.len()..payload_start]
            .copy_from_slice(&0_u32.to_be_bytes());
        reject(invalid);
        let mut invalid = bytes.clone();
        invalid[DRIVER_FAKE_INPUT_PREFIX.len()..payload_start]
            .copy_from_slice(&((MAX_DRIVER_ENDPOINT_BYTES + 1) as u32).to_be_bytes());
        reject(invalid);
        reject(bytes[..payload_start + payload_len - 1].to_vec());
        let mut invalid = bytes.clone();
        invalid[secret_start] = b'!';
        reject(invalid);
        reject(bytes[..secret_start + 11].to_vec());
        let mut invalid = bytes.clone();
        *invalid.last_mut().unwrap() = b'x';
        reject(invalid);
        reject(bytes[..bytes.len() - 1].to_vec());

        let raw_frame = |candidate: &DriverFakeEndpointInputV1| {
            let payload = serde_json::to_vec(candidate).unwrap();
            let mut raw = DRIVER_FAKE_INPUT_PREFIX.to_vec();
            raw.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            raw.extend_from_slice(&payload);
            raw.extend_from_slice(TEST_SECRET.as_bytes());
            raw.push(b'\n');
            raw
        };
        let valid_input = DriverFakeEndpointInputV1 {
            schema_version: DRIVER_FAKE_INPUT_SCHEMA.to_owned(),
            notice: NOTICE.to_owned(),
            execution_eligible: false,
            endpoints: std::array::from_fn(|index| ObserverEndpointV2 {
                node_ordinal: index as u8,
                address: addresses[index],
            }),
        };
        let mut invalid_inputs = Vec::new();
        let mut candidate = valid_input.clone();
        candidate.schema_version = "wrong".to_owned();
        invalid_inputs.push(candidate);
        let mut candidate = valid_input.clone();
        candidate.notice = "wrong".to_owned();
        invalid_inputs.push(candidate);
        let mut candidate = valid_input.clone();
        candidate.execution_eligible = true;
        invalid_inputs.push(candidate);
        let mut candidate = valid_input.clone();
        candidate.endpoints[0].node_ordinal = 2;
        invalid_inputs.push(candidate);
        let mut candidate = valid_input.clone();
        candidate.endpoints[0].address.set_port(0);
        invalid_inputs.push(candidate);
        let mut candidate = valid_input.clone();
        candidate.endpoints[1].address = candidate.endpoints[0].address;
        invalid_inputs.push(candidate);
        let mut candidate = valid_input;
        candidate.endpoints[0].address = "192.0.2.1:31001".parse().unwrap();
        invalid_inputs.push(candidate);
        for candidate in invalid_inputs {
            reject(raw_frame(&candidate));
        }

        let mut noncanonical_payload = bytes[payload_start..payload_start + payload_len].to_vec();
        noncanonical_payload.push(b' ');
        let mut noncanonical = DRIVER_FAKE_INPUT_PREFIX.to_vec();
        noncanonical.extend_from_slice(&(noncanonical_payload.len() as u32).to_be_bytes());
        noncanonical.extend_from_slice(&noncanonical_payload);
        noncanonical.extend_from_slice(TEST_SECRET.as_bytes());
        noncanonical.push(b'\n');
        let error = match DriverFakeInputSource::new(std::io::Cursor::new(noncanonical)).take() {
            Ok(_) => panic!("noncanonical driver fake input was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.kind, ProtocolFailureKind::InvalidBootstrap);
        assert!(write_driver_fake_input(
            Vec::new(),
            [addresses[0], addresses[0], addresses[2]],
            &secret()
        )
        .is_err());
        assert!(write_driver_fake_input(
            Vec::new(),
            [
                addresses[0],
                "192.0.2.1:31002".parse().unwrap(),
                addresses[2]
            ],
            &secret()
        )
        .is_err());
        bytes.zeroize();
        noncanonical_payload.zeroize();
    }

    #[test]
    fn result_validation_is_canonical_plan_bound_and_counter_consistent() {
        let addresses = [
            "127.0.0.1:32001".parse().unwrap(),
            "127.0.0.1:32002".parse().unwrap(),
            "127.0.0.1:32003".parse().unwrap(),
        ];
        let plan = validated_plan(addresses);
        let expectation = ObserverProtocolResultExpectationV3 {
            plan: plan.clone(),
            arm: Arm::PollingControl,
            invocation_id: invocation_id(),
        };
        let result = run_observer_protocol(
            ProvisionedObserverInput {
                plan,
                invocation_id: invocation_id(),
                secret: secret(),
            },
            Arm::PollingControl,
            &ProtocolCancellation::default(),
        )
        .unwrap();
        let bytes = serde_json::to_vec(&result).unwrap();
        assert_eq!(
            validate_observer_protocol_result(&bytes, &expectation).unwrap(),
            result
        );
        let stale_expectation = ObserverProtocolResultExpectationV3 {
            plan: expectation.plan.clone(),
            arm: expectation.arm,
            invocation_id: Uuid::parse_str("9cb0c82b-ced1-465d-bfe6-a9d904328644").unwrap(),
        };
        assert_eq!(
            validate_observer_protocol_result(&bytes, &stale_expectation)
                .unwrap_err()
                .kind,
            ProtocolFailureKind::InvalidResult
        );

        let mut noncanonical = bytes.clone();
        noncanonical.push(b'\n');
        assert_eq!(
            validate_observer_protocol_result(&noncanonical, &expectation)
                .unwrap_err()
                .kind,
            ProtocolFailureKind::InvalidResult
        );
        let mut wrong_hash = result.clone();
        wrong_hash.sanitized_plan_sha256 = "f".repeat(64);
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&wrong_hash).unwrap(),
            &expectation
        )
        .is_err());
        let treatment_expectation = ObserverProtocolResultExpectationV3 {
            plan: expectation.plan.clone(),
            arm: Arm::PollingTreatment,
            invocation_id: invocation_id(),
        };
        let mut treatment = result.clone();
        treatment.arm = Arm::PollingTreatment;
        treatment.suppressed_probes = 0;
        treatment.connection_attempts = 348;
        treatment.parsed_responses = 348;
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&treatment).unwrap(),
            &treatment_expectation
        )
        .is_ok());
        let mut zero_traffic = treatment.clone();
        zero_traffic.connection_attempts = 0;
        zero_traffic.parsed_responses = 0;
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&zero_traffic).unwrap(),
            &treatment_expectation
        )
        .is_err());
        let mut wrong_event_semantics = treatment.clone();
        wrong_event_semantics.process_transitions = 1;
        wrong_event_semantics
            .retained_events
            .push(ObserverProtocolEventV1 {
                slot_ordinal: 0,
                node_ordinal: 0,
                route: DiagnosticRouteV1::ExactTiming,
                kind: ProtocolEventKindV1::ProcessChanged,
            });
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&wrong_event_semantics).unwrap(),
            &treatment_expectation
        )
        .is_err());
        let mut impossible_retries = treatment.clone();
        impossible_retries.connection_attempts = 697;
        impossible_retries.retries = 349;
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&impossible_retries).unwrap(),
            &treatment_expectation
        )
        .is_err());
        let mut short_incomplete = treatment.clone();
        short_incomplete.connection_attempts = 346;
        short_incomplete.parsed_responses = 346;
        short_incomplete.transient_failures = 1;
        short_incomplete.disposition = ProtocolDispositionV2::Incomplete;
        short_incomplete
            .retained_events
            .push(ObserverProtocolEventV1 {
                slot_ordinal: 0,
                node_ordinal: 0,
                route: DiagnosticRouteV1::LocalEvidence,
                kind: ProtocolEventKindV1::ServerBusy,
            });
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&short_incomplete).unwrap(),
            &treatment_expectation
        )
        .is_err());
        let mut short_deferral = treatment.clone();
        short_deferral.connection_attempts = 353;
        short_deferral.parsed_responses = 353;
        short_deferral.timing_records = 383;
        short_deferral.page_budget_deferrals = 1;
        short_deferral.unresolved_timing_nodes = 1;
        short_deferral.disposition = ProtocolDispositionV2::Incomplete;
        short_deferral
            .retained_events
            .push(ObserverProtocolEventV1 {
                slot_ordinal: 0,
                node_ordinal: 0,
                route: DiagnosticRouteV1::ExactTiming,
                kind: ProtocolEventKindV1::TimingPageBudgetExhausted,
            });
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&short_deferral).unwrap(),
            &treatment_expectation
        )
        .is_err());
        let mut terminal_before_process = treatment.clone();
        terminal_before_process.connection_attempts = 347;
        terminal_before_process.parsed_responses = 346;
        terminal_before_process.transient_failures = 1;
        terminal_before_process.process_transitions = 1;
        terminal_before_process.disposition = ProtocolDispositionV2::Incomplete;
        terminal_before_process.retained_events = vec![
            ObserverProtocolEventV1 {
                slot_ordinal: 0,
                node_ordinal: 0,
                route: DiagnosticRouteV1::ExactTiming,
                kind: ProtocolEventKindV1::ServerBusy,
            },
            ObserverProtocolEventV1 {
                slot_ordinal: 0,
                node_ordinal: 0,
                route: DiagnosticRouteV1::LocalEvidence,
                kind: ProtocolEventKindV1::ProcessChanged,
            },
        ];
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&terminal_before_process).unwrap(),
            &treatment_expectation
        )
        .is_err());
        let mut mixed_terminals = treatment.clone();
        mixed_terminals.connection_attempts = 352;
        mixed_terminals.parsed_responses = 351;
        mixed_terminals.transient_failures = 1;
        mixed_terminals.timing_records = 384;
        mixed_terminals.page_budget_deferrals = 1;
        mixed_terminals.unresolved_timing_nodes = 1;
        mixed_terminals.disposition = ProtocolDispositionV2::Incomplete;
        mixed_terminals.retained_events = vec![
            ObserverProtocolEventV1 {
                slot_ordinal: 0,
                node_ordinal: 0,
                route: DiagnosticRouteV1::LocalEvidence,
                kind: ProtocolEventKindV1::ServerBusy,
            },
            ObserverProtocolEventV1 {
                slot_ordinal: 0,
                node_ordinal: 0,
                route: DiagnosticRouteV1::ExactTiming,
                kind: ProtocolEventKindV1::TimingPageBudgetExhausted,
            },
        ];
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&mixed_terminals).unwrap(),
            &treatment_expectation
        )
        .is_err());
        let mut excessive_unresolved = short_incomplete.clone();
        excessive_unresolved.connection_attempts = 347;
        excessive_unresolved.unresolved_timing_nodes = 2;
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&excessive_unresolved).unwrap(),
            &treatment_expectation
        )
        .is_err());
        let mut deferral_after_two_restarts = treatment.clone();
        deferral_after_two_restarts.connection_attempts = 353;
        deferral_after_two_restarts.parsed_responses = 353;
        deferral_after_two_restarts.process_transitions = 2;
        deferral_after_two_restarts.timing_records = 384;
        deferral_after_two_restarts.page_budget_deferrals = 1;
        deferral_after_two_restarts.unresolved_timing_nodes = 1;
        deferral_after_two_restarts.disposition = ProtocolDispositionV2::Incomplete;
        deferral_after_two_restarts.retained_events = vec![
            ObserverProtocolEventV1 {
                slot_ordinal: 0,
                node_ordinal: 0,
                route: DiagnosticRouteV1::LocalEvidence,
                kind: ProtocolEventKindV1::ProcessChanged,
            },
            ObserverProtocolEventV1 {
                slot_ordinal: 0,
                node_ordinal: 0,
                route: DiagnosticRouteV1::LocalEvidence,
                kind: ProtocolEventKindV1::ProcessChanged,
            },
            ObserverProtocolEventV1 {
                slot_ordinal: 0,
                node_ordinal: 0,
                route: DiagnosticRouteV1::ExactTiming,
                kind: ProtocolEventKindV1::TimingPageBudgetExhausted,
            },
        ];
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&deferral_after_two_restarts).unwrap(),
            &treatment_expectation
        )
        .is_err());
        let mut excessive_terminals = treatment.clone();
        excessive_terminals.connection_attempts = 1_392;
        excessive_terminals.parsed_responses = 1_392;
        excessive_terminals.transient_failures = 174;
        excessive_terminals.timing_records = 384;
        excessive_terminals.page_budget_deferrals = 1;
        excessive_terminals.unresolved_timing_nodes = 1;
        excessive_terminals.disposition = ProtocolDispositionV2::Incomplete;
        excessive_terminals.retained_events = retained_prefix(
            DiagnosticRouteV1::LocalEvidence,
            ProtocolEventKindV1::ServerBusy,
        );
        excessive_terminals.retained_events_dropped = 143;
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&excessive_terminals).unwrap(),
            &treatment_expectation
        )
        .is_err());
        let mut excessive_transitions = treatment.clone();
        excessive_transitions.connection_attempts = 1_392;
        excessive_transitions.parsed_responses = 1_392;
        excessive_transitions.process_transitions = 345;
        excessive_transitions.timing_records = 384;
        excessive_transitions.page_budget_deferrals = 1;
        excessive_transitions.retained_events = retained_prefix(
            DiagnosticRouteV1::LocalEvidence,
            ProtocolEventKindV1::ProcessChanged,
        );
        excessive_transitions.retained_events_dropped = 314;
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&excessive_transitions).unwrap(),
            &treatment_expectation
        )
        .is_err());
        let mut timing_probe_shortfall = treatment.clone();
        timing_probe_shortfall.connection_attempts = 183;
        timing_probe_shortfall.parsed_responses = 11;
        timing_probe_shortfall.transient_failures = 174;
        timing_probe_shortfall.timing_records = 640;
        timing_probe_shortfall.disposition = ProtocolDispositionV2::Incomplete;
        timing_probe_shortfall.retained_events = retained_prefix(
            DiagnosticRouteV1::LocalEvidence,
            ProtocolEventKindV1::TransportUnavailable,
        );
        timing_probe_shortfall.retained_events_dropped = 142;
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&timing_probe_shortfall).unwrap(),
            &treatment_expectation
        )
        .is_err());
        let mut unknown_field: Value = serde_json::to_value(&treatment).unwrap();
        unknown_field["unknown"] = Value::Bool(true);
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&unknown_field).unwrap(),
            &treatment_expectation
        )
        .is_err());
        assert!(validate_observer_protocol_result(
            &vec![b' '; PROTOCOL_RESULT_MAX_BYTES + 1],
            &treatment_expectation
        )
        .is_err());
        let mut premature_drop = treatment;
        premature_drop.process_transitions = 1;
        premature_drop.retained_events_dropped = 1;
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&premature_drop).unwrap(),
            &treatment_expectation
        )
        .is_err());
        let mut impossible_control = result;
        impossible_control.connection_attempts = 1;
        assert!(validate_observer_protocol_result(
            &serde_json::to_vec(&impossible_control).unwrap(),
            &expectation
        )
        .is_err());
    }
}
