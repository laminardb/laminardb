//! Control-plane contracts and monotonic pacing primitives for the owned-fake paced observer.
//!
//! This module is deliberately separate from the accelerated fake protocol. It cannot contact a
//! LaminarDB process, cannot produce A/B evidence, and keeps `execution_eligible` false. Later
//! cycles can build evidence framing and owned-fake transport on these validated primitives without
//! changing the accelerated v2/v3 schemas.

use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::observer_protocol::ProtocolCancellation;
use crate::{
    domain_hash, is_lower_sha256, Arm, DiagnosticRouteV1, LifecycleBoundaryV1, ObserverScheduleV1,
    ObserverSlotV1, SealedPlanV1, NOTICE,
};

pub const PACED_OWNED_FAKE_PLAN_SCHEMA: &str = "paced-owned-fake-plan/v1";
pub const PACED_OWNED_FAKE_READY_SCHEMA: &str = "paced-owned-fake-ready/v1";
pub const PACED_OWNED_FAKE_DIAGNOSTIC_POLICY_SCHEMA: &str = "paced-owned-fake-diagnostic-policy/v1";

pub const PACED_OWNED_FAKE_START_MAGIC: &[u8] = b"LAMINARDB_PACED_OWNED_FAKE_START_V1\0";
pub const PACED_OWNED_FAKE_ACK_MAGIC: &[u8] = b"LAMINARDB_PACED_OWNED_FAKE_ACK_V1\0";
pub const PACED_OWNED_FAKE_START_FRAME_BYTES: usize =
    PACED_OWNED_FAKE_START_MAGIC.len() + 16 + 32 + 16;
pub const PACED_OWNED_FAKE_ACK_FRAME_BYTES: usize =
    PACED_OWNED_FAKE_ACK_MAGIC.len() + 16 + 32 + 16 + 8;

const MAX_PACED_PLAN_BYTES: usize = 128 * 1_024;
const MAX_PACED_READY_BYTES: usize = 4 * 1_024;
const EXPECTED_OBSERVER_SLOTS: usize = 58;
const OBSERVER_INTERVAL_NS: u64 = 5_000_000_000;
const INPUT_TARGET_END_NS: u64 = 200_000_000_000;
const POST_RECOVERY_BASE_NS: u64 = 255_000_000_000;
const RELEASE_LATENESS_LIMIT_NS: u64 = 50_000_000;
const NODE_WORK_DEADLINE_NS: u64 = 4_500_000_000;
const LANE_QUIESCENCE_DEADLINE_NS: u64 = 4_750_000_000;
const RESULT_DEADLINE_NS: u64 = 290_000_000_000;
const LAST_LANE_DEADLINE_NS: u64 = 289_750_000_000;
const CLIENT_START_LIMIT: usize = 7;
const START_ROLLING_WINDOW_NS: u64 = 1_000_000_000;
const SYSTEM_CANCELLATION_POLL_NS: u64 = 50_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacedOwnedFakePolicyV1 {
    pub slot_interval_ns: u64,
    pub release_lateness_limit_ns: u64,
    pub start_ack_round_trip_limit_ns: u64,
    pub node_work_deadline_ns: u64,
    pub lane_quiescence_deadline_ns: u64,
    pub last_lane_deadline_ns: u64,
    pub result_deadline_ns: u64,
    pub client_start_limit: u8,
    pub server_start_limit: u8,
    pub start_rolling_window_ns: u64,
    pub retry_delay_ns: u64,
    pub max_request_attempts: u8,
    pub max_node_slot_starts: u8,
    pub max_timing_pages_per_slot: u8,
    pub max_timing_records_per_page: u8,
    pub max_timing_bytes_per_slot: u32,
    pub max_timing_records_per_slot: u16,
    pub max_result_bytes: u32,
    pub max_transcript_bytes: u32,
    pub max_attempt_rows: u16,
    pub max_retry_rows: u16,
    pub max_timing_pages: u16,
    pub max_timing_records: u32,
}

impl PacedOwnedFakePolicyV1 {
    pub const FROZEN: Self = Self {
        slot_interval_ns: OBSERVER_INTERVAL_NS,
        release_lateness_limit_ns: RELEASE_LATENESS_LIMIT_NS,
        start_ack_round_trip_limit_ns: RELEASE_LATENESS_LIMIT_NS,
        node_work_deadline_ns: NODE_WORK_DEADLINE_NS,
        lane_quiescence_deadline_ns: LANE_QUIESCENCE_DEADLINE_NS,
        last_lane_deadline_ns: LAST_LANE_DEADLINE_NS,
        result_deadline_ns: RESULT_DEADLINE_NS,
        client_start_limit: CLIENT_START_LIMIT as u8,
        server_start_limit: 8,
        start_rolling_window_ns: START_ROLLING_WINDOW_NS,
        retry_delay_ns: 100_000_000,
        max_request_attempts: 2,
        max_node_slot_starts: 8,
        max_timing_pages_per_slot: 6,
        max_timing_records_per_page: 64,
        max_timing_bytes_per_slot: 384 * 1_024,
        max_timing_records_per_slot: 384,
        max_result_bytes: 1_048_576,
        max_transcript_bytes: 134_217_728,
        max_attempt_rows: 1_392,
        max_retry_rows: 696,
        max_timing_pages: 1_044,
        max_timing_records: 66_816,
    };
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacedOwnedFakeDiagnosticPolicyV1 {
    pub schema_version: String,
    pub handler_concurrency_limit: u8,
    pub authenticated_start_limit: u8,
    pub start_rolling_window_ns: u64,
    pub handler_deadline_ns: u64,
    pub local_evidence_response_max_bytes: u32,
    pub timing_response_max_bytes: u32,
    pub timing_records_per_page: u8,
}

impl PacedOwnedFakeDiagnosticPolicyV1 {
    pub fn frozen() -> Self {
        Self {
            schema_version: PACED_OWNED_FAKE_DIAGNOSTIC_POLICY_SCHEMA.to_owned(),
            handler_concurrency_limit: 1,
            authenticated_start_limit: 8,
            start_rolling_window_ns: START_ROLLING_WINDOW_NS,
            handler_deadline_ns: 2_000_000_000,
            local_evidence_response_max_bytes: 4 * 1_024,
            timing_response_max_bytes: 64 * 1_024,
            timing_records_per_page: 64,
        }
    }

    pub fn canonical_sha256(&self) -> Result<String, PacedContractError> {
        if self != &Self::frozen() {
            return Err(PacedContractError::InvalidDiagnosticPolicy);
        }
        let bytes =
            serde_json::to_vec(self).map_err(|_| PacedContractError::InvalidDiagnosticPolicy)?;
        Ok(domain_hash(
            b"paced-owned-fake-diagnostic-policy/v1\0",
            &bytes,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedFakeProcessDescriptorV1 {
    pub node_ordinal: u8,
    pub stable_node_id: Uuid,
    pub boot_uuid: Uuid,
    pub process_term: u64,
    pub gate_instance_id: Uuid,
    pub server_sha256: String,
    pub configuration_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacedOwnedFakePlanV1 {
    pub schema_version: String,
    pub notice: String,
    pub execution_eligible: bool,
    pub arm: Arm,
    pub invocation_id: Uuid,
    pub base_plan_sha256: String,
    pub observer_schedule_sha256: String,
    pub observer_schedule: ObserverScheduleV1,
    pub diagnostic_policy: PacedOwnedFakeDiagnosticPolicyV1,
    pub diagnostic_policy_sha256: String,
    pub process_descriptors: [OwnedFakeProcessDescriptorV1; 3],
    pub protocol: PacedOwnedFakePolicyV1,
}

#[derive(Debug, Clone)]
pub struct PacedOwnedFakePlanExpectationV1 {
    expected_plan: PacedOwnedFakePlanV1,
    canonical_bytes: Vec<u8>,
    canonical_sha256: String,
}

impl PacedOwnedFakePlanExpectationV1 {
    pub fn expected_plan(&self) -> &PacedOwnedFakePlanV1 {
        &self.expected_plan
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub fn canonical_sha256(&self) -> &str {
        &self.canonical_sha256
    }
}

#[derive(Debug, Clone)]
pub struct ValidatedPacedOwnedFakePlanV1 {
    plan: PacedOwnedFakePlanV1,
    canonical_bytes: Vec<u8>,
    canonical_sha256: String,
}

impl ValidatedPacedOwnedFakePlanV1 {
    pub fn plan(&self) -> &PacedOwnedFakePlanV1 {
        &self.plan
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub fn canonical_sha256(&self) -> &str {
        &self.canonical_sha256
    }
}

pub fn bind_paced_owned_fake_plan(
    base_plan: &SealedPlanV1,
    arm: Arm,
    invocation_id: Uuid,
    process_descriptors: [OwnedFakeProcessDescriptorV1; 3],
) -> Result<PacedOwnedFakePlanExpectationV1, PacedContractError> {
    if process_descriptors.iter().any(|descriptor| {
        descriptor.server_sha256 != base_plan.plan().artifact_digests.server.sha256
            || descriptor.configuration_sha256
                != base_plan.plan().artifact_digests.redacted_config.sha256
    }) {
        return Err(PacedContractError::InvalidProcessDescriptors);
    }
    let diagnostic_policy = PacedOwnedFakeDiagnosticPolicyV1::frozen();
    let plan = PacedOwnedFakePlanV1 {
        schema_version: PACED_OWNED_FAKE_PLAN_SCHEMA.to_owned(),
        notice: NOTICE.to_owned(),
        execution_eligible: false,
        arm,
        invocation_id,
        base_plan_sha256: base_plan.sha256().to_owned(),
        observer_schedule_sha256: base_plan.observer_schedule_sha256().to_owned(),
        observer_schedule: base_plan.plan().observer_schedule.clone(),
        diagnostic_policy_sha256: diagnostic_policy.canonical_sha256()?,
        diagnostic_policy,
        process_descriptors,
        protocol: PacedOwnedFakePolicyV1::FROZEN,
    };
    validate_paced_owned_fake_plan(&plan)?;
    let canonical_bytes = serde_json::to_vec(&plan).map_err(|_| PacedContractError::InvalidPlan)?;
    let canonical_sha256 = domain_hash(b"paced-owned-fake-plan/v1\0", &canonical_bytes);
    Ok(PacedOwnedFakePlanExpectationV1 {
        expected_plan: plan,
        canonical_bytes,
        canonical_sha256,
    })
}

pub fn validate_paced_owned_fake_plan_bytes(
    bytes: &[u8],
    expectation: &PacedOwnedFakePlanExpectationV1,
) -> Result<ValidatedPacedOwnedFakePlanV1, PacedContractError> {
    if bytes.is_empty() || bytes.len() > MAX_PACED_PLAN_BYTES {
        return Err(PacedContractError::InvalidPlan);
    }
    let plan: PacedOwnedFakePlanV1 =
        serde_json::from_slice(bytes).map_err(|_| PacedContractError::InvalidPlan)?;
    let canonical = serde_json::to_vec(&plan).map_err(|_| PacedContractError::InvalidPlan)?;
    if canonical != bytes {
        return Err(PacedContractError::NonCanonicalEncoding);
    }
    validate_paced_owned_fake_plan(&plan)?;
    if &plan != expectation.expected_plan()
        || canonical != expectation.canonical_bytes()
        || domain_hash(b"paced-owned-fake-plan/v1\0", &canonical) != expectation.canonical_sha256()
    {
        return Err(PacedContractError::PlanBindingMismatch);
    }
    Ok(ValidatedPacedOwnedFakePlanV1 {
        plan,
        canonical_bytes: canonical,
        canonical_sha256: expectation.canonical_sha256.clone(),
    })
}

fn validate_paced_owned_fake_plan(plan: &PacedOwnedFakePlanV1) -> Result<(), PacedContractError> {
    if plan.schema_version != PACED_OWNED_FAKE_PLAN_SCHEMA
        || plan.notice != NOTICE
        || plan.execution_eligible
        || !is_rfc4122_v4(plan.invocation_id)
        || !is_lower_sha256(&plan.base_plan_sha256)
        || !is_lower_sha256(&plan.observer_schedule_sha256)
        || plan.protocol != PacedOwnedFakePolicyV1::FROZEN
        || plan.diagnostic_policy != PacedOwnedFakeDiagnosticPolicyV1::frozen()
        || plan.diagnostic_policy_sha256 != plan.diagnostic_policy.canonical_sha256()?
    {
        return Err(PacedContractError::InvalidPlan);
    }
    validate_observer_schedule(&plan.observer_schedule)?;
    let schedule_bytes =
        serde_json::to_vec(&plan.observer_schedule).map_err(|_| PacedContractError::InvalidPlan)?;
    if domain_hash(
        b"laminardb-instrumentation-ab-observer-schedule/v1\0",
        &schedule_bytes,
    ) != plan.observer_schedule_sha256
    {
        return Err(PacedContractError::InvalidPlan);
    }
    validate_process_descriptors(&plan.process_descriptors)
}

fn validate_process_descriptors(
    descriptors: &[OwnedFakeProcessDescriptorV1; 3],
) -> Result<(), PacedContractError> {
    let mut stable_ids = BTreeSet::new();
    let mut boot_ids = BTreeSet::new();
    let mut gate_ids = BTreeSet::new();
    let expected_server = &descriptors[0].server_sha256;
    let expected_configuration = &descriptors[0].configuration_sha256;
    for (index, descriptor) in descriptors.iter().enumerate() {
        if descriptor.node_ordinal != index as u8
            || !is_rfc4122_v4(descriptor.stable_node_id)
            || !is_rfc4122_v4(descriptor.boot_uuid)
            || !is_rfc4122_v4(descriptor.gate_instance_id)
            || descriptor.process_term == 0
            || !is_lower_sha256(&descriptor.server_sha256)
            || !is_lower_sha256(&descriptor.configuration_sha256)
            || &descriptor.server_sha256 != expected_server
            || &descriptor.configuration_sha256 != expected_configuration
            || !stable_ids.insert(descriptor.stable_node_id)
            || !boot_ids.insert(descriptor.boot_uuid)
            || !gate_ids.insert(descriptor.gate_instance_id)
        {
            return Err(PacedContractError::InvalidProcessDescriptors);
        }
    }
    Ok(())
}

fn validate_observer_schedule(schedule: &ObserverScheduleV1) -> Result<(), PacedContractError> {
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
        return Err(PacedContractError::InvalidSchedule);
    }
    for (index, slot) in schedule.slots.iter().enumerate() {
        let ordinal = u32::try_from(index).map_err(|_| PacedContractError::InvalidSchedule)?;
        let at_ns = u64::from(ordinal)
            .checked_mul(OBSERVER_INTERVAL_NS)
            .ok_or(PacedContractError::ArithmeticOverflow)?;
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
            return Err(PacedContractError::InvalidSchedule);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacedOwnedFakeReadyV1 {
    pub schema_version: String,
    pub notice: String,
    pub execution_eligible: bool,
    pub invocation_id: Uuid,
    pub paced_plan_sha256: String,
    pub process_descriptors_sha256: String,
}

pub fn build_paced_owned_fake_ready(
    plan: &ValidatedPacedOwnedFakePlanV1,
) -> Result<PacedOwnedFakeReadyV1, PacedContractError> {
    Ok(PacedOwnedFakeReadyV1 {
        schema_version: PACED_OWNED_FAKE_READY_SCHEMA.to_owned(),
        notice: NOTICE.to_owned(),
        execution_eligible: false,
        invocation_id: plan.plan.invocation_id,
        paced_plan_sha256: plan.canonical_sha256.clone(),
        process_descriptors_sha256: process_descriptor_digest(&plan.plan.process_descriptors)?,
    })
}

pub fn validate_paced_owned_fake_ready_bytes(
    bytes: &[u8],
    plan: &ValidatedPacedOwnedFakePlanV1,
) -> Result<PacedOwnedFakeReadyV1, PacedContractError> {
    if bytes.is_empty() || bytes.len() > MAX_PACED_READY_BYTES {
        return Err(PacedContractError::InvalidReady);
    }
    let ready: PacedOwnedFakeReadyV1 =
        serde_json::from_slice(bytes).map_err(|_| PacedContractError::InvalidReady)?;
    let canonical = serde_json::to_vec(&ready).map_err(|_| PacedContractError::InvalidReady)?;
    if canonical != bytes {
        return Err(PacedContractError::NonCanonicalEncoding);
    }
    if ready != build_paced_owned_fake_ready(plan)? {
        return Err(PacedContractError::InvalidReady);
    }
    Ok(ready)
}

fn process_descriptor_digest(
    descriptors: &[OwnedFakeProcessDescriptorV1; 3],
) -> Result<String, PacedContractError> {
    let bytes = serde_json::to_vec(descriptors)
        .map_err(|_| PacedContractError::InvalidProcessDescriptors)?;
    Ok(domain_hash(b"paced-owned-fake-process-set/v1\0", &bytes))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PacedOwnedFakeStartV1 {
    pub invocation_id: Uuid,
    pub paced_plan_sha256: String,
    pub start_nonce: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PacedOwnedFakeStartAckV1 {
    pub invocation_id: Uuid,
    pub paced_plan_sha256: String,
    pub start_nonce: [u8; 16],
    pub observer_anchor_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchoredPacedOwnedFakeStartV1 {
    start: PacedOwnedFakeStartV1,
    observer_anchor_ns: u64,
}

impl AnchoredPacedOwnedFakeStartV1 {
    pub fn start(&self) -> &PacedOwnedFakeStartV1 {
        &self.start
    }

    pub fn observer_anchor_ns(&self) -> u64 {
        self.observer_anchor_ns
    }
}

pub fn prepare_paced_owned_fake_start_frame(
    plan: &ValidatedPacedOwnedFakePlanV1,
    start_nonce: [u8; 16],
) -> Result<(PacedOwnedFakeStartV1, Vec<u8>), PacedContractError> {
    if start_nonce == [0; 16] {
        return Err(PacedContractError::InvalidStart);
    }
    let digest = decode_sha256(plan.canonical_sha256())?;
    let mut frame = Vec::with_capacity(PACED_OWNED_FAKE_START_FRAME_BYTES);
    frame.extend_from_slice(PACED_OWNED_FAKE_START_MAGIC);
    frame.extend_from_slice(plan.plan.invocation_id.as_bytes());
    frame.extend_from_slice(&digest);
    frame.extend_from_slice(&start_nonce);
    debug_assert_eq!(frame.len(), PACED_OWNED_FAKE_START_FRAME_BYTES);
    let start = PacedOwnedFakeStartV1 {
        invocation_id: plan.plan.invocation_id,
        paced_plan_sha256: plan.canonical_sha256.clone(),
        start_nonce,
    };
    Ok((start, frame))
}

fn parse_paced_owned_fake_start_frame(
    frame: &[u8],
    plan: &ValidatedPacedOwnedFakePlanV1,
) -> Result<PacedOwnedFakeStartV1, PacedContractError> {
    if frame.len() != PACED_OWNED_FAKE_START_FRAME_BYTES
        || !frame.starts_with(PACED_OWNED_FAKE_START_MAGIC)
    {
        return Err(PacedContractError::InvalidStart);
    }
    let mut cursor = PACED_OWNED_FAKE_START_MAGIC.len();
    let invocation_id = take_uuid(frame, &mut cursor).ok_or(PacedContractError::InvalidStart)?;
    let digest = take_array::<32>(frame, &mut cursor).ok_or(PacedContractError::InvalidStart)?;
    let start_nonce =
        take_array::<16>(frame, &mut cursor).ok_or(PacedContractError::InvalidStart)?;
    if cursor != frame.len()
        || invocation_id != plan.plan.invocation_id
        || encode_hex(&digest) != plan.canonical_sha256
        || start_nonce == [0; 16]
    {
        return Err(PacedContractError::InvalidStart);
    }
    Ok(PacedOwnedFakeStartV1 {
        invocation_id,
        paced_plan_sha256: plan.canonical_sha256.clone(),
        start_nonce,
    })
}

pub fn receive_and_anchor_paced_owned_fake_start(
    frame: &[u8],
    plan: &ValidatedPacedOwnedFakePlanV1,
    clock: &dyn MonotonicClock,
) -> Result<AnchoredPacedOwnedFakeStartV1, PacedStartReceiveError> {
    let start = parse_paced_owned_fake_start_frame(frame, plan)?;
    let observer_anchor_ns = clock.now_ns()?;
    Ok(AnchoredPacedOwnedFakeStartV1 {
        start,
        observer_anchor_ns,
    })
}

pub fn encode_paced_owned_fake_ack_frame(
    anchored: &AnchoredPacedOwnedFakeStartV1,
) -> Result<Vec<u8>, PacedContractError> {
    let start = anchored.start();
    if !is_rfc4122_v4(start.invocation_id) || start.start_nonce == [0; 16] {
        return Err(PacedContractError::InvalidAck);
    }
    let digest = decode_sha256(&start.paced_plan_sha256)?;
    let mut frame = Vec::with_capacity(PACED_OWNED_FAKE_ACK_FRAME_BYTES);
    frame.extend_from_slice(PACED_OWNED_FAKE_ACK_MAGIC);
    frame.extend_from_slice(start.invocation_id.as_bytes());
    frame.extend_from_slice(&digest);
    frame.extend_from_slice(&start.start_nonce);
    frame.extend_from_slice(&anchored.observer_anchor_ns.to_be_bytes());
    debug_assert_eq!(frame.len(), PACED_OWNED_FAKE_ACK_FRAME_BYTES);
    Ok(frame)
}

pub fn decode_paced_owned_fake_ack_frame(
    frame: &[u8],
    start: &PacedOwnedFakeStartV1,
) -> Result<PacedOwnedFakeStartAckV1, PacedContractError> {
    if frame.len() != PACED_OWNED_FAKE_ACK_FRAME_BYTES
        || !frame.starts_with(PACED_OWNED_FAKE_ACK_MAGIC)
    {
        return Err(PacedContractError::InvalidAck);
    }
    let mut cursor = PACED_OWNED_FAKE_ACK_MAGIC.len();
    let invocation_id = take_uuid(frame, &mut cursor).ok_or(PacedContractError::InvalidAck)?;
    let digest = take_array::<32>(frame, &mut cursor).ok_or(PacedContractError::InvalidAck)?;
    let start_nonce = take_array::<16>(frame, &mut cursor).ok_or(PacedContractError::InvalidAck)?;
    let observer_anchor_ns = take_array::<8>(frame, &mut cursor)
        .map(u64::from_be_bytes)
        .ok_or(PacedContractError::InvalidAck)?;
    if cursor != frame.len()
        || invocation_id != start.invocation_id
        || encode_hex(&digest) != start.paced_plan_sha256
        || start_nonce != start.start_nonce
    {
        return Err(PacedContractError::InvalidAck);
    }
    Ok(PacedOwnedFakeStartAckV1 {
        invocation_id,
        paced_plan_sha256: start.paced_plan_sha256.clone(),
        start_nonce,
        observer_anchor_ns,
    })
}

fn take_uuid(bytes: &[u8], cursor: &mut usize) -> Option<Uuid> {
    take_array::<16>(bytes, cursor).map(Uuid::from_bytes)
}

fn take_array<const N: usize>(bytes: &[u8], cursor: &mut usize) -> Option<[u8; N]> {
    let end = cursor.checked_add(N)?;
    let value = bytes.get(*cursor..end)?.try_into().ok()?;
    *cursor = end;
    Some(value)
}

fn decode_sha256(value: &str) -> Result<[u8; 32], PacedContractError> {
    if !is_lower_sha256(value) {
        return Err(PacedContractError::InvalidDigest);
    }
    let bytes = value.as_bytes();
    let mut decoded = [0_u8; 32];
    for (index, pair) in bytes.chunks_exact(2).enumerate() {
        decoded[index] = (decode_hex_nibble(pair[0]).ok_or(PacedContractError::InvalidDigest)?
            << 4)
            | decode_hex_nibble(pair[1]).ok_or(PacedContractError::InvalidDigest)?;
    }
    Ok(decoded)
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn is_rfc4122_v4(value: Uuid) -> bool {
    value.get_version_num() == 4 && value.get_variant() == uuid::Variant::RFC4122
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockWaitOutcome {
    Reached { observed_ns: u64 },
    Cancelled { observed_ns: u64 },
}

pub trait MonotonicClock: Send + Sync {
    fn now_ns(&self) -> Result<u64, PacedClockError>;

    fn wait_until(
        &self,
        target_ns: u64,
        cancellation: &ProtocolCancellation,
    ) -> Result<ClockWaitOutcome, PacedClockError>;
}

#[derive(Debug)]
pub struct SystemMonotonicClock {
    origin: Instant,
    cancellation_poll: Duration,
}

impl SystemMonotonicClock {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
            cancellation_poll: Duration::from_nanos(SYSTEM_CANCELLATION_POLL_NS),
        }
    }

    fn elapsed_ns(&self) -> Result<u64, PacedClockError> {
        self.origin
            .elapsed()
            .as_nanos()
            .try_into()
            .map_err(|_| PacedClockError::Overflow)
    }
}

impl Default for SystemMonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl MonotonicClock for SystemMonotonicClock {
    fn now_ns(&self) -> Result<u64, PacedClockError> {
        self.elapsed_ns()
    }

    fn wait_until(
        &self,
        target_ns: u64,
        cancellation: &ProtocolCancellation,
    ) -> Result<ClockWaitOutcome, PacedClockError> {
        loop {
            let observed_ns = self.elapsed_ns()?;
            if cancellation.is_cancelled() {
                return Ok(ClockWaitOutcome::Cancelled { observed_ns });
            }
            if observed_ns >= target_ns {
                return Ok(ClockWaitOutcome::Reached { observed_ns });
            }
            let remaining = Duration::from_nanos(target_ns - observed_ns);
            std::thread::sleep(remaining.min(self.cancellation_poll));
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartAckRoundTripDecision {
    Accepted {
        round_trip_ns: u64,
        anchor_uncertainty_ns: u64,
    },
    InvalidLate {
        round_trip_ns: u64,
    },
}

pub fn classify_start_ack_round_trip(
    start_send_ns: u64,
    ack_received_ns: u64,
) -> Result<StartAckRoundTripDecision, PacedClockError> {
    let round_trip_ns = ack_received_ns
        .checked_sub(start_send_ns)
        .ok_or(PacedClockError::ClockRegressed)?;
    if round_trip_ns <= RELEASE_LATENESS_LIMIT_NS {
        Ok(StartAckRoundTripDecision::Accepted {
            round_trip_ns,
            anchor_uncertainty_ns: round_trip_ns,
        })
    } else {
        Ok(StartAckRoundTripDecision::InvalidLate { round_trip_ns })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReleasedSlotWindow {
    pub target_ns: u64,
    pub actual_release_ns: u64,
    pub lateness_ns: u64,
    pub node_deadline_ns: u64,
    pub lane_quiescence_deadline_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbsoluteDeadlineDecision {
    Within,
    Exceeded,
}

pub fn classify_absolute_deadline(observed_ns: u64, deadline_ns: u64) -> AbsoluteDeadlineDecision {
    if observed_ns <= deadline_ns {
        AbsoluteDeadlineDecision::Within
    } else {
        AbsoluteDeadlineDecision::Exceeded
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotReleaseDecision {
    Released(ReleasedSlotWindow),
    Missed {
        target_ns: u64,
        observed_ns: u64,
        lateness_ns: u64,
    },
    Cancelled {
        target_ns: u64,
        observed_ns: u64,
    },
}

pub fn classify_slot_release(
    target_ns: u64,
    observed_ns: u64,
) -> Result<SlotReleaseDecision, PacedClockError> {
    if observed_ns < target_ns {
        return Err(PacedClockError::ClockRegressed);
    }
    let lateness_ns = observed_ns - target_ns;
    if lateness_ns > RELEASE_LATENESS_LIMIT_NS {
        return Ok(SlotReleaseDecision::Missed {
            target_ns,
            observed_ns,
            lateness_ns,
        });
    }
    Ok(SlotReleaseDecision::Released(ReleasedSlotWindow {
        target_ns,
        actual_release_ns: observed_ns,
        lateness_ns,
        node_deadline_ns: target_ns
            .checked_add(NODE_WORK_DEADLINE_NS)
            .ok_or(PacedClockError::Overflow)?,
        lane_quiescence_deadline_ns: target_ns
            .checked_add(LANE_QUIESCENCE_DEADLINE_NS)
            .ok_or(PacedClockError::Overflow)?,
    }))
}

pub fn wait_for_slot_release(
    clock: &dyn MonotonicClock,
    target_ns: u64,
    cancellation: &ProtocolCancellation,
) -> Result<SlotReleaseDecision, PacedClockError> {
    match clock.wait_until(target_ns, cancellation)? {
        ClockWaitOutcome::Reached { observed_ns } => classify_slot_release(target_ns, observed_ns),
        ClockWaitOutcome::Cancelled { observed_ns } => Ok(SlotReleaseDecision::Cancelled {
            target_ns,
            observed_ns,
        }),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartAdmission {
    Started { start_ns: u64 },
    WaitUntil(u64),
    RateDeferred { eligible_at_ns: u64 },
    DeadlineElapsed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollingStartShaper {
    starts_ns: VecDeque<u64>,
    last_observed_ns: Option<u64>,
    max_observed_in_window: u8,
}

impl RollingStartShaper {
    pub fn new() -> Self {
        Self {
            starts_ns: VecDeque::with_capacity(CLIENT_START_LIMIT),
            last_observed_ns: None,
            max_observed_in_window: 0,
        }
    }

    pub fn admit_start(
        &mut self,
        now_ns: u64,
        absolute_deadline_ns: u64,
    ) -> Result<StartAdmission, PacedClockError> {
        self.observe(now_ns)?;
        if now_ns >= absolute_deadline_ns {
            return Ok(StartAdmission::DeadlineElapsed);
        }
        if self.starts_ns.len() < CLIENT_START_LIMIT {
            return self.record_admitted_start(now_ns);
        }
        let eligible_at_ns = self
            .starts_ns
            .front()
            .copied()
            .ok_or(PacedClockError::InvalidShaperState)?
            .checked_add(START_ROLLING_WINDOW_NS)
            .ok_or(PacedClockError::Overflow)?;
        if eligible_at_ns >= absolute_deadline_ns {
            Ok(StartAdmission::RateDeferred { eligible_at_ns })
        } else if eligible_at_ns <= now_ns {
            self.record_admitted_start(now_ns)
        } else {
            Ok(StartAdmission::WaitUntil(eligible_at_ns))
        }
    }

    fn record_admitted_start(&mut self, now_ns: u64) -> Result<StartAdmission, PacedClockError> {
        now_ns
            .checked_add(START_ROLLING_WINDOW_NS)
            .ok_or(PacedClockError::Overflow)?;
        self.starts_ns.push_back(now_ns);
        self.max_observed_in_window = self
            .max_observed_in_window
            .max(u8::try_from(self.starts_ns.len()).map_err(|_| PacedClockError::Overflow)?);
        Ok(StartAdmission::Started { start_ns: now_ns })
    }

    pub fn retained_starts(&self) -> usize {
        self.starts_ns.len()
    }

    pub fn max_observed_in_window(&self) -> u8 {
        self.max_observed_in_window
    }

    fn observe(&mut self, now_ns: u64) -> Result<(), PacedClockError> {
        if self.last_observed_ns.is_some_and(|last| now_ns < last) {
            return Err(PacedClockError::ClockRegressed);
        }
        self.last_observed_ns = Some(now_ns);
        while let Some(start) = self.starts_ns.front().copied() {
            let expiry = start
                .checked_add(START_ROLLING_WINDOW_NS)
                .ok_or(PacedClockError::Overflow)?;
            if expiry > now_ns {
                break;
            }
            self.starts_ns.pop_front();
        }
        Ok(())
    }
}

impl Default for RollingStartShaper {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacedContractError {
    InvalidPlan,
    InvalidSchedule,
    InvalidDiagnosticPolicy,
    InvalidProcessDescriptors,
    InvalidReady,
    InvalidStart,
    InvalidAck,
    InvalidDigest,
    NonCanonicalEncoding,
    PlanBindingMismatch,
    ArithmeticOverflow,
}

impl fmt::Display for PacedContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "paced observer contract failed: {self:?}")
    }
}

impl std::error::Error for PacedContractError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacedStartReceiveError {
    Contract(PacedContractError),
    Clock(PacedClockError),
}

impl From<PacedContractError> for PacedStartReceiveError {
    fn from(error: PacedContractError) -> Self {
        Self::Contract(error)
    }
}

impl From<PacedClockError> for PacedStartReceiveError {
    fn from(error: PacedClockError) -> Self {
        Self::Clock(error)
    }
}

impl fmt::Display for PacedStartReceiveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "paced observer start receipt failed: {self:?}")
    }
}

impl std::error::Error for PacedStartReceiveError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacedClockError {
    Overflow,
    ClockRegressed,
    InvalidShaperState,
}

impl fmt::Display for PacedClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "paced observer clock failed: {self:?}")
    }
}

impl std::error::Error for PacedClockError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArtifactDigestsV1, BasePlanV1, ObserverPolicyV1, BASE_PLAN_SCHEMA};

    #[derive(Debug)]
    struct ScriptedClock {
        now_ns: u64,
        wait_observed_ns: u64,
    }

    impl MonotonicClock for ScriptedClock {
        fn now_ns(&self) -> Result<u64, PacedClockError> {
            Ok(self.now_ns)
        }

        fn wait_until(
            &self,
            _target_ns: u64,
            cancellation: &ProtocolCancellation,
        ) -> Result<ClockWaitOutcome, PacedClockError> {
            if cancellation.is_cancelled() {
                Ok(ClockWaitOutcome::Cancelled {
                    observed_ns: self.wait_observed_ns,
                })
            } else {
                Ok(ClockWaitOutcome::Reached {
                    observed_ns: self.wait_observed_ns,
                })
            }
        }
    }

    fn uuid(value: u128) -> Uuid {
        let mut bytes = value.to_be_bytes();
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    fn digest(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    fn schedule() -> ObserverScheduleV1 {
        ObserverScheduleV1 {
            slots: (0..EXPECTED_OBSERVER_SLOTS)
                .map(|index| {
                    let ordinal = index as u32;
                    let at_ns = u64::from(ordinal) * OBSERVER_INTERVAL_NS;
                    let boundary = match at_ns {
                        0 => Some(LifecycleBoundaryV1::WindowStart),
                        120_000_000_000 => Some(LifecycleBoundaryV1::FaultCheckpoint),
                        INPUT_TARGET_END_NS => Some(LifecycleBoundaryV1::InputTargetEnd),
                        POST_RECOVERY_BASE_NS => {
                            Some(LifecycleBoundaryV1::PostRecoverySamplingAnchor)
                        }
                        _ => None,
                    };
                    ObserverSlotV1 {
                        ordinal,
                        at_ns,
                        boundary,
                    }
                })
                .collect(),
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

    fn sealed_plan() -> SealedPlanV1 {
        let observer_schedule = schedule();
        let base = BasePlanV1 {
            schema_version: BASE_PLAN_SCHEMA.to_owned(),
            notice: NOTICE.to_owned(),
            execution_eligible: false,
            source_manifest_sha256: digest('1'),
            artifact_digests: ArtifactDigestsV1 {
                driver: artifact('2'),
                observer: artifact('3'),
                server: artifact('4'),
                trace_manifest: artifact('5'),
                redacted_config: artifact('6'),
                dependency_manifest: artifact('7'),
                virtual_control_script: artifact('8'),
                protocol_spec: artifact('9'),
            },
            node_ordinals: [0, 1, 2],
            workload: crate::WorkloadV1 {
                record_count: 80_000,
                records_per_second: 400,
                input_target_end_ns: INPUT_TARGET_END_NS,
            },
            driver_schedule: crate::DriverScheduleV1 {
                actions: Vec::new(),
            },
            observer_schedule,
        };
        let canonical_bytes = serde_json::to_vec(&base).unwrap();
        let driver_schedule_bytes = serde_json::to_vec(&base.driver_schedule).unwrap();
        let observer_schedule_bytes = serde_json::to_vec(&base.observer_schedule).unwrap();
        SealedPlanV1 {
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
            plan: base,
            canonical_bytes,
        }
    }

    fn artifact(hex: char) -> crate::ArtifactDigestV1 {
        crate::ArtifactDigestV1 {
            byte_length: 1,
            sha256: digest(hex),
        }
    }

    fn descriptors() -> [OwnedFakeProcessDescriptorV1; 3] {
        std::array::from_fn(|index| OwnedFakeProcessDescriptorV1 {
            node_ordinal: index as u8,
            stable_node_id: uuid(10 + index as u128),
            boot_uuid: uuid(20 + index as u128),
            process_term: 1,
            gate_instance_id: uuid(30 + index as u128),
            server_sha256: digest('4'),
            configuration_sha256: digest('6'),
        })
    }

    fn plan_pair() -> (
        PacedOwnedFakePlanExpectationV1,
        ValidatedPacedOwnedFakePlanV1,
    ) {
        let expectation = bind_paced_owned_fake_plan(
            &sealed_plan(),
            Arm::PollingTreatment,
            uuid(1),
            descriptors(),
        )
        .unwrap();
        let plan =
            validate_paced_owned_fake_plan_bytes(expectation.canonical_bytes(), &expectation)
                .unwrap();
        (expectation, plan)
    }

    fn validated_plan() -> ValidatedPacedOwnedFakePlanV1 {
        plan_pair().1
    }

    #[test]
    fn plan_is_canonical_and_separate_from_accelerated_schema() {
        let plan = validated_plan();
        assert_eq!(plan.plan.schema_version, PACED_OWNED_FAKE_PLAN_SCHEMA);
        assert_ne!(
            PACED_OWNED_FAKE_PLAN_SCHEMA,
            crate::observer_protocol::SANITIZED_PLAN_SCHEMA
        );
        assert!(!plan.plan.execution_eligible);
        assert_eq!(plan.plan.protocol, PacedOwnedFakePolicyV1::FROZEN);
        assert_eq!(
            plan.canonical_sha256(),
            "5c40a7706d70788700b8cfd17f83899f4a9ae4c4f1c8dedf07b33b7e0a96a4a8"
        );
        let (expectation, _) = plan_pair();
        assert_eq!(
            validate_paced_owned_fake_plan_bytes(plan.canonical_bytes(), &expectation)
                .unwrap()
                .canonical_sha256(),
            plan.canonical_sha256()
        );
    }

    #[test]
    fn plan_rejects_noncanonical_unknown_and_accelerated_json() {
        let (expectation, plan) = plan_pair();
        let mut spaced = plan.canonical_bytes().to_vec();
        spaced.insert(1, b' ');
        assert_eq!(
            validate_paced_owned_fake_plan_bytes(&spaced, &expectation).unwrap_err(),
            PacedContractError::NonCanonicalEncoding
        );

        let mut value: serde_json::Value = serde_json::from_slice(plan.canonical_bytes()).unwrap();
        value["unexpected"] = serde_json::json!(true);
        assert_eq!(
            validate_paced_owned_fake_plan_bytes(
                &serde_json::to_vec(&value).unwrap(),
                &expectation
            )
            .unwrap_err(),
            PacedContractError::InvalidPlan
        );

        let mut wrong_schema = plan.plan.clone();
        wrong_schema.schema_version = crate::observer_protocol::SANITIZED_PLAN_SCHEMA.to_owned();
        assert_eq!(
            validate_paced_owned_fake_plan_bytes(
                &serde_json::to_vec(&wrong_schema).unwrap(),
                &expectation
            )
            .unwrap_err(),
            PacedContractError::InvalidPlan
        );
        assert!(
            crate::observer_protocol::validate_sanitized_plan_bytes(plan.canonical_bytes())
                .is_err()
        );
    }

    #[test]
    fn plan_parser_rejects_empty_caps_trailing_and_duplicate_fields() {
        let (expectation, plan) = plan_pair();
        assert_eq!(
            validate_paced_owned_fake_plan_bytes(&[], &expectation).unwrap_err(),
            PacedContractError::InvalidPlan
        );
        assert_eq!(
            validate_paced_owned_fake_plan_bytes(&vec![b' '; MAX_PACED_PLAN_BYTES], &expectation)
                .unwrap_err(),
            PacedContractError::InvalidPlan
        );
        assert_eq!(
            validate_paced_owned_fake_plan_bytes(
                &vec![b' '; MAX_PACED_PLAN_BYTES + 1],
                &expectation
            )
            .unwrap_err(),
            PacedContractError::InvalidPlan
        );

        let mut trailing = plan.canonical_bytes().to_vec();
        trailing.push(b'\n');
        assert_eq!(
            validate_paced_owned_fake_plan_bytes(&trailing, &expectation).unwrap_err(),
            PacedContractError::NonCanonicalEncoding
        );

        let reordered: serde_json::Value = serde_json::from_slice(plan.canonical_bytes()).unwrap();
        assert_eq!(
            validate_paced_owned_fake_plan_bytes(
                &serde_json::to_vec(&reordered).unwrap(),
                &expectation
            )
            .unwrap_err(),
            PacedContractError::NonCanonicalEncoding
        );

        let mut duplicate = br#"{"schema_version":"paced-owned-fake-plan/v1","#.to_vec();
        duplicate.extend_from_slice(&plan.canonical_bytes()[1..]);
        assert_eq!(
            validate_paced_owned_fake_plan_bytes(&duplicate, &expectation).unwrap_err(),
            PacedContractError::InvalidPlan
        );
    }

    #[test]
    fn plan_requires_external_base_arm_invocation_and_descriptor_bindings() {
        let (expectation, plan) = plan_pair();
        let mut mutations = Vec::new();

        let mut wrong_base = plan.plan.clone();
        wrong_base.base_plan_sha256 = digest('a');
        mutations.push(wrong_base);

        let mut wrong_arm = plan.plan.clone();
        wrong_arm.arm = Arm::PollingControl;
        mutations.push(wrong_arm);

        let mut wrong_invocation = plan.plan.clone();
        wrong_invocation.invocation_id = uuid(99);
        mutations.push(wrong_invocation);

        let mut wrong_descriptors = plan.plan.clone();
        for descriptor in &mut wrong_descriptors.process_descriptors {
            descriptor.server_sha256 = digest('a');
        }
        mutations.push(wrong_descriptors);

        for mutation in mutations {
            assert_eq!(
                validate_paced_owned_fake_plan_bytes(
                    &serde_json::to_vec(&mutation).unwrap(),
                    &expectation
                )
                .unwrap_err(),
                PacedContractError::PlanBindingMismatch
            );
        }
    }

    #[test]
    fn plan_rejects_gate_alias_and_schedule_drift() {
        let (expectation, plan) = plan_pair();
        let mut aliased = plan.plan.clone();
        aliased.process_descriptors[2].gate_instance_id =
            aliased.process_descriptors[0].gate_instance_id;
        assert_eq!(
            validate_paced_owned_fake_plan_bytes(
                &serde_json::to_vec(&aliased).unwrap(),
                &expectation
            )
            .unwrap_err(),
            PacedContractError::InvalidProcessDescriptors
        );

        let mut drifted = plan.plan.clone();
        drifted.observer_schedule.slots[1].at_ns += 1;
        assert_eq!(
            validate_paced_owned_fake_plan_bytes(
                &serde_json::to_vec(&drifted).unwrap(),
                &expectation
            )
            .unwrap_err(),
            PacedContractError::InvalidSchedule
        );

        let mut wrong_artifact = descriptors();
        wrong_artifact[0].server_sha256 = digest('a');
        assert_eq!(
            bind_paced_owned_fake_plan(
                &sealed_plan(),
                Arm::PollingTreatment,
                uuid(1),
                wrong_artifact
            )
            .unwrap_err(),
            PacedContractError::InvalidProcessDescriptors
        );
    }

    #[test]
    fn ready_binds_plan_invocation_and_process_set() {
        let plan = validated_plan();
        let ready = build_paced_owned_fake_ready(&plan).unwrap();
        let bytes = serde_json::to_vec(&ready).unwrap();
        assert_eq!(
            validate_paced_owned_fake_ready_bytes(&bytes, &plan).unwrap(),
            ready
        );

        let mut wrong = ready;
        wrong.invocation_id = uuid(99);
        assert_eq!(
            validate_paced_owned_fake_ready_bytes(&serde_json::to_vec(&wrong).unwrap(), &plan)
                .unwrap_err(),
            PacedContractError::InvalidReady
        );
    }

    #[test]
    fn ready_rejects_noncanonical_unknown_duplicate_and_caps() {
        let plan = validated_plan();
        let ready = build_paced_owned_fake_ready(&plan).unwrap();
        let bytes = serde_json::to_vec(&ready).unwrap();

        for invalid in [Vec::new(), vec![b' '; MAX_PACED_READY_BYTES + 1]] {
            assert_eq!(
                validate_paced_owned_fake_ready_bytes(&invalid, &plan).unwrap_err(),
                PacedContractError::InvalidReady
            );
        }

        let mut trailing = bytes.clone();
        trailing.push(b'\n');
        assert_eq!(
            validate_paced_owned_fake_ready_bytes(&trailing, &plan).unwrap_err(),
            PacedContractError::NonCanonicalEncoding
        );

        let mut duplicate = br#"{"schema_version":"paced-owned-fake-ready/v1","#.to_vec();
        duplicate.extend_from_slice(&bytes[1..]);
        assert_eq!(
            validate_paced_owned_fake_ready_bytes(&duplicate, &plan).unwrap_err(),
            PacedContractError::InvalidReady
        );

        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["unexpected"] = serde_json::json!(true);
        assert_eq!(
            validate_paced_owned_fake_ready_bytes(&serde_json::to_vec(&value).unwrap(), &plan)
                .unwrap_err(),
            PacedContractError::InvalidReady
        );
    }

    #[test]
    fn fixed_start_and_ack_round_trip_exact_bindings() {
        let plan = validated_plan();
        let nonce = [7_u8; 16];
        let (expected_start, start_bytes) =
            prepare_paced_owned_fake_start_frame(&plan, nonce).unwrap();
        assert_eq!(start_bytes.len(), PACED_OWNED_FAKE_START_FRAME_BYTES);
        let clock = ScriptedClock {
            now_ns: 123,
            wait_observed_ns: 123,
        };
        let anchored =
            receive_and_anchor_paced_owned_fake_start(&start_bytes, &plan, &clock).unwrap();
        let start = anchored.start();
        assert_eq!(start, &expected_start);
        assert_eq!(start.start_nonce, nonce);
        assert_eq!(anchored.observer_anchor_ns(), 123);

        let ack_bytes = encode_paced_owned_fake_ack_frame(&anchored).unwrap();
        assert_eq!(ack_bytes.len(), PACED_OWNED_FAKE_ACK_FRAME_BYTES);
        let ack = decode_paced_owned_fake_ack_frame(&ack_bytes, start).unwrap();
        assert_eq!(ack.observer_anchor_ns, 123);
        assert_eq!(ack.start_nonce, nonce);
        assert_eq!(&ack_bytes[ack_bytes.len() - 8..], &123_u64.to_be_bytes());
    }

    #[test]
    fn fixed_frames_reject_truncation_extension_and_binding_mutation() {
        let plan = validated_plan();
        let (start, bytes) = prepare_paced_owned_fake_start_frame(&plan, [1; 16]).unwrap();
        let clock = ScriptedClock {
            now_ns: 1,
            wait_observed_ns: 1,
        };
        assert_eq!(
            PACED_OWNED_FAKE_START_MAGIC,
            b"LAMINARDB_PACED_OWNED_FAKE_START_V1\0"
        );
        assert_eq!(
            PACED_OWNED_FAKE_ACK_MAGIC,
            b"LAMINARDB_PACED_OWNED_FAKE_ACK_V1\0"
        );
        assert_eq!(
            encode_hex(&bytes),
            "4c414d494e415244425f50414345445f4f574e45445f46414b455f53544152545f563100000000000000400080000000000000015c40a7706d70788700b8cfd17f83899f4a9ae4c4f1c8dedf07b33b7e0a96a4a801010101010101010101010101010101"
        );
        for length in 0..bytes.len() {
            assert!(matches!(
                receive_and_anchor_paced_owned_fake_start(&bytes[..length], &plan, &clock),
                Err(PacedStartReceiveError::Contract(
                    PacedContractError::InvalidStart
                ))
            ));
        }
        let mut extended = bytes.clone();
        extended.push(0);
        assert_eq!(
            receive_and_anchor_paced_owned_fake_start(&extended, &plan, &clock).unwrap_err(),
            PacedStartReceiveError::Contract(PacedContractError::InvalidStart)
        );
        for offset in [
            0,
            PACED_OWNED_FAKE_START_MAGIC.len(),
            PACED_OWNED_FAKE_START_MAGIC.len() + 16,
        ] {
            let mut mutated = bytes.clone();
            mutated[offset] ^= 1;
            assert!(matches!(
                receive_and_anchor_paced_owned_fake_start(&mutated, &plan, &clock),
                Err(PacedStartReceiveError::Contract(
                    PacedContractError::InvalidStart
                ))
            ));
        }
        let mut zero_nonce = bytes.clone();
        zero_nonce[PACED_OWNED_FAKE_START_MAGIC.len() + 16 + 32..].fill(0);
        assert!(matches!(
            receive_and_anchor_paced_owned_fake_start(&zero_nonce, &plan, &clock),
            Err(PacedStartReceiveError::Contract(
                PacedContractError::InvalidStart
            ))
        ));

        let other_expectation = bind_paced_owned_fake_plan(
            &sealed_plan(),
            Arm::PollingTreatment,
            uuid(2),
            descriptors(),
        )
        .unwrap();
        let other_plan = validate_paced_owned_fake_plan_bytes(
            other_expectation.canonical_bytes(),
            &other_expectation,
        )
        .unwrap();
        assert!(matches!(
            receive_and_anchor_paced_owned_fake_start(&bytes, &other_plan, &clock),
            Err(PacedStartReceiveError::Contract(
                PacedContractError::InvalidStart
            ))
        ));

        let (_, clean_start_bytes) =
            prepare_paced_owned_fake_start_frame(&plan, start.start_nonce).unwrap();
        let anchored =
            receive_and_anchor_paced_owned_fake_start(&clean_start_bytes, &plan, &clock).unwrap();
        let ack = encode_paced_owned_fake_ack_frame(&anchored).unwrap();
        assert_eq!(
            encode_hex(&ack),
            "4c414d494e415244425f50414345445f4f574e45445f46414b455f41434b5f563100000000000000400080000000000000015c40a7706d70788700b8cfd17f83899f4a9ae4c4f1c8dedf07b33b7e0a96a4a8010101010101010101010101010101010000000000000001"
        );

        let mut different_nonce = clean_start_bytes.clone();
        different_nonce[PACED_OWNED_FAKE_START_MAGIC.len() + 16 + 32] ^= 1;
        let differently_anchored =
            receive_and_anchor_paced_owned_fake_start(&different_nonce, &plan, &clock).unwrap();
        let different_ack = encode_paced_owned_fake_ack_frame(&differently_anchored).unwrap();
        assert_eq!(
            decode_paced_owned_fake_ack_frame(&different_ack, &start).unwrap_err(),
            PacedContractError::InvalidAck
        );

        for length in 0..ack.len() {
            assert_eq!(
                decode_paced_owned_fake_ack_frame(&ack[..length], &start).unwrap_err(),
                PacedContractError::InvalidAck
            );
        }
        let mut extended_ack = ack.clone();
        extended_ack.push(0);
        assert_eq!(
            decode_paced_owned_fake_ack_frame(&extended_ack, &start).unwrap_err(),
            PacedContractError::InvalidAck
        );
        for offset in [
            0,
            PACED_OWNED_FAKE_ACK_MAGIC.len(),
            PACED_OWNED_FAKE_ACK_MAGIC.len() + 16,
            PACED_OWNED_FAKE_ACK_MAGIC.len() + 16 + 32,
        ] {
            let mut mutated_ack = ack.clone();
            mutated_ack[offset] ^= 1;
            assert_eq!(
                decode_paced_owned_fake_ack_frame(&mutated_ack, &start).unwrap_err(),
                PacedContractError::InvalidAck
            );
        }
    }

    #[test]
    fn zero_nonce_is_rejected() {
        let plan = validated_plan();
        assert_eq!(
            prepare_paced_owned_fake_start_frame(&plan, [0; 16]).unwrap_err(),
            PacedContractError::InvalidStart
        );
    }

    #[test]
    fn release_boundary_is_inclusive_at_fifty_milliseconds() {
        let target = 5_000_000_000;
        for lateness in [49_000_000, 50_000_000] {
            assert!(matches!(
                classify_slot_release(target, target + lateness).unwrap(),
                SlotReleaseDecision::Released(_)
            ));
        }
        assert!(matches!(
            classify_slot_release(target, target + 51_000_000).unwrap(),
            SlotReleaseDecision::Missed { .. }
        ));
        assert_eq!(
            classify_slot_release(target, target - 1).unwrap_err(),
            PacedClockError::ClockRegressed
        );
    }

    #[test]
    fn start_ack_round_trip_boundary_is_inclusive_at_fifty_milliseconds() {
        for elapsed in [49_000_000, 50_000_000] {
            assert_eq!(
                classify_start_ack_round_trip(1_000, 1_000 + elapsed).unwrap(),
                StartAckRoundTripDecision::Accepted {
                    round_trip_ns: elapsed,
                    anchor_uncertainty_ns: elapsed
                }
            );
        }
        assert_eq!(
            classify_start_ack_round_trip(1_000, 51_001_000).unwrap(),
            StartAckRoundTripDecision::InvalidLate {
                round_trip_ns: 51_000_000
            }
        );
        assert_eq!(
            classify_start_ack_round_trip(1_000, 999).unwrap_err(),
            PacedClockError::ClockRegressed
        );
    }

    #[test]
    fn release_deadlines_are_absolute_and_overflow_checked() {
        let released = classify_slot_release(5_000_000_000, 5_050_000_000).unwrap();
        let SlotReleaseDecision::Released(window) = released else {
            panic!("expected released slot");
        };
        assert_eq!(window.node_deadline_ns, 9_500_000_000);
        assert_eq!(window.lane_quiescence_deadline_ns, 9_750_000_000);
        assert_eq!(
            classify_slot_release(u64::MAX - 10, u64::MAX - 10).unwrap_err(),
            PacedClockError::Overflow
        );
        for deadline in [window.node_deadline_ns, window.lane_quiescence_deadline_ns] {
            assert_eq!(
                classify_absolute_deadline(deadline - 1, deadline),
                AbsoluteDeadlineDecision::Within
            );
            assert_eq!(
                classify_absolute_deadline(deadline, deadline),
                AbsoluteDeadlineDecision::Within
            );
            assert_eq!(
                classify_absolute_deadline(deadline + 1, deadline),
                AbsoluteDeadlineDecision::Exceeded
            );
        }
        assert_eq!(
            PacedOwnedFakePolicyV1::FROZEN.last_lane_deadline_ns,
            289_750_000_000
        );
        assert_eq!(
            PacedOwnedFakePolicyV1::FROZEN.result_deadline_ns,
            290_000_000_000
        );
    }

    #[test]
    fn injected_clock_classifies_absolute_target_and_cancellation() {
        let clock = ScriptedClock {
            now_ns: 0,
            wait_observed_ns: 100,
        };
        let cancellation = ProtocolCancellation::default();
        assert!(matches!(
            wait_for_slot_release(&clock, 100, &cancellation).unwrap(),
            SlotReleaseDecision::Released(ReleasedSlotWindow {
                actual_release_ns: 100,
                ..
            })
        ));

        let cancelled_clock = ScriptedClock {
            now_ns: 0,
            wait_observed_ns: 99,
        };
        let cancelled = ProtocolCancellation::default();
        cancelled.cancel();
        assert_eq!(
            wait_for_slot_release(&cancelled_clock, 100, &cancelled).unwrap(),
            SlotReleaseDecision::Cancelled {
                target_ns: 100,
                observed_ns: 99
            }
        );
    }

    #[test]
    fn system_clock_is_monotonic_and_pre_cancelled_wait_does_not_sleep() {
        let clock = SystemMonotonicClock::new();
        let first = clock.now_ns().unwrap();
        let second = clock.now_ns().unwrap();
        assert!(second >= first);
        let cancellation = ProtocolCancellation::default();
        cancellation.cancel();
        assert!(matches!(
            clock.wait_until(u64::MAX, &cancellation).unwrap(),
            ClockWaitOutcome::Cancelled { .. }
        ));
    }

    #[test]
    fn shaper_allows_seven_and_defers_the_eighth() {
        let mut shaper = RollingStartShaper::new();
        for _ in 0..7 {
            assert_eq!(
                shaper.admit_start(0, 2_000_000_000).unwrap(),
                StartAdmission::Started { start_ns: 0 }
            );
        }
        assert_eq!(shaper.max_observed_in_window(), 7);
        assert_eq!(
            shaper.admit_start(7, 2_000_000_000).unwrap(),
            StartAdmission::WaitUntil(START_ROLLING_WINDOW_NS)
        );
        assert_eq!(shaper.retained_starts(), 7);
    }

    #[test]
    fn shaper_window_has_open_lower_bound() {
        let mut shaper = RollingStartShaper::new();
        for _ in 0..7 {
            assert!(matches!(
                shaper.admit_start(0, 2_000_000_000).unwrap(),
                StartAdmission::Started { .. }
            ));
        }
        assert_eq!(
            shaper
                .admit_start(START_ROLLING_WINDOW_NS - 1, 2_000_000_000)
                .unwrap(),
            StartAdmission::WaitUntil(START_ROLLING_WINDOW_NS)
        );
        assert_eq!(
            shaper
                .admit_start(START_ROLLING_WINDOW_NS, 2_000_000_000)
                .unwrap(),
            StartAdmission::Started {
                start_ns: START_ROLLING_WINDOW_NS
            }
        );
        assert_eq!(shaper.retained_starts(), 1);
    }

    #[test]
    fn shaper_defers_at_absolute_deadline_and_rejects_clock_regression() {
        let mut shaper = RollingStartShaper::new();
        for _ in 0..7 {
            shaper.admit_start(0, 2_000_000_000).unwrap();
        }
        assert_eq!(
            shaper.admit_start(7, START_ROLLING_WINDOW_NS).unwrap(),
            StartAdmission::RateDeferred {
                eligible_at_ns: START_ROLLING_WINDOW_NS
            }
        );
        assert_eq!(
            shaper
                .admit_start(START_ROLLING_WINDOW_NS, START_ROLLING_WINDOW_NS)
                .unwrap(),
            StartAdmission::DeadlineElapsed
        );
        assert_eq!(
            shaper.admit_start(6, 2_000_000_000).unwrap_err(),
            PacedClockError::ClockRegressed
        );
    }

    #[test]
    fn shaper_uses_absolute_deadline_boundaries_without_catch_up() {
        fn saturated() -> RollingStartShaper {
            let mut shaper = RollingStartShaper::new();
            for _ in 0..7 {
                shaper.admit_start(0, 2_000_000_000).unwrap();
            }
            shaper
        }

        assert_eq!(
            saturated()
                .admit_start(1, START_ROLLING_WINDOW_NS + 1)
                .unwrap(),
            StartAdmission::WaitUntil(START_ROLLING_WINDOW_NS)
        );
        assert_eq!(
            saturated().admit_start(1, START_ROLLING_WINDOW_NS).unwrap(),
            StartAdmission::RateDeferred {
                eligible_at_ns: START_ROLLING_WINDOW_NS
            }
        );
        assert_eq!(
            saturated()
                .admit_start(1, START_ROLLING_WINDOW_NS - 1)
                .unwrap(),
            StartAdmission::RateDeferred {
                eligible_at_ns: START_ROLLING_WINDOW_NS
            }
        );

        let mut cross_slot = RollingStartShaper::new();
        for _ in 0..7 {
            cross_slot
                .admit_start(4_900_000_000, 9_500_000_000)
                .unwrap();
        }
        assert_eq!(
            cross_slot
                .admit_start(5_000_000_000, 9_500_000_000)
                .unwrap(),
            StartAdmission::WaitUntil(5_900_000_000)
        );
        assert_eq!(cross_slot.retained_starts(), 7);
    }

    #[test]
    fn shaper_rejects_timestamp_expiry_overflow() {
        let mut shaper = RollingStartShaper::new();
        assert_eq!(
            shaper.admit_start(u64::MAX - 1, u64::MAX).unwrap_err(),
            PacedClockError::Overflow
        );
        assert_eq!(shaper.retained_starts(), 0);
    }
}
