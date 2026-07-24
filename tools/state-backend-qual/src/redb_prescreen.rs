//! Content-only validation for the redb prescreen protected-review documents.
//!
//! This module deliberately accepts exact JSON bytes, not a packet directory. It performs no
//! provider access, artifact dereference, candidate execution, disposition classification, or
//! evidence sealing. A valid copied receipt remains authorization-unverified.

use std::fmt::{Display, Write as _};

use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::{decode_unique_json, reject_non_u64_numbers, reject_placeholder_strings, CheckErrors};

pub const MAX_REDB_REVIEW_POLICY_BYTES: usize = 32 * 1024;
pub const MAX_REDB_APPROVAL_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_REDB_REVIEW_RECEIPT_BYTES: usize = 64 * 1024;

const POLICY_SCHEMA: &str =
    include_str!("../schema/redb-prescreen-protected-review-policy-v1.schema.json");
const PAYLOAD_SCHEMA: &str =
    include_str!("../schema/redb-prescreen-approval-payload-v1.schema.json");
const RECEIPT_SCHEMA: &str =
    include_str!("../schema/redb-prescreen-protected-review-receipt-v1.schema.json");

const PRE_RUN_DECISION: &str = "APPROVE_REDB_PRESCREEN_EXECUTION_V1";
const MAX_JSON_DEPTH: usize = 16;
const MAX_JSON_NODES: usize = 4_096;
const MAX_NON_FIXTURE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_BASE_256M_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_BASE_1G_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAX_BASE_4G_BYTES: u64 = 12 * 1024 * 1024 * 1024;
const MAX_PRE_RUN_DECLARED_BYTES: u64 = 20 * 1024 * 1024 * 1024;
const REDB_CRATE_BYTES: u64 = 188_200;
const REDB_CRATE_SHA256: &str = "8e925444704b5f17d32bf42f5b6e2df050bceebc3dcd6e71cc73dafe8092e839";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RedbPrescreenAuthorization {
    Unverified,
}

impl Display for RedbPrescreenAuthorization {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unverified => formatter.write_str("authorization_unverified"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RedbPrescreenContentSummary {
    pub payload_id: String,
    pub authorization: RedbPrescreenAuthorization,
}

impl RedbPrescreenContentSummary {
    #[must_use]
    pub const fn execution_authorized(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn result_sealing_authorized(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    role: String,
    locator: String,
    byte_length: u64,
    sha256: String,
    media_type: String,
}

#[derive(Clone, Copy)]
struct ExpectedArtifact {
    role: &'static str,
    locator: &'static str,
    media_type: &'static str,
    maximum_bytes: u64,
}

const EXPECTED_ARTIFACTS: [ExpectedArtifact; 28] = [
    artifact(
        "redb-prescreen-protocol",
        "contract/protocol.md",
        "text/markdown; charset=utf-8",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-exact-source-mechanism-note",
        "contract/redb-mechanism-note.md",
        "text/markdown; charset=utf-8",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-wire-schemas",
        "contract/wire-schemas.tar.zst",
        "application/zstd",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-literal-goldens",
        "contract/literal-goldens.tar.zst",
        "application/zstd",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-fixture-recipe",
        "contract/fixture-recipe.json",
        "application/json",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-execution-plan",
        "contract/execution-plan.json",
        "application/json",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-candidate-configuration",
        "contract/candidate-configuration.json",
        "application/json",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-target-identity-policy",
        "contract/target-identity.json",
        "application/json",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-preflight-policy",
        "contract/preflight-policy.json",
        "application/json",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-schedule",
        "contract/schedule.json",
        "application/json",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-clock-isolation-policy",
        "contract/clock-isolation-policy.json",
        "application/json",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-trigger-delay-policy",
        "contract/trigger-delay-policy.json",
        "application/json",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-bounds",
        "contract/bounds.json",
        "application/json",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-protected-review-policy",
        "contract/protected-review-policy.json",
        "application/json",
        MAX_REDB_REVIEW_POLICY_BYTES as u64,
    ),
    artifact(
        "redb-4.1.0-crate-archive",
        "subject/redb-4.1.0.crate",
        "application/octet-stream",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-source",
        "build/source.tar.zst",
        "application/zstd",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-cargo-lock",
        "build/Cargo.lock",
        "text/plain; charset=utf-8",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-sbom",
        "build/sbom.spdx.json",
        "application/spdx+json",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-build-manifest",
        "build/build-manifest.json",
        "application/json",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-fixture-generator",
        "build/redb-prescreen-fixture-generator",
        "application/octet-stream",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-supervisor",
        "build/redb-prescreen-supervisor",
        "application/octet-stream",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-child",
        "build/redb-prescreen-child",
        "application/octet-stream",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-actuator",
        "build/redb-prescreen-actuator",
        "application/octet-stream",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-oracle",
        "build/redb-prescreen-oracle",
        "application/octet-stream",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-verifier",
        "build/redb-prescreen-verifier",
        "application/octet-stream",
        MAX_NON_FIXTURE_BYTES,
    ),
    artifact(
        "redb-prescreen-base-256m",
        "fixtures/base-256m.redb",
        "application/octet-stream",
        MAX_BASE_256M_BYTES,
    ),
    artifact(
        "redb-prescreen-base-1g",
        "fixtures/base-1g.redb",
        "application/octet-stream",
        MAX_BASE_1G_BYTES,
    ),
    artifact(
        "redb-prescreen-base-4g",
        "fixtures/base-4g.redb",
        "application/octet-stream",
        MAX_BASE_4G_BYTES,
    ),
];

const fn artifact(
    role: &'static str,
    locator: &'static str,
    media_type: &'static str,
    maximum_bytes: u64,
) -> ExpectedArtifact {
    ExpectedArtifact {
        role,
        locator,
        media_type,
        maximum_bytes,
    }
}

pub fn validate_redb_prescreen_pre_run_content(
    policy_bytes: &[u8],
    payload_bytes: &[u8],
    receipt_bytes: &[u8],
) -> Result<RedbPrescreenContentSummary, CheckErrors> {
    let policy = decode_contract(
        policy_bytes,
        MAX_REDB_REVIEW_POLICY_BYTES,
        "redb protected-review policy",
        POLICY_SCHEMA,
    )?;
    let payload = decode_contract(
        payload_bytes,
        MAX_REDB_APPROVAL_PAYLOAD_BYTES,
        "redb approval payload",
        PAYLOAD_SCHEMA,
    )?;
    let receipt = decode_contract(
        receipt_bytes,
        MAX_REDB_REVIEW_RECEIPT_BYTES,
        "redb protected-review receipt",
        RECEIPT_SCHEMA,
    )?;

    let mut errors = Vec::new();
    check_policy(&policy, &mut errors);
    let policy_descriptor = check_payload(&payload, policy_bytes, &mut errors);
    check_pre_run_receipt(
        &policy,
        &payload,
        &receipt,
        policy_bytes,
        payload_bytes,
        policy_descriptor.as_ref(),
        &mut errors,
    );
    if !errors.is_empty() {
        return Err(CheckErrors::many(errors));
    }

    Ok(RedbPrescreenContentSummary {
        payload_id: text(&payload, "/payload_id").to_owned(),
        authorization: RedbPrescreenAuthorization::Unverified,
    })
}

fn decode_contract(
    bytes: &[u8],
    maximum_bytes: usize,
    label: &str,
    schema_source: &str,
) -> Result<Value, CheckErrors> {
    let value = decode_unique_json(bytes, maximum_bytes, label)?;
    check_json_shape(&value, label)?;

    let schema: Value = serde_json::from_str(schema_source)
        .map_err(|error| CheckErrors::one(format!("decode embedded {label} schema: {error}")))?;
    let validator = jsonschema::validator_for(&schema)
        .map_err(|error| CheckErrors::one(format!("compile embedded {label} schema: {error}")))?;
    let schema_errors = validator
        .iter_errors(&value)
        .map(|error| format!("{label} schema {}: {error}", error.instance_path()))
        .collect::<Vec<_>>();
    if !schema_errors.is_empty() {
        return Err(CheckErrors::many(schema_errors));
    }

    let mut errors = Vec::new();
    reject_placeholder_strings(&value, "", &mut errors);
    reject_non_u64_numbers(&value, "", &mut errors);
    if !errors.is_empty() {
        return Err(CheckErrors::many(errors));
    }
    Ok(value)
}

fn check_json_shape(value: &Value, label: &str) -> Result<(), CheckErrors> {
    fn visit(value: &Value, depth: usize, nodes: &mut usize) -> Result<(), &'static str> {
        if depth > MAX_JSON_DEPTH {
            return Err("nesting depth exceeds 16");
        }
        *nodes = nodes.checked_add(1).ok_or("node count overflow")?;
        if *nodes > MAX_JSON_NODES {
            return Err("node count exceeds 4096");
        }
        match value {
            Value::Array(values) => {
                for child in values {
                    visit(child, depth + 1, nodes)?;
                }
            }
            Value::Object(values) => {
                for child in values.values() {
                    visit(child, depth + 1, nodes)?;
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
        Ok(())
    }

    let mut nodes = 0;
    visit(value, 1, &mut nodes).map_err(|reason| CheckErrors::one(format!("{label} {reason}")))
}

fn check_policy(policy: &Value, errors: &mut Vec<String>) {
    let workload_group = text(policy, "/review_groups/0/group_id");
    let operations_group = text(policy, "/review_groups/1/group_id");
    if workload_group == operations_group {
        errors.push("policy review group IDs must be distinct".to_owned());
    }
}

fn check_payload(
    payload: &Value,
    policy_bytes: &[u8],
    errors: &mut Vec<String>,
) -> Option<Descriptor> {
    let artifacts = payload
        .pointer("/artifacts")
        .and_then(Value::as_array)
        .unwrap_or_else(|| unreachable!("schema requires artifacts"));
    let mut total = 0_u64;
    let mut policy_descriptor = None;

    for (index, (value, expected)) in artifacts.iter().zip(EXPECTED_ARTIFACTS).enumerate() {
        let descriptor = descriptor(value, &format!("payload artifact {index}"), errors);
        let Some(descriptor) = descriptor else {
            continue;
        };
        check_expected_descriptor(&descriptor, expected, index, errors);
        total = match total.checked_add(descriptor.byte_length) {
            Some(value) => value,
            None => {
                errors.push("approval descriptor byte-length sum overflows u64".to_owned());
                total
            }
        };
        if index == 13 {
            check_descriptor_bytes(
                &descriptor,
                policy_bytes,
                "payload policy descriptor",
                errors,
            );
            policy_descriptor = Some(descriptor.clone());
        }
        if index == 14
            && (descriptor.byte_length != REDB_CRATE_BYTES
                || descriptor.sha256 != REDB_CRATE_SHA256)
        {
            errors.push("redb crate descriptor does not bind the exact 4.1.0 archive".to_owned());
        }
    }

    if let Some(value) = payload.pointer("/prior_smoke_result") {
        if !value.is_null() {
            if let Some(descriptor) = descriptor(value, "prior smoke descriptor", errors) {
                check_descriptor_tuple(
                    &descriptor,
                    artifact(
                        "redb-prescreen-reviewed-smoke-result",
                        "evidence/prior-smoke-result.json",
                        "application/json",
                        MAX_NON_FIXTURE_BYTES,
                    ),
                    "prior smoke descriptor",
                    errors,
                );
                total = match total.checked_add(descriptor.byte_length) {
                    Some(value) => value,
                    None => {
                        errors.push("approval descriptor byte-length sum overflows u64".to_owned());
                        total
                    }
                };
            }
        }
    }

    if total > MAX_PRE_RUN_DECLARED_BYTES {
        errors.push(format!(
            "approval descriptors declare {total} bytes; maximum aggregate is {MAX_PRE_RUN_DECLARED_BYTES}"
        ));
    }
    policy_descriptor
}

fn check_expected_descriptor(
    descriptor: &Descriptor,
    expected: ExpectedArtifact,
    index: usize,
    errors: &mut Vec<String>,
) {
    check_descriptor_tuple(
        descriptor,
        expected,
        &format!("payload artifact {index}"),
        errors,
    );
}

fn check_descriptor_tuple(
    descriptor: &Descriptor,
    expected: ExpectedArtifact,
    label: &str,
    errors: &mut Vec<String>,
) {
    if descriptor.role != expected.role {
        errors.push(format!(
            "{label} role must be `{}`, found `{}`",
            expected.role, descriptor.role
        ));
    }
    if descriptor.locator != expected.locator {
        errors.push(format!(
            "{label} locator must be `{}`, found `{}`",
            expected.locator, descriptor.locator
        ));
    }
    if descriptor.media_type != expected.media_type {
        errors.push(format!(
            "{label} media type must be `{}`, found `{}`",
            expected.media_type, descriptor.media_type
        ));
    }
    if descriptor.byte_length > expected.maximum_bytes {
        errors.push(format!(
            "{label} declares {} bytes; maximum is {}",
            descriptor.byte_length, expected.maximum_bytes
        ));
    }
}

fn check_pre_run_receipt(
    policy: &Value,
    payload: &Value,
    receipt: &Value,
    policy_bytes: &[u8],
    payload_bytes: &[u8],
    payload_policy_descriptor: Option<&Descriptor>,
    errors: &mut Vec<String>,
) {
    if text(receipt, "/stage") != "pre_run" {
        errors.push("content validator accepts only a pre_run receipt".to_owned());
        return;
    }

    if receipt.pointer("/provider") != policy.pointer("/provider") {
        errors.push("receipt provider does not equal the protected-review policy".to_owned());
    }
    if text(receipt, "/change/base_ref") != text(policy, "/provider/base_ref") {
        errors.push("receipt base ref does not equal the protected-review policy".to_owned());
    }
    for field in ["workflow_file", "job_name", "environment_id"] {
        let receipt_pointer = format!("/protected_execution/{field}");
        let policy_pointer = format!("/protected_execution/{field}");
        if text(receipt, &receipt_pointer) != text(policy, &policy_pointer) {
            errors.push(format!(
                "receipt protected execution {field} does not equal the policy"
            ));
        }
    }

    let receipt_policy = descriptor_at(receipt, "/policy", "receipt policy", errors);
    if let Some(descriptor) = receipt_policy.as_ref() {
        check_descriptor_tuple(
            descriptor,
            artifact(
                "redb-prescreen-protected-review-policy",
                "contract/protected-review-policy.json",
                "application/json",
                MAX_REDB_REVIEW_POLICY_BYTES as u64,
            ),
            "receipt policy descriptor",
            errors,
        );
        check_descriptor_bytes(
            descriptor,
            policy_bytes,
            "receipt policy descriptor",
            errors,
        );
        if payload_policy_descriptor
            .is_some_and(|payload_descriptor| payload_descriptor != descriptor)
        {
            errors.push(
                "payload and receipt policy descriptors do not bind identical bytes".to_owned(),
            );
        }
    }

    let receipt_payload = descriptor_at(receipt, "/payload", "receipt payload", errors);
    if let Some(descriptor) = receipt_payload.as_ref() {
        check_descriptor_tuple(
            descriptor,
            artifact(
                "redb-prescreen-approval-payload",
                "approval/payload.json",
                "application/json",
                MAX_REDB_APPROVAL_PAYLOAD_BYTES as u64,
            ),
            "receipt payload descriptor",
            errors,
        );
        check_descriptor_bytes(
            descriptor,
            payload_bytes,
            "receipt payload descriptor",
            errors,
        );
    }

    let expected_payload_length = u64::try_from(payload_bytes.len()).ok();
    let expected_payload_sha = sha256_hex(payload_bytes);
    let expected_head = text(receipt, "/change/head_revision");
    let verified_at = text(receipt, "/protected_execution/provider_verified_at_utc");
    check_canonical_utc(verified_at, "provider verification timestamp", errors);

    let reviews = receipt
        .pointer("/reviews")
        .and_then(Value::as_array)
        .unwrap_or_else(|| unreachable!("schema requires reviews"));
    if text(&reviews[0], "/stable_account_id") == text(&reviews[1], "/stable_account_id") {
        errors.push("receipt review stable account IDs must be distinct".to_owned());
    }
    if text(&reviews[0], "/review_event_id") == text(&reviews[1], "/review_event_id") {
        errors.push("receipt review event IDs must be distinct".to_owned());
    }
    for (index, review) in reviews.iter().enumerate() {
        if text(review, "/decision_literal") != PRE_RUN_DECISION
            || text(review, "/decision_literal") != text(payload, "/required_decision_literal")
            || text(review, "/decision_literal") != text(policy, "/decision_literals/pre_run")
        {
            errors.push(format!(
                "receipt review {index} decision does not equal the bound pre-run decision"
            ));
        }
        if text(review, "/reviewed_head_revision") != expected_head {
            errors.push(format!(
                "receipt review {index} head does not equal the receipt change head"
            ));
        }
        if Some(number(review, "/reviewed_payload_byte_length")) != expected_payload_length {
            errors.push(format!(
                "receipt review {index} payload length does not equal the exact payload bytes"
            ));
        }
        if text(review, "/reviewed_payload_sha256") != expected_payload_sha {
            errors.push(format!(
                "receipt review {index} payload sha256 does not equal the exact payload bytes"
            ));
        }
        let reviewed_at = text(review, "/reviewed_at_utc");
        check_canonical_utc(
            reviewed_at,
            &format!("receipt review {index} timestamp"),
            errors,
        );
        if reviewed_at > verified_at {
            errors.push(format!(
                "receipt review {index} timestamp is after provider verification"
            ));
        }
    }
}

fn descriptor_at(
    value: &Value,
    pointer: &str,
    label: &str,
    errors: &mut Vec<String>,
) -> Option<Descriptor> {
    let value = value
        .pointer(pointer)
        .unwrap_or_else(|| unreachable!("schema requires {pointer}"));
    descriptor(value, label, errors)
}

fn descriptor(value: &Value, label: &str, errors: &mut Vec<String>) -> Option<Descriptor> {
    match serde_json::from_value(value.clone()) {
        Ok(descriptor) => Some(descriptor),
        Err(error) => {
            errors.push(format!("decode schema-valid {label}: {error}"));
            None
        }
    }
}

fn check_descriptor_bytes(
    descriptor: &Descriptor,
    bytes: &[u8],
    label: &str,
    errors: &mut Vec<String>,
) {
    let length = u64::try_from(bytes.len());
    if length.ok() != Some(descriptor.byte_length) {
        errors.push(format!(
            "{label} length does not equal the exact supplied bytes"
        ));
    }
    if descriptor.sha256 != sha256_hex(bytes) {
        errors.push(format!(
            "{label} sha256 does not equal the exact supplied bytes"
        ));
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

fn check_canonical_utc(value: &str, label: &str, errors: &mut Vec<String>) {
    let bytes = value.as_bytes();
    let separators = bytes.len() == 20
        && bytes.get(4) == Some(&b'-')
        && bytes.get(7) == Some(&b'-')
        && bytes.get(10) == Some(&b'T')
        && bytes.get(13) == Some(&b':')
        && bytes.get(16) == Some(&b':')
        && bytes.get(19) == Some(&b'Z');
    if !separators {
        errors.push(format!("{label} is not canonical UTC second precision"));
        return;
    }
    let Some(year) = decimal(bytes, 0, 4) else {
        errors.push(format!("{label} has a non-decimal year"));
        return;
    };
    let Some(month) = decimal(bytes, 5, 7) else {
        errors.push(format!("{label} has a non-decimal month"));
        return;
    };
    let Some(day) = decimal(bytes, 8, 10) else {
        errors.push(format!("{label} has a non-decimal day"));
        return;
    };
    let Some(hour) = decimal(bytes, 11, 13) else {
        errors.push(format!("{label} has a non-decimal hour"));
        return;
    };
    let Some(minute) = decimal(bytes, 14, 16) else {
        errors.push(format!("{label} has a non-decimal minute"));
        return;
    };
    let Some(second) = decimal(bytes, 17, 19) else {
        errors.push(format!("{label} has a non-decimal second"));
        return;
    };
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let maximum_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => 0,
    };
    if year == 0 || day == 0 || day > maximum_day || hour > 23 || minute > 59 || second > 59 {
        errors.push(format!("{label} is not a real canonical UTC timestamp"));
    }
}

fn decimal(bytes: &[u8], start: usize, end: usize) -> Option<u32> {
    bytes
        .get(start..end)?
        .iter()
        .try_fold(0_u32, |value, byte| {
            byte.is_ascii_digit()
                .then(|| value * 10 + u32::from(byte - b'0'))
        })
}

fn text<'a>(value: &'a Value, pointer: &str) -> &'a str {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .unwrap_or_else(|| unreachable!("schema requires string at {pointer}"))
}

fn number(value: &Value, pointer: &str) -> u64 {
    value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .unwrap_or_else(|| unreachable!("schema requires u64 at {pointer}"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn policy() -> Value {
        json!({
            "schema_version": "state-backend-redb-prescreen-protected-review-policy/v1",
            "notice": "NOT QUALIFICATION EVIDENCE",
            "policy_id": "redb-prescreen-review-policy-v1",
            "provider": {
                "contract": "github-protected-review-export/v1",
                "repository_full_name": "laminardb/laminardb",
                "repository_id": "R_laminardb",
                "base_ref": "refs/heads/main"
            },
            "review_groups": [
                {"role": "workload_owner", "group_id": "T_workload"},
                {"role": "operations_owner", "group_id": "T_operations"}
            ],
            "decision_literals": {
                "pre_run": PRE_RUN_DECISION,
                "post_run": "ACCEPT_REDB_PRESCREEN_RESULT_V1"
            },
            "required_controls": {
                "immutable_change_id": true,
                "reviews_on_exact_head": true,
                "current_group_membership": true,
                "approved_review_state": true,
                "dismiss_stale_reviews": true,
                "distinct_principals": true,
                "distinct_review_events": true,
                "self_review_allowed": false,
                "admin_bypass_allowed": false
            },
            "protected_execution": {
                "workflow_file": ".github/workflows/redb-prescreen.yml",
                "job_name": "redb-prescreen",
                "environment_id": "E_redb-prescreen"
            }
        })
    }

    fn json_bytes(value: &Value) -> Vec<u8> {
        serde_json::to_vec_pretty(value).unwrap()
    }

    fn descriptor_value(expected: ExpectedArtifact, index: usize) -> Value {
        let (byte_length, sha256) = if index == 14 {
            (REDB_CRATE_BYTES, REDB_CRATE_SHA256.to_owned())
        } else {
            (
                u64::try_from(index + 1).unwrap(),
                format!("{:064x}", index + 1),
            )
        };
        json!({
            "role": expected.role,
            "locator": expected.locator,
            "byte_length": byte_length,
            "sha256": sha256,
            "media_type": expected.media_type
        })
    }

    fn payload(policy_bytes: &[u8]) -> Value {
        let mut artifacts = EXPECTED_ARTIFACTS
            .iter()
            .copied()
            .enumerate()
            .map(|(index, expected)| descriptor_value(expected, index))
            .collect::<Vec<_>>();
        artifacts[13]["byte_length"] = u64::try_from(policy_bytes.len()).unwrap().into();
        artifacts[13]["sha256"] = sha256_hex(policy_bytes).into();
        json!({
            "schema_version": "state-backend-redb-prescreen-approval-payload/v1",
            "notice": "NOT QUALIFICATION EVIDENCE",
            "payload_id": "redb-prescreen-synthetic-pre-run-v1",
            "protocol_id": "state-backend-redb-prescreen/v1",
            "run_class": "docker_smoke_no_decision",
            "required_decision_literal": PRE_RUN_DECISION,
            "required_review_roles": ["workload_owner", "operations_owner"],
            "artifacts": artifacts,
            "prior_smoke_result": null,
            "evidence_scope": {
                "prescreen_only": true,
                "qualification_eligible": false,
                "candidate_admission_eligible": false,
                "backend_selection_eligible": false,
                "production_eligible": false,
                "independent_soak_eligible": false,
                "c1_c2_c3_eligible": false,
                "fault_endurance_eligible": false,
                "checkpoint_exactly_once_eligible": false,
                "source_sink_delivery_eligible": false
            }
        })
    }

    fn receipt(policy_bytes: &[u8], payload_bytes: &[u8]) -> Value {
        let payload_length = u64::try_from(payload_bytes.len()).unwrap();
        let payload_sha = sha256_hex(payload_bytes);
        let review = |role: &str, account: &str, event: &str, time: &str| {
            json!({
                "role": role,
                "stable_account_id": account,
                "review_event_id": event,
                "provider_state": "APPROVED",
                "decision_literal": PRE_RUN_DECISION,
                "reviewed_head_revision": "1234567890abcdef1234567890abcdef12345678",
                "reviewed_payload_byte_length": payload_length,
                "reviewed_payload_sha256": payload_sha,
                "reviewed_at_utc": time
            })
        };
        json!({
            "schema_version": "state-backend-redb-prescreen-protected-review-receipt/v1",
            "notice": "NOT QUALIFICATION EVIDENCE",
            "stage": "pre_run",
            "provider": {
                "contract": "github-protected-review-export/v1",
                "repository_full_name": "laminardb/laminardb",
                "repository_id": "R_laminardb",
                "base_ref": "refs/heads/main"
            },
            "change": {
                "change_id": "PR_42",
                "base_ref": "refs/heads/main",
                "head_revision": "1234567890abcdef1234567890abcdef12345678"
            },
            "policy": {
                "role": "redb-prescreen-protected-review-policy",
                "locator": "contract/protected-review-policy.json",
                "byte_length": policy_bytes.len(),
                "sha256": sha256_hex(policy_bytes),
                "media_type": "application/json"
            },
            "payload": {
                "role": "redb-prescreen-approval-payload",
                "locator": "approval/payload.json",
                "byte_length": payload_length,
                "sha256": payload_sha,
                "media_type": "application/json"
            },
            "reviews": [
                review("workload_owner", "U_workload", "RV_workload", "2026-07-24T10:00:00Z"),
                review("operations_owner", "U_operations", "RV_operations", "2026-07-24T10:01:00Z")
            ],
            "protected_execution": {
                "workflow_file": ".github/workflows/redb-prescreen.yml",
                "job_name": "redb-prescreen",
                "environment_id": "E_redb-prescreen",
                "workflow_run_id": "RUN_100",
                "workflow_run_attempt": 1,
                "workflow_job_id": "JOB_100",
                "provider_verified_at_utc": "2026-07-24T10:02:00Z"
            },
            "retained_evidence": null
        })
    }

    fn valid_bytes() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let policy_bytes = json_bytes(&policy());
        let payload_bytes = json_bytes(&payload(&policy_bytes));
        let receipt_bytes = json_bytes(&receipt(&policy_bytes, &payload_bytes));
        (policy_bytes, payload_bytes, receipt_bytes)
    }

    fn invalid_payload_error(policy_bytes: &[u8], payload: &Value) -> String {
        let payload_bytes = json_bytes(payload);
        let receipt_bytes = json_bytes(&receipt(policy_bytes, &payload_bytes));
        validate_redb_prescreen_pre_run_content(policy_bytes, &payload_bytes, &receipt_bytes)
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn successor_schemas_are_valid_draft_2020_12() {
        for source in [POLICY_SCHEMA, PAYLOAD_SCHEMA, RECEIPT_SCHEMA] {
            let schema: Value = serde_json::from_str(source).unwrap();
            jsonschema::draft202012::meta::validate(&schema).unwrap();
        }
    }

    #[test]
    fn valid_copied_content_is_always_authorization_unverified() {
        let (policy, payload, receipt) = valid_bytes();
        let summary = validate_redb_prescreen_pre_run_content(&policy, &payload, &receipt).unwrap();
        assert_eq!(
            summary.authorization,
            RedbPrescreenAuthorization::Unverified
        );
        assert_eq!(
            summary.authorization.to_string(),
            "authorization_unverified"
        );
        assert!(!summary.execution_authorized());
        assert!(!summary.result_sealing_authorized());
    }

    #[test]
    fn copied_receipt_cannot_move_to_modified_policy_or_payload() {
        let (policy, payload, receipt) = valid_bytes();
        let mut changed_policy: Value = serde_json::from_slice(&policy).unwrap();
        changed_policy["policy_id"] = "redb-prescreen-review-policy-v2".into();
        assert!(validate_redb_prescreen_pre_run_content(
            &json_bytes(&changed_policy),
            &payload,
            &receipt
        )
        .is_err());

        let mut changed_payload: Value = serde_json::from_slice(&payload).unwrap();
        changed_payload["payload_id"] = "redb-prescreen-synthetic-pre-run-v2".into();
        assert!(validate_redb_prescreen_pre_run_content(
            &policy,
            &json_bytes(&changed_payload),
            &receipt
        )
        .is_err());
    }

    #[test]
    fn role_event_head_time_and_stage_mutations_fail_closed() {
        let (policy, payload, receipt) = valid_bytes();
        let base: Value = serde_json::from_slice(&receipt).unwrap();
        for mutation in [
            ("/reviews/1/stable_account_id", "U_workload"),
            ("/reviews/1/review_event_id", "RV_workload"),
            (
                "/reviews/1/reviewed_head_revision",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            ("/reviews/1/reviewed_at_utc", "2026-07-24T10:03:00Z"),
        ] {
            let mut changed = base.clone();
            *changed.pointer_mut(mutation.0).unwrap() = mutation.1.into();
            assert!(validate_redb_prescreen_pre_run_content(
                &policy,
                &payload,
                &json_bytes(&changed)
            )
            .is_err());
        }

        let mut post_run = base;
        post_run["stage"] = "post_run".into();
        post_run["reviews"][0]["decision_literal"] = "ACCEPT_REDB_PRESCREEN_RESULT_V1".into();
        post_run["reviews"][1]["decision_literal"] = "ACCEPT_REDB_PRESCREEN_RESULT_V1".into();
        post_run["retained_evidence"] = json!({
            "kind": "state-backend-redb-prescreen-retained-evidence-root/v1",
            "artifact_index": {
                "role": "redb-prescreen-artifact-index",
                "locator": "result/artifact-index.json",
                "byte_length": 1,
                "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "media_type": "application/json"
            }
        });
        let error =
            validate_redb_prescreen_pre_run_content(&policy, &payload, &json_bytes(&post_run))
                .unwrap_err()
                .to_string();
        assert!(error.contains("accepts only a pre_run receipt"));
    }

    #[test]
    fn descriptor_order_subject_pin_and_limits_fail_closed() {
        let (policy, payload, receipt) = valid_bytes();
        let mut reordered: Value = serde_json::from_slice(&payload).unwrap();
        reordered["artifacts"].as_array_mut().unwrap().swap(0, 1);
        let error = invalid_payload_error(&policy, &reordered);
        assert!(error.contains("payload artifact 0 role must be `redb-prescreen-protocol`"));

        let mut repinned: Value = serde_json::from_slice(&payload).unwrap();
        repinned["artifacts"][14]["sha256"] = "a".repeat(64).into();
        let error = invalid_payload_error(&policy, &repinned);
        assert!(error.contains("does not bind the exact 4.1.0 archive"));

        let mut wrong_length: Value = serde_json::from_slice(&payload).unwrap();
        wrong_length["artifacts"][14]["byte_length"] = (REDB_CRATE_BYTES + 1).into();
        let error = invalid_payload_error(&policy, &wrong_length);
        assert!(error.contains("does not bind the exact 4.1.0 archive"));

        let oversized = vec![b' '; MAX_REDB_REVIEW_POLICY_BYTES + 1];
        assert!(validate_redb_prescreen_pre_run_content(&oversized, &payload, &receipt).is_err());
    }

    #[test]
    fn every_control_json_cap_is_inclusive_and_cap_plus_one_is_rejected() {
        let pad = |mut bytes: Vec<u8>, length: usize| {
            assert!(bytes.len() < length);
            bytes.resize(length, b' ');
            bytes
        };

        let policy_at_cap = pad(json_bytes(&policy()), MAX_REDB_REVIEW_POLICY_BYTES);
        let payload_bytes = json_bytes(&payload(&policy_at_cap));
        let receipt_bytes = json_bytes(&receipt(&policy_at_cap, &payload_bytes));
        validate_redb_prescreen_pre_run_content(&policy_at_cap, &payload_bytes, &receipt_bytes)
            .unwrap();
        let mut policy_above_cap = policy_at_cap;
        policy_above_cap.push(b' ');
        let error = validate_redb_prescreen_pre_run_content(
            &policy_above_cap,
            &payload_bytes,
            &receipt_bytes,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains(&format!("maximum is {MAX_REDB_REVIEW_POLICY_BYTES}")));

        let policy = json_bytes(&policy());
        let payload_at_cap = pad(
            json_bytes(&payload(&policy)),
            MAX_REDB_APPROVAL_PAYLOAD_BYTES,
        );
        let receipt_bytes = json_bytes(&receipt(&policy, &payload_at_cap));
        validate_redb_prescreen_pre_run_content(&policy, &payload_at_cap, &receipt_bytes).unwrap();
        let mut payload_above_cap = payload_at_cap;
        payload_above_cap.push(b' ');
        let error =
            validate_redb_prescreen_pre_run_content(&policy, &payload_above_cap, &receipt_bytes)
                .unwrap_err()
                .to_string();
        assert!(error.contains(&format!("maximum is {MAX_REDB_APPROVAL_PAYLOAD_BYTES}")));

        let payload_bytes = json_bytes(&payload(&policy));
        let receipt_at_cap = pad(
            json_bytes(&receipt(&policy, &payload_bytes)),
            MAX_REDB_REVIEW_RECEIPT_BYTES,
        );
        validate_redb_prescreen_pre_run_content(&policy, &payload_bytes, &receipt_at_cap).unwrap();
        let mut receipt_above_cap = receipt_at_cap;
        receipt_above_cap.push(b' ');
        let error =
            validate_redb_prescreen_pre_run_content(&policy, &payload_bytes, &receipt_above_cap)
                .unwrap_err()
                .to_string();
        assert!(error.contains(&format!("maximum is {MAX_REDB_REVIEW_RECEIPT_BYTES}")));
    }

    #[test]
    fn native_shape_requires_a_bounded_prior_smoke_descriptor_but_stays_unverified() {
        let policy_bytes = json_bytes(&policy());
        let mut native = payload(&policy_bytes);
        native["run_class"] = "native_prescreen_decision".into();
        native["prior_smoke_result"] = json!({
            "role": "redb-prescreen-reviewed-smoke-result",
            "locator": "evidence/prior-smoke-result.json",
            "byte_length": 1024,
            "sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "media_type": "application/json"
        });
        let payload_bytes = json_bytes(&native);
        let receipt_bytes = json_bytes(&receipt(&policy_bytes, &payload_bytes));
        let summary =
            validate_redb_prescreen_pre_run_content(&policy_bytes, &payload_bytes, &receipt_bytes)
                .unwrap();
        assert_eq!(
            summary.authorization,
            RedbPrescreenAuthorization::Unverified
        );

        native["prior_smoke_result"]["byte_length"] = (MAX_NON_FIXTURE_BYTES + 1).into();
        let error = invalid_payload_error(&policy_bytes, &native);
        assert!(error.contains(&format!(
            "prior smoke descriptor declares {} bytes; maximum is {MAX_NON_FIXTURE_BYTES}",
            MAX_NON_FIXTURE_BYTES + 1
        )));

        native["prior_smoke_result"]["byte_length"] = 1024.into();
        native["prior_smoke_result"]["locator"] = "evidence/other.json".into();
        let payload_bytes = json_bytes(&native);
        let receipt_bytes = json_bytes(&receipt(&policy_bytes, &payload_bytes));
        assert!(validate_redb_prescreen_pre_run_content(
            &policy_bytes,
            &payload_bytes,
            &receipt_bytes
        )
        .is_err());
    }

    #[test]
    fn every_fixed_artifact_tuple_position_is_semantic_not_advisory() {
        let policy_bytes = json_bytes(&policy());
        for index in 0..EXPECTED_ARTIFACTS.len() {
            let media_replacement = if EXPECTED_ARTIFACTS[index].media_type == "application/json" {
                "application/octet-stream"
            } else {
                "application/json"
            };
            for (field, replacement) in [
                ("role", "synthetic-mutated-role"),
                ("locator", "synthetic/mutated-locator"),
                ("media_type", media_replacement),
            ] {
                let mut changed = payload(&policy_bytes);
                changed["artifacts"][index][field] = replacement.into();
                let error = invalid_payload_error(&policy_bytes, &changed);
                let semantic = if field == "media_type" {
                    "media type"
                } else {
                    field
                };
                assert!(error.contains(&format!("payload artifact {index} {semantic} must be")));
            }
        }
    }

    #[test]
    fn descriptor_role_caps_and_checked_aggregate_fail_closed() {
        let policy_bytes = json_bytes(&policy());
        for (index, above_cap) in [
            (0, MAX_NON_FIXTURE_BYTES + 1),
            (25, MAX_BASE_256M_BYTES + 1),
            (26, MAX_BASE_1G_BYTES + 1),
            (27, MAX_BASE_4G_BYTES + 1),
        ] {
            let mut changed = payload(&policy_bytes);
            changed["artifacts"][index]["byte_length"] = above_cap.into();
            let error = invalid_payload_error(&policy_bytes, &changed);
            assert!(error.contains(&format!("payload artifact {index} declares")));
        }

        let mut aggregate = payload(&policy_bytes);
        for index in 0..25 {
            if index != 13 && index != 14 {
                aggregate["artifacts"][index]["byte_length"] = MAX_NON_FIXTURE_BYTES.into();
            }
        }
        aggregate["artifacts"][25]["byte_length"] = MAX_BASE_256M_BYTES.into();
        aggregate["artifacts"][26]["byte_length"] = MAX_BASE_1G_BYTES.into();
        aggregate["artifacts"][27]["byte_length"] = MAX_BASE_4G_BYTES.into();
        let payload_bytes = json_bytes(&aggregate);
        let receipt_bytes = json_bytes(&receipt(&policy_bytes, &payload_bytes));
        let error =
            validate_redb_prescreen_pre_run_content(&policy_bytes, &payload_bytes, &receipt_bytes)
                .unwrap_err()
                .to_string();
        assert!(error.contains("maximum aggregate"));
    }

    #[test]
    fn packet_nominated_trust_and_review_bindings_remain_untrusted_and_strict() {
        let mut changed_policy = policy();
        changed_policy["review_groups"][1]["group_id"] = "T_workload".into();
        let policy_bytes = json_bytes(&changed_policy);
        let payload_bytes = json_bytes(&payload(&policy_bytes));
        let receipt_bytes = json_bytes(&receipt(&policy_bytes, &payload_bytes));
        let error =
            validate_redb_prescreen_pre_run_content(&policy_bytes, &payload_bytes, &receipt_bytes)
                .unwrap_err()
                .to_string();
        assert!(error.contains("review group IDs must be distinct"));

        let policy_bytes = json_bytes(&policy());
        let payload_bytes = json_bytes(&payload(&policy_bytes));
        let base_receipt = receipt(&policy_bytes, &payload_bytes);
        for (pointer, replacement) in [
            ("/provider/repository_id", "R_attacker"),
            (
                "/reviews/0/reviewed_payload_sha256",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            ("/protected_execution/environment_id", "E_attacker"),
        ] {
            let mut changed = base_receipt.clone();
            *changed.pointer_mut(pointer).unwrap() = replacement.into();
            assert!(validate_redb_prescreen_pre_run_content(
                &policy_bytes,
                &payload_bytes,
                &json_bytes(&changed)
            )
            .is_err());
        }
    }

    #[test]
    fn duplicate_unknown_authority_and_noncanonical_time_fail_closed() {
        let (policy, payload, receipt) = valid_bytes();
        let source = String::from_utf8(policy.clone()).unwrap();
        let duplicate = format!(
            "{{\"notice\":\"NOT QUALIFICATION EVIDENCE\",{}",
            &source[1..]
        );
        assert!(
            validate_redb_prescreen_pre_run_content(duplicate.as_bytes(), &payload, &receipt)
                .is_err()
        );

        let nested_duplicate = source.replacen(
            "\"repository_id\": \"R_laminardb\"",
            "\"repository_id\": \"R_laminardb\", \"repository_id\": \"R_attacker\"",
            1,
        );
        assert!(validate_redb_prescreen_pre_run_content(
            nested_duplicate.as_bytes(),
            &payload,
            &receipt
        )
        .is_err());

        let mut authority: Value = serde_json::from_slice(&receipt).unwrap();
        authority["execution_authorized"] = true.into();
        assert!(validate_redb_prescreen_pre_run_content(
            &policy,
            &payload,
            &json_bytes(&authority)
        )
        .is_err());

        let mut bad_date: Value = serde_json::from_slice(&receipt).unwrap();
        bad_date["reviews"][0]["reviewed_at_utc"] = "2026-02-30T10:00:00Z".into();
        assert!(
            validate_redb_prescreen_pre_run_content(&policy, &payload, &json_bytes(&bad_date))
                .is_err()
        );
    }

    #[test]
    fn json_shape_caps_count_root_as_one() {
        let at_limit = (0..(MAX_JSON_NODES - 1))
            .map(|_| Value::Null)
            .collect::<Vec<_>>();
        assert!(check_json_shape(&Value::Array(at_limit), "at-limit").is_ok());

        let above_limit = (0..MAX_JSON_NODES).map(|_| Value::Null).collect::<Vec<_>>();
        assert!(check_json_shape(&Value::Array(above_limit), "above-limit").is_err());

        let mut nested = Value::Null;
        for _ in 1..MAX_JSON_DEPTH {
            nested = Value::Array(vec![nested]);
        }
        assert!(check_json_shape(&nested, "depth-limit").is_ok());
        assert!(check_json_shape(&Value::Array(vec![nested]), "depth-plus-one").is_err());
    }
}
