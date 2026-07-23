use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::{
    decode_unique_json, reject_non_u64_numbers, reject_placeholder_strings,
    validated_profile_value, CheckErrors,
};

pub const SCHEMA_VERSION: &str = "state-backend-mechanism-mapping/v1";
pub const MAX_MECHANISM_MAPPING_BYTES: usize = 262_144;

const MAPPING_SCHEMA: &str = include_str!("../schema/mechanism-mapping-v1.schema.json");
const SOURCE_ROLE: &str = "candidate-source";
const CONFIGURATION_ROLE: &str = "candidate-configuration";
const MECHANISM_SOURCE_PROOF_ROLE: &str = "mechanism-source-proof";
const MECHANISM_CONFIGURATION_PROOF_ROLE: &str = "mechanism-configuration-proof";
const MECHANISM_PROBE_PROOF_ROLE: &str = "mechanism-bounded-probe-proof";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MechanismMappingSummary {
    pub mapping_id: String,
    pub candidate_id: String,
    pub background_maintenance_debt_kind: String,
    pub engine_pressure_stalls_kind: String,
    pub debt_mechanism_count: usize,
    pub stall_mechanism_count: usize,
}

pub fn validate_mechanism_mapping(
    profile_bytes: &[u8],
    mapping_bytes: &[u8],
) -> Result<MechanismMappingSummary, CheckErrors> {
    let profile = validated_profile_value(profile_bytes)?;
    if text(&profile, "/schema_version") != "distributed-state-qual/v3" {
        return Err(CheckErrors::one(
            "mechanism mapping v1 requires an exact distributed-state-qual/v3 profile",
        ));
    }

    let mapping = decode_unique_json(
        mapping_bytes,
        MAX_MECHANISM_MAPPING_BYTES,
        "mechanism mapping",
    )?;
    let schema: Value = serde_json::from_str(MAPPING_SCHEMA)
        .map_err(|error| CheckErrors::one(format!("decode embedded mechanism schema: {error}")))?;
    let validator = jsonschema::validator_for(&schema)
        .map_err(|error| CheckErrors::one(format!("compile embedded mechanism schema: {error}")))?;
    let schema_errors = validator
        .iter_errors(&mapping)
        .map(|error| format!("schema {}: {error}", error.instance_path()))
        .collect::<Vec<_>>();
    if !schema_errors.is_empty() {
        return Err(CheckErrors::many(schema_errors));
    }

    let mut errors = Vec::new();
    reject_placeholder_strings(&mapping, "", &mut errors);
    reject_non_u64_numbers(&mapping, "", &mut errors);

    let expected_profile_id = text(&profile, "/profile_id");
    if text(&mapping, "/profile/id") != expected_profile_id {
        errors.push("mapping profile id does not match the exact profile".to_owned());
    }
    let expected_profile_sha = lowercase_sha256(profile_bytes);
    if text(&mapping, "/profile/sha256") != expected_profile_sha {
        errors.push("mapping profile sha256 does not match the exact profile bytes".to_owned());
    }

    check_descriptor_role(&mapping, "/candidate/source", SOURCE_ROLE, &mut errors);
    check_descriptor_role(
        &mapping,
        "/candidate/configuration",
        CONFIGURATION_ROLE,
        &mut errors,
    );
    check_nonzero_digests(&mapping, "", &mut errors);

    let debt_kind = text(&mapping, "/background_maintenance_debt/kind");
    let debt_count = if debt_kind == "observed" {
        check_observed_mechanisms(
            &mapping,
            "/background_maintenance_debt/mechanisms",
            &mut errors,
        )
    } else {
        check_not_applicable_proofs(&mapping, "/background_maintenance_debt", &mut errors);
        0
    };

    let stall_kind = text(&mapping, "/engine_pressure_stalls/kind");
    let stall_count = if stall_kind == "observed" {
        check_observed_mechanisms(&mapping, "/engine_pressure_stalls/mechanisms", &mut errors)
    } else {
        check_not_applicable_proofs(&mapping, "/engine_pressure_stalls", &mut errors);
        0
    };

    if !errors.is_empty() {
        return Err(CheckErrors::many(errors));
    }

    Ok(MechanismMappingSummary {
        mapping_id: text(&mapping, "/mapping_id").to_owned(),
        candidate_id: text(&mapping, "/candidate/id").to_owned(),
        background_maintenance_debt_kind: debt_kind.to_owned(),
        engine_pressure_stalls_kind: stall_kind.to_owned(),
        debt_mechanism_count: debt_count,
        stall_mechanism_count: stall_count,
    })
}

fn check_observed_mechanisms(mapping: &Value, pointer: &str, errors: &mut Vec<String>) -> usize {
    let mechanisms = mapping
        .pointer(pointer)
        .and_then(Value::as_array)
        .unwrap_or_else(|| unreachable!("schema requires observed mechanisms"));
    let ids = mechanisms
        .iter()
        .map(|mechanism| text(mechanism, "/mechanism_id"))
        .collect::<Vec<_>>();
    if !ids.windows(2).all(|pair| pair[0] < pair[1]) {
        errors.push(format!("{pointer} must be sorted by unique mechanism_id"));
    }
    for mechanism in mechanisms {
        check_descriptor_role(
            mechanism,
            "/source_proof",
            MECHANISM_SOURCE_PROOF_ROLE,
            errors,
        );
        check_descriptor_role(
            mechanism,
            "/configuration_proof",
            MECHANISM_CONFIGURATION_PROOF_ROLE,
            errors,
        );
    }
    mechanisms.len()
}

fn check_not_applicable_proofs(mapping: &Value, pointer: &str, errors: &mut Vec<String>) {
    for (field, role) in [
        ("source_proof", MECHANISM_SOURCE_PROOF_ROLE),
        ("configuration_proof", MECHANISM_CONFIGURATION_PROOF_ROLE),
        ("bounded_probe_proof", MECHANISM_PROBE_PROOF_ROLE),
    ] {
        check_descriptor_role(mapping, &format!("{pointer}/{field}"), role, errors);
    }
}

fn check_descriptor_role(value: &Value, pointer: &str, expected: &str, errors: &mut Vec<String>) {
    let actual = text(value, &format!("{pointer}/role"));
    if actual != expected {
        errors.push(format!(
            "{pointer}/role must be `{expected}`, found `{actual}`"
        ));
    }
}

fn check_nonzero_digests(value: &Value, path: &str, errors: &mut Vec<String>) {
    match value {
        Value::Object(fields) => {
            for (key, child) in fields {
                let child_path = format!("{path}/{key}");
                if key == "sha256"
                    && child
                        .as_str()
                        .is_some_and(|digest| digest.bytes().all(|byte| byte == b'0'))
                {
                    errors.push(format!("all-zero digest at {child_path}"));
                }
                check_nonzero_digests(child, &child_path, errors);
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                check_nonzero_digests(child, &format!("{path}/{index}"), errors);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn lowercase_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn text<'a>(value: &'a Value, pointer: &str) -> &'a str {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .unwrap_or_else(|| unreachable!("schema requires string at {pointer}"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const PROFILE: &[u8] = include_bytes!("../profiles/linux-nvme-v3.candidate.json");
    const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    fn descriptor(role: &str, digest: &str) -> Value {
        json!({
            "role": role,
            "byte_length": 1,
            "sha256": digest,
            "media_type": "application/octet-stream"
        })
    }

    fn observed_mapping() -> Value {
        json!({
            "schema_version": SCHEMA_VERSION,
            "notice": "NOT QUALIFICATION EVIDENCE",
            "mapping_id": "synthetic-observed-v1",
            "status": "candidate_unapproved",
            "qualification_eligible": false,
            "profile": {
                "id": "linux-nvme-v3",
                "sha256": lowercase_sha256(PROFILE)
            },
            "candidate": {
                "id": "synthetic-candidate",
                "engine": "synthetic",
                "version": "1",
                "source": descriptor(SOURCE_ROLE, A),
                "configuration": descriptor(CONFIGURATION_ROLE, B)
            },
            "background_maintenance_debt": {
                "kind": "observed",
                "artifact_schema_version": "state-backend-maintenance-debt-samples/v1",
                "normalization": "direct-unsigned-bytes/v1",
                "population": "common-resource-v2-samples-and-required-cuts/v1",
                "aggregation": "checked-sum-per-observation-then-maximum/v1",
                "mechanisms": [{
                    "mechanism_id": "pending-work",
                    "source_contract": "synthetic-pending-work-bytes/v1",
                    "unit": "bytes",
                    "value_semantics": "outstanding-background-work-bytes",
                    "population_relation": "pairwise-disjoint-with-other-mapped-mechanisms",
                    "source_proof": descriptor(MECHANISM_SOURCE_PROOF_ROLE, A),
                    "configuration_proof": descriptor(MECHANISM_CONFIGURATION_PROOF_ROLE, B)
                }]
            },
            "engine_pressure_stalls": {
                "kind": "observed",
                "artifact_schema_version": "state-backend-stall-intervals/v1",
                "population": "all-source-intervals-intersecting-measurement/v1",
                "aggregation": "interval-union-intersect-measurement-ceil-permille/v1",
                "mechanisms": [{
                    "mechanism_id": "writer-pressure",
                    "source_contract": "synthetic-writer-pressure-intervals/v1",
                    "coverage": "foreground-admission-or-progress-stall-intervals",
                    "source_proof": descriptor(MECHANISM_SOURCE_PROOF_ROLE, A),
                    "configuration_proof": descriptor(MECHANISM_CONFIGURATION_PROOF_ROLE, B)
                }]
            },
            "target_device_io_latency": {
                "kind": "observed",
                "artifact_schema_version": "state-backend-target-device-io/v1",
                "population": "requests-issued-during-measurement/v1",
                "operations": ["read", "write", "flush"],
                "attribution": "exclusive-target-device",
                "aggregation": "maximum-issue-to-terminal-ceil-milliseconds/v1",
                "incomplete_request_policy": "candidate-fail-with-censored-lower-bound"
            }
        })
    }

    #[test]
    fn observed_mapping_is_valid_but_ineligible() {
        let bytes = serde_json::to_vec(&observed_mapping()).unwrap();
        let summary = validate_mechanism_mapping(PROFILE, &bytes).unwrap();
        assert_eq!(summary.mapping_id, "synthetic-observed-v1");
        assert_eq!(summary.background_maintenance_debt_kind, "observed");
        assert_eq!(summary.engine_pressure_stalls_kind, "observed");
        assert_eq!(summary.debt_mechanism_count, 1);
        assert_eq!(summary.stall_mechanism_count, 1);
    }

    #[test]
    fn proof_backed_not_applicable_arms_are_distinct_from_zero() {
        let mut mapping = observed_mapping();
        mapping["background_maintenance_debt"] = json!({
            "kind": "not_applicable",
            "reason_code": "no-background-maintenance-mechanism-in-exact-build",
            "claim_scope": "complete-candidate-process",
            "source_proof": descriptor(MECHANISM_SOURCE_PROOF_ROLE, A),
            "configuration_proof": descriptor(MECHANISM_CONFIGURATION_PROOF_ROLE, B),
            "bounded_probe_proof": descriptor(MECHANISM_PROBE_PROOF_ROLE, C)
        });
        mapping["engine_pressure_stalls"] = json!({
            "kind": "not_applicable",
            "reason_code": "no-engine-pressure-stall-mechanism-in-exact-build",
            "claim_scope": "complete-candidate-process",
            "source_proof": descriptor(MECHANISM_SOURCE_PROOF_ROLE, A),
            "configuration_proof": descriptor(MECHANISM_CONFIGURATION_PROOF_ROLE, B),
            "bounded_probe_proof": descriptor(MECHANISM_PROBE_PROOF_ROLE, C)
        });
        let bytes = serde_json::to_vec(&mapping).unwrap();
        let summary = validate_mechanism_mapping(PROFILE, &bytes).unwrap();
        assert_eq!(summary.background_maintenance_debt_kind, "not_applicable");
        assert_eq!(summary.engine_pressure_stalls_kind, "not_applicable");
        assert_eq!(summary.debt_mechanism_count, 0);
        assert_eq!(summary.stall_mechanism_count, 0);
    }

    #[test]
    fn rejects_profile_drift_unsorted_mechanisms_and_unproved_na() {
        let mut mapping = observed_mapping();
        mapping["profile"]["sha256"] = Value::String(C.to_owned());
        let error = validate_mechanism_mapping(PROFILE, &serde_json::to_vec(&mapping).unwrap())
            .unwrap_err()
            .to_string();
        assert!(error.contains("profile sha256"));

        let mut mapping = observed_mapping();
        let first = mapping["background_maintenance_debt"]["mechanisms"][0].clone();
        let mut earlier = first.clone();
        earlier["mechanism_id"] = "a-earlier".into();
        earlier["source_contract"] = "a-earlier-source/v1".into();
        mapping["background_maintenance_debt"]["mechanisms"] = json!([first, earlier]);
        let error = validate_mechanism_mapping(PROFILE, &serde_json::to_vec(&mapping).unwrap())
            .unwrap_err()
            .to_string();
        assert!(error.contains("sorted by unique mechanism_id"));

        let mut mapping = observed_mapping();
        mapping["background_maintenance_debt"] = json!({
            "kind": "not_applicable",
            "reason_code": "no-background-maintenance-mechanism-in-exact-build",
            "claim_scope": "complete-candidate-process",
            "source_proof": descriptor(MECHANISM_SOURCE_PROOF_ROLE, A),
            "configuration_proof": descriptor(MECHANISM_CONFIGURATION_PROOF_ROLE, B)
        });
        assert!(
            validate_mechanism_mapping(PROFILE, &serde_json::to_vec(&mapping).unwrap()).is_err()
        );
    }

    #[test]
    fn rejects_wrong_roles_zero_digests_duplicates_and_pre_v3_profile() {
        let mut mapping = observed_mapping();
        mapping["candidate"]["source"]["role"] = "mechanism-source-proof".into();
        let error = validate_mechanism_mapping(PROFILE, &serde_json::to_vec(&mapping).unwrap())
            .unwrap_err()
            .to_string();
        assert!(error.contains("candidate/source/role"));

        let mut mapping = observed_mapping();
        mapping["candidate"]["source"]["sha256"] = "0".repeat(64).into();
        let error = validate_mechanism_mapping(PROFILE, &serde_json::to_vec(&mapping).unwrap())
            .unwrap_err()
            .to_string();
        assert!(error.contains("all-zero digest"));

        let source = serde_json::to_string(&observed_mapping()).unwrap();
        let duplicate = source.replacen(
            "\"mapping_id\":\"synthetic-observed-v1\"",
            "\"mapping_id\":\"synthetic-observed-v1\",\"mapping_id\":\"duplicate\"",
            1,
        );
        assert!(validate_mechanism_mapping(PROFILE, duplicate.as_bytes()).is_err());

        const PROFILE_V2: &[u8] = include_bytes!("../profiles/linux-nvme-v2.candidate.json");
        let error = validate_mechanism_mapping(
            PROFILE_V2,
            &serde_json::to_vec(&observed_mapping()).unwrap(),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("requires an exact distributed-state-qual/v3"));
    }
}
