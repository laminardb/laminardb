#![forbid(unsafe_code)]

use std::fmt::{Display, Formatter};

use serde::Deserialize;
use serde_json::Value;

mod oracle_v2;
mod provenance_v1;
#[cfg(test)]
mod transactional_writer_v1;
mod wire_v1;

pub const NOTICE: &str = "NOT CERTIFICATION EVIDENCE";

const CONTRACT_SCHEMA: &str = include_str!("../schema/contract-v1alpha1.schema.json");

#[derive(Debug)]
pub struct CheckErrors {
    messages: Vec<String>,
}

impl CheckErrors {
    fn one(message: impl Into<String>) -> Self {
        Self {
            messages: vec![message.into()],
        }
    }

    fn many(mut messages: Vec<String>) -> Self {
        messages.sort();
        messages.dedup();
        Self { messages }
    }
}

impl Display for CheckErrors {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.messages.join("; "))
    }
}

impl std::error::Error for CheckErrors {}

#[derive(Debug, PartialEq, Eq)]
pub struct DraftSummary {
    pub schema_version: String,
    pub unresolved_gates: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DraftContract {
    schema_version: String,
    notice: String,
    status: String,
    certification_eligible: bool,
    scenario: DraftScenario,
    oracle: DraftOracle,
    gates: DraftGates,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DraftScenario {
    id: String,
    admission: String,
    delivery_guarantee: String,
    source: String,
    source_group_ordering: String,
    operator: String,
    output: String,
    sink: DraftSink,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DraftSink {
    required_capability: String,
    implementation: String,
    eligibility: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DraftOracle {
    status: String,
    public_surfaces_only: bool,
    laminar_library_dependencies: bool,
    numeric_coverage: String,
    stable_operation_identity: String,
    assignment_generation: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DraftGates {
    owners_and_independent_review: UnresolvedGate,
    immutable_artifact_identities: UnresolvedGate,
    production_environment: UnresolvedGate,
    numerical_contract: UnresolvedGate,
    fault_controller: UnresolvedGate,
    evidence_store: UnresolvedGate,
    cluster_admission: UnresolvedGate,
    operation_identity: UnresolvedGate,
    assignment_provenance_and_fencing: UnresolvedGate,
    independent_dry_run: UnresolvedGate,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UnresolvedGate {
    state: String,
    approved_values: Option<Value>,
}

impl DraftContract {
    fn semantic_errors(&self) -> Vec<String> {
        let mut errors = Vec::new();
        expect_equal(
            &mut errors,
            "schema_version",
            &self.schema_version,
            "distributed-state-soak/v1alpha1",
        );
        expect_equal(&mut errors, "notice", &self.notice, NOTICE);
        expect_equal(&mut errors, "status", &self.status, "draft_ineligible");
        if self.certification_eligible {
            errors.push("certification_eligible must remain false".to_owned());
        }

        let scenario = &self.scenario;
        expect_equal(
            &mut errors,
            "scenario.id",
            &scenario.id,
            "grouped-count-sum-alo",
        );
        expect_equal(
            &mut errors,
            "scenario.admission",
            &scenario.admission,
            "blocked_ldb_4007",
        );
        expect_equal(
            &mut errors,
            "scenario.delivery_guarantee",
            &scenario.delivery_guarantee,
            "at_least_once",
        );
        expect_equal(
            &mut errors,
            "scenario.source",
            &scenario.source,
            "replayable_splittable_kafka",
        );
        expect_equal(
            &mut errors,
            "scenario.source_group_ordering",
            &scenario.source_group_ordering,
            "one_topic_partition_per_logical_group",
        );
        expect_equal(
            &mut errors,
            "scenario.operator",
            &scenario.operator,
            "grouped_count_star_sum_exact_integer_decimal",
        );
        expect_equal(
            &mut errors,
            "scenario.output",
            &scenario.output,
            "append_result_snapshots",
        );
        expect_equal(
            &mut errors,
            "scenario.sink.required_capability",
            &scenario.sink.required_capability,
            "durable_multiwriter_append",
        );
        expect_equal(
            &mut errors,
            "scenario.sink.implementation",
            &scenario.sink.implementation,
            "kafka_envelope_append_candidate",
        );
        expect_equal(
            &mut errors,
            "scenario.sink.eligibility",
            &scenario.sink.eligibility,
            "candidate_ineligible",
        );

        let oracle = &self.oracle;
        expect_equal(&mut errors, "oracle.status", &oracle.status, "fixture_only");
        if !oracle.public_surfaces_only {
            errors.push("oracle.public_surfaces_only must be true".to_owned());
        }
        if oracle.laminar_library_dependencies {
            errors.push("oracle.laminar_library_dependencies must be false".to_owned());
        }
        expect_equal(
            &mut errors,
            "oracle.numeric_coverage",
            &oracle.numeric_coverage,
            "integer_fixture_only_decimal_required",
        );
        expect_equal(
            &mut errors,
            "oracle.stable_operation_identity",
            &oracle.stable_operation_identity,
            "required_not_implemented",
        );
        expect_equal(
            &mut errors,
            "oracle.assignment_generation",
            &oracle.assignment_generation,
            "required_not_implemented",
        );

        for (name, gate) in self.gates.iter() {
            expect_equal(
                &mut errors,
                &format!("gates.{name}.state"),
                &gate.state,
                "unresolved",
            );
            if gate.approved_values.is_some() {
                errors.push(format!(
                    "gates.{name}.approved_values must remain null in v1alpha1"
                ));
            }
        }
        errors
    }
}

impl DraftGates {
    fn iter(&self) -> [(&'static str, &UnresolvedGate); 10] {
        [
            (
                "owners_and_independent_review",
                &self.owners_and_independent_review,
            ),
            (
                "immutable_artifact_identities",
                &self.immutable_artifact_identities,
            ),
            ("production_environment", &self.production_environment),
            ("numerical_contract", &self.numerical_contract),
            ("fault_controller", &self.fault_controller),
            ("evidence_store", &self.evidence_store),
            ("cluster_admission", &self.cluster_admission),
            ("operation_identity", &self.operation_identity),
            (
                "assignment_provenance_and_fencing",
                &self.assignment_provenance_and_fencing,
            ),
            ("independent_dry_run", &self.independent_dry_run),
        ]
    }
}

fn expect_equal(errors: &mut Vec<String>, field: &str, actual: &str, expected: &str) {
    if actual != expected {
        errors.push(format!("{field} must be {expected:?}"));
    }
}

pub fn validate_contract(bytes: &[u8]) -> Result<DraftSummary, CheckErrors> {
    // Deserialize to concrete, deny-unknown structs first so duplicate keys cannot
    // disappear when the document is converted to a generic JSON value.
    let contract: DraftContract = serde_json::from_slice(bytes)
        .map_err(|error| CheckErrors::one(format!("decode contract: {error}")))?;
    let instance: Value = serde_json::from_slice(bytes)
        .map_err(|error| CheckErrors::one(format!("decode contract JSON: {error}")))?;
    let schema: Value = serde_json::from_str(CONTRACT_SCHEMA)
        .map_err(|error| CheckErrors::one(format!("decode embedded schema: {error}")))?;
    let validator = jsonschema::validator_for(&schema)
        .map_err(|error| CheckErrors::one(format!("compile embedded schema: {error}")))?;

    let mut errors = validator
        .iter_errors(&instance)
        .map(|error| format!("schema {}: {error}", error.instance_path()))
        .collect::<Vec<_>>();
    errors.extend(contract.semantic_errors());
    if !errors.is_empty() {
        return Err(CheckErrors::many(errors));
    }

    Ok(DraftSummary {
        schema_version: contract.schema_version,
        unresolved_gates: contract.gates.iter().len(),
    })
}

#[derive(Debug, PartialEq, Eq)]
pub struct FixtureSummary {
    pub cases: usize,
    pub model_matches: usize,
    pub product_failures: usize,
    pub invalid_runs: usize,
}

#[derive(Deserialize)]
struct FixtureVersionProbe {
    schema_version: String,
}

pub fn verify_oracle_fixture(bytes: &[u8]) -> Result<FixtureSummary, CheckErrors> {
    let version: FixtureVersionProbe = serde_json::from_slice(bytes)
        .map_err(|error| CheckErrors::one(format!("decode oracle fixture: {error}")))?;
    if version.schema_version != "independent-oracle-fixture/v2" {
        return Err(CheckErrors::one(format!(
            "unsupported oracle fixture schema_version {:?}",
            version.schema_version
        )));
    }
    oracle_v2::verify(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DRAFT: &[u8] = include_bytes!("../contracts/draft-v1alpha1.json");
    const MANIFEST: &str = include_str!("../Cargo.toml");

    #[test]
    fn committed_contract_is_only_a_valid_ineligible_draft() {
        assert_eq!(
            validate_contract(DRAFT).unwrap(),
            DraftSummary {
                schema_version: "distributed-state-soak/v1alpha1".to_owned(),
                unresolved_gates: 10,
            }
        );
    }

    #[test]
    fn standalone_tool_has_no_path_or_workspace_dependencies() {
        assert!(!MANIFEST.contains("path ="));
        assert!(!MANIFEST.contains("workspace = true"));
    }

    #[test]
    fn draft_rejects_eligibility_notice_and_unknown_field_changes() {
        let mut value: Value = serde_json::from_slice(DRAFT).unwrap();
        value["certification_eligible"] = Value::Bool(true);
        assert!(validate_contract(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_slice(DRAFT).unwrap();
        value["notice"] = Value::String("schema check".to_owned());
        assert!(validate_contract(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_slice(DRAFT).unwrap();
        value["unsupported_claim"] = Value::Bool(true);
        assert!(validate_contract(&serde_json::to_vec(&value).unwrap()).is_err());
    }

    #[test]
    fn draft_rejects_numerical_values_and_placeholders() {
        let mut value: Value = serde_json::from_slice(DRAFT).unwrap();
        value["gates"]["numerical_contract"]["approved_values"] =
            serde_json::json!({"unapproved_numeric_value": 123});
        assert!(validate_contract(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_slice(DRAFT).unwrap();
        value["gates"]["numerical_contract"]["approved_values"] = Value::String("TBD".to_owned());
        assert!(validate_contract(&serde_json::to_vec(&value).unwrap()).is_err());
    }

    #[test]
    fn typed_decode_rejects_duplicate_contract_fields() {
        let draft = std::str::from_utf8(DRAFT).unwrap();
        let duplicate = draft.replacen(
            "\"notice\": \"NOT CERTIFICATION EVIDENCE\",",
            "\"notice\": \"NOT CERTIFICATION EVIDENCE\",\n  \"notice\": \"NOT CERTIFICATION EVIDENCE\",",
            1,
        );
        let error = validate_contract(duplicate.as_bytes()).unwrap_err();
        assert!(error.to_string().contains("duplicate field"));
    }
}
