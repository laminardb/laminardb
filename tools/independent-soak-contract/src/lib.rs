#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};

use serde::Deserialize;
use serde_json::Value;

mod oracle_v2;
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

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
enum FixtureClassification {
    #[serde(rename = "FIXTURE_MODEL_MATCH")]
    ModelMatch,
    #[serde(rename = "FIXTURE_PRODUCT_FAIL")]
    ProductFail,
    #[serde(rename = "FIXTURE_RUN_INVALID")]
    RunInvalid,
}

impl Display for FixtureClassification {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ModelMatch => write!(formatter, "FIXTURE_MODEL_MATCH"),
            Self::ProductFail => write!(formatter, "FIXTURE_PRODUCT_FAIL"),
            Self::RunInvalid => write!(formatter, "FIXTURE_RUN_INVALID"),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OracleFixture {
    schema_version: String,
    notice: String,
    scope: String,
    model: String,
    operation_identity_model: String,
    ledger: Vec<LedgerRecord>,
    cases: Vec<OracleCase>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LedgerRecord {
    event_id: String,
    topic: String,
    partition: i32,
    offset: i64,
    logical_key: String,
    value: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OracleCase {
    id: String,
    frozen_source_cut: Vec<PartitionCut>,
    durable_source_cut: Vec<PartitionCut>,
    frozen_sink_cut: Vec<PartitionCut>,
    consumed_sink_cut: Vec<PartitionCut>,
    records: Vec<SinkRecord>,
    expected: ExpectedOutcome,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PartitionCut {
    topic: String,
    partition: i32,
    exclusive_end: i64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SinkRecord {
    topic: String,
    partition: i32,
    offset: i64,
    operation_id: String,
    payload: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedOutcome {
    classification: FixtureClassification,
    diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct AggregatePayload {
    logical_key: String,
    count: u64,
    sum: i64,
}

#[derive(Debug)]
struct ExpectedOperation {
    payload: AggregatePayload,
}

#[derive(Debug)]
struct ExpectedModel {
    operations: BTreeMap<String, ExpectedOperation>,
    final_operation_ids: BTreeSet<String>,
    final_counts: BTreeMap<String, u64>,
}

#[derive(Debug, PartialEq, Eq)]
struct OracleOutcome {
    classification: FixtureClassification,
    diagnostics: BTreeSet<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct FixtureSummary {
    pub cases: usize,
    pub model_matches: usize,
    pub product_failures: usize,
    pub invalid_runs: usize,
}

type CutKey = (String, i32);

fn cut_map(cuts: &[PartitionCut], label: &str) -> Result<BTreeMap<CutKey, i64>, CheckErrors> {
    let mut result = BTreeMap::new();
    for cut in cuts {
        if cut.partition < 0 || cut.exclusive_end < 0 {
            return Err(CheckErrors::one(format!(
                "{label} contains a negative partition or boundary"
            )));
        }
        let key = (cut.topic.clone(), cut.partition);
        if result.insert(key, cut.exclusive_end).is_some() {
            return Err(CheckErrors::one(format!(
                "{label} contains a duplicate topic/partition"
            )));
        }
    }
    Ok(result)
}

fn evaluate_case(fixture: &OracleFixture, case: &OracleCase) -> Result<OracleOutcome, CheckErrors> {
    let frozen_source = cut_map(&case.frozen_source_cut, "frozen_source_cut")?;
    let durable_source = cut_map(&case.durable_source_cut, "durable_source_cut")?;
    let frozen_sink = cut_map(&case.frozen_sink_cut, "frozen_sink_cut")?;
    let consumed_sink = cut_map(&case.consumed_sink_cut, "consumed_sink_cut")?;
    let mut invalid = BTreeSet::new();

    for (key, frozen) in &frozen_source {
        if durable_source
            .get(key)
            .is_none_or(|durable| durable < frozen)
        {
            invalid.insert("source_cut_incomplete".to_owned());
        }
    }
    for (key, frozen) in &frozen_sink {
        if consumed_sink
            .get(key)
            .is_none_or(|consumed| consumed < frozen)
        {
            invalid.insert("sink_cut_incomplete".to_owned());
        }
    }
    if !invalid.is_empty() {
        return Ok(OracleOutcome {
            classification: FixtureClassification::RunInvalid,
            diagnostics: invalid,
        });
    }

    let expected = expected_model(fixture, &frozen_source)?;
    let mut diagnostics = BTreeSet::new();
    let mut observed = BTreeMap::<String, SinkRecord>::new();
    let mut observed_final = BTreeSet::new();

    for record in &case.records {
        let sink_key = (record.topic.clone(), record.partition);
        if record.offset < 0
            || frozen_sink
                .get(&sink_key)
                .is_none_or(|boundary| record.offset >= *boundary)
        {
            diagnostics.insert("output_beyond_frozen_sink_cut".to_owned());
        }

        let decoded_payload = serde_json::from_str::<AggregatePayload>(&record.payload);
        match expected.operations.get(&record.operation_id) {
            Some(expected_operation) => match &decoded_payload {
                Ok(payload) if payload == &expected_operation.payload => {
                    if expected.final_operation_ids.contains(&record.operation_id) {
                        observed_final.insert(record.operation_id.clone());
                    }
                }
                Ok(_) => {
                    diagnostics.insert("aggregate_divergence".to_owned());
                }
                Err(_) => {
                    diagnostics.insert("malformed_output".to_owned());
                }
            },
            None => match &decoded_payload {
                Ok(payload)
                    if expected
                        .final_counts
                        .get(&payload.logical_key)
                        .is_some_and(|final_count| payload.count > *final_count) =>
                {
                    diagnostics.insert("aggregate_divergence".to_owned());
                }
                Ok(_) => {
                    diagnostics.insert("extra_output".to_owned());
                }
                Err(_) => {
                    diagnostics.insert("malformed_output".to_owned());
                }
            },
        }

        match observed.get(&record.operation_id) {
            Some(previous) if previous.payload.as_bytes() == record.payload.as_bytes() => {}
            Some(_) => {
                diagnostics.insert("conflicting_operation_identity".to_owned());
            }
            None => {
                observed.insert(record.operation_id.clone(), record.clone());
            }
        }
    }

    if expected
        .final_operation_ids
        .iter()
        .any(|operation_id| !observed_final.contains(operation_id))
    {
        diagnostics.insert("missing_final_state".to_owned());
    }

    Ok(OracleOutcome {
        classification: if diagnostics.is_empty() {
            FixtureClassification::ModelMatch
        } else {
            FixtureClassification::ProductFail
        },
        diagnostics,
    })
}

fn expected_model(
    fixture: &OracleFixture,
    frozen_source: &BTreeMap<CutKey, i64>,
) -> Result<ExpectedModel, CheckErrors> {
    let mut records = fixture.ledger.iter().collect::<Vec<_>>();
    records.sort_by_key(|record| (&record.topic, record.partition, record.offset));
    let mut key_partitions = BTreeMap::<&str, (&str, i32)>::new();
    let mut aggregates = BTreeMap::<String, (u64, i64)>::new();
    let mut operations = BTreeMap::new();
    let mut event_ids = BTreeSet::new();
    let mut source_offsets = BTreeSet::new();

    for record in records {
        if record.partition < 0 || record.offset < 0 {
            return Err(CheckErrors::one(
                "ledger contains a negative partition or offset",
            ));
        }
        if !event_ids.insert(record.event_id.as_str()) {
            return Err(CheckErrors::one("ledger contains a duplicate event_id"));
        }
        if !source_offsets.insert((record.topic.as_str(), record.partition, record.offset)) {
            return Err(CheckErrors::one(
                "ledger contains a duplicate topic/partition/offset",
            ));
        }
        let source_key = (record.topic.clone(), record.partition);
        let Some(boundary) = frozen_source.get(&source_key) else {
            return Err(CheckErrors::one(
                "frozen_source_cut does not cover every ledger partition",
            ));
        };
        if record.offset >= *boundary {
            continue;
        }

        let partition = (record.topic.as_str(), record.partition);
        if key_partitions
            .insert(record.logical_key.as_str(), partition)
            .is_some_and(|previous| previous != partition)
        {
            return Err(CheckErrors::one(
                "fixture routes one logical key through multiple source partitions",
            ));
        }
        let (count, sum) = aggregates
            .entry(record.logical_key.clone())
            .or_insert((0, 0));
        *count = count
            .checked_add(1)
            .ok_or_else(|| CheckErrors::one("fixture aggregate count overflow"))?;
        *sum = sum
            .checked_add(record.value)
            .ok_or_else(|| CheckErrors::one("fixture aggregate sum overflow"))?;
        let operation = ExpectedOperation {
            payload: AggregatePayload {
                logical_key: record.logical_key.clone(),
                count: *count,
                sum: *sum,
            },
        };
        let operation_id = fixture_operation_id(&record.logical_key, *count);
        if operations.insert(operation_id, operation).is_some() {
            return Err(CheckErrors::one(
                "fixture operation identity is not unique for a legal group version",
            ));
        }
    }

    let final_operation_ids = aggregates
        .iter()
        .map(|(logical_key, (count, _))| fixture_operation_id(logical_key, *count))
        .collect();
    let final_counts = aggregates
        .into_iter()
        .map(|(logical_key, (count, _))| (logical_key, count))
        .collect();
    Ok(ExpectedModel {
        operations,
        final_operation_ids,
        final_counts,
    })
}

fn fixture_operation_id(logical_key: &str, count: u64) -> String {
    format!("fixture/{logical_key}/count/{count}")
}

#[derive(Deserialize)]
struct FixtureVersionProbe {
    schema_version: String,
}

pub fn verify_oracle_fixture(bytes: &[u8]) -> Result<FixtureSummary, CheckErrors> {
    let version: FixtureVersionProbe = serde_json::from_slice(bytes)
        .map_err(|error| CheckErrors::one(format!("decode oracle fixture: {error}")))?;
    match version.schema_version.as_str() {
        "independent-oracle-fixture/v1" => verify_oracle_fixture_v1(bytes),
        "independent-oracle-fixture/v2" => oracle_v2::verify(bytes),
        unsupported => Err(CheckErrors::one(format!(
            "unsupported oracle fixture schema_version {unsupported:?}"
        ))),
    }
}

fn verify_oracle_fixture_v1(bytes: &[u8]) -> Result<FixtureSummary, CheckErrors> {
    let fixture: OracleFixture = serde_json::from_slice(bytes)
        .map_err(|error| CheckErrors::one(format!("decode oracle fixture: {error}")))?;
    let mut errors = Vec::new();
    expect_equal(
        &mut errors,
        "fixture.schema_version",
        &fixture.schema_version,
        "independent-oracle-fixture/v1",
    );
    expect_equal(&mut errors, "fixture.notice", &fixture.notice, NOTICE);
    expect_equal(
        &mut errors,
        "fixture.scope",
        &fixture.scope,
        "semantic_fixture_only",
    );
    expect_equal(
        &mut errors,
        "fixture.model",
        &fixture.model,
        "grouped_count_star_sum_exact_integer_snapshot_at_least_once",
    );
    expect_equal(
        &mut errors,
        "fixture.operation_identity_model",
        &fixture.operation_identity_model,
        "fixture_only_logical_group_and_count_version",
    );
    if fixture.cases.is_empty() {
        errors.push("oracle fixture must contain cases".to_owned());
    }

    let mut case_ids = BTreeSet::new();
    let mut summary = FixtureSummary {
        cases: fixture.cases.len(),
        model_matches: 0,
        product_failures: 0,
        invalid_runs: 0,
    };
    for case in &fixture.cases {
        if !case_ids.insert(case.id.as_str()) {
            errors.push(format!("duplicate oracle case id {:?}", case.id));
            continue;
        }
        match evaluate_case(&fixture, case) {
            Ok(outcome) => {
                if outcome.classification != case.expected.classification {
                    errors.push(format!(
                        "case {:?}: expected {}, observed {}",
                        case.id, case.expected.classification, outcome.classification
                    ));
                }
                let expected_diagnostics = case
                    .expected
                    .diagnostics
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>();
                if outcome.diagnostics != expected_diagnostics {
                    errors.push(format!(
                        "case {:?}: expected diagnostics {:?}, observed {:?}",
                        case.id, expected_diagnostics, outcome.diagnostics
                    ));
                }
                match outcome.classification {
                    FixtureClassification::ModelMatch => summary.model_matches += 1,
                    FixtureClassification::ProductFail => summary.product_failures += 1,
                    FixtureClassification::RunInvalid => summary.invalid_runs += 1,
                }
            }
            Err(error) => errors.push(format!("case {:?}: {error}", case.id)),
        }
    }
    if errors.is_empty() {
        Ok(summary)
    } else {
        Err(CheckErrors::many(errors))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DRAFT: &[u8] = include_bytes!("../contracts/draft-v1alpha1.json");
    const FIXTURE: &[u8] = include_bytes!("../fixtures/grouped-count-sum-alo-v1.json");
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

    #[test]
    fn grouped_aggregate_fixture_covers_match_failure_and_invalid_results() {
        assert_eq!(
            verify_oracle_fixture(FIXTURE).unwrap(),
            FixtureSummary {
                cases: 9,
                model_matches: 3,
                product_failures: 4,
                invalid_runs: 2,
            }
        );
    }

    #[test]
    fn final_snapshot_is_required_but_intermediate_prefix_is_optional() {
        let fixture: OracleFixture = serde_json::from_slice(FIXTURE).unwrap();
        let case = fixture
            .cases
            .iter()
            .find(|case| case.id == "final-snapshots-with-intermediate-prefix-omitted")
            .unwrap();
        let outcome = evaluate_case(&fixture, case).unwrap();
        assert_eq!(outcome.classification, FixtureClassification::ModelMatch);
        assert!(outcome.diagnostics.is_empty());
    }

    #[test]
    fn semantic_fixture_does_not_guess_assignment_provenance() {
        let fixture = std::str::from_utf8(FIXTURE).unwrap();
        assert!(!fixture.contains("assignment_generation"));
        assert!(!fixture.contains("stale_assignment"));
    }

    #[test]
    fn fixture_rejects_one_group_spanning_source_partitions() {
        let mut value: Value = serde_json::from_slice(FIXTURE).unwrap();
        value["ledger"][2]["logical_key"] = Value::String("alpha".to_owned());
        let error = verify_oracle_fixture(&serde_json::to_vec(&value).unwrap()).unwrap_err();
        assert!(error
            .to_string()
            .contains("one logical key through multiple source partitions"));
    }
}
