use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

use super::{provenance_v1, wire_v1};

use super::{expect_equal, CheckErrors, FixtureSummary, NOTICE};

mod case_evaluation;
mod output_evaluation;
mod topology;
mod wire_envelopes;
mod wire_identity;

use case_evaluation::evaluate_case;
use output_evaluation::{evaluate_output, same_common_marker};
use topology::{validate_ledger, validate_source_topology, validate_topology};
use wire_envelopes::evaluate_wire_envelopes;
#[cfg(test)]
use wire_envelopes::{compare_wire_u64, decode_wire_hex, vnode_bitmap};
use wire_identity::{
    hex_nibble, operation_id_context, validate_operation_group_keys, validate_wire_id_map,
};

const SCHEMA_VERSION: &str = "independent-oracle-fixture/v2";
const ABI: &str = "fixture-utf8-byte-sum-mod-v1";
const PARTITIONING_ABI: &str = "fixture-sink-shard-partition-map-v1";
const ENVELOPE_VERSION: u16 = 1;
const PIPELINE_IDENTITY_VERSION: u16 = 5;
const WIRE_ABI_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum Classification {
    #[serde(rename = "MODEL_MATCH")]
    ModelMatch,
    #[serde(rename = "PRODUCT_FAIL")]
    ProductFail,
    #[serde(rename = "RUN_INVALID")]
    RunInvalid,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema_version: String,
    notice: String,
    scope: String,
    certification_eligible: bool,
    model: String,
    operation_identity_model: String,
    key_vnode_abi: String,
    vnode_count: u16,
    wire_id_map: WireIdMap,
    operation_group_keys: Vec<OperationGroupKey>,
    expected_run: RunIdentity,
    source_topology: SourceTopology,
    sink_topology: SinkTopology,
    ledger: Vec<LedgerRecord>,
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceTopology {
    partitions: Vec<PartitionRef>,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(deny_unknown_fields)]
struct PartitionRef {
    topic: String,
    partition: i32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct RunIdentity {
    deployment_id: String,
    pipeline_incarnation: String,
    pipeline_identity: String,
    sink_id: String,
    operator_id: String,
    output_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireIdMap {
    ids_16: Vec<WireHexId>,
    ids_32: Vec<WireHexId>,
    u64_values: Vec<WireU64Id>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireHexId {
    label: String,
    hex: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireU64Id {
    label: String,
    value: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OperationGroupKey {
    logical_key: String,
    canonical_hex: String,
}

#[derive(Debug)]
struct WireIdLookup {
    ids_16: BTreeMap<String, [u8; 16]>,
    labels_16: BTreeMap<[u8; 16], String>,
    ids_32: BTreeMap<String, [u8; 32]>,
    labels_32: BTreeMap<[u8; 32], String>,
    u64_values: BTreeMap<String, u64>,
    known_u64_values: BTreeSet<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SinkTopology {
    topic: String,
    partitioning_abi: String,
    topology_digest: String,
    baseline: Vec<PartitionCut>,
    shards: Vec<SinkShard>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SinkShard {
    shard_id: String,
    partitions: Vec<i32>,
    vnodes: Vec<u16>,
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
struct Case {
    id: String,
    frozen_source_cut: Vec<PartitionCut>,
    durable_source_cut: Vec<PartitionCut>,
    frozen_sink_cut: Vec<PartitionCut>,
    consumed_sink_cut: Vec<PartitionCut>,
    checkpoint_evidence: Vec<CheckpointEvidence>,
    assignment_authority_evidence: Vec<AssignmentAuthority>,
    bootstrap_observation: Option<BootstrapObservation>,
    interval_markers: Vec<IntervalMarker>,
    data_records: Vec<DataRecord>,
    expected: ExpectedOutcome,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(deny_unknown_fields)]
struct PartitionCut {
    topic: String,
    partition: i32,
    exclusive_end: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointEvidence {
    immutable: bool,
    run: RunIdentity,
    checkpoint_id: String,
    epoch: u64,
    terminal: String,
    purpose: String,
    committed_index_digest: String,
    base_assignment: AssignmentRef,
    sealed_source_cut: Vec<PartitionCut>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct AssignmentRef {
    version: u64,
    digest: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AssignmentAuthority {
    run: RunIdentity,
    version: u64,
    digest: String,
    owners: Vec<VnodeOwner>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct VnodeOwner {
    vnode: u16,
    node_id: String,
    boot_incarnation: String,
    process_term: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapObservation {
    observed_source_baseline: Vec<PartitionCut>,
    source_deliveries: u64,
    state_items: u64,
    timer_items: u64,
    outputs_computed: u64,
    outputs_queued: u64,
    sink_records_admitted: u64,
    sink_records_flushed: u64,
    checkpoint_id: String,
    checkpoint_epoch: u64,
    first_marker_interval: String,
    baseline_observed_order: u64,
    empty_unactivated_flush_order: u64,
    checkpoint_commit_order: u64,
    first_marker_confirmation_order: u64,
    data_admission_opened_order: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(deny_unknown_fields)]
struct BrokerPosition {
    topic: String,
    partition: i32,
    offset: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IntervalMarker {
    broker: BrokerPosition,
    wire_envelope_hex: String,
    envelope_version: u16,
    envelope_kind: String,
    interval_id: String,
    predecessor_interval: Option<String>,
    provenance: RunIdentity,
    shard_id: String,
    vnodes: Vec<u16>,
    writer: WriterIdentity,
    current_assignment: AssignmentRef,
    recovery: RecoveryReference,
    abi: AbiBinding,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct AbiBinding {
    key_vnode_abi: String,
    vnode_count: u16,
    partitioning_abi: String,
    topology_digest: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct WriterIdentity {
    node_id: String,
    boot_incarnation: String,
    process_term: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct RecoveryReference {
    checkpoint_id: String,
    checkpoint_epoch: u64,
    committed_index_digest: String,
    base_assignment: AssignmentRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct DataRecord {
    broker: BrokerPosition,
    wire_envelope_hex: String,
    envelope_version: u16,
    envelope_kind: String,
    operation_id: String,
    writer_interval: String,
    admission_sequence: u64,
    raw_payload: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedOutcome {
    classification: Classification,
    diagnostics: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct AggregatePayload {
    logical_key: String,
    count: u64,
    sum: i64,
}

#[derive(Debug)]
struct ExpectedOperation {
    payload: AggregatePayload,
    raw_payload: String,
    source_key: CutKey,
    causal_offset: i64,
    vnode: u16,
}

#[derive(Debug)]
struct ExpectedModel {
    operations: BTreeMap<String, ExpectedOperation>,
    final_operation_ids: BTreeSet<String>,
    final_counts: BTreeMap<String, u64>,
}

#[derive(Debug, Eq, PartialEq)]
struct Outcome {
    classification: Classification,
    diagnostics: BTreeSet<String>,
}

type CutKey = (String, i32);

struct Topology {
    topic: String,
    baseline: BTreeMap<CutKey, i64>,
    partition_shard: BTreeMap<i32, String>,
    shard_vnodes: BTreeMap<String, BTreeSet<u16>>,
    vnode_shard: BTreeMap<u16, String>,
}

struct CheckpointView<'a> {
    evidence: &'a CheckpointEvidence,
    sealed_source: BTreeMap<CutKey, i64>,
}

struct AssignmentView<'a> {
    evidence: &'a AssignmentAuthority,
    owner_by_vnode: BTreeMap<u16, &'a VnodeOwner>,
}

pub(super) fn verify(bytes: &[u8]) -> Result<FixtureSummary, CheckErrors> {
    let fixture: Fixture = serde_json::from_slice(bytes)
        .map_err(|error| CheckErrors::one(format!("decode oracle fixture: {error}")))?;
    let mut errors = Vec::new();

    validate_fixture_metadata(&fixture, &mut errors);
    let inputs = prepare_fixture(&fixture, &mut errors)?;
    let summary = evaluate_cases(&fixture, &inputs, &mut errors);
    if errors.is_empty() {
        Ok(summary)
    } else {
        Err(CheckErrors::many(errors))
    }
}

struct FixtureInputs {
    wire_ids: WireIdLookup,
    operation_group_keys: BTreeMap<String, Vec<u8>>,
    operation_id_context: provenance_v1::GroupedCountSumOperationIdContextV1,
    source_partitions: BTreeSet<CutKey>,
    topology: Topology,
}

fn validate_fixture_metadata(fixture: &Fixture, errors: &mut Vec<String>) {
    expect_equal(
        errors,
        "fixture.schema_version",
        &fixture.schema_version,
        SCHEMA_VERSION,
    );
    expect_equal(errors, "fixture.notice", &fixture.notice, NOTICE);
    expect_equal(
        errors,
        "fixture.scope",
        &fixture.scope,
        "semantic_fixture_only",
    );
    expect_equal(
        errors,
        "fixture.model",
        &fixture.model,
        "grouped_count_star_sum_exact_integer_snapshot_at_least_once",
    );
    expect_equal(
        errors,
        "fixture.operation_identity_model",
        &fixture.operation_identity_model,
        "operation-id-v1-with-explicit-abi-v1-group-key-bytes",
    );
    if fixture.certification_eligible {
        errors.push("fixture.certification_eligible must remain false".to_owned());
    }
    expect_equal(errors, "fixture.key_vnode_abi", &fixture.key_vnode_abi, ABI);
    expect_equal(
        errors,
        "fixture.sink_topology.partitioning_abi",
        &fixture.sink_topology.partitioning_abi,
        PARTITIONING_ABI,
    );
    if fixture.sink_topology.topology_digest.is_empty() {
        errors.push("fixture.sink_topology.topology_digest must not be empty".to_owned());
    }
    if fixture.cases.is_empty() {
        errors.push("oracle fixture must contain cases".to_owned());
    }
    if fixture.vnode_count == 0 {
        errors.push("fixture.vnode_count must be positive".to_owned());
    }
    if [
        &fixture.expected_run.deployment_id,
        &fixture.expected_run.pipeline_incarnation,
        &fixture.expected_run.pipeline_identity,
        &fixture.expected_run.sink_id,
        &fixture.expected_run.operator_id,
        &fixture.expected_run.output_id,
    ]
    .into_iter()
    .any(String::is_empty)
    {
        errors.push("fixture.expected_run fields must not be empty".to_owned());
    }
}

fn prepare_fixture(
    fixture: &Fixture,
    errors: &mut Vec<String>,
) -> Result<FixtureInputs, CheckErrors> {
    let wire_ids = require_fixture_input(validate_wire_id_map(fixture), errors)?;
    let operation_group_keys =
        require_fixture_input(validate_operation_group_keys(fixture), errors)?;
    let operation_id_context =
        require_fixture_input(operation_id_context(fixture, &wire_ids), errors)?;
    let source_partitions = require_fixture_input(validate_source_topology(fixture), errors)?;
    let topology = require_fixture_input(validate_topology(fixture), errors)?;

    validate_ledger(fixture, &source_partitions, errors);
    if !errors.is_empty() {
        return Err(CheckErrors::many(std::mem::take(errors)));
    }

    Ok(FixtureInputs {
        wire_ids,
        operation_group_keys,
        operation_id_context,
        source_partitions,
        topology,
    })
}

fn require_fixture_input<T>(
    result: Result<T, CheckErrors>,
    errors: &mut Vec<String>,
) -> Result<T, CheckErrors> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            errors.push(error.to_string());
            Err(CheckErrors::many(std::mem::take(errors)))
        }
    }
}

fn evaluate_cases(
    fixture: &Fixture,
    inputs: &FixtureInputs,
    errors: &mut Vec<String>,
) -> FixtureSummary {
    let mut case_ids = BTreeSet::new();
    let mut summary = FixtureSummary {
        cases: fixture.cases.len(),
        model_matches: 0,
        product_failures: 0,
        invalid_runs: 0,
    };
    for case in &fixture.cases {
        if case.id.is_empty() {
            errors.push("oracle case id must not be empty".to_owned());
            continue;
        }
        if !case_ids.insert(case.id.as_str()) {
            errors.push(format!("duplicate oracle case id {:?}", case.id));
            continue;
        }
        if case
            .expected
            .diagnostics
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            != case.expected.diagnostics.len()
        {
            errors.push(format!(
                "case {:?}: expected diagnostics contain duplicates",
                case.id
            ));
            continue;
        }
        evaluate_fixture_case(fixture, inputs, case, &mut summary, errors);
    }
    summary
}

fn evaluate_fixture_case(
    fixture: &Fixture,
    inputs: &FixtureInputs,
    case: &Case,
    summary: &mut FixtureSummary,
    errors: &mut Vec<String>,
) {
    let mut outcome =
        match evaluate_case(fixture, &inputs.source_partitions, &inputs.topology, case) {
            Ok(outcome) => outcome,
            Err(error) => {
                errors.push(format!("case {:?}: {error}", case.id));
                return;
            }
        };

    if outcome.classification != Classification::RunInvalid {
        let wire_diagnostics = evaluate_wire_envelopes(
            fixture,
            case,
            &inputs.wire_ids,
            &inputs.operation_group_keys,
            &inputs.operation_id_context,
        );
        if !wire_diagnostics.is_empty() {
            outcome.classification = Classification::ProductFail;
            outcome.diagnostics.extend(wire_diagnostics);
        }
    }

    let expected_diagnostics = case
        .expected
        .diagnostics
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if outcome.classification != case.expected.classification {
        errors.push(format!(
            "case {:?}: expected {:?}, observed {:?}",
            case.id, case.expected.classification, outcome.classification
        ));
    }
    if outcome.diagnostics != expected_diagnostics {
        errors.push(format!(
            "case {:?}: expected diagnostics {:?}, observed {:?}",
            case.id, expected_diagnostics, outcome.diagnostics
        ));
    }
    match outcome.classification {
        Classification::ModelMatch => summary.model_matches += 1,
        Classification::ProductFail => summary.product_failures += 1,
        Classification::RunInvalid => summary.invalid_runs += 1,
    }
}

fn expected_model(
    fixture: &Fixture,
    source_baseline: &BTreeMap<CutKey, i64>,
    frozen_source: &BTreeMap<CutKey, i64>,
) -> Result<ExpectedModel, CheckErrors> {
    let mut records = fixture.ledger.iter().collect::<Vec<_>>();
    records.sort_by_key(|record| (&record.topic, record.partition, record.offset));
    let mut aggregates = BTreeMap::<String, (u64, i64)>::new();
    let mut operations = BTreeMap::new();

    for record in records {
        let source_key = (record.topic.clone(), record.partition);
        let Some(boundary) = frozen_source.get(&source_key) else {
            return Err(CheckErrors::one(
                "frozen_source_cut does not cover every ledger partition",
            ));
        };
        let Some(baseline) = source_baseline.get(&source_key) else {
            return Err(CheckErrors::one(
                "bootstrap source baseline does not cover every ledger partition",
            ));
        };
        if record.offset < *baseline || record.offset >= *boundary {
            continue;
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
        let payload = AggregatePayload {
            logical_key: record.logical_key.clone(),
            count: *count,
            sum: *sum,
        };
        let raw_payload = serde_json::to_string(&payload)
            .map_err(|error| CheckErrors::one(format!("encode expected payload: {error}")))?;
        let operation_id = fixture_operation_id(&record.logical_key, *count);
        let operation = ExpectedOperation {
            payload,
            raw_payload,
            source_key,
            causal_offset: record.offset,
            vnode: fixture_vnode(&record.logical_key, fixture.vnode_count),
        };
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

fn fixture_vnode(logical_key: &str, vnode_count: u16) -> u16 {
    let byte_sum = logical_key
        .as_bytes()
        .iter()
        .fold(0_u64, |sum, byte| sum.wrapping_add(u64::from(*byte)));
    (byte_sum % u64::from(vnode_count)) as u16
}

#[cfg(test)]
mod tests;
