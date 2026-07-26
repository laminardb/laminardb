use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

use super::{expect_equal, CheckErrors, FixtureSummary, NOTICE};

const SCHEMA_VERSION: &str = "independent-oracle-fixture/v2";
const ABI: &str = "fixture-utf8-byte-sum-mod-v1";
const PARTITIONING_ABI: &str = "fixture-sink-shard-partition-map-v1";
const ENVELOPE_VERSION: u16 = 1;

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
    capsule_digest: String,
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
    capsule_digest: String,
    base_assignment: AssignmentRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct DataRecord {
    broker: BrokerPosition,
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
    expect_equal(
        &mut errors,
        "fixture.schema_version",
        &fixture.schema_version,
        SCHEMA_VERSION,
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
    if fixture.certification_eligible {
        errors.push("fixture.certification_eligible must remain false".to_owned());
    }
    expect_equal(
        &mut errors,
        "fixture.key_vnode_abi",
        &fixture.key_vnode_abi,
        ABI,
    );
    expect_equal(
        &mut errors,
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
    let source_partitions = match validate_source_topology(&fixture) {
        Ok(partitions) => partitions,
        Err(error) => {
            errors.push(error.to_string());
            return Err(CheckErrors::many(errors));
        }
    };
    let topology = match validate_topology(&fixture) {
        Ok(topology) => topology,
        Err(error) => {
            errors.push(error.to_string());
            return Err(CheckErrors::many(errors));
        }
    };
    validate_ledger(&fixture, &source_partitions, &mut errors);
    if !errors.is_empty() {
        return Err(CheckErrors::many(errors));
    }

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
        match evaluate_case(&fixture, &source_partitions, &topology, case) {
            Ok(outcome) => {
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
            Err(error) => errors.push(format!("case {:?}: {error}", case.id)),
        }
    }
    if errors.is_empty() {
        Ok(summary)
    } else {
        Err(CheckErrors::many(errors))
    }
}

fn validate_source_topology(fixture: &Fixture) -> Result<BTreeSet<CutKey>, CheckErrors> {
    if fixture.source_topology.partitions.is_empty() {
        return Err(CheckErrors::one(
            "source topology must contain at least one partition",
        ));
    }
    let mut partitions = BTreeSet::new();
    for partition in &fixture.source_topology.partitions {
        if partition.topic.is_empty()
            || partition.partition < 0
            || !partitions.insert((partition.topic.clone(), partition.partition))
        {
            return Err(CheckErrors::one(
                "source topology contains an empty, negative, or duplicate partition",
            ));
        }
    }
    Ok(partitions)
}

fn validate_topology(fixture: &Fixture) -> Result<Topology, CheckErrors> {
    if fixture.vnode_count == 0 {
        return Err(CheckErrors::one("fixture.vnode_count must be positive"));
    }
    if fixture.sink_topology.topic.is_empty()
        || fixture.sink_topology.baseline.is_empty()
        || fixture.sink_topology.shards.is_empty()
    {
        return Err(CheckErrors::one(
            "sink topology must contain a topic, baseline, and shards",
        ));
    }
    let mut baseline = BTreeMap::new();
    for cut in &fixture.sink_topology.baseline {
        if cut.topic != fixture.sink_topology.topic
            || cut.partition < 0
            || cut.exclusive_end < 0
            || baseline
                .insert((cut.topic.clone(), cut.partition), cut.exclusive_end)
                .is_some()
        {
            return Err(CheckErrors::one(
                "sink topology contains an invalid or duplicate baseline",
            ));
        }
    }
    let mut partition_shard = BTreeMap::new();
    let mut shard_vnodes = BTreeMap::new();
    let mut vnode_shard = BTreeMap::new();
    for shard in &fixture.sink_topology.shards {
        if shard.shard_id.is_empty()
            || shard.partitions.is_empty()
            || shard.vnodes.is_empty()
            || shard_vnodes.contains_key(&shard.shard_id)
        {
            return Err(CheckErrors::one(
                "sink topology contains an empty or duplicate shard",
            ));
        }
        let mut vnodes = BTreeSet::new();
        for vnode in &shard.vnodes {
            if *vnode >= fixture.vnode_count
                || !vnodes.insert(*vnode)
                || vnode_shard.insert(*vnode, shard.shard_id.clone()).is_some()
            {
                return Err(CheckErrors::one(
                    "sink topology contains an invalid or duplicate vnode",
                ));
            }
        }
        for partition in &shard.partitions {
            if *partition < 0
                || partition_shard
                    .insert(*partition, shard.shard_id.clone())
                    .is_some()
            {
                return Err(CheckErrors::one(
                    "sink topology contains an invalid or duplicate partition",
                ));
            }
        }
        shard_vnodes.insert(shard.shard_id.clone(), vnodes);
    }
    if vnode_shard.len() != usize::from(fixture.vnode_count)
        || (0..fixture.vnode_count).any(|vnode| !vnode_shard.contains_key(&vnode))
    {
        return Err(CheckErrors::one(
            "sink topology must assign every fixture vnode exactly once",
        ));
    }
    let baseline_partitions = baseline
        .keys()
        .map(|(_, partition)| *partition)
        .collect::<BTreeSet<_>>();
    if baseline_partitions != partition_shard.keys().copied().collect() {
        return Err(CheckErrors::one(
            "sink topology baseline must cover every sink partition exactly once",
        ));
    }
    Ok(Topology {
        topic: fixture.sink_topology.topic.clone(),
        baseline,
        partition_shard,
        shard_vnodes,
        vnode_shard,
    })
}

fn validate_ledger(
    fixture: &Fixture,
    source_partitions: &BTreeSet<CutKey>,
    errors: &mut Vec<String>,
) {
    let mut event_ids = BTreeSet::new();
    let mut positions = BTreeSet::new();
    let mut key_partitions = BTreeMap::<&str, (&str, i32)>::new();
    for record in &fixture.ledger {
        if record.event_id.is_empty() || record.topic.is_empty() {
            errors.push("ledger contains an empty event_id or topic".to_owned());
        }
        if record.partition < 0 || record.offset < 0 || record.offset == i64::MAX {
            errors.push("ledger contains an invalid partition or offset".to_owned());
        }
        if !source_partitions.contains(&(record.topic.clone(), record.partition)) {
            errors.push("ledger contains a partition outside source topology".to_owned());
        }
        if !event_ids.insert(record.event_id.as_str()) {
            errors.push("ledger contains a duplicate event_id".to_owned());
        }
        if !positions.insert((record.topic.as_str(), record.partition, record.offset)) {
            errors.push("ledger contains a duplicate topic/partition/offset".to_owned());
        }
        let partition = (record.topic.as_str(), record.partition);
        if key_partitions
            .insert(record.logical_key.as_str(), partition)
            .is_some_and(|previous| previous != partition)
        {
            errors.push(
                "fixture routes one logical key through multiple source partitions".to_owned(),
            );
        }
    }
}

fn evaluate_case(
    fixture: &Fixture,
    source_partitions: &BTreeSet<CutKey>,
    topology: &Topology,
    case: &Case,
) -> Result<Outcome, CheckErrors> {
    validate_case_broker_positions(case)?;
    let mut invalid = BTreeSet::new();
    let mut product = BTreeSet::new();
    let frozen_source = controller_cut_map(&case.frozen_source_cut, "frozen_source_cut")?;
    let durable_source = controller_cut_map(&case.durable_source_cut, "durable_source_cut")?;
    let frozen_sink = controller_cut_map(&case.frozen_sink_cut, "frozen_sink_cut")?;
    let consumed_sink = controller_cut_map(&case.consumed_sink_cut, "consumed_sink_cut")?;
    if frozen_source.keys().cloned().collect::<BTreeSet<_>>() != *source_partitions
        || durable_source.keys().cloned().collect::<BTreeSet<_>>() != *source_partitions
    {
        invalid.insert("source_cut_incomplete".to_owned());
    }
    for key in source_partitions {
        let Some(frozen) = frozen_source.get(key) else {
            continue;
        };
        if durable_source
            .get(key)
            .is_none_or(|observed| observed < frozen)
        {
            invalid.insert("source_cut_incomplete".to_owned());
        }
    }
    let sink_partitions = topology.baseline.keys().cloned().collect::<BTreeSet<_>>();
    if frozen_sink.keys().cloned().collect::<BTreeSet<_>>() != sink_partitions
        || consumed_sink.keys().cloned().collect::<BTreeSet<_>>() != sink_partitions
    {
        invalid.insert("sink_cut_incomplete".to_owned());
    }
    for key in topology.baseline.keys() {
        let Some(frozen) = frozen_sink.get(key) else {
            invalid.insert("sink_cut_incomplete".to_owned());
            continue;
        };
        if consumed_sink
            .get(key)
            .is_none_or(|consumed| consumed < frozen)
        {
            invalid.insert("sink_cut_incomplete".to_owned());
        }
    }

    let bootstrap = match &case.bootstrap_observation {
        Some(bootstrap) => Some(bootstrap),
        None => {
            invalid.insert("bootstrap_evidence_missing".to_owned());
            None
        }
    };
    let observed_source_baseline = match bootstrap {
        Some(bootstrap) => controller_cut_map(
            &bootstrap.observed_source_baseline,
            "bootstrap_observed_source_baseline",
        )?,
        None => BTreeMap::new(),
    };
    if bootstrap.is_some()
        && observed_source_baseline
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != *source_partitions
    {
        invalid.insert("bootstrap_source_baseline_incomplete".to_owned());
    }
    if bootstrap.is_some_and(|bootstrap| {
        bootstrap.checkpoint_id.is_empty() || bootstrap.first_marker_interval.is_empty()
    }) {
        invalid.insert("bootstrap_evidence_incomplete".to_owned());
    }

    let mut checkpoint_records = BTreeMap::new();
    for checkpoint in &case.checkpoint_evidence {
        let key = (checkpoint.checkpoint_id.clone(), checkpoint.epoch);
        if checkpoint_records.insert(key, checkpoint).is_some() {
            return Err(CheckErrors::one(
                "checkpoint_evidence contains a duplicate checkpoint identity",
            ));
        }
    }
    let mut required_checkpoint_keys = BTreeSet::new();
    if let Some(bootstrap) = bootstrap {
        required_checkpoint_keys
            .insert((bootstrap.checkpoint_id.clone(), bootstrap.checkpoint_epoch));
    }
    for marker in &case.interval_markers {
        required_checkpoint_keys.insert((
            marker.recovery.checkpoint_id.clone(),
            marker.recovery.checkpoint_epoch,
        ));
    }
    let mut checkpoint_views = BTreeMap::new();
    for key in &required_checkpoint_keys {
        let Some(checkpoint) = checkpoint_records.get(key).copied() else {
            invalid.insert("checkpoint_evidence_missing".to_owned());
            continue;
        };
        if checkpoint.checkpoint_id.is_empty()
            || checkpoint.capsule_digest.is_empty()
            || checkpoint.base_assignment.version == 0
            || checkpoint.base_assignment.digest.is_empty()
        {
            invalid.insert("checkpoint_evidence_incomplete".to_owned());
        }
        if checkpoint.run != fixture.expected_run {
            invalid.insert("checkpoint_run_identity_mismatch".to_owned());
        }
        let sealed_source = evidence_cut_map(
            &checkpoint.sealed_source_cut,
            "checkpoint_sealed_source_cut",
            &mut product,
        );
        if sealed_source.keys().cloned().collect::<BTreeSet<_>>() != *source_partitions {
            invalid.insert("checkpoint_source_cut_incomplete".to_owned());
        }
        checkpoint_views.insert(
            key.clone(),
            CheckpointView {
                evidence: checkpoint,
                sealed_source,
            },
        );
    }

    let mut assignment_records = BTreeMap::new();
    for assignment in &case.assignment_authority_evidence {
        if assignment_records
            .insert(assignment.version, assignment)
            .is_some()
        {
            return Err(CheckErrors::one(
                "assignment_authority_evidence contains a duplicate version",
            ));
        }
    }
    let mut required_assignment_versions = case
        .interval_markers
        .iter()
        .map(|marker| marker.current_assignment.version)
        .collect::<BTreeSet<_>>();
    required_assignment_versions.extend(
        checkpoint_views
            .values()
            .map(|checkpoint| checkpoint.evidence.base_assignment.version),
    );
    let mut assignment_views = BTreeMap::new();
    for version in required_assignment_versions {
        let Some(assignment) = assignment_records.get(&version).copied() else {
            invalid.insert("assignment_evidence_missing".to_owned());
            continue;
        };
        let mut owner_by_vnode = BTreeMap::new();
        for owner in &assignment.owners {
            if owner.vnode >= fixture.vnode_count
                || owner_by_vnode.insert(owner.vnode, owner).is_some()
            {
                product.insert("contradictory_assignment_owner".to_owned());
            }
            if owner.node_id.is_empty()
                || owner.boot_incarnation.is_empty()
                || owner.process_term.is_empty()
            {
                invalid.insert("assignment_evidence_incomplete".to_owned());
            }
        }
        if assignment.version == 0
            || assignment.digest.is_empty()
            || (0..fixture.vnode_count).any(|vnode| !owner_by_vnode.contains_key(&vnode))
        {
            invalid.insert("assignment_evidence_incomplete".to_owned());
        }
        if assignment.run != fixture.expected_run {
            invalid.insert("assignment_run_identity_mismatch".to_owned());
        }
        assignment_views.insert(
            assignment.version,
            AssignmentView {
                evidence: assignment,
                owner_by_vnode,
            },
        );
    }

    if !invalid.is_empty() {
        return Ok(Outcome {
            classification: Classification::RunInvalid,
            diagnostics: invalid,
        });
    }

    let bootstrap = bootstrap.expect("missing bootstrap was classified invalid");
    let bootstrap_key = (bootstrap.checkpoint_id.clone(), bootstrap.checkpoint_epoch);
    let bootstrap_checkpoint = checkpoint_views
        .get(&bootstrap_key)
        .expect("missing bootstrap checkpoint was classified invalid");
    for (key, checkpoint) in &checkpoint_views {
        validate_checkpoint(
            checkpoint.evidence,
            &checkpoint.sealed_source,
            &observed_source_baseline,
            &frozen_source,
            key == &bootstrap_key,
            &mut product,
        );
    }
    for checkpoint in checkpoint_views.values() {
        let base_assignment = assignment_views
            .get(&checkpoint.evidence.base_assignment.version)
            .expect("missing checkpoint base assignment was classified invalid");
        if base_assignment.evidence.digest != checkpoint.evidence.base_assignment.digest {
            product.insert("checkpoint_base_assignment_mismatch".to_owned());
        }
    }
    validate_bootstrap(
        &bootstrap_checkpoint.sealed_source,
        bootstrap,
        &observed_source_baseline,
        &mut product,
    );

    let expected = expected_model(fixture, &observed_source_baseline, &frozen_source)?;
    evaluate_output(
        fixture,
        topology,
        case,
        bootstrap,
        &checkpoint_views,
        &assignment_views,
        &frozen_sink,
        &expected,
        &mut product,
    );

    Ok(Outcome {
        classification: if product.is_empty() {
            Classification::ModelMatch
        } else {
            Classification::ProductFail
        },
        diagnostics: product,
    })
}

fn validate_case_broker_positions(case: &Case) -> Result<(), CheckErrors> {
    let mut positions = BTreeSet::new();
    for position in case
        .interval_markers
        .iter()
        .map(|marker| &marker.broker)
        .chain(case.data_records.iter().map(|record| &record.broker))
    {
        if position.topic.is_empty()
            || position.partition < 0
            || position.offset < 0
            || !positions.insert(position.clone())
        {
            return Err(CheckErrors::one(
                "output evidence contains an empty, negative, or duplicate broker position",
            ));
        }
    }
    Ok(())
}

fn controller_cut_map(
    cuts: &[PartitionCut],
    label: &str,
) -> Result<BTreeMap<CutKey, i64>, CheckErrors> {
    let mut result = BTreeMap::new();
    for cut in cuts {
        let key = (cut.topic.clone(), cut.partition);
        if cut.topic.is_empty()
            || cut.partition < 0
            || cut.exclusive_end < 0
            || result.insert(key, cut.exclusive_end).is_some()
        {
            return Err(CheckErrors::one(format!(
                "{label} contains an empty, negative, or duplicate partition cut"
            )));
        }
    }
    Ok(result)
}

fn evidence_cut_map(
    cuts: &[PartitionCut],
    label: &str,
    product: &mut BTreeSet<String>,
) -> BTreeMap<CutKey, i64> {
    let mut result = BTreeMap::new();
    for cut in cuts {
        let key = (cut.topic.clone(), cut.partition);
        if cut.partition < 0
            || cut.exclusive_end < 0
            || result.insert(key, cut.exclusive_end).is_some()
        {
            product.insert(format!("contradictory_{label}"));
        }
    }
    result
}

fn validate_checkpoint(
    checkpoint: &CheckpointEvidence,
    sealed_source: &BTreeMap<CutKey, i64>,
    bootstrap_source_baseline: &BTreeMap<CutKey, i64>,
    frozen_source: &BTreeMap<CutKey, i64>,
    is_bootstrap: bool,
    product: &mut BTreeSet<String>,
) {
    if !checkpoint.immutable {
        product.insert("checkpoint_evidence_not_immutable".to_owned());
    }
    if checkpoint.terminal != "Commit" {
        product.insert("recovery_checkpoint_not_committed".to_owned());
    }
    let expected_purpose = if is_bootstrap {
        "zero_input_bootstrap"
    } else {
        "recovery_base"
    };
    if checkpoint.purpose != expected_purpose {
        product.insert("checkpoint_purpose_mismatch".to_owned());
    }
    for (key, sealed) in sealed_source {
        if bootstrap_source_baseline
            .get(key)
            .is_some_and(|baseline| sealed < baseline)
        {
            product.insert("recovery_cut_before_bootstrap_baseline".to_owned());
        }
        if frozen_source.get(key).is_some_and(|frozen| sealed > frozen) {
            product.insert("recovery_cut_after_frozen_source".to_owned());
        }
    }
}

fn validate_bootstrap(
    checkpoint_source_cut: &BTreeMap<CutKey, i64>,
    bootstrap: &BootstrapObservation,
    observed_source_baseline: &BTreeMap<CutKey, i64>,
    product: &mut BTreeSet<String>,
) {
    if observed_source_baseline != checkpoint_source_cut {
        product.insert("bootstrap_source_baseline_mismatch".to_owned());
    }
    if [
        bootstrap.source_deliveries,
        bootstrap.state_items,
        bootstrap.timer_items,
        bootstrap.outputs_computed,
        bootstrap.outputs_queued,
        bootstrap.sink_records_admitted,
        bootstrap.sink_records_flushed,
    ]
    .into_iter()
    .any(|count| count != 0)
    {
        product.insert("bootstrap_not_empty".to_owned());
    }
    if !(bootstrap.baseline_observed_order < bootstrap.empty_unactivated_flush_order
        && bootstrap.empty_unactivated_flush_order < bootstrap.checkpoint_commit_order
        && bootstrap.checkpoint_commit_order < bootstrap.first_marker_confirmation_order
        && bootstrap.first_marker_confirmation_order < bootstrap.data_admission_opened_order)
    {
        product.insert("bootstrap_order_invalid".to_owned());
    }
}

#[allow(clippy::too_many_arguments)]
fn evaluate_output(
    fixture: &Fixture,
    topology: &Topology,
    case: &Case,
    bootstrap: &BootstrapObservation,
    checkpoint_views: &BTreeMap<(String, u64), CheckpointView<'_>>,
    assignment_views: &BTreeMap<u64, AssignmentView<'_>>,
    frozen_sink: &BTreeMap<CutKey, i64>,
    expected: &ExpectedModel,
    product: &mut BTreeSet<String>,
) {
    let mut marker_index = BTreeMap::<(String, String, i32), &IntervalMarker>::new();
    let mut common_markers = BTreeMap::<(String, String), &IntervalMarker>::new();
    let mut partition_marker_chains = BTreeMap::<(String, i32), Vec<&IntervalMarker>>::new();

    for marker in &case.interval_markers {
        validate_broker_position(topology, frozen_sink, &marker.broker, product);
        if marker.envelope_version != ENVELOPE_VERSION {
            product.insert("unsupported_envelope_version".to_owned());
        }
        if marker.envelope_kind != "interval_marker" {
            product.insert("envelope_kind_mismatch".to_owned());
        }
        if marker.interval_id.is_empty()
            || marker.writer.node_id.is_empty()
            || marker.writer.boot_incarnation.is_empty()
            || marker.writer.process_term.is_empty()
        {
            product.insert("marker_authority_incomplete".to_owned());
        }
        let expected_shard = topology.partition_shard.get(&marker.broker.partition);
        if expected_shard != Some(&marker.shard_id) {
            product.insert("marker_shard_mismatch".to_owned());
        }
        let marker_vnodes = marker.vnodes.iter().copied().collect::<BTreeSet<_>>();
        if marker_vnodes.len() != marker.vnodes.len()
            || topology.shard_vnodes.get(&marker.shard_id) != Some(&marker_vnodes)
        {
            product.insert("marker_vnode_mismatch".to_owned());
        }
        if marker.provenance != fixture.expected_run {
            product.insert("marker_run_identity_mismatch".to_owned());
        }
        let assignment = assignment_views
            .get(&marker.current_assignment.version)
            .expect("missing marker assignment was classified invalid");
        if marker.current_assignment.digest != assignment.evidence.digest {
            product.insert("marker_assignment_mismatch".to_owned());
        }
        if marker.abi.key_vnode_abi != ABI
            || marker.abi.vnode_count != fixture.vnode_count
            || marker.abi.partitioning_abi != topology_partitioning_abi(fixture)
            || marker.abi.topology_digest != fixture.sink_topology.topology_digest
        {
            product.insert("marker_abi_mismatch".to_owned());
        }
        let checkpoint_key = (
            marker.recovery.checkpoint_id.clone(),
            marker.recovery.checkpoint_epoch,
        );
        let checkpoint = checkpoint_views
            .get(&checkpoint_key)
            .expect("missing marker checkpoint was classified invalid");
        if marker.recovery.capsule_digest != checkpoint.evidence.capsule_digest
            || marker.recovery.base_assignment != checkpoint.evidence.base_assignment
        {
            product.insert("marker_recovery_mismatch".to_owned());
        }
        if marker.current_assignment.version < checkpoint.evidence.base_assignment.version {
            product.insert("marker_assignment_precedes_recovery_base".to_owned());
        }
        if marker.predecessor_interval.as_deref() == Some(marker.interval_id.as_str()) {
            product.insert("marker_predecessor_invalid".to_owned());
        }
        for vnode in &marker_vnodes {
            if assignment
                .owner_by_vnode
                .get(vnode)
                .is_none_or(|owner| !writer_matches(&marker.writer, owner))
            {
                product.insert("marker_writer_not_current_owner".to_owned());
            }
        }
        let per_partition_key = (
            marker.shard_id.clone(),
            marker.interval_id.clone(),
            marker.broker.partition,
        );
        if marker_index.insert(per_partition_key, marker).is_some() {
            product.insert("duplicate_interval_marker".to_owned());
        }
        let common_key = (marker.shard_id.clone(), marker.interval_id.clone());
        if common_markers
            .insert(common_key, marker)
            .is_some_and(|previous| !same_common_marker(previous, marker))
        {
            product.insert("contradictory_interval_marker".to_owned());
        }
        partition_marker_chains
            .entry((marker.shard_id.clone(), marker.broker.partition))
            .or_default()
            .push(marker);
    }
    for (shard_id, interval_id) in common_markers.keys() {
        for partition in topology
            .partition_shard
            .iter()
            .filter_map(|(partition, shard)| (shard == shard_id).then_some(*partition))
        {
            if !marker_index.contains_key(&(shard_id.clone(), interval_id.clone(), partition)) {
                product.insert("interval_marker_coverage_incomplete".to_owned());
            }
        }
    }
    let mut shard_chain_sequences = BTreeMap::<String, Vec<String>>::new();
    for (partition, shard_id) in &topology.partition_shard {
        let key = (shard_id.clone(), *partition);
        let Some(chain) = partition_marker_chains.get_mut(&key) else {
            product.insert("bootstrap_first_marker_missing".to_owned());
            continue;
        };
        chain.sort_by_key(|marker| marker.broker.offset);
        let first = chain[0];
        if first.predecessor_interval.is_some()
            || first.interval_id != bootstrap.first_marker_interval
        {
            product.insert("bootstrap_first_marker_mismatch".to_owned());
        }
        if first.recovery.checkpoint_id != bootstrap.checkpoint_id
            || first.recovery.checkpoint_epoch != bootstrap.checkpoint_epoch
        {
            product.insert("bootstrap_first_marker_checkpoint_mismatch".to_owned());
        }
        for pair in chain.windows(2) {
            if pair[1].predecessor_interval.as_deref() != Some(pair[0].interval_id.as_str()) {
                product.insert("marker_chain_invalid".to_owned());
            }
            if pair[1].current_assignment.version < pair[0].current_assignment.version {
                product.insert("marker_assignment_version_regressed".to_owned());
            }
        }
        let sequence = chain
            .iter()
            .map(|marker| marker.interval_id.clone())
            .collect::<Vec<_>>();
        if shard_chain_sequences
            .insert(shard_id.clone(), sequence.clone())
            .is_some_and(|previous| previous != sequence)
        {
            product.insert("marker_chain_inconsistent".to_owned());
        }
    }

    let mut observed_payloads = BTreeMap::<String, String>::new();
    let mut observed_final = BTreeSet::new();
    let mut sequence_groups = BTreeMap::<(String, String), Vec<&DataRecord>>::new();
    let mut group_versions = BTreeMap::<(String, String, String), Vec<(u64, u64)>>::new();

    for record in &case.data_records {
        validate_broker_position(topology, frozen_sink, &record.broker, product);
        if record.envelope_version != ENVELOPE_VERSION {
            product.insert("unsupported_envelope_version".to_owned());
        }
        if record.envelope_kind != "data" {
            product.insert("envelope_kind_mismatch".to_owned());
        }
        if record.writer_interval.is_empty() {
            product.insert("data_authority_incomplete".to_owned());
        }
        let shard_id = topology.partition_shard.get(&record.broker.partition);
        if let Some(shard_id) = shard_id {
            sequence_groups
                .entry((shard_id.clone(), record.writer_interval.clone()))
                .or_default()
                .push(record);
            if let Some(operation) = expected.operations.get(&record.operation_id) {
                group_versions
                    .entry((
                        shard_id.clone(),
                        record.writer_interval.clone(),
                        operation.payload.logical_key.clone(),
                    ))
                    .or_default()
                    .push((record.admission_sequence, operation.payload.count));
            }
            let marker_key = (
                shard_id.clone(),
                record.writer_interval.clone(),
                record.broker.partition,
            );
            match marker_index.get(&marker_key) {
                Some(marker) => {
                    if marker.broker.offset >= record.broker.offset {
                        product.insert("marker_not_before_data".to_owned());
                    }
                    if let Some(operation) = expected.operations.get(&record.operation_id) {
                        if marker.shard_id
                            != topology
                                .vnode_shard
                                .get(&operation.vnode)
                                .cloned()
                                .unwrap_or_default()
                            || !marker.vnodes.contains(&operation.vnode)
                        {
                            product.insert("data_vnode_shard_mismatch".to_owned());
                        }
                        let checkpoint_key = (
                            marker.recovery.checkpoint_id.clone(),
                            marker.recovery.checkpoint_epoch,
                        );
                        let checkpoint = checkpoint_views
                            .get(&checkpoint_key)
                            .expect("missing marker checkpoint was classified invalid");
                        if checkpoint
                            .sealed_source
                            .get(&operation.source_key)
                            .is_some_and(|sealed| operation.causal_offset < *sealed)
                        {
                            product.insert("operation_precedes_recovery_cut".to_owned());
                        }
                    }
                }
                None => {
                    product.insert("missing_interval_marker".to_owned());
                }
            }
            for successor in case.interval_markers.iter().filter(|marker| {
                marker.broker.partition == record.broker.partition
                    && marker.shard_id == *shard_id
                    && marker.predecessor_interval.as_deref()
                        == Some(record.writer_interval.as_str())
            }) {
                if record.broker.offset > successor.broker.offset {
                    product.insert("predecessor_data_after_successor_marker".to_owned());
                }
            }
        }

        match expected.operations.get(&record.operation_id) {
            Some(operation) => {
                if record.raw_payload.as_bytes() == operation.raw_payload.as_bytes() {
                    if expected.final_operation_ids.contains(&record.operation_id) {
                        observed_final.insert(record.operation_id.clone());
                    }
                } else {
                    product.insert("raw_payload_mismatch".to_owned());
                    match serde_json::from_str::<AggregatePayload>(&record.raw_payload) {
                        Ok(payload) if payload != operation.payload => {
                            product.insert("aggregate_divergence".to_owned());
                        }
                        Err(_) => {
                            product.insert("malformed_output".to_owned());
                        }
                        Ok(_) => {}
                    }
                }
            }
            None => match serde_json::from_str::<AggregatePayload>(&record.raw_payload) {
                Ok(payload)
                    if expected
                        .final_counts
                        .get(&payload.logical_key)
                        .is_some_and(|final_count| payload.count > *final_count) =>
                {
                    product.insert("aggregate_divergence".to_owned());
                }
                Ok(_) => {
                    product.insert("extra_output".to_owned());
                }
                Err(_) => {
                    product.insert("malformed_output".to_owned());
                }
            },
        }
        match observed_payloads.get(&record.operation_id) {
            Some(previous) if previous.as_bytes() != record.raw_payload.as_bytes() => {
                product.insert("conflicting_operation_identity".to_owned());
            }
            Some(_) => {}
            None => {
                observed_payloads.insert(record.operation_id.clone(), record.raw_payload.clone());
            }
        }
    }

    for records in sequence_groups.values_mut() {
        if records.iter().map(|record| record.admission_sequence).min() != Some(0) {
            product.insert("admission_sequence_does_not_start_at_zero".to_owned());
        }
        let mut sequences = BTreeSet::new();
        for record in records.iter() {
            if !sequences.insert(record.admission_sequence) {
                product.insert("admission_sequence_not_unique".to_owned());
            }
        }
        records.sort_by_key(|record| (record.broker.partition, record.broker.offset));
        for partition_records in
            records.chunk_by(|left, right| left.broker.partition == right.broker.partition)
        {
            if partition_records
                .windows(2)
                .any(|window| window[0].admission_sequence >= window[1].admission_sequence)
            {
                product.insert("admission_sequence_not_monotonic".to_owned());
            }
        }
    }
    for versions in group_versions.values_mut() {
        versions.sort_by_key(|(sequence, _)| *sequence);
        if versions.windows(2).any(|window| window[0].1 >= window[1].1) {
            product.insert("group_version_not_increasing".to_owned());
        }
    }
    if expected
        .final_operation_ids
        .iter()
        .any(|operation_id| !observed_final.contains(operation_id))
    {
        product.insert("missing_final_state".to_owned());
    }
}

fn validate_broker_position(
    topology: &Topology,
    frozen_sink: &BTreeMap<CutKey, i64>,
    position: &BrokerPosition,
    product: &mut BTreeSet<String>,
) {
    let key = (position.topic.clone(), position.partition);
    if position.topic != topology.topic
        || position.partition < 0
        || position.offset < 0
        || topology
            .baseline
            .get(&key)
            .is_none_or(|baseline| position.offset < *baseline)
        || frozen_sink
            .get(&key)
            .is_none_or(|frozen| position.offset >= *frozen)
    {
        product.insert("output_beyond_frozen_sink_cut".to_owned());
    }
}

fn writer_matches(writer: &WriterIdentity, owner: &VnodeOwner) -> bool {
    writer.node_id == owner.node_id
        && writer.boot_incarnation == owner.boot_incarnation
        && writer.process_term == owner.process_term
}

fn same_common_marker(left: &IntervalMarker, right: &IntervalMarker) -> bool {
    left.envelope_version == right.envelope_version
        && left.envelope_kind == right.envelope_kind
        && left.interval_id == right.interval_id
        && left.predecessor_interval == right.predecessor_interval
        && left.provenance == right.provenance
        && left.shard_id == right.shard_id
        && left.vnodes == right.vnodes
        && left.writer == right.writer
        && left.current_assignment == right.current_assignment
        && left.recovery == right.recovery
        && left.abi == right.abi
}

fn topology_partitioning_abi(fixture: &Fixture) -> &str {
    &fixture.sink_topology.partitioning_abi
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
mod tests {
    use serde_json::Value;

    use super::*;

    const FIXTURE: &[u8] = include_bytes!("../fixtures/grouped-count-sum-alo-v2.json");

    fn mutated_outcome(mutate: impl FnOnce(&mut Value)) -> Outcome {
        let mut value: Value = serde_json::from_slice(FIXTURE).unwrap();
        mutate(&mut value);
        let fixture: Fixture = serde_json::from_value(value).unwrap();
        let source_partitions = validate_source_topology(&fixture).unwrap();
        let topology = validate_topology(&fixture).unwrap();
        evaluate_case(&fixture, &source_partitions, &topology, &fixture.cases[0]).unwrap()
    }

    fn mutated_verify_error(mutate: impl FnOnce(&mut Value)) -> String {
        let mut value: Value = serde_json::from_slice(FIXTURE).unwrap();
        mutate(&mut value);
        verify(&serde_json::to_vec(&value).unwrap())
            .unwrap_err()
            .to_string()
    }

    fn assert_diagnostic(
        classification: Classification,
        diagnostic: &str,
        mutate: impl FnOnce(&mut Value),
    ) {
        let outcome = mutated_outcome(mutate);
        assert_eq!(
            outcome.classification, classification,
            "expected diagnostic {diagnostic:?}: {outcome:?}"
        );
        assert!(outcome.diagnostics.contains(diagnostic), "{outcome:?}");
    }

    fn assert_exact_diagnostics(
        classification: Classification,
        diagnostics: &[&str],
        mutate: impl FnOnce(&mut Value),
    ) {
        let outcome = mutated_outcome(mutate);
        assert_eq!(outcome.classification, classification, "{outcome:?}");
        assert_eq!(
            outcome.diagnostics,
            diagnostics
                .iter()
                .map(|diagnostic| (*diagnostic).to_owned())
                .collect::<BTreeSet<_>>()
        );
    }

    fn configure_two_shards(value: &mut Value) {
        value["sink_topology"]["topology_digest"] =
            Value::String("sha256:fixture-topology-two-shards".to_owned());
        value["sink_topology"]["shards"] = serde_json::json!([
            {
                "shard_id": "sink-shard-alpha",
                "partitions": [0],
                "vnodes": [2, 3]
            },
            {
                "shard_id": "sink-shard-beta",
                "partitions": [1],
                "vnodes": [0, 1]
            }
        ]);
        for (index, shard, vnodes) in [
            (0, "sink-shard-alpha", serde_json::json!([2, 3])),
            (1, "sink-shard-beta", serde_json::json!([0, 1])),
            (2, "sink-shard-alpha", serde_json::json!([2, 3])),
            (3, "sink-shard-beta", serde_json::json!([0, 1])),
        ] {
            value["cases"][0]["interval_markers"][index]["shard_id"] =
                Value::String(shard.to_owned());
            value["cases"][0]["interval_markers"][index]["vnodes"] = vnodes;
            value["cases"][0]["interval_markers"][index]["abi"]["topology_digest"] =
                Value::String("sha256:fixture-topology-two-shards".to_owned());
        }
        value["cases"][0]["data_records"][1]["admission_sequence"] = Value::from(0);
        value["cases"][0]["data_records"][3]["admission_sequence"] = Value::from(0);
    }

    #[test]
    fn canonical_fixture_proves_rebalance_replay_gaps_and_exact_duplicate() {
        assert_eq!(
            verify(FIXTURE).unwrap(),
            FixtureSummary {
                cases: 1,
                model_matches: 1,
                product_failures: 0,
                invalid_runs: 0,
            }
        );
        let fixture: Fixture = serde_json::from_slice(FIXTURE).unwrap();
        let case = &fixture.cases[0];
        assert_ne!(
            case.checkpoint_evidence[0].base_assignment.version,
            case.assignment_authority_evidence[1].version
        );
        assert_eq!(fixture_vnode("alpha", 4), 2);
        assert_eq!(fixture_vnode("beta", 4), 0);
        assert_eq!(fixture.source_topology.partitions.len(), 3);
        assert!(case
            .bootstrap_observation
            .as_ref()
            .unwrap()
            .observed_source_baseline
            .iter()
            .any(|cut| cut.partition == 2 && cut.exclusive_end == 9));
        assert_eq!(
            case.data_records
                .iter()
                .filter(|record| record.operation_id == "fixture/alpha/count/2")
                .count(),
            2
        );
        let successor_cut = case.checkpoint_evidence[1].sealed_source_cut[0].exclusive_end;
        let replay_cause = fixture
            .ledger
            .iter()
            .find(|record| record.logical_key == "alpha" && record.offset == 1)
            .unwrap()
            .offset;
        assert_eq!(replay_cause, successor_cut);
    }

    #[test]
    fn source_inventory_and_nonzero_bootstrap_baselines_are_authoritative() {
        assert_diagnostic(
            Classification::RunInvalid,
            "checkpoint_source_cut_incomplete",
            |value| {
                value["cases"][0]["checkpoint_evidence"][1]["sealed_source_cut"]
                    .as_array_mut()
                    .unwrap()
                    .pop();
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "recovery_cut_before_bootstrap_baseline",
            |value| {
                value["cases"][0]["checkpoint_evidence"][1]["sealed_source_cut"][2]
                    ["exclusive_end"] = Value::from(8);
            },
        );

        let outcome = mutated_outcome(|value| {
            value["ledger"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "event_id": "pre-start-gamma",
                    "topic": "fixture-input",
                    "partition": 2,
                    "offset": 8,
                    "logical_key": "gamma",
                    "value": 9
                }));
        });
        assert_eq!(outcome.classification, Classification::ModelMatch);

        assert_diagnostic(Classification::ProductFail, "extra_output", |value| {
            value["ledger"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "event_id": "pre-start-gamma",
                    "topic": "fixture-input",
                    "partition": 2,
                    "offset": 8,
                    "logical_key": "gamma",
                    "value": 9
                }));
            value["cases"][0]["frozen_sink_cut"][1]["exclusive_end"] = Value::from(5);
            value["cases"][0]["consumed_sink_cut"][1]["exclusive_end"] = Value::from(5);
            value["cases"][0]["data_records"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "broker": {"topic": "fixture-output", "partition": 1, "offset": 4},
                    "envelope_version": 1,
                    "envelope_kind": "data",
                    "operation_id": "fixture/gamma/count/1",
                    "writer_interval": "writer-new",
                    "admission_sequence": 4,
                    "raw_payload": "{\"logical_key\":\"gamma\",\"count\":1,\"sum\":9}"
                }));
        });
    }

    #[test]
    fn assignment_authority_is_complete_resolved_and_monotonic() {
        assert_diagnostic(
            Classification::RunInvalid,
            "assignment_evidence_missing",
            |value| {
                value["cases"][0]["assignment_authority_evidence"]
                    .as_array_mut()
                    .unwrap()
                    .remove(0);
            },
        );
        assert_diagnostic(
            Classification::RunInvalid,
            "assignment_evidence_incomplete",
            |value| {
                value["cases"][0]["assignment_authority_evidence"][1]["owners"][0]
                    ["process_term"] = Value::String(String::new());
            },
        );
        assert_diagnostic(
            Classification::RunInvalid,
            "assignment_evidence_incomplete",
            |value| {
                value["cases"][0]["assignment_authority_evidence"][1]["version"] = Value::from(0);
                for marker in [2, 3] {
                    value["cases"][0]["interval_markers"][marker]["current_assignment"]
                        ["version"] = Value::from(0);
                }
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "checkpoint_base_assignment_mismatch",
            |value| {
                value["cases"][0]["checkpoint_evidence"][1]["base_assignment"]["digest"] =
                    Value::String("sha256:wrong-base".to_owned());
                for marker in [2, 3] {
                    value["cases"][0]["interval_markers"][marker]["recovery"]["base_assignment"]
                        ["digest"] = Value::String("sha256:wrong-base".to_owned());
                }
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "marker_assignment_version_regressed",
            |value| {
                for marker in [0, 1] {
                    value["cases"][0]["interval_markers"][marker]["current_assignment"] =
                        serde_json::json!({"version": 8, "digest": "sha256:assignment-8"});
                    value["cases"][0]["interval_markers"][marker]["writer"] = serde_json::json!({
                        "node_id": "node-b",
                        "boot_incarnation": "boot-b",
                        "process_term": "term-b-2"
                    });
                }
                for marker in [2, 3] {
                    value["cases"][0]["interval_markers"][marker]["current_assignment"] =
                        serde_json::json!({"version": 7, "digest": "sha256:assignment-7"});
                    value["cases"][0]["interval_markers"][marker]["writer"] = serde_json::json!({
                        "node_id": "node-a",
                        "boot_incarnation": "boot-a",
                        "process_term": "term-a-1"
                    });
                }
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "marker_assignment_precedes_recovery_base",
            |value| {
                value["cases"][0]["checkpoint_evidence"][1]["base_assignment"] =
                    serde_json::json!({"version": 8, "digest": "sha256:assignment-8"});
                for marker in [2, 3] {
                    value["cases"][0]["interval_markers"][marker]["recovery"]["base_assignment"] =
                        serde_json::json!({"version": 8, "digest": "sha256:assignment-8"});
                    value["cases"][0]["interval_markers"][marker]["current_assignment"] =
                        serde_json::json!({"version": 7, "digest": "sha256:assignment-7"});
                    value["cases"][0]["interval_markers"][marker]["writer"] = serde_json::json!({
                        "node_id": "node-a",
                        "boot_incarnation": "boot-a",
                        "process_term": "term-a-1"
                    });
                }
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "marker_authority_incomplete",
            |value| {
                for marker in [2, 3] {
                    value["cases"][0]["interval_markers"][marker]["interval_id"] =
                        Value::String(String::new());
                }
                for record in [3, 4, 5] {
                    value["cases"][0]["data_records"][record]["writer_interval"] =
                        Value::String(String::new());
                }
            },
        );
    }

    #[test]
    fn vnode_routing_is_derived_independently_across_shards() {
        let outcome = mutated_outcome(configure_two_shards);
        assert_eq!(
            outcome.classification,
            Classification::ModelMatch,
            "{outcome:?}"
        );

        assert_diagnostic(
            Classification::ProductFail,
            "data_vnode_shard_mismatch",
            |value| {
                configure_two_shards(value);
                value["cases"][0]["frozen_sink_cut"][1]["exclusive_end"] = Value::from(5);
                value["cases"][0]["consumed_sink_cut"][1]["exclusive_end"] = Value::from(5);
                value["cases"][0]["data_records"][4]["broker"]["partition"] = Value::from(1);
                value["cases"][0]["data_records"][4]["admission_sequence"] = Value::from(2);
                value["cases"][0]["data_records"][5]["admission_sequence"] = Value::from(0);
            },
        );
    }

    #[test]
    fn missing_or_incomplete_observation_evidence_invalidates_the_run() {
        assert_diagnostic(
            Classification::RunInvalid,
            "checkpoint_evidence_missing",
            |value| {
                value["cases"][0]["checkpoint_evidence"]
                    .as_array_mut()
                    .unwrap()
                    .pop();
            },
        );
        assert_diagnostic(
            Classification::RunInvalid,
            "assignment_evidence_missing",
            |value| {
                value["cases"][0]["assignment_authority_evidence"]
                    .as_array_mut()
                    .unwrap()
                    .pop();
            },
        );
        assert_diagnostic(
            Classification::RunInvalid,
            "bootstrap_evidence_missing",
            |value| value["cases"][0]["bootstrap_observation"] = Value::Null,
        );
        assert_diagnostic(
            Classification::RunInvalid,
            "bootstrap_source_baseline_incomplete",
            |value| {
                value["cases"][0]["bootstrap_observation"]["observed_source_baseline"]
                    .as_array_mut()
                    .unwrap()
                    .pop();
            },
        );
        assert_diagnostic(
            Classification::RunInvalid,
            "source_cut_incomplete",
            |value| {
                value["cases"][0]["durable_source_cut"]
                    .as_array_mut()
                    .unwrap()
                    .pop();
            },
        );
        assert_diagnostic(Classification::RunInvalid, "sink_cut_incomplete", |value| {
            value["cases"][0]["consumed_sink_cut"]
                .as_array_mut()
                .unwrap()
                .pop();
        });
        assert_diagnostic(Classification::RunInvalid, "sink_cut_incomplete", |value| {
            value["cases"][0]["frozen_sink_cut"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "topic": "fixture-output",
                    "partition": 2,
                    "exclusive_end": 0
                }));
        });
        assert_diagnostic(
            Classification::RunInvalid,
            "assignment_evidence_incomplete",
            |value| {
                value["cases"][0]["assignment_authority_evidence"][1]["owners"]
                    .as_array_mut()
                    .unwrap()
                    .pop();
            },
        );
    }

    #[test]
    fn invalid_and_product_failure_boundaries_have_exact_diagnostics() {
        assert_exact_diagnostics(
            Classification::RunInvalid,
            &["bootstrap_evidence_missing"],
            |value| value["cases"][0]["bootstrap_observation"] = Value::Null,
        );
        assert_exact_diagnostics(
            Classification::RunInvalid,
            &["checkpoint_run_identity_mismatch"],
            |value| {
                value["cases"][0]["checkpoint_evidence"][1]["run"]["pipeline_identity"] =
                    Value::String("wrong-pipeline".to_owned());
            },
        );
        assert_exact_diagnostics(
            Classification::RunInvalid,
            &["assignment_run_identity_mismatch"],
            |value| {
                value["cases"][0]["assignment_authority_evidence"][1]["run"]
                    ["pipeline_incarnation"] = Value::String("wrong-incarnation".to_owned());
            },
        );
        assert_exact_diagnostics(
            Classification::ProductFail,
            &["recovery_checkpoint_not_committed"],
            |value| {
                value["cases"][0]["checkpoint_evidence"][1]["terminal"] =
                    Value::String("Abort".to_owned());
            },
        );
        assert_exact_diagnostics(
            Classification::ProductFail,
            &["marker_run_identity_mismatch"],
            |value| {
                for marker in [0, 1] {
                    value["cases"][0]["interval_markers"][marker]["provenance"]["operator_id"] =
                        Value::String("wrong-operator".to_owned());
                }
            },
        );
        assert_exact_diagnostics(
            Classification::ProductFail,
            &["operation_precedes_recovery_cut"],
            |value| {
                value["cases"][0]["checkpoint_evidence"][1]["sealed_source_cut"][0]
                    ["exclusive_end"] = Value::from(2);
            },
        );
    }

    #[test]
    fn existing_checkpoint_and_bootstrap_contradictions_are_product_failures() {
        assert_diagnostic(
            Classification::ProductFail,
            "checkpoint_evidence_not_immutable",
            |value| {
                value["cases"][0]["checkpoint_evidence"][1]["immutable"] = Value::Bool(false);
            },
        );
        assert_diagnostic(
            Classification::RunInvalid,
            "checkpoint_run_identity_mismatch",
            |value| {
                value["cases"][0]["checkpoint_evidence"][1]["run"]["pipeline_identity"] =
                    Value::String("wrong-pipeline".to_owned());
            },
        );
        assert_diagnostic(
            Classification::RunInvalid,
            "checkpoint_evidence_incomplete",
            |value| {
                value["cases"][0]["checkpoint_evidence"][1]["capsule_digest"] =
                    Value::String(String::new());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "recovery_checkpoint_not_committed",
            |value| {
                value["cases"][0]["checkpoint_evidence"][1]["terminal"] =
                    Value::String("Abort".to_owned());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "checkpoint_purpose_mismatch",
            |value| {
                value["cases"][0]["checkpoint_evidence"][0]["purpose"] =
                    Value::String("recovery_base".to_owned());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "bootstrap_source_baseline_mismatch",
            |value| {
                value["cases"][0]["bootstrap_observation"]["observed_source_baseline"][0]
                    ["exclusive_end"] = Value::from(1);
            },
        );
        for field in [
            "source_deliveries",
            "state_items",
            "timer_items",
            "outputs_computed",
            "outputs_queued",
            "sink_records_admitted",
            "sink_records_flushed",
        ] {
            assert_diagnostic(
                Classification::ProductFail,
                "bootstrap_not_empty",
                |value| {
                    value["cases"][0]["bootstrap_observation"][field] = Value::from(1);
                },
            );
        }
        assert_diagnostic(
            Classification::ProductFail,
            "bootstrap_order_invalid",
            |value| {
                value["cases"][0]["bootstrap_observation"]["checkpoint_commit_order"] =
                    Value::from(50);
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "recovery_cut_after_frozen_source",
            |value| {
                value["cases"][0]["checkpoint_evidence"][1]["sealed_source_cut"][0]
                    ["exclusive_end"] = Value::from(4);
            },
        );
    }

    #[test]
    fn marker_provenance_authority_topology_and_abi_are_checked() {
        assert_diagnostic(
            Classification::RunInvalid,
            "assignment_run_identity_mismatch",
            |value| {
                value["cases"][0]["assignment_authority_evidence"][1]["run"]
                    ["pipeline_incarnation"] = Value::String("wrong-incarnation".to_owned());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "marker_run_identity_mismatch",
            |value| {
                value["cases"][0]["interval_markers"][0]["provenance"]["operator_id"] =
                    Value::String("wrong-operator".to_owned());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "marker_recovery_mismatch",
            |value| {
                value["cases"][0]["interval_markers"][2]["recovery"]["capsule_digest"] =
                    Value::String("sha256:wrong".to_owned());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "marker_assignment_mismatch",
            |value| {
                value["cases"][0]["interval_markers"][2]["current_assignment"]["digest"] =
                    Value::String("sha256:wrong".to_owned());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "marker_writer_not_current_owner",
            |value| {
                value["cases"][0]["interval_markers"][2]["writer"]["node_id"] =
                    Value::String("node-a".to_owned());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "marker_shard_mismatch",
            |value| {
                value["cases"][0]["interval_markers"][0]["shard_id"] =
                    Value::String("wrong-shard".to_owned());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "marker_vnode_mismatch",
            |value| {
                value["cases"][0]["interval_markers"][0]["vnodes"] = serde_json::json!([0, 1, 3]);
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "marker_abi_mismatch",
            |value| {
                value["cases"][0]["interval_markers"][0]["abi"]["topology_digest"] =
                    Value::String("sha256:wrong".to_owned());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "envelope_kind_mismatch",
            |value| {
                value["cases"][0]["data_records"][0]["envelope_kind"] =
                    Value::String("interval_marker".to_owned());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "envelope_kind_mismatch",
            |value| {
                value["cases"][0]["interval_markers"][0]["envelope_kind"] =
                    Value::String("data".to_owned());
            },
        );
        for pointer in [
            "/cases/0/interval_markers/0/envelope_version",
            "/cases/0/data_records/0/envelope_version",
        ] {
            assert_diagnostic(
                Classification::ProductFail,
                "unsupported_envelope_version",
                |value| *value.pointer_mut(pointer).unwrap() = Value::from(2),
            );
        }
    }

    #[test]
    fn marker_chains_coverage_and_predecessor_fence_are_checked() {
        assert_diagnostic(
            Classification::ProductFail,
            "bootstrap_first_marker_missing",
            |value| {
                value["cases"][0]["interval_markers"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|marker| marker["broker"]["partition"] != 0);
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "bootstrap_first_marker_mismatch",
            |value| {
                for marker in [0, 1] {
                    value["cases"][0]["interval_markers"][marker]["predecessor_interval"] =
                        Value::String("ghost".to_owned());
                }
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "interval_marker_coverage_incomplete",
            |value| {
                value["cases"][0]["interval_markers"]
                    .as_array_mut()
                    .unwrap()
                    .remove(3);
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "bootstrap_first_marker_checkpoint_mismatch",
            |value| {
                let marker = &mut value["cases"][0]["interval_markers"][0]["recovery"];
                marker["checkpoint_id"] = Value::String("checkpoint-45".to_owned());
                marker["checkpoint_epoch"] = Value::from(45);
                marker["capsule_digest"] = Value::String("sha256:capsule-45".to_owned());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "marker_chain_invalid",
            |value| {
                value["cases"][0]["interval_markers"][2]["predecessor_interval"] = Value::Null;
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "marker_predecessor_invalid",
            |value| {
                for marker in [2, 3] {
                    value["cases"][0]["interval_markers"][marker]["predecessor_interval"] =
                        Value::String("writer-new".to_owned());
                }
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "marker_chain_inconsistent",
            |value| {
                value["cases"][0]["interval_markers"][3]["interval_id"] =
                    Value::String("writer-other".to_owned());
                value["cases"][0]["data_records"][3]["writer_interval"] =
                    Value::String("writer-other".to_owned());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "missing_interval_marker",
            |value| {
                value["cases"][0]["data_records"][3]["writer_interval"] =
                    Value::String("writer-missing".to_owned());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "marker_not_before_data",
            |value| {
                value["cases"][0]["interval_markers"][2]["broker"]["offset"] = Value::from(4);
                value["cases"][0]["data_records"][4]["broker"]["offset"] = Value::from(3);
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "predecessor_data_after_successor_marker",
            |value| {
                value["cases"][0]["frozen_sink_cut"][0]["exclusive_end"] = Value::from(7);
                value["cases"][0]["consumed_sink_cut"][0]["exclusive_end"] = Value::from(7);
                value["cases"][0]["data_records"][0]["broker"]["offset"] = Value::from(6);
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "output_beyond_frozen_sink_cut",
            |value| {
                value["cases"][0]["data_records"][5]["broker"]["offset"] = Value::from(6);
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "data_authority_incomplete",
            |value| {
                value["cases"][0]["data_records"][4]["writer_interval"] =
                    Value::String(String::new());
            },
        );
    }

    #[test]
    fn admission_sequences_are_global_per_interval_and_monotonic_per_partition() {
        assert_diagnostic(
            Classification::ProductFail,
            "admission_sequence_does_not_start_at_zero",
            |value| {
                value["cases"][0]["data_records"][0]["admission_sequence"] = Value::from(2);
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "admission_sequence_not_unique",
            |value| {
                value["cases"][0]["data_records"][1]["admission_sequence"] = Value::from(0);
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "admission_sequence_not_monotonic",
            |value| {
                value["cases"][0]["data_records"][5]["admission_sequence"] = Value::from(0);
            },
        );
    }

    #[test]
    fn replay_causality_raw_bytes_and_final_aggregate_are_checked() {
        assert_diagnostic(
            Classification::ProductFail,
            "operation_precedes_recovery_cut",
            |value| {
                value["cases"][0]["checkpoint_evidence"][1]["sealed_source_cut"][0]
                    ["exclusive_end"] = Value::from(2);
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "raw_payload_mismatch",
            |value| {
                value["cases"][0]["data_records"][2]["raw_payload"] =
                    Value::String("{ \"logical_key\":\"alpha\",\"count\":2,\"sum\":7}".to_owned());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "conflicting_operation_identity",
            |value| {
                value["cases"][0]["data_records"][2]["raw_payload"] =
                    Value::String("{\"logical_key\":\"alpha\",\"count\":1,\"sum\":11}".to_owned());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "group_version_not_increasing",
            |value| {
                value["cases"][0]["data_records"][5]["operation_id"] =
                    Value::String("fixture/alpha/count/2".to_owned());
                value["cases"][0]["data_records"][5]["raw_payload"] =
                    Value::String("{\"logical_key\":\"alpha\",\"count\":2,\"sum\":7}".to_owned());
            },
        );
        assert_diagnostic(
            Classification::ProductFail,
            "missing_final_state",
            |value| {
                let records = value["cases"][0]["data_records"].as_array_mut().unwrap();
                records.retain(|record| record["operation_id"] != "fixture/alpha/count/3");
            },
        );
    }

    #[test]
    fn malformed_v2_inputs_fail_closed_without_panicking() {
        assert!(
            mutated_verify_error(|value| value["vnode_count"] = Value::from(0))
                .contains("vnode_count")
        );
        assert!(mutated_verify_error(|value| {
            value["ledger"][0]["offset"] = Value::from(i64::MAX);
        })
        .contains("invalid partition or offset"));
        assert!(mutated_verify_error(|value| {
            value["cases"][0]["data_records"][0]["broker"] =
                value["cases"][0]["interval_markers"][0]["broker"].clone();
        })
        .contains("duplicate broker position"));
        assert!(mutated_verify_error(|value| {
            let duplicate = value["cases"][0]["durable_source_cut"][0].clone();
            value["cases"][0]["durable_source_cut"]
                .as_array_mut()
                .unwrap()
                .push(duplicate);
        })
        .contains("duplicate partition cut"));
        assert!(mutated_verify_error(|value| {
            let duplicate = value["cases"][0]["checkpoint_evidence"][0].clone();
            value["cases"][0]["checkpoint_evidence"]
                .as_array_mut()
                .unwrap()
                .push(duplicate);
        })
        .contains("duplicate checkpoint identity"));
        assert!(mutated_verify_error(|value| {
            value["cases"][0]["expected"]["diagnostics"] = serde_json::json!(["x", "x"]);
        })
        .contains("expected diagnostics contain duplicates"));
        assert!(mutated_verify_error(|value| {
            value["unexpected"] = Value::Bool(true);
        })
        .contains("unknown field"));
    }

    #[test]
    fn v2_remains_explicitly_synthetic_and_ineligible() {
        let mut value: Value = serde_json::from_slice(FIXTURE).unwrap();
        value["certification_eligible"] = Value::Bool(true);
        assert!(verify(&serde_json::to_vec(&value).unwrap())
            .unwrap_err()
            .to_string()
            .contains("certification_eligible"));

        let fixture = std::str::from_utf8(FIXTURE).unwrap();
        assert!(fixture.contains("fixture_only_logical_group_and_count_version"));
        assert!(fixture.contains(NOTICE));
    }
}
