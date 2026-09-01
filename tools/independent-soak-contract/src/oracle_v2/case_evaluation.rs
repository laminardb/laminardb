//! Per-case checkpoint, bootstrap, and recovery evaluation.

use super::*;

pub(super) fn evaluate_case(
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
            || checkpoint.committed_index_digest.is_empty()
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
