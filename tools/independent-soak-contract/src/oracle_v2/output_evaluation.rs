//! Output-marker, writer-authority, and broker-position evaluation.

use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn evaluate_output(
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
        if marker.recovery.committed_index_digest != checkpoint.evidence.committed_index_digest
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

pub(super) fn same_common_marker(left: &IntervalMarker, right: &IntervalMarker) -> bool {
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
