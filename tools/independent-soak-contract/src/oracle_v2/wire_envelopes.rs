//! Independent decoding and comparison of checkpoint and data wire envelopes.

use super::*;

pub(super) fn decode_wire_hex(hex: &str, maximum_bytes: usize) -> Result<Vec<u8>, ()> {
    if !hex.len().is_multiple_of(2) || hex.len() > maximum_bytes.saturating_mul(2) {
        return Err(());
    }
    let mut decoded = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().chunks_exact(2) {
        let high = hex_nibble(pair[0]).ok_or(())?;
        let low = hex_nibble(pair[1]).ok_or(())?;
        decoded.push((high << 4) | low);
    }
    Ok(decoded)
}

fn compare_wire_id_16(
    observed: &[u8; 16],
    expected_label: &str,
    wire_ids: &WireIdLookup,
    diagnostics: &mut BTreeSet<String>,
) {
    let expected = wire_ids
        .ids_16
        .get(expected_label)
        .expect("wire ID map coverage was validated");
    if observed == expected {
        return;
    }
    match wire_ids.labels_16.get(observed) {
        None => {
            diagnostics.insert("wire_envelope_unknown_id".to_owned());
        }
        Some(_) => {
            diagnostics.insert("wire_envelope_mismatch".to_owned());
        }
    }
}

fn compare_wire_id_32(
    observed: &[u8; 32],
    expected_label: &str,
    wire_ids: &WireIdLookup,
    diagnostics: &mut BTreeSet<String>,
) {
    let expected = wire_ids
        .ids_32
        .get(expected_label)
        .expect("wire ID map coverage was validated");
    if observed == expected {
        return;
    }
    match wire_ids.labels_32.get(observed) {
        None => {
            diagnostics.insert("wire_envelope_unknown_id".to_owned());
        }
        Some(_) => {
            diagnostics.insert("wire_envelope_mismatch".to_owned());
        }
    }
}

pub(super) fn compare_wire_u64(
    observed: u64,
    expected_label: &str,
    wire_ids: &WireIdLookup,
    diagnostics: &mut BTreeSet<String>,
) {
    let expected = wire_ids
        .u64_values
        .get(expected_label)
        .expect("wire ID map coverage was validated");
    if observed == *expected {
        return;
    }
    if wire_ids.known_u64_values.contains(&observed) {
        diagnostics.insert("wire_envelope_mismatch".to_owned());
    } else {
        diagnostics.insert("wire_envelope_unknown_id".to_owned());
    }
}

pub(super) fn vnode_bitmap(vnode_count: u16, vnodes: &[u16]) -> Option<Vec<u8>> {
    if vnode_count == 0 {
        return None;
    }
    let mut bitmap = vec![0_u8; usize::from(vnode_count).div_ceil(8)];
    for vnode in vnodes {
        if *vnode >= vnode_count {
            return None;
        }
        bitmap[usize::from(*vnode) / 8] |= 1 << (*vnode % 8);
    }
    Some(bitmap)
}

pub(super) fn evaluate_wire_envelopes(
    _fixture: &Fixture,
    case: &Case,
    wire_ids: &WireIdLookup,
    operation_group_keys: &BTreeMap<String, Vec<u8>>,
    operation_id_context: &provenance_v1::GroupedCountSumOperationIdContextV1,
) -> BTreeSet<String> {
    let mut diagnostics = BTreeSet::new();
    let mut common_marker_bytes = BTreeMap::<(String, String), (&IntervalMarker, &str)>::new();

    for marker in &case.interval_markers {
        let common_key = (marker.shard_id.clone(), marker.interval_id.clone());
        if let Some((previous, previous_hex)) =
            common_marker_bytes.insert(common_key, (marker, &marker.wire_envelope_hex))
        {
            if same_common_marker(previous, marker)
                && previous_hex != marker.wire_envelope_hex.as_str()
            {
                diagnostics.insert("wire_common_marker_bytes_mismatch".to_owned());
            }
        }

        let bytes =
            match decode_wire_hex(&marker.wire_envelope_hex, wire_v1::MAX_MARKER_ENCODED_LEN) {
                Ok(bytes) => bytes,
                Err(()) => {
                    diagnostics.insert("wire_envelope_malformed".to_owned());
                    continue;
                }
            };
        let decoded = match wire_v1::decode_marker(&bytes) {
            Ok(decoded) => decoded,
            Err(_) => {
                diagnostics.insert("wire_envelope_malformed".to_owned());
                continue;
            }
        };

        compare_wire_id_16(
            decoded.current_interval_id,
            &marker.interval_id,
            wire_ids,
            &mut diagnostics,
        );
        match (
            decoded.predecessor_interval_id,
            marker.predecessor_interval.as_deref(),
        ) {
            (Some(observed), Some(expected)) => {
                compare_wire_id_16(observed, expected, wire_ids, &mut diagnostics);
            }
            (None, None) => {}
            _ => {
                diagnostics.insert("wire_envelope_mismatch".to_owned());
            }
        }
        compare_wire_id_16(
            decoded.deployment_uuid,
            &marker.provenance.deployment_id,
            wire_ids,
            &mut diagnostics,
        );
        compare_wire_id_16(
            decoded.pipeline_incarnation_id,
            &marker.provenance.pipeline_incarnation,
            wire_ids,
            &mut diagnostics,
        );
        compare_wire_id_32(
            decoded.pipeline_identity_sha256,
            &marker.provenance.pipeline_identity,
            wire_ids,
            &mut diagnostics,
        );
        compare_wire_id_32(
            decoded.current_assignment_sha256,
            &marker.current_assignment.digest,
            wire_ids,
            &mut diagnostics,
        );
        compare_wire_u64(
            decoded.writer_node_id,
            &marker.writer.node_id,
            wire_ids,
            &mut diagnostics,
        );
        compare_wire_id_16(
            decoded.writer_boot_uuid,
            &marker.writer.boot_incarnation,
            wire_ids,
            &mut diagnostics,
        );
        compare_wire_u64(
            decoded.durable_process_term,
            &marker.writer.process_term,
            wire_ids,
            &mut diagnostics,
        );
        compare_wire_u64(
            decoded.recovery_checkpoint_id,
            &marker.recovery.checkpoint_id,
            wire_ids,
            &mut diagnostics,
        );
        compare_wire_id_32(
            decoded.committed_index_sha256,
            &marker.recovery.committed_index_digest,
            wire_ids,
            &mut diagnostics,
        );
        compare_wire_id_32(
            decoded.recovery_base_assignment_sha256,
            &marker.recovery.base_assignment.digest,
            wire_ids,
            &mut diagnostics,
        );
        compare_wire_id_32(
            decoded.topology_sha256,
            &marker.abi.topology_digest,
            wire_ids,
            &mut diagnostics,
        );

        let expected_bitmap = vnode_bitmap(marker.abi.vnode_count, &marker.vnodes);
        if decoded.pipeline_identity_version != PIPELINE_IDENTITY_VERSION
            || decoded.key_to_vnode_abi_version != WIRE_ABI_VERSION
            || decoded.sink_partitioning_abi_version != WIRE_ABI_VERSION
            || decoded.vnode_count != marker.abi.vnode_count
            || decoded.current_assignment_version != marker.current_assignment.version
            || decoded.recovery_epoch != marker.recovery.checkpoint_epoch
            || decoded.recovery_base_assignment_version != marker.recovery.base_assignment.version
            || decoded.sink_id != marker.provenance.sink_id
            || decoded.operator_id != marker.provenance.operator_id
            || decoded.output_id != marker.provenance.output_id
            || decoded.shard_id != marker.shard_id
            || expected_bitmap.as_deref() != Some(decoded.vnode_bitmap)
        {
            diagnostics.insert("wire_envelope_mismatch".to_owned());
        }
    }

    for record in &case.data_records {
        let bytes = match decode_wire_hex(&record.wire_envelope_hex, wire_v1::DATA_ENCODED_LEN) {
            Ok(bytes) => bytes,
            Err(()) => {
                diagnostics.insert("wire_envelope_malformed".to_owned());
                continue;
            }
        };
        let decoded = match wire_v1::decode_data(&bytes) {
            Ok(decoded) => decoded,
            Err(_) => {
                diagnostics.insert("wire_envelope_malformed".to_owned());
                continue;
            }
        };
        compare_wire_id_32(
            decoded.operation_id,
            &record.operation_id,
            wire_ids,
            &mut diagnostics,
        );
        if let Ok(payload) = serde_json::from_str::<AggregatePayload>(&record.raw_payload) {
            if let Some(canonical_group_key) = operation_group_keys.get(&payload.logical_key) {
                match operation_id_context.derive(canonical_group_key, payload.count) {
                    Ok(expected) if decoded.operation_id == &expected => {}
                    Ok(_) | Err(_) => {
                        diagnostics.insert("wire_operation_identity_mismatch".to_owned());
                    }
                }
            }
        }
        compare_wire_id_16(
            decoded.writer_interval_id,
            &record.writer_interval,
            wire_ids,
            &mut diagnostics,
        );
        if decoded.admission_sequence != record.admission_sequence {
            diagnostics.insert("wire_envelope_mismatch".to_owned());
        }
    }

    diagnostics
}
