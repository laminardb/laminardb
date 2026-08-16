//! Canonical fixture wire-identity decoding and cross-map validation.

use super::*;

pub(super) fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn decode_fixed_hex<const N: usize>(hex: &str) -> Result<[u8; N], String> {
    if hex.len() != N * 2 {
        return Err(format!(
            "must contain exactly {} lowercase hexadecimal characters",
            N * 2
        ));
    }
    let mut decoded = [0_u8; N];
    for (index, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0])
            .ok_or_else(|| "must contain only lowercase hexadecimal characters".to_owned())?;
        let low = hex_nibble(pair[1])
            .ok_or_else(|| "must contain only lowercase hexadecimal characters".to_owned())?;
        decoded[index] = (high << 4) | low;
    }
    if decoded.iter().all(|byte| *byte == 0) {
        return Err("must not encode the all-zero value".to_owned());
    }
    Ok(decoded)
}

fn decode_variable_hex(hex: &str) -> Result<Vec<u8>, String> {
    if !hex.len().is_multiple_of(2) {
        return Err("must contain an even number of lowercase hexadecimal characters".to_owned());
    }
    let byte_len = hex.len() / 2;
    u32::try_from(byte_len)
        .map_err(|_| "exceeds the operation identity u32 key-length field".to_owned())?;
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(byte_len)
        .map_err(|_| "could not allocate canonical group-key bytes".to_owned())?;
    for pair in hex.as_bytes().chunks_exact(2) {
        let high = hex_nibble(pair[0])
            .ok_or_else(|| "must contain only lowercase hexadecimal characters".to_owned())?;
        let low = hex_nibble(pair[1])
            .ok_or_else(|| "must contain only lowercase hexadecimal characters".to_owned())?;
        decoded.push((high << 4) | low);
    }
    Ok(decoded)
}

pub(super) fn validate_operation_group_keys(
    fixture: &Fixture,
) -> Result<BTreeMap<String, Vec<u8>>, CheckErrors> {
    let mut errors = Vec::new();
    let mut keys = BTreeMap::new();
    let mut labels_by_bytes = BTreeMap::new();
    for entry in &fixture.operation_group_keys {
        if entry.logical_key.is_empty() {
            errors.push("fixture.operation_group_keys contains an empty logical key".to_owned());
            continue;
        }
        let decoded = match decode_variable_hex(&entry.canonical_hex) {
            Ok(decoded) => decoded,
            Err(error) => {
                errors.push(format!(
                    "fixture.operation_group_keys logical key {:?} {error}",
                    entry.logical_key
                ));
                continue;
            }
        };
        if let Some(previous) = labels_by_bytes.insert(decoded.clone(), entry.logical_key.clone()) {
            errors.push(format!(
                "fixture.operation_group_keys logical keys {previous:?} and {:?} map to the same bytes",
                entry.logical_key
            ));
        }
        if keys.insert(entry.logical_key.clone(), decoded).is_some() {
            errors.push(format!(
                "fixture.operation_group_keys contains duplicate logical key {:?}",
                entry.logical_key
            ));
        }
    }

    let required = fixture
        .ledger
        .iter()
        .map(|record| record.logical_key.as_str())
        .collect::<BTreeSet<_>>();
    let actual = keys.keys().map(String::as_str).collect::<BTreeSet<_>>();
    for missing in required.difference(&actual) {
        errors.push(format!(
            "fixture.operation_group_keys is missing logical key {missing:?}"
        ));
    }
    for unused in actual.difference(&required) {
        errors.push(format!(
            "fixture.operation_group_keys contains unused logical key {unused:?}"
        ));
    }
    if errors.is_empty() {
        Ok(keys)
    } else {
        Err(CheckErrors::many(errors))
    }
}

pub(super) fn operation_id_context(
    fixture: &Fixture,
    wire_ids: &WireIdLookup,
) -> Result<provenance_v1::GroupedCountSumOperationIdContextV1, CheckErrors> {
    let deployment_uuid = wire_ids
        .ids_16
        .get(&fixture.expected_run.deployment_id)
        .ok_or_else(|| CheckErrors::one("operation identity deployment UUID is unmapped"))?;
    let pipeline_incarnation_id = wire_ids
        .ids_16
        .get(&fixture.expected_run.pipeline_incarnation)
        .ok_or_else(|| CheckErrors::one("operation identity pipeline incarnation is unmapped"))?;
    let pipeline_identity_sha256 = wire_ids
        .ids_32
        .get(&fixture.expected_run.pipeline_identity)
        .ok_or_else(|| CheckErrors::one("operation identity pipeline digest is unmapped"))?;
    provenance_v1::GroupedCountSumOperationIdContextV1::new(
        deployment_uuid,
        pipeline_incarnation_id,
        pipeline_identity_sha256,
        &fixture.expected_run.sink_id,
        &fixture.expected_run.operator_id,
        &fixture.expected_run.output_id,
    )
    .map_err(|error| CheckErrors::one(format!("invalid operation identity context: {error}")))
}

fn insert_hex_mapping<const N: usize>(
    entry: &WireHexId,
    category: &str,
    all_labels: &mut BTreeSet<String>,
    by_label: &mut BTreeMap<String, [u8; N]>,
    by_value: &mut BTreeMap<[u8; N], String>,
    errors: &mut Vec<String>,
) {
    if entry.label.is_empty() {
        errors.push(format!(
            "fixture.wire_id_map.{category} contains an empty label"
        ));
        return;
    }
    if !all_labels.insert(entry.label.clone()) {
        errors.push(format!(
            "fixture.wire_id_map contains duplicate label {:?}",
            entry.label
        ));
        return;
    }
    let decoded = match decode_fixed_hex::<N>(&entry.hex) {
        Ok(decoded) => decoded,
        Err(error) => {
            errors.push(format!(
                "fixture.wire_id_map.{category} label {:?} {error}",
                entry.label
            ));
            return;
        }
    };
    if let Some(previous) = by_value.insert(decoded, entry.label.clone()) {
        errors.push(format!(
            "fixture.wire_id_map.{category} labels {previous:?} and {:?} map to the same value",
            entry.label
        ));
    }
    by_label.insert(entry.label.clone(), decoded);
}

pub(super) fn validate_wire_id_map(fixture: &Fixture) -> Result<WireIdLookup, CheckErrors> {
    let mut errors = Vec::new();
    if fixture.wire_id_map.ids_16.is_empty()
        || fixture.wire_id_map.ids_32.is_empty()
        || fixture.wire_id_map.u64_values.is_empty()
    {
        errors.push("fixture.wire_id_map categories must not be empty".to_owned());
    }

    let mut all_labels = BTreeSet::new();
    let mut ids_16 = BTreeMap::new();
    let mut labels_16 = BTreeMap::new();
    for entry in &fixture.wire_id_map.ids_16 {
        insert_hex_mapping(
            entry,
            "ids_16",
            &mut all_labels,
            &mut ids_16,
            &mut labels_16,
            &mut errors,
        );
    }
    let mut ids_32 = BTreeMap::new();
    let mut labels_32 = BTreeMap::new();
    for entry in &fixture.wire_id_map.ids_32 {
        insert_hex_mapping(
            entry,
            "ids_32",
            &mut all_labels,
            &mut ids_32,
            &mut labels_32,
            &mut errors,
        );
    }
    let mut u64_values = BTreeMap::new();
    let mut known_u64_values = BTreeSet::new();
    for entry in &fixture.wire_id_map.u64_values {
        if entry.label.is_empty() {
            errors.push("fixture.wire_id_map.u64_values contains an empty label".to_owned());
            continue;
        }
        if !all_labels.insert(entry.label.clone()) {
            errors.push(format!(
                "fixture.wire_id_map contains duplicate label {:?}",
                entry.label
            ));
            continue;
        }
        if entry.value == 0 {
            errors.push(format!(
                "fixture.wire_id_map.u64_values label {:?} must not map to zero",
                entry.label
            ));
        }
        known_u64_values.insert(entry.value);
        u64_values.insert(entry.label.clone(), entry.value);
    }

    let mut required_16 = BTreeSet::new();
    let mut required_32 = BTreeSet::new();
    let mut required_u64 = BTreeSet::new();
    for case in &fixture.cases {
        for marker in &case.interval_markers {
            required_16.insert(marker.interval_id.as_str());
            if let Some(predecessor) = &marker.predecessor_interval {
                required_16.insert(predecessor.as_str());
            }
            required_16.insert(marker.provenance.deployment_id.as_str());
            required_16.insert(marker.provenance.pipeline_incarnation.as_str());
            required_16.insert(marker.writer.boot_incarnation.as_str());

            required_32.insert(marker.provenance.pipeline_identity.as_str());
            required_32.insert(marker.current_assignment.digest.as_str());
            required_32.insert(marker.recovery.committed_index_digest.as_str());
            required_32.insert(marker.recovery.base_assignment.digest.as_str());
            required_32.insert(marker.abi.topology_digest.as_str());

            required_u64.insert(marker.writer.node_id.as_str());
            required_u64.insert(marker.writer.process_term.as_str());
            required_u64.insert(marker.recovery.checkpoint_id.as_str());
        }
        for record in &case.data_records {
            required_16.insert(record.writer_interval.as_str());
            required_32.insert(record.operation_id.as_str());
        }
    }

    validate_exact_mapping_keys("ids_16", ids_16.keys(), &required_16, &mut errors);
    validate_exact_mapping_keys("ids_32", ids_32.keys(), &required_32, &mut errors);
    validate_exact_mapping_keys("u64_values", u64_values.keys(), &required_u64, &mut errors);

    if errors.is_empty() {
        Ok(WireIdLookup {
            ids_16,
            labels_16,
            ids_32,
            labels_32,
            u64_values,
            known_u64_values,
        })
    } else {
        Err(CheckErrors::many(errors))
    }
}

fn validate_exact_mapping_keys<'a>(
    category: &str,
    actual: impl Iterator<Item = &'a String>,
    required: &BTreeSet<&str>,
    errors: &mut Vec<String>,
) {
    let actual = actual.map(String::as_str).collect::<BTreeSet<_>>();
    for missing in required.difference(&actual) {
        errors.push(format!(
            "fixture.wire_id_map.{category} is missing label {missing:?}"
        ));
    }
    for unused in actual.difference(required) {
        errors.push(format!(
            "fixture.wire_id_map.{category} contains unused label {unused:?}"
        ));
    }
}
