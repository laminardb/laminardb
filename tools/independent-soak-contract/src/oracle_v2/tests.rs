use serde_json::Value;

use super::*;

const FIXTURE: &[u8] = include_bytes!("../../fixtures/grouped-count-sum-alo-v2.json");

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
        value["cases"][0]["interval_markers"][index]["shard_id"] = Value::String(shard.to_owned());
        value["cases"][0]["interval_markers"][index]["vnodes"] = vnodes;
        value["cases"][0]["interval_markers"][index]["abi"]["topology_digest"] =
            Value::String("sha256:fixture-topology-two-shards".to_owned());
    }
    value["cases"][0]["data_records"][1]["admission_sequence"] = Value::from(0);
    value["cases"][0]["data_records"][3]["admission_sequence"] = Value::from(0);
}

fn encoded_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn encoded_data_record(record: &DataRecord, wire_ids: &WireIdLookup) -> String {
    let encoded = wire_v1::encode_data(&wire_v1::DataHeaderRef {
        operation_id: wire_ids.ids_32.get(&record.operation_id).unwrap(),
        writer_interval_id: wire_ids.ids_16.get(&record.writer_interval).unwrap(),
        admission_sequence: record.admission_sequence,
    })
    .unwrap();
    encoded_hex(&encoded)
}

fn encoded_interval_marker(marker: &IntervalMarker, wire_ids: &WireIdLookup) -> String {
    let bitmap = vnode_bitmap(marker.abi.vnode_count, &marker.vnodes).unwrap();
    let encoded_marker = wire_v1::MarkerRef {
        current_interval_id: wire_ids.ids_16.get(&marker.interval_id).unwrap(),
        predecessor_interval_id: marker
            .predecessor_interval
            .as_ref()
            .map(|label| wire_ids.ids_16.get(label).unwrap()),
        deployment_uuid: wire_ids
            .ids_16
            .get(&marker.provenance.deployment_id)
            .unwrap(),
        pipeline_incarnation_id: wire_ids
            .ids_16
            .get(&marker.provenance.pipeline_incarnation)
            .unwrap(),
        pipeline_identity_version: PIPELINE_IDENTITY_VERSION,
        pipeline_identity_sha256: wire_ids
            .ids_32
            .get(&marker.provenance.pipeline_identity)
            .unwrap(),
        key_to_vnode_abi_version: WIRE_ABI_VERSION,
        sink_partitioning_abi_version: WIRE_ABI_VERSION,
        vnode_count: marker.abi.vnode_count,
        current_assignment_version: marker.current_assignment.version,
        current_assignment_sha256: wire_ids
            .ids_32
            .get(&marker.current_assignment.digest)
            .unwrap(),
        writer_node_id: *wire_ids.u64_values.get(&marker.writer.node_id).unwrap(),
        writer_boot_uuid: wire_ids
            .ids_16
            .get(&marker.writer.boot_incarnation)
            .unwrap(),
        durable_process_term: *wire_ids
            .u64_values
            .get(&marker.writer.process_term)
            .unwrap(),
        recovery_epoch: marker.recovery.checkpoint_epoch,
        recovery_checkpoint_id: *wire_ids
            .u64_values
            .get(&marker.recovery.checkpoint_id)
            .unwrap(),
        committed_index_sha256: wire_ids
            .ids_32
            .get(&marker.recovery.committed_index_digest)
            .unwrap(),
        recovery_base_assignment_version: marker.recovery.base_assignment.version,
        recovery_base_assignment_sha256: wire_ids
            .ids_32
            .get(&marker.recovery.base_assignment.digest)
            .unwrap(),
        topology_sha256: wire_ids.ids_32.get(&marker.abi.topology_digest).unwrap(),
        sink_id: &marker.provenance.sink_id,
        operator_id: &marker.provenance.operator_id,
        output_id: &marker.provenance.output_id,
        shard_id: &marker.shard_id,
        vnode_bitmap: &bitmap,
    };
    let mut encoded = Vec::with_capacity(wire_v1::encoded_marker_len(&encoded_marker).unwrap());
    wire_v1::encode_marker_into(&encoded_marker, &mut encoded).unwrap();
    encoded_hex(&encoded)
}

fn mutated_wire_outcome(mutate: impl FnOnce(&mut Value)) -> Outcome {
    let mut value: Value = serde_json::from_slice(FIXTURE).unwrap();
    mutate(&mut value);
    let fixture: Fixture = serde_json::from_value(value).unwrap();
    let source_partitions = validate_source_topology(&fixture).unwrap();
    let topology = validate_topology(&fixture).unwrap();
    let wire_ids = validate_wire_id_map(&fixture).unwrap();
    let operation_group_keys = validate_operation_group_keys(&fixture).unwrap();
    let operation_id_context = operation_id_context(&fixture, &wire_ids).unwrap();
    let case = &fixture.cases[0];
    let mut outcome = evaluate_case(&fixture, &source_partitions, &topology, case).unwrap();
    if outcome.classification != Classification::RunInvalid {
        let diagnostics = evaluate_wire_envelopes(
            &fixture,
            case,
            &wire_ids,
            &operation_group_keys,
            &operation_id_context,
        );
        if !diagnostics.is_empty() {
            outcome.classification = Classification::ProductFail;
            outcome.diagnostics.extend(diagnostics);
        }
    }
    outcome
}

#[test]
fn canonical_wire_envelopes_match_the_test_encoder_and_common_markers() {
    let fixture: Fixture = serde_json::from_slice(FIXTURE).unwrap();
    let wire_ids = validate_wire_id_map(&fixture).unwrap();
    let operation_group_keys = validate_operation_group_keys(&fixture).unwrap();
    let operation_id_context = operation_id_context(&fixture, &wire_ids).unwrap();
    for marker in &fixture.cases[0].interval_markers {
        assert_eq!(
            marker.wire_envelope_hex,
            encoded_interval_marker(marker, &wire_ids)
        );
        let bytes =
            decode_wire_hex(&marker.wire_envelope_hex, wire_v1::MAX_MARKER_ENCODED_LEN).unwrap();
        let decoded = wire_v1::decode_marker(&bytes).unwrap();
        let mut reencoded = Vec::new();
        wire_v1::encode_marker_into(&decoded, &mut reencoded).unwrap();
        assert_eq!(reencoded, bytes);
    }
    for record in &fixture.cases[0].data_records {
        assert_eq!(
            record.wire_envelope_hex,
            encoded_data_record(record, &wire_ids)
        );
        let bytes = decode_wire_hex(&record.wire_envelope_hex, wire_v1::DATA_ENCODED_LEN).unwrap();
        let decoded = wire_v1::decode_data(&bytes).unwrap();
        assert_eq!(wire_v1::encode_data(&decoded).unwrap().as_slice(), bytes);
        let payload: AggregatePayload = serde_json::from_str(&record.raw_payload).unwrap();
        let derived = operation_id_context
            .derive(&operation_group_keys[&payload.logical_key], payload.count)
            .unwrap();
        assert_eq!(wire_ids.ids_32[&record.operation_id], derived);
        assert_eq!(decoded.operation_id, &derived);
    }
    let markers = &fixture.cases[0].interval_markers;
    assert_eq!(markers[0].wire_envelope_hex, markers[1].wire_envelope_hex);
    assert_eq!(markers[2].wire_envelope_hex, markers[3].wire_envelope_hex);
    assert!(evaluate_wire_envelopes(
        &fixture,
        &fixture.cases[0],
        &wire_ids,
        &operation_group_keys,
        &operation_id_context,
    )
    .is_empty());
}

#[test]
fn wire_map_rejects_missing_noncanonical_nonbijective_and_unused_entries() {
    for (expected, mutate) in [
        (
            "missing field `wire_id_map`",
            Box::new(|value: &mut Value| {
                value.as_object_mut().unwrap().remove("wire_id_map");
            }) as Box<dyn Fn(&mut Value)>,
        ),
        (
            "lowercase hexadecimal",
            Box::new(|value: &mut Value| {
                value["wire_id_map"]["ids_16"][0]["hex"] =
                    Value::String("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned());
            }),
        ),
        (
            "map to the same value",
            Box::new(|value: &mut Value| {
                value["wire_id_map"]["ids_16"][1]["hex"] =
                    value["wire_id_map"]["ids_16"][0]["hex"].clone();
            }),
        ),
        (
            "must not map to zero",
            Box::new(|value: &mut Value| {
                value["wire_id_map"]["u64_values"][0]["value"] = Value::from(0);
            }),
        ),
        (
            "contains unused label",
            Box::new(|value: &mut Value| {
                value["wire_id_map"]["ids_32"]
                    .as_array_mut()
                    .unwrap()
                    .push(serde_json::json!({
                        "label": "unused",
                        "hex": "3131313131313131313131313131313131313131313131313131313131313131"
                    }));
            }),
        ),
    ] {
        assert!(
            mutated_verify_error(mutate).contains(expected),
            "{expected}"
        );
    }
}

#[test]
fn operation_group_keys_are_explicit_canonical_and_bijective() {
    for (expected, mutate) in [
        (
            "missing field `operation_group_keys`",
            Box::new(|value: &mut Value| {
                value
                    .as_object_mut()
                    .unwrap()
                    .remove("operation_group_keys");
            }) as Box<dyn Fn(&mut Value)>,
        ),
        (
            "lowercase hexadecimal",
            Box::new(|value: &mut Value| {
                value["operation_group_keys"][0]["canonical_hex"] = Value::String("AA".to_owned());
            }),
        ),
        (
            "even number",
            Box::new(|value: &mut Value| {
                value["operation_group_keys"][0]["canonical_hex"] = Value::String("0".to_owned());
            }),
        ),
        (
            "duplicate logical key",
            Box::new(|value: &mut Value| {
                let duplicate = value["operation_group_keys"][0].clone();
                value["operation_group_keys"]
                    .as_array_mut()
                    .unwrap()
                    .push(duplicate);
            }),
        ),
        (
            "map to the same bytes",
            Box::new(|value: &mut Value| {
                value["operation_group_keys"][1]["canonical_hex"] =
                    value["operation_group_keys"][0]["canonical_hex"].clone();
            }),
        ),
        (
            "missing logical key \"beta\"",
            Box::new(|value: &mut Value| {
                value["operation_group_keys"].as_array_mut().unwrap().pop();
            }),
        ),
        (
            "unused logical key \"gamma\"",
            Box::new(|value: &mut Value| {
                value["operation_group_keys"]
                    .as_array_mut()
                    .unwrap()
                    .push(serde_json::json!({
                        "logical_key": "gamma",
                        "canonical_hex": "00"
                    }));
            }),
        ),
    ] {
        assert!(
            mutated_verify_error(mutate).contains(expected),
            "expected {expected:?}"
        );
    }
}

#[test]
fn wire_u64_ids_are_typed_and_equal_values_remain_legal() {
    let mut value: Value = serde_json::from_slice(FIXTURE).unwrap();
    for entry in value["wire_id_map"]["u64_values"].as_array_mut().unwrap() {
        if matches!(
            entry["label"].as_str().unwrap(),
            "node-a" | "term-a-1" | "checkpoint-44"
        ) {
            entry["value"] = Value::from(44);
        }
    }

    let mut fixture: Fixture = serde_json::from_value(value).unwrap();
    let wire_ids = validate_wire_id_map(&fixture).unwrap();
    let operation_group_keys = validate_operation_group_keys(&fixture).unwrap();
    let operation_id_context = operation_id_context(&fixture, &wire_ids).unwrap();
    let encoded_markers = fixture.cases[0]
        .interval_markers
        .iter()
        .map(|marker| encoded_interval_marker(marker, &wire_ids))
        .collect::<Vec<_>>();
    for (marker, encoded) in fixture.cases[0]
        .interval_markers
        .iter_mut()
        .zip(encoded_markers)
    {
        marker.wire_envelope_hex = encoded;
    }
    assert!(evaluate_wire_envelopes(
        &fixture,
        &fixture.cases[0],
        &wire_ids,
        &operation_group_keys,
        &operation_id_context,
    )
    .is_empty());

    let mut diagnostics = BTreeSet::new();
    compare_wire_u64(45, "node-a", &wire_ids, &mut diagnostics);
    assert_eq!(
        diagnostics,
        BTreeSet::from(["wire_envelope_mismatch".to_owned()])
    );
}

#[test]
fn malformed_noncanonical_and_oversized_wire_hex_are_product_failures() {
    for replacement in ["AA".to_owned(), "00".repeat(wire_v1::DATA_ENCODED_LEN + 1)] {
        let outcome = mutated_wire_outcome(|value| {
            value["cases"][0]["data_records"][0]["wire_envelope_hex"] = Value::String(replacement);
        });
        assert_eq!(outcome.classification, Classification::ProductFail);
        assert_eq!(
            outcome.diagnostics,
            BTreeSet::from(["wire_envelope_malformed".to_owned()])
        );
    }
}

#[test]
fn decoded_wire_mismatches_and_unknown_ids_fail_deterministically() {
    let data_mismatch = mutated_wire_outcome(|value| {
        value["cases"][0]["data_records"][0]["wire_envelope_hex"] =
            value["cases"][0]["data_records"][1]["wire_envelope_hex"].clone();
    });
    assert_eq!(data_mismatch.classification, Classification::ProductFail);
    assert_eq!(
        data_mismatch.diagnostics,
        BTreeSet::from([
            "wire_envelope_mismatch".to_owned(),
            "wire_operation_identity_mismatch".to_owned(),
        ])
    );

    let marker_mismatch = mutated_wire_outcome(|value| {
        value["cases"][0]["interval_markers"][0]["wire_envelope_hex"] =
            value["cases"][0]["interval_markers"][2]["wire_envelope_hex"].clone();
    });
    assert_eq!(marker_mismatch.classification, Classification::ProductFail);
    assert_eq!(
        marker_mismatch.diagnostics,
        BTreeSet::from([
            "wire_common_marker_bytes_mismatch".to_owned(),
            "wire_envelope_mismatch".to_owned(),
        ])
    );

    let unknown_id = mutated_wire_outcome(|value| {
        let mut hex = value["cases"][0]["data_records"][0]["wire_envelope_hex"]
            .as_str()
            .unwrap()
            .to_owned();
        hex.replace_range(20..22, "fe");
        value["cases"][0]["data_records"][0]["wire_envelope_hex"] = Value::String(hex);
    });
    assert_eq!(unknown_id.classification, Classification::ProductFail);
    assert_eq!(
        unknown_id.diagnostics,
        BTreeSet::from([
            "wire_envelope_unknown_id".to_owned(),
            "wire_operation_identity_mismatch".to_owned(),
        ])
    );
}

#[test]
fn operation_identity_is_derived_independently_of_fixture_labels() {
    let changed_map_and_wire = mutated_wire_outcome(|value| {
        let replacement = "aa".repeat(32);
        let entry = value["wire_id_map"]["ids_32"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["label"] == "fixture/alpha/count/1")
            .unwrap();
        entry["hex"] = Value::String(replacement.clone());
        let mut wire = value["cases"][0]["data_records"][0]["wire_envelope_hex"]
            .as_str()
            .unwrap()
            .to_owned();
        wire.replace_range(20..84, &replacement);
        value["cases"][0]["data_records"][0]["wire_envelope_hex"] = Value::String(wire);
    });
    assert_eq!(
        changed_map_and_wire.diagnostics,
        BTreeSet::from(["wire_operation_identity_mismatch".to_owned()])
    );

    let changed_canonical_key = mutated_wire_outcome(|value| {
        value["operation_group_keys"][0]["canonical_hex"] =
            Value::String("02616c70686100000004".to_owned());
    });
    assert_eq!(
        changed_canonical_key.diagnostics,
        BTreeSet::from(["wire_operation_identity_mismatch".to_owned()])
    );

    let changed_sum = mutated_wire_outcome(|value| {
        value["cases"][0]["data_records"][0]["raw_payload"] =
            Value::String("{\"logical_key\":\"alpha\",\"count\":1,\"sum\":11}".to_owned());
    });
    assert!(changed_sum.diagnostics.contains("raw_payload_mismatch"));
    assert!(changed_sum.diagnostics.contains("aggregate_divergence"));
    assert!(!changed_sum
        .diagnostics
        .contains("wire_operation_identity_mismatch"));

    let changed_count = mutated_wire_outcome(|value| {
        value["cases"][0]["data_records"][0]["raw_payload"] =
            Value::String("{\"logical_key\":\"alpha\",\"count\":2,\"sum\":10}".to_owned());
    });
    assert!(changed_count
        .diagnostics
        .contains("wire_operation_identity_mismatch"));
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
            value["cases"][0]["checkpoint_evidence"][1]["sealed_source_cut"][2]["exclusive_end"] =
                Value::from(8);
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
                "wire_envelope_hex": "",
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
            value["cases"][0]["assignment_authority_evidence"][1]["owners"][0]["process_term"] =
                Value::String(String::new());
        },
    );
    assert_diagnostic(
        Classification::RunInvalid,
        "assignment_evidence_incomplete",
        |value| {
            value["cases"][0]["assignment_authority_evidence"][1]["version"] = Value::from(0);
            for marker in [2, 3] {
                value["cases"][0]["interval_markers"][marker]["current_assignment"]["version"] =
                    Value::from(0);
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
            value["cases"][0]["assignment_authority_evidence"][1]["run"]["pipeline_incarnation"] =
                Value::String("wrong-incarnation".to_owned());
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
            value["cases"][0]["checkpoint_evidence"][1]["sealed_source_cut"][0]["exclusive_end"] =
                Value::from(2);
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
            value["cases"][0]["checkpoint_evidence"][1]["committed_index_digest"] =
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
            value["cases"][0]["bootstrap_observation"]["checkpoint_commit_order"] = Value::from(50);
        },
    );
    assert_diagnostic(
        Classification::ProductFail,
        "recovery_cut_after_frozen_source",
        |value| {
            value["cases"][0]["checkpoint_evidence"][1]["sealed_source_cut"][0]["exclusive_end"] =
                Value::from(4);
        },
    );
}

#[test]
fn marker_provenance_authority_topology_and_abi_are_checked() {
    assert_diagnostic(
        Classification::RunInvalid,
        "assignment_run_identity_mismatch",
        |value| {
            value["cases"][0]["assignment_authority_evidence"][1]["run"]["pipeline_incarnation"] =
                Value::String("wrong-incarnation".to_owned());
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
            value["cases"][0]["interval_markers"][2]["recovery"]["committed_index_digest"] =
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
            marker["committed_index_digest"] =
                Value::String("sha256:committed-index-45".to_owned());
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
            value["cases"][0]["data_records"][4]["writer_interval"] = Value::String(String::new());
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
            value["cases"][0]["checkpoint_evidence"][1]["sealed_source_cut"][0]["exclusive_end"] =
                Value::from(2);
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
        mutated_verify_error(|value| value["vnode_count"] = Value::from(0)).contains("vnode_count")
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
    assert!(fixture.contains("operation-id-v1-with-explicit-abi-v1-group-key-bytes"));
    assert!(fixture.contains(NOTICE));
}
