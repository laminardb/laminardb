use std::num::NonZeroU32;
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, NullArray};
use arrow::datatypes::{DataType, Field};
use proptest::prelude::*;

use super::*;
use crate::vnode_partial::v2 as partial_v2;

fn schema(data_type: DataType, nullable: bool) -> PartitionKeySchemaV1 {
    PartitionKeySchemaV1::try_new(&[Arc::new(Field::new("group", data_type, nullable))]).unwrap()
}

fn context(
    routing_schema: &PartitionKeySchemaV1,
    kind: ArtifactKind,
    attempt: u64,
    parent: Option<u64>,
) -> ArtifactContext<'_> {
    ArtifactContext {
        kind,
        attempt: CheckpointAttempt::canonical(attempt),
        parent: parent.map(|checkpoint| {
            ParentLink::new(CheckpointAttempt::canonical(checkpoint), [0x44; 32])
        }),
        assignment_version: 7,
        assignment_certificate_sha256: [0x11; 32],
        operator_identity_sha256: [0x22; 32],
        state_table_identity_sha256: [0x33; 32],
        vnode_count: NonZeroU32::new(1).unwrap(),
        vnode: 0,
        routing_schema,
        contract: AggregateContractV1::new(routing_schema, true),
    }
}

fn budget() -> AggregateObjectBudget {
    AggregateObjectBudget {
        envelope_metadata_bytes_max: 4096,
        routing_schema_bytes_max: 1024,
        state_contract_bytes_max: 1024,
        encoded_key_bytes_max: 256,
        stored_state_bytes_max: 256,
        remaining_artifact_bytes: 1 << 20,
        remaining_rows: 1024,
        remaining_key_bytes: 1 << 16,
        remaining_state_bytes: 1 << 16,
    }
}

fn encoded_keys(
    routing_schema: &PartitionKeySchemaV1,
    nullability: &[bool],
    columns: &[ArrayRef],
) -> Vec<Vec<u8>> {
    assert_eq!(columns.len(), nullability.len());
    let fields = columns
        .iter()
        .zip(nullability)
        .enumerate()
        .map(|(index, (column, nullable))| {
            Arc::new(Field::new(
                format!("group_{index}"),
                column.data_type().clone(),
                *nullable,
            ))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        PartitionKeySchemaV1::try_new(&fields).unwrap(),
        *routing_schema
    );
    let codec =
        PartitionKeyCodecV1::try_new(columns.iter().map(|column| column.data_type().clone()))
            .unwrap();
    codec
        .encode_columns(columns)
        .unwrap()
        .iter()
        .map(|row| row.as_ref().to_vec())
        .collect()
}

fn row(key: &[u8], count: u64, non_null: u64, sum: i64) -> AggregateRow<'_> {
    AggregateRow {
        key,
        state: CountSumStateV1::persisted(count, non_null, sum).unwrap(),
    }
}

fn put_test(bytes: &mut [u8], offset: usize, value: &[u8]) {
    bytes[offset..offset + value.len()].copy_from_slice(value);
}

fn rows_offset(bytes: &[u8]) -> usize {
    usize::try_from(u64::from_be_bytes(field(bytes, 144).unwrap())).unwrap()
}

fn refresh_rows_digest(bytes: &mut [u8]) {
    let offset = rows_offset(bytes);
    let digest = sha256(&bytes[offset..]);
    put_test(bytes, 320, &digest);
}

fn fixture_bytes(text: &str) -> Vec<u8> {
    let compact = text
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    let mut chunks = compact.chunks_exact(2);
    let bytes = chunks
        .by_ref()
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16)
                .expect("fixture contains only hexadecimal bytes")
        })
        .collect();
    assert!(chunks.remainder().is_empty(), "fixture has an odd nibble");
    bytes
}

fn partial_limits() -> partial_v2::VnodePartialV2Limits {
    partial_v2::VnodePartialV2Limits {
        encoded_artifact_bytes_max: 1 << 20,
        envelope_metadata_bytes_max: 4096,
        directory_entries_per_artifact_max: 64,
    }
}

#[test]
fn frozen_aggregate_goldens_decode_and_encoder_reproduces_them() {
    let int64 = schema(DataType::Int64, false);
    let empty_context = context(&int64, ArtifactKind::Empty, 1, None);
    let empty_fixture = fixture_bytes(include_str!(
        "../../../tests/fixtures/managed_state_v1/aggregate_empty.hex"
    ));
    let empty_encoded = encode(empty_context, &[], &mut budget()).unwrap();
    assert_eq!(empty_encoded, empty_fixture);
    assert_eq!(
        decode(&empty_fixture, empty_context, &mut budget())
            .unwrap()
            .row_count(),
        0
    );

    let null = schema(DataType::Null, true);
    let null_context = context(&null, ArtifactKind::Full, 1, None);
    let null_keys = encoded_keys(&null, &[true], &[Arc::new(NullArray::new(1))]);
    assert_eq!(null_keys, [Vec::<u8>::new()]);
    let null_rows = [row(&null_keys[0], 3, 0, 0)];
    let null_fixture = fixture_bytes(include_str!(
        "../../../tests/fixtures/managed_state_v1/aggregate_full_null_sum.hex"
    ));
    let null_encoded = encode(null_context, &null_rows, &mut budget()).unwrap();
    assert_eq!(null_encoded, null_fixture);
    assert_eq!(
        decode(&null_fixture, null_context, &mut budget())
            .unwrap()
            .rows()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        null_rows
    );

    let binary = schema(DataType::Binary, false);
    let full_context = context(&binary, ArtifactKind::Full, 1, None);
    let full_keys = encoded_keys(
        &binary,
        &[false],
        &[Arc::new(BinaryArray::from(vec![
            Some(b"" as &[u8]),
            Some(b"\x00\xff" as &[u8]),
        ]))],
    );
    assert_eq!(
        full_keys,
        [vec![0x01], vec![0x02, 0x00, 0xff, 0, 0, 0, 0, 0, 0, 0x02]]
    );
    let full_rows = [
        row(&full_keys[0], 1, 1, i64::MIN),
        row(&full_keys[1], MAX_SQL_COUNT, 1, i64::MAX),
    ];
    let full_fixture = fixture_bytes(include_str!(
        "../../../tests/fixtures/managed_state_v1/aggregate_full_two_rows.hex"
    ));
    let full_encoded = encode(full_context, &full_rows, &mut budget()).unwrap();
    assert_eq!(full_encoded, full_fixture);
    assert_eq!(
        decode(&full_fixture, full_context, &mut budget())
            .unwrap()
            .rows()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        full_rows
    );

    let delta_context = context(&binary, ArtifactKind::Delta, 2, Some(1));
    let delta_keys = encoded_keys(
        &binary,
        &[false],
        &[Arc::new(BinaryArray::from(vec![Some(b"\x7f" as &[u8])]))],
    );
    assert_eq!(delta_keys, [vec![0x02, 0x7f, 0, 0, 0, 0, 0, 0, 0, 0x01]]);
    let delta_rows = [row(&delta_keys[0], 2, 1, 9)];
    let delta_fixture = fixture_bytes(include_str!(
        "../../../tests/fixtures/managed_state_v1/aggregate_delta_one_row.hex"
    ));
    let delta_encoded = encode(delta_context, &delta_rows, &mut budget()).unwrap();
    assert_eq!(delta_encoded, delta_fixture);
    assert_eq!(
        decode(&delta_fixture, delta_context, &mut budget())
            .unwrap()
            .rows()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        delta_rows
    );
}

#[test]
fn frozen_composed_full_and_delta_chain_uses_real_inner_envelopes() {
    let routing = schema(DataType::Binary, false);
    let keys = encoded_keys(
        &routing,
        &[false],
        &[Arc::new(BinaryArray::from(vec![Some(
            b"\x00\xff" as &[u8],
        )]))],
    );
    let vnode_count = NonZeroU32::new(4).unwrap();
    let vnode = PartitionKeyCodecV1::vnode_for_encoded(&keys[0], vnode_count);
    let roster = [partial_v2::ExpectedRosterEntry {
        operator_identity_sha256: [0x22; 32],
        state_table_identity_sha256: [0x33; 32],
        vnode,
        managed_envelope_version: 1,
    }];

    let mut full_context = context(&routing, ArtifactKind::Full, 1, None);
    full_context.vnode_count = vnode_count;
    full_context.vnode = vnode;
    full_context.contract = AggregateContractV1::new(&routing, false);
    let full_body = encode(full_context, &[row(&keys[0], 1, 1, -1)], &mut budget()).unwrap();
    let full_outer_context = partial_v2::ExpectedContext {
        attempt: CheckpointAttempt::canonical(1),
        assignment_version: 7,
        partitioning_abi_version: PARTITIONING_ABI_VERSION,
        vnode_count: vnode_count.get(),
        vnode,
        assignment_certificate_sha256: [0x11; 32],
        roster: &roster,
    };
    let full_outer = partial_v2::encode(
        full_outer_context,
        &[partial_v2::EncodeEntry {
            operator_identity_sha256: [0x22; 32],
            state_table_identity_sha256: [0x33; 32],
            payload: partial_v2::EncodeEntryPayload::Body {
                artifact_kind: partial_v2::ArtifactKind::Full,
                body: &full_body,
                parent: None,
            },
        }],
        partial_limits(),
    )
    .unwrap();
    let full_fixture = fixture_bytes(include_str!(
        "../../../tests/fixtures/managed_state_v1/vnode_partial_composed_full.hex"
    ));
    assert_eq!(full_outer, full_fixture);
    let decoded_full_outer =
        partial_v2::decode(&full_fixture, full_outer_context, partial_limits()).unwrap();
    let full_entry = decoded_full_outer.entries().next().unwrap().unwrap();
    let parent_entry_sha256 = full_entry.contextual_sha256;
    let partial_v2::DecodedEntryPayload::Body {
        artifact_kind: partial_v2::ArtifactKind::Full,
        body: decoded_full_body,
        parent: None,
        ..
    } = full_entry.payload
    else {
        panic!("composed FULL fixture did not contain one FULL BODY");
    };
    assert_eq!(
        decode(decoded_full_body, full_context, &mut budget())
            .unwrap()
            .rows()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        [row(&keys[0], 1, 1, -1)]
    );

    let aggregate_parent = ParentLink::new(CheckpointAttempt::canonical(1), parent_entry_sha256);
    let outer_parent = partial_v2::ParentEntryLink {
        attempt: CheckpointAttempt::canonical(1),
        entry_sha256: parent_entry_sha256,
    };
    let mut delta_context = context(&routing, ArtifactKind::Delta, 2, Some(1));
    delta_context.parent = Some(aggregate_parent);
    delta_context.vnode_count = vnode_count;
    delta_context.vnode = vnode;
    delta_context.contract = AggregateContractV1::new(&routing, false);
    let delta_body = encode(delta_context, &[row(&keys[0], 2, 2, 8)], &mut budget()).unwrap();
    let delta_outer_context = partial_v2::ExpectedContext {
        attempt: CheckpointAttempt::canonical(2),
        ..full_outer_context
    };
    let delta_outer = partial_v2::encode(
        delta_outer_context,
        &[partial_v2::EncodeEntry {
            operator_identity_sha256: [0x22; 32],
            state_table_identity_sha256: [0x33; 32],
            payload: partial_v2::EncodeEntryPayload::Body {
                artifact_kind: partial_v2::ArtifactKind::Delta,
                body: &delta_body,
                parent: Some(outer_parent),
            },
        }],
        partial_limits(),
    )
    .unwrap();
    let delta_fixture = fixture_bytes(include_str!(
        "../../../tests/fixtures/managed_state_v1/vnode_partial_composed_delta.hex"
    ));
    assert_eq!(delta_outer, delta_fixture);
    let decoded_delta_outer =
        partial_v2::decode(&delta_fixture, delta_outer_context, partial_limits()).unwrap();
    let delta_entry = decoded_delta_outer.entries().next().unwrap().unwrap();
    let partial_v2::DecodedEntryPayload::Body {
        artifact_kind: partial_v2::ArtifactKind::Delta,
        body: decoded_delta_body,
        parent: Some(decoded_parent),
        ..
    } = delta_entry.payload
    else {
        panic!("composed DELTA fixture did not contain one DELTA BODY");
    };
    assert_eq!(decoded_parent, outer_parent);
    assert_eq!(
        decode(decoded_delta_body, delta_context, &mut budget())
            .unwrap()
            .rows()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        [row(&keys[0], 2, 2, 8)]
    );
}

#[test]
fn state_is_exactly_24_bytes_and_rejects_impossible_values() {
    let state = CountSumStateV1::persisted(2, 1, -2).unwrap();
    let encoded = state.encode().unwrap();
    assert_eq!(encoded.len(), 24);
    assert_eq!(&encoded[0..8], &2_u64.to_be_bytes());
    assert_eq!(&encoded[8..16], &1_u64.to_be_bytes());
    assert_eq!(&encoded[16..24], &(-2_i64).to_be_bytes());
    assert_eq!(CountSumStateV1::decode(&encoded).unwrap(), state);
    assert_eq!(state.count(), 2);
    assert_eq!(state.sum_non_null_count(), 1);
    assert_eq!(state.sum(), Some(-2));

    assert!(CountSumStateV1::persisted(0, 0, 0).is_err());
    assert!(CountSumStateV1::persisted(MAX_SQL_COUNT + 1, 0, 0).is_err());
    assert!(CountSumStateV1::persisted(1, 2, 1).is_err());
    assert!(CountSumStateV1::persisted(1, 0, 1).is_err());
    assert_eq!(CountSumStateV1::persisted(1, 0, 0).unwrap().sum(), None);
}

#[test]
fn append_preview_checks_every_prefix_and_leaves_the_caller_unchanged() {
    let empty = CountSumStateV1::empty();
    assert_eq!(
        empty.preview_append(&[Some(i64::MAX), Some(1), Some(-1)]),
        Err(ArtifactError::SumOverflow)
    );
    assert_eq!(empty, CountSumStateV1::empty());

    let after_first = empty.preview_append(&[Some(i64::MAX)]).unwrap();
    assert_eq!(
        after_first.preview_append(&[Some(1), Some(-1)]),
        Err(ArtifactError::SumOverflow)
    );
    assert_eq!(after_first.sum(), Some(i64::MAX));

    assert_eq!(
        empty.preview_append(&[Some(i64::MIN), Some(-1), Some(1)]),
        Err(ArtifactError::SumOverflow)
    );
    let max_count = CountSumStateV1::persisted(MAX_SQL_COUNT, 0, 0).unwrap();
    assert_eq!(
        max_count.preview_append(&[None]),
        Err(ArtifactError::CountOverflow)
    );
}

#[test]
fn contract_bytes_freeze_the_first_semantic_shape() {
    let routing = schema(DataType::Int64, true);
    let contract = AggregateContractV1::new(&routing, true).encode();
    assert_eq!(&contract[0..8], b"LDBMAC\0\0");
    assert_eq!(&contract[8..10], &1_u16.to_be_bytes());
    assert_eq!(&contract[10..12], &64_u16.to_be_bytes());
    assert_eq!(&contract[12..16], &1_u32.to_be_bytes());
    assert_eq!(&contract[16..20], &[0, 1, 0, 1]);
    assert_eq!(&contract[20..32], &[1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 24]);
    assert_eq!(&contract[32..64], &routing.sha256());

    let non_nullable = AggregateContractV1::new(&routing, false).encode();
    assert_ne!(contract, non_nullable);
    assert_eq!(non_nullable[25], 0);
}

#[test]
fn non_nullable_sum_requires_one_non_null_input_per_counted_row() {
    let routing = schema(DataType::Int64, false);
    let mut expected = context(&routing, ArtifactKind::Full, 1, None);
    expected.contract = AggregateContractV1::new(&routing, false);

    assert_eq!(
        encode(expected, &[row(&[1], 3, 0, 0)], &mut budget()),
        Err(ArtifactError::Invalid("non-null SUM count"))
    );

    let mut encoded = encode(expected, &[row(&[1], 1, 1, 9)], &mut budget()).unwrap();
    let state_offset = rows_offset(&encoded) + 4 + 1;
    put_test(&mut encoded, state_offset + 8, &0_u64.to_be_bytes());
    put_test(&mut encoded, state_offset + 16, &0_i64.to_be_bytes());
    refresh_rows_digest(&mut encoded);
    assert_eq!(
        decode(&encoded, expected, &mut budget()),
        Err(ArtifactError::Invalid("non-null SUM count"))
    );
}

#[test]
fn full_round_trip_allows_a_zero_length_key_but_not_zero_rows() {
    let routing = schema(DataType::Null, true);
    let expected = context(&routing, ArtifactKind::Full, 1, None);
    let encoded = encode(expected, &[row(&[], 3, 0, 0)], &mut budget()).unwrap();
    let decoded = decode(&encoded, expected, &mut budget()).unwrap();
    assert_eq!(decoded.row_count(), 1);
    assert_eq!(decoded.key_bytes(), 0);
    assert_eq!(decoded.state_bytes(), 24);
    let decoded_rows = decoded.rows().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(decoded_rows, vec![row(&[], 3, 0, 0)]);

    assert_eq!(
        encode(expected, &[], &mut budget()),
        Err(ArtifactError::Invalid("zero-row FULL/DELTA"))
    );
}

#[test]
fn empty_is_an_explicit_nonzero_envelope() {
    let routing = schema(DataType::Int64, false);
    let expected = context(&routing, ArtifactKind::Empty, 1, None);
    let encoded = encode(expected, &[], &mut budget()).unwrap();
    assert!(encoded.len() > ARTIFACT_HEADER_LEN);
    let decoded = decode(&encoded, expected, &mut budget()).unwrap();
    assert_eq!(decoded.row_count(), 0);
    assert_eq!(decoded.rows().count(), 0);
    assert_eq!(&encoded[320..352], &sha256(&[]));
    assert_eq!(
        encode(expected, &[row(&[1], 1, 1, 1)], &mut budget()),
        Err(ArtifactError::Invalid("EMPTY rows"))
    );
}

#[test]
fn delta_requires_the_immediately_preceding_attempt() {
    let routing = schema(DataType::Int64, false);
    let valid = context(&routing, ArtifactKind::Delta, 2, Some(1));
    assert!(encode(valid, &[row(&[1], 2, 1, 4)], &mut budget()).is_ok());

    let skipped = context(&routing, ArtifactKind::Delta, 3, Some(1));
    assert_eq!(
        encode(skipped, &[row(&[1], 2, 1, 4)], &mut budget()),
        Err(ArtifactError::Invalid("expected parent context"))
    );
    let parentless = context(&routing, ArtifactKind::Delta, 2, None);
    assert_eq!(
        encode(parentless, &[row(&[1], 2, 1, 4)], &mut budget()),
        Err(ArtifactError::Invalid("expected parent context"))
    );
}

#[test]
fn sorted_unique_keys_are_required() {
    let routing = schema(DataType::Binary, false);
    let expected = context(&routing, ArtifactKind::Full, 1, None);
    assert_eq!(
        encode(
            expected,
            &[row(&[2], 1, 1, 2), row(&[1], 1, 1, 1)],
            &mut budget()
        ),
        Err(ArtifactError::Invalid("row key order"))
    );
    assert_eq!(
        encode(
            expected,
            &[row(&[1], 1, 1, 1), row(&[1], 2, 2, 2)],
            &mut budget()
        ),
        Err(ArtifactError::Invalid("row key order"))
    );
}

#[test]
fn decoder_rejects_duplicate_keys_even_after_payload_digest_is_recomputed() {
    let routing = schema(DataType::Binary, false);
    let expected = context(&routing, ArtifactKind::Full, 1, None);
    let mut encoded = encode(
        expected,
        &[row(&[1], 1, 1, 1), row(&[2], 2, 2, 3)],
        &mut budget(),
    )
    .unwrap();
    let second_key = rows_offset(&encoded) + (4 + 1 + STATE_WIDTH) + 4;
    encoded[second_key] = 1;
    refresh_rows_digest(&mut encoded);
    assert_eq!(
        decode(&encoded, expected, &mut budget()),
        Err(ArtifactError::Invalid("row key order"))
    );
}

#[test]
fn decoder_rejects_cross_vnode_rows() {
    let routing = schema(DataType::Binary, false);
    let vnode_count = NonZeroU32::new(2).unwrap();
    let key = (0_u8..=u8::MAX)
        .map(|value| [value])
        .find(|value| PartitionKeyCodecV1::vnode_for_encoded(value, vnode_count) == 0)
        .unwrap();
    let mut source = context(&routing, ArtifactKind::Full, 1, None);
    source.vnode_count = vnode_count;
    source.vnode = 0;
    let mut encoded = encode(source, &[row(&key, 1, 1, 1)], &mut budget()).unwrap();

    let mut wrong = source;
    wrong.vnode = 1;
    put_test(&mut encoded, 76, &1_u32.to_be_bytes());
    assert_eq!(
        decode(&encoded, wrong, &mut budget()),
        Err(ArtifactError::Invalid("row vnode"))
    );
}

#[test]
fn every_truncation_and_trailing_bytes_fail() {
    let routing = schema(DataType::Int64, false);
    let expected = context(&routing, ArtifactKind::Full, 1, None);
    let encoded = encode(expected, &[row(&[1], 1, 1, 9)], &mut budget()).unwrap();
    for cut in 0..encoded.len() {
        assert!(
            decode(&encoded[..cut], expected, &mut budget()).is_err(),
            "cut={cut}"
        );
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert_eq!(
        decode(&trailing, expected, &mut budget()),
        Err(ArtifactError::Invalid("total length"))
    );
}

#[test]
fn reserved_digest_and_expected_context_checks_are_independent() {
    let routing = schema(DataType::Int64, false);
    let expected = context(&routing, ArtifactKind::Full, 1, None);
    let encoded = encode(expected, &[row(&[1], 1, 1, 9)], &mut budget()).unwrap();

    let mut reserved = encoded.clone();
    reserved[14] = 1;
    assert_eq!(
        decode(&reserved, expected, &mut budget()),
        Err(ArtifactError::Invalid("flags/reserved field"))
    );

    let mut stale_digest = encoded.clone();
    let offset = rows_offset(&stale_digest);
    stale_digest[offset + 4] ^= 1;
    assert_eq!(
        decode(&stale_digest, expected, &mut budget()),
        Err(ArtifactError::Invalid("contract/payload digest"))
    );

    let mut rewritten_routing = encoded;
    rewritten_routing[ARTIFACT_HEADER_LEN] ^= 1;
    let routing_len =
        usize::try_from(u64::from_be_bytes(field(&rewritten_routing, 120).unwrap())).unwrap();
    let digest = sha256(&rewritten_routing[ARTIFACT_HEADER_LEN..ARTIFACT_HEADER_LEN + routing_len]);
    put_test(&mut rewritten_routing, 256, &digest);
    assert_eq!(
        decode(&rewritten_routing, expected, &mut budget()),
        Err(ArtifactError::Invalid("routing schema"))
    );
}

#[test]
fn semantic_corruption_fails_after_internal_digest_is_recomputed() {
    let routing = schema(DataType::Null, true);
    let expected = context(&routing, ArtifactKind::Full, 1, None);
    let mut encoded = encode(expected, &[row(&[], 1, 0, 0)], &mut budget()).unwrap();
    let count_offset = rows_offset(&encoded) + 4;
    put_test(&mut encoded, count_offset, &0_u64.to_be_bytes());
    refresh_rows_digest(&mut encoded);
    assert_eq!(
        decode(&encoded, expected, &mut budget()),
        Err(ArtifactError::Invalid("zero persisted COUNT(*)"))
    );
}

#[test]
fn exact_limits_pass_and_max_plus_one_fails() {
    let routing = schema(DataType::Binary, false);
    let expected = context(&routing, ArtifactKind::Full, 1, None);
    let encoded = encode(expected, &[row(&[1, 2], 1, 1, 2)], &mut budget()).unwrap();

    let mut exact_template = budget();
    exact_template.remaining_artifact_bytes = u64::try_from(encoded.len()).unwrap();
    exact_template.remaining_rows = 1;
    exact_template.remaining_key_bytes = 2;
    exact_template.remaining_state_bytes = 24;
    exact_template.encoded_key_bytes_max = 2;
    exact_template.stored_state_bytes_max = 24;
    exact_template.state_contract_bytes_max = 64;
    exact_template.routing_schema_bytes_max = u64::try_from(routing.as_bytes().len()).unwrap();
    exact_template.envelope_metadata_bytes_max = u64::from_be_bytes(field(&encoded, 144).unwrap());
    let mut exact_decode = exact_template.clone();
    assert!(decode(&encoded, expected, &mut exact_decode).is_ok());
    assert_eq!(exact_decode.remaining_artifact_bytes, 0);
    assert_eq!(exact_decode.remaining_rows, 0);
    assert_eq!(exact_decode.remaining_key_bytes, 0);
    assert_eq!(exact_decode.remaining_state_bytes, 0);

    let cases = [
        ("artifact", {
            let mut value = exact_template.clone();
            value.remaining_artifact_bytes -= 1;
            value
        }),
        ("rows", {
            let mut value = exact_template.clone();
            value.remaining_rows = 0;
            value
        }),
        ("keys", {
            let mut value = exact_template.clone();
            value.remaining_key_bytes = 1;
            value
        }),
        ("state", {
            let mut value = exact_template.clone();
            value.remaining_state_bytes = 23;
            value
        }),
        ("key", {
            let mut value = exact_template.clone();
            value.encoded_key_bytes_max = 1;
            value
        }),
        ("state width", {
            let mut value = exact_template.clone();
            value.stored_state_bytes_max = 23;
            value
        }),
        ("contract", {
            let mut value = exact_template.clone();
            value.state_contract_bytes_max = 63;
            value
        }),
        ("routing", {
            let mut value = exact_template.clone();
            value.routing_schema_bytes_max -= 1;
            value
        }),
        ("metadata", {
            let mut value = exact_template.clone();
            value.envelope_metadata_bytes_max -= 1;
            value
        }),
    ];
    for (name, mut too_small) in cases {
        let before = too_small.clone();
        assert!(
            decode(&encoded, expected, &mut too_small).is_err(),
            "{name}"
        );
        assert_eq!(too_small, before, "failed decode charged {name} budget");
    }
}

#[test]
fn one_mutable_budget_is_consumed_across_multiple_bodies() {
    let routing = schema(DataType::Binary, false);
    let expected = context(&routing, ArtifactKind::Full, 1, None);
    let encoded = encode(expected, &[row(&[1, 2], 1, 1, 2)], &mut budget()).unwrap();
    let encoded_len = u64::try_from(encoded.len()).unwrap();
    let mut object_budget = budget();
    object_budget.remaining_artifact_bytes = encoded_len * 2;
    object_budget.remaining_rows = 1;
    object_budget.remaining_key_bytes = 2;
    object_budget.remaining_state_bytes = 24;

    assert!(decode(&encoded, expected, &mut object_budget).is_ok());
    assert_eq!(object_budget.remaining_artifact_bytes, encoded_len);
    assert_eq!(object_budget.remaining_rows, 0);
    assert_eq!(object_budget.remaining_key_bytes, 0);
    assert_eq!(object_budget.remaining_state_bytes, 0);

    let exhausted = object_budget.clone();
    assert_eq!(
        decode(&encoded, expected, &mut object_budget),
        Err(ArtifactError::Limit("remaining row limit"))
    );
    assert_eq!(object_budget, exhausted);
}

proptest! {
    #[test]
    fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..1024)) {
        let routing = schema(DataType::Int64, false);
        let expected = context(&routing, ArtifactKind::Full, 1, None);
        let _ = decode(&bytes, expected, &mut budget());
    }
}
