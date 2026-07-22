use std::num::NonZeroU32;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use proptest::prelude::*;

use super::*;

fn schema(data_type: DataType, nullable: bool) -> PartitionKeySchemaV1 {
    PartitionKeySchemaV1::try_new(&[Arc::new(Field::new("group", data_type, nullable))]).unwrap()
}

fn context<'a>(
    routing_schema: &'a PartitionKeySchemaV1,
    kind: ArtifactKind,
    attempt: u64,
    parent: Option<u64>,
) -> ArtifactContext<'a> {
    ArtifactContext {
        kind,
        attempt: CheckpointAttempt::canonical(attempt),
        parent: parent.map(|checkpoint| {
            ParentLink::new(CheckpointAttempt::canonical(checkpoint), [0x44; 32])
        }),
        assignment_version: 7,
        assignment_certificate_sha256: [0x11; 32],
        operator_identity_sha256: [0x22; 32],
        table_identity_sha256: [0x33; 32],
        vnode_count: NonZeroU32::new(1).unwrap(),
        vnode: 0,
        routing_schema,
        contract: AggregateContractV1::new(routing_schema, true),
    }
}

fn limits() -> AggregateBodyLimits {
    AggregateBodyLimits {
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

fn row<'a>(key: &'a [u8], count: u64, non_null: u64, sum: i64) -> AggregateRow<'a> {
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
fn full_round_trip_allows_a_zero_length_key_but_not_zero_rows() {
    let routing = schema(DataType::Null, true);
    let expected = context(&routing, ArtifactKind::Full, 1, None);
    let encoded = encode(expected, &[row(&[], 3, 0, 0)], limits()).unwrap();
    let decoded = decode(&encoded, expected, limits()).unwrap();
    assert_eq!(decoded.row_count(), 1);
    assert_eq!(decoded.key_bytes(), 0);
    assert_eq!(decoded.state_bytes(), 24);
    let decoded_rows = decoded.rows().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(decoded_rows, vec![row(&[], 3, 0, 0)]);

    assert_eq!(
        encode(expected, &[], limits()),
        Err(ArtifactError::Invalid("zero-row FULL/DELTA"))
    );
}

#[test]
fn empty_is_an_explicit_nonzero_envelope() {
    let routing = schema(DataType::Int64, false);
    let expected = context(&routing, ArtifactKind::Empty, 1, None);
    let encoded = encode(expected, &[], limits()).unwrap();
    assert!(encoded.len() > ARTIFACT_HEADER_LEN);
    let decoded = decode(&encoded, expected, limits()).unwrap();
    assert_eq!(decoded.row_count(), 0);
    assert_eq!(decoded.rows().count(), 0);
    assert_eq!(&encoded[320..352], &sha256(&[]));
    assert_eq!(
        encode(expected, &[row(&[1], 1, 1, 1)], limits()),
        Err(ArtifactError::Invalid("EMPTY rows"))
    );
}

#[test]
fn delta_requires_the_immediately_preceding_attempt() {
    let routing = schema(DataType::Int64, false);
    let valid = context(&routing, ArtifactKind::Delta, 2, Some(1));
    assert!(encode(valid, &[row(&[1], 2, 1, 4)], limits()).is_ok());

    let skipped = context(&routing, ArtifactKind::Delta, 3, Some(1));
    assert_eq!(
        encode(skipped, &[row(&[1], 2, 1, 4)], limits()),
        Err(ArtifactError::Invalid("expected parent context"))
    );
    let parentless = context(&routing, ArtifactKind::Delta, 2, None);
    assert_eq!(
        encode(parentless, &[row(&[1], 2, 1, 4)], limits()),
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
            limits()
        ),
        Err(ArtifactError::Invalid("row key order"))
    );
    assert_eq!(
        encode(
            expected,
            &[row(&[1], 1, 1, 1), row(&[1], 2, 2, 2)],
            limits()
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
        limits(),
    )
    .unwrap();
    let second_key = rows_offset(&encoded) + (4 + 1 + STATE_WIDTH) + 4;
    encoded[second_key] = 1;
    refresh_rows_digest(&mut encoded);
    assert_eq!(
        decode(&encoded, expected, limits()),
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
    let mut encoded = encode(source, &[row(&key, 1, 1, 1)], limits()).unwrap();

    let mut wrong = source;
    wrong.vnode = 1;
    put_test(&mut encoded, 76, &1_u32.to_be_bytes());
    assert_eq!(
        decode(&encoded, wrong, limits()),
        Err(ArtifactError::Invalid("row vnode"))
    );
}

#[test]
fn every_truncation_and_trailing_bytes_fail() {
    let routing = schema(DataType::Int64, false);
    let expected = context(&routing, ArtifactKind::Full, 1, None);
    let encoded = encode(expected, &[row(&[1], 1, 1, 9)], limits()).unwrap();
    for cut in 0..encoded.len() {
        assert!(
            decode(&encoded[..cut], expected, limits()).is_err(),
            "cut={cut}"
        );
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert_eq!(
        decode(&trailing, expected, limits()),
        Err(ArtifactError::Invalid("total length"))
    );
}

#[test]
fn reserved_digest_and_expected_context_checks_are_independent() {
    let routing = schema(DataType::Int64, false);
    let expected = context(&routing, ArtifactKind::Full, 1, None);
    let encoded = encode(expected, &[row(&[1], 1, 1, 9)], limits()).unwrap();

    let mut reserved = encoded.clone();
    reserved[14] = 1;
    assert_eq!(
        decode(&reserved, expected, limits()),
        Err(ArtifactError::Invalid("flags/reserved field"))
    );

    let mut stale_digest = encoded.clone();
    let offset = rows_offset(&stale_digest);
    stale_digest[offset + 4] ^= 1;
    assert_eq!(
        decode(&stale_digest, expected, limits()),
        Err(ArtifactError::Invalid("contract/payload digest"))
    );

    let mut rewritten_routing = encoded;
    rewritten_routing[ARTIFACT_HEADER_LEN] ^= 1;
    let routing_len =
        usize::try_from(u64::from_be_bytes(field(&rewritten_routing, 120).unwrap())).unwrap();
    let digest = sha256(&rewritten_routing[ARTIFACT_HEADER_LEN..ARTIFACT_HEADER_LEN + routing_len]);
    put_test(&mut rewritten_routing, 256, &digest);
    assert_eq!(
        decode(&rewritten_routing, expected, limits()),
        Err(ArtifactError::Invalid("routing schema"))
    );
}

#[test]
fn semantic_corruption_fails_after_internal_digest_is_recomputed() {
    let routing = schema(DataType::Null, true);
    let expected = context(&routing, ArtifactKind::Full, 1, None);
    let mut encoded = encode(expected, &[row(&[], 1, 0, 0)], limits()).unwrap();
    let count_offset = rows_offset(&encoded) + 4;
    put_test(&mut encoded, count_offset, &0_u64.to_be_bytes());
    refresh_rows_digest(&mut encoded);
    assert_eq!(
        decode(&encoded, expected, limits()),
        Err(ArtifactError::Invalid("zero persisted COUNT(*)"))
    );
}

#[test]
fn exact_limits_pass_and_max_plus_one_fails() {
    let routing = schema(DataType::Binary, false);
    let expected = context(&routing, ArtifactKind::Full, 1, None);
    let encoded = encode(expected, &[row(&[1, 2], 1, 1, 2)], limits()).unwrap();

    let mut exact = limits();
    exact.remaining_artifact_bytes = u64::try_from(encoded.len()).unwrap();
    exact.remaining_rows = 1;
    exact.remaining_key_bytes = 2;
    exact.remaining_state_bytes = 24;
    exact.encoded_key_bytes_max = 2;
    exact.stored_state_bytes_max = 24;
    exact.state_contract_bytes_max = 64;
    exact.routing_schema_bytes_max = u64::try_from(routing.as_bytes().len()).unwrap();
    exact.envelope_metadata_bytes_max = u64::from_be_bytes(field(&encoded, 144).unwrap());
    assert!(decode(&encoded, expected, exact).is_ok());

    let cases = [
        ("artifact", {
            let mut value = exact;
            value.remaining_artifact_bytes -= 1;
            value
        }),
        ("rows", {
            let mut value = exact;
            value.remaining_rows = 0;
            value
        }),
        ("keys", {
            let mut value = exact;
            value.remaining_key_bytes = 1;
            value
        }),
        ("state", {
            let mut value = exact;
            value.remaining_state_bytes = 23;
            value
        }),
        ("key", {
            let mut value = exact;
            value.encoded_key_bytes_max = 1;
            value
        }),
        ("state width", {
            let mut value = exact;
            value.stored_state_bytes_max = 23;
            value
        }),
        ("contract", {
            let mut value = exact;
            value.state_contract_bytes_max = 63;
            value
        }),
        ("routing", {
            let mut value = exact;
            value.routing_schema_bytes_max -= 1;
            value
        }),
        ("metadata", {
            let mut value = exact;
            value.envelope_metadata_bytes_max -= 1;
            value
        }),
    ];
    for (name, too_small) in cases {
        assert!(decode(&encoded, expected, too_small).is_err(), "{name}");
    }
}

proptest! {
    #[test]
    fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..1024)) {
        let routing = schema(DataType::Int64, false);
        let expected = context(&routing, ArtifactKind::Full, 1, None);
        let _ = decode(&bytes, expected, limits());
    }
}
