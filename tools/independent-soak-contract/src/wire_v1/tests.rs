use super::*;

const OPERATION_ID: [u8; 32] = [0xa1; 32];
const DATA_INTERVAL: [u8; 16] = [0xb2; 16];
const FIRST_INTERVAL: [u8; 16] = [0x11; 16];
const SUCCESSOR_INTERVAL: [u8; 16] = [0x12; 16];
const DEPLOYMENT: [u8; 16] = [0x22; 16];
const PIPELINE_INCARNATION: [u8; 16] = [0x33; 16];
const PIPELINE_DIGEST: [u8; 32] = [0x44; 32];
const ASSIGNMENT_DIGEST: [u8; 32] = [0x55; 32];
const WRITER_BOOT: [u8; 16] = [0x66; 16];
const COMMITTED_INDEX_DIGEST: [u8; 32] = [0x77; 32];
const BASE_ASSIGNMENT_DIGEST: [u8; 32] = [0x88; 32];
const TOPOLOGY_DIGEST: [u8; 32] = [0x99; 32];
const FOUR_VNODES: [u8; 1] = [0x0f];

// These literals were produced from the frozen offset table, not by the encoder under test.
const DATA_GOLDEN_HEX: &str = concat!(
        "4c44424f010100000038a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1b2b2b2b2b2b2",
        "b2b2b2b2b2b2b2b2b2b20102030405060708",
    );
const FIRST_MARKER_GOLDEN_HEX: &str = concat!(
        "4c44424f01020000013b1111111111111111111111111111111100000000000000000000000000000000222222222222",
        "222222222222222222223333333333333333333333333333333300054444444444444444444444444444444444444444",
        "444444444444444444444444000100010004010203040506070855555555555555555555555555555555555555555555",
        "555555555555555555551112131415161718666666666666666666666666666666662122232425262728313233343536",
        "373831323334353637387777777777777777777777777777777777777777777777777777777777777777515253545556",
        "575888888888888888888888888888888888888888888888888888888888888888889999999999999999999999999999",
        "9999999999999999999999999999999999990473696e6b026f70036f75740573686172640f",
    );
const SUCCESSOR_MARKER_GOLDEN_HEX: &str = concat!(
        "4c44424f01020001013b1212121212121212121212121212121211111111111111111111111111111111222222222222",
        "222222222222222222223333333333333333333333333333333300054444444444444444444444444444444444444444",
        "444444444444444444444444000100010004010203040506070855555555555555555555555555555555555555555555",
        "555555555555555555551112131415161718666666666666666666666666666666662122232425262728313233343536",
        "373831323334353637387777777777777777777777777777777777777777777777777777777777777777515253545556",
        "575888888888888888888888888888888888888888888888888888888888888888889999999999999999999999999999",
        "9999999999999999999999999999999999990473696e6b026f70036f75740573686172640f",
    );

const _: [(); MAX_MARKER_BODY_LEN] = [(); MARKER_FIXED_BODY_LEN
    + 4
    + MAX_SINK_ID_LEN
    + MAX_OPERATOR_ID_LEN
    + MAX_OUTPUT_ID_LEN
    + MAX_SHARD_ID_LEN
    + MAX_VNODE_BITMAP_LEN];
const _: [(); 4_325_376] = [(); MAX_DATA_HEADER_BYTES_PER_BATCH];

fn sample_marker(successor: bool) -> MarkerRef<'static> {
    MarkerRef {
        current_interval_id: if successor {
            &SUCCESSOR_INTERVAL
        } else {
            &FIRST_INTERVAL
        },
        predecessor_interval_id: successor.then_some(&FIRST_INTERVAL),
        deployment_uuid: &DEPLOYMENT,
        pipeline_incarnation_id: &PIPELINE_INCARNATION,
        pipeline_identity_version: PIPELINE_IDENTITY_VERSION,
        pipeline_identity_sha256: &PIPELINE_DIGEST,
        key_to_vnode_abi_version: KEY_TO_VNODE_ABI_VERSION,
        sink_partitioning_abi_version: SINK_PARTITIONING_ABI_VERSION,
        vnode_count: 4,
        current_assignment_version: 0x0102_0304_0506_0708,
        current_assignment_sha256: &ASSIGNMENT_DIGEST,
        writer_node_id: 0x1112_1314_1516_1718,
        writer_boot_uuid: &WRITER_BOOT,
        durable_process_term: 0x2122_2324_2526_2728,
        recovery_epoch: 0x3132_3334_3536_3738,
        recovery_checkpoint_id: 0x3132_3334_3536_3738,
        committed_index_sha256: &COMMITTED_INDEX_DIGEST,
        recovery_base_assignment_version: 0x5152_5354_5556_5758,
        recovery_base_assignment_sha256: &BASE_ASSIGNMENT_DIGEST,
        topology_sha256: &TOPOLOGY_DIGEST,
        sink_id: "sink",
        operator_id: "op",
        output_id: "out",
        shard_id: "shard",
        vnode_bitmap: &FOUR_VNODES,
    }
}

#[test]
fn data_literal_golden_round_trips_on_the_stack() {
    let golden = decode_hex(DATA_GOLDEN_HEX);
    assert_eq!(golden.len(), DATA_ENCODED_LEN);
    let header = DataHeaderRef {
        operation_id: &OPERATION_ID,
        writer_interval_id: &DATA_INTERVAL,
        admission_sequence: 0x0102_0304_0506_0708,
    };

    let encoded = encode_data(&header).unwrap();
    assert_eq!(encoded.as_slice(), golden);
    assert_eq!(std::mem::size_of_val(&encoded), DATA_ENCODED_LEN);

    let decoded = decode_data(&golden).unwrap();
    assert_eq!(decoded, header);
    assert_borrowed_from(decoded.operation_id.as_ptr(), &golden);
    assert_borrowed_from(decoded.writer_interval_id.as_ptr(), &golden);
    assert_eq!(&golden[58..66], &0x0102_0304_0506_0708_u64.to_be_bytes());
}

#[test]
fn first_and_successor_marker_literal_goldens_reencode_exactly() {
    for (expected, literal) in [
        (sample_marker(false), FIRST_MARKER_GOLDEN_HEX),
        (sample_marker(true), SUCCESSOR_MARKER_GOLDEN_HEX),
    ] {
        let golden = decode_hex(literal);
        assert_eq!(golden.len(), 325);
        assert_eq!(u16::from_be_bytes([golden[8], golden[9]]), 315);

        let mut encoded = Vec::new();
        assert_eq!(encode_marker_into(&expected, &mut encoded).unwrap(), 325);
        assert_eq!(encoded, golden);

        let decoded = decode_marker(&golden).unwrap();
        assert_eq!(decoded, expected);
        let mut reencoded = Vec::new();
        encode_marker_into(&decoded, &mut reencoded).unwrap();
        assert_eq!(reencoded, golden);
        assert_borrowed_from(decoded.current_interval_id.as_ptr(), &golden);
        assert_borrowed_from(decoded.sink_id.as_ptr(), &golden);
        assert_borrowed_from(decoded.vnode_bitmap.as_ptr(), &golden);
    }
}

#[test]
fn every_literal_prefix_is_rejected_as_truncated() {
    for golden in [
        decode_hex(DATA_GOLDEN_HEX),
        decode_hex(FIRST_MARKER_GOLDEN_HEX),
        decode_hex(SUCCESSOR_MARKER_GOLDEN_HEX),
    ] {
        for length in 0..golden.len() {
            assert!(decode_data(&golden[..length]).is_err());
            assert!(decode_marker(&golden[..length]).is_err());
        }
    }
}

#[test]
fn every_small_trailing_suffix_is_rejected() {
    for (golden, data) in [
        (decode_hex(DATA_GOLDEN_HEX), true),
        (decode_hex(FIRST_MARKER_GOLDEN_HEX), false),
        (decode_hex(SUCCESSOR_MARKER_GOLDEN_HEX), false),
    ] {
        for extra in 1..=PREFIX_LEN {
            let mut hostile = golden.clone();
            hostile.resize(hostile.len() + extra, 0xa5);
            let error = if data {
                decode_data(&hostile).unwrap_err()
            } else {
                decode_marker(&hostile).unwrap_err()
            };
            assert_eq!(error, WireError::TrailingBytes);
        }
    }
}

#[test]
fn magic_version_kind_flags_and_cross_kind_fail_closed() {
    let data = decode_hex(DATA_GOLDEN_HEX);
    let marker = decode_hex(FIRST_MARKER_GOLDEN_HEX);

    for index in 0..MAGIC.len() {
        let mut hostile = data.clone();
        hostile[index] ^= 0xff;
        assert_eq!(decode_data(&hostile), Err(WireError::InvalidMagic));
    }
    for version in [0, 2, u8::MAX] {
        let mut hostile = data.clone();
        hostile[4] = version;
        assert_eq!(
            decode_data(&hostile),
            Err(WireError::UnsupportedVersion(version))
        );
    }
    for kind in [0, 3, u8::MAX] {
        let mut hostile = data.clone();
        hostile[5] = kind;
        assert_eq!(decode_data(&hostile), Err(WireError::InvalidKind(kind)));
    }

    assert!(matches!(
        decode_marker(&data),
        Err(WireError::UnexpectedKind {
            expected: MARKER_KIND,
            actual: DATA_KIND
        })
    ));
    assert!(matches!(
        decode_data(&marker),
        Err(WireError::UnexpectedKind {
            expected: DATA_KIND,
            actual: MARKER_KIND
        })
    ));

    for bit in 0..u16::BITS {
        let flags = 1_u16 << bit;
        let mut hostile = data.clone();
        hostile[6..8].copy_from_slice(&flags.to_be_bytes());
        assert_eq!(decode_data(&hostile), Err(WireError::InvalidFlags(flags)));
    }
    for bit in 1..u16::BITS {
        let flags = 1_u16 << bit;
        let mut hostile = marker.clone();
        hostile[6..8].copy_from_slice(&flags.to_be_bytes());
        assert_eq!(decode_marker(&hostile), Err(WireError::InvalidFlags(flags)));
    }
}

#[test]
fn structurally_consistent_short_and_extra_bodies_are_rejected() {
    let marker = decode_hex(FIRST_MARKER_GOLDEN_HEX);
    let original_body_len = marker.len() - PREFIX_LEN;
    for body_len in 0..original_body_len {
        let mut hostile = marker[..PREFIX_LEN + body_len].to_vec();
        hostile[8..10].copy_from_slice(&(body_len as u16).to_be_bytes());
        assert!(
            decode_marker(&hostile).is_err(),
            "accepted body length {body_len}"
        );
    }

    let mut extra = marker;
    extra.push(0);
    let body_len = extra.len() - PREFIX_LEN;
    extra[8..10].copy_from_slice(&(body_len as u16).to_be_bytes());
    assert_eq!(
        decode_marker(&extra),
        Err(WireError::InvalidLength("vnode bitmap"))
    );

    let data = decode_hex(DATA_GOLDEN_HEX);
    let mut short = data[..DATA_ENCODED_LEN - 1].to_vec();
    short[8..10].copy_from_slice(&((DATA_BODY_LEN - 1) as u16).to_be_bytes());
    assert_eq!(
        decode_data(&short),
        Err(WireError::InvalidLength("data body"))
    );
    let mut long = data;
    long.push(0);
    long[8..10].copy_from_slice(&((DATA_BODY_LEN + 1) as u16).to_be_bytes());
    assert_eq!(decode_data(&long), Err(WireError::LimitExceeded("body")));
}

#[test]
fn fixed_versions_authority_and_nonzero_fields_are_canonical() {
    let golden = decode_hex(FIRST_MARKER_GOLDEN_HEX);
    for (offset, length) in [
        (CURRENT_INTERVAL_OFFSET, 16),
        (DEPLOYMENT_UUID_OFFSET, 16),
        (PIPELINE_INCARNATION_OFFSET, 16),
        (PIPELINE_IDENTITY_SHA256_OFFSET, 32),
        (VNODE_COUNT_OFFSET, 2),
        (CURRENT_ASSIGNMENT_VERSION_OFFSET, 8),
        (CURRENT_ASSIGNMENT_SHA256_OFFSET, 32),
        (WRITER_NODE_ID_OFFSET, 8),
        (WRITER_BOOT_UUID_OFFSET, 16),
        (DURABLE_PROCESS_TERM_OFFSET, 8),
        (RECOVERY_EPOCH_OFFSET, 8),
        (RECOVERY_CHECKPOINT_ID_OFFSET, 8),
        (COMMITTED_INDEX_SHA256_OFFSET, 32),
        (RECOVERY_BASE_ASSIGNMENT_VERSION_OFFSET, 8),
        (RECOVERY_BASE_ASSIGNMENT_SHA256_OFFSET, 32),
        (TOPOLOGY_SHA256_OFFSET, 32),
    ] {
        let mut hostile = golden.clone();
        hostile[PREFIX_LEN + offset..PREFIX_LEN + offset + length].fill(0);
        assert!(
            decode_marker(&hostile).is_err(),
            "accepted zero field at {offset}"
        );
    }

    for (offset, expected) in [
        (PIPELINE_IDENTITY_VERSION_OFFSET, PIPELINE_IDENTITY_VERSION),
        (KEY_TO_VNODE_ABI_VERSION_OFFSET, KEY_TO_VNODE_ABI_VERSION),
        (
            SINK_PARTITIONING_ABI_VERSION_OFFSET,
            SINK_PARTITIONING_ABI_VERSION,
        ),
    ] {
        for observed in [0, 1, 2, 3, u16::MAX] {
            let mut hostile = golden.clone();
            hostile[PREFIX_LEN + offset..PREFIX_LEN + offset + 2]
                .copy_from_slice(&observed.to_be_bytes());
            if observed == expected {
                assert!(decode_marker(&hostile).is_ok());
            } else {
                assert!(matches!(
                    decode_marker(&hostile),
                    Err(WireError::UnsupportedFieldVersion { .. })
                ));
            }
        }
    }

    let mut mismatched_checkpoint = golden;
    mismatched_checkpoint[PREFIX_LEN + RECOVERY_CHECKPOINT_ID_OFFSET + 7] ^= 1;
    assert_eq!(
        decode_marker(&mismatched_checkpoint),
        Err(WireError::InvalidField("recovery_checkpoint_id"))
    );
}

#[test]
fn predecessor_flag_zero_and_self_consistency_are_enforced() {
    let first = decode_hex(FIRST_MARKER_GOLDEN_HEX);
    let successor = decode_hex(SUCCESSOR_MARKER_GOLDEN_HEX);

    let mut flag_without_id = first;
    flag_without_id[7] = 1;
    assert_eq!(
        decode_marker(&flag_without_id),
        Err(WireError::InvalidField("predecessor_interval_id"))
    );

    let mut id_without_flag = successor.clone();
    id_without_flag[7] = 0;
    assert_eq!(
        decode_marker(&id_without_flag),
        Err(WireError::InvalidField("predecessor flag"))
    );

    let mut self_predecessor = successor;
    let current = self_predecessor
        [PREFIX_LEN + CURRENT_INTERVAL_OFFSET..PREFIX_LEN + CURRENT_INTERVAL_OFFSET + 16]
        .to_vec();
    self_predecessor
        [PREFIX_LEN + PREDECESSOR_INTERVAL_OFFSET..PREFIX_LEN + PREDECESSOR_INTERVAL_OFFSET + 16]
        .copy_from_slice(&current);
    assert_eq!(
        decode_marker(&self_predecessor),
        Err(WireError::InvalidField("predecessor_interval_id"))
    );

    let mut input_self = sample_marker(true);
    input_self.current_interval_id = &FIRST_INTERVAL;
    assert_eq!(
        encoded_marker_len(&input_self),
        Err(WireError::InvalidField("predecessor_interval_id"))
    );
}

#[test]
fn string_caps_utf8_nul_and_byte_identity_are_enforced() {
    let sink = "é".repeat(64);
    let operator = format!("{}aa", "€".repeat(42));
    let output = "🦀".repeat(32);
    let shard = "🦀".repeat(16);
    assert_eq!(
        (sink.len(), operator.len(), output.len(), shard.len()),
        (128, 128, 128, 64)
    );
    let mut marker = sample_marker(false);
    marker.sink_id = &sink;
    marker.operator_id = &operator;
    marker.output_id = &output;
    marker.shard_id = &shard;
    let mut encoded = Vec::new();
    encode_marker_into(&marker, &mut encoded).unwrap();
    let decoded = decode_marker(&encoded).unwrap();
    assert_eq!(
        (
            decoded.sink_id,
            decoded.operator_id,
            decoded.output_id,
            decoded.shard_id
        ),
        (
            sink.as_str(),
            operator.as_str(),
            output.as_str(),
            shard.as_str()
        )
    );

    for (field, oversized) in [
        (0, "s".repeat(MAX_SINK_ID_LEN + 1)),
        (1, "o".repeat(MAX_OPERATOR_ID_LEN + 1)),
        (2, "u".repeat(MAX_OUTPUT_ID_LEN + 1)),
        (3, "h".repeat(MAX_SHARD_ID_LEN + 1)),
    ] {
        let mut hostile = sample_marker(false);
        match field {
            0 => hostile.sink_id = &oversized,
            1 => hostile.operator_id = &oversized,
            2 => hostile.output_id = &oversized,
            _ => hostile.shard_id = &oversized,
        }
        assert!(matches!(
            encoded_marker_len(&hostile),
            Err(WireError::LimitExceeded(_))
        ));
    }

    for (field, value) in [(0, ""), (1, "bad\0id"), (2, ""), (3, "bad\0id")] {
        let mut hostile = sample_marker(false);
        match field {
            0 => hostile.sink_id = value,
            1 => hostile.operator_id = value,
            2 => hostile.output_id = value,
            _ => hostile.shard_id = value,
        }
        assert!(matches!(
            encoded_marker_len(&hostile),
            Err(WireError::InvalidField(_))
        ));
    }

    let golden = decode_hex(FIRST_MARKER_GOLDEN_HEX);
    for byte_offset in [307, 312, 315, 319] {
        let mut invalid_utf8 = golden.clone();
        invalid_utf8[byte_offset] = 0xff;
        assert!(matches!(
            decode_marker(&invalid_utf8),
            Err(WireError::InvalidUtf8(_))
        ));
        let mut nul = golden.clone();
        nul[byte_offset] = 0;
        assert!(matches!(
            decode_marker(&nul),
            Err(WireError::InvalidField(_))
        ));
    }
    for (length_offset, field) in [
        (306, "sink_id"),
        (311, "operator_id"),
        (314, "output_id"),
        (318, "shard_id"),
    ] {
        let mut empty = golden.clone();
        empty[length_offset] = 0;
        assert_eq!(decode_marker(&empty), Err(WireError::InvalidField(field)));
    }

    let mut cap_before_utf8 = encoded;
    cap_before_utf8[PREFIX_LEN + MARKER_FIXED_BODY_LEN] = 129;
    assert_eq!(
        decode_marker(&cap_before_utf8),
        Err(WireError::LimitExceeded("sink_id"))
    );

    let mut composed = sample_marker(false);
    composed.sink_id = "é";
    let mut decomposed = sample_marker(false);
    decomposed.sink_id = "e\u{301}";
    let mut composed_bytes = Vec::new();
    let mut decomposed_bytes = Vec::new();
    encode_marker_into(&composed, &mut composed_bytes).unwrap();
    encode_marker_into(&decomposed, &mut decomposed_bytes).unwrap();
    assert_ne!(
        composed_bytes, decomposed_bytes,
        "codec must not normalize identity bytes"
    );
}

#[test]
fn invalid_and_truncated_multibyte_utf8_is_rejected() {
    let golden = decode_hex(FIRST_MARKER_GOLDEN_HEX);
    for invalid in [
        &[0x80_u8][..],
        &[0xc0, 0x80][..],
        &[0xed, 0xa0, 0x80][..],
        &[0xf4, 0x90, 0x80, 0x80][..],
        &[0xe2, 0x82][..],
        &[0xf0, 0x9f, 0xa6][..],
    ] {
        let mut hostile = golden.clone();
        let start = PREFIX_LEN + MARKER_FIXED_BODY_LEN;
        let original_sink_end = start + 1 + usize::from(hostile[start]);
        hostile.splice(
            start..original_sink_end,
            std::iter::once(invalid.len() as u8).chain(invalid.iter().copied()),
        );
        let body_len = hostile.len() - PREFIX_LEN;
        hostile[8..10].copy_from_slice(&(body_len as u16).to_be_bytes());
        assert!(matches!(
            decode_marker(&hostile),
            Err(WireError::InvalidUtf8("sink_id"))
        ));
    }
}

#[test]
fn vnode_bitmap_boundaries_lsb_order_and_padding_are_canonical() {
    for (count, bitmap) in [
        (1_u16, vec![0x01]),
        (7, vec![0x7f]),
        (8, vec![0x81]),
        (9, vec![0x01, 0x01]),
        (u16::MAX, {
            let mut bitmap = vec![0xff; MAX_VNODE_BITMAP_LEN];
            *bitmap.last_mut().unwrap() = 0x7f;
            bitmap
        }),
    ] {
        let mut marker = sample_marker(false);
        marker.vnode_count = count;
        marker.vnode_bitmap = &bitmap;
        let mut encoded = Vec::new();
        encode_marker_into(&marker, &mut encoded).unwrap();
        let decoded = decode_marker(&encoded).unwrap();
        assert_eq!(decoded.vnode_count, count);
        assert!(decoded.owns_vnode(0));
        assert!(decoded.owns_vnode(count - 1));
        assert!(!decoded.owns_vnode(count));
    }

    for (count, bitmap) in [
        (1_u16, vec![0x02]),
        (7, vec![0x80]),
        (9, vec![0x01, 0x02]),
        (9, vec![0x00, 0x00]),
    ] {
        let mut marker = sample_marker(false);
        marker.vnode_count = count;
        marker.vnode_bitmap = &bitmap;
        assert!(encoded_marker_len(&marker).is_err());
    }
    let mut invalid_max_bitmap = vec![0xff; MAX_VNODE_BITMAP_LEN];
    *invalid_max_bitmap.last_mut().unwrap() = 0x80;
    let mut invalid_max = sample_marker(false);
    invalid_max.vnode_count = u16::MAX;
    invalid_max.vnode_bitmap = &invalid_max_bitmap;
    assert_eq!(
        encoded_marker_len(&invalid_max),
        Err(WireError::InvalidField("vnode bitmap padding"))
    );

    let sample_bytes = decode_hex(FIRST_MARKER_GOLDEN_HEX);
    let sample = decode_marker(&sample_bytes).unwrap();
    for vnode in 0..4 {
        assert!(sample.owns_vnode(vnode));
    }
    assert!(!sample.owns_vnode(4));

    let mut wrong_length = sample_marker(false);
    wrong_length.vnode_bitmap = &[0x0f, 0];
    assert_eq!(
        encoded_marker_len(&wrong_length),
        Err(WireError::InvalidLength("vnode bitmap"))
    );
}

#[test]
fn exact_maximum_marker_passes_without_reallocating_caller_buffer() {
    let sink = "s".repeat(MAX_SINK_ID_LEN);
    let operator = "o".repeat(MAX_OPERATOR_ID_LEN);
    let output = "u".repeat(MAX_OUTPUT_ID_LEN);
    let shard = "h".repeat(MAX_SHARD_ID_LEN);
    let mut bitmap = vec![0xff; MAX_VNODE_BITMAP_LEN];
    *bitmap.last_mut().unwrap() = 0x7f;
    let mut marker = sample_marker(false);
    marker.sink_id = &sink;
    marker.operator_id = &operator;
    marker.output_id = &output;
    marker.shard_id = &shard;
    marker.vnode_count = u16::MAX;
    marker.vnode_bitmap = &bitmap;

    assert_eq!(encoded_marker_len(&marker).unwrap(), MAX_MARKER_ENCODED_LEN);
    let mut encoded = Vec::with_capacity(MAX_MARKER_ENCODED_LEN);
    let pointer_before = encoded.as_ptr();
    let capacity_before = encoded.capacity();
    encode_marker_into(&marker, &mut encoded).unwrap();
    assert_eq!(encoded.len(), MAX_MARKER_ENCODED_LEN);
    assert_eq!(encoded.as_ptr(), pointer_before);
    assert_eq!(encoded.capacity(), capacity_before);
    assert_eq!(
        u16::from_be_bytes([encoded[8], encoded[9]]) as usize,
        MAX_MARKER_BODY_LEN
    );
    for (offset, oversized, field) in [
        (PREFIX_LEN + MARKER_FIXED_BODY_LEN, 129, "sink_id"),
        (
            PREFIX_LEN + MARKER_FIXED_BODY_LEN + 1 + MAX_SINK_ID_LEN,
            129,
            "operator_id",
        ),
        (
            PREFIX_LEN + MARKER_FIXED_BODY_LEN + 2 + MAX_SINK_ID_LEN + MAX_OPERATOR_ID_LEN,
            129,
            "output_id",
        ),
        (
            PREFIX_LEN
                + MARKER_FIXED_BODY_LEN
                + 3
                + MAX_SINK_ID_LEN
                + MAX_OPERATOR_ID_LEN
                + MAX_OUTPUT_ID_LEN,
            65,
            "shard_id",
        ),
    ] {
        let mut hostile = encoded.clone();
        hostile[offset] = oversized;
        assert_eq!(
            decode_marker(&hostile),
            Err(WireError::LimitExceeded(field))
        );
    }
    let decoded = decode_marker(&encoded).unwrap();
    assert!(decoded.owns_vnode(u16::MAX - 1));
    assert!(!decoded.owns_vnode(u16::MAX));

    let oversized_body = MAX_MARKER_BODY_LEN + 1;
    let mut hostile = vec![0_u8; PREFIX_LEN + oversized_body];
    hostile[..4].copy_from_slice(&MAGIC);
    hostile[4] = VERSION;
    hostile[5] = MARKER_KIND;
    hostile[8..10].copy_from_slice(&(oversized_body as u16).to_be_bytes());
    assert_eq!(
        decode_marker(&hostile),
        Err(WireError::LimitExceeded("body"))
    );
}

#[test]
fn maximum_data_sequence_is_codec_valid() {
    let header = DataHeaderRef {
        operation_id: &OPERATION_ID,
        writer_interval_id: &DATA_INTERVAL,
        admission_sequence: u64::MAX,
    };
    let encoded = encode_data(&header).unwrap();
    assert_eq!(decode_data(&encoded).unwrap(), header);

    let mut zero_operation_wire = encoded;
    zero_operation_wire[PREFIX_LEN..PREFIX_LEN + 32].fill(0);
    assert_eq!(
        decode_data(&zero_operation_wire),
        Err(WireError::InvalidField("operation_id"))
    );
    let mut zero_interval_wire = encoded;
    zero_interval_wire[PREFIX_LEN + 32..PREFIX_LEN + 48].fill(0);
    assert_eq!(
        decode_data(&zero_interval_wire),
        Err(WireError::InvalidField("writer_interval_id"))
    );

    let zero_operation = [0_u8; 32];
    let zero_interval = [0_u8; 16];
    assert_eq!(
        encode_data(&DataHeaderRef {
            operation_id: &zero_operation,
            ..header
        }),
        Err(WireError::InvalidField("operation_id"))
    );
    assert_eq!(
        encode_data(&DataHeaderRef {
            writer_interval_id: &zero_interval,
            ..header
        }),
        Err(WireError::InvalidField("writer_interval_id"))
    );
}

#[test]
fn maximum_data_header_batch_is_structurally_bounded_and_borrowed() {
    assert_eq!(data_header_batch_bytes(0).unwrap(), 0);
    assert_eq!(
        data_header_batch_bytes(MAX_DATA_HEADERS_PER_BATCH).unwrap(),
        MAX_DATA_HEADER_BYTES_PER_BATCH
    );
    assert_eq!(
        data_header_batch_bytes(MAX_DATA_HEADERS_PER_BATCH + 1),
        Err(WireError::LimitExceeded("data header batch count"))
    );

    let encoded: [u8; DATA_ENCODED_LEN] = decode_hex(DATA_GOLDEN_HEX).try_into().unwrap();
    let batch = vec![encoded; MAX_DATA_HEADERS_PER_BATCH];
    let batch_pointer = batch.as_ptr();
    let batch_capacity = batch.capacity();
    let headers = batch
        .iter()
        .map(<[u8; DATA_ENCODED_LEN]>::as_slice)
        .collect::<Vec<_>>();
    assert_eq!(
        validate_data_header_batch(&headers).unwrap(),
        MAX_DATA_HEADER_BYTES_PER_BATCH
    );
    assert_eq!(batch.as_ptr(), batch_pointer);
    assert_eq!(batch.capacity(), batch_capacity);

    let first = decode_data(headers[0]).unwrap();
    assert_borrowed_from(first.operation_id.as_ptr(), headers[0]);
    assert_borrowed_from(first.writer_interval_id.as_ptr(), headers[0]);
    let last = decode_data(headers[MAX_DATA_HEADERS_PER_BATCH - 1]).unwrap();
    assert_borrowed_from(
        last.operation_id.as_ptr(),
        headers[MAX_DATA_HEADERS_PER_BATCH - 1],
    );

    let mut over_limit = headers;
    over_limit.push(batch[0].as_slice());
    over_limit[0] = &[];
    assert_eq!(
        validate_data_header_batch(&over_limit),
        Err(WireError::LimitExceeded("data header batch count"))
    );

    let oversized_header = [0_u8; DATA_ENCODED_LEN + 1];
    let last = over_limit.len() - 1;
    over_limit.pop();
    over_limit[0] = batch[0].as_slice();
    over_limit[last - 1] = &oversized_header;
    assert_eq!(
        validate_data_header_batch(&over_limit),
        Err(WireError::LimitExceeded("data header batch bytes"))
    );

    assert_eq!(
        validate_data_header_batch(&[&batch[0][..DATA_ENCODED_LEN - 1]]),
        Err(WireError::InvalidLength("data header batch"))
    );
}

#[test]
fn deterministic_hostile_corpus_never_panics() {
    let data = decode_hex(DATA_GOLDEN_HEX);
    let marker = decode_hex(SUCCESSOR_MARKER_GOLDEN_HEX);

    for original in [&data, &marker] {
        for index in 0..original.len().min(PREFIX_LEN + MARKER_FIXED_BODY_LEN) {
            for bit in 0..8 {
                let mut hostile = original.clone();
                hostile[index] ^= 1 << bit;
                assert!(std::panic::catch_unwind(|| {
                    let _ = decode_data(&hostile);
                    let _ = decode_marker(&hostile);
                })
                .is_ok());
            }
        }
    }

    let mut state = 0x4c44_424f_cafe_f00d_u64;
    for sample in 0..512_usize {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let length = (state as usize ^ sample) % 512;
        let mut hostile = vec![0_u8; length];
        for byte in &mut hostile {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            *byte = (state >> 56) as u8;
        }
        assert!(std::panic::catch_unwind(|| {
            let _ = decode_data(&hostile);
            let _ = decode_marker(&hostile);
        })
        .is_ok());
    }
}

fn decode_hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| (nibble(pair[0]) << 4) | nibble(pair[1]))
        .collect()
}

fn nibble(value: u8) -> u8 {
    match value {
        b'0'..=b'9' => value - b'0',
        b'a'..=b'f' => value - b'a' + 10,
        _ => panic!("non-lowercase hexadecimal test literal"),
    }
}

fn assert_borrowed_from(pointer: *const u8, input: &[u8]) {
    let start = input.as_ptr() as usize;
    let end = start + input.len();
    assert!((start..end).contains(&(pointer as usize)));
}
