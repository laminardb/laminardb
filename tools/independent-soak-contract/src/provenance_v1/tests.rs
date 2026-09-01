use super::*;

const DEPLOYMENT: [u8; 16] = [1; 16];
const INCARNATION: [u8; 16] = [2; 16];
const PIPELINE: [u8; 32] = [3; 32];
const OTHER_PIPELINE: [u8; 32] = [4; 32];
const ASSIGNMENT_6: [u8; 32] = [
    0xa7, 0xf4, 0xc4, 0xc5, 0xe5, 0x1e, 0xe6, 0x60, 0xfe, 0xd6, 0xd7, 0x4c, 0xa7, 0x10, 0x57, 0x89,
    0x26, 0x4a, 0xce, 0x8b, 0x22, 0x19, 0x80, 0x72, 0x1d, 0xf6, 0xcc, 0x83, 0x6d, 0xa1, 0x30, 0x5d,
];
const ASSIGNMENT_7: [u8; 32] = [
    0x5d, 0x96, 0xe2, 0x1c, 0x70, 0x59, 0xa1, 0x06, 0x71, 0x71, 0x46, 0xb0, 0x15, 0x15, 0xe9, 0xb9,
    0x7a, 0xe0, 0x88, 0x2c, 0x69, 0x03, 0x3d, 0x10, 0x8b, 0xc8, 0x2d, 0x77, 0xaa, 0x00, 0x97, 0x2f,
];
const ASSIGNMENT_8: [u8; 32] = [
    0x47, 0x8e, 0xcb, 0x49, 0x73, 0xd7, 0xd5, 0x57, 0x6f, 0x7c, 0x88, 0x03, 0x15, 0x85, 0x76, 0xe3,
    0xd6, 0xb2, 0x6f, 0xb4, 0x0e, 0xbe, 0xfa, 0xf5, 0xe3, 0xbf, 0x5c, 0x4b, 0x8b, 0xd6, 0x35, 0x56,
];
const ASSIGNMENT_8_MIXED: [u8; 32] = [
    0x50, 0x9a, 0x4b, 0x03, 0xfd, 0x0c, 0x3c, 0x92, 0x29, 0x51, 0x5b, 0x75, 0xb5, 0xb6, 0xb6, 0x5a,
    0x3e, 0xa3, 0xc5, 0x73, 0x07, 0x16, 0x0b, 0x54, 0x8a, 0x83, 0x6d, 0xd9, 0x9f, 0x34, 0x96, 0xed,
];
const ASSIGNMENT_8_BOOT_B: [u8; 32] = [
    0x7a, 0x40, 0x48, 0x0c, 0xc6, 0x34, 0x39, 0x22, 0xb4, 0x27, 0x7f, 0xc7, 0x4e, 0x8f, 0x84, 0x49,
    0x37, 0xb1, 0xf1, 0x62, 0xea, 0x61, 0x46, 0x70, 0x2d, 0xc2, 0x61, 0xf7, 0x51, 0x55, 0x7c, 0x02,
];
const OWNER_MAP_DIGEST: [u8; 32] = [
    0x81, 0x88, 0x88, 0x2d, 0x83, 0x68, 0x66, 0x00, 0x55, 0xc5, 0x2a, 0xa9, 0x7d, 0xa7, 0x08, 0x27,
    0x41, 0x5c, 0x0e, 0x7a, 0x1f, 0x39, 0xc8, 0x9f, 0x53, 0x18, 0x54, 0xc2, 0x6f, 0x12, 0xec, 0x0c,
];
const COMMITTED_INDEX_DIGEST: [u8; 32] = [9; 32];
const TOPOLOGY: [u8; 32] = [10; 32];
const BOOT_A: [u8; 16] = [11; 16];
const BOOT_B: [u8; 16] = [12; 16];
const PREDECESSOR: [u8; 16] = [13; 16];
const INTERVAL: [u8; 16] = [14; 16];
const OPERATION: [u8; 32] = [15; 32];
const ZERO_16: [u8; 16] = [0; 16];
const ZERO_32: [u8; 32] = [0; 32];
const PLANNED: [u16; 2] = [0, 2];
const OWNERS_A: [VnodeOwnerRef<'static>; 4] = [
    VnodeOwnerRef {
        vnode: 0,
        node_id: 41,
        boot_uuid: &BOOT_A,
    },
    VnodeOwnerRef {
        vnode: 1,
        node_id: 41,
        boot_uuid: &BOOT_A,
    },
    VnodeOwnerRef {
        vnode: 2,
        node_id: 41,
        boot_uuid: &BOOT_A,
    },
    VnodeOwnerRef {
        vnode: 3,
        node_id: 41,
        boot_uuid: &BOOT_A,
    },
];
const PARTICIPANTS_A: [AssignmentParticipantRef<'static>; 1] = [AssignmentParticipantRef {
    node_id: 41,
    boot_uuid: &BOOT_A,
}];
const OWNERS_MIXED: [VnodeOwnerRef<'static>; 4] = [
    OWNERS_A[0],
    OWNERS_A[1],
    VnodeOwnerRef {
        vnode: 2,
        node_id: 42,
        boot_uuid: &BOOT_B,
    },
    OWNERS_A[3],
];
const PARTICIPANTS_MIXED: [AssignmentParticipantRef<'static>; 2] = [
    AssignmentParticipantRef {
        node_id: 41,
        boot_uuid: &BOOT_A,
    },
    AssignmentParticipantRef {
        node_id: 42,
        boot_uuid: &BOOT_B,
    },
];
const OWNERS_BOOT_B: [VnodeOwnerRef<'static>; 4] = [
    VnodeOwnerRef {
        vnode: 0,
        node_id: 41,
        boot_uuid: &BOOT_B,
    },
    VnodeOwnerRef {
        vnode: 1,
        node_id: 41,
        boot_uuid: &BOOT_B,
    },
    VnodeOwnerRef {
        vnode: 2,
        node_id: 41,
        boot_uuid: &BOOT_B,
    },
    VnodeOwnerRef {
        vnode: 3,
        node_id: 41,
        boot_uuid: &BOOT_B,
    },
];
const PARTICIPANTS_BOOT_B: [AssignmentParticipantRef<'static>; 1] = [AssignmentParticipantRef {
    node_id: 41,
    boot_uuid: &BOOT_B,
}];

fn decode_hex<const N: usize>(value: &str) -> [u8; N] {
    assert_eq!(value.len(), N * 2);
    let mut decoded = [0_u8; N];
    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let nibble = |byte: u8| match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            _ => panic!("invalid test hex"),
        };
        decoded[index] = (nibble(pair[0]) << 4) | nibble(pair[1]);
    }
    decoded
}

fn encode_hex(value: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn identity() -> PipelineIdentityRef<'static> {
    PipelineIdentityRef {
        deployment_uuid: &DEPLOYMENT,
        pipeline_incarnation_id: &INCARNATION,
        pipeline_identity_version: wire_v1::PIPELINE_IDENTITY_VERSION,
        pipeline_identity_sha256: &PIPELINE,
    }
}

fn current_process() -> ProcessLeaseView<'static> {
    ProcessLeaseView {
        node_id: 41,
        boot_uuid: &BOOT_A,
        durable_process_term: 51,
    }
}

fn assignment(
    version: u64,
    digest: &'static [u8; 32],
    owners: &'static [VnodeOwnerRef<'static>],
) -> AssignmentCertificateView<'static> {
    AssignmentCertificateView {
        version,
        certificate_sha256: digest,
        vnode_count: 4,
        owners,
        participants: &PARTICIPANTS_A,
    }
}

fn recovery() -> RecoveryCheckpointView<'static> {
    RecoveryCheckpointView {
        immutable: true,
        terminal: RecoveryTerminal::Commit,
        identity: identity(),
        epoch: 61,
        checkpoint_id: 61,
        committed_index_sha256: &COMMITTED_INDEX_DIGEST,
        base_assignment_version: 7,
        base_assignment_certificate_sha256: &ASSIGNMENT_7,
    }
}

fn valid_input() -> OutputMarkerInput<'static> {
    OutputMarkerInput {
        identity: identity(),
        current_assignment: assignment(8, &ASSIGNMENT_8, &OWNERS_A),
        current_process: Some(current_process()),
        recovery: recovery(),
        interval: WriterIntervalInput {
            current_interval_id: &INTERVAL,
            predecessor_interval_id: Some(&PREDECESSOR),
            claimed_writer: current_process(),
        },
        topology_sha256: &TOPOLOGY,
        sink_id: "sink-a",
        operator_id: "operator-grouped-aggregate-a",
        output_id: "output-a",
        shard_id: "shard-a",
        planned_vnodes: &PLANNED,
    }
}

fn prepare_error(input: OutputMarkerInput<'_>) -> ProvenanceError {
    let mut bitmap = [0_u8; 1];
    prepare_output_authority_v1(input, &mut bitmap).unwrap_err()
}

#[test]
fn literal_operation_preimage_and_digest_are_stable() {
    let deployment = decode_hex::<16>("00112233445566778899aabbccddeeff");
    let incarnation = decode_hex::<16>("102132435465768798a9bacbdcedfe0f");
    let pipeline = [0x33; 32];
    let key = decode_hex::<10>("02616c70686100000005");
    let context = GroupedCountSumOperationIdContextV1::new(
        &deployment,
        &incarnation,
        &pipeline,
        "sink-a",
        "operator-grouped-aggregate-a",
        "output-a",
    )
    .unwrap();

    let mut literal_preimage = Vec::new();
    literal_preimage.extend_from_slice(OPERATION_ID_DOMAIN);
    literal_preimage.extend_from_slice(&deployment);
    literal_preimage.extend_from_slice(&incarnation);
    literal_preimage.extend_from_slice(&5_u16.to_be_bytes());
    literal_preimage.extend_from_slice(&pipeline);
    for value in ["sink-a", "operator-grouped-aggregate-a", "output-a"] {
        literal_preimage.push(value.len() as u8);
        literal_preimage.extend_from_slice(value.as_bytes());
    }
    literal_preimage.extend_from_slice(&10_u32.to_be_bytes());
    literal_preimage.extend_from_slice(&key);
    literal_preimage.extend_from_slice(&2_u64.to_be_bytes());
    assert_eq!(
            encode_hex(&literal_preimage),
            "6c616d696e617264622f67726f757065642d636f756e742d73756d2f6f7065726174696f6e2d69642f76310000112233445566778899aabbccddeeff102132435465768798a9bacbdcedfe0f000533333333333333333333333333333333333333333333333333333333333333330673696e6b2d611c6f70657261746f722d67726f757065642d6167677265676174652d61086f75747075742d610000000a02616c706861000000050000000000000002"
        );
    assert_eq!(
        context.derive(&key, 2).unwrap(),
        decode_hex::<32>("ef552fad285c75205cf8744f3dc9d9fdf91b6d69897199028dd375797688fffb")
    );
    assert_eq!(
        context.derive(&key, 2).unwrap(),
        Sha256::digest(&literal_preimage).as_slice()
    );
}

#[test]
fn every_identity_axis_and_length_boundary_is_effective() {
    let base = GroupedCountSumOperationIdContextV1::new(
        &DEPLOYMENT,
        &INCARNATION,
        &PIPELINE,
        "sink-a",
        "operator-a",
        "output-a",
    )
    .unwrap();
    let base_id = base.derive(b"key", 1).unwrap();
    let changed_deployment = [21; 16];
    let changed_incarnation = [22; 16];
    let changed_pipeline = [23; 32];
    for changed in [
        GroupedCountSumOperationIdContextV1::new(
            &changed_deployment,
            &INCARNATION,
            &PIPELINE,
            "sink-a",
            "operator-a",
            "output-a",
        )
        .unwrap(),
        GroupedCountSumOperationIdContextV1::new(
            &DEPLOYMENT,
            &changed_incarnation,
            &PIPELINE,
            "sink-a",
            "operator-a",
            "output-a",
        )
        .unwrap(),
        GroupedCountSumOperationIdContextV1::new(
            &DEPLOYMENT,
            &INCARNATION,
            &changed_pipeline,
            "sink-a",
            "operator-a",
            "output-a",
        )
        .unwrap(),
        GroupedCountSumOperationIdContextV1::new(
            &DEPLOYMENT,
            &INCARNATION,
            &PIPELINE,
            "sink-b",
            "operator-a",
            "output-a",
        )
        .unwrap(),
        GroupedCountSumOperationIdContextV1::new(
            &DEPLOYMENT,
            &INCARNATION,
            &PIPELINE,
            "sink-a",
            "operator-b",
            "output-a",
        )
        .unwrap(),
        GroupedCountSumOperationIdContextV1::new(
            &DEPLOYMENT,
            &INCARNATION,
            &PIPELINE,
            "sink-a",
            "operator-a",
            "output-b",
        )
        .unwrap(),
    ] {
        assert_ne!(changed.derive(b"key", 1).unwrap(), base_id);
    }
    assert_ne!(base.derive(b"Key", 1).unwrap(), base_id);
    assert_ne!(base.derive(b"key", 2).unwrap(), base_id);

    let split_a = GroupedCountSumOperationIdContextV1::new(
        &DEPLOYMENT,
        &INCARNATION,
        &PIPELINE,
        "a",
        "bc",
        "d",
    )
    .unwrap();
    let split_b = GroupedCountSumOperationIdContextV1::new(
        &DEPLOYMENT,
        &INCARNATION,
        &PIPELINE,
        "ab",
        "c",
        "d",
    )
    .unwrap();
    assert_ne!(
        split_a.derive(b"key", 1).unwrap(),
        split_b.derive(b"key", 1).unwrap()
    );

    let maximum = "x".repeat(128);
    assert!(GroupedCountSumOperationIdContextV1::new(
        &DEPLOYMENT,
        &INCARNATION,
        &PIPELINE,
        &maximum,
        &maximum,
        &maximum,
    )
    .is_ok());
    let oversized = "x".repeat(129);
    assert_eq!(
        GroupedCountSumOperationIdContextV1::new(
            &DEPLOYMENT,
            &INCARNATION,
            &PIPELINE,
            &oversized,
            "operator",
            "output",
        )
        .unwrap_err(),
        ProvenanceError::TextLimitExceeded("sink_id")
    );
    assert_eq!(
        GroupedCountSumOperationIdContextV1::new(
            &DEPLOYMENT,
            &INCARNATION,
            &PIPELINE,
            "sink\0suffix",
            "operator",
            "output",
        )
        .unwrap_err(),
        ProvenanceError::InvalidText("sink_id")
    );
}

#[test]
fn empty_key_and_checked_count_boundaries_are_explicit() {
    let context = GroupedCountSumOperationIdContextV1::new(
        &DEPLOYMENT,
        &INCARNATION,
        &PIPELINE,
        "sink",
        "operator",
        "output",
    )
    .unwrap();
    assert!(context.derive(&[], 1).is_ok());
    assert!(context.derive(b"key", i64::MAX as u64).is_ok());
    assert_eq!(
        context.derive(b"key", 0),
        Err(ProvenanceError::InvalidCheckedCount(0))
    );
    assert_eq!(
        context.derive(b"key", i64::MAX as u64 + 1),
        Err(ProvenanceError::InvalidCheckedCount(i64::MAX as u64 + 1))
    );
}

#[test]
fn crash_replay_is_stable_recreate_rotates_and_batch_output_is_caller_owned() {
    let context = GroupedCountSumOperationIdContextV1::new(
        &DEPLOYMENT,
        &INCARNATION,
        &PIPELINE,
        "sink",
        "operator",
        "output",
    )
    .unwrap();
    let before_crash = context.derive(b"key", 7).unwrap();
    // Assignment, recovery attempt, process, interval, sequence, and payload are absent from
    // this API, so a replay after any of those changes uses the same semantic ID.
    assert_eq!(context.derive(b"key", 7).unwrap(), before_crash);

    let recreated_incarnation = [31; 16];
    let recreated = GroupedCountSumOperationIdContextV1::new(
        &DEPLOYMENT,
        &recreated_incarnation,
        &PIPELINE,
        "sink",
        "operator",
        "output",
    )
    .unwrap();
    assert_ne!(recreated.derive(b"key", 7).unwrap(), before_crash);

    let mut caller_output = vec![[0_u8; 32]; wire_v1::MAX_DATA_HEADERS_PER_BATCH];
    for (index, output) in caller_output.iter_mut().enumerate() {
        *output = context.derive(b"key", (index + 1) as u64).unwrap();
    }
    assert_eq!(caller_output[0], context.derive(b"key", 1).unwrap());
    assert_eq!(
        caller_output[wire_v1::MAX_DATA_HEADERS_PER_BATCH - 1],
        context
            .derive(b"key", wire_v1::MAX_DATA_HEADERS_PER_BATCH as u64)
            .unwrap()
    );
}

#[test]
fn authority_projects_full_certificate_current_process_and_intervals() {
    let mut bitmap = [0_u8; 1];
    let prepared = prepare_output_authority_v1(valid_input(), &mut bitmap).unwrap();
    let marker = prepared.marker_ref();
    assert_eq!(marker.current_interval_id, &INTERVAL);
    assert_eq!(marker.predecessor_interval_id, Some(&PREDECESSOR));
    assert_eq!(marker.deployment_uuid, &DEPLOYMENT);
    assert_eq!(marker.pipeline_incarnation_id, &INCARNATION);
    assert_eq!(
        marker.pipeline_identity_version,
        wire_v1::PIPELINE_IDENTITY_VERSION
    );
    assert_eq!(marker.pipeline_identity_sha256, &PIPELINE);
    assert_eq!(
        marker.key_to_vnode_abi_version,
        wire_v1::KEY_TO_VNODE_ABI_VERSION
    );
    assert_eq!(
        marker.sink_partitioning_abi_version,
        wire_v1::SINK_PARTITIONING_ABI_VERSION
    );
    assert_eq!(marker.vnode_count, 4);
    assert_eq!(marker.current_assignment_version, 8);
    assert_eq!(marker.current_assignment_sha256, &ASSIGNMENT_8);
    assert_eq!(marker.writer_node_id, 41);
    assert_eq!(marker.writer_boot_uuid, &BOOT_A);
    assert_eq!(marker.durable_process_term, 51);
    assert_eq!(marker.recovery_epoch, 61);
    assert_eq!(marker.recovery_checkpoint_id, 61);
    assert_eq!(marker.committed_index_sha256, &COMMITTED_INDEX_DIGEST);
    assert_eq!(marker.recovery_base_assignment_version, 7);
    assert_eq!(marker.recovery_base_assignment_sha256, &ASSIGNMENT_7);
    assert_eq!(marker.topology_sha256, &TOPOLOGY);
    assert_eq!(marker.sink_id, "sink-a");
    assert_eq!(marker.operator_id, "operator-grouped-aggregate-a");
    assert_eq!(marker.output_id, "output-a");
    assert_eq!(marker.shard_id, "shard-a");
    assert_eq!(marker.vnode_bitmap, &[0b0000_0101]);

    let mut encoded_marker = Vec::new();
    wire_v1::encode_marker_into(&marker, &mut encoded_marker).unwrap();
    let decoded_marker = wire_v1::decode_marker(&encoded_marker).unwrap();
    assert_eq!(decoded_marker.current_assignment_sha256, &ASSIGNMENT_8);
    assert_eq!(decoded_marker.durable_process_term, 51);
    assert_eq!(decoded_marker.vnode_bitmap, &[0b0000_0101]);

    let header = prepared.project_data_header(&OPERATION, u64::MAX).unwrap();
    assert_eq!(header.writer_interval_id, &INTERVAL);
    let encoded_data = wire_v1::encode_data(&header).unwrap();
    let decoded_data = wire_v1::decode_data(&encoded_data).unwrap();
    assert_eq!(decoded_data.operation_id, &OPERATION);
    assert_eq!(decoded_data.writer_interval_id, &INTERVAL);
    assert_eq!(decoded_data.admission_sequence, u64::MAX);
}

#[test]
fn recovery_and_assignment_authorities_remain_distinct() {
    let mut same_version = valid_input();
    same_version.current_assignment = assignment(7, &ASSIGNMENT_7, &OWNERS_A);
    let mut bitmap = [0_u8; 1];
    assert!(prepare_output_authority_v1(same_version, &mut bitmap).is_ok());

    let mut before_base = valid_input();
    before_base.current_assignment = assignment(6, &ASSIGNMENT_6, &OWNERS_A);
    assert_eq!(
        prepare_error(before_base),
        ProvenanceError::CurrentAssignmentBeforeRecoveryBase
    );

    let mut equal_version_wrong_certificate = valid_input();
    equal_version_wrong_certificate.current_assignment = assignment(7, &ASSIGNMENT_7, &OWNERS_A);
    equal_version_wrong_certificate
        .recovery
        .base_assignment_certificate_sha256 = &ASSIGNMENT_8;
    assert_eq!(
        prepare_error(equal_version_wrong_certificate),
        ProvenanceError::EqualAssignmentVersionDigestMismatch
    );

    for terminal in [RecoveryTerminal::Abort, RecoveryTerminal::Pending] {
        let mut input = valid_input();
        input.recovery.terminal = terminal;
        assert_eq!(prepare_error(input), ProvenanceError::RecoveryNotCommitted);
    }
    let mut mutable = valid_input();
    mutable.recovery.immutable = false;
    assert_eq!(
        prepare_error(mutable),
        ProvenanceError::RecoveryNotImmutable
    );

    let mut wrong_identity = valid_input();
    wrong_identity.recovery.identity.pipeline_identity_sha256 = &OTHER_PIPELINE;
    assert_eq!(
        prepare_error(wrong_identity),
        ProvenanceError::RecoveryIdentityMismatch
    );

    let mut wrong_attempt = valid_input();
    wrong_attempt.recovery.checkpoint_id = 62;
    assert_eq!(
        prepare_error(wrong_attempt),
        ProvenanceError::InvalidRecoveryAttempt
    );
}

#[test]
fn writer_process_and_exact_shard_ownership_fail_closed() {
    let mut missing = valid_input();
    missing.current_process = None;
    assert_eq!(
        prepare_error(missing),
        ProvenanceError::MissingCurrentProcess
    );

    let mut wrong_term = valid_input();
    wrong_term.interval.claimed_writer.durable_process_term = 52;
    assert_eq!(
        prepare_error(wrong_term),
        ProvenanceError::ClaimedWriterMismatch
    );

    let mut zero_term = valid_input();
    zero_term
        .current_process
        .as_mut()
        .unwrap()
        .durable_process_term = 0;
    assert_eq!(
        prepare_error(zero_term),
        ProvenanceError::ZeroField("durable_process_term")
    );

    let mut mixed = valid_input();
    mixed.current_assignment = AssignmentCertificateView {
        version: 8,
        certificate_sha256: &ASSIGNMENT_8_MIXED,
        vnode_count: 4,
        owners: &OWNERS_MIXED,
        participants: &PARTICIPANTS_MIXED,
    };
    assert_eq!(
        prepare_error(mixed),
        ProvenanceError::WriterDoesNotOwnPlannedVnode(2)
    );

    let mut boot_only = valid_input();
    boot_only.current_assignment = AssignmentCertificateView {
        version: 8,
        certificate_sha256: &ASSIGNMENT_8_BOOT_B,
        vnode_count: 4,
        owners: &OWNERS_BOOT_B,
        participants: &PARTICIPANTS_BOOT_B,
    };
    assert_eq!(
        prepare_error(boot_only),
        ProvenanceError::WriterDoesNotOwnPlannedVnode(0)
    );

    let mut bitmap = [];
    assert_eq!(
        prepare_output_authority_v1(valid_input(), &mut bitmap).unwrap_err(),
        ProvenanceError::InvalidVnodeBitmapLength {
            expected: 1,
            actual: 0,
        }
    );
}

#[test]
fn assignment_certificate_digest_and_participant_roster_are_source_shaped() {
    assert_eq!(
        assignment_certificate_digest(assignment(7, &ASSIGNMENT_7, &OWNERS_A)),
        ASSIGNMENT_7
    );
    assert_eq!(
        assignment_certificate_digest(assignment(8, &ASSIGNMENT_8, &OWNERS_A)),
        ASSIGNMENT_8
    );
    assert_ne!(ASSIGNMENT_8, OWNER_MAP_DIGEST);

    let inner_digest = AssignmentCertificateView {
        version: 8,
        certificate_sha256: &OWNER_MAP_DIGEST,
        vnode_count: 4,
        owners: &OWNERS_A,
        participants: &PARTICIPANTS_A,
    };
    assert_eq!(
        validate_assignment(inner_digest),
        Err(ProvenanceError::AssignmentCertificateDigestMismatch)
    );

    let stale_after_boot_rotation = AssignmentCertificateView {
        version: 8,
        certificate_sha256: &ASSIGNMENT_8,
        vnode_count: 4,
        owners: &OWNERS_BOOT_B,
        participants: &PARTICIPANTS_BOOT_B,
    };
    assert_eq!(
        validate_assignment(stale_after_boot_rotation),
        Err(ProvenanceError::AssignmentCertificateDigestMismatch)
    );

    let disagreeing_owner_boots = [OWNERS_A[0], OWNERS_A[1], OWNERS_BOOT_B[2], OWNERS_A[3]];
    let disagreement = AssignmentCertificateView {
        version: 8,
        certificate_sha256: &ASSIGNMENT_8,
        vnode_count: 4,
        owners: &disagreeing_owner_boots,
        participants: &PARTICIPANTS_A,
    };
    assert_eq!(
        validate_assignment(disagreement),
        Err(ProvenanceError::AssignmentOwnerBootMismatch)
    );

    for (participant_count, expected) in [
        (129_usize, Ok(())),
        (
            130_usize,
            Err(ProvenanceError::AssignmentParticipantsNotCanonical),
        ),
    ] {
        let boots = vec![[1_u8; 16]; participant_count];
        let participants = boots
            .iter()
            .enumerate()
            .map(|(index, boot_uuid)| AssignmentParticipantRef {
                node_id: (index + 1) as u64,
                boot_uuid,
            })
            .collect::<Vec<_>>();
        let owners = boots
            .iter()
            .enumerate()
            .map(|(index, boot_uuid)| VnodeOwnerRef {
                vnode: index as u16,
                node_id: (index + 1) as u64,
                boot_uuid,
            })
            .collect::<Vec<_>>();
        let untrusted_digest = [1_u8; 32];
        let draft = AssignmentCertificateView {
            version: 9,
            certificate_sha256: &untrusted_digest,
            vnode_count: participant_count as u16,
            owners: &owners,
            participants: &participants,
        };
        let digest = assignment_certificate_digest(draft);
        let view = AssignmentCertificateView {
            certificate_sha256: &digest,
            ..draft
        };
        assert_eq!(validate_assignment(view), expected);
    }
}

#[test]
fn malformed_roster_plan_and_interval_are_rejected() {
    let incomplete_owners = [OWNERS_A[0], OWNERS_A[1], OWNERS_A[2]];
    let mut incomplete = valid_input();
    incomplete.current_assignment.owners = &incomplete_owners;
    assert_eq!(
        prepare_error(incomplete),
        ProvenanceError::IncompleteAssignmentRoster
    );

    let noncanonical_owners = [OWNERS_A[1], OWNERS_A[0], OWNERS_A[2], OWNERS_A[3]];
    let mut noncanonical = valid_input();
    noncanonical.current_assignment.owners = &noncanonical_owners;
    assert_eq!(
        prepare_error(noncanonical),
        ProvenanceError::AssignmentRosterNotCanonical
    );

    for planned in [&[][..], &[2, 0][..], &[0, 0][..]] {
        let mut input = valid_input();
        input.planned_vnodes = planned;
        assert!(matches!(
            prepare_error(input),
            ProvenanceError::EmptyPlannedVnodes | ProvenanceError::NonCanonicalPlannedVnodes
        ));
    }
    let mut outside = valid_input();
    outside.planned_vnodes = &[0, 4];
    assert_eq!(
        prepare_error(outside),
        ProvenanceError::PlannedVnodeOutOfRange(4)
    );

    let mut self_predecessor = valid_input();
    self_predecessor.interval.predecessor_interval_id = Some(&INTERVAL);
    assert_eq!(
        prepare_error(self_predecessor),
        ProvenanceError::SelfPredecessorInterval
    );
    let mut zero_interval = valid_input();
    zero_interval.interval.current_interval_id = &ZERO_16;
    assert_eq!(
        prepare_error(zero_interval),
        ProvenanceError::ZeroField("current_interval_id")
    );

    let mut bitmap = [0_u8; 1];
    let prepared = prepare_output_authority_v1(valid_input(), &mut bitmap).unwrap();
    assert_eq!(
        prepared.project_data_header(&ZERO_32, 0),
        Err(ProvenanceError::ZeroField("operation_id"))
    );
}
