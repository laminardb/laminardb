use super::*;
use crate::provenance_v1::{
    prepare_output_authority_v1, AssignmentCertificateView, AssignmentParticipantRef,
    OutputMarkerInput, PipelineIdentityRef, ProcessLeaseView, RecoveryCheckpointView,
    RecoveryTerminal, VnodeOwnerRef, WriterIntervalInput,
};

const DEPLOYMENT: [u8; 16] = [1; 16];
const OTHER_DEPLOYMENT: [u8; 16] = [21; 16];
const INCARNATION: [u8; 16] = [2; 16];
const OTHER_INCARNATION: [u8; 16] = [22; 16];
const PIPELINE: [u8; 32] = [3; 32];
const ASSIGNMENT_7: [u8; 32] = [
    0x5d, 0x96, 0xe2, 0x1c, 0x70, 0x59, 0xa1, 0x06, 0x71, 0x71, 0x46, 0xb0, 0x15, 0x15, 0xe9, 0xb9,
    0x7a, 0xe0, 0x88, 0x2c, 0x69, 0x03, 0x3d, 0x10, 0x8b, 0xc8, 0x2d, 0x77, 0xaa, 0x00, 0x97, 0x2f,
];
const COMMITTED_INDEX_DIGEST: [u8; 32] = [9; 32];
const TOPOLOGY: [u8; 32] = [10; 32];
const BOOT: [u8; 16] = [11; 16];
const INTERVAL_A: [u8; 16] = [12; 16];
const INTERVAL_B: [u8; 16] = [13; 16];
const INTERVAL_C: [u8; 16] = [14; 16];
const OPERATION_A: [u8; 32] = [15; 32];
const OPERATION_B: [u8; 32] = [16; 32];
const ZERO_OPERATION: [u8; 32] = [0; 32];
const PLANNED_VNODES: [u16; 2] = [0, 2];
const PARTITIONS: [i32; 3] = [0, 2, 4];
const TWO_PARTITIONS: [i32; 2] = [0, 2];
const OWNERS: [VnodeOwnerRef<'static>; 4] = [
    VnodeOwnerRef {
        vnode: 0,
        node_id: 41,
        boot_uuid: &BOOT,
    },
    VnodeOwnerRef {
        vnode: 1,
        node_id: 41,
        boot_uuid: &BOOT,
    },
    VnodeOwnerRef {
        vnode: 2,
        node_id: 41,
        boot_uuid: &BOOT,
    },
    VnodeOwnerRef {
        vnode: 3,
        node_id: 41,
        boot_uuid: &BOOT,
    },
];
const PARTICIPANTS: [AssignmentParticipantRef<'static>; 1] = [AssignmentParticipantRef {
    node_id: 41,
    boot_uuid: &BOOT,
}];

fn prepared_authority<'a>(
    bitmap: &'a mut [u8; 1],
    current_interval: &'a [u8; 16],
    predecessor_interval: Option<&'a [u8; 16]>,
    deployment: &'a [u8; 16],
    incarnation: &'a [u8; 16],
    sink_id: &'a str,
    shard_id: &'a str,
) -> PreparedOutputAuthorityV1<'a> {
    let identity = PipelineIdentityRef {
        deployment_uuid: deployment,
        pipeline_incarnation_id: incarnation,
        pipeline_identity_version: wire_v1::PIPELINE_IDENTITY_VERSION,
        pipeline_identity_sha256: &PIPELINE,
    };
    let process = ProcessLeaseView {
        node_id: 41,
        boot_uuid: &BOOT,
        durable_process_term: 51,
    };
    prepare_output_authority_v1(
        OutputMarkerInput {
            identity,
            current_assignment: AssignmentCertificateView {
                version: 7,
                certificate_sha256: &ASSIGNMENT_7,
                vnode_count: 4,
                owners: &OWNERS,
                participants: &PARTICIPANTS,
            },
            current_process: Some(process),
            recovery: RecoveryCheckpointView {
                immutable: true,
                terminal: RecoveryTerminal::Commit,
                identity,
                epoch: 61,
                checkpoint_id: 61,
                committed_index_sha256: &COMMITTED_INDEX_DIGEST,
                base_assignment_version: 7,
                base_assignment_certificate_sha256: &ASSIGNMENT_7,
            },
            interval: WriterIntervalInput {
                current_interval_id: current_interval,
                predecessor_interval_id: predecessor_interval,
                claimed_writer: process,
            },
            topology_sha256: &TOPOLOGY,
            sink_id,
            operator_id: "aggregate-1",
            output_id: "grouped-count-sum",
            shard_id,
            planned_vnodes: &PLANNED_VNODES,
        },
        bitmap,
    )
    .unwrap()
}

fn roomy_limits() -> TransactionLimitsV1 {
    TransactionLimitsV1::new(16, 64 * 1024).unwrap()
}

fn ready_first<'a>(bitmap: &'a mut [u8; 1]) -> FakeTransactionalWriterV1<'a> {
    let authority = prepared_authority(
        bitmap,
        &INTERVAL_A,
        None,
        &DEPLOYMENT,
        &INCARNATION,
        "sink-a",
        "shard-0",
    );
    let mut writer =
        FakeTransactionalWriterV1::first(authority, &PARTITIONS, roomy_limits()).unwrap();
    assert!(writer.initialize(SimulatedOutcomeV1::Confirmed).unwrap());
    assert_eq!(
        writer
            .commit_marker(&[SimulatedAttemptV1::CommitConfirmed])
            .unwrap(),
        1
    );
    writer
}

#[test]
fn limits_and_chunk_planner_freeze_only_modeled_bytes() {
    assert_eq!(
        TransactionLimitsV1::new(0, 1),
        Err(TransactionModelError::InvalidLimit("max_records"))
    );
    assert_eq!(
        TransactionLimitsV1::new(wire_v1::MAX_DATA_HEADERS_PER_BATCH + 1, 1),
        Err(TransactionModelError::InvalidLimit("max_records"))
    );
    assert_eq!(
        TransactionLimitsV1::new(1, 0),
        Err(TransactionModelError::InvalidLimit("max_modeled_bytes"))
    );
    assert!(TransactionLimitsV1::new(wire_v1::MAX_DATA_HEADERS_PER_BATCH, 1).is_ok());

    let records = [
        DataRecordRefV1 {
            partition: 0,
            operation_id: &OPERATION_A,
            payload: b"a",
        },
        DataRecordRefV1 {
            partition: 2,
            operation_id: &OPERATION_B,
            payload: b"b",
        },
        DataRecordRefV1 {
            partition: 4,
            operation_id: &OPERATION_A,
            payload: b"c",
        },
    ];
    let count_limited = TransactionLimitsV1::new(2, 1_000).unwrap();
    assert_eq!(
        plan_data_chunks_v1(&records, count_limited).unwrap(),
        vec![0..2, 2..3]
    );
    let byte_limited = TransactionLimitsV1::new(3, 2 * (wire_v1::DATA_ENCODED_LEN + 1)).unwrap();
    assert_eq!(
        plan_data_chunks_v1(&records, byte_limited).unwrap(),
        vec![0..2, 2..3]
    );
    assert!(plan_data_chunks_v1(&[], byte_limited).unwrap().is_empty());

    let oversized = [DataRecordRefV1 {
        partition: 0,
        operation_id: &OPERATION_A,
        payload: b"ab",
    }];
    let one_byte_short = TransactionLimitsV1::new(1, wire_v1::DATA_ENCODED_LEN + 1).unwrap();
    assert_eq!(
        plan_data_chunks_v1(&oversized, one_byte_short),
        Err(TransactionModelError::LimitExceeded(
            "single record modeled bytes"
        ))
    );
    assert!(matches!(
        checked_modeled_data_bytes([usize::MAX]),
        Err(TransactionModelError::ArithmeticOverflow(_))
    ));
    assert!(matches!(
        checked_modeled_data_bytes([usize::MAX - wire_v1::DATA_ENCODED_LEN, 1]),
        Err(TransactionModelError::ArithmeticOverflow(_))
    ));
}

#[test]
fn marker_fanout_is_canonical_bounded_and_unsplittable() {
    let mut sizing_bitmap = [0_u8; 1];
    let sizing_authority = prepared_authority(
        &mut sizing_bitmap,
        &INTERVAL_A,
        None,
        &DEPLOYMENT,
        &INCARNATION,
        "sink-a",
        "shard-0",
    );
    let marker_len = wire_v1::encoded_marker_len(&sizing_authority.marker_ref()).unwrap();

    let mut exact_bitmap = [0_u8; 1];
    let exact_authority = prepared_authority(
        &mut exact_bitmap,
        &INTERVAL_A,
        None,
        &DEPLOYMENT,
        &INCARNATION,
        "sink-a",
        "shard-0",
    );
    assert!(FakeTransactionalWriterV1::first(
        exact_authority,
        &PARTITIONS,
        TransactionLimitsV1::new(PARTITIONS.len(), marker_len * PARTITIONS.len()).unwrap(),
    )
    .is_ok());

    let mut count_bitmap = [0_u8; 1];
    let count_authority = prepared_authority(
        &mut count_bitmap,
        &INTERVAL_A,
        None,
        &DEPLOYMENT,
        &INCARNATION,
        "sink-a",
        "shard-0",
    );
    assert_eq!(
        FakeTransactionalWriterV1::first(
            count_authority,
            &PARTITIONS,
            TransactionLimitsV1::new(PARTITIONS.len() - 1, marker_len * PARTITIONS.len()).unwrap(),
        )
        .unwrap_err(),
        TransactionModelError::LimitExceeded("record count")
    );

    let mut byte_bitmap = [0_u8; 1];
    let byte_authority = prepared_authority(
        &mut byte_bitmap,
        &INTERVAL_A,
        None,
        &DEPLOYMENT,
        &INCARNATION,
        "sink-a",
        "shard-0",
    );
    assert_eq!(
        FakeTransactionalWriterV1::first(
            byte_authority,
            &PARTITIONS,
            TransactionLimitsV1::new(PARTITIONS.len(), marker_len * PARTITIONS.len() - 1,).unwrap(),
        )
        .unwrap_err(),
        TransactionModelError::LimitExceeded("modeled bytes")
    );

    for partitions in [&[][..], &[0, 0][..], &[2, 0][..], &[-1, 0][..]] {
        let mut bitmap = [0_u8; 1];
        let authority = prepared_authority(
            &mut bitmap,
            &INTERVAL_A,
            None,
            &DEPLOYMENT,
            &INCARNATION,
            "sink-a",
            "shard-0",
        );
        assert_eq!(
            FakeTransactionalWriterV1::first(authority, partitions, roomy_limits()).unwrap_err(),
            TransactionModelError::InvalidPartitions
        );
    }
}

#[test]
fn first_marker_is_confirmed_before_any_data_opens() {
    let mut bitmap = [0_u8; 1];
    let authority = prepared_authority(
        &mut bitmap,
        &INTERVAL_A,
        None,
        &DEPLOYMENT,
        &INCARNATION,
        "sink-a",
        "shard-0",
    );
    let mut writer =
        FakeTransactionalWriterV1::first(authority, &PARTITIONS, roomy_limits()).unwrap();
    let record = [DataRecordRefV1 {
        partition: 0,
        operation_id: &OPERATION_A,
        payload: b"alpha",
    }];

    assert!(matches!(
        writer.commit_data_batch(&record, &[SimulatedAttemptV1::CommitConfirmed]),
        Err(TransactionModelError::InvalidState { .. })
    ));
    assert!(!writer
        .initialize(SimulatedOutcomeV1::DefinitelyRejected)
        .unwrap());
    assert_eq!(writer.state, WriterStateV1::Uninitialized);
    assert!(writer.initialize(SimulatedOutcomeV1::Confirmed).unwrap());
    assert!(matches!(
        writer.commit_data_batch(&record, &[SimulatedAttemptV1::CommitConfirmed]),
        Err(TransactionModelError::InvalidState { .. })
    ));

    let attempts = [
        SimulatedAttemptV1::ConfirmedAbortAt(FaultPointV1::Begin),
        SimulatedAttemptV1::ConfirmedAbortAt(FaultPointV1::Send),
        SimulatedAttemptV1::ConfirmedAbortAt(FaultPointV1::Commit),
        SimulatedAttemptV1::CommitConfirmed,
    ];
    assert_eq!(writer.commit_marker(&attempts).unwrap(), 4);
    assert_eq!(writer.state, WriterStateV1::DataOpen);
    let marker = writer.confirmed_marker().unwrap();
    assert_eq!(marker.partitions, &PARTITIONS);
    let decoded = wire_v1::decode_marker(marker.envelope).unwrap();
    assert_eq!(decoded.current_interval_id, &INTERVAL_A);
    assert_eq!(decoded.predecessor_interval_id, None);
    assert!(writer.confirmed_data().is_empty());
    assert_eq!(writer.next_sequence, Some(0));
}

#[test]
fn transaction_in_flight_phases_and_invalid_actions_are_real_states() {
    let mut bitmap = [0_u8; 1];
    let authority = prepared_authority(
        &mut bitmap,
        &INTERVAL_A,
        None,
        &DEPLOYMENT,
        &INCARNATION,
        "sink-a",
        "shard-0",
    );
    let mut writer =
        FakeTransactionalWriterV1::first(authority, &PARTITIONS, roomy_limits()).unwrap();
    writer.initialize(SimulatedOutcomeV1::Confirmed).unwrap();

    assert!(matches!(
        writer.begin_transaction(TransactionKindV1::Data, SimulatedOutcomeV1::Confirmed),
        Err(TransactionModelError::InvalidState { .. })
    ));
    writer
        .begin_transaction(TransactionKindV1::Marker, SimulatedOutcomeV1::Confirmed)
        .unwrap();
    assert_eq!(
        writer.state,
        WriterStateV1::TransactionInFlight {
            kind: TransactionKindV1::Marker,
            phase: TransactionPhaseV1::Begun,
            return_state: StableStateV1::MarkerPending,
        }
    );
    let before_invalid_commit = writer.state;
    assert!(matches!(
        writer.commit_transaction(SimulatedOutcomeV1::Confirmed),
        Err(TransactionModelError::InvalidState { .. })
    ));
    assert_eq!(writer.state, before_invalid_commit);
    writer.abort_transaction().unwrap();
    assert_eq!(writer.state, WriterStateV1::MarkerPending);

    writer
        .begin_transaction(TransactionKindV1::Marker, SimulatedOutcomeV1::Confirmed)
        .unwrap();
    writer
        .send_transaction(SimulatedOutcomeV1::Confirmed)
        .unwrap();
    assert_eq!(
        writer.state,
        WriterStateV1::TransactionInFlight {
            kind: TransactionKindV1::Marker,
            phase: TransactionPhaseV1::Staged,
            return_state: StableStateV1::MarkerPending,
        }
    );
    writer.abort_transaction().unwrap();
    assert_eq!(writer.state, WriterStateV1::MarkerPending);
    writer
        .commit_marker(&[SimulatedAttemptV1::CommitConfirmed])
        .unwrap();
    assert_eq!(writer.state, WriterStateV1::DataOpen);
    assert!(writer.confirmed_marker().is_ok());
}

#[test]
fn deterministic_data_aborts_retry_identical_headers_and_range() {
    let mut bitmap = [0_u8; 1];
    let mut writer = ready_first(&mut bitmap);
    let records = [
        DataRecordRefV1 {
            partition: 2,
            operation_id: &OPERATION_A,
            payload: b"alpha",
        },
        DataRecordRefV1 {
            partition: 0,
            operation_id: &OPERATION_A,
            payload: b"beta",
        },
        DataRecordRefV1 {
            partition: 4,
            operation_id: &OPERATION_B,
            payload: b"gamma",
        },
    ];
    let attempts = [
        SimulatedAttemptV1::ConfirmedAbortAt(FaultPointV1::Begin),
        SimulatedAttemptV1::ConfirmedAbortAt(FaultPointV1::Send),
        SimulatedAttemptV1::ConfirmedAbortAt(FaultPointV1::Commit),
        SimulatedAttemptV1::CommitConfirmed,
    ];
    let committed = writer.commit_data_batch(&records, &attempts).unwrap();
    assert_eq!(
        committed,
        CommittedBatchV1 {
            attempts: 4,
            first_sequence: 0,
            last_sequence: 2,
        }
    );
    assert_eq!(writer.next_sequence, Some(3));
    assert_eq!(writer.confirmed_data().len(), 1);
    let transaction = &writer.confirmed_data()[0];
    assert_eq!(transaction.records.len(), 3);
    assert_eq!(transaction.payload(0), Some(&b"alpha"[..]));
    assert_eq!(transaction.payload(1), Some(&b"beta"[..]));
    assert_eq!(transaction.payload(2), Some(&b"gamma"[..]));
    let mut header_refs = Vec::new();
    for (index, record) in transaction.records.iter().enumerate() {
        let decoded = wire_v1::decode_data(&record.header).unwrap();
        assert_eq!(decoded.writer_interval_id, &INTERVAL_A);
        assert_eq!(decoded.admission_sequence, index as u64);
        header_refs.push(record.header.as_slice());
    }
    assert_eq!(
        wire_v1::validate_data_header_batch(&header_refs).unwrap(),
        3 * wire_v1::DATA_ENCODED_LEN
    );
}

#[test]
fn ambiguous_initialize_marker_and_data_are_terminal_and_inert() {
    let mut initialize_bitmap = [0_u8; 1];
    let initialize_authority = prepared_authority(
        &mut initialize_bitmap,
        &INTERVAL_A,
        None,
        &DEPLOYMENT,
        &INCARNATION,
        "sink-a",
        "shard-0",
    );
    let mut initialize_writer =
        FakeTransactionalWriterV1::first(initialize_authority, &PARTITIONS, roomy_limits())
            .unwrap();
    assert_eq!(
        initialize_writer.initialize(SimulatedOutcomeV1::Ambiguous),
        Err(TransactionModelError::OutcomeUnknown(
            PoisonPointV1::Initialize
        ))
    );
    assert_eq!(
        initialize_writer.initialize(SimulatedOutcomeV1::Confirmed),
        Err(TransactionModelError::Poisoned(PoisonPointV1::Initialize))
    );

    for point in [
        FaultPointV1::Begin,
        FaultPointV1::Send,
        FaultPointV1::Commit,
    ] {
        let mut bitmap = [0_u8; 1];
        let authority = prepared_authority(
            &mut bitmap,
            &INTERVAL_A,
            None,
            &DEPLOYMENT,
            &INCARNATION,
            "sink-a",
            "shard-0",
        );
        let mut writer =
            FakeTransactionalWriterV1::first(authority, &PARTITIONS, roomy_limits()).unwrap();
        writer.initialize(SimulatedOutcomeV1::Confirmed).unwrap();
        assert!(matches!(
            writer.commit_marker(&[SimulatedAttemptV1::OutcomeUnknownAt(point)]),
            Err(TransactionModelError::OutcomeUnknown(_))
        ));
        assert!(matches!(writer.state, WriterStateV1::TerminalPoison { .. }));
        assert_eq!(
            writer.confirmed_marker(),
            Err(TransactionModelError::MarkerNotConfirmed)
        );
        assert!(matches!(
            writer.commit_marker(&[SimulatedAttemptV1::CommitConfirmed]),
            Err(TransactionModelError::Poisoned(_))
        ));
        assert!(matches!(
            writer.initialize(SimulatedOutcomeV1::Confirmed),
            Err(TransactionModelError::Poisoned(_))
        ));
    }

    for point in [
        FaultPointV1::Begin,
        FaultPointV1::Send,
        FaultPointV1::Commit,
    ] {
        let mut bitmap = [0_u8; 1];
        let mut writer = ready_first(&mut bitmap);
        let record = [DataRecordRefV1 {
            partition: 0,
            operation_id: &OPERATION_A,
            payload: b"alpha",
        }];
        assert!(matches!(
            writer.commit_data_batch(&record, &[SimulatedAttemptV1::OutcomeUnknownAt(point)]),
            Err(TransactionModelError::OutcomeUnknown(_))
        ));
        assert!(writer.confirmed_data().is_empty());
        assert_eq!(writer.next_sequence, Some(0));
        assert!(matches!(
            writer.commit_data_batch(&record, &[SimulatedAttemptV1::CommitConfirmed]),
            Err(TransactionModelError::Poisoned(_))
        ));
        assert!(matches!(
            writer.commit_marker(&[SimulatedAttemptV1::CommitConfirmed]),
            Err(TransactionModelError::Poisoned(_))
        ));
    }
}

#[test]
fn preflight_and_script_failures_have_no_state_or_sequence_effect() {
    let mut bitmap = [0_u8; 1];
    let mut writer = ready_first(&mut bitmap);
    let state = writer.state;
    let next = writer.next_sequence;

    assert_eq!(
        writer.commit_data_batch(&[], &[SimulatedAttemptV1::CommitConfirmed]),
        Err(TransactionModelError::EmptyTransaction)
    );
    let zero = [DataRecordRefV1 {
        partition: 0,
        operation_id: &ZERO_OPERATION,
        payload: b"zero",
    }];
    assert!(matches!(
        writer.commit_data_batch(&zero, &[SimulatedAttemptV1::CommitConfirmed]),
        Err(TransactionModelError::Provenance(_))
    ));
    let wrong_partition = [DataRecordRefV1 {
        partition: 1,
        operation_id: &OPERATION_A,
        payload: b"wrong",
    }];
    assert_eq!(
        writer.commit_data_batch(&wrong_partition, &[SimulatedAttemptV1::CommitConfirmed]),
        Err(TransactionModelError::UnknownPartition(1))
    );
    let valid = [DataRecordRefV1 {
        partition: 0,
        operation_id: &OPERATION_A,
        payload: b"valid",
    }];
    assert_eq!(
        writer.commit_data_batch(&valid, &[]),
        Err(TransactionModelError::EmptyAttemptScript)
    );
    assert_eq!(
        writer.commit_data_batch(
            &valid,
            &[SimulatedAttemptV1::ConfirmedAbortAt(FaultPointV1::Send)]
        ),
        Err(TransactionModelError::UnresolvedAttemptScript)
    );
    assert_eq!(
        writer.commit_data_batch(
            &valid,
            &[
                SimulatedAttemptV1::CommitConfirmed,
                SimulatedAttemptV1::OutcomeUnknownAt(FaultPointV1::Commit),
            ]
        ),
        Err(TransactionModelError::NonCanonicalAttemptScript)
    );
    let too_many = vec![valid[0]; roomy_limits().max_records + 1];
    assert_eq!(
        writer.commit_data_batch(&too_many, &[SimulatedAttemptV1::CommitConfirmed]),
        Err(TransactionModelError::LimitExceeded("record count"))
    );
    let oversized_payload = vec![0_u8; roomy_limits().max_modeled_bytes];
    let oversized = [DataRecordRefV1 {
        partition: 0,
        operation_id: &OPERATION_A,
        payload: &oversized_payload,
    }];
    assert_eq!(
        writer.commit_data_batch(&oversized, &[SimulatedAttemptV1::CommitConfirmed]),
        Err(TransactionModelError::LimitExceeded("modeled bytes"))
    );
    assert_eq!(writer.state, state);
    assert_eq!(writer.next_sequence, next);
    assert!(writer.confirmed_data().is_empty());
}

#[test]
fn sequence_maximum_is_usable_once_then_explicitly_exhausted() {
    let record = [DataRecordRefV1 {
        partition: 0,
        operation_id: &OPERATION_A,
        payload: b"max",
    }];
    let mut max_bitmap = [0_u8; 1];
    let mut max_writer = ready_first(&mut max_bitmap);
    max_writer.next_sequence = Some(u64::MAX);
    let committed = max_writer
        .commit_data_batch(&record, &[SimulatedAttemptV1::CommitConfirmed])
        .unwrap();
    assert_eq!(committed.first_sequence, u64::MAX);
    assert_eq!(committed.last_sequence, u64::MAX);
    assert_eq!(max_writer.next_sequence, None);
    assert_eq!(
        max_writer.commit_data_batch(&record, &[SimulatedAttemptV1::CommitConfirmed]),
        Err(TransactionModelError::SequenceExhausted)
    );
    assert_eq!(max_writer.confirmed_data().len(), 1);

    let two_records = [record[0], record[0]];
    let mut pair_bitmap = [0_u8; 1];
    let mut pair_writer = ready_first(&mut pair_bitmap);
    pair_writer.next_sequence = Some(u64::MAX - 1);
    let committed = pair_writer
        .commit_data_batch(&two_records, &[SimulatedAttemptV1::CommitConfirmed])
        .unwrap();
    assert_eq!(committed.first_sequence, u64::MAX - 1);
    assert_eq!(committed.last_sequence, u64::MAX);
    assert_eq!(pair_writer.next_sequence, None);

    let three_records = [record[0], record[0], record[0]];
    let mut overflow_bitmap = [0_u8; 1];
    let mut overflow_writer = ready_first(&mut overflow_bitmap);
    overflow_writer.next_sequence = Some(u64::MAX - 1);
    assert_eq!(
        overflow_writer.commit_data_batch(&three_records, &[SimulatedAttemptV1::CommitConfirmed]),
        Err(TransactionModelError::SequenceExhausted)
    );
    assert_eq!(overflow_writer.next_sequence, Some(u64::MAX - 1));
    assert!(overflow_writer.confirmed_data().is_empty());
    assert_eq!(overflow_writer.state, WriterStateV1::DataOpen);
}

#[test]
fn successor_replay_keeps_operation_bytes_but_rotates_interval_and_sequence() {
    let record = [DataRecordRefV1 {
        partition: 2,
        operation_id: &OPERATION_A,
        payload: b"stable-payload",
    }];
    let mut first_bitmap = [0_u8; 1];
    let mut first = ready_first(&mut first_bitmap);
    first
        .commit_data_batch(&record, &[SimulatedAttemptV1::CommitConfirmed])
        .unwrap();
    let predecessor = first.confirmed_interval().unwrap();

    let mut successor_bitmap = [0_u8; 1];
    let successor_authority = prepared_authority(
        &mut successor_bitmap,
        &INTERVAL_B,
        Some(&INTERVAL_A),
        &DEPLOYMENT,
        &INCARNATION,
        "sink-a",
        "shard-0",
    );
    let mut successor = FakeTransactionalWriterV1::successor(
        successor_authority,
        predecessor,
        &PARTITIONS,
        roomy_limits(),
    )
    .unwrap();
    successor.initialize(SimulatedOutcomeV1::Confirmed).unwrap();
    successor
        .commit_marker(&[SimulatedAttemptV1::CommitConfirmed])
        .unwrap();
    let marker = wire_v1::decode_marker(successor.confirmed_marker().unwrap().envelope).unwrap();
    assert_eq!(marker.current_interval_id, &INTERVAL_B);
    assert_eq!(marker.predecessor_interval_id, Some(&INTERVAL_A));
    let committed = successor
        .commit_data_batch(&record, &[SimulatedAttemptV1::CommitConfirmed])
        .unwrap();
    assert_eq!(committed.first_sequence, 0);

    let first_record = &first.confirmed_data()[0].records[0];
    let successor_record = &successor.confirmed_data()[0].records[0];
    let first_header = wire_v1::decode_data(&first_record.header).unwrap();
    let successor_header = wire_v1::decode_data(&successor_record.header).unwrap();
    assert_eq!(first_header.operation_id, successor_header.operation_id);
    assert_eq!(first_header.admission_sequence, 0);
    assert_eq!(successor_header.admission_sequence, 0);
    assert_ne!(
        first_header.writer_interval_id,
        successor_header.writer_interval_id
    );
    assert_eq!(
        first.confirmed_data()[0].payload(0),
        Some(&b"stable-payload"[..])
    );
    assert_eq!(
        successor.confirmed_data()[0].payload(0),
        Some(&b"stable-payload"[..])
    );
}

#[test]
fn successor_requires_confirmed_predecessor_and_exact_stable_scope() {
    let mut first_bitmap = [0_u8; 1];
    let first = ready_first(&mut first_bitmap);
    let predecessor = first.confirmed_interval().unwrap();

    let mut first_with_predecessor_bitmap = [0_u8; 1];
    let first_with_predecessor = prepared_authority(
        &mut first_with_predecessor_bitmap,
        &INTERVAL_B,
        Some(&INTERVAL_A),
        &DEPLOYMENT,
        &INCARNATION,
        "sink-a",
        "shard-0",
    );
    assert_eq!(
        FakeTransactionalWriterV1::first(first_with_predecessor, &PARTITIONS, roomy_limits())
            .unwrap_err(),
        TransactionModelError::FirstIntervalHasPredecessor
    );

    let mut wrong_predecessor_bitmap = [0_u8; 1];
    let wrong_predecessor = prepared_authority(
        &mut wrong_predecessor_bitmap,
        &INTERVAL_B,
        Some(&INTERVAL_C),
        &DEPLOYMENT,
        &INCARNATION,
        "sink-a",
        "shard-0",
    );
    assert_eq!(
        FakeTransactionalWriterV1::successor(
            wrong_predecessor,
            predecessor,
            &PARTITIONS,
            roomy_limits(),
        )
        .unwrap_err(),
        TransactionModelError::SuccessorPredecessorMismatch
    );

    fn assert_scope_mismatch(
        predecessor: ConfirmedIntervalV1<'_>,
        deployment: &'static [u8; 16],
        incarnation: &'static [u8; 16],
        sink_id: &'static str,
        shard_id: &'static str,
    ) {
        let mut bitmap = [0_u8; 1];
        let authority = prepared_authority(
            &mut bitmap,
            &INTERVAL_B,
            Some(&INTERVAL_A),
            deployment,
            incarnation,
            sink_id,
            shard_id,
        );
        assert_eq!(
            FakeTransactionalWriterV1::successor(
                authority,
                predecessor,
                &PARTITIONS,
                roomy_limits(),
            )
            .unwrap_err(),
            TransactionModelError::StableProducerScopeMismatch
        );
    }
    assert_scope_mismatch(
        predecessor,
        &OTHER_DEPLOYMENT,
        &INCARNATION,
        "sink-a",
        "shard-0",
    );
    assert_scope_mismatch(
        predecessor,
        &DEPLOYMENT,
        &OTHER_INCARNATION,
        "sink-a",
        "shard-0",
    );
    assert_scope_mismatch(predecessor, &DEPLOYMENT, &INCARNATION, "sink-b", "shard-0");
    assert_scope_mismatch(predecessor, &DEPLOYMENT, &INCARNATION, "sink-a", "shard-1");
}

#[test]
fn explicit_chunk_execution_preserves_confirmed_prefix_on_later_ambiguity() {
    let records = [
        DataRecordRefV1 {
            partition: 0,
            operation_id: &OPERATION_A,
            payload: b"a",
        },
        DataRecordRefV1 {
            partition: 2,
            operation_id: &OPERATION_B,
            payload: b"b",
        },
        DataRecordRefV1 {
            partition: 0,
            operation_id: &OPERATION_A,
            payload: b"c",
        },
    ];
    let limits = TransactionLimitsV1::new(2, 64 * 1024).unwrap();
    let ranges = plan_data_chunks_v1(&records, limits).unwrap();
    assert_eq!(ranges, vec![0..2, 2..3]);

    let mut bitmap = [0_u8; 1];
    let authority = prepared_authority(
        &mut bitmap,
        &INTERVAL_A,
        None,
        &DEPLOYMENT,
        &INCARNATION,
        "sink-a",
        "shard-0",
    );
    let mut writer = FakeTransactionalWriterV1::first(authority, &TWO_PARTITIONS, limits).unwrap();
    writer.initialize(SimulatedOutcomeV1::Confirmed).unwrap();
    writer
        .commit_marker(&[SimulatedAttemptV1::CommitConfirmed])
        .unwrap();
    writer
        .commit_data_batch(
            &records[ranges[0].clone()],
            &[SimulatedAttemptV1::CommitConfirmed],
        )
        .unwrap();
    assert_eq!(writer.confirmed_data().len(), 1);
    assert_eq!(writer.next_sequence, Some(2));

    assert!(matches!(
        writer.commit_data_batch(
            &records[ranges[1].clone()],
            &[SimulatedAttemptV1::OutcomeUnknownAt(FaultPointV1::Commit)]
        ),
        Err(TransactionModelError::OutcomeUnknown(_))
    ));
    assert_eq!(writer.confirmed_data().len(), 1);
    assert_eq!(writer.next_sequence, Some(2));
    assert!(matches!(writer.state, WriterStateV1::TerminalPoison { .. }));
}
