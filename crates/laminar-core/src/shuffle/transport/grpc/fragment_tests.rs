use super::*;
use crate::serialization::BatchStreamEncoder;

#[test]
fn received_envelope_retains_admission_through_consumer_fold() {
    let node = Arc::new(Semaphore::new(1));
    let peer = Arc::new(Semaphore::new(1));
    let reservation = Arc::new(InboundReservation {
        node: Arc::clone(&node).try_acquire_owned().unwrap(),
        peer: Arc::clone(&peer).try_acquire_owned().unwrap(),
        wire_bytes: 1,
    });
    let batch = RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()));
    let inbound = Inbound {
        peer: 7,
        msg: ShuffleMessage::checkpointed("stage".into(), 3, batch),
        budget: Some(reservation),
        fence: StreamFence {
            sender_node_id: 7,
            sender_incarnation: Uuid::from_u128(1),
            receiver_incarnation: Uuid::from_u128(2),
            stream_id: Uuid::from_u128(3),
            assignment_version: 4,
            assignment_certificate_digest: [4; 32],
            recovery_gen: 5,
        },
        assignment_digest: None,
        checkpoint_sequence: 0,
    };

    let received = inbound.into_received();
    assert_eq!(node.available_permits(), 0);
    assert_eq!(peer.available_permits(), 0);

    assert_eq!(received.peer(), 7);
    assert_eq!(received.checkpoint_sequence(), 0);
    assert!(matches!(received.message(), ShuffleMessage::Data { .. }));
    assert_eq!(node.available_permits(), 0);
    assert_eq!(peer.available_permits(), 0);
    drop(received);

    assert_eq!(node.available_permits(), 1);
    assert_eq!(peer.available_permits(), 1);
}

#[test]
fn batch_admission_releases_after_last_retaining_consumer() {
    let node = Arc::new(Semaphore::new(1));
    let peer = Arc::new(Semaphore::new(1));
    let received = ReceivedBatch {
        batch: RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty())),
        routed_vnodes: Arc::from([11]),
        reservation: Some(Arc::new(InboundReservation {
            node: Arc::clone(&node).try_acquire_owned().unwrap(),
            peer: Arc::clone(&peer).try_acquire_owned().unwrap(),
            wire_bytes: 1,
        })),
        peer: 7,
        sender_incarnation: Uuid::from_u128(1),
        receiver_incarnation: Uuid::from_u128(2),
        stream_id: Uuid::from_u128(3),
        assignment_version: 4,
        recovery_gen: 5,
        checkpoint_sequence: 0,
    };

    assert_eq!(received.routed_vnodes(), &[11]);

    let (batch, admission) = received.into_parts();
    let second_consumer = admission.clone();
    drop(batch);
    drop(admission);
    assert_eq!(node.available_permits(), 0);
    assert_eq!(peer.available_permits(), 0);

    drop(second_consumer);
    assert_eq!(node.available_permits(), 1);
    assert_eq!(peer.available_permits(), 1);
}

#[tokio::test]
async fn cancelled_scope_releases_blocked_inbound_budget() {
    let budget = InboundBudget::new(INBOUND_NODE_BUDGET_BYTES);
    let held_node = Arc::clone(&budget.node)
        .acquire_many_owned(u32::try_from(INBOUND_NODE_BUDGET_BYTES).expect("node budget fits u32"))
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    let mut reservation = Box::pin(budget.reserve_frame(9, 1, &cancel));

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut reservation)
            .await
            .is_err()
    );
    let peer_budget = Arc::clone(budget.peers.lock().get(&9).unwrap());
    assert!(peer_budget.available_permits() < INBOUND_PEER_BUDGET_BYTES);

    cancel.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), &mut reservation)
        .await
        .expect("cancelled inbound reservation remained blocked");
    let Err(error) = result else {
        panic!("cancelled inbound reservation was admitted");
    };

    assert_eq!(error.code(), tonic::Code::Cancelled);
    assert_eq!(peer_budget.available_permits(), INBOUND_PEER_BUDGET_BYTES);
    drop(held_node);
    assert_eq!(budget.node.available_permits(), INBOUND_NODE_BUDGET_BYTES);
}

#[tokio::test]
async fn cancelled_blocking_decode_retains_admission_until_worker_exits() {
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "value",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow_array::Int64Array::from(vec![7; 200_000]))],
    )
    .unwrap();
    let payload = crate::serialization::serialize_batch_stream(&batch).unwrap();
    assert!(payload.len() >= BLOCKING_IPC_THRESHOLD_BYTES);
    let budget = InboundBudget::new(INBOUND_NODE_BUDGET_BYTES);
    let reservation = budget
        .reserve_frame(9, payload.len(), &CancellationToken::new())
        .await
        .unwrap();
    let peer_budget = Arc::clone(budget.peers.lock().get(&9).unwrap());
    let admitted_node = budget.node.available_permits();
    let admitted_peer = peer_budget.available_permits();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let task = tokio::spawn(async move {
        decode_ipc_payload_isolated(payload, reservation, move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv();
        })
        .await
    });

    started_rx.await.unwrap();
    task.abort();
    assert_eq!(budget.node.available_permits(), admitted_node);
    assert_eq!(peer_budget.available_permits(), admitted_peer);
    release_tx.send(()).unwrap();
    let _ = task.await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while budget.node.available_permits() != INBOUND_NODE_BUDGET_BYTES
            || peer_budget.available_permits() != INBOUND_PEER_BUDGET_BYTES
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled decoder retained its admission after the worker exited");
}

#[tokio::test]
async fn aggregate_decoded_payload_expansion_is_rejected_and_releases_admission() {
    let budget = InboundBudget::new(INBOUND_NODE_BUDGET_BYTES);
    let reservation = budget
        .reserve_frame(9, 1, &CancellationToken::new())
        .await
        .unwrap();
    let peer_budget = Arc::clone(budget.peers.lock().get(&9).unwrap());
    let rows = crate::shuffle::ROUTE_MAX_BATCH_BYTES / (2 * std::mem::size_of::<i64>()) + 1024;
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "value",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let make_batch = || {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow_array::Int64Array::from(vec![0; rows]))],
        )
        .unwrap()
    };
    let batches = [make_batch(), make_batch()];
    assert!(
        batches
            .iter()
            .all(|batch| batch.get_array_memory_size() <= crate::shuffle::ROUTE_MAX_BATCH_BYTES),
        "each decoded batch must fit by itself"
    );

    let error = InboundBudget::validate_decoded(&batches).unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert!(error.message().contains("decoded shuffle payload"));

    drop(reservation);
    assert_eq!(budget.node.available_permits(), INBOUND_NODE_BUDGET_BYTES);
    assert_eq!(peer_budget.available_permits(), INBOUND_PEER_BUDGET_BYTES);
}

#[tokio::test]
async fn many_tiny_decoded_batches_coexist_under_measured_admission() {
    const HELD_BATCHES: usize = 256;

    let budget = InboundBudget::new(INBOUND_NODE_BUDGET_BYTES);
    let wire_bytes = 128;
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "value",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow_array::Int64Array::from(vec![1]))],
    )
    .unwrap();
    let decoded_bytes = InboundBudget::validate_decoded(std::slice::from_ref(&batch)).unwrap();
    let stage = "stage".to_string();
    let routed_vnodes = vec![1];
    let metadata_bytes = retained_batch_metadata_bytes(&stage, &routed_vnodes, &batch).unwrap();
    let retained_bytes = wire_bytes + decoded_bytes + metadata_bytes;
    let decode_reservation_bytes = wire_bytes
        + crate::shuffle::ROUTE_MAX_BATCH_BYTES
        + INBOUND_BATCH_METADATA_BYTES
        + MAX_WIRE_PAYLOAD_BYTES;
    let mut held = Vec::with_capacity(HELD_BATCHES);

    for count in 1..=HELD_BATCHES {
        assert!(budget.node.available_permits() >= decode_reservation_bytes);
        if let Some(peer_budget) = budget.peers.lock().get(&9) {
            assert!(peer_budget.available_permits() >= decode_reservation_bytes);
        }
        let mut reservation = budget
            .reserve_frame(9, wire_bytes, &CancellationToken::new())
            .await
            .unwrap();
        reservation
            .retain_decoded(decoded_bytes, metadata_bytes)
            .unwrap();
        held.push(Arc::new(reservation));

        let peer_budget = Arc::clone(budget.peers.lock().get(&9).unwrap());
        assert_eq!(
            budget.node.available_permits(),
            INBOUND_NODE_BUDGET_BYTES - count * retained_bytes
        );
        assert_eq!(
            peer_budget.available_permits(),
            INBOUND_PEER_BUDGET_BYTES - count * retained_bytes
        );
    }

    let peer_budget = Arc::clone(budget.peers.lock().get(&9).unwrap());
    drop(held);
    assert_eq!(budget.node.available_permits(), INBOUND_NODE_BUDGET_BYTES);
    assert_eq!(peer_budget.available_permits(), INBOUND_PEER_BUDGET_BYTES);
}

#[tokio::test]
async fn decoded_admission_releases_exact_excess_and_last_holder_releases_rest() {
    let budget = InboundBudget::new(INBOUND_NODE_BUDGET_BYTES);
    let wire_bytes = 2048;
    let mut reservation = budget
        .reserve_frame(11, wire_bytes, &CancellationToken::new())
        .await
        .unwrap();
    let peer_budget = Arc::clone(budget.peers.lock().get(&11).unwrap());
    let decode_reservation_bytes = wire_bytes
        + crate::shuffle::ROUTE_MAX_BATCH_BYTES
        + INBOUND_BATCH_METADATA_BYTES
        + MAX_WIRE_PAYLOAD_BYTES;
    assert_eq!(
        budget.node.available_permits(),
        INBOUND_NODE_BUDGET_BYTES - decode_reservation_bytes
    );
    assert_eq!(
        peer_budget.available_permits(),
        INBOUND_PEER_BUDGET_BYTES - decode_reservation_bytes
    );

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "value",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let decoded_bytes = InboundBudget::validate_decoded(std::slice::from_ref(&batch)).unwrap();
    let metadata_bytes = retained_batch_metadata_bytes(&"stage".to_string(), &[1], &batch).unwrap();
    reservation
        .retain_decoded(decoded_bytes, metadata_bytes)
        .unwrap();
    let retained_bytes = wire_bytes + decoded_bytes + metadata_bytes;
    assert_eq!(
        budget.node.available_permits(),
        INBOUND_NODE_BUDGET_BYTES - retained_bytes
    );
    assert_eq!(
        peer_budget.available_permits(),
        INBOUND_PEER_BUDGET_BYTES - retained_bytes
    );

    let reservation = Arc::new(reservation);
    let final_holder = Arc::clone(&reservation);
    drop(reservation);
    assert_eq!(
        budget.node.available_permits(),
        INBOUND_NODE_BUDGET_BYTES - retained_bytes
    );
    assert_eq!(
        peer_budget.available_permits(),
        INBOUND_PEER_BUDGET_BYTES - retained_bytes
    );
    drop(final_holder);
    assert_eq!(budget.node.available_permits(), INBOUND_NODE_BUDGET_BYTES);
    assert_eq!(peer_budget.available_permits(), INBOUND_PEER_BUDGET_BYTES);
}

#[test]
fn ipc_message_may_not_span_logical_shuffle_payloads() {
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "value",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let mut encoder = BatchStreamEncoder::new(&schema).unwrap();
    let payload = encoder.encode(&batch).unwrap();
    let truncated = payload[..payload.len() - 1].to_vec();

    let error = decode_ipc_payload(&mut BatchStreamDecoder::new(), truncated).unwrap_err();
    assert!(error.to_string().contains("Unexpected End of Stream"));
}

#[test]
fn one_logical_payload_rejects_multiple_complete_batches() {
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "value",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let batch = |value| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow_array::Int64Array::from(vec![value]))],
        )
        .unwrap()
    };
    let mut encoder = BatchStreamEncoder::new(&schema).unwrap();
    let mut payload = encoder.encode(&batch(1)).unwrap();
    payload.extend(encoder.encode(&batch(2)).unwrap());

    let error = decode_ipc_payload(&mut BatchStreamDecoder::new(), payload).unwrap_err();
    assert!(error.to_string().contains("expected exactly one"));
}

#[test]
fn logical_payload_rejects_zero_batches() {
    let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "value",
        arrow_schema::DataType::Int64,
        false,
    )]);
    let mut encoder = BatchStreamEncoder::new(&schema).unwrap();
    let payload = encoder.finish().unwrap();

    let error = decode_ipc_payload(&mut BatchStreamDecoder::new(), payload).unwrap_err();
    assert!(error.to_string().contains("expected exactly one"));
}

fn fragment(index: u32, count: u32, total: u32, payload: Vec<u8>) -> RoutedData {
    RoutedData {
        stage: if index == 0 {
            "stage".into()
        } else {
            Default::default()
        },
        routed_vnodes: (index == 0).then_some(vec![3]).unwrap_or_default(),
        arrow_ipc: payload.into(),
        recovery_gen: 4,
        seq: 12,
        fragment_index: index,
        fragment_count: count,
        total_payload_bytes: total,
    }
}

fn padded_ipc_payload(len: usize, fill: u8) -> Vec<u8> {
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "value",
            arrow_schema::DataType::Int64,
            false,
        )])),
        vec![Arc::new(arrow_array::Int64Array::from(vec![1]))],
    )
    .unwrap();
    let mut payload = crate::serialization::serialize_batch_stream(&batch).unwrap();
    assert!(payload.len() <= len);
    payload.resize(len, fill);
    payload
}

#[test]
fn ipc_schema_header_is_verified_before_arrow_decode() {
    let valid = padded_ipc_payload(1024, 0);
    assert!(validate_ipc_schema_header(&valid, valid.len()).is_ok());

    for len in 0..8 {
        assert!(validate_ipc_schema_header(&valid[..len], len).is_err());
    }

    let mut wrong_marker = valid.clone();
    wrong_marker[0] = 0;
    assert!(validate_ipc_schema_header(&wrong_marker, wrong_marker.len()).is_err());

    let mut zero_len = valid.clone();
    zero_len[4..8].copy_from_slice(&0u32.to_le_bytes());
    assert!(validate_ipc_schema_header(&zero_len, zero_len.len()).is_err());

    let mut over_limit = valid.clone();
    over_limit[4..8].copy_from_slice(
        &u32::try_from(MAX_SCHEMA_WIRE_BYTES + 1)
            .unwrap()
            .to_le_bytes(),
    );
    assert!(validate_ipc_schema_header(&over_limit, over_limit.len()).is_err());

    let schema_len = usize::try_from(u32::from_le_bytes(valid[4..8].try_into().unwrap())).unwrap();
    let truncated = &valid[..8 + schema_len - 1];
    assert!(validate_ipc_schema_header(truncated, valid.len()).is_err());

    let mut invalid_flatbuffer = vec![0xff; 4];
    invalid_flatbuffer.extend_from_slice(&8u32.to_le_bytes());
    invalid_flatbuffer.extend_from_slice(&[0; 8]);
    assert!(validate_ipc_schema_header(&invalid_flatbuffer, invalid_flatbuffer.len()).is_err());

    let record_batch = &valid[8 + schema_len..];
    assert!(record_batch.len() >= 8);
    assert!(validate_ipc_schema_header(record_batch, record_batch.len()).is_err());
}

fn reservation() -> InboundReservation {
    let node = Arc::new(Semaphore::new(1))
        .try_acquire_owned()
        .expect("test node permit");
    let peer = Arc::new(Semaphore::new(1))
        .try_acquire_owned()
        .expect("test peer permit");
    InboundReservation {
        node,
        peer,
        wire_bytes: 1,
    }
}

#[test]
fn fragments_reassemble_exactly_once_in_order() {
    let total = MAX_WIRE_PAYLOAD_BYTES + 3;
    let total_wire = u32::try_from(total).unwrap();
    let mut assembly = None;
    assert!(push_fragment(
        &mut assembly,
        &fragment(
            0,
            2,
            total_wire,
            padded_ipc_payload(MAX_WIRE_PAYLOAD_BYTES, 1),
        ),
        Some(reservation()),
    )
    .unwrap()
    .is_none());
    let complete = push_fragment(&mut assembly, &fragment(1, 2, total_wire, vec![2; 3]), None)
        .unwrap()
        .expect("logical frame completed");

    assert_eq!(complete.stage, "stage");
    assert_eq!(complete.routed_vnodes, vec![3]);
    assert_eq!(complete.seq, 12);
    assert_eq!(complete.arrow_ipc.len(), total);
    assert_eq!(complete.arrow_ipc[MAX_WIRE_PAYLOAD_BYTES], 2);
    assert!(assembly.is_none());
}

#[test]
fn route_metadata_must_be_nonempty_and_canonical() {
    let payload = padded_ipc_payload(1024, 1);
    let valid = fragment(0, 1, u32::try_from(payload.len()).unwrap(), payload);
    assert!(validate_fragment(&valid).is_ok());

    for routed_vnodes in [Vec::new(), vec![3, 3], vec![4, 3]] {
        let mut malformed = valid.clone();
        malformed.routed_vnodes = routed_vnodes;
        assert!(validate_fragment(&malformed).is_err());
    }
}

#[test]
fn malformed_or_interleaved_fragments_fail_closed() {
    let total = u32::try_from(MAX_WIRE_PAYLOAD_BYTES + 1).unwrap();

    let mut missing_zero = None;
    assert!(push_fragment(&mut missing_zero, &fragment(1, 2, total, vec![1]), None).is_err());

    let mut reordered = None;
    push_fragment(
        &mut reordered,
        &fragment(0, 2, total, padded_ipc_payload(MAX_WIRE_PAYLOAD_BYTES, 1)),
        Some(reservation()),
    )
    .unwrap();
    let mut wrong = fragment(1, 2, total, vec![2]);
    wrong.seq += 1;
    assert!(push_fragment(&mut reordered, &wrong, None).is_err());

    let excessive_count = u32::try_from(MAX_FRAGMENTS + 1).unwrap();
    let mut excessive = None;
    assert!(push_fragment(
        &mut excessive,
        &fragment(0, excessive_count, 1, vec![1]),
        None,
    )
    .is_err());

    let mut oversized = None;
    assert!(push_fragment(
        &mut oversized,
        &fragment(0, 2, total, vec![1; MAX_WIRE_PAYLOAD_BYTES + 1],),
        None,
    )
    .is_err());
}

#[tokio::test]
async fn concurrent_mid_fragment_streams_are_byte_bounded() {
    let budget = Arc::new(InboundBudget::new(INBOUND_NODE_BUDGET_BYTES));
    let wire_bytes = 3 * 1024 * 1024;
    let total = u32::try_from(wire_bytes).unwrap();
    let count = u32::try_from(wire_bytes / MAX_WIRE_PAYLOAD_BYTES).unwrap();
    let mut assemblies = Vec::new();
    for _ in 0..2 {
        let admitted = budget
            .reserve_frame(9, wire_bytes, &CancellationToken::new())
            .await
            .unwrap();
        let mut assembly = None;
        push_fragment(
            &mut assembly,
            &fragment(
                0,
                count,
                total,
                padded_ipc_payload(MAX_WIRE_PAYLOAD_BYTES, 1),
            ),
            Some(admitted),
        )
        .unwrap();
        assemblies.push(assembly);
    }

    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(20),
        budget.reserve_frame(9, wire_bytes, &CancellationToken::new()),
    )
    .await
    .is_err());

    assemblies.pop();
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        budget.reserve_frame(9, wire_bytes, &CancellationToken::new()),
    )
    .await
    .expect("released bytes unblock the peer")
    .unwrap();
}
