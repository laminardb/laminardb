use super::*;

const ACQUIRE: Ordering = Ordering::Acquire;

#[test]
fn only_exact_remote_scope_cancellation_is_typed() {
    let exact = status_io(scope_cancelled_status());
    assert!(is_scope_cancelled(&exact));

    let unrelated = status_io(tonic::Status::cancelled("shuffle caller stopped"));
    assert!(!is_scope_cancelled(&unrelated));
    assert!(!is_scope_cancelled(&process_lease_expired_io()));
}

fn live_process_lease() -> ProcessLeaseGate {
    let gate = ProcessLeaseGate::default();
    gate.install_live_for_test();
    gate
}

fn fence(sender: u128, receiver: u128, stream: u128, version: u64) -> StreamFence {
    StreamFence {
        sender_node_id: 7,
        sender_incarnation: Uuid::from_u128(sender),
        receiver_incarnation: Uuid::from_u128(receiver),
        stream_id: Uuid::from_u128(stream),
        assignment_version: version,
        assignment_certificate_digest: [1; 32],
        recovery_gen: 0,
    }
}

fn deliver(
    tracker: &DeliveryTracker,
    fence: &StreamFence,
    seq: u64,
) -> Result<bool, tonic::Status> {
    let Some(reservation) = tracker.prepare_data(fence, seq)? else {
        return Ok(false);
    };
    tracker.commit_data(reservation)?;
    Ok(true)
}

#[test]
fn ordered_delivery_suppresses_duplicates_and_reports_gaps() {
    let tracker = DeliveryTracker::default();
    let stream = fence(1, 10, 100, 1);
    tracker.observe_hello(stream).unwrap();

    assert!(deliver(&tracker, &stream, 0).unwrap());
    assert!(!deliver(&tracker, &stream, 0).unwrap());
    assert!(deliver(&tracker, &stream, 2).unwrap());
    assert_eq!(tracker.delivery_loss_incidents.load(ACQUIRE), 1);
}

fn admitted_test_budget() -> InboundReservation {
    let bytes = crate::shuffle::message::MAX_PAYLOAD_BYTES;
    let permits = u32::try_from(bytes).unwrap();
    InboundReservation {
        node: Arc::new(Semaphore::new(bytes))
            .try_acquire_many_owned(permits)
            .unwrap(),
        peer: Arc::new(Semaphore::new(bytes))
            .try_acquire_many_owned(permits)
            .unwrap(),
        wire_bytes: 1,
    }
}

/// A sender process cannot reset its sequence inside the same assignment/recovery scope.
#[test]
fn sender_scope_replacement_rejection_does_not_infer_a_missing_frame() {
    let d = DeliveryTracker::default();
    let first = fence(1, 10, 100, 1);
    d.observe_hello(first).unwrap();
    assert!(deliver(&d, &first, 0).unwrap());
    let restarted = fence(2, 10, 200, 1);
    let error = d.observe_hello(restarted).unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 0);
}

#[test]
fn handshake_token_is_both_single_use_and_age_bounded_at_consumption() {
    let stream = fence(1, 10, 100, 1);
    let pending = PendingHandshakes::default();
    let now = std::time::Instant::now();
    pending.0.lock().insert(
        stream.sender_node_id,
        PendingHandshake {
            fence: stream,
            issued_at: now.checked_sub(HANDSHAKE_TOKEN_TTL).unwrap(),
        },
    );

    assert!(!consume_handshake_token(&pending, &stream, now));
    assert!(
        pending.0.lock().is_empty(),
        "expired token must be consumed"
    );

    pending.0.lock().insert(
        stream.sender_node_id,
        PendingHandshake {
            fence: stream,
            issued_at: now,
        },
    );
    assert!(consume_handshake_token(&pending, &stream, now));
    assert!(!consume_handshake_token(&pending, &stream, now));
}

#[tokio::test]
async fn active_stream_replacement_is_per_peer() {
    let registry = Arc::new(ActiveStreamRegistry::default());
    let parent = CancellationToken::new();
    let permits = Arc::new(Semaphore::new(1));
    let original = fence(1, 10, 100, 1);
    let mut original_lease = registry.replace(&original, &parent);
    original_lease.acquire_permit(&permits).await.unwrap();
    assert!(!original_lease.cancel.is_cancelled());
    assert_eq!(registry.streams.lock().len(), 1);

    let replacement = StreamFence {
        stream_id: Uuid::from_u128(101),
        ..original
    };
    let replacement_lease = registry.replace(&replacement, &parent);
    assert!(original_lease.cancel.is_cancelled());
    drop(original_lease);
    assert_eq!(registry.streams.lock().len(), 1);
    drop(replacement_lease);
    assert!(registry.streams.lock().is_empty());
}

#[test]
fn assignment_advance_replaces_old_sender_scope_at_sequence_zero() {
    let d = DeliveryTracker::default();
    let first = fence(1, 10, 100, 1);
    d.observe_hello(first).unwrap();
    assert!(deliver(&d, &first, 0).unwrap());

    let next_assignment = fence(2, 10, 200, 2);
    d.observe_hello(next_assignment).unwrap();

    assert!(deliver(&d, &next_assignment, 0).unwrap());
    assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 0);
}

#[test]
fn assignment_and_recovery_scope_changes_clear_tracked_ingress_peers() {
    let d = DeliveryTracker::default();
    for peer in 1..=256 {
        let mut stream = fence(peer, 10, peer + 1_000, 1);
        stream.sender_node_id = u64::try_from(peer).unwrap();
        d.ingress_lock(stream.sender_node_id).unwrap();
        d.observe_hello(stream).unwrap();
    }
    assert_eq!(d.ingress.lock().len(), 256);
    assert_eq!(d.peers.lock().len(), 256);

    d.reset_assignment();
    assert!(d.ingress.lock().is_empty());
    assert!(d.peers.lock().is_empty());

    d.ingress_lock(7).unwrap();
    d.prepare_recovery(1);
    assert!(d.ingress.lock().is_empty());
}

/// A reconnect of the same incarnation keeps its expectation so a discarded outbound
/// queue remains detectable.
#[test]
fn same_incarnation_reconnect_keeps_expectation() {
    let d = DeliveryTracker::default();
    let first = fence(1, 10, 100, 1);
    d.observe_hello(first).unwrap();
    assert!(deliver(&d, &first, 0).unwrap());
    let reconnect = fence(1, 10, 101, 1);
    d.observe_hello(reconnect).unwrap();
    assert!(deliver(&d, &reconnect, 2).unwrap()); // frame 1 died with the queue
    assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);
}

/// Recovery resets to sequence zero; a missing first frame is visible at the barrier.
#[test]
fn recovery_does_not_rebaseline_past_missing_sequence_zero() {
    let d = DeliveryTracker::default();
    let stream = fence(1, 10, 100, 1);
    d.observe_hello(stream).unwrap();
    assert!(deliver(&d, &stream, 0).unwrap());
    let mut rewound = fence(1, 10, 101, 1);
    rewound.recovery_gen = 1;
    d.observe_hello(rewound).unwrap();
    let barrier = d.prepare_barrier(&rewound, 1).unwrap();
    d.commit_barrier(barrier).unwrap();
    assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);
}

#[test]
fn cancelled_pre_enqueue_admission_is_recorded_once() {
    let d = DeliveryTracker::default();
    let stream = fence(1, 10, 100, 1);
    let cancel = CancellationToken::new();
    d.observe_hello(stream).unwrap();
    let cancelled = DataAdmission::new(&d, d.prepare_data(&stream, 0).unwrap().unwrap(), &cancel);
    drop(cancelled);
    assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);

    let barrier = d.prepare_barrier(&stream, 1).unwrap();
    d.commit_barrier(barrier).unwrap();

    assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);
}

#[test]
fn scope_rotation_does_not_report_transport_loss() {
    let d = DeliveryTracker::default();
    let stream = fence(1, 10, 100, 1);
    let cancel = CancellationToken::new();
    d.observe_hello(stream).unwrap();
    let admission = DataAdmission::new(&d, d.prepare_data(&stream, 0).unwrap().unwrap(), &cancel);

    cancel.cancel();
    drop(admission);

    assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 0);
}

#[test]
fn enqueued_frame_commits_expectation_after_scope_cancellation() {
    let d = DeliveryTracker::default();
    let stream = fence(1, 10, 100, 1);
    let cancel = CancellationToken::new();
    d.observe_hello(stream).unwrap();
    let admission = DataAdmission::new(&d, d.prepare_data(&stream, 0).unwrap().unwrap(), &cancel);

    cancel.cancel();
    admission.commit_after_enqueue().unwrap();
    let barrier = d.prepare_barrier(&stream, 1).unwrap();
    d.commit_barrier(barrier).unwrap();

    assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 0);
}

#[test]
fn exhausted_cancelled_admission_fails_closed_without_overflow() {
    let d = DeliveryTracker::default();
    let stream = fence(1, 10, 100, 1);
    let cancel = CancellationToken::new();
    d.observe_hello(stream).unwrap();
    let exhausted = DataAdmission::new(
        &d,
        DataReservation {
            fence: stream,
            seq: u64::MAX,
            expected: 0,
        },
        &cancel,
    );

    drop(exhausted);

    assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);
}

#[tokio::test]
async fn barrier_loss_is_visible_before_the_barrier_can_be_dequeued() {
    let d = DeliveryTracker::default();
    let stream = fence(1, 10, 100, 1);
    d.observe_hello(stream).unwrap();
    let assignment = AtomicU64::new(stream.assignment_version);
    let recovery = AtomicU64::new(stream.recovery_gen);
    let barrier_arrivals = AtomicU64::new(0);
    let cancel = CancellationToken::new();
    let process_lease = live_process_lease();
    let (tx, rx) = mpsc::bounded_async::<Inbound>(1);
    let work_ready = tokio::sync::Notify::new();
    let holdover = Holdover::new(1);
    let publish = publish_barrier(
        &tx,
        &work_ready,
        &barrier_arrivals,
        &holdover,
        &assignment,
        &recovery,
        &d,
        stream,
        CheckpointBarrier::new(1, 1),
        [1; 32],
        1,
        &cancel,
        &process_lease,
    );
    let consume = async {
        let received = rx.recv().await.unwrap();
        assert!(matches!(received.msg, ShuffleMessage::Barrier(_)));
        d.delivery_loss_incidents.load(ACQUIRE)
    };

    let (published, loss_at_visibility) = tokio::join!(publish, consume);

    assert!(published.unwrap());
    assert_eq!(loss_at_visibility, 1);
    assert_eq!(barrier_arrivals.load(ACQUIRE), 1);
}

#[tokio::test]
async fn transport_rejects_retired_checkpoint_id_with_a_different_assignment() {
    let d = DeliveryTracker::default();
    let mut stream = fence(1, 10, 100, 2);
    stream.assignment_certificate_digest = [2; 32];
    d.observe_hello(stream).unwrap();
    let assignment = AtomicU64::new(stream.assignment_version);
    let recovery = AtomicU64::new(stream.recovery_gen);
    let barrier_arrivals = AtomicU64::new(0);
    let cancel = CancellationToken::new();
    let process_lease = live_process_lease();
    let (tx, rx) = mpsc::bounded_async::<Inbound>(1);
    let work_ready = tokio::sync::Notify::new();
    let holdover = Holdover::new(1);
    holdover
        .retire_checkpoint_attempt(CheckpointAttempt::canonical(7), [1; 32])
        .unwrap();

    let error = publish_barrier(
        &tx,
        &work_ready,
        &barrier_arrivals,
        &holdover,
        &assignment,
        &recovery,
        &d,
        stream,
        CheckpointBarrier::new(7, 7),
        [2; 32],
        0,
        &cancel,
        &process_lease,
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("different assignment digest"));
    assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);
    assert_eq!(barrier_arrivals.load(ACQUIRE), 0);
    assert!(rx.try_recv().is_err());
}

#[test]
fn backward_barrier_high_water_is_a_protocol_fault() {
    let d = DeliveryTracker::default();
    let stream = fence(1, 10, 100, 1);
    d.observe_hello(stream).unwrap();
    assert!(deliver(&d, &stream, 0).unwrap());

    let error = d.prepare_barrier(&stream, 0).unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);
}

#[test]
fn maximum_barrier_high_water_records_one_nonwrapping_incident() {
    let d = DeliveryTracker::default();
    let stream = fence(1, 10, 100, 1);
    d.observe_hello(stream).unwrap();

    let barrier = d.prepare_barrier(&stream, u64::MAX).unwrap();
    d.commit_barrier(barrier).unwrap();
    assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);

    d.note_loss(stream.sender_node_id, 1, "test-successor");
    assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 2);
}

#[test]
fn exhausted_loss_counter_is_permanently_fail_closed() {
    let d = DeliveryTracker::default();
    d.delivery_loss_incidents
        .store(u64::MAX - 1, Ordering::Release);

    d.note_loss(7, 1, "test-exhaustion");
    d.note_loss(7, 1, "test-after-exhaustion");
    assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), u64::MAX);

    d.prepare_recovery(1);
    assert!(d.complete_recovery(1));
    assert_eq!(
        d.recovered_delivery_loss_incidents.load(ACQUIRE),
        u64::MAX - 1
    );
    assert!(
        d.delivery_loss_incidents.load(ACQUIRE) > d.recovered_delivery_loss_incidents.load(ACQUIRE)
    );
}

#[test]
fn successor_hello_invalidates_predecessor_reservation_before_commit() {
    let d = DeliveryTracker::default();
    let predecessor = fence(1, 10, 100, 1);
    d.observe_hello(predecessor).unwrap();
    let reserved = d.prepare_data(&predecessor, 0).unwrap().unwrap();
    let successor = fence(1, 10, 101, 1);
    d.observe_hello(successor).unwrap();

    assert!(d.commit_data(reserved).is_err());
    assert!(deliver(&d, &successor, 0).unwrap());
}

#[tokio::test]
async fn coalesced_multi_vnode_frame_has_one_atomic_queue_admission() {
    use arrow_array::{Int64Array, UInt32Array};
    use arrow_schema::{DataType, Field, Schema};

    let d = DeliveryTracker::default();
    let stream = fence(1, 10, 100, 1);
    let cancel = CancellationToken::new();
    let process_lease = live_process_lease();
    d.observe_hello(stream).unwrap();
    let admission = DataAdmission::new(&d, d.prepare_data(&stream, 0).unwrap().unwrap(), &cancel);

    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new("__laminar_vnode", DataType::UInt32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(UInt32Array::from(vec![0, 1])),
        ],
    )
    .unwrap();
    let decoded_bytes = InboundBudget::validate_decoded(std::slice::from_ref(&batch)).unwrap();
    let budget = admitted_test_budget();
    let (tx, rx) = mpsc::bounded_async::<Inbound>(1);
    let work_ready = tokio::sync::Notify::new();
    let owners = [10, 10];
    let assignment_fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &owners,
        vec![crate::checkpoint::CheckpointParticipant {
            node_id: 10,
            boot_incarnation: Uuid::from_u128(10),
        }],
    )
    .unwrap();
    let assignment =
        InstalledAssignment::for_process(&assignment_fence, &owners, 10, Uuid::from_u128(10))
            .unwrap();
    assert!(forward_routed_batch(
        &tx,
        &work_ready,
        stream,
        10,
        &assignment,
        "same-stage".to_string(),
        vec![0, 1],
        batch,
        budget,
        decoded_bytes,
        0,
        &cancel,
        &process_lease,
    )
    .await
    .unwrap());
    admission.commit_after_enqueue().unwrap();

    let received = rx.recv().await.unwrap();
    let ShuffleMessage::Data {
        stage,
        routed_vnodes,
        batch,
    } = received.msg
    else {
        panic!("expected one coalesced routed batch");
    };
    assert_eq!(stage, "same-stage");
    assert_eq!(&*routed_vnodes, &[0, 1]);
    assert_eq!(batch.num_rows(), 2);
    assert!(batch.schema().field_with_name("__laminar_vnode").is_ok());
    assert!(rx.try_recv().is_err());
    assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 0);
}

#[tokio::test]
async fn foreign_route_is_rejected_before_queue_publication() {
    let stream = fence(1, 10, 100, 1);
    let cancel = CancellationToken::new();
    let process_lease = live_process_lease();
    let owners = [10, 20];
    let assignment_fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &owners,
        vec![
            crate::checkpoint::CheckpointParticipant {
                node_id: 10,
                boot_incarnation: Uuid::from_u128(10),
            },
            crate::checkpoint::CheckpointParticipant {
                node_id: 20,
                boot_incarnation: Uuid::from_u128(20),
            },
        ],
    )
    .unwrap();
    let assignment =
        InstalledAssignment::for_process(&assignment_fence, &owners, 10, Uuid::from_u128(10))
            .unwrap();
    let (tx, rx) = mpsc::bounded_async::<Inbound>(1);
    let work_ready = tokio::sync::Notify::new();
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "value",
            arrow_schema::DataType::Int64,
            false,
        )])),
        vec![Arc::new(arrow_array::Int64Array::from(vec![10]))],
    )
    .unwrap();
    let decoded_bytes = InboundBudget::validate_decoded(std::slice::from_ref(&batch)).unwrap();
    let budget = admitted_test_budget();

    let error = forward_routed_batch(
        &tx,
        &work_ready,
        stream,
        10,
        &assignment,
        "stage".to_string(),
        vec![1],
        batch,
        budget,
        decoded_bytes,
        0,
        &cancel,
        &process_lease,
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn coalesced_batch_with_any_foreign_vnode_is_rejected_atomically() {
    use arrow_array::{Int64Array, UInt32Array};
    use arrow_schema::{DataType, Field, Schema};

    let stream = fence(1, 10, 100, 1);
    let cancel = CancellationToken::new();
    let process_lease = live_process_lease();
    let owners = [10, 20];
    let assignment_fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &owners,
        vec![
            crate::checkpoint::CheckpointParticipant {
                node_id: 10,
                boot_incarnation: Uuid::from_u128(10),
            },
            crate::checkpoint::CheckpointParticipant {
                node_id: 20,
                boot_incarnation: Uuid::from_u128(20),
            },
        ],
    )
    .unwrap();
    let assignment =
        InstalledAssignment::for_process(&assignment_fence, &owners, 10, Uuid::from_u128(10))
            .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("__laminar_vnode", DataType::UInt32, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(UInt32Array::from(vec![0, 1])),
        ],
    )
    .unwrap();
    let decoded_bytes = InboundBudget::validate_decoded(std::slice::from_ref(&batch)).unwrap();
    let budget = admitted_test_budget();
    let (tx, rx) = mpsc::bounded_async::<Inbound>(2);
    let work_ready = tokio::sync::Notify::new();

    let error = forward_routed_batch(
        &tx,
        &work_ready,
        stream,
        10,
        &assignment,
        "stage".to_string(),
        vec![0, 1],
        batch,
        budget,
        decoded_bytes,
        0,
        &cancel,
        &process_lease,
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(rx.try_recv().is_err());
}

#[test]
fn superseded_stream_is_rejected_and_fences_the_epoch() {
    let d = DeliveryTracker::default();
    let old = fence(1, 10, 100, 1);
    let replacement = fence(1, 10, 101, 1);
    d.observe_hello(old).unwrap();
    d.observe_hello(replacement).unwrap();

    assert!(d.prepare_data(&old, 0).is_err());
    assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);
}
