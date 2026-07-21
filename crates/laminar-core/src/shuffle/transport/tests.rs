use std::io;
use std::sync::Arc;

use arrow_array::Int64Array;
use arrow_schema::{DataType, Field, Schema};

use super::*;
use crate::checkpoint::{CheckpointAssignmentFence, CheckpointBarrier, CheckpointParticipant};
use crate::cluster::control::LeaseDeadline;
use crate::state::CheckpointAttempt;
use uuid::Uuid;

fn assignment_owners(nodes: &[ShufflePeerId]) -> Vec<ShufflePeerId> {
    let mut participants = nodes.to_vec();
    participants.sort_unstable();
    participants.dedup();
    let first_owner = participants
        .iter()
        .copied()
        .find(|node| *node == 2)
        .or_else(|| participants.last().copied())
        .expect("test assignment has a participant");
    let mut rotation = Vec::with_capacity(participants.len());
    rotation.push(first_owner);
    rotation.extend(
        participants
            .iter()
            .copied()
            .filter(|node| *node != first_owner),
    );
    let vnode_count = 8.max(rotation.len());
    rotation.into_iter().cycle().take(vnode_count).collect()
}

fn assignment_fence(version: u64, nodes: &[ShufflePeerId]) -> CheckpointAssignmentFence {
    let mut nodes = nodes.to_vec();
    nodes.sort_unstable();
    nodes.dedup();
    let owners = assignment_owners(&nodes);
    CheckpointAssignmentFence::from_owner_map(
        version,
        &owners,
        nodes
            .iter()
            .copied()
            .map(|node_id| CheckpointParticipant {
                node_id,
                boot_incarnation: Uuid::from_u128(u128::from(node_id) + 1),
            })
            .collect(),
    )
    .unwrap()
}

async fn send_barrier(
    sender: &ShuffleSender,
    peers: &[ShufflePeerId],
    barrier: CheckpointBarrier,
) -> io::Result<()> {
    let mut nodes = peers.to_vec();
    nodes.push(sender.local_id());
    let fence = assignment_fence(sender.assignment_version(), &nodes);
    sender.fan_out_barrier(peers, barrier, &fence).await
}

async fn bind_on_loopback_with_incarnation(
    local_id: ShufflePeerId,
    incarnation: Uuid,
) -> ShuffleReceiver {
    let receiver = ShuffleReceiver::bind(local_id, "127.0.0.1:0".parse().unwrap(), incarnation)
        .await
        .expect("bind");
    receiver
        .install_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(60),
        )))
        .expect("install receiver process lease");
    let mut nodes = vec![1, local_id];
    if local_id == 1 {
        nodes.push(2);
    }
    nodes.sort_unstable();
    nodes.dedup();
    let mut fence = assignment_fence(1, &nodes);
    let participant = fence
        .participants
        .iter_mut()
        .find(|participant| participant.node_id == local_id)
        .expect("receiver belongs to its test assignment");
    participant.boot_incarnation = incarnation;
    receiver
        .install_assignment_fence(&fence, &assignment_owners(&nodes))
        .unwrap();
    receiver
}

async fn bind_on_loopback(local_id: ShufflePeerId) -> ShuffleReceiver {
    bind_on_loopback_with_incarnation(local_id, Uuid::from_u128(u128::from(local_id) + 1)).await
}

fn sender(local_id: ShufflePeerId) -> ShuffleSender {
    let sender = ShuffleSender::new(local_id, Uuid::from_u128(u128::from(local_id) + 1));
    sender.install_live_process_lease_for_test();
    let fence = assignment_fence(1, &[1, 2]);
    sender
        .install_assignment_fence(&fence, &assignment_owners(&[1, 2]))
        .unwrap();
    sender
}

fn one_row(value: i64) -> arrow_array::RecordBatch {
    arrow_array::RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![value]))],
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_natural_process_lease_expiry_cancels_blocked_admission() {
    let receiver = bind_on_loopback(2).await;
    let sender = ShuffleSender::new(1, Uuid::from_u128(2));
    let deadline = Arc::new(LeaseDeadline::live_for(std::time::Duration::from_millis(
        250,
    )));
    sender
        .install_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    let fence = assignment_fence(1, &[1, 2]);
    sender
        .install_assignment_fence(&fence, &assignment_owners(&[1, 2]))
        .unwrap();
    sender.register_peer(2, receiver.local_addr());
    let held_budget = sender.hold_outbound_budget_for_test(2).await.unwrap();
    let message = ShuffleMessage::checkpointed("stage".into(), 0, one_row(1));
    let mut blocked = Box::pin(sender.send_to(2, &message));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut blocked)
            .await
            .is_err(),
        "send did not block on its exhausted byte admission"
    );

    let error = tokio::time::timeout(std::time::Duration::from_secs(1), blocked)
        .await
        .expect("process lease expiry did not wake blocked shuffle admission")
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    assert_eq!(sender.assignment_version(), 1);
    drop(held_budget);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_process_lease_expiry_rejects_handshake_without_assignment_invalidation() {
    use super::shuffle_v1::shuffle_transport_client::ShuffleTransportClient;
    use super::shuffle_v1::HandshakeRequest;

    let receiver = ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), Uuid::from_u128(3))
        .await
        .unwrap();
    let deadline = Arc::new(LeaseDeadline::live_for(std::time::Duration::from_secs(60)));
    receiver
        .install_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    let fence = assignment_fence(1, &[1, 2]);
    receiver
        .install_assignment_fence(&fence, &assignment_owners(&[1, 2]))
        .unwrap();
    deadline.fence();

    let endpoint =
        crate::cluster::control::tls::client_endpoint(&receiver.local_addr().to_string()).unwrap();
    let channel = endpoint.connect().await.unwrap();
    let mut client = ShuffleTransportClient::new(channel);
    let stream_id = Uuid::new_v4();
    let error = client
        .handshake(tonic::Request::new(HandshakeRequest {
            sender_node_id: 1,
            sender_incarnation: Uuid::from_u128(2).as_bytes().to_vec(),
            stream_id: stream_id.as_bytes().to_vec(),
            assignment_version: fence.assignment_version,
            recovery_gen: 0,
            assignment_certificate_digest: fence.digest().to_vec(),
        }))
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(error.message(), "shuffle process lease is no longer live");
    assert_eq!(receiver.assignment_version(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_process_lease_expiry_discards_already_queued_frames() {
    let receiver = ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), Uuid::from_u128(3))
        .await
        .unwrap();
    let deadline = Arc::new(LeaseDeadline::live_for(std::time::Duration::from_secs(60)));
    receiver
        .install_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    let fence = assignment_fence(1, &[1, 2]);
    receiver
        .install_assignment_fence(&fence, &assignment_owners(&[1, 2]))
        .unwrap();
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());
    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
        )
        .await
        .unwrap();
    wait_until(|| receiver.committed_sequence_for_test(1) == Some(1)).await;

    deadline.fence();

    assert!(receiver.drain_available().is_empty());
    assert_eq!(receiver.assignment_version(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_natural_process_lease_expiry_wakes_an_idle_consumer() {
    let receiver = ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), Uuid::from_u128(3))
        .await
        .unwrap();
    let deadline = Arc::new(LeaseDeadline::live_for(std::time::Duration::from_millis(
        100,
    )));
    receiver
        .install_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    let fence = assignment_fence(1, &[1, 2]);
    receiver
        .install_assignment_fence(&fence, &assignment_owners(&[1, 2]))
        .unwrap();
    let mut waiting = Box::pin(receiver.recv());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut waiting)
            .await
            .is_err(),
        "empty receiver unexpectedly completed"
    );

    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .expect("process lease expiry did not wake the idle consumer")
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_renewal_supersedes_the_old_idle_wait_deadline() {
    let receiver = ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), Uuid::from_u128(3))
        .await
        .unwrap();
    let deadline = Arc::new(LeaseDeadline::live_for(std::time::Duration::from_millis(
        100,
    )));
    receiver
        .install_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    let fence = assignment_fence(1, &[1, 2]);
    receiver
        .install_assignment_fence(&fence, &assignment_owners(&[1, 2]))
        .unwrap();
    let mut waiting = Box::pin(receiver.recv());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );

    deadline.extend(std::time::Duration::from_millis(300));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(120), &mut waiting)
            .await
            .is_err(),
        "idle receiver retained the superseded lease deadline"
    );
    deadline.fence();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .expect("terminal fence did not wake the renewed receiver")
            .is_none()
    );
}

#[tokio::test]
async fn assignment_activation_requires_an_installed_process_lease() {
    let fence = assignment_fence(1, &[1, 2]);
    let owners = assignment_owners(&[1, 2]);
    let sender = ShuffleSender::new(1, Uuid::from_u128(2));
    let sender_error = sender
        .install_assignment_fence(&fence, &owners)
        .expect_err("unwired outbound shuffle must fail closed");
    assert_eq!(sender_error.kind(), io::ErrorKind::PermissionDenied);

    let receiver = ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), Uuid::from_u128(3))
        .await
        .unwrap();
    let receiver_error = receiver
        .install_assignment_fence(&fence, &owners)
        .expect_err("unwired inbound shuffle must fail closed");
    assert_eq!(receiver_error.kind(), io::ErrorKind::PermissionDenied);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assignment_mesh_waits_for_exact_peer_certificate_without_consuming_sequence() {
    let fence = assignment_fence(1, &[1, 2]);
    let owners = assignment_owners(&[1, 2]);
    let receiver = ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), Uuid::from_u128(3))
        .await
        .unwrap();
    receiver
        .install_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(60),
        )))
        .unwrap();
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());

    let error = sender
        .establish_assignment_mesh(&fence)
        .await
        .expect_err("an uncertified receiver must keep the assignment mesh fenced");
    assert!(error.to_string().contains("peer 2"), "{error}");
    assert_eq!(sender.tracked_resources_for_test().3, 0);

    receiver.install_assignment_fence(&fence, &owners).unwrap();
    sender.establish_assignment_mesh(&fence).await.unwrap();
    let resources = sender.tracked_resources_for_test();
    assert_eq!(resources.1, 1);
    assert_eq!(
        resources.3, 0,
        "a handshake must not allocate a data sequence"
    );

    let mut conflicting_owners = owners;
    conflicting_owners.swap(0, 1);
    let conflicting = CheckpointAssignmentFence::from_owner_map(
        fence.assignment_version,
        &conflicting_owners,
        fence.participants.clone(),
    )
    .unwrap();
    assert_eq!(
        sender
            .establish_assignment_mesh(&conflicting)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
}

async fn wait_until(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shuffle state did not settle");
}

#[test]
fn shuffle_assignment_admission_rejects_oversized_roster() {
    let maximum = u64::try_from(crate::checkpoint::MAX_CHECKPOINT_PARTICIPANTS).unwrap();
    let nodes = (1..=maximum).collect::<Vec<_>>();
    let mut forged = assignment_fence(1, &nodes);
    forged.participants.push(CheckpointParticipant {
        node_id: maximum + 1,
        boot_incarnation: Uuid::from_u128(u128::from(maximum + 1) + 1),
    });
    let sender = ShuffleSender::new(1, Uuid::from_u128(2));

    assert_eq!(
        sender
            .install_assignment_fence(&forged, &assignment_owners(&nodes))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(sender.assignment_version(), 0);
}

#[test]
fn shuffle_assignment_admission_rejects_an_unbound_owner_vector() {
    let fence = assignment_fence(1, &[1, 2]);
    let sender = ShuffleSender::new(1, Uuid::from_u128(2));

    let error = sender
        .install_assignment_fence(&fence, &[1, 2])
        .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(sender.assignment_version(), 0);
}

#[test]
fn shuffle_assignment_admission_rejects_a_partitioning_abi_mismatch() {
    let owners = assignment_owners(&[1, 2]);
    let mut fence = assignment_fence(1, &[1, 2]);
    fence.partitioning_abi_version = crate::state::PARTITIONING_ABI_VERSION.saturating_add(1);
    let sender = ShuffleSender::new(1, Uuid::from_u128(2));

    let error = sender
        .install_assignment_fence(&fence, &owners)
        .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(sender.assignment_version(), 0);
}

#[test]
fn same_version_certificate_replacement_is_rejected_even_while_invalidated() {
    let sender = sender(1);
    let installed = assignment_fence(1, &[1, 2]);
    let conflicting =
        CheckpointAssignmentFence::from_owner_map(1, &[2, 1], installed.participants.clone())
            .unwrap();

    assert_eq!(
        sender
            .install_assignment_fence(&conflicting, &[2, 1])
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    sender.invalidate_assignment_fence();
    assert_eq!(sender.assignment_version(), 0);
    assert_eq!(
        sender
            .install_assignment_fence(&conflicting, &[2, 1])
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        sender
            .install_assignment_fence(&installed, &assignment_owners(&[1, 2]))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        sender.assignment_version(),
        0,
        "an invalidated version cannot be reactivated"
    );
    let successor = assignment_fence(2, &[1, 2]);
    assert!(sender
        .install_assignment_fence(&successor, &assignment_owners(&[1, 2]))
        .unwrap());
    assert_eq!(sender.assignment_version(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_suspension_reactivates_exact_certificate_without_resetting_sequence() {
    use std::sync::atomic::Ordering as O;

    let fence = assignment_fence(1, &[1, 2]);
    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());

    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
        )
        .await
        .unwrap();
    let _ = receiver.recv().await.unwrap();

    sender.suspend_assignment_fence();
    receiver.suspend_assignment_fence();
    assert_eq!(sender.assignment_version(), 0);
    assert_eq!(receiver.assignment_version(), 0);
    let owners = assignment_owners(&[1, 2]);
    assert!(sender.install_assignment_fence(&fence, &owners).unwrap());
    assert!(receiver.install_assignment_fence(&fence, &owners).unwrap());

    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("stage".into(), 0, one_row(2)),
        )
        .await
        .unwrap();
    let received = receiver.recv().await.unwrap();
    assert!(matches!(received.message(), ShuffleMessage::Data { .. }));
    assert_eq!(
        receiver.delivery_loss_incidents().load(O::Acquire),
        0,
        "same-version reactivation must preserve the sender and receiver sequence domain"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn suspended_receiver_does_not_consume_checkpoint_holdover() {
    let fence = assignment_fence(1, &[1, 2]);
    let owners = assignment_owners(&[1, 2]);
    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());

    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("held".into(), 0, one_row(5)),
        )
        .await
        .unwrap();
    send_barrier(&sender, &[2], CheckpointBarrier::new(70, 70))
        .await
        .unwrap();
    wait_until(|| receiver.committed_sequence_for_test(1) == Some(1)).await;
    assert!(receiver.drain_checkpointed_data_for("absent").is_empty());

    receiver.suspend_assignment_fence();
    let error = receiver.drain_checkpointed_holdover().unwrap_err();
    assert!(is_scope_cancelled(&error));

    assert!(receiver.install_assignment_fence(&fence, &owners).unwrap());
    let drained = receiver.drain_checkpointed_holdover().unwrap();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].0, "held");
    assert_eq!(
        drained[0]
            .1
            .batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assignment_resume_cannot_be_lost_before_the_waiter_is_polled() {
    let receiver = Arc::new(bind_on_loopback(2).await);
    let installed = receiver.assignment_fence_for_test();
    let participants: Vec<_> = installed
        .participants
        .iter()
        .map(|participant| participant.node_id)
        .collect();
    let owners = assignment_owners(&participants);
    receiver.suspend_assignment_fence();
    let (entered, release) = receiver.pause_next_assignment_wait_for_test();
    let waiting = {
        let receiver = Arc::clone(&receiver);
        tokio::spawn(async move { receiver.wait_while_assignment_suspended_for_test().await })
    };

    tokio::time::timeout(std::time::Duration::from_secs(1), entered.notified())
        .await
        .expect("assignment waiter did not reach the pre-poll boundary");
    assert!(receiver
        .install_assignment_fence(&installed, &owners)
        .unwrap());
    release.notify_one();

    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .expect("same-version resume notification was lost")
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assignment_churn_prunes_sender_peer_and_connection_state() {
    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    for peer in 2..=64 {
        sender.register_peer(peer, receiver.local_addr());
    }
    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
        )
        .await
        .unwrap();
    let _ = receiver.recv().await.unwrap();
    let before = sender.tracked_resources_for_test();
    assert_eq!(before.0, 63);
    assert!(before.1 > 0 && before.2 > 0 && before.3 > 0);

    for version in 2..=64 {
        let peer = version + 100;
        sender.register_peer(peer, receiver.local_addr());
        let fence = assignment_fence(version, &[1, peer]);
        assert!(sender
            .install_assignment_fence(&fence, &assignment_owners(&[1, peer]))
            .unwrap());
        assert_eq!(sender.tracked_resources_for_test(), (1, 0, 0, 0));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_envelope_survives_exact_same_version_suspension() {
    use std::sync::atomic::Ordering as O;

    let fence = assignment_fence(1, &[1, 2]);
    let owners = assignment_owners(&[1, 2]);
    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());
    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
        )
        .await
        .unwrap();
    wait_until(|| receiver.committed_sequence_for_test(1) == Some(1)).await;

    let entered = receiver.pause_next_recv_after_defer_for_test();
    let mut blocked_recv = Box::pin(receiver.recv());
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        tokio::select! {
            () = entered.notified() => {}
            received = &mut blocked_recv => {
                panic!("receive completed before deferral: {received:?}");
            }
        }
    })
    .await
    .expect("receive did not enter the deferred slot");

    let mut competing_recv = Box::pin(receiver.recv());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut competing_recv)
            .await
            .is_err(),
        "a competing receive bypassed the receiver lease"
    );
    sender.suspend_assignment_fence();
    receiver.suspend_assignment_fence();
    drop(blocked_recv);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut competing_recv)
            .await
            .is_err(),
        "a competing receive consumed the deferred frame while suspended"
    );
    drop(competing_recv);
    assert!(sender.install_assignment_fence(&fence, &owners).unwrap());
    assert!(receiver.install_assignment_fence(&fence, &owners).unwrap());

    let received = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
        .await
        .expect("cancelled suspended receive did not retain its envelope")
        .expect("shuffle queue closed");
    assert!(matches!(received.message(), ShuffleMessage::Data { .. }));
    drop(received);

    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("state".into(), 0, one_row(2)),
        )
        .await
        .unwrap();
    wait_until(|| receiver.committed_sequence_for_test(1) == Some(2)).await;
    sender.suspend_assignment_fence();
    receiver.suspend_assignment_fence();
    assert!(sender.install_assignment_fence(&fence, &owners).unwrap());
    assert!(receiver.install_assignment_fence(&fence, &owners).unwrap());

    let drained = receiver.drain_checkpointed_staged();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].0, "state");
    assert_eq!(drained[0].1.batch().num_rows(), 1);
    assert_eq!(receiver.delivery_loss_incidents().load(O::Acquire), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn superseded_scope_cancels_blocked_outbound_budget_without_switching_scope() {
    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());
    let held_budget = sender.hold_outbound_budget_for_test(2).await.unwrap();
    let message = ShuffleMessage::checkpointed("stage".into(), 0, one_row(1));
    let mut blocked = Box::pin(sender.send_to_for_assignment(2, 1, &message));

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut blocked)
            .await
            .is_err()
    );

    let successor = assignment_fence(2, &[1, 2]);
    let owners = assignment_owners(&[1, 2]);
    assert!(receiver
        .install_assignment_fence(&successor, &owners)
        .unwrap());
    assert!(sender
        .install_assignment_fence(&successor, &owners)
        .unwrap());

    let error = tokio::time::timeout(std::time::Duration::from_secs(1), &mut blocked)
        .await
        .expect("superseded outbound admission remained blocked")
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    assert!(is_scope_cancelled(&error));

    sender
        .send_to_for_assignment(
            2,
            2,
            &ShuffleMessage::checkpointed("stage".into(), 0, one_row(2)),
        )
        .await
        .unwrap();
    let received = receiver.recv().await.unwrap();
    assert_eq!(received.assignment_version(), 2);
    drop(held_budget);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_scope_cancellation_releases_idle_inbound_stream_slot() {
    use std::sync::atomic::Ordering as O;

    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());
    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
        )
        .await
        .unwrap();
    let _ = receiver.recv().await.unwrap();
    wait_until(|| receiver.active_streams_for_test() == 1).await;

    receiver.set_recovery_gen(1);

    wait_until(|| receiver.active_streams_for_test() == 0).await;
    assert_eq!(receiver.delivery_loss_incidents().load(O::Acquire), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_to_receiver_delivers_with_peer_attribution() {
    let recv = bind_on_loopback(2).await;
    let recv_addr = recv.local_addr();

    let sender = sender(1);
    sender.register_peer(2, recv_addr);
    send_barrier(&sender, &[2], CheckpointBarrier::new(1234, 1234))
        .await
        .unwrap();

    let received = recv.recv().await.unwrap();
    let from = received.peer();
    assert_eq!(from, 1, "receiver attributes frame to sender id");
    assert_eq!(
        received.message(),
        &ShuffleMessage::Barrier(CheckpointBarrier::new(1234, 1234))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_wire_rejects_noncanonical_barrier_before_publication() {
    use super::shuffle_v1::shuffle_transport_client::ShuffleTransportClient;
    use super::shuffle_v1::{
        shuffle_frame, Barrier as WireBarrier, HandshakeRequest, Hello, ShuffleFrame,
    };

    let receiver = bind_on_loopback(2).await;
    let fence = assignment_fence(1, &[1, 2]);
    let endpoint =
        crate::cluster::control::tls::client_endpoint(&receiver.local_addr().to_string()).unwrap();
    let channel = endpoint.connect().await.unwrap();
    let mut client = ShuffleTransportClient::new(channel);
    let sender_incarnation = Uuid::from_u128(2);
    let stream_id = Uuid::new_v4();
    let handshake = client
        .handshake(tonic::Request::new(HandshakeRequest {
            sender_node_id: 1,
            sender_incarnation: sender_incarnation.as_bytes().to_vec(),
            stream_id: stream_id.as_bytes().to_vec(),
            assignment_version: fence.assignment_version,
            recovery_gen: 0,
            assignment_certificate_digest: fence.digest().to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();
    let hello = ShuffleFrame {
        kind: Some(shuffle_frame::Kind::Hello(Hello {
            node_id: 1,
            sender_incarnation: sender_incarnation.as_bytes().to_vec(),
            receiver_incarnation: handshake.receiver_incarnation,
            stream_id: stream_id.as_bytes().to_vec(),
            assignment_version: fence.assignment_version,
            recovery_gen: 0,
            assignment_certificate_digest: fence.digest().to_vec(),
        })),
    };
    let malformed = ShuffleFrame {
        kind: Some(shuffle_frame::Kind::Barrier(WireBarrier {
            checkpoint_id: 7,
            epoch: 8,
            flags: 0,
            last_seq: 0,
            assignment_version: fence.assignment_version,
            assignment_digest: fence.digest().to_vec(),
            recovery_gen: 0,
        })),
    };

    let error = client
        .shuffle(tonic::Request::new(futures::stream::iter([
            hello, malformed,
        ])))
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(error.message(), NONCANONICAL_BARRIER);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), receiver.recv())
            .await
            .is_err(),
        "a noncanonical wire barrier reached the receive queue"
    );
    assert_eq!(
        receiver
            .delivery_loss_incidents()
            .load(std::sync::atomic::Ordering::Acquire),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncertified_network_handshakes_are_rejected_without_reporting_data_loss() {
    use super::shuffle_v1::shuffle_transport_client::ShuffleTransportClient;
    use super::shuffle_v1::HandshakeRequest;

    let receiver = bind_on_loopback(2).await;
    let fence = assignment_fence(1, &[1, 2]);
    let endpoint =
        crate::cluster::control::tls::client_endpoint(&receiver.local_addr().to_string()).unwrap();
    let channel = endpoint.connect().await.unwrap();
    let mut client = ShuffleTransportClient::new(channel);
    let stream_id = Uuid::new_v4();
    let error = client
        .handshake(tonic::Request::new(HandshakeRequest {
            sender_node_id: 1,
            sender_incarnation: Uuid::from_u128(999).as_bytes().to_vec(),
            stream_id: stream_id.as_bytes().to_vec(),
            assignment_version: fence.assignment_version,
            recovery_gen: 0,
            assignment_certificate_digest: fence.digest().to_vec(),
        }))
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    let stream_id = Uuid::new_v4();
    let error = client
        .handshake(tonic::Request::new(HandshakeRequest {
            sender_node_id: 1,
            sender_incarnation: Uuid::from_u128(2).as_bytes().to_vec(),
            stream_id: stream_id.as_bytes().to_vec(),
            assignment_version: fence.assignment_version,
            recovery_gen: 0,
            assignment_certificate_digest: [9; 32].to_vec(),
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        receiver
            .delivery_loss_incidents()
            .load(std::sync::atomic::Ordering::Acquire),
        0,
        "traffic rejected before stream admission is not delivered-data loss"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_restart_is_rejected_until_recovery_generation_advances() {
    let receiver_v1 = bind_on_loopback(2).await;
    let receiver_v1_incarnation = receiver_v1.incarnation();
    let sender = sender(1);
    sender.register_peer(2, receiver_v1.local_addr());

    for value in [10, 20] {
        sender
            .send_to(
                2,
                &ShuffleMessage::checkpointed("pipeline/stage/input-0".into(), 0, one_row(value)),
            )
            .await
            .unwrap();
        let received = receiver_v1.recv().await.unwrap();
        let ShuffleMessage::Data { batch, .. } = received.message() else {
            panic!("expected data frame")
        };
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            value
        );
    }

    sender.disconnect_peer_for_test(2);
    drop(receiver_v1);
    let receiver_v2 = bind_on_loopback_with_incarnation(2, Uuid::from_u128(300)).await;
    sender.register_peer(2, receiver_v2.local_addr());
    let error = sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("pipeline/stage/input-0".into(), 0, one_row(30)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), receiver_v2.recv())
            .await
            .is_err(),
        "same-generation receiver restart must not fold data"
    );

    let restarted_owners = assignment_owners(&[1, 2]);
    let stale_receiver_fence = CheckpointAssignmentFence::from_owner_map(
        2,
        &restarted_owners,
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: sender.incarnation(),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: receiver_v1_incarnation,
            },
        ],
    )
    .unwrap();
    assert_eq!(
        receiver_v2
            .install_assignment_fence(&stale_receiver_fence, &restarted_owners)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let restarted_fence = CheckpointAssignmentFence::from_owner_map(
        2,
        &restarted_owners,
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: sender.incarnation(),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: receiver_v2.incarnation(),
            },
        ],
    )
    .unwrap();
    receiver_v2
        .install_assignment_fence(&restarted_fence, &restarted_owners)
        .unwrap();
    sender
        .install_assignment_fence(&restarted_fence, &restarted_owners)
        .unwrap();
    receiver_v2.set_recovery_gen(1);
    sender.set_recovery_gen(1);
    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("pipeline/stage/input-0".into(), 0, one_row(30)),
        )
        .await
        .unwrap();
    let received = receiver_v2.recv().await.unwrap();
    let ShuffleMessage::Data { batch, .. } = received.message() else {
        panic!("expected data frame after receiver restart")
    };
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        30
    );

    sender
        .fan_out_barrier(&[2], CheckpointBarrier::new(1, 1), &restarted_fence)
        .await
        .unwrap();
    assert!(matches!(
        receiver_v2.recv().await.unwrap().message(),
        ShuffleMessage::Barrier(_)
    ));
    assert_eq!(
        receiver_v2
            .delivery_loss_incidents()
            .load(std::sync::atomic::Ordering::Acquire),
        0,
        "post-recovery receiver restart must reset the sender sequence exactly once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_senders_preserve_sequence_and_every_record() {
    let receiver = bind_on_loopback(2).await;
    let sender = Arc::new(sender(1));
    sender.register_peer(2, receiver.local_addr());

    let mut tasks = Vec::new();
    for value in 0..128i64 {
        let sender = Arc::clone(&sender);
        tasks.push(tokio::spawn(async move {
            sender
                .send_to(
                    2,
                    &ShuffleMessage::checkpointed(
                        "pipeline/stage/input-0".into(),
                        0,
                        one_row(value),
                    ),
                )
                .await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    send_barrier(&sender, &[2], CheckpointBarrier::new(1, 1))
        .await
        .unwrap();

    let mut values = std::collections::BTreeSet::new();
    loop {
        let envelope = receiver.recv().await.unwrap();
        let ShuffleMessage::Data { batch, .. } = envelope.message() else {
            break;
        };
        values.insert(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
        );
    }
    assert_eq!(values, (0..128i64).collect());
    assert_eq!(
        receiver
            .delivery_loss_incidents()
            .load(std::sync::atomic::Ordering::Acquire),
        0,
        "concurrent enqueue order must equal assigned sequence order"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_assignment_stream_is_rejected_before_folding() {
    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());
    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("pipeline/stage/input-0".into(), 0, one_row(1)),
        )
        .await
        .unwrap();
    assert!(matches!(
        receiver.recv().await.unwrap().message(),
        ShuffleMessage::Data { .. }
    ));

    let next_assignment = assignment_fence(2, &[1, 2]);
    receiver
        .install_assignment_fence(&next_assignment, &assignment_owners(&[1, 2]))
        .unwrap();
    let _ = sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("pipeline/stage/input-0".into(), 0, one_row(999)),
        )
        .await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), receiver.recv())
            .await
            .is_err()
    );
    assert_eq!(
        receiver
            .delivery_loss_incidents()
            .load(std::sync::atomic::Ordering::Acquire),
        0,
        "a deliberate assignment transition is not transit loss"
    );

    sender
        .install_assignment_fence(&next_assignment, &assignment_owners(&[1, 2]))
        .unwrap();
    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("pipeline/stage/input-0".into(), 0, one_row(2)),
        )
        .await
        .unwrap();
    let envelope = receiver.recv().await.unwrap();
    let ShuffleMessage::Data { batch, .. } = envelope.message() else {
        panic!("expected current-assignment data")
    };
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assignment_guard_rejects_stale_routing_before_enqueue() {
    let receiver = bind_on_loopback(2).await;
    let next_assignment = assignment_fence(2, &[1, 2]);
    receiver
        .install_assignment_fence(&next_assignment, &assignment_owners(&[1, 2]))
        .unwrap();
    let sender = sender(1);
    sender
        .install_assignment_fence(&next_assignment, &assignment_owners(&[1, 2]))
        .unwrap();
    sender.register_peer(2, receiver.local_addr());

    let error = sender
        .send_to_for_assignment(
            2,
            1,
            &ShuffleMessage::checkpointed("pipeline/stage/input-0".into(), 0, one_row(999)),
        )
        .await
        .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), receiver.recv())
            .await
            .is_err(),
        "data routed with an old assignment must not enter the outbound queue"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_shuffle_is_rejected_before_connection_or_enqueue() {
    let receiver = bind_on_loopback(1).await;
    let sender = sender(1);
    sender.register_peer(1, receiver.local_addr());

    let error = sender
        .send_to(
            1,
            &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
        )
        .await
        .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), receiver.recv())
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_reconciliation_does_not_scan_ordinary_shuffle_traffic() {
    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());
    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("live".into(), 0, one_row(1)),
        )
        .await
        .unwrap();

    assert!(!receiver.stage_checkpointed_inbound());
    let received = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
        .await
        .expect("ordinary traffic must remain on the live receive path")
        .expect("shuffle receiver closed");
    assert!(matches!(received.message(), ShuffleMessage::Data { .. }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_barrier_retirement_unblocks_data_and_preserves_data_holdover() {
    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());

    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("held".into(), 0, one_row(1)),
        )
        .await
        .unwrap();
    let terminal = CheckpointBarrier::new(70, 70);
    send_barrier(&sender, &[2], terminal).await.unwrap();
    let older = CheckpointBarrier::new(60, 60);
    send_barrier(&sender, &[2], older).await.unwrap();
    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("after".into(), 0, one_row(2)),
        )
        .await
        .unwrap();
    wait_until(|| receiver.committed_sequence_for_test(1) == Some(2)).await;

    assert!(!receiver.has_staged_checkpoint_barriers());
    assert!(receiver.stage_checkpointed_inbound());
    let queued_older = receiver.recv().await.unwrap();
    assert!(matches!(
        queued_older.message(),
        ShuffleMessage::Barrier(barrier) if *barrier == older
    ));
    receiver.stash_barrier(queued_older);

    receiver
        .retire_checkpoint_barriers(
            CheckpointAttempt::canonical(70),
            receiver.assignment_fence_for_test().digest(),
        )
        .unwrap();
    assert!(receiver.drain_staged_barriers().is_empty());

    let after = receiver.drain_checkpointed_data_for("after");
    assert_eq!(after.len(), 1);
    assert_eq!(
        after[0]
            .batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );

    let held = receiver.drain_checkpointed_data_for("held");
    assert_eq!(held.len(), 1);
    assert_eq!(
        held[0]
            .batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn barrier_retirement_rejects_exact_digest_mismatch_and_noncanonical_attempt() {
    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());
    let terminal = CheckpointAttempt::canonical(70);
    let assignment_digest = receiver.assignment_fence_for_test().digest();

    send_barrier(&sender, &[2], CheckpointBarrier::new(70, 70))
        .await
        .unwrap();
    let mut wrong_digest = receiver.recv().await.unwrap();
    wrong_digest.assignment_digest = Some([9; 32]);
    receiver.stash_barrier(wrong_digest);
    let error = receiver
        .retire_checkpoint_barriers(terminal, assignment_digest)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("different assignment digest"));
    assert_eq!(receiver.drain_staged_barriers().len(), 1);

    let error = receiver
        .retire_checkpoint_barriers(CheckpointAttempt::new(8, 69), assignment_digest)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("canonical checkpoint ID"));
    assert!(!receiver.has_staged_checkpoint_barriers());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_retired_barrier_does_not_block_normal_drain() {
    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());

    let terminal = CheckpointAttempt::canonical(70);
    let assignment_digest = receiver.assignment_fence_for_test().digest();
    receiver
        .retire_checkpoint_barriers(terminal, assignment_digest)
        .unwrap();
    receiver
        .retire_checkpoint_barriers(CheckpointAttempt::canonical(60), assignment_digest)
        .unwrap();
    assert_eq!(
        receiver
            .retire_checkpoint_barriers(CheckpointAttempt::new(8, 69), assignment_digest,)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );

    send_barrier(&sender, &[2], CheckpointBarrier::new(70, 70))
        .await
        .unwrap();
    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("after".into(), 0, one_row(3)),
        )
        .await
        .unwrap();
    wait_until(|| receiver.committed_sequence_for_test(1) == Some(1)).await;

    let after = receiver.drain_checkpointed_data_for("after");
    assert_eq!(after.len(), 1);
    assert_eq!(
        after[0]
            .batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    assert!(receiver.drain_staged_barriers().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_rotation_retired_barrier_is_discarded_after_rotation_without_loss() {
    use std::sync::atomic::Ordering as O;

    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());

    let terminal = CheckpointAttempt::canonical(70);
    send_barrier(&sender, &[2], CheckpointBarrier::new(70, 70))
        .await
        .unwrap();
    wait_until(|| receiver.committed_sequence_for_test(1) == Some(0)).await;
    receiver
        .retire_checkpoint_barriers(terminal, receiver.assignment_fence_for_test().digest())
        .unwrap();

    let successor = assignment_fence(2, &[1, 2]);
    let owners = assignment_owners(&[1, 2]);
    assert!(receiver
        .install_assignment_fence(&successor, &owners)
        .unwrap());
    assert!(sender
        .install_assignment_fence(&successor, &owners)
        .unwrap());
    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("after".into(), 0, one_row(4)),
        )
        .await
        .unwrap();
    wait_until(|| receiver.committed_sequence_for_test(1) == Some(1)).await;

    let after = receiver.drain_checkpointed_data_for("after");
    assert_eq!(after.len(), 1);
    assert_eq!(
        after[0]
            .batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        4
    );
    assert_eq!(receiver.delivery_loss_incidents().load(O::Acquire), 0);
    assert!(receiver.drain_staged_barriers().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_checkpoint_id_reuse_after_assignment_rotation_faults_the_cut() {
    use std::sync::atomic::Ordering as O;

    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());

    let terminal = CheckpointAttempt::canonical(70);
    let retired_digest = receiver.assignment_fence_for_test().digest();
    receiver
        .retire_checkpoint_barriers(terminal, retired_digest)
        .unwrap();

    let successor = assignment_fence(2, &[1, 2]);
    assert_ne!(successor.digest(), retired_digest);
    let owners = assignment_owners(&[1, 2]);
    assert!(receiver
        .install_assignment_fence(&successor, &owners)
        .unwrap());
    assert!(sender
        .install_assignment_fence(&successor, &owners)
        .unwrap());

    send_barrier(&sender, &[2], CheckpointBarrier::new(70, 70))
        .await
        .unwrap();
    wait_until(|| {
        let _ = receiver.stage_checkpointed_inbound();
        receiver.delivery_loss_incidents().load(O::Acquire) == 1
    })
    .await;

    assert!(!receiver.has_staged_checkpoint_barriers());
    assert!(receiver.drain_staged_barriers().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn barrier_holdover_is_bounded_faults_the_cut_and_reopens_after_drain() {
    use std::sync::atomic::Ordering as O;

    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());

    for checkpoint_id in 0..=SHUFFLE_RECV_QUEUE {
        send_barrier(
            &sender,
            &[2],
            CheckpointBarrier::new(
                u64::try_from(checkpoint_id + 1).unwrap(),
                u64::try_from(checkpoint_id + 1).unwrap(),
            ),
        )
        .await
        .unwrap();
        let received = receiver.recv().await.unwrap();
        receiver.stash_barrier(received);
    }

    assert_eq!(
        receiver.delivery_loss_incidents().load(O::Acquire),
        1,
        "the first barrier beyond the holdover bound must fault the cut"
    );
    assert_eq!(receiver.drain_staged_barriers().len(), SHUFFLE_RECV_QUEUE);

    send_barrier(
        &sender,
        &[2],
        CheckpointBarrier::new(
            u64::try_from(SHUFFLE_RECV_QUEUE + 2).unwrap(),
            u64::try_from(SHUFFLE_RECV_QUEUE + 2).unwrap(),
        ),
    )
    .await
    .unwrap();
    receiver.stash_barrier(receiver.recv().await.unwrap());
    assert_eq!(receiver.drain_staged_barriers().len(), 1);
    assert_eq!(receiver.delivery_loss_incidents().load(O::Acquire), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_holdover_is_bounded_and_backpressures_without_loss() {
    use std::sync::atomic::Ordering as O;

    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());

    for value in 0..=SHUFFLE_RECV_QUEUE {
        sender
            .send_to(
                2,
                &ShuffleMessage::checkpointed(
                    format!("stage-{value}"),
                    0,
                    one_row(i64::try_from(value).unwrap()),
                ),
            )
            .await
            .unwrap();
    }
    wait_until(|| receiver.committed_sequence_for_test(1) == Some(SHUFFLE_RECV_QUEUE as u64)).await;
    assert!(receiver.drain_checkpointed_data_for("absent").is_empty());
    assert_eq!(receiver.drain_all_staged().len(), SHUFFLE_RECV_QUEUE);
    wait_until(|| {
        receiver.committed_sequence_for_test(1)
            == Some(u64::try_from(SHUFFLE_RECV_QUEUE + 1).unwrap())
    })
    .await;
    assert_eq!(
        receiver
            .drain_checkpointed_data_for(&format!("stage-{SHUFFLE_RECV_QUEUE}"))
            .len(),
        1
    );
    assert_eq!(receiver.delivery_loss_incidents().load(O::Acquire), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_old_generation_is_discarded_without_refaulting_recovery() {
    use std::sync::atomic::Ordering as O;

    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());
    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
        )
        .await
        .unwrap();
    wait_until(|| receiver.committed_sequence_for_test(1) == Some(1)).await;
    assert!(receiver.drain_checkpointed_data_for("other").is_empty());

    receiver.set_recovery_gen(1);

    assert!(receiver.drain_checkpointed_data_for("stage").is_empty());
    assert_eq!(receiver.delivery_loss_incidents().load(O::Acquire), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_old_assignment_faults_instead_of_reaching_an_operator() {
    use std::sync::atomic::Ordering as O;

    let receiver = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, receiver.local_addr());
    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
        )
        .await
        .unwrap();
    wait_until(|| receiver.committed_sequence_for_test(1) == Some(1)).await;
    assert!(receiver.drain_checkpointed_data_for("other").is_empty());

    let successor = assignment_fence(2, &[1, 2]);
    receiver
        .install_assignment_fence(&successor, &assignment_owners(&[1, 2]))
        .unwrap();

    assert!(receiver.drain_checkpointed_data_for("stage").is_empty());
    assert_eq!(receiver.delivery_loss_incidents().load(O::Acquire), 1);
}

/// A receiver past a coordinated rewind rejects the whole old-generation stream. Accepting
/// even its barrier could align a checkpoint around data the receiver intentionally discarded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_rejects_pre_rewind_stream_until_sender_catches_up() {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    let recv = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, recv.local_addr());

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = arrow_array::RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![7]))],
    )
    .unwrap();

    // Receiver rewinds to generation 5; the sender is still stamping generation 0.
    recv.set_recovery_gen(5);
    let error = sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("s".into(), 0, batch.clone()),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Other);
    let error = send_barrier(&sender, &[2], CheckpointBarrier::new(1, 1))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), recv.recv())
            .await
            .is_err()
    );

    // Once the sender catches up to the receiver's generation, data flows again.
    sender.set_recovery_gen(5);
    sender
        .send_to(2, &ShuffleMessage::checkpointed("s".into(), 0, batch))
        .await
        .unwrap();
    let received = recv.recv().await.unwrap();
    assert!(
        matches!(received.message(), ShuffleMessage::Data { .. }),
        "same-generation data frame must be delivered; got {received:?}"
    );
}

/// A frame discarded on reconnect or mid-stream must not vanish silently. The sequence makes
/// the hole visible at the receiver.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_records_a_delivery_loss_incident() {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::atomic::Ordering as O;

    let recv = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, recv.local_addr());
    let incidents = recv.delivery_loss_incidents();

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = arrow_array::RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let data = || ShuffleMessage::checkpointed("s".into(), 0, batch.clone());

    // Frame 0 lands. Frame 1 is "lost": burn its sequence without sending it.
    sender.send_to(2, &data()).await.unwrap();
    let _ = recv.recv().await.unwrap();
    sender.burn_seq_for_test(2);
    assert_eq!(
        incidents.load(O::Acquire),
        0,
        "no loss until the hole is observed"
    );

    // Frame 2 arrives with a gap where frame 1 should have been.
    sender.send_to(2, &data()).await.unwrap();
    let _ = recv.recv().await.unwrap();
    assert_eq!(
        incidents.load(O::Acquire),
        1,
        "the gap must record exactly one loss incident"
    );
}

#[tokio::test]
async fn recovery_resolves_only_the_loss_cutoff_captured_for_its_generation() {
    use std::sync::atomic::Ordering as O;

    let recv = bind_on_loopback(2).await;
    let incidents = recv.delivery_loss_incidents();
    let recovered_incidents = recv.recovered_delivery_loss_incidents();

    incidents.store(2, O::Release);
    recv.set_recovery_gen(5);
    // This loss happened after the rewind began and must not be forgiven by generation 5.
    incidents.store(3, O::Release);

    assert!(!recv.complete_recovery(4));
    assert!(recv.complete_recovery(5));
    assert!(recv.complete_recovery(5), "completion must be idempotent");
    assert_eq!(recovered_incidents.load(O::Acquire), 2);

    recv.set_recovery_gen(6);
    incidents.store(4, O::Release);
    assert!(recv.complete_recovery(6));
    assert_eq!(recovered_incidents.load(O::Acquire), 3);
}

/// Re-publishing the current (or an older) recovery generation is not a rewind. Resetting
/// delivery continuity here would let a lost frame disappear before the next checkpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_current_recovery_generation_does_not_hide_sequence_gap() {
    use std::sync::atomic::Ordering as O;

    let recv = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, recv.local_addr());
    let incidents = recv.delivery_loss_incidents();

    recv.set_recovery_gen(5);
    sender.set_recovery_gen(5);
    sender
        .send_to(2, &ShuffleMessage::checkpointed("s".into(), 0, one_row(1)))
        .await
        .unwrap();
    let _ = recv.recv().await.unwrap();

    sender.burn_seq_for_test(2);
    recv.set_recovery_gen(5);
    recv.set_recovery_gen(4);
    sender
        .send_to(2, &ShuffleMessage::checkpointed("s".into(), 0, one_row(2)))
        .await
        .unwrap();
    let _ = recv.recv().await.unwrap();

    assert_eq!(
        incidents.load(O::Acquire),
        1,
        "equal or stale generation updates must not hide the sequence gap"
    );
}

/// The trailing-loss case the data-gap check cannot see: the last frames of an epoch are
/// dropped and nothing follows them. The barrier's high-water mark exposes it while the
/// checkpoint can still be fenced.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn barrier_high_water_mark_catches_trailing_loss() {
    use std::sync::atomic::Ordering as O;

    let recv = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, recv.local_addr());
    let incidents = recv.delivery_loss_incidents();

    // Two data frames enqueued, both dropped in transit; then the epoch's barrier.
    sender.burn_seq_for_test(2);
    sender.burn_seq_for_test(2);
    send_barrier(&sender, &[2], CheckpointBarrier::new(1, 1))
        .await
        .unwrap();
    let received = recv.recv().await.unwrap();
    assert!(matches!(received.message(), ShuffleMessage::Barrier(_)));
    assert_eq!(
        incidents.load(O::Acquire),
        1,
        "the barrier must reveal frames that never arrived before the epoch seals"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_generation_starts_at_zero_and_detects_a_missing_first_frame() {
    use std::sync::atomic::Ordering as O;

    let recv = bind_on_loopback(2).await;
    let sender = sender(1);
    recv.set_recovery_gen(1);
    sender.set_recovery_gen(1);
    sender.register_peer(2, recv.local_addr());

    // Sequence zero is allocated but never reaches the receiver. A new recovery Hello must
    // not rebaseline at the barrier's high-water mark.
    sender.burn_seq_for_test(2);
    send_barrier(&sender, &[2], CheckpointBarrier::new(1, 1))
        .await
        .unwrap();

    assert!(matches!(
        recv.recv().await.unwrap().message(),
        ShuffleMessage::Barrier(_)
    ));
    assert_eq!(
        recv.delivery_loss_incidents().load(O::Acquire),
        1,
        "recovery must retain a zero-based sequence domain"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_checkpointed_data_budget_does_not_block_barrier_control_admission() {
    let recv = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, recv.local_addr());
    let _held = sender.hold_outbound_budget_for_test(2).await.unwrap();

    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        send_barrier(&sender, &[2], CheckpointBarrier::new(8, 8)),
    )
    .await
    .expect("checkpoint barrier waited on the data byte semaphore")
    .unwrap();

    let received = tokio::time::timeout(std::time::Duration::from_secs(1), recv.recv())
        .await
        .expect("checkpoint barrier did not reach the ordered stream")
        .expect("ordered stream closed");
    assert!(matches!(received.message(), ShuffleMessage::Barrier(_)));
}

/// A rewind deliberately discards in-flight frames, and a restarted peer restarts its
/// sequence at zero. Neither is transit loss.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rewind_and_restart_do_not_count_as_loss() {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::atomic::Ordering as O;

    let recv = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, recv.local_addr());
    let incidents = recv.delivery_loss_incidents();

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = arrow_array::RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let data = || ShuffleMessage::checkpointed("s".into(), 0, batch.clone());

    sender.send_to(2, &data()).await.unwrap();
    let _ = recv.recv().await.unwrap();

    // A round discards whatever was queued, then both ends move to the new generation.
    sender.burn_seq_for_test(2);
    sender.burn_seq_for_test(2);
    recv.set_recovery_gen(9);
    sender.set_recovery_gen(9);

    sender.send_to(2, &data()).await.unwrap();
    let _ = recv.recv().await.unwrap();
    assert_eq!(
        incidents.load(O::Acquire),
        0,
        "a rewind's deliberate discards are not transit loss"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_reuses_stream_across_sends() {
    let recv = bind_on_loopback(2).await;
    let sender = sender(1);
    sender.register_peer(2, recv.local_addr());

    for delta in [10u64, 20, 30, 40] {
        send_barrier(&sender, &[2], CheckpointBarrier::new(delta, delta))
            .await
            .unwrap();
    }

    let mut got = Vec::new();
    for _ in 0..4 {
        let received = recv.recv().await.unwrap();
        got.push(received.message().clone());
    }
    assert_eq!(
        got,
        vec![
            ShuffleMessage::Barrier(CheckpointBarrier::new(10, 10)),
            ShuffleMessage::Barrier(CheckpointBarrier::new(20, 20)),
            ShuffleMessage::Barrier(CheckpointBarrier::new(30, 30)),
            ShuffleMessage::Barrier(CheckpointBarrier::new(40, 40)),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_to_uncertified_peer_errors() {
    let sender = sender(1);
    let err = sender
        .send_to(
            99,
            &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn barrier_fan_out_attempts_all_peers_but_rejects_a_partial_cut() {
    let receiver = bind_on_loopback(10).await;
    let sender = sender(1);
    sender.register_peer(10, receiver.local_addr());
    let barrier = CheckpointBarrier::new(5, 5);

    let fence = assignment_fence(2, &[1, 10, 99]);
    receiver
        .install_assignment_fence(&fence, &assignment_owners(&[1, 10, 99]))
        .unwrap();
    sender
        .install_assignment_fence(&fence, &assignment_owners(&[1, 10, 99]))
        .unwrap();
    let error = sender
        .fan_out_barrier(&[99, 10], barrier, &fence)
        .await
        .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    let received = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
        .await
        .expect("reachable peer did not receive its barrier")
        .expect("shuffle receiver closed");
    assert!(matches!(
        received.message(),
        ShuffleMessage::Barrier(received) if *received == barrier
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn barrier_fan_out_does_not_mask_peer_failure_with_scope_cancellation() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sender = sender(1);
    sender.register_peer(2, listener.local_addr().unwrap());
    let fence = assignment_fence(2, &[1, 2, 99]);
    sender
        .install_assignment_fence(&fence, &assignment_owners(&[1, 2, 99]))
        .unwrap();
    let accepted = Arc::new(tokio::sync::Notify::new());
    let stalled_peer = {
        let accepted = Arc::clone(&accepted);
        tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            accepted.notify_one();
            std::future::pending::<()>().await;
        })
    };
    let fanout = sender.fan_out_barrier(&[2, 99], CheckpointBarrier::new(5, 5), &fence);
    tokio::pin!(fanout);
    tokio::select! {
        () = accepted.notified() => {}
        result = &mut fanout => panic!("fan-out completed before scope cancellation: {result:?}"),
    }

    sender.suspend_assignment_fence();
    let error = tokio::time::timeout(std::time::Duration::from_secs(1), fanout)
        .await
        .expect("scope cancellation did not release the stalled peer")
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    assert!(!is_scope_cancelled(&error));
    stalled_peer.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn barrier_fan_out_process_lease_expiry_preserves_remote_holdover() {
    let receiver = ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), Uuid::from_u128(3))
        .await
        .unwrap();
    receiver
        .install_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(60),
        )))
        .unwrap();

    let sender = ShuffleSender::new(1, Uuid::from_u128(2));
    let sender_deadline = Arc::new(LeaseDeadline::live_for(std::time::Duration::from_secs(60)));
    sender
        .install_process_lease_deadline(Arc::clone(&sender_deadline))
        .unwrap();
    let fence = assignment_fence(2, &[1, 2, 3]);
    let owners = assignment_owners(&[1, 2, 3]);
    receiver.install_assignment_fence(&fence, &owners).unwrap();
    sender.install_assignment_fence(&fence, &owners).unwrap();
    sender.register_peer(2, receiver.local_addr());

    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("held".into(), 0, one_row(7)),
        )
        .await
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    sender.register_peer(3, listener.local_addr().unwrap());
    let accepted = Arc::new(tokio::sync::Notify::new());
    let stalled_peer = {
        let accepted = Arc::clone(&accepted);
        tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            accepted.notify_one();
            std::future::pending::<()>().await;
        })
    };

    let barrier = CheckpointBarrier::new(5, 5);
    let fanout = sender.fan_out_barrier(&[2, 3], barrier, &fence);
    tokio::pin!(fanout);
    tokio::select! {
        () = accepted.notified() => {}
        result = &mut fanout => panic!("fan-out completed before peer 3 stalled: {result:?}"),
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            tokio::select! {
                result = &mut fanout => {
                    panic!("fan-out completed before process lease expiry: {result:?}");
                }
                () = tokio::time::sleep(std::time::Duration::from_millis(5)) => {
                    if receiver.stage_checkpointed_inbound() {
                        break;
                    }
                }
            }
        }
    })
    .await
    .expect("reachable peer did not stage its checkpoint cut");

    sender_deadline.fence();
    let error = tokio::time::timeout(std::time::Duration::from_secs(1), fanout)
        .await
        .expect("process lease expiry did not release the stalled fan-out")
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(error.to_string(), "shuffle process lease is no longer live");
    assert!(
        !is_scope_cancelled(&error),
        "process lease loss must enter recovery, not clean topology cancellation"
    );

    assert!(receiver.has_staged_checkpoint_barriers());
    let held = receiver.drain_checkpointed_holdover().unwrap();
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].0, "held");
    assert_eq!(
        held[0]
            .1
            .batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    let barriers = receiver.drain_staged_barriers();
    assert_eq!(barriers.len(), 1);
    assert!(matches!(
        barriers[0].message(),
        ShuffleMessage::Barrier(received) if *received == barrier
    ));
    stalled_peer.abort();
}

#[tokio::test]
async fn barrier_fan_out_rejects_an_incomplete_or_duplicate_roster() {
    let sender = sender(1);
    let fence = assignment_fence(1, &[1, 2]);
    let barrier = CheckpointBarrier::new(5, 5);

    for peers in [&[][..], &[2, 2][..]] {
        let error = sender
            .fan_out_barrier(peers, barrier, &fence)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}

#[tokio::test]
async fn barrier_fan_out_rejects_noncanonical_identity_before_peer_work() {
    let sender = sender(1);
    let fence = assignment_fence(1, &[1, 2]);

    let error = sender
        .fan_out_barrier(&[2], CheckpointBarrier::new(7, 8), &fence)
        .await
        .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(error.to_string(), NONCANONICAL_BARRIER);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_discovers_peer_address_from_kv() {
    use crate::cluster::control::{ClusterKv, InMemoryKv};
    use crate::cluster::discovery::NodeId;

    // Peer 2 binds for real; its address is seeded into peer 1's KV so the
    // KV-backed sender resolves it on first send without an explicit
    // `register_peer`. End-to-end delivery proves the discovery glue.
    let recv = bind_on_loopback(2).await;
    let kv = Arc::new(InMemoryKv::new(NodeId(1)));
    kv.seed(NodeId(2), SHUFFLE_ADDR_KEY, recv.local_addr().to_string());
    let sender = ShuffleSender::with_kv(1, kv as Arc<dyn ClusterKv>, Uuid::from_u128(2));
    sender.install_live_process_lease_for_test();
    let fence = assignment_fence(1, &[1, 2]);
    sender
        .install_assignment_fence(&fence, &assignment_owners(&[1, 2]))
        .unwrap();

    send_barrier(&sender, &[2], CheckpointBarrier::new(7, 7))
        .await
        .unwrap();
    let received = recv.recv().await.unwrap();
    assert_eq!(received.peer(), 1);
    assert_eq!(
        received.message(),
        &ShuffleMessage::Barrier(CheckpointBarrier::new(7, 7))
    );
}
