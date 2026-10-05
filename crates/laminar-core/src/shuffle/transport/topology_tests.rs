//! Same-process, same-assignment topology changes through the actual loopback gRPC fabric.

use super::*;
use futures::FutureExt;
use std::sync::atomic::Ordering;

fn target() -> ShuffleTopologyFence {
    ShuffleTopologyFence::new(2, [42; 32]).unwrap()
}

async fn fabrics() -> (
    ShuffleSender,
    ShuffleReceiver,
    ShuffleSender,
    ShuffleReceiver,
) {
    let local = bind_on_loopback(1).await;
    let remote = bind_on_loopback(2).await;
    let first = sender(1);
    let second = sender(2);
    first.register_peer(2, remote.local_addr());
    second.register_peer(1, local.local_addr());
    (first, local, second, remote)
}

async fn receive(receiver: &ShuffleReceiver) -> ReceivedShuffle {
    tokio::time::timeout(std::time::Duration::from_secs(5), receiver.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn queued_send_cannot_acquire_the_next_recovery_generation() {
    let (first, _local, _second, remote) = fabrics().await;
    let message = ShuffleMessage::checkpointed("stage".into(), 0, one_row(10));
    let old_plan = first.send_to_for_generation(2, 1, 0, None, &message);
    first.set_recovery_gen(1);
    remote.set_recovery_gen(1);
    let error = old_plan.await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    assert!(!shuffle_send_may_have_been_admitted(&error));
    assert!(remote.drain_checkpointed_staged().is_empty());
    first
        .send_to_for_generation(2, 1, 1, None, &message)
        .await
        .unwrap();
    let received = receive(&remote).await;
    assert_eq!(received.recovery_gen(), 1);
    assert_eq!(received.checkpoint_sequence(), 0);
    assert_eq!(received.message(), &message);
}

#[tokio::test]
async fn topology_transport_reconnect_and_identical_install_preserve_sequence_domain() {
    let (first, local, second, remote) = fabrics().await;
    let topology = target();
    assert!(first
        .install_topology_fence_pair(&local, None, topology)
        .unwrap());
    assert!(second
        .install_topology_fence_pair(&remote, None, topology)
        .unwrap());
    let message = ShuffleMessage::checkpointed("stage".into(), 0, one_row(10));
    first
        .send_to_for_topology(2, 1, Some(topology), &message)
        .await
        .unwrap();
    let received = receive(&remote).await;
    assert_eq!(received.topology_fence(), Some(topology));
    assert_eq!(received.checkpoint_sequence(), 0);
    assert!(!first
        .install_topology_fence_pair(&local, None, topology)
        .unwrap());
    assert!(!second
        .install_topology_fence_pair(&remote, None, topology)
        .unwrap());
    first.disconnect_peer_for_test(2);
    first
        .send_to_for_topology(
            2,
            1,
            Some(topology),
            &ShuffleMessage::Frontier {
                stage: "stage".into(),
                watermark: Some(15),
                idle: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(receive(&remote).await.checkpoint_sequence(), 1);
    first
        .fan_out_barrier_for_topology(
            &[2],
            CheckpointBarrier::new(7, 7),
            &assignment_fence(1, &[1, 2]),
            Some(topology),
        )
        .await
        .unwrap();
    let barrier = receive(&remote).await;
    assert_eq!(barrier.checkpoint_sequence(), 2);
    assert_eq!(barrier.topology_fence(), Some(topology));
    assert_eq!(remote.delivery_loss_incidents().load(Ordering::Acquire), 0);
    assert_eq!(first.assignment_version(), 1);
    assert_eq!(first.recovery_gen(), 0);
}

#[tokio::test]
async fn topology_transport_rejects_legacy_and_divergent_peer_handshakes() {
    let (first, local, second, remote) = fabrics().await;
    let topology = target();
    first
        .install_topology_fence_pair(&local, None, topology)
        .unwrap();
    let message = ShuffleMessage::checkpointed("stage".into(), 0, one_row(10));
    assert!(first.send_to(2, &message).await.is_err());
    assert!(first.send_to_for_assignment(2, 1, &message).await.is_err());
    assert!(first
        .send_to_for_topology(2, 1, Some(topology), &message)
        .await
        .is_err());
    assert!(second
        .send_to(
            1,
            &ShuffleMessage::checkpointed("stage".into(), 1, one_row(20))
        )
        .await
        .is_err());
    let divergent = ShuffleTopologyFence::new(2, [43; 32]).unwrap();
    second
        .install_topology_fence_pair(&remote, None, divergent)
        .unwrap();
    assert!(first
        .send_to_for_topology(2, 1, Some(topology), &message)
        .await
        .is_err());
    assert!(remote.drain_available().is_empty());
    assert_eq!(local.delivery_loss_incidents().load(Ordering::Acquire), 0);
    assert_eq!(remote.delivery_loss_incidents().load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn topology_transport_discards_queued_and_staged_predecessor_controls_and_batches() {
    let (first, local, second, remote) = fabrics().await;
    first
        .send_to(
            2,
            &ShuffleMessage::checkpointed("stage".into(), 0, one_row(10)),
        )
        .await
        .unwrap();
    first
        .send_to(
            2,
            &ShuffleMessage::Frontier {
                stage: "stage".into(),
                watermark: Some(11),
                idle: false,
            },
        )
        .await
        .unwrap();
    send_barrier(&first, &[2], CheckpointBarrier::new(1, 1))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while remote.barrier_arrivals_for_test() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Deliberately leave the old data staged; its frontier is observed but not consumed.
    assert!(!remote.stage_checkpointed_inbound());
    let topology = target();
    first
        .install_topology_fence_pair(&local, None, topology)
        .unwrap();
    second
        .install_topology_fence_pair(&remote, None, topology)
        .unwrap();
    assert!(remote.drain_checkpointed_staged().is_empty());
    assert!(remote.drain_staged_frontiers().is_empty());
    assert!(remote.drain_staged_barriers().is_empty());
    assert!(remote.drain_available().is_empty());
    first
        .send_to_for_topology(
            2,
            1,
            Some(topology),
            &ShuffleMessage::checkpointed("stage".into(), 0, one_row(20)),
        )
        .await
        .unwrap();
    let received = receive(&remote).await;
    assert_eq!(received.checkpoint_sequence(), 0);
    assert_eq!(received.topology_fence(), Some(topology));
    assert_eq!(remote.delivery_loss_incidents().load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn topology_transport_rotation_cancels_budget_blocked_old_sends() {
    let (first, local, second, remote) = fabrics().await;
    let budget = first.hold_outbound_budget_for_test(2).await.unwrap();
    let message = ShuffleMessage::checkpointed("stage".into(), 0, one_row(10));
    let send = first.send_to_for_assignment(2, 1, &message);
    tokio::pin!(send);
    assert!(send.as_mut().now_or_never().is_none());
    let topology = target();
    first
        .install_topology_fence_pair(&local, None, topology)
        .unwrap();
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), send)
        .await
        .unwrap()
        .unwrap_err();
    assert!(is_scope_cancelled(&error));
    drop(budget);
    second
        .install_topology_fence_pair(&remote, None, topology)
        .unwrap();
    first
        .send_to_for_topology(2, 1, Some(topology), &message)
        .await
        .unwrap();
    assert_eq!(receive(&remote).await.checkpoint_sequence(), 0);
    assert_eq!(remote.delivery_loss_incidents().load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn topology_transport_failed_pair_is_atomic_and_cannot_reactivate_inactive_assignment() {
    let (first, local, second, remote) = fabrics().await;
    assert!(first
        .install_topology_fence_pair(&remote, None, target())
        .is_err());
    assert_eq!(first.topology_version(), 0);
    assert_eq!(remote.topology_version(), 0);
    second.set_recovery_gen(1);
    assert!(second
        .install_topology_fence_pair(&remote, None, target())
        .is_err());
    assert_eq!(second.topology_fence(), None);
    assert_eq!(remote.topology_fence(), None);
    local.invalidate_assignment_fence();
    assert!(first
        .install_topology_fence_pair(&local, None, target())
        .is_err());
    assert_eq!(first.assignment_version(), 1);
    assert_eq!(local.assignment_version(), 0);
    assert_eq!(first.topology_fence(), None);
}

#[tokio::test]
async fn topology_transport_assignment_and_recovery_keep_topology_conflict_floor() {
    let (first, local, second, remote) = fabrics().await;
    let topology = target();
    first
        .install_topology_fence_pair(&local, None, topology)
        .unwrap();
    second
        .install_topology_fence_pair(&remote, None, topology)
        .unwrap();
    let conflict = ShuffleTopologyFence::new(2, [44; 32]).unwrap();
    let older = ShuffleTopologyFence::new(1, [41; 32]).unwrap();
    assert!(first
        .install_topology_fence_pair(&local, Some(topology), conflict)
        .is_err());
    assert!(first
        .install_topology_fence_pair(&local, Some(topology), older)
        .is_err());
    let assignment = assignment_fence(2, &[1, 2]);
    let owners = assignment_owners(&[1, 2]);
    for sender in [&first, &second] {
        sender
            .install_assignment_fence(&assignment, &owners)
            .unwrap();
        sender.set_recovery_gen(9);
    }
    for receiver in [&local, &remote] {
        receiver
            .install_assignment_fence(&assignment, &owners)
            .unwrap();
        receiver.set_recovery_gen(9);
    }
    assert_eq!(first.topology_fence(), Some(topology));
    assert_eq!(remote.topology_fence(), Some(topology));
    let message = ShuffleMessage::checkpointed("stage".into(), 0, one_row(20));
    assert!(first.send_to_for_assignment(2, 2, &message).await.is_err());
    first
        .send_to_for_topology(2, 2, Some(topology), &message)
        .await
        .unwrap();
    let received = receive(&remote).await;
    assert_eq!(received.assignment_version(), 2);
    assert_eq!(received.recovery_gen(), 9);
    assert_eq!(received.topology_fence(), Some(topology));
    local.suspend_assignment_fence();
    assert!(first
        .install_topology_fence_pair(&local, Some(topology), topology)
        .is_err());
    local
        .install_assignment_fence(&assignment, &owners)
        .unwrap();
    assert!(!first
        .install_topology_fence_pair(&local, None, topology)
        .unwrap());
    assert_eq!(local.topology_version(), 2);
}

#[tokio::test]
async fn topology_transport_unrepaired_loss_blocks_generation_installation() {
    let (first, _local, second, remote) = fabrics().await;
    first.burn_seq_for_test(2);
    first
        .send_to(
            2,
            &ShuffleMessage::checkpointed("stage".into(), 0, one_row(10)),
        )
        .await
        .unwrap();
    receive(&remote).await;
    assert!(remote.has_unrecovered_delivery_loss());
    assert!(second
        .install_topology_fence_pair(&remote, None, target())
        .is_err());
    assert_eq!(second.topology_version(), 0);
    assert_eq!(remote.topology_version(), 0);
    assert_eq!(remote.delivery_loss_incidents().load(Ordering::Acquire), 1);
    assert_eq!(
        remote
            .recovered_delivery_loss_incidents()
            .load(Ordering::Acquire),
        0
    );
}

#[tokio::test]
async fn topology_transport_recovery_install_retains_loss_until_exact_completion() {
    let (first, _local, second, remote) = fabrics().await;
    first.burn_seq_for_test(2);
    first
        .send_to(
            2,
            &ShuffleMessage::checkpointed("stage".into(), 0, one_row(10)),
        )
        .await
        .unwrap();
    receive(&remote).await;
    assert!(remote.has_unrecovered_delivery_loss());
    second.set_recovery_gen(1);
    remote.set_recovery_gen(1);
    for generation in [0, 2] {
        assert!(second
            .install_topology_fence_pair_for_recovery(&remote, None, target(), generation)
            .is_err());
    }
    assert!(second
        .install_topology_fence_pair(&remote, None, target())
        .is_err());
    assert_eq!(second.topology_version(), 0);
    assert!(second
        .install_topology_fence_pair_for_recovery(&remote, None, target(), 1)
        .unwrap());
    assert!(remote.has_unrecovered_delivery_loss());
    assert_eq!(
        remote
            .recovered_delivery_loss_incidents()
            .load(Ordering::Acquire),
        0
    );
    assert!(!second
        .install_topology_fence_pair_for_recovery(&remote, None, target(), 1)
        .unwrap());
    assert!(!remote.complete_recovery(2));
    assert!(remote.has_unrecovered_delivery_loss());
    assert!(remote.complete_recovery(1));
    assert!(!remote.has_unrecovered_delivery_loss());
    assert_eq!(
        remote
            .recovered_delivery_loss_incidents()
            .load(Ordering::Acquire),
        1
    );
}

#[tokio::test]
async fn topology_transport_recovery_install_rejects_late_loss_and_poisoned_counter() {
    for poisoned in [false, true] {
        let (_first, _local, second, remote) = fabrics().await;
        let incidents = remote.delivery_loss_incidents();
        incidents.store(if poisoned { u64::MAX } else { 1 }, Ordering::Release);
        second.set_recovery_gen(1);
        remote.set_recovery_gen(1);
        if !poisoned {
            incidents.fetch_add(1, Ordering::AcqRel);
        }
        assert!(second
            .install_topology_fence_pair_for_recovery(&remote, None, target(), 1)
            .is_err());
        assert_eq!(second.topology_version(), 0);
        assert_eq!(remote.topology_version(), 0);
        assert_eq!(
            remote
                .recovered_delivery_loss_incidents()
                .load(Ordering::Acquire),
            0
        );
        if poisoned {
            assert!(remote.complete_recovery(1));
            assert!(remote.has_unrecovered_delivery_loss());
            assert!(second
                .install_topology_fence_pair_for_recovery(&remote, None, target(), 1)
                .is_err());
        } else {
            second.set_recovery_gen(2);
            remote.set_recovery_gen(2);
            assert!(second
                .install_topology_fence_pair_for_recovery(&remote, None, target(), 1)
                .is_err());
            assert!(second
                .install_topology_fence_pair_for_recovery(&remote, None, target(), 2)
                .unwrap());
            assert!(remote.has_unrecovered_delivery_loss());
            assert!(!remote.complete_recovery(1));
            assert!(remote.complete_recovery(2));
            assert!(!remote.has_unrecovered_delivery_loss());
        }
    }
}

#[tokio::test]
async fn topology_transport_old_handshake_token_cannot_open_a_target_stream() {
    use super::super::shuffle_v1::shuffle_transport_client::ShuffleTransportClient;
    use super::super::shuffle_v1::{shuffle_frame, HandshakeRequest, Hello, ShuffleFrame};
    let (_first, _local, second, remote) = fabrics().await;
    let endpoint =
        crate::cluster::control::tls::client_endpoint(&remote.local_addr().to_string()).unwrap();
    let mut client = ShuffleTransportClient::new(endpoint.connect().await.unwrap());
    let stream_id = Uuid::new_v4();
    let assignment = assignment_fence(1, &[1, 2]);
    let response = client
        .handshake(tonic::Request::new(HandshakeRequest {
            sender_node_id: 1,
            sender_incarnation: Uuid::from_u128(2).as_bytes().to_vec(),
            stream_id: stream_id.as_bytes().to_vec(),
            assignment_version: 1,
            recovery_gen: 0,
            assignment_certificate_digest: assignment.digest().to_vec(),
            topology_version: 0,
            topology_manifest_sha256: Vec::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    second
        .install_topology_fence_pair(&remote, None, target())
        .unwrap();
    let hello = ShuffleFrame {
        kind: Some(shuffle_frame::Kind::Hello(Hello {
            node_id: 1,
            sender_incarnation: response.sender_incarnation,
            receiver_incarnation: response.receiver_incarnation,
            stream_id: response.stream_id,
            assignment_version: 1,
            recovery_gen: 0,
            assignment_certificate_digest: assignment.digest().to_vec(),
            topology_version: 0,
            topology_manifest_sha256: Vec::new(),
        })),
    };
    let error = client
        .shuffle(tonic::Request::new(futures::stream::iter([hello])))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(remote.drain_available().is_empty());
    assert_eq!(remote.delivery_loss_incidents().load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn topology_transport_expired_process_cannot_install_either_direction() {
    let local = bind_on_loopback(1).await;
    let sender = ShuffleSender::new(1, Uuid::from_u128(2));
    sender
        .install_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(1),
        )))
        .unwrap();
    sender
        .install_assignment_fence(&assignment_fence(1, &[1, 2]), &assignment_owners(&[1, 2]))
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert!(sender
        .install_topology_fence_pair(&local, None, target())
        .is_err());
    assert_eq!(sender.topology_fence(), None);
    assert_eq!(local.topology_fence(), None);
}
