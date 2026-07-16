//! Connection-pool + handshake coverage for `ShuffleSender` /
//! `ShuffleReceiver` over loopback TCP.

#![cfg(feature = "cluster")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use laminar_core::checkpoint::{
    CheckpointAssignmentFence, CheckpointBarrier, CheckpointParticipant,
};
use laminar_core::shuffle::{ShuffleMessage, ShuffleReceiver, ShuffleSender};
use uuid::Uuid;

const ASSIGNMENT_OWNERS: [u64; 8] = [2; 8];

fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

fn batch(values: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
}

fn fragmented_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("y", DataType::Int64, false),
        Field::new("z", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        (0..3)
            .map(|_| Arc::new(Int64Array::from(vec![11; rows])) as ArrayRef)
            .collect(),
    )
    .unwrap()
}

fn assignment_fence() -> CheckpointAssignmentFence {
    CheckpointAssignmentFence::from_owner_map(
        1,
        &ASSIGNMENT_OWNERS,
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: Uuid::from_u128(1),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: Uuid::from_u128(2),
            },
        ],
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_exchange_data_bidirectionally() {
    // Node 1 and Node 2 each bind a receiver and construct a sender.
    let recv_a = ShuffleReceiver::bind(1, loopback(), Uuid::from_u128(1))
        .await
        .unwrap();
    let recv_b = ShuffleReceiver::bind(2, loopback(), Uuid::from_u128(2))
        .await
        .unwrap();
    let fence = assignment_fence();
    recv_a
        .install_assignment_fence(&fence, &ASSIGNMENT_OWNERS)
        .unwrap();
    recv_b
        .install_assignment_fence(&fence, &ASSIGNMENT_OWNERS)
        .unwrap();
    let addr_a = recv_a.local_addr();
    let addr_b = recv_b.local_addr();

    let send_a = ShuffleSender::new(1, Uuid::from_u128(1));
    let send_b = ShuffleSender::new(2, Uuid::from_u128(2));
    send_a
        .install_assignment_fence(&fence, &ASSIGNMENT_OWNERS)
        .unwrap();
    send_b
        .install_assignment_fence(&fence, &ASSIGNMENT_OWNERS)
        .unwrap();
    send_a.register_peer(2, addr_b).await;
    send_b.register_peer(1, addr_a).await;

    // A → B: three pre-routed batches.
    send_a
        .send_to(
            2,
            &ShuffleMessage::checkpointed("s".into(), 0, batch(vec![1, 2, 3])),
        )
        .await
        .unwrap();
    send_a
        .send_to(
            2,
            &ShuffleMessage::checkpointed("s".into(), 0, batch(vec![4, 5, 6])),
        )
        .await
        .unwrap();
    // B → A: one barrier proves the reverse stream is independently scoped.
    send_b
        .fan_out_barrier(&[1], CheckpointBarrier::new(2, 1), &assignment_fence())
        .await
        .unwrap();
    send_a
        .send_to(
            2,
            &ShuffleMessage::checkpointed("s".into(), 0, batch(vec![7, 8, 9])),
        )
        .await
        .unwrap();

    // Drain both receivers under a modest deadline — loopback is
    // near-instant, 2s is a huge safety margin.
    let mut from_a_to_b = Vec::new();
    for _ in 0..3 {
        let received = tokio::time::timeout(Duration::from_secs(2), recv_b.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.peer(), 1, "A's frames must carry peer=1");
        from_a_to_b.push(received.message().clone());
    }
    let received = tokio::time::timeout(Duration::from_secs(2), recv_a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.peer(), 2, "B's frame must carry peer=2");
    assert_eq!(
        received.message(),
        &ShuffleMessage::Barrier(CheckpointBarrier::new(2, 1))
    );

    // FIFO: the three A→B batches arrive in send order.
    let values: Vec<Vec<i64>> = from_a_to_b
        .into_iter()
        .map(|m| match m {
            ShuffleMessage::Data { batch: b, .. } => b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec(),
            other => panic!("expected data, got {other:?}"),
        })
        .collect();
    assert_eq!(values, vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unregistered_peer_returns_not_found() {
    let sender = ShuffleSender::new(1, Uuid::from_u128(1));
    let fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[1, 42],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: Uuid::from_u128(1),
            },
            CheckpointParticipant {
                node_id: 42,
                boot_incarnation: Uuid::from_u128(42),
            },
        ],
    )
    .unwrap();
    sender.install_assignment_fence(&fence, &[1, 42]).unwrap();
    let err = sender
        .send_to(
            42,
            &ShuffleMessage::checkpointed("stage".into(), 0, batch(vec![1])),
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fragmented_batch_is_delivered_before_its_barrier() {
    const ROWS: usize = 60_000;

    let receiver = ShuffleReceiver::bind(2, loopback(), Uuid::from_u128(2))
        .await
        .unwrap();
    let fence = assignment_fence();
    receiver
        .install_assignment_fence(&fence, &ASSIGNMENT_OWNERS)
        .unwrap();
    let sender = ShuffleSender::new(1, Uuid::from_u128(1));
    sender
        .install_assignment_fence(&fence, &ASSIGNMENT_OWNERS)
        .unwrap();
    sender.register_peer(2, receiver.local_addr()).await;

    sender
        .send_to(
            2,
            &ShuffleMessage::checkpointed("large".into(), 7, fragmented_batch(ROWS)),
        )
        .await
        .unwrap();
    sender
        .fan_out_barrier(&[2], CheckpointBarrier::new(9, 3), &assignment_fence())
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let ShuffleMessage::Data {
        stage,
        routed_vnodes,
        batch,
        ..
    } = received.message()
    else {
        panic!("fragmented data must precede its barrier");
    };
    assert_eq!(stage, "large");
    assert_eq!(&**routed_vnodes, &[7]);
    assert_eq!(batch.num_rows(), ROWS);

    let received = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        received.message(),
        &ShuffleMessage::Barrier(CheckpointBarrier::new(9, 3))
    );
    assert_eq!(
        receiver
            .delivery_loss_incidents()
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
}
