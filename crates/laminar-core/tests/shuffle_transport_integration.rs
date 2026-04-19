//! Connection-pool + handshake coverage for `ShuffleSender` /
//! `ShuffleReceiver` over loopback TCP.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use laminar_core::shuffle::{ShuffleMessage, ShuffleReceiver, ShuffleSender};

fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

fn batch(values: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_exchange_data_bidirectionally() {
    // Node 1 and Node 2 each bind a receiver and construct a sender.
    let recv_a = ShuffleReceiver::bind(1, loopback()).await.unwrap();
    let recv_b = ShuffleReceiver::bind(2, loopback()).await.unwrap();
    let addr_a = recv_a.local_addr();
    let addr_b = recv_b.local_addr();

    let send_a = ShuffleSender::new(1);
    let send_b = ShuffleSender::new(2);
    send_a.register_peer(2, addr_b).await;
    send_b.register_peer(1, addr_a).await;

    // A → B: three pre-routed batches.
    send_a
        .send_to(2, &ShuffleMessage::VnodeData(0, batch(vec![1, 2, 3])))
        .await
        .unwrap();
    send_a
        .send_to(2, &ShuffleMessage::VnodeData(0, batch(vec![4, 5, 6])))
        .await
        .unwrap();
    // B → A: one Hello (handshake already happened — this is just a
    // reachable frame in the reverse direction).
    send_b
        .send_to(1, &ShuffleMessage::Hello(2))
        .await
        .unwrap();
    send_a
        .send_to(2, &ShuffleMessage::VnodeData(0, batch(vec![7, 8, 9])))
        .await
        .unwrap();

    // Drain both receivers under a modest deadline — loopback is
    // near-instant, 2s is a huge safety margin.
    let mut from_a_to_b = Vec::new();
    for _ in 0..3 {
        let (from, msg) = tokio::time::timeout(Duration::from_secs(2), recv_b.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(from, 1, "A's frames must carry peer=1");
        from_a_to_b.push(msg);
    }
    let (from, hello) = tokio::time::timeout(Duration::from_secs(2), recv_a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(from, 2, "B's frame must carry peer=2");
    assert_eq!(hello, ShuffleMessage::Hello(2));

    // FIFO: the three A→B batches arrive in send order.
    let values: Vec<Vec<i64>> = from_a_to_b
        .into_iter()
        .map(|m| match m {
            ShuffleMessage::VnodeData(_, b) => b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec(),
            other => panic!("expected VnodeData, got {other:?}"),
        })
        .collect();
    assert_eq!(values, vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unregistered_peer_returns_not_found() {
    let sender = ShuffleSender::new(1);
    let err = sender
        .send_to(42, &ShuffleMessage::Hello(1))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}
