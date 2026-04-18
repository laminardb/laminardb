//! Prove the shuffle codec + credit-flow primitives compose over
//! real TCP. Phase C.3 validated message framing on in-memory
//! duplex; this file binds a real `TcpListener` on loopback,
//! exchanges `ShuffleMessage`s in both directions, and confirms:
//!
//! - Framed encode/decode works over a real socket
//! - Back-pressure via `CreditSemaphore` blocks the sender when
//!   credit is exhausted and unblocks on `grant`
//! - Ordering is FIFO per (sender, receiver) as the design doc
//!   claims
//! - Graceful `Close` round-trips before the reader sees EOF
//!
//! The higher-level `ShuffleSender` / `ShuffleReceiver` types (one
//! per instance, pool of peers, hello handshake) land with the
//! distributed aggregate operator. These tests are the floor those
//! types will stand on.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use laminar_core::checkpoint::barrier::CheckpointBarrier;
use laminar_core::shuffle::{read_message, write_message, CreditSemaphore, ShuffleMessage};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};

fn sample_batch(values: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
}

/// Read at most `n` messages from `reader`. Short-circuits on EOF
/// (an `UnexpectedEof` error from the codec) so callers can treat
/// stream-closed as a normal termination.
async fn read_upto<R>(reader: &mut R, n: usize) -> Vec<ShuffleMessage>
where
    R: AsyncRead + Unpin,
{
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        match read_message(reader).await {
            Ok(msg) => out.push(msg),
            Err(_) => break,
        }
    }
    out
}

async fn send_all<W>(writer: &mut W, msgs: impl IntoIterator<Item = ShuffleMessage>)
where
    W: AsyncWrite + Unpin,
{
    for m in msgs {
        write_message(writer, &m).await.expect("send");
    }
}

/// Bind a loopback `TcpListener` on an ephemeral port and return
/// `(listener, addr)`. Callers use `addr` from the client side to
/// connect.
async fn bind_loopback() -> (TcpListener, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("tcp bind");
    let addr = listener.local_addr().expect("local_addr");
    (listener, addr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_data_roundtrip() {
    let (listener, addr) = bind_loopback().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        let (mut r, _w) = sock.split();
        read_upto(&mut r, 3).await
    });

    let mut client = TcpStream::connect(addr).await.expect("connect");
    let (_r, mut w) = client.split();
    send_all(
        &mut w,
        [
            ShuffleMessage::Data(sample_batch(vec![1, 2, 3])),
            ShuffleMessage::Data(sample_batch(vec![4, 5, 6])),
            ShuffleMessage::Data(sample_batch(vec![7, 8, 9])),
        ],
    )
    .await;

    // Drop client → server EOFs after the third message.
    drop(client);

    let received = server.await.expect("server task");
    assert_eq!(received.len(), 3, "all three batches delivered");
    for (i, msg) in received.iter().enumerate() {
        match msg {
            ShuffleMessage::Data(b) => {
                let expected: Vec<i64> = (1..=3).map(|x| x + (i as i64) * 3).collect();
                let col = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let got: Vec<i64> = col.values().to_vec();
                assert_eq!(got, expected, "frame {i} mismatch");
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_preserves_barrier_and_close_ordering() {
    // FIFO claim from the shuffle-protocol doc §3: "all events for a
    // given key traverse the same connection in order". Regression
    // guard that mixing Data + Barrier + Credit + Close on one
    // connection preserves the order we wrote.
    let (listener, addr) = bind_loopback().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        let (mut r, _w) = sock.split();
        read_upto(&mut r, 4).await
    });

    let mut client = TcpStream::connect(addr).await.expect("connect");
    let (_r, mut w) = client.split();
    let barrier = CheckpointBarrier::new(42, 7);
    send_all(
        &mut w,
        [
            ShuffleMessage::Data(sample_batch(vec![10])),
            ShuffleMessage::Barrier(barrier),
            ShuffleMessage::Credit(64 * 1024),
            ShuffleMessage::Close("test done".into()),
        ],
    )
    .await;
    drop(client);

    let got = server.await.expect("server");
    assert_eq!(got.len(), 4);
    assert!(matches!(got[0], ShuffleMessage::Data(_)));
    assert_eq!(got[1], ShuffleMessage::Barrier(barrier));
    assert_eq!(got[2], ShuffleMessage::Credit(64 * 1024));
    match &got[3] {
        ShuffleMessage::Close(reason) => assert_eq!(reason, "test done"),
        other => panic!("expected Close, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bidirectional_exchange_over_one_connection() {
    // Prove the codec isn't accidentally sender-directional — both
    // halves of the same TcpStream carry their own FIFO streams.
    let (listener, addr) = bind_loopback().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        let (mut r, mut w) = sock.split();
        // Read one, respond with one, read one more, respond with
        // Close.
        let first = read_message(&mut r).await.expect("first");
        write_message(&mut w, &ShuffleMessage::Credit(1024))
            .await
            .expect("grant");
        let second = read_message(&mut r).await.expect("second");
        write_message(&mut w, &ShuffleMessage::Close("bye".into()))
            .await
            .expect("close");
        (first, second)
    });

    let mut client = TcpStream::connect(addr).await.expect("connect");
    let (mut cr, mut cw) = client.split();

    write_message(&mut cw, &ShuffleMessage::Data(sample_batch(vec![1])))
        .await
        .expect("send 1");
    let grant = read_message(&mut cr).await.expect("grant");
    assert_eq!(grant, ShuffleMessage::Credit(1024));

    write_message(&mut cw, &ShuffleMessage::Data(sample_batch(vec![2])))
        .await
        .expect("send 2");
    let close = read_message(&mut cr).await.expect("close");
    assert_eq!(close, ShuffleMessage::Close("bye".into()));

    let (a, b) = server.await.expect("server");
    assert!(matches!(a, ShuffleMessage::Data(_)));
    assert!(matches!(b, ShuffleMessage::Data(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn credit_semaphore_back_pressure_against_tcp() {
    // Simulates the sender side of the credit-flow protocol:
    //   1. Bind a listener that intentionally does not read
    //      promptly.
    //   2. Sender exhausts its credit, then blocks on the
    //      semaphore.
    //   3. Test grants more credit → sender unblocks and sends
    //      the remaining data.
    //
    // TCP socket buffers will eventually back-pressure too, but the
    // CreditSemaphore fires first at the app layer — that's the
    // point of app-level credit.
    let (listener, addr) = bind_loopback().await;

    // Server drains slowly: accept, then sleep, then read.
    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (mut r, _w) = sock.split();
        read_upto(&mut r, 4).await
    });

    let mut client = TcpStream::connect(addr).await.expect("connect");
    let (_r, mut w) = client.split();

    // Small initial credit — exhausted by two smallish batches.
    let credit = CreditSemaphore::new(1024);
    assert_eq!(credit.available(), 1024);

    // First two messages: acquire 512 each, send.
    for i in 0..2 {
        credit.acquire(512).await.expect("credit");
        write_message(&mut w, &ShuffleMessage::Data(sample_batch(vec![i]))).await.unwrap();
    }
    assert_eq!(credit.available(), 0);

    // Third acquire blocks until we grant.
    let credit2 = credit.clone();
    let waiter = tokio::spawn(async move { credit2.acquire(512).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiter.is_finished(), "sender must be credit-blocked");

    // Grant from the test (simulating a Credit message arriving
    // from the receiver), sender unblocks.
    credit.grant(1024);
    waiter.await.unwrap().expect("unblocked acquire");
    write_message(&mut w, &ShuffleMessage::Data(sample_batch(vec![2])))
        .await
        .unwrap();

    // One more with remaining credit.
    credit.acquire(256).await.unwrap();
    write_message(&mut w, &ShuffleMessage::Data(sample_batch(vec![3])))
        .await
        .unwrap();

    drop(client);
    let got = server.await.expect("server");
    assert_eq!(got.len(), 4, "receiver saw all batches after unblock");
}
