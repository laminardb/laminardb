use std::time::Duration;

use super::*;

fn row(value: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({ "value": value })).unwrap()
}

fn payload(message: &Utf8Bytes) -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(message.as_ref()).unwrap()
}

#[tokio::test]
async fn one_shared_publish_reaches_every_subscriber() {
    let manager = FanoutManager::with_limits(1024, 4);
    let (_first_id, mut first) = manager.subscribe();
    let (_second_id, mut second) = manager.subscribe();

    let result = manager.publish_rows(&[row("hello")]).unwrap();

    assert_eq!(result.frames, 1);
    assert_eq!(result.receiver_enqueues, 2);
    assert_eq!(
        payload(&first.recv().await.unwrap())["data"][0]["value"],
        "hello"
    );
    assert_eq!(payload(&second.recv().await.unwrap())["sequence"], 1);
}

#[tokio::test]
async fn multi_frame_batch_does_not_evict_itself() {
    let rows = [row("a"), row("b"), row("c")];
    let two_row_bytes = encode_data_frame(&rows[..2], u64::MAX).unwrap().len();
    let manager = FanoutManager::with_limits(two_row_bytes, 2);
    let (_id, mut receiver) = manager.subscribe();

    let result = manager.publish_rows(&rows).unwrap();

    assert_eq!(result.frames, 2);
    assert_eq!(payload(&receiver.recv().await.unwrap())["sequence"], 1);
    assert_eq!(payload(&receiver.recv().await.unwrap())["sequence"], 2);
}

#[tokio::test]
async fn oversized_row_fails_before_delivery_or_sequence_reservation() {
    let rows = [row("small"), row(&"x".repeat(128))];
    let first_row_bytes = encode_data_frame(&rows[..1], u64::MAX).unwrap().len();
    let manager = FanoutManager::with_limits(first_row_bytes, 2);
    let (_id, mut receiver) = manager.subscribe();

    let error = manager.publish_rows(&rows).unwrap_err().to_string();

    assert!(error.contains("row 1"), "{error}");
    assert_eq!(manager.sequence.load(Ordering::Relaxed), 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), receiver.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn over_capacity_burst_fails_before_delivery() {
    let rows = [row("a"), row("b"), row("c")];
    let one_row_bytes = encode_data_frame(&rows[..1], u64::MAX).unwrap().len();
    let manager = FanoutManager::with_limits(one_row_bytes, 2);
    let (_id, mut receiver) = manager.subscribe();

    let error = manager.publish_rows(&rows).unwrap_err().to_string();

    assert!(error.contains("more than 2 frames"), "{error}");
    assert_eq!(manager.sequence.load(Ordering::Relaxed), 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), receiver.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn lag_is_reported_by_the_shared_ring() {
    let manager = FanoutManager::with_limits(1024, 2);
    let (_id, mut receiver) = manager.subscribe();
    manager.publish_rows(&[row("a")]).unwrap();
    manager.publish_rows(&[row("b")]).unwrap();
    manager.publish_rows(&[row("c")]).unwrap();

    assert!(matches!(
        receiver.recv().await,
        Err(broadcast::error::RecvError::Lagged(1))
    ));
}
