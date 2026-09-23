use super::*;
use arrow_array::{ArrayRef, BinaryArray, Int64Array, StringViewArray};
use laminar_connectors::{checkpoint::SourceCheckpoint, connector::SourceBatchCursor};
use std::time::Duration;

fn batch() -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1; 32])) as ArrayRef)])
        .unwrap()
}

fn message(source_idx: usize) -> SourceMsg {
    SourceMsg::Batch {
        source_idx,
        batch: batch(),
        cursor: SourceBatchCursor::Complete(SourceCheckpoint::new()),
    }
}

fn charged_bytes() -> usize {
    let (tx, _) = channel(1, 64 * 1024 * 1024);
    tx.batch_bytes(&batch()).unwrap() as usize
}

#[tokio::test]
async fn parked_message_keeps_shared_bytes_until_staging_or_discard() {
    let bytes = charged_bytes();
    let (tx, rx) = channel(8, bytes);
    tx.send(message(0)).await.unwrap();
    let parked = rx.recv().await.unwrap();
    let other_source = tx.clone();
    assert!(matches!(
        other_source.try_send(message(1)),
        Err(SourceQueueError::Full)
    ));
    assert_eq!(tx.budget.available_permits(), 0);
    let waiting = other_source.send(message(1));
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut waiting)
            .await
            .is_err()
    );
    drop(parked);
    tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap();
    let admitted = rx.recv().await.unwrap();
    assert!(matches!(
        admitted.message,
        SourceMsg::Batch { source_idx: 1, .. }
    ));
    drop(admitted);
    assert_eq!(tx.budget.available_permits(), bytes);
}

#[tokio::test]
async fn barriers_keep_fifo_and_bypass_saturated_bytes_and_byte_waiters() {
    let bytes = charged_bytes();
    let (tx, rx) = channel(4, bytes);
    tx.send(message(0)).await.unwrap();
    let waiting = tx.send(message(1));
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut waiting)
            .await
            .is_err()
    );
    tx.send(SourceMsg::Barrier {
        source_idx: 0,
        barrier: laminar_core::checkpoint::CheckpointBarrier::new(1, 1),
        checkpoint: SourceCheckpoint::new(),
    })
    .await
    .unwrap();
    let first = rx.recv().await.unwrap();
    assert!(matches!(
        first.message,
        SourceMsg::Batch { source_idx: 0, .. }
    ));
    let second = rx.recv().await.unwrap();
    assert!(matches!(
        second.message,
        SourceMsg::Barrier { source_idx: 0, .. }
    ));
    assert_eq!(tx.budget.available_permits(), 0);
    drop(first);
    waiting.await.unwrap();
    drop(rx.recv().await.unwrap());
    assert_eq!(tx.budget.available_permits(), bytes);
}

#[tokio::test]
async fn cancellation_and_failed_tail_sends_refund_count_and_byte_admission() {
    let bytes = charged_bytes();
    let (tx, rx) = channel(1, bytes * 2);
    tx.send(message(0)).await.unwrap();
    assert!(matches!(
        tx.try_send(message(1)),
        Err(SourceQueueError::Full)
    ));
    assert_eq!(tx.budget.available_permits(), bytes);
    {
        let waiting = tx.send(message(1));
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut waiting)
                .await
                .is_err()
        );
        assert_eq!(tx.budget.available_permits(), 0);
    }
    assert_eq!(tx.budget.available_permits(), bytes);
    drop(rx.recv().await.unwrap());
    assert_eq!(tx.budget.available_permits(), bytes * 2);
    tx.try_send(message(1)).unwrap();
    drop(rx.recv().await.unwrap());
    assert_eq!(tx.budget.available_permits(), bytes * 2);
}

#[tokio::test]
async fn receiver_drop_wakes_byte_waiter_even_with_a_live_parked_message() {
    let bytes = charged_bytes();
    let (tx, rx) = channel(4, bytes);
    tx.send(message(0)).await.unwrap();
    let parked = rx.recv().await.unwrap();
    let waiting = tx.send(message(1));
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut waiting)
            .await
            .is_err()
    );
    drop(rx);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap(),
        Err(SourceQueueError::Closed)
    ));
    drop(parked);
    assert_eq!(tx.budget.available_permits(), bytes);
}

#[tokio::test]
async fn oversized_batches_fail_promptly_without_acquiring_or_settling() {
    let (tx, rx) = channel(8, charged_bytes() - 1);
    assert!(matches!(
        tx.try_send(message(0)),
        Err(SourceQueueError::Oversized { .. })
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), tx.send(message(0)))
            .await
            .unwrap(),
        Err(SourceQueueError::Oversized { .. })
    ));
    assert!(matches!(rx.try_recv(), Err(crossfire::TryRecvError::Empty)));
    assert_eq!(tx.budget.available_permits(), charged_bytes() - 1);
}

#[tokio::test]
async fn shutdown_cancels_a_partial_byte_reservation_without_leaking_capacity() {
    let bytes = charged_bytes();
    let limit = bytes + bytes / 2;
    let (tx, rx) = channel(8, limit);
    tx.send(message(0)).await.unwrap();
    let shutdown = tokio::sync::Notify::new();
    {
        let waiting = super::super::send_source_msg(
            &tx,
            message(1),
            &shutdown,
            #[cfg(feature = "cluster")]
            None,
        );
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut waiting)
                .await
                .is_err()
        );
        shutdown.notify_one();
        assert!(!tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap());
    }
    assert_eq!(tx.budget.available_permits(), bytes / 2);
    drop(rx.recv().await.unwrap());
    assert_eq!(tx.budget.available_permits(), limit);
    assert!(matches!(rx.try_recv(), Err(crossfire::TryRecvError::Empty)));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn lease_loss_cancels_byte_admission_before_source_publication() {
    use super::super::SourceProcessAuthority;
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv, LeaseDeadline};

    let node_id = laminar_core::state::NodeId(34);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
    let (_members, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node_id, kv, None, members_rx));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    let authority = SourceProcessAuthority::new(Arc::clone(&controller));
    let bytes = charged_bytes();
    let (tx, rx) = channel(8, bytes);
    tx.send(message(0)).await.unwrap();
    let shutdown = tokio::sync::Notify::new();
    {
        let waiting = super::super::send_source_msg(&tx, message(1), &shutdown, Some(&authority));
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut waiting)
                .await
                .is_err()
        );
        controller.fence_process_lease();
        assert!(!tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap());
    }
    drop(rx.recv().await.unwrap());
    assert_eq!(tx.budget.available_permits(), bytes);
    assert!(matches!(rx.try_recv(), Err(crossfire::TryRecvError::Empty)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_sources_stay_bounded_and_drain_without_lost_batches() {
    let bytes = charged_bytes();
    let (tx, rx) = channel(8, bytes * 2);
    let mut producers = Vec::new();
    for source in 0..4 {
        let sender = tx.clone();
        producers.push(tokio::spawn(async move {
            for _ in 0..32 {
                sender.send(message(source)).await.unwrap();
            }
        }));
    }
    let mut counts = [0; 4];
    for _ in 0..128 {
        let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(tx.budget.available_permits() <= bytes);
        if let SourceMsg::Batch { source_idx, .. } = received.message {
            counts[source_idx] += 1;
        }
        // Emulate a slow consumer with a dequeued batch still retained.
        tokio::task::yield_now().await;
        drop(received);
    }
    for producer in producers {
        producer.await.unwrap();
    }
    assert_eq!(counts, [32; 4]);
    assert_eq!(tx.budget.available_permits(), bytes * 2);
}

#[test]
fn wide_slices_and_view_backing_storage_are_charged_not_just_visible_rows() {
    let payload = vec![b'x'; 64 * 1024];
    let wide = RecordBatch::try_from_iter([(
        "payload",
        Arc::new(BinaryArray::from_vec(vec![payload.as_slice(); 4])) as ArrayRef,
    )])
    .unwrap();
    let (tx, _) = channel(1, 128 * 1024);
    assert!(matches!(
        tx.batch_bytes(&wide.slice(0, 1)),
        Err(SourceQueueError::Oversized { .. })
    ));
    let text = "y".repeat(64 * 1024);
    let views = RecordBatch::try_from_iter([(
        "payload",
        Arc::new(StringViewArray::from(vec![text.as_str(); 4])) as ArrayRef,
    )])
    .unwrap();
    assert!(matches!(
        tx.batch_bytes(&views.slice(0, 1)),
        Err(SourceQueueError::Oversized { .. })
    ));
}
