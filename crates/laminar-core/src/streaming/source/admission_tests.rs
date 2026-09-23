use super::*;
use crate::streaming::{retained_arrow_bytes, MAX_SOURCE_QUEUED_BYTES};
use arrow::array::{Int64Array, ListArray, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};

#[derive(Clone, Debug)]
struct Payload(String);

impl Record for Payload {
    fn schema() -> SchemaRef {
        Arc::new(Schema::empty())
    }

    fn to_record_batch(&self) -> RecordBatch {
        RecordBatch::try_from_iter(vec![(
            "payload",
            Arc::new(StringArray::from(vec![self.0.as_str()])) as arrow::array::ArrayRef,
        )])
        .unwrap()
    }
}

fn source_with_limit(limit: usize, capacity: usize) -> (Source<Payload>, Sink<Payload>) {
    create_with_config(SourceConfig {
        max_queued_bytes: limit,
        channel: super::super::ChannelConfig::with_buffer_size(capacity),
        ..Default::default()
    })
}

#[tokio::test]
async fn byte_limit_survives_broadcast_and_slow_subscribers() {
    let batch = Payload("x".repeat(4096)).to_record_batch();
    let bytes = retained_arrow_bytes(&batch);
    let (source, sink) = source_with_limit(bytes, 16);
    let mut fast = sink.subscribe();
    let slow = sink.subscribe();
    source.push_arrow(batch.clone()).unwrap();
    assert_eq!(source.queued_arrow_bytes(), bytes);
    let received = fast.recv_async().await.unwrap();
    assert_eq!(source.pending(), 0);
    assert_eq!(
        source.push_arrow(batch.clone()),
        Err(StreamingError::ChannelFull)
    );
    assert_eq!(source.sequence(), 1);
    drop(slow);
    assert_eq!(source.queued_arrow_bytes(), 0);
    source.push_arrow(batch).unwrap();
    fast.recv_async().await.unwrap();
    assert_eq!(source.queued_arrow_bytes(), 0);
    // Returned batches belong to the caller, outside the queue reservation.
    assert_eq!(received.num_rows(), 1);
}

#[tokio::test]
async fn cloned_concurrent_producers_share_one_budget() {
    let batch = Payload("x".repeat(4096)).to_record_batch();
    let bytes = retained_arrow_bytes(&batch);
    let (source, sink) = source_with_limit(bytes * 8, 64);
    let mut subscription = sink.subscribe();
    let accepted = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let source = source.clone();
                let batch = batch.clone();
                scope.spawn(move || {
                    (0..8)
                        .filter(|_| source.push_arrow(batch.clone()).is_ok())
                        .count()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .sum::<usize>()
    });
    assert_eq!(accepted, 8);
    assert_eq!(source.sequence(), 8);
    assert_eq!(source.queued_arrow_bytes(), bytes * 8);
    for _ in 0..accepted {
        subscription.recv_async().await.unwrap();
    }
    assert_eq!(source.queued_arrow_bytes(), 0);
    source.push_arrow(batch).unwrap();
}

#[tokio::test]
async fn count_rejection_returns_arrow_charge() {
    let batch = Payload("x".repeat(32)).to_record_batch();
    let bytes = retained_arrow_bytes(&batch);
    let (source, sink) = source_with_limit(bytes * 16, 4);
    let mut subscription = sink.subscribe();
    for _ in 0..4 {
        source.push_arrow(batch.clone()).unwrap();
    }
    assert_eq!(
        source.push_arrow(batch.clone()),
        Err(StreamingError::ChannelFull)
    );
    assert_eq!(source.queued_arrow_bytes(), bytes * 4);
    assert_eq!(source.sequence(), 4);
    for _ in 0..4 {
        subscription.recv_async().await.unwrap();
    }
    assert_eq!(source.queued_arrow_bytes(), 0);
    source.push_arrow(batch).unwrap();
}

#[test]
fn runtime_cancellation_closes_admission_and_final_producer_drop_releases_charges() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let batch = Payload("x".repeat(4096)).to_record_batch();
    let bytes = retained_arrow_bytes(&batch);
    let (source, sink) = {
        let _guard = runtime.enter();
        source_with_limit(bytes, 16)
    };
    let budget = Arc::clone(&source.inner.arrow_budget);
    source.push_arrow(batch.clone()).unwrap();
    drop(runtime);
    assert!(source.is_closed());
    // Crossfire retains unread queue storage until its remaining producer handles drop.
    assert_eq!(source.pending(), 1);
    assert_eq!(source.queued_arrow_bytes(), bytes);
    assert_eq!(source.push_arrow(batch), Err(StreamingError::Disconnected));
    assert_eq!(source.queued_arrow_bytes(), bytes);
    assert_eq!(source.sequence(), 1);
    assert_eq!(
        source.try_push(Payload(String::new())).unwrap_err().error,
        StreamingError::Disconnected
    );
    assert_eq!(source.sequence(), 1);
    drop(source);
    assert_eq!(budget.available_permits(), bytes);
    drop(sink);
}

#[tokio::test]
async fn dropped_subscription_releases_bytes_after_cancelled_receive() {
    let batch = Payload("x".repeat(4096)).to_record_batch();
    let bytes = retained_arrow_bytes(&batch);
    let (source, sink) = source_with_limit(bytes, 16);
    let mut subscription = sink.subscribe();
    assert!(
        tokio::time::timeout(Duration::from_millis(1), subscription.recv_async())
            .await
            .is_err()
    );
    source.push_arrow(batch.clone()).unwrap();
    tokio::task::yield_now().await;
    assert_eq!(source.queued_arrow_bytes(), bytes);
    drop(subscription);
    assert_eq!(source.queued_arrow_bytes(), 0);
    source.push_arrow(batch).unwrap();
    tokio::task::yield_now().await;
    assert_eq!(source.queued_arrow_bytes(), 0);
}

#[tokio::test]
async fn broadcast_eviction_releases_only_the_evicted_batch() {
    let batch = Payload("x".repeat(32)).to_record_batch();
    let bytes = retained_arrow_bytes(&batch);
    let (source, sink) = source_with_limit(bytes * 4096, 4096);
    let subscription = sink.subscribe();
    for _ in 0..2049 {
        source.push_arrow(batch.clone()).unwrap();
    }
    while source.pending() != 0 {
        tokio::task::yield_now().await;
    }
    assert_eq!(source.queued_arrow_bytes(), bytes * 2048);
    drop(subscription);
    assert_eq!(source.queued_arrow_bytes(), 0);
}

#[tokio::test]
async fn source_and_sink_drop_preserve_accepted_tail_then_release_bytes() {
    let batch = Payload("x".repeat(32)).to_record_batch();
    let bytes = retained_arrow_bytes(&batch);
    let (source, sink) = source_with_limit(bytes * 2, 16);
    let budget = Arc::clone(&source.inner.arrow_budget);
    let mut subscription = sink.subscribe();
    source.push_arrow(batch.clone()).unwrap();
    source.push_arrow(batch).unwrap();
    drop(source);
    drop(sink);
    assert_eq!(subscription.recv_async().await.unwrap().num_rows(), 1);
    assert_eq!(subscription.recv_async().await.unwrap().num_rows(), 1);
    assert_eq!(budget.available_permits(), bytes * 2);
    assert!(matches!(
        subscription.recv_async().await,
        Err(crate::streaming::RecvError::Disconnected)
    ));
}

#[tokio::test]
async fn wide_slices_and_views_charge_retained_storage() {
    let text = "x".repeat(4096);
    let arrays: Vec<arrow::array::ArrayRef> = vec![
        Arc::new(StringArray::from(vec![text.as_str(); 8]).slice(0, 1)),
        Arc::new(StringViewArray::from(vec![text.as_str(); 8]).slice(0, 1)),
        Arc::new(Int64Array::from(vec![0; 8192]).slice(0, 1)),
        Arc::new(
            ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>([
                Some(vec![Some(1); 8192]),
                Some(vec![Some(2); 8192]),
            ])
            .slice(0, 1),
        ),
    ];
    for array in arrays {
        let batch = RecordBatch::try_from_iter(vec![("value", array)]).unwrap();
        let (source, _sink) = source_with_limit(1024, 16);
        assert!(matches!(
            source.push_arrow(batch),
            Err(StreamingError::BatchTooLarge { limit: 1024, .. })
        ));
        assert_eq!(source.sequence(), 0);
        assert_eq!(source.queued_arrow_bytes(), 0);
    }
}

#[tokio::test]
async fn invalid_arrow_limits_fail_without_panicking_or_admitting() {
    for limit in [0, MAX_SOURCE_QUEUED_BYTES + 1] {
        let (source, _sink) = source_with_limit(limit, 16);
        assert!(matches!(
            source.push_arrow(Payload("x".into()).to_record_batch()),
            Err(StreamingError::InvalidConfig(_))
        ));
        assert_eq!(source.sequence(), 0);
        assert_eq!(source.queued_arrow_bytes(), 0);
    }
}

#[tokio::test]
async fn generic_records_are_count_bounded_without_heap_size_estimation() {
    let (source, _sink) = source_with_limit(1, 4);
    for _ in 0..4 {
        source.push(Payload("x".repeat(4096))).unwrap();
    }
    assert_eq!(source.queued_arrow_bytes(), 0);
    assert_eq!(source.sequence(), 4);
    assert_eq!(
        source.push(Payload("x".repeat(4096))),
        Err(StreamingError::ChannelFull)
    );
}

#[tokio::test]
async fn failed_typed_push_does_not_advance_watermark() {
    let (source, _sink) = create::<super::tests::TestEvent>(4);
    for timestamp in 0..4 {
        source
            .push(super::tests::TestEvent {
                id: 0,
                value: 0.0,
                timestamp,
            })
            .unwrap();
    }
    let rejected = super::tests::TestEvent {
        id: 0,
        value: 0.0,
        timestamp: 1000,
    };
    assert_eq!(
        source.push(rejected.clone()),
        Err(StreamingError::ChannelFull)
    );
    assert_eq!(source.try_push(rejected).unwrap_err().value.timestamp, 1000);
    assert_eq!(source.current_watermark(), 3);
    assert_eq!(source.sequence(), 4);
}

#[tokio::test]
async fn schema_failure_does_not_reserve_bytes() {
    let (source, _sink) = create::<super::tests::TestEvent>(16);
    let batch = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
        "other",
        DataType::Int64,
        true,
    )])));
    assert!(matches!(
        source.push_arrow(batch),
        Err(StreamingError::SchemaMismatch { .. })
    ));
    assert_eq!(source.queued_arrow_bytes(), 0);
    assert_eq!(source.sequence(), 0);
}
