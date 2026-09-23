use super::*;
use crate::{LaminarConfig, LaminarDB};
use arrow::array::StringArray;
use laminar_core::streaming::{retained_arrow_bytes, Record};

#[derive(Clone)]
struct Payload(String);

impl Record for Payload {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "payload",
            arrow::datatypes::DataType::Utf8,
            true,
        )]))
    }

    fn to_record_batch(&self) -> RecordBatch {
        RecordBatch::try_new(
            Self::schema(),
            vec![Arc::new(StringArray::from(vec![self.0.as_str()]))],
        )
        .unwrap()
    }
}

fn entry(limit: usize) -> Arc<SourceEntry> {
    SourceCatalog::from_config(&LaminarConfig {
        push_source_max_bytes: limit,
        default_buffer_size: 16,
        ..Default::default()
    })
    .register_source("input", Payload::schema(), vec![], None, None, None, None)
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_budget_halt_remains_terminal_across_public_stop_and_start() {
    let db = LaminarDB::builder()
        .pipeline_max_input_buf_bytes(1)
        .build()
        .await
        .unwrap();
    db.execute("CREATE SOURCE input (payload VARCHAR)")
        .await
        .unwrap();
    db.execute("CREATE STREAM output AS SELECT payload FROM input")
        .await
        .unwrap();
    db.start().await.unwrap();
    db.source::<Payload>("input")
        .unwrap()
        .push(Payload("accepted at ingress".into()))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while db.pipeline_state() != "Faulted" {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("graph budget exhaustion must halt the running pipeline");
    db.stop_pipeline().await.unwrap();
    let reason = db.last_fault().expect("terminal reason");
    assert!(reason.contains("Graph input budget exceeded"), "{reason}");
    let error = db.start().await.unwrap_err();
    assert!(error.requires_pipeline_halt(), "{error}");
    assert_eq!(db.last_fault().as_deref(), Some(reason.as_str()));
    assert!(matches!(
        db.shutdown().await,
        Err(crate::DbError::Pipeline(message)) if message.contains(reason.as_str())
    ));
}

#[tokio::test]
async fn byte_rejection_preserves_sequence_snapshot_and_wakeup() {
    let batch = Payload("a".repeat(4096)).to_record_batch();
    let bytes = retained_arrow_bytes(&batch);
    let entry = entry(bytes);
    let mut subscription = entry.sink.subscribe();
    entry.push_and_buffer(batch.clone()).unwrap();
    entry.data_notify.notified().await;
    assert_eq!(
        entry.push_and_buffer(batch.clone()),
        Err(StreamingError::ChannelFull)
    );
    assert_eq!(entry.source.sequence(), 1);
    assert_eq!(entry.snapshot(), vec![batch]);
    assert!(
        tokio::time::timeout(Duration::from_millis(1), entry.data_notify.notified())
            .await
            .is_err()
    );
    subscription.recv_async().await.unwrap();
    let next = Payload("b".repeat(4096)).to_record_batch();
    entry.push_and_buffer(next.clone()).unwrap();
    assert_eq!(entry.source.sequence(), 2);
    assert_eq!(entry.snapshot(), vec![next]);
}

#[tokio::test]
async fn snapshot_history_evicts_by_bytes_after_broadcast_releases_input() {
    let batch = Payload("a".repeat(4096)).to_record_batch();
    let bytes = retained_arrow_bytes(&batch);
    let entry = entry(bytes * 2);
    let mut subscription = entry.sink.subscribe();
    entry.push_and_buffer(batch.clone()).unwrap();
    subscription.recv_async().await.unwrap();
    let external_snapshot = entry.snapshot();
    for value in ["b", "c", "d"] {
        entry
            .push_and_buffer(Payload(value.repeat(4096)).to_record_batch())
            .unwrap();
        subscription.recv_async().await.unwrap();
        assert!(
            entry
                .snapshot()
                .iter()
                .map(retained_arrow_bytes)
                .sum::<usize>()
                <= bytes * 2
        );
    }
    assert_eq!(entry.source.queued_arrow_bytes(), 0);
    assert_eq!(entry.snapshot().len(), 2);
    assert_eq!(
        entry.snapshot()[0],
        Payload("c".repeat(4096)).to_record_batch()
    );
    assert_eq!(external_snapshot, vec![batch]);
}

#[tokio::test]
async fn concurrent_db_handles_cannot_oversubscribe_or_publish_rejected_snapshots() {
    let batch = Payload("x".repeat(4096)).to_record_batch();
    let bytes = retained_arrow_bytes(&batch);
    let entry = entry(bytes * 4);
    let mut subscription = entry.sink.subscribe();
    let accepted = std::thread::scope(|scope| {
        let producers: Vec<_> = (0..16)
            .map(|index| {
                let entry = Arc::clone(&entry);
                let batch = Payload(format!("{index:0>4096}")).to_record_batch();
                scope.spawn(move || {
                    crate::UntypedSourceHandle::new(entry)
                        .push_arrow(batch)
                        .is_ok()
                })
            })
            .collect();
        producers
            .into_iter()
            .map(|producer| usize::from(producer.join().unwrap()))
            .sum::<usize>()
    });
    assert_eq!(accepted, 4);
    assert_eq!(entry.source.sequence(), 4);
    assert_eq!(entry.snapshot().len(), 4);
    assert_eq!(entry.source.queued_arrow_bytes(), bytes * 4);
    let snapshot = entry.snapshot();
    for expected in snapshot {
        assert_eq!(subscription.recv_async().await.unwrap(), expected);
    }
    assert_eq!(entry.source.queued_arrow_bytes(), 0);
}

#[tokio::test]
async fn typed_variable_width_records_and_untyped_handles_use_db_limit() {
    let small = Payload("x".repeat(256));
    let bytes = retained_arrow_bytes(&small.to_record_batch());
    let db = LaminarDB::builder()
        .push_source_max_bytes(bytes * 2)
        .build()
        .await
        .unwrap();
    db.execute("CREATE SOURCE input (payload VARCHAR)")
        .await
        .unwrap();
    let typed = db.source::<Payload>("input").unwrap();
    let untyped = db.source_untyped("input").unwrap();
    let oversized = Payload("x".repeat(4096));
    assert!(matches!(
        typed.push(oversized.clone()),
        Err(StreamingError::BatchTooLarge { .. })
    ));
    assert!(matches!(
        untyped.push_arrow(oversized.to_record_batch()),
        Err(StreamingError::BatchTooLarge { .. })
    ));
    assert_eq!(typed.push_batch(vec![oversized; 2]), 0);
    let entry = db.catalog.get_source("input").unwrap();
    assert_eq!(entry.source.sequence(), 0);
    assert!(entry.snapshot().is_empty());
    typed.push(small.clone()).unwrap();
    untyped.push_arrow(small.to_record_batch()).unwrap();
    assert_eq!(typed.push(small), Err(StreamingError::ChannelFull));
    assert_eq!(entry.source.sequence(), 2);
    assert_eq!(entry.snapshot().len(), 2);
    assert!(typed.is_backpressured());
    assert!(untyped.is_backpressured());
    assert!(db.source_metrics("input").unwrap().is_backpressured);
}

#[tokio::test]
async fn query_output_has_separate_ownership_from_push_input() {
    let db = LaminarDB::builder()
        .push_source_max_bytes(1)
        .build()
        .await
        .unwrap();
    let mut query = db
        .execute("SELECT repeat('x', 67108865) AS payload")
        .await
        .unwrap()
        .into_query()
        .unwrap();
    let mut subscription = query.subscribe_raw().unwrap();
    let batch = tokio::time::timeout(Duration::from_secs(5), subscription.recv_async())
        .await
        .unwrap()
        .unwrap();
    assert!(retained_arrow_bytes(&batch) > streaming::DEFAULT_SOURCE_MAX_QUEUED_BYTES);
    assert_eq!(batch.num_rows(), 1);
}

#[tokio::test]
#[cfg(feature = "api")]
async fn writer_flush_and_close_do_not_release_queued_input_early() {
    let batch = Payload("x".repeat(4096)).to_record_batch();
    let bytes = retained_arrow_bytes(&batch);
    let entry = entry(bytes);
    let mut subscription = entry.sink.subscribe();
    let mut writer = crate::api::Writer::new(crate::UntypedSourceHandle::new(Arc::clone(&entry)));
    writer.write(batch.clone()).unwrap();
    writer.flush().unwrap();
    assert!(writer.write(batch).is_err());
    assert_eq!(entry.source.queued_arrow_bytes(), bytes);
    writer.close().unwrap();
    assert_eq!(entry.source.queued_arrow_bytes(), bytes);
    subscription.recv_async().await.unwrap();
    assert_eq!(entry.source.queued_arrow_bytes(), 0);
    assert_eq!(entry.snapshot().len(), 1);
}
