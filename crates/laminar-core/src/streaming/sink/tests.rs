use crate::streaming::source::create;
use crate::streaming::source::Record;
use crate::streaming::StreamingError;
use arrow::array::{Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

#[derive(Clone, Debug)]
struct TestEvent {
    id: i64,
    value: f64,
}

impl Record for TestEvent {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]))
    }

    fn to_record_batch(&self) -> RecordBatch {
        RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(Int64Array::from(vec![self.id])),
                Arc::new(Float64Array::from(vec![self.value])),
            ],
        )
        .unwrap()
    }
}

#[tokio::test]
async fn test_poll_empty() {
    let (_source, mut sink) = create::<TestEvent>(16);
    assert!(sink.poll().is_none());
    assert!(!sink.is_disconnected());
}

#[tokio::test]
async fn test_receives_in_push_order() {
    let (source, mut sink) = create::<TestEvent>(16);
    source.push(TestEvent { id: 1, value: 1.0 }).unwrap();
    source.push(TestEvent { id: 2, value: 2.0 }).unwrap();

    let first = sink.recv_async().await.unwrap();
    let second = sink.poll().unwrap();
    let id = |batch: &RecordBatch| {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!((id(&first), id(&second)), (1, 2));
}

#[tokio::test]
async fn test_full_queue_rejects_until_the_sink_receives() {
    let (source, mut sink) = create::<TestEvent>(4);
    for id in 0..4 {
        source.push(TestEvent { id, value: 0.0 }).unwrap();
    }
    assert_eq!(
        source.push(TestEvent { id: 4, value: 0.0 }),
        Err(StreamingError::ChannelFull)
    );
    assert!(sink.poll().is_some());
    source.push(TestEvent { id: 4, value: 0.0 }).unwrap();
}

#[tokio::test]
async fn test_disconnected_after_source_drop_and_drain() {
    let (source, mut sink) = create::<TestEvent>(16);
    source.push(TestEvent { id: 1, value: 1.0 }).unwrap();
    drop(source);

    assert!(sink.poll().is_some());
    assert!(!sink.is_disconnected());
    assert!(sink.poll().is_none());
    assert!(sink.is_disconnected());
    assert!(sink.recv_async().await.is_err());
}

#[tokio::test]
async fn test_sink_drop_disconnects_producers() {
    let (source, sink) = create::<TestEvent>(16);
    drop(sink);
    assert!(source.is_closed());
    assert_eq!(
        source.push(TestEvent { id: 1, value: 1.0 }),
        Err(StreamingError::Disconnected)
    );
}

#[tokio::test]
async fn test_schema() {
    let (_source, sink) = create::<TestEvent>(16);
    assert_eq!(sink.schema().fields().len(), 2);
}
