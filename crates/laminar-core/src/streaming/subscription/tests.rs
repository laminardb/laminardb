use super::*;
use crate::streaming::source::create;
use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};

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
    let (_source, sink) = create::<TestEvent>(16);
    let mut sub = sink.subscribe();
    assert!(sub.poll().is_none());
}

#[tokio::test]
async fn test_single_subscriber_async() {
    let (source, sink) = create::<TestEvent>(16);
    let mut sub = sink.subscribe();

    source.push(TestEvent { id: 1, value: 1.0 }).unwrap();
    let batch = sub.recv_async().await.unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[tokio::test]
async fn test_multiple_subscribers_all_receive() {
    let (source, sink) = create::<TestEvent>(16);
    let mut sub1 = sink.subscribe();
    let mut sub2 = sink.subscribe();

    source.push(TestEvent { id: 1, value: 1.0 }).unwrap();

    let b1 = sub1.recv_async().await.unwrap();
    let b2 = sub2.recv_async().await.unwrap();
    assert_eq!(b1.num_rows(), 1);
    assert_eq!(b2.num_rows(), 1);
}

#[tokio::test]
async fn test_disconnected_after_source_and_sink_drop() {
    let (source, sink) = create::<TestEvent>(16);
    let mut sub = sink.subscribe();

    drop(source);
    drop(sink);
    // Drain task exits on source disconnect; once Sink is dropped too,
    // the broadcast closes and recv_async returns Disconnected.
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(sub.recv_async().await.is_err());
    assert!(sub.is_disconnected());
}

#[tokio::test]
async fn test_schema() {
    let (_source, sink) = create::<TestEvent>(16);
    let sub = sink.subscribe();
    assert_eq!(sub.schema().fields().len(), 2);
}
