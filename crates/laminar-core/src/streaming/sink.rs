//! Sink — consumption endpoint with broadcast fan-out to multiple subscribers.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use tokio::sync::broadcast;

use super::channel::AsyncConsumer;
use super::source::{Record, SourceMessage};
use super::subscription::Subscription;

const DEFAULT_BROADCAST_CAPACITY: usize = 2048;

/// A streaming data sink. Each `subscribe()` call returns an independent
/// receiver that gets a copy of every message via broadcast.
pub struct Sink<T: Record> {
    broadcast_tx: broadcast::Sender<SourceMessage<T>>,
    schema: SchemaRef,
}

impl<T: Record> Sink<T> {
    pub(crate) fn new(consumer: AsyncConsumer<SourceMessage<T>>, schema: SchemaRef) -> Self {
        let (broadcast_tx, _) = broadcast::channel(DEFAULT_BROADCAST_CAPACITY);
        let tx = broadcast_tx.clone();

        tokio::spawn(async move {
            drain_loop(consumer, tx).await;
        });

        Self {
            broadcast_tx,
            schema,
        }
    }

    /// Subscribe to this sink. Returns an independent receiver.
    #[must_use]
    pub fn subscribe(&self) -> Subscription<T> {
        Subscription::new(self.broadcast_tx.subscribe(), Arc::clone(&self.schema))
    }

    /// Returns the schema for this sink.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Number of active broadcast subscribers.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.broadcast_tx.receiver_count()
    }
}

impl<T: Record + std::fmt::Debug> std::fmt::Debug for Sink<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sink")
            .field("subscribers", &self.subscriber_count())
            .finish()
    }
}

async fn drain_loop<T: Record>(
    mut consumer: AsyncConsumer<SourceMessage<T>>,
    tx: broadcast::Sender<SourceMessage<T>>,
) {
    while let Ok(msg) = consumer.recv().await {
        let _ = tx.send(msg);
    }
}

#[cfg(test)]
mod tests {
    use crate::streaming::source::create;
    use crate::streaming::source::Record;
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
    async fn test_single_subscriber() {
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
    async fn test_schema() {
        let (_source, sink) = create::<TestEvent>(16);
        assert_eq!(sink.schema().fields().len(), 2);
    }

    #[tokio::test]
    async fn test_subscriber_count() {
        let (_source, sink) = create::<TestEvent>(16);
        assert_eq!(sink.subscriber_count(), 0);

        let _sub1 = sink.subscribe();
        assert_eq!(sink.subscriber_count(), 1);

        let _sub2 = sink.subscribe();
        assert_eq!(sink.subscriber_count(), 2);

        drop(_sub1);
        assert_eq!(sink.subscriber_count(), 1);
    }
}
