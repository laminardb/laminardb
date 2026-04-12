//! Subscription — receive records from a Sink.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use tokio::sync::broadcast;

use super::error::RecvError;
use super::source::{Record, SourceMessage};

/// A subscription to a streaming sink. Each subscriber independently receives
/// every message via broadcast.
pub struct Subscription<T: Record> {
    rx: broadcast::Receiver<SourceMessage<T>>,
    schema: SchemaRef,
    closed: bool,
}

impl<T: Record> Subscription<T> {
    pub(crate) fn new(rx: broadcast::Receiver<SourceMessage<T>>, schema: SchemaRef) -> Self {
        Self {
            rx,
            schema,
            closed: false,
        }
    }

    /// Non-blocking poll. Returns the next batch, skipping watermarks.
    /// Returns `None` on empty or closed channel. Check `is_disconnected()`
    /// to distinguish.
    pub fn poll(&mut self) -> Option<RecordBatch> {
        loop {
            match self.rx.try_recv() {
                Ok(msg) => {
                    if let Some(batch) = message_to_batch(msg) {
                        return Some(batch);
                    }
                }
                Err(broadcast::error::TryRecvError::Empty) => return None,
                Err(broadcast::error::TryRecvError::Closed) => {
                    self.closed = true;
                    return None;
                }
                Err(broadcast::error::TryRecvError::Lagged(_)) => {}
            }
        }
    }

    /// Async receive. Awaits the next batch, skipping watermarks.
    ///
    /// # Errors
    ///
    /// Returns `RecvError::Disconnected` if the source has been dropped.
    pub async fn recv_async(&mut self) -> Result<RecordBatch, RecvError> {
        loop {
            match self.rx.recv().await {
                Ok(msg) => {
                    if let Some(batch) = message_to_batch(msg) {
                        return Ok(batch);
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    self.closed = true;
                    return Err(RecvError::Disconnected);
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {}
            }
        }
    }

    /// Blocking receive. Uses tokio's waker-based `blocking_recv`.
    ///
    /// # Errors
    ///
    /// Returns `RecvError::Disconnected` if the source has been dropped.
    pub fn recv(&mut self) -> Result<RecordBatch, RecvError> {
        loop {
            match self.rx.blocking_recv() {
                Ok(msg) => {
                    if let Some(batch) = message_to_batch(msg) {
                        return Ok(batch);
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    self.closed = true;
                    return Err(RecvError::Disconnected);
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {}
            }
        }
    }

    /// Blocking receive with timeout. Requires a tokio runtime in the current
    /// thread context.
    ///
    /// # Errors
    ///
    /// Returns `RecvError::Timeout` or `RecvError::Disconnected`.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<RecordBatch, RecvError> {
        let handle = tokio::runtime::Handle::current();
        match handle.block_on(tokio::time::timeout(timeout, self.recv_async())) {
            Ok(Ok(batch)) => Ok(batch),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(RecvError::Timeout),
        }
    }

    /// Returns true if the channel has been observed closed.
    #[must_use]
    pub fn is_disconnected(&self) -> bool {
        self.closed
    }

    /// Returns the schema for records in this subscription.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn message_to_batch<T: Record>(msg: SourceMessage<T>) -> Option<RecordBatch> {
    match msg {
        SourceMessage::Record(r) => Some(r.to_record_batch()),
        SourceMessage::Batch(b) => Some(b),
        SourceMessage::Watermark(_) => None,
    }
}

impl<T: Record + std::fmt::Debug> std::fmt::Debug for Subscription<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("closed", &self.closed)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
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
}
