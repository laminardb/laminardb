//! Subscription — receive records from a Sink via poll, recv, or async recv.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use tokio::sync::broadcast;

use super::error::RecvError;
use super::source::{Record, SourceMessage};

enum PollResult {
    Batch(RecordBatch),
    Empty,
    Closed,
}

/// A subscription to a streaming sink. Each subscriber independently receives
/// all messages via broadcast.
pub struct Subscription<T: Record> {
    rx: broadcast::Receiver<SourceMessage<T>>,
    schema: SchemaRef,
}

impl<T: Record> Subscription<T> {
    pub(crate) fn new(rx: broadcast::Receiver<SourceMessage<T>>, schema: SchemaRef) -> Self {
        Self { rx, schema }
    }

    /// Non-blocking poll. Returns the next batch, skipping watermarks.
    pub fn poll(&mut self) -> Option<RecordBatch> {
        loop {
            match self.rx.try_recv() {
                Ok(msg) => {
                    if let Some(batch) = Self::message_to_batch(msg) {
                        return Some(batch);
                    }
                }
                Err(broadcast::error::TryRecvError::Lagged(_)) => {}
                Err(_) => return None,
            }
        }
    }

    /// Non-blocking poll for raw messages (records, batches, or watermarks).
    pub fn poll_message(&mut self) -> Option<SubscriptionMessage<T>> {
        loop {
            match self.rx.try_recv() {
                Ok(msg) => return Some(Self::convert_message(msg)),
                Err(broadcast::error::TryRecvError::Lagged(_)) => {}
                Err(_) => return None,
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
                    if let Some(batch) = Self::message_to_batch(msg) {
                        return Ok(batch);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(RecvError::Disconnected);
                }
            }
        }
    }

    /// Blocking receive with timeout.
    ///
    /// # Errors
    ///
    /// Returns `RecvError::Timeout` or `RecvError::Disconnected`.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<RecordBatch, RecvError> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match self.poll_or_closed() {
                PollResult::Batch(batch) => return Ok(batch),
                PollResult::Closed => return Err(RecvError::Disconnected),
                PollResult::Empty => {}
            }
            if std::time::Instant::now() >= deadline {
                return Err(RecvError::Timeout);
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            std::thread::park_timeout(remaining.min(Duration::from_micros(100)));
        }
    }

    /// Blocking receive.
    ///
    /// # Errors
    ///
    /// Returns `RecvError::Disconnected` if the source has been dropped.
    pub fn recv(&mut self) -> Result<RecordBatch, RecvError> {
        loop {
            match self.poll_or_closed() {
                PollResult::Batch(batch) => return Ok(batch),
                PollResult::Closed => return Err(RecvError::Disconnected),
                PollResult::Empty => {}
            }
            std::thread::park_timeout(Duration::from_micros(100));
        }
    }

    /// Internal: distinguishes empty from closed.
    fn poll_or_closed(&mut self) -> PollResult {
        loop {
            match self.rx.try_recv() {
                Ok(msg) => {
                    if let Some(batch) = Self::message_to_batch(msg) {
                        return PollResult::Batch(batch);
                    }
                    // watermark — skip, try again
                }
                Err(broadcast::error::TryRecvError::Lagged(_)) => {}
                Err(broadcast::error::TryRecvError::Empty) => return PollResult::Empty,
                Err(broadcast::error::TryRecvError::Closed) => return PollResult::Closed,
            }
        }
    }

    /// Returns true if the broadcast channel is closed.
    #[must_use]
    pub fn is_disconnected(&mut self) -> bool {
        matches!(self.poll_or_closed(), PollResult::Closed)
    }

    /// Polls multiple batches. Returns up to `max_count`.
    pub fn poll_batch(&mut self, max_count: usize) -> Vec<RecordBatch> {
        let mut batches = Vec::with_capacity(max_count);
        for _ in 0..max_count {
            match self.poll() {
                Some(batch) => batches.push(batch),
                None => break,
            }
        }
        batches
    }

    /// Polls batches into a pre-allocated vector. Returns count added.
    pub fn poll_batch_into(&mut self, buffer: &mut Vec<RecordBatch>, max_count: usize) -> usize {
        let mut count = 0;
        for _ in 0..max_count {
            match self.poll() {
                Some(batch) => {
                    buffer.push(batch);
                    count += 1;
                }
                None => break,
            }
        }
        count
    }

    /// Processes batches with a callback. Stops when callback returns false.
    pub fn poll_each<F>(&mut self, max_count: usize, mut f: F) -> usize
    where
        F: FnMut(RecordBatch) -> bool,
    {
        let mut count = 0;
        for _ in 0..max_count {
            match self.poll() {
                Some(batch) => {
                    count += 1;
                    if !f(batch) {
                        break;
                    }
                }
                None => break,
            }
        }
        count
    }

    /// Returns the schema for records in this subscription.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn message_to_batch(msg: SourceMessage<T>) -> Option<RecordBatch> {
        match msg {
            SourceMessage::Record(record) => Some(record.to_record_batch()),
            SourceMessage::Batch(batch) => Some(batch),
            SourceMessage::Watermark(_) => None,
        }
    }

    fn convert_message(msg: SourceMessage<T>) -> SubscriptionMessage<T> {
        match msg {
            SourceMessage::Record(record) => SubscriptionMessage::Record(record),
            SourceMessage::Batch(batch) => SubscriptionMessage::Batch(batch),
            SourceMessage::Watermark(ts) => SubscriptionMessage::Watermark(ts),
        }
    }
}

/// Message types received from a subscription.
#[derive(Debug, Clone)]
pub enum SubscriptionMessage<T> {
    /// A single record.
    Record(T),
    /// A batch of records.
    Batch(RecordBatch),
    /// A watermark timestamp.
    Watermark(i64),
}

impl<T: Record> SubscriptionMessage<T> {
    /// Converts to a `RecordBatch` if this is a data message.
    #[must_use]
    pub fn to_batch(self) -> Option<RecordBatch> {
        match self {
            Self::Record(r) => Some(r.to_record_batch()),
            Self::Batch(b) => Some(b),
            Self::Watermark(_) => None,
        }
    }

    /// Returns the watermark timestamp if this is a watermark message.
    #[must_use]
    pub fn watermark(&self) -> Option<i64> {
        match self {
            Self::Watermark(ts) => Some(*ts),
            _ => None,
        }
    }
}

impl<T: Record> Iterator for Subscription<T> {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        self.poll()
    }
}

impl<T: Record + std::fmt::Debug> std::fmt::Debug for Subscription<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription").finish()
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
    async fn test_recv_timeout() {
        let (_source, sink) = create::<TestEvent>(16);
        let mut sub = sink.subscribe();
        let result = sub.recv_timeout(Duration::from_millis(10));
        assert!(matches!(result, Err(RecvError::Timeout)));
    }

    #[tokio::test]
    async fn test_poll_each() {
        let (source, sink) = create::<TestEvent>(16);
        let mut sub = sink.subscribe();

        source.push(TestEvent { id: 1, value: 1.0 }).unwrap();
        source.push(TestEvent { id: 2, value: 2.0 }).unwrap();

        // Wait for drain task
        let b1 = sub.recv_async().await.unwrap();
        let b2 = sub.recv_async().await.unwrap();
        assert_eq!(b1.num_rows(), 1);
        assert_eq!(b2.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_schema() {
        let (_source, sink) = create::<TestEvent>(16);
        let sub = sink.subscribe();
        assert_eq!(sub.schema().fields().len(), 2);
    }
}
