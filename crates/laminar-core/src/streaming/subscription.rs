//! Streaming Subscription API.
//!
//! A Subscription provides access to records from a Sink. It supports:
//!
//! - Non-blocking poll
//! - Blocking receive with optional timeout
//! - Iterator interface
//! - Zero-allocation batch operations
//!
//! ## Usage
//!
//! ```rust,ignore
//! let subscription = sink.subscribe();
//!
//! // Non-blocking poll
//! while let Some(batch) = subscription.poll() {
//!     process(batch);
//! }
//!
//! // Blocking receive
//! let batch = subscription.recv()?;
//!
//! // With timeout
//! let batch = subscription.recv_timeout(Duration::from_secs(1))?;
//!
//! // As iterator
//! for batch in subscription {
//!     process(batch);
//! }
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use super::channel::Consumer;
use super::error::RecvError;
use super::sink::SinkInner;
use super::source::{Record, SourceMessage};

/// A subscription to a streaming sink.
///
/// Subscriptions receive records from a Sink and provide them via
/// polling, blocking receive, or iterator interfaces.
///
/// ## Modes
///
/// - **Direct**: First subscriber, reads directly from source channel
/// - **Broadcast**: Additional subscribers, reads from dedicated channel
pub struct Subscription<T: Record> {
    inner: SubscriptionInner<T>,
    schema: SchemaRef,
}

enum SubscriptionInner<T: Record> {
    /// Direct subscription to sink's consumer.
    Direct(Arc<SinkInner<T>>),
    /// Broadcast subscription with dedicated channel.
    Broadcast(Consumer<SourceMessage<T>>),
}

impl<T: Record> Subscription<T> {
    /// Creates a direct subscription (first subscriber).
    pub(crate) fn new_direct(sink_inner: Arc<SinkInner<T>>) -> Self {
        let schema = sink_inner.schema();
        Self {
            inner: SubscriptionInner::Direct(sink_inner),
            schema,
        }
    }

    /// Creates a broadcast subscription (additional subscribers).
    pub(crate) fn new_broadcast(consumer: Consumer<SourceMessage<T>>, schema: SchemaRef) -> Self {
        Self {
            inner: SubscriptionInner::Broadcast(consumer),
            schema,
        }
    }

    /// Polls for the next record batch without blocking.
    ///
    /// Returns `Some(RecordBatch)` if data is available, `None` if empty.
    ///
    /// Records are automatically converted to Arrow `RecordBatch` format.
    #[must_use]
    pub fn poll(&self) -> Option<RecordBatch> {
        let msg = match &self.inner {
            SubscriptionInner::Direct(sink) => sink.consumer().poll(),
            SubscriptionInner::Broadcast(consumer) => consumer.poll(),
        }?;

        Self::message_to_batch(msg)
    }

    /// Polls for raw messages (without conversion to `RecordBatch`).
    ///
    /// This is useful when you need to handle watermarks separately.
    #[must_use]
    pub fn poll_message(&self) -> Option<SubscriptionMessage<T>> {
        let msg = match &self.inner {
            SubscriptionInner::Direct(sink) => sink.consumer().poll(),
            SubscriptionInner::Broadcast(consumer) => consumer.poll(),
        }?;

        Some(Self::convert_message(msg))
    }

    /// Receives the next record batch, blocking until available.
    ///
    /// # Errors
    ///
    /// Returns `RecvError::Disconnected` if the source has been dropped
    /// and there are no more buffered records.
    pub fn recv(&self) -> Result<RecordBatch, RecvError> {
        loop {
            if let Some(batch) = self.poll() {
                return Ok(batch);
            }

            if self.is_disconnected() {
                return Err(RecvError::Disconnected);
            }

            // Brief yield before retrying
            std::hint::spin_loop();
        }
    }

    /// Receives the next record batch with a timeout.
    ///
    /// # Errors
    ///
    /// Returns `RecvError::Timeout` if no record becomes available within the timeout.
    /// Returns `RecvError::Disconnected` if the source has been dropped.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<RecordBatch, RecvError> {
        let deadline = Instant::now() + timeout;

        loop {
            if let Some(batch) = self.poll() {
                return Ok(batch);
            }

            if self.is_disconnected() {
                return Err(RecvError::Disconnected);
            }

            if Instant::now() >= deadline {
                return Err(RecvError::Timeout);
            }

            std::hint::spin_loop();
        }
    }

    /// Polls multiple record batches into a vector.
    ///
    /// Returns up to `max_count` batches.
    ///
    /// # Performance Warning
    ///
    /// **This method allocates a `Vec` on every call.** Do not use on hot paths
    /// where allocation overhead matters. For zero-allocation consumption, use
    /// [`poll_each`](Self::poll_each) or [`poll_batch_into`](Self::poll_batch_into).
    #[cold]
    #[must_use]
    pub fn poll_batch(&self, max_count: usize) -> Vec<RecordBatch> {
        let mut batches = Vec::with_capacity(max_count);

        for _ in 0..max_count {
            if let Some(batch) = self.poll() {
                batches.push(batch);
            } else {
                break;
            }
        }

        batches
    }

    /// Polls multiple record batches into a pre-allocated vector (zero-allocation).
    ///
    /// Appends up to `max_count` batches to the provided vector.
    /// Returns the number of batches added.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut buffer = Vec::with_capacity(100);
    /// loop {
    ///     buffer.clear();
    ///     let count = subscription.poll_batch_into(&mut buffer, 100);
    ///     if count == 0 { break; }
    ///     for batch in &buffer {
    ///         process(batch);
    ///     }
    /// }
    /// ```
    pub fn poll_batch_into(&self, buffer: &mut Vec<RecordBatch>, max_count: usize) -> usize {
        let mut count = 0;

        for _ in 0..max_count {
            if let Some(batch) = self.poll() {
                buffer.push(batch);
                count += 1;
            } else {
                break;
            }
        }

        count
    }

    /// Processes records with a callback (zero-allocation).
    ///
    /// The callback receives each `RecordBatch`. Processing stops when:
    /// - `max_count` batches have been processed
    /// - No more batches are available
    /// - The callback returns `false`
    ///
    /// Returns the number of batches processed.
    pub fn poll_each<F>(&self, max_count: usize, mut f: F) -> usize
    where
        F: FnMut(RecordBatch) -> bool,
    {
        let mut count = 0;

        for _ in 0..max_count {
            if let Some(batch) = self.poll() {
                count += 1;
                if !f(batch) {
                    break;
                }
            } else {
                break;
            }
        }

        count
    }

    /// Returns true if the source has been dropped and buffer is empty.
    #[must_use]
    pub fn is_disconnected(&self) -> bool {
        match &self.inner {
            SubscriptionInner::Direct(sink) => sink.is_disconnected(),
            SubscriptionInner::Broadcast(consumer) => consumer.is_disconnected(),
        }
    }

    /// Returns the number of pending items.
    #[must_use]
    pub fn pending(&self) -> usize {
        match &self.inner {
            SubscriptionInner::Direct(sink) => sink.consumer().len(),
            SubscriptionInner::Broadcast(consumer) => consumer.len(),
        }
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
            SourceMessage::Watermark(_) => {
                // Skip watermarks in poll(), they're handled separately
                None
            }
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

/// Message types that can be received from a subscription.
#[derive(Debug)]
pub enum SubscriptionMessage<T> {
    /// A single record.
    Record(T),
    /// A batch of records.
    Batch(RecordBatch),
    /// A watermark timestamp.
    Watermark(i64),
}

impl<T: Record> SubscriptionMessage<T> {
    /// Returns true if this is a record message.
    #[must_use]
    pub fn is_record(&self) -> bool {
        matches!(self, Self::Record(_))
    }

    /// Returns true if this is a batch message.
    #[must_use]
    pub fn is_batch(&self) -> bool {
        matches!(self, Self::Batch(_))
    }

    /// Returns true if this is a watermark message.
    #[must_use]
    pub fn is_watermark(&self) -> bool {
        matches!(self, Self::Watermark(_))
    }

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

/// Iterator implementation for Subscription.
///
/// Iterates over record batches, blocking on each call to `next()`.
/// Iteration stops when the source is disconnected.
impl<T: Record> Iterator for Subscription<T> {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        self.recv().ok()
    }
}

impl<T: Record + std::fmt::Debug> std::fmt::Debug for Subscription<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mode = match &self.inner {
            SubscriptionInner::Direct(_) => "Direct",
            SubscriptionInner::Broadcast(_) => "Broadcast",
        };

        f.debug_struct("Subscription")
            .field("mode", &mode)
            .field("pending", &self.pending())
            .field("is_disconnected", &self.is_disconnected())
            .field("schema", &self.schema)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::source::create;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
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

    #[test]
    fn test_poll_empty() {
        let (_source, sink) = create::<TestEvent>(16);
        let sub = sink.subscribe();

        assert!(sub.poll().is_none());
    }

    #[test]
    fn test_poll_records() {
        let (source, sink) = create::<TestEvent>(16);
        let sub = sink.subscribe();

        source.push(TestEvent { id: 1, value: 1.0 }).unwrap();
        source.push(TestEvent { id: 2, value: 2.0 }).unwrap();

        let batch1 = sub.poll().unwrap();
        assert_eq!(batch1.num_rows(), 1);

        let batch2 = sub.poll().unwrap();
        assert_eq!(batch2.num_rows(), 1);

        assert!(sub.poll().is_none());
    }

    #[test]
    fn test_poll_message() {
        let (source, sink) = create::<TestEvent>(16);
        let sub = sink.subscribe();

        source.push(TestEvent { id: 1, value: 1.0 }).unwrap();

        let msg = sub.poll_message().unwrap();
        assert!(msg.is_record());
    }

    #[test]
    fn test_recv_timeout() {
        let (_source, sink) = create::<TestEvent>(16);
        let sub = sink.subscribe();

        // Should timeout on empty subscription
        let result = sub.recv_timeout(Duration::from_millis(10));
        assert!(matches!(result, Err(RecvError::Timeout)));
    }

    #[test]
    fn test_recv_timeout_success() {
        let (source, sink) = create::<TestEvent>(16);
        let sub = sink.subscribe();

        source.push(TestEvent { id: 1, value: 1.0 }).unwrap();

        let result = sub.recv_timeout(Duration::from_secs(1));
        assert!(result.is_ok());
    }

    #[test]
    fn test_poll_batch() {
        let (source, sink) = create::<TestEvent>(16);
        let sub = sink.subscribe();

        source.push(TestEvent { id: 1, value: 1.0 }).unwrap();
        source.push(TestEvent { id: 2, value: 2.0 }).unwrap();
        source.push(TestEvent { id: 3, value: 3.0 }).unwrap();

        let batches = sub.poll_batch(10);
        assert_eq!(batches.len(), 3);
    }

    #[test]
    fn test_poll_each() {
        let (source, sink) = create::<TestEvent>(16);
        let sub = sink.subscribe();

        source.push(TestEvent { id: 1, value: 1.0 }).unwrap();
        source.push(TestEvent { id: 2, value: 2.0 }).unwrap();

        let mut total_rows = 0;
        let count = sub.poll_each(10, |batch| {
            total_rows += batch.num_rows();
            true
        });

        assert_eq!(count, 2);
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_poll_each_early_stop() {
        let (source, sink) = create::<TestEvent>(16);
        let sub = sink.subscribe();

        source.push(TestEvent { id: 1, value: 1.0 }).unwrap();
        source.push(TestEvent { id: 2, value: 2.0 }).unwrap();
        source.push(TestEvent { id: 3, value: 3.0 }).unwrap();

        let mut seen = 0;
        let count = sub.poll_each(10, |_| {
            seen += 1;
            seen < 2 // Stop after 2
        });

        assert_eq!(count, 2);
        assert_eq!(seen, 2);
        assert_eq!(sub.pending(), 1); // One left
    }

    #[test]
    fn test_disconnected() {
        let (source, sink) = create::<TestEvent>(16);
        let sub = sink.subscribe();

        assert!(!sub.is_disconnected());

        drop(source);

        assert!(sub.is_disconnected());
    }

    #[test]
    fn test_pending() {
        let (source, sink) = create::<TestEvent>(16);
        let sub = sink.subscribe();

        assert_eq!(sub.pending(), 0);

        source.push(TestEvent { id: 1, value: 1.0 }).unwrap();
        source.push(TestEvent { id: 2, value: 2.0 }).unwrap();

        assert_eq!(sub.pending(), 2);
    }

    #[test]
    fn test_schema() {
        let (_source, sink) = create::<TestEvent>(16);
        let sub = sink.subscribe();

        let schema = sub.schema();
        assert_eq!(schema.fields().len(), 2);
    }

    #[test]
    fn test_subscription_message() {
        let msg = SubscriptionMessage::Record(TestEvent { id: 1, value: 1.0 });
        assert!(msg.is_record());
        assert!(!msg.is_batch());
        assert!(!msg.is_watermark());

        let batch = msg.to_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let wm = SubscriptionMessage::<TestEvent>::Watermark(1000);
        assert!(wm.is_watermark());
        assert_eq!(wm.watermark(), Some(1000));
    }

    #[test]
    fn test_iterator() {
        let (source, sink) = create::<TestEvent>(16);
        let mut sub = sink.subscribe();

        source.push(TestEvent { id: 1, value: 1.0 }).unwrap();
        source.push(TestEvent { id: 2, value: 2.0 }).unwrap();

        drop(source);

        let batches: Vec<_> = sub.by_ref().collect();
        assert_eq!(batches.len(), 2);
    }

    #[test]
    fn test_debug_format() {
        let (_source, sink) = create::<TestEvent>(16);
        let sub = sink.subscribe();

        let debug = format!("{sub:?}");
        assert!(debug.contains("Subscription"));
        assert!(debug.contains("Direct"));
    }
}
