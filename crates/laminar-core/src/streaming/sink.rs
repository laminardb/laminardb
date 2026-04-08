//! Sink — consumption endpoint with subscribe-based fan-out.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use super::channel::Consumer;
use super::config::ChannelConfig;
use super::source::{Record, SourceMessage};
use super::subscription::Subscription;

/// Sink mode indicator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkMode {
    /// Single subscriber - records go to one consumer.
    Single,
    /// Broadcast - records are copied to all subscribers.
    Broadcast,
}

/// Shared state for a Sink.
pub(crate) struct SinkInner<T: Record> {
    /// Consumer for receiving from source.
    consumer: Consumer<SourceMessage<T>>,

    /// Schema for record validation.
    schema: SchemaRef,

    /// Configuration for subscriber channels.
    #[allow(dead_code)]
    channel_config: ChannelConfig,

    /// Number of active subscribers.
    subscriber_count: AtomicUsize,
}

impl<T: Record> SinkInner<T> {
    pub(crate) fn release_subscriber(&self) {
        self.subscriber_count.fetch_sub(1, Ordering::AcqRel);
    }
}

/// A streaming data sink. Supports single-subscriber and broadcast modes.
pub struct Sink<T: Record> {
    inner: Arc<SinkInner<T>>,
}

impl<T: Record> Sink<T> {
    /// Creates a new sink from a channel consumer.
    pub(crate) fn new(
        consumer: Consumer<SourceMessage<T>>,
        schema: SchemaRef,
        channel_config: ChannelConfig,
    ) -> Self {
        Self {
            inner: Arc::new(SinkInner {
                consumer,
                schema,
                channel_config,
                subscriber_count: AtomicUsize::new(0),
            }),
        }
    }

    /// Creates a subscription to receive records from this sink.
    #[must_use]
    pub fn subscribe(&self) -> Subscription<T> {
        self.inner.subscriber_count.fetch_add(1, Ordering::AcqRel);
        Subscription::new_direct(Arc::clone(&self.inner))
    }

    /// Returns the number of active subscribers.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.inner.subscriber_count.load(Ordering::Acquire)
    }

    /// Returns the sink mode based on subscriber count.
    #[must_use]
    pub fn mode(&self) -> SinkMode {
        if self.subscriber_count() <= 1 {
            SinkMode::Single
        } else {
            SinkMode::Broadcast
        }
    }

    /// Returns the schema for this sink.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.inner.schema)
    }

    /// Returns true if the source has been dropped.
    #[must_use]
    pub fn is_disconnected(&self) -> bool {
        self.inner.consumer.is_disconnected()
    }

    /// Returns the number of pending items from the source.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.inner.consumer.len()
    }
}

impl<T: Record + std::fmt::Debug> std::fmt::Debug for Sink<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sink")
            .field("mode", &self.mode())
            .field("subscriber_count", &self.subscriber_count())
            .field("pending", &self.pending())
            .field("is_disconnected", &self.is_disconnected())
            .finish()
    }
}

// Internal accessor for Subscription
impl<T: Record> SinkInner<T> {
    pub(crate) fn consumer(&self) -> &Consumer<SourceMessage<T>> {
        &self.consumer
    }

    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub(crate) fn is_disconnected(&self) -> bool {
        self.consumer.is_disconnected()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::source::create;
    use arrow::array::{Float64Array, Int64Array, RecordBatch};
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
    fn test_sink_creation() {
        let (_source, sink) = create::<TestEvent>(16);

        assert_eq!(sink.subscriber_count(), 0);
        assert_eq!(sink.mode(), SinkMode::Single);
        assert!(!sink.is_disconnected());
    }

    #[test]
    fn test_single_subscriber() {
        let (_source, sink) = create::<TestEvent>(16);

        let _sub = sink.subscribe();

        assert_eq!(sink.subscriber_count(), 1);
        assert_eq!(sink.mode(), SinkMode::Single);
    }

    #[test]
    fn test_schema() {
        let (_source, sink) = create::<TestEvent>(16);

        let schema = sink.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "value");
    }

    #[test]
    fn test_disconnected_on_source_drop() {
        let (source, sink) = create::<TestEvent>(16);

        assert!(!sink.is_disconnected());

        drop(source);

        assert!(sink.is_disconnected());
    }

    #[test]
    fn test_pending() {
        let (source, sink) = create::<TestEvent>(16);

        assert_eq!(sink.pending(), 0);

        source.push(TestEvent { id: 1, value: 1.0 }).unwrap();
        source.push(TestEvent { id: 2, value: 2.0 }).unwrap();

        assert_eq!(sink.pending(), 2);
    }

    #[test]
    fn test_debug_format() {
        let (_source, sink) = create::<TestEvent>(16);

        let debug = format!("{sink:?}");
        assert!(debug.contains("Sink"));
        assert!(debug.contains("Single"));
    }
}
