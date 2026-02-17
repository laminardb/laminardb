//! Streaming Sink API.
//!
//! A Sink is the consumption endpoint of a streaming pipeline.
//! It receives records from a Source and provides them to subscribers.
//!
//! ## Modes
//!
//! - **Single subscriber (SPSC)**: Optimal performance, subscriber gets all records
//! - **Broadcast**: Multiple subscribers, each gets a copy of all records
//!
//! ## Usage
//!
//! ```rust,ignore
//! use laminar_core::streaming;
//!
//! let (source, sink) = streaming::create::<MyEvent>(1024);
//!
//! // Single subscriber
//! let subscription = sink.subscribe();
//!
//! // Or broadcast to multiple subscribers
//! let sub1 = sink.subscribe();
//! let sub2 = sink.subscribe();
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use super::channel::{channel_with_config, Consumer, Producer};
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

/// Internal subscriber state.
struct SubscriberInner<T: Record> {
    /// Producer for this subscriber's channel.
    producer: Producer<SourceMessage<T>>,
}

/// Shared state for a Sink.
pub(crate) struct SinkInner<T: Record> {
    /// Consumer for receiving from source.
    consumer: Consumer<SourceMessage<T>>,

    /// Schema for record validation.
    schema: SchemaRef,

    /// Configuration for subscriber channels.
    channel_config: ChannelConfig,

    /// Number of active subscribers.
    subscriber_count: AtomicUsize,

    /// Whether the sink is disconnected (source dropped).
    #[allow(dead_code)] // Reserved for future use
    disconnected: std::sync::atomic::AtomicBool,
}

/// A streaming data sink.
///
/// Sinks receive records from a Source and distribute them to subscribers.
/// The sink supports both single-subscriber and broadcast modes.
///
/// ## Subscription Model
///
/// When you call `subscribe()`, you get a `Subscription` that receives
/// records from this sink. If you subscribe multiple times, the sink
/// automatically enters broadcast mode where each subscriber gets a
/// copy of every record.
///
/// ## Performance Notes
///
/// - Single subscriber mode: Zero overhead, direct channel access
/// - Broadcast mode: Uses `RwLock` for subscriber list (read-heavy optimization)
/// - `poll_and_distribute()`: Takes a read lock (fast, non-blocking with other readers)
/// - `subscribe()`: Takes a write lock (rare, happens at setup time)
///
/// ## Example
///
/// ```rust,ignore
/// let (source, sink) = streaming::create::<MyEvent>(1024);
///
/// // Subscribe to receive records
/// let subscription = sink.subscribe();
///
/// // Process records
/// while let Some(batch) = subscription.poll() {
///     process(batch);
/// }
/// ```
pub struct Sink<T: Record> {
    inner: Arc<SinkInner<T>>,
    /// Subscribers (only used in broadcast mode).
    /// Uses `RwLock` for fast read access on hot path.
    subscribers: Arc<parking_lot::RwLock<Vec<SubscriberInner<T>>>>,
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
                disconnected: std::sync::atomic::AtomicBool::new(false),
            }),
            subscribers: Arc::new(parking_lot::RwLock::new(Vec::new())),
        }
    }

    /// Creates a subscription to receive records from this sink.
    ///
    /// The first subscriber receives records directly from the source channel.
    /// Additional subscribers trigger broadcast mode where records are copied.
    ///
    /// # Returns
    ///
    /// A `Subscription` that can be used to poll or receive records.
    ///
    /// # Performance
    ///
    /// This method takes a write lock and should only be called during setup,
    /// not on the hot path. Subscription setup is O(1).
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned (should not happen in normal use).
    #[must_use]
    pub fn subscribe(&self) -> Subscription<T> {
        let count = self.inner.subscriber_count.fetch_add(1, Ordering::AcqRel);

        if count == 0 {
            // First subscriber - direct connection to source
            Subscription::new_direct(Arc::clone(&self.inner))
        } else {
            // Additional subscriber - create broadcast channel
            let (producer, consumer) =
                channel_with_config::<SourceMessage<T>>(self.inner.channel_config.clone());

            // Store producer for broadcasting (write lock, not hot path)
            {
                let mut subs = self.subscribers.write();
                subs.push(SubscriberInner { producer });
            }

            Subscription::new_broadcast(consumer, Arc::clone(&self.inner.schema))
        }
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

    /// Polls for records and distributes them to subscribers.
    ///
    /// In single-subscriber mode, this is a no-op (direct channel).
    /// In broadcast mode, this copies records to all subscriber channels.
    ///
    /// Returns the number of records distributed.
    ///
    /// # Performance
    ///
    /// Uses a read lock which is fast and non-blocking with other readers.
    /// The lock is held for the minimum time necessary.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned (should not happen in normal use).
    #[must_use]
    pub fn poll_and_distribute(&self) -> usize
    where
        T: Clone,
    {
        // Only needed in broadcast mode
        if self.mode() != SinkMode::Broadcast {
            return 0;
        }

        // Take read lock (fast, doesn't block other readers)
        let subscribers = self.subscribers.read();
        if subscribers.is_empty() {
            return 0;
        }

        let mut count = 0;

        // Poll records from source and broadcast
        while let Some(msg) = self.inner.consumer.poll() {
            for sub in subscribers.iter() {
                // Clone and send to each subscriber
                let msg_clone = match &msg {
                    SourceMessage::Record(r) => SourceMessage::Record(r.clone()),
                    SourceMessage::Batch(b) => SourceMessage::Batch(b.clone()),
                    SourceMessage::Watermark(ts) => SourceMessage::Watermark(*ts),
                };
                let _ = sub.producer.try_push(msg_clone);
            }
            count += 1;
        }

        count
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
    fn test_broadcast_mode() {
        let (_source, sink) = create::<TestEvent>(16);

        let _sub1 = sink.subscribe();
        let _sub2 = sink.subscribe();

        assert_eq!(sink.subscriber_count(), 2);
        assert_eq!(sink.mode(), SinkMode::Broadcast);
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

        // The sink consumer should detect disconnection
        // (may need to poll to see it)
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
