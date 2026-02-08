//! Streaming Source API.
//!
//! A Source is the entry point for data into a streaming pipeline.
//! It wraps a channel producer with a type-safe interface and supports:
//!
//! - Push individual records or batches
//! - Push Arrow `RecordBatch` directly
//! - Watermark emission for event-time processing
//! - Automatic SPSC → MPSC upgrade on clone
//!
//! ## Usage
//!
//! ```rust,ignore
//! use laminar_core::streaming::{Source, SourceConfig};
//!
//! // Create a source
//! let (source, sink) = streaming::create::<MyEvent>(config);
//!
//! // Push records
//! source.push(event)?;
//! source.push_batch(&events)?;
//!
//! // Emit watermark
//! source.watermark(timestamp);
//!
//! // Clone for multi-producer (triggers MPSC upgrade)
//! let source2 = source.clone();
//! ```

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use super::channel::{channel_with_config, ChannelMode, Producer};
use super::config::SourceConfig;
use super::error::{StreamingError, TryPushError};
use super::sink::Sink;

/// Trait for types that can be streamed through a Source.
///
/// Implementations must provide:
/// - Conversion to/from Arrow `RecordBatch`
/// - Schema definition
/// - Optional event time extraction
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::streaming::Record;
/// use arrow::array::RecordBatch;
/// use arrow::datatypes::{Schema, SchemaRef, Field, DataType};
///
/// #[derive(Clone)]
/// struct TradeEvent {
///     symbol: String,
///     price: f64,
///     timestamp: i64,
/// }
///
/// impl Record for TradeEvent {
///     fn schema() -> SchemaRef {
///         Arc::new(Schema::new(vec![
///             Field::new("symbol", DataType::Utf8, false),
///             Field::new("price", DataType::Float64, false),
///             Field::new("timestamp", DataType::Int64, false),
///         ]))
///     }
///
///     fn to_record_batch(&self) -> RecordBatch {
///         // Convert to RecordBatch...
///     }
///
///     fn event_time(&self) -> Option<i64> {
///         Some(self.timestamp)
///     }
/// }
/// ```
pub trait Record: Send + Sized + 'static {
    /// Returns the Arrow schema for this record type.
    fn schema() -> SchemaRef;

    /// Converts this record to an Arrow `RecordBatch`.
    ///
    /// The batch will contain a single row with this record's data.
    fn to_record_batch(&self) -> RecordBatch;

    /// Returns the event time for this record, if applicable.
    ///
    /// Event time is used for watermark generation and window assignment.
    /// Returns `None` if the record doesn't have an event time.
    fn event_time(&self) -> Option<i64> {
        None
    }

    /// Converts a batch of records to an Arrow `RecordBatch`.
    ///
    /// The default implementation converts each record individually and concatenates them.
    /// Derived implementations can override this to optimize allocation and copying.
    fn to_record_batch_from_iter<I>(records: I) -> RecordBatch
    where
        I: IntoIterator<Item = Self>,
    {
        let batches: Vec<RecordBatch> = records.into_iter().map(|r| r.to_record_batch()).collect();
        if batches.is_empty() {
            return RecordBatch::new_empty(Self::schema());
        }
        arrow::compute::concat_batches(&Self::schema(), &batches).expect("concat failed")
    }
}

/// Internal message type that wraps records and control signals.
pub(crate) enum SourceMessage<T> {
    /// A data record.
    Record(T),

    /// A batch of Arrow records.
    Batch(RecordBatch),

    /// A watermark timestamp.
    Watermark(i64),
}

/// Shared state for watermark tracking.
struct SourceWatermark {
    /// Current watermark value.
    /// Atomically updated to support multi-producer scenarios.
    /// Wrapped in `Arc` so the checkpoint manager can read it without locking.
    current: Arc<AtomicI64>,
}

impl SourceWatermark {
    fn new() -> Self {
        Self {
            current: Arc::new(AtomicI64::new(i64::MIN)),
        }
    }

    fn from_arc(arc: Arc<AtomicI64>) -> Self {
        Self { current: arc }
    }

    fn update(&self, timestamp: i64) {
        // Only advance watermark, never go backwards
        let mut current = self.current.load(Ordering::Acquire);
        while timestamp > current {
            match self.current.compare_exchange_weak(
                current,
                timestamp,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    fn get(&self) -> i64 {
        self.current.load(Ordering::Acquire)
    }

    fn arc(&self) -> Arc<AtomicI64> {
        Arc::clone(&self.current)
    }
}

/// Shared state for a Source/Sink pair.
struct SourceInner<T: Record> {
    /// Channel producer for sending records.
    producer: Producer<SourceMessage<T>>,

    /// Watermark state.
    watermark: SourceWatermark,

    /// Schema for type validation.
    schema: SchemaRef,

    /// Source name (for debugging/metrics).
    name: Option<String>,

    /// Monotonic sequence counter, incremented on each successful push.
    /// Wrapped in `Arc` so the checkpoint manager can read it without locking.
    sequence: Arc<AtomicU64>,
}

/// A streaming data source.
///
/// Sources are the entry point for data into a streaming pipeline.
/// They provide a type-safe interface for pushing records and control
/// signals (watermarks).
///
/// ## Thread Safety
///
/// Sources can be cloned to create multiple producers. The first clone
/// triggers an automatic upgrade from SPSC to MPSC mode.
///
/// ## Example
///
/// ```rust,ignore
/// let (source, sink) = streaming::create::<MyEvent>(config);
///
/// // Single producer (SPSC mode)
/// source.push(event1)?;
///
/// // Clone for multiple producers (MPSC mode)
/// let source2 = source.clone();
/// std::thread::spawn(move || {
///     source2.push(event2)?;
/// });
/// ```
pub struct Source<T: Record> {
    inner: Arc<SourceInner<T>>,
}

impl<T: Record> Source<T> {
    /// Creates a new Source/Sink pair.
    pub(crate) fn new(config: SourceConfig) -> (Self, Sink<T>) {
        let channel_config = config.channel;
        let (producer, consumer) = channel_with_config::<SourceMessage<T>>(channel_config.clone());

        let schema = T::schema();

        let inner = Arc::new(SourceInner {
            producer,
            watermark: SourceWatermark::new(),
            schema: schema.clone(),
            name: config.name,
            sequence: Arc::new(AtomicU64::new(0)),
        });

        let source = Self { inner };
        let sink = Sink::new(consumer, schema, channel_config);

        (source, sink)
    }

    /// Pushes a record to the source, blocking if necessary.
    ///
    /// # Errors
    ///
    /// Returns `StreamingError::ChannelClosed` if the sink has been dropped.
    /// Returns `StreamingError::ChannelFull` if backpressure strategy is `Reject`.
    pub fn push(&self, record: T) -> Result<(), StreamingError> {
        // Update watermark if record has event time
        if let Some(event_time) = record.event_time() {
            self.inner.watermark.update(event_time);
        }

        self.inner
            .producer
            .push(SourceMessage::Record(record))
            .map_err(|_| StreamingError::ChannelFull)?;

        self.inner.sequence.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Tries to push a record without blocking.
    ///
    /// # Errors
    ///
    /// Returns `TryPushError` containing the record if the push failed.
    pub fn try_push(&self, record: T) -> Result<(), TryPushError<T>> {
        // Update watermark if record has event time
        if let Some(event_time) = record.event_time() {
            self.inner.watermark.update(event_time);
        }

        self.inner
            .producer
            .try_push(SourceMessage::Record(record))
            .map_err(|e| match e.into_inner() {
                SourceMessage::Record(r) => TryPushError {
                    value: r,
                    error: StreamingError::ChannelFull,
                },
                _ => unreachable!("pushed a record, got something else back"),
            })?;

        self.inner.sequence.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Pushes multiple records, returning the number successfully pushed.
    ///
    /// Stops at the first failure. Requires `Clone` because records are cloned
    /// before being sent.
    ///
    /// # Performance Warning
    ///
    /// **This method clones each record.** For zero-clone batch insertion,
    /// use [`push_batch_drain`](Self::push_batch_drain) which takes ownership
    /// via an iterator.
    pub fn push_batch(&self, records: &[T]) -> usize
    where
        T: Clone,
    {
        let mut count = 0;
        for record in records {
            if self.try_push(record.clone()).is_err() {
                break;
            }
            count += 1;
        }
        count
    }

    /// Pushes records from an iterator, consuming them (zero-clone).
    ///
    /// Returns the number of records successfully pushed. Stops at the first
    /// failure (channel full or closed).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let events = vec![event1, event2, event3];
    /// let pushed = source.push_batch_drain(events.into_iter());
    /// ```
    pub fn push_batch_drain<I>(&self, records: I) -> usize
    where
        I: IntoIterator<Item = T>,
    {
        let mut count = 0;
        for record in records {
            // Update watermark if record has event time
            if let Some(event_time) = record.event_time() {
                self.inner.watermark.update(event_time);
            }

            if self
                .inner
                .producer
                .try_push(SourceMessage::Record(record))
                .is_err()
            {
                break;
            }
            self.inner.sequence.fetch_add(1, Ordering::Relaxed);
            count += 1;
        }
        count
    }

    /// Pushes an Arrow `RecordBatch` directly.
    ///
    /// This is more efficient than pushing individual records when you
    /// already have data in Arrow format.
    ///
    /// # Errors
    ///
    /// Returns `StreamingError::SchemaMismatch` if the batch schema doesn't match.
    /// Returns `StreamingError::ChannelClosed` if the sink has been dropped.
    pub fn push_arrow(&self, batch: RecordBatch) -> Result<(), StreamingError> {
        // Validate schema matches (skip for type-erased sources with empty schema)
        if !self.inner.schema.fields().is_empty() && batch.schema() != self.inner.schema {
            return Err(StreamingError::SchemaMismatch {
                expected: self
                    .inner
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect(),
                actual: batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect(),
            });
        }

        self.inner
            .producer
            .push(SourceMessage::Batch(batch))
            .map_err(|_| StreamingError::ChannelFull)?;

        self.inner.sequence.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Emits a watermark timestamp.
    ///
    /// Watermarks signal that no events with timestamps less than or equal
    /// to this value will arrive in the future. This enables window triggers
    /// and garbage collection.
    ///
    /// Watermarks are monotonically increasing - if a lower timestamp is
    /// passed, it will be ignored.
    pub fn watermark(&self, timestamp: i64) {
        self.inner.watermark.update(timestamp);

        // Best-effort send of watermark message
        // It's okay if this fails - the atomic watermark state is updated
        let _ = self
            .inner
            .producer
            .try_push(SourceMessage::Watermark(timestamp));
    }

    /// Returns the current watermark value.
    #[must_use]
    pub fn current_watermark(&self) -> i64 {
        self.inner.watermark.get()
    }

    /// Returns the schema for this source.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.inner.schema)
    }

    /// Returns the source name, if configured.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.inner.name.as_deref()
    }

    /// Returns true if the source is in MPSC mode.
    #[must_use]
    pub fn is_mpsc(&self) -> bool {
        self.inner.producer.is_mpsc()
    }

    /// Returns the channel mode.
    #[must_use]
    pub fn mode(&self) -> ChannelMode {
        self.inner.producer.mode()
    }

    /// Returns true if the sink has been dropped.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.producer.is_closed()
    }

    /// Returns the number of pending items in the buffer.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.inner.producer.len()
    }

    /// Returns the buffer capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.inner.producer.capacity()
    }

    /// Returns the current sequence number (total successful pushes).
    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.inner.sequence.load(Ordering::Acquire)
    }

    /// Returns the shared sequence counter for checkpoint registration.
    #[must_use]
    pub fn sequence_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.inner.sequence)
    }

    /// Returns the shared watermark atomic for checkpoint registration.
    #[must_use]
    pub fn watermark_atomic(&self) -> Arc<AtomicI64> {
        self.inner.watermark.arc()
    }
}

impl<T: Record> Clone for Source<T> {
    /// Clones the source, triggering automatic SPSC → MPSC upgrade.
    ///
    /// # Performance Warning
    ///
    /// **This method allocates a new `Arc<SourceInner>`.** The first clone also
    /// triggers an upgrade from SPSC to MPSC mode, which adds synchronization
    /// overhead to all subsequent `push` operations.
    ///
    /// For maximum performance with a single producer, avoid cloning the source.
    /// Use clones only when you genuinely need multiple producer threads.
    fn clone(&self) -> Self {
        // Clone the producer (triggers MPSC upgrade)
        let producer = self.inner.producer.clone();

        // Create new inner with cloned producer.
        // Sequence and watermark are shared across clones so the checkpoint
        // manager sees a single, consistent counter per logical source.
        Self {
            inner: Arc::new(SourceInner {
                producer,
                watermark: SourceWatermark::from_arc(self.inner.watermark.arc()),
                schema: Arc::clone(&self.inner.schema),
                name: self.inner.name.clone(),
                sequence: Arc::clone(&self.inner.sequence),
            }),
        }
    }
}

impl<T: Record + std::fmt::Debug> std::fmt::Debug for Source<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Source")
            .field("name", &self.inner.name)
            .field("mode", &self.mode())
            .field("pending", &self.pending())
            .field("capacity", &self.capacity())
            .field("watermark", &self.current_watermark())
            .finish()
    }
}

/// Creates a new Source/Sink pair with default configuration.
///
/// This is the primary entry point for creating streaming pipelines.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::streaming;
///
/// let (source, sink) = streaming::create::<MyEvent>(1024);
///
/// // Push data
/// source.push(event)?;
///
/// // Consume data
/// let subscription = sink.subscribe();
/// while let Some(batch) = subscription.poll() {
///     // Process batch
/// }
/// ```
#[must_use]
pub fn create<T: Record>(buffer_size: usize) -> (Source<T>, Sink<T>) {
    Source::new(SourceConfig::with_buffer_size(buffer_size))
}

/// Creates a new Source/Sink pair with custom configuration.
#[must_use]
pub fn create_with_config<T: Record>(config: SourceConfig) -> (Source<T>, Sink<T>) {
    Source::new(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    // Test record type
    #[derive(Clone, Debug)]
    struct TestEvent {
        id: i64,
        value: f64,
        timestamp: i64,
    }

    impl Record for TestEvent {
        fn schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Float64, false),
                Field::new("timestamp", DataType::Int64, false),
            ]))
        }

        fn to_record_batch(&self) -> RecordBatch {
            RecordBatch::try_new(
                Self::schema(),
                vec![
                    Arc::new(Int64Array::from(vec![self.id])),
                    Arc::new(Float64Array::from(vec![self.value])),
                    Arc::new(Int64Array::from(vec![self.timestamp])),
                ],
            )
            .unwrap()
        }

        fn event_time(&self) -> Option<i64> {
            Some(self.timestamp)
        }
    }

    #[test]
    fn test_create_source_sink() {
        let (source, _sink) = create::<TestEvent>(1024);

        assert!(!source.is_mpsc());
        assert!(!source.is_closed());
        assert_eq!(source.pending(), 0);
    }

    #[test]
    fn test_push_single() {
        let (source, _sink) = create::<TestEvent>(16);

        let event = TestEvent {
            id: 1,
            value: 42.0,
            timestamp: 1000,
        };

        assert!(source.push(event).is_ok());
        assert_eq!(source.pending(), 1);
    }

    #[test]
    fn test_try_push() {
        let (source, _sink) = create::<TestEvent>(16);

        let event = TestEvent {
            id: 1,
            value: 42.0,
            timestamp: 1000,
        };

        assert!(source.try_push(event).is_ok());
    }

    #[test]
    fn test_push_batch() {
        let (source, _sink) = create::<TestEvent>(16);

        let events = vec![
            TestEvent {
                id: 1,
                value: 1.0,
                timestamp: 1000,
            },
            TestEvent {
                id: 2,
                value: 2.0,
                timestamp: 2000,
            },
            TestEvent {
                id: 3,
                value: 3.0,
                timestamp: 3000,
            },
        ];

        let count = source.push_batch(&events);
        assert_eq!(count, 3);
        assert_eq!(source.pending(), 3);
    }

    #[test]
    fn test_push_arrow() {
        let (source, _sink) = create::<TestEvent>(16);

        let batch = RecordBatch::try_new(
            TestEvent::schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
                Arc::new(Int64Array::from(vec![1000, 2000, 3000])),
            ],
        )
        .unwrap();

        assert!(source.push_arrow(batch).is_ok());
    }

    #[test]
    fn test_push_arrow_schema_mismatch() {
        let (source, _sink) = create::<TestEvent>(16);

        // Create batch with different schema
        let wrong_schema = Arc::new(Schema::new(vec![Field::new(
            "wrong",
            DataType::Utf8,
            false,
        )]));

        let batch = RecordBatch::try_new(
            wrong_schema,
            vec![Arc::new(StringArray::from(vec!["test"]))],
        )
        .unwrap();

        let result = source.push_arrow(batch);
        assert!(matches!(result, Err(StreamingError::SchemaMismatch { .. })));
    }

    #[test]
    fn test_watermark() {
        let (source, _sink) = create::<TestEvent>(16);

        assert_eq!(source.current_watermark(), i64::MIN);

        source.watermark(1000);
        assert_eq!(source.current_watermark(), 1000);

        source.watermark(2000);
        assert_eq!(source.current_watermark(), 2000);

        // Watermark should not go backwards
        source.watermark(1500);
        assert_eq!(source.current_watermark(), 2000);
    }

    #[test]
    fn test_watermark_from_event_time() {
        let (source, _sink) = create::<TestEvent>(16);

        let event = TestEvent {
            id: 1,
            value: 42.0,
            timestamp: 5000,
        };

        source.push(event).unwrap();

        // Watermark should be updated from event time
        assert_eq!(source.current_watermark(), 5000);
    }

    #[test]
    fn test_clone_upgrades_to_mpsc() {
        let (source, _sink) = create::<TestEvent>(16);

        assert!(!source.is_mpsc());
        assert_eq!(source.mode(), ChannelMode::Spsc);

        let source2 = source.clone();

        assert!(source.is_mpsc());
        assert!(source2.is_mpsc());
    }

    #[test]
    fn test_closed_on_sink_drop() {
        let (source, sink) = create::<TestEvent>(16);

        assert!(!source.is_closed());

        drop(sink);

        assert!(source.is_closed());
    }

    #[test]
    fn test_schema() {
        let (source, _sink) = create::<TestEvent>(16);

        let schema = source.schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "value");
        assert_eq!(schema.field(2).name(), "timestamp");
    }

    #[test]
    fn test_named_source() {
        let config = SourceConfig::named("my_source");
        let (source, _sink) = create_with_config::<TestEvent>(config);

        assert_eq!(source.name(), Some("my_source"));
    }

    #[test]
    fn test_debug_format() {
        let (source, _sink) = create::<TestEvent>(16);

        let debug = format!("{source:?}");
        assert!(debug.contains("Source"));
        assert!(debug.contains("Spsc"));
    }
}
