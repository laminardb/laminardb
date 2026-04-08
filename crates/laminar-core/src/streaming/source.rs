//! Source — entry point for data into a streaming pipeline.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use super::channel::{channel_with_config, Producer};
use super::config::SourceConfig;
use super::error::{StreamingError, TryPushError};
use super::sink::Sink;

/// Trait for types that can be streamed through a Source.
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
        arrow::compute::concat_batches(&Self::schema(), &batches)
            .unwrap_or_else(|_| RecordBatch::new_empty(Self::schema()))
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

    /// Event-time column name set via programmatic API.
    /// Read once at pipeline startup, not on the hot path.
    event_time_column: OnceLock<String>,
}

/// A streaming data source. Cloneable for multi-producer use.
pub struct Source<T: Record> {
    inner: Arc<SourceInner<T>>,
}

impl<T: Record> Source<T> {
    /// Creates a new Source/Sink pair.
    pub(crate) fn new(config: SourceConfig) -> (Self, Sink<T>) {
        let channel_config = config.channel;
        let (producer, consumer) = channel_with_config::<SourceMessage<T>>(&channel_config);

        let schema = T::schema();

        let inner = Arc::new(SourceInner {
            producer,
            watermark: SourceWatermark::new(),
            schema: schema.clone(),
            name: config.name,
            sequence: Arc::new(AtomicU64::new(0)),
            event_time_column: OnceLock::new(),
        });

        let source = Self { inner };
        let sink = Sink::new(consumer, schema, channel_config);

        (source, sink)
    }

    /// Pushes a record. Non-blocking — returns `ChannelFull` if the buffer is full.
    ///
    /// # Errors
    ///
    /// Returns `StreamingError::ChannelFull` if the buffer is full or the sink was dropped.
    pub fn push(&self, record: T) -> Result<(), StreamingError> {
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

    /// Pushes a record, returning it on failure.
    ///
    /// # Errors
    ///
    /// Returns `TryPushError` containing the record if the channel is full.
    pub fn try_push(&self, record: T) -> Result<(), TryPushError<T>> {
        if let Some(event_time) = record.event_time() {
            self.inner.watermark.update(event_time);
        }

        self.inner
            .producer
            .push(SourceMessage::Record(record))
            .map_err(|msg| match msg {
                SourceMessage::Record(r) => TryPushError {
                    value: r,
                    error: StreamingError::ChannelFull,
                },
                _ => unreachable!(),
            })?;

        self.inner.sequence.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Pushes multiple records (cloned). Stops at the first failure.
    pub fn push_batch(&self, records: &[T]) -> usize
    where
        T: Clone,
    {
        self.push_batch_drain(records.iter().cloned())
    }

    /// Pushes records from an iterator, consuming them (zero-clone).
    /// Stops at the first failure. Returns the number pushed.
    pub fn push_batch_drain<I>(&self, records: I) -> usize
    where
        I: IntoIterator<Item = T>,
    {
        let mut count = 0;
        for record in records {
            if self.push(record).is_err() {
                break;
            }
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

    /// Declare which column in the source data represents event time.
    ///
    /// When set, `source.watermark()` enables late-row filtering
    /// without a SQL `WATERMARK FOR` clause.
    ///
    /// Only the first call takes effect; subsequent calls are silently ignored.
    pub fn set_event_time_column(&self, column: &str) {
        let _ = self.inner.event_time_column.set(column.to_owned());
    }

    /// Returns the configured event-time column, if any.
    #[must_use]
    pub fn event_time_column(&self) -> Option<String> {
        self.inner.event_time_column.get().cloned()
    }
}

impl<T: Record> Clone for Source<T> {
    fn clone(&self) -> Self {
        let producer = self.inner.producer.clone();
        let event_time_col = self.inner.event_time_column.get().cloned();
        let event_time_column = OnceLock::new();
        if let Some(col) = event_time_col {
            let _ = event_time_column.set(col);
        }
        Self {
            inner: Arc::new(SourceInner {
                producer,
                watermark: SourceWatermark::from_arc(self.inner.watermark.arc()),
                schema: Arc::clone(&self.inner.schema),
                name: self.inner.name.clone(),
                sequence: Arc::clone(&self.inner.sequence),
                event_time_column,
            }),
        }
    }
}

impl<T: Record + std::fmt::Debug> std::fmt::Debug for Source<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Source")
            .field("name", &self.inner.name)
            .field("pending", &self.pending())
            .field("capacity", &self.capacity())
            .field("watermark", &self.current_watermark())
            .finish()
    }
}

/// Creates a new Source/Sink pair with the given buffer size.
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
    fn test_clone_multi_producer() {
        let (source, sink) = create::<TestEvent>(16);
        let source2 = source.clone();

        source
            .push(TestEvent {
                id: 1,
                value: 1.0,
                timestamp: 1000,
            })
            .unwrap();
        source2
            .push(TestEvent {
                id: 2,
                value: 2.0,
                timestamp: 2000,
            })
            .unwrap();

        let sub = sink.subscribe();
        assert!(sub.poll().is_some());
        assert!(sub.poll().is_some());
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
    }

    #[test]
    fn test_set_event_time_column() {
        let (source, _sink) = create::<TestEvent>(16);

        assert!(source.event_time_column().is_none());

        source.set_event_time_column("timestamp");
        assert_eq!(source.event_time_column(), Some("timestamp".to_string()));
    }

    #[test]
    fn test_event_time_column_preserved_on_clone() {
        let (source, _sink) = create::<TestEvent>(16);
        source.set_event_time_column("ts");

        let source2 = source.clone();
        assert_eq!(source2.event_time_column(), Some("ts".to_string()));
    }
}
