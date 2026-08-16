//! Source — entry point for data into a streaming pipeline.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use super::channel::{channel_with_config, Producer};
use super::config::SourceConfig;
use super::error::{StreamingError, TryPushError};
use super::sink::Sink;

/// Trait for types that can be streamed through a Source.
pub trait Record: Clone + Send + Sized + 'static {
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
#[derive(Clone)]
pub(crate) enum SourceMessage<T> {
    /// A data record.
    Record(T),

    /// A batch of Arrow records.
    Batch(RecordBatch),
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

    fn restore_for_recovery(&self, timestamp: i64) {
        self.current.store(timestamp, Ordering::Release);
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

    /// Max out-of-orderness bound, paired with `event_time_column`.
    /// Read once at pipeline startup, not on the hot path.
    max_out_of_orderness: OnceLock<Duration>,
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
            max_out_of_orderness: OnceLock::new(),
        });

        let source = Self { inner };
        let sink = Sink::new(consumer, schema);

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
                SourceMessage::Batch(_) => unreachable!("only Record is pushed here"),
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
        // The shared atomic is the authoritative watermark: the pipeline's
        // watermark UDF, late-row filter, and checkpoint registration all
        // read it via `watermark_atomic()`. Subscribers receive data only,
        // so there is no in-band watermark message to emit.
        self.inner.watermark.update(timestamp);
    }

    /// Replaces the shared watermark with an exact recovered value.
    ///
    /// Unlike [`Self::watermark`], this may lower the watermark. The caller
    /// must hold the intake/compute recovery fence and ensure every clone and
    /// producer of this source is quiesced until restoration completes. Use
    /// [`Self::watermark`] during normal operation.
    pub fn restore_watermark_for_recovery(&self, timestamp: i64) {
        self.inner.watermark.restore_for_recovery(timestamp);
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

    /// Set the max out-of-orderness bound for watermark generation.
    ///
    /// Only the first call takes effect; subsequent calls are silently ignored.
    pub fn set_max_out_of_orderness(&self, dur: Duration) {
        let _ = self.inner.max_out_of_orderness.set(dur);
    }

    /// Returns the configured max out-of-orderness, if any.
    #[must_use]
    pub fn max_out_of_orderness(&self) -> Option<Duration> {
        self.inner.max_out_of_orderness.get().copied()
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
        let max_ooo = self.inner.max_out_of_orderness.get().copied();
        let max_out_of_orderness = OnceLock::new();
        if let Some(dur) = max_ooo {
            let _ = max_out_of_orderness.set(dur);
        }
        Self {
            inner: Arc::new(SourceInner {
                producer,
                watermark: SourceWatermark::from_arc(self.inner.watermark.arc()),
                schema: Arc::clone(&self.inner.schema),
                name: self.inner.name.clone(),
                sequence: Arc::clone(&self.inner.sequence),
                event_time_column,
                max_out_of_orderness,
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
mod tests;
