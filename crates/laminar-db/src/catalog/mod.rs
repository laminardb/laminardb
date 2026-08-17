//! Source and sink catalog for tracking registered streaming objects.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use parking_lot::RwLock;
use tokio::sync::Notify;

use laminar_core::streaming::{
    self, BackpressureStrategy, SourceConfig, StreamingError, WaitStrategy,
};

pub(crate) fn schema_has_reserved_mutation_columns(schema: &Schema) -> bool {
    schema.fields().iter().any(|field| {
        ["_op", "__op", laminar_core::changelog::WEIGHT_COLUMN]
            .iter()
            .any(|reserved| field.name().eq_ignore_ascii_case(reserved))
    })
}

pub(crate) fn validate_source_batch(
    source_name: &str,
    expected_schema: &SchemaRef,
    primary_key: &[String],
    primary_key_indices: &[usize],
    batch: &RecordBatch,
) -> Result<(), StreamingError> {
    let actual_schema = batch.schema();
    if !Arc::ptr_eq(&actual_schema, expected_schema)
        && actual_schema.as_ref() != expected_schema.as_ref()
    {
        return Err(StreamingError::SchemaMismatch {
            expected: expected_schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect(),
            actual: actual_schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect(),
        });
    }
    if primary_key.len() != primary_key_indices.len() {
        return Err(StreamingError::InvalidConfig(format!(
            "source '{source_name}' primary-key metadata is inconsistent"
        )));
    }
    for (column, &index) in primary_key.iter().zip(primary_key_indices) {
        let null_count = batch.column(index).null_count();
        if null_count != 0 {
            return Err(StreamingError::InvalidConfig(format!(
                "source '{source_name}' primary-key column '{column}' contains {null_count} null value(s)"
            )));
        }
    }
    Ok(())
}

/// Record type for Arrow-based streaming subscriptions.
#[derive(Clone, Debug)]
pub struct ArrowRecord {
    pub(crate) batch: RecordBatch,
}

impl laminar_core::streaming::Record for ArrowRecord {
    fn schema() -> SchemaRef {
        // This is a placeholder; the actual schema is on the SourceEntry.
        // ArrowRecord is only used as a type parameter; push_arrow bypasses this.
        Arc::new(arrow::datatypes::Schema::empty())
    }

    fn to_record_batch(&self) -> RecordBatch {
        self.batch.clone()
    }
}

/// Bounded ring buffer for snapshot batches.
///
/// Concurrent `push()` calls each get a unique slot via atomic `fetch_add`.
/// Per-slot mutex protects the actual read/write.
struct SnapshotRing {
    slots: Box<[parking_lot::Mutex<Option<RecordBatch>>]>,
    tail: AtomicUsize,
    capacity: usize,
}

impl SnapshotRing {
    fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        let slots: Vec<_> = (0..cap).map(|_| parking_lot::Mutex::new(None)).collect();
        Self {
            slots: slots.into_boxed_slice(),
            tail: AtomicUsize::new(0),
            capacity: cap,
        }
    }

    fn push(&self, batch: RecordBatch) {
        // fetch_add is atomic — concurrent pushers each get a unique slot.
        let idx = self.tail.fetch_add(1, Ordering::Relaxed) % self.capacity;
        *self.slots[idx].lock() = Some(batch);
    }

    fn snapshot(&self) -> Vec<RecordBatch> {
        let tail = self.tail.load(Ordering::Acquire);
        let count = tail.min(self.capacity);
        // Read the most recent `count` slots, oldest first.
        let start = if tail <= self.capacity {
            0
        } else {
            tail % self.capacity
        };
        let mut result = Vec::with_capacity(count);
        for i in 0..count {
            let idx = (start + i) % self.capacity;
            if let Some(batch) = self.slots[idx].lock().as_ref() {
                result.push(batch.clone());
            }
        }
        result
    }
}

/// A registered source in the catalog.
pub struct SourceEntry {
    /// Source name.
    pub name: String,
    /// Arrow schema.
    pub schema: SchemaRef,
    /// Primary-key columns in declaration order.
    pub primary_key: Vec<String>,
    primary_key_indices: Vec<usize>,
    /// Watermark column name, if configured.
    pub watermark_column: Option<String>,
    /// Maximum out-of-orderness for watermark generation.
    pub max_out_of_orderness: Option<Duration>,
    /// Whether this source uses `PROCTIME()` watermarks.
    pub is_processing_time: std::sync::atomic::AtomicBool,
    pub(crate) source: streaming::Source<ArrowRecord>,
    pub(crate) sink: streaming::Sink<ArrowRecord>,
    buffer: SnapshotRing,
    /// Wakeup handle for `db.insert()` event-driven notification.
    data_notify: Arc<Notify>,
}

impl SourceEntry {
    /// Push a batch to both the channel and the snapshot ring.
    pub(crate) fn push_and_buffer(
        &self,
        batch: RecordBatch,
    ) -> Result<(), laminar_core::streaming::StreamingError> {
        validate_source_batch(
            &self.name,
            &self.schema,
            &self.primary_key,
            &self.primary_key_indices,
            &batch,
        )?;
        self.source.push_arrow(batch.clone())?;
        self.buffer.push(batch);
        self.data_notify.notify_one();
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> Vec<RecordBatch> {
        self.buffer.snapshot()
    }

    pub(crate) fn data_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.data_notify)
    }
}

pub(crate) struct SinkEntry {
    pub(crate) input: String,
}

pub(crate) struct QueryEntry {
    pub(crate) id: u64,
    pub(crate) sql: String,
    pub(crate) active: bool,
}

pub(crate) struct StreamEntry {
    pub(crate) name: String,
    emitted_rows: AtomicU64,
}

impl StreamEntry {
    pub(crate) fn record_emitted_rows(&self, rows: u64) {
        self.emitted_rows.fetch_add(rows, Ordering::Relaxed);
    }

    pub(crate) fn emitted_rows(&self) -> u64 {
        self.emitted_rows.load(Ordering::Relaxed)
    }
}

/// Central registry of sources, sinks, streams, and queries.
pub struct SourceCatalog {
    sources: RwLock<HashMap<String, Arc<SourceEntry>>>,
    sinks: RwLock<HashMap<String, SinkEntry>>,
    streams: RwLock<HashMap<String, Arc<StreamEntry>>>,
    queries: RwLock<HashMap<u64, QueryEntry>>,
    next_query_id: AtomicU64,
    default_buffer_size: usize,
    default_backpressure: BackpressureStrategy,
}

impl SourceCatalog {
    /// Create a catalog with the given defaults for new sources.
    #[must_use]
    pub fn new(buffer_size: usize, backpressure: BackpressureStrategy) -> Self {
        Self {
            sources: RwLock::new(HashMap::new()),
            sinks: RwLock::new(HashMap::new()),
            streams: RwLock::new(HashMap::new()),
            queries: RwLock::new(HashMap::new()),
            next_query_id: AtomicU64::new(1),
            default_buffer_size: buffer_size,
            default_backpressure: backpressure,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn register_source(
        &self,
        name: &str,
        schema: SchemaRef,
        primary_key: Vec<String>,
        watermark_column: Option<String>,
        max_out_of_orderness: Option<Duration>,
        buffer_size: Option<usize>,
        backpressure: Option<BackpressureStrategy>,
    ) -> Result<Arc<SourceEntry>, crate::DbError> {
        let mut sources = self.sources.write();
        if sources.contains_key(name) {
            return Err(crate::DbError::SourceAlreadyExists(name.to_string()));
        }

        let mut primary_key_indices = Vec::with_capacity(primary_key.len());
        for column in &primary_key {
            let index = schema.index_of(column).map_err(|_| {
                crate::DbError::InvalidOperation(format!(
                    "source '{name}' primary-key column '{column}' is absent from its schema"
                ))
            })?;
            if primary_key_indices.contains(&index) {
                return Err(crate::DbError::InvalidOperation(format!(
                    "source '{name}' primary key repeats column '{column}'"
                )));
            }
            if schema.field(index).is_nullable() {
                return Err(crate::DbError::InvalidOperation(format!(
                    "source '{name}' primary-key column '{column}' must be non-nullable"
                )));
            }
            primary_key_indices.push(index);
        }

        let buf_size = buffer_size.unwrap_or(self.default_buffer_size);
        let bp = backpressure.unwrap_or(self.default_backpressure);

        // Channel buffer is at least 1024 to avoid blocking on small snapshot rings.
        let channel_buf = buf_size.max(1024);
        let config = SourceConfig {
            channel: streaming::ChannelConfig {
                buffer_size: channel_buf,
                backpressure: bp,
                wait_strategy: WaitStrategy::SpinYield,
                track_stats: false,
            },
            name: Some(name.to_string()),
        };

        let (source, sink) = streaming::create_with_config::<ArrowRecord>(config);

        let entry = Arc::new(SourceEntry {
            name: name.to_string(),
            schema,
            primary_key,
            primary_key_indices,
            watermark_column,
            max_out_of_orderness,
            is_processing_time: std::sync::atomic::AtomicBool::new(false),
            source,
            sink,
            buffer: SnapshotRing::new(buf_size),
            data_notify: Arc::new(Notify::new()),
        });

        sources.insert(name.to_string(), Arc::clone(&entry));
        Ok(entry)
    }

    #[cfg(test)]
    pub(crate) fn register_source_or_replace(
        &self,
        name: &str,
        schema: SchemaRef,
        primary_key: Vec<String>,
        watermark_column: Option<String>,
        max_out_of_orderness: Option<Duration>,
        buffer_size: Option<usize>,
        backpressure: Option<BackpressureStrategy>,
    ) -> Arc<SourceEntry> {
        // Remove existing if present
        self.sources.write().remove(name);
        // Safe to unwrap since we just removed any conflict
        self.register_source(
            name,
            schema,
            primary_key,
            watermark_column,
            max_out_of_orderness,
            buffer_size,
            backpressure,
        )
        .unwrap()
    }

    /// Look up a registered source by name.
    pub fn get_source(&self, name: &str) -> Option<Arc<SourceEntry>> {
        self.sources.read().get(name).cloned()
    }

    /// Returns `true` if the source existed.
    pub fn drop_source(&self, name: &str) -> bool {
        self.sources.write().remove(name).is_some()
    }

    pub(crate) fn register_sink(&self, name: &str, input: &str) -> Result<(), crate::DbError> {
        let mut sinks = self.sinks.write();
        if sinks.contains_key(name) {
            return Err(crate::DbError::SinkAlreadyExists(name.to_string()));
        }
        sinks.insert(
            name.to_string(),
            SinkEntry {
                input: input.to_string(),
            },
        );
        Ok(())
    }

    /// Returns `true` if the sink existed.
    pub fn drop_sink(&self, name: &str) -> bool {
        self.sinks.write().remove(name).is_some()
    }

    pub(crate) fn register_stream(&self, name: &str) -> Result<(), crate::DbError> {
        let mut streams = self.streams.write();
        if streams.contains_key(name) {
            return Err(crate::DbError::StreamAlreadyExists(name.to_string()));
        }

        streams.insert(
            name.to_string(),
            Arc::new(StreamEntry {
                name: name.to_string(),
                emitted_rows: AtomicU64::new(0),
            }),
        );
        Ok(())
    }

    pub(crate) fn get_stream_entry(&self, name: &str) -> Option<Arc<StreamEntry>> {
        self.streams.read().get(name).cloned()
    }

    /// Returns `true` if the stream existed.
    pub fn drop_stream(&self, name: &str) -> bool {
        self.streams.write().remove(name).is_some()
    }

    /// All registered stream names.
    pub fn list_streams(&self) -> Vec<String> {
        self.streams.read().keys().cloned().collect()
    }

    /// All registered source names.
    pub fn list_sources(&self) -> Vec<String> {
        self.sources.read().keys().cloned().collect()
    }

    /// All registered sink names.
    pub fn list_sinks(&self) -> Vec<String> {
        self.sinks.read().keys().cloned().collect()
    }

    /// Input source/table name for a sink, if registered.
    pub fn get_sink_input(&self, name: &str) -> Option<String> {
        self.sinks.read().get(name).map(|e| e.input.clone())
    }

    pub(crate) fn register_query(&self, sql: &str) -> u64 {
        let id = self.next_query_id.fetch_add(1, Ordering::Relaxed);
        let mut queries = self.queries.write();
        queries.insert(
            id,
            QueryEntry {
                id,
                sql: sql.to_string(),
                active: true,
            },
        );
        id
    }

    pub(crate) fn deactivate_query(&self, id: u64) -> bool {
        // Cap retained deactivated queries so finished SELECTs can't accumulate
        // unboundedly; over the cap, the oldest (lowest id) is dropped.
        const MAX_INACTIVE_QUERIES: usize = 100;
        let mut queries = self.queries.write();
        if let Some(entry) = queries.get_mut(&id) {
            let was_active = entry.active;
            entry.active = false;
            if was_active {
                let inactive_count = queries.values().filter(|q| !q.active).count();
                if inactive_count > MAX_INACTIVE_QUERIES {
                    let oldest_inactive_id =
                        queries.values().filter(|q| !q.active).map(|q| q.id).min();
                    if let Some(oldest_id) = oldest_inactive_id {
                        queries.remove(&oldest_id);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    pub(crate) fn list_queries(&self) -> Vec<(u64, String, bool)> {
        self.queries
            .read()
            .values()
            .map(|q| (q.id, q.sql.clone(), q.active))
            .collect()
    }

    /// Schema for DESCRIBE queries.
    pub fn describe_source(&self, name: &str) -> Option<SchemaRef> {
        self.sources.read().get(name).map(|e| e.schema.clone())
    }
}

#[cfg(test)]
mod tests;
