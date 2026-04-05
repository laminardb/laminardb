//! Source and sink catalog for tracking registered streaming objects.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parking_lot::RwLock;
use tokio::sync::Notify;

use laminar_core::streaming::{self, BackpressureStrategy, SourceConfig, WaitStrategy};

#[derive(Clone, Debug)]
pub(crate) struct ArrowRecord {
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
    /// Push a batch to both the SPSC channel and the snapshot ring.
    pub(crate) fn push_and_buffer(
        &self,
        batch: RecordBatch,
    ) -> Result<(), laminar_core::streaming::StreamingError> {
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
    pub(crate) source: streaming::Source<ArrowRecord>,
    pub(crate) sink: streaming::Sink<ArrowRecord>,
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
        watermark_column: Option<String>,
        max_out_of_orderness: Option<Duration>,
        buffer_size: Option<usize>,
        backpressure: Option<BackpressureStrategy>,
    ) -> Result<Arc<SourceEntry>, crate::DbError> {
        let mut sources = self.sources.write();
        if sources.contains_key(name) {
            return Err(crate::DbError::SourceAlreadyExists(name.to_string()));
        }

        let buf_size = buffer_size.unwrap_or(self.default_buffer_size);
        let bp = backpressure.unwrap_or(self.default_backpressure);

        let config = SourceConfig {
            channel: streaming::ChannelConfig {
                buffer_size: buf_size,
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

    pub(crate) fn register_source_or_replace(
        &self,
        name: &str,
        schema: SchemaRef,
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

        let config = SourceConfig {
            channel: streaming::ChannelConfig {
                buffer_size: self.default_buffer_size,
                backpressure: self.default_backpressure,
                wait_strategy: WaitStrategy::SpinYield,
                track_stats: false,
            },
            name: Some(name.to_string()),
        };

        let (source, sink) = streaming::create_with_config::<ArrowRecord>(config);

        streams.insert(
            name.to_string(),
            Arc::new(StreamEntry {
                name: name.to_string(),
                source,
                sink,
            }),
        );
        Ok(())
    }

    pub(crate) fn get_stream_subscription(
        &self,
        name: &str,
    ) -> Option<streaming::Subscription<ArrowRecord>> {
        self.streams
            .read()
            .get(name)
            .map(|entry| entry.sink.subscribe())
    }

    pub(crate) fn get_stream_entry(&self, name: &str) -> Option<Arc<StreamEntry>> {
        self.streams.read().get(name).cloned()
    }

    pub(crate) fn get_stream_source(&self, name: &str) -> Option<streaming::Source<ArrowRecord>> {
        self.streams
            .read()
            .get(name)
            .map(|entry| entry.source.clone())
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
        if let Some(entry) = self.queries.write().get_mut(&id) {
            entry.active = false;
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
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]))
    }

    #[test]
    fn test_register_source() {
        let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
        let result = catalog.register_source("test", test_schema(), None, None, None, None);
        assert!(result.is_ok());
        assert!(catalog.get_source("test").is_some());
    }

    #[test]
    fn test_register_duplicate_source() {
        let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
        catalog
            .register_source("test", test_schema(), None, None, None, None)
            .unwrap();
        let result = catalog.register_source("test", test_schema(), None, None, None, None);
        assert!(matches!(
            result,
            Err(crate::DbError::SourceAlreadyExists(_))
        ));
    }

    #[test]
    fn test_drop_source() {
        let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
        catalog
            .register_source("test", test_schema(), None, None, None, None)
            .unwrap();
        assert!(catalog.drop_source("test"));
        assert!(catalog.get_source("test").is_none());
    }

    #[test]
    fn test_list_sources() {
        let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
        catalog
            .register_source("a", test_schema(), None, None, None, None)
            .unwrap();
        catalog
            .register_source("b", test_schema(), None, None, None, None)
            .unwrap();
        let mut names = catalog.list_sources();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn test_register_sink() {
        let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
        assert!(catalog.register_sink("output", "events").is_ok());
        assert_eq!(catalog.list_sinks(), vec!["output"]);
    }

    #[test]
    fn test_register_query() {
        let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
        let id = catalog.register_query("SELECT * FROM events");
        assert_eq!(id, 1);
        let queries = catalog.list_queries();
        assert_eq!(queries.len(), 1);
        assert!(queries[0].2); // active
    }

    #[test]
    fn test_deactivate_query() {
        let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
        let id = catalog.register_query("SELECT * FROM events");
        catalog.deactivate_query(id);
        let queries = catalog.list_queries();
        assert!(!queries[0].2); // inactive
    }

    #[test]
    fn test_describe_source() {
        let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
        let schema = test_schema();
        catalog
            .register_source("test", schema.clone(), None, None, None, None)
            .unwrap();
        let result = catalog.describe_source("test");
        assert!(result.is_some());
        assert_eq!(result.unwrap().fields().len(), 2);
    }

    #[test]
    fn test_or_replace() {
        let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
        catalog
            .register_source("test", test_schema(), None, None, None, None)
            .unwrap();
        let entry = catalog.register_source_or_replace(
            "test",
            test_schema(),
            Some("ts".into()),
            None,
            None,
            None,
        );
        assert_eq!(entry.watermark_column, Some("ts".to_string()));
    }

    #[test]
    fn test_push_and_buffer_snapshot() {
        let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
        let schema = test_schema();
        let entry = catalog
            .register_source("test", schema.clone(), None, None, None, None)
            .unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Float64Array::from(vec![1.5])),
            ],
        )
        .unwrap();

        entry.push_and_buffer(batch).unwrap();
        let snap = entry.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].num_rows(), 1);
    }

    #[test]
    fn test_buffer_capacity_drops_oldest() {
        // Use a small buffer size so we can test overflow
        let catalog = SourceCatalog::new(2, BackpressureStrategy::DropOldest);
        let schema = test_schema();
        let entry = catalog
            .register_source("test", schema.clone(), None, None, None, None)
            .unwrap();

        let values: [(i64, f64); 3] = [(0, 1.0), (1, 2.0), (2, 3.0)];
        for (id, val) in values {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(arrow::array::Int64Array::from(vec![id])),
                    Arc::new(arrow::array::Float64Array::from(vec![val])),
                ],
            )
            .unwrap();
            entry.push_and_buffer(batch).unwrap();
        }

        let snap = entry.snapshot();
        // buffer_capacity=2, so only the last 2 batches should remain
        assert_eq!(snap.len(), 2);
        let col = snap[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 1); // batch 0 was dropped
    }
}
