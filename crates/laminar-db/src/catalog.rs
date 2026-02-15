//! Source and sink catalog for tracking registered streaming objects.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parking_lot::{Mutex, RwLock};

use laminar_core::streaming::{self, BackpressureStrategy, SourceConfig, WaitStrategy};

/// Internal record type for untyped sources (stores raw `RecordBatch`).
#[derive(Clone, Debug)]
pub(crate) struct ArrowRecord {
    /// The record batch.
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
    /// The underlying streaming source (type-erased via `ArrowRecord`).
    pub(crate) source: streaming::Source<ArrowRecord>,
    /// The underlying streaming sink (type-erased via `ArrowRecord`).
    #[allow(dead_code)] // Reserved for Phase 3 connector manager sink routing
    pub(crate) sink: streaming::Sink<ArrowRecord>,
    /// Bounded buffer of recent batches for ad-hoc snapshot queries.
    buffer: Mutex<VecDeque<RecordBatch>>,
    /// Max batches retained (defaults to channel `buffer_size`).
    buffer_capacity: usize,
}

impl SourceEntry {
    /// Push a batch to both the SPSC channel and the snapshot buffer.
    ///
    /// The snapshot buffer is bounded — oldest batches are dropped when
    /// `buffer_capacity` is exceeded.
    pub(crate) fn push_and_buffer(
        &self,
        batch: RecordBatch,
    ) -> Result<(), laminar_core::streaming::StreamingError> {
        self.source.push_arrow(batch.clone())?;
        let mut buf = self.buffer.lock();
        if buf.len() >= self.buffer_capacity {
            buf.pop_front();
        }
        buf.push_back(batch);
        Ok(())
    }

    /// Return a snapshot of all buffered batches for ad-hoc queries.
    pub(crate) fn snapshot(&self) -> Vec<RecordBatch> {
        self.buffer.lock().iter().cloned().collect()
    }
}

/// A registered sink in the catalog.
#[allow(dead_code)] // Public API for Phase 3 CREATE SINK execution
pub(crate) struct SinkEntry {
    /// Sink name.
    pub(crate) name: String,
    /// Input source or table name.
    pub(crate) input: String,
}

/// A registered query.
pub(crate) struct QueryEntry {
    /// Query identifier.
    pub(crate) id: u64,
    /// Human-readable name or SQL text.
    pub(crate) sql: String,
    /// Whether the query is still active.
    pub(crate) active: bool,
}

/// A registered stream in the catalog.
#[allow(dead_code)] // Public API for Phase 3 CREATE STREAM execution
pub(crate) struct StreamEntry {
    /// Stream name.
    pub(crate) name: String,
    /// The underlying streaming source (for pushing data into the stream).
    pub(crate) source: streaming::Source<ArrowRecord>,
    /// The underlying streaming sink (for subscribing to the stream).
    pub(crate) sink: streaming::Sink<ArrowRecord>,
}

/// Catalog of registered sources, sinks, streams, and queries.
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
    /// Create a new empty catalog.
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

    /// Register a source from a SQL CREATE SOURCE definition.
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
            source,
            sink,
            buffer: Mutex::new(VecDeque::with_capacity(buf_size)),
            buffer_capacity: buf_size,
        });

        sources.insert(name.to_string(), Arc::clone(&entry));
        Ok(entry)
    }

    /// Register a source, replacing if it already exists.
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

    /// Get a registered source by name.
    pub fn get_source(&self, name: &str) -> Option<Arc<SourceEntry>> {
        self.sources.read().get(name).cloned()
    }

    /// Remove a source by name.
    pub fn drop_source(&self, name: &str) -> bool {
        self.sources.write().remove(name).is_some()
    }

    /// Register a sink.
    pub(crate) fn register_sink(&self, name: &str, input: &str) -> Result<(), crate::DbError> {
        let mut sinks = self.sinks.write();
        if sinks.contains_key(name) {
            return Err(crate::DbError::SinkAlreadyExists(name.to_string()));
        }
        sinks.insert(
            name.to_string(),
            SinkEntry {
                name: name.to_string(),
                input: input.to_string(),
            },
        );
        Ok(())
    }

    /// Remove a sink by name.
    pub fn drop_sink(&self, name: &str) -> bool {
        self.sinks.write().remove(name).is_some()
    }

    /// Register a named stream.
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

    /// Get a subscription to a named stream.
    pub(crate) fn get_stream_subscription(
        &self,
        name: &str,
    ) -> Option<streaming::Subscription<ArrowRecord>> {
        self.streams
            .read()
            .get(name)
            .map(|entry| entry.sink.subscribe())
    }

    /// Get a stream entry by name.
    pub(crate) fn get_stream_entry(&self, name: &str) -> Option<Arc<StreamEntry>> {
        self.streams.read().get(name).cloned()
    }

    /// Get a clone of the stream's source handle (for pushing results).
    pub(crate) fn get_stream_source(&self, name: &str) -> Option<streaming::Source<ArrowRecord>> {
        self.streams
            .read()
            .get(name)
            .map(|entry| entry.source.clone())
    }

    /// Remove a stream by name.
    pub fn drop_stream(&self, name: &str) -> bool {
        self.streams.write().remove(name).is_some()
    }

    /// List all stream names.
    pub fn list_streams(&self) -> Vec<String> {
        self.streams.read().keys().cloned().collect()
    }

    /// List all source names.
    pub fn list_sources(&self) -> Vec<String> {
        self.sources.read().keys().cloned().collect()
    }

    /// List all sink names.
    pub fn list_sinks(&self) -> Vec<String> {
        self.sinks.read().keys().cloned().collect()
    }

    /// Get the input name for a registered sink.
    pub fn get_sink_input(&self, name: &str) -> Option<String> {
        self.sinks.read().get(name).map(|e| e.input.clone())
    }

    /// Register a query and return its ID.
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

    /// Mark a query as inactive.
    #[allow(dead_code)]
    pub(crate) fn deactivate_query(&self, id: u64) {
        if let Some(entry) = self.queries.write().get_mut(&id) {
            entry.active = false;
        }
    }

    /// List all queries.
    pub(crate) fn list_queries(&self) -> Vec<(u64, String, bool)> {
        self.queries
            .read()
            .values()
            .map(|q| (q.id, q.sql.clone(), q.active))
            .collect()
    }

    /// Get source schema for DESCRIBE.
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
