//! Source and sink catalog for tracking registered streaming objects.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parking_lot::RwLock;

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
    /// The underlying streaming source (type-erased via `ArrowRecord`).
    pub(crate) source: streaming::Source<ArrowRecord>,
    /// The underlying streaming sink (type-erased via `ArrowRecord`).
    #[allow(dead_code)]
    pub(crate) sink: streaming::Sink<ArrowRecord>,
}

/// A registered sink in the catalog.
#[allow(dead_code)]
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

/// Catalog of registered sources, sinks, and queries.
pub struct SourceCatalog {
    sources: RwLock<HashMap<String, Arc<SourceEntry>>>,
    sinks: RwLock<HashMap<String, SinkEntry>>,
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
            source,
            sink,
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
        buffer_size: Option<usize>,
        backpressure: Option<BackpressureStrategy>,
    ) -> Arc<SourceEntry> {
        // Remove existing if present
        self.sources.write().remove(name);
        // Safe to unwrap since we just removed any conflict
        self.register_source(name, schema, watermark_column, buffer_size, backpressure)
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

    /// List all source names.
    pub fn list_sources(&self) -> Vec<String> {
        self.sources.read().keys().cloned().collect()
    }

    /// List all sink names.
    pub fn list_sinks(&self) -> Vec<String> {
        self.sinks.read().keys().cloned().collect()
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
        let result = catalog.register_source("test", test_schema(), None, None, None);
        assert!(result.is_ok());
        assert!(catalog.get_source("test").is_some());
    }

    #[test]
    fn test_register_duplicate_source() {
        let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
        catalog
            .register_source("test", test_schema(), None, None, None)
            .unwrap();
        let result = catalog.register_source("test", test_schema(), None, None, None);
        assert!(matches!(result, Err(crate::DbError::SourceAlreadyExists(_))));
    }

    #[test]
    fn test_drop_source() {
        let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
        catalog
            .register_source("test", test_schema(), None, None, None)
            .unwrap();
        assert!(catalog.drop_source("test"));
        assert!(catalog.get_source("test").is_none());
    }

    #[test]
    fn test_list_sources() {
        let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
        catalog
            .register_source("a", test_schema(), None, None, None)
            .unwrap();
        catalog
            .register_source("b", test_schema(), None, None, None)
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
            .register_source("test", schema.clone(), None, None, None)
            .unwrap();
        let result = catalog.describe_source("test");
        assert!(result.is_some());
        assert_eq!(result.unwrap().fields().len(), 2);
    }

    #[test]
    fn test_or_replace() {
        let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
        catalog
            .register_source("test", test_schema(), None, None, None)
            .unwrap();
        let entry = catalog.register_source_or_replace("test", test_schema(), Some("ts".into()), None, None);
        assert_eq!(entry.watermark_column, Some("ts".to_string()));
    }
}
