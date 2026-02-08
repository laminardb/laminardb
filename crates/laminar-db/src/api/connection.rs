//! Thread-safe database connection for FFI.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use super::error::ApiError;
use super::ingestion::Writer;
use super::query::{QueryResult, QueryStream};
use crate::{LaminarConfig, LaminarDB};

/// Thread-safe database connection for FFI.
///
/// This is a wrapper around `LaminarDB` that provides:
/// - Explicit `Send + Sync` markers for FFI safety
/// - `close()` method returning `Result` for error handling
/// - Untyped Arrow API without Rust trait bounds
///
/// # Example
///
/// ```rust,ignore
/// use laminar_db::api::Connection;
///
/// let conn = Connection::open()?;
/// conn.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")?;
///
/// let result = conn.query("SELECT * FROM trades")?;
/// println!("Got {} rows", result.num_rows());
///
/// conn.close()?;
/// ```
pub struct Connection {
    inner: Arc<LaminarDB>,
}

impl Connection {
    /// Open an in-memory database with default settings.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if database creation fails.
    pub fn open() -> Result<Self, ApiError> {
        let db = LaminarDB::open().map_err(ApiError::from)?;
        Ok(Self {
            inner: Arc::new(db),
        })
    }

    /// Open with custom configuration.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if database creation fails.
    pub fn open_with_config(config: LaminarConfig) -> Result<Self, ApiError> {
        let db = LaminarDB::open_with_config(config).map_err(ApiError::from)?;
        Ok(Self {
            inner: Arc::new(db),
        })
    }

    /// Execute a SQL statement (blocking wrapper around async).
    ///
    /// Supports: `CREATE SOURCE/SINK/STREAM`, `DROP`, `SELECT`, `INSERT INTO`,
    /// `SHOW`, `DESCRIBE`, `EXPLAIN`, `CREATE MATERIALIZED VIEW`.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if SQL parsing, planning, or execution fails.
    ///
    /// # Panics
    ///
    /// Panics if the internal thread used for async execution panics.
    pub fn execute(&self, sql: &str) -> Result<ExecuteResult, ApiError> {
        if self.inner.is_closed() {
            return Err(ApiError::shutdown());
        }

        // Create or use existing runtime for blocking
        let result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // Already in async context - use spawn_blocking to avoid nesting
            std::thread::scope(|s| {
                s.spawn(|| {
                    let inner = Arc::clone(&self.inner);
                    let sql = sql.to_string();
                    handle.block_on(async move { inner.execute(&sql).await })
                })
                .join()
                .unwrap()
            })
        } else {
            // No runtime - create one
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| ApiError::internal(format!("Runtime error: {e}")))?;

            rt.block_on(self.inner.execute(sql))
        };

        result.map(ExecuteResult::from).map_err(ApiError::from)
    }

    /// Execute SQL and wait for all results (materialized).
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if execution fails or the result is not a query.
    pub fn query(&self, sql: &str) -> Result<QueryResult, ApiError> {
        let result = self.execute(sql)?;
        match result {
            ExecuteResult::Query(stream) => stream.collect(),
            ExecuteResult::Metadata(batch) => Ok(QueryResult::from_batch(batch)),
            ExecuteResult::RowsAffected(n) => Err(ApiError::Query {
                code: super::error::codes::QUERY_FAILED,
                message: format!("Expected query result, got {n} rows affected"),
            }),
            ExecuteResult::Ddl(info) => Err(ApiError::Query {
                code: super::error::codes::QUERY_FAILED,
                message: format!("Expected query result, got DDL: {}", info.statement_type),
            }),
        }
    }

    /// Execute SQL with streaming results.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if execution fails or the result is not a query.
    pub fn query_stream(&self, sql: &str) -> Result<QueryStream, ApiError> {
        let result = self.execute(sql)?;
        match result {
            ExecuteResult::Query(stream) => Ok(stream),
            _ => Err(ApiError::Query {
                code: super::error::codes::QUERY_FAILED,
                message: "Expected streaming query result".into(),
            }),
        }
    }

    /// Get a writer for inserting data into a source.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if the source is not found.
    pub fn writer(&self, source_name: &str) -> Result<Writer, ApiError> {
        let handle = self
            .inner
            .source_untyped(source_name)
            .map_err(ApiError::from)?;
        Ok(Writer::new(handle))
    }

    /// Insert a `RecordBatch` directly into a source.
    ///
    /// Returns the number of rows inserted.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if the source is not found or ingestion fails.
    pub fn insert(&self, source_name: &str, batch: RecordBatch) -> Result<u64, ApiError> {
        let handle = self
            .inner
            .source_untyped(source_name)
            .map_err(ApiError::from)?;
        let num_rows = batch.num_rows() as u64;
        handle
            .push_arrow(batch)
            .map_err(|e| ApiError::ingestion(e.to_string()))?;
        Ok(num_rows)
    }

    /// Get schema for a source or stream.
    ///
    /// # Errors
    ///
    /// Returns `ApiError::table_not_found` if the name is not found.
    pub fn get_schema(&self, name: &str) -> Result<SchemaRef, ApiError> {
        // Check sources
        for source in self.inner.sources() {
            if source.name == name {
                return Ok(source.schema);
            }
        }
        Err(ApiError::table_not_found(name))
    }

    /// List all source names.
    #[must_use]
    pub fn list_sources(&self) -> Vec<String> {
        self.inner.sources().into_iter().map(|s| s.name).collect()
    }

    /// List all stream names.
    #[must_use]
    pub fn list_streams(&self) -> Vec<String> {
        self.inner.streams().into_iter().map(|s| s.name).collect()
    }

    /// List all sink names.
    #[must_use]
    pub fn list_sinks(&self) -> Vec<String> {
        self.inner.sinks().into_iter().map(|s| s.name).collect()
    }

    /// Start the streaming pipeline.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if the pipeline cannot be started.
    ///
    /// # Panics
    ///
    /// Panics if the internal thread used for async execution panics.
    pub fn start(&self) -> Result<(), ApiError> {
        let result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            std::thread::scope(|s| {
                s.spawn(|| {
                    let inner = Arc::clone(&self.inner);
                    handle.block_on(async move { inner.start().await })
                })
                .join()
                .unwrap()
            })
        } else {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| ApiError::internal(format!("Runtime error: {e}")))?;
            rt.block_on(self.inner.start())
        };

        result.map_err(ApiError::from)
    }

    /// Explicitly close the connection.
    ///
    /// Unlike `Drop`, this returns errors and ensures cleanup completes.
    ///
    /// # Errors
    ///
    /// Currently always succeeds.
    #[allow(clippy::unnecessary_wraps)]
    pub fn close(self) -> Result<(), ApiError> {
        // Signal shutdown
        self.inner.close();

        // Try to get exclusive ownership for cleanup
        match Arc::try_unwrap(self.inner) {
            Ok(_db) => {
                // Exclusive ownership - cleanup happens in drop
                Ok(())
            }
            Err(_arc) => {
                // Other references exist - still marked as closed
                Ok(())
            }
        }
    }

    /// Check if the connection is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Trigger a checkpoint.
    ///
    /// Returns the checkpoint ID on success.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if checkpointing fails.
    pub fn checkpoint(&self) -> Result<Option<u64>, ApiError> {
        self.inner.checkpoint().map_err(ApiError::from)
    }

    /// Check if checkpointing is enabled.
    #[must_use]
    pub fn is_checkpoint_enabled(&self) -> bool {
        self.inner.is_checkpoint_enabled()
    }

    // ── Catalog info ──

    /// List source info with schemas and watermark columns.
    #[must_use]
    pub fn source_info(&self) -> Vec<crate::SourceInfo> {
        self.inner.sources()
    }

    /// List sink info.
    #[must_use]
    pub fn sink_info(&self) -> Vec<crate::SinkInfo> {
        self.inner.sinks()
    }

    /// List stream info with SQL.
    #[must_use]
    pub fn stream_info(&self) -> Vec<crate::StreamInfo> {
        self.inner.streams()
    }

    /// List active/completed query info.
    #[must_use]
    pub fn query_info(&self) -> Vec<crate::QueryInfo> {
        self.inner.queries()
    }

    // ── Pipeline topology & state ──

    /// Get the pipeline topology graph.
    #[must_use]
    pub fn pipeline_topology(&self) -> crate::PipelineTopology {
        self.inner.pipeline_topology()
    }

    /// Get the pipeline state as a string ("Created", "Running", "Stopped", etc.).
    #[must_use]
    pub fn pipeline_state(&self) -> String {
        self.inner.pipeline_state().to_string()
    }

    /// Get the global pipeline watermark.
    #[must_use]
    pub fn pipeline_watermark(&self) -> i64 {
        self.inner.pipeline_watermark()
    }

    /// Get total events processed across all sources.
    #[must_use]
    pub fn total_events_processed(&self) -> u64 {
        self.inner.total_events_processed()
    }

    /// Get the number of registered sources.
    #[must_use]
    pub fn source_count(&self) -> usize {
        self.inner.source_count()
    }

    /// Get the number of registered sinks.
    #[must_use]
    pub fn sink_count(&self) -> usize {
        self.inner.sink_count()
    }

    /// Get the number of active queries.
    #[must_use]
    pub fn active_query_count(&self) -> usize {
        self.inner.active_query_count()
    }

    // ── Metrics ──

    /// Get pipeline-wide metrics snapshot.
    #[must_use]
    pub fn metrics(&self) -> crate::PipelineMetrics {
        self.inner.metrics()
    }

    /// Get metrics for a specific source.
    #[must_use]
    pub fn source_metrics(&self, name: &str) -> Option<crate::SourceMetrics> {
        self.inner.source_metrics(name)
    }

    /// Get metrics for all sources.
    #[must_use]
    pub fn all_source_metrics(&self) -> Vec<crate::SourceMetrics> {
        self.inner.all_source_metrics()
    }

    /// Get metrics for a specific stream.
    #[must_use]
    pub fn stream_metrics(&self, name: &str) -> Option<crate::StreamMetrics> {
        self.inner.stream_metrics(name)
    }

    /// Get metrics for all streams.
    #[must_use]
    pub fn all_stream_metrics(&self) -> Vec<crate::StreamMetrics> {
        self.inner.all_stream_metrics()
    }

    // ── Query control ──

    /// Cancel a running query by ID.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if the query is not found.
    pub fn cancel_query(&self, query_id: u64) -> Result<(), ApiError> {
        self.inner.cancel_query(query_id).map_err(ApiError::from)
    }

    // ── Shutdown ──

    /// Gracefully shut down the streaming pipeline.
    ///
    /// Unlike `close()`, this waits for in-flight events to drain.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if the shutdown fails.
    ///
    /// # Panics
    ///
    /// Panics if the internal thread used for async execution panics.
    pub fn shutdown(&self) -> Result<(), ApiError> {
        let result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            std::thread::scope(|s| {
                s.spawn(|| {
                    let inner = Arc::clone(&self.inner);
                    handle.block_on(async move { inner.shutdown().await })
                })
                .join()
                .unwrap()
            })
        } else {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| ApiError::internal(format!("Runtime error: {e}")))?;
            rt.block_on(self.inner.shutdown())
        };
        result.map_err(ApiError::from)
    }

    // ── Subscription ──

    /// Subscribe to a named stream, returning an `ArrowSubscription`.
    ///
    /// The stream must already exist (created via `CREATE STREAM ... AS SELECT ...`).
    /// Returns a channel-based subscription that delivers `RecordBatch`es as
    /// they are produced by the streaming pipeline.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if the stream is not found.
    pub fn subscribe(
        &self,
        stream_name: &str,
    ) -> Result<super::subscription::ArrowSubscription, ApiError> {
        let sub = self
            .inner
            .subscribe_raw(stream_name)
            .map_err(ApiError::from)?;
        Ok(super::subscription::ArrowSubscription::new(
            sub,
            std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        ))
    }
}

// Thread safety guarantees for FFI.
// SAFETY: LaminarDB uses Arc, Mutex, and atomic types internally.
// All public methods take &self and use internal synchronization.
unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

/// Result of executing a SQL statement (FFI variant).
#[derive(Debug)]
pub enum ExecuteResult {
    /// DDL statement completed (CREATE, DROP, ALTER).
    Ddl(DdlInfo),
    /// Query is running, results available via stream.
    Query(QueryStream),
    /// Rows were affected (INSERT INTO).
    RowsAffected(u64),
    /// Metadata result (SHOW, DESCRIBE).
    Metadata(RecordBatch),
}

/// Information about a completed DDL statement.
#[derive(Debug, Clone)]
pub struct DdlInfo {
    /// The statement type (e.g., "CREATE SOURCE").
    pub statement_type: String,
    /// The object name affected.
    pub object_name: String,
}

impl From<crate::ExecuteResult> for ExecuteResult {
    fn from(result: crate::ExecuteResult) -> Self {
        match result {
            crate::ExecuteResult::Ddl(info) => Self::Ddl(DdlInfo {
                statement_type: info.statement_type,
                object_name: info.object_name,
            }),
            crate::ExecuteResult::Query(handle) => Self::Query(QueryStream::from_handle(handle)),
            crate::ExecuteResult::RowsAffected(n) => Self::RowsAffected(n),
            crate::ExecuteResult::Metadata(batch) => Self::Metadata(batch),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Connection>();
    }

    #[test]
    fn test_connection_open_close() {
        let conn = Connection::open().unwrap();
        assert!(!conn.is_closed());
        conn.close().unwrap();
    }

    #[test]
    fn test_connection_thread_safe() {
        let conn = Arc::new(Connection::open().unwrap());

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let conn = Arc::clone(&conn);
                std::thread::spawn(move || {
                    let _ = conn.list_sources();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_execute_create_source() {
        let conn = Connection::open().unwrap();
        let result = conn.execute("CREATE SOURCE test_api (id BIGINT, name VARCHAR)");
        assert!(result.is_ok());

        let sources = conn.list_sources();
        assert!(sources.contains(&"test_api".to_string()));
    }

    #[test]
    fn test_get_schema() {
        let conn = Connection::open().unwrap();
        conn.execute("CREATE SOURCE schema_test (id BIGINT, value DOUBLE)")
            .unwrap();

        let schema = conn.get_schema("schema_test").unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
    }

    #[test]
    fn test_get_schema_not_found() {
        let conn = Connection::open().unwrap();
        let result = conn.get_schema("nonexistent");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().code(),
            super::super::error::codes::TABLE_NOT_FOUND
        );
    }

    #[test]
    fn test_source_info() {
        let conn = Connection::open().unwrap();
        conn.execute("CREATE SOURCE test_info (id BIGINT, name VARCHAR)")
            .unwrap();
        let info = conn.source_info();
        assert_eq!(info.len(), 1);
        assert_eq!(info[0].name, "test_info");
        assert_eq!(info[0].schema.fields().len(), 2);
    }

    #[test]
    fn test_pipeline_state() {
        let conn = Connection::open().unwrap();
        let state = conn.pipeline_state();
        assert!(!state.is_empty());
    }

    #[test]
    fn test_metrics() {
        let conn = Connection::open().unwrap();
        let m = conn.metrics();
        assert_eq!(m.total_events_ingested, 0);
    }

    #[test]
    fn test_source_count() {
        let conn = Connection::open().unwrap();
        assert_eq!(conn.source_count(), 0);
        conn.execute("CREATE SOURCE cnt_test (x BIGINT)").unwrap();
        assert_eq!(conn.source_count(), 1);
    }

    #[test]
    fn test_cancel_query_invalid() {
        let conn = Connection::open().unwrap();
        // Cancelling a non-existent query should succeed (no-op in current impl)
        let result = conn.cancel_query(999);
        assert!(result.is_ok());
    }

    #[test]
    fn test_shutdown() {
        let conn = Connection::open().unwrap();
        assert!(conn.shutdown().is_ok());
    }
}
