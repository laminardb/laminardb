//! Thread-safe database connection for FFI.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use super::error::ApiError;
use super::ingestion::Writer;
use super::query::{QueryResult, QueryStream};
use crate::{LaminarConfig, LaminarDB};

/// Thread-safe database connection for FFI.
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
        Ok(Self { inner: db })
    }

    /// Open with custom configuration.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if database creation fails.
    pub fn open_with_config(config: LaminarConfig) -> Result<Self, ApiError> {
        let db = LaminarDB::open_with_config(config).map_err(ApiError::from)?;
        Ok(Self { inner: db })
    }

    /// Execute a SQL statement (blocking wrapper around async).
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

        let result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
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
    /// Returns an error if shutdown or creation of the temporary runtime fails.
    ///
    /// # Panics
    ///
    /// Panics if the scoped shutdown thread panics.
    pub fn close(self) -> Result<(), ApiError> {
        let shutdown = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            std::thread::scope(|scope| {
                scope
                    .spawn(|| {
                        let inner = Arc::clone(&self.inner);
                        handle.block_on(async move { inner.shutdown().await })
                    })
                    .join()
                    .unwrap()
            })
        } else {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| ApiError::internal(format!("Runtime error: {error}")))?;
            runtime.block_on(self.inner.shutdown())
        };
        shutdown.map_err(ApiError::from)?;
        // Exclusive ownership → cleanup runs in drop; else other handles persist.
        match Arc::try_unwrap(self.inner) {
            Ok(_db) => Ok(()),
            Err(_arc) => Ok(()),
        }
    }

    /// Check if the connection is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Trigger a checkpoint that persists to disk.
    ///
    /// Returns the checkpoint ID on success. The pipeline must be started
    /// first via [`start()`](Self::start).
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if checkpointing fails or is not enabled.
    ///
    /// # Panics
    ///
    /// Panics if the internal thread used for async execution panics.
    pub fn checkpoint(&self) -> Result<u64, ApiError> {
        let result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            std::thread::scope(|s| {
                s.spawn(|| {
                    let inner = Arc::clone(&self.inner);
                    handle.block_on(async move { inner.checkpoint().await })
                })
                .join()
                .unwrap()
            })
        } else {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| ApiError::internal(format!("Runtime error: {e}")))?;
            rt.block_on(self.inner.checkpoint())
        };

        result.map(|r| r.checkpoint_id).map_err(ApiError::from)
    }

    /// Check if checkpointing is enabled.
    #[must_use]
    pub fn is_checkpoint_enabled(&self) -> bool {
        self.inner.is_checkpoint_enabled()
    }

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

    /// Get the pipeline topology graph.
    #[must_use]
    pub fn pipeline_topology(&self) -> crate::PipelineTopology {
        self.inner.pipeline_topology()
    }

    /// Get the pipeline state as a string.
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

    /// Cancel a running query by ID.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if the query is not found.
    pub fn cancel_query(&self, query_id: u64) -> Result<(), ApiError> {
        self.inner.cancel_query(query_id).map_err(ApiError::from)
    }

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

    /// Subscribe to a named stream from synchronous code.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if called inside an async runtime or if the stream or
    /// its output schema is unresolved.
    pub fn subscribe(
        &self,
        stream_name: &str,
    ) -> Result<super::subscription::ArrowSubscription, ApiError> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(ApiError::subscription(
                "blocking subscribe is unavailable inside an async runtime; use subscribe_async",
            ));
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| ApiError::internal(format!("Runtime error: {error}")))?;
        runtime.block_on(self.subscribe_async(stream_name))
    }

    /// Subscribe to a named stream without blocking the calling runtime.
    ///
    /// # Errors
    /// Returns `ApiError` if the stream or its output schema is unresolved.
    pub async fn subscribe_async(
        &self,
        stream_name: &str,
    ) -> Result<super::subscription::ArrowSubscription, ApiError> {
        let portal = self
            .inner
            .open_subscription(stream_name, None, crate::subscription::SubscribeStart::Tail)
            .await
            .map_err(ApiError::from)?;
        Ok(super::subscription::ArrowSubscription::new(portal))
    }
}

// SAFETY: LaminarDB uses Arc, Mutex, and atomic types internally.
unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

/// Result of executing a SQL statement.
#[derive(Debug)]
pub enum ExecuteResult {
    /// DDL statement completed.
    Ddl(crate::DdlInfo),
    /// Query running; results available via stream.
    Query(QueryStream),
    /// Rows affected (INSERT INTO).
    RowsAffected(u64),
    /// Metadata result (SHOW, DESCRIBE).
    Metadata(RecordBatch),
}

impl From<crate::ExecuteResult> for ExecuteResult {
    fn from(result: crate::ExecuteResult) -> Self {
        match result {
            crate::ExecuteResult::Ddl(info) => Self::Ddl(info),
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
    fn close_is_terminal_after_start_from_temporary_runtime() {
        let conn = Connection::open().unwrap();
        conn.execute("CREATE SOURCE events (id BIGINT)").unwrap();

        conn.start().unwrap();
        let db = Arc::clone(&conn.inner);
        assert_eq!(
            crate::db::DbState::load(&db.state),
            crate::db::DbState::Running
        );

        let inspection_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        conn.close().unwrap();
        assert_eq!(
            crate::db::DbState::load(&db.state),
            crate::db::DbState::Stopped
        );
        inspection_runtime.block_on(async {
            assert!(db.runtime_handle.lock().await.is_none());
        });
        assert!(db.owned_source_tasks.lock().is_empty());
        assert!(db.owned_sink_handles.lock().is_empty());
        assert!(db.checkpoint_namespace_lock.lock().is_none());
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
    fn named_subscription_uses_resolved_output_schema() {
        let conn = Connection::open().unwrap();
        conn.execute("CREATE SOURCE input (id BIGINT, value DOUBLE)")
            .unwrap();
        conn.execute("CREATE STREAM output AS SELECT id, value FROM input")
            .unwrap();

        let Err(unresolved) = conn.subscribe("output") else {
            panic!("unresolved stream schema must not open a subscription");
        };
        assert_eq!(
            unresolved.code(),
            super::super::error::codes::TABLE_NOT_FOUND
        );

        conn.start().unwrap();
        let subscription = conn.subscribe("output").unwrap();
        let schema = subscription.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "value");
        conn.shutdown().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_subscribe_fails_closed_inside_runtime() {
        let conn = Connection::open().unwrap();
        let Err(error) = conn.subscribe("anything") else {
            panic!("blocking subscribe must reject an async runtime");
        };
        assert_eq!(
            error.code(),
            super::super::error::codes::SUBSCRIPTION_FAILED
        );
        assert!(error.message().contains("subscribe_async"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_subscribe_opens_on_current_thread_runtime() {
        let conn = Connection::open().unwrap();
        conn.inner
            .execute("CREATE SOURCE async_input (id BIGINT)")
            .await
            .unwrap();
        conn.inner
            .execute("CREATE STREAM async_output AS SELECT id FROM async_input")
            .await
            .unwrap();
        conn.inner.start().await.unwrap();

        let mut subscription = conn.subscribe_async("async_output").await.unwrap();
        assert_eq!(subscription.schema().field(0).name(), "id");
        let error = subscription.next_frame().unwrap_err();
        assert_eq!(
            error.code(),
            super::super::error::codes::SUBSCRIPTION_FAILED
        );
        assert!(error.message().contains("next_frame_async"));
        conn.inner.shutdown().await.unwrap();
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
        let result = conn.cancel_query(999);
        assert!(result.is_err());
    }

    #[test]
    fn test_shutdown() {
        let conn = Connection::open().unwrap();
        assert!(conn.shutdown().is_ok());
    }
}
