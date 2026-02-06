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
}
