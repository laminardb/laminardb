# F-FFI-001: FFI API Module

## Feature Specification v1.0

**Target Phase:** Phase 3 (Connectors & Integration)
**Priority:** P0
**Estimated Complexity:** Medium (3-5 days)
**Prerequisites:** None (builds on existing laminar-db facade)

---

## Executive Summary

This specification defines the FFI API module for LaminarDB: a minimal, stable, C-compatible API surface that enables high-performance language bindings (Python, Java, Node.js, .NET). The module lives within the `laminar-db` crate behind an `api` feature flag and provides:

1. **Thread-safe handles** with explicit `Send + Sync` guarantees
2. **Zero-copy data paths** using Arrow RecordBatch at all boundaries
3. **Numeric error codes** for cross-language error handling
4. **Explicit resource management** (no reliance on Drop for critical cleanup)
5. **Untyped Arrow API** that doesn't require Rust's trait system

The design follows 2025-2026 best practices including Arrow C Data Interface for zero-copy IPC, explicit resource handles (not opaque pointers), and predictable error codes.

---

## 1. Problem Statement

### Current Limitation

LaminarDB's public API in `laminar-db` is designed for Rust consumers:

```rust
// Requires Rust-specific traits
let sub: TypedSubscription<Trade> = db.subscribe("ohlc")?;
let records: Vec<Trade> = sub.recv()?;

// Error handling via Display, not codes
match result {
    Err(DbError::SourceNotFound(name)) => // How to pass to Python?
}
```

This presents several barriers for FFI:

1. **Trait bounds**: `TypedSubscription<T: FromBatch>` requires Rust's trait system
2. **No error codes**: `DbError` uses `thiserror` Display, but FFI needs `i32` codes
3. **Implicit lifetimes**: Handles rely on `Drop` for cleanup
4. **No explicit Send/Sync**: Thread safety is implicit, not guaranteed

### Required Capability

An API module that:

1. Provides untyped variants returning `RecordBatch` directly
2. Adds numeric error codes to all error types
3. Offers explicit `close()` methods returning `Result`
4. Marks all public types as `Send + Sync` explicitly
5. Enables Arrow C Data Interface export for zero-copy IPC

### Design Principles

1. **Minimal invasion** - Wrapper types, not modifications to existing code
2. **Zero-copy data paths** - Arrow RecordBatch at all boundaries
3. **Thread-safe handles** - All exposed types must be `Send + Sync`
4. **Explicit resource management** - No reliance on Drop for critical cleanup
5. **Single API module** - One `api` module as the FFI contract

---

## 2. Detailed Design

### 2.1 Module Structure

```
laminar-db/src/
├── api/
│   ├── mod.rs          # Public re-exports, module docs
│   ├── error.rs        # ApiError with numeric codes
│   ├── connection.rs   # Connection wrapper (Send + Sync)
│   ├── schema.rs       # Schema operations extension
│   ├── ingestion.rs    # Writer type with explicit lifecycle
│   ├── query.rs        # QueryResult (materialized), QueryStream
│   └── subscription.rs # ArrowSubscription (untyped)
├── db.rs               # Existing LaminarDB (unchanged)
├── handle.rs           # Existing handles (unchanged)
└── lib.rs              # Add `#[cfg(feature = "api")] pub mod api;`
```

### 2.2 Error Types

```rust
// api/error.rs

use thiserror::Error;

/// Error codes for FFI interop.
///
/// Ranges:
/// - 100-199: Connection errors
/// - 200-299: Schema errors
/// - 300-399: Ingestion errors
/// - 400-499: Query errors
/// - 500-599: Subscription errors
/// - 900-999: Internal errors
pub mod codes {
    // Connection
    pub const CONNECTION_FAILED: i32 = 100;
    pub const CONNECTION_CLOSED: i32 = 101;
    pub const CONNECTION_IN_USE: i32 = 102;

    // Schema
    pub const TABLE_NOT_FOUND: i32 = 200;
    pub const TABLE_EXISTS: i32 = 201;
    pub const SCHEMA_MISMATCH: i32 = 202;
    pub const INVALID_SCHEMA: i32 = 203;

    // Ingestion
    pub const INGESTION_FAILED: i32 = 300;
    pub const WRITER_CLOSED: i32 = 301;
    pub const BATCH_SCHEMA_MISMATCH: i32 = 302;

    // Query
    pub const QUERY_FAILED: i32 = 400;
    pub const SQL_PARSE_ERROR: i32 = 401;
    pub const QUERY_CANCELLED: i32 = 402;

    // Subscription
    pub const SUBSCRIPTION_FAILED: i32 = 500;
    pub const SUBSCRIPTION_CLOSED: i32 = 501;
    pub const SUBSCRIPTION_TIMEOUT: i32 = 502;

    // Internal
    pub const INTERNAL_ERROR: i32 = 900;
    pub const SHUTDOWN: i32 = 901;
}

/// API error with numeric code for FFI.
#[derive(Debug, Clone, Error)]
pub enum ApiError {
    #[error("Connection error ({code}): {message}")]
    Connection { code: i32, message: String },

    #[error("Schema error ({code}): {message}")]
    Schema { code: i32, message: String },

    #[error("Ingestion error ({code}): {message}")]
    Ingestion { code: i32, message: String },

    #[error("Query error ({code}): {message}")]
    Query { code: i32, message: String },

    #[error("Subscription error ({code}): {message}")]
    Subscription { code: i32, message: String },

    #[error("Internal error ({code}): {message}")]
    Internal { code: i32, message: String },
}

impl ApiError {
    /// Get the numeric error code.
    pub fn code(&self) -> i32 {
        match self {
            Self::Connection { code, .. } => *code,
            Self::Schema { code, .. } => *code,
            Self::Ingestion { code, .. } => *code,
            Self::Query { code, .. } => *code,
            Self::Subscription { code, .. } => *code,
            Self::Internal { code, .. } => *code,
        }
    }

    /// Get the error message.
    pub fn message(&self) -> &str {
        match self {
            Self::Connection { message, .. } => message,
            Self::Schema { message, .. } => message,
            Self::Ingestion { message, .. } => message,
            Self::Query { message, .. } => message,
            Self::Subscription { message, .. } => message,
            Self::Internal { message, .. } => message,
        }
    }

    // Constructor helpers
    pub fn connection(message: impl Into<String>) -> Self {
        Self::Connection {
            code: codes::CONNECTION_FAILED,
            message: message.into(),
        }
    }

    pub fn table_not_found(table: &str) -> Self {
        Self::Schema {
            code: codes::TABLE_NOT_FOUND,
            message: format!("Table not found: {table}"),
        }
    }

    pub fn table_exists(table: &str) -> Self {
        Self::Schema {
            code: codes::TABLE_EXISTS,
            message: format!("Table already exists: {table}"),
        }
    }

    pub fn schema_mismatch(message: impl Into<String>) -> Self {
        Self::Schema {
            code: codes::SCHEMA_MISMATCH,
            message: message.into(),
        }
    }

    pub fn ingestion(message: impl Into<String>) -> Self {
        Self::Ingestion {
            code: codes::INGESTION_FAILED,
            message: message.into(),
        }
    }

    pub fn query(message: impl Into<String>) -> Self {
        Self::Query {
            code: codes::QUERY_FAILED,
            message: message.into(),
        }
    }

    pub fn sql_parse(message: impl Into<String>) -> Self {
        Self::Query {
            code: codes::SQL_PARSE_ERROR,
            message: message.into(),
        }
    }

    pub fn subscription(message: impl Into<String>) -> Self {
        Self::Subscription {
            code: codes::SUBSCRIPTION_FAILED,
            message: message.into(),
        }
    }

    pub fn subscription_closed() -> Self {
        Self::Subscription {
            code: codes::SUBSCRIPTION_CLOSED,
            message: "Subscription closed".into(),
        }
    }

    pub fn subscription_timeout() -> Self {
        Self::Subscription {
            code: codes::SUBSCRIPTION_TIMEOUT,
            message: "Subscription timeout".into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            code: codes::INTERNAL_ERROR,
            message: message.into(),
        }
    }

    pub fn shutdown() -> Self {
        Self::Internal {
            code: codes::SHUTDOWN,
            message: "Database is shut down".into(),
        }
    }
}

impl From<crate::DbError> for ApiError {
    fn from(e: crate::DbError) -> Self {
        use crate::DbError;
        match e {
            DbError::SourceNotFound(name) => Self::table_not_found(&name),
            DbError::TableNotFound(name) => Self::table_not_found(&name),
            DbError::StreamNotFound(name) => Self::table_not_found(&name),
            DbError::SourceAlreadyExists(name) => Self::table_exists(&name),
            DbError::TableAlreadyExists(name) => Self::table_exists(&name),
            DbError::StreamAlreadyExists(name) => Self::table_exists(&name),
            DbError::SchemaMismatch(msg) => Self::schema_mismatch(msg),
            DbError::InsertError(msg) => Self::ingestion(msg),
            DbError::Sql(e) => Self::sql_parse(e.to_string()),
            DbError::SqlParse(e) => Self::sql_parse(e.to_string()),
            DbError::Streaming(e) => Self::ingestion(e.to_string()),
            DbError::Shutdown => Self::shutdown(),
            other => Self::internal(other.to_string()),
        }
    }
}
```

### 2.3 Connection Type

```rust
// api/connection.rs

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::{LaminarConfig, LaminarDB};
use super::error::ApiError;
use super::query::{QueryResult, QueryStream};
use super::subscription::ArrowSubscription;
use super::ingestion::Writer;

/// Thread-safe database connection for FFI.
///
/// This is a wrapper around `LaminarDB` that provides:
/// - Explicit `Send + Sync` markers for FFI safety
/// - `close()` method returning `Result` for error handling
/// - Untyped Arrow API without Rust trait bounds
pub struct Connection {
    inner: Arc<LaminarDB>,
}

impl Connection {
    /// Open an in-memory database with default settings.
    pub fn open() -> Result<Self, ApiError> {
        let db = LaminarDB::open().map_err(ApiError::from)?;
        Ok(Self {
            inner: Arc::new(db),
        })
    }

    /// Open with custom configuration.
    pub fn open_with_config(config: LaminarConfig) -> Result<Self, ApiError> {
        let db = LaminarDB::open_with_config(config).map_err(ApiError::from)?;
        Ok(Self {
            inner: Arc::new(db),
        })
    }

    /// Execute a SQL statement (blocking wrapper around async).
    ///
    /// Supports: CREATE SOURCE/SINK/STREAM, DROP, SELECT, INSERT INTO,
    /// SHOW, DESCRIBE, EXPLAIN, CREATE MATERIALIZED VIEW.
    pub fn execute(&self, sql: &str) -> Result<ExecuteResult, ApiError> {
        if self.inner.is_closed() {
            return Err(ApiError::shutdown());
        }

        // Block on async execute
        let rt = tokio::runtime::Handle::try_current()
            .or_else(|_| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
            })
            .map_err(|e| ApiError::internal(format!("Runtime error: {e}")))?;

        rt.block_on(async { self.inner.execute(sql).await })
            .map(ExecuteResult::from)
            .map_err(ApiError::from)
    }

    /// Execute SQL and wait for all results (materialized).
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
    pub fn writer(&self, source_name: &str) -> Result<Writer, ApiError> {
        let handle = self
            .inner
            .source_untyped(source_name)
            .map_err(ApiError::from)?;
        Ok(Writer::new(handle))
    }

    /// Insert a RecordBatch directly into a source.
    ///
    /// Returns the number of rows inserted.
    pub fn insert(&self, source_name: &str, batch: RecordBatch) -> Result<u64, ApiError> {
        let handle = self
            .inner
            .source_untyped(source_name)
            .map_err(ApiError::from)?;
        handle
            .push_arrow(batch.clone())
            .map_err(|e| ApiError::ingestion(e.to_string()))?;
        Ok(batch.num_rows() as u64)
    }

    /// Subscribe to a named stream (untyped Arrow variant).
    pub fn subscribe(&self, stream_name: &str) -> Result<ArrowSubscription, ApiError> {
        // Get the stream's sink subscription
        let entry = self
            .inner
            .sources()
            .into_iter()
            .find(|s| s.name == stream_name);

        if entry.is_none() {
            // Try streams
            let streams = self.inner.streams();
            if !streams.iter().any(|s| s.name == stream_name) {
                return Err(ApiError::table_not_found(stream_name));
            }
        }

        // Use internal subscribe mechanism
        // Note: This requires adding an untyped variant to LaminarDB
        Err(ApiError::subscription(
            "Untyped subscription not yet implemented - use typed API",
        ))
    }

    /// Get schema for a source or stream.
    pub fn get_schema(&self, name: &str) -> Result<SchemaRef, ApiError> {
        // Check sources
        for source in self.inner.sources() {
            if source.name == name {
                return Ok(source.schema);
            }
        }
        Err(ApiError::table_not_found(name))
    }

    /// List all sources.
    pub fn list_sources(&self) -> Vec<String> {
        self.inner.sources().into_iter().map(|s| s.name).collect()
    }

    /// List all streams.
    pub fn list_streams(&self) -> Vec<String> {
        self.inner.streams().into_iter().map(|s| s.name).collect()
    }

    /// List all sinks.
    pub fn list_sinks(&self) -> Vec<String> {
        self.inner.sinks().into_iter().map(|s| s.name).collect()
    }

    /// Start the streaming pipeline.
    pub fn start(&self) -> Result<(), ApiError> {
        let rt = tokio::runtime::Handle::try_current()
            .or_else(|_| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
            })
            .map_err(|e| ApiError::internal(format!("Runtime error: {e}")))?;

        rt.block_on(async { self.inner.start().await })
            .map_err(ApiError::from)
    }

    /// Explicitly close the connection.
    ///
    /// Unlike `Drop`, this returns errors and ensures cleanup completes.
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
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Trigger a checkpoint.
    pub fn checkpoint(&self) -> Result<Option<u64>, ApiError> {
        self.inner.checkpoint().map_err(ApiError::from)
    }
}

// Thread safety guarantees for FFI
// SAFETY: LaminarDB uses Arc, Mutex, and atomic types internally.
// All public methods take &self and use internal synchronization.
unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

/// Result of executing a SQL statement (FFI variant).
#[derive(Debug)]
pub enum ExecuteResult {
    /// DDL statement completed.
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
```

### 2.4 Query Types

```rust
// api/query.rs

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use super::error::ApiError;

/// Materialized query result containing all batches.
#[derive(Debug, Clone)]
pub struct QueryResult {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl QueryResult {
    /// Create from a single batch.
    pub fn from_batch(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            schema,
            batches: vec![batch],
        }
    }

    /// Create from multiple batches.
    pub fn from_batches(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }

    /// Get the schema.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Get all batches by reference.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Consume and return all batches.
    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.batches
    }

    /// Total row count across all batches.
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Total column count.
    pub fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }
}

/// Streaming query result.
#[derive(Debug)]
pub struct QueryStream {
    schema: SchemaRef,
    handle: Option<crate::QueryHandle>,
    subscription: Option<crate::handle::Subscription>,
}

impl QueryStream {
    /// Create from a QueryHandle.
    pub(crate) fn from_handle(mut handle: crate::QueryHandle) -> Self {
        let schema = handle.schema().clone();
        let subscription = handle.subscribe_raw().ok();
        Self {
            schema,
            handle: Some(handle),
            subscription,
        }
    }

    /// Get the schema.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Get the next batch (blocking).
    pub fn next(&mut self) -> Result<Option<RecordBatch>, ApiError> {
        match &self.subscription {
            Some(sub) => match sub.recv() {
                Ok(batch) => Ok(Some(batch)),
                Err(laminar_core::streaming::RecvError::Disconnected) => Ok(None),
                Err(e) => Err(ApiError::subscription(e.to_string())),
            },
            None => Ok(None),
        }
    }

    /// Try to get the next batch (non-blocking).
    pub fn try_next(&mut self) -> Result<Option<RecordBatch>, ApiError> {
        match &self.subscription {
            Some(sub) => Ok(sub.poll()),
            None => Ok(None),
        }
    }

    /// Collect all results into a QueryResult.
    pub fn collect(mut self) -> Result<QueryResult, ApiError> {
        let mut batches = Vec::new();
        while let Some(batch) = self.try_next()? {
            batches.push(batch);
        }
        Ok(QueryResult::from_batches(self.schema, batches))
    }

    /// Check if the stream is still active.
    pub fn is_active(&self) -> bool {
        self.handle.as_ref().is_some_and(|h| h.is_active())
    }

    /// Cancel the query.
    pub fn cancel(&mut self) {
        if let Some(ref mut handle) = self.handle {
            handle.cancel();
        }
        self.subscription = None;
    }
}

unsafe impl Send for QueryStream {}
```

### 2.5 Ingestion Types

```rust
// api/ingestion.rs

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::UntypedSourceHandle;
use super::error::ApiError;

/// Writer for inserting data into a source.
///
/// Provides explicit lifecycle management for FFI.
pub struct Writer {
    handle: UntypedSourceHandle,
    closed: bool,
}

impl Writer {
    /// Create a new writer from an untyped source handle.
    pub(crate) fn new(handle: UntypedSourceHandle) -> Self {
        Self {
            handle,
            closed: false,
        }
    }

    /// Write a RecordBatch.
    pub fn write(&mut self, batch: RecordBatch) -> Result<(), ApiError> {
        if self.closed {
            return Err(ApiError::Ingestion {
                code: super::error::codes::WRITER_CLOSED,
                message: "Writer is closed".into(),
            });
        }

        // Validate schema matches
        if batch.schema() != self.handle.schema().clone() {
            return Err(ApiError::Ingestion {
                code: super::error::codes::BATCH_SCHEMA_MISMATCH,
                message: format!(
                    "Batch schema mismatch: expected {:?}, got {:?}",
                    self.handle.schema(),
                    batch.schema()
                ),
            });
        }

        self.handle
            .push_arrow(batch)
            .map_err(|e| ApiError::ingestion(e.to_string()))
    }

    /// Flush pending data (no-op for in-memory, but part of the API contract).
    pub fn flush(&mut self) -> Result<(), ApiError> {
        if self.closed {
            return Err(ApiError::Ingestion {
                code: super::error::codes::WRITER_CLOSED,
                message: "Writer is closed".into(),
            });
        }
        // In-memory sources don't buffer, but external connectors might
        Ok(())
    }

    /// Explicitly close the writer.
    pub fn close(mut self) -> Result<(), ApiError> {
        self.closed = true;
        // Explicit cleanup if needed
        Ok(())
    }

    /// Get the schema.
    pub fn schema(&self) -> SchemaRef {
        self.handle.schema().clone()
    }

    /// Get the source name.
    pub fn name(&self) -> &str {
        self.handle.name()
    }

    /// Emit a watermark.
    pub fn watermark(&self, timestamp: i64) {
        self.handle.watermark(timestamp);
    }
}

unsafe impl Send for Writer {}
```

### 2.6 Module Re-exports

```rust
// api/mod.rs

//! FFI-friendly API for language bindings (Python, Java, Node.js, .NET).
//!
//! This module provides a stable, thread-safe API surface with:
//!
//! - **Numeric error codes** for cross-language error handling
//! - **Arrow RecordBatch** at all data boundaries (zero-copy via C Data Interface)
//! - **Explicit resource management** (close() methods return Result)
//! - **Thread safety guarantees** (all types are Send + Sync)
//!
//! # Example
//!
//! ```rust,ignore
//! use laminar_db::api::{Connection, ApiError};
//!
//! let conn = Connection::open()?;
//!
//! conn.execute("CREATE SOURCE trades (
//!     symbol VARCHAR, price DOUBLE, ts BIGINT
//! )")?;
//!
//! let result = conn.query("SELECT * FROM trades")?;
//! println!("Got {} rows", result.num_rows());
//!
//! conn.close()?;
//! ```

mod connection;
mod error;
mod ingestion;
mod query;
mod subscription;

pub use connection::{Connection, DdlInfo, ExecuteResult};
pub use error::{codes, ApiError};
pub use ingestion::Writer;
pub use query::{QueryResult, QueryStream};
pub use subscription::ArrowSubscription;
```

### 2.7 Subscription Types

```rust
// api/subscription.rs

use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use super::error::ApiError;

/// Untyped subscription returning Arrow RecordBatches.
///
/// Unlike `TypedSubscription<T>`, this doesn't require Rust trait bounds,
/// making it suitable for FFI where the binding language handles deserialization.
pub struct ArrowSubscription {
    inner: laminar_core::streaming::Subscription<crate::catalog::ArrowRecord>,
    schema: SchemaRef,
    active: bool,
}

impl ArrowSubscription {
    /// Create from internal subscription.
    pub(crate) fn new(
        inner: laminar_core::streaming::Subscription<crate::catalog::ArrowRecord>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            inner,
            schema,
            active: true,
        }
    }

    /// Get the schema.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Blocking wait for next batch.
    pub fn next(&mut self) -> Result<Option<RecordBatch>, ApiError> {
        if !self.active {
            return Ok(None);
        }

        match self.inner.recv() {
            Ok(batch) => Ok(Some(batch)),
            Err(laminar_core::streaming::RecvError::Disconnected) => {
                self.active = false;
                Ok(None)
            }
            Err(e) => Err(ApiError::subscription(e.to_string())),
        }
    }

    /// Receive with timeout.
    pub fn next_timeout(&mut self, timeout: Duration) -> Result<Option<RecordBatch>, ApiError> {
        if !self.active {
            return Ok(None);
        }

        match self.inner.recv_timeout(timeout) {
            Ok(batch) => Ok(Some(batch)),
            Err(laminar_core::streaming::RecvError::Disconnected) => {
                self.active = false;
                Ok(None)
            }
            Err(laminar_core::streaming::RecvError::Timeout) => {
                Err(ApiError::subscription_timeout())
            }
            Err(e) => Err(ApiError::subscription(e.to_string())),
        }
    }

    /// Non-blocking poll for next batch.
    pub fn try_next(&mut self) -> Result<Option<RecordBatch>, ApiError> {
        if !self.active {
            return Ok(None);
        }

        Ok(self.inner.poll())
    }

    /// Check if subscription is still active.
    pub fn is_active(&self) -> bool {
        self.active && !self.inner.is_disconnected()
    }

    /// Cancel the subscription.
    pub fn cancel(&mut self) {
        self.active = false;
    }
}

unsafe impl Send for ArrowSubscription {}
```

---

## 3. Cargo.toml Changes

```toml
# In laminar-db/Cargo.toml

[features]
default = []
api = []  # FFI-friendly API module

# Note: parking_lot is already a dependency via laminar-core
```

---

## 4. lib.rs Changes

```rust
// In laminar-db/src/lib.rs

// Add after existing module declarations:

/// FFI-friendly API for language bindings.
///
/// Enable with the `api` feature flag:
/// ```toml
/// laminar-db = { version = "0.1", features = ["api"] }
/// ```
#[cfg(feature = "api")]
pub mod api;
```

---

## 5. Testing

### 5.1 Unit Tests

```rust
// api/tests.rs (or in each module)

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    fn test_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(test_schema()),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_connection_open_close() {
        let conn = Connection::open().unwrap();
        assert!(!conn.is_closed());
        conn.close().unwrap();
    }

    #[test]
    fn test_connection_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Connection>();
        assert_send_sync::<Writer>();
        assert_send_sync::<QueryResult>();
    }

    #[test]
    fn test_connection_thread_safe() {
        let conn = Arc::new(Connection::open().unwrap());

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let conn = conn.clone();
                std::thread::spawn(move || {
                    let _ = conn.list_sources();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[tokio::test]
    async fn test_create_source_and_insert() {
        let conn = Connection::open().unwrap();

        conn.execute(
            "CREATE SOURCE test_source (id BIGINT, name VARCHAR)",
        )
        .unwrap();

        let sources = conn.list_sources();
        assert!(sources.contains(&"test_source".to_string()));

        // Get schema
        let schema = conn.get_schema("test_source").unwrap();
        assert_eq!(schema.fields().len(), 2);
    }

    #[test]
    fn test_error_codes() {
        let err = ApiError::table_not_found("missing");
        assert_eq!(err.code(), codes::TABLE_NOT_FOUND);
        assert!(err.message().contains("missing"));

        let err = ApiError::shutdown();
        assert_eq!(err.code(), codes::SHUTDOWN);
    }

    #[test]
    fn test_error_conversion() {
        let db_err = crate::DbError::SourceNotFound("foo".into());
        let api_err: ApiError = db_err.into();
        assert_eq!(api_err.code(), codes::TABLE_NOT_FOUND);
    }

    #[test]
    fn test_query_result() {
        let batch = test_batch();
        let result = QueryResult::from_batch(batch);

        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.batches().len(), 1);
    }
}
```

### 5.2 Integration Tests

```rust
// tests/api_integration.rs

#[cfg(feature = "api")]
mod api_integration {
    use laminar_db::api::{Connection, ApiError};

    #[tokio::test]
    async fn test_full_pipeline() {
        let conn = Connection::open().unwrap();

        // Create source
        conn.execute("CREATE SOURCE events (id BIGINT, value DOUBLE)")
            .unwrap();

        // Insert data via SQL
        conn.execute("INSERT INTO events VALUES (1, 10.0), (2, 20.0)")
            .unwrap();

        // Query
        let result = conn.query("SELECT * FROM events").unwrap();
        assert_eq!(result.num_rows(), 2);

        conn.close().unwrap();
    }
}
```

---

## 6. Success Criteria

| Criterion | Validation |
|-----------|------------|
| All API types are `Send + Sync` | Compile-time check in tests |
| All functions use Arrow types at boundaries | Code review |
| All resources have explicit `close()` methods | API inspection |
| All errors have numeric codes | `ApiError::code()` returns i32 |
| Tests pass including thread-safety tests | `cargo test --features api` |
| No internal types leak through the API | Public API inspection |
| Clippy passes | `cargo clippy --features api -- -D warnings` |

---

## 7. Future Work (Out of Scope)

The following are explicitly out of scope for this feature but planned for future FFI work:

1. **F-FFI-002: C Header Generation** - Use `cbindgen` to generate C headers
2. **F-FFI-003: Arrow C Data Interface** - Export RecordBatch via FFI_ArrowArray
3. **F-FFI-004: Async FFI** - Callback-based API for non-blocking operations
4. **Language-specific bindings** - Python (PyO3), Java (JNI), Node.js (napi-rs), .NET (P/Invoke)

---

## 8. References

- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
- [PyO3 User Guide](https://pyo3.rs/)
- [cbindgen](https://github.com/mozilla/cbindgen)
- [LaminarDB Architecture](../../ARCHITECTURE.md)
- [FFI Discovery Report](../../ffi-discovery.md)
- [FFI Gap Analysis](../../ffi-gap-analysis.md)
