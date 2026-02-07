//! Unified database facade for `LaminarDB`.
//!
//! Provides a single entry point (`LaminarDB`) that ties together
//! the SQL parser, query planner, `DataFusion` context, and streaming API.
//!
//! # Example
//!
//! ```rust,ignore
//! use laminar_db::LaminarDB;
//!
//! let db = LaminarDB::open()?;
//!
//! db.execute("CREATE SOURCE trades (
//!     symbol VARCHAR, price DOUBLE, ts BIGINT,
//!     WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
//! )").await?;
//!
//! let query = db.execute("SELECT symbol, AVG(price)
//!     FROM trades GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE)
//! ").await?;
//! ```

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

mod asof_batch;
mod builder;
mod catalog;
mod config;
mod connector_manager;
mod db;
mod error;
mod handle;
mod metrics;
mod pipeline_checkpoint;
mod sql_utils;
mod stream_executor;
mod table_backend;
mod table_cache_mode;
mod table_provider;
mod table_store;

/// FFI-friendly API for language bindings.
///
/// Enable with the `api` feature flag:
/// ```toml
/// laminar-db = { version = "0.1", features = ["api"] }
/// ```
///
/// This module provides thread-safe types with numeric error codes,
/// explicit resource management, and Arrow RecordBatch at all boundaries.
#[cfg(feature = "api")]
pub mod api;

/// C FFI layer for LaminarDB.
///
/// Enable with the `ffi` feature flag:
/// ```toml
/// laminar-db = { version = "0.1", features = ["ffi"] }
/// ```
///
/// This module provides `extern "C"` functions for calling LaminarDB from C
/// and any language with C FFI support (Python, Java, Node.js, .NET, etc.).
#[cfg(feature = "ffi")]
pub mod ffi;

pub use builder::LaminarDbBuilder;
pub use catalog::{SourceCatalog, SourceEntry};
pub use config::LaminarConfig;
pub use db::LaminarDB;
pub use error::DbError;
pub use handle::{
    DdlInfo, ExecuteResult, FromBatch, PipelineEdge, PipelineNode, PipelineNodeType,
    PipelineTopology, QueryHandle, QueryInfo, SinkInfo, SourceHandle, SourceInfo, StreamInfo,
    TypedSubscription, UntypedSourceHandle,
};
pub use metrics::{PipelineCounters, PipelineMetrics, PipelineState, SourceMetrics, StreamMetrics};
pub use pipeline_checkpoint::PipelineCheckpoint;

/// Re-export the connector registry for custom connector registration.
pub use laminar_connectors::registry::ConnectorRegistry;
