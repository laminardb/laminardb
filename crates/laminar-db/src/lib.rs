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
//!     symbol VARCHAR, price DOUBLE, ts TIMESTAMP,
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

mod aggregate_state;
mod asof_batch;
mod builder;
mod catalog;
mod catalog_connector;
mod changelog_filter;
/// Unified checkpoint coordination.
pub mod checkpoint_coordinator;
mod config;
mod connector_manager;
mod core_window_state;
mod db;
/// Prometheus metrics for the streaming engine.
pub mod engine_metrics;
mod eowc_state;
// Reopened `impl LaminarDB` modules — split from db.rs
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
mod ddl;
mod error;
mod handle;
mod interval_join;
mod key_column;
mod metrics;
mod metrics_api;
mod mv_store;
mod operator;
mod operator_graph;
/// Thread-per-core connector pipeline.
pub mod pipeline;
mod pipeline_callback;
mod pipeline_lifecycle;
/// Deployment profiles.
pub mod profile;
/// Unified recovery manager.
pub mod recovery_manager;
mod retractable_accumulator;
mod show_commands;
mod sink_task;
mod sql_analysis;
mod sql_utils;
mod table_backend;
mod table_cache_mode;
mod table_provider;
mod table_store;
mod temporal_probe;

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
pub use catalog::{ArrowRecord, SourceCatalog, SourceEntry};
pub use checkpoint_coordinator::{
    CheckpointConfig, CheckpointCoordinator, CheckpointPhase, CheckpointRequest, CheckpointResult,
    CheckpointStats,
};
pub use config::{IdentifierCaseSensitivity, LaminarConfig, TieringConfig};
pub use db::LaminarDB;
pub use engine_metrics::EngineMetrics;
pub use error::DbError;
pub use handle::{
    DdlInfo, ExecuteResult, FromBatch, PipelineEdge, PipelineNode, PipelineNodeType,
    PipelineTopology, QueryHandle, QueryInfo, SinkInfo, SourceHandle, SourceInfo, StreamInfo,
    TypedSubscription, UntypedSourceHandle,
};
pub use metrics::{PipelineMetrics, PipelineState, SourceMetrics, StreamMetrics};
pub use profile::{Profile, ProfileError};
pub use recovery_manager::{RecoveredState, RecoveryManager};

/// Re-export the connector registry for custom connector registration.
pub use laminar_connectors::registry::ConnectorRegistry;
