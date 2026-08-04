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
#![allow(clippy::duration_suboptimal_units)] // MSRV 1.85; from_mins/from_hours are 1.91+
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::too_many_arguments, clippy::too_many_lines)] // Lifecycle protocols remain contiguous and keep fencing inputs explicit.
#![allow(clippy::disallowed_types)] // Control-plane maps mirror persisted and cross-crate checkpoint types.
#![allow(clippy::unused_self)]
// Feature stubs preserve shared protocol call sites.
// Lifecycle fixtures favor explicit protocol setup and boundary assertions.
#![cfg_attr(
    test,
    allow(
        clippy::assertions_on_constants,
        clippy::default_trait_access,
        clippy::field_reassign_with_default,
        clippy::filter_map_bool_then,
        clippy::float_cmp,
        clippy::items_after_statements,
        clippy::manual_let_else,
        clippy::match_wildcard_for_single_variants,
        clippy::needless_borrow,
        clippy::needless_pass_by_value,
        clippy::needless_return,
        clippy::redundant_closure,
        clippy::similar_names,
        clippy::single_char_pattern,
        clippy::type_complexity,
        clippy::unchecked_time_subtraction,
        clippy::unnecessary_to_owned,
        clippy::unnecessary_wraps,
        clippy::unnested_or_patterns,
        clippy::used_underscore_binding
    )
)]

mod aggregate_state;
/// AI inference module, containing model registry, provider trait, and backends.
pub mod ai;
mod ai_catalog;
mod ai_worker;
mod builder;
mod catalog;
mod catalog_connector;
mod changelog_filter;
/// Unified checkpoint coordination.
#[doc(hidden)]
pub mod checkpoint_coordinator;
/// Bounded process-local evidence for cluster checkpoint barrier pauses.
#[cfg(feature = "cluster")]
pub mod checkpoint_timing;
mod config;
mod connector_manager;
mod connector_task_fence;
#[cfg(feature = "cluster")]
mod coordinated_recovery;
mod core_window_state;
mod db;
/// Prometheus metrics for the streaming engine.
pub mod engine_metrics;
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
mod filter_compile;
mod handle;
mod interval_join;
mod key_column;
mod log_throttle;
mod metrics;
mod metrics_api;
mod mv_store;
mod operator;
mod operator_graph;
/// Thread-per-core connector pipeline.
pub mod pipeline;
mod pipeline_callback;
mod pipeline_identity;
mod pipeline_lifecycle;
/// Deployment profiles.
pub mod profile;
/// Dynamic vnode rebalance control plane.
#[cfg(feature = "cluster")]
pub mod rebalance;
/// Unified recovery manager.
pub mod recovery_manager;
mod retractable_accumulator;
mod show_commands;
mod sink_task;
mod sql_analysis;
mod sql_utils;
/// External named-subscription substrate: byte-bounded shared logs and cursor portals.
pub mod subscription;
mod table_provider;
mod table_rows;
mod table_store;
mod temporal_join_state;
#[cfg(feature = "cluster")]
mod vnode_transition_staging;

// End-to-end tests for the crypto-sentiment demo pipeline, backed by wiremock.
// In-crate (not tests/) so it can drive the OperatorGraph directly.
#[cfg(test)]
mod e2e_crypto_sentiment;

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
    CheckpointFailureDisposition, CheckpointPhase, CheckpointResult, CheckpointStats,
};
pub use config::{
    BackpressurePolicy, LaminarConfig, RestartPolicy, DEFAULT_MAX_MANAGED_STATE_BYTES,
    DEFAULT_MAX_RETRACTABLE_EXTREMUM_CHECKPOINT_BYTES,
};
pub use db::LaminarDB;
pub use engine_metrics::EngineMetrics;
pub use error::DbError;
pub use handle::{
    DdlInfo, ExecuteResult, FromBatch, MaterializedViewInfo, PipelineEdge, PipelineNode,
    PipelineNodeType, PipelineTopology, QueryHandle, QueryInfo, SinkInfo, SourceHandle, SourceInfo,
    StreamInfo, SubscriptionError, TypedSubscription, TypedSubscriptionFrame, UntypedSourceHandle,
};
pub use laminar_connectors::connector::DeliveryGuarantee;
pub use metrics::{PipelineMetrics, PipelineState, SourceMetrics, StreamMetrics};
pub use profile::{Profile, ProfileError};
pub use recovery_manager::{RecoveredState, RecoveryManager};

/// Cluster assignment lifecycle results.
#[cfg(feature = "cluster")]
pub use db::{ClusterStartupDisposition, SnapshotAdoption};

/// Re-export the connector registry for custom connector registration.
pub use laminar_connectors::registry::ConnectorRegistry;

/// Re-export connector metadata types for the control-plane HTTP API
/// (connector catalog / source-creation wizard).
pub use laminar_connectors::config::{ConfigKeySpec, ConnectorInfo};
