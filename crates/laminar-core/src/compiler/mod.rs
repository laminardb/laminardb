//! Plan compiler infrastructure for Ring 0 event processing.
//!
//! This module provides the foundation for compiling `DataFusion` logical plans
//! into native functions that operate on a fixed-layout row format.
//!
//! # Components
//!
//! - [`row`]: Fixed-layout [`EventRow`] format with zero-copy field access
//! - [`bridge`]: [`RowBatchBridge`] for converting rows back to Arrow `RecordBatch`
//! - [`policy`]: [`BatchPolicy`] and [`BridgeOverflow`] for bridge configuration
//! - [`pipeline_bridge`]: Ring 0 / Ring 1 SPSC bridge with watermark-aware batching
//! - [`event_time`]: Schema-aware event time extraction for compiled queries
//! - [`query_lifecycle`]: Query lifecycle types

pub mod batch_reader;
pub mod bridge;
pub mod compilation_metrics;
pub mod event_time;
pub mod pipeline_bridge;
pub mod policy;
pub mod query_lifecycle;
pub mod row;

pub use batch_reader::BatchRowReader;
pub use bridge::{BridgeError, RowBatchBridge};
pub use compilation_metrics::{CacheSnapshot, CompilationMetrics, MetricsSnapshot};
pub use event_time::{EventTimeConfig, RowEventTimeExtractor};
pub use pipeline_bridge::{
    create_pipeline_bridge, BridgeConsumer, BridgeMessage, BridgeStats, BridgeStatsSnapshot,
    PipelineBridge, PipelineBridgeError, Ring1Action,
};
pub use policy::{BatchPolicy, BridgeOverflow};
pub use query_lifecycle::{
    QueryConfig, QueryError, QueryId, QueryMetadata, QueryMetrics, QueryState, StateStoreConfig,
    SubmitResult,
};
pub use row::{EventRow, FieldLayout, FieldType, MutableEventRow, RowError, RowSchema};
