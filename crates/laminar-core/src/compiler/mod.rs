//! Plan compiler infrastructure for Ring 0 event processing.
//!
//! This module provides the foundation for compiling `DataFusion` logical plans
//! into native functions that operate on a fixed-layout row format.
//!
//! # Components
//!
//! - [`row`]: Fixed-layout [`EventRow`] format with zero-copy field access
//! - [`bridge`]: [`RowBatchBridge`] for converting rows back to Arrow `RecordBatch`
//! - [`policy`]: [`BatchPolicy`] and [`BackpressureStrategy`] for bridge configuration
//! - [`pipeline_bridge`]: Ring 0 / Ring 1 SPSC bridge with watermark-aware batching
//! - [`event_time`]: Schema-aware event time extraction for compiled queries
//! - [`metrics`]: Query lifecycle types (always available, not JIT-gated)
//!
//! ## JIT Compilation (requires `jit` feature)
//!
//! - `error`: Error types and compiled function pointer wrappers
//! - `expr`: Cranelift-based expression compiler
//! - `fold`: Constant folding pre-pass
//! - `jit`: Cranelift JIT context management
//! - `pipeline`: Pipeline types and compiled pipeline wrapper
//! - `extractor`: Pipeline extraction from `DataFusion` logical plans
//! - `pipeline_compiler`: Cranelift codegen for fused pipelines
//! - `cache`: Compiler cache for compiled pipelines
//! - `fallback`: Fallback mechanism for uncompilable pipelines
//! - `query`: `StreamingQuery` lifecycle management
//! - `breaker_executor`: Compiled stateful pipeline bridge (Ring 1 operator wiring)

pub mod batch_reader;
pub mod bridge;
pub mod compilation_metrics;
pub mod event_time;
pub mod metrics;
pub mod pipeline_bridge;
pub mod policy;
pub mod row;

#[cfg(feature = "jit")]
pub mod breaker_executor;
#[cfg(feature = "jit")]
pub mod cache;
#[cfg(feature = "jit")]
pub mod error;
#[cfg(feature = "jit")]
pub mod expr;
#[cfg(feature = "jit")]
pub mod extractor;
#[cfg(feature = "jit")]
pub mod fallback;
#[cfg(feature = "jit")]
pub mod fold;
#[cfg(feature = "jit")]
pub mod jit;
#[cfg(feature = "jit")]
pub mod orchestrate;
#[cfg(feature = "jit")]
pub mod pipeline;
#[cfg(feature = "jit")]
pub mod pipeline_compiler;
#[cfg(feature = "jit")]
pub mod query;

pub use batch_reader::BatchRowReader;
pub use bridge::{BridgeError, RowBatchBridge};
pub use compilation_metrics::{CacheSnapshot, CompilationMetrics, MetricsSnapshot};
pub use event_time::{EventTimeConfig, RowEventTimeExtractor};
pub use metrics::{
    QueryConfig, QueryError, QueryId, QueryMetadata, QueryMetrics, QueryState, StateStoreConfig,
    SubmitResult,
};
pub use pipeline_bridge::{
    create_pipeline_bridge, BridgeConsumer, BridgeMessage, BridgeStats, BridgeStatsSnapshot,
    PipelineBridge, PipelineBridgeError, Ring1Action,
};
pub use policy::{BackpressureStrategy, BatchPolicy};
pub use row::{EventRow, FieldLayout, FieldType, MutableEventRow, RowError, RowSchema};

#[cfg(feature = "jit")]
pub use error::{CompileError, CompiledExpr, ExtractError, FilterFn, MaybeCompiledExpr, ScalarFn};
#[cfg(feature = "jit")]
pub use expr::ExprCompiler;
#[cfg(feature = "jit")]
pub use jit::JitContext;

// Plan Compiler Core types
#[cfg(feature = "jit")]
pub use cache::CompilerCache;
#[cfg(feature = "jit")]
pub use extractor::{ExtractedPlan, PipelineExtractor};
#[cfg(feature = "jit")]
pub use fallback::ExecutablePipeline;
#[cfg(feature = "jit")]
pub use pipeline::{
    CompiledPipeline, Pipeline, PipelineAction, PipelineBreaker, PipelineFn, PipelineId,
    PipelineStage, PipelineStats,
};
#[cfg(feature = "jit")]
pub use pipeline_compiler::PipelineCompiler;

// Streaming Query Lifecycle
#[cfg(feature = "jit")]
pub use query::{StreamingQuery, StreamingQueryBuilder};

// SQL Compiler Orchestrator
#[cfg(feature = "jit")]
pub use orchestrate::{compile_streaming_query, CompiledStreamingQuery};

// Compiled Stateful Pipeline Bridge
#[cfg(feature = "jit")]
pub use breaker_executor::{BreakerExecutor, CompiledQueryGraph, Ring1Operator};
