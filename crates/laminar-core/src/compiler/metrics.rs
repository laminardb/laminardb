//! Streaming query lifecycle types: identifiers, states, configuration, errors, and metrics.
//!
//! These types are always available (not gated behind `jit`) so that configuration,
//! error handling, and metrics aggregation can be used without pulling in Cranelift.

use std::fmt;
use std::time::Duration;

use super::pipeline_bridge::PipelineBridgeError;
use super::policy::{BackpressureStrategy, BatchPolicy};

// ────────────────────────────── QueryId ──────────────────────────────

/// Unique identifier for a streaming query, derived from hashing the SQL text.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct QueryId(pub u64);

impl fmt::Display for QueryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:016x}", self.0)
    }
}

// ────────────────────────────── QueryState ───────────────────────────

/// Lifecycle state of a `StreamingQuery` (requires `jit` feature).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryState {
    /// Query is built but not yet started.
    Ready,
    /// Query is actively processing events.
    Running,
    /// Query is temporarily paused (can be resumed).
    Paused,
    /// Query has been stopped (terminal state).
    Stopped,
}

impl QueryState {
    /// Returns `true` if this is a terminal state (no further transitions allowed).
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped)
    }
}

impl fmt::Display for QueryState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready => write!(f, "Ready"),
            Self::Running => write!(f, "Running"),
            Self::Paused => write!(f, "Paused"),
            Self::Stopped => write!(f, "Stopped"),
        }
    }
}

// ────────────────────────────── QueryConfig ──────────────────────────

/// Configuration for a streaming query.
#[derive(Debug, Clone)]
pub struct QueryConfig {
    /// Whether JIT compilation is enabled.
    pub jit_enabled: bool,
    /// Batching policy for the Ring 0 → Ring 1 bridge.
    pub batch_policy: BatchPolicy,
    /// Backpressure strategy for the bridge producer.
    pub backpressure: BackpressureStrategy,
    /// Maximum entries in the pipeline compiler cache.
    pub max_cache_entries: usize,
    /// SPSC queue capacity for each bridge.
    pub queue_capacity: usize,
    /// Output buffer size in bytes for each compiled pipeline.
    pub output_buffer_size: usize,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            jit_enabled: cfg!(feature = "jit"),
            batch_policy: BatchPolicy::default(),
            backpressure: BackpressureStrategy::default(),
            max_cache_entries: 64,
            queue_capacity: 4096,
            output_buffer_size: 8192,
        }
    }
}

// ────────────────────────────── StateStoreConfig ─────────────────────

/// State store backend configuration (kept minimal for now).
#[derive(Debug, Clone, Default)]
pub enum StateStoreConfig {
    /// In-memory state store (default).
    #[default]
    InMemory,
}

// ────────────────────────────── QueryError ───────────────────────────

/// Errors from streaming query operations.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// The query is in the wrong state for this operation.
    #[error("invalid state: expected {expected}, actual {actual}")]
    InvalidState {
        /// The expected state description.
        expected: &'static str,
        /// The actual state.
        actual: QueryState,
    },
    /// Bridge error during event submission or control message send.
    #[error("bridge error: {0}")]
    Bridge(#[from] PipelineBridgeError),
    /// Pipeline execution failed.
    #[error("pipeline {pipeline_idx} execution error")]
    PipelineError {
        /// Index of the failed pipeline.
        pipeline_idx: usize,
    },
    /// Schemas are incompatible (e.g., during hot-swap).
    #[error("incompatible schemas: {0}")]
    IncompatibleSchemas(String),
    /// No pipelines were added to the builder.
    #[error("no pipelines added to query builder")]
    NoPipelines,
    /// Build-time error.
    #[error("build error: {0}")]
    Build(String),
}

// ────────────────────────────── SubmitResult ─────────────────────────

/// Result of submitting an event row to a streaming query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitResult {
    /// At least one pipeline emitted output for this row.
    Emitted,
    /// All pipelines filtered out this row.
    Filtered,
}

// ────────────────────────────── QueryMetadata ────────────────────────

/// Metadata about how a query was compiled.
#[derive(Debug, Clone, Default)]
pub struct QueryMetadata {
    /// Time spent planning the query.
    pub plan_time: Duration,
    /// Time spent extracting pipelines from the logical plan.
    pub extract_time: Duration,
    /// Time spent compiling pipelines via Cranelift.
    pub compile_time: Duration,
    /// Number of pipelines successfully compiled to native code.
    pub compiled_pipeline_count: usize,
    /// Number of pipelines using fallback (interpreted) execution.
    pub fallback_pipeline_count: usize,
    /// Whether JIT was enabled for this compilation.
    pub jit_enabled: bool,
}

impl QueryMetadata {
    /// Returns the total number of pipelines (compiled + fallback).
    #[must_use]
    pub fn total_pipelines(&self) -> usize {
        self.compiled_pipeline_count + self.fallback_pipeline_count
    }
}

// ────────────────────────────── QueryMetrics ─────────────────────────

/// Aggregated runtime metrics for a streaming query.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryMetrics {
    /// Total events submitted to Ring 0.
    pub ring0_events_in: u64,
    /// Events emitted from Ring 0 to bridges.
    pub ring0_events_out: u64,
    /// Events dropped by Ring 0 pipelines (filtered out).
    pub ring0_events_dropped: u64,
    /// Cumulative Ring 0 processing time in nanoseconds.
    pub ring0_total_ns: u64,
    /// Events pending in bridge queues.
    pub bridge_pending: u64,
    /// Events dropped due to bridge backpressure.
    pub bridge_backpressure_drops: u64,
    /// Total batches flushed to Ring 1.
    pub bridge_batches_flushed: u64,
    /// Total rows flushed to Ring 1 across all batches.
    pub ring1_rows_flushed: u64,
    /// Number of compiled (JIT) pipelines.
    pub pipelines_compiled: u64,
    /// Number of fallback (interpreted) pipelines.
    pub pipelines_fallback: u64,
}

// ────────────────────────────── Tests ────────────────────────────────

#[cfg(test)]
#[allow(clippy::single_char_pattern)]
mod tests {
    use super::*;

    #[test]
    fn query_id_display() {
        let id = QueryId(0xDEAD_BEEF_CAFE_BABE);
        let s = format!("{id}");
        assert_eq!(s, "0xdeadbeefcafebabe");
    }

    #[test]
    fn query_state_is_terminal() {
        assert!(!QueryState::Ready.is_terminal());
        assert!(!QueryState::Running.is_terminal());
        assert!(!QueryState::Paused.is_terminal());
        assert!(QueryState::Stopped.is_terminal());
    }

    #[test]
    fn query_config_defaults() {
        let config = QueryConfig::default();
        assert_eq!(config.max_cache_entries, 64);
        assert_eq!(config.queue_capacity, 4096);
        assert_eq!(config.output_buffer_size, 8192);
        assert!(matches!(
            config.backpressure,
            BackpressureStrategy::DropNewest
        ));
    }

    #[test]
    fn query_metadata_total_pipelines() {
        let meta = QueryMetadata {
            compiled_pipeline_count: 3,
            fallback_pipeline_count: 2,
            ..Default::default()
        };
        assert_eq!(meta.total_pipelines(), 5);
    }

    #[test]
    fn query_error_display() {
        let err = QueryError::InvalidState {
            expected: "Running",
            actual: QueryState::Ready,
        };
        let s = format!("{err}");
        assert!(s.contains("Running"));
        assert!(s.contains("Ready"));

        let err2 = QueryError::NoPipelines;
        assert!(format!("{err2}").contains("no pipelines"));

        let err3 = QueryError::PipelineError { pipeline_idx: 5 };
        assert!(format!("{err3}").contains("5"));
    }

    #[test]
    fn submit_result_variants() {
        assert_eq!(SubmitResult::Emitted, SubmitResult::Emitted);
        assert_ne!(SubmitResult::Emitted, SubmitResult::Filtered);
    }

    #[test]
    fn query_metrics_default() {
        let m = QueryMetrics::default();
        assert_eq!(m.ring0_events_in, 0);
        assert_eq!(m.ring0_events_out, 0);
        assert_eq!(m.ring0_events_dropped, 0);
        assert_eq!(m.ring0_total_ns, 0);
        assert_eq!(m.bridge_pending, 0);
        assert_eq!(m.bridge_backpressure_drops, 0);
        assert_eq!(m.bridge_batches_flushed, 0);
        assert_eq!(m.ring1_rows_flushed, 0);
        assert_eq!(m.pipelines_compiled, 0);
        assert_eq!(m.pipelines_fallback, 0);
    }

    #[test]
    fn state_store_config_default() {
        let c = StateStoreConfig::default();
        assert!(matches!(c, StateStoreConfig::InMemory));
    }
}
