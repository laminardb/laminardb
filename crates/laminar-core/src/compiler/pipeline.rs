//! Pipeline abstraction for compiled Ring 0 event processing.
//!
//! A streaming SQL query decomposes into **pipelines** (compilable chains of
//! stateless operators) separated by **breakers** (stateful operators like
//! windows, joins, and aggregations). Each pipeline compiles into a single
//! native function that processes one event at a time with zero allocations.
//!
//! # Types
//!
//! - [`Pipeline`]: A chain of [`PipelineStage`]s with input/output schemas.
//! - [`PipelineBreaker`]: Stateful operators that cannot be fused.
//! - [`CompiledPipeline`]: A compiled native function with execution stats.
//! - [`PipelineAction`]: Result of executing a compiled pipeline (drop/emit/error).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion_expr::Expr;

use super::row::RowSchema;

/// Unique identifier for a pipeline segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PipelineId(pub u32);

/// A single stage within a compilable pipeline.
#[derive(Debug, Clone)]
pub enum PipelineStage {
    /// Filter rows by a boolean predicate.
    Filter {
        /// The predicate expression. Rows where this evaluates to false/null are dropped.
        predicate: Expr,
    },
    /// Project (compute) new columns from expressions.
    Project {
        /// Each entry is `(expression, output_column_name)`.
        expressions: Vec<(Expr, String)>,
    },
    /// Extract key columns for downstream grouping or partitioning.
    KeyExtract {
        /// Expressions that compute the key fields.
        key_exprs: Vec<Expr>,
    },
}

/// Stateful operators that break pipeline fusion.
///
/// A breaker sits between two pipelines: it consumes output from the upstream
/// pipeline and feeds input to the downstream pipeline.
#[derive(Debug, Clone)]
pub enum PipelineBreaker {
    /// GROUP BY aggregation.
    Aggregate {
        /// Group-by key expressions.
        group_exprs: Vec<Expr>,
        /// Aggregate function expressions.
        aggr_exprs: Vec<Expr>,
    },
    /// ORDER BY (requires full materialization).
    Sort {
        /// Sort key expressions.
        order_exprs: Vec<Expr>,
    },
    /// Stream-stream or lookup join.
    Join {
        /// The type of join (inner, left, right, full).
        join_type: String,
        /// Left-side join key expressions.
        left_keys: Vec<Expr>,
        /// Right-side join key expressions.
        right_keys: Vec<Expr>,
    },
    /// Terminal sink (no downstream pipeline).
    Sink,
}

/// A compilable pipeline: a chain of stateless stages with known schemas.
#[derive(Debug, Clone)]
pub struct Pipeline {
    /// Unique pipeline identifier.
    pub id: PipelineId,
    /// Ordered list of stages to execute.
    pub stages: Vec<PipelineStage>,
    /// Schema of rows entering this pipeline.
    pub input_schema: SchemaRef,
    /// Schema of rows leaving this pipeline.
    pub output_schema: SchemaRef,
}

/// Result of executing a compiled pipeline on one event row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PipelineAction {
    /// Row was filtered out (dropped).
    Drop = 0,
    /// Row passed all stages and should be emitted.
    Emit = 1,
    /// An error occurred during execution.
    Error = 2,
}

impl PipelineAction {
    /// Converts a raw `u8` return value to a [`PipelineAction`].
    #[must_use]
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Drop,
            1 => Self::Emit,
            _ => Self::Error,
        }
    }
}

/// Function pointer type for a compiled pipeline.
///
/// Signature: `fn(input_row: *const u8, output_row: *mut u8) -> u8`
///
/// - `input_row`: pointer to the input [`EventRow`](super::row::EventRow) byte buffer
/// - `output_row`: pointer to the output row buffer (must be pre-allocated)
/// - Returns: `0` = Drop, `1` = Emit, `2` = Error
pub type PipelineFn = unsafe extern "C" fn(*const u8, *mut u8) -> u8;

/// Runtime statistics for a compiled pipeline.
#[derive(Debug)]
pub struct PipelineStats {
    /// Total events processed.
    pub events_processed: AtomicU64,
    /// Events that passed all stages (emitted).
    pub events_emitted: AtomicU64,
    /// Events filtered out (dropped).
    pub events_dropped: AtomicU64,
    /// Cumulative execution time in nanoseconds.
    pub total_ns: AtomicU64,
}

impl PipelineStats {
    /// Creates a new zeroed stats instance.
    #[must_use]
    pub fn new() -> Self {
        Self {
            events_processed: AtomicU64::new(0),
            events_emitted: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
            total_ns: AtomicU64::new(0),
        }
    }

    /// Records a single execution result.
    pub fn record(&self, action: PipelineAction, elapsed_ns: u64) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
        match action {
            PipelineAction::Emit => {
                self.events_emitted.fetch_add(1, Ordering::Relaxed);
            }
            PipelineAction::Drop => {
                self.events_dropped.fetch_add(1, Ordering::Relaxed);
            }
            PipelineAction::Error => {}
        }
        self.total_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
    }
}

impl Default for PipelineStats {
    fn default() -> Self {
        Self::new()
    }
}

/// A compiled pipeline ready for Ring 0 execution.
pub struct CompiledPipeline {
    /// Pipeline identifier.
    pub id: PipelineId,
    /// The compiled native function.
    func: PipelineFn,
    /// Schema for input rows.
    pub input_schema: Arc<RowSchema>,
    /// Schema for output rows.
    pub output_schema: Arc<RowSchema>,
    /// Runtime statistics.
    pub stats: PipelineStats,
}

impl CompiledPipeline {
    /// Creates a new compiled pipeline.
    pub fn new(
        id: PipelineId,
        func: PipelineFn,
        input_schema: Arc<RowSchema>,
        output_schema: Arc<RowSchema>,
    ) -> Self {
        Self {
            id,
            func,
            input_schema,
            output_schema,
            stats: PipelineStats::new(),
        }
    }

    /// Executes the compiled pipeline on one input row.
    ///
    /// # Safety
    ///
    /// - `input` must point to a valid `EventRow` byte buffer matching `input_schema`.
    /// - `output` must point to a buffer of at least `output_schema.min_row_size()` bytes.
    #[inline]
    pub unsafe fn execute(&self, input: *const u8, output: *mut u8) -> PipelineAction {
        let result = (self.func)(input, output);
        PipelineAction::from_u8(result)
    }

    /// Returns the compiled function pointer.
    #[must_use]
    pub fn func(&self) -> PipelineFn {
        self.func
    }
}

// SAFETY: The function pointer is valid across threads (compiled code is immutable).
unsafe impl Send for CompiledPipeline {}
unsafe impl Sync for CompiledPipeline {}

impl std::fmt::Debug for CompiledPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledPipeline")
            .field("id", &self.id)
            .field("func", &"<native fn>")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_action_repr() {
        assert_eq!(PipelineAction::Drop as u8, 0);
        assert_eq!(PipelineAction::Emit as u8, 1);
        assert_eq!(PipelineAction::Error as u8, 2);
    }

    #[test]
    fn pipeline_action_from_u8() {
        assert_eq!(PipelineAction::from_u8(0), PipelineAction::Drop);
        assert_eq!(PipelineAction::from_u8(1), PipelineAction::Emit);
        assert_eq!(PipelineAction::from_u8(2), PipelineAction::Error);
        assert_eq!(PipelineAction::from_u8(255), PipelineAction::Error);
    }

    #[test]
    fn pipeline_id_equality() {
        assert_eq!(PipelineId(0), PipelineId(0));
        assert_ne!(PipelineId(0), PipelineId(1));
    }

    #[test]
    fn pipeline_stats_record() {
        let stats = PipelineStats::new();
        stats.record(PipelineAction::Emit, 100);
        stats.record(PipelineAction::Drop, 50);
        stats.record(PipelineAction::Emit, 80);

        assert_eq!(stats.events_processed.load(Ordering::Relaxed), 3);
        assert_eq!(stats.events_emitted.load(Ordering::Relaxed), 2);
        assert_eq!(stats.events_dropped.load(Ordering::Relaxed), 1);
        assert_eq!(stats.total_ns.load(Ordering::Relaxed), 230);
    }

    #[test]
    fn compiled_pipeline_execute() {
        use arrow_schema::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let row_schema = Arc::new(RowSchema::from_arrow(&schema).unwrap());

        // A dummy pipeline function that always emits.
        unsafe extern "C" fn always_emit(_input: *const u8, _output: *mut u8) -> u8 {
            1
        }

        let compiled = CompiledPipeline::new(
            PipelineId(0),
            always_emit,
            Arc::clone(&row_schema),
            Arc::clone(&row_schema),
        );

        let input = vec![0u8; row_schema.min_row_size()];
        let mut output = vec![0u8; row_schema.min_row_size()];
        let action = unsafe { compiled.execute(input.as_ptr(), output.as_mut_ptr()) };
        assert_eq!(action, PipelineAction::Emit);
    }

    #[test]
    fn compiled_pipeline_drop_action() {
        use arrow_schema::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let row_schema = Arc::new(RowSchema::from_arrow(&schema).unwrap());

        unsafe extern "C" fn always_drop(_input: *const u8, _output: *mut u8) -> u8 {
            0
        }

        let compiled = CompiledPipeline::new(
            PipelineId(1),
            always_drop,
            Arc::clone(&row_schema),
            row_schema,
        );

        let input = vec![0u8; 64];
        let mut output = vec![0u8; 64];
        let action = unsafe { compiled.execute(input.as_ptr(), output.as_mut_ptr()) };
        assert_eq!(action, PipelineAction::Drop);
    }

    #[test]
    fn pipeline_debug_impl() {
        use arrow_schema::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let row_schema = Arc::new(RowSchema::from_arrow(&schema).unwrap());

        unsafe extern "C" fn noop(_: *const u8, _: *mut u8) -> u8 {
            0
        }

        let compiled =
            CompiledPipeline::new(PipelineId(42), noop, Arc::clone(&row_schema), row_schema);
        let dbg = format!("{compiled:?}");
        assert!(dbg.contains("42"));
        assert!(dbg.contains("native fn"));
    }
}
