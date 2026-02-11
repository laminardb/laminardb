//! Compiled stateful pipeline bridge — wires pipeline breakers to Ring 1 operators.
//!
//! When a query plan contains stateful operators (aggregation, sort, join),
//! the compiler splits execution into multiple compiled segments separated by
//! breaker boundaries. Each [`BreakerExecutor`] handles the Ring 1 operator
//! at a pipeline boundary, consuming output from an upstream compiled segment
//! and producing input for a downstream one.
//!
//! # Architecture
//!
//! ```text
//!    Compiled Segment 1        BreakerExecutor        Compiled Segment 2
//!   (Filter + Project)       (Ring 1 Operator)       (Output Projection)
//!   ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
//!   │ fn(in, out) → u8 │──► │  process_batch   │──► │ fn(in, out) → u8 │
//!   └──────────────────┘    └──────────────────┘    └──────────────────┘
//!        PipelineBridge          State Store          PipelineBridge
//! ```

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;

use super::pipeline::PipelineBreaker;

/// A Ring 1 operator that processes batches at a pipeline boundary.
///
/// Implementations handle stateful operations like windowed aggregation,
/// stream joins, and sorting. The operator receives `RecordBatch`es from
/// the upstream compiled segment and produces output batches for the
/// downstream segment.
pub trait Ring1Operator: Send {
    /// Processes an incoming `RecordBatch` from the upstream compiled segment.
    ///
    /// Returns zero or more output batches. For windowed aggregation, output
    /// may be empty until a window trigger fires.
    fn process_batch(&mut self, batch: RecordBatch) -> Vec<RecordBatch>;

    /// Advances the watermark — may trigger window emissions.
    ///
    /// Returns any batches emitted by window closure or timer firing.
    fn advance_watermark(&mut self, timestamp: i64) -> Vec<RecordBatch>;

    /// Returns the output schema for batches produced by this operator.
    fn output_schema(&self) -> SchemaRef;

    /// Serializes operator state for checkpointing.
    fn checkpoint(&self) -> Vec<u8>;

    /// Restores operator state from a checkpoint.
    fn restore(&mut self, state: &[u8]);
}

/// Executes a Ring 1 stateful operator at a pipeline boundary.
///
/// Wraps a [`Ring1Operator`] with the breaker metadata and tracks
/// per-executor statistics.
pub struct BreakerExecutor {
    /// The type of breaker (aggregate, sort, join).
    breaker: PipelineBreaker,
    /// The Ring 1 operator implementation.
    operator: Box<dyn Ring1Operator>,
    /// Count of batches processed.
    batches_in: u64,
    /// Count of batches emitted.
    batches_out: u64,
}

impl BreakerExecutor {
    /// Creates a new executor for the given breaker and operator.
    #[must_use]
    pub fn new(breaker: PipelineBreaker, operator: Box<dyn Ring1Operator>) -> Self {
        Self {
            breaker,
            operator,
            batches_in: 0,
            batches_out: 0,
        }
    }

    /// Processes a batch from the upstream compiled segment.
    ///
    /// Returns any output batches ready for the downstream segment.
    pub fn process(&mut self, batch: RecordBatch) -> Vec<RecordBatch> {
        self.batches_in += 1;
        let out = self.operator.process_batch(batch);
        self.batches_out += out.len() as u64;
        out
    }

    /// Advances the watermark through the operator.
    pub fn advance_watermark(&mut self, timestamp: i64) -> Vec<RecordBatch> {
        let out = self.operator.advance_watermark(timestamp);
        self.batches_out += out.len() as u64;
        out
    }

    /// Returns the breaker type.
    #[must_use]
    pub fn breaker(&self) -> &PipelineBreaker {
        &self.breaker
    }

    /// Returns the output schema of the operator.
    #[must_use]
    pub fn output_schema(&self) -> SchemaRef {
        self.operator.output_schema()
    }

    /// Returns the number of input batches processed.
    #[must_use]
    pub fn batches_in(&self) -> u64 {
        self.batches_in
    }

    /// Returns the number of output batches emitted.
    #[must_use]
    pub fn batches_out(&self) -> u64 {
        self.batches_out
    }

    /// Checkpoints the operator state.
    #[must_use]
    pub fn checkpoint(&self) -> Vec<u8> {
        self.operator.checkpoint()
    }

    /// Restores the operator state.
    pub fn restore(&mut self, state: &[u8]) {
        self.operator.restore(state);
    }
}

impl std::fmt::Debug for BreakerExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BreakerExecutor")
            .field("breaker", &self.breaker)
            .field("batches_in", &self.batches_in)
            .field("batches_out", &self.batches_out)
            .finish_non_exhaustive()
    }
}

/// A graph of compiled segments connected by Ring 1 operators.
///
/// Each segment is a compiled pipeline; between segments sit [`BreakerExecutor`]s
/// that handle stateful operations. The graph executes in topological order:
/// segment 0 → executor 0 → segment 1 → executor 1 → ...
///
/// For the MVP, this is a linear chain (no fan-out or fan-in).
pub struct CompiledQueryGraph {
    /// Breaker executors between segments, in execution order.
    executors: Vec<BreakerExecutor>,
    /// Input schema for the first segment.
    input_schema: SchemaRef,
    /// Output schema of the final segment.
    output_schema: SchemaRef,
}

impl CompiledQueryGraph {
    /// Creates a new graph with the given executors.
    #[must_use]
    pub fn new(
        executors: Vec<BreakerExecutor>,
        input_schema: SchemaRef,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            executors,
            input_schema,
            output_schema,
        }
    }

    /// Creates an empty graph (no breakers — passthrough).
    #[must_use]
    pub fn empty(schema: SchemaRef) -> Self {
        Self {
            executors: Vec::new(),
            input_schema: Arc::clone(&schema),
            output_schema: schema,
        }
    }

    /// Returns the number of breaker executors in the graph.
    #[must_use]
    pub fn executor_count(&self) -> usize {
        self.executors.len()
    }

    /// Returns a reference to the executors.
    #[must_use]
    pub fn executors(&self) -> &[BreakerExecutor] {
        &self.executors
    }

    /// Returns a mutable reference to the executors.
    pub fn executors_mut(&mut self) -> &mut [BreakerExecutor] {
        &mut self.executors
    }

    /// Returns the input schema.
    #[must_use]
    pub fn input_schema(&self) -> &SchemaRef {
        &self.input_schema
    }

    /// Returns the output schema.
    #[must_use]
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    /// Processes a batch through all executors in sequence.
    ///
    /// The batch enters the first executor; its output feeds the next, and so on.
    /// Returns the final output batches after all executors have processed.
    pub fn process_batch(&mut self, batch: RecordBatch) -> Vec<RecordBatch> {
        if self.executors.is_empty() {
            return vec![batch];
        }

        let mut current = vec![batch];
        for executor in &mut self.executors {
            let mut next = Vec::new();
            for b in current {
                next.extend(executor.process(b));
            }
            current = next;
        }
        current
    }

    /// Advances watermark through all executors.
    pub fn advance_watermark(&mut self, timestamp: i64) -> Vec<RecordBatch> {
        let mut output = Vec::new();
        for executor in &mut self.executors {
            output.extend(executor.advance_watermark(timestamp));
        }
        output
    }

    /// Checkpoints all executor states.
    #[must_use]
    pub fn checkpoint(&self) -> Vec<Vec<u8>> {
        self.executors
            .iter()
            .map(BreakerExecutor::checkpoint)
            .collect()
    }

    /// Restores all executor states from checkpoint data.
    ///
    /// # Panics
    ///
    /// Panics if `states.len() != self.executor_count()`.
    pub fn restore(&mut self, states: &[Vec<u8>]) {
        assert_eq!(
            states.len(),
            self.executors.len(),
            "state count mismatch: expected {}, got {}",
            self.executors.len(),
            states.len()
        );
        for (executor, state) in self.executors.iter_mut().zip(states.iter()) {
            executor.restore(state);
        }
    }
}

impl std::fmt::Debug for CompiledQueryGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledQueryGraph")
            .field("executors", &self.executors.len())
            .field("input_schema_fields", &self.input_schema.fields().len())
            .field("output_schema_fields", &self.output_schema.fields().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_expr::col;

    /// A passthrough Ring 1 operator that forwards batches unchanged.
    struct PassthroughOperator {
        schema: SchemaRef,
    }

    impl PassthroughOperator {
        fn new(schema: SchemaRef) -> Self {
            Self { schema }
        }
    }

    impl Ring1Operator for PassthroughOperator {
        fn process_batch(&mut self, batch: RecordBatch) -> Vec<RecordBatch> {
            vec![batch]
        }

        fn advance_watermark(&mut self, _timestamp: i64) -> Vec<RecordBatch> {
            vec![]
        }

        fn output_schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn checkpoint(&self) -> Vec<u8> {
            vec![]
        }

        fn restore(&mut self, _state: &[u8]) {}
    }

    /// A buffering operator that accumulates batches and emits on watermark.
    struct BufferingOperator {
        schema: SchemaRef,
        buffer: Vec<RecordBatch>,
    }

    impl BufferingOperator {
        fn new(schema: SchemaRef) -> Self {
            Self {
                schema,
                buffer: Vec::new(),
            }
        }
    }

    impl Ring1Operator for BufferingOperator {
        fn process_batch(&mut self, batch: RecordBatch) -> Vec<RecordBatch> {
            self.buffer.push(batch);
            vec![] // Hold until watermark.
        }

        fn advance_watermark(&mut self, _timestamp: i64) -> Vec<RecordBatch> {
            std::mem::take(&mut self.buffer)
        }

        fn output_schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn checkpoint(&self) -> Vec<u8> {
            // Simple: just store the count.
            (self.buffer.len() as u32).to_le_bytes().to_vec()
        }

        fn restore(&mut self, state: &[u8]) {
            if state.len() >= 4 {
                let count = u32::from_le_bytes(state[..4].try_into().unwrap());
                // Can't restore actual batches — just validate checkpoint works.
                assert!(count <= 1000, "sanity check");
            }
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Float64, false),
        ]))
    }

    fn test_batch(schema: &SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap()
    }

    // ── BreakerExecutor tests ────────────────────────────────────

    #[test]
    fn executor_passthrough() {
        let schema = test_schema();
        let op = Box::new(PassthroughOperator::new(Arc::clone(&schema)));
        let breaker = PipelineBreaker::Aggregate {
            group_exprs: vec![],
            aggr_exprs: vec![],
        };
        let mut exec = BreakerExecutor::new(breaker, op);

        let batch = test_batch(&schema);
        let out = exec.process(batch);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 3);
        assert_eq!(exec.batches_in(), 1);
        assert_eq!(exec.batches_out(), 1);
    }

    #[test]
    fn executor_buffering_emits_on_watermark() {
        let schema = test_schema();
        let op = Box::new(BufferingOperator::new(Arc::clone(&schema)));
        let breaker = PipelineBreaker::Aggregate {
            group_exprs: vec![col("key")],
            aggr_exprs: vec![],
        };
        let mut exec = BreakerExecutor::new(breaker, op);

        // Process batch — buffered, no output.
        let batch = test_batch(&schema);
        let out = exec.process(batch);
        assert!(out.is_empty());

        // Watermark triggers emission.
        let out = exec.advance_watermark(100);
        assert_eq!(out.len(), 1);
        assert_eq!(exec.batches_out(), 1);
    }

    #[test]
    fn executor_output_schema() {
        let schema = test_schema();
        let op = Box::new(PassthroughOperator::new(Arc::clone(&schema)));
        let breaker = PipelineBreaker::Sort {
            order_exprs: vec![],
        };
        let exec = BreakerExecutor::new(breaker, op);
        assert_eq!(exec.output_schema().fields().len(), 2);
    }

    #[test]
    fn executor_checkpoint_restore() {
        let schema = test_schema();
        let op = Box::new(BufferingOperator::new(Arc::clone(&schema)));
        let breaker = PipelineBreaker::Aggregate {
            group_exprs: vec![],
            aggr_exprs: vec![],
        };
        let mut exec = BreakerExecutor::new(breaker, op);

        // Process a batch to populate buffer.
        let batch = test_batch(&schema);
        exec.process(batch);

        // Checkpoint.
        let state = exec.checkpoint();
        assert!(!state.is_empty());

        // Restore.
        exec.restore(&state);
    }

    #[test]
    fn executor_debug() {
        let schema = test_schema();
        let op = Box::new(PassthroughOperator::new(schema));
        let breaker = PipelineBreaker::Sink;
        let exec = BreakerExecutor::new(breaker, op);
        let s = format!("{exec:?}");
        assert!(s.contains("BreakerExecutor"));
    }

    // ── CompiledQueryGraph tests ─────────────────────────────────

    #[test]
    fn empty_graph_passthrough() {
        let schema = test_schema();
        let mut graph = CompiledQueryGraph::empty(Arc::clone(&schema));

        assert_eq!(graph.executor_count(), 0);

        let batch = test_batch(&schema);
        let out = graph.process_batch(batch);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 3);
    }

    #[test]
    fn single_breaker_graph() {
        let schema = test_schema();
        let op = Box::new(PassthroughOperator::new(Arc::clone(&schema)));
        let breaker = PipelineBreaker::Aggregate {
            group_exprs: vec![],
            aggr_exprs: vec![],
        };
        let exec = BreakerExecutor::new(breaker, op);

        let mut graph =
            CompiledQueryGraph::new(vec![exec], Arc::clone(&schema), Arc::clone(&schema));

        assert_eq!(graph.executor_count(), 1);

        let batch = test_batch(&schema);
        let out = graph.process_batch(batch);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn multiple_breaker_graph() {
        let schema = test_schema();
        let executors = vec![
            BreakerExecutor::new(
                PipelineBreaker::Aggregate {
                    group_exprs: vec![],
                    aggr_exprs: vec![],
                },
                Box::new(PassthroughOperator::new(Arc::clone(&schema))),
            ),
            BreakerExecutor::new(
                PipelineBreaker::Sort {
                    order_exprs: vec![],
                },
                Box::new(PassthroughOperator::new(Arc::clone(&schema))),
            ),
        ];

        let mut graph =
            CompiledQueryGraph::new(executors, Arc::clone(&schema), Arc::clone(&schema));

        assert_eq!(graph.executor_count(), 2);

        let batch = test_batch(&schema);
        let out = graph.process_batch(batch);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn graph_watermark_propagation() {
        let schema = test_schema();
        let executors = vec![BreakerExecutor::new(
            PipelineBreaker::Aggregate {
                group_exprs: vec![],
                aggr_exprs: vec![],
            },
            Box::new(BufferingOperator::new(Arc::clone(&schema))),
        )];

        let mut graph =
            CompiledQueryGraph::new(executors, Arc::clone(&schema), Arc::clone(&schema));

        // Process — buffered.
        let batch = test_batch(&schema);
        let out = graph.process_batch(batch);
        assert!(out.is_empty());

        // Watermark — emits.
        let out = graph.advance_watermark(1000);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn graph_checkpoint_restore() {
        let schema = test_schema();
        let executors = vec![
            BreakerExecutor::new(
                PipelineBreaker::Aggregate {
                    group_exprs: vec![],
                    aggr_exprs: vec![],
                },
                Box::new(BufferingOperator::new(Arc::clone(&schema))),
            ),
            BreakerExecutor::new(
                PipelineBreaker::Sort {
                    order_exprs: vec![],
                },
                Box::new(PassthroughOperator::new(Arc::clone(&schema))),
            ),
        ];

        let mut graph =
            CompiledQueryGraph::new(executors, Arc::clone(&schema), Arc::clone(&schema));

        // Process a batch.
        let batch = test_batch(&schema);
        graph.process_batch(batch);

        // Checkpoint.
        let states = graph.checkpoint();
        assert_eq!(states.len(), 2);

        // Restore.
        graph.restore(&states);
    }

    #[test]
    fn graph_debug() {
        let schema = test_schema();
        let graph = CompiledQueryGraph::empty(schema);
        let s = format!("{graph:?}");
        assert!(s.contains("CompiledQueryGraph"));
        assert!(s.contains("executors"));
    }

    #[test]
    fn graph_schemas() {
        let schema = test_schema();
        let graph = CompiledQueryGraph::empty(Arc::clone(&schema));
        assert_eq!(graph.input_schema().fields().len(), 2);
        assert_eq!(graph.output_schema().fields().len(), 2);
    }

    #[test]
    fn graph_executors_mut() {
        let schema = test_schema();
        let executors = vec![BreakerExecutor::new(
            PipelineBreaker::Sink,
            Box::new(PassthroughOperator::new(Arc::clone(&schema))),
        )];

        let mut graph =
            CompiledQueryGraph::new(executors, Arc::clone(&schema), Arc::clone(&schema));

        assert_eq!(graph.executors_mut().len(), 1);
    }
}
