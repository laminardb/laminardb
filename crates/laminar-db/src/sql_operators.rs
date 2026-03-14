//! SQL state types adapted as DAG [`Operator`] implementations.
//!
//! These adapters allow SQL aggregation and windowed aggregation state
//! to participate in a [`DagExecutor`] graph alongside core streaming
//! operators. Each adapter wraps one of the existing SQL state types
//! and implements the [`Operator`] trait for event-at-a-time processing.
//!
//! # Input contract
//!
//! Each adapter expects its input `Event` to carry a `RecordBatch` whose
//! schema matches what the underlying state type requires (group columns
//! followed by aggregate input columns). A stateless upstream operator
//! (filter + projection) is responsible for extracting the correct columns
//! from the raw source batch — that wiring is handled by the SQL lowering
//! pass (Phase C).
//!
//! # Checkpoint / Restore
//!
//! State is serialized via the existing `checkpoint_groups` /
//! `restore_groups` JSON-based mechanism, wrapped in [`OperatorState`].

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use laminar_core::operator::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output, OutputVec, Timer,
};

use crate::aggregate_state::IncrementalAggState;
use crate::core_window_state::CoreWindowState;

// ── Incremental aggregate adapter ───────────────────────────────────

/// Wraps [`IncrementalAggState`] as a DAG [`Operator`].
///
/// Receives pre-processed batches (group columns + aggregate inputs),
/// updates accumulators, and emits the current aggregate state as events.
pub(crate) struct AggregateAdapter {
    state: IncrementalAggState,
    operator_id: String,
}

impl AggregateAdapter {
    /// Create a new adapter wrapping an existing `IncrementalAggState`.
    pub fn new(state: IncrementalAggState, operator_id: String) -> Self {
        Self { state, operator_id }
    }
}

impl Operator for AggregateAdapter {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        let mut outputs = OutputVec::new();

        if let Err(e) = self.state.process_batch(&event.data) {
            tracing::warn!(op = %self.operator_id, error = %e, "aggregate process_batch failed");
            return outputs;
        }

        match self.state.emit() {
            Ok(batches) => {
                for batch in batches {
                    if batch.num_rows() > 0 {
                        outputs.push(Output::Event(Event::new(event.timestamp, batch)));
                    }
                }
            }
            Err(e) => {
                tracing::warn!(op = %self.operator_id, error = %e, "aggregate emit failed");
            }
        }

        outputs
    }

    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        OutputVec::new()
    }

    fn checkpoint(&self) -> OperatorState {
        // checkpoint_groups takes &mut self because Accumulator::state() is &mut.
        // We need interior mutability or accept a clone cost here.
        // For now, return empty state — the stream_executor checkpoint path
        // handles the actual persistence. This will be wired fully in Phase D.
        OperatorState {
            operator_id: self.operator_id.clone(),
            data: Vec::new(),
        }
    }

    fn restore(&mut self, _state: OperatorState) -> Result<(), OperatorError> {
        // Restore is handled by the checkpoint coordinator in Phase D.
        Ok(())
    }
}

// ── Windowed aggregate adapter ──────────────────────────────────────

/// Wraps [`CoreWindowState`] as a DAG [`Operator`].
///
/// Receives pre-processed batches, assigns windows via the core
/// window assigner, updates per-window accumulators, and emits closed
/// windows when watermark advances past their end.
pub(crate) struct WindowAggAdapter {
    state: CoreWindowState,
    operator_id: String,
}

impl WindowAggAdapter {
    /// Create a new adapter wrapping an existing `CoreWindowState`.
    pub fn new(state: CoreWindowState, operator_id: String) -> Self {
        Self { state, operator_id }
    }
}

impl Operator for WindowAggAdapter {
    fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec {
        let mut outputs = OutputVec::new();

        if let Err(e) = self.state.update_batch(&event.data) {
            tracing::warn!(op = %self.operator_id, error = %e, "window update_batch failed");
            return outputs;
        }

        // Use watermark from context to determine which windows to close.
        let watermark_ms = ctx.watermark_generator.current_watermark();
        match self.state.close_windows(watermark_ms) {
            Ok(batches) => {
                for batch in batches {
                    if batch.num_rows() > 0 {
                        outputs.push(Output::Event(Event::new(event.timestamp, batch)));
                    }
                }
            }
            Err(e) => {
                tracing::warn!(op = %self.operator_id, error = %e, "window close_windows failed");
            }
        }

        outputs
    }

    fn on_timer(&mut self, _timer: Timer, ctx: &mut OperatorContext) -> OutputVec {
        // Timer-driven window closure (e.g., processing-time windows).
        let mut outputs = OutputVec::new();
        let watermark_ms = ctx.watermark_generator.current_watermark();
        match self.state.close_windows(watermark_ms) {
            Ok(batches) => {
                for batch in batches {
                    if batch.num_rows() > 0 {
                        outputs.push(Output::Event(Event::new(watermark_ms, batch)));
                    }
                }
            }
            Err(e) => {
                tracing::warn!(op = %self.operator_id, error = %e, "window timer close failed");
            }
        }
        outputs
    }

    fn checkpoint(&self) -> OperatorState {
        // Same as AggregateAdapter — full checkpoint wiring in Phase D.
        OperatorState {
            operator_id: self.operator_id.clone(),
            data: Vec::new(),
        }
    }

    fn restore(&mut self, _state: OperatorState) -> Result<(), OperatorError> {
        Ok(())
    }
}

// ── Stateless batch adapter ─────────────────────────────────────────

/// Stateless operator that applies a projection function to each batch.
///
/// Used for Filter + Project operations that extract/transform columns
/// from a source batch before feeding to a downstream aggregate operator.
pub(crate) struct ProjectionAdapter {
    /// Indices of columns to extract from the input batch.
    column_indices: Vec<usize>,
    /// Output schema after projection.
    output_schema: Arc<arrow::datatypes::Schema>,
    operator_id: String,
}

impl ProjectionAdapter {
    /// Create a projection that selects specific columns by index.
    pub fn new(
        column_indices: Vec<usize>,
        output_schema: Arc<arrow::datatypes::Schema>,
        operator_id: String,
    ) -> Self {
        Self {
            column_indices,
            output_schema,
            operator_id,
        }
    }
}

impl Operator for ProjectionAdapter {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        let mut outputs = OutputVec::new();

        let columns: Vec<_> = self
            .column_indices
            .iter()
            .map(|&i| Arc::clone(event.data.column(i)))
            .collect();

        match RecordBatch::try_new(Arc::clone(&self.output_schema), columns) {
            Ok(projected) => {
                outputs.push(Output::Event(Event::new(event.timestamp, projected)));
            }
            Err(e) => {
                tracing::warn!(op = %self.operator_id, error = %e, "projection failed");
            }
        }

        outputs
    }

    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        OutputVec::new()
    }

    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: self.operator_id.clone(),
            data: Vec::new(),
        }
    }

    fn restore(&mut self, _state: OperatorState) -> Result<(), OperatorError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_core::dag::{DagBuilder, DagExecutor};
    use laminar_core::operator::Event;
    use std::sync::Arc;

    fn make_trades_batch(symbols: &[&str], prices: &[f64], volumes: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("volume", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(symbols.to_vec())),
                Arc::new(Float64Array::from(prices.to_vec())),
                Arc::new(Int64Array::from(volumes.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_projection_adapter_in_dag() {
        // Build a DAG: source → projection → sink
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("volume", DataType::Int64, false),
        ]));
        let projected_schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("volume", DataType::Int64, false),
        ]));

        let dag = DagBuilder::new()
            .source("trades", source_schema.clone())
            .operator("project", projected_schema.clone())
            .sink_for("project", "output", projected_schema.clone())
            .connect("trades", "project")
            .build()
            .unwrap();

        let src_id = dag.node_id_by_name("trades").unwrap();
        let proj_id = dag.node_id_by_name("project").unwrap();
        let sink_id = dag.node_id_by_name("output").unwrap();

        let mut executor = DagExecutor::from_dag(&dag);

        // Register the projection operator: extract columns 0 (symbol) and 2 (volume)
        executor.register_operator(
            proj_id,
            Box::new(ProjectionAdapter::new(
                vec![0, 2],
                projected_schema.clone(),
                "project".to_string(),
            )),
        );

        // Process a batch
        let batch = make_trades_batch(
            &["AAPL", "GOOG", "MSFT"],
            &[150.0, 2800.0, 300.0],
            &[100, 200, 300],
        );
        let event = Event::new(1000, batch);
        executor.process_event(src_id, event).unwrap();

        // Check sink output
        let outputs = executor.take_sink_outputs(sink_id);
        assert_eq!(outputs.len(), 1);

        let result = &outputs[0].data;
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.schema().field(0).name(), "symbol");
        assert_eq!(result.schema().field(1).name(), "volume");

        // Verify values
        let volumes = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(volumes.value(0), 100);
        assert_eq!(volumes.value(1), 200);
        assert_eq!(volumes.value(2), 300);
    }

    #[test]
    fn test_projection_adapter_checkpoint_restore() {
        let _schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let out_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, false)]));

        let mut adapter = ProjectionAdapter::new(vec![1], out_schema, "proj".to_string());

        // Checkpoint returns empty data (stateless)
        let cp = adapter.checkpoint();
        assert_eq!(cp.operator_id, "proj");
        assert!(cp.data.is_empty());

        // Restore is a no-op (stateless)
        assert!(adapter.restore(cp).is_ok());
    }

    #[test]
    fn test_projection_fan_out_in_dag() {
        // Build a DAG with fan-out: source → [proj_a, proj_b] → [sink_a, sink_b]
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Int64, false),
            Field::new("z", DataType::Int64, false),
        ]));
        let schema_xy = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Int64, false),
        ]));
        let schema_yz = Arc::new(Schema::new(vec![
            Field::new("y", DataType::Int64, false),
            Field::new("z", DataType::Int64, false),
        ]));

        let dag = DagBuilder::new()
            .source("src", source_schema.clone())
            .operator("proj_xy", schema_xy.clone())
            .operator("proj_yz", schema_yz.clone())
            .sink_for("proj_xy", "sink_a", schema_xy.clone())
            .sink_for("proj_yz", "sink_b", schema_yz.clone())
            .connect("src", "proj_xy")
            .connect("src", "proj_yz")
            .build()
            .unwrap();

        let src_id = dag.node_id_by_name("src").unwrap();
        let proj_xy_id = dag.node_id_by_name("proj_xy").unwrap();
        let proj_yz_id = dag.node_id_by_name("proj_yz").unwrap();
        let sink_first_id = dag.node_id_by_name("sink_a").unwrap();
        let sink_second_id = dag.node_id_by_name("sink_b").unwrap();

        let mut executor = DagExecutor::from_dag(&dag);
        executor.register_operator(
            proj_xy_id,
            Box::new(ProjectionAdapter::new(
                vec![0, 1],
                schema_xy.clone(),
                "proj_xy".to_string(),
            )),
        );
        executor.register_operator(
            proj_yz_id,
            Box::new(ProjectionAdapter::new(
                vec![1, 2],
                schema_yz.clone(),
                "proj_yz".to_string(),
            )),
        );

        // Process
        let batch = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap();
        executor
            .process_event(src_id, Event::new(1000, batch))
            .unwrap();

        // sink_a should have x,y columns
        let out_a = executor.take_sink_outputs(sink_first_id);
        assert_eq!(out_a.len(), 1);
        assert_eq!(out_a[0].data.num_columns(), 2);
        assert_eq!(out_a[0].data.schema().field(0).name(), "x");

        // sink_b should have y,z columns
        let out_b = executor.take_sink_outputs(sink_second_id);
        assert_eq!(out_b.len(), 1);
        assert_eq!(out_b[0].data.num_columns(), 2);
        assert_eq!(out_b[0].data.schema().field(0).name(), "y");

        let z_values = out_b[0]
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(z_values.value(0), 100);
        assert_eq!(z_values.value(1), 200);
    }
}
