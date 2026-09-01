use super::*;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, Partitioning};
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};
use datafusion_common::config::ConfigOptions;

// ── Mock unbounded leaf node ────────────────────────────────────

#[derive(Debug)]
struct MockUnboundedExec {
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl MockUnboundedExec {
    fn new(schema: SchemaRef) -> Self {
        let eq = EquivalenceProperties::new(Arc::clone(&schema));
        let props = Arc::new(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        ));
        Self { schema, props }
    }
}

impl DisplayAs for MockUnboundedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MockUnboundedExec")
    }
}

impl ExecutionPlan for MockUnboundedExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "MockUnboundedExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        unimplemented!("mock")
    }
}

// ── Mock bounded leaf node ──────────────────────────────────────

#[derive(Debug)]
struct MockBoundedExec {
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl MockBoundedExec {
    fn new(schema: SchemaRef) -> Self {
        let eq = EquivalenceProperties::new(Arc::clone(&schema));
        let props = Arc::new(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self { schema, props }
    }
}

impl DisplayAs for MockBoundedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MockBoundedExec")
    }
}

impl ExecutionPlan for MockBoundedExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "MockBoundedExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        unimplemented!("mock")
    }
}

// ── Mock passthrough node (not sort/aggregate) ──────────────────

#[derive(Debug)]
struct MockPassthroughExec {
    child: Arc<dyn ExecutionPlan>,
    props: Arc<PlanProperties>,
}

impl MockPassthroughExec {
    fn new(child: Arc<dyn ExecutionPlan>) -> Self {
        let props = child.properties().clone();
        Self { child, props }
    }
}

impl DisplayAs for MockPassthroughExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MockPassthroughExec")
    }
}

impl ExecutionPlan for MockPassthroughExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "MockPassthroughExec"
    }

    fn schema(&self) -> SchemaRef {
        self.child.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(Arc::clone(&children[0]))))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        unimplemented!("mock")
    }
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
    ]))
}

fn make_sort_on(child: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    use arrow_schema::SortOptions;
    use datafusion::physical_expr::{expressions::Column, PhysicalSortExpr};

    let sort_expr = PhysicalSortExpr::new(Arc::new(Column::new("id", 0)), SortOptions::default());
    let ordering = LexOrdering::new(vec![sort_expr]).expect("non-empty sort expr list");
    Arc::new(SortExec::new(ordering, child))
}

fn make_final_aggregate_on(child: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;

    let schema = child.schema();
    let group_by = PhysicalGroupBy::new_single(vec![]);
    let agg = AggregateExec::try_new(
        AggregateMode::Final,
        group_by,
        vec![],
        vec![],
        child,
        Arc::clone(&schema),
    )
    .expect("failed to create AggregateExec");
    Arc::new(agg)
}

// ── Unit tests: violation detection ─────────────────────────────

#[test]
fn test_sort_on_unbounded_rejected() {
    let leaf = Arc::new(MockUnboundedExec::new(test_schema()));
    let plan = make_sort_on(leaf);
    let violations = find_streaming_violations(&plan);
    assert_eq!(violations.len(), 1);
    assert!(violations[0].reason.contains("Sort requires buffering"));
}

#[test]
fn test_sort_on_bounded_allowed() {
    let leaf = Arc::new(MockBoundedExec::new(test_schema()));
    let plan = make_sort_on(leaf);
    let violations = find_streaming_violations(&plan);
    assert!(violations.is_empty());
}

#[test]
fn test_final_aggregate_on_unbounded_rejected() {
    let leaf = Arc::new(MockUnboundedExec::new(test_schema()));
    let plan = make_final_aggregate_on(leaf);
    let violations = find_streaming_violations(&plan);
    assert_eq!(violations.len(), 1);
    assert!(violations[0].reason.contains("Final aggregation"));
}

#[test]
fn test_passthrough_on_unbounded_allowed() {
    let leaf = Arc::new(MockUnboundedExec::new(test_schema()));
    let plan: Arc<dyn ExecutionPlan> = Arc::new(MockPassthroughExec::new(leaf));
    let violations = find_streaming_violations(&plan);
    assert!(violations.is_empty());
}

#[test]
fn test_nested_plan_violation_detected() {
    let leaf = Arc::new(MockUnboundedExec::new(test_schema()));
    let sort = make_sort_on(leaf);
    // Wrap the sort inside a passthrough so the violation is deep in the tree
    let plan: Arc<dyn ExecutionPlan> = Arc::new(MockPassthroughExec::new(sort));
    let violations = find_streaming_violations(&plan);
    assert_eq!(violations.len(), 1);
    assert!(
        violations[0].plan_path.contains("SortExec"),
        "path was: {}",
        violations[0].plan_path
    );
}

// ── Unit tests: modes ───────────────────────────────────────────

#[test]
fn test_reject_mode_returns_error() {
    let validator = StreamingPhysicalValidator::new(StreamingValidatorMode::Reject);
    let leaf = Arc::new(MockUnboundedExec::new(test_schema()));
    let plan = make_sort_on(leaf);
    let config = ConfigOptions::new();
    let result = validator.optimize(plan, &config);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("Streaming plan validation failed"),
        "error was: {err}"
    );
}

#[test]
fn test_warn_mode_passes_through() {
    let validator = StreamingPhysicalValidator::new(StreamingValidatorMode::Warn);
    let leaf = Arc::new(MockUnboundedExec::new(test_schema()));
    let plan = make_sort_on(leaf);
    let config = ConfigOptions::new();
    let result = validator.optimize(plan, &config);
    assert!(result.is_ok());
}

#[test]
fn test_off_mode_skips_validation() {
    let validator = StreamingPhysicalValidator::new(StreamingValidatorMode::Off);
    let leaf = Arc::new(MockUnboundedExec::new(test_schema()));
    let plan = make_sort_on(leaf);
    let config = ConfigOptions::new();
    let result = validator.optimize(plan, &config);
    assert!(result.is_ok());
}

// ── Integration test via create_streaming_context ───────────────

#[tokio::test]
async fn test_streaming_context_rejects_unbounded_sort() {
    use crate::datafusion::{
        create_streaming_context, ChannelStreamSource, StreamingTableProvider,
    };
    use arrow_schema::{DataType, Field, Schema};

    let ctx = create_streaming_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
    ]));

    let source = Arc::new(ChannelStreamSource::new(Arc::clone(&schema)));
    let _sender = source.take_sender();
    let provider = StreamingTableProvider::new("events", source);
    ctx.register_table("events", Arc::new(provider)).unwrap();

    // ORDER BY on unbounded source should fail at plan creation
    let result = ctx.sql("SELECT * FROM events ORDER BY id").await;

    // The physical optimizer should reject this plan
    // (DataFusion creates the physical plan during sql() or at collect())
    match result {
        Ok(df) => {
            // Physical plan creation may be deferred to collect()
            let exec_result = df.collect().await;
            assert!(
                exec_result.is_err(),
                "Sort on unbounded stream should be rejected"
            );
            let err = exec_result.unwrap_err().to_string();
            assert!(
                err.contains("Streaming plan validation failed")
                    || err.contains("Sort requires buffering"),
                "Expected streaming validation error, got: {err}"
            );
        }
        Err(e) => {
            let err = e.to_string();
            assert!(
                err.contains("Streaming plan validation failed")
                    || err.contains("Sort requires buffering"),
                "Expected streaming validation error, got: {err}"
            );
        }
    }
}
