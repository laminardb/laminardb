//! Physical optimizer rule for streaming plan validation.
//!
//! Detects pipeline-breaking operators (Sort, Final Aggregate) on unbounded
//! inputs and rejects or warns at plan-creation time, before any execution
//! begins.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_common::config::ConfigOptions;
use datafusion_common::DataFusionError;

/// How the validator handles streaming plan violations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingValidatorMode {
    /// Return an error, preventing plan execution. Default.
    Reject,
    /// Log a warning but allow execution.
    Warn,
    /// Disable validation entirely.
    Off,
}

/// A streaming plan violation detected during validation.
#[derive(Debug)]
struct StreamingViolation {
    operator: String,
    reason: String,
    plan_path: String,
}

/// Validates that a physical plan is safe for streaming execution.
///
/// Detects pipeline-breaking operators (Sort, Final Aggregate) on
/// unbounded inputs and rejects or warns depending on configuration.
#[derive(Debug)]
pub struct StreamingPhysicalValidator {
    mode: StreamingValidatorMode,
}

impl StreamingPhysicalValidator {
    /// Creates a new validator with the given mode.
    #[must_use]
    pub fn new(mode: StreamingValidatorMode) -> Self {
        Self { mode }
    }
}

impl PhysicalOptimizerRule for StreamingPhysicalValidator {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if matches!(self.mode, StreamingValidatorMode::Off) {
            return Ok(plan);
        }

        let violations = find_streaming_violations(&plan);
        if violations.is_empty() {
            return Ok(plan);
        }

        match self.mode {
            StreamingValidatorMode::Reject => {
                Err(DataFusionError::Plan(format_violations(&violations)))
            }
            StreamingValidatorMode::Warn => {
                for v in &violations {
                    tracing::warn!(
                        operator = %v.operator,
                        path = %v.plan_path,
                        "Streaming plan violation: {}", v.reason
                    );
                }
                Ok(plan)
            }
            StreamingValidatorMode::Off => unreachable!(),
        }
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "streaming_physical_validator"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn find_streaming_violations(plan: &Arc<dyn ExecutionPlan>) -> Vec<StreamingViolation> {
    let mut violations = Vec::new();
    walk_plan(plan, &mut violations, "");
    violations
}

fn walk_plan(plan: &Arc<dyn ExecutionPlan>, violations: &mut Vec<StreamingViolation>, path: &str) {
    let name = plan.name();
    let current_path = if path.is_empty() {
        name.to_string()
    } else {
        format!("{path} -> {name}")
    };

    // Check 1: SortExec on unbounded input
    if plan.as_any().downcast_ref::<SortExec>().is_some() && has_unbounded_child(plan) {
        violations.push(StreamingViolation {
            operator: name.to_string(),
            reason: "Sort requires buffering all input; unbounded source will \
                     buffer forever. Remove ORDER BY or add a window."
                .to_string(),
            plan_path: current_path.clone(),
        });
    }

    // Check 2: Final AggregateExec on unbounded input
    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        if matches!(
            agg.mode(),
            &AggregateMode::Final | &AggregateMode::FinalPartitioned
        ) && has_unbounded_child(plan)
        {
            violations.push(StreamingViolation {
                operator: name.to_string(),
                reason: "Final aggregation on unbounded input will never emit \
                         results. Use a window function (TUMBLE/HOP/SESSION) or \
                         add an EMIT clause."
                    .to_string(),
                plan_path: current_path.clone(),
            });
        }
    }

    for child in plan.children() {
        walk_plan(child, violations, &current_path);
    }
}

fn has_unbounded_child(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.children()
        .iter()
        .any(|c| matches!(c.boundedness(), Boundedness::Unbounded { .. }))
}

fn format_violations(violations: &[StreamingViolation]) -> String {
    use std::fmt::Write;

    let mut msg = String::from("Streaming plan validation failed:\n");
    for (i, v) in violations.iter().enumerate() {
        let _ = writeln!(
            msg,
            "  {}. [{}] {} (at: {})",
            i + 1,
            v.operator,
            v.reason,
            v.plan_path
        );
    }
    msg
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;

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
        props: PlanProperties,
    }

    impl MockUnboundedExec {
        fn new(schema: SchemaRef) -> Self {
            let eq = EquivalenceProperties::new(Arc::clone(&schema));
            let props = PlanProperties::new(
                eq,
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Unbounded {
                    requires_infinite_memory: false,
                },
            );
            Self { schema, props }
        }
    }

    impl DisplayAs for MockUnboundedExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "MockUnboundedExec")
        }
    }

    impl ExecutionPlan for MockUnboundedExec {
        fn name(&self) -> &str {
            "MockUnboundedExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn properties(&self) -> &PlanProperties {
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
        props: PlanProperties,
    }

    impl MockBoundedExec {
        fn new(schema: SchemaRef) -> Self {
            let eq = EquivalenceProperties::new(Arc::clone(&schema));
            let props = PlanProperties::new(
                eq,
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            );
            Self { schema, props }
        }
    }

    impl DisplayAs for MockBoundedExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "MockBoundedExec")
        }
    }

    impl ExecutionPlan for MockBoundedExec {
        fn name(&self) -> &str {
            "MockBoundedExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn properties(&self) -> &PlanProperties {
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
        props: PlanProperties,
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
        fn name(&self) -> &str {
            "MockPassthroughExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.child.schema()
        }

        fn properties(&self) -> &PlanProperties {
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

        let sort_expr =
            PhysicalSortExpr::new(Arc::new(Column::new("id", 0)), SortOptions::default());
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
}
