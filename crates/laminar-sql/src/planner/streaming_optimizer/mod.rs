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
    if plan.downcast_ref::<SortExec>().is_some() && has_unbounded_child(plan) {
        violations.push(StreamingViolation {
            operator: name.to_string(),
            reason: "Sort requires buffering all input; unbounded source will \
                     buffer forever. Remove ORDER BY or add a window."
                .to_string(),
            plan_path: current_path.clone(),
        });
    }

    // Check 2: Final AggregateExec on unbounded input
    if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
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
mod tests;
