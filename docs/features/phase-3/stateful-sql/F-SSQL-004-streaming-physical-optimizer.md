# F-SSQL-004: Streaming Physical Optimizer Rule

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SSQL-004 |
| **Status** | 📝 Draft |
| **Priority** | P2 |
| **Phase** | 3 |
| **Effort** | M |
| **Dependencies** | — |
| **Owner** | TBD |
| **Created** | 2026-02-25 |
| **Updated** | 2026-02-25 |

## Summary

Add a `StreamingPhysicalValidator` implementing DataFusion's `PhysicalOptimizerRule` that detects unsafe operators in streaming query plans. DataFusion's default optimizer does not enforce streaming constraints: a `SELECT * FROM unbounded_source ORDER BY price` will silently buffer forever (blocking sort on unbounded input), and `AggregateExec(Final)` without windowing will never emit results. This rule inspects the physical plan tree and warns/rejects plans that contain pipeline-breaking operators on unbounded inputs.

## Goals

- Detect `SortExec` on `Boundedness::Unbounded` inputs and reject/warn
- Detect `AggregateExec(Final)` without windowing on unbounded inputs and reject/warn
- Configurable behavior: `Reject` (default), `Warn`, or `Off`
- Register automatically on `SessionContext` creation in `create_session_context()`
- Zero overhead for valid streaming plans (rule runs once at plan creation, not per batch)

## Non-Goals

- Rewriting invalid plans into valid streaming alternatives (that's F-SSQL-003's job)
- Validating logical plans (this operates on physical plans only)
- Custom operators beyond DataFusion's built-in set
- Runtime monitoring of operator behavior (this is a compile-time check)

## Technical Design

### Architecture

**Ring**: Ring 1 (plan compilation)
**Crate**: `laminar-sql`
**Module**: `crates/laminar-sql/src/planner/streaming_optimizer.rs`

The rule is registered as the last `PhysicalOptimizerRule` in the session context, after DataFusion's default rules have run. It inspects the final physical plan and either passes it through (valid) or returns an error (invalid).

### API/Interface

```rust
/// Validates that a physical plan is safe for streaming execution.
///
/// Detects pipeline-breaking operators (Sort, Final Aggregate) on
/// unbounded inputs and rejects or warns depending on configuration.
pub struct StreamingPhysicalValidator {
    mode: StreamingValidatorMode,
}

/// How the validator handles violations.
#[derive(Debug, Clone, Copy)]
pub enum StreamingValidatorMode {
    /// Return an error, preventing plan execution. Default.
    Reject,
    /// Log a warning but allow execution.
    Warn,
    /// Disable validation entirely.
    Off,
}

impl StreamingPhysicalValidator {
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
                    tracing::warn!("Streaming plan violation: {v}");
                }
                Ok(plan)
            }
            StreamingValidatorMode::Off => unreachable!(),
        }
    }

    fn name(&self) -> &str {
        "streaming_physical_validator"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
```

### Violation Detection

```rust
/// A streaming plan violation found during validation.
struct StreamingViolation {
    /// Which operator is problematic.
    operator: String,
    /// Why it's a problem.
    reason: String,
    /// Position in the plan tree (for error messages).
    plan_path: String,
}

/// Walk the physical plan tree and collect violations.
fn find_streaming_violations(plan: &Arc<dyn ExecutionPlan>) -> Vec<StreamingViolation> {
    let mut violations = Vec::new();
    walk_plan(plan, &mut violations, String::new());
    violations
}

fn walk_plan(
    plan: &Arc<dyn ExecutionPlan>,
    violations: &mut Vec<StreamingViolation>,
    path: String,
) {
    let boundedness = plan.boundedness();
    let name = plan.name().to_string();
    let current_path = if path.is_empty() {
        name.clone()
    } else {
        format!("{path} → {name}")
    };

    // Check 1: SortExec on unbounded input
    if name == "SortExec" && has_unbounded_child(plan) {
        violations.push(StreamingViolation {
            operator: name.clone(),
            reason: "Sort requires buffering all input; unbounded source will \
                     buffer forever".to_string(),
            plan_path: current_path.clone(),
        });
    }

    // Check 2: Final AggregateExec on unbounded input without windowing
    if name == "AggregateExec" && is_final_aggregate(plan) && has_unbounded_child(plan) {
        violations.push(StreamingViolation {
            operator: name.clone(),
            reason: "Final aggregation on unbounded input will never emit \
                     results; use a window function or EMIT clause".to_string(),
            plan_path: current_path.clone(),
        });
    }

    // Recurse into children
    for child in plan.children() {
        walk_plan(child, violations, current_path.clone());
    }
}

fn has_unbounded_child(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.children().iter().any(|c| {
        matches!(c.boundedness(), Boundedness::Unbounded { .. })
    })
}
```

### Registration

```rust
// In create_session_context() (laminar-sql or laminar-db):
let validator = StreamingPhysicalValidator::new(StreamingValidatorMode::Reject);
ctx.add_physical_optimizer_rule(Arc::new(validator));
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `DataFusionError::Plan` | Sort on unbounded input detected | User must add window or remove ORDER BY |
| `DataFusionError::Plan` | Final aggregate on unbounded input | User must add window or EMIT clause |

## Test Plan

### Unit Tests

- [ ] `test_sort_on_unbounded_rejected`
- [ ] `test_sort_on_bounded_allowed`
- [ ] `test_final_aggregate_on_unbounded_rejected`
- [ ] `test_windowed_aggregate_on_unbounded_allowed`
- [ ] `test_projection_filter_on_unbounded_allowed`
- [ ] `test_warn_mode_logs_but_passes`
- [ ] `test_off_mode_skips_validation`
- [ ] `test_nested_plan_violation_detected`
- [ ] `test_violation_message_includes_plan_path`

### Integration Tests

- [ ] End-to-end: `CREATE STREAM ... AS SELECT * FROM source ORDER BY price` returns clear error
- [ ] End-to-end: `CREATE STREAM ... AS SELECT symbol, COUNT(*) FROM source GROUP BY symbol` with EMIT clause succeeds
- [ ] End-to-end: valid streaming plan passes validation without overhead

## Rollout Plan

1. **Phase 1**: `StreamingPhysicalValidator` + `find_streaming_violations()` + unit tests
2. **Phase 2**: Register on `SessionContext` + integration tests
3. **Phase 3**: Add configuration to `LaminarDB::Config` for validator mode
4. **Phase 4**: Code review + merge

## Open Questions

- [ ] Should we detect `HashJoinExec` with unbounded right side (full outer join on unbounded)?
- [ ] Should the validator be aware of LaminarDB-specific operators (ASOF, temporal join)?
- [ ] Should violation messages suggest specific fixes (e.g., "add TUMBLE(...) to GROUP BY")?
- [ ] Should the mode be configurable per-query (via SQL hint) or only globally?

## Completion Checklist

- [ ] Code implemented
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## Notes

- This rule is intentionally conservative: it only detects obviously unsafe patterns. Queries that are technically unsafe but might work in practice (e.g., aggregation with periodic EMIT) are allowed through.
- The rule runs at plan creation time (once per `add_query()`), not per execution cycle, so there is zero ongoing overhead.
- DataFusion v52 already has `Boundedness` tracking on all `ExecutionPlan` implementations, so we can rely on its correctness for the unbounded detection.

## References

- [DataFusion PhysicalOptimizerRule trait](https://docs.rs/datafusion/latest/datafusion/physical_optimizer/trait.PhysicalOptimizerRule.html)
- [Gap Analysis: Missing 4 — Physical Optimizer Rule](../../../GAP-ANALYSIS-STATEFUL-STREAMING.md)
- [StreamingScanExec — Boundedness::Unbounded](../../../../crates/laminar-sql/src/datafusion/exec.rs)
