# F-SSQL-005: DataFusion Cooperative Scheduling

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SSQL-005 |
| **Status** | 📝 Draft |
| **Priority** | P3 |
| **Phase** | 3 |
| **Effort** | S |
| **Dependencies** | — |
| **Owner** | TBD |
| **Created** | 2026-02-25 |
| **Updated** | 2026-02-25 |

## Summary

Add `supports_cooperative_scheduling() -> bool { true }` to `StreamingScanExec` and any future custom `ExecutionPlan` implementations. DataFusion v49+ introduced cooperative scheduling via the `EnsureCooperative` optimizer rule, which auto-wraps execution plans that opt in. This ensures that custom operators yield periodically, preventing any single operator from starving others in concurrent query execution. Not critical for Ring 0 (which has its own scheduling) but needed for correct behavior when DataFusion runs queries asynchronously in Ring 1.

## Goals

- Opt into DataFusion's cooperative scheduling for `StreamingScanExec`
- Prevent operator starvation in multi-query Ring 1 execution
- Future-proof for async DataFusion query execution

## Non-Goals

- Implementing custom cooperative scheduling logic (DataFusion handles this via `EnsureCooperative`)
- Changes to Ring 0 scheduling (Ring 0 uses `TaskBudget` from F070)
- Changes to the streaming query execution model

## Technical Design

### Architecture

**Ring**: Ring 1
**Crate**: `laminar-sql`
**File**: `crates/laminar-sql/src/datafusion/exec.rs`

### Implementation

```rust
// In the ExecutionPlan impl for StreamingScanExec:
impl ExecutionPlan for StreamingScanExec {
    // ... existing methods ...

    fn supports_cooperative_scheduling(&self) -> bool {
        true
    }
}
```

This is a one-method addition (~5 lines of code including the doc comment). When DataFusion sees `supports_cooperative_scheduling() == true`, its `EnsureCooperative` optimizer rule wraps the plan node to yield control periodically during execution.

### Error Handling

No error cases. This is a pure capability declaration.

## Test Plan

### Unit Tests

- [ ] `test_streaming_scan_exec_supports_cooperative_scheduling`

### Integration Tests

- [ ] End-to-end: verify `StreamingScanExec` is wrapped by `EnsureCooperative` in the optimized plan

## Rollout Plan

1. **Phase 1**: Add method + unit test
2. **Phase 2**: Verify via integration test that `EnsureCooperative` wrapping is applied
3. **Phase 3**: Code review + merge

## Open Questions

- [ ] Should we also add this to any other custom `ExecutionPlan` implementations (e.g., ASOF join exec)?

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

- This is the smallest feature in the series. It enables DataFusion's built-in cooperative scheduling without any custom logic.
- Ring 0 already has its own scheduling via `TaskBudget` (F070), so this only affects Ring 1 DataFusion execution.
- The `EnsureCooperative` rule was added in DataFusion v49 and refined in v50-52.

## References

- [DataFusion cooperative scheduling](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html#method.supports_cooperative_scheduling)
- [Gap Analysis: Opportunity 1](../../../GAP-ANALYSIS-STATEFUL-STREAMING.md)
- [StreamingScanExec](../../../../crates/laminar-sql/src/datafusion/exec.rs)
- [F070: Task Budget Enforcement](../../phase-2/F070-task-budget-enforcement.md)
