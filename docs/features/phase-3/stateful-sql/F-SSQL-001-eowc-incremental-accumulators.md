# F-SSQL-001: EOWC Incremental Window Accumulators

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SSQL-001 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | L |
| **Dependencies** | F-SSQL-000, F075 |
| **Owner** | TBD |
| **Created** | 2026-02-25 |
| **Updated** | 2026-02-25 |

## Summary

Replace the `EowcState.accumulated_sources` raw-batch accumulation pattern (O(N) memory and compute per window close) with per-window-per-group incremental `Accumulator` state (O(groups) memory, O(1) per event). Currently, EOWC queries store every raw source batch across an entire window and re-aggregate from scratch on window close. For a 1-hour tumbling window at 100K events/sec, this means re-processing 360M rows. This feature eliminates that cost by feeding each micro-batch to accumulators incrementally and emitting directly from accumulator state on window close.

## Goals

- Replace raw-batch storage with incremental per-window-per-group accumulator state
- Reduce window-close latency from O(N) to O(groups) for aggregate EOWC queries
- Reduce memory from O(events) to O(windows * groups) for aggregate EOWC queries
- Support tumbling, hopping, and session window assignment
- Keep raw-batch path for non-aggregate EOWC queries (projection-only)

## Non-Goals

- Full Ring 0 operator routing (deferred to F-SSQL-003)
- Checkpoint/restore of EOWC state (deferred to F-SSQL-002)
- Changes to Ring 0 window operators (already production-ready)
- Non-EOWC streaming aggregations (already fixed by Gap 1 / `IncrementalAggState`)

## Technical Design

### Architecture

**Ring**: Ring 1 (SQL execution path)
**Crate**: `laminar-db`
**Key insight**: Reuse `IncrementalAggState`'s aggregate detection and accumulator creation logic, but organize state per-window rather than globally.

### Data Structures

```rust
/// Per-window accumulator state for EOWC queries.
/// Keyed by window start timestamp (ms since epoch).
pub(crate) struct IncrementalEowcState {
    /// Window type determines assignment logic.
    window_type: EowcWindowType,
    /// Per-window aggregate state: window_start → per-group accumulators.
    windows: BTreeMap<i64, WindowAccumGroup>,
    /// Aggregate function specs (extracted once from the query plan).
    agg_specs: Vec<AggFuncSpec>,
    /// Group-by column indices in the pre-aggregation output.
    group_col_indices: Vec<usize>,
    /// Pre-aggregation SQL for expression evaluation.
    pre_agg_sql: String,
    /// Output schema for emitted batches.
    output_schema: SchemaRef,
}

/// Aggregate state for all groups within a single window instance.
struct WindowAccumGroup {
    /// group_key → one Accumulator per aggregate function.
    groups: HashMap<Vec<ScalarValue>, Vec<Box<dyn Accumulator>>>,
}

/// Window type with parameters for window assignment.
enum EowcWindowType {
    /// Tumbling: non-overlapping, fixed-size windows.
    Tumbling { size_ms: i64 },
    /// Hopping: overlapping windows with fixed size and slide.
    Hopping { size_ms: i64, slide_ms: i64 },
    /// Session: gap-based windows that merge on activity.
    Session { gap_ms: i64 },
}
```

### Algorithm/Flow

**Per-cycle update** (`update_batch`):

1. Run `pre_agg_sql` via DataFusion to evaluate expressions (e.g., `price * quantity`)
2. For each row in the result:
   a. Extract event timestamp from the configured time column
   b. Assign window(s) using `EowcWindowType`:
      - Tumbling: `window_start = floor(ts / size) * size`
      - Hopping: multiple windows `[ts - size + slide, ..., ts]` aligned to slide
      - Session: find or create window, merge adjacent if gap < threshold
   c. Extract group key from group-by columns
   d. Look up or create `Accumulator` instances in the target window's `WindowAccumGroup`
   e. Call `accumulator.update_batch()` with the row's aggregate input values
3. Raw source batches are NOT stored (only accumulator state persists)

**On window close** (watermark passes `window_end`):

1. Iterate over `windows` entries where `window_start + size <= watermark`
2. For each closing window:
   a. For each group: call `accumulator.evaluate()` to get final values
   b. Build a `RecordBatch` with group keys + aggregate results
   c. Remove the window entry from the `BTreeMap`
3. Return the emitted batches

**Non-aggregate fallback**:

If the query has no aggregations (projection/filter only), fall back to the existing raw-batch accumulation path (no accumulators needed).

### API/Interface

```rust
impl IncrementalEowcState {
    /// Create from a SQL query, detecting aggregation and window type.
    pub fn try_from_query(
        sql: &str,
        ctx: &SessionContext,
        window_type: EowcWindowType,
        time_column: &str,
    ) -> Result<Option<Self>, DbError>;

    /// Update accumulators with a new micro-batch.
    pub fn update_batch(&mut self, batch: &RecordBatch, ctx: &SessionContext) -> Result<(), DbError>;

    /// Close windows whose end <= watermark, returning emitted batches.
    pub fn close_windows(&mut self, watermark_ms: i64) -> Result<Vec<RecordBatch>, DbError>;

    /// Return the number of open windows (for observability).
    pub fn open_window_count(&self) -> usize;
}
```

### Window Assignment

```rust
/// Assign a timestamp to one or more window starts.
fn assign_windows(ts_ms: i64, window_type: &EowcWindowType) -> Vec<i64> {
    match window_type {
        EowcWindowType::Tumbling { size_ms } => {
            vec![ts_ms / size_ms * size_ms]
        }
        EowcWindowType::Hopping { size_ms, slide_ms } => {
            let mut windows = Vec::new();
            let last_start = ts_ms / slide_ms * slide_ms;
            let mut start = last_start;
            while start + size_ms > ts_ms {
                windows.push(start);
                start -= slide_ms;
                if start < 0 { break; }
            }
            windows
        }
        EowcWindowType::Session { .. } => {
            // Session windows are handled by merge logic, not assignment.
            // Initial window: [ts, ts + gap)
            vec![ts_ms]
        }
    }
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `DbError::InvalidQuery` | EOWC query has no window specification | Return error at `add_query` time |
| `DbError::Execution` | Accumulator update fails (type mismatch) | Propagate to caller; log and skip batch |
| `DbError::Internal` | Window assignment produces negative timestamp | Clamp to 0, log warning |

## Test Plan

### Unit Tests

- [ ] `test_tumbling_window_assignment_aligns_to_boundary`
- [ ] `test_hopping_window_assignment_returns_multiple_windows`
- [ ] `test_session_window_merge_on_close_gap`
- [ ] `test_incremental_eowc_sum_matches_full_recompute`
- [ ] `test_incremental_eowc_avg_multi_group`
- [ ] `test_window_close_emits_correct_schema`
- [ ] `test_non_aggregate_eowc_falls_back_to_raw_batches`
- [ ] `test_empty_batch_update_is_noop`
- [ ] `test_late_data_after_window_close_is_dropped`

### Integration Tests

- [ ] End-to-end: tumbling window aggregation via SQL matches Ring 0 operator output
- [ ] End-to-end: hopping window with 3 overlapping windows produces correct results
- [ ] Memory: 1M events in 1-hour window uses O(groups) memory, not O(events)

### Benchmarks

- [ ] `bench_eowc_incremental_vs_raw_recompute` - Target: 10x improvement at 100K events
- [ ] `bench_window_close_latency` - Target: < 1ms for 10K groups

## Rollout Plan

1. **Phase 1**: `IncrementalEowcState` struct + tumbling window support + unit tests
2. **Phase 2**: Hopping window support + integration tests
3. **Phase 3**: Session window merge logic + benchmarks
4. **Phase 4**: Wire into `StreamExecutor::execute_eowc_query()`, keeping raw-batch fallback
5. **Phase 5**: Code review + merge

## Open Questions

- [ ] Should session window merge use the same logic as `SessionWindowOperator` in Ring 0, or a simplified version?
- [ ] Should we track window metadata (event count, last update time) for observability?
- [ ] How should hopping windows handle the case where `size_ms` is not evenly divisible by `slide_ms`?

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

- The existing `IncrementalAggState` (Gap 1 fix) handles non-windowed aggregations. This feature handles the windowed EOWC case using the same `AggFuncSpec` pattern but with per-window state partitioning.
- The `BTreeMap<i64, WindowAccumGroup>` ordering by window start enables efficient cleanup: iterate from the beginning until `window_start + size > watermark`.
- Session window merging is the most complex part: when two session windows' gaps overlap, their accumulator states must be merged via `Accumulator::merge_batch(state_a, state_b)`.

## References

- [Gap Analysis: Stateful Streaming Execution](../../../GAP-ANALYSIS-STATEFUL-STREAMING.md)
- [F075: DataFusion Aggregate Bridge](../../phase-2/F075-datafusion-aggregate-bridge.md)
- [stream_executor.rs — EowcState](../../../../crates/laminar-db/src/stream_executor.rs)
- [aggregate_state.rs — IncrementalAggState](../../../../crates/laminar-db/src/aggregate_state.rs)
