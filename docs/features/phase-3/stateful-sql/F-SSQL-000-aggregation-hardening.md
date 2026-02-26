# F-SSQL-000: Streaming Aggregation Hardening

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SSQL-000 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | L |
| **Dependencies** | Gap 1 fix (done) |
| **Owner** | TBD |
| **Created** | 2026-02-25 |
| **Updated** | 2026-02-25 |

## Summary

Production-readiness hardening for the existing streaming aggregation code in `IncrementalAggState` (Gap 1 fix), `DataFusionAccumulatorAdapter` (F075 aggregate bridge), and the EOWC execution path in `StreamExecutor`. A comprehensive audit identified 22 gaps across wrong-result bugs (P0), crash/data-loss paths (P1), and resource management issues (P2). This feature tracks fixing all of them as a prerequisite for the rest of the Stateful Streaming SQL features (F-SSQL-001 through F-SSQL-006).

## Goals

- Fix all P0 wrong-result bugs in the streaming aggregation path
- Eliminate all `.expect()` / `.unwrap()` panics on non-guaranteed conditions
- Add resource bounds (max groups, max EOWC accumulation)
- Properly handle DISTINCT, FILTER, and HAVING SQL clauses
- Replace fragile string-slicing SQL extraction with AST-based parsing
- Log all currently-silent error paths

## Non-Goals

- Replacing the aggregation architecture (that's F-SSQL-003's job)
- Performance optimization beyond fixing O(N) issues
- Adding new aggregate functions

## Audit Findings

### P0: Wrong Results

| ID | Issue | Location | Description |
|----|-------|----------|-------------|
| H-01 | DISTINCT silently ignored | `aggregate_state.rs:75` | `is_distinct: false` hardcoded. `COUNT(DISTINCT x)` counts duplicates, `SUM(DISTINCT x)` sums duplicates. Must read `agg_func.params.distinct` from the logical plan and pass to `AccumulatorArgs`. |
| H-02 | FILTER clause unhandled | `aggregate_state.rs:172-206` | `SUM(x) FILTER (WHERE x > 0)` treated as `SUM(x)`. Must check for filter expression in the aggregate function and apply via CASE WHEN in pre-agg SQL or post-filter in accumulator update. |
| H-03 | HAVING clause silently dropped | `aggregate_state.rs:228-233` | `SELECT dept, SUM(sal) FROM t GROUP BY dept HAVING SUM(sal) > 100000` — HAVING is not in pre-agg SQL (correct) but also not applied after accumulator evaluation in `emit()`. Groups that fail the HAVING predicate are emitted anyway. |
| H-04 | DISTINCT/ignore_nulls hardcoded in bridge | `aggregate_bridge.rs:315-319` | Same issue as H-01 but in the Ring 0 bridge path. `AccumulatorArgs` constructed with `is_distinct: false`, `ignore_nulls: false`. |

### P1: Crash / Data Loss

| ID | Issue | Location | Description |
|----|-------|----------|-------------|
| H-05 | `.expect()` in create_accumulator | `aggregate_state.rs:79-81` | `udf.accumulator(args).expect(...)` panics if accumulator creation fails (e.g., unsupported type combination). Should return `Result`. |
| H-06 | `.expect()` in process_batch take | `aggregate_state.rs:300` | `compute::take(...).expect(...)` panics on invalid indices. Should propagate error. |
| H-07 | `.unwrap()` in stream_executor | `stream_executor.rs:412,590,605,611` | Multiple `.unwrap()` calls on state lookups that are logically guaranteed but not structurally enforced. Should use `.ok_or_else()`. |
| H-08 | `clone_box()` panic in bridge | `aggregate_bridge.rs:189-195` | `DataFusionAccumulatorAdapter::clone_box()` panics unconditionally. If `CompositeAccumulator::clone()` includes a DataFusion adapter, the process crashes. |
| H-09 | Silent error in `add_event` | `aggregate_bridge.rs:158` | `let _ = self.inner.borrow_mut().update_batch(&columns)` discards errors. Failed accumulator updates produce wrong results with no warning. |
| H-10 | Silent error in `merge_batch` | `aggregate_bridge.rs:167-175` | `filter_map(ok())` drops ScalarValue conversion failures silently. `let _ = merge_batch()` discards merge errors. |
| H-11 | Force-emit data loss | `stream_executor.rs:727-733` | When `accumulated_rows > MAX_EOWC_ACCUMULATED_ROWS` but watermark hasn't advanced, force-emit uses `current_watermark` as cutoff, potentially emitting rows from open windows that should be buffered. |
| H-12 | `expr_to_sql` incomplete | `aggregate_state.rs:433-451` | Only handles 5 expression types. Complex expressions (ScalarFunction, CASE, EXTRACT, etc.) fall through to DataFusion's `.to_string()` debug format, producing invalid pre-agg SQL. |
| H-13 | Type inference backwards | `aggregate_state.rs:494-505` | `infer_input_type` only special-cases COUNT and AVG. For SUM, the output type (widened Int64) is used as the input type (original Int32), producing type mismatches. |
| H-14 | Fragile SQL string slicing | `aggregate_state.rs:455-488` | `extract_from_clause`/`extract_where_clause` use keyword search + string slicing. Fails for: keywords inside string literals/comments, multi-byte UTF-8 boundaries, complex JOINs with ON conditions containing stop keywords. |
| H-15 | GROUP BY expressions unsupported | `aggregate_state.rs:138-148,154-157` | `GROUP BY EXTRACT(HOUR FROM ts)` or `GROUP BY CASE WHEN ...` produces invalid pre-agg SQL. Group-by column names are quoted as identifiers (`"EXTRACT(ts)"`) instead of generated as expressions. |

### P2: Resource / Performance

| ID | Issue | Location | Description |
|----|-------|----------|-------------|
| H-16 | Unbounded group HashMap | `aggregate_state.rs:103` | `groups: HashMap<...>` grows without limit. `GROUP BY user_id` with millions of unique users exhausts memory. No max-groups policy, no eviction, no warning. |
| H-17 | Unbounded `last_emitted` in window ops | `window.rs:~3280` | `last_emitted: FxHashMap<WindowId, Event>` grows unbounded if windows don't close. Each entry contains a full Event (potentially MBs). Long-running queries can OOM. |
| H-18 | Sliding window overflow risk | `stream_executor.rs:990-991` | `(base / slide + 1) * slide` can overflow if slide is very small and base is large. Should use saturating arithmetic. |
| H-19 | Timestamp truncation in batch_filter | `batch_filter.rs:~75` | Converting `threshold_ms / 1000` truncates. For second-precision timestamps, this introduces off-by-one errors at window boundaries. |

### P3: Minor / Observability

| ID | Issue | Location | Description |
|----|-------|----------|-------------|
| H-20 | Cycle detection not logged | `stream_executor.rs:347-355` | Circular query dependencies silently fall back to insertion order. Should log a warning. |
| H-21 | Batch coalescing failures not logged | `stream_executor.rs:695-700` | `concat_batches()` failure silently keeps fragmented batches. Should log. |
| H-22 | Zero-size window accepted silently | `stream_executor.rs:968-970` | Zero or negative window size falls through without warning, disabling EOWC filtering. |

## Technical Design

### Phase 1: P0 Fixes (Wrong Results)

#### H-01/H-04: DISTINCT Support

```rust
// In aggregate_state.rs, extract distinct flag from logical plan:
if let datafusion_expr::Expr::AggregateFunction(agg_func) = expr {
    let is_distinct = agg_func.params.distinct;
    // ...
    let args = AccumulatorArgs {
        is_distinct,
        // ...
    };
}
```

For pre-aggregation: when `is_distinct` is true, deduplicate input rows per group before feeding to the accumulator.

#### H-02: FILTER Clause

```rust
// Wrap input expression with CASE WHEN:
if let Some(filter_expr) = &agg_func.params.filter {
    let filter_sql = expr_to_sql(filter_expr);
    let input_sql = expr_to_sql(arg_expr);
    pre_agg_select_items.push(format!(
        "CASE WHEN {filter_sql} THEN {input_sql} ELSE NULL END AS \"__agg_input_{col_idx}\""
    ));
}
```

#### H-03: HAVING Clause

Apply HAVING as a post-emission filter in `emit()`:

```rust
pub fn emit(&mut self) -> Result<Vec<RecordBatch>, DbError> {
    let batch = /* build batch from groups */;
    if let Some(having_predicate) = &self.having_predicate {
        // Evaluate HAVING predicate against the aggregate results
        // Filter rows where predicate is false
    }
    Ok(vec![batch])
}
```

### Phase 2: P1 Fixes (Crash Prevention)

#### H-05/H-06: Replace `.expect()` with `Result`

```rust
fn create_accumulator(&self) -> Result<Box<dyn Accumulator>, DbError> {
    self.udf.accumulator(args)
        .map_err(|e| DbError::Pipeline(format!("accumulator creation failed for {}: {e}", self.udf.name())))
}
```

#### H-08: Fix `clone_box()`

Either implement proper cloning by storing factory reference, or make `clone_box()` fallible:

```rust
fn clone_box(&self) -> Box<dyn DynAccumulator> {
    let new_inner = self.factory.create_df_accumulator();
    // Merge current state into new accumulator
    Box::new(DataFusionAccumulatorAdapter {
        inner: RefCell::new(new_inner),
        // ...
    })
}
```

#### H-14: AST-Based SQL Extraction

Replace `extract_from_clause` / `extract_where_clause` with proper sqlparser AST walking:

```rust
fn extract_from_clause_safe(sql: &str) -> Result<String, DbError> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql)
        .map_err(|e| DbError::Pipeline(format!("parse error: {e}")))?;
    // Walk AST to extract FROM tables and WHERE clause
}
```

#### H-15: GROUP BY Expression Support

Generate expressions in pre-agg SQL instead of quoting names:

```rust
for (i, group_expr) in group_exprs.iter().enumerate() {
    let group_sql = expr_to_sql(group_expr);
    pre_agg_select_items.push(format!("{group_sql} AS \"__group_{i}\""));
}
```

### Phase 3: P2 Fixes (Resource Management)

#### H-16: Group Cardinality Limit

```rust
pub(crate) struct IncrementalAggState {
    // ...
    max_groups: usize, // Default: 1_000_000
}

impl IncrementalAggState {
    fn process_batch(&mut self, batch: &RecordBatch) -> Result<(), DbError> {
        // ...
        if self.groups.len() >= self.max_groups && !self.groups.contains_key(&key) {
            tracing::warn!(
                "Group cardinality limit ({}) reached, dropping new group",
                self.max_groups
            );
            continue;
        }
        // ...
    }
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `DbError::Unsupported` | DISTINCT aggregate with complex expression | Return error to user |
| `DbError::Pipeline` | Accumulator creation/update fails | Log, skip batch, emit current state |
| `DbError::Pipeline` | Pre-agg SQL parse error | Fall back to standard DataFusion path |
| `DbError::Pipeline` | Group limit exceeded | Log warning, drop new groups |

## Test Plan

### Unit Tests

- [ ] `test_distinct_count_produces_correct_result`
- [ ] `test_distinct_sum_produces_correct_result`
- [ ] `test_filter_clause_applied_correctly`
- [ ] `test_having_clause_filters_output`
- [ ] `test_group_by_expression_extract_hour`
- [ ] `test_group_by_expression_case_when`
- [ ] `test_create_accumulator_returns_error_on_failure`
- [ ] `test_process_batch_propagates_take_error`
- [ ] `test_null_group_keys_grouped_correctly`
- [ ] `test_group_cardinality_limit_enforced`
- [ ] `test_force_emit_does_not_lose_open_window_data`
- [ ] `test_extract_from_clause_with_string_literal_containing_keyword`
- [ ] `test_extract_from_clause_with_complex_join`
- [ ] `test_expr_to_sql_scalar_function`
- [ ] `test_expr_to_sql_case_when`
- [ ] `test_type_inference_sum_int32_returns_int32_not_int64`
- [ ] `test_sliding_window_boundary_no_overflow`

### Integration Tests

- [ ] End-to-end: `SELECT COUNT(DISTINCT symbol) FROM trades GROUP BY exchange` produces correct result
- [ ] End-to-end: `SELECT SUM(amt) FILTER (WHERE status = 'ok') FROM orders GROUP BY dept` works
- [ ] End-to-end: query with 100K+ unique groups stays within memory bounds
- [ ] End-to-end: EOWC force-emit does not silently drop data

### Property Tests

- [ ] For any batch sequence, DISTINCT aggregate state equals batch-union + deduplicate + count
- [ ] Group cardinality never exceeds `max_groups + batch_size`

## Rollout Plan

1. **Phase 1**: P0 fixes (H-01 through H-04) — DISTINCT, FILTER, HAVING
2. **Phase 2**: P1 crash fixes (H-05 through H-11) — unwrap/expect elimination, error propagation
3. **Phase 3**: P1 correctness fixes (H-12 through H-15) — expr_to_sql, type inference, SQL parsing
4. **Phase 4**: P2 resource fixes (H-16 through H-19) — group limits, overflow prevention
5. **Phase 5**: P3 observability (H-20 through H-22) — logging

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

- NULL grouping semantics: DataFusion's `ScalarValue` derives `Hash`+`Eq` and treats `NULL == NULL`, which is correct for SQL `GROUP BY` (NULLs are grouped together). No fix needed.
- RefCell in `DataFusionAccumulatorAdapter`: safe in single-threaded Ring 1 context. The `unsafe impl Send` is acceptable given the architecture. Not a production issue.
- The `infer_input_type` heuristic is the root cause of subtle type mismatches. The proper fix is to extract input types from the logical plan's expression types, not reverse-engineer them from the output type.
- H-14 (string slicing) is the most impactful P1 fix — it affects all complex queries. The `sqlparser` crate is already a dependency in `stream_executor.rs`, so AST-based extraction has zero new dependencies.

## References

- [aggregate_state.rs](../../../../crates/laminar-db/src/aggregate_state.rs)
- [aggregate_bridge.rs](../../../../crates/laminar-sql/src/datafusion/aggregate_bridge.rs)
- [stream_executor.rs](../../../../crates/laminar-db/src/stream_executor.rs)
- [Gap Analysis](../../../GAP-ANALYSIS-STATEFUL-STREAMING.md)
