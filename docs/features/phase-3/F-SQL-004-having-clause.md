# F-SQL-004: HAVING Clause Execution

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SQL-004 |
| **Status** | ✅ Done |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | M (2-3 days) |
| **Dependencies** | F077 (Extended Aggregation Parser) |

## Summary

Wire the already-parsed HAVING clause through the planner and into the streaming
executor so that `GROUP BY ... HAVING <predicate>` filters aggregation results
before emission. The parser already detects HAVING (`has_having: bool` in
`AggregationAnalysis`), but the planner and executor ignore it.

## Motivation

HAVING is standard SQL for filtering grouped results:

```sql
-- Anomaly detection: only emit high-volume windows
SELECT symbol, SUM(volume) as total_volume, COUNT(*) as trade_count
FROM market_ticks
GROUP BY symbol, tumble(ts, INTERVAL '5' SECOND)
HAVING SUM(volume) > 10000;

-- Active symbols: only emit if enough trades
SELECT symbol, AVG(price) as avg_price
FROM market_ticks
GROUP BY symbol, tumble(ts, INTERVAL '5' SECOND)
HAVING COUNT(*) >= 50;
```

Without HAVING, users must create cascading materialized views (aggregate in
inner MV, filter with WHERE in outer MV), which adds latency, memory, and
complexity.

## Current State

- **Parser**: `aggregation_parser.rs` sets `analysis.has_having = true` when
  `select.having.is_some()` (line 343). The `having` expression is available
  via `select.having` from sqlparser-rs AST.
- **Planner**: `QueryPlan` has no `having_predicate` field. The HAVING expression
  is silently dropped.
- **Executor**: No filtering step after window aggregation emission.

## Design

### Parser Changes (`aggregation_parser.rs`)

Extract the HAVING expression and store it alongside the aggregation analysis:

```rust
pub struct AggregationAnalysis {
    pub aggregates: Vec<AggregateInfo>,
    pub group_by_columns: Vec<String>,
    pub has_having: bool,
    pub having_expr: Option<String>,  // NEW: serialized SQL expression
}
```

Parse the `select.having` AST node into a string representation that DataFusion
can evaluate. Handle aggregate references (`SUM(volume)`, `COUNT(*)`) by mapping
them to the output column aliases.

### Planner Changes (`planner/mod.rs`)

Add `having_predicate: Option<HavingPredicate>` to `QueryPlan`:

```rust
pub struct HavingPredicate {
    /// SQL expression string for DataFusion evaluation
    pub expression: String,
    /// Column references used (for validation)
    pub referenced_columns: Vec<String>,
}
```

Wire the HAVING expression from `AggregationAnalysis` into the query plan during
`plan_standard_statement()` and `analyze_query()`.

### Translator Changes

Create `having_translator.rs`:

```rust
pub struct HavingFilterConfig {
    /// DataFusion-compatible filter expression
    pub predicate: String,
    /// Output schema of the preceding aggregation
    pub input_schema: SchemaRef,
}
```

### Executor Changes

After window aggregation emits a `RecordBatch`, apply the HAVING filter:

1. Parse the HAVING expression into a DataFusion `Expr`
2. Create a `PhysicalExpr` from the logical expression
3. Evaluate the predicate against the aggregated batch
4. Filter rows where predicate is false
5. Emit only matching rows

This is equivalent to a post-aggregation `WHERE` but applied at the operator
output stage, not the input stage.

### Streaming Semantics

- HAVING is evaluated **after** window closure (or after each EMIT cycle)
- Filtered-out windows produce no output (not retracted)
- With `EMIT CHANGES`, HAVING applies to each changelog entry independently
- With `EMIT FINAL`, HAVING applies once to the final aggregation result

### Edge Cases

- HAVING with non-aggregate columns: reject at parse time (SQL standard)
- HAVING referencing columns not in SELECT: allowed (SQL standard)
- HAVING with window functions: not supported (reject at parse time)
- HAVING on non-grouped query: reject at parse time

## Test Plan

| Module | Tests | What |
|--------|-------|------|
| `aggregation_parser` | 5 | HAVING expression extraction, column references, complex predicates |
| `having_translator` | 3 | Config conversion, schema validation |
| `planner` | 4 | HAVING in query plan, validation errors, with/without GROUP BY |
| `executor` | 6 | Filter evaluation, all-pass, all-reject, mixed, with EMIT modes |

## Files

- `crates/laminar-sql/src/parser/aggregation_parser.rs` — Extract HAVING expr
- `crates/laminar-sql/src/translator/having_translator.rs` — NEW: HavingFilterConfig
- `crates/laminar-sql/src/translator/mod.rs` — Re-export
- `crates/laminar-sql/src/planner/mod.rs` — Wire into QueryPlan
- `crates/laminar-db/src/db.rs` — Apply filter in streaming executor
