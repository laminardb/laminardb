# F-SQL-005: Multi-Way JOIN Support

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SQL-005 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | M (3-4 days) |
| **Dependencies** | F019 (Stream-Stream Joins), F020 (Lookup Joins), F-SQL-001 (ASOF JOIN) |

## Summary

Extend the SQL join parser and planner to support queries with multiple JOINs in
a single statement (e.g., `A JOIN B ON ... JOIN C ON ...`). Currently the join
parser only analyzes `joins[0]` and silently ignores subsequent joins.

## Motivation

Production pipelines frequently join three or more streams/tables:

```sql
-- Enrich orders with market data AND customer reference
SELECT o.order_id, o.symbol, t.price as market_price,
       r.sector, r.exchange, o.price - t.price as slippage
FROM order_events o
ASOF JOIN market_ticks t MATCH_CONDITION(o.ts >= t.ts) ON o.symbol = t.symbol
JOIN reference_data r ON o.symbol = r.symbol;

-- Combine trades with quotes and instrument metadata
SELECT tr.*, q.bid, q.ask, i.lot_size
FROM trades tr
JOIN quotes q ON tr.symbol = q.symbol
    AND q.ts BETWEEN tr.ts - INTERVAL '1' SECOND AND tr.ts
JOIN instruments i ON tr.symbol = i.symbol;
```

Without multi-way joins, users must create intermediate materialized views for
each pair, adding latency and complexity.

## Current State

- `join_parser.rs:184` — `let join = &first_table.joins[0]` — hardcoded to first join
- `count_joins()` helper already counts total joins across all FROM tables
- `JoinAnalysis` holds a single join's info (left/right table, keys, type)
- `JoinOperatorConfig` is an enum with one variant per join type

## Design

### Parser Changes (`join_parser.rs`)

Replace single `JoinAnalysis` with `MultiJoinAnalysis`:

```rust
/// Analysis of all joins in a query.
pub struct MultiJoinAnalysis {
    /// Ordered list of join steps (left-to-right as written in SQL)
    pub joins: Vec<JoinStep>,
    /// All table/source names referenced
    pub tables: Vec<TableRef>,
}

/// A single join step in a multi-way join.
pub struct JoinStep {
    /// Left input: either a base table name or the output of a previous join
    pub left: JoinInput,
    /// Right table being joined
    pub right_table: String,
    pub right_alias: Option<String>,
    /// Join analysis for this step
    pub join_type: JoinType,
    pub left_key: String,
    pub right_key: String,
    pub time_bound: Option<TimeBound>,
    pub is_lookup_join: bool,
    /// ASOF-specific fields
    pub asof_direction: Option<AsofSqlDirection>,
    pub asof_tolerance: Option<i64>,
}

pub enum JoinInput {
    /// Direct table reference
    Table(String),
    /// Output of a previous join step (index into joins vec)
    PreviousJoin(usize),
}
```

Iterate over `first_table.joins` (all of them, not just `[0]`) and build a
`JoinStep` for each. The left input of join N is the output of join N-1 (or
the base table for join 0).

### Planner Changes (`planner/mod.rs`)

Add `multi_join_config: Option<MultiJoinConfig>` to `QueryPlan`:

```rust
pub struct MultiJoinConfig {
    /// Ordered join steps to execute
    pub steps: Vec<JoinStepConfig>,
}

pub struct JoinStepConfig {
    /// Existing JoinOperatorConfig for this step
    pub config: JoinOperatorConfig,
    /// Output schema after this join step
    pub output_schema: SchemaRef,
}
```

For single joins, continue using the existing `join_config` field. For
multi-joins (2+), use `multi_join_config`.

### Translator Changes (`join_translator.rs`)

Add `MultiJoinConfig::from_analysis(multi: &MultiJoinAnalysis)`:

- For each `JoinStep`, create the appropriate `JoinOperatorConfig` (Stream,
  Lookup, or ASOF)
- Compute intermediate output schemas by merging left + right columns
- Handle column name conflicts with table-qualified prefixes

### Executor Changes

Execute join steps sequentially in the DAG:

```
Source A ──┐
           ├── JOIN Step 1 (A ⟷ B) ──┐
Source B ──┘                          ├── JOIN Step 2 (AB ⟷ C) ──► Output
Source C ─────────────────────────────┘
```

For the streaming executor in `start_connector_pipeline()`:
1. Process join step 1: A JOIN B → intermediate RecordBatch AB
2. Process join step 2: AB JOIN C → final RecordBatch ABC
3. Each step uses the appropriate join operator (stream-stream, lookup, ASOF)

For DAG-based execution:
- Create intermediate DAG nodes for each join step
- Wire edges: source nodes → join node 1 → join node 2 → ... → output

### Join Type Mixing

Support heterogeneous join types in a single query:

```sql
-- ASOF + Lookup in one query
FROM orders o
ASOF JOIN ticks t MATCH_CONDITION(o.ts >= t.ts) ON o.symbol = t.symbol
JOIN reference r ON o.symbol = r.symbol;
```

Step 1: ASOF JOIN (orders ⟷ ticks) → enriched_orders
Step 2: Lookup JOIN (enriched_orders ⟷ reference) → final output

### Streaming Semantics

- Each join step operates independently on its inputs
- Watermarks propagate through join steps (min of input watermarks)
- Checkpointing captures state of each join operator independently
- Time bounds in stream-stream joins apply per-step

### Limitations (Out of Scope)

- Join reordering optimization (always execute left-to-right as written)
- Bushy join trees (only left-deep plans: `((A JOIN B) JOIN C) JOIN D`)
- Self-joins (same table joined to itself) — defer to future work

## Test Plan

| Module | Tests | What |
|--------|-------|------|
| `join_parser` | 8 | Two-way, three-way, mixed types (ASOF + lookup), alias handling |
| `join_translator` | 5 | MultiJoinConfig creation, schema merging, column conflicts |
| `planner` | 4 | Multi-join query plans, single join backward compat |
| `executor` | 6 | Two-way execution, three-way, mixed ASOF + lookup, watermark propagation |

## Files

- `crates/laminar-sql/src/parser/join_parser.rs` — MultiJoinAnalysis, iterate all joins
- `crates/laminar-sql/src/translator/join_translator.rs` — MultiJoinConfig
- `crates/laminar-sql/src/translator/mod.rs` — Re-export
- `crates/laminar-sql/src/planner/mod.rs` — Wire multi-join into QueryPlan
- `crates/laminar-core/src/dag/builder.rs` — Multi-join DAG node creation
- `crates/laminar-db/src/db.rs` — Sequential join execution in pipeline
