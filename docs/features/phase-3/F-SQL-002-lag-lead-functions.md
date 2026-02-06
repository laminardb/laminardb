# F-SQL-002: LAG/LEAD Window Functions

> Status: ✅ Done
> Priority: P1
> Phase: 3

## Summary

Add LAG/LEAD analytic window function support to the LaminarDB SQL parser,
translator, and core operator, enabling per-row lookback and lookahead
computations partitioned by key and ordered by event time.

## Motivation

LAG and LEAD are essential for time-series analytics:
- **LAG**: Compare current price to previous price (momentum, returns)
- **LEAD**: Pre-compute next expected value (gap detection, prediction)
- **Financial**: OHLC candle construction, spread analysis, tick-by-tick delta
- **IoT**: Sensor drift detection, rate-of-change calculation

No existing LaminarDB path supports per-row analytic window functions. The
window infrastructure only handles GROUP BY aggregate windows (TUMBLE/HOP/SESSION).

## Syntax

```sql
-- LAG: look back 1 row (default offset)
SELECT price, LAG(price) OVER (PARTITION BY symbol ORDER BY ts) AS prev_price
FROM trades;

-- LAG with offset and default
SELECT LAG(price, 3, 0) OVER (ORDER BY ts) AS prev3 FROM trades;

-- LEAD: look ahead 1 row
SELECT LEAD(price) OVER (PARTITION BY symbol ORDER BY ts) AS next_price
FROM trades;

-- LEAD with offset and default
SELECT LEAD(price, 2, -1) OVER (ORDER BY ts) AS next2 FROM trades;

-- Multiple analytic functions
SELECT
  LAG(price) OVER (ORDER BY ts) AS prev,
  LEAD(price) OVER (ORDER BY ts) AS next
FROM trades;

-- FIRST_VALUE / LAST_VALUE
SELECT FIRST_VALUE(price) OVER (PARTITION BY symbol ORDER BY ts) AS first
FROM trades;
```

## Streaming Semantics

- **LAG**: Emit immediately. History buffer is always available.
- **LEAD**: Buffer current event, wait for `offset` future events. Flush
  remaining with default values on watermark advance.
- Memory: O(P * max(lag_offset, lead_offset)) where P = partitions.

## Design

### Parser (`analytic_parser.rs`)

- `AnalyticFunctionType` enum: Lag, Lead, FirstValue, LastValue, NthValue
- `AnalyticFunctionInfo`: function_type, column, offset, default_value, alias
- `AnalyticWindowAnalysis`: functions, partition_columns, order_columns
- `analyze_analytic_functions(stmt)`: Walk SELECT items for `Expr::Function`
  with `.over` clause, match against known analytic function names

### Translator (`analytic_translator.rs`)

- `AnalyticWindowConfig`: functions, partition_columns, order_columns, max_partitions
- `AnalyticFunctionConfig`: function_type, source_column, offset, default_value, output_alias
- `AnalyticWindowConfig::from_analysis()`: Convert parser analysis to operator config

### Operator (`lag_lead.rs`)

- `LagLeadConfig`: operator_id, functions, partition_columns, max_partitions
- `LagLeadFunctionSpec`: is_lag, source_column, offset, default_value, output_column
- `LagLeadOperator` implementing `Operator` trait
- Per-partition state with `VecDeque` for LAG history and LEAD pending queues
- Checkpoint/restore support for durability

### Planner Integration

- `QueryPlan.analytic_config: Option<AnalyticWindowConfig>`
- Wired into both `plan_standard_statement()` and `analyze_query()`
- Included in `has_streaming_features` check

## Test Plan

| Module | Tests | What |
|--------|-------|------|
| `analytic_parser` | 11 | LAG/LEAD/FIRST_VALUE/LAST_VALUE parsing, partitions, offsets, defaults |
| `analytic_translator` | 5 | Config conversion, max_partitions, lookahead detection |
| `lag_lead` | 13 | LAG lookback, LEAD buffering, watermark flush, partitions, checkpoint |
| `planner` | 2 | LAG and LEAD queries produce analytic_config in QueryPlan |

## References

- Flink `OVER` window functions
- Spark SQL `LAG`/`LEAD` window functions
- DataFusion window function support
