# laminar-sql

SQL layer for LaminarDB with streaming extensions.

## Overview

Extends standard SQL (via sqlparser-rs) with streaming constructs like tumbling windows, session windows, watermarks, EMIT clauses, and ASOF joins. Integrates with Apache DataFusion for query planning and execution.

## Key Modules

| Module | Purpose |
|--------|---------|
| `parser` | Streaming SQL parser: windows, emit, late data, joins, aggregation, analytics, ranking |
| `planner` | `StreamingPlanner` converts parsed SQL into `StreamingPlan` / `QueryPlan` |
| `translator` | Operator config builders: window, join, analytic, order, having, DDL |
| `datafusion` | DataFusion integration: custom UDFs, aggregate bridge, `execute_streaming_sql` |

## Streaming SQL Extensions

```sql
-- Tumbling windows
SELECT ... FROM source GROUP BY tumble(ts, INTERVAL '1' MINUTE)

-- Sliding windows
SELECT ... FROM source GROUP BY slide(ts, INTERVAL '5' MINUTE, INTERVAL '1' MINUTE)

-- Session windows
SELECT ... FROM source GROUP BY session(ts, INTERVAL '30' SECOND)

-- Watermarks
CREATE SOURCE events (..., WATERMARK FOR ts AS ts - INTERVAL '5' SECOND)

-- EMIT clause
SELECT ... EMIT ON WINDOW CLOSE

-- ASOF JOIN
SELECT ... FROM orders ASOF JOIN trades MATCH_CONDITION(o.ts >= t.ts) ON o.symbol = t.symbol

-- Late data handling
SELECT ... ALLOWED_LATENESS INTERVAL '10' SECOND
```

## Related Crates

- [`laminar-core`](../laminar-core) -- Operator implementations that execute the plans
- [`laminar-db`](../laminar-db) -- Database facade that orchestrates SQL execution
