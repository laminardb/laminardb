# laminar-sql

SQL layer for LaminarDB with streaming extensions.

## Overview

Extends standard SQL (via sqlparser-rs) with streaming constructs like tumbling windows, session windows, watermarks, EMIT clauses, and ASOF joins. Integrates with Apache DataFusion for query planning and execution.

## Key Modules

| Module | Purpose |
|--------|---------|
| `parser` | Streaming SQL parser: windows, emit, late data, joins, aggregation, analytics, ranking, DDL (CREATE SOURCE/STREAM/SINK/LOOKUP TABLE) |
| `planner` | `StreamingPlanner` converts parsed SQL into `StreamingPlan` / `QueryPlan` |
| `translator` | Operator config builders: window, join, analytic, order, having, DDL, ASOF join |
| `datafusion` | DataFusion integration: custom UDFs (tumble, hop, session, slide, first_value, last_value), aggregate bridge, `execute_streaming_sql`, watermark filter pushdown, PROCTIME() UDF, JSON functions, complex type functions |
| `error` | User-friendly DataFusion error translation with `LDB-NNNN` codes |

## Streaming SQL Extensions

```sql
-- Tumbling windows
SELECT ... FROM source GROUP BY tumble(ts, INTERVAL '1' MINUTE)

-- Sliding windows
SELECT ... FROM source GROUP BY slide(ts, INTERVAL '5' MINUTE, INTERVAL '1' MINUTE)

-- Hopping windows
SELECT ... FROM source GROUP BY hop(ts, INTERVAL '10' SECOND, INTERVAL '5' SECOND)

-- Session windows
SELECT ... FROM source GROUP BY session(ts, INTERVAL '30' SECOND)

-- Watermarks
CREATE SOURCE events (..., WATERMARK FOR ts AS ts - INTERVAL '5' SECOND)

-- EMIT clause
SELECT ... EMIT ON WINDOW CLOSE
SELECT ... EMIT CHANGES
SELECT ... EMIT FINAL

-- ASOF JOIN
SELECT ... FROM orders ASOF JOIN trades ON o.symbol = t.symbol AND o.ts >= t.ts

-- Lookup tables
CREATE LOOKUP TABLE instruments FROM POSTGRES (...)

-- Late data handling
SELECT ... ALLOWED_LATENESS INTERVAL '10' SECOND

-- Window functions
SELECT ..., LAG(price, 1) OVER (PARTITION BY symbol ORDER BY ts) FROM trades
SELECT ..., ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY price DESC) FROM trades

-- Window frames
SELECT ..., SUM(vol) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)

-- Connector DDL
CREATE SOURCE ... FROM KAFKA (brokers = '...', topic = '...', format = 'json')
CREATE SOURCE ... FROM POSTGRES_CDC (hostname = '...', database = '...')
CREATE SOURCE ... FROM MYSQL_CDC (hostname = '...', database = '...')
CREATE SINK ... INTO KAFKA (brokers = '...', topic = '...')
CREATE SINK ... INTO DELTA_LAKE (path = '...')
```

## Custom UDFs Registered with DataFusion

| Function | Description |
|----------|-------------|
| `tumble(ts, interval)` | Tumbling window assignment |
| `tumble(ts, interval, offset)` | Tumbling window with timezone offset |
| `hop(ts, slide, size)` | Hopping/sliding window assignment |
| `hop(ts, slide, size, offset)` | Hopping window with timezone offset |
| `session(ts, gap)` | Session window assignment |
| `slide(ts, size, slide)` | Alias for hop |
| `first_value(col)` | First value in window (retractable) |
| `last_value(col)` | Last value in window (retractable) |
| `PROCTIME()` | Processing time function |

## Streaming Physical Optimizer

The `StreamingPhysicalValidator` rule catches invalid physical plans (e.g., SortExec + Final AggregateExec on unbounded inputs) before execution. Configurable via `StreamingValidatorMode`:

- **Reject** (default) -- fails with an error
- **Warn** -- logs a warning but allows execution
- **Off** -- disables validation

## Watermark Filter Pushdown

The `WatermarkDynamicFilter` pushes `ts >= watermark` predicates down to `StreamingScanExec` so late rows are dropped before expression evaluation. Uses shared `Arc<AtomicI64>` for zero-copy watermark updates from Ring 0.

## Public API

```rust
use laminar_sql::{parse_streaming_sql, StreamingPlanner, execute_streaming_sql};

// Parse streaming SQL
let statements = parse_streaming_sql("CREATE SOURCE trades (...)")?;

// Plan a query
let planner = StreamingPlanner::new();
let plan = planner.plan(&statement)?;

// Execute via DataFusion
let result = execute_streaming_sql(&ctx, sql).await?;
```

## Related Crates

- [`laminar-core`](../laminar-core) -- Operator implementations that execute the plans
- [`laminar-db`](../laminar-db) -- Database facade that orchestrates SQL execution
