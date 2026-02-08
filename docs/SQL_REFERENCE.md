# SQL Dialect Reference

> Practical guide to LaminarDB's SQL dialect — gotchas, working patterns, and tested examples.

LaminarDB uses [Apache DataFusion](https://datafusion.apache.org/) as its SQL engine. While most standard SQL works, streaming-specific operations have differences from other streaming databases (Flink SQL, ksqlDB, etc.). This reference documents patterns that are **confirmed working** in LaminarDB embedded mode.

---

## Quick Reference

| What you might try | What actually works | Notes |
|---|---|---|
| `TUMBLE_START(ts, ...)` | `CAST(tumble(ts, ...) AS BIGINT)` | DataFusion doesn't have `TUMBLE_START()` |
| `FIRST(price)` / `LAST(price)` | `first_value(price)` / `last_value(price)` | DataFusion aggregate function names |
| `ts - INTERVAL '10' SECOND` (on BIGINT) | `ts - 10000` | INTERVAL only works on TIMESTAMP types |
| `CASE WHEN ... THEN vol ELSE 0` | `CASE WHEN ... THEN vol ELSE CAST(0 AS BIGINT)` | ELSE branch must match column type |

---

## Sources

Create data sources using `CREATE SOURCE`. All timestamp columns should be BIGINT (milliseconds since epoch) and annotated with `#[event_time]` in Rust structs.

```sql
CREATE SOURCE trades (
    account_id VARCHAR NOT NULL,
    symbol     VARCHAR NOT NULL,
    side       VARCHAR NOT NULL,
    price      DOUBLE NOT NULL,
    volume     BIGINT NOT NULL,
    order_ref  VARCHAR NOT NULL,
    ts         BIGINT NOT NULL
)
```

**Rust side:**
```rust
#[derive(Record)]
pub struct Trade {
    pub account_id: String,
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub volume: i64,
    pub order_ref: String,
    #[event_time]
    pub ts: i64,
}
```

---

## Window Types

### TUMBLE (Fixed windows)

Non-overlapping windows of fixed size. Every event belongs to exactly one window.

```sql
CREATE STREAM ohlc AS
SELECT symbol,
       CAST(tumble(ts, INTERVAL '5' SECOND) AS BIGINT) AS window_start,
       first_value(price) AS open,
       MAX(price) AS high,
       MIN(price) AS low,
       last_value(price) AS close,
       SUM(volume) AS volume,
       COUNT(*) AS trade_count
FROM trades
GROUP BY symbol, tumble(ts, INTERVAL '5' SECOND)
```

**Key points:**
- Use lowercase `tumble()` (not `TUMBLE()` — both work, but lowercase is canonical)
- Extract window start with `CAST(tumble(...) AS BIGINT)` — there is no `TUMBLE_START()` function
- Window closes when watermark passes window end

### HOP (Sliding windows)

Overlapping windows: each event appears in multiple windows. Useful for smoothing/baselines.

```sql
CREATE STREAM vol_baseline AS
SELECT symbol,
       SUM(volume) AS total_volume,
       COUNT(*) AS trade_count,
       AVG(price) AS avg_price
FROM trades
GROUP BY symbol, HOP(ts, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
```

**Key points:**
- First interval = slide, second interval = window size
- Each event appears in `size / slide` windows (here: 10/2 = 5)
- More output rows than TUMBLE — plan downstream capacity accordingly

### SESSION (Gap-based windows)

Windows that close after a period of inactivity. Groups bursts of events.

```sql
CREATE STREAM rapid_fire AS
SELECT account_id,
       COUNT(*) AS burst_trades,
       SUM(volume) AS burst_volume,
       MIN(price) AS low,
       MAX(price) AS high
FROM trades
GROUP BY account_id, SESSION(ts, INTERVAL '2' SECOND)
```

**Key points:**
- Session closes when watermark passes `last_event_ts + gap_duration`
- Useful for detecting bursts/spikes in per-entity activity
- Each entity (GROUP BY key) has independent sessions

---

## Joins

### INNER JOIN (Time-bounded)

Join two sources within a time window. Both sources must have compatible timestamps.

```sql
CREATE STREAM suspicious_match AS
SELECT t.symbol,
       t.price AS trade_price,
       t.volume,
       o.order_id,
       o.account_id,
       o.side,
       o.price AS order_price,
       t.price - o.price AS price_diff
FROM trades t
INNER JOIN orders o
ON t.symbol = o.symbol
AND o.ts BETWEEN t.ts - 10000 AND t.ts + 10000
```

**Key points:**
- Use numeric arithmetic for BIGINT timestamps: `t.ts - 10000` (not `INTERVAL '10' SECOND`)
- Both sources need watermarks advanced for the join to emit
- Column aliases (`AS trade_price`) are required when both sources have columns with the same name

### ASOF JOIN

Match each row from the left source with the closest preceding row from the right source.

```sql
CREATE STREAM enriched AS
SELECT t.symbol,
       t.price,
       r.reference_price
FROM trades t
ASOF JOIN reference r
ON t.symbol = r.symbol AND t.ts >= r.ts
```

**Key points:**
- Right source must have events preceding left source events
- Useful for enrichment (e.g., matching trades to latest reference data)

---

## Aggregation Patterns

### CASE WHEN inside aggregates

Split aggregations by condition within a single stream:

```sql
CREATE STREAM wash_score AS
SELECT account_id,
       symbol,
       SUM(CASE WHEN side = 'buy' THEN volume ELSE CAST(0 AS BIGINT) END) AS buy_volume,
       SUM(CASE WHEN side = 'sell' THEN volume ELSE CAST(0 AS BIGINT) END) AS sell_volume,
       SUM(CASE WHEN side = 'buy' THEN 1 ELSE 0 END) AS buy_count,
       SUM(CASE WHEN side = 'sell' THEN 1 ELSE 0 END) AS sell_count
FROM trades
GROUP BY account_id, symbol, TUMBLE(ts, INTERVAL '5' SECOND)
```

**Key point:** The `ELSE` branch must match the column type. Use `CAST(0 AS BIGINT)` when summing BIGINT columns — `ELSE 0` alone produces INT32, causing a type mismatch.

### Computed columns

Arithmetic in SELECT works inline:

```sql
SELECT symbol,
       MAX(price) - MIN(price) AS price_range,
       AVG(price) * COUNT(*) AS notional
FROM trades
GROUP BY symbol, tumble(ts, INTERVAL '5' SECOND)
```

### Available aggregate functions

| Function | Notes |
|----------|-------|
| `COUNT(*)` | Row count |
| `SUM(col)` | Sum (respects type) |
| `AVG(col)` | Average (returns DOUBLE) |
| `MIN(col)` / `MAX(col)` | Min/max |
| `first_value(col)` | First value in window (NOT `FIRST()`) |
| `last_value(col)` | Last value in window (NOT `LAST()`) |

---

## Sinks and Subscriptions

After creating a stream, create a sink and subscribe to get results in Rust:

```sql
CREATE SINK ohlc_sink FROM ohlc
```

```rust
// FromRow struct fields must match SELECT column order exactly
#[derive(FromRow)]
pub struct OhlcRow {
    pub symbol: String,      // 1st SELECT column
    pub window_start: i64,   // 2nd SELECT column
    pub open: f64,           // 3rd SELECT column
    // ... etc
}

let sub = db.subscribe::<OhlcRow>("ohlc")?;

// Poll for results (non-blocking)
while let Some(rows) = sub.poll() {
    for row in &rows {
        println!("{}: O={} H={} L={} C={}", row.symbol, row.open, row.high, row.low, row.close);
    }
}
```

**Critical:** `FromRow` struct field order must match the SQL `SELECT` column order. Field names don't matter — only position.

---

## Watermarks

Watermarks tell the engine "no more data before this time." Windows emit when the watermark passes the window boundary.

```rust
// Advance watermark on all sources
source.watermark(current_ts + 10_000);  // 10s ahead covers HOP(10s) windows
```

**Key points:**
- Advance watermark on **all** sources (both sides of a join)
- Watermark should be at least `current_ts + largest_window_size`
- HOP(10s) needs watermark 10s ahead; SESSION(2s) needs 2s past last event
- LaminarDB processes in 100ms micro-batch ticks — results appear after next tick

---

## Common Gotchas

### 1. BIGINT timestamps + INTERVAL don't mix

```sql
-- FAILS: INTERVAL on BIGINT column
WHERE o.ts BETWEEN t.ts - INTERVAL '10' SECOND AND t.ts + INTERVAL '10' SECOND

-- WORKS: numeric arithmetic (BIGINT milliseconds)
WHERE o.ts BETWEEN t.ts - 10000 AND t.ts + 10000
```

### 2. Type mismatch in CASE WHEN

```sql
-- FAILS: ELSE 0 is INT32, volume is BIGINT
SUM(CASE WHEN side = 'buy' THEN volume ELSE 0 END)

-- WORKS: explicit cast
SUM(CASE WHEN side = 'buy' THEN volume ELSE CAST(0 AS BIGINT) END)
```

### 3. Window function names

```sql
-- FAILS
SELECT TUMBLE_START(ts, INTERVAL '5' SECOND), FIRST(price), LAST(price)

-- WORKS
SELECT CAST(tumble(ts, INTERVAL '5' SECOND) AS BIGINT), first_value(price), last_value(price)
```

### 4. FromRow field ordering

```rust
// SQL: SELECT symbol, SUM(volume) AS total, AVG(price) AS avg_price

// WRONG — field names don't matter, ORDER matters
#[derive(FromRow)]
pub struct Bad {
    pub avg_price: f64,  // This gets the 1st column (symbol), not avg_price!
    pub symbol: String,
    pub total: i64,
}

// CORRECT — matches SELECT column order
#[derive(FromRow)]
pub struct Good {
    pub symbol: String,    // 1st column
    pub total: i64,        // 2nd column
    pub avg_price: f64,    // 3rd column
}
```

### 5. Both sources need watermarks for joins

```rust
// WRONG — join will never emit because orders watermark isn't advancing
trade_source.watermark(ts + 10_000);

// CORRECT — advance both
trade_source.watermark(ts + 10_000);
order_source.watermark(ts + 10_000);
```

---

## Tested Combinations

The following patterns are confirmed working in LaminarDB embedded mode (tested in [laminardb-test](https://github.com/laminardb/laminardb-test) and [laminardb-fraud-detect](https://github.com/laminardb/laminardb-fraud-detect)):

| Pattern | Example |
|---------|---------|
| TUMBLE + multiple aggregates | SUM, COUNT, AVG, MIN, MAX, first_value, last_value |
| TUMBLE + CASE WHEN in SUM | Buy/sell volume split |
| TUMBLE + computed columns | MAX(price) - MIN(price) |
| HOP + aggregates | Rolling volume baselines |
| SESSION + aggregates | Burst detection |
| INNER JOIN + time window | Trade-order correlation |
| ASOF JOIN | Reference data enrichment |
| Cascading materialized views | Stream A -> Stream B -> Stream C |
| 5+ concurrent streams, 2 sources | Single LaminarDB instance, sub-ms latency |
| Multiple GROUP BY columns | account_id + symbol + window |

---

## Type Mapping

| SQL Type | Rust Type | Notes |
|----------|-----------|-------|
| `VARCHAR` | `String` | |
| `BIGINT` | `i64` | Use for timestamps, volumes, counts |
| `DOUBLE` | `f64` | Use for prices, averages |
| `INT` / `INTEGER` | `i32` | Avoid mixing with BIGINT in CASE WHEN |
| `BOOLEAN` | `bool` | |

---

*This reference is based on LaminarDB v0.1.x with DataFusion 57.x. Contributions and corrections welcome — please open an issue or PR.*
