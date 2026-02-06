# SQL Parser Gaps Analysis

> **Purpose**: Document SQL features NOT currently implemented in LaminarDB that would be useful for the production demo.
> **Date**: 2026-02-06

## Summary

The LaminarDB SQL parser is **very comprehensive** for streaming use cases, with 30+ aggregate functions, all window types (TUMBLE/HOP/SESSION), and full EMIT clause support. However, several SQL features commonly used in timeseries analytics are missing.

---

## Implemented Features (Reference)

### Aggregate Functions (30+ types)
- **Core**: COUNT, COUNT(DISTINCT), SUM, MIN, MAX, AVG
- **Statistical**: STDDEV, STDDEV_POP, VARIANCE, VAR_POP, MEDIAN
- **Percentile**: PERCENTILE_CONT, PERCENTILE_DISC
- **Boolean**: BOOL_AND, BOOL_OR (aliases: EVERY, ANY)
- **Collection**: STRING_AGG, ARRAY_AGG (aliases: LISTAGG, GROUP_CONCAT)
- **Approximate**: APPROX_COUNT_DISTINCT, APPROX_PERCENTILE_CONT, APPROX_MEDIAN
- **Correlation**: COVAR_SAMP, COVAR_POP, CORR, REGR_SLOPE, REGR_INTERCEPT
- **Bit**: BIT_AND, BIT_OR, BIT_XOR
- **Streaming**: FIRST_VALUE, LAST_VALUE (as aggregates)
- **Advanced**: FILTER clause, WITHIN GROUP, DISTINCT support

### Window Types
- **TUMBLE(time_col, interval)** - Fixed non-overlapping
- **HOP(time_col, slide, size)** / **SLIDE()** - Overlapping
- **SESSION(time_col, gap)** - Gap-based sessions

### EMIT Strategies
- `EMIT AFTER WATERMARK` / `EMIT ON WATERMARK`
- `EMIT ON WINDOW CLOSE`
- `EMIT ON UPDATE`
- `EMIT EVERY INTERVAL 'N' UNIT`
- `EMIT CHANGES` (changelog/Z-set mode)
- `EMIT FINAL`

### Join Types
- INNER, LEFT, RIGHT, FULL
- ASOF (Backward/Forward/Nearest with tolerance)
- Stream-stream with time bounds
- Lookup with TTL
- Temporal joins

---

## Missing Features (Gaps)

### 1. Ranking Window Functions

**Status**: NOT IMPLEMENTED

**Missing Functions**:
- `ROW_NUMBER() OVER (...)`
- `RANK() OVER (...)`
- `DENSE_RANK() OVER (...)`
- `PERCENT_RANK() OVER (...)`
- `NTILE(n) OVER (...)`

**Use Cases**:
```sql
-- Top 3 price movers per window (CANNOT DO)
SELECT * FROM (
    SELECT symbol, pct_change,
           ROW_NUMBER() OVER (ORDER BY ABS(pct_change) DESC) as rank
    FROM price_changes
) WHERE rank <= 3;

-- Percentile ranking of volumes (CANNOT DO)
SELECT symbol, volume,
       PERCENT_RANK() OVER (ORDER BY volume) as volume_percentile
FROM trades;
```

**Workaround**: Compute rankings in application layer after receiving window results.

**Implementation Notes**:
- Core operator exists (`PartitionedTopKOperator`) for ROW_NUMBER pattern
- SQL parser doesn't recognize these as window functions
- Need to add to `aggregation_parser.rs` and create operator bridge

**Effort**: Medium (2-3 days)

---

### 2. Analytic Window Functions (LAG/LEAD)

**Status**: NOT IMPLEMENTED

**Missing Functions**:
- `LAG(column, offset, default) OVER (...)`
- `LEAD(column, offset, default) OVER (...)`
- `FIRST_VALUE(column) OVER (...)` (as window function, not aggregate)
- `LAST_VALUE(column) OVER (...)` (as window function, not aggregate)
- `NTH_VALUE(column, n) OVER (...)`

**Use Cases**:
```sql
-- Price change from previous tick (CANNOT DO)
SELECT symbol, price,
       price - LAG(price, 1) OVER (PARTITION BY symbol ORDER BY ts) as price_delta,
       (price - LAG(price, 1) OVER (...)) / LAG(price, 1) OVER (...) * 100 as pct_change
FROM market_ticks;

-- Detect price gaps (CANNOT DO)
SELECT symbol, price, prev_price,
       CASE WHEN ABS(price - prev_price) / prev_price > 0.05
            THEN 'GAP' ELSE 'NORMAL' END as gap_type
FROM (
    SELECT symbol, price,
           LAG(price) OVER (PARTITION BY symbol ORDER BY ts) as prev_price
    FROM market_ticks
);

-- Forward-looking analysis (CANNOT DO)
SELECT symbol, price,
       LEAD(price, 5) OVER (PARTITION BY symbol ORDER BY ts) as price_5_ahead
FROM market_ticks;
```

**Workaround**: Buffer recent events in application layer and compute deltas manually.

**Current Demo Workaround** (`asof_merge.rs`):
```rust
// Application-level tick buffer for ASOF matching
pub struct TickBuffer {
    // symbol -> BTreeMap<timestamp, (price, bid, ask)>
    index: HashMap<String, BTreeMap<i64, (f64, f64, f64)>>,
}

// Can extend this pattern for LAG/LEAD
pub fn get_previous_tick(&self, symbol: &str, ts: i64) -> Option<&(f64, f64, f64)> {
    self.index.get(symbol)?.range(..ts).last().map(|(_, v)| v)
}
```

**Implementation Notes**:
- Requires new window function infrastructure
- Need state management for tracking previous N values per partition
- Complex interaction with watermarks for out-of-order data

**Effort**: High (5-7 days)

---

### 3. Common Table Expressions (CTEs)

**Status**: NOT IMPLEMENTED

**Missing Syntax**:
```sql
WITH temp AS (
    SELECT ...
)
SELECT * FROM temp;
```

**Use Cases**:
```sql
-- Reusable subquery for complex analytics (CANNOT DO)
WITH ohlc AS (
    SELECT symbol,
           FIRST_VALUE(price) as open,
           MAX(price) as high,
           MIN(price) as low,
           LAST_VALUE(price) as close
    FROM market_ticks
    GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE)
),
prev_ohlc AS (
    SELECT symbol, open, high, low, close,
           LAG(close) OVER (PARTITION BY symbol ORDER BY window_start) as prev_close
    FROM ohlc
)
SELECT symbol,
       (close - prev_close) / prev_close * 100 as change_pct
FROM prev_ohlc;
```

**Workaround**: Create multiple intermediate streams.

```sql
-- Current approach: chain streams
CREATE STREAM ohlc_1m AS
SELECT symbol, FIRST_VALUE(price) as open, ...
FROM market_ticks
GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE);

CREATE STREAM ohlc_change AS
SELECT symbol, close, ...
FROM ohlc_1m;
-- Still can't compute change without LAG
```

**Effort**: High (5-7 days)

---

### 4. Set Operations (UNION/INTERSECT/EXCEPT)

**Status**: NOT IMPLEMENTED

**Missing Syntax**:
- `SELECT ... UNION SELECT ...`
- `SELECT ... UNION ALL SELECT ...`
- `SELECT ... INTERSECT SELECT ...`
- `SELECT ... EXCEPT SELECT ...`

**Use Cases**:
```sql
-- Combine alerts from multiple sources (CANNOT DO)
SELECT symbol, 'VOLUME_SPIKE' as alert_type, ts
FROM volume_anomalies
UNION ALL
SELECT symbol, 'PRICE_GAP' as alert_type, ts
FROM price_gap_alerts
UNION ALL
SELECT symbol, 'SPREAD_WIDE' as alert_type, ts
FROM spread_alerts;
```

**Workaround**: Subscribe to multiple streams separately in application.

```rust
// Current approach: multiple subscriptions
let volume_alerts = db.subscribe::<Alert>("volume_anomalies")?;
let spread_alerts = db.subscribe::<Alert>("spread_alerts")?;

// Merge in app
let mut all_alerts = Vec::new();
if let Some(v) = volume_alerts.poll() { all_alerts.extend(v); }
if let Some(s) = spread_alerts.poll() { all_alerts.extend(s); }
```

**Effort**: Medium (3-4 days) - sqlparser supports it, needs integration

---

### 5. Subqueries

**Status**: PARTIAL/UNKNOWN

**Types of Subqueries**:
| Type | Status |
|------|--------|
| Scalar subquery in SELECT | Unknown |
| Subquery in FROM (derived table) | Partially supported via DataFusion |
| Correlated subquery | Likely not supported |
| EXISTS/NOT EXISTS | Unknown |
| IN (subquery) | Unknown |

**Use Cases**:
```sql
-- Scalar subquery (UNKNOWN)
SELECT symbol, price,
       price - (SELECT AVG(price) FROM market_ticks WHERE symbol = t.symbol) as deviation
FROM market_ticks t;

-- EXISTS (UNKNOWN)
SELECT * FROM orders o
WHERE EXISTS (
    SELECT 1 FROM market_ticks t
    WHERE t.symbol = o.symbol AND t.ts > o.ts - 1000
);
```

**Workaround**: Use JOINs or compute in application layer.

**Effort**: Unknown - depends on DataFusion integration depth

---

### 6. Advanced GROUP BY

**Status**: NOT IMPLEMENTED

**Missing Syntax**:
- `GROUP BY ROLLUP(a, b)`
- `GROUP BY CUBE(a, b)`
- `GROUP BY GROUPING SETS ((a), (b), ())`

**Use Cases**:
```sql
-- Multi-level aggregation (CANNOT DO)
SELECT symbol, side,
       SUM(volume) as total_volume,
       GROUPING(symbol) as is_symbol_total,
       GROUPING(side) as is_side_total
FROM market_ticks
GROUP BY ROLLUP(symbol, side);

-- Output includes:
-- (AAPL, buy, 1000, 0, 0)   -- per symbol+side
-- (AAPL, NULL, 2500, 0, 1)  -- per symbol total
-- (NULL, NULL, 10000, 1, 1) -- grand total
```

**Workaround**: Create separate streams for each aggregation level.

**Effort**: Medium-High (4-5 days)

---

## Priority Ranking for Demo

| Gap | Priority | Impact | Effort | Recommendation |
|-----|----------|--------|--------|----------------|
| LAG/LEAD | **P0** | Critical for price deltas | High | App-layer workaround for demo |
| ROW_NUMBER | P1 | Useful for top-N | Medium | App-layer workaround |
| UNION | P2 | Nice for alert consolidation | Medium | Multiple subscriptions |
| CTEs | P3 | Code organization | High | Multiple streams |
| Subqueries | P3 | Complex analytics | Unknown | JOINs/app-layer |
| ROLLUP/CUBE | P4 | Multi-level reporting | Medium-High | Separate streams |

---

## Demo-Specific Workarounds

### Price Delta Calculation (LAG replacement)

```rust
// In app.rs - track last price per symbol
pub struct PriceTracker {
    last_prices: HashMap<String, f64>,
}

impl PriceTracker {
    pub fn update(&mut self, symbol: &str, price: f64) -> Option<f64> {
        let prev = self.last_prices.insert(symbol.to_string(), price);
        prev.map(|p| price - p)
    }

    pub fn pct_change(&mut self, symbol: &str, price: f64) -> Option<f64> {
        let prev = self.last_prices.insert(symbol.to_string(), price);
        prev.map(|p| (price - p) / p * 100.0)
    }
}
```

### Top-N Movers (ROW_NUMBER replacement)

```rust
// In app.rs - sort and take top N after receiving window results
pub fn top_movers(changes: &[PriceChange], n: usize) -> Vec<&PriceChange> {
    let mut sorted: Vec<_> = changes.iter().collect();
    sorted.sort_by(|a, b| {
        b.pct_change.abs().partial_cmp(&a.pct_change.abs()).unwrap()
    });
    sorted.into_iter().take(n).collect()
}
```

### Alert Consolidation (UNION replacement)

```rust
// In app.rs - merge multiple alert subscriptions
pub fn drain_all_alerts(
    volume_sub: &TypedSubscription<Alert>,
    spread_sub: &TypedSubscription<Alert>,
    // ... other alert subscriptions
) -> Vec<Alert> {
    let mut alerts = Vec::new();

    while let Some(batch) = volume_sub.poll() {
        alerts.extend(batch.into_iter().map(|a| a.with_type("VOLUME")));
    }
    while let Some(batch) = spread_sub.poll() {
        alerts.extend(batch.into_iter().map(|a| a.with_type("SPREAD")));
    }

    // Sort by timestamp
    alerts.sort_by_key(|a| a.ts);
    alerts
}
```

---

## Future Enhancement Recommendations

### Short Term (Demo Completion)
- Use app-layer workarounds for LAG/LEAD, ROW_NUMBER, UNION
- Document limitations clearly in demo README

### Medium Term (Post-Demo)
1. **F-SQL-002**: Implement LAG/LEAD window functions
2. **F-SQL-003**: Implement ROW_NUMBER/RANK
3. **F-SQL-004**: Integrate UNION/INTERSECT from sqlparser

### Long Term
1. CTE support
2. Correlated subqueries
3. ROLLUP/CUBE/GROUPING SETS

---

## References

- `crates/laminar-sql/src/parser/aggregation_parser.rs` - Aggregate function parsing (1,118 lines)
- `crates/laminar-sql/src/parser/window_rewriter.rs` - Window clause handling (658 lines)
- `crates/laminar-sql/src/datafusion/aggregate_bridge.rs` - DataFusion accumulator bridge
- `docs/features/phase-2/F077-extended-aggregation-parser.md` - Aggregation parser spec
