# Time-Series Database Storage for Real-Time Financial Streaming: 2026 Research & Gaps Analysis

## Executive Summary

This research document analyzes the latest trends (2025-2026) in time-series database storage for real-time financial data streaming, identifies key gaps in current solutions, and provides actionable guidance for extending LaminarDB's implementation to address these gaps.

**Key Finding:** A significant opportunity exists for LaminarDB to fill the gap between high-performance tick databases (kdb+, QuestDB) and streaming SQL databases (RisingWave, Flink). No current solution delivers sub-microsecond embedded streaming with native financial data primitives.

---

## Part 1: Current Landscape (2025-2026)

### 1.1 Market Leaders & Their Focus

| Database | Latency | Focus | Financial Features | Gaps |
|----------|---------|-------|-------------------|------|
| **kdb+** | Sub-μs queries | Tick storage, HFT | ASOF JOINs, q language | Proprietary, expensive, no streaming SQL, no open formats |
| **QuestDB** | Sub-ms queries | Tick storage, capital markets | ASOF JOINs, arrays, OHLC | No embedded mode, millisecond not microsecond |
| **RisingWave** | ~10ms | Streaming SQL | Basic aggregations | No sub-ms, no tick-specific primitives |
| **Apache Flink** | ~50-100ms | Stream processing | Complex event processing | JVM overhead, no embedded, operational complexity |
| **InfluxDB 3.0** | ~10ms | Time-series, IoT | Limited financial features | Not optimized for tick data, no ASOF JOINs |
| **TimescaleDB** | ~100ms | PostgreSQL extension | Window functions | Too slow for HFT, row-oriented core |

### 1.2 Key 2025-2026 Trends

#### A. Open Format Convergence
- **Parquet/Iceberg adoption**: QuestDB, InfluxDB 3.0, and newer systems store historical data in Apache Parquet
- **Arrow-native processing**: DataFusion-based systems gaining traction
- **Zero vendor lock-in**: Industry moving away from proprietary formats (kdb+ binary)

#### B. Tiered Storage Architecture
```
Hot Tier:   In-memory, sub-microsecond access (mmap, ring buffers)
Warm Tier:  Local SSD, columnar, millisecond access
Cold Tier:  Object storage (S3), Parquet files, seconds access
```

#### C. ASOF JOINs as Table Stakes
- Essential for financial analytics: matching trades to quotes at execution time
- QuestDB, kdb+, DuckDB all support ASOF JOINs
- Streaming ASOF JOINs remain challenging (state management)

#### D. Order Book Storage Evolution
- **2D Array support** emerging (QuestDB 8.2.4): Store full order book snapshots efficiently
- **Delta encoding**: Store only changes to reduce storage 10-100x
- **Level compression**: L1 (BBO), L2 (top 10), L3 (full book) tiering

### 1.3 Financial Data Types & Requirements

| Data Type | Update Rate | Latency Requirement | Storage Challenge |
|-----------|-------------|---------------------|-------------------|
| L1 Quotes (BBO) | 10K-100K/sec/symbol | < 100μs | High cardinality |
| L2 Order Book | 1K-10K/sec/symbol | < 1ms | 2D structure, frequent updates |
| L3 Full Book | 100-1K/sec/symbol | < 10ms | Very large state per symbol |
| Trades | 100-10K/sec/symbol | < 100μs | ASOF JOIN requirement |
| OHLC Bars | 1/interval | < 1s | Cascading aggregations |

---

## Part 2: Identified Gaps in Current Solutions

### Gap 1: No Embedded Sub-Microsecond Streaming SQL

**Problem:** 
- kdb+ achieves sub-μs but requires expensive licenses and q expertise
- QuestDB is fast but not embeddable and targets milliseconds
- Embedded options (DuckDB, SQLite) lack streaming capabilities

**LaminarDB Opportunity:**
- Embedded, zero-dependency deployment
- Sub-500ns hot path for state lookups
- SQL interface with streaming extensions
- Thread-per-core architecture for deterministic latency

### Gap 2: Financial-Specific Streaming Primitives Missing

**Problem:** No streaming database natively supports:
- Streaming ASOF JOINs with bounded state
- Order book delta processing with automatic compaction
- Multi-resolution OHLC bar generation (cascading materialized views)
- Tick deduplication with sequence number tracking
- Cross-venue arbitrage detection primitives

**LaminarDB Opportunity:**
```sql
-- Native streaming ASOF JOIN (not available elsewhere)
CREATE MATERIALIZED VIEW trade_with_quote AS
SELECT t.*, q.bid, q.ask
FROM trades t
ASOF JOIN quotes q ON t.symbol = q.symbol AND t.event_time >= q.event_time
WITHIN INTERVAL '1 second';

-- Cascading OHLC bars
CREATE MATERIALIZED VIEW ohlc_1s AS
SELECT symbol, 
       TUMBLE_START(event_time, INTERVAL '1 second') as bar_time,
       FIRST_VALUE(price) as open,
       MAX(price) as high,
       MIN(price) as low,
       LAST_VALUE(price) as close,
       SUM(quantity) as volume
FROM trades
GROUP BY symbol, TUMBLE(event_time, INTERVAL '1 second');

CREATE MATERIALIZED VIEW ohlc_1m AS
SELECT symbol,
       TUMBLE_START(bar_time, INTERVAL '1 minute') as bar_time,
       FIRST_VALUE(open) as open,
       MAX(high) as high,
       MIN(low) as low,
       LAST_VALUE(close) as close,
       SUM(volume) as volume
FROM ohlc_1s
GROUP BY symbol, TUMBLE(bar_time, INTERVAL '1 minute');
```

### Gap 3: Order Book State Management

**Problem:** 
- Order books are 2D structures (price level × depth)
- Most databases store as flat tables, requiring expensive reconstruction
- No streaming database handles order book deltas natively
- State size explodes with L3 full book data

**LaminarDB Opportunity:**
```rust
// Native order book state type
pub struct OrderBook {
    symbol: Symbol,
    bids: BTreeMap<Price, VecDeque<Order>>,  // Price-sorted, time-priority queue
    asks: BTreeMap<Price, VecDeque<Order>>,
    sequence: u64,
}

// Delta-compressed storage
pub enum OrderBookDelta {
    Add { side: Side, price: Price, order: Order },
    Modify { side: Side, price: Price, order_id: OrderId, new_qty: Quantity },
    Delete { side: Side, price: Price, order_id: OrderId },
    Snapshot { book: OrderBook },  // Periodic full snapshots
}
```

### Gap 4: Nanosecond Timestamp Precision & Clock Sync

**Problem:**
- Most TSDBs use microsecond precision (sufficient for IoT, not HFT)
- Clock synchronization across venues is critical
- No standard handling of exchange sequence numbers vs. capture timestamps

**LaminarDB Opportunity:**
```sql
CREATE TABLE trades (
    symbol VARCHAR,
    price DECIMAL(18, 8),
    quantity DECIMAL(18, 8),
    
    -- Nanosecond precision timestamps
    exchange_time TIMESTAMP(9),      -- Exchange matching engine time
    capture_time TIMESTAMP(9),       -- Local capture time
    processing_time TIMESTAMP(9),    -- Database processing time
    
    -- Sequence tracking
    exchange_seq BIGINT,             -- Exchange sequence number
    capture_seq BIGINT,              -- Local monotonic sequence
    
    WATERMARK FOR exchange_time AS exchange_time - INTERVAL '100' MILLISECOND
);
```

### Gap 5: Cross-Venue & Multi-Asset Correlation

**Problem:**
- Financial analysis requires correlating data across venues/assets
- Different venues have different latencies and timestamp semantics
- No streaming database handles venue-aware event time alignment

**LaminarDB Opportunity:**
```sql
-- Venue-aware timestamp alignment
CREATE SOURCE binance_btc WITH (
    venue = 'BINANCE',
    symbol = 'BTC/USDT',
    latency_offset = INTERVAL '5' MILLISECOND  -- Known venue latency
);

CREATE SOURCE coinbase_btc WITH (
    venue = 'COINBASE', 
    symbol = 'BTC-USD',
    latency_offset = INTERVAL '3' MILLISECOND
);

-- Cross-venue spread detection with latency compensation
CREATE MATERIALIZED VIEW arbitrage_opportunities AS
SELECT 
    b.event_time + b.latency_offset as aligned_time,
    b.bid as binance_bid,
    c.ask as coinbase_ask,
    b.bid - c.ask as spread
FROM binance_btc b
ASOF JOIN coinbase_btc c 
    ON b.event_time + b.latency_offset >= c.event_time + c.latency_offset
WHERE b.bid > c.ask;  -- Arbitrage exists
```

### Gap 6: Deterministic Latency & Jitter Control

**Problem:**
- JVM-based systems (Flink, Spark) have GC pauses causing latency spikes
- Most databases don't guarantee tail latency (p99, p99.9)
- HFT requires deterministic, not just fast, performance

**LaminarDB Opportunity:**
- Rust: No GC, predictable memory management
- Thread-per-core: No context switches on hot path
- Ring 0 hot path: Zero allocations, lock-free
- Target: p99 < 10μs, p99.9 < 50μs

### Gap 7: Real-Time + Historical Query Unification

**Problem:**
- Most systems either do real-time OR historical well, not both
- Switching between streaming views and historical queries requires different tools
- No unified query model for "give me live data + backfill from history"

**LaminarDB Opportunity:**
```sql
-- Unified query: live stream + historical backfill
SELECT * FROM trades
WHERE symbol = 'AAPL'
  AND event_time >= NOW() - INTERVAL '1 hour'  -- Historical
UNION ALL
SELECT * FROM trades_stream                      -- Live
WHERE symbol = 'AAPL';

-- Or with automatic materialization
CREATE MATERIALIZED VIEW aapl_vwap AS
SELECT 
    TUMBLE_START(event_time, INTERVAL '1 minute') as bar_time,
    SUM(price * quantity) / SUM(quantity) as vwap
FROM trades
WHERE symbol = 'AAPL'
GROUP BY TUMBLE(event_time, INTERVAL '1 minute')
WITH (
    historical_backfill = '7 days',  -- Backfill from storage on creation
    emit = 'on_watermark'
);
```

---

## Part 3: Competitive Benchmarks (2025-2026)

### Query Performance: 5-Minute OHLCV Aggregation

Source: Independent benchmark (September 2025)

| Database | Avg Query Time | Notes |
|----------|---------------|-------|
| PostgreSQL | 3,493ms | Baseline row-store |
| TimescaleDB | 1,021ms | Hypertables help |
| ClickHouse | 547ms | Columnar, compression |
| kdb+ | 109ms | Proprietary, expensive |
| QuestDB | **25ms** | Current fastest open-source |
| **LaminarDB Target** | **< 10ms** | With pre-computed MVs |

### Ingestion Throughput

| Database | Rows/sec (single node) | Notes |
|----------|----------------------|-------|
| InfluxDB | ~500K | Good but not tick-level |
| TimescaleDB | ~200K | PostgreSQL overhead |
| QuestDB | ~2M+ | ILP protocol |
| kdb+ | ~5M+ | In-memory, proprietary |
| **LaminarDB Target** | **~1M+** | With sub-μs state updates |

---

## Part 4: Architecture Recommendations for LaminarDB

### 4.1 Financial Data Model — No Custom Types Needed

**Design Decision:** Use standard SQL types and aggregates. No custom `Price`, `Quantity`, `OhlcBar`, or `MarketTimestamp` types.

**Rationale:**
- OHLC is just `FIRST_VALUE`, `MAX`, `MIN`, `LAST_VALUE`, `SUM` — standard SQL
- DataFusion already provides these aggregates
- Prices and quantities are just `f64` (DOUBLE)
- Timestamps are just `i64` nanoseconds (TIMESTAMP with precision)
- Custom types add complexity without clear benefit
- Can add specialized types later if users demand them

**What you actually need to implement:**

| Component | Status | Notes |
|-----------|--------|-------|
| `FIRST_VALUE()` aggregate | ✅ In DataFusion | Works in window functions |
| `LAST_VALUE()` aggregate | ✅ In DataFusion | Works in window functions |
| `MAX/MIN/SUM` aggregates | ✅ In DataFusion | Standard SQL |
| Tumbling window operator | 🔨 You build | Groups by time intervals |
| Watermark handling | 🔨 You build | Triggers window close |
| `SAMPLE BY` syntax (optional) | 🔨 Nice to have | Sugar for `GROUP BY TUMBLE()` |

**Example: OHLC bars with pure SQL (no custom types)**

```sql
-- This just works once you have tumbling windows
SELECT 
    symbol,
    TUMBLE_START(event_time, INTERVAL '1 minute') as bar_time,
    FIRST_VALUE(price) as open,
    MAX(price) as high,
    MIN(price) as low,
    LAST_VALUE(price) as close,
    SUM(quantity) as volume,
    COUNT(*) as trade_count
FROM trades
GROUP BY symbol, TUMBLE(event_time, INTERVAL '1 minute');
```

**Table schema — just standard types:**

```sql
CREATE TABLE trades (
    symbol VARCHAR,
    price DOUBLE,           -- f64, no custom type
    quantity DOUBLE,        -- f64, no custom type
    side VARCHAR,           -- 'buy' or 'sell'
    event_time TIMESTAMP(9) -- i64 nanoseconds internally
);
```

### 4.2 Streaming ASOF JOIN — The Key Differentiator

ASOF JOIN is where LaminarDB can truly differentiate. This is a **Phase 2** feature that requires state management.

**What ASOF JOIN does:**
```sql
-- Match each trade to the most recent quote for that symbol
SELECT t.*, q.bid, q.ask
FROM trades t
ASOF JOIN quotes q 
    ON t.symbol = q.symbol 
    AND t.event_time >= q.event_time;
```

**Implementation approach (Phase 2):**

```rust
/// Streaming ASOF JOIN operator
/// Keeps latest quote per symbol in state, joins incoming trades
pub struct AsofJoinOperator {
    /// Right side state: symbol -> latest row
    /// Just store as Arrow RecordBatch or raw bytes
    right_state: HashMap<String, Vec<u8>>,
    
    /// Maximum lookback tolerance (nanoseconds)
    tolerance_nanos: i64,
    
    /// Watermark for state cleanup
    watermark_nanos: i64,
}

impl AsofJoinOperator {
    /// O(1) lookup - critical for sub-μs latency
    pub fn lookup(&self, symbol: &str, timestamp_nanos: i64) -> Option<&[u8]> {
        // Return latest quote for symbol if within tolerance
        self.right_state.get(symbol)
    }
    
    /// Update state with new quote
    pub fn update_right(&mut self, symbol: String, row_bytes: Vec<u8>) {
        self.right_state.insert(symbol, row_bytes);
    }
}
```

**Note:** No custom `Quote` or `Trade` types needed — just work with Arrow RecordBatches or raw serialized rows.
```

### 4.3 Order Book State — Phase 2/3 Feature

Order book storage is complex and should be deferred until core streaming works.

**The challenge:** Order books are 2D structures (price levels × orders at each level) that update thousands of times per second.

**Approach options (decide later):**

1. **Flat table** — Store each order as a row, reconstruct book via query
   ```sql
   SELECT price, SUM(quantity) as size
   FROM orders
   WHERE symbol = 'AAPL' AND side = 'bid'
   GROUP BY price
   ORDER BY price DESC
   LIMIT 10;
   ```

2. **Materialized view** — Pre-aggregate order book state
   ```sql
   CREATE MATERIALIZED VIEW order_book AS
   SELECT symbol, side, price, SUM(quantity) as size
   FROM orders
   GROUP BY symbol, side, price;
   ```

3. **Custom state store** — Only if flat table is too slow (probably Phase 3+)

**Recommendation:** Start with flat table + materialized view. Add custom order book state only if benchmarks show it's needed.
```

### 4.4 Cascading Aggregations — Just Chained Materialized Views

Multi-resolution OHLC (1s → 1m → 1h) doesn't need custom types. It's just materialized views feeding into each other.

**SQL approach:**

```sql
-- Base: 1-second bars from raw trades
CREATE MATERIALIZED VIEW ohlc_1s AS
SELECT 
    symbol,
    TUMBLE_START(event_time, INTERVAL '1 second') as bar_time,
    FIRST_VALUE(price) as open,
    MAX(price) as high,
    MIN(price) as low,
    LAST_VALUE(price) as close,
    SUM(quantity) as volume
FROM trades
GROUP BY symbol, TUMBLE(event_time, INTERVAL '1 second');

-- Derived: 1-minute bars from 1-second bars
CREATE MATERIALIZED VIEW ohlc_1m AS
SELECT 
    symbol,
    TUMBLE_START(bar_time, INTERVAL '1 minute') as bar_time,
    FIRST_VALUE(open) as open,    -- First 1s bar's open
    MAX(high) as high,             -- Highest high
    MIN(low) as low,               -- Lowest low
    LAST_VALUE(close) as close,   -- Last 1s bar's close
    SUM(volume) as volume
FROM ohlc_1s
GROUP BY symbol, TUMBLE(bar_time, INTERVAL '1 minute');

-- Derived: 1-hour bars from 1-minute bars  
CREATE MATERIALIZED VIEW ohlc_1h AS
SELECT 
    symbol,
    TUMBLE_START(bar_time, INTERVAL '1 hour') as bar_time,
    FIRST_VALUE(open) as open,
    MAX(high) as high,
    MIN(low) as low,
    LAST_VALUE(close) as close,
    SUM(volume) as volume
FROM ohlc_1m
GROUP BY symbol, TUMBLE(bar_time, INTERVAL '1 hour');
```

**What you need to implement:**
1. Tumbling window operator (Phase 1)
2. Materialized views that can read from other MVs (Phase 2)
3. Proper watermark propagation through the chain (Phase 2)

**No custom `OhlcBar` type needed** — it's all standard aggregates.

---

## Part 5: Implementation Roadmap for LaminarDB

### When to Implement (Phase Alignment)

**Design Decision:** No custom types. Use standard SQL types (`DOUBLE`, `TIMESTAMP`) and standard aggregates (`FIRST_VALUE`, `MAX`, `MIN`, `LAST_VALUE`, `SUM`).

| Feature | Phase | Dependencies | Priority |
|---------|-------|--------------|----------|
| Tumbling window operator | **Phase 1** | Core reactor | P0 |
| `FIRST_VALUE`/`LAST_VALUE` aggregates | **Phase 1** | Check if in DataFusion | P0 |
| Watermark handling | **Phase 1** | Event time tracking | P0 |
| ASOF JOIN (batch) | Phase 2 | State store | P1 |
| Streaming ASOF JOIN | Phase 2 | Watermarks, state | P1 |
| Cascading materialized views | Phase 2 | MV infrastructure | P1 |
| Order book via MVs | Phase 2 | Materialized views | P2 |
| Cross-venue alignment | Phase 3 | Multi-source | P2 |
| Historical backfill | Phase 4 | Cold storage | P2 |

### Recommended Phase 1 Focus

Since you're in Phase 1, focus on:

1. **Tumbling Window Operator**
   - Groups events by time intervals
   - Triggers on watermark advancement
   - Runs standard aggregates per group

2. **Verify DataFusion Aggregates**
   - `FIRST_VALUE()` — needed for OHLC open
   - `LAST_VALUE()` — needed for OHLC close
   - If missing, implement as custom UDAFs

3. **Benchmark Framework**
   - Set up tick data ingestion benchmark
   - Compare against QuestDB
   - Target: match QuestDB throughput with lower latency

**No custom types to implement!** OHLC is just a SQL query once you have tumbling windows.

---

## Part 6: Claude Code Implementation Prompt

Save this as `docs/research/financial-timeseries-implementation.md` for use with Claude Code:

```markdown
# Financial Time-Series Implementation Context for Claude Code

## Summary

Extend LaminarDB with financial-specific time-series capabilities using **standard SQL types and aggregates only**. No custom types needed.

## Design Decisions

- **No custom types** — Use standard `DOUBLE`, `TIMESTAMP`, `VARCHAR`
- **OHLC = standard aggregates** — `FIRST_VALUE`, `MAX`, `MIN`, `LAST_VALUE`, `SUM`
- **Keep it simple** — Add specialized types only if benchmarks demand it

## Current Phase: 1 (Core Engine)

This implementation is appropriate for **Phase 1** when:
- Core reactor is working
- Basic SQL parsing (via DataFusion) is integrated

## What to Implement

### 1. Tumbling Window Operator (P0)

The key building block. Groups events by time intervals and runs aggregates.

```rust
/// Tumbling window operator - groups events by fixed time intervals
pub struct TumblingWindowOperator {
    /// Window size in nanoseconds
    window_size_nanos: i64,
    
    /// Current windows, keyed by (group_key, window_start)
    windows: HashMap<(GroupKey, i64), WindowState>,
    
    /// Aggregates to compute per window
    aggregates: Vec<Box<dyn Aggregate>>,
    
    /// Current watermark (nanoseconds)
    watermark_nanos: i64,
}

impl TumblingWindowOperator {
    /// Process an event
    pub fn process(&mut self, event: &RecordBatch, event_time_nanos: i64) {
        let window_start = self.align_to_window(event_time_nanos);
        // Update aggregates for this window
        // ...
    }
    
    /// Called when watermark advances - emit closed windows
    pub fn on_watermark(&mut self, watermark_nanos: i64) -> Vec<RecordBatch> {
        self.watermark_nanos = watermark_nanos;
        // Emit and remove windows where window_end <= watermark
        // ...
    }
    
    fn align_to_window(&self, time_nanos: i64) -> i64 {
        (time_nanos / self.window_size_nanos) * self.window_size_nanos
    }
}
```

### 2. Verify/Add FIRST_VALUE and LAST_VALUE Aggregates

Check if DataFusion has these. If not, implement as UDAFs:

```rust
/// FIRST_VALUE aggregate - returns first value seen in window
pub struct FirstValueAccumulator {
    value: Option<ScalarValue>,
    time: Option<i64>,
}

impl Accumulator for FirstValueAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // Track value with earliest timestamp
    }
    
    fn evaluate(&self) -> Result<ScalarValue> {
        self.value.clone().unwrap_or(ScalarValue::Null)
    }
}

/// LAST_VALUE aggregate - returns last value seen in window  
pub struct LastValueAccumulator {
    value: Option<ScalarValue>,
    time: Option<i64>,
}
```

### 3. SAMPLE BY Syntax (Nice to Have)

Sugar for `GROUP BY TUMBLE()`:

```sql
-- QuestDB-style syntax
SELECT symbol, FIRST(price), MAX(price), MIN(price), LAST(price)
FROM trades
SAMPLE BY 1m;

-- Translates to standard SQL
SELECT symbol, 
       FIRST_VALUE(price), MAX(price), MIN(price), LAST_VALUE(price)
FROM trades  
GROUP BY symbol, TUMBLE(event_time, INTERVAL '1 minute');
```

## Example: OHLC Query (No Custom Types!)

Once tumbling windows work, OHLC is just a query:

```sql
SELECT 
    symbol,
    TUMBLE_START(event_time, INTERVAL '1 minute') as bar_time,
    FIRST_VALUE(price) as open,
    MAX(price) as high,
    MIN(price) as low,
    LAST_VALUE(price) as close,
    SUM(quantity) as volume,
    COUNT(*) as trade_count,
    SUM(price * quantity) / SUM(quantity) as vwap
FROM trades
GROUP BY symbol, TUMBLE(event_time, INTERVAL '1 minute');
```

## Test Cases

```rust
#[test]
fn test_tumbling_window_basic() {
    let mut op = TumblingWindowOperator::new(
        Duration::from_secs(60).as_nanos() as i64,  // 1 minute windows
        vec![/* MAX, MIN, SUM aggregates */],
    );
    
    // Send events
    op.process(&make_trade(100.0, 10.0), nanos("09:30:05"));
    op.process(&make_trade(102.0, 5.0),  nanos("09:30:30"));
    op.process(&make_trade(98.0,  15.0), nanos("09:30:45"));
    
    // Advance watermark past window end
    let results = op.on_watermark(nanos("09:31:01"));
    
    assert_eq!(results.len(), 1);
    // Check aggregates...
}

#[test]
fn test_first_last_value_aggregates() {
    // Test that FIRST_VALUE returns earliest, LAST_VALUE returns latest
}
```

## Benchmark Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| Window update | < 1μs | Per event |
| Window close + emit | < 10μs | Per window |
| Tick ingestion | > 500K/sec | Phase 1 |
| OHLC query (1 day) | < 50ms | Via tumbling windows |

## What NOT to Implement (Yet)

- Custom `Price`/`Quantity` types — use `f64`
- Custom `OhlcBar` struct — use SQL aggregates
- Custom `Timestamp` wrapper — use `i64` nanoseconds
- Order book state store — use flat tables + MVs first
- Streaming ASOF JOIN — Phase 2

## References

- DataFusion aggregates: https://datafusion.apache.org/user-guide/sql/aggregate_functions.html
- QuestDB SAMPLE BY: https://questdb.io/docs/reference/sql/sample-by/
```

---

## Part 7: Quick Reference Card

Save as `docs/research/financial-timeseries-quick-ref.md`:

```markdown
# Financial Time-Series Quick Reference

## Design Philosophy

**No custom types unless benchmarks demand them.**

- `DOUBLE` for prices/quantities (same as QuestDB, kdb+)
- `TIMESTAMP` for event times (nanosecond precision via `i64`)
- OHLC = just SQL aggregates (`FIRST_VALUE`, `MAX`, `MIN`, `LAST_VALUE`)
- Order books = flat tables + materialized views

## Key Gaps LaminarDB Fills

1. **Embedded sub-μs streaming** — No competitor offers this
2. **Streaming ASOF JOINs** — Only batch support elsewhere  
3. **Cascading MVs** — MVs feeding into other MVs

## Phase 1 Focus (Current)

- [ ] Tumbling window operator
- [ ] Verify `FIRST_VALUE`/`LAST_VALUE` in DataFusion
- [ ] Watermark handling for window close
- [ ] Basic tick ingestion benchmark

## Phase 2 Focus (Later)

- [ ] Streaming ASOF JOIN operator
- [ ] Cascading materialized views
- [ ] `SAMPLE BY` syntax sugar

## OHLC is Just a Query

```sql
SELECT 
    symbol,
    TUMBLE_START(event_time, INTERVAL '1 minute') as bar_time,
    FIRST_VALUE(price) as open,
    MAX(price) as high,
    MIN(price) as low,
    LAST_VALUE(price) as close,
    SUM(quantity) as volume
FROM trades
GROUP BY symbol, TUMBLE(event_time, INTERVAL '1 minute');
```

**No `OhlcBar` type needed!**

## Performance Targets

| Metric | Phase 1 | Phase 2 |
|--------|---------|---------|
| Window update | < 1μs | < 500ns |
| Tick ingestion | 500K/sec | 1M/sec |
| OHLC query | < 50ms | < 20ms |

## Claude Code Prompts

### Implement tumbling windows
```
@docs/research/financial-timeseries-quick-ref.md
Implement TumblingWindowOperator that groups by time intervals and runs aggregates
```

### Add FIRST_VALUE/LAST_VALUE
```
@docs/research/financial-timeseries-quick-ref.md
Check if DataFusion has FIRST_VALUE/LAST_VALUE, if not implement as UDAFs
```

### Benchmark tick ingestion  
```
@docs/research/financial-timeseries-quick-ref.md
Create benchmark for tick data ingestion throughput
```

---

## Conclusion

LaminarDB has a unique opportunity to fill the gap between high-performance tick databases and streaming SQL engines.

**Key insight:** You don't need custom financial types. OHLC bars, VWAP, and most financial analytics are just standard SQL aggregates over tumbling windows.

**Recommended next steps:**
1. Implement tumbling window operator (Phase 1 core)
2. Verify `FIRST_VALUE`/`LAST_VALUE` work in DataFusion
3. Set up tick ingestion benchmark
4. Plan streaming ASOF JOIN for Phase 2

**Add custom types later only if:**
- Users complain about `f64` precision
- Benchmarks show specialized structs are faster
- Order book performance requires specialized state
