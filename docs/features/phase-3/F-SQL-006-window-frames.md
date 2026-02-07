# F-SQL-006: Window Frame (ROWS BETWEEN) Support

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SQL-006 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | L (4-5 days) |
| **Dependencies** | F-SQL-002 (LAG/LEAD — analytic parser infrastructure) |

## Summary

Add support for SQL window frame specifications (`ROWS BETWEEN N PRECEDING AND
CURRENT ROW`, `RANGE BETWEEN`, etc.) enabling true per-row sliding window
computations like moving averages, running sums, and rolling statistics.

## Motivation

Window frames are essential for time-series analytics:

```sql
-- 10-tick moving average per symbol
SELECT symbol, price, ts,
    AVG(price) OVER (
        PARTITION BY symbol ORDER BY ts
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS moving_avg_10
FROM market_ticks;

-- Running cumulative volume
SELECT symbol, volume, ts,
    SUM(volume) OVER (
        PARTITION BY symbol ORDER BY ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_volume
FROM market_ticks;

-- Rolling max price (last 20 ticks)
SELECT symbol, price, ts,
    MAX(price) OVER (
        PARTITION BY symbol ORDER BY ts
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS rolling_max
FROM market_ticks;

-- Range-based: average over last 60 seconds
SELECT symbol, price, ts,
    AVG(price) OVER (
        PARTITION BY symbol ORDER BY ts
        RANGE BETWEEN INTERVAL '60' SECOND PRECEDING AND CURRENT ROW
    ) AS avg_60s
FROM market_ticks;
```

Currently LaminarDB supports GROUP BY windows (TUMBLE/HOP/SESSION) which produce
one output per window, and LAG/LEAD which access specific offsets. Window frames
produce one output per input row with an aggregate computed over a sliding frame.

## Design

### Frame Types

```rust
/// Window frame specification.
pub enum WindowFrame {
    /// ROWS BETWEEN: count-based frame
    Rows {
        start: FrameBound,
        end: FrameBound,
    },
    /// RANGE BETWEEN: value-based frame (requires ORDER BY)
    Range {
        start: FrameBound,
        end: FrameBound,
    },
}

pub enum FrameBound {
    /// UNBOUNDED PRECEDING
    UnboundedPreceding,
    /// N PRECEDING
    Preceding(u64),
    /// CURRENT ROW
    CurrentRow,
    /// N FOLLOWING
    Following(u64),
    /// UNBOUNDED FOLLOWING (only valid for end bound)
    UnboundedFollowing,
}
```

### Parser Changes (`analytic_parser.rs`)

Extend `analyze_analytic_functions()` to detect window frame specifications
in the OVER clause. sqlparser-rs already parses `WindowFrame` in `WindowSpec`:

```rust
pub struct WindowFrameAnalysis {
    /// Aggregate function (AVG, SUM, MAX, MIN, COUNT, etc.)
    pub function_type: WindowFrameFunction,
    /// Column being aggregated
    pub column: String,
    /// Frame specification
    pub frame: WindowFrame,
    /// Output alias
    pub alias: Option<String>,
}

pub enum WindowFrameFunction {
    Avg,
    Sum,
    Min,
    Max,
    Count,
    StdDev,
    Variance,
    FirstValue,
    LastValue,
}
```

Detect aggregate functions with OVER + frame spec (not just LAG/LEAD). Add
`WindowFrameAnalysis` to the existing `AnalyticWindowAnalysis` struct.

### Operator (`window_frame.rs` — NEW)

Per-partition sliding window operator:

```rust
pub struct WindowFrameOperator {
    config: WindowFrameConfig,
    /// Per-partition state: ring buffer of recent values
    partitions: FxHashMap<u64, PartitionFrameState>,
}

struct PartitionFrameState {
    /// Ring buffer of (value, order_key) tuples
    buffer: VecDeque<FrameEntry>,
}

struct FrameEntry {
    /// The value being aggregated
    value: ScalarValue,
    /// ORDER BY key (for RANGE frames)
    order_key: i64,
    /// Full row data (for output emission)
    row: Vec<ScalarValue>,
}
```

**Processing model**:

For each input event:
1. Hash partition key → get/create `PartitionFrameState`
2. Append new entry to ring buffer
3. For ROWS frame: trim buffer to `preceding + 1 + following` entries
4. For RANGE frame: trim buffer by order_key range
5. If FOLLOWING > 0: buffer current row, emit when enough future rows arrive
6. Compute aggregate over frame entries
7. Emit input row + computed aggregate column(s)

**Memory bounds**: O(P * frame_size) where P = partitions, frame_size = preceding + following + 1.

**Watermark handling**: For ROWS frames, flush buffered rows when watermark
advances (LEAD-like semantics for FOLLOWING). For RANGE frames, use watermark
to determine when the range can no longer grow.

### Incremental Computation

For commutative aggregates (SUM, COUNT, AVG), maintain a running accumulator
and subtract the leaving value / add the entering value instead of recomputing
over the entire frame. This gives O(1) per-event instead of O(frame_size):

```rust
trait IncrementalFrameAccumulator {
    fn add(&mut self, value: &ScalarValue);
    fn remove(&mut self, value: &ScalarValue);
    fn result(&self) -> ScalarValue;
}
```

For non-incremental aggregates (MIN, MAX, MEDIAN), use a monotonic deque or
segment tree for O(log n) sliding min/max.

### Streaming Semantics

- Each input row produces exactly one output row (1:1 mapping)
- ROWS BETWEEN N PRECEDING AND CURRENT ROW: emit immediately (no future buffering)
- ROWS BETWEEN ... AND M FOLLOWING: buffer M future rows before emitting
- RANGE BETWEEN: use watermark to determine completeness
- UNBOUNDED PRECEDING: running aggregate (memory = O(partitions), not O(events))
- UNBOUNDED FOLLOWING: not supported in streaming (infinite lookahead)

### Checkpoint/Restore

Checkpoint per-partition buffer state (ring buffer contents). On restore,
rebuild accumulators from buffer.

## Test Plan

| Module | Tests | What |
|--------|-------|------|
| `analytic_parser` | 8 | ROWS frame parsing, RANGE frame, UNBOUNDED, defaults |
| `window_frame` operator | 15 | AVG/SUM/MIN/MAX frames, partitioned, incremental, watermark flush |
| `translator` | 4 | Config conversion, schema extension |
| `planner` | 4 | Frame queries in query plan, validation (reject UNBOUNDED FOLLOWING) |
| Integration | 4 | End-to-end frame queries via `db.execute()` |

## Files

- `crates/laminar-sql/src/parser/analytic_parser.rs` — Extend for frame detection
- `crates/laminar-sql/src/translator/analytic_translator.rs` — WindowFrameConfig
- `crates/laminar-core/src/operator/window_frame.rs` — NEW: WindowFrameOperator
- `crates/laminar-core/src/operator/mod.rs` — Re-export
- `crates/laminar-sql/src/planner/mod.rs` — Wire into QueryPlan
