# F004: Tumbling Windows

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F004 |
| **Status** | ✅ Done |
| **Priority** | P0 (Critical) |
| **Phase** | 1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F001, F003 |
| **Owner** | TBD |
| **Created** | 2026-01-20 |
| **Updated** | 2026-01-22 |

## Summary

Implement tumbling (non-overlapping, fixed-size) window aggregations. Tumbling windows are the simplest window type and serve as the foundation for more complex windowing strategies.

## Goals

- Support configurable window sizes (1s, 1m, 1h, etc.)
- Event-time based windowing with watermarks
- Efficient state management per window
- Clean window cleanup after emission

## Non-Goals

- SQL syntax support (deferred to F005/F006 - DataFusion Integration)
- Sliding/Session windows (F016/F017)

## Technical Design

### Architecture

Located in `crates/laminar-core/src/operator/window.rs`, implements the `Operator` trait.

### Window Assignment

```rust
pub struct TumblingWindowAssigner {
    size_ms: i64,
}

impl TumblingWindowAssigner {
    pub fn assign(&self, timestamp: i64) -> WindowId {
        let window_start = (timestamp / self.size_ms) * self.size_ms;
        let window_end = window_start + self.size_ms;
        WindowId::new(window_start, window_end)
    }
}
```

### Aggregator Trait

```rust
pub trait Accumulator: Default + Clone + Serialize + Deserialize + Send {
    type Input;
    type Output: ResultToI64;

    fn add(&mut self, value: Self::Input);
    fn merge(&mut self, other: &Self);
    fn result(&self) -> Self::Output;
    fn is_empty(&self) -> bool;
}

pub trait Aggregator: Send + Clone {
    type Acc: Accumulator;
    fn create_accumulator(&self) -> Self::Acc;
    fn extract(&self, event: &Event) -> Option<<Self::Acc as Accumulator>::Input>;
}
```

### Built-in Aggregators

- **CountAggregator** - Counts events in window
- **SumAggregator** - Sums i64 column values
- **MinAggregator** - Tracks minimum value
- **MaxAggregator** - Tracks maximum value
- **AvgAggregator** - Computes average

### Operator Implementation

```rust
pub struct TumblingWindowOperator<A: Aggregator> {
    assigner: TumblingWindowAssigner,
    aggregator: A,
    allowed_lateness_ms: i64,
    registered_windows: HashSet<WindowId>,
    operator_id: String,
}

impl<A: Aggregator> Operator for TumblingWindowOperator<A> {
    fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> Vec<Output>;
    fn on_timer(&mut self, timer: Timer, ctx: &mut OperatorContext) -> Vec<Output>;
    fn checkpoint(&self) -> OperatorState;
    fn restore(&mut self, state: OperatorState) -> Result<(), OperatorError>;
}
```

## SQL Syntax (Future - F005/F006)

```sql
SELECT
    TUMBLE_START(ts, INTERVAL '1' HOUR) as window_start,
    TUMBLE_END(ts, INTERVAL '1' HOUR) as window_end,
    COUNT(*) as event_count,
    SUM(amount) as total_amount
FROM orders
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR);
```

## Performance Results

| Metric | Target | Achieved |
|--------|--------|----------|
| Window assignment | < 10ns | ~4.4ns |
| Accumulator add | < 100ns | < 1ns |
| Window emit | < 1μs | ~773ns |
| Checkpoint (10 windows) | - | ~334ns |
| Restore (10 windows) | - | ~219ns |

**Window assignment is 2x better than target** - Simple division-based assignment achieves sub-5ns latency.

**Accumulator operations are sub-nanosecond** - Count/Sum/Min/Max all under 1ns.

**Window emit meets target** - Schema caching reduced emit time by 57% (from ~1.8μs to ~773ns).

## Completion Checklist

- [x] Window assignment correct (handles negative timestamps)
- [x] State management working (uses StateStore with typed access)
- [x] Watermark triggering (via TimerService)
- [x] Late data handling (allowed_lateness support)
- [x] Checkpoint/restore working
- [x] Comprehensive test suite (17 window-specific tests)
- [x] Benchmarks added (`cargo bench --bench window_bench`)
- [ ] SQL syntax supported (deferred to F005/F006)

## Code Pointers

- **Implementation**: `crates/laminar-core/src/operator/window.rs`
- **Benchmarks**: `crates/laminar-core/benches/window_bench.rs`
- **Tests**: In `window.rs` (tests module)
