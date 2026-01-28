# F059: FIRST_VALUE and LAST_VALUE Aggregates

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F059 |
| **Status** | ✅ Done |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | S (2-3 days) |
| **Dependencies** | F004 |
| **Owner** | TBD |
| **Research** | [Time-Series Financial Research 2026](../../research/laminardb-timeseries-financial-research-2026.md) |

## Summary

Implement FIRST_VALUE and LAST_VALUE aggregation functions for streaming windows. These are **essential** for OHLC (Open-High-Low-Close) bar generation - the foundational financial analytics use case.

## Motivation

From research review:
- **"OHLC is just SQL aggregates"** - No custom types needed
- Current aggregators: COUNT, SUM, MIN, MAX, AVG (all i64)
- **Missing**: FIRST_VALUE (open), LAST_VALUE (close)

Without FIRST_VALUE/LAST_VALUE, this standard OHLC query cannot be executed:

```sql
SELECT
    symbol,
    TUMBLE_START(event_time, INTERVAL '1 minute') as bar_time,
    FIRST_VALUE(price) as open,    -- MISSING
    MAX(price) as high,            -- ✅ Have
    MIN(price) as low,             -- ✅ Have
    LAST_VALUE(price) as close,    -- MISSING
    SUM(quantity) as volume        -- ✅ Have
FROM trades
GROUP BY symbol, TUMBLE(event_time, INTERVAL '1 minute');
```

## Goals

1. Implement `FirstValueAggregator` for streaming windows
2. Implement `LastValueAggregator` for streaming windows
3. Support timestamp-ordered semantics (earliest/latest by event time)
4. Support different data types (i64, f64, String)
5. Integrate with existing `Aggregator` trait

## Non-Goals

- Custom OHLC type (standard aggregates sufficient)
- DataFusion UDAF registration (Phase 1.5/F006B scope)
- Process-time semantics (event-time only)

## Technical Design

### FirstValue Aggregator

```rust
/// FIRST_VALUE aggregator - returns first value seen in window.
///
/// Tracks the value with the earliest timestamp in the window.
/// For deterministic results, uses event timestamp, not arrival order.
#[derive(Debug, Clone)]
pub struct FirstValueAggregator {
    /// Column index to extract value from
    value_column_index: usize,
    /// Column index for event timestamp (for ordering)
    timestamp_column_index: usize,
}

/// Accumulator for FIRST_VALUE aggregation.
#[derive(Debug, Clone, Default, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct FirstValueAccumulator {
    /// The first value seen (None if no values yet)
    value: Option<i64>,
    /// Timestamp of the first value (for merge ordering)
    timestamp: Option<i64>,
}

impl FirstValueAggregator {
    /// Creates a new FIRST_VALUE aggregator.
    ///
    /// # Arguments
    ///
    /// * `value_column_index` - Column to extract value from
    /// * `timestamp_column_index` - Column for event timestamp ordering
    #[must_use]
    pub fn new(value_column_index: usize, timestamp_column_index: usize) -> Self {
        Self {
            value_column_index,
            timestamp_column_index,
        }
    }
}

impl Accumulator for FirstValueAccumulator {
    type Input = (i64, i64); // (value, timestamp)
    type Output = Option<i64>;

    fn add(&mut self, (value, timestamp): (i64, i64)) {
        match self.timestamp {
            None => {
                // First value
                self.value = Some(value);
                self.timestamp = Some(timestamp);
            }
            Some(existing_ts) if timestamp < existing_ts => {
                // Earlier timestamp - replace
                self.value = Some(value);
                self.timestamp = Some(timestamp);
            }
            _ => {
                // Later or equal timestamp - keep existing
            }
        }
    }

    fn merge(&mut self, other: &Self) {
        match (self.timestamp, other.timestamp) {
            (None, Some(_)) => {
                self.value = other.value;
                self.timestamp = other.timestamp;
            }
            (Some(self_ts), Some(other_ts)) if other_ts < self_ts => {
                self.value = other.value;
                self.timestamp = other.timestamp;
            }
            _ => {
                // Keep self
            }
        }
    }

    fn result(&self) -> Option<i64> {
        self.value
    }

    fn is_empty(&self) -> bool {
        self.value.is_none()
    }
}

impl Aggregator for FirstValueAggregator {
    type Acc = FirstValueAccumulator;

    fn create_accumulator(&self) -> FirstValueAccumulator {
        FirstValueAccumulator::default()
    }

    fn extract(&self, event: &Event) -> Option<(i64, i64)> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;

        let batch = &event.data;

        // Extract value
        let value_col = batch.column(self.value_column_index);
        let value_array = value_col.as_primitive_opt::<Int64Type>()?;
        let value = value_array.iter().flatten().next()?;

        // Extract timestamp
        let ts_col = batch.column(self.timestamp_column_index);
        let ts_array = ts_col.as_primitive_opt::<Int64Type>()?;
        let timestamp = ts_array.iter().flatten().next()?;

        Some((value, timestamp))
    }
}
```

### LastValue Aggregator

```rust
/// LAST_VALUE aggregator - returns last value seen in window.
///
/// Tracks the value with the latest timestamp in the window.
/// For deterministic results, uses event timestamp, not arrival order.
#[derive(Debug, Clone)]
pub struct LastValueAggregator {
    /// Column index to extract value from
    value_column_index: usize,
    /// Column index for event timestamp (for ordering)
    timestamp_column_index: usize,
}

/// Accumulator for LAST_VALUE aggregation.
#[derive(Debug, Clone, Default, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct LastValueAccumulator {
    /// The last value seen (None if no values yet)
    value: Option<i64>,
    /// Timestamp of the last value (for merge ordering)
    timestamp: Option<i64>,
}

impl Accumulator for LastValueAccumulator {
    type Input = (i64, i64); // (value, timestamp)
    type Output = Option<i64>;

    fn add(&mut self, (value, timestamp): (i64, i64)) {
        match self.timestamp {
            None => {
                // First value
                self.value = Some(value);
                self.timestamp = Some(timestamp);
            }
            Some(existing_ts) if timestamp > existing_ts => {
                // Later timestamp - replace
                self.value = Some(value);
                self.timestamp = Some(timestamp);
            }
            Some(existing_ts) if timestamp == existing_ts => {
                // Same timestamp - keep latest arrival (replace)
                self.value = Some(value);
            }
            _ => {
                // Earlier timestamp - keep existing
            }
        }
    }

    fn merge(&mut self, other: &Self) {
        match (self.timestamp, other.timestamp) {
            (None, Some(_)) => {
                self.value = other.value;
                self.timestamp = other.timestamp;
            }
            (Some(self_ts), Some(other_ts)) if other_ts > self_ts => {
                self.value = other.value;
                self.timestamp = other.timestamp;
            }
            _ => {
                // Keep self
            }
        }
    }

    fn result(&self) -> Option<i64> {
        self.value
    }

    fn is_empty(&self) -> bool {
        self.value.is_none()
    }
}
```

### Multi-Type Support

```rust
/// Generic FIRST_VALUE for different Arrow types.
#[derive(Debug, Clone)]
pub enum FirstValueType {
    Int64(FirstValueAggregator),
    Float64(FirstValueF64Aggregator),
    Utf8(FirstValueStringAggregator),
}

/// Float64 variant of FIRST_VALUE.
#[derive(Debug, Clone, Default, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct FirstValueF64Accumulator {
    value: Option<f64>,
    timestamp: Option<i64>,
}

/// String variant of FIRST_VALUE (stores as bytes).
#[derive(Debug, Clone, Default, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct FirstValueStringAccumulator {
    value: Option<Vec<u8>>,
    timestamp: Option<i64>,
}
```

## Implementation Phases

### Phase 1: Core i64 Implementation (1 day)

1. Implement `FirstValueAggregator` and `FirstValueAccumulator`
2. Implement `LastValueAggregator` and `LastValueAccumulator`
3. Unit tests for basic functionality
4. Tests for merge behavior (distributed aggregation)

### Phase 2: Multi-Type Support (1 day)

1. Add `FirstValueF64Aggregator` / `LastValueF64Aggregator`
2. Add `FirstValueStringAggregator` / `LastValueStringAggregator`
3. Add type enum for runtime dispatch
4. Tests for each type variant

### Phase 3: Integration (0.5-1 day)

1. Export from `operator::window` module
2. Add to window operator factory
3. Integration tests with `TumblingWindowOperator`
4. OHLC example in documentation

## Test Cases

```rust
#[test]
fn test_first_value_single_event() {
    let mut acc = FirstValueAccumulator::default();
    acc.add((100, 1000));
    assert_eq!(acc.result(), Some(100));
}

#[test]
fn test_first_value_multiple_events() {
    let mut acc = FirstValueAccumulator::default();
    acc.add((100, 1000)); // timestamp 1000
    acc.add((200, 500));  // timestamp 500 (earlier)
    acc.add((300, 1500)); // timestamp 1500 (later)
    assert_eq!(acc.result(), Some(200)); // earliest timestamp wins
}

#[test]
fn test_first_value_merge() {
    let mut acc1 = FirstValueAccumulator::default();
    acc1.add((100, 1000));

    let mut acc2 = FirstValueAccumulator::default();
    acc2.add((200, 500)); // Earlier

    acc1.merge(&acc2);
    assert_eq!(acc1.result(), Some(200)); // 500 < 1000
}

#[test]
fn test_last_value_multiple_events() {
    let mut acc = LastValueAccumulator::default();
    acc.add((100, 1000));
    acc.add((200, 500));  // Earlier - ignored
    acc.add((300, 1500)); // Latest timestamp wins
    assert_eq!(acc.result(), Some(300));
}

#[test]
fn test_ohlc_bars() {
    // Full OHLC aggregation test
    let first = FirstValueAggregator::new(0, 1); // price col, ts col
    let max = MaxAggregator::new(0);
    let min = MinAggregator::new(0);
    let last = LastValueAggregator::new(0, 1);
    let sum = SumAggregator::new(2); // quantity col

    // Simulate trades: (price, timestamp, quantity)
    // t=100: price=100, qty=10
    // t=200: price=105, qty=5
    // t=300: price=98, qty=15
    // t=400: price=102, qty=8

    // Expected OHLC: open=100, high=105, low=98, close=102, volume=38
}

#[test]
fn test_checkpoint_restore_first_value() {
    // Verify accumulator state survives checkpoint/restore
}
```

## Acceptance Criteria

- [ ] `FirstValueAggregator` implemented with timestamp ordering
- [ ] `LastValueAggregator` implemented with timestamp ordering
- [ ] Support for i64 type (primary)
- [ ] Support for f64 type (financial prices)
- [ ] Proper merge behavior for distributed aggregation
- [ ] Checkpoint/restore working
- [ ] 10+ unit tests passing
- [ ] Integration test with window operator
- [ ] OHLC example documented

## Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| add() | < 50ns | Single comparison + conditional store |
| merge() | < 50ns | Single comparison + conditional copy |
| result() | < 10ns | Return stored value |
| Memory per accumulator | 24 bytes | value (8) + timestamp (8) + option tags (8) |

## Usage Example

```rust
use laminar_core::operator::window::{
    TumblingWindowOperator, TumblingWindowAssigner,
    FirstValueAggregator, LastValueAggregator,
    MaxAggregator, MinAggregator, SumAggregator,
};
use std::time::Duration;

// Create OHLC aggregator (composite)
struct OhlcAggregator {
    first: FirstValueAggregator,  // open
    max: MaxAggregator,           // high
    min: MinAggregator,           // low
    last: LastValueAggregator,    // close
    sum: SumAggregator,           // volume
}

// 1-minute OHLC bars
let assigner = TumblingWindowAssigner::new(Duration::from_secs(60));
let ohlc = OhlcAggregator::new(
    0,  // price column
    1,  // timestamp column
    2,  // quantity column
);
let operator = TumblingWindowOperator::new(
    assigner,
    ohlc,
    Duration::from_secs(5),  // 5 second grace period
);
```

## References

- [Time-Series Financial Research 2026](../../research/laminardb-timeseries-financial-research-2026.md)
- QuestDB FIRST/LAST: https://questdb.io/docs/reference/function/aggregation/#first
- DataFusion FIRST_VALUE: Window function (not aggregate) - we need streaming aggregate variant
