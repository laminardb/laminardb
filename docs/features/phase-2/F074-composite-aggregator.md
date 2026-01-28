# F074: Composite Aggregator & f64 Type Support

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F074 |
| **Status** | ✅ Done |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | None |
| **Owner** | TBD |

## Summary

Add composite aggregation and f64 type support to window operators. Currently `TumblingWindowOperator<A: Aggregator>` accepts only ONE aggregator and all results go through `ResultToI64` (i64 only). Real OHLC queries need multiple aggregates with float support.

## Motivation

```sql
SELECT
    symbol,
    TUMBLE_START(event_time, INTERVAL '1 minute') as bar_time,
    FIRST(price) as open,
    MAX(price) as high,
    MIN(price) as low,
    LAST(price) as close,
    SUM(quantity) as volume,
    COUNT(*) as trade_count
FROM trades
GROUP BY symbol, TUMBLE(event_time, INTERVAL '1 minute');
```

This requires 6 aggregates running simultaneously with f64 results.

## Goals

1. `ScalarResult` enum supporting Int64, Float64, UInt64, Null
2. `DynAccumulator` trait for dynamic-dispatch accumulation
3. `DynAggregatorFactory` trait for creating accumulators
4. f64 aggregator variants (SumF64, MinF64, MaxF64, AvgF64)
5. `CompositeAggregator` combining multiple aggregators
6. Composite output path in all window operators

## Non-Goals

- Replacing the existing static-dispatch path (backward compat)
- String/binary aggregators (future)
- Ring 0 allocation-free composite path (composite = Ring 1)

## Technical Design

### ScalarResult

```rust
pub enum ScalarResult {
    Int64(i64),
    Float64(f64),
    UInt64(u64),
    OptionalInt64(Option<i64>),
    OptionalFloat64(Option<f64>),
    Null,
}
```

### DynAccumulator Trait

```rust
pub trait DynAccumulator: Send {
    fn add_event(&mut self, event: &Event);
    fn merge_dyn(&mut self, other: &dyn DynAccumulator);
    fn result_scalar(&self) -> ScalarResult;
    fn is_empty(&self) -> bool;
    fn clone_box(&self) -> Box<dyn DynAccumulator>;
    fn serialize(&self) -> Vec<u8>;
    fn result_field(&self) -> Field;
}
```

### CompositeAggregator

```rust
pub struct CompositeAggregator {
    factories: Vec<Box<dyn DynAggregatorFactory>>,
    field_names: Vec<String>,
}
```

Multi-column RecordBatch output: `window_start, window_end, field_0, field_1, ...`

## Testing

35+ tests covering ScalarResult conversions, f64 aggregators, composite lifecycle, OHLC integration, window operator composite output, and backward compatibility.
