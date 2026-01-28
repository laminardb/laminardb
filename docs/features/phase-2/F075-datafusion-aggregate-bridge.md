# F075: DataFusion Aggregate Bridge

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F075 |
| **Status** | ✅ Done |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F074 |
| **Owner** | TBD |

## Summary

Bridge DataFusion's 50+ built-in aggregates (STDDEV, VARIANCE, PERCENTILE, etc.) into LaminarDB's `DynAccumulator` trait. This avoids reimplementing statistical functions.

## Motivation

DataFusion provides battle-tested implementations of STDDEV, VARIANCE, PERCENTILE_CONT, MEDIAN, APPROX_DISTINCT, and many more. LaminarDB only has 7 hand-written aggregators. The bridge wraps DataFusion's `Accumulator` as LaminarDB's `DynAccumulator`.

## Goals

1. `DataFusionAccumulatorAdapter` wrapping DataFusion accumulators
2. `DataFusionAggregateFactory` implementing `DynAggregatorFactory`
3. Type conversion between Arrow ScalarValue and ScalarResult
4. Registration function for streaming aggregate context

## Non-Goals

- Replacing hand-written aggregators (those stay for Ring 0)
- User-defined aggregate functions (future)

## Technical Design

### Adapter

```rust
pub struct DataFusionAccumulatorAdapter {
    inner: Box<dyn datafusion_expr::Accumulator>,
    column_indices: Vec<usize>,
    input_types: Vec<DataType>,
    function_name: String,
}
impl DynAccumulator for DataFusionAccumulatorAdapter { ... }
```

### Factory

```rust
pub struct DataFusionAggregateFactory {
    udf: Arc<AggregateUDF>,
    column_indices: Vec<usize>,
    input_types: Vec<DataType>,
}
impl DynAggregatorFactory for DataFusionAggregateFactory { ... }
```

## Testing

30+ tests covering adapter basics, merge, type conversion, factory, built-in aggregate pass-through, and registration.
