# F077: Extended Aggregation Parser

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F077 |
| **Status** | ✅ Done |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F074, F075 |
| **Owner** | TBD |

## Summary

Extend the `AggregateType` enum to recognize 20+ additional SQL aggregate functions, add FILTER clause and WITHIN GROUP detection, and provide DataFusion function name mapping.

## Motivation

The current parser only recognizes 8 function names (COUNT, SUM, MIN, MAX, AVG, FIRST_VALUE, LAST_VALUE, COUNT DISTINCT). Real SQL workloads need STDDEV, VARIANCE, PERCENTILE, MEDIAN, STRING_AGG, ARRAY_AGG, BOOL_AND/OR, APPROX_COUNT_DISTINCT, and more. The extended parser maps these to DataFusion built-in names via F075.

## Goals

1. ~20 new `AggregateType` variants
2. SQL alias recognition (STDDEV_SAMP=STDDEV, VAR_SAMP=VARIANCE, etc.)
3. FILTER clause detection on AggregateInfo
4. WITHIN GROUP clause detection
5. `datafusion_name()` mapping method
6. Updated `is_decomposable()` and `is_order_sensitive()` for new types

## Non-Goals

- Implementing the actual aggregate computation (that's F074/F075)
- Window function detection (that's the window_rewriter)

## Technical Design

### New AggregateType Variants

```rust
StdDev, StdDevPop, Variance, VariancePop,
Percentile, PercentileCont, PercentileDisc,
Median, StringAgg, ArrayAgg, BoolAnd, BoolOr,
ApproxCountDistinct, ApproxPercentile, ApproxMedian,
Covar, CovarPop, Corr, RegrSlope, RegrIntercept,
```

### AggregateInfo Extensions

```rust
pub filter: Option<Box<Expr>>,
pub within_group: Option<Vec<OrderByExpr>>,
```

### datafusion_name() Method

Maps LaminarDB aggregate names to DataFusion function registry names.

## Testing

35+ tests covering new aggregate detection, alias synonyms, FILTER clause, WITHIN GROUP, datafusion_name() mapping, is_decomposable() for new types, and multi-aggregate queries.
