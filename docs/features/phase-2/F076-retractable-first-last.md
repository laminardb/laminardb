# F076: Retractable FIRST/LAST Accumulators

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F076 |
| **Status** | ✅ Done |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | S (2-3 days) |
| **Dependencies** | None |
| **Owner** | TBD |

## Summary

Add retractable versions of FIRST_VALUE and LAST_VALUE accumulators for use with `EMIT CHANGES` (changelog mode). Current FirstValue/LastValue only track the current best with no history, making retraction impossible.

## Motivation

OHLC with `EMIT CHANGES` needs to retract previous open/close values when events are removed from a window. Without retractable FIRST/LAST, changelog-mode OHLC queries cannot produce correct retractions.

## Goals

1. `RetractableFirstValueAccumulator` with sorted entry storage
2. `RetractableLastValueAccumulator` with sorted entry storage
3. f64 variants using bits-as-i64 pattern
4. Integration with existing `RetractableAccumulator` trait

## Non-Goals

- Replacing non-retractable FirstValue/LastValue (those stay for non-changelog mode)
- String/binary retractable variants (future)

## Technical Design

### Storage

Store sorted `Vec<(i64, i64)>` entries (timestamp, value). On retraction, remove the entry and return the next best value.

```rust
pub struct RetractableFirstValueAccumulator {
    entries: Vec<(i64, i64)>,  // sorted by timestamp asc
}
pub struct RetractableLastValueAccumulator {
    entries: Vec<(i64, i64)>,  // sorted by timestamp asc
}
```

### f64 Variants

Use `f64::to_bits()` / `f64::from_bits()` for lossless i64 storage.

## Testing

30+ tests covering basic add/retract, merge, reset, f64 variants, edge cases, and OHLC retraction simulation.
