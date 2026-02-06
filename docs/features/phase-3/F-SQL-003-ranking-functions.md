# F-SQL-003: ROW_NUMBER/RANK/DENSE_RANK Enhancement

> Status: ✅ Done
> Priority: P1
> Phase: 3

## Summary

Fix the ROW_NUMBER() per-group top-K detection bug and extend ranking function
support to include RANK() and DENSE_RANK() in the SQL parser and translator.

## Motivation

1. **Bug**: `analyze_order_by()` returned early with `OrderPattern::None` when
   the outer query had no ORDER BY, preventing `detect_row_number_pattern()`
   from running. The common subquery pattern
   `SELECT * FROM (...ROW_NUMBER()...) WHERE rn <= 5` failed detection.

2. **Missing**: RANK() and DENSE_RANK() were not recognized. Only ROW_NUMBER
   was checked. These are essential for analytics with tie handling.

## Syntax

```sql
-- ROW_NUMBER (existing, now correctly detected)
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rn
  FROM trades
) sub WHERE rn <= 5;

-- RANK (new: ties get same rank, gaps after ties)
SELECT * FROM (
  SELECT *, RANK() OVER (PARTITION BY region ORDER BY revenue DESC) AS rn
  FROM sales
) sub WHERE rn <= 3;

-- DENSE_RANK (new: ties get same rank, no gaps)
SELECT * FROM (
  SELECT *, DENSE_RANK() OVER (PARTITION BY category ORDER BY score DESC) AS rn
  FROM results
) sub WHERE rn <= 10;
```

## Design

### Parser (`order_analyzer.rs`)

- Added `RankType` enum: `RowNumber`, `Rank`, `DenseRank`
- Added `rank_type: RankType` field to `OrderPattern::PerGroupTopK`
- **Bug fix**: Moved `detect_row_number_pattern()` call BEFORE the
  `order_columns.is_empty()` early return
- Extended `extract_row_number_info()` to match RANK and DENSE_RANK
- Updated `detect_row_number_pattern()` return type to include `RankType`

### Translator (`order_translator.rs`)

- Added `rank_type: RankType` field to `PerGroupTopKConfig`
- Updated `from_analysis()` to pass through `rank_type`

### Re-exports (`translator/mod.rs`)

- `RankType` re-exported for downstream consumers

## Test Plan

| Module | Tests | What |
|--------|-------|------|
| `order_analyzer` | 8 new + 2 fixed | Subquery detection, RANK, DENSE_RANK, multiple partitions |
| `order_translator` | 2 new + 1 fixed | RankType preservation in config |

## References

- SQL:2003 window function specification
- PostgreSQL ROW_NUMBER/RANK/DENSE_RANK documentation
