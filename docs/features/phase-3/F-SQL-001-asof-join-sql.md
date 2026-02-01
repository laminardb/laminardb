# F-SQL-001: ASOF JOIN SQL Support

> Status: 🚧 In Progress
> Priority: P1
> Phase: 3

## Summary

Add ASOF JOIN SQL syntax support to the LaminarDB SQL parser and translator, mapping
Snowflake-style `ASOF JOIN ... MATCH_CONDITION(...) ON ...` to the existing Ring 0
`AsofJoinOperator` (F056).

## Motivation

ASOF joins are essential for time-series analytics: enriching trades with the most recent
quote, joining sensor readings with the nearest calibration, or matching orders to the
latest market price. The core operator exists (F056) but there is no SQL path to reach it.

## Syntax

Based on sqlparser-rs 0.60 Snowflake-style ASOF JOIN support:

```sql
-- Backward (default): find most recent right row where right.ts <= left.ts
SELECT *
FROM trades t
ASOF JOIN quotes q
  MATCH_CONDITION(t.ts >= q.ts)
  ON t.symbol = q.symbol;

-- Forward: find next right row where right.ts >= left.ts
SELECT *
FROM trades t
ASOF JOIN quotes q
  MATCH_CONDITION(t.ts <= q.ts)
  ON t.symbol = q.symbol;

-- With tolerance: backward, but only within 5 seconds
SELECT *
FROM trades t
ASOF JOIN quotes q
  MATCH_CONDITION(t.ts >= q.ts AND t.ts - q.ts <= 5000)
  ON t.symbol = q.symbol;

-- With INTERVAL tolerance
SELECT *
FROM trades t
ASOF JOIN quotes q
  MATCH_CONDITION(t.ts >= q.ts AND t.ts - q.ts <= INTERVAL '5' SECOND)
  ON t.symbol = q.symbol;
```

### Direction Inference

| MATCH_CONDITION inequality | Direction |
|---------------------------|-----------|
| `left.ts >= right.ts`     | Backward  |
| `left.ts <= right.ts`     | Forward   |

### Tolerance Extraction

Tolerance is extracted from an optional `AND` clause:
- `AND left.ts - right.ts <= <literal>` — millisecond tolerance
- `AND left.ts - right.ts <= INTERVAL '5' SECOND` — interval tolerance

## Design

### Parser Changes (`join_parser.rs`)

1. New `JoinType::AsOf` variant
2. New `AsofSqlDirection` enum (Backward/Forward)
3. New fields on `JoinAnalysis`: `is_asof_join`, `asof_direction`, `left_time_column`,
   `right_time_column`, `asof_tolerance`
4. New `analyze_asof_match_condition(expr)` function parses MATCH_CONDITION expression
5. Updated `analyze_join()` to handle `JoinOperator::AsOf { match_condition, constraint }`

### Translator Changes (`join_translator.rs`)

1. New `AsofJoinTranslatorConfig` struct with direction, time columns, tolerance
2. New `JoinOperatorConfig::Asof` variant
3. Updated `from_analysis()` to emit Asof config when `is_asof_join` is true

### Mapping to Ring 0

```
SQL:  ASOF JOIN ... MATCH_CONDITION(t.ts >= q.ts) ON t.sym = q.sym
        |
        v
Parser: JoinAnalysis { is_asof_join: true, direction: Backward, ... }
        |
        v
Translator: AsofJoinTranslatorConfig { direction: Backward, key: "sym", ... }
        |
        v
Ring 0: AsofJoinOperator { mode: Backward, key_extractor, time_extractor, tolerance }
```

## Test Plan

### Parser Tests (8+)

| Test | Description |
|------|-------------|
| `test_asof_join_backward` | `>=` in MATCH_CONDITION → Backward direction |
| `test_asof_join_forward` | `<=` in MATCH_CONDITION → Forward direction |
| `test_asof_join_with_tolerance` | Numeric literal tolerance extraction |
| `test_asof_join_with_interval_tolerance` | INTERVAL tolerance extraction |
| `test_asof_join_type_mapping` | `JoinType::AsOf` returned correctly |
| `test_asof_join_extracts_time_columns` | Left/right time column names |
| `test_asof_join_extracts_key_columns` | Key columns from ON clause |
| `test_asof_join_aliases` | Table aliases preserved |

### Translator Tests (4+)

| Test | Description |
|------|-------------|
| `test_from_analysis_asof` | ASOF analysis → AsofJoinTranslatorConfig |
| `test_asof_config_fields` | Direction, tolerance, time columns correct |
| `test_asof_is_asof` | `is_asof()` returns true |
| `test_asof_key_accessors` | `left_key()`/`right_key()` work |

## References

- [Snowflake ASOF JOIN](https://docs.snowflake.com/en/sql-reference/constructs/asof-join)
- [DuckDB ASOF JOIN](https://duckdb.org/docs/sql/query_syntax/from.html#as-of-joins)
- [ClickHouse ASOF JOIN](https://clickhouse.com/docs/en/sql-reference/statements/select/join#asof-join)
- F056: ASOF Joins (Ring 0 operator)
- sqlparser-rs 0.60 `JoinOperator::AsOf` variant
