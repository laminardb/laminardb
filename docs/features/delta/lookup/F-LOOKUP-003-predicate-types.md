# F-LOOKUP-003: Predicate Types (Flat Enum)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-LOOKUP-003 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | S (1-2 days) |
| **Dependencies** | None (foundational) |
| **Blocks** | F-LOOKUP-002 (LookupSource trait), F-LSQL-003 (SQL predicate extraction) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/lookup/predicate.rs` |

## Summary

Flat `Predicate` enum for lookup table filter pushdown. This is a deliberate design choice to use a flat enumeration of simple comparison operations rather than a recursive expression tree. The flat enum maps directly to what real data sources can actually handle: Postgres parameterized queries (`WHERE col = $1`), Parquet row-group statistics filters, and Redis key patterns.

A `&[Predicate]` slice is implicitly ANDed. OR logic, NOT logic, nested expressions, and arbitrary function calls are explicitly excluded from pushdown and must be evaluated engine-side after fetching.

**Why NOT an expression tree:**

1. **Scope explosion risk**: An expression tree invites adding `And(Box<Expr>, Box<Expr>)`, `Or(...)`, `Not(...)`, `FunctionCall(...)`, etc. Each addition requires pushdown support in every source connector.
2. **Mini-SQL parser anti-pattern**: An expression tree eventually becomes a mini SQL parser/evaluator, duplicating DataFusion's work.
3. **Source capability mismatch**: Most sources handle simple predicates well. Complex expressions either cannot be pushed down or require source-specific translation that is fragile.
4. **Predictable performance**: Flat predicates have O(n) evaluation cost. Expression trees have unbounded depth and unpredictable cost.

## Goals

- Define a `Predicate` enum with exactly 9 variants covering common comparison operations
- Define `ColumnId` type for referencing columns in predicates
- Define `ScalarValue` enum for literal values in predicates
- Provide predicate splitting logic: separate pushable vs. local predicates
- Keep the enum `Clone`, `Debug`, `PartialEq` for testing and logging
- Ensure all variants map cleanly to SQL WHERE clauses and Parquet filters

## Non-Goals

- OR logic (`Predicate::Or(Vec<Predicate>)`) -- evaluate engine-side
- NOT logic (`Predicate::Not(Box<Predicate>)`) -- use negated variants (NotEq, IsNotNull)
- Nested expressions (`Predicate::And(Box<Predicate>, Box<Predicate>)`) -- implicit AND via slice
- Function calls (`Predicate::Function(name, args)`) -- evaluate engine-side
- LIKE/regex patterns -- too source-specific for a universal predicate
- BETWEEN -- decompose to Gte + Lt at the SQL layer
- Subqueries -- out of scope entirely
- Type coercion -- handled at SQL layer before predicate construction

## Technical Design

### Architecture

**Ring**: N/A (data structure, used across rings)
**Crate**: `laminar-core`
**Module**: `laminar-core/src/lookup/predicate.rs`

Predicates flow through the system as follows:

```
SQL Layer (DataFusion)
    │
    │ Extract WHERE clause predicates
    │ Decompose complex expressions into flat Predicate variants
    │ Reject unsupported expressions (OR, functions, subqueries)
    ▼
&[Predicate]  (flat list, implicitly ANDed)
    │
    ├──→ PushdownAdapter.split_predicates()
    │       ├──→ Pushable: sent to LookupSource.query()
    │       └──→ Local: evaluated in-engine after fetch
    │
    ├──→ PostgresLookupSource: translates to WHERE clause
    │       Eq(col, val)  →  "col = $N"
    │       In(col, vals) →  "col = ANY($N)"
    │       Gt(col, val)  →  "col > $N"
    │
    └──→ ParquetLookupSource: translates to row-group filter
            Gt(col, val)  →  skip row groups where max(col) <= val
            Eq(col, val)  →  skip row groups where min > val OR max < val
```

### API/Interface

```rust
/// Unique column identifier within a lookup table schema.
///
/// Maps to a column index in the table's Arrow schema. Column IDs
/// are assigned at table creation time and are stable across schema
/// evolution (columns can be added but existing IDs never change).
pub type ColumnId = u32;

/// Scalar value for use in predicate comparisons.
///
/// Covers the types needed for lookup table predicates. Intentionally
/// smaller than DataFusion's ScalarValue -- we only need types that
/// appear in primary keys and common filter columns.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    /// Null value.
    Null,
    /// Boolean value.
    Boolean(bool),
    /// 32-bit signed integer.
    Int32(i32),
    /// 64-bit signed integer.
    Int64(i64),
    /// 32-bit floating point.
    Float32(f32),
    /// 64-bit floating point.
    Float64(f64),
    /// UTF-8 string.
    Utf8(String),
    /// Binary data.
    Binary(Vec<u8>),
    /// Timestamp in microseconds since Unix epoch.
    TimestampMicros(i64),
    /// Date as days since Unix epoch.
    Date32(i32),
    /// Decimal128 with precision and scale.
    Decimal128(i128, u8, i8),
}

/// Flat predicate enum for lookup table filter pushdown.
///
/// Each variant represents a single comparison operation on a single
/// column. A `&[Predicate]` slice is implicitly ANDed: all predicates
/// must be satisfied for a row to match.
///
/// # Design: Why Flat, Not Tree
///
/// An expression tree (`And(Box<Expr>, Box<Expr>)`, `Or(...)`, etc.)
/// is a common anti-pattern for pushdown predicates because:
///
/// 1. **Scope explosion**: Every new node type must be handled by every
///    source connector's pushdown translator.
/// 2. **Mini-SQL anti-pattern**: The tree eventually becomes a SQL
///    parser/evaluator, duplicating DataFusion.
/// 3. **Source mismatch**: Real sources (Postgres, Parquet, Redis)
///    handle simple predicates. Complex expressions need source-specific
///    translation that is fragile and hard to test.
/// 4. **Unpredictable cost**: Flat predicates are O(n). Trees have
///    unbounded depth.
///
/// # OR Handling
///
/// OR cannot be expressed in the flat model. Queries with OR in the
/// WHERE clause are handled as follows:
///
/// 1. SQL layer attempts to convert OR to IN where possible:
///    `col = 1 OR col = 2` → `In(col, [1, 2])`
/// 2. Remaining OR conditions are not pushed down. The engine fetches
///    rows matching the AND predicates and evaluates OR locally.
///
/// # BETWEEN Handling
///
/// BETWEEN is decomposed by the SQL layer:
/// `col BETWEEN 10 AND 20` → `[Gte(col, 10), Lte(col, 20)]`
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// Column equals value. Maps to `col = $N` in SQL.
    Eq(ColumnId, ScalarValue),

    /// Column does not equal value. Maps to `col != $N` in SQL.
    NotEq(ColumnId, ScalarValue),

    /// Column greater than value. Maps to `col > $N` in SQL.
    Gt(ColumnId, ScalarValue),

    /// Column greater than or equal to value. Maps to `col >= $N` in SQL.
    Gte(ColumnId, ScalarValue),

    /// Column less than value. Maps to `col < $N` in SQL.
    Lt(ColumnId, ScalarValue),

    /// Column less than or equal to value. Maps to `col <= $N` in SQL.
    Lte(ColumnId, ScalarValue),

    /// Column value is in the given set. Maps to `col = ANY($N)` in SQL.
    /// The Vec should be non-empty; an empty Vec matches nothing.
    In(ColumnId, Vec<ScalarValue>),

    /// Column value is NULL. Maps to `col IS NULL` in SQL.
    IsNull(ColumnId),

    /// Column value is not NULL. Maps to `col IS NOT NULL` in SQL.
    IsNotNull(ColumnId),
}

impl Predicate {
    /// Returns the column ID this predicate references.
    pub fn column_id(&self) -> ColumnId {
        match self {
            Predicate::Eq(col, _)
            | Predicate::NotEq(col, _)
            | Predicate::Gt(col, _)
            | Predicate::Gte(col, _)
            | Predicate::Lt(col, _)
            | Predicate::Lte(col, _)
            | Predicate::In(col, _)
            | Predicate::IsNull(col)
            | Predicate::IsNotNull(col) => *col,
        }
    }

    /// Returns whether this predicate is a simple equality check.
    /// Simple equalities are the most universally supported pushdown.
    pub fn is_equality(&self) -> bool {
        matches!(self, Predicate::Eq(_, _))
    }

    /// Returns whether this predicate is a range check (Gt, Gte, Lt, Lte).
    /// Range predicates map to Parquet row-group statistics filtering.
    pub fn is_range(&self) -> bool {
        matches!(
            self,
            Predicate::Gt(_, _)
                | Predicate::Gte(_, _)
                | Predicate::Lt(_, _)
                | Predicate::Lte(_, _)
        )
    }

    /// Returns whether this predicate is a membership check (In).
    pub fn is_membership(&self) -> bool {
        matches!(self, Predicate::In(_, _))
    }

    /// Returns whether this predicate is a nullability check.
    pub fn is_null_check(&self) -> bool {
        matches!(self, Predicate::IsNull(_) | Predicate::IsNotNull(_))
    }
}
```

### Data Structures

```rust
/// Result of splitting predicates into pushable and local groups.
///
/// Used by `PushdownAdapter` and source connectors to determine
/// which predicates to send to the source and which to evaluate
/// in-engine after fetching.
#[derive(Debug)]
pub struct PredicateSplit<'a> {
    /// Predicates that can be pushed to the source.
    pub pushable: Vec<&'a Predicate>,
    /// Predicates that must be evaluated locally after fetch.
    pub local: Vec<&'a Predicate>,
}

/// Split a predicate list based on source capabilities.
///
/// Rules:
/// 1. If source does not support pushdown: all predicates are local.
/// 2. If source supports pushdown: push up to `max_predicate_count`.
/// 3. Equality predicates are prioritized for pushdown (most selective).
/// 4. Range predicates are pushed next.
/// 5. Membership (In) predicates are pushed next.
/// 6. Null checks are pushed last.
/// 7. Excess predicates beyond `max_predicate_count` are local.
pub fn split_predicates<'a>(
    predicates: &'a [Predicate],
    supports_pushdown: bool,
    max_predicate_count: usize,
) -> PredicateSplit<'a> {
    if !supports_pushdown || max_predicate_count == 0 {
        return PredicateSplit {
            pushable: Vec::new(),
            local: predicates.iter().collect(),
        };
    }

    // Priority order: Eq > Range > In > NullCheck > NotEq
    let mut prioritized: Vec<(usize, &Predicate)> = predicates
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let priority = match p {
                Predicate::Eq(_, _) => 0,
                Predicate::Gt(_, _)
                | Predicate::Gte(_, _)
                | Predicate::Lt(_, _)
                | Predicate::Lte(_, _) => 1,
                Predicate::In(_, _) => 2,
                Predicate::IsNull(_) | Predicate::IsNotNull(_) => 3,
                Predicate::NotEq(_, _) => 4,
            };
            (priority, &predicates[i])
        })
        .collect();

    prioritized.sort_by_key(|(priority, _)| *priority);

    let mut pushable = Vec::new();
    let mut local = Vec::new();

    for (_priority, pred) in prioritized {
        if pushable.len() < max_predicate_count {
            pushable.push(pred);
        } else {
            local.push(pred);
        }
    }

    PredicateSplit { pushable, local }
}

/// Convert a Predicate to a SQL WHERE clause fragment.
///
/// Returns a tuple of (sql_fragment, parameter_values) for use
/// in parameterized queries. Parameter placeholders use `$N` syntax
/// (Postgres-style).
///
/// # Example
///
/// ```rust,ignore
/// let pred = Predicate::Eq(0, ScalarValue::Int64(42));
/// let (sql, params) = predicate_to_sql(&pred, 1);
/// assert_eq!(sql, "col_0 = $1");
/// ```
pub fn predicate_to_sql(
    pred: &Predicate,
    param_offset: usize,
    column_names: &[String],
) -> (String, Vec<ScalarValue>) {
    match pred {
        Predicate::Eq(col, val) => {
            let col_name = &column_names[*col as usize];
            (format!("{} = ${}", col_name, param_offset), vec![val.clone()])
        }
        Predicate::NotEq(col, val) => {
            let col_name = &column_names[*col as usize];
            (format!("{} != ${}", col_name, param_offset), vec![val.clone()])
        }
        Predicate::Gt(col, val) => {
            let col_name = &column_names[*col as usize];
            (format!("{} > ${}", col_name, param_offset), vec![val.clone()])
        }
        Predicate::Gte(col, val) => {
            let col_name = &column_names[*col as usize];
            (format!("{} >= ${}", col_name, param_offset), vec![val.clone()])
        }
        Predicate::Lt(col, val) => {
            let col_name = &column_names[*col as usize];
            (format!("{} < ${}", col_name, param_offset), vec![val.clone()])
        }
        Predicate::Lte(col, val) => {
            let col_name = &column_names[*col as usize];
            (format!("{} <= ${}", col_name, param_offset), vec![val.clone()])
        }
        Predicate::In(col, vals) => {
            let col_name = &column_names[*col as usize];
            (format!("{} = ANY(${})", col_name, param_offset), vals.clone())
        }
        Predicate::IsNull(col) => {
            let col_name = &column_names[*col as usize];
            (format!("{} IS NULL", col_name), Vec::new())
        }
        Predicate::IsNotNull(col) => {
            let col_name = &column_names[*col as usize];
            (format!("{} IS NOT NULL", col_name), Vec::new())
        }
    }
}
```

### Algorithm/Flow

#### Predicate Construction (SQL Layer)

```
1. DataFusion parses WHERE clause into Expr tree
2. LaminarDB SQL extension walks the Expr tree:
   a. Simple comparisons (col op literal) → Predicate variant
   b. AND nodes → flatten into Vec<Predicate>
   c. BETWEEN → decompose to Gte + Lte
   d. OR where all branches share same column with Eq:
      → convert to In(col, [val1, val2, ...])
   e. Remaining OR, NOT, functions → mark as "local only"
3. Return (pushable: Vec<Predicate>, local: Vec<Expr>)
```

#### Predicate Evaluation (Local)

```
1. Receive serialized row bytes from source
2. Deserialize row to access column values
3. For each local predicate:
   a. Extract column value from row
   b. Compare against predicate value
   c. If any predicate fails → row does not match
4. Return match/no-match decision
```

#### Predicate Pushdown to Parquet

```
1. For each row group in Parquet file:
   a. Read column statistics (min, max, null_count)
   b. For each pushable predicate:
      - Eq(col, val): skip if val < min OR val > max
      - Gt(col, val): skip if max <= val
      - Lt(col, val): skip if min >= val
      - IsNull(col): skip if null_count == 0
      - IsNotNull(col): skip if null_count == row_count
   c. If any predicate eliminates the row group → skip it
2. Read only non-eliminated row groups
3. Apply predicates to individual rows within row groups
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| Column ID out of bounds | Predicate references non-existent column | Fail query with clear error message |
| Type mismatch | ScalarValue type doesn't match column type | Fail query, log column/value types |
| Empty In list | `In(col, [])` matches nothing | Short-circuit: return no results |
| Very large In list | `In(col, [v1,...,v10000])` | Chunk into multiple queries if needed |
| NaN comparison | Float NaN in Gt/Lt predicate | Follow IEEE 754: NaN comparisons return false |
| Null comparison | Eq(col, Null) | Rewrite to IsNull at construction time |

## Performance Targets

| Metric | Target | Condition | Measurement |
|--------|--------|-----------|-------------|
| Predicate construction | < 100ns | Single predicate from Expr | `bench_predicate_construction` |
| Predicate split (10 preds) | < 500ns | Split 10 predicates | `bench_predicate_split_10` |
| Local predicate evaluation | < 200ns per predicate | Single column comparison | `bench_predicate_eval_single` |
| SQL generation (5 preds) | < 1μs | 5 predicates to SQL string | `bench_predicate_to_sql_5` |
| `column_id()` accessor | < 10ns | Match and return | `bench_predicate_column_id` |
| Predicate clone | < 50ns | Simple Eq predicate | `bench_predicate_clone` |
| Predicate clone (In 100) | < 5μs | In with 100 values | `bench_predicate_clone_in_100` |

## Test Plan

### Unit Tests

- [ ] `test_predicate_eq_construction` -- verify fields
- [ ] `test_predicate_not_eq_construction` -- verify fields
- [ ] `test_predicate_gt_gte_lt_lte_construction` -- all range variants
- [ ] `test_predicate_in_construction` -- verify Vec<ScalarValue>
- [ ] `test_predicate_is_null_is_not_null` -- nullability checks
- [ ] `test_predicate_column_id_all_variants` -- column_id() returns correct ID
- [ ] `test_predicate_is_equality` -- true for Eq, false for others
- [ ] `test_predicate_is_range` -- true for Gt/Gte/Lt/Lte
- [ ] `test_predicate_is_membership` -- true for In
- [ ] `test_predicate_is_null_check` -- true for IsNull/IsNotNull
- [ ] `test_predicate_clone_equality` -- clone produces equal value
- [ ] `test_predicate_debug_output` -- Debug format is readable
- [ ] `test_scalar_value_all_types` -- construct each variant
- [ ] `test_scalar_value_equality` -- PartialEq works correctly
- [ ] `test_split_predicates_no_pushdown` -- all local
- [ ] `test_split_predicates_full_pushdown` -- all pushed
- [ ] `test_split_predicates_partial_pushdown` -- excess goes to local
- [ ] `test_split_predicates_priority_order` -- Eq pushed before Range
- [ ] `test_predicate_to_sql_eq` -- generates `col = $1`
- [ ] `test_predicate_to_sql_in` -- generates `col = ANY($1)`
- [ ] `test_predicate_to_sql_is_null` -- generates `col IS NULL`
- [ ] `test_predicate_to_sql_range` -- generates `col > $1`

### Integration Tests

- [ ] `test_predicate_roundtrip_sql_to_predicate` -- SQL WHERE → Predicate → SQL
- [ ] `test_predicate_with_postgres_source` -- predicates pushed to Postgres
- [ ] `test_predicate_with_parquet_source` -- predicates used for row group pruning
- [ ] `test_predicate_local_evaluation` -- non-pushable predicates evaluated in engine
- [ ] `test_between_decomposition` -- BETWEEN becomes Gte + Lte
- [ ] `test_or_to_in_conversion` -- OR on same column becomes In

### Benchmarks

- [ ] `bench_predicate_construction` -- Target: < 100ns
- [ ] `bench_predicate_split_10` -- Target: < 500ns for 10 predicates
- [ ] `bench_predicate_eval_single` -- Target: < 200ns per predicate
- [ ] `bench_predicate_to_sql_5` -- Target: < 1μs for 5 predicates
- [ ] `bench_predicate_column_id` -- Target: < 10ns
- [ ] `bench_predicate_clone` -- Target: < 50ns for simple
- [ ] `bench_predicate_clone_in_100` -- Target: < 5μs for In(100)

## Rollout Plan

1. **Step 1**: Define `ScalarValue` enum in `laminar-core/src/lookup/predicate.rs`
2. **Step 2**: Define `Predicate` enum with all 9 variants
3. **Step 3**: Implement accessor methods (`column_id()`, `is_equality()`, etc.)
4. **Step 4**: Implement `split_predicates()` function
5. **Step 5**: Implement `predicate_to_sql()` function
6. **Step 6**: Unit tests for all types and functions
7. **Step 7**: Integration tests with SQL layer
8. **Step 8**: Benchmarks
9. **Step 9**: Code review and merge

## Open Questions

- [ ] Should `ScalarValue` include `Uuid` as a dedicated variant? UUIDs are common in financial systems as entity IDs. Currently they would be stored as `Binary(Vec<u8>)` or `Utf8(String)`.
- [ ] Should `Predicate` have a `Between(ColumnId, ScalarValue, ScalarValue)` convenience variant, or is decomposition to Gte+Lte always acceptable? Between is common enough that a dedicated variant could simplify source translation.
- [ ] Should `predicate_to_sql()` use column names or positional references? Column names are more readable but require schema lookup. Positional references are faster but harder to debug.
- [ ] Should `In` have a maximum cardinality? Postgres handles `= ANY($1::int[])` well up to ~10K values, but larger sets may benefit from temporary tables or CTEs.

## Completion Checklist

- [ ] `ScalarValue` enum defined with all type variants
- [ ] `Predicate` enum defined with all 9 variants
- [ ] Accessor methods implemented
- [ ] `split_predicates()` function implemented
- [ ] `predicate_to_sql()` function implemented
- [ ] `PredicateSplit` struct defined
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation with `#![deny(missing_docs)]`
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## References

- [F-LOOKUP-002: LookupSource Trait](./F-LOOKUP-002-lookup-source-trait.md) -- consumer of predicates
- [F-LOOKUP-007: PostgresLookupSource](./F-LOOKUP-007-postgres-lookup-source.md) -- SQL predicate translation
- [F-LOOKUP-008: ParquetLookupSource](./F-LOOKUP-008-parquet-lookup-source.md) -- Parquet row-group filtering
- [Apache DataFusion Expr](https://docs.rs/datafusion-expr/) -- source of SQL expressions
- [Apache Parquet Statistics](https://parquet.apache.org/docs/file-format/metadata/) -- row-group stats
