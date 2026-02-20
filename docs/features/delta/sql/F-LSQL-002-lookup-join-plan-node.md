# F-LSQL-002: Lookup Join Plan Node (DataFusion Extension)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-LSQL-002 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | L (5-10 days) |
| **Dependencies** | F-LSQL-001 (CREATE LOOKUP TABLE DDL), F005B (Advanced DataFusion Integration) |
| **Blocks** | F-LSQL-003 (Predicate Splitting) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-sql` |
| **Module** | `laminar-sql/src/planner/lookup_join.rs` |

## Summary

Implements a custom DataFusion logical plan node, `LookupJoinNode`, for temporal lookup joins in streaming SQL queries. When a query joins a streaming source against a lookup table using the `FOR SYSTEM_TIME AS OF NOW()` temporal syntax, the planner replaces the standard join node with a `LookupJoinNode` that knows how to perform point-in-time lookups against the lookup table's local cache or remote source. The plan node extends DataFusion's `UserDefinedLogicalNode` trait, participates in the optimizer's rule passes (predicate pushdown, column pruning), and converts to a physical `LookupJoinOperator` for execution. This is the central planning component that bridges SQL syntax with LaminarDB's lookup table runtime.

## Goals

- Define `LookupJoinNode` as a DataFusion `UserDefinedLogicalNode` for temporal lookup joins
- Support `FOR SYSTEM_TIME AS OF NOW()` temporal join syntax
- Extract join keys from the ON clause for efficient point lookups
- Participate in DataFusion optimizer rules: predicate pushdown, column pruning, filter reordering
- Convert to a physical `LookupJoinExec` plan node for execution
- Support both inner and left outer temporal joins
- Carry lookup table metadata (connector, strategy, pushdown mode) for physical plan conversion
- Handle multi-column join keys (composite lookups)

## Non-Goals

- Predicate splitting and pushdown planning (covered by F-LSQL-003)
- Lookup table runtime execution (covered by F-LOOKUP-001)
- Stream-stream joins (covered by F019)
- Full outer or right outer temporal joins (semantically unclear for streaming)
- Cross-node lookup routing (handled at execution layer)

## Technical Design

### Architecture

**Ring**: N/A (query planning is a control plane operation)

**Crate**: `laminar-sql`

**Module**: `laminar-sql/src/planner/lookup_join.rs`

The `LookupJoinNode` is inserted into the logical plan when the planner recognizes a temporal join pattern. It replaces DataFusion's standard `Join` node with a custom node that carries additional metadata about the lookup table and join semantics.

```
┌─────────────────────────────────────────────────────────────────────┐
│  SQL Query                                                           │
│                                                                      │
│  SELECT t.symbol, t.price, i.currency, i.exchange                   │
│  FROM trades AS t                                                    │
│  JOIN instruments FOR SYSTEM_TIME AS OF NOW()                        │
│      ON t.isin = i.isin                                              │
│  WHERE i.exchange = 'NYSE';                                          │
│                                                                      │
├──────────────────────┬──────────────────────────────────────────────┤
│  Standard Plan       │  Optimized Plan                              │
│                      │                                               │
│  Projection          │  Projection                                   │
│    Filter            │    LookupJoinNode                             │
│      Join            │      stream_input: Scan(trades)               │
│        Scan(trades)  │      lookup_table: instruments                │
│        Scan(instr.)  │      join_keys: [(t.isin, i.isin)]           │
│                      │      join_type: Inner                         │
│                      │      pushdown: [i.exchange = 'NYSE']         │
│                      │      output_cols: [t.symbol, t.price,        │
│                      │                    i.currency, i.exchange]    │
│                      │                                               │
└──────────────────────┴──────────────────────────────────────────────┘
```

### API/Interface

```rust
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

/// Custom logical plan node for temporal lookup joins.
///
/// Replaces DataFusion's standard Join node when the query involves
/// a temporal join against a lookup table using
/// `FOR SYSTEM_TIME AS OF NOW()`.
///
/// This node carries all the information needed for:
/// 1. Join key extraction (for point lookups)
/// 2. Predicate pushdown to the lookup source (F-LSQL-003)
/// 3. Column pruning (only fetch needed columns from lookup table)
/// 4. Physical plan conversion (to LookupJoinExec)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LookupJoinNode {
    /// The streaming side of the join (left input).
    pub stream_input: LogicalPlan,
    /// Name of the lookup table (right side).
    pub lookup_table: String,
    /// Schema of the lookup table.
    pub lookup_schema: DFSchemaRef,
    /// Join key pairs: (stream_column_expr, lookup_column_name).
    pub join_keys: Vec<JoinKeyPair>,
    /// Join type (Inner or LeftOuter).
    pub join_type: LookupJoinType,
    /// Predicates that can be pushed down to the lookup source.
    /// Populated by the PredicateSplitter optimizer rule (F-LSQL-003).
    pub pushdown_predicates: Vec<Expr>,
    /// Predicates that must be evaluated locally after the join.
    pub local_predicates: Vec<Expr>,
    /// Columns needed from the lookup table (for projection pushdown).
    pub required_lookup_columns: HashSet<String>,
    /// Full output schema of the join.
    pub output_schema: DFSchemaRef,
    /// Lookup table metadata for physical plan conversion.
    pub lookup_metadata: LookupTableMetadata,
}

/// A pair of join key expressions: stream-side and lookup-side.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JoinKeyPair {
    /// Expression on the streaming side (e.g., `t.isin`).
    pub stream_expr: Expr,
    /// Column name on the lookup side (e.g., `isin`).
    pub lookup_column: String,
}

/// Supported join types for lookup joins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LookupJoinType {
    /// Inner join: only emit rows where lookup key exists.
    Inner,
    /// Left outer join: emit all stream rows; NULL for missing lookups.
    LeftOuter,
}

/// Metadata about the lookup table for physical planning.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LookupTableMetadata {
    /// Connector type.
    pub connector: String,
    /// Lookup strategy (replicated, partitioned, on-demand).
    pub strategy: String,
    /// Pushdown mode (auto, enabled, disabled).
    pub pushdown_mode: String,
    /// Primary key columns of the lookup table.
    pub primary_key: Vec<String>,
    /// Whether the lookup table has an in-memory cache.
    pub has_cache: bool,
}

impl UserDefinedLogicalNodeCore for LookupJoinNode {
    fn name(&self) -> &str {
        "LookupJoin"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.stream_input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = Vec::new();
        // Include join key expressions
        for pair in &self.join_keys {
            exprs.push(pair.stream_expr.clone());
        }
        // Include pushdown predicates
        exprs.extend(self.pushdown_predicates.clone());
        // Include local predicates
        exprs.extend(self.local_predicates.clone());
        exprs
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LookupJoin: table={}, keys=[{}], type={:?}",
            self.lookup_table,
            self.join_keys
                .iter()
                .map(|k| format!("{} = {}", k.stream_expr, k.lookup_column))
                .collect::<Vec<_>>()
                .join(", "),
            self.join_type,
        )?;
        if !self.pushdown_predicates.is_empty() {
            write!(
                f,
                ", pushdown=[{}]",
                self.pushdown_predicates
                    .iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        assert_eq!(inputs.len(), 1, "LookupJoinNode has exactly one input");

        let num_keys = self.join_keys.len();
        let num_pushdown = self.pushdown_predicates.len();

        let join_keys: Vec<JoinKeyPair> = self
            .join_keys
            .iter()
            .zip(exprs.iter().take(num_keys))
            .map(|(orig, new_expr)| JoinKeyPair {
                stream_expr: new_expr.clone(),
                lookup_column: orig.lookup_column.clone(),
            })
            .collect();

        let pushdown_predicates = exprs[num_keys..num_keys + num_pushdown].to_vec();
        let local_predicates = exprs[num_keys + num_pushdown..].to_vec();

        Ok(Self {
            stream_input: inputs.into_iter().next().unwrap(),
            join_keys,
            pushdown_predicates,
            local_predicates,
            ..self.clone()
        })
    }
}
```

### Data Structures

```rust
use datafusion::logical_expr::{LogicalPlan, Expr};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

/// Planning context used when converting SQL AST into a LookupJoinNode.
pub struct LookupJoinPlanner {
    /// Registry of known lookup tables (from catalog).
    lookup_tables: HashMap<String, LookupTableMetadata>,
}

impl LookupJoinPlanner {
    /// Create a new planner with access to the catalog of lookup tables.
    pub fn new(lookup_tables: HashMap<String, LookupTableMetadata>) -> Self {
        Self { lookup_tables }
    }

    /// Check if a table name refers to a registered lookup table.
    pub fn is_lookup_table(&self, table_name: &str) -> bool {
        self.lookup_tables.contains_key(table_name)
    }

    /// Attempt to rewrite a standard Join node into a LookupJoinNode.
    ///
    /// Returns `Some(LookupJoinNode)` if the join matches the temporal
    /// lookup pattern:
    /// 1. One side is a streaming source
    /// 2. The other side is a registered lookup table
    /// 3. The join uses temporal syntax (FOR SYSTEM_TIME AS OF NOW())
    ///
    /// Returns `None` if the join is not a lookup join.
    pub fn try_rewrite_join(
        &self,
        left: &LogicalPlan,
        right: &LogicalPlan,
        on: &[(Expr, Expr)],
        join_type: &datafusion::logical_expr::JoinType,
        filter: Option<&Expr>,
    ) -> Option<LookupJoinNode> {
        // Detect which side is the lookup table
        let (stream_input, lookup_name, join_keys) =
            self.detect_lookup_side(left, right, on)?;

        let metadata = self.lookup_tables.get(&lookup_name)?;

        let lookup_join_type = match join_type {
            datafusion::logical_expr::JoinType::Inner => LookupJoinType::Inner,
            datafusion::logical_expr::JoinType::Left => LookupJoinType::LeftOuter,
            _ => return None, // Unsupported join type for lookup
        };

        // Build output schema as union of stream + lookup schemas
        let output_schema = self.build_output_schema(stream_input, &lookup_name)?;

        // Initially, all predicates are local (F-LSQL-003 will split them later)
        let local_predicates = filter.map(|f| vec![f.clone()]).unwrap_or_default();

        // Required columns: initially all lookup columns (column pruning will narrow)
        let required_lookup_columns: HashSet<String> = metadata
            .primary_key
            .iter()
            .cloned()
            .collect();

        Some(LookupJoinNode {
            stream_input: stream_input.clone(),
            lookup_table: lookup_name,
            lookup_schema: self.get_lookup_schema(&lookup_name)?,
            join_keys,
            join_type: lookup_join_type,
            pushdown_predicates: Vec::new(),
            local_predicates,
            required_lookup_columns,
            output_schema,
            lookup_metadata: metadata.clone(),
        })
    }

    /// Detect which side of the join is the lookup table.
    fn detect_lookup_side(
        &self,
        left: &LogicalPlan,
        right: &LogicalPlan,
        on: &[(Expr, Expr)],
    ) -> Option<(&LogicalPlan, String, Vec<JoinKeyPair>)> {
        // Check if right side is a lookup table (most common: stream JOIN lookup)
        if let Some(name) = self.extract_table_name(right) {
            if self.is_lookup_table(&name) {
                let keys = on
                    .iter()
                    .map(|(l, r)| JoinKeyPair {
                        stream_expr: l.clone(),
                        lookup_column: self.extract_column_name(r),
                    })
                    .collect();
                return Some((left, name, keys));
            }
        }

        // Check if left side is a lookup table (less common)
        if let Some(name) = self.extract_table_name(left) {
            if self.is_lookup_table(&name) {
                let keys = on
                    .iter()
                    .map(|(l, r)| JoinKeyPair {
                        stream_expr: r.clone(),
                        lookup_column: self.extract_column_name(l),
                    })
                    .collect();
                return Some((right, name, keys));
            }
        }

        None
    }

    fn extract_table_name(&self, plan: &LogicalPlan) -> Option<String> {
        match plan {
            LogicalPlan::TableScan(scan) => Some(scan.table_name.to_string()),
            _ => None,
        }
    }

    fn extract_column_name(&self, expr: &Expr) -> String {
        match expr {
            Expr::Column(col) => col.name.clone(),
            _ => expr.to_string(),
        }
    }

    fn get_lookup_schema(&self, _table_name: &str) -> Option<DFSchemaRef> {
        // Look up from catalog
        None // placeholder
    }

    fn build_output_schema(
        &self,
        _stream: &LogicalPlan,
        _lookup: &str,
    ) -> Option<DFSchemaRef> {
        // Merge stream schema + lookup schema
        None // placeholder
    }
}

/// DataFusion optimizer rule that rewrites standard joins involving
/// lookup tables into LookupJoinNode nodes.
pub struct LookupJoinRewriteRule {
    planner: LookupJoinPlanner,
}

impl LookupJoinRewriteRule {
    pub fn new(planner: LookupJoinPlanner) -> Self {
        Self { planner }
    }
}

impl OptimizerRule for LookupJoinRewriteRule {
    fn name(&self) -> &str {
        "lookup_join_rewrite"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::error::Result<Option<LogicalPlan>> {
        // Walk the plan tree looking for Join nodes where one side
        // is a lookup table. Replace with LookupJoinNode.
        match plan {
            LogicalPlan::Join(join) => {
                if let Some(lookup_node) = self.planner.try_rewrite_join(
                    &join.left,
                    &join.right,
                    &join.on,
                    &join.join_type,
                    join.filter.as_ref(),
                ) {
                    Ok(Some(LogicalPlan::Extension(
                        datafusion::logical_expr::Extension {
                            node: Arc::new(lookup_node),
                        },
                    )))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }
}

/// DataFusion optimizer rule for column pruning on lookup joins.
///
/// Narrows the set of columns fetched from the lookup table to only
/// those referenced in the query's SELECT, WHERE, and JOIN ON clauses.
pub struct LookupColumnPruningRule;

impl OptimizerRule for LookupColumnPruningRule {
    fn name(&self) -> &str {
        "lookup_column_pruning"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::error::Result<Option<LogicalPlan>> {
        if let LogicalPlan::Extension(ext) = plan {
            if let Some(lookup) = ext.node.as_any().downcast_ref::<LookupJoinNode>() {
                // Analyze parent plan to determine which lookup columns are needed
                let needed = self.compute_needed_columns(plan)?;
                if needed != lookup.required_lookup_columns {
                    let mut pruned = lookup.clone();
                    pruned.required_lookup_columns = needed;
                    return Ok(Some(LogicalPlan::Extension(
                        datafusion::logical_expr::Extension {
                            node: Arc::new(pruned),
                        },
                    )));
                }
            }
        }
        Ok(None)
    }
}

impl LookupColumnPruningRule {
    fn compute_needed_columns(
        &self,
        _plan: &LogicalPlan,
    ) -> datafusion::error::Result<HashSet<String>> {
        // Walk parent expressions and collect all column references
        // that belong to the lookup table
        Ok(HashSet::new()) // placeholder
    }
}
```

### Algorithm/Flow

#### Plan Rewrite Flow

```
Input: SELECT t.price, i.currency FROM trades t JOIN instruments i ON t.isin = i.isin

1. DataFusion parser produces standard LogicalPlan:
   Projection [t.price, i.currency]
     Join (Inner, on: t.isin = i.isin)
       TableScan (trades)
       TableScan (instruments)

2. LookupJoinRewriteRule fires:
   a. Visit Join node
   b. Check if either side is a registered lookup table
   c. "instruments" is a lookup table → rewrite
   d. Extract join keys: (t.isin, i.isin)
   e. Determine join type: Inner
   f. Build LookupJoinNode

3. Resulting plan:
   Projection [t.price, i.currency]
     LookupJoin (table=instruments, keys=[t.isin=i.isin], type=Inner)
       TableScan (trades)

4. LookupColumnPruningRule fires:
   a. Projection only needs i.currency
   b. Join key needs i.isin
   c. Set required_lookup_columns = {isin, currency}

5. PredicateSplitter (F-LSQL-003) fires:
   a. Split WHERE predicates into pushdown vs local
   b. Attach pushable predicates to LookupJoinNode.pushdown_predicates
```

#### FOR SYSTEM_TIME AS OF NOW() Detection

```
The temporal join syntax is detected during SQL-to-plan conversion:

SQL: JOIN instruments FOR SYSTEM_TIME AS OF NOW() ON ...

1. Parser recognizes FOR SYSTEM_TIME AS OF as a temporal clause
2. NOW() indicates "current time" -- this is a streaming temporal join
3. The planner marks this as a temporal lookup join (not a time-travel query)
4. Standard join node is tagged with temporal metadata
5. LookupJoinRewriteRule recognizes the temporal tag and rewrites

For point-in-time lookups (not yet supported):
  FOR SYSTEM_TIME AS OF trades.event_time
  Would perform historical lookups, requiring versioned state.
  Deferred to a future feature.
```

#### Physical Plan Conversion

```
When the query engine converts logical plan to physical plan:

1. LookupJoinNode is converted to LookupJoinExec:
   a. Resolve lookup table runtime from catalog
   b. Create LookupJoinOperator with:
      - Join keys (for point lookup in cache/source)
      - Pushdown predicates (sent to source if supported)
      - Required columns (projection pushdown)
      - Join type (inner/left outer)
   c. Wire stream input as the driving side

2. At execution time, for each stream event:
   a. Extract join key values from stream event
   b. Lookup key in lookup table (cache first, then source)
   c. If found: emit joined row
   d. If not found and Inner join: skip
   e. If not found and LeftOuter: emit with NULL lookup columns
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `PlanError::LookupTableNotFound` | Table in JOIN is not registered as lookup table | Return plan error; user should CREATE LOOKUP TABLE first |
| `PlanError::UnsupportedJoinType` | RIGHT or FULL OUTER join with lookup table | Return error; only Inner and LeftOuter supported |
| `PlanError::NoJoinKeys` | ON clause is empty or does not reference lookup columns | Return error; lookup joins require explicit join keys |
| `PlanError::SchemaConflict` | Column name collision between stream and lookup table | Require alias qualification; suggest using AS |
| `PlanError::TemporalClauseRequired` | Join against lookup table without FOR SYSTEM_TIME | Warning; treat as temporal join by default for lookup tables |
| `PlanError::InvalidJoinKey` | Join key expression is not a simple column reference | Return error; only column references supported as join keys |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Plan rewrite time (single join) | < 100us | `bench_plan_rewrite_single_join` |
| Plan rewrite time (3 joins in query) | < 500us | `bench_plan_rewrite_multi_join` |
| Column pruning pass | < 50us | `bench_column_pruning` |
| Physical plan conversion | < 200us | `bench_physical_conversion` |
| Lookup join execution (cache hit) | < 1us per row | `bench_lookup_join_cache_hit` |
| Lookup join execution (cache miss) | < 5ms per row | `bench_lookup_join_cache_miss` |

## Test Plan

### Unit Tests

- [ ] `test_lookup_join_node_name` - Returns "LookupJoin"
- [ ] `test_lookup_join_node_inputs` - Single input (stream side)
- [ ] `test_lookup_join_node_schema` - Output schema is stream + lookup
- [ ] `test_lookup_join_node_expressions` - Returns join keys + predicates
- [ ] `test_lookup_join_node_explain` - EXPLAIN output format
- [ ] `test_lookup_join_node_with_exprs_and_inputs` - Reconstruction works
- [ ] `test_rewrite_rule_detects_lookup_table` - Standard join rewritten
- [ ] `test_rewrite_rule_ignores_non_lookup_table` - Regular join unchanged
- [ ] `test_rewrite_rule_detects_lookup_on_right` - stream JOIN lookup pattern
- [ ] `test_rewrite_rule_detects_lookup_on_left` - lookup JOIN stream pattern
- [ ] `test_rewrite_rule_extracts_join_keys` - ON clause parsed correctly
- [ ] `test_rewrite_rule_multi_column_join_keys` - Composite join key
- [ ] `test_rewrite_rule_inner_join_type` - Inner join preserved
- [ ] `test_rewrite_rule_left_outer_join_type` - Left outer join preserved
- [ ] `test_rewrite_rule_rejects_right_outer` - Right outer returns None
- [ ] `test_rewrite_rule_rejects_full_outer` - Full outer returns None
- [ ] `test_column_pruning_narrows_columns` - Only needed columns in set
- [ ] `test_column_pruning_includes_join_key` - Join key always included
- [ ] `test_column_pruning_includes_predicate_columns` - Filter columns included

### Integration Tests

- [ ] `test_end_to_end_lookup_join_plan` - Parse SQL, plan, verify LookupJoinNode
- [ ] `test_lookup_join_with_where_clause` - WHERE clause attached to node
- [ ] `test_lookup_join_column_pruning_integration` - Full optimizer pass prunes columns
- [ ] `test_lookup_join_explain_output` - EXPLAIN PLAN shows LookupJoin node
- [ ] `test_lookup_join_with_aggregation` - SELECT SUM(...) FROM stream JOIN lookup GROUP BY
- [ ] `test_multiple_lookup_joins_in_query` - Two lookup tables joined in one query

### Benchmarks

- [ ] `bench_plan_rewrite_single_join` - Target: < 100us
- [ ] `bench_plan_rewrite_multi_join` - Target: < 500us
- [ ] `bench_column_pruning` - Target: < 50us
- [ ] `bench_physical_conversion` - Target: < 200us

## Rollout Plan

1. **Phase 1**: Define `LookupJoinNode` struct implementing `UserDefinedLogicalNodeCore`
2. **Phase 2**: Implement `LookupJoinRewriteRule` optimizer rule
3. **Phase 3**: Implement join side detection and key extraction
4. **Phase 4**: Implement `LookupColumnPruningRule`
5. **Phase 5**: Register rules in DataFusion optimizer pipeline
6. **Phase 6**: Implement physical plan conversion (LookupJoinExec skeleton)
7. **Phase 7**: Unit tests for rewrite rule and node behavior
8. **Phase 8**: Integration tests with full SQL-to-plan pipeline
9. **Phase 9**: Benchmarks and optimization
10. **Phase 10**: Documentation and code review

## Open Questions

- [ ] Should `FOR SYSTEM_TIME AS OF NOW()` be required for lookup joins, or should the planner automatically detect lookup tables and use temporal semantics? Flink requires explicit temporal syntax; implicit detection is more user-friendly but less explicit.
- [ ] Should the lookup join support `FOR SYSTEM_TIME AS OF stream.event_time` for historical point-in-time lookups? This requires versioned lookup tables (CDC with full history).
- [ ] How should the LookupJoinNode interact with DataFusion's join reordering rules? Currently, we want the stream side to always be the driving side; reordering could break this.
- [ ] Should there be a cost model for the optimizer to choose between lookup join and broadcast hash join (for small lookup tables)?
- [ ] How to handle the case where the lookup table is not yet populated (cold start)? Should the join block, return NULLs, or fail?

## Completion Checklist

- [ ] `LookupJoinNode` implementing `UserDefinedLogicalNodeCore`
- [ ] `JoinKeyPair` and `LookupJoinType` types
- [ ] `LookupTableMetadata` carried on the node
- [ ] `LookupJoinRewriteRule` optimizer rule
- [ ] `LookupColumnPruningRule` optimizer rule
- [ ] `LookupJoinPlanner` with join detection and rewrite logic
- [ ] Physical plan conversion skeleton (`LookupJoinExec`)
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests with full SQL-to-plan pipeline
- [ ] Benchmarks meet targets
- [ ] EXPLAIN output displays LookupJoin node correctly
- [ ] Documentation updated (`#![deny(missing_docs)]` clean)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [DataFusion UserDefinedLogicalNode](https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.UserDefinedLogicalNodeCore.html) -- Extension point for custom plan nodes
- [Apache Flink Temporal Joins](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/joins/#temporal-joins) -- Flink's temporal join semantics
- [SQL:2011 Temporal](https://en.wikipedia.org/wiki/SQL:2011) -- FOR SYSTEM_TIME AS OF syntax origin
- [F-LSQL-001: CREATE LOOKUP TABLE](./F-LSQL-001-create-lookup-table.md) -- DDL that registers lookup tables
- [F-LSQL-003: Predicate Splitting](./F-LSQL-003-predicate-splitting.md) -- Predicate pushdown for lookup joins
- [F005B: Advanced DataFusion](../../phase-2/F005B-advanced-datafusion-integration.md) -- DataFusion integration
