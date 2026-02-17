# F-LSQL-003: Predicate Splitting & Pushdown Planning

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-LSQL-003 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 6a |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-LSQL-002 (Lookup Join Plan Node), F-LOOKUP-003 (Source Capabilities) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-sql` |
| **Module** | `laminar-sql/src/planner/predicate_split.rs` |

## Summary

Implements a DataFusion optimizer rule that splits WHERE clause predicates in lookup join queries into two categories: predicates that can be pushed down to the lookup source (reducing data transferred from the external system) and predicates that must be evaluated locally (because they reference stream columns or cross-reference both stream and lookup columns). The optimizer inspects each predicate's expression tree, classifies its column references, and attaches pushable predicates to the `LookupJoinNode`'s `pushdown_predicates` field while keeping non-pushable predicates in `local_predicates`. This optimization can dramatically reduce lookup join latency and network transfer, especially for large lookup tables with selective predicates.

## Goals

- Implement `PredicateSplitterRule` as a DataFusion `OptimizerRule`
- Classify each predicate into: pushable (only lookup columns), local (stream or cross-reference)
- Attach pushable predicates to `LookupJoinNode.pushdown_predicates`
- Keep non-pushable predicates in `LookupJoinNode.local_predicates`
- Respect source capabilities: skip pushdown for sources that do not support predicate filtering
- Support conjunctive splitting: break AND-ed predicates into individual terms for finer classification
- Handle nested expressions: walk the full expression tree to find all column references
- Provide EXPLAIN output showing which predicates are pushed down

## Non-Goals

- Predicate pushdown into stream sources (handled by DataFusion's built-in filter pushdown)
- OR predicate splitting (complex; deferred to a future optimization)
- Expression rewriting or simplification (handled by DataFusion's constant folding/simplification rules)
- Dynamic pushdown based on runtime statistics (this is a static planning optimization)
- Cross-node predicate routing (handled at execution layer)

## Technical Design

### Architecture

**Ring**: N/A (query planning is a control plane operation)

**Crate**: `laminar-sql`

**Module**: `laminar-sql/src/planner/predicate_split.rs`

The `PredicateSplitterRule` runs as part of DataFusion's optimizer rule chain, after the `LookupJoinRewriteRule` (F-LSQL-002) has created `LookupJoinNode` nodes. It visits each `LookupJoinNode`, examines the predicates (from WHERE and ON clauses), classifies them, and updates the node's pushdown and local predicate lists. The rule also consults the `SourceCapabilities` registry to determine whether the lookup source actually supports predicate pushdown.

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Query:                                                                   │
│  SELECT t.symbol, t.price, c.name                                        │
│  FROM trades t                                                           │
│  JOIN customers c FOR SYSTEM_TIME AS OF NOW() ON t.customer_id = c.id    │
│  WHERE c.region = 'APAC'                                                 │
│    AND c.credit_limit > 1000000                                          │
│    AND t.volume > c.min_order_size                                       │
│    AND t.price > 100                                                     │
│                                                                           │
│  PREDICATE CLASSIFICATION:                                               │
│                                                                           │
│  c.region = 'APAC'              → PUSHABLE (only lookup columns)         │
│  c.credit_limit > 1000000       → PUSHABLE (only lookup columns)         │
│  t.volume > c.min_order_size    → LOCAL (cross-reference: stream+lookup) │
│  t.price > 100                  → LOCAL (only stream columns, but        │
│                                    DataFusion handles stream-side push)   │
│                                                                           │
│  RESULT:                                                                  │
│  LookupJoinNode {                                                        │
│    pushdown_predicates: [c.region = 'APAC', c.credit_limit > 1000000]   │
│    local_predicates: [t.volume > c.min_order_size, t.price > 100]       │
│  }                                                                        │
└──────────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use std::collections::HashSet;
use std::sync::Arc;

/// DataFusion optimizer rule that splits predicates in lookup join queries
/// into pushable (sent to lookup source) and local (evaluated after join).
///
/// This rule runs AFTER the LookupJoinRewriteRule and BEFORE physical
/// planning. It modifies the LookupJoinNode in-place by moving
/// qualifying predicates from `local_predicates` to `pushdown_predicates`.
pub struct PredicateSplitterRule {
    /// Registry of source capabilities for pushdown decisions.
    capabilities: Arc<SourceCapabilitiesRegistry>,
}

impl PredicateSplitterRule {
    /// Create a new predicate splitter with access to source capabilities.
    pub fn new(capabilities: Arc<SourceCapabilitiesRegistry>) -> Self {
        Self { capabilities }
    }
}

impl OptimizerRule for PredicateSplitterRule {
    fn name(&self) -> &str {
        "predicate_splitter"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::error::Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Extension(ext) => {
                if let Some(lookup) = ext.node.as_any().downcast_ref::<LookupJoinNode>() {
                    let optimized = self.split_predicates(lookup)?;
                    Ok(Some(LogicalPlan::Extension(
                        datafusion::logical_expr::Extension {
                            node: Arc::new(optimized),
                        },
                    )))
                } else {
                    Ok(None)
                }
            }
            // Also check for Filter nodes directly above a LookupJoinNode
            LogicalPlan::Filter(filter) => {
                if let LogicalPlan::Extension(ext) = filter.input.as_ref() {
                    if let Some(lookup) =
                        ext.node.as_any().downcast_ref::<LookupJoinNode>()
                    {
                        // Pull the filter predicate into the LookupJoinNode
                        let mut updated = lookup.clone();
                        let conjuncts = self.split_conjunction(&filter.predicate);
                        updated.local_predicates.extend(conjuncts);
                        let optimized = self.split_predicates(&updated)?;
                        Ok(Some(LogicalPlan::Extension(
                            datafusion::logical_expr::Extension {
                                node: Arc::new(optimized),
                            },
                        )))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }
}

impl PredicateSplitterRule {
    /// Split predicates into pushable and local categories.
    ///
    /// **AUDIT FIX (C7)**: Builds qualified column sets from table aliases
    /// and passes them to `PredicateClassifier` for unambiguous resolution.
    ///
    /// **AUDIT FIX (H10)**: For LEFT JOINs, only predicates from the ON
    /// clause (join condition) are eligible for pushdown. WHERE predicates
    /// on the lookup side of a LEFT JOIN filter NULL-extended rows, which
    /// changes semantics if pushed down (the source would never return the
    /// row, so the LEFT JOIN would produce NULL instead of filtering it).
    fn split_predicates(
        &self,
        lookup_node: &LookupJoinNode,
    ) -> datafusion::error::Result<LookupJoinNode> {
        // Get source capabilities for this lookup table
        let source_caps = self
            .capabilities
            .get(&lookup_node.lookup_table)
            .unwrap_or(&SourceCapabilities::NONE);

        // If source does not support pushdown at all, or pushdown is disabled, skip
        if !source_caps.predicate_pushdown
            || lookup_node.lookup_metadata.pushdown_mode == "disabled"
        {
            return Ok(lookup_node.clone());
        }

        // Get lookup table column names for classification (unqualified)
        let lookup_columns: HashSet<String> = lookup_node
            .lookup_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let stream_columns: HashSet<String> = lookup_node
            .stream_input
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        // AUDIT FIX (C7): Build qualified column sets from table aliases.
        // These enable unambiguous classification when both schemas have
        // columns with the same name (e.g., both have "id").
        let lookup_alias = lookup_node.lookup_alias.as_deref().unwrap_or("");
        let stream_alias = lookup_node.stream_alias.as_deref().unwrap_or("");

        let lookup_qualified: HashSet<String> = lookup_node
            .lookup_schema
            .fields()
            .iter()
            .map(|f| format!("{}.{}", lookup_alias, f.name()))
            .collect();

        let stream_qualified: HashSet<String> = lookup_node
            .stream_input
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}.{}", stream_alias, f.name()))
            .collect();

        // Classify each local predicate
        let mut pushable = lookup_node.pushdown_predicates.clone();
        let mut local = Vec::new();
        let classifier = PredicateClassifier::new(
            &lookup_columns,
            &stream_columns,
            &lookup_qualified,
            &stream_qualified,
        );

        // AUDIT FIX (H10): LEFT JOIN pushdown safety.
        // For LEFT JOINs, WHERE-clause predicates on the lookup side must
        // NOT be pushed down because they filter NULL-extended rows.
        // Only ON-clause (join condition) predicates are safe to push.
        //
        // Example:
        //   SELECT * FROM trades t LEFT JOIN customers c ON t.cid = c.id
        //   WHERE c.region = 'APAC'
        //
        // If we push `c.region = 'APAC'` to the source, customers not in
        // APAC won't be returned, but the LEFT JOIN should still produce
        // a row with NULLs for the customer columns. The WHERE clause
        // filters those NULL rows *after* the join, not during lookup.
        let is_left_join = lookup_node.join_type == JoinType::Left;

        for predicate in &lookup_node.local_predicates {
            // Split AND-ed predicates into individual terms
            let conjuncts = self.split_conjunction(predicate);
            for conjunct in conjuncts {
                let classification = classifier.classify(&conjunct);

                // H10: For LEFT JOIN, only push if the predicate came from
                // the ON clause (join_condition), not the WHERE clause.
                let is_from_where = !lookup_node.join_condition_predicates
                    .contains(&conjunct);

                let should_push = match classification {
                    PredicateClass::LookupOnly => {
                        if is_left_join && is_from_where {
                            // H10: WHERE predicate on lookup side of LEFT JOIN
                            // — do NOT push, it filters NULL-extended rows.
                            false
                        } else {
                            true
                        }
                    }
                    PredicateClass::StreamOnly | PredicateClass::CrossReference => false,
                };

                if should_push {
                    pushable.push(conjunct);
                } else {
                    local.push(conjunct);
                }
            }
        }

        Ok(LookupJoinNode {
            pushdown_predicates: pushable,
            local_predicates: local,
            ..lookup_node.clone()
        })
    }

    /// Split a conjunction (AND expression) into individual terms.
    ///
    /// `a AND b AND c` → `[a, b, c]`
    /// `a OR b` → `[a OR b]` (OR is not split)
    fn split_conjunction(&self, expr: &Expr) -> Vec<Expr> {
        match expr {
            Expr::BinaryExpr(binary)
                if binary.op == datafusion::logical_expr::Operator::And =>
            {
                let mut left = self.split_conjunction(&binary.left);
                let right = self.split_conjunction(&binary.right);
                left.extend(right);
                left
            }
            other => vec![other.clone()],
        }
    }
}

/// Classifies predicates by their column references.
///
/// **AUDIT FIX (C7)**: Added `lookup_qualified` and `stream_qualified` sets
/// for resolving qualified column references (e.g., `c.id` vs `t.id`).
/// Without these, columns with the same name in both schemas are always
/// classified as `CrossReference`, preventing pushdown even when the user
/// explicitly qualified the column with a table alias.
pub struct PredicateClassifier {
    /// Unqualified column names belonging to the lookup table.
    lookup_columns: HashSet<String>,
    /// Unqualified column names belonging to the stream.
    stream_columns: HashSet<String>,
    /// Qualified column names belonging to the lookup table (e.g., "c.region").
    lookup_qualified: HashSet<String>,
    /// Qualified column names belonging to the stream (e.g., "t.price").
    stream_qualified: HashSet<String>,
}

/// Classification result for a predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateClass {
    /// Predicate references ONLY lookup table columns.
    /// Can be pushed down to the lookup source.
    LookupOnly,
    /// Predicate references ONLY stream columns.
    /// Evaluated locally, but could be pushed to stream source by DataFusion.
    StreamOnly,
    /// Predicate references BOTH stream and lookup columns.
    /// Must be evaluated locally after the join.
    CrossReference,
}
```

### Data Structures

```rust
impl PredicateClassifier {
    /// Create a new classifier with known column sets.
    ///
    /// **AUDIT FIX (C7)**: Now accepts qualified column sets in addition
    /// to unqualified. Qualified names are "table_alias.column" format,
    /// built from the lookup table alias and stream table alias.
    pub fn new(
        lookup_columns: &HashSet<String>,
        stream_columns: &HashSet<String>,
        lookup_qualified: &HashSet<String>,
        stream_qualified: &HashSet<String>,
    ) -> Self {
        Self {
            lookup_columns: lookup_columns.clone(),
            stream_columns: stream_columns.clone(),
            lookup_qualified: lookup_qualified.clone(),
            stream_qualified: stream_qualified.clone(),
        }
    }

    /// Classify a predicate expression by walking its expression tree.
    ///
    /// Collects all column references and determines whether they
    /// belong to the lookup table, stream, or both.
    pub fn classify(&self, expr: &Expr) -> PredicateClass {
        let mut has_lookup = false;
        let mut has_stream = false;
        self.walk_columns(expr, &mut has_lookup, &mut has_stream);

        match (has_lookup, has_stream) {
            (true, false) => PredicateClass::LookupOnly,
            (false, true) => PredicateClass::StreamOnly,
            (true, true) => PredicateClass::CrossReference,
            (false, false) => {
                // Constant expression (e.g., 1 = 1). Treat as pushable.
                PredicateClass::LookupOnly
            }
        }
    }

    /// Recursively walk an expression tree and collect column reference flags.
    ///
    /// **AUDIT FIX (C7)**: Complete qualified column resolution. When a column
    /// has a `relation` qualifier (e.g., `c.id`), we check the `lookup_qualified`
    /// and `stream_qualified` sets FIRST, using the qualified name to disambiguate.
    /// Only falls back to unqualified matching when no qualifier is present.
    /// This ensures `WHERE c.id = 5` is correctly classified as LookupOnly
    /// even when both schemas have an `id` column.
    fn walk_columns(&self, expr: &Expr, has_lookup: &mut bool, has_stream: &mut bool) {
        match expr {
            Expr::Column(col) => {
                if let Some(ref qualifier) = col.relation {
                    // AUDIT FIX (C7): Qualified column — use qualifier to
                    // determine ownership unambiguously.
                    let qualified_name = format!("{}.{}", qualifier, col.name);
                    if self.lookup_qualified.contains(&qualified_name) {
                        *has_lookup = true;
                    } else if self.stream_qualified.contains(&qualified_name) {
                        *has_stream = true;
                    } else {
                        // Unknown qualifier (e.g., subquery alias).
                        // Conservative fallback: mark as cross-reference.
                        *has_lookup = true;
                        *has_stream = true;
                    }
                } else {
                    // Unqualified column — check if ambiguous
                    let in_lookup = self.lookup_columns.contains(&col.name);
                    let in_stream = self.stream_columns.contains(&col.name);

                    match (in_lookup, in_stream) {
                        (true, false) => *has_lookup = true,
                        (false, true) => *has_stream = true,
                        (true, true) => {
                            // Ambiguous: column exists in both schemas.
                            // This should have been caught by DataFusion's
                            // ambiguity check. Classify as CrossReference
                            // (safe fallback — prevents incorrect pushdown).
                            *has_lookup = true;
                            *has_stream = true;
                        }
                        (false, false) => {
                            // Unknown column — may be from a subquery or alias.
                            // Conservative: treat as CrossReference.
                            *has_lookup = true;
                            *has_stream = true;
                        }
                    }
                }
            }
            Expr::BinaryExpr(binary) => {
                self.walk_columns(&binary.left, has_lookup, has_stream);
                self.walk_columns(&binary.right, has_lookup, has_stream);
            }
            Expr::Not(inner) => {
                self.walk_columns(inner, has_lookup, has_stream);
            }
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                self.walk_columns(inner, has_lookup, has_stream);
            }
            Expr::Between(between) => {
                self.walk_columns(&between.expr, has_lookup, has_stream);
                self.walk_columns(&between.low, has_lookup, has_stream);
                self.walk_columns(&between.high, has_lookup, has_stream);
            }
            Expr::InList(in_list) => {
                self.walk_columns(&in_list.expr, has_lookup, has_stream);
                for item in &in_list.list {
                    self.walk_columns(item, has_lookup, has_stream);
                }
            }
            Expr::ScalarFunction(func) => {
                for arg in &func.args {
                    self.walk_columns(arg, has_lookup, has_stream);
                }
            }
            Expr::Cast(cast) => {
                self.walk_columns(&cast.expr, has_lookup, has_stream);
            }
            Expr::Literal(_) => {
                // Literals do not reference any columns
            }
            _ => {
                // For other expression types, conservatively mark as cross-reference
                // if we cannot determine the column ownership
                *has_lookup = true;
                *has_stream = true;
            }
        }
    }
}

/// Registry of source capabilities for lookup tables.
///
/// Used by the predicate splitter to determine whether a lookup source
/// supports predicate pushdown, projection pushdown, or other optimizations.
pub struct SourceCapabilitiesRegistry {
    /// Map from lookup table name to capabilities.
    capabilities: std::collections::HashMap<String, SourceCapabilities>,
}

impl SourceCapabilitiesRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            capabilities: std::collections::HashMap::new(),
        }
    }

    /// Register capabilities for a lookup table's source.
    pub fn register(&mut self, table_name: impl Into<String>, caps: SourceCapabilities) {
        self.capabilities.insert(table_name.into(), caps);
    }

    /// Get capabilities for a lookup table.
    pub fn get(&self, table_name: &str) -> Option<&SourceCapabilities> {
        self.capabilities.get(table_name)
    }
}

/// Capabilities of a lookup source for query optimization.
///
/// These capabilities are probed at pipeline startup when pushdown='auto',
/// or assumed from connector type when pushdown='enabled'.
#[derive(Debug, Clone)]
pub struct SourceCapabilities {
    /// Whether the source supports predicate pushdown (WHERE clause filtering).
    pub predicate_pushdown: bool,
    /// Whether the source supports projection pushdown (SELECT column subset).
    pub projection_pushdown: bool,
    /// The mechanism used for predicate pushdown (for logging/debugging).
    pub pushdown_mechanism: PushdownMechanism,
    /// Maximum number of predicates that can be pushed simultaneously.
    pub max_pushdown_predicates: Option<usize>,
    /// Supported predicate types (equality, comparison, IN list, LIKE, etc.).
    pub supported_predicate_types: Vec<PredicateType>,
}

impl SourceCapabilities {
    /// No pushdown capabilities.
    pub const NONE: SourceCapabilities = SourceCapabilities {
        predicate_pushdown: false,
        projection_pushdown: false,
        pushdown_mechanism: PushdownMechanism::None,
        max_pushdown_predicates: None,
        supported_predicate_types: Vec::new(),
    };
}

/// Mechanism by which predicates are pushed to the source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushdownMechanism {
    /// No pushdown supported.
    None,
    /// Predicates are converted to SQL WHERE clause (Postgres, MySQL).
    ParameterizedSql,
    /// Predicates are converted to key-based lookups (Redis: GET by key only).
    KeyLookup,
    /// Predicates are used for row group pruning (Parquet: min/max statistics).
    RowGroupPruning,
    /// Predicates are evaluated by local filtering on cached data.
    LocalFilter,
}

/// Types of predicates that a source can accept for pushdown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateType {
    /// Equality: column = value
    Equality,
    /// Comparison: column > value, column < value, etc.
    Comparison,
    /// Range: column BETWEEN low AND high
    Range,
    /// IN list: column IN (v1, v2, v3)
    InList,
    /// Pattern matching: column LIKE 'pattern%'
    Like,
    /// NULL checks: column IS NULL, column IS NOT NULL
    IsNull,
}
```

### Algorithm/Flow

#### Predicate Classification Algorithm

```
AUDIT FIX (C7): Updated to use qualified column resolution.

Input: Expression tree for a single predicate

classify(expr):
  has_lookup = false
  has_stream = false
  walk_columns(expr, &mut has_lookup, &mut has_stream)

  walk_columns(expr, has_lookup, has_stream):
    match expr:
      Column(col):
        if col.relation is Some(qualifier):
          // AUDIT FIX (C7): Qualified column — use qualifier to
          // determine ownership unambiguously.
          qualified = "{qualifier}.{col.name}"
          if qualified in lookup_qualified: has_lookup = true
          else if qualified in stream_qualified: has_stream = true
          else: has_lookup = true; has_stream = true  // unknown qualifier
        else:
          // Unqualified — check for ambiguity
          in_lookup = col.name in lookup_columns
          in_stream = col.name in stream_columns
          if in_lookup and not in_stream: has_lookup = true
          if in_stream and not in_lookup: has_stream = true
          if in_lookup and in_stream: has_lookup = true; has_stream = true  // ambiguous
          if not in_lookup and not in_stream: has_lookup = true; has_stream = true  // unknown

      BinaryExpr(left, op, right):
        walk_columns(left, has_lookup, has_stream)
        walk_columns(right, has_lookup, has_stream)
      Literal(_):
        // no columns, no effect
      ScalarFunction(args):
        for arg in args: walk_columns(arg, has_lookup, has_stream)
      other:
        // conservative: mark both
        has_lookup = true; has_stream = true

  return:
    (true, false)  → LookupOnly   (pushable)
    (false, true)  → StreamOnly   (local)
    (true, true)   → CrossReference (local)
    (false, false)  → LookupOnly   (constant, safe to push)
```

#### Full Optimization Pass

```
1. PredicateSplitterRule visits each node in the logical plan
2. For each LookupJoinNode found:
   a. Check if a Filter node sits directly above it
      → If so, pull Filter predicates into the LookupJoinNode's local list
   b. Get source capabilities for the lookup table
      → If predicate_pushdown = false, skip (all predicates stay local)
      → If pushdown_mode = 'disabled', skip
   c. AUDIT FIX (C7): Build qualified column sets from table aliases
      → lookup_qualified: {"c.id", "c.region", ...}
      → stream_qualified: {"t.id", "t.price", ...}
   d. For each predicate in local_predicates:
      i. Split conjunctions: (A AND B AND C) → [A, B, C]
      ii. Classify each conjunct using qualified + unqualified resolution:
          - LookupOnly → candidate for pushdown
          - StreamOnly or CrossReference → keep in local_predicates
   e. AUDIT FIX (H10): LEFT JOIN safety check
      → If join_type == LEFT and predicate is from WHERE (not ON clause):
        Do NOT push lookup-only predicates — they filter NULL-extended rows
      → If join_type == INNER or predicate is from ON clause:
        Push normally
   f. Validate pushdown predicates against source capabilities:
      - Check max_pushdown_predicates limit
      - Check supported predicate types
      - Demote unsupported predicates back to local
   g. Return updated LookupJoinNode
```

#### Source Pushdown Capabilities Reference

| Source | Predicate Pushdown | Projection Pushdown | Mechanism |
|---|---|---|---|
| Postgres (direct query) | Full SQL WHERE | SELECT columns | Parameterized SQL |
| Postgres (CDC cache) | No | Selective deserialization | Local filtering on cache |
| MySQL (direct query) | Full SQL WHERE | SELECT columns | Parameterized SQL |
| MySQL (CDC cache) | No | Selective deserialization | Local filtering on cache |
| Redis | No | No | GET by key only (join key lookup) |
| S3 Parquet | Row group pruning via min/max stats | Column pruning | arrow-rs reader |
| Static (in-memory) | No (full scan is fast enough) | No | Direct HashMap lookup |

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `PlanError::ClassificationFailed` | Unknown expression type in predicate tree | Conservative fallback: classify as CrossReference (keep local) |
| `PlanError::AmbiguousColumn` | Column name exists in both stream and lookup schemas without qualifier | AUDIT FIX (C7): Now classified as CrossReference (safe fallback — prevents incorrect pushdown). DataFusion's own ambiguity check should catch this during planning. |
| `PlanError::PushdownNotSupported` | Pushdown mode is 'enabled' but source does not support it | Return planning error (user explicitly requested pushdown) |
| `PlanError::TooManyPushdownPredicates` | More pushable predicates than source max_pushdown_predicates | Keep excess predicates in local_predicates; log warning |
| `PlanError::UnsupportedPredicateType` | Predicate type (e.g., LIKE) not supported by source | Demote predicate to local evaluation; log info |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Predicate classification time (single predicate) | < 1us | `bench_classify_single_predicate` |
| Full splitting pass (10 predicates) | < 20us | `bench_split_10_predicates` |
| Conjunctive splitting (deeply nested AND) | < 5us | `bench_split_conjunction` |
| Expression tree walk depth (10 levels) | < 2us | `bench_walk_deep_expression` |
| Pushdown reduction in lookup data transfer | > 80% for selective predicates | Integration benchmark |

## Test Plan

### Unit Tests

- [ ] `test_classify_lookup_only_equality` - `c.region = 'APAC'` is LookupOnly
- [ ] `test_classify_lookup_only_comparison` - `c.credit_limit > 1000000` is LookupOnly
- [ ] `test_classify_stream_only` - `t.price > 100` is StreamOnly
- [ ] `test_classify_cross_reference` - `t.volume > c.min_order_size` is CrossReference
- [ ] `test_classify_constant_expression` - `1 = 1` is LookupOnly (constant)
- [ ] `test_classify_nested_function_lookup_only` - `UPPER(c.region) = 'APAC'` is LookupOnly
- [ ] `test_classify_nested_function_cross_ref` - `t.price * c.rate > 100` is CrossReference
- [ ] `test_classify_in_list_lookup_only` - `c.status IN ('A', 'B')` is LookupOnly
- [ ] `test_classify_between_lookup_only` - `c.credit BETWEEN 1000 AND 5000` is LookupOnly
- [ ] `test_classify_is_null_lookup_only` - `c.email IS NOT NULL` is LookupOnly
- [ ] `test_classify_or_expression_mixed` - `c.region = 'APAC' OR t.price > 100` is CrossReference
- [ ] `test_split_conjunction_flat` - `A AND B AND C` → `[A, B, C]`
- [ ] `test_split_conjunction_nested` - `(A AND B) AND C` → `[A, B, C]`
- [ ] `test_split_conjunction_single` - `A` → `[A]`
- [ ] `test_split_conjunction_or_not_split` - `A OR B` → `[A OR B]`
- [ ] `test_splitter_moves_pushable_predicates` - Pushable predicates moved to pushdown list
- [ ] `test_splitter_keeps_local_predicates` - Non-pushable predicates stay in local list
- [ ] `test_splitter_respects_source_capabilities` - No pushdown when source doesn't support it
- [ ] `test_splitter_respects_pushdown_disabled` - No pushdown when mode is disabled
- [ ] `test_splitter_pulls_filter_from_above` - Filter node above LookupJoinNode absorbed
- [ ] `test_splitter_handles_qualified_columns` - `t.id` vs `c.id` correctly classified
- [ ] `test_splitter_max_pushdown_limit` - Excess predicates demoted to local
- [ ] `test_source_capabilities_postgres_direct` - Full predicate and projection pushdown
- [ ] `test_source_capabilities_redis` - No predicate pushdown
- [ ] `test_source_capabilities_s3_parquet` - Row group pruning support
- [ ] `test_classify_qualified_column_lookup_only` - (C7) `c.id = 5` with qualifier resolves to LookupOnly even if both schemas have `id`
- [ ] `test_classify_qualified_column_stream_only` - (C7) `t.id = 5` with qualifier resolves to StreamOnly
- [ ] `test_classify_ambiguous_unqualified_column` - (C7) `id = 5` without qualifier when both schemas have `id` → CrossReference
- [ ] `test_classify_unknown_qualifier_conservative` - (C7) `x.foo = 5` with unknown qualifier → CrossReference (conservative)
- [ ] `test_left_join_where_predicate_not_pushed` - (H10) LEFT JOIN with `WHERE c.region = 'APAC'` keeps predicate local
- [ ] `test_left_join_on_predicate_pushed` - (H10) LEFT JOIN with `ON c.id = t.cid AND c.active = true` pushes `c.active` from ON clause
- [ ] `test_inner_join_where_predicate_pushed` - (H10) INNER JOIN with `WHERE c.region = 'APAC'` pushes normally
- [ ] `test_left_join_mixed_predicates` - (H10) LEFT JOIN with ON and WHERE predicates — only ON predicates pushed

### Integration Tests

- [ ] `test_predicate_pushdown_end_to_end_postgres` - SQL with pushable predicate, verify SQL sent to Postgres includes WHERE
- [ ] `test_predicate_pushdown_reduces_data_transfer` - Measure bytes fetched with and without pushdown
- [ ] `test_predicate_split_with_multiple_lookup_joins` - Query with two lookup tables, independent pushdown
- [ ] `test_predicate_pushdown_auto_probes_capabilities` - Auto mode probes source at startup
- [ ] `test_explain_shows_pushdown_predicates` - EXPLAIN output shows pushdown vs local split
- [ ] `test_predicate_split_preserves_query_correctness` - Results identical with and without pushdown

### Benchmarks

- [ ] `bench_classify_single_predicate` - Target: < 1us
- [ ] `bench_split_10_predicates` - Target: < 20us
- [ ] `bench_split_conjunction` - Target: < 5us
- [ ] `bench_walk_deep_expression` - Target: < 2us
- [ ] `bench_pushdown_data_reduction` - Measure data transfer reduction with selective predicates

## Rollout Plan

1. **Phase 1**: Define `PredicateClassifier` with expression tree walking
2. **Phase 2**: Define `SourceCapabilities` and `SourceCapabilitiesRegistry`
3. **Phase 3**: Implement `PredicateSplitterRule` as DataFusion `OptimizerRule`
4. **Phase 4**: Implement conjunctive splitting for AND expressions
5. **Phase 5**: Wire capabilities for Postgres, Redis, S3 Parquet sources
6. **Phase 6**: Register rule in DataFusion optimizer pipeline (after LookupJoinRewriteRule)
7. **Phase 7**: Unit tests for classification and splitting
8. **Phase 8**: Integration tests with real lookup sources
9. **Phase 9**: Benchmarks measuring data transfer reduction
10. **Phase 10**: Documentation and code review

## Open Questions

- [ ] Should OR predicates be split when one branch is pushable and the other is not? For example, `c.region = 'APAC' OR c.region = 'EMEA'` is fully pushable, but `c.region = 'APAC' OR t.price > 100` is not. Currently, OR expressions are classified conservatively as CrossReference if any branch touches stream columns.
- [ ] Should the classifier handle subqueries in predicates (e.g., `c.id IN (SELECT id FROM ...)`)?  Currently, subqueries are classified as CrossReference (conservative).
- [ ] Should there be a cost model that decides whether pushdown is beneficial? For very simple predicates on small cached tables, the overhead of parameterized SQL may exceed local filtering cost.
- [x] ~~How to handle qualified column names when aliases are used?~~ **Resolved (AUDIT FIX C7)**: `PredicateClassifier` now maintains both `lookup_qualified` and `stream_qualified` sets containing "alias.column" strings. When a column has a `relation` qualifier, the qualified name is checked first. This handles `FROM customers AS c WHERE c.region = 'APAC'` correctly even when both schemas have overlapping column names.
- [ ] Should the capabilities probing in 'auto' mode be cached, or re-probed on each pipeline restart? Cached probing reduces startup latency but may miss source capability changes.

## Completion Checklist

- [ ] `PredicateClassifier` with expression tree walking
- [ ] `PredicateClass` enum (LookupOnly, StreamOnly, CrossReference)
- [ ] `PredicateSplitterRule` implementing `OptimizerRule`
- [ ] Conjunctive splitting for AND expressions
- [ ] `SourceCapabilities` and `SourceCapabilitiesRegistry`
- [ ] `PushdownMechanism` and `PredicateType` enums
- [ ] Source capabilities wired for Postgres, Redis, S3 Parquet
- [ ] Filter node absorption (Filter above LookupJoinNode)
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests with real sources
- [ ] Benchmarks meet targets
- [ ] EXPLAIN output shows pushdown split
- [ ] Documentation updated (`#![deny(missing_docs)]` clean)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [DataFusion Optimizer Rules](https://docs.rs/datafusion/latest/datafusion/optimizer/trait.OptimizerRule.html) -- Extension point for custom optimization
- [Apache Calcite Predicate Pushdown](https://calcite.apache.org/docs/algebra.html) -- Predicate pushdown in relational algebra
- [F-LSQL-002: Lookup Join Plan Node](./F-LSQL-002-lookup-join-plan-node.md) -- Plan node this rule optimizes
- [F-LSQL-001: CREATE LOOKUP TABLE](./F-LSQL-001-create-lookup-table.md) -- DDL defining lookup tables
- [F-LOOKUP-003: Source Capabilities](../lookup/F-LOOKUP-003-source-capabilities.md) -- Runtime source capability probing
- [ARCHITECTURE.md](../../../ARCHITECTURE.md) -- System design
