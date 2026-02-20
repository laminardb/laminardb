//! Predicate splitting and pushdown for lookup joins.
//!
//! This module classifies WHERE/ON predicates in lookup join queries and
//! splits them into pushdown vs local evaluation categories. It implements
//! a DataFusion optimizer rule (`PredicateSplitterRule`) that absorbs
//! filter nodes above `LookupJoinNode` and assigns each predicate to the
//! correct execution site.
//!
//! ## Key Safety Rules
//!
//! - **H10 (LEFT JOIN safety):** WHERE-clause predicates on lookup-only
//!   columns above a `LeftOuter` join must NOT be pushed down — doing so
//!   changes the semantics by filtering out NULL-extended rows.
//! - **C7 (qualified columns):** When aliases are present, `col.relation`
//!   is checked first for unambiguous resolution before falling back to
//!   unqualified column name matching.
//! - **`NotEq`** predicates are classified normally but are never pushed
//!   down (they cannot use equality indexes on the source).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::logical_expr::logical_plan::LogicalPlan;
use datafusion::logical_expr::{
    BinaryExpr, Expr, Extension, Filter, Operator as DfOperator, UserDefinedLogicalNodeCore,
};
use datafusion_common::tree_node::Transformed;
use datafusion_common::Result;
use datafusion_optimizer::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

use crate::datafusion::lookup_join::{LookupJoinNode, LookupJoinType};

// ---------------------------------------------------------------------------
// Predicate Classification
// ---------------------------------------------------------------------------

/// Classification of a predicate based on which side(s) it references.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateClass {
    /// References only lookup table columns — candidate for pushdown.
    LookupOnly,
    /// References only stream columns — evaluate locally.
    StreamOnly,
    /// References columns from both sides — evaluate locally.
    CrossReference,
    /// References no columns (constant expression) — evaluate locally.
    Constant,
}

/// Classifies predicates based on column membership.
///
/// Uses both unqualified column names and qualified `"alias.col"` names
/// for resolution (audit C7). When a column has a relation qualifier,
/// the qualified form is checked first.
#[derive(Debug)]
pub struct PredicateClassifier {
    /// Unqualified lookup column names.
    lookup_columns: HashSet<String>,
    /// Unqualified stream column names.
    stream_columns: HashSet<String>,
    /// Qualified lookup names: `"alias.col"` or `"table.col"`.
    lookup_qualified: HashSet<String>,
    /// Qualified stream names: `"alias.col"` or `"table.col"`.
    stream_qualified: HashSet<String>,
}

impl PredicateClassifier {
    /// Creates a new classifier from column sets.
    ///
    /// `lookup_alias` / `stream_alias` are the SQL aliases (e.g., `c` for
    /// `customers c`). When provided, qualified lookups like `c.name` can
    /// be resolved unambiguously.
    #[must_use]
    pub fn new(
        lookup_columns: HashSet<String>,
        stream_columns: HashSet<String>,
        lookup_alias: Option<&str>,
        stream_alias: Option<&str>,
    ) -> Self {
        let mut lookup_qualified = HashSet::new();
        let mut stream_qualified = HashSet::new();

        if let Some(alias) = lookup_alias {
            for col in &lookup_columns {
                lookup_qualified.insert(format!("{alias}.{col}"));
            }
        }
        if let Some(alias) = stream_alias {
            for col in &stream_columns {
                stream_qualified.insert(format!("{alias}.{col}"));
            }
        }

        Self {
            lookup_columns,
            stream_columns,
            lookup_qualified,
            stream_qualified,
        }
    }

    /// Classify a predicate expression.
    #[must_use]
    pub fn classify(&self, expr: &Expr) -> PredicateClass {
        let mut has_lookup = false;
        let mut has_stream = false;
        self.walk_columns(expr, &mut has_lookup, &mut has_stream);

        match (has_lookup, has_stream) {
            (true, false) => PredicateClass::LookupOnly,
            (false, true) => PredicateClass::StreamOnly,
            (true, true) => PredicateClass::CrossReference,
            (false, false) => PredicateClass::Constant,
        }
    }

    /// Recursively walk an expression to find column references.
    fn walk_columns(&self, expr: &Expr, has_lookup: &mut bool, has_stream: &mut bool) {
        match expr {
            Expr::Column(col) => {
                // C7: check qualified form first
                if let Some(relation) = &col.relation {
                    let qualified = format!("{}.{}", relation, col.name);
                    if self.lookup_qualified.contains(&qualified) {
                        *has_lookup = true;
                        return;
                    }
                    if self.stream_qualified.contains(&qualified) {
                        *has_stream = true;
                        return;
                    }
                }
                // Fall back to unqualified
                if self.lookup_columns.contains(&col.name) {
                    *has_lookup = true;
                }
                if self.stream_columns.contains(&col.name) {
                    *has_stream = true;
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
                self.walk_columns(left, has_lookup, has_stream);
                self.walk_columns(right, has_lookup, has_stream);
            }
            Expr::Not(inner)
            | Expr::IsNull(inner)
            | Expr::IsNotNull(inner)
            | Expr::Negative(inner)
            | Expr::Cast(datafusion::logical_expr::Cast { expr: inner, .. })
            | Expr::TryCast(datafusion::logical_expr::TryCast { expr: inner, .. }) => {
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
            Expr::Like(like) => {
                self.walk_columns(&like.expr, has_lookup, has_stream);
                self.walk_columns(&like.pattern, has_lookup, has_stream);
            }
            Expr::Case(case) => {
                if let Some(operand) = &case.expr {
                    self.walk_columns(operand, has_lookup, has_stream);
                }
                for (when, then) in &case.when_then_expr {
                    self.walk_columns(when, has_lookup, has_stream);
                    self.walk_columns(then, has_lookup, has_stream);
                }
                if let Some(else_expr) = &case.else_expr {
                    self.walk_columns(else_expr, has_lookup, has_stream);
                }
            }
            // Literals, placeholders — no columns
            Expr::Literal(..) | Expr::Placeholder(_) => {}
            // Catch-all: conservative — mark both sides
            _ => {
                *has_lookup = true;
                *has_stream = true;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Source Capabilities
// ---------------------------------------------------------------------------

/// Mode describing how far predicates can be pushed to a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanPushdownMode {
    /// Full predicate pushdown (eq, range, in, null checks).
    Full,
    /// Only key equality predicates.
    KeyOnly,
    /// No pushdown at all.
    None,
}

/// Describes a source's pushdown capabilities for the optimizer.
#[derive(Debug, Clone)]
pub struct PlanSourceCapabilities {
    /// Overall pushdown mode.
    pub pushdown_mode: PlanPushdownMode,
    /// Columns that support equality pushdown.
    pub eq_columns: HashSet<String>,
    /// Columns that support range pushdown.
    pub range_columns: HashSet<String>,
    /// Columns that support IN-list pushdown.
    pub in_columns: HashSet<String>,
    /// Whether the source supports IS NULL / IS NOT NULL checks.
    pub supports_null_check: bool,
}

impl Default for PlanSourceCapabilities {
    fn default() -> Self {
        Self {
            pushdown_mode: PlanPushdownMode::None,
            eq_columns: HashSet::new(),
            range_columns: HashSet::new(),
            in_columns: HashSet::new(),
            supports_null_check: false,
        }
    }
}

/// Registry mapping lookup table names to their source capabilities.
#[derive(Debug, Default)]
pub struct SourceCapabilitiesRegistry {
    capabilities: HashMap<String, PlanSourceCapabilities>,
}

impl SourceCapabilitiesRegistry {
    /// Register capabilities for a lookup table.
    pub fn register(&mut self, table_name: String, caps: PlanSourceCapabilities) {
        self.capabilities.insert(table_name, caps);
    }

    /// Get capabilities for a lookup table.
    #[must_use]
    pub fn get(&self, table_name: &str) -> Option<&PlanSourceCapabilities> {
        self.capabilities.get(table_name)
    }
}

// ---------------------------------------------------------------------------
// Conjunction Splitting
// ---------------------------------------------------------------------------

/// Splits a conjunction (AND chain) into individual predicates.
///
/// `A AND B AND C` → `[A, B, C]`.
/// OR expressions and non-AND binary expressions are kept as single items.
#[must_use]
pub fn split_conjunction(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: DfOperator::And,
            right,
        }) => {
            let mut parts = split_conjunction(left);
            parts.extend(split_conjunction(right));
            parts
        }
        other => vec![other.clone()],
    }
}

// ---------------------------------------------------------------------------
// Optimizer Rule
// ---------------------------------------------------------------------------

/// DataFusion optimizer rule that splits predicates for lookup joins.
///
/// Runs `TopDown` to catch `Filter` nodes above `LookupJoinNode` first.
///
/// Two cases:
/// 1. **Filter above LookupJoinNode** — absorb the filter, classify
///    each conjunct, and assign to pushdown or local.
/// 2. **Direct LookupJoinNode** — re-classify existing pushdown predicates
///    (e.g., after a previous pass added them).
#[derive(Debug)]
pub struct PredicateSplitterRule {
    /// Per-table source capabilities.
    capabilities: SourceCapabilitiesRegistry,
}

impl PredicateSplitterRule {
    /// Creates a new rule with the given capabilities registry.
    #[must_use]
    pub fn new(capabilities: SourceCapabilitiesRegistry) -> Self {
        Self { capabilities }
    }

    /// Split predicates for a `LookupJoinNode`, given a list of predicates
    /// that come from an absorbed `Filter` (if any) plus the node's
    /// existing predicates.
    fn split_for_node(
        &self,
        node: &LookupJoinNode,
        filter_predicates: &[Expr],
    ) -> (Vec<Expr>, Vec<Expr>) {
        // Build column sets from schemas
        let lookup_columns: HashSet<String> = node
            .lookup_schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let input_schema = node.inputs()[0].schema();
        let stream_columns: HashSet<String> = input_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let classifier = PredicateClassifier::new(
            lookup_columns,
            stream_columns,
            node.lookup_alias(),
            node.stream_alias(),
        );

        let caps = self.capabilities.get(node.lookup_table_name());
        let pushdown_disabled = caps.is_none_or(|c| c.pushdown_mode == PlanPushdownMode::None);

        let is_left_outer = node.join_type() == LookupJoinType::LeftOuter;

        let mut pushdown = Vec::new();
        let mut local = Vec::new();

        // Include existing predicates from the node
        let all_predicates = node
            .pushdown_predicates()
            .iter()
            .chain(node.local_predicates().iter())
            .chain(filter_predicates.iter())
            .cloned();

        for pred in all_predicates {
            let class = classifier.classify(&pred);

            // NotEq predicates never push down
            let has_not_eq = contains_not_eq(&pred);

            match class {
                PredicateClass::LookupOnly => {
                    // H10: LEFT OUTER WHERE-clause lookup-only preds stay local
                    if is_left_outer || pushdown_disabled || has_not_eq {
                        local.push(pred);
                    } else {
                        pushdown.push(pred);
                    }
                }
                PredicateClass::StreamOnly
                | PredicateClass::CrossReference
                | PredicateClass::Constant => {
                    local.push(pred);
                }
            }
        }

        (pushdown, local)
    }
}

/// Check if an expression contains a `NotEq` operator.
fn contains_not_eq(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            *op == DfOperator::NotEq || contains_not_eq(left) || contains_not_eq(right)
        }
        Expr::Not(inner) => contains_not_eq(inner),
        _ => false,
    }
}

impl OptimizerRule for PredicateSplitterRule {
    fn name(&self) -> &'static str {
        "predicate_splitter"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Case 1: Filter above a LookupJoinNode
        if let LogicalPlan::Filter(Filter {
            predicate, input, ..
        }) = &plan
        {
            if let LogicalPlan::Extension(ext) = input.as_ref() {
                if let Some(node) = ext.node.as_any().downcast_ref::<LookupJoinNode>() {
                    let filter_preds = split_conjunction(predicate);
                    let (pushdown, local) = self.split_for_node(node, &filter_preds);

                    let inputs = node.inputs();
                    let rebuilt = LookupJoinNode::new(
                        inputs[0].clone(),
                        node.lookup_table_name().to_string(),
                        node.lookup_schema().clone(),
                        node.join_keys().to_vec(),
                        node.join_type(),
                        pushdown,
                        node.required_lookup_columns().clone(),
                        UserDefinedLogicalNodeCore::schema(node).clone(),
                        node.metadata().clone(),
                    )
                    .with_local_predicates(local)
                    .with_aliases(
                        node.lookup_alias().map(String::from),
                        node.stream_alias().map(String::from),
                    );

                    return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                        node: Arc::new(rebuilt),
                    })));
                }
            }
        }

        // Case 2: Direct LookupJoinNode (re-classify existing predicates)
        if let LogicalPlan::Extension(ext) = &plan {
            if let Some(node) = ext.node.as_any().downcast_ref::<LookupJoinNode>() {
                // Only re-classify if there are predicates to work with
                if !node.pushdown_predicates().is_empty() || !node.local_predicates().is_empty() {
                    let (pushdown, local) = self.split_for_node(node, &[]);
                    let inputs = node.inputs();
                    let rebuilt = LookupJoinNode::new(
                        inputs[0].clone(),
                        node.lookup_table_name().to_string(),
                        node.lookup_schema().clone(),
                        node.join_keys().to_vec(),
                        node.join_type(),
                        pushdown,
                        node.required_lookup_columns().clone(),
                        UserDefinedLogicalNodeCore::schema(node).clone(),
                        node.metadata().clone(),
                    )
                    .with_local_predicates(local)
                    .with_aliases(
                        node.lookup_alias().map(String::from),
                        node.stream_alias().map(String::from),
                    );

                    return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                        node: Arc::new(rebuilt),
                    })));
                }
            }
        }

        Ok(Transformed::no(plan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::col;
    use datafusion::prelude::lit;

    use crate::datafusion::lookup_join::{
        JoinKeyPair, LookupJoinNode, LookupJoinType, LookupTableMetadata,
    };

    fn lookup_cols() -> HashSet<String> {
        HashSet::from(["id".to_string(), "name".to_string(), "region".to_string()])
    }

    fn stream_cols() -> HashSet<String> {
        HashSet::from([
            "order_id".to_string(),
            "customer_id".to_string(),
            "amount".to_string(),
        ])
    }

    fn classifier() -> PredicateClassifier {
        PredicateClassifier::new(lookup_cols(), stream_cols(), None, None)
    }

    fn classifier_with_aliases() -> PredicateClassifier {
        PredicateClassifier::new(lookup_cols(), stream_cols(), Some("c"), Some("o"))
    }

    // -----------------------------------------------------------------------
    // PredicateClassifier tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_classify_lookup_only() {
        let c = classifier();
        let expr = col("region").eq(lit("US"));
        assert_eq!(c.classify(&expr), PredicateClass::LookupOnly);
    }

    #[test]
    fn test_classify_stream_only() {
        let c = classifier();
        let expr = col("amount").gt(lit(100));
        assert_eq!(c.classify(&expr), PredicateClass::StreamOnly);
    }

    #[test]
    fn test_classify_cross_reference() {
        let c = classifier();
        // amount (stream) > id (lookup) → cross-reference
        let expr = col("amount").gt(col("id"));
        assert_eq!(c.classify(&expr), PredicateClass::CrossReference);
    }

    #[test]
    fn test_classify_constant() {
        let c = classifier();
        let expr = lit(1).eq(lit(1));
        assert_eq!(c.classify(&expr), PredicateClass::Constant);
    }

    #[test]
    fn test_classify_qualified_lookup_c7() {
        let c = classifier_with_aliases();
        // c.name should resolve to lookup via qualified match
        let expr = Expr::Column(datafusion::common::Column::new(Some::<&str>("c"), "name"))
            .eq(lit("Alice"));
        assert_eq!(c.classify(&expr), PredicateClass::LookupOnly);
    }

    #[test]
    fn test_classify_qualified_stream_c7() {
        let c = classifier_with_aliases();
        let expr =
            Expr::Column(datafusion::common::Column::new(Some::<&str>("o"), "amount")).gt(lit(50));
        assert_eq!(c.classify(&expr), PredicateClass::StreamOnly);
    }

    #[test]
    fn test_classify_ambiguous_both_sides() {
        // Column name exists in both sides without qualifier → both flags set
        let lookup = HashSet::from(["id".to_string()]);
        let stream = HashSet::from(["id".to_string()]);
        let c = PredicateClassifier::new(lookup, stream, None, None);
        let expr = col("id").eq(lit(1));
        assert_eq!(c.classify(&expr), PredicateClass::CrossReference);
    }

    #[test]
    fn test_classify_nested_function() {
        let c = classifier();
        // UPPER(name) = 'ALICE' — name is lookup-only
        let expr = Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction {
            func: datafusion::functions::string::upper(),
            args: vec![col("name")],
        })
        .eq(lit("ALICE"));
        assert_eq!(c.classify(&expr), PredicateClass::LookupOnly);
    }

    #[test]
    fn test_classify_is_null() {
        let c = classifier();
        let expr = col("name").is_null();
        assert_eq!(c.classify(&expr), PredicateClass::LookupOnly);
    }

    #[test]
    fn test_classify_between() {
        let c = classifier();
        let expr = Expr::Between(datafusion::logical_expr::expr::Between {
            expr: Box::new(col("amount")),
            negated: false,
            low: Box::new(lit(10)),
            high: Box::new(lit(100)),
        });
        assert_eq!(c.classify(&expr), PredicateClass::StreamOnly);
    }

    #[test]
    fn test_classify_in_list() {
        let c = classifier();
        let expr = col("region").in_list(vec![lit("US"), lit("EU")], false);
        assert_eq!(c.classify(&expr), PredicateClass::LookupOnly);
    }

    // -----------------------------------------------------------------------
    // split_conjunction tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_split_flat_conjunction() {
        let expr = col("a")
            .eq(lit(1))
            .and(col("b").eq(lit(2)))
            .and(col("c").eq(lit(3)));
        let parts = split_conjunction(&expr);
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn test_split_nested_conjunction() {
        // (A AND B) AND (C AND D)
        let left = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
        let right = col("c").eq(lit(3)).and(col("d").eq(lit(4)));
        let expr = left.and(right);
        let parts = split_conjunction(&expr);
        assert_eq!(parts.len(), 4);
    }

    #[test]
    fn test_split_single_predicate() {
        let expr = col("a").eq(lit(1));
        let parts = split_conjunction(&expr);
        assert_eq!(parts.len(), 1);
    }

    #[test]
    fn test_split_or_not_split() {
        // OR should NOT be split
        let expr = col("a").eq(lit(1)).or(col("b").eq(lit(2)));
        let parts = split_conjunction(&expr);
        assert_eq!(parts.len(), 1);
    }

    // -----------------------------------------------------------------------
    // PredicateSplitterRule integration tests
    // -----------------------------------------------------------------------

    fn test_metadata() -> LookupTableMetadata {
        LookupTableMetadata {
            connector: "postgres-cdc".to_string(),
            strategy: "replicated".to_string(),
            pushdown_mode: "auto".to_string(),
            primary_key: vec!["id".to_string()],
        }
    }

    fn test_stream_schema() -> Arc<DFSchema> {
        Arc::new(
            DFSchema::try_from(Schema::new(vec![
                Field::new("order_id", DataType::Int64, false),
                Field::new("customer_id", DataType::Int64, false),
                Field::new("amount", DataType::Float64, false),
            ]))
            .unwrap(),
        )
    }

    fn test_lookup_schema() -> Arc<DFSchema> {
        Arc::new(
            DFSchema::try_from(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("region", DataType::Utf8, true),
            ]))
            .unwrap(),
        )
    }

    fn test_output_schema() -> Arc<DFSchema> {
        Arc::new(
            DFSchema::try_from(Schema::new(vec![
                Field::new("order_id", DataType::Int64, false),
                Field::new("customer_id", DataType::Int64, false),
                Field::new("amount", DataType::Float64, false),
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("region", DataType::Utf8, true),
            ]))
            .unwrap(),
        )
    }

    fn make_lookup_node(join_type: LookupJoinType) -> LookupJoinNode {
        let stream_schema = test_stream_schema();
        let input = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: stream_schema,
        });

        LookupJoinNode::new(
            input,
            "customers".to_string(),
            test_lookup_schema(),
            vec![JoinKeyPair {
                stream_expr: col("customer_id"),
                lookup_column: "id".to_string(),
            }],
            join_type,
            vec![],
            HashSet::from(["id".to_string(), "name".to_string(), "region".to_string()]),
            test_output_schema(),
            test_metadata(),
        )
    }

    fn make_filter_over_node(node: LookupJoinNode, predicate: Expr) -> LogicalPlan {
        let ext = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });
        LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(ext)).unwrap())
    }

    fn full_capabilities() -> SourceCapabilitiesRegistry {
        let mut reg = SourceCapabilitiesRegistry::default();
        reg.register(
            "customers".to_string(),
            PlanSourceCapabilities {
                pushdown_mode: PlanPushdownMode::Full,
                eq_columns: HashSet::from([
                    "id".to_string(),
                    "name".to_string(),
                    "region".to_string(),
                ]),
                range_columns: HashSet::new(),
                in_columns: HashSet::new(),
                supports_null_check: true,
            },
        );
        reg
    }

    fn no_capabilities() -> SourceCapabilitiesRegistry {
        SourceCapabilitiesRegistry::default()
    }

    #[test]
    fn test_pushdown_inner_join_lookup_only() {
        let node = make_lookup_node(LookupJoinType::Inner);
        let filter_pred = col("region").eq(lit("US"));
        let plan = make_filter_over_node(node, filter_pred);

        let rule = PredicateSplitterRule::new(full_capabilities());
        let result = rule
            .rewrite(
                plan,
                &datafusion_optimizer::optimizer::OptimizerContext::new(),
            )
            .unwrap();

        assert!(result.transformed);
        if let LogicalPlan::Extension(ext) = &result.data {
            let rebuilt = ext.node.as_any().downcast_ref::<LookupJoinNode>().unwrap();
            assert_eq!(rebuilt.pushdown_predicates().len(), 1);
            assert_eq!(rebuilt.local_predicates().len(), 0);
        } else {
            panic!("Expected Extension node");
        }
    }

    #[test]
    fn test_stream_predicate_stays_local() {
        let node = make_lookup_node(LookupJoinType::Inner);
        let filter_pred = col("amount").gt(lit(100));
        let plan = make_filter_over_node(node, filter_pred);

        let rule = PredicateSplitterRule::new(full_capabilities());
        let result = rule
            .rewrite(
                plan,
                &datafusion_optimizer::optimizer::OptimizerContext::new(),
            )
            .unwrap();

        assert!(result.transformed);
        if let LogicalPlan::Extension(ext) = &result.data {
            let rebuilt = ext.node.as_any().downcast_ref::<LookupJoinNode>().unwrap();
            assert_eq!(rebuilt.pushdown_predicates().len(), 0);
            assert_eq!(rebuilt.local_predicates().len(), 1);
        } else {
            panic!("Expected Extension node");
        }
    }

    #[test]
    fn test_cross_ref_stays_local() {
        let node = make_lookup_node(LookupJoinType::Inner);
        // amount > id  (crosses stream and lookup)
        let filter_pred = col("amount").gt(col("id"));
        let plan = make_filter_over_node(node, filter_pred);

        let rule = PredicateSplitterRule::new(full_capabilities());
        let result = rule
            .rewrite(
                plan,
                &datafusion_optimizer::optimizer::OptimizerContext::new(),
            )
            .unwrap();

        assert!(result.transformed);
        if let LogicalPlan::Extension(ext) = &result.data {
            let rebuilt = ext.node.as_any().downcast_ref::<LookupJoinNode>().unwrap();
            assert_eq!(rebuilt.pushdown_predicates().len(), 0);
            assert_eq!(rebuilt.local_predicates().len(), 1);
        } else {
            panic!("Expected Extension node");
        }
    }

    #[test]
    fn test_pushdown_disabled_keeps_local() {
        let node = make_lookup_node(LookupJoinType::Inner);
        let filter_pred = col("region").eq(lit("US"));
        let plan = make_filter_over_node(node, filter_pred);

        // No capabilities registered → pushdown disabled
        let rule = PredicateSplitterRule::new(no_capabilities());
        let result = rule
            .rewrite(
                plan,
                &datafusion_optimizer::optimizer::OptimizerContext::new(),
            )
            .unwrap();

        assert!(result.transformed);
        if let LogicalPlan::Extension(ext) = &result.data {
            let rebuilt = ext.node.as_any().downcast_ref::<LookupJoinNode>().unwrap();
            assert_eq!(rebuilt.pushdown_predicates().len(), 0);
            assert_eq!(rebuilt.local_predicates().len(), 1);
        } else {
            panic!("Expected Extension node");
        }
    }

    #[test]
    fn test_left_join_h10_safety() {
        // H10: LEFT OUTER lookup-only preds must NOT be pushed down
        let node = make_lookup_node(LookupJoinType::LeftOuter);
        let filter_pred = col("region").eq(lit("US"));
        let plan = make_filter_over_node(node, filter_pred);

        let rule = PredicateSplitterRule::new(full_capabilities());
        let result = rule
            .rewrite(
                plan,
                &datafusion_optimizer::optimizer::OptimizerContext::new(),
            )
            .unwrap();

        assert!(result.transformed);
        if let LogicalPlan::Extension(ext) = &result.data {
            let rebuilt = ext.node.as_any().downcast_ref::<LookupJoinNode>().unwrap();
            // Should stay local due to H10
            assert_eq!(rebuilt.pushdown_predicates().len(), 0);
            assert_eq!(rebuilt.local_predicates().len(), 1);
        } else {
            panic!("Expected Extension node");
        }
    }

    #[test]
    fn test_no_filter_no_predicates_passthrough() {
        let node = make_lookup_node(LookupJoinType::Inner);
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });

        let rule = PredicateSplitterRule::new(full_capabilities());
        let result = rule
            .rewrite(
                plan,
                &datafusion_optimizer::optimizer::OptimizerContext::new(),
            )
            .unwrap();

        // No predicates to split → no transformation
        assert!(!result.transformed);
    }

    #[test]
    fn test_mixed_conjunction_split() {
        let node = make_lookup_node(LookupJoinType::Inner);
        // region = 'US' AND amount > 100
        let filter_pred = col("region").eq(lit("US")).and(col("amount").gt(lit(100)));
        let plan = make_filter_over_node(node, filter_pred);

        let rule = PredicateSplitterRule::new(full_capabilities());
        let result = rule
            .rewrite(
                plan,
                &datafusion_optimizer::optimizer::OptimizerContext::new(),
            )
            .unwrap();

        assert!(result.transformed);
        if let LogicalPlan::Extension(ext) = &result.data {
            let rebuilt = ext.node.as_any().downcast_ref::<LookupJoinNode>().unwrap();
            // region = 'US' → pushdown, amount > 100 → local
            assert_eq!(rebuilt.pushdown_predicates().len(), 1);
            assert_eq!(rebuilt.local_predicates().len(), 1);
        } else {
            panic!("Expected Extension node");
        }
    }

    #[test]
    fn test_not_eq_stays_local() {
        let node = make_lookup_node(LookupJoinType::Inner);
        let filter_pred = col("region").not_eq(lit("US"));
        let plan = make_filter_over_node(node, filter_pred);

        let rule = PredicateSplitterRule::new(full_capabilities());
        let result = rule
            .rewrite(
                plan,
                &datafusion_optimizer::optimizer::OptimizerContext::new(),
            )
            .unwrap();

        assert!(result.transformed);
        if let LogicalPlan::Extension(ext) = &result.data {
            let rebuilt = ext.node.as_any().downcast_ref::<LookupJoinNode>().unwrap();
            // NotEq never pushed down
            assert_eq!(rebuilt.pushdown_predicates().len(), 0);
            assert_eq!(rebuilt.local_predicates().len(), 1);
        } else {
            panic!("Expected Extension node");
        }
    }

    #[test]
    fn test_source_capabilities_registry() {
        let mut reg = SourceCapabilitiesRegistry::default();
        assert!(reg.get("foo").is_none());

        reg.register(
            "foo".to_string(),
            PlanSourceCapabilities {
                pushdown_mode: PlanPushdownMode::Full,
                ..Default::default()
            },
        );
        assert_eq!(
            reg.get("foo").unwrap().pushdown_mode,
            PlanPushdownMode::Full
        );
    }

    #[test]
    fn test_plan_source_capabilities_default() {
        let caps = PlanSourceCapabilities::default();
        assert_eq!(caps.pushdown_mode, PlanPushdownMode::None);
        assert!(caps.eq_columns.is_empty());
        assert!(!caps.supports_null_check);
    }
}
