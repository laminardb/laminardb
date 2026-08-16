use super::*;

#[allow(clippy::disallowed_types)] // cold path: query planning
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
    let expr =
        Expr::Column(datafusion::common::Column::new(Some::<&str>("c"), "name")).eq(lit("Alice"));
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
        connector: "postgres".to_string(),
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
            eq_columns: HashSet::from(["id".to_string(), "name".to_string(), "region".to_string()]),
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
