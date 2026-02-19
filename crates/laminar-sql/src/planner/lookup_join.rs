//! Optimizer rules that rewrite standard JOINs to `LookupJoinNode`.
//!
//! When a query joins a streaming source with a registered lookup table,
//! the [`LookupJoinRewriteRule`] replaces the standard hash/merge join
//! with a [`LookupJoinNode`] that uses the lookup source connector.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::logical_plan::LogicalPlan;
use datafusion::logical_expr::{Extension, Join, TableScan, UserDefinedLogicalNodeCore};
use datafusion_common::tree_node::Transformed;
use datafusion_optimizer::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

use crate::datafusion::lookup_join::{
    JoinKeyPair, LookupJoinNode, LookupJoinType, LookupTableMetadata,
};
use crate::planner::LookupTableInfo;

/// Rewrites standard JOIN nodes that reference a lookup table into
/// `LookupJoinNode` extension nodes.
#[derive(Debug)]
pub struct LookupJoinRewriteRule {
    /// Registered lookup tables, keyed by name.
    lookup_tables: HashMap<String, LookupTableInfo>,
}

impl LookupJoinRewriteRule {
    /// Creates a new rewrite rule with the given set of registered lookup tables.
    #[must_use]
    pub fn new(lookup_tables: HashMap<String, LookupTableInfo>) -> Self {
        Self { lookup_tables }
    }

    /// Detects which side of a join (if any) is a lookup table scan.
    /// Returns `Some((lookup_side_is_right, table_name))`.
    fn detect_lookup_side(&self, join: &Join) -> Option<(bool, String)> {
        // Check right side
        if let Some(name) = scan_table_name(&join.right) {
            if self.lookup_tables.contains_key(&name) {
                return Some((true, name));
            }
        }
        // Check left side
        if let Some(name) = scan_table_name(&join.left) {
            if self.lookup_tables.contains_key(&name) {
                return Some((false, name));
            }
        }
        None
    }
}

impl OptimizerRule for LookupJoinRewriteRule {
    fn name(&self) -> &'static str {
        "lookup_join_rewrite"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Join(join) = &plan else {
            return Ok(Transformed::no(plan));
        };

        let Some((lookup_is_right, table_name)) = self.detect_lookup_side(join) else {
            return Ok(Transformed::no(plan));
        };

        let info = &self.lookup_tables[&table_name];

        // Determine which side is stream and which is lookup
        let (stream_plan, lookup_plan) = if lookup_is_right {
            (join.left.as_ref(), join.right.as_ref())
        } else {
            (join.right.as_ref(), join.left.as_ref())
        };

        // Extract aliases for qualified column resolution (C7)
        let stream_alias =
            scan_table_name_and_alias(stream_plan).and_then(|(_, a)| a);
        let lookup_alias =
            scan_table_name_and_alias(lookup_plan).and_then(|(_, a)| a);

        let lookup_schema = lookup_plan.schema().clone();

        // Build join key pairs from the equijoin conditions
        let join_keys: Vec<JoinKeyPair> = join
            .on
            .iter()
            .map(|(left_expr, right_expr)| {
                if lookup_is_right {
                    JoinKeyPair {
                        stream_expr: left_expr.clone(),
                        lookup_column: right_expr.to_string(),
                    }
                } else {
                    JoinKeyPair {
                        stream_expr: right_expr.clone(),
                        lookup_column: left_expr.to_string(),
                    }
                }
            })
            .collect();

        // Convert DataFusion join type to our lookup join type
        let join_type = match join.join_type {
            datafusion::logical_expr::JoinType::Inner => LookupJoinType::Inner,
            datafusion::logical_expr::JoinType::Left if lookup_is_right => {
                LookupJoinType::LeftOuter
            }
            datafusion::logical_expr::JoinType::Right if !lookup_is_right => {
                LookupJoinType::LeftOuter
            }
            _ => return Ok(Transformed::no(plan)),
        };

        // All lookup columns are required initially; pruning is done later
        let required_columns: HashSet<String> = lookup_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        // Build output schema from stream + lookup
        let stream_schema = stream_plan.schema();
        let merged_fields: Vec<_> = stream_schema
            .fields()
            .iter()
            .chain(lookup_schema.fields().iter())
            .cloned()
            .collect();
        let output_schema = Arc::new(DFSchema::from_unqualified_fields(
            merged_fields.into(),
            HashMap::new(),
        )?);

        let metadata = LookupTableMetadata {
            connector: info.properties.connector.to_string(),
            strategy: info.properties.strategy.to_string(),
            pushdown_mode: info.properties.pushdown_mode.to_string(),
            primary_key: info.primary_key.clone(),
        };

        let node = LookupJoinNode::new(
            stream_plan.clone(),
            table_name,
            lookup_schema,
            join_keys,
            join_type,
            vec![], // predicates pushed down later
            required_columns,
            output_schema,
            metadata,
        )
        .with_aliases(lookup_alias, stream_alias);

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        })))
    }
}

/// Column pruning rule for `LookupJoinNode`.
///
/// Narrows `required_lookup_columns` to only the columns referenced
/// by downstream plan nodes.
#[derive(Debug)]
pub struct LookupColumnPruningRule;

impl OptimizerRule for LookupColumnPruningRule {
    fn name(&self) -> &'static str {
        "lookup_column_pruning"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Extension(ext) = &plan else {
            return Ok(Transformed::no(plan));
        };

        let Some(node) = ext.node.as_any().downcast_ref::<LookupJoinNode>() else {
            return Ok(Transformed::no(plan));
        };

        // Collect columns actually used downstream by walking the parent plan.
        // For now, we use the node's schema to determine which lookup columns
        // appear in the output. A full implementation would track column usage
        // from parent nodes; this is a conservative starting point.
        let schema = UserDefinedLogicalNodeCore::schema(node);
        let used: HashSet<String> = schema
            .fields()
            .iter()
            .filter(|f| node.required_lookup_columns().contains(f.name()))
            .map(|f| f.name().clone())
            .collect();

        if used == *node.required_lookup_columns() {
            return Ok(Transformed::no(plan));
        }

        // Rebuild with narrowed columns
        let node_inputs = UserDefinedLogicalNodeCore::inputs(node);
        let pruned = LookupJoinNode::new(
            node_inputs[0].clone(),
            node.lookup_table_name().to_string(),
            node.lookup_schema().clone(),
            node.join_keys().to_vec(),
            node.join_type(),
            node.pushdown_predicates().to_vec(),
            used,
            schema.clone(),
            node.metadata().clone(),
        )
        .with_local_predicates(node.local_predicates().to_vec())
        .with_aliases(
            node.lookup_alias().map(String::from),
            node.stream_alias().map(String::from),
        );

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(pruned),
        })))
    }
}

/// Extracts the table name and optional alias from a plan node.
///
/// Returns `(base_table_name, alias)` — alias is the `SubqueryAlias` name
/// if the scan is wrapped in one, `None` otherwise.
fn scan_table_name_and_alias(plan: &LogicalPlan) -> Option<(String, Option<String>)> {
    match plan {
        LogicalPlan::TableScan(TableScan { table_name, .. }) => {
            Some((table_name.table().to_string(), None))
        }
        LogicalPlan::SubqueryAlias(alias) => {
            let alias_name = alias.alias.table().to_string();
            scan_table_name_and_alias(&alias.input)
                .map(|(base, _)| (base, Some(alias_name)))
        }
        _ => None,
    }
}

/// Extracts the table name from a `TableScan` node, unwrapping aliases.
fn scan_table_name(plan: &LogicalPlan) -> Option<String> {
    scan_table_name_and_alias(plan).map(|(name, _)| name)
}

/// Display helpers for connector/strategy/pushdown types.
impl fmt::Display for crate::parser::lookup_table::ConnectorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PostgresCdc => write!(f, "postgres-cdc"),
            Self::MysqlCdc => write!(f, "mysql-cdc"),
            Self::Redis => write!(f, "redis"),
            Self::S3Parquet => write!(f, "s3-parquet"),
            Self::Static => write!(f, "static"),
            Self::Custom(s) => write!(f, "{s}"),
        }
    }
}

impl fmt::Display for crate::parser::lookup_table::LookupStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Replicated => write!(f, "replicated"),
            Self::Partitioned => write!(f, "partitioned"),
            Self::OnDemand => write!(f, "on-demand"),
        }
    }
}

impl fmt::Display for crate::parser::lookup_table::PushdownMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Auto => write!(f, "auto"),
            Self::Enabled => write!(f, "enabled"),
            Self::Disabled => write!(f, "disabled"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::lookup_table::{
        ByteSize, ConnectorType, LookupStrategy, LookupTableProperties, PushdownMode,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use datafusion_common::tree_node::TreeNode;
    use datafusion_optimizer::optimizer::OptimizerContext;

    fn test_lookup_info() -> LookupTableInfo {
        LookupTableInfo {
            name: "customers".to_string(),
            columns: vec![
                ("id".to_string(), "INT".to_string()),
                ("name".to_string(), "VARCHAR".to_string()),
            ],
            primary_key: vec!["id".to_string()],
            properties: LookupTableProperties {
                connector: ConnectorType::PostgresCdc,
                connection: Some("postgresql://localhost/db".to_string()),
                strategy: LookupStrategy::Replicated,
                cache_memory: Some(ByteSize(512 * 1024 * 1024)),
                cache_disk: None,
                cache_ttl: None,
                pushdown_mode: PushdownMode::Auto,
            },
        }
    }

    fn register_test_tables(ctx: &SessionContext) {
        let orders_schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        let customers_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        ctx.register_batch("orders", arrow::array::RecordBatch::new_empty(orders_schema))
            .unwrap();
        ctx.register_batch(
            "customers",
            arrow::array::RecordBatch::new_empty(customers_schema),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn test_rewrite_join_on_lookup_table() {
        let ctx = SessionContext::new();
        register_test_tables(&ctx);

        let plan = ctx
            .sql("SELECT o.order_id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id")
            .await
            .unwrap()
            .into_unoptimized_plan();

        let mut lookup_tables = HashMap::new();
        lookup_tables.insert("customers".to_string(), test_lookup_info());
        let rule = LookupJoinRewriteRule::new(lookup_tables);

        let transformed = plan
            .transform_down(|p| rule.rewrite(p, &OptimizerContext::new()))
            .unwrap();

        // Verify rewrite happened
        assert!(transformed.transformed);
        let has_lookup = format!("{:?}", transformed.data).contains("LookupJoin");
        assert!(has_lookup, "Expected LookupJoin in plan");
    }

    #[tokio::test]
    async fn test_non_lookup_join_not_rewritten() {
        let ctx = SessionContext::new();
        // Register both as regular tables (neither is a lookup table)
        let schema_a = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("a_id", DataType::Int64, false),
        ]));
        ctx.register_batch("a", arrow::array::RecordBatch::new_empty(schema_a))
            .unwrap();
        ctx.register_batch("b", arrow::array::RecordBatch::new_empty(schema_b))
            .unwrap();

        let plan = ctx
            .sql("SELECT * FROM a JOIN b ON a.id = b.a_id")
            .await
            .unwrap()
            .into_unoptimized_plan();

        // No lookup tables registered
        let rule = LookupJoinRewriteRule::new(HashMap::new());

        let transformed = plan
            .transform_down(|p| rule.rewrite(p, &OptimizerContext::new()))
            .unwrap();

        assert!(!transformed.transformed);
    }

    #[tokio::test]
    async fn test_left_outer_produces_left_outer_type() {
        let ctx = SessionContext::new();
        register_test_tables(&ctx);

        let plan = ctx
            .sql("SELECT o.order_id, c.name FROM orders o LEFT JOIN customers c ON o.customer_id = c.id")
            .await
            .unwrap()
            .into_unoptimized_plan();

        let mut lookup_tables = HashMap::new();
        lookup_tables.insert("customers".to_string(), test_lookup_info());
        let rule = LookupJoinRewriteRule::new(lookup_tables);

        let transformed = plan
            .transform_down(|p| rule.rewrite(p, &OptimizerContext::new()))
            .unwrap();

        assert!(transformed.transformed);
        let debug_str = format!("{:?}", transformed.data);
        assert!(debug_str.contains("LeftOuter"), "Expected LeftOuter join type, got: {debug_str}");
    }

    #[test]
    fn test_fmt_display_connector_type() {
        assert_eq!(ConnectorType::PostgresCdc.to_string(), "postgres-cdc");
        assert_eq!(ConnectorType::Redis.to_string(), "redis");
        assert_eq!(
            ConnectorType::Custom("my-conn".into()).to_string(),
            "my-conn"
        );
    }

    #[test]
    fn test_fmt_display_strategy() {
        assert_eq!(LookupStrategy::Replicated.to_string(), "replicated");
        assert_eq!(LookupStrategy::OnDemand.to_string(), "on-demand");
    }

    #[test]
    fn test_fmt_display_pushdown_mode() {
        assert_eq!(PushdownMode::Auto.to_string(), "auto");
        assert_eq!(PushdownMode::Disabled.to_string(), "disabled");
    }
}
