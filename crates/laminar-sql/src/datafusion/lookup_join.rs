//! `LookupJoinNode` — custom DataFusion logical plan node for lookup joins.
//!
//! This node represents a join between a streaming input and a registered
//! lookup table. It is produced by the `LookupJoinRewriteRule` optimizer
//! rule when a standard JOIN references a registered lookup table.

use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::logical_plan::LogicalPlan;
use datafusion::logical_expr::{Expr, UserDefinedLogicalNodeCore};
use datafusion_common::Result;

/// Join type for lookup joins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LookupJoinType {
    /// Inner join — only emit rows with a match.
    Inner,
    /// Left outer join — emit all stream rows, NULLs for non-matches.
    LeftOuter,
}

impl fmt::Display for LookupJoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inner => write!(f, "Inner"),
            Self::LeftOuter => write!(f, "LeftOuter"),
        }
    }
}

/// A pair of expressions defining how stream keys map to lookup columns.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JoinKeyPair {
    /// Expression on the stream side (e.g., `stream.customer_id`).
    pub stream_expr: Expr,
    /// Column name on the lookup table side.
    pub lookup_column: String,
}

/// Metadata about a lookup table for plan construction.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LookupTableMetadata {
    /// Connector type (e.g., "postgres-cdc").
    pub connector: String,
    /// Lookup strategy (e.g., "replicated").
    pub strategy: String,
    /// Pushdown mode (e.g., "auto").
    pub pushdown_mode: String,
    /// Primary key column names.
    pub primary_key: Vec<String>,
}

/// Custom logical plan node for a lookup join.
///
/// Represents a join between a streaming input plan and a lookup table.
/// The lookup table is not a DataFusion table; it is resolved at execution
/// time via the lookup source connector.
#[derive(Debug, Clone)]
pub struct LookupJoinNode {
    /// The streaming input plan.
    input: Arc<LogicalPlan>,
    /// Name of the lookup table.
    lookup_table: String,
    /// Schema of the lookup table columns.
    lookup_schema: DFSchemaRef,
    /// Join key pairs (stream expression -> lookup column).
    join_keys: Vec<JoinKeyPair>,
    /// Join type (Inner or LeftOuter).
    join_type: LookupJoinType,
    /// Predicates to push down to the lookup source.
    pushdown_predicates: Vec<Expr>,
    /// Predicates evaluated locally after the join.
    local_predicates: Vec<Expr>,
    /// Required columns from the lookup table.
    required_lookup_columns: HashSet<String>,
    /// Combined output schema (stream + lookup columns).
    output_schema: DFSchemaRef,
    /// Metadata about the lookup table.
    metadata: LookupTableMetadata,
    /// Alias for the lookup table (for qualified column resolution).
    lookup_alias: Option<String>,
    /// Alias for the stream input (for qualified column resolution).
    stream_alias: Option<String>,
}

impl PartialEq for LookupJoinNode {
    fn eq(&self, other: &Self) -> bool {
        self.lookup_table == other.lookup_table
            && self.join_keys == other.join_keys
            && self.join_type == other.join_type
            && self.pushdown_predicates == other.pushdown_predicates
            && self.local_predicates == other.local_predicates
            && self.required_lookup_columns == other.required_lookup_columns
            && self.metadata == other.metadata
    }
}

impl Eq for LookupJoinNode {}

impl Hash for LookupJoinNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.lookup_table.hash(state);
        self.join_keys.hash(state);
        self.join_type.hash(state);
        self.pushdown_predicates.hash(state);
        self.local_predicates.hash(state);
        self.metadata.hash(state);
        // HashSet doesn't implement Hash; hash sorted elements instead
        let mut cols: Vec<&String> = self.required_lookup_columns.iter().collect();
        cols.sort();
        cols.hash(state);
    }
}

impl PartialOrd for LookupJoinNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.lookup_table.partial_cmp(&other.lookup_table)
    }
}

impl LookupJoinNode {
    /// Creates a new lookup join node.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: LogicalPlan,
        lookup_table: String,
        lookup_schema: DFSchemaRef,
        join_keys: Vec<JoinKeyPair>,
        join_type: LookupJoinType,
        pushdown_predicates: Vec<Expr>,
        required_lookup_columns: HashSet<String>,
        output_schema: DFSchemaRef,
        metadata: LookupTableMetadata,
    ) -> Self {
        Self {
            input: Arc::new(input),
            lookup_table,
            lookup_schema,
            join_keys,
            join_type,
            pushdown_predicates,
            local_predicates: vec![],
            required_lookup_columns,
            output_schema,
            metadata,
            lookup_alias: None,
            stream_alias: None,
        }
    }

    /// Sets predicates to be evaluated locally after the join.
    #[must_use]
    pub fn with_local_predicates(mut self, predicates: Vec<Expr>) -> Self {
        self.local_predicates = predicates;
        self
    }

    /// Sets table aliases for qualified column resolution.
    #[must_use]
    pub fn with_aliases(
        mut self,
        lookup_alias: Option<String>,
        stream_alias: Option<String>,
    ) -> Self {
        self.lookup_alias = lookup_alias;
        self.stream_alias = stream_alias;
        self
    }

    /// Returns the lookup table name.
    #[must_use]
    pub fn lookup_table_name(&self) -> &str {
        &self.lookup_table
    }

    /// Returns the join key pairs.
    #[must_use]
    pub fn join_keys(&self) -> &[JoinKeyPair] {
        &self.join_keys
    }

    /// Returns the join type.
    #[must_use]
    pub fn join_type(&self) -> LookupJoinType {
        self.join_type
    }

    /// Returns the pushdown predicates.
    #[must_use]
    pub fn pushdown_predicates(&self) -> &[Expr] {
        &self.pushdown_predicates
    }

    /// Returns the required lookup columns.
    #[must_use]
    pub fn required_lookup_columns(&self) -> &HashSet<String> {
        &self.required_lookup_columns
    }

    /// Returns the lookup table metadata.
    #[must_use]
    pub fn metadata(&self) -> &LookupTableMetadata {
        &self.metadata
    }

    /// Returns the lookup table schema.
    #[must_use]
    pub fn lookup_schema(&self) -> &DFSchemaRef {
        &self.lookup_schema
    }

    /// Returns the local predicates (evaluated after the join).
    #[must_use]
    pub fn local_predicates(&self) -> &[Expr] {
        &self.local_predicates
    }

    /// Returns the lookup table alias.
    #[must_use]
    pub fn lookup_alias(&self) -> Option<&str> {
        self.lookup_alias.as_deref()
    }

    /// Returns the stream input alias.
    #[must_use]
    pub fn stream_alias(&self) -> Option<&str> {
        self.stream_alias.as_deref()
    }
}

impl UserDefinedLogicalNodeCore for LookupJoinNode {
    fn name(&self) -> &'static str {
        "LookupJoin"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.join_keys
            .iter()
            .map(|k| k.stream_expr.clone())
            .chain(self.pushdown_predicates.clone())
            .chain(self.local_predicates.clone())
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let keys: Vec<String> = self
            .join_keys
            .iter()
            .map(|k| format!("{}={}", k.stream_expr, k.lookup_column))
            .collect();
        write!(
            f,
            "LookupJoin: table={}, keys=[{}], type={}, pushdown={}, local={}",
            self.lookup_table,
            keys.join(", "),
            self.join_type,
            self.pushdown_predicates.len(),
            self.local_predicates.len(),
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let input = inputs.swap_remove(0);

        // Split expressions: keys | pushdown predicates | local predicates
        let num_keys = self.join_keys.len();
        let num_pushdown = self.pushdown_predicates.len();
        let (key_exprs, rest) = exprs.split_at(num_keys.min(exprs.len()));
        let (pushdown_exprs, local_exprs) = rest.split_at(num_pushdown.min(rest.len()));

        let join_keys: Vec<JoinKeyPair> = key_exprs
            .iter()
            .zip(self.join_keys.iter())
            .map(|(expr, old)| JoinKeyPair {
                stream_expr: expr.clone(),
                lookup_column: old.lookup_column.clone(),
            })
            .collect();

        Ok(Self {
            input: Arc::new(input),
            lookup_table: self.lookup_table.clone(),
            lookup_schema: Arc::clone(&self.lookup_schema),
            join_keys,
            join_type: self.join_type,
            pushdown_predicates: pushdown_exprs.to_vec(),
            local_predicates: local_exprs.to_vec(),
            required_lookup_columns: self.required_lookup_columns.clone(),
            output_schema: Arc::clone(&self.output_schema),
            metadata: self.metadata.clone(),
            lookup_alias: self.lookup_alias.clone(),
            stream_alias: self.stream_alias.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::col;

    fn test_stream_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::try_from(Schema::new(vec![
                Field::new("order_id", DataType::Int64, false),
                Field::new("customer_id", DataType::Int64, false),
                Field::new("amount", DataType::Float64, false),
            ]))
            .unwrap(),
        )
    }

    fn test_lookup_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::try_from(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("region", DataType::Utf8, true),
            ]))
            .unwrap(),
        )
    }

    fn test_output_schema() -> DFSchemaRef {
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

    fn test_metadata() -> LookupTableMetadata {
        LookupTableMetadata {
            connector: "postgres-cdc".to_string(),
            strategy: "replicated".to_string(),
            pushdown_mode: "auto".to_string(),
            primary_key: vec!["id".to_string()],
        }
    }

    fn test_node() -> LookupJoinNode {
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
            LookupJoinType::Inner,
            vec![],
            HashSet::from(["name".to_string(), "region".to_string()]),
            test_output_schema(),
            test_metadata(),
        )
    }

    #[test]
    fn test_name() {
        let node = test_node();
        assert_eq!(node.name(), "LookupJoin");
    }

    #[test]
    fn test_inputs() {
        let node = test_node();
        assert_eq!(node.inputs().len(), 1);
    }

    #[test]
    fn test_schema() {
        let node = test_node();
        assert_eq!(node.schema().fields().len(), 6);
    }

    #[test]
    fn test_expressions() {
        let node = test_node();
        let exprs = node.expressions();
        assert_eq!(exprs.len(), 1); // one join key, no pushdown predicates
    }

    #[test]
    fn test_fmt_for_explain() {
        let node = test_node();
        let explain = format!("{node:?}");
        assert!(explain.contains("LookupJoin"));

        // Test the Display-like explain output
        let mut buf = String::new();
        write!(buf, "{}", DisplayExplain(&node)).unwrap();
        assert!(buf.contains("LookupJoin: table=customers"));
        assert!(buf.contains("type=Inner"));
    }

    #[test]
    fn test_with_exprs_and_inputs_roundtrip() {
        let node = test_node();
        let exprs = node.expressions();
        let inputs: Vec<LogicalPlan> = node.inputs().into_iter().cloned().collect();

        let rebuilt = node.with_exprs_and_inputs(exprs, inputs).unwrap();
        assert_eq!(rebuilt.lookup_table, "customers");
        assert_eq!(rebuilt.join_keys.len(), 1);
        assert_eq!(rebuilt.join_type, LookupJoinType::Inner);
    }

    #[test]
    fn test_left_outer_join() {
        let stream_schema = test_stream_schema();
        let input = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: stream_schema,
        });

        let node = LookupJoinNode::new(
            input,
            "customers".to_string(),
            test_lookup_schema(),
            vec![JoinKeyPair {
                stream_expr: col("customer_id"),
                lookup_column: "id".to_string(),
            }],
            LookupJoinType::LeftOuter,
            vec![],
            HashSet::new(),
            test_output_schema(),
            test_metadata(),
        );

        assert_eq!(node.join_type(), LookupJoinType::LeftOuter);
    }

    /// Helper to test `fmt_for_explain` through the trait method.
    struct DisplayExplain<'a>(&'a LookupJoinNode);

    impl fmt::Display for DisplayExplain<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            UserDefinedLogicalNodeCore::fmt_for_explain(self.0, f)
        }
    }
}
