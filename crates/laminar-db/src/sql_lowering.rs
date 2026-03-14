//! SQL lowering pass: converts registered stream queries into a DAG operator
//! graph using the existing `try_from_sql` constructors.
//!
//! This module bridges the gap between SQL query registration (Phase C) and
//! the unified DAG execution substrate (Phase B). Each stream query is
//! lowered to one or more DAG nodes with the appropriate SQL operator
//! adapter.
//!
//! # Lowering Rules
//!
//! | SQL Pattern | DAG Node |
//! |---|---|
//! | `SELECT agg(...) GROUP BY x` | `AggregateAdapter` wrapping `IncrementalAggState` |
//! | Windowed aggregate with `EMIT ON WINDOW CLOSE` | `WindowAggAdapter` wrapping `CoreWindowState` |
//! | `SELECT cols FROM t WHERE pred` | Passthrough — `DataFusion` fallback |
//! | ASOF JOIN / TEMPORAL JOIN | Not lowered — fallback to `StreamExecutor` |
//!
//! Queries that cannot be lowered return `None`, signaling the caller to
//! use the existing `StreamExecutor` path as a fallback.

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::prelude::SessionContext;
use laminar_core::dag::{DagBuilder, NodeId, StreamingDag};
use laminar_core::operator::Operator;

/// A built DAG topology paired with its operators to register.
pub(crate) type BuiltDag = (StreamingDag, Vec<(NodeId, Box<dyn Operator>)>);

use crate::aggregate_state::IncrementalAggState;
use crate::core_window_state::CoreWindowState;
use crate::error::DbError;
use crate::sql_operators::{AggregateAdapter, WindowAggAdapter};

use laminar_sql::parser::EmitClause;
use laminar_sql::translator::WindowOperatorConfig;

/// Result of lowering a single stream query.
pub(crate) enum LoweredQuery {
    /// Query was lowered to an incremental aggregate operator.
    Aggregate {
        /// The operator to register in the DAG executor.
        operator: Box<dyn Operator>,
        /// Schema of the operator's output.
        output_schema: Arc<Schema>,
        /// Pre-aggregation SQL that extracts/transforms columns from the
        /// source batch. This must be evaluated before feeding the batch
        /// to the operator.
        pre_agg_sql: String,
        /// Optional HAVING filter SQL applied after emission.
        having_sql: Option<String>,
        /// Source tables referenced by this query.
        source_tables: Vec<String>,
    },
    /// Query was lowered to a windowed aggregate operator.
    WindowedAggregate {
        /// The operator to register in the DAG executor.
        operator: Box<dyn Operator>,
        /// Schema of the operator's output.
        output_schema: Arc<Schema>,
        /// Pre-aggregation SQL for column extraction.
        pre_agg_sql: String,
        /// Source tables referenced by this query.
        source_tables: Vec<String>,
    },
    /// Query cannot be lowered — use `StreamExecutor` fallback.
    ///
    /// This includes non-aggregate queries (simple `SELECT`), ASOF joins,
    /// temporal joins, and any pattern the `try_from_sql` constructors
    /// don't recognize.
    Fallback,
}

/// Attempt to lower a stream query to a DAG operator.
///
/// Uses the existing [`IncrementalAggState::try_from_sql`] and
/// [`CoreWindowState::try_from_sql`] constructors for plan introspection.
/// These run `DataFusion` plan analysis at registration time only — the
/// resulting operators execute without `DataFusion` at runtime.
///
/// # Arguments
///
/// * `ctx` — `DataFusion` session context for plan introspection
/// * `query_name` — Name of the stream query (used as operator ID)
/// * `sql` — The SQL query text
/// * `window_config` — Window configuration (if EMIT ON WINDOW CLOSE)
/// * `emit_clause` — Emission strategy
///
/// # Returns
///
/// `LoweredQuery::Aggregate` or `LoweredQuery::WindowedAggregate` if the
/// query was successfully lowered, `LoweredQuery::Fallback` otherwise.
pub(crate) async fn try_lower_query(
    ctx: &SessionContext,
    query_name: &str,
    sql: &str,
    source_tables: Vec<String>,
    window_config: Option<&WindowOperatorConfig>,
    emit_clause: Option<&EmitClause>,
) -> Result<LoweredQuery, DbError> {
    // Try windowed aggregate first (EMIT ON WINDOW CLOSE queries).
    if let Some(wc) = window_config {
        if let Some(ec) = emit_clause {
            match CoreWindowState::try_from_sql(ctx, sql, wc, Some(ec)).await {
                Ok(Some(state)) => {
                    let output_schema = state.output_schema();
                    let pre_agg_sql = state.pre_agg_sql().to_string();
                    return Ok(LoweredQuery::WindowedAggregate {
                        operator: Box::new(WindowAggAdapter::new(state, query_name.to_string())),
                        output_schema,
                        pre_agg_sql,
                        source_tables: source_tables.clone(),
                    });
                }
                Ok(None) => {
                    // CoreWindowState can't handle this pattern — fall through
                    // to incremental agg or fallback.
                }
                Err(e) => {
                    tracing::debug!(
                        query = %query_name,
                        error = %e,
                        "CoreWindowState::try_from_sql failed, trying fallback"
                    );
                }
            }
        }
    }

    // Try incremental aggregate (non-windowed GROUP BY).
    match IncrementalAggState::try_from_sql(ctx, sql).await {
        Ok(Some(state)) => {
            let output_schema = state.output_schema();
            let pre_agg_sql = state.pre_agg_sql().to_string();
            let having_sql = state.having_sql().map(String::from);
            Ok(LoweredQuery::Aggregate {
                operator: Box::new(AggregateAdapter::new(state, query_name.to_string())),
                output_schema,
                pre_agg_sql,
                having_sql,
                source_tables,
            })
        }
        Ok(None) => {
            // Not an aggregation query — fallback to DataFusion execution.
            Ok(LoweredQuery::Fallback)
        }
        Err(e) => {
            tracing::debug!(
                query = %query_name,
                error = %e,
                "IncrementalAggState::try_from_sql failed, using fallback"
            );
            Ok(LoweredQuery::Fallback)
        }
    }
}

/// Build a DAG for a set of lowered queries connected to source nodes.
///
/// Creates a `StreamingDag` with:
/// - One source node per input stream
/// - One operator node per lowered query
/// - One sink node per query output
/// - Edges from source → operator → sink
///
/// Queries that returned `LoweredQuery::Fallback` are excluded from the
/// DAG (they remain on the `StreamExecutor` path).
///
/// # Returns
///
/// The topology and the operators to register, or `None` if no queries
/// were lowered.
pub(crate) fn build_query_dag(
    source_schemas: &[(&str, Arc<Schema>)],
    lowered: &mut [(String, LoweredQuery)],
) -> Option<BuiltDag> {
    // Filter out fallback queries.
    let has_lowered = lowered
        .iter()
        .any(|(_, lq)| !matches!(lq, LoweredQuery::Fallback));
    if !has_lowered {
        return None;
    }

    let mut builder = DagBuilder::new();

    // Add source nodes.
    for &(name, ref schema) in source_schemas {
        builder = builder.source(name, schema.clone());
    }

    let mut operators_to_register: Vec<(String, Box<dyn Operator>)> = Vec::new();
    // Track (op_name, source_tables) for wiring edges after all nodes are added.
    let mut op_sources: Vec<(String, Vec<String>)> = Vec::new();

    // Build a lookup of available source names for validation.
    let available_sources: rustc_hash::FxHashSet<&str> =
        source_schemas.iter().map(|&(name, _)| name).collect();

    // Add operator and sink nodes for each lowered query.
    for (query_name, lowered_query) in lowered.iter_mut() {
        let (operator, output_schema, source_tables) = match lowered_query {
            LoweredQuery::Aggregate {
                operator,
                output_schema,
                source_tables,
                ..
            }
            | LoweredQuery::WindowedAggregate {
                operator,
                output_schema,
                source_tables,
                ..
            } => (operator, output_schema.clone(), source_tables.clone()),
            LoweredQuery::Fallback => continue,
        };

        let op_name = format!("op_{query_name}");
        let sink_name = format!("sink_{query_name}");

        builder = builder.operator(&op_name, output_schema.clone()).sink_for(
            &op_name,
            &sink_name,
            output_schema,
        );

        let op = std::mem::replace(
            operator,
            Box::new(crate::sql_operators::ProjectionAdapter::new(
                vec![],
                Arc::new(Schema::empty()),
                String::new(),
            )),
        );
        operators_to_register.push((op_name.clone(), op));
        op_sources.push((op_name, source_tables));
    }

    // Connect each operator to its source tables.
    for (op_name, source_tables) in &op_sources {
        for table in source_tables {
            if available_sources.contains(table.as_str()) {
                builder = builder.connect(table, op_name);
            } else {
                tracing::warn!(
                    operator = %op_name,
                    source = %table,
                    "source not found in DAG — skipping edge"
                );
            }
        }
    }

    let dag = match builder.build() {
        Ok(dag) => dag,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to build query DAG");
            return None;
        }
    };

    // Resolve node IDs and pair with operators.
    let mut resolved: Vec<(NodeId, Box<dyn Operator>)> = Vec::new();
    for (op_name, operator) in operators_to_register {
        if let Some(node_id) = dag.node_id_by_name(&op_name) {
            resolved.push((node_id, operator));
        }
    }

    Some((dag, resolved))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use datafusion::prelude::SessionContext;
    use laminar_core::dag::DagExecutor;

    fn trades_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("volume", DataType::Int64, false),
        ]))
    }

    async fn ctx_with_trades() -> SessionContext {
        let ctx = laminar_sql::create_session_context();
        let schema = trades_schema();
        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
        ctx.register_table("trades", Arc::new(mem_table)).unwrap();
        ctx
    }

    #[tokio::test]
    async fn test_lower_aggregate_query() {
        let ctx = ctx_with_trades().await;
        let result = try_lower_query(
            &ctx,
            "trade_summary",
            "SELECT symbol, SUM(volume) AS total_volume FROM trades GROUP BY symbol",
            vec!["trades".to_string()],
            None,
            None,
        )
        .await
        .unwrap();

        match result {
            LoweredQuery::Aggregate {
                output_schema,
                pre_agg_sql,
                having_sql,
                ..
            } => {
                // Output schema should have group col + agg result
                assert!(output_schema.fields().len() >= 2);
                // Pre-agg SQL should reference trades
                assert!(
                    pre_agg_sql.to_lowercase().contains("trades"),
                    "pre_agg_sql should reference source: {pre_agg_sql}"
                );
                // No HAVING
                assert!(having_sql.is_none());
            }
            other => panic!("Expected Aggregate, got {:?}", variant_name(&other)),
        }
    }

    #[tokio::test]
    async fn test_lower_non_aggregate_returns_fallback() {
        let ctx = ctx_with_trades().await;
        let result = try_lower_query(
            &ctx,
            "passthrough",
            "SELECT symbol, price FROM trades WHERE price > 100",
            vec!["trades".to_string()],
            None,
            None,
        )
        .await
        .unwrap();

        assert!(
            matches!(result, LoweredQuery::Fallback),
            "Non-aggregate SELECT should return Fallback"
        );
    }

    #[tokio::test]
    async fn test_lower_aggregate_with_having() {
        let ctx = ctx_with_trades().await;
        let result = try_lower_query(
            &ctx,
            "filtered_agg",
            "SELECT symbol, SUM(volume) AS tv FROM trades GROUP BY symbol HAVING SUM(volume) > 1000",
            vec!["trades".to_string()],
            None,
            None,
        )
        .await
        .unwrap();

        match result {
            LoweredQuery::Aggregate { having_sql, .. } => {
                assert!(having_sql.is_some(), "HAVING should be detected");
            }
            other => panic!("Expected Aggregate, got {:?}", variant_name(&other)),
        }
    }

    #[tokio::test]
    async fn test_build_query_dag() {
        let ctx = ctx_with_trades().await;
        let schema = trades_schema();

        // Lower a query.
        let lowered = try_lower_query(
            &ctx,
            "agg1",
            "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol",
            vec!["trades".to_string()],
            None,
            None,
        )
        .await
        .unwrap();

        let mut queries = vec![("agg1".to_string(), lowered)];
        let sources = vec![("trades", schema)];

        let result = build_query_dag(&sources, &mut queries);
        assert!(result.is_some(), "Should build DAG for lowered aggregate");

        let (dag, operators) = result.unwrap();
        assert!(!operators.is_empty(), "Should have at least one operator");

        // Create executor and register operators.
        let mut executor = DagExecutor::from_dag(&dag);
        for (node_id, operator) in operators {
            executor.register_operator(node_id, operator);
        }

        // Verify source and sink nodes exist.
        assert!(dag.node_id_by_name("trades").is_some());
        assert!(dag.node_id_by_name("op_agg1").is_some());
        assert!(dag.node_id_by_name("sink_agg1").is_some());
    }

    #[tokio::test]
    async fn test_build_dag_all_fallback_returns_none() {
        let ctx = ctx_with_trades().await;
        let schema = trades_schema();

        // Lower a non-aggregate query (returns Fallback).
        let lowered = try_lower_query(
            &ctx,
            "passthrough",
            "SELECT * FROM trades",
            vec!["trades".to_string()],
            None,
            None,
        )
        .await
        .unwrap();

        let mut queries = vec![("passthrough".to_string(), lowered)];
        let sources = vec![("trades", schema)];

        let result = build_query_dag(&sources, &mut queries);
        assert!(result.is_none(), "All-fallback should return None");
    }

    fn variant_name(lq: &LoweredQuery) -> &'static str {
        match lq {
            LoweredQuery::Aggregate { .. } => "Aggregate",
            LoweredQuery::WindowedAggregate { .. } => "WindowedAggregate",
            LoweredQuery::Fallback => "Fallback",
        }
    }
}
