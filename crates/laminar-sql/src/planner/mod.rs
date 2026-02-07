//! Query planner for streaming SQL
//!
//! This module translates parsed streaming SQL statements into execution plans.
//! It integrates with the parser and translator modules to produce complete
//! operator configurations for Ring 0 execution.

pub mod channel_derivation;

use std::collections::HashMap;

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use sqlparser::ast::{ObjectName, SetExpr, Statement};

use crate::parser::analytic_parser::{analyze_analytic_functions, analyze_window_frames, FrameBound};
use crate::parser::join_parser::analyze_joins;
use crate::parser::order_analyzer::analyze_order_by;
use crate::parser::{
    CreateSinkStatement, CreateSourceStatement, EmitClause, SinkFrom, StreamingStatement,
    WindowFunction, WindowRewriter,
};
use crate::parser::aggregation_parser::analyze_aggregates;
use crate::translator::{
    AnalyticWindowConfig, DagExplainOutput, HavingFilterConfig, JoinOperatorConfig,
    OrderOperatorConfig, WindowFrameConfig, WindowOperatorConfig,
};

/// Streaming query planner
pub struct StreamingPlanner {
    /// Registered sources
    sources: HashMap<String, SourceInfo>,
    /// Registered sinks
    sinks: HashMap<String, SinkInfo>,
}

/// Information about a registered source
#[derive(Debug, Clone)]
pub struct SourceInfo {
    /// Source name
    pub name: String,
    /// Watermark column (if configured)
    pub watermark_column: Option<String>,
    /// Connector options
    pub options: HashMap<String, String>,
}

/// Information about a registered sink
#[derive(Debug, Clone)]
pub struct SinkInfo {
    /// Sink name
    pub name: String,
    /// Source table or query name
    pub from: String,
    /// Connector options
    pub options: HashMap<String, String>,
}

/// Result of planning a streaming statement
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum StreamingPlan {
    /// Source registration (DDL)
    RegisterSource(SourceInfo),

    /// Sink registration (DDL)
    RegisterSink(SinkInfo),

    /// Query plan with streaming configurations
    Query(QueryPlan),

    /// Standard SQL statement (pass-through to DataFusion)
    Standard(Box<Statement>),

    /// DAG topology explanation (from EXPLAIN DAG)
    DagExplain(DagExplainOutput),
}

/// A query plan with streaming operator configurations
#[derive(Debug)]
pub struct QueryPlan {
    /// Optional name for the continuous query
    pub name: Option<String>,
    /// Window configuration if the query has windowed aggregation
    pub window_config: Option<WindowOperatorConfig>,
    /// Join configuration(s) if the query has joins (one per join step)
    pub join_config: Option<Vec<JoinOperatorConfig>>,
    /// ORDER BY configuration if the query has ordering
    pub order_config: Option<OrderOperatorConfig>,
    /// Analytic window function configuration (LAG/LEAD/etc.)
    pub analytic_config: Option<AnalyticWindowConfig>,
    /// HAVING clause filter configuration
    pub having_config: Option<HavingFilterConfig>,
    /// Window frame configuration (ROWS BETWEEN / RANGE BETWEEN)
    pub frame_config: Option<WindowFrameConfig>,
    /// Emit strategy
    pub emit_clause: Option<EmitClause>,
    /// The underlying SQL statement
    pub statement: Box<Statement>,
}

impl StreamingPlanner {
    /// Creates a new streaming planner
    #[must_use]
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
            sinks: HashMap::new(),
        }
    }

    /// Plans a streaming statement.
    ///
    /// # Errors
    ///
    /// Returns `PlanningError` if the statement cannot be planned.
    pub fn plan(&mut self, statement: &StreamingStatement) -> Result<StreamingPlan, PlanningError> {
        match statement {
            StreamingStatement::CreateSource(source) => self.plan_create_source(source),
            StreamingStatement::CreateSink(sink) => self.plan_create_sink(sink),
            StreamingStatement::CreateContinuousQuery {
                name,
                query,
                emit_clause,
            }
            | StreamingStatement::CreateStream {
                name,
                query,
                emit_clause,
                ..
            } => self.plan_continuous_query(name, query, emit_clause.as_ref()),
            StreamingStatement::Standard(stmt) => self.plan_standard_statement(stmt),
            StreamingStatement::DropSource { .. }
            | StreamingStatement::DropSink { .. }
            | StreamingStatement::DropStream { .. }
            | StreamingStatement::DropMaterializedView { .. }
            | StreamingStatement::Show(_)
            | StreamingStatement::Describe { .. }
            | StreamingStatement::Explain { .. }
            | StreamingStatement::CreateMaterializedView { .. }
            | StreamingStatement::InsertInto { .. } => {
                // These statements are handled directly by the database facade
                // and don't need query planning. Return as Standard pass-through.
                Err(PlanningError::UnsupportedSql(format!(
                    "Statement type {:?} is handled by the database layer, not the planner",
                    std::mem::discriminant(statement)
                )))
            }
        }
    }

    /// Plans a CREATE SOURCE statement.
    fn plan_create_source(
        &mut self,
        source: &CreateSourceStatement,
    ) -> Result<StreamingPlan, PlanningError> {
        let name = object_name_to_string(&source.name);

        // Check for existing source
        if !source.or_replace && !source.if_not_exists && self.sources.contains_key(&name) {
            return Err(PlanningError::InvalidQuery(format!(
                "Source '{}' already exists",
                name
            )));
        }

        // Extract watermark column
        let watermark_column = source.watermark.as_ref().map(|w| w.column.value.clone());

        let info = SourceInfo {
            name: name.clone(),
            watermark_column,
            options: source.with_options.clone(),
        };

        // Register the source
        self.sources.insert(name, info.clone());

        Ok(StreamingPlan::RegisterSource(info))
    }

    /// Plans a CREATE SINK statement.
    fn plan_create_sink(
        &mut self,
        sink: &CreateSinkStatement,
    ) -> Result<StreamingPlan, PlanningError> {
        let name = object_name_to_string(&sink.name);

        // Check for existing sink
        if !sink.or_replace && !sink.if_not_exists && self.sinks.contains_key(&name) {
            return Err(PlanningError::InvalidQuery(format!(
                "Sink '{}' already exists",
                name
            )));
        }

        // Determine the source
        let from = match &sink.from {
            SinkFrom::Table(table) => object_name_to_string(table),
            SinkFrom::Query(_) => format!("{}_query", name),
        };

        let info = SinkInfo {
            name: name.clone(),
            from,
            options: sink.with_options.clone(),
        };

        // Register the sink
        self.sinks.insert(name, info.clone());

        Ok(StreamingPlan::RegisterSink(info))
    }

    /// Plans a CREATE CONTINUOUS QUERY statement.
    #[allow(clippy::unused_self)] // Will use planner state for query registration
    fn plan_continuous_query(
        &mut self,
        name: &ObjectName,
        query: &StreamingStatement,
        emit_clause: Option<&EmitClause>,
    ) -> Result<StreamingPlan, PlanningError> {
        // The query inside should be a standard SELECT
        let stmt = match query {
            StreamingStatement::Standard(stmt) => stmt.as_ref().clone(),
            _ => {
                return Err(PlanningError::InvalidQuery(
                    "Continuous query must contain a SELECT statement".to_string(),
                ))
            }
        };

        // Analyze the query for streaming features
        let query_plan = Self::analyze_query(&stmt, emit_clause)?;

        Ok(StreamingPlan::Query(QueryPlan {
            name: Some(object_name_to_string(name)),
            window_config: query_plan.window_config,
            join_config: query_plan.join_config,
            order_config: query_plan.order_config,
            analytic_config: query_plan.analytic_config,
            having_config: query_plan.having_config,
            frame_config: query_plan.frame_config,
            emit_clause: emit_clause.cloned(),
            statement: Box::new(stmt),
        }))
    }

    /// Plans a standard SQL statement.
    #[allow(clippy::unused_self)] // Will use planner state for plan optimization
    fn plan_standard_statement(&self, stmt: &Statement) -> Result<StreamingPlan, PlanningError> {
        // Check if it's a query that might have streaming features
        if let Statement::Query(query) = stmt {
            if let SetExpr::Select(select) = query.body.as_ref() {
                // Check for window functions in GROUP BY
                let window_function = Self::extract_window_from_select(select);

                // Check for joins (multi-way)
                let join_analysis = analyze_joins(select).map_err(|e| {
                    PlanningError::InvalidQuery(format!("Join analysis failed: {e}"))
                })?;

                // Check for ORDER BY
                let order_analysis = analyze_order_by(stmt);
                let order_config = OrderOperatorConfig::from_analysis(&order_analysis)
                    .map_err(PlanningError::InvalidQuery)?;

                // Check for analytic functions (LAG/LEAD/etc.)
                let analytic_analysis = analyze_analytic_functions(stmt);
                let analytic_config =
                    analytic_analysis.map(|a| AnalyticWindowConfig::from_analysis(&a));

                // Check for HAVING clause
                let agg_analysis = analyze_aggregates(stmt);
                let having_config = agg_analysis
                    .having_expr
                    .map(HavingFilterConfig::new);

                // Check for window frame functions (ROWS BETWEEN / RANGE BETWEEN)
                let frame_analysis = analyze_window_frames(stmt);
                let frame_config = frame_analysis
                    .as_ref()
                    .map(WindowFrameConfig::from_analysis);

                // Validate: reject UNBOUNDED FOLLOWING (streaming can't buffer infinite future)
                if let Some(fa) = &frame_analysis {
                    for f in &fa.functions {
                        if matches!(f.end_bound, FrameBound::UnboundedFollowing) {
                            return Err(PlanningError::InvalidQuery(
                                "UNBOUNDED FOLLOWING is not supported in streaming window frames"
                                    .to_string(),
                            ));
                        }
                    }
                }

                let has_streaming_features = window_function.is_some()
                    || join_analysis.is_some()
                    || order_config.is_some()
                    || analytic_config.is_some()
                    || having_config.is_some()
                    || frame_config.is_some();

                if has_streaming_features {
                    let window_config = match window_function {
                        Some(w) => Some(
                            WindowOperatorConfig::from_window_function(&w)
                                .map_err(|e| PlanningError::InvalidQuery(e.to_string()))?,
                        ),
                        None => None,
                    };

                    let join_config =
                        join_analysis.map(|m| JoinOperatorConfig::from_multi_analysis(&m));

                    return Ok(StreamingPlan::Query(QueryPlan {
                        name: None,
                        window_config,
                        join_config,
                        order_config,
                        analytic_config,
                        having_config,
                        frame_config,
                        emit_clause: None,
                        statement: Box::new(stmt.clone()),
                    }));
                }
            }
        }

        // Pass through standard SQL
        Ok(StreamingPlan::Standard(Box::new(stmt.clone())))
    }

    /// Analyzes a query for streaming features.
    fn analyze_query(
        stmt: &Statement,
        emit_clause: Option<&EmitClause>,
    ) -> Result<QueryAnalysis, PlanningError> {
        let mut analysis = QueryAnalysis::default();

        if let Statement::Query(query) = stmt {
            if let SetExpr::Select(select) = query.body.as_ref() {
                // Extract window function
                if let Some(window) = Self::extract_window_from_select(select) {
                    let mut config = WindowOperatorConfig::from_window_function(&window)
                        .map_err(|e| PlanningError::InvalidQuery(e.to_string()))?;

                    // Apply emit clause if present
                    if let Some(emit) = emit_clause {
                        config = config
                            .with_emit_clause(emit)
                            .map_err(|e| PlanningError::InvalidQuery(e.to_string()))?;
                    }

                    analysis.window_config = Some(config);
                }

                // Extract join info (multi-way)
                if let Some(multi) = analyze_joins(select).map_err(|e| {
                    PlanningError::InvalidQuery(format!("Join analysis failed: {e}"))
                })? {
                    analysis.join_config =
                        Some(JoinOperatorConfig::from_multi_analysis(&multi));
                }
            }
        }

        // Extract ORDER BY info
        let order_analysis = analyze_order_by(stmt);
        analysis.order_config = OrderOperatorConfig::from_analysis(&order_analysis)
            .map_err(PlanningError::InvalidQuery)?;

        // Extract analytic function info (LAG/LEAD/etc.)
        if let Some(analytic) = analyze_analytic_functions(stmt) {
            analysis.analytic_config = Some(AnalyticWindowConfig::from_analysis(&analytic));
        }

        // Extract HAVING clause
        let agg_analysis = analyze_aggregates(stmt);
        analysis.having_config = agg_analysis
            .having_expr
            .map(HavingFilterConfig::new);

        // Extract window frame functions (ROWS BETWEEN / RANGE BETWEEN)
        if let Some(frame_analysis) = analyze_window_frames(stmt) {
            // Validate: reject UNBOUNDED FOLLOWING
            for f in &frame_analysis.functions {
                if matches!(f.end_bound, FrameBound::UnboundedFollowing) {
                    return Err(PlanningError::InvalidQuery(
                        "UNBOUNDED FOLLOWING is not supported in streaming window frames"
                            .to_string(),
                    ));
                }
            }
            analysis.frame_config = Some(WindowFrameConfig::from_analysis(&frame_analysis));
        }

        Ok(analysis)
    }

    /// Extracts window function from a SELECT.
    fn extract_window_from_select(select: &sqlparser::ast::Select) -> Option<WindowFunction> {
        // Check GROUP BY for window functions
        use sqlparser::ast::GroupByExpr;
        match &select.group_by {
            GroupByExpr::Expressions(exprs, _modifiers) => {
                for group_by_expr in exprs {
                    if let Ok(Some(window)) = WindowRewriter::extract_window_function(group_by_expr)
                    {
                        return Some(window);
                    }
                }
            }
            GroupByExpr::All(_) => {}
        }
        None
    }

    /// Gets a registered source by name.
    #[must_use]
    pub fn get_source(&self, name: &str) -> Option<&SourceInfo> {
        self.sources.get(name)
    }

    /// Gets a registered sink by name.
    #[must_use]
    pub fn get_sink(&self, name: &str) -> Option<&SinkInfo> {
        self.sinks.get(name)
    }

    /// Lists all registered sources.
    #[must_use]
    pub fn list_sources(&self) -> Vec<&SourceInfo> {
        self.sources.values().collect()
    }

    /// Lists all registered sinks.
    #[must_use]
    pub fn list_sinks(&self) -> Vec<&SinkInfo> {
        self.sinks.values().collect()
    }

    /// Creates a `DataFusion` logical plan from a query plan.
    ///
    /// Converts the query plan's SQL statement into a `DataFusion`
    /// `LogicalPlan` using the session context's state. Window UDFs
    /// (TUMBLE, HOP, SESSION) must be registered on the context via
    /// [`register_streaming_functions`](crate::datafusion::register_streaming_functions)
    /// for windowed queries to resolve correctly.
    ///
    /// # Arguments
    ///
    /// * `plan` - The streaming query plan containing the SQL statement
    /// * `ctx` - `DataFusion` session context with registered UDFs
    ///
    /// # Errors
    ///
    /// Returns `PlanningError` if `DataFusion` cannot create the logical plan.
    #[allow(clippy::unused_self)] // Method will use planner state for plan optimization
    pub async fn to_logical_plan(
        &self,
        plan: &QueryPlan,
        ctx: &SessionContext,
    ) -> Result<LogicalPlan, PlanningError> {
        // Convert the AST statement back to SQL and let DataFusion re-parse
        // it with its own sqlparser version. This avoids version mismatches
        // between our sqlparser (0.60) and DataFusion's (0.59).
        let sql = plan.statement.to_string();
        ctx.state()
            .create_logical_plan(&sql)
            .await
            .map_err(PlanningError::DataFusion)
    }
}

impl Default for StreamingPlanner {
    fn default() -> Self {
        Self::new()
    }
}

/// Intermediate query analysis result
#[derive(Debug, Default)]
#[allow(clippy::struct_field_names)]
struct QueryAnalysis {
    window_config: Option<WindowOperatorConfig>,
    join_config: Option<Vec<JoinOperatorConfig>>,
    order_config: Option<OrderOperatorConfig>,
    analytic_config: Option<AnalyticWindowConfig>,
    having_config: Option<HavingFilterConfig>,
    frame_config: Option<WindowFrameConfig>,
}

/// Helper to convert `ObjectName` to String
fn object_name_to_string(name: &ObjectName) -> String {
    name.to_string()
}

/// Planning errors
#[derive(Debug, thiserror::Error)]
pub enum PlanningError {
    /// Unsupported SQL feature
    #[error("Unsupported SQL: {0}")]
    UnsupportedSql(String),

    /// Invalid query
    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    /// Source not found
    #[error("Source not found: {0}")]
    SourceNotFound(String),

    /// Sink not found
    #[error("Sink not found: {0}")]
    SinkNotFound(String),

    /// `DataFusion` error during logical plan creation
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion_common::DataFusionError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::StreamingParser;

    #[test]
    fn test_plan_create_source() {
        let mut planner = StreamingPlanner::new();
        let statements =
            StreamingParser::parse_sql("CREATE SOURCE events (id INT, name VARCHAR)").unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::RegisterSource(info) => {
                assert_eq!(info.name, "events");
            }
            _ => panic!("Expected RegisterSource plan"),
        }
    }

    #[test]
    fn test_plan_create_sink() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql("CREATE SINK output FROM events").unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::RegisterSink(info) => {
                assert_eq!(info.name, "output");
                assert_eq!(info.from, "events");
            }
            _ => panic!("Expected RegisterSink plan"),
        }
    }

    #[test]
    fn test_plan_duplicate_source() {
        let mut planner = StreamingPlanner::new();

        // First source
        let statements =
            StreamingParser::parse_sql("CREATE SOURCE events (id INT, name VARCHAR)").unwrap();
        planner.plan(&statements[0]).unwrap();

        // Duplicate should fail
        let result = planner.plan(&statements[0]);
        assert!(result.is_err());
    }

    #[test]
    fn test_plan_source_if_not_exists() {
        let mut planner = StreamingPlanner::new();

        // First source
        let statements =
            StreamingParser::parse_sql("CREATE SOURCE events (id INT, name VARCHAR)").unwrap();
        planner.plan(&statements[0]).unwrap();

        // IF NOT EXISTS should succeed
        let statements =
            StreamingParser::parse_sql("CREATE SOURCE IF NOT EXISTS events (id INT, name VARCHAR)")
                .unwrap();
        let result = planner.plan(&statements[0]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_plan_source_or_replace() {
        let mut planner = StreamingPlanner::new();

        // First source
        let statements =
            StreamingParser::parse_sql("CREATE SOURCE events (id INT, name VARCHAR)").unwrap();
        planner.plan(&statements[0]).unwrap();

        // OR REPLACE should succeed
        let statements =
            StreamingParser::parse_sql("CREATE OR REPLACE SOURCE events (id INT, name VARCHAR)")
                .unwrap();
        let result = planner.plan(&statements[0]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_plan_source_with_watermark() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "CREATE SOURCE events (
                id INT,
                ts TIMESTAMP,
                WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
            )",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::RegisterSource(info) => {
                assert_eq!(info.name, "events");
                assert_eq!(info.watermark_column, Some("ts".to_string()));
            }
            _ => panic!("Expected RegisterSource plan"),
        }
    }

    #[test]
    fn test_plan_standard_select() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql("SELECT * FROM events").unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Standard(_) => {}
            _ => panic!("Expected Standard plan for simple SELECT"),
        }
    }

    #[test]
    fn test_list_sources_and_sinks() {
        let mut planner = StreamingPlanner::new();

        // Create sources
        let s1 = StreamingParser::parse_sql("CREATE SOURCE src1 (id INT)").unwrap();
        let s2 = StreamingParser::parse_sql("CREATE SOURCE src2 (id INT)").unwrap();
        planner.plan(&s1[0]).unwrap();
        planner.plan(&s2[0]).unwrap();

        // Create sinks
        let k1 = StreamingParser::parse_sql("CREATE SINK sink1 FROM src1").unwrap();
        planner.plan(&k1[0]).unwrap();

        assert_eq!(planner.list_sources().len(), 2);
        assert_eq!(planner.list_sinks().len(), 1);
        assert!(planner.get_source("src1").is_some());
        assert!(planner.get_sink("sink1").is_some());
    }

    #[test]
    fn test_plan_query_with_window() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT COUNT(*) FROM events GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE)",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Query(query_plan) => {
                assert!(query_plan.window_config.is_some());
                let config = query_plan.window_config.unwrap();
                assert_eq!(config.time_column, "event_time");
                assert_eq!(config.size.as_secs(), 300);
            }
            _ => panic!("Expected Query plan"),
        }
    }

    #[test]
    fn test_plan_query_with_join() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT * FROM orders o JOIN payments p ON o.order_id = p.order_id",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Query(query_plan) => {
                assert!(query_plan.join_config.is_some());
                let configs = query_plan.join_config.unwrap();
                assert_eq!(configs.len(), 1);
                assert_eq!(configs[0].left_key(), "order_id");
                assert_eq!(configs[0].right_key(), "order_id");
            }
            _ => panic!("Expected Query plan"),
        }
    }

    #[test]
    fn test_plan_query_with_lag() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT price, LAG(price) OVER (PARTITION BY symbol ORDER BY ts) AS prev FROM trades",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Query(query_plan) => {
                assert!(query_plan.analytic_config.is_some());
                let config = query_plan.analytic_config.unwrap();
                assert_eq!(config.functions.len(), 1);
                assert_eq!(config.partition_columns, vec!["symbol".to_string()]);
            }
            _ => panic!("Expected Query plan with analytic config"),
        }
    }

    #[test]
    fn test_plan_query_with_having() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT symbol, COUNT(*) AS cnt FROM trades \
             GROUP BY symbol, TUMBLE(ts, INTERVAL '5' MINUTE) \
             HAVING COUNT(*) > 10",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Query(query_plan) => {
                assert!(query_plan.window_config.is_some());
                assert!(query_plan.having_config.is_some());
                let config = query_plan.having_config.unwrap();
                assert!(
                    config.predicate().contains("COUNT(*)"),
                    "predicate was: {}",
                    config.predicate()
                );
            }
            _ => panic!("Expected Query plan with having config"),
        }
    }

    #[test]
    fn test_plan_query_without_having() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT COUNT(*) FROM events GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE)",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Query(query_plan) => {
                assert!(query_plan.having_config.is_none());
            }
            _ => panic!("Expected Query plan"),
        }
    }

    #[test]
    fn test_plan_having_only_produces_query_plan() {
        // HAVING without window function still produces a Query plan
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT category, SUM(amount) FROM orders GROUP BY category HAVING SUM(amount) > 1000",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Query(query_plan) => {
                assert!(query_plan.having_config.is_some());
                assert!(query_plan.window_config.is_none());
            }
            _ => panic!("Expected Query plan for HAVING-only query"),
        }
    }

    #[test]
    fn test_plan_having_compound_predicate() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT symbol, COUNT(*) AS cnt, SUM(vol) AS total \
             FROM trades GROUP BY symbol \
             HAVING COUNT(*) >= 5 AND SUM(vol) > 10000",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Query(query_plan) => {
                let config = query_plan.having_config.unwrap();
                let pred = config.predicate();
                assert!(pred.contains("AND"), "predicate was: {pred}");
            }
            _ => panic!("Expected Query plan"),
        }
    }

    #[test]
    fn test_plan_query_with_lead() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT LEAD(price, 2) OVER (ORDER BY ts) AS next2 FROM trades",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Query(query_plan) => {
                assert!(query_plan.analytic_config.is_some());
                let config = query_plan.analytic_config.unwrap();
                assert!(config.has_lookahead());
                assert_eq!(config.functions[0].offset, 2);
            }
            _ => panic!("Expected Query plan with analytic config"),
        }
    }

    // -- Multi-way join planner tests (F-SQL-005) --

    #[test]
    fn test_plan_single_join_produces_vec_of_one() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT * FROM a JOIN b ON a.id = b.a_id",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Query(qp) => {
                let configs = qp.join_config.unwrap();
                assert_eq!(configs.len(), 1);
            }
            _ => panic!("Expected Query plan"),
        }
    }

    #[test]
    fn test_plan_two_way_join() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Query(qp) => {
                let configs = qp.join_config.unwrap();
                assert_eq!(configs.len(), 2);
                assert_eq!(configs[0].left_key(), "id");
                assert_eq!(configs[0].right_key(), "a_id");
                assert_eq!(configs[1].left_key(), "id");
                assert_eq!(configs[1].right_key(), "b_id");
            }
            _ => panic!("Expected Query plan"),
        }
    }

    #[test]
    fn test_plan_mixed_join_types() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT * FROM orders o \
             JOIN payments p ON o.id = p.order_id \
                 AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR \
             JOIN customers c ON p.cust_id = c.id",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Query(qp) => {
                let configs = qp.join_config.unwrap();
                assert_eq!(configs.len(), 2);
                assert!(configs[0].is_stream_stream());
                assert!(configs[1].is_lookup());
            }
            _ => panic!("Expected Query plan"),
        }
    }

    #[test]
    fn test_plan_backward_compat_no_join() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT * FROM orders",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Standard(_) => {} // No join → pass-through
            _ => panic!("Expected Standard plan for simple SELECT"),
        }
    }

    // -- Window Frame planner tests (F-SQL-006) --

    #[test]
    fn test_plan_query_with_rows_frame() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT AVG(price) OVER (ORDER BY ts \
             ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma FROM trades",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Query(qp) => {
                assert!(qp.frame_config.is_some());
                let fc = qp.frame_config.unwrap();
                assert_eq!(fc.functions.len(), 1);
                assert_eq!(fc.functions[0].source_column, "price");
            }
            _ => panic!("Expected Query plan with frame_config"),
        }
    }

    #[test]
    fn test_plan_frame_with_partition() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT AVG(price) OVER (PARTITION BY symbol ORDER BY ts \
             ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS ma FROM trades",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Query(qp) => {
                let fc = qp.frame_config.unwrap();
                assert_eq!(fc.partition_columns, vec!["symbol".to_string()]);
                assert_eq!(fc.order_columns, vec!["ts".to_string()]);
            }
            _ => panic!("Expected Query plan with frame_config"),
        }
    }

    #[test]
    fn test_plan_no_frame_is_standard() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT * FROM trades",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Standard(_) => {} // No frame → pass-through
            _ => panic!("Expected Standard plan for simple SELECT"),
        }
    }

    #[test]
    fn test_plan_unbounded_following_rejected() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT SUM(amount) OVER (ORDER BY id \
             ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS rest \
             FROM orders",
        )
        .unwrap();

        let result = planner.plan(&statements[0]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("UNBOUNDED FOLLOWING"),
            "error was: {err}"
        );
    }
}
