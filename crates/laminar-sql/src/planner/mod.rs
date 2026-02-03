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

use crate::parser::join_parser::analyze_join;
use crate::parser::order_analyzer::analyze_order_by;
use crate::parser::{
    CreateSinkStatement, CreateSourceStatement, EmitClause, SinkFrom, StreamingStatement,
    WindowFunction, WindowRewriter,
};
use crate::translator::{
    DagExplainOutput, JoinOperatorConfig, OrderOperatorConfig, WindowOperatorConfig,
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
    /// Join configuration if the query has joins
    pub join_config: Option<JoinOperatorConfig>,
    /// ORDER BY configuration if the query has ordering
    pub order_config: Option<OrderOperatorConfig>,
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

                // Check for joins
                let join_analysis = analyze_join(select).map_err(|e| {
                    PlanningError::InvalidQuery(format!("Join analysis failed: {e}"))
                })?;

                // Check for ORDER BY
                let order_analysis = analyze_order_by(stmt);
                let order_config = OrderOperatorConfig::from_analysis(&order_analysis)
                    .map_err(PlanningError::InvalidQuery)?;

                let has_streaming_features =
                    window_function.is_some() || join_analysis.is_some() || order_config.is_some();

                if has_streaming_features {
                    let window_config = match window_function {
                        Some(w) => Some(
                            WindowOperatorConfig::from_window_function(&w)
                                .map_err(|e| PlanningError::InvalidQuery(e.to_string()))?,
                        ),
                        None => None,
                    };

                    let join_config = join_analysis.map(|j| JoinOperatorConfig::from_analysis(&j));

                    return Ok(StreamingPlan::Query(QueryPlan {
                        name: None,
                        window_config,
                        join_config,
                        order_config,
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

                // Extract join info
                if let Some(join) = analyze_join(select).map_err(|e| {
                    PlanningError::InvalidQuery(format!("Join analysis failed: {e}"))
                })? {
                    analysis.join_config = Some(JoinOperatorConfig::from_analysis(&join));
                }
            }
        }

        // Extract ORDER BY info
        let order_analysis = analyze_order_by(stmt);
        analysis.order_config = OrderOperatorConfig::from_analysis(&order_analysis)
            .map_err(PlanningError::InvalidQuery)?;

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
    join_config: Option<JoinOperatorConfig>,
    order_config: Option<OrderOperatorConfig>,
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
                let config = query_plan.join_config.unwrap();
                assert_eq!(config.left_key(), "order_id");
                assert_eq!(config.right_key(), "order_id");
            }
            _ => panic!("Expected Query plan"),
        }
    }
}
