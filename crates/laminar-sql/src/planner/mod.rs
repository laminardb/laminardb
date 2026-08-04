//! Query planner for streaming SQL
//!
//! This module translates parsed streaming SQL statements into execution plans.
//! It integrates with the parser and translator modules to produce complete
//! operator configurations for Ring 0 execution.

pub mod channel_derivation;
/// Optimizer rules for lookup join rewriting.
pub mod lookup_join;
/// Predicate splitting and pushdown for lookup joins.
pub mod predicate_split;
/// Physical optimizer rule for streaming plan validation.
pub mod streaming_optimizer;

#[allow(clippy::disallowed_types)] // cold path: query planning
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use sqlparser::ast::{ObjectName, Select, SetExpr, Statement, TableFactor};

use crate::parser::aggregation_parser::analyze_aggregates;
use crate::parser::analytic_parser::{
    analyze_analytic_functions, analyze_window_frames, FrameBound,
};
use crate::parser::join_parser::{analyze_joins, JoinAnalysis, JoinType, MultiJoinAnalysis};
use crate::parser::lookup_table::{validate_properties, LookupTableProperties};
use crate::parser::order_analyzer::analyze_order_by;
use crate::parser::{
    CreateLookupTableStatement, CreateSinkStatement, CreateSourceStatement, EmitClause, SinkFrom,
    StreamingStatement, WindowFunction, WindowRewriter,
};
use crate::temporal::temporal_table_version_count;
use crate::translator::{
    AnalyticWindowConfig, HavingFilterConfig, JoinOperatorConfig, OrderOperatorConfig,
    WindowFrameConfig, WindowOperatorConfig,
};

/// Information about a registered lookup table.
#[derive(Debug, Clone)]
pub struct LookupTableInfo {
    /// Table name.
    pub name: String,
    /// Column names and types.
    pub columns: Vec<(String, String)>,
    /// Primary key columns.
    pub primary_key: Vec<String>,
    /// Validated properties.
    pub properties: LookupTableProperties,
    /// Pre-computed Arrow schema from column definitions.
    pub arrow_schema: SchemaRef,
    /// Raw WITH options for connector configuration pass-through.
    pub raw_options: HashMap<String, String>,
}

/// Streaming query planner
pub struct StreamingPlanner {
    /// Registered sources
    sources: HashMap<String, SourceInfo>,
    /// Registered sinks
    sinks: HashMap<String, SinkInfo>,
    /// Registered lookup tables
    lookup_tables: HashMap<String, LookupTableInfo>,
    /// Names of views/streams for which planning retains window classification.
    windowed_views: std::collections::HashSet<String>,
}

/// Information about a registered source
#[derive(Debug, Clone)]
pub struct SourceInfo {
    /// Source name
    pub name: String,
    /// Watermark column (if configured)
    pub watermark_column: Option<String>,
    /// Declared non-null primary-key columns.
    pub primary_key: Vec<String>,
}

/// Information about a registered sink
#[derive(Debug, Clone)]
pub struct SinkInfo {
    /// Sink name
    pub name: String,
    /// Source table or query name
    pub from: String,
}

fn is_inline_unnest(factor: &TableFactor) -> bool {
    match factor {
        TableFactor::UNNEST { .. } => true,
        TableFactor::Table {
            name,
            args: Some(_),
            ..
        }
        | TableFactor::Function { name, .. } => {
            name.0.len() == 1 && name.to_string().eq_ignore_ascii_case("unnest")
        }
        _ => false,
    }
}

fn has_implicit_multi_source(select: &Select) -> bool {
    select
        .from
        .iter()
        .filter(|from| !is_inline_unnest(&from.relation))
        .count()
        > 1
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

    /// Lookup table registration (DDL)
    RegisterLookupTable(LookupTableInfo),

    /// Drop a lookup table
    DropLookupTable {
        /// Name of the lookup table to drop.
        name: String,
    },
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

/// Exact one-call admission for database-certified changelog-to-static enrichment.
///
/// The certificate is matched against the planner's independent parse, so it
/// cannot authorize another relation pair, key mapping, or join type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangelogEnrichAdmission {
    left_table: String,
    right_table: String,
    left_keys: Vec<String>,
    right_keys: Vec<String>,
    join_type: JoinType,
}

impl ChangelogEnrichAdmission {
    /// Construct an exact INNER or LEFT changelog enrichment admission.
    ///
    /// # Errors
    /// Returns an error for empty relations/keys or a mismatched key arity.
    pub fn try_new(
        left_table: impl Into<String>,
        right_table: impl Into<String>,
        left_keys: Vec<String>,
        right_keys: Vec<String>,
        left_outer: bool,
    ) -> Result<Self, String> {
        let left_table = left_table.into();
        let right_table = right_table.into();
        if left_table.is_empty() || right_table.is_empty() {
            return Err("changelog enrichment relations cannot be empty".into());
        }
        if left_keys.is_empty()
            || left_keys.len() != right_keys.len()
            || left_keys.iter().chain(&right_keys).any(String::is_empty)
        {
            return Err("changelog enrichment keys must be non-empty with matching arity".into());
        }
        Ok(Self {
            left_table,
            right_table,
            left_keys,
            right_keys,
            join_type: if left_outer {
                JoinType::Left
            } else {
                JoinType::Inner
            },
        })
    }

    fn matches(&self, step: &JoinAnalysis) -> bool {
        let mut left_keys = Vec::with_capacity(1 + step.additional_key_columns.len());
        let mut right_keys = Vec::with_capacity(1 + step.additional_key_columns.len());
        left_keys.push(step.left_key_column.as_str());
        right_keys.push(step.right_key_column.as_str());
        for (left, right) in &step.additional_key_columns {
            left_keys.push(left);
            right_keys.push(right);
        }
        self.left_table == step.left_table
            && self.right_table == step.right_table
            && self.join_type == step.join_type
            && self.left_keys.iter().map(String::as_str).eq(left_keys)
            && self.right_keys.iter().map(String::as_str).eq(right_keys)
            && !step.is_temporal_join()
            && step.time_bound.is_none()
    }
}

impl StreamingPlanner {
    /// Creates a new streaming planner
    #[must_use]
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
            sinks: HashMap::new(),
            lookup_tables: HashMap::new(),
            windowed_views: std::collections::HashSet::new(),
        }
    }

    /// Plans a streaming statement.
    ///
    /// # Errors
    ///
    /// Returns `PlanningError` if the statement cannot be planned.
    pub fn plan(&mut self, statement: &StreamingStatement) -> Result<StreamingPlan, PlanningError> {
        self.plan_internal(statement, None)
    }

    /// Plan one query whose unbounded join shape has been certified as
    /// changelog-to-static enrichment by the database.
    ///
    /// This does not relax multi-way, join-type, temporal, or implicit-join
    /// validation. Raw stream callers must use [`Self::plan`].
    ///
    /// # Errors
    /// Returns `PlanningError` if the statement fails the remaining streaming checks.
    pub fn plan_changelog_enrich(
        &mut self,
        statement: &StreamingStatement,
        admission: &ChangelogEnrichAdmission,
    ) -> Result<StreamingPlan, PlanningError> {
        if !matches!(
            statement,
            StreamingStatement::CreateContinuousQuery { .. }
                | StreamingStatement::CreateStream { .. }
        ) {
            return Err(PlanningError::InvalidQuery(
                "changelog enrichment admission is valid only for a named streaming query".into(),
            ));
        }
        self.plan_internal(statement, Some(admission))
    }

    fn plan_internal(
        &mut self,
        statement: &StreamingStatement,
        changelog_enrich: Option<&ChangelogEnrichAdmission>,
    ) -> Result<StreamingPlan, PlanningError> {
        match statement {
            StreamingStatement::CreateSource(source) => self.plan_create_source(source),
            StreamingStatement::CreateSink(sink) => self.plan_create_sink(sink),
            StreamingStatement::CreateContinuousQuery {
                name,
                query,
                emit_clause,
                ..
            }
            | StreamingStatement::CreateStream {
                name,
                query,
                emit_clause,
                ..
            } => self.plan_continuous_query(name, query, emit_clause.as_ref(), changelog_enrich),
            StreamingStatement::Standard(stmt) => self.plan_standard_statement(stmt, None),
            StreamingStatement::TemporalProbeQuery {
                statement,
                analysis,
            } => self.plan_standard_statement(statement, Some(analysis)),
            StreamingStatement::CreateLookupTable(lt) => self.plan_create_lookup_table(lt),
            StreamingStatement::DropLookupTable { name, if_exists } => {
                self.plan_drop_lookup_table(name, *if_exists)
            }
            StreamingStatement::DropSource { .. }
            | StreamingStatement::DropSink { .. }
            | StreamingStatement::DropStream { .. }
            | StreamingStatement::DropMaterializedView { .. }
            | StreamingStatement::Show(_)
            | StreamingStatement::Describe { .. }
            | StreamingStatement::Explain { .. }
            | StreamingStatement::CreateMaterializedView { .. }
            | StreamingStatement::InsertInto { .. }
            | StreamingStatement::AlterSource { .. }
            | StreamingStatement::Checkpoint
            | StreamingStatement::RestoreCheckpoint { .. }
            | StreamingStatement::Subscribe(_)
            | StreamingStatement::DeclareCursorForSubscribe { .. } => {
                // These statements are handled directly by the database facade
                // and don't need query planning. Return as Standard pass-through.
                Err(PlanningError::UnsupportedSql(format!(
                    "Statement type {:?} is handled by the database layer, not the planner",
                    std::mem::discriminant(statement)
                )))
            }
        }
    }

    /// Remove query classification installed by a successful plan when the surrounding catalog
    /// transaction is rolled back or the query is dropped.
    pub fn unregister_query(&mut self, name: &str) {
        self.windowed_views.remove(name);
    }

    /// Whether query classification state remains for a catalog object.
    #[must_use]
    pub fn has_query(&self, name: &str) -> bool {
        self.windowed_views.contains(name)
    }

    /// Remove a source installed by a catalog transaction that was dropped or rolled back.
    pub fn unregister_source(&mut self, name: &str) {
        self.sources.remove(name);
    }

    /// Remove a sink installed by a catalog transaction that was dropped or rolled back.
    pub fn unregister_sink(&mut self, name: &str) {
        self.sinks.remove(name);
    }

    /// Remove a lookup table installed by a catalog transaction that was dropped or rolled back.
    pub fn unregister_lookup_table(&mut self, name: &str) {
        self.lookup_tables.remove(name);
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
            primary_key: source
                .primary_key
                .iter()
                .map(|column| column.value.clone())
                .collect(),
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
        };

        // Register the sink
        self.sinks.insert(name, info.clone());

        Ok(StreamingPlan::RegisterSink(info))
    }

    /// Plans a CREATE CONTINUOUS QUERY statement.
    fn plan_continuous_query(
        &mut self,
        name: &ObjectName,
        query: &StreamingStatement,
        emit_clause: Option<&EmitClause>,
        changelog_enrich: Option<&ChangelogEnrichAdmission>,
    ) -> Result<StreamingPlan, PlanningError> {
        // The query inside should be a standard SELECT
        let (stmt, temporal_probe) = match query {
            StreamingStatement::Standard(stmt) => (stmt.as_ref().clone(), None),
            StreamingStatement::TemporalProbeQuery {
                statement,
                analysis,
            } => (statement.as_ref().clone(), Some(analysis.as_ref())),
            _ => {
                return Err(PlanningError::InvalidQuery(
                    "Continuous query must contain a SELECT statement".to_string(),
                ))
            }
        };

        // Analyze the query for streaming features
        let query_plan =
            self.analyze_query(&stmt, emit_clause, changelog_enrich, temporal_probe)?;

        // Keep planner classification in sync with catalog rollback/drop. A windowed query is the
        // only query shape for which the planner retains classification after planning.
        let view_name = object_name_to_string(name);
        if query_plan.window_config.is_some() {
            self.windowed_views.insert(view_name);
        } else {
            self.windowed_views.remove(&view_name);
        }

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
    fn plan_standard_statement(
        &self,
        stmt: &Statement,
        temporal_probe: Option<&JoinAnalysis>,
    ) -> Result<StreamingPlan, PlanningError> {
        // Check if it's a query that might have streaming features
        if let Statement::Query(query) = stmt {
            if let SetExpr::Select(select) = query.body.as_ref() {
                if has_implicit_multi_source(select) {
                    return Err(PlanningError::InvalidQuery(
                        "implicit multi-source joins are unsupported; use one bounded INNER JOIN"
                            .to_string(),
                    ));
                }
                // Check for window functions in GROUP BY
                let window_function = Self::extract_window_from_select(select);

                // Check for joins (multi-way)
                let mut join_analysis = analyze_joins(select).map_err(|e| {
                    PlanningError::InvalidQuery(format!("Join analysis failed: {e}"))
                })?;

                validate_temporal_version_shape(
                    stmt,
                    join_analysis.as_ref().map_or(0, |multi| {
                        multi
                            .joins
                            .iter()
                            .filter(|join| join.is_temporal_join())
                            .count()
                    }),
                )?;

                if let Some(ref mut multi) = join_analysis {
                    apply_temporal_probe_analysis(multi, temporal_probe)?;
                    self.resolve_temporal_source_contracts(multi)?;
                    validate_streaming_joins(multi, &self.lookup_tables, None)?;
                }

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
                let having_config = agg_analysis.having_expr.map(HavingFilterConfig::new);

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

                    let join_config = join_analysis
                        .map(|m| JoinOperatorConfig::from_multi_analysis(&m))
                        .transpose()
                        .map_err(PlanningError::InvalidQuery)?;

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

        validate_temporal_version_shape(stmt, 0)?;

        // Pass through standard SQL
        Ok(StreamingPlan::Standard(Box::new(stmt.clone())))
    }

    /// Analyzes a query for streaming features.
    fn analyze_query(
        &self,
        stmt: &Statement,
        emit_clause: Option<&EmitClause>,
        changelog_enrich: Option<&ChangelogEnrichAdmission>,
        temporal_probe: Option<&JoinAnalysis>,
    ) -> Result<QueryAnalysis, PlanningError> {
        let mut analysis = QueryAnalysis::default();
        let mut recognized_temporal_versions = 0;

        if let Statement::Query(query) = stmt {
            if let SetExpr::Select(select) = query.body.as_ref() {
                if has_implicit_multi_source(select) {
                    return Err(PlanningError::InvalidQuery(
                        "implicit multi-source joins are unsupported; use one bounded INNER JOIN"
                            .to_string(),
                    ));
                }
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
                let join_analysis = analyze_joins(select).map_err(|e| {
                    PlanningError::InvalidQuery(format!("Join analysis failed: {e}"))
                })?;
                recognized_temporal_versions = join_analysis.as_ref().map_or(0, |multi| {
                    multi
                        .joins
                        .iter()
                        .filter(|join| join.is_temporal_join())
                        .count()
                });
                if let Some(mut multi) = join_analysis {
                    apply_temporal_probe_analysis(&mut multi, temporal_probe)?;
                    self.resolve_temporal_source_contracts(&mut multi)?;
                    validate_streaming_joins(&multi, &self.lookup_tables, changelog_enrich)?;
                    analysis.join_config = Some(
                        JoinOperatorConfig::from_multi_analysis(&multi)
                            .map_err(PlanningError::InvalidQuery)?,
                    );
                }
            }
        }

        validate_temporal_version_shape(stmt, recognized_temporal_versions)?;

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
        analysis.having_config = agg_analysis.having_expr.map(HavingFilterConfig::new);

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

    /// Plans a CREATE LOOKUP TABLE statement.
    fn plan_create_lookup_table(
        &mut self,
        lt: &CreateLookupTableStatement,
    ) -> Result<StreamingPlan, PlanningError> {
        let name = object_name_to_string(&lt.name);

        if !lt.or_replace && !lt.if_not_exists && self.lookup_tables.contains_key(&name) {
            return Err(PlanningError::InvalidQuery(format!(
                "Lookup table '{}' already exists",
                name
            )));
        }

        let columns: Vec<(String, String)> = lt
            .columns
            .iter()
            .map(|c| (c.name.value.clone(), c.data_type.to_string()))
            .collect();

        let properties = validate_properties(&lt.with_options).map_err(|e| {
            PlanningError::InvalidQuery(format!("Invalid lookup table properties: {e}"))
        })?;

        // Compute Arrow schema from column definitions
        let arrow_fields: Vec<Field> = lt
            .columns
            .iter()
            .map(|c| {
                let dt = crate::translator::streaming_ddl::sql_type_to_arrow(&c.data_type)
                    .map_err(|e| PlanningError::InvalidQuery(e.to_string()))?;
                let nullable = !c
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull));
                Ok(Field::new(&c.name.value, dt, nullable))
            })
            .collect::<Result<_, PlanningError>>()?;
        let arrow_schema = Arc::new(Schema::new(arrow_fields));

        let info = LookupTableInfo {
            name: name.clone(),
            columns,
            primary_key: lt.primary_key.clone(),
            properties,
            arrow_schema,
            raw_options: lt.with_options.clone(),
        };

        self.lookup_tables.insert(name, info.clone());

        Ok(StreamingPlan::RegisterLookupTable(info))
    }

    /// Plans a DROP LOOKUP TABLE statement.
    fn plan_drop_lookup_table(
        &mut self,
        name: &ObjectName,
        if_exists: bool,
    ) -> Result<StreamingPlan, PlanningError> {
        let name_str = object_name_to_string(name);

        if !if_exists && !self.lookup_tables.contains_key(&name_str) {
            return Err(PlanningError::InvalidQuery(format!(
                "Lookup table '{}' does not exist",
                name_str
            )));
        }

        self.lookup_tables.remove(&name_str);

        Ok(StreamingPlan::DropLookupTable { name: name_str })
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

    fn resolve_temporal_source_contracts(
        &self,
        multi: &mut MultiJoinAnalysis,
    ) -> Result<(), PlanningError> {
        for step in &mut multi.joins {
            if !step.is_temporal_join() {
                continue;
            }
            let (_, right_key_columns) = temporal_key_columns(step)?;
            let left = self.sources.get(&step.left_table).ok_or_else(|| {
                PlanningError::SourceNotFound(format!(
                    "{} (temporal left input must be a registered event-time source)",
                    step.left_table
                ))
            })?;
            let left_time = step.left_time_column.as_ref().ok_or_else(|| {
                PlanningError::InvalidQuery(
                    "temporal join is missing its explicit left event-time column".into(),
                )
            })?;
            let left_watermark = left.watermark_column.as_ref().ok_or_else(|| {
                PlanningError::InvalidQuery(format!(
                    "temporal left source '{}' must declare WATERMARK FOR {}",
                    step.left_table, left_time
                ))
            })?;
            if left_watermark != left_time {
                return Err(PlanningError::InvalidQuery(format!(
                    "temporal left timestamp '{}' does not match WATERMARK FOR {} on source '{}'",
                    left_time, left_watermark, step.left_table
                )));
            }
            let right = self.sources.get(&step.right_table).ok_or_else(|| {
                PlanningError::SourceNotFound(format!(
                    "{} (temporal right input must be a registered source)",
                    step.right_table
                ))
            })?;
            if !right
                .primary_key
                .iter()
                .map(String::as_str)
                .eq(right_key_columns.iter().copied())
            {
                return Err(PlanningError::InvalidQuery(format!(
                    "temporal right source '{}' must declare PRIMARY KEY ({}) matching the join key",
                    step.right_table,
                    right_key_columns.join(", ")
                )));
            }
            let right_time = right.watermark_column.as_ref().ok_or_else(|| {
                PlanningError::InvalidQuery(format!(
                    "temporal right source '{}' must declare WATERMARK FOR its version column",
                    step.right_table
                ))
            })?;
            if step
                .right_time_column
                .as_ref()
                .is_some_and(|column| column != right_time)
            {
                return Err(PlanningError::InvalidQuery(format!(
                    "temporal right timestamp '{}' does not match WATERMARK FOR {} on source '{}'",
                    step.right_time_column.as_deref().unwrap_or_default(),
                    right_time,
                    step.right_table
                )));
            }
            step.right_time_column = Some(right_time.clone());
        }
        Ok(())
    }

    /// Lists all registered sinks.
    #[must_use]
    pub fn list_sinks(&self) -> Vec<&SinkInfo> {
        self.sinks.values().collect()
    }

    /// Gets a registered lookup table by name.
    #[must_use]
    pub fn get_lookup_table(&self, name: &str) -> Option<&LookupTableInfo> {
        self.lookup_tables.get(name)
    }

    /// Lists all registered lookup tables.
    #[must_use]
    pub fn list_lookup_tables(&self) -> Vec<&LookupTableInfo> {
        self.lookup_tables.values().collect()
    }

    /// Returns a clone of the lookup tables map for optimizer rule construction.
    #[must_use]
    pub fn lookup_tables_cloned(&self) -> HashMap<String, LookupTableInfo> {
        self.lookup_tables.clone()
    }

    /// Converts a query plan's SQL statement into a `DataFusion`
    /// `LogicalPlan`. Window UDFs (TUMBLE, HOP, SESSION) must be registered
    /// on `ctx` via
    /// [`register_streaming_functions`](crate::datafusion::register_streaming_functions)
    /// for windowed queries to resolve correctly.
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
    match name.0.as_slice() {
        [sqlparser::ast::ObjectNamePart::Identifier(ident)] => ident.value.clone(),
        _ => name.to_string(),
    }
}

fn temporal_key_columns(step: &JoinAnalysis) -> Result<(Vec<&str>, Vec<&str>), PlanningError> {
    let mut left = Vec::with_capacity(1 + step.additional_key_columns.len());
    let mut right = Vec::with_capacity(1 + step.additional_key_columns.len());
    left.push(step.left_key_column.as_str());
    right.push(step.right_key_column.as_str());
    for (left_column, right_column) in &step.additional_key_columns {
        left.push(left_column);
        right.push(right_column);
    }
    if left.is_empty()
        || left.len() != right.len()
        || left.iter().chain(&right).any(|column| column.is_empty())
    {
        return Err(PlanningError::InvalidQuery(
            "temporal join equality keys must be non-empty and have matching cardinality".into(),
        ));
    }
    Ok((left, right))
}

fn apply_temporal_probe_analysis(
    multi: &mut MultiJoinAnalysis,
    temporal_probe: Option<&JoinAnalysis>,
) -> Result<(), PlanningError> {
    let Some(temporal_probe) = temporal_probe else {
        return Ok(());
    };
    let [normalized] = multi.joins.as_slice() else {
        return Err(PlanningError::InvalidQuery(
            "TEMPORAL PROBE JOIN requires one explicitly named two-way stage".into(),
        ));
    };
    let (normalized_left_keys, normalized_right_keys) = temporal_key_columns(normalized)?;
    let (probe_left_keys, probe_right_keys) = temporal_key_columns(temporal_probe)?;
    if !normalized.is_temporal_join()
        || normalized.left_table != temporal_probe.left_table
        || normalized.right_table != temporal_probe.right_table
        || normalized_left_keys != probe_left_keys
        || normalized_right_keys != probe_right_keys
        || normalized.left_time_column != temporal_probe.left_time_column
        || normalized.join_type != temporal_probe.join_type
    {
        return Err(PlanningError::InvalidQuery(
            "TEMPORAL PROBE JOIN metadata does not match its normalized AS-OF plan".into(),
        ));
    }
    multi.joins[0] = temporal_probe.clone();
    Ok(())
}

fn validate_temporal_version_shape(
    statement: &Statement,
    recognized_versions: usize,
) -> Result<(), PlanningError> {
    let ast_versions = temporal_table_version_count(statement);
    if ast_versions != recognized_versions {
        return Err(PlanningError::InvalidQuery(
            "FOR SYSTEM_TIME AS OF is supported only on the right input of one direct two-input temporal join; nested and set-operation temporal joins are unsupported"
                .into(),
        ));
    }
    Ok(())
}

/// Fail closed on stream-join shapes whose state/output semantics are not implemented.
fn validate_streaming_joins(
    multi: &MultiJoinAnalysis,
    lookup_tables: &HashMap<String, LookupTableInfo>,
    changelog_enrich: Option<&ChangelogEnrichAdmission>,
) -> Result<(), PlanningError> {
    if multi.joins.len() != 1 {
        return Err(PlanningError::InvalidQuery(
            "multi-way streaming joins require explicitly named two-way stages".to_string(),
        ));
    }
    for step in &multi.joins {
        if step.time_bound.is_some_and(|bound| bound.is_zero()) {
            return Err(PlanningError::InvalidQuery(
                "streaming interval joins require a positive finite time bound".to_string(),
            ));
        }
        if step.is_bounded() {
            continue;
        }
        let left_lookup = lookup_tables.contains_key(&step.left_table);
        let right_lookup = lookup_tables.contains_key(&step.right_table);
        if !left_lookup && !right_lookup {
            if changelog_enrich.is_some_and(|admission| admission.matches(step)) {
                continue;
            }
            return Err(PlanningError::InvalidQuery(format!(
                "unbounded join between streaming sources '{}' and '{}'; \
                 add a temporal predicate or use a lookup table",
                step.left_table, step.right_table,
            )));
        }
    }
    Ok(())
}

/// Planning errors
#[derive(Debug, thiserror::Error)]
pub enum PlanningError {
    /// Unsupported SQL feature
    UnsupportedSql(String),

    /// Invalid query
    InvalidQuery(String),

    /// Source not found
    SourceNotFound(String),

    /// Sink not found
    SinkNotFound(String),

    /// `DataFusion` error during logical plan creation (translated on display)
    DataFusion(#[from] datafusion_common::DataFusionError),
}

impl std::fmt::Display for PlanningError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedSql(msg) => write!(f, "Unsupported SQL: {msg}"),
            Self::InvalidQuery(msg) => write!(f, "Invalid query: {msg}"),
            Self::SourceNotFound(name) => write!(f, "Source not found: {name}"),
            Self::SinkNotFound(name) => write!(f, "Sink not found: {name}"),
            Self::DataFusion(e) => {
                let translated = crate::error::translate_datafusion_error(&e.to_string());
                write!(f, "{translated}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::StreamingParser;

    fn register_temporal_sources(planner: &mut StreamingPlanner) {
        for sql in [
            "CREATE SOURCE trades (symbol VARCHAR, trade_time TIMESTAMP, WATERMARK FOR trade_time)",
            "CREATE SOURCE quotes (symbol VARCHAR PRIMARY KEY, quote_time TIMESTAMP, WATERMARK FOR quote_time)",
        ] {
            let statement = StreamingParser::parse_sql(sql).unwrap();
            planner.plan(&statement[0]).unwrap();
        }
    }

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
            "SELECT * FROM orders o JOIN payments p \
             ON o.tenant_id = p.account_id AND o.order_id = p.payment_order_id \
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR",
        )
        .unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Query(query_plan) => {
                assert!(query_plan.join_config.is_some());
                let configs = query_plan.join_config.unwrap();
                assert_eq!(configs.len(), 1);
                assert_eq!(configs[0].left_keys(), ["tenant_id", "order_id"]);
                assert_eq!(configs[0].right_keys(), ["account_id", "payment_order_id"]);
            }
            _ => panic!("Expected Query plan"),
        }
    }

    #[test]
    fn test_plan_rejects_unbounded_streaming_join() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "CREATE STREAM joined AS SELECT * FROM orders o JOIN payments p \
             ON o.order_id = p.order_id",
        )
        .unwrap();
        let err = planner.plan(&statements[0]).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("unbounded join"), "got: {msg}");
        assert!(msg.contains("lookup table"), "got: {msg}");
    }

    #[test]
    fn test_plan_rejects_unbounded_standard_select_join() {
        let statements = StreamingParser::parse_sql(
            "SELECT * FROM orders o JOIN payments p ON o.order_id = p.order_id",
        )
        .unwrap();
        assert!(matches!(
            statements.first(),
            Some(StreamingStatement::Standard(_))
        ));

        let error = StreamingPlanner::new().plan(&statements[0]).unwrap_err();
        assert!(error.to_string().contains("unbounded join"));
    }

    #[test]
    fn changelog_enrich_certificate_allows_only_its_join() {
        let statements = StreamingParser::parse_sql(
            "CREATE STREAM joined AS SELECT * FROM orders o JOIN payments p \
             ON o.order_id = p.order_id",
        )
        .unwrap();
        let admission = ChangelogEnrichAdmission::try_new(
            "orders",
            "payments",
            vec!["order_id".into()],
            vec!["order_id".into()],
            false,
        )
        .unwrap();
        StreamingPlanner::new()
            .plan_changelog_enrich(&statements[0], &admission)
            .unwrap();

        let wrong_key = ChangelogEnrichAdmission::try_new(
            "orders",
            "payments",
            vec!["tenant_id".into()],
            vec!["tenant_id".into()],
            false,
        )
        .unwrap();
        let error = StreamingPlanner::new()
            .plan_changelog_enrich(&statements[0], &wrong_key)
            .unwrap_err();
        assert!(error.to_string().contains("unbounded join"));
    }

    #[test]
    fn composite_changelog_enrichment_is_rejected_until_operator_supports_it() {
        let statements = StreamingParser::parse_sql(
            "CREATE STREAM joined AS SELECT * FROM orders o JOIN payments p \
             ON o.tenant_id = p.account_id AND o.order_id = p.payment_order_id",
        )
        .unwrap();
        let exact = ChangelogEnrichAdmission::try_new(
            "orders",
            "payments",
            vec!["tenant_id".into(), "order_id".into()],
            vec!["account_id".into(), "payment_order_id".into()],
            false,
        )
        .unwrap();
        let error = StreamingPlanner::new()
            .plan_changelog_enrich(&statements[0], &exact)
            .unwrap_err();
        assert!(error.to_string().contains("exactly one equality key"));
    }

    #[test]
    fn state_backed_certificate_binds_inner_vs_left() {
        let statements = StreamingParser::parse_sql(
            "CREATE STREAM joined AS SELECT * FROM orders o LEFT JOIN payments p \
             ON o.order_id = p.order_id",
        )
        .unwrap();
        let left = ChangelogEnrichAdmission::try_new(
            "orders",
            "payments",
            vec!["order_id".into()],
            vec!["order_id".into()],
            true,
        )
        .unwrap();
        StreamingPlanner::new()
            .plan_changelog_enrich(&statements[0], &left)
            .unwrap();

        let inner = ChangelogEnrichAdmission::try_new(
            "orders",
            "payments",
            vec!["order_id".into()],
            vec!["order_id".into()],
            false,
        )
        .unwrap();
        assert!(StreamingPlanner::new()
            .plan_changelog_enrich(&statements[0], &inner)
            .is_err());
    }

    #[test]
    fn changelog_enrich_certificate_does_not_relax_right_or_multiway_joins() {
        let admission = ChangelogEnrichAdmission::try_new(
            "orders",
            "payments",
            vec!["order_id".into()],
            vec!["order_id".into()],
            false,
        )
        .unwrap();
        for sql in [
            "CREATE STREAM joined AS SELECT * FROM orders o RIGHT JOIN payments p \
             ON o.order_id = p.order_id",
            "CREATE STREAM joined AS SELECT * FROM orders o JOIN payments p \
             ON o.order_id = p.order_id JOIN refunds r ON p.order_id = r.order_id",
        ] {
            let statements = StreamingParser::parse_sql(sql).unwrap();
            assert!(StreamingPlanner::new()
                .plan_changelog_enrich(&statements[0], &admission)
                .is_err());
        }
    }

    #[test]
    fn test_plan_rejects_implicit_cross_join() {
        let statements = StreamingParser::parse_sql("SELECT * FROM orders, payments").unwrap();
        let error = StreamingPlanner::new().plan(&statements[0]).unwrap_err();
        assert!(error.to_string().contains("implicit multi-source"));
    }

    #[test]
    fn test_plan_allows_unnest_after_one_stream_source() {
        let statements = StreamingParser::parse_sql(
            "SELECT event_id, tag FROM events, UNNEST(make_array('a', 'b')) AS tags(tag)",
        )
        .unwrap();
        StreamingPlanner::new().plan(&statements[0]).unwrap();
    }

    #[test]
    fn test_plan_rejects_unbounded_join_between_windowed_views() {
        let mut planner = StreamingPlanner::new();
        // Two windowed views register as bounded inputs.
        for sql in [
            "CREATE STREAM price_1m AS SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS bucket, \
             AVG(p) AS price FROM trades GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)",
            "CREATE STREAM sent_1m AS SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS bucket, \
             AVG(s) AS ms FROM posts GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)",
        ] {
            let st = StreamingParser::parse_sql(sql).unwrap();
            planner.plan(&st[0]).unwrap();
        }
        // Closed-window batches are not an ordering/alignment contract between two independent
        // streams. A per-cycle join can silently miss rows when the batches arrive in different
        // cycles, so this shape remains unbounded and must fail closed.
        let st = StreamingParser::parse_sql(
            "CREATE STREAM joined AS SELECT a.bucket, a.price, b.ms \
             FROM price_1m a JOIN sent_1m b ON a.bucket = b.bucket",
        )
        .unwrap();
        let error = planner.plan(&st[0]).unwrap_err();
        assert!(error.to_string().contains("unbounded join"));
    }

    #[test]
    fn test_plan_allows_all_bounded_equi_join_kinds() {
        for (sql_join_type, expected) in [
            ("INNER", JoinType::Inner),
            ("LEFT", JoinType::Left),
            ("RIGHT", JoinType::Right),
            ("FULL", JoinType::Full),
            ("LEFT SEMI", JoinType::LeftSemi),
            ("LEFT ANTI", JoinType::LeftAnti),
            ("RIGHT SEMI", JoinType::RightSemi),
            ("RIGHT ANTI", JoinType::RightAnti),
        ] {
            let sql = format!(
                "SELECT * FROM orders o {sql_join_type} JOIN payments p \
                 ON o.order_id = p.order_id \
                 AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR"
            );
            let statements = StreamingParser::parse_sql(&sql).unwrap();
            let plan = StreamingPlanner::new().plan(&statements[0]).unwrap();
            let StreamingPlan::Query(query) = plan else {
                panic!("{sql_join_type} interval join did not produce a query plan");
            };
            let Some(JoinOperatorConfig::StreamStream(config)) =
                query.join_config.and_then(|mut configs| configs.pop())
            else {
                panic!("{sql_join_type} interval join did not produce a stream-join config");
            };
            assert_eq!(config.join_type, expected);
        }
    }

    #[test]
    fn test_plan_rejects_zero_interval_bound() {
        let statements = StreamingParser::parse_sql(
            "SELECT * FROM orders o JOIN payments p ON o.order_id = p.order_id \
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '0' SECOND",
        )
        .unwrap();
        let error = StreamingPlanner::new().plan(&statements[0]).unwrap_err();
        assert!(error.to_string().contains("positive finite time bound"));
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

    // -- Multi-way join planner tests --

    #[test]
    fn test_plan_single_join_produces_vec_of_one() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT * FROM a JOIN b ON a.id = b.a_id \
             AND b.ts BETWEEN a.ts AND a.ts + INTERVAL '1' HOUR",
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
    fn test_plan_rejects_multi_way_interval_join() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql(
            "SELECT * FROM a JOIN b ON a.id = b.a_id \
                 AND b.ts BETWEEN a.ts AND a.ts + INTERVAL '1' HOUR \
             JOIN c ON b.id = c.b_id \
                 AND c.ts BETWEEN b.ts AND b.ts + INTERVAL '1' HOUR",
        )
        .unwrap();

        let error = planner.plan(&statements[0]).unwrap_err();
        assert!(error
            .to_string()
            .contains("explicitly named two-way stages"));
    }

    #[test]
    fn test_plan_rejects_mixed_multi_way_join() {
        let mut planner = StreamingPlanner::new();
        // A lookup-table final step does not make a multi-way operator atomic; users must expose
        // the interval result as a named stage before enriching it.
        let _ = planner.plan(
            &StreamingParser::parse_sql(
                "CREATE LOOKUP TABLE customers (id BIGINT NOT NULL, name VARCHAR, \
                 PRIMARY KEY (id)) WITH (connector = 'parquet', path = '/tmp/x.parquet')",
            )
            .unwrap()[0],
        );
        let statements = StreamingParser::parse_sql(
            "SELECT * FROM orders o \
             JOIN payments p ON o.id = p.order_id \
                 AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR \
             JOIN customers c ON p.cust_id = c.id",
        )
        .unwrap();

        let error = planner.plan(&statements[0]).unwrap_err();
        assert!(error
            .to_string()
            .contains("explicitly named two-way stages"));
    }

    #[test]
    fn test_plan_non_join_query() {
        let mut planner = StreamingPlanner::new();
        let statements = StreamingParser::parse_sql("SELECT * FROM orders").unwrap();

        let plan = planner.plan(&statements[0]).unwrap();
        match plan {
            StreamingPlan::Standard(_) => {} // No join → pass-through
            _ => panic!("Expected Standard plan for simple SELECT"),
        }
    }

    #[test]
    fn temporal_as_of_resolves_both_event_time_contracts() {
        let mut planner = StreamingPlanner::new();
        register_temporal_sources(&mut planner);
        let statement = StreamingParser::parse_sql(
            "SELECT t.symbol, q.price FROM trades t \
             JOIN quotes FOR SYSTEM_TIME AS OF t.trade_time AS q \
             ON t.symbol = q.symbol",
        )
        .unwrap();
        let StreamingPlan::Query(plan) = planner.plan(&statement[0]).unwrap() else {
            panic!("expected temporal query plan");
        };
        let Some(JoinOperatorConfig::Temporal(config)) =
            plan.join_config.and_then(|mut configs| configs.pop())
        else {
            panic!("expected canonical temporal config");
        };
        assert_eq!(config.left_time_column, "trade_time");
        assert_eq!(config.right_time_column, "quote_time");
        assert_eq!(config.join_kind, crate::temporal::TemporalJoinKind::Inner);
        assert_eq!(config.probe_schedule.offsets_ms(), [0]);
    }

    #[test]
    fn temporal_as_of_preserves_composite_key_order_and_requires_exact_primary_key() {
        for (primary_key, accepted) in [
            ("tenant, symbol", true),
            ("symbol, tenant", false),
            ("tenant", false),
        ] {
            let mut planner = StreamingPlanner::new();
            for sql in [
                "CREATE SOURCE trades (tenant VARCHAR, symbol VARCHAR, trade_time TIMESTAMP, WATERMARK FOR trade_time)".to_string(),
                format!(
                    "CREATE SOURCE quotes (tenant VARCHAR NOT NULL, symbol VARCHAR NOT NULL, quote_time TIMESTAMP, PRIMARY KEY ({primary_key}), WATERMARK FOR quote_time)"
                ),
            ] {
                let statement = StreamingParser::parse_sql(&sql).unwrap();
                planner.plan(&statement[0]).unwrap();
            }

            let statement = StreamingParser::parse_sql(
                "SELECT * FROM trades t \
                 JOIN quotes FOR SYSTEM_TIME AS OF t.trade_time AS q \
                 ON t.tenant = q.tenant AND t.symbol = q.symbol",
            )
            .unwrap();
            let planned = planner.plan(&statement[0]);
            if accepted {
                let StreamingPlan::Query(plan) = planned.unwrap() else {
                    panic!("expected temporal query plan");
                };
                let Some(JoinOperatorConfig::Temporal(config)) =
                    plan.join_config.and_then(|mut configs| configs.pop())
                else {
                    panic!("expected canonical temporal config");
                };
                assert_eq!(config.left_key_columns, ["tenant", "symbol"]);
                assert_eq!(config.right_key_columns, ["tenant", "symbol"]);
            } else {
                let error = planned.unwrap_err().to_string();
                assert!(error.contains("PRIMARY KEY (tenant, symbol)"), "{error}");
            }
        }
    }

    #[test]
    fn temporal_as_of_rejects_invalid_equality_expressions() {
        for (query, expected) in [
            (
                "SELECT * FROM trades t \
                 JOIN quotes FOR SYSTEM_TIME AS OF t.trade_time AS q \
                 ON LOWER(t.symbol) = q.symbol",
                "qualified column references",
            ),
            (
                "SELECT * FROM trades AS quotes \
                 JOIN quotes FOR SYSTEM_TIME AS OF trades.trade_time AS q \
                 ON quotes.symbol = q.symbol",
                "names both inputs",
            ),
            (
                "SELECT * FROM trades t \
                 JOIN quotes FOR SYSTEM_TIME AS OF t.trade_time AS q \
                 ON t.symbol = q.symbol AND q.quote_time BETWEEN t.trade_time AND t.trade_time + INTERVAL '1' SECOND",
                "additional time-bound",
            ),
        ] {
            let mut planner = StreamingPlanner::new();
            register_temporal_sources(&mut planner);
            let statement = StreamingParser::parse_sql(query).unwrap();
            let error = planner.plan(&statement[0]).unwrap_err().to_string();
            assert!(error.contains(expected), "{error}");
        }
    }

    #[test]
    fn temporal_probe_list_uses_the_as_of_config() {
        let mut planner = StreamingPlanner::new();
        for sql in [
            "CREATE SOURCE trades (symbol VARCHAR, venue VARCHAR, trade_time TIMESTAMP, WATERMARK FOR trade_time)",
            "CREATE SOURCE quotes (symbol VARCHAR NOT NULL, venue VARCHAR NOT NULL, quote_time TIMESTAMP, PRIMARY KEY (symbol, venue), WATERMARK FOR quote_time)",
        ] {
            let statement = StreamingParser::parse_sql(sql).unwrap();
            planner.plan(&statement[0]).unwrap();
        }
        let statement = StreamingParser::parse_sql(
            "CREATE STREAM markouts AS SELECT probe.offset_ms, probe.probe_time, q.price \
             FROM trades t TEMPORAL PROBE JOIN quotes q ON (symbol, venue) \
             TIMESTAMPS (trade_time, quote_time) LIST (-1s, 0s, 5s) AS probe",
        )
        .unwrap();
        let StreamingPlan::Query(plan) = planner.plan(&statement[0]).unwrap() else {
            panic!("expected temporal probe query plan");
        };
        let Some(JoinOperatorConfig::Temporal(config)) =
            plan.join_config.and_then(|mut configs| configs.pop())
        else {
            panic!("expected canonical temporal config");
        };
        assert_eq!(config.left_key_columns, ["symbol", "venue"]);
        assert_eq!(config.right_key_columns, ["symbol", "venue"]);
        assert_eq!(config.probe_schedule.offsets_ms(), [-1_000, 0, 5_000]);
        assert_eq!(config.probe_alias.as_deref(), Some("probe"));
    }

    #[test]
    fn temporal_join_requires_source_watermarks_and_matching_right_key() {
        for (left, right, expected) in [
            (
                "CREATE SOURCE trades (symbol VARCHAR, trade_time TIMESTAMP)",
                "CREATE SOURCE quotes (symbol VARCHAR PRIMARY KEY, quote_time TIMESTAMP, WATERMARK FOR quote_time)",
                "temporal left source 'trades' must declare WATERMARK",
            ),
            (
                "CREATE SOURCE trades (symbol VARCHAR, trade_time TIMESTAMP, WATERMARK FOR trade_time)",
                "CREATE SOURCE quotes (symbol VARCHAR, quote_time TIMESTAMP, WATERMARK FOR quote_time)",
                "must declare PRIMARY KEY (symbol)",
            ),
            (
                "CREATE SOURCE trades (symbol VARCHAR, trade_time TIMESTAMP, WATERMARK FOR trade_time)",
                "CREATE SOURCE quotes (symbol VARCHAR PRIMARY KEY, quote_time TIMESTAMP)",
                "must declare WATERMARK FOR its version column",
            ),
        ] {
            let mut planner = StreamingPlanner::new();
            for sql in [left, right] {
                let statement = StreamingParser::parse_sql(sql).unwrap();
                planner.plan(&statement[0]).unwrap();
            }
            let query = StreamingParser::parse_sql(
                "SELECT * FROM trades t JOIN quotes FOR SYSTEM_TIME AS OF t.trade_time AS q \
                 ON t.symbol = q.symbol",
            )
            .unwrap();
            let error = planner.plan(&query[0]).unwrap_err().to_string();
            assert!(error.contains(expected), "{error}");
        }
    }

    #[test]
    fn nested_temporal_versions_fail_closed() {
        let mut planner = StreamingPlanner::new();
        register_temporal_sources(&mut planner);
        let statement = StreamingParser::parse_sql(
            "WITH matched AS (\
                 SELECT t.symbol FROM trades t \
                 JOIN quotes FOR SYSTEM_TIME AS OF t.trade_time AS q \
                 ON t.symbol = q.symbol\
             ) SELECT * FROM matched",
        )
        .unwrap();
        let error = planner.plan(&statement[0]).unwrap_err().to_string();
        assert!(error.contains("nested and set-operation"), "{error}");
    }

    // -- Window Frame planner tests --

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
        let statements = StreamingParser::parse_sql("SELECT * FROM trades").unwrap();

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
        assert!(err.contains("UNBOUNDED FOLLOWING"), "error was: {err}");
    }
}
