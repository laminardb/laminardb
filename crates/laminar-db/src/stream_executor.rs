//! `DataFusion` micro-batch stream executor.
//!
//! Executes registered streaming queries against source data using `DataFusion`'s
//! SQL engine. Each processing cycle:
//!
//! 1. Source batches are registered as temporary `MemTable` tables
//! 2. Each stream query is executed via `ctx.sql()`
//! 3. Results are collected as `RecordBatch` vectors
//! 4. Temporary tables are cleared for the next cycle

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::prelude::SessionContext;
use sqlparser::ast::{
    Expr, SelectItem, SetExpr, Statement, TableFactor, WildcardAdditionalOptions,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use laminar_sql::parser::join_parser::analyze_joins;
use laminar_sql::parser::{EmitClause, EmitStrategy as SqlEmitStrategy};
use laminar_sql::translator::{
    AsofJoinTranslatorConfig, JoinOperatorConfig, OrderOperatorConfig,
    TemporalJoinTranslatorConfig, WindowOperatorConfig, WindowType,
};

use crate::aggregate_state::IncrementalAggState;
use crate::core_window_state::CoreWindowState;
use crate::eowc_state::IncrementalEowcState;
use crate::error::DbError;

/// Convert a SQL-layer `EmitStrategy` to a core-layer `EmitStrategy`.
///
/// The SQL parser produces `EmitStrategy::FinalOnly`, while core operators
/// expect `EmitStrategy::Final`. This function maps all variants correctly.
///
/// Cannot use a `From` impl due to the orphan rule (neither type is local).
pub(crate) fn sql_emit_to_core(
    s: &SqlEmitStrategy,
) -> laminar_core::operator::window::EmitStrategy {
    use laminar_core::operator::window::EmitStrategy as CoreEmit;
    match s {
        SqlEmitStrategy::OnWatermark => CoreEmit::OnWatermark,
        SqlEmitStrategy::OnWindowClose => CoreEmit::OnWindowClose,
        SqlEmitStrategy::Periodic(d) => CoreEmit::Periodic(*d),
        SqlEmitStrategy::OnUpdate => CoreEmit::OnUpdate,
        SqlEmitStrategy::Changelog => CoreEmit::Changelog,
        SqlEmitStrategy::FinalOnly => CoreEmit::Final,
    }
}

/// Convert an `EmitClause` (SQL AST) to a core `EmitStrategy`.
///
/// Calls `EmitClause::to_emit_strategy()` to resolve the clause to a
/// runtime strategy, then converts via [`sql_emit_to_core`].
pub(crate) fn emit_clause_to_core(
    clause: &EmitClause,
) -> Result<laminar_core::operator::window::EmitStrategy, laminar_sql::parser::ParseError> {
    let sql_strategy = clause.to_emit_strategy()?;
    Ok(sql_emit_to_core(&sql_strategy))
}

/// Extract all table names referenced in FROM/JOIN clauses of a SQL query.
///
/// Parses the SQL and walks the AST to find `TableFactor::Table` references,
/// recursing into subqueries, nested joins, and set operations (UNION, etc.).
pub(crate) fn extract_table_references(sql: &str) -> HashSet<String> {
    let mut tables = HashSet::new();
    let dialect = GenericDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
        return tables;
    };
    for stmt in &statements {
        if let Statement::Query(query) = stmt {
            collect_tables_from_set_expr(query.body.as_ref(), &mut tables);
        }
    }
    tables
}

/// Recursively collect table names from a `SetExpr`.
fn collect_tables_from_set_expr(set_expr: &SetExpr, tables: &mut HashSet<String>) {
    match set_expr {
        SetExpr::Select(select) => {
            for table_with_joins in &select.from {
                collect_tables_from_factor(&table_with_joins.relation, tables);
                for join in &table_with_joins.joins {
                    collect_tables_from_factor(&join.relation, tables);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_tables_from_set_expr(left.as_ref(), tables);
            collect_tables_from_set_expr(right.as_ref(), tables);
        }
        SetExpr::Query(query) => {
            collect_tables_from_set_expr(query.body.as_ref(), tables);
        }
        _ => {}
    }
}

/// Collect table names from a single `TableFactor`.
fn collect_tables_from_factor(factor: &TableFactor, tables: &mut HashSet<String>) {
    match factor {
        TableFactor::Table { name, .. } => {
            // Use last component of potentially qualified name (e.g., "schema.table")
            tables.insert(name.to_string());
        }
        TableFactor::Derived { subquery, .. } => {
            collect_tables_from_set_expr(subquery.body.as_ref(), tables);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_tables_from_factor(&table_with_joins.relation, tables);
            for join in &table_with_joins.joins {
                collect_tables_from_factor(&join.relation, tables);
            }
        }
        _ => {}
    }
}

/// A registered stream query for execution.
#[derive(Debug, Clone)]
pub(crate) struct StreamQuery {
    /// Stream name.
    pub name: String,
    /// SQL query text.
    pub sql: String,
    /// ASOF join config (set when the query contains an ASOF JOIN).
    pub asof_config: Option<AsofJoinTranslatorConfig>,
    /// Rewritten projection SQL to apply aliases/expressions after the ASOF
    /// join result is registered as `__asof_tmp`.
    pub projection_sql: Option<String>,
    /// Temporal join config (set when the query contains FOR `SYSTEM_TIME` AS OF).
    pub temporal_config: Option<TemporalJoinTranslatorConfig>,
    /// EMIT clause from the planner (e.g., `OnWindowClose`, `Final`).
    pub emit_clause: Option<EmitClause>,
    /// Window configuration from the planner (window type, size, gap, etc.).
    pub window_config: Option<WindowOperatorConfig>,
    /// ORDER BY configuration (Top-K, `PerGroupTopK`, etc.).
    pub order_config: Option<OrderOperatorConfig>,
    /// Pre-computed table references (extracted once at registration).
    /// Avoids re-parsing SQL for dependency analysis every cycle.
    table_refs: HashSet<String>,
}

impl StreamQuery {
    /// Returns `true` if this query suppresses intermediate results
    /// (i.e., uses `EMIT ON WINDOW CLOSE` or `EMIT FINAL`).
    fn suppresses_intermediate(&self) -> bool {
        self.emit_clause
            .as_ref()
            .is_some_and(|ec| matches!(ec, EmitClause::OnWindowClose | EmitClause::Final))
    }
}

/// Maximum rows an EOWC accumulator may hold before forcing emission.
/// Prevents unbounded memory growth when windows fail to close or late
/// data keeps arriving.
const MAX_EOWC_ACCUMULATED_ROWS: usize = 1_000_000;

/// When an EOWC source has more than this many separate batches, coalesce
/// them into a single batch to reduce per-batch overhead and memory
/// fragmentation.
const EOWC_COALESCE_BATCH_THRESHOLD: usize = 32;

/// Per-query EOWC accumulation state (raw-batch path).
///
/// For non-aggregate EOWC queries (`EMIT ON WINDOW CLOSE` / `EMIT FINAL`),
/// source batches are accumulated across cycles and replayed when the
/// watermark indicates new windows have closed. Aggregate EOWC queries
/// bypass this struct entirely — they are routed through
/// `CoreWindowState` (tumbling windows) or `IncrementalEowcState`
/// (other window types) for `O(groups)` memory via incremental
/// accumulators. Batches are coalesced at
/// `EOWC_COALESCE_BATCH_THRESHOLD` to mitigate fragmentation.
struct EowcState {
    /// Accumulated source batches keyed by source table name.
    accumulated_sources: HashMap<String, Vec<RecordBatch>>,
    /// The last closed-window boundary that was emitted.
    last_closed_boundary: i64,
    /// Total rows currently accumulated (for diagnostics).
    accumulated_rows: usize,
}

/// DataFusion-based micro-batch stream executor.
///
/// Holds a `SessionContext` and registered stream queries. Each execution
/// cycle registers source data as `MemTable`, runs queries, and returns
/// named results.
pub(crate) struct StreamExecutor {
    ctx: SessionContext,
    queries: Vec<StreamQuery>,
    /// Tracks which temporary source tables are registered (for cleanup).
    registered_sources: Vec<String>,
    /// Indices into `queries` in topological (dependency) order.
    topo_order: Vec<usize>,
    /// When true, `topo_order` must be recomputed before next cycle.
    topo_dirty: bool,
    /// Known source schemas — used to register empty tables when a source
    /// has no data in a given cycle, preventing `DataFusion` planning errors.
    source_schemas: HashMap<String, SchemaRef>,
    /// Per-query EOWC accumulation state, keyed by query index.
    eowc_states: HashMap<usize, EowcState>,
    /// Per-query incremental aggregation state, keyed by query index.
    /// Initialized lazily on first cycle when the logical plan reveals
    /// the query contains a GROUP BY / aggregate.
    agg_states: HashMap<usize, IncrementalAggState>,
    /// Set of query indices that have been checked for aggregation but
    /// found not to be aggregate queries (avoids re-checking).
    non_agg_queries: HashSet<usize>,
    /// Per-query incremental EOWC aggregation state, keyed by query index.
    /// Initialized lazily on first EOWC cycle when the logical plan reveals
    /// the query contains a GROUP BY / aggregate.
    eowc_agg_states: HashMap<usize, IncrementalEowcState>,
    /// Set of EOWC query indices that have been checked for aggregation but
    /// found not to be aggregate queries (fall back to raw-batch path).
    non_eowc_agg_queries: HashSet<usize>,
    /// Per-query core window pipeline state for tumbling-window aggregates.
    /// Initialized lazily on first EOWC cycle when the query qualifies.
    core_window_states: HashMap<usize, CoreWindowState>,
    /// Set of EOWC query indices that were checked for core window routing but
    /// found not to qualify (fall through to `IncrementalEowcState`).
    non_core_window_queries: HashSet<usize>,
    /// Pending checkpoint data for deferred restore. Held until agg states
    /// are lazily initialized, then applied and cleared.
    pending_restore: Option<crate::aggregate_state::StreamExecutorCheckpoint>,
    /// Reusable per-cycle results map (cleared each cycle to avoid reallocation).
    cycle_results: HashMap<String, Vec<RecordBatch>>,
    /// Reusable per-cycle intermediate table name list.
    cycle_intermediates: Vec<String>,
}

impl StreamExecutor {
    /// Create a new executor with the given `SessionContext`.
    pub fn new(ctx: SessionContext) -> Self {
        Self {
            ctx,
            queries: Vec::new(),
            registered_sources: Vec::new(),
            topo_order: Vec::new(),
            topo_dirty: true,
            source_schemas: HashMap::new(),
            eowc_states: HashMap::new(),
            agg_states: HashMap::new(),
            non_agg_queries: HashSet::new(),
            eowc_agg_states: HashMap::new(),
            non_eowc_agg_queries: HashSet::new(),
            core_window_states: HashMap::new(),
            non_core_window_queries: HashSet::new(),
            pending_restore: None,
            cycle_results: HashMap::new(),
            cycle_intermediates: Vec::new(),
        }
    }

    /// Register a source schema so that empty placeholder tables can be
    /// created for sources that have no data in a given cycle.
    pub fn register_source_schema(&mut self, name: String, schema: SchemaRef) {
        self.source_schemas.insert(name, schema);
    }

    /// Register a stream query for execution.
    ///
    /// If the query contains an ASOF JOIN, it is detected at registration time
    /// and routed to a custom execution path in `execute_cycle()` (since
    /// `DataFusion` cannot parse ASOF syntax).
    pub fn add_query(
        &mut self,
        name: String,
        sql: String,
        emit_clause: Option<EmitClause>,
        window_config: Option<WindowOperatorConfig>,
        order_config: Option<OrderOperatorConfig>,
    ) {
        let (asof_config, projection_sql) = detect_asof_query(&sql);
        let temporal_config = detect_temporal_query(&sql);
        let table_refs = extract_table_references(&sql);
        let idx = self.queries.len();
        let query = StreamQuery {
            name,
            sql,
            asof_config,
            projection_sql,
            temporal_config,
            emit_clause,
            window_config,
            order_config,
            table_refs,
        };
        // Initialize EOWC state for queries that suppress intermediate results
        if query.suppresses_intermediate() {
            self.eowc_states.insert(
                idx,
                EowcState {
                    accumulated_sources: HashMap::new(),
                    last_closed_boundary: i64::MIN,
                    accumulated_rows: 0,
                },
            );
        }
        self.queries.push(query);
        self.topo_dirty = true;
    }

    /// Register a static reference table (e.g., from `CREATE TABLE`).
    ///
    /// Unlike source tables, these persist across cycles.
    #[allow(dead_code)]
    pub fn register_table(&self, name: &str, batch: RecordBatch) -> Result<(), DbError> {
        let schema = batch.schema();
        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| DbError::query_pipeline(name, &e))?;

        self.ctx
            .register_table(name, Arc::new(mem_table))
            .map_err(|e| DbError::query_pipeline(name, &e))?;
        Ok(())
    }

    /// Recompute topological order of queries using Kahn's algorithm.
    ///
    /// Queries that reference other query names in their FROM/JOIN clauses are
    /// ordered after their dependencies so intermediate results can be registered
    /// as temp tables before downstream queries execute.
    fn compute_topo_order(&mut self) {
        // Map query names to indices
        let name_to_idx: HashMap<&str, usize> = self
            .queries
            .iter()
            .enumerate()
            .map(|(i, q)| (q.name.as_str(), i))
            .collect();

        // Build in-degree counts (only count dependencies on other queries)
        let mut in_degree = vec![0usize; self.queries.len()];
        // dependents[i] = list of query indices that depend on query i
        let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); self.queries.len()];

        for (i, query) in self.queries.iter().enumerate() {
            for table_ref in &query.table_refs {
                if let Some(&dep_idx) = name_to_idx.get(table_ref.as_str()) {
                    if dep_idx != i {
                        in_degree[i] += 1;
                        dependents[dep_idx].push(i);
                    }
                }
            }
        }

        // Kahn's BFS
        let mut queue = VecDeque::new();
        for (i, &deg) in in_degree.iter().enumerate() {
            if deg == 0 {
                queue.push_back(i);
            }
        }

        self.topo_order.clear();
        while let Some(idx) = queue.pop_front() {
            self.topo_order.push(idx);
            for &dep in &dependents[idx] {
                in_degree[dep] = in_degree[dep].saturating_sub(1);
                if in_degree[dep] == 0 {
                    queue.push_back(dep);
                }
            }
        }

        // Fallback: if cycle detected, append any missing indices in insertion order
        if self.topo_order.len() < self.queries.len() {
            tracing::warn!(
                ordered = self.topo_order.len(),
                total = self.queries.len(),
                "circular dependency detected in query DAG, \
                 falling back to insertion order for remaining queries"
            );
            let in_order: HashSet<usize> = self.topo_order.iter().copied().collect();
            for i in 0..self.queries.len() {
                if !in_order.contains(&i) {
                    self.topo_order.push(i);
                }
            }
        }

        self.topo_dirty = false;
    }

    /// Execute one processing cycle.
    ///
    /// Registers `source_batches` as temporary tables, runs all stream queries,
    /// and returns a map from stream name to result batches.
    ///
    /// `current_watermark` is the current pipeline watermark in milliseconds.
    /// For EOWC queries, this drives the closed-window boundary computation.
    /// Pass `i64::MAX` to disable EOWC gating (all data passes through).
    #[allow(clippy::too_many_lines)]
    pub async fn execute_cycle(
        &mut self,
        source_batches: &HashMap<String, Vec<RecordBatch>>,
        current_watermark: i64,
    ) -> Result<HashMap<String, Vec<RecordBatch>>, DbError> {
        if self.topo_dirty {
            self.compute_topo_order();
        }

        self.register_source_tables(source_batches)?;

        // Reuse per-cycle allocations: take the pre-allocated maps out of
        // self so the borrow checker allows &mut self calls while results
        // is borrowed immutably within the loop.
        let mut results = std::mem::take(&mut self.cycle_results);
        results.clear();
        let mut intermediate_tables = std::mem::take(&mut self.cycle_intermediates);
        intermediate_tables.clear();

        let topo_len = self.topo_order.len();
        for i in 0..topo_len {
            let idx = self.topo_order[i];
            let is_eowc = self.queries[idx].suppresses_intermediate();
            let has_asof = self.queries[idx].asof_config.is_some();
            let has_temporal = self.queries[idx].temporal_config.is_some();

            let batches = if is_eowc {
                let query_name = self.queries[idx].name.clone();
                let window_config = self.queries[idx].window_config.clone();
                let asof_config = self.queries[idx].asof_config.clone();
                let projection_sql = self.queries[idx].projection_sql.clone();
                self.execute_eowc_query(
                    idx,
                    &query_name,
                    window_config.as_ref(),
                    asof_config.as_ref(),
                    projection_sql.as_deref(),
                    source_batches,
                    &results,
                    current_watermark,
                )
                .await?
            } else if has_asof {
                let query_name = self.queries[idx].name.clone();
                let cfg = self.queries[idx]
                    .asof_config
                    .clone()
                    .expect("has_asof guard ensures asof_config is Some");
                let projection_sql = self.queries[idx].projection_sql.clone();
                self.execute_asof_query(
                    &query_name,
                    &cfg,
                    projection_sql.as_deref(),
                    source_batches,
                    &results,
                )
                .await?
            } else if has_temporal {
                let query_name = self.queries[idx].name.clone();
                let cfg = self.queries[idx]
                    .temporal_config
                    .clone()
                    .expect("has_temporal guard ensures temporal_config is Some");
                self.execute_temporal_query(&query_name, &cfg, source_batches, &results)
                    .await?
            } else {
                self.execute_standard_query(idx).await?
            };

            // Apply Top-K post-filter if configured
            let batches = match &self.queries[idx].order_config {
                Some(OrderOperatorConfig::TopK(config)) => apply_topk_filter(&batches, config.k),
                Some(OrderOperatorConfig::PerGroupTopK(config)) => {
                    apply_topk_filter(&batches, config.k)
                }
                _ => batches,
            };

            if !batches.is_empty() {
                let query_name = self.queries[idx].name.clone();
                let schema = batches[0].schema();
                if let Ok(mem_table) =
                    datafusion::datasource::MemTable::try_new(schema, vec![batches.clone()])
                {
                    let _ = self.ctx.deregister_table(&query_name);
                    let _ = self.ctx.register_table(&query_name, Arc::new(mem_table));
                    intermediate_tables.push(query_name.clone());
                }
                results.insert(query_name, batches);
            }
        }

        self.cleanup_source_tables();
        for name in &intermediate_tables {
            let _ = self.ctx.deregister_table(name);
        }

        // Stash the (now-empty after take) intermediates back for reuse
        self.cycle_intermediates = intermediate_tables;
        Ok(results)
    }

    /// Register source batches as temporary `MemTable` providers.
    ///
    /// Sources with data get their batches registered. Sources with known
    /// schemas but no data this cycle get an empty `MemTable` so that
    /// `DataFusion` can still plan queries that reference them.
    fn register_source_tables(
        &mut self,
        source_batches: &HashMap<String, Vec<RecordBatch>>,
    ) -> Result<(), DbError> {
        for (name, batches) in source_batches {
            if batches.is_empty() {
                continue;
            }

            let schema = batches[0].schema();
            let mem_table =
                datafusion::datasource::MemTable::try_new(schema, vec![batches.clone()])
                    .map_err(|e| DbError::query_pipeline(name, &e))?;

            // Deregister first if it exists (from previous cycle)
            let _ = self.ctx.deregister_table(name);

            self.ctx
                .register_table(name, Arc::new(mem_table))
                .map_err(|e| DbError::query_pipeline(name, &e))?;

            self.registered_sources.push(name.clone());
        }

        // Register empty tables for known sources that had no data this cycle
        for (name, schema) in &self.source_schemas {
            if source_batches.contains_key(name) {
                continue;
            }
            let empty = datafusion::datasource::MemTable::try_new(schema.clone(), vec![])
                .map_err(|e| DbError::query_pipeline(name, &e))?;
            let _ = self.ctx.deregister_table(name);
            self.ctx
                .register_table(name, Arc::new(empty))
                .map_err(|e| DbError::query_pipeline(name, &e))?;
            self.registered_sources.push(name.clone());
        }

        Ok(())
    }

    /// Remove temporary source tables from the context.
    fn cleanup_source_tables(&mut self) {
        for name in self.registered_sources.drain(..) {
            let _ = self.ctx.deregister_table(&name);
        }
    }

    /// Execute a standard (non-EOWC, non-ASOF) query.
    ///
    /// On the first call for each query, attempts to detect whether the query
    /// contains a GROUP BY / aggregate. If so, creates an
    /// `IncrementalAggState` and routes subsequent calls through incremental
    /// accumulators for correct running totals across cycles.
    ///
    /// Non-aggregate queries continue using the existing `DataFusion`
    /// `MemTable` + SQL execution path.
    async fn execute_standard_query(&mut self, idx: usize) -> Result<Vec<RecordBatch>, DbError> {
        // Fast path: already have incremental agg state for this query
        if self.agg_states.contains_key(&idx) {
            return self.execute_incremental_agg(idx).await;
        }

        // Fast path: already checked and found non-aggregate
        if self.non_agg_queries.contains(&idx) {
            return self.execute_plain_query(idx).await;
        }

        // First call: try to initialize IncrementalAggState
        let query_name = self.queries[idx].name.clone();
        let query_sql = self.queries[idx].sql.clone();
        match IncrementalAggState::try_from_sql(&self.ctx, &query_sql).await {
            Ok(Some(state)) => {
                self.agg_states.insert(idx, state);
                self.try_restore_pending_agg(idx);
                self.execute_incremental_agg(idx).await
            }
            Ok(None) => {
                // Not an aggregation query — use standard path
                self.non_agg_queries.insert(idx);
                self.execute_plain_query(idx).await
            }
            Err(e) => {
                // Plan introspection failed — fall back to standard path
                tracing::debug!(
                    query = %query_name,
                    error = %e,
                    "Could not introspect query plan, using per-batch execution"
                );
                self.non_agg_queries.insert(idx);
                self.execute_plain_query(idx).await
            }
        }
    }

    /// Execute a non-aggregate query via `DataFusion` SQL.
    async fn execute_plain_query(&self, idx: usize) -> Result<Vec<RecordBatch>, DbError> {
        let query_name = &self.queries[idx].name;
        let query_sql = &self.queries[idx].sql;
        let df = self
            .ctx
            .sql(query_sql)
            .await
            .map_err(|e| DbError::query_pipeline(query_name, &e))?;
        df.collect()
            .await
            .map_err(|e| DbError::query_pipeline(query_name, &e))
    }

    /// Execute an aggregation query using incremental accumulators.
    ///
    /// Runs the pre-aggregation SQL (projection only) through `DataFusion`,
    /// then feeds the result to per-group accumulators. Emits running
    /// aggregate totals.
    async fn execute_incremental_agg(&mut self, idx: usize) -> Result<Vec<RecordBatch>, DbError> {
        let query_name = self.queries[idx].name.clone();

        let agg_state = self.agg_states.get(&idx).ok_or_else(|| {
            DbError::Pipeline(format!("internal: missing agg_state for query index {idx}"))
        })?;
        let pre_agg_sql = agg_state.pre_agg_sql().to_string();

        let pre_agg_batches = match self.ctx.sql(&pre_agg_sql).await {
            Ok(df) => df
                .collect()
                .await
                .map_err(|e| DbError::query_pipeline(&query_name, &e))?,
            Err(e) => {
                // Pre-agg SQL failed (e.g., no source data this cycle)
                // — emit current state without updating
                tracing::trace!(
                    query = %query_name,
                    error = %e,
                    "Pre-agg SQL failed, emitting current state"
                );
                let agg_state = self.agg_states.get_mut(&idx).ok_or_else(|| {
                    DbError::Pipeline(format!("internal: missing agg_state for query index {idx}"))
                })?;
                return agg_state.emit();
            }
        };

        let agg_state = self.agg_states.get_mut(&idx).ok_or_else(|| {
            DbError::Pipeline(format!("internal: missing agg_state for query index {idx}"))
        })?;
        for batch in &pre_agg_batches {
            agg_state.process_batch(batch)?;
        }

        let having_sql = agg_state.having_sql().map(String::from);
        let mut batches = agg_state.emit()?;

        // Apply HAVING filter post-emission
        if let Some(having_sql) = having_sql {
            batches = self
                .apply_having_filter(&query_name, &batches, &having_sql)
                .await?;
        }

        Ok(batches)
    }

    /// Apply a HAVING predicate to emitted aggregate batches using the
    /// session context's SQL engine.
    async fn apply_having_filter(
        &self,
        query_name: &str,
        batches: &[RecordBatch],
        having_sql: &str,
    ) -> Result<Vec<RecordBatch>, DbError> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        let schema = batches[0].schema();
        let col_list: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect();
        let filter_sql = format!(
            "SELECT {} FROM \"__having_{}\" WHERE {having_sql}",
            col_list.join(", "),
            query_name
        );
        let table_name = format!("__having_{query_name}");

        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![batches.to_vec()])
            .map_err(|e| DbError::query_pipeline(query_name, &e))?;
        let _ = self.ctx.deregister_table(&table_name);
        self.ctx
            .register_table(&table_name, Arc::new(mem_table))
            .map_err(|e| DbError::query_pipeline(query_name, &e))?;

        let result = match self.ctx.sql(&filter_sql).await {
            Ok(df) => df
                .collect()
                .await
                .map_err(|e| DbError::query_pipeline(query_name, &e)),
            Err(e) => Err(DbError::query_pipeline(query_name, &e)),
        };

        let _ = self.ctx.deregister_table(&table_name);
        result
    }

    /// Number of registered queries.
    #[allow(dead_code)]
    pub fn query_count(&self) -> usize {
        self.queries.len()
    }

    /// Returns the current EOWC backpressure level (0.0 = no pressure,
    /// 1.0 = at memory limit). Callers can use this to throttle ingestion.
    #[allow(dead_code)]
    pub fn backpressure_level(&self) -> f64 {
        let max_rows = self
            .eowc_states
            .values()
            .map(|s| s.accumulated_rows)
            .max()
            .unwrap_or(0);
        #[allow(clippy::cast_precision_loss)]
        let level = max_rows as f64 / MAX_EOWC_ACCUMULATED_ROWS as f64;
        level.min(1.0)
    }

    /// Execute an EOWC aggregate query using incremental per-window accumulators.
    ///
    /// Runs the pre-aggregation SQL against currently registered source tables,
    /// feeds the result to per-window-per-group accumulators, then closes
    /// windows whose end <= watermark.
    async fn execute_incremental_eowc(
        &mut self,
        idx: usize,
        query_name: &str,
        current_watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let pre_agg_sql = self
            .eowc_agg_states
            .get(&idx)
            .ok_or_else(|| {
                DbError::Pipeline(format!(
                    "internal: missing eowc_agg_state for query index {idx}"
                ))
            })?
            .pre_agg_sql()
            .to_string();

        let pre_agg_batches = match self.ctx.sql(&pre_agg_sql).await {
            Ok(df) => df
                .collect()
                .await
                .map_err(|e| DbError::query_pipeline(query_name, &e))?,
            Err(e) => {
                tracing::trace!(
                    query = %query_name,
                    error = %e,
                    "EOWC pre-agg SQL failed, skipping update"
                );
                Vec::new()
            }
        };

        let eowc_state = self.eowc_agg_states.get_mut(&idx).ok_or_else(|| {
            DbError::Pipeline(format!(
                "internal: missing eowc_agg_state for query index {idx}"
            ))
        })?;
        for batch in &pre_agg_batches {
            eowc_state.update_batch(batch)?;
        }

        let having_sql = eowc_state.having_sql().map(String::from);
        let mut batches = eowc_state.close_windows(current_watermark)?;

        // Apply HAVING filter if present
        if let Some(having_sql) = having_sql {
            batches = self
                .apply_having_filter(query_name, &batches, &having_sql)
                .await?;
        }

        Ok(batches)
    }

    /// Execute a core-window tumbling-window aggregate query.
    ///
    /// Runs pre-aggregation SQL, feeds results through the core engine's
    /// `TumblingWindowAssigner` for O(1) window assignment, then
    /// closes windows whose end <= watermark.
    async fn execute_core_window_query(
        &mut self,
        idx: usize,
        query_name: &str,
        current_watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let pre_agg_sql = self
            .core_window_states
            .get(&idx)
            .ok_or_else(|| {
                DbError::Pipeline(format!("internal: missing cw_state for query index {idx}"))
            })?
            .pre_agg_sql()
            .to_string();

        let pre_agg_batches = match self.ctx.sql(&pre_agg_sql).await {
            Ok(df) => df
                .collect()
                .await
                .map_err(|e| DbError::query_pipeline(query_name, &e))?,
            Err(e) => {
                tracing::trace!(
                    query = %query_name,
                    error = %e,
                    "core window pre-agg SQL failed, skipping update"
                );
                Vec::new()
            }
        };

        let cw_state = self.core_window_states.get_mut(&idx).ok_or_else(|| {
            DbError::Pipeline(format!("internal: missing cw_state for query index {idx}"))
        })?;
        for batch in &pre_agg_batches {
            cw_state.update_batch(batch)?;
        }

        let having_sql = cw_state.having_sql().map(String::from);
        let mut batches = cw_state.close_windows(current_watermark)?;

        // Apply HAVING filter if present
        if let Some(having_sql) = having_sql {
            batches = self
                .apply_having_filter(query_name, &batches, &having_sql)
                .await?;
        }

        Ok(batches)
    }

    /// Execute an EOWC (Emit On Window Close) query.
    ///
    /// For aggregate queries with a window config, routes through incremental
    /// per-window accumulators (`IncrementalEowcState`) for O(groups) memory.
    /// Non-aggregate queries and ASOF joins fall back to the raw-batch path.
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    async fn execute_eowc_query(
        &mut self,
        idx: usize,
        query_name: &str,
        window_config: Option<&WindowOperatorConfig>,
        asof_config: Option<&AsofJoinTranslatorConfig>,
        projection_sql: Option<&str>,
        source_batches: &HashMap<String, Vec<RecordBatch>>,
        intermediate_results: &HashMap<String, Vec<RecordBatch>>,
        current_watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        // Try core-window and incremental EOWC paths for aggregate queries with
        // window config (non-ASOF only — ASOF joins use a custom execution path).
        if asof_config.is_none() {
            if let Some(cfg) = window_config {
                // ── core window fast path: already routed ──
                if self.core_window_states.contains_key(&idx) {
                    return self
                        .execute_core_window_query(idx, query_name, current_watermark)
                        .await;
                }

                // ── core window detection (first call only) ──
                if !self.non_core_window_queries.contains(&idx)
                    && !self.eowc_agg_states.contains_key(&idx)
                    && !self.non_eowc_agg_queries.contains(&idx)
                {
                    let query_sql = self.queries[idx].sql.clone();
                    let cfg_clone = cfg.clone();
                    let emit_ref = self.queries[idx].emit_clause.as_ref();
                    match CoreWindowState::try_from_sql(&self.ctx, &query_sql, &cfg_clone, emit_ref)
                        .await
                    {
                        Ok(Some(state)) => {
                            tracing::info!(
                                query = query_name,
                                window_type = ?cfg_clone.window_type,
                                "EOWC query routed to core window pipeline"
                            );
                            self.core_window_states.insert(idx, state);
                            self.try_restore_pending_core_window(idx);
                            return self
                                .execute_core_window_query(idx, query_name, current_watermark)
                                .await;
                        }
                        Ok(None) => {
                            self.non_core_window_queries.insert(idx);
                        }
                        Err(e) => {
                            tracing::debug!(
                                query = query_name,
                                error = %e,
                                "core window detection failed, \
                                 falling back to EOWC path"
                            );
                            self.non_core_window_queries.insert(idx);
                        }
                    }
                }

                // ── IncrementalEowcState fast path: already have state ──
                if self.eowc_agg_states.contains_key(&idx) {
                    return self
                        .execute_incremental_eowc(idx, query_name, current_watermark)
                        .await;
                }

                // ── IncrementalEowcState detection (first call only) ──
                if !self.non_eowc_agg_queries.contains(&idx) {
                    let query_sql = self.queries[idx].sql.clone();
                    let cfg_clone = cfg.clone();
                    match IncrementalEowcState::try_from_sql(&self.ctx, &query_sql, &cfg_clone)
                        .await
                    {
                        Ok(Some(state)) => {
                            tracing::info!(
                                query = query_name,
                                "EOWC query detected as aggregate, \
                                 using incremental per-window accumulators"
                            );
                            self.eowc_agg_states.insert(idx, state);
                            self.try_restore_pending_eowc(idx);
                            return self
                                .execute_incremental_eowc(idx, query_name, current_watermark)
                                .await;
                        }
                        Ok(None) => {
                            tracing::debug!(
                                query = query_name,
                                "EOWC query is not aggregate, \
                                 using raw-batch path"
                            );
                            self.non_eowc_agg_queries.insert(idx);
                        }
                        Err(e) => {
                            tracing::debug!(
                                query = query_name,
                                error = %e,
                                "Could not introspect EOWC plan, \
                                 falling back to raw-batch path"
                            );
                            self.non_eowc_agg_queries.insert(idx);
                        }
                    }
                }
            }
        }

        // Fall through to raw-batch EOWC path.
        // Collect table refs as owned strings to avoid borrowing self.queries
        // while mutating self.eowc_states.
        let table_refs: Vec<String> = self.queries[idx].table_refs.iter().cloned().collect();

        // Accumulate current source batches into EOWC state.
        if let Some(eowc) = self.eowc_states.get_mut(&idx) {
            for table_name in &table_refs {
                // Check source_batches first, then intermediate_results
                let batches_to_add = if let Some(batches) = source_batches.get(table_name.as_str())
                {
                    Some(batches)
                } else {
                    intermediate_results.get(table_name.as_str())
                };

                if let Some(batches) = batches_to_add {
                    let entry = eowc
                        .accumulated_sources
                        .entry(table_name.clone())
                        .or_default();
                    for batch in batches {
                        if batch.num_rows() > 0 {
                            eowc.accumulated_rows += batch.num_rows();
                            entry.push(batch.clone());
                        }
                    }

                    // Coalesce when batch count exceeds threshold to reduce
                    // per-batch overhead and memory fragmentation.
                    if entry.len() > EOWC_COALESCE_BATCH_THRESHOLD {
                        let schema = entry[0].schema();
                        match arrow::compute::concat_batches(&schema, entry.as_slice()) {
                            Ok(coalesced) => *entry = vec![coalesced],
                            Err(e) => tracing::warn!(
                                table = %table_name,
                                batches = entry.len(),
                                "EOWC batch coalescing failed, \
                                 keeping fragmented batches: {e}"
                            ),
                        }
                    }
                }
            }
        }

        // Compute closed-window boundary
        let closed_cut = window_config.map_or(current_watermark, |cfg| {
            compute_closed_boundary(current_watermark, cfg)
        });

        let last_boundary = self
            .eowc_states
            .get(&idx)
            .map_or(i64::MIN, |s| s.last_closed_boundary);

        if closed_cut <= last_boundary {
            // No new windows closed. Check if memory pressure requires
            // coalescing, but do NOT advance the cutoff — that would emit
            // rows from open windows, causing data loss.
            let over_limit = self
                .eowc_states
                .get(&idx)
                .is_some_and(|s| s.accumulated_rows > MAX_EOWC_ACCUMULATED_ROWS);
            if over_limit {
                tracing::warn!(
                    query = query_name,
                    accumulated_rows = self.eowc_states.get(&idx).map_or(0, |s| s.accumulated_rows),
                    limit = MAX_EOWC_ACCUMULATED_ROWS,
                    "EOWC memory pressure: watermark has not advanced, \
                     coalescing batches to reduce fragmentation"
                );
                // Coalesce to reduce fragmentation without changing semantics
                if let Some(eowc) = self.eowc_states.get_mut(&idx) {
                    for batches in eowc.accumulated_sources.values_mut() {
                        if batches.len() > 1 {
                            let schema = batches[0].schema();
                            match arrow::compute::concat_batches(&schema, batches.as_slice()) {
                                Ok(coalesced) => *batches = vec![coalesced],
                                Err(e) => tracing::warn!("EOWC pressure coalescing failed: {e}"),
                            }
                        }
                    }
                }
            }
            return Ok(Vec::new());
        }

        let time_column = window_config.map(|cfg| cfg.time_column.clone());

        // Single-pass: split accumulated data into closed-window rows (for query)
        // and retained rows (for next cycle), avoiding a second filter pass.
        let Some(eowc) = self.eowc_states.get(&idx) else {
            return Ok(Vec::new());
        };

        let mut filtered_sources: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        let mut retained_sources: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        let mut has_data = false;
        let mut retained_rows = 0usize;

        if let Some(ref ts_col) = time_column {
            for (table_name, batches) in &eowc.accumulated_sources {
                let mut filtered_batches = Vec::new();
                let mut retained_batches = Vec::new();
                let format = batches
                    .first()
                    .map_or(laminar_core::time::TimestampFormat::UnixMillis, |b| {
                        infer_ts_format_from_batch(b, ts_col)
                    });
                for batch in batches {
                    // Closed-window rows (ts < closed_cut) — for query execution
                    if let Some(closed) = crate::batch_filter::filter_batch_by_timestamp(
                        batch,
                        ts_col,
                        closed_cut,
                        format,
                        crate::batch_filter::ThresholdOp::Less,
                    ) {
                        has_data = true;
                        filtered_batches.push(closed);
                    }
                    // Open-window rows (ts >= closed_cut) — retained for next cycle
                    if let Some(open) = crate::batch_filter::filter_batch_by_timestamp(
                        batch,
                        ts_col,
                        closed_cut,
                        format,
                        crate::batch_filter::ThresholdOp::GreaterEq,
                    ) {
                        retained_rows += open.num_rows();
                        retained_batches.push(open);
                    }
                }
                if !filtered_batches.is_empty() {
                    let schema = filtered_batches[0].schema();
                    match arrow::compute::concat_batches(&schema, &filtered_batches) {
                        Ok(coalesced) => {
                            filtered_sources.insert(table_name.clone(), vec![coalesced]);
                        }
                        Err(e) => {
                            tracing::warn!(
                                table = %table_name,
                                batches = filtered_batches.len(),
                                "EOWC filtered batch coalescing failed, \
                                 keeping fragmented: {e}"
                            );
                            filtered_sources.insert(table_name.clone(), filtered_batches);
                        }
                    }
                }
                if !retained_batches.is_empty() {
                    retained_sources.insert(table_name.clone(), retained_batches);
                }
            }
        } else {
            // No time column — pass through all accumulated data
            for (table_name, batches) in &eowc.accumulated_sources {
                if !batches.is_empty() {
                    has_data = true;
                    filtered_sources.insert(table_name.clone(), batches.clone());
                }
            }
        }

        if !has_data {
            return Ok(Vec::new());
        }

        // Register filtered data as temp tables and run the query
        let mut eowc_temp_tables: Vec<String> = Vec::new();
        for (name, batches) in &filtered_sources {
            if batches.is_empty() {
                continue;
            }
            let schema = batches[0].schema();
            let mem_table =
                datafusion::datasource::MemTable::try_new(schema, vec![batches.clone()])
                    .map_err(|e| DbError::query_pipeline(name, &e))?;
            let _ = self.ctx.deregister_table(name);
            self.ctx
                .register_table(name, Arc::new(mem_table))
                .map_err(|e| DbError::query_pipeline(name, &e))?;
            eowc_temp_tables.push(name.clone());
        }

        // Execute the query
        let query_sql = &self.queries[idx].sql;
        let temporal_config = self.queries[idx].temporal_config.as_ref();
        let batches = if let Some(cfg) = asof_config {
            self.execute_asof_query(
                query_name,
                cfg,
                projection_sql,
                &filtered_sources,
                intermediate_results,
            )
            .await?
        } else if let Some(tcfg) = temporal_config {
            self.execute_temporal_query(query_name, tcfg, &filtered_sources, intermediate_results)
                .await?
        } else {
            let df = self
                .ctx
                .sql(query_sql)
                .await
                .map_err(|e| DbError::query_pipeline(query_name, &e))?;
            df.collect()
                .await
                .map_err(|e| DbError::query_pipeline(query_name, &e))?
        };

        // Cleanup EOWC temp tables
        for name in &eowc_temp_tables {
            let _ = self.ctx.deregister_table(name);
        }

        // Apply pre-computed retained data (already split during single-pass above)
        if let Some(eowc) = self.eowc_states.get_mut(&idx) {
            if time_column.is_some() {
                eowc.accumulated_sources = retained_sources;
                eowc.accumulated_rows = retained_rows;
            } else {
                eowc.accumulated_sources.clear();
                eowc.accumulated_rows = 0;
            }
            eowc.last_closed_boundary = closed_cut;
        }

        Ok(batches)
    }

    /// Execute an ASOF join query by fetching left/right batches and performing
    /// the join in-process. Optionally applies a projection SQL for aliases and
    /// computed columns.
    async fn execute_asof_query(
        &self,
        query_name: &str,
        config: &AsofJoinTranslatorConfig,
        projection_sql: Option<&str>,
        source_batches: &HashMap<String, Vec<RecordBatch>>,
        intermediate_results: &HashMap<String, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        // Resolve left batches: source_batches → intermediate → DataFusion
        let left_batches = self
            .resolve_table_batches(&config.left_table, source_batches, intermediate_results)
            .await?;
        let right_batches = self
            .resolve_table_batches(&config.right_table, source_batches, intermediate_results)
            .await?;

        let joined =
            crate::asof_batch::execute_asof_join_batch(&left_batches, &right_batches, config)?;

        if joined.num_rows() == 0 {
            return Ok(Vec::new());
        }

        // Apply projection if present (handles aliases and computed columns)
        if let Some(proj_sql) = projection_sql {
            let schema = joined.schema();
            let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![joined]])
                .map_err(|e| DbError::query_pipeline(query_name, &e))?;

            let _ = self.ctx.deregister_table("__asof_tmp");
            self.ctx
                .register_table("__asof_tmp", Arc::new(mem_table))
                .map_err(|e| DbError::query_pipeline(query_name, &e))?;

            let df = self
                .ctx
                .sql(proj_sql)
                .await
                .map_err(|e| DbError::query_pipeline(query_name, &e))?;
            let result = df
                .collect()
                .await
                .map_err(|e| DbError::query_pipeline(query_name, &e))?;

            let _ = self.ctx.deregister_table("__asof_tmp");
            Ok(result)
        } else {
            Ok(vec![joined])
        }
    }

    /// Execute a temporal join query by fetching stream/table batches and
    /// performing a point-in-time version lookup.
    async fn execute_temporal_query(
        &self,
        query_name: &str,
        config: &TemporalJoinTranslatorConfig,
        source_batches: &HashMap<String, Vec<RecordBatch>>,
        intermediate_results: &HashMap<String, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let stream_batches = self
            .resolve_table_batches(&config.stream_table, source_batches, intermediate_results)
            .await?;
        let table_batches = self
            .resolve_table_batches(&config.table_name, source_batches, intermediate_results)
            .await?;

        let joined = crate::temporal_batch::execute_temporal_join_batch(
            &stream_batches,
            &table_batches,
            config,
        )
        .map_err(|e| DbError::Pipeline(format!("temporal join [{query_name}]: {e}")))?;

        if joined.num_rows() == 0 {
            return Ok(Vec::new());
        }

        Ok(vec![joined])
    }

    /// Resolve batches for a table name by checking source batches first,
    /// then intermediate results, then falling back to the `DataFusion` context.
    async fn resolve_table_batches(
        &self,
        table_name: &str,
        source_batches: &HashMap<String, Vec<RecordBatch>>,
        intermediate_results: &HashMap<String, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        if let Some(batches) = source_batches.get(table_name) {
            return Ok(batches.clone());
        }
        if let Some(batches) = intermediate_results.get(table_name) {
            return Ok(batches.clone());
        }
        // Fall back to DataFusion context (e.g., static reference tables)
        let sql = format!("SELECT * FROM {table_name}");
        let df = self
            .ctx
            .sql(&sql)
            .await
            .map_err(|e| DbError::query_pipeline(table_name, &e))?;
        df.collect()
            .await
            .map_err(|e| DbError::query_pipeline(table_name, &e))
    }

    /// Checkpoint all aggregate state (both running-total and EOWC).
    ///
    /// Returns `Ok(None)` if there is no state to checkpoint (no aggregate
    /// queries have accumulated any groups). Otherwise returns the serialized
    /// JSON bytes.
    pub fn checkpoint_state(&mut self) -> Result<Option<Vec<u8>>, DbError> {
        use crate::aggregate_state::StreamExecutorCheckpoint;

        let mut agg_checkpoints = HashMap::new();
        for (&idx, state) in &mut self.agg_states {
            let name = self.queries[idx].name.clone();
            agg_checkpoints.insert(name, state.checkpoint_groups()?);
        }

        let mut eowc_checkpoints = HashMap::new();
        for (&idx, state) in &mut self.eowc_agg_states {
            let name = self.queries[idx].name.clone();
            eowc_checkpoints.insert(name, state.checkpoint_windows()?);
        }

        let mut cw_checkpoints = HashMap::new();
        for (&idx, state) in &mut self.core_window_states {
            let name = self.queries[idx].name.clone();
            cw_checkpoints.insert(name, state.checkpoint_windows()?);
        }

        if agg_checkpoints.is_empty() && eowc_checkpoints.is_empty() && cw_checkpoints.is_empty() {
            return Ok(None);
        }

        let checkpoint = StreamExecutorCheckpoint {
            version: 1,
            agg_states: agg_checkpoints,
            eowc_states: eowc_checkpoints,
            core_window_states: cw_checkpoints,
        };

        let bytes = serde_json::to_vec(&checkpoint).map_err(|e| {
            DbError::Pipeline(format!("stream executor checkpoint serialization: {e}"))
        })?;

        Ok(Some(bytes))
    }

    /// Restore aggregate state from a previous checkpoint.
    ///
    /// Deserializes the checkpoint and stores it for deferred restore.
    /// Aggregate states are lazily initialized on first execution cycle,
    /// so the actual restore happens when each state is first created
    /// (via `try_restore_pending`). Any already-initialized states are
    /// restored immediately.
    ///
    /// Returns an error if the checkpoint is corrupt.
    pub fn restore_state(&mut self, bytes: &[u8]) -> Result<usize, DbError> {
        use crate::aggregate_state::StreamExecutorCheckpoint;

        let checkpoint: StreamExecutorCheckpoint = serde_json::from_slice(bytes).map_err(|e| {
            DbError::Pipeline(format!("stream executor checkpoint deserialization: {e}"))
        })?;

        let mut restored = 0usize;

        // Build name → index lookup
        let name_to_idx: HashMap<&str, usize> = self
            .queries
            .iter()
            .enumerate()
            .map(|(i, q)| (q.name.as_str(), i))
            .collect();

        // Immediately restore any already-initialized states
        for (name, agg_cp) in &checkpoint.agg_states {
            if let Some(&idx) = name_to_idx.get(name.as_str()) {
                if let Some(state) = self.agg_states.get_mut(&idx) {
                    state.restore_groups(agg_cp)?;
                    restored += 1;
                }
            }
        }
        for (name, eowc_cp) in &checkpoint.eowc_states {
            if let Some(&idx) = name_to_idx.get(name.as_str()) {
                if let Some(state) = self.eowc_agg_states.get_mut(&idx) {
                    state.restore_windows(eowc_cp)?;
                    restored += 1;
                }
            }
        }
        for (name, cw_cp) in &checkpoint.core_window_states {
            if let Some(&idx) = name_to_idx.get(name.as_str()) {
                if let Some(state) = self.core_window_states.get_mut(&idx) {
                    state.restore_windows(cw_cp)?;
                    restored += 1;
                }
            }
        }

        // Store for deferred restore of lazily-initialized states
        self.pending_restore = Some(checkpoint);

        Ok(restored)
    }

    /// Try to restore pending checkpoint data for a newly initialized
    /// aggregate state. Called after lazy initialization.
    fn try_restore_pending_agg(&mut self, idx: usize) {
        let query_name = &self.queries[idx].name;
        let pending = self.pending_restore.as_ref();
        if let Some(cp) = pending.and_then(|p| p.agg_states.get(query_name)) {
            // Clone the checkpoint data to avoid borrow conflict
            let cp_clone = cp.clone();
            if let Some(state) = self.agg_states.get_mut(&idx) {
                match state.restore_groups(&cp_clone) {
                    Ok(n) => {
                        tracing::info!(
                            query = %query_name,
                            groups = n,
                            "Restored agg state from checkpoint"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            query = %query_name,
                            error = %e,
                            "Failed to restore agg state from checkpoint"
                        );
                    }
                }
            }
        }
    }

    /// Try to restore pending checkpoint data for a newly initialized
    /// EOWC aggregate state. Called after lazy initialization.
    fn try_restore_pending_eowc(&mut self, idx: usize) {
        let query_name = &self.queries[idx].name;
        let pending = self.pending_restore.as_ref();
        if let Some(cp) = pending.and_then(|p| p.eowc_states.get(query_name)) {
            let cp_clone = cp.clone();
            if let Some(state) = self.eowc_agg_states.get_mut(&idx) {
                match state.restore_windows(&cp_clone) {
                    Ok(n) => {
                        tracing::info!(
                            query = %query_name,
                            groups = n,
                            "Restored EOWC state from checkpoint"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            query = %query_name,
                            error = %e,
                            "Failed to restore EOWC state from checkpoint"
                        );
                    }
                }
            }
        }
    }

    /// Try to restore pending checkpoint data for a newly initialized
    /// Core window pipeline state. Called after lazy initialization.
    fn try_restore_pending_core_window(&mut self, idx: usize) {
        let query_name = &self.queries[idx].name;
        let pending = self.pending_restore.as_ref();
        if let Some(cp) = pending.and_then(|p| p.core_window_states.get(query_name)) {
            let cp_clone = cp.clone();
            if let Some(state) = self.core_window_states.get_mut(&idx) {
                match state.restore_windows(&cp_clone) {
                    Ok(n) => {
                        tracing::info!(
                            query = %query_name,
                            groups = n,
                            "Restored core window state from checkpoint"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            query = %query_name,
                            error = %e,
                            "Failed to restore core window state from checkpoint"
                        );
                    }
                }
            }
        }
    }
}

/// Detect whether a SQL query contains an ASOF JOIN and, if so, extract the
/// `AsofJoinTranslatorConfig` and build a projection SQL string.
///
/// Returns `(None, None)` for non-ASOF queries.
/// Compute the closed-window boundary from the current watermark and window config.
///
/// All input data with `ts < boundary` belongs to **only** closed windows.
///
/// - **Tumbling**: floor to nearest window boundary.
/// - **Session**: `watermark - gap` (best approximation without session state).
/// - **Sliding**: align to slide interval — the earliest open window starts at
///   `((watermark - size) / slide + 1) * slide`, so data below that threshold
///   belongs only to closed windows.
fn compute_closed_boundary(watermark_ms: i64, config: &WindowOperatorConfig) -> i64 {
    match config.window_type {
        WindowType::Tumbling => {
            #[allow(clippy::cast_possible_truncation)]
            let size = config.size.as_millis() as i64;
            if size <= 0 {
                tracing::warn!("tumbling window size is zero or negative, EOWC filtering disabled");
                return watermark_ms;
            }
            // Floor to nearest window boundary
            (watermark_ms / size) * size
        }
        WindowType::Session => {
            #[allow(clippy::cast_possible_truncation)]
            let gap = config.gap.map_or(0, |g| g.as_millis() as i64);
            watermark_ms.saturating_sub(gap)
        }
        WindowType::Sliding => {
            #[allow(clippy::cast_possible_truncation)]
            let size = config.size.as_millis() as i64;
            #[allow(clippy::cast_possible_truncation)]
            let slide = config.slide.map_or(size, |s| s.as_millis() as i64);
            if slide <= 0 || size <= 0 {
                tracing::warn!(
                    slide_ms = slide,
                    size_ms = size,
                    "sliding window size/slide is zero or negative, EOWC filtering disabled"
                );
                return watermark_ms;
            }
            // The earliest open window starts at the first slide-aligned
            // boundary after (watermark - size). Data below that start
            // belongs only to fully closed windows.
            let base = watermark_ms.saturating_sub(size);
            (base / slide).saturating_add(1).saturating_mul(slide)
        }
        WindowType::Cumulate => {
            // Cumulate windows share the same epoch alignment as tumbling.
            // A full epoch is closed when watermark >= epoch_end.
            #[allow(clippy::cast_possible_truncation)]
            let size = config.size.as_millis() as i64;
            if size <= 0 {
                tracing::warn!("cumulate window size is zero or negative, EOWC filtering disabled");
                return watermark_ms;
            }
            (watermark_ms / size) * size
        }
    }
}

/// Infer the `TimestampFormat` from a `RecordBatch` column's `DataType`.
fn infer_ts_format_from_batch(
    batch: &RecordBatch,
    column: &str,
) -> laminar_core::time::TimestampFormat {
    if let Ok(idx) = batch.schema().index_of(column) {
        match batch.schema().field(idx).data_type() {
            DataType::Timestamp(_, _) => laminar_core::time::TimestampFormat::ArrowNative,
            _ => laminar_core::time::TimestampFormat::UnixMillis,
        }
    } else {
        laminar_core::time::TimestampFormat::UnixMillis
    }
}

fn detect_asof_query(sql: &str) -> (Option<AsofJoinTranslatorConfig>, Option<String>) {
    // Parse using the streaming parser which understands ASOF syntax
    let Ok(statements) = laminar_sql::parse_streaming_sql(sql) else {
        return (None, None);
    };

    // We need a raw sqlparser Statement::Query to inspect the SELECT AST
    let Some(laminar_sql::parser::StreamingStatement::Standard(stmt)) = statements.first() else {
        return (None, None);
    };

    let Statement::Query(query) = stmt.as_ref() else {
        return (None, None);
    };

    let SetExpr::Select(select) = query.body.as_ref() else {
        return (None, None);
    };

    let Ok(Some(multi)) = analyze_joins(select) else {
        return (None, None);
    };

    // Find the first ASOF join step
    let Some(asof_analysis) = multi.joins.iter().find(|j| j.is_asof_join) else {
        return (None, None);
    };

    let JoinOperatorConfig::Asof(config) = JoinOperatorConfig::from_analysis(asof_analysis) else {
        return (None, None);
    };

    // Build a projection SQL that rewrites the original SELECT list to reference
    // the flattened __asof_tmp table (no table qualifiers, disambiguated names).
    let projection_sql = build_projection_sql(select, asof_analysis, &config);

    (Some(config), Some(projection_sql))
}

fn detect_temporal_query(sql: &str) -> Option<TemporalJoinTranslatorConfig> {
    let Ok(statements) = laminar_sql::parse_streaming_sql(sql) else {
        return None;
    };

    let Some(laminar_sql::parser::StreamingStatement::Standard(stmt)) = statements.first() else {
        return None;
    };

    let Statement::Query(query) = stmt.as_ref() else {
        return None;
    };

    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };

    let Ok(Some(multi)) = analyze_joins(select) else {
        return None;
    };

    let temporal_analysis = multi.joins.iter().find(|j| j.is_temporal_join)?;

    let JoinOperatorConfig::Temporal(config) = JoinOperatorConfig::from_analysis(temporal_analysis)
    else {
        return None;
    };

    Some(config)
}

/// Build a `SELECT ... FROM __asof_tmp` projection query from the original
/// SELECT items, rewriting table-qualified references to plain column names.
fn build_projection_sql(
    select: &sqlparser::ast::Select,
    analysis: &laminar_sql::parser::join_parser::JoinAnalysis,
    config: &AsofJoinTranslatorConfig,
) -> String {
    let left_alias = analysis.left_alias.as_deref();
    let right_alias = analysis.right_alias.as_deref();

    // Collect disambiguation mapping: right-side columns that collide with left
    // get suffixed with _{right_table} in the output schema.
    // We don't know the exact schemas here, but we know the key column is shared.
    // For the right time column, it often collides (e.g., both sides have "ts").
    // The actual renaming is done in build_output_schema; here we just need to
    // handle the common case of right-qualified columns referencing their
    // potentially-renamed counterparts.

    let items: Vec<String> = select
        .projection
        .iter()
        .map(|item| rewrite_select_item(item, left_alias, right_alias, config))
        .collect();

    let select_clause = items.join(", ");

    // Rewrite WHERE clause if present
    let where_clause = select.selection.as_ref().map(|expr| {
        let rewritten = rewrite_expr(expr, left_alias, right_alias, config);
        format!(" WHERE {rewritten}")
    });

    format!(
        "SELECT {select_clause} FROM __asof_tmp{}",
        where_clause.unwrap_or_default()
    )
}

/// Rewrite a single `SelectItem` to remove table qualifiers.
fn rewrite_select_item(
    item: &SelectItem,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    config: &AsofJoinTranslatorConfig,
) -> String {
    match item {
        SelectItem::UnnamedExpr(expr) => rewrite_expr(expr, left_alias, right_alias, config),
        SelectItem::ExprWithAlias { expr, alias } => {
            let rewritten = rewrite_expr(expr, left_alias, right_alias, config);
            format!("{rewritten} AS {alias}")
        }
        SelectItem::Wildcard(WildcardAdditionalOptions { .. }) => "*".to_string(),
        SelectItem::QualifiedWildcard(name, _) => {
            let table = name.to_string();
            // t.* or q.* — just use * since all columns are flattened
            if Some(table.as_str()) == left_alias || Some(table.as_str()) == right_alias {
                "*".to_string()
            } else {
                format!("{table}.*")
            }
        }
    }
}

/// Recursively rewrite an expression tree to remove table qualifiers
/// and map right-side columns to their disambiguated names.
fn rewrite_expr(
    expr: &Expr,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    config: &AsofJoinTranslatorConfig,
) -> String {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let table = parts[0].value.as_str();
            let column = parts[1].value.as_str();

            let is_left = Some(table) == left_alias || table == config.left_table;
            let is_right = Some(table) == right_alias || table == config.right_table;

            if is_left {
                column.to_string()
            } else if is_right {
                // Check if this right column might be disambiguated.
                // The key column is excluded from the right side entirely,
                // and other duplicate columns get suffixed.
                // We suffix if the column name matches a "well-known" left-side
                // column that could collide — specifically the key column
                // (already excluded) or columns sharing the same name.
                // We use a heuristic: if the right column name equals the
                // left time column, suffix it.
                if column == config.left_time_column && column != config.right_time_column {
                    // Left and right time columns have different names — no collision
                    column.to_string()
                } else if column == config.key_column {
                    // Key column: just use the bare name (from left side)
                    column.to_string()
                } else {
                    // For other right-side columns, check if the column name
                    // matches any "standard" left-side column name.
                    // Since we don't have the full schema here, use a
                    // conservative approach: only suffix the right time column
                    // when it matches the left time column name.
                    if column == config.left_time_column && column == config.right_time_column {
                        // Same name for time columns — right side is suffixed
                        format!("{}_{}", column, config.right_table)
                    } else {
                        column.to_string()
                    }
                }
            } else {
                expr.to_string()
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = rewrite_expr(left, left_alias, right_alias, config);
            let r = rewrite_expr(right, left_alias, right_alias, config);
            format!("{l} {op} {r}")
        }
        Expr::UnaryOp { op, expr: inner } => {
            let e = rewrite_expr(inner, left_alias, right_alias, config);
            format!("{op} {e}")
        }
        Expr::Nested(inner) => {
            let e = rewrite_expr(inner, left_alias, right_alias, config);
            format!("({e})")
        }
        Expr::Function(func) => {
            // Rewrite function arguments
            let name = &func.name;
            let args: Vec<String> = match &func.args {
                sqlparser::ast::FunctionArguments::List(arg_list) => arg_list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => rewrite_expr(e, left_alias, right_alias, config),
                        other => other.to_string(),
                    })
                    .collect(),
                other => vec![other.to_string()],
            };
            format!("{name}({})", args.join(", "))
        }
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let e = rewrite_expr(inner, left_alias, right_alias, config);
            format!("CAST({e} AS {data_type})")
        }
        // For any other expression variant, fall back to sqlparser's Display
        _ => expr.to_string(),
    }
}

/// Apply a Top-K filter to batches, keeping at most `k` rows total.
///
/// `DataFusion` applies `LIMIT N` per micro-batch, but streaming Top-K
/// needs a global limit across the combined result. This function
/// concatenates all batches and slices to the first `k` rows.
fn apply_topk_filter(batches: &[RecordBatch], k: usize) -> Vec<RecordBatch> {
    if batches.is_empty() || k == 0 {
        return Vec::new();
    }

    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if total_rows <= k {
        return batches.to_vec();
    }

    // Slice across batches to keep exactly k rows
    let mut remaining = k;
    let mut result = Vec::new();
    for batch in batches {
        if remaining == 0 {
            break;
        }
        let take = remaining.min(batch.num_rows());
        result.push(batch.slice(0, take));
        remaining -= take;
    }
    result
}

#[cfg(test)]
#[allow(clippy::redundant_closure_for_method_calls)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_sql::create_session_context;
    use laminar_sql::register_streaming_functions;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]))
    }

    fn test_batch() -> RecordBatch {
        let schema = test_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_executor_basic_query() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "test_stream".to_string(),
            "SELECT name, SUM(value) as total FROM events GROUP BY name".to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();
        assert!(results.contains_key("test_stream"));
        let batches = &results["test_stream"];
        assert!(!batches.is_empty());
        // Each name should have one row
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_executor_empty_source() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "test_stream".to_string(),
            "SELECT * FROM events".to_string(),
            None,
            None,
            None,
        );

        let source_batches = HashMap::new();
        // When no source data is registered, the query references a missing table —
        // this should surface as an error, not silently produce empty results.
        let result = executor.execute_cycle(&source_batches, i64::MAX).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("test_stream"),
            "error should name the stream: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_executor_multiple_queries() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "count_stream".to_string(),
            "SELECT COUNT(*) as cnt FROM events".to_string(),
            None,
            None,
            None,
        );
        executor.add_query(
            "sum_stream".to_string(),
            "SELECT SUM(value) as total FROM events".to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();
        assert!(results.contains_key("count_stream"));
        assert!(results.contains_key("sum_stream"));
    }

    #[tokio::test]
    async fn test_executor_with_filter() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "filtered".to_string(),
            "SELECT * FROM events WHERE value > 1.5".to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();
        assert!(results.contains_key("filtered"));
        let total_rows: usize = results["filtered"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // value 2.0 and 3.0
    }

    #[tokio::test]
    async fn test_executor_cleanup_between_cycles() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "pass".to_string(),
            "SELECT * FROM events".to_string(),
            None,
            None,
            None,
        );

        // First cycle
        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);
        let r1 = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();
        assert!(r1.contains_key("pass"));

        // Second cycle with no data — table was cleaned up, so query fails
        let empty = HashMap::new();
        let r2 = executor.execute_cycle(&empty, i64::MAX).await;
        assert!(
            r2.is_err(),
            "query referencing cleaned-up table should fail"
        );
    }

    #[tokio::test]
    async fn test_executor_register_table() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        let dim_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let dim_batch = RecordBatch::try_new(
            dim_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["one", "two"])),
            ],
        )
        .unwrap();

        executor.register_table("dim", dim_batch).unwrap();

        executor.add_query(
            "joined".to_string(),
            "SELECT e.name, d.label FROM events e JOIN dim d ON e.id = d.id".to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();
        assert!(results.contains_key("joined"));
        let total_rows: usize = results["joined"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // ids 1 and 2 match
    }

    #[test]
    fn test_extract_table_references() {
        // Simple FROM
        let refs = extract_table_references("SELECT * FROM events");
        assert!(refs.contains("events"));

        // JOIN
        let refs = extract_table_references(
            "SELECT a.id, b.name FROM orders a JOIN customers b ON a.cid = b.id",
        );
        assert!(refs.contains("orders"));
        assert!(refs.contains("customers"));

        // Subquery
        let refs = extract_table_references("SELECT * FROM (SELECT * FROM raw_events) AS sub");
        assert!(refs.contains("raw_events"));

        // UNION
        let refs =
            extract_table_references("SELECT id FROM table_a UNION ALL SELECT id FROM table_b");
        assert!(refs.contains("table_a"));
        assert!(refs.contains("table_b"));

        // Invalid SQL returns empty set
        let refs = extract_table_references("NOT VALID SQL ???");
        assert!(refs.is_empty());
    }

    #[tokio::test]
    async fn test_cascading_queries() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // level1: aggregate events
        executor.add_query(
            "level1".to_string(),
            "SELECT name, SUM(value) as total FROM events GROUP BY name".to_string(),
            None,
            None,
            None,
        );
        // level2: filter level1 results
        executor.add_query(
            "level2".to_string(),
            "SELECT name, total FROM level1 WHERE total > 1.0".to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();

        // Both queries should produce output
        assert!(results.contains_key("level1"), "level1 should have results");
        assert!(results.contains_key("level2"), "level2 should have results");

        // level1: 3 rows (a=1.0, b=2.0, c=3.0)
        let l1_rows: usize = results["level1"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(l1_rows, 3);

        // level2: 2 rows (b=2.0, c=3.0 have total > 1.0)
        let l2_rows: usize = results["level2"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(l2_rows, 2);
    }

    #[tokio::test]
    async fn test_three_level_cascade() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // level1: pass through
        executor.add_query(
            "level1".to_string(),
            "SELECT name, value FROM events".to_string(),
            None,
            None,
            None,
        );
        // level2: filter
        executor.add_query(
            "level2".to_string(),
            "SELECT name, value FROM level1 WHERE value >= 2.0".to_string(),
            None,
            None,
            None,
        );
        // level3: aggregate
        executor.add_query(
            "level3".to_string(),
            "SELECT COUNT(*) as cnt FROM level2".to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();

        assert!(results.contains_key("level1"));
        assert!(results.contains_key("level2"));
        assert!(results.contains_key("level3"));

        // level1: 3 rows
        let l1_rows: usize = results["level1"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(l1_rows, 3);

        // level2: 2 rows (value 2.0 and 3.0)
        let l2_rows: usize = results["level2"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(l2_rows, 2);

        // level3: 1 row with cnt=2
        let l3_rows: usize = results["level3"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(l3_rows, 1);
    }

    #[tokio::test]
    async fn test_diamond_cascade() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // Two queries from the same source
        executor.add_query(
            "agg".to_string(),
            "SELECT name, SUM(value) as total FROM events GROUP BY name".to_string(),
            None,
            None,
            None,
        );
        executor.add_query(
            "filtered".to_string(),
            "SELECT name, value FROM events WHERE value > 1.5".to_string(),
            None,
            None,
            None,
        );
        // Third query joins results of both
        executor.add_query(
            "combined".to_string(),
            "SELECT a.name, a.total, f.value FROM agg a JOIN filtered f ON a.name = f.name"
                .to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();

        assert!(results.contains_key("agg"));
        assert!(results.contains_key("filtered"));
        assert!(results.contains_key("combined"));

        // combined: should have rows for names b and c (those in filtered)
        let combined_rows: usize = results["combined"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(combined_rows, 2);
    }

    #[tokio::test]
    async fn test_topo_order_ignores_insertion_order() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // Register downstream BEFORE upstream
        executor.add_query(
            "downstream".to_string(),
            "SELECT name, total FROM upstream WHERE total > 1.0".to_string(),
            None,
            None,
            None,
        );
        executor.add_query(
            "upstream".to_string(),
            "SELECT name, SUM(value) as total FROM events GROUP BY name".to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();

        // Both should produce output despite reversed insertion order
        assert!(
            results.contains_key("upstream"),
            "upstream should have results"
        );
        assert!(
            results.contains_key("downstream"),
            "downstream should have results"
        );

        let downstream_rows: usize = results["downstream"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(downstream_rows, 2); // b=2.0, c=3.0
    }

    #[tokio::test]
    async fn test_cascade_empty_source() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "level1".to_string(),
            "SELECT name, value FROM events".to_string(),
            None,
            None,
            None,
        );
        executor.add_query(
            "level2".to_string(),
            "SELECT name FROM level1".to_string(),
            None,
            None,
            None,
        );

        // No source data — first query references missing table, so cycle fails
        let source_batches = HashMap::new();
        let result = executor.execute_cycle(&source_batches, i64::MAX).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_cycle_propagates_planning_error() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // Register a query with invalid SQL that will fail planning
        executor.add_query(
            "bad_query".to_string(),
            "SELECTTTT * FROMM nowhere".to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let result = executor.execute_cycle(&source_batches, i64::MAX).await;
        assert!(result.is_err(), "invalid SQL should surface as error");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("bad_query"),
            "error should name the stream: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_execute_cycle_propagates_execution_error() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // Query references a column that doesn't exist in the source schema
        executor.add_query(
            "missing_col".to_string(),
            "SELECT nonexistent_column FROM events".to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let result = executor.execute_cycle(&source_batches, i64::MAX).await;
        assert!(result.is_err(), "missing column should surface as error");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("missing_col"),
            "error should name the stream: {err_msg}"
        );
    }

    // --- ASOF JOIN streaming tests ---

    fn trades_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
            Field::new("volume", DataType::Int64, false),
        ]))
    }

    fn quotes_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("bid", DataType::Float64, false),
            Field::new("ask", DataType::Float64, false),
        ]))
    }

    fn trades_batch_for_asof() -> RecordBatch {
        RecordBatch::try_new(
            trades_schema(),
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "AAPL", "GOOG"])),
                Arc::new(Int64Array::from(vec![100, 200, 150])),
                Arc::new(Float64Array::from(vec![150.0, 152.0, 2800.0])),
                Arc::new(Int64Array::from(vec![100, 200, 50])),
            ],
        )
        .unwrap()
    }

    fn quotes_batch_for_asof() -> RecordBatch {
        RecordBatch::try_new(
            quotes_schema(),
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "AAPL", "GOOG"])),
                Arc::new(Int64Array::from(vec![90, 180, 140])),
                Arc::new(Float64Array::from(vec![149.0, 151.0, 2790.0])),
                Arc::new(Float64Array::from(vec![150.0, 152.0, 2800.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_detect_asof_query_positive() {
        let sql = "SELECT t.symbol, t.price AS trade_price, q.bid \
                   FROM trades t ASOF JOIN quotes q \
                   MATCH_CONDITION(t.ts >= q.ts) ON t.symbol = q.symbol";
        let (cfg, proj) = detect_asof_query(sql);
        assert!(cfg.is_some(), "should detect ASOF config");
        assert!(proj.is_some(), "should produce projection SQL");

        let config = cfg.unwrap();
        assert_eq!(config.left_table, "trades");
        assert_eq!(config.right_table, "quotes");
        assert_eq!(config.key_column, "symbol");

        let proj_sql = proj.unwrap();
        assert!(
            proj_sql.contains("__asof_tmp"),
            "projection should reference __asof_tmp: {proj_sql}"
        );
        assert!(
            proj_sql.contains("trade_price"),
            "projection should preserve alias: {proj_sql}"
        );
    }

    #[test]
    fn test_detect_asof_query_negative() {
        let sql = "SELECT name, SUM(value) FROM events GROUP BY name";
        let (cfg, proj) = detect_asof_query(sql);
        assert!(cfg.is_none());
        assert!(proj.is_none());
    }

    #[tokio::test]
    async fn test_asof_streaming_basic() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "enriched".to_string(),
            "SELECT t.symbol, t.price, q.bid, q.ask \
             FROM trades t ASOF JOIN quotes q \
             MATCH_CONDITION(t.ts >= q.ts) ON t.symbol = q.symbol"
                .to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("trades".to_string(), vec![trades_batch_for_asof()]);
        source_batches.insert("quotes".to_string(), vec![quotes_batch_for_asof()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();
        assert!(
            results.contains_key("enriched"),
            "ASOF query should produce results"
        );
        let batches = &results["enriched"];
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "should have one row per trade");

        // Verify column names in the projected output
        let schema = batches[0].schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["symbol", "price", "bid", "ask"]);
    }

    #[tokio::test]
    async fn test_asof_streaming_with_aliases_and_computed_columns() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "spread_stream".to_string(),
            "SELECT t.symbol, t.price AS trade_price, q.bid, t.price - q.bid AS spread \
             FROM trades t ASOF JOIN quotes q \
             MATCH_CONDITION(t.ts >= q.ts) ON t.symbol = q.symbol"
                .to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("trades".to_string(), vec![trades_batch_for_asof()]);
        source_batches.insert("quotes".to_string(), vec![quotes_batch_for_asof()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();
        assert!(results.contains_key("spread_stream"));
        let batches = &results["spread_stream"];
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        // Verify column names include aliases and computed column
        let schema = batches[0].schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["symbol", "trade_price", "bid", "spread"]);

        // Verify computed spread value: AAPL trade@100 price=150.0, quote@90 bid=149.0 → spread=1.0
        let spread = batches[0]
            .column_by_name("spread")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((spread.value(0) - 1.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_asof_streaming_empty_sources() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "enriched".to_string(),
            "SELECT t.symbol, q.bid \
             FROM trades t ASOF JOIN quotes q \
             MATCH_CONDITION(t.ts >= q.ts) ON t.symbol = q.symbol"
                .to_string(),
            None,
            None,
            None,
        );

        // Only trades, no quotes → should still work (left join with nulls)
        let mut source_batches = HashMap::new();
        source_batches.insert("trades".to_string(), vec![trades_batch_for_asof()]);
        source_batches.insert(
            "quotes".to_string(),
            vec![RecordBatch::new_empty(quotes_schema())],
        );

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();
        // ASOF join with empty right produces 3 rows with null bids
        assert!(results.contains_key("enriched"));
    }

    #[tokio::test]
    async fn test_asof_in_cascade() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // ASOF join produces enriched results
        executor.add_query(
            "enriched".to_string(),
            "SELECT t.symbol, t.price, q.bid \
             FROM trades t ASOF JOIN quotes q \
             MATCH_CONDITION(t.ts >= q.ts) ON t.symbol = q.symbol"
                .to_string(),
            None,
            None,
            None,
        );

        // Downstream query filters ASOF results
        executor.add_query(
            "filtered".to_string(),
            "SELECT symbol, price, bid FROM enriched WHERE price > 151.0".to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("trades".to_string(), vec![trades_batch_for_asof()]);
        source_batches.insert("quotes".to_string(), vec![quotes_batch_for_asof()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();
        assert!(results.contains_key("enriched"));
        assert!(results.contains_key("filtered"));

        // filtered: only trades with price > 151.0 → AAPL@152.0 and GOOG@2800.0
        let filtered_rows: usize = results["filtered"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(filtered_rows, 2);
    }

    #[tokio::test]
    async fn test_non_asof_queries_unaffected() {
        // Verify that non-ASOF queries still work exactly as before
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "simple".to_string(),
            "SELECT name, SUM(value) as total FROM events GROUP BY name".to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();
        assert!(results.contains_key("simple"));
        let total_rows: usize = results["simple"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    // ── EOWC (Emit On Window Close) tests ──

    fn eowc_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("ts", DataType::Int64, false),
        ]))
    }

    fn eowc_batch(symbols: Vec<&str>, prices: Vec<f64>, timestamps: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            eowc_test_schema(),
            vec![
                Arc::new(StringArray::from(symbols)),
                Arc::new(Float64Array::from(prices)),
                Arc::new(Int64Array::from(timestamps)),
            ],
        )
        .unwrap()
    }

    fn tumbling_window_config(size_ms: u64) -> WindowOperatorConfig {
        use laminar_sql::parser::EmitStrategy;
        WindowOperatorConfig {
            window_type: WindowType::Tumbling,
            time_column: "ts".to_string(),
            size: std::time::Duration::from_millis(size_ms),
            slide: None,
            gap: None,
            allowed_lateness: std::time::Duration::ZERO,
            emit_strategy: EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        }
    }

    #[tokio::test]
    async fn test_eowc_suppresses_until_watermark_advances() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // Register with EOWC emit clause and a 1000ms tumbling window
        executor.add_query(
            "avg_price".to_string(),
            "SELECT symbol, AVG(price) as avg_price FROM trades GROUP BY symbol".to_string(),
            Some(EmitClause::OnWindowClose),
            Some(tumbling_window_config(1000)),
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert(
            "trades".to_string(),
            vec![eowc_batch(vec!["AAPL"], vec![150.0], vec![500])],
        );

        // Watermark at 500 → window [0, 1000) not closed yet
        let results = executor.execute_cycle(&source_batches, 500).await.unwrap();
        assert!(
            !results.contains_key("avg_price"),
            "EOWC should suppress output before window closes"
        );

        // Advance watermark to 1000 → window [0, 1000) is now closed
        let empty: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        let results = executor.execute_cycle(&empty, 1000).await.unwrap();
        assert!(
            results.contains_key("avg_price"),
            "EOWC should emit when watermark crosses window boundary"
        );
        let total_rows: usize = results["avg_price"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn test_eowc_accumulates_across_cycles() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "total".to_string(),
            "SELECT symbol, SUM(price) as total FROM trades GROUP BY symbol".to_string(),
            Some(EmitClause::OnWindowClose),
            Some(tumbling_window_config(1000)),
            None,
        );

        // Cycle 1: push data at ts=100
        let mut batches1 = HashMap::new();
        batches1.insert(
            "trades".to_string(),
            vec![eowc_batch(vec!["AAPL"], vec![100.0], vec![100])],
        );
        let r1 = executor.execute_cycle(&batches1, 200).await.unwrap();
        assert!(!r1.contains_key("total"), "window not closed at wm=200");

        // Cycle 2: push more data at ts=400
        let mut batches2 = HashMap::new();
        batches2.insert(
            "trades".to_string(),
            vec![eowc_batch(vec!["AAPL"], vec![200.0], vec![400])],
        );
        let r2 = executor.execute_cycle(&batches2, 600).await.unwrap();
        assert!(!r2.contains_key("total"), "window not closed at wm=600");

        // Cycle 3: watermark crosses 1000 → emit accumulated
        let empty: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        let r3 = executor.execute_cycle(&empty, 1000).await.unwrap();
        assert!(r3.contains_key("total"), "window closed at wm=1000");
        let batches = &r3["total"];
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);

        // Verify the sum is 100 + 200 = 300
        let total_col = batches[0]
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((total_col.value(0) - 300.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_eowc_purges_old_data() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "cnt".to_string(),
            "SELECT COUNT(*) as cnt FROM trades".to_string(),
            Some(EmitClause::OnWindowClose),
            Some(tumbling_window_config(1000)),
            None,
        );

        // Push data spanning two windows: [0,1000) and [1000,2000)
        let mut batches = HashMap::new();
        batches.insert(
            "trades".to_string(),
            vec![eowc_batch(
                vec!["AAPL", "AAPL", "AAPL"],
                vec![100.0, 200.0, 300.0],
                vec![500, 800, 1500],
            )],
        );

        // Watermark at 1000 → window [0,1000) closes, 2 rows (ts=500,800)
        let r1 = executor.execute_cycle(&batches, 1000).await.unwrap();
        assert!(r1.contains_key("cnt"));
        let cnt = r1["cnt"][0]
            .column_by_name("cnt")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt, 2, "first window should have 2 rows");

        // Watermark at 2000 → window [1000,2000) closes, 1 row (ts=1500)
        let empty: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        let r2 = executor.execute_cycle(&empty, 2000).await.unwrap();
        assert!(r2.contains_key("cnt"));
        let cnt2 = r2["cnt"][0]
            .column_by_name("cnt")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt2, 1, "second window should have 1 row (purged old data)");
    }

    #[tokio::test]
    async fn test_non_eowc_unaffected() {
        // Verify that non-EOWC queries still emit every cycle
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "passthrough".to_string(),
            "SELECT symbol, price FROM trades".to_string(),
            None, // no emit clause → non-EOWC
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert(
            "trades".to_string(),
            vec![eowc_batch(vec!["AAPL"], vec![150.0], vec![500])],
        );

        // Should emit immediately, even with low watermark
        let results = executor.execute_cycle(&source_batches, 0).await.unwrap();
        assert!(
            results.contains_key("passthrough"),
            "non-EOWC query should emit every cycle"
        );
        let total_rows: usize = results["passthrough"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn test_mixed_eowc_and_non_eowc() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // Register source schema so empty placeholder tables can be created
        executor.register_source_schema("trades".to_string(), eowc_test_schema());

        // Non-EOWC query
        executor.add_query(
            "realtime".to_string(),
            "SELECT symbol, price FROM trades".to_string(),
            None,
            None,
            None,
        );

        // EOWC query
        executor.add_query(
            "windowed".to_string(),
            "SELECT symbol, AVG(price) as avg_price FROM trades GROUP BY symbol".to_string(),
            Some(EmitClause::Final),
            Some(tumbling_window_config(1000)),
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert(
            "trades".to_string(),
            vec![eowc_batch(vec!["AAPL"], vec![150.0], vec![500])],
        );

        // Non-EOWC emits, EOWC suppresses
        let results = executor.execute_cycle(&source_batches, 500).await.unwrap();
        assert!(results.contains_key("realtime"), "non-EOWC should emit");
        assert!(!results.contains_key("windowed"), "EOWC should suppress");

        // Advance watermark → EOWC now emits
        // Provide an empty batch so the "trades" table is still registered
        let mut empty_source = HashMap::new();
        empty_source.insert(
            "trades".to_string(),
            vec![RecordBatch::new_empty(eowc_test_schema())],
        );
        let results = executor.execute_cycle(&empty_source, 1000).await.unwrap();
        assert!(
            results.contains_key("windowed"),
            "EOWC should emit when window closes"
        );
    }

    #[test]
    fn test_compute_closed_boundary_tumbling() {
        use laminar_sql::parser::EmitStrategy;

        let config = WindowOperatorConfig {
            window_type: WindowType::Tumbling,
            time_column: "ts".to_string(),
            size: std::time::Duration::from_millis(1000),
            slide: None,
            gap: None,
            allowed_lateness: std::time::Duration::ZERO,
            emit_strategy: EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        assert_eq!(compute_closed_boundary(999, &config), 0);
        assert_eq!(compute_closed_boundary(1000, &config), 1000);
        assert_eq!(compute_closed_boundary(1500, &config), 1000);
        assert_eq!(compute_closed_boundary(2000, &config), 2000);
    }

    #[test]
    fn test_compute_closed_boundary_session() {
        use laminar_sql::parser::EmitStrategy;

        let config = WindowOperatorConfig {
            window_type: WindowType::Session,
            time_column: "ts".to_string(),
            size: std::time::Duration::ZERO,
            slide: None,
            gap: Some(std::time::Duration::from_millis(500)),
            allowed_lateness: std::time::Duration::ZERO,
            emit_strategy: EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        assert_eq!(compute_closed_boundary(1000, &config), 500);
        assert_eq!(compute_closed_boundary(500, &config), 0);
    }

    #[test]
    fn test_compute_closed_boundary_sliding() {
        use laminar_sql::parser::EmitStrategy;

        let config = WindowOperatorConfig {
            window_type: WindowType::Sliding,
            time_column: "ts".to_string(),
            size: std::time::Duration::from_millis(2000),
            slide: Some(std::time::Duration::from_millis(500)),
            gap: None,
            allowed_lateness: std::time::Duration::ZERO,
            emit_strategy: EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        // size=2000, slide=500
        // At watermark=2000: closed=[0,2000), open=[500,2500)…
        // Earliest open window starts at 500 → boundary=500
        assert_eq!(compute_closed_boundary(2000, &config), 500);
        // At watermark=3000: closed=…[1000,3000), open=[1500,3500)…
        // Earliest open window starts at 1500 → boundary=1500
        assert_eq!(compute_closed_boundary(3000, &config), 1500);
    }

    // ── Pre-computed table refs tests ──

    #[test]
    fn test_precomputed_table_refs() {
        let ctx = create_session_context();
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "level1".to_string(),
            "SELECT name, value FROM events".to_string(),
            None,
            None,
            None,
        );
        executor.add_query(
            "level2".to_string(),
            "SELECT name FROM level1 WHERE name = 'a'".to_string(),
            None,
            None,
            None,
        );
        executor.add_query(
            "joined".to_string(),
            "SELECT a.name, b.name FROM level1 a JOIN level2 b ON a.name = b.name".to_string(),
            None,
            None,
            None,
        );

        // Table refs should be pre-computed at registration time
        assert!(
            executor.queries[0].table_refs.contains("events"),
            "level1 should reference 'events'"
        );
        assert!(
            executor.queries[1].table_refs.contains("level1"),
            "level2 should reference 'level1'"
        );
        assert!(
            executor.queries[2].table_refs.contains("level1"),
            "joined should reference 'level1'"
        );
        assert!(
            executor.queries[2].table_refs.contains("level2"),
            "joined should reference 'level2'"
        );
    }

    #[tokio::test]
    async fn test_precomputed_table_refs_used_in_topo_order() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // Register downstream BEFORE upstream (reversed order)
        executor.add_query(
            "downstream".to_string(),
            "SELECT name, total FROM upstream WHERE total > 1.0".to_string(),
            None,
            None,
            None,
        );
        executor.add_query(
            "upstream".to_string(),
            "SELECT name, SUM(value) as total FROM events GROUP BY name".to_string(),
            None,
            None,
            None,
        );

        // Pre-computed refs should enable correct topo ordering
        assert!(executor.queries[0].table_refs.contains("upstream"));
        assert!(executor.queries[1].table_refs.contains("events"));

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();

        assert!(results.contains_key("upstream"));
        assert!(results.contains_key("downstream"));
    }

    // ── EmitStrategy conversion tests ──

    #[test]
    fn test_sql_emit_to_core_all_variants() {
        use laminar_core::operator::window::EmitStrategy as CoreEmit;
        use laminar_sql::parser::EmitStrategy as SqlEmit;

        assert_eq!(
            sql_emit_to_core(&SqlEmit::OnWatermark),
            CoreEmit::OnWatermark
        );
        assert_eq!(
            sql_emit_to_core(&SqlEmit::OnWindowClose),
            CoreEmit::OnWindowClose
        );
        assert_eq!(
            sql_emit_to_core(&SqlEmit::Periodic(std::time::Duration::from_secs(5))),
            CoreEmit::Periodic(std::time::Duration::from_secs(5))
        );
        assert_eq!(sql_emit_to_core(&SqlEmit::OnUpdate), CoreEmit::OnUpdate);
        assert_eq!(sql_emit_to_core(&SqlEmit::Changelog), CoreEmit::Changelog);
        // This is the key mapping: SQL FinalOnly → Core Final
        assert_eq!(sql_emit_to_core(&SqlEmit::FinalOnly), CoreEmit::Final);
    }

    #[test]
    fn test_emit_clause_to_core() {
        use laminar_core::operator::window::EmitStrategy as CoreEmit;

        assert_eq!(
            emit_clause_to_core(&EmitClause::AfterWatermark).unwrap(),
            CoreEmit::OnWatermark
        );
        assert_eq!(
            emit_clause_to_core(&EmitClause::Final).unwrap(),
            CoreEmit::Final
        );
        assert_eq!(
            emit_clause_to_core(&EmitClause::Changes).unwrap(),
            CoreEmit::Changelog
        );
        assert_eq!(
            emit_clause_to_core(&EmitClause::OnWindowClose).unwrap(),
            CoreEmit::OnWindowClose
        );
        assert_eq!(
            emit_clause_to_core(&EmitClause::OnUpdate).unwrap(),
            CoreEmit::OnUpdate
        );
    }

    // ── Top-K post-filter tests ──

    #[test]
    fn test_apply_topk_filter_limits_rows() {
        let batch = test_batch(); // 3 rows
        let result = apply_topk_filter(&[batch], 2);
        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn test_apply_topk_filter_no_op_when_under_limit() {
        let batch = test_batch(); // 3 rows
        let result = apply_topk_filter(std::slice::from_ref(&batch), 10);
        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn test_apply_topk_filter_across_batches() {
        let batch = test_batch(); // 3 rows each
        let result = apply_topk_filter(&[batch.clone(), batch], 4);
        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn test_apply_topk_filter_empty() {
        let result = apply_topk_filter(&[], 5);
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_executor_topk_query() {
        use laminar_sql::parser::order_analyzer::OrderColumn;

        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // Register with a Top-K config that limits to 2 rows
        executor.add_query(
            "topk_stream".to_string(),
            "SELECT * FROM events ORDER BY value DESC".to_string(),
            None,
            None,
            Some(OrderOperatorConfig::TopK(
                laminar_sql::translator::TopKConfig {
                    k: 2,
                    sort_columns: vec![OrderColumn {
                        column: "value".to_string(),
                        descending: true,
                        nulls_first: false,
                    }],
                },
            )),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]); // 3 rows

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();
        assert!(results.contains_key("topk_stream"));
        let total_rows: usize = results["topk_stream"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    // -- Mixed-case column tests (identifier normalization disabled) --

    fn mixed_case_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("tradeId", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("lastPrice", DataType::Float64, false),
            Field::new("orderQty", DataType::Int64, false),
        ]))
    }

    fn mixed_case_batch() -> RecordBatch {
        RecordBatch::try_new(
            mixed_case_schema(),
            vec![
                Arc::new(Int64Array::from(vec![101, 102, 103])),
                Arc::new(StringArray::from(vec!["AAPL", "GOOG", "MSFT"])),
                Arc::new(Float64Array::from(vec![150.5, 2800.0, 310.25])),
                Arc::new(Int64Array::from(vec![10, 5, 20])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_mixed_case_select() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "trades_stream".to_string(),
            "SELECT tradeId, symbol, lastPrice FROM trades".to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("trades".to_string(), vec![mixed_case_batch()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();
        assert!(results.contains_key("trades_stream"));
        let batches = &results["trades_stream"];
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        // Verify column names are preserved
        let schema = batches[0].schema();
        assert_eq!(schema.field(0).name(), "tradeId");
        assert_eq!(schema.field(1).name(), "symbol");
        assert_eq!(schema.field(2).name(), "lastPrice");
    }

    #[tokio::test]
    async fn test_mixed_case_aggregate() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "agg_stream".to_string(),
            "SELECT symbol, AVG(lastPrice) as avgPrice, MAX(orderQty) as maxQty \
             FROM trades GROUP BY symbol"
                .to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("trades".to_string(), vec![mixed_case_batch()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();
        assert!(results.contains_key("agg_stream"));
        let batches = &results["agg_stream"];
        assert!(!batches.is_empty());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // 3 distinct symbols
    }

    #[tokio::test]
    async fn test_mixed_case_filter() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "filtered_stream".to_string(),
            "SELECT tradeId, lastPrice FROM trades WHERE lastPrice > 200.0".to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("trades".to_string(), vec![mixed_case_batch()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();
        assert!(results.contains_key("filtered_stream"));
        let batches = &results["filtered_stream"];
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // GOOG (2800) and MSFT (310.25) > 200
    }

    #[tokio::test]
    async fn test_mixed_case_star_select() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "star_stream".to_string(),
            "SELECT * FROM trades".to_string(),
            None,
            None,
            None,
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("trades".to_string(), vec![mixed_case_batch()]);

        let results = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();
        assert!(results.contains_key("star_stream"));
        let batches = &results["star_stream"];
        let schema = batches[0].schema();
        // All original mixed-case names should be preserved
        assert_eq!(schema.field(0).name(), "tradeId");
        assert_eq!(schema.field(1).name(), "symbol");
        assert_eq!(schema.field(2).name(), "lastPrice");
        assert_eq!(schema.field(3).name(), "orderQty");
    }

    /// Verify that memory pressure does NOT cause force-emit of
    /// open-window rows when the watermark hasn't advanced.
    #[tokio::test]
    async fn test_eowc_force_emit_does_not_lose_open_window_data() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "total".to_string(),
            "SELECT symbol, SUM(price) as total FROM trades GROUP BY symbol".to_string(),
            Some(EmitClause::OnWindowClose),
            Some(tumbling_window_config(1000)),
            None,
        );

        // Cycle 1: push data in window [0, 1000) with watermark at 500
        let mut batches1 = HashMap::new();
        batches1.insert(
            "trades".to_string(),
            vec![eowc_batch(vec!["AAPL"], vec![100.0], vec![500])],
        );
        let r1 = executor.execute_cycle(&batches1, 500).await.unwrap();
        assert!(!r1.contains_key("total"), "window not closed at wm=500");

        // Cycle 2: push more data, watermark STAYS at 500 (hasn't
        // advanced). Even if memory were over the threshold, should
        // NOT emit open-window rows.
        let mut batches2 = HashMap::new();
        batches2.insert(
            "trades".to_string(),
            vec![eowc_batch(vec!["AAPL"], vec![200.0], vec![600])],
        );
        let r2 = executor.execute_cycle(&batches2, 500).await.unwrap();
        assert!(
            !r2.contains_key("total"),
            "watermark unchanged at 500 — should NOT emit"
        );

        // Cycle 3: watermark advances to 1000 → window closes
        let empty: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        let r3 = executor.execute_cycle(&empty, 1000).await.unwrap();
        assert!(r3.contains_key("total"), "window closed at wm=1000");

        // CRITICAL: Both rows (100 + 200) must be present — no data loss
        let batches = &r3["total"];
        let total_col = batches[0]
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (total_col.value(0) - 300.0).abs() < f64::EPSILON,
            "expected 300 (100+200), got {} — data loss from force-emit!",
            total_col.value(0)
        );
    }
}
