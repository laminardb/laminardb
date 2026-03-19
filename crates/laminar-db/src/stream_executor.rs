//! `DataFusion` micro-batch stream executor (legacy).
//!
//! Replaced by `OperatorGraph` for production execution. Retained for its
//! comprehensive test suite and reusable detection functions.
//!
//! # Reusable functions (used by `OperatorGraph`)
//! - [`extract_table_references`] — SQL table ref extraction
//! - [`single_source_table`] — single-source detection
//! - [`detect_asof_query`] — ASOF join detection
//! - [`detect_temporal_query`] — temporal join detection
//! - [`detect_stream_join_query`] — interval join detection
//! - [`compute_closed_boundary`] — EOWC window boundary computation
//! - [`apply_topk_filter`] — Top-K post-filtering
#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::prelude::SessionContext;
use sqlparser::ast::{
    Expr, SelectItem, SetExpr, Statement, TableFactor, WildcardAdditionalOptions,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use laminar_sql::parser::join_parser::analyze_joins;
use laminar_sql::parser::{EmitClause, EmitStrategy as SqlEmitStrategy};
use laminar_sql::translator::{
    AsofJoinTranslatorConfig, JoinOperatorConfig, OrderOperatorConfig, StreamJoinConfig,
    StreamJoinType, TemporalJoinTranslatorConfig, WindowOperatorConfig, WindowType,
};

use datafusion_expr::LogicalPlan;

use crate::aggregate_state::{apply_compiled_having, CompiledProjection, IncrementalAggState};
use crate::core_window_state::CoreWindowState;
use crate::eowc_state::IncrementalEowcState;
use crate::error::DbError;

/// Convert a SQL-layer `EmitStrategy` to a core-layer `EmitStrategy`.
///
/// The SQL parser produces `EmitStrategy::FinalOnly`, while core operators
/// expect `EmitStrategy::Final`. This function maps all variants correctly.
///
/// Cannot use a `From` impl due to the orphan rule (neither type is local).
#[allow(dead_code)] // Used in tests and by emit_clause_to_core.
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
#[allow(dead_code)] // Used in tests.
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
///
/// Returns a **deduplicated** set. For self-join detection use
/// [`is_single_source_query`] instead.
pub(crate) fn extract_table_references(sql: &str) -> FxHashSet<String> {
    let mut tables = FxHashSet::default();
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

/// Check whether `sql` references exactly one logical table occurrence.
///
/// Unlike [`extract_table_references`] (which deduplicates), this counts every
/// FROM/JOIN table occurrence. A self-join like `events e1 JOIN events e2`
/// returns `false` because there are two occurrences even though the base table
/// name is the same.
///
/// Returns `Some(table_name)` when there is exactly one occurrence, `None`
/// otherwise.
pub(crate) fn single_source_table(sql: &str) -> Option<String> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).ok()?;
    let mut tables = Vec::new();
    for stmt in &statements {
        if let Statement::Query(query) = stmt {
            collect_tables_counting(query.body.as_ref(), &mut tables);
        }
    }
    if tables.len() == 1 {
        tables.into_iter().next()
    } else {
        None
    }
}

/// Like [`collect_tables_from_set_expr`] but collects every occurrence (not deduplicated).
fn collect_tables_counting(set_expr: &SetExpr, tables: &mut Vec<String>) {
    match set_expr {
        SetExpr::Select(select) => {
            for table_with_joins in &select.from {
                collect_factor_counting(&table_with_joins.relation, tables);
                for join in &table_with_joins.joins {
                    collect_factor_counting(&join.relation, tables);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_tables_counting(left.as_ref(), tables);
            collect_tables_counting(right.as_ref(), tables);
        }
        SetExpr::Query(query) => {
            collect_tables_counting(query.body.as_ref(), tables);
        }
        _ => {}
    }
}

/// Collect a single table occurrence from a `TableFactor`.
fn collect_factor_counting(factor: &TableFactor, tables: &mut Vec<String>) {
    match factor {
        TableFactor::Table { name, .. } => {
            tables.push(name.to_string());
        }
        TableFactor::Derived { subquery, .. } => {
            collect_tables_counting(subquery.body.as_ref(), tables);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_factor_counting(&table_with_joins.relation, tables);
            for join in &table_with_joins.joins {
                collect_factor_counting(&join.relation, tables);
            }
        }
        _ => {}
    }
}

/// Recursively collect table names from a `SetExpr`.
fn collect_tables_from_set_expr(set_expr: &SetExpr, tables: &mut FxHashSet<String>) {
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
fn collect_tables_from_factor(factor: &TableFactor, tables: &mut FxHashSet<String>) {
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

/// Lazily compiled post-join projection for ASOF/temporal queries.
///
/// Compiled from the projection SQL (e.g., `SELECT a, b AS alias FROM __asof_tmp`)
/// on first execution when the join output schema is known.
pub(crate) struct CompiledPostProjection {
    /// Physical expressions to evaluate per output column.
    pub(crate) exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// Output schema.
    pub(crate) output_schema: SchemaRef,
}

impl std::fmt::Debug for CompiledPostProjection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledPostProjection")
            .field("output_schema", &self.output_schema)
            .finish_non_exhaustive()
    }
}

/// A registered stream query for execution.
#[derive(Debug, Clone)]
pub(crate) struct StreamQuery {
    /// Stream name (Arc-shared to avoid cloning on hot path).
    pub name: Arc<str>,
    /// SQL query text.
    pub sql: String,
    /// ASOF join config (Arc-shared to avoid cloning on hot path).
    pub asof_config: Option<Arc<AsofJoinTranslatorConfig>>,
    /// Rewritten projection SQL (Arc-shared to avoid cloning on hot path).
    pub projection_sql: Option<Arc<str>>,
    /// Temporal join config (Arc-shared to avoid cloning on hot path).
    pub temporal_config: Option<Arc<TemporalJoinTranslatorConfig>>,
    /// Stream-stream interval join config.
    pub stream_join_config: Option<Arc<StreamJoinConfig>>,
    /// EMIT clause from the planner (e.g., `OnWindowClose`, `Final`).
    pub emit_clause: Option<EmitClause>,
    /// Window configuration (Arc-shared to avoid cloning on hot path).
    pub window_config: Option<Arc<WindowOperatorConfig>>,
    /// ORDER BY configuration (Top-K, `PerGroupTopK`, etc.).
    pub order_config: Option<OrderOperatorConfig>,
    /// Pre-computed table references (extracted once at registration).
    /// Avoids re-parsing SQL for dependency analysis every cycle.
    table_refs: FxHashSet<String>,
    /// Tombstone flag: when `true`, the query is skipped during execution
    /// and excluded from checkpoints. Used by the control channel to remove
    /// queries without invalidating indices in state maps.
    removed: bool,
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
    accumulated_sources: FxHashMap<Arc<str>, Vec<RecordBatch>>,
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
    /// Lookup table registry for versioned temporal join state.
    lookup_registry: Option<Arc<laminar_sql::datafusion::LookupTableRegistry>>,
    /// Tracks which temporary source tables are registered (for cleanup).
    registered_sources: Vec<String>,
    /// Indices into `queries` in topological (dependency) order.
    topo_order: Vec<usize>,
    /// When true, `topo_order` must be recomputed before next cycle.
    topo_dirty: bool,
    /// Known source schemas — used to register empty tables when a source
    /// has no data in a given cycle, preventing `DataFusion` planning errors.
    source_schemas: FxHashMap<String, SchemaRef>,
    /// Per-query EOWC accumulation state, keyed by query index.
    eowc_states: FxHashMap<usize, EowcState>,
    /// Per-query incremental aggregation state, keyed by query index.
    /// Initialized lazily on first cycle when the logical plan reveals
    /// the query contains a GROUP BY / aggregate.
    agg_states: FxHashMap<usize, IncrementalAggState>,
    /// Set of query indices that have been checked for aggregation but
    /// found not to be aggregate queries (avoids re-checking).
    non_agg_queries: FxHashSet<usize>,
    /// Pre-compiled projections for non-aggregate single-source queries.
    /// Evaluates SELECT + WHERE via `PhysicalExpr` without SQL parsing.
    plain_compiled: FxHashMap<usize, CompiledProjection>,
    /// Cached optimized logical plans for complex non-aggregate queries
    /// (ORDER BY, LIMIT, DISTINCT, JOINs). Skips SQL parsing + optimization
    /// on subsequent cycles; only physical planning runs per cycle.
    cached_logical_plans: FxHashMap<usize, LogicalPlan>,
    /// Schema fingerprints at cache time, keyed by query index.
    /// Used to invalidate `cached_logical_plans` when source schema changes.
    cached_plan_fingerprints: FxHashMap<usize, u64>,
    /// Per-query incremental EOWC aggregation state, keyed by query index.
    /// Initialized lazily on first EOWC cycle when the logical plan reveals
    /// the query contains a GROUP BY / aggregate.
    eowc_agg_states: FxHashMap<usize, IncrementalEowcState>,
    /// Set of EOWC query indices that have been checked for aggregation but
    /// found not to be aggregate queries (fall back to raw-batch path).
    non_eowc_agg_queries: FxHashSet<usize>,
    /// Per-query core window pipeline state for tumbling-window aggregates.
    /// Initialized lazily on first EOWC cycle when the query qualifies.
    core_window_states: FxHashMap<usize, CoreWindowState>,
    /// Set of EOWC query indices that were checked for core window routing but
    /// found not to qualify (fall through to `IncrementalEowcState`).
    non_core_window_queries: FxHashSet<usize>,
    /// Pending checkpoint data for deferred restore. Held until agg states
    /// are lazily initialized, then applied and cleared.
    pending_restore: Option<crate::aggregate_state::StreamExecutorCheckpoint>,
    /// Maximum state bytes per operator before the query returns an error.
    /// `None` = unlimited (default).
    max_state_bytes: Option<usize>,
    /// Reusable per-cycle results map (cleared each cycle to avoid reallocation).
    cycle_results: FxHashMap<Arc<str>, Vec<RecordBatch>>,
    /// Reusable per-cycle intermediate table name list.
    cycle_intermediates: Vec<String>,
    /// Lazily determined: `Some(true)` when all queries are on
    /// compiled paths (no `ctx.sql()` needed), allowing `register_source_tables()`
    /// to be skipped entirely. `None` = not yet determined.
    all_queries_compiled: Option<bool>,
    /// Lazily compiled post-join projection expressions for ASOF/temporal joins.
    /// Keyed by query index. Compiled on first execution when join output schema
    /// is known.
    compiled_post_projections: FxHashMap<usize, CompiledPostProjection>,
    /// Query indices for which post-projection compilation was attempted and failed.
    post_projection_compile_failed: FxHashSet<usize>,
    /// Cached logical plans for HAVING filter fallback paths (keyed by query index).
    /// On first call, `ctx.sql()` parses and optimizes; subsequent cycles use
    /// `create_physical_plan()` directly, skipping SQL parsing.
    cached_having_plans: FxHashMap<usize, LogicalPlan>,
    /// Cached logical plans for post-projection SQL fallback paths (keyed by query
    /// index). Same caching pattern as `cached_having_plans`.
    cached_post_projection_plans: FxHashMap<usize, LogicalPlan>,
    /// Query indices that were skipped last cycle due to budget exhaustion.
    /// These are processed first (priority) on the next cycle.
    skipped_last_cycle: FxHashSet<usize>,
    /// Per-query budget in nanoseconds. When elapsed time exceeds this,
    /// remaining queries are deferred to the next cycle. Default: 8ms.
    query_budget_ns: u64,
    /// Shared pipeline counters for fallback monitoring.
    counters: Option<Arc<crate::metrics::PipelineCounters>>,
    /// Query indices for which the execution-tier has already been logged
    /// (compiled vs cached-plan). Avoids per-cycle log spam.
    fallback_logged: FxHashSet<usize>,
    /// Per-query interval join state, keyed by query index.
    interval_join_states: FxHashMap<usize, crate::interval_join::IntervalJoinState>,
    /// Tracks the last-seen row count for temporal join tables, keyed by query index.
    /// Used to detect table modifications and warn about missing retractions.
    last_temporal_row_count: FxHashMap<usize, usize>,
}

impl StreamExecutor {
    /// Create a new executor with the given `SessionContext`.
    pub fn new(ctx: SessionContext) -> Self {
        Self {
            ctx,
            queries: Vec::new(),
            lookup_registry: None,
            registered_sources: Vec::new(),
            topo_order: Vec::new(),
            topo_dirty: true,
            source_schemas: FxHashMap::default(),
            eowc_states: FxHashMap::default(),
            agg_states: FxHashMap::default(),
            non_agg_queries: FxHashSet::default(),
            plain_compiled: FxHashMap::default(),
            cached_logical_plans: FxHashMap::default(),
            cached_plan_fingerprints: FxHashMap::default(),
            eowc_agg_states: FxHashMap::default(),
            non_eowc_agg_queries: FxHashSet::default(),
            core_window_states: FxHashMap::default(),
            non_core_window_queries: FxHashSet::default(),
            pending_restore: None,
            max_state_bytes: None,
            cycle_results: FxHashMap::default(),
            cycle_intermediates: Vec::new(),
            all_queries_compiled: None,
            compiled_post_projections: FxHashMap::default(),
            post_projection_compile_failed: FxHashSet::default(),
            cached_having_plans: FxHashMap::default(),
            cached_post_projection_plans: FxHashMap::default(),
            skipped_last_cycle: FxHashSet::default(),
            query_budget_ns: 8_000_000,
            counters: None,
            fallback_logged: FxHashSet::default(),
            interval_join_states: FxHashMap::default(),
            last_temporal_row_count: FxHashMap::default(),
        }
    }

    /// Set the maximum state bytes per operator before the query fails.
    ///
    /// `None` = unlimited (default). When set, the executor checks each
    /// aggregate/window operator's estimated memory after every cycle.
    pub fn set_max_state_bytes(&mut self, limit: Option<usize>) {
        self.max_state_bytes = limit;
    }

    /// Set the per-query execution budget in nanoseconds.
    pub fn set_query_budget_ns(&mut self, ns: u64) {
        self.query_budget_ns = ns;
    }

    /// Set shared pipeline counters for fallback monitoring.
    pub fn set_counters(&mut self, c: Arc<crate::metrics::PipelineCounters>) {
        self.counters = Some(c);
    }

    /// Record whether a query uses the compiled or cached-plan execution tier.
    /// Increments counters once per query index and logs on first occurrence.
    fn record_query_tier(&mut self, idx: usize, compiled: bool) {
        if !self.fallback_logged.insert(idx) {
            return; // already recorded
        }
        if let Some(ref c) = self.counters {
            if compiled {
                c.queries_compiled
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                c.queries_cached_plan
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    /// Set the lookup table registry for versioned temporal join lookups.
    pub fn set_lookup_registry(
        &mut self,
        registry: Arc<laminar_sql::datafusion::LookupTableRegistry>,
    ) {
        self.lookup_registry = Some(registry);
    }

    /// Returns the temporal join configs for tables that appear as right-side
    /// in temporal joins. Used during pipeline startup to pre-register versioned
    /// tables in the lookup registry.
    pub fn temporal_join_configs(&self) -> Vec<TemporalJoinTranslatorConfig> {
        self.queries
            .iter()
            .filter_map(|q| q.temporal_config.as_ref().map(|c| (**c).clone()))
            .collect()
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
        let (asof_config, mut projection_sql) = detect_asof_query(&sql);
        let (temporal_config, temporal_projection_sql) = detect_temporal_query(&sql);
        let (stream_join_config, stream_join_projection_sql) = detect_stream_join_query(&sql);
        if projection_sql.is_none() {
            projection_sql = temporal_projection_sql;
        }
        if projection_sql.is_none() {
            projection_sql = stream_join_projection_sql;
        }
        // Warn when a JOIN+BETWEEN query was not detected as an interval join
        if stream_join_config.is_none() && asof_config.is_none() && temporal_config.is_none() {
            let sql_upper = sql.to_uppercase();
            if sql_upper.contains("JOIN") && sql_upper.contains("BETWEEN") {
                tracing::warn!(
                    query = %name,
                    "Query contains JOIN with BETWEEN but was not detected as an interval join. \
                     It will execute as a batch join (matches within one cycle only). \
                     Ensure time columns in the BETWEEN clause are simple column references."
                );
            }
        }

        let table_refs = extract_table_references(&sql);
        let idx = self.queries.len();
        let query = StreamQuery {
            name: Arc::from(name),
            sql,
            asof_config: asof_config.map(Arc::new),
            projection_sql: projection_sql.map(|s| Arc::from(s.as_str())),
            temporal_config: temporal_config.map(Arc::new),
            stream_join_config: stream_join_config.map(Arc::new),
            emit_clause,
            window_config: window_config.map(Arc::new),
            order_config,
            table_refs,
            removed: false,
        };
        // Initialize EOWC state for queries that suppress intermediate results
        if query.suppresses_intermediate() {
            self.eowc_states.insert(
                idx,
                EowcState {
                    accumulated_sources: FxHashMap::default(),
                    last_closed_boundary: i64::MIN,
                    accumulated_rows: 0,
                },
            );
        }
        self.queries.push(query);
        self.topo_dirty = true;
    }

    /// Remove a query by name using a tombstone (sets `removed = true`).
    ///
    /// Tombstone approach avoids index invalidation that would break all
    /// state map keys (`agg_states`, `eowc_states`, etc.). The query struct
    /// remains in the `queries` Vec but is skipped during execution and
    /// excluded from checkpoints.
    pub fn remove_query(&mut self, name: &str) {
        for (idx, q) in self.queries.iter_mut().enumerate() {
            if &*q.name == name && !q.removed {
                q.removed = true;
                // Clean up associated state maps.
                self.agg_states.remove(&idx);
                self.eowc_states.remove(&idx);
                self.eowc_agg_states.remove(&idx);
                self.core_window_states.remove(&idx);
                self.plain_compiled.remove(&idx);
                self.cached_logical_plans.remove(&idx);
                self.cached_plan_fingerprints.remove(&idx);
                self.non_agg_queries.remove(&idx);
                self.non_eowc_agg_queries.remove(&idx);
                self.non_core_window_queries.remove(&idx);
                self.fallback_logged.remove(&idx);
                self.interval_join_states.remove(&idx);
                self.last_temporal_row_count.remove(&idx);
                self.topo_dirty = true;
                // Invalidate the compiled-path cache so it's re-evaluated.
                self.all_queries_compiled = None;
                break;
            }
        }
    }

    /// Register a static reference table (e.g., from `CREATE TABLE`).
    ///
    /// Unlike source tables, these persist across cycles.
    /// Currently only called from tests; will be wired to `CREATE TABLE` DDL.
    #[cfg(test)]
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
        let name_to_idx: FxHashMap<&str, usize> = self
            .queries
            .iter()
            .enumerate()
            .map(|(i, q)| (&*q.name, i))
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
            let in_order: FxHashSet<usize> = self.topo_order.iter().copied().collect();
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
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        current_watermark: i64,
    ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, DbError> {
        if self.topo_dirty {
            self.compute_topo_order();
        }

        // Determine if all non-skipped queries use compiled paths.
        // When true, we can skip MemTable registration entirely since compiled
        // projections read from source_batches directly. Re-evaluate each
        // cycle until it settles to true (states are lazily populated).
        if self.all_queries_compiled != Some(true) {
            let all_compiled = self.topo_order.iter().all(|&idx| {
                self.plain_compiled.contains_key(&idx)
                    || self
                        .agg_states
                        .get(&idx)
                        .is_some_and(|s| s.compiled_projection().is_some())
                    || self
                        .core_window_states
                        .get(&idx)
                        .is_some_and(|s| s.compiled_projection().is_some())
                    || self
                        .eowc_agg_states
                        .get(&idx)
                        .is_some_and(|s| s.compiled_projection().is_some())
            });
            self.all_queries_compiled = Some(all_compiled);
        }

        // Skip MemTable registration when all queries use compiled projections.
        // Intermediate result registration (for downstream queries) still runs
        // inside the loop — this only skips SOURCE table registration.
        if self.all_queries_compiled != Some(true) {
            self.register_source_tables(source_batches)?;
        }

        // Reuse per-cycle allocations: take the pre-allocated maps out of
        // self so the borrow checker allows &mut self calls while results
        // is borrowed immutably within the loop.
        let mut results = std::mem::take(&mut self.cycle_results);
        results.clear();
        let mut intermediate_tables = std::mem::take(&mut self.cycle_intermediates);
        intermediate_tables.clear();

        self.skipped_last_cycle.clear();
        let cycle_start = std::time::Instant::now();

        let topo_len = self.topo_order.len();
        for i in 0..topo_len {
            let idx = self.topo_order[i];

            // Skip tombstoned queries (removed via control channel).
            if self.queries[idx].removed {
                continue;
            }

            // Budget check: stop if elapsed time exceeds the per-query budget
            // (leaves headroom for maintenance). Always execute at least one
            // query per cycle for forward progress.
            if i > 0 {
                #[allow(clippy::cast_possible_truncation)]
                let elapsed_ns = cycle_start.elapsed().as_nanos() as u64;
                if elapsed_ns > self.query_budget_ns {
                    for j in i..topo_len {
                        self.skipped_last_cycle.insert(self.topo_order[j]);
                    }
                    tracing::debug!(
                        skipped = topo_len - i,
                        elapsed_ms = elapsed_ns / 1_000_000,
                        "per-query budget exceeded — deferring remaining queries"
                    );
                    break;
                }
            }

            let is_eowc = self.queries[idx].suppresses_intermediate();
            let has_asof = self.queries[idx].asof_config.is_some();
            let has_temporal = self.queries[idx].temporal_config.is_some();
            let has_stream_join = self.queries[idx].stream_join_config.is_some();

            let query_result = if is_eowc {
                let query_name = Arc::clone(&self.queries[idx].name);
                let window_config = self.queries[idx].window_config.as_ref().map(Arc::clone);
                let asof_config = self.queries[idx].asof_config.as_ref().map(Arc::clone);
                let projection_sql = self.queries[idx].projection_sql.as_ref().map(Arc::clone);
                self.execute_eowc_query(
                    idx,
                    &query_name,
                    window_config.as_deref(),
                    asof_config.as_deref(),
                    projection_sql.as_deref(),
                    source_batches,
                    &results,
                    current_watermark,
                )
                .await
            } else if has_asof {
                let query_name = Arc::clone(&self.queries[idx].name);
                let cfg = Arc::clone(
                    self.queries[idx]
                        .asof_config
                        .as_ref()
                        .expect("has_asof guard ensures asof_config is Some"),
                );
                let projection_sql = self.queries[idx].projection_sql.as_ref().map(Arc::clone);
                self.execute_asof_query(
                    idx,
                    &query_name,
                    &cfg,
                    projection_sql.as_deref(),
                    source_batches,
                    &results,
                )
                .await
            } else if has_temporal {
                let query_name = Arc::clone(&self.queries[idx].name);
                let cfg = Arc::clone(
                    self.queries[idx]
                        .temporal_config
                        .as_ref()
                        .expect("has_temporal guard ensures temporal_config is Some"),
                );
                let projection_sql = self.queries[idx].projection_sql.as_ref().map(Arc::clone);
                self.execute_temporal_query(
                    idx,
                    &query_name,
                    &cfg,
                    projection_sql.as_deref(),
                    source_batches,
                    &results,
                )
                .await
            } else if has_stream_join {
                let query_name = Arc::clone(&self.queries[idx].name);
                let cfg = Arc::clone(
                    self.queries[idx]
                        .stream_join_config
                        .as_ref()
                        .expect("has_stream_join guard ensures stream_join_config is Some"),
                );
                let projection_sql = self.queries[idx].projection_sql.as_ref().map(Arc::clone);
                self.execute_interval_join_query(
                    idx,
                    &query_name,
                    &cfg,
                    projection_sql.as_deref(),
                    source_batches,
                    &results,
                    current_watermark,
                )
                .await
            } else {
                self.execute_standard_query(idx, source_batches, &results)
                    .await
            };

            // Queries that depend on other stream queries may fail during
            // warmup because their upstream (e.g. EOWC) hasn't emitted yet.
            // Skip those gracefully. Queries that only reference source tables
            // propagate errors normally.
            let batches = match query_result {
                Ok(b) => b,
                Err(e) => {
                    let depends_on_stream = self.queries[idx]
                        .table_refs
                        .iter()
                        .any(|tr| self.queries.iter().any(|q| &*q.name == tr.as_str()));
                    if depends_on_stream {
                        tracing::debug!(
                            query = %self.queries[idx].name,
                            error = %e,
                            "Query skipped (upstream not ready)"
                        );
                        continue;
                    }
                    return Err(e);
                }
            };

            // ── State size bounds check ──
            if let Some(limit) = self.max_state_bytes {
                // Check incremental aggregate state
                if let Some(state) = self.agg_states.get(&idx) {
                    let size = state.estimated_size_bytes();
                    if size >= limit {
                        return Err(DbError::Pipeline(format!(
                            "state size limit exceeded for query '{}' ({size} bytes > {limit} limit)",
                            self.queries[idx].name
                        )));
                    } else if size >= limit * 4 / 5 {
                        tracing::warn!(
                            query = %self.queries[idx].name,
                            size_bytes = size,
                            limit_bytes = limit,
                            "state size at 80% of limit"
                        );
                    }
                }
                // Check core window state
                if let Some(state) = self.core_window_states.get(&idx) {
                    let size = state.estimated_size_bytes();
                    if size >= limit {
                        return Err(DbError::Pipeline(format!(
                            "state size limit exceeded for query '{}' ({size} bytes > {limit} limit)",
                            self.queries[idx].name
                        )));
                    } else if size >= limit * 4 / 5 {
                        tracing::warn!(
                            query = %self.queries[idx].name,
                            size_bytes = size,
                            limit_bytes = limit,
                            "state size at 80% of limit"
                        );
                    }
                }
                // Check EOWC aggregate state
                if let Some(state) = self.eowc_agg_states.get(&idx) {
                    let size = state.estimated_size_bytes();
                    if size >= limit {
                        return Err(DbError::Pipeline(format!(
                            "state size limit exceeded for query '{}' ({size} bytes > {limit} limit)",
                            self.queries[idx].name
                        )));
                    } else if size >= limit * 4 / 5 {
                        tracing::warn!(
                            query = %self.queries[idx].name,
                            size_bytes = size,
                            limit_bytes = limit,
                            "state size at 80% of limit"
                        );
                    }
                }
                // Check interval join state
                if let Some(state) = self.interval_join_states.get(&idx) {
                    let size = state.estimated_size_bytes();
                    if size >= limit {
                        return Err(DbError::Pipeline(format!(
                            "state size limit exceeded for query '{}' ({size} bytes > {limit} limit)",
                            self.queries[idx].name
                        )));
                    } else if size >= limit * 4 / 5 {
                        tracing::warn!(
                            query = %self.queries[idx].name,
                            size_bytes = size,
                            limit_bytes = limit,
                            "interval join state size at 80% of limit"
                        );
                    }
                }
            }

            // Apply Top-K post-filter if configured
            let batches = match &self.queries[idx].order_config {
                Some(OrderOperatorConfig::TopK(config)) => apply_topk_filter(&batches, config.k),
                Some(OrderOperatorConfig::PerGroupTopK(config)) => {
                    apply_topk_filter(&batches, config.k)
                }
                _ => batches,
            };

            let query_name = Arc::clone(&self.queries[idx].name);
            if !batches.is_empty() {
                let schema = batches[0].schema();
                if let Ok(mem_table) =
                    datafusion::datasource::MemTable::try_new(schema, vec![batches.clone()])
                {
                    let _ = self.ctx.deregister_table(&*query_name);
                    if let Err(e) = self.ctx.register_table(&*query_name, Arc::new(mem_table)) {
                        tracing::warn!(
                            query = %query_name,
                            error = %e,
                            "[LDB-3015] Failed to register intermediate table"
                        );
                    }
                    intermediate_tables.push(query_name.to_string());
                }
                results.insert(query_name, batches);
            }
        }

        if self.all_queries_compiled != Some(true) {
            self.cleanup_source_tables();
        }
        for name in &intermediate_tables {
            // Cleanup: deregister failures during teardown are benign
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
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<(), DbError> {
        for (name, batches) in source_batches {
            if batches.is_empty() {
                continue;
            }

            let schema = batches[0].schema();
            let mem_table =
                datafusion::datasource::MemTable::try_new(schema, vec![batches.clone()])
                    .map_err(|e| DbError::query_pipeline(&**name, &e))?;

            // Deregister first if it exists (from previous cycle)
            let _ = self.ctx.deregister_table(&**name);

            self.ctx
                .register_table(&**name, Arc::new(mem_table))
                .map_err(|e| DbError::query_pipeline(&**name, &e))?;

            self.registered_sources.push(name.to_string());
        }

        // Register empty tables for known sources that had no data this cycle
        for (name, schema) in &self.source_schemas {
            if source_batches.contains_key(name.as_str()) {
                continue;
            }
            let empty = datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![]])
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
}

/// Evaluate a compiled projection against source data.
///
/// Looks up the source table in both `source_batches` and `results`,
/// then evaluates the projection against each batch.
pub(crate) fn evaluate_compiled_projection(
    proj: &CompiledProjection,
    source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
) -> Vec<RecordBatch> {
    let batches = source_batches
        .get(proj.source_table())
        .or_else(|| results.get(proj.source_table()));
    let Some(batches) = batches else {
        return Vec::new();
    };
    let mut result = Vec::with_capacity(batches.len());
    for batch in batches {
        match proj.evaluate(batch) {
            Ok(b) if b.num_rows() > 0 => result.push(b),
            Ok(_) => {}
            Err(e) => {
                tracing::trace!(
                    source = proj.source_table(),
                    error = %e,
                    "Compiled projection failed, skipping batch"
                );
            }
        }
    }
    result
}

/// Information extracted from a simple Projection + Filter logical plan.
pub(crate) struct ProjectionFilterInfo {
    /// Projection expressions from the top-level Projection node.
    pub(crate) proj_exprs: Vec<datafusion_expr::Expr>,
    /// Optional WHERE predicate from a Filter node below the Projection.
    pub(crate) filter_predicate: Option<datafusion_expr::Expr>,
    /// `DFSchema` of the scan input (for compiling expressions).
    pub(crate) input_df_schema: Arc<datafusion_common::DFSchema>,
    /// Source table name from the `TableScan`.
    pub(crate) source_table: String,
}

/// Walk an optimized `LogicalPlan` to extract a simple Projection + Filter shape.
///
/// Returns `Some` only for plans of the shape:
///   `Projection? → Filter? → TableScan`
/// (with optional `SubqueryAlias` wrappers).
///
/// Returns `None` if the plan has Sort, Limit, Distinct, Join, Aggregate,
/// or any other node that `CompiledProjection` cannot handle.
pub(crate) fn extract_projection_filter(plan: &LogicalPlan) -> Option<ProjectionFilterInfo> {
    match plan {
        LogicalPlan::Projection(proj) => {
            let proj_exprs = proj.expr.clone();
            extract_filter_or_scan(&proj.input).map(|(filter_pred, input_schema, table_name)| {
                ProjectionFilterInfo {
                    proj_exprs,
                    filter_predicate: filter_pred,
                    input_df_schema: input_schema,
                    source_table: table_name,
                }
            })
        }
        // No Projection wrapper — check for Filter → TableScan directly
        _ => match extract_filter_or_scan(plan) {
            Some((filter_pred, input_schema, table_name)) => {
                // Build identity projection from the scan schema
                let proj_exprs: Vec<datafusion_expr::Expr> = input_schema
                    .fields()
                    .iter()
                    .map(|f| {
                        datafusion_expr::Expr::Column(datafusion_common::Column::new_unqualified(
                            f.name(),
                        ))
                    })
                    .collect();
                Some(ProjectionFilterInfo {
                    proj_exprs,
                    filter_predicate: filter_pred,
                    input_df_schema: input_schema,
                    source_table: table_name,
                })
            }
            None => None,
        },
    }
}

/// Extract optional `Filter` predicate + `TableScan` from the inner plan.
/// Returns `(optional_predicate, input_df_schema, table_name)`.
fn extract_filter_or_scan(
    plan: &LogicalPlan,
) -> Option<(
    Option<datafusion_expr::Expr>,
    Arc<datafusion_common::DFSchema>,
    String,
)> {
    match plan {
        LogicalPlan::Filter(filter) => match &*filter.input {
            LogicalPlan::TableScan(scan) => Some((
                Some(filter.predicate.clone()),
                Arc::clone(filter.input.schema()),
                scan.table_name.to_string(),
            )),
            LogicalPlan::SubqueryAlias(alias) => {
                if let LogicalPlan::TableScan(scan) = &*alias.input {
                    Some((
                        Some(filter.predicate.clone()),
                        Arc::clone(filter.input.schema()),
                        scan.table_name.to_string(),
                    ))
                } else {
                    None
                }
            }
            _ => None,
        },
        LogicalPlan::TableScan(scan) => {
            Some((None, Arc::clone(plan.schema()), scan.table_name.to_string()))
        }
        LogicalPlan::SubqueryAlias(alias) => extract_filter_or_scan(&alias.input),
        _ => None,
    }
}

/// Extract projection expressions from a logical plan for post-join compilation.
///
/// Walks the plan to find the Projection node, compiles each expression to a
/// `PhysicalExpr`, and returns the expressions with the output schema.
pub(crate) fn extract_projection_exprs(
    plan: &LogicalPlan,
    input_schema: &SchemaRef,
    ctx: &SessionContext,
) -> Option<(Vec<Arc<dyn PhysicalExpr>>, SchemaRef)> {
    let proj = match plan {
        LogicalPlan::Projection(p) => p,
        LogicalPlan::SubqueryAlias(a) => {
            return extract_projection_exprs(&a.input, input_schema, ctx);
        }
        _ => return None,
    };

    let df_schema = datafusion_common::DFSchema::try_from(input_schema.as_ref().clone()).ok()?;
    let state = ctx.state();
    let exec_props = state.execution_props();

    let mut exprs = Vec::with_capacity(proj.expr.len());
    let mut fields = Vec::with_capacity(proj.expr.len());

    for (i, expr) in proj.expr.iter().enumerate() {
        let phys =
            datafusion::physical_expr::create_physical_expr(expr, &df_schema, exec_props).ok()?;
        let name = proj.schema.field(i).name().clone();
        let dt = phys.data_type(input_schema).ok()?;
        let nullable = phys.nullable(input_schema).unwrap_or(true);
        fields.push(arrow::datatypes::Field::new(name, dt, nullable));
        exprs.push(phys);
    }

    let output_schema = Arc::new(arrow::datatypes::Schema::new(fields));
    Some((exprs, output_schema))
}

/// Compute a fast schema fingerprint from field names and types.
fn schema_fingerprint(schema: &arrow::datatypes::Schema) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = rustc_hash::FxHasher::default();
    for field in schema.fields() {
        field.name().hash(&mut hasher);
        std::mem::discriminant(field.data_type()).hash(&mut hasher);
    }
    hasher.finish()
}

/// Compute a combined fingerprint of all source schemas that a query references.
fn source_schemas_fingerprint(
    table_refs: &FxHashSet<String>,
    source_schemas: &FxHashMap<String, SchemaRef>,
) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = rustc_hash::FxHasher::default();
    // Sort table names for deterministic hashing
    let mut refs: Vec<&String> = table_refs.iter().collect();
    refs.sort();
    for name in refs {
        name.hash(&mut hasher);
        if let Some(schema) = source_schemas.get(name.as_str()) {
            schema_fingerprint(schema).hash(&mut hasher);
        }
    }
    hasher.finish()
}

impl StreamExecutor {
    /// Execute a standard (non-EOWC, non-ASOF) query.
    ///
    /// On the first call for each query, attempts to detect whether the query
    /// contains a GROUP BY / aggregate. If so, creates an
    /// `IncrementalAggState` and routes subsequent calls through incremental
    /// accumulators for correct running totals across cycles.
    ///
    /// Non-aggregate queries are optimized in two tiers:
    /// 1. **Compiled projection**: Single-source SELECT+WHERE queries get compiled
    ///    to `PhysicalExpr` evaluation (zero SQL overhead per cycle).
    /// 2. **Cached logical plan**: Complex queries (ORDER BY, LIMIT, DISTINCT, JOINs)
    ///    cache the optimized logical plan to skip parsing + optimization.
    async fn execute_standard_query(
        &mut self,
        idx: usize,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        // Fast path: already have incremental agg state for this query
        if self.agg_states.contains_key(&idx) {
            self.record_query_tier(idx, true);
            return self
                .execute_incremental_agg(idx, source_batches, results)
                .await;
        }

        // Fast path: already have compiled projection for this query
        if self.plain_compiled.contains_key(&idx) {
            self.record_query_tier(idx, true);
            let proj = &self.plain_compiled[&idx];
            return Ok(evaluate_compiled_projection(proj, source_batches, results));
        }

        // Fast path: already have cached logical plan for this query
        if self.cached_logical_plans.contains_key(&idx) {
            self.record_query_tier(idx, false);
            return self.execute_cached_plan_query(idx).await;
        }

        // Fast path: already checked and found non-aggregate
        if self.non_agg_queries.contains(&idx) {
            return self.execute_plain_query_with_caching(idx).await;
        }

        // First call: try to initialize IncrementalAggState
        let query_name = self.queries[idx].name.clone();
        let query_sql = self.queries[idx].sql.clone();
        match IncrementalAggState::try_from_sql(&self.ctx, &query_sql).await {
            Ok(Some(state)) => {
                self.agg_states.insert(idx, state);
                self.try_restore_pending_agg(idx);
                self.execute_incremental_agg(idx, source_batches, results)
                    .await
            }
            Ok(None) => {
                // Not an aggregation query — try compiled projection or plan caching
                self.non_agg_queries.insert(idx);
                self.try_compile_plain_query(idx, source_batches, results)
                    .await
            }
            Err(e) => {
                // Plan introspection failed — fall back to standard path
                tracing::debug!(
                    query = %query_name,
                    error = %e,
                    "Could not introspect query plan, using per-batch execution"
                );
                self.non_agg_queries.insert(idx);
                self.execute_plain_query_with_caching(idx).await
            }
        }
    }

    /// First-call path for non-aggregate queries: attempt to compile a projection
    /// or cache the logical plan, then execute.
    async fn try_compile_plain_query(
        &mut self,
        idx: usize,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let query_sql = self.queries[idx].sql.clone();
        let query_name = self.queries[idx].name.clone();

        // Only attempt compiled projection for single-source queries.
        // Use single_source_table (counts occurrences) to reject self-joins.
        if single_source_table(&query_sql).is_none() {
            return self.execute_plain_query_with_caching(idx).await;
        }

        // Plan the query to get the optimized logical plan
        let df = self
            .ctx
            .sql(&query_sql)
            .await
            .map_err(|e| DbError::query_pipeline(&*query_name, &e))?;

        let plan = df.logical_plan();
        if let Some(proj) = self.try_build_compiled_projection(plan) {
            let result = evaluate_compiled_projection(&proj, source_batches, results);
            self.plain_compiled.insert(idx, proj);
            tracing::debug!(
                query = %query_name,
                "Compiled non-aggregate single-source query to PhysicalExpr"
            );
            return Ok(result);
        }

        // Could not compile — cache the logical plan instead
        let plan = df.logical_plan().clone();
        let table_refs = &self.queries[idx].table_refs;
        let fingerprint = source_schemas_fingerprint(table_refs, &self.source_schemas);
        self.cached_logical_plans.insert(idx, plan);
        self.cached_plan_fingerprints.insert(idx, fingerprint);
        self.record_query_tier(idx, false);
        tracing::info!(
            query = %query_name,
            "Query using DataFusion cached-plan path (physical planning per cycle)"
        );
        df.collect()
            .await
            .map_err(|e| DbError::query_pipeline(&*query_name, &e))
    }

    /// Try to build a `CompiledProjection` from a logical plan.
    ///
    /// Returns `Some` if the plan is a simple Projection + Filter over a single
    /// `TableScan` and all expressions compile to `PhysicalExpr`.
    fn try_build_compiled_projection(&self, plan: &LogicalPlan) -> Option<CompiledProjection> {
        let info = extract_projection_filter(plan)?;
        let state = self.ctx.state();
        let props = state.execution_props();
        let mut compiled_exprs = Vec::with_capacity(info.proj_exprs.len());
        let mut proj_fields = Vec::with_capacity(info.proj_exprs.len());

        for expr in &info.proj_exprs {
            let phys =
                datafusion::physical_expr::create_physical_expr(expr, &info.input_df_schema, props)
                    .ok()?;
            let dt = phys
                .data_type(info.input_df_schema.as_arrow())
                .unwrap_or(DataType::Utf8);
            let name = match expr {
                datafusion_expr::Expr::Column(col) => col.name.clone(),
                datafusion_expr::Expr::Alias(alias) => alias.name.clone(),
                _ => expr.schema_name().to_string(),
            };
            proj_fields.push(arrow::datatypes::Field::new(name, dt, true));
            compiled_exprs.push(phys);
        }

        // Compile filter predicate if present
        let compiled_filter = if let Some(ref pred) = info.filter_predicate {
            Some(
                datafusion::physical_expr::create_physical_expr(pred, &info.input_df_schema, props)
                    .ok()?,
            )
        } else {
            None
        };

        let output_schema = Arc::new(arrow::datatypes::Schema::new(proj_fields));
        Some(CompiledProjection {
            source_table: info.source_table,
            exprs: compiled_exprs,
            filter: compiled_filter,
            output_schema,
        })
    }

    /// Execute a non-aggregate query, caching the optimized logical plan on first call.
    async fn execute_plain_query_with_caching(
        &mut self,
        idx: usize,
    ) -> Result<Vec<RecordBatch>, DbError> {
        // Already cached → use cached plan path
        if self.cached_logical_plans.contains_key(&idx) {
            return self.execute_cached_plan_query(idx).await;
        }

        // First call — plan from SQL and cache
        let query_name = self.queries[idx].name.clone();
        let query_sql = self.queries[idx].sql.clone();
        let df = self
            .ctx
            .sql(&query_sql)
            .await
            .map_err(|e| DbError::query_pipeline(&*query_name, &e))?;

        let plan = df.logical_plan().clone();
        let table_refs = &self.queries[idx].table_refs;
        let fingerprint = source_schemas_fingerprint(table_refs, &self.source_schemas);
        self.cached_logical_plans.insert(idx, plan);
        self.cached_plan_fingerprints.insert(idx, fingerprint);

        df.collect()
            .await
            .map_err(|e| DbError::query_pipeline(&*query_name, &e))
    }

    /// Execute a query using a cached optimized logical plan.
    ///
    /// Skips SQL parsing and logical optimization. Only physical planning
    /// runs per cycle, producing fresh physical operators with no stale state.
    async fn execute_cached_plan_query(&mut self, idx: usize) -> Result<Vec<RecordBatch>, DbError> {
        let query_name = self.queries[idx].name.clone();
        let query_sql = self.queries[idx].sql.clone();
        let table_refs = &self.queries[idx].table_refs;

        // Schema guard: invalidate cache if source schema changed
        let current_fingerprint = source_schemas_fingerprint(table_refs, &self.source_schemas);
        if let Some(&cached_fp) = self.cached_plan_fingerprints.get(&idx) {
            if cached_fp != current_fingerprint {
                tracing::debug!(
                    query = %query_name,
                    "Source schema changed, invalidating cached logical plan"
                );
                self.cached_logical_plans.remove(&idx);
                self.cached_plan_fingerprints.remove(&idx);
                // Re-plan from SQL and re-cache
                let df = self
                    .ctx
                    .sql(&query_sql)
                    .await
                    .map_err(|e| DbError::query_pipeline(&*query_name, &e))?;
                let plan = df.logical_plan().clone();
                self.cached_logical_plans.insert(idx, plan);
                self.cached_plan_fingerprints
                    .insert(idx, current_fingerprint);
                return df
                    .collect()
                    .await
                    .map_err(|e| DbError::query_pipeline(&*query_name, &e));
            }
        }

        let plan = self.cached_logical_plans.get(&idx).ok_or_else(|| {
            DbError::Pipeline(format!(
                "internal: missing cached_logical_plan for query index {idx}"
            ))
        })?;

        let physical = self
            .ctx
            .state()
            .create_physical_plan(plan)
            .await
            .map_err(|e| DbError::query_pipeline(&*query_name, &e))?;

        let task_ctx = self.ctx.task_ctx();
        datafusion::physical_plan::collect(physical, task_ctx)
            .await
            .map_err(|e| DbError::query_pipeline(&*query_name, &e))
    }

    /// Execute a cached logical plan (physical planning only, no SQL parse).
    ///
    /// Physical planning runs per cycle because `MemTable` contents change each cycle.
    /// Cost: ~5-10μs for simple scans (measured). SQL parsing and logical optimization
    /// are eliminated by caching the `LogicalPlan`. This is the best achievable for
    /// multi-source (JOIN) queries without reimplementing `DataFusion`'s join operators.
    async fn execute_cached_plan(
        &self,
        query_name: &str,
        plan: &LogicalPlan,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let physical = self
            .ctx
            .state()
            .create_physical_plan(plan)
            .await
            .map_err(|e| DbError::query_pipeline(query_name, &e))?;

        let task_ctx = self.ctx.task_ctx();
        datafusion::physical_plan::collect(physical, task_ctx)
            .await
            .map_err(|e| DbError::query_pipeline(query_name, &e))
    }

    /// Execute an aggregation query using incremental accumulators.
    ///
    /// Runs the pre-aggregation SQL (projection only) through `DataFusion`,
    /// then feeds the result to per-group accumulators. Emits running
    /// aggregate totals.
    async fn execute_incremental_agg(
        &mut self,
        idx: usize,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let query_name = self.queries[idx].name.clone();

        let agg_state = self.agg_states.get(&idx).ok_or_else(|| {
            DbError::Pipeline(format!("internal: missing agg_state for query index {idx}"))
        })?;

        // Use compiled projection if available, then cached logical plan,
        // then fall back to full SQL parsing.
        let pre_agg_batches = if let Some(proj) = agg_state.compiled_projection() {
            evaluate_compiled_projection(proj, source_batches, results)
        } else if let Some(plan) = agg_state.cached_pre_agg_plan() {
            // Cached path: skip SQL parsing + logical optimization, only
            // physical planning runs per cycle.
            let plan = plan.clone();
            self.execute_cached_plan(&query_name, &plan).await?
        } else {
            return Err(DbError::Pipeline(format!(
                "[LDB-8050] query '{query_name}': no compiled projection or cached plan"
            )));
        };

        let agg_state = self.agg_states.get_mut(&idx).ok_or_else(|| {
            DbError::Pipeline(format!("internal: missing agg_state for query index {idx}"))
        })?;
        for batch in &pre_agg_batches {
            agg_state.process_batch(batch)?;
        }

        let having_filter = agg_state.having_filter().cloned();
        let having_sql = agg_state.having_sql().map(String::from);
        let mut batches = agg_state.emit()?;

        // Apply HAVING filter post-emission (compiled or SQL fallback)
        if let Some(ref filter) = having_filter {
            batches = apply_compiled_having(&batches, filter)?;
        } else if let Some(having_sql) = having_sql {
            batches = self
                .apply_having_filter(idx, &query_name, &batches, &having_sql)
                .await?;
        }

        Ok(batches)
    }

    /// Apply a HAVING predicate to emitted aggregate batches.
    ///
    /// Caches the `LogicalPlan` on first call so subsequent cycles skip SQL
    /// parsing and only run physical planning against the new `MemTable`.
    async fn apply_having_filter(
        &mut self,
        idx: usize,
        query_name: &str,
        batches: &[RecordBatch],
        having_sql: &str,
    ) -> Result<Vec<RecordBatch>, DbError> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        let schema = batches[0].schema();
        let table_name = format!("__having_{query_name}");

        let col_list: Option<Vec<String>> = if self.cached_having_plans.contains_key(&idx) {
            None
        } else {
            Some(
                schema
                    .fields()
                    .iter()
                    .map(|f| format!("\"{}\"", f.name()))
                    .collect(),
            )
        };

        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![batches.to_vec()])
            .map_err(|e| DbError::query_pipeline(query_name, &e))?;
        let _ = self.ctx.deregister_table(&table_name);
        self.ctx
            .register_table(&table_name, Arc::new(mem_table))
            .map_err(|e| DbError::query_pipeline(query_name, &e))?;

        let result = if let Some(plan) = self.cached_having_plans.get(&idx) {
            self.execute_cached_plan(query_name, plan).await
        } else {
            let col_list = col_list.expect("col_list built for uncached path");
            let filter_sql = format!(
                "SELECT {} FROM \"__having_{}\" WHERE {having_sql}",
                col_list.join(", "),
                query_name
            );
            tracing::warn!(
                query = %query_name,
                "HAVING filter compiled to PhysicalExpr failed — using cached SQL plan"
            );
            match self.ctx.sql(&filter_sql).await {
                Ok(df) => {
                    self.cached_having_plans
                        .insert(idx, df.logical_plan().clone());
                    df.collect()
                        .await
                        .map_err(|e| DbError::query_pipeline(query_name, &e))
                }
                Err(e) => Err(DbError::query_pipeline(query_name, &e)),
            }
        };

        let _ = self.ctx.deregister_table(&table_name);
        result
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
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let eowc_state = self.eowc_agg_states.get(&idx).ok_or_else(|| {
            DbError::Pipeline(format!(
                "internal: missing eowc_agg_state for query index {idx}"
            ))
        })?;

        let pre_agg_batches = if let Some(proj) = eowc_state.compiled_projection() {
            evaluate_compiled_projection(proj, source_batches, results)
        } else if let Some(plan) = eowc_state.cached_pre_agg_plan() {
            let plan = plan.clone();
            self.execute_cached_plan(query_name, &plan).await?
        } else {
            return Err(DbError::Pipeline(format!(
                "[LDB-8050] query '{query_name}': no compiled projection or cached plan"
            )));
        };

        let eowc_state = self.eowc_agg_states.get_mut(&idx).ok_or_else(|| {
            DbError::Pipeline(format!(
                "internal: missing eowc_agg_state for query index {idx}"
            ))
        })?;
        for batch in &pre_agg_batches {
            eowc_state.update_batch(batch)?;
        }

        let having_filter = eowc_state.having_filter().cloned();
        let having_sql = eowc_state.having_sql().map(String::from);
        let mut batches = eowc_state.close_windows(current_watermark)?;

        // Apply HAVING filter (compiled or SQL fallback)
        if let Some(ref filter) = having_filter {
            batches = apply_compiled_having(&batches, filter)?;
        } else if let Some(having_sql) = having_sql {
            batches = self
                .apply_having_filter(idx, query_name, &batches, &having_sql)
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
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let cw_state = self.core_window_states.get(&idx).ok_or_else(|| {
            DbError::Pipeline(format!("internal: missing cw_state for query index {idx}"))
        })?;

        let pre_agg_batches = if let Some(proj) = cw_state.compiled_projection() {
            evaluate_compiled_projection(proj, source_batches, results)
        } else if let Some(plan) = cw_state.cached_pre_agg_plan() {
            let plan = plan.clone();
            self.execute_cached_plan(query_name, &plan).await?
        } else {
            return Err(DbError::Pipeline(format!(
                "[LDB-8050] query '{query_name}': no compiled projection or cached plan"
            )));
        };

        let cw_state = self.core_window_states.get_mut(&idx).ok_or_else(|| {
            DbError::Pipeline(format!("internal: missing cw_state for query index {idx}"))
        })?;
        for batch in &pre_agg_batches {
            cw_state.update_batch(batch)?;
        }

        let having_filter = cw_state.having_filter().cloned();
        let having_sql = cw_state.having_sql().map(String::from);
        let mut batches = cw_state.close_windows(current_watermark)?;

        // Apply HAVING filter (compiled or SQL fallback)
        if let Some(ref filter) = having_filter {
            batches = apply_compiled_having(&batches, filter)?;
        } else if let Some(having_sql) = having_sql {
            batches = self
                .apply_having_filter(idx, query_name, &batches, &having_sql)
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
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        intermediate_results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        current_watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        // Try core-window and incremental EOWC paths for aggregate queries with
        // window config (non-ASOF only — ASOF joins use a custom execution path).
        if asof_config.is_none() {
            if let Some(cfg) = window_config {
                // ── core window fast path: already routed ──
                if self.core_window_states.contains_key(&idx) {
                    return self
                        .execute_core_window_query(
                            idx,
                            query_name,
                            current_watermark,
                            source_batches,
                            intermediate_results,
                        )
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
                                .execute_core_window_query(
                                    idx,
                                    query_name,
                                    current_watermark,
                                    source_batches,
                                    intermediate_results,
                                )
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
                        .execute_incremental_eowc(
                            idx,
                            query_name,
                            current_watermark,
                            source_batches,
                            intermediate_results,
                        )
                        .await;
                }

                // ── IncrementalEowcState detection (first call only) ──
                // Guard: session windows MUST route through CoreWindowState.
                // The EOWC session fallback (one "window" per timestamp) is
                // incorrect. If CoreWindowState rejected this query, skip
                // the broken EOWC path and fall through to raw-batch.
                if matches!(cfg.window_type, WindowType::Session) {
                    tracing::warn!(
                        query = query_name,
                        "session window query could not route through CoreWindowState; \
                         skipping incremental EOWC (falling back to raw-batch)"
                    );
                    self.non_eowc_agg_queries.insert(idx);
                }

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
                                .execute_incremental_eowc(
                                    idx,
                                    query_name,
                                    current_watermark,
                                    source_batches,
                                    intermediate_results,
                                )
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
        let table_refs: Vec<Arc<str>> = self.queries[idx]
            .table_refs
            .iter()
            .map(|s| Arc::from(s.as_str()))
            .collect();

        // Accumulate current source batches into EOWC state.
        if let Some(eowc) = self.eowc_states.get_mut(&idx) {
            for table_name in &table_refs {
                // Check source_batches first, then intermediate_results
                let batches_to_add = if let Some(batches) = source_batches.get(&**table_name) {
                    Some(batches)
                } else {
                    intermediate_results.get(&**table_name)
                };

                if let Some(batches) = batches_to_add {
                    let entry = eowc
                        .accumulated_sources
                        .entry(Arc::clone(table_name))
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

        let mut filtered_sources: FxHashMap<Arc<str>, Vec<RecordBatch>> =
            FxHashMap::with_capacity_and_hasher(
                eowc.accumulated_sources.len(),
                rustc_hash::FxBuildHasher,
            );
        let mut retained_sources: FxHashMap<Arc<str>, Vec<RecordBatch>> =
            FxHashMap::with_capacity_and_hasher(
                eowc.accumulated_sources.len(),
                rustc_hash::FxBuildHasher,
            );
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
                    .map_err(|e| DbError::query_pipeline(&**name, &e))?;
            let _ = self.ctx.deregister_table(&**name);
            self.ctx
                .register_table(&**name, Arc::new(mem_table))
                .map_err(|e| DbError::query_pipeline(&**name, &e))?;
            eowc_temp_tables.push(name.to_string());
        }

        // Execute the query
        let query_sql = self.queries[idx].sql.clone();
        let temporal_config = self.queries[idx].temporal_config.clone();
        let batches = if let Some(cfg) = asof_config {
            self.execute_asof_query(
                idx,
                query_name,
                cfg,
                projection_sql,
                &filtered_sources,
                intermediate_results,
            )
            .await?
        } else if let Some(ref tcfg) = temporal_config {
            let temporal_proj = self.queries[idx].projection_sql.as_ref().map(Arc::clone);
            self.execute_temporal_query(
                idx,
                query_name,
                tcfg,
                temporal_proj.as_deref(),
                &filtered_sources,
                intermediate_results,
            )
            .await?
        } else {
            let df = self
                .ctx
                .sql(&query_sql)
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
        &mut self,
        idx: usize,
        query_name: &str,
        config: &AsofJoinTranslatorConfig,
        projection_sql: Option<&str>,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        intermediate_results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
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
            // Try compiled post-projection (lazy compile on first call)
            if let Some(compiled) = self.compiled_post_projections.get(&idx) {
                let result = Self::apply_compiled_post_projection(compiled, &joined)?;
                return Ok(vec![result]);
            }

            if !self.post_projection_compile_failed.contains(&idx) {
                let schema = joined.schema();
                if let Some(compiled) = self
                    .try_compile_post_projection(proj_sql, "__asof_tmp", &schema)
                    .await
                {
                    let result = Self::apply_compiled_post_projection(&compiled, &joined)?;
                    self.compiled_post_projections.insert(idx, compiled);
                    return Ok(vec![result]);
                }
                self.post_projection_compile_failed.insert(idx);
                tracing::warn!(
                    query = %query_name,
                    "ASOF post-projection could not be compiled — using DataFusion SQL fallback"
                );
            }

            // SQL fallback with plan caching
            let schema = joined.schema();
            let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![joined]])
                .map_err(|e| DbError::query_pipeline(query_name, &e))?;

            let _ = self.ctx.deregister_table("__asof_tmp");
            self.ctx
                .register_table("__asof_tmp", Arc::new(mem_table))
                .map_err(|e| DbError::query_pipeline(query_name, &e))?;

            let result = if let Some(plan) = self.cached_post_projection_plans.get(&idx) {
                self.execute_cached_plan(query_name, plan).await?
            } else {
                let df = self
                    .ctx
                    .sql(proj_sql)
                    .await
                    .map_err(|e| DbError::query_pipeline(query_name, &e))?;
                self.cached_post_projection_plans
                    .insert(idx, df.logical_plan().clone());
                df.collect()
                    .await
                    .map_err(|e| DbError::query_pipeline(query_name, &e))?
            };

            let _ = self.ctx.deregister_table("__asof_tmp");
            Ok(result)
        } else {
            Ok(vec![joined])
        }
    }

    /// Execute a temporal join query using the versioned lookup registry.
    ///
    /// The table must be registered as `RegisteredLookup::Versioned` in
    /// the lookup registry. The pre-built `VersionedIndex` is reused
    /// across cycles (only rebuilt on CDC updates in `poll_tables`).
    async fn execute_temporal_query(
        &mut self,
        idx: usize,
        query_name: &str,
        config: &TemporalJoinTranslatorConfig,
        projection_sql: Option<&str>,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        intermediate_results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let registry = self.lookup_registry.as_ref().ok_or_else(|| {
            DbError::Pipeline(format!(
                "temporal join [{query_name}]: lookup registry not set"
            ))
        })?;

        let Some(laminar_sql::datafusion::RegisteredLookup::Versioned(versioned)) =
            registry.get_entry(&config.table_name)
        else {
            return Err(DbError::Pipeline(format!(
                "temporal join [{query_name}]: table '{}' not registered as versioned",
                config.table_name
            )));
        };

        self.execute_versioned_temporal_query(
            idx,
            query_name,
            config,
            projection_sql,
            &versioned,
            source_batches,
            intermediate_results,
        )
        .await
    }

    /// Execute a temporal join against a versioned lookup table from the registry.
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    async fn execute_versioned_temporal_query(
        &mut self,
        idx: usize,
        query_name: &str,
        config: &TemporalJoinTranslatorConfig,
        projection_sql: Option<&str>,
        versioned: &laminar_sql::datafusion::VersionedLookupState,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        intermediate_results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        use datafusion::catalog::TableProvider;
        use datafusion::physical_plan::ExecutionPlan as _;
        use futures::TryStreamExt as _;
        use laminar_sql::datafusion::lookup_join::LookupJoinType;
        use laminar_sql::datafusion::lookup_join_exec::VersionedLookupJoinExec;

        // Warn once per query when the temporal table is modified
        let current_rows = versioned.batch.num_rows();
        let last_rows = self.last_temporal_row_count.get(&idx).copied().unwrap_or(0);
        if current_rows < last_rows {
            tracing::warn!(
                query = %query_name,
                table = %config.table_name,
                previous_rows = last_rows,
                current_rows = current_rows,
                "Temporal join table has been modified. \
                 Retractions for previously-joined events are NOT emitted. \
                 Use append-only tables for correct temporal join semantics."
            );
        }
        self.last_temporal_row_count.insert(idx, current_rows);

        let stream_batches = self
            .resolve_table_batches(&config.stream_table, source_batches, intermediate_results)
            .await?;

        if stream_batches.is_empty() {
            return Ok(Vec::new());
        }

        let table_schema = versioned.batch.schema();
        let stream_schema = stream_batches[0].schema();

        let stream_key_idx = stream_schema
            .index_of(&config.stream_key_column)
            .map_err(|_| {
                DbError::Pipeline(format!(
                    "temporal join [{query_name}]: stream key column '{}' not found",
                    config.stream_key_column
                ))
            })?;
        let stream_time_idx = stream_schema
            .index_of(&config.stream_time_column)
            .map_err(|_| {
                DbError::Pipeline(format!(
                    "temporal join [{query_name}]: stream time column '{}' not found",
                    config.stream_time_column
                ))
            })?;

        let join_type = if config.join_type == "left" {
            LookupJoinType::LeftOuter
        } else {
            LookupJoinType::Inner
        };

        let key_sort_fields: Vec<arrow::row::SortField> = versioned
            .key_columns
            .iter()
            .filter_map(|k| {
                table_schema
                    .index_of(k)
                    .ok()
                    .map(|i| arrow::row::SortField::new(table_schema.field(i).data_type().clone()))
            })
            .collect();

        let mut output_fields = stream_schema.fields().to_vec();
        output_fields.extend(table_schema.fields().iter().cloned());
        let output_schema = Arc::new(arrow::datatypes::Schema::new(output_fields));

        if stream_batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(Vec::new());
        }

        // Build the exec node with the persistent versioned table data.
        // The index is pre-built and cached in the registry — not rebuilt per cycle.
        let mem_table = datafusion::datasource::MemTable::try_new(
            Arc::clone(&stream_schema),
            vec![stream_batches],
        )
        .map_err(|e| {
            DbError::Pipeline(format!("temporal join [{query_name}]: memory table: {e}"))
        })?;
        let input = mem_table
            .scan(&self.ctx.state(), None, &[], None)
            .await
            .map_err(|e| DbError::Pipeline(format!("temporal join [{query_name}]: scan: {e}")))?;
        let exec = VersionedLookupJoinExec::try_new(
            input,
            versioned.batch.clone(),
            Arc::clone(&versioned.index),
            vec![stream_key_idx],
            stream_time_idx,
            join_type,
            output_schema,
            key_sort_fields,
        )
        .map_err(|e| {
            DbError::Pipeline(format!(
                "temporal join [{query_name}]: exec build error: {e}"
            ))
        })?;

        let task_ctx = self.ctx.state().task_ctx();
        let stream = exec
            .execute(0, task_ctx)
            .map_err(|e| DbError::Pipeline(format!("temporal join [{query_name}]: {e}")))?;

        let batches: Vec<arrow_array::RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| DbError::Pipeline(format!("temporal join [{query_name}]: {e}")))?;

        // Apply projection SQL to rewrite column names/aliases/expressions.
        if let Some(proj_sql) = projection_sql {
            if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
                return Ok(Vec::new());
            }

            // Try compiled post-projection (lazy compile on first call)
            if let Some(compiled) = self.compiled_post_projections.get(&idx) {
                let mut result = Vec::with_capacity(batches.len());
                for batch in &batches {
                    if batch.num_rows() > 0 {
                        result.push(Self::apply_compiled_post_projection(compiled, batch)?);
                    }
                }
                return Ok(result);
            }

            if !self.post_projection_compile_failed.contains(&idx) {
                let schema = batches[0].schema();
                if let Some(compiled) = self
                    .try_compile_post_projection(proj_sql, "__temporal_tmp", &schema)
                    .await
                {
                    let mut result = Vec::with_capacity(batches.len());
                    for batch in &batches {
                        if batch.num_rows() > 0 {
                            result.push(Self::apply_compiled_post_projection(&compiled, batch)?);
                        }
                    }
                    self.compiled_post_projections.insert(idx, compiled);
                    return Ok(result);
                }
                self.post_projection_compile_failed.insert(idx);
                tracing::warn!(
                    query = %query_name,
                    "Temporal post-projection could not be compiled — using DataFusion SQL fallback"
                );
            }

            // SQL fallback with plan caching
            let schema = batches[0].schema();
            let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![batches])
                .map_err(|e| DbError::query_pipeline(query_name, &e))?;

            let _ = self.ctx.deregister_table("__temporal_tmp");
            self.ctx
                .register_table("__temporal_tmp", Arc::new(mem_table))
                .map_err(|e| DbError::query_pipeline(query_name, &e))?;

            let result = if let Some(plan) = self.cached_post_projection_plans.get(&idx) {
                self.execute_cached_plan(query_name, plan).await?
            } else {
                let df = self
                    .ctx
                    .sql(proj_sql)
                    .await
                    .map_err(|e| DbError::query_pipeline(query_name, &e))?;
                self.cached_post_projection_plans
                    .insert(idx, df.logical_plan().clone());
                df.collect()
                    .await
                    .map_err(|e| DbError::query_pipeline(query_name, &e))?
            };

            let _ = self.ctx.deregister_table("__temporal_tmp");
            Ok(result)
        } else {
            Ok(batches)
        }
    }

    /// Try to compile a post-join projection SQL to physical expressions.
    ///
    /// On success, returns `CompiledPostProjection` that can evaluate the
    /// projection directly on a `RecordBatch` without SQL overhead.
    async fn try_compile_post_projection(
        &self,
        proj_sql: &str,
        tmp_table_name: &str,
        batch_schema: &SchemaRef,
    ) -> Option<CompiledPostProjection> {
        let empty =
            datafusion::datasource::MemTable::try_new(batch_schema.clone(), vec![vec![]]).ok()?;
        let _ = self.ctx.deregister_table(tmp_table_name);
        self.ctx
            .register_table(tmp_table_name, Arc::new(empty))
            .ok()?;

        let df = self.ctx.sql(proj_sql).await.ok()?;
        let plan = df.logical_plan().clone();
        let _ = self.ctx.deregister_table(tmp_table_name);

        // Extract projection expressions from the logical plan
        let (exprs, output_schema) = extract_projection_exprs(&plan, batch_schema, &self.ctx)?;
        Some(CompiledPostProjection {
            exprs,
            output_schema,
        })
    }

    /// Apply a compiled post-projection to a batch.
    fn apply_compiled_post_projection(
        proj: &CompiledPostProjection,
        batch: &RecordBatch,
    ) -> Result<RecordBatch, DbError> {
        if batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&proj.output_schema)));
        }
        let mut arrays = Vec::with_capacity(proj.exprs.len());
        for expr in &proj.exprs {
            let col = expr
                .evaluate(batch)
                .map_err(|e| DbError::Pipeline(format!("post-projection evaluate: {e}")))?
                .into_array(batch.num_rows())
                .map_err(|e| DbError::Pipeline(format!("post-projection to array: {e}")))?;
            arrays.push(col);
        }
        RecordBatch::try_new(Arc::clone(&proj.output_schema), arrays)
            .map_err(|e| DbError::Pipeline(format!("post-projection batch: {e}")))
    }

    /// Resolve batches for a table name by checking source batches first,
    /// then intermediate results, then falling back to the `DataFusion` context.
    async fn resolve_table_batches(
        &self,
        table_name: &str,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        intermediate_results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        if let Some(batches) = source_batches.get(table_name) {
            return Ok(batches.clone());
        }
        if let Some(batches) = intermediate_results.get(table_name) {
            return Ok(batches.clone());
        }
        // Fall back to DataFusion context (e.g., static reference tables).
        // Use ctx.table() instead of ctx.sql() to skip SQL parsing overhead.
        let df = self
            .ctx
            .table(table_name)
            .await
            .map_err(|e| DbError::query_pipeline(table_name, &e))?;
        df.collect()
            .await
            .map_err(|e| DbError::query_pipeline(table_name, &e))
    }

    /// Snapshot all aggregate state into a `StreamExecutorCheckpoint` struct.
    ///
    /// This requires `&mut self` because `Accumulator::state()` is `&mut`.
    /// Returns `Ok(None)` if there is no state to checkpoint.
    pub fn snapshot_state(
        &mut self,
    ) -> Result<Option<crate::aggregate_state::StreamExecutorCheckpoint>, DbError> {
        use crate::aggregate_state::{RawEowcCheckpoint, StreamExecutorCheckpoint};

        let mut agg_checkpoints =
            FxHashMap::with_capacity_and_hasher(self.agg_states.len(), rustc_hash::FxBuildHasher);
        for (&idx, state) in &mut self.agg_states {
            let name = self.queries[idx].name.to_string();
            agg_checkpoints.insert(name, state.checkpoint_groups()?);
        }

        let mut eowc_checkpoints = FxHashMap::with_capacity_and_hasher(
            self.eowc_agg_states.len(),
            rustc_hash::FxBuildHasher,
        );
        for (&idx, state) in &mut self.eowc_agg_states {
            let name = self.queries[idx].name.to_string();
            eowc_checkpoints.insert(name, state.checkpoint_windows()?);
        }

        let mut cw_checkpoints = FxHashMap::with_capacity_and_hasher(
            self.core_window_states.len(),
            rustc_hash::FxBuildHasher,
        );
        for (&idx, state) in &mut self.core_window_states {
            let name = self.queries[idx].name.to_string();
            cw_checkpoints.insert(name, state.checkpoint_windows()?);
        }

        // Checkpoint raw-batch EOWC states (non-aggregate EOWC queries)
        let mut raw_eowc_checkpoints =
            FxHashMap::with_capacity_and_hasher(self.eowc_states.len(), rustc_hash::FxBuildHasher);
        for (&idx, eowc) in &self.eowc_states {
            if eowc.accumulated_rows == 0 {
                continue;
            }
            let name = self.queries[idx].name.to_string();
            let mut sources = FxHashMap::with_capacity_and_hasher(
                eowc.accumulated_sources.len(),
                rustc_hash::FxBuildHasher,
            );
            for (src_name, batches) in &eowc.accumulated_sources {
                let mut ipc_batches = Vec::with_capacity(batches.len());
                for batch in batches {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let ipc_bytes = laminar_core::serialization::serialize_batch_stream(batch)
                        .map_err(|e| DbError::Pipeline(format!("EOWC batch serialization: {e}")))?;
                    ipc_batches.push(ipc_bytes);
                }
                if !ipc_batches.is_empty() {
                    sources.insert(src_name.to_string(), ipc_batches);
                }
            }
            raw_eowc_checkpoints.insert(
                name,
                RawEowcCheckpoint {
                    last_closed_boundary: eowc.last_closed_boundary,
                    accumulated_rows: eowc.accumulated_rows,
                    sources,
                },
            );
        }

        // Checkpoint interval join states (compact before serializing)
        let mut join_checkpoints = FxHashMap::with_capacity_and_hasher(
            self.interval_join_states.len(),
            rustc_hash::FxBuildHasher,
        );
        for (&idx, state) in &mut self.interval_join_states {
            let name = self.queries[idx].name.to_string();
            if let Some(ref cfg) = self.queries[idx].stream_join_config {
                join_checkpoints.insert(
                    name,
                    state.snapshot_checkpoint(
                        &cfg.left_key,
                        &cfg.left_time_column,
                        &cfg.right_key,
                        &cfg.right_time_column,
                    )?,
                );
            } else {
                // Fallback: checkpoint without column names (shouldn't happen)
                join_checkpoints.insert(name, state.snapshot_checkpoint("", "", "", "")?);
            }
        }

        if agg_checkpoints.is_empty()
            && eowc_checkpoints.is_empty()
            && cw_checkpoints.is_empty()
            && join_checkpoints.is_empty()
            && raw_eowc_checkpoints.is_empty()
        {
            return Ok(None);
        }

        Ok(Some(StreamExecutorCheckpoint {
            version: 2,
            vnode_count: laminar_core::state::VNODE_COUNT,
            agg_states: agg_checkpoints,
            eowc_states: eowc_checkpoints,
            core_window_states: cw_checkpoints,
            join_states: join_checkpoints,
            raw_eowc_states: raw_eowc_checkpoints,
        }))
    }

    /// Serialize a `StreamExecutorCheckpoint` to JSON bytes.
    ///
    /// This is a standalone function that does not require `&self`, so it
    /// can be offloaded to `spawn_blocking` to avoid blocking the
    /// coordinator during serialization.
    pub fn serialize_checkpoint(
        cp: &crate::aggregate_state::StreamExecutorCheckpoint,
    ) -> Result<Vec<u8>, DbError> {
        serde_json::to_vec(&cp).map_err(|e| {
            DbError::Pipeline(format!("stream executor checkpoint serialization: {e}"))
        })
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
        let name_to_idx: FxHashMap<&str, usize> = self
            .queries
            .iter()
            .enumerate()
            .map(|(i, q)| (&*q.name, i))
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

        // Restore raw-batch EOWC states
        for (name, raw_cp) in &checkpoint.raw_eowc_states {
            if let Some(&idx) = name_to_idx.get(name.as_str()) {
                let mut accumulated_sources = FxHashMap::with_capacity_and_hasher(
                    raw_cp.sources.len(),
                    rustc_hash::FxBuildHasher,
                );
                let mut total_rows = 0usize;
                for (src_name, ipc_batches) in &raw_cp.sources {
                    let mut batches = Vec::with_capacity(ipc_batches.len());
                    for ipc_bytes in ipc_batches {
                        let batch =
                            laminar_core::serialization::deserialize_batch_stream(ipc_bytes)
                                .map_err(|e| {
                                    DbError::Pipeline(format!("EOWC batch deserialization: {e}"))
                                })?;
                        total_rows += batch.num_rows();
                        batches.push(batch);
                    }
                    accumulated_sources.insert(Arc::from(src_name.as_str()), batches);
                }
                self.eowc_states.insert(
                    idx,
                    EowcState {
                        accumulated_sources,
                        last_closed_boundary: raw_cp.last_closed_boundary,
                        accumulated_rows: total_rows,
                    },
                );
                restored += 1;
                tracing::info!(
                    query = %name,
                    rows = total_rows,
                    boundary = raw_cp.last_closed_boundary,
                    "Restored raw EOWC state from checkpoint"
                );
            }
        }

        // Store for deferred restore of lazily-initialized states
        self.pending_restore = Some(checkpoint);

        Ok(restored)
    }

    /// Try to restore checkpoint data for a newly initialized state.
    /// Logs success/failure with `label`.
    fn log_restore<C: Clone, S>(
        query_name: &str,
        cp: &C,
        state: &mut S,
        restore_fn: impl FnOnce(&mut S, &C) -> Result<usize, DbError>,
        label: &str,
    ) {
        let cp_clone = cp.clone();
        match restore_fn(state, &cp_clone) {
            Ok(n) => {
                tracing::info!(query = %query_name, groups = n, "Restored {label} from checkpoint");
            }
            Err(e) => {
                tracing::warn!(query = %query_name, error = %e, "Failed to restore {label} from checkpoint");
            }
        }
    }

    fn try_restore_pending_agg(&mut self, idx: usize) {
        let name = &self.queries[idx].name;
        let cp = self
            .pending_restore
            .as_ref()
            .and_then(|p| p.agg_states.get(&**name))
            .cloned();
        if let (Some(cp), Some(state)) = (cp, self.agg_states.get_mut(&idx)) {
            Self::log_restore(
                name,
                &cp,
                state,
                IncrementalAggState::restore_groups,
                "agg state",
            );
        }
    }

    fn try_restore_pending_eowc(&mut self, idx: usize) {
        let name = &self.queries[idx].name;
        let cp = self
            .pending_restore
            .as_ref()
            .and_then(|p| p.eowc_states.get(&**name))
            .cloned();
        if let (Some(cp), Some(state)) = (cp, self.eowc_agg_states.get_mut(&idx)) {
            Self::log_restore(
                name,
                &cp,
                state,
                IncrementalEowcState::restore_windows,
                "EOWC state",
            );
        }
    }

    /// Execute an interval join query for one cycle.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn execute_interval_join_query(
        &mut self,
        idx: usize,
        query_name: &str,
        config: &StreamJoinConfig,
        projection_sql: Option<&str>,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        intermediate_results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        current_watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        // Resolve left and right batches from source tables
        let left_batches = self
            .resolve_table_batches(&config.left_table, source_batches, intermediate_results)
            .await?;
        let right_batches = self
            .resolve_table_batches(&config.right_table, source_batches, intermediate_results)
            .await?;

        // Get or create interval join state
        if !self.interval_join_states.contains_key(&idx) {
            let mut new_state = crate::interval_join::IntervalJoinState::new();
            // Try restoring from checkpoint
            if let Some(ref pending) = self.pending_restore {
                if let Some(cp) = pending.join_states.get(&*self.queries[idx].name) {
                    match crate::interval_join::IntervalJoinState::from_checkpoint(
                        cp,
                        &config.left_key,
                        &config.left_time_column,
                        &config.right_key,
                        &config.right_time_column,
                    ) {
                        Ok(restored) => {
                            tracing::info!(
                                query = %query_name,
                                left_rows = cp.left_buffer_rows,
                                right_rows = cp.right_buffer_rows,
                                "Restored interval join state from checkpoint"
                            );
                            new_state = restored;
                        }
                        Err(e) => {
                            tracing::warn!(
                                query = %query_name,
                                error = %e,
                                "Failed to restore interval join state from checkpoint"
                            );
                        }
                    }
                }
            }
            self.interval_join_states.insert(idx, new_state);
        }

        let state = self
            .interval_join_states
            .get_mut(&idx)
            .expect("interval join state was just inserted");

        let join_result = crate::interval_join::execute_interval_join_cycle(
            state,
            &left_batches,
            &right_batches,
            config,
            current_watermark,
        )?;

        // Apply post-projection if needed
        if join_result.is_empty() {
            return Ok(join_result);
        }

        if let Some(proj_sql) = projection_sql {
            // Concatenate all result batches for projection
            let schema = join_result[0].schema();
            let joined = arrow::compute::concat_batches(&schema, &join_result)
                .map_err(|e| DbError::query_pipeline_arrow("interval join (concat)", &e))?;

            // Try compiled post-projection (lazy compile on first call)
            if let Some(compiled) = self.compiled_post_projections.get(&idx) {
                let result = Self::apply_compiled_post_projection(compiled, &joined)?;
                return Ok(vec![result]);
            }

            if !self.post_projection_compile_failed.contains(&idx) {
                let schema = joined.schema();
                if let Some(compiled) = self
                    .try_compile_post_projection(proj_sql, "__interval_tmp", &schema)
                    .await
                {
                    let result = Self::apply_compiled_post_projection(&compiled, &joined)?;
                    self.compiled_post_projections.insert(idx, compiled);
                    return Ok(vec![result]);
                }
                self.post_projection_compile_failed.insert(idx);
                tracing::warn!(
                    query = %query_name,
                    "Interval join post-projection could not be compiled — using DataFusion SQL fallback"
                );
            }

            // SQL fallback with plan caching
            let schema = joined.schema();
            let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![joined]])
                .map_err(|e| DbError::query_pipeline(query_name, &e))?;

            let _ = self.ctx.deregister_table("__interval_tmp");
            self.ctx
                .register_table("__interval_tmp", Arc::new(mem_table))
                .map_err(|e| DbError::query_pipeline(query_name, &e))?;

            let result = if let Some(plan) = self.cached_post_projection_plans.get(&idx) {
                self.execute_cached_plan(query_name, plan).await?
            } else {
                let df = self
                    .ctx
                    .sql(proj_sql)
                    .await
                    .map_err(|e| DbError::query_pipeline(query_name, &e))?;
                self.cached_post_projection_plans
                    .insert(idx, df.logical_plan().clone());
                df.collect()
                    .await
                    .map_err(|e| DbError::query_pipeline(query_name, &e))?
            };

            let _ = self.ctx.deregister_table("__interval_tmp");
            Ok(result)
        } else {
            Ok(join_result)
        }
    }

    fn try_restore_pending_core_window(&mut self, idx: usize) {
        let name = &self.queries[idx].name;
        let cp = self
            .pending_restore
            .as_ref()
            .and_then(|p| p.core_window_states.get(&**name))
            .cloned();
        if let (Some(cp), Some(state)) = (cp, self.core_window_states.get_mut(&idx)) {
            Self::log_restore(
                name,
                &cp,
                state,
                CoreWindowState::restore_windows,
                "core window state",
            );
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
pub(crate) fn compute_closed_boundary(watermark_ms: i64, config: &WindowOperatorConfig) -> i64 {
    match config.window_type {
        WindowType::Tumbling => {
            #[allow(clippy::cast_possible_truncation)]
            let size = config.size.as_millis() as i64;
            if size <= 0 {
                tracing::warn!("tumbling window size is zero or negative, EOWC filtering disabled");
                return watermark_ms;
            }
            // Floor to nearest window boundary, accounting for offset.
            // Must match TumblingWindowAssigner::assign() formula:
            //   floor((ts - offset) / size) * size + offset
            let offset = config.offset_ms;
            let adjusted = watermark_ms - offset;
            let floored = if adjusted >= 0 {
                (adjusted / size) * size
            } else {
                ((adjusted - size + 1) / size) * size
            };
            floored + offset
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
            // Account for window offset to match SlidingWindowAssigner.
            let offset = config.offset_ms;
            let base = (watermark_ms - offset).saturating_sub(size);
            let boundary = if base >= 0 {
                (base / slide).saturating_add(1).saturating_mul(slide)
            } else {
                ((base - slide + 1) / slide)
                    .saturating_add(1)
                    .saturating_mul(slide)
            };
            boundary + offset
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
            // Same offset-aware floor as Tumbling
            let offset = config.offset_ms;
            let adjusted = watermark_ms - offset;
            let floored = if adjusted >= 0 {
                (adjusted / size) * size
            } else {
                ((adjusted - size + 1) / size) * size
            };
            floored + offset
        }
    }
}

/// Infer the `TimestampFormat` from a `RecordBatch` column's `DataType`.
pub(crate) fn infer_ts_format_from_batch(
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

pub(crate) fn detect_asof_query(sql: &str) -> (Option<AsofJoinTranslatorConfig>, Option<String>) {
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

pub(crate) fn detect_temporal_query(
    sql: &str,
) -> (Option<TemporalJoinTranslatorConfig>, Option<String>) {
    let Ok(statements) = laminar_sql::parse_streaming_sql(sql) else {
        return (None, None);
    };

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

    let Some(temporal_analysis) = multi.joins.iter().find(|j| j.is_temporal_join) else {
        return (None, None);
    };

    let JoinOperatorConfig::Temporal(config) = JoinOperatorConfig::from_analysis(temporal_analysis)
    else {
        return (None, None);
    };

    let projection_sql = build_temporal_projection_sql(select, temporal_analysis, &config);

    (Some(config), Some(projection_sql))
}

pub(crate) fn detect_stream_join_query(sql: &str) -> (Option<StreamJoinConfig>, Option<String>) {
    let Ok(statements) = laminar_sql::parse_streaming_sql(sql) else {
        return (None, None);
    };

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

    // Find the first stream-stream join step (has time_bound, not ASOF/temporal/lookup)
    let Some(stream_analysis) = multi.joins.iter().find(|j| {
        j.time_bound.is_some() && !j.is_asof_join && !j.is_temporal_join && !j.is_lookup_join
    }) else {
        return (None, None);
    };

    let JoinOperatorConfig::StreamStream(config) =
        JoinOperatorConfig::from_analysis(stream_analysis)
    else {
        return (None, None);
    };

    // Only route to interval join if we have time columns
    if config.left_time_column.is_empty() || config.right_time_column.is_empty() {
        return (None, None);
    }

    // Only INNER JOIN is supported by the interval join engine. LEFT/RIGHT/FULL
    // would silently produce inner-join behavior (missing unmatched rows).
    if !matches!(config.join_type, StreamJoinType::Inner) {
        tracing::warn!(
            join_type = %config.join_type,
            "Stream-stream interval join only supports INNER JOIN. \
             {} JOIN would produce incorrect results. Query will use \
             DataFusion batch join instead.",
            config.join_type
        );
        return (None, None);
    }

    let projection_sql = build_stream_join_projection_sql(select, stream_analysis, &config);

    (Some(config), Some(projection_sql))
}

/// Build a `SELECT ... FROM __interval_tmp` projection query from the original
/// SELECT items, rewriting table-qualified references to plain column names.
fn build_stream_join_projection_sql(
    select: &sqlparser::ast::Select,
    analysis: &laminar_sql::parser::join_parser::JoinAnalysis,
    config: &StreamJoinConfig,
) -> String {
    let left_alias = analysis.left_alias.as_deref();
    let right_alias = analysis.right_alias.as_deref();

    let items: Vec<String> = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => {
                rewrite_stream_join_expr(expr, left_alias, right_alias, config)
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let rewritten = rewrite_stream_join_expr(expr, left_alias, right_alias, config);
                format!("{rewritten} AS {alias}")
            }
            SelectItem::Wildcard(_) => "*".to_string(),
            SelectItem::QualifiedWildcard(name, _) => {
                let table = name.to_string();
                if table == config.left_table
                    || left_alias.is_some_and(|a| a == table)
                    || table == config.right_table
                    || right_alias.is_some_and(|a| a == table)
                {
                    "*".to_string()
                } else {
                    format!("{table}.*")
                }
            }
        })
        .collect();

    let where_clause = select
        .selection
        .as_ref()
        .map(|expr| {
            let rewritten = rewrite_stream_join_expr(expr, left_alias, right_alias, config);
            format!(" WHERE {rewritten}")
        })
        .unwrap_or_default();

    format!(
        "SELECT {} FROM __interval_tmp{where_clause}",
        items.join(", ")
    )
}

/// Rewrite table-qualified column refs for the `__interval_tmp` schema.
fn rewrite_stream_join_expr(
    expr: &sqlparser::ast::Expr,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    config: &StreamJoinConfig,
) -> String {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let table = &parts[0].value;
            let col = &parts[1].value;
            let is_left = table == &config.left_table || left_alias.is_some_and(|a| a == table);
            let is_right = table == &config.right_table || right_alias.is_some_and(|a| a == table);
            if is_left || is_right {
                if is_right {
                    format!("{col}_{}", config.right_table)
                } else {
                    col.clone()
                }
            } else {
                expr.to_string()
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = rewrite_stream_join_expr(left, left_alias, right_alias, config);
            let r = rewrite_stream_join_expr(right, left_alias, right_alias, config);
            format!("{l} {op} {r}")
        }
        Expr::UnaryOp { op, expr: inner } => {
            let r = rewrite_stream_join_expr(inner, left_alias, right_alias, config);
            format!("{op} {r}")
        }
        Expr::Nested(inner) => {
            let r = rewrite_stream_join_expr(inner, left_alias, right_alias, config);
            format!("({r})")
        }
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let r = rewrite_stream_join_expr(inner, left_alias, right_alias, config);
            format!("CAST({r} AS {data_type})")
        }
        Expr::IsNull(inner) => {
            let r = rewrite_stream_join_expr(inner, left_alias, right_alias, config);
            format!("{r} IS NULL")
        }
        Expr::IsNotNull(inner) => {
            let r = rewrite_stream_join_expr(inner, left_alias, right_alias, config);
            format!("{r} IS NOT NULL")
        }
        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => {
            let e = rewrite_stream_join_expr(inner, left_alias, right_alias, config);
            let l = rewrite_stream_join_expr(low, left_alias, right_alias, config);
            let h = rewrite_stream_join_expr(high, left_alias, right_alias, config);
            if *negated {
                format!("{e} NOT BETWEEN {l} AND {h}")
            } else {
                format!("{e} BETWEEN {l} AND {h}")
            }
        }
        Expr::Function(func) => {
            let name = &func.name;
            let args_str = match &func.args {
                sqlparser::ast::FunctionArguments::List(arg_list) => {
                    let rewritten_args: Vec<String> = arg_list
                        .args
                        .iter()
                        .map(|arg| match arg {
                            sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(e),
                            ) => rewrite_stream_join_expr(e, left_alias, right_alias, config),
                            other => other.to_string(),
                        })
                        .collect();
                    rewritten_args.join(", ")
                }
                other => other.to_string(),
            };
            format!("{name}({args_str})")
        }
        _ => expr.to_string(),
    }
}

/// Build a `SELECT ... FROM __temporal_tmp` projection query from the original
/// SELECT items, rewriting table-qualified references to plain column names.
fn build_temporal_projection_sql(
    select: &sqlparser::ast::Select,
    analysis: &laminar_sql::parser::join_parser::JoinAnalysis,
    config: &TemporalJoinTranslatorConfig,
) -> String {
    let left_alias = analysis.left_alias.as_deref();
    let right_alias = analysis.right_alias.as_deref();

    let items: Vec<String> = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => {
                rewrite_temporal_expr(expr, left_alias, right_alias, config)
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let rewritten = rewrite_temporal_expr(expr, left_alias, right_alias, config);
                format!("{rewritten} AS {alias}")
            }
            SelectItem::Wildcard(_) => "*".to_string(),
            SelectItem::QualifiedWildcard(name, _) => {
                let table = name.to_string();
                if Some(table.as_str()) == left_alias || Some(table.as_str()) == right_alias {
                    "*".to_string()
                } else {
                    format!("{table}.*")
                }
            }
        })
        .collect();

    let select_clause = items.join(", ");

    let where_clause = select.selection.as_ref().map(|expr| {
        let rewritten = rewrite_temporal_expr(expr, left_alias, right_alias, config);
        format!(" WHERE {rewritten}")
    });

    format!(
        "SELECT {select_clause} FROM __temporal_tmp{}",
        where_clause.unwrap_or_default()
    )
}

/// Rewrite an expression tree to remove table qualifiers for temporal joins.
/// Stream-side columns keep their names. Table-side columns keep their names
/// (temporal join output is a flat concatenation of both sides).
fn rewrite_temporal_expr(
    expr: &Expr,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    config: &TemporalJoinTranslatorConfig,
) -> String {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let table = parts[0].value.as_str();
            let column = parts[1].value.as_str();

            let is_left = Some(table) == left_alias || table == config.stream_table;
            let is_right = Some(table) == right_alias || table == config.table_name;

            if is_left || is_right {
                column.to_string()
            } else {
                expr.to_string()
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = rewrite_temporal_expr(left, left_alias, right_alias, config);
            let r = rewrite_temporal_expr(right, left_alias, right_alias, config);
            format!("{l} {op} {r}")
        }
        Expr::UnaryOp { op, expr: inner } => {
            let e = rewrite_temporal_expr(inner, left_alias, right_alias, config);
            format!("{op} {e}")
        }
        Expr::Nested(inner) => {
            let e = rewrite_temporal_expr(inner, left_alias, right_alias, config);
            format!("({e})")
        }
        Expr::Function(func) => {
            let name = &func.name;
            let args: Vec<String> = match &func.args {
                sqlparser::ast::FunctionArguments::List(arg_list) => arg_list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => rewrite_temporal_expr(e, left_alias, right_alias, config),
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
            let e = rewrite_temporal_expr(inner, left_alias, right_alias, config);
            format!("CAST({e} AS {data_type})")
        }
        _ => expr.to_string(),
    }
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
pub(crate) fn apply_topk_filter(batches: &[RecordBatch], k: usize) -> Vec<RecordBatch> {
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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("events"), vec![test_batch()]);

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

        let source_batches = FxHashMap::default();
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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("events"), vec![test_batch()]);

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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("events"), vec![test_batch()]);

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
        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("events"), vec![test_batch()]);
        let r1 = executor
            .execute_cycle(&source_batches, i64::MAX)
            .await
            .unwrap();
        assert!(r1.contains_key("pass"));

        // Second cycle with no data — compiled projection returns empty results
        // (no SQL execution needed, so no table-not-found error)
        let empty = FxHashMap::default();
        let r2 = executor.execute_cycle(&empty, i64::MAX).await.unwrap();
        let pass_rows: usize = r2
            .get("pass")
            .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum());
        assert_eq!(pass_rows, 0, "no source data should produce no output");
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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("events"), vec![test_batch()]);

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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("events"), vec![test_batch()]);

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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("events"), vec![test_batch()]);

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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("events"), vec![test_batch()]);

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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("events"), vec![test_batch()]);

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
        let source_batches = FxHashMap::default();
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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("events"), vec![test_batch()]);

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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("events"), vec![test_batch()]);

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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("trades"), vec![trades_batch_for_asof()]);
        source_batches.insert(Arc::from("quotes"), vec![quotes_batch_for_asof()]);

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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("trades"), vec![trades_batch_for_asof()]);
        source_batches.insert(Arc::from("quotes"), vec![quotes_batch_for_asof()]);

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
        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("trades"), vec![trades_batch_for_asof()]);
        source_batches.insert(
            Arc::from("quotes"),
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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("trades"), vec![trades_batch_for_asof()]);
        source_batches.insert(Arc::from("quotes"), vec![quotes_batch_for_asof()]);

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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("events"), vec![test_batch()]);

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
            offset_ms: 0,
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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(
            Arc::from("trades"),
            vec![eowc_batch(vec!["AAPL"], vec![150.0], vec![500])],
        );

        // Watermark at 500 → window [0, 1000) not closed yet
        let results = executor.execute_cycle(&source_batches, 500).await.unwrap();
        assert!(
            !results.contains_key("avg_price"),
            "EOWC should suppress output before window closes"
        );

        // Advance watermark to 1000 → window [0, 1000) is now closed
        let empty: FxHashMap<Arc<str>, Vec<RecordBatch>> = FxHashMap::default();
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
        let mut batches1 = FxHashMap::default();
        batches1.insert(
            Arc::from("trades"),
            vec![eowc_batch(vec!["AAPL"], vec![100.0], vec![100])],
        );
        let r1 = executor.execute_cycle(&batches1, 200).await.unwrap();
        assert!(!r1.contains_key("total"), "window not closed at wm=200");

        // Cycle 2: push more data at ts=400
        let mut batches2 = FxHashMap::default();
        batches2.insert(
            Arc::from("trades"),
            vec![eowc_batch(vec!["AAPL"], vec![200.0], vec![400])],
        );
        let r2 = executor.execute_cycle(&batches2, 600).await.unwrap();
        assert!(!r2.contains_key("total"), "window not closed at wm=600");

        // Cycle 3: watermark crosses 1000 → emit accumulated
        let empty: FxHashMap<Arc<str>, Vec<RecordBatch>> = FxHashMap::default();
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
        let mut batches = FxHashMap::default();
        batches.insert(
            Arc::from("trades"),
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
        let empty: FxHashMap<Arc<str>, Vec<RecordBatch>> = FxHashMap::default();
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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(
            Arc::from("trades"),
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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(
            Arc::from("trades"),
            vec![eowc_batch(vec!["AAPL"], vec![150.0], vec![500])],
        );

        // Non-EOWC emits, EOWC suppresses
        let results = executor.execute_cycle(&source_batches, 500).await.unwrap();
        assert!(results.contains_key("realtime"), "non-EOWC should emit");
        assert!(!results.contains_key("windowed"), "EOWC should suppress");

        // Advance watermark → EOWC now emits
        // Provide an empty batch so the "trades" table is still registered
        let mut empty_source = FxHashMap::default();
        empty_source.insert(
            Arc::from("trades"),
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
            offset_ms: 0,
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
            offset_ms: 0,
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
            offset_ms: 0,
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

    #[test]
    fn test_compute_closed_boundary_tumbling_with_offset() {
        use laminar_sql::parser::EmitStrategy;

        // 1-hour window with UTC+8 offset (28_800_000 ms)
        let config = WindowOperatorConfig {
            window_type: WindowType::Tumbling,
            time_column: "ts".to_string(),
            size: std::time::Duration::from_millis(3_600_000),
            slide: None,
            gap: None,
            offset_ms: 28_800_000, // 8 hours
            allowed_lateness: std::time::Duration::ZERO,
            emit_strategy: EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        // Window boundaries: ..., [28800000, 32400000), [32400000, 36000000), ...
        // watermark=32000000 is inside [28800000, 32400000) → boundary=28800000
        assert_eq!(compute_closed_boundary(32_000_000, &config), 28_800_000);
        // watermark=32400000 is at window boundary → boundary=32400000
        assert_eq!(compute_closed_boundary(32_400_000, &config), 32_400_000);
    }

    #[test]
    fn test_compute_closed_boundary_sliding_with_offset() {
        use laminar_sql::parser::EmitStrategy;

        let config = WindowOperatorConfig {
            window_type: WindowType::Sliding,
            time_column: "ts".to_string(),
            size: std::time::Duration::from_millis(2000),
            slide: Some(std::time::Duration::from_millis(500)),
            gap: None,
            offset_ms: 200,
            allowed_lateness: std::time::Duration::ZERO,
            emit_strategy: EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        // With offset=200, slide boundaries are at 200, 700, 1200, 1700, 2200, ...
        // At watermark=2200: adjusted=2000, base=2000-2000=0, boundary=(0/500+1)*500=500
        //   result=500+200=700
        assert_eq!(compute_closed_boundary(2200, &config), 700);
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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("events"), vec![test_batch()]);

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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("events"), vec![test_batch()]); // 3 rows

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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("trades"), vec![mixed_case_batch()]);

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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("trades"), vec![mixed_case_batch()]);

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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("trades"), vec![mixed_case_batch()]);

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

        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("trades"), vec![mixed_case_batch()]);

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
        let mut batches1 = FxHashMap::default();
        batches1.insert(
            Arc::from("trades"),
            vec![eowc_batch(vec!["AAPL"], vec![100.0], vec![500])],
        );
        let r1 = executor.execute_cycle(&batches1, 500).await.unwrap();
        assert!(!r1.contains_key("total"), "window not closed at wm=500");

        // Cycle 2: push more data, watermark STAYS at 500 (hasn't
        // advanced). Even if memory were over the threshold, should
        // NOT emit open-window rows.
        let mut batches2 = FxHashMap::default();
        batches2.insert(
            Arc::from("trades"),
            vec![eowc_batch(vec!["AAPL"], vec![200.0], vec![600])],
        );
        let r2 = executor.execute_cycle(&batches2, 500).await.unwrap();
        assert!(
            !r2.contains_key("total"),
            "watermark unchanged at 500 — should NOT emit"
        );

        // Cycle 3: watermark advances to 1000 → window closes
        let empty: FxHashMap<Arc<str>, Vec<RecordBatch>> = FxHashMap::default();
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

    #[tokio::test]
    async fn test_state_size_limit_fails_query() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);
        executor.set_max_state_bytes(Some(100)); // very low limit

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]));
        executor.register_source_schema("t".to_string(), schema.clone());
        executor.add_query(
            "agg".to_string(),
            "SELECT key, SUM(val) FROM t GROUP BY key".to_string(),
            None,
            None,
            None,
        );

        // Push enough distinct groups to exceed the limit
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    (0..200).map(|i| format!("key_{i}")).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from((0..200).collect::<Vec<i64>>())),
            ],
        )
        .unwrap();
        let mut source = FxHashMap::default();
        source.insert(Arc::from("t"), vec![batch]);

        let result = executor.execute_cycle(&source, i64::MIN).await;
        assert!(result.is_err(), "should fail when state limit exceeded");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("state size limit exceeded"), "error: {err}");
    }

    #[tokio::test]
    async fn test_state_size_no_limit_succeeds() {
        // Default (no limit) should succeed even with many groups
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);
        // max_state_bytes defaults to None

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]));
        executor.register_source_schema("t".to_string(), schema.clone());
        executor.add_query(
            "agg".to_string(),
            "SELECT key, SUM(val) FROM t GROUP BY key".to_string(),
            None,
            None,
            None,
        );

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    (0..200).map(|i| format!("key_{i}")).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from((0..200).collect::<Vec<i64>>())),
            ],
        )
        .unwrap();
        let mut source = FxHashMap::default();
        source.insert(Arc::from("t"), vec![batch]);

        let result = executor.execute_cycle(&source, i64::MIN).await;
        assert!(result.is_ok(), "should succeed without limit");
    }

    // ── Interval Join Integration Tests ───────────────────────────────────

    fn orders_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]))
    }

    fn payments_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("paid", DataType::Float64, false),
        ]))
    }

    fn make_orders(ids: &[&str], timestamps: &[i64], amounts: &[f64]) -> RecordBatch {
        RecordBatch::try_new(
            orders_schema(),
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(amounts.to_vec())),
            ],
        )
        .unwrap()
    }

    fn make_payments(ids: &[&str], timestamps: &[i64], amounts: &[f64]) -> RecordBatch {
        RecordBatch::try_new(
            payments_schema(),
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(amounts.to_vec())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_interval_join_cross_cycle_via_sql() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "joined".to_string(),
            "SELECT o.order_id, o.amount, p.paid \
             FROM orders o \
             JOIN payments p ON o.order_id = p.order_id \
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR"
                .to_string(),
            None,
            None,
            None,
        );

        // Verify interval join was detected
        assert!(
            executor.queries[0].stream_join_config.is_some(),
            "Should detect interval join"
        );

        // Cycle 1: only orders
        let mut sources = FxHashMap::default();
        sources.insert(
            Arc::from("orders"),
            vec![make_orders(&["A"], &[1000], &[100.0])],
        );
        sources.insert(Arc::from("payments"), vec![]);

        let r1 = executor.execute_cycle(&sources, 0).await.unwrap();
        let c1_rows: usize = r1
            .get("joined")
            .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum());
        assert_eq!(c1_rows, 0, "No payments yet, no matches");

        // Cycle 2: payment arrives within time bound
        sources.clear();
        sources.insert(Arc::from("orders"), vec![]);
        sources.insert(
            Arc::from("payments"),
            vec![make_payments(&["A"], &[2000], &[100.0])],
        );

        let r2 = executor.execute_cycle(&sources, 0).await.unwrap();
        let c2_rows: usize = r2
            .get("joined")
            .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum());
        assert_eq!(c2_rows, 1, "Cross-cycle match: order@1000 ~ payment@2000");
    }

    #[tokio::test]
    async fn test_interval_join_null_keys_via_sql() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "joined".to_string(),
            "SELECT o.order_id, o.amount, p.paid \
             FROM orders o \
             JOIN payments p ON o.order_id = p.order_id \
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR"
                .to_string(),
            None,
            None,
            None,
        );

        // Both sides include null keys
        let orders_schema_nullable = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        let orders = RecordBatch::try_new(
            orders_schema_nullable,
            vec![
                Arc::new(StringArray::from(vec![Some("A"), None])),
                Arc::new(Int64Array::from(vec![1000, 1000])),
                Arc::new(Float64Array::from(vec![100.0, 200.0])),
            ],
        )
        .unwrap();

        let payments_schema_nullable = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, false),
            Field::new("paid", DataType::Float64, false),
        ]));
        let payments = RecordBatch::try_new(
            payments_schema_nullable,
            vec![
                Arc::new(StringArray::from(vec![Some("A"), None])),
                Arc::new(Int64Array::from(vec![1500, 1500])),
                Arc::new(Float64Array::from(vec![100.0, 200.0])),
            ],
        )
        .unwrap();

        let mut sources = FxHashMap::default();
        sources.insert(Arc::from("orders"), vec![orders]);
        sources.insert(Arc::from("payments"), vec![payments]);

        let results = executor.execute_cycle(&sources, 0).await.unwrap();
        let rows: usize = results
            .get("joined")
            .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum());
        // Only "A" matches "A"; null keys produce no matches
        assert_eq!(rows, 1, "Null keys should not match");
    }

    #[test]
    fn test_left_join_not_routed_to_interval() {
        // LEFT JOIN with BETWEEN should NOT route to interval join engine
        let (config, _) = detect_stream_join_query(
            "SELECT * FROM orders o \
             LEFT JOIN payments p ON o.order_id = p.order_id \
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR",
        );
        assert!(
            config.is_none(),
            "LEFT JOIN should not route to interval join engine"
        );
    }

    #[tokio::test]
    async fn test_interval_join_checkpoint_roundtrip() {
        let ctx = create_session_context();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "joined".to_string(),
            "SELECT o.order_id, o.amount, p.paid \
             FROM orders o \
             JOIN payments p ON o.order_id = p.order_id \
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR"
                .to_string(),
            None,
            None,
            None,
        );

        // Cycle 1: buffer orders
        let mut sources = FxHashMap::default();
        sources.insert(
            Arc::from("orders"),
            vec![make_orders(&["A"], &[1000], &[100.0])],
        );
        sources.insert(Arc::from("payments"), vec![]);
        let _ = executor.execute_cycle(&sources, 0).await.unwrap();

        // Checkpoint
        let checkpoint = executor.snapshot_state().unwrap();
        assert!(checkpoint.is_some(), "Should have join state to checkpoint");
        let cp = checkpoint.unwrap();
        assert!(
            !cp.join_states.is_empty(),
            "Should have interval join state"
        );

        // Serialize then restore into new executor
        let cp_bytes = serde_json::to_vec(&cp).unwrap();
        let ctx2 = create_session_context();
        register_streaming_functions(&ctx2);
        let mut executor2 = StreamExecutor::new(ctx2);
        executor2.add_query(
            "joined".to_string(),
            "SELECT o.order_id, o.amount, p.paid \
             FROM orders o \
             JOIN payments p ON o.order_id = p.order_id \
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR"
                .to_string(),
            None,
            None,
            None,
        );
        executor2.restore_state(&cp_bytes).unwrap();

        // Cycle 2 on restored executor: payment matches buffered order
        sources.clear();
        sources.insert(Arc::from("orders"), vec![]);
        sources.insert(
            Arc::from("payments"),
            vec![make_payments(&["A"], &[2000], &[100.0])],
        );
        let results = executor2.execute_cycle(&sources, 0).await.unwrap();
        let rows: usize = results
            .get("joined")
            .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum());
        assert_eq!(rows, 1, "Restored state should match cross-cycle");
    }
}
