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
use datafusion::prelude::SessionContext;
use sqlparser::ast::{
    Expr, SelectItem, SetExpr, Statement, TableFactor, WildcardAdditionalOptions,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use laminar_sql::parser::join_parser::analyze_joins;
use laminar_sql::translator::{AsofJoinTranslatorConfig, JoinOperatorConfig};

use crate::error::DbError;

/// Extract all table names referenced in FROM/JOIN clauses of a SQL query.
///
/// Parses the SQL and walks the AST to find `TableFactor::Table` references,
/// recursing into subqueries, nested joins, and set operations (UNION, etc.).
fn extract_table_references(sql: &str) -> HashSet<String> {
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
        }
    }

    /// Register a stream query for execution.
    ///
    /// If the query contains an ASOF JOIN, it is detected at registration time
    /// and routed to a custom execution path in `execute_cycle()` (since
    /// `DataFusion` cannot parse ASOF syntax).
    pub fn add_query(&mut self, name: String, sql: String) {
        let (asof_config, projection_sql) = detect_asof_query(&sql);
        self.queries.push(StreamQuery {
            name,
            sql,
            asof_config,
            projection_sql,
        });
        self.topo_dirty = true;
    }

    /// Register a static reference table (e.g., from `CREATE TABLE`).
    ///
    /// Unlike source tables, these persist across cycles.
    #[allow(dead_code)] // Public API for Phase 3 CREATE TABLE support
    pub fn register_table(&self, name: &str, batch: RecordBatch) -> Result<(), DbError> {
        let schema = batch.schema();
        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| DbError::Pipeline(format!("Failed to create table '{name}': {e}")))?;

        self.ctx
            .register_table(name, Arc::new(mem_table))
            .map_err(|e| DbError::Pipeline(format!("Failed to register table '{name}': {e}")))?;
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
            let refs = extract_table_references(&query.sql);
            for table_ref in &refs {
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
    pub async fn execute_cycle(
        &mut self,
        source_batches: &HashMap<String, Vec<RecordBatch>>,
    ) -> Result<HashMap<String, Vec<RecordBatch>>, DbError> {
        // 1. Recompute topological order if queries changed
        if self.topo_dirty {
            self.compute_topo_order();
        }

        // 2. Register source data as temporary MemTables
        self.register_source_tables(source_batches)?;

        // 3. Execute queries in dependency order, registering intermediate results
        let mut results = HashMap::new();
        let mut intermediate_tables: Vec<String> = Vec::new();

        for &idx in &self.topo_order.clone() {
            let query = &self.queries[idx];
            let query_name = query.name.clone();
            let query_sql = query.sql.clone();
            let asof_config = query.asof_config.clone();
            let projection_sql = query.projection_sql.clone();

            let batches = if let Some(ref cfg) = asof_config {
                self.execute_asof_query(
                    &query_name,
                    cfg,
                    projection_sql.as_deref(),
                    source_batches,
                    &results,
                )
                .await?
            } else {
                let df = self.ctx.sql(&query_sql).await.map_err(|e| {
                    DbError::Pipeline(format!("Stream '{query_name}' planning failed: {e}"))
                })?;
                df.collect().await.map_err(|e| {
                    DbError::Pipeline(format!("Stream '{query_name}' execution failed: {e}"))
                })?
            };

            if !batches.is_empty() {
                // Register results as a temp MemTable for downstream queries
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

        // 4. Cleanup temporary source tables AND intermediate tables
        self.cleanup_source_tables();
        for name in &intermediate_tables {
            let _ = self.ctx.deregister_table(name);
        }

        Ok(results)
    }

    /// Register source batches as temporary `MemTable` providers.
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
                datafusion::datasource::MemTable::try_new(schema, vec![batches.clone()]).map_err(
                    |e| DbError::Pipeline(format!("Failed to create temp table '{name}': {e}")),
                )?;

            // Deregister first if it exists (from previous cycle)
            let _ = self.ctx.deregister_table(name);

            self.ctx
                .register_table(name, Arc::new(mem_table))
                .map_err(|e| {
                    DbError::Pipeline(format!("Failed to register temp table '{name}': {e}"))
                })?;

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

    /// Get the number of registered queries.
    #[allow(dead_code)] // Public API for admin/observability queries
    pub fn query_count(&self) -> usize {
        self.queries.len()
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
                .map_err(|e| {
                DbError::Pipeline(format!("Stream '{query_name}' ASOF temp table failed: {e}"))
            })?;

            let _ = self.ctx.deregister_table("__asof_tmp");
            self.ctx
                .register_table("__asof_tmp", Arc::new(mem_table))
                .map_err(|e| {
                    DbError::Pipeline(format!(
                        "Stream '{query_name}' ASOF temp registration failed: {e}"
                    ))
                })?;

            let df = self.ctx.sql(proj_sql).await.map_err(|e| {
                DbError::Pipeline(format!("Stream '{query_name}' ASOF projection failed: {e}"))
            })?;
            let result = df.collect().await.map_err(|e| {
                DbError::Pipeline(format!(
                    "Stream '{query_name}' ASOF projection execution failed: {e}"
                ))
            })?;

            let _ = self.ctx.deregister_table("__asof_tmp");
            Ok(result)
        } else {
            Ok(vec![joined])
        }
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
        let df =
            self.ctx.sql(&sql).await.map_err(|e| {
                DbError::Pipeline(format!("ASOF table '{table_name}' not found: {e}"))
            })?;
        df.collect()
            .await
            .map_err(|e| DbError::Pipeline(format!("ASOF table '{table_name}' query failed: {e}")))
    }
}

/// Detect whether a SQL query contains an ASOF JOIN and, if so, extract the
/// `AsofJoinTranslatorConfig` and build a projection SQL string.
///
/// Returns `(None, None)` for non-ASOF queries.
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

#[cfg(test)]
#[allow(clippy::redundant_closure_for_method_calls)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
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
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "test_stream".to_string(),
            "SELECT name, SUM(value) as total FROM events GROUP BY name".to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();
        assert!(results.contains_key("test_stream"));
        let batches = &results["test_stream"];
        assert!(!batches.is_empty());
        // Each name should have one row
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_executor_empty_source() {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "test_stream".to_string(),
            "SELECT * FROM events".to_string(),
        );

        let source_batches = HashMap::new();
        // When no source data is registered, the query references a missing table —
        // this should surface as an error, not silently produce empty results.
        let result = executor.execute_cycle(&source_batches).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("test_stream"),
            "error should name the stream: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_executor_multiple_queries() {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "count_stream".to_string(),
            "SELECT COUNT(*) as cnt FROM events".to_string(),
        );
        executor.add_query(
            "sum_stream".to_string(),
            "SELECT SUM(value) as total FROM events".to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();
        assert!(results.contains_key("count_stream"));
        assert!(results.contains_key("sum_stream"));
    }

    #[tokio::test]
    async fn test_executor_with_filter() {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "filtered".to_string(),
            "SELECT * FROM events WHERE value > 1.5".to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();
        assert!(results.contains_key("filtered"));
        let total_rows: usize = results["filtered"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // value 2.0 and 3.0
    }

    #[tokio::test]
    async fn test_executor_cleanup_between_cycles() {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query("pass".to_string(), "SELECT * FROM events".to_string());

        // First cycle
        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);
        let r1 = executor.execute_cycle(&source_batches).await.unwrap();
        assert!(r1.contains_key("pass"));

        // Second cycle with no data — table was cleaned up, so query fails
        let empty = HashMap::new();
        let r2 = executor.execute_cycle(&empty).await;
        assert!(
            r2.is_err(),
            "query referencing cleaned-up table should fail"
        );
    }

    #[tokio::test]
    async fn test_executor_register_table() {
        let ctx = SessionContext::new();
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
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();
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
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // level1: aggregate events
        executor.add_query(
            "level1".to_string(),
            "SELECT name, SUM(value) as total FROM events GROUP BY name".to_string(),
        );
        // level2: filter level1 results
        executor.add_query(
            "level2".to_string(),
            "SELECT name, total FROM level1 WHERE total > 1.0".to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();

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
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // level1: pass through
        executor.add_query(
            "level1".to_string(),
            "SELECT name, value FROM events".to_string(),
        );
        // level2: filter
        executor.add_query(
            "level2".to_string(),
            "SELECT name, value FROM level1 WHERE value >= 2.0".to_string(),
        );
        // level3: aggregate
        executor.add_query(
            "level3".to_string(),
            "SELECT COUNT(*) as cnt FROM level2".to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();

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
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // Two queries from the same source
        executor.add_query(
            "agg".to_string(),
            "SELECT name, SUM(value) as total FROM events GROUP BY name".to_string(),
        );
        executor.add_query(
            "filtered".to_string(),
            "SELECT name, value FROM events WHERE value > 1.5".to_string(),
        );
        // Third query joins results of both
        executor.add_query(
            "combined".to_string(),
            "SELECT a.name, a.total, f.value FROM agg a JOIN filtered f ON a.name = f.name"
                .to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();

        assert!(results.contains_key("agg"));
        assert!(results.contains_key("filtered"));
        assert!(results.contains_key("combined"));

        // combined: should have rows for names b and c (those in filtered)
        let combined_rows: usize = results["combined"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(combined_rows, 2);
    }

    #[tokio::test]
    async fn test_topo_order_ignores_insertion_order() {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // Register downstream BEFORE upstream
        executor.add_query(
            "downstream".to_string(),
            "SELECT name, total FROM upstream WHERE total > 1.0".to_string(),
        );
        executor.add_query(
            "upstream".to_string(),
            "SELECT name, SUM(value) as total FROM events GROUP BY name".to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();

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
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "level1".to_string(),
            "SELECT name, value FROM events".to_string(),
        );
        executor.add_query("level2".to_string(), "SELECT name FROM level1".to_string());

        // No source data — first query references missing table, so cycle fails
        let source_batches = HashMap::new();
        let result = executor.execute_cycle(&source_batches).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_cycle_propagates_planning_error() {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // Register a query with invalid SQL that will fail planning
        executor.add_query(
            "bad_query".to_string(),
            "SELECTTTT * FROMM nowhere".to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let result = executor.execute_cycle(&source_batches).await;
        assert!(result.is_err(), "invalid SQL should surface as error");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("bad_query"),
            "error should name the stream: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_execute_cycle_propagates_execution_error() {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // Query references a column that doesn't exist in the source schema
        executor.add_query(
            "missing_col".to_string(),
            "SELECT nonexistent_column FROM events".to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let result = executor.execute_cycle(&source_batches).await;
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
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "enriched".to_string(),
            "SELECT t.symbol, t.price, q.bid, q.ask \
             FROM trades t ASOF JOIN quotes q \
             MATCH_CONDITION(t.ts >= q.ts) ON t.symbol = q.symbol"
                .to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("trades".to_string(), vec![trades_batch_for_asof()]);
        source_batches.insert("quotes".to_string(), vec![quotes_batch_for_asof()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();
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
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "spread_stream".to_string(),
            "SELECT t.symbol, t.price AS trade_price, q.bid, t.price - q.bid AS spread \
             FROM trades t ASOF JOIN quotes q \
             MATCH_CONDITION(t.ts >= q.ts) ON t.symbol = q.symbol"
                .to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("trades".to_string(), vec![trades_batch_for_asof()]);
        source_batches.insert("quotes".to_string(), vec![quotes_batch_for_asof()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();
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
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "enriched".to_string(),
            "SELECT t.symbol, q.bid \
             FROM trades t ASOF JOIN quotes q \
             MATCH_CONDITION(t.ts >= q.ts) ON t.symbol = q.symbol"
                .to_string(),
        );

        // Only trades, no quotes → should still work (left join with nulls)
        let mut source_batches = HashMap::new();
        source_batches.insert("trades".to_string(), vec![trades_batch_for_asof()]);
        source_batches.insert(
            "quotes".to_string(),
            vec![RecordBatch::new_empty(quotes_schema())],
        );

        let results = executor.execute_cycle(&source_batches).await.unwrap();
        // ASOF join with empty right produces 3 rows with null bids
        assert!(results.contains_key("enriched"));
    }

    #[tokio::test]
    async fn test_asof_in_cascade() {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // ASOF join produces enriched results
        executor.add_query(
            "enriched".to_string(),
            "SELECT t.symbol, t.price, q.bid \
             FROM trades t ASOF JOIN quotes q \
             MATCH_CONDITION(t.ts >= q.ts) ON t.symbol = q.symbol"
                .to_string(),
        );

        // Downstream query filters ASOF results
        executor.add_query(
            "filtered".to_string(),
            "SELECT symbol, price, bid FROM enriched WHERE price > 151.0".to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("trades".to_string(), vec![trades_batch_for_asof()]);
        source_batches.insert("quotes".to_string(), vec![quotes_batch_for_asof()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();
        assert!(results.contains_key("enriched"));
        assert!(results.contains_key("filtered"));

        // filtered: only trades with price > 151.0 → AAPL@152.0 and GOOG@2800.0
        let filtered_rows: usize = results["filtered"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(filtered_rows, 2);
    }

    #[tokio::test]
    async fn test_non_asof_queries_unaffected() {
        // Verify that non-ASOF queries still work exactly as before
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "simple".to_string(),
            "SELECT name, SUM(value) as total FROM events GROUP BY name".to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();
        assert!(results.contains_key("simple"));
        let total_rows: usize = results["simple"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }
}
