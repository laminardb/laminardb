//! Operator implementations for the `OperatorGraph`.
//!
//! Each module contains a `GraphOperator` implementation that wraps an existing
//! state type from `StreamExecutor`'s decomposed state maps.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::prelude::SessionContext;

use crate::error::DbError;
use crate::sql_analysis::{extract_projection_exprs, CompiledPostProjection};

/// Execute a pre-built physical plan. The plan's source leaves are
/// `LiveSourceExec` (registered once at startup), so re-executing reads
/// fresh per-cycle data without re-running the analyzer/optimizer/physical
/// planner. All EOWC and multi-source operator paths go through this.
pub(crate) async fn execute_cached_physical(
    ctx: &SessionContext,
    op_name: &str,
    physical: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> Result<Vec<RecordBatch>, DbError> {
    let task_ctx = ctx.task_ctx();
    datafusion::physical_plan::collect(Arc::clone(physical), task_ctx)
        .await
        .map_err(|e| DbError::query_pipeline(op_name, &e))
}

/// Pre-built physical plan whose only source leaf is a
/// `LiveSourceProvider` — callers swap fresh batches in via the handle
/// and re-execute without re-planning. Underpins HAVING-SQL fallback
/// (see [`HavingSqlCache`]) and raw-EOWC close-cycle execution.
pub(crate) struct LiveSqlCache {
    handle: laminar_sql::datafusion::LiveSourceHandle,
    physical: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
}

impl LiveSqlCache {
    /// Register a `LiveSourceProvider` under `table_name`, plan `sql`
    /// against it, and cache the physical plan. `what` is used in error
    /// messages to identify the caller (`"HAVING"`, `"raw EOWC"`, ...).
    pub(crate) async fn build(
        ctx: &SessionContext,
        table_name: &str,
        schema: SchemaRef,
        sql: &str,
        what: &str,
    ) -> Result<Self, DbError> {
        use laminar_sql::datafusion::LiveSourceProvider;
        let provider = Arc::new(LiveSourceProvider::new(schema));
        let handle = provider.handle();
        let _ = ctx.deregister_table(table_name);
        ctx.register_table(table_name, provider)
            .map_err(|e| DbError::Pipeline(format!("{what} register_table: {e}")))?;
        let logical = ctx
            .sql(sql)
            .await
            .map_err(|e| DbError::Pipeline(format!("{what} plan: {e}")))?
            .logical_plan()
            .clone();
        let physical = ctx
            .state()
            .create_physical_plan(&logical)
            .await
            .map_err(|e| DbError::Pipeline(format!("{what} physical: {e}")))?;
        Ok(Self { handle, physical })
    }

    pub(crate) async fn apply(
        &self,
        ctx: &SessionContext,
        op_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.handle.swap(batches);
        execute_cached_physical(ctx, op_name, &self.physical).await
    }
}

/// HAVING-SQL fallback: a [`LiveSqlCache`] over `SELECT cols FROM
/// table WHERE having_sql`. Used by both `EowcQueryOperator` and
/// `SqlQueryOperator` when the HAVING predicate can't compile to a
/// `PhysicalExpr`.
pub(crate) struct HavingSqlCache(LiveSqlCache);

impl HavingSqlCache {
    pub(crate) async fn build(
        ctx: &SessionContext,
        table_name: &str,
        schema: SchemaRef,
        having_sql: &str,
    ) -> Result<Self, DbError> {
        let col_list = schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {col_list} FROM \"{table_name}\" WHERE {having_sql}");
        LiveSqlCache::build(ctx, table_name, schema, &sql, "HAVING")
            .await
            .map(Self)
    }

    pub(crate) async fn apply(
        &self,
        ctx: &SessionContext,
        op_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.0.apply(ctx, op_name, batches).await
    }
}

pub(crate) mod asof_join;
pub(crate) mod eowc_query;
pub(crate) mod interval_join;
pub(crate) mod sql_query;
pub(crate) mod temporal_filter;
pub(crate) mod temporal_join;
pub(crate) mod temporal_probe_join;

pub(crate) async fn try_compile_post_projection(
    ctx: &SessionContext,
    proj_sql: &str,
    tmp_table_name: &str,
    batch_schema: &SchemaRef,
) -> Option<CompiledPostProjection> {
    let empty =
        datafusion::datasource::MemTable::try_new(batch_schema.clone(), vec![vec![]]).ok()?;
    let _ = ctx.deregister_table(tmp_table_name);
    ctx.register_table(tmp_table_name, Arc::new(empty)).ok()?;

    let df = ctx.sql(proj_sql).await.ok()?;
    let plan = df.logical_plan().clone();
    let _ = ctx.deregister_table(tmp_table_name);

    let (exprs, output_schema) = extract_projection_exprs(&plan, batch_schema, ctx)?;
    Some(CompiledPostProjection {
        exprs,
        output_schema,
    })
}

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

/// Post-projection state shared by the four join operators.
pub(crate) struct ProjectingJoinState {
    pub(crate) op_name: Arc<str>,
    ctx: SessionContext,
    projection_sql: Option<Arc<str>>,
    tmp_table_name: &'static str,
    compiled: Option<CompiledPostProjection>,
    compile_failed: bool,
}

impl ProjectingJoinState {
    pub(crate) fn new(
        op_name: &str,
        ctx: SessionContext,
        projection_sql: Option<Arc<str>>,
        tmp_table_name: &'static str,
    ) -> Self {
        Self {
            op_name: Arc::from(op_name),
            ctx,
            projection_sql,
            tmp_table_name,
            compiled: None,
            compile_failed: false,
        }
    }

    pub(crate) async fn apply(
        &mut self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        apply_post_projection(
            &self.ctx,
            &self.op_name,
            self.tmp_table_name,
            self.projection_sql.as_deref(),
            &mut self.compiled,
            &mut self.compile_failed,
            batches,
        )
        .await
    }
}

pub(crate) async fn apply_post_projection(
    ctx: &SessionContext,
    op_name: &str,
    tmp_table_name: &str,
    proj_sql: Option<&str>,
    compiled: &mut Option<CompiledPostProjection>,
    compile_failed: &mut bool,
    batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, DbError> {
    let Some(proj_sql) = proj_sql else {
        return Ok(batches);
    };

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(Vec::new());
    }

    // Try compiled path
    if compiled.is_none() && !*compile_failed {
        let schema = batches[0].schema();
        match try_compile_post_projection(ctx, proj_sql, tmp_table_name, &schema).await {
            Some(c) => *compiled = Some(c),
            None => *compile_failed = true,
        }
    }

    if let Some(ref proj) = compiled {
        let mut result = Vec::with_capacity(batches.len());
        for batch in &batches {
            let projected = apply_compiled_post_projection(proj, batch)?;
            if projected.num_rows() > 0 {
                result.push(projected);
            }
        }
        return Ok(result);
    }

    // SQL fallback
    let schema = batches[0].schema();
    let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![batches])
        .map_err(|e| DbError::query_pipeline(op_name, &e))?;

    let _ = ctx.deregister_table(tmp_table_name);
    ctx.register_table(tmp_table_name, Arc::new(mem_table))
        .map_err(|e| DbError::query_pipeline(op_name, &e))?;

    let result = ctx
        .sql(proj_sql)
        .await
        .map_err(|e| DbError::query_pipeline(op_name, &e))?
        .collect()
        .await
        .map_err(|e| DbError::query_pipeline(op_name, &e))?;

    let _ = ctx.deregister_table(tmp_table_name);
    Ok(result)
}
