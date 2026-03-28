//! Operator implementations for the `OperatorGraph`.
//!
//! Each module contains a `GraphOperator` implementation that wraps an existing
//! state type from `StreamExecutor`'s decomposed state maps.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::prelude::SessionContext;

use datafusion_expr::LogicalPlan;

use crate::error::DbError;
use crate::sql_analysis::{extract_projection_exprs, CompiledPostProjection};

/// Execute a cached `LogicalPlan` through `DataFusion` physical planning.
///
/// The logical plan is cached (skips SQL parsing + logical optimization),
/// but the physical plan is rebuilt per call because sub-query `MemTable`
/// leaves have different data each time. Only the main query path
/// (`SqlQueryOperator::CachedPlan`) caches the physical plan — its leaves
/// are `LiveSourceExec` which reads fresh data at `execute()` time.
pub(crate) async fn execute_logical_plan(
    ctx: &SessionContext,
    op_name: &str,
    plan: &LogicalPlan,
) -> Result<Vec<RecordBatch>, DbError> {
    let physical = ctx
        .state()
        .create_physical_plan(plan)
        .await
        .map_err(|e| DbError::query_pipeline(op_name, &e))?;
    let task_ctx = ctx.task_ctx();
    datafusion::physical_plan::collect(physical, task_ctx)
        .await
        .map_err(|e| DbError::query_pipeline(op_name, &e))
}

pub(crate) mod asof_join;
pub(crate) mod eowc_query;
pub(crate) mod interval_join;
pub(crate) mod sql_query;
pub(crate) mod temporal_join;
pub(crate) mod temporal_probe_join;

/// Try to compile a post-join projection SQL to physical expressions.
///
/// On success, returns a `CompiledPostProjection` that evaluates the
/// projection directly on a `RecordBatch` without SQL overhead.
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

/// Apply a compiled post-projection to a single batch.
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

/// Apply optional post-join projection, preferring compiled `PhysicalExpr` over SQL.
///
/// Shared by ASOF, temporal, and interval join operators.
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
