//! Shared SQL filter compile + apply for sinks and SUBSCRIBE.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow_array::{BooleanArray, RecordBatch};
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::prelude::SessionContext;
use datafusion_common::DFSchema;
use datafusion_expr::{Expr, LogicalPlan};

use crate::error::DbError;

static COMPILE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Registers a temporary table on construction and deregisters on drop, so a
/// concurrent compile or an early return can never leak the registration onto
/// the shared `SessionContext`.
struct ScopedTable<'a> {
    ctx: &'a SessionContext,
    name: String,
}

impl<'a> ScopedTable<'a> {
    fn new(ctx: &'a SessionContext, schema: &SchemaRef) -> Option<Self> {
        let name = format!(
            "__filter_compile_{}",
            COMPILE_SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let empty = datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![]]).ok()?;
        ctx.register_table(&name, Arc::new(empty)).ok()?;
        Some(Self { ctx, name })
    }
}

impl Drop for ScopedTable<'_> {
    fn drop(&mut self) {
        let _ = self.ctx.deregister_table(&self.name);
    }
}

/// Compile `filter_sql` against `schema`. Returns `None` on any failure.
pub(crate) async fn compile(
    ctx: &SessionContext,
    filter_sql: &str,
    schema: &SchemaRef,
) -> Option<Arc<dyn PhysicalExpr>> {
    let scoped = ScopedTable::new(ctx, schema)?;
    let sql = format!("SELECT * FROM {} WHERE {filter_sql}", scoped.name);
    let plan = ctx.sql(&sql).await.ok()?.logical_plan().clone();
    drop(scoped);

    let expr = find_predicate(&plan)?;
    let df_schema = DFSchema::try_from(schema.as_ref().clone()).ok()?;
    create_physical_expr(&expr, &df_schema, ctx.state().execution_props()).ok()
}

fn find_predicate(plan: &LogicalPlan) -> Option<Expr> {
    match plan {
        LogicalPlan::Filter(f) => Some(f.predicate.clone()),
        LogicalPlan::Projection(p) => find_predicate(&p.input),
        LogicalPlan::Sort(s) => find_predicate(&s.input),
        LogicalPlan::Limit(l) => find_predicate(&l.input),
        _ => plan.inputs().iter().find_map(|i| find_predicate(i)),
    }
}

/// Apply a compiled boolean predicate. `Ok(None)` if every row is filtered out.
pub(crate) fn apply(
    batch: &RecordBatch,
    expr: &dyn PhysicalExpr,
) -> Result<Option<RecordBatch>, DbError> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let result = expr
        .evaluate(batch)
        .map_err(|e| DbError::Pipeline(format!("filter eval: {e}")))?;
    let array = result
        .into_array(batch.num_rows())
        .map_err(|e| DbError::Pipeline(format!("filter to_array: {e}")))?;
    let mask = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| DbError::Pipeline("filter must return BooleanArray".into()))?;
    let filtered = arrow::compute::filter_record_batch(batch, mask)
        .map_err(|e| DbError::Pipeline(format!("filter: {e}")))?;
    if filtered.num_rows() == 0 {
        Ok(None)
    } else {
        Ok(Some(filtered))
    }
}
