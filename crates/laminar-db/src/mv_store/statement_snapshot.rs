//! Statement planning that reads every scanned materialized view from one completed cycle.

use std::collections::HashMap;

use arrow::array::RecordBatch;
use datafusion::catalog::default_table_source::source_as_provider;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::TreeNodeRecursion;

use super::MvStore;
use crate::error::DbError;
use crate::table_provider::MvTableProvider;

/// Results of every view a statement scans, captured under one store read guard. A cycle
/// applies all views under one write guard, so the statement observes a single cycle.
#[derive(Debug)]
pub(crate) struct StatementViewSnapshot {
    views: HashMap<String, RecordBatch>,
}

impl StatementViewSnapshot {
    pub(crate) fn get(&self, view: &str) -> Option<&RecordBatch> {
        self.views.get(view)
    }
}

impl MvStore {
    fn statement_snapshot(&self, views: &[String]) -> Result<StatementViewSnapshot, DbError> {
        let mut captured = HashMap::with_capacity(views.len());
        for view in views {
            if let Some(batch) = self.to_record_batch(view)? {
                captured.insert(view.clone(), batch);
            }
        }
        Ok(StatementViewSnapshot { views: captured })
    }
}

/// Plan `sql` so that every materialized view it scans reads the same completed cycle.
/// DDL and session statements keep the context's own execution path.
pub(crate) async fn plan_statement(
    ctx: &SessionContext,
    store: &parking_lot::RwLock<MvStore>,
    sql: &str,
) -> Result<DataFrame, DbError> {
    let mut state = ctx.state();
    let plan = state.create_logical_plan(sql).await?;
    if matches!(plan, LogicalPlan::Ddl(_) | LogicalPlan::Statement(_)) {
        return Ok(ctx.execute_logical_plan(plan).await?);
    }
    let views = scanned_views(&plan)?;
    if !views.is_empty() {
        let snapshot = store.read().statement_snapshot(&views)?;
        state
            .config_mut()
            .set_extension(std::sync::Arc::new(snapshot));
    }
    Ok(DataFrame::new(state, plan))
}

/// Sorted, distinct names of the materialized views `plan` scans, including subqueries and
/// views that `DataFusion` inlines at planning.
fn scanned_views(plan: &LogicalPlan) -> Result<Vec<String>, DataFusionError> {
    let mut views = Vec::new();
    collect_scanned_views(plan, &mut views)?;
    views.sort_unstable();
    views.dedup();
    Ok(views)
}

fn collect_scanned_views(
    plan: &LogicalPlan,
    views: &mut Vec<String>,
) -> Result<(), DataFusionError> {
    plan.apply_with_subqueries(|node| {
        let LogicalPlan::TableScan(scan) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let Ok(provider) = source_as_provider(&scan.source) else {
            return Ok(TreeNodeRecursion::Continue);
        };
        if let Some(view) = provider.as_any().downcast_ref::<MvTableProvider>() {
            views.push(view.name().to_owned());
        } else if let Some(inner) = provider.get_logical_plan() {
            collect_scanned_views(&inner, views)?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;

    use super::*;
    use crate::mv_store::MvStorageMode;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("n", DataType::Int64, false),
        ]))
    }

    fn rows(k: &[i64], n: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(k.to_vec())),
                Arc::new(Int64Array::from(n.to_vec())),
            ],
        )
        .unwrap()
    }

    /// Two replace-every-cycle views registered on a fresh context, both at cycle 1.
    fn two_views() -> (SessionContext, Arc<parking_lot::RwLock<MvStore>>) {
        let store = Arc::new(parking_lot::RwLock::new(MvStore::new()));
        let ctx = laminar_sql::create_session_context();
        for view in ["a", "b"] {
            store
                .write()
                .create_mv(view, schema(), MvStorageMode::Aggregate)
                .unwrap();
            store.write().update(view, &rows(&[1, 2], &[1, 1]));
            let provider = MvTableProvider::new(view.into(), schema(), Arc::clone(&store));
            ctx.register_table(view, Arc::new(provider)).unwrap();
        }
        (ctx, store)
    }

    /// Apply cycle 2 to both views under one write guard, as the coordinator does.
    fn apply_second_cycle(store: &parking_lot::RwLock<MvStore>) {
        let mut store = store.write();
        store.update("a", &rows(&[1, 2], &[2, 2]));
        store.update("b", &rows(&[1, 2], &[2, 2]));
    }

    async fn joined_counts(df: DataFrame) -> Vec<(i64, i64)> {
        let mut out = Vec::new();
        for batch in df.collect().await.unwrap() {
            let a = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let b = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            out.extend((0..batch.num_rows()).map(|row| (a.value(row), b.value(row))));
        }
        out.sort_unstable();
        out
    }

    const JOIN: &str = "SELECT a.n, b.n FROM a JOIN b ON a.k = b.k";

    #[tokio::test]
    async fn a_statement_reads_every_view_from_the_cycle_it_was_planned_at() {
        let (ctx, store) = two_views();
        let df = plan_statement(&ctx, &store, JOIN).await.unwrap();
        apply_second_cycle(&store);
        assert_eq!(joined_counts(df).await, vec![(1, 1), (1, 1)]);
    }

    #[tokio::test]
    async fn an_unsnapshotted_scan_reads_the_store_when_it_executes() {
        let (ctx, store) = two_views();
        let df = ctx.sql(JOIN).await.unwrap();
        apply_second_cycle(&store);
        assert_eq!(joined_counts(df).await, vec![(2, 2), (2, 2)]);
    }

    #[tokio::test]
    async fn scanned_views_cover_subqueries_ctes_and_inlined_views() {
        let (ctx, _store) = two_views();
        let plain = MemTable::try_new(schema(), vec![vec![rows(&[1], &[1])]]).unwrap();
        ctx.register_table("plain", Arc::new(plain)).unwrap();
        ctx.sql("CREATE VIEW over_b AS SELECT k FROM b")
            .await
            .unwrap();
        for (sql, expected) in [
            ("SELECT * FROM plain", vec![]),
            (
                "SELECT * FROM a JOIN a AS again ON a.k = again.k",
                vec!["a"],
            ),
            (
                "SELECT k FROM plain WHERE k IN (SELECT k FROM b)",
                vec!["b"],
            ),
            (
                "WITH x AS (SELECT k FROM a) SELECT * FROM x JOIN b USING (k)",
                vec!["a", "b"],
            ),
            (
                "SELECT * FROM a WHERE k IN (SELECT k FROM over_b)",
                vec!["a", "b"],
            ),
        ] {
            let plan = ctx.state().create_logical_plan(sql).await.unwrap();
            assert_eq!(scanned_views(&plan).unwrap(), expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn ddl_keeps_the_context_execution_path() {
        let (ctx, store) = two_views();
        plan_statement(&ctx, &store, "CREATE VIEW over_a AS SELECT * FROM a")
            .await
            .unwrap();
        let df = plan_statement(&ctx, &store, "SELECT COUNT(*) FROM over_a")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let count = batches[0].column(0).as_any().downcast_ref::<Int64Array>();
        assert_eq!(count.map(|c| c.value(0)), Some(2));
    }
}
