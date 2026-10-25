//! Statement planning that reads every scanned materialized view from one completed cycle.

use std::collections::HashMap;

use arrow::array::RecordBatch;
use datafusion::catalog::default_table_source::source_as_provider;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::TreeNodeRecursion;

use super::append_log::LogReadError;
use super::{MvStore, ViewRead};
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

/// Attempts at reading views whose captured checkpoint is retired before its segments load.
const VIEW_READ_ATTEMPTS: u32 = 4;
const VIEW_READ_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(20);

/// Read `views` as of one completed cycle. Every view is captured under one store read guard;
/// append views then load their committed segments, and a read whose checkpoint was retired
/// meanwhile is captured again.
pub(crate) async fn read_views(
    store: &parking_lot::RwLock<MvStore>,
    views: &[String],
) -> Result<HashMap<String, RecordBatch>, DbError> {
    let mut attempt = 1;
    loop {
        let captured = {
            let guard = store.read();
            let mut captured = Vec::with_capacity(views.len());
            for view in views {
                if let Some(read) = guard.read(view)? {
                    captured.push((view.clone(), read));
                }
            }
            captured
        };
        match materialize(captured).await {
            Ok(views) => return Ok(views),
            Err(LogReadError::Failed(error)) => return Err(error),
            Err(LogReadError::Retired) if attempt < VIEW_READ_ATTEMPTS => {
                tokio::time::sleep(VIEW_READ_RETRY_DELAY * attempt).await;
                attempt += 1;
            }
            Err(LogReadError::Retired) => {
                return Err(DbError::MaterializedView(format!(
                    "materialized views [{}] were checkpointed {VIEW_READ_ATTEMPTS} times while \
                     being read; retry the statement",
                    views.join(", ")
                )))
            }
        }
    }
}

async fn materialize(
    captured: Vec<(String, ViewRead)>,
) -> Result<HashMap<String, RecordBatch>, LogReadError> {
    let mut views = HashMap::with_capacity(captured.len());
    for (view, read) in captured {
        let batch = match read {
            ViewRead::Ready(batch) => batch,
            ViewRead::Log(log) => log.materialize().await?,
        };
        views.insert(view, batch);
    }
    Ok(views)
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
        let snapshot = StatementViewSnapshot {
            views: read_views(store, &views).await?,
        };
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
    use crate::mv_store::{MvStorageMode, ViewKey};

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

    /// Upserts of `(k, n)` rows into a keyed view.
    fn upserts(k: &[i64], n: &[i64]) -> RecordBatch {
        let plain = rows(k, n);
        let mut fields = plain.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            laminar_core::changelog::WEIGHT_COLUMN,
            DataType::Int64,
            false,
        )));
        let mut columns = plain.columns().to_vec();
        columns.push(Arc::new(Int64Array::from(vec![1; k.len()])));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    /// Two keyed views registered on a fresh context, both at cycle 1.
    fn two_views() -> (SessionContext, Arc<parking_lot::RwLock<MvStore>>) {
        let store = Arc::new(parking_lot::RwLock::new(MvStore::new()));
        let ctx = laminar_sql::create_session_context();
        for view in ["a", "b"] {
            store
                .write()
                .create_mv(
                    view,
                    schema(),
                    MvStorageMode::changelog(ViewKey::Keyed(vec![0])),
                )
                .unwrap();
            store.write().update(view, &upserts(&[1, 2], &[1, 1]));
            let provider = MvTableProvider::new(view.into(), schema(), Arc::clone(&store));
            ctx.register_table(view, Arc::new(provider)).unwrap();
        }
        (ctx, store)
    }

    /// Apply cycle 2 to both views under one write guard, as the coordinator does.
    fn apply_second_cycle(store: &parking_lot::RwLock<MvStore>) {
        let mut store = store.write();
        store.update("a", &upserts(&[1, 2], &[2, 2]));
        store.update("b", &upserts(&[1, 2], &[2, 2]));
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
