//! Statement planning that reads every scanned materialized view from one completed cycle, or
//! from one committed checkpoint.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use datafusion::catalog::default_table_source::source_as_provider;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::TreeNodeRecursion;

use super::append_log::LogReadError;
use super::committed::{CommittedRead, CommittedReadError, CommittedViews, PinnedCut, ViewShape};
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

/// The committed cut a statement reads every view at, pinned until the statement is dropped.
/// Each scan reads its view from the cut's artifacts with the scan's filters.
pub(crate) struct CommittedStatement {
    views: Arc<CommittedViews>,
    cut: PinnedCut,
    shapes: HashMap<String, ViewShape>,
}

impl std::fmt::Debug for CommittedStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommittedStatement")
            .field("epoch", &self.cut.epoch)
            .field("views", &self.shapes.keys())
            .finish_non_exhaustive()
    }
}

impl CommittedStatement {
    pub(crate) async fn read(
        &self,
        view: &str,
        filters: &[Expr],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let shape = self.shapes.get(view).ok_or_else(|| {
            DbError::MaterializedView(format!("view '{view}' was not planned for this read"))
        })?;
        self.cut.read_view(&self.views, shape, filters).await
    }
}

/// The committed cut a planned statement reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadCut {
    pub(crate) epoch: u64,
    pub(crate) checkpoint_id: u64,
    pub(crate) taken_at_ms: u64,
}

/// Attempts at reading views whose captured checkpoint retention moves past before it is pinned.
const VIEW_READ_ATTEMPTS: u32 = 4;
const VIEW_READ_RETRY_DELAY: Duration = Duration::from_millis(20);
/// How long a read may hold a committed checkpoint against retention.
pub(crate) const VIEW_READ_DEADLINE: Duration = Duration::from_secs(30);

/// Read `views` as of one completed cycle. Every view is captured under one store read guard;
/// append views then pin the checkpoint they captured and load its segments. A read whose
/// checkpoint retention moved past before it was pinned is captured again.
pub(crate) async fn read_views(
    store: &parking_lot::RwLock<MvStore>,
    views: &[String],
) -> Result<HashMap<String, RecordBatch>, DbError> {
    let (views, ()) = read_captured(store, &views.join(", "), |guard| {
        let mut captured = Vec::with_capacity(views.len());
        for view in views {
            if let Some(read) = guard.read(view)? {
                captured.push((view.clone(), read));
            }
        }
        Ok((captured, ()))
    })
    .await?;
    Ok(views)
}

/// Read what `capture` captures under one store read guard, with what it takes beside the
/// views under that guard, as [`read_views`] does. A capture whose checkpoint retention moved
/// past is dropped and taken again.
pub(crate) async fn read_captured<T>(
    store: &parking_lot::RwLock<MvStore>,
    described: &str,
    mut capture: impl FnMut(&MvStore) -> Result<(Vec<(String, ViewRead)>, T), DbError>,
) -> Result<(HashMap<String, RecordBatch>, T), DbError> {
    let mut attempt = 1;
    loop {
        let (captured, taken) = capture(&store.read())?;
        match materialize(captured, Instant::now() + VIEW_READ_DEADLINE).await {
            Ok(views) => return Ok((views, taken)),
            Err(LogReadError::Failed(error)) => return Err(error),
            Err(LogReadError::Retired) if attempt < VIEW_READ_ATTEMPTS => {
                drop(taken);
                tokio::time::sleep(VIEW_READ_RETRY_DELAY * attempt).await;
                attempt += 1;
            }
            Err(LogReadError::Retired) => {
                return Err(DbError::MaterializedView(format!(
                    "materialized views [{described}] were checkpointed {VIEW_READ_ATTEMPTS} \
                     times while being read; retry the statement"
                )))
            }
        }
    }
}

async fn materialize(
    captured: Vec<(String, ViewRead)>,
    deadline: Instant,
) -> Result<HashMap<String, RecordBatch>, LogReadError> {
    let mut views = HashMap::with_capacity(captured.len());
    for (view, read) in captured {
        let batch = match read {
            ViewRead::Ready(batch) => batch,
            ViewRead::Log(log) => log.materialize(deadline).await?,
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
    let views = scanned_tables(&plan)?.views;
    if !views.is_empty() && store.read().holds_node_part() {
        return Err(DbError::InvalidOperation(format!(
            "a cluster node holds only its part of materialized views [{}]; read them at a \
             committed cut",
            views.join(", ")
        )));
    }
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

/// Plan the query `sql` so that every materialized view it scans reads the committed cut `at`
/// resolves to. The cut stays pinned until the returned frame and every stream executed from it
/// are dropped, or until `deadline`.
pub(crate) async fn plan_committed_statement(
    ctx: &SessionContext,
    store: &parking_lot::RwLock<MvStore>,
    sql: &str,
    at: CommittedRead,
    deadline: Instant,
) -> Result<(DataFrame, ReadCut), DbError> {
    let mut state = ctx.state();
    let plan = state.create_logical_plan(sql).await?;
    if matches!(plan, LogicalPlan::Ddl(_) | LogicalPlan::Statement(_)) {
        return Err(DbError::InvalidOperation(
            "a committed read must be a query".into(),
        ));
    }
    let scanned = scanned_tables(&plan)?;
    if let Some(table) = scanned.live.into_iter().next() {
        return Err(CommittedReadError::LiveTable { table }.into());
    }
    let views = store
        .read()
        .committed_views()
        .ok_or(CommittedReadError::CheckpointingDisabled)?;
    let cut = views.pin(at, deadline).await?;
    let shapes = {
        let guard = store.read();
        scanned
            .views
            .iter()
            .filter_map(|view| Some((view.clone(), guard.view_shape(view)?)))
            .collect()
    };
    let read_cut = ReadCut {
        epoch: cut.epoch,
        checkpoint_id: cut.checkpoint_id,
        taken_at_ms: cut.taken_at_ms,
    };
    state
        .config_mut()
        .set_extension(Arc::new(CommittedStatement { views, cut, shapes }));
    Ok((DataFrame::new(state, plan), read_cut))
}

/// Tables a plan scans: sorted, distinct materialized views, including subqueries and views
/// that `DataFusion` inlines at planning, and every other table.
#[derive(Debug, Default)]
struct ScannedTables {
    views: Vec<String>,
    live: Vec<String>,
}

fn scanned_tables(plan: &LogicalPlan) -> Result<ScannedTables, DataFusionError> {
    let mut scanned = ScannedTables::default();
    collect_scanned_tables(plan, &mut scanned)?;
    scanned.views.sort_unstable();
    scanned.views.dedup();
    Ok(scanned)
}

fn collect_scanned_tables(
    plan: &LogicalPlan,
    scanned: &mut ScannedTables,
) -> Result<(), DataFusionError> {
    plan.apply_with_subqueries(|node| {
        let LogicalPlan::TableScan(scan) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let Ok(provider) = source_as_provider(&scan.source) else {
            scanned.live.push(scan.table_name.to_string());
            return Ok(TreeNodeRecursion::Continue);
        };
        if let Some(view) = provider.as_any().downcast_ref::<MvTableProvider>() {
            scanned.views.push(view.name().to_owned());
        } else if let Some(inner) = provider.get_logical_plan() {
            collect_scanned_tables(&inner, scanned)?;
        } else {
            scanned.live.push(scan.table_name.to_string());
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
            assert_eq!(scanned_tables(&plan).unwrap().views, expected, "{sql}");
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
