use super::*;
use crate::operator::{create_cached_physical_plan, execute_cached_physical, LiveSqlCache};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::ExecutionPlan;
use laminar_sql::datafusion::{LiveSourceHandle, LiveSourceProvider};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn batch(ids: &[i64], values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

fn register_source(ctx: &SessionContext, name: &str) -> LiveSourceHandle {
    let provider = Arc::new(LiveSourceProvider::new(schema()));
    let handle = provider.handle();
    ctx.register_table(name, provider).unwrap();
    handle
}

fn values(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

fn assert_unexecuted(plan: &Arc<dyn ExecutionPlan>) {
    plan.apply(|node| {
        if let Some(metrics) = node.metrics() {
            assert_eq!(
                metrics.iter().count(),
                0,
                "{} retained metrics",
                node.name()
            );
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
}

#[tokio::test]
async fn cached_plan_sorted_batches_use_fresh_values_and_release_metrics() {
    for order in ["ASC", "DESC"] {
        let ctx = laminar_sql::create_session_context();
        let handle = register_source(&ctx, "events");
        let sql = format!(
            "SELECT value + 1 AS v FROM events WHERE id > 0 ORDER BY value {order} LIMIT 2"
        );
        let mut operator = SqlQueryOperator::new("sorted", &sql, ctx, None, false);
        for cycle in 0..64 {
            let base = if cycle % 2 == 0 {
                cycle * 100
            } else {
                -cycle * 100
            };
            let input = vec![
                batch(&[1, 2], &[base + 3, base + 1]),
                batch(&[3], &[base + 2]),
            ];
            handle.swap(input.clone());
            let output = operator.process(&[input], &[i64::MIN]).await.unwrap();
            let expected = if order == "ASC" {
                vec![base + 2, base + 3]
            } else {
                vec![base + 4, base + 3]
            };
            assert_eq!(values(&output, 0), expected, "cycle {cycle}, {order}");
            let QueryState::CachedPlan(plan) = &operator.state else {
                panic!("sort must exercise the cached physical path");
            };
            assert_unexecuted(plan);
        }
    }
}

#[tokio::test]
async fn cached_plan_projection_preserves_live_source_after_error() {
    let ctx = laminar_sql::create_session_context();
    let cache = LiveSqlCache::build(
        &ctx,
        "events",
        schema(),
        "SELECT 100 / value AS quotient FROM events",
        "division",
    )
    .await
    .unwrap();
    for cycle in 0..32 {
        let error = cache
            .apply("division", vec![batch(&[cycle], &[0])])
            .await
            .unwrap_err();
        assert!(matches!(error, DbError::QueryPipeline { .. }));
        assert!(
            error
                .to_string()
                .to_ascii_lowercase()
                .contains("divide by zero"),
            "{error}"
        );
        assert_unexecuted(&cache.physical);
        let output = cache
            .apply("division", vec![batch(&[cycle], &[2])])
            .await
            .unwrap();
        assert_eq!(values(&output, 0), [50]);
        assert_unexecuted(&cache.physical);
    }
}

#[tokio::test]
async fn cached_plan_compiled_fallback_recovers_on_the_next_batch() {
    let ctx = laminar_sql::create_session_context();
    let handle = register_source(&ctx, "events");
    let mut operator = SqlQueryOperator::new(
        "division",
        "SELECT 100 / value AS quotient FROM events",
        ctx,
        None,
        false,
    );
    operator.lazy_init().await.unwrap();
    assert!(matches!(operator.state, QueryState::Compiled(_)));
    let invalid = batch(&[1], &[0]);
    handle.swap(vec![invalid.clone()]);
    assert!(matches!(
        operator.process(&[vec![invalid]], &[i64::MIN]).await,
        Err(DbError::QueryPipeline { .. })
    ));
    let valid = batch(&[2], &[4]);
    handle.swap(vec![valid.clone()]);
    let output = operator.process(&[vec![valid]], &[i64::MIN]).await.unwrap();
    assert_eq!(values(&output, 0), [25]);
    let QueryState::CachedPlan(plan) = &operator.state else {
        panic!("expected cached fallback");
    };
    assert_unexecuted(plan);
}

#[tokio::test]
async fn cached_plan_join_rebuilds_both_live_inputs() {
    let ctx = laminar_sql::create_session_context();
    let left = register_source(&ctx, "events");
    let right = register_source(&ctx, "other");
    let mut operator = SqlQueryOperator::new(
        "joined",
        "SELECT a.value + b.value AS total FROM events a JOIN other b ON a.id = b.id",
        ctx,
        None,
        false,
    );
    for cycle in 1..33 {
        let input = batch(&[cycle, -1], &[cycle * 10, -100]);
        left.swap(vec![input.clone()]);
        right.swap(vec![batch(&[cycle, -2], &[cycle, -200])]);
        let output = operator.process(&[vec![input]], &[i64::MIN]).await.unwrap();
        assert_eq!(values(&output, 0), [cycle * 11]);
        let QueryState::CachedPhysical(plan) = &operator.state else {
            panic!("join must exercise the multi-source cached path");
        };
        assert_unexecuted(plan);
    }
}

#[tokio::test]
async fn cached_plan_pre_aggregate_keeps_running_state_and_fresh_subquery() {
    let ctx = laminar_sql::create_session_context();
    let events = register_source(&ctx, "events");
    let allowed = register_source(&ctx, "allowed");
    let mut operator = SqlQueryOperator::new(
        "sums",
        "SELECT id, SUM(value) AS total FROM events \
         WHERE id IN (SELECT id FROM allowed) GROUP BY id",
        ctx,
        None,
        false,
    );
    let mut totals = [0, 0];
    for cycle in 0..32 {
        let selected = cycle % 2;
        let input = batch(&[0, 1], &[3, 7]);
        events.swap(vec![input.clone()]);
        allowed.swap(vec![batch(&[selected], &[0])]);
        let output = operator.process(&[vec![input]], &[i64::MIN]).await.unwrap();
        if selected == 0 {
            totals[0] += 3;
        } else {
            totals[1] += 7;
        }
        let mut actual: Vec<_> = values(&output, 0)
            .into_iter()
            .zip(values(&output, 1))
            .collect();
        actual.sort_unstable();
        let expected = if cycle == 0 {
            vec![(0, 3)]
        } else {
            vec![(0, totals[0]), (1, totals[1])]
        };
        assert_eq!(actual, expected);
        let QueryState::Agg(state) = &operator.state else {
            panic!("expected managed aggregate");
        };
        assert!(state.compiled_projection().is_none());
        assert_unexecuted(state.cached_pre_agg_physical().unwrap());
    }
}

#[tokio::test]
async fn cached_plan_cancellation_drops_buffers_and_allows_next_execution() {
    use laminar_sql::datafusion::{ChannelStreamSource, StreamingTableProvider};

    let ctx = laminar_sql::create_session_context();
    let source = Arc::new(ChannelStreamSource::with_capacity(schema(), 2));
    let sender = source.take_sender().unwrap();
    ctx.register_table(
        "events",
        Arc::new(StreamingTableProvider::new("events", source.clone())),
    )
    .unwrap();
    let df = ctx.sql("SELECT id AS copy FROM events").await.unwrap();
    let plan = create_cached_physical_plan(&ctx, df.logical_plan())
        .await
        .unwrap();
    let ids: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![42]));
    let input = RecordBatch::try_new(
        schema(),
        vec![ids.clone(), Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    sender.send(input).await.unwrap();
    let mut execution = Box::pin(execute_cached_physical(ctx.task_ctx(), "cancelled", &plan));
    assert!(futures::poll!(execution.as_mut()).is_pending());
    assert!(Arc::strong_count(&ids) > 1);
    drop(execution);
    assert_eq!(
        Arc::strong_count(&ids),
        1,
        "cancelled execution retained its output buffer"
    );
    assert_unexecuted(&plan);
    drop(sender);

    drop(source.reset());
    let sender = source.take_sender().unwrap();
    sender.send(batch(&[99], &[2])).await.unwrap();
    drop(sender);
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        execute_cached_physical(ctx.task_ctx(), "resumed", &plan),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(values(&output, 0), [99]);
    assert_unexecuted(&plan);
}

#[tokio::test]
async fn cached_plan_rejects_file_scans_without_changing_ad_hoc_config() {
    let ctx = laminar_sql::create_session_context();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.parquet");
    let path = path.to_str().unwrap();
    ctx.read_batch(batch(&[1, 2, 3], &[20, 10, 30]))
        .unwrap()
        .write_parquet(
            path,
            datafusion::dataframe::DataFrameWriteOptions::new(),
            None,
        )
        .await
        .unwrap();
    ctx.register_parquet(
        "events",
        path,
        datafusion::prelude::ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    let df = ctx
        .sql("SELECT value FROM events WHERE id > 0 ORDER BY value LIMIT 1")
        .await
        .unwrap();
    let error = create_cached_physical_plan(&ctx, df.logical_plan())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        datafusion::common::DataFusionError::NotImplemented(_)
    ));
    assert!(error
        .to_string()
        .contains("file scans cannot use cached streaming execution"));
    let ad_hoc = ctx
        .state()
        .create_physical_plan(df.logical_plan())
        .await
        .unwrap();
    let ad_hoc_display = datafusion::physical_plan::displayable(ad_hoc.as_ref())
        .indent(false)
        .to_string();
    assert!(ad_hoc_display.contains("DynamicFilter"), "{ad_hoc_display}");
    assert_eq!(values(&df.collect().await.unwrap(), 0), [10]);
    let state = ctx.state();
    let optimizer = &state.config_options().optimizer;
    assert!(optimizer.enable_dynamic_filter_pushdown);
    assert!(optimizer.enable_topk_dynamic_filter_pushdown);
    assert!(optimizer.enable_join_dynamic_filter_pushdown);
    assert!(optimizer.enable_aggregate_dynamic_filter_pushdown);
}

#[tokio::test]
async fn cached_plan_rejects_recursive_work_tables() {
    let ctx = laminar_sql::create_session_context();
    let df = ctx
        .sql(
            "WITH RECURSIVE numbers AS (SELECT 1 AS n UNION ALL \
        SELECT n + 1 FROM numbers WHERE n < 3) SELECT n FROM numbers",
        )
        .await
        .unwrap();
    ctx.register_table("recursive_view", df.clone().into_view())
        .unwrap();
    let view = ctx.sql("SELECT n FROM recursive_view").await.unwrap();
    for logical in [df.logical_plan(), view.logical_plan()] {
        let error = create_cached_physical_plan(&ctx, logical)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            datafusion::common::DataFusionError::NotImplemented(_)
        ));
        assert!(error
            .to_string()
            .contains("recursive queries cannot use cached streaming execution"));
    }
    assert_eq!(
        df.collect()
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        3
    );
}
