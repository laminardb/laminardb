use super::*;
use arrow::array::Int64Array;
use datafusion::common::DataFusionError;
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::MemoryConsumer;

fn input(rows: i64) -> RecordBatch {
    RecordBatch::try_from_iter(vec![(
        "value",
        Arc::new(Int64Array::from_iter_values((0..rows).rev())) as arrow::array::ArrayRef,
    )])
    .unwrap()
}

fn register_input(context: &SessionContext, rows: i64) {
    let batch = input(rows);
    context.deregister_table("events").unwrap();
    context
        .register_table(
            "events",
            Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap()),
        )
        .unwrap();
}

fn bounded_db(bytes: usize) -> Arc<LaminarDB> {
    LaminarDB::open_with_config(LaminarConfig {
        datafusion_memory_limit_bytes: bytes,
        ..Default::default()
    })
    .unwrap()
}

#[test]
fn datafusion_memory_default_rejects_excess_reservation() {
    let db = LaminarDB::open().unwrap();
    let runtime = db.ctx.runtime_env();
    let reservation = MemoryConsumer::new("limit regression").register(&runtime.memory_pool);
    assert!(reservation
        .try_grow(crate::DEFAULT_DATAFUSION_MEMORY_LIMIT_BYTES + 1)
        .is_err());
    assert_eq!(runtime.memory_pool.reserved(), 0);
}

#[test]
fn datafusion_memory_runtime_disables_spill_files() {
    let db = LaminarDB::open().unwrap();
    let disk = &db.ctx.runtime_env().disk_manager;
    assert!(!disk.tmp_files_enabled());
    assert!(disk.create_tmp_file("regression").is_err());
    assert!(disk.temp_dir_paths().is_empty());
}

#[tokio::test]
async fn datafusion_memory_configuration_is_enforced_for_direct_and_builder_entry() {
    let error = LaminarDB::open_with_config(LaminarConfig {
        datafusion_memory_limit_bytes: 0,
        ..Default::default()
    })
    .err()
    .unwrap();
    assert!(matches!(error, DbError::Config(_)));
    let error = LaminarDB::builder()
        .datafusion_memory_limit_bytes(0)
        .build()
        .await
        .err()
        .unwrap();
    assert!(matches!(error, DbError::Config(_)));
    let db = LaminarDB::builder()
        .datafusion_memory_limit_bytes(1234)
        .build()
        .await
        .unwrap();
    let runtime = db.ctx.runtime_env();
    let reservation = MemoryConsumer::new("configured limit").register(&runtime.memory_pool);
    reservation.try_grow(1234).unwrap();
    assert!(reservation.try_grow(1).is_err());
}

#[test]
fn operator_context_preserves_the_root_optimizer_sequence_once() {
    let db = bounded_db(1024);
    let state = db.ctx.state();
    db.ctx
        .add_optimizer_rule(Arc::clone(&state.optimizers()[0]));
    let root = db.ctx.state();
    let operator = db.create_operator_context().state();
    assert_eq!(operator.optimizers().len(), root.optimizers().len());
    for (actual, expected) in operator.optimizers().iter().zip(root.optimizers()) {
        assert!(Arc::ptr_eq(actual, expected));
    }
}

#[test]
fn datafusion_memory_contexts_share_a_budget_across_generations_but_not_databases() {
    let db = bounded_db(1024);
    let other = bounded_db(1024);
    let contexts = [
        db.ctx.clone(),
        db.create_operator_context(),
        db.create_auxiliary_context(),
    ];
    let reservation =
        MemoryConsumer::new("old graph").register(&contexts[1].runtime_env().memory_pool);
    reservation.try_grow(1024).unwrap();
    for context in contexts
        .iter()
        .chain(std::iter::once(&db.create_operator_context()))
    {
        let runtime = context.runtime_env();
        let contender = MemoryConsumer::new("contender").register(&runtime.memory_pool);
        assert!(contender.try_grow(1).is_err());
        assert!(!runtime.disk_manager.tmp_files_enabled());
    }
    let independent =
        MemoryConsumer::new("other DB").register(&other.ctx.runtime_env().memory_pool);
    independent.try_grow(1024).unwrap();
    drop(reservation);
    let released = MemoryConsumer::new("released").register(&db.ctx.runtime_env().memory_pool);
    released.try_grow(1024).unwrap();
}

#[test]
fn datafusion_memory_concurrent_contexts_cannot_each_take_the_full_budget() {
    let db = bounded_db(1024);
    let contexts = [
        db.ctx.clone(),
        db.create_operator_context(),
        db.create_auxiliary_context(),
    ];
    let barrier = std::sync::Barrier::new(contexts.len());
    std::thread::scope(|scope| {
        let tasks: Vec<_> = contexts
            .iter()
            .map(|context| {
                let barrier = &barrier;
                scope.spawn(move || {
                    let runtime = context.runtime_env();
                    let reservation =
                        MemoryConsumer::new("concurrent").register(&runtime.memory_pool);
                    barrier.wait();
                    let acquired = reservation.try_grow(768).is_ok();
                    barrier.wait();
                    acquired
                })
            })
            .collect();
        assert_eq!(
            tasks
                .into_iter()
                .map(|task| usize::from(task.join().unwrap()))
                .sum::<usize>(),
            1
        );
    });
    assert_eq!(db.ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn datafusion_memory_real_plans_fail_without_spilling_and_release_reservations() {
    let db = bounded_db(64 * 1024);
    for context in [
        db.ctx.clone(),
        db.create_operator_context(),
        db.create_auxiliary_context(),
    ] {
        register_input(&context, 16_384);
        for sql in [
            "SELECT value FROM events ORDER BY value",
            "SELECT value, COUNT(*) FROM events GROUP BY value",
            "SELECT a.value FROM events a JOIN events b ON a.value = b.value",
        ] {
            let error = context.sql(sql).await.unwrap().collect().await.unwrap_err();
            assert!(
                matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
                "{sql}: {error}"
            );
            let runtime = context.runtime_env();
            assert_eq!(runtime.memory_pool.reserved(), 0, "{sql}");
            assert!(runtime.disk_manager.temp_dir_paths().is_empty());
            assert_eq!(runtime.disk_manager.used_disk_space(), 0);
        }
        register_input(&context, 3);
        let batches = context
            .sql("SELECT value FROM events ORDER BY value")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    }
}

#[tokio::test]
async fn datafusion_memory_cached_plan_retries_after_exhaustion() {
    use crate::operator::{create_cached_physical_plan, execute_cached_physical};
    use laminar_sql::datafusion::LiveSourceProvider;

    let db = bounded_db(64 * 1024);
    let context = db.create_operator_context();
    let provider = Arc::new(LiveSourceProvider::new(input(1).schema()));
    let handle = provider.handle();
    context.register_table("events", provider).unwrap();
    let df = context
        .sql("SELECT value FROM events ORDER BY value")
        .await
        .unwrap();
    let plan = create_cached_physical_plan(&context, df.logical_plan())
        .await
        .unwrap();
    for _ in 0..4 {
        handle.swap(vec![input(16_384)]);
        let error = execute_cached_physical(context.task_ctx(), "sorted", &plan)
            .await
            .unwrap_err();
        assert!(matches!(error, DbError::QueryPipeline { .. }));
        assert!(error.to_string().contains("LDB-9001"), "{error}");
        assert_eq!(context.runtime_env().memory_pool.reserved(), 0);
        handle.swap(vec![input(3)]);
        let output = execute_cached_physical(context.task_ctx(), "sorted", &plan)
            .await
            .unwrap();
        let values = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values().as_ref(), &[0, 1, 2]);
        assert_eq!(context.runtime_env().memory_pool.reserved(), 0);
    }
}

#[tokio::test]
async fn datafusion_memory_cancelled_sort_releases_reservations_and_reuses_plan() {
    use crate::operator::execute_cached_physical;
    use datafusion::physical_expr::{expressions::Column, LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::{sorts::sort::SortExec, ExecutionPlan};
    use laminar_sql::datafusion::{ChannelStreamSource, StreamingScanExec};

    let db = bounded_db(64 * 1024);
    let source = Arc::new(ChannelStreamSource::with_capacity(input(1).schema(), 2));
    let sender = source.take_sender().unwrap();
    // Drive the actual sorter with a paused input so cancellation happens with live reservations.
    let plan: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
            "value", 0,
        )))])
        .unwrap(),
        Arc::new(StreamingScanExec::new(source.clone(), None, vec![])),
    ));
    sender.send(input(128)).await.unwrap();
    let mut execution = Box::pin(execute_cached_physical(
        db.ctx.task_ctx(),
        "cancelled",
        &plan,
    ));
    assert!(futures::poll!(execution.as_mut()).is_pending());
    assert!(db.ctx.runtime_env().memory_pool.reserved() > 0);
    drop(execution);
    assert_eq!(db.ctx.runtime_env().memory_pool.reserved(), 0);
    drop(sender);
    drop(source.reset());
    let sender = source.take_sender().unwrap();
    sender.send(input(3)).await.unwrap();
    drop(sender);
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        execute_cached_physical(db.ctx.task_ctx(), "resumed", &plan),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    assert_eq!(db.ctx.runtime_env().memory_pool.reserved(), 0);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn datafusion_memory_local_table_diagnostic_uses_the_db_budget() {
    let db = bounded_db(64 * 1024);
    register_input(&db.ctx, 16_384);
    let view = db
        .ctx
        .sql("SELECT value, COUNT(*) AS n FROM events GROUP BY value")
        .await
        .unwrap()
        .into_view();
    db.ctx.register_table("sorted", view).unwrap();
    let error = db.collect_local_table("sorted").await.unwrap_err();
    let DbError::DataFusion(error) = error else {
        panic!("{error}");
    };
    assert!(matches!(
        error.find_root(),
        DataFusionError::ResourcesExhausted(_)
    ));
    assert_eq!(db.ctx.runtime_env().memory_pool.reserved(), 0);
}
