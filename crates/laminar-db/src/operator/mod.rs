//! `GraphOperator` implementations for each streaming operator type.

pub(crate) mod capability;

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionContext;

use crate::aggregate_state::CompiledProjection;
use crate::db::exact_table_reference;
use crate::error::DbError;
use crate::sql_analysis::extract_projection_filter;

#[cfg(feature = "cluster")]
#[derive(Clone)]
pub(crate) struct RetainedBatch {
    batch: RecordBatch,
    _admissions: Arc<[laminar_core::shuffle::ShuffleBatchAdmission]>,
    assignment_version: Option<u64>,
    uniform_vnode: Option<u32>,
}

#[cfg(feature = "cluster")]
pub(crate) fn uniform_vnode_hint(routed_vnodes: &[u32]) -> Option<u32> {
    match routed_vnodes {
        [vnode] => Some(*vnode),
        _ => None,
    }
}

#[cfg(feature = "cluster")]
impl RetainedBatch {
    pub(crate) fn local(batch: RecordBatch) -> Self {
        Self {
            batch,
            _admissions: Arc::from([]),
            assignment_version: None,
            uniform_vnode: None,
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn from_received(received: laminar_core::shuffle::ReceivedBatch) -> Self {
        let assignment_version = received.assignment_version();
        let uniform_vnode = uniform_vnode_hint(received.routed_vnodes());
        let (batch, admission) = received.into_parts();
        Self {
            batch,
            _admissions: Arc::from([admission]),
            assignment_version: Some(assignment_version),
            uniform_vnode,
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn admitted(
        batch: RecordBatch,
        admission: laminar_core::shuffle::ShuffleBatchAdmission,
        assignment_version: u64,
        uniform_vnode: Option<u32>,
    ) -> Self {
        Self {
            batch,
            _admissions: Arc::from([admission]),
            assignment_version: Some(assignment_version),
            uniform_vnode,
        }
    }

    pub(crate) const fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    #[cfg(feature = "cluster")]
    pub(crate) const fn assignment_version(&self) -> Option<u64> {
        self.assignment_version
    }

    pub(crate) const fn uniform_vnode(&self) -> Option<u32> {
        self.uniform_vnode
    }
}

#[cfg(feature = "cluster")]
impl std::ops::Deref for RetainedBatch {
    type Target = RecordBatch;

    fn deref(&self) -> &Self::Target {
        &self.batch
    }
}

#[cfg(feature = "cluster")]
pub(crate) fn shuffle_routing_error(
    context: &str,
    error: &laminar_core::shuffle::ShuffleRoutingError,
) -> DbError {
    if error.is_not_ready() {
        DbError::ShuffleNotReady(format!("{context}: {error}"))
    } else {
        DbError::ShuffleTerminal(format!("{context}: {error}"))
    }
}

#[cfg(feature = "cluster")]
pub(crate) fn shuffle_send_error(
    context: &str,
    peer: u64,
    error: &std::io::Error,
    sent_any: bool,
) -> DbError {
    if sent_any {
        return DbError::ShufflePartialSend(format!(
            "{context}: send to peer {peer} failed after an earlier frame was admitted: {error}"
        ));
    }
    if matches!(
        error.kind(),
        std::io::ErrorKind::InvalidInput
            | std::io::ErrorKind::InvalidData
            | std::io::ErrorKind::Unsupported
    ) {
        DbError::ShuffleTerminal(format!("{context}: send to peer {peer}: {error}"))
    } else {
        DbError::ShuffleNotReady(format!("{context}: send to peer {peer}: {error}"))
    }
}

#[cfg(feature = "cluster")]
pub(crate) async fn send_shuffle_plan(
    sender: &laminar_core::shuffle::ShuffleSender,
    assignment_version: u64,
    outbound: Vec<(u64, laminar_core::shuffle::ShuffleMessage)>,
    context: &str,
) -> Result<(), DbError> {
    let mut sent_any = false;
    for (peer, message) in outbound {
        match sender
            .send_to_for_assignment(peer, assignment_version, &message)
            .await
        {
            Ok(()) => sent_any = true,
            Err(error) => return Err(shuffle_send_error(context, peer, &error, sent_any)),
        }
    }
    Ok(())
}

/// Re-execute a cached physical plan without re-planning; source leaves are swapped per cycle.
/// Takes a cached `task_ctx` — `SessionContext::task_ctx()` clones the function registries.
pub(crate) async fn execute_cached_physical(
    task_ctx: Arc<TaskContext>,
    op_name: &str,
    physical: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> Result<Vec<RecordBatch>, DbError> {
    datafusion::physical_plan::collect(Arc::clone(physical), task_ctx)
        .await
        .map_err(|e| DbError::query_pipeline(op_name, &e))
}

/// Cached physical plan over a `LiveSourceProvider`; callers swap fresh batches in each cycle.
pub(crate) struct LiveSqlCache {
    handle: laminar_sql::datafusion::LiveSourceHandle,
    physical: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
}

impl LiveSqlCache {
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
        let _ = ctx.deregister_table(exact_table_reference(table_name));
        ctx.register_table(exact_table_reference(table_name), provider)
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
        let task_ctx = ctx.task_ctx();
        Ok(Self {
            handle,
            physical,
            task_ctx,
        })
    }

    pub(crate) async fn apply(
        &self,
        op_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.handle.swap(batches);
        execute_cached_physical(self.task_ctx.clone(), op_name, &self.physical).await
    }
}

/// `LiveSqlCache` wrapping HAVING SQL; used when the predicate can't compile to a `PhysicalExpr`.
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
        op_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.0.apply(op_name, batches).await
    }
}

pub(crate) mod ai_inference;
pub(crate) mod eowc_query;
pub(crate) mod interval_join;
pub(crate) mod lookup_enrich;
pub(crate) mod sql_query;
pub(crate) mod temporal_filter;
pub(crate) mod window_frame;

pub(crate) async fn try_compile_post_projection(
    ctx: &SessionContext,
    proj_sql: &str,
    tmp_table_name: &str,
    batch_schema: &SchemaRef,
) -> Option<CompiledProjection> {
    let empty =
        datafusion::datasource::MemTable::try_new(batch_schema.clone(), vec![vec![]]).ok()?;
    let _ = ctx.deregister_table(exact_table_reference(tmp_table_name));
    ctx.register_table(exact_table_reference(tmp_table_name), Arc::new(empty))
        .ok()?;

    let plan = ctx
        .sql(proj_sql)
        .await
        .ok()
        .map(|dataframe| dataframe.logical_plan().clone());
    let _ = ctx.deregister_table(exact_table_reference(tmp_table_name));
    let plan = plan?;

    let info = extract_projection_filter(&plan)?;
    let state = ctx.state();
    let props = state.execution_props();
    let mut exprs = Vec::with_capacity(info.proj_exprs.len());
    let mut fields = Vec::with_capacity(info.proj_exprs.len());
    for expr in &info.proj_exprs {
        let physical =
            datafusion::physical_expr::create_physical_expr(expr, &info.input_df_schema, props)
                .ok()?;
        let data_type = physical.data_type(info.input_df_schema.as_arrow()).ok()?;
        let nullable = physical
            .nullable(info.input_df_schema.as_arrow())
            .unwrap_or(true);
        let name = match expr {
            datafusion_expr::Expr::Column(column) => column.name.clone(),
            datafusion_expr::Expr::Alias(alias) => alias.name.clone(),
            _ => expr.schema_name().to_string(),
        };
        fields.push(arrow::datatypes::Field::new(name, data_type, nullable));
        exprs.push(physical);
    }
    let filter = if let Some(predicate) = info.filter_predicate.as_ref() {
        Some(
            datafusion::physical_expr::create_physical_expr(
                predicate,
                &info.input_df_schema,
                props,
            )
            .ok()?,
        )
    } else {
        None
    };
    Some(CompiledProjection {
        exprs,
        filter,
        output_schema: Arc::new(arrow::datatypes::Schema::new(fields)),
    })
}

#[cfg(all(test, feature = "cluster"))]
mod shuffle_tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::cluster::control::LeaseDeadline;
    use laminar_core::shuffle::{ShuffleMessage, ShuffleReceiver, ShuffleSender};
    use uuid::Uuid;

    fn batch(value: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap()
    }

    fn assignment() -> CheckpointAssignmentFence {
        CheckpointAssignmentFence::from_owner_map(
            1,
            &[1, 2, 3],
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: Uuid::from_u128(1),
                },
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: Uuid::from_u128(2),
                },
                CheckpointParticipant {
                    node_id: 3,
                    boot_incarnation: Uuid::from_u128(3),
                },
            ],
        )
        .unwrap()
    }

    async fn sender_with_reachable_peer_two() -> (ShuffleSender, ShuffleReceiver) {
        let fence = assignment();
        let receiver = ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), Uuid::from_u128(2))
            .await
            .unwrap();
        receiver
            .install_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
                std::time::Duration::from_secs(60),
            )))
            .unwrap();
        receiver
            .install_assignment_fence(&fence, &[1, 2, 3])
            .unwrap();
        let sender = ShuffleSender::new(1, Uuid::from_u128(1));
        sender
            .install_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
                std::time::Duration::from_secs(60),
            )))
            .unwrap();
        sender.install_assignment_fence(&fence, &[1, 2, 3]).unwrap();
        sender.register_peer(2, receiver.local_addr());
        (sender, receiver)
    }

    #[test]
    fn uniform_vnode_hint_requires_exactly_one_route() {
        assert_eq!(uniform_vnode_hint(&[7]), Some(7));
        assert_eq!(uniform_vnode_hint(&[]), None);
        assert_eq!(uniform_vnode_hint(&[3, 7]), None);
    }

    #[tokio::test]
    async fn send_plan_classifies_first_unreachable_peer_as_not_ready() {
        let (sender, _receiver) = sender_with_reachable_peer_two().await;
        let error = send_shuffle_plan(
            &sender,
            1,
            vec![(3, ShuffleMessage::checkpointed("stage".into(), 2, batch(1)))],
            "test shuffle",
        )
        .await
        .unwrap_err();

        assert!(matches!(error, DbError::ShuffleNotReady(_)));
    }

    #[tokio::test]
    async fn send_plan_classifies_failure_after_admission_as_partial() {
        let (sender, receiver) = sender_with_reachable_peer_two().await;
        let error = send_shuffle_plan(
            &sender,
            1,
            vec![
                (2, ShuffleMessage::checkpointed("left".into(), 1, batch(1))),
                (3, ShuffleMessage::checkpointed("right".into(), 2, batch(2))),
            ],
            "join shuffle",
        )
        .await
        .unwrap_err();

        assert!(matches!(error, DbError::ShufflePartialSend(_)));
        assert!(error.requires_pipeline_recovery());
        let received = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
            .await
            .expect("admitted first frame was not delivered")
            .expect("shuffle receiver closed");
        assert!(matches!(received.message(), ShuffleMessage::Data { .. }));
    }

    #[tokio::test]
    async fn send_plan_classifies_invalid_route_as_terminal() {
        let (sender, _receiver) = sender_with_reachable_peer_two().await;
        let error = send_shuffle_plan(
            &sender,
            1,
            vec![(1, ShuffleMessage::checkpointed("stage".into(), 0, batch(1)))],
            "test shuffle",
        )
        .await
        .unwrap_err();

        assert!(matches!(error, DbError::ShuffleTerminal(_)));
        assert!(error.requires_pipeline_halt());
    }
}

/// Compiled-expr or `LiveSqlCache` fallback for post-projection; populated on first use.
#[derive(Default)]
pub(crate) struct PostProjectionCache {
    compiled: Option<CompiledProjection>,
    compile_failed: bool,
    sql_cache: Option<LiveSqlCache>,
}

/// Post-projection state shared by join operators.
pub(crate) struct ProjectingJoinState {
    pub(crate) op_name: Arc<str>,
    ctx: SessionContext,
    projection_sql: Option<Arc<str>>,
    tmp_table_name: &'static str,
    cache: PostProjectionCache,
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
            cache: PostProjectionCache::default(),
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
            &mut self.cache,
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
    cache: &mut PostProjectionCache,
    batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, DbError> {
    let Some(proj_sql) = proj_sql else {
        return Ok(batches);
    };

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(Vec::new());
    }

    if cache.compiled.is_none() && !cache.compile_failed {
        let schema = batches[0].schema();
        match try_compile_post_projection(ctx, proj_sql, tmp_table_name, &schema).await {
            Some(c) => cache.compiled = Some(c),
            None => cache.compile_failed = true,
        }
    }

    if let Some(ref proj) = cache.compiled {
        let mut result = Vec::with_capacity(batches.len());
        for batch in &batches {
            let projected = proj.evaluate(batch)?;
            if projected.num_rows() > 0 {
                result.push(projected);
            }
        }
        return Ok(result);
    }

    if cache.sql_cache.is_none() {
        let schema = batches[0].schema();
        cache.sql_cache =
            Some(LiveSqlCache::build(ctx, tmp_table_name, schema, proj_sql, op_name).await?);
    }
    cache
        .sql_cache
        .as_ref()
        .expect("sql_cache built above")
        .apply(op_name, batches)
        .await
}

#[cfg(test)]
mod post_projection_tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    #[tokio::test]
    async fn compiled_post_projection_applies_where_filter() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let mut cache = PostProjectionCache::default();
        let result = apply_post_projection(
            &SessionContext::new(),
            "projection_filter_test",
            "__post_projection_filter_test",
            Some("SELECT value + 1 AS adjusted FROM __post_projection_filter_test WHERE id >= 2"),
            &mut cache,
            vec![batch],
        )
        .await
        .unwrap();

        assert!(cache.compiled.is_some());
        assert!(cache.sql_cache.is_none());
        assert_eq!(result.len(), 1);
        let adjusted = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(adjusted.values(), &[21, 31]);
    }
}
