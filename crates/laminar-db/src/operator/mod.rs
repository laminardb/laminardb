//! `GraphOperator` implementations for each streaming operator type.

pub(crate) mod capability;

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
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
    admissions: Arc<[laminar_core::shuffle::ShuffleBatchAdmission]>,
    assignment_version: Option<u64>,
    peer: Option<u64>,
    recovery_gen: Option<u64>,
    routed_vnodes: Arc<[u32]>,
}

#[cfg(feature = "cluster")]
impl RetainedBatch {
    #[cfg(test)]
    pub(crate) fn local(batch: RecordBatch) -> Self {
        Self {
            batch,
            admissions: Arc::from([]),
            assignment_version: None,
            peer: None,
            recovery_gen: None,
            routed_vnodes: Arc::from([]),
        }
    }

    pub(crate) fn from_received(received: laminar_core::shuffle::ReceivedBatch) -> Self {
        let assignment_version = received.assignment_version();
        let peer = received.peer();
        let recovery_gen = received.recovery_gen();
        let routed_vnodes = received.routed_vnodes_arc();
        let (batch, admission) = received.into_parts();
        Self {
            batch,
            admissions: Arc::from([admission]),
            assignment_version: Some(assignment_version),
            peer: Some(peer),
            recovery_gen: Some(recovery_gen),
            routed_vnodes,
        }
    }

    pub(crate) fn admitted(
        batch: RecordBatch,
        admission: laminar_core::shuffle::ShuffleBatchAdmission,
        peer: u64,
        assignment_version: u64,
        recovery_gen: u64,
        routed_vnodes: Arc<[u32]>,
    ) -> Self {
        Self {
            batch,
            admissions: Arc::from([admission]),
            assignment_version: Some(assignment_version),
            peer: Some(peer),
            recovery_gen: Some(recovery_gen),
            routed_vnodes,
        }
    }

    pub(crate) fn restored_channel(
        batch: RecordBatch,
        peer: u64,
        assignment_version: u64,
        recovery_gen: u64,
        routed_vnodes: Arc<[u32]>,
    ) -> Self {
        Self {
            batch,
            admissions: Arc::from([]),
            assignment_version: Some(assignment_version),
            peer: Some(peer),
            recovery_gen: Some(recovery_gen),
            routed_vnodes,
        }
    }

    pub(crate) const fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub(crate) const fn assignment_version(&self) -> Option<u64> {
        self.assignment_version
    }

    pub(crate) const fn peer(&self) -> Option<u64> {
        self.peer
    }

    pub(crate) const fn recovery_gen(&self) -> Option<u64> {
        self.recovery_gen
    }

    pub(crate) fn routed_vnodes(&self) -> &[u32] {
        &self.routed_vnodes
    }

    pub(crate) fn heap_bytes(&self) -> Option<usize> {
        self.batch
            .num_columns()
            .checked_mul(std::mem::size_of::<Arc<dyn arrow::array::Array>>())?
            .checked_add(self.batch.get_array_memory_size())?
            .checked_add(
                self.routed_vnodes
                    .len()
                    .checked_mul(std::mem::size_of::<u32>())?,
            )?
            .checked_add(self.admissions.len().checked_mul(std::mem::size_of::<
                laminar_core::shuffle::ShuffleBatchAdmission,
            >())?)?
            .checked_add(4 * std::mem::size_of::<usize>())
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
            "{context}: send to peer {peer} failed after a frame was or may have been admitted: {error}"
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
pub(crate) async fn send_shuffle_plan_retaining(
    sender: &laminar_core::shuffle::ShuffleSender,
    assignment_version: u64,
    outbound: Vec<(u64, laminar_core::shuffle::ShuffleMessage)>,
    context: &str,
) -> (
    Result<(), DbError>,
    Option<Vec<(u64, laminar_core::shuffle::ShuffleMessage)>>,
) {
    let mut group_indices = rustc_hash::FxHashMap::default();
    let mut peer_groups = Vec::<(u64, Vec<(usize, laminar_core::shuffle::ShuffleMessage)>)>::new();
    for (index, (peer, message)) in outbound.into_iter().enumerate() {
        let group_index = if let Some(group_index) = group_indices.get(&peer) {
            *group_index
        } else {
            let group_index = peer_groups.len();
            group_indices.insert(peer, group_index);
            peer_groups.push((peer, Vec::new()));
            group_index
        };
        peer_groups[group_index].1.push((index, message));
    }

    let outcomes =
        futures::future::join_all(peer_groups.into_iter().map(|(peer, messages)| async move {
            let mut admitted_any = false;
            let mut messages = messages.into_iter();
            while let Some((index, message)) = messages.next() {
                match sender
                    .send_to_for_assignment(peer, assignment_version, &message)
                    .await
                {
                    Ok(()) => admitted_any = true,
                    Err(error) => {
                        admitted_any |=
                            laminar_core::shuffle::shuffle_send_may_have_been_admitted(&error);
                        let mut retained = Vec::new();
                        if !admitted_any {
                            retained.push((index, peer, message));
                            retained
                                .extend(messages.map(|(index, message)| (index, peer, message)));
                        }
                        return (admitted_any, Some((index, peer, error)), retained);
                    }
                }
            }
            (admitted_any, None, Vec::new())
        }))
        .await;

    let mut sent_any = false;
    let mut errors = Vec::new();
    let mut retained = Vec::new();
    for (admitted, error, peer_retained) in outcomes {
        sent_any |= admitted;
        if let Some(error) = error {
            errors.push(error);
        }
        retained.extend(peer_retained);
    }

    let first_error = if sent_any {
        errors.into_iter().min_by_key(|(index, _, _)| *index)
    } else {
        errors.into_iter().min_by_key(|(index, _, error)| {
            let terminal = matches!(
                error.kind(),
                std::io::ErrorKind::InvalidInput
                    | std::io::ErrorKind::InvalidData
                    | std::io::ErrorKind::Unsupported
            );
            (!terminal, *index)
        })
    };

    let result = match first_error {
        Some((_, peer, error)) => Err(shuffle_send_error(context, peer, &error, sent_any)),
        None => Ok(()),
    };
    let retry_plan = if matches!(&result, Err(error) if error.is_shuffle_not_ready()) {
        retained.sort_unstable_by_key(|(index, _, _)| *index);
        Some(
            retained
                .into_iter()
                .map(|(_, peer, message)| (peer, message))
                .collect(),
        )
    } else {
        None
    };
    (result, retry_plan)
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

struct ScopedTableRegistration<'a> {
    ctx: &'a SessionContext,
    table: datafusion::common::TableReference,
}

impl<'a> ScopedTableRegistration<'a> {
    fn register(
        ctx: &'a SessionContext,
        table_name: &str,
        provider: Arc<dyn TableProvider>,
    ) -> datafusion::common::Result<Self> {
        let table = exact_table_reference(table_name);
        if let Some(previous) = ctx.register_table(table.clone(), provider)? {
            ctx.register_table(table.clone(), previous)?;
            return Err(datafusion::common::DataFusionError::Execution(format!(
                "temporary planning table '{table_name}' is already registered"
            )));
        }
        Ok(Self { ctx, table })
    }
}

impl Drop for ScopedTableRegistration<'_> {
    fn drop(&mut self) {
        let _ = self.ctx.deregister_table(self.table.clone());
    }
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
        let _registration = ScopedTableRegistration::register(ctx, table_name, provider)
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

pub(crate) mod ai_inference;
pub(crate) mod eowc_query;
#[cfg(feature = "cluster")]
mod frontier;
pub(crate) mod interval_join;
/// Private mutable-input normalization state used only by explicitly configured bounded joins.
pub(crate) mod interval_join_input;
pub(crate) mod lookup_enrich;
pub(crate) mod sql_query;
pub(crate) mod temporal_filter;
pub(crate) mod temporal_join;
pub(crate) mod window_frame;

pub(crate) async fn try_compile_post_projection(
    ctx: &SessionContext,
    proj_sql: &str,
    tmp_table_name: &str,
    batch_schema: &SchemaRef,
) -> Option<CompiledProjection> {
    let empty =
        datafusion::datasource::MemTable::try_new(batch_schema.clone(), vec![vec![]]).ok()?;
    let _registration =
        ScopedTableRegistration::register(ctx, tmp_table_name, Arc::new(empty)).ok()?;

    let plan = ctx
        .sql(proj_sql)
        .await
        .ok()
        .map(|dataframe| dataframe.logical_plan().clone());
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

pub(crate) async fn prepare_post_projection(
    ctx: &SessionContext,
    projection_sql: &str,
    input_table: &str,
    input_schema: &SchemaRef,
    what: &str,
) -> Result<(PostProjectionCache, SchemaRef), DbError> {
    if let Some(compiled) =
        try_compile_post_projection(ctx, projection_sql, input_table, input_schema).await
    {
        let schema = Arc::clone(&compiled.output_schema);
        return Ok((
            PostProjectionCache {
                compiled: Some(compiled),
                compile_failed: false,
                sql_cache: None,
            },
            schema,
        ));
    }

    let sql_cache = LiveSqlCache::build(
        ctx,
        input_table,
        Arc::clone(input_schema),
        projection_sql,
        what,
    )
    .await?;
    let schema = sql_cache.physical.schema();
    Ok((
        PostProjectionCache {
            compiled: None,
            compile_failed: true,
            sql_cache: Some(sql_cache),
        },
        schema,
    ))
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

    #[tokio::test]
    async fn send_plan_classifies_first_unreachable_peer_as_not_ready() {
        let (sender, _receiver) = sender_with_reachable_peer_two().await;
        let outbound = vec![
            (3, ShuffleMessage::checkpointed("stage".into(), 2, batch(1))),
            (3, ShuffleMessage::checkpointed("stage".into(), 2, batch(2))),
        ];
        let (result, retry_plan) =
            send_shuffle_plan_retaining(&sender, 1, outbound.clone(), "test shuffle").await;
        let error = result.unwrap_err();

        assert!(matches!(error, DbError::ShuffleNotReady(_)));
        assert_eq!(retry_plan, Some(outbound));
    }

    #[tokio::test]
    async fn send_plan_prioritizes_terminal_error_without_admission() {
        let (sender, _receiver) = sender_with_reachable_peer_two().await;
        let (result, retry_plan) = send_shuffle_plan_retaining(
            &sender,
            1,
            vec![
                (3, ShuffleMessage::checkpointed("stage".into(), 2, batch(1))),
                (1, ShuffleMessage::checkpointed("stage".into(), 0, batch(2))),
            ],
            "test shuffle",
        )
        .await;
        let error = result.unwrap_err();

        assert!(matches!(error, DbError::ShuffleTerminal(_)));
        assert!(retry_plan.is_none());
    }

    #[tokio::test]
    async fn send_plan_completes_peer_groups_and_preserves_peer_order() {
        let (sender, receiver) = sender_with_reachable_peer_two().await;
        let (result, retry_plan) = send_shuffle_plan_retaining(
            &sender,
            1,
            vec![
                (3, ShuffleMessage::checkpointed("right".into(), 2, batch(2))),
                (2, ShuffleMessage::checkpointed("left".into(), 1, batch(1))),
                (
                    2,
                    ShuffleMessage::Frontier {
                        stage: "left".into(),
                        watermark: Some(10),
                        idle: false,
                    },
                ),
            ],
            "join shuffle",
        )
        .await;
        let error = result.unwrap_err();

        assert!(matches!(error, DbError::ShufflePartialSend(_)));
        assert!(error.requires_pipeline_recovery());
        assert!(retry_plan.is_none());
        let first = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
            .await
            .expect("admitted data frame was not delivered")
            .expect("shuffle receiver closed");
        let second = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
            .await
            .expect("admitted frontier frame was not delivered")
            .expect("shuffle receiver closed");
        assert!(matches!(first.message(), ShuffleMessage::Data { .. }));
        assert!(matches!(second.message(), ShuffleMessage::Frontier { .. }));
    }

    #[tokio::test]
    async fn send_plan_classifies_invalid_route_as_terminal() {
        let (sender, _receiver) = sender_with_reachable_peer_two().await;
        let (result, retry_plan) = send_shuffle_plan_retaining(
            &sender,
            1,
            vec![(1, ShuffleMessage::checkpointed("stage".into(), 0, batch(1)))],
            "test shuffle",
        )
        .await;
        let error = result.unwrap_err();

        assert!(matches!(error, DbError::ShuffleTerminal(_)));
        assert!(error.requires_pipeline_halt());
        assert!(retry_plan.is_none());
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

    pub(crate) async fn initialize(&mut self, input_schema: &SchemaRef) -> Result<(), DbError> {
        let Some(projection_sql) = self.projection_sql.as_deref() else {
            return Ok(());
        };
        if self.cache.compiled.is_some() || self.cache.sql_cache.is_some() {
            return Ok(());
        }
        let (cache, _) = prepare_post_projection(
            &self.ctx,
            projection_sql,
            self.tmp_table_name,
            input_schema,
            &self.op_name,
        )
        .await?;
        self.cache = cache;
        Ok(())
    }

    pub(crate) fn is_initialized(&self) -> bool {
        self.projection_sql.is_none()
            || self.cache.compiled.is_some()
            || self.cache.sql_cache.is_some()
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
    use laminar_sql::temporal::{TemporalJoinKind, TemporalProbeSchedule};
    use laminar_sql::translator::TemporalJoinTranslatorConfig;

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

    #[tokio::test]
    async fn compiled_and_cached_post_projection_preserve_filtered_weights_identically() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
            Field::new(
                laminar_core::changelog::WEIGHT_COLUMN,
                DataType::Int64,
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![1, -1, 2])),
            ],
        )
        .unwrap();
        let sql = "SELECT value + 1 AS adjusted, \"__weight\" AS \"__weight\" \
                   FROM __weighted_post_projection WHERE id >= 2";

        let mut compiled_cache = PostProjectionCache::default();
        let compiled = apply_post_projection(
            &SessionContext::new(),
            "weighted_compiled_projection",
            "__weighted_post_projection",
            Some(sql),
            &mut compiled_cache,
            vec![batch.clone()],
        )
        .await
        .unwrap();
        assert!(compiled_cache.compiled.is_some());
        assert!(compiled_cache.sql_cache.is_none());

        let mut cached_sql = PostProjectionCache {
            compiled: None,
            compile_failed: true,
            sql_cache: None,
        };
        let interpreted = apply_post_projection(
            &SessionContext::new(),
            "weighted_cached_projection",
            "__weighted_post_projection",
            Some(sql),
            &mut cached_sql,
            vec![batch],
        )
        .await
        .unwrap();
        assert!(cached_sql.compiled.is_none());
        assert!(cached_sql.sql_cache.is_some());

        for output in [&compiled, &interpreted] {
            assert_eq!(output.len(), 1);
            assert_eq!(
                output[0].schema().field(1).name(),
                laminar_core::changelog::WEIGHT_COLUMN
            );
            let adjusted = output[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let weights = output[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(adjusted.values(), &[21, 31]);
            assert_eq!(weights.values(), &[-1, 2]);
        }
    }

    #[tokio::test]
    async fn temporal_composite_output_name_is_independent_of_registration_key() {
        let ctx = SessionContext::new();
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("trade_id", DataType::Int64, false),
            Field::new("price_quotes", DataType::Int64, false),
        ]));
        let config = TemporalJoinTranslatorConfig {
            left_table: "trades".into(),
            right_table: "quotes".into(),
            left_key_columns: vec!["symbol".into()],
            right_key_columns: vec!["symbol".into()],
            left_time_column: "trade_time".into(),
            right_time_column: "quote_time".into(),
            join_kind: TemporalJoinKind::Left,
            probe_schedule: TemporalProbeSchedule::as_of(),
            probe_alias: None,
        };
        let sql = "SELECT t.trade_id + q.price FROM trades t LEFT JOIN quotes \
            FOR SYSTEM_TIME AS OF t.trade_time AS q ON t.symbol = q.symbol";

        let mut schemas = Vec::new();
        for table in ["__temporal_schema_first", "__temporal_schema_second"] {
            let projection =
                crate::sql_analysis::temporal_projection_sql_for_input(sql, &config, table)
                    .unwrap();
            let (_, schema) = prepare_post_projection(
                &ctx,
                &projection,
                table,
                &input_schema,
                "temporal projection test",
            )
            .await
            .unwrap();
            assert!(!ctx.table_exist(exact_table_reference(table)).unwrap());
            schemas.push(schema);
        }

        assert_eq!(schemas[0], schemas[1]);
        let name = schemas[0].field(0).name();
        assert!(!name.contains("__temporal_schema_first"));
        assert!(!name.contains("__temporal_schema_second"));
        assert!(!name.contains("__temporal_projection_input"), "{name}");

        let collision = "__temporal_schema_collision";
        let sentinel: Arc<dyn TableProvider> = Arc::new(
            datafusion::datasource::MemTable::try_new(Arc::clone(&input_schema), vec![vec![]])
                .unwrap(),
        );
        ctx.register_table(exact_table_reference(collision), Arc::clone(&sentinel))
            .unwrap();
        let projection =
            crate::sql_analysis::temporal_projection_sql_for_input(sql, &config, collision)
                .unwrap();
        let error = match prepare_post_projection(
            &ctx,
            &projection,
            collision,
            &input_schema,
            "temporal projection collision",
        )
        .await
        {
            Ok(_) => panic!("temporary planning table collision was admitted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("already exists"), "{error}");
        let restored = ctx
            .table_provider(exact_table_reference(collision))
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&restored, &sentinel));
    }
}
