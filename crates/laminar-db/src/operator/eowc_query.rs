//! EOWC (Emit On Window Close) operator backed by `CoreWindowState`.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionContext;

use crate::aggregate_state::apply_compiled_having;
#[cfg(feature = "cluster")]
use crate::core_window_state::PreparedCoreWindowTransition;
use crate::core_window_state::{CoreWindowCheckpoint, CoreWindowState};
use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
use crate::operator::capability::{
    ManagedStateContract, OperatorCapability, OperatorImplementation,
};
#[cfg(feature = "cluster")]
use crate::operator::sql_query::ClusterShuffleConfig;
#[cfg(feature = "cluster")]
use crate::operator_graph::ManagedVnodeTransition;
use crate::operator_graph::{
    try_evaluate_compiled, GraphOperator, ManagedStateAccountingSnapshot, OperatorCheckpoint,
};
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::WindowOperatorConfig;

#[cfg(feature = "cluster")]
enum CoreWindowTransitionCleanup {
    Aborted(PreparedCoreWindowTransition),
    Published(PreparedCoreWindowTransition),
}

/// EOWC query operator: suppresses intermediate results and emits only
/// when windows close.
pub(crate) struct EowcQueryOperator {
    op_name: Arc<str>,
    sql: Arc<str>,
    emit_clause: Option<EmitClause>,
    window_config: Option<WindowOperatorConfig>,
    ctx: SessionContext,
    task_ctx: Arc<TaskContext>,
    capability: OperatorCapability,
    state: Option<Box<CoreWindowState>>,
    pending_restore: Option<CoreWindowCheckpoint>,
    prom: Option<Arc<EngineMetrics>>,
    #[cfg(feature = "cluster")]
    cluster_scope: Option<ClusterShuffleConfig>,
    #[cfg(feature = "cluster")]
    prepared_vnode_transition: Option<PreparedCoreWindowTransition>,
    #[cfg(feature = "cluster")]
    vnode_transition_cleanup: Option<CoreWindowTransitionCleanup>,
}

impl EowcQueryOperator {
    pub fn new(
        name: &str,
        sql: &str,
        emit_clause: Option<EmitClause>,
        window_config: Option<WindowOperatorConfig>,
        ctx: SessionContext,
        prom: Option<Arc<EngineMetrics>>,
    ) -> Self {
        let task_ctx = ctx.task_ctx();
        let capability = if window_config.as_ref().is_some_and(|config| {
            matches!(
                config.window_type,
                laminar_sql::translator::WindowType::Tumbling
            )
        }) {
            OperatorCapability::managed_global_tumbling_window()
        } else {
            OperatorCapability::fixed(OperatorImplementation::EowcQuery)
        };
        Self {
            op_name: Arc::from(name),
            sql: Arc::from(sql),
            emit_clause,
            window_config,
            ctx,
            task_ctx,
            capability,
            state: None,
            pending_restore: None,
            prom,
            #[cfg(feature = "cluster")]
            cluster_scope: None,
            #[cfg(feature = "cluster")]
            prepared_vnode_transition: None,
            #[cfg(feature = "cluster")]
            vnode_transition_cleanup: None,
        }
    }

    async fn initialize(&mut self) -> Result<(), DbError> {
        let cfg = self.window_config.as_ref().ok_or_else(|| {
            DbError::Unsupported(format!(
                "[LDB-1001] EOWC query '{}' requires a supported TUMBLE, HOP, or SESSION aggregate",
                self.op_name
            ))
        })?;
        let Some(mut window) =
            CoreWindowState::try_from_sql(&self.ctx, &self.sql, cfg, self.emit_clause.as_ref())
                .await?
        else {
            return Err(DbError::Unsupported(format!(
                "[LDB-1001] EOWC query '{}' is not a supported TUMBLE, HOP, or SESSION aggregate",
                self.op_name
            )));
        };

        window.attach_metrics(self.prom.clone());
        tracing::info!(
            query = %self.op_name,
            window_type = ?cfg.window_type,
            "EOWC operator: initialized core window state"
        );
        self.capability = if window.supports_managed_global_tumbling() {
            OperatorCapability::managed_global_tumbling_window()
        } else {
            OperatorCapability::fixed(OperatorImplementation::EowcQuery)
        };
        self.state = Some(Box::new(window));
        self.apply_pending_restore()?;
        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn attach_cluster_scope(&mut self, scope: ClusterShuffleConfig) {
        self.cluster_scope = Some(scope);
    }

    fn resolve_managed_capability(&mut self) {
        if self.capability.managed_state == Some(ManagedStateContract::CoreWindowV1)
            && !self
                .state
                .as_ref()
                .is_some_and(|window| window.supports_managed_global_tumbling())
        {
            self.capability = OperatorCapability::fixed(OperatorImplementation::EowcQuery);
        }
    }

    fn apply_pending_restore(&mut self) -> Result<(), DbError> {
        let Some(checkpoint) = self.pending_restore.take() else {
            return Ok(());
        };
        if let Err(error) = self.apply_checkpoint(&checkpoint) {
            // Keep recovery pending so a caller that mishandles the error cannot
            // process or checkpoint an empty/partially restored operator.
            self.pending_restore = Some(checkpoint);
            return Err(error);
        }
        Ok(())
    }

    fn apply_checkpoint(&mut self, checkpoint: &CoreWindowCheckpoint) -> Result<(), DbError> {
        let window = self.state.as_mut().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "EOWC CoreWindow restore for '{}' targeted uninitialized state",
                self.op_name
            ))
        })?;
        let previous = window.checkpoint_windows().map_err(|error| {
            DbError::Checkpoint(format!(
                "EOWC CoreWindow restore snapshot for '{}': {error}",
                self.op_name
            ))
        })?;
        if let Err(apply_error) = window.restore_windows(checkpoint) {
            window
                .restore_windows(&previous)
                .map_err(|rollback_error| {
                    DbError::Checkpoint(format!(
                        "EOWC CoreWindow restore for '{}' failed: {apply_error}; \
                         rollback also failed: {rollback_error}",
                        self.op_name
                    ))
                })?;
            return Err(DbError::Checkpoint(format!(
                "EOWC CoreWindow restore for '{}': {apply_error}",
                self.op_name
            )));
        }
        Ok(())
    }

    fn core_window_apply_error(op_name: &str, phase: &str, error: DbError) -> DbError {
        if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
            return error;
        }
        DbError::StatefulOperatorPartialApply(format!(
            "managed CoreWindow '{op_name}' {phase} failed after window state mutation began; recovery from the committed checkpoint is required: {error}"
        ))
    }

    async fn process_core_window(
        cw: &mut CoreWindowState,
        inputs: &[RecordBatch],
        watermark: i64,
        op_name: &str,
        ctx: &SessionContext,
        task_ctx: &Arc<TaskContext>,
        recovery_fenced: bool,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let now_filtered = cw.apply_dynamic_now_filter(ctx, inputs, watermark)?;
        let inputs: &[RecordBatch] = now_filtered.as_deref().unwrap_or(inputs);

        let pre_agg_batches = if let Some(proj) = cw.compiled_projection() {
            match try_evaluate_compiled(proj, inputs) {
                Ok(result) => result,
                Err(e) => {
                    tracing::debug!(
                        query = %op_name,
                        error = %e,
                        "EOWC compiled pre-agg failed, falling back to cached plan"
                    );
                    if let Some(physical) = cw.cached_pre_agg_physical() {
                        super::execute_cached_physical(task_ctx.clone(), op_name, physical).await?
                    } else {
                        return Err(DbError::Pipeline(format!(
                            "[LDB-8051] EOWC query '{op_name}': compiled pre-agg failed and no cached plan: {e}"
                        )));
                    }
                }
            }
        } else if let Some(physical) = cw.cached_pre_agg_physical() {
            super::execute_cached_physical(task_ctx.clone(), op_name, physical).await?
        } else {
            return Err(DbError::Pipeline(format!(
                "[LDB-8050] EOWC query '{op_name}': no compiled projection or cached plan"
            )));
        };

        for batch in &pre_agg_batches {
            if let Err(error) = cw.update_batch(batch) {
                return Err(if recovery_fenced {
                    Self::core_window_apply_error(op_name, "state update", error)
                } else {
                    error
                });
            }
        }

        let having_filter = cw.having_filter().cloned();
        let having_sql = cw.having_sql().map(String::from);
        let mut batches = cw.close_windows(watermark).map_err(|error| {
            if recovery_fenced {
                Self::core_window_apply_error(op_name, "window close", error)
            } else {
                error
            }
        })?;

        if let Some(ref filter) = having_filter {
            batches = apply_compiled_having(&batches, filter).map_err(|error| {
                if recovery_fenced {
                    Self::core_window_apply_error(op_name, "HAVING evaluation", error)
                } else {
                    error
                }
            })?;
        } else if let Some(ref sql) = having_sql {
            batches = apply_having_via_sql(ctx, op_name, &batches, sql, cw.having_sql_cache_mut())
                .await
                .map_err(|error| {
                    if recovery_fenced {
                        Self::core_window_apply_error(op_name, "HAVING execution", error)
                    } else {
                        error
                    }
                })?;
        }

        Ok(batches)
    }

    fn encode_checkpoint(
        checkpoint: &CoreWindowCheckpoint,
        op_name: &str,
    ) -> Result<Vec<u8>, DbError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(checkpoint)
            .map(|bytes| bytes.to_vec())
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "EOWC checkpoint serialization for '{op_name}': {error}"
                ))
            })
    }

    #[cfg(feature = "cluster")]
    fn target_owns_global_vnode(
        &self,
        target: &laminar_core::checkpoint::CheckpointAssignmentFence,
    ) -> Result<bool, DbError> {
        let scope = self.cluster_scope.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "managed CoreWindow '{}' has no cluster assignment scope",
                self.op_name
            ))
        })?;
        let assignment = scope.registry.versioned_snapshot();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        if target.vnode_count != scope.registry.vnode_count()
            || target.assignment_version != assignment.version()
            || !target.matches_owner_map(&owners)
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' transition target does not match assignment {}",
                self.op_name,
                assignment.version()
            )));
        }
        Ok(assignment.owners()[0] == scope.self_id)
    }

    #[cfg(feature = "cluster")]
    fn preflight_managed_base<'a>(
        window: &CoreWindowState,
        bytes: &'a [u8],
    ) -> Result<crate::core_window_state::PreflightedCoreWindowArchive<'a>, DbError> {
        window.preflight_managed_tumbling_bytes(bytes)
    }
}

#[async_trait]
impl GraphOperator for EowcQueryOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        self.capability
    }

    fn managed_state_accounting(&self) -> Option<ManagedStateAccountingSnapshot> {
        if self.capability.managed_state != Some(ManagedStateContract::CoreWindowV1) {
            return None;
        }
        let window = self.state.as_ref()?;
        #[cfg(feature = "cluster")]
        let (prepared, retired) = {
            let staged = self
                .prepared_vnode_transition
                .as_ref()
                .map_or(0, PreparedCoreWindowTransition::accounted_state_bytes);
            match self.vnode_transition_cleanup.as_ref() {
                Some(CoreWindowTransitionCleanup::Aborted(cleanup)) => {
                    (staged.saturating_add(cleanup.accounted_state_bytes()), 0)
                }
                Some(CoreWindowTransitionCleanup::Published(cleanup)) => {
                    (staged, cleanup.accounted_state_bytes())
                }
                None => (staged, 0),
            }
        };
        #[cfg(not(feature = "cluster"))]
        let (prepared, retired) = (0, 0);
        Some(ManagedStateAccountingSnapshot {
            live: window.accounted_state_bytes(),
            prepared,
            retired,
        })
    }

    async fn initialize_managed_state(&mut self) -> Result<(), DbError> {
        if self.state.is_none() {
            self.initialize().await?;
        }
        self.resolve_managed_capability();
        Ok(())
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let watermark = watermarks.first().copied().unwrap_or(i64::MIN);
        let input_batches = inputs.first().map_or(&[][..], Vec::as_slice);

        if self.state.is_none() {
            self.initialize().await?;
        } else {
            // A failed deferred restore remains pending. Retrying it here
            // prevents processing against empty state if the first error was ignored.
            self.apply_pending_restore()?;
        }
        self.resolve_managed_capability();

        let window = self.state.as_mut().ok_or_else(|| {
            DbError::Pipeline(format!(
                "EOWC query '{}': state not initialized",
                self.op_name
            ))
        })?;
        let recovery_fenced =
            self.capability.managed_state == Some(ManagedStateContract::CoreWindowV1);
        Self::process_core_window(
            window,
            input_batches,
            watermark,
            &self.op_name,
            &self.ctx,
            &self.task_ctx,
            recovery_fenced,
        )
        .await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        if self.state.is_none() {
            let Some(checkpoint) = self.pending_restore.as_ref() else {
                return Ok(None);
            };
            let data = Self::encode_checkpoint(checkpoint, &self.op_name)?;
            return Ok(Some(OperatorCheckpoint { data }));
        }
        // Never publish a checkpoint while recovery is unapplied.
        self.apply_pending_restore()?;
        let checkpoint = self
            .state
            .as_mut()
            .expect("EOWC state was checked above")
            .checkpoint_windows()?;
        let data = Self::encode_checkpoint(&checkpoint, &self.op_name)?;
        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let checkpoint =
            rkyv::from_bytes::<CoreWindowCheckpoint, rkyv::rancor::Error>(&checkpoint.data)
                .map_err(|e| {
                    DbError::Checkpoint(format!(
                        "EOWC checkpoint deserialization for '{}': {e}",
                        self.op_name
                    ))
                })?;

        if self.state.is_none() {
            self.pending_restore = Some(checkpoint);
        } else {
            self.apply_checkpoint(&checkpoint)?;
        }

        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_by_vnode(
        &mut self,
        required_vnodes: &[u32],
        vnode_count: u32,
    ) -> Result<
        Option<std::collections::HashMap<u32, crate::checkpoint_coordinator::StagedSlice>>,
        DbError,
    > {
        let scope = self.cluster_scope.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "managed CoreWindow '{}' has no cluster assignment scope",
                self.op_name
            ))
        })?;
        let assignment = scope.registry.versioned_snapshot();
        let expected = if assignment.owners()[0] == scope.self_id {
            &[0_u32][..]
        } else {
            &[][..]
        };
        if vnode_count != scope.registry.vnode_count()
            || required_vnodes != expected
            || required_vnodes.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' capture roster {required_vnodes:?} does not match vnode-zero ownership for vnode_count {vnode_count}",
                self.op_name
            )));
        }
        if required_vnodes.is_empty() {
            return Ok(None);
        }
        if self.capability.managed_state != Some(ManagedStateContract::CoreWindowV1) {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow capture targeted unsupported operator '{}'",
                self.op_name
            )));
        }
        let Some(window) = self.state.as_mut() else {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow capture targeted uninitialized operator '{}'",
                self.op_name
            )));
        };
        let checkpoint = window.checkpoint_windows()?;
        let bytes = Self::encode_checkpoint(&checkpoint, &self.op_name)?;
        let mut captured = std::collections::HashMap::new();
        captured.try_reserve(1).map_err(|error| {
            DbError::Checkpoint(format!(
                "managed CoreWindow capture roster reserve failed: {error}"
            ))
        })?;
        captured.insert(
            0,
            crate::checkpoint_coordinator::StagedSlice::Bytes(bytes::Bytes::from(bytes)),
        );
        Ok(Some(captured))
    }

    #[cfg(feature = "cluster")]
    fn prepare_vnode_transition(
        &mut self,
        transition: ManagedVnodeTransition<'_>,
    ) -> Result<(), DbError> {
        if self.prepared_vnode_transition.is_some() || self.vnode_transition_cleanup.is_some() {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' already owns vnode transition state",
                self.op_name
            )));
        }
        if self.capability.managed_state != Some(ManagedStateContract::CoreWindowV1) {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow transition targeted unsupported operator '{}'",
                self.op_name
            )));
        }
        if transition.revoked.len() > 1
            || transition.revoked.iter().any(|vnode| *vnode != 0)
            || transition.restores.len() > 1
            || transition.restores.iter().any(|restore| restore.vnode != 0)
        {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow '{}' transition is not scoped exactly to vnode zero",
                self.op_name
            )));
        }
        let target_owns_global = self.target_owns_global_vnode(transition.target)?;
        let Some(window) = self.state.as_ref() else {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow transition targeted uninitialized operator '{}'",
                self.op_name
            )));
        };
        let prepared = if target_owns_global {
            if !transition.revoked.is_empty() {
                return Err(DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' vnode-zero acquisition cannot also revoke vnode zero",
                    self.op_name
                )));
            }
            let restore = transition.restores.first().ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' acquisition is missing its vnode-zero FULL base",
                    self.op_name
                ))
            })?;
            if !restore.deltas.is_empty() {
                return Err(DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' accepts FULL vnode-zero images only",
                    self.op_name
                )));
            }
            let archive = Self::preflight_managed_base(window, restore.base)?;
            window.prepare_managed_tumbling_restore(&archive)?
        } else {
            if !transition.restores.is_empty() || !transition.revoked.contains(&0) {
                return Err(DbError::Checkpoint(format!(
                    "managed CoreWindow '{}' owner exit requires exact vnode-zero revoke without restore",
                    self.op_name
                )));
            }
            window.prepare_managed_tumbling_empty()
        };
        self.prepared_vnode_transition = Some(prepared);
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn abort_vnode_transition(&mut self) {
        let Some(prepared) = self.prepared_vnode_transition.take() else {
            return;
        };
        assert!(
            self.vnode_transition_cleanup.is_none(),
            "managed CoreWindow cleanup must finish before abort"
        );
        self.vnode_transition_cleanup = Some(CoreWindowTransitionCleanup::Aborted(prepared));
    }

    #[cfg(feature = "cluster")]
    fn publish_vnode_transition(&mut self) {
        let prepared = self
            .prepared_vnode_transition
            .take()
            .expect("managed CoreWindow transition must be prepared before publication");
        assert!(
            self.vnode_transition_cleanup.is_none(),
            "managed CoreWindow cleanup must finish before publication"
        );
        let window = self
            .state
            .as_mut()
            .expect("managed CoreWindow publication targeted uninitialized state");
        let retired = window.publish_managed_tumbling_transition(prepared);
        self.vnode_transition_cleanup = Some(CoreWindowTransitionCleanup::Published(retired));
    }

    #[cfg(feature = "cluster")]
    fn finish_vnode_transition(&mut self) {
        drop(self.vnode_transition_cleanup.take());
    }
}

/// Apply a HAVING predicate via SQL using a cached physical plan.
async fn apply_having_via_sql(
    ctx: &SessionContext,
    query_name: &str,
    batches: &[RecordBatch],
    having_sql: &str,
    cache: &mut Option<super::HavingSqlCache>,
) -> Result<Vec<RecordBatch>, DbError> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    if cache.is_none() {
        let temp_name = format!("_having_{}", query_name.replace(['-', ' '], "_"));
        *cache = Some(
            super::HavingSqlCache::build(ctx, &temp_name, batches[0].schema(), having_sql).await?,
        );
    }
    cache
        .as_ref()
        .expect("just initialized")
        .apply(query_name, batches.to_vec())
        .await
}

#[cfg(test)]
mod core_tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    #[cfg(feature = "cluster")]
    use laminar_core::cluster::control::LeaseDeadline;
    #[cfg(feature = "cluster")]
    use laminar_core::state::{NodeId, VnodeRegistry};
    use std::time::Duration;

    const AGG_SQL: &str = "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol";

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("ts", DataType::Int64, false),
        ]))
    }

    fn test_batch(ts_values: Vec<i64>) -> RecordBatch {
        let n = ts_values.len();
        let symbols: Vec<&str> = (0..n)
            .map(|i| if i % 2 == 0 { "AAPL" } else { "GOOG" })
            .collect();
        #[allow(clippy::cast_precision_loss)]
        let prices: Vec<f64> = (0..n).map(|i| (i as f64 + 1.0) * 100.0).collect();
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(StringArray::from(symbols)),
                Arc::new(Float64Array::from(prices)),
                Arc::new(Int64Array::from(ts_values)),
            ],
        )
        .unwrap()
    }

    fn aggregate_context() -> SessionContext {
        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);
        let empty = MemTable::try_new(test_schema(), vec![vec![]]).unwrap();
        ctx.register_table("trades", Arc::new(empty)).unwrap();
        ctx
    }

    fn test_window_config() -> WindowOperatorConfig {
        WindowOperatorConfig {
            window_type: laminar_sql::translator::WindowType::Tumbling,
            time_column: "ts".to_string(),
            size: Duration::from_secs(60),
            slide: None,
            gap: None,
            offset_ms: 0,
            allowed_lateness: Duration::ZERO,
            emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        }
    }

    fn checkpoint_from_core(checkpoint: &CoreWindowCheckpoint) -> OperatorCheckpoint {
        OperatorCheckpoint {
            data: rkyv::to_bytes::<rkyv::rancor::Error>(checkpoint)
                .unwrap()
                .to_vec(),
        }
    }

    fn core_from_checkpoint(checkpoint: &OperatorCheckpoint) -> CoreWindowCheckpoint {
        rkyv::from_bytes::<CoreWindowCheckpoint, rkyv::rancor::Error>(&checkpoint.data).unwrap()
    }

    #[cfg(feature = "cluster")]
    async fn single_owner_cluster_scope() -> (
        ClusterShuffleConfig,
        laminar_core::checkpoint::CheckpointAssignmentFence,
    ) {
        let self_id = NodeId(1);
        let incarnation = uuid::Uuid::from_u128(1);
        let registry = Arc::new(VnodeRegistry::single_owner(1, self_id));
        let receiver = Arc::new(
            laminar_core::shuffle::ShuffleReceiver::bind(
                self_id.0,
                "127.0.0.1:0".parse().unwrap(),
                incarnation,
            )
            .await
            .unwrap(),
        );
        let sender = Arc::new(laminar_core::shuffle::ShuffleSender::new(
            self_id.0,
            incarnation,
        ));
        let process_deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
        receiver
            .install_process_lease_deadline(Arc::clone(&process_deadline))
            .unwrap();
        sender
            .install_process_lease_deadline(process_deadline)
            .unwrap();
        let owners = [self_id.0];
        let target = laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
            registry.assignment_version(),
            &owners,
            vec![laminar_core::checkpoint::CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: incarnation,
            }],
        )
        .unwrap();
        sender.install_assignment_fence(&target, &owners).unwrap();
        receiver.install_assignment_fence(&target, &owners).unwrap();
        (
            ClusterShuffleConfig {
                registry,
                sender,
                receiver,
                self_id,
            },
            target,
        )
    }

    #[cfg(feature = "cluster")]
    async fn managed_core_window_operator(
        name: &str,
        updates: usize,
    ) -> (
        EowcQueryOperator,
        laminar_core::checkpoint::CheckpointAssignmentFence,
    ) {
        const SQL: &str = "SELECT SUM(price) AS total FROM trades";

        let ctx = aggregate_context();
        let mut operator = EowcQueryOperator::new(
            name,
            SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            ctx,
            None,
        );
        let (scope, target) = single_owner_cluster_scope().await;
        operator.attach_cluster_scope(scope);
        operator.initialize_managed_state().await.unwrap();
        assert_eq!(
            operator.cluster_capability().managed_state,
            Some(ManagedStateContract::CoreWindowV1)
        );
        for _ in 0..updates {
            operator
                .process(&[vec![test_batch(vec![100])]], &[i64::MIN])
                .await
                .unwrap();
        }
        (operator, target)
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn managed_core_window_transition_is_offside_and_accounted() {
        let (mut donor, _) = managed_core_window_operator("managed_donor", 1).await;
        let donor_slice = donor
            .checkpoint_by_vnode(&[0], 1)
            .unwrap()
            .expect("the vnode-zero owner must capture a full image")
            .remove(&0)
            .expect("the capture must name vnode zero");
        let crate::checkpoint_coordinator::StagedSlice::Bytes(donor_slice) = donor_slice else {
            panic!("managed CoreWindow capture must be a full image");
        };

        let (mut subject, target) = managed_core_window_operator("managed_subject", 2).await;
        let revoked = rustc_hash::FxHashSet::default();
        let before = subject.checkpoint().unwrap().unwrap().data;

        let mut corrupt = core_from_checkpoint(&OperatorCheckpoint {
            data: donor_slice.to_vec(),
        });
        corrupt.fingerprint = corrupt.fingerprint.wrapping_add(1);
        let corrupt = checkpoint_from_core(&corrupt);
        let corrupt_restores = [crate::operator_graph::ManagedVnodeRestore {
            vnode: 0,
            base: &corrupt.data,
            deltas: &[],
        }];
        let error = subject
            .prepare_vnode_transition(ManagedVnodeTransition {
                target: &target,
                revoked: &revoked,
                restores: &corrupt_restores,
            })
            .expect_err("borrowed fingerprint preflight must reject the image");
        assert!(
            error.to_string().contains("fingerprint mismatch"),
            "{error}"
        );
        assert_eq!(subject.checkpoint().unwrap().unwrap().data, before);
        assert_eq!(
            subject.managed_state_accounting().unwrap().prepared,
            0,
            "rejected preflight must not retain an off-side image"
        );

        let restores = [crate::operator_graph::ManagedVnodeRestore {
            vnode: 0,
            base: donor_slice.as_ref(),
            deltas: &[],
        }];
        let live_before = subject.managed_state_accounting().unwrap().live;
        subject
            .prepare_vnode_transition(ManagedVnodeTransition {
                target: &target,
                revoked: &revoked,
                restores: &restores,
            })
            .unwrap();
        let prepared = subject.managed_state_accounting().unwrap();
        assert_eq!(prepared.live, live_before);
        assert!(prepared.prepared > 0);
        assert_eq!(prepared.retired, 0);

        subject.abort_vnode_transition();
        let aborted = subject.managed_state_accounting().unwrap();
        assert_eq!(aborted.live, live_before);
        assert!(aborted.prepared > 0);
        assert_eq!(aborted.retired, 0);
        subject.finish_vnode_transition();
        assert_eq!(subject.checkpoint().unwrap().unwrap().data, before);
        let live_before = subject.managed_state_accounting().unwrap().live;
        assert_eq!(
            subject.managed_state_accounting().unwrap(),
            ManagedStateAccountingSnapshot {
                live: live_before,
                prepared: 0,
                retired: 0,
            }
        );

        subject
            .prepare_vnode_transition(ManagedVnodeTransition {
                target: &target,
                revoked: &revoked,
                restores: &restores,
            })
            .unwrap();
        let prepared_bytes = subject.managed_state_accounting().unwrap().prepared;
        subject.publish_vnode_transition();
        let published = subject.managed_state_accounting().unwrap();
        assert_eq!(published.live, prepared_bytes);
        assert_eq!(published.prepared, 0);
        assert_eq!(published.retired, live_before);
        subject.finish_vnode_transition();
        assert_eq!(subject.managed_state_accounting().unwrap().retired, 0);

        let restored = subject
            .checkpoint_by_vnode(&[0], 1)
            .unwrap()
            .unwrap()
            .remove(&0)
            .unwrap();
        let crate::checkpoint_coordinator::StagedSlice::Bytes(restored) = restored else {
            panic!("managed CoreWindow capture must remain full-only");
        };
        assert_eq!(restored, donor_slice);
    }

    async fn core_window_operator() -> EowcQueryOperator {
        let ctx = aggregate_context();
        let config = test_window_config();
        let state =
            CoreWindowState::try_from_sql(&ctx, AGG_SQL, &config, Some(&EmitClause::OnWindowClose))
                .await
                .unwrap()
                .unwrap();
        let mut op = EowcQueryOperator::new(
            "test_core_restore",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(config),
            ctx,
            None,
        );
        op.state = Some(Box::new(state));
        op.process(&[vec![test_batch(vec![100])]], &[i64::MIN])
            .await
            .unwrap();
        op
    }

    #[tokio::test]
    async fn corrupt_core_window_payload_rolls_back_all_state() {
        let mut op = core_window_operator().await;
        let before = op.checkpoint().unwrap().unwrap();
        let mut corrupt = core_from_checkpoint(&before);
        corrupt.high_watermark_ms = 1234;
        corrupt.windows[0].groups[0].key = vec![0xff, 0x00, 0x7f];

        let error = op.restore(checkpoint_from_core(&corrupt)).unwrap_err();

        assert!(error.to_string().contains("CoreWindow restore"));
        assert!(error.requires_pipeline_recovery());
        assert_eq!(op.checkpoint().unwrap().unwrap().data, before.data);
    }

    #[test]
    fn test_eowc_operator_creation() {
        let ctx = laminar_sql::create_session_context();
        let op = EowcQueryOperator::new(
            "test_eowc",
            "SELECT symbol, SUM(price) FROM trades GROUP BY symbol",
            Some(EmitClause::OnWindowClose),
            None,
            ctx,
            None,
        );
        assert_eq!(&*op.op_name, "test_eowc");
        assert!(op.state.is_none());
    }

    #[test]
    fn test_eowc_checkpoint_uninit_returns_none() {
        let ctx = laminar_sql::create_session_context();
        let mut op = EowcQueryOperator::new(
            "test_eowc",
            "SELECT * FROM trades",
            Some(EmitClause::OnWindowClose),
            None,
            ctx,
            None,
        );
        let cp = op.checkpoint().unwrap();
        assert!(cp.is_none());
    }

    #[tokio::test]
    async fn test_eowc_process_empty_inputs() {
        let ctx = aggregate_context();
        let mut op = EowcQueryOperator::new(
            "test_eowc",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            ctx,
            None,
        );
        op.initialize_managed_state().await.unwrap();

        let result = op.process(&[vec![]], &[0]).await.unwrap();
        assert!(result.is_empty());
    }
}
