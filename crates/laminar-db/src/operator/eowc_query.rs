//! EOWC (Emit On Window Close) operator backed by `CoreWindowState`.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionContext;
use laminar_core::state::KeyGroupCount;

use crate::core_window_state::{CoreWindowState, CoreWindowVnodeCheckpoint};
#[cfg(feature = "cluster")]
use crate::core_window_state::{
    PreparedCoreWindowVnodeTransition, RetiredCoreWindowVnodeTransition,
};
use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
use crate::operator::capability::{ManagedStateContract, OperatorCapability};
#[cfg(feature = "cluster")]
use crate::operator::sql_query::ClusterShuffleConfig;
#[cfg(feature = "cluster")]
use crate::operator_graph::ManagedVnodeTransition;
use crate::operator_graph::{
    try_evaluate_compiled, EncodedStateFrame, GraphOperator, ManagedStateAccountingSnapshot,
    OperatorCheckpoint, StateFrameCapture,
};
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::WindowOperatorConfig;

#[cfg(feature = "cluster")]
enum CoreWindowTransitionCleanup {
    Aborted(PreparedCoreWindowVnodeTransition),
    Published(RetiredCoreWindowVnodeTransition),
}

const OPERATOR_CHECKPOINT_VERSION: u8 = 1;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct EowcOperatorCheckpoint {
    version: u8,
    high_watermark_ms: i64,
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
    key_group_count: KeyGroupCount,
    capability: OperatorCapability,
    state: Option<Box<CoreWindowState>>,
    whole_restore_applied: bool,
    prom: Option<Arc<EngineMetrics>>,
    #[cfg(feature = "cluster")]
    cluster_scope: Option<ClusterShuffleConfig>,
    #[cfg(feature = "cluster")]
    prepared_vnode_transition: Option<PreparedCoreWindowVnodeTransition>,
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
        key_group_count: KeyGroupCount,
        prom: Option<Arc<EngineMetrics>>,
    ) -> Self {
        let task_ctx = ctx.task_ctx();
        Self {
            op_name: Arc::from(name),
            sql: Arc::from(sql),
            emit_clause,
            window_config,
            ctx,
            task_ctx,
            key_group_count,
            capability: OperatorCapability::managed_core_window(),
            state: None,
            whole_restore_applied: false,
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
        let Some(mut window) = CoreWindowState::try_from_sql(
            &self.ctx,
            &self.sql,
            cfg,
            self.emit_clause.as_ref(),
            self.key_group_count,
        )
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
        self.state = Some(Box::new(window));
        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn attach_cluster_scope(&mut self, scope: ClusterShuffleConfig) {
        self.cluster_scope = Some(scope);
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
            if let Err(error) = cw.update_batch_for_vnode(batch, None) {
                return Err(Self::core_window_apply_error(
                    op_name,
                    "state update",
                    error,
                ));
            }
        }

        let batches = cw
            .close_windows(watermark)
            .map_err(|error| Self::core_window_apply_error(op_name, "window close", error))?;

        Ok(batches)
    }

    fn encode_vnode_checkpoint(
        checkpoint: &CoreWindowVnodeCheckpoint,
        op_name: &str,
        vnode: u32,
        max_encoded_bytes: usize,
    ) -> Result<EncodedStateFrame, DbError> {
        let writer = rkyv::ser::writer::IoWriter::new(
            laminar_core::serialization::BoundedBytesWriter::new(max_encoded_bytes),
        );
        rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(checkpoint, writer)
            .map(|bytes| EncodedStateFrame::from_vec(bytes.into_inner().into_vec()))
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "CoreWindow '{op_name}' vnode {vnode} checkpoint exceeded its {max_encoded_bytes}-byte archive limit: {error}"
                ))
            })
    }

    #[cfg(feature = "cluster")]
    fn validate_assignment_target(
        &self,
        target: &laminar_core::checkpoint::CheckpointAssignmentFence,
    ) -> Result<(), DbError> {
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
        Ok(())
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
                .map_or(0, PreparedCoreWindowVnodeTransition::accounted_state_bytes);
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
        }

        let window = self.state.as_mut().ok_or_else(|| {
            DbError::Pipeline(format!(
                "EOWC query '{}': state not initialized",
                self.op_name
            ))
        })?;
        Self::process_core_window(
            window,
            input_batches,
            watermark,
            &self.op_name,
            &self.ctx,
            &self.task_ctx,
        )
        .await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        let Some(window) = self.state.as_ref() else {
            return Ok(None);
        };
        let checkpoint = EowcOperatorCheckpoint {
            version: OPERATOR_CHECKPOINT_VERSION,
            high_watermark_ms: window.high_watermark_ms(),
        };
        let data = rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "EOWC whole checkpoint serialization for '{}': {error}",
                    self.op_name
                ))
            })?
            .to_vec();
        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let checkpoint =
            rkyv::from_bytes::<EowcOperatorCheckpoint, rkyv::rancor::Error>(&checkpoint.data)
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "EOWC whole checkpoint deserialization for '{}': {error}",
                        self.op_name
                    ))
                })?;
        if checkpoint.version != OPERATOR_CHECKPOINT_VERSION {
            return Err(DbError::Checkpoint(format!(
                "EOWC whole checkpoint for '{}' has unsupported version {}",
                self.op_name, checkpoint.version
            )));
        }
        let window = self.state.as_mut().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "EOWC whole restore for '{}' requires initialized state",
                self.op_name
            ))
        })?;
        window
            .restore_high_watermark_ms(checkpoint.high_watermark_ms)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "EOWC whole restore for '{}': {error}",
                    self.op_name
                ))
            })?;
        self.whole_restore_applied = true;
        Ok(())
    }

    fn checkpoint_vnodes(
        &mut self,
        required_vnodes: &[u32],
        vnode_count: u32,
        max_capture_bytes: u64,
    ) -> Result<Option<Vec<crate::operator_graph::CapturedVnodeState>>, DbError> {
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
        let vnode_captures =
            window.capture_checkpoint_vnodes(required_vnodes, vnode_count, max_capture_bytes)?;
        let mut captured = Vec::with_capacity(vnode_captures.len());
        for (vnode, capture) in vnode_captures {
            let retained_bytes = u64::try_from(capture.retained_bytes()).unwrap_or(u64::MAX);
            let op_name = Arc::clone(&self.op_name);
            let state = StateFrameCapture::deferred(retained_bytes, move |max_encoded_bytes| {
                let checkpoint = capture.encode(max_encoded_bytes)?;
                let intermediate_bytes = checkpoint.retained_serialization_bytes()?;
                let archive_budget = max_encoded_bytes
                    .checked_sub(intermediate_bytes)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "CoreWindow '{op_name}' vnode {vnode} intermediate checkpoint exhausted its frame budget"
                        ))
                    })?;
                Self::encode_vnode_checkpoint(&checkpoint, &op_name, vnode, archive_budget)
            });
            captured.push(crate::operator_graph::CapturedVnodeState {
                vnode,
                state: Some(state),
            });
        }
        Ok(Some(captured))
    }

    fn restore_vnode(&mut self, vnode: u32, vnode_count: u32, state: &[u8]) -> Result<(), DbError> {
        if !self.whole_restore_applied {
            return Err(DbError::Checkpoint(format!(
                "CoreWindow '{}' vnode restore requires its whole watermark frame",
                self.op_name
            )));
        }
        let window = self.state.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "managed CoreWindow vnode restore targeted uninitialized operator '{}'",
                self.op_name
            ))
        })?;
        let checkpoint = window.preflight_vnode_bytes(vnode, vnode_count, state)?;
        let checkpoint = rkyv::deserialize::<CoreWindowVnodeCheckpoint, rkyv::rancor::Error>(
            checkpoint.checkpoint,
        )
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "CoreWindow '{}' vnode {vnode} checkpoint deserialization: {error}",
                self.op_name
            ))
        })?;
        let restored_high_watermark_ms = window.high_watermark_ms();
        let window = self
            .state
            .as_mut()
            .expect("CoreWindow restore state was checked above");
        window
            .restore_vnode(vnode, vnode_count, checkpoint)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "CoreWindow '{}' vnode {vnode} restore: {error}",
                    self.op_name
                ))
            })?;
        window
            .restore_high_watermark_ms(restored_high_watermark_ms)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "CoreWindow '{}' vnode {vnode} frontier validation: {error}",
                    self.op_name
                ))
            })
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
        self.validate_assignment_target(transition.target)?;
        let Some(window) = self.state.as_ref() else {
            return Err(DbError::Checkpoint(format!(
                "managed CoreWindow transition targeted uninitialized operator '{}'",
                self.op_name
            )));
        };
        window.validate_vnode_count(transition.target.vnode_count)?;

        let mut preflighted = Vec::new();
        preflighted
            .try_reserve_exact(transition.restores.len())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "CoreWindow '{}' could not reserve vnode restore metadata: {error}",
                    self.op_name
                ))
            })?;
        for restore in transition.restores {
            let state = window.preflight_vnode_bytes(
                restore.vnode,
                transition.target.vnode_count,
                restore.state,
            )?;
            preflighted.push((restore.vnode, state));
        }
        let owned_restores = preflighted.into_iter().map(|(vnode, state)| {
            let state = rkyv::deserialize::<CoreWindowVnodeCheckpoint, rkyv::rancor::Error>(
                state.checkpoint,
            )
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "CoreWindow '{}' vnode {vnode} transition deserialization: {error}",
                    self.op_name
                ))
            })?;
            Ok(crate::core_window_state::OwnedCoreWindowVnodeRestore { vnode, state })
        });
        let prepared = window.prepare_owned_vnode_transition(
            transition.target.vnode_count,
            owned_restores,
            transition.revoked,
        )?;
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
        let retired = window.publish_prepared_vnode_transition(prepared);
        self.vnode_transition_cleanup = Some(CoreWindowTransitionCleanup::Published(retired));
    }

    #[cfg(feature = "cluster")]
    fn finish_vnode_transition(&mut self) {
        match self.vnode_transition_cleanup.take() {
            Some(CoreWindowTransitionCleanup::Aborted(prepared)) => drop(prepared),
            Some(CoreWindowTransitionCleanup::Published(retired)) => {
                CoreWindowState::finish_vnode_transition(retired);
            }
            None => {}
        }
    }

    fn force_full_vnode_capture(&mut self) {
        if let Some(window) = self.state.as_mut() {
            window.force_full_vnode_capture();
        }
    }
}

#[cfg(test)]
mod core_tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
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

    fn key_groups() -> KeyGroupCount {
        KeyGroupCount::try_from(8_u32).unwrap()
    }

    fn materialize_capture(
        capture: crate::operator_graph::CapturedVnodeState,
    ) -> (u32, bytes::Bytes) {
        let state = capture.state.unwrap();
        let mut staged_bytes = state.retained_bytes();
        let bytes = state.materialize(&mut staged_bytes, u64::MAX).unwrap();
        (capture.vnode, bytes)
    }

    #[tokio::test]
    async fn grouped_window_restores_exact_vnode_frames_and_frontier() {
        let mut original = EowcQueryOperator::new(
            "managed_window",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            aggregate_context(),
            key_groups(),
            None,
        );
        original.initialize_managed_state().await.unwrap();
        original
            .process(&[vec![test_batch(vec![100, 200])]], &[10_000])
            .await
            .unwrap();

        let required = (0..u32::from(key_groups())).collect::<Vec<_>>();
        let captures = original
            .checkpoint_vnodes(&required, u32::from(key_groups()), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(captures.len(), required.len());
        let frames = captures
            .into_iter()
            .map(materialize_capture)
            .collect::<Vec<_>>();
        assert!(original
            .checkpoint_vnodes(&required, u32::from(key_groups()), u64::MAX)
            .unwrap()
            .unwrap()
            .is_empty());
        original.process(&[vec![]], &[20_000]).await.unwrap();
        assert!(original
            .checkpoint_vnodes(&required, u32::from(key_groups()), u64::MAX)
            .unwrap()
            .unwrap()
            .is_empty());
        let whole = original.checkpoint().unwrap().unwrap();

        let mut restored = EowcQueryOperator::new(
            "managed_window",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            aggregate_context(),
            key_groups(),
            None,
        );
        restored.initialize_managed_state().await.unwrap();
        assert!(restored
            .restore_vnode(frames[0].0, u32::from(key_groups()), &frames[0].1)
            .unwrap_err()
            .to_string()
            .contains("whole watermark frame"));
        restored.restore(whole).unwrap();
        assert_eq!(restored.state.as_ref().unwrap().high_watermark_ms(), 20_000);
        assert!(restored
            .restore_vnode(1, u32::from(key_groups()), &frames[0].1)
            .is_err());
        for (vnode, state) in &frames {
            restored
                .restore_vnode(*vnode, u32::from(key_groups()), state)
                .unwrap();
        }

        let expected = original.process(&[vec![]], &[60_000]).await.unwrap();
        let actual = restored.process(&[vec![]], &[60_000]).await.unwrap();
        assert_eq!(actual, expected);
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
            key_groups(),
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
            key_groups(),
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
            key_groups(),
            None,
        );
        op.initialize_managed_state().await.unwrap();

        let result = op.process(&[vec![]], &[0]).await.unwrap();
        assert!(result.is_empty());
    }
}
