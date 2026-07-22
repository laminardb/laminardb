//! EOWC (Emit On Window Close) operator: routes to `CoreWindowState`,
//! `IncrementalEowcState`, or raw-batch accumulation on first `process()`.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionContext;

use crate::aggregate_state::{apply_compiled_having, EowcStateCheckpoint};
use crate::core_window_state::{CoreWindowCheckpoint, CoreWindowState};
use crate::engine_metrics::EngineMetrics;
use crate::eowc_state::IncrementalEowcState;
use crate::error::DbError;
use crate::operator_graph::{try_evaluate_compiled, GraphOperator, OperatorCheckpoint};
use crate::sql_analysis::compute_closed_boundary;
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::WindowOperatorConfig;

const MAX_EOWC_ACCUMULATED_BYTES: usize = 256 * 1024 * 1024;

#[derive(
    serde::Serialize, serde::Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
enum EowcCheckpointEnvelope {
    CoreWindow(CoreWindowCheckpoint),
    EowcAgg(EowcStateCheckpoint),
    /// Non-aggregate path; empty `ipc` means no rows were buffered.
    Raw(RawCheckpoint),
}

#[derive(
    serde::Serialize, serde::Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
struct RawCheckpoint {
    ipc: Vec<u8>,
    last_closed_boundary: i64,
}

/// Lazy-initialized EOWC state, variant chosen on the first `process()` call.
enum EowcInnerState {
    Uninit,
    CoreWindow(Box<CoreWindowState>),
    EowcAgg(Box<IncrementalEowcState>),
    /// Non-aggregate path: accumulate batches, replay SQL when windows close.
    Raw {
        accumulated: Vec<RecordBatch>,
        last_closed_boundary: i64,
        accumulated_bytes: usize,
        // Built on first close-cycle; cached thereafter to avoid re-planning.
        sql_cache: Option<RawSqlCache>,
    },
}

/// User SQL with its source AST-rewritten to a private table; cached physical plan.
struct RawSqlCache(super::LiveSqlCache);

impl RawSqlCache {
    async fn build(
        ctx: &SessionContext,
        op_name: &str,
        original_sql: &str,
        source_schema: arrow::datatypes::SchemaRef,
    ) -> Result<Self, DbError> {
        let source = crate::sql_analysis::single_source_table(original_sql).ok_or_else(|| {
            DbError::Unsupported(format!(
                "[LDB-1001] non-aggregate EMIT ON WINDOW CLOSE on multi-source \
                 query '{op_name}' is not supported"
            ))
        })?;
        let temp_table = format!("_eowc_raw_{}", op_name.replace(['-', ' '], "_"));
        let rewritten = rewrite_source(original_sql, &source, &temp_table)?;
        super::LiveSqlCache::build(ctx, &temp_table, source_schema, &rewritten, "raw EOWC")
            .await
            .map(Self)
    }

    async fn apply(
        &self,
        op_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.0.apply(op_name, batches).await
    }
}

fn snapshot_raw(
    accumulated: &[RecordBatch],
    last_closed_boundary: i64,
) -> Result<RawCheckpoint, DbError> {
    let ipc = match accumulated.first() {
        None => Vec::new(),
        Some(first) => crate::mv_store::batches_to_ipc(&first.schema(), accumulated)?,
    };
    Ok(RawCheckpoint {
        ipc,
        last_closed_boundary,
    })
}

fn raw_batches_bytes(batches: &[RecordBatch]) -> Result<usize, DbError> {
    batches.iter().try_fold(0usize, |total, batch| {
        total
            .checked_add(batch.get_array_memory_size())
            .ok_or_else(|| DbError::Pipeline("raw EOWC state byte accounting overflow".into()))
    })
}

fn restore_raw(cp: &RawCheckpoint) -> Result<(Vec<RecordBatch>, usize), DbError> {
    if cp.ipc.is_empty() {
        return Ok((Vec::new(), 0));
    }
    if cp.ipc.len() > MAX_EOWC_ACCUMULATED_BYTES {
        return Err(DbError::Checkpoint(format!(
            "EOWC raw checkpoint is {} bytes, exceeding the {}-byte state limit",
            cp.ipc.len(),
            MAX_EOWC_ACCUMULATED_BYTES
        )));
    }
    let batches = crate::mv_store::ipc_to_batches(&cp.ipc)
        .map_err(|e| DbError::Checkpoint(format!("EOWC raw restore: {e}")))?;
    let bytes = raw_batches_bytes(&batches)
        .map_err(|e| DbError::Checkpoint(format!("EOWC raw restore accounting: {e}")))?;
    if bytes > MAX_EOWC_ACCUMULATED_BYTES {
        return Err(DbError::Checkpoint(format!(
            "EOWC raw checkpoint expands to {bytes} bytes, exceeding the {MAX_EOWC_ACCUMULATED_BYTES}-byte state limit"
        )));
    }
    Ok((batches, bytes))
}

/// Replace every unqualified `source` table reference in SQL with `temp`.
fn rewrite_source(sql: &str, source: &str, temp: &str) -> Result<String, DbError> {
    use sqlparser::ast::{Ident, ObjectName, SetExpr, Statement, TableFactor};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn unqualify(s: &str) -> &str {
        s.rsplit('.').next().unwrap_or(s)
    }
    fn walk_factor(f: &mut TableFactor, source: &str, temp: &str) {
        match f {
            TableFactor::Table { name, .. } => {
                let s = name.to_string();
                if unqualify(&s).eq_ignore_ascii_case(unqualify(source)) {
                    *name = ObjectName::from(vec![Ident::new(temp)]);
                }
            }
            TableFactor::Derived { subquery, .. } => walk_set(&mut subquery.body, source, temp),
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                walk_factor(&mut table_with_joins.relation, source, temp);
                for j in &mut table_with_joins.joins {
                    walk_factor(&mut j.relation, source, temp);
                }
            }
            _ => {}
        }
    }
    fn walk_set(s: &mut SetExpr, source: &str, temp: &str) {
        match s {
            SetExpr::Select(sel) => {
                for twj in &mut sel.from {
                    walk_factor(&mut twj.relation, source, temp);
                    for j in &mut twj.joins {
                        walk_factor(&mut j.relation, source, temp);
                    }
                }
            }
            SetExpr::Query(q) => walk_set(&mut q.body, source, temp),
            SetExpr::SetOperation { left, right, .. } => {
                walk_set(left, source, temp);
                walk_set(right, source, temp);
            }
            _ => {}
        }
    }

    let mut stmts = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|e| DbError::Pipeline(format!("raw EOWC sql parse: {e}")))?;
    for stmt in &mut stmts {
        if let Statement::Query(q) = stmt {
            walk_set(&mut q.body, source, temp);
        }
    }
    Ok(stmts
        .first()
        .map(std::string::ToString::to_string)
        .unwrap_or_default())
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
    state: EowcInnerState,
    pending_restore: Option<EowcCheckpointEnvelope>,
    prom: Option<Arc<EngineMetrics>>,
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
        Self {
            op_name: Arc::from(name),
            sql: Arc::from(sql),
            emit_clause,
            window_config,
            ctx,
            task_ctx,
            state: EowcInnerState::Uninit,
            pending_restore: None,
            prom,
        }
    }

    async fn initialize(&mut self) -> Result<(), DbError> {
        if let Some(ref cfg) = self.window_config {
            let emit_ref = self.emit_clause.as_ref();
            match CoreWindowState::try_from_sql(&self.ctx, &self.sql, cfg, emit_ref).await {
                Ok(Some(mut cw)) => {
                    cw.attach_metrics(self.prom.clone());
                    tracing::info!(
                        query = %self.op_name,
                        window_type = ?cfg.window_type,
                        "EOWC operator: routed to core window pipeline"
                    );
                    self.state = EowcInnerState::CoreWindow(Box::new(cw));
                    self.apply_pending_restore()?;
                    return Ok(());
                }
                Ok(None) => {}
                // Propagate wallclock misuse; other gaps fall through.
                Err(e @ DbError::Unsupported(_))
                    if {
                        let s = e.to_string();
                        s.contains("now()") || s.contains("current_timestamp")
                    } =>
                {
                    return Err(e);
                }
                Err(e) => {
                    tracing::debug!(
                        query = %self.op_name,
                        error = %e,
                        "EOWC operator: core window detection failed, trying incremental"
                    );
                }
            }

            // Session windows must go through CoreWindowState; the incremental
            // path would panic on a session query.
            if matches!(
                cfg.window_type,
                laminar_sql::translator::WindowType::Session
            ) {
                tracing::warn!(
                    query = %self.op_name,
                    "Session window query could not route through CoreWindowState; \
                     falling back to raw-batch EOWC"
                );
            } else {
                match IncrementalEowcState::try_from_sql(&self.ctx, &self.sql, cfg, emit_ref).await
                {
                    Ok(Some(mut eowc)) => {
                        eowc.attach_metrics(self.prom.clone());
                        tracing::info!(
                            query = %self.op_name,
                            "EOWC operator: using incremental per-window accumulators"
                        );
                        self.state = EowcInnerState::EowcAgg(Box::new(eowc));
                        self.apply_pending_restore()?;
                        return Ok(());
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::debug!(
                            query = %self.op_name,
                            error = %e,
                            "EOWC operator: incremental detection failed, using raw path"
                        );
                    }
                }
            }
        }

        tracing::debug!(
            query = %self.op_name,
            "EOWC operator: using raw-batch accumulation path"
        );
        self.state = EowcInnerState::Raw {
            accumulated: Vec::new(),
            last_closed_boundary: i64::MIN,
            accumulated_bytes: 0,
            sql_cache: None,
        };
        self.apply_pending_restore()?;
        Ok(())
    }

    fn apply_pending_restore(&mut self) -> Result<(), DbError> {
        let Some(envelope) = self.pending_restore.take() else {
            return Ok(());
        };
        if let Err(error) = self.apply_checkpoint_envelope(&envelope) {
            // Keep recovery pending so a caller that mishandles the error cannot
            // process or checkpoint an empty/partially restored operator.
            self.pending_restore = Some(envelope);
            return Err(error);
        }
        Ok(())
    }

    fn apply_checkpoint_envelope(
        &mut self,
        envelope: &EowcCheckpointEnvelope,
    ) -> Result<(), DbError> {
        match (&mut self.state, envelope) {
            (EowcInnerState::CoreWindow(cw), EowcCheckpointEnvelope::CoreWindow(cp)) => {
                let previous = cw.checkpoint_windows().map_err(|error| {
                    DbError::Checkpoint(format!(
                        "EOWC CoreWindow restore snapshot for '{}': {error}",
                        self.op_name
                    ))
                })?;
                if let Err(apply_error) = cw.restore_windows(cp) {
                    cw.restore_windows(&previous).map_err(|rollback_error| {
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
            }
            (EowcInnerState::EowcAgg(eowc), EowcCheckpointEnvelope::EowcAgg(cp)) => {
                let previous = eowc.checkpoint_windows().map_err(|error| {
                    DbError::Checkpoint(format!(
                        "EOWC aggregate restore snapshot for '{}': {error}",
                        self.op_name
                    ))
                })?;
                if let Err(apply_error) = eowc.restore_windows(cp) {
                    eowc.restore_windows(&previous).map_err(|rollback_error| {
                        DbError::Checkpoint(format!(
                            "EOWC aggregate restore for '{}' failed: {apply_error}; \
                             rollback also failed: {rollback_error}",
                            self.op_name
                        ))
                    })?;
                    return Err(DbError::Checkpoint(format!(
                        "EOWC aggregate restore for '{}': {apply_error}",
                        self.op_name
                    )));
                }
            }
            (
                EowcInnerState::Raw {
                    accumulated,
                    last_closed_boundary,
                    accumulated_bytes,
                    ..
                },
                EowcCheckpointEnvelope::Raw(cp),
            ) => {
                // Decode before assigning any fields so malformed IPC cannot
                // leave a mixture of old and restored raw state.
                let (batches, bytes) = restore_raw(cp)?;
                *accumulated = batches;
                *accumulated_bytes = bytes;
                *last_closed_boundary = cp.last_closed_boundary;
            }
            (state, envelope) => {
                let state_name = match state {
                    EowcInnerState::CoreWindow(_) => "CoreWindow",
                    EowcInnerState::EowcAgg(_) => "EowcAgg",
                    EowcInnerState::Raw { .. } => "Raw",
                    EowcInnerState::Uninit => "Uninit",
                };
                let checkpoint_name = match envelope {
                    EowcCheckpointEnvelope::CoreWindow(_) => "CoreWindow",
                    EowcCheckpointEnvelope::EowcAgg(_) => "EowcAgg",
                    EowcCheckpointEnvelope::Raw(_) => "Raw",
                };
                return Err(DbError::Checkpoint(format!(
                    "EOWC checkpoint variant mismatch for '{}': state={} checkpoint={}; \
                     refusing to discard state",
                    self.op_name, state_name, checkpoint_name
                )));
            }
        }
        Ok(())
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
            cw.update_batch(batch)?;
        }

        let having_filter = cw.having_filter().cloned();
        let having_sql = cw.having_sql().map(String::from);
        let mut batches = cw.close_windows(watermark)?;

        if let Some(ref filter) = having_filter {
            batches = apply_compiled_having(&batches, filter)?;
        } else if let Some(ref sql) = having_sql {
            batches = apply_having_via_sql(ctx, op_name, &batches, sql, cw.having_sql_cache_mut())
                .await?;
        }

        Ok(batches)
    }

    async fn process_eowc_agg(
        eowc: &mut IncrementalEowcState,
        inputs: &[RecordBatch],
        watermark: i64,
        op_name: &str,
        ctx: &SessionContext,
        task_ctx: &Arc<TaskContext>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let pre_agg_batches = if let Some(proj) = eowc.compiled_projection() {
            match try_evaluate_compiled(proj, inputs) {
                Ok(result) => result,
                Err(e) => {
                    tracing::debug!(
                        query = %op_name,
                        error = %e,
                        "EOWC-agg compiled pre-agg failed, falling back to cached plan"
                    );
                    if let Some(physical) = eowc.cached_pre_agg_physical() {
                        super::execute_cached_physical(task_ctx.clone(), op_name, physical).await?
                    } else {
                        return Err(DbError::Pipeline(format!(
                            "[LDB-8051] EOWC query '{op_name}': compiled pre-agg failed and no cached plan: {e}"
                        )));
                    }
                }
            }
        } else if let Some(physical) = eowc.cached_pre_agg_physical() {
            super::execute_cached_physical(task_ctx.clone(), op_name, physical).await?
        } else {
            return Err(DbError::Pipeline(format!(
                "[LDB-8050] EOWC query '{op_name}': no compiled projection or cached plan"
            )));
        };

        for batch in &pre_agg_batches {
            eowc.update_batch(batch)?;
        }

        let having_filter = eowc.having_filter().cloned();
        let having_sql = eowc.having_sql().map(String::from);
        let mut batches = eowc.close_windows(watermark)?;

        if let Some(ref filter) = having_filter {
            batches = apply_compiled_having(&batches, filter)?;
        } else if let Some(ref sql) = having_sql {
            batches =
                apply_having_via_sql(ctx, op_name, &batches, sql, eowc.having_sql_cache_mut())
                    .await?;
        }

        Ok(batches)
    }

    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    async fn process_raw(
        accumulated: &mut Vec<RecordBatch>,
        last_closed_boundary: &mut i64,
        accumulated_bytes: &mut usize,
        sql_cache: &mut Option<RawSqlCache>,
        inputs: &[RecordBatch],
        watermark: i64,
        window_config: Option<&WindowOperatorConfig>,
        sql: &str,
        op_name: &str,
        ctx: &SessionContext,
        max_accumulated_bytes: usize,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let input_bytes = inputs
            .iter()
            .filter(|batch| batch.num_rows() > 0)
            .try_fold(0usize, |total, batch| {
                total
                    .checked_add(batch.get_array_memory_size())
                    .ok_or_else(|| DbError::Pipeline("raw EOWC input byte overflow".into()))
            })?;
        let next_bytes = (*accumulated_bytes)
            .checked_add(input_bytes)
            .ok_or_else(|| {
                DbError::Pipeline(format!("raw EOWC state byte overflow for '{op_name}'"))
            })?;
        if next_bytes > max_accumulated_bytes {
            return Err(DbError::Pipeline(format!(
                "raw EOWC state for '{op_name}' would grow to {next_bytes} bytes, exceeding the \
                 {max_accumulated_bytes}-byte limit; the batch was not applied"
            )));
        }

        let mut staged = accumulated.clone();
        staged.extend(inputs.iter().filter(|batch| batch.num_rows() > 0).cloned());

        let closed_cut =
            window_config.map_or(watermark, |cfg| compute_closed_boundary(watermark, cfg));

        if closed_cut <= *last_closed_boundary {
            *accumulated = staged;
            *accumulated_bytes = next_bytes;
            return Ok(Vec::new());
        }

        if staged.is_empty() {
            *last_closed_boundary = closed_cut;
            *accumulated_bytes = 0;
            return Ok(Vec::new());
        }

        let (query_batches, retained_batches) = if let Some(cfg) = window_config {
            split_by_timestamp(&staged, &cfg.time_column, closed_cut)
        } else {
            (staged, Vec::new())
        };
        let retained_bytes = raw_batches_bytes(&retained_batches)?;

        if query_batches.is_empty() {
            *accumulated = retained_batches;
            *accumulated_bytes = retained_bytes;
            *last_closed_boundary = closed_cut;
            return Ok(Vec::new());
        }

        if sql_cache.is_none() {
            *sql_cache =
                Some(RawSqlCache::build(ctx, op_name, sql, query_batches[0].schema()).await?);
        }
        let output = sql_cache
            .as_ref()
            .expect("just initialized")
            .apply(op_name, query_batches)
            .await?;
        *accumulated = retained_batches;
        *accumulated_bytes = retained_bytes;
        *last_closed_boundary = closed_cut;
        Ok(output)
    }
}

#[async_trait]
impl GraphOperator for EowcQueryOperator {
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::fixed(
            crate::operator::capability::OperatorImplementation::EowcQuery,
        )
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let watermark = watermarks.first().copied().unwrap_or(i64::MIN);
        // Flatten inputs from port 0
        let input_batches: Vec<RecordBatch> = inputs.first().cloned().unwrap_or_default();

        if matches!(self.state, EowcInnerState::Uninit) {
            self.initialize().await?;
        } else {
            // A failed deferred restore remains pending. Retrying it here
            // prevents processing against empty state if the first error was ignored.
            self.apply_pending_restore()?;
        }

        match &mut self.state {
            EowcInnerState::Uninit => Err(DbError::Pipeline(format!(
                "EOWC query '{}': state not initialized",
                self.op_name
            ))),
            EowcInnerState::CoreWindow(ref mut cw) => {
                Self::process_core_window(
                    cw,
                    &input_batches,
                    watermark,
                    &self.op_name,
                    &self.ctx,
                    &self.task_ctx,
                )
                .await
            }
            EowcInnerState::EowcAgg(ref mut eowc) => {
                Self::process_eowc_agg(
                    eowc,
                    &input_batches,
                    watermark,
                    &self.op_name,
                    &self.ctx,
                    &self.task_ctx,
                )
                .await
            }
            EowcInnerState::Raw {
                ref mut accumulated,
                ref mut last_closed_boundary,
                ref mut accumulated_bytes,
                ref mut sql_cache,
            } => {
                let wc = self.window_config.as_ref();
                Self::process_raw(
                    accumulated,
                    last_closed_boundary,
                    accumulated_bytes,
                    sql_cache,
                    &input_batches,
                    watermark,
                    wc,
                    &self.sql,
                    &self.op_name,
                    &self.ctx,
                    MAX_EOWC_ACCUMULATED_BYTES,
                )
                .await
            }
        }
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        if !matches!(self.state, EowcInnerState::Uninit) {
            // Never publish a checkpoint while recovery is unapplied.
            self.apply_pending_restore()?;
        }
        let envelope = match &mut self.state {
            EowcInnerState::Uninit => {
                // Re-serialize a pending restore so a restore→checkpoint before
                // the first process() doesn't silently drop buffered state.
                if let Some(ref env) = self.pending_restore {
                    let data = rkyv::to_bytes::<rkyv::rancor::Error>(env)
                        .map(|v| v.to_vec())
                        .map_err(|e| {
                            DbError::Pipeline(format!(
                                "EOWC checkpoint serialization of pending restore for '{}': {e}",
                                self.op_name
                            ))
                        })?;
                    return Ok(Some(OperatorCheckpoint { data }));
                }
                return Ok(None);
            }
            EowcInnerState::CoreWindow(ref mut cw) => {
                let cp = cw.checkpoint_windows()?;
                EowcCheckpointEnvelope::CoreWindow(cp)
            }
            EowcInnerState::EowcAgg(ref mut eowc) => {
                let cp = eowc.checkpoint_windows()?;
                EowcCheckpointEnvelope::EowcAgg(cp)
            }
            EowcInnerState::Raw {
                accumulated,
                last_closed_boundary,
                ..
            } => EowcCheckpointEnvelope::Raw(snapshot_raw(accumulated, *last_closed_boundary)?),
        };

        let data = rkyv::to_bytes::<rkyv::rancor::Error>(&envelope)
            .map(|v| v.to_vec())
            .map_err(|e| {
                DbError::Pipeline(format!(
                    "EOWC checkpoint serialization for '{}': {e}",
                    self.op_name
                ))
            })?;

        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let envelope: EowcCheckpointEnvelope =
            rkyv::from_bytes::<EowcCheckpointEnvelope, rkyv::rancor::Error>(&checkpoint.data)
                .map_err(|e| {
                    DbError::Checkpoint(format!(
                        "EOWC checkpoint deserialization for '{}': {e}",
                        self.op_name
                    ))
                })?;

        if matches!(self.state, EowcInnerState::Uninit) {
            self.pending_restore = Some(envelope);
        } else {
            self.apply_checkpoint_envelope(&envelope)?;
        }

        Ok(())
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

/// Split batches at `boundary` into closed (ts < boundary) and retained rows.
fn split_by_timestamp(
    batches: &[RecordBatch],
    time_column: &str,
    boundary: i64,
) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
    use laminar_core::time::{filter_batch_by_timestamp, ThresholdOp};

    let mut closed_batches = Vec::new();
    let mut retained_batches = Vec::new();

    for batch in batches {
        match filter_batch_by_timestamp(batch, time_column, boundary, ThresholdOp::Less) {
            Ok(Some(closed)) => closed_batches.push(closed),
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(
                    column = %time_column,
                    error = %e,
                    "split_by_timestamp: pushing batch to closed bucket due to filter error"
                );
                closed_batches.push(batch.clone());
                continue;
            }
        }
        if let Ok(Some(retained)) =
            filter_batch_by_timestamp(batch, time_column, boundary, ThresholdOp::GreaterEq)
        {
            retained_batches.push(retained);
        }
    }

    (closed_batches, retained_batches)
}

#[cfg(test)]
mod tests {
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

    fn checkpoint_from_envelope(envelope: &EowcCheckpointEnvelope) -> OperatorCheckpoint {
        OperatorCheckpoint {
            data: rkyv::to_bytes::<rkyv::rancor::Error>(envelope)
                .unwrap()
                .to_vec(),
        }
    }

    fn envelope_from_checkpoint(checkpoint: &OperatorCheckpoint) -> EowcCheckpointEnvelope {
        rkyv::from_bytes::<EowcCheckpointEnvelope, rkyv::rancor::Error>(&checkpoint.data).unwrap()
    }

    fn raw_operator() -> EowcQueryOperator {
        let mut op = EowcQueryOperator::new(
            "test_raw_restore",
            "SELECT * FROM trades",
            Some(EmitClause::OnWindowClose),
            None,
            laminar_sql::create_session_context(),
            None,
        );
        op.state = raw_state(vec![test_batch(vec![100, 200])], 99);
        op
    }

    fn raw_state(accumulated: Vec<RecordBatch>, last_closed_boundary: i64) -> EowcInnerState {
        let accumulated_bytes = raw_batches_bytes(&accumulated).unwrap();
        EowcInnerState::Raw {
            accumulated,
            last_closed_boundary,
            accumulated_bytes,
            sql_cache: None,
        }
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
        op.state = EowcInnerState::CoreWindow(Box::new(state));
        op.process(&[vec![test_batch(vec![100])]], &[i64::MIN])
            .await
            .unwrap();
        op
    }

    async fn eowc_aggregate_operator() -> EowcQueryOperator {
        let ctx = aggregate_context();
        let config = test_window_config();
        let state = IncrementalEowcState::try_from_sql(
            &ctx,
            AGG_SQL,
            &config,
            Some(&EmitClause::OnWindowClose),
        )
        .await
        .unwrap()
        .unwrap();
        let mut op = EowcQueryOperator::new(
            "test_eowc_agg_restore",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(config),
            ctx,
            None,
        );
        op.state = EowcInnerState::EowcAgg(Box::new(state));
        op.process(&[vec![test_batch(vec![100])]], &[i64::MIN])
            .await
            .unwrap();
        op
    }

    #[test]
    fn corrupt_checkpoint_envelope_fails_without_mutating_raw_state() {
        let mut op = raw_operator();
        let before = op.checkpoint().unwrap().unwrap();

        let error = op
            .restore(OperatorCheckpoint {
                data: vec![0xff, 0x00, 0x7f],
            })
            .unwrap_err();

        assert!(error.to_string().contains("deserialization"));
        assert!(error.requires_pipeline_recovery());
        assert_eq!(op.checkpoint().unwrap().unwrap().data, before.data);
    }

    #[test]
    fn corrupt_raw_payload_fails_without_partial_state_mutation() {
        let mut op = raw_operator();
        let before = op.checkpoint().unwrap().unwrap();
        let corrupt = EowcCheckpointEnvelope::Raw(RawCheckpoint {
            ipc: vec![0xff, 0x00, 0x7f],
            last_closed_boundary: 1234,
        });

        let error = op.restore(checkpoint_from_envelope(&corrupt)).unwrap_err();

        assert!(error.to_string().contains("raw restore"));
        assert!(error.requires_pipeline_recovery());
        assert_eq!(op.checkpoint().unwrap().unwrap().data, before.data);
    }

    #[tokio::test]
    async fn corrupt_core_window_payload_rolls_back_all_state() {
        let mut op = core_window_operator().await;
        let before = op.checkpoint().unwrap().unwrap();
        let mut corrupt = envelope_from_checkpoint(&before);
        let EowcCheckpointEnvelope::CoreWindow(ref mut checkpoint) = corrupt else {
            panic!("expected CoreWindow checkpoint");
        };
        checkpoint.high_watermark_ms = 1234;
        checkpoint.windows[0].groups[0].key = vec![0xff, 0x00, 0x7f];

        let error = op.restore(checkpoint_from_envelope(&corrupt)).unwrap_err();

        assert!(error.to_string().contains("CoreWindow restore"));
        assert!(error.requires_pipeline_recovery());
        assert_eq!(op.checkpoint().unwrap().unwrap().data, before.data);
    }

    #[tokio::test]
    async fn corrupt_eowc_aggregate_payload_rolls_back_all_state() {
        let mut op = eowc_aggregate_operator().await;
        let before = op.checkpoint().unwrap().unwrap();
        let mut corrupt = envelope_from_checkpoint(&before);
        let EowcCheckpointEnvelope::EowcAgg(ref mut checkpoint) = corrupt else {
            panic!("expected EowcAgg checkpoint");
        };
        checkpoint.high_watermark_ms = 1234;
        checkpoint.windows[0].groups[0].key = vec![0xff, 0x00, 0x7f];

        let error = op.restore(checkpoint_from_envelope(&corrupt)).unwrap_err();

        assert!(error.to_string().contains("aggregate restore"));
        assert!(error.requires_pipeline_recovery());
        assert_eq!(op.checkpoint().unwrap().unwrap().data, before.data);
    }

    #[tokio::test]
    async fn every_checkpoint_variant_mismatch_fails_without_mutation() {
        let core_checkpoint = CoreWindowCheckpoint {
            fingerprint: 0,
            windows: Vec::new(),
            session_state: Vec::new(),
            window_type: "tumbling".to_string(),
            high_watermark_ms: i64::MIN,
        };
        let aggregate_checkpoint = EowcStateCheckpoint {
            fingerprint: 0,
            windows: Vec::new(),
            high_watermark_ms: i64::MIN,
        };

        let mut raw = raw_operator();
        let raw_before = raw.checkpoint().unwrap().unwrap();
        for envelope in [
            EowcCheckpointEnvelope::CoreWindow(core_checkpoint),
            EowcCheckpointEnvelope::EowcAgg(aggregate_checkpoint),
        ] {
            let error = raw
                .restore(checkpoint_from_envelope(&envelope))
                .unwrap_err();
            assert!(error.to_string().contains("variant mismatch"));
            assert!(error.requires_pipeline_recovery());
            assert_eq!(raw.checkpoint().unwrap().unwrap().data, raw_before.data);
        }

        let raw_checkpoint = EowcCheckpointEnvelope::Raw(RawCheckpoint {
            ipc: Vec::new(),
            last_closed_boundary: 1234,
        });
        let mut core = core_window_operator().await;
        let core_before = core.checkpoint().unwrap().unwrap();
        let error = core
            .restore(checkpoint_from_envelope(&raw_checkpoint))
            .unwrap_err();
        assert!(error.to_string().contains("variant mismatch"));
        assert!(error.requires_pipeline_recovery());
        assert_eq!(core.checkpoint().unwrap().unwrap().data, core_before.data);

        let mut aggregate = eowc_aggregate_operator().await;
        let aggregate_before = aggregate.checkpoint().unwrap().unwrap();
        let error = aggregate
            .restore(checkpoint_from_envelope(&raw_checkpoint))
            .unwrap_err();
        assert!(error.to_string().contains("variant mismatch"));
        assert!(error.requires_pipeline_recovery());
        assert_eq!(
            aggregate.checkpoint().unwrap().unwrap().data,
            aggregate_before.data
        );
    }

    #[tokio::test]
    async fn failed_pending_restore_remains_fail_closed() {
        let mut op = EowcQueryOperator::new(
            "test_pending_restore",
            "SELECT * FROM trades",
            Some(EmitClause::OnWindowClose),
            None,
            laminar_sql::create_session_context(),
            None,
        );
        let corrupt = EowcCheckpointEnvelope::Raw(RawCheckpoint {
            ipc: vec![0xff, 0x00, 0x7f],
            last_closed_boundary: 1234,
        });
        op.restore(checkpoint_from_envelope(&corrupt)).unwrap();

        let error = op.process(&[vec![]], &[0]).await.unwrap_err();
        assert!(error.requires_pipeline_recovery());
        assert!(op.pending_restore.is_some());
        let retry_error = op.process(&[vec![]], &[0]).await.unwrap_err();
        assert!(retry_error.requires_pipeline_recovery());
        let checkpoint_error = op
            .checkpoint()
            .err()
            .expect("failed pending restore must reject checkpointing");
        assert!(checkpoint_error.requires_pipeline_recovery());
        let EowcInnerState::Raw {
            accumulated,
            last_closed_boundary,
            accumulated_bytes,
            ..
        } = &op.state
        else {
            panic!("expected initialized Raw state");
        };
        assert!(accumulated.is_empty());
        assert_eq!(*accumulated_bytes, 0);
        assert_eq!(*last_closed_boundary, i64::MIN);
    }

    #[tokio::test]
    async fn raw_byte_limit_accepts_boundary_and_close_releases_accounting() {
        let ctx = laminar_sql::create_session_context();
        let first = test_batch(vec![100]);
        let second = test_batch(vec![200]);
        let mut accumulated = vec![first];
        let mut accumulated_bytes = raw_batches_bytes(&accumulated).unwrap();
        let exact_limit = accumulated_bytes + second.get_array_memory_size();
        let mut boundary = 0;
        let mut cache = None;

        let output = EowcQueryOperator::process_raw(
            &mut accumulated,
            &mut boundary,
            &mut accumulated_bytes,
            &mut cache,
            &[second],
            0,
            None,
            "SELECT * FROM trades",
            "raw_limit",
            &ctx,
            exact_limit,
        )
        .await
        .unwrap();
        assert!(output.is_empty());
        assert_eq!(accumulated_bytes, exact_limit);

        let output = EowcQueryOperator::process_raw(
            &mut accumulated,
            &mut boundary,
            &mut accumulated_bytes,
            &mut cache,
            &[],
            1,
            None,
            "SELECT * FROM trades",
            "raw_limit",
            &ctx,
            exact_limit,
        )
        .await
        .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(accumulated.is_empty());
        assert_eq!(accumulated_bytes, 0);
        assert_eq!(boundary, 1);
    }

    #[tokio::test]
    async fn raw_byte_limit_rejection_is_atomic_and_retryable() {
        let ctx = laminar_sql::create_session_context();
        let first = test_batch(vec![100]);
        let incoming = test_batch(vec![200]);
        let mut accumulated = vec![first];
        let mut accumulated_bytes = raw_batches_bytes(&accumulated).unwrap();
        let limit = accumulated_bytes + incoming.get_array_memory_size() - 1;
        let mut boundary = 0;
        let mut cache = None;
        let before = snapshot_raw(&accumulated, boundary).unwrap();

        for _ in 0..2 {
            let error = EowcQueryOperator::process_raw(
                &mut accumulated,
                &mut boundary,
                &mut accumulated_bytes,
                &mut cache,
                std::slice::from_ref(&incoming),
                0,
                None,
                "SELECT * FROM trades",
                "raw_limit",
                &ctx,
                limit,
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("batch was not applied"));
            let after = snapshot_raw(&accumulated, boundary).unwrap();
            assert_eq!(after.ipc, before.ipc);
            assert_eq!(after.last_closed_boundary, before.last_closed_boundary);
            assert_eq!(accumulated_bytes, raw_batches_bytes(&accumulated).unwrap());
        }
    }

    /// Regression test for the raw-EOWC source-leak bug: before the fix,
    /// `process_raw` registered a `_eowc_raw_*` `MemTable` but then ran the
    /// user SQL referencing the real source. We set up a `SessionContext`
    /// where the source `trades` holds DIFFERENT data than the operator's
    /// `accumulated`, then trigger a close. With the fix, output reflects
    /// `accumulated`; pre-fix, it leaked the source's contents.
    #[tokio::test]
    async fn test_eowc_raw_runs_against_source_not_accumulated() {
        use datafusion::datasource::MemTable;
        let ctx = laminar_sql::create_session_context();
        // Register `trades` in the SessionContext with batch_A (ts=999).
        let batch_a = test_batch(vec![999]);
        let mem = MemTable::try_new(test_schema(), vec![vec![batch_a]]).unwrap();
        ctx.register_table("trades", Arc::new(mem)).unwrap();

        // Construct an operator whose Raw state accumulates batch_B (ts=10,20).
        let mut op = EowcQueryOperator::new(
            "test_raw",
            "SELECT symbol, ts FROM trades",
            Some(EmitClause::OnWindowClose),
            None,
            ctx,
            None,
        );
        op.state = raw_state(vec![test_batch(vec![10, 20])], i64::MIN);
        // Drive process(): empty inputs, watermark advances to 100 — should
        // close the window and emit accumulated (ts in {10,20}).
        let out = op.process(&[vec![]], &[100]).await.unwrap();
        let ts_out: Vec<i64> = out
            .iter()
            .flat_map(|b| {
                b.column(b.schema().index_of("ts").unwrap())
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .map(Option::unwrap)
                    .collect::<Vec<_>>()
            })
            .collect();
        // Expected (the fix): {10, 20}. Bug: {999}.
        assert!(
            !ts_out.contains(&999),
            "raw EOWC leaked source data (ts=999) into the close-cycle output: got {ts_out:?}"
        );
        let mut sorted = ts_out;
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            vec![10_i64, 20],
            "expected accumulated rows at close"
        );
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
        assert!(matches!(op.state, EowcInnerState::Uninit));
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

    #[test]
    fn test_raw_checkpoint_roundtrip() {
        let mut op = EowcQueryOperator::new(
            "test_eowc",
            "SELECT * FROM trades",
            Some(EmitClause::OnWindowClose),
            None,
            laminar_sql::create_session_context(),
            None,
        );
        op.state = raw_state(vec![test_batch(vec![100, 200]), test_batch(vec![300])], 999);
        let cp = op.checkpoint().unwrap().unwrap();

        let mut restored = EowcQueryOperator::new(
            "test_eowc",
            "SELECT * FROM trades",
            Some(EmitClause::OnWindowClose),
            None,
            laminar_sql::create_session_context(),
            None,
        );
        restored.state = raw_state(Vec::new(), i64::MIN);
        restored.restore(cp).unwrap();
        let EowcInnerState::Raw {
            accumulated,
            last_closed_boundary,
            accumulated_bytes,
            ..
        } = &restored.state
        else {
            panic!("expected Raw state after restore");
        };
        assert_eq!(*accumulated_bytes, raw_batches_bytes(accumulated).unwrap());
        assert_eq!(*last_closed_boundary, 999);
        assert_eq!(
            accumulated.iter().map(RecordBatch::num_rows).sum::<usize>(),
            3
        );
    }

    #[tokio::test]
    async fn test_eowc_process_empty_inputs() {
        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);

        // Register trades table so SQL planning works
        let schema = test_schema();
        let empty = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
        ctx.register_table("trades", Arc::new(empty)).unwrap();

        let mut op = EowcQueryOperator::new(
            "test_eowc",
            "SELECT * FROM trades",
            Some(EmitClause::OnWindowClose),
            None,
            ctx,
            None,
        );

        let result = op.process(&[vec![]], &[0]).await.unwrap();
        assert!(result.is_empty());
    }
}
