//! EOWC (Emit On Window Close) operator. First `process()` picks one of
//! three paths — `CoreWindowState`, `IncrementalEowcState`, or raw-batch
//! accumulation — then commits to it.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
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

/// Maximum rows an EOWC raw-batch accumulator may hold before coalescing.
/// Prevents unbounded memory growth when windows fail to close or late
/// data keeps arriving.
const MAX_EOWC_ACCUMULATED_ROWS: usize = 1_000_000;

/// Wrapper for checkpoint data that discriminates between state variants.
#[derive(
    serde::Serialize, serde::Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
enum EowcCheckpointEnvelope {
    /// Checkpoint from `CoreWindowState`.
    CoreWindow(CoreWindowCheckpoint),
    /// Checkpoint from `IncrementalEowcState`.
    EowcAgg(EowcStateCheckpoint),
    /// Non-aggregate path: accumulated batches + boundary. Empty
    /// `ipc` means no rows were buffered.
    Raw(RawCheckpoint),
}

/// Snapshot of the raw-EOWC accumulator. Source replay alone may not
/// cover the window on recovery, so we ship the buffered batches.
#[derive(
    serde::Serialize, serde::Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
struct RawCheckpoint {
    ipc: Vec<u8>,
    last_closed_boundary: i64,
}

/// Lazy-initialized EOWC state. The variant is chosen on the first
/// `process()` call by probing the SQL plan.
enum EowcInnerState {
    Uninit,
    CoreWindow(Box<CoreWindowState>),
    EowcAgg(Box<IncrementalEowcState>),
    /// Non-aggregate EOWC: accumulate batches and replay via SQL when
    /// windows close.
    Raw {
        accumulated: Vec<RecordBatch>,
        last_closed_boundary: i64,
        accumulated_rows: usize,
        /// Lazy cache: `LiveSourceProvider` registered under a private
        /// table name + the user SQL rewritten to reference it + cached
        /// physical plan. Built on the first close-cycle, when the
        /// source schema is known.
        sql_cache: Option<RawSqlCache>,
    },
}

/// Raw-EOWC close-cycle cache: the user's SQL with its single source
/// AST-rewritten to a private table backed by [`LiveSqlCache`].
struct RawSqlCache(super::LiveSqlCache);

impl RawSqlCache {
    async fn build(
        ctx: &SessionContext,
        op_name: &str,
        original_sql: &str,
        source_schema: arrow::datatypes::SchemaRef,
    ) -> Result<Self, DbError> {
        // Single source only — multi-source raw EOWC can't be safely
        // rewritten (which side do we swap?). Fail loud.
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
        ctx: &SessionContext,
        op_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.0.apply(ctx, op_name, batches).await
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

fn restore_raw(cp: &RawCheckpoint) -> Result<Vec<RecordBatch>, DbError> {
    if cp.ipc.is_empty() {
        return Ok(Vec::new());
    }
    crate::mv_store::ipc_to_batches(&cp.ipc)
        .map_err(|e| DbError::Pipeline(format!("EOWC raw restore: {e}")))
}

/// Walk a parsed SQL statement, replacing every `TableFactor::Table` whose
/// unqualified name equals `source` with `temp`. AST-based so qualified
/// names / aliases / nested joins / subqueries all preserve their structure.
fn rewrite_source(sql: &str, source: &str, temp: &str) -> Result<String, DbError> {
    use sqlparser::ast::{Ident, ObjectName, SetExpr, Statement, TableFactor};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    // Compare on the unqualified tail of both sides so `db.events` and
    // bare `events` match when either form names the registered source.
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
        Self {
            op_name: Arc::from(name),
            sql: Arc::from(sql),
            emit_clause,
            window_config,
            ctx,
            state: EowcInnerState::Uninit,
            pending_restore: None,
            prom,
        }
    }

    /// Lazy initialization: probe `CoreWindowState`, then
    /// `IncrementalEowcState`, then fall back to `Raw`.
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
                // `now()`-misuse is a hard error — propagate. Other
                // feature gaps (CUMULATE, hop wpe cap, ...) fall through
                // to the incremental/raw paths.
                Err(e @ DbError::Unsupported(_)) if e.to_string().contains("now()") => {
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

            // Guard: session windows MUST route through CoreWindowState.
            // If it rejected, skip incremental EOWC (its session path panics).
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
            accumulated_rows: 0,
            sql_cache: None,
        };
        // Apply any pending Raw checkpoint that landed before init.
        self.apply_pending_restore()?;
        Ok(())
    }

    /// Apply a deferred checkpoint after `initialize()` has set the state.
    fn apply_pending_restore(&mut self) -> Result<(), DbError> {
        let Some(envelope) = self.pending_restore.take() else {
            return Ok(());
        };
        match (&mut self.state, envelope) {
            (EowcInnerState::CoreWindow(cw), EowcCheckpointEnvelope::CoreWindow(cp)) => {
                if let Err(e) = cw.restore_windows(&cp) {
                    tracing::warn!(
                        query = %self.op_name, error = %e,
                        "Failed to restore EOWC CoreWindow checkpoint"
                    );
                }
            }
            (EowcInnerState::EowcAgg(eowc), EowcCheckpointEnvelope::EowcAgg(cp)) => {
                if let Err(e) = eowc.restore_windows(&cp) {
                    tracing::warn!(
                        query = %self.op_name, error = %e,
                        "Failed to restore EOWC aggregate checkpoint"
                    );
                }
            }
            (
                EowcInnerState::Raw {
                    accumulated,
                    last_closed_boundary,
                    accumulated_rows,
                    ..
                },
                EowcCheckpointEnvelope::Raw(cp),
            ) => match restore_raw(&cp) {
                Ok(batches) => {
                    *accumulated_rows = batches.iter().map(RecordBatch::num_rows).sum();
                    *accumulated = batches;
                    *last_closed_boundary = cp.last_closed_boundary;
                }
                Err(e) => tracing::warn!(
                    query = %self.op_name, error = %e,
                    "Failed to restore EOWC raw-batch checkpoint"
                ),
            },
            (state, envelope) => {
                let state_name = match state {
                    EowcInnerState::CoreWindow(_) => "CoreWindow",
                    EowcInnerState::EowcAgg(_) => "EowcAgg",
                    EowcInnerState::Raw { .. } => "Raw",
                    EowcInnerState::Uninit => "Uninit",
                };
                let cp_name = match envelope {
                    EowcCheckpointEnvelope::CoreWindow(_) => "CoreWindow",
                    EowcCheckpointEnvelope::EowcAgg(_) => "EowcAgg",
                    EowcCheckpointEnvelope::Raw(_) => "Raw",
                };
                // Fail loud rather than silently dropping state and
                // re-emitting freshly-zeroed windows after restart —
                // a benign-looking edit (WHERE clause change re-routes
                // the operator path) would otherwise produce duplicate
                // window output, breaking exactly-once on the sink.
                return Err(DbError::Pipeline(format!(
                    "EOWC checkpoint variant mismatch for '{}': state={} checkpoint={}; \
                     refusing to silently discard partial state. Drop the checkpoint or \
                     keep the previous query shape.",
                    self.op_name, state_name, cp_name
                )));
            }
        }
        Ok(())
    }

    /// Execute the `CoreWindowState` path: pre-agg, update, close.
    async fn process_core_window(
        cw: &mut CoreWindowState,
        inputs: &[RecordBatch],
        watermark: i64,
        op_name: &str,
        ctx: &SessionContext,
    ) -> Result<Vec<RecordBatch>, DbError> {
        // Resolve a `now()` WHERE clause against the event-time watermark
        // (replay-deterministic) and pre-filter the inputs. No-op for
        // predicates without `now()`.
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
                        super::execute_cached_physical(ctx, op_name, physical).await?
                    } else {
                        return Err(DbError::Pipeline(format!(
                            "[LDB-8051] EOWC query '{op_name}': compiled pre-agg failed and no cached plan: {e}"
                        )));
                    }
                }
            }
        } else if let Some(physical) = cw.cached_pre_agg_physical() {
            super::execute_cached_physical(ctx, op_name, physical).await?
        } else {
            return Err(DbError::Pipeline(format!(
                "[LDB-8050] EOWC query '{op_name}': no compiled projection or cached plan"
            )));
        };

        // Update windowed state
        for batch in &pre_agg_batches {
            cw.update_batch(batch)?;
        }

        // Close windows and apply HAVING filter
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

    /// Execute the `IncrementalEowcState` path: pre-agg, update, close.
    async fn process_eowc_agg(
        eowc: &mut IncrementalEowcState,
        inputs: &[RecordBatch],
        watermark: i64,
        op_name: &str,
        ctx: &SessionContext,
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
                        super::execute_cached_physical(ctx, op_name, physical).await?
                    } else {
                        return Err(DbError::Pipeline(format!(
                            "[LDB-8051] EOWC query '{op_name}': compiled pre-agg failed and no cached plan: {e}"
                        )));
                    }
                }
            }
        } else if let Some(physical) = eowc.cached_pre_agg_physical() {
            super::execute_cached_physical(ctx, op_name, physical).await?
        } else {
            return Err(DbError::Pipeline(format!(
                "[LDB-8050] EOWC query '{op_name}': no compiled projection or cached plan"
            )));
        };

        // Update windowed state
        for batch in &pre_agg_batches {
            eowc.update_batch(batch)?;
        }

        // Close windows and apply HAVING filter
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

    /// Execute the raw accumulation path: accumulate batches, replay SQL
    /// when windows close.
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    async fn process_raw(
        accumulated: &mut Vec<RecordBatch>,
        last_closed_boundary: &mut i64,
        accumulated_rows: &mut usize,
        sql_cache: &mut Option<RawSqlCache>,
        inputs: &[RecordBatch],
        watermark: i64,
        window_config: Option<&WindowOperatorConfig>,
        sql: &str,
        op_name: &str,
        ctx: &SessionContext,
    ) -> Result<Vec<RecordBatch>, DbError> {
        // Accumulate new input batches
        for batch in inputs {
            if batch.num_rows() > 0 {
                *accumulated_rows += batch.num_rows();
                accumulated.push(batch.clone());
            }
        }

        // Memory pressure guard: coalesce when over the row limit
        if *accumulated_rows > MAX_EOWC_ACCUMULATED_ROWS && accumulated.len() > 1 {
            tracing::warn!(
                query = op_name,
                accumulated_rows = *accumulated_rows,
                limit = MAX_EOWC_ACCUMULATED_ROWS,
                "EOWC memory pressure: coalescing batches to reduce fragmentation"
            );
            let schema = accumulated[0].schema();
            match arrow::compute::concat_batches(&schema, accumulated.as_slice()) {
                Ok(coalesced) => {
                    *accumulated = vec![coalesced];
                }
                Err(e) => {
                    tracing::warn!("EOWC pressure coalescing failed: {e}");
                }
            }
        }

        // Compute closed-window boundary
        let closed_cut =
            window_config.map_or(watermark, |cfg| compute_closed_boundary(watermark, cfg));

        if closed_cut <= *last_closed_boundary {
            // No new windows closed
            return Ok(Vec::new());
        }

        if accumulated.is_empty() {
            *last_closed_boundary = closed_cut;
            return Ok(Vec::new());
        }

        // Split accumulated data: closed-window rows for query, retained for
        // the next cycle. If we have a time column, filter by timestamp.
        let (query_batches, retained_batches) = if let Some(cfg) = window_config {
            split_by_timestamp(accumulated, &cfg.time_column, closed_cut)
        } else {
            // No window config means all data is emitted
            (std::mem::take(accumulated), Vec::new())
        };

        // Replace accumulated state with retained batches
        *accumulated = retained_batches;
        *accumulated_rows = accumulated.iter().map(RecordBatch::num_rows).sum();
        *last_closed_boundary = closed_cut;

        if query_batches.is_empty() {
            return Ok(Vec::new());
        }

        // Lazy-init the per-operator LiveSourceProvider + cached physical
        // plan. The user SQL is AST-rewritten once to reference the temp
        // table; subsequent closes just swap data and re-collect.
        if sql_cache.is_none() {
            *sql_cache =
                Some(RawSqlCache::build(ctx, op_name, sql, query_batches[0].schema()).await?);
        }
        sql_cache
            .as_ref()
            .expect("just initialized")
            .apply(ctx, op_name, query_batches)
            .await
    }
}

#[async_trait]
impl GraphOperator for EowcQueryOperator {
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let watermark = watermarks.first().copied().unwrap_or(i64::MIN);
        // Flatten inputs from port 0
        let input_batches: Vec<RecordBatch> = inputs.first().cloned().unwrap_or_default();

        // Lazy initialization on first call
        if matches!(self.state, EowcInnerState::Uninit) {
            self.initialize().await?;
        }

        match &mut self.state {
            EowcInnerState::Uninit => Err(DbError::Pipeline(format!(
                "EOWC query '{}': state not initialized",
                self.op_name
            ))),
            EowcInnerState::CoreWindow(ref mut cw) => {
                Self::process_core_window(cw, &input_batches, watermark, &self.op_name, &self.ctx)
                    .await
            }
            EowcInnerState::EowcAgg(ref mut eowc) => {
                Self::process_eowc_agg(eowc, &input_batches, watermark, &self.op_name, &self.ctx)
                    .await
            }
            EowcInnerState::Raw {
                ref mut accumulated,
                ref mut last_closed_boundary,
                ref mut accumulated_rows,
                ref mut sql_cache,
            } => {
                let wc = self.window_config.as_ref();
                Self::process_raw(
                    accumulated,
                    last_closed_boundary,
                    accumulated_rows,
                    sql_cache,
                    &input_batches,
                    watermark,
                    wc,
                    &self.sql,
                    &self.op_name,
                    &self.ctx,
                )
                .await
            }
        }
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        let envelope = match &mut self.state {
            EowcInnerState::Uninit => {
                // If we have a pending restore, re-serialize it so a
                // restore->checkpoint cycle before first process() preserves data.
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
                    DbError::Pipeline(format!(
                        "EOWC checkpoint deserialization for '{}': {e}",
                        self.op_name
                    ))
                })?;

        match (&mut self.state, &envelope) {
            (EowcInnerState::CoreWindow(cw), EowcCheckpointEnvelope::CoreWindow(cp)) => {
                cw.restore_windows(cp)?;
            }
            (EowcInnerState::EowcAgg(eowc), EowcCheckpointEnvelope::EowcAgg(cp)) => {
                eowc.restore_windows(cp)?;
            }
            (
                EowcInnerState::Raw {
                    accumulated,
                    last_closed_boundary,
                    accumulated_rows,
                    ..
                },
                EowcCheckpointEnvelope::Raw(cp),
            ) => {
                let batches = restore_raw(cp)?;
                *accumulated_rows = batches.iter().map(RecordBatch::num_rows).sum();
                *accumulated = batches;
                *last_closed_boundary = cp.last_closed_boundary;
            }
            (EowcInnerState::Uninit, _) => {
                self.pending_restore = Some(envelope);
            }
            _ => {
                tracing::warn!(
                    query = %self.op_name,
                    "EOWC checkpoint/state variant mismatch, ignoring"
                );
            }
        }

        Ok(())
    }

    fn estimated_state_bytes(&self) -> usize {
        match &self.state {
            EowcInnerState::Uninit => 0,
            EowcInnerState::CoreWindow(cw) => cw.estimated_size_bytes(),
            EowcInnerState::EowcAgg(eowc) => eowc.estimated_size_bytes(),
            EowcInnerState::Raw { accumulated, .. } => {
                // Rough estimate: sum of batch memory sizes
                accumulated
                    .iter()
                    .map(RecordBatch::get_array_memory_size)
                    .sum()
            }
        }
    }
}

/// Apply a HAVING filter expressed as SQL against the candidate batches.
/// First call builds a `LiveSourceProvider`-backed cache; subsequent calls
/// swap batches into the handle and re-run the cached physical plan — no
/// per-cycle SQL parse/plan/optimize.
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
        .apply(ctx, query_name, batches.to_vec())
        .await
}

/// Split accumulated batches into closed-window rows (ts < boundary) and
/// retained rows (ts >= boundary). When the time column is missing or the
/// wrong type the whole batch goes to `closed` — there is no way to split
/// it by time, and leaking it into both buckets would double-emit.
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
        op.state = EowcInnerState::Raw {
            accumulated: vec![test_batch(vec![10, 20])],
            last_closed_boundary: i64::MIN,
            accumulated_rows: 2,
            sql_cache: None,
        };
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
    fn test_eowc_raw_state_estimated_bytes() {
        let ctx = laminar_sql::create_session_context();
        let mut op = EowcQueryOperator::new(
            "test_eowc",
            "SELECT * FROM trades",
            Some(EmitClause::OnWindowClose),
            None,
            ctx,
            None,
        );
        // Manually set to raw state with a batch
        let batch = test_batch(vec![100, 200]);
        op.state = EowcInnerState::Raw {
            accumulated: vec![batch],
            last_closed_boundary: i64::MIN,
            accumulated_rows: 2,
            sql_cache: None,
        };
        assert!(op.estimated_state_bytes() > 0);
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
        op.state = EowcInnerState::Raw {
            accumulated: vec![test_batch(vec![100, 200]), test_batch(vec![300])],
            last_closed_boundary: 999,
            accumulated_rows: 3,
            sql_cache: None,
        };
        let cp = op.checkpoint().unwrap().unwrap();

        let mut restored = EowcQueryOperator::new(
            "test_eowc",
            "SELECT * FROM trades",
            Some(EmitClause::OnWindowClose),
            None,
            laminar_sql::create_session_context(),
            None,
        );
        restored.state = EowcInnerState::Raw {
            accumulated: Vec::new(),
            last_closed_boundary: i64::MIN,
            accumulated_rows: 0,
            sql_cache: None,
        };
        restored.restore(cp).unwrap();
        let EowcInnerState::Raw {
            accumulated,
            last_closed_boundary,
            accumulated_rows,
            ..
        } = &restored.state
        else {
            panic!("expected Raw state after restore");
        };
        assert_eq!(*accumulated_rows, 3);
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
