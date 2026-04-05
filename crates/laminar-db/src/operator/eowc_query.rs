//! EOWC (Emit On Window Close) query operator for the `OperatorGraph`.
//!
//! Handles window-close semantics with lazy initialization: on the first
//! `process()` call, the operator probes `CoreWindowState`, then
//! `IncrementalEowcState`, and falls back to a raw-batch accumulation path.
//!
//! For aggregate paths, each cycle:
//! 1. Evaluate pre-agg projection (compiled or cached plan) on input batches.
//! 2. Feed pre-agg data to windowed state via `update_batch()`.
//! 3. Close windows whose boundary has been passed by the watermark.
//!
//! For the raw path, batches are accumulated until the watermark advances past
//! a window boundary, at which point accumulated data is replayed via SQL.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;

use crate::aggregate_state::{apply_compiled_having, EowcStateCheckpoint};
use crate::core_window_state::{CoreWindowCheckpoint, CoreWindowState};
use crate::eowc_state::IncrementalEowcState;
use crate::error::DbError;
use crate::metrics::PipelineCounters;
use crate::operator_graph::{try_evaluate_compiled, GraphOperator, OperatorCheckpoint};
use crate::sql_analysis::compute_closed_boundary;
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::WindowOperatorConfig;

/// Maximum rows an EOWC raw-batch accumulator may hold before coalescing.
/// Prevents unbounded memory growth when windows fail to close or late
/// data keeps arriving.
const MAX_EOWC_ACCUMULATED_ROWS: usize = 1_000_000;

/// Wrapper for checkpoint data that discriminates between state variants.
#[derive(serde::Serialize, serde::Deserialize)]
enum EowcCheckpointEnvelope {
    /// Checkpoint from `CoreWindowState`.
    CoreWindow(CoreWindowCheckpoint),
    /// Checkpoint from `IncrementalEowcState`.
    EowcAgg(EowcStateCheckpoint),
    /// Raw path has no persistent state worth checkpointing (accumulated
    /// batches are regenerated from replayed source data on recovery).
    Raw,
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
    },
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
}

impl EowcQueryOperator {
    pub fn new(
        name: &str,
        sql: &str,
        emit_clause: Option<EmitClause>,
        window_config: Option<WindowOperatorConfig>,
        ctx: SessionContext,
        _counters: Option<Arc<PipelineCounters>>,
    ) -> Self {
        Self {
            op_name: Arc::from(name),
            sql: Arc::from(sql),
            emit_clause,
            window_config,
            ctx,
            state: EowcInnerState::Uninit,
            pending_restore: None,
        }
    }

    /// Lazy initialization: probe `CoreWindowState`, then
    /// `IncrementalEowcState`, then fall back to `Raw`.
    async fn initialize(&mut self) -> Result<(), DbError> {
        if let Some(ref cfg) = self.window_config {
            let emit_ref = self.emit_clause.as_ref();
            match CoreWindowState::try_from_sql(&self.ctx, &self.sql, cfg, emit_ref).await {
                Ok(Some(cw)) => {
                    tracing::info!(
                        query = %self.op_name,
                        window_type = ?cfg.window_type,
                        "EOWC operator: routed to core window pipeline"
                    );
                    self.state = EowcInnerState::CoreWindow(Box::new(cw));
                    self.apply_pending_restore();
                    return Ok(());
                }
                Ok(None) => {}
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
                match IncrementalEowcState::try_from_sql(&self.ctx, &self.sql, cfg).await {
                    Ok(Some(eowc)) => {
                        tracing::info!(
                            query = %self.op_name,
                            "EOWC operator: using incremental per-window accumulators"
                        );
                        self.state = EowcInnerState::EowcAgg(Box::new(eowc));
                        self.apply_pending_restore();
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
        };
        // Raw path has no restorable state — discard any pending checkpoint.
        self.pending_restore = None;
        Ok(())
    }

    /// Apply a deferred checkpoint after `initialize()` has set the state.
    fn apply_pending_restore(&mut self) {
        let Some(envelope) = self.pending_restore.take() else {
            return;
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
            (_, EowcCheckpointEnvelope::Raw) => {}
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
                    EowcCheckpointEnvelope::Raw => "Raw",
                };
                tracing::warn!(
                    query = %self.op_name,
                    state = state_name,
                    checkpoint = cp_name,
                    "EOWC checkpoint/state variant mismatch, discarding (schema evolution?)"
                );
            }
        }
    }

    /// Execute the `CoreWindowState` path: pre-agg, update, close.
    async fn process_core_window(
        cw: &mut CoreWindowState,
        inputs: &[RecordBatch],
        watermark: i64,
        op_name: &str,
        ctx: &SessionContext,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let pre_agg_batches = if let Some(proj) = cw.compiled_projection() {
            match try_evaluate_compiled(proj, inputs) {
                Ok(result) => result,
                Err(e) => {
                    tracing::debug!(
                        query = %op_name,
                        error = %e,
                        "EOWC compiled pre-agg failed, falling back to cached plan"
                    );
                    if let Some(plan) = cw.cached_pre_agg_plan() {
                        super::execute_logical_plan(ctx, op_name, plan).await?
                    } else {
                        return Err(DbError::Pipeline(format!(
                            "[LDB-8051] EOWC query '{op_name}': compiled pre-agg failed and no cached plan: {e}"
                        )));
                    }
                }
            }
        } else if let Some(plan) = cw.cached_pre_agg_plan() {
            super::execute_logical_plan(ctx, op_name, plan).await?
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
            batches = apply_having_via_sql(ctx, op_name, &batches, sql).await?;
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
                    if let Some(plan) = eowc.cached_pre_agg_plan() {
                        super::execute_logical_plan(ctx, op_name, plan).await?
                    } else {
                        return Err(DbError::Pipeline(format!(
                            "[LDB-8051] EOWC query '{op_name}': compiled pre-agg failed and no cached plan: {e}"
                        )));
                    }
                }
            }
        } else if let Some(plan) = eowc.cached_pre_agg_plan() {
            super::execute_logical_plan(ctx, op_name, plan).await?
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
            batches = apply_having_via_sql(ctx, op_name, &batches, sql).await?;
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

        // Register accumulated data as a temporary MemTable and run SQL
        let schema = query_batches[0].schema();

        // Register the closed batches as a temporary source table.
        let temp_table = format!("_eowc_raw_{}", op_name.replace(['-', ' '], "_"));
        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![query_batches])
            .map_err(|e| DbError::query_pipeline(op_name, &e))?;

        let _ = ctx.deregister_table(&temp_table);
        ctx.register_table(&temp_table, Arc::new(mem_table))
            .map_err(|e| DbError::query_pipeline(op_name, &e))?;

        // Execute the SQL query against the MemTable
        let result = match ctx.sql(sql).await {
            Ok(df) => df
                .collect()
                .await
                .map_err(|e| DbError::query_pipeline(op_name, &e)),
            Err(e) => Err(DbError::query_pipeline(op_name, &e)),
        };

        // Cleanup
        let _ = ctx.deregister_table(&temp_table);

        result
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
            } => {
                let wc = self.window_config.as_ref();
                Self::process_raw(
                    accumulated,
                    last_closed_boundary,
                    accumulated_rows,
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
                    let data = serde_json::to_vec(env).map_err(|e| {
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
            EowcInnerState::Raw { .. } => {
                // Raw path: accumulated batches are not checkpointed.
                // On recovery, data will be replayed from sources.
                EowcCheckpointEnvelope::Raw
            }
        };

        let data = serde_json::to_vec(&envelope).map_err(|e| {
            DbError::Pipeline(format!(
                "EOWC checkpoint serialization for '{}': {e}",
                self.op_name
            ))
        })?;

        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let envelope: EowcCheckpointEnvelope =
            serde_json::from_slice(&checkpoint.data).map_err(|e| {
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
            (EowcInnerState::Uninit, _) => {
                self.pending_restore = Some(envelope);
            }
            (_, EowcCheckpointEnvelope::Raw) => {}
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

/// Apply a HAVING filter expressed as SQL by running it against a temporary
/// `MemTable` containing the candidate batches.
async fn apply_having_via_sql(
    ctx: &SessionContext,
    query_name: &str,
    batches: &[RecordBatch],
    having_sql: &str,
) -> Result<Vec<RecordBatch>, DbError> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = batches[0].schema();
    let temp_name = format!("_having_{}", query_name.replace(['-', ' '], "_"));

    let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![batches.to_vec()])
        .map_err(|e| DbError::query_pipeline(query_name, &e))?;

    let _ = ctx.deregister_table(&temp_name);
    ctx.register_table(&temp_name, Arc::new(mem_table))
        .map_err(|e| DbError::query_pipeline(query_name, &e))?;

    let having_query = format!("SELECT * FROM \"{temp_name}\" WHERE {having_sql}");
    let result = match ctx.sql(&having_query).await {
        Ok(df) => df
            .collect()
            .await
            .map_err(|e| DbError::query_pipeline(query_name, &e)),
        Err(e) => Err(DbError::query_pipeline(query_name, &e)),
    };

    let _ = ctx.deregister_table(&temp_name);
    result
}

/// Split accumulated batches into closed-window rows (ts < boundary) and
/// retained rows (ts >= boundary) using timestamp column filtering.
fn split_by_timestamp(
    batches: &[RecordBatch],
    time_column: &str,
    boundary: i64,
) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
    let format = batches
        .first()
        .map_or(laminar_core::time::TimestampFormat::UnixMillis, |b| {
            crate::sql_analysis::infer_ts_format_from_batch(b, time_column)
        });

    let mut closed_batches = Vec::new();
    let mut retained_batches = Vec::new();

    for batch in batches {
        if let Some(closed) = crate::batch_filter::filter_batch_by_timestamp(
            batch,
            time_column,
            boundary,
            format,
            crate::batch_filter::ThresholdOp::Less,
        ) {
            closed_batches.push(closed);
        }
        if let Some(retained) = crate::batch_filter::filter_batch_by_timestamp(
            batch,
            time_column,
            boundary,
            format,
            crate::batch_filter::ThresholdOp::GreaterEq,
        ) {
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
        };
        assert!(op.estimated_state_bytes() > 0);
    }

    #[test]
    fn test_raw_checkpoint_roundtrip() {
        let ctx = laminar_sql::create_session_context();
        let mut op = EowcQueryOperator::new(
            "test_eowc",
            "SELECT * FROM trades",
            Some(EmitClause::OnWindowClose),
            None,
            ctx,
            None,
        );
        op.state = EowcInnerState::Raw {
            accumulated: Vec::new(),
            last_closed_boundary: i64::MIN,
            accumulated_rows: 0,
        };
        let cp = op.checkpoint().unwrap().unwrap();
        assert!(!cp.data.is_empty());

        // Restore should succeed (Raw checkpoint is a no-op)
        op.restore(cp).unwrap();
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
