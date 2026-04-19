//! Standard SQL query operator with lazy initialization.
//!
//! Handles all non-EOWC, non-join queries. On first `process()` call,
//! introspects the SQL via `DataFusion` to determine the execution path:
//! - Aggregate (GROUP BY) -> incremental accumulators
//! - Simple single-source -> compiled `PhysicalExpr` projection
//! - Complex non-aggregate -> cached physical plan (`LiveSourceExec` reads fresh data)

use std::sync::Arc;

use arrow::array::RecordBatch;
#[cfg(feature = "cluster-unstable")]
use arrow::compute::take;
#[cfg(feature = "cluster-unstable")]
use arrow::row::{RowConverter, SortField};
#[cfg(feature = "cluster-unstable")]
use arrow_array::UInt32Array;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion_expr::LogicalPlan;
#[cfg(feature = "cluster-unstable")]
use laminar_core::shuffle::ShuffleMessage;
#[cfg(feature = "cluster-unstable")]
use laminar_core::state::key_hash;

use crate::aggregate_state::{
    apply_compiled_having, AggStateCheckpoint, CompiledProjection, IncrementalAggState,
};
use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
use crate::operator_graph::{try_evaluate_compiled, GraphOperator, OperatorCheckpoint};
use crate::sql_analysis::{extract_projection_filter, single_source_table};

use super::execute_logical_plan;

/// Internal state for the query operator (lazy initialization).
enum QueryState {
    /// Not yet initialized -- need to introspect SQL on first call.
    Uninit,
    /// Aggregate query -- incremental accumulator path.
    Agg(Box<IncrementalAggState>),
    /// Non-aggregate single-source -- compiled `PhysicalExpr` evaluation.
    Compiled(CompiledProjection),
    /// Single-source non-compilable -- cached physical plan. `LiveSourceExec`
    /// reads fresh data from swapped handles on each `execute()`.
    CachedPlan(Arc<dyn datafusion::physical_plan::ExecutionPlan>),
    /// Multi-source (JOIN) -- cached logical plan, physical rebuilt per cycle.
    /// `HashJoinExec::OnceAsync` freezes the build-side hash table on first
    /// execution, so reusing the physical plan returns stale lookup data.
    CachedLogical(Box<LogicalPlan>),
}

/// Row-shuffle config threaded in cluster mode. Pre-aggregate rows are
/// hashed by their GROUP BY columns; local rows feed `IncrementalAggState`,
/// remote rows are serialised and shipped via [`ShuffleSender`]. Remote
/// rows arriving on [`ShuffleReceiver`] are drained at the start of
/// every cycle and fed into the same accumulator.
///
/// Row-shuffle (γ in Phase 0a) ships raw rows, not partial-aggregate
/// state. Cheaper to implement than two-stage partial aggregation,
/// linearly more bandwidth-hungry. Phase 5 upgrades this path.
#[cfg(feature = "cluster-unstable")]
#[derive(Clone)]
pub struct ClusterShuffleConfig {
    pub registry: Arc<laminar_core::state::VnodeRegistry>,
    pub sender: Arc<laminar_core::shuffle::ShuffleSender>,
    pub receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    pub self_id: laminar_core::state::NodeId,
}

#[cfg(feature = "cluster-unstable")]
impl std::fmt::Debug for ClusterShuffleConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterShuffleConfig")
            .field("self_id", &self.self_id)
            .finish_non_exhaustive()
    }
}

pub(crate) struct SqlQueryOperator {
    op_name: Arc<str>,
    sql: String,
    ctx: SessionContext,
    state: QueryState,
    prom: Option<Arc<EngineMetrics>>,
    pending_restore: Option<AggStateCheckpoint>,
    tier_logged: bool,
    cached_having_plan: Option<LogicalPlan>,
    emit_changelog: bool,
    idle_ttl_ms: Option<u64>,
    #[cfg(feature = "cluster-unstable")]
    cluster_shuffle: Option<ClusterShuffleConfig>,
}

impl SqlQueryOperator {
    pub fn new(
        name: &str,
        sql: &str,
        ctx: SessionContext,
        prom: Option<Arc<EngineMetrics>>,
        emit_changelog: bool,
        idle_ttl_ms: Option<u64>,
    ) -> Self {
        Self {
            op_name: Arc::from(name),
            sql: sql.to_string(),
            ctx,
            state: QueryState::Uninit,
            prom,
            pending_restore: None,
            tier_logged: false,
            cached_having_plan: None,
            emit_changelog,
            idle_ttl_ms,
            #[cfg(feature = "cluster-unstable")]
            cluster_shuffle: None,
        }
    }

    /// Install the row-shuffle config. When present and the query
    /// resolves to the aggregate fast-path, each cycle's pre-aggregate
    /// rows are hash-routed across the cluster before reaching
    /// `IncrementalAggState`. See Phase 0a.
    #[cfg(feature = "cluster-unstable")]
    pub fn attach_cluster_shuffle(&mut self, config: ClusterShuffleConfig) {
        self.cluster_shuffle = Some(config);
    }

    async fn lazy_init(&mut self) -> Result<(), DbError> {
        // 1. Try aggregate path first
        match IncrementalAggState::try_from_sql(&self.ctx, &self.sql, self.emit_changelog).await {
            Ok(Some(mut agg_state)) => {
                if let Some(ref cp) = self.pending_restore {
                    if let Err(e) = agg_state.restore_groups(cp) {
                        tracing::warn!(
                            query = %self.op_name,
                            error = %e,
                            "Failed to restore aggregate checkpoint (schema evolution?)"
                        );
                    }
                }
                self.pending_restore = None;
                if let Some(ttl) = self.idle_ttl_ms {
                    agg_state.idle_ttl_ms = Some(ttl);
                }
                self.log_tier(agg_state.compiled_projection().is_some());
                self.state = QueryState::Agg(Box::new(agg_state));
                return Ok(());
            }
            Ok(None) => {}
            Err(e) => {
                tracing::debug!(
                    query = %self.op_name,
                    error = %e,
                    "Could not introspect query plan for aggregate detection, using cached plan"
                );
            }
        }

        // 2. Non-aggregate: try compiled projection, otherwise cache physical plan.
        let df = self
            .ctx
            .sql(&self.sql)
            .await
            .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
        let plan = df.logical_plan().clone();

        if single_source_table(&self.sql).is_some() {
            if let Some(proj) = self.try_build_compiled_projection(&plan) {
                tracing::debug!(
                    query = %self.op_name,
                    "Non-aggregate single-source query compiled to PhysicalExpr"
                );
                self.log_tier(true);
                self.state = QueryState::Compiled(proj);
                return Ok(());
            }
            // Single-source, non-compilable: cache physical plan.
            // LiveSourceExec reads fresh data per execute(), so reuse is safe.
            let physical = self
                .ctx
                .state()
                .create_physical_plan(&plan)
                .await
                .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
            self.log_tier(false);
            self.state = QueryState::CachedPlan(physical);
        } else {
            // Multi-source (JOIN): cache logical plan, rebuild physical per cycle.
            self.log_tier(false);
            self.state = QueryState::CachedLogical(Box::new(plan));
        }
        Ok(())
    }

    fn log_tier(&mut self, compiled: bool) {
        if self.tier_logged {
            return;
        }
        self.tier_logged = true;
        if let Some(ref m) = self.prom {
            if compiled {
                m.queries_compiled.inc();
            } else {
                m.queries_cached_plan.inc();
            }
        }
    }

    fn try_build_compiled_projection(
        &self,
        plan: &datafusion_expr::LogicalPlan,
    ) -> Option<CompiledProjection> {
        let info = extract_projection_filter(plan)?;
        let state = self.ctx.state();
        let props = state.execution_props();
        let mut compiled_exprs = Vec::with_capacity(info.proj_exprs.len());
        let mut proj_fields = Vec::with_capacity(info.proj_exprs.len());

        for expr in &info.proj_exprs {
            let phys =
                datafusion::physical_expr::create_physical_expr(expr, &info.input_df_schema, props)
                    .ok()?;
            let dt = phys.data_type(info.input_df_schema.as_arrow()).ok()?;
            let name = match expr {
                datafusion_expr::Expr::Column(col) => col.name.clone(),
                datafusion_expr::Expr::Alias(alias) => alias.name.clone(),
                _ => expr.schema_name().to_string(),
            };
            proj_fields.push(arrow::datatypes::Field::new(name, dt, true));
            compiled_exprs.push(phys);
        }

        let compiled_filter = if let Some(ref pred) = info.filter_predicate {
            Some(
                datafusion::physical_expr::create_physical_expr(pred, &info.input_df_schema, props)
                    .ok()?,
            )
        } else {
            None
        };

        let output_schema = Arc::new(arrow::datatypes::Schema::new(proj_fields));
        Some(CompiledProjection {
            source_table: info.source_table,
            exprs: compiled_exprs,
            filter: compiled_filter,
            output_schema,
        })
    }

    /// Build a physical plan from `self.sql` and store it as `CachedPlan`.
    async fn build_and_cache_physical_plan(&mut self) -> Result<(), DbError> {
        let df = self
            .ctx
            .sql(&self.sql)
            .await
            .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
        let plan = df.logical_plan().clone();
        let physical = self
            .ctx
            .state()
            .create_physical_plan(&plan)
            .await
            .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
        self.state = QueryState::CachedPlan(physical);
        Ok(())
    }

    /// Execute the cached physical plan. Assumes state is `CachedPlan`.
    async fn execute_cached_plan(&self) -> Result<Vec<RecordBatch>, DbError> {
        let QueryState::CachedPlan(ref plan) = self.state else {
            return Err(DbError::Pipeline(
                "internal: execute_cached_plan called on non-CachedPlan state".into(),
            ));
        };
        let task_ctx = self.ctx.task_ctx();
        datafusion::physical_plan::collect(plan.clone(), task_ctx)
            .await
            .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))
    }

    /// Execute the aggregate path: pre-agg -> `process_batch` -> emit -> HAVING.
    async fn execute_agg(
        &mut self,
        inputs: &[RecordBatch],
        watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let QueryState::Agg(ref mut agg_state) = self.state else {
            return Err(DbError::Pipeline(
                "internal: execute_agg called on non-agg state".into(),
            ));
        };

        // Step 1: Pre-aggregation
        let pre_agg_batches = if let Some(proj) = agg_state.compiled_projection() {
            match try_evaluate_compiled(proj, inputs) {
                Ok(result) => result,
                Err(e) => {
                    tracing::debug!(
                        query = %self.op_name,
                        error = %e,
                        "Compiled pre-agg projection failed, falling back to cached plan"
                    );
                    if let Some(plan) = agg_state.cached_pre_agg_plan() {
                        let plan = plan.clone();
                        execute_logical_plan(&self.ctx, &self.op_name, &plan).await?
                    } else {
                        return Err(DbError::Pipeline(format!(
                            "[LDB-8051] query '{}': compiled pre-agg failed and no cached plan: {e}",
                            self.op_name
                        )));
                    }
                }
            }
        } else if let Some(plan) = agg_state.cached_pre_agg_plan() {
            let plan = plan.clone();
            execute_logical_plan(&self.ctx, &self.op_name, &plan).await?
        } else {
            return Err(DbError::Pipeline(format!(
                "[LDB-8050] query '{}': no compiled projection or cached plan",
                self.op_name
            )));
        };

        // Re-borrow agg_state mutably after the await point.
        let QueryState::Agg(ref mut agg_state) = self.state else {
            unreachable!();
        };

        // Step 1.5: cluster row-shuffle. Hash pre-agg rows by their
        // group columns, keep local, ship remote via `ShuffleSender`.
        // Drain inbound remote rows from `ShuffleReceiver` and merge
        // into this cycle's input. Single-node path is a no-op.
        #[cfg(feature = "cluster-unstable")]
        let pre_agg_batches = {
            let num_group_cols = agg_state.num_group_cols();
            shuffle_pre_agg_batches(
                self.cluster_shuffle.as_ref(),
                &self.op_name,
                num_group_cols,
                pre_agg_batches,
            )
            .await?
        };

        // Step 2: Feed pre-agg batches to incremental accumulators.
        for batch in &pre_agg_batches {
            agg_state.process_batch(batch, watermark)?;
        }

        self.emit_agg_output(watermark).await
    }

    /// Shared emit path for aggregate queries: evict → emit → HAVING.
    async fn emit_agg_output(&mut self, watermark: i64) -> Result<Vec<RecordBatch>, DbError> {
        let QueryState::Agg(ref mut agg_state) = self.state else {
            return Err(DbError::Pipeline(
                "internal: emit_agg_output on non-agg".into(),
            ));
        };

        let mut eviction = if self.emit_changelog {
            agg_state.evict_idle(watermark)?
        } else {
            Vec::new()
        };

        let mut batches = agg_state.emit()?;

        // HAVING is skipped in changelog mode — retractions and HAVING interact
        // incorrectly (a retraction that no longer satisfies HAVING would be
        // silently dropped, leaving stale state downstream).
        if !self.emit_changelog {
            let having_filter = agg_state.having_filter().cloned();
            let having_sql = agg_state.having_sql().map(String::from);
            if let Some(ref filter) = having_filter {
                batches = apply_compiled_having(&batches, filter)?;
            } else if let Some(ref having_sql) = having_sql {
                batches = self.apply_having_sql(&batches, having_sql).await?;
            }
        }

        if eviction.is_empty() {
            Ok(batches)
        } else {
            eviction.extend(batches);
            Ok(eviction)
        }
    }

    /// Apply a HAVING predicate via SQL. Caches the `LogicalPlan` on first call
    /// (saves SQL parsing + logical optimization), but physical plan is rebuilt
    /// per call because the `MemTable` leaf has different data each time.
    async fn apply_having_sql(
        &mut self,
        batches: &[RecordBatch],
        having_sql: &str,
    ) -> Result<Vec<RecordBatch>, DbError> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        let schema = batches[0].schema();
        let table_name = format!("__having_{}", self.op_name);

        let mem_table =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![batches.to_vec()])
                .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
        let _ = self.ctx.deregister_table(&table_name);
        self.ctx
            .register_table(&table_name, Arc::new(mem_table))
            .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;

        let result = if let Some(ref plan) = self.cached_having_plan {
            execute_logical_plan(&self.ctx, &self.op_name, plan).await
        } else {
            let col_list: Vec<String> = schema
                .fields()
                .iter()
                .map(|f| format!("\"{}\"", f.name()))
                .collect();
            let filter_sql = format!(
                "SELECT {} FROM \"{}\" WHERE {having_sql}",
                col_list.join(", "),
                table_name,
            );
            tracing::warn!(
                query = %self.op_name,
                "HAVING filter compiled to PhysicalExpr failed -- using cached SQL plan"
            );
            match self.ctx.sql(&filter_sql).await {
                Ok(df) => {
                    self.cached_having_plan = Some(df.logical_plan().clone());
                    df.collect()
                        .await
                        .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))
                }
                Err(e) => Err(DbError::query_pipeline(&*self.op_name, &e)),
            }
        };

        let _ = self.ctx.deregister_table(&table_name);
        result
    }
}

/// Cluster row-shuffle step applied between pre-aggregate compilation
/// and `IncrementalAggState::process_batch`.
///
/// For each pre-aggregate row:
/// - hash the leading `num_group_cols` columns to a vnode
/// - if `registry.owner(vnode) == self_id`, keep the row local
/// - otherwise serialise the row-slice and ship via [`ShuffleSender`]
///
/// Inbound rows already on [`ShuffleReceiver`] are drained (non-blocking)
/// and appended to the local batch set. The returned vector carries
/// every row that should feed into the local `IncrementalAggState` this
/// cycle — a mix of originally-local rows and remote rows routed here
/// by peers.
///
/// `num_group_cols == 0` is treated as "hash by all columns" — a degenerate
/// global aggregate; all rows land on the same vnode deterministically.
/// Acceptable for `SELECT SUM(x) FROM src`-shape queries: the cluster
/// agrees on one owner, only that owner produces output.
///
/// Single-node fallback: if `config` is `None` the function is a pass-through.
#[cfg(feature = "cluster-unstable")]
async fn shuffle_pre_agg_batches(
    config: Option<&ClusterShuffleConfig>,
    op_name: &str,
    num_group_cols: usize,
    batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, DbError> {
    let Some(cfg) = config else {
        return Ok(batches);
    };

    let vnode_count = cfg.registry.vnode_count();
    let mut local: Vec<RecordBatch> = Vec::new();

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let row_vn = hash_rows_to_vnodes(&batch, num_group_cols, vnode_count);
        let mut seen = row_vn.clone();
        seen.sort_unstable();
        seen.dedup();

        for v in seen {
            let indices: UInt32Array = row_vn
                .iter()
                .enumerate()
                .filter_map(|(i, &rv)| {
                    if rv == v {
                        u32::try_from(i).ok()
                    } else {
                        None
                    }
                })
                .collect();
            if indices.is_empty() {
                continue;
            }
            let cols: Vec<arrow_array::ArrayRef> = batch
                .columns()
                .iter()
                .map(|c| take(c, &indices, None))
                .collect::<Result<_, _>>()
                .map_err(|e| {
                    DbError::Pipeline(format!("[{op_name}] row-shuffle take: {e}"))
                })?;
            let slice = RecordBatch::try_new(batch.schema(), cols).map_err(|e| {
                DbError::Pipeline(format!("[{op_name}] row-shuffle rebuild: {e}"))
            })?;

            let owner = cfg.registry.owner(v);
            if owner == cfg.self_id {
                local.push(slice);
            } else if !owner.is_unassigned() {
                let msg = ShuffleMessage::VnodeData(v, slice);
                // Drop on unreachable peer — source replay preserves
                // at-least-once semantics. Logging each drop would
                // swamp every cycle where a peer is down.
                let _ = cfg.sender.send_to(owner.0, &msg).await;
            }
        }
    }

    // Drain every currently-available remote row into this cycle. Bound
    // by channel depth; never blocks. Messages that arrive after we
    // drain land in the next cycle.
    for (_from, msg) in cfg.receiver.drain_available() {
        if let ShuffleMessage::VnodeData(_vnode, batch) = msg {
            if batch.num_rows() > 0 {
                local.push(batch);
            }
        }
    }

    Ok(local)
}

/// Compute the vnode id for each row by hashing the leading
/// `num_group_cols` columns with the same `arrow::row` + `key_hash`
/// encoding [`ClusterRepartitionExec`] uses. `num_group_cols == 0`
/// collapses to a constant vnode (global aggregate — everyone hashes
/// to 0 which round-robin assigns to the first peer).
#[cfg(feature = "cluster-unstable")]
fn hash_rows_to_vnodes(
    batch: &RecordBatch,
    num_group_cols: usize,
    vnode_count: u32,
) -> Vec<u32> {
    if num_group_cols == 0 || batch.num_rows() == 0 {
        return vec![0; batch.num_rows()];
    }
    let cols: Vec<arrow_array::ArrayRef> = (0..num_group_cols)
        .map(|i| arrow_array::Array::slice(batch.column(i).as_ref(), 0, batch.num_rows()))
        .collect();
    // `Array::slice` above returns `Arc<dyn Array>` — turn it into the
    // `Vec<ArrayRef>` RowConverter expects. (Defensive copy-free clone.)
    let cols: Vec<arrow_array::ArrayRef> = cols.into_iter().collect();
    let fields: Vec<SortField> = cols
        .iter()
        .map(|c| SortField::new(c.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields).expect("RowConverter for hash key");
    let rows = converter
        .convert_columns(&cols)
        .expect("convert_columns for hash key");

    (0..batch.num_rows())
        .map(|row| {
            #[allow(clippy::cast_possible_truncation)]
            let v = (key_hash(rows.row(row).as_ref()) % u64::from(vnode_count)) as u32;
            v
        })
        .collect()
}

#[async_trait]
impl GraphOperator for SqlQueryOperator {
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        if matches!(self.state, QueryState::Uninit) {
            self.lazy_init().await?;
        }

        let watermark = watermarks.first().copied().unwrap_or(i64::MIN);

        let input_batches = inputs.first().map_or(&[] as &[RecordBatch], Vec::as_slice);

        if input_batches.is_empty() || input_batches.iter().all(|b| b.num_rows() == 0) {
            if matches!(self.state, QueryState::Agg(_)) {
                // In cluster mode, `execute_agg` also drains the shuffle
                // receiver for remote rows — skipping it here would
                // strand rows shipped from the leader when the follower
                // has no local input. `execute_agg` handles empty local
                // input gracefully (pre-agg on empty → empty pre-agg,
                // shuffle drain supplies any remote rows).
                #[cfg(feature = "cluster-unstable")]
                {
                    if self.cluster_shuffle.is_some() {
                        return self.execute_agg(input_batches, watermark).await;
                    }
                }
                // Single-node: no shuffle to drain — just emit current state.
                return self.emit_agg_output(watermark).await;
            }
            return Ok(Vec::new());
        }

        match &self.state {
            QueryState::Uninit => unreachable!("lazy_init already called"),
            QueryState::Agg(_) => self.execute_agg(input_batches, watermark).await,
            QueryState::Compiled(_) => {
                let QueryState::Compiled(ref proj) = self.state else {
                    unreachable!();
                };
                match try_evaluate_compiled(proj, input_batches) {
                    Ok(result) => Ok(result),
                    Err(e) => {
                        tracing::debug!(
                            query = %self.op_name,
                            error = %e,
                            "Compiled projection failed, falling back to cached plan"
                        );
                        self.build_and_cache_physical_plan().await?;
                        self.execute_cached_plan().await
                    }
                }
            }
            QueryState::CachedPlan(_) => match self.execute_cached_plan().await {
                Ok(batches) => Ok(batches),
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("Schema error") || err_str.contains("schema mismatch") {
                        tracing::debug!(
                            query = %self.op_name,
                            error = %e,
                            "Cached physical plan invalidated, re-planning"
                        );
                        self.build_and_cache_physical_plan().await?;
                        self.execute_cached_plan().await
                    } else {
                        Err(e)
                    }
                }
            },
            QueryState::CachedLogical(ref plan) => {
                execute_logical_plan(&self.ctx, &self.op_name, plan).await
            }
        }
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        if matches!(self.state, QueryState::Uninit) {
            if let Some(ref cp) = self.pending_restore {
                let data = serde_json::to_vec(cp).map_err(|e| {
                    DbError::Pipeline(format!(
                        "checkpoint serialization of pending restore for '{}': {e}",
                        self.op_name
                    ))
                })?;
                return Ok(Some(OperatorCheckpoint { data }));
            }
            return Ok(None);
        }

        let QueryState::Agg(ref mut agg_state) = self.state else {
            return Ok(None);
        };

        let cp = agg_state.checkpoint_groups()?;
        let data = serde_json::to_vec(&cp).map_err(|e| {
            DbError::Pipeline(format!(
                "checkpoint serialization for '{}': {e}",
                self.op_name
            ))
        })?;
        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let cp: AggStateCheckpoint = serde_json::from_slice(&checkpoint.data).map_err(|e| {
            DbError::Pipeline(format!(
                "checkpoint deserialization for '{}': {e}",
                self.op_name
            ))
        })?;

        match self.state {
            QueryState::Agg(ref mut agg_state) => {
                agg_state.restore_groups(&cp)?;
            }
            QueryState::Uninit => {
                self.pending_restore = Some(cp);
            }
            QueryState::Compiled(_) | QueryState::CachedPlan(_) | QueryState::CachedLogical(_) => {
                tracing::warn!(
                    query = %self.op_name,
                    "Ignoring aggregate checkpoint for non-aggregate query (schema evolution?)"
                );
            }
        }
        Ok(())
    }

    fn estimated_state_bytes(&self) -> usize {
        match &self.state {
            QueryState::Agg(ref agg_state) => agg_state.estimated_size_bytes(),
            _ => 0,
        }
    }
}
