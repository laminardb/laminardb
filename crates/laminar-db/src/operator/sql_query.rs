//! Standard SQL query operator with lazy initialization.
//!
//! Handles all non-EOWC, non-join queries. On first `process()` call,
//! introspects the SQL via `DataFusion` to determine the execution path:
//! - Aggregate (GROUP BY) -> incremental accumulators
//! - Simple single-source -> compiled `PhysicalExpr` projection
//! - Complex non-aggregate -> cached physical plan (`LiveSourceExec` reads fresh data)

use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
#[cfg(feature = "cluster-unstable")]
use laminar_core::shuffle::ShuffleMessage;

use crate::aggregate_state::{
    apply_compiled_having, AggStateCheckpoint, CompiledProjection, IncrementalAggState,
};
use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
use crate::operator_graph::{try_evaluate_compiled, GraphOperator, OperatorCheckpoint};
use crate::sql_analysis::{extract_projection_filter, single_source_table};

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
    /// Multi-source (JOIN) -- cached physical plan with `LiveSourceExec`
    /// leaves. Each `collect()` re-runs `HashJoinExec::execute()`, which
    /// creates a fresh `OnceFut<JoinLeftData>` for the build side, so the
    /// hash table is rebuilt per cycle from the latest live-source data.
    CachedPhysical(Arc<dyn datafusion::physical_plan::ExecutionPlan>),
}

/// Row-shuffle config threaded in cluster mode. Pre-aggregate rows are
/// hashed by their GROUP BY columns; local rows feed `IncrementalAggState`,
/// remote rows are serialised and shipped via [`ShuffleSender`]. Remote
/// rows arriving on [`ShuffleReceiver`] are drained at the start of
/// every cycle and fed into the same accumulator.
///
/// Row-shuffle ships raw rows, not partial-aggregate state. Cheaper
/// to implement than two-stage partial aggregation, linearly more
/// bandwidth-hungry.
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
    having_cache: Option<super::HavingSqlCache>,
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
            having_cache: None,
            emit_changelog,
            idle_ttl_ms,
            #[cfg(feature = "cluster-unstable")]
            cluster_shuffle: None,
        }
    }

    /// Install the row-shuffle config. When present and the query
    /// resolves to the aggregate fast-path, each cycle's pre-aggregate
    /// rows are hash-routed across the cluster before reaching
    /// `IncrementalAggState`.
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
            // Multi-source (JOIN): bake the physical plan once. Source leaves
            // are `LiveSourceExec`; each cycle's `collect()` recreates the
            // HashJoinExec build side from fresh data.
            let physical = self
                .ctx
                .state()
                .create_physical_plan(&plan)
                .await
                .map_err(|e| DbError::query_pipeline(&*self.op_name, &e))?;
            self.log_tier(false);
            self.state = QueryState::CachedPhysical(physical);
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
                    if let Some(physical) = agg_state.cached_pre_agg_physical() {
                        super::execute_cached_physical(&self.ctx, &self.op_name, physical).await?
                    } else {
                        return Err(DbError::Pipeline(format!(
                            "[LDB-8051] query '{}': compiled pre-agg failed and no cached plan: {e}",
                            self.op_name
                        )));
                    }
                }
            }
        } else if let Some(physical) = agg_state.cached_pre_agg_physical() {
            super::execute_cached_physical(&self.ctx, &self.op_name, physical).await?
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

        #[cfg(feature = "cluster-unstable")]
        let num_group_cols = agg_state.num_group_cols();

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

        let result = if eviction.is_empty() {
            batches
        } else {
            eviction.extend(batches);
            eviction
        };

        #[cfg(feature = "cluster-unstable")]
        return self.suppress_restoring_output(result, num_group_cols);
        #[cfg(not(feature = "cluster-unstable"))]
        Ok(result)
    }

    /// Drop output rows whose group key hashes to a vnode currently
    /// [`Restoring`](laminar_core::state::VnodeLifecycleState::Restoring), so a
    /// downstream MV/sink never observes a transiently-incomplete aggregate
    /// for a vnode whose committed state hasn't been merged in yet. The vnode
    /// flips back to `Active` (and its rows resume emitting) once the graph
    /// applies its rehydrated slice. No-op when nothing is restoring — the
    /// common case, gated by a single atomic scan.
    #[cfg(feature = "cluster-unstable")]
    fn suppress_restoring_output(
        &self,
        batches: Vec<RecordBatch>,
        num_group_cols: usize,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let Some(ref cfg) = self.cluster_shuffle else {
            return Ok(batches);
        };
        if !cfg.registry.any_restoring() {
            return Ok(batches);
        }
        let vnode_count = cfg.registry.vnode_count();
        let mut out = Vec::with_capacity(batches.len());
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let vnodes: Vec<u32> = if num_group_cols == 0 {
                vec![0; batch.num_rows()]
            } else {
                let cols: Vec<usize> = (0..num_group_cols).collect();
                laminar_core::shuffle::row_vnodes(&batch, &cols, vnode_count)
            };
            let keep: Vec<bool> = vnodes
                .iter()
                .map(|&v| !cfg.registry.is_restoring(v))
                .collect();
            let kept = keep.iter().filter(|&&k| k).count();
            if kept == batch.num_rows() {
                out.push(batch);
            } else if kept > 0 {
                let mask = arrow::array::BooleanArray::from(keep);
                let filtered = arrow::compute::filter_record_batch(&batch, &mask).map_err(|e| {
                    DbError::Pipeline(format!("restoring-vnode emission filter: {e}"))
                })?;
                out.push(filtered);
            }
            // kept == 0 → entire batch suppressed; drop it.
        }
        Ok(out)
    }

    /// Apply a HAVING predicate via SQL. First call builds a
    /// `LiveSourceProvider`-backed cache; subsequent calls swap batches into
    /// the handle and re-run the cached physical plan.
    async fn apply_having_sql(
        &mut self,
        batches: &[RecordBatch],
        having_sql: &str,
    ) -> Result<Vec<RecordBatch>, DbError> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        if self.having_cache.is_none() {
            tracing::warn!(
                query = %self.op_name,
                "HAVING filter compiled to PhysicalExpr failed -- using cached SQL plan"
            );
            let table_name = format!("__having_{}", self.op_name);
            self.having_cache = Some(
                super::HavingSqlCache::build(
                    &self.ctx,
                    &table_name,
                    batches[0].schema(),
                    having_sql,
                )
                .await?,
            );
        }
        self.having_cache
            .as_ref()
            .expect("just initialized")
            .apply(&self.ctx, &self.op_name, batches.to_vec())
            .await
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
        for &v in &row_vn {
            let owner = cfg.registry.owner(v);
            if owner.is_unassigned() {
                return Err(DbError::Pipeline(format!(
                    "[{op_name}] row-shuffle: vnode {v} is unassigned — refusing to drop rows"
                )));
            }
        }

        let (local_slices, remote_slices) = laminar_core::shuffle::slice_batch_by_targets(
            &batch,
            &row_vn,
            &cfg.registry,
            cfg.self_id,
        );

        for (_v, slice) in local_slices {
            local.push(slice);
        }

        for (owner, slice) in remote_slices {
            let msg = ShuffleMessage::VnodeData(op_name.to_string(), 0, slice);
            cfg.sender.send_to(owner.0, &msg).await.map_err(|e| {
                DbError::Pipeline(format!(
                    "[{op_name}] row-shuffle send_to peer {}: {e}",
                    owner.0
                ))
            })?;
        }
    }

    // Drain this query's currently-available remote rows into this cycle.
    // The receiver demuxes by stage, so a co-resident sharded operator
    // (e.g. a lookup-enrich join) doesn't steal our rows. Bound by channel
    // depth; never blocks. Rows that arrive after we drain land next cycle.
    for batch in cfg.receiver.drain_vnode_data_for(op_name) {
        if batch.num_rows() > 0 {
            local.push(batch);
        }
    }

    Ok(local)
}

/// Vnode per row, hashing the leading `num_group_cols` group columns.
/// `num_group_cols == 0` is a global aggregate — every row hashes to vnode 0
/// so a single owner produces output.
#[cfg(feature = "cluster-unstable")]
fn hash_rows_to_vnodes(batch: &RecordBatch, num_group_cols: usize, vnode_count: u32) -> Vec<u32> {
    if num_group_cols == 0 || batch.num_rows() == 0 {
        return vec![0; batch.num_rows()];
    }
    let columns: Vec<usize> = (0..num_group_cols).collect();
    laminar_core::shuffle::row_vnodes(batch, &columns, vnode_count)
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
            QueryState::CachedPhysical(ref physical) => {
                super::execute_cached_physical(&self.ctx, &self.op_name, physical).await
            }
        }
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        if matches!(self.state, QueryState::Uninit) {
            if let Some(ref cp) = self.pending_restore {
                let data = rkyv::to_bytes::<rkyv::rancor::Error>(cp)
                    .map(|v| v.to_vec())
                    .map_err(|e| {
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
        let data = rkyv::to_bytes::<rkyv::rancor::Error>(&cp)
            .map(|v| v.to_vec())
            .map_err(|e| {
                DbError::Pipeline(format!(
                    "checkpoint serialization for '{}': {e}",
                    self.op_name
                ))
            })?;
        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let cp: AggStateCheckpoint = rkyv::from_bytes::<AggStateCheckpoint, rkyv::rancor::Error>(
            &checkpoint.data,
        )
        .map_err(|e| {
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
            QueryState::Compiled(_) | QueryState::CachedPlan(_) | QueryState::CachedPhysical(_) => {
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

    #[cfg(feature = "cluster-unstable")]
    async fn ingest_shuffle(&mut self, _stage: &str, batch: RecordBatch, watermark: i64) -> Result<(), DbError> {
        // A peer's pre-aggregate rows — fold them into the accumulator so they
        // enter this snapshot, exactly as the per-cycle shuffle drain does.
        if matches!(self.state, QueryState::Uninit) {
            self.lazy_init().await?;
        }
        if let QueryState::Agg(ref mut agg) = self.state {
            agg.process_batch(&batch, watermark)?;
        }
        Ok(())
    }

    #[cfg(feature = "cluster-unstable")]
    #[allow(clippy::disallowed_types)] // cold checkpoint path; vnode-keyed map
    fn checkpoint_by_vnode(
        &mut self,
        vnode_count: u32,
    ) -> Result<Option<std::collections::HashMap<u32, bytes::Bytes>>, DbError> {
        let QueryState::Agg(ref mut agg_state) = self.state else {
            return Ok(None);
        };
        let per_vnode = agg_state.checkpoint_groups_by_vnode(vnode_count)?;
        if per_vnode.is_empty() {
            return Ok(None);
        }
        let mut out = std::collections::HashMap::with_capacity(per_vnode.len());
        for (vnode, cp) in per_vnode {
            let data = rkyv::to_bytes::<rkyv::rancor::Error>(&cp)
                .map(|v| v.to_vec())
                .map_err(|e| {
                    DbError::Pipeline(format!(
                        "per-vnode checkpoint serialization for '{}': {e}",
                        self.op_name
                    ))
                })?;
            out.insert(vnode, bytes::Bytes::from(data));
        }
        Ok(Some(out))
    }

    #[cfg(feature = "cluster-unstable")]
    fn apply_vnode_state(&mut self, vnode: u32, bytes: &[u8]) -> Result<(), DbError> {
        let cp: AggStateCheckpoint =
            rkyv::from_bytes::<AggStateCheckpoint, rkyv::rancor::Error>(bytes).map_err(|e| {
                DbError::Pipeline(format!(
                    "per-vnode state deserialization for '{}' vnode {vnode}: {e}",
                    self.op_name
                ))
            })?;
        match self.state {
            QueryState::Agg(ref mut agg_state) => {
                let merged = agg_state.merge_groups(&cp)?;
                tracing::debug!(
                    query = %self.op_name, vnode, groups = merged,
                    "applied rehydrated vnode aggregate state"
                );
            }
            QueryState::Uninit => {
                // Not yet initialized (rare — an owning node is normally
                // running). Fold into the pending restore; vnodes are disjoint
                // so concatenating their group lists is safe.
                match self.pending_restore {
                    Some(ref mut existing) if existing.fingerprint == cp.fingerprint => {
                        existing.groups.extend(cp.groups);
                        existing.last_emitted.extend(cp.last_emitted);
                    }
                    Some(_) => tracing::warn!(
                        query = %self.op_name, vnode,
                        "pending restore fingerprint mismatch — dropping rehydrated slice"
                    ),
                    None => self.pending_restore = Some(cp),
                }
            }
            _ => tracing::warn!(
                query = %self.op_name, vnode,
                "ignoring rehydrated vnode state for non-aggregate query"
            ),
        }
        Ok(())
    }
}
