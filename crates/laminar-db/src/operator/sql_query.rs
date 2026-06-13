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
#[cfg(feature = "cluster")]
use laminar_core::shuffle::ShuffleMessage;

use crate::aggregate_state::{
    apply_compiled_having, AggStateCheckpoint, CompiledProjection, IncrementalAggState,
};
use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
use crate::operator_graph::{try_evaluate_compiled, GraphOperator, OperatorCheckpoint};
use crate::sql_analysis::{extract_projection_filter, single_source_table};

#[cfg(feature = "state-tier")]
use bytes::Bytes;
#[cfg(feature = "state-tier")]
use rustc_hash::{FxHashMap, FxHashSet};
#[cfg(feature = "state-tier")]
use std::collections::VecDeque;
// Reply receivers are stored across cycles in `AggPromotion.inflight`, so they
// must be `Sync` (this operator's `process` future holds `&self` across an
// await); crossfire's `RxOneshot` is `!Sync`, so replies use tokio oneshot.
#[cfg(feature = "state-tier")]
use tokio::sync::oneshot;

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
#[cfg(feature = "cluster")]
#[derive(Clone)]
pub struct ClusterShuffleConfig {
    pub registry: Arc<laminar_core::state::VnodeRegistry>,
    pub sender: Arc<laminar_core::shuffle::ShuffleSender>,
    pub receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    pub self_id: laminar_core::state::NodeId,
}

#[cfg(feature = "cluster")]
impl std::fmt::Debug for ClusterShuffleConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterShuffleConfig")
            .field("self_id", &self.self_id)
            .finish_non_exhaustive()
    }
}

/// Async-decoupled promotion of demoted (cold-tier) aggregate state.
///
/// When a pre-aggregate row's group hashes to a vnode that was demoted to
/// the cold tier, the row cannot process inline — the group's accumulators
/// are not in memory. Mirroring the lookup-enrich / AI-operator pattern,
/// the row is deferred, a Ring-1 fetch of the vnode's slice runs off the
/// compute thread (the cold read never stalls the pipeline), and the row
/// replays a later cycle once the slice is merged back in. The output
/// watermark is held behind the oldest deferred row so its eventual
/// contribution is not treated as late downstream.
#[cfg(feature = "state-tier")]
struct AggPromotion {
    op_name: Arc<str>,
    tier: crate::state_tier::TierTx,
    /// Pre-aggregate batches deferred because they touch a cold vnode, each
    /// with the watermark it was first ingested at (replayed at that
    /// watermark, like lookup-enrich, so a late replay is not mis-aged).
    deferred: VecDeque<(i64, RecordBatch)>,
    /// Vnodes with a fetch in flight → its reply channel.
    inflight: FxHashMap<u32, oneshot::Receiver<Result<Option<Bytes>, DbError>>>,
    /// Backpressure cap: stop accepting input past this many deferred rows.
    max_deferred_rows: usize,
}

#[cfg(feature = "state-tier")]
const MAX_DEFERRED_PROMOTION_ROWS: usize = 8192;

#[cfg(feature = "state-tier")]
impl AggPromotion {
    fn new(op_name: Arc<str>, tier: crate::state_tier::TierTx) -> Self {
        Self {
            op_name,
            tier,
            deferred: VecDeque::new(),
            inflight: FxHashMap::default(),
            max_deferred_rows: MAX_DEFERRED_PROMOTION_ROWS,
        }
    }

    /// Poll every in-flight fetch; return the slices that resolved this
    /// cycle as `(vnode, slice_bytes)` (a `None` slice = the tier no longer
    /// has it, surfaced so the caller can re-fetch). Errored or worker-gone
    /// fetches are dropped so the still-cold deferred batch re-issues them.
    fn drain_ready(&mut self) -> Vec<(u32, Option<Bytes>)> {
        let mut ready = Vec::new();
        let mut still: FxHashMap<u32, oneshot::Receiver<Result<Option<Bytes>, DbError>>> =
            FxHashMap::default();
        for (vnode, mut rx) in self.inflight.drain() {
            match rx.try_recv() {
                Ok(Ok(slice)) => ready.push((vnode, slice)),
                Ok(Err(e)) => tracing::warn!(
                    operator = %self.op_name, vnode, error = %e,
                    "cold-tier promotion fetch failed — will retry"
                ),
                Err(oneshot::error::TryRecvError::Empty) => {
                    still.insert(vnode, rx);
                }
                Err(oneshot::error::TryRecvError::Closed) => tracing::warn!(
                    operator = %self.op_name, vnode,
                    "cold-tier worker dropped a promotion fetch"
                ),
            }
        }
        self.inflight = still;
        ready
    }

    /// Start a fetch for `vnode` unless one is already in flight. A full or
    /// closed channel is treated as backpressure: the deferred batch retries
    /// the fetch next cycle.
    fn issue_fetch(&mut self, vnode: u32) {
        if self.inflight.contains_key(&vnode) {
            return;
        }
        let (reply, rx) = oneshot::channel();
        let req = crate::state_tier::TierRequest::Fetch {
            operator: Arc::clone(&self.op_name),
            vnode,
            reply,
        };
        // crossfire `try_send`: full or closed channel → backpressure, retry.
        if self.tier.try_send(req).is_ok() {
            self.inflight.insert(vnode, rx);
        }
    }

    /// Best-effort delete of a vnode's slice after promotion put it back in
    /// memory, so resident bytes track only truly-cold vnodes. Fire-and-forget:
    /// a full channel just leaves the slice for the next demotion to overwrite
    /// or restart to wipe. Race-safe against a re-demote — promotion marks the
    /// vnode dirty (no re-demote until the next capture, after this Drop is
    /// enqueued) and the request channel is FIFO.
    fn drop_slice(&self, vnode: u32) {
        let (reply, _rx) = oneshot::channel();
        let req = crate::state_tier::TierRequest::Drop {
            operator: Arc::clone(&self.op_name),
            vnode,
            reply,
        };
        let _ = self.tier.try_send(req);
    }

    fn defer(&mut self, watermark: i64, batch: RecordBatch) {
        self.deferred.push_back((watermark, batch));
    }

    fn take_deferred(&mut self) -> Vec<(i64, RecordBatch)> {
        self.deferred.drain(..).collect()
    }

    fn deferred_rows(&self) -> usize {
        self.deferred.iter().map(|(_, b)| b.num_rows()).sum()
    }

    fn min_deferred_watermark(&self) -> Option<i64> {
        self.deferred.iter().map(|(wm, _)| *wm).min()
    }
}

/// Whole-operator checkpoint for the aggregate path: the group state plus
/// any pre-aggregate batches the cold-tier promotion buffer has deferred
/// (empty unless the state tier is active). Replaces the bare
/// `AggStateCheckpoint` blob so a checkpoint taken mid-promotion does not
/// lose deferred rows — the source counts them consumed, so only the
/// operator can carry them across a restart.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct AggOpCheckpoint {
    /// In-memory group state — only the resident (hot) vnodes; demoted
    /// vnodes are listed in `cold_vnodes` and recovered from their durable
    /// partials on restart.
    agg: Option<AggStateCheckpoint>,
    /// `(ingest watermark, IPC-serialized pre-aggregate batch)`.
    deferred: Vec<(i64, Vec<u8>)>,
    /// Vnodes demoted to the cold tier at capture time. Their groups are
    /// absent from `agg`; restart recovery replays each from its vnode
    /// partial (the tier itself is wiped on restart). Empty without tiering.
    cold_vnodes: Vec<u32>,
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
    #[cfg(feature = "cluster")]
    cluster_shuffle: Option<ClusterShuffleConfig>,
    /// Cold-tier promotion buffer, attached when the engine wires a state
    /// tier and this operator runs the vnode-sharded aggregate path.
    #[cfg(feature = "state-tier")]
    promotion: Option<AggPromotion>,
    /// Cold-tier sender held until `lazy_init` builds the aggregate state;
    /// then moved into `promotion`. `None` = no tier wired.
    #[cfg(feature = "state-tier")]
    tier_sender: Option<crate::state_tier::TierTx>,
    /// Deferred batches restored from a checkpoint, replayed into the
    /// promotion buffer once `lazy_init` builds it.
    #[cfg(feature = "state-tier")]
    pending_deferred: Vec<(i64, RecordBatch)>,
    /// Vnodes that were demoted at the restored checkpoint; the restart path
    /// drains this (`take_tier_cold_vnodes`) to know which durable partials
    /// to replay back into `pending_restore` before `lazy_init`.
    #[cfg(feature = "state-tier")]
    pending_cold_rehydrate: Vec<u32>,
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
            #[cfg(feature = "cluster")]
            cluster_shuffle: None,
            #[cfg(feature = "state-tier")]
            promotion: None,
            #[cfg(feature = "state-tier")]
            tier_sender: None,
            #[cfg(feature = "state-tier")]
            pending_deferred: Vec::new(),
            #[cfg(feature = "state-tier")]
            pending_cold_rehydrate: Vec::new(),
        }
    }

    /// Install the row-shuffle config. When present and the query
    /// resolves to the aggregate fast-path, each cycle's pre-aggregate
    /// rows are hash-routed across the cluster before reaching
    /// `IncrementalAggState`.
    #[cfg(feature = "cluster")]
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
                // Activate cold-tier promotion now the aggregate path is
                // live: move the held sender into a promotion buffer and
                // re-queue any deferred batches restored from a checkpoint.
                #[cfg(feature = "state-tier")]
                if let Some(tier) = self.tier_sender.take() {
                    let mut promo = AggPromotion::new(Arc::clone(&self.op_name), tier);
                    for (wm, batch) in self.pending_deferred.drain(..) {
                        promo.defer(wm, batch);
                    }
                    self.promotion = Some(promo);
                }
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
        #[cfg(feature = "cluster")]
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

        // Step 2: Feed pre-agg batches to incremental accumulators. When a
        // cold tier is wired, rows hitting a demoted vnode route through the
        // promotion buffer instead (fetched back off-thread, replayed later).
        #[cfg(feature = "state-tier")]
        if self.promotion.is_some() {
            return self
                .process_with_promotion(pre_agg_batches, watermark)
                .await;
        }
        for batch in &pre_agg_batches {
            agg_state.process_batch(batch, watermark)?;
        }

        self.emit_agg_output(watermark).await
    }

    /// Aggregate ingest with cold-tier promotion. Drains resolved fetches
    /// into memory, splits new + previously-deferred rows into those whose
    /// groups are resident (processed now) and those still demoted
    /// (re-deferred, fetch issued), then emits — the output watermark is
    /// held behind the oldest deferred row by [`Self::watermark_hold`].
    #[cfg(feature = "state-tier")]
    async fn process_with_promotion(
        &mut self,
        pre_agg_batches: Vec<RecordBatch>,
        watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        // 1. Apply slices whose fetch resolved this cycle.
        let ready = self
            .promotion
            .as_mut()
            .map(AggPromotion::drain_ready)
            .unwrap_or_default();
        for (vnode, slice) in ready {
            match slice {
                Some(bytes) => {
                    self.apply_vnode_state(vnode, &bytes)?;
                    // Slice is back in memory and the vnode is now dirty (so it
                    // can't be re-demoted before the next capture) — delete the
                    // redundant tier copy so resident bytes track only cold vnodes.
                    if let Some(p) = self.promotion.as_ref() {
                        p.drop_slice(vnode);
                    }
                }
                None => {
                    // The tier no longer has the slice (should not happen
                    // mid-run). Re-issue; the deferred batch keeps waiting.
                    if let Some(p) = self.promotion.as_mut() {
                        p.issue_fetch(vnode);
                    }
                }
            }
        }

        // 2. Candidate set = previously-deferred batches + this cycle's input.
        let mut candidates = self
            .promotion
            .as_mut()
            .map(AggPromotion::take_deferred)
            .unwrap_or_default();
        for batch in pre_agg_batches {
            candidates.push((watermark, batch));
        }

        // 3. Split by whether every touched vnode is now resident.
        let num_group_cols = match self.state {
            QueryState::Agg(ref agg) => agg.num_group_cols(),
            _ => unreachable!("process_with_promotion on non-agg state"),
        };
        let vnode_count = self
            .cluster_shuffle
            .as_ref()
            .map_or(1, |c| c.registry.vnode_count());
        let cold: FxHashSet<u32> = match self.state {
            QueryState::Agg(ref agg) => agg.cold_vnodes().clone(),
            _ => FxHashSet::default(),
        };

        let mut to_process: Vec<(i64, RecordBatch)> = Vec::new();
        for (wm, batch) in candidates {
            let touched = cold_vnodes_touched(&batch, num_group_cols, vnode_count, &cold);
            if touched.is_empty() {
                to_process.push((wm, batch));
            } else if let Some(p) = self.promotion.as_mut() {
                for v in touched {
                    p.issue_fetch(v);
                }
                p.defer(wm, batch);
            }
        }

        // 4. Fold the resident rows in, each at its own ingest watermark.
        {
            let QueryState::Agg(ref mut agg_state) = self.state else {
                unreachable!();
            };
            for (wm, batch) in &to_process {
                agg_state.process_batch(batch, *wm)?;
            }
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

        #[cfg(feature = "cluster")]
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

        #[cfg(feature = "cluster")]
        return self.suppress_restoring_output(result, num_group_cols);
        #[cfg(not(feature = "cluster"))]
        Ok(result)
    }

    /// Drop output rows whose group key hashes to a vnode currently
    /// [`Restoring`](laminar_core::state::VnodeLifecycleState::Restoring), so a
    /// downstream MV/sink never observes a transiently-incomplete aggregate
    /// for a vnode whose committed state hasn't been merged in yet. The vnode
    /// flips back to `Active` (and its rows resume emitting) once the graph
    /// applies its rehydrated slice. No-op when nothing is restoring — the
    /// common case, gated by a single atomic scan.
    #[cfg(feature = "cluster")]
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
#[cfg(feature = "cluster")]
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
#[cfg(feature = "cluster")]
fn hash_rows_to_vnodes(batch: &RecordBatch, num_group_cols: usize, vnode_count: u32) -> Vec<u32> {
    if num_group_cols == 0 || batch.num_rows() == 0 {
        return vec![0; batch.num_rows()];
    }
    let columns: Vec<usize> = (0..num_group_cols).collect();
    laminar_core::shuffle::row_vnodes(batch, &columns, vnode_count)
}

/// The demoted vnodes a pre-aggregate batch's rows hash to. Uses the same
/// `row_vnodes` hashing as the cluster shuffle and the per-vnode capture, so
/// a row's vnode here is exactly the one its group's slice was demoted under.
/// Empty `cold` (the common case) short-circuits before any row hashing.
#[cfg(feature = "state-tier")]
fn cold_vnodes_touched(
    batch: &RecordBatch,
    num_group_cols: usize,
    vnode_count: u32,
    cold: &FxHashSet<u32>,
) -> Vec<u32> {
    if cold.is_empty() || batch.num_rows() == 0 {
        return Vec::new();
    }
    let mut touched: FxHashSet<u32> = FxHashSet::default();
    for v in hash_rows_to_vnodes(batch, num_group_cols, vnode_count) {
        if cold.contains(&v) {
            touched.insert(v);
        }
    }
    touched.into_iter().collect()
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
                #[cfg(feature = "cluster")]
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
        let agg: Option<AggStateCheckpoint> = match self.state {
            QueryState::Uninit => self.pending_restore.clone(),
            QueryState::Agg(ref mut agg_state) => Some(agg_state.checkpoint_groups()?),
            QueryState::Compiled(_) | QueryState::CachedPlan(_) | QueryState::CachedPhysical(_) => {
                None
            }
        };
        // Carry cold-tier promotion-deferred batches (the source counts them
        // consumed, so only the operator can restore them). Always empty
        // without the state tier.
        #[cfg(feature = "state-tier")]
        let deferred: Vec<(i64, Vec<u8>)> = {
            let mut out = Vec::new();
            let batches = self
                .promotion
                .as_ref()
                .map(|p| p.deferred.iter())
                .into_iter()
                .flatten()
                .chain(self.pending_deferred.iter());
            for (wm, batch) in batches {
                let blob = laminar_core::serialization::serialize_batch_stream(batch)
                    .map_err(|e| DbError::Pipeline(format!("promotion checkpoint: {e}")))?;
                out.push((*wm, blob));
            }
            out
        };
        #[cfg(not(feature = "state-tier"))]
        let deferred: Vec<(i64, Vec<u8>)> = Vec::new();

        // Demoted vnodes are absent from `agg` (their groups are not in
        // memory); list them so restart recovery replays them from partials.
        #[cfg(feature = "state-tier")]
        let cold_vnodes: Vec<u32> = match self.state {
            QueryState::Agg(ref agg_state) => agg_state.cold_vnodes().iter().copied().collect(),
            _ => Vec::new(),
        };
        #[cfg(not(feature = "state-tier"))]
        let cold_vnodes: Vec<u32> = Vec::new();

        if agg.is_none() && deferred.is_empty() && cold_vnodes.is_empty() {
            return Ok(None);
        }
        let cp = AggOpCheckpoint {
            agg,
            deferred,
            cold_vnodes,
        };
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
        let cp: AggOpCheckpoint = rkyv::from_bytes::<AggOpCheckpoint, rkyv::rancor::Error>(
            &checkpoint.data,
        )
        .map_err(|e| {
            DbError::Pipeline(format!(
                "checkpoint deserialization for '{}': {e}",
                self.op_name
            ))
        })?;

        // Re-queue cold-tier promotion-deferred batches. Before `lazy_init`
        // they wait in `pending_deferred`; after, straight into the buffer.
        #[cfg(feature = "state-tier")]
        for (wm, blob) in cp.deferred {
            let batch = laminar_core::serialization::deserialize_batch_stream(&blob)
                .map_err(|e| DbError::Pipeline(format!("promotion restore: {e}")))?;
            match self.promotion.as_mut() {
                Some(p) => p.defer(wm, batch),
                None => self.pending_deferred.push((wm, batch)),
            }
        }
        #[cfg(not(feature = "state-tier"))]
        if !cp.deferred.is_empty() {
            tracing::warn!(
                query = %self.op_name,
                count = cp.deferred.len(),
                "dropping checkpointed promotion-deferred batches — \
                 this binary has no state-tier support"
            );
        }

        // Remember which vnodes were demoted so the restart path can replay
        // their durable partials into `pending_restore` before `lazy_init`.
        #[cfg(feature = "state-tier")]
        if !cp.cold_vnodes.is_empty() {
            self.pending_cold_rehydrate = cp.cold_vnodes;
        }

        let Some(agg_cp) = cp.agg else {
            return Ok(());
        };
        match self.state {
            QueryState::Agg(ref mut agg_state) => {
                agg_state.restore_groups(&agg_cp)?;
            }
            QueryState::Uninit => {
                self.pending_restore = Some(agg_cp);
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

    /// Hold the output watermark behind the oldest pre-aggregate row deferred
    /// for cold-tier promotion, so its eventual contribution is not dropped as
    /// late by a downstream window. `None` when nothing is deferred.
    #[cfg(feature = "state-tier")]
    fn watermark_hold(&self) -> Option<i64> {
        self.promotion
            .as_ref()
            .and_then(AggPromotion::min_deferred_watermark)
    }

    /// Backpressure the source once too many rows are parked waiting on
    /// promotion, so a storm of cold-vnode hits can't grow the buffer
    /// unbounded.
    #[cfg(feature = "state-tier")]
    fn wants_input(&self) -> bool {
        self.promotion
            .as_ref()
            .is_none_or(|p| p.deferred_rows() < p.max_deferred_rows)
    }

    #[cfg(feature = "cluster")]
    async fn ingest_shuffle(
        &mut self,
        _stage: &str,
        batch: RecordBatch,
        watermark: i64,
    ) -> Result<(), DbError> {
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

    #[cfg(feature = "cluster")]
    #[allow(clippy::disallowed_types)] // cold checkpoint path; vnode-keyed map
    fn checkpoint_by_vnode(
        &mut self,
        vnode_count: u32,
    ) -> Result<
        Option<std::collections::HashMap<u32, crate::checkpoint_coordinator::StagedSlice>>,
        DbError,
    > {
        use crate::checkpoint_coordinator::StagedSlice;
        let QueryState::Agg(ref mut agg_state) = self.state else {
            return Ok(None);
        };
        let per_vnode = agg_state.checkpoint_groups_by_vnode(vnode_count)?;
        #[cfg(feature = "state-tier")]
        let cold: Vec<u32> = agg_state.cold_vnodes().iter().copied().collect();
        #[cfg(not(feature = "state-tier"))]
        let cold: Vec<u32> = Vec::new();
        if per_vnode.is_empty() && cold.is_empty() {
            return Ok(None);
        }
        let mut out = std::collections::HashMap::with_capacity(per_vnode.len() + cold.len());
        for (vnode, cp) in per_vnode {
            let data = rkyv::to_bytes::<rkyv::rancor::Error>(&cp)
                .map(|v| v.to_vec())
                .map_err(|e| {
                    DbError::Pipeline(format!(
                        "per-vnode checkpoint serialization for '{}': {e}",
                        self.op_name
                    ))
                })?;
            out.insert(vnode, StagedSlice::Bytes(bytes::Bytes::from(data)));
        }
        // Demoted vnodes have no groups in memory; stage a cold marker so
        // the coordinator references (or tier-fetches) their slice instead
        // of treating the vnode as emptied.
        for vnode in cold {
            out.insert(vnode, StagedSlice::Cold);
        }
        Ok(Some(out))
    }

    #[cfg(feature = "state-tier")]
    fn demote_vnode(&mut self, vnode: u32, vnode_count: u32) -> bool {
        match self.state {
            QueryState::Agg(ref mut agg_state) => agg_state.demote_vnode(vnode, vnode_count),
            _ => false,
        }
    }

    #[cfg(feature = "state-tier")]
    fn can_demote(&self, vnode: u32, vnode_count: u32) -> bool {
        match self.state {
            QueryState::Agg(ref agg_state) => agg_state.can_demote(vnode, vnode_count),
            _ => false,
        }
    }

    #[cfg(feature = "state-tier")]
    fn attach_state_tier(&mut self, tier: crate::state_tier::TierTx) {
        // Promotion only runs on the aggregate path. If the state is already
        // resolved to aggregate, build the buffer now; otherwise hold the
        // sender until `lazy_init` builds it.
        if matches!(self.state, QueryState::Agg(_)) {
            self.promotion = Some(AggPromotion::new(Arc::clone(&self.op_name), tier));
        } else {
            self.tier_sender = Some(tier);
        }
    }

    #[cfg(feature = "state-tier")]
    fn take_tier_cold_vnodes(&mut self) -> Vec<u32> {
        std::mem::take(&mut self.pending_cold_rehydrate)
    }

    #[cfg(feature = "cluster")]
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
                // Whether this is a rebalance acquisition or a promotion
                // from the cold tier, the vnode's state now lives in memory.
                // `mark_vnode_hot` clears the cold flag (promotion only);
                // `mark_vnode_dirty` then protects *this* vnode from demotion
                // until the next capture — without it a rebalance-acquired
                // vnode (never cold) could be demoted against stale tier bytes.
                #[cfg(feature = "state-tier")]
                {
                    agg_state.mark_vnode_hot(vnode);
                    agg_state.mark_vnode_dirty(vnode);
                }
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

#[cfg(all(test, feature = "state-tier"))]
mod promotion_tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_core::state::{NodeId, VnodeRegistry};
    use rustc_hash::FxHashMap;
    use std::time::Duration;

    const VNODES: u32 = 8;

    fn events_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]))
    }

    fn events_batch(keys: &[&str], vals: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            events_schema(),
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(Int64Array::from(vals.to_vec())),
            ],
        )
        .unwrap()
    }

    /// `key -> total` for the changelog inserts (weight > 0) in the output.
    fn inserts(batches: &[RecordBatch]) -> FxHashMap<String, i64> {
        let mut out = FxHashMap::default();
        for b in batches {
            if b.num_rows() == 0 {
                continue;
            }
            let key = b
                .column(b.schema().index_of("key").unwrap())
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let total = b
                .column(b.schema().index_of("total").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            // The Z-set weight is the trailing column in changelog output.
            let weight = b
                .column(b.num_columns() - 1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                if weight.value(i) > 0 {
                    out.insert(key.value(i).to_string(), total.value(i));
                }
            }
        }
        out
    }

    async fn single_node_shuffle() -> ClusterShuffleConfig {
        let registry = Arc::new(VnodeRegistry::new(VNODES));
        let assignment: Arc<[NodeId]> = (0..VNODES).map(|_| NodeId(1)).collect::<Vec<_>>().into();
        registry.set_assignment(assignment);
        let sender = laminar_core::shuffle::ShuffleSender::new(1);
        let receiver = Arc::new(
            laminar_core::shuffle::ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap(),
        );
        ClusterShuffleConfig {
            registry,
            sender: Arc::new(sender),
            receiver,
            self_id: NodeId(1),
        }
    }

    async fn build_op(tier: crate::state_tier::TierTx) -> SqlQueryOperator {
        let ctx = laminar_sql::create_session_context();
        let mem = datafusion::datasource::MemTable::try_new(
            events_schema(),
            vec![vec![events_batch(&["seed"], &[0])]],
        )
        .unwrap();
        ctx.register_table("events", Arc::new(mem)).unwrap();
        let mut op = SqlQueryOperator::new(
            "out",
            "SELECT key, SUM(val) AS total FROM events GROUP BY key",
            ctx,
            None,
            true, // emit_changelog — required for demotion
            None,
        );
        op.attach_cluster_shuffle(single_node_shuffle().await);
        op.attach_state_tier(tier);
        op
    }

    /// Establish groups, demote every vnode to the tier, then feed a new row
    /// for an existing key: it must defer (hold the watermark), promote its
    /// vnode off-thread, and replay so the aggregate reflects both the
    /// restored and the new contribution.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promotes_demoted_vnode_on_incoming_row() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            crate::state_tier::StateTierStore::open(tmp.path().join("tier"), None).unwrap(),
        );
        let tier_tx = crate::state_tier::spawn_worker(
            &tokio::runtime::Handle::current(),
            Arc::clone(&store),
            64,
        );
        let mut op = build_op(tier_tx).await;

        // Cycle 1: a=1, b=2, c=3.
        let out1 = op
            .process(&[vec![events_batch(&["a", "b", "c"], &[1, 2, 3])]], &[10])
            .await
            .unwrap();
        assert_eq!(inserts(&out1).get("a"), Some(&1));

        // Capture the clean baseline, persist every vnode's slice, demote all.
        let staged = op.checkpoint_by_vnode(VNODES).unwrap().unwrap();
        for (v, slice) in &staged {
            if let crate::checkpoint_coordinator::StagedSlice::Bytes(bytes) = slice {
                store.put("out", *v, bytes.as_ref()).unwrap();
            }
        }
        for &v in staged.keys() {
            assert!(op.demote_vnode(v, VNODES), "clean vnode must demote");
        }
        assert_eq!(op.estimated_state_bytes(), 0, "all groups demoted");

        // Cycle 2: a new row for 'a' hits a cold vnode → deferred, not emitted.
        let out2 = op
            .process(&[vec![events_batch(&["a"], &[5])]], &[20])
            .await
            .unwrap();
        assert!(
            !inserts(&out2).contains_key("a"),
            "a row for a demoted vnode must defer, not emit a partial aggregate"
        );
        assert_eq!(
            op.watermark_hold(),
            Some(20),
            "the deferred row must hold the output watermark"
        );

        // Idle cycles drive the promotion fetch + replay to completion.
        let mut final_a = None;
        for _ in 0..200 {
            let out = op.process(&[vec![]], &[30]).await.unwrap();
            if let Some(&t) = inserts(&out).get("a") {
                final_a = Some(t);
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert_eq!(
            final_a,
            Some(6),
            "promoted slice merged: original 1 + new 5"
        );
        assert_eq!(
            op.watermark_hold(),
            None,
            "nothing deferred after promotion"
        );
    }

    /// A checkpoint taken while a row is deferred for promotion must carry
    /// that row across operator restore (the source counts it consumed).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deferred_row_survives_checkpoint_restore() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            crate::state_tier::StateTierStore::open(tmp.path().join("tier"), None).unwrap(),
        );
        let tier_tx = crate::state_tier::spawn_worker(
            &tokio::runtime::Handle::current(),
            Arc::clone(&store),
            64,
        );
        let mut op = build_op(tier_tx.clone()).await;

        op.process(&[vec![events_batch(&["a"], &[1])]], &[10])
            .await
            .unwrap();
        let staged = op.checkpoint_by_vnode(VNODES).unwrap().unwrap();
        for (v, slice) in &staged {
            if let crate::checkpoint_coordinator::StagedSlice::Bytes(bytes) = slice {
                store.put("out", *v, bytes.as_ref()).unwrap();
            }
        }
        for &v in staged.keys() {
            assert!(op.demote_vnode(v, VNODES));
        }

        // Defer a row, then checkpoint mid-promotion.
        op.process(&[vec![events_batch(&["a"], &[5])]], &[20])
            .await
            .unwrap();
        assert_eq!(op.watermark_hold(), Some(20));
        let cp = op
            .checkpoint()
            .unwrap()
            .expect("deferred row → non-empty checkpoint");

        // The blob must carry the deferred row at its ingest watermark.
        let decoded: AggOpCheckpoint =
            rkyv::from_bytes::<AggOpCheckpoint, rkyv::rancor::Error>(&cp.data).unwrap();
        assert_eq!(
            decoded.deferred.len(),
            1,
            "deferred row must be checkpointed"
        );
        assert_eq!(decoded.deferred[0].0, 20, "carried at its ingest watermark");

        // Restoring into a fresh operator replays the row rather than dropping
        // it. The merge of the demoted slice's prior contribution (+1) is
        // restored only once restart recovery rehydrates cold vnodes from the
        // partials (Phase 4a); a fresh operator holds no demoted state, so the
        // replayed row stands alone — what matters here is that it is not lost.
        let mut op2 = build_op(tier_tx).await;
        op2.restore(cp).unwrap();
        let mut final_a = None;
        for _ in 0..200 {
            let out = op2.process(&[vec![]], &[30]).await.unwrap();
            if let Some(&t) = inserts(&out).get("a") {
                final_a = Some(t);
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert_eq!(
            final_a,
            Some(5),
            "the restored deferred row replays, not dropped"
        );
    }

    /// Simulates a restart: a tier operator demotes every vnode, checkpoints
    /// (the manifest blob then holds no groups, only the cold-vnode list),
    /// and a fresh operator restores from it and rehydrates each demoted
    /// vnode from its saved partial slice — exactly what the restart path
    /// does with `VnodeRehydrator`. Recovered state must be complete: a new
    /// row sees the rehydrated prior contribution.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restart_rehydrates_demoted_vnodes_from_partials() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            crate::state_tier::StateTierStore::open(tmp.path().join("tier"), None).unwrap(),
        );
        let tier_tx = crate::state_tier::spawn_worker(
            &tokio::runtime::Handle::current(),
            Arc::clone(&store),
            64,
        );
        let mut op = build_op(tier_tx.clone()).await;

        op.process(&[vec![events_batch(&["a", "b", "c"], &[1, 2, 3])]], &[10])
            .await
            .unwrap();

        // Capture every vnode's slice (the bytes a partial would carry), then
        // demote every vnode.
        let staged = op.checkpoint_by_vnode(VNODES).unwrap().unwrap();
        let mut saved: FxHashMap<u32, bytes::Bytes> = FxHashMap::default();
        for (v, slice) in &staged {
            if let crate::checkpoint_coordinator::StagedSlice::Bytes(bytes) = slice {
                saved.insert(*v, bytes.clone());
            }
        }
        for &v in staged.keys() {
            assert!(op.demote_vnode(v, VNODES));
        }

        // The manifest blob now omits the demoted groups but lists them.
        let manifest = op.checkpoint().unwrap().expect("non-empty manifest");
        let decoded: AggOpCheckpoint =
            rkyv::from_bytes::<AggOpCheckpoint, rkyv::rancor::Error>(&manifest.data).unwrap();
        assert!(
            decoded.agg.as_ref().is_some_and(|a| a.groups.is_empty()),
            "all vnodes demoted → manifest carries no groups"
        );
        let mut cold_listed: Vec<u32> = decoded.cold_vnodes.clone();
        cold_listed.sort_unstable();
        let mut cold_expected: Vec<u32> = staged.keys().copied().collect();
        cold_expected.sort_unstable();
        assert_eq!(
            cold_listed, cold_expected,
            "manifest lists the demoted vnodes"
        );

        // Fresh operator restores from the manifest, then the restart path
        // replays each demoted vnode from its partial slice.
        let mut op2 = build_op(tier_tx).await;
        op2.restore(manifest).unwrap();
        let cold = op2.take_tier_cold_vnodes();
        assert_eq!(cold.len(), cold_expected.len());
        for v in cold {
            let slice = saved
                .get(&v)
                .expect("a slice was saved for every demoted vnode");
            op2.apply_vnode_state(v, slice).unwrap();
        }

        // A new row for 'a' must reflect the rehydrated prior value (1 + 5),
        // proving the demoted state survived the simulated restart.
        let out = op2
            .process(&[vec![events_batch(&["a"], &[5])]], &[30])
            .await
            .unwrap();
        assert_eq!(
            inserts(&out).get("a"),
            Some(&6),
            "rehydrated demoted state: original 1 + new 5"
        );
    }
}
