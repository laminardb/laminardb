//! Operator graph: wires streaming SQL operators into a DAG and drives them in topological order.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use laminar_sql::datafusion::live_source::{LiveSourceHandle, LiveSourceProvider};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};

use crate::config::BackpressurePolicy;
use crate::db::exact_table_reference;
use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
#[cfg(feature = "cluster")]
use crate::operator::RetainedBatch;
use crate::sql_analysis::{
    apply_topk_filter, detect_asof_query, detect_stream_join_query, detect_temporal_probe_query,
    detect_temporal_query, detect_unbounded_join_steps, extract_table_references, has_join_clause,
    join_clause_count, StreamJoinDetection,
};
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::{
    OrderOperatorConfig, TemporalJoinTranslatorConfig, WindowOperatorConfig,
};

#[async_trait]
pub(crate) trait GraphOperator: Send {
    /// `watermarks[i]` is the upstream output watermark for `inputs[i]`.
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError>;

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError>;
    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Err(DbError::Checkpoint(
            "operator does not accept checkpoint state".into(),
        ))
    }

    /// Output watermark ceiling. Holds the watermark at `min(input_wm, hold)` so
    /// in-flight rows (e.g. async AI enrichment) aren't treated as late downstream.
    /// `None` = no hold.
    fn watermark_hold(&self) -> Option<i64> {
        None
    }

    /// Safe output watermark reconstructed from retained checkpoint work. Restore uses this seed
    /// before any cycle runs; returning a value asserts that the durable cut had reached it.
    #[cfg(feature = "cluster")]
    fn restored_output_watermark(&self) -> Option<i64> {
        None
    }

    /// Whether the operator can accept new input this cycle. When `false`, input
    /// stays buffered and the operator is still stepped with empty input to drain.
    fn wants_input(&self) -> bool {
        true
    }

    /// Retain a peer-shipped shuffle batch as channel state outside the normal `process` path so
    /// the barrier-aligned row and its pending downstream emission enter the snapshot together.
    #[cfg(feature = "cluster")]
    fn stage_checkpointed_shuffle(
        &mut self,
        stage: &str,
        _batch: RetainedBatch,
        _watermark: i64,
    ) -> Result<(), DbError> {
        Err(DbError::Pipeline(format!(
            "operator does not accept checkpointed shuffle stage '{stage}'"
        )))
    }

    /// Per-vnode state snapshot for cross-node rehydration. `None` for operators
    /// that don't key state by vnode (they recover from the whole-node manifest).
    #[cfg(feature = "cluster")]
    #[allow(clippy::disallowed_types)] // checkpoint path; vnode-keyed map
    fn checkpoint_by_vnode(
        &mut self,
        _vnode_count: u32,
    ) -> Result<
        Option<std::collections::HashMap<u32, crate::checkpoint_coordinator::StagedSlice>>,
        DbError,
    > {
        Ok(None)
    }

    /// Merge one vnode's rehydrated state slice into this operator.
    #[cfg(feature = "cluster")]
    fn apply_vnode_state(&mut self, _vnode: u32, _bytes: &[u8]) -> Result<(), DbError> {
        Ok(())
    }

    /// Replay one operator's recovery chain for a vnode: a FULL base then ordered deltas.
    #[cfg(feature = "cluster")]
    fn apply_vnode_chain(
        &mut self,
        _vnode: u32,
        _base: &[u8],
        _deltas: &[&[u8]],
    ) -> Result<(), DbError> {
        Ok(())
    }

    /// Drop in-memory state for vnodes this node lost on a rebalance, before a later authoritative
    /// vnode image is installed. Default no-op; only vnode-sharded aggregates act on it.
    #[cfg(feature = "cluster")]
    fn drop_owned_vnodes(&mut self, _revoked: &FxHashSet<u32>) -> Result<(), DbError> {
        Ok(())
    }

    /// Force the next delta capture to re-base FULL after a failed epoch (destructive capture
    /// cleared the dirty sets before durability). Default no-op; only delta aggregates act.
    #[cfg(feature = "cluster")]
    fn force_full_rebase(&mut self) {}
}

pub(crate) struct OperatorCheckpoint {
    pub data: Vec<u8>,
}

enum GateDecision {
    Run,
    Skip,
    Fail,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GraphExecutionMode {
    Normal,
    CheckpointDrain,
}

const STATS_SAMPLE_INTERVAL: u64 = 32;

// std HashMap: rkyv supports HashMap<K,V> natively but not FxHashMap; checkpoint path only.
#[allow(clippy::disallowed_types)]
pub(crate) type OperatorStateMap = std::collections::HashMap<String, Vec<u8>>;

/// Persisted operator-graph state ABI.
pub(crate) const GRAPH_CHECKPOINT_VERSION: u32 = 4;

#[derive(Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct GraphCheckpoint {
    pub version: u32,
    pub operators: OperatorStateMap,
}

struct GraphNode {
    name: Arc<str>,
    operator: Box<dyn GraphOperator>,
    input_port_count: usize,
    output_routes: Vec<(usize, u8)>,
    removed: bool,
}

struct GraphEdge {
    source: usize,
    target: usize,
}

struct SourcePassthrough;

#[async_trait]
impl GraphOperator for SourcePassthrough {
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(inputs.first().cloned().unwrap_or_default())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
}

struct TombstonedOperator;

#[async_trait]
impl GraphOperator for TombstonedOperator {
    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
}

struct SqlFilterOperator {
    filter_sql: String,
    ctx: SessionContext,
    tmp_table: String,
    cache: Option<crate::operator::LiveSqlCache>,
}

impl SqlFilterOperator {
    fn new(filter_sql: String, ctx: SessionContext, node_name: &str) -> Self {
        let tmp_table = format!(
            "__prefilter_{}",
            node_name.replace(|c: char| !c.is_alphanumeric(), "_")
        );
        Self {
            filter_sql,
            ctx,
            tmp_table,
            cache: None,
        }
    }
}

#[async_trait]
impl GraphOperator for SqlFilterOperator {
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let batches = inputs.first().cloned().unwrap_or_default();
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(Vec::new());
        }

        if self.cache.is_none() {
            let schema = batches[0].schema();
            let sql = format!("SELECT * FROM {} WHERE {}", self.tmp_table, self.filter_sql);
            let cache = crate::operator::LiveSqlCache::build(
                &self.ctx,
                &self.tmp_table,
                schema,
                &sql,
                "pre-filter",
            )
            .await?;
            self.cache = Some(cache);
        }

        self.cache
            .as_ref()
            .unwrap()
            .apply("pre-filter", batches)
            .await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
}

/// Enriches an incremental MV's changelog with a static dimension, preserving `__weight`. Re-creates
/// the physical plan each cycle so the hash join's `OnceAsync` build side doesn't freeze on cycle-1 temp.
struct ChangelogEnrichOperator {
    join_sql: String,
    ctx: SessionContext,
    handle: Option<LiveSourceHandle>,
    logical: Option<datafusion_expr::LogicalPlan>,
}

impl ChangelogEnrichOperator {
    fn new(ctx: SessionContext, join_sql: String) -> Self {
        Self {
            join_sql,
            ctx,
            handle: None,
            logical: None,
        }
    }
}

#[async_trait]
impl GraphOperator for ChangelogEnrichOperator {
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let batches = inputs.first().cloned().unwrap_or_default();
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(Vec::new());
        }
        if self.handle.is_none() {
            let provider = LiveSourceProvider::new(batches[0].schema());
            self.handle = Some(provider.handle());
            let tmp = crate::sql_analysis::CHANGELOG_ENRICH_TMP;
            let _ = self.ctx.deregister_table(exact_table_reference(tmp));
            self.ctx
                .register_table(exact_table_reference(tmp), Arc::new(provider))
                .map_err(|e| DbError::Pipeline(format!("changelog-enrich register temp: {e}")))?;
            let logical = self
                .ctx
                .sql(&self.join_sql)
                .await
                .map_err(|e| DbError::query_pipeline("changelog-enrich", &e))?
                .logical_plan()
                .clone();
            self.logical = Some(logical);
        }
        self.handle.as_ref().unwrap().swap(batches);
        // Fresh physical plan each cycle resets the hash join's cached build side.
        let physical = self
            .ctx
            .state()
            .create_physical_plan(self.logical.as_ref().unwrap())
            .await
            .map_err(|e| DbError::query_pipeline("changelog-enrich", &e))?;
        datafusion::physical_plan::collect(physical, self.ctx.task_ctx())
            .await
            .map_err(|e| DbError::query_pipeline("changelog-enrich", &e))
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
}

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShuffleAlignmentOutcome {
    Aligned,
    Aborted,
    ScopeCancelledBeforeStaging,
}

#[allow(clippy::struct_excessive_bools)] // distinct independent flags, not a state enum
pub(crate) struct OperatorGraph {
    nodes: Vec<GraphNode>,
    // Node indices are embedded in edges and parallel buffers. Live DDL runs at
    // the coordinator safe point, where a fully detached tombstone can be reused.
    free_node_ids: Vec<usize>,
    edges: Vec<GraphEdge>,
    topo_order: Vec<usize>,
    topo_dirty: bool,
    // Failure domain (connected component) per node; a fatal error is isolated to its domain.
    node_domain: Vec<usize>,
    domain_count: usize,
    // When set, `compute_node_domains` cuts at source nodes so shared-source queries become
    // distinct domains. Off: shared-source queries fuse into one domain.
    shared_source_isolation: bool,
    // Per-operator cap on the preserved input a faulted domain replays from. Past it, replay
    // is abandoned and the input dropped (EO recovery).
    max_replay_buffer_bytes: usize,
    // Local source names whose domain faulted this cycle; drained by `take_cycle_failures`.
    cycle_failed_sources: FxHashSet<Arc<str>>,
    cycle_any_failed: bool,
    // Local sources whose graph work remains buffered for replay. `cycle_any_deferred` also
    // covers remote-only cluster work, where there is no local source cursor to retain.
    cycle_deferred_sources: FxHashSet<Arc<str>>,
    cycle_any_deferred: bool,
    source_map: FxHashMap<Arc<str>, usize>,
    source_list: Vec<(Arc<str>, usize)>,
    source_node_ids: FxHashSet<usize>,
    output_map: FxHashMap<Arc<str>, usize>,
    // Reverse of `output_map` (node id → is an output); rebuilt in `compute_topo_order`.
    output_node_ids: FxHashSet<usize>,
    input_bufs: Vec<Vec<Vec<RecordBatch>>>,
    input_buf_bytes: Vec<Vec<usize>>,
    input_sources: Vec<Vec<usize>>,
    output_watermarks: Vec<i64>,
    max_input_buf_batches: usize,
    max_input_buf_bytes: Option<usize>,
    backpressure_policy: BackpressurePolicy,
    query_budget_ns: u64,
    deferred_scan_offset: usize,
    stats_tick: u64,
    ctx: SessionContext,
    prom: Option<Arc<EngineMetrics>>,
    lookup_registry: Option<Arc<laminar_sql::datafusion::LookupTableRegistry>>,
    source_schemas: FxHashMap<String, SchemaRef>,
    temporal_configs: Vec<(String, TemporalJoinTranslatorConfig)>,
    depends_on_stream: FxHashSet<usize>,
    order_configs: FxHashMap<usize, OrderOperatorConfig>,
    // Covers source tables and intermediates (lazily created on first operator output).
    live_handles: FxHashMap<String, LiveSourceHandle>,
    // None unless [ai]/[models] are configured.
    ai_runtime: Option<Arc<crate::ai::AiRuntime>>,
    // Must be the main multi-threaded runtime; Ring-1 workers (AI, lookup-enrich) spawn here.
    main_runtime_handle: Option<tokio::runtime::Handle>,
    // Lookup table name → column names; routes lookup-enrich joins to the async operator.
    partial_lookup_tables: FxHashMap<String, Vec<String>>,
    // Incremental MV names (changelog producers); routes a `changelog ⋈ static dim` join to the
    // ChangelogEnrich operator. Kept current as incremental MVs are added.
    incremental_tables: FxHashSet<String>,
    // Static reference/dimension table names — valid right sides of a changelog enrich join.
    reference_tables: FxHashSet<String>,
    // Plan-time errors from add_query (returns ()); surfaced by take_build_errors at start.
    build_errors: Vec<DbError>,
    // Whole-graph restore is a one-shot startup transition and closes before the first cycle.
    whole_restore_open: bool,
    #[cfg(feature = "cluster")]
    cluster_shuffle: Option<crate::operator::sql_query::ClusterShuffleConfig>,
    #[cfg(feature = "cluster")]
    last_execution_assignment_version: Option<u64>,
    // `Some(chain_bound)` enables incremental delta checkpoints on aggregate operators; the delta
    // chain then becomes the PRIMARY agg checkpoint (skip the whole-node manifest, recover from the chain).
    #[cfg(feature = "cluster")]
    delta_chain_bound: Option<u32>,
    // Per-vnode partials are the authoritative agg checkpoint (cluster + durable backend);
    // whole-node capture into the per-node-incomplete manifest is skipped.
    #[cfg(feature = "cluster")]
    vnode_partials_authoritative: bool,
    // Set from the shuffle registry in cluster mode.
    #[cfg(feature = "cluster")]
    vnode_count: Option<u32>,
    // Staged per-vnode rehydration map; drained at the top of each cycle.
    #[cfg(feature = "cluster")]
    #[allow(clippy::disallowed_types)] // shares the DB's std-HashMap-typed handle
    rehydrated_vnode_state:
        Option<Arc<parking_lot::Mutex<std::collections::HashMap<u32, crate::db::RehydratedVnode>>>>,
    // Staged set of vnodes lost on rebalance; drained at the top of each cycle to drop their state.
    #[cfg(feature = "cluster")]
    pending_revoke_vnodes: Option<Arc<parking_lot::Mutex<FxHashSet<u32>>>>,
    #[cfg(feature = "cluster")]
    rotation_execution_fence: Option<Arc<tokio::sync::RwLock<()>>>,
}

impl OperatorGraph {
    pub fn new(ctx: SessionContext) -> Self {
        Self {
            nodes: Vec::new(),
            free_node_ids: Vec::new(),
            edges: Vec::new(),
            topo_order: Vec::new(),
            topo_dirty: true,
            node_domain: Vec::new(),
            domain_count: 0,
            shared_source_isolation: false,
            max_replay_buffer_bytes: usize::MAX,
            cycle_failed_sources: FxHashSet::default(),
            cycle_any_failed: false,
            cycle_deferred_sources: FxHashSet::default(),
            cycle_any_deferred: false,
            source_map: FxHashMap::default(),
            source_list: Vec::new(),
            source_node_ids: FxHashSet::default(),
            output_map: FxHashMap::default(),
            output_node_ids: FxHashSet::default(),
            input_bufs: Vec::new(),
            input_buf_bytes: Vec::new(),
            input_sources: Vec::new(),
            output_watermarks: Vec::new(),
            max_input_buf_batches: 0,
            max_input_buf_bytes: None,
            backpressure_policy: BackpressurePolicy::default(),
            query_budget_ns: 8_000_000,
            deferred_scan_offset: 0,
            stats_tick: 0,
            #[cfg(feature = "cluster")]
            cluster_shuffle: None,
            #[cfg(feature = "cluster")]
            last_execution_assignment_version: None,
            #[cfg(feature = "cluster")]
            delta_chain_bound: None,
            #[cfg(feature = "cluster")]
            vnode_partials_authoritative: false,
            #[cfg(feature = "cluster")]
            vnode_count: None,
            #[cfg(feature = "cluster")]
            rehydrated_vnode_state: None,
            #[cfg(feature = "cluster")]
            pending_revoke_vnodes: None,
            #[cfg(feature = "cluster")]
            rotation_execution_fence: None,
            ctx,
            prom: None,
            lookup_registry: None,
            source_schemas: FxHashMap::default(),
            temporal_configs: Vec::new(),
            depends_on_stream: FxHashSet::default(),
            order_configs: FxHashMap::default(),
            live_handles: FxHashMap::default(),
            ai_runtime: None,
            main_runtime_handle: None,
            partial_lookup_tables: FxHashMap::default(),
            incremental_tables: FxHashSet::default(),
            reference_tables: FxHashSet::default(),
            build_errors: Vec::new(),
            whole_restore_open: true,
        }
    }

    /// Register the static reference/dimension table names (valid right sides of a changelog
    /// enrich join).
    pub fn set_reference_tables(&mut self, tables: FxHashSet<String>) {
        self.reference_tables = tables;
    }

    /// Seed the incremental-MV (changelog producer) set before operators are built, so a
    /// `changelog ⋈ static dim` consumer detects its source regardless of build order. Kept
    /// current by `add_query` as MVs are hot-added.
    pub fn set_incremental_tables(&mut self, tables: FxHashSet<String>) {
        self.incremental_tables = tables;
    }

    /// Install the AI subsystem and main runtime handle for inference workers.
    pub fn set_ai_runtime(
        &mut self,
        runtime: Arc<crate::ai::AiRuntime>,
        handle: tokio::runtime::Handle,
    ) {
        self.ai_runtime = Some(runtime);
        self.main_runtime_handle = Some(handle);
    }

    /// Install the main runtime handle for Ring-1 workers (lookup-enrich, AI).
    pub fn set_runtime_handle(&mut self, handle: tokio::runtime::Handle) {
        self.main_runtime_handle = Some(handle);
    }

    /// Register on-demand lookup tables so `add_query` can route lookup-enrich joins.
    pub fn set_partial_lookup_tables(&mut self, tables: FxHashMap<String, Vec<String>>) {
        self.partial_lookup_tables = tables;
    }

    /// Return the first plan-time build error, if any.
    ///
    /// # Errors
    ///
    /// Returns the first recorded [`DbError`] (unknown model, unsupported task, etc.).
    pub fn take_build_errors(&mut self) -> Result<(), DbError> {
        match self.build_errors.drain(..).next() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    pub fn set_shared_source_isolation(&mut self, on: bool, max_replay_buffer_bytes: usize) {
        self.shared_source_isolation = on;
        self.max_replay_buffer_bytes = max_replay_buffer_bytes;
    }

    pub fn set_max_input_buf_batches(&mut self, cap: usize) {
        self.max_input_buf_batches = cap;
    }

    pub fn set_max_input_buf_bytes(&mut self, cap: Option<usize>) {
        self.max_input_buf_bytes = cap;
    }

    pub fn set_backpressure_policy(&mut self, policy: BackpressurePolicy) {
        self.backpressure_policy = policy;
    }

    pub fn set_query_budget_ns(&mut self, ns: u64) {
        self.query_budget_ns = ns;
    }

    pub fn set_metrics(&mut self, m: Arc<EngineMetrics>) {
        self.prom = Some(m);
    }

    #[allow(clippy::cast_precision_loss)]
    pub fn input_buf_pressure(&self) -> f64 {
        let cap = self.max_input_buf_batches;
        let max_bytes = self.max_input_buf_bytes;

        let count_ratio = if cap > 0 {
            let max_len = self
                .input_bufs
                .iter()
                .flat_map(|ports| ports.iter())
                .map(Vec::len)
                .max()
                .unwrap_or(0);
            max_len as f64 / cap as f64
        } else {
            0.0
        };

        let bytes_ratio = if let Some(max) = max_bytes {
            let max_bytes_used = self
                .input_buf_bytes
                .iter()
                .flat_map(|ports| ports.iter().copied())
                .max()
                .unwrap_or(0);
            max_bytes_used as f64 / max as f64
        } else {
            0.0
        };

        count_ratio.max(bytes_ratio).min(1.0)
    }

    pub fn has_pending_input(&self) -> bool {
        self.input_bufs.iter().enumerate().any(|(id, ports)| {
            ports.iter().any(|port| !port.is_empty()) && !self.source_node_ids.contains(&id)
        })
    }

    /// Logical bytes queued on every live input port. Uses the maintained per-port byte counters
    /// rather than walking Arrow batches, so checkpoint drain polling is independent of the number
    /// of buffered batches.
    pub(crate) fn checkpoint_pending_input_bytes(&self) -> usize {
        self.input_buf_bytes
            .iter()
            .enumerate()
            .filter(|(node_id, _)| !self.nodes[*node_id].removed)
            .flat_map(|(_, port_bytes)| port_bytes.iter().copied())
            .fold(0usize, usize::saturating_add)
    }

    /// Whether an aligned checkpoint can snapshot without leaving queued graph input outside the
    /// cut. Buffer presence is checked separately from bytes because Arrow permits positive-row
    /// record batches whose arrays occupy zero bytes.
    pub(crate) fn checkpoint_is_quiescent(&self) -> bool {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| !node.removed)
            .all(|(node_id, _)| !self.node_has_checkpoint_pending_work(node_id))
    }

    fn node_has_checkpoint_pending_work(&self, node_id: usize) -> bool {
        if self.input_bufs[node_id].iter().any(|port| !port.is_empty()) {
            return true;
        }
        false
    }

    fn checkpoint_drain_nodes(&self) -> FxHashSet<usize> {
        let mut drain = FxHashSet::default();
        let mut pending = VecDeque::new();
        for (node_id, node) in self.nodes.iter().enumerate() {
            if !node.removed && self.node_has_checkpoint_pending_work(node_id) {
                drain.insert(node_id);
                pending.push_back(node_id);
            }
        }
        while let Some(node_id) = pending.pop_front() {
            for &(target, _) in &self.nodes[node_id].output_routes {
                if !self.nodes[target].removed && drain.insert(target) {
                    pending.push_back(target);
                }
            }
        }
        drain
    }

    pub fn set_lookup_registry(
        &mut self,
        registry: Arc<laminar_sql::datafusion::LookupTableRegistry>,
    ) {
        self.lookup_registry = Some(registry);
    }

    /// Install the cluster shuffle config for streaming aggregates.
    #[cfg(feature = "cluster")]
    pub fn set_cluster_shuffle(
        &mut self,
        config: crate::operator::sql_query::ClusterShuffleConfig,
    ) {
        self.vnode_count = Some(config.registry.vnode_count());
        self.cluster_shuffle = Some(config);
    }

    /// Enable incremental delta checkpoints on aggregate operators with the given re-base bound.
    #[cfg(feature = "cluster")]
    pub fn set_delta_chain_bound(&mut self, chain_bound: u32) {
        self.delta_chain_bound = Some(chain_bound);
    }

    /// Per-vnode partials are the authoritative agg checkpoint; skip the whole-node manifest copy.
    #[cfg(feature = "cluster")]
    pub fn set_vnode_partials_authoritative(&mut self) {
        self.vnode_partials_authoritative = true;
    }

    /// Cluster shuffle config, if installed; reused by the pipeline callback for subscriptions.
    #[cfg(feature = "cluster")]
    pub(crate) fn cluster_shuffle_config(
        &self,
    ) -> Option<&crate::operator::sql_query::ClusterShuffleConfig> {
        self.cluster_shuffle.as_ref()
    }

    #[cfg(feature = "cluster")]
    #[cfg(test)]
    pub(crate) const fn last_execution_assignment_version(&self) -> Option<u64> {
        self.last_execution_assignment_version
    }

    /// Share the staged per-vnode rehydration map; drained at the top of each cycle.
    #[cfg(feature = "cluster")]
    #[allow(clippy::disallowed_types)] // shares the DB's std-HashMap-typed handle
    pub fn set_rehydration_handle(
        &mut self,
        staged: Arc<parking_lot::Mutex<std::collections::HashMap<u32, crate::db::RehydratedVnode>>>,
    ) {
        self.rehydrated_vnode_state = Some(staged);
    }

    /// Share the staged revoked-vnode set; drained at the top of each cycle to drop lost state.
    #[cfg(feature = "cluster")]
    pub fn set_revoke_handle(&mut self, staged: Arc<parking_lot::Mutex<FxHashSet<u32>>>) {
        self.pending_revoke_vnodes = Some(staged);
    }

    #[cfg(feature = "cluster")]
    pub fn set_rotation_execution_fence(&mut self, fence: Arc<tokio::sync::RwLock<()>>) {
        self.rotation_execution_fence = Some(fence);
    }

    /// Drop in-memory state for vnodes lost since the last cycle, before `apply_rehydrated_vnodes`
    /// merges any re-acquired ones — so a lose-then-reacquire merges into empty state. Disjoint from
    /// the rehydrated set per rotation; the ordering is defensive against rapid cross-rotation churn.
    #[cfg(feature = "cluster")]
    fn apply_revoked_vnodes(&mut self) -> Result<(), DbError> {
        let Some(handle) = self.pending_revoke_vnodes.as_ref().map(Arc::clone) else {
            return Ok(());
        };
        let revoked: FxHashSet<u32> = {
            let guard = handle.lock();
            if guard.is_empty() {
                return Ok(());
            }
            guard.clone()
        };
        for node in &mut self.nodes {
            if node.removed {
                continue;
            }
            node.operator.drop_owned_vnodes(&revoked).map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] failed to revoke vnode state for operator '{}': {error}",
                    node.name
                ))
            })?;
        }
        handle.lock().retain(|vnode| !revoked.contains(vnode));
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn apply_rehydrated_vnodes(&mut self) -> Result<(), DbError> {
        // Clone owned handles out so no borrow of `self` survives into the
        // `self.nodes.iter_mut()` dispatch below.
        let (registry, self_id, staged_arc) = match (
            self.cluster_shuffle.as_ref(),
            self.rehydrated_vnode_state.as_ref(),
        ) {
            (Some(cfg), Some(staged)) => {
                (Arc::clone(&cfg.registry), cfg.self_id, Arc::clone(staged))
            }
            _ => return Ok(()),
        };

        // Ownership may have changed again since staging; evict chains for vnodes we no longer own
        // so an acquire→lose race cannot resurrect stale state, then drain the currently owned set.
        let drained: Vec<(u32, crate::db::RehydratedVnode)> = {
            let mut guard = staged_arc.lock();
            if guard.is_empty() {
                return Ok(());
            }
            let owned: FxHashSet<u32> = laminar_core::state::owned_vnodes(&registry, self_id)
                .into_iter()
                .collect();
            guard.retain(|v, _| owned.contains(v));
            guard.drain().collect()
        };
        if drained.is_empty() {
            return Ok(());
        }

        for (vnode, rehydrated) in drained {
            let chain: Vec<crate::vnode_partial::VnodePartial> = rehydrated
                .chain
                .iter()
                .enumerate()
                .map(|(link, bytes)| {
                    crate::vnode_partial::VnodePartial::decode(bytes).map_err(|error| {
                        DbError::Checkpoint(format!(
                            "[LDB-6051] vnode {vnode} rehydration chain link {link} is corrupt: \
                             {error}"
                        ))
                    })
                })
                .collect::<Result<_, _>>()?;
            // Every operator present anywhere in the chain (full or delta), resolved independently.
            let mut op_names: Vec<String> = Vec::new();
            for p in &chain {
                for (n, _) in &p.operators {
                    if !op_names.iter().any(|o| o == n) {
                        op_names.push(n.clone());
                    }
                }
                for (n, _) in &p.deltas {
                    if !op_names.iter().any(|o| o == n) {
                        op_names.push(n.clone());
                    }
                }
            }

            // Resolve and validate every required slice before mutating any operator. A missing
            // FULL base or topology drift must fault the cycle; consuming the staged chain and
            // starting fresh would silently lose committed state.
            let mut resolved = Vec::with_capacity(op_names.len());
            for op_name in &op_names {
                let (base, deltas) = crate::recovery_manager::resolve_op_chain(&chain, op_name)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "[LDB-6051] vnode {vnode} rehydration chain has no FULL base for \
                             operator '{op_name}'"
                        ))
                    })?;
                let node_idx = self
                    .nodes
                    .iter()
                    .position(|node| !node.removed && &*node.name == op_name.as_str())
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "[LDB-6051] vnode {vnode} rehydration requires missing operator \
                             '{op_name}' (topology drift)"
                        ))
                    })?;
                resolved.push((node_idx, op_name, base, deltas));
            }

            let operator_count = resolved.len();
            for (node_idx, op_name, base, deltas) in resolved {
                self.nodes[node_idx]
                    .operator
                    .apply_vnode_chain(vnode, base, &deltas)
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "[LDB-6051] failed to apply vnode {vnode} rehydration chain for \
                             operator '{op_name}': {error}"
                        ))
                    })?;
            }
            tracing::info!(
                vnode,
                epoch = rehydrated.epoch,
                operators = operator_count,
                links = chain.len(),
                "applied rehydrated vnode chain"
            );
            // This is the only Restoring→Active transition: every named operator slice above
            // resolved to a FULL base and applied successfully.
            registry.mark_active(&[vnode]);
        }
        Ok(())
    }

    fn is_downstream_at_capacity(&self, node_id: usize) -> bool {
        let cap = self.max_input_buf_batches;
        let max_bytes = self.max_input_buf_bytes;
        if cap == 0 && max_bytes.is_none() {
            return false;
        }
        self.nodes[node_id]
            .output_routes
            .iter()
            .any(|&(target, port)| {
                let p = port as usize;
                let over_count = cap > 0 && self.input_bufs[target][p].len() >= cap;
                let over_bytes =
                    max_bytes.is_some_and(|max| self.input_buf_bytes[target][p] >= max);
                over_count || over_bytes
            })
    }

    fn shed_to_cap(&mut self, target: usize, port: u8) -> usize {
        if !matches!(self.backpressure_policy, BackpressurePolicy::ShedOldest) {
            return 0;
        }
        let cap = self.max_input_buf_batches;
        let max_bytes = self.max_input_buf_bytes;
        let p = port as usize;

        let mut drop_n = if cap > 0 && self.input_bufs[target][p].len() > cap {
            self.input_bufs[target][p].len() - cap
        } else {
            0
        };
        if let Some(max) = max_bytes {
            let buf = &self.input_bufs[target][p];
            let mut remaining = self.input_buf_bytes[target][p];
            for b in buf.iter().take(drop_n) {
                remaining = remaining.saturating_sub(b.get_array_memory_size());
            }
            while remaining > max && drop_n < buf.len() {
                remaining = remaining.saturating_sub(buf[drop_n].get_array_memory_size());
                drop_n += 1;
            }
        }
        if drop_n == 0 {
            return 0;
        }
        let mut bytes_removed = 0usize;
        let rows: usize = self.input_bufs[target][p]
            .drain(..drop_n)
            .map(|b| {
                bytes_removed += b.get_array_memory_size();
                b.num_rows()
            })
            .sum();
        let slot = &mut self.input_buf_bytes[target][p];
        *slot = slot.saturating_sub(bytes_removed);
        rows
    }

    fn gate_decision(&self, node_id: usize) -> GateDecision {
        if !self.is_downstream_at_capacity(node_id) {
            return GateDecision::Run;
        }
        match self.backpressure_policy {
            BackpressurePolicy::Backpressure => GateDecision::Skip,
            BackpressurePolicy::Fail => GateDecision::Fail,
            BackpressurePolicy::ShedOldest => GateDecision::Run,
        }
    }

    #[cfg(debug_assertions)]
    fn debug_assert_byte_sums(&self) {
        for (id, ports) in self.input_bufs.iter().enumerate() {
            for (port, buf) in ports.iter().enumerate() {
                let actual: usize = buf.iter().map(RecordBatch::get_array_memory_size).sum();
                debug_assert_eq!(
                    self.input_buf_bytes[id][port], actual,
                    "input_buf_bytes drift at node={} port={}",
                    &*self.nodes[id].name, port,
                );
            }
        }
    }

    fn push_to_port(&mut self, target: usize, port: u8, batches: Vec<RecordBatch>, bytes: usize) {
        let buf = &mut self.input_bufs[target][port as usize];
        if buf.is_empty() {
            *buf = batches;
        } else {
            buf.extend(batches);
        }
        self.input_buf_bytes[target][port as usize] += bytes;
        self.record_shed(target, port);
    }

    fn record_shed(&mut self, target: usize, port: u8) {
        let rows = self.shed_to_cap(target, port);
        if rows == 0 {
            return;
        }
        if let Some(ref prom) = self.prom {
            prom.shed_records_total
                .with_label_values(&[&self.nodes[target].name])
                .inc_by(rows as u64);
        }
    }

    pub fn register_source_schema(&mut self, name: String, schema: SchemaRef) {
        self.ensure_live_provider(&name, &schema);
        self.source_schemas.insert(name, schema);
    }

    fn ensure_live_provider(&mut self, name: &str, schema: &SchemaRef) {
        if self.live_handles.contains_key(name) {
            return;
        }
        let provider = LiveSourceProvider::new(schema.clone());
        let handle = provider.handle();
        let _ = self.ctx.deregister_table(exact_table_reference(name));
        if let Err(e) = self
            .ctx
            .register_table(exact_table_reference(name), Arc::new(provider))
        {
            // Table was just deregistered, so re-registration should always succeed.
            tracing::error!(
                table = %name,
                error = %e,
                "BUG: Failed to register LiveSourceProvider after deregister"
            );
            return;
        }
        self.live_handles.insert(name.to_string(), handle);
    }

    pub fn temporal_join_configs(&self) -> Vec<TemporalJoinTranslatorConfig> {
        self.temporal_configs
            .iter()
            .map(|(_, config)| config.clone())
            .collect()
    }

    fn find_node(&self, name: &str) -> Option<usize> {
        self.nodes
            .iter()
            .position(|n| &*n.name == name && !n.removed)
    }

    fn allocate_node(&mut self, node: GraphNode) -> usize {
        let input_port_count = node.input_port_count;
        if let Some(id) = self.free_node_ids.pop() {
            debug_assert!(self.nodes[id].removed);
            self.nodes[id] = node;
            self.input_bufs[id] = vec![Vec::new(); input_port_count];
            self.input_buf_bytes[id] = vec![0; input_port_count];
            self.input_sources[id] = vec![usize::MAX; input_port_count];
            self.output_watermarks[id] = i64::MIN;
            if let Some(domain) = self.node_domain.get_mut(id) {
                *domain = 0;
            }
            self.output_node_ids.remove(&id);
            self.source_node_ids.remove(&id);
            self.depends_on_stream.remove(&id);
            self.order_configs.remove(&id);
            id
        } else {
            let id = self.nodes.len();
            self.nodes.push(node);
            self.input_bufs.push(vec![Vec::new(); input_port_count]);
            self.input_buf_bytes.push(vec![0; input_port_count]);
            self.input_sources.push(vec![usize::MAX; input_port_count]);
            self.output_watermarks.push(i64::MIN);
            id
        }
    }

    fn ensure_source_node(&mut self, table_name: &str) -> usize {
        if let Some(&id) = self.source_map.get(table_name) {
            return id;
        }
        let name: Arc<str> = Arc::from(table_name);
        let node_id = self.allocate_node(GraphNode {
            name: Arc::clone(&name),
            operator: Box::new(SourcePassthrough),
            input_port_count: 1,
            output_routes: Vec::new(),
            removed: false,
        });
        self.source_map.insert(name, node_id);
        self.source_node_ids.insert(node_id);
        node_id
    }

    fn insert_filter_node(&mut self, name: &str, filter_sql: String, source_id: usize) -> usize {
        let node_id = self.allocate_node(GraphNode {
            name: Arc::from(name),
            operator: Box::new(SqlFilterOperator::new(filter_sql, self.ctx.clone(), name)),
            input_port_count: 1,
            output_routes: Vec::new(),
            removed: false,
        });
        self.add_edge(source_id, node_id, 0);
        self.topo_dirty = true;
        node_id
    }

    fn add_edge(&mut self, source: usize, target: usize, target_port: u8) {
        self.edges.push(GraphEdge { source, target });
        self.nodes[source].output_routes.push((target, target_port));
        let port = target_port as usize;
        if port < self.input_sources[target].len() {
            self.input_sources[target][port] = source;
        }
    }

    fn ensure_query_source_nodes(
        &mut self,
        temporal_probe_config: Option<&laminar_sql::translator::TemporalProbeConfig>,
        asof_config: Option<&laminar_sql::translator::AsofJoinTranslatorConfig>,
        stream_join_config: Option<&laminar_sql::translator::StreamJoinConfig>,
        temporal_config: Option<&TemporalJoinTranslatorConfig>,
        table_refs: &FxHashSet<String>,
    ) {
        if let Some(tpc) = temporal_probe_config {
            self.find_node(&tpc.left_table)
                .unwrap_or_else(|| self.ensure_source_node(&tpc.left_table));
            self.find_node(&tpc.right_table)
                .unwrap_or_else(|| self.ensure_source_node(&tpc.right_table));
        } else if let Some(asof_cfg) = asof_config {
            self.find_node(&asof_cfg.left_table)
                .unwrap_or_else(|| self.ensure_source_node(&asof_cfg.left_table));
            self.find_node(&asof_cfg.right_table)
                .unwrap_or_else(|| self.ensure_source_node(&asof_cfg.right_table));
        } else if let Some(sjc) = stream_join_config {
            self.find_node(&sjc.left_table)
                .unwrap_or_else(|| self.ensure_source_node(&sjc.left_table));
            self.find_node(&sjc.right_table)
                .unwrap_or_else(|| self.ensure_source_node(&sjc.right_table));
        } else if let Some(tc) = temporal_config {
            if self.find_node(&tc.stream_table).is_none() {
                self.ensure_source_node(&tc.stream_table);
            }
        } else {
            for table_ref in table_refs {
                if self.find_node(table_ref).is_none() {
                    self.ensure_source_node(table_ref);
                }
            }
        }
    }

    // Returns true when the node depends on another query output (not just raw sources).
    #[allow(clippy::too_many_arguments)]
    fn wire_query_edges(
        &mut self,
        node_id: usize,
        temporal_probe_config: Option<&laminar_sql::translator::TemporalProbeConfig>,
        asof_config: Option<&laminar_sql::translator::AsofJoinTranslatorConfig>,
        stream_join_config: Option<&laminar_sql::translator::StreamJoinConfig>,
        stream_join_detection: Option<&StreamJoinDetection>,
        temporal_config: Option<&TemporalJoinTranslatorConfig>,
        incremental_join_config: Option<&crate::sql_analysis::IncrementalJoinConfig>,
        table_refs: &FxHashSet<String>,
    ) -> bool {
        if let Some(ijc) = incremental_join_config {
            // Both sides are incremental MV producers — wire left → port 0, right → 1.
            let left_id = self.find_node(&ijc.left_table).expect("source ensured");
            let right_id = self.find_node(&ijc.right_table).expect("source ensured");
            self.add_edge(left_id, node_id, 0);
            self.add_edge(right_id, node_id, 1);
            true
        } else if let Some(tpc) = temporal_probe_config {
            let left_id = self.find_node(&tpc.left_table).expect("source ensured");
            let right_id = self.find_node(&tpc.right_table).expect("source ensured");
            self.add_edge(left_id, node_id, 0);
            self.add_edge(right_id, node_id, 1);
            false
        } else if let Some(asof_cfg) = asof_config {
            let left_id = self
                .find_node(&asof_cfg.left_table)
                .expect("source ensured");
            let right_id = self
                .find_node(&asof_cfg.right_table)
                .expect("source ensured");
            self.add_edge(left_id, node_id, 0);
            self.add_edge(right_id, node_id, 1);
            false
        } else if let Some(sjc) = stream_join_config {
            let source_id = self.find_node(&sjc.left_table).expect("source ensured");

            let has_pre_filters = stream_join_detection
                .is_some_and(|d| d.left_pre_filter.is_some() || d.right_pre_filter.is_some());

            if sjc.left_table == sjc.right_table && has_pre_filters {
                let det = stream_join_detection.unwrap();

                let left_input = if let Some(ref filter_sql) = det.left_pre_filter {
                    self.insert_filter_node(
                        &format!("{}::left_prefilter", self.nodes[node_id].name),
                        filter_sql.clone(),
                        source_id,
                    )
                } else {
                    source_id
                };

                let right_input = if let Some(ref filter_sql) = det.right_pre_filter {
                    self.insert_filter_node(
                        &format!("{}::right_prefilter", self.nodes[node_id].name),
                        filter_sql.clone(),
                        source_id,
                    )
                } else {
                    source_id
                };

                self.add_edge(left_input, node_id, 0);
                self.add_edge(right_input, node_id, 1);
            } else {
                let right_id = self.find_node(&sjc.right_table).expect("source ensured");
                self.add_edge(source_id, node_id, 0);
                self.add_edge(right_id, node_id, 1);
            }
            false
        } else if let Some(tc) = temporal_config {
            let stream_id = self.find_node(&tc.stream_table).expect("source ensured");
            self.add_edge(stream_id, node_id, 0);
            self.output_map.contains_key(tc.stream_table.as_str())
        } else {
            let mut depends_on_query = false;
            for table_ref in table_refs {
                let upstream_id = self.find_node(table_ref).expect("source ensured");
                let already_connected = self.nodes[upstream_id]
                    .output_routes
                    .iter()
                    .any(|&(t, p)| t == node_id && p == 0);
                if !already_connected {
                    self.add_edge(upstream_id, node_id, 0);
                }
                if self.output_map.contains_key(table_ref.as_str()) {
                    depends_on_query = true;
                }
            }
            depends_on_query
        }
    }

    #[allow(
        clippy::too_many_lines,
        clippy::too_many_arguments,
        clippy::needless_pass_by_value
    )]
    pub fn add_query(
        &mut self,
        name: String,
        sql: String,
        emit_clause: Option<EmitClause>,
        window_config: Option<WindowOperatorConfig>,
        order_config: Option<OrderOperatorConfig>,
        join_config: Option<Vec<laminar_sql::translator::JoinOperatorConfig>>,
        incremental: bool,
    ) {
        use laminar_sql::translator::JoinOperatorConfig;

        if join_clause_count(&sql) > 1 {
            self.build_errors.push(DbError::InvalidOperation(
                "multi-way streaming joins require explicitly named two-way stages".to_string(),
            ));
            return;
        }

        let ai_calls = crate::sql_analysis::detect_ai_functions(&sql);
        if ai_calls.len() > 1 {
            self.build_errors.push(DbError::InvalidOperation(
                "v0.1 supports at most one AI function per query".to_string(),
            ));
            return;
        }
        if ai_calls.len() == 1 {
            match crate::sql_analysis::plan_ai_query(&sql) {
                Some(plan) => {
                    if let Err(e) = self.build_ai_operator_node(&name, &plan) {
                        self.build_errors.push(e);
                    }
                }
                None => self.build_errors.push(DbError::InvalidOperation(
                    "an AI function must be a top-level SELECT item with an `AS` alias over a \
                     single (un-joined) source"
                        .to_string(),
                )),
            }
            return;
        }

        if let Some(plan) = crate::sql_analysis::plan_frame_query(&sql) {
            if let Err(error) = self.build_frame_operator_node(&name, &plan) {
                self.build_errors.push(error);
            }
            return;
        }

        // `changelog ⋈ static dim`: detected first so it wins over the generic processing-time
        // equi-join — a changelog left makes this a retraction-aware enrich, not a stream join.
        let changelog_enrich_config = if self.incremental_tables.is_empty() {
            None
        } else {
            crate::sql_analysis::detect_changelog_enrich_query(
                &sql,
                &self.incremental_tables,
                &self.reference_tables,
            )
        };
        let enrich = changelog_enrich_config.is_some();

        // `changelog ⋈ changelog` two-sided IVM join — both sides incremental MVs. Like enrich, it
        // wins over the generic stream join (a changelog left/right makes it retraction-aware).
        let incremental_join_config = if enrich || self.incremental_tables.len() < 2 {
            None
        } else {
            crate::sql_analysis::detect_changelog_incremental_join(&sql, &self.incremental_tables)
        };
        let inc_join = incremental_join_config.is_some();

        // TemporalProbe is parsed off the token stream (not the sqlparser AST), so it
        // never appears in join_config and always needs its own detector pass.
        let needs_specialized_detection = join_config.as_ref().is_none_or(|jcs| {
            jcs.iter().any(|c| {
                matches!(
                    c,
                    JoinOperatorConfig::StreamStream(_)
                        | JoinOperatorConfig::Asof(_)
                        | JoinOperatorConfig::Temporal(_)
                )
            })
        });

        let (temporal_probe_config, temporal_probe_projection_sql) = if enrich || inc_join {
            (None, None)
        } else {
            detect_temporal_probe_query(&sql)
        };
        let specialized =
            !enrich && !inc_join && temporal_probe_config.is_none() && needs_specialized_detection;
        let (asof_config, projection_sql) = if specialized {
            detect_asof_query(&sql)
        } else {
            (None, None)
        };
        let (temporal_config, temporal_projection_sql) = if specialized {
            detect_temporal_query(&sql)
        } else {
            (None, None)
        };
        let stream_join_detection = if specialized {
            detect_stream_join_query(&sql)
        } else {
            None
        };
        let stream_join_config = stream_join_detection.as_ref().map(|d| d.config.clone());
        if let Some(config) = &stream_join_config {
            if config.join_type != laminar_sql::translator::StreamJoinType::Inner
                || config.time_bound.is_zero()
                || i64::try_from(config.time_bound.as_millis()).is_err()
            {
                self.build_errors.push(DbError::InvalidOperation(format!(
                    "streaming interval join '{name}' requires an INNER join with a positive finite time bound"
                )));
                return;
            }
        }
        let stream_join_projection_sql = stream_join_detection
            .as_ref()
            .map(|d| d.projection_sql.clone());

        // Lookup-enrich: only when no other specialized join (incl. changelog-enrich) matched.
        let (lookup_enrich_config, lookup_projection_sql) = if !enrich
            && !inc_join
            && temporal_probe_config.is_none()
            && asof_config.is_none()
            && temporal_config.is_none()
            && stream_join_config.is_none()
            && !self.partial_lookup_tables.is_empty()
        {
            crate::sql_analysis::detect_lookup_enrich_query(
                &sql,
                &self.partial_lookup_tables,
                &self.source_schemas,
            )
        } else {
            (None, None)
        };

        let unbounded_lookup_join = if !enrich && !inc_join {
            if let Some(steps) = detect_unbounded_join_steps(&sql) {
                let lookup_only = steps.iter().all(|(_, right)| {
                    self.reference_tables.contains(right)
                        || self.partial_lookup_tables.contains_key(right)
                });
                if !lookup_only {
                    let relations = steps
                        .iter()
                        .map(|(left, right)| format!("'{left}' and '{right}'"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    self.build_errors.push(DbError::InvalidOperation(format!(
                        "unbounded join between streaming relations {relations}; add a temporal predicate or use a lookup table"
                    )));
                    return;
                }
                lookup_only
            } else {
                false
            }
        } else {
            false
        };

        let projection_sql = projection_sql
            .or(temporal_probe_projection_sql)
            .or(temporal_projection_sql)
            .or(stream_join_projection_sql)
            .or(lookup_projection_sql);

        let unrecognized_join = has_join_clause(&sql)
            && !enrich
            && !inc_join
            && temporal_probe_config.is_none()
            && asof_config.is_none()
            && temporal_config.is_none()
            && stream_join_config.is_none()
            && lookup_enrich_config.is_none()
            && !unbounded_lookup_join;
        if unrecognized_join {
            self.build_errors.push(DbError::InvalidOperation(format!(
                "stream join '{name}' could not be planned as a supported bounded interval or lookup join"
            )));
            return;
        }

        let mut table_refs = extract_table_references(&sql);
        // Lookup-enrich reads its table from the registry, not as a graph input.
        if let Some(cfg) = &lookup_enrich_config {
            table_refs.remove(&cfg.table_name);
        }
        // ChangelogEnrich: only the changelog (left) is a graph input; the dimension is read from
        // the context, not wired as an edge.
        if let Some(cfg) = &changelog_enrich_config {
            table_refs.retain(|t| t == &cfg.changelog_table);
        }

        let operator: Box<dyn GraphOperator> = self.create_operator(
            &name,
            &sql,
            emit_clause.as_ref(),
            window_config.as_ref(),
            asof_config.as_ref(),
            temporal_config.as_ref(),
            stream_join_config.as_ref(),
            temporal_probe_config.as_ref(),
            lookup_enrich_config,
            projection_sql.as_deref(),
            incremental,
            changelog_enrich_config,
            incremental_join_config.clone(),
        );
        let input_port_count = if asof_config.is_some()
            || stream_join_config.is_some()
            || temporal_probe_config.is_some()
            || inc_join
        {
            2
        } else {
            1
        };

        self.ensure_query_source_nodes(
            temporal_probe_config.as_ref(),
            asof_config.as_ref(),
            stream_join_config.as_ref(),
            temporal_config.as_ref(),
            &table_refs,
        );
        let node_id = self.place_prepared_operator_node(name.as_str(), operator, input_port_count);
        let depends = self.wire_query_edges(
            node_id,
            temporal_probe_config.as_ref(),
            asof_config.as_ref(),
            stream_join_config.as_ref(),
            stream_join_detection.as_ref(),
            temporal_config.as_ref(),
            incremental_join_config.as_ref(),
            &table_refs,
        );
        if depends {
            self.depends_on_stream.insert(node_id);
        }
        if let Some(oc) = order_config {
            self.order_configs.insert(node_id, oc);
        }
        self.output_map.insert(Arc::from(name.as_str()), node_id);
        if incremental {
            self.incremental_tables.insert(name.clone());
        }
        if let Some(ref tc) = temporal_config {
            self.temporal_configs.push((name.clone(), tc.clone()));
        }
        self.topo_dirty = true;
    }

    // Replace a SourcePassthrough placeholder in place (preserving its id and outbound edges),
    // or append a fresh node. Callers must ensure source nodes before and wire edges after.
    #[cfg(test)]
    fn place_operator_node(
        &mut self,
        name: &str,
        operator: Box<dyn GraphOperator>,
        input_port_count: usize,
    ) -> Result<usize, DbError> {
        Ok(self.place_prepared_operator_node(name, operator, input_port_count))
    }

    // The caller has completed validation before making any graph mutation.
    fn place_prepared_operator_node(
        &mut self,
        name: &str,
        operator: Box<dyn GraphOperator>,
        input_port_count: usize,
    ) -> usize {
        if let Some(&id) = self.source_map.get(name) {
            self.nodes[id].operator = operator;
            self.nodes[id].input_port_count = input_port_count;
            self.input_bufs[id] = vec![Vec::new(); input_port_count];
            self.input_buf_bytes[id] = vec![0; input_port_count];
            self.input_sources[id] = vec![usize::MAX; input_port_count];
            self.source_map.remove(name);
            self.source_node_ids.remove(&id);
            // Downstream nodes already wired to the placeholder now depend on this query.
            for &(target, _) in &self.nodes[id].output_routes {
                self.depends_on_stream.insert(target);
            }
            id
        } else {
            self.allocate_node(GraphNode {
                name: Arc::from(name),
                operator,
                input_port_count,
                output_routes: Vec::new(),
                removed: false,
            })
        }
    }

    fn build_ai_operator_node(
        &mut self,
        name: &str,
        plan: &crate::sql_analysis::AiQueryPlan,
    ) -> Result<(), DbError> {
        use crate::operator::ai_inference::{AiInferenceOperator, AiOperatorConfig};

        let handle = self.main_runtime_handle.clone().ok_or_else(|| {
            DbError::InvalidOperation("AI runtime handle is not configured".to_string())
        })?;
        let ctx = self.ctx.clone();

        let (operator, table_refs): (Box<dyn GraphOperator>, FxHashSet<String>) = {
            let runtime = self.ai_runtime.as_ref().ok_or_else(|| {
                DbError::InvalidOperation(
                    "AI functions require `[ai]` providers and `[models]` configuration"
                        .to_string(),
                )
            })?;

            crate::sql_analysis::validate_ai_calls(
                runtime.registry(),
                std::slice::from_ref(&plan.call),
            )?;

            let model_name = match &plan.call.model {
                Some(m) => m.clone(),
                None => runtime
                    .registry()
                    .default_for(plan.call.task)
                    .map(str::to_string)
                    .ok_or_else(|| {
                        DbError::InvalidOperation(format!(
                            "no model given for task '{}' and no [ai.defaults] default is \
                             configured",
                            plan.call.task
                        ))
                    })?,
            };
            let resolved = runtime
                .resolve(&model_name)
                .map_err(|e| DbError::InvalidOperation(e.to_string()))?;

            let output_column = plan.call.output_alias.clone().ok_or_else(|| {
                DbError::InvalidOperation("AI function requires an `AS` alias".to_string())
            })?;
            let labels = plan.call.labels.clone().or_else(|| resolved.labels.clone());

            let config = AiOperatorConfig {
                task: plan.call.task,
                kind: resolved.kind,
                model_id: resolved.model_id,
                model: resolved.provider_model.clone(),
                input_column: plan.call.input.clone(),
                output_column,
                labels,
            };
            let operator: Box<dyn GraphOperator> = Box::new(AiInferenceOperator::new(
                name,
                config,
                Some(Arc::from(plan.projection_sql.as_str())),
                ctx,
                resolved.provider,
                Arc::clone(runtime.cache()),
                Arc::clone(runtime.call_log()),
                &handle,
            ));
            let mut table_refs = FxHashSet::default();
            table_refs.insert(plan.source_table.clone());
            (operator, table_refs)
        };

        self.ensure_query_source_nodes(None, None, None, None, &table_refs);
        let node_id = self.place_prepared_operator_node(name, operator, 1);
        let depends =
            self.wire_query_edges(node_id, None, None, None, None, None, None, &table_refs);
        if depends {
            self.depends_on_stream.insert(node_id);
        }
        self.output_map.insert(Arc::from(name), node_id);
        self.topo_dirty = true;
        Ok(())
    }

    fn build_frame_operator_node(
        &mut self,
        name: &str,
        plan: &crate::sql_analysis::FrameQueryPlan,
    ) -> Result<(), DbError> {
        let operator: Box<dyn GraphOperator> =
            Box::new(crate::operator::window_frame::WindowFrameOperator::new(
                name,
                crate::operator::window_frame::MomentFrameConfig {
                    func: plan.func,
                    x_column: plan.x_column.clone(),
                    y_column: plan.y_column.clone(),
                    output_column: plan.output_alias.clone(),
                    retain: plan.retain,
                },
                Arc::from(plan.projection_sql.as_str()),
                self.ctx.clone(),
            ));
        let mut table_refs = FxHashSet::default();
        table_refs.insert(plan.source_table.clone());
        self.ensure_query_source_nodes(None, None, None, None, &table_refs);
        let node_id = self.place_prepared_operator_node(name, operator, 1);
        let depends =
            self.wire_query_edges(node_id, None, None, None, None, None, None, &table_refs);
        if depends {
            self.depends_on_stream.insert(node_id);
        }
        self.output_map.insert(Arc::from(name), node_id);
        self.topo_dirty = true;
        Ok(())
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn create_operator(
        &self,
        name: &str,
        sql: &str,
        emit_clause: Option<&EmitClause>,
        window_config: Option<&WindowOperatorConfig>,
        asof_config: Option<&laminar_sql::translator::AsofJoinTranslatorConfig>,
        temporal_config: Option<&TemporalJoinTranslatorConfig>,
        stream_join_config: Option<&laminar_sql::translator::StreamJoinConfig>,
        temporal_probe_config: Option<&laminar_sql::translator::TemporalProbeConfig>,
        lookup_enrich_config: Option<crate::operator::lookup_enrich::LookupEnrichConfig>,
        projection_sql: Option<&str>,
        incremental: bool,
        changelog_enrich_config: Option<crate::sql_analysis::ChangelogEnrichConfig>,
        incremental_join_config: Option<crate::sql_analysis::IncrementalJoinConfig>,
    ) -> Box<dyn GraphOperator> {
        use crate::operator;

        // `changelog ⋈ changelog` two-sided IVM join — a hand-rolled Z-set join emitting a joined
        // changelog into the join MV's `Multiset` store.
        if let Some(cfg) = incremental_join_config {
            return Box::new(operator::incremental_join::IncrementalJoinOperator::new(
                cfg,
            ));
        }

        // `changelog ⋈ static dim` — consume the changelog, join against the dimension (in the
        // graph context), preserve `__weight` → joined changelog.
        if let Some(cfg) = changelog_enrich_config {
            return Box::new(ChangelogEnrichOperator::new(
                self.ctx.clone(),
                cfg.projection_sql,
            ));
        }

        // Falls through to the DataFusion lookup path if the registry/handle is absent.
        if let Some(cfg) = lookup_enrich_config {
            if let (Some(reg), Some(handle)) = (&self.lookup_registry, &self.main_runtime_handle) {
                let op = operator::lookup_enrich::LookupEnrichOperator::new(
                    name,
                    cfg,
                    projection_sql.map(Arc::from),
                    self.ctx.clone(),
                    Arc::clone(reg),
                    handle.clone(),
                    self.prom.clone(),
                );
                return Box::new(op);
            }
        }

        if let Some(cfg) = temporal_probe_config {
            return Box::new(
                operator::temporal_probe_join::TemporalProbeJoinOperator::new(
                    name,
                    cfg.clone(),
                    projection_sql.map(Arc::from),
                    self.ctx.clone(),
                ),
            );
        }

        if let Some(cfg) = asof_config {
            return Box::new(operator::asof_join::AsofJoinOperator::new(
                name,
                cfg.clone(),
                projection_sql.map(Arc::from),
                self.ctx.clone(),
            ));
        }

        if let Some(cfg) = temporal_config {
            return Box::new(operator::temporal_join::TemporalJoinOperator::new(
                name,
                cfg.clone(),
                projection_sql.map(Arc::from),
                self.ctx.clone(),
                self.lookup_registry.clone(),
            ));
        }

        if let Some(cfg) = stream_join_config {
            let op = operator::interval_join::IntervalJoinOperator::new(
                name,
                cfg.clone(),
                projection_sql.map(Arc::from),
                self.ctx.clone(),
            );
            return Box::new(op);
        }

        // Non-windowed now() is only valid as a retracting temporal filter under EMIT CHANGES;
        // anything else gets a typed LDB-1001 rejection.
        if window_config.is_none() {
            use crate::sql_analysis::TemporalFilterAnalysis as Tfa;
            match crate::sql_analysis::analyze_temporal_filter(sql) {
                Tfa::NotPresent => {}
                Tfa::Recognized(cfg) => {
                    let emit_changes =
                        emit_clause.is_some_and(|ec| matches!(ec, EmitClause::Changes));
                    if emit_changes {
                        return Box::new(operator::temporal_filter::TemporalFilterOperator::new(
                            name,
                            sql,
                            *cfg,
                            self.prom.clone(),
                        ));
                    }
                    return Box::new(operator::temporal_filter::RejectingOperator::new(
                        "[LDB-1001] a retracting temporal filter (time_col vs \
                         now() ± INTERVAL) must be declared `EMIT CHANGES`; \
                         append-only / EMIT ON WINDOW CLOSE / text SUBSCRIBE \
                         consumers cannot consume retractions",
                    ));
                }
                Tfa::PresentUnrecognized => {
                    return Box::new(operator::temporal_filter::RejectingOperator::new(
                        "[LDB-1001] now()/current_timestamp() in a non-windowed \
                         query is only supported as a retracting temporal filter \
                         `SELECT * FROM <src> WHERE time_col {>|>=|<|<=} now() ± \
                         INTERVAL` (or BETWEEN) declared `EMIT CHANGES`",
                    ));
                }
            }
        }

        let is_eowc = emit_clause
            .is_some_and(|ec| matches!(ec, EmitClause::OnWindowClose | EmitClause::Final));

        if is_eowc {
            return Box::new(operator::eowc_query::EowcQueryOperator::new(
                name,
                sql,
                emit_clause.cloned(),
                window_config.cloned(),
                self.ctx.clone(),
                self.prom.clone(),
            ));
        }

        // `EMIT CHANGES` is an explicit changelog; `incremental` drives the same dirty-only emit
        // internally for a terminal running-state aggregate MV.
        let emit_changelog =
            incremental || emit_clause.is_some_and(|ec| matches!(ec, EmitClause::Changes));

        #[cfg_attr(not(feature = "cluster"), allow(unused_mut))]
        let mut op = operator::sql_query::SqlQueryOperator::new(
            name,
            sql,
            self.ctx.clone(),
            self.prom.clone(),
            emit_changelog,
        );
        #[cfg(feature = "cluster")]
        if let Some(ref cfg) = self.cluster_shuffle {
            op.attach_cluster_shuffle(cfg.clone());
            // Delta checkpoints are a cluster (per-vnode) capability — only wire when sharded.
            // Enabling delta also makes the chain the primary agg checkpoint.
            if let Some(chain_bound) = self.delta_chain_bound {
                op.enable_delta_checkpoints(chain_bound);
            }
            if self.vnode_partials_authoritative {
                op.set_vnode_partials_authoritative();
            }
        }
        Box::new(op)
    }

    pub fn remove_query(&mut self, name: &str) {
        let prefix = format!("{name}::");
        let ids_to_remove: smallvec::SmallVec<[usize; 3]> = self
            .output_map
            .get(name)
            .copied()
            .into_iter()
            .chain(
                self.nodes
                    .iter()
                    .enumerate()
                    .filter(|(_, n)| !n.removed && n.name.starts_with(&prefix))
                    .map(|(i, _)| i),
            )
            .collect();

        for &id in &ids_to_remove {
            self.nodes[id].removed = true;
            self.nodes[id].operator = Box::new(TombstonedOperator);
            self.nodes[id].output_routes.clear();
            for port_buf in &mut self.input_bufs[id] {
                port_buf.clear();
            }
            for slot in &mut self.input_buf_bytes[id] {
                *slot = 0;
            }
            self.order_configs.remove(&id);
            self.depends_on_stream.remove(&id);
            self.edges.retain(|e| e.source != id && e.target != id);
            self.input_sources[id].fill(usize::MAX);
            self.output_watermarks[id] = i64::MIN;
            self.free_node_ids.push(id);
        }

        for node in &mut self.nodes {
            node.output_routes
                .retain(|&(t, _)| !ids_to_remove.contains(&t));
        }

        self.output_map.remove(name);
        self.incremental_tables.remove(name);
        self.temporal_configs
            .retain(|(query_name, _)| query_name != name);
        self.live_handles.remove(name);
        if !ids_to_remove.is_empty() {
            self.topo_dirty = true;
        }
    }

    #[cfg(test)]
    pub(crate) fn has_query(&self, name: &str) -> bool {
        self.output_map.contains_key(name)
    }

    #[cfg(test)]
    pub(crate) fn node_count(&self) -> usize {
        self.nodes.len()
    }

    fn compute_topo_order(&mut self) {
        let n = self.nodes.len();
        let mut in_degree = vec![0usize; n];
        let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n];

        for edge in &self.edges {
            if !self.nodes[edge.source].removed && !self.nodes[edge.target].removed {
                in_degree[edge.target] += 1;
                dependents[edge.source].push(edge.target);
            }
        }

        for deps in &mut dependents {
            deps.sort_unstable();
            deps.dedup();
        }

        in_degree.fill(0);
        for deps in &dependents {
            for &dep in deps {
                in_degree[dep] += 1;
            }
        }

        let mut queue = VecDeque::new();
        for (i, &deg) in in_degree.iter().enumerate() {
            if deg == 0 && !self.nodes[i].removed {
                queue.push_back(i);
            }
        }

        self.topo_order.clear();
        while let Some(idx) = queue.pop_front() {
            self.topo_order.push(idx);
            for &dep in &dependents[idx] {
                in_degree[dep] = in_degree[dep].saturating_sub(1);
                if in_degree[dep] == 0 {
                    queue.push_back(dep);
                }
            }
        }

        // Cycle detected: fall back to insertion order for remaining nodes.
        let active_count = self.nodes.iter().filter(|n| !n.removed).count();
        if self.topo_order.len() < active_count {
            tracing::warn!(
                ordered = self.topo_order.len(),
                total = active_count,
                "circular dependency in operator graph, \
                 falling back to insertion order for remaining nodes"
            );
            let in_order: FxHashSet<usize> = self.topo_order.iter().copied().collect();
            for i in 0..n {
                if !in_order.contains(&i) && !self.nodes[i].removed {
                    self.topo_order.push(i);
                }
            }
        }

        self.compute_node_domains();

        self.source_list.clear();
        self.source_list
            .extend(self.source_map.iter().map(|(k, v)| (Arc::clone(k), *v)));

        self.output_node_ids.clear();
        self.output_node_ids
            .extend(self.output_map.values().copied());

        self.topo_dirty = false;
    }

    /// Partition into failure domains (connected components) via union-find over undirected
    /// edges. Queries sharing a source node join one domain so they recover together —
    /// re-seeking a shared source for one would re-feed the other.
    fn compute_node_domains(&mut self) {
        fn find(parent: &mut [usize], mut x: usize) -> usize {
            while parent[x] != x {
                parent[x] = parent[parent[x]];
                x = parent[x];
            }
            x
        }

        let n = self.nodes.len();
        let mut parent: Vec<usize> = (0..n).collect();

        for edge in &self.edges {
            if self.nodes[edge.source].removed || self.nodes[edge.target].removed {
                continue;
            }
            // Cut at source nodes so two queries reading one source don't fuse into a single domain.
            // Sources have no incoming edges, so skipping their outgoing edges leaves each isolated.
            if self.shared_source_isolation && self.source_node_ids.contains(&edge.source) {
                continue;
            }
            let a = find(&mut parent, edge.source);
            let b = find(&mut parent, edge.target);
            if a != b {
                parent[a] = b;
            }
        }

        self.node_domain.clear();
        self.node_domain.resize(n, usize::MAX);
        let mut root_to_domain: FxHashMap<usize, usize> = FxHashMap::default();
        for i in 0..n {
            if self.nodes[i].removed {
                continue;
            }
            // Under isolation, sources are shared infrastructure, not a failure domain: a
            // consumer fault holds the source back, but the source itself never faults.
            // Leaving them unassigned (MAX) keeps `domain_count` equal to the number of
            // query domains, so the all-domains-failed check below stays exact.
            if self.shared_source_isolation && self.source_node_ids.contains(&i) {
                continue;
            }
            let root = find(&mut parent, i);
            let next = root_to_domain.len();
            self.node_domain[i] = *root_to_domain.entry(root).or_insert(next);
        }
        // When isolation is off this includes inert source-only domains, so
        // `failed_domains.len() == domain_count` is a conservative "all domains failed" test.
        self.domain_count = root_to_domain.len();
    }

    // A source is held back when its own domain faulted (isolation off / source unioned into a
    // consumer's domain) or, under isolation, when any domain it feeds faulted — the source is
    // cut out of every consumer's domain, so check its direct targets.
    fn source_feeds_failed_domain(&self, source_node: usize, failed: &FxHashSet<usize>) -> bool {
        if failed.contains(&self.node_domain[source_node]) {
            return true;
        }
        self.shared_source_isolation
            && self.nodes[source_node]
                .output_routes
                .iter()
                .any(|&(target, _)| failed.contains(&self.node_domain[target]))
    }

    fn register_source_tables(&mut self, source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        for (name, batches) in source_batches {
            if batches.is_empty() {
                continue;
            }
            // Lazily create the provider if register_source_schema wasn't called (e.g. tests).
            if !self.live_handles.contains_key(name.as_ref()) {
                let schema = batches[0].schema();
                self.ensure_live_provider(name, &schema);
            }
            if let Some(handle) = self.live_handles.get(name.as_ref()) {
                handle.swap(batches.clone());
            }
        }
    }

    fn finish_cycle(&mut self) {
        for handle in self.live_handles.values() {
            handle.clear();
        }
    }

    async fn execute_single_operator(
        &mut self,
        node_id: usize,
        current_watermark: i64,
        results: &mut FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<(), DbError> {
        let accept = self.nodes[node_id].operator.wants_input();
        let (mut inputs, mut input_bytes) = if accept {
            (
                std::mem::take(&mut self.input_bufs[node_id]),
                std::mem::take(&mut self.input_buf_bytes[node_id]),
            )
        } else {
            (Vec::new(), Vec::new())
        };

        let port_count = self.nodes[node_id].input_port_count;
        let watermarks: smallvec::SmallVec<[i64; 2]> = (0..port_count)
            .map(|port| {
                let upstream = self.input_sources[node_id][port];
                if upstream < self.output_watermarks.len() {
                    self.output_watermarks[upstream]
                } else {
                    current_watermark
                }
            })
            .collect();

        let output_result = self.nodes[node_id]
            .operator
            .process(
                if accept { inputs.as_slice() } else { &[][..] },
                &watermarks,
            )
            .await;

        let batches = match output_result {
            Ok(b) => {
                // Reuse the Vecs (clear preserves capacity); when !accept, leave buffers intact.
                if accept {
                    for v in &mut inputs {
                        v.clear();
                    }
                    input_bytes.fill(0);
                    self.input_bufs[node_id] = inputs;
                    self.input_buf_bytes[node_id] = input_bytes;
                    // A watermark may cross this operator only after the corresponding input was
                    // consumed. Operators return `wants_input == false` while graph-owned rows
                    // remain buffered, so advancing here would close downstream windows past data
                    // that has not run yet.
                    self.propagate_operator_watermark(node_id, &watermarks, current_watermark);
                }
                b
            }
            Err(e) => {
                if e.requires_pipeline_recovery() || e.requires_pipeline_halt() {
                    return Err(e);
                }
                // Defer (preserve input, keep the cycle alive) when the upstream
                // isn't ready, OR when a cross-node shuffle target isn't reachable
                // yet (cluster formation): aborting the whole cycle would also drop
                // co-located streams (e.g. a pass-through exactly-once sink) whose
                // source rows the generator has already advanced past — an EO gap.
                if accept && (self.depends_on_stream.contains(&node_id) || e.is_shuffle_not_ready())
                {
                    self.input_bufs[node_id] = inputs;
                    self.input_buf_bytes[node_id] = input_bytes;
                    tracing::debug!(
                        query = %self.nodes[node_id].name,
                        error = %e,
                        "Query deferred (upstream/shuffle not ready); batches preserved for retry"
                    );
                    return Ok(());
                }
                if !accept {
                    return Err(e);
                }
                // Under shared-source isolation, keep the faulted operator's input so the next cycle
                // replays it with new arrivals; returns Err to isolate the domain. Bounded by `max_replay_buffer_bytes`.
                if self.shared_source_isolation
                    && input_bytes.iter().sum::<usize>() <= self.max_replay_buffer_bytes
                {
                    self.input_bufs[node_id] = inputs;
                    self.input_buf_bytes[node_id] = input_bytes;
                    return Err(e);
                }
                for v in &mut inputs {
                    v.clear();
                }
                input_bytes.fill(0);
                self.input_bufs[node_id] = inputs;
                self.input_buf_bytes[node_id] = input_bytes;
                return Err(e);
            }
        };

        let batches = if let Some(oc) = self.order_configs.get(&node_id) {
            match oc {
                OrderOperatorConfig::TopK(c) => apply_topk_filter(&batches, c.k),
                OrderOperatorConfig::PerGroupTopK(c) => apply_topk_filter(&batches, c.k),
                _ => batches,
            }
        } else {
            batches
        };

        self.route_output(node_id, batches, results);

        Ok(())
    }

    /// Source nodes are pre-seeded in `execute_cycle`, so skip them here.
    fn propagate_operator_watermark(
        &mut self,
        node_id: usize,
        watermarks: &[i64],
        current_watermark: i64,
    ) {
        if self.source_node_ids.contains(&node_id) {
            return;
        }
        let mut wm = watermarks
            .iter()
            .copied()
            .min()
            .unwrap_or(current_watermark);
        if let Some(hold) = self.nodes[node_id].operator.watermark_hold() {
            wm = wm.min(hold);
        }
        self.output_watermarks[node_id] = wm;
        if let Some(ref prom) = self.prom {
            prom.stream_watermark_ms
                .with_label_values(&[&self.nodes[node_id].name])
                .set(wm);
        }
    }

    fn route_output(
        &mut self,
        node_id: usize,
        batches: Vec<RecordBatch>,
        results: &mut FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) {
        if batches.is_empty() {
            return;
        }
        let node_name = Arc::clone(&self.nodes[node_id].name);
        let has_routes = !self.nodes[node_id].output_routes.is_empty();
        let is_output = self.output_node_ids.contains(&node_id);

        if has_routes {
            let name_ref = node_name.as_ref();
            if !self.live_handles.contains_key(name_ref) {
                let schema = batches[0].schema();
                self.ensure_live_provider(name_ref, &schema);
            }
            if let Some(handle) = self.live_handles.get(name_ref) {
                handle.swap(batches.clone());
            }
        }

        if is_output {
            results.insert(node_name, batches.clone());
        }

        let bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        let route_count = self.nodes[node_id].output_routes.len();
        if route_count == 1 {
            let (target, port) = self.nodes[node_id].output_routes[0];
            self.push_to_port(target, port, batches, bytes);
        } else if route_count > 1 {
            // Clone batches N-1 times; the last route takes ownership.
            for i in 0..route_count - 1 {
                let (target, port) = self.nodes[node_id].output_routes[i];
                self.push_to_port(target, port, batches.clone(), bytes);
            }
            let (target, port) = self.nodes[node_id].output_routes[route_count - 1];
            self.push_to_port(target, port, batches, bytes);
        }
    }

    pub async fn execute_cycle(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        current_watermark: i64,
        source_watermarks: Option<&FxHashMap<Arc<str>, i64>>,
    ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, DbError> {
        self.execute_cycle_with_mode(
            source_batches,
            current_watermark,
            source_watermarks,
            GraphExecutionMode::Normal,
        )
        .await
    }

    /// Execute one aligned-checkpoint drain pass. This shares the normal cycle path but does not
    /// defer operators because the interactive query budget elapsed; backpressure gates and all
    /// operator error, watermark, state-limit, and routing behavior remain unchanged.
    pub(crate) async fn execute_checkpoint_drain_cycle(
        &mut self,
        current_watermark: i64,
        frozen_source_watermarks: Option<&FxHashMap<Arc<str>, i64>>,
    ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, DbError> {
        let source_batches = FxHashMap::default();
        self.execute_cycle_with_mode(
            &source_batches,
            current_watermark,
            frozen_source_watermarks,
            GraphExecutionMode::CheckpointDrain,
        )
        .await
    }

    async fn execute_cycle_with_mode(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        current_watermark: i64,
        source_watermarks: Option<&FxHashMap<Arc<str>, i64>>,
        mode: GraphExecutionMode,
    ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, DbError> {
        self.whole_restore_open = false;
        #[cfg(feature = "cluster")]
        let _rotation_guard = match self.rotation_execution_fence.as_ref() {
            Some(fence) => Some(Arc::clone(fence).read_owned().await),
            None => None,
        };

        #[cfg(feature = "cluster")]
        {
            self.last_execution_assignment_version = None;
            let execution_assignment_version = if let Some(cfg) = &self.cluster_shuffle {
                let version = cfg.registry.versioned_snapshot().version();
                if cfg.sender.assignment_version() != version
                    || cfg.receiver.assignment_version() != version
                {
                    return Err(DbError::ShuffleNotReady(format!(
                        "shuffle transport assignment does not match execution assignment {version}"
                    )));
                }
                Some(version)
            } else {
                None
            };
            self.apply_revoked_vnodes()?;
            self.apply_rehydrated_vnodes()?;
            self.last_execution_assignment_version = execution_assignment_version;
        }

        if self.topo_dirty {
            self.compute_topo_order();
        }

        self.register_source_tables(source_batches);
        self.prime_sources(source_batches, current_watermark, source_watermarks);

        let checkpoint_drain_nodes = if mode == GraphExecutionMode::CheckpointDrain {
            Some(self.checkpoint_drain_nodes())
        } else {
            None
        };

        let mut results = FxHashMap::default();
        let cycle_start = std::time::Instant::now();
        let topo_len = self.topo_order.len();

        self.cycle_failed_sources.clear();
        self.cycle_any_failed = false;
        self.cycle_deferred_sources.clear();
        self.cycle_any_deferred = false;
        let mut failed_domains: FxHashSet<usize> = FxHashSet::default();
        let mut first_error: Option<DbError> = None;

        for i in 0..topo_len {
            let node_id = self.topo_order[i];

            if self.nodes[node_id].removed {
                continue;
            }
            if checkpoint_drain_nodes
                .as_ref()
                .is_some_and(|drain| !drain.contains(&node_id))
            {
                continue;
            }

            // Skip a faulted domain; downstream nodes share it, so this cascades.
            if failed_domains.contains(&self.node_domain[node_id]) {
                continue;
            }

            match self.gate_decision(node_id) {
                GateDecision::Run => {}
                GateDecision::Skip => continue,
                GateDecision::Fail => {
                    self.finish_cycle();
                    return Err(DbError::BackpressureFail(format!(
                        "input buffer at capacity downstream of '{}'",
                        self.nodes[node_id].name
                    )));
                }
            }

            if mode == GraphExecutionMode::Normal && i > 0 {
                #[allow(clippy::cast_possible_truncation)]
                let elapsed_ns = cycle_start.elapsed().as_nanos() as u64;
                if elapsed_ns > self.query_budget_ns {
                    tracing::debug!(
                        skipped = topo_len - i,
                        elapsed_ms = elapsed_ns / 1_000_000,
                        "per-query budget exceeded — deferring remaining operators"
                    );

                    if let Err(e) = self
                        .run_one_deferred_operator(
                            i,
                            topo_len,
                            current_watermark,
                            &mut results,
                            &mut failed_domains,
                            &mut first_error,
                        )
                        .await
                    {
                        self.finish_cycle();
                        return Err(e);
                    }

                    break;
                }
            }

            if let Err(e) = self
                .execute_single_operator(node_id, current_watermark, &mut results)
                .await
            {
                if e.requires_pipeline_recovery() || e.requires_pipeline_halt() {
                    self.finish_cycle();
                    return Err(e);
                }
                let domain = self.node_domain[node_id];
                tracing::warn!(
                    query = %self.nodes[node_id].name,
                    error = %e,
                    domain,
                    "[LDB-3023] operator faulted; isolating its failure domain"
                );
                failed_domains.insert(domain);
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        self.record_cycle_deferrals();
        self.complete_cycle(&failed_domains, first_error)?;

        Ok(results)
    }

    fn complete_cycle(
        &mut self,
        failed_domains: &FxHashSet<usize>,
        first_error: Option<DbError>,
    ) -> Result<(), DbError> {
        self.finish_cycle();

        #[cfg(debug_assertions)]
        self.debug_assert_byte_sums();

        self.sample_buffer_stats();
        if failed_domains.is_empty() {
            return Ok(());
        }

        self.cycle_any_failed = true;
        let failed_names: Vec<Arc<str>> = self
            .source_list
            .iter()
            .filter(|(_, node_id)| self.source_feeds_failed_domain(*node_id, failed_domains))
            .map(|(name, _)| Arc::clone(name))
            .collect();
        self.cycle_failed_sources.extend(failed_names);
        if failed_domains.len() == self.domain_count {
            return Err(first_error.unwrap_or_else(|| {
                DbError::Pipeline("all operator failure domains failed without an error".into())
            }));
        }
        Ok(())
    }

    /// `(any domain faulted, local source names whose domain faulted)` from the last
    /// `execute_cycle`, draining the set. The coordinator holds back these sources' offsets.
    pub fn take_cycle_failures(&mut self) -> (bool, FxHashSet<Arc<str>>) {
        (
            self.cycle_any_failed,
            std::mem::take(&mut self.cycle_failed_sources),
        )
    }

    /// `(any retained graph work, local source names whose cursor must be withheld)` from the
    /// last cycle. A remote-only cluster deferral has an empty source set but still returns true.
    pub fn take_cycle_deferrals(&mut self) -> (bool, FxHashSet<Arc<str>>) {
        (
            self.cycle_any_deferred,
            std::mem::take(&mut self.cycle_deferred_sources),
        )
    }

    fn record_cycle_deferrals(&mut self) {
        let deferred_nodes: FxHashSet<usize> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(node_id, node)| {
                !node.removed && self.node_has_checkpoint_pending_work(*node_id)
            })
            .map(|(node_id, _)| node_id)
            .collect();
        if deferred_nodes.is_empty() {
            return;
        }
        self.cycle_any_deferred = true;
        let deferred_domains: FxHashSet<usize> = deferred_nodes
            .iter()
            .filter(|node_id| !self.source_node_ids.contains(node_id))
            .map(|node_id| self.node_domain[*node_id])
            .collect();
        let deferred_sources: Vec<Arc<str>> = self
            .source_list
            .iter()
            .filter(|(_, node_id)| {
                deferred_nodes.contains(node_id)
                    || self.source_feeds_failed_domain(*node_id, &deferred_domains)
            })
            .map(|(name, _)| Arc::clone(name))
            .collect();
        self.cycle_deferred_sources.extend(deferred_sources);
    }

    fn prime_sources(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        current_watermark: i64,
        source_watermarks: Option<&FxHashMap<Arc<str>, i64>>,
    ) {
        for &(ref name, node_id) in &self.source_list {
            if let Some(batches) = source_batches.get(name) {
                if !batches.is_empty() {
                    let bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
                    self.input_bufs[node_id][0].extend(batches.iter().cloned());
                    self.input_buf_bytes[node_id][0] += bytes;
                }
            }
            let wm = source_watermarks
                .and_then(|m| m.get(name).copied())
                .unwrap_or(current_watermark);
            self.output_watermarks[node_id] = wm;
            if let Some(ref prom) = self.prom {
                prom.stream_watermark_ms.with_label_values(&[name]).set(wm);
            }
        }
    }

    /// Round-robin one deferred operator so a budget overrun can't starve the tail.
    /// Skips and records failed domains exactly like the main loop; only a backpressure
    /// `Fail` returns `Err` (whole-cycle halt).
    async fn run_one_deferred_operator(
        &mut self,
        i: usize,
        topo_len: usize,
        current_watermark: i64,
        results: &mut FxHashMap<Arc<str>, Vec<RecordBatch>>,
        failed_domains: &mut FxHashSet<usize>,
        first_error: &mut Option<DbError>,
    ) -> Result<(), DbError> {
        let deferred_count = topo_len - i;
        let start = self.deferred_scan_offset % deferred_count;
        for offset in 0..deferred_count {
            let j = i + (start + offset) % deferred_count;
            let deferred_id = self.topo_order[j];
            if self.nodes[deferred_id].removed {
                continue;
            }
            if failed_domains.contains(&self.node_domain[deferred_id]) {
                continue;
            }
            let has_input = self.input_bufs[deferred_id]
                .iter()
                .any(|port| !port.is_empty());
            if !has_input {
                continue;
            }
            match self.gate_decision(deferred_id) {
                GateDecision::Skip => continue,
                GateDecision::Fail => {
                    return Err(DbError::BackpressureFail(format!(
                        "input buffer at capacity downstream of '{}'",
                        self.nodes[deferred_id].name
                    )));
                }
                GateDecision::Run => {}
            }
            if let Err(e) = self
                .execute_single_operator(deferred_id, current_watermark, results)
                .await
            {
                if e.requires_pipeline_recovery() || e.requires_pipeline_halt() {
                    return Err(e);
                }
                let domain = self.node_domain[deferred_id];
                tracing::warn!(
                    query = %self.nodes[deferred_id].name,
                    error = %e,
                    domain,
                    "[LDB-3023] deferred operator faulted; isolating its failure domain"
                );
                failed_domains.insert(domain);
                if first_error.is_none() {
                    *first_error = Some(e);
                }
            }
            self.deferred_scan_offset = self.deferred_scan_offset.wrapping_add(1);
            break;
        }
        Ok(())
    }

    fn sample_buffer_stats(&mut self) {
        self.stats_tick = self.stats_tick.wrapping_add(1);
        if !self.stats_tick.is_multiple_of(STATS_SAMPLE_INTERVAL) {
            return;
        }
        if let Some(ref prom) = self.prom {
            for (id, ports) in self.input_buf_bytes.iter().enumerate() {
                if self.nodes[id].removed {
                    continue;
                }
                let total: usize = ports.iter().sum();
                prom.input_buf_bytes
                    .with_label_values(&[&self.nodes[id].name])
                    .set(i64::try_from(total).unwrap_or(i64::MAX));
            }
        }
    }

    #[cfg(feature = "cluster")]
    fn stage_checkpointed_shuffle(
        &mut self,
        stage: &str,
        batch: RetainedBatch,
        watermark: i64,
    ) -> Result<(), DbError> {
        let node_name = stage
            .strip_suffix("::left")
            .or_else(|| stage.strip_suffix("::right"))
            .unwrap_or(stage);
        let idx = self.find_node(node_name).ok_or_else(|| {
            DbError::Pipeline(format!(
                "shuffle frame targets unknown or removed stage '{stage}'"
            ))
        })?;
        let result = self.nodes[idx]
            .operator
            .stage_checkpointed_shuffle(stage, batch, watermark);
        if result.is_ok() {
            self.output_watermarks[idx] = self.output_watermarks[idx].min(watermark);
        }
        result
    }

    #[cfg(feature = "cluster")]
    fn stage_received_shuffle_data(
        &mut self,
        received: laminar_core::shuffle::ReceivedShuffle,
        watermark: i64,
    ) -> Result<(), DbError> {
        let assignment_version = received.assignment_version();
        let (message, admission) = received.into_parts();
        let laminar_core::shuffle::ShuffleMessage::Data { stage, batch, .. } = message else {
            return Err(DbError::Pipeline(
                "non-data frame entered shuffle data staging".into(),
            ));
        };
        self.stage_checkpointed_shuffle(
            &stage,
            RetainedBatch::admitted(batch, admission, assignment_version),
            watermark,
        )
    }

    #[cfg(feature = "cluster")]
    fn validate_shuffle_attempt_scope(
        cfg: &crate::operator::sql_query::ClusterShuffleConfig,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        recovery_gen: u64,
        controller: Option<&laminar_core::cluster::control::ClusterController>,
    ) -> Result<(), DbError> {
        if !assignment_fence.is_canonical() || !assignment_fence.contains(cfg.self_id.0) {
            return Err(DbError::Pipeline(
                "shuffle alignment has a non-canonical or incomplete assignment certificate".into(),
            ));
        }
        let assignment = cfg.registry.versioned_snapshot();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        let local_incarnation = assignment_fence.participant_incarnation(cfg.self_id.0);
        if assignment.version() != assignment_fence.assignment_version
            || !assignment_fence.matches_owner_map(&owners)
            || cfg.sender.assignment_version() != assignment_fence.assignment_version
            || cfg.receiver.assignment_version() != assignment_fence.assignment_version
            || local_incarnation != Some(cfg.sender.incarnation())
            || local_incarnation != Some(cfg.receiver.incarnation())
        {
            return Err(DbError::Pipeline(format!(
                "shuffle assignment differs from admitted certificate version {}",
                assignment_fence.assignment_version
            )));
        }
        if cfg.sender.recovery_gen() != recovery_gen || cfg.receiver.recovery_gen() != recovery_gen
        {
            return Err(DbError::Pipeline(format!(
                "shuffle recovery generation changed during alignment from {recovery_gen}"
            )));
        }
        if let Some(controller) = controller {
            let current = controller
                .checkpoint_assignment_fence(assignment_fence.assignment_version)
                .ok_or_else(|| {
                    DbError::Pipeline(format!(
                        "shuffle assignment {} is no longer checkpoint-ready",
                        assignment_fence.assignment_version
                    ))
                })?;
            if current != *assignment_fence {
                return Err(DbError::Pipeline(format!(
                    "shuffle assignment certificate changed at version {}",
                    assignment_fence.assignment_version
                )));
            }
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn validate_received_shuffle_scope(
        received: &laminar_core::shuffle::ReceivedShuffle,
        self_id: u64,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        recovery_gen: u64,
    ) -> Result<(), DbError> {
        let peer = received.peer();
        let expected_sender = assignment_fence.participant_incarnation(peer);
        let expected_receiver = assignment_fence.participant_incarnation(self_id);
        if peer == self_id
            || !assignment_fence.contains(peer)
            || expected_sender != Some(received.sender_incarnation())
            || expected_receiver != Some(received.receiver_incarnation())
            || received.stream_id().is_nil()
            || received.assignment_version() != assignment_fence.assignment_version
            || received.recovery_gen() != recovery_gen
        {
            return Err(DbError::Pipeline(format!(
                "shuffle frame from peer {peer} has the wrong assignment, recovery, or stream scope"
            )));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn validate_received_batch_scope(
        received: &laminar_core::shuffle::ReceivedBatch,
        self_id: u64,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        recovery_gen: u64,
    ) -> Result<(), DbError> {
        let peer = received.peer();
        let expected_sender = assignment_fence.participant_incarnation(peer);
        let expected_receiver = assignment_fence.participant_incarnation(self_id);
        if peer == self_id
            || !assignment_fence.contains(peer)
            || expected_sender != Some(received.sender_incarnation())
            || expected_receiver != Some(received.receiver_incarnation())
            || received.stream_id().is_nil()
            || received.assignment_version() != assignment_fence.assignment_version
            || received.recovery_gen() != recovery_gen
        {
            return Err(DbError::Pipeline(format!(
                "shuffle batch from peer {peer} has the wrong assignment, recovery, or stream scope"
            )));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn compare_shuffle_attempts(
        expected: laminar_core::state::CheckpointAttempt,
        observed: laminar_core::state::CheckpointAttempt,
    ) -> Result<std::cmp::Ordering, DbError> {
        match observed.relation_to(expected) {
            laminar_core::state::CheckpointAttemptRelation::Exact => Ok(std::cmp::Ordering::Equal),
            laminar_core::state::CheckpointAttemptRelation::Newer => {
                Ok(std::cmp::Ordering::Greater)
            }
            laminar_core::state::CheckpointAttemptRelation::Older => Ok(std::cmp::Ordering::Less),
            laminar_core::state::CheckpointAttemptRelation::Conflict => {
                Err(DbError::Pipeline(format!(
                "shuffle barrier attempt mismatch: expected {expected:?}, received {observed:?}"
            )))
            }
        }
    }

    #[cfg(feature = "cluster")]
    fn is_shuffle_alignment_terminal_hint(
        attempt: laminar_core::state::CheckpointAttempt,
        announcement: &laminar_core::cluster::control::BarrierAnnouncement,
        ignored: Option<(u64, u64, laminar_core::cluster::control::Phase)>,
    ) -> bool {
        use laminar_core::cluster::control::Phase;

        if ignored
            == Some((
                announcement.epoch,
                announcement.checkpoint_id,
                announcement.phase,
            ))
            || !matches!(announcement.phase, Phase::Commit | Phase::Abort)
        {
            return false;
        }
        let announced = laminar_core::state::CheckpointAttempt::new(
            announcement.epoch,
            announcement.checkpoint_id,
        );
        !matches!(
            announced.relation_to(attempt),
            laminar_core::state::CheckpointAttemptRelation::Older
        )
    }

    #[cfg(feature = "cluster")]
    async fn wait_for_shuffle_alignment_terminal_hint(
        controller: Option<&laminar_core::cluster::control::ClusterController>,
        attempt: laminar_core::state::CheckpointAttempt,
        ignored: Option<(u64, u64, laminar_core::cluster::control::Phase)>,
        deadline: tokio::time::Instant,
    ) -> Result<Option<laminar_core::cluster::control::BarrierAnnouncement>, DbError> {
        let Some(controller) = controller else {
            tokio::time::sleep_until(deadline).await;
            return Ok(None);
        };
        controller
            .wait_for_barrier(
                |announcement| {
                    Self::is_shuffle_alignment_terminal_hint(attempt, announcement, ignored)
                },
                deadline.saturating_duration_since(tokio::time::Instant::now()),
            )
            .await
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "shuffle barrier control observation failed: {error}"
                ))
            })
    }

    #[cfg(feature = "cluster")]
    async fn audit_shuffle_alignment_settlement(
        controller: Option<&laminar_core::cluster::control::ClusterController>,
        attempt: laminar_core::state::CheckpointAttempt,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    ) -> Result<Option<ShuffleAlignmentOutcome>, DbError> {
        let Some(controller) = controller else {
            return Ok(None);
        };

        // Announcements only wake this audit; immutable authority alone settles alignment.
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Pipeline(format!(
                "shuffle barrier terminal authority is unavailable: {error}"
            ))
        })?;
        let durable = authority
            .cluster_attempt_settlement(attempt)
            .await
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "shuffle barrier terminal outcome audit failed: {error}"
                ))
            })?;
        let Some(durable) = durable else {
            return Ok(None);
        };
        if durable.scope != laminar_core::checkpoint_decision::CheckpointScope::Cluster {
            return Err(DbError::Pipeline(format!(
                "shuffle barrier settlement for checkpoint {} epoch {} has non-cluster scope {:?}",
                attempt.checkpoint_id, attempt.epoch, durable.scope
            )));
        }
        let durable_attempt =
            laminar_core::state::CheckpointAttempt::new(durable.epoch, durable.checkpoint_id);
        match durable_attempt.relation_to(attempt) {
            laminar_core::state::CheckpointAttemptRelation::Exact => {
                if durable.assignment_fence.as_ref() != Some(assignment_fence) {
                    return Err(DbError::Pipeline(format!(
                        "shuffle barrier terminal outcome for checkpoint {} epoch {} has a different assignment certificate",
                        attempt.checkpoint_id, attempt.epoch
                    )));
                }
                if durable.verdict
                    != laminar_core::checkpoint_decision::CheckpointVerdict::Abort
                {
                    return Err(DbError::Pipeline(format!(
                        "shuffle barrier alignment for checkpoint {} epoch {} observed durable {:?} instead of Abort",
                        attempt.checkpoint_id, attempt.epoch, durable.verdict
                    )));
                }
                Ok(Some(ShuffleAlignmentOutcome::Aborted))
            }
            laminar_core::state::CheckpointAttemptRelation::Newer => Err(DbError::Pipeline(
                format!(
                    "checkpoint {} epoch {} was superseded by durable terminal checkpoint {} epoch {} ({:?})",
                    attempt.checkpoint_id,
                    attempt.epoch,
                    durable.checkpoint_id,
                    durable.epoch,
                    durable.verdict
                ),
            )),
            laminar_core::state::CheckpointAttemptRelation::Older
            | laminar_core::state::CheckpointAttemptRelation::Conflict => {
                Err(DbError::Pipeline(format!(
                    "shuffle barrier authority returned invalid settlement {durable_attempt:?} for pending attempt {attempt:?}"
                )))
            }
        }
    }

    /// Aligned shuffle checkpointing: fan out an in-band barrier, retain each peer's pre-barrier
    /// rows as channel state, and wait until every peer's barrier is observed before snapshotting.
    /// A barrier closes that peer's current-attempt channel: data from an already aligned peer while
    /// other peers are outstanding is a protocol violation and requires recovery.
    ///
    /// An exact, authority-validated leader Abort is a normal terminal outcome: rows dequeued
    /// before observing it remain staged in the live graph, and no snapshot was captured. A
    /// transport scope cancellation before local staging preserves the complete holdover for
    /// exact-attempt cleanup and a later checkpoint. Every failure after staging still requires
    /// coordinated recovery because partial staging may otherwise lose or double-apply data.
    ///
    /// # Errors
    /// Returns a recovery-classified error for timeout, loss, scope conflict, or supersession.
    #[cfg(feature = "cluster")]
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn align_shuffle_barriers(
        &mut self,
        attempt: laminar_core::state::CheckpointAttempt,
        watermark: i64,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        deadline: tokio::time::Instant,
        controller: Option<&laminar_core::cluster::control::ClusterController>,
    ) -> Result<ShuffleAlignmentOutcome, DbError> {
        use laminar_core::checkpoint::barrier::CheckpointBarrier;
        use laminar_core::shuffle::ShuffleMessage;
        use rustc_hash::{FxHashMap, FxHashSet};

        const RECHECK: std::time::Duration = std::time::Duration::from_millis(500);

        let Some(cfg) = self.cluster_shuffle.clone() else {
            return Ok(ShuffleAlignmentOutcome::Aligned);
        };
        let alignment = tokio::time::timeout_at(deadline, async {
            if cfg.receiver.assignment_version() == 0 || cfg.sender.assignment_version() == 0 {
                return Ok(ShuffleAlignmentOutcome::ScopeCancelledBeforeStaging);
            }
            let recovery_gen = cfg.receiver.recovery_gen();
            Self::validate_shuffle_attempt_scope(
                &cfg,
                assignment_fence,
                recovery_gen,
                controller,
            )?;
            let ensure_no_delivery_loss = || {
                if cfg.receiver.has_unrecovered_delivery_loss() {
                    Err(DbError::Pipeline(
                        "shuffle delivery-domain or transit loss requires recovery".into(),
                    ))
                } else {
                    Ok(())
                }
            };
            ensure_no_delivery_loss()?;
            let peers: Vec<u64> = assignment_fence
                .participants
                .iter()
                .map(|participant| participant.node_id)
                .filter(|peer| *peer != cfg.self_id.0)
                .collect();
            if peers.is_empty() {
                return Ok(ShuffleAlignmentOutcome::Aligned);
            }

            let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);
            if let Err(error) = cfg
                .sender
                .fan_out_barrier(&peers, barrier, assignment_fence)
                .await
            {
                if laminar_core::shuffle::is_scope_cancelled(&error) {
                    return Ok(ShuffleAlignmentOutcome::ScopeCancelledBeforeStaging);
                }
                return Err(DbError::Pipeline(format!(
                    "shuffle barrier fan-out: {error}"
                )));
            }

            // Fan-out can cancel while waiting on admission. Keep holdover ownership in the
            // receiver until every failed peer has either accepted or explicitly left the scope.
            let staged_batches = match cfg.receiver.drain_checkpointed_holdover() {
                Ok(staged) => staged,
                Err(error) if laminar_core::shuffle::is_scope_cancelled(&error) => {
                    return Ok(ShuffleAlignmentOutcome::ScopeCancelledBeforeStaging);
                }
                Err(error) => {
                    return Err(DbError::Pipeline(format!(
                        "shuffle checkpoint holdover drain: {error}"
                    )));
                }
            };
            ensure_no_delivery_loss()?;

            let mut remaining: FxHashSet<u64> = peers.iter().copied().collect();
            let mut barrier_cuts: FxHashMap<u64, u64> = FxHashMap::default();
            tracing::debug!(
                checkpoint_id = attempt.checkpoint_id,
                epoch = attempt.epoch,
                self_id = cfg.self_id.0,
                peers = ?peers,
                "shuffle align: start"
            );

            for received in cfg.receiver.drain_staged_barriers() {
                Self::validate_received_shuffle_scope(
                    &received,
                    cfg.self_id.0,
                    assignment_fence,
                    recovery_gen,
                )?;
                if received.assignment_digest() != Some(assignment_fence.digest()) {
                    return Err(DbError::Pipeline(format!(
                        "shuffle barrier from peer {} has the wrong assignment certificate",
                        received.peer()
                    )));
                }
                let ShuffleMessage::Barrier(barrier) = received.message() else {
                    return Err(DbError::Pipeline(
                        "non-barrier frame entered shuffle barrier holdover".into(),
                    ));
                };
                let observed = laminar_core::state::CheckpointAttempt::new(
                    barrier.epoch,
                    barrier.checkpoint_id,
                );
                match Self::compare_shuffle_attempts(attempt, observed)? {
                    std::cmp::Ordering::Equal => {
                        let peer = received.peer();
                        let cut = received.checkpoint_sequence();
                        if let Some(previous) = barrier_cuts.insert(peer, cut) {
                            if previous != cut {
                                return Err(DbError::Pipeline(format!(
                                    "shuffle peer {peer} repeated checkpoint barrier with conflicting high-water sequences {previous} and {cut}"
                                )));
                            }
                        }
                        remaining.remove(&peer);
                    }
                    std::cmp::Ordering::Greater => cfg.receiver.stash_barrier(received),
                    std::cmp::Ordering::Less => {}
                }
            }

            // A normal execution cycle may already have bucketed data and a later barrier into
            // separate holdovers. The transport sequence reconstructs their per-peer order: data
            // below the barrier's exclusive high-water belongs to this cut; data at/above it does
            // not and cannot be sealed into the current snapshot.
            let mut post_cut_batches = Vec::new();
            let mut post_cut_error = None;
            for (stage, received) in staged_batches {
                Self::validate_received_batch_scope(
                    &received,
                    cfg.self_id.0,
                    assignment_fence,
                    recovery_gen,
                )?;
                let peer = received.peer();
                let sequence = received.checkpoint_sequence();
                if let Some(cut) = barrier_cuts.get(&peer) {
                    if sequence >= *cut {
                        post_cut_error.get_or_insert_with(|| {
                            format!(
                                "shuffle data sequence {sequence} from peer {peer} is at or after its checkpoint barrier high-water {cut}"
                            )
                        });
                        post_cut_batches.push((stage, received));
                        continue;
                    }
                }
                self.stage_checkpointed_shuffle(
                    &stage,
                    RetainedBatch::from_received(received),
                    watermark,
                )?;
            }
            ensure_no_delivery_loss()?;
            if let Some(error) = post_cut_error {
                if let Some(outcome) =
                    Self::audit_shuffle_alignment_settlement(
                        controller,
                        attempt,
                        assignment_fence,
                    )
                    .await?
                {
                    for (stage, received) in post_cut_batches {
                        self.stage_checkpointed_shuffle(
                            &stage,
                            RetainedBatch::from_received(received),
                            watermark,
                        )?;
                    }
                    return Ok(outcome);
                }
                return Err(DbError::Pipeline(error));
            }
            if remaining.is_empty() {
                Self::validate_shuffle_attempt_scope(
                    &cfg,
                    assignment_fence,
                    recovery_gen,
                    controller,
                )?;
                ensure_no_delivery_loss()?;
                if let Some(outcome) =
                    Self::audit_shuffle_alignment_settlement(
                        controller,
                        attempt,
                        assignment_fence,
                    )
                    .await?
                {
                    return Ok(outcome);
                }
                return Ok(ShuffleAlignmentOutcome::Aligned);
            }

            let mut check_interval = tokio::time::interval_at(
                tokio::time::Instant::now() + RECHECK,
                RECHECK,
            );
            check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut ignored_terminal_hint = None;
            let terminal_hint = Self::wait_for_shuffle_alignment_terminal_hint(
                controller,
                attempt,
                ignored_terminal_hint,
                deadline,
            );
            tokio::pin!(terminal_hint);
            loop {
                tokio::select! {
                    res = cfg.receiver.recv() => {
                        let received = res.ok_or_else(|| DbError::Pipeline(
                            "shuffle receiver closed during barrier alignment".into(),
                        ))?;
                        Self::validate_received_shuffle_scope(
                            &received,
                            cfg.self_id.0,
                            assignment_fence,
                            recovery_gen,
                        )?;
                        if matches!(received.message(), ShuffleMessage::Data { .. }) {
                            let peer = received.peer();
                            let sequence = received.checkpoint_sequence();
                            if let Some(cut) = barrier_cuts.get(&peer) {
                                if let Some(outcome) = Self::audit_shuffle_alignment_settlement(
                                    controller,
                                    attempt,
                                    assignment_fence,
                                )
                                .await?
                                {
                                    self.stage_received_shuffle_data(received, watermark)?;
                                    return Ok(outcome);
                                }
                                return Err(DbError::Pipeline(format!(
                                    "shuffle data sequence {sequence} from peer {peer} arrived after its checkpoint barrier high-water {cut} while peers {remaining:?} were still outstanding"
                                )));
                            }
                            self.stage_received_shuffle_data(received, watermark)?;
                            continue;
                        }

                        let ShuffleMessage::Barrier(barrier) = received.message() else {
                            unreachable!("shuffle message variants are exhaustive");
                        };
                        if received.assignment_digest() != Some(assignment_fence.digest()) {
                            return Err(DbError::Pipeline(format!(
                                "shuffle barrier from peer {} has the wrong assignment certificate",
                                received.peer()
                            )));
                        }
                        let observed = laminar_core::state::CheckpointAttempt::new(
                            barrier.epoch,
                            barrier.checkpoint_id,
                        );
                        match Self::compare_shuffle_attempts(attempt, observed)? {
                            std::cmp::Ordering::Equal => {
                                let peer = received.peer();
                                let cut = received.checkpoint_sequence();
                                if let Some(previous) = barrier_cuts.insert(peer, cut) {
                                    if previous != cut {
                                        return Err(DbError::Pipeline(format!(
                                            "shuffle peer {peer} repeated checkpoint barrier with conflicting high-water sequences {previous} and {cut}"
                                        )));
                                    }
                                }
                                let first_observation = remaining.remove(&peer);
                                if first_observation && remaining.is_empty() {
                                    break;
                                }
                            }
                            std::cmp::Ordering::Greater => {
                                cfg.receiver.stash_barrier(received);
                            }
                            std::cmp::Ordering::Less => {}
                        }
                    }
                    hint = &mut terminal_hint => {
                        let Some(hint) = hint? else {
                            return Err(DbError::Checkpoint(format!(
                                "shuffle barrier control wait exhausted the absolute deadline for checkpoint {} epoch {}",
                                attempt.checkpoint_id, attempt.epoch
                            )));
                        };
                        ignored_terminal_hint = Some((
                            hint.epoch,
                            hint.checkpoint_id,
                            hint.phase,
                        ));
                        Self::validate_shuffle_attempt_scope(
                            &cfg,
                            assignment_fence,
                            recovery_gen,
                            controller,
                        )?;
                        ensure_no_delivery_loss()?;
                        if let Some(outcome) = Self::audit_shuffle_alignment_settlement(
                            controller,
                            attempt,
                            assignment_fence,
                        )
                        .await?
                        {
                            return Ok(outcome);
                        }
                        check_interval.reset_at(tokio::time::Instant::now() + RECHECK);
                        terminal_hint.set(Self::wait_for_shuffle_alignment_terminal_hint(
                            controller,
                            attempt,
                            ignored_terminal_hint,
                            deadline,
                        ));
                    }
                    _ = check_interval.tick() => {
                        Self::validate_shuffle_attempt_scope(
                            &cfg,
                            assignment_fence,
                            recovery_gen,
                            controller,
                        )?;
                        ensure_no_delivery_loss()?;
                        if let Some(outcome) =
                            Self::audit_shuffle_alignment_settlement(
                                controller,
                                attempt,
                                assignment_fence,
                            )
                            .await?
                        {
                            return Ok(outcome);
                        }
                    }
                }
            }
            Self::validate_shuffle_attempt_scope(
                &cfg,
                assignment_fence,
                recovery_gen,
                controller,
            )?;
            ensure_no_delivery_loss()?;
            if let Some(outcome) =
                Self::audit_shuffle_alignment_settlement(controller, attempt, assignment_fence)
                    .await?
            {
                return Ok(outcome);
            }
            tracing::debug!(
                checkpoint_id = attempt.checkpoint_id,
                epoch = attempt.epoch,
                "shuffle align: complete"
            );
            Ok(ShuffleAlignmentOutcome::Aligned)
        })
        .await
        .map_err(|_| {
            DbError::Checkpoint(format!(
                "shuffle barrier alignment exhausted the absolute deadline for checkpoint {} epoch {}",
                attempt.checkpoint_id, attempt.epoch
            ))
        })?;
        alignment.map_err(|error| {
            DbError::Checkpoint(format!(
                "shuffle barrier alignment for checkpoint {} epoch {} requires recovery: {error}",
                attempt.checkpoint_id, attempt.epoch
            ))
        })
    }

    #[cfg(test)]
    pub(crate) fn push_test_node(&mut self, name: &str, operator: Box<dyn GraphOperator>) {
        self.allocate_node(GraphNode {
            name: Arc::from(name),
            operator,
            input_port_count: 1,
            output_routes: Vec::new(),
            removed: false,
        });
        self.topo_dirty = true;
    }

    #[cfg(all(test, feature = "cluster"))]
    pub(crate) fn set_test_vnode_count(&mut self, vnode_count: u32) {
        self.vnode_count = Some(vnode_count);
    }

    pub fn snapshot_state(&mut self) -> Result<Option<GraphCheckpoint>, DbError> {
        let mut operators = OperatorStateMap::new();
        for node in &mut self.nodes {
            if node.removed {
                continue;
            }
            if let Some(cp) = node.operator.checkpoint()? {
                operators.insert(node.name.to_string(), cp.data);
            }
        }
        if operators.is_empty() {
            return Ok(None);
        }
        Ok(Some(GraphCheckpoint {
            version: GRAPH_CHECKPOINT_VERSION,
            operators,
        }))
    }

    /// Per-vnode state snapshot (`vnode → operator → bytes`) for cross-node rehydration.
    #[cfg(feature = "cluster")]
    #[allow(clippy::disallowed_types)] // std HashMap matches the trait/CheckpointRequest shape
    pub fn snapshot_state_by_vnode(
        &mut self,
    ) -> Result<crate::checkpoint_coordinator::StagedVnodeStates, DbError> {
        let Some(vnode_count) = self.vnode_count else {
            return Ok(std::collections::HashMap::new());
        };
        let mut out: crate::checkpoint_coordinator::StagedVnodeStates =
            std::collections::HashMap::new();
        for node in &mut self.nodes {
            if node.removed {
                continue;
            }
            if let Some(per_vnode) = node.operator.checkpoint_by_vnode(vnode_count)? {
                for (vnode, bytes) in per_vnode {
                    out.entry(vnode)
                        .or_default()
                        .insert(node.name.to_string(), bytes);
                }
            }
        }
        Ok(out)
    }

    /// Force every operator's next delta capture to re-base FULL after a failed epoch, so no chain
    /// outruns the coordinator's parent link.
    #[cfg(feature = "cluster")]
    pub(crate) fn force_full_rebase(&mut self) {
        for node in &mut self.nodes {
            if !node.removed {
                node.operator.force_full_rebase();
            }
        }
    }

    /// Restore a newly built graph. The graph is consumed so any late operator failure drops the
    /// partially restored image instead of returning it to the caller.
    pub fn restore_state(mut self, checkpoint: &GraphCheckpoint) -> Result<(Self, usize), DbError> {
        if !self.whole_restore_open {
            return Err(DbError::Checkpoint(
                "[LDB-6029] operator graph restore is only valid before the first execution cycle"
                    .into(),
            ));
        }
        if checkpoint.version != GRAPH_CHECKPOINT_VERSION {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6043] unsupported operator graph checkpoint version {}; expected {}",
                checkpoint.version, GRAPH_CHECKPOINT_VERSION
            )));
        }
        let mut missing: Vec<&str> = checkpoint
            .operators
            .keys()
            .map(String::as_str)
            .filter(|operator| {
                !self
                    .nodes
                    .iter()
                    .any(|node| !node.removed && &*node.name == *operator)
            })
            .collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            return Err(DbError::Checkpoint(format!(
                "[LDB-6029] operator graph checkpoint requires missing operator(s): {}",
                missing.join(", ")
            )));
        }
        let mut restored = 0;
        for node_id in 0..self.nodes.len() {
            let node = &mut self.nodes[node_id];
            if node.removed {
                continue;
            }
            if let Some(bytes) = checkpoint.operators.get(&*node.name) {
                node.operator
                    .restore(OperatorCheckpoint {
                        data: bytes.clone(),
                    })
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "[LDB-6029] operator '{}' restore failed: {error}",
                            node.name
                        ))
                    })?;
                #[cfg(feature = "cluster")]
                if let Some(watermark) = node.operator.restored_output_watermark() {
                    self.output_watermarks[node_id] = watermark;
                }
                restored += 1;
            }
        }
        self.whole_restore_open = false;
        Ok((self, restored))
    }

    pub fn serialize_checkpoint_bounded(
        cp: &GraphCheckpoint,
        max_bytes: u64,
    ) -> Result<Vec<u8>, DbError> {
        let max_bytes = usize::try_from(max_bytes).map_err(|_| {
            DbError::Checkpoint("operator graph checkpoint budget does not fit usize".into())
        })?;
        let writer = rkyv::ser::writer::IoWriter::new(
            laminar_core::serialization::BoundedBytesWriter::new(max_bytes),
        );
        rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(cp, writer)
            .map(|writer| writer.into_inner().into_vec())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "operator graph checkpoint serialization exceeded its {max_bytes}-byte budget: {error}"
                ))
            })
    }

    pub fn restore_from_bytes(self, bytes: &[u8]) -> Result<(Self, usize), DbError> {
        let checkpoint: GraphCheckpoint =
            rkyv::from_bytes::<GraphCheckpoint, rkyv::rancor::Error>(bytes).map_err(|e| {
                DbError::Checkpoint(format!("operator graph checkpoint deserialization: {e}"))
            })?;
        self.restore_state(&checkpoint)
    }
}

pub(crate) fn try_evaluate_compiled(
    proj: &crate::aggregate_state::CompiledProjection,
    batches: &[RecordBatch],
) -> Result<Vec<RecordBatch>, crate::error::DbError> {
    let mut result = Vec::with_capacity(batches.len());
    for batch in batches {
        let b = proj.evaluate(batch)?;
        if b.num_rows() > 0 {
            result.push(b);
        }
    }
    Ok(result)
}

#[cfg(test)]
#[allow(clippy::redundant_closure_for_method_calls)]
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

    fn test_batch() -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(Float64Array::from(vec![150.0, 2800.0])),
                Arc::new(Int64Array::from(vec![1000, 2000])),
            ],
        )
        .unwrap()
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn default_operator_rejects_checkpointed_shuffle() {
        let mut operator = SourcePassthrough;

        let error = operator
            .stage_checkpointed_shuffle(
                "unadmitted-join-stage",
                RetainedBatch::local(test_batch()),
                0,
            )
            .expect_err("operators without an admitted shuffle path must fail closed");

        assert!(error
            .to_string()
            .contains("does not accept checkpointed shuffle stage"));
    }

    struct RestoreProbe(Arc<std::sync::atomic::AtomicUsize>);

    #[async_trait]
    impl GraphOperator for RestoreProbe {
        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    struct RestoreFailureProbe {
        restores: Arc<std::sync::atomic::AtomicUsize>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
        fail: bool,
    }

    impl Drop for RestoreFailureProbe {
        fn drop(&mut self) {
            self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl GraphOperator for RestoreFailureProbe {
        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
            self.restores
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.fail {
                Err(DbError::Pipeline("injected late restore failure".into()))
            } else {
                Ok(())
            }
        }
    }

    #[cfg(feature = "cluster")]
    struct RestoredReplayWatermarkProbe {
        replay_watermark: Option<i64>,
        processed: Arc<std::sync::atomic::AtomicBool>,
    }

    #[cfg(feature = "cluster")]
    #[async_trait]
    impl GraphOperator for RestoredReplayWatermarkProbe {
        async fn process(
            &mut self,
            inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            if self.replay_watermark.is_none() {
                return Ok(Vec::new());
            }
            assert!(inputs.is_empty(), "replay-only cycle accepted new input");
            self.replay_watermark = None;
            self.processed
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(vec![test_batch()])
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(self.replay_watermark.map(|watermark| OperatorCheckpoint {
                data: watermark.to_le_bytes().to_vec(),
            }))
        }

        fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
            let encoded: [u8; 8] = checkpoint.data.try_into().map_err(|_| {
                DbError::Checkpoint("invalid replay-watermark probe checkpoint".into())
            })?;
            self.replay_watermark = Some(i64::from_le_bytes(encoded));
            Ok(())
        }

        fn watermark_hold(&self) -> Option<i64> {
            self.replay_watermark
        }

        fn restored_output_watermark(&self) -> Option<i64> {
            self.replay_watermark
        }

        fn wants_input(&self) -> bool {
            self.replay_watermark.is_none()
        }
    }

    #[test]
    fn whole_graph_restore_rejects_old_abi_before_mutation() {
        let restores = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
        graph.allocate_node(GraphNode {
            name: Arc::from("present"),
            operator: Box::new(RestoreProbe(Arc::clone(&restores))),
            input_port_count: 1,
            output_routes: Vec::new(),
            removed: false,
        });
        let mut operators = OperatorStateMap::new();
        operators.insert("present".into(), vec![1]);

        let error = graph
            .restore_state(&GraphCheckpoint {
                version: GRAPH_CHECKPOINT_VERSION - 1,
                operators,
            })
            .err()
            .expect("old graph ABI must fail");

        assert!(error.to_string().contains("[LDB-6043]"), "{error}");
        assert_eq!(restores.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn whole_graph_checkpoint_serialization_enforces_its_byte_budget() {
        let mut operators = OperatorStateMap::new();
        operators.insert("stateful".into(), vec![42; 4_096]);
        let checkpoint = GraphCheckpoint {
            version: GRAPH_CHECKPOINT_VERSION,
            operators,
        };
        let encoded = OperatorGraph::serialize_checkpoint_bounded(&checkpoint, u64::MAX).unwrap();
        let restored = rkyv::from_bytes::<GraphCheckpoint, rkyv::rancor::Error>(&encoded).unwrap();
        assert_eq!(restored.version, GRAPH_CHECKPOINT_VERSION);
        assert_eq!(restored.operators["stateful"], vec![42; 4_096]);

        let error = OperatorGraph::serialize_checkpoint_bounded(
            &checkpoint,
            u64::try_from(encoded.len() - 1).unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("byte budget"), "{error}");
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn restored_replay_seeds_and_holds_output_watermark_through_final_emission() {
        let donor_processed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut donor = OperatorGraph::new(laminar_sql::create_session_context());
        donor.push_test_node(
            "replay",
            Box::new(RestoredReplayWatermarkProbe {
                replay_watermark: Some(42),
                processed: donor_processed,
            }),
        );
        let checkpoint = donor.snapshot_state().unwrap().unwrap();
        let encoded = OperatorGraph::serialize_checkpoint_bounded(&checkpoint, u64::MAX).unwrap();

        let processed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut target = OperatorGraph::new(laminar_sql::create_session_context());
        target.push_test_node(
            "replay",
            Box::new(RestoredReplayWatermarkProbe {
                replay_watermark: None,
                processed: Arc::clone(&processed),
            }),
        );
        let (mut restored, count) = target.restore_from_bytes(&encoded).unwrap();
        assert_eq!(count, 1);
        assert_eq!(restored.output_watermarks[0], 42);

        let mut results = FxHashMap::default();
        restored
            .execute_single_operator(0, 100, &mut results)
            .await
            .unwrap();
        assert!(processed.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            restored.output_watermarks[0], 42,
            "the replay-only emission cycle must not advance past its restored watermark"
        );

        restored
            .execute_single_operator(0, 100, &mut results)
            .await
            .unwrap();
        assert_eq!(
            restored.output_watermarks[0], 100,
            "the next input-accepting cycle may advance after replay drains"
        );
    }

    #[test]
    fn whole_graph_restore_rejects_missing_operator_before_mutation() {
        let restores = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
        graph.allocate_node(GraphNode {
            name: Arc::from("present"),
            operator: Box::new(RestoreProbe(Arc::clone(&restores))),
            input_port_count: 1,
            output_routes: Vec::new(),
            removed: false,
        });
        let mut operators = OperatorStateMap::new();
        operators.insert("present".into(), vec![1]);
        operators.insert("missing".into(), vec![2]);

        let error = graph
            .restore_state(&GraphCheckpoint {
                version: GRAPH_CHECKPOINT_VERSION,
                operators,
            })
            .err()
            .expect("missing operator must fail");

        assert!(error.to_string().contains("missing operator(s): missing"));
        assert_eq!(restores.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn whole_graph_restore_closes_before_first_execution_cycle() {
        let restores = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
        graph.allocate_node(GraphNode {
            name: Arc::from("present"),
            operator: Box::new(RestoreProbe(Arc::clone(&restores))),
            input_port_count: 1,
            output_routes: Vec::new(),
            removed: false,
        });
        graph
            .execute_cycle(&FxHashMap::default(), i64::MIN, None)
            .await
            .unwrap();
        let operators = [("present".to_string(), vec![1])].into_iter().collect();

        let error = graph
            .restore_state(&GraphCheckpoint {
                version: GRAPH_CHECKPOINT_VERSION,
                operators,
            })
            .err()
            .expect("restore after execution must fail");

        assert!(error
            .to_string()
            .contains("before the first execution cycle"));
        assert_eq!(restores.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn late_restore_failure_consumes_and_drops_partial_graph() {
        let restores = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
        for (name, fail) in [("first", false), ("second", true)] {
            graph.allocate_node(GraphNode {
                name: Arc::from(name),
                operator: Box::new(RestoreFailureProbe {
                    restores: Arc::clone(&restores),
                    drops: Arc::clone(&drops),
                    fail,
                }),
                input_port_count: 1,
                output_routes: Vec::new(),
                removed: false,
            });
        }
        let operators = [
            ("first".to_string(), vec![1]),
            ("second".to_string(), vec![2]),
        ]
        .into_iter()
        .collect();

        let error = graph
            .restore_state(&GraphCheckpoint {
                version: GRAPH_CHECKPOINT_VERSION,
                operators,
            })
            .err()
            .expect("late restore fault must fail the graph");

        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.requires_pipeline_recovery());
        assert!(error.to_string().contains("second"), "{error}");
        assert_eq!(restores.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[test]
    fn stateless_operator_rejects_unexpected_checkpoint_state() {
        let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
        graph.allocate_node(GraphNode {
            name: Arc::from("source"),
            operator: Box::new(SourcePassthrough),
            input_port_count: 1,
            output_routes: Vec::new(),
            removed: false,
        });
        let operators = [("source".to_string(), vec![1])].into_iter().collect();

        let error = graph
            .restore_state(&GraphCheckpoint {
                version: GRAPH_CHECKPOINT_VERSION,
                operators,
            })
            .err()
            .expect("stateless operator state must be rejected");

        assert!(error
            .to_string()
            .contains("does not accept checkpoint state"));
        assert!(error.requires_pipeline_recovery());
    }

    /// Records the batches handed to `stage_checkpointed_shuffle`.
    #[cfg(feature = "cluster")]
    struct RecordingOperator(Arc<parking_lot::Mutex<Vec<RetainedBatch>>>);

    #[cfg(feature = "cluster")]
    #[async_trait]
    impl GraphOperator for RecordingOperator {
        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }
        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }
        fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
            Ok(())
        }
        fn stage_checkpointed_shuffle(
            &mut self,
            _stage: &str,
            batch: RetainedBatch,
            _watermark: i64,
        ) -> Result<(), DbError> {
            self.0.lock().push(batch);
            Ok(())
        }
    }

    #[cfg(feature = "cluster")]
    struct RehydrationApplyOperator {
        applied: Arc<parking_lot::Mutex<Vec<u32>>>,
        failure: Option<&'static str>,
    }

    #[cfg(feature = "cluster")]
    #[async_trait]
    impl GraphOperator for RehydrationApplyOperator {
        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
            Ok(())
        }

        fn apply_vnode_chain(
            &mut self,
            vnode: u32,
            _base: &[u8],
            _deltas: &[&[u8]],
        ) -> Result<(), DbError> {
            if let Some(message) = self.failure {
                return Err(DbError::Pipeline(message.into()));
            }
            self.applied.lock().push(vnode);
            Ok(())
        }
    }

    #[cfg(feature = "cluster")]
    fn encoded_vnode_partial(partial: &crate::vnode_partial::VnodePartial) -> bytes::Bytes {
        bytes::Bytes::from(partial.encode().expect("encode test vnode partial"))
    }

    #[cfg(feature = "cluster")]
    async fn rehydration_test_graph(
        chain: Vec<bytes::Bytes>,
    ) -> (OperatorGraph, Arc<laminar_core::state::VnodeRegistry>) {
        use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
        use laminar_core::state::{NodeId, VnodeRegistry};

        let self_id = NodeId(1);
        let registry = Arc::new(VnodeRegistry::single_owner(1, self_id));
        registry.mark_restoring(&[0]);
        let receiver = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(1))
                .await
                .expect("bind test shuffle receiver"),
        );
        let sender = Arc::new(ShuffleSender::new(1, uuid::Uuid::from_u128(1)));
        let process_deadline = Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
            std::time::Duration::from_secs(60),
        ));
        receiver
            .install_process_lease_deadline(Arc::clone(&process_deadline))
            .unwrap();
        sender
            .install_process_lease_deadline(process_deadline)
            .unwrap();
        let fence = laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
            registry.assignment_version(),
            &[self_id.0],
            vec![laminar_core::checkpoint::CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: uuid::Uuid::from_u128(1),
            }],
        )
        .unwrap();
        receiver
            .install_assignment_fence(&fence, &[self_id.0])
            .unwrap();
        sender
            .install_assignment_fence(&fence, &[self_id.0])
            .unwrap();

        let mut graph = test_graph();
        graph.set_cluster_shuffle(crate::operator::sql_query::ClusterShuffleConfig {
            registry: Arc::clone(&registry),
            sender,
            receiver,
            self_id,
        });
        #[allow(clippy::disallowed_types)] // matches the public rehydration-handle shape
        let staged = Arc::new(parking_lot::Mutex::new(std::collections::HashMap::from([
            (0, crate::db::RehydratedVnode { epoch: 7, chain }),
        ])));
        graph.set_rehydration_handle(staged);
        (graph, registry)
    }

    #[cfg(feature = "cluster")]
    struct AlignmentHarness {
        graph: OperatorGraph,
        local_receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
        remote_receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
        remote_sender: laminar_core::shuffle::ShuffleSender,
        fence: laminar_core::checkpoint::CheckpointAssignmentFence,
        recorded: Arc<parking_lot::Mutex<Vec<RetainedBatch>>>,
    }

    #[cfg(feature = "cluster")]
    async fn alignment_harness() -> AlignmentHarness {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
        use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
        use laminar_core::state::{NodeId, VnodeRegistry};

        let registry = Arc::new(VnodeRegistry::new(2));
        registry.set_assignment(vec![NodeId(1), NodeId(2)].into());
        let assignment_version = registry.assignment_version();
        let fence = CheckpointAssignmentFence::from_owner_map(
            assignment_version,
            &[1, 2],
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: uuid::Uuid::from_u128(1),
                },
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::from_u128(2),
                },
            ],
        )
        .unwrap();
        let local_receiver = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(1))
                .await
                .unwrap(),
        );
        let remote_receiver = Arc::new(
            ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(2))
                .await
                .unwrap(),
        );
        let local_sender = ShuffleSender::new(1, uuid::Uuid::from_u128(1));
        local_sender
            .register_peer(2, remote_receiver.local_addr())
            .await;
        let remote_sender = ShuffleSender::new(2, uuid::Uuid::from_u128(2));
        remote_sender
            .register_peer(1, local_receiver.local_addr())
            .await;
        let local_process_deadline =
            Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
                std::time::Duration::from_secs(60),
            ));
        local_receiver
            .install_process_lease_deadline(Arc::clone(&local_process_deadline))
            .unwrap();
        local_sender
            .install_process_lease_deadline(local_process_deadline)
            .unwrap();
        let remote_process_deadline =
            Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
                std::time::Duration::from_secs(60),
            ));
        remote_receiver
            .install_process_lease_deadline(Arc::clone(&remote_process_deadline))
            .unwrap();
        remote_sender
            .install_process_lease_deadline(remote_process_deadline)
            .unwrap();
        local_receiver
            .install_assignment_fence(&fence, &[1, 2])
            .unwrap();
        remote_receiver
            .install_assignment_fence(&fence, &[1, 2])
            .unwrap();
        local_sender
            .install_assignment_fence(&fence, &[1, 2])
            .unwrap();
        remote_sender
            .install_assignment_fence(&fence, &[1, 2])
            .unwrap();

        let recorded = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
        graph.push_test_node("out", Box::new(RecordingOperator(Arc::clone(&recorded))));
        graph.set_cluster_shuffle(crate::operator::sql_query::ClusterShuffleConfig {
            registry,
            sender: Arc::new(local_sender),
            receiver: Arc::clone(&local_receiver),
            self_id: NodeId(1),
        });
        AlignmentHarness {
            graph,
            local_receiver,
            remote_receiver,
            remote_sender,
            fence,
            recorded,
        }
    }

    #[cfg(feature = "cluster")]
    struct ThreeNodeAlignmentHarness {
        graph: OperatorGraph,
        local_receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
        _peer_two_receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
        waiting_peer_receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
        peer_two_sender: laminar_core::shuffle::ShuffleSender,
        peer_three_sender: laminar_core::shuffle::ShuffleSender,
        fence: laminar_core::checkpoint::CheckpointAssignmentFence,
        recorded: Arc<parking_lot::Mutex<Vec<RetainedBatch>>>,
    }

    #[cfg(feature = "cluster")]
    async fn three_node_alignment_harness() -> ThreeNodeAlignmentHarness {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
        use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
        use laminar_core::state::{NodeId, VnodeRegistry};

        let registry = Arc::new(VnodeRegistry::new(3));
        registry.set_assignment(vec![NodeId(1), NodeId(2), NodeId(3)].into());
        let fence = CheckpointAssignmentFence::from_owner_map(
            registry.assignment_version(),
            &[1, 2, 3],
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: uuid::Uuid::from_u128(1),
                },
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::from_u128(2),
                },
                CheckpointParticipant {
                    node_id: 3,
                    boot_incarnation: uuid::Uuid::from_u128(3),
                },
            ],
        )
        .unwrap();
        let local_receiver = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(1))
                .await
                .unwrap(),
        );
        let peer_two_receiver = Arc::new(
            ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(2))
                .await
                .unwrap(),
        );
        let waiting_peer_receiver = Arc::new(
            ShuffleReceiver::bind(3, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(3))
                .await
                .unwrap(),
        );
        let local_process_deadline =
            Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
                std::time::Duration::from_secs(60),
            ));
        local_receiver
            .install_process_lease_deadline(Arc::clone(&local_process_deadline))
            .unwrap();
        let peer_two_process_deadline =
            Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
                std::time::Duration::from_secs(60),
            ));
        peer_two_receiver
            .install_process_lease_deadline(Arc::clone(&peer_two_process_deadline))
            .unwrap();
        let peer_three_process_deadline =
            Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
                std::time::Duration::from_secs(60),
            ));
        waiting_peer_receiver
            .install_process_lease_deadline(Arc::clone(&peer_three_process_deadline))
            .unwrap();
        for receiver in [&local_receiver, &peer_two_receiver, &waiting_peer_receiver] {
            receiver
                .install_assignment_fence(&fence, &[1, 2, 3])
                .unwrap();
        }

        let local_sender = ShuffleSender::new(1, uuid::Uuid::from_u128(1));
        local_sender
            .register_peer(2, peer_two_receiver.local_addr())
            .await;
        local_sender
            .register_peer(3, waiting_peer_receiver.local_addr())
            .await;
        local_sender
            .install_process_lease_deadline(local_process_deadline)
            .unwrap();
        local_sender
            .install_assignment_fence(&fence, &[1, 2, 3])
            .unwrap();
        let peer_two_sender = ShuffleSender::new(2, uuid::Uuid::from_u128(2));
        peer_two_sender
            .register_peer(1, local_receiver.local_addr())
            .await;
        peer_two_sender
            .register_peer(3, waiting_peer_receiver.local_addr())
            .await;
        peer_two_sender
            .install_process_lease_deadline(peer_two_process_deadline)
            .unwrap();
        peer_two_sender
            .install_assignment_fence(&fence, &[1, 2, 3])
            .unwrap();
        let peer_three_sender = ShuffleSender::new(3, uuid::Uuid::from_u128(3));
        peer_three_sender
            .register_peer(1, local_receiver.local_addr())
            .await;
        peer_three_sender
            .register_peer(2, peer_two_receiver.local_addr())
            .await;
        peer_three_sender
            .install_process_lease_deadline(peer_three_process_deadline)
            .unwrap();
        peer_three_sender
            .install_assignment_fence(&fence, &[1, 2, 3])
            .unwrap();

        let recorded = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
        graph.push_test_node("out", Box::new(RecordingOperator(Arc::clone(&recorded))));
        graph.set_cluster_shuffle(crate::operator::sql_query::ClusterShuffleConfig {
            registry,
            sender: Arc::new(local_sender),
            receiver: Arc::clone(&local_receiver),
            self_id: NodeId(1),
        });

        ThreeNodeAlignmentHarness {
            graph,
            local_receiver,
            _peer_two_receiver: peer_two_receiver,
            waiting_peer_receiver,
            peer_two_sender,
            peer_three_sender,
            fence,
            recorded,
        }
    }

    #[cfg(feature = "cluster")]
    async fn stage_peer_two_data_and_barrier(
        harness: &ThreeNodeAlignmentHarness,
        attempt: laminar_core::state::CheckpointAttempt,
    ) -> RecordBatch {
        use laminar_core::checkpoint::CheckpointBarrier;
        use laminar_core::shuffle::ShuffleMessage;

        let batch = test_batch();
        harness
            .peer_two_sender
            .send_to(
                1,
                &ShuffleMessage::checkpointed("out".into(), 0, batch.clone()),
            )
            .await
            .unwrap();
        harness
            .peer_two_sender
            .fan_out_barrier(
                &[1, 3],
                CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
                &harness.fence,
            )
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let _ = harness
                .local_receiver
                .drain_checkpointed_data_for("__alignment_probe");
            let barriers = harness.local_receiver.drain_staged_barriers();
            if !barriers.is_empty() {
                for barrier in barriers {
                    harness.local_receiver.stash_barrier(barrier);
                }
                return batch;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "peer data and barrier did not reach the holdover"
            );
            tokio::task::yield_now().await;
        }
    }

    #[cfg(feature = "cluster")]
    async fn stage_peer_two_data_barrier_data(
        harness: &ThreeNodeAlignmentHarness,
        attempt: laminar_core::state::CheckpointAttempt,
    ) -> (RecordBatch, RecordBatch) {
        use laminar_core::checkpoint::CheckpointBarrier;
        use laminar_core::shuffle::ShuffleMessage;

        let before_barrier = test_batch();
        let after_barrier = RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(StringArray::from(vec!["MSFT", "NVDA"])),
                Arc::new(Float64Array::from(vec![420.0, 125.0])),
                Arc::new(Int64Array::from(vec![3000, 4000])),
            ],
        )
        .unwrap();
        harness
            .peer_two_sender
            .send_to(
                1,
                &ShuffleMessage::checkpointed("out".into(), 0, before_barrier.clone()),
            )
            .await
            .unwrap();
        harness
            .peer_two_sender
            .fan_out_barrier(
                &[1, 3],
                CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
                &harness.fence,
            )
            .await
            .unwrap();
        harness
            .peer_two_sender
            .send_to(
                1,
                &ShuffleMessage::checkpointed("out".into(), 0, after_barrier.clone()),
            )
            .await
            .unwrap();

        // Reproduce the normal drainer splitting a queued data/barrier/data sequence: the first
        // batch and barrier enter holdovers, while the post-barrier batch remains on the live queue.
        let stage_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let _ = harness
                .local_receiver
                .drain_checkpointed_data_for("__alignment_probe");
            let barriers = harness.local_receiver.drain_staged_barriers();
            if !barriers.is_empty() {
                for barrier in barriers {
                    harness.local_receiver.stash_barrier(barrier);
                }
                break;
            }
            assert!(
                tokio::time::Instant::now() < stage_deadline,
                "remote barrier did not reach the staged holdover"
            );
            tokio::task::yield_now().await;
        }
        (before_barrier, after_barrier)
    }

    #[cfg(feature = "cluster")]
    async fn alignment_abort_controller(
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        attempt: laminar_core::state::CheckpointAttempt,
        durable: bool,
    ) -> Arc<laminar_core::cluster::control::ClusterController> {
        alignment_abort_controller_with_announcement(fence, attempt, durable, true).await
    }

    #[cfg(feature = "cluster")]
    async fn alignment_abort_controller_with_announcement(
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        attempt: laminar_core::state::CheckpointAttempt,
        durable: bool,
        announce: bool,
    ) -> Arc<laminar_core::cluster::control::ClusterController> {
        use laminar_core::checkpoint_decision::CheckpointVerdict;
        use laminar_core::cluster::control::{
            BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, LeaderLeaseOwner,
            LeaderLeaseStore, LeaseDeadline, LeaseOutcome, Phase, ANNOUNCEMENT_KEY,
        };
        use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};

        let node_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(node_id));
        let kv_trait: Arc<dyn ClusterKv> = kv.clone();
        let info = |id| NodeInfo {
            id: NodeId(id),
            name: format!("node-{id}"),
            rpc_address: String::new(),
            raft_address: String::new(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        };
        let (_members_tx, members_rx) =
            tokio::sync::watch::channel(vec![info(1), info(2), info(3)]);
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            node_id,
            Arc::clone(&kv_trait),
            kv_trait,
            None,
            members_rx,
            uuid::Uuid::from_u128(1),
        ));
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
                std::time::Duration::from_secs(60),
            )))
            .unwrap();
        controller.set_active(true);
        controller.publish_checkpoint_assignment_fence(Some(fence.clone()));

        let backing: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let authority = Arc::new(LeaderLeaseStore::new(backing, 1_000));
        let owner = LeaderLeaseOwner {
            node: node_id,
            boot: uuid::Uuid::from_u128(1),
            process_term: 1,
        };
        let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap()
        else {
            panic!("empty alignment authority must grant leadership");
        };
        let proof = lease.proof();
        if durable {
            authority
                .record_cluster_outcome(
                    &proof,
                    attempt.epoch,
                    attempt.checkpoint_id,
                    fence.clone(),
                    CheckpointVerdict::Abort,
                    None,
                )
                .await
                .unwrap();
        }
        controller.set_leader_lease_store(authority);
        if announce {
            kv.seed(
                node_id,
                ANNOUNCEMENT_KEY,
                serde_json::to_string(&BarrierAnnouncement {
                    epoch: attempt.epoch,
                    checkpoint_id: attempt.checkpoint_id,
                    assignment_fence: Some(fence.clone()),
                    leader_proof: Some(proof),
                    phase: Phase::Abort,
                    flags: 0,
                })
                .unwrap(),
            );
        }
        controller
    }

    /// A peer ships a row + its exact-attempt barrier; alignment retains the row as channel state
    /// before completing the certified distributed cut.
    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn align_shuffle_barriers_retains_peer_rows_then_aligns_exact_attempt() {
        use laminar_core::checkpoint::CheckpointBarrier;
        use laminar_core::shuffle::ShuffleMessage;
        use laminar_core::state::CheckpointAttempt;

        let mut harness = alignment_harness().await;
        let attempt = CheckpointAttempt::new(70, 70);

        let batch = test_batch();
        harness
            .remote_sender
            .send_to(
                1,
                &ShuffleMessage::checkpointed("out".into(), 0, batch.clone()),
            )
            .await
            .unwrap();
        harness
            .remote_sender
            .fan_out_barrier(
                &[1],
                CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
                &harness.fence,
            )
            .await
            .unwrap();

        harness
            .graph
            .align_shuffle_barriers(
                attempt,
                0,
                &harness.fence,
                tokio::time::Instant::now() + std::time::Duration::from_secs(2),
                None,
            )
            .await
            .unwrap();

        let received = harness.remote_receiver.recv().await.unwrap();
        assert_eq!(received.peer(), 1);
        assert_eq!(received.assignment_digest(), Some(harness.fence.digest()));
        assert!(matches!(
            received.message(),
            ShuffleMessage::Barrier(barrier)
                if barrier.epoch == attempt.epoch
                    && barrier.checkpoint_id == attempt.checkpoint_id
        ));

        let got = harness.recorded.lock();
        assert_eq!(
            got.len(),
            1,
            "peer's pre-barrier row retained by the operator"
        );
        assert_eq!(got[0].num_rows(), batch.num_rows());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shuffle_scope_cancellation_preserves_holdover_for_the_next_attempt() {
        use laminar_core::checkpoint::CheckpointBarrier;
        use laminar_core::state::CheckpointAttempt;

        let mut harness = three_node_alignment_harness().await;
        let cancelled = CheckpointAttempt::new(70, 70);
        let retained = stage_peer_two_data_and_barrier(&harness, cancelled).await;
        let sender = Arc::clone(
            &harness
                .graph
                .cluster_shuffle_config()
                .expect("cluster shuffle")
                .sender,
        );
        let live_peer_three = harness.waiting_peer_receiver.local_addr();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        sender
            .register_peer(3, listener.local_addr().unwrap())
            .await;
        let accepted = Arc::new(tokio::sync::Notify::new());
        let stalled_peer = {
            let accepted = Arc::clone(&accepted);
            tokio::spawn(async move {
                let (_socket, _) = listener.accept().await.unwrap();
                accepted.notify_one();
                std::future::pending::<()>().await;
            })
        };
        let outcome = {
            let alignment = harness.graph.align_shuffle_barriers(
                cancelled,
                0,
                &harness.fence,
                tokio::time::Instant::now() + std::time::Duration::from_secs(2),
                None,
            );
            tokio::pin!(alignment);
            tokio::select! {
                () = accepted.notified() => {}
                result = &mut alignment => panic!("alignment completed before scope cancellation: {result:?}"),
            }

            sender.suspend_assignment_fence();
            tokio::time::timeout(std::time::Duration::from_secs(1), &mut alignment)
                .await
                .expect("scope cancellation did not release barrier fan-out")
                .unwrap()
        };
        assert_eq!(
            outcome,
            ShuffleAlignmentOutcome::ScopeCancelledBeforeStaging
        );
        assert!(
            harness.recorded.lock().is_empty(),
            "cancelled alignment staged checkpoint holdover"
        );

        harness
            .local_receiver
            .retire_checkpoint_barriers(cancelled, harness.fence.digest())
            .unwrap();
        sender.register_peer(3, live_peer_three).await;
        assert!(sender
            .install_assignment_fence(&harness.fence, &[1, 2, 3])
            .unwrap());
        let successor = CheckpointAttempt::new(71, 71);
        harness
            .peer_two_sender
            .fan_out_barrier(
                &[1, 3],
                CheckpointBarrier::new(successor.checkpoint_id, successor.epoch),
                &harness.fence,
            )
            .await
            .unwrap();
        harness
            .peer_three_sender
            .fan_out_barrier(
                &[1, 2],
                CheckpointBarrier::new(successor.checkpoint_id, successor.epoch),
                &harness.fence,
            )
            .await
            .unwrap();

        assert_eq!(
            harness
                .graph
                .align_shuffle_barriers(
                    successor,
                    0,
                    &harness.fence,
                    tokio::time::Instant::now() + std::time::Duration::from_secs(2),
                    None,
                )
                .await
                .unwrap(),
            ShuffleAlignmentOutcome::Aligned
        );
        let recorded = harness.recorded.lock();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].num_rows(), retained.num_rows());
        stalled_peer.abort();
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn receiver_scope_suspension_preserves_holdover_before_graph_staging() {
        use laminar_core::state::CheckpointAttempt;

        let mut harness = three_node_alignment_harness().await;
        let cancelled = CheckpointAttempt::new(70, 70);
        let retained = stage_peer_two_data_and_barrier(&harness, cancelled).await;
        harness.local_receiver.suspend_assignment_fence();

        let outcome = harness
            .graph
            .align_shuffle_barriers(
                cancelled,
                0,
                &harness.fence,
                tokio::time::Instant::now() + std::time::Duration::from_secs(2),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            outcome,
            ShuffleAlignmentOutcome::ScopeCancelledBeforeStaging
        );
        assert!(harness.recorded.lock().is_empty());

        assert!(harness
            .local_receiver
            .install_assignment_fence(&harness.fence, &[1, 2, 3])
            .unwrap());
        harness
            .local_receiver
            .retire_checkpoint_barriers(cancelled, harness.fence.digest())
            .unwrap();
        let preserved = harness
            .local_receiver
            .drain_checkpointed_holdover()
            .unwrap();
        assert_eq!(preserved.len(), 1);
        assert_eq!(preserved[0].0, "out");
        assert_eq!(preserved[0].1.batch().num_rows(), retained.num_rows());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shuffle_alignment_rejects_staged_data_barrier_data_sequence() {
        use laminar_core::state::CheckpointAttempt;

        let mut harness = three_node_alignment_harness().await;
        let attempt = CheckpointAttempt::new(70, 70);
        let (before_barrier, _after_barrier) =
            stage_peer_two_data_barrier_data(&harness, attempt).await;

        let error = harness
            .graph
            .align_shuffle_barriers(
                attempt,
                0,
                &harness.fence,
                tokio::time::Instant::now() + std::time::Duration::from_secs(2),
                None,
            )
            .await
            .expect_err("data behind an observed peer barrier must fail the checkpoint");
        assert!(error.to_string().contains("checkpoint barrier"), "{error}");
        assert!(
            error.requires_pipeline_recovery(),
            "destructive alignment failure must rewind the pipeline"
        );
        let retained = harness.recorded.lock();
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].num_rows(), before_barrier.num_rows());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shuffle_alignment_retains_resumed_peer_data_on_durable_abort() {
        use laminar_core::state::CheckpointAttempt;

        let mut harness = three_node_alignment_harness().await;
        let attempt = CheckpointAttempt::new(80, 80);
        let (before_barrier, after_barrier) =
            stage_peer_two_data_barrier_data(&harness, attempt).await;
        let controller = alignment_abort_controller(&harness.fence, attempt, true).await;

        let outcome = harness
            .graph
            .align_shuffle_barriers(
                attempt,
                0,
                &harness.fence,
                tokio::time::Instant::now() + std::time::Duration::from_secs(2),
                Some(controller.as_ref()),
            )
            .await
            .expect("an exact durable Abort must end pre-capture alignment cleanly");

        assert_eq!(outcome, ShuffleAlignmentOutcome::Aborted);
        let mut retained: Vec<_> = harness
            .recorded
            .lock()
            .iter()
            .map(|batch| batch.batch().clone())
            .collect();
        assert!(
            matches!(retained.len(), 1 | 2),
            "the pre-barrier batch must be staged before Abort"
        );
        if retained.len() == 1 {
            let receiver_owned = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    let batches = harness.local_receiver.drain_checkpointed_data_for("out");
                    if !batches.is_empty() {
                        break batches;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("post-barrier batch remained in flight after Abort");
            retained.extend(
                receiver_owned
                    .into_iter()
                    .map(|batch| batch.batch().clone()),
            );
        }
        assert_eq!(
            retained.len(),
            2,
            "the graph and receiver must jointly own each batch exactly once after Abort"
        );
        assert_eq!(retained[0], before_barrier);
        assert_eq!(retained[1], after_barrier);
        assert!(
            harness
                .local_receiver
                .drain_checkpointed_data_for("out")
                .is_empty(),
            "post-barrier batch was duplicated in receiver ownership"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shuffle_alignment_audits_durable_abort_when_announcement_is_lost() {
        use laminar_core::state::CheckpointAttempt;

        let mut harness = three_node_alignment_harness().await;
        let attempt = CheckpointAttempt::new(90, 90);
        let retained = stage_peer_two_data_and_barrier(&harness, attempt).await;
        let controller =
            alignment_abort_controller_with_announcement(&harness.fence, attempt, true, false)
                .await;

        let outcome = harness
            .graph
            .align_shuffle_barriers(
                attempt,
                0,
                &harness.fence,
                tokio::time::Instant::now() + std::time::Duration::from_secs(2),
                Some(controller.as_ref()),
            )
            .await
            .expect("the periodic authority audit must observe an Abort without gossip");

        assert_eq!(outcome, ShuffleAlignmentOutcome::Aborted);
        let recorded = harness.recorded.lock();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].num_rows(), retained.num_rows());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn shuffle_alignment_does_not_trust_abort_hint_without_durable_outcome() {
        use laminar_core::state::CheckpointAttempt;

        let harness = three_node_alignment_harness().await;
        let attempt = CheckpointAttempt::new(90, 90);
        let controller = alignment_abort_controller(&harness.fence, attempt, false).await;

        let hint = OperatorGraph::wait_for_shuffle_alignment_terminal_hint(
            Some(controller.as_ref()),
            attempt,
            None,
            tokio::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .expect("Abort announcement must wake alignment");
        assert_eq!(hint.epoch, attempt.epoch);
        assert_eq!(hint.checkpoint_id, attempt.checkpoint_id);
        assert_eq!(hint.phase, laminar_core::cluster::control::Phase::Abort);
        assert_eq!(
            OperatorGraph::audit_shuffle_alignment_settlement(
                Some(controller.as_ref()),
                attempt,
                &harness.fence,
            )
            .await
            .unwrap(),
            None
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn shuffle_alignment_rejects_abort_with_a_different_assignment_certificate() {
        use laminar_core::checkpoint::CheckpointAssignmentFence;
        use laminar_core::state::CheckpointAttempt;

        let harness = three_node_alignment_harness().await;
        let attempt = CheckpointAttempt::new(90, 90);
        let other_fence = CheckpointAssignmentFence::from_owner_map(
            harness.fence.assignment_version,
            &[1, 3, 2],
            harness.fence.participants.clone(),
        )
        .unwrap();
        let controller = alignment_abort_controller(&other_fence, attempt, true).await;

        let error = OperatorGraph::audit_shuffle_alignment_settlement(
            Some(controller.as_ref()),
            attempt,
            &harness.fence,
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("different assignment certificate"),
            "{error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shuffle_sender_rejects_wrong_epoch_for_same_checkpoint_id() {
        use laminar_core::checkpoint::CheckpointBarrier;
        use laminar_core::state::CheckpointAttempt;

        let harness = alignment_harness().await;
        let expected = CheckpointAttempt::new(70, 70);
        let error = harness
            .remote_sender
            .fan_out_barrier(
                &[1],
                CheckpointBarrier::new(expected.checkpoint_id, 8),
                &harness.fence,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            error.to_string().contains("canonical checkpoint ID"),
            "{error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn shuffle_attempt_comparison_rejects_all_conflicting_orders() {
        use laminar_core::state::CheckpointAttempt;

        let expected = CheckpointAttempt::new(70, 70);
        for observed in [
            CheckpointAttempt::new(69, 71),
            CheckpointAttempt::new(71, 69),
            CheckpointAttempt::new(70, 69),
            CheckpointAttempt::new(70, 71),
            CheckpointAttempt::new(69, 70),
            CheckpointAttempt::new(71, 70),
        ] {
            assert!(
                OperatorGraph::compare_shuffle_attempts(expected, observed).is_err(),
                "mixed attempt order must fail: {observed:?}"
            );
        }
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn shuffle_alignment_rejects_newer_durable_terminal_without_announcement() {
        use laminar_core::state::CheckpointAttempt;

        let attempt = CheckpointAttempt::new(70, 70);
        let newer = CheckpointAttempt::new(71, 71);
        let harness = three_node_alignment_harness().await;
        let controller =
            alignment_abort_controller_with_announcement(&harness.fence, newer, true, false).await;
        let error = OperatorGraph::audit_shuffle_alignment_settlement(
            Some(controller.as_ref()),
            attempt,
            &harness.fence,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("superseded by durable terminal"),
            "{error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shuffle_alignment_rejects_wrong_assignment_digest() {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointBarrier};
        use laminar_core::state::CheckpointAttempt;

        let harness = alignment_harness().await;
        let wrong_fence = CheckpointAssignmentFence::from_owner_map(
            harness.fence.assignment_version,
            &[2, 1],
            harness.fence.participants.clone(),
        )
        .unwrap();
        let attempt = CheckpointAttempt::new(70, 70);
        let error = harness
            .remote_sender
            .fan_out_barrier(
                &[1],
                CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
                &wrong_fence,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("assignment roster"), "{error}");
        assert!(harness.recorded.lock().is_empty());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shuffle_alignment_rejects_changed_local_assignment_scope() {
        use laminar_core::checkpoint::CheckpointAssignmentFence;
        use laminar_core::state::CheckpointAttempt;

        let mut harness = alignment_harness().await;
        let next = CheckpointAssignmentFence::from_owner_map(
            harness.fence.assignment_version + 1,
            &[1, 2],
            harness.fence.participants.clone(),
        )
        .unwrap();
        let cfg = harness.graph.cluster_shuffle_config().unwrap();
        cfg.sender.install_assignment_fence(&next, &[1, 2]).unwrap();
        cfg.receiver
            .install_assignment_fence(&next, &[1, 2])
            .unwrap();
        let error = harness
            .graph
            .align_shuffle_barriers(
                CheckpointAttempt::new(70, 70),
                0,
                &harness.fence,
                tokio::time::Instant::now() + std::time::Duration::from_secs(2),
                None,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("assignment differs"), "{error}");
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_transition_discards_staged_pre_recovery_barrier() {
        use laminar_core::checkpoint::CheckpointBarrier;
        use laminar_core::state::CheckpointAttempt;

        let harness = alignment_harness().await;
        let attempt = CheckpointAttempt::new(70, 70);
        harness
            .remote_sender
            .fan_out_barrier(
                &[1],
                CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
                &harness.fence,
            )
            .await
            .unwrap();
        let old = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            harness.local_receiver.recv(),
        )
        .await
        .unwrap()
        .unwrap();
        harness.local_receiver.stash_barrier(old);
        harness.local_receiver.set_recovery_gen(1);
        harness.remote_receiver.set_recovery_gen(1);
        harness
            .graph
            .cluster_shuffle_config()
            .unwrap()
            .sender
            .set_recovery_gen(1);

        assert!(harness.local_receiver.drain_staged_barriers().is_empty());
        assert!(harness.remote_receiver.drain_staged_barriers().is_empty());
        assert!(harness.recorded.lock().is_empty());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shuffle_alignment_fails_closed_on_unknown_stage() {
        use laminar_core::checkpoint::CheckpointBarrier;
        use laminar_core::shuffle::ShuffleMessage;
        use laminar_core::state::CheckpointAttempt;

        let mut harness = alignment_harness().await;
        let attempt = CheckpointAttempt::new(70, 70);
        harness
            .remote_sender
            .send_to(
                1,
                &ShuffleMessage::checkpointed("missing".into(), 0, test_batch()),
            )
            .await
            .unwrap();
        harness
            .remote_sender
            .fan_out_barrier(
                &[1],
                CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
                &harness.fence,
            )
            .await
            .unwrap();
        let error = harness
            .graph
            .align_shuffle_barriers(
                attempt,
                0,
                &harness.fence,
                tokio::time::Instant::now() + std::time::Duration::from_secs(2),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("unknown or removed stage"),
            "{error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shuffle_alignment_uses_supplied_absolute_deadline() {
        use laminar_core::state::CheckpointAttempt;

        let mut harness = alignment_harness().await;
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            harness.graph.align_shuffle_barriers(
                CheckpointAttempt::new(70, 70),
                0,
                &harness.fence,
                tokio::time::Instant::now() + std::time::Duration::from_millis(30),
                None,
            ),
        )
        .await
        .expect("alignment ignored its supplied deadline")
        .unwrap_err();
        assert!(error.to_string().contains("absolute deadline"), "{error}");
    }

    #[test]
    fn test_source_passthrough() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut op = SourcePassthrough;
            let batch = test_batch();
            let result = op.process(&[vec![batch.clone()]], &[0]).await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].num_rows(), 2);
        });
    }

    #[test]
    fn test_graph_construction() {
        let ctx = laminar_sql::create_session_context();
        let mut graph = OperatorGraph::new(ctx);

        graph.add_query(
            "q1".to_string(),
            "SELECT symbol, price FROM trades WHERE price > 100".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        assert_eq!(graph.nodes.len(), 2); // source "trades" + query "q1"
        assert_eq!(graph.edges.len(), 1); // trades → q1
        assert!(graph.source_map.contains_key("trades"));
        assert!(graph.output_map.contains_key("q1"));
    }

    #[test]
    fn test_cascading_queries() {
        let ctx = laminar_sql::create_session_context();
        let mut graph = OperatorGraph::new(ctx);

        graph.add_query(
            "q1".to_string(),
            "SELECT symbol, price FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.add_query(
            "q2".to_string(),
            "SELECT symbol FROM q1 WHERE price > 100".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        // source "trades" + query "q1" + query "q2" = 3 nodes
        assert_eq!(graph.nodes.len(), 3);
        // trades → q1, q1 → q2 = 2 edges
        assert_eq!(graph.edges.len(), 2);
        assert!(graph.depends_on_stream.contains(&2)); // q2 depends on q1
    }

    #[test]
    fn test_topo_order() {
        let ctx = laminar_sql::create_session_context();
        let mut graph = OperatorGraph::new(ctx);

        // Add in reverse dependency order
        graph.add_query(
            "q2".to_string(),
            "SELECT * FROM q1".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.add_query(
            "q1".to_string(),
            "SELECT * FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        graph.compute_topo_order();

        // Find positions in topo order
        let q1_pos = graph
            .topo_order
            .iter()
            .position(|&id| &*graph.nodes[id].name == "q1");
        let q2_pos = graph
            .topo_order
            .iter()
            .position(|&id| &*graph.nodes[id].name == "q2");

        // q1 should appear before q2 (but note: q2 was added first and created
        // a source node "q1" which gets the first edge; the real q1 query node
        // doesn't have that edge. This test mainly verifies no panics.)
        assert!(q1_pos.is_some());
        assert!(q2_pos.is_some());
    }

    #[test]
    fn test_remove_query() {
        let ctx = laminar_sql::create_session_context();
        let mut graph = OperatorGraph::new(ctx);

        graph.add_query(
            "q1".to_string(),
            "SELECT * FROM trades".to_string(),
            None,
            None,
            None,
            None,
            true,
        );
        assert!(graph.output_map.contains_key("q1"));
        let original_node = graph.output_map["q1"];
        graph.ensure_live_provider("q1", &test_schema());
        let temporal_config = TemporalJoinTranslatorConfig {
            stream_table: "trades".to_string(),
            table_name: "versions".to_string(),
            stream_key_column: "symbol".to_string(),
            table_key_column: "symbol".to_string(),
            stream_time_column: "ts".to_string(),
            table_version_column: "valid_from".to_string(),
            semantics: "event_time".to_string(),
            join_type: "inner".to_string(),
        };
        graph
            .temporal_configs
            .push(("q1".to_string(), temporal_config.clone()));

        graph.remove_query("q1");
        assert!(!graph.output_map.contains_key("q1"));
        assert!(graph.nodes[1].removed); // node 0 = source, node 1 = q1
        assert!(!graph.incremental_tables.contains("q1"));
        assert!(graph.temporal_configs.is_empty());
        assert!(!graph.live_handles.contains_key("q1"));

        graph.add_query(
            "q1".to_string(),
            "SELECT * FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        let replacement_node = graph.output_map["q1"];
        assert_eq!(replacement_node, original_node);
        assert_eq!(
            graph
                .nodes
                .iter()
                .filter(|node| !node.removed && &*node.name == "q1")
                .count(),
            1
        );
        assert!(!graph.incremental_tables.contains("q1"));
        graph.ensure_live_provider("q1", &test_schema());
        assert!(graph.live_handles.contains_key("q1"));

        graph.incremental_tables.insert("metadata_only".to_string());
        graph
            .temporal_configs
            .push(("metadata_only".to_string(), temporal_config));
        graph.ensure_live_provider("metadata_only", &test_schema());
        assert!(!graph.output_map.contains_key("metadata_only"));
        graph.remove_query("metadata_only");
        assert!(!graph.incremental_tables.contains("metadata_only"));
        assert!(graph
            .temporal_configs
            .iter()
            .all(|(query_name, _)| query_name != "metadata_only"));
        assert!(!graph.live_handles.contains_key("metadata_only"));
    }

    #[test]
    fn rejected_control_add_removes_all_query_artifacts() {
        let ctx = laminar_sql::create_session_context();
        let mut graph = OperatorGraph::new(ctx);
        graph
            .build_errors
            .push(DbError::Pipeline("forced admission rejection".into()));

        let mutation = Arc::new(crate::pipeline::ControlMutation::new());
        let (reply, mut result) = tokio::sync::oneshot::channel();
        let message = crate::pipeline::ControlMsg::add_stream(
            "rejected".to_string(),
            "SELECT * FROM events".to_string(),
            None,
            None,
            None,
            None,
            true,
            reply,
            Arc::clone(&mutation),
        );
        crate::pipeline_callback::apply_control_to_graph(&mut graph, message);
        let error = result
            .try_recv()
            .expect("control result must be sent synchronously")
            .unwrap_err();

        assert_eq!(
            mutation.state(),
            crate::pipeline::ControlMutationState::Cancelled
        );
        assert!(matches!(error, DbError::Pipeline(_)));
        assert!(!graph.output_map.contains_key("rejected"));
        assert!(!graph.incremental_tables.contains("rejected"));
        assert!(!graph.live_handles.contains_key("rejected"));
        assert!(graph
            .temporal_configs
            .iter()
            .all(|(query_name, _)| query_name != "rejected"));
        let rejected_nodes: FxHashSet<_> = graph
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| &*node.name == "rejected")
            .map(|(id, node)| {
                assert!(node.removed);
                id
            })
            .collect();
        assert!(!rejected_nodes.is_empty());
        assert!(graph
            .edges
            .iter()
            .all(|edge| !rejected_nodes.contains(&edge.source)
                && !rejected_nodes.contains(&edge.target)));
        assert!(graph.nodes.iter().all(|node| node
            .output_routes
            .iter()
            .all(|(target, _)| !rejected_nodes.contains(target))));
    }

    #[test]
    fn repeated_live_control_create_drop_reuses_graph_slots() {
        let ctx = laminar_sql::create_session_context();
        let mut graph = OperatorGraph::new(ctx);

        for _ in 0..128 {
            let create_mutation = Arc::new(crate::pipeline::ControlMutation::new());
            let (create_reply, mut create_result) = tokio::sync::oneshot::channel();
            crate::pipeline_callback::apply_control_to_graph(
                &mut graph,
                crate::pipeline::ControlMsg::add_stream(
                    "churn".to_string(),
                    "SELECT * FROM events".to_string(),
                    None,
                    None,
                    None,
                    None,
                    false,
                    create_reply,
                    Arc::clone(&create_mutation),
                ),
            );
            create_result
                .try_recv()
                .expect("CREATE acknowledgement must be synchronous")
                .unwrap();
            assert_eq!(
                create_mutation.state(),
                crate::pipeline::ControlMutationState::Applied
            );

            let drop_mutation = Arc::new(crate::pipeline::ControlMutation::new());
            let (drop_reply, mut drop_result) = tokio::sync::oneshot::channel();
            crate::pipeline_callback::apply_control_to_graph(
                &mut graph,
                crate::pipeline::ControlMsg::drop_streams(
                    vec!["churn".to_string()],
                    drop_reply,
                    Arc::clone(&drop_mutation),
                ),
            );
            drop_result
                .try_recv()
                .expect("DROP acknowledgement must be synchronous")
                .unwrap();
            assert_eq!(
                drop_mutation.state(),
                crate::pipeline::ControlMutationState::Applied
            );
        }

        assert_eq!(
            graph.nodes.len(),
            2,
            "one source slot plus one reusable query slot"
        );
        assert_eq!(graph.free_node_ids.len(), 1);
        assert!(graph.edges.is_empty());
    }

    #[tokio::test]
    async fn test_execute_cycle_basic() {
        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);
        let mut graph = OperatorGraph::new(ctx);

        graph.add_query(
            "filtered".to_string(),
            "SELECT symbol, price FROM trades WHERE price > 200".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        let batch = test_batch();
        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("trades"), vec![batch]);

        let results = graph
            .execute_cycle(&source_batches, i64::MAX, None)
            .await
            .unwrap();
        assert!(results.contains_key("filtered"));
        let filtered = &results[&Arc::from("filtered") as &Arc<str>];
        // Only GOOG (price=2800) passes the filter
        let total_rows: usize = filtered.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    // --- AI routing ---

    struct PosProvider;

    #[async_trait]
    impl crate::ai::InferenceProvider for PosProvider {
        async fn infer_batch(
            &self,
            request: crate::ai::InferenceRequest,
        ) -> Result<crate::ai::InferenceResponse, crate::ai::ProviderError> {
            Ok(crate::ai::InferenceResponse {
                outputs: crate::ai::InferenceOutputs::Text(vec![
                    "pos".to_string();
                    request.inputs.len()
                ]),
                usage: crate::ai::Usage::ZERO,
            })
        }
        fn name(&self) -> &'static str {
            "pos"
        }
    }

    fn stub_ai_runtime() -> Arc<crate::ai::AiRuntime> {
        use crate::ai::{ModelBackend, ModelEntry, ModelRegistry, Task};
        let mut registry = ModelRegistry::new();
        registry
            .register(ModelEntry {
                id: "m".into(),
                tasks: vec![Task::Classify],
                backend: ModelBackend::Remote {
                    provider: "p".into(),
                    model: "stub-model".into(),
                },
            })
            .unwrap();
        let providers = [(
            "p".to_string(),
            Arc::new(PosProvider) as Arc<dyn crate::ai::InferenceProvider>,
        )];
        Arc::new(crate::ai::AiRuntime::new(
            registry,
            providers,
            None,
            Arc::new(crate::ai::AiResultCache::with_defaults()),
            Arc::new(crate::ai::AiCallLog::with_defaults()),
        ))
    }

    fn docs_batch() -> RecordBatch {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["great quarter"])),
            ],
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ai_routing_enriches_rows() {
        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);
        let mut graph = OperatorGraph::new(ctx);
        graph.set_ai_runtime(stub_ai_runtime(), tokio::runtime::Handle::current());
        graph.register_source_schema("docs".to_string(), docs_batch().schema());

        graph.add_query(
            "labeled".to_string(),
            "SELECT id, ai_classify(text, model => 'm', labels => ARRAY['pos','neg']) AS label \
             FROM docs"
                .to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph
            .take_build_errors()
            .expect("AI query should route cleanly");

        // Cycle 1: the row misses the cache and is handed to the worker.
        let mut sources = FxHashMap::default();
        sources.insert(Arc::from("docs"), vec![docs_batch()]);
        let _ = graph.execute_cycle(&sources, i64::MAX, None).await.unwrap();

        // Let the off-thread worker finish, then drain on a later cycle.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let empty = FxHashMap::default();
        let results = graph.execute_cycle(&empty, i64::MAX, None).await.unwrap();

        let out = &results[&(Arc::from("labeled") as Arc<str>)];
        let rows: usize = out.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1, "the enriched row should be emitted");
        // Output schema is the residual projection: (id, label).
        let batch = out.iter().find(|b| b.num_rows() > 0).unwrap();
        let label = batch
            .column(batch.schema().index_of("label").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(label.value(0), "pos");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ai_routing_unknown_model_fails_at_build() {
        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);
        let mut graph = OperatorGraph::new(ctx);
        graph.set_ai_runtime(stub_ai_runtime(), tokio::runtime::Handle::current());

        graph.add_query(
            "bad".to_string(),
            "SELECT ai_classify(text, model => 'ghost', labels => ARRAY['a']) AS label FROM docs"
                .to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        assert!(
            graph.take_build_errors().is_err(),
            "unknown model must fail"
        );
    }

    /// End-to-end through the real graph: `ai_sentiment` lifts to the AI
    /// operator, the worker scores on Ring 1, and the emitted column is a
    /// numeric `Float64`, not a label.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ai_sentiment_emits_a_double_score() {
        use crate::ai::{
            AiCallLog, AiResultCache, AiRuntime, InferenceOutputs, InferenceProvider,
            InferenceRequest, InferenceResponse, ModelBackend, ModelEntry, ModelRegistry,
            ProviderError, Task, Usage,
        };

        struct ScoreProvider;
        #[async_trait::async_trait]
        impl InferenceProvider for ScoreProvider {
            async fn infer_batch(
                &self,
                req: InferenceRequest,
            ) -> Result<InferenceResponse, ProviderError> {
                // A compliant sentiment model replies with a bare number.
                Ok(InferenceResponse {
                    outputs: InferenceOutputs::Text(vec!["0.8".to_string(); req.inputs.len()]),
                    usage: Usage::ZERO,
                })
            }
            fn name(&self) -> &'static str {
                "score"
            }
        }

        let mut registry = ModelRegistry::new();
        registry
            .register(ModelEntry {
                id: "m".into(),
                tasks: vec![Task::Sentiment],
                backend: ModelBackend::Remote {
                    provider: "p".into(),
                    model: "stub".into(),
                },
            })
            .unwrap();
        let call_log = Arc::new(AiCallLog::with_defaults());
        let runtime = Arc::new(AiRuntime::new(
            registry,
            [(
                "p".to_string(),
                Arc::new(ScoreProvider) as Arc<dyn InferenceProvider>,
            )],
            None,
            Arc::new(AiResultCache::with_defaults()),
            Arc::clone(&call_log),
        ));

        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);
        let mut graph = OperatorGraph::new(ctx);
        graph.set_ai_runtime(runtime, tokio::runtime::Handle::current());
        graph.register_source_schema("docs".to_string(), docs_batch().schema());

        graph.add_query(
            "scored".to_string(),
            "SELECT id, ai_sentiment(text, model => 'm') AS sentiment FROM docs".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph
            .take_build_errors()
            .expect("ai_sentiment should route cleanly");

        let mut sources = FxHashMap::default();
        sources.insert(Arc::from("docs"), vec![docs_batch()]);
        let _ = graph.execute_cycle(&sources, i64::MAX, None).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let results = graph
            .execute_cycle(&FxHashMap::default(), i64::MAX, None)
            .await
            .unwrap();

        let out = &results[&(Arc::from("scored") as Arc<str>)];
        let batch = out.iter().find(|b| b.num_rows() > 0).expect("a scored row");
        let col = batch.column(batch.schema().index_of("sentiment").unwrap());
        let scores = col
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("sentiment is a Float64 score, not a label");
        assert!((scores.value(0) - 0.8).abs() < 1e-9);
        assert_eq!(
            call_log.total_recorded(),
            1,
            "the call is in laminar.ai_calls"
        );
    }

    #[tokio::test]
    async fn test_execute_cycle_empty_source() {
        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);
        let mut graph = OperatorGraph::new(ctx);

        // Register schema so the graph can create empty placeholder tables
        graph.register_source_schema("trades".to_string(), test_schema());

        graph.add_query(
            "q1".to_string(),
            "SELECT * FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        let source_batches = FxHashMap::default();
        let results = graph
            .execute_cycle(&source_batches, i64::MAX, None)
            .await
            .unwrap();
        // No source data → empty results (or no entry)
        let total: usize = results
            .get("q1")
            .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum());
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn test_fan_out() {
        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);
        let mut graph = OperatorGraph::new(ctx);

        graph.add_query(
            "q1".to_string(),
            "SELECT symbol, price FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.add_query(
            "q2".to_string(),
            "SELECT symbol FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        let batch = test_batch();
        let mut source_batches = FxHashMap::default();
        source_batches.insert(Arc::from("trades"), vec![batch]);

        let results = graph
            .execute_cycle(&source_batches, i64::MAX, None)
            .await
            .unwrap();
        assert!(results.contains_key("q1"));
        assert!(results.contains_key("q2"));
    }

    #[test]
    fn test_checkpoint_empty() {
        let ctx = laminar_sql::create_session_context();
        let mut graph = OperatorGraph::new(ctx);
        graph.add_query(
            "q1".to_string(),
            "SELECT * FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        // No state yet → None
        let cp = graph.snapshot_state().unwrap();
        assert!(cp.is_none());
    }

    #[tokio::test]
    async fn test_temporal_filter_checkpoint_restore_through_graph() {
        use laminar_sql::parser::EmitClause;
        // test_batch(): ts is Int64 epoch-ms — AAPL@1000, GOOG@2000.
        let sql = "SELECT * FROM trades WHERE ts > now() - INTERVAL '10' SECOND";
        let mut g1 = test_graph();
        g1.add_query(
            "recent".into(),
            sql.into(),
            Some(EmitClause::Changes),
            None,
            None,
            None,
            false,
        );
        let mut src = FxHashMap::default();
        src.insert(Arc::from("trades"), vec![test_batch()]);
        // Frontier 5000ms: both rows are members (exit 11000/12000) ⇒ +1,+1.
        let r = g1.execute_cycle(&src, 5_000, None).await.unwrap();
        assert_eq!(total_rows(&r, "recent"), 2);

        // Snapshot + restore through the real GraphCheckpoint/rkyv path.
        let cp = g1.snapshot_state().unwrap().expect("buffered state");
        let bytes = OperatorGraph::serialize_checkpoint_bounded(&cp, u64::MAX).unwrap();
        let mut g2 = test_graph();
        g2.add_query(
            "recent".into(),
            sql.into(),
            Some(EmitClause::Changes),
            None,
            None,
            None,
            false,
        );
        let (restored_graph, restored) = g2.restore_from_bytes(&bytes).unwrap();
        let mut g2 = restored_graph;
        assert_eq!(restored, 1);

        // Advancing to 11000ms ages out AAPL@1000 (exit 11000, strict `>`)
        // but not GOOG@2000 (exit 12000): exactly one -1, nothing lost.
        let empty = FxHashMap::default();
        let r = g2.execute_cycle(&empty, 11_000, None).await.unwrap();
        let batches = r.get("recent").expect("recent output");
        let mut wts = Vec::new();
        for b in batches {
            let w = b
                .column(b.schema().index_of("__weight").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let ts = b
                .column(b.schema().index_of("ts").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                wts.push((w.value(i), ts.value(i)));
            }
        }
        assert_eq!(
            wts,
            vec![(-1, 1000)],
            "only AAPL@1000 ages out post-restore"
        );

        // Re-advancing to the same frontier must not double-retract.
        let r = g2.execute_cycle(&empty, 11_000, None).await.unwrap();
        assert_eq!(total_rows(&r, "recent"), 0);
    }

    struct DelayOperator;

    #[async_trait]
    impl GraphOperator for DelayOperator {
        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
            Ok(())
        }
    }

    /// Helper: total row count from result batches.
    fn total_rows(results: &FxHashMap<Arc<str>, Vec<RecordBatch>>, key: &str) -> usize {
        results
            .get(key)
            .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum())
    }

    /// Creates a graph with streaming functions registered and generous budget.
    fn test_graph() -> OperatorGraph {
        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);
        let mut graph = OperatorGraph::new(ctx);
        // Debug builds are slow — use a generous budget for tests.
        graph.set_query_budget_ns(5_000_000_000); // 5 seconds
        graph
    }

    struct AlwaysFailOperator;

    #[async_trait]
    impl GraphOperator for AlwaysFailOperator {
        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Err(DbError::Pipeline("injected operator failure".into()))
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
            Ok(())
        }
    }

    struct TerminalShuffleOperator;

    #[async_trait]
    impl GraphOperator for TerminalShuffleOperator {
        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Err(DbError::ShuffleTerminal(
                "injected permanent routing failure".into(),
            ))
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
            Ok(())
        }
    }

    fn terminal_shuffle_graph(query_budget_ns: u64) -> OperatorGraph {
        let mut graph = test_graph();
        graph.set_query_budget_ns(query_budget_ns);
        graph.set_shared_source_isolation(true, usize::MAX);
        let source = graph.ensure_source_node("trades");
        let terminal = graph
            .place_operator_node("terminal", Box::new(TerminalShuffleOperator), 1)
            .unwrap();
        let healthy = graph
            .place_operator_node("healthy", Box::new(SourcePassthrough), 1)
            .unwrap();
        graph.add_edge(source, terminal, 0);
        graph.add_edge(source, healthy, 0);
        graph.output_map.insert(Arc::from("terminal"), terminal);
        graph.output_map.insert(Arc::from("healthy"), healthy);
        graph.topo_dirty = true;
        graph
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn apply_revoked_vnodes_drains_handle() {
        let mut graph = test_graph();
        let handle = Arc::new(parking_lot::Mutex::new(
            [1u32, 2, 3].into_iter().collect::<FxHashSet<u32>>(),
        ));
        graph.set_revoke_handle(Arc::clone(&handle));
        graph.apply_revoked_vnodes().unwrap();
        assert!(
            handle.lock().is_empty(),
            "the revoke handle is drained after apply_revoked_vnodes",
        );
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn vnode_revoke_failure_faults_and_retains_pending_work() {
        struct RevokeFailureOperator;

        #[async_trait]
        impl GraphOperator for RevokeFailureOperator {
            async fn process(
                &mut self,
                _inputs: &[Vec<RecordBatch>],
                _watermarks: &[i64],
            ) -> Result<Vec<RecordBatch>, DbError> {
                Ok(Vec::new())
            }

            fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
                Ok(None)
            }

            fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
                Ok(())
            }

            fn drop_owned_vnodes(&mut self, _revoked: &FxHashSet<u32>) -> Result<(), DbError> {
                Err(DbError::Pipeline("injected vnode revoke failure".into()))
            }
        }

        let mut graph = test_graph();
        graph.push_test_node("revoke-failure", Box::new(RevokeFailureOperator));
        let handle = Arc::new(parking_lot::Mutex::new(
            [7u32].into_iter().collect::<FxHashSet<u32>>(),
        ));
        graph.set_revoke_handle(Arc::clone(&handle));

        let error = graph.apply_revoked_vnodes().unwrap_err();
        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.to_string().contains("revoke-failure"));
        assert_eq!(
            *handle.lock(),
            [7u32].into_iter().collect::<FxHashSet<u32>>()
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn corrupt_rehydration_chain_faults_and_keeps_vnode_restoring() {
        let (mut graph, registry) =
            rehydration_test_graph(vec![bytes::Bytes::from_static(b"not-rkyv")]).await;

        let error = graph
            .execute_cycle(&FxHashMap::default(), i64::MAX, None)
            .await
            .expect_err("corrupt vnode state must fault the cycle");
        let message = error.to_string();
        assert!(message.contains("[LDB-6051]"), "{message}");
        assert!(message.contains("link 0"), "{message}");
        assert!(message.contains("corrupt"), "{message}");
        assert!(
            registry.is_restoring(0),
            "a corrupt chain must not activate the vnode"
        );
        assert_eq!(graph.last_execution_assignment_version(), None);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn execution_assignment_is_not_published_when_transport_scope_is_stale() {
        let (mut graph, registry) = rehydration_test_graph(Vec::new()).await;
        registry.set_assignment(vec![laminar_core::state::NodeId(1)].into());

        let error = graph
            .execute_cycle(&FxHashMap::default(), i64::MAX, None)
            .await
            .expect_err("stale shuffle transport scope must reject the cycle");

        assert!(matches!(error, DbError::ShuffleNotReady(_)));
        assert_eq!(graph.last_execution_assignment_version(), None);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn rehydration_delta_without_full_base_faults_before_apply() {
        let partial = crate::vnode_partial::VnodePartial {
            operators: Vec::new(),
            base: Some(laminar_core::state::CheckpointAttempt::new(1, 1)),
            deltas: vec![("agg".to_string(), vec![1])],
        };
        let (mut graph, registry) =
            rehydration_test_graph(vec![encoded_vnode_partial(&partial)]).await;
        let applied = Arc::new(parking_lot::Mutex::new(Vec::new()));
        graph.push_test_node(
            "agg",
            Box::new(RehydrationApplyOperator {
                applied: Arc::clone(&applied),
                failure: None,
            }),
        );

        let error = graph
            .execute_cycle(&FxHashMap::default(), i64::MAX, None)
            .await
            .expect_err("a delta-only chain must fault the cycle");
        let message = error.to_string();
        assert!(message.contains("no FULL base"), "{message}");
        assert!(message.contains("agg"), "{message}");
        assert!(applied.lock().is_empty(), "invalid state was applied");
        assert!(registry.is_restoring(0));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn missing_rehydration_operator_faults_before_any_apply() {
        let partial = crate::vnode_partial::VnodePartial {
            operators: vec![
                ("present".to_string(), vec![1]),
                ("ghost".to_string(), vec![2]),
            ],
            base: None,
            deltas: Vec::new(),
        };
        let (mut graph, registry) =
            rehydration_test_graph(vec![encoded_vnode_partial(&partial)]).await;
        let present_applied = Arc::new(parking_lot::Mutex::new(Vec::new()));
        graph.push_test_node(
            "present",
            Box::new(RehydrationApplyOperator {
                applied: Arc::clone(&present_applied),
                failure: None,
            }),
        );

        let error = graph
            .execute_cycle(&FxHashMap::default(), i64::MAX, None)
            .await
            .expect_err("topology drift must fault the cycle");
        let message = error.to_string();
        assert!(message.contains("missing operator"), "{message}");
        assert!(message.contains("ghost"), "{message}");
        assert!(
            present_applied.lock().is_empty(),
            "validation must finish before any operator is mutated"
        );
        assert!(registry.is_restoring(0));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn rehydration_apply_failure_faults_without_activating_vnode() {
        let partial = crate::vnode_partial::VnodePartial {
            operators: vec![
                ("good".to_string(), vec![1]),
                ("broken".to_string(), vec![2]),
            ],
            base: None,
            deltas: Vec::new(),
        };
        let (mut graph, registry) =
            rehydration_test_graph(vec![encoded_vnode_partial(&partial)]).await;
        let good_applied = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let broken_applied = Arc::new(parking_lot::Mutex::new(Vec::new()));
        graph.push_test_node(
            "good",
            Box::new(RehydrationApplyOperator {
                applied: Arc::clone(&good_applied),
                failure: None,
            }),
        );
        graph.push_test_node(
            "broken",
            Box::new(RehydrationApplyOperator {
                applied: Arc::clone(&broken_applied),
                failure: Some("injected vnode apply failure"),
            }),
        );

        let error = graph
            .execute_cycle(&FxHashMap::default(), i64::MAX, None)
            .await
            .expect_err("operator apply failure must fault the cycle");
        let message = error.to_string();
        assert!(message.contains("failed to apply"), "{message}");
        assert!(message.contains("broken"), "{message}");
        assert!(
            message.contains("injected vnode apply failure"),
            "{message}"
        );
        assert_eq!(&*good_applied.lock(), &[0]);
        assert!(broken_applied.lock().is_empty());
        assert!(
            registry.is_restoring(0),
            "partial application must not activate the vnode"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn successful_rehydration_activates_vnode_after_all_operators_apply() {
        let partial = crate::vnode_partial::VnodePartial {
            operators: vec![
                ("left".to_string(), vec![1]),
                ("right".to_string(), vec![2]),
            ],
            base: None,
            deltas: Vec::new(),
        };
        let (mut graph, registry) =
            rehydration_test_graph(vec![encoded_vnode_partial(&partial)]).await;
        let left_applied = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let right_applied = Arc::new(parking_lot::Mutex::new(Vec::new()));
        graph.push_test_node(
            "left",
            Box::new(RehydrationApplyOperator {
                applied: Arc::clone(&left_applied),
                failure: None,
            }),
        );
        graph.push_test_node(
            "right",
            Box::new(RehydrationApplyOperator {
                applied: Arc::clone(&right_applied),
                failure: None,
            }),
        );

        graph
            .execute_cycle(&FxHashMap::default(), i64::MAX, None)
            .await
            .expect("complete vnode state should apply");

        assert_eq!(&*left_applied.lock(), &[0]);
        assert_eq!(&*right_applied.lock(), &[0]);
        assert!(
            !registry.is_restoring(0),
            "the vnode activates only after every operator applies"
        );
        assert_eq!(
            graph.last_execution_assignment_version(),
            Some(registry.assignment_version())
        );
    }

    #[test]
    fn test_node_domains_disjoint_queries_separate() {
        let mut graph = test_graph();
        graph.register_source_schema("trades_a".to_string(), test_schema());
        graph.register_source_schema("trades_b".to_string(), test_schema());
        graph.add_query(
            "qa".to_string(),
            "SELECT symbol FROM trades_a".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.add_query(
            "qb".to_string(),
            "SELECT symbol FROM trades_b".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.compute_topo_order();

        assert_eq!(
            graph.domain_count, 2,
            "disjoint-source queries are separate domains"
        );
        let a = graph.source_map.get("trades_a").copied().unwrap();
        let b = graph.source_map.get("trades_b").copied().unwrap();
        assert_ne!(graph.node_domain[a], graph.node_domain[b]);
    }

    #[test]
    fn test_node_domains_shared_source_joined() {
        let mut graph = test_graph();
        graph.register_source_schema("trades".to_string(), test_schema());
        graph.add_query(
            "qa".to_string(),
            "SELECT symbol FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.add_query(
            "qb".to_string(),
            "SELECT price FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.compute_topo_order();

        assert_eq!(
            graph.domain_count, 1,
            "queries sharing a source recover together"
        );
    }

    #[test]
    fn test_node_domains_shared_source_isolated() {
        let mut graph = test_graph();
        graph.set_shared_source_isolation(true, usize::MAX);
        graph.register_source_schema("trades".to_string(), test_schema());
        graph.add_query(
            "qa".to_string(),
            "SELECT symbol FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.add_query(
            "qb".to_string(),
            "SELECT price FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.compute_topo_order();

        assert_eq!(
            graph.domain_count, 2,
            "isolation splits shared-source queries into separate domains"
        );
        let qa = graph.find_node("qa").unwrap();
        let qb = graph.find_node("qb").unwrap();
        assert_ne!(graph.node_domain[qa], graph.node_domain[qb]);
        let src = graph.source_map.get("trades").copied().unwrap();
        assert_eq!(
            graph.node_domain[src],
            usize::MAX,
            "an isolated source is not a failure domain of its own"
        );
    }

    // A fault in one query sharing a source must not sink a sibling reading the same source: the
    // healthy query still emits, and the shared source is held back because it feeds the faulted domain.

    #[tokio::test]
    async fn terminal_shuffle_bypasses_main_failure_domain_isolation() {
        let mut graph = terminal_shuffle_graph(u64::MAX);

        let error = graph
            .execute_cycle(&trades_source(), i64::MAX, None)
            .await
            .expect_err("terminal routing must abort before isolating one domain");

        assert!(matches!(error, DbError::ShuffleTerminal(_)));
        assert!(!graph.take_cycle_failures().0);
    }

    #[tokio::test]
    async fn terminal_shuffle_bypasses_deferred_failure_domain_isolation() {
        let mut graph = terminal_shuffle_graph(0);

        let error = graph
            .execute_cycle(&trades_source(), i64::MAX, None)
            .await
            .expect_err("a deferred terminal routing failure must abort the cycle");

        assert!(matches!(error, DbError::ShuffleTerminal(_)));
        assert!(!graph.take_cycle_failures().0);
    }

    #[tokio::test]
    async fn test_execute_cycle_isolates_shared_source_sibling() {
        let mut graph = test_graph();
        graph.set_shared_source_isolation(true, usize::MAX);
        let source_node = graph.ensure_source_node("trades");
        let failing = graph
            .place_operator_node("failing", Box::new(AlwaysFailOperator), 1)
            .unwrap();
        let healthy = graph
            .place_operator_node("healthy", Box::new(SourcePassthrough), 1)
            .unwrap();
        graph.add_edge(source_node, failing, 0);
        graph.add_edge(source_node, healthy, 0);
        graph.output_map.insert(Arc::from("failing"), failing);
        graph.output_map.insert(Arc::from("healthy"), healthy);
        graph.topo_dirty = true;

        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades"), vec![test_batch()]);

        let results = graph
            .execute_cycle(&source, i64::MAX, None)
            .await
            .expect("the healthy sibling keeps the cycle Ok though they share a source");

        assert_eq!(
            total_rows(&results, "healthy"),
            2,
            "healthy sibling emitted despite sharing the faulted source"
        );
        assert_eq!(
            total_rows(&results, "failing"),
            0,
            "faulted domain emitted nothing"
        );

        let (any_failed, failed_sources) = graph.take_cycle_failures();
        assert!(any_failed);
        assert!(
            failed_sources.contains(&Arc::from("trades")),
            "the shared source is held back: it feeds the faulted domain"
        );
    }

    // A transient fault in one shared-source query replays from the preserved input on the next
    // cycle (cycle-1 rows + cycle-2 rows), while the healthy sibling only sees new rows.
    #[tokio::test]
    async fn test_shared_source_isolation_replays_faulted_domain() {
        struct ReplayTestOp {
            fail_once: bool,
            has_failed: bool,
        }
        #[async_trait]
        impl GraphOperator for ReplayTestOp {
            async fn process(
                &mut self,
                inputs: &[Vec<RecordBatch>],
                _watermarks: &[i64],
            ) -> Result<Vec<RecordBatch>, DbError> {
                if self.fail_once && !self.has_failed {
                    self.has_failed = true;
                    return Err(DbError::Pipeline("transient fault".into()));
                }
                Ok(inputs.first().cloned().unwrap_or_default())
            }
            fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
                Ok(None)
            }
            fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
                Ok(())
            }
        }

        let mut graph = test_graph();
        graph.set_shared_source_isolation(true, usize::MAX);
        let src = graph.ensure_source_node("trades");
        let a = graph
            .place_operator_node(
                "a",
                Box::new(ReplayTestOp {
                    fail_once: true,
                    has_failed: false,
                }),
                1,
            )
            .unwrap();
        graph.add_edge(src, a, 0);
        graph.output_map.insert(Arc::from("a"), a);
        let b = graph
            .place_operator_node(
                "b",
                Box::new(ReplayTestOp {
                    fail_once: false,
                    has_failed: false,
                }),
                1,
            )
            .unwrap();
        graph.add_edge(src, b, 0);
        graph.output_map.insert(Arc::from("b"), b);
        graph.topo_dirty = true;

        let mut cycle1 = FxHashMap::default();
        cycle1.insert(Arc::from("trades"), vec![test_batch()]);
        let r1 = graph
            .execute_cycle(&cycle1, i64::MAX, None)
            .await
            .expect("healthy sibling keeps cycle 1 Ok");
        assert_eq!(total_rows(&r1, "b"), 2, "healthy sibling emitted cycle 1");
        assert_eq!(
            total_rows(&r1, "a"),
            0,
            "faulted op emitted nothing cycle 1"
        );
        let (_, failed) = graph.take_cycle_failures();
        assert!(failed.contains(&Arc::from("trades")));

        let mut cycle2 = FxHashMap::default();
        cycle2.insert(Arc::from("trades"), vec![test_batch()]);
        let r2 = graph
            .execute_cycle(&cycle2, i64::MAX, None)
            .await
            .expect("cycle 2 Ok");
        assert_eq!(
            total_rows(&r2, "a"),
            4,
            "faulted op replays preserved cycle-1 rows plus new cycle-2 rows"
        );
        assert_eq!(
            total_rows(&r2, "b"),
            2,
            "healthy sibling sees only new rows (no replay)"
        );
        let (any_failed2, _) = graph.take_cycle_failures();
        assert!(!any_failed2, "no fault on the replay cycle");
    }

    // A fatal error in one disjoint query must not sink the sibling query: the healthy domain
    // still produces output, and only the faulted domain's source is held back from committing.
    #[tokio::test]
    async fn test_execute_cycle_isolates_failed_domain() {
        let mut graph = test_graph();
        let source_a = graph.ensure_source_node("trades_a");
        let source_b = graph.ensure_source_node("trades_b");
        let failing = graph
            .place_operator_node("failing", Box::new(AlwaysFailOperator), 1)
            .unwrap();
        let healthy = graph
            .place_operator_node("filtered", Box::new(SourcePassthrough), 1)
            .unwrap();
        graph.add_edge(source_a, failing, 0);
        graph.add_edge(source_b, healthy, 0);
        graph.output_map.insert(Arc::from("failing"), failing);
        graph.output_map.insert(Arc::from("filtered"), healthy);
        graph.topo_dirty = true;

        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades_a"), vec![test_batch()]);
        source.insert(Arc::from("trades_b"), vec![test_batch()]);

        let results = graph
            .execute_cycle(&source, i64::MAX, None)
            .await
            .expect("a healthy sibling domain keeps the cycle Ok");

        assert_eq!(
            total_rows(&results, "filtered"),
            2,
            "healthy domain emitted"
        );
        assert_eq!(
            total_rows(&results, "failing"),
            0,
            "faulted domain emitted nothing"
        );

        let (any_failed, failed_sources) = graph.take_cycle_failures();
        assert!(any_failed);
        assert!(failed_sources.contains(&Arc::from("trades_a")));
        assert!(!failed_sources.contains(&Arc::from("trades_b")));
    }

    #[tokio::test]
    async fn test_og_compiled_projection() {
        // Non-aggregate projection-only query should compile to PhysicalExpr
        let mut graph = test_graph();
        graph.add_query(
            "projected".to_string(),
            "SELECT symbol, price FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades"), vec![test_batch()]);

        // First cycle triggers lazy init
        let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        assert_eq!(total_rows(&r, "projected"), 2); // Both rows projected

        // Second cycle reuses compiled path (no SQL overhead)
        let r2 = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        assert_eq!(total_rows(&r2, "projected"), 2);
    }

    #[tokio::test]
    async fn test_og_compiled_fallback_on_type_mismatch() {
        // WHERE price > 200 has Float64 > Int64 type mismatch that
        // DataFusion's create_physical_expr doesn't coerce. Compiled
        // path should fall back to CachedPlan transparently.
        let mut graph = test_graph();
        graph.add_query(
            "filtered".to_string(),
            "SELECT symbol, price FROM trades WHERE price > 200".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades"), vec![test_batch()]);

        let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        assert_eq!(total_rows(&r, "filtered"), 1); // Only GOOG passes
    }

    #[tokio::test]
    async fn test_og_aggregate_incremental() {
        // GROUP BY should route through IncrementalAggState
        let mut graph = test_graph();
        graph.add_query(
            "agg".to_string(),
            "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades"), vec![test_batch()]);

        // Cycle 1
        let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        assert_eq!(total_rows(&r, "agg"), 2); // AAPL + GOOG groups

        // Cycle 2: running totals accumulate
        let r2 = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        let agg_batches = &r2[&Arc::from("agg") as &Arc<str>];
        assert_eq!(total_rows(&r2, "agg"), 2); // Still 2 groups

        // Verify accumulation: AAPL should be 150+150=300
        let price_col = agg_batches[0]
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let symbol_col = agg_batches[0]
            .column_by_name("symbol")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..agg_batches[0].num_rows() {
            match symbol_col.value(i) {
                "AAPL" => assert!((price_col.value(i) - 300.0).abs() < f64::EPSILON),
                "GOOG" => assert!((price_col.value(i) - 5600.0).abs() < f64::EPSILON),
                other => panic!("unexpected symbol: {other}"),
            }
        }
    }

    #[tokio::test]
    async fn test_og_cascading() {
        // Query A feeds Query B through intermediate LiveSourceProvider
        let mut graph = test_graph();
        graph.add_query(
            "step1".to_string(),
            "SELECT symbol, price * 2 AS doubled FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.add_query(
            "step2".to_string(),
            "SELECT symbol, doubled FROM step1 WHERE doubled > 400".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades"), vec![test_batch()]);

        let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        // step1: AAPL=300, GOOG=5600 (2 rows)
        assert_eq!(total_rows(&r, "step1"), 2);
        // step2: only GOOG=5600 passes WHERE doubled > 400
        assert_eq!(total_rows(&r, "step2"), 1);
    }

    #[test]
    fn test_og_rejects_unbounded_diamond_fanin() {
        let mut graph = test_graph();
        graph.add_query(
            "high".to_string(),
            "SELECT symbol, price FROM trades WHERE price > 200".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.add_query(
            "low".to_string(),
            "SELECT symbol, price FROM trades WHERE price <= 200".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.add_query(
            "combined".to_string(),
            "SELECT h.symbol, h.price FROM high h INNER JOIN low l ON h.symbol = l.symbol"
                .to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        let error = graph.take_build_errors().unwrap_err();
        assert!(error.to_string().contains("unbounded join"));
        assert!(!graph.has_query("combined"));
    }

    #[test]
    fn test_og_rejects_generic_cross_join_fallback() {
        let mut graph = test_graph();
        graph.add_query(
            "crossed".to_string(),
            "SELECT l.symbol FROM trades l CROSS JOIN trades r".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        let error = graph.take_build_errors().unwrap_err();
        assert!(error.to_string().contains("could not be planned"));
        assert!(!graph.has_query("crossed"));
    }

    #[tokio::test]
    async fn test_og_budget_exhaustion() {
        // With a tiny budget (1 ns), only the first operator runs
        let mut graph = test_graph();
        graph.set_query_budget_ns(1); // 1 ns budget — effectively skip after first

        graph.add_query(
            "q1".to_string(),
            "SELECT * FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.add_query(
            "q2".to_string(),
            "SELECT * FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades"), vec![test_batch()]);

        let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();

        // With 1ns budget, not all queries should produce output
        let produced = r.len();
        assert!(
            produced < 2,
            "with 1ns budget, at most one query should run"
        );
    }

    #[tokio::test]
    async fn test_og_budget_deferred_forward_progress() {
        // With a 1ns budget, only the first operator runs in the main loop.
        // The deferred execution pass must guarantee every operator eventually
        // processes its input within N cycles (N = number of deferred operators).
        let mut graph = test_graph();
        graph.set_query_budget_ns(1); // forces break after first operator

        // Add 5 independent queries — all read from "trades"
        for i in 0..5 {
            graph.add_query(
                format!("q{i}"),
                "SELECT * FROM trades".to_string(),
                None,
                None,
                None,
                None,
                false,
            );
        }

        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades"), vec![test_batch()]);

        // Run enough cycles for all 5 operators to get their turn via
        // deferred execution (1 main + 1 deferred per cycle = 5 cycles).
        let mut produced = FxHashSet::default();
        for _ in 0..5 {
            let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
            for key in r.keys() {
                produced.insert(key.to_string());
            }
        }

        assert_eq!(
            produced.len(),
            5,
            "all 5 operators should produce output within 5 cycles, got: {produced:?}"
        );
    }

    #[tokio::test]
    async fn checkpoint_drain_bypasses_query_budget_and_emits_each_row_once() {
        let mut graph = test_graph();
        // This root runs before the source and makes the near-zero budget deterministic.
        graph
            .place_operator_node("delay", Box::new(DelayOperator), 1)
            .unwrap();
        let source = graph.ensure_source_node("trades");
        let middle = graph
            .place_operator_node("middle", Box::new(SourcePassthrough), 1)
            .unwrap();
        let output = graph
            .place_operator_node("output", Box::new(SourcePassthrough), 1)
            .unwrap();
        graph.add_edge(source, middle, 0);
        graph.add_edge(middle, output, 0);
        graph.output_map.insert(Arc::from("output"), output);
        graph.topo_dirty = true;
        graph.set_query_budget_ns(1);

        let batch = test_batch();
        let expected_edge_bytes = batch.get_array_memory_size();
        let mut sources = FxHashMap::default();
        sources.insert(Arc::from("trades"), vec![batch]);

        let normal = graph.execute_cycle(&sources, i64::MAX, None).await.unwrap();
        assert_eq!(total_rows(&normal, "output"), 0);
        let (any_deferred, deferred_sources) = graph.take_cycle_deferrals();
        assert!(any_deferred);
        assert!(deferred_sources.contains(&Arc::from("trades")));
        assert_eq!(
            graph.checkpoint_pending_input_bytes(),
            expected_edge_bytes,
            "normal budget deferral leaves the source row batch on the middle edge"
        );

        let mut emitted_symbols = Vec::new();
        for _ in 0..3 {
            let mut drained = graph
                .execute_checkpoint_drain_cycle(i64::MAX, None)
                .await
                .unwrap();
            for output_batch in drained.remove("output").unwrap_or_default() {
                let symbols = output_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                emitted_symbols
                    .extend((0..output_batch.num_rows()).map(|row| symbols.value(row).to_string()));
            }
            if graph.checkpoint_is_quiescent() {
                break;
            }
        }

        assert_eq!(graph.checkpoint_pending_input_bytes(), 0);
        assert!(graph.checkpoint_is_quiescent());
        assert_eq!(emitted_symbols, ["AAPL", "GOOG"]);

        let after_quiescence = graph
            .execute_checkpoint_drain_cycle(i64::MAX, None)
            .await
            .unwrap();
        assert_eq!(
            total_rows(&after_quiescence, "output"),
            0,
            "a drained edge is not replayed"
        );
    }

    #[tokio::test]
    async fn checkpoint_drain_accounting_includes_deferred_source_ports() {
        let mut graph = test_graph();
        graph
            .place_operator_node("delay", Box::new(DelayOperator), 1)
            .unwrap();
        let source_a = graph.ensure_source_node("source_a");
        let source_b = graph.ensure_source_node("source_b");
        let output_a = graph
            .place_operator_node("output_a", Box::new(SourcePassthrough), 1)
            .unwrap();
        let output_b = graph
            .place_operator_node("output_b", Box::new(SourcePassthrough), 1)
            .unwrap();
        graph.add_edge(source_a, output_a, 0);
        graph.add_edge(source_b, output_b, 0);
        graph.output_map.insert(Arc::from("output_a"), output_a);
        graph.output_map.insert(Arc::from("output_b"), output_b);
        graph.topo_dirty = true;
        graph.set_query_budget_ns(1);

        let batch = test_batch();
        let batch_bytes = batch.get_array_memory_size();
        let mut sources = FxHashMap::default();
        sources.insert(Arc::from("source_a"), vec![batch.clone()]);
        sources.insert(Arc::from("source_b"), vec![batch]);

        let normal = graph.execute_cycle(&sources, 10, None).await.unwrap();
        assert!(normal.is_empty());
        assert_eq!(graph.input_bufs[source_b][0].len(), 1);
        assert_eq!(
            graph.checkpoint_pending_input_bytes(),
            batch_bytes.saturating_mul(2),
            "one routed edge and one budget-deferred source port are both accounted"
        );
        assert!(!graph.checkpoint_is_quiescent());

        let drained = graph
            .execute_checkpoint_drain_cycle(10, None)
            .await
            .unwrap();
        assert_eq!(total_rows(&drained, "output_a"), 2);
        assert_eq!(total_rows(&drained, "output_b"), 2);
        assert!(graph.checkpoint_is_quiescent());
    }

    #[tokio::test]
    async fn checkpoint_drain_quiescence_detects_zero_byte_row_batch() {
        let mut graph = test_graph();
        let source = graph.ensure_source_node("empty_schema_source");
        let output = graph
            .place_operator_node("output", Box::new(SourcePassthrough), 1)
            .unwrap();
        graph.add_edge(source, output, 0);
        graph.output_map.insert(Arc::from("output"), output);
        graph.topo_dirty = true;

        let options = arrow::array::RecordBatchOptions::new().with_row_count(Some(3));
        let zero_byte_rows =
            RecordBatch::try_new_with_options(Arc::new(Schema::empty()), Vec::new(), &options)
                .unwrap();
        assert_eq!(zero_byte_rows.num_rows(), 3);
        assert_eq!(zero_byte_rows.get_array_memory_size(), 0);
        prefill_port(&mut graph, output, 0, vec![zero_byte_rows]);

        assert_eq!(graph.checkpoint_pending_input_bytes(), 0);
        assert!(!graph.checkpoint_is_quiescent());

        let drained = graph
            .execute_checkpoint_drain_cycle(10, None)
            .await
            .unwrap();
        assert_eq!(total_rows(&drained, "output"), 3);
        assert!(graph.checkpoint_is_quiescent());
    }

    #[tokio::test]
    async fn checkpoint_drain_does_not_poll_unrelated_aggregate_branch() {
        let mut graph = test_graph();
        graph.register_source_schema("trades".to_string(), test_schema());
        graph.add_query(
            "agg".to_string(),
            "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        let mut trades = FxHashMap::default();
        trades.insert(Arc::from("trades"), vec![test_batch()]);
        let initial = graph.execute_cycle(&trades, 10, None).await.unwrap();
        assert_eq!(total_rows(&initial, "agg"), 2);
        assert!(graph.checkpoint_is_quiescent());

        let other_source = graph.ensure_source_node("other");
        let other_output = graph
            .place_operator_node("other_output", Box::new(SourcePassthrough), 1)
            .unwrap();
        graph.add_edge(other_source, other_output, 0);
        graph
            .output_map
            .insert(Arc::from("other_output"), other_output);
        graph.topo_dirty = true;
        prefill_port(&mut graph, other_source, 0, vec![test_batch()]);

        let drained = graph
            .execute_checkpoint_drain_cycle(10, None)
            .await
            .unwrap();
        assert_eq!(total_rows(&drained, "other_output"), 2);
        assert_eq!(
            total_rows(&drained, "agg"),
            0,
            "the unchanged aggregate branch must not re-emit during another branch's drain"
        );
        assert!(graph.checkpoint_is_quiescent());
    }

    #[tokio::test]
    async fn checkpoint_drain_failure_or_no_progress_preserves_pending_edges() {
        struct PausedOperator;

        #[async_trait]
        impl GraphOperator for PausedOperator {
            async fn process(
                &mut self,
                inputs: &[Vec<RecordBatch>],
                _watermarks: &[i64],
            ) -> Result<Vec<RecordBatch>, DbError> {
                assert!(inputs.is_empty(), "paused operator must not accept input");
                Ok(Vec::new())
            }

            fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
                Ok(None)
            }

            fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
                Ok(())
            }

            fn wants_input(&self) -> bool {
                false
            }
        }

        let mut graph = test_graph();
        let source = graph.ensure_source_node("trades");
        let middle = graph
            .place_operator_node("middle", Box::new(SourcePassthrough), 1)
            .unwrap();
        let paused = graph
            .place_operator_node("paused", Box::new(PausedOperator), 1)
            .unwrap();
        graph.add_edge(source, middle, 0);
        graph.add_edge(middle, paused, 0);
        graph.topo_dirty = true;
        prefill_port(&mut graph, middle, 0, vec![test_batch()]);
        prefill_port(&mut graph, paused, 0, vec![test_batch()]);
        graph.set_max_input_buf_batches(1);

        let pending_before = graph.checkpoint_pending_input_bytes();
        assert_eq!(pending_before, 2 * test_batch().get_array_memory_size());
        assert!(!graph.checkpoint_is_quiescent());

        graph.set_backpressure_policy(BackpressurePolicy::Fail);
        let error = graph
            .execute_checkpoint_drain_cycle(i64::MAX, None)
            .await
            .expect_err("the checkpoint drain must preserve Fail backpressure semantics");
        assert!(matches!(error, DbError::BackpressureFail(_)));
        assert_eq!(graph.checkpoint_pending_input_bytes(), pending_before);
        assert!(!graph.checkpoint_is_quiescent());

        graph.set_backpressure_policy(BackpressurePolicy::Backpressure);
        graph
            .execute_checkpoint_drain_cycle(i64::MAX, None)
            .await
            .unwrap();
        assert_eq!(
            graph.checkpoint_pending_input_bytes(),
            pending_before,
            "a gated/paused drain cycle must not clear pending edge buffers"
        );
        assert_eq!(
            graph.output_watermarks[paused],
            i64::MIN,
            "an operator that declined buffered input must not advance its output watermark"
        );
        assert!(!graph.checkpoint_is_quiescent());
    }

    #[tokio::test]
    async fn test_og_checkpoint_roundtrip_aggregate() {
        // Aggregate state should survive checkpoint + restore
        let mut graph = test_graph();
        graph.add_query(
            "agg".to_string(),
            "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades"), vec![test_batch()]);

        // Cycle 1: build up state
        let _ = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();

        // Snapshot
        let cp = graph
            .snapshot_state()
            .unwrap()
            .expect("aggregate should have state");
        let bytes = OperatorGraph::serialize_checkpoint_bounded(&cp, u64::MAX).unwrap();

        // Create a new graph with same query and restore
        let mut graph2 = test_graph();
        graph2.add_query(
            "agg".to_string(),
            "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        let (restored_graph, restored) = graph2.restore_from_bytes(&bytes).unwrap();
        let mut graph2 = restored_graph;
        assert!(restored > 0, "should restore at least one operator");

        // New input is applied on top of the authoritative restored image.
        let r = graph2.execute_cycle(&source, i64::MAX, None).await.unwrap();
        assert_eq!(total_rows(&r, "agg"), 2);
    }

    #[tokio::test]
    async fn test_og_aggregate_empty_source_emits_state() {
        // Aggregate queries should emit running state even with no new input
        let mut graph = test_graph();
        graph.register_source_schema("trades".to_string(), test_schema());
        graph.add_query(
            "agg".to_string(),
            "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades"), vec![test_batch()]);

        // First cycle with data
        let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        assert_eq!(total_rows(&r, "agg"), 2);

        // Second cycle with no data — should still emit accumulated state
        let empty_source = FxHashMap::default();
        let r2 = graph
            .execute_cycle(&empty_source, i64::MAX, None)
            .await
            .unwrap();
        assert_eq!(total_rows(&r2, "agg"), 2);
    }

    #[tokio::test]
    async fn test_og_reverse_order_cascading() {
        // Queries added in reverse dependency order (q2 before q1).
        // q2 creates a SourcePassthrough placeholder for "q1". When q1 is
        // added, it replaces the placeholder in place so q2's existing edge
        // automatically receives q1's real output.
        let mut graph = test_graph();
        graph.add_query(
            "q2".to_string(),
            "SELECT symbol FROM q1 WHERE price > 200".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.add_query(
            "q1".to_string(),
            "SELECT symbol, price FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        // "q1" should NOT be in source_map (it was replaced with a real query)
        assert!(
            !graph.source_map.contains_key("q1"),
            "q1 placeholder should be replaced, not in source_map"
        );
        assert!(graph.output_map.contains_key("q1"));
        assert!(graph.output_map.contains_key("q2"));

        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades"), vec![test_batch()]);

        let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        assert_eq!(total_rows(&r, "q1"), 2); // AAPL + GOOG
        assert_eq!(total_rows(&r, "q2"), 1); // Only GOOG (price=2800 > 200)
    }

    #[tokio::test]
    async fn test_temporal_probe_through_graph() {
        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);
        let mut graph = OperatorGraph::new(ctx);

        let trades_schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        let market_schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("mts", DataType::Int64, false),
            Field::new("mprice", DataType::Float64, false),
        ]));

        graph.register_source_schema("trades".to_string(), trades_schema.clone());
        graph.register_source_schema("market_data".to_string(), market_schema);

        graph.add_query(
            "probed".to_string(),
            "SELECT t.symbol, p.offset_ms, mprice \
             FROM trades t \
             TEMPORAL PROBE JOIN market_data m ON (symbol) \
             TIMESTAMPS (ts, mts) LIST (0s, 5s) AS p"
                .to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        // Cycle 1: inject both sides, watermark=102k (only offset=0 resolves)
        let trades = RecordBatch::try_new(
            trades_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["AAPL"])),
                Arc::new(Int64Array::from(vec![100_000])),
                Arc::new(Float64Array::from(vec![152.5])),
            ],
        )
        .unwrap();
        let market = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("symbol", DataType::Utf8, false),
                Field::new("mts", DataType::Int64, false),
                Field::new("mprice", DataType::Float64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "AAPL"])),
                Arc::new(Int64Array::from(vec![100_000, 105_000])),
                Arc::new(Float64Array::from(vec![150.0, 155.0])),
            ],
        )
        .unwrap();

        let mut sources = FxHashMap::default();
        sources.insert(Arc::from("trades"), vec![trades]);
        sources.insert(Arc::from("market_data"), vec![market]);

        let r1 = graph.execute_cycle(&sources, 102_000, None).await.unwrap();
        let rows1 = total_rows(&r1, "probed");
        assert_eq!(rows1, 1, "only offset=0 should resolve at watermark=102k");

        // Cycle 2: no new data, advance watermark past offset=5000 (probe_ts=105000)
        let empty = FxHashMap::default();
        let r2 = graph.execute_cycle(&empty, 110_000, None).await.unwrap();
        let rows2 = total_rows(&r2, "probed");
        assert_eq!(rows2, 1, "offset=5000 should resolve at watermark=110k");
    }

    #[test]
    fn test_pressure_zero_when_cap_disabled() {
        let mut graph = test_graph();
        graph.set_max_input_buf_batches(0); // unlimited
        graph.add_query(
            "q1".to_string(),
            "SELECT * FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        // Push some data into the source buffer
        if let Some(&node_id) = graph.source_map.get("trades") {
            prefill_port(&mut graph, node_id, 0, vec![test_batch(); 10]);
        }
        assert!((graph.input_buf_pressure() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_pressure_reflects_fill_ratio() {
        let mut graph = test_graph();
        graph.set_max_input_buf_batches(100);
        graph.add_query(
            "q1".to_string(),
            "SELECT * FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        // Fill source buffer to 50% of cap
        if let Some(&node_id) = graph.source_map.get("trades") {
            prefill_port(&mut graph, node_id, 0, vec![test_batch(); 50]);
        }
        assert!((graph.input_buf_pressure() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_pressure_clamped_at_one() {
        let mut graph = test_graph();
        graph.set_max_input_buf_batches(10);
        graph.add_query(
            "q1".to_string(),
            "SELECT * FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        // Overfill the buffer beyond cap — pressure clamps at 1.0.
        if let Some(&node_id) = graph.source_map.get("trades") {
            prefill_port(&mut graph, node_id, 0, vec![test_batch(); 20]);
        }
        assert!((graph.input_buf_pressure() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_pressure_empty_graph() {
        let graph = test_graph();
        assert!((graph.input_buf_pressure() - 0.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_credit_gate_defers_producer_when_downstream_full() {
        let mut graph = test_graph();
        graph.set_max_input_buf_batches(4);

        // Two queries chained via an intermediate stream: the first projects
        // `trades`, the second reads from the first. The gate should skip the
        // first when the second's input port is full.
        graph.add_query(
            "proj".to_string(),
            "SELECT symbol, price FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.add_query(
            "downstream".to_string(),
            "SELECT symbol FROM proj".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        // Find the downstream node id and pre-fill its input buffer at cap,
        // simulating a slow consumer.
        let downstream_id = *graph.output_map.get("downstream").unwrap();
        prefill_port(&mut graph, downstream_id, 0, vec![test_batch(); 4]);

        let proj_id = *graph.output_map.get("proj").unwrap();
        assert!(
            graph.is_downstream_at_capacity(proj_id),
            "proj's downstream should register as at capacity"
        );

        // Run a cycle with trade input. proj must be deferred because its
        // downstream is full — so proj's output_bufs should still hold its
        // source input, and downstream's input should not grow.
        let before_len = graph.input_bufs[downstream_id][0].len();
        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades"), vec![test_batch()]);
        let _ = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        assert_eq!(
            graph.input_bufs[downstream_id][0].len(),
            before_len,
            "deferred producer must not have extended a full downstream buffer"
        );
    }

    // Replacing a SourcePassthrough placeholder must also clear source_node_ids,
    // otherwise the node keeps its source-class flag and output_watermarks is
    // never advanced — downstream TUMBLE windows never close.
    #[tokio::test]
    async fn test_placeholder_replacement_clears_source_classification() {
        let mut graph = test_graph();

        // Register the downstream query FIRST — its SQL references
        // `derived`, which triggers an `ensure_source_node("derived")` and
        // seeds `source_node_ids` with the placeholder.
        graph.add_query(
            "aggregate".to_string(),
            "SELECT symbol, SUM(price) AS total FROM derived GROUP BY symbol".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        // Now register `derived` — this replaces the placeholder.
        graph.add_query(
            "derived".to_string(),
            "SELECT symbol, price FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        let derived_id = *graph.output_map.get("derived").unwrap();
        assert!(
            !graph.source_node_ids.contains(&derived_id),
            "real operator node must not be classified as a source after \
             placeholder replacement (blocks output_watermarks updates)"
        );
    }

    #[tokio::test]
    async fn test_source_inputs_accumulate_when_deferred() {
        let mut graph = test_graph();
        graph.set_max_input_buf_batches(2);
        graph.add_query(
            "sink".to_string(),
            "SELECT symbol FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        // Pre-fill sink's input at cap. Because sink has no downstream, sink
        // will still run this cycle — so to keep trades deferred across a
        // second cycle we keep the cap threshold tight and re-fill sink each
        // cycle, simulating a continuous slow-consumer scenario.
        let sink_id = *graph.output_map.get("sink").unwrap();
        let source_id = *graph.source_map.get("trades").unwrap();
        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades"), vec![test_batch()]);

        // Cycle 1: sink's input pre-filled to cap, trades deferred, trades
        // input extended by 1.
        prefill_port(&mut graph, sink_id, 0, vec![test_batch(); 2]);
        let _ = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        assert_eq!(
            graph.input_bufs[source_id][0].len(),
            1,
            "deferred source must accumulate its input buffer"
        );

        // Cycle 2: re-fill sink to cap so trades stays deferred; trades input
        // must grow from 1 to 2 (extend, not clone_from).
        prefill_port(&mut graph, sink_id, 0, vec![test_batch(); 2]);
        let _ = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        assert_eq!(
            graph.input_bufs[source_id][0].len(),
            2,
            "source input must accumulate across deferred cycles"
        );
    }

    /// Regression test: LEFT JOIN between a streaming source and a
    /// `ReferenceTableProvider` (lookup table) must work across multiple
    /// cycles without panicking. Before the fix, `RepartitionExec` in the
    /// cached physical plan had consumed internal channels on the first
    /// cycle, causing `"partition not used yet"` on the second.
    #[tokio::test]
    async fn test_lookup_left_join_multi_cycle() {
        use crate::table_store::TableStore;

        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);

        // Register a lookup table via ReferenceTableProvider
        let lookup_schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("company_name", DataType::Utf8, true),
        ]));
        let ts = Arc::new(parking_lot::RwLock::new(TableStore::new()));
        {
            let mut store = ts.write();
            store
                .create_table("instruments", lookup_schema.clone(), "symbol")
                .unwrap();
            let batch = RecordBatch::try_new(
                lookup_schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["AAPL", "GOOG"])),
                    Arc::new(StringArray::from(vec!["Apple Inc.", "Alphabet"])),
                ],
            )
            .unwrap();
            store.upsert("instruments", &batch).unwrap();
        }
        let provider = crate::table_provider::ReferenceTableProvider::new(
            "instruments".to_string(),
            lookup_schema,
            ts,
        );
        ctx.register_table("instruments", Arc::new(provider))
            .unwrap();

        let mut graph = OperatorGraph::new(ctx);
        graph.register_source_schema("trades".to_string(), test_schema());
        graph.set_reference_tables(["instruments".to_string()].into_iter().collect());

        graph.add_query(
            "enriched".to_string(),
            "SELECT t.symbol, t.price, i.company_name \
             FROM trades t LEFT JOIN instruments i ON t.symbol = i.symbol"
                .to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        let batch = test_batch(); // AAPL + GOOG
        let mut source = FxHashMap::default();
        source.insert(Arc::from("trades"), vec![batch.clone()]);

        // Cycle 1
        let r1 = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        let rows1: usize = r1
            .get("enriched")
            .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum());
        assert_eq!(rows1, 2, "cycle 1 should produce 2 joined rows");

        // Cycle 2 — this panicked before the fix
        source.insert(Arc::from("trades"), vec![batch]);
        let r2 = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        let rows2: usize = r2
            .get("enriched")
            .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum());
        assert_eq!(rows2, 2, "cycle 2 should also produce 2 joined rows");
    }

    #[tokio::test]
    async fn test_self_join_prefilter_end_to_end() {
        use arrow::array::TimestampMillisecondArray;
        use arrow::datatypes::TimeUnit;

        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);
        let mut graph = OperatorGraph::new(ctx);

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));
        graph.register_source_schema("events".to_string(), Arc::clone(&schema));

        graph.add_query(
            "joined".to_string(),
            "SELECT p.key, p.type, a.type \
             FROM events p \
             JOIN events a ON p.key = a.key \
             AND a.ts BETWEEN p.ts AND p.ts + INTERVAL '10' SECOND \
             WHERE p.type = 'A' AND a.type = 'B'"
                .to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        // source + 2 filter nodes + join operator = 4
        assert!(
            graph.nodes.len() >= 4,
            "expected 4+ nodes, got {}",
            graph.nodes.len()
        );

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["k1", "k1", "k1", "k1"])),
                Arc::new(StringArray::from(vec!["A", "B", "A", "B"])),
                Arc::new(TimestampMillisecondArray::from(vec![
                    1000, 2000, 3000, 4000,
                ])),
            ],
        )
        .unwrap();

        let mut source = FxHashMap::default();
        source.insert(Arc::from("events"), vec![batch.clone()]);

        // First cycle seeds the join buffers; second cycle produces matches
        // when buffered left (type=A) rows see right (type=B) rows. Keep the
        // watermark below the rows so the first cycle does not close their interval.
        let _ = graph.execute_cycle(&source, 0, None).await.unwrap();

        source.clear();
        source.insert(Arc::from("events"), vec![batch]);
        let results = graph.execute_cycle(&source, 0, None).await.unwrap();

        let total_rows: usize = results
            .get("joined")
            .map_or(0, |batches| batches.iter().map(|b| b.num_rows()).sum());

        assert!(
            total_rows > 0,
            "should produce matches from prefiltered self-join"
        );
    }

    fn prefill_port(
        graph: &mut OperatorGraph,
        node: usize,
        port: usize,
        batches: Vec<RecordBatch>,
    ) {
        let bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        graph.input_bufs[node][port] = batches;
        graph.input_buf_bytes[node][port] = bytes;
    }

    fn producer_consumer_graph(policy: BackpressurePolicy, cap: usize) -> (OperatorGraph, usize) {
        let mut graph = test_graph();
        graph.set_max_input_buf_batches(cap);
        graph.set_backpressure_policy(policy);
        graph.add_query(
            "producer".to_string(),
            "SELECT symbol, price FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.add_query(
            "consumer".to_string(),
            "SELECT symbol FROM producer".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        let consumer_id = *graph.output_map.get("consumer").unwrap();
        prefill_port(&mut graph, consumer_id, 0, vec![test_batch(); cap]);
        (graph, consumer_id)
    }

    fn trades_source() -> FxHashMap<Arc<str>, Vec<RecordBatch>> {
        let mut s = FxHashMap::default();
        s.insert(Arc::from("trades"), vec![test_batch()]);
        s
    }

    #[tokio::test]
    async fn test_backpressure_policy_defers_without_shedding() {
        let (mut graph, consumer_id) = producer_consumer_graph(BackpressurePolicy::Backpressure, 2);
        let _ = graph
            .execute_cycle(&trades_source(), i64::MAX, None)
            .await
            .unwrap();
        assert_eq!(
            graph.input_bufs[consumer_id][0].len(),
            2,
            "consumer input stays at cap — producer must have been deferred"
        );
    }

    #[tokio::test]
    async fn test_shed_oldest_policy_drops_rows_and_increments_counter() {
        let registry = prometheus::Registry::new();
        let prom = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
        let (mut graph, consumer_id) = producer_consumer_graph(BackpressurePolicy::ShedOldest, 2);
        graph.set_metrics(Arc::clone(&prom));

        let _ = graph
            .execute_cycle(&trades_source(), i64::MAX, None)
            .await
            .unwrap();

        assert!(graph.input_bufs[consumer_id][0].len() <= 2);
        assert!(
            prom.shed_records_total
                .with_label_values(&["consumer"])
                .get()
                > 0,
            "shed_records_total should have incremented"
        );
    }

    #[tokio::test]
    async fn test_fail_policy_returns_error_at_cap() {
        let (mut graph, _) = producer_consumer_graph(BackpressurePolicy::Fail, 2);
        let err = graph
            .execute_cycle(&trades_source(), i64::MAX, None)
            .await
            .expect_err("Fail policy must return an error at capacity");
        assert!(
            matches!(err, DbError::BackpressureFail(_)),
            "expected DbError::BackpressureFail, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_byte_budget_gates_capacity() {
        let mut graph = test_graph();
        graph.set_max_input_buf_bytes(Some(1));
        graph.add_query(
            "producer".to_string(),
            "SELECT symbol, price FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        graph.add_query(
            "consumer".to_string(),
            "SELECT symbol FROM producer".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
        let consumer_id = *graph.output_map.get("consumer").unwrap();
        prefill_port(&mut graph, consumer_id, 0, vec![test_batch()]);

        let producer_id = *graph.output_map.get("producer").unwrap();
        assert!(graph.is_downstream_at_capacity(producer_id));
    }
}
